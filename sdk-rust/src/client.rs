//! Cliente de flux. Nivel de conformidad: **L2**.
//!
//! Contrato normativo: `specification/00-protocol.md` §5
//!
//! Regla de diseño: este módulo **NO expone ningún tipo de NATS en su API pública**. Si
//! lo hiciera, sustituir el broker dejaría de ser un cambio de capa 0-1 y pasaría a tocar
//! las aplicaciones — que es exactamente lo que flux existe para evitar
//! (00-protocol.md §3). Los tipos de `async_nats` viven detrás de campos privados, y los
//! errores de transporte se envuelven en [`FluxError::Transport`].

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, ReplayPolicy};
use async_nats::jetstream::context::{ConsumerInfoErrorKind, GetStreamErrorKind};
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::stream::{DiscardPolicy, RetentionPolicy, StorageType};
use async_nats::jetstream::{consumer, stream, AckKind, ErrorCode};
use chrono::{DateTime, Utc};
use futures::{FutureExt, StreamExt};
use serde::Serialize;
use serde_json::json;
use tokio::sync::watch;
use uuid::Uuid;

use crate::classify::{Classifier, ClassifierOptions};
use crate::context::{self, EventContext};
use crate::envelope::{
    build_event, data_to_raw, format_time, parse_event, serialize, strip_dlq_extensions,
    to_dlq_event, BuildEventInput, DataClassification, DlqInfo, DlqReason, Event,
};
use crate::errors::{
    describe, Classification, ConfigDifference, ErrorClass, FluxError, HandlerError, HandlerResult,
    ResourceKind,
};
use crate::metrics::{
    ConnectionState, ConsumeOutcome, DlqReasonLabel, MetricsSink, NoMetrics, PublishOutcome,
};
use crate::protocol::{
    dlq_stream_name, dlq_subject, durable_name, inbox_prefix, parse_subject, source_uri,
    stream_name, validate_service_name, ParsedSubject, CANONICAL_BACKOFF, DEFAULT_ACK_WAIT,
    DEFAULT_MAX_ACK_PENDING, DEFAULT_MAX_DELIVER, DLQ_STREAM_MAX_AGE, DUPLICATE_WINDOW,
    STREAM_MAX_AGE,
};
use crate::security::{
    connect_error_detail, fatal_connect_kind, parse_violation, validate_nkey_seed, Denial,
    DenialLog, DeniedAction, FatalConnect, TlsOptions,
};

// ─── Observabilidad ──────────────────────────────────────────────────────────

/// Cada cuánto se pregunta al servidor por el `num_pending` de cada consumidor
/// — 08-observability.md §2.3.
///
/// 15 s es el orden de magnitud que fija la spec: una petición cada 15 segundos por
/// consumidor es despreciable frente al tráfico de eventos, y es de sobra frecuente para
/// una alerta de "consumidor atascado" que dispara a los 10 minutos.
pub const DEFAULT_PENDING_POLL: Duration = Duration::from_secs(15);

/// Cuánto espera la sonda del permiso de pull (`Bus::ensure_pull_allowed`) antes de darse por
/// no concluyente. Una respuesta o una denegación llegan en ~1 ms; 2 s solo se agotan con un
/// broker que no contesta, y entonces `subscribe()` sigue —no poder comprobar un permiso no es
/// lo mismo que no tenerlo— y avisa por el `logger`.
const PULL_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Severidad de un mensaje del SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    /// Reintento programado, evento descartado por filtro de tenant.
    Warn,
    /// POISON, enrutado a la DLQ, fallo del bucle de consumo.
    Error,
}

/// Función de log inyectada por la aplicación.
///
/// ⚠️ Divergencia con Go y Node, y es deliberada: allí el SDK toma `log/slog` y `pino`
/// respectivamente, que son la elección obvia de cada ecosistema. En Rust no la hay —
/// `tracing` y `log` conviven — y una dependencia dura obligaría a todo servicio a
/// arrastrar la que no usa. Se invierte: la aplicación pasa un closure. Con `tracing` es
/// una línea.
pub type LogFn = Arc<dyn Fn(LogLevel, &str) + Send + Sync>;

/// Extrae el `traceparent` W3C del contexto activo.
///
/// Misma inversión que [`LogFn`] y por la misma razón que en Go: Node hace un `import()`
/// dinámico de `@opentelemetry/api` y falla en silencio si no está; Rust no tiene
/// imports dinámicos, y una dependencia dura de OpenTelemetry la pagaría todo el mundo.
///
/// Devolver `None` significa "no hay span activo" y el atributo se omite.
pub type TraceparentFn = Arc<dyn Fn() -> Option<String> + Send + Sync>;

/// Un mensaje que no se pudo interpretar.
#[derive(Debug)]
pub struct PoisonInfo {
    /// Subject por el que llegó.
    pub subject: String,
    /// El fallo de parseo, con su código estable.
    pub error: FluxError,
    /// El cuerpo tal cual llegó. Es lo único que queda para el forense.
    pub raw: Vec<u8>,
}

/// Un evento enrutado a la DLQ.
#[derive(Debug)]
pub struct DlqEventInfo {
    /// Subject original (no el `dlq.`).
    pub subject: String,
    /// El evento tal cual se guardó, ya con las extensiones `dlq*`.
    pub event: Event,
    /// Clasificación que lo mandó allí.
    pub classification: Classification,
}

/// Aislamiento entre tenants — 09-multitenancy.md §3.
///
/// El Modelo A de v1 mezcla todos los tenants en un stream por dominio y **el aislamiento
/// es una convención del SDK, no una frontera del broker**. Con `Strict`, esa convención
/// deja al menos de depender de que alguien se acuerde.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TenantIsolation {
    /// **Default.** El filtrado es opcional por suscripción.
    #[default]
    Off,
    /// Toda suscripción filtra por tenant, y suscribirse sin uno configurado es un
    /// [`FluxError::TenantIsolation`].
    ///
    /// Es el punto que importa de §3: un filtro que hay que acordarse de poner es un
    /// filtro que alguien olvidará, y el fallo —ver los datos de otro tenant— **no produce
    /// ningún error**: produce un incidente de privacidad que se descubre semanas después.
    Strict,
}

// ─── Opciones de conexión ────────────────────────────────────────────────────

/// Credenciales de NATS. **NUNCA versionadas** — 06-security.md §2.
///
/// Un valor mal formado (un valor vacío, un `.creds` ilegible o sin JWT, una semilla que no es
/// `SU…`) falla **en [`connect`]**, antes de tocar la red, con [`FluxError::InvalidCredentials`]
/// (`INVALID_CREDENTIALS`). Una credencial bien formada pero rechazada por el broker también
/// devuelve el error en `connect()`, con [`FluxError::ConnectionRejected`]
/// (`CONNECTION_REJECTED`, motivo [`crate::RejectionReason::Auth`]): reintentarla no la arregla.
///
/// `Debug` **no imprime secretos**: un `{:?}` de la configuración acaba en un log.
#[derive(Clone, Default)]
pub enum Credentials {
    /// Sin autenticación. Solo para el docker-compose de desarrollo.
    #[default]
    None,
    /// Ruta a un fichero `.creds` (JWT de usuario + semilla nkey). Es la forma recomendada
    /// con un operador/JWT.
    CredsFile(PathBuf),
    /// Semilla nkey de usuario (`SU…`), con las cuentas definidas en la configuración del
    /// broker. Pensada para inyectarla desde un gestor de secretos.
    NkeySeed(String),
    /// Ruta a un fichero con una semilla nkey de usuario (`.nk`).
    NkeyFile(PathBuf),
    /// Token estático.
    Token(String),
    /// Usuario y contraseña.
    UserPassword {
        /// Usuario.
        user: String,
        /// Contraseña.
        password: String,
    },
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            // La ruta no es un secreto; lo que hay dentro, sí.
            Self::CredsFile(p) => f.debug_tuple("CredsFile").field(p).finish(),
            Self::NkeyFile(p) => f.debug_tuple("NkeyFile").field(p).finish(),
            Self::NkeySeed(_) => f.write_str("NkeySeed(<oculto>)"),
            Self::Token(_) => f.write_str("Token(<oculto>)"),
            Self::UserPassword { user, .. } => f
                .debug_struct("UserPassword")
                .field("user", user)
                .field("password", &"<oculto>")
                .finish(),
        }
    }
}

/// Configuración de la conexión al bus.
///
/// `service`, `environment` y `version` son obligatorios porque alimentan `source` y
/// `producerversion`, que son atributos obligatorios del envelope.
pub struct ConnectOptions {
    /// `nats://host:4222`, o **varios separados por coma** para HA
    /// (`nats://a:4222,nats://b:4222`): se prueban hasta que uno contesta, y la reconexión usa la
    /// lista entera. Los espacios alrededor de cada dirección y las comas sobrantes se toleran.
    ///
    /// `async-nats` 0.50 interpreta un `&str` como **una** dirección y una lista con comas falla
    /// con «NATS server URL is invalid» (medido): `connect()` la separa antes de dárselo. Para
    /// construirla desde una lista, [`ConnectOptions::with_servers`] (el `string[]` de Node).
    pub servers: String,
    /// Nombre del servicio. Alimenta `source` y los nombres de durable. Se valida contra
    /// `^[a-z0-9]+(-[a-z0-9]+)*$` en [`connect`].
    pub service: String,
    /// `produccion`, `staging`, `dev`. Alimenta `source`.
    pub environment: String,
    /// SemVer del servicio. Va en `producerversion`.
    pub version: String,

    /// Tenant por defecto de los eventos publicados. `None` significa `"system"`.
    pub tenant_id: Option<String>,
    /// Aislamiento entre tenants al consumir — 09-multitenancy.md §3.
    pub tenant_isolation: TenantIsolation,
    /// Clasificación por defecto. `None` significa `internal` — 06-security.md §5.
    pub classification: Option<DataClassification>,

    /// Destino de las métricas. `None` significa [`NoMetrics`].
    ///
    /// Los nombres y las etiquetas los fija el protocolo (08-observability.md), no la
    /// aplicación: si cada SDK nombrara a su manera, un panel del ecosistema sería
    /// imposible. Lo que elige la aplicación es el backend.
    pub metrics: Option<Arc<dyn MetricsSink>>,

    /// Firma Ed25519 de eventos — extensión **OPCIONAL**, detrás de la feature `signing`.
    ///
    /// Traslada la autenticidad del canal al evento: un evento firmado sigue siendo
    /// verificable dentro de un fichero, un backup o un correo, donde ya no hay ACL que lo
    /// respalde — 07-signing.md §1.
    #[cfg(feature = "signing")]
    pub signing: crate::signing::SigningOptions,

    /// Validación del payload contra su JSON Schema — **L3**, detrás de la feature
    /// `validation`.
    ///
    /// Con `mode = Strict`, publicar un payload que viola su propio contrato falla en el
    /// productor en vez de aparecer como un misterio en un consumidor de otro equipo la
    /// semana que viene — 00-protocol.md §5.
    #[cfg(feature = "validation")]
    pub validation: crate::validation::ValidationOptions,

    /// Mapa exacto subject → URI de `dataschema`. Gana sobre [`Self::schema_base_url`].
    pub schemas: HashMap<String, String>,
    /// Base para derivar `dataschema` cuando el subject no está en [`Self::schemas`].
    pub schema_base_url: Option<String>,

    /// Cada cuánto preguntar al servidor el `num_pending` de cada consumidor.
    /// [`Duration::ZERO`] lo desactiva. Default: 15 s.
    ///
    /// No es un capricho de configuración: `flux_consumer_pending` tiene **dos** fuentes y
    /// un SDK L2 **DEBE** usar las dos, porque cada una falla justo donde la otra sirve
    /// (08-observability.md §2.3). Los metadatos de cada entrega son gratis y frescos,
    /// pero **si el bucle del consumidor muere dejan de llegar mensajes**: el gauge se
    /// queda plano en su último valor en vez de crecer, y un panel dibuja una línea
    /// horizontal indistinguible de "no pasa nada". El sondeo sigue corriendo y reporta el
    /// `num_pending` creciente. Ésa es la señal.
    ///
    /// Solo se sondea si hay [`Self::metrics`] configurado: sin destino, la petición al
    /// servidor no le sirve a nadie.
    pub pending_poll: Duration,

    /// Política de clasificación de errores. Ver [`crate::classify`].
    pub classifier: ClassifierOptions,

    /// Credenciales.
    pub credentials: Credentials,

    /// TLS: CA propia, certificado de cliente y exigir TLS — 06-security.md §2.
    /// `None` deja que decida la URL (`tls://`) o el broker.
    pub tls: Option<TlsOptions>,

    /// Extractor de `traceparent`.
    pub traceparent: Option<TraceparentFn>,

    /// Se invoca ante un POISON. Es el único caso que debe **despertar a alguien**: casi
    /// siempre significa que un productor está roto — 04-errors.md §1.3.
    pub on_poison: Option<Arc<dyn Fn(PoisonInfo) + Send + Sync>>,

    /// Se invoca al enrutar cualquier evento a la DLQ.
    pub on_dlq: Option<Arc<dyn Fn(DlqEventInfo) + Send + Sync>>,

    /// Log opcional. `None` significa silencio.
    pub logger: Option<LogFn>,
}

impl fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectOptions")
            .field("servers", &self.servers)
            .field("service", &self.service)
            .field("environment", &self.environment)
            .field("version", &self.version)
            .field("tenant_id", &self.tenant_id)
            .field("tenant_isolation", &self.tenant_isolation)
            .field("classification", &self.classification)
            .field("schema_base_url", &self.schema_base_url)
            .field("pending_poll", &self.pending_poll)
            .field("classifier", &self.classifier)
            // Las credenciales NUNCA se imprimen: un Debug acaba en un log.
            .field("credentials", &"<oculto>")
            .field("tls", &self.tls)
            .finish_non_exhaustive()
    }
}

/// Separa la lista de servidores de [`ConnectOptions::servers`]: por comas, sin espacios
/// alrededor y sin entradas vacías. **Una** dirección (el caso corriente) es una lista de uno.
///
/// Existe porque `async-nats` 0.50 interpreta una `&str` como **una** dirección: una lista con
/// comas no se entiende ni cuando todas sus direcciones son válidas (medido), así que una alta
/// disponibilidad configurable como `nats://a:4222,nats://b:4222` —lo que la documentación de
/// `servers` prometía— no funcionaba. Node admite `string | string[]`; aquí es una cadena con
/// comas, o [`ConnectOptions::with_servers`].
pub(crate) fn split_servers(servers: &str) -> Vec<String> {
    servers
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

/// [`split_servers`], y un error claro si no queda ninguna: una lista vacía es un descuido (una
/// variable de entorno sin definir) y falla **antes de la red** diciéndolo.
fn require_servers(servers: &str) -> Result<Vec<String>, FluxError> {
    let list = split_servers(servers);
    if list.is_empty() {
        return Err(FluxError::Config(
            "`servers` está vacío: pasa `nats://host:4222`, o varias direcciones separadas por coma \
             (`ConnectOptions::with_servers`). Una lista vacía suele ser una variable de entorno \
             sin definir"
                .to_string(),
        ));
    }
    Ok(list)
}

impl ConnectOptions {
    /// Sustituye la lista de servidores por la dada, **una dirección por elemento** — el
    /// `servers: string[]` de Node. Equivale a pasar a [`ConnectOptions::new`] las direcciones
    /// unidas por comas.
    ///
    /// ```
    /// let opts = flux::ConnectOptions::new("nats://a:4222", "pedidos-api", "produccion", "1.0.0")
    ///     .with_servers(["nats://a:4222", "nats://b:4222", "nats://c:4222"]);
    /// assert_eq!(opts.servers, "nats://a:4222,nats://b:4222,nats://c:4222");
    /// ```
    #[must_use]
    pub fn with_servers<I, S>(mut self, servers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.servers = servers
            .into_iter()
            .map(|s| s.as_ref().trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(",");
        self
    }

    /// Los cuatro campos obligatorios. El resto tiene defaults de la spec.
    #[must_use]
    pub fn new(
        servers: impl Into<String>,
        service: impl Into<String>,
        environment: impl Into<String>,
        version: impl Into<String>,
    ) -> Self {
        Self {
            servers: servers.into(),
            service: service.into(),
            environment: environment.into(),
            version: version.into(),
            tenant_id: None,
            tenant_isolation: TenantIsolation::Off,
            classification: None,
            metrics: None,
            #[cfg(feature = "signing")]
            signing: crate::signing::SigningOptions::default(),
            #[cfg(feature = "validation")]
            validation: crate::validation::ValidationOptions::default(),
            schemas: HashMap::new(),
            schema_base_url: None,
            pending_poll: DEFAULT_PENDING_POLL,
            classifier: ClassifierOptions::default(),
            credentials: Credentials::None,
            tls: None,
            traceparent: None,
            on_poison: None,
            on_dlq: None,
            logger: None,
        }
    }

    /// Tenant por defecto de los eventos publicados.
    #[must_use]
    pub fn with_tenant_id(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant.into());
        self
    }

    /// Exige filtro de tenant en toda suscripción — 09-multitenancy.md §3.
    #[must_use]
    pub fn with_tenant_isolation(mut self, isolation: TenantIsolation) -> Self {
        self.tenant_isolation = isolation;
        self
    }

    /// Destino de las métricas — 08-observability.md.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsSink>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Firma Ed25519 de eventos — 07-signing.md.
    #[cfg(feature = "signing")]
    #[must_use]
    pub fn with_signing(mut self, signing: crate::signing::SigningOptions) -> Self {
        self.signing = signing;
        self
    }

    /// Validación L3 del payload contra su JSON Schema — 00-protocol.md §5.
    ///
    /// ```no_run
    /// # fn ejemplo() -> Result<(), flux::FluxError> {
    /// use flux::validation::{SchemaBundle, ValidationOptions};
    ///
    /// let bundle = SchemaBundle::from_file("schemas/bundle.json")?;
    /// let opts = flux::ConnectOptions::new("nats://localhost:4222", "pedidos-api", "produccion", "3.4.1")
    ///     .with_validation(ValidationOptions::strict(bundle));
    /// # let _ = opts;
    /// # Ok(()) }
    /// ```
    #[cfg(feature = "validation")]
    #[must_use]
    pub fn with_validation(mut self, validation: crate::validation::ValidationOptions) -> Self {
        self.validation = validation;
        self
    }

    /// Cada cuánto sondear `num_pending`. [`Duration::ZERO`] lo desactiva
    /// — 08-observability.md §2.3.
    #[must_use]
    pub fn with_pending_poll(mut self, every: Duration) -> Self {
        self.pending_poll = every;
        self
    }

    /// Clasificación por defecto.
    #[must_use]
    pub fn with_classification(mut self, c: DataClassification) -> Self {
        self.classification = Some(c);
        self
    }

    /// Base para derivar `dataschema`.
    #[must_use]
    pub fn with_schema_base_url(mut self, url: impl Into<String>) -> Self {
        self.schema_base_url = Some(url.into());
        self
    }

    /// Declara el `dataschema` exacto de un subject.
    #[must_use]
    pub fn with_schema(mut self, subject: impl Into<String>, uri: impl Into<String>) -> Self {
        self.schemas.insert(subject.into(), uri.into());
        self
    }

    /// Credenciales.
    #[must_use]
    pub fn with_credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = credentials;
        self
    }

    /// TLS: CA propia, certificado de cliente y exigir TLS — 06-security.md §2.
    ///
    /// Los ficheros se comprueban en [`connect`], antes de tocar la red.
    #[must_use]
    pub fn with_tls(mut self, tls: TlsOptions) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Política de clasificación de errores.
    #[must_use]
    pub fn with_classifier(mut self, classifier: ClassifierOptions) -> Self {
        self.classifier = classifier;
        self
    }

    /// Log de la aplicación.
    #[must_use]
    pub fn with_logger(mut self, logger: LogFn) -> Self {
        self.logger = Some(logger);
        self
    }

    /// Extractor de `traceparent`.
    #[must_use]
    pub fn with_traceparent(mut self, f: TraceparentFn) -> Self {
        self.traceparent = Some(f);
        self
    }

    /// Callback de POISON. Debe despertar a alguien.
    #[must_use]
    pub fn on_poison(mut self, f: Arc<dyn Fn(PoisonInfo) + Send + Sync>) -> Self {
        self.on_poison = Some(f);
        self
    }

    /// Callback de enrutado a la DLQ.
    #[must_use]
    pub fn on_dlq(mut self, f: Arc<dyn Fn(DlqEventInfo) + Send + Sync>) -> Self {
        self.on_dlq = Some(f);
        self
    }
}

// ─── Opciones de publicación ─────────────────────────────────────────────────

/// Ajustes de una publicación concreta.
#[derive(Debug, Clone, Default)]
pub struct PublishOptions {
    /// Id del agregado. Va al atributo `subject` de **CloudEvents**, NO al subject de
    /// NATS — 01-envelope.md §2.1.
    pub aggregate_id: Option<String>,
    /// Sobrescribe el tenant para esta publicación.
    pub tenant_id: Option<String>,
    /// Sobrescribe la clasificación para esta publicación.
    pub classification: Option<DataClassification>,
    /// Instante en que **ocurrió** el hecho, si no es "ahora". El `time` de CloudEvents
    /// no es el de publicación — 01-envelope.md §2.
    pub time: Option<DateTime<Utc>>,
    /// Sobrescribe la clave de partición, que por defecto es el `aggregate_id`.
    pub partition_key: Option<String>,
    /// Contexto de correlación explícito. **Gana sobre el task-local**, para publicar
    /// desde un job diferido o un task hijo — ver [`crate::context`].
    pub event_context: Option<EventContext>,
}

impl PublishOptions {
    /// Id del agregado.
    #[must_use]
    pub fn with_aggregate_id(mut self, id: impl Into<String>) -> Self {
        self.aggregate_id = Some(id.into());
        self
    }
    /// Tenant de esta publicación.
    #[must_use]
    pub fn with_tenant_id(mut self, id: impl Into<String>) -> Self {
        self.tenant_id = Some(id.into());
        self
    }
    /// Clasificación de esta publicación.
    #[must_use]
    pub fn with_classification(mut self, c: DataClassification) -> Self {
        self.classification = Some(c);
        self
    }
    /// Instante en que ocurrió el hecho.
    #[must_use]
    pub fn with_time(mut self, t: DateTime<Utc>) -> Self {
        self.time = Some(t);
        self
    }
    /// Clave de partición.
    #[must_use]
    pub fn with_partition_key(mut self, k: impl Into<String>) -> Self {
        self.partition_key = Some(k.into());
        self
    }
    /// Contexto de correlación explícito.
    #[must_use]
    pub fn with_context(mut self, ctx: EventContext) -> Self {
        self.event_context = Some(ctx);
        self
    }
}

// ─── Opciones de suscripción ─────────────────────────────────────────────────

/// Ajustes de una suscripción.
#[derive(Debug, Clone, Default)]
pub struct SubscribeOptions {
    /// Durable explícito. Por defecto se deriva del servicio y el subject.
    pub durable: Option<String>,
    /// Ventana de mensajes sin confirmar. Por defecto [`DEFAULT_MAX_ACK_PENDING`].
    pub max_ack_pending: Option<u32>,
    /// Descarta —con ack— los eventos de otros tenants antes de invocar al handler
    /// — 06-security.md §4.
    pub tenant_filter: Option<String>,
}

impl SubscribeOptions {
    /// Durable explícito.
    #[must_use]
    pub fn with_durable(mut self, durable: impl Into<String>) -> Self {
        self.durable = Some(durable.into());
        self
    }
    /// Ventana de mensajes sin confirmar.
    #[must_use]
    pub fn with_max_ack_pending(mut self, n: u32) -> Self {
        self.max_ack_pending = Some(n);
        self
    }
    /// Filtro de tenant.
    #[must_use]
    pub fn with_tenant_filter(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_filter = Some(tenant.into());
        self
    }
}

// ─── Handler ─────────────────────────────────────────────────────────────────

/// Metadatos de ESTA entrega concreta del evento.
///
/// Van como segundo parámetro y no dentro del evento a propósito: son datos de la
/// entrega, no del hecho, y meterlos en el envelope los haría viajar por el bus.
#[derive(Debug, Clone)]
pub struct Delivery {
    /// Número de entrega, empezando en 1. Si es > 1, este evento **YA** se intentó
    /// procesar antes: la idempotencia no es opcional — 03-delivery.md §4.
    pub attempt: u32,
    /// Techo del consumidor ([`DEFAULT_MAX_DELIVER`]). Al alcanzarlo, el SDK enruta a la
    /// DLQ — antes si la clasificación del error impone un presupuesto menor, cosa que
    /// aquí todavía no se sabe porque el handler aún no ha fallado — 04-errors.md §2.1.
    pub max_attempts: u32,
    /// Subject de NATS por el que llegó el evento.
    pub subject: String,
    /// Nombre del consumidor. Aparece como `dlqconsumer` en la DLQ.
    pub durable: String,
}

/// Procesa un evento.
///
/// Devolver `Ok(())` **ACK-ea** el evento: es el ack explícito del protocolo, y el SDK
/// jamás confirma un mensaje antes de que el handler termine. Devolver `Err` lo clasifica
/// según [`ClassifierOptions`] y produce `nak`, `term` o `term`+alerta.
///
/// El handler **DEBE** ser idempotente. La garantía es *at-least-once*: los duplicados
/// llegan, no son un fallo — 03-delivery.md §1.
///
/// Se implementa automáticamente para cualquier `Fn(Event, Delivery) -> Future`, así que
/// en la práctica se escribe como un closure.
pub trait Handler: Send + Sync + 'static {
    /// Invoca el handler.
    fn call(
        &self,
        event: Event,
        delivery: Delivery,
    ) -> std::pin::Pin<Box<dyn Future<Output = HandlerResult> + Send>>;
}

impl<F, Fut> Handler for F
where
    F: Fn(Event, Delivery) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = HandlerResult> + Send + 'static,
{
    fn call(
        &self,
        event: Event,
        delivery: Delivery,
    ) -> std::pin::Pin<Box<dyn Future<Output = HandlerResult> + Send>> {
        Box::pin(self(event, delivery))
    }
}

/// Una suscripción viva.
///
/// Al soltarse **no** se cancela la entrega: eso convertiría un `let _ = bus.subscribe(…)`
/// en un consumidor que muere sin decir nada. Para parar, [`Subscription::unsubscribe`]
/// o [`Bus::close`].
#[derive(Debug)]
pub struct Subscription {
    /// Subject de NATS suscrito.
    pub subject: String,
    /// Durable del consumidor.
    pub durable: String,
    stop: watch::Sender<bool>,
}

impl Subscription {
    /// Detiene la entrega.
    ///
    /// El mensaje en curso termina de procesarse; los que ya estén en el búfer se
    /// descartan y **se reentregarán**. Eso es correcto, y es exactamente el caso que
    /// cubre la idempotencia — 03-delivery.md §4.
    pub fn unsubscribe(&self) {
        let _ = self.stop.send(true);
    }
}

// ─── Bus ─────────────────────────────────────────────────────────────────────

// Los campos son `pub(crate)` porque el estado replicado ([`crate::state`]) es un módulo
// hermano que opera sobre el mismo bus —su conexión, sus denegaciones, su clasificador y sus
// métricas—; nada de esto es API pública.
pub(crate) struct BusInner {
    pub(crate) client: async_nats::Client,
    pub(crate) js: async_nats::jetstream::Context,
    pub(crate) opts: ConnectOptions,
    pub(crate) classifier: Classifier,
    pub(crate) source: String,
    pub(crate) metrics: Arc<dyn MetricsSink>,
    #[cfg(feature = "signing")]
    pub(crate) signer: Option<crate::signing::Signer>,
    #[cfg(feature = "signing")]
    pub(crate) verifier: Option<crate::signing::Verifier>,
    /// `None` cuando el modo es `Off`: en L2 no se compila ni un esquema.
    #[cfg(feature = "validation")]
    pub(crate) validator: Option<crate::validation::Validator>,
    ensured: Mutex<HashSet<String>>,
    pub(crate) stops: Mutex<Vec<watch::Sender<bool>>>,
    /// Denegaciones de permisos que el broker comunica por la conexión. Ver
    /// [`crate::security`]: sin esto, un permiso que falta es un timeout mudo.
    pub(crate) denials: Arc<DenialLog>,
    /// `_INBOX.<servicio>`. Siempre; no es una opción — 06-security.md §3.
    pub(crate) inbox_prefix: String,
    /// `flux_consumer_pending` de los `watch` de estado: la suma de sus durables — 10-state.md §9.
    pub(crate) state_pending: crate::state::PendingSum,
    /// Los responders de petición/respuesta vivos: `close()` los para, en orden, antes de cerrar —
    /// 11-request-reply.md §5.5.
    pub(crate) rpc: crate::rpc::RpcRegistry,
}

/// El cliente de flux: publica y consume eventos.
///
/// Es barato de clonar (un `Arc` por dentro) y seguro de usar desde varios tasks, así que
/// se pasa por valor a los handlers que necesiten publicar.
#[derive(Clone)]
pub struct Bus {
    pub(crate) inner: Arc<BusInner>,
}

impl fmt::Debug for Bus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bus")
            .field("source", &self.inner.source)
            .field("connected", &self.is_connected())
            .finish_non_exhaustive()
    }
}

/// Toma un mutex tolerando el envenenamiento.
///
/// Un `Mutex` de Rust queda "envenenado" si un hilo entra en pánico teniéndolo cogido, y
/// `lock().unwrap()` propagaría ese pánico a todos los demás. Las dos secciones críticas
/// de este fichero son un `HashSet::insert` y un `Vec::take` —no pueden entrar en
/// pánico— así que recuperar el valor es siempre correcto y evita que un bug en otro
/// sitio del proceso tumbe el bus entero.
pub(crate) fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(crate) fn transport(context: impl Into<String>, source: impl Into<HandlerError>) -> FluxError {
    FluxError::Transport {
        context: context.into(),
        source: source.into(),
    }
}

/// Abre la conexión al bus.
///
/// Espera al broker con backoff **y jitter** —un servicio que arranca antes que el broker no
/// debe morir en el primer intento; sin jitter, mil servicios reconectan en el mismo
/// milisegundo y tiran el cluster que acaba de levantarse (03-delivery.md §6)— y, una vez
/// conectado, reconecta indefinidamente. Lo que reintentar no arregla (credencial rechazada,
/// TLS que no encaja) **no se reintenta**: devuelve el error aquí.
///
/// El inbox es siempre `_INBOX.<servicio>` (06-security.md §3): no es una opción.
///
/// # Errores
///
/// Los tres códigos estables de la conexión inicial (04-errors.md §5.3), todos PERMANENT:
///
/// - [`FluxError::InvalidCredentials`] (`INVALID_CREDENTIALS`) si un valor de [`Credentials`]
///   está mal formado: vacío, un `.creds` ilegible o sin JWT y semilla, una semilla que no es
///   `SU…`. Se comprueba **antes** de tocar la red.
/// - [`FluxError::InvalidTlsOptions`] (`INVALID_TLS_OPTIONS`) si [`TlsOptions`] está mal
///   formado: un fichero ilegible o que no es PEM, o un certificado de cliente sin su clave.
///   También **antes** de tocar la red.
/// - [`FluxError::ConnectionRejected`] (`CONNECTION_REJECTED`) si el broker **sí** contestó y
///   rechazó la conexión por algo que esperar no arregla: la credencial
///   ([`crate::RejectionReason::Auth`]) o el handshake TLS ([`crate::RejectionReason::Tls`]: el certificado
///   no verifica, o se exigió TLS y el broker no lo ofrece). Un broker que no ha arrancado
///   **no** es esto: se espera.
///
/// Además, [`FluxError::Config`] si falta alguno de los cuatro campos obligatorios,
/// [`FluxError::InvalidServiceName`] si el servicio no es kebab-case —lo que produciría
/// durables no parseables— y [`FluxError::Transport`] si la lista de servidores no se
/// entiende (una dirección que no es `nats://host:4222`), o [`FluxError::Config`] si está vacía.
pub async fn connect(mut opts: ConnectOptions) -> Result<Bus, FluxError> {
    if opts.service.is_empty() || opts.environment.is_empty() || opts.version.is_empty() {
        return Err(FluxError::Config(
            "service, environment y version son obligatorios: alimentan `source` y \
             `producerversion`, que son atributos obligatorios del envelope"
                .to_string(),
        ));
    }
    // protocol.json → naming.service. NATS aceptaría `FacturacionAPI__pedidos_…` sin
    // error y el incumplimiento solo se descubriría al parsear nombres en una
    // herramienta de operación, así que se valida aquí y no al crear el durable.
    validate_service_name(&opts.service)?;

    let servers = require_servers(&opts.servers)?;

    // El firmante y el verificador se construyen ANTES de tocar la red: una clave mal
    // configurada es un fallo de arranque, no algo que descubrir con el primer evento
    // — 07-signing.md §7.
    #[cfg(feature = "signing")]
    let (signer, verifier) = (
        crate::signing::Signer::new(&opts.signing)?,
        crate::signing::Verifier::new(&opts.signing, opts.logger.clone())?,
    );

    // Los esquemas se compilan UNA vez aquí, no por evento: compilar un JSON Schema es
    // caro y hacerlo en la ruta caliente tiraría el throughput. Y como el signer, es un
    // fallo de ARRANQUE: un bundle roto descubierto con el primer publish sería un
    // servicio que pasa el healthcheck y no puede publicar.
    #[cfg(feature = "validation")]
    let validator = crate::validation::Validator::new(&opts.validation, opts.logger.clone())?;

    let denials = Arc::new(DenialLog::new());
    let inbox = inbox_prefix(&opts.service);

    let mut nats = async_nats::ConnectOptions::new()
        .name(format!("{}@{}", opts.service, opts.environment))
        // -1 en Go, None aquí: reconexión indefinida.
        .max_reconnects(None)
        // Inbox PRIVADO del servicio, siempre — 06-security.md §3. async-nats lo usa para
        // TODO lo que viaja por un inbox: las peticiones a la API de JetStream (la
        // multiplexión de `request`), las entregas de los consumidores pull y el acuse de
        // cada publicación. Los acks, en cambio, son un publish a `$JS.ACK.…` y no pasan
        // por él. Con el inbox común `_INBOX.>` cualquiera con permiso de suscripción lee
        // las entregas de los demás (plan v1.1, evidencia A-2).
        .custom_inbox_prefix(inbox.clone())
        // El broker no devuelve un error cuando una ACL deniega algo: descarta el mensaje y
        // manda un `-ERR` aparte. Sin escucharlo, el síntoma es un timeout de 5-30 s.
        .event_callback({
            let denials = Arc::clone(&denials);
            let logger = opts.logger.clone();
            move |event| {
                let denials = Arc::clone(&denials);
                let logger = logger.clone();
                async move {
                    let async_nats::Event::ServerError(async_nats::ServerError::Other(text)) =
                        event
                    else {
                        return;
                    };
                    if let Some(denial) = parse_violation(&text) {
                        if let Some(log) = &logger {
                            let pista = denial_hint(&denial);
                            log(
                                LogLevel::Error,
                                &format!(
                                    "[flux] el broker denegó {}: la ACL del servicio no lo \
                                     concede (06-security.md §3).{pista}",
                                    denial.describe()
                                ),
                            );
                        }
                        denials.record(denial);
                    }
                }
            }
        });

    nats = apply_credentials(nats, &opts.credentials).await?;

    if let Some(tls) = &opts.tls {
        tls.validate()?;
        nats = nats.require_tls(tls.required);
        if let Some(ca) = &tls.ca_file {
            nats = nats.add_root_certificates(ca.clone());
        }
        if let (Some(cert), Some(key)) = (&tls.client_cert, &tls.client_key) {
            nats = nats.add_client_certificate(cert.clone(), key.clone());
        }
    }

    let client = open_connection(&nats, &opts, &servers).await?;

    let js = async_nats::jetstream::new(client.clone());
    let source = source_uri(&opts.environment, &opts.service);

    // El clasificador se construye una sola vez, al conectar: la política no debe poder
    // cambiar bajo los pies de un consumidor en marcha.
    let classifier = Classifier::new(std::mem::take(&mut opts.classifier));

    let metrics: Arc<dyn MetricsSink> = opts
        .metrics
        .clone()
        .unwrap_or_else(|| Arc::new(NoMetrics) as Arc<dyn MetricsSink>);
    metrics.connection_state(ConnectionState::Connected);

    Ok(Bus {
        inner: Arc::new(BusInner {
            client,
            js,
            opts,
            classifier,
            source,
            metrics,
            #[cfg(feature = "signing")]
            signer,
            #[cfg(feature = "signing")]
            verifier,
            #[cfg(feature = "validation")]
            validator,
            ensured: Mutex::new(HashSet::new()),
            stops: Mutex::new(Vec::new()),
            denials,
            inbox_prefix: inbox,
            state_pending: crate::state::PendingSum::default(),
            rpc: crate::rpc::RpcRegistry::default(),
        }),
    })
}

/// Traduce [`Credentials`] a las del cliente de NATS, validando antes de tocar la red.
async fn apply_credentials(
    nats: async_nats::ConnectOptions,
    credentials: &Credentials,
) -> Result<async_nats::ConnectOptions, FluxError> {
    Ok(match credentials {
        Credentials::None => nats,
        Credentials::CredsFile(path) => {
            // `credentials_file` lee Y parsea (JWT + semilla): un fichero ilegible o sin
            // las dos secciones falla aquí. Es un error de CONFIGURACIÓN, no de transporte.
            nats.credentials_file(path).await.map_err(|e| {
                FluxError::InvalidCredentials(format!(
                    "el fichero .creds `{}` no se puede usar: {e}. Debe llevar el JWT de \
                     usuario y la semilla `SU…`",
                    path.display()
                ))
            })?
        }
        Credentials::NkeySeed(seed) => {
            let seed = seed.trim();
            validate_nkey_seed(seed)?;
            nats.nkey(seed.to_string())
        }
        Credentials::NkeyFile(path) => {
            let seed = std::fs::read_to_string(path).map_err(|e| {
                FluxError::InvalidCredentials(format!(
                    "no se puede leer el fichero de semilla nkey `{}`: {e}",
                    path.display()
                ))
            })?;
            let seed = seed.trim();
            validate_nkey_seed(seed)?;
            nats.nkey(seed.to_string())
        }
        Credentials::Token(t) => {
            not_blank("el token", t)?;
            nats.token(t.clone())
        }
        Credentials::UserPassword { user, password } => {
            not_blank("el usuario", user)?;
            not_blank("la contraseña", password)?;
            nats.user_and_password(user.clone(), password.clone())
        }
    })
}

/// Un valor de credencial vacío no es «sin credencial»: es un descuido —una variable de
/// entorno sin definir— que el broker rechazaría después con un mensaje que no lo dice. Misma
/// regla que en el SDK de Node (`credentials.<campo>` debe ser un texto no vacío). Nunca se
/// vuelca el valor: acaba en un log.
fn not_blank(what: &str, value: &str) -> Result<(), FluxError> {
    if value.trim().is_empty() {
        return Err(FluxError::InvalidCredentials(format!(
            "{what} está vacío: una credencial en blanco suele ser una variable de entorno \
             sin definir. Si no quieres autenticarte, usa `Credentials::None`"
        )));
    }
    Ok(())
}

/// Abre la conexión inicial: espera al broker, pero no reintenta lo que no se arregla.
///
/// `async-nats` ofrece `retry_on_initial_connect`, y es lo que este SDK usaba: `connect()`
/// vuelve al instante y el cliente sigue intentándolo en segundo plano, **para siempre y sin
/// distinguir causas**. Una contraseña equivocada, una semilla rechazada o una CA que no
/// encaja se convierten en un servicio que arranca, pasa el healthcheck y se queda mudo: la
/// primera publicación espera 5-30 s y falla con un timeout que no dice nada.
///
/// Aquí el reintento es propio y filtra: el broker que aún no ha arrancado se espera con
/// backoff y jitter —03-delivery.md §6— y lo que reintentar no arregla se devuelve al
/// instante ([`fatal_connect_kind`]), con su código estable (04-errors.md §5.3).
async fn open_connection(
    nats: &async_nats::ConnectOptions,
    opts: &ConnectOptions,
    servers: &[String],
) -> Result<async_nats::Client, FluxError> {
    let mut attempt: u32 = 0;
    loop {
        let err = match nats.clone().connect(servers.to_vec()).await {
            Ok(client) => return Ok(client),
            Err(e) => e,
        };
        if let Some(fatal) = fatal_connect_kind(&err) {
            return Err(fatal_connect_error(opts, fatal, err));
        }
        attempt = attempt.saturating_add(1);
        let wait = connect_backoff(attempt);
        if let Some(log) = &opts.logger {
            log(
                LogLevel::Warn,
                &format!(
                    "[flux] el broker `{}` aún no responde ({err}). Reintento en {wait:?}",
                    opts.servers
                ),
            );
        }
        tokio::time::sleep(wait).await;
    }
}

/// 100 ms, 200 ms, … hasta 5 s, más hasta 250 ms de jitter: sin él, mil servicios que
/// arrancan a la vez reintentan en el mismo milisegundo y tiran el broker que acaba de
/// levantarse — 03-delivery.md §6.
pub(crate) fn connect_backoff(attempt: u32) -> Duration {
    let exp = attempt.saturating_sub(1).min(6);
    let base = Duration::from_millis(100u64 << exp).min(Duration::from_secs(5));
    // Los bits bajos de un UUIDv7 son aleatorios: no hace falta una dependencia más para
    // desincronizar reintentos.
    let jitter = u64::try_from(Uuid::now_v7().as_u128() % 250).unwrap_or(0);
    base + Duration::from_millis(jitter)
}

/// El error de `connect()` para un fallo que reintentar no arregla, con la causa dicha.
///
/// Un rechazo del broker (credencial o TLS) es [`FluxError::ConnectionRejected`], con su
/// código estable. Una lista de servidores que no se entiende no es un rechazo del broker y
/// sigue siendo un fallo de transporte sin código.
fn fatal_connect_error(
    opts: &ConnectOptions,
    fatal: FatalConnect,
    err: async_nats::ConnectError,
) -> FluxError {
    match fatal {
        FatalConnect::Rejected(reason) => FluxError::ConnectionRejected {
            reason,
            servers: opts.servers.clone(),
            detail: connect_error_detail(&err),
            source: Box::new(err),
        },
        FatalConnect::BadServers => transport(
            format!(
                "no se pudo conectar a `{}`: la lista de servidores no se entiende \
                 (`nats://host:4222`, o varias direcciones separadas por coma)",
                opts.servers
            ),
            err,
        ),
    }
}

impl Bus {
    /// Informa si la conexión sigue viva. Expuesto para el healthcheck que exige
    /// 03-delivery.md §6.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        matches!(
            self.inner.client.connection_state(),
            async_nats::connection::State::Connected
        )
    }

    /// El `source` que este bus pone en cada evento: `/<entorno>/<servicio>`.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.inner.source
    }

    /// Detiene las suscripciones y drena la conexión.
    ///
    /// Drena en vez de cerrar: los acks pendientes no se pierden en silencio. Los **responders** de
    /// petición/respuesta se paran antes —dejar de escuchar y esperar a lo que está en vuelo, hasta
    /// 25 s—: cerrar la conexión de golpe pierde la petición que se atiende.
    ///
    /// # Errores
    ///
    /// [`FluxError::Transport`] si el drenado falla.
    pub async fn close(&self) -> Result<(), FluxError> {
        let stops: Vec<_> = {
            let mut guard = lock(&self.inner.stops);
            std::mem::take(&mut *guard)
        };
        for s in stops {
            let _ = s.send(true);
        }
        // Los que responden peticiones, ANTES de cerrar: dejar de escuchar, esperar a lo que está en
        // vuelo y solo entonces drenar. Cerrar de golpe pierde lo que se atiende (A-62, 11-request-reply.md §5.5).
        self.inner.rpc.stop_all().await;
        self.inner
            .metrics
            .connection_state(ConnectionState::Disconnected);
        self.inner
            .client
            .drain()
            .await
            .map_err(|e| transport("fallo al drenar la conexión", e))
    }

    /// El destino de métricas configurado. Útil para servir `/metrics` desde el healthcheck.
    #[must_use]
    pub fn metrics(&self) -> &Arc<dyn MetricsSink> {
        &self.inner.metrics
    }

    /// El **estado replicado**: `put`, `delete`, `get` y `watch` — extensión opcional de v1.1,
    /// [`crate::state`] y `specification/10-state.md`.
    ///
    /// Es barato (un `Arc` clonado) y no abre nada: la conexión es la del bus.
    #[must_use]
    pub fn state(&self) -> crate::state::State {
        crate::state::State::new(self.clone())
    }

    pub(crate) fn log(&self, level: LogLevel, msg: &str) {
        if let Some(logger) = &self.inner.opts.logger {
            logger(level, &format!("[flux] {msg}"));
        }
    }

    // ─── publish ─────────────────────────────────────────────────────────────

    /// Construye el envelope y publica el evento.
    ///
    /// **El desarrollador solo escribe subject y `data`** (y opcionalmente el
    /// `aggregate_id`, vía [`Bus::publish_with`]). Todo lo demás —`id`, `source`, `time`,
    /// `specversion`, `type`, `dataschema`, `correlationid`, `causationid`,
    /// `producerversion`, `traceparent`— lo rellena el SDK. Si tu código asigna alguno de
    /// esos a mano, está mal — 01-envelope.md §5.
    ///
    /// # Errores
    ///
    /// Ver [`Bus::publish_with`].
    pub async fn publish<T: Serialize + ?Sized>(
        &self,
        subject: &str,
        data: &T,
    ) -> Result<Event, FluxError> {
        self.publish_with(subject, data, PublishOptions::default())
            .await
    }

    /// Como [`Bus::publish`], con ajustes para esta publicación concreta.
    ///
    /// # Errores
    ///
    /// [`FluxError::InvalidSubject`] si el subject no cumple el naming (falla **antes**
    /// de tocar la red), [`FluxError::Envelope`] si `data` no es un objeto JSON o el
    /// mensaje supera 1 MiB, [`FluxError::Config`] si no hay `dataschema` para el
    /// subject, y [`FluxError::Transport`] si el broker rechaza la publicación.
    pub async fn publish_with<T: Serialize + ?Sized>(
        &self,
        subject: &str,
        data: &T,
        opts: PublishOptions,
    ) -> Result<Event, FluxError> {
        // Los dominios reservados van ANTES que el formato: `dlq.pedidos.pedido.v1.creado` tiene
        // 5 tokens y diría «debe tener 4», que es cierto y no ayuda. Y un `publish()` a `state.…`
        // no es un subject mal formado sino uno que llegaría al stream de estado SIN el chequeo
        // de secuencia y sobrescribiría sin condición (A-45) — 10-state.md §2.7.
        crate::state::reject_reserved_domain(subject)?;
        // Falla temprano y con un mensaje útil: un subject con una mayúscula es un
        // subject fantasma — 02-naming.md §1.1.
        let parsed = parse_subject(subject)?;
        let raw = data_to_raw(data)?;

        self.ensure_stream(&parsed).await?;
        let dataschema = self.schema_for(subject, &parsed)?;

        // UUIDv7: monotónico en el tiempo, así que ordenar por `id` dentro de un mismo
        // `source` equivale a ordenar por instante de generación — útil al reconstruir
        // historiales desde la DLQ (01-envelope.md §2.6).
        let event_id = Uuid::now_v7().to_string();

        // El contexto explícito gana sobre el task-local: un job diferido sabe más que
        // el task en el que le tocó ejecutarse.
        let inherited = opts.event_context.or_else(context::current);

        let tenantid = opts
            .tenant_id
            .or_else(|| {
                // El contexto heredado gana sobre el default de la conexión: un evento
                // derivado pertenece al tenant del evento que lo causó, no al del
                // servicio.
                inherited
                    .as_ref()
                    .map(|c| c.tenant_id.clone())
                    .filter(|t| !t.is_empty())
            })
            .or_else(|| self.inner.opts.tenant_id.clone())
            .unwrap_or_else(|| "system".to_string());

        let dataclassification = opts
            .classification
            .or(self.inner.opts.classification)
            .unwrap_or(DataClassification::Internal);

        // Si el evento no nace de otro, correlationid se inicializa con su propio id
        // — 01-envelope.md §3.1.
        let (correlationid, causationid) = match inherited.as_ref() {
            Some(c) if !c.correlation_id.is_empty() => {
                (c.correlation_id.clone(), Some(c.causation_id.clone()))
            }
            _ => (event_id.clone(), None),
        };

        let (traceparent, tracestate) = match inherited.as_ref() {
            Some(c) if c.traceparent.is_some() => (c.traceparent.clone(), c.tracestate.clone()),
            _ => (self.inner.opts.traceparent.as_ref().and_then(|f| f()), None),
        };

        let event = build_event(BuildEventInput {
            subject: subject.to_string(),
            data: raw,
            id: event_id,
            source: self.inner.source.clone(),
            producerversion: self.inner.opts.version.clone(),
            tenantid,
            dataclassification,
            dataschema,
            correlationid,
            time: opts.time,
            aggregate_id: opts.aggregate_id,
            causationid,
            partitionkey: opts.partition_key,
            traceparent,
            tracestate,
        })?;

        // L3: validar ANTES de publicar. Un payload que viola su contrato debe fallar aquí,
        // en el servicio que lo generó, y no aparecer como un misterio en un consumidor de
        // otro equipo la semana que viene — 00-protocol.md §5.
        //
        // Va antes de firmar porque la firma es lo último antes de serializar, y porque
        // firmar un evento que se va a rechazar es trabajo tirado.
        #[cfg(feature = "validation")]
        if let Some(validator) = &self.inner.validator {
            if let Err(e) = validator.validate(&event, subject) {
                // `invalid_schema` y no `error`: "el broker rechazó la publicación" y "el
                // payload incumple su contrato" son dos incidentes distintos con dos
                // dueños distintos — 08-observability.md §2.1.
                self.inner
                    .metrics
                    .event_published(subject, PublishOutcome::InvalidSchema);
                return Err(e);
            }
        }

        // Firmar es LO ÚLTIMO antes de serializar: la firma cubre el envelope completo, así
        // que cualquier atributo añadido después la invalidaría — 07-signing.md §5.
        #[cfg(feature = "signing")]
        let event = match &self.inner.signer {
            Some(signer) => signer.sign(event)?,
            None => event,
        };

        let payload = match serialize(&event) {
            Ok(p) => p,
            Err(e) => {
                self.inner
                    .metrics
                    .event_published(subject, PublishOutcome::Error);
                return Err(e);
            }
        };
        if let Err(e) = self.publish_raw(subject, payload, &event.id).await {
            self.inner
                .metrics
                .event_published(subject, PublishOutcome::Error);
            return Err(e);
        }
        self.inner
            .metrics
            .event_published(subject, PublishOutcome::Ok);
        Ok(event)
    }

    /// Publica por **JetStream** y espera el acuse.
    ///
    /// Esperar el ack no es opcional: un `publish` de **core** NATS a un subject que
    /// ningún stream captura **no da ningún error** y el evento se evapora sin rastro.
    /// El de JetStream sí falla, porque espera el acuse del stream, y ésa es la única red
    /// de seguridad automática contra una errata de subject — 02-naming.md §1.1.
    ///
    /// `message_id` pone la cabecera `Nats-Msg-Id`. Deduplica reintentos de
    /// **PUBLICACIÓN** dentro de `duplicate_window`: un publish que no recibe el ACK del
    /// broker por un corte de red y se reintenta no deja dos copias en el stream. **NO**
    /// deduplica reentregas de consumo: un `nak` reentrega el mismo mensaje con el mismo
    /// `Nats-Msg-Id` y eso es correcto y deseado — 03-delivery.md §3.
    pub(crate) async fn publish_raw(
        &self,
        subject: &str,
        payload: Vec<u8>,
        message_id: &str,
    ) -> Result<(), FluxError> {
        // Sin permiso de publicación sobre `subject` el broker descarta el mensaje y el
        // acuse nunca llega: el síntoma sería un timeout de hasta 30 s. La carrera con la
        // denegación lo convierte en un error que nombra el subject y la regla.
        let sent = self
            .guarded(&[subject.to_string()], async {
                let ack = self
                    .inner
                    .js
                    .send_publish(
                        subject.to_string(),
                        PublishMessage::build()
                            .payload(payload.into())
                            .message_id(message_id),
                    )
                    .await
                    .map_err(|e| transport(format!("fallo al publicar en `{subject}`"), e))?;
                ack.await.map_err(|e| {
                    transport(
                        format!("el stream no confirmó la publicación en `{subject}`"),
                        e,
                    )
                })?;
                Ok::<(), FluxError>(())
            })
            .await;
        match sent {
            Ok(result) => result,
            Err(denial) => Err(permission_denied(
                format!(
                    "publicar en `{subject}`: solo el servicio dueño del dominio publica en él \
                     y `dlq.<subject>` solo en lo que consume"
                ),
                &denial,
            )),
        }
    }

    /// Resuelve la URI de `dataschema` del subject.
    fn schema_for(&self, subject: &str, p: &ParsedSubject) -> Result<String, FluxError> {
        if let Some(s) = self
            .inner
            .opts
            .schemas
            .get(subject)
            .filter(|s| !s.is_empty())
        {
            return Ok(s.clone());
        }

        // El bundle L3 conoce el MINOR real de cada subject: dentro de un mayor todo es
        // BACKWARD-compatible, así que el más alto acepta lo que aceptan los anteriores
        // — 00-protocol.md §5. Se consulta ANTES de derivar la URI a mano porque derivarla
        // es adivinar, y adivinar mal aquí significa validar contra el esquema equivocado.
        #[cfg(feature = "validation")]
        if let Some(uri) = self
            .inner
            .opts
            .validation
            .bundle
            .as_ref()
            .and_then(|b| b.uri_for(subject))
        {
            return Ok(uri.to_string());
        }

        let Some(base) = &self.inner.opts.schema_base_url else {
            return Err(FluxError::Config(format!(
                "no hay dataschema para `{subject}`. Declara schemas[\"{subject}\"], \
                 schema_base_url, o pasa un bundle en ConnectOptions::with_validation"
            )));
        };
        // Sin bundle ni mapa explícito solo se puede asumir el `.0.0` del mayor. Es
        // suficiente para L2 —el atributo es informativo— pero no para L3.
        Ok(format!(
            "{}/{}/{}/{}/{}.0.0.json",
            base.trim_end_matches('/'),
            p.domain,
            p.aggregate,
            p.event,
            p.major
        ))
    }

    // ─── subscribe ───────────────────────────────────────────────────────────

    /// Crea (o actualiza) el durable consumer canónico y empieza a entregar.
    ///
    /// # Errores
    ///
    /// Ver [`Bus::subscribe_with`].
    pub async fn subscribe<H: Handler>(
        &self,
        subject: &str,
        handler: H,
    ) -> Result<Subscription, FluxError> {
        self.subscribe_with(subject, handler, SubscribeOptions::default())
            .await
    }

    /// Como [`Bus::subscribe`], con ajustes de consumidor.
    ///
    /// # Errores
    ///
    /// [`FluxError::InvalidSubject`], [`FluxError::Transport`] si no se puede crear el
    /// consumidor, y sobre todo [`FluxError::ConsumerConfigMismatch`] si el servidor
    /// aplicó una configuración distinta de la solicitada — requisito **L2**, y la única
    /// defensa contra la sobrescritura silenciosa de `ack_wait` (03-delivery.md §2.1).
    ///
    /// [`FluxError::PermissionDenied`] (`PERMISSION_DENIED`) si la ACL del servicio no
    /// concede alguno de los permisos que hacen falta, **incluido el de pedir mensajes del
    /// consumidor** (`$JS.API.CONSUMER.MSG.NEXT.<stream>.<durable>`). Ese último es el
    /// peligroso: el broker no contesta a un pull denegado, así que sin esta comprobación
    /// `subscribe()` devolvería `Ok` y el consumidor se quedaría inerte, sin un solo error
    /// (06-security.md §3.7). Un permiso que se pierda **después** de suscribirse ya no puede
    /// devolverse como `Result`; solo lo registra el `logger`.
    pub async fn subscribe_with<H: Handler>(
        &self,
        subject: &str,
        handler: H,
        opts: SubscribeOptions,
    ) -> Result<Subscription, FluxError> {
        // `subscribe` también: un `EVT_STATE` no puede existir junto a `STATE_CLIENTES`
        // (A-43), y suscribirse a `state.…` por la vía de los eventos se saltaría la foto, la
        // lápida y la comprobación de identidad del `watch` — 10-state.md §2.7.
        crate::state::reject_reserved_domain(subject)?;
        let parsed = parse_subject(subject)?;

        // El filtro efectivo se resuelve ANTES de crear nada: si `strict` no encuentra
        // tenant, no tiene sentido dejar un durable consumer huérfano en el servidor.
        let tenant_filter = effective_tenant_filter(
            opts.tenant_filter.as_deref(),
            self.inner.opts.tenant_id.as_deref(),
        );
        if self.inner.opts.tenant_isolation == TenantIsolation::Strict && tenant_filter.is_none() {
            return Err(FluxError::TenantIsolation {
                subject: subject.to_string(),
                reason: tenant_isolation_reason(self.inner.opts.tenant_id.as_deref()).to_string(),
            });
        }

        self.ensure_stream(&parsed).await?;
        self.ensure_dlq_stream(&parsed).await?;

        let durable = match opts.durable.clone() {
            Some(d) => d,
            None => durable_name(&self.inner.opts.service, subject)?,
        };
        let max_ack_pending = opts.max_ack_pending.unwrap_or(DEFAULT_MAX_ACK_PENDING);

        let requested = consumer::pull::Config {
            durable_name: Some(durable.clone()),
            filter_subject: subject.to_string(),
            ack_policy: AckPolicy::Explicit,
            // ack_wait DEBE ser backoff[0]: JetStream lo sobrescribe con ese valor sin
            // avisar — 03-delivery.md §2.1.
            ack_wait: DEFAULT_ACK_WAIT,
            max_deliver: i64::from(DEFAULT_MAX_DELIVER),
            backoff: CANONICAL_BACKOFF.to_vec(),
            max_ack_pending: i64::from(max_ack_pending),
            deliver_policy: DeliverPolicy::All,
            replay_policy: ReplayPolicy::Instant,
            ..Default::default()
        };

        let stream_name = stream_name(&parsed.domain);
        let js_stream = self.open_stream(&stream_name).await?;
        let consumer = self
            .obtain_consumer(&js_stream, &stream_name, &durable, requested.clone())
            .await?;

        // El servidor no siempre devuelve lo que se le pide. Requisito L2.
        assert_config_honored(&durable, &requested, &consumer.cached_info().config)?;

        // Sin permiso de pull el consumidor queda inerte y PARECE sano: es el peor modo de
        // fallo, y se descubriría semanas después por la ausencia de eventos. 06-security.md
        // §3.7 manda fallar aquí, antes de arrancar nada.
        self.ensure_pull_allowed(&stream_name, &durable).await?;

        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| transport(format!("no se pudo iniciar el consumo de `{durable}`"), e))?;

        let (stop_tx, mut stop_rx) = watch::channel(false);
        lock(&self.inner.stops).push(stop_tx.clone());

        self.spawn_pending_poll(&js_stream, subject, &durable, &stop_tx);

        let bus = self.clone();
        let handler: Arc<dyn Handler> = Arc::new(handler);
        let sub_subject = subject.to_string();
        let sub_durable = durable.clone();

        tokio::spawn(async move {
            loop {
                let msg = tokio::select! {
                    _ = stop_rx.changed() => break,
                    next = messages.next() => match next {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            // Un error del stream de mensajes NO debe romper el bucle:
                            // async-nats reconecta por dentro y aquí solo llegan cosas
                            // como un heartbeat perdido. Salir dejaría un consumidor
                            // muerto con is_connected() == true, que es peor que uno
                            // que se cae.
                            bus.log(LogLevel::Error, &format!(
                                "error leyendo de `{sub_durable}`: {e}. El consumo continúa."
                            ));
                            continue;
                        }
                        None => break,
                    },
                };

                // Sin este catch_unwind, un pánico FUERA del handler —por ejemplo al
                // serializar el evento de DLQ— rompería el bucle y el consumidor dejaría
                // de consumir EN SILENCIO. Es el bug que el SDK de Node tuvo con el
                // `for await`, portado a la forma que toma en Rust.
                let dispatched = AssertUnwindSafe(bus.dispatch(
                    msg,
                    &sub_subject,
                    &sub_durable,
                    handler.as_ref(),
                    tenant_filter.as_deref(),
                ))
                .catch_unwind()
                .await;

                if dispatched.is_err() {
                    bus.log(
                        LogLevel::Error,
                        &format!(
                            "pánico despachando `{sub_subject}`. El mensaje no se confirma \
                             y será reentregado; el consumo continúa."
                        ),
                    );
                }
            }
        });

        Ok(Subscription {
            subject: subject.to_string(),
            durable,
            stop: stop_tx,
        })
    }

    /// Arranca el sondeo periódico de `num_pending` de este consumidor.
    ///
    /// **La segunda fuente obligatoria de `flux_consumer_pending`** — 08-observability.md
    /// §2.3. La primera son los metadatos de cada entrega, que son gratis y más frescos,
    /// pero que fallan exactamente en el caso que la métrica existe para detectar: si el
    /// bucle del consumidor muere, dejan de entregarse mensajes, así que el gauge se queda
    /// **plano en su último valor** en vez de crecer. Un panel dibuja entonces una línea
    /// horizontal, que es indistinguible de "no pasa nada", mientras la conexión sigue
    /// reportándose sana y el healthcheck dice que todo va bien.
    ///
    /// El sondeo sigue corriendo y reporta el `num_pending` creciente. Ésa es la señal.
    ///
    /// Se salta si no hay destino de métricas —la petición no le serviría a nadie— o si el
    /// intervalo es cero.
    fn spawn_pending_poll(
        &self,
        js_stream: &stream::Stream,
        subject: &str,
        durable: &str,
        stop_tx: &watch::Sender<bool>,
    ) {
        let every = self.inner.opts.pending_poll;
        if every.is_zero() || self.inner.opts.metrics.is_none() {
            return;
        }

        let stream = js_stream.clone();
        let metrics = Arc::clone(&self.inner.metrics);
        let mut stop = stop_tx.subscribe();
        let subject = subject.to_string();
        let durable = durable.to_string();

        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.tick().await; // el primer tick de un interval es inmediato

            loop {
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = tick.tick() => {
                        // Un fallo del sondeo **NO DEBE** afectar al consumo: esto es
                        // telemetría, y una petición perdida se recupera sola en el
                        // siguiente tick. Ni se propaga ni se registra: un broker con hipos
                        // llenaría el log de ruido y escondería los fallos de verdad.
                        if let Ok(info) = stream.consumer_info(&durable).await {
                            metrics.consumer_pending(&subject, &durable, info.num_pending);
                        }
                    }
                }
            }
        });
    }

    // ─── despacho ────────────────────────────────────────────────────────────

    async fn dispatch(
        &self,
        msg: async_nats::jetstream::Message,
        subject: &str,
        durable: &str,
        handler: &dyn Handler,
        tenant_filter: Option<&str>,
    ) {
        let info = msg.info().ok();
        // `delivered` empieza en 1 en la primera entrega, no en 0.
        let attempt = info
            .as_ref()
            .map_or(1, |i| u32::try_from(i.delivered).unwrap_or(1).max(1));

        // La única señal que delata a un consumidor cuyo bucle murió: la conexión sigue
        // sana y solo el crecimiento de `pending` lo evidencia — 08-observability.md §4.
        if let Some(i) = &info {
            self.inner
                .metrics
                .consumer_pending(subject, durable, i.pending);
        }

        // POISON se detecta ANTES del handler: el mensaje no es interpretable, así que el
        // handler nunca llega a verlo — 04-errors.md §1.3.
        let event = match parse_event(&msg.payload) {
            Ok(e) => e,
            Err(err) => {
                self.handle_poison(subject, durable, &msg, err).await;
                return;
            }
        };

        // Filtrar ANTES del handler: un evento de otro tenant no es un fallo, no es para
        // nosotros. Se ACKea y se descarta — 09-multitenancy.md §3.
        if let Some(want) = tenant_filter {
            if event.tenantid != want {
                let _ = msg.ack().await;
                return;
            }
        }

        let delivery = Delivery {
            attempt,
            max_attempts: DEFAULT_MAX_DELIVER,
            subject: subject.to_string(),
            durable: durable.to_string(),
        };

        let inicio = std::time::Instant::now();

        let (signature_warning, outcome) =
            self.verify_and_run(handler, &msg, &event, delivery).await;

        let handler_err: HandlerError = match outcome {
            Ok(Ok(())) => {
                let _ = msg.ack().await;
                // El aviso de firma GANA sobre el `ok`: el evento se procesó, sí, pero la
                // pregunta que hay que poder responder antes de pasar a `require` es
                // cuántos eventos siguen sin firmar. Contarlo como `ok` la borraría, y
                // contarlo dos veces rompería `sum by (outcome) == total consumido`.
                self.inner.metrics.event_consumed(
                    subject,
                    durable,
                    if signature_warning {
                        ConsumeOutcome::InvalidSignature
                    } else {
                        ConsumeOutcome::Ok
                    },
                );
                self.inner.metrics.handler_duration(
                    subject,
                    durable,
                    inicio.elapsed().as_secs_f64(),
                );
                return;
            }
            Ok(Err(e)) => e,
            // Un pánico es un bug del consumidor, y reintentarlo cinco veces no lo
            // arregla: PERMANENT. En Node una excepción ya es capturable; aquí un
            // `unwrap()` en el handler mataría el task de consumo y con él la
            // suscripción entera.
            Err(_) => Box::new(
                FluxError::permanent(format!("pánico en el handler de {subject}"))
                    .with_code("HANDLER_PANIC"),
            ),
        };

        let c = self.inner.classifier.classify(handler_err.as_ref());

        let budget = effective_budget(c.max_attempts);

        if c.class == ErrorClass::Retryable && attempt < budget {
            let delay = retry_delay(attempt, c.retry_after);
            self.log(
                LogLevel::Warn,
                &format!(
                    "RETRYABLE {} en {subject} (intento {attempt}/{budget}), reintento en {:?}",
                    c.code, delay
                ),
            );
            self.inner.metrics.event_retried(subject, durable, attempt);
            let _ = msg.ack_with(AckKind::Nak(Some(delay))).await;
            return;
        }

        self.route_to_dlq(
            msg,
            subject,
            durable,
            event,
            &handler_err,
            &c,
            attempt,
            inicio,
            signature_warning,
        )
        .await;
    }

    /// Verifica la firma y ejecuta el handler.
    ///
    /// La firma se comprueba **ANTES del handler** y antes que cualquier validación de
    /// payload: un evento manipulado puede tener un `data` impecable y aun así no ser del
    /// productor que dice — 07-signing.md §5.
    ///
    /// El `bool` devuelto es "este evento habría sido rechazado en modo `require`". En
    /// modo `warn` el evento se acepta, pero el fallo **se cuenta igual**: sin esa métrica,
    /// `warn` es inútil para lo único que existe —pilotar la migración—, y la pregunta
    /// "¿cuántos eventos siguen sin firma y de qué productores?" habría que buscarla a
    /// mano en los logs de siete servicios (07-signing.md §7.1).
    #[allow(clippy::type_complexity)]
    async fn verify_and_run(
        &self,
        handler: &dyn Handler,
        msg: &async_nats::jetstream::Message,
        event: &Event,
        delivery: Delivery,
    ) -> (bool, Result<HandlerResult, Box<dyn std::any::Any + Send>>) {
        #[cfg(feature = "signing")]
        let signature_warning = match self
            .inner
            .verifier
            .as_ref()
            .map_or(Ok(None), |v| v.check(event))
        {
            Ok(aviso) => aviso.is_some(),
            Err(e) => return (false, Ok(Err(Box::new(e) as HandlerError))),
        };

        #[cfg(not(feature = "signing"))]
        let signature_warning = false;

        // L3 al consumir, DESPUÉS de la firma y ANTES del handler. El fallo se clasifica
        // PERMANENT: el evento es sintácticamente correcto pero incumple su contrato, y
        // reintentarlo dará exactamente el mismo resultado — 04-errors.md §1.2.
        if let Err(e) = self.validate_on_consume(event, &delivery.subject) {
            return (signature_warning, Ok(Err(e)));
        }

        (
            signature_warning,
            self.run_handler(handler, msg, event, delivery).await,
        )
    }

    /// Validación L3 opcional al consumir. Sin la feature, o con `on_consume = false`, no
    /// hace nada.
    ///
    /// Es defensa en profundidad y por eso es opt-in dentro de lo opt-in: el sitio donde un
    /// contrato roto se arregla es el productor. Aquí solo evita que un evento inválido
    /// llegue a un handler que asume que su `data` cumple lo que promete.
    // Sin la feature `validation` esto es un no-op: no toca `self` y no puede fallar, así
    // que clippy marca `unused_self` y `unnecessary_wraps`. La firma DEBE ser idéntica en
    // las dos compilaciones —quien llama no sabe qué features están activas— y por eso el
    // allow se aplica SOLO a esa variante, con `cfg_attr`: en la compilación con
    // validación, donde ambos lints sí dirían algo, siguen activos.
    #[cfg_attr(
        not(feature = "validation"),
        allow(clippy::unused_self, clippy::unnecessary_wraps)
    )]
    fn validate_on_consume(&self, event: &Event, subject: &str) -> HandlerResult {
        #[cfg(feature = "validation")]
        if self.inner.opts.validation.on_consume {
            if let Some(validator) = &self.inner.validator {
                validator
                    .validate(event, subject)
                    .map_err(|e| Box::new(e) as HandlerError)?;
            }
        }
        #[cfg(not(feature = "validation"))]
        let _ = (event, subject);

        Ok(())
    }

    /// El final del despacho: guardar en la DLQ, contar y terminar el mensaje.
    ///
    /// Vive aparte de [`Bus::dispatch`] solo por tamaño; el orden de los pasos es el que
    /// importa y es el mismo: **primero guardar, después terminar**.
    #[allow(clippy::too_many_arguments)]
    async fn route_to_dlq(
        &self,
        msg: async_nats::jetstream::Message,
        subject: &str,
        durable: &str,
        event: Event,
        handler_err: &HandlerError,
        c: &Classification,
        attempt: u32,
        inicio: std::time::Instant,
        signature_warning: bool,
    ) {
        let reason = DlqReason::from(c.class);
        let error_text = format!("{}: {}", c.code, describe(handler_err.as_ref()));

        let dlq_event = match self
            .send_to_dlq(subject, event, durable, attempt, reason, error_text)
            .await
        {
            Ok(e) => e,
            Err(e) => {
                // **NO** se hace term(): terminar aquí borraría el evento sin haberlo
                // guardado en ningún sitio. Se deja reentregar — es preferible
                // reprocesar un evento que perderlo sin rastro. La causa suele ser un
                // problema del broker, no del evento.
                self.log(
                    LogLevel::Error,
                    &format!(
                        "no se pudo enrutar a la DLQ {}: {e}. El mensaje NO se termina; \
                         se reentregará.",
                        dlq_subject(subject)
                    ),
                );
                return;
            }
        };

        // `invalid_signature` no es una clase de error aparte —un fallo de firma es POISON,
        // y su `dlqreason` en la DLQ **sí** es `poison`— pero sí es un `outcome` propio:
        // 07-signing.md §7.2 lo exige porque son dos preguntas distintas y las dos
        // importan. `outcome="poison"` es "un productor publica basura", un bug de
        // serialización; `outcome="invalid_signature"` es "alguien publica eventos que no
        // son suyos", un incidente de seguridad o una migración a medias. Mezclarlas hace
        // que `rate(…{outcome="poison"})` mida cosas distintas según el lenguaje del
        // consumidor, que es justo lo que 08-observability.md §1 existe para impedir.
        // `invalid_schema` sigue la misma lógica y por el mismo motivo: su `dlqreason` es
        // `permanent` —el consumidor lo rechazó definitivamente— pero el `outcome` tiene
        // que poder distinguir "un productor publica payloads que violan su contrato" de
        // "mi lógica de negocio rechazó el evento". Son dos preguntas con dos dueños.
        self.inner.metrics.event_consumed(
            subject,
            durable,
            if signature_warning || is_signature_code(&c.code) {
                ConsumeOutcome::InvalidSignature
            } else if is_schema_code(&c.code) {
                ConsumeOutcome::InvalidSchema
            } else {
                ConsumeOutcome::from(reason)
            },
        );
        self.inner
            .metrics
            .event_dlq(subject, durable, DlqReasonLabel::from(reason), &c.code);
        self.inner
            .metrics
            .handler_duration(subject, durable, inicio.elapsed().as_secs_f64());

        if let Some(cb) = &self.inner.opts.on_dlq {
            cb(DlqEventInfo {
                subject: subject.to_string(),
                event: dlq_event,
                classification: c.clone(),
            });
        }
        self.log(
            LogLevel::Error,
            &format!(
                "DLQ ({reason:?}) {} en {subject} tras {attempt} intento(s): {}",
                c.code,
                describe(handler_err.as_ref())
            ),
        );
        let _ = msg.ack_with(AckKind::Term).await;
    }

    /// Ejecuta el handler emitiendo WIP cada `ack_wait / 2` mientras siga vivo.
    ///
    /// El WIP resetea el temporizador de reentrega en el servidor y evita que el mismo
    /// evento se ejecute en concurrencia consigo mismo. Cada `ack_wait / 2` para que un
    /// ciclo perdido no agote el plazo — 03-delivery.md §2.1.
    ///
    /// Va en el mismo task que el handler —con `select!` en vez de un task aparte— para
    /// no tener que clonar el mensaje ni coordinar la parada del ticker: cuando el
    /// handler termina, el bucle sale y el ticker muere con él.
    #[allow(clippy::type_complexity)]
    async fn run_handler(
        &self,
        handler: &dyn Handler,
        msg: &async_nats::jetstream::Message,
        event: &Event,
        delivery: Delivery,
    ) -> Result<HandlerResult, Box<dyn std::any::Any + Send>> {
        // El contexto del evento entrante se instala aquí, de modo que un publish() a
        // cualquier profundidad de la pila hereda correlationid y traceparent sin que
        // nadie pase nada — ver el módulo `context`.
        let call = context::scope(
            Some(EventContext::from_event(event)),
            handler.call(event.clone(), delivery),
        );
        let call = AssertUnwindSafe(call).catch_unwind();
        tokio::pin!(call);

        let mut wip = tokio::time::interval(DEFAULT_ACK_WAIT / 2);
        wip.tick().await; // el primer tick de un interval es inmediato

        loop {
            tokio::select! {
                result = &mut call => return result,
                _ = wip.tick() => {
                    // Un error aquí significa que el mensaje ya está resuelto.
                    let _ = msg.ack_with(AckKind::Progress).await;
                }
            }
        }
    }

    async fn handle_poison(
        &self,
        subject: &str,
        durable: &str,
        msg: &async_nats::jetstream::Message,
        err: FluxError,
    ) {
        let text = describe(&err);
        let code = err.code().unwrap_or("POISON").to_string();
        self.inner
            .metrics
            .event_consumed(subject, durable, ConsumeOutcome::Poison);
        self.inner
            .metrics
            .event_dlq(subject, durable, DlqReasonLabel::Poison, &code);
        self.log(LogLevel::Error, &format!("POISON en {subject}: {text}"));
        if let Some(cb) = &self.inner.opts.on_poison {
            cb(PoisonInfo {
                subject: subject.to_string(),
                error: err,
                raw: msg.payload.to_vec(),
            });
        }
        if let Err(e) = self
            .send_raw_to_dlq(subject, &msg.payload, durable, &text)
            .await
        {
            // Igual que arriba: no se termina lo que no se ha llegado a guardar.
            self.log(
                LogLevel::Error,
                &format!(
                    "no se pudo guardar el POISON de {subject} en la DLQ: {e}. Se reentregará."
                ),
            );
            return;
        }
        let _ = msg.ack_with(AckKind::Term).await;
    }

    /// Publica el CloudEvent original **íntegro** más las extensiones `dlq*`.
    async fn send_to_dlq(
        &self,
        subject: &str,
        event: Event,
        consumer: &str,
        attempts: u32,
        reason: DlqReason,
        error: String,
    ) -> Result<Event, FluxError> {
        let dlq_event = to_dlq_event(
            event,
            &DlqInfo {
                reason,
                attempts,
                consumer: consumer.to_string(),
                error,
            },
            Utc::now(),
        );
        let payload = serialize(&dlq_event)?;
        // El msgID incluye el consumidor: dos consumidores distintos del mismo evento
        // producen dos entradas de DLQ, y son dos hechos distintos.
        let msg_id = format!("dlq-{}-{consumer}", dlq_event.id);
        self.publish_raw(&dlq_subject(subject), payload, &msg_id)
            .await?;
        Ok(dlq_event)
    }

    /// Guarda un mensaje ininterpretable.
    ///
    /// Un POISON no tiene CloudEvent que preservar, así que se envuelve el cuerpo crudo
    /// en un envelope sintético del dominio `system`. Es la única excepción a la regla de
    /// "el mensaje de DLQ es el original íntegro": no había original que preservar.
    async fn send_raw_to_dlq(
        &self,
        subject: &str,
        raw: &[u8],
        consumer: &str,
        error: &str,
    ) -> Result<(), FluxError> {
        let event_id = Uuid::now_v7().to_string();
        let now = format_time(Utc::now());

        // El base64 se recorta para que el envelope quepa en 1 MiB con margen para el
        // resto de atributos.
        let mut encoded = base64(raw);
        encoded.truncate(700_000);

        let mut envelope = build_event(BuildEventInput {
            subject: "system.poison.v1.capturado".to_string(),
            data: data_to_raw(&json!({
                "originalSubject": subject,
                "rawBase64": encoded,
                "rawBytes": raw.len(),
            }))?,
            id: event_id.clone(),
            source: self.inner.source.clone(),
            producerversion: self.inner.opts.version.clone(),
            tenantid: "system".to_string(),
            dataclassification: DataClassification::Internal,
            dataschema: "https://schemas.internal/system/poison/capturado/1.0.0.json".to_string(),
            correlationid: event_id.clone(),
            time: None,
            aggregate_id: None,
            causationid: None,
            partitionkey: None,
            traceparent: None,
            tracestate: None,
        })?;
        envelope.dlqreason = Some(DlqReason::Poison);
        envelope.dlqattempts = Some(1);
        envelope.dlqconsumer = Some(consumer.to_string());
        envelope.dlqerror = Some(error.chars().take(1024).collect());
        envelope.dlqtime = Some(now);

        let payload = serialize(&envelope)?;
        self.publish_raw(
            &dlq_subject(subject),
            payload,
            &format!("poison-{event_id}"),
        )
        .await
    }

    // ─── provisión de streams ────────────────────────────────────────────────

    async fn ensure_stream(&self, p: &ParsedSubject) -> Result<(), FluxError> {
        let name = stream_name(&p.domain);
        self.ensure(
            &name,
            stream::Config {
                name: name.clone(),
                subjects: vec![format!("{}.>", p.domain)],
                storage: StorageType::File,
                // LimitsPolicy: el stream retiene por edad, no hasta que alguien consuma.
                // Un consumidor lento no debe poder hacer crecer el disco sin límite.
                retention: RetentionPolicy::Limits,
                discard: DiscardPolicy::Old,
                max_age: STREAM_MAX_AGE,
                // Duplicates solo aplica a publicaciones. Ver DUPLICATE_WINDOW.
                duplicate_window: DUPLICATE_WINDOW,
                ..Default::default()
            },
        )
        .await
    }

    async fn ensure_dlq_stream(&self, p: &ParsedSubject) -> Result<(), FluxError> {
        let name = dlq_stream_name(&p.domain);
        // Prefijo `dlq.`, nunca sufijo: con sufijo encajaría con `<domain>.>` y el stream
        // principal capturaría sus propios muertos — 02-naming.md §3.1.
        self.ensure(
            &name,
            stream::Config {
                name: name.clone(),
                subjects: vec![format!("dlq.{}.>", p.domain)],
                storage: StorageType::File,
                retention: RetentionPolicy::Limits,
                discard: DiscardPolicy::Old,
                max_age: DLQ_STREAM_MAX_AGE,
                ..Default::default()
            },
        )
        .await
    }

    /// Crea el stream si no existe, y si existe lo deja **tal cual**.
    ///
    /// No se actualiza a propósito: un SDK que actualiza streams ajenos en cada arranque
    /// puede pisar en silencio la configuración que puso el equipo de plataforma
    /// (réplicas, límites, mirrors). En producción los streams los provisiona
    /// infraestructura; esto es una comodidad de desarrollo — 02-naming.md §3.2.
    ///
    /// Por eso la ACL de producción **no** concede `$JS.API.STREAM.CREATE.*` a un servicio
    /// (06-security.md §3), y por eso se hace en dos pasos —consultar y, solo si no existe,
    /// crear— y no con `get_or_create_stream`: hay que saber en cuál se deniega. Sin
    /// permiso para consultar es [`FluxError::PermissionDenied`]; sin permiso para crear
    /// lo que falta, [`FluxError::NotProvisioned`], con el nombre del stream y el remedio.
    ///
    /// `num_replicas` se deja sin fijar: la spec pide 3, pero un cluster de un solo nodo
    /// —el docker-compose de desarrollo— rechazaría la creación.
    pub(crate) async fn ensure(&self, name: &str, cfg: stream::Config) -> Result<(), FluxError> {
        if lock(&self.inner.ensured).contains(name) {
            return Ok(());
        }

        let info = format!("$JS.API.STREAM.INFO.{name}");
        match self.guarded(&[info], self.inner.js.get_stream(name)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if is_stream_not_found(&e) => {
                let create = format!("$JS.API.STREAM.CREATE.{name}");
                match self
                    .guarded(&[create], self.inner.js.create_stream(cfg))
                    .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        return Err(transport(format!("no se pudo crear el stream `{name}`"), e))
                    }
                    Err(denial) => {
                        return Err(FluxError::NotProvisioned {
                            kind: ResourceKind::Stream,
                            name: name.to_string(),
                            denied: denial.describe(),
                        })
                    }
                }
            }
            Ok(Err(e)) => {
                return Err(transport(
                    format!("no se pudo consultar el stream `{name}`"),
                    e,
                ))
            }
            Err(denial) => {
                return Err(permission_denied(
                    format!("consultar el stream `{name}`"),
                    &denial,
                ))
            }
        }

        lock(&self.inner.ensured).insert(name.to_string());
        Ok(())
    }

    /// Abre un stream que ya existe.
    async fn open_stream(&self, name: &str) -> Result<stream::Stream, FluxError> {
        let info = format!("$JS.API.STREAM.INFO.{name}");
        match self.guarded(&[info], self.inner.js.get_stream(name)).await {
            Ok(Ok(stream)) => Ok(stream),
            Ok(Err(e)) => Err(transport(format!("no se pudo abrir el stream `{name}`"), e)),
            Err(denial) => Err(permission_denied(
                format!("abrir el stream `{name}`"),
                &denial,
            )),
        }
    }

    /// Comprueba que el servicio PUEDE pedir mensajes de su consumidor, o falla con
    /// [`FluxError::PermissionDenied`] nombrando el subject denegado.
    ///
    /// **Por qué una sonda y no esperar al primer pull.** Un pull denegado no da error: el
    /// servidor descarta el mensaje y manda un `-ERR` aparte (A-20, A-28). El SDK de Node lo
    /// detecta arrancando el bucle, cediendo un turno y haciendo un `flush`, que allí es un
    /// PING/PONG: el `-ERR` llega antes que el PONG porque el protocolo es ordenado. Aquí no
    /// hay equivalente. El `flush()` de `async-nats` solo vacía el buffer de escritura —no hace
    /// ida y vuelta— y el primer pull lo publica una tarea de fondo de la librería, sin orden
    /// respecto a quien llama: esperar «un poco» y mirar sería una carrera. Se midió: con el
    /// permiso quitado, `subscribe()` devolvía `Ok` a los ~4 ms y la denegación se registraba
    /// ~2 ms **después**.
    ///
    /// Lo determinista es no depender del pull de la librería: se pregunta lo mismo sobre
    /// **el mismo subject** (`$JS.API.CONSUMER.MSG.NEXT.<stream>.<durable>`, el que usará el
    /// pull) con una petición propia y se compite su respuesta con la denegación, que es el
    /// mecanismo de [`Self::guarded`]. El cuerpo es un JSON truncado, `{`, **a propósito**: el
    /// servidor lo contesta `400 Bad Request` al instante, sin apuntar la petición como
    /// pendiente y sin sacar ningún mensaje del consumidor —cualquier pull válido podría
    /// llevarse uno, que quedaría sin ack hasta `ack_wait—. Medido contra nats-server 2.14.5:
    /// `400 Bad Request - unexpected end of JSON input` en ~0,4 ms. Lo único que importa de la
    /// respuesta es que **no** sea una denegación.
    ///
    /// Si la sonda no obtiene respuesta ni denegación (el broker no contesta, o contesta algo
    /// inesperado) **no** se falla: se avisa por el `logger` y se sigue. No poder comprobarlo
    /// no es lo mismo que estar denegado, y una denegación posterior sigue llegando al
    /// `logger` por su cauce de siempre.
    pub(crate) async fn ensure_pull_allowed(
        &self,
        stream_name: &str,
        durable: &str,
    ) -> Result<(), FluxError> {
        let subject = format!("$JS.API.CONSUMER.MSG.NEXT.{stream_name}.{durable}");
        let probe = tokio::time::timeout(
            PULL_PROBE_TIMEOUT,
            self.inner.client.request(subject.clone(), "{".into()),
        );
        match self.guarded(&[subject], probe).await {
            Err(denial) => Err(permission_denied(
                format!(
                    "pedir mensajes del consumidor `{durable}` del stream `{stream_name}` \
                     (sin ese permiso el consumidor se quedaría inerte, sin recibir nada y sin \
                     ningún error)"
                ),
                &denial,
            )),
            Ok(Ok(Ok(reply))) if reply.status.is_some_and(|st| st == 400) => Ok(()),
            Ok(other) => {
                self.log(
                    LogLevel::Warn,
                    &format!(
                        "no se pudo comprobar el permiso de pull de `{durable}` en \
                         `{stream_name}`: la sonda no obtuvo el 400 esperado ({}). Si la ACL no \
                         concede `$JS.API.CONSUMER.MSG.NEXT.{stream_name}.{durable}`, el \
                         consumidor se quedará inerte y solo lo dirá este logger",
                        match other {
                            Ok(Ok(reply)) => format!("respuesta {:?}", reply.status),
                            Ok(Err(e)) => e.to_string(),
                            Err(_) => format!("sin respuesta en {PULL_PROBE_TIMEOUT:?}"),
                        }
                    ),
                );
                Ok(())
            }
        }
    }

    /// Ejecuta una petición al broker vigilando las denegaciones de permisos — ver
    /// [`crate::security`].
    ///
    /// `subjects` son los subjects **exactos** a los que publica la petición: una
    /// denegación de cualquier otro no es culpa de esta operación.
    pub(crate) async fn guarded<T, F>(&self, subjects: &[String], fut: F) -> Result<T, Denial>
    where
        F: Future<Output = T>,
    {
        self.inner
            .denials
            .guard(&self.inner.inbox_prefix, subjects, fut)
            .await
    }

    /// El consumidor durable: **pregunta primero** si existe, y solo si no existe lo crea.
    ///
    /// ```text
    /// CONSUMER.INFO → existe                    → lo usa tal cual; `assert_config_honored`
    ///                                             sigue comparando su config con la canónica
    /// CONSUMER.INFO → no existe
    ///   + CONSUMER.CREATE concedido             → lo crea (desarrollo, o ACL amplia)
    ///   + CONSUMER.CREATE denegado              → NotProvisioned (nombra el consumidor)
    /// CONSUMER.INFO denegado                    → PermissionDenied (ni siquiera puede mirar)
    /// ```
    ///
    /// **Por qué info antes de crear** (06-security.md §3.4, y es lo que hace el SDK de Node):
    /// en producción `flux provision` crea el consumidor y la ACL del servicio **no** concede
    /// `CONSUMER.CREATE`. Intentar crear primero provocaba, en cada arranque, una denegación
    /// que el broker dejaba escrita en su log (`Publish Violation - Subject
    /// "$JS.API.CONSUMER.CREATE…"`) aunque el SDK la absorbiera: ruido que entierra las
    /// violaciones de verdad, y una ACL que funciona «con un error por arranque». Preguntar
    /// primero lo deja en cero.
    ///
    /// «No existe» y «no puedo crearlo» son **dos** respuestas del broker, y `NotProvisioned`
    /// nace de la pareja: consultar → no existe **y** crear → denegado. Una denegación sola no
    /// lo es (un `CREATE` denegado sobre un consumidor que existe ni se intenta).
    async fn obtain_consumer(
        &self,
        js_stream: &stream::Stream,
        stream_name: &str,
        durable: &str,
        requested: consumer::pull::Config,
    ) -> Result<consumer::Consumer<consumer::pull::Config>, FluxError> {
        // 1. ¿Existe ya? (lo normal en producción: lo aprovisionó infraestructura)
        if let Some(existing) = self.consumer_info(js_stream, stream_name, durable).await? {
            return Ok(existing);
        }

        // 2. No existe: crearlo, si la ACL lo permite.
        let create = format!(
            "$JS.API.CONSUMER.CREATE.{stream_name}.{durable}.{}",
            requested.filter_subject
        );
        match self
            .guarded(&[create], js_stream.create_consumer(requested))
            .await
        {
            Ok(Ok(consumer)) => Ok(consumer),
            Ok(Err(create_error)) => {
                // Otra instancia del servicio pudo crearlo entre la pregunta y la creación.
                if let Ok(Some(existing)) =
                    self.consumer_info(js_stream, stream_name, durable).await
                {
                    return Ok(existing);
                }
                Err(transport(
                    format!("no se pudo crear el consumidor `{durable}` en `{stream_name}`"),
                    create_error,
                ))
            }
            Err(denial) => Err(FluxError::NotProvisioned {
                kind: ResourceKind::Consumer,
                name: durable.to_string(),
                denied: denial.describe(),
            }),
        }
    }

    /// `CONSUMER.INFO` del durable: `Some` si existe, `None` si el servidor contesta «no
    /// existe» (error 10014), y un error si no se puede saber —sin permiso de consulta es
    /// [`FluxError::PermissionDenied`]—.
    async fn consumer_info(
        &self,
        js_stream: &stream::Stream,
        stream_name: &str,
        durable: &str,
    ) -> Result<Option<consumer::Consumer<consumer::pull::Config>>, FluxError> {
        let info = format!("$JS.API.CONSUMER.INFO.{stream_name}.{durable}");
        match self
            .guarded(&[info], js_stream.consumer_info(durable))
            .await
        {
            Ok(Ok(info)) => {
                let config =
                    <consumer::pull::Config as consumer::FromConsumer>::try_from_consumer_config(
                        info.config.clone(),
                    )
                    .map_err(|e| {
                        transport(
                            format!("el consumidor `{durable}` de `{stream_name}` no es un pull"),
                            e,
                        )
                    })?;
                Ok(Some(consumer::Consumer::new(
                    config,
                    info,
                    self.inner.js.clone(),
                )))
            }
            Ok(Err(e)) if matches!(e.kind(), ConsumerInfoErrorKind::NotFound) => Ok(None),
            Ok(Err(e)) => Err(transport(
                format!("no se pudo consultar el consumidor `{durable}` en `{stream_name}`"),
                e,
            )),
            Err(denial) => Err(permission_denied(
                format!(
                    "consultar el consumidor `{durable}` en `{stream_name}` (la ACL debe \
                     conceder `$JS.API.CONSUMER.INFO.{stream_name}.{durable}`: es lo que el \
                     SDK pregunta antes de crear, y lo único que necesita si lo aprovisiona \
                     infraestructura)"
                ),
                &denial,
            )),
        }
    }
}

/// `true` si el servidor contestó «stream no encontrado» (error 10059).
fn is_stream_not_found(e: &async_nats::jetstream::context::GetStreamError) -> bool {
    matches!(
        e.kind(),
        GetStreamErrorKind::JetStream(je) if je.error_code() == ErrorCode::STREAM_NOT_FOUND
    )
}

/// Una pista para el log cuando lo denegado es **publicar en un inbox**: es lo que hace un dueño al
/// CONTESTAR una petición, y sin `allow_responses` en su ACL quien llama solo ve un timeout (A-68,
/// 11-request-reply.md §7.2).
fn denial_hint(denial: &Denial) -> &'static str {
    if denial.action == DeniedAction::Publish && denial.subject.starts_with("_INBOX.") {
        " Si es la respuesta a una petición, a la ACL del dueño le falta `allow_responses` (mejor \
         `{ max: 1, expires: 30s }`): quien llama verá un RPC_TIMEOUT (11-request-reply.md §7.2)."
    } else {
        ""
    }
}

pub(crate) fn permission_denied(operation: String, denial: &Denial) -> FluxError {
    FluxError::PermissionDenied {
        operation,
        denied: denial.describe(),
    }
}

impl Bus {
    /// Reproduce un evento de la DLQ en su subject original.
    ///
    /// El replay es exactamente "borrar las extensiones `dlq*` y republicar" — el mismo
    /// `jq 'del(.dlq*)'` de 04-errors.md §4.1 — y **conserva el `id` original**.
    ///
    /// Antes de llamar a esto, **dos comprobaciones obligatorias**:
    ///
    /// 1. **¿Se ha arreglado la causa?** Reproducir contra el mismo bug devuelve el evento
    ///    a la DLQ y ensucia el rastro.
    /// 2. **¿Sigue siendo idempotente el consumidor para este `id`?** Como el `id` se
    ///    conserva, si el consumidor aplicó un efecto parcial antes de fallar, la tabla de
    ///    eventos procesados **DEBE** ser lo que impida duplicarlo. Si el handler no es
    ///    idempotente, el replay es más peligroso que el fallo original.
    ///
    /// # Errores
    ///
    /// [`FluxError::InvalidSubject`] si el `type` del evento no es parseable, y
    /// [`FluxError::Transport`] si la republicación falla.
    pub async fn replay_from_dlq(&self, event: Event) -> Result<Event, FluxError> {
        let subject = event.parsed_type()?.subject();
        let clean = strip_dlq_extensions(event);
        let payload = serialize(&clean)?;
        // El id original se CONSERVA: regenerarlo rompería la idempotencia de todos los
        // consumidores aguas abajo y convertiría una recuperación en un incidente nuevo.
        self.publish_raw(&subject, payload, &clean.id).await?;
        Ok(clean)
    }
}

/// El presupuesto de entregas de ESTE error: el **menor** entre el del consumidor y el
/// que imponga su clasificación.
///
/// Así un error desconocido agota su presupuesto acotado (2 entregas, ~30 s) sin recortar
/// los 6 intentos de un `ECONNRESET` reconocido, que es exactamente lo que pasaría si el
/// presupuesto se configurase en `max_deliver` — que es por consumidor, no por mensaje
/// — 04-errors.md §2.1.
pub(crate) fn effective_budget(max_attempts: Option<u32>) -> u32 {
    max_attempts.map_or(DEFAULT_MAX_DELIVER, |m| m.min(DEFAULT_MAX_DELIVER))
}

/// El filtro de tenant que aplicará esta suscripción.
///
/// El de la suscripción gana sobre el de la conexión. Y **`"system"` NO cuenta como
/// filtro**: es la ausencia de tenant, el valor reservado para los eventos de plataforma,
/// y usarlo como comodín o como default cuando el tenant real se desconoce está prohibido
/// — 09-multitenancy.md §5. Si `"system"` contase, un servicio sin `tenant_id` real
/// pasaría la comprobación de `strict` sin filtrar nada, que es exactamente el descuido
/// silencioso que §3 obliga a convertir en error.
fn effective_tenant_filter(subscription: Option<&str>, connection: Option<&str>) -> Option<String> {
    subscription
        .or(connection)
        .filter(|t| !t.is_empty() && *t != "system")
        .map(ToString::to_string)
}

/// Por qué `strict` no encontró filtro, dicho de forma accionable.
fn tenant_isolation_reason(connection_tenant: Option<&str>) -> &'static str {
    match connection_tenant {
        Some("system") => {
            "el tenant de la conexión es \"system\", que es la AUSENCIA de tenant y no un \
             filtro: se reserva para eventos de plataforma y no debe usarse como comodín \
             (09-multitenancy.md §5). Pasa un tenant real en ConnectOptions::with_tenant_id \
             o en SubscribeOptions::with_tenant_filter"
        }
        _ => {
            "no hay tenant ni en ConnectOptions::with_tenant_id ni en \
             SubscribeOptions::with_tenant_filter"
        }
    }
}

/// Los tres códigos POISON de la firma — 07-signing.md §7.
fn is_signature_code(code: &str) -> bool {
    matches!(
        code,
        "MISSING_SIGNATURE" | "INVALID_SIGNATURE" | "UNKNOWN_SIGNING_KEY"
    )
}

/// Los dos códigos de la validación L3 — 00-protocol.md §5.
pub(crate) fn is_schema_code(code: &str) -> bool {
    code == crate::errors::INVALID_SCHEMA_CODE || code == crate::errors::SCHEMA_NOT_FOUND_CODE
}

impl From<DlqReason> for ConsumeOutcome {
    fn from(r: DlqReason) -> Self {
        match r {
            DlqReason::Retryable => Self::Retryable,
            DlqReason::Permanent => Self::Permanent,
            DlqReason::Poison => Self::Poison,
        }
    }
}

/// El retraso del siguiente intento.
///
/// Se emite un retraso **explícito** según el backoff canónico en vez de dejar expirar
/// `ack_wait`: así no se retiene la ranura de `max_ack_pending` 30 s de más. El
/// `retry_after` de la clasificación gana, porque viene de un `Retry-After` que la
/// dependencia anunció.
///
/// ⚠️ **`retry_after` es una SUGERENCIA para el PRIMER reintento, no un control del
/// calendario de reintentos** — 03-delivery.md §2.2.
///
/// Medido contra nats-server 2.14.5 durante el port a Rust, y ya normativo en la spec:
/// cuando el consumidor tiene `backoff` configurado —y en flux lo tiene **siempre**—, el
/// servidor honra el delay de un `-NAK` **solo en la primera reentrega**. A partir de la
/// segunda manda el array `backoff`, y el delay pedido se ignora sin ningún aviso:
///
/// ```text
/// consumidor SIN backoff, nak(300ms): entregas a 0 ms, 300 ms, 600 ms, 900 ms  ← honrado siempre
/// consumidor CON backoff, nak(300ms): entregas a 0 ms, 300 ms, luego backoff[1] = 60 s
/// ```
///
/// Consecuencia práctica: un `Retry-After: 5` de un proveedor **acorta el primer reintento
/// y nada más**; del segundo en adelante manda el backoff canónico (1 m, 5 m, 15 m, 30 m).
/// Los seis SDKs emiten el mismo `nak(delay)` y obtienen el mismo comportamiento, así que
/// no hay divergencia entre ellos — lo que había que arreglar era la documentación. **Un
/// SDK NO DEBE construir lógica que dependa de que el delay se respete más allá de la
/// primera vez.** Ver `conformance/cases/nak-delay-ignored-with-backoff.json`.
pub(crate) fn retry_delay(attempt: u32, retry_after: Option<Duration>) -> Duration {
    retry_after.unwrap_or_else(|| {
        // `attempt` empieza en 1, así que el primer reintento usa backoff[0]. Se satura
        // en la última entrada por si algún día max_deliver crece por encima del backoff.
        let idx = ((attempt.max(1) - 1) as usize).min(CANONICAL_BACKOFF.len() - 1);
        CANONICAL_BACKOFF[idx]
    })
}

/// Verifica que el servidor aplicó lo solicitado.
///
/// Requisito **L2** y única defensa contra sobrescrituras silenciosas — 03-delivery.md
/// §2.1. Se comprueban solo los campos que el SDK pide explícitamente: el resto lo rellena
/// el servidor con sus defaults y compararlos daría falsos positivos.
pub(crate) fn assert_config_honored(
    durable: &str,
    requested: &consumer::pull::Config,
    effective: &consumer::Config,
) -> Result<(), FluxError> {
    let mut diffs = Vec::new();

    let mut check = |field: &str, req: String, eff: String| {
        if req != eff {
            diffs.push(ConfigDifference {
                field: field.to_string(),
                requested: req,
                effective: eff,
            });
        }
    };

    check(
        "ack_wait",
        format!("{:?}", requested.ack_wait),
        format!("{:?}", effective.ack_wait),
    );
    check(
        "max_deliver",
        requested.max_deliver.to_string(),
        effective.max_deliver.to_string(),
    );
    check(
        "max_ack_pending",
        requested.max_ack_pending.to_string(),
        effective.max_ack_pending.to_string(),
    );
    check(
        "ack_policy",
        format!("{:?}", requested.ack_policy),
        format!("{:?}", effective.ack_policy),
    );
    check(
        "backoff",
        format!("{:?}", requested.backoff),
        format!("{:?}", effective.backoff),
    );

    // Invariante del protocolo, no solo del SDK: aunque el servidor haya devuelto lo
    // mismo que se le pidió, ack_wait y backoff[0] tienen que coincidir. Si alguien
    // cambia CANONICAL_BACKOFF y olvida DEFAULT_ACK_WAIT, esto lo caza aquí y no en
    // producción a las 3 de la mañana.
    if let Some(first) = effective.backoff.first() {
        if *first != effective.ack_wait {
            diffs.push(ConfigDifference {
                field: "ack_wait == backoff[0]".to_string(),
                requested: format!("{first:?}"),
                effective: format!("{:?}", effective.ack_wait),
            });
        }
    }

    if diffs.is_empty() {
        return Ok(());
    }
    Err(FluxError::ConsumerConfigMismatch {
        durable: durable.to_string(),
        differences: diffs,
    })
}

/// base64 estándar, 30 líneas frente a una dependencia entera.
///
/// Solo se usa para guardar el cuerpo de un POISON, que es material forense y se lee una
/// vez al año. Añadir el crate `base64` al árbol de dependencias de todo servicio del
/// ecosistema por esto no sale a cuenta.
pub(crate) fn base64(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── La lista de servidores ───────────────────────────────────────────────

    /// `async-nats` 0.50 interpreta un `&str` como UNA dirección: una lista con comas no se entiende
    /// ni cuando todas sus direcciones son válidas (medido). Aquí se separa, y se comprueba **que
    /// lo que sale lo entiende `async-nats`** (el otro sentido: sin separar, no).
    #[test]
    fn la_lista_de_servidores_se_separa_por_comas() {
        use async_nats::ToServerAddrs;

        assert_eq!(split_servers("nats://a:4222"), ["nats://a:4222"]);
        assert_eq!(
            split_servers("nats://a:4222,nats://b:4222,nats://c:4222"),
            ["nats://a:4222", "nats://b:4222", "nats://c:4222"]
        );
        // Espacios y comas sobrantes se toleran (una variable de entorno escrita a mano).
        assert_eq!(
            split_servers(" nats://a:4222 , nats://b:4222,, "),
            ["nats://a:4222", "nats://b:4222"]
        );
        assert_eq!(split_servers("a:4222,b:4222"), ["a:4222", "b:4222"]);
        // Nada que separar: vacía (y `connect()` falla antes de la red).
        assert_eq!(split_servers(""), Vec::<String>::new());
        assert_eq!(split_servers(" , ,"), Vec::<String>::new());

        // CONTROL, el defecto medido: la cadena entera NO la entiende `async-nats`…
        let lista = "nats://a:4222,nats://b:4222";
        assert!(
            lista.to_server_addrs().is_err(),
            "async-nats 0.50 debería rechazar la lista entera"
        );
        // …y lo que sale de `split_servers` SÍ: dos direcciones, en orden.
        let direcciones: Vec<_> = split_servers(lista)
            .to_server_addrs()
            .expect("cada dirección es válida")
            .collect();
        assert_eq!(direcciones.len(), 2);
        assert_eq!(direcciones[0].host(), "a");
        assert_eq!(direcciones[1].host(), "b");
        // Una dirección mal escrita SIGUE siendo un error de entrada (no se traga).
        assert!(split_servers("nats://a:4222,no es una url")
            .to_server_addrs()
            .is_err());
    }

    /// `with_servers` es el `servers: string[]` de Node: lo mismo que unir por comas.
    #[test]
    fn with_servers_equivale_a_la_lista_con_comas() {
        let a = ConnectOptions::new("x", "pedidos-api", "dev", "1.0.0").with_servers([
            "nats://a:4222",
            " nats://b:4222 ",
            "",
            "nats://c:4222",
        ]);
        assert_eq!(a.servers, "nats://a:4222,nats://b:4222,nats://c:4222");
        assert_eq!(
            split_servers(&a.servers),
            ["nats://a:4222", "nats://b:4222", "nats://c:4222"]
        );
        let b = ConnectOptions::new("x", "pedidos-api", "dev", "1.0.0")
            .with_servers(vec![String::from("nats://solo:4222")]);
        assert_eq!(b.servers, "nats://solo:4222");
    }

    fn requested() -> consumer::pull::Config {
        consumer::pull::Config {
            durable_name: Some("facturacion-api__pedidos_pedido_v1_creado".into()),
            filter_subject: "pedidos.pedido.v1.creado".into(),
            ack_policy: AckPolicy::Explicit,
            ack_wait: DEFAULT_ACK_WAIT,
            max_deliver: i64::from(DEFAULT_MAX_DELIVER),
            backoff: CANONICAL_BACKOFF.to_vec(),
            max_ack_pending: i64::from(DEFAULT_MAX_ACK_PENDING),
            deliver_policy: DeliverPolicy::All,
            replay_policy: ReplayPolicy::Instant,
            ..Default::default()
        }
    }

    fn effective_from(req: &consumer::pull::Config) -> consumer::Config {
        use async_nats::jetstream::consumer::IntoConsumerConfig;
        req.clone().into_consumer_config()
    }

    #[test]
    fn la_config_canonica_se_acepta() {
        let req = requested();
        let eff = effective_from(&req);
        assert!(assert_config_honored("d", &req, &eff).is_ok());
    }

    /// La trampa verificada contra NATS 2.14.5: el servidor devuelve un ack_wait
    /// distinto del solicitado y no avisa.
    #[test]
    fn se_detecta_la_sobrescritura_de_ack_wait() {
        let req = requested();
        let mut eff = effective_from(&req);
        eff.ack_wait = Duration::from_secs(1);

        let err = assert_config_honored("d", &req, &eff).unwrap_err();
        let FluxError::ConsumerConfigMismatch { differences, .. } = &err else {
            panic!("se esperaba ConsumerConfigMismatch, llegó {err:?}");
        };
        // Se reporta dos veces a propósito: el campo difiere del solicitado Y rompe la
        // invariante ack_wait == backoff[0].
        assert!(differences.iter().any(|d| d.field == "ack_wait"));
        assert!(differences
            .iter()
            .any(|d| d.field == "ack_wait == backoff[0]"));
    }

    #[test]
    fn se_detecta_un_backoff_distinto() {
        let req = requested();
        let mut eff = effective_from(&req);
        eff.backoff = vec![Duration::from_secs(30), Duration::from_secs(60)];
        let err = assert_config_honored("d", &req, &eff).unwrap_err();
        assert!(err.to_string().contains("backoff"));
    }

    #[test]
    fn se_detecta_un_ack_policy_distinto() {
        let req = requested();
        let mut eff = effective_from(&req);
        eff.ack_policy = AckPolicy::None;
        let err = assert_config_honored("d", &req, &eff).unwrap_err();
        assert!(err.to_string().contains("ack_policy"));
    }

    /// Aunque el servidor devuelva exactamente lo pedido, la invariante del protocolo se
    /// comprueba sobre la config EFECTIVA.
    #[test]
    fn la_invariante_se_verifica_sobre_lo_efectivo() {
        let mut req = requested();
        req.ack_wait = Duration::from_secs(5);
        req.backoff = vec![Duration::from_secs(30)];
        let eff = effective_from(&req);
        let err = assert_config_honored("d", &req, &eff).unwrap_err();
        assert!(err.to_string().contains("ack_wait == backoff[0]"));
    }

    // ── presupuesto de reintentos — 04-errors.md §2.1 ─────────────────────────

    #[test]
    fn un_error_reconocido_conserva_las_seis_entregas() {
        // Sin tope propio manda el max_deliver del consumidor.
        assert_eq!(effective_budget(None), 6);
    }

    #[test]
    fn un_error_desconocido_se_acota_a_dos() {
        assert_eq!(effective_budget(Some(2)), 2);
    }

    #[test]
    fn el_presupuesto_nunca_supera_al_del_consumidor() {
        // Una clasificación no puede REGALAR entregas por encima de max_deliver: el
        // servidor dejaría de entregar antes y el tope sería una mentira.
        assert_eq!(effective_budget(Some(99)), 6);
    }

    /// La tabla de 04-errors.md §2.1, en números.
    #[test]
    fn los_tres_presupuestos_de_la_spec() {
        let reconocido = effective_budget(None); // ECONNRESET, 503
        let desconocido = effective_budget(Some(2)); // lo que nadie previó
        assert_eq!((reconocido, desconocido), (6, 2));
        // Un PERMANENT no entra en la rama de reintento, así que gasta 1 entrega.
    }

    #[test]
    fn el_backoff_se_recorre_en_orden_y_satura() {
        assert_eq!(retry_delay(1, None), Duration::from_secs(30));
        assert_eq!(retry_delay(2, None), Duration::from_secs(60));
        assert_eq!(retry_delay(5, None), Duration::from_secs(1800));
        // Más allá del backoff se satura en la última entrada en vez de desbordar.
        assert_eq!(retry_delay(99, None), Duration::from_secs(1800));
        // Un intento 0 no debería ocurrir, pero no puede restar por debajo de cero.
        assert_eq!(retry_delay(0, None), Duration::from_secs(30));
    }

    #[test]
    fn el_retry_after_anunciado_gana_al_backoff() {
        assert_eq!(
            retry_delay(1, Some(Duration::from_secs(7))),
            Duration::from_secs(7)
        );
    }

    #[test]
    fn la_clase_se_traduce_al_literal_de_dlqreason() {
        // El valor que acaba escrito en la extensión `dlqreason` del evento en la DLQ.
        for (class, reason) in [
            (ErrorClass::Retryable, DlqReason::Retryable),
            (ErrorClass::Permanent, DlqReason::Permanent),
            (ErrorClass::Poison, DlqReason::Poison),
        ] {
            assert_eq!(DlqReason::from(class), reason);
        }
    }

    // ── aislamiento de tenant — 09-multitenancy.md §3 ─────────────────────────

    #[test]
    fn el_filtro_de_la_suscripcion_gana_al_de_la_conexion() {
        assert_eq!(
            effective_tenant_filter(Some("globex"), Some("acme")),
            Some("globex".to_string())
        );
    }

    #[test]
    fn sin_filtro_de_suscripcion_manda_el_de_la_conexion() {
        assert_eq!(
            effective_tenant_filter(None, Some("acme")),
            Some("acme".to_string())
        );
    }

    /// `"system"` es la AUSENCIA de tenant, no un tenant: se reserva para eventos de
    /// plataforma y **NO DEBE** usarse como comodín ni como default — 09-multitenancy.md
    /// §5. Si contase como filtro, un servicio sin tenant real pasaría la comprobación de
    /// `strict` sin filtrar nada.
    #[test]
    fn system_no_cuenta_como_filtro_de_tenant() {
        assert_eq!(effective_tenant_filter(None, Some("system")), None);
        assert_eq!(effective_tenant_filter(Some("system"), None), None);
        assert_eq!(effective_tenant_filter(None, Some("")), None);
        assert_eq!(effective_tenant_filter(None, None), None);
    }

    #[test]
    fn el_default_es_off() {
        assert_eq!(
            ConnectOptions::new("nats://x", "svc", "produccion", "1.0.0").tenant_isolation,
            TenantIsolation::Off
        );
    }

    /// El mensaje tiene que decir qué hacer, y distinguir el caso de `"system"` del de
    /// "no hay tenant": son dos errores distintos con la misma cara.
    #[test]
    fn el_error_de_strict_explica_el_caso_de_system() {
        assert!(tenant_isolation_reason(Some("system")).contains("AUSENCIA de tenant"));
        assert!(tenant_isolation_reason(None).contains("with_tenant_id"));
    }

    #[test]
    fn el_error_de_aislamiento_nombra_el_subject_y_la_seccion() {
        let err = FluxError::TenantIsolation {
            subject: "pedidos.pedido.v1.creado".into(),
            reason: tenant_isolation_reason(None).to_string(),
        }
        .to_string();
        assert!(err.contains("pedidos.pedido.v1.creado"), "{err}");
        assert!(err.contains("09-multitenancy.md §3"), "{err}");
        assert!(err.contains("TODOS los tenants"), "{err}");
    }

    /// Suscribirse en `strict` sin tenant es un error de CONFIGURACIÓN y falla **antes**
    /// de tocar la red: si esperase al broker, un servicio mal configurado arrancaría, se
    /// conectaría y solo fallaría al llegar el primer evento.
    #[tokio::test]
    async fn strict_sin_tenant_falla_antes_de_conectar() {
        // `connect` sí necesitaría broker, así que se comprueba la decisión pura: es la
        // misma que toma `subscribe_with` antes de crear el consumidor.
        let opts = ConnectOptions::new("nats://x", "svc", "produccion", "1.0.0")
            .with_tenant_isolation(TenantIsolation::Strict);
        assert_eq!(opts.tenant_isolation, TenantIsolation::Strict);
        assert!(effective_tenant_filter(None, opts.tenant_id.as_deref()).is_none());
    }

    // ── métricas ──────────────────────────────────────────────────────────────

    #[test]
    fn los_codigos_de_firma_producen_el_outcome_invalid_signature() {
        // 08-observability.md §2.1 distingue `invalid_signature` de `poison` para que un
        // pico de firmas rotas no se confunda con un pico de JSON corrupto: tienen causas
        // y respuestas distintas.
        for code in [
            "MISSING_SIGNATURE",
            "INVALID_SIGNATURE",
            "UNKNOWN_SIGNING_KEY",
        ] {
            assert!(is_signature_code(code), "{code}");
        }
        for code in ["MALFORMED_JSON", "HTTP_503", "PEDIDO_YA_CANCELADO"] {
            assert!(!is_signature_code(code), "{code}");
        }
    }

    /// Un fallo de esquema muere con `dlqreason = permanent` pero se cuenta como
    /// `outcome = invalid_schema`: "un productor publica payloads que violan su contrato" y
    /// "mi lógica rechazó el evento" son dos preguntas con dos dueños distintos
    /// — 08-observability.md §2.1.
    #[test]
    fn los_codigos_de_esquema_producen_el_outcome_invalid_schema() {
        for code in [
            crate::errors::INVALID_SCHEMA_CODE,
            crate::errors::SCHEMA_NOT_FOUND_CODE,
        ] {
            assert!(is_schema_code(code), "{code}");
            assert!(!is_signature_code(code), "{code}");
        }
        for code in ["MALFORMED_JSON", "HTTP_503", "PEDIDO_YA_CANCELADO"] {
            assert!(!is_schema_code(code), "{code}");
        }
    }

    /// El default del sondeo es el de la spec, y `Duration::ZERO` lo desactiva
    /// — 08-observability.md §2.3.
    #[test]
    fn el_sondeo_de_pendientes_viene_activado_por_defecto() {
        let opts = ConnectOptions::new("nats://x", "svc", "produccion", "1.0.0");
        assert_eq!(opts.pending_poll, DEFAULT_PENDING_POLL);
        assert_eq!(DEFAULT_PENDING_POLL, Duration::from_secs(15));

        let sin_sondeo = ConnectOptions::new("nats://x", "svc", "produccion", "1.0.0")
            .with_pending_poll(Duration::ZERO);
        assert!(sin_sondeo.pending_poll.is_zero());
    }

    #[test]
    fn la_razon_de_dlq_se_traduce_al_outcome_correspondiente() {
        for (reason, outcome) in [
            (DlqReason::Retryable, ConsumeOutcome::Retryable),
            (DlqReason::Permanent, ConsumeOutcome::Permanent),
            (DlqReason::Poison, ConsumeOutcome::Poison),
        ] {
            assert_eq!(ConsumeOutcome::from(reason), outcome);
        }
    }

    #[test]
    fn el_default_de_metricas_es_no_op() {
        let opts = ConnectOptions::new("nats://x", "svc", "produccion", "1.0.0");
        assert!(
            opts.metrics.is_none(),
            "un SDK no debe imponer un backend de métricas"
        );
    }

    #[test]
    fn base64_estandar() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"foob"), "Zm9vYg==");
        assert_eq!(base64(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64(&[0xFF, 0xFE, 0xFD]), "//79");
    }

    // ── credenciales — 06-security.md §2 ──────────────────────────────────────

    use crate::security::TmpDir;

    /// Un `.creds` con la forma que escribe `nsc`: JWT de usuario + semilla `SU…`.
    fn creds_validas() -> String {
        let seed = nkeys::KeyPair::new_user().seed().unwrap();
        format!(
            "-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.e30.AAAA\n\
             ------END NATS USER JWT------\n\n\
             ************************* IMPORTANT *************************\n\
             -----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n"
        )
    }

    /// **Verificado a nivel unitario, NO contra un broker en modo operador/JWT**: el `.creds`
    /// se lee, se parsea y se cablea al cliente sin tocar la red. Que el broker acepte el JWT
    /// y la firma del nonce es lo que un test de integración con un operador real probaría.
    #[tokio::test]
    async fn un_creds_bien_formado_se_cablea_sin_tocar_la_red() {
        let dir = TmpDir::new();
        let ok = dir.file("ok.creds", &creds_validas());
        let r = apply_credentials(
            async_nats::ConnectOptions::new(),
            &Credentials::CredsFile(ok),
        )
        .await;
        assert!(r.is_ok(), "{:?}", r.err());
    }

    #[tokio::test]
    async fn un_creds_mal_formado_es_invalid_credentials() {
        let dir = TmpDir::new();
        for (nombre, contenido) in [
            ("vacio.creds", ""),
            ("basura.creds", "esto no es un .creds"),
            (
                "sin-semilla.creds",
                "-----BEGIN NATS USER JWT-----\neyJ.e30.AAAA\n------END NATS USER JWT------\n",
            ),
        ] {
            let p = dir.file(nombre, contenido);
            let err = apply_credentials(
                async_nats::ConnectOptions::new(),
                &Credentials::CredsFile(p),
            )
            .await
            .unwrap_err();
            assert!(
                matches!(err, FluxError::InvalidCredentials(_)),
                "{nombre}: {err:?}"
            );
            assert_eq!(err.code(), Some("INVALID_CREDENTIALS"));
            assert!(err.to_string().contains(".creds"), "{err}");
        }
        let err = apply_credentials(
            async_nats::ConnectOptions::new(),
            &Credentials::CredsFile("/no/existe.creds".into()),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, FluxError::InvalidCredentials(_)), "{err:?}");
    }

    /// Las cinco formas se cablean; las dos de nkey validan la semilla antes de aceptarla.
    #[tokio::test]
    async fn las_credenciales_simples_se_cablean() {
        let seed = nkeys::KeyPair::new_user().seed().unwrap();
        let dir = TmpDir::new();
        let fichero = dir.file("seed.nk", &format!("{seed}\n"));
        for c in [
            Credentials::None,
            Credentials::Token("t".into()),
            Credentials::UserPassword {
                user: "u".into(),
                password: "p".into(),
            },
            Credentials::NkeySeed(format!("  {seed}  ")),
            Credentials::NkeyFile(fichero),
        ] {
            assert!(
                apply_credentials(async_nats::ConnectOptions::new(), &c)
                    .await
                    .is_ok(),
                "{c:?}"
            );
        }
        let cuenta = nkeys::KeyPair::new_account().seed().unwrap();
        for malo in [
            Credentials::NkeySeed("SUMAL".into()),
            Credentials::NkeySeed(cuenta),
            Credentials::NkeyFile(dir.file("malo.nk", "basura")),
            Credentials::NkeyFile("/no/existe.nk".into()),
        ] {
            let err = apply_credentials(async_nats::ConnectOptions::new(), &malo)
                .await
                .unwrap_err();
            assert!(
                matches!(err, FluxError::InvalidCredentials(_)),
                "{malo:?}: {err:?}"
            );
        }
    }

    /// Un valor en blanco no es «sin credencial»: es una variable de entorno sin definir. Se
    /// rechaza antes de tocar la red —como en Node— y sin volcar el valor. Y lo contrario: un
    /// valor con contenido, aunque lleve espacios dentro, se acepta.
    #[tokio::test]
    async fn un_token_usuario_o_contrasena_en_blanco_es_invalid_credentials() {
        for malo in [
            Credentials::Token(String::new()),
            Credentials::Token("   ".into()),
            Credentials::UserPassword {
                user: String::new(),
                password: "pw".into(),
            },
            Credentials::UserPassword {
                user: "ana".into(),
                password: " \t".into(),
            },
        ] {
            let err = apply_credentials(async_nats::ConnectOptions::new(), &malo)
                .await
                .unwrap_err();
            assert!(
                matches!(err, FluxError::InvalidCredentials(_)),
                "{malo:?}: {err:?}"
            );
            assert_eq!(err.code(), Some("INVALID_CREDENTIALS"));
            assert!(err.to_string().contains("Credentials::None"), "{err}");
        }
        for bueno in [
            Credentials::Token("tok en".into()),
            Credentials::UserPassword {
                user: "ana".into(),
                password: "con espacio".into(),
            },
        ] {
            assert!(
                apply_credentials(async_nats::ConnectOptions::new(), &bueno)
                    .await
                    .is_ok(),
                "{bueno:?}"
            );
        }
    }

    /// Un `{:?}` de la configuración acaba en un log: ningún secreto puede salir en él.
    #[test]
    fn el_debug_de_las_credenciales_no_imprime_secretos() {
        let seed = nkeys::KeyPair::new_user().seed().unwrap();
        for c in [
            Credentials::Token("tok-secreto".into()),
            Credentials::NkeySeed(seed.clone()),
            Credentials::UserPassword {
                user: "ana".into(),
                password: "pw-secreta".into(),
            },
        ] {
            let texto = format!("{c:?}");
            for secreto in ["tok-secreto", "pw-secreta", seed.as_str()] {
                assert!(!texto.contains(secreto), "{texto}");
            }
            assert!(texto.contains("oculto"), "{texto}");
        }
        // El usuario no es un secreto y ayuda a depurar.
        let texto = format!(
            "{:?}",
            Credentials::UserPassword {
                user: "ana".into(),
                password: "x".into()
            }
        );
        assert!(texto.contains("ana"));
    }

    #[test]
    fn el_backoff_de_conexion_crece_con_jitter_y_tiene_techo() {
        let primero = connect_backoff(1);
        assert!(primero >= Duration::from_millis(100) && primero < Duration::from_millis(350));
        // El techo: 5 s de base + hasta 250 ms de jitter, por muchos intentos que lleve.
        for intento in [7, 20, u32::MAX] {
            let d = connect_backoff(intento);
            assert!(
                d >= Duration::from_secs(5) && d < Duration::from_millis(5250),
                "{d:?}"
            );
        }
        // Sin jitter, mil servicios reconectarían en el mismo milisegundo.
        let distintos: std::collections::HashSet<_> = (0..50).map(|_| connect_backoff(3)).collect();
        assert!(distintos.len() > 1, "el jitter no varía");
    }

    /// `with_tls` y las credenciales no se imprimen enteras ni arrastran secretos al `Debug`.
    #[test]
    fn la_config_con_tls_se_describe_sin_secretos() {
        let o = ConnectOptions::new("nats://x", "svc", "produccion", "1.0.0")
            .with_credentials(Credentials::Token("tok-secreto".into()))
            .with_tls(TlsOptions::new().with_ca_file("/etc/ca.pem"));
        let texto = format!("{o:?}");
        assert!(!texto.contains("tok-secreto"), "{texto}");
        assert!(texto.contains("/etc/ca.pem"), "{texto}");
    }

    #[tokio::test]
    async fn connect_exige_los_campos_del_envelope() {
        for opts in [
            ConnectOptions::new("nats://x", "", "produccion", "1.0.0"),
            ConnectOptions::new("nats://x", "svc", "", "1.0.0"),
            ConnectOptions::new("nats://x", "svc", "produccion", ""),
        ] {
            assert!(matches!(connect(opts).await, Err(FluxError::Config(_))));
        }
    }

    /// protocol.json → naming.service: el SDK DEBE validar el nombre de servicio en
    /// connect(), no al construir el durable.
    #[tokio::test]
    async fn connect_valida_el_nombre_de_servicio() {
        let opts = ConnectOptions::new("nats://x", "FacturacionAPI", "produccion", "1.0.0");
        let err = connect(opts).await.unwrap_err();
        assert!(
            matches!(err, FluxError::InvalidServiceName { .. }),
            "{err:?}"
        );
    }
}
