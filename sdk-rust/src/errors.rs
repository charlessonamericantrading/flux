//! Taxonomía de errores de flux.
//!
//! Contrato normativo: `specification/04-errors.md`
//!
//! El error más caro de un sistema de eventos no es perder un mensaje: es reintentar
//! durante 51 minutos algo que nunca va a funcionar mientras los eventos sanos se
//! acumulan detrás. Por eso flux no tiene "una política de reintentos": tiene una
//! **taxonomía** de tres clases, y cada clase determina una acción distinta sobre el
//! mensaje de NATS.

use std::error::Error as StdError;
use std::fmt;
use std::time::Duration;

use thiserror::Error;

use crate::protocol::RESERVED_DOMAINS;

/// Error de aplicación devuelto por un handler.
///
/// Es el `Box<dyn Error>` idiomático y no un tipo propio a propósito: el handler debe
/// poder devolver el error de *su* dominio (`sqlx::Error`, `reqwest::Error`, uno propio)
/// sin envolverlo, y el clasificador lo recorre con `source()` — el equivalente de
/// `errors.As` de Go. Ver [`crate::classify`].
pub type HandlerError = Box<dyn StdError + Send + Sync + 'static>;

/// Lo que devuelve un handler. `Ok(())` **es** el ack explícito.
pub type HandlerResult = Result<(), HandlerError>;

// ─── Las tres clases ─────────────────────────────────────────────────────────

/// Una de las tres clases del protocolo — 04-errors.md §1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorClass {
    /// El fallo es del entorno y podría desaparecer solo.
    /// → `nak(delay)` y reintento con el backoff canónico.
    Retryable,

    /// El evento es válido pero este consumidor nunca podrá procesarlo por mucho que
    /// espere. → `term()` + DLQ inmediato, sin reintentos.
    Permanent,

    /// El mensaje ni siquiera es interpretable. → `term()` + DLQ + alerta.
    ///
    /// Lo detecta el SDK **antes** del handler; casi siempre significa que un productor
    /// está roto. Es el único caso que **DEBE** despertar a alguien.
    Poison,
}

impl ErrorClass {
    /// Valor que acaba escrito en la extensión `dlqreason` — 04-errors.md §3.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
            Self::Poison => "poison",
        }
    }
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Resultado de clasificar un error: lo que el runtime del consumidor consume para
/// decidir entre `nak`, `term` y alerta.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classification {
    /// Determina la acción sobre el mensaje.
    pub class: ErrorClass,

    /// Código estable para métricas y alertas (`HTTP_503`, `PEDIDO_YA_CANCELADO`).
    /// El mensaje del error cambia; el código no debería.
    pub code: String,

    /// Solo aplica a [`ErrorClass::Retryable`]: **sugerencia para el PRIMER reintento**.
    /// Úsala cuando la dependencia dice explícitamente cuánto esperar (cabecera
    /// `Retry-After`). `None` significa "usa el backoff canónico".
    ///
    /// ⚠️ **No sobrescribe el calendario de reintentos** — 03-delivery.md §2.2. Con
    /// `backoff` configurado —y flux lo configura siempre—, JetStream honra el delay de un
    /// `nak` solo en la primera reentrega; a partir de la segunda manda el array `backoff`
    /// y el delay pedido se ignora **sin ningún aviso**. Un `Retry-After: 5` acorta el
    /// primer reintento y nada más. No construyas lógica que dependa de lo contrario.
    pub retry_after: Option<Duration>,

    /// Solo aplica a [`ErrorClass::Retryable`]: entregas máximas para ESTE error, por
    /// debajo del `max_deliver` del consumidor. `None` significa "sin tope propio", es
    /// decir, manda el del consumidor.
    ///
    /// Existe porque `max_deliver` es **por consumidor, no por mensaje**: bajarlo a 2
    /// para acotar los errores desconocidos recortaría también los reintentos de los
    /// que sí sabemos transitorios (`ECONNRESET`, HTTP 503), que deben conservar sus 6
    /// intentos — 04-errors.md §2.1.
    ///
    /// ⚠️ Divergencia con Go, y a favor de Rust: allí es un `int` con `0 == "sin tope"`
    /// porque un `*int` añadiría una asignación y un alias mutable a un struct que se
    /// copia en cada despacho. `Option<u32>` no tiene ese coste y expresa exactamente
    /// lo que el protocolo dice: ausente ≠ cero (01-envelope.md §3.3).
    pub max_attempts: Option<u32>,
}

impl Classification {
    /// Clasificación mínima: clase y código, sin tope ni retraso propios.
    #[must_use]
    pub fn new(class: ErrorClass, code: impl Into<String>) -> Self {
        Self {
            class,
            code: code.into(),
            retry_after: None,
            max_attempts: None,
        }
    }

    /// Fija el retraso explícito del siguiente intento.
    #[must_use]
    pub fn with_retry_after(mut self, delay: Option<Duration>) -> Self {
        self.retry_after = delay;
        self
    }

    /// Fija el presupuesto de entregas de ESTE error.
    #[must_use]
    pub fn with_max_attempts(mut self, attempts: Option<u32>) -> Self {
        self.max_attempts = attempts;
        self
    }
}

// ─── Detalle común de los errores clasificados ───────────────────────────────

/// Cuerpo de un error que declara su propia clase de flux.
///
/// Los tres tipos del protocolo comparten forma, así que comparten struct en vez de
/// triplicar `RetryableError` / `PermanentError` / `PoisonError` como hacen Node y Go.
/// La clase la lleva la variante de [`FluxError`], no el struct, para que sea imposible
/// construir un "retryable con clase permanent".
#[derive(Debug)]
pub struct Failure {
    /// Texto legible del fallo.
    pub message: String,
    /// Código estable para métricas. `None` cae al nombre de la clase.
    pub code: Option<String>,
    /// Solo lo consume la clase RETRYABLE; en las otras se ignora, igual que en Node.
    pub retry_after: Option<Duration>,
    /// Error subyacente, recuperable con [`std::error::Error::source`].
    pub source: Option<HandlerError>,
}

impl Failure {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: None,
            retry_after: None,
            source: None,
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for Failure {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_ref()
            .map(|e| &**e as &(dyn StdError + 'static))
    }
}

// ─── Errores de validación L3 ────────────────────────────────────────────────

/// Código estable de un payload que incumple su esquema. Etiqueta `code` de
/// `flux_events_dlq_total` y motivo del `outcome="invalid_schema"`
/// — 08-observability.md §2.1.
pub const INVALID_SCHEMA_CODE: &str = "INVALID_SCHEMA";

/// Código estable de un `dataschema` que no está en el bundle desplegado.
///
/// Es un código distinto de [`INVALID_SCHEMA_CODE`] a propósito: "el payload está mal" y
/// "el servicio se desplegó con un bundle viejo" son dos incidentes con dos respuestas
/// distintas, y mezclarlos bajo una etiqueta común obliga a leer mensajes de error para
/// distinguirlos.
pub const SCHEMA_NOT_FOUND_CODE: &str = "SCHEMA_NOT_FOUND";

/// El payload no cumple el JSON Schema que su propio `dataschema` declara — L3.
///
/// Vive en la taxonomía y no en el módulo `validation` (que solo existe con la feature del mismo
/// nombre, y por eso no se enlaza: sin ella el enlace no resolvería) porque es un error del protocolo:
/// se puede construir, propagar y clasificar sin la feature `validation` activada, que
/// solo hace falta para *detectarlo*.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaValidationError {
    /// Subject del evento que no validó.
    pub subject: String,
    /// URI del esquema contra el que se validó.
    pub dataschema: String,
    /// **TODOS** los incumplimientos, no solo el primero.
    ///
    /// Reportarlos de uno en uno convierte arreglar un payload con tres campos mal en
    /// tres despliegues, que es exactamente el coste que L3 existe para evitar
    /// — 00-protocol.md §5.
    pub errors: Vec<String>,
}

impl fmt::Display for SchemaValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "el payload de `{}` no cumple su esquema ({}):",
            self.subject, self.dataschema
        )?;
        for e in &self.errors {
            write!(f, "\n  · {e}")?;
        }
        Ok(())
    }
}

impl StdError for SchemaValidationError {}

/// El `dataschema` del evento no está en el bundle desplegado con el servicio — L3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaNotFoundError {
    /// Subject del evento.
    pub subject: String,
    /// URI que se buscó en el bundle.
    pub dataschema: String,
}

impl fmt::Display for SchemaNotFoundError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "no hay esquema para `{}` ({}) en el bundle. Regenéralo con \
             `node scripts/bundle-schemas.mjs` y despliégalo con el servicio, o baja el modo \
             de validación a `warn`",
            self.subject, self.dataschema
        )
    }
}

impl StdError for SchemaNotFoundError {}

// ─── Errores de aprovisionamiento y de permisos ──────────────────────────────

/// Código estable de un stream o consumidor que no existe y que este servicio no puede
/// crear — 06-security.md §3.
pub const NOT_PROVISIONED_CODE: &str = "NOT_PROVISIONED";

/// Código estable de una operación que el broker denegó por permisos — 06-security.md §3.
pub const PERMISSION_DENIED_CODE: &str = "PERMISSION_DENIED";

/// Código estable de una conexión inicial que el broker rechazó por algo que reintentar no
/// arregla (credencial rechazada, TLS que no encaja) — 04-errors.md §5.3.
pub const CONNECTION_REJECTED_CODE: &str = "CONNECTION_REJECTED";

/// Código estable de unas credenciales mal formadas, detectadas **antes** de abrir ningún
/// socket — 04-errors.md §5.3.
pub const INVALID_CREDENTIALS_CODE: &str = "INVALID_CREDENTIALS";

/// Código estable de unas opciones TLS mal formadas, detectadas **antes** de abrir ningún
/// socket — 04-errors.md §5.3.
pub const INVALID_TLS_OPTIONS_CODE: &str = "INVALID_TLS_OPTIONS";

// ─── Los siete códigos de la capacidad `state` — 04-errors.md §6 ─────────────

/// Código estable: el contrato de un estado no es `<dominio>.<agregado>.v<major>` —
/// 04-errors.md §6. **PERMANENT.** Es el séptimo: la lista de seis no tenía dónde poner un
/// contrato mal formado, y este SDK lanzaba un `InvalidSubject` sin código de estado.
pub const INVALID_STATE_CONTRACT_CODE: &str = "INVALID_STATE_CONTRACT";

/// Código estable: el tenant de un estado no sirve, o no se pudo resolver ninguno —
/// 04-errors.md §6. **PERMANENT.**
pub const INVALID_TENANT_FOR_STATE_CODE: &str = "INVALID_TENANT_FOR_STATE";

/// Código estable: la clave de un estado no cumple `^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$` —
/// 04-errors.md §6. **PERMANENT.**
pub const INVALID_STATE_KEY_CODE: &str = "INVALID_STATE_KEY";

/// Código estable: `data` no lleva un `aggregateVersion` entero ≥ 1 y ≤ 2^53 − 1 —
/// 04-errors.md §6. **PERMANENT**, y también al consumir.
pub const MISSING_AGGREGATE_VERSION_CODE: &str = "MISSING_AGGREGATE_VERSION";

/// Código estable: el sobre de una entrada de estado no coincide con el subject de NATS en el
/// que llegó — 04-errors.md §6. **POISON**: el mensaje es legible pero miente sobre su identidad.
pub const STATE_SUBJECT_MISMATCH_CODE: &str = "STATE_SUBJECT_MISMATCH";

/// Código estable: el chequeo de secuencia de `put`/`delete` agotó sus 3 intentos —
/// 04-errors.md §6. **RETRYABLE**: el conflicto es del entorno, no de la llamada.
pub const STATE_CONFLICT_CODE: &str = "STATE_CONFLICT";

/// Código estable: un subject o un contrato usa un dominio reservado (`dlq`, `state`, `rpc`,
/// `system`) — 04-errors.md §6. **PERMANENT.**
pub const RESERVED_DOMAIN_CODE: &str = "RESERVED_DOMAIN";

/// Por qué el broker rechazó la conexión inicial: el `reason` de
/// [`FluxError::ConnectionRejected`]. Los mismos dos valores que `reason` en el SDK de Node.
///
/// Son dos y no más a propósito: es lo que separa a quien tiene que arreglar algo. Con
/// [`Self::Auth`] se mira el usuario y su credencial en el broker; con [`Self::Tls`], los
/// certificados y la CA. Distinguir `tls` de «TLS que el broker no ofrece» o de «certificado
/// de cliente que falta» no lo permite la librería de NATS (04-errors.md §5.3): esos matices
/// viajan en el mensaje, no en el motivo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionReason {
    /// El broker rechazó la credencial del servicio (`Authorization Violation`).
    Auth,
    /// El handshake TLS no se pudo completar: el certificado del broker no verifica, el
    /// broker no ofrece TLS y se exigió, o exige un certificado de cliente que no hay.
    Tls,
}

impl RejectionReason {
    /// `"auth"` o `"tls"`, tal como los escribe el SDK de Node en `reason`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auth => "auth",
            Self::Tls => "tls",
        }
    }
}

impl fmt::Display for RejectionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Qué recurso de JetStream falta. Es un tipo de flux y no el de NATS: la API pública no
/// expone el broker (00-protocol.md §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKind {
    /// Un stream (`EVT_<DOMINIO>`, `DLQ_<DOMINIO>`).
    Stream,
    /// Un consumidor durable.
    Consumer,
}

impl fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Stream => "stream",
            Self::Consumer => "consumidor",
        })
    }
}

/// Un campo en el que el servidor no honró lo solicitado.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigDifference {
    /// Nombre del campo en la API de JetStream (`ack_wait`, `backoff`…).
    pub field: String,
    /// Lo que el SDK pidió.
    pub requested: String,
    /// Lo que el servidor aplicó.
    pub effective: String,
}

// ─── El error del SDK ────────────────────────────────────────────────────────

/// Todo error que produce o acepta este SDK.
///
/// Las tres primeras variantes son la **taxonomía del protocolo** y la aplicación las
/// usa para anular al clasificador. El resto son fallos locales del SDK: un subject mal
/// escrito o un envelope que no se puede construir son bugs del productor, no mensajes
/// ajenos corruptos.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FluxError {
    /// El fallo es del entorno y podría desaparecer solo. Lánzalo cuando **sabes** que
    /// es transitorio: timeout de red, 503 de un proveedor, deadlock de BD.
    #[error("{0}")]
    Retryable(#[source] Failure),

    /// El evento es válido pero tu lógica lo rechaza de forma definitiva. Reintentarlo
    /// son 51 minutos de cola bloqueada para llegar al mismo sitio.
    #[error("{0}")]
    Permanent(#[source] Failure),

    /// El mensaje no pudo interpretarse como CloudEvent. Lo produce el SDK, no la
    /// aplicación: el handler nunca llega a verlo — 04-errors.md §1.3.
    #[error("{0}")]
    Poison(#[source] Failure),

    /// Un subject que no cumple 02-naming.md §1.
    #[error("subject inválido `{subject}`: {reason}")]
    InvalidSubject {
        /// El subject rechazado.
        subject: String,
        /// Por qué se rechazó.
        reason: String,
    },

    /// Un nombre de servicio que no cumple `naming.service` de `protocol.json`.
    #[error("nombre de servicio inválido `{service}`: {reason}")]
    InvalidServiceName {
        /// El nombre rechazado.
        service: String,
        /// Por qué se rechazó.
        reason: String,
    },

    /// Un envelope que el SDK se niega a construir o serializar. A diferencia de
    /// [`FluxError::Poison`], se produce del lado del **productor**.
    #[error("{0}")]
    Envelope(String),

    /// Configuración de `connect()` incompleta o incoherente.
    #[error("{0}")]
    Config(String),

    /// El payload no cumple su `dataschema` — requisito **L3**, 00-protocol.md §5.
    ///
    /// Al **publicar** hace fallar el `publish()`: es lo que convierte un contrato roto en
    /// un fallo del servicio que lo generó, en vez de un misterio que aparece en un
    /// consumidor de otro equipo la semana que viene.
    ///
    /// Al **consumir** se clasifica PERMANENT: el evento es sintácticamente correcto pero
    /// incumple su contrato, así que reintentarlo dará exactamente el mismo resultado
    /// — 04-errors.md §1.2.
    #[error("{0}")]
    SchemaValidation(#[source] SchemaValidationError),

    /// El `dataschema` del evento no está en el bundle. Se clasifica igual que
    /// [`FluxError::SchemaValidation`] y por la misma razón: reintentar no lo arregla.
    #[error("{0}")]
    SchemaNotFound(#[source] SchemaNotFoundError),

    /// Material de clave o configuración de firma inválidos — 07-signing.md.
    ///
    /// Es un error de **arranque**, no de mensaje: se produce al construir el firmante o
    /// el verificador, antes de tocar la red. Un evento con la firma rota no da esto, da
    /// [`FluxError::Poison`] con `INVALID_SIGNATURE`.
    #[error("{0}")]
    Signing(String),

    /// `tenant_isolation = strict` y una suscripción sin filtro de tenant.
    ///
    /// Requisito de 09-multitenancy.md §3: consumir sin filtro **DEBE** ser un error de
    /// configuración, no un descuido silencioso. Un filtro que hay que acordarse de poner
    /// es un filtro que alguien olvidará, y el fallo —ver los datos de otro tenant— no
    /// produce ningún error visible: produce un incidente de privacidad que se descubre
    /// semanas después.
    #[error(
        "tenant_isolation = strict, pero {reason} al suscribirse a `{subject}`. Sin filtro \
         de tenant este consumidor vería los eventos de TODOS los tenants, y eso no produce \
         ningún error (09-multitenancy.md §3)"
    )]
    TenantIsolation {
        /// Subject al que se intentaba suscribir.
        subject: String,
        /// Qué falta exactamente.
        reason: String,
    },

    /// El servidor aplicó una configuración de consumidor distinta de la solicitada.
    ///
    /// Requisito **L2** — 03-delivery.md §2.1. Es la ÚNICA defensa contra la
    /// sobrescritura silenciosa de `ack_wait` por `backoff[0]`: JetStream acepta la
    /// petición, no avisa, y devuelve otra cosa. Sin esta comprobación un handler de más
    /// de un segundo se ejecuta en concurrencia consigo mismo bajo carga y nada lo indica.
    #[error("{}", format_mismatch(.durable, .differences))]
    ConsumerConfigMismatch {
        /// Durable afectado.
        durable: String,
        /// Campos en los que la config efectiva difiere de la solicitada.
        differences: Vec<ConfigDifference>,
    },

    /// Falta un stream o un consumidor y el broker deniega crearlo — 06-security.md §3.
    ///
    /// **No es un timeout.** En producción streams y consumidores los aprovisiona la
    /// infraestructura (`flux provision`) y la ACL del servicio no concede crearlos: con un
    /// recurso ausente, la petición de creación se deniega, el broker no responde y
    /// `async-nats` solo lo cuenta como "timed out" a los 5–30 s. El SDK escucha las
    /// denegaciones y falla al instante, nombrando el recurso que falta.
    ///
    /// Se clasifica **PERMANENT**: reintentar no hace aparecer el recurso, igual que un 403
    /// no se arregla esperando (04-errors.md §1.2).
    #[error("{}", format_not_provisioned(*.kind, .name, .denied))]
    NotProvisioned {
        /// Qué falta.
        kind: ResourceKind,
        /// Su nombre (`EVT_PEDIDOS`, `facturacion-api__pedidos_pedido_v1_creado`).
        name: String,
        /// La denegación literal del broker (`Publish to "$JS.API.STREAM.CREATE.…"`).
        ///
        /// **Vacía** cuando el SDK no intentó crear el recurso —el estado replicado nunca crea
        /// su stream—: el mensaje dice entonces que no contesta nadie, en vez de «el broker
        /// denegó …».
        denied: String,
    },

    /// El broker denegó una operación por permisos — 06-security.md §3.
    ///
    /// Es la otra cara de [`FluxError::NotProvisioned`]: aquí ni siquiera hay permiso para
    /// *mirar* si el recurso existe, o para publicar en un subject. El caso típico es una
    /// ACL de la v1, que no concede nada de `$JS.API.*` ni un inbox privado: el SDK moderno
    /// no puede ni consultar su stream, y sin esta detección el síntoma es un timeout que no
    /// dice nada.
    ///
    /// **PERMANENT**, por la misma razón que [`FluxError::NotProvisioned`].
    #[error("{}", format_permission_denied(.operation, .denied))]
    PermissionDenied {
        /// Lo que el SDK intentaba, en términos de flux (`abrir el stream EVT_PEDIDOS`).
        operation: String,
        /// La denegación literal del broker.
        denied: String,
    },

    /// El broker rechazó la **conexión inicial** por algo que reintentar no arregla —
    /// 04-errors.md §5.3 y 03-delivery.md §6.1.
    ///
    /// `connect()` espera a un broker que aún no ha arrancado, pero no a esto: una credencial
    /// que el broker no acepta o un handshake TLS que no encaja no cambian por esperar, y un
    /// servicio que los reintentara para siempre arrancaría, pasaría su healthcheck y se
    /// quedaría mudo (medido, A-30). Por eso se devuelve al instante, con la causa.
    ///
    /// **PERMANENT**: es el 403 de la conexión. A diferencia de [`FluxError::NotProvisioned`]
    /// no nace en un handler, pero la clase significa lo mismo: reintentar da el mismo fallo.
    ///
    /// Cubre **solo** la primera conexión. Qué hacer si el broker rechaza la credencial en una
    /// *re*conexión (caducada, rotada) no está medido y la spec no lo fija.
    #[error("{}", format_connection_rejected(*.reason, .servers, .detail))]
    ConnectionRejected {
        /// Qué se rechazó: la credencial o el TLS.
        reason: RejectionReason,
        /// La lista de servidores a la que se intentaba conectar.
        servers: String,
        /// La causa tal como la cuenta la librería (`invalid peer certificate: UnknownIssuer`).
        /// No lleva credenciales: es el texto del error de TLS o de autorización.
        detail: String,
        /// El error de `async-nats` que lo causó. Nunca se expone su tipo en la firma.
        #[source]
        source: HandlerError,
    },

    /// Credenciales mal formadas, detectadas **antes** de abrir ningún socket —
    /// 04-errors.md §5.3.
    ///
    /// Es un error de **configuración del servicio**, no del broker: un valor vacío, un
    /// `.creds` ilegible o sin JWT y semilla, una semilla que no es de usuario (`SU…`). Se
    /// distingue de [`FluxError::ConnectionRejected`] —credencial **bien formada** que el
    /// broker no acepta— porque lo que hay que revisar es distinto: aquello es lo que se
    /// pasó a `connect()`, esto es lo que hay en el broker.
    ///
    /// **PERMANENT.** El texto nunca incluye el valor de una credencial: acaba en logs.
    #[error("credenciales inválidas en connect(): {0}")]
    InvalidCredentials(String),

    /// Opciones TLS mal formadas, detectadas **antes** de abrir ningún socket —
    /// 04-errors.md §5.3.
    ///
    /// Un fichero ilegible o que no es PEM, o un certificado de cliente sin su clave (o al
    /// revés). Sin esta comprobación `async-nats` los lee dentro de su bucle de conexión, y ahí
    /// un fichero equivocado es un error más que se reintenta. Un certificado que el broker no
    /// acepta no es esto: es [`FluxError::ConnectionRejected`] con [`RejectionReason::Tls`].
    ///
    /// **PERMANENT.**
    #[error("opciones TLS inválidas en connect(): {0}")]
    InvalidTlsOptions(String),

    // ─── Estado replicado — 10-state.md, 04-errors.md §6 ─────────────────────────
    //
    // Los siete son variantes propias y no un `Permanent(Failure)` con `with_code(…)` por la misma
    // razón que `NotProvisioned`: el código es el que agrupa el panel
    // (`flux_events_dlq_total{code}`) y **la aplicación no puede sustituirlo**.
    /// El contrato de un estado no es `<dominio>.<agregado>.v<major>` — 10-state.md §2.2.
    ///
    /// El subject del estado sin `state.`, sin tenant y sin clave, en minúsculas y `kebab-case`:
    /// `clientes.cliente.v1`. Son tres tokens y el último es `v` y un entero ≥ 1. Un dominio
    /// reservado **no** es este error: es [`FluxError::ReservedDomain`], que se evalúa antes
    /// (10-state.md §3.6). No se cuenta en `flux_state_writes_total`: su valor es la etiqueta
    /// `contract`, y uno mal formado crearía una serie por cada error de tipeo. Falla **antes** de
    /// tocar la red.
    ///
    /// **PERMANENT.**
    #[error("{}", format_invalid_contract(.contract, .reason))]
    InvalidStateContract {
        /// El contrato rechazado.
        contract: String,
        /// Por qué se rechazó.
        reason: String,
    },

    /// El tenant de un estado no sirve, o no se pudo resolver ninguno — 10-state.md §2.4.
    ///
    /// A diferencia de un evento, un estado **no** cae en `system` por defecto: aquí el tenant
    /// es una dirección (va en el subject), y un tenant equivocado no es una etiqueta mal puesta
    /// sino un dato guardado en otro sitio. Se resuelve de la opción de la llamada, del evento en
    /// curso y del `tenant_id` de `connect()`; si no sale ninguno, es este error. Falla **antes**
    /// de tocar la red.
    ///
    /// **PERMANENT.** El texto nunca incluye valores de `data`: acaba en logs.
    #[error("{}", format_invalid_tenant(.tenant.as_deref(), .reason))]
    InvalidTenantForState {
        /// El tenant rechazado, o `None` si no se pudo resolver ninguno.
        tenant: Option<String>,
        /// Por qué no sirve.
        reason: String,
    },

    /// La clave de un estado no cabe en un token de NATS — 10-state.md §2.3.
    ///
    /// El broker **no** valida la clave (A-41): acepta `*`, `>`, `a.b` —que son *dos* tokens, y la
    /// entrada quedaría fuera del filtro de todos los observadores, sin error— y 2000 caracteres.
    /// Por eso la valida el SDK, **antes** de tocar la red, y sin ninguna codificación automática:
    /// dos escapes distintos serían dos claves distintas para el mismo agregado.
    ///
    /// **PERMANENT.**
    #[error("{}", format_invalid_key(.key, .reason))]
    InvalidStateKey {
        /// La clave rechazada. Una clave es un identificador opaco, no PII, y puede citarse.
        key: String,
        /// Por qué se rechazó.
        reason: String,
    },

    /// `data` no lleva un `aggregateVersion` válido — 10-state.md §3.4.
    ///
    /// El nombre cubre todo lo que no sea un entero ≥ 1 y ≤ 2^53 − 1, no solo la ausencia: `null`,
    /// una cadena, un decimal (también `7.0`), `0`, un negativo o uno mayor. Se comprueba
    /// **siempre**, con o sin validación L3: es una regla del protocolo, no del esquema.
    ///
    /// **PERMANENT**, y también al **consumir**: una entrada sin versión válida no se puede
    /// aplicar de forma condicional. El texto **no** incluye el valor recibido.
    #[error("{}", format_missing_version(.reason))]
    MissingAggregateVersion {
        /// Qué le pasa al campo, sin citar su valor.
        reason: String,
    },

    /// El sobre de una entrada de estado no coincide con el subject de NATS en el que llegó —
    /// 10-state.md §5.4.
    ///
    /// El mensaje es legible pero **miente sobre su identidad** (su tenant, su clave o su
    /// `type`), y quien consume no puede saber cuál de los dos dice la verdad: un productor roto
    /// u hostil. **POISON** (`term()` + DLQ de estado + alerta), y la entrada **no** se aplica.
    #[error("{}", format_subject_mismatch(.attribute, .envelope, .subject))]
    StateSubjectMismatch {
        /// El atributo del sobre que discrepa: `tenantid`, `subject` o `type`.
        attribute: String,
        /// Lo que dice el sobre.
        envelope: String,
        /// Lo que dice el subject de NATS (el que decidió el broker).
        subject: String,
    },

    /// El chequeo de secuencia agotó sus intentos — 10-state.md §3.2.
    ///
    /// Alguien escribió la misma clave entre la lectura y la publicación, las tres veces. Es un
    /// error y **no** un tercer resultado de `put`: un conflicto no es ni `ok` ni `stale`, es que
    /// no se sabe. **RETRYABLE**: repetir la llamada relee y suele resolverlo, o acaba en `stale`.
    #[error("{}", format_state_conflict(.key, *.attempts))]
    StateConflict {
        /// La clave en disputa.
        key: String,
        /// Cuántas veces se intentó.
        attempts: u32,
    },

    /// Un subject o un contrato usa un dominio reservado — 10-state.md §2.7.
    ///
    /// `dlq`, `state`, `rpc` y `system` son espacios de nombres del protocolo. Lo rechazan
    /// `publish`, `subscribe`, `put`, `get`, `delete` y `watch`, **antes** de tocar la red.
    /// En particular `publish()` no puede escribir en `state.`: el evento llegaría al stream de
    /// estado **sin** el chequeo de secuencia y sobrescribiría sin condición (A-45).
    ///
    /// **PERMANENT.**
    #[error("{}", format_reserved_domain(.domain, .subject))]
    ReservedDomain {
        /// El dominio reservado.
        domain: String,
        /// El subject o contrato que lo usaba.
        subject: String,
    },

    /// Fallo de transporte: conexión, publicación, creación de stream o de consumidor.
    ///
    /// Una conexión inicial rechazada por la credencial o por el TLS **no** es esto: es
    /// [`FluxError::ConnectionRejected`], con su código estable.
    #[error("{context}")]
    Transport {
        /// Qué se estaba intentando, en términos de flux.
        context: String,
        /// El error de `async-nats` que lo causó. Nunca se expone su tipo en la firma.
        #[source]
        source: HandlerError,
    },
}

fn format_invalid_contract(contract: &str, reason: &str) -> String {
    format!(
        "contrato de estado inválido {contract:?}: {reason}. Un contrato es \
         `<dominio>.<agregado>.v<major>` —el subject del estado sin `state.`, sin tenant y sin \
         clave—, en minúsculas y kebab-case: `clientes.cliente.v1` (10-state.md §2.2)"
    )
}

fn format_invalid_tenant(tenant: Option<&str>, reason: &str) -> String {
    match tenant {
        Some(t) => format!(
            "el tenant {t:?} no sirve para un estado: {reason}. Debe cumplir \
             ^[a-z0-9]+(-[a-z0-9]+)*$ con 64 caracteres como máximo (el tenant de un evento es \
             cualquier cadena; el de un estado, no: va en el subject de NATS — 10-state.md §2.4). \
             Pásalo en la opción `tenant_id` de la llamada o en `ConnectOptions::with_tenant_id`"
        ),
        None => format!(
            "no se pudo resolver el tenant del estado: {reason}. En un estado NO hay tenant por \
             defecto, ni `system` implícito: el tenant es una dirección y un tenant equivocado es \
             un dato guardado en otro sitio (10-state.md §2.4). Pásalo en la opción `tenant_id` de \
             la llamada o en `ConnectOptions::with_tenant_id`; quien quiera `system`, que lo diga"
        ),
    }
}

fn format_invalid_key(key: &str, reason: &str) -> String {
    format!(
        "clave de estado inválida {key:?}: {reason}. Debe cumplir \
         ^[A-Za-z0-9][A-Za-z0-9_-]{{0,127}}$ —un único token de NATS: sin puntos, sin comodines, \
         sin espacios, de 1 a 128 caracteres, sin `_` inicial— y NO se codifica sola: el broker no \
         valida la clave y una clave con un punto son dos tokens, con lo que la entrada quedaría \
         fuera del filtro de todos los observadores sin ningún error (10-state.md §2.3). Una clave \
         es un identificador opaco (`cli-1`), nunca un email"
    )
}

fn format_missing_version(reason: &str) -> String {
    format!(
        "`aggregateVersion` inválido en `data`: {reason}. Es obligatorio y debe ser un entero ≥ 1 \
         y ≤ 2^53 − 1 (9007199254740991): lo pone el dueño del agregado —su contador monotónico, el \
         mismo de sus eventos—, no el SDK. `7.5` y `7.0` no valen, ni una cadena (`\"7\"`) \
         (10-state.md §3.4)"
    )
}

fn format_subject_mismatch(attribute: &str, envelope: &str, subject: &str) -> String {
    format!(
        "la entrada de estado miente sobre su identidad: el atributo `{attribute}` del sobre vale \
         {envelope:?} y el subject de NATS dice {subject:?}. La identidad de una entrada está en el \
         subject (el tenant y la clave), y el sobre la repite; si discrepan uno de los dos miente y \
         no se sabe cuál, así que NO se aplica: un productor roto u hostil (10-state.md §5.4)"
    )
}

fn format_state_conflict(key: &str, attempts: u32) -> String {
    format!(
        "el chequeo de secuencia de la clave {key:?} agotó sus {attempts} intentos: otro escritor \
         modificó la misma clave entre la lectura y la publicación las {attempts} veces. Es \
         RETRYABLE: repetir la llamada relee la clave y suele resolverlo, o acaba en `stale` si el \
         otro escribió una versión mayor. Si ocurre a menudo, hay más de un escritor compitiendo por \
         la misma clave (10-state.md §3.2)"
    )
}

fn format_reserved_domain(domain: &str, subject: &str) -> String {
    format!(
        "el dominio `{domain}` de {subject:?} está reservado: {} son espacios de nombres del \
         protocolo y no pueden ser un dominio (un `EVT_{}` colisionaría con ellos en el broker, y \
         un `publish()` a `state.…` llegaría al stream de estado sin el chequeo de secuencia — \
         10-state.md §2.7). Elige otro nombre para el dominio",
        RESERVED_DOMAINS.join(", "),
        domain.to_uppercase()
    )
}

fn format_mismatch(durable: &str, diffs: &[ConfigDifference]) -> String {
    use fmt::Write as _;
    let mut s = format!(
        "el servidor devolvió una configuración distinta de la solicitada para `{durable}`:\n"
    );
    for d in diffs {
        // El write! sobre un String no puede fallar.
        let _ = writeln!(
            s,
            "  {}: solicitado {}, efectivo {}",
            d.field, d.requested, d.effective
        );
    }
    s.push_str(
        "JetStream sobrescribe algunos campos en silencio (03-delivery.md §2.1). \
         Si el campo es ack_wait, comprueba que backoff[0] valga exactamente lo mismo.",
    );
    s
}

fn format_not_provisioned(kind: ResourceKind, name: &str, denied: &str) -> String {
    // Sin denegación: el recurso falta y el SDK **no intentó** crearlo. Es el caso del estado
    // replicado, que ni consulta ni crea su stream (10-state.md §2.1): su señal es que nadie
    // contesta (`no responders`) o que `CONSUMER.INFO` dice «stream not found» (A-32, A-48).
    if denied.is_empty() {
        return format!(
            "el {kind} `{name}` no existe, o existe sin `allow_direct`: en los dos casos no \
             contesta nadie (`no responders`) y se ven exactamente igual (A-32). El SDK de estado \
             no crea ni consulta el stream de estado (10-state.md §2.1): aprovisiónalo con \
             `flux provision` y audita su configuración (`max_msgs_per_subject: 1`, \
             `allow_direct: true`, `max_age: 0`, `discard: old`) con `flux doctor`. No es un \
             timeout."
        );
    }
    format!(
        "el {kind} `{name}` no existe y este servicio no tiene permiso para crearlo \
         (el broker denegó {denied}). En producción los streams y los consumidores se \
         aprovisionan fuera del SDK con `flux provision`, con credenciales de operación \
         (06-security.md §3): aprovisiónalo y reinicia el servicio. No es un timeout."
    )
}

fn format_connection_rejected(reason: RejectionReason, servers: &str, detail: &str) -> String {
    let (what, check) = match reason {
        RejectionReason::Auth => (
            "el broker rechazó la credencial de este servicio",
            "Revisa `Credentials` y que el usuario exista en la account del servicio \
             (06-security.md §2)",
        ),
        RejectionReason::Tls => (
            "no se pudo establecer TLS",
            "Revisa: ¿la CA de `TlsOptions` es la que firma el certificado del broker (una \
             CA propia sustituye a las raíces del sistema)?, ¿cubren sus SAN el host al que \
             te conectas?, ¿ofrece el broker TLS?, ¿exige certificado de cliente (mTLS)? El \
             SDK no baja a texto plano (06-security.md §2)",
        ),
    };
    format!(
        "no se pudo conectar a `{servers}`: {what} ({detail}). Reintentar no lo arregla, así \
         que `connect()` no espera. {check}."
    )
}

fn format_permission_denied(operation: &str, denied: &str) -> String {
    format!(
        "el broker denegó {denied} al {operation}. La ACL de este servicio no concede ese \
         subject (06-security.md §3). Una ACL de la v1 —sin permisos sobre `$JS.API.*` ni \
         inbox privado `_INBOX.<servicio>.>`— no basta: ver «Seguridad» en el README del \
         SDK para la lista exacta de permisos."
    )
}

impl FluxError {
    /// Construye un RETRYABLE.
    ///
    /// ```
    /// # use std::time::Duration;
    /// let err = flux::FluxError::retryable("proveedor 503")
    ///     .with_code("HTTP_503")
    ///     .with_retry_after(Duration::from_secs(5));
    /// ```
    #[must_use]
    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable(Failure::new(message))
    }

    /// Construye un PERMANENT.
    ///
    /// ```
    /// let err = flux::FluxError::permanent("pedido ya cancelado")
    ///     .with_code("PEDIDO_YA_CANCELADO");
    /// ```
    #[must_use]
    pub fn permanent(message: impl Into<String>) -> Self {
        Self::Permanent(Failure::new(message))
    }

    /// Construye un POISON. Normalmente solo lo llama el propio SDK.
    #[must_use]
    pub fn poison(message: impl Into<String>) -> Self {
        Self::Poison(Failure::new(message))
    }

    /// Fija el código estable para métricas y alertas.
    ///
    /// Sobre una variante no clasificada es un no-op: esas ya llevan su propio texto.
    #[must_use]
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        if let Some(f) = self.failure_mut() {
            f.code = Some(code.into());
        }
        self
    }

    /// Fija el retraso explícito del siguiente intento. Solo tiene efecto sobre un
    /// RETRYABLE; en las otras clases se ignora, igual que en Node y Go.
    #[must_use]
    pub fn with_retry_after(mut self, delay: Duration) -> Self {
        if let Some(f) = self.failure_mut() {
            f.retry_after = Some(delay);
        }
        self
    }

    /// Encadena el error subyacente, recuperable con [`std::error::Error::source`].
    #[must_use]
    pub fn with_source(mut self, source: impl Into<HandlerError>) -> Self {
        if let Some(f) = self.failure_mut() {
            f.source = Some(source.into());
        }
        self
    }

    fn failure_mut(&mut self) -> Option<&mut Failure> {
        match self {
            Self::Retryable(f) | Self::Permanent(f) | Self::Poison(f) => Some(f),
            _ => None,
        }
    }

    fn failure(&self) -> Option<&Failure> {
        match self {
            Self::Retryable(f) | Self::Permanent(f) | Self::Poison(f) => Some(f),
            _ => None,
        }
    }

    /// La clase que este error declara, si declara alguna.
    ///
    /// Solo las tres variantes de la taxonomía la declaran: un `InvalidSubject` es un
    /// bug del productor y no tiene sentido clasificarlo como si viniese de un handler.
    #[must_use]
    pub fn class(&self) -> Option<ErrorClass> {
        match self {
            // Las dos de esquema SÍ declaran clase, y comparten la de PERMANENT: el evento
            // es sintácticamente correcto —se parseó, no es POISON— pero incumple su
            // contrato, y reintentarlo seis veces dará seis veces lo mismo
            // — 00-protocol.md §5 y 04-errors.md §1.2.
            //
            // `NotProvisioned` y `PermissionDenied` también: son el 403 del broker. Un
            // handler que publica y recibe uno de los dos no lo arregla esperando, así que
            // sin clase caerían en la política de lo desconocido —2 entregas, ~30 s de cola
            // bloqueada— para acabar en el mismo sitio.
            //
            // Los tres de `connect()` (04-errors.md §5.3) también: reintentar una credencial
            // rechazada, un TLS que no encaja o una opción mal formada da el mismo fallo.
            Self::Permanent(_)
            | Self::SchemaValidation(_)
            | Self::SchemaNotFound(_)
            | Self::NotProvisioned { .. }
            | Self::PermissionDenied { .. }
            | Self::ConnectionRejected { .. }
            | Self::InvalidCredentials(_)
            | Self::InvalidTlsOptions(_)
            // Los cuatro de estado que se lanzan al LLAMAR: la llamada está mal y repetirla da
            // el mismo error — 04-errors.md §6. Un `put` dentro de un handler que los provoque va
            // a la DLQ en la primera entrega.
            | Self::InvalidStateContract { .. }
            | Self::InvalidTenantForState { .. }
            | Self::InvalidStateKey { .. }
            | Self::MissingAggregateVersion { .. }
            | Self::ReservedDomain { .. } => Some(ErrorClass::Permanent),
            // POISON y no PERMANENT: el mensaje es legible pero miente sobre su identidad, y un
            // productor que lo hace está roto o es hostil. Es lo que 04-errors.md §1.3 pide
            // despertar a alguien por.
            Self::Poison(_) | Self::StateSubjectMismatch { .. } => Some(ErrorClass::Poison),
            // El conflicto es del entorno —otra instancia escribe la misma clave—, no de la
            // llamada: un reintento relee y suele resolverlo.
            Self::Retryable(_) | Self::StateConflict { .. } => Some(ErrorClass::Retryable),
            _ => None,
        }
    }

    /// El código estable, o el nombre de la clase si no se fijó uno. Las métricas nunca
    /// quedan sin etiqueta.
    #[must_use]
    pub fn code(&self) -> Option<&str> {
        // Las dos de esquema no llevan `Failure`: su código es fijo porque es lo que
        // agrupa el panel del ecosistema, y dejar que la aplicación lo cambiara rompería
        // esa agrupación — 08-observability.md §2.2.
        match self {
            Self::SchemaValidation(_) => return Some(INVALID_SCHEMA_CODE),
            Self::SchemaNotFound(_) => return Some(SCHEMA_NOT_FOUND_CODE),
            // Igual que los de esquema: el código es el que agrupa el panel, no la app.
            Self::NotProvisioned { .. } => return Some(NOT_PROVISIONED_CODE),
            Self::PermissionDenied { .. } => return Some(PERMISSION_DENIED_CODE),
            Self::ConnectionRejected { .. } => return Some(CONNECTION_REJECTED_CODE),
            Self::InvalidCredentials(_) => return Some(INVALID_CREDENTIALS_CODE),
            Self::InvalidTlsOptions(_) => return Some(INVALID_TLS_OPTIONS_CODE),
            // Los siete de estado: el código es el que agrupa el panel, no la aplicación.
            Self::InvalidStateContract { .. } => return Some(INVALID_STATE_CONTRACT_CODE),
            Self::InvalidTenantForState { .. } => return Some(INVALID_TENANT_FOR_STATE_CODE),
            Self::InvalidStateKey { .. } => return Some(INVALID_STATE_KEY_CODE),
            Self::MissingAggregateVersion { .. } => return Some(MISSING_AGGREGATE_VERSION_CODE),
            Self::StateSubjectMismatch { .. } => return Some(STATE_SUBJECT_MISMATCH_CODE),
            Self::StateConflict { .. } => return Some(STATE_CONFLICT_CODE),
            Self::ReservedDomain { .. } => return Some(RESERVED_DOMAIN_CODE),
            _ => {}
        }
        let f = self.failure()?;
        Some(match (&f.code, self) {
            (Some(c), _) => c.as_str(),
            (None, Self::Retryable(_)) => "RetryableError",
            (None, Self::Permanent(_)) => "PermanentError",
            (None, Self::Poison(_)) => "PoisonError",
            (None, _) => unreachable!("failure() solo devuelve Some en las tres clasificadas"),
        })
    }

    /// El `retry_after` declarado, si el error es concretamente RETRYABLE. Las otras dos
    /// clases no tienen reintento del que hablar.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Retryable(f) => f.retry_after,
            _ => None,
        }
    }
}

/// Recorre la cadena de causas buscando un [`FluxError`] que declare su clase.
///
/// Es el equivalente de `errors.As` de Go y **mejora** al `instanceof` de Node: un
/// `FluxError::Retryable` envuelto por una capa intermedia de la aplicación sigue
/// clasificándose bien.
#[must_use]
pub fn as_classified<'a>(err: &'a (dyn StdError + 'static)) -> Option<&'a FluxError> {
    let mut cursor = Some(err);
    while let Some(e) = cursor {
        if let Some(flux) = e.downcast_ref::<FluxError>() {
            if flux.class().is_some() {
                return Some(flux);
            }
        }
        cursor = e.source();
    }
    None
}

/// Aplana una cadena de errores en un texto legible: `"mensaje: causa: causa raíz"`.
///
/// Rust no compone el mensaje con sus causas al hacer `Display` (a diferencia del
/// `Error()` de Go, que en este ecosistema concatena a mano). El `dlqerror` que acaba en
/// la DLQ tiene que ser legible sin herramientas, así que se aplana aquí.
#[must_use]
pub fn describe(err: &(dyn StdError + 'static)) -> String {
    let mut out = err.to_string();
    let mut cursor = err.source();
    while let Some(e) = cursor {
        let text = e.to_string();
        // Un `#[source]` cuyo Display ya está incluido en el del padre duplicaría texto.
        if !out.ends_with(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        cursor = e.source();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Error)]
    #[error("capa intermedia")]
    struct Envuelto(#[source] FluxError);

    #[test]
    fn las_tres_clases_se_declaran() {
        assert_eq!(
            FluxError::retryable("x").class(),
            Some(ErrorClass::Retryable)
        );
        assert_eq!(
            FluxError::permanent("x").class(),
            Some(ErrorClass::Permanent)
        );
        assert_eq!(FluxError::poison("x").class(), Some(ErrorClass::Poison));
        assert_eq!(FluxError::Envelope("x".into()).class(), None);
    }

    /// L3: un payload que incumple su contrato es PERMANENT, no RETRYABLE. Si cayera en la
    /// política de lo desconocido —`retryable-bounded`— el mismo evento inválido gastaría
    /// dos entregas y ~30 s de cola para acabar en el mismo sitio.
    #[test]
    fn los_errores_de_esquema_son_permanent() {
        let invalido = FluxError::SchemaValidation(SchemaValidationError {
            subject: "pedidos.pedido.v1.creado".into(),
            dataschema: "https://schemas.internal/x/1.0.0.json".into(),
            errors: vec!["/totalCents no es un entero".into()],
        });
        let ausente = FluxError::SchemaNotFound(SchemaNotFoundError {
            subject: "pedidos.pedido.v1.creado".into(),
            dataschema: "https://schemas.internal/x/1.0.0.json".into(),
        });

        assert_eq!(invalido.class(), Some(ErrorClass::Permanent));
        assert_eq!(ausente.class(), Some(ErrorClass::Permanent));
        // Códigos distintos: "el payload está mal" y "el bundle está viejo" son dos
        // incidentes con dos respuestas — 08-observability.md §2.1.
        assert_eq!(invalido.code(), Some(INVALID_SCHEMA_CODE));
        assert_eq!(ausente.code(), Some(SCHEMA_NOT_FOUND_CODE));
    }

    /// El mensaje lleva los TRES errores, no el primero: es lo que decide si arreglarlo
    /// cuesta un despliegue o tres — 00-protocol.md §5.
    #[test]
    fn el_mensaje_de_validacion_lista_todos_los_errores() {
        let err = FluxError::SchemaValidation(SchemaValidationError {
            subject: "pedidos.pedido.v1.creado".into(),
            dataschema: "https://schemas.internal/x/1.0.0.json".into(),
            errors: vec![
                "/totalCents no es un entero".into(),
                "/moneda no encaja".into(),
            ],
        })
        .to_string();

        assert!(err.contains("pedidos.pedido.v1.creado"), "{err}");
        assert!(err.contains("/totalCents"), "{err}");
        assert!(err.contains("/moneda"), "{err}");
    }

    /// Envuelto por una capa de la aplicación sigue clasificándose bien: es el mismo
    /// recorrido de `source()` que usa el clasificador con las tres clases.
    #[test]
    fn un_error_de_esquema_envuelto_se_sigue_clasificando() {
        let envuelto = Envuelto(FluxError::SchemaValidation(SchemaValidationError {
            subject: "s".into(),
            dataschema: "u".into(),
            errors: vec!["e".into()],
        }));
        let encontrado = as_classified(&envuelto).expect("se encuentra en la cadena");
        assert_eq!(encontrado.class(), Some(ErrorClass::Permanent));
    }

    #[test]
    fn el_codigo_cae_al_nombre_de_la_clase() {
        assert_eq!(FluxError::retryable("x").code(), Some("RetryableError"));
        assert_eq!(
            FluxError::permanent("x").with_code("YA_CANCELADO").code(),
            Some("YA_CANCELADO")
        );
    }

    #[test]
    fn retry_after_solo_en_retryable() {
        let d = Duration::from_secs(5);
        assert_eq!(
            FluxError::retryable("x").with_retry_after(d).retry_after(),
            Some(d)
        );
        // En PERMANENT se acepta pero se ignora, igual que en Node y Go.
        assert_eq!(
            FluxError::permanent("x").with_retry_after(d).retry_after(),
            None
        );
    }

    /// La mejora sobre el `instanceof` de Node: la clase sobrevive al envoltorio.
    #[test]
    fn as_classified_atraviesa_la_cadena() {
        let envuelto = Envuelto(FluxError::retryable("proveedor 503").with_code("HTTP_503"));
        let found = as_classified(&envuelto).expect("debería encontrar el FluxError");
        assert_eq!(found.class(), Some(ErrorClass::Retryable));
        assert_eq!(found.code(), Some("HTTP_503"));
    }

    #[test]
    fn as_classified_ignora_las_variantes_no_clasificadas() {
        let err = FluxError::Envelope("data debe ser objeto".into());
        assert!(as_classified(&err).is_none());
    }

    #[test]
    fn describe_aplana_la_cadena() {
        let err = FluxError::retryable("proveedor caído")
            .with_source(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        let text = describe(&err);
        assert!(text.starts_with("proveedor caído: "), "{text}");
    }

    #[test]
    fn dlqreason_usa_los_literales_de_la_spec() {
        assert_eq!(ErrorClass::Retryable.as_str(), "retryable");
        assert_eq!(ErrorClass::Permanent.as_str(), "permanent");
        assert_eq!(ErrorClass::Poison.as_str(), "poison");
    }

    /// 06-security.md §3: un recurso que falta y no se puede crear es PERMANENT — esperar no
    /// lo crea — y lleva un código estable que NO cambia la aplicación.
    #[test]
    fn not_provisioned_es_permanent_con_codigo_estable() {
        let err = FluxError::NotProvisioned {
            kind: ResourceKind::Stream,
            name: "EVT_PEDIDOS".into(),
            denied: "Publish to \"$JS.API.STREAM.CREATE.EVT_PEDIDOS\"".into(),
        };
        assert_eq!(err.class(), Some(ErrorClass::Permanent));
        assert_eq!(err.code(), Some("NOT_PROVISIONED"));
        assert_eq!(err.code(), Some(NOT_PROVISIONED_CODE));
        assert_eq!(err.retry_after(), None);

        // Accionable: nombra el recurso y dice cómo se arregla. Y dice que NO es un timeout.
        let texto = err.to_string();
        assert!(texto.contains("stream `EVT_PEDIDOS`"), "{texto}");
        assert!(texto.contains("flux provision"), "{texto}");
        assert!(texto.contains("No es un timeout"), "{texto}");

        let consumidor = FluxError::NotProvisioned {
            kind: ResourceKind::Consumer,
            name: "facturacion-api__pedidos_pedido_v1_creado".into(),
            denied: "Publish to \"$JS.API.CONSUMER.CREATE.…\"".into(),
        };
        assert!(consumidor
            .to_string()
            .contains("consumidor `facturacion-api__"));
    }

    #[test]
    fn permission_denied_es_permanent_con_codigo_estable() {
        let err = FluxError::PermissionDenied {
            operation: "consultar el stream `EVT_PEDIDOS`".into(),
            denied: "Publish to \"$JS.API.STREAM.INFO.EVT_PEDIDOS\"".into(),
        };
        assert_eq!(err.class(), Some(ErrorClass::Permanent));
        assert_eq!(err.code(), Some(PERMISSION_DENIED_CODE));
        assert!(err.to_string().contains("$JS.API.STREAM.INFO.EVT_PEDIDOS"));
        // Los dos códigos son distintos: «falta el recurso» y «no puedo ni mirarlo» son dos
        // incidentes con dos remedios.
        assert_ne!(NOT_PROVISIONED_CODE, PERMISSION_DENIED_CODE);
    }

    /// 04-errors.md §5.3: los tres errores de `connect()` son PERMANENT y su código es estable
    /// —el literal exacto de la spec— y distinto entre sí: son tres remedios.
    #[test]
    fn los_errores_de_connect_son_permanent_con_codigo_estable() {
        let rechazo = |reason| FluxError::ConnectionRejected {
            reason,
            servers: "tls://nats:4222".into(),
            detail: "invalid peer certificate: UnknownIssuer".into(),
            source: "causa".into(),
        };
        let casos = [
            (rechazo(RejectionReason::Auth), "CONNECTION_REJECTED"),
            (rechazo(RejectionReason::Tls), "CONNECTION_REJECTED"),
            (
                FluxError::InvalidCredentials("semilla".into()),
                "INVALID_CREDENTIALS",
            ),
            (
                FluxError::InvalidTlsOptions("ca".into()),
                "INVALID_TLS_OPTIONS",
            ),
        ];
        for (err, codigo) in &casos {
            assert_eq!(err.code(), Some(*codigo), "{err:?}");
            assert_eq!(err.class(), Some(ErrorClass::Permanent), "{err:?}");
            assert_eq!(err.retry_after(), None);
        }
        assert_eq!(CONNECTION_REJECTED_CODE, "CONNECTION_REJECTED");
        assert_eq!(INVALID_CREDENTIALS_CODE, "INVALID_CREDENTIALS");
        assert_eq!(INVALID_TLS_OPTIONS_CODE, "INVALID_TLS_OPTIONS");
        // No se confunden con los de permisos, ni entre ellos.
        let todos = [
            NOT_PROVISIONED_CODE,
            PERMISSION_DENIED_CODE,
            CONNECTION_REJECTED_CODE,
            INVALID_CREDENTIALS_CODE,
            INVALID_TLS_OPTIONS_CODE,
        ];
        let distintos: std::collections::HashSet<_> = todos.iter().collect();
        assert_eq!(distintos.len(), todos.len());
        // Lo que NO lleva código estable sigue sin llevarlo: un `Config` o un `Transport`
        // genéricos no se disfrazan de uno de estos tres.
        assert_eq!(FluxError::Config("x".into()).code(), None);
        assert_eq!(
            FluxError::Transport {
                context: "x".into(),
                source: "y".into()
            }
            .code(),
            None
        );
    }

    /// El mensaje de un rechazo dice la causa y qué revisar, según el motivo, y el `reason`
    /// se lee como en Node (`"auth"` / `"tls"`).
    #[test]
    fn el_rechazo_de_la_conexion_dice_la_causa_y_que_revisar() {
        let auth = FluxError::ConnectionRejected {
            reason: RejectionReason::Auth,
            servers: "nats://nats:4222".into(),
            detail: "authorization violation".into(),
            source: "x".into(),
        }
        .to_string();
        assert!(auth.contains("rechazó la credencial"), "{auth}");
        assert!(auth.contains("`nats://nats:4222`"), "{auth}");
        assert!(auth.contains("authorization violation"), "{auth}");
        assert!(auth.contains("Reintentar no lo arregla"), "{auth}");
        assert!(!auth.contains("SAN"), "no mezcla las dos causas: {auth}");

        let tls = FluxError::ConnectionRejected {
            reason: RejectionReason::Tls,
            servers: "tls://nats:4222".into(),
            detail: "invalid peer certificate: UnknownIssuer".into(),
            source: "x".into(),
        }
        .to_string();
        assert!(tls.contains("UnknownIssuer"), "{tls}");
        assert!(tls.contains("CA"), "{tls}");
        assert!(tls.contains("SAN"), "{tls}");
        assert!(tls.contains("no baja a texto plano"), "{tls}");
        assert!(!tls.contains("rechazó la credencial"), "{tls}");

        assert_eq!(RejectionReason::Auth.as_str(), "auth");
        assert_eq!(RejectionReason::Tls.to_string(), "tls");
    }

    /// Envueltos por una capa de la aplicación siguen clasificándose: un handler que publica
    /// y recibe uno de los dos no puede caer en la política de lo desconocido.
    #[test]
    fn los_errores_de_provision_y_permisos_se_clasifican_envueltos() {
        for err in [
            FluxError::NotProvisioned {
                kind: ResourceKind::Consumer,
                name: "c".into(),
                denied: "d".into(),
            },
            FluxError::PermissionDenied {
                operation: "o".into(),
                denied: "d".into(),
            },
            FluxError::ConnectionRejected {
                reason: RejectionReason::Tls,
                servers: "s".into(),
                detail: "d".into(),
                source: "x".into(),
            },
            FluxError::InvalidCredentials("c".into()),
            FluxError::InvalidTlsOptions("t".into()),
        ] {
            let envuelto = Envuelto(err);
            let c = as_classified(&envuelto).expect("se encuentra en la cadena");
            assert_eq!(c.class(), Some(ErrorClass::Permanent));
        }
    }

    #[test]
    fn el_mismatch_explica_la_trampa_de_ack_wait() {
        let err = FluxError::ConsumerConfigMismatch {
            durable: "facturacion-api__pedidos_pedido_v1_creado".into(),
            differences: vec![ConfigDifference {
                field: "ack_wait".into(),
                requested: "30s".into(),
                effective: "1s".into(),
            }],
        };
        let msg = err.to_string();
        assert!(msg.contains("ack_wait"), "{msg}");
        assert!(msg.contains("backoff[0]"), "{msg}");
    }
}
