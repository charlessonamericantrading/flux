//! Estado replicado — **extensión OPCIONAL** de v1.1, capacidad `state`.
//!
//! Contrato normativo: `specification/10-state.md`. Si algo de esta documentación diverge de la
//! spec, manda la spec.
//!
//! Un evento dice **qué pasó**; el estado es **el valor actual** de un agregado, que su dueño
//! publica y los demás observan. Sustituye a `GET /clientes/123` (`get`), a la carga inicial
//! (`watch` entrega una foto completa) y al polling de cambios (`watch` sigue entregándolos),
//! sin recrear la API que flux quiere eliminar.
//!
//! ```no_run
//! use serde_json::json;
//!
//! # async fn ejemplo(bus: flux::Bus) -> Result<(), flux::FluxError> {
//! // El dueño escribe. `aggregateVersion` lo pone él: es el contador de SU agregado.
//! let r = bus
//!     .state()
//!     .put_with(
//!         "clientes.cliente.v1",
//!         "cli-1",
//!         &json!({ "clienteId": "cli-1", "nombre": "Ana", "email": "ana@example.com", "aggregateVersion": 7 }),
//!         flux::PutOptions::default().with_tenant_id("acme"),
//!     )
//!     .await?;
//! assert!(r.applied); // `stale` NO es un error: es un valor cuya versión no avanzaba
//!
//! // Quien observa recibe la foto y después los cambios, por el mismo canal.
//! let _vigilante = bus
//!     .state()
//!     .watch_with(
//!         "clientes.cliente.v1",
//!         |entrada: flux::StateEntry, _entrega| async move {
//!             if entrada.deleted {
//!                 // El derecho de supresión se cumple aquí: borrar la copia local.
//!                 return Ok(());
//!             }
//!             // UPDATE … WHERE aggregate_version < entrada.aggregate_version
//!             Ok(())
//!         },
//!         flux::WatchOptions::default().with_tenant_id("acme").on_synced(|| println!("listo")),
//!     )
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! # Lo que el SDK hace y no te dice
//!
//! - **`put` es condicional** (§3.2): lee la secuencia de la clave (`DIRECT.GET`), no escribe si
//!   la versión no avanza y publica con `Nats-Expected-Last-Subject-Sequence`. Reconoce la propia
//!   escritura por su `id` (paso 1b: un acuse perdido no se convierte en `stale`) y reconoce **los
//!   dos** códigos de conflicto de secuencia, 10071 (un nodo) y 10164 (cluster, bajo carrera).
//! - **Todo se valida antes de la red**: dominio reservado, clave, tenant, `aggregateVersion` y
//!   clasificación. El broker no valida la clave (A-41).
//! - **Nunca por core NATS**: un `publish` de core a un subject de estado sobrescribe **sin
//!   chequeo ni acuse** (A-45).
//! - **Sin `STREAM.INFO` del stream de estado**: lista todas las claves y todos los tenants
//!   (A-39). `get` y `watch` funcionan sin él, y por eso un observador no lo necesita.
//!
//! # Funciones puras
//!
//! [`state_subject`], [`build_state_event`], [`build_state_tombstone`] y [`to_state_dlq_event`]
//! construyen los **bytes** exactos de `10-state.md` §2.6 sin reloj ni UUID propios: `id`, `time`
//! y `dlqtime` vienen dados, que es lo que permite compararlos byte a byte entre SDKs.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::{self, AckPolicy, DeliverPolicy, ReplayPolicy};
use async_nats::jetstream::stream::{self, ConsumerErrorKind, DiscardPolicy, RetentionPolicy};
use async_nats::jetstream::{AckKind, ErrorCode};
use chrono::{DateTime, Utc};
use futures::{FutureExt, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::value::RawValue;
use serde_json::{json, Value};
use tokio::sync::watch;
use uuid::Uuid;

use crate::client::{
    assert_config_honored, effective_budget, is_schema_code, lock, permission_denied, retry_delay,
    transport, Bus, Delivery, DlqEventInfo, LogLevel, PoisonInfo, TenantIsolation,
};
use crate::context::{self, EventContext};
use crate::envelope::{
    format_time, parse_event, serialize, to_dlq_event, DataClassification, DlqInfo, DlqReason,
    Event,
};
use crate::errors::{
    describe, Classification, ConfigDifference, ErrorClass, FluxError, HandlerError, HandlerResult,
    ResourceKind,
};
use crate::metrics::{ConsumeOutcome, DlqReasonLabel, StateWriteOutcome};
use crate::protocol::{
    dlq_stream_name, is_kebab_lower, is_reserved_domain, parse_major, state_stream_name,
    CANONICAL_BACKOFF, DATA_CONTENT_TYPE, DEFAULT_ACK_WAIT, DEFAULT_MAX_ACK_PENDING,
    DEFAULT_MAX_DELIVER, DLQ_STREAM_MAX_AGE, SPEC_VERSION,
};

// ─── Constantes del protocolo — protocol.json → state ───────────────────────

/// El mayor `aggregateVersion` válido: 2^53 − 1. Es el mayor entero que Node, y todo lo que use
/// `double`, representa sin perder precisión — 10-state.md §3.4.
pub const MAX_AGGREGATE_VERSION: u64 = 9_007_199_254_740_991;

/// Intentos de `put`/`delete` antes de `STATE_CONFLICT` — 10-state.md §3.2, paso 4.
///
/// Tres y no más: medido con 3 instancias escribiendo la **misma** clave sin pausa (A-49), de 90
/// `put` se aplicaron 49, 27 fueron `stale` y 14 agotaron los 3 intentos. Es una carga artificial,
/// y dice qué pasa si existe: el conflicto es RETRYABLE y se reintenta con el backoff de siempre.
pub const STATE_MAX_ATTEMPTS: u32 = 3;

/// Longitud máxima de una clave — 10-state.md §2.3.
pub const STATE_KEY_MAX_LEN: usize = 128;

/// Longitud máxima de un tenant de estado — 10-state.md §2.4. Sale de que el durable de un
/// observador lleva el tenant en el nombre y NATS limita los nombres de consumidor a 255 (A-42).
pub const STATE_TENANT_MAX_LEN: usize = 64;

/// El `dataschema` **fijo** de toda lápida, de cualquier contrato — 10-state.md §4.
pub const TOMBSTONE_DATASCHEMA: &str = "https://schemas.internal/system/state/eliminado/1.0.0.json";

/// Los dos `err_code` de JetStream que significan «alguien escribió antes»: **10071**
/// (`wrong last sequence: N`, un nodo o un cluster sin carrera) y **10164** (`wrong last
/// sequence`, **sin número**, un stream replicado cuando el conflicto se detecta al aplicar la
/// entrada). Medido: 210 de 210 rechazos de una carrera de 8 escritores sobre un cluster de 3
/// nodos fueron 10164 (A-49).
///
/// Un SDK que solo reconozca el 10071 trata un conflicto corriente como un error desconocido y
/// **no reintenta**: pasa todas sus pruebas contra un NATS de un nodo y falla en producción.
pub const SEQUENCE_CONFLICT_CODES: [u64; 2] = [10071, 10164];

/// Cuánto espera una lectura o una publicación de estado. Una respuesta o una denegación llegan
/// en ~1 ms; 5 s solo se agotan con un broker que no contesta.
const STATE_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

// ─── Dominios reservados ─────────────────────────────────────────────────────

/// Rechaza un subject o un contrato cuyo **primer token** es un dominio reservado.
///
/// Se mira el primer token y no el subject entero parseado: `dlq.pedidos.pedido.v1.creado` tiene
/// 5 tokens y un parser de 4 diría «debe tener 4», que es cierto y no ayuda.
pub(crate) fn reject_reserved_domain(subject_or_contract: &str) -> Result<(), FluxError> {
    let domain = subject_or_contract.split('.').next().unwrap_or_default();
    if is_reserved_domain(domain) {
        return Err(FluxError::ReservedDomain {
            domain: domain.to_string(),
            subject: subject_or_contract.to_string(),
        });
    }
    Ok(())
}

// ─── El contrato ─────────────────────────────────────────────────────────────

/// Un contrato de estado: `<dominio>.<agregado>.v<major>` — 10-state.md §2.2.
///
/// Es el subject **sin** `state.`, sin tenant y sin clave. Es lo que recibe `watch`, lo que
/// indexa `bundle.state` y lo que va en la etiqueta `contract` de las métricas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateContract {
    /// Dominio, en kebab-case y **nunca** uno de [`crate::RESERVED_DOMAINS`].
    pub domain: String,
    /// Agregado, en kebab-case.
    pub aggregate: String,
    /// Versión mayor, entero ≥ 1.
    pub major: u32,
}

impl StateContract {
    /// Valida y descompone un contrato.
    ///
    /// # Errores
    ///
    /// [`FluxError::ReservedDomain`] si el dominio es `dlq`, `state`, `rpc` o `system`;
    /// [`FluxError::InvalidSubject`] si no son tres tokens kebab-case y `v<major>`.
    pub fn parse(contract: &str) -> Result<Self, FluxError> {
        reject_reserved_domain(contract)?;
        let invalid = |reason: &str| FluxError::InvalidSubject {
            subject: contract.to_string(),
            reason: reason.to_string(),
        };
        let tokens: Vec<&str> = contract.split('.').collect();
        if tokens.len() != 3 {
            return Err(invalid(
                "un contrato de estado son exactamente 3 tokens, `<dominio>.<agregado>.v<major>` \
                 (`clientes.cliente.v1`): sin `state.`, sin tenant y sin clave",
            ));
        }
        if !is_kebab_lower(tokens[0]) {
            return Err(invalid(
                "el dominio debe ser un sustantivo plural en kebab-case ([a-z0-9-])",
            ));
        }
        if !is_kebab_lower(tokens[1]) {
            return Err(invalid(
                "el agregado debe ser un sustantivo singular en kebab-case ([a-z0-9-])",
            ));
        }
        let Some(major) = parse_major(tokens[2]) else {
            return Err(invalid(
                "el tercer token debe ser la versión mayor: literal `v` + entero >= 1 (`v1`)",
            ));
        };
        Ok(Self {
            domain: tokens[0].to_string(),
            aggregate: tokens[1].to_string(),
            major,
        })
    }

    /// `clientes.cliente.v1`.
    #[must_use]
    pub fn name(&self) -> String {
        format!("{}.{}.v{}", self.domain, self.aggregate, self.major)
    }

    /// `STATE_CLIENTES`.
    #[must_use]
    pub fn stream_name(&self) -> String {
        state_stream_name(&self.domain)
    }

    /// `com.flux.state.clientes.cliente.v1` — 10-state.md §2.5.
    #[must_use]
    pub fn event_type(&self) -> String {
        format!(
            "com.flux.state.{}.{}.v{}",
            self.domain, self.aggregate, self.major
        )
    }

    /// `com.flux.state.clientes.cliente.deleted.v1` — el `type` de la lápida, 10-state.md §4.
    ///
    /// `deleted` en inglés viene del plan mientras el esquema se llama `eliminado`: una mezcla de
    /// idiomas heredada (10-state.md §12, decisión 16).
    #[must_use]
    pub fn tombstone_type(&self) -> String {
        format!(
            "com.flux.state.{}.{}.deleted.v{}",
            self.domain, self.aggregate, self.major
        )
    }

    /// `dlq.state.clientes.cliente.v1` — **sin tenant ni clave**, para que no se multiplique por
    /// ellos (10-state.md §6).
    #[must_use]
    pub fn dlq_subject(&self) -> String {
        format!(
            "dlq.state.{}.{}.v{}",
            self.domain, self.aggregate, self.major
        )
    }

    /// `state.clientes.cliente.v1`, el prefijo de todo subject del contrato.
    fn prefix(&self) -> String {
        format!("state.{}.{}.v{}", self.domain, self.aggregate, self.major)
    }

    /// El subject de NATS de una entrada. **No valida** `tenant` ni `key`: úsalo con valores ya
    /// comprobados, o llama a [`state_subject`].
    fn subject_of(&self, tenant: &str, key: &str) -> String {
        format!("{}.{tenant}.{key}", self.prefix())
    }

    /// El filtro de un `watch`: `…<tenant>.*`, o `….*.*` sin tenant — 10-state.md §5.2.
    #[must_use]
    pub fn watch_filter(&self, tenant: Option<&str>) -> String {
        format!("{}.{}.*", self.prefix(), tenant.unwrap_or("*"))
    }

    /// El `dataschema` derivado de una URL base: `<base>/state/<dom>/<agr>/<major>.0.0.json`.
    #[must_use]
    pub fn default_dataschema(&self, base_url: &str) -> String {
        format!(
            "{}/state/{}/{}/{}.0.0.json",
            base_url.trim_end_matches('/'),
            self.domain,
            self.aggregate,
            self.major
        )
    }
}

// ─── Validación: clave, tenant, versión ──────────────────────────────────────

/// Valida una clave contra `^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$` — 10-state.md §2.3.
///
/// Con coincidencia **completa**, no con `$`: en Python y PHP el `$` acepta **un `\n` final**
/// (medido), y `"cli-1\n"` llegaría con su salto de línea a la librería de NATS. Aquí se recorre
/// carácter a carácter, así que el problema no existe; los vectores incluyen esa clave de todos
/// modos.
///
/// # Errores
///
/// [`FluxError::InvalidStateKey`].
pub fn validate_state_key(key: &str) -> Result<(), FluxError> {
    let reject = |reason: String| FluxError::InvalidStateKey {
        key: key.to_string(),
        reason,
    };
    if key.is_empty() {
        return Err(reject("está vacía".to_string()));
    }
    if key.len() > STATE_KEY_MAX_LEN {
        return Err(reject(format!(
            "tiene {} bytes y el máximo son {STATE_KEY_MAX_LEN}",
            key.len()
        )));
    }
    if key.starts_with('_') {
        return Err(reject(
            "empieza por `_`, prefijo reservado al protocolo".to_string(),
        ));
    }
    for (i, c) in key.chars().enumerate() {
        let ok = c.is_ascii_alphanumeric() || (i > 0 && (c == '_' || c == '-'));
        if ok {
            continue;
        }
        let why = match c {
            '.' => {
                "lleva un punto: son DOS tokens de NATS y la entrada quedaría fuera de todo filtro"
                    .to_string()
            }
            '*' | '>' => "lleva un comodín de NATS".to_string(),
            c if c.is_whitespace() => "lleva un espacio o un salto de línea".to_string(),
            '-' | '_' => "debe empezar por una letra o un dígito".to_string(),
            c if !c.is_ascii() => format!(
                "lleva el carácter {c:?}, fuera de [A-Za-z0-9_-] (no hay codificación automática)"
            ),
            c => format!("lleva el carácter {c:?}, fuera de [A-Za-z0-9_-]"),
        };
        return Err(reject(why));
    }
    Ok(())
}

/// Valida un tenant de estado contra `^[a-z0-9]+(-[a-z0-9]+)*$` con 64 caracteres como máximo —
/// 10-state.md §2.4. `"system"` es un tenant válido.
///
/// # Errores
///
/// [`FluxError::InvalidTenantForState`].
pub fn validate_state_tenant(tenant: &str) -> Result<(), FluxError> {
    let reject = |reason: &str| FluxError::InvalidTenantForState {
        tenant: Some(tenant.to_string()),
        reason: reason.to_string(),
    };
    if tenant.is_empty() {
        return Err(reject("está vacío"));
    }
    if tenant.len() > STATE_TENANT_MAX_LEN {
        return Err(reject("supera los 64 caracteres"));
    }
    if !is_kebab_lower(tenant) {
        return Err(reject(
            "no es kebab-case en minúsculas ([a-z0-9] separados por guiones simples)",
        ));
    }
    Ok(())
}

/// Resuelve el tenant de una escritura o una lectura — 10-state.md §2.4: la opción de la llamada,
/// el evento en curso y el `tenant_id` de `connect()`, **por ese orden**. Si no sale ninguno es
/// un error: un estado **no** tiene tenant por defecto.
pub(crate) fn resolve_tenant(
    option: Option<&str>,
    in_progress: Option<&str>,
    connection: Option<&str>,
) -> Result<String, FluxError> {
    // La opción gana aunque esté vacía (y entonces falla): `Some("")` es un descuido que debe
    // verse, no un «sin opción». El evento en curso vacío sí cuenta como ausente: es el
    // `EventContext::default()` de quien no está dentro de un handler.
    let chosen = option
        .or_else(|| in_progress.filter(|t| !t.is_empty()))
        .or(connection);
    let Some(tenant) = chosen else {
        return Err(FluxError::InvalidTenantForState {
            tenant: None,
            reason: "ni la llamada, ni el evento en curso, ni `connect()` traen un tenant"
                .to_string(),
        });
    };
    validate_state_tenant(tenant)?;
    Ok(tenant.to_string())
}

/// El tenant de un `watch`: la opción o el de `connect()`. A diferencia de `put`, **no** es un
/// error que falte: un `watch` sin tenant recibe el de todos (salvo con `strict`).
fn resolve_watch_tenant(
    option: Option<&str>,
    connection: Option<&str>,
) -> Result<Option<String>, FluxError> {
    match option.or(connection) {
        None => Ok(None),
        Some(t) => {
            validate_state_tenant(t)?;
            Ok(Some(t.to_string()))
        }
    }
}

/// Extrae y valida el `aggregateVersion` de un `data` ya parseado — 10-state.md §3.4.
///
/// Un entero ≥ 1 y ≤ 2^53 − 1. **Todo lo demás** es [`FluxError::MissingAggregateVersion`], no
/// solo la ausencia: `null`, una cadena (`"7"`), un decimal (`7.5`, y también `7.0`), `0`, un
/// negativo o uno mayor. El mensaje describe el defecto **sin citar el valor**.
///
/// # Errores
///
/// [`FluxError::MissingAggregateVersion`].
pub fn aggregate_version_of(data: &Value) -> Result<u64, FluxError> {
    let missing = |reason: &str| FluxError::MissingAggregateVersion {
        reason: reason.to_string(),
    };
    let Some(object) = data.as_object() else {
        return Err(missing(
            "`data` debe ser un objeto JSON que lleve `aggregateVersion`",
        ));
    };
    match object.get("aggregateVersion") {
        None => Err(missing("falta el campo")),
        Some(Value::Null) => Err(missing("es `null`")),
        Some(Value::String(_)) => Err(missing("es una cadena, y debe ser un número entero")),
        Some(Value::Number(n)) => match n.as_u64() {
            Some(0) => Err(missing("es 0, y el mínimo es 1")),
            Some(v) if v > MAX_AGGREGATE_VERSION => Err(missing("supera 2^53 − 1")),
            Some(v) => Ok(v),
            None if n.is_i64() => Err(missing("es negativo")),
            // Cualquier otro número que no es un `u64` es un decimal para serde_json, y `7.0`
            // también: dos escrituras con la misma versión son el mismo dato, y un contador es
            // un entero.
            None => Err(missing("es un decimal, y debe ser un número entero")),
        },
        Some(_) => Err(missing("no es un número")),
    }
}

/// El `aggregateVersion` ya como entero, para `delete` (donde llega tipado): el rango, nada más.
fn check_version_number(version: u64) -> Result<u64, FluxError> {
    if version == 0 {
        return Err(FluxError::MissingAggregateVersion {
            reason: "es 0, y el mínimo es 1".to_string(),
        });
    }
    if version > MAX_AGGREGATE_VERSION {
        return Err(FluxError::MissingAggregateVersion {
            reason: "supera 2^53 − 1".to_string(),
        });
    }
    Ok(version)
}

// ─── Funciones puras: subject, sobre, lápida, DLQ ────────────────────────────
//
// Sin reloj ni UUID propios: `id`, `time` y `dlqtime` vienen dados. Es lo que permite que el
// arnés cruzado compare los bytes con los de los otros SDKs, y que un test fije que son
// exactamente los de `10-state.md` §2.6.

/// El subject de NATS de una entrada: `state.<dominio>.<agregado>.v<major>.<tenant>.<clave>` —
/// 10-state.md §2.2. Seis tokens, siempre.
///
/// Valida **en el orden de §3.6**: dominio reservado, contrato, clave y tenant. El primero que
/// falla gana. No es el atributo `subject` de CloudEvents, que aquí vale la clave.
///
/// # Errores
///
/// [`FluxError::ReservedDomain`], [`FluxError::InvalidSubject`] (contrato mal formado),
/// [`FluxError::InvalidStateKey`] o [`FluxError::InvalidTenantForState`].
pub fn state_subject(contract: &str, tenant_id: &str, key: &str) -> Result<String, FluxError> {
    let c = StateContract::parse(contract)?;
    validate_state_key(key)?;
    validate_state_tenant(tenant_id)?;
    Ok(c.subject_of(tenant_id, key))
}

/// El nombre del durable de un `watch`: `<servicio>__state_<dom>_<agr>_v<M>` y, si filtra por
/// tenant, el sufijo `__<tenant>` — 10-state.md §5.2.
///
/// Los puntos y los guiones pasan a `_`, como en [`crate::durable_name`]. **El tenant va en el
/// nombre** porque un durable compartido reparte los mensajes entre quienes lo usan (A-47): dos
/// instancias del mismo servicio, una por tenant, con un durable sin sufijo, se quitarían la
/// mitad de la foto cada una.
///
/// # Errores
///
/// Los de [`crate::validate_service_name`] y los de [`StateContract::parse`] y
/// [`validate_state_tenant`].
pub fn state_durable_name(
    service: &str,
    contract: &str,
    tenant: Option<&str>,
) -> Result<String, FluxError> {
    crate::protocol::validate_service_name(service)?;
    let c = StateContract::parse(contract)?;
    let mut name = format!(
        "{service}__state_{}_{}_v{}",
        c.domain.replace('-', "_"),
        c.aggregate.replace('-', "_"),
        c.major
    );
    if let Some(t) = tenant {
        validate_state_tenant(t)?;
        name.push_str("__");
        name.push_str(&t.replace('-', "_"));
    }
    Ok(name)
}

/// El `dataschema` de un contrato derivado de la URL base de esquemas:
/// `https://schemas.internal` → `https://schemas.internal/state/clientes/cliente/1.0.0.json`.
///
/// Sin bundle solo se puede asumir el `<major>.0.0`: es suficiente para L2 —el atributo es
/// informativo— pero no para L3, que resuelve el exacto en el índice `state` del bundle.
///
/// # Errores
///
/// Los de [`StateContract::parse`].
pub fn state_dataschema(base_url: &str, contract: &str) -> Result<String, FluxError> {
    Ok(StateContract::parse(contract)?.default_dataschema(base_url))
}

/// Todo lo que hace falta para construir la entrada de un `put` — 10-state.md §2.5.
///
/// El desarrollador **no** rellena esto: lo rellena [`State::put`]. Se expone para el arnés de
/// conformidad y para quien construya un transporte propio.
#[derive(Debug, Clone)]
pub struct StateEventInput {
    /// `clientes.cliente.v1`.
    pub contract: String,
    /// Tenant de estado (§2.4).
    pub tenant_id: String,
    /// Clave (§2.3). Va al atributo `subject` **y** a `partitionkey`.
    pub key: String,
    /// UUIDv7 dado: sin él los bytes no serían comparables.
    pub id: String,
    /// `/<entorno>/<servicio>`.
    pub source: String,
    /// El instante del `put`.
    pub time: DateTime<Utc>,
    /// Correlación heredada, o el propio `id`.
    pub correlationid: String,
    /// SemVer del servicio emisor.
    pub producerversion: String,
    /// `public`, `internal` o `confidential`. **`restricted` no se admite** en estado (§7).
    pub classification: DataClassification,
    /// URI del esquema del contrato.
    pub dataschema: String,
    /// `id` del evento que provocó este estado, si lo hay.
    pub causationid: Option<String>,
    /// W3C Trace Context.
    pub traceparent: Option<String>,
    /// W3C Trace Context.
    pub tracestate: Option<String>,
    /// El valor. Objeto JSON con `aggregateVersion` (§3.4); viaja **sin reordenar**.
    pub data: Box<RawValue>,
}

/// Todo lo que hace falta para construir una lápida — 10-state.md §4.
#[derive(Debug, Clone)]
pub struct StateTombstoneInput {
    /// `clientes.cliente.v1`.
    pub contract: String,
    /// Tenant de estado.
    pub tenant_id: String,
    /// Clave.
    pub key: String,
    /// UUIDv7 dado.
    pub id: String,
    /// `/<entorno>/<servicio>`.
    pub source: String,
    /// El instante del `delete`.
    pub time: DateTime<Utc>,
    /// Correlación heredada, o el propio `id`.
    pub correlationid: String,
    /// SemVer del servicio emisor.
    pub producerversion: String,
    /// La versión **de la lápida**: debe superar la del valor que borra.
    pub aggregate_version: u64,
    /// `id` del evento que provocó el borrado, si lo hay.
    pub causationid: Option<String>,
    /// W3C Trace Context.
    pub traceparent: Option<String>,
    /// W3C Trace Context.
    pub tracestate: Option<String>,
}

/// Campos obligatorios que no pueden llegar vacíos: mismo criterio que `build_event`.
fn require_non_empty(fields: &[(&str, &str)]) -> Result<(), FluxError> {
    for (name, value) in fields {
        if value.is_empty() {
            return Err(FluxError::Envelope(format!(
                "`{name}` es obligatorio y llegó vacío. Lo rellena el SDK a partir de \
                 ConnectOptions; si estás construyendo la entrada a mano, rellénalo \
                 (01-envelope.md §5)"
            )));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn assemble(
    contract: &StateContract,
    tombstone: bool,
    tenant_id: String,
    key: String,
    id: String,
    source: String,
    time: DateTime<Utc>,
    correlationid: String,
    producerversion: String,
    classification: DataClassification,
    dataschema: String,
    causationid: Option<String>,
    traceparent: Option<String>,
    tracestate: Option<String>,
    data: Box<RawValue>,
) -> Event {
    Event {
        specversion: SPEC_VERSION.to_string(),
        id,
        source,
        event_type: if tombstone {
            contract.tombstone_type()
        } else {
            contract.event_type()
        },
        time: format_time(time),
        datacontenttype: DATA_CONTENT_TYPE.to_string(),
        dataschema,
        // El atributo `subject` de CloudEvents ES la clave, y su valor DEBE ser idéntico al
        // último token del subject de NATS — 10-state.md §2.5.
        aggregate_id: Some(key.clone()),
        correlationid,
        // …y `tenantid` idéntico al token de tenant del subject de NATS.
        tenantid: tenant_id,
        producerversion,
        dataclassification: classification,
        causationid,
        partitionkey: Some(key),
        traceparent,
        tracestate,
        signkeyid: None,
        signature: None,
        dlqreason: None,
        dlqattempts: None,
        dlqconsumer: None,
        dlqerror: None,
        dlqtime: None,
        data,
    }
}

/// Construye la entrada de un `put`, **sin red**.
///
/// Valida en el orden de §3.6 —dominio reservado, contrato, clave, tenant, `data` y
/// `aggregateVersion`, clasificación— y devuelve el [`Event`] listo para [`serialize`]. Es la
/// misma construcción que usa [`State::put`], y los bytes son los de `10-state.md` §2.6.
///
/// # Errores
///
/// Los de [`state_subject`], [`FluxError::MissingAggregateVersion`] y, para `restricted`, un
/// PERMANENT con código `INVALID_DATACLASSIFICATION`.
pub fn build_state_event(input: StateEventInput) -> Result<Event, FluxError> {
    let contract = StateContract::parse(&input.contract)?;
    validate_state_key(&input.key)?;
    validate_state_tenant(&input.tenant_id)?;
    let data: Value = serde_json::from_str(input.data.get())
        .map_err(|e| FluxError::Envelope(format!("`data` no es JSON válido: {e}")))?;
    aggregate_version_of(&data)?;
    reject_restricted(input.classification)?;
    require_non_empty(&[
        ("id", &input.id),
        ("source", &input.source),
        ("dataschema", &input.dataschema),
        ("correlationid", &input.correlationid),
        ("producerversion", &input.producerversion),
    ])?;
    Ok(assemble(
        &contract,
        false,
        input.tenant_id,
        input.key,
        input.id,
        input.source,
        input.time,
        input.correlationid,
        input.producerversion,
        input.classification,
        input.dataschema,
        input.causationid,
        input.traceparent,
        input.tracestate,
        input.data,
    ))
}

/// Construye la **lápida** de un `delete`, sin red: `type` con `.deleted.`, `dataschema` fijo,
/// `dataclassification: internal` y `data` con **solo** la versión — 10-state.md §4.
///
/// # Errores
///
/// Los de [`state_subject`] y [`FluxError::MissingAggregateVersion`].
pub fn build_state_tombstone(input: StateTombstoneInput) -> Result<Event, FluxError> {
    let contract = StateContract::parse(&input.contract)?;
    validate_state_key(&input.key)?;
    validate_state_tenant(&input.tenant_id)?;
    let version = check_version_number(input.aggregate_version)?;
    require_non_empty(&[
        ("id", &input.id),
        ("source", &input.source),
        ("correlationid", &input.correlationid),
        ("producerversion", &input.producerversion),
    ])?;
    let data = serde_json::value::to_raw_value(&json!({ "aggregateVersion": version }))
        .map_err(|e| FluxError::Envelope(format!("la lápida no es serializable: {e}")))?;
    Ok(assemble(
        &contract,
        true,
        input.tenant_id,
        input.key,
        input.id,
        input.source,
        input.time,
        input.correlationid,
        input.producerversion,
        // La lápida lleva SIEMPRE `internal`: no tiene dato que clasificar.
        DataClassification::Internal,
        TOMBSTONE_DATASCHEMA.to_string(),
        input.causationid,
        input.traceparent,
        input.tracestate,
        data,
    ))
}

/// Convierte la entrada que un observador no pudo aplicar en el **evento de DLQ de estado** —
/// 10-state.md §6: el sobre original con las extensiones `dlq*` y **sin su `data`**, que se
/// sustituye por `{ "key": "…", "revision": n }`.
///
/// - **No lleva el dato**: la DLQ guarda 90 días; un dato personal que el dueño ya borró seguiría
///   vivo ahí tres meses y el derecho de supresión no se cumpliría.
/// - **No conserva `signkeyid` ni `signature`**: firman un `data` que ya no está, y una firma que
///   no verifica sobre un sobre que parece el original es una afirmación falsa.
/// - `key` es la **del subject de NATS** —la que decidió el broker—, no la del sobre: ante un
///   `STATE_SUBJECT_MISMATCH` son distintas.
///
/// El `id` se conserva y `data` pasa a ser lo último, como en todo evento.
#[must_use]
pub fn to_state_dlq_event(
    event: Event,
    key: &str,
    revision: u64,
    info: &DlqInfo,
    dlqtime: DateTime<Utc>,
) -> Event {
    let mut dlq = to_dlq_event(event, info, dlqtime);
    dlq.signkeyid = None;
    dlq.signature = None;
    dlq.data = serde_json::value::to_raw_value(&json!({ "key": key, "revision": revision }))
        // Un objeto con una cadena y un entero siempre serializa: el `null` no se alcanza.
        .unwrap_or_else(|_| RawValue::NULL.to_owned());
    dlq
}

fn reject_restricted(classification: DataClassification) -> Result<(), FluxError> {
    if classification == DataClassification::Restricted {
        return Err(FluxError::permanent(
            "`restricted` (credenciales, salud, financiero) no se admite en un estado: es un dato \
             de 24 h de retención y el estado no caduca. Usa `confidential` —con `x-pii` en el \
             esquema— o `internal` (10-state.md §7)",
        )
        .with_code("INVALID_DATACLASSIFICATION"));
    }
    Ok(())
}

// ─── La entrada que recibe quien observa ─────────────────────────────────────

/// Una entrada de estado: lo que recibe el handler de [`State::watch`] y lo que devuelve
/// [`State::get`] — 10-state.md §5.3.
#[derive(Debug, Clone)]
pub struct StateEntry {
    /// La clave. Sale del **subject de NATS**, no del sobre.
    pub key: String,
    /// El tenant, del subject de NATS.
    ///
    /// **No estaba en el plan y es necesario**: un `watch` sin tenant recibe `cli-1` de varios, y
    /// una copia local que guarda por clave mezclaría los tenants en silencio.
    pub tenant_id: String,
    /// `true` si es una lápida: borra o anonimiza tu copia local.
    pub deleted: bool,
    /// El `data` del evento, **sin interpretar**; `None` si `deleted` —la lápida no lleva dato—.
    ///
    /// `None` es otra cosa que un `null` dentro de `data` en el cable (01-envelope.md §4).
    pub data: Option<Box<RawValue>>,
    /// La versión del valor, o la de la lápida.
    pub aggregate_version: u64,
    /// La secuencia del stream. Sirve para saber *qué mensaje* es; **no** para ordenar versiones
    /// de una clave —eso es `aggregate_version`— ni se compara entre contratos.
    pub revision: u64,
}

impl PartialEq for StateEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.tenant_id == other.tenant_id
            && self.deleted == other.deleted
            && self.data.as_ref().map(|d| d.get()) == other.data.as_ref().map(|d| d.get())
            && self.aggregate_version == other.aggregate_version
            && self.revision == other.revision
    }
}

impl Eq for StateEntry {}

impl StateEntry {
    /// Decodifica el valor en el tipo de la aplicación. `Ok(None)` si es una lápida.
    ///
    /// # Errores
    ///
    /// [`FluxError::Permanent`] con `DATA_SCHEMA_MISMATCH`, igual que [`Event::data_as`]: el
    /// sobre era correcto, es un desajuste de contrato entre dueño y observador.
    pub fn data_as<T: DeserializeOwned>(&self) -> Result<Option<T>, FluxError> {
        let Some(data) = &self.data else {
            return Ok(None);
        };
        serde_json::from_str(data.get()).map(Some).map_err(|e| {
            FluxError::permanent(format!(
                "el valor de la clave `{}` no encaja en el tipo esperado",
                self.key
            ))
            .with_code("DATA_SCHEMA_MISMATCH")
            .with_source(e)
        })
    }
}

// ─── Resultado de una escritura ──────────────────────────────────────────────

/// El resultado de una escritura que **no** lanzó: `ok` o `stale` — 10-state.md §3.5.
///
/// No hay un tercer valor: un conflicto agotado es un error ([`FluxError::StateConflict`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteOutcome {
    /// La escritura entró, o ya había entrado con ese mismo `id`.
    Ok,
    /// La versión no avanzaba, y una igual **tampoco** avanza. No es un error.
    Stale,
}

impl WriteOutcome {
    /// `"ok"` o `"stale"`: el literal de las métricas y los logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Stale => "stale",
        }
    }
}

impl fmt::Display for WriteOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Lo que devuelve [`State::put`] y [`State::delete`] — 10-state.md §3.5.
///
/// `applied` y `outcome` dicen lo mismo: el booleano es para `if`, el enum para métricas y logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateWrite {
    /// `true` si la escritura (o la lápida) entró.
    pub applied: bool,
    /// `ok` o `stale`.
    pub outcome: WriteOutcome,
    /// Escrito: la secuencia del stream del mensaje nuevo. `stale`: la del mensaje **actual**, el
    /// que hizo obsoleta la escritura.
    pub revision: u64,
    /// Cuántos intentos hicieron falta (1 si nadie compitió). Diagnóstico: no está en la spec.
    pub attempts: u32,
    /// `true` si la escritura se reconoció por su propio `id` —un acuse perdido: se aplicó y
    /// quien la hizo no lo vio— y por tanto no se publicó de nuevo. Diagnóstico.
    pub own_write: bool,
}

// ─── El algoritmo de put/delete — 10-state.md §3.2 ───────────────────────────

/// Lo que el paso 1 lee de la clave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Current {
    /// `404`: no hay valor. `S = 0`, y no hay versión `V`.
    Absent,
    /// Hay un mensaje (un valor o una lápida: para el algoritmo es lo mismo).
    Present {
        /// `Nats-Sequence`: la `S` del chequeo condicional.
        seq: u64,
        /// El `id` del sobre, si el mensaje es legible. Es lo que reconoce la propia escritura.
        event_id: Option<String>,
        /// `data.aggregateVersion`, si es válida. Un valor ilegible, o sin versión válida, es
        /// «sin versión»: se sobrescribe con el chequeo sobre **su** `S`, para que una entrada
        /// rota no deje la clave inescribible para siempre (§3.3).
        version: Option<u64>,
    },
}

/// Lo que devuelve la publicación del paso 3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Published {
    /// El acuse del stream: la secuencia del mensaje nuevo.
    Acked { seq: u64 },
    /// `10071` o `10164`: alguien escribió entre el paso 1 y el 3.
    SequenceConflict,
}

/// El transporte del algoritmo: leer la clave y publicar condicionalmente.
///
/// Es un trait **solo para poder probar el algoritmo sin broker**: con una implementación en
/// memoria se ejercitan el paso 1b, los dos códigos de conflicto, los tres intentos y el mismo
/// `id` en todos ellos. En producción lo implementa [`BusIo`], contra NATS.
pub(crate) trait StateIo: Send + Sync {
    /// Paso 1: el último mensaje de la clave (`DIRECT.GET` por subject).
    fn read(&self, subject: &str) -> impl Future<Output = Result<Current, FluxError>> + Send;

    /// Paso 3: publicar por JetStream con `Nats-Expected-Last-Subject-Sequence: expected` y
    /// `Nats-Msg-Id: id`.
    fn publish(
        &self,
        subject: &str,
        id: &str,
        expected: u64,
        bytes: &[u8],
    ) -> impl Future<Output = Result<Published, FluxError>> + Send;
}

/// Una escritura ya construida: **los mismos `id` y bytes en todos los intentos** — §3.3. Con un
/// `id` nuevo por intento el paso 1b no reconocería nunca la propia escritura.
pub(crate) struct WriteJob<'a> {
    pub(crate) subject: &'a str,
    pub(crate) key: &'a str,
    pub(crate) id: &'a str,
    pub(crate) bytes: &'a [u8],
    pub(crate) version: u64,
}

/// Un punto de inyección **solo para pruebas** del algoritmo: los escenarios de
/// `conformance/cases/state-semantics.json` (`hook.beforePublish` y `hook.lostAckOnce`) hacen
/// escribir a un rival entre la lectura y la publicación, o pierden un acuse. No es API: no hay
/// otra forma determinista de ejercitar el paso 1b ni los tres intentos contra un broker real.
#[doc(hidden)]
pub trait WriteHook: Send + Sync + 'static {
    /// Se invoca tras leer y **antes** de publicar, en cada intento (`attempt` desde 1).
    fn before_publish(&self, attempt: u32) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    /// `true` hace que el acuse de este intento se dé por perdido: el publish **se aplicó**,
    /// pero quien lo hizo ve un error de secuencia, que es lo que pasa cuando la librería de NATS
    /// reenvía el publish tras una reconexión y el servidor contesta `wrong last sequence`.
    fn lose_ack(&self, attempt: u32) -> bool {
        let _ = attempt;
        false
    }
}

/// El algoritmo de `put`/`delete`, **tal cual `10-state.md` §3.2**:
///
/// ```text
/// 1. Leer el último mensaje de la clave. 404 → S = 0 y sin versión.
/// 1b. Si el id leído es el de ESTE put → ya se aplicó (acuse perdido): ok, revision S.
/// 2. Si hay V y nueva.aggregateVersion <= V → stale. No es un error.
/// 3. Publicar con Nats-Expected-Last-Subject-Sequence: S y Nats-Msg-Id: <id>.
/// 4. Conflicto (10071 o 10164) → volver a 1. Tres intentos; agotados, STATE_CONFLICT.
/// ```
pub(crate) async fn run_write<I: StateIo>(
    io: &I,
    job: &WriteJob<'_>,
    hook: Option<&dyn WriteHook>,
) -> Result<StateWrite, FluxError> {
    for attempt in 1..=STATE_MAX_ATTEMPTS {
        // Paso 1.
        let current = io.read(job.subject).await?;
        let expected = match &current {
            Current::Absent => 0,
            Current::Present { seq, .. } => *seq,
        };

        if let Current::Present {
            seq,
            event_id,
            version,
        } = &current
        {
            // Paso 1b: sin él, un acuse perdido convierte una escritura buena en `stale`. El
            // primer intento se aplica, el acuse se pierde, el reintento relee, ve su propia
            // versión y concluiría `stale` con su propio dato. El `id` lo distingue porque el
            // broker conserva las cabeceras —`Nats-Msg-Id` incluida— (A-32) y porque el evento
            // lleva el mismo `id` en todos los intentos.
            if event_id.as_deref() == Some(job.id) {
                return Ok(StateWrite {
                    applied: true,
                    outcome: WriteOutcome::Ok,
                    revision: *seq,
                    attempts: attempt,
                    own_write: true,
                });
            }
            // Paso 2: una versión igual TAMBIÉN es `stale`.
            if version.is_some_and(|v| job.version <= v) {
                return Ok(StateWrite {
                    applied: false,
                    outcome: WriteOutcome::Stale,
                    revision: *seq,
                    attempts: attempt,
                    own_write: false,
                });
            }
        }

        if let Some(h) = hook {
            h.before_publish(attempt).await;
        }

        // Paso 3.
        match io.publish(job.subject, job.id, expected, job.bytes).await? {
            Published::Acked { seq } if !hook.is_some_and(|h| h.lose_ack(attempt)) => {
                // Paso 4: el acuse.
                return Ok(StateWrite {
                    applied: true,
                    outcome: WriteOutcome::Ok,
                    revision: seq,
                    attempts: attempt,
                    own_write: false,
                });
            }
            // Un conflicto de secuencia —o un acuse perdido— → volver al paso 1.
            Published::Acked { .. } | Published::SequenceConflict => {
                if attempt < STATE_MAX_ATTEMPTS {
                    conflict_pause().await;
                }
            }
        }
    }
    Err(FluxError::StateConflict {
        key: job.key.to_string(),
        attempts: STATE_MAX_ATTEMPTS,
    })
}

/// Una pausa con jitter entre intentos: la spec la permite (§3.3) y reduce el choque de dos
/// escritores que reintentan a la vez. Los bits bajos de un UUIDv7 son aleatorios.
async fn conflict_pause() {
    let jitter = u64::try_from(Uuid::now_v7().as_u128() % 8).unwrap_or(0);
    tokio::time::sleep(Duration::from_millis(1 + jitter)).await;
}

/// Interpreta el acuse de una publicación de JetStream: `{"stream":…,"seq":N}` o
/// `{"error":{"code":…,"err_code":N,"description":"…"}}`.
///
/// **Los dos códigos de conflicto** —[`SEQUENCE_CONFLICT_CODES`]— son «alguien escribió antes».
/// El número que trae el 10071 no hace falta: se relee.
pub(crate) fn parse_ack(payload: &[u8]) -> Result<Published, FluxError> {
    let v: Value = serde_json::from_slice(payload)
        .map_err(|e| transport("el stream contestó algo que no es un acuse de JetStream", e))?;
    if let Some(error) = v.get("error") {
        let err_code = error.get("err_code").and_then(Value::as_u64);
        if err_code.is_some_and(|c| SEQUENCE_CONFLICT_CODES.contains(&c)) {
            return Ok(Published::SequenceConflict);
        }
        let description = error
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("sin descripción");
        return Err(transport(
            format!(
                "el stream de estado rechazó la escritura: {description} (err_code {err_code:?})"
            ),
            "acuse de error de JetStream",
        ));
    }
    let seq = v.get("seq").and_then(Value::as_u64).ok_or_else(|| {
        transport(
            "el acuse de JetStream no trae `seq`",
            "acuse de JetStream sin secuencia",
        )
    })?;
    Ok(Published::Acked { seq })
}

/// El paso 1 sobre los bytes de un mensaje: su `id` y su versión, **leyendo con indulgencia**.
///
/// No es `parse_event`: un valor ilegible no bloquea la clave (§3.3). Si el cuerpo no es JSON, o
/// no tiene un `aggregateVersion` válido, es «sin versión» y se sobrescribe.
pub(crate) fn current_of(seq: u64, payload: &[u8]) -> Current {
    let value: Option<Value> = serde_json::from_slice(payload).ok();
    let object = value.as_ref().and_then(Value::as_object);
    let event_id = object
        .and_then(|o| o.get("id"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let version = object
        .and_then(|o| o.get("data"))
        .and_then(|d| aggregate_version_of(d).ok());
    Current::Present {
        seq,
        event_id,
        version,
    }
}

/// Qué cuenta `flux_state_writes_total` de lo que pasó — `protocol.json` → `state.metrics`.
///
/// `Ok(applied)` → `ok`; `stale` → `stale`; `STATE_CONFLICT` → `conflict`; `MISSING_AGGREGATE_
/// VERSION` y los fallos de esquema → `invalid_schema` (también cuando la llamada **lanza**). Lo
/// demás —clave, tenant, dominio reservado, clasificación, transporte— son errores de quien llama
/// o del entorno y **no** se cuentan.
pub(crate) fn outcome_of(result: &Result<StateWrite, FluxError>) -> Option<StateWriteOutcome> {
    match result {
        Ok(w) if w.applied => Some(StateWriteOutcome::Ok),
        Ok(_) => Some(StateWriteOutcome::Stale),
        Err(
            FluxError::MissingAggregateVersion { .. }
            | FluxError::SchemaValidation(_)
            | FluxError::SchemaNotFound(_),
        ) => Some(StateWriteOutcome::InvalidSchema),
        Err(FluxError::StateConflict { .. }) => Some(StateWriteOutcome::Conflict),
        Err(_) => None,
    }
}

// ─── Opciones ────────────────────────────────────────────────────────────────

/// Ajustes de un [`State::put_with`].
#[derive(Clone, Default)]
pub struct PutOptions {
    /// Tenant de esta escritura. Sin él: el del evento en curso y, si no, el de `connect()`. Si no
    /// sale ninguno, [`FluxError::InvalidTenantForState`]: un estado no cae en `system`.
    pub tenant_id: Option<String>,
    /// Clasificación. **Sin ella, `confidential`**, y no el valor por defecto de la conexión: un
    /// estado es donde se permite que viva el dato personal, y equivocarse hacia arriba cuesta un
    /// log redactado, mientras que hacia abajo cuesta PII en un log o en una DLQ. `restricted` se
    /// rechaza (10-state.md §3.6, §7).
    pub classification: Option<DataClassification>,
    /// Ver [`WriteHook`]. No es API.
    #[doc(hidden)]
    pub hook: Option<Arc<dyn WriteHook>>,
}

impl fmt::Debug for PutOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PutOptions")
            .field("tenant_id", &self.tenant_id)
            .field("classification", &self.classification)
            .finish_non_exhaustive()
    }
}

impl PutOptions {
    /// Tenant de esta escritura.
    #[must_use]
    pub fn with_tenant_id(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant.into());
        self
    }

    /// Clasificación de esta escritura.
    #[must_use]
    pub fn with_classification(mut self, classification: DataClassification) -> Self {
        self.classification = Some(classification);
        self
    }

    /// Instala un [`WriteHook`]. Solo para pruebas.
    #[doc(hidden)]
    #[must_use]
    pub fn with_hook(mut self, hook: Arc<dyn WriteHook>) -> Self {
        self.hook = Some(hook);
        self
    }
}

/// Ajustes de un [`State::delete_with`].
#[derive(Clone, Default)]
pub struct DeleteOptions {
    /// Tenant del borrado: el mismo orden de resolución que [`PutOptions::tenant_id`].
    pub tenant_id: Option<String>,
    /// Ver [`WriteHook`]. No es API.
    #[doc(hidden)]
    pub hook: Option<Arc<dyn WriteHook>>,
}

impl fmt::Debug for DeleteOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeleteOptions")
            .field("tenant_id", &self.tenant_id)
            .finish_non_exhaustive()
    }
}

impl DeleteOptions {
    /// Tenant de este borrado.
    #[must_use]
    pub fn with_tenant_id(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant.into());
        self
    }

    /// Instala un [`WriteHook`]. Solo para pruebas.
    #[doc(hidden)]
    #[must_use]
    pub fn with_hook(mut self, hook: Arc<dyn WriteHook>) -> Self {
        self.hook = Some(hook);
        self
    }
}

/// Ajustes de un [`State::get_with`].
#[derive(Debug, Clone, Default)]
pub struct GetOptions {
    /// Tenant de la lectura: el mismo orden de resolución que [`PutOptions::tenant_id`].
    pub tenant_id: Option<String>,
}

impl GetOptions {
    /// Tenant de esta lectura.
    #[must_use]
    pub fn with_tenant_id(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant.into());
        self
    }
}

/// Un hueco **solo para pruebas** entre la creación del durable y el primer pull: los escenarios
/// «lo escrito mientras se sincroniza» necesitan escribir tras abrir el `watch` y **antes** de que
/// empiece a consumir. No es API.
#[doc(hidden)]
pub type AfterOpen = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Ajustes de un [`State::watch_with`].
#[derive(Clone, Default)]
pub struct WatchOptions {
    /// Filtra por este tenant. Sin él: el de `connect()`, **siempre**, sea cual sea su valor
    /// —aquí `system` es un tenant como cualquier otro—; y sin ninguno, recibe el de todos, salvo
    /// con [`TenantIsolation::Strict`], donde es un error de configuración — 10-state.md §5.2.
    pub tenant_id: Option<String>,
    /// Se invoca **una sola vez por `watch`** —no una por tenant ni por reconexión— cuando el
    /// handler ya ha procesado **todo lo que había pendiente al abrir** (§5.5). Con el contrato
    /// vacío, **de inmediato**: antes de entregar nada y aunque no llegue ninguna entrada después.
    pub on_synced: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Ventana de mensajes sin confirmar. Por defecto [`DEFAULT_MAX_ACK_PENDING`].
    pub max_ack_pending: Option<u32>,
    /// Ver [`AfterOpen`]. No es API.
    #[doc(hidden)]
    pub after_open: Option<AfterOpen>,
}

impl fmt::Debug for WatchOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WatchOptions")
            .field("tenant_id", &self.tenant_id)
            .field("on_synced", &self.on_synced.is_some())
            .field("max_ack_pending", &self.max_ack_pending)
            .finish_non_exhaustive()
    }
}

impl WatchOptions {
    /// Filtra por este tenant.
    #[must_use]
    pub fn with_tenant_id(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant.into());
        self
    }

    /// El callback de «mi réplica local ya puede contestar lecturas».
    #[must_use]
    pub fn on_synced(mut self, f: impl Fn() + Send + Sync + 'static) -> Self {
        self.on_synced = Some(Arc::new(f));
        self
    }

    /// Ventana de mensajes sin confirmar.
    #[must_use]
    pub fn with_max_ack_pending(mut self, n: u32) -> Self {
        self.max_ack_pending = Some(n);
        self
    }

    /// Instala un [`AfterOpen`]. Solo para pruebas.
    #[doc(hidden)]
    #[must_use]
    pub fn with_after_open(mut self, hook: AfterOpen) -> Self {
        self.after_open = Some(hook);
        self
    }
}

// ─── Handler ─────────────────────────────────────────────────────────────────

/// Procesa una entrada de estado.
///
/// Devolver `Ok(())` **ACK-ea** la entrada; devolver `Err` la clasifica como un evento
/// ([`crate::classify`]) y produce `nak`, `term` o `term` + DLQ de estado. Se implementa para
/// cualquier `Fn(StateEntry, Delivery) -> Future`, así que en la práctica se escribe como un
/// closure.
///
/// **El handler DEBE ser idempotente y no asumir orden**: la entrada de una clave puede llegar
/// mientras se procesa la anterior. La defensa es `UPDATE … WHERE aggregate_version < $n`, que
/// aquí es la regla de aplicar la entrada, no una optimización (10-state.md §5.3). Ante una
/// lápida, **borra o anonimiza** la copia local y recuerda su versión.
///
/// **El mensaje de un error NO DEBE contener valores de la entrada**: acaba en `dlqerror`, que se
/// guarda 90 días, y en estado `data` es dato personal.
pub trait StateHandler: Send + Sync + 'static {
    /// Invoca el handler.
    fn call(
        &self,
        entry: StateEntry,
        delivery: Delivery,
    ) -> Pin<Box<dyn Future<Output = HandlerResult> + Send>>;
}

impl<F, Fut> StateHandler for F
where
    F: Fn(StateEntry, Delivery) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = HandlerResult> + Send + 'static,
{
    fn call(
        &self,
        entry: StateEntry,
        delivery: Delivery,
    ) -> Pin<Box<dyn Future<Output = HandlerResult> + Send>> {
        Box::pin(self(entry, delivery))
    }
}

/// Un `watch` vivo.
///
/// Al soltarse **no** se cancela la entrega, por la misma razón que [`crate::Subscription`]:
/// convertiría un `let _ = state.watch(…)` en un consumidor que muere sin decir nada.
#[derive(Debug)]
pub struct StateWatch {
    /// El contrato observado.
    pub contract: String,
    /// El tenant del filtro, si lo hay.
    pub tenant_id: Option<String>,
    /// El durable: `<servicio>__state_<dom>_<agr>_v<M>[__<tenant>]`.
    pub durable: String,
    stop: watch::Sender<bool>,
    synced: watch::Receiver<bool>,
}

impl StateWatch {
    /// Detiene la entrega. La entrada en curso termina de procesarse; las que estén en el búfer
    /// se reentregarán, que es justo lo que cubre la idempotencia.
    pub fn unsubscribe(&self) {
        let _ = self.stop.send(true);
    }

    /// ¿Ha terminado ya la foto inicial? Ver [`WatchOptions::on_synced`].
    #[must_use]
    pub fn is_synced(&self) -> bool {
        *self.synced.borrow()
    }

    /// Espera a que termine la foto inicial: es `on_synced` como `Future`. Vuelve también si el
    /// `watch` se detiene antes de sincronizar: quien espera no debe quedarse colgado.
    pub async fn wait_synced(&self) {
        let mut synced = self.synced.clone();
        let mut stopped = self.stop.subscribe();
        tokio::select! {
            _ = synced.wait_for(|s| *s) => {}
            _ = stopped.wait_for(|s| *s) => {}
        }
    }
}

// ─── La suma de pendientes de los durables de un mismo contrato ──────────────

/// `flux_consumer_pending` de los `watch` de un servicio, por `(subject, consumer)`.
///
/// Las métricas de consumo de un estado usan `subject = "state.<contrato>"` y `consumer` = el
/// durable **sin** el sufijo de tenant: con él, cada tenant nuevo crearía series nuevas
/// (08-observability.md §2.2). Pero entonces dos `watch` del mismo contrato, uno por tenant,
/// publicarían el mismo gauge y **se pisarían** — el último en escribir se llevaría el valor.
/// Este registro guarda el `pending` de cada durable y publica **la suma**: es lo que 10-state.md
/// §9 manda («`flux_consumer_pending` como la suma de los suyos»).
#[derive(Debug, Default)]
pub(crate) struct PendingSum {
    inner: Mutex<HashMap<(String, String), HashMap<String, u64>>>,
}

impl PendingSum {
    /// Anota el `pending` de un durable y devuelve la suma de los de su serie.
    pub(crate) fn set(&self, subject: &str, consumer: &str, durable: &str, pending: u64) -> u64 {
        let mut all = lock(&self.inner);
        let series = all
            .entry((subject.to_string(), consumer.to_string()))
            .or_default();
        series.insert(durable.to_string(), pending);
        series.values().sum()
    }

    /// Olvida un durable. Devuelve la suma de los que quedan, o `None` si no queda ninguno.
    pub(crate) fn forget(&self, subject: &str, consumer: &str, durable: &str) -> Option<u64> {
        let mut all = lock(&self.inner);
        let key = (subject.to_string(), consumer.to_string());
        let series = all.get_mut(&key)?;
        series.remove(durable);
        if series.is_empty() {
            all.remove(&key);
            return None;
        }
        Some(series.values().sum())
    }
}

// ─── onSynced ────────────────────────────────────────────────────────────────

/// ¿Está sincronizado nada más abrir? — 10-state.md §5.5, puntos 1 y 2.
///
/// Con el stream vacío, o con un durable al día, el consumidor informa `num_pending: 0` y
/// `num_ack_pending: 0** y no entrega nada (A-35): esperar a una entrada que quizá no llegue nunca
/// dejaría al servicio «sincronizando» para siempre.
pub(crate) fn synced_at_open(num_pending: u64, num_ack_pending: usize) -> bool {
    num_pending == 0 && num_ack_pending == 0
}

/// ¿Cierra la foto esta entrega? — 10-state.md §5.5, punto 3: la que llega con `pending == 0`, el
/// número que el broker pone en los metadatos de **cada** mensaje.
///
/// `num_pending` de `CONSUMER.INFO` no basta: cuenta lo no entregado y no lo entregado-y-sin-
/// terminar. Este SDK procesa las entradas **una a una**, así que cuando llega a esta pregunta
/// todas las anteriores ya terminaron —con éxito o con error—; con handlers concurrentes habría
/// que esperar además a los que siguen en vuelo.
pub(crate) fn synced_after_delivery(pending: u64) -> bool {
    pending == 0
}

/// La puerta de `on_synced`: se abre **una sola vez**.
struct SyncGate {
    fired: AtomicBool,
    callback: Option<Arc<dyn Fn() + Send + Sync>>,
    tx: watch::Sender<bool>,
}

/// Lo que pasó al abrir la puerta.
#[derive(Debug, PartialEq, Eq)]
enum Fired {
    /// Ya estaba abierta: el callback **no** se vuelve a llamar.
    AlreadyOpen,
    /// La abrió esta llamada.
    Opened,
    /// La abrió esta llamada, pero el callback de la aplicación entró en pánico.
    CallbackPanicked,
}

impl SyncGate {
    /// Abre la puerta, **una sola vez**.
    fn fire(&self) -> Fired {
        if self.fired.swap(true, Ordering::SeqCst) {
            return Fired::AlreadyOpen;
        }
        let mut result = Fired::Opened;
        if let Some(callback) = &self.callback {
            // Es código de la aplicación en una tarea de fondo: un pánico no debe matar el
            // consumo —el mismo bug que `catch_unwind` evita en `subscribe`—.
            if std::panic::catch_unwind(AssertUnwindSafe(|| callback())).is_err() {
                result = Fired::CallbackPanicked;
            }
        }
        // Después del callback: quien vea `synced` sabe que el callback ya se ejecutó.
        self.tx.send_replace(true);
        result
    }

    /// Como [`SyncGate::fire`], registrando por el `logger` un callback que entró en pánico.
    fn fire_logged(&self, bus: &Bus) {
        if self.fire() == Fired::CallbackPanicked {
            bus.log(
                LogLevel::Error,
                "pánico en el callback `on_synced` de un watch de estado. El consumo continúa.",
            );
        }
    }
}

// ─── El transporte real ──────────────────────────────────────────────────────

/// Por qué una petición al broker no devolvió respuesta.
enum RequestFailure {
    /// El broker denegó el subject (o el inbox): llega en milisegundos por la conexión.
    Denied(crate::security::Denial),
    /// `no responders`: el stream no existe, o existe sin `allow_direct` (A-32).
    NoResponders,
    /// Cualquier otro fallo, ya traducido.
    Other(FluxError),
}

impl RequestFailure {
    /// La traducción a un error de flux. `operation` es lo que se intentaba, en términos de flux.
    fn into_flux(self, operation: &str, stream: &str) -> FluxError {
        match self {
            Self::Denied(denial) => permission_denied(operation.to_string(), &denial),
            // La señal medida (A-32, A-48): nadie contesta, y un stream que no existe se ve
            // exactamente igual que uno sin `allow_direct`. El mensaje nombra las dos causas.
            Self::NoResponders => FluxError::NotProvisioned {
                kind: ResourceKind::Stream,
                name: stream.to_string(),
                denied: String::new(),
            },
            Self::Other(e) => e,
        }
    }
}

/// Un mensaje leído con `DIRECT.GET`.
struct DirectMessage {
    seq: u64,
    subject: String,
    payload: Vec<u8>,
}

impl Bus {
    /// Una petición al broker con la respuesta por el inbox privado, **vigilando denegaciones**
    /// (ver [`crate::security`]): sin esto, un permiso que falta es un timeout de 5 s.
    ///
    /// `subject` es el subject **exacto** al que publica la petición.
    async fn state_request(
        &self,
        subject: String,
        headers: Option<async_nats::HeaderMap>,
        payload: Vec<u8>,
    ) -> Result<async_nats::Message, RequestFailure> {
        let mut request = async_nats::Request::new()
            .payload(payload.into())
            .timeout(Some(STATE_REQUEST_TIMEOUT));
        if let Some(h) = headers {
            request = request.headers(h);
        }
        let sent = self
            .guarded(
                std::slice::from_ref(&subject),
                self.inner.client.send_request(subject.clone(), request),
            )
            .await;
        match sent {
            Err(denial) => Err(RequestFailure::Denied(denial)),
            Ok(Ok(message)) => Ok(message),
            Ok(Err(e)) => Err(match e.kind() {
                async_nats::RequestErrorKind::NoResponders => RequestFailure::NoResponders,
                async_nats::RequestErrorKind::TimedOut => RequestFailure::Other(transport(
                    format!("el broker no contestó en {STATE_REQUEST_TIMEOUT:?} a `{subject}`"),
                    e,
                )),
                _ => RequestFailure::Other(transport(
                    format!("fallo al pedir `{subject}` al broker"),
                    e,
                )),
            }),
        }
    }

    /// `DIRECT.GET` por subject: el último mensaje de una clave, o `None` si no hay.
    ///
    /// ```text
    /// $JS.API.DIRECT.GET.STATE_<DOM>.state.<dom>.<agr>.v<M>.<tenant>.<clave>
    /// ```
    ///
    /// No consume nada —no hay consumidor ni acuse— y la respuesta lleva el mensaje tal cual,
    /// con sus cabeceras. **Un cuerpo no sirve para saltarse el permiso**: la forma por subject
    /// lo rechaza con `408` (A-38), así que la forma de cuerpo `{"seq": n}` —que lee cualquier
    /// mensaje— no puede colarse por un permiso de subject.
    async fn state_direct_get(
        &self,
        stream: &str,
        subject: &str,
        operation: &str,
    ) -> Result<Option<DirectMessage>, FluxError> {
        let request_subject = format!("$JS.API.DIRECT.GET.{stream}.{subject}");
        let message = self
            .state_request(request_subject, None, Vec::new())
            .await
            .map_err(|f| f.into_flux(operation, stream))?;

        match message.status.map(|s| s.as_u16()) {
            // `404 Message Not Found`: la clave no existe (A-32).
            Some(404) => return Ok(None),
            Some(code) if code >= 400 => {
                return Err(transport(
                    format!(
                        "DIRECT.GET de `{subject}` contestó {code} {}",
                        message.description.as_deref().unwrap_or("")
                    ),
                    "respuesta de error del broker",
                ))
            }
            _ => {}
        }

        let header = |name: &str| {
            message
                .headers
                .as_ref()
                .and_then(|h| h.get(name))
                .map(ToString::to_string)
        };
        let seq = header("Nats-Sequence")
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| {
                transport(
                    format!("DIRECT.GET de `{subject}` no devolvió `Nats-Sequence`"),
                    "respuesta sin secuencia",
                )
            })?;
        Ok(Some(DirectMessage {
            seq,
            subject: header("Nats-Subject").unwrap_or_else(|| subject.to_string()),
            payload: message.payload.to_vec(),
        }))
    }
}

/// [`StateIo`] contra un bus real.
struct BusIo<'a> {
    bus: &'a Bus,
    stream: String,
    key: String,
}

impl StateIo for BusIo<'_> {
    async fn read(&self, subject: &str) -> Result<Current, FluxError> {
        let operation = format!(
            "leer la clave `{}` de `{}` antes de escribirla (DIRECT.GET)",
            self.key, self.stream
        );
        Ok(
            match self
                .bus
                .state_direct_get(&self.stream, subject, &operation)
                .await?
            {
                None => Current::Absent,
                Some(m) => current_of(m.seq, &m.payload),
            },
        )
    }

    async fn publish(
        &self,
        subject: &str,
        id: &str,
        expected: u64,
        bytes: &[u8],
    ) -> Result<Published, FluxError> {
        // **Siempre por JetStream** y siempre con la cabecera de secuencia: un `publish` de core
        // a un subject de estado lo captura el stream SIN acuse y SIN chequeo, y sobrescribe
        // (A-45). Una petición con respuesta es una publicación de JetStream —el stream contesta
        // con su acuse—, y deja inspeccionar el `err_code` tal cual lo manda el broker.
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(async_nats::header::NATS_MESSAGE_ID, id);
        headers.insert(
            async_nats::header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE,
            expected.to_string(),
        );
        let operation = format!(
            "escribir la clave `{}` en `{subject}`: solo el servicio dueño del dominio escribe \
             su estado",
            self.key
        );
        let reply = self
            .bus
            .state_request(subject.to_string(), Some(headers), bytes.to_vec())
            .await
            .map_err(|f| f.into_flux(&operation, &self.stream))?;
        parse_ack(&reply.payload)
    }
}

// ─── La API ──────────────────────────────────────────────────────────────────

/// El estado replicado de un [`Bus`]: `put`, `delete`, `get` y `watch`. Se obtiene con
/// [`Bus::state`].
#[derive(Clone)]
pub struct State {
    bus: Bus,
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("State")
            .field("bus", &self.bus)
            .finish_non_exhaustive()
    }
}

impl State {
    pub(crate) fn new(bus: Bus) -> Self {
        Self { bus }
    }

    fn resolve_tenant(&self, option: Option<&str>) -> Result<String, FluxError> {
        let in_progress = context::current().map(|c| c.tenant_id);
        resolve_tenant(
            option,
            in_progress.as_deref(),
            self.bus.inner.opts.tenant_id.as_deref(),
        )
    }

    /// El `dataschema` de un contrato: el mapa explícito, el índice `state` del bundle (L3) y,
    /// por último, la URL base. Nunca por red.
    fn dataschema_for(&self, c: &StateContract) -> Result<String, FluxError> {
        let name = c.name();
        let opts = &self.bus.inner.opts;
        if let Some(uri) = opts.schemas.get(&name).filter(|u| !u.is_empty()) {
            return Ok(uri.clone());
        }
        #[cfg(feature = "validation")]
        if let Some(uri) = opts
            .validation
            .bundle
            .as_ref()
            .and_then(|b| b.state_uri_for(&name))
        {
            return Ok(uri.to_string());
        }
        match &opts.schema_base_url {
            Some(base) => Ok(c.default_dataschema(base)),
            None => Err(FluxError::Config(format!(
                "no hay dataschema para el contrato de estado `{name}`. Declara \
                 schemas[\"{name}\"], schema_base_url, o pasa un bundle con el índice `state` en \
                 ConnectOptions::with_validation (10-state.md §10)"
            ))),
        }
    }

    // ─── put ─────────────────────────────────────────────────────────────────

    /// Escribe el valor actual de una clave — **solo el dueño del dominio**.
    ///
    /// Equivale a [`State::put_with`] con las opciones por defecto: tenant del evento en curso o
    /// de `connect()`, y clasificación `confidential`.
    ///
    /// # Errores
    ///
    /// Ver [`State::put_with`].
    pub async fn put<T: Serialize + ?Sized>(
        &self,
        contract: &str,
        key: &str,
        data: &T,
    ) -> Result<StateWrite, FluxError> {
        self.put_with(contract, key, data, PutOptions::default())
            .await
    }

    /// Como [`State::put`], con ajustes.
    ///
    /// **Es condicional** (10-state.md §3.2): no escribe si `aggregateVersion` no avanza
    /// —`stale`, que **no** es un error— y publica con `Nats-Expected-Last-Subject-Sequence`, de
    /// modo que dos instancias del dueño no pueden dejar un valor viejo como «actual». Un
    /// consumidor nuevo lo recibiría como la verdad: en un stream de estado no hay historia de la
    /// que corregirlo.
    ///
    /// `aggregateVersion` **lo pone el dueño** y va dentro de `data`: es el contador monotónico
    /// de su agregado. El SDK no lo inventa ni lo incrementa.
    ///
    /// # Errores
    ///
    /// **Antes de abrir ninguna petición**, y en este orden: [`FluxError::ReservedDomain`],
    /// [`FluxError::InvalidStateKey`], [`FluxError::InvalidTenantForState`],
    /// [`FluxError::MissingAggregateVersion`], `INVALID_DATACLASSIFICATION` (para `restricted`) y,
    /// con la feature `validation`, [`FluxError::SchemaValidation`] —que reporta **todos** los
    /// errores y cita la ruta del campo, no su valor—.
    ///
    /// Después: [`FluxError::StateConflict`] (RETRYABLE) si el chequeo de secuencia agota sus 3
    /// intentos, [`FluxError::NotProvisioned`] si el stream no existe o no tiene `allow_direct`,
    /// [`FluxError::PermissionDenied`] si la ACL no concede la clave, y [`FluxError::Transport`].
    pub async fn put_with<T: Serialize + ?Sized>(
        &self,
        contract: &str,
        key: &str,
        data: &T,
        opts: PutOptions,
    ) -> Result<StateWrite, FluxError> {
        let c = StateContract::parse(contract)?;
        validate_state_key(key)?;
        let tenant = self.resolve_tenant(opts.tenant_id.as_deref())?;

        let result = self.put_checked(&c, key, &tenant, data, &opts).await;
        self.account(&c, &result);
        result
    }

    async fn put_checked<T: Serialize + ?Sized>(
        &self,
        c: &StateContract,
        key: &str,
        tenant: &str,
        data: &T,
        opts: &PutOptions,
    ) -> Result<StateWrite, FluxError> {
        let raw = serde_json::value::to_raw_value(data)
            .map_err(|e| FluxError::Envelope(format!("`data` no es serializable a JSON: {e}")))?;
        let value: Value = serde_json::from_str(raw.get())
            .map_err(|e| FluxError::Envelope(format!("`data` no es JSON válido: {e}")))?;
        // `data` que no es un objeto es el mismo error que sin versión: §3.6, cuarta fila.
        let version = aggregate_version_of(&value)?;

        let classification = opts
            .classification
            .unwrap_or(DataClassification::Confidential);
        reject_restricted(classification)?;

        let ctx = context::current();
        let id = Uuid::now_v7().to_string();
        let (correlationid, causationid) = match ctx.as_ref() {
            Some(c) if !c.correlation_id.is_empty() => {
                (c.correlation_id.clone(), Some(c.causation_id.clone()))
            }
            _ => (id.clone(), None),
        };
        let (traceparent, tracestate) = match ctx.as_ref() {
            Some(c) if c.traceparent.is_some() => (c.traceparent.clone(), c.tracestate.clone()),
            _ => (
                self.bus.inner.opts.traceparent.as_ref().and_then(|f| f()),
                None,
            ),
        };

        let event = build_state_event(StateEventInput {
            contract: c.name(),
            tenant_id: tenant.to_string(),
            key: key.to_string(),
            id,
            source: self.bus.inner.source.clone(),
            time: Utc::now(),
            correlationid,
            producerversion: self.bus.inner.opts.version.clone(),
            classification,
            dataschema: self.dataschema_for(c)?,
            causationid,
            traceparent,
            tracestate,
            data: raw,
        })?;
        self.commit(c, key, tenant, event, version, opts.hook.as_deref())
            .await
    }

    // ─── delete ──────────────────────────────────────────────────────────────

    /// Borra una clave publicando una **lápida** — **solo el dueño del dominio**.
    ///
    /// Equivale a [`State::delete_with`] con las opciones por defecto.
    ///
    /// # Errores
    ///
    /// Ver [`State::delete_with`].
    pub async fn delete(
        &self,
        contract: &str,
        key: &str,
        aggregate_version: u64,
    ) -> Result<StateWrite, FluxError> {
        self.delete_with(contract, key, aggregate_version, DeleteOptions::default())
            .await
    }

    /// Como [`State::delete`], con ajustes.
    ///
    /// **`delete` es `put` de una lápida** (10-state.md §4): el mismo algoritmo, las mismas reglas
    /// de versión y el mismo resultado. `aggregate_version` es la versión **de la lápida** y debe
    /// superar la del valor que borra: una lápida con versión ≤ la actual es `stale`.
    ///
    /// - Borrar una clave que **nunca existió** escribe la lápida (`applied: true`).
    /// - Al entrar, el valor anterior **sale del stream**: sigue habiendo un mensaje por clave, y
    ///   el dato personal ya no está en él.
    /// - Se puede recrear una clave borrada con una versión **estrictamente mayor** que la de su
    ///   lápida. **La trampa**: un dueño que borra su fila y la recrea con `aggregateVersion: 1`
    ///   recibe `stale` y la clave **sigue borrada**, sin ningún error. Su contador tiene que
    ///   sobrevivir al borrado.
    ///
    /// # Errores
    ///
    /// Los de [`State::put_with`] que aplican: dominio reservado, clave, tenant, versión
    /// ([`FluxError::MissingAggregateVersion`] si es 0 o supera 2^53 − 1) y, con `validation`, la
    /// lápida contra su esquema fijo.
    pub async fn delete_with(
        &self,
        contract: &str,
        key: &str,
        aggregate_version: u64,
        opts: DeleteOptions,
    ) -> Result<StateWrite, FluxError> {
        let c = StateContract::parse(contract)?;
        validate_state_key(key)?;
        let tenant = self.resolve_tenant(opts.tenant_id.as_deref())?;

        let result = self
            .delete_checked(&c, key, &tenant, aggregate_version, &opts)
            .await;
        self.account(&c, &result);
        result
    }

    async fn delete_checked(
        &self,
        c: &StateContract,
        key: &str,
        tenant: &str,
        aggregate_version: u64,
        opts: &DeleteOptions,
    ) -> Result<StateWrite, FluxError> {
        let version = check_version_number(aggregate_version)?;
        let ctx = context::current();
        let id = Uuid::now_v7().to_string();
        let (correlationid, causationid) = match ctx.as_ref() {
            Some(c) if !c.correlation_id.is_empty() => {
                (c.correlation_id.clone(), Some(c.causation_id.clone()))
            }
            _ => (id.clone(), None),
        };
        let (traceparent, tracestate) = match ctx.as_ref() {
            Some(c) if c.traceparent.is_some() => (c.traceparent.clone(), c.tracestate.clone()),
            _ => (
                self.bus.inner.opts.traceparent.as_ref().and_then(|f| f()),
                None,
            ),
        };
        let event = build_state_tombstone(StateTombstoneInput {
            contract: c.name(),
            tenant_id: tenant.to_string(),
            key: key.to_string(),
            id,
            source: self.bus.inner.source.clone(),
            time: Utc::now(),
            correlationid,
            producerversion: self.bus.inner.opts.version.clone(),
            aggregate_version: version,
            causationid,
            traceparent,
            tracestate,
        })?;
        self.commit(c, key, tenant, event, version, opts.hook.as_deref())
            .await
    }

    /// Lo común de `put` y `delete`: L3, firma, serialización y el algoritmo.
    async fn commit(
        &self,
        c: &StateContract,
        key: &str,
        tenant: &str,
        event: Event,
        version: u64,
        hook: Option<&dyn WriteHook>,
    ) -> Result<StateWrite, FluxError> {
        // L3 ANTES de leer ni escribir, y antes de firmar: firmar lo que se va a rechazar es
        // trabajo tirado. Reporta todos los errores, citando la ruta y no el valor.
        #[cfg(feature = "validation")]
        if let Some(validator) = &self.bus.inner.validator {
            validator.validate_state(&event, &c.name())?;
        }

        // Firmar es lo último antes de serializar — 07-signing.md §5.
        #[cfg(feature = "signing")]
        let event = match &self.bus.inner.signer {
            Some(signer) => signer.sign(event)?,
            None => event,
        };

        let bytes = serialize(&event)?;
        let subject = c.subject_of(tenant, key);
        let io = BusIo {
            bus: &self.bus,
            stream: c.stream_name(),
            key: key.to_string(),
        };
        run_write(
            &io,
            &WriteJob {
                subject: &subject,
                key,
                id: &event.id,
                bytes: &bytes,
                version,
            },
            hook,
        )
        .await
    }

    /// Cuenta `flux_state_writes_total{contract, outcome}`. Ver [`outcome_of`].
    fn account(&self, c: &StateContract, result: &Result<StateWrite, FluxError>) {
        if let Some(outcome) = outcome_of(result) {
            self.bus.inner.metrics.state_write(&c.name(), outcome);
        }
    }

    // ─── get ─────────────────────────────────────────────────────────────────

    /// Lee el valor actual de una clave, con el tenant de `connect()` o del evento en curso.
    ///
    /// # Errores
    ///
    /// Ver [`State::get_with`].
    pub async fn get(&self, contract: &str, key: &str) -> Result<Option<StateEntry>, FluxError> {
        self.get_with(contract, key, GetOptions::default()).await
    }

    /// Como [`State::get`], con ajustes.
    ///
    /// Una lectura puntual por `DIRECT.GET` sobre el subject exacto: no hay consumidor ni acuse.
    /// `None` si la clave no existe **y** si tiene una lápida.
    ///
    /// **Es una lectura de réplica** (§3.7): con `replicas: 3` puede contestarla una réplica que
    /// aún no ha aplicado la última escritura, así que **no hay lectura de tu propia escritura
    /// garantizada**. Quien la necesite la deriva del `revision` del `put`.
    ///
    /// # Errores
    ///
    /// [`FluxError::ReservedDomain`], [`FluxError::InvalidStateKey`],
    /// [`FluxError::InvalidTenantForState`]; [`FluxError::NotProvisioned`] si el stream no existe
    /// **o existe sin `allow_direct`** —nadie contesta y los dos casos se ven igual—;
    /// [`FluxError::PermissionDenied`] si la ACL no concede ese tenant; y
    /// [`FluxError::StateSubjectMismatch`] si el mensaje miente sobre su tenant, su clave o su
    /// `type`: se lanza en vez de devolverlo.
    pub async fn get_with(
        &self,
        contract: &str,
        key: &str,
        opts: GetOptions,
    ) -> Result<Option<StateEntry>, FluxError> {
        let c = StateContract::parse(contract)?;
        validate_state_key(key)?;
        let tenant = self.resolve_tenant(opts.tenant_id.as_deref())?;
        let subject = c.subject_of(&tenant, key);
        let operation = format!(
            "leer la clave `{key}` de `{}` (DIRECT.GET, tenant `{tenant}`)",
            c.stream_name()
        );
        let Some(m) = self
            .bus
            .state_direct_get(&c.stream_name(), &subject, &operation)
            .await?
        else {
            return Ok(None);
        };
        if m.subject != subject {
            return Err(transport(
                format!(
                    "DIRECT.GET de `{subject}` devolvió un mensaje de `{}`",
                    m.subject
                ),
                "respuesta de otro subject",
            ));
        }
        let incoming = self
            .bus
            .check_incoming(&c, &m.subject, &m.payload, m.seq)
            .map_err(Reject::into_error)?;
        Ok((!incoming.entry.deleted).then_some(incoming.entry))
    }
}

// ─── Qué comprueba el SDK al consumir — 10-state.md §5.4 ─────────────────────

/// El veredicto de las comprobaciones que no necesitan nada más que el sobre.
#[derive(Debug)]
pub(crate) enum Verdict {
    /// `STATE_SUBJECT_MISMATCH` o `restricted`: el mensaje miente. POISON.
    Poison(FluxError),
    /// `MISSING_AGGREGATE_VERSION` (o L3): PERMANENT.
    Permanent(FluxError),
}

/// Las comprobaciones de §5.4, **en su orden**, sobre un sobre ya parseado: identidad
/// (`tenantid`, `subject`, `type`), clasificación y versión. Lo que falla **nunca** se aplica y
/// **nunca** llega al handler.
///
/// La clave y el tenant de la entrada salen del **subject de NATS** —lo que decidió el broker—,
/// no del sobre: el sobre solo los repite para quien lo lea fuera del broker.
pub(crate) fn check_state_message(
    c: &StateContract,
    nats_subject: &str,
    event: &Event,
    revision: u64,
) -> Result<StateEntry, Verdict> {
    let mismatch = |attribute: &str, envelope: &str, subject: &str| {
        Verdict::Poison(FluxError::StateSubjectMismatch {
            attribute: attribute.to_string(),
            envelope: envelope.to_string(),
            subject: subject.to_string(),
        })
    };

    let tail = nats_subject
        .strip_prefix(&format!("{}.", c.prefix()))
        .and_then(|t| t.split_once('.'));
    let Some((tenant, key)) = tail else {
        return Err(mismatch(
            "subject de NATS",
            "",
            &format!("{nats_subject} (no es un subject de {})", c.name()),
        ));
    };

    if event.tenantid != tenant {
        return Err(mismatch("tenantid", &event.tenantid, tenant));
    }
    if event.aggregate_id.as_deref() != Some(key) {
        return Err(mismatch(
            "subject",
            event.aggregate_id.as_deref().unwrap_or("<ausente>"),
            key,
        ));
    }
    let deleted = if event.event_type == c.event_type() {
        false
    } else if event.event_type == c.tombstone_type() {
        true
    } else {
        // Decidido provisionalmente (§12, 8): un `type` que no es el del contrato también miente.
        return Err(mismatch(
            "type",
            &event.event_type,
            &format!("{} o {}", c.event_type(), c.tombstone_type()),
        ));
    };

    // `restricted` es una clasificación válida para un evento y POISON para un estado: un dato de
    // 24 h de retención no cabe en un almacén que no caduca (§7).
    if event.dataclassification == DataClassification::Restricted {
        return Err(Verdict::Poison(
            FluxError::poison(
                "una entrada de estado no puede llevar `dataclassification: restricted`: es un \
                 dato de 24 h de retención y el estado no caduca (10-state.md §7)",
            )
            .with_code("INVALID_DATACLASSIFICATION"),
        ));
    }

    let data: Value = serde_json::from_str(event.data.get()).map_err(|_| {
        Verdict::Permanent(FluxError::MissingAggregateVersion {
            reason: "`data` no es JSON válido".to_string(),
        })
    })?;
    let aggregate_version = aggregate_version_of(&data).map_err(Verdict::Permanent)?;

    Ok(StateEntry {
        key: key.to_string(),
        tenant_id: tenant.to_string(),
        deleted,
        data: (!deleted).then(|| event.data.clone()),
        aggregate_version,
        revision,
    })
}

/// Una entrada ya comprobada, lista para el handler.
pub(crate) struct Incoming {
    pub(crate) event: Event,
    pub(crate) entry: StateEntry,
    /// El evento habría sido rechazado en modo `require` de la firma: se acepta (`warn`), pero se
    /// cuenta — 07-signing.md §7.1.
    pub(crate) signature_warning: bool,
}

/// Por qué una entrada no llega al handler.
pub(crate) enum Reject {
    /// No es un CloudEvent válido: no hay sobre que conservar, y su cuerpo **no** se copia a la
    /// DLQ de estado en ninguna forma (podría contener el dato).
    Unreadable(FluxError),
    /// Legible pero miente sobre su identidad, o lleva `restricted`: POISON.
    Poison(Box<Event>, FluxError),
    /// Sin `aggregateVersion` válido, o incumple su esquema: PERMANENT.
    Permanent(Box<Event>, FluxError),
}

impl Reject {
    /// El error, para `get`, que lo lanza en vez de enrutarlo.
    pub(crate) fn into_error(self) -> FluxError {
        match self {
            Self::Unreadable(e) | Self::Poison(_, e) | Self::Permanent(_, e) => e,
        }
    }
}

impl Bus {
    /// Parsea y comprueba un mensaje de estado — 10-state.md §5.4. Sirve a `get` y a `watch`.
    pub(crate) fn check_incoming(
        &self,
        c: &StateContract,
        nats_subject: &str,
        payload: &[u8],
        revision: u64,
    ) -> Result<Incoming, Reject> {
        // 1. Un CloudEvent válido con las cuatro extensiones: los POISON de siempre.
        let event = parse_event(payload).map_err(Reject::Unreadable)?;

        // La firma se comprueba antes que cualquier validación de payload — 07-signing.md §5.
        #[cfg(feature = "signing")]
        let signature_warning = match self
            .inner
            .verifier
            .as_ref()
            .map_or(Ok(None), |v| v.check(&event))
        {
            Ok(aviso) => aviso.is_some(),
            Err(e) => return Err(Reject::Poison(Box::new(event), e)),
        };
        #[cfg(not(feature = "signing"))]
        let signature_warning = false;

        // 2-4. Identidad, clasificación y versión.
        let entry = match check_state_message(c, nats_subject, &event, revision) {
            Ok(entry) => entry,
            Err(Verdict::Poison(e)) => return Err(Reject::Poison(Box::new(event), e)),
            Err(Verdict::Permanent(e)) => return Err(Reject::Permanent(Box::new(event), e)),
        };

        // 5. L3 al consumir, si se pidió.
        if let Err(e) = self.validate_state_on_consume(&event, c) {
            return Err(Reject::Permanent(Box::new(event), e));
        }

        Ok(Incoming {
            event,
            entry,
            signature_warning,
        })
    }

    // Sin la feature `validation` esto es un no-op: la firma DEBE ser idéntica en las dos
    // compilaciones, y por eso el `allow` va solo en esa variante (ver `validate_on_consume`).
    #[cfg_attr(
        not(feature = "validation"),
        allow(clippy::unused_self, clippy::unnecessary_wraps)
    )]
    fn validate_state_on_consume(&self, event: &Event, c: &StateContract) -> Result<(), FluxError> {
        #[cfg(feature = "validation")]
        if self.inner.opts.validation.on_consume {
            if let Some(validator) = &self.inner.validator {
                validator.validate_state(event, &c.name())?;
            }
        }
        #[cfg(not(feature = "validation"))]
        let _ = (event, c);
        Ok(())
    }
}

// ─── watch ───────────────────────────────────────────────────────────────────

/// Compara la configuración efectiva de un durable de estado con la pedida: la canónica de
/// 03-delivery.md §2 **más** `deliver_policy: last_per_subject` y el filtro. El servidor devuelve
/// lo que quiere sin avisar (`ack_wait` sobrescrito por `backoff[0]`); un durable que
/// aprovisionó otro, con otro filtro, entregaría **otro** conjunto de claves del que se pidió.
fn assert_state_config_honored(
    durable: &str,
    requested: &consumer::pull::Config,
    effective: &consumer::Config,
) -> Result<(), FluxError> {
    let mut differences: Vec<ConfigDifference> =
        match assert_config_honored(durable, requested, effective) {
            Ok(()) => Vec::new(),
            Err(FluxError::ConsumerConfigMismatch { differences, .. }) => differences,
            Err(other) => return Err(other),
        };
    if requested.deliver_policy != effective.deliver_policy {
        differences.push(ConfigDifference {
            field: "deliver_policy".to_string(),
            requested: format!("{:?}", requested.deliver_policy),
            effective: format!("{:?}", effective.deliver_policy),
        });
    }
    let effective_filter = if effective.filter_subjects.is_empty() {
        effective.filter_subject.clone()
    } else {
        effective.filter_subjects.join(",")
    };
    if requested.filter_subject != effective_filter {
        differences.push(ConfigDifference {
            field: "filter_subject".to_string(),
            requested: requested.filter_subject.clone(),
            effective: effective_filter,
        });
    }
    if differences.is_empty() {
        return Ok(());
    }
    Err(FluxError::ConsumerConfigMismatch {
        durable: durable.to_string(),
        differences,
    })
}

/// Todo lo que el bucle de un `watch` necesita saber de él.
struct WatchCtx {
    contract: StateContract,
    /// `state.<contrato>`: la etiqueta `subject` de las métricas de consumo (§9).
    metric_subject: String,
    /// El durable, **con** el sufijo de tenant: es el `dlqconsumer`.
    durable: String,
    /// El durable **sin** el sufijo de tenant: la etiqueta `consumer` de las métricas, para que un
    /// tenant nuevo no cree series nuevas.
    consumer_label: String,
    /// El `dataschema` del contrato, para el sobre sintetizado de un mensaje ilegible.
    synth_dataschema: String,
    gate: Arc<SyncGate>,
}

impl State {
    /// Observa un contrato: **una foto completa y después los cambios, por el mismo canal**.
    ///
    /// Equivale a [`State::watch_with`] con las opciones por defecto.
    ///
    /// # Errores
    ///
    /// Ver [`State::watch_with`].
    pub async fn watch<H: StateHandler>(
        &self,
        contract: &str,
        handler: H,
    ) -> Result<StateWatch, FluxError> {
        self.watch_with(contract, handler, WatchOptions::default())
            .await
    }

    /// Como [`State::watch`], con ajustes.
    ///
    /// Es un consumidor durable de `STATE_<DOM>` (10-state.md §5.2): `last_per_subject`, la
    /// configuración canónica de 03-delivery.md §2 y el filtro
    /// `state.<dom>.<agr>.v<M>.<tenant>.*` (o `….*.*` sin tenant). Un consumidor que estuvo caído
    /// recibe **solo el último valor** de cada clave que cambió, y un valor reemplazado mientras
    /// estaba en vuelo **no se reentrega**: nunca se aplica un valor que ya no existe.
    ///
    /// El `watch` **no** necesita `STREAM.INFO.STATE_<DOM>` —que lista todas las claves y todos
    /// los tenants (A-39)—: `CONSUMER.INFO` distingue «el consumidor no existe» (10014) de «el
    /// stream no existe» (10059) (A-48).
    ///
    /// # Errores
    ///
    /// Antes de la red: [`FluxError::ReservedDomain`], [`FluxError::InvalidSubject`] (contrato) e
    /// [`FluxError::InvalidTenantForState`]; y [`FluxError::TenantIsolation`] si la conexión es
    /// `strict` y no hay tenant.
    ///
    /// Después: [`FluxError::NotProvisioned`] si el stream **o el durable** no existen y la ACL no
    /// concede crearlos; [`FluxError::PermissionDenied`] si ni siquiera se pueden consultar, o si
    /// no se pueden pedir mensajes (`CONSUMER.MSG.NEXT`): el broker no contesta a un pull
    /// denegado, y sin esta comprobación el consumidor quedaría inerte y parecería sano; y
    /// [`FluxError::ConsumerConfigMismatch`] si un durable aprovisionado no es el pedido.
    pub async fn watch_with<H: StateHandler>(
        &self,
        contract: &str,
        handler: H,
        opts: WatchOptions,
    ) -> Result<StateWatch, FluxError> {
        let bus = &self.bus;
        let c = StateContract::parse(contract)?;
        let tenant = resolve_watch_tenant(
            opts.tenant_id.as_deref(),
            bus.inner.opts.tenant_id.as_deref(),
        )?;
        // Con `strict`, observar sin filtro de tenant es un error de configuración: un filtro
        // opcional es un filtro que alguien olvidará, y el fallo —ver los datos de otro tenant—
        // no produce ningún error (09-multitenancy.md §3). Aquí `system` SÍ cuenta como tenant.
        if tenant.is_none() && bus.inner.opts.tenant_isolation == TenantIsolation::Strict {
            return Err(FluxError::TenantIsolation {
                subject: format!("state.{}", c.name()),
                reason: "no hay tenant ni en `WatchOptions::with_tenant_id` ni en \
                         `ConnectOptions::with_tenant_id`: sin filtro este `watch` vería el \
                         estado de TODOS los tenants"
                    .to_string(),
            });
        }

        let service = &bus.inner.opts.service;
        let stream_name = c.stream_name();
        let durable = state_durable_name(service, &c.name(), tenant.as_deref())?;
        let consumer_label = state_durable_name(service, &c.name(), None)?;

        let requested = consumer::pull::Config {
            durable_name: Some(durable.clone()),
            filter_subject: c.watch_filter(tenant.as_deref()),
            ack_policy: AckPolicy::Explicit,
            // `ack_wait` DEBE ser `backoff[0]`: JetStream lo sobrescribe sin avisar — 03-delivery.md
            // §2.1.
            ack_wait: DEFAULT_ACK_WAIT,
            max_deliver: i64::from(DEFAULT_MAX_DELIVER),
            backoff: CANONICAL_BACKOFF.to_vec(),
            max_ack_pending: i64::from(opts.max_ack_pending.unwrap_or(DEFAULT_MAX_ACK_PENDING)),
            // La foto: el último valor de cada clave del filtro, y después los cambios.
            deliver_policy: DeliverPolicy::LastPerSubject,
            replay_policy: ReplayPolicy::Instant,
            ..Default::default()
        };

        let consumer = self
            .obtain_consumer(&stream_name, &durable, requested.clone())
            .await?;
        assert_state_config_honored(&durable, &requested, &consumer.cached_info().config)?;

        // El `DLQ_<DOM>` de un dominio con estado captura también `dlq.state.<dom>.>`. Se
        // comprueba al abrir, como en los eventos: no descubrir en el primer fallo que no hay
        // dónde enrutarlo.
        self.ensure_state_dlq(&c).await?;
        bus.ensure_pull_allowed(&stream_name, &durable).await?;

        let info = consumer.cached_info();
        let immediate = synced_at_open(info.num_pending, info.num_ack_pending);

        let (stop_tx, stop_rx) = watch::channel(false);
        let (synced_tx, synced_rx) = watch::channel(false);
        lock(&bus.inner.stops).push(stop_tx.clone());

        let gate = Arc::new(SyncGate {
            fired: AtomicBool::new(false),
            callback: opts.on_synced.clone(),
            tx: synced_tx,
        });
        // Stream vacío (o durable al día): `onSynced` de inmediato, ANTES de entregar nada y
        // aunque no llegue ninguna entrada después (§5.5, punto 2).
        if immediate {
            gate.fire_logged(bus);
        }

        // Solo para pruebas: escribir tras abrir y ANTES del primer pull.
        if let Some(after_open) = &opts.after_open {
            after_open().await;
        }

        let messages = consumer
            .messages()
            .await
            .map_err(|e| transport(format!("no se pudo iniciar el consumo de `{durable}`"), e))?;

        let ctx = Arc::new(WatchCtx {
            metric_subject: format!("state.{}", c.name()),
            durable: durable.clone(),
            consumer_label,
            synth_dataschema: self
                .dataschema_for(&c)
                .unwrap_or_else(|_| c.default_dataschema("https://schemas.internal")),
            contract: c.clone(),
            gate: Arc::clone(&gate),
        });

        self.spawn_pending_poll(consumer.clone(), &ctx, &stop_tx);

        self.spawn_watch_loop(messages, stop_rx, Arc::new(handler), ctx);

        Ok(StateWatch {
            contract: c.name(),
            tenant_id: tenant,
            durable,
            stop: stop_tx,
            synced: synced_rx,
        })
    }

    /// El bucle de consumo de un `watch`: una entrada a la vez, en el orden en que el broker las
    /// entrega, hasta que se pide parar.
    fn spawn_watch_loop(
        &self,
        mut messages: consumer::pull::Stream,
        mut stop_rx: watch::Receiver<bool>,
        handler: Arc<dyn StateHandler>,
        ctx: Arc<WatchCtx>,
    ) {
        let loop_bus = self.bus.clone();
        let loop_ctx = ctx;
        tokio::spawn(async move {
            loop {
                let msg = tokio::select! {
                    _ = stop_rx.changed() => break,
                    next = messages.next() => match next {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            // Un error del stream de mensajes NO debe romper el bucle:
                            // async-nats reconecta por dentro.
                            loop_bus.log(LogLevel::Error, &format!(
                                "error leyendo de `{}`: {e}. El consumo continúa.",
                                loop_ctx.durable
                            ));
                            continue;
                        }
                        None => break,
                    },
                };

                // La que lleva `pending == 0` cierra la foto, pero solo DESPUÉS de procesarla:
                // `onSynced` promete que el handler ya terminó con todo lo que había al abrir.
                let closes_the_snapshot = msg
                    .info()
                    .ok()
                    .is_some_and(|i| synced_after_delivery(i.pending));

                // Un pánico FUERA del handler rompería el bucle y el consumidor dejaría de
                // consumir en silencio — el mismo bug que `subscribe` evita.
                let dispatched =
                    AssertUnwindSafe(loop_bus.state_dispatch(msg, &loop_ctx, handler.as_ref()))
                        .catch_unwind()
                        .await;
                if dispatched.is_err() {
                    loop_bus.log(
                        LogLevel::Error,
                        &format!(
                            "pánico despachando `{}`. La entrada no se confirma y será \
                             reentregada; el consumo continúa.",
                            loop_ctx.metric_subject
                        ),
                    );
                }
                if closes_the_snapshot {
                    loop_ctx.gate.fire_logged(&loop_bus);
                }
            }
            // El `pending` de un durable que se va no debe seguir sumando.
            if let Some(rest) = loop_bus.inner.state_pending.forget(
                &loop_ctx.metric_subject,
                &loop_ctx.consumer_label,
                &loop_ctx.durable,
            ) {
                loop_bus.inner.metrics.consumer_pending(
                    &loop_ctx.metric_subject,
                    &loop_ctx.consumer_label,
                    rest,
                );
            }
        });
    }

    /// El durable: **pregunta primero** si existe y solo si no existe lo crea.
    ///
    /// ```text
    /// CONSUMER.INFO → existe                    → lo usa tal cual
    /// CONSUMER.INFO → 10059 «stream not found»  → NotProvisioned (el stream de estado)
    /// CONSUMER.INFO → 10014 «consumer not found»
    ///   + CONSUMER.CREATE concedido             → lo crea (desarrollo, o una ACL amplia)
    ///   + CONSUMER.CREATE denegado              → NotProvisioned (el durable)
    /// CONSUMER.INFO denegado                    → PermissionDenied (ni siquiera puede mirar)
    /// ```
    ///
    /// Sin `STREAM.INFO` (A-39, A-48): el código de la respuesta distingue las dos ausencias.
    async fn obtain_consumer(
        &self,
        stream_name: &str,
        durable: &str,
        requested: consumer::pull::Config,
    ) -> Result<consumer::Consumer<consumer::pull::Config>, FluxError> {
        let bus = &self.bus;
        let info_subject = format!("$JS.API.CONSUMER.INFO.{stream_name}.{durable}");
        match bus
            .guarded(
                &[info_subject],
                bus.inner
                    .js
                    .get_consumer_from_stream::<consumer::pull::Config, _, _>(durable, stream_name),
            )
            .await
        {
            Ok(Ok(existing)) => return Ok(existing),
            Ok(Err(e)) => match e.kind() {
                ConsumerErrorKind::JetStream(je)
                    if je.error_code() == ErrorCode::STREAM_NOT_FOUND =>
                {
                    return Err(FluxError::NotProvisioned {
                        kind: ResourceKind::Stream,
                        name: stream_name.to_string(),
                        denied: String::new(),
                    });
                }
                ConsumerErrorKind::JetStream(je)
                    if je.error_code() == ErrorCode::CONSUMER_NOT_FOUND => {}
                _ => {
                    return Err(transport(
                        format!(
                            "no se pudo consultar el consumidor `{durable}` en `{stream_name}`"
                        ),
                        e,
                    ))
                }
            },
            Err(denial) => {
                return Err(permission_denied(
                    format!(
                        "consultar el consumidor `{durable}` en `{stream_name}` (la ACL del \
                         observador debe conceder `$JS.API.CONSUMER.INFO.{stream_name}.{durable}`: \
                         es lo único que necesita si lo aprovisiona infraestructura)"
                    ),
                    &denial,
                ))
            }
        }

        // No existe: crearlo, si la ACL lo permite.
        let create_subject = format!(
            "$JS.API.CONSUMER.CREATE.{stream_name}.{durable}.{}",
            requested.filter_subject
        );
        match bus
            .guarded(
                &[create_subject],
                bus.inner
                    .js
                    .create_consumer_on_stream(requested, stream_name),
            )
            .await
        {
            Ok(Ok(created)) => Ok(created),
            Ok(Err(create_error)) => {
                // Otra instancia del servicio pudo crearlo entre la pregunta y la creación.
                if let Ok(existing) = bus
                    .inner
                    .js
                    .get_consumer_from_stream::<consumer::pull::Config, _, _>(durable, stream_name)
                    .await
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

    /// El `DLQ_<DOM>` de un dominio con estado: `dlq.<dom>.>` **y** `dlq.state.<dom>.>`.
    ///
    /// Como los demás streams, se **comprueba** y solo se crea si no existe y la ACL lo permite
    /// (una comodidad de desarrollo): en producción lo provisiona `flux provision`.
    async fn ensure_state_dlq(&self, c: &StateContract) -> Result<(), FluxError> {
        let name = dlq_stream_name(&c.domain);
        self.bus
            .ensure(
                &name,
                stream::Config {
                    name: name.clone(),
                    subjects: vec![
                        format!("dlq.{}.>", c.domain),
                        format!("dlq.state.{}.>", c.domain),
                    ],
                    storage: stream::StorageType::File,
                    retention: RetentionPolicy::Limits,
                    discard: DiscardPolicy::Old,
                    max_age: DLQ_STREAM_MAX_AGE,
                    ..Default::default()
                },
            )
            .await
    }

    /// La segunda fuente de `flux_consumer_pending`: el sondeo periódico de `num_pending`
    /// (08-observability.md §2.3). Los metadatos de cada entrega son gratis y frescos, pero si el
    /// bucle muere dejan de llegar y el gauge se queda plano.
    fn spawn_pending_poll(
        &self,
        consumer: consumer::Consumer<consumer::pull::Config>,
        ctx: &Arc<WatchCtx>,
        stop_tx: &watch::Sender<bool>,
    ) {
        let every = self.bus.inner.opts.pending_poll;
        if every.is_zero() || self.bus.inner.opts.metrics.is_none() {
            return;
        }
        let bus = self.bus.clone();
        let ctx = Arc::clone(ctx);
        let mut stop = stop_tx.subscribe();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.tick().await; // el primer tick de un interval es inmediato
            loop {
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = tick.tick() => {
                        // Telemetría: un fallo del sondeo no afecta al consumo.
                        if let Ok(info) = consumer.get_info().await {
                            bus.state_report_pending(&ctx, info.num_pending);
                        }
                    }
                }
            }
        });
    }
}

// ─── despacho ────────────────────────────────────────────────────────────────

/// `(tenant, clave)` del subject de NATS de una entrada: los tokens 5.º y 6.º.
fn subject_tokens(nats_subject: &str) -> (&str, &str) {
    let mut tokens = nats_subject.split('.').skip(4);
    (
        tokens.next().unwrap_or("system"),
        tokens.next().unwrap_or("desconocida"),
    )
}

impl Bus {
    /// Anota el `pending` de este durable y publica la **suma** de los de su serie.
    fn state_report_pending(&self, w: &WatchCtx, pending: u64) {
        let sum =
            self.inner
                .state_pending
                .set(&w.metric_subject, &w.consumer_label, &w.durable, pending);
        self.inner
            .metrics
            .consumer_pending(&w.metric_subject, &w.consumer_label, sum);
    }

    async fn state_dispatch(
        &self,
        msg: async_nats::jetstream::Message,
        w: &WatchCtx,
        handler: &dyn StateHandler,
    ) {
        let info = msg.info().ok();
        let attempt = info
            .as_ref()
            .map_or(1, |i| u32::try_from(i.delivered).unwrap_or(1).max(1));
        let revision = info.as_ref().map_or(0, |i| i.stream_sequence);
        if let Some(i) = &info {
            self.state_report_pending(w, i.pending);
        }
        let subject = msg.subject.to_string();
        let inicio = Instant::now();

        // POISON y PERMANENT se detectan ANTES del handler: lo que falla nunca se aplica.
        let incoming = match self.check_incoming(&w.contract, &subject, &msg.payload, revision) {
            Ok(i) => i,
            Err(reject) => {
                self.state_reject(w, &msg, &subject, revision, attempt, reject)
                    .await;
                return;
            }
        };
        let Incoming {
            event,
            entry,
            signature_warning,
        } = incoming;
        let key = entry.key.clone();

        let delivery = Delivery {
            attempt,
            max_attempts: DEFAULT_MAX_DELIVER,
            subject: subject.clone(),
            durable: w.durable.clone(),
        };
        let outcome = self
            .run_state_handler(handler, &msg, &event, entry, delivery)
            .await;

        let handler_err: HandlerError = match outcome {
            Ok(Ok(())) => {
                // El SDK confirma cuando el handler termina SIN error, nunca antes.
                let _ = msg.ack().await;
                self.inner.metrics.event_consumed(
                    &w.metric_subject,
                    &w.consumer_label,
                    if signature_warning {
                        ConsumeOutcome::InvalidSignature
                    } else {
                        ConsumeOutcome::Ok
                    },
                );
                self.inner.metrics.handler_duration(
                    &w.metric_subject,
                    &w.consumer_label,
                    inicio.elapsed().as_secs_f64(),
                );
                return;
            }
            Ok(Err(e)) => e,
            Err(_) => Box::new(
                FluxError::permanent(format!("pánico en el handler de {}", w.metric_subject))
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
                    "RETRYABLE {} en {} (intento {attempt}/{budget}), reintento en {delay:?}",
                    c.code, w.metric_subject
                ),
            );
            self.inner
                .metrics
                .event_retried(&w.metric_subject, &w.consumer_label, attempt);
            let _ = msg.ack_with(AckKind::Nak(Some(delay))).await;
            return;
        }

        self.state_route_to_dlq(
            w,
            &msg,
            event,
            &key,
            revision,
            &handler_err,
            &c,
            attempt,
            inicio,
            signature_warning,
        )
        .await;
    }

    /// Ejecuta el handler emitiendo WIP cada `ack_wait / 2` mientras siga vivo, con el contexto
    /// del evento instalado para que un `publish` o un `put` a cualquier profundidad herede
    /// `correlationid`, `causationid`, tenant y `traceparent`.
    #[allow(clippy::type_complexity)]
    async fn run_state_handler(
        &self,
        handler: &dyn StateHandler,
        msg: &async_nats::jetstream::Message,
        event: &Event,
        entry: StateEntry,
        delivery: Delivery,
    ) -> Result<HandlerResult, Box<dyn std::any::Any + Send>> {
        let call = context::scope(
            Some(EventContext::from_event(event)),
            handler.call(entry, delivery),
        );
        let call = AssertUnwindSafe(call).catch_unwind();
        tokio::pin!(call);

        let mut wip = tokio::time::interval(DEFAULT_ACK_WAIT / 2);
        wip.tick().await; // el primer tick de un interval es inmediato

        loop {
            tokio::select! {
                result = &mut call => return result,
                _ = wip.tick() => {
                    let _ = msg.ack_with(AckKind::Progress).await;
                }
            }
        }
    }

    async fn state_reject(
        &self,
        w: &WatchCtx,
        msg: &async_nats::jetstream::Message,
        subject: &str,
        revision: u64,
        attempt: u32,
        reject: Reject,
    ) {
        match reject {
            Reject::Unreadable(err) => {
                self.state_poison(w, msg, subject, revision, attempt, None, err)
                    .await;
            }
            Reject::Poison(event, err) => {
                self.state_poison(w, msg, subject, revision, attempt, Some(*event), err)
                    .await;
            }
            Reject::Permanent(event, err) => {
                let event = *event;
                let (_, key) = subject_tokens(subject);
                let c = self.inner.classifier.classify(&err);
                let handler_err: HandlerError = Box::new(err);
                self.state_route_to_dlq(
                    w,
                    msg,
                    event,
                    key,
                    revision,
                    &handler_err,
                    &c,
                    attempt,
                    Instant::now(),
                    false,
                )
                .await;
            }
        }
    }

    /// POISON: `term()` + DLQ de estado + **alerta** (`on_poison`), y la entrada **no** se aplica:
    /// la clave conserva su valor anterior. La clave que se registra es la **del subject** —la
    /// que decidió el broker—, no la del sobre.
    #[allow(clippy::too_many_arguments)]
    async fn state_poison(
        &self,
        w: &WatchCtx,
        msg: &async_nats::jetstream::Message,
        subject: &str,
        revision: u64,
        attempt: u32,
        original: Option<Event>,
        err: FluxError,
    ) {
        let (tenant, key) = subject_tokens(subject);
        let code = err.code().unwrap_or("POISON").to_string();

        // Un mensaje ilegible NO se copia a la DLQ en ninguna forma —ni su cuerpo ni los
        // `rawBase64` de los eventos—: podría contener el dato. Tampoco el texto del error de
        // parseo, que puede citar fragmentos de lo recibido. El `dlqerror` dice el código.
        let (event, error_text) = match original {
            Some(event) => (event, format!("{code}: {}", describe(&err))),
            None => (
                self.synth_state_event(w, tenant, key),
                format!(
                    "{code}: mensaje ilegible; su cuerpo no se copia a la DLQ de estado \
                     (10-state.md §6)"
                ),
            ),
        };
        let dlq_event = to_state_dlq_event(
            event,
            key,
            revision,
            &DlqInfo {
                reason: DlqReason::Poison,
                attempts: attempt,
                consumer: w.durable.clone(),
                error: error_text,
            },
            Utc::now(),
        );

        let outcome = if matches!(
            code.as_str(),
            "MISSING_SIGNATURE" | "INVALID_SIGNATURE" | "UNKNOWN_SIGNING_KEY"
        ) {
            ConsumeOutcome::InvalidSignature
        } else {
            ConsumeOutcome::Poison
        };
        self.inner
            .metrics
            .event_consumed(&w.metric_subject, &w.consumer_label, outcome);
        self.inner.metrics.event_dlq(
            &w.metric_subject,
            &w.consumer_label,
            DlqReasonLabel::Poison,
            &code,
        );
        self.log(
            LogLevel::Error,
            &format!(
                "POISON {code} en {} (clave `{key}`, tenant `{tenant}`): la entrada NO se aplica",
                w.metric_subject
            ),
        );
        if let Some(cb) = &self.inner.opts.on_poison {
            cb(PoisonInfo {
                subject: subject.to_string(),
                error: err,
                // El cuerpo de una entrada de estado es dato personal: ni siquiera el callback
                // de alerta lo recibe.
                raw: Vec::new(),
            });
        }

        if self.publish_state_dlq(w, &dlq_event).await.is_err() {
            // NO se hace term(): terminar aquí borraría la entrada sin haberla guardado.
            return;
        }
        let _ = msg.ack_with(AckKind::Term).await;
    }

    /// El sobre de un mensaje ilegible: no hay original que conservar, así que se sintetiza con
    /// lo único fiable, **el subject** — 10-state.md §6.
    fn synth_state_event(&self, w: &WatchCtx, tenant: &str, key: &str) -> Event {
        let id = Uuid::now_v7().to_string();
        let data = serde_json::value::to_raw_value(&json!({}))
            .unwrap_or_else(|_| RawValue::NULL.to_owned());
        assemble(
            &w.contract,
            false,
            tenant.to_string(),
            key.to_string(),
            id.clone(),
            self.inner.source.clone(),
            Utc::now(),
            id,
            self.inner.opts.version.clone(),
            DataClassification::Internal,
            w.synth_dataschema.clone(),
            None,
            None,
            None,
            data,
        )
    }

    /// Publica un evento de DLQ de estado en `dlq.state.<contrato>`, con `Nats-Msg-Id`
    /// `dlq-<id>-<dlqconsumer>`. Si falla, lo registra y devuelve el error: quien llama **no**
    /// termina el mensaje.
    async fn publish_state_dlq(&self, w: &WatchCtx, dlq_event: &Event) -> Result<(), FluxError> {
        let published = async {
            let payload = serialize(dlq_event)?;
            let msg_id = format!("dlq-{}-{}", dlq_event.id, w.durable);
            self.publish_raw(&w.contract.dlq_subject(), payload, &msg_id)
                .await
        }
        .await;
        if let Err(e) = &published {
            // Es preferible reprocesar una entrada que perderla sin rastro.
            self.log(
                LogLevel::Error,
                &format!(
                    "no se pudo enrutar a la DLQ de estado {}: {e}. El mensaje NO se termina; se \
                     reentregará.",
                    w.contract.dlq_subject()
                ),
            );
        }
        published
    }

    /// El final del despacho de una entrada que el handler (o una comprobación) rechazó para
    /// siempre: guardar en la DLQ de estado —**sin el dato**—, contar y terminar.
    #[allow(clippy::too_many_arguments)]
    async fn state_route_to_dlq(
        &self,
        w: &WatchCtx,
        msg: &async_nats::jetstream::Message,
        event: Event,
        key: &str,
        revision: u64,
        handler_err: &HandlerError,
        c: &Classification,
        attempt: u32,
        inicio: Instant,
        signature_warning: bool,
    ) {
        let reason = DlqReason::from(c.class);
        let dlq_event = to_state_dlq_event(
            event,
            key,
            revision,
            &DlqInfo {
                reason,
                attempts: attempt,
                consumer: w.durable.clone(),
                error: format!("{}: {}", c.code, describe(handler_err.as_ref())),
            },
            Utc::now(),
        );
        if self.publish_state_dlq(w, &dlq_event).await.is_err() {
            return;
        }

        self.inner.metrics.event_consumed(
            &w.metric_subject,
            &w.consumer_label,
            if signature_warning {
                ConsumeOutcome::InvalidSignature
            } else if is_schema_code(&c.code) {
                ConsumeOutcome::InvalidSchema
            } else {
                ConsumeOutcome::from(reason)
            },
        );
        self.inner.metrics.event_dlq(
            &w.metric_subject,
            &w.consumer_label,
            DlqReasonLabel::from(reason),
            &c.code,
        );
        self.inner.metrics.handler_duration(
            &w.metric_subject,
            &w.consumer_label,
            inicio.elapsed().as_secs_f64(),
        );
        if let Some(cb) = &self.inner.opts.on_dlq {
            cb(DlqEventInfo {
                subject: msg.subject.to_string(),
                event: dlq_event,
                classification: c.clone(),
            });
        }
        self.log(
            LogLevel::Error,
            &format!(
                "DLQ de estado ({reason:?}) {} en {} (clave `{key}`) tras {attempt} intento(s)",
                c.code, w.metric_subject
            ),
        );
        let _ = msg.ack_with(AckKind::Term).await;
    }
}

// ═════════════════════════════════════════════════════════════════════════════
//  Tests unitarios
// ═════════════════════════════════════════════════════════════════════════════
//
// Sin broker. Lo que necesita uno real —el ciclo de 100 claves, los permisos exactos, los
// escenarios de `conformance/cases/state-semantics.json`— está en `tests/state.rs`.
//
// Cada comprobación va **en los dos sentidos**: lo correcto pasa y lo incorrecto falla. Los
// vectores salen de los ficheros de la spec y no de copias: `protocol.json`, `10-state.md` y
// `conformance/cases/cross-sdk-state-envelope.json`.

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::errors::{
        INVALID_STATE_KEY_CODE, INVALID_TENANT_FOR_STATE_CODE, MISSING_AGGREGATE_VERSION_CODE,
        RESERVED_DOMAIN_CODE, STATE_CONFLICT_CODE, STATE_SUBJECT_MISMATCH_CODE,
    };

    const FIXTURES: &str = include_str!("../../conformance/cases/cross-sdk-state-envelope.json");
    const SPEC: &str = include_str!("../../specification/10-state.md");
    const PROTOCOL: &str = include_str!("../../protocol.json");
    const BASE: &str = "https://schemas.internal";

    fn json_of(text: &str) -> Value {
        serde_json::from_str(text).expect("JSON válido")
    }

    fn strings(v: &Value) -> Vec<String> {
        v.as_array()
            .expect("array")
            .iter()
            .map(|x| x.as_str().expect("cadena").to_string())
            .collect()
    }

    fn code_of<T: fmt::Debug>(r: &Result<T, FluxError>) -> Option<String> {
        r.as_ref()
            .err()
            .and_then(FluxError::code)
            .map(ToString::to_string)
    }

    // ── SHA-256, para fijar los hashes del fixture sin añadir una dependencia ─

    #[allow(clippy::many_single_char_names, clippy::too_many_lines)]
    fn sha256_hex(data: &[u8]) -> String {
        const K: [u32; 64] = [
            0x428a_2f98,
            0x7137_4491,
            0xb5c0_fbcf,
            0xe9b5_dba5,
            0x3956_c25b,
            0x59f1_11f1,
            0x923f_82a4,
            0xab1c_5ed5,
            0xd807_aa98,
            0x1283_5b01,
            0x2431_85be,
            0x550c_7dc3,
            0x72be_5d74,
            0x80de_b1fe,
            0x9bdc_06a7,
            0xc19b_f174,
            0xe49b_69c1,
            0xefbe_4786,
            0x0fc1_9dc6,
            0x240c_a1cc,
            0x2de9_2c6f,
            0x4a74_84aa,
            0x5cb0_a9dc,
            0x76f9_88da,
            0x983e_5152,
            0xa831_c66d,
            0xb003_27c8,
            0xbf59_7fc7,
            0xc6e0_0bf3,
            0xd5a7_9147,
            0x06ca_6351,
            0x1429_2967,
            0x27b7_0a85,
            0x2e1b_2138,
            0x4d2c_6dfc,
            0x5338_0d13,
            0x650a_7354,
            0x766a_0abb,
            0x81c2_c92e,
            0x9272_2c85,
            0xa2bf_e8a1,
            0xa81a_664b,
            0xc24b_8b70,
            0xc76c_51a3,
            0xd192_e819,
            0xd699_0624,
            0xf40e_3585,
            0x106a_a070,
            0x19a4_c116,
            0x1e37_6c08,
            0x2748_774c,
            0x34b0_bcb5,
            0x391c_0cb3,
            0x4ed8_aa4a,
            0x5b9c_ca4f,
            0x682e_6ff3,
            0x748f_82ee,
            0x78a5_636f,
            0x84c8_7814,
            0x8cc7_0208,
            0x90be_fffa,
            0xa450_6ceb,
            0xbef9_a3f7,
            0xc671_78f2,
        ];
        let mut h: [u32; 8] = [
            0x6a09_e667,
            0xbb67_ae85,
            0x3c6e_f372,
            0xa54f_f53a,
            0x510e_527f,
            0x9b05_688c,
            0x1f83_d9ab,
            0x5be0_cd19,
        ];
        let mut msg = data.to_vec();
        let bits = (data.len() as u64) * 8;
        msg.push(0x80);
        while msg.len() % 64 != 56 {
            msg.push(0);
        }
        msg.extend_from_slice(&bits.to_be_bytes());
        for chunk in msg.chunks(64) {
            let mut w = [0u32; 64];
            for i in 0..16 {
                w[i] = u32::from_be_bytes([
                    chunk[4 * i],
                    chunk[4 * i + 1],
                    chunk[4 * i + 2],
                    chunk[4 * i + 3],
                ]);
            }
            for i in 16..64 {
                let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
                let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
                w[i] = w[i - 16]
                    .wrapping_add(s0)
                    .wrapping_add(w[i - 7])
                    .wrapping_add(s1);
            }
            let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
            for i in 0..64 {
                let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
                let ch = (e & f) ^ (!e & g);
                let t1 = hh
                    .wrapping_add(s1)
                    .wrapping_add(ch)
                    .wrapping_add(K[i])
                    .wrapping_add(w[i]);
                let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
                let maj = (a & b) ^ (a & c) ^ (b & c);
                let t2 = s0.wrapping_add(maj);
                hh = g;
                g = f;
                f = e;
                e = d.wrapping_add(t1);
                d = c;
                c = b;
                b = a;
                a = t1.wrapping_add(t2);
            }
            for (slot, v) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
                *slot = slot.wrapping_add(v);
            }
        }
        h.iter().fold(String::new(), |mut out, x| {
            let _ = write!(out, "{x:08x}");
            out
        })
    }

    /// El SHA-256 de prueba es el de verdad: si no, los hashes del fixture no probarían nada.
    #[test]
    fn el_sha256_de_los_tests_es_correcto() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    // ── Los ejemplos byte a byte de 10-state.md §2.6 ─────────────────────────

    fn instant(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s)
            .expect("RFC 3339")
            .with_timezone(&Utc)
    }

    fn text(v: &Value, k: &str) -> String {
        v[k].as_str().unwrap_or_else(|| panic!("{k}")).to_string()
    }

    fn raw_of(v: &Value) -> Box<RawValue> {
        RawValue::from_string(serde_json::to_string(v).expect("JSON")).expect("RawValue")
    }

    /// La entrada del arnés `state-build`: los campos del fixture más el `dataschema`, que sale de
    /// la URL base como lo derivaría el SDK.
    fn put_input(i: &Value) -> StateEventInput {
        StateEventInput {
            contract: text(i, "contract"),
            tenant_id: text(i, "tenantId"),
            key: text(i, "key"),
            id: text(i, "id"),
            source: text(i, "source"),
            time: instant(&text(i, "time")),
            correlationid: text(i, "correlationid"),
            producerversion: text(i, "producerversion"),
            classification: DataClassification::from_str_exact(&text(i, "classification"))
                .expect("clasificación"),
            dataschema: state_dataschema(BASE, &text(i, "contract")).expect("dataschema"),
            causationid: None,
            traceparent: None,
            tracestate: None,
            data: raw_of(&i["data"]),
        }
    }

    fn fixture(id: &str) -> Value {
        json_of(FIXTURES)["fixtures"]
            .as_array()
            .expect("fixtures")
            .iter()
            .find(|f| f["id"] == id)
            .unwrap_or_else(|| panic!("fixture {id}"))
            .clone()
    }

    fn put_bytes(f: &Value) -> Vec<u8> {
        serialize(&build_state_event(put_input(&f["input"])).expect("put")).expect("bytes")
    }

    fn tombstone_bytes(f: &Value) -> Vec<u8> {
        let i = &f["input"];
        serialize(
            &build_state_tombstone(StateTombstoneInput {
                contract: text(i, "contract"),
                tenant_id: text(i, "tenantId"),
                key: text(i, "key"),
                id: text(i, "id"),
                source: text(i, "source"),
                time: instant(&text(i, "time")),
                correlationid: text(i, "correlationid"),
                producerversion: text(i, "producerversion"),
                aggregate_version: i["aggregateVersion"].as_u64().expect("versión"),
                causationid: None,
                traceparent: None,
                tracestate: None,
            })
            .expect("lápida"),
        )
        .expect("bytes")
    }

    fn dlq_bytes(f: &Value) -> Vec<u8> {
        let i = &f["input"];
        let original = build_state_event(put_input(&i["original"])).expect("original");
        let d = &i["dlq"];
        serialize(&to_state_dlq_event(
            original,
            &text(&i["original"], "key"),
            i["revision"].as_u64().expect("revision"),
            &DlqInfo {
                reason: match text(d, "reason").as_str() {
                    "permanent" => DlqReason::Permanent,
                    "retryable" => DlqReason::Retryable,
                    _ => DlqReason::Poison,
                },
                attempts: u32::try_from(d["attempts"].as_u64().expect("attempts")).unwrap(),
                consumer: text(d, "consumer"),
                error: text(d, "error"),
            },
            instant(&text(d, "dlqtime")),
        ))
        .expect("bytes")
    }

    fn assert_fixture(id: &str, bytes: &[u8]) {
        let f = fixture(id);
        assert_eq!(
            bytes.len(),
            usize::try_from(f["bytes"].as_u64().unwrap()).unwrap(),
            "{id}: longitud"
        );
        assert_eq!(sha256_hex(bytes), text(&f, "sha256"), "{id}: sha256");
        assert_eq!(
            crate::client::base64(bytes),
            text(&f, "base64"),
            "{id}: base64"
        );
        // Y son los del evento del fixture serializado por `serde_json` con el orden del fichero.
        assert_eq!(
            std::str::from_utf8(bytes).expect("UTF-8"),
            serde_json::to_string(&f["event"]).expect("JSON"),
            "{id}: el JSON del fixture"
        );
    }

    #[test]
    fn el_put_son_los_bytes_de_la_spec() {
        assert_fixture("state-put", &put_bytes(&fixture("state-put")));
    }

    #[test]
    fn la_lapida_son_los_bytes_de_la_spec() {
        assert_fixture(
            "state-tombstone",
            &tombstone_bytes(&fixture("state-tombstone")),
        );
    }

    #[test]
    fn el_evento_de_dlq_de_estado_son_los_bytes_de_la_spec() {
        assert_fixture("state-dlq", &dlq_bytes(&fixture("state-dlq")));
    }

    /// Los tres ejemplos de `10-state.md` §2.6 —el texto, no el fixture— y sus SHA-256: la spec
    /// y el fixture dicen lo mismo, y este SDK produce lo que dicen.
    #[test]
    fn los_bytes_son_los_del_texto_de_la_spec() {
        let lineas: Vec<&str> = SPEC
            .lines()
            .filter(|l| l.starts_with("{\"specversion\":\"1.0\""))
            .collect();
        assert_eq!(lineas.len(), 3, "§2.6 tiene tres ejemplos");
        let hashes: Vec<&str> = SPEC
            .lines()
            .filter_map(|l| l.strip_prefix("SHA-256: `"))
            .filter_map(|l| l.strip_suffix("`."))
            .collect();
        assert_eq!(hashes.len(), 3);

        let ours = [
            put_bytes(&fixture("state-put")),
            tombstone_bytes(&fixture("state-tombstone")),
            dlq_bytes(&fixture("state-dlq")),
        ];
        for (i, bytes) in ours.iter().enumerate() {
            assert_eq!(
                std::str::from_utf8(bytes).expect("UTF-8"),
                lineas[i],
                "ejemplo {i}"
            );
            assert_eq!(sha256_hex(bytes), hashes[i], "SHA-256 del ejemplo {i}");
        }
    }

    /// **Control**: otro `tenantid` cambia el hash. Sin esto los hashes de arriba podrían ser
    /// constantes que ningún cambio rompe.
    #[test]
    fn cambiar_el_tenant_cambia_el_hash() {
        let f = fixture("state-put");
        let bytes = put_bytes(&f);

        let mut otro = put_input(&f["input"]);
        otro.tenant_id = "globex".to_string();
        let otro = serialize(&build_state_event(otro).unwrap()).unwrap();
        assert_ne!(sha256_hex(&otro), sha256_hex(&bytes));

        // Y la mutación del runner de Node: `ACME` en el sobre.
        let roto = String::from_utf8(bytes.clone())
            .unwrap()
            .replace("\"tenantid\":\"acme\"", "\"tenantid\":\"ACME\"");
        assert_ne!(sha256_hex(roto.as_bytes()), text(&f, "sha256"));
    }

    /// 01-envelope.md §1.1: UTF-8 **literal**, sin escapes. `María` y no `María`: son
    /// JSON válido los dos y no son los mismos bytes.
    #[test]
    fn el_utf8_es_literal_y_data_va_el_ultimo() {
        let bytes = String::from_utf8(put_bytes(&fixture("state-put"))).unwrap();
        assert!(bytes.contains("María Peña"), "{bytes}");
        assert!(!bytes.contains("\\u"), "{bytes}");
        let dlq = String::from_utf8(dlq_bytes(&fixture("state-dlq"))).unwrap();
        assert!(dlq.contains("país"), "{dlq}");
        assert!(!dlq.contains("\\u"), "{dlq}");
        for b in [&bytes, &dlq] {
            let value = json_of(b);
            assert_eq!(
                value.as_object().unwrap().keys().next_back().unwrap(),
                "data"
            );
        }
    }

    // ── El evento de DLQ de estado — 10-state.md §6 ──────────────────────────

    #[test]
    fn la_dlq_de_estado_no_lleva_ni_el_dato_ni_la_firma() {
        let f = fixture("state-put");
        let mut firmado = build_state_event(put_input(&f["input"])).unwrap();
        firmado.signkeyid = Some("crm-api-1".to_string());
        firmado.signature = Some("AAAA".to_string());
        let dlq = to_state_dlq_event(
            firmado,
            "cli-1",
            41,
            &DlqInfo {
                reason: DlqReason::Permanent,
                attempts: 1,
                consumer: "tienda-web__state_clientes_cliente_v1__acme".to_string(),
                error: "X: y".to_string(),
            },
            instant("2026-08-20T10:25:40.117Z"),
        );
        let bytes = String::from_utf8(serialize(&dlq).unwrap()).unwrap();
        assert!(
            bytes.ends_with("\"data\":{\"key\":\"cli-1\",\"revision\":41}}"),
            "{bytes}"
        );
        for filtrado in [
            "maria@example.com",
            "María",
            "signature",
            "signkeyid",
            "nombre",
        ] {
            assert!(
                !bytes.contains(filtrado),
                "la DLQ de estado filtra `{filtrado}`: {bytes}"
            );
        }
        // El `id` se conserva y el sobre sigue siendo el original.
        assert!(bytes.contains("\"id\":\"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55\""));
        assert!(bytes.contains("\"dataclassification\":\"confidential\""));
    }

    /// La clave que se registra es la **del subject**, no la del sobre: ante un
    /// `STATE_SUBJECT_MISMATCH` son distintas, y la del sobre es justo la que miente.
    #[test]
    fn la_dlq_registra_la_clave_que_se_le_da() {
        let f = fixture("state-put");
        let e = build_state_event(put_input(&f["input"])).unwrap();
        let dlq = to_state_dlq_event(
            e,
            "la-del-subject",
            7,
            &DlqInfo {
                reason: DlqReason::Poison,
                attempts: 1,
                consumer: "c".to_string(),
                error: "e".to_string(),
            },
            instant("2026-08-20T10:25:40.117Z"),
        );
        assert_eq!(
            dlq.data.get(),
            "{\"key\":\"la-del-subject\",\"revision\":7}"
        );
    }

    // ── La clave, el tenant, el contrato: los ejemplos de protocol.json ──────

    #[test]
    fn las_claves_de_protocol_json() {
        let key = &json_of(PROTOCOL)["state"]["key"];
        for k in strings(&key["valid"]) {
            assert!(validate_state_key(&k).is_ok(), "{k:?} debería ser válida");
        }
        for k in strings(&key["invalid"]) {
            let r = validate_state_key(&k);
            assert_eq!(code_of(&r).as_deref(), Some("INVALID_STATE_KEY"), "{k:?}");
        }
        assert_eq!(key["maxLength"], STATE_KEY_MAX_LEN);
    }

    /// Los vectores de la spec incluyen la clave de la trampa de `$`: `"cli-1\n"`. Aquí no se usa
    /// `$` —se recorre carácter a carácter—, y el test fija que el salto de línea final se rechaza.
    #[test]
    fn una_clave_con_salto_de_linea_final_no_pasa() {
        for k in [
            "cli-1\n",
            "cli-1\r\n",
            "\ncli-1",
            "cli 1",
            "cli\t1",
            "cli-1 ",
        ] {
            assert!(validate_state_key(k).is_err(), "{k:?}");
        }
        // Los límites: 128 sí, 129 no.
        assert!(validate_state_key(&"a".repeat(128)).is_ok());
        assert!(validate_state_key(&"a".repeat(129)).is_err());
        // Se distinguen mayúsculas: `Cli-1` y `cli-1` son claves distintas, las dos válidas.
        assert!(validate_state_key("Cli-1").is_ok());
        // `*`, `>`, `a.b` y `café` los acepta el broker y NO el SDK (A-41).
        for k in ["*", ">", "a.b", "café", "-uno", "_reservada", ""] {
            assert!(validate_state_key(k).is_err(), "{k:?}");
        }
    }

    #[test]
    fn los_tenants_de_protocol_json() {
        let tenant = &json_of(PROTOCOL)["state"]["tenant"];
        for t in strings(&tenant["valid"]) {
            assert!(
                validate_state_tenant(&t).is_ok(),
                "{t:?} debería ser válido"
            );
        }
        for t in strings(&tenant["invalid"]) {
            let r = validate_state_tenant(&t);
            assert_eq!(
                code_of(&r).as_deref(),
                Some("INVALID_TENANT_FOR_STATE"),
                "{t:?}"
            );
        }
        assert_eq!(tenant["maxLength"], STATE_TENANT_MAX_LEN);
        assert!(validate_state_tenant(&"t".repeat(64)).is_ok());
        assert!(validate_state_tenant(&"t".repeat(65)).is_err());
        // `system` es un tenant válido como cualquier otro.
        assert!(validate_state_tenant("system").is_ok());
    }

    #[test]
    fn los_contratos_de_protocol_json() {
        let contract = &json_of(PROTOCOL)["state"]["contract"];
        for c in strings(&contract["valid"]) {
            assert_eq!(StateContract::parse(&c).expect(&c).name(), c);
        }
        for c in strings(&contract["invalid"]) {
            assert!(
                StateContract::parse(&c).is_err(),
                "{c:?} debería ser inválido"
            );
        }
    }

    /// Los ejemplos de subject de `protocol.json` se **descomponen** y se **recomponen**: seis
    /// tokens, con el tenant y la clave dentro.
    #[test]
    fn los_subjects_de_protocol_json() {
        fn descompone(s: &str) -> Result<String, FluxError> {
            let t: Vec<&str> = s.split('.').collect();
            if t.len() != 6 || t[0] != "state" {
                return Err(FluxError::InvalidSubject {
                    subject: s.to_string(),
                    reason: "no son 6 tokens de `state.`".to_string(),
                });
            }
            state_subject(&format!("{}.{}.{}", t[1], t[2], t[3]), t[4], t[5])
        }
        let subject = &json_of(PROTOCOL)["state"]["subject"];
        for s in strings(&subject["valid"]) {
            assert_eq!(descompone(&s).expect(&s), s);
        }
        for s in strings(&subject["invalid"]) {
            assert!(descompone(&s).is_err(), "{s:?} debería ser inválido");
        }
        assert_eq!(subject["tokens"], 6);
    }

    /// Los vectores del fixture: el subject, o el **código estable** de cada rechazo, y en el
    /// orden de §3.6: dominio reservado, clave, tenant.
    #[test]
    fn los_vectores_del_subject() {
        let vectores = json_of(FIXTURES);
        let vectores = vectores["subjectVectors"]
            .as_array()
            .expect("subjectVectors");
        assert!(vectores.len() >= 20);
        for v in vectores {
            let r = state_subject(&text(v, "contract"), &text(v, "tenantId"), &text(v, "key"));
            match v.get("natsSubject") {
                Some(esperado) => assert_eq!(r.as_deref().ok(), esperado.as_str(), "{v}"),
                None => assert_eq!(code_of(&r).as_deref(), v["errorCode"].as_str(), "{v}"),
            }
        }
    }

    /// El orden de §3.6: el primero que falla gana. Con tres fallos a la vez, gana el dominio.
    #[test]
    fn el_primer_fallo_gana_en_el_orden_de_la_spec() {
        let r = state_subject("state.cliente.v1", "ACME", "a.b");
        assert_eq!(code_of(&r).as_deref(), Some("RESERVED_DOMAIN"));
        let r = state_subject("clientes.cliente.v1", "ACME", "a.b");
        assert_eq!(code_of(&r).as_deref(), Some("INVALID_STATE_KEY"));
        let r = state_subject("clientes.cliente.v1", "ACME", "a");
        assert_eq!(code_of(&r).as_deref(), Some("INVALID_TENANT_FOR_STATE"));
    }

    #[test]
    fn un_contrato_mal_formado_no_es_uno_de_los_seis_codigos() {
        // La lista de seis es CERRADA (04-errors.md §6): un contrato que no son 3 tokens es un
        // `InvalidSubject`, sin código estable, y no un código nuevo inventado por este SDK.
        for c in [
            "clientes.cliente",
            "clientes.cliente.v1.creado",
            "clientes.cliente.1",
            "",
        ] {
            let r = StateContract::parse(c);
            assert!(
                matches!(r, Err(FluxError::InvalidSubject { .. })),
                "{c:?}: {r:?}"
            );
            assert_eq!(code_of(&r), None);
        }
    }

    // ── El durable, el filtro, los nombres ───────────────────────────────────

    #[test]
    fn los_durables_de_protocol_json() {
        let durable = &json_of(PROTOCOL)["state"]["watch"]["durable"];
        let nuestros = [
            state_durable_name("tienda-web", "clientes.cliente.v1", None),
            state_durable_name("tienda-web", "clientes.cliente.v1", Some("acme")),
            state_durable_name("tienda-web", "logistica.linea-envio.v2", Some("acme-corp")),
        ];
        let valid = strings(&durable["valid"]);
        for (esperado, nuestro) in valid.iter().zip(nuestros) {
            assert_eq!(&nuestro.expect(esperado), esperado);
        }
        assert_eq!(
            state_durable_name("tienda-web", "clientes.cliente.v1", Some("acme")).unwrap(),
            text(durable, "example")
        );
        // Los inválidos no los produce el SDK nunca: sin puntos, con el tenant en `_`.
        for malo in strings(&durable["invalid"]) {
            for bueno in &valid {
                assert_ne!(&malo, bueno);
            }
        }
        // Un tenant con guiones pasa a `_`; el nombre no lleva puntos ni guiones en el contrato.
        let d = state_durable_name("tienda-web", "logistica.linea-envio.v2", Some("acme-corp"))
            .unwrap();
        assert!(!d.contains('.'), "{d}");
        assert!(d.ends_with("__acme_corp"), "{d}");
        // Dos tenants, dos durables: un durable compartido REPARTE los mensajes (A-47).
        assert_ne!(
            state_durable_name("t", "clientes.cliente.v1", Some("acme")).unwrap(),
            state_durable_name("t", "clientes.cliente.v1", Some("globex")).unwrap()
        );
        assert!(state_durable_name("T", "clientes.cliente.v1", None).is_err());
        assert!(state_durable_name("t", "state.cliente.v1", None).is_err());
    }

    #[test]
    fn el_filtro_y_el_stream_de_un_watch() {
        let c = StateContract::parse("clientes.cliente.v1").unwrap();
        assert_eq!(
            c.watch_filter(Some("acme")),
            "state.clientes.cliente.v1.acme.*"
        );
        assert_eq!(c.watch_filter(None), "state.clientes.cliente.v1.*.*");
        assert_eq!(c.stream_name(), "STATE_CLIENTES");
        assert_eq!(c.event_type(), "com.flux.state.clientes.cliente.v1");
        assert_eq!(
            c.tombstone_type(),
            "com.flux.state.clientes.cliente.deleted.v1"
        );
        assert_eq!(c.dlq_subject(), "dlq.state.clientes.cliente.v1");
        let pattern = &json_of(PROTOCOL)["state"]["dlq"]["subject"];
        assert_eq!(c.dlq_subject(), text(pattern, "example"));
        let lapida = &json_of(PROTOCOL)["state"]["type"]["tombstone"];
        assert_eq!(c.tombstone_type(), text(lapida, "example"));
    }

    // ── aggregateVersion ─────────────────────────────────────────────────────

    #[test]
    fn la_version_es_un_entero_entre_1_y_2_a_la_53_menos_1() {
        let v = |x: &str| aggregate_version_of(&json_of(&format!("{{\"aggregateVersion\":{x}}}")));
        assert_eq!(v("1").unwrap(), 1);
        assert_eq!(v("7").unwrap(), 7);
        assert_eq!(v("9007199254740991").unwrap(), MAX_AGGREGATE_VERSION);
        for malo in [
            "0",
            "-1",
            "7.5",
            "7.0",
            "1e2",
            "\"7\"",
            "null",
            "true",
            "[1]",
            "{}",
            "9007199254740992",
            "18446744073709551616",
        ] {
            let r = v(malo);
            assert_eq!(
                code_of(&r).as_deref(),
                Some("MISSING_AGGREGATE_VERSION"),
                "{malo}: {r:?}"
            );
        }
        // Ausente, y `data` que no es un objeto.
        for malo in ["{}", "{\"otro\":1}", "[1]", "7", "\"x\"", "null"] {
            let r = aggregate_version_of(&json_of(malo));
            assert_eq!(
                code_of(&r).as_deref(),
                Some("MISSING_AGGREGATE_VERSION"),
                "{malo}"
            );
        }
    }

    /// El mensaje dice **qué** está mal, nunca **cuál es el valor**: `data` es dato personal.
    #[test]
    fn el_mensaje_de_version_no_cita_valores() {
        let e = aggregate_version_of(&json!({ "aggregateVersion": "SECRETO-7", "email": "a@b.c" }))
            .unwrap_err()
            .to_string();
        assert!(e.contains("aggregateVersion"), "{e}");
        assert!(e.contains("cadena"), "{e}");
        assert!(!e.contains("SECRETO-7") && !e.contains("a@b.c"), "{e}");
    }

    // ── El tenant: su resolución ─────────────────────────────────────────────

    #[test]
    fn el_tenant_se_resuelve_por_opcion_evento_y_conexion_en_ese_orden() {
        assert_eq!(
            resolve_tenant(Some("a"), Some("b"), Some("c")).unwrap(),
            "a"
        );
        assert_eq!(resolve_tenant(None, Some("b"), Some("c")).unwrap(), "b");
        assert_eq!(resolve_tenant(None, None, Some("c")).unwrap(), "c");
        // Un evento en curso sin tenant (`EventContext::default()`) no cuenta.
        assert_eq!(resolve_tenant(None, Some(""), Some("c")).unwrap(), "c");
        // Quien quiere `system` lo dice: no hay un `system` implícito.
        assert_eq!(
            resolve_tenant(Some("system"), None, None).unwrap(),
            "system"
        );
        let sin = resolve_tenant(None, None, None);
        assert_eq!(code_of(&sin).as_deref(), Some("INVALID_TENANT_FOR_STATE"));
        let msg = sin.unwrap_err().to_string();
        assert!(
            msg.contains("no hay tenant por defecto") || msg.contains("NO hay tenant"),
            "{msg}"
        );
        assert!(msg.contains("ConnectOptions::with_tenant_id"), "{msg}");
        // La opción gana aunque esté mal: un descuido debe verse.
        assert!(resolve_tenant(Some(""), None, Some("c")).is_err());
        assert!(resolve_tenant(Some("ACME"), None, Some("c")).is_err());
        // El tenant de un EVENTO es cualquier cadena; el de un estado, no.
        assert!(resolve_tenant(None, Some("ACME_Corp"), Some("c")).is_err());
    }

    #[test]
    fn el_tenant_de_un_watch_es_opcional_y_si_viene_se_valida() {
        assert_eq!(resolve_watch_tenant(None, None).unwrap(), None);
        assert_eq!(
            resolve_watch_tenant(Some("a"), Some("b"))
                .unwrap()
                .as_deref(),
            Some("a")
        );
        // Con tenant en `connect()` SIEMPRE filtra, también `system` (decisión provisional 9).
        assert_eq!(
            resolve_watch_tenant(None, Some("system"))
                .unwrap()
                .as_deref(),
            Some("system")
        );
        assert!(resolve_watch_tenant(Some("Acme"), None).is_err());
    }

    // ── Dominios reservados ──────────────────────────────────────────────────

    #[test]
    fn los_dominios_reservados_se_rechazan_antes_que_el_formato() {
        for s in [
            "state.cliente.v1.creado",
            "dlq.pedidos.pedido.v1.creado", // 5 tokens: «debe tener 4» no ayuda
            "rpc.cliente.v1.creado",
            "system.poison.v1.capturado",
            "system",
            "dlq.x",
        ] {
            let r = reject_reserved_domain(s);
            assert_eq!(code_of(&r).as_deref(), Some(RESERVED_DOMAIN_CODE), "{s}");
            let msg = r.unwrap_err().to_string();
            assert!(msg.contains("dlq, state, rpc, system"), "{msg}");
        }
        for s in [
            "pedidos.pedido.v1.creado",
            "state-clientes.cliente.v1.creado",
            "Dlq.x.v1.y",
            "statex.cliente.v1.creado",
            "",
        ] {
            assert!(reject_reserved_domain(s).is_ok(), "{s:?}");
        }
    }

    // ── Los seis códigos de error — 04-errors.md §6 ──────────────────────────

    #[test]
    fn los_seis_codigos_de_estado_con_su_clase() {
        let casos: [(FluxError, &str, ErrorClass); 6] = [
            (
                FluxError::InvalidTenantForState {
                    tenant: None,
                    reason: "x".into(),
                },
                "INVALID_TENANT_FOR_STATE",
                ErrorClass::Permanent,
            ),
            (
                FluxError::InvalidStateKey {
                    key: "a.b".into(),
                    reason: "x".into(),
                },
                "INVALID_STATE_KEY",
                ErrorClass::Permanent,
            ),
            (
                FluxError::MissingAggregateVersion { reason: "x".into() },
                "MISSING_AGGREGATE_VERSION",
                ErrorClass::Permanent,
            ),
            (
                FluxError::StateSubjectMismatch {
                    attribute: "tenantid".into(),
                    envelope: "a".into(),
                    subject: "b".into(),
                },
                "STATE_SUBJECT_MISMATCH",
                ErrorClass::Poison,
            ),
            (
                FluxError::StateConflict {
                    key: "k".into(),
                    attempts: 3,
                },
                "STATE_CONFLICT",
                ErrorClass::Retryable,
            ),
            (
                FluxError::ReservedDomain {
                    domain: "state".into(),
                    subject: "state.x.v1.y".into(),
                },
                "RESERVED_DOMAIN",
                ErrorClass::Permanent,
            ),
        ];
        for (err, code, class) in &casos {
            assert_eq!(err.code(), Some(*code), "{err:?}");
            assert_eq!(err.class(), Some(*class), "{err:?}");
        }
        // Las constantes públicas son los literales de la spec, y los de `protocol.json`.
        assert_eq!(INVALID_TENANT_FOR_STATE_CODE, "INVALID_TENANT_FOR_STATE");
        assert_eq!(INVALID_STATE_KEY_CODE, "INVALID_STATE_KEY");
        assert_eq!(MISSING_AGGREGATE_VERSION_CODE, "MISSING_AGGREGATE_VERSION");
        assert_eq!(STATE_SUBJECT_MISMATCH_CODE, "STATE_SUBJECT_MISMATCH");
        assert_eq!(STATE_CONFLICT_CODE, "STATE_CONFLICT");
        assert_eq!(RESERVED_DOMAIN_CODE, "RESERVED_DOMAIN");

        let protocol = json_of(PROTOCOL);
        let codes = &protocol["errors"]["codes"];
        for (_, code, class) in &casos {
            assert_eq!(codes[*code]["class"], class.as_str(), "{code}");
            assert_eq!(codes[*code]["capability"], "state", "{code}");
        }
        // Una aplicación NO puede sustituir el código.
        assert_eq!(
            FluxError::StateConflict {
                key: "k".into(),
                attempts: 3
            }
            .with_code("OTRO")
            .code(),
            Some("STATE_CONFLICT")
        );
        // Y `STATE_CONFLICT` lleva `retry_after: None`: se reintenta con el backoff de siempre.
        assert_eq!(
            FluxError::StateConflict {
                key: "k".into(),
                attempts: 3
            }
            .retry_after(),
            None
        );
    }

    /// Un handler cuyo `put` falla con uno de los cuatro de la llamada va a la DLQ en la
    /// **primera** entrega (PERMANENT); un `STATE_CONFLICT` se reintenta (RETRYABLE reconocido).
    #[test]
    fn el_clasificador_trata_los_codigos_de_estado_segun_su_clase() {
        let clasificador = crate::classify::Classifier::new(crate::ClassifierOptions::default());
        let c = clasificador.classify(&FluxError::InvalidStateKey {
            key: "a.b".into(),
            reason: "x".into(),
        });
        assert_eq!(
            (c.class, c.code.as_str()),
            (ErrorClass::Permanent, "INVALID_STATE_KEY")
        );
        let c = clasificador.classify(&FluxError::StateConflict {
            key: "k".into(),
            attempts: 3,
        });
        assert_eq!(
            (c.class, c.code.as_str()),
            (ErrorClass::Retryable, "STATE_CONFLICT")
        );
        // Reconocido: conserva sus 6 entregas, no el presupuesto acotado de lo desconocido.
        assert_eq!(c.max_attempts, None);
        let c = clasificador.classify(&FluxError::StateSubjectMismatch {
            attribute: "type".into(),
            envelope: "a".into(),
            subject: "b".into(),
        });
        assert_eq!(c.class, ErrorClass::Poison);
    }

    /// El mensaje de cada código dice qué está mal y cómo se arregla, y nombra lo que la spec
    /// pide que nombre (04-errors.md §6): la clave con el patrón, el tenant y de dónde sale.
    #[test]
    fn los_mensajes_nombran_lo_que_la_spec_pide() {
        let m = validate_state_key("a.b").unwrap_err().to_string();
        assert!(
            m.contains("\"a.b\"") && m.contains("[A-Za-z0-9][A-Za-z0-9_-]{0,127}"),
            "{m}"
        );
        assert!(m.contains("DOS tokens"), "{m}");
        let m = validate_state_tenant("ACME").unwrap_err().to_string();
        assert!(m.contains("\"ACME\"") && m.contains("tenant_id"), "{m}");
        let m = reject_reserved_domain("state.x.v1.y")
            .unwrap_err()
            .to_string();
        assert!(m.contains("`state`") && m.contains("rpc"), "{m}");
        let m = FluxError::StateConflict {
            key: "cli-1".into(),
            attempts: 3,
        }
        .to_string();
        assert!(
            m.contains("\"cli-1\"") && m.contains('3') && m.contains("RETRYABLE"),
            "{m}"
        );
        let m = FluxError::StateSubjectMismatch {
            attribute: "tenantid".into(),
            envelope: "globex".into(),
            subject: "acme".into(),
        }
        .to_string();
        assert!(
            m.contains("tenantid") && m.contains("globex") && m.contains("acme"),
            "{m}"
        );
    }

    /// El estado nunca crea ni consulta su stream (A-39): su `NOT_PROVISIONED` no puede decir «el
    /// broker denegó …», dice que nadie contesta, **nombra las dos causas** y apunta a
    /// `flux provision` y a `flux doctor` (04-errors.md §6).
    #[test]
    fn not_provisioned_de_estado_nombra_las_dos_causas() {
        let m = FluxError::NotProvisioned {
            kind: ResourceKind::Stream,
            name: "STATE_CLIENTES".into(),
            denied: String::new(),
        };
        assert_eq!(m.code(), Some("NOT_PROVISIONED"));
        assert_eq!(m.class(), Some(ErrorClass::Permanent));
        let t = m.to_string();
        assert!(t.contains("`STATE_CLIENTES`"), "{t}");
        assert!(t.contains("no existe, o existe sin `allow_direct`"), "{t}");
        assert!(
            t.contains("flux provision") && t.contains("flux doctor"),
            "{t}"
        );
        assert!(!t.contains("denegó"), "{t}");
    }

    // ── El algoritmo de put/delete contra un stream en memoria ───────────────

    #[derive(Default)]
    struct FakeInner {
        seq: u64,
        messages: HashMap<String, (u64, Vec<u8>)>,
        /// Cada publicación intentada: (id, S esperada, bytes) — también las rechazadas.
        publishes: Vec<(String, u64, Vec<u8>)>,
        reads: u32,
    }

    /// Un stream de estado en memoria con la semántica que se midió: `Nats-Expected-Last-Subject-
    /// Sequence` se evalúa **antes** que cualquier otra cosa, y `0` significa «el subject no tiene
    /// mensaje».
    #[derive(Default)]
    struct FakeStream {
        inner: Mutex<FakeInner>,
    }

    fn body(id: &str, version: u64) -> Vec<u8> {
        format!("{{\"id\":\"{id}\",\"data\":{{\"aggregateVersion\":{version}}}}}").into_bytes()
    }

    impl FakeStream {
        /// Escribe como lo haría otro escritor, sin chequeo. Devuelve su secuencia.
        fn rival(&self, subject: &str, id: &str, version: u64) -> u64 {
            let mut i = lock(&self.inner);
            i.seq += 1;
            let seq = i.seq;
            i.messages
                .insert(subject.to_string(), (seq, body(id, version)));
            seq
        }

        fn put_raw(&self, subject: &str, bytes: Vec<u8>) -> u64 {
            let mut i = lock(&self.inner);
            i.seq += 1;
            let seq = i.seq;
            i.messages.insert(subject.to_string(), (seq, bytes));
            seq
        }

        fn messages(&self) -> usize {
            lock(&self.inner).messages.len()
        }

        fn publishes(&self) -> Vec<(String, u64, Vec<u8>)> {
            lock(&self.inner).publishes.clone()
        }
    }

    impl StateIo for FakeStream {
        async fn read(&self, subject: &str) -> Result<Current, FluxError> {
            let mut i = lock(&self.inner);
            i.reads += 1;
            Ok(i.messages
                .get(subject)
                .map_or(Current::Absent, |(seq, bytes)| current_of(*seq, bytes)))
        }

        async fn publish(
            &self,
            subject: &str,
            id: &str,
            expected: u64,
            bytes: &[u8],
        ) -> Result<Published, FluxError> {
            let mut i = lock(&self.inner);
            i.publishes.push((id.to_string(), expected, bytes.to_vec()));
            let actual = i.messages.get(subject).map_or(0, |(seq, _)| *seq);
            if expected != actual {
                return Ok(Published::SequenceConflict);
            }
            i.seq += 1;
            let seq = i.seq;
            i.messages
                .insert(subject.to_string(), (seq, bytes.to_vec()));
            Ok(Published::Acked { seq })
        }
    }

    const SUBJECT: &str = "state.clientes.cliente.v1.acme.cli-1";

    /// Un hook que hace lo que los escenarios de `state-semantics.json`: un rival escribe entre
    /// la lectura y la publicación de un intento, o se pierde el acuse de un intento.
    struct Hook {
        rival: Box<dyn Fn(u32) + Send + Sync>,
        lose: Vec<u32>,
    }

    impl WriteHook for Hook {
        fn before_publish(&self, attempt: u32) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            (self.rival)(attempt);
            Box::pin(async {})
        }

        fn lose_ack(&self, attempt: u32) -> bool {
            self.lose.contains(&attempt)
        }
    }

    async fn write(
        io: &FakeStream,
        id: &str,
        version: u64,
        hook: Option<&dyn WriteHook>,
    ) -> Result<StateWrite, FluxError> {
        let bytes = body(id, version);
        run_write(
            io,
            &WriteJob {
                subject: SUBJECT,
                key: "cli-1",
                id,
                bytes: &bytes,
                version,
            },
            hook,
        )
        .await
    }

    #[tokio::test]
    async fn una_clave_nueva_se_crea_con_s_igual_a_cero() {
        let io = FakeStream::default();
        let r = write(&io, "id-1", 1, None).await.unwrap();
        assert_eq!(
            r,
            StateWrite {
                applied: true,
                outcome: WriteOutcome::Ok,
                revision: 1,
                attempts: 1,
                own_write: false
            }
        );
        // `Nats-Expected-Last-Subject-Sequence: 0`: «la clave no existe». Es lo que hace que dos
        // escritores concurrentes sobre una clave nueva no se pisen.
        assert_eq!(io.publishes()[0].1, 0);
    }

    #[tokio::test]
    async fn una_version_que_no_avanza_es_stale_y_no_publica() {
        let io = FakeStream::default();
        let seq = io.rival(SUBJECT, "otro", 5);
        for (version, esperado) in [(5, "igual"), (4, "menor"), (1, "mucho menor")] {
            let r = write(&io, "id-x", version, None).await.unwrap();
            assert!(
                !r.applied && r.outcome == WriteOutcome::Stale,
                "{esperado}: {r:?}"
            );
            // `revision` es la del mensaje ACTUAL, el que la hizo obsoleta.
            assert_eq!(r.revision, seq, "{esperado}");
        }
        assert!(io.publishes().is_empty(), "stale no escribe");
        // Una versión mayor sí, con la S de lo que hay.
        let r = write(&io, "id-y", 6, None).await.unwrap();
        assert!(r.applied);
        assert_eq!(io.publishes()[0].1, seq);
    }

    /// **El paso 1b.** El primer intento se aplica y su acuse se pierde: el reintento relee, ve su
    /// propio `id` y concluye `ok`. Sin el paso 1b vería su propia versión, no avanzaría, y daría
    /// `stale` con su propio dato — una escritura buena convertida en un resultado falso.
    #[tokio::test]
    async fn reconoce_su_propia_escritura_tras_un_acuse_perdido() {
        let io = FakeStream::default();
        let hook = Hook {
            rival: Box::new(|_| {}),
            lose: vec![1],
        };
        let r = write(&io, "id-propio", 1, Some(&hook)).await.unwrap();
        assert!(r.applied, "{r:?}");
        assert_eq!(r.outcome, WriteOutcome::Ok);
        assert!(r.own_write, "se reconoció por su id");
        assert_eq!(r.attempts, 2);
        assert_eq!(r.revision, 1, "la secuencia de su mensaje");
        assert_eq!(io.publishes().len(), 1, "no se publica de nuevo");
        assert_eq!(io.messages(), 1);
    }

    /// El `id` y los bytes son **los mismos en todos los intentos**: con un `id` nuevo por intento
    /// el paso 1b no reconocería nunca la propia escritura.
    #[tokio::test]
    async fn un_conflicto_se_reintenta_con_el_mismo_id_y_los_mismos_bytes() {
        // El hook necesita 'static: el stream en memoria se comparte con un Arc.
        let io = Arc::new(FakeStream::default());
        let io2 = Arc::clone(&io);
        let hook = Hook {
            rival: Box::new(move |attempt| {
                if attempt == 1 {
                    io2.rival(SUBJECT, "rival", 1);
                }
            }),
            lose: vec![],
        };
        let r = write(&io, "id-estable", 10, Some(&hook)).await.unwrap();
        assert!(r.applied);
        assert_eq!(r.attempts, 2);
        let p = io.publishes();
        assert_eq!(p.len(), 2, "el primero lo rechazó el chequeo de secuencia");
        assert_eq!(p[0].0, p[1].0, "mismo id");
        assert_eq!(p[0].2, p[1].2, "mismos bytes");
        assert_ne!(p[0].1, p[1].1, "con la S corregida");
        assert_eq!(p[1].1, 1, "la del rival");
    }

    #[tokio::test]
    async fn tres_conflictos_agotan_el_put_con_state_conflict() {
        let io = Arc::new(FakeStream::default());
        let io2 = Arc::clone(&io);
        let hook = Hook {
            rival: Box::new(move |attempt| {
                io2.rival(SUBJECT, &format!("rival-{attempt}"), u64::from(attempt));
            }),
            lose: vec![],
        };
        let r = write(&io, "id", 100, Some(&hook)).await;
        assert_eq!(code_of(&r).as_deref(), Some("STATE_CONFLICT"), "{r:?}");
        assert!(matches!(
            r,
            Err(FluxError::StateConflict { attempts: 3, ref key }) if key == "cli-1"
        ));
        assert_eq!(io.publishes().len(), 3, "exactamente tres intentos");
    }

    /// Un rival con una versión **mayor** no es un conflicto: hace obsoleta mi escritura. Es
    /// `stale`, en el siguiente intento, y no se reintenta más.
    #[tokio::test]
    async fn un_rival_con_version_mayor_hace_stale_la_escritura() {
        let io = Arc::new(FakeStream::default());
        let io2 = Arc::clone(&io);
        let hook = Hook {
            rival: Box::new(move |attempt| {
                if attempt == 1 {
                    io2.rival(SUBJECT, "rival", 11);
                }
            }),
            lose: vec![],
        };
        let r = write(&io, "id", 10, Some(&hook)).await.unwrap();
        assert!(!r.applied && r.outcome == WriteOutcome::Stale);
        assert_eq!(r.attempts, 2);
        assert_eq!(io.publishes().len(), 1);
    }

    /// §3.3: un valor ilegible no bloquea la clave. Si no, una entrada rota la dejaría
    /// inescribible para siempre, porque ningún servicio puede purgar el stream.
    #[tokio::test]
    async fn un_valor_ilegible_no_bloquea_la_clave() {
        let io = FakeStream::default();
        for roto in [
            b"esto no es json".to_vec(),
            b"{\"id\":\"x\",\"data\":{}}".to_vec(),
            b"{\"id\":\"x\",\"data\":{\"aggregateVersion\":\"7\"}}".to_vec(),
            b"[1,2]".to_vec(),
        ] {
            let seq = io.put_raw(SUBJECT, roto);
            let r = write(&io, "id-nuevo", 1, None).await.unwrap();
            assert!(r.applied, "{r:?}");
            // Se sobrescribe con el chequeo sobre SU secuencia.
            assert_eq!(io.publishes().last().unwrap().1, seq);
        }
    }

    /// Una lápida es un valor más, con su versión: una escritura con la versión de la lápida es
    /// `stale`, y recrear exige una **estrictamente mayor**.
    #[tokio::test]
    async fn una_lapida_es_un_valor_mas_con_su_version() {
        let io = FakeStream::default();
        io.put_raw(
            SUBJECT,
            b"{\"id\":\"lapida\",\"type\":\"com.flux.state.clientes.cliente.deleted.v1\",\"data\":{\"aggregateVersion\":3}}"
                .to_vec(),
        );
        for version in [3, 2, 1] {
            let r = write(&io, "id", version, None).await.unwrap();
            assert!(!r.applied, "v{version} no recrea: {r:?}");
        }
        assert!(
            write(&io, "id-4", 4, None).await.unwrap().applied,
            "v4 recrea"
        );
    }

    /// El paso 4 reconoce **los dos** códigos de conflicto. Se prueba con las respuestas
    /// fabricadas de cada forma —un nodo (10071, con número) y un cluster bajo carrera (10164, sin
    /// número)— a través de `parse_ack` y del bucle de reintento.
    #[tokio::test]
    async fn el_put_reintenta_ante_10071_y_ante_10164() {
        struct Scripted {
            replies: Mutex<Vec<&'static str>>,
            calls: AtomicUsize,
        }
        impl StateIo for Scripted {
            async fn read(&self, _: &str) -> Result<Current, FluxError> {
                Ok(Current::Absent)
            }
            async fn publish(
                &self,
                _: &str,
                _: &str,
                _: u64,
                _: &[u8],
            ) -> Result<Published, FluxError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let body = lock(&self.replies).remove(0);
                parse_ack(body.as_bytes())
            }
        }
        const UN_NODO: &str =
            r#"{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 402"}}"#;
        const CLUSTER: &str =
            r#"{"error":{"code":503,"err_code":10164,"description":"wrong last sequence"}}"#;
        const ACUSE: &str = r#"{"stream":"STATE_CLIENTES","seq":41}"#;

        let io = Scripted {
            replies: Mutex::new(vec![UN_NODO, CLUSTER, ACUSE]),
            calls: AtomicUsize::new(0),
        };
        let r = run_write(
            &io,
            &WriteJob {
                subject: SUBJECT,
                key: "cli-1",
                id: "i",
                bytes: b"{}",
                version: 1,
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!((r.applied, r.revision, r.attempts), (true, 41, 3));
        assert_eq!(io.calls.load(Ordering::SeqCst), 3);

        // Y con el 10164 solo: un SDK que reconozca únicamente el 10071 aquí NO reintenta.
        let solo_cluster = Scripted {
            replies: Mutex::new(vec![CLUSTER, ACUSE]),
            calls: AtomicUsize::new(0),
        };
        let r = run_write(
            &solo_cluster,
            &WriteJob {
                subject: SUBJECT,
                key: "k",
                id: "i",
                bytes: b"{}",
                version: 1,
            },
            None,
        )
        .await
        .unwrap();
        assert!(r.applied && r.attempts == 2, "{r:?}");
    }

    #[test]
    fn los_acuses_de_jetstream_se_interpretan() {
        let conflicto =
            |s: &str| matches!(parse_ack(s.as_bytes()), Ok(Published::SequenceConflict));
        assert!(conflicto(
            r#"{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 402"}}"#
        ));
        assert!(conflicto(
            r#"{"error":{"code":503,"err_code":10164,"description":"wrong last sequence"}}"#
        ));
        // Sin `code` ni descripción: lo que manda es `err_code`.
        assert!(conflicto(r#"{"error":{"err_code":10164}}"#));
        assert!(matches!(
            parse_ack(br#"{"stream":"STATE_CLIENTES","seq":41}"#),
            Ok(Published::Acked { seq: 41 })
        ));
        // `duplicate` no cambia nada: es la propia escritura.
        assert!(matches!(
            parse_ack(br#"{"stream":"S","seq":7,"duplicate":true}"#),
            Ok(Published::Acked { seq: 7 })
        ));
        // Lo que NO es un conflicto de secuencia no se reintenta a ciegas.
        for malo in [
            r#"{"error":{"code":400,"err_code":10077,"description":"maximum messages per subject exceeded"}}"#,
            r#"{"error":{"code":503,"err_code":10008}}"#,
            r#"{"error":{"description":"sin código"}}"#,
            r#"{"stream":"S"}"#,
            "no es json",
            "",
        ] {
            assert!(parse_ack(malo.as_bytes()).is_err(), "{malo}");
        }
        assert_eq!(SEQUENCE_CONFLICT_CODES, [10071, 10164]);
    }

    #[test]
    fn el_paso_1_lee_con_indulgencia() {
        let c = current_of(9, &body("abc", 7));
        assert_eq!(
            c,
            Current::Present {
                seq: 9,
                event_id: Some("abc".into()),
                version: Some(7)
            }
        );
        // Ilegible: solo la secuencia.
        assert_eq!(
            current_of(9, b"\xff\xfe"),
            Current::Present {
                seq: 9,
                event_id: None,
                version: None
            }
        );
        // Legible sin versión válida: el id sí, la versión no.
        let c = current_of(2, br#"{"id":"x","data":{"aggregateVersion":0}}"#);
        assert_eq!(
            c,
            Current::Present {
                seq: 2,
                event_id: Some("x".into()),
                version: None
            }
        );
    }

    // ── Qué cuenta la métrica ────────────────────────────────────────────────

    #[test]
    fn la_metrica_cuenta_lo_que_dice_protocol_json() {
        let ok = StateWrite {
            applied: true,
            outcome: WriteOutcome::Ok,
            revision: 1,
            attempts: 1,
            own_write: false,
        };
        let stale = StateWrite {
            applied: false,
            outcome: WriteOutcome::Stale,
            ..ok
        };
        assert_eq!(outcome_of(&Ok(ok)), Some(StateWriteOutcome::Ok));
        assert_eq!(
            outcome_of(&Ok(StateWrite {
                own_write: true,
                ..ok
            })),
            Some(StateWriteOutcome::Ok)
        );
        assert_eq!(outcome_of(&Ok(stale)), Some(StateWriteOutcome::Stale));
        assert_eq!(
            outcome_of(&Err(FluxError::StateConflict {
                key: "k".into(),
                attempts: 3
            })),
            Some(StateWriteOutcome::Conflict)
        );
        for invalido in [
            FluxError::MissingAggregateVersion { reason: "x".into() },
            FluxError::SchemaValidation(crate::SchemaValidationError {
                subject: "s".into(),
                dataschema: "d".into(),
                errors: vec![],
            }),
        ] {
            assert_eq!(
                outcome_of(&Err(invalido)),
                Some(StateWriteOutcome::InvalidSchema)
            );
        }
        // `notCounted` de protocol.json: errores de quien llama y de transporte.
        for no_cuenta in [
            FluxError::InvalidStateKey {
                key: "a.b".into(),
                reason: "x".into(),
            },
            FluxError::InvalidTenantForState {
                tenant: None,
                reason: "x".into(),
            },
            FluxError::ReservedDomain {
                domain: "state".into(),
                subject: "s".into(),
            },
            FluxError::Transport {
                context: "x".into(),
                source: "y".into(),
            },
            FluxError::permanent("restricted").with_code("INVALID_DATACLASSIFICATION"),
        ] {
            assert_eq!(outcome_of(&Err(no_cuenta)), None);
        }
        let metrica = &json_of(PROTOCOL)["state"]["metrics"]["flux_state_writes_total"];
        assert_eq!(metrica["notCounted"].as_array().unwrap().len(), 4);
    }

    // ── Qué comprueba el SDK al consumir — §5.4 ──────────────────────────────

    fn entrada_de(f: &str) -> Event {
        build_state_event(put_input(&fixture(f)["input"])).expect("evento")
    }

    #[test]
    fn una_entrada_valida_se_convierte_en_entrada() {
        let e = entrada_de("state-put");
        let entry = check_state_message(
            &StateContract::parse("clientes.cliente.v1").unwrap(),
            SUBJECT,
            &e,
            41,
        )
        .unwrap();
        assert_eq!(entry.key, "cli-1");
        assert_eq!(entry.tenant_id, "acme");
        assert!(!entry.deleted);
        assert_eq!(entry.aggregate_version, 7);
        assert_eq!(entry.revision, 41);
        let data: Value = serde_json::from_str(entry.data.as_ref().unwrap().get()).unwrap();
        assert_eq!(data["clienteId"], "cli-1");
        let tipada: Option<Value> = entry.data_as().unwrap();
        assert_eq!(tipada.unwrap()["aggregateVersion"], 7);
    }

    #[test]
    fn una_lapida_llega_como_deleted_con_data_none() {
        let f = fixture("state-tombstone");
        let i = &f["input"];
        let lapida = build_state_tombstone(StateTombstoneInput {
            contract: text(i, "contract"),
            tenant_id: text(i, "tenantId"),
            key: text(i, "key"),
            id: text(i, "id"),
            source: text(i, "source"),
            time: instant(&text(i, "time")),
            correlationid: text(i, "correlationid"),
            producerversion: text(i, "producerversion"),
            aggregate_version: 8,
            causationid: None,
            traceparent: None,
            tracestate: None,
        })
        .unwrap();
        let entry = check_state_message(
            &StateContract::parse("clientes.cliente.v1").unwrap(),
            SUBJECT,
            &lapida,
            42,
        )
        .unwrap();
        assert!(entry.deleted);
        assert!(entry.data.is_none(), "la lápida no lleva dato");
        assert_eq!(entry.aggregate_version, 8, "pero sí su versión");
        assert_eq!(entry.data_as::<Value>().unwrap(), None);
    }

    /// `STATE_SUBJECT_MISMATCH` es POISON, y la clave y el tenant de la entrada son los **del
    /// subject**, nunca los del sobre.
    #[test]
    fn un_sobre_que_miente_sobre_su_identidad_es_poison() {
        let c = StateContract::parse("clientes.cliente.v1").unwrap();
        let verdict =
            |e: &Event, subject: &str| check_state_message(&c, subject, e, 1).unwrap_err();
        let atributo = |v: Verdict| match v {
            Verdict::Poison(FluxError::StateSubjectMismatch { attribute, .. }) => attribute,
            otro => panic!("se esperaba un mismatch POISON: {otro:?}"),
        };

        // `tenantid` del sobre ≠ tenant del subject.
        assert_eq!(
            atributo(verdict(
                &entrada_de("state-put"),
                "state.clientes.cliente.v1.globex.cli-1"
            )),
            "tenantid"
        );
        // `subject` del sobre ≠ clave del subject.
        assert_eq!(
            atributo(verdict(
                &entrada_de("state-put"),
                "state.clientes.cliente.v1.acme.cli-2"
            )),
            "subject"
        );
        // Sin `subject` en el sobre.
        let mut sin = entrada_de("state-put");
        sin.aggregate_id = None;
        assert_eq!(atributo(verdict(&sin, SUBJECT)), "subject");
        // `type` de otro contrato (decisión provisional 8).
        let mut otro = entrada_de("state-put");
        otro.event_type = "com.flux.state.pedidos.pedido.v1".to_string();
        assert_eq!(atributo(verdict(&otro, SUBJECT)), "type");
        // Un subject que no es de este contrato.
        assert!(matches!(
            verdict(
                &entrada_de("state-put"),
                "state.pedidos.pedido.v1.acme.cli-1"
            ),
            Verdict::Poison(FluxError::StateSubjectMismatch { .. })
        ));
        // Y el mensaje trae los dos valores.
        let Verdict::Poison(e) = verdict(
            &entrada_de("state-put"),
            "state.clientes.cliente.v1.globex.cli-1",
        ) else {
            panic!()
        };
        assert_eq!(e.class(), Some(ErrorClass::Poison));
        let msg = e.to_string();
        assert!(msg.contains("acme") && msg.contains("globex"), "{msg}");
    }

    #[test]
    fn restricted_en_una_entrada_de_estado_es_poison() {
        let mut e = entrada_de("state-put");
        e.dataclassification = DataClassification::Restricted;
        let v = check_state_message(
            &StateContract::parse("clientes.cliente.v1").unwrap(),
            SUBJECT,
            &e,
            1,
        )
        .unwrap_err();
        let Verdict::Poison(err) = v else {
            panic!("{v:?}")
        };
        assert_eq!(err.code(), Some("INVALID_DATACLASSIFICATION"));
        assert_eq!(err.class(), Some(ErrorClass::Poison));
    }

    /// `MISSING_AGGREGATE_VERSION` también al consumir, y es PERMANENT (no POISON): el mensaje es
    /// legible y su identidad es correcta, simplemente no se puede aplicar de forma condicional.
    #[test]
    fn sin_version_valida_al_consumir_es_permanent() {
        for data in [
            json!({}),
            json!({ "aggregateVersion": 0 }),
            json!({ "aggregateVersion": "7" }),
        ] {
            let mut e = entrada_de("state-put");
            e.data = raw_of(&data);
            let v = check_state_message(
                &StateContract::parse("clientes.cliente.v1").unwrap(),
                SUBJECT,
                &e,
                1,
            )
            .unwrap_err();
            let Verdict::Permanent(err) = v else {
                panic!("{v:?}")
            };
            assert_eq!(err.code(), Some("MISSING_AGGREGATE_VERSION"), "{data}");
            assert_eq!(err.class(), Some(ErrorClass::Permanent));
        }
    }

    /// El orden de §5.4: identidad antes que clasificación antes que versión. Un sobre que miente
    /// Y no tiene versión es POISON, no PERMANENT.
    #[test]
    fn el_orden_de_las_comprobaciones_de_consumo() {
        let mut e = entrada_de("state-put");
        e.tenantid = "globex".to_string();
        e.dataclassification = DataClassification::Restricted;
        e.data = raw_of(&json!({}));
        let c = StateContract::parse("clientes.cliente.v1").unwrap();
        assert!(matches!(
            check_state_message(&c, SUBJECT, &e, 1),
            Err(Verdict::Poison(FluxError::StateSubjectMismatch { .. }))
        ));
        e.tenantid = "acme".to_string();
        let Err(Verdict::Poison(err)) = check_state_message(&c, SUBJECT, &e, 1) else {
            panic!()
        };
        assert_eq!(err.code(), Some("INVALID_DATACLASSIFICATION"));
        e.dataclassification = DataClassification::Confidential;
        assert!(matches!(
            check_state_message(&c, SUBJECT, &e, 1),
            Err(Verdict::Permanent(_))
        ));
    }

    #[test]
    fn los_tokens_de_un_subject_de_estado() {
        assert_eq!(subject_tokens(SUBJECT), ("acme", "cli-1"));
        // Un subject raro no entra en pánico.
        assert_eq!(subject_tokens("state"), ("system", "desconocida"));
    }

    // ── onSynced ─────────────────────────────────────────────────────────────

    /// §5.5, punto 2: vacío (o al día) → de inmediato. `num_pending` **y** `num_ack_pending`.
    #[test]
    fn on_synced_de_inmediato_solo_si_no_hay_nada_pendiente_ni_sin_confirmar() {
        assert!(synced_at_open(0, 0));
        assert!(!synced_at_open(1, 0), "queda una clave por entregar");
        assert!(!synced_at_open(0, 1), "hay una entregada y sin confirmar");
        assert!(!synced_at_open(100, 3));
    }

    /// §5.5, punto 3: la entrega con `pending == 0` cierra la foto, y solo esa.
    #[test]
    fn on_synced_al_llegar_la_entrega_con_pending_cero() {
        assert!(synced_after_delivery(0));
        assert!(!synced_after_delivery(1));
        assert!(!synced_after_delivery(99));
    }

    fn gate(callback: Option<Arc<dyn Fn() + Send + Sync>>) -> (SyncGate, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(false);
        (
            SyncGate {
                fired: AtomicBool::new(false),
                callback,
                tx,
            },
            rx,
        )
    }

    /// `onSynced` se invoca **una sola vez** por `watch`: ni una por tenant, ni por reconexión,
    /// ni por cada entrega con `pending == 0`.
    #[test]
    fn la_puerta_de_on_synced_se_abre_una_sola_vez() {
        let llamadas = Arc::new(AtomicUsize::new(0));
        let l = Arc::clone(&llamadas);
        let (puerta, rx) = gate(Some(Arc::new(move || {
            l.fetch_add(1, Ordering::SeqCst);
        })));
        assert!(!*rx.borrow());
        assert_eq!(puerta.fire(), Fired::Opened);
        assert_eq!(puerta.fire(), Fired::AlreadyOpen);
        assert_eq!(puerta.fire(), Fired::AlreadyOpen);
        assert_eq!(llamadas.load(Ordering::SeqCst), 1);
        assert!(*rx.borrow());
    }

    /// El callback es código de la aplicación en una tarea de fondo: su pánico no mata el
    /// consumo, y la puerta se abre igualmente.
    #[test]
    fn un_panico_en_on_synced_no_mata_el_consumo() {
        let (puerta, rx) = gate(Some(Arc::new(|| panic!("la aplicación se equivoca"))));
        assert_eq!(puerta.fire(), Fired::CallbackPanicked);
        assert!(*rx.borrow(), "la puerta se abrió");
        assert_eq!(puerta.fire(), Fired::AlreadyOpen);
    }

    // ── La suma de pendientes ────────────────────────────────────────────────

    /// §9: dos `watch` del mismo contrato (uno por tenant) publican **la suma** y no se pisan.
    #[test]
    fn el_gauge_de_pendientes_es_la_suma_de_los_durables_de_la_serie() {
        let p = PendingSum::default();
        let (s, c) = (
            "state.clientes.cliente.v1",
            "tienda__state_clientes_cliente_v1",
        );
        assert_eq!(
            p.set(s, c, "tienda__state_clientes_cliente_v1__acme", 10),
            10
        );
        assert_eq!(
            p.set(s, c, "tienda__state_clientes_cliente_v1__globex", 5),
            15
        );
        // Un durable se SOBRESCRIBE, no se acumula.
        assert_eq!(p.set(s, c, "tienda__state_clientes_cliente_v1__acme", 2), 7);
        // Otro contrato es otra serie.
        assert_eq!(p.set("state.pedidos.pedido.v1", c, "x", 1), 1);
        assert_eq!(
            p.forget(s, c, "tienda__state_clientes_cliente_v1__acme"),
            Some(5)
        );
        assert_eq!(
            p.forget(s, c, "tienda__state_clientes_cliente_v1__globex"),
            None
        );
        assert_eq!(p.forget(s, c, "inexistente"), None);
    }

    // ── La config del durable ────────────────────────────────────────────────

    fn pedida(filter: &str) -> consumer::pull::Config {
        consumer::pull::Config {
            durable_name: Some("tienda-web__state_clientes_cliente_v1__acme".into()),
            filter_subject: filter.into(),
            ack_policy: AckPolicy::Explicit,
            ack_wait: DEFAULT_ACK_WAIT,
            max_deliver: i64::from(DEFAULT_MAX_DELIVER),
            backoff: CANONICAL_BACKOFF.to_vec(),
            max_ack_pending: i64::from(DEFAULT_MAX_ACK_PENDING),
            deliver_policy: DeliverPolicy::LastPerSubject,
            replay_policy: ReplayPolicy::Instant,
            ..Default::default()
        }
    }

    fn efectiva(req: &consumer::pull::Config) -> consumer::Config {
        consumer::Config {
            durable_name: req.durable_name.clone(),
            filter_subject: req.filter_subject.clone(),
            ack_policy: req.ack_policy,
            ack_wait: req.ack_wait,
            max_deliver: req.max_deliver,
            backoff: req.backoff.clone(),
            max_ack_pending: req.max_ack_pending,
            deliver_policy: req.deliver_policy,
            replay_policy: req.replay_policy,
            ..Default::default()
        }
    }

    #[test]
    fn un_durable_aprovisionado_distinto_se_detecta() {
        let req = pedida("state.clientes.cliente.v1.acme.*");
        let nombre = "tienda-web__state_clientes_cliente_v1__acme";
        assert!(assert_state_config_honored(nombre, &req, &efectiva(&req)).is_ok());

        // `deliver_policy: all` entregaría la historia, no la foto.
        let mut todo = efectiva(&req);
        todo.deliver_policy = DeliverPolicy::All;
        let err = assert_state_config_honored(nombre, &req, &todo).unwrap_err();
        let FluxError::ConsumerConfigMismatch { differences, .. } = err else {
            panic!()
        };
        assert!(
            differences.iter().any(|d| d.field == "deliver_policy"),
            "{differences:?}"
        );

        // Un filtro más ancho rompería el aislamiento por tenant.
        let mut ancho = efectiva(&req);
        ancho.filter_subject = "state.clientes.cliente.v1.*.*".into();
        let err = assert_state_config_honored(nombre, &req, &ancho).unwrap_err();
        let FluxError::ConsumerConfigMismatch { differences, .. } = err else {
            panic!()
        };
        assert!(
            differences.iter().any(|d| d.field == "filter_subject"),
            "{differences:?}"
        );

        // Y lo canónico sigue comprobándose: `ack_wait` sobrescrito por `backoff[0]`.
        let mut corto = efectiva(&req);
        corto.ack_wait = Duration::from_secs(1);
        let err = assert_state_config_honored(nombre, &req, &corto).unwrap_err();
        let FluxError::ConsumerConfigMismatch { differences, .. } = err else {
            panic!()
        };
        assert!(
            differences.iter().any(|d| d.field == "ack_wait"),
            "{differences:?}"
        );
    }

    // ── Opciones y resultado ─────────────────────────────────────────────────

    #[test]
    fn put_sin_clasificacion_es_confidential_y_restricted_se_rechaza() {
        // El default NO es el de la conexión: un estado es donde se permite que viva el dato
        // personal (§3.6). Lo fija la construcción de la entrada, que es lo que ve el broker.
        assert_eq!(PutOptions::default().classification, None);
        let f = fixture("state-put");
        let mut input = put_input(&f["input"]);
        input.classification = DataClassification::Restricted;
        let err = build_state_event(input).unwrap_err();
        assert_eq!(err.code(), Some("INVALID_DATACLASSIFICATION"));
        assert_eq!(err.class(), Some(ErrorClass::Permanent));
        for c in [
            DataClassification::Public,
            DataClassification::Internal,
            DataClassification::Confidential,
        ] {
            let mut input = put_input(&f["input"]);
            input.classification = c;
            assert!(build_state_event(input).is_ok(), "{c}");
        }
    }

    #[test]
    fn el_constructor_puro_valida_en_el_orden_de_la_spec() {
        let f = fixture("state-put");
        let con = |m: &dyn Fn(&mut StateEventInput)| {
            let mut i = put_input(&f["input"]);
            m(&mut i);
            code_of(&build_state_event(i))
        };
        assert_eq!(
            con(&|i| i.contract = "state.cliente.v1".into()).as_deref(),
            Some("RESERVED_DOMAIN")
        );
        assert_eq!(
            con(&|i| i.key = "a.b".into()).as_deref(),
            Some("INVALID_STATE_KEY")
        );
        assert_eq!(
            con(&|i| i.tenant_id = "ACME".into()).as_deref(),
            Some("INVALID_TENANT_FOR_STATE")
        );
        assert_eq!(
            con(&|i| i.data = raw_of(&json!({ "clienteId": "x" }))).as_deref(),
            Some("MISSING_AGGREGATE_VERSION")
        );
        assert_eq!(
            con(&|i| i.data = raw_of(&json!([1]))).as_deref(),
            Some("MISSING_AGGREGATE_VERSION")
        );
        // Un campo obligatorio vacío es un error del que construye, no uno de los seis.
        assert_eq!(con(&|i| i.id = String::new()), None);
    }

    #[test]
    fn la_lapida_exige_una_version_valida() {
        let f = fixture("state-tombstone");
        let i = &f["input"];
        let con = |version: u64| {
            build_state_tombstone(StateTombstoneInput {
                contract: text(i, "contract"),
                tenant_id: text(i, "tenantId"),
                key: text(i, "key"),
                id: text(i, "id"),
                source: text(i, "source"),
                time: instant(&text(i, "time")),
                correlationid: text(i, "correlationid"),
                producerversion: text(i, "producerversion"),
                aggregate_version: version,
                causationid: None,
                traceparent: None,
                tracestate: None,
            })
        };
        assert!(con(1).is_ok());
        assert!(con(MAX_AGGREGATE_VERSION).is_ok());
        assert_eq!(
            code_of(&con(0)).as_deref(),
            Some("MISSING_AGGREGATE_VERSION")
        );
        assert_eq!(
            code_of(&con(MAX_AGGREGATE_VERSION + 1)).as_deref(),
            Some("MISSING_AGGREGATE_VERSION")
        );
        // La lápida lleva `internal` y su `dataschema` fijo, de cualquier contrato.
        let l = con(8).unwrap();
        assert_eq!(l.dataclassification, DataClassification::Internal);
        assert_eq!(l.dataschema, TOMBSTONE_DATASCHEMA);
        assert_eq!(l.data.get(), "{\"aggregateVersion\":8}");
        let protocol = json_of(PROTOCOL);
        assert_eq!(
            TOMBSTONE_DATASCHEMA,
            protocol["state"]["tombstone"]["dataschema"]
        );
    }

    #[test]
    fn el_dataschema_se_deriva_de_la_url_base() {
        assert_eq!(
            state_dataschema("https://schemas.internal/", "clientes.cliente.v1").unwrap(),
            "https://schemas.internal/state/clientes/cliente/1.0.0.json"
        );
        assert_eq!(
            state_dataschema(BASE, "logistica.linea-envio.v2").unwrap(),
            "https://schemas.internal/state/logistica/linea-envio/2.0.0.json"
        );
        assert!(state_dataschema(BASE, "state.cliente.v1").is_err());
    }

    #[test]
    fn el_resultado_dice_lo_mismo_de_dos_formas() {
        assert_eq!(WriteOutcome::Ok.as_str(), "ok");
        assert_eq!(WriteOutcome::Stale.to_string(), "stale");
        assert_eq!(
            json_of(PROTOCOL)["state"]["write"]["outcomes"],
            json!([WriteOutcome::Ok.as_str(), WriteOutcome::Stale.as_str()])
        );
        assert_eq!(
            json_of(PROTOCOL)["state"]["write"]["maxAttempts"],
            STATE_MAX_ATTEMPTS
        );
        assert_eq!(
            json_of(PROTOCOL)["state"]["write"]["conflictErrCodes"],
            json!(SEQUENCE_CONFLICT_CODES)
        );
        assert_eq!(
            json_of(PROTOCOL)["state"]["aggregateVersion"]["maximum"],
            MAX_AGGREGATE_VERSION
        );
    }

    #[test]
    fn la_entrada_se_compara_por_valor() {
        let a = StateEntry {
            key: "k".into(),
            tenant_id: "t".into(),
            deleted: false,
            data: Some(raw_of(&json!({ "aggregateVersion": 1 }))),
            aggregate_version: 1,
            revision: 3,
        };
        assert_eq!(a, a.clone());
        let mut b = a.clone();
        b.revision = 4;
        assert_ne!(a, b);
        let mut c = a.clone();
        c.data = None;
        assert_ne!(a, c);
        // `data_as` de un valor que no encaja es PERMANENT (DATA_SCHEMA_MISMATCH), no POISON.
        let err = a.data_as::<Vec<u8>>().unwrap_err();
        assert_eq!(err.class(), Some(ErrorClass::Permanent));
        assert_eq!(err.code(), Some("DATA_SCHEMA_MISMATCH"));
    }

    #[test]
    fn el_stream_de_estado_en_los_nombres_de_protocol_json() {
        let stream = &json_of(PROTOCOL)["state"]["stream"];
        assert_eq!(
            StateContract::parse("clientes.cliente.v1")
                .unwrap()
                .stream_name(),
            "STATE_CLIENTES"
        );
        assert_eq!(stream["maxMsgsPerSubject"], 1);
        assert_eq!(stream["sdkQueriesStreamInfo"], false);
        assert_eq!(stream["format"], "STATE_<DOMINIO>");
    }
}
