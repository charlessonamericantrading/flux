//! Petición/respuesta — **extensión OPCIONAL** de v1.1, capacidad `rpc`.
//!
//! Contrato normativo: `specification/11-request-reply.md`. Si algo de esta documentación diverge
//! de la spec, manda la spec.
//!
//! Un evento dice **qué pasó**; el estado es **el dato**; la petición/respuesta es la **orden que
//! necesita respuesta ya** y el **cálculo**: reservar el stock y saber si se pudo, pedir el precio de
//! un carrito. Sustituye a `POST /accion` entre servicios propios, con el mismo sobre, el mismo
//! tenant, la misma clasificación de errores y los mismos permisos que el resto del protocolo.
//!
//! ```no_run
//! use serde_json::json;
//!
//! # async fn ejemplo(bus: flux::Bus) -> Result<(), flux::FluxError> {
//! // El dueño atiende la operación. Una suscripción por operación, con el servicio como grupo de
//! // cola: dos instancias se reparten las peticiones.
//! let responder = bus
//!     .respond("clientes.cliente.v1.calcular-descuento", |peticion: flux::RpcRequest, ctx: flux::RpcContext| async move {
//!         // OBLIGATORIO: deduplicar por `ctx.id`. Una orden puede llegar dos veces (03-delivery.md §4).
//!         let total: serde_json::Value = peticion.data_as()?;
//!         let _ = (ctx.id, total);
//!         Ok(json!({ "descuentoCents": 500, "moneda": "EUR" }))
//!     })
//!     .await?;
//!
//! // Quien llama recibe el sobre de la respuesta; `data_as` decodifica su `data`.
//! let respuesta = bus
//!     .request_with(
//!         "clientes.cliente.v1.calcular-descuento",
//!         &json!({ "clienteId": "cli-1", "totalCents": 9990, "moneda": "EUR" }),
//!         flux::RequestOptions::default().with_timeout_ms(2000).with_aggregate_id("cli-1"),
//!     )
//!     .await?;
//! let _: serde_json::Value = respuesta.data_as()?;
//!
//! responder.stop().await; // dejar de escuchar, esperar a lo que está en vuelo, y solo entonces cerrar
//! # Ok(()) }
//! ```
//!
//! # Lo que el SDK hace y no te dice
//!
//! - **Core NATS, nunca JetStream** (§2.1): una petición no se persiste, y si nadie escucha quien
//!   llama lo sabe en milisegundos (`RPC_UNAVAILABLE`), no tras su timeout. Eso exige que la conexión
//!   anuncie `headers` y `no_responders` en CONNECT: `async-nats` 0.50 lo hace siempre (A-57).
//! - **Una petición denegada por la ACL no agota el timeout**: `async-nats` no la entrega como error
//!   —su petición solo da un timeout— y el `-ERR` llega por el evento de la conexión (A-72). El SDK
//!   lo escucha y lo compite con la respuesta: `PERMISSION_DENIED` en milisegundos (§7.1).
//! - **Una suscripción por operación, al subject exacto** (§2.3), con el servicio como grupo de cola.
//!   Nunca `rpc.<dominio>.>`: con el comodín una operación sin handler daría timeout en vez de
//!   `no responders` (A-64). Un dueño sin permiso de suscripción **falla al arrancar**, no se queda
//!   mudo (A-73).
//! - **La respuesta se valida contra la petición** (§4.3, cinco comprobaciones): lo que no es la
//!   respuesta a esta petición es `RPC_INVALID_REPLY`, también el acuse de un stream que captura
//!   `rpc.>` (A-18, A-67).
//! - **Quien responde siempre contesta, con un error si hace falta** (§5.2) y comprueba el tamaño de
//!   su respuesta **antes** de publicarla: si no cupiese, `async-nats` lanzaría en él y quien llama
//!   solo vería un timeout (A-65).
//! - **Parar es dejar de escuchar, esperar a lo que está en vuelo y cerrar** (§5.5):
//!   [`Responder::stop`] y [`crate::Bus::close`]. Cerrar la conexión de golpe pierde lo que se atiende
//!   (A-62).
//!
//! # Funciones puras
//!
//! [`rpc_subject`], [`resolve_timeout_ms`], [`build_rpc_request`], [`build_rpc_reply`] y
//! [`build_rpc_error`] construyen los **bytes** exactos de `11-request-reply.md` §3.5 sin reloj ni
//! UUID propios: `id` y `time` vienen dados, que es lo que permite compararlos byte a byte entre SDKs.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures::{FutureExt, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::value::RawValue;
use serde_json::Value;
use tokio::sync::{watch, Semaphore};
use uuid::Uuid;

use crate::client::{lock, permission_denied, transport, Bus, LogLevel};
use crate::context::{self, EventContext};
use crate::envelope::{format_time, parse_event, DataClassification, Event};
use crate::errors::{
    as_classified, describe, Classification, ErrorClass, Failure, FluxError, HandlerError,
};
use crate::metrics::{RpcClientOutcome, RpcRole, RpcServerOutcome};
use crate::protocol::{is_kebab_lower, MAX_MESSAGE_BYTES, SPEC_VERSION};

// ─── Constantes del protocolo — protocol.json → rpc ──────────────────────────

/// El timeout por defecto de una petición: 5 s — 11-request-reply.md §4.2.
pub const DEFAULT_TIMEOUT_MS: u64 = 5_000;

/// El timeout mínimo: 1 ms. Fuera de rango es `INVALID_RPC_CALL`, **no se recorta en silencio**.
pub const MIN_TIMEOUT_MS: u64 = 1;

/// El timeout máximo: 25 s, **por debajo del `ack_wait` de 30 s** ([`crate::DEFAULT_ACK_WAIT`]):
/// para que una llamada hecha desde un handler de eventos no provoque su reentrega mientras espera.
pub const MAX_TIMEOUT_MS: u64 = 25_000;

/// La ventana de `STALE_REQUEST`: con la firma en `require`, quien responde rechaza una petición cuyo
/// `time` se aleje más de 300 s de su reloj, por delante o por detrás — 11-request-reply.md §7.4.
pub const STALE_REQUEST_WINDOW_SECS: i64 = 300;

/// Handlers concurrentes por operación: el mismo orden que `max_ack_pending` (256). El exceso espera
/// en la cola de la suscripción — 11-request-reply.md §5.6.
pub const MAX_CONCURRENT_HANDLERS: usize = 256;

/// Cuánto espera [`Responder::stop`] a los handlers en vuelo: 25 s, el timeout máximo —una petición
/// más lenta ya la dio por perdida quien llamó— — 11-request-reply.md §5.5.
pub const STOP_TIMEOUT: Duration = Duration::from_secs(25);

/// El `type` del error de toda operación: **uno solo**, no depende del contrato — §3.3.
pub const RPC_ERROR_TYPE: &str = "com.flux.rpc.error.v1";

/// El `dataschema` **fijo** del error — §3.3.
pub const RPC_ERROR_DATASCHEMA: &str = "https://schemas.internal/system/rpc/error/1.0.0.json";

/// El máximo de caracteres de `message` en el esquema del error (`schemas/system/rpc/error/1.0.0.json`).
const ERROR_MESSAGE_MAX_CHARS: usize = 1_000;

/// El máximo de caracteres de `code` en el esquema del error.
const ERROR_CODE_MAX_CHARS: usize = 64;

/// Cuánto espera `respond()` el aviso de una suscripción denegada.
///
/// `async-nats` no ofrece un viaje de ida y vuelta (su `flush()` solo vacía el búfer de escritura) y
/// el dueño de una operación no tiene permiso de publicar en ningún sitio con el que provocarlo, así
/// que **no hay una señal positiva** de «el servidor aceptó la suscripción»: solo se puede esperar el
/// `-ERR`. Medido contra nats-server 2.14.5 en local, llega en ~1 ms; 100 ms cubren una red de
/// área local con holgura (A-90). Es el único temporizador fijo de este módulo y es de arranque: se
/// paga una vez por operación atendida.
const SUBSCRIBE_SETTLE: Duration = Duration::from_millis(100);

/// Un margen breve tras dejar de escuchar y antes de cerrar: en un cluster el interés se propaga de
/// forma asíncrona, y la petición que sale justo cuando se va el último puede agotar su timeout en
/// vez de recibir `no responders` (A-70: 4 de 10 sin esperar, 0 de 10 a los 5 ms) — §5.5.
const STOP_MARGIN: Duration = Duration::from_millis(10);

/// Los reintentos que admite una llamada: más es casi siempre un error (cada intento gasta un
/// timeout entero) y la idempotencia del que responde es lo que los hace seguros.
pub const MAX_RETRIES: u32 = 5;

/// La pausa entre reintentos si la llamada no pide otra: 100 ms. Un `RPC_UNAVAILABLE` es inmediato
/// —el broker lo dice en ~1 ms— y reintentarlo en bucle cerrado no daría tiempo a que el servicio
/// vuelva.
pub const DEFAULT_RETRY_DELAY: Duration = Duration::from_millis(100);

// ─── La operación ────────────────────────────────────────────────────────────

/// Qué lado de una operación: su petición o su respuesta. Cada una tiene su tipo y su esquema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RpcSide {
    /// La petición.
    Request,
    /// La respuesta.
    Reply,
}

impl RpcSide {
    /// `"request"` o `"reply"`: va en el `type` y en la ruta del esquema.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Reply => "reply",
        }
    }
}

/// Una operación de petición/respuesta: `<dominio>.<agregado>.v<major>.<operacion>` — §2.2.
///
/// Es lo que reciben [`Bus::request`] y [`Bus::respond`]: **cuatro tokens, sin el prefijo `rpc.`**,
/// que lo pone el SDK. Tiene exactamente la forma sintáctica de un subject de evento; lo que las
/// separa es el prefijo `rpc.` del subject de NATS, del `type` y de la ruta del esquema, y la forma
/// gramatical del último token: el evento va en pasado, la operación en infinitivo (una convención
/// de revisión que ninguna regex distingue).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcOperation {
    /// Dominio, kebab-case y **nunca** uno de [`crate::RESERVED_DOMAINS`].
    pub domain: String,
    /// Agregado, kebab-case.
    pub aggregate: String,
    /// La versión mayor **como texto**: `1`, `2`… Sin límite de tamaño, como el patrón
    /// `v[1-9][0-9]*` de `protocol.json` (un `u32` rechazaría `v99999999999`).
    pub major: String,
    /// El nombre de la operación: un infinitivo en kebab-case (`calcular-descuento`).
    pub name: String,
}

/// `v` + un entero ≥ 1 sin ceros a la izquierda, sin tope: `^v[1-9][0-9]*$`.
fn parse_major_text(token: &str) -> Option<&str> {
    let digits = token.strip_prefix('v')?;
    let valid = !digits.is_empty()
        && !digits.starts_with('0')
        && digits.bytes().all(|b| b.is_ascii_digit());
    valid.then_some(digits)
}

impl RpcOperation {
    /// Valida y descompone el nombre de una operación.
    ///
    /// # Errores
    ///
    /// En este orden, el primero que falla gana: [`FluxError::ReservedDomain`] si el dominio es `dlq`,
    /// `state`, `rpc` o `system` —también un nombre con el prefijo `rpc.`: su dominio sería `rpc`—, y
    /// [`FluxError::InvalidRpcCall`] (`INVALID_RPC_CALL`) si no son cuatro tokens
    /// `<dominio>.<agregado>.v<major>.<operacion>` en minúsculas y kebab-case, o si lleva un comodín.
    /// Los dos **antes de tocar la red**.
    pub fn parse(operation: &str) -> Result<Self, FluxError> {
        crate::state::reject_reserved_domain(operation)?;
        let invalid = |reason: &str| {
            FluxError::InvalidRpcCall {
            reason: format!(
                "la operación {operation:?} no es válida: {reason}. Una operación son cuatro tokens \
                 `<dominio>.<agregado>.v<major>.<operacion>` (`clientes.cliente.v1.calcular-descuento`) \
                 en minúsculas y kebab-case, sin el prefijo `rpc.` —lo pone el SDK— y sin comodines \
                 (patrón `^[a-z0-9]+(-[a-z0-9]+)*\\.[a-z0-9]+(-[a-z0-9]+)*\\.v[1-9][0-9]*\\.[a-z0-9]+(-[a-z0-9]+)*$`)"
            ),
        }
        };
        let tokens: Vec<&str> = operation.split('.').collect();
        if tokens.len() != 4 {
            return Err(invalid(&format!(
                "tiene {} token(s) y deben ser exactamente 4",
                tokens.len()
            )));
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
        let Some(major) = parse_major_text(tokens[2]) else {
            return Err(invalid(
                "el tercer token debe ser la versión mayor: literal `v` + entero >= 1 (`v1`)",
            ));
        };
        if !is_kebab_lower(tokens[3]) {
            return Err(invalid(
                "la operación (el último token) debe ser un infinitivo en kebab-case ([a-z0-9-]): \
                 sin mayúsculas, guiones bajos, espacios ni comodines",
            ));
        }
        Ok(Self {
            domain: tokens[0].to_string(),
            aggregate: tokens[1].to_string(),
            major: major.to_string(),
            name: tokens[3].to_string(),
        })
    }

    /// `clientes.cliente.v1.calcular-descuento`: lo que reciben `request` y `respond` y lo que va en
    /// la etiqueta `operation` de las métricas.
    #[must_use]
    pub fn name_with_path(&self) -> String {
        format!(
            "{}.{}.v{}.{}",
            self.domain, self.aggregate, self.major, self.name
        )
    }

    /// `rpc.clientes.cliente.v1.calcular-descuento`: el subject de NATS. Cinco tokens, siempre.
    #[must_use]
    pub fn subject(&self) -> String {
        format!("rpc.{}", self.name_with_path())
    }

    /// `com.flux.rpc.clientes.cliente.calcular-descuento.request.v1` o `….reply.v1` — §3.1, §3.2.
    #[must_use]
    pub fn event_type(&self, side: RpcSide) -> String {
        format!(
            "com.flux.rpc.{}.{}.{}.{}.v{}",
            self.domain,
            self.aggregate,
            self.name,
            side.as_str(),
            self.major
        )
    }

    /// El `dataschema` derivado de una URL base, sin bundle: `<base>/rpc/<dom>/<agr>/<op>/<lado>/<major>.0.0.json`
    /// — §3.1. Sin bundle solo se puede asumir el `<major>.0.0`: basta para L2 (el atributo es
    /// informativo) pero no para L3, que resuelve el exacto en el índice `rpc` del bundle.
    #[must_use]
    pub fn default_dataschema(&self, base_url: &str, side: RpcSide) -> String {
        format!(
            "{}/rpc/{}/{}/{}/{}/{}.0.0.json",
            base_url.trim_end_matches('/'),
            self.domain,
            self.aggregate,
            self.name,
            side.as_str(),
            self.major
        )
    }
}

/// El subject de NATS de una operación: `rpc.<operación>` — §2.2. Es lo que comprueba la operación
/// `rpc-operation` del arnés cruzado.
///
/// # Errores
///
/// Los de [`RpcOperation::parse`].
pub fn rpc_subject(operation: &str) -> Result<String, FluxError> {
    Ok(RpcOperation::parse(operation)?.subject())
}

/// El `dataschema` de un lado de una operación, derivado de la URL base de esquemas:
/// `https://schemas.internal` → `https://schemas.internal/rpc/clientes/cliente/obtener/request/1.0.0.json`.
///
/// # Errores
///
/// Los de [`RpcOperation::parse`].
pub fn rpc_dataschema(base_url: &str, operation: &str, side: RpcSide) -> Result<String, FluxError> {
    Ok(RpcOperation::parse(operation)?.default_dataschema(base_url, side))
}

// ─── El timeout ──────────────────────────────────────────────────────────────

/// Resuelve el timeout de una llamada, en milisegundos — §4.2: ausente es 5000; si no, un entero entre
/// 1 y 25 000.
///
/// Recibe un entero **con signo** porque `0` y los negativos son errores que un `u64` taparía. Lo que
/// un lenguaje de tipos débiles puede mandar —`1.5`, `"5000"`, `null`, `NaN`— no es representable
/// aquí: el arnés de conformidad lo rechaza antes de llegar.
///
/// # Errores
///
/// [`FluxError::InvalidRpcCall`] (`INVALID_RPC_CALL`) fuera de rango: **no se recorta en silencio**.
pub fn resolve_timeout_ms(timeout_ms: Option<i64>) -> Result<u64, FluxError> {
    let Some(ms) = timeout_ms else {
        return Ok(DEFAULT_TIMEOUT_MS);
    };
    match u64::try_from(ms) {
        Ok(v) if (MIN_TIMEOUT_MS..=MAX_TIMEOUT_MS).contains(&v) => Ok(v),
        _ => Err(FluxError::InvalidRpcCall {
            reason: format!(
                "`timeout` fuera de rango: {ms} ms. Debe ser un entero entre {MIN_TIMEOUT_MS} y \
                 {MAX_TIMEOUT_MS} ms (por defecto {DEFAULT_TIMEOUT_MS}): el máximo es 25 s para \
                 quedar por debajo del `ack_wait` de 30 s, y un valor fuera de rango se rechaza, no se \
                 recorta (11-request-reply.md §4.2)"
            ),
        }),
    }
}

/// [`resolve_timeout_ms`] para un `Duration`: tiene que ser un número **entero** de milisegundos
/// (`1.5 ms` es un error, como el `1.5` de los vectores).
fn resolve_timeout(timeout: Option<Duration>) -> Result<Duration, FluxError> {
    let Some(d) = timeout else {
        return Ok(Duration::from_millis(DEFAULT_TIMEOUT_MS));
    };
    if d.subsec_nanos() % 1_000_000 != 0 {
        return Err(FluxError::InvalidRpcCall {
            reason: format!(
                "`timeout` de {d:?} no es un número entero de milisegundos: debe ser un entero entre \
                 {MIN_TIMEOUT_MS} y {MAX_TIMEOUT_MS} ms (11-request-reply.md §4.2)"
            ),
        });
    }
    let ms = i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
    resolve_timeout_ms(Some(ms)).map(Duration::from_millis)
}

// ─── El error que viaja en la respuesta ──────────────────────────────────────

/// El `data` de un error `com.flux.rpc.error.v1` — §3.3: `{ class, code, message, retryAfterMs? }`.
///
/// `class` es **binaria**: `poison` no viaja. Para quien llama, una petición que el otro lado no pudo
/// interpretar es `permanent`; quien responde la cuenta como `poison` en **su** métrica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcErrorBody {
    /// `retryable` o `permanent`, y nada más.
    pub class: ErrorClass,
    /// Identificador **estable** (`^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*$`, 64 como máximo): el que agrupa el
    /// panel. Nunca el mensaje.
    pub code: String,
    /// Qué falló y cómo se arregla (1 a 1000 caracteres). **Sin valores de `data`.**
    pub message: String,
    /// Sugerencia para el **primer** reintento, en milisegundos. Solo con `retryable`.
    pub retry_after_ms: Option<u64>,
}

/// `^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*$` — `errors.codePattern` de `protocol.json`.
fn is_stable_code(code: &str) -> bool {
    let mut segments = code.split('_');
    let Some(first) = segments.next() else {
        return false;
    };
    let mut chars = first.chars();
    let head_ok = chars.next().is_some_and(|c| c.is_ascii_uppercase());
    head_ok
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        && segments.all(|s| {
            !s.is_empty()
                && s.chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        })
}

impl RpcErrorBody {
    /// Comprueba el cuerpo contra el esquema del error (`schemas/system/rpc/error/1.0.0.json`): es lo
    /// mismo que quien llama exige de lo que recibe (la comprobación 5 de §4.3).
    ///
    /// Devuelve el motivo, que **nunca** cita un valor recibido.
    fn check(&self) -> Result<(), String> {
        if !matches!(self.class, ErrorClass::Retryable | ErrorClass::Permanent) {
            return Err("`class` debe ser `retryable` o `permanent` (`poison` no viaja)".into());
        }
        if self.code.chars().count() > ERROR_CODE_MAX_CHARS || !is_stable_code(&self.code) {
            return Err(format!(
                "`code` debe cumplir ^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*$ con {ERROR_CODE_MAX_CHARS} \
                 caracteres como máximo"
            ));
        }
        let chars = self.message.chars().count();
        if chars == 0 || chars > ERROR_MESSAGE_MAX_CHARS {
            return Err(format!(
                "`message` debe tener entre 1 y {ERROR_MESSAGE_MAX_CHARS} caracteres"
            ));
        }
        if self.retry_after_ms.is_some() && self.class != ErrorClass::Retryable {
            return Err("`retryAfterMs` solo tiene sentido con `class: retryable`".into());
        }
        Ok(())
    }

    /// Lo que se publica: el cuerpo con el código sustituido por el de la clase si no cumple el
    /// patrón, y el mensaje recortado a 1000 caracteres —un error que la propia respuesta no puede
    /// cumplir lo recibiría quien llama como `RPC_INVALID_REPLY—.
    ///
    /// Es lo que usa quien responde con lo que **la aplicación** lanzó: un código en minúsculas o un
    /// mensaje larguísimo es un descuido de la aplicación, no motivo para que quien llama vea un
    /// incidente.
    fn normalized(mut self) -> Self {
        if self.code.chars().count() > ERROR_CODE_MAX_CHARS || !is_stable_code(&self.code) {
            self.code = default_code(self.class).to_string();
        }
        if self.message.trim().is_empty() {
            self.message = "error del servicio".to_string();
        }
        if self.message.chars().count() > ERROR_MESSAGE_MAX_CHARS {
            let mut cut: String = self
                .message
                .chars()
                .take(ERROR_MESSAGE_MAX_CHARS - 1)
                .collect();
            cut.push('…');
            self.message = cut;
        }
        if self.class != ErrorClass::Retryable {
            self.retry_after_ms = None;
        }
        self
    }

    /// El `data` serializado, con las claves en el orden de §3.3: `class`, `code`, `message` y
    /// `retryAfterMs` (solo si hay).
    fn to_data(&self) -> Result<Box<RawValue>, FluxError> {
        let mut data = serde_json::Map::new();
        data.insert("class".into(), Value::String(self.class.as_str().into()));
        data.insert("code".into(), Value::String(self.code.clone()));
        data.insert("message".into(), Value::String(self.message.clone()));
        if let Some(ms) = self.retry_after_ms {
            data.insert("retryAfterMs".into(), Value::from(ms));
        }
        serde_json::value::to_raw_value(&Value::Object(data))
            .map_err(|e| FluxError::Envelope(format!("el error no es serializable a JSON: {e}")))
    }

    /// Interpreta el `data` de un error recibido y lo comprueba contra su esquema.
    ///
    /// # Errores
    ///
    /// El motivo (sin valores) por el que **no** cumple el esquema del error.
    fn from_data(data: &str) -> Result<Self, String> {
        let value: Value =
            serde_json::from_str(data).map_err(|_| "su `data` no es JSON válido".to_string())?;
        let Some(object) = value.as_object() else {
            return Err("el `data` del error no es un objeto".into());
        };
        if let Some(extra) = object
            .keys()
            .find(|k| !matches!(k.as_str(), "class" | "code" | "message" | "retryAfterMs"))
        {
            return Err(format!(
                "el `data` del error lleva una propiedad que el esquema no declara (`{extra}`)"
            ));
        }
        let text = |key: &str| -> Result<String, String> {
            match object.get(key) {
                Some(Value::String(s)) => Ok(s.clone()),
                Some(_) => Err(format!("`{key}` del error debe ser una cadena")),
                None => Err(format!("falta `{key}` en el `data` del error")),
            }
        };
        let class = match text("class")?.as_str() {
            "retryable" => ErrorClass::Retryable,
            "permanent" => ErrorClass::Permanent,
            _ => return Err("`class` del error debe ser `retryable` o `permanent`".into()),
        };
        let retry_after_ms = match object.get("retryAfterMs") {
            None => None,
            Some(Value::Number(n)) => {
                let ms = n
                    .as_u64()
                    // `5000.0` es un entero para JSON Schema; los escribe un serializador ajeno.
                    .or_else(|| {
                        n.as_f64()
                            .filter(|f| *f >= 0.0 && f.fract() == 0.0)
                            .map(|f| {
                                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                                {
                                    f as u64
                                }
                            })
                    })
                    .ok_or_else(|| "`retryAfterMs` debe ser un entero >= 0".to_string())?;
                Some(ms)
            }
            Some(_) => return Err("`retryAfterMs` debe ser un entero >= 0".into()),
        };
        let body = Self {
            class,
            code: text("code")?,
            message: text("message")?,
            retry_after_ms,
        };
        body.check()?;
        Ok(body)
    }

    /// El error de flux que recibe quien llama: `RetryableError` o `PermanentError`, con el `code`, el
    /// `message` y, si lo trae, el `retryAfterMs` como `retry_after` — §3.3.
    fn into_flux_error(self) -> FluxError {
        let failure = Failure {
            message: self.message,
            code: Some(self.code),
            retry_after: self.retry_after_ms.map(Duration::from_millis),
            source: None,
        };
        match self.class {
            ErrorClass::Retryable => FluxError::Retryable(failure),
            _ => FluxError::Permanent(failure),
        }
    }
}

/// El código de una clase cuando la aplicación no puso ninguno — §5.3: `PERMANENT`, y `RETRYABLE`.
const fn default_code(class: ErrorClass) -> &'static str {
    match class {
        ErrorClass::Retryable => "RETRYABLE",
        _ => "PERMANENT",
    }
}

// ─── Funciones puras: la petición, la respuesta, el error ────────────────────
//
// Sin reloj ni UUID propios: `id` y `time` vienen dados. Es lo que permite que el arnés cruzado
// compare los bytes con los de los otros SDKs, y que un test fije que son exactamente los de
// `11-request-reply.md` §3.5.

/// Todo lo que hace falta para construir la petición de una llamada — §3.1.
///
/// El desarrollador **no** rellena esto: lo rellena [`Bus::request`]. Se expone para el arnés de
/// conformidad y para quien construya un transporte propio.
#[derive(Debug, Clone)]
pub struct RpcRequestInput {
    /// `clientes.cliente.v1.calcular-descuento`: sin el prefijo `rpc.`.
    pub operation: String,
    /// Tenant de la llamada.
    pub tenant_id: String,
    /// El `aggregateId`: va al atributo `subject` **y** a `partitionkey`. **No** es el subject de NATS.
    pub aggregate_id: Option<String>,
    /// UUIDv7 dado: el mismo en todos los intentos de una llamada.
    pub id: String,
    /// `/<entorno>/<servicio>` de quien llama.
    pub source: String,
    /// El instante en que se **construye** la petición.
    pub time: DateTime<Utc>,
    /// Correlación heredada, o el propio `id` si la llamada no nace de otro.
    pub correlationid: String,
    /// El `id` del evento o la petición en curso, si lo hay.
    pub causationid: Option<String>,
    /// SemVer del servicio de quien llama.
    pub producerversion: String,
    /// Clasificación de la llamada.
    pub classification: DataClassification,
    /// URI del esquema de la petición.
    pub dataschema: String,
    /// W3C Trace Context.
    pub traceparent: Option<String>,
    /// W3C Trace Context.
    pub tracestate: Option<String>,
    /// Los parámetros. Un **objeto** JSON; viaja **sin reordenar**.
    pub data: Box<RawValue>,
}

/// De qué petición es una respuesta: lo que hereda de ella — §3.2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcRequestRef {
    /// El nombre de la operación.
    pub operation: String,
    /// El `id` de la petición: pasa a ser el `causationid` de la respuesta.
    pub id: String,
    /// Se hereda **sin modificar**.
    pub correlationid: String,
    /// Se hereda **sin modificar**.
    pub tenantid: String,
    /// El `subject` de la petición (el `aggregateId`), si lo traía.
    pub subject: Option<String>,
    /// El `partitionkey` de la petición, si lo traía. Ausente, se usa el `subject`.
    pub partitionkey: Option<String>,
}

impl RpcRequestRef {
    /// Lo que una respuesta hereda de una petición ya interpretada.
    fn of(operation: &str, request: &Event) -> Self {
        Self {
            operation: operation.to_string(),
            id: request.id.clone(),
            correlationid: request.correlationid.clone(),
            tenantid: request.tenantid.clone(),
            subject: request.aggregate_id.clone(),
            partitionkey: request.partitionkey.clone(),
        }
    }
}

/// Todo lo que hace falta para construir la respuesta de una operación — §3.2.
#[derive(Debug, Clone)]
pub struct RpcReplyInput {
    /// La petición a la que contesta.
    pub request: RpcRequestRef,
    /// UUIDv7 **nuevo**: nunca el de la petición.
    pub id: String,
    /// `/<entorno>/<servicio>` de quien responde.
    pub source: String,
    /// El instante en que se construye la respuesta.
    pub time: DateTime<Utc>,
    /// SemVer del servicio de quien responde.
    pub producerversion: String,
    /// La clasificación de la operación.
    pub classification: DataClassification,
    /// URI del esquema de la respuesta.
    pub dataschema: String,
    /// El contexto de quien responde, de la misma traza.
    pub traceparent: Option<String>,
    /// W3C Trace Context.
    pub tracestate: Option<String>,
    /// Lo que devuelve el handler: un **objeto** JSON.
    pub data: Box<RawValue>,
}

/// A quién contesta un error: a una petición legible, o a una que no se pudo atribuir.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyTo {
    /// Una petición legible: el error hereda de ella como una respuesta.
    Request(RpcRequestRef),
    /// Una petición **ilegible**: no hay `correlationid` ni `tenantid` que heredar, así que el error
    /// lleva `correlationid` = su propio `id` y `tenantid` = `system` (es un aviso de plataforma sobre
    /// un mensaje que no se pudo atribuir) y `causationid` = el `id` de la petición **solo si es una
    /// cadena válida** — §5.2.
    Unreadable {
        /// El `id` rescatado del cuerpo, si es una cadena válida.
        request_id: Option<String>,
    },
}

/// Todo lo que hace falta para construir un error `com.flux.rpc.error.v1` — §3.3.
#[derive(Debug, Clone)]
pub struct RpcErrorInput {
    /// A quién contesta.
    pub reply_to: ReplyTo,
    /// UUIDv7 **nuevo**.
    pub id: String,
    /// `/<entorno>/<servicio>` de quien responde.
    pub source: String,
    /// El instante en que se construye el error.
    pub time: DateTime<Utc>,
    /// SemVer del servicio de quien responde.
    pub producerversion: String,
    /// W3C Trace Context.
    pub traceparent: Option<String>,
    /// W3C Trace Context.
    pub tracestate: Option<String>,
    /// Qué falló.
    pub error: RpcErrorBody,
}

fn require_present(fields: &[(&str, &str)]) -> Result<(), FluxError> {
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

/// `data` debe ser un **objeto** JSON en la raíz — 01-envelope.md §4.
fn require_object(data: &RawValue, what: &str) -> Result<(), FluxError> {
    if data.get().trim_start().starts_with('{') {
        return Ok(());
    }
    Err(FluxError::InvalidRpcCall {
        reason: format!(
            "{what} debe ser un objeto JSON en la raíz (ni array ni escalar), para poder añadir \
             campos sin romper el contrato (01-envelope.md §4)"
        ),
    })
}

/// Construye la petición de una llamada, **sin red** — §3.1.
///
/// Valida la operación y `data` y devuelve el [`Event`] listo para serializar. Es la misma
/// construcción que usa [`Bus::request`], y los bytes son los de `11-request-reply.md` §3.5.
///
/// # Errores
///
/// Los de [`RpcOperation::parse`], [`FluxError::InvalidRpcCall`] si `data` no es un objeto y
/// [`FluxError::Envelope`] si falta algún atributo obligatorio.
pub fn build_rpc_request(input: RpcRequestInput) -> Result<Event, FluxError> {
    let op = RpcOperation::parse(&input.operation)?;
    require_object(&input.data, "`data` de la petición")?;
    require_present(&[
        ("id", &input.id),
        ("source", &input.source),
        ("dataschema", &input.dataschema),
        ("correlationid", &input.correlationid),
        ("tenantid", &input.tenant_id),
        ("producerversion", &input.producerversion),
    ])?;
    Ok(Event {
        specversion: SPEC_VERSION.to_string(),
        id: input.id,
        source: input.source,
        event_type: op.event_type(RpcSide::Request),
        time: format_time(input.time),
        datacontenttype: crate::protocol::DATA_CONTENT_TYPE.to_string(),
        dataschema: input.dataschema,
        // El `aggregateId` va al atributo `subject` de CloudEvents y a `partitionkey` —la misma
        // convención que en los eventos, aunque aquí no particione nada—. NO es el subject de NATS.
        partitionkey: input.aggregate_id.clone(),
        aggregate_id: input.aggregate_id,
        correlationid: input.correlationid,
        tenantid: input.tenant_id,
        producerversion: input.producerversion,
        dataclassification: input.classification,
        causationid: input.causationid,
        traceparent: input.traceparent,
        tracestate: input.tracestate,
        signkeyid: None,
        signature: None,
        dlqreason: None,
        dlqattempts: None,
        dlqconsumer: None,
        dlqerror: None,
        dlqtime: None,
        data: input.data,
    })
}

/// Lo que una respuesta o un error hereda de una petición — §3.2.
struct Inherited {
    correlationid: String,
    tenantid: String,
    causationid: Option<String>,
    subject: Option<String>,
    partitionkey: Option<String>,
}

impl Inherited {
    fn from_request(request: &RpcRequestRef) -> Self {
        Self {
            correlationid: request.correlationid.clone(),
            tenantid: request.tenantid.clone(),
            causationid: Some(request.id.clone()),
            subject: request.subject.clone(),
            // `partitionkey`: el de la petición; si no lo traía, la misma convención que en los
            // eventos —el `subject`—.
            partitionkey: request
                .partitionkey
                .clone()
                .or_else(|| request.subject.clone()),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn assemble_reply(
    event_type: String,
    dataschema: String,
    id: String,
    source: String,
    time: DateTime<Utc>,
    producerversion: String,
    classification: DataClassification,
    inherited: Inherited,
    traceparent: Option<String>,
    tracestate: Option<String>,
    data: Box<RawValue>,
) -> Event {
    Event {
        specversion: SPEC_VERSION.to_string(),
        id,
        source,
        event_type,
        time: format_time(time),
        datacontenttype: crate::protocol::DATA_CONTENT_TYPE.to_string(),
        dataschema,
        aggregate_id: inherited.subject,
        correlationid: inherited.correlationid,
        tenantid: inherited.tenantid,
        producerversion,
        dataclassification: classification,
        causationid: inherited.causationid,
        partitionkey: inherited.partitionkey,
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

/// Construye la respuesta de una operación, **sin red** — §3.2.
///
/// **Hereda** `correlationid`, `tenantid`, `subject` y `partitionkey` de la petición; su `causationid`
/// es el `id` de la petición y su `id` es **nuevo**.
///
/// # Errores
///
/// Los de [`RpcOperation::parse`], [`FluxError::InvalidRpcCall`] si `data` no es un objeto y
/// [`FluxError::Envelope`] si falta algún atributo obligatorio.
pub fn build_rpc_reply(input: RpcReplyInput) -> Result<Event, FluxError> {
    let op = RpcOperation::parse(&input.request.operation)?;
    require_object(&input.data, "`data` de la respuesta")?;
    require_present(&[
        ("id", &input.id),
        ("source", &input.source),
        ("dataschema", &input.dataschema),
        ("producerversion", &input.producerversion),
        ("request.id", &input.request.id),
        ("request.correlationid", &input.request.correlationid),
        ("request.tenantid", &input.request.tenantid),
    ])?;
    Ok(assemble_reply(
        op.event_type(RpcSide::Reply),
        input.dataschema,
        input.id,
        input.source,
        input.time,
        input.producerversion,
        input.classification,
        Inherited::from_request(&input.request),
        input.traceparent,
        input.tracestate,
        input.data,
    ))
}

/// Construye un error `com.flux.rpc.error.v1`, **sin red** — §3.3: el del handler que lanzó, o el que
/// fabrica el SDK de quien responde.
///
/// `type` y `dataschema` son fijos, la clasificación es **siempre `internal`** y `data` lleva las
/// claves en el orden `class`, `code`, `message`, `retryAfterMs`. Como una respuesta, hereda de la
/// petición; si no se pudo leer ([`ReplyTo::Unreadable`]), ver ahí.
///
/// # Errores
///
/// [`FluxError::InvalidRpcCall`] si `error` no cumple el esquema del error (`class` binaria, `code`
/// estable, `message` de 1 a 1000 caracteres, `retryAfterMs` solo con `retryable`) y
/// [`FluxError::Envelope`] si falta algún atributo obligatorio.
pub fn build_rpc_error(input: RpcErrorInput) -> Result<Event, FluxError> {
    input
        .error
        .check()
        .map_err(|reason| FluxError::InvalidRpcCall {
            reason: format!("el error que se iba a contestar no cumple su esquema: {reason}"),
        })?;
    require_present(&[
        ("id", &input.id),
        ("source", &input.source),
        ("producerversion", &input.producerversion),
    ])?;
    let inherited = match &input.reply_to {
        ReplyTo::Request(request) => {
            require_present(&[
                ("request.id", &request.id),
                ("request.correlationid", &request.correlationid),
                ("request.tenantid", &request.tenantid),
            ])?;
            Inherited::from_request(request)
        }
        ReplyTo::Unreadable { request_id } => Inherited {
            correlationid: input.id.clone(),
            tenantid: "system".to_string(),
            causationid: request_id.clone(),
            subject: None,
            partitionkey: None,
        },
    };
    let data = input.error.to_data()?;
    Ok(assemble_reply(
        RPC_ERROR_TYPE.to_string(),
        RPC_ERROR_DATASCHEMA.to_string(),
        input.id,
        input.source,
        input.time,
        input.producerversion,
        // El error es SIEMPRE `internal`: no lleva dato que clasificar.
        DataClassification::Internal,
        inherited,
        input.traceparent,
        input.tracestate,
        data,
    ))
}

// ─── Opciones ────────────────────────────────────────────────────────────────

/// Ajustes de una llamada concreta — §3.1, §4.2, §4.5.
///
/// **El desarrollador solo escribe la operación, `data` y, opcionalmente, el `aggregate_id`.** El `id`,
/// el `time`, el `type`, el `source`, el `correlationid`… los rellena el SDK.
#[derive(Debug, Clone, Default)]
pub struct RequestOptions {
    /// Cuánto espera **un intento** (§4.2). `None` es 5 s; un valor fuera de 1 ms a 25 s, o que no sea
    /// un número entero de milisegundos, es `INVALID_RPC_CALL`: no se recorta en silencio.
    pub timeout: Option<Duration>,
    /// La identidad del agregado sobre el que se pide (`cli-1`). Va al atributo `subject` de CloudEvents
    /// y a `partitionkey`; **no** es el subject de NATS.
    pub aggregate_id: Option<String>,
    /// Sobrescribe el tenant de la llamada. Por defecto, el del evento en curso, el de `connect()` y,
    /// si no hay, `system` —como en los eventos—.
    pub tenant_id: Option<String>,
    /// Sobrescribe la clasificación. Por defecto, la de la conexión.
    pub classification: Option<DataClassification>,
    /// Reintentos **opcionales** (§4.5), por defecto 0: `request` hace **un** intento. Solo ante
    /// `RPC_UNAVAILABLE` y `RPC_TIMEOUT`, nunca ante un error permanente, y cada reintento reutiliza el
    /// **mismo `id` y los mismos bytes** —`time` incluido—: un `id` nuevo por intento haría imposible
    /// reconocerlo.
    pub retries: u32,
    /// La pausa entre reintentos. Por defecto [`DEFAULT_RETRY_DELAY`].
    pub retry_delay: Option<Duration>,
    /// Contexto de correlación explícito. **Gana sobre el task-local**, para llamar desde un job
    /// diferido o un task hijo — ver [`crate::context`].
    pub event_context: Option<EventContext>,
}

impl RequestOptions {
    /// El timeout de un intento. Tiene que ser un entero de milisegundos entre 1 y 25 000.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// El timeout de un intento, en milisegundos (1 a 25 000).
    #[must_use]
    pub fn with_timeout_ms(self, ms: u64) -> Self {
        self.with_timeout(Duration::from_millis(ms))
    }

    /// La identidad del agregado.
    #[must_use]
    pub fn with_aggregate_id(mut self, id: impl Into<String>) -> Self {
        self.aggregate_id = Some(id.into());
        self
    }

    /// El tenant de esta llamada.
    #[must_use]
    pub fn with_tenant_id(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant.into());
        self
    }

    /// La clasificación de esta llamada.
    #[must_use]
    pub fn with_classification(mut self, classification: DataClassification) -> Self {
        self.classification = Some(classification);
        self
    }

    /// Reintentos ante `RPC_UNAVAILABLE` y `RPC_TIMEOUT` (hasta [`MAX_RETRIES`]), con el mismo `id`.
    ///
    /// **Solo es seguro si quien responde deduplica por el `id`** (§4.5): un timeout no dice que la
    /// operación no se ejecutó.
    #[must_use]
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// La pausa entre reintentos.
    #[must_use]
    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = Some(delay);
        self
    }

    /// Contexto de correlación explícito.
    #[must_use]
    pub fn with_context(mut self, ctx: EventContext) -> Self {
        self.event_context = Some(ctx);
        self
    }
}

/// Ajustes de un [`Bus::respond_with`].
#[derive(Debug, Clone, Default)]
pub struct RespondOptions {
    /// La clasificación de las respuestas de esta operación. Por defecto, la de la conexión. Un error
    /// es siempre `internal`.
    pub classification: Option<DataClassification>,
    /// Handlers concurrentes. Por defecto [`MAX_CONCURRENT_HANDLERS`] (256): core NATS no tiene acuse
    /// y un responder sobrecargado no puede decir «ahora no»; el exceso espera en la cola de la
    /// suscripción (§5.6).
    pub max_concurrent: Option<usize>,
}

impl RespondOptions {
    /// La clasificación de las respuestas.
    #[must_use]
    pub fn with_classification(mut self, classification: DataClassification) -> Self {
        self.classification = Some(classification);
        self
    }

    /// Handlers concurrentes (al menos 1).
    #[must_use]
    pub fn with_max_concurrent(mut self, n: usize) -> Self {
        self.max_concurrent = Some(n);
        self
    }
}

// ─── Lo que recibe el handler de quien responde ──────────────────────────────

/// La petición que recibe el handler de [`Bus::respond`]: el `data` de la petición, sin interpretar.
///
/// El sobre completo está en [`RpcContext::event`].
#[derive(Debug, Clone)]
pub struct RpcRequest {
    data: Box<RawValue>,
}

impl RpcRequest {
    /// El `data` tal cual llegó: un objeto JSON, con el orden de claves y la fidelidad numérica de la
    /// petición.
    #[must_use]
    pub fn raw(&self) -> &RawValue {
        &self.data
    }

    /// Decodifica los parámetros en el tipo de la aplicación.
    ///
    /// # Errores
    ///
    /// [`FluxError::Permanent`] con `DATA_SCHEMA_MISMATCH`, igual que [`Event::data_as`]: el sobre era
    /// correcto, es un desajuste de contrato entre quien llama y quien responde. El mensaje **no**
    /// cita valores de `data`.
    pub fn data_as<T: DeserializeOwned>(&self) -> Result<T, FluxError> {
        serde_json::from_str(self.data.get()).map_err(|e| {
            FluxError::permanent("los parámetros de la petición no encajan en el tipo esperado")
                .with_code("DATA_SCHEMA_MISMATCH")
                .with_source(e)
        })
    }
}

/// Los metadatos de **esta** petición, como segundo parámetro del handler — §5.1.
///
/// Van aparte de los parámetros por la misma razón que [`crate::Delivery`]: son datos de la llamada,
/// no del contrato. **Un handler de operación continúa la cadena**: lo que publique o llame desde él
/// hereda `correlationid` y `tenantid`, y su `causationid` es el `id` de la petición.
#[derive(Debug, Clone)]
pub struct RpcContext {
    /// El sobre de la petición.
    pub event: Event,
    /// El `id` de la petición: **la clave de la idempotencia**. Una orden puede llegar dos veces
    /// (un reintento tras un timeout, o dos copias) y el broker no deduplica en core.
    pub id: String,
    /// El `correlationid` de la petición.
    pub correlation_id: String,
    /// El tenant de la petición. El responder **no** filtra por el de `connect()` (§5.6): autorizar es
    /// cosa del handler.
    pub tenant_id: String,
    /// El `aggregateId` con que se llamó, si se dio.
    pub aggregate_id: Option<String>,
    /// El instante en que se construyó la petición.
    pub time: Option<DateTime<Utc>>,
    /// El nombre de la operación atendida.
    pub operation: String,
}

/// Atiende una operación.
///
/// Devuelve **un objeto** (cualquier `Serialize` que produzca un objeto JSON) o un error: `Err` lo
/// traduce el SDK a un error `com.flux.rpc.error.v1` (§5.3). Se implementa automáticamente para cualquier
/// `Fn(RpcRequest, RpcContext) -> Future<Output = Result<R, HandlerError>>`, así que en la práctica se
/// escribe como un closure.
///
/// - [`FluxError::retryable`] / [`FluxError::permanent`]: su clase, su código y **su mensaje**.
/// - Un error que el clasificador reconoce (un 503 de una dependencia, `ECONNRESET`): su clase y el
///   código del clasificador, con un mensaje **genérico**.
/// - Cualquier otro: **`permanent`**, código `UNKNOWN` y «error interno del servicio». El texto de la
///   excepción va al log de quien responde, con el `id` de la petición: puede llevar valores de `data`,
///   rutas o datos personales.
///
/// El handler **DEBE** ser idempotente por el `id` de la petición — 11-request-reply.md §4.5.
pub trait RpcHandler: Send + Sync + 'static {
    /// Invoca el handler.
    fn call(
        &self,
        request: RpcRequest,
        ctx: RpcContext,
    ) -> Pin<Box<dyn Future<Output = Result<Box<RawValue>, HandlerError>> + Send>>;
}

impl<F, Fut, R> RpcHandler for F
where
    F: Fn(RpcRequest, RpcContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, HandlerError>> + Send + 'static,
    R: Serialize,
{
    fn call(
        &self,
        request: RpcRequest,
        ctx: RpcContext,
    ) -> Pin<Box<dyn Future<Output = Result<Box<RawValue>, HandlerError>> + Send>> {
        let fut = self(request, ctx);
        Box::pin(async move {
            let value = fut.await?;
            // Sin comprobar que es un objeto: eso es de §5.3 («devuelve algo que no es un objeto»), y
            // lo contesta el SDK con `INVALID_SCHEMA`, no el handler con un fallo de serialización.
            serde_json::value::to_raw_value(&value).map_err(|e| Box::new(e) as HandlerError)
        })
    }
}

// ─── Quien llama: el transporte y el algoritmo de §4.1 ───────────────────────

/// Lo que llega a quien llama por su inbox: el cuerpo y, si lo hay, el estado de NATS.
#[derive(Debug, Clone)]
pub(crate) struct Incoming {
    pub(crate) payload: Vec<u8>,
    /// El estado de la cabecera (`503` es `no responders`, ya traducido antes de llegar aquí).
    pub(crate) status: Option<u16>,
}

/// El transporte del algoritmo de quien llama: **un intento**, ya traducido a los errores de flux.
///
/// Es un trait **solo para poder probar el algoritmo sin broker**: con una implementación en memoria
/// se ejercitan los reintentos —mismo `id` y mismos bytes—, cuáles se reintentan y cuáles no. En
/// producción lo implementa [`BusIo`], contra NATS.
pub(crate) trait RequestIo: Send + Sync {
    /// Publica por **core NATS** en `subject` con un subject de respuesta propio y espera, lo
    /// primero que ocurra: `no responders` ([`FluxError::RpcUnavailable`]), una denegación de la ACL
    /// ([`FluxError::PermissionDenied`]), una respuesta o el timeout ([`FluxError::RpcTimeout`]).
    fn send(
        &self,
        payload: &[u8],
        timeout: Duration,
    ) -> impl Future<Output = Result<Incoming, FluxError>> + Send;
}

/// El estado del acuse de JetStream: `{"stream":"…","seq":n}` — A-18, A-67.
fn looks_like_stream_ack(payload: &[u8]) -> bool {
    serde_json::from_slice::<Value>(payload).is_ok_and(|v| {
        v.as_object().is_some_and(|o| {
            o.get("stream").is_some_and(Value::is_string)
                && o.get("seq").is_some_and(Value::is_number)
        })
    })
}

/// Recorta un valor que viene de fuera antes de citarlo en un mensaje: un `type` hostil no debe poder
/// llenar un log.
fn shown(text: &str) -> String {
    const MAX: usize = 80;
    if text.chars().count() <= MAX {
        return text.to_string();
    }
    let mut cut: String = text.chars().take(MAX).collect();
    cut.push('…');
    cut
}

/// Lo que se espera de la respuesta a **esta** petición.
pub(crate) struct Expectation<'a> {
    pub(crate) operation: &'a RpcOperation,
    pub(crate) request: &'a RpcRequestRef,
}

/// Una respuesta que pasó las comprobaciones: o la respuesta de la operación, o un error.
#[derive(Debug)]
pub(crate) enum Checked {
    /// La respuesta de la operación.
    Reply(Event),
    /// Un `com.flux.rpc.error.v1` con su `data` ya interpretado, y el sobre del que salió.
    Failure(RpcErrorBody, Event),
}

/// ¿Es el error que contesta quien responde a una petición que **no pudo leer** (§5.2)? Sin sobre no
/// hay `correlationid` ni `tenantid` que heredar: el error lleva `correlationid` igual a **su propio
/// `id`** y `tenantid: system`. Es la única respuesta a la que quien llama no le exige heredar los dos
/// (§4.3, comprobación 4). La forma es estrecha a propósito —un `tenantid: system` cualquiera, o un
/// `correlationid` cualquiera, no basta—, y la comprobación 3 (`causationid` = el `id` de la
/// petición) sigue en pie y es la que la atribuye.
pub(crate) fn is_error_of_unreadable_request(event: &Event, is_error: bool) -> bool {
    is_error && event.tenantid == "system" && event.correlationid == event.id
}

/// Las comprobaciones de §4.3: **si falla cualquiera, `RPC_INVALID_REPLY`** (PERMANENT).
///
/// 1. Es un **CloudEvent válido**.
/// 2. Su `type` es el de la respuesta de esa operación o `com.flux.rpc.error.v1`.
/// 3. Su `causationid` es el `id` de la petición.
/// 4. Su `correlationid` y su `tenantid` son los de la petición. **Excepción única**: el error de una
///    petición ilegible ([`is_error_of_unreadable_request`]) no hereda ninguno de los dos.
/// 5. Si es un error, su `data` cumple el esquema del error.
///
/// Una respuesta que no es de esta petición es **un incidente** —un productor roto, un stream mal
/// puesto, algo hostil— y no un fallo que esperar.
pub(crate) fn check_reply(
    expect: &Expectation<'_>,
    incoming: &Incoming,
) -> Result<Checked, FluxError> {
    let op_name = expect.operation.name_with_path();
    let invalid = |reason: String| FluxError::RpcInvalidReply {
        operation: op_name.clone(),
        reason,
        stream_ack: looks_like_stream_ack(&incoming.payload),
    };

    if let Some(status) = incoming.status {
        return Err(invalid(format!(
            "llegó un mensaje de estado {status} y no un CloudEvent"
        )));
    }
    // 1. Un CloudEvent válido: parsea, con las cuatro extensiones y los tipos exactos.
    let event = parse_event(&incoming.payload).map_err(|e| {
        invalid(format!(
            "no es un CloudEvent válido ({})",
            e.code().unwrap_or("ERROR")
        ))
    })?;
    // 2. El `type`.
    let reply_type = expect.operation.event_type(RpcSide::Reply);
    let is_error = event.event_type == RPC_ERROR_TYPE;
    if !is_error && event.event_type != reply_type {
        return Err(invalid(format!(
            "su `type` es `{}` y debía ser `{reply_type}` o `{RPC_ERROR_TYPE}`",
            shown(&event.event_type)
        )));
    }
    // 3. `causationid` = el `id` de la petición.
    if event.causationid.as_deref() != Some(expect.request.id.as_str()) {
        return Err(invalid(
            "su `causationid` no es el `id` de la petición (la respuesta no es a esta llamada)"
                .into(),
        ));
    }
    // 4. `correlationid` y `tenantid` heredados, sin modificar. EXCEPCIÓN ÚNICA (§4.3.4): el error de
    // una petición que quien responde no pudo leer (§5.2) no tiene nada que heredar, y a quien llama le
    // basta la comprobación 3 para atribuirlo y ver el código del sobre (`MALFORMED_JSON`,
    // `INVALID_RPC_CALL`…) en vez de `RPC_INVALID_REPLY`.
    if !is_error_of_unreadable_request(&event, is_error) {
        if event.correlationid != expect.request.correlationid {
            return Err(invalid(
                "su `correlationid` no es el de la petición: una respuesta lo hereda sin modificar"
                    .into(),
            ));
        }
        if event.tenantid != expect.request.tenantid {
            return Err(invalid(
                "su `tenantid` no es el de la petición: una respuesta lo hereda sin modificar"
                    .into(),
            ));
        }
    }
    // 5. Un error cumple el esquema del error.
    if is_error {
        let body = RpcErrorBody::from_data(event.data.get())
            .map_err(|reason| invalid(format!("es un error que no cumple su esquema: {reason}")))?;
        return Ok(Checked::Failure(body, event));
    }
    Ok(Checked::Reply(event))
}

/// El algoritmo de reintentos de §4.5: **un intento**, y reintentos solo ante `RPC_UNAVAILABLE` y
/// `RPC_TIMEOUT`, con los **mismos bytes** —el mismo `id` y el mismo `time`— en todos.
///
/// Nunca ante un error permanente, ni ante una denegación, ni ante una respuesta: reintentar lo que no
/// se arregla esperando es el modo de fallo que la taxonomía existe para impedir.
pub(crate) async fn send_with_retries<I: RequestIo>(
    io: &I,
    payload: &[u8],
    timeout: Duration,
    retries: u32,
    delay: Duration,
) -> Result<Incoming, FluxError> {
    let mut attempt: u32 = 0;
    loop {
        match io.send(payload, timeout).await {
            Err(e @ (FluxError::RpcUnavailable { .. } | FluxError::RpcTimeout { .. }))
                if attempt < retries =>
            {
                let _ = e;
                attempt += 1;
                // Multiplicada por el intento: la pausa crece sin volverse un backoff que la
                // aplicación no pidió.
                tokio::time::sleep(delay.saturating_mul(attempt)).await;
            }
            other => return other,
        }
    }
}

/// El outcome de una llamada que terminó con este error — 08-observability.md §2.5.
///
/// `None` para los errores de programación (`INVALID_RPC_CALL`, `RESERVED_DOMAIN`) y de
/// configuración: no se cuentan, su valor sería una serie por cada error de tipeo.
fn client_outcome_of(error: &FluxError) -> Option<RpcClientOutcome> {
    Some(match error {
        FluxError::RpcUnavailable { .. } => RpcClientOutcome::Unavailable,
        FluxError::RpcTimeout { .. } => RpcClientOutcome::Timeout,
        FluxError::RpcInvalidReply { .. } => RpcClientOutcome::InvalidReply,
        FluxError::SchemaValidation(_) | FluxError::SchemaNotFound(_) => {
            RpcClientOutcome::InvalidSchema
        }
        FluxError::Retryable(_) | FluxError::Transport { .. } => RpcClientOutcome::Retryable,
        FluxError::InvalidRpcCall { .. }
        | FluxError::ReservedDomain { .. }
        | FluxError::Config(_)
        | FluxError::Envelope(_) => return None,
        // PERMISSION_DENIED, STALE_REQUEST, RPC_MESSAGE_TOO_LARGE, un fallo de firma y un error
        // `permanent` del que responde.
        _ => RpcClientOutcome::Permanent,
    })
}

/// Una llamada ya construida: **los mismos `id` y bytes en todos los intentos** — §4.5.
struct Prepared {
    op: RpcOperation,
    subject: String,
    request: RpcRequestRef,
    payload: Vec<u8>,
    timeout: Duration,
    retries: u32,
    retry_delay: Duration,
}

/// [`RequestIo`] contra un bus real.
struct BusIo<'a> {
    bus: &'a Bus,
    prepared: &'a Prepared,
}

impl RequestIo for BusIo<'_> {
    async fn send(&self, payload: &[u8], timeout: Duration) -> Result<Incoming, FluxError> {
        let p = self.prepared;
        let request = async_nats::Request::new()
            .payload(payload.to_vec().into())
            .timeout(Some(timeout));
        // **Carrera con la denegación.** Una petición denegada por la ACL NO falla rápido con
        // `async-nats`: el servidor descarta el mensaje, el `-ERR 'Permissions Violation'` llega
        // solo por el evento de la conexión y la petición espera su timeout entero (A-72). Sin esta
        // carrera se vería `RPC_TIMEOUT` (RETRYABLE) y un handler que llama reintentaría 51 minutos
        // algo que nunca va a funcionar (11-request-reply.md §7.1).
        let sent = self
            .bus
            .guarded(
                std::slice::from_ref(&p.subject),
                self.bus
                    .inner
                    .client
                    .send_request(p.subject.clone(), request),
            )
            .await;
        let op_name = p.op.name_with_path();
        match sent {
            Err(denial) => Err(permission_denied(
                format!(
                    "llamar a la operación `{op_name}` (publicar en `{}`: quien llama necesita \
                     permiso de publicación sobre el subject EXACTO de cada operación concedida, y \
                     de suscripción a su inbox `_INBOX.<servicio>.>`)",
                    p.subject
                ),
                &denial,
            )),
            Ok(Ok(message)) => Ok(Incoming {
                payload: message.payload.to_vec(),
                status: message.status.map(|s| s.as_u16()),
            }),
            Ok(Err(e)) => Err(match e.kind() {
                // El `no responders` del broker, en ~1 ms (A-56). Lo reconoce la librería porque la
                // conexión lo pidió en CONNECT (`headers` y `no_responders`, A-57).
                async_nats::RequestErrorKind::NoResponders => {
                    FluxError::RpcUnavailable { operation: op_name }
                }
                async_nats::RequestErrorKind::TimedOut => FluxError::RpcTimeout {
                    operation: op_name,
                    timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
                },
                // Ya se comprobó antes de abrir nada; si la librería discrepa (cabeceras), que se vea.
                async_nats::RequestErrorKind::MaxPayloadExceeded => FluxError::RpcMessageTooLarge {
                    what: "la petición".to_string(),
                    bytes: payload.len(),
                    limit: self.bus.max_message_bytes(),
                },
                _ => transport(format!("fallo al llamar a la operación `{op_name}`"), e),
            }),
        }
    }
}

impl Bus {
    /// El mayor mensaje que se publica: el **mínimo** entre 1 MiB y el `max_payload` que anuncia el
    /// broker en su `INFO` — 11-request-reply.md §4.4. Las cabeceras cuentan (A-65); como no usamos
    /// ninguna, el límite efectivo es el del cuerpo.
    fn max_message_bytes(&self) -> usize {
        match self.inner.client.try_server_info() {
            Some(info) if info.max_payload > 0 => info.max_payload.min(MAX_MESSAGE_BYTES),
            _ => MAX_MESSAGE_BYTES,
        }
    }

    /// El `dataschema` de un lado de una operación: el índice `rpc` del bundle (L3) y, por último, la
    /// URL base. **Nunca por red** — §3.1.
    fn rpc_schema_uri(&self, op: &RpcOperation, side: RpcSide) -> Result<String, FluxError> {
        let opts = &self.inner.opts;
        #[cfg(feature = "validation")]
        if let Some(uris) = opts
            .validation
            .bundle
            .as_ref()
            .and_then(|b| b.rpc_uris_for(&op.name_with_path()))
        {
            return Ok(match side {
                RpcSide::Request => uris.request.clone(),
                RpcSide::Reply => uris.reply.clone(),
            });
        }
        match &opts.schema_base_url {
            Some(base) => Ok(op.default_dataschema(base, side)),
            None => Err(FluxError::Config(format!(
                "no hay dataschema para la operación `{}`. Pasa schema_base_url, o un bundle con el \
                 índice `rpc` en ConnectOptions::with_validation (11-request-reply.md §11.1)",
                op.name_with_path()
            ))),
        }
    }

    /// Valida `data` contra el esquema de un lado de la operación (L3). `Ok(true)` si cumple o si L3
    /// está apagado; `Ok(false)` si no cumple y el modo es `warn`.
    #[cfg_attr(
        not(feature = "validation"),
        allow(clippy::unused_self, clippy::unnecessary_wraps)
    )]
    fn validate_rpc_data(
        &self,
        op: &RpcOperation,
        side: RpcSide,
        data: &RawValue,
    ) -> Result<bool, FluxError> {
        #[cfg(feature = "validation")]
        if let Some(validator) = &self.inner.validator {
            let uri = self.rpc_schema_uri(op, side)?;
            return validator.validate_rpc(&op.name_with_path(), &uri, data);
        }
        #[cfg(not(feature = "validation"))]
        let _ = (op, side, data);
        Ok(true)
    }

    /// Todo lo de §4.1 paso 0: validar y construir **sin red**, en este orden —el primero que falla
    /// gana—: `RESERVED_DOMAIN`, `INVALID_RPC_CALL` (operación, timeout, `data` objeto),
    /// `INVALID_DATACLASSIFICATION` (aquí no puede darse: la clasificación es un enum), L3 (todos los
    /// errores), el sobre **una sola vez** (`id`, `time`, bytes) y el tamaño.
    fn prepare_request<T: Serialize + ?Sized>(
        &self,
        operation: &str,
        data: &T,
        opts: &RequestOptions,
    ) -> Result<Prepared, FluxError> {
        let op = RpcOperation::parse(operation)?;
        let timeout = resolve_timeout(opts.timeout)?;
        if opts.retries > MAX_RETRIES {
            return Err(FluxError::InvalidRpcCall {
                reason: format!(
                    "`retries` de {} es demasiado: el máximo son {MAX_RETRIES}. Cada intento gasta un \
                     timeout entero y solo es seguro si quien responde deduplica por el `id`",
                    opts.retries
                ),
            });
        }
        let raw = serde_json::value::to_raw_value(data).map_err(|e| FluxError::InvalidRpcCall {
            reason: format!("`data` no es serializable a JSON: {e}"),
        })?;
        require_object(&raw, "`data` de la petición")?;

        // L3: ANTES de construir y de firmar. Un payload que viola su contrato debe fallar aquí, en el
        // servicio que lo generó (00-protocol.md §5).
        let op_name = op.name_with_path();
        if !self.validate_rpc_data(&op, RpcSide::Request, &raw)? {
            self.log(
                LogLevel::Warn,
                &format!("la petición a `{op_name}` no cumple su esquema (validación en modo `warn`): se envía igual"),
            );
        }

        let id = Uuid::now_v7().to_string();
        let inherited = opts.event_context.clone().or_else(context::current);
        let tenant_id = opts
            .tenant_id
            .clone()
            .or_else(|| {
                inherited
                    .as_ref()
                    .map(|c| c.tenant_id.clone())
                    .filter(|t| !t.is_empty())
            })
            .or_else(|| self.inner.opts.tenant_id.clone())
            .unwrap_or_else(|| "system".to_string());
        let classification = opts
            .classification
            .or(self.inner.opts.classification)
            .unwrap_or(DataClassification::Internal);
        // Si la llamada no nace de otra, `correlationid` es su propio `id` y no hay `causationid`.
        let (correlationid, causationid) = match inherited.as_ref() {
            Some(c) if !c.correlation_id.is_empty() => {
                (c.correlation_id.clone(), Some(c.causation_id.clone()))
            }
            _ => (id.clone(), None),
        };
        let (traceparent, tracestate) = match inherited.as_ref() {
            Some(c) if c.traceparent.is_some() => (c.traceparent.clone(), c.tracestate.clone()),
            _ => (self.inner.opts.traceparent.as_ref().and_then(|f| f()), None),
        };

        let event = build_rpc_request(RpcRequestInput {
            operation: op_name,
            tenant_id,
            aggregate_id: opts.aggregate_id.clone(),
            id,
            source: self.inner.source.clone(),
            time: Utc::now(),
            correlationid,
            causationid,
            producerversion: self.inner.opts.version.clone(),
            classification,
            dataschema: self.rpc_schema_uri(&op, RpcSide::Request)?,
            traceparent,
            tracestate,
            data: raw,
        })?;
        let request = RpcRequestRef::of(&op.name_with_path(), &event);

        // Firmar es LO ÚLTIMO antes de serializar: la firma cubre el sobre completo (07-signing.md §5).
        #[cfg(feature = "signing")]
        let event = match &self.inner.signer {
            Some(signer) => signer.sign(event)?,
            None => event,
        };

        let payload = serde_json::to_vec(&event).map_err(|e| {
            FluxError::Envelope(format!("la petición no es serializable a JSON: {e}"))
        })?;
        // El tamaño ANTES de abrir nada: `async-nats` lo rechaza al instante (`MaxPayloadExceeded`) y
        // un cliente en bruto que lo enviara perdería la conexión (A-65).
        let limit = self.max_message_bytes();
        if payload.len() > limit {
            return Err(FluxError::RpcMessageTooLarge {
                what: "la petición".to_string(),
                bytes: payload.len(),
                limit,
            });
        }
        Ok(Prepared {
            subject: op.subject(),
            op,
            request,
            payload,
            timeout,
            retries: opts.retries,
            retry_delay: opts.retry_delay.unwrap_or(DEFAULT_RETRY_DELAY),
        })
    }

    /// Verifica la firma de lo que llegó (§4.1 paso 3, con la firma en `require`).
    ///
    /// Un fallo de firma es para quien llama **PERMANENT** —con el código de la firma—, no POISON:
    /// POISON es la calificación de quien *consume* un mensaje ilegible, y aquí lo que falla es la
    /// respuesta de una llamada, que reintentar no arregla (08-observability.md §2.5 lo cuenta como
    /// `permanent`).
    #[cfg_attr(
        not(feature = "signing"),
        allow(clippy::unused_self, clippy::unnecessary_wraps)
    )]
    fn verify_reply(&self, event: &Event) -> Result<(), FluxError> {
        #[cfg(feature = "signing")]
        if let Some(verifier) = &self.inner.verifier {
            if let Err(e) = verifier.check(event) {
                let code = e.code().unwrap_or("INVALID_SIGNATURE").to_string();
                return Err(FluxError::permanent(e.to_string()).with_code(code));
            }
        }
        #[cfg(not(feature = "signing"))]
        let _ = event;
        Ok(())
    }

    /// Llama a una operación y espera **un** `data` de respuesta — §4.
    ///
    /// Equivale a [`Bus::request_with`] con las opciones por defecto: timeout de 5 s, tenant del evento
    /// en curso o de `connect()`, un solo intento.
    ///
    /// # Errores
    ///
    /// Ver [`Bus::request_with`].
    pub async fn request<T: Serialize + ?Sized>(
        &self,
        operation: &str,
        data: &T,
    ) -> Result<Event, FluxError> {
        self.request_with(operation, data, RequestOptions::default())
            .await
    }

    /// Llama a una operación con ajustes para esta llamada. Devuelve el **sobre de la respuesta**
    /// (`data_as` decodifica su `data`; `id`, `time` y `correlationid` están ahí) — §4.
    ///
    /// **El desarrollador solo escribe la operación, `data` y, opcionalmente, el `aggregate_id`.** El
    /// SDK construye el sobre una sola vez (`id`, `time`, bytes), publica por **core NATS** en
    /// `rpc.<operación>` con un subject de respuesta propio —el inbox privado— y espera, lo primero
    /// que ocurra: `no responders`, una denegación de la ACL, una respuesta o el timeout.
    ///
    /// # Errores
    ///
    /// Antes de tocar la red, y en este orden: [`FluxError::ReservedDomain`], [`FluxError::InvalidRpcCall`]
    /// (operación, `timeout`, `data` que no es un objeto), [`FluxError::SchemaValidation`] (L3: todos los
    /// errores, ningún valor) y [`FluxError::RpcMessageTooLarge`] (el menor entre 1 MiB y el
    /// `max_payload` del broker).
    ///
    /// Después: [`FluxError::RpcUnavailable`] (`no responders`: nadie escucha, en ~1 ms, **RETRYABLE**),
    /// [`FluxError::PermissionDenied`] (la ACL lo deniega: en milisegundos, no por timeout),
    /// [`FluxError::RpcTimeout`] (**RETRYABLE y ambiguo**: la operación PUEDE haberse ejecutado),
    /// [`FluxError::RpcInvalidReply`] (lo que llegó no es la respuesta a esta petición), los de la firma
    /// (`MISSING_SIGNATURE`, `INVALID_SIGNATURE`, `UNKNOWN_SIGNING_KEY`) y, si quien responde falló, un
    /// [`FluxError::Retryable`] o un [`FluxError::Permanent`] con **su** `code` y `message`: un handler de
    /// eventos que llama a una operación hereda la clasificación sin hacer nada.
    pub async fn request_with<T: Serialize + ?Sized>(
        &self,
        operation: &str,
        data: &T,
        opts: RequestOptions,
    ) -> Result<Event, FluxError> {
        let started = Instant::now();
        let prepared = match self.prepare_request(operation, data, &opts) {
            Ok(p) => p,
            Err(e) => {
                // Los errores de programación no se cuentan; `INVALID_SCHEMA` sí (§2.5).
                if let (Some(outcome), Ok(op)) =
                    (client_outcome_of(&e), RpcOperation::parse(operation))
                {
                    self.inner
                        .metrics
                        .rpc_client_request(&op.name_with_path(), outcome);
                }
                return Err(e);
            }
        };

        let (result, warned) = self.call_prepared(&prepared).await;
        let name = prepared.op.name_with_path();
        let outcome = match &result {
            Ok(_) if warned => Some(RpcClientOutcome::InvalidSchema),
            Ok(_) => Some(RpcClientOutcome::Ok),
            Err(e) => client_outcome_of(e),
        };
        if let Some(outcome) = outcome {
            self.inner.metrics.rpc_client_request(&name, outcome);
        }
        self.inner
            .metrics
            .rpc_duration(&name, RpcRole::Client, started.elapsed().as_secs_f64());
        result
    }

    /// Los pasos 1 a 5 de §4.1. El `bool` es «la respuesta no cumplía su esquema y el modo es `warn`».
    async fn call_prepared(&self, prepared: &Prepared) -> (Result<Event, FluxError>, bool) {
        let io = BusIo {
            bus: self,
            prepared,
        };
        let incoming = match send_with_retries(
            &io,
            &prepared.payload,
            prepared.timeout,
            prepared.retries,
            prepared.retry_delay,
        )
        .await
        {
            Ok(i) => i,
            Err(e) => return (Err(e), false),
        };
        let expect = Expectation {
            operation: &prepared.op,
            request: &prepared.request,
        };
        // 3. ¿Es la respuesta a ESTA petición? Si no, RPC_INVALID_REPLY; y con la firma en `require`,
        // verificarla.
        let checked = match check_reply(&expect, &incoming) {
            Ok(c) => c,
            Err(e) => return (Err(e), false),
        };
        match checked {
            // 4. Un error: RetryableError o PermanentError según su `class`. Se verifica antes la firma:
            // un error sin firma de quien debía firmar no es de fiar.
            Checked::Failure(body, event) => match self.verify_reply(&event) {
                Ok(()) => (Err(body.into_flux_error()), false),
                Err(e) => (Err(e), false),
            },
            // 5. Una respuesta: verificar, validar `data` contra el esquema de la respuesta (L3) y
            // devolverla.
            Checked::Reply(event) => {
                if let Err(e) = self.verify_reply(&event) {
                    return (Err(e), false);
                }
                match self.validate_rpc_data(&prepared.op, RpcSide::Reply, &event.data) {
                    Ok(true) => (Ok(event), false),
                    Ok(false) => {
                        self.log(
                            LogLevel::Warn,
                            &format!(
                                "la respuesta de `{}` no cumple su esquema (validación en modo `warn`)",
                                prepared.op.name_with_path()
                            ),
                        );
                        (Ok(event), true)
                    }
                    Err(e) => (Err(e), false),
                }
            }
        }
    }
}

// ─── Quien responde: el algoritmo de §5.1 ────────────────────────────────────

/// Lo que cambia de una operación atendida a otra y no del bus: se calcula **una vez**, al
/// arrancar `respond()`.
#[derive(Debug, Clone)]
pub(crate) struct ServeConfig {
    pub(crate) op: RpcOperation,
    /// El `type` de la petición de esta operación: lo que se espera recibir.
    pub(crate) request_type: String,
    /// El `dataschema` de la respuesta (índice `rpc` del bundle, o derivado de la URL base).
    pub(crate) reply_dataschema: String,
    /// La clasificación de las respuestas.
    pub(crate) classification: DataClassification,
}

/// Lo que el algoritmo de quien responde necesita del exterior. Es un trait **solo para poder probar
/// el algoritmo sin broker**, con un reloj y unos `id` fijos: en producción lo implementa [`BusEnv`].
pub(crate) trait ServeEnv: Send + Sync {
    /// `/<entorno>/<servicio>` de quien responde.
    fn source(&self) -> &str;
    /// SemVer del servicio de quien responde.
    fn producer_version(&self) -> &str;
    /// La hora de ahora.
    fn now(&self) -> DateTime<Utc>;
    /// Un UUIDv7 nuevo.
    fn new_id(&self) -> String;
    /// El `traceparent` del contexto activo de quien responde, si hay uno.
    fn traceparent(&self) -> Option<String>;
    /// Clasifica un error del handler.
    fn classify(&self, error: &(dyn StdError + 'static)) -> Classification;
    /// ¿Está la firma en `require`? Solo entonces rige la ventana de 300 s (§7.4).
    fn require_signature(&self) -> bool;
    /// Verifica la firma de la petición. Solo falla con la firma en `require`.
    fn verify(&self, event: &Event) -> Result<(), FluxError>;
    /// Firma lo que sale, si quien responde tiene la firma activa.
    fn sign(&self, event: Event) -> Result<Event, FluxError>;
    /// Valida `data` contra el esquema del lado (L3). `Ok(false)`: no cumple y el modo es `warn`.
    fn validate(
        &self,
        op: &RpcOperation,
        side: RpcSide,
        data: &RawValue,
    ) -> Result<bool, FluxError>;
    /// El mayor mensaje que se publica: el mínimo entre 1 MiB y el `max_payload` del broker.
    fn max_message_bytes(&self) -> usize;
    /// Registra algo en el log del servicio.
    fn log(&self, level: LogLevel, message: &str);
}

/// Lo que produce atender una petición.
#[derive(Debug)]
pub(crate) struct Served {
    /// El `outcome` de `flux_rpc_server_requests_total`.
    pub(crate) outcome: RpcServerOutcome,
    /// Los bytes de la respuesta o del error; `None` si no hay a quién contestar.
    pub(crate) reply: Option<Vec<u8>>,
}

/// El `id` de una petición ilegible, **solo si es una cadena válida**: no vacía, de 128 caracteres como
/// máximo y sin caracteres de control. Va al `causationid` del error (§5.2).
fn rescue_request_id(payload: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(payload).ok()?;
    let id = value.as_object()?.get("id")?.as_str()?;
    let valid = !id.is_empty() && id.chars().count() <= 128 && !id.chars().any(char::is_control);
    valid.then(|| id.to_string())
}

/// La respuesta de error: construye, firma y serializa. Si algo de eso falla (no debería: el cuerpo ya
/// está normalizado) se registra y no se contesta: callar es peor, pero publicar basura también.
fn error_reply(
    env: &dyn ServeEnv,
    reply_to: ReplyTo,
    request_trace: (Option<String>, Option<String>),
    body: RpcErrorBody,
    outcome: RpcServerOutcome,
) -> Served {
    let own_trace = env.traceparent();
    let (traceparent, tracestate) = if own_trace.is_some() {
        (own_trace, None)
    } else {
        request_trace
    };
    let built = build_rpc_error(RpcErrorInput {
        reply_to,
        id: env.new_id(),
        source: env.source().to_string(),
        time: env.now(),
        producerversion: env.producer_version().to_string(),
        traceparent,
        tracestate,
        error: body.normalized(),
    })
    .and_then(|e| env.sign(e))
    .and_then(|e| {
        serde_json::to_vec(&e).map_err(|err| {
            FluxError::Envelope(format!("el error no es serializable a JSON: {err}"))
        })
    });
    match built {
        Ok(bytes) => Served {
            outcome,
            reply: Some(bytes),
        },
        Err(e) => {
            env.log(
                LogLevel::Error,
                &format!("no se pudo construir el error de respuesta: {e}"),
            );
            Served {
                outcome,
                reply: None,
            }
        }
    }
}

/// El cuerpo de un error de esquema: cita **rutas y reglas**, nunca valores, y dice **de quién** es el
/// fallo —«la petición no cumple…» o «la respuesta no cumple…»— porque quien llama recibe el mismo
/// `INVALID_SCHEMA` en los dos casos y debe saber a quién arreglar (§5.2).
fn schema_body(what: &str, error: &FluxError) -> RpcErrorBody {
    let message = match error {
        FluxError::SchemaValidation(e) => {
            format!("{what} no cumple {}: {}", e.dataschema, e.errors.join(", "))
        }
        FluxError::SchemaNotFound(e) => format!(
            "no se pudo validar {what}: el bundle de quien responde no tiene el esquema {}",
            e.dataschema
        ),
        other => format!("no se pudo validar {what}: {other}"),
    };
    RpcErrorBody {
        class: ErrorClass::Permanent,
        code: "INVALID_SCHEMA".to_string(),
        message,
        retry_after_ms: None,
    }
}

/// Traduce el error del handler al error que viaja — §5.3.
///
/// | El handler… | Se responde |
/// |---|---|
/// | lanza `RetryableError` | `retryable`, su código (o `RETRYABLE`), **su** mensaje, y `retryAfterMs` si lo da |
/// | lanza `PermanentError` | `permanent`, su código (o `PERMANENT`), **su** mensaje |
/// | un error que el clasificador reconoce | su clase y el código del clasificador; mensaje **genérico** |
/// | un error **desconocido** | **`permanent`**, `UNKNOWN`, «error interno del servicio» |
///
/// Un error que la aplicación no lanzó a propósito **no** reenvía el texto de la excepción: puede llevar
/// valores de `data`, rutas del servidor o datos personales. Va al log de quien responde, con el `id` de
/// la petición para encontrarlo.
fn wire_error(
    env: &dyn ServeEnv,
    operation: &str,
    request_id: &str,
    error: &(dyn StdError + 'static),
) -> (RpcErrorBody, RpcServerOutcome) {
    let generic = |class: ErrorClass| match class {
        ErrorClass::Retryable => "dependencia no disponible",
        _ => "la operación no se pudo completar",
    };
    let to_ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);

    // 1. Un error tipado de flux gana: la aplicación sabe más que el SDK.
    if let Some(flux) = as_classified(error) {
        let body = match flux {
            FluxError::Retryable(f) => RpcErrorBody {
                class: ErrorClass::Retryable,
                code: f
                    .code
                    .clone()
                    .unwrap_or_else(|| default_code(ErrorClass::Retryable).into()),
                message: f.message.clone(),
                retry_after_ms: f.retry_after.map(to_ms),
            },
            FluxError::Permanent(f) | FluxError::Poison(f) => RpcErrorBody {
                class: ErrorClass::Permanent,
                code: f
                    .code
                    .clone()
                    .unwrap_or_else(|| default_code(ErrorClass::Permanent).into()),
                message: f.message.clone(),
                retry_after_ms: None,
            },
            // Un error del propio SDK (`RPC_UNAVAILABLE` de una operación que este handler llamó,
            // `PERMISSION_DENIED`…): su clase y su código, no su texto, que describe la topología.
            other => {
                let class = match other.class() {
                    Some(ErrorClass::Retryable) => ErrorClass::Retryable,
                    _ => ErrorClass::Permanent,
                };
                env.log(
                    LogLevel::Error,
                    &format!(
                        "el handler de `{operation}` falló en la petición {request_id}: {}",
                        describe(other)
                    ),
                );
                RpcErrorBody {
                    class,
                    code: other.code().unwrap_or("UNKNOWN").to_string(),
                    message: generic(class).to_string(),
                    retry_after_ms: None,
                }
            }
        };
        let outcome = outcome_of_class(body.class);
        return (body, outcome);
    }

    // 2. El clasificador, y si no reconoce nada, lo desconocido.
    env.log(
        LogLevel::Error,
        &format!(
            "el handler de `{operation}` falló en la petición {request_id}: {}",
            describe(error)
        ),
    );
    let c = env.classify(error);
    let body = if c.code == "UNKNOWN" {
        // Lo desconocido es `permanent` y no «retryable acotado»: el presupuesto de 04-errors.md §2.1
        // se cuenta por entregas de un consumidor y una llamada no tiene número de entrega. La política
        // configurable se aplica así: `permanent` y `retryable-bounded` dan `permanent`; `retryable`
        // da `retryable` (§5.3).
        let class = if c.class == ErrorClass::Retryable && c.max_attempts.is_none() {
            ErrorClass::Retryable
        } else {
            ErrorClass::Permanent
        };
        RpcErrorBody {
            class,
            code: "UNKNOWN".to_string(),
            message: "error interno del servicio".to_string(),
            retry_after_ms: None,
        }
    } else {
        // `poison` no viaja.
        let class = if c.class == ErrorClass::Retryable {
            ErrorClass::Retryable
        } else {
            ErrorClass::Permanent
        };
        RpcErrorBody {
            class,
            code: c.code.clone(),
            message: generic(class).to_string(),
            retry_after_ms: if class == ErrorClass::Retryable {
                c.retry_after.map(to_ms)
            } else {
                None
            },
        }
    };
    let outcome = outcome_of_class(body.class);
    (body, outcome)
}

const fn outcome_of_class(class: ErrorClass) -> RpcServerOutcome {
    match class {
        ErrorClass::Retryable => RpcServerOutcome::Retryable,
        _ => RpcServerOutcome::Permanent,
    }
}

/// Cómo se contesta a **una** petición legible: hereda de ella y comparte su traza.
struct Answering<'a> {
    env: &'a dyn ServeEnv,
    request: RpcRequestRef,
    trace: (Option<String>, Option<String>),
}

impl Answering<'_> {
    fn reject(&self, body: RpcErrorBody, outcome: RpcServerOutcome) -> Served {
        error_reply(
            self.env,
            ReplyTo::Request(self.request.clone()),
            self.trace.clone(),
            body,
            outcome,
        )
    }

    fn reject_permanent(&self, code: &str, message: String, outcome: RpcServerOutcome) -> Served {
        self.reject(
            RpcErrorBody {
                class: ErrorClass::Permanent,
                code: code.to_string(),
                message,
                retry_after_ms: None,
            },
            outcome,
        )
    }

    /// Un fallo **nuestro**, que no es del handler ni de quien llama: lo desconocido, `permanent`, sin
    /// el texto de la causa.
    fn reject_internal(&self) -> Served {
        self.reject_permanent(
            "UNKNOWN",
            "error interno del servicio".to_string(),
            RpcServerOutcome::Permanent,
        )
    }
}

/// Una petición que pasó los pasos 2 a 6: legible, de esta operación, autenticada y válida.
struct Admitted {
    event: Event,
    answering_request: RpcRequestRef,
    trace: (Option<String>, Option<String>),
    /// La petición no cumplía su esquema y el modo es `warn`: el handler corre, pero se cuenta.
    schema_warned: bool,
}

/// Atiende **una** petición — §5.1, los nueve pasos. **Siempre se contesta con un error, nunca en
/// silencio**: callar dejaría a quien llama esperando su timeout para un fallo que no se arregla
/// esperando.
pub(crate) async fn serve_request(
    env: &dyn ServeEnv,
    cfg: &ServeConfig,
    handler: &dyn RpcHandler,
    payload: &[u8],
    has_reply_subject: bool,
) -> Served {
    match admit(env, cfg, payload, has_reply_subject) {
        Ok(admitted) => execute(env, cfg, handler, admitted).await,
        Err(served) => served,
    }
}

/// Los pasos 2 a 6 de §5.1: sin subject de respuesta, el sobre, el `type`, la firma y la ventana, y el
/// esquema de la petición.
fn admit(
    env: &dyn ServeEnv,
    cfg: &ServeConfig,
    payload: &[u8],
    has_reply_subject: bool,
) -> Result<Admitted, Served> {
    let op_name = cfg.op.name_with_path();

    // 2. Sin subject de respuesta no se atiende: nadie espera el resultado de una orden (`nats pub` a
    //    mano). Es lo que debe despertar a alguien.
    if !has_reply_subject {
        env.log(
            LogLevel::Error,
            &format!(
                "una petición a `{op_name}` llegó SIN subject de respuesta y no se atiende: nadie espera \
                 el resultado de una orden. Alguien publica a mano (`nats pub`) o hay un SDK roto"
            ),
        );
        return Err(Served {
            outcome: RpcServerOutcome::Poison,
            reply: None,
        });
    }

    // 3. Interpretar el sobre. Si ni siquiera se puede leer no hay `correlationid` ni `tenantid` que
    //    heredar (§5.2).
    let event = match parse_event(payload) {
        Ok(e) => e,
        Err(e) => {
            env.log(
                LogLevel::Error,
                &format!("una petición a `{op_name}` no se pudo interpretar: {e}"),
            );
            return Err(error_reply(
                env,
                ReplyTo::Unreadable {
                    request_id: rescue_request_id(payload),
                },
                (None, None),
                RpcErrorBody {
                    class: ErrorClass::Permanent,
                    code: e.code().unwrap_or("MALFORMED_JSON").to_string(),
                    message: format!(
                        "la petición no se pudo interpretar como un CloudEvent de flux: {e}"
                    ),
                    retry_after_ms: None,
                },
                RpcServerOutcome::Poison,
            ));
        }
    };
    let trace = (event.traceparent.clone(), event.tracestate.clone());
    let answering = Answering {
        env,
        request: RpcRequestRef::of(&op_name, &event),
        trace: trace.clone(),
    };

    // 4. El `type` es el de la petición de ESTA operación.
    if event.event_type != cfg.request_type {
        return Err(answering.reject_permanent(
            "INVALID_RPC_CALL",
            format!(
                "la petición trae el `type` `{}` y la operación `{op_name}` atiende `{}`: \
                 comprueba que quien llama usa el nombre y la versión mayor de la operación",
                shown(&event.event_type),
                cfg.request_type
            ),
            RpcServerOutcome::Poison,
        ));
    }

    // 5. La firma, y con ella la ventana de 300 s.
    if let Err(e) = env.verify(&event) {
        return Err(answering.reject_permanent(
            e.code().unwrap_or("INVALID_SIGNATURE"),
            format!("la petición no se aceptó: {e}"),
            RpcServerOutcome::InvalidSignature,
        ));
    }
    if env.require_signature() {
        let now = env.now();
        let stale = event.event_time().map_or(true, |t| {
            now.signed_duration_since(t).num_milliseconds().abs()
                > STALE_REQUEST_WINDOW_SECS * 1_000
        });
        if stale {
            let error = FluxError::StaleRequest {
                time: shown(&event.time),
                window_secs: STALE_REQUEST_WINDOW_SECS,
                now: format_time(now),
            };
            return Err(answering.reject_permanent(
                crate::errors::STALE_REQUEST_CODE,
                error.to_string(),
                RpcServerOutcome::StaleRequest,
            ));
        }
    }

    // 6. L3: `data` contra el esquema de la petición. Falla → el handler NO se invoca.
    let schema_warned = match env.validate(&cfg.op, RpcSide::Request, &event.data) {
        Ok(valid) => !valid,
        Err(e) => {
            return Err(answering.reject(
                schema_body("la petición", &e),
                RpcServerOutcome::InvalidSchema,
            ));
        }
    };
    Ok(Admitted {
        answering_request: answering.request,
        event,
        trace,
        schema_warned,
    })
}

/// Los pasos 7 a 9: invocar al handler, comprobar su resultado y construir la respuesta.
async fn execute(
    env: &dyn ServeEnv,
    cfg: &ServeConfig,
    handler: &dyn RpcHandler,
    admitted: Admitted,
) -> Served {
    let Admitted {
        event,
        answering_request,
        trace,
        mut schema_warned,
    } = admitted;
    let op_name = cfg.op.name_with_path();
    let answering = Answering {
        env,
        request: answering_request,
        trace,
    };

    // 7. Invocar el handler, dentro del contexto de la petición: lo que llame o publique desde él
    //    hereda `correlationid` y `tenantid`, y su `causationid` es el `id` de la petición.
    let ctx = RpcContext {
        event: event.clone(),
        id: event.id.clone(),
        correlation_id: event.correlationid.clone(),
        tenant_id: event.tenantid.clone(),
        aggregate_id: event.aggregate_id.clone(),
        time: event.event_time().ok(),
        operation: op_name.clone(),
    };
    let call = context::scope(
        Some(EventContext::from_event(&event)),
        handler.call(
            RpcRequest {
                data: event.data.clone(),
            },
            ctx,
        ),
    );
    let raw = match AssertUnwindSafe(call).catch_unwind().await {
        // Un pánico es un bug del que responde: lo desconocido, `permanent`.
        Err(_) => {
            env.log(
                LogLevel::Error,
                &format!(
                    "pánico en el handler de `{op_name}` (petición {})",
                    event.id
                ),
            );
            return answering.reject_internal();
        }
        Ok(Err(error)) => {
            let (body, outcome) = wire_error(env, &op_name, &event.id, error.as_ref());
            return answering.reject(body, outcome);
        }
        Ok(Ok(raw)) => raw,
    };

    // 8. Su resultado es un objeto y cumple el esquema de la respuesta.
    if !raw.get().trim_start().starts_with('{') {
        return answering.reject_permanent(
            "INVALID_SCHEMA",
            "la respuesta no es un objeto".to_string(),
            RpcServerOutcome::InvalidSchema,
        );
    }
    match env.validate(&cfg.op, RpcSide::Reply, &raw) {
        Ok(valid) => schema_warned |= !valid,
        // El fallo es **del que responde**, no de quien llama: el mensaje lo dice.
        Err(e) => {
            return answering.reject(
                schema_body("la respuesta", &e),
                RpcServerOutcome::InvalidSchema,
            );
        }
    }

    finish(env, cfg, &answering, &event.id, raw, schema_warned)
}

/// El paso 9 de §5.1: construir la respuesta (§3.2), firmarla y comprobar que cabe **antes** de
/// publicar: si no cupiese, la librería lanzaría en quien responde y quien llama solo vería un
/// timeout (A-65).
fn finish(
    env: &dyn ServeEnv,
    cfg: &ServeConfig,
    answering: &Answering<'_>,
    request_id: &str,
    raw: Box<RawValue>,
    schema_warned: bool,
) -> Served {
    let op_name = cfg.op.name_with_path();
    let own_trace = env.traceparent();
    let (traceparent, tracestate) = if own_trace.is_some() {
        (own_trace, None)
    } else {
        answering.trace.clone()
    };
    let built = build_rpc_reply(RpcReplyInput {
        request: answering.request.clone(),
        id: env.new_id(),
        source: env.source().to_string(),
        time: env.now(),
        producerversion: env.producer_version().to_string(),
        classification: cfg.classification,
        dataschema: cfg.reply_dataschema.clone(),
        traceparent,
        tracestate,
        data: raw,
    })
    .and_then(|e| env.sign(e))
    .and_then(|e| {
        serde_json::to_vec(&e).map_err(|err| {
            FluxError::Envelope(format!("la respuesta no es serializable a JSON: {err}"))
        })
    });
    let bytes = match built {
        Ok(b) => b,
        Err(e) => {
            env.log(
                LogLevel::Error,
                &format!(
                    "no se pudo construir la respuesta de `{op_name}` (petición {request_id}): {e}"
                ),
            );
            return answering.reject_internal();
        }
    };
    let limit = env.max_message_bytes();
    if bytes.len() > limit {
        let error = FluxError::RpcMessageTooLarge {
            what: "la respuesta".to_string(),
            bytes: bytes.len(),
            limit,
        };
        env.log(
            LogLevel::Error,
            &format!("la respuesta de `{op_name}` a la petición {request_id} no cabe: {error}"),
        );
        return answering.reject_permanent(
            crate::errors::RPC_MESSAGE_TOO_LARGE_CODE,
            error.to_string(),
            RpcServerOutcome::Permanent,
        );
    }
    Served {
        outcome: if schema_warned {
            RpcServerOutcome::InvalidSchema
        } else {
            RpcServerOutcome::Ok
        },
        reply: Some(bytes),
    }
}

/// [`ServeEnv`] contra un bus real.
struct BusEnv {
    bus: Bus,
}

impl ServeEnv for BusEnv {
    fn source(&self) -> &str {
        &self.bus.inner.source
    }

    fn producer_version(&self) -> &str {
        &self.bus.inner.opts.version
    }

    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn new_id(&self) -> String {
        Uuid::now_v7().to_string()
    }

    fn traceparent(&self) -> Option<String> {
        self.bus.inner.opts.traceparent.as_ref().and_then(|f| f())
    }

    fn classify(&self, error: &(dyn StdError + 'static)) -> Classification {
        self.bus.inner.classifier.classify(error)
    }

    fn require_signature(&self) -> bool {
        #[cfg(feature = "signing")]
        {
            self.bus.inner.opts.signing.verify == crate::signing::VerificationMode::Require
        }
        #[cfg(not(feature = "signing"))]
        {
            false
        }
    }

    fn verify(&self, event: &Event) -> Result<(), FluxError> {
        self.bus.verify_reply(event)
    }

    #[cfg_attr(not(feature = "signing"), allow(clippy::unused_self))]
    fn sign(&self, event: Event) -> Result<Event, FluxError> {
        #[cfg(feature = "signing")]
        if let Some(signer) = &self.bus.inner.signer {
            return signer.sign(event);
        }
        Ok(event)
    }

    fn validate(
        &self,
        op: &RpcOperation,
        side: RpcSide,
        data: &RawValue,
    ) -> Result<bool, FluxError> {
        self.bus.validate_rpc_data(op, side, data)
    }

    fn max_message_bytes(&self) -> usize {
        self.bus.max_message_bytes()
    }

    fn log(&self, level: LogLevel, message: &str) {
        self.bus.log(level, message);
    }
}

// ─── Quien responde: el ciclo de vida ────────────────────────────────────────

/// El estado compartido de un responder: lo que `stop()` y `Bus::close()` necesitan para pararlo.
#[derive(Debug)]
pub(crate) struct ResponderShared {
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
}

impl ResponderShared {
    /// Dejar de escuchar, esperar a lo que está en vuelo y terminar — §5.5. Idempotente.
    async fn stop(&self) {
        let _ = self.stop.send(true);
        let mut done = self.done.clone();
        let _ = done.wait_for(|finished| *finished).await;
    }
}

/// Los responders vivos de un bus: la operación → su estado. Lo mira `Bus::close()` para parar antes de
/// cerrar, y `respond()` para no dejar dos handlers sobre la misma operación.
#[derive(Debug, Default)]
pub(crate) struct RpcRegistry {
    responders: Mutex<HashMap<String, Arc<ResponderShared>>>,
}

impl RpcRegistry {
    /// Para **todos** los responders del bus, a la vez, y espera a que terminen. Después deja un
    /// margen breve para que, en un cluster, se propague que ya nadie escucha (A-70).
    pub(crate) async fn stop_all(&self) {
        let all: Vec<Arc<ResponderShared>> = lock(&self.responders).values().cloned().collect();
        if all.is_empty() {
            return;
        }
        futures::future::join_all(all.iter().map(|r| r.stop())).await;
        tokio::time::sleep(STOP_MARGIN).await;
    }
}

/// Una operación atendida. Se obtiene con [`Bus::respond`].
///
/// Al soltarse **no** se deja de atender: eso convertiría un `let _ = bus.respond(…)` en un servicio
/// que muere sin decir nada. Para parar, [`Responder::stop`] o [`Bus::close`].
#[derive(Debug, Clone)]
pub struct Responder {
    /// El nombre de la operación (`clientes.cliente.v1.calcular-descuento`).
    pub operation: String,
    /// El subject de NATS al que está suscrito: `rpc.<operación>`, **exacto**.
    pub subject: String,
    shared: Arc<ResponderShared>,
}

impl Responder {
    /// Para de atender la operación, en el orden de §5.5: **dejar de escuchar** (con el drenaje de la
    /// librería: desde ese instante las peticiones nuevas dan `no responders` y no caen en una
    /// instancia que se va), **esperar a los handlers en vuelo** (hasta [`STOP_TIMEOUT`], 25 s) y
    /// terminar. No cierra la conexión: es la del bus.
    ///
    /// Cerrar la conexión de golpe **pierde lo que se atiende** (A-62).
    pub async fn stop(&self) {
        self.shared.stop().await;
        // Vacía lo que quedó en el búfer de escritura: las últimas respuestas.
        tokio::task::yield_now().await;
    }

    /// `true` cuando ya dejó de atender y no queda nada en vuelo.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        *self.shared.done.borrow()
    }
}

/// El bucle de un responder: recibe, atiende con a lo sumo `max` handlers a la vez, y al parar drena.
async fn responder_loop(
    bus: Bus,
    cfg: Arc<ServeConfig>,
    handler: Arc<dyn RpcHandler>,
    mut sub: async_nats::Subscriber,
    mut stop: watch::Receiver<bool>,
    max: usize,
) {
    let semaphore = Arc::new(Semaphore::new(max));
    let env = Arc::new(BusEnv { bus: bus.clone() });

    let dispatch = |msg: async_nats::Message, permit: tokio::sync::OwnedSemaphorePermit| {
        let (env, cfg, handler) = (Arc::clone(&env), Arc::clone(&cfg), Arc::clone(&handler));
        tokio::spawn(async move {
            // El permiso vive lo que dure el handler: es lo que cuenta lo que está en vuelo.
            let _permit = permit;
            serve_message(&env, &cfg, handler.as_ref(), msg).await;
        });
    };

    // Atender. Primero un hueco y después el mensaje: así el exceso espera en la cola de la suscripción
    // (§5.6) y no en un task por mensaje.
    loop {
        let permit = tokio::select! {
            biased;
            _ = stop.wait_for(|stopping| *stopping) => break,
            p = Arc::clone(&semaphore).acquire_owned() => match p {
                Ok(p) => p,
                Err(_) => break,
            },
        };
        let msg = tokio::select! {
            biased;
            _ = stop.wait_for(|stopping| *stopping) => break,
            m = sub.next() => match m {
                Some(m) => m,
                None => break,
            },
        };
        dispatch(msg, permit);
    }

    // 1. Dejar de escuchar con el DRENAJE de la librería: UNSUB, y se sigue entregando lo que ya viajaba
    //    hasta que la suscripción se cierra. No con una baja que descarte lo que aún llega.
    if sub.drain().await.is_ok() {
        while let Some(msg) = sub.next().await {
            let Ok(permit) = Arc::clone(&semaphore).acquire_owned().await else {
                break;
            };
            dispatch(msg, permit);
        }
    }

    // 2. Esperar a los handlers en vuelo, con un tope: una petición más lenta ya la dio por perdida
    //    quien llamó. Cada handler tiene un permiso; tenerlos todos es que no queda ninguno.
    let all = u32::try_from(max).unwrap_or(u32::MAX);
    match tokio::time::timeout(STOP_TIMEOUT, Arc::clone(&semaphore).acquire_many_owned(all)).await {
        Ok(_) => {}
        Err(_) => bus.log(
            LogLevel::Error,
            &format!(
                "al parar `{}` quedaron handlers en vuelo pasados {STOP_TIMEOUT:?}: las peticiones que \
                 atendían se darán por perdidas",
                cfg.op.name_with_path()
            ),
        ),
    }
    // Lo último que salió: que el cliente lo escriba ya.
    let _ = bus.inner.client.flush().await;
}

/// Atiende un mensaje de la suscripción: el algoritmo, la publicación de la respuesta y las métricas.
async fn serve_message(
    env: &BusEnv,
    cfg: &ServeConfig,
    handler: &dyn RpcHandler,
    msg: async_nats::Message,
) {
    let started = Instant::now();
    let op_name = cfg.op.name_with_path();
    let served = serve_request(env, cfg, handler, &msg.payload, msg.reply.is_some()).await;
    if let (Some(bytes), Some(reply_to)) = (served.reply, msg.reply) {
        // **Core NATS**, al subject de respuesta de la petición: el broker admite UNA respuesta por
        // petición con `allow_responses` (A-68), y una segunda la deniega.
        if let Err(e) = env.bus.inner.client.publish(reply_to, bytes.into()).await {
            env.log(
                LogLevel::Error,
                &format!("no se pudo publicar la respuesta de `{op_name}`: {e}"),
            );
        }
    }
    let metrics = &env.bus.inner.metrics;
    metrics.rpc_server_request(&op_name, served.outcome);
    metrics.rpc_duration(&op_name, RpcRole::Server, started.elapsed().as_secs_f64());
}

impl Bus {
    /// Atiende una operación — **solo el dueño del dominio** responde (§2.3).
    ///
    /// Equivale a [`Bus::respond_with`] con las opciones por defecto.
    ///
    /// # Errores
    ///
    /// Ver [`Bus::respond_with`].
    pub async fn respond<H: RpcHandler>(
        &self,
        operation: &str,
        handler: H,
    ) -> Result<Responder, FluxError> {
        self.respond_with(operation, handler, RespondOptions::default())
            .await
    }

    /// Atiende una operación con ajustes.
    ///
    /// Una **suscripción por operación, al subject EXACTO** `rpc.<operación>`, con el nombre del
    /// servicio como **grupo de cola**: dos instancias del mismo servicio se reparten las peticiones, y
    /// una petición a una operación que este servicio no atiende (una errata, una versión aún no
    /// desplegada) da `no responders` en ~1 ms. **Nunca** `rpc.<dominio>.>`: con el comodín el servicio
    /// recibiría interés sobre todo lo de su dominio y esa petición agotaría su timeout (A-64). Lo que
    /// se **concede** no es lo que se **usa**.
    ///
    /// # Errores
    ///
    /// Antes de tocar la red: [`FluxError::ReservedDomain`] y [`FluxError::InvalidRpcCall`] (operación
    /// mal formada, `max_concurrent` cero, o una segunda `respond` sobre la misma operación en este
    /// bus).
    ///
    /// **[`FluxError::PermissionDenied`] si la ACL no concede la suscripción**: la suscripción denegada
    /// no lanza al crearse —el aviso llega aparte— y un dueño sin ese permiso parecería, para quien
    /// llama, un servicio caído (`RPC_UNAVAILABLE`, A-73). Falla **al arrancar**, nombrando el subject,
    /// en vez de arrancar mudo.
    pub async fn respond_with<H: RpcHandler>(
        &self,
        operation: &str,
        handler: H,
        opts: RespondOptions,
    ) -> Result<Responder, FluxError> {
        let op = RpcOperation::parse(operation)?;
        let max = opts.max_concurrent.unwrap_or(MAX_CONCURRENT_HANDLERS);
        if max == 0 || u32::try_from(max).is_err() {
            return Err(FluxError::InvalidRpcCall {
                reason: format!(
                    "`max_concurrent` de {max} no sirve: debe ser al menos 1 (por defecto \
                     {MAX_CONCURRENT_HANDLERS})"
                ),
            });
        }
        let op_name = op.name_with_path();
        let subject = op.subject();

        // La respuesta lleva su `dataschema` desde el primer momento: sin bundle ni URL base es un
        // error de arranque, no uno que se descubre con la primera petición.
        let reply_dataschema = self.rpc_schema_uri(&op, RpcSide::Reply)?;
        self.check_rpc_bundle(&op)?;
        let cfg = Arc::new(ServeConfig {
            request_type: op.event_type(RpcSide::Request),
            op,
            reply_dataschema,
            classification: opts
                .classification
                .or(self.inner.opts.classification)
                .unwrap_or(DataClassification::Internal),
        });

        let (stop_tx, stop_rx) = watch::channel(false);
        let (done_tx, done_rx) = watch::channel(false);
        let shared = Arc::new(ResponderShared {
            stop: stop_tx,
            done: done_rx,
        });
        {
            let mut registry = lock(&self.inner.rpc.responders);
            if registry.contains_key(&op_name) {
                return Err(FluxError::InvalidRpcCall {
                    reason: format!(
                        "ya hay un `respond` sobre `{op_name}` en este bus: dos handlers bajo el mismo \
                         grupo de cola se repartirían las peticiones al azar. Para dos instancias, dos \
                         procesos (cada uno su `connect()`); para sustituirlo, `stop()` el anterior"
                    ),
                });
            }
            registry.insert(op_name.clone(), Arc::clone(&shared));
        }

        match self.subscribe_exact(&subject, &op_name).await {
            Ok(sub) => {
                let bus = self.clone();
                let handler: Arc<dyn RpcHandler> = Arc::new(handler);
                tokio::spawn(async move {
                    let name = cfg.op.name_with_path();
                    responder_loop(bus.clone(), cfg, handler, sub, stop_rx, max).await;
                    lock(&bus.inner.rpc.responders).remove(&name);
                    let _ = done_tx.send(true);
                });
                Ok(Responder {
                    operation: op_name,
                    subject,
                    shared,
                })
            }
            Err(e) => {
                lock(&self.inner.rpc.responders).remove(&op_name);
                Err(e)
            }
        }
    }

    /// La suscripción al subject exacto, con el servicio como grupo de cola, **comprobando que el broker
    /// la concede**.
    async fn subscribe_exact(
        &self,
        subject: &str,
        op_name: &str,
    ) -> Result<async_nats::Subscriber, FluxError> {
        let mark = self.inner.denials.mark();
        let sub = self
            .inner
            .client
            .queue_subscribe(subject.to_string(), self.inner.opts.service.clone())
            .await
            .map_err(|e| transport(format!("fallo al suscribirse a `{subject}`"), e))?;
        // `async-nats` no entrega la denegación como error: llega por el evento de la conexión, y su
        // `flush()` no es un viaje de ida y vuelta. Se espera el aviso hasta un tope (A-73, A-90).
        let _ = self.inner.client.flush().await;
        if let Some(denial) = self
            .inner
            .denials
            .subscription_denial(mark, subject, SUBSCRIBE_SETTLE)
            .await
        {
            drop(sub);
            return Err(permission_denied(
                format!(
                    "atender la operación `{op_name}` (`respond`: suscribirse a `{subject}`, con \
                     `{}` como grupo de cola; el dueño de un dominio necesita permiso de suscripción \
                     a `rpc.<dominio>.>` y `allow_responses` para contestar). Sin ese permiso el \
                     servicio arrancaría mudo y quien llame recibiría `RPC_UNAVAILABLE`, como si \
                     estuviera caído (A-73)",
                    self.inner.opts.service
                ),
                &denial,
            ));
        }
        Ok(sub)
    }

    /// Con L3 activo, el bundle desplegado con el servicio **tiene** que traer los dos esquemas de la
    /// operación: un bundle viejo descubierto con la primera petición es un servicio que pasa el
    /// healthcheck y rechaza todo.
    #[cfg_attr(
        not(feature = "validation"),
        allow(clippy::unused_self, clippy::unnecessary_wraps)
    )]
    fn check_rpc_bundle(&self, op: &RpcOperation) -> Result<(), FluxError> {
        #[cfg(feature = "validation")]
        if let Some(validator) = &self.inner.validator {
            for side in [RpcSide::Request, RpcSide::Reply] {
                let uri = self.rpc_schema_uri(op, side)?;
                if !validator.knows(&uri) {
                    let error = FluxError::SchemaNotFound(crate::errors::SchemaNotFoundError {
                        subject: op.name_with_path(),
                        dataschema: uri,
                    });
                    if self.inner.opts.validation.mode == crate::validation::ValidationMode::Strict
                    {
                        return Err(error);
                    }
                    self.log(LogLevel::Warn, &error.to_string());
                }
            }
        }
        #[cfg(not(feature = "validation"))]
        let _ = op;
        Ok(())
    }
}

// ═════════════════════════════════════════════════════════════════════════════
//  Tests unitarios — sin broker
// ═════════════════════════════════════════════════════════════════════════════
//
// Ejercitan lo que se puede probar sin NATS: las funciones puras contra los **bytes de la spec**
// (`conformance/cases/cross-sdk-rpc-envelope.json`, §3.5), el algoritmo de quien responde con un
// entorno de mentira —reloj, `id` y traza fijos— y el de quien llama con un transporte en memoria.
// Lo que solo puede verse contra un broker real —la denegación que `async-nats` no entrega, el
// reparto entre instancias, la parada ordenada— está en `tests/rpc.rs`.
#[cfg(test)]
mod tests {
    use std::fmt::{self, Write as _};
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

    use serde_json::json;

    use super::*;
    use crate::classify::{Classifier, ClassifierOptions, HttpError, UnknownPolicy};
    use crate::errors::{
        SchemaValidationError, INVALID_RPC_CALL_CODE, RESERVED_DOMAIN_CODE, RPC_INVALID_REPLY_CODE,
        RPC_MESSAGE_TOO_LARGE_CODE, RPC_TIMEOUT_CODE, RPC_UNAVAILABLE_CODE, STALE_REQUEST_CODE,
    };

    const FIXTURES: &str = include_str!("../../conformance/cases/cross-sdk-rpc-envelope.json");
    const PROTOCOL: &str = include_str!("../../protocol.json");
    const BASE: &str = "https://schemas.internal";
    const OPERACION: &str = "clientes.cliente.v1.calcular-descuento";

    fn json_of(text: &str) -> Value {
        serde_json::from_str(text).expect("JSON válido")
    }

    fn code_of<T: fmt::Debug>(r: &Result<T, FluxError>) -> Option<String> {
        r.as_ref()
            .err()
            .and_then(FluxError::code)
            .map(ToString::to_string)
    }

    // ── SHA-256 y base64, para fijar los bytes del fixture sin añadir dependencias ──

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

    fn b64_decode(text: &str) -> Vec<u8> {
        let mut acc: u32 = 0;
        let mut bits: u32 = 0;
        let mut out = Vec::new();
        for c in text.bytes() {
            let v = match c {
                b'A'..=b'Z' => c - b'A',
                b'a'..=b'z' => c - b'a' + 26,
                b'0'..=b'9' => c - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                _ => continue,
            };
            acc = (acc << 6) | u32::from(v);
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push(u8::try_from((acc >> bits) & 0xFF).unwrap());
            }
        }
        out
    }

    /// El SHA-256 de prueba es el de verdad: si no, los hashes del fixture no probarían nada.
    #[test]
    fn el_sha256_de_los_tests_es_correcto() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    // ── El fixture: las cuatro líneas de la spec ────────────────────────────

    fn fixture(id: &str) -> Value {
        json_of(FIXTURES)["fixtures"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["id"] == id)
            .unwrap_or_else(|| panic!("no hay fixture `{id}`"))
            .clone()
    }

    fn utc(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .expect("RFC 3339")
            .with_timezone(&Utc)
    }

    fn raw_of(v: &Value) -> Box<RawValue> {
        serde_json::value::to_raw_value(v).expect("raw")
    }

    fn text(v: &Value, key: &str) -> String {
        v[key].as_str().unwrap_or_default().to_string()
    }

    fn opt_text(v: &Value, key: &str) -> Option<String> {
        v[key].as_str().map(ToString::to_string)
    }

    fn request_input() -> RpcRequestInput {
        let i = &fixture("rpc-request")["input"];
        RpcRequestInput {
            operation: text(i, "operation"),
            tenant_id: text(i, "tenantId"),
            aggregate_id: opt_text(i, "aggregateId"),
            id: text(i, "id"),
            source: text(i, "source"),
            time: utc(&text(i, "time")),
            correlationid: text(i, "correlationid"),
            causationid: None,
            producerversion: text(i, "producerversion"),
            classification: DataClassification::from_str_exact(&text(i, "classification")).unwrap(),
            dataschema: rpc_dataschema(BASE, &text(i, "operation"), RpcSide::Request).unwrap(),
            traceparent: opt_text(i, "traceparent"),
            tracestate: None,
            data: raw_of(&i["data"]),
        }
    }

    fn request_ref(v: &Value) -> RpcRequestRef {
        RpcRequestRef {
            operation: text(v, "operation"),
            id: text(v, "id"),
            correlationid: text(v, "correlationid"),
            tenantid: text(v, "tenantid"),
            subject: opt_text(v, "subject"),
            partitionkey: None,
        }
    }

    fn reply_input() -> RpcReplyInput {
        let i = &fixture("rpc-reply")["input"];
        RpcReplyInput {
            request: request_ref(&i["request"]),
            id: text(i, "id"),
            source: text(i, "source"),
            time: utc(&text(i, "time")),
            producerversion: text(i, "producerversion"),
            classification: DataClassification::from_str_exact(&text(i, "classification")).unwrap(),
            dataschema: rpc_dataschema(BASE, &text(&i["request"], "operation"), RpcSide::Reply)
                .unwrap(),
            traceparent: opt_text(i, "traceparent"),
            tracestate: None,
            data: raw_of(&i["data"]),
        }
    }

    fn error_input(id: &str) -> RpcErrorInput {
        let i = &fixture(id)["input"];
        let e = &i["error"];
        RpcErrorInput {
            reply_to: ReplyTo::Request(request_ref(&i["request"])),
            id: text(i, "id"),
            source: text(i, "source"),
            time: utc(&text(i, "time")),
            producerversion: text(i, "producerversion"),
            traceparent: opt_text(i, "traceparent"),
            tracestate: None,
            error: RpcErrorBody {
                class: if e["class"] == "retryable" {
                    ErrorClass::Retryable
                } else {
                    ErrorClass::Permanent
                },
                code: text(e, "code"),
                message: text(e, "message"),
                retry_after_ms: e["retryAfterMs"].as_u64(),
            },
        }
    }

    /// Los bytes del evento tal como se publican.
    fn wire(event: &Event) -> Vec<u8> {
        serde_json::to_vec(event).expect("serializa")
    }

    /// Comprueba bytes, longitud y SHA-256 contra el fixture —que `rpc-broker.mjs` reconstruye aparte
    /// desde sus datos y compara con el hash de la spec—.
    fn assert_fixture_bytes(id: &str, produced: &[u8]) {
        let f = fixture(id);
        let expected = b64_decode(f["base64"].as_str().unwrap());
        assert_eq!(
            String::from_utf8_lossy(produced),
            String::from_utf8_lossy(&expected),
            "los bytes de `{id}` no son los de la spec"
        );
        assert_eq!(
            produced.len() as u64,
            f["bytes"].as_u64().unwrap(),
            "{id}: longitud"
        );
        assert_eq!(
            sha256_hex(produced),
            f["sha256"].as_str().unwrap(),
            "{id}: sha256"
        );
    }

    // ── Los bytes de §3.5 ───────────────────────────────────────────────────

    #[test]
    fn la_peticion_produce_los_bytes_de_la_spec() {
        let event = build_rpc_request(request_input()).unwrap();
        assert_fixture_bytes("rpc-request", &wire(&event));
        // Lo que dice la spec de esos bytes, sin fiarse del fixture: `data` es lo último, el `subject`
        // y el `partitionkey` son el `aggregateId`, y no hay `causationid` (no nace de otro evento).
        assert_eq!(
            event.event_type,
            "com.flux.rpc.clientes.cliente.calcular-descuento.request.v1"
        );
        assert_eq!(event.aggregate_id.as_deref(), Some("cli-1"));
        assert_eq!(event.partitionkey.as_deref(), Some("cli-1"));
        assert_eq!(event.correlationid, event.id);
        assert_eq!(event.causationid, None);
        let claves: Vec<String> = json_of(&String::from_utf8(wire(&event)).unwrap())
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        assert_eq!(claves.last().map(String::as_str), Some("data"));
    }

    #[test]
    fn la_respuesta_produce_los_bytes_de_la_spec_y_hereda_de_la_peticion() {
        let event = build_rpc_reply(reply_input()).unwrap();
        assert_fixture_bytes("rpc-reply", &wire(&event));
        // Hereda `correlationid`, `tenantid`, `subject` y `partitionkey`; su `causationid` es el `id` de
        // la petición y su `id` es nuevo — §3.2.
        let peticion = build_rpc_request(request_input()).unwrap();
        assert_eq!(event.causationid.as_deref(), Some(peticion.id.as_str()));
        assert_eq!(event.correlationid, peticion.correlationid);
        assert_eq!(event.tenantid, peticion.tenantid);
        assert_eq!(event.aggregate_id, peticion.aggregate_id);
        assert_eq!(event.partitionkey, peticion.partitionkey);
        assert_ne!(event.id, peticion.id);
        assert_eq!(event.source, "/produccion/crm-api");
    }

    #[test]
    fn los_dos_errores_producen_los_bytes_de_la_spec() {
        for id in ["rpc-error-handler", "rpc-error-sdk"] {
            let event = build_rpc_error(error_input(id)).unwrap();
            assert_fixture_bytes(id, &wire(&event));
            assert_eq!(event.event_type, RPC_ERROR_TYPE);
            assert_eq!(event.dataschema, RPC_ERROR_DATASCHEMA);
            // El error es SIEMPRE `internal`, aunque la operación sea otra cosa.
            assert_eq!(event.dataclassification, DataClassification::Internal);
        }
    }

    /// UTF-8 literal, sin escapes `\u` — 01-envelope.md §1.1. `serde_json` lo hace solo, y este test
    /// existe por si alguien cambia el serializador.
    #[test]
    fn los_bytes_llevan_utf8_literal() {
        for bytes in [
            wire(&build_rpc_request(request_input()).unwrap()),
            wire(&build_rpc_reply(reply_input()).unwrap()),
        ] {
            let texto = String::from_utf8(bytes).unwrap();
            assert!(!texto.contains("\\u"), "no debe haber escapes: {texto}");
        }
        assert!(
            String::from_utf8(wire(&build_rpc_request(request_input()).unwrap()))
                .unwrap()
                .contains("Otoño")
        );
    }

    #[test]
    fn el_orden_de_claves_de_data_es_el_de_entrada() {
        let mut input = request_input();
        input.data = raw_of(&json!({ "z": 1, "a": 2, "m": { "y": 1, "b": 2 } }));
        let texto = String::from_utf8(wire(&build_rpc_request(input).unwrap())).unwrap();
        assert!(
            texto.ends_with(r#""data":{"z":1,"a":2,"m":{"y":1,"b":2}}}"#),
            "{texto}"
        );
    }

    /// Un error `retryable` sin `retryAfterMs` no lo lleva: la clave solo está cuando hay valor.
    #[test]
    fn retry_after_ms_solo_esta_si_hay_valor() {
        let mut input = error_input("rpc-error-handler");
        input.error.retry_after_ms = None;
        let texto = String::from_utf8(wire(&build_rpc_error(input).unwrap())).unwrap();
        assert!(!texto.contains("retryAfterMs"), "{texto}");
        assert!(
            texto.ends_with(r#""message":"el servicio de tarifas no responde"}}"#),
            "{texto}"
        );
    }

    // ── La operación y el timeout: los vectores del fixture ─────────────────

    #[test]
    fn los_vectores_de_la_operacion_dan_el_subject_o_el_codigo() {
        let vectores = json_of(FIXTURES)["subjectVectors"].clone();
        let vectores = vectores.as_array().unwrap();
        assert!(vectores.len() >= 20, "el fixture trae sus 20 vectores");
        for v in vectores {
            let operacion = v["operation"].as_str().unwrap();
            let r = rpc_subject(operacion);
            match (v["natsSubject"].as_str(), v["errorCode"].as_str()) {
                (Some(subject), None) => {
                    assert_eq!(r.as_deref().ok(), Some(subject), "{operacion:?}");
                }
                (None, Some(code)) => {
                    assert_eq!(code_of(&r).as_deref(), Some(code), "{operacion:?}: {r:?}");
                }
                other => panic!("vector mal formado: {other:?}"),
            }
        }
    }

    /// Lo que un lenguaje de tipos débiles puede mandar como `timeoutMs` y que aquí no es un entero.
    fn timeout_de_json(v: Option<&Value>) -> Result<u64, FluxError> {
        match v {
            None => resolve_timeout_ms(None),
            Some(Value::Number(n)) => match n.as_i64() {
                Some(ms) => resolve_timeout_ms(Some(ms)),
                None => Err(FluxError::InvalidRpcCall {
                    reason: "no es un entero".into(),
                }),
            },
            Some(_) => Err(FluxError::InvalidRpcCall {
                reason: "no es un número".into(),
            }),
        }
    }

    #[test]
    fn los_vectores_del_timeout_dan_los_milisegundos_o_el_codigo() {
        let vectores = json_of(FIXTURES)["timeoutVectors"].clone();
        for v in vectores.as_array().unwrap() {
            let r = timeout_de_json(v.get("timeoutMs"));
            match (v["effectiveMs"].as_u64(), v["errorCode"].as_str()) {
                (Some(ms), None) => assert_eq!(r.ok(), Some(ms), "{v}"),
                (None, Some(code)) => assert_eq!(code_of(&r).as_deref(), Some(code), "{v}"),
                other => panic!("vector mal formado: {other:?}"),
            }
        }
    }

    #[test]
    fn el_timeout_se_rechaza_fuera_de_rango_y_no_se_recorta() {
        assert_eq!(resolve_timeout_ms(None).unwrap(), 5_000);
        assert_eq!(resolve_timeout_ms(Some(1)).unwrap(), 1);
        assert_eq!(resolve_timeout_ms(Some(25_000)).unwrap(), 25_000);
        for malo in [0, -1, 25_001, 30_000, i64::MIN, i64::MAX] {
            let r = resolve_timeout_ms(Some(malo));
            assert_eq!(
                code_of(&r).as_deref(),
                Some(INVALID_RPC_CALL_CODE),
                "{malo}"
            );
            // El mensaje dice el rango y que no se recorta.
            let msg = r.unwrap_err().to_string();
            assert!(msg.contains("entre 1 y 25000"), "{msg}");
            assert!(msg.contains("no se recorta"), "{msg}");
        }
        // Un `Duration` tiene que ser un número ENTERO de milisegundos: 1,5 ms es el `1.5` de los vectores.
        assert!(resolve_timeout(Some(Duration::from_micros(1_500))).is_err());
        assert!(resolve_timeout(Some(Duration::from_micros(500))).is_err());
        assert_eq!(
            resolve_timeout(Some(Duration::from_secs(2))).unwrap(),
            Duration::from_secs(2)
        );
        assert_eq!(resolve_timeout(None).unwrap(), Duration::from_secs(5));
        assert!(resolve_timeout(Some(Duration::from_secs(26))).is_err());
        assert!(resolve_timeout(Some(Duration::ZERO)).is_err());
    }

    #[test]
    fn un_dominio_reservado_gana_a_un_patron_roto() {
        // El orden de §4.1 paso 0: RESERVED_DOMAIN primero, INVALID_RPC_CALL después.
        for (op, code) in [
            ("rpc.clientes.cliente.v1.obtener", RESERVED_DOMAIN_CODE),
            ("state.cliente.v1.obtener", RESERVED_DOMAIN_CODE),
            ("dlq.cliente.v1.obtener", RESERVED_DOMAIN_CODE),
            ("system.cliente.v1.obtener", RESERVED_DOMAIN_CODE),
            // Reservado Y con mayúsculas y un 5º token: sigue siendo RESERVED_DOMAIN.
            ("rpc.Clientes.cliente.v1.obtener.x", RESERVED_DOMAIN_CODE),
            ("clientes.cliente.v1", INVALID_RPC_CALL_CODE),
            ("", INVALID_RPC_CALL_CODE),
        ] {
            assert_eq!(
                code_of(&RpcOperation::parse(op)).as_deref(),
                Some(code),
                "{op:?}"
            );
        }
        // El mensaje del prefijo dice que lo pone el SDK.
        let msg = RpcOperation::parse("rpc.clientes.cliente.v1.obtener")
            .unwrap_err()
            .to_string();
        assert!(msg.contains("lo pone el SDK"), "{msg}");
    }

    #[test]
    fn la_version_mayor_no_tiene_tope_como_dice_el_patron() {
        // `v[1-9][0-9]*` sin límite: un `u32` rechazaría esta operación y Node la aceptaría.
        let op = RpcOperation::parse("clientes.cliente.v99999999999999999999.obtener").unwrap();
        assert_eq!(op.major, "99999999999999999999");
        assert_eq!(
            op.event_type(RpcSide::Request),
            "com.flux.rpc.clientes.cliente.obtener.request.v99999999999999999999"
        );
        for malo in ["v0", "v01", "v", "V1", "1", "v1.5", "v-1"] {
            let op = format!("clientes.cliente.{malo}.obtener");
            assert_eq!(
                code_of(&RpcOperation::parse(&op)).as_deref(),
                Some(INVALID_RPC_CALL_CODE),
                "{op}"
            );
        }
    }

    #[test]
    fn la_operacion_deriva_su_subject_su_type_y_su_esquema() {
        let op = RpcOperation::parse(OPERACION).unwrap();
        assert_eq!(op.subject(), "rpc.clientes.cliente.v1.calcular-descuento");
        assert_eq!(op.name_with_path(), OPERACION);
        assert_eq!(
            op.event_type(RpcSide::Reply),
            "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1"
        );
        assert_eq!(
            op.default_dataschema("https://schemas.internal/", RpcSide::Request),
            "https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json"
        );
        // Con los patrones de protocol.json, sin repetirlos a mano.
        let protocol = json_of(PROTOCOL);
        let patron = |ruta: &[&str]| {
            let mut v = &protocol["rpc"];
            for k in ruta {
                v = &v[*k];
            }
            regex::Regex::new(v.as_str().unwrap()).unwrap()
        };
        assert!(patron(&["subject", "pattern"]).is_match(&op.subject()));
        assert!(patron(&["operation", "pattern"]).is_match(&op.name_with_path()));
        assert!(patron(&["type", "request", "pattern"]).is_match(&op.event_type(RpcSide::Request)));
        assert!(patron(&["type", "reply", "pattern"]).is_match(&op.event_type(RpcSide::Reply)));
        assert!(patron(&["dataschema", "request", "pattern"])
            .is_match(&op.default_dataschema(BASE, RpcSide::Request)));
        assert!(patron(&["dataschema", "reply", "pattern"])
            .is_match(&op.default_dataschema(BASE, RpcSide::Reply)));
    }

    #[test]
    fn la_operacion_cumple_los_ejemplos_validos_e_invalidos_de_protocol_json() {
        let protocol = json_of(PROTOCOL);
        let valid = protocol["rpc"]["operation"]["valid"].as_array().unwrap();
        let invalid = protocol["rpc"]["operation"]["invalid"].as_array().unwrap();
        assert!(!valid.is_empty() && !invalid.is_empty());
        for v in valid {
            assert!(RpcOperation::parse(v.as_str().unwrap()).is_ok(), "{v}");
        }
        for v in invalid {
            assert!(RpcOperation::parse(v.as_str().unwrap()).is_err(), "{v}");
        }
        for v in protocol["rpc"]["subject"]["valid"].as_array().unwrap() {
            let subject = v.as_str().unwrap();
            let op = subject.strip_prefix("rpc.").unwrap();
            assert_eq!(rpc_subject(op).as_deref().ok(), Some(subject));
        }
    }

    // ── Las funciones puras validan ─────────────────────────────────────────

    #[test]
    fn data_que_no_es_un_objeto_es_invalid_rpc_call() {
        for malo in [
            json!([1, 2]),
            json!("texto"),
            json!(7),
            json!(null),
            json!(true),
        ] {
            let mut input = request_input();
            input.data = raw_of(&malo);
            let r = build_rpc_request(input);
            assert_eq!(
                code_of(&r).as_deref(),
                Some(INVALID_RPC_CALL_CODE),
                "{malo}"
            );
        }
        let mut input = reply_input();
        input.data = raw_of(&json!([1]));
        assert_eq!(
            code_of(&build_rpc_reply(input)).as_deref(),
            Some(INVALID_RPC_CALL_CODE)
        );
    }

    #[test]
    fn los_atributos_obligatorios_vacios_se_rechazan() {
        for campo in [
            "id",
            "source",
            "correlationid",
            "producerversion",
            "tenantid",
            "dataschema",
        ] {
            let mut input = request_input();
            match campo {
                "id" => input.id.clear(),
                "source" => input.source.clear(),
                "correlationid" => input.correlationid.clear(),
                "producerversion" => input.producerversion.clear(),
                "tenantid" => input.tenant_id.clear(),
                _ => input.dataschema.clear(),
            }
            let r = build_rpc_request(input);
            assert!(matches!(r, Err(FluxError::Envelope(_))), "{campo}: {r:?}");
        }
    }

    #[test]
    fn una_operacion_con_prefijo_o_mal_formada_no_construye_nada() {
        for malo in [
            "rpc.clientes.cliente.v1.obtener",
            "Clientes.cliente.v1.obtener",
        ] {
            let mut input = request_input();
            input.operation = malo.to_string();
            assert!(build_rpc_request(input).is_err(), "{malo}");
        }
    }

    #[test]
    fn el_error_que_se_construye_cumple_su_esquema() {
        type Rompe = fn(&mut RpcErrorBody);
        let casos: [(&str, Rompe); 6] = [
            (
                "class poison",
                // Sin `retryAfterMs`: si no, fallaría por esa otra regla y no por la clase.
                |e| {
                    e.class = ErrorClass::Poison;
                    e.retry_after_ms = None;
                },
            ),
            ("code en minúsculas", |e| e.code = "http_503".into()),
            ("code vacío", |e| e.code.clear()),
            ("message vacío", |e| e.message.clear()),
            ("message de 1001", |e| e.message = "x".repeat(1_001)),
            ("retryAfterMs con permanent", |e| {
                e.class = ErrorClass::Permanent;
                e.retry_after_ms = Some(5);
            }),
        ];
        for (nombre, rompe) in casos {
            let mut input = error_input("rpc-error-handler");
            rompe(&mut input.error);
            let r = build_rpc_error(input);
            assert_eq!(
                code_of(&r).as_deref(),
                Some(INVALID_RPC_CALL_CODE),
                "{nombre}"
            );
        }
        // Los 1000 caracteres justos pasan, y se cuentan en caracteres, no en bytes.
        let mut input = error_input("rpc-error-handler");
        input.error.message = "ñ".repeat(1_000);
        assert!(build_rpc_error(input).is_ok());
    }

    #[test]
    fn el_codigo_estable_sigue_el_patron_de_protocol_json() {
        let patron = json_of(PROTOCOL)["errors"]["codePattern"]
            .as_str()
            .unwrap()
            .to_string();
        let re = regex::Regex::new(&patron).unwrap();
        for c in [
            "HTTP_503",
            "UNKNOWN",
            "PEDIDO_YA_CANCELADO",
            "A",
            "A1",
            "ECONNRESET",
            "RPC_TIMEOUT",
            "X_1",
            "http_503",
            "",
            "_X",
            "X_",
            "X__Y",
            "1X",
            "X-Y",
            "X Y",
            "É",
            "X_y",
        ] {
            assert_eq!(is_stable_code(c), re.is_match(c), "{c:?}");
        }
    }

    // ── Quien llama: las cinco comprobaciones de §4.3 ───────────────────────

    fn peticion_de_ejemplo() -> (RpcOperation, RpcRequestRef) {
        let op = RpcOperation::parse(OPERACION).unwrap();
        let event = build_rpc_request(request_input()).unwrap();
        let request = RpcRequestRef::of(OPERACION, &event);
        (op, request)
    }

    fn llega(event: &Event) -> Incoming {
        Incoming {
            payload: wire(event),
            status: None,
        }
    }

    fn comprueba(incoming: &Incoming) -> Result<Checked, FluxError> {
        let (op, request) = peticion_de_ejemplo();
        check_reply(
            &Expectation {
                operation: &op,
                request: &request,
            },
            incoming,
        )
    }

    fn motivo(r: &Result<Checked, FluxError>) -> String {
        match r {
            Err(FluxError::RpcInvalidReply { reason, .. }) => reason.clone(),
            other => panic!("se esperaba RPC_INVALID_REPLY y fue {other:?}"),
        }
    }

    #[test]
    fn la_respuesta_valida_pasa_las_cinco_comprobaciones() {
        let reply = build_rpc_reply(reply_input()).unwrap();
        assert!(matches!(comprueba(&llega(&reply)), Ok(Checked::Reply(_))));
        let error = build_rpc_error(error_input("rpc-error-handler")).unwrap();
        match comprueba(&llega(&error)) {
            Ok(Checked::Failure(body, _)) => {
                assert_eq!(body.class, ErrorClass::Retryable);
                assert_eq!(body.code, "HTTP_503");
                assert_eq!(body.retry_after_ms, Some(5_000));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn cada_comprobacion_rota_es_rpc_invalid_reply() {
        let buena = build_rpc_reply(reply_input()).unwrap();
        let casos: Vec<(&str, Event, &str)> = vec![
            (
                "type de otra operación",
                {
                    let mut e = buena.clone();
                    e.event_type = "com.flux.rpc.clientes.cliente.obtener.reply.v1".into();
                    e
                },
                "su `type`",
            ),
            (
                "type de la petición, no de la respuesta",
                {
                    let mut e = buena.clone();
                    e.event_type =
                        "com.flux.rpc.clientes.cliente.calcular-descuento.request.v1".into();
                    e
                },
                "su `type`",
            ),
            (
                "causationid de otra petición",
                {
                    let mut e = buena.clone();
                    e.causationid = Some("otra-peticion".into());
                    e
                },
                "causationid",
            ),
            (
                "sin causationid",
                {
                    let mut e = buena.clone();
                    e.causationid = None;
                    e
                },
                "causationid",
            ),
            (
                "correlationid ajeno",
                {
                    let mut e = buena.clone();
                    e.correlationid = "otra-cadena".into();
                    e
                },
                "correlationid",
            ),
            (
                "tenantid ajeno",
                {
                    let mut e = buena.clone();
                    e.tenantid = "globex".into();
                    e
                },
                "tenantid",
            ),
        ];
        for (nombre, event, trozo) in casos {
            let r = comprueba(&llega(&event));
            assert_eq!(
                code_of(&r).as_deref(),
                Some(RPC_INVALID_REPLY_CODE),
                "{nombre}"
            );
            let m = motivo(&r);
            assert!(m.contains(trozo), "{nombre}: {m}");
        }
    }

    #[test]
    fn lo_que_no_es_un_cloudevent_es_rpc_invalid_reply() {
        for cuerpo in [
            &b"no es json"[..],
            b"[]",
            b"{}",
            b"{\"specversion\":\"1.0\"}",
            b"",
        ] {
            let r = comprueba(&Incoming {
                payload: cuerpo.to_vec(),
                status: None,
            });
            assert_eq!(code_of(&r).as_deref(), Some(RPC_INVALID_REPLY_CODE));
            assert!(motivo(&r).contains("no es un CloudEvent válido"));
        }
        // Un mensaje de estado (que no sea el 503, ya traducido) tampoco es una respuesta.
        let r = comprueba(&Incoming {
            payload: Vec::new(),
            status: Some(408),
        });
        assert!(motivo(&r).contains("408"));
    }

    /// El acuse de un stream que captura `rpc.>` (A-18, A-67): el mensaje apunta a `flux doctor`.
    #[test]
    fn el_acuse_de_un_stream_es_rpc_invalid_reply_con_la_pista_del_doctor() {
        let ack = Incoming {
            payload: br#"{"stream":"TRAMPA","seq":1}"#.to_vec(),
            status: None,
        };
        let r = comprueba(&ack);
        let msg = r.unwrap_err().to_string();
        assert!(msg.contains("flux doctor"), "{msg}");
        assert!(msg.contains("rpc.>"), "{msg}");
        // Una respuesta rota que NO parece un acuse no lleva la pista.
        let otra = comprueba(&Incoming {
            payload: b"basura".to_vec(),
            status: None,
        });
        assert!(!otra.unwrap_err().to_string().contains("flux doctor"));
        // Con `duplicate` y `domain` sigue siendo un acuse.
        assert!(looks_like_stream_ack(
            br#"{"stream":"X","seq":3,"duplicate":true,"domain":"hub"}"#
        ));
        assert!(!looks_like_stream_ack(br#"{"stream":1,"seq":3}"#));
        assert!(!looks_like_stream_ack(b"[]"));
    }

    #[test]
    fn un_error_con_un_data_fuera_de_su_esquema_es_rpc_invalid_reply() {
        let base = build_rpc_error(error_input("rpc-error-handler")).unwrap();
        let casos = [
            r#"{"class":"poison","code":"X","message":"m"}"#,
            r#"{"class":"retryable","code":"x","message":"m"}"#,
            r#"{"class":"retryable","code":"X"}"#,
            r#"{"class":"retryable","code":"X","message":""}"#,
            r#"{"class":"permanent","code":"X","message":"m","retryAfterMs":5}"#,
            r#"{"class":"retryable","code":"X","message":"m","retryAfterMs":-1}"#,
            r#"{"class":"retryable","code":"X","message":"m","retryAfterMs":"5"}"#,
            r#"{"class":"retryable","code":"X","message":"m","extra":1}"#,
            r#"{"class":7,"code":"X","message":"m"}"#,
            r"[1]",
        ];
        for data in casos {
            let mut e = base.clone();
            e.data = serde_json::from_str(data).unwrap();
            let r = comprueba(&llega(&e));
            assert_eq!(
                code_of(&r).as_deref(),
                Some(RPC_INVALID_REPLY_CODE),
                "{data}"
            );
            assert!(motivo(&r).contains("esquema"), "{data}");
        }
        // `5000.0` es un entero para JSON Schema.
        let mut e = base.clone();
        e.data = serde_json::from_str(
            r#"{"class":"retryable","code":"X","message":"m","retryAfterMs":5000.0}"#,
        )
        .unwrap();
        assert!(matches!(comprueba(&llega(&e)), Ok(Checked::Failure(_, _))));
    }

    /// El error que contesta quien responde a una petición que **no pudo leer** (§5.2): el que construye
    /// `build_rpc_error` con `ReplyTo::Unreadable`.
    fn error_de_una_peticion_ilegible(request_id: Option<&str>, code: &str) -> Event {
        let mut input = error_input("rpc-error-handler");
        input.reply_to = ReplyTo::Unreadable {
            request_id: request_id.map(str::to_string),
        };
        input.error = RpcErrorBody {
            class: ErrorClass::Permanent,
            code: code.into(),
            message: "la petición no es un CloudEvent válido de flux".into(),
            retry_after_ms: None,
        };
        build_rpc_error(input).unwrap()
    }

    /// EXCEPCIÓN ÚNICA de la comprobación 4 (§4.3): el error de una petición ilegible lleva `correlationid`
    /// propio y `tenantid: system`, y a quien llama le basta la comprobación 3 para atribuirlo.
    #[test]
    fn el_error_de_una_peticion_ilegible_pasa_y_quien_llama_ve_el_codigo_del_sobre() {
        let (_, request) = peticion_de_ejemplo();
        for code in [
            "MALFORMED_JSON",
            "MISSING_REQUIRED_EXTENSION",
            "INVALID_RPC_CALL",
        ] {
            let e = error_de_una_peticion_ilegible(Some(&request.id), code);
            assert_eq!(e.tenantid, "system");
            assert_eq!(e.correlationid, e.id, "correlationid propio");
            match comprueba(&llega(&e)) {
                Ok(Checked::Failure(body, _)) => {
                    assert_eq!(body.code, code);
                    let flux = body.into_flux_error();
                    assert_eq!(flux.class(), Some(ErrorClass::Permanent));
                    assert_eq!(
                        flux.code(),
                        Some(code),
                        "el código del sobre, no RPC_INVALID_REPLY"
                    );
                }
                other => panic!("{code}: se esperaba el error del sobre y fue {other:?}"),
            }
        }
    }

    /// La excepción es ESTRECHA, y las demás comprobaciones siguen en pie.
    #[test]
    fn la_excepcion_de_la_peticion_ilegible_es_estrecha() {
        let (_, request) = peticion_de_ejemplo();
        // Sin `causationid` (o con el de otra petición): la comprobación 3 sigue en pie.
        let sin_causa = error_de_una_peticion_ilegible(None, "MALFORMED_JSON");
        assert_eq!(
            code_of(&comprueba(&llega(&sin_causa))).as_deref(),
            Some(RPC_INVALID_REPLY_CODE)
        );
        let de_otra = error_de_una_peticion_ilegible(Some("otra-peticion"), "MALFORMED_JSON");
        assert!(motivo(&comprueba(&llega(&de_otra))).contains("causationid"));
        // `tenantid: system` pero con un `correlationid` que no es el suyo.
        let mut e = error_de_una_peticion_ilegible(Some(&request.id), "MALFORMED_JSON");
        e.correlationid = "otro-flujo".into();
        assert!(motivo(&comprueba(&llega(&e))).contains("correlationid"));
        // `correlationid` propio pero con un tenant que no es `system` ni el de la petición.
        let mut e = error_de_una_peticion_ilegible(Some(&request.id), "MALFORMED_JSON");
        e.tenantid = "globex".into();
        assert_eq!(
            code_of(&comprueba(&llega(&e))).as_deref(),
            Some(RPC_INVALID_REPLY_CODE)
        );
        // Una RESPUESTA (no un error) con esa forma no es lo que la excepción cubre.
        let mut r = build_rpc_reply(reply_input()).unwrap();
        r.tenantid = "system".into();
        r.correlationid = r.id.clone();
        assert!(
            motivo(&comprueba(&llega(&r))).contains("tenantid")
                || motivo(&comprueba(&llega(&r))).contains("correlationid")
        );
        // La quinta comprobación sigue en pie para ese error: su `data` debe cumplir el esquema.
        let mut e = error_de_una_peticion_ilegible(Some(&request.id), "MALFORMED_JSON");
        e.data = serde_json::from_str(r#"{"class":"poison","code":"X","message":"m"}"#).unwrap();
        assert!(motivo(&comprueba(&llega(&e))).contains("esquema"));
        // CONTROL: un error de una petición LEÍDA sigue heredando.
        let ok = build_rpc_error(error_input("rpc-error-handler")).unwrap();
        assert!(matches!(comprueba(&llega(&ok)), Ok(Checked::Failure(_, _))));
        let mut raro = ok.clone();
        raro.correlationid = "otra-cadena".into();
        assert!(motivo(&comprueba(&llega(&raro))).contains("correlationid"));
    }

    #[test]
    fn el_error_recibido_se_convierte_en_retryable_o_permanent_con_su_codigo() {
        let r = RpcErrorBody {
            class: ErrorClass::Retryable,
            code: "HTTP_503".into(),
            message: "el servicio de tarifas no responde".into(),
            retry_after_ms: Some(5_000),
        }
        .into_flux_error();
        assert_eq!(r.class(), Some(ErrorClass::Retryable));
        assert_eq!(r.code(), Some("HTTP_503"));
        assert_eq!(r.retry_after(), Some(Duration::from_secs(5)));
        assert_eq!(r.to_string(), "el servicio de tarifas no responde");
        let p = RpcErrorBody {
            class: ErrorClass::Permanent,
            code: "PEDIDO_YA_CANCELADO".into(),
            message: "ya cancelado".into(),
            retry_after_ms: None,
        }
        .into_flux_error();
        assert_eq!(p.class(), Some(ErrorClass::Permanent));
        assert_eq!(p.code(), Some("PEDIDO_YA_CANCELADO"));
    }

    // ── Quien responde: el algoritmo de §5.1 con un entorno de mentira ──────

    type Validador = Box<dyn Fn(RpcSide, &RawValue) -> Result<bool, FluxError> + Send + Sync>;

    struct FakeEnv {
        now: DateTime<Utc>,
        ids: Mutex<Vec<String>>,
        traceparent: Option<String>,
        require: bool,
        verify_code: Option<&'static str>,
        validate: Validador,
        max_bytes: usize,
        classifier: Classifier,
        logs: Mutex<Vec<String>>,
        firmados: AtomicUsize,
    }

    impl FakeEnv {
        /// Quien responde en los ejemplos de la spec: `/produccion/crm-api` 2.1.0, a las 10:41:07.329.
        fn nuevo(ahora: &str, id: &str) -> Self {
            Self {
                now: utc(ahora),
                ids: Mutex::new(vec![id.to_string()]),
                traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01".into()),
                require: false,
                verify_code: None,
                validate: Box::new(|_, _| Ok(true)),
                max_bytes: MAX_MESSAGE_BYTES,
                classifier: Classifier::default(),
                logs: Mutex::new(Vec::new()),
                firmados: AtomicUsize::new(0),
            }
        }

        fn ejemplo() -> Self {
            Self::nuevo(
                "2026-08-20T10:41:07.329Z",
                "01924f91-5d0f-7e03-9b6a-1c2d3e4f5a67",
            )
        }

        fn registro(&self) -> Vec<String> {
            lock(&self.logs).clone()
        }
    }

    impl ServeEnv for FakeEnv {
        fn source(&self) -> &'static str {
            "/produccion/crm-api"
        }
        fn producer_version(&self) -> &'static str {
            "2.1.0"
        }
        fn now(&self) -> DateTime<Utc> {
            self.now
        }
        fn new_id(&self) -> String {
            let mut ids = lock(&self.ids);
            if ids.is_empty() {
                Uuid::now_v7().to_string()
            } else {
                ids.remove(0)
            }
        }
        fn traceparent(&self) -> Option<String> {
            self.traceparent.clone()
        }
        fn classify(&self, error: &(dyn StdError + 'static)) -> Classification {
            self.classifier.classify(error)
        }
        fn require_signature(&self) -> bool {
            self.require
        }
        fn verify(&self, _event: &Event) -> Result<(), FluxError> {
            match self.verify_code {
                Some(code) => Err(FluxError::permanent("la firma no verifica").with_code(code)),
                None => Ok(()),
            }
        }
        fn sign(&self, event: Event) -> Result<Event, FluxError> {
            self.firmados.fetch_add(1, Ordering::SeqCst);
            Ok(event)
        }
        fn validate(
            &self,
            _op: &RpcOperation,
            side: RpcSide,
            data: &RawValue,
        ) -> Result<bool, FluxError> {
            (self.validate)(side, data)
        }
        fn max_message_bytes(&self) -> usize {
            self.max_bytes
        }
        fn log(&self, _level: LogLevel, message: &str) {
            lock(&self.logs).push(message.to_string());
        }
    }

    fn cfg() -> ServeConfig {
        let op = RpcOperation::parse(OPERACION).unwrap();
        ServeConfig {
            request_type: op.event_type(RpcSide::Request),
            reply_dataschema: op.default_dataschema(BASE, RpcSide::Reply),
            op,
            classification: DataClassification::Internal,
        }
    }

    fn bytes_de_la_peticion() -> Vec<u8> {
        b64_decode(fixture("rpc-request")["base64"].as_str().unwrap())
    }

    fn datos_de_la_respuesta() -> Value {
        fixture("rpc-reply")["input"]["data"].clone()
    }

    /// Un handler que devuelve siempre lo mismo.
    fn devuelve(v: Value) -> impl RpcHandler {
        move |_: RpcRequest, _: RpcContext| {
            let v = v.clone();
            async move { Ok::<Value, HandlerError>(v) }
        }
    }

    /// Un handler que falla siempre con el error que construye `f`.
    fn falla<F>(f: F) -> impl RpcHandler
    where
        F: Fn() -> HandlerError + Send + Sync + 'static,
    {
        move |_: RpcRequest, _: RpcContext| {
            let e = f();
            async move { Err::<Value, HandlerError>(e) }
        }
    }

    async fn atiende(env: &FakeEnv, handler: &dyn RpcHandler, payload: &[u8]) -> Served {
        serve_request(env, &cfg(), handler, payload, true).await
    }

    /// El error que viaja, interpretado: la clase, el código, el mensaje y el sobre.
    fn error_de(served: &Served) -> (RpcErrorBody, Event) {
        let bytes = served.reply.as_ref().expect("se contesta SIEMPRE");
        let event = parse_event(bytes).expect("lo que se contesta es un CloudEvent válido");
        assert_eq!(event.event_type, RPC_ERROR_TYPE, "{event:?}");
        let body = RpcErrorBody::from_data(event.data.get()).expect("cumple su esquema");
        (body, event)
    }

    #[tokio::test]
    async fn atender_la_peticion_del_ejemplo_produce_los_bytes_de_la_respuesta_de_la_spec() {
        let env = FakeEnv::ejemplo();
        let handler = devuelve(datos_de_la_respuesta());
        let served = atiende(&env, &handler, &bytes_de_la_peticion()).await;
        assert_eq!(served.outcome, RpcServerOutcome::Ok);
        assert_fixture_bytes("rpc-reply", served.reply.as_ref().unwrap());
        assert_eq!(
            env.firmados.load(Ordering::SeqCst),
            1,
            "la respuesta se firma si procede"
        );
    }

    #[tokio::test]
    async fn un_retryable_del_handler_produce_los_bytes_del_error_de_la_spec() {
        let env = FakeEnv::nuevo(
            "2026-08-20T10:41:07.331Z",
            "01924f91-5d11-7a62-8f0d-4e5f6a7b8c91",
        );
        let handler = falla(|| {
            Box::new(
                FluxError::retryable("el servicio de tarifas no responde")
                    .with_code("HTTP_503")
                    .with_retry_after(Duration::from_secs(5)),
            )
        });
        let served = atiende(&env, &handler, &bytes_de_la_peticion()).await;
        assert_eq!(served.outcome, RpcServerOutcome::Retryable);
        assert_fixture_bytes("rpc-error-handler", served.reply.as_ref().unwrap());
    }

    #[tokio::test]
    async fn una_peticion_fuera_de_su_esquema_produce_los_bytes_del_error_del_sdk_y_no_invoca_al_handler(
    ) {
        let mut env = FakeEnv::nuevo(
            "2026-08-20T10:41:07.333Z",
            "01924f91-5d13-7b85-a3e1-6a7b8c9d0e22",
        );
        env.validate = Box::new(|side, _| {
            match side {
            RpcSide::Request => Err(FluxError::SchemaValidation(SchemaValidationError {
                subject: OPERACION.into(),
                dataschema: "https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json".into(),
                errors: vec![
                    "/totalCents debe ser un entero".into(),
                    "/moneda es obligatoria".into(),
                ],
            })),
            RpcSide::Reply => Ok(true),
        }
        });
        let llamado = Arc::new(AtomicUsize::new(0));
        let l2 = Arc::clone(&llamado);
        let handler = move |_: RpcRequest, _: RpcContext| {
            l2.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<Value, HandlerError>(json!({})) }
        };
        let served = atiende(&env, &handler, &bytes_de_la_peticion()).await;
        assert_eq!(served.outcome, RpcServerOutcome::InvalidSchema);
        assert_eq!(llamado.load(Ordering::SeqCst), 0, "el handler NO se invoca");
        assert_fixture_bytes("rpc-error-sdk", served.reply.as_ref().unwrap());
    }

    /// Con `warn` el handler corre, y el fallo se cuenta igual.
    #[tokio::test]
    async fn con_warn_la_peticion_invalida_sigue_y_se_cuenta_como_invalid_schema() {
        let mut env = FakeEnv::ejemplo();
        env.validate = Box::new(|_, _| Ok(false));
        let served = atiende(
            &env,
            &devuelve(datos_de_la_respuesta()),
            &bytes_de_la_peticion(),
        )
        .await;
        assert_eq!(served.outcome, RpcServerOutcome::InvalidSchema);
        // Y SE contesta con la respuesta del handler, no con un error.
        let event = parse_event(served.reply.as_ref().unwrap()).unwrap();
        assert_eq!(
            event.event_type,
            "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1"
        );
    }

    // ── Lo que no se puede leer ─────────────────────────────────────────────

    #[tokio::test]
    async fn una_peticion_sin_subject_de_respuesta_no_se_atiende_y_es_poison() {
        let env = FakeEnv::ejemplo();
        let served = serve_request(
            &env,
            &cfg(),
            &devuelve(json!({})),
            &bytes_de_la_peticion(),
            false,
        )
        .await;
        assert_eq!(served.outcome, RpcServerOutcome::Poison);
        assert!(
            served.reply.is_none(),
            "nadie espera el resultado de una orden"
        );
        assert!(env
            .registro()
            .iter()
            .any(|l| l.contains("SIN subject de respuesta")));
    }

    #[tokio::test]
    async fn una_peticion_ilegible_se_contesta_con_un_error_de_plataforma() {
        let env = FakeEnv::ejemplo();
        for (cuerpo, causation) in [
            (&b"esto no es json"[..], None),
            (b"[1,2]", None),
            // JSON legible con un `id` válido pero sin las extensiones: el `id` se rescata.
            (br#"{"specversion":"1.0","id":"peti-7"}"#, Some("peti-7")),
            // Un `id` que no es una cadena, vacío, largo o con caracteres de control NO se rescata.
            (br#"{"id":42}"#, None),
            (br#"{"id":""}"#, None),
            (br#"{"id":"a\nb"}"#, None),
        ] {
            let served = atiende(&env, &devuelve(json!({})), cuerpo).await;
            assert_eq!(served.outcome, RpcServerOutcome::Poison);
            let (body, event) = error_de(&served);
            assert_eq!(body.class, ErrorClass::Permanent, "poison NO viaja");
            // Sin petición que heredar: `correlationid` = su propio `id`, `tenantid` = `system`.
            assert_eq!(event.correlationid, event.id);
            assert_eq!(event.tenantid, "system");
            assert_eq!(
                event.causationid.as_deref(),
                causation,
                "{}",
                String::from_utf8_lossy(cuerpo)
            );
            assert_eq!(event.dataclassification, DataClassification::Internal);
        }
        // Un `id` de más de 128 caracteres tampoco.
        let largo = format!(r#"{{"id":"{}"}}"#, "x".repeat(129));
        let (_, event) = error_de(&atiende(&env, &devuelve(json!({})), largo.as_bytes()).await);
        assert_eq!(event.causationid, None);
    }

    #[tokio::test]
    async fn el_error_de_un_sobre_roto_lleva_el_codigo_del_sobre() {
        let env = FakeEnv::ejemplo();
        let mut roto = parse_event(&bytes_de_la_peticion()).unwrap();
        roto.tenantid.clear();
        let served = atiende(&env, &devuelve(json!({})), &wire(&roto)).await;
        let (body, _) = error_de(&served);
        assert_eq!(body.code, "MISSING_REQUIRED_EXTENSION");
        assert_eq!(served.outcome, RpcServerOutcome::Poison);
        let served = atiende(&env, &devuelve(json!({})), b"{ roto").await;
        assert_eq!(error_de(&served).0.code, "MALFORMED_JSON");
    }

    #[tokio::test]
    async fn un_type_que_no_es_el_de_la_operacion_es_invalid_rpc_call_y_hereda() {
        let env = FakeEnv::ejemplo();
        let mut otra = parse_event(&bytes_de_la_peticion()).unwrap();
        otra.event_type = "com.flux.rpc.clientes.cliente.obtener.request.v1".into();
        let served = atiende(&env, &devuelve(json!({})), &wire(&otra)).await;
        assert_eq!(served.outcome, RpcServerOutcome::Poison);
        let (body, event) = error_de(&served);
        assert_eq!(body.code, INVALID_RPC_CALL_CODE);
        // Es legible: el error hereda, no cae en el `system` de las ilegibles.
        assert_eq!(event.tenantid, "acme");
        assert_eq!(event.causationid.as_deref(), Some(otra.id.as_str()));
        assert!(
            body.message.contains(OPERACION),
            "nombra la operación: {}",
            body.message
        );
    }

    // ── La firma y la ventana de 300 s ──────────────────────────────────────

    fn peticion_con_hora(hora: &str) -> Vec<u8> {
        let mut e = parse_event(&bytes_de_la_peticion()).unwrap();
        e.time = hora.to_string();
        wire(&e)
    }

    #[tokio::test]
    async fn una_firma_rechazada_es_invalid_signature_y_el_error_lleva_su_codigo() {
        let mut env = FakeEnv::ejemplo();
        for code in [
            "MISSING_SIGNATURE",
            "INVALID_SIGNATURE",
            "UNKNOWN_SIGNING_KEY",
        ] {
            env.verify_code = Some(code);
            let served = atiende(&env, &devuelve(json!({})), &bytes_de_la_peticion()).await;
            assert_eq!(served.outcome, RpcServerOutcome::InvalidSignature, "{code}");
            let (body, _) = error_de(&served);
            assert_eq!(body.code, code);
            assert_eq!(body.class, ErrorClass::Permanent);
        }
    }

    /// La ventana es de 300 s, simétrica, y solo con la firma en `require` — §7.4.
    #[tokio::test]
    async fn la_ventana_de_300_s_es_simetrica_y_solo_con_require() {
        let mut env = FakeEnv::nuevo(
            "2026-08-20T10:46:07.318Z",
            "01924f91-5d0f-7e03-9b6a-1c2d3e4f5a67",
        );
        env.require = true;
        let handler = devuelve(datos_de_la_respuesta());
        // La petición es de 10:41:07.318: justo 300 s detrás → se acepta.
        let ok = atiende(&env, &handler, &bytes_de_la_peticion()).await;
        assert_eq!(
            ok.outcome,
            RpcServerOutcome::Ok,
            "300 s exactos están dentro"
        );
        // 1 ms más → STALE_REQUEST.
        env.now = utc("2026-08-20T10:46:07.319Z");
        let tarde = atiende(&env, &handler, &bytes_de_la_peticion()).await;
        assert_eq!(tarde.outcome, RpcServerOutcome::StaleRequest);
        let (body, _) = error_de(&tarde);
        assert_eq!(body.code, STALE_REQUEST_CODE);
        assert_eq!(body.class, ErrorClass::Permanent);
        // El mensaje nombra el `time`, la ventana y la hora local.
        assert!(
            body.message.contains("2026-08-20T10:41:07.318Z"),
            "{}",
            body.message
        );
        assert!(body.message.contains("300 s"), "{}", body.message);
        assert!(
            body.message.contains("2026-08-20T10:46:07.319Z"),
            "{}",
            body.message
        );
        // Por DELANTE también: un reloj adelantado es tan sospechoso como una petición vieja.
        env.now = utc("2026-08-20T10:36:07.317Z");
        assert_eq!(
            atiende(&env, &handler, &bytes_de_la_peticion())
                .await
                .outcome,
            RpcServerOutcome::StaleRequest
        );
        env.now = utc("2026-08-20T10:36:07.318Z");
        assert_eq!(
            atiende(&env, &handler, &bytes_de_la_peticion())
                .await
                .outcome,
            RpcServerOutcome::Ok
        );
        // Un `time` que no se puede interpretar no se puede comparar: se rechaza.
        let ilegible = atiende(&env, &handler, &peticion_con_hora("ayer por la tarde")).await;
        assert_eq!(ilegible.outcome, RpcServerOutcome::StaleRequest);
        // Sin `require` no hay ventana: un `time` es un campo que cualquiera escribe.
        env.require = false;
        env.now = utc("2030-01-01T00:00:00.000Z");
        assert_eq!(
            atiende(&env, &handler, &bytes_de_la_peticion())
                .await
                .outcome,
            RpcServerOutcome::Ok
        );
    }

    // ── Los errores del handler: §5.3 ───────────────────────────────────────

    #[derive(Debug, thiserror::Error)]
    #[error("fallo con el dato secreto ana@example.com")]
    struct ErrorConDato;

    #[tokio::test]
    async fn un_error_desconocido_es_permanent_unknown_y_no_reenvia_la_excepcion() {
        let env = FakeEnv::ejemplo();
        let served = atiende(
            &env,
            &falla(|| Box::new(ErrorConDato)),
            &bytes_de_la_peticion(),
        )
        .await;
        assert_eq!(served.outcome, RpcServerOutcome::Permanent);
        let (body, _) = error_de(&served);
        assert_eq!(
            body.class,
            ErrorClass::Permanent,
            "lo desconocido es permanent, no retryable acotado"
        );
        assert_eq!(body.code, "UNKNOWN");
        assert_eq!(body.message, "error interno del servicio");
        // El texto de la excepción —que puede llevar datos personales— no viaja, pero SÍ va al log con
        // el `id` de la petición para encontrarlo.
        let texto = String::from_utf8_lossy(served.reply.as_ref().unwrap()).to_string();
        assert!(!texto.contains("ana@example.com"), "{texto}");
        let log = env.registro().join("\n");
        assert!(log.contains("ana@example.com"), "{log}");
        assert!(
            log.contains("01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10"),
            "el id de la petición: {log}"
        );
    }

    /// La política configurable de §5.3: `permanent` y `retryable-bounded` dan `permanent`; `retryable`
    /// da `retryable`.
    #[tokio::test]
    async fn la_politica_de_lo_desconocido_se_aplica_asi() {
        for (politica, clase) in [
            (UnknownPolicy::RetryableBounded, ErrorClass::Permanent),
            (UnknownPolicy::Permanent, ErrorClass::Permanent),
            (UnknownPolicy::Retryable, ErrorClass::Retryable),
        ] {
            let mut env = FakeEnv::ejemplo();
            env.classifier = Classifier::new(ClassifierOptions {
                unknown_policy: politica,
                ..ClassifierOptions::default()
            });
            let served = atiende(
                &env,
                &falla(|| Box::new(ErrorConDato)),
                &bytes_de_la_peticion(),
            )
            .await;
            let (body, _) = error_de(&served);
            assert_eq!(body.class, clase, "{politica:?}");
            assert_eq!(body.code, "UNKNOWN");
            assert_eq!(body.retry_after_ms, None);
        }
    }

    #[tokio::test]
    async fn un_error_que_el_clasificador_reconoce_conserva_su_clase_con_mensaje_generico() {
        let env = FakeEnv::ejemplo();
        let secreto = || {
            Box::new(
                HttpError::new(503, "POST /v1/tarifas?email=ana@example.com")
                    .with_retry_after(Duration::from_secs(2)),
            ) as HandlerError
        };
        let (body, _) = error_de(&atiende(&env, &falla(secreto), &bytes_de_la_peticion()).await);
        assert_eq!(body.class, ErrorClass::Retryable);
        assert_eq!(body.code, "HTTP_503");
        assert_eq!(body.message, "dependencia no disponible");
        assert_eq!(body.retry_after_ms, Some(2_000));
        let (body, _) = error_de(
            &atiende(
                &env,
                &falla(|| Box::new(HttpError::new(404, "GET /x")) as HandlerError),
                &bytes_de_la_peticion(),
            )
            .await,
        );
        assert_eq!(body.class, ErrorClass::Permanent);
        assert_eq!(body.code, "HTTP_404");
        assert_eq!(body.retry_after_ms, None);
        let (body, _) = error_de(
            &atiende(
                &env,
                &falla(|| {
                    Box::new(std::io::Error::from(std::io::ErrorKind::ConnectionReset))
                        as HandlerError
                }),
                &bytes_de_la_peticion(),
            )
            .await,
        );
        assert_eq!(
            (body.class, body.code.as_str()),
            (ErrorClass::Retryable, "ECONNRESET")
        );
    }

    #[tokio::test]
    async fn el_error_de_la_aplicacion_conserva_codigo_y_mensaje_y_tiene_codigos_por_defecto() {
        let env = FakeEnv::ejemplo();
        let (body, _) = error_de(
            &atiende(
                &env,
                &falla(|| {
                    Box::new(
                        FluxError::permanent("el pedido ya está cancelado")
                            .with_code("PEDIDO_YA_CANCELADO"),
                    )
                }),
                &bytes_de_la_peticion(),
            )
            .await,
        );
        assert_eq!(
            (body.class, body.code.as_str(), body.message.as_str()),
            (
                ErrorClass::Permanent,
                "PEDIDO_YA_CANCELADO",
                "el pedido ya está cancelado"
            )
        );
        // Sin código: `PERMANENT` y `RETRYABLE` — los nombres de la clase, que sí cumplen el patrón.
        let (body, _) = error_de(
            &atiende(
                &env,
                &falla(|| Box::new(FluxError::permanent("no"))),
                &bytes_de_la_peticion(),
            )
            .await,
        );
        assert_eq!(body.code, "PERMANENT");
        let (body, _) = error_de(
            &atiende(
                &env,
                &falla(|| Box::new(FluxError::retryable("luego"))),
                &bytes_de_la_peticion(),
            )
            .await,
        );
        assert_eq!(
            (body.class, body.code.as_str()),
            (ErrorClass::Retryable, "RETRYABLE")
        );
        // `poison` no viaja: es `permanent`.
        let (body, _) = error_de(
            &atiende(
                &env,
                &falla(|| Box::new(FluxError::poison("ilegible"))),
                &bytes_de_la_peticion(),
            )
            .await,
        );
        assert_eq!(body.class, ErrorClass::Permanent);
    }

    /// Un código que no cumple el patrón, o un mensaje de más de 1000 caracteres, no pueden hacer que
    /// quien llama vea un incidente (`RPC_INVALID_REPLY`) por un descuido de la aplicación.
    #[tokio::test]
    async fn un_codigo_o_un_mensaje_fuera_del_esquema_se_normalizan() {
        let env = FakeEnv::ejemplo();
        let (body, _) = error_de(
            &atiende(
                &env,
                &falla(|| Box::new(FluxError::retryable("m".repeat(2_000)).with_code("http-503"))),
                &bytes_de_la_peticion(),
            )
            .await,
        );
        assert_eq!(body.code, "RETRYABLE");
        assert_eq!(body.message.chars().count(), 1_000);
        assert!(body.message.ends_with('…'));
        let (body, _) = error_de(
            &atiende(
                &env,
                &falla(|| Box::new(FluxError::permanent("  "))),
                &bytes_de_la_peticion(),
            )
            .await,
        );
        assert_eq!(body.message, "error del servicio");
        // `retryAfterMs` en un permanent se descarta.
        let normal = RpcErrorBody {
            class: ErrorClass::Permanent,
            code: "X".into(),
            message: "m".into(),
            retry_after_ms: Some(5),
        }
        .normalized();
        assert_eq!(normal.retry_after_ms, None);
    }

    #[tokio::test]
    async fn un_error_del_propio_sdk_en_el_handler_viaja_con_su_clase_y_sin_su_texto() {
        // El handler llamó a otra operación y esa no tenía a nadie: `?` propaga un RPC_UNAVAILABLE.
        let env = FakeEnv::ejemplo();
        let (body, _) = error_de(
            &atiende(
                &env,
                &falla(|| {
                    Box::new(FluxError::RpcUnavailable {
                        operation: "interna.secreta.v1.hacer".into(),
                    })
                }),
                &bytes_de_la_peticion(),
            )
            .await,
        );
        assert_eq!(
            (body.class, body.code.as_str(), body.message.as_str()),
            (
                ErrorClass::Retryable,
                "RPC_UNAVAILABLE",
                "dependencia no disponible"
            )
        );
        assert!(
            !body.message.contains("secreta"),
            "el texto describe la topología interna"
        );
    }

    #[tokio::test]
    async fn un_panico_en_el_handler_es_un_error_desconocido_y_no_mata_al_responder() {
        fn explota() -> Result<Value, HandlerError> {
            panic!("bug con el dato ana@example.com")
        }
        let env = FakeEnv::ejemplo();
        let handler = |_: RpcRequest, _: RpcContext| async move { explota() };
        let served = atiende(&env, &handler, &bytes_de_la_peticion()).await;
        let (body, _) = error_de(&served);
        assert_eq!(
            (body.class, body.code.as_str()),
            (ErrorClass::Permanent, "UNKNOWN")
        );
        assert!(!String::from_utf8_lossy(served.reply.as_ref().unwrap()).contains("ana@"));
        assert!(env.registro().iter().any(|l| l.contains("pánico")));
    }

    // ── La respuesta del handler ────────────────────────────────────────────

    #[tokio::test]
    async fn una_respuesta_que_no_es_un_objeto_es_invalid_schema() {
        let env = FakeEnv::ejemplo();
        for malo in [json!([1]), json!("texto"), json!(7), json!(null)] {
            let served = atiende(&env, &devuelve(malo.clone()), &bytes_de_la_peticion()).await;
            assert_eq!(served.outcome, RpcServerOutcome::InvalidSchema, "{malo}");
            let (body, _) = error_de(&served);
            assert_eq!(body.code, "INVALID_SCHEMA");
            assert_eq!(body.message, "la respuesta no es un objeto");
        }
    }

    /// «La respuesta no cumple…» y «la petición no cumple…» son dos mensajes distintos: quien llama recibe
    /// el mismo `INVALID_SCHEMA` y debe saber a quién arreglar.
    #[tokio::test]
    async fn el_mensaje_de_esquema_dice_de_quien_es_el_fallo() {
        let mut env = FakeEnv::ejemplo();
        env.validate = Box::new(|side, _| match side {
            RpcSide::Request => Ok(true),
            RpcSide::Reply => Err(FluxError::SchemaValidation(SchemaValidationError {
                subject: OPERACION.into(),
                dataschema: "https://schemas.internal/x/reply/1.0.0.json".into(),
                errors: vec!["/descuentoCents incumple la regla `type`".into()],
            })),
        });
        let served = atiende(
            &env,
            &devuelve(datos_de_la_respuesta()),
            &bytes_de_la_peticion(),
        )
        .await;
        assert_eq!(served.outcome, RpcServerOutcome::InvalidSchema);
        let (body, _) = error_de(&served);
        assert!(
            body.message.starts_with("la respuesta no cumple"),
            "{}",
            body.message
        );
        assert!(
            body.message.contains("/descuentoCents"),
            "cita rutas: {}",
            body.message
        );
        assert_eq!(
            body.class,
            ErrorClass::Permanent,
            "el fallo es de quien responde y no se arregla reintentando"
        );
    }

    /// Una respuesta que no cabe se contesta con un error: si se intentara publicar, `async-nats` lanzaría en
    /// quien responde y quien llama solo vería un timeout (A-65).
    #[tokio::test]
    async fn una_respuesta_que_no_cabe_se_contesta_con_rpc_message_too_large() {
        let mut env = FakeEnv::ejemplo();
        env.max_bytes = 600; // la respuesta de la spec ocupa 703
        let served = atiende(
            &env,
            &devuelve(datos_de_la_respuesta()),
            &bytes_de_la_peticion(),
        )
        .await;
        assert_eq!(served.outcome, RpcServerOutcome::Permanent);
        let (body, _) = error_de(&served);
        assert_eq!(body.code, RPC_MESSAGE_TOO_LARGE_CODE);
        assert_eq!(body.class, ErrorClass::Permanent);
        // Dice los bytes y el límite, y apunta al claim-check.
        assert!(body.message.contains("703"), "{}", body.message);
        assert!(body.message.contains("600"), "{}", body.message);
        assert!(body.message.contains("claim-check"), "{}", body.message);
        // El error cabe; la respuesta justo en el límite también.
        env.max_bytes = 703;
        env.ids = Mutex::new(vec!["01924f91-5d0f-7e03-9b6a-1c2d3e4f5a67".into()]);
        assert_eq!(
            atiende(
                &env,
                &devuelve(datos_de_la_respuesta()),
                &bytes_de_la_peticion()
            )
            .await
            .outcome,
            RpcServerOutcome::Ok
        );
    }

    /// El handler corre DENTRO del contexto de la petición: lo que llame o publique desde él hereda la
    /// cadena — §5.1.
    #[tokio::test]
    async fn el_handler_continua_la_cadena_de_la_peticion() {
        let env = FakeEnv::ejemplo();
        let visto = Arc::new(Mutex::new(None));
        let v2 = Arc::clone(&visto);
        let handler = move |peticion: RpcRequest, ctx: RpcContext| {
            let v2 = Arc::clone(&v2);
            async move {
                let heredado = context::current();
                let parametros: Value = peticion.data_as()?;
                *lock(&v2) = Some((ctx, heredado, parametros));
                Ok::<Value, HandlerError>(json!({ "ok": true }))
            }
        };
        let served = atiende(&env, &handler, &bytes_de_la_peticion()).await;
        assert_eq!(served.outcome, RpcServerOutcome::Ok);
        let (ctx, heredado, parametros) = lock(&visto).take().unwrap();
        assert_eq!(ctx.id, "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10");
        assert_eq!(ctx.tenant_id, "acme");
        assert_eq!(ctx.aggregate_id.as_deref(), Some("cli-1"));
        assert_eq!(ctx.operation, OPERACION);
        assert_eq!(ctx.time, Some(utc("2026-08-20T10:41:07.318Z")));
        assert_eq!(
            ctx.event.event_type,
            "com.flux.rpc.clientes.cliente.calcular-descuento.request.v1"
        );
        assert_eq!(parametros["campana"], "Otoño");
        let heredado = heredado.expect("hay un contexto dentro del handler");
        assert_eq!(heredado.correlation_id, ctx.correlation_id);
        assert_eq!(
            heredado.causation_id, ctx.id,
            "el causationid de lo que haga es el id de la petición"
        );
        assert_eq!(heredado.tenant_id, "acme");
    }

    #[tokio::test]
    async fn la_respuesta_hereda_la_traza_de_la_peticion_si_quien_responde_no_tiene_una() {
        let mut env = FakeEnv::ejemplo();
        env.traceparent = None;
        let served = atiende(
            &env,
            &devuelve(datos_de_la_respuesta()),
            &bytes_de_la_peticion(),
        )
        .await;
        let event = parse_event(served.reply.as_ref().unwrap()).unwrap();
        assert_eq!(
            event.traceparent.as_deref(),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            "la misma traza, que es lo que necesita un seguimiento extremo a extremo"
        );
        // Y cada respuesta tiene un `id` NUEVO: nunca el de la petición.
        assert_ne!(event.id, event.causationid.clone().unwrap());
    }

    // ── Quien llama: el transporte y los reintentos ─────────────────────────

    struct FakeIo {
        guion: Mutex<Vec<Result<Incoming, FluxError>>>,
        vistos: Mutex<Vec<Vec<u8>>>,
        timeouts: Mutex<Vec<Duration>>,
    }

    impl FakeIo {
        fn con(guion: Vec<Result<Incoming, FluxError>>) -> Self {
            Self {
                guion: Mutex::new(guion),
                vistos: Mutex::new(Vec::new()),
                timeouts: Mutex::new(Vec::new()),
            }
        }
    }

    impl RequestIo for FakeIo {
        fn send(
            &self,
            payload: &[u8],
            timeout: Duration,
        ) -> impl Future<Output = Result<Incoming, FluxError>> + Send {
            lock(&self.vistos).push(payload.to_vec());
            lock(&self.timeouts).push(timeout);
            let siguiente = {
                let mut g = lock(&self.guion);
                if g.is_empty() {
                    Err(FluxError::RpcTimeout {
                        operation: OPERACION.into(),
                        timeout_ms: 1,
                    })
                } else {
                    g.remove(0)
                }
            };
            std::future::ready(siguiente)
        }
    }

    fn sin_nadie() -> Result<Incoming, FluxError> {
        Err(FluxError::RpcUnavailable {
            operation: OPERACION.into(),
        })
    }

    fn sin_tiempo() -> Result<Incoming, FluxError> {
        Err(FluxError::RpcTimeout {
            operation: OPERACION.into(),
            timeout_ms: 2_000,
        })
    }

    #[allow(clippy::unnecessary_wraps)]
    fn una_respuesta() -> Result<Incoming, FluxError> {
        Ok(Incoming {
            payload: b"ok".to_vec(),
            status: None,
        })
    }

    const SIN_PAUSA: Duration = Duration::from_millis(1);

    #[tokio::test]
    async fn por_defecto_se_hace_un_solo_intento() {
        let io = FakeIo::con(vec![sin_nadie(), una_respuesta()]);
        let r = send_with_retries(&io, b"bytes", Duration::from_secs(1), 0, SIN_PAUSA).await;
        assert!(matches!(r, Err(FluxError::RpcUnavailable { .. })));
        assert_eq!(lock(&io.vistos).len(), 1, "`request` hace UN intento");
    }

    /// §4.5: cada reintento reutiliza el MISMO `id` y los MISMOS bytes —`time` incluido—.
    #[tokio::test]
    async fn los_reintentos_reutilizan_los_mismos_bytes() {
        let io = FakeIo::con(vec![sin_nadie(), sin_tiempo(), una_respuesta()]);
        let payload = wire(&build_rpc_request(request_input()).unwrap());
        let r = send_with_retries(&io, &payload, Duration::from_millis(250), 2, SIN_PAUSA).await;
        assert!(r.is_ok());
        let vistos = lock(&io.vistos);
        assert_eq!(vistos.len(), 3);
        assert!(
            vistos.iter().all(|v| *v == payload),
            "mismo id, mismo time, mismos bytes"
        );
        assert!(
            lock(&io.timeouts)
                .iter()
                .all(|t| *t == Duration::from_millis(250)),
            "el timeout es POR intento"
        );
    }

    #[tokio::test]
    async fn se_agotan_los_reintentos_y_se_devuelve_el_ultimo_error() {
        let io = FakeIo::con(vec![
            sin_tiempo(),
            sin_nadie(),
            sin_tiempo(),
            una_respuesta(),
        ]);
        let r = send_with_retries(&io, b"x", Duration::from_secs(1), 2, SIN_PAUSA).await;
        assert!(matches!(r, Err(FluxError::RpcTimeout { .. })), "{r:?}");
        assert_eq!(lock(&io.vistos).len(), 3, "1 intento + 2 reintentos");
    }

    /// Nunca se reintenta lo que reintentar no arregla.
    #[tokio::test]
    async fn no_se_reintenta_un_error_permanente_ni_una_denegacion() {
        for error in [
            FluxError::PermissionDenied {
                operation: "llamar".into(),
                denied: "Publish to \"rpc.x\"".into(),
            },
            FluxError::RpcMessageTooLarge {
                what: "la petición".into(),
                bytes: 2,
                limit: 1,
            },
            FluxError::RpcInvalidReply {
                operation: OPERACION.into(),
                reason: "x".into(),
                stream_ack: false,
            },
            FluxError::permanent("no"),
        ] {
            let io = FakeIo::con(vec![Err(error), una_respuesta()]);
            let r = send_with_retries(&io, b"x", Duration::from_secs(1), 3, SIN_PAUSA).await;
            assert!(r.is_err());
            assert_eq!(lock(&io.vistos).len(), 1);
        }
    }

    #[tokio::test]
    async fn la_pausa_entre_reintentos_crece_con_el_intento() {
        let io = FakeIo::con(vec![sin_nadie(), sin_nadie(), sin_nadie()]);
        let inicio = Instant::now();
        let _ = send_with_retries(
            &io,
            b"x",
            Duration::from_secs(1),
            2,
            Duration::from_millis(30),
        )
        .await;
        // 30 ms + 60 ms entre los tres intentos.
        assert!(
            inicio.elapsed() >= Duration::from_millis(90),
            "{:?}",
            inicio.elapsed()
        );
    }

    // ── Métricas: los literales son los de protocol.json ────────────────────

    #[test]
    fn los_outcomes_de_las_metricas_son_los_de_protocol_json() {
        let metricas = &json_of(PROTOCOL)["rpc"]["metrics"];
        let literales = |v: &Value| -> Vec<String> {
            v.as_array()
                .unwrap()
                .iter()
                .map(|x| x.as_str().unwrap().to_string())
                .collect()
        };
        let cliente: Vec<String> = [
            RpcClientOutcome::Ok,
            RpcClientOutcome::Unavailable,
            RpcClientOutcome::Timeout,
            RpcClientOutcome::Retryable,
            RpcClientOutcome::Permanent,
            RpcClientOutcome::InvalidSchema,
            RpcClientOutcome::InvalidReply,
        ]
        .iter()
        .map(|o| o.as_str().to_string())
        .collect();
        assert_eq!(
            cliente,
            literales(&metricas["flux_rpc_client_requests_total"]["outcomes"])
        );
        let servidor: Vec<String> = [
            RpcServerOutcome::Ok,
            RpcServerOutcome::Retryable,
            RpcServerOutcome::Permanent,
            RpcServerOutcome::Poison,
            RpcServerOutcome::InvalidSchema,
            RpcServerOutcome::InvalidSignature,
            RpcServerOutcome::StaleRequest,
        ]
        .iter()
        .map(|o| o.as_str().to_string())
        .collect();
        assert_eq!(
            servidor,
            literales(&metricas["flux_rpc_server_requests_total"]["outcomes"])
        );
        let roles: Vec<String> = [RpcRole::Client, RpcRole::Server]
            .iter()
            .map(|r| r.as_str().to_string())
            .collect();
        assert_eq!(
            roles,
            literales(&metricas["flux_rpc_duration_seconds"]["roles"])
        );
        // Las etiquetas de cada métrica son exactamente dos y ninguna es una de las prohibidas.
        for nombre in [
            "flux_rpc_client_requests_total",
            "flux_rpc_server_requests_total",
            "flux_rpc_duration_seconds",
        ] {
            let etiquetas = literales(&metricas[nombre]["labels"]);
            assert_eq!(etiquetas.len(), 2, "{nombre}");
            for prohibida in literales(&metricas["forbiddenLabels"]) {
                assert!(
                    !etiquetas.contains(&prohibida),
                    "{nombre} no puede llevar `{prohibida}`"
                );
            }
        }
    }

    #[test]
    fn el_outcome_de_cada_error_de_quien_llama() {
        use RpcClientOutcome as O;
        let casos: Vec<(FluxError, Option<O>)> = vec![
            (
                FluxError::RpcUnavailable {
                    operation: "a".into(),
                },
                Some(O::Unavailable),
            ),
            (
                FluxError::RpcTimeout {
                    operation: "a".into(),
                    timeout_ms: 1,
                },
                Some(O::Timeout),
            ),
            (
                FluxError::RpcInvalidReply {
                    operation: "a".into(),
                    reason: "x".into(),
                    stream_ack: true,
                },
                Some(O::InvalidReply),
            ),
            (FluxError::retryable("x"), Some(O::Retryable)),
            (FluxError::permanent("x"), Some(O::Permanent)),
            (
                FluxError::PermissionDenied {
                    operation: "x".into(),
                    denied: "y".into(),
                },
                Some(O::Permanent),
            ),
            (
                FluxError::RpcMessageTooLarge {
                    what: "x".into(),
                    bytes: 1,
                    limit: 0,
                },
                Some(O::Permanent),
            ),
            (
                FluxError::StaleRequest {
                    time: "t".into(),
                    window_secs: 300,
                    now: "n".into(),
                },
                Some(O::Permanent),
            ),
            (
                FluxError::SchemaValidation(SchemaValidationError {
                    subject: "s".into(),
                    dataschema: "d".into(),
                    errors: vec![],
                }),
                Some(O::InvalidSchema),
            ),
            // Los errores de programación NO se cuentan: sería una serie por cada error de tipeo.
            (FluxError::InvalidRpcCall { reason: "x".into() }, None),
            (
                FluxError::ReservedDomain {
                    domain: "rpc".into(),
                    subject: "x".into(),
                },
                None,
            ),
        ];
        for (error, esperado) in casos {
            assert_eq!(client_outcome_of(&error), esperado, "{error:?}");
        }
    }

    // ── Los seis códigos: clase, texto y constantes ─────────────────────────

    /// Los seis códigos de 04-errors.md §7, con su clase, por su literal —que es lo que
    /// `scripts/check-sdk-codes.mjs` busca—.
    #[test]
    fn los_seis_codigos_estables_con_su_clase() {
        let casos: [(FluxError, &str, ErrorClass); 6] = [
            (
                FluxError::RpcUnavailable {
                    operation: OPERACION.into(),
                },
                "RPC_UNAVAILABLE",
                ErrorClass::Retryable,
            ),
            (
                FluxError::RpcTimeout {
                    operation: OPERACION.into(),
                    timeout_ms: 5_000,
                },
                "RPC_TIMEOUT",
                ErrorClass::Retryable,
            ),
            (
                FluxError::StaleRequest {
                    time: "t".into(),
                    window_secs: 300,
                    now: "n".into(),
                },
                "STALE_REQUEST",
                ErrorClass::Permanent,
            ),
            (
                FluxError::RpcInvalidReply {
                    operation: OPERACION.into(),
                    reason: "x".into(),
                    stream_ack: false,
                },
                "RPC_INVALID_REPLY",
                ErrorClass::Permanent,
            ),
            (
                FluxError::RpcMessageTooLarge {
                    what: "la petición".into(),
                    bytes: 2_000_000,
                    limit: 1_048_576,
                },
                "RPC_MESSAGE_TOO_LARGE",
                ErrorClass::Permanent,
            ),
            (
                FluxError::InvalidRpcCall { reason: "x".into() },
                "INVALID_RPC_CALL",
                ErrorClass::Permanent,
            ),
        ];
        for (error, code, clase) in &casos {
            assert_eq!(error.code(), Some(*code));
            assert_eq!(error.class(), Some(*clase), "{code}");
        }
        // Y son los que dice protocol.json, con su clase.
        let protocol = json_of(PROTOCOL);
        for (_, code, clase) in &casos {
            let def = &protocol["errors"]["codes"][*code];
            assert_eq!(def["capability"], "rpc", "{code}");
            assert_eq!(def["class"], clase.as_str(), "{code}");
        }
        let en_protocol: Vec<&str> = protocol["errors"]["codes"]
            .as_object()
            .unwrap()
            .iter()
            .filter(|(_, v)| v["capability"] == "rpc")
            .map(|(k, _)| k.as_str())
            .collect();
        assert_eq!(en_protocol.len(), 6, "la lista es CERRADA: seis");
        assert_eq!(
            [
                RPC_UNAVAILABLE_CODE,
                RPC_TIMEOUT_CODE,
                STALE_REQUEST_CODE,
                RPC_INVALID_REPLY_CODE,
                RPC_MESSAGE_TOO_LARGE_CODE,
                INVALID_RPC_CALL_CODE
            ],
            [
                "RPC_UNAVAILABLE",
                "RPC_TIMEOUT",
                "STALE_REQUEST",
                "RPC_INVALID_REPLY",
                "RPC_MESSAGE_TOO_LARGE",
                "INVALID_RPC_CALL"
            ]
        );
        assert_eq!(RESERVED_DOMAIN_CODE, "RESERVED_DOMAIN");
    }

    /// Lo que cada mensaje DEBE decir — 04-errors.md §7, «Qué DEBE decir el mensaje».
    #[test]
    fn cada_mensaje_dice_lo_que_la_spec_exige() {
        let m = FluxError::RpcTimeout {
            operation: OPERACION.into(),
            timeout_ms: 2_000,
        }
        .to_string();
        assert!(
            m.contains(OPERACION) && m.contains("2000") && m.contains("PUEDE haberse ejecutado"),
            "{m}"
        );
        let m = FluxError::RpcUnavailable {
            operation: OPERACION.into(),
        }
        .to_string();
        assert!(m.contains(OPERACION) && m.contains("no responders"), "{m}");
        let m = FluxError::RpcMessageTooLarge {
            what: "la petición".into(),
            bytes: 2_000_000,
            limit: 1_048_576,
        }
        .to_string();
        assert!(
            m.contains("2000000") && m.contains("1048576") && m.contains("claim-check"),
            "{m}"
        );
        let m = FluxError::StaleRequest {
            time: "2026-01-01T00:00:00.000Z".into(),
            window_secs: 300,
            now: "2026-01-01T01:00:00.000Z".into(),
        }
        .to_string();
        assert!(
            m.contains("2026-01-01T00:00:00.000Z")
                && m.contains("300")
                && m.contains("2026-01-01T01:00:00.000Z")
                && m.contains("NTP"),
            "{m}"
        );
        let m = FluxError::RpcInvalidReply {
            operation: OPERACION.into(),
            reason: "su `causationid` no es el `id` de la petición".into(),
            stream_ack: false,
        }
        .to_string();
        assert!(
            m.contains(OPERACION) && m.contains("causationid") && m.contains("incidente"),
            "{m}"
        );
        let m = RpcOperation::parse("clientes.cliente.v1")
            .unwrap_err()
            .to_string();
        assert!(
            m.contains('4')
                && m.contains("<dominio>.<agregado>.v<major>.<operacion>")
                && m.contains("^[a-z0-9]"),
            "{m}"
        );
    }

    #[test]
    fn las_constantes_son_las_de_protocol_json() {
        let rpc = &json_of(PROTOCOL)["rpc"];
        assert_eq!(
            DEFAULT_TIMEOUT_MS,
            rpc["timeout"]["defaultMs"].as_u64().unwrap()
        );
        assert_eq!(MIN_TIMEOUT_MS, rpc["timeout"]["minMs"].as_u64().unwrap());
        assert_eq!(MAX_TIMEOUT_MS, rpc["timeout"]["maxMs"].as_u64().unwrap());
        // El máximo está POR DEBAJO del `ack_wait`: es la razón de ser de los 25 s.
        assert!(Duration::from_millis(MAX_TIMEOUT_MS) < crate::protocol::DEFAULT_ACK_WAIT);
        assert_eq!(
            Duration::from_millis(rpc["timeout"]["ackWaitMs"].as_u64().unwrap()),
            crate::protocol::DEFAULT_ACK_WAIT
        );
        assert_eq!(
            STALE_REQUEST_WINDOW_SECS,
            rpc["signing"]["staleRequestWindowSeconds"]
                .as_i64()
                .unwrap()
        );
        assert_eq!(
            RPC_ERROR_TYPE,
            rpc["type"]["error"]["value"].as_str().unwrap()
        );
        assert_eq!(
            RPC_ERROR_DATASCHEMA,
            rpc["dataschema"]["error"].as_str().unwrap()
        );
        assert_eq!(
            MAX_MESSAGE_BYTES as u64,
            rpc["size"]["maxMessageBytes"].as_u64().unwrap()
        );
        // ~256 handlers concurrentes: el orden de `max_ack_pending`.
        assert_eq!(
            MAX_CONCURRENT_HANDLERS,
            crate::protocol::DEFAULT_MAX_ACK_PENDING as usize
        );
        // Las tres clases del error: solo dos viajan.
        assert_eq!(rpc["error"]["classes"], json!(["retryable", "permanent"]));
        // Las claves del `data` del error, en orden.
        assert_eq!(
            rpc["error"]["dataKeys"],
            json!(["class", "code", "message", "retryAfterMs"])
        );
        assert_eq!(
            rpc["noResponders"]["connectFlags"],
            json!(["headers", "no_responders"])
        );
        assert_eq!(rpc["queueGroup"]["name"], "service-name");
        assert_eq!(rpc["queueGroup"]["wildcardSubscriptionForbidden"], true);
        // La ventana de reintentos es una decisión nuestra, acotada.
        assert!(MAX_RETRIES >= 1 && DEFAULT_RETRY_DELAY > Duration::ZERO);
    }

    #[test]
    fn las_opciones_se_componen() {
        let o = RequestOptions::default()
            .with_timeout_ms(2_000)
            .with_aggregate_id("cli-1")
            .with_tenant_id("acme")
            .with_classification(DataClassification::Confidential)
            .with_retries(2)
            .with_retry_delay(Duration::from_millis(7));
        assert_eq!(o.timeout, Some(Duration::from_secs(2)));
        assert_eq!(
            (o.aggregate_id.as_deref(), o.tenant_id.as_deref()),
            (Some("cli-1"), Some("acme"))
        );
        assert_eq!(o.retries, 2);
        assert_eq!(
            RespondOptions::default()
                .with_max_concurrent(3)
                .max_concurrent,
            Some(3)
        );
        let mut texto = String::new();
        let _ = write!(texto, "{:?}", RespondOptions::default());
        assert!(texto.contains("RespondOptions"));
        let n = AtomicU32::new(0);
        n.fetch_add(1, Ordering::SeqCst);
    }
}
