//! Constantes verificadas del protocolo y reglas de naming.
//!
//! Contrato normativo: `specification/02-naming.md` y `protocol.json`.
//!
//! Todo lo de aquí sale de `protocol.json`. Si divergen, manda `protocol.json`: es lo
//! que consumen los demás SDKs y los agentes de IA.

use std::time::Duration;

use crate::errors::FluxError;

// ─── Constantes del protocolo ────────────────────────────────────────────────

/// Nombre del contrato. Identifica el protocolo, no este SDK.
pub const PROTOCOL_NAME: &str = "flux";

/// Versión del contrato. No es la versión del crate.
pub const PROTOCOL_VERSION: &str = "1.0.0";

/// Literal exigido por CloudEvents — 01-envelope.md §2.
pub const SPEC_VERSION: &str = "1.0";

/// flux v1 solo admite JSON — 01-envelope.md §2.
pub const DATA_CONTENT_TYPE: &str = "application/json";

/// Nivel de conformidad declarado por este SDK — 00-protocol.md §5.
pub const CONFORMANCE_LEVEL: &str = "L2";

/// Techo del mensaje serializado (1 MiB).
///
/// Por encima, claim-check: se publica `{uri, sha256, bytes}`, no el contenido
/// — 01-envelope.md §7.
pub const MAX_MESSAGE_BYTES: usize = 1_048_576;

// ─── Configuración canónica de consumidor — 03-delivery.md §2 ────────────────

/// Presupuesto de duración del handler. **DEBE** coincidir con `CANONICAL_BACKOFF[0]`.
///
/// ⚠️ JetStream **SOBRESCRIBE** `ack_wait` con `backoff[0]` y no devuelve error: pides
/// 30 s con un backoff que empieza en 1 s y obtienes un `ack_wait` efectivo de 1 s.
/// Cualquier handler que toque una base de datos se ejecuta entonces en concurrencia
/// consigo mismo, en cada mensaje, sin ninguna señal visible. Verificado contra
/// nats-server 2.14.5 — ver 03-delivery.md §2.1 y `conformance/cases/consumer-config.json`.
///
/// Consecuencia de diseño: `backoff[0]` **ES** el presupuesto de duración del handler.
/// Por eso el backoff canónico empieza en 30 s y no en 1 s; un primer reintento rápido
/// es imposible por construcción y buscarlo es lo que rompe la configuración.
pub const DEFAULT_ACK_WAIT: Duration = Duration::from_secs(30);

/// 1 entrega inicial + 5 reintentos, uno por entrada de [`CANONICAL_BACKOFF`].
///
/// Si fuese 5, la última entrada (30 min) no se aplicaría nunca y la configuración
/// mentiría sobre su propio comportamiento.
pub const DEFAULT_MAX_DELIVER: u32 = 6;

/// Ventana de mensajes sin confirmar.
///
/// Ojo: un mensaje esperando reintento ocupa una ranura, así que con backoffs largos y
/// mucho fallo simultáneo esta ventana se llena — 03-delivery.md §2.1, nota final.
pub const DEFAULT_MAX_ACK_PENDING: u32 = 256;

/// Backoff canónico `[30s, 1m, 5m, 15m, 30m]` — 03-delivery.md §2.
///
/// Es una constante y no una función que devuelve una copia (como en Go): en Rust un
/// array `const` no puede mutarse desde fuera, así que la invariante
/// `DEFAULT_ACK_WAIT == CANONICAL_BACKOFF[0]` no puede romperse en tiempo de ejecución.
///
/// Tiempo total hasta la DLQ ≈ 51 min 30 s. Es una decisión de producto: cuánto tiempo
/// aceptas que un fallo transitorio siga reintentando antes de que un humano se entere.
/// Solo lo recorren los RETRYABLE; un PERMANENT no gasta ni un reintento.
pub const CANONICAL_BACKOFF: [Duration; 5] = [
    Duration::from_secs(30),
    Duration::from_secs(60),
    Duration::from_secs(300),
    Duration::from_secs(900),
    Duration::from_secs(1800),
];

/// Suma del backoff canónico: lo que tarda un RETRYABLE en agotar los reintentos y
/// caer en la DLQ.
#[must_use]
pub fn total_time_to_dlq() -> Duration {
    CANONICAL_BACKOFF.iter().sum()
}

// ─── Configuración canónica de stream — 02-naming.md §3.3 ────────────────────

/// Retención del stream de eventos.
pub const STREAM_MAX_AGE: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Retención del stream de DLQ. Más larga a propósito: la DLQ es material forense.
/// Pero es un límite real, no un archivo — a los 90 días el evento desaparece.
pub const DLQ_STREAM_MAX_AGE: Duration = Duration::from_secs(90 * 24 * 60 * 60);

/// Ventana de deduplicación de **publicaciones** con el mismo `Nats-Msg-Id`.
///
/// **NO** deduplica reentregas de consumo, y nunca sustituye a la idempotencia del
/// consumidor: es el malentendido más frecuente del protocolo — 03-delivery.md §3.
pub const DUPLICATE_WINDOW: Duration = Duration::from_secs(120);

// ─── Naming ──────────────────────────────────────────────────────────────────

/// Los cuatro tokens de un subject ya validado.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSubject {
    /// Bounded context. Sustantivo plural en kebab-case: `pedidos`.
    pub domain: String,
    /// Raíz de agregado. Sustantivo singular en kebab-case: `pedido`.
    pub aggregate: String,
    /// Versión mayor del contrato, entero ≥ 1.
    pub major: u32,
    /// Hecho en pasado, kebab-case: `creado`, `entrega-fallida`.
    pub event: String,
}

impl ParsedSubject {
    /// Reconstruye el subject original. La transformación es biyectiva.
    #[must_use]
    pub fn subject(&self) -> String {
        format!(
            "{}.{}.v{}.{}",
            self.domain, self.aggregate, self.major, self.event
        )
    }

    /// Deriva el `type` de CloudEvents de este subject — 02-naming.md §2.
    #[must_use]
    pub fn event_type(&self) -> String {
        format!(
            "com.flux.{}.{}.{}.v{}",
            self.domain, self.aggregate, self.event, self.major
        )
    }
}

/// Equivale a `^[a-z0-9]+(-[a-z0-9]+)*$`.
///
/// Se comprueba a mano en vez de con el crate `regex` porque es la única expresión
/// regular que necesitaría todo el SDK y arrastrarlo entero por cuatro tokens no sale
/// a cuenta. Un test cruza esta función con los ejemplos válidos e inválidos de
/// `protocol.json` para que la equivalencia no se quede en una afirmación.
fn is_kebab_lower(token: &str) -> bool {
    if token.is_empty() || token.starts_with('-') || token.ends_with('-') {
        return false;
    }
    if token.contains("--") {
        return false;
    }
    token
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// Valida `v<major>`: literal `v` + entero ≥ 1 sin ceros a la izquierda.
fn parse_major(token: &str) -> Option<u32> {
    let digits = token.strip_prefix('v')?;
    if digits.is_empty() || digits.starts_with('0') || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Valida y descompone un subject de NATS.
///
/// ⚠️ No confundir con el atributo `subject` de CloudEvents, que es el **id del
/// agregado** (`"ped-123"`). En este SDK ese atributo se llama `aggregate_id` y solo se
/// mapea a `subject` al serializar — 01-envelope.md §2.1.
///
/// # Errores
///
/// [`FluxError::InvalidSubject`] si no cumple `<dominio>.<agregado>.v<major>.<evento>`.
pub fn parse_subject(subject: &str) -> Result<ParsedSubject, FluxError> {
    // La comprobación de minúsculas va ANTES que la del formato para poder dar un
    // mensaje útil: los subjects de NATS son case-sensitive, así que
    // "Pedidos.pedido.v1.creado" crea un subject fantasma al que nadie está suscrito y
    // no produce ningún error. Sin este mensaje, el desarrollador solo ve "no llegan
    // mis eventos" — 02-naming.md §1.1.
    if subject.chars().any(|c| c.is_ascii_uppercase()) {
        return Err(FluxError::InvalidSubject {
            subject: subject.to_string(),
            reason: "debe ir todo en minúsculas — NATS es case-sensitive y una mayúscula \
                     crea un subject al que nadie está suscrito, sin producir error"
                .to_string(),
        });
    }

    let tokens: Vec<&str> = subject.split('.').collect();
    if tokens.len() != 4 {
        return Err(FluxError::InvalidSubject {
            subject: subject.to_string(),
            reason: format!(
                "debe tener exactamente 4 tokens (<dominio>.<agregado>.v<major>.<evento>), tiene {}",
                tokens.len()
            ),
        });
    }

    let invalid = |reason: &str| FluxError::InvalidSubject {
        subject: subject.to_string(),
        reason: reason.to_string(),
    };

    if !is_kebab_lower(tokens[0]) {
        return Err(invalid(
            "el dominio debe ser un sustantivo plural en kebab-case ([a-z0-9-], sin `_`, \
             sin guiones dobles ni a los extremos)",
        ));
    }
    if !is_kebab_lower(tokens[1]) {
        return Err(invalid(
            "el agregado debe ser un sustantivo singular en kebab-case ([a-z0-9-])",
        ));
    }
    let Some(major) = parse_major(tokens[2]) else {
        return Err(invalid(
            "el tercer token debe ser la versión mayor: literal `v` + entero >= 1 (`v1`, `v2`)",
        ));
    };
    if !is_kebab_lower(tokens[3]) {
        return Err(invalid(
            "el evento debe ser un verbo en PASADO en kebab-case (`creado`, `entrega-fallida`); \
             un comando (`crear`) no es un evento",
        ));
    }

    Ok(ParsedSubject {
        domain: tokens[0].to_string(),
        aggregate: tokens[1].to_string(),
        major,
        event: tokens[3].to_string(),
    })
}

/// Informa si el subject cumple el contrato, sin exponer el motivo.
#[must_use]
pub fn is_valid_subject(subject: &str) -> bool {
    parse_subject(subject).is_ok()
}

/// Deriva el `type` de CloudEvents:
/// `pedidos.pedido.v1.creado` → `com.flux.pedidos.pedido.creado.v1` — 02-naming.md §2.
///
/// Los dos formatos existen porque sirven a consumidores distintos: el subject enruta y
/// necesita la versión en posición fija para que los wildcards funcionen; el `type`
/// identifica el contrato en un catálogo y ahí lee mejor con la versión al final. La
/// transformación es mecánica, así que jamás se le pide al desarrollador.
///
/// # Errores
///
/// [`FluxError::InvalidSubject`] si el subject no es válido.
pub fn subject_to_type(subject: &str) -> Result<String, FluxError> {
    Ok(parse_subject(subject)?.event_type())
}

/// Devuelve `EVT_PEDIDOS` para el dominio `pedidos`.
///
/// NATS no admite `.`, `*`, `>`, `/`, `\` ni espacios en nombres de stream: de ahí el
/// guion bajo. Las mayúsculas son convención, para distinguir de un vistazo un stream
/// de un subject en los logs — 02-naming.md §3.
#[must_use]
pub fn stream_name(domain: &str) -> String {
    format!("EVT_{}", domain.replace('-', "_").to_uppercase())
}

/// Devuelve `DLQ_PEDIDOS` para el dominio `pedidos` — 02-naming.md §3.
#[must_use]
pub fn dlq_stream_name(domain: &str) -> String {
    format!("DLQ_{}", domain.replace('-', "_").to_uppercase())
}

/// Valida el nombre de servicio contra `^[a-z0-9]+(-[a-z0-9]+)*$`.
///
/// Existe porque **NATS aceptaría** un durable `FacturacionAPI__pedidos_…` sin error, y
/// el incumplimiento del patrón de `durableConsumer` solo se descubriría al parsear
/// nombres en una herramienta de operación. `protocol.json` lo exige explícitamente en
/// `naming.service`: el SDK **DEBE** validar el nombre de servicio en `connect()`.
///
/// # Errores
///
/// [`FluxError::InvalidServiceName`] si no cumple el patrón.
pub fn validate_service_name(service: &str) -> Result<(), FluxError> {
    if is_kebab_lower(service) {
        return Ok(());
    }
    Err(FluxError::InvalidServiceName {
        service: service.to_string(),
        reason: "debe ser kebab-case en minúsculas ([a-z0-9] separados por guiones simples): \
                 `facturacion-api`, no `FacturacionAPI` ni `facturacion_api`. NATS aceptaría \
                 el durable resultante sin error y el nombre dejaría de ser parseable por \
                 las herramientas de operación (protocol.json → naming.service)"
            .to_string(),
    })
}

/// Devuelve `facturacion-api__pedidos_pedido_v1_creado`.
///
/// NATS tampoco admite `.` en nombres de durable consumer. Separar el servicio con `__`
/// y los tokens con `_` mantiene la reversibilidad: partiendo por `__` recuperas
/// servicio y subject exactos. Un nombre de consumidor que no dice qué servicio lo tiene
/// abierto es inútil en `nats consumer ls` a las 3 de la mañana — 02-naming.md §4.
///
/// # Errores
///
/// [`FluxError::InvalidServiceName`] o [`FluxError::InvalidSubject`] si alguno de los
/// dos no cumple su patrón.
pub fn durable_name(service: &str, subject: &str) -> Result<String, FluxError> {
    validate_service_name(service)?;
    parse_subject(subject)?;
    let flat = subject.replace(['.', '-'], "_");
    Ok(format!("{service}__{flat}"))
}

/// Antepone `dlq.` al subject original.
///
/// PREFIJO, nunca sufijo. Un sufijo (`pedidos.pedido.v1.creado.dlq`) encajaría con
/// `pedidos.>` y el stream `EVT_PEDIDOS` capturaría sus propios muertos: contarían
/// contra su retención, un consumidor de `pedidos.pedido.v1.>` los recibiría, y un
/// replay masivo podría reinyectarse en su propia DLQ — 02-naming.md §3.1.
#[must_use]
pub fn dlq_subject(subject: &str) -> String {
    format!("dlq.{subject}")
}

/// Informa si el subject pertenece al espacio de nombres de la DLQ.
#[must_use]
pub fn is_dlq_subject(subject: &str) -> bool {
    subject.starts_with("dlq.")
}

/// Devuelve `/produccion/pedidos-api` — 01-envelope.md §2.
///
/// `id` + `source` son la clave de deduplicación del ecosistema entero, así que el
/// `source` tiene que identificar de forma estable entorno y servicio.
#[must_use]
pub fn source_uri(environment: &str, service: &str) -> String {
    format!("/{environment}/{service}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// La invariante más cara del protocolo. No puede expresarse en el sistema de tipos
    /// en ninguno de los seis lenguajes, así que se defiende con un test — y además,
    /// sobre la config EFECTIVA del servidor, en `client::assert_config_honored`.
    #[test]
    fn ack_wait_es_backoff_cero() {
        assert_eq!(DEFAULT_ACK_WAIT, CANONICAL_BACKOFF[0]);
    }

    #[test]
    fn max_deliver_cuadra_con_el_backoff() {
        // 1 entrega inicial + una por entrada de backoff.
        assert_eq!(DEFAULT_MAX_DELIVER as usize, CANONICAL_BACKOFF.len() + 1);
    }

    #[test]
    fn total_hasta_dlq_son_3090_segundos() {
        // protocol.json → consumer.totalTimeToDlqSeconds
        assert_eq!(total_time_to_dlq(), Duration::from_secs(3090));
    }

    /// Los ejemplos son literalmente los de `protocol.json` → `naming.subject`.
    #[test]
    fn subjects_validos_de_protocol_json() {
        for s in [
            "pedidos.pedido.v1.creado",
            "logistica.envio.v1.entrega-fallida",
        ] {
            assert!(is_valid_subject(s), "{s} debería ser válido");
        }
    }

    #[test]
    fn subjects_invalidos_de_protocol_json() {
        for s in [
            "pedidos.crear-pedido",
            "Pedidos.Pedido.V1.Creado",
            "pedidos.pedido.v1.creado.retry",
        ] {
            assert!(!is_valid_subject(s), "{s} debería ser inválido");
        }
    }

    #[test]
    fn el_mensaje_de_mayusculas_explica_el_subject_fantasma() {
        let err = parse_subject("Pedidos.pedido.v1.creado").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("minúsculas"), "{msg}");
        // Lo importante no es que falle: es que diga POR QUÉ falla en silencio en NATS.
        assert!(msg.contains("nadie está suscrito"), "{msg}");
    }

    #[test]
    fn tokens_rechazados() {
        for s in [
            "pedidos.pedido.v0.creado",   // v0 no existe
            "pedidos.pedido.v01.creado",  // cero a la izquierda
            "pedidos.pedido.1.creado",    // falta la `v`
            "pedidos.pedido_v1.creado",   // guion bajo
            "pedidos..v1.creado",         // token vacío
            "pedidos.-pedido.v1.creado",  // guion al principio
            "pedidos.pedido-.v1.creado",  // guion al final
            "pedidos.pedi--do.v1.creado", // guion doble
            "pedidos.pedido.v1.creado ",  // espacio
        ] {
            assert!(!is_valid_subject(s), "{s} debería ser inválido");
        }
    }

    #[test]
    fn subject_a_type_y_vuelta() {
        assert_eq!(
            subject_to_type("pedidos.pedido.v1.creado").unwrap(),
            "com.flux.pedidos.pedido.creado.v1"
        );
        assert_eq!(
            subject_to_type("logistica.envio.v2.entrega-fallida").unwrap(),
            "com.flux.logistica.envio.entrega-fallida.v2"
        );
        let p = parse_subject("pedidos.pedido.v1.creado").unwrap();
        assert_eq!(p.subject(), "pedidos.pedido.v1.creado");
    }

    #[test]
    fn nombres_de_stream_y_durable_sin_puntos() {
        assert_eq!(stream_name("pedidos"), "EVT_PEDIDOS");
        assert_eq!(dlq_stream_name("pedidos"), "DLQ_PEDIDOS");
        assert_eq!(stream_name("linea-envio"), "EVT_LINEA_ENVIO");

        let d = durable_name("facturacion-api", "pedidos.pedido.v1.creado").unwrap();
        assert_eq!(d, "facturacion-api__pedidos_pedido_v1_creado");
        assert!(!d.contains('.'), "un durable con puntos NATS lo rechaza");

        // Reversible: partiendo por `__` se recuperan servicio y subject.
        let (service, flat) = d.split_once("__").unwrap();
        assert_eq!(service, "facturacion-api");
        assert_eq!(flat, "pedidos_pedido_v1_creado");
    }

    #[test]
    fn el_durable_valida_tambien_el_nombre_de_servicio() {
        for bad in ["FacturacionAPI", "facturacion_api", "-api", "api-", ""] {
            assert!(
                durable_name(bad, "pedidos.pedido.v1.creado").is_err(),
                "{bad} debería rechazarse"
            );
        }
        assert!(validate_service_name("facturacion-api").is_ok());
        assert!(validate_service_name("pedidos2").is_ok());
    }

    #[test]
    fn la_dlq_va_por_prefijo() {
        let dlq = dlq_subject("pedidos.pedido.v1.creado");
        assert_eq!(dlq, "dlq.pedidos.pedido.v1.creado");
        assert!(is_dlq_subject(&dlq));
        // La razón del prefijo: con sufijo, `pedidos.>` capturaría sus propios muertos.
        assert!(!dlq.starts_with("pedidos."));
    }

    #[test]
    fn source_uri_formato() {
        assert_eq!(
            source_uri("produccion", "pedidos-api"),
            "/produccion/pedidos-api"
        );
    }
}
