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

    /// Solo aplica a [`ErrorClass::Retryable`]: sobrescribe el backoff canónico para
    /// ESTE intento. Úsalo cuando la dependencia dice explícitamente cuánto esperar
    /// (cabecera `Retry-After`). `None` significa "usa el backoff canónico".
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

    /// Fallo de transporte: conexión, publicación, creación de stream o de consumidor.
    #[error("{context}")]
    Transport {
        /// Qué se estaba intentando, en términos de flux.
        context: String,
        /// El error de `async-nats` que lo causó. Nunca se expone su tipo en la firma.
        #[source]
        source: HandlerError,
    },
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
            Self::Retryable(_) => Some(ErrorClass::Retryable),
            Self::Permanent(_) => Some(ErrorClass::Permanent),
            Self::Poison(_) => Some(ErrorClass::Poison),
            _ => None,
        }
    }

    /// El código estable, o el nombre de la clase si no se fijó uno. Las métricas nunca
    /// quedan sin etiqueta.
    #[must_use]
    pub fn code(&self) -> Option<&str> {
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
