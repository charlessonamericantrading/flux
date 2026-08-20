//! Clasificación de errores del handler.
//!
//! Contrato normativo: `specification/04-errors.md` §2
//!
//! Este fichero es el punto donde el protocolo se encuentra con la realidad operativa
//! del ecosistema. Todo lo demás en el SDK es mecánica; esto es **política** — y por eso
//! la política es un parámetro, no una constante.

use std::error::Error as StdError;
use std::fmt;
use std::io;
use std::time::Duration;

use crate::errors::{as_classified, Classification, ErrorClass, HandlerError};

/// Status HTTP que merecen reintento — 04-errors.md §1.1.
///
/// Nótese qué **NO** está aquí: 400, 403, 404 y 422 son PERMANENT. Reintentarlos es
/// gastar 51 minutos para obtener exactamente la misma respuesta.
pub const RETRYABLE_HTTP_STATUS: [u16; 4] = [429, 502, 503, 504];

/// Entregas que gasta un error desconocido bajo la política acotada. Incluye la primera
/// entrega, así que 2 = un reintento — 04-errors.md §2.1.
pub const DEFAULT_UNKNOWN_RETRY_BUDGET: u32 = 2;

// ─── HTTP ────────────────────────────────────────────────────────────────────

/// Un fallo de una dependencia HTTP, con su status.
///
/// ⚠️ **Divergencia con Node, y de fondo.** Allí el clasificador hurga en `err.status`,
/// `err.statusCode` y `err.response.status` porque en JavaScript cualquier objeto puede
/// tener cualquier propiedad. Go lo resuelve con una interfaz y `errors.As`. Rust **no
/// puede hacer ninguna de las dos cosas**: `dyn Error` solo permite hacer `downcast_ref`
/// a un tipo **concreto**, no a otro trait, así que una interfaz `HttpStatusError` sería
/// indetectable desde el clasificador.
///
/// El contrato es por tanto este tipo concreto: envuelve el fallo con
/// [`HttpError::new`] —o registra una [`ClassifierOptions::rules`] que reconozca el error
/// de tu cliente HTTP— y el clasificador lo encuentra recorriendo la cadena de `source()`.
#[derive(Debug)]
pub struct HttpError {
    /// Status devuelto por la dependencia.
    pub status: u16,
    /// Qué se estaba llamando.
    pub message: String,
    /// El `Retry-After` anunciado por la dependencia, si lo anunció.
    pub retry_after: Option<Duration>,
    /// Error subyacente del cliente HTTP.
    pub source: Option<HandlerError>,
}

impl HttpError {
    /// Construye un [`HttpError`].
    ///
    /// ```
    /// # use std::time::Duration;
    /// let err = flux::HttpError::new(503, "POST /v1/charges")
    ///     .with_retry_after(Duration::from_secs(5));
    /// ```
    #[must_use]
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            retry_after: None,
            source: None,
        }
    }

    /// Declara el `Retry-After` anunciado por la dependencia.
    #[must_use]
    pub fn with_retry_after(mut self, delay: Duration) -> Self {
        self.retry_after = Some(delay);
        self
    }

    /// Encadena el error del cliente HTTP.
    #[must_use]
    pub fn with_source(mut self, source: impl Into<HandlerError>) -> Self {
        self.source = Some(source.into());
        self
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(f, "HTTP {}", self.status)
        } else {
            write!(f, "HTTP {}: {}", self.status, self.message)
        }
    }
}

impl StdError for HttpError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_ref()
            .map(|e| &**e as &(dyn StdError + 'static))
    }
}

// ─── Detección semántica de lo transitorio ───────────────────────────────────

/// Traduce un [`io::ErrorKind`] transitorio al nombre que usa el resto del ecosistema.
///
/// La **detección** es semántica —`ErrorKind`, el mecanismo idiomático de Rust— y nunca
/// por substring sobre el mensaje de error, que es justo lo que invita a hacer una lista
/// de códigos tratada como normativa (04-errors.md §1.1). El **nombre emitido** sí es el
/// errno clásico, porque el `code` acaba en las métricas y agrupar por causa tiene que
/// funcionar con eventos que vienen de los seis SDKs.
///
/// Un port literal de la lista de Node produjo un bug real: en Windows el mismo corte de
/// red se clasificaba PERMANENT y en Linux RETRYABLE. Con `ErrorKind` eso no puede pasar:
/// `std` ya traduce `WSAECONNRESET` y `ECONNRESET` al mismo
/// [`io::ErrorKind::ConnectionReset`].
fn transient_io_name(kind: io::ErrorKind) -> Option<&'static str> {
    Some(match kind {
        io::ErrorKind::ConnectionReset => "ECONNRESET",
        io::ErrorKind::ConnectionRefused => "ECONNREFUSED",
        io::ErrorKind::ConnectionAborted => "ECONNABORTED",
        io::ErrorKind::TimedOut => "ETIMEDOUT",
        io::ErrorKind::BrokenPipe => "EPIPE",
        io::ErrorKind::HostUnreachable => "EHOSTUNREACH",
        io::ErrorKind::NetworkUnreachable => "ENETUNREACH",
        io::ErrorKind::NetworkDown => "ENETDOWN",
        _ => return None,
    })
}

/// Recorre la cadena de causas buscando un tipo concreto.
///
/// Es el `errors.As` de Go escrito a mano: Rust no lo trae en `std`, y sin él un
/// `HttpError` envuelto por una capa de la aplicación quedaría sin clasificar.
fn find_source<'a, T: StdError + 'static>(err: &'a (dyn StdError + 'static)) -> Option<&'a T> {
    let mut cursor = Some(err);
    while let Some(e) = cursor {
        if let Some(found) = e.downcast_ref::<T>() {
            return Some(found);
        }
        cursor = e.source();
    }
    None
}

// ─── Política ────────────────────────────────────────────────────────────────

/// Qué hacer con un error que no encaja en ninguna regla conocida — 04-errors.md §2.1.
///
/// Es un tipo propio y no un [`ErrorClass`] porque "retryable acotado" **no es una clase
/// del protocolo**: son dos clases (RETRYABLE) con presupuestos distintos. Meterlo en
/// `ErrorClass` contaminaría el valor que acaba escrito en `dlqreason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UnknownPolicy {
    /// El default de la spec: RETRYABLE con presupuesto reducido
    /// ([`DEFAULT_UNKNOWN_RETRY_BUDGET`], 2 entregas) en vez de los 6 completos.
    ///
    /// Un transitorio se recupera en el segundo intento; un sistemático llega a la DLQ en
    /// ~30 s sin atascar la cola. **Domina a las otras dos**: cuesta 30 s de latencia
    /// sobre los permanentes genuinos y elimina los dos modos de fallo.
    #[default]
    RetryableBounded,

    /// A la DLQ sin gastar reintentos. Falla rápido, pero un hipo de red manda a la DLQ
    /// un evento perfectamente válido y alguien lo reproduce a mano cada mañana.
    Permanent,

    /// Backoff completo, 51 minutos. Elígelo solo si vuestras dependencias internas
    /// tienen hipos frecuentes y podéis asumir que un modo de fallo nuevo atasque la cola
    /// y **se amplifique** con cada mensaje siguiente.
    Retryable,
}

/// Una regla de clasificación de la aplicación.
///
/// Devuelve `None` para ceder al resto de la cadena. Se evalúa antes que todo lo demás
/// salvo los errores que ya declaran su clase.
pub type Rule = Box<dyn Fn(&(dyn StdError + 'static)) -> Option<Classification> + Send + Sync>;

/// La política de clasificación del consumidor.
pub struct ClassifierOptions {
    /// Qué hacer con un error que no encaja en ninguna regla conocida.
    pub unknown_policy: UnknownPolicy,

    /// Entregas máximas de un error desconocido cuando la política es
    /// [`UnknownPolicy::RetryableBounded`].
    ///
    /// **NO** se traduce a `max_deliver` del consumidor: eso es por consumidor, no por
    /// mensaje, y recortaría también los reintentos de los RETRYABLE reconocidos. Viaja
    /// en [`Classification::max_attempts`] y lo aplica el runtime a ese error concreto
    /// — 04-errors.md §2.1.
    pub unknown_retry_budget: u32,

    /// Un timeout, ¿es "el mundo va lento" o "esta operación no cabe en la ventana"?
    ///
    /// El default es [`ErrorClass::Retryable`]: un timeout suele indicar saturación
    /// transitoria. Si vuestros timeouts son casi siempre consultas que nunca van a
    /// terminar, [`ErrorClass::Permanent`] evita reintentar lo imposible.
    pub timeout_policy: ErrorClass,

    /// Reglas propias, evaluadas antes que todo lo demás.
    ///
    /// Es también el punto de extensión para clientes HTTP de terceros: si no quieres
    /// envolver en [`HttpError`], escribe aquí la regla que reconozca tu error.
    pub rules: Vec<Rule>,
}

impl Default for ClassifierOptions {
    fn default() -> Self {
        Self {
            unknown_policy: UnknownPolicy::default(),
            unknown_retry_budget: DEFAULT_UNKNOWN_RETRY_BUDGET,
            timeout_policy: ErrorClass::Retryable,
            rules: Vec::new(),
        }
    }
}

impl fmt::Debug for ClassifierOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClassifierOptions")
            .field("unknown_policy", &self.unknown_policy)
            .field("unknown_retry_budget", &self.unknown_retry_budget)
            .field("timeout_policy", &self.timeout_policy)
            .field("rules", &format_args!("[{} reglas]", self.rules.len()))
            .finish()
    }
}

/// Traduce un error cualquiera a una de las tres clases del protocolo.
///
/// El runtime del consumidor usa el resultado así:
///
/// ```text
/// Retryable → nak(retry_after ?: backoff canónico)
/// Permanent → term() + publicar en dlq.<subject> con dlqattempts = intento actual
/// Poison    → term() + publicar en dlq.<subject> + alerta inmediata
/// ```
pub struct Classifier {
    unknown_class: ErrorClass,
    unknown_budget: Option<u32>,
    timeout_class: ErrorClass,
    rules: Vec<Rule>,
}

impl fmt::Debug for Classifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Classifier")
            .field("unknown_class", &self.unknown_class)
            .field("unknown_budget", &self.unknown_budget)
            .field("timeout_class", &self.timeout_class)
            .finish_non_exhaustive()
    }
}

impl Default for Classifier {
    /// El clasificador con los defaults de la spec: desconocido → RETRYABLE acotado a 2
    /// entregas, timeout → RETRYABLE.
    fn default() -> Self {
        Self::new(ClassifierOptions::default())
    }
}

impl Classifier {
    /// Construye un clasificador a partir de la política.
    #[must_use]
    pub fn new(opts: ClassifierOptions) -> Self {
        let unknown_class = match opts.unknown_policy {
            UnknownPolicy::Permanent => ErrorClass::Permanent,
            UnknownPolicy::Retryable | UnknownPolicy::RetryableBounded => ErrorClass::Retryable,
        };
        // Solo la política acotada impone un tope propio; las otras dos dejan mandar al
        // max_deliver del consumidor.
        let unknown_budget = match opts.unknown_policy {
            UnknownPolicy::RetryableBounded => Some(opts.unknown_retry_budget.max(1)),
            _ => None,
        };
        let timeout_class = match opts.timeout_policy {
            ErrorClass::Permanent => ErrorClass::Permanent,
            // POISON no es una respuesta razonable a un timeout: el mensaje se
            // interpretó bien, lo que falló fue el mundo.
            _ => ErrorClass::Retryable,
        };
        Self {
            unknown_class,
            unknown_budget,
            timeout_class,
            rules: opts.rules,
        }
    }

    /// Clasifica un error.
    ///
    /// El orden de evaluación es deliberado: lo más específico primero y el default al
    /// final. Esa última línea es la decisión de política de verdad.
    #[must_use]
    pub fn classify(&self, err: &(dyn StdError + 'static)) -> Classification {
        // 1. Un error tipado de flux siempre gana: la aplicación sabe más que el SDK.
        //    Se busca en toda la cadena, no solo en el error de arriba, así que un
        //    FluxError envuelto por una capa intermedia sigue clasificándose bien.
        if let Some((class, code, retry_after)) = as_classified(err).and_then(|f| {
            Some((
                f.class()?,
                f.code().unwrap_or("UNKNOWN").to_string(),
                f.retry_after(),
            ))
        }) {
            return Classification::new(class, code).with_retry_after(retry_after);
        }

        // 2. Reglas de la aplicación.
        for rule in &self.rules {
            if let Some(c) = rule(err) {
                return c;
            }
        }

        // 3. Status HTTP: la señal más fiable que da una dependencia.
        if let Some(http) = find_source::<HttpError>(err) {
            let retryable = RETRYABLE_HTTP_STATUS.contains(&http.status);
            let class = if retryable {
                ErrorClass::Retryable
            } else {
                ErrorClass::Permanent
            };
            return Classification::new(class, format!("HTTP_{}", http.status))
                .with_retry_after(if retryable { http.retry_after } else { None });
        }

        // 4. Errores de sistema: red y DNS son transitorios por definición.
        if let Some(io_err) = find_source::<io::Error>(err) {
            if let Some(name) = transient_io_name(io_err.kind()) {
                return Classification::new(ErrorClass::Retryable, name);
            }
        }

        // 5. Timeouts — política configurable. Aquí caen los de `tokio::time::timeout`,
        //    que son el equivalente del context.DeadlineExceeded de Go y no llegan como
        //    io::Error.
        if find_source::<tokio::time::error::Elapsed>(err).is_some() {
            return Classification::new(self.timeout_class, "TIMEOUT");
        }

        // 6. Lo desconocido. Aquí se decide el comportamiento del ecosistema ante lo que
        //    nadie previó. El default acotado da al transitorio una segunda oportunidad
        //    sin regalarle 51 minutos de cola al sistemático — y el tope viaja en la
        //    clasificación, no en max_deliver, para no recortar los reintentos de los
        //    RETRYABLE reconocidos — 04-errors.md §2.1.
        Classification::new(self.unknown_class, "UNKNOWN").with_max_attempts(self.unknown_budget)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::FluxError;
    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("fallo de la aplicación")]
    struct Desconocido;

    #[derive(Debug, Error)]
    #[error("capa de repositorio")]
    struct Envuelto(#[source] Box<dyn StdError + Send + Sync>);

    fn c(err: &(dyn StdError + 'static)) -> Classification {
        Classifier::default().classify(err)
    }

    #[test]
    fn un_error_tipado_de_flux_siempre_gana() {
        let err = FluxError::permanent("pedido ya cancelado").with_code("PEDIDO_YA_CANCELADO");
        let got = c(&err);
        assert_eq!(got.class, ErrorClass::Permanent);
        assert_eq!(got.code, "PEDIDO_YA_CANCELADO");
        assert_eq!(got.max_attempts, None, "un PERMANENT no lleva presupuesto");
    }

    #[test]
    fn retryable_conserva_su_retry_after() {
        let err = FluxError::retryable("proveedor 503").with_retry_after(Duration::from_secs(5));
        let got = c(&err);
        assert_eq!(got.class, ErrorClass::Retryable);
        assert_eq!(got.retry_after, Some(Duration::from_secs(5)));
    }

    #[test]
    fn la_clase_sobrevive_al_envoltorio() {
        let err = Envuelto(Box::new(
            FluxError::retryable("deadlock").with_code("DEADLOCK"),
        ));
        let got = c(&err);
        assert_eq!(got.class, ErrorClass::Retryable);
        assert_eq!(got.code, "DEADLOCK");
    }

    #[test]
    fn los_status_http_de_la_spec() {
        for status in [429, 502, 503, 504] {
            let err = HttpError::new(status, "GET /x");
            let got = c(&err);
            assert_eq!(got.class, ErrorClass::Retryable, "{status}");
            assert_eq!(got.code, format!("HTTP_{status}"));
        }
        for status in [400, 403, 404, 422] {
            let err = HttpError::new(status, "GET /x");
            let got = c(&err);
            assert_eq!(got.class, ErrorClass::Permanent, "{status}");
            // Un permanente no lleva presupuesto de reintento: gasta 1 entrega.
            assert_eq!(got.max_attempts, None);
        }
    }

    #[test]
    fn el_retry_after_del_429_se_propaga() {
        let err = HttpError::new(429, "POST /v1/charges").with_retry_after(Duration::from_secs(7));
        assert_eq!(c(&err).retry_after, Some(Duration::from_secs(7)));
        // Pero no en un permanente: ahí no hay reintento del que hablar.
        let err = HttpError::new(422, "POST /v1/charges").with_retry_after(Duration::from_secs(7));
        assert_eq!(c(&err).retry_after, None);
    }

    #[test]
    fn el_http_error_se_encuentra_a_traves_de_la_cadena() {
        let err = Envuelto(Box::new(HttpError::new(503, "GET /x")));
        assert_eq!(c(&err).code, "HTTP_503");
    }

    /// Detección por SEMÁNTICA (`ErrorKind`), nunca por substring del mensaje.
    #[test]
    fn los_errores_de_red_son_retryable() {
        for (kind, name) in [
            (io::ErrorKind::ConnectionReset, "ECONNRESET"),
            (io::ErrorKind::ConnectionRefused, "ECONNREFUSED"),
            (io::ErrorKind::ConnectionAborted, "ECONNABORTED"),
            (io::ErrorKind::TimedOut, "ETIMEDOUT"),
            (io::ErrorKind::BrokenPipe, "EPIPE"),
            (io::ErrorKind::HostUnreachable, "EHOSTUNREACH"),
            (io::ErrorKind::NetworkUnreachable, "ENETUNREACH"),
        ] {
            let err = io::Error::from(kind);
            let got = c(&err);
            assert_eq!(got.class, ErrorClass::Retryable, "{kind:?}");
            assert_eq!(got.code, name);
            assert_eq!(
                got.max_attempts, None,
                "un transitorio reconocido conserva los 6"
            );
        }
    }

    #[test]
    fn un_error_de_io_no_transitorio_es_desconocido() {
        // NotFound no dice nada sobre la red: cae al default, no a RETRYABLE completo.
        let err = io::Error::from(io::ErrorKind::NotFound);
        let got = c(&err);
        assert_eq!(got.max_attempts, Some(2));
    }

    #[tokio::test]
    async fn un_timeout_de_tokio_es_retryable_por_defecto() {
        let elapsed = tokio::time::timeout(Duration::from_millis(1), std::future::pending::<()>())
            .await
            .unwrap_err();
        let got = c(&elapsed);
        assert_eq!(got.class, ErrorClass::Retryable);
        assert_eq!(got.code, "TIMEOUT");
    }

    #[tokio::test]
    async fn la_politica_de_timeout_es_configurable() {
        let classifier = Classifier::new(ClassifierOptions {
            timeout_policy: ErrorClass::Permanent,
            ..Default::default()
        });
        let elapsed = tokio::time::timeout(Duration::from_millis(1), std::future::pending::<()>())
            .await
            .unwrap_err();
        assert_eq!(classifier.classify(&elapsed).class, ErrorClass::Permanent);
    }

    // ── el default de lo desconocido ──────────────────────────────────────────

    /// 04-errors.md §2.1: RETRYABLE con presupuesto ACOTADO de 2 entregas.
    #[test]
    fn lo_desconocido_es_retryable_acotado_a_dos() {
        let got = c(&Desconocido);
        assert_eq!(got.class, ErrorClass::Retryable);
        assert_eq!(got.code, "UNKNOWN");
        assert_eq!(got.max_attempts, Some(2));
    }

    #[test]
    fn politica_permanent_para_lo_desconocido() {
        let classifier = Classifier::new(ClassifierOptions {
            unknown_policy: UnknownPolicy::Permanent,
            ..Default::default()
        });
        let got = classifier.classify(&Desconocido);
        assert_eq!(got.class, ErrorClass::Permanent);
        assert_eq!(got.max_attempts, None);
    }

    #[test]
    fn politica_retryable_completa_para_lo_desconocido() {
        let classifier = Classifier::new(ClassifierOptions {
            unknown_policy: UnknownPolicy::Retryable,
            ..Default::default()
        });
        let got = classifier.classify(&Desconocido);
        assert_eq!(got.class, ErrorClass::Retryable);
        assert_eq!(got.max_attempts, None, "sin tope propio: manda max_deliver");
    }

    #[test]
    fn el_presupuesto_acotado_es_configurable() {
        let classifier = Classifier::new(ClassifierOptions {
            unknown_retry_budget: 3,
            ..Default::default()
        });
        assert_eq!(classifier.classify(&Desconocido).max_attempts, Some(3));
    }

    #[test]
    fn un_presupuesto_de_cero_se_eleva_a_uno() {
        // Un mensaje se entrega al menos una vez: 0 no es un presupuesto válido.
        let classifier = Classifier::new(ClassifierOptions {
            unknown_retry_budget: 0,
            ..Default::default()
        });
        assert_eq!(classifier.classify(&Desconocido).max_attempts, Some(1));
    }

    #[test]
    fn las_reglas_de_la_aplicacion_van_antes_que_todo_menos_los_tipados() {
        let classifier = Classifier::new(ClassifierOptions {
            rules: vec![Box::new(|e| {
                e.downcast_ref::<HttpError>()
                    .map(|_| Classification::new(ErrorClass::Permanent, "REGLA_PROPIA"))
            })],
            ..Default::default()
        });
        // La regla gana sobre la tabla de status HTTP…
        let got = classifier.classify(&HttpError::new(503, "x"));
        assert_eq!(got.code, "REGLA_PROPIA");
        // …pero no sobre un error que ya declara su clase.
        let err = FluxError::retryable("x").with_code("MIO");
        assert_eq!(classifier.classify(&err).code, "MIO");
    }
}
