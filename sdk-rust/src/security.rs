//! Seguridad de la conexión: TLS, credenciales mal formadas y denegaciones del broker.
//!
//! Contrato normativo: `specification/06-security.md` §2 y §3.
//!
//! Este módulo existe por un hecho medido contra nats-server 2.14.5 y que `async-nats` no
//! esconde sino que **no cuenta**: cuando una ACL deniega una publicación, el servidor
//! manda un `-ERR 'Permissions Violation for Publish to "…"'` por la conexión y **descarta
//! el mensaje**. La petición no recibe respuesta, así que quien la hizo ve un *timeout* de
//! 5 a 30 s sin una palabra sobre permisos. La única pista llega como un evento aparte
//! (`Event::ServerError`) que nadie escucha por defecto.
//!
//! De ahí el [`DenialLog`]: el SDK escucha esos eventos y, mientras espera una respuesta del
//! broker, **también** espera una denegación del subject al que acaba de publicar. La que
//! llega primero gana, y una denegación llega en milisegundos.
//!
//! Regla de diseño heredada de `client.rs`: ningún tipo de NATS en la API pública. Las
//! denegaciones se parsean aquí y salen como [`crate::FluxError`].

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tokio::sync::watch;

use crate::errors::{FluxError, RejectionReason};

// ─── TLS ─────────────────────────────────────────────────────────────────────

/// Opciones TLS de la conexión — 06-security.md §2: «TLS **DEBE** ser obligatorio».
///
/// Se traducen a las de `async-nats` (`require_tls`, `add_root_certificates`,
/// `add_client_certificate`) en [`crate::connect`]; ningún tipo del cliente de NATS ni de
/// `rustls` aparece aquí.
///
/// ```no_run
/// let tls = flux::TlsOptions::new()
///     .with_ca_file("/etc/flux/ca.pem")                          // CA propia
///     .with_client_cert("/etc/flux/cert.pem", "/etc/flux/key.pem"); // mTLS, opcional
/// let opts = flux::ConnectOptions::new("tls://nats:4222", "pedidos-api", "produccion", "1.0.0")
///     .with_tls(tls);
/// # let _ = opts;
/// ```
#[derive(Debug, Clone)]
pub struct TlsOptions {
    /// CA que firma el certificado del broker, en PEM. Puede ser un bundle con varias.
    ///
    /// **Sustituye** a las raíces del sistema, no las amplía: con una CA propia, solo esa CA
    /// es de fiar. Un broker interno con un certificado de una CA pública no lo necesita,
    /// y entonces se usan las raíces del sistema.
    pub ca_file: Option<PathBuf>,
    /// Certificado de cliente en PEM (mTLS). Siempre va junto a [`Self::client_key`].
    pub client_cert: Option<PathBuf>,
    /// Clave privada del certificado de cliente, en PEM.
    pub client_key: Option<PathBuf>,
    /// Si es `true` (el default), la conexión falla si no se puede establecer TLS.
    ///
    /// El default es `true` y no `false` a propósito: quien construye un `TlsOptions` está
    /// diciendo que quiere TLS, y degradar a texto plano porque el broker no lo ofrece
    /// convertiría una mala configuración en credenciales viajando en claro.
    pub required: bool,
}

impl Default for TlsOptions {
    fn default() -> Self {
        Self {
            ca_file: None,
            client_cert: None,
            client_key: None,
            required: true,
        }
    }
}

impl TlsOptions {
    /// TLS exigido, raíces del sistema, sin certificado de cliente.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// CA propia que firma el certificado del broker.
    #[must_use]
    pub fn with_ca_file(mut self, ca: impl Into<PathBuf>) -> Self {
        self.ca_file = Some(ca.into());
        self
    }

    /// Certificado y clave de cliente (mTLS). Van juntos por construcción: un certificado
    /// sin su clave no se puede expresar, y no hay nada que descubrir en el handshake.
    #[must_use]
    pub fn with_client_cert(mut self, cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        self.client_cert = Some(cert.into());
        self.client_key = Some(key.into());
        self
    }

    /// Exige (o no) TLS. Solo tiene sentido `false` para el docker-compose de desarrollo.
    #[must_use]
    pub fn with_required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    /// Comprueba las opciones ANTES de tocar la red; si fallan, es
    /// [`FluxError::InvalidTlsOptions`] (`INVALID_TLS_OPTIONS`, 04-errors.md §5.3).
    ///
    /// `async-nats` lee los ficheros dentro de su bucle de conexión, y ahí un fichero
    /// inexistente es un error más que se reintenta. Un error de configuración debe parar el
    /// arranque.
    ///
    /// Los campos son públicos, así que «certificado sin clave» **sí** se puede expresar
    /// aunque [`Self::with_client_cert`] no lo permita: se rechaza, porque `connect()` solo
    /// cablea el par completo y un certificado de cliente suelto se ignoraría sin decir nada
    /// —el broker con mTLS rechazaría luego la conexión por una causa que no se ve—.
    pub(crate) fn validate(&self) -> Result<(), FluxError> {
        if self.client_cert.is_some() != self.client_key.is_some() {
            return Err(FluxError::InvalidTlsOptions(
                "`client_cert` y `client_key` van juntos (certificado de cliente para mTLS): \
                 con uno solo no hay certificado que presentar y se ignoraría sin decir nada"
                    .to_string(),
            ));
        }
        if let Some(ca) = &self.ca_file {
            check_pem(ca, "la CA", &["-----BEGIN CERTIFICATE-----"])?;
        }
        if let (Some(cert), Some(key)) = (&self.client_cert, &self.client_key) {
            check_pem(
                cert,
                "el certificado de cliente",
                &["-----BEGIN CERTIFICATE-----"],
            )?;
            check_pem(key, "la clave de cliente", &["PRIVATE KEY-----"])?;
        }
        Ok(())
    }
}

/// Lee el fichero y comprueba que contiene al menos uno de los marcadores PEM.
fn check_pem(path: &Path, what: &str, markers: &[&str]) -> Result<(), FluxError> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        FluxError::InvalidTlsOptions(format!("no se puede leer {what} `{}`: {e}", path.display()))
    })?;
    if markers.iter().any(|m| text.contains(m)) {
        return Ok(());
    }
    Err(FluxError::InvalidTlsOptions(format!(
        "{what} `{}` no parece un PEM (no contiene `{}`). Un fichero equivocado \
         —o una clave en DER— haría fallar el handshake con un error que no lo dice",
        path.display(),
        markers[0]
    )))
}

// ─── nkey ────────────────────────────────────────────────────────────────────

/// Comprueba que `seed` es una semilla nkey **de usuario** (`SU…`) bien formada.
///
/// `async-nats` la parsea al autenticarse, dentro de su bucle de reconexión; un valor mal
/// formado se convierte allí en un reintento infinito y silencioso. Aquí falla en alto, en
/// `connect()`. Nunca se incluye la semilla en el mensaje: acaba en un log.
pub(crate) fn validate_nkey_seed(seed: &str) -> Result<(), FluxError> {
    let pair = nkeys::KeyPair::from_seed(seed).map_err(|_| {
        FluxError::InvalidCredentials(
            "la semilla nkey no es válida: debe ser una semilla de usuario (`SU…`, 58 \
             caracteres). Se omite el valor a propósito: acabaría en un log"
                .to_string(),
        )
    })?;
    // 'U' = user. Una semilla de cuenta u operador (`SA…`, `SO…`) se parsea bien y el
    // broker la rechazaría después con un `authorization violation` sin explicación.
    if !pair.public_key().starts_with('U') {
        return Err(FluxError::InvalidCredentials(
            "la semilla nkey no es de usuario: una conexión se autentica con una semilla \
             `SU…`, no con la de cuenta (`SA…`) ni la de operador (`SO…`)"
                .to_string(),
        ));
    }
    Ok(())
}

// ─── Errores de conexión ─────────────────────────────────────────────────────

/// Qué fallo de la conexión inicial no se arregla reintentando.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FatalConnect {
    /// El broker rechazó la credencial, o el handshake TLS no encaja: `CONNECTION_REJECTED`.
    Rejected(RejectionReason),
    /// La lista de servidores no se entiende: no es un rechazo del broker, es la entrada.
    BadServers,
}

/// ¿Reintentar la conexión inicial arreglaría este error? `None` = sí, es transitorio.
///
/// `connect()` espera al broker con reintentos —un servicio que arranca antes que el broker
/// no debe morir en el primer intento— pero hay errores que reintentar no arregla: una
/// credencial rechazada, una CA que no encaja, un servidor que no habla TLS. Esos se
/// devuelven al instante; reintentarlos a ciegas es el síntoma «el servicio arranca, pasa el
/// healthcheck y no puede hacer nada».
///
/// El handshake TLS fallido llega de `async-nats` como `ConnectErrorKind::Io` —no como
/// `Tls`— con un `io::Error` de tipo `InvalidData` dentro, que es lo que `tokio-rustls`
/// produce al traducir un error de `rustls`. Por eso se mira la cadena de causas y no solo
/// la etiqueta. Observado contra nats-server 2.14.5, los tres con `kind = Io`:
///
/// ```text
/// CA que no firmó el certificado del broker   InvalidCertificate(UnknownIssuer)
/// TLS exigido, broker sin TLS                 InvalidMessage(InvalidContentType)
/// mTLS exigido, sin certificado de cliente    AlertReceived(CertificateRequired)
/// credencial rechazada                        kind = AuthorizationViolation
/// ```
pub(crate) fn fatal_connect_kind(err: &async_nats::ConnectError) -> Option<FatalConnect> {
    use async_nats::ConnectErrorKind as K;
    match err.kind() {
        K::AuthorizationViolation | K::Authentication => {
            Some(FatalConnect::Rejected(RejectionReason::Auth))
        }
        K::Tls => Some(FatalConnect::Rejected(RejectionReason::Tls)),
        K::ServerParse => Some(FatalConnect::BadServers),
        K::Io if invalid_data_cause(err).is_some() => {
            Some(FatalConnect::Rejected(RejectionReason::Tls))
        }
        _ => None,
    }
}

/// El `io::Error` de tipo `InvalidData` de la cadena de causas, si lo hay: es como llega un
/// fallo de `rustls` a través de `tokio-rustls`.
fn invalid_data_cause(err: &async_nats::ConnectError) -> Option<&io::Error> {
    let mut cursor = std::error::Error::source(err);
    while let Some(e) = cursor {
        if let Some(io) = e
            .downcast_ref::<io::Error>()
            .filter(|io| io.kind() == io::ErrorKind::InvalidData)
        {
            return Some(io);
        }
        cursor = e.source();
    }
    None
}

/// La causa de un rechazo, para el mensaje: el texto del error de TLS cuando lo hay
/// (`invalid peer certificate: UnknownIssuer`) y el del propio error si no.
///
/// No lleva credenciales: son los textos de `rustls` y de la autorización, que no las citan.
pub(crate) fn connect_error_detail(err: &async_nats::ConnectError) -> String {
    invalid_data_cause(err).map_or_else(|| err.to_string(), ToString::to_string)
}

// ─── Denegaciones del broker ─────────────────────────────────────────────────

/// Qué intentó hacer el cliente cuando el broker lo denegó.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeniedAction {
    Publish,
    Subscribe,
}

/// Una denegación de permisos que el broker comunicó por la conexión.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Denial {
    pub(crate) action: DeniedAction,
    pub(crate) subject: String,
}

impl Denial {
    /// El texto con el que se presenta al operador: el mismo vocabulario que el servidor
    /// usa en su log, para que se pueda buscar tal cual.
    pub(crate) fn describe(&self) -> String {
        let verb = match self.action {
            DeniedAction::Publish => "Publish",
            DeniedAction::Subscribe => "Subscription",
        };
        format!("{verb} to \"{}\"", self.subject)
    }
}

/// Interpreta un `-ERR` del servidor. Devuelve `None` si no es una denegación de permisos.
///
/// Formato observado en nats-server 2.14.5, con las comillas simples ya recortadas por
/// `async-nats`:
///
/// ```text
/// Permissions Violation for Publish to "$JS.API.STREAM.INFO.EVT_PEDIDOS"
/// Permissions Violation for Subscription to "_INBOX.otro.>"
/// ```
pub(crate) fn parse_violation(text: &str) -> Option<Denial> {
    let rest = text.strip_prefix("Permissions Violation for ")?;
    let (verb, rest) = rest.split_once(" to \"")?;
    let (subject, _) = rest.split_once('"')?;
    let action = match verb {
        "Publish" => DeniedAction::Publish,
        "Subscription" => DeniedAction::Subscribe,
        _ => return None,
    };
    Some(Denial {
        action,
        subject: subject.to_string(),
    })
}

/// Cuántas denegaciones recientes se recuerdan. Solo hace falta cubrir la ventana de una
/// petición en vuelo; un servicio con la ACL rota las genera sin parar y no debe crecer.
const KEEP: usize = 64;

struct State {
    seq: u64,
    recent: VecDeque<(u64, Denial)>,
}

/// Registro de las denegaciones del broker, con la capacidad de **esperar** una.
pub(crate) struct DenialLog {
    state: Mutex<State>,
    wake: watch::Sender<u64>,
}

impl DenialLog {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(State {
                seq: 0,
                recent: VecDeque::new(),
            }),
            wake: watch::channel(0).0,
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        // Dos operaciones triviales bajo el cerrojo: no pueden entrar en pánico, y si
        // alguna vez ocurriera en otro sitio, perder el registro es peor que usarlo.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Anota una denegación y despierta a quien esté esperando.
    pub(crate) fn record(&self, denial: Denial) {
        let seq = {
            let mut s = self.state();
            s.seq += 1;
            let seq = s.seq;
            if s.recent.len() == KEEP {
                s.recent.pop_front();
            }
            s.recent.push_back((seq, denial));
            seq
        };
        // `send_replace` y no `send`: funciona aunque nadie esté escuchando ahora mismo.
        self.wake.send_replace(seq);
    }

    /// Primera denegación posterior a `from` que cumple `matches`.
    fn since(&self, from: u64, matches: impl Fn(&Denial) -> bool) -> Option<Denial> {
        self.state()
            .recent
            .iter()
            .find(|(seq, d)| *seq > from && matches(d))
            .map(|(_, d)| d.clone())
    }

    /// Ejecuta `fut` **y** vigila una denegación que lo invalide.
    ///
    /// Gana la primera: la respuesta del broker, o la denegación de alguno de los
    /// `subjects` publicados —o de una suscripción bajo `inbox_prefix`, sin la cual la
    /// respuesta no podría llegar nunca—. Una denegación llega en milisegundos; sin esta
    /// carrera, el mismo fallo es un timeout de 5 a 30 s.
    ///
    /// Una denegación anterior a la llamada no cuenta: se compara contra la marca tomada
    /// **antes** de empezar, para no culpar a esta operación de lo que falló la anterior.
    pub(crate) async fn guard<T, F>(
        &self,
        inbox_prefix: &str,
        subjects: &[String],
        fut: F,
    ) -> Result<T, Denial>
    where
        F: Future<Output = T>,
    {
        // Primero suscribirse y después tomar la marca: así no hay hueco por el que se
        // cuele una denegación sin despertarnos.
        let mut rx = self.wake.subscribe();
        let from = self.state().seq;
        let inbox = format!("{inbox_prefix}.");
        let relevant = |d: &Denial| match d.action {
            DeniedAction::Publish => subjects.contains(&d.subject),
            DeniedAction::Subscribe => d.subject.starts_with(&inbox),
        };

        tokio::pin!(fut);
        loop {
            tokio::select! {
                out = &mut fut => return Ok(out),
                changed = rx.changed() => {
                    if changed.is_err() {
                        // El emisor vive en `self`, así que no ocurre; si ocurriera,
                        // simplemente no hay nada que vigilar.
                        return Ok(fut.await);
                    }
                    if let Some(d) = self.since(from, relevant) {
                        return Err(d);
                    }
                }
            }
        }
    }
}

/// Directorio temporal de un test, borrado al soltarlo (también si el test entra en pánico).
#[cfg(test)]
pub(crate) struct TmpDir(PathBuf);

#[cfg(test)]
impl TmpDir {
    pub(crate) fn new() -> Self {
        static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "flux-unit-{}-{}",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    pub(crate) fn file(&self, nombre: &str, contenido: &str) -> PathBuf {
        let p = self.0.join(nombre);
        std::fs::write(&p, contenido).unwrap();
        p
    }
}

#[cfg(test)]
impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn publish(subject: &str) -> Denial {
        Denial {
            action: DeniedAction::Publish,
            subject: subject.to_string(),
        }
    }

    #[test]
    fn se_parsean_las_dos_denegaciones_que_emite_el_servidor() {
        // Cadenas literales observadas en nats-server 2.14.5.
        assert_eq!(
            parse_violation(
                "Permissions Violation for Publish to \"$JS.API.STREAM.INFO.EVT_PEDIDOS\""
            ),
            Some(publish("$JS.API.STREAM.INFO.EVT_PEDIDOS"))
        );
        assert_eq!(
            parse_violation("Permissions Violation for Subscription to \"_INBOX.otro.>\""),
            Some(Denial {
                action: DeniedAction::Subscribe,
                subject: "_INBOX.otro.>".to_string(),
            })
        );
    }

    /// Lo incorrecto NO se interpreta: un `-ERR` que no es una denegación de permisos no
    /// puede convertirse en un `PERMISSION_DENIED`.
    #[test]
    fn otros_errores_del_servidor_no_son_denegaciones() {
        for text in [
            "Authorization Violation",
            "Unknown Protocol Operation",
            "Maximum Connections Exceeded",
            "Permissions Violation for Other to \"x\"",
            "Permissions Violation for Publish to x",
            "",
        ] {
            assert_eq!(parse_violation(text), None, "{text}");
        }
    }

    #[test]
    fn la_descripcion_reproduce_el_vocabulario_del_servidor() {
        assert_eq!(
            publish("pedidos.pedido.v1.creado").describe(),
            "Publish to \"pedidos.pedido.v1.creado\""
        );
    }

    /// Sin denegación, `guard` es transparente: devuelve lo que devuelve la operación.
    #[tokio::test]
    async fn sin_denegacion_gana_la_respuesta() {
        let log = DenialLog::new();
        let out = log
            .guard("_INBOX.svc", &["x".to_string()], async { 7 })
            .await;
        assert_eq!(out, Ok(7));
    }

    /// Una denegación del subject publicado gana a una operación que nunca responde — el
    /// caso real: el servidor descarta la petición y `async-nats` espera hasta su timeout.
    #[tokio::test]
    async fn una_denegacion_del_subject_gana_a_una_respuesta_que_no_llega() {
        let log = std::sync::Arc::new(DenialLog::new());
        let l2 = log.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            l2.record(publish("$JS.API.STREAM.INFO.EVT_PEDIDOS"));
        });
        let out = tokio::time::timeout(
            Duration::from_secs(5),
            log.guard(
                "_INBOX.svc",
                &["$JS.API.STREAM.INFO.EVT_PEDIDOS".to_string()],
                std::future::pending::<()>(),
            ),
        )
        .await
        .expect("la denegación debe despertar al guard mucho antes de 5 s");
        assert_eq!(out, Err(publish("$JS.API.STREAM.INFO.EVT_PEDIDOS")));
    }

    /// Una denegación de OTRO subject no invalida esta operación: sin esto, la ACL rota de
    /// una tarea concurrente haría fallar peticiones que no tienen nada que ver.
    #[tokio::test]
    async fn una_denegacion_ajena_no_cuenta() {
        let log = std::sync::Arc::new(DenialLog::new());
        let l2 = log.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            l2.record(publish("$JS.API.STREAM.INFO.OTRO"));
            l2.record(Denial {
                action: DeniedAction::Subscribe,
                subject: "_INBOX.otro.>".to_string(),
            });
        });
        let out = log
            .guard("_INBOX.svc", &["x".to_string()], async {
                tokio::time::sleep(Duration::from_millis(80)).await;
                "respuesta"
            })
            .await;
        assert_eq!(out, Ok("respuesta"));
    }

    /// Sin permiso de suscripción sobre el inbox propio la respuesta no puede llegar, y la
    /// causa no es el subject publicado sino el inbox: se reporta como denegación.
    #[tokio::test]
    async fn la_denegacion_del_inbox_propio_cuenta() {
        let log = std::sync::Arc::new(DenialLog::new());
        let l2 = log.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            l2.record(Denial {
                action: DeniedAction::Subscribe,
                subject: "_INBOX.svc.abc.*".to_string(),
            });
        });
        let out = log
            .guard(
                "_INBOX.svc",
                &["x".to_string()],
                std::future::pending::<()>(),
            )
            .await;
        assert!(out.is_err());
    }

    /// Una denegación ANTERIOR a la llamada no se le imputa: es de la operación anterior.
    #[tokio::test]
    async fn una_denegacion_previa_no_se_imputa_a_la_operacion_siguiente() {
        let log = DenialLog::new();
        log.record(publish("x"));
        let out = log
            .guard("_INBOX.svc", &["x".to_string()], async { "ok" })
            .await;
        assert_eq!(out, Ok("ok"));
    }

    #[test]
    fn el_registro_no_crece_sin_limite() {
        let log = DenialLog::new();
        for i in 0..(KEEP * 3) {
            log.record(publish(&format!("s{i}")));
        }
        assert_eq!(log.state().recent.len(), KEEP);
    }

    // ── nkey ──────────────────────────────────────────────────────────────────

    #[test]
    fn una_semilla_de_usuario_valida_se_acepta() {
        let seed = nkeys::KeyPair::new_user().seed().unwrap();
        assert!(validate_nkey_seed(&seed).is_ok());
    }

    /// Los tres modos de equivocarse: basura, una semilla de otro tipo y una clave pública.
    /// Ninguno puede llegar al broker, y ninguno puede imprimir el valor recibido.
    #[test]
    fn una_semilla_mal_formada_falla_sin_imprimirla() {
        let cuenta = nkeys::KeyPair::new_account().seed().unwrap();
        let publica = nkeys::KeyPair::new_user().public_key();
        for malo in ["", "no-es-una-semilla", &cuenta, &publica] {
            let err = validate_nkey_seed(malo).unwrap_err();
            assert!(matches!(err, FluxError::InvalidCredentials(_)), "{err:?}");
            assert_eq!(err.code(), Some("INVALID_CREDENTIALS"));
            let texto = err.to_string();
            if !malo.is_empty() {
                assert!(!texto.contains(malo), "no debe volcar el valor: {texto}");
            }
        }
        assert!(validate_nkey_seed(&cuenta)
            .unwrap_err()
            .to_string()
            .contains("no es de usuario"));
    }

    // ── TLS ───────────────────────────────────────────────────────────────────

    #[test]
    fn tls_exigido_por_defecto() {
        assert!(TlsOptions::new().required);
        assert!(!TlsOptions::new().with_required(false).required);
    }

    #[test]
    fn los_pem_correctos_se_aceptan() {
        let dir = TmpDir::new();
        let ca = dir.file(
            "ca.pem",
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        );
        let key = dir.file(
            "key.pem",
            "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n",
        );
        assert!(TlsOptions::new().with_ca_file(&ca).validate().is_ok());
        assert!(TlsOptions::new()
            .with_ca_file(&ca)
            .with_client_cert(&ca, &key)
            .validate()
            .is_ok());
    }

    /// Lo incorrecto falla en `validate`, antes de tocar la red: fichero inexistente,
    /// fichero que no es PEM, y la clave pasada donde va el certificado.
    #[test]
    fn los_ficheros_tls_equivocados_fallan_en_alto() {
        let dir = TmpDir::new();
        let ca = dir.file(
            "ca2.pem",
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        );
        let key = dir.file(
            "key2.pem",
            "-----BEGIN EC PRIVATE KEY-----\nAAAA\n-----END EC PRIVATE KEY-----\n",
        );
        let basura = dir.file("basura.pem", "esto no es un PEM");

        let inexistente = TlsOptions::new().with_ca_file("/no/existe/ca.pem");
        assert!(inexistente
            .validate()
            .unwrap_err()
            .to_string()
            .contains("no se puede leer la CA"));

        let err = TlsOptions::new().with_ca_file(&basura).validate();
        assert!(err.unwrap_err().to_string().contains("no parece un PEM"));

        // La clave donde va el certificado de cliente, y el certificado donde va la clave.
        assert!(TlsOptions::new()
            .with_client_cert(&key, &key)
            .validate()
            .is_err());
        assert!(TlsOptions::new()
            .with_client_cert(&ca, &ca)
            .validate()
            .is_err());

        // Todos son INVALID_TLS_OPTIONS —el código estable de 04-errors.md §5.3—, y ninguno
        // otro: ni un `Config` genérico, ni un rechazo del broker.
        for malo in [
            TlsOptions::new().with_ca_file("/no/existe/ca.pem"),
            TlsOptions::new().with_ca_file(&basura),
            TlsOptions::new().with_client_cert(&key, &key),
            TlsOptions::new().with_client_cert(&ca, &ca),
        ] {
            let err = malo.validate().unwrap_err();
            assert!(matches!(err, FluxError::InvalidTlsOptions(_)), "{err:?}");
            assert_eq!(err.code(), Some("INVALID_TLS_OPTIONS"));
            assert!(err.to_string().contains("opciones TLS inválidas"), "{err}");
        }
    }

    /// Los campos son públicos, así que un certificado de cliente sin su clave (o al revés)
    /// SÍ se puede construir. Sin esta comprobación `connect()` lo ignoraría sin decir nada.
    /// Y el par completo, que es lo correcto, pasa.
    #[test]
    fn el_certificado_de_cliente_y_su_clave_van_juntos() {
        let dir = TmpDir::new();
        let cert = dir.file(
            "c.pem",
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        );
        let key = dir.file(
            "k.pem",
            "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n",
        );
        let solo_cert = TlsOptions {
            client_cert: Some(cert.clone()),
            ..TlsOptions::new()
        };
        let solo_clave = TlsOptions {
            client_key: Some(key.clone()),
            ..TlsOptions::new()
        };
        for malo in [solo_cert, solo_clave] {
            let err = malo.validate().unwrap_err();
            assert!(matches!(err, FluxError::InvalidTlsOptions(_)), "{err:?}");
            assert!(err.to_string().contains("van juntos"), "{err}");
        }
        assert!(TlsOptions::new()
            .with_client_cert(&cert, &key)
            .validate()
            .is_ok());
    }

    // ── Fallos de la conexión inicial ─────────────────────────────────────────

    use async_nats::{ConnectError, ConnectErrorKind as K};

    fn io_invalid_data(texto: &str) -> ConnectError {
        ConnectError::with_source(
            K::Io,
            io::Error::new(io::ErrorKind::InvalidData, texto.to_string()),
        )
    }

    /// Lo que reintentar no arregla se reconoce, y con el motivo correcto: la credencial es
    /// `Auth`; todo lo del handshake TLS —incluido el que `async-nats` etiqueta `Io`, no
    /// `Tls` (A-30)— es `Tls`.
    #[test]
    fn los_fallos_que_esperar_no_arregla_se_reconocen_con_su_motivo() {
        use RejectionReason::{Auth, Tls};
        let rechazo = |r| Some(FatalConnect::Rejected(r));
        assert_eq!(
            fatal_connect_kind(&ConnectError::new(K::AuthorizationViolation)),
            rechazo(Auth)
        );
        assert_eq!(
            fatal_connect_kind(&ConnectError::new(K::Authentication)),
            rechazo(Auth)
        );
        assert_eq!(fatal_connect_kind(&ConnectError::new(K::Tls)), rechazo(Tls));
        // Los tres handshakes de A-30, tal como llegan: `Io` con un `InvalidData` dentro.
        for texto in [
            "invalid peer certificate: UnknownIssuer",
            "received corrupt message of type InvalidContentType",
            "received fatal alert: CertificateRequired",
        ] {
            assert_eq!(fatal_connect_kind(&io_invalid_data(texto)), rechazo(Tls));
        }
        assert_eq!(
            fatal_connect_kind(&ConnectError::new(K::ServerParse)),
            Some(FatalConnect::BadServers)
        );
    }

    /// Lo contrario, y es lo que justifica el bucle de espera: un broker que aún no ha
    /// arrancado (conexión rechazada, DNS, timeout) **no** es un rechazo y se espera. Sin
    /// este caso, `connect()` moriría en el primer intento de cada despliegue.
    #[test]
    fn un_broker_que_aun_no_esta_no_es_un_rechazo() {
        let refused = io::Error::from(io::ErrorKind::ConnectionRefused);
        for transitorio in [
            ConnectError::with_source(K::Io, refused),
            ConnectError::with_source(K::Io, io::Error::from(io::ErrorKind::TimedOut)),
            // Un `Io` sin causa: tampoco es un `InvalidData`.
            ConnectError::new(K::Io),
            ConnectError::new(K::Dns),
            ConnectError::new(K::TimedOut),
            ConnectError::new(K::MaxReconnects),
        ] {
            assert_eq!(fatal_connect_kind(&transitorio), None, "{transitorio:?}");
        }
    }

    /// La causa del mensaje es el texto del error de TLS cuando lo hay, y el del propio error
    /// si no.
    #[test]
    fn la_causa_del_rechazo_sale_del_error_de_tls() {
        assert_eq!(
            connect_error_detail(&io_invalid_data("invalid peer certificate: UnknownIssuer")),
            "invalid peer certificate: UnknownIssuer"
        );
        assert_eq!(
            connect_error_detail(&ConnectError::new(K::AuthorizationViolation)),
            "authorization violation"
        );
    }
}
