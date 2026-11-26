//! Seguridad ejecutada contra un `nats-server` REAL — plan v1.1, Fase 1.
//!
//! Estos tests arrancan su propio broker, con una configuración escrita por el propio test:
//! usuarios con permisos **exactos** —la forma de `docs/plan-v1.1/evidencia/candidata.conf`,
//! ampliada con lo que este SDK usa de verdad— y, para el contraste, los permisos de la ACL
//! **antigua** que generaba `scripts/gen-acl.mjs` v1.
//!
//! Cada comprobación se hace **en los dos sentidos**: lo concedido funciona y lo no
//! concedido falla. Un test que solo se ha visto en verde no demuestra que el broker confine.
//!
//! ```text
//! NATS_SERVER=/ruta/a/nats-server cargo test --test security
//! ```
//!
//! Sin binario, los tests se **saltan con un aviso**… salvo que `FLUX_REQUIRE_BROKER=1`: en
//! CI eso los convierte en un fallo. Un fixture que nadie ejecuta aparenta cobertura.
//!
//! Los puertos salen del rango `FLUX_TEST_PORT_BASE` (default 4450) + 50.

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_nats::jetstream::{self, consumer, stream};
use flux::{
    connect, ConnectOptions, Credentials, Delivery, ErrorClass, Event, FluxError, LogFn, LogLevel,
    PublishOptions, RejectionReason, SubscribeOptions, TenantIsolation, TlsOptions,
};
use futures::StreamExt;
use serde_json::json;

// ═════════════════════════════════════════════════════════════════════════════
//  Broker de prueba
// ═════════════════════════════════════════════════════════════════════════════

/// `FLUX_REQUIRE_BROKER` definida y distinta de `0`/vacío.
fn broker_required() -> bool {
    std::env::var("FLUX_REQUIRE_BROKER").is_ok_and(|v| !v.is_empty() && v != "0")
}

/// El binario de `nats-server`: `NATS_SERVER`, o el que haya en el `PATH`.
fn nats_server_bin() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("NATS_SERVER").filter(|p| !p.is_empty()) {
        return Some(PathBuf::from(p)).filter(|p| p.is_file());
    }
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|d| d.join("nats-server"))
        .find(|p| p.is_file())
}

/// Sin binario: salta con aviso, o FALLA si el entorno exige broker (CI).
fn sin_broker() {
    assert!(
        !broker_required(),
        "FLUX_REQUIRE_BROKER=1 y no hay nats-server: define NATS_SERVER con la ruta del binario \
         (o ponlo en el PATH). Sin esto los tests de seguridad no se ejecutan, y un fixture que \
         nadie ejecuta aparenta cobertura."
    );
    eprintln!(
        "saltado: no hay nats-server (define NATS_SERVER). Con FLUX_REQUIRE_BROKER=1 fallaría."
    );
}

macro_rules! broker_bin {
    () => {
        match nats_server_bin() {
            Some(b) => b,
            None => {
                sin_broker();
                return;
            }
        }
    };
}

static NEXT_PORT: AtomicU16 = AtomicU16::new(0);
static NEXT_DIR: AtomicU16 = AtomicU16::new(0);

/// Un puerto libre del rango propio. Se comprueba con un `bind` real: otros procesos usan
/// otros rangos, pero un servidor anterior que no terminó de morir no debe romper este.
fn free_port() -> u16 {
    let base: u16 = std::env::var("FLUX_TEST_PORT_BASE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4450);
    const SPAN: u16 = 50;
    for _ in 0..SPAN {
        let port = base + NEXT_PORT.fetch_add(1, Ordering::SeqCst) % SPAN;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("no queda ningún puerto libre en {base}..{}", base + SPAN);
}

/// Directorio temporal que se borra al soltarlo —también si el test entra en pánico—.
/// Se comporta como un `Path` (`dir.join(…)`, `dir.display()`).
struct Tmp(PathBuf);

impl Tmp {
    fn new(prefijo: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "flux-{prefijo}-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).expect("directorio temporal");
        Self(dir)
    }
}

impl std::ops::Deref for Tmp {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Un `nats-server` con JetStream, su configuración y su directorio temporal. Al soltarlo
/// mata SOLO su proceso y borra su almacén.
struct Broker {
    child: Child,
    port: u16,
    dir: PathBuf,
}

impl Broker {
    /// `top` es configuración de nivel superior (TLS…) y `accounts` el bloque `accounts {…}`
    /// o cualquier otra forma de autenticación.
    fn start(bin: &Path, top: &str, auth: &str) -> Self {
        let port = free_port();
        let dir = std::env::temp_dir().join(format!(
            "flux-sec-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).expect("directorio temporal");
        let conf = format!(
            "host: 127.0.0.1\nport: {port}\njetstream {{ store_dir: \"{}\" }}\n{top}\n{auth}\n",
            dir.join("js").display()
        );
        let conf_path = dir.join("nats.conf");
        std::fs::write(&conf_path, conf).expect("config");

        let mut child = Command::new(bin)
            .arg("-c")
            .arg(&conf_path)
            .arg("-l")
            .arg(dir.join("server.log"))
            .stdout(Stdio::null())
            // Un error de configuración sale por stderr y no llega al log (`-l`).
            .stderr(std::fs::File::create(dir.join("server.err")).expect("stderr"))
            .spawn()
            .expect("no se pudo lanzar nats-server");

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if std::net::TcpStream::connect_timeout(
                &([127, 0, 0, 1], port).into(),
                Duration::from_millis(200),
            )
            .is_ok()
            {
                break;
            }
            if let Ok(Some(status)) = child.try_wait() {
                let log = std::fs::read_to_string(dir.join("server.log")).unwrap_or_default();
                let err = std::fs::read_to_string(dir.join("server.err")).unwrap_or_default();
                let _ = std::fs::remove_dir_all(&dir);
                panic!("nats-server terminó ({status}) antes de aceptar conexiones:\n{log}\n{err}");
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                let _ = std::fs::remove_dir_all(&dir);
                panic!("nats-server no abrió el puerto {port}");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Self { child, port, dir }
    }

    fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.join("server.log")).unwrap_or_default()
    }

    /// Las violaciones de permisos que el servidor registró. Formato observado en
    /// nats-server 2.14.5: `Publish Violation - Subject "…"` y `Subscription Violation -
    /// Subject "…"`. (No `Permissions Violation`: ese es el texto que llega al CLIENTE por
    /// el `-ERR`, y buscarlo en el log del servidor haría vacía cualquier aserción negativa.)
    fn violations(&self) -> Vec<String> {
        self.log()
            .lines()
            .filter(|l| l.contains("Violation - Subject"))
            .map(ToString::to_string)
            .collect()
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// ═════════════════════════════════════════════════════════════════════════════
//  Permisos
// ═════════════════════════════════════════════════════════════════════════════

/// Permisos de publicación y de suscripción de un usuario.
#[derive(Clone, Default)]
struct Perms {
    publish: Vec<String>,
    subscribe: Vec<String>,
}

impl Perms {
    /// **Dueño de un dominio**: publica en él y consulta su stream.
    ///
    /// El stream `EVT_<DOMINIO>` lo aprovisiona `flux provision`: el servicio no recibe
    /// `STREAM.CREATE` (06-security.md §3), solo poder consultarlo.
    fn owner(service: &str, domain: &str) -> Self {
        Self {
            publish: vec![
                format!("{domain}.>"),
                format!("$JS.API.STREAM.INFO.{}", flux::stream_name(domain)),
            ],
            subscribe: vec![format!("{}.>", flux::inbox_prefix(service))],
        }
    }

    /// **Consumidor de un subject**: exactamente lo que el SDK usa para consumirlo.
    ///
    /// Sin ningún permiso de suscripción sobre subjects de dominio: se consume por pull y lo
    /// que confina es `$JS.API`. Ver la lista y su justificación en el README del SDK.
    fn consumer(service: &str, subject: &str) -> Self {
        let domain = subject.split('.').next().expect("dominio");
        let evt = flux::stream_name(domain);
        let durable = flux::durable_name(service, subject).expect("durable");
        Self {
            publish: vec![
                format!("$JS.API.STREAM.INFO.{evt}"),
                // El SDK comprueba también la DLQ al suscribirse: sin ella no podría
                // enrutar un fallo. NO estaba en `candidata.conf`.
                format!("$JS.API.STREAM.INFO.{}", flux::dlq_stream_name(domain)),
                format!("$JS.API.CONSUMER.CREATE.{evt}.{durable}.{subject}"),
                format!("$JS.API.CONSUMER.INFO.{evt}.{durable}"),
                format!("$JS.API.CONSUMER.MSG.NEXT.{evt}.{durable}"),
                format!("$JS.ACK.{evt}.{durable}.>"),
                format!("dlq.{subject}"),
            ],
            subscribe: vec![format!("{}.>", flux::inbox_prefix(service))],
        }
    }

    /// Une dos conjuntos de permisos sin duplicar.
    fn and(mut self, other: Self) -> Self {
        for p in other.publish {
            if !self.publish.contains(&p) {
                self.publish.push(p);
            }
        }
        for s in other.subscribe {
            if !self.subscribe.contains(&s) {
                self.subscribe.push(s);
            }
        }
        self
    }

    /// Quita los permisos de publicación que empiezan por `prefix`.
    fn without(mut self, prefix: &str) -> Self {
        self.publish.retain(|p| !p.starts_with(prefix));
        self
    }
}

/// Un usuario de la configuración con sus permisos.
fn user_block(auth: &str, perms: &Perms) -> String {
    let list = |v: &[String]| {
        v.iter()
            .map(|s| format!("\"{s}\""))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "{{ {auth}, permissions: {{ publish: {{ allow: [{}] }}, subscribe: {{ allow: [{}] }} }} }}",
        list(&perms.publish),
        list(&perms.subscribe)
    )
}

fn pw_user(name: &str, perms: &Perms) -> String {
    user_block(&format!("user: \"{name}\", password: \"pw\""), perms)
}

/// `accounts { PROD: {…} }` con un usuario `ops` SIN restricciones: hace de infraestructura
/// (`flux provision`) y de testigo.
fn accounts(users: &[String]) -> String {
    let mut all = vec!["{ user: \"ops\", password: \"pw\" }".to_string()];
    all.extend(users.iter().cloned());
    format!(
        "accounts {{ PROD: {{ jetstream: enabled, users: [ {} ] }} }}",
        all.join(",\n")
    )
}

// ─── La ACL ANTIGUA ──────────────────────────────────────────────────────────

/// Permisos de `facturacion-api` tal cual los generaba `scripts/gen-acl.mjs` v1:
/// `node scripts/gen-acl.mjs examples/services.json` en el commit a4db4b8.
///
/// **Congelado a propósito**: es el contraste de la regresión. No concede nada de
/// `$JS.API.*` —y JetStream se consume por ahí— y concede el inbox común `_INBOX.>`.
fn old_acl_facturacion() -> Perms {
    Perms {
        publish: vec!["facturacion.>".into(), "dlq.pedidos.pedido.v1.>".into()],
        subscribe: vec![
            "pedidos.pedido.v1.>".into(),
            "dlq.facturacion.>".into(),
            "_INBOX.>".into(),
        ],
    }
}

// ═════════════════════════════════════════════════════════════════════════════
//  Aprovisionamiento y testigos (con el usuario `ops`, API cruda de NATS)
// ═════════════════════════════════════════════════════════════════════════════

async fn ops(broker: &Broker) -> async_nats::Client {
    async_nats::ConnectOptions::with_user_and_password("ops".into(), "pw".into())
        .connect(broker.url())
        .await
        .expect("conexión de ops")
}

/// Crea `EVT_<D>` y `DLQ_<D>` como lo haría `flux provision`.
async fn provision_streams(ops: &async_nats::Client, domains: &[&str]) {
    let js = jetstream::new(ops.clone());
    for d in domains {
        js.create_stream(stream::Config {
            name: flux::stream_name(d),
            subjects: vec![format!("{d}.>")],
            storage: stream::StorageType::File,
            retention: stream::RetentionPolicy::Limits,
            discard: stream::DiscardPolicy::Old,
            max_age: flux::STREAM_MAX_AGE,
            duplicate_window: flux::DUPLICATE_WINDOW,
            ..Default::default()
        })
        .await
        .expect("crear stream de eventos");
        js.create_stream(stream::Config {
            name: flux::dlq_stream_name(d),
            subjects: vec![format!("dlq.{d}.>")],
            storage: stream::StorageType::File,
            retention: stream::RetentionPolicy::Limits,
            discard: stream::DiscardPolicy::Old,
            max_age: flux::DLQ_STREAM_MAX_AGE,
            ..Default::default()
        })
        .await
        .expect("crear stream de DLQ");
    }
}

/// Crea el consumidor durable con la config canónica de `03-delivery.md` §2.
async fn provision_consumer(ops: &async_nats::Client, service: &str, subject: &str) {
    let domain = subject.split('.').next().expect("dominio");
    jetstream::new(ops.clone())
        .create_consumer_on_stream(
            consumer::pull::Config {
                durable_name: Some(flux::durable_name(service, subject).expect("durable")),
                filter_subject: subject.to_string(),
                ack_policy: consumer::AckPolicy::Explicit,
                ack_wait: flux::DEFAULT_ACK_WAIT,
                max_deliver: i64::from(flux::DEFAULT_MAX_DELIVER),
                backoff: flux::CANONICAL_BACKOFF.to_vec(),
                max_ack_pending: i64::from(flux::DEFAULT_MAX_ACK_PENDING),
                deliver_policy: consumer::DeliverPolicy::All,
                replay_policy: consumer::ReplayPolicy::Instant,
                ..Default::default()
            },
            flux::stream_name(domain),
        )
        .await
        .expect("crear consumidor");
}

/// Estado del consumidor visto por `ops`: (pendientes de ack, suelo de ack, redeliveries).
async fn consumer_state(
    ops: &async_nats::Client,
    service: &str,
    subject: &str,
) -> (usize, u64, i64) {
    let domain = subject.split('.').next().expect("dominio");
    let stream = jetstream::new(ops.clone())
        .get_stream(flux::stream_name(domain))
        .await
        .expect("stream");
    let info = stream
        .consumer_info(flux::durable_name(service, subject).expect("durable"))
        .await
        .expect("info del consumidor");
    (
        info.num_ack_pending,
        info.ack_floor.stream_sequence,
        i64::try_from(info.num_redelivered).unwrap_or(-1),
    )
}

/// Los eventos que hay en la DLQ de un dominio, leídos con la API cruda.
async fn dlq_events(ops: &async_nats::Client, domain: &str) -> Vec<Event> {
    let stream = jetstream::new(ops.clone())
        .get_stream(flux::dlq_stream_name(domain))
        .await
        .expect("stream DLQ");
    let mut stream = stream;
    let info = stream.info().await.expect("info").state.clone();
    let mut out = vec![];
    for seq in info.first_sequence..=info.last_sequence {
        if let Ok(m) = stream.get_raw_message(seq).await {
            out.push(flux::parse_event(&m.payload).expect("la DLQ guarda eventos válidos"));
        }
    }
    out
}

/// Un mensaje visto por el testigo.
#[derive(Clone, Debug)]
struct Seen {
    subject: String,
    reply: String,
    payload: String,
}

/// Se suscribe a `>` con el usuario sin restricciones y lo apunta todo.
async fn sniff(ops: &async_nats::Client) -> Arc<Mutex<Vec<Seen>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let mut sub = ops.subscribe(">").await.expect("sniffer");
    tokio::spawn(async move {
        while let Some(m) = sub.next().await {
            sink.lock().unwrap().push(Seen {
                subject: m.subject.to_string(),
                reply: m
                    .reply
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                payload: String::from_utf8_lossy(&m.payload)
                    .chars()
                    .take(12)
                    .collect(),
            });
        }
    });
    ops.flush().await.expect("flush");
    seen
}

/// `*` casa un token, `>` casa el resto — la semántica de los subjects de NATS.
fn nats_match(pattern: &str, subject: &str) -> bool {
    let mut p = pattern.split('.');
    let mut s = subject.split('.');
    loop {
        match (p.next(), s.next()) {
            (Some(">"), Some(_)) | (None, None) => return true,
            (Some("*"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            _ => return false,
        }
    }
}

#[test]
fn el_comparador_de_subjects_se_comporta_como_nats() {
    assert!(nats_match("a.>", "a.b.c"));
    assert!(nats_match("a.*.c", "a.b.c"));
    assert!(nats_match("a.b", "a.b"));
    assert!(!nats_match("a.>", "a"));
    assert!(!nats_match("a.b", "a.b.c"));
    assert!(!nats_match("a.*", "a.b.c"));
    assert!(!nats_match("_INBOX.fx.>", "_INBOX.fx-fact.1"));
}

async fn wait_for(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    cond()
}

// ═════════════════════════════════════════════════════════════════════════════
//  Clientes flux
// ═════════════════════════════════════════════════════════════════════════════

fn capture_log() -> (LogFn, Arc<Mutex<Vec<String>>>) {
    let lines = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&lines);
    let log: LogFn = Arc::new(move |level: LogLevel, msg: &str| {
        sink.lock().unwrap().push(format!("{level:?} {msg}"));
    });
    (log, lines)
}

fn opts(broker: &Broker, service: &str) -> ConnectOptions {
    ConnectOptions::new(broker.url(), service, "test", "1.0.0")
        .with_credentials(Credentials::UserPassword {
            user: service.to_string(),
            password: "pw".to_string(),
        })
        .with_tenant_id("acme")
        .with_schema_base_url("https://schemas.internal")
}

fn denials(lines: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    lines
        .lock()
        .unwrap()
        .iter()
        .filter(|l| l.contains("el broker denegó"))
        .cloned()
        .collect()
}

/// Lo que el handler vio de cada evento.
type Calls = Arc<Mutex<Vec<(String, u32)>>>;

/// Un handler que se comporta según `data.caso`: `ok`, `permanente`, `reintento` (falla en
/// la 1ª entrega y acepta en la 2ª) o `lento` (tarda 16 s: más que `ack_wait / 2`).
fn handler(
    calls: &Calls,
) -> impl Fn(
    Event,
    Delivery,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = flux::HandlerResult> + Send>>
       + Send
       + Sync
       + 'static {
    let calls = Arc::clone(calls);
    move |ev: Event, d: Delivery| {
        let calls = Arc::clone(&calls);
        Box::pin(async move {
            let data: serde_json::Value = serde_json::from_str(ev.data.get()).expect("json");
            let caso = data["caso"].as_str().unwrap_or("ok").to_string();
            calls.lock().unwrap().push((caso.clone(), d.attempt));
            match caso.as_str() {
                "permanente" => Err(FluxError::permanent("pedido ya cancelado")
                    .with_code("PEDIDO_YA_CANCELADO")
                    .into()),
                "reintento" if d.attempt == 1 => Err(FluxError::retryable("proveedor 503")
                    .with_code("HTTP_503")
                    .with_retry_after(Duration::from_millis(300))
                    .into()),
                "lento" => {
                    tokio::time::sleep(Duration::from_secs(16)).await;
                    Ok(())
                }
                _ => Ok(()),
            }
        })
    }
}

const SUBJECT: &str = "pedidos.pedido.v1.creado";

// ═════════════════════════════════════════════════════════════════════════════
//  (a) Extremo a extremo con inbox privado y permisos exactos
// ═════════════════════════════════════════════════════════════════════════════

/// Publicar, suscribirse, ack, DLQ por error permanente y reintento: todo de extremo a
/// extremo bajo los permisos exactos, **sin una sola denegación**.
///
/// Además fija la lista de permisos: cada subject que el SDK usa está en la ACL (si no, el
/// broker lo habría denegado), y **cada permiso de la ACL se usa** — ni falta ni sobra.
#[tokio::test]
async fn a_extremo_a_extremo_con_inbox_privado_y_permisos_exactos() {
    let bin = broker_bin!();

    let pedidos = Perms::owner("pedidos-api", "pedidos");
    let facturacion = Perms::owner("facturacion-api", "facturacion")
        .and(Perms::consumer("facturacion-api", SUBJECT));
    let broker = Broker::start(
        &bin,
        "",
        &accounts(&[
            pw_user("pedidos-api", &pedidos),
            pw_user("facturacion-api", &facturacion),
        ]),
    );
    let ops = ops(&broker).await;
    provision_streams(&ops, &["pedidos", "facturacion"]).await;
    let seen = sniff(&ops).await;

    let (log, lines) = capture_log();
    let productor = connect(opts(&broker, "pedidos-api"))
        .await
        .expect("conexión");
    let consumidor = connect(
        opts(&broker, "facturacion-api")
            .with_logger(log)
            // Con métricas y sondeo rápido el SDK también usa `CONSUMER.INFO`.
            .with_metrics(Arc::new(flux::InMemoryMetrics::new()))
            .with_pending_poll(Duration::from_millis(250)),
    )
    .await
    .expect("conexión");

    let calls: Calls = Arc::new(Mutex::new(Vec::new()));
    let sub = consumidor
        .subscribe(SUBJECT, handler(&calls))
        .await
        .expect("suscripción bajo permisos exactos");

    for (i, caso) in ["ok", "permanente", "reintento"].into_iter().enumerate() {
        productor
            .publish_with(
                SUBJECT,
                &json!({ "pedidoId": format!("ped-{i}"), "caso": caso, "aggregateVersion": 1 }),
                PublishOptions::default().with_aggregate_id(format!("ped-{i}")),
            )
            .await
            .expect("el dueño del dominio publica");
    }
    // El consumidor, además, es dueño de su propio dominio.
    consumidor
        .publish(
            "facturacion.factura.v1.emitida",
            &json!({ "facturaId": "f-1" }),
        )
        .await
        .expect("publicar en el propio dominio");

    // ok (1) + permanente (1) + reintento (2: falla y acepta) = 4 ejecuciones.
    assert!(
        wait_for(Duration::from_secs(10), || calls.lock().unwrap().len() >= 4).await,
        "el consumo no completó: {:?}",
        calls.lock().unwrap()
    );
    tokio::time::sleep(Duration::from_millis(800)).await; // acks y DLQ en vuelo

    // ── Ni una denegación: ni en el SDK ni en el log del broker ──
    assert!(
        denials(&lines).is_empty(),
        "bajo permisos exactos no debe haber denegaciones: {:?}",
        denials(&lines)
    );
    assert!(
        broker.violations().is_empty(),
        "el broker registró violaciones de permisos: {:?}",
        broker.violations()
    );

    // ── Lo que ocurrió con cada evento ──
    let llamadas = calls.lock().unwrap().clone();
    assert_eq!(llamadas.iter().filter(|(c, _)| c == "ok").count(), 1);
    assert_eq!(
        llamadas.iter().filter(|(c, _)| c == "permanente").count(),
        1,
        "un PERMANENT no se reintenta"
    );
    let intentos: Vec<u32> = llamadas
        .iter()
        .filter(|(c, _)| c == "reintento")
        .map(|(_, a)| *a)
        .collect();
    assert_eq!(
        intentos,
        vec![1, 2],
        "el RETRYABLE se reentrega UNA vez (nak)"
    );

    // ── Instantánea de lo que el SDK publicó, ANTES de que ops consulte nada ──
    let snapshot = seen.lock().unwrap().clone();

    // El inbox es SIEMPRE el privado: toda petición del SDK responde bajo `_INBOX.<servicio>.`
    let propias = |m: &Seen| {
        m.reply.starts_with("_INBOX.pedidos-api.") || m.reply.starts_with("_INBOX.facturacion-api.")
    };
    let publicados: Vec<&Seen> = snapshot
        .iter()
        .filter(|m| propias(m) || m.subject.starts_with("$JS.ACK."))
        .collect();
    assert!(
        snapshot
            .iter()
            .filter(|m| m.reply.starts_with("_INBOX."))
            .all(propias),
        "una petición del SDK usó un inbox que no es el privado del servicio: {snapshot:#?}"
    );

    // Suficiente: todo lo que el SDK publica está concedido…
    let todo: BTreeSet<String> = pedidos
        .clone()
        .and(facturacion.clone())
        .publish
        .into_iter()
        .collect();
    for m in &publicados {
        assert!(
            todo.iter().any(|p| nats_match(p, &m.subject)),
            "el SDK publicó en `{}` y ninguna regla lo concede",
            m.subject
        );
    }
    // …y necesario: cada permiso concedido se usa. Ni falta ni sobra.
    for p in &todo {
        assert!(
            publicados.iter().any(|m| nats_match(p, &m.subject)),
            "el permiso `{p}` no lo usó el SDK en todo el flujo: sobra, o falta ejercitarlo"
        );
    }
    // Y se vio un WIP-capable ack, un NAK y un TERM: los tres caminos de `$JS.ACK`.
    let acks: Vec<&str> = publicados
        .iter()
        .filter(|m| m.subject.starts_with("$JS.ACK."))
        .map(|m| m.payload.as_str())
        .collect();
    assert!(acks.contains(&""), "falta un ack (+ACK vacío): {acks:?}");
    assert!(
        acks.iter().any(|p| p.starts_with("-NAK")),
        "falta un nak: {acks:?}"
    );
    assert!(
        acks.iter().any(|p| p.starts_with("+TERM")),
        "falta un term: {acks:?}"
    );

    // ── Lo que dejó el flujo en el broker ──
    let (pendientes, suelo, _) = consumer_state(&ops, "facturacion-api", SUBJECT).await;
    assert_eq!(pendientes, 0, "todo quedó confirmado");
    assert_eq!(suelo, 3, "los tres eventos están resueltos");

    let dlq = dlq_events(&ops, "pedidos").await;
    assert_eq!(dlq.len(), 1, "solo el PERMANENT acaba en la DLQ");
    assert_eq!(dlq[0].dlqreason, Some(flux::DlqReason::Permanent));
    assert_eq!(dlq[0].dlqattempts, Some(1));

    sub.unsubscribe();
    productor.close().await.expect("close");
    consumidor.close().await.expect("close");
}

/// WIP: un handler que tarda más de `ack_wait / 2` emite `+WPI` por `$JS.ACK.…`, y está
/// concedido por el mismo permiso que el ack. Bajo permisos exactos, sin denegaciones.
///
/// Tarda ~17 s: es el tiempo real que el SDK espera antes del primer WIP (15 s).
#[tokio::test]
async fn a_wip_bajo_permisos_exactos() {
    let bin = broker_bin!();
    let pedidos = Perms::owner("pedidos-api", "pedidos");
    let facturacion = Perms::consumer("facturacion-api", SUBJECT);
    let broker = Broker::start(
        &bin,
        "",
        &accounts(&[
            pw_user("pedidos-api", &pedidos),
            pw_user("facturacion-api", &facturacion),
        ]),
    );
    let ops = ops(&broker).await;
    provision_streams(&ops, &["pedidos"]).await;
    let seen = sniff(&ops).await;

    let (log, lines) = capture_log();
    let productor = connect(opts(&broker, "pedidos-api"))
        .await
        .expect("conexión");
    let consumidor = connect(opts(&broker, "facturacion-api").with_logger(log))
        .await
        .expect("conexión");
    let calls: Calls = Arc::new(Mutex::new(Vec::new()));
    consumidor
        .subscribe(SUBJECT, handler(&calls))
        .await
        .expect("suscripción");

    productor
        .publish(
            SUBJECT,
            &json!({ "pedidoId": "ped-1", "caso": "lento", "aggregateVersion": 1 }),
        )
        .await
        .expect("publicación");

    // El ack llega al terminar el handler (16 s). Se espera a que el suelo de ack avance.
    let mut listo = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if consumer_state(&ops, "facturacion-api", SUBJECT).await.1 == 1 {
            listo = true;
            break;
        }
    }
    assert!(listo, "el evento lento no llegó a confirmarse");

    let wip = seen
        .lock()
        .unwrap()
        .iter()
        .any(|m| m.subject.starts_with("$JS.ACK.") && m.payload.starts_with("+WPI"));
    assert!(wip, "el SDK no emitió ningún WIP en un handler de 16 s");
    assert!(denials(&lines).is_empty(), "{:?}", denials(&lines));
    assert!(broker.violations().is_empty(), "{:?}", broker.violations());
    let (pendientes, _, redeliveries) = consumer_state(&ops, "facturacion-api", SUBJECT).await;
    assert_eq!(pendientes, 0);
    assert_eq!(
        redeliveries, 0,
        "el WIP evita la reentrega mientras el handler vive"
    );
}

/// Sin el permiso del ack, el consumo se queda colgado EN SILENCIO para el broker —el ack
/// se descarta y el mensaje seguirá pendiente—. El SDK, al menos, lo **dice** por el logger.
///
/// Es el contraste de (a): demuestra que «ninguna denegación» en (a) significa algo.
#[tokio::test]
async fn a_sin_permiso_de_ack_el_sdk_lo_dice_por_el_logger() {
    let bin = broker_bin!();
    let pedidos = Perms::owner("pedidos-api", "pedidos");
    let facturacion = Perms::consumer("facturacion-api", SUBJECT).without("$JS.ACK.");
    let broker = Broker::start(
        &bin,
        "",
        &accounts(&[
            pw_user("pedidos-api", &pedidos),
            pw_user("facturacion-api", &facturacion),
        ]),
    );
    let ops = ops(&broker).await;
    provision_streams(&ops, &["pedidos"]).await;

    let (log, lines) = capture_log();
    let productor = connect(opts(&broker, "pedidos-api"))
        .await
        .expect("conexión");
    let consumidor = connect(opts(&broker, "facturacion-api").with_logger(log))
        .await
        .expect("conexión");
    let calls: Calls = Arc::new(Mutex::new(Vec::new()));
    consumidor
        .subscribe(SUBJECT, handler(&calls))
        .await
        .expect("suscripción");
    productor
        .publish(
            SUBJECT,
            &json!({ "pedidoId": "ped-1", "caso": "ok", "aggregateVersion": 1 }),
        )
        .await
        .expect("publicación");

    assert!(wait_for(Duration::from_secs(5), || !calls.lock().unwrap().is_empty()).await);
    assert!(
        wait_for(Duration::from_secs(3), || denials(&lines)
            .iter()
            .any(|l| l.contains("$JS.ACK.")))
        .await,
        "el SDK debe registrar la denegación del ack: {:?}",
        lines.lock().unwrap()
    );
    let (pendientes, suelo, _) = consumer_state(&ops, "facturacion-api", SUBJECT).await;
    assert_eq!(
        (pendientes, suelo),
        (1, 0),
        "sin ack, el mensaje sigue pendiente"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
//  (b) La ACL ANTIGUA falla de forma clara, y rápido
// ═════════════════════════════════════════════════════════════════════════════

/// Con los permisos de `gen-acl.mjs` v1 el SDK **no cuelga**: falla al instante con un error
/// que nombra el subject denegado. Sin la detección sería un timeout de 5-30 s que no habla
/// de permisos (plan v1.1, hallazgo H1).
#[tokio::test]
async fn b_la_acl_antigua_falla_de_forma_clara_y_rapida() {
    let bin = broker_bin!();
    let broker = Broker::start(
        &bin,
        "",
        &accounts(&[pw_user("facturacion-api", &old_acl_facturacion())]),
    );
    let ops = ops(&broker).await;
    // Los streams EXISTEN: el fallo es de permisos, no de aprovisionamiento.
    provision_streams(&ops, &["pedidos", "facturacion"]).await;

    let (log, lines) = capture_log();
    let bus = connect(opts(&broker, "facturacion-api").with_logger(log))
        .await
        .expect("la autenticación funciona: lo que falla es la ACL");

    // — publicar —
    let t = Instant::now();
    let err = tokio::time::timeout(
        Duration::from_secs(4),
        bus.publish(
            "facturacion.factura.v1.emitida",
            &json!({ "facturaId": "f-1" }),
        ),
    )
    .await
    .expect("NO debe colgarse: la denegación tiene que cortar la espera")
    .unwrap_err();
    assert!(
        t.elapsed() < Duration::from_secs(3),
        "tardó {:?}",
        t.elapsed()
    );
    assert!(matches!(err, FluxError::PermissionDenied { .. }), "{err:?}");
    assert_eq!(err.code(), Some("PERMISSION_DENIED"));
    assert_eq!(err.class(), Some(ErrorClass::Permanent));
    let texto = err.to_string();
    assert!(
        texto.contains("$JS.API.STREAM.INFO.EVT_FACTURACION"),
        "{texto}"
    );
    assert!(texto.contains("06-security.md"), "{texto}");

    // — suscribirse —
    let t = Instant::now();
    let err = tokio::time::timeout(
        Duration::from_secs(4),
        bus.subscribe(SUBJECT, |_e: Event, _d| async { Ok(()) }),
    )
    .await
    .expect("NO debe colgarse")
    .unwrap_err();
    assert!(
        t.elapsed() < Duration::from_secs(3),
        "tardó {:?}",
        t.elapsed()
    );
    assert!(matches!(err, FluxError::PermissionDenied { .. }), "{err:?}");
    assert!(
        err.to_string().contains("$JS.API.STREAM.INFO.EVT_PEDIDOS"),
        "{err}"
    );

    // El log del SDK nombra lo denegado, con el vocabulario del servidor.
    assert!(
        denials(&lines)
            .iter()
            .any(|l| l.contains("Publish to \"$JS.API.STREAM.INFO.EVT_PEDIDOS\"")),
        "{:?}",
        lines.lock().unwrap()
    );
    // Y el servidor lo confirma: el fallo es una violación de permisos real.
    assert!(
        broker
            .violations()
            .iter()
            .any(|l| l
                .contains("Publish Violation - Subject \"$JS.API.STREAM.INFO.EVT_FACTURACION\"")),
        "{:?}",
        broker.violations()
    );
}

/// El contrario de (b): la **misma** operación con permisos exactos sí funciona. Sin esto,
/// «falla con la ACL antigua» no distinguiría una ACL mala de un SDK que siempre falla.
#[tokio::test]
async fn b_con_permisos_exactos_la_misma_operacion_funciona() {
    let bin = broker_bin!();
    let perms = Perms::owner("facturacion-api", "facturacion")
        .and(Perms::consumer("facturacion-api", SUBJECT));
    let broker = Broker::start(&bin, "", &accounts(&[pw_user("facturacion-api", &perms)]));
    let ops = ops(&broker).await;
    provision_streams(&ops, &["pedidos", "facturacion"]).await;

    let bus = connect(opts(&broker, "facturacion-api"))
        .await
        .expect("conexión");
    bus.publish(
        "facturacion.factura.v1.emitida",
        &json!({ "facturaId": "f-1" }),
    )
    .await
    .expect("publicar");
    let sub = bus
        .subscribe(SUBJECT, |_e: Event, _d| async { Ok(()) })
        .await
        .expect("suscribirse");
    sub.unsubscribe();
}

// ═════════════════════════════════════════════════════════════════════════════
//  (c) NOT_PROVISIONED
// ═════════════════════════════════════════════════════════════════════════════

fn assert_not_provisioned(err: &FluxError, kind: flux::ResourceKind, name: &str, t: Instant) {
    assert!(
        t.elapsed() < Duration::from_secs(3),
        "no debe ser un timeout: {:?}",
        t.elapsed()
    );
    let FluxError::NotProvisioned {
        kind: k, name: n, ..
    } = err
    else {
        panic!("se esperaba NotProvisioned, llegó {err:?}");
    };
    assert_eq!((*k, n.as_str()), (kind, name));
    assert_eq!(err.code(), Some("NOT_PROVISIONED"));
    assert_eq!(
        err.class(),
        Some(ErrorClass::Permanent),
        "PERMANENT: esperar no crea el recurso"
    );
    let texto = err.to_string();
    assert!(texto.contains(name), "debe nombrar el recurso: {texto}");
    assert!(
        texto.contains("flux provision"),
        "debe decir cómo se arregla: {texto}"
    );
    assert!(texto.contains("No es un timeout"), "{texto}");
}

/// Falta el stream y la ACL no concede crearlo → `NOT_PROVISIONED`, no un timeout.
#[tokio::test]
async fn c_stream_no_aprovisionado_y_sin_permiso_de_crear() {
    let bin = broker_bin!();
    let perms = Perms::owner("facturacion-api", "facturacion")
        .and(Perms::consumer("facturacion-api", SUBJECT));
    let broker = Broker::start(&bin, "", &accounts(&[pw_user("facturacion-api", &perms)]));
    // NADIE aprovisiona nada.
    let bus = connect(opts(&broker, "facturacion-api"))
        .await
        .expect("conexión");

    let t = Instant::now();
    let err = bus
        .publish(
            "facturacion.factura.v1.emitida",
            &json!({ "facturaId": "f-1" }),
        )
        .await
        .unwrap_err();
    assert_not_provisioned(&err, flux::ResourceKind::Stream, "EVT_FACTURACION", t);

    let t = Instant::now();
    let err = bus
        .subscribe(SUBJECT, |_e: Event, _d| async { Ok(()) })
        .await
        .unwrap_err();
    assert_not_provisioned(&err, flux::ResourceKind::Stream, "EVT_PEDIDOS", t);
}

/// Falta el consumidor (el stream sí existe) y la ACL no concede `CONSUMER.CREATE`.
#[tokio::test]
async fn c_consumidor_no_aprovisionado_y_sin_permiso_de_crear() {
    let bin = broker_bin!();
    let perms = Perms::consumer("facturacion-api", SUBJECT).without("$JS.API.CONSUMER.CREATE.");
    let broker = Broker::start(&bin, "", &accounts(&[pw_user("facturacion-api", &perms)]));
    let ops = ops(&broker).await;
    provision_streams(&ops, &["pedidos"]).await;

    let bus = connect(opts(&broker, "facturacion-api"))
        .await
        .expect("conexión");
    let t = Instant::now();
    let err = bus
        .subscribe(SUBJECT, |_e: Event, _d| async { Ok(()) })
        .await
        .unwrap_err();
    assert_not_provisioned(
        &err,
        flux::ResourceKind::Consumer,
        "facturacion-api__pedidos_pedido_v1_creado",
        t,
    );
}

/// Producción con `flux provision`: la infraestructura crea el consumidor y la ACL del
/// servicio **omite** `CONSUMER.CREATE`. El SDK usa el que existe y el flujo funciona, **sin
/// una sola denegación** —ni en su logger ni en el log del servidor—: pregunta antes de crear.
#[tokio::test]
async fn c_consumidor_aprovisionado_se_usa_sin_permiso_de_crear() {
    let bin = broker_bin!();
    let pedidos = Perms::owner("pedidos-api", "pedidos");
    let facturacion =
        Perms::consumer("facturacion-api", SUBJECT).without("$JS.API.CONSUMER.CREATE.");
    let broker = Broker::start(
        &bin,
        "",
        &accounts(&[
            pw_user("pedidos-api", &pedidos),
            pw_user("facturacion-api", &facturacion),
        ]),
    );
    let ops = ops(&broker).await;
    provision_streams(&ops, &["pedidos"]).await;
    provision_consumer(&ops, "facturacion-api", SUBJECT).await;

    let (log, lines) = capture_log();
    let productor = connect(opts(&broker, "pedidos-api"))
        .await
        .expect("conexión");
    let consumidor = connect(opts(&broker, "facturacion-api").with_logger(log))
        .await
        .expect("conexión");
    let calls: Calls = Arc::new(Mutex::new(Vec::new()));
    consumidor
        .subscribe(SUBJECT, handler(&calls))
        .await
        .expect("usa el consumidor aprovisionado");
    productor
        .publish(
            SUBJECT,
            &json!({ "pedidoId": "ped-1", "caso": "ok", "aggregateVersion": 1 }),
        )
        .await
        .expect("publicación");

    assert!(wait_for(Duration::from_secs(5), || !calls.lock().unwrap().is_empty()).await);
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Ninguna denegación. Antes se toleraba la del `CONSUMER.CREATE` que el SDK intentaba
    // ANTES de preguntar y que el broker dejaba en su log en cada arranque.
    assert!(denials(&lines).is_empty(), "{:?}", denials(&lines));
    assert!(
        broker.violations().is_empty(),
        "el broker registró violaciones: {:?}",
        broker.violations()
    );
    assert_eq!(consumer_state(&ops, "facturacion-api", SUBJECT).await.0, 0);
}

/// Los permisos que el SDK PUBLICÓ al suscribirse, de la consola del testigo.
fn publicados_por(seen: &Arc<Mutex<Vec<Seen>>>, servicio: &str) -> Vec<String> {
    let inbox = format!("_INBOX.{servicio}.");
    seen.lock()
        .unwrap()
        .iter()
        .filter(|m| m.reply.starts_with(&inbox))
        .map(|m| m.subject.clone())
        .collect()
}

/// «Info antes de crear» (06-security.md §3.4), en los dos sentidos y **sin depender de lo que
/// diga la ACL**: lo que importa es lo que el SDK publica.
///
/// 1. Consumidor aprovisionado, ACL sin CREATE: ninguna violación en el log del servidor.
/// 2. Consumidor aprovisionado y ACL CON permiso de crear: **no** crea —el permiso existe,
///    pero el consumidor también, y crear sobre uno existente no hace falta—.
/// 3. Consumidor sin aprovisionar y ACL con permiso de crear: pregunta (no existe) y LUEGO
///    lo crea, con la configuración canónica.
#[tokio::test]
async fn c_info_antes_de_crear_en_los_dos_sentidos() {
    let bin = broker_bin!();
    let durable = flux::durable_name("facturacion-api", SUBJECT).expect("durable");
    let info = format!("$JS.API.CONSUMER.INFO.EVT_PEDIDOS.{durable}");
    let create = format!("$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.{durable}.{SUBJECT}");

    // — 1. y 2. Aprovisionado: con y sin permiso de crear. —
    for (con_create, nombre) in [
        (false, "sin permiso de crear"),
        (true, "con permiso de crear"),
    ] {
        let perms = if con_create {
            Perms::consumer("facturacion-api", SUBJECT)
        } else {
            Perms::consumer("facturacion-api", SUBJECT).without("$JS.API.CONSUMER.CREATE.")
        };
        let broker = Broker::start(&bin, "", &accounts(&[pw_user("facturacion-api", &perms)]));
        let ops = ops(&broker).await;
        provision_streams(&ops, &["pedidos"]).await;
        provision_consumer(&ops, "facturacion-api", SUBJECT).await;
        let seen = sniff(&ops).await;

        let (log, lines) = capture_log();
        let bus = connect(opts(&broker, "facturacion-api").with_logger(log))
            .await
            .expect("conexión");
        let sub = bus
            .subscribe(SUBJECT, |_e: Event, _d| async { Ok(()) })
            .await
            .unwrap_or_else(|e| panic!("{nombre}: subscribe() sobre el aprovisionado: {e:?}"));
        tokio::time::sleep(Duration::from_millis(300)).await;
        sub.unsubscribe();

        let publicados = publicados_por(&seen, "facturacion-api");
        assert!(
            publicados.contains(&info),
            "{nombre}: debe preguntar por el consumidor: {publicados:?}"
        );
        assert!(
            !publicados
                .iter()
                .any(|s| s.starts_with("$JS.API.CONSUMER.CREATE.")),
            "{nombre}: el consumidor existe, no hay nada que crear: {publicados:?}"
        );
        assert!(
            broker.violations().is_empty(),
            "{nombre}: el broker registró violaciones: {:?}",
            broker.violations()
        );
        assert!(
            denials(&lines).is_empty(),
            "{nombre}: {:?}",
            denials(&lines)
        );
    }

    // — 3. Sin aprovisionar y con permiso de crear: pregunta y luego crea. —
    let perms = Perms::consumer("facturacion-api", SUBJECT);
    let broker = Broker::start(&bin, "", &accounts(&[pw_user("facturacion-api", &perms)]));
    let ops = ops(&broker).await;
    provision_streams(&ops, &["pedidos"]).await;
    let seen = sniff(&ops).await;
    let bus = connect(opts(&broker, "facturacion-api"))
        .await
        .expect("conexión");
    let sub = bus
        .subscribe(SUBJECT, |_e: Event, _d| async { Ok(()) })
        .await
        .expect("con permiso de crear y sin consumidor, lo crea");
    tokio::time::sleep(Duration::from_millis(300)).await;
    sub.unsubscribe();

    let publicados = publicados_por(&seen, "facturacion-api");
    let pos = |subject: &str| publicados.iter().position(|s| s == subject);
    let (i_info, i_create) = (pos(&info), pos(&create));
    assert!(
        i_info.is_some() && i_create.is_some() && i_info < i_create,
        "debe preguntar y LUEGO crear: {publicados:?}"
    );
    assert!(broker.violations().is_empty(), "{:?}", broker.violations());
    // El consumidor existe y lleva la configuración canónica (03-delivery.md §2).
    let stream = jetstream::new(ops.clone())
        .get_stream("EVT_PEDIDOS")
        .await
        .expect("stream");
    let creado = stream
        .consumer_info(&durable)
        .await
        .expect("lo creó el SDK");
    assert_eq!(creado.config.ack_wait, flux::DEFAULT_ACK_WAIT);
    assert_eq!(
        creado.config.max_deliver,
        i64::from(flux::DEFAULT_MAX_DELIVER)
    );
    assert_eq!(creado.config.backoff, flux::CANONICAL_BACKOFF.to_vec());
}

/// Si ni siquiera puede **consultar** el consumidor, no sabe si falta: es `PERMISSION_DENIED`
/// —nombrando el subject de la consulta—, no `NOT_PROVISIONED`. Aunque el consumidor exista y
/// aunque la ACL conceda crear: sin la consulta no hay forma de preguntar antes de crear.
#[tokio::test]
async fn c_sin_permiso_de_consultar_el_consumidor_es_permission_denied() {
    let bin = broker_bin!();
    let perms = Perms::consumer("facturacion-api", SUBJECT).without("$JS.API.CONSUMER.INFO.");
    let broker = Broker::start(&bin, "", &accounts(&[pw_user("facturacion-api", &perms)]));
    let ops = ops(&broker).await;
    provision_streams(&ops, &["pedidos"]).await;
    provision_consumer(&ops, "facturacion-api", SUBJECT).await;

    let bus = connect(opts(&broker, "facturacion-api"))
        .await
        .expect("conexión");
    let t = Instant::now();
    let err = tokio::time::timeout(
        Duration::from_secs(4),
        bus.subscribe(SUBJECT, |_e: Event, _d| async { Ok(()) }),
    )
    .await
    .expect("NO debe colgarse")
    .unwrap_err();
    assert!(t.elapsed() < Duration::from_secs(3), "{:?}", t.elapsed());
    assert!(matches!(err, FluxError::PermissionDenied { .. }), "{err:?}");
    assert_eq!(err.code(), Some("PERMISSION_DENIED"));
    let durable = flux::durable_name("facturacion-api", SUBJECT).expect("durable");
    assert!(
        err.to_string().contains(&format!(
            "Publish to \"$JS.API.CONSUMER.INFO.EVT_PEDIDOS.{durable}\""
        )),
        "{err}"
    );
}

/// El contrario: si la ACL SÍ concede crear (desarrollo, o una ACL amplia), nada falla ni
/// se queja y el stream y el consumidor aparecen. `NOT_PROVISIONED` nace de una denegación,
/// no de que el recurso falte.
#[tokio::test]
async fn c_con_permiso_de_crear_el_sdk_aprovisiona() {
    let bin = broker_bin!();
    let mut perms = Perms::owner("facturacion-api", "facturacion");
    perms
        .publish
        .push("$JS.API.STREAM.CREATE.EVT_FACTURACION".to_string());
    let broker = Broker::start(&bin, "", &accounts(&[pw_user("facturacion-api", &perms)]));

    let bus = connect(opts(&broker, "facturacion-api"))
        .await
        .expect("conexión");
    bus.publish(
        "facturacion.factura.v1.emitida",
        &json!({ "facturaId": "f-1" }),
    )
    .await
    .expect("con permiso de crear, el SDK crea el stream");

    let ops = ops(&broker).await;
    let mut s = jetstream::new(ops)
        .get_stream("EVT_FACTURACION")
        .await
        .expect("existe");
    assert_eq!(s.info().await.expect("info").state.messages, 1);
}

// ═════════════════════════════════════════════════════════════════════════════
//  (d) Sin concesión sobre un dominio: ni recibe sus eventos ni espía el inbox ajeno
// ═════════════════════════════════════════════════════════════════════════════

/// Resultado de un escenario con un espía: qué leyó y qué le denegó el broker.
struct Espia {
    leido: Vec<String>,
    violaciones: Vec<String>,
    handler_vio_el_evento: bool,
}

/// Un consumidor legítimo (`facturacion-api`) procesa un evento de `pedidos` mientras
/// `chismes-api`, con **su** concesión de suscripción `espia_sub`, intenta leerlo.
async fn escenario_espia(bin: &Path, espia_sub: &[&str]) -> Espia {
    let pedidos = Perms::owner("pedidos-api", "pedidos");
    let facturacion = Perms::consumer("facturacion-api", SUBJECT);
    let chismes = Perms {
        publish: vec!["chismes.>".into()],
        subscribe: espia_sub.iter().map(ToString::to_string).collect(),
    };
    let broker = Broker::start(
        bin,
        "",
        &accounts(&[
            pw_user("pedidos-api", &pedidos),
            pw_user("facturacion-api", &facturacion),
            pw_user("chismes-api", &chismes),
        ]),
    );
    let ops = ops(&broker).await;
    provision_streams(&ops, &["pedidos"]).await;

    let violaciones = Arc::new(Mutex::new(Vec::<String>::new()));
    let v2 = Arc::clone(&violaciones);
    let espia =
        async_nats::ConnectOptions::with_user_and_password("chismes-api".into(), "pw".into())
            .event_callback(move |ev| {
                let v2 = Arc::clone(&v2);
                async move {
                    if let async_nats::Event::ServerError(e) = ev {
                        v2.lock().unwrap().push(e.to_string());
                    }
                }
            })
            .connect(broker.url())
            .await
            .expect("conexión del espía");

    let leido = Arc::new(Mutex::new(Vec::<String>::new()));
    for pattern in ["_INBOX.>", "_INBOX.facturacion-api.>", "pedidos.>"] {
        let mut sub = espia.subscribe(pattern).await.expect("suscripción local");
        let sink = Arc::clone(&leido);
        tokio::spawn(async move {
            while let Some(m) = sub.next().await {
                sink.lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&m.payload).into_owned());
            }
        });
    }
    espia.flush().await.expect("flush");

    let productor = connect(opts(&broker, "pedidos-api"))
        .await
        .expect("conexión");
    let consumidor = connect(opts(&broker, "facturacion-api"))
        .await
        .expect("conexión");
    let calls: Calls = Arc::new(Mutex::new(Vec::new()));
    consumidor
        .subscribe(SUBJECT, handler(&calls))
        .await
        .expect("suscripción");
    productor
        .publish(
            SUBJECT,
            &json!({ "pedidoId": "SECRETO-42", "caso": "ok", "aggregateVersion": 1 }),
        )
        .await
        .expect("publicación");
    assert!(wait_for(Duration::from_secs(5), || !calls.lock().unwrap().is_empty()).await);

    // El espía también intenta pedir mensajes del consumidor ajeno (evidencia A-8).
    let durable = flux::durable_name("facturacion-api", SUBJECT).expect("durable");
    let _ = tokio::time::timeout(
        Duration::from_millis(400),
        espia.request(
            format!("$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.{durable}"),
            r#"{"batch":1}"#.into(),
        ),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(700)).await;

    let resultado = Espia {
        leido: leido.lock().unwrap().clone(),
        violaciones: violaciones.lock().unwrap().clone(),
        handler_vio_el_evento: !calls.lock().unwrap().is_empty(),
    };
    productor.close().await.ok();
    consumidor.close().await.ok();
    resultado
}

/// Con el inbox privado y la ACL exacta, el servicio sin concesión no lee nada: ni el
/// dominio, ni el inbox ajeno, ni los mensajes del consumidor ajeno.
#[tokio::test]
async fn d_el_espia_confinado_no_lee_nada() {
    let bin = broker_bin!();
    let r = escenario_espia(&bin, &["_INBOX.chismes-api.>"]).await;

    assert!(
        r.handler_vio_el_evento,
        "el consumidor legítimo SÍ recibió el evento"
    );
    assert!(
        !r.leido.iter().any(|m| m.contains("SECRETO-42")),
        "el espía leyó un evento sin concesión: {:?}",
        r.leido
    );
    for denegado in [
        "Subscription to \"_INBOX.>\"",
        "Subscription to \"_INBOX.facturacion-api.>\"",
        "Subscription to \"pedidos.>\"",
        "Publish to \"$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado\"",
    ] {
        assert!(
            r.violaciones.iter().any(|v| v.contains(denegado)),
            "el broker debía denegar {denegado}: {:?}",
            r.violaciones
        );
    }
}

/// El contrario, y la razón de ser de todo: con la concesión **antigua** `_INBOX.>` el mismo
/// espía **sí** lee el evento completo (plan v1.1, evidencia A-2 / hallazgo H2). Sin este
/// test, el anterior podría pasar por una razón equivocada.
#[tokio::test]
async fn d_con_el_inbox_comun_antiguo_el_espia_lee_el_evento() {
    let bin = broker_bin!();
    let r = escenario_espia(&bin, &["_INBOX.>"]).await;

    assert!(
        r.leido.iter().any(|m| m.contains("SECRETO-42")),
        "con `_INBOX.>` el espía debía poder leer el evento (la fuga H2): {:?} / {:?}",
        r.leido,
        r.violaciones
    );
    // Lo que sigue denegado es la suscripción directa al dominio, que la v1 sí confinaba.
    assert!(r
        .violaciones
        .iter()
        .any(|v| v.contains("Subscription to \"pedidos.>\"")));
}

/// Un servicio sin concesión sobre un dominio tampoco puede consumirlo ni publicar en él.
#[tokio::test]
async fn d_el_sdk_sin_concesion_no_puede_suscribirse_ni_publicar() {
    let bin = broker_bin!();
    // `chismes-api` no tiene NADA sobre `pedidos`. `contable-api` puede consultar el stream
    // (es consumidor de otro subject de `pedidos`) pero no es su dueño: no puede publicar.
    let chismes = Perms::owner("chismes-api", "chismes");
    let contable = Perms::consumer("contable-api", "pedidos.pedido.v1.cancelado");
    let broker = Broker::start(
        &bin,
        "",
        &accounts(&[
            pw_user("chismes-api", &chismes),
            pw_user("contable-api", &contable),
        ]),
    );
    let ops = ops(&broker).await;
    provision_streams(&ops, &["pedidos", "chismes"]).await;

    let bus = connect(opts(&broker, "chismes-api"))
        .await
        .expect("conexión");
    let err = bus
        .subscribe(SUBJECT, |_e: Event, _d| async { Ok(()) })
        .await
        .unwrap_err();
    assert!(matches!(err, FluxError::PermissionDenied { .. }), "{err:?}");
    assert!(
        err.to_string().contains("$JS.API.STREAM.INFO.EVT_PEDIDOS"),
        "{err}"
    );
    let err = bus
        .publish(SUBJECT, &json!({ "pedidoId": "x", "aggregateVersion": 1 }))
        .await
        .unwrap_err();
    assert!(matches!(err, FluxError::PermissionDenied { .. }), "{err:?}");
    // Lo suyo, en cambio, funciona.
    bus.publish("chismes.chisme.v1.contado", &json!({ "chismeId": "c-1" }))
        .await
        .expect("el dueño de su dominio publica");

    // Con acceso de lectura al stream, publicar sigue denegado: la regla de ownership.
    let bus = connect(opts(&broker, "contable-api"))
        .await
        .expect("conexión");
    let err = bus
        .publish(SUBJECT, &json!({ "pedidoId": "x", "aggregateVersion": 1 }))
        .await
        .unwrap_err();
    assert!(matches!(err, FluxError::PermissionDenied { .. }), "{err:?}");
    assert!(
        err.to_string()
            .contains("Publish to \"pedidos.pedido.v1.creado\""),
        "{err}"
    );
    assert!(
        err.to_string()
            .contains("solo el servicio dueño del dominio"),
        "{err}"
    );
    // Y nada llegó al stream.
    let mut s = jetstream::new(ops)
        .get_stream("EVT_PEDIDOS")
        .await
        .expect("stream");
    assert_eq!(s.info().await.expect("info").state.messages, 0);
}

// ═════════════════════════════════════════════════════════════════════════════
//  (e) Aislamiento estricto de tenant, bajo ACL
// ═════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn e_el_aislamiento_estricto_de_tenant_funciona_bajo_acl() {
    let bin = broker_bin!();
    let pedidos = Perms::owner("pedidos-api", "pedidos");
    let facturacion = Perms::consumer("facturacion-api", SUBJECT);
    let broker = Broker::start(
        &bin,
        "",
        &accounts(&[
            pw_user("pedidos-api", &pedidos),
            pw_user("facturacion-api", &facturacion),
        ]),
    );
    let ops = ops(&broker).await;
    provision_streams(&ops, &["pedidos"]).await;

    let productor = connect(opts(&broker, "pedidos-api"))
        .await
        .expect("conexión");

    // Sin tenant, `strict` es un error de CONFIGURACIÓN: falla antes de tocar el broker y no
    // deja un consumidor huérfano.
    let sin_tenant = connect(
        ConnectOptions::new(broker.url(), "facturacion-api", "test", "1.0.0")
            .with_credentials(Credentials::UserPassword {
                user: "facturacion-api".into(),
                password: "pw".into(),
            })
            .with_tenant_isolation(TenantIsolation::Strict),
    )
    .await
    .expect("conexión");
    let err = sin_tenant
        .subscribe(SUBJECT, |_e: Event, _d| async { Ok(()) })
        .await
        .unwrap_err();
    assert!(matches!(err, FluxError::TenantIsolation { .. }), "{err:?}");
    assert!(
        jetstream::new(ops.clone())
            .get_stream("EVT_PEDIDOS")
            .await
            .expect("stream")
            .consumer_info("facturacion-api__pedidos_pedido_v1_creado")
            .await
            .is_err(),
        "strict sin tenant no debe haber creado ningún consumidor"
    );

    // Con tenant: solo llegan los eventos de `acme`; los de `globex` se confirman y se
    // descartan ANTES del handler. Que el descarte confirme exige el permiso del ack.
    let (log, lines) = capture_log();
    let estricto = connect(
        opts(&broker, "facturacion-api")
            .with_logger(log)
            .with_tenant_isolation(TenantIsolation::Strict),
    )
    .await
    .expect("conexión");
    let calls: Calls = Arc::new(Mutex::new(Vec::new()));
    let vistos = Arc::new(Mutex::new(Vec::<String>::new()));
    let (v2, c2) = (Arc::clone(&vistos), Arc::clone(&calls));
    estricto
        .subscribe_with(
            SUBJECT,
            move |ev: Event, d: Delivery| {
                let (v2, c2) = (Arc::clone(&v2), Arc::clone(&c2));
                async move {
                    v2.lock().unwrap().push(ev.tenantid.clone());
                    c2.lock().unwrap().push(("ok".into(), d.attempt));
                    Ok(())
                }
            },
            SubscribeOptions::default().with_tenant_filter("acme"),
        )
        .await
        .expect("suscripción estricta con tenant");

    for tenant in ["globex", "acme", "globex"] {
        productor
            .publish_with(
                SUBJECT,
                &json!({ "pedidoId": "p", "aggregateVersion": 1 }),
                PublishOptions::default().with_tenant_id(tenant),
            )
            .await
            .expect("publicación");
    }
    assert!(
        wait_for(Duration::from_secs(5), || !vistos
            .lock()
            .unwrap()
            .is_empty())
        .await
    );
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert_eq!(
        *vistos.lock().unwrap(),
        vec!["acme".to_string()],
        "solo el tenant propio llega al handler"
    );
    let (pendientes, suelo, _) = consumer_state(&ops, "facturacion-api", SUBJECT).await;
    assert_eq!(
        (pendientes, suelo),
        (0, 3),
        "los eventos de otro tenant se CONFIRMAN al descartarlos, bajo la ACL exacta"
    );
    assert!(denials(&lines).is_empty(), "{:?}", denials(&lines));
}

// ═════════════════════════════════════════════════════════════════════════════
//  (f) Autenticación: nkey y usuario/contraseña
// ═════════════════════════════════════════════════════════════════════════════

/// Un `connect()` que debe fallar y debe hacerlo YA, no colgarse reintentando.
async fn connect_debe_fallar(o: ConnectOptions) -> FluxError {
    let t = Instant::now();
    let r = tokio::time::timeout(Duration::from_secs(6), connect(o))
        .await
        .expect("connect() no debe colgarse reintentando una credencial que no se arregla");
    assert!(
        t.elapsed() < Duration::from_secs(5),
        "tardó {:?}",
        t.elapsed()
    );
    r.expect_err("connect() debía fallar")
}

/// El error lleva el código estable `codigo` (04-errors.md §5.3) y es PERMANENT.
fn assert_code(err: &FluxError, codigo: &str) {
    assert_eq!(err.code(), Some(codigo), "{err:?}");
    assert_eq!(err.class(), Some(ErrorClass::Permanent), "{err:?}");
}

/// El broker contestó y rechazó la conexión: `CONNECTION_REJECTED` con ese motivo. Y no es
/// ninguno de los otros dos códigos de `connect()` —que son opciones mal formadas, detectadas
/// sin red—.
fn assert_rejected(err: &FluxError, reason: RejectionReason) {
    match err {
        FluxError::ConnectionRejected { reason: r, .. } => assert_eq!(*r, reason, "{err:?}"),
        otro => panic!("debía ser ConnectionRejected({reason}), es {otro:?}"),
    }
    assert_code(err, "CONNECTION_REJECTED");
}

#[tokio::test]
async fn f_usuario_y_contrasena() {
    let bin = broker_bin!();
    let perms = Perms::owner("pedidos-api", "pedidos");
    let broker = Broker::start(&bin, "", &accounts(&[pw_user("pedidos-api", &perms)]));
    provision_streams(&ops(&broker).await, &["pedidos"]).await;

    // Lo correcto.
    let bus = connect(opts(&broker, "pedidos-api"))
        .await
        .expect("credencial correcta");
    bus.publish(
        "pedidos.pedido.v1.creado",
        &json!({ "pedidoId": "p", "aggregateVersion": 1 }),
    )
    .await
    .expect("publicar autenticado");

    // Lo incorrecto: contraseña equivocada, y usuario inexistente.
    let err = connect_debe_fallar(opts(&broker, "pedidos-api").with_credentials(
        Credentials::UserPassword {
            user: "pedidos-api".into(),
            password: "equivocada".into(),
        },
    ))
    .await;
    assert_rejected(&err, RejectionReason::Auth);
    assert!(err.to_string().contains("rechazó la credencial"), "{err}");
    assert!(
        !err.to_string().contains("equivocada"),
        "el mensaje no debe volcar la contraseña"
    );
    let err = connect_debe_fallar(opts(&broker, "pedidos-api").with_credentials(
        Credentials::UserPassword {
            user: "nadie".into(),
            password: "pw".into(),
        },
    ))
    .await;
    assert_rejected(&err, RejectionReason::Auth);
    assert!(err.to_string().contains("rechazó la credencial"), "{err}");

    // Sin credencial en un broker que la exige.
    let err =
        connect_debe_fallar(opts(&broker, "pedidos-api").with_credentials(Credentials::None)).await;
    assert_rejected(&err, RejectionReason::Auth);
    assert!(err.to_string().contains("rechazó la credencial"), "{err}");
}

#[tokio::test]
async fn f_nkey() {
    let bin = broker_bin!();
    let usuario = nkeys::KeyPair::new_user();
    let seed = usuario.seed().expect("semilla");
    let perms = Perms::owner("pedidos-api", "pedidos");
    let broker = Broker::start(
        &bin,
        "",
        &accounts(&[user_block(
            &format!("nkey: \"{}\"", usuario.public_key()),
            &perms,
        )]),
    );
    provision_streams(&ops(&broker).await, &["pedidos"]).await;

    let o = |c: Credentials| {
        ConnectOptions::new(broker.url(), "pedidos-api", "test", "1.0.0")
            .with_credentials(c)
            .with_tenant_id("acme")
            .with_schema_base_url("https://schemas.internal")
    };

    // Lo correcto, desde una semilla en memoria…
    let bus = connect(o(Credentials::NkeySeed(seed.clone())))
        .await
        .expect("nkey correcta");
    bus.publish(
        "pedidos.pedido.v1.creado",
        &json!({ "pedidoId": "p", "aggregateVersion": 1 }),
    )
    .await
    .expect("publicar con nkey");

    // …y desde un fichero `.nk` (con salto de línea final, como lo dejan los gestores).
    let dir = Tmp::new("nk");
    let nk = dir.join("servicio.nk");
    std::fs::write(&nk, format!("{seed}\n")).expect("nk");
    connect(o(Credentials::NkeyFile(nk.clone())))
        .await
        .expect("nkey desde fichero");

    // Lo incorrecto 1: una semilla bien formada pero de una clave que el broker no conoce.
    let otra = nkeys::KeyPair::new_user().seed().expect("semilla");
    let err = connect_debe_fallar(o(Credentials::NkeySeed(otra.clone()))).await;
    assert_rejected(&err, RejectionReason::Auth);
    assert!(err.to_string().contains("rechazó la credencial"), "{err}");
    assert!(
        !err.to_string().contains(&otra),
        "el mensaje no debe volcar la semilla"
    );

    // Lo incorrecto 2: mal formada. Falla en alto SIN tocar la red (ni siquiera hay broker
    // en esta URL) y sin volcar el valor.
    let sin_broker = |c: Credentials| {
        ConnectOptions::new("nats://127.0.0.1:1", "pedidos-api", "test", "1.0.0")
            .with_credentials(c)
    };
    for malo in [
        "",
        "SUAQUEESTAMAL",
        &nkeys::KeyPair::new_account().seed().expect("seed"),
    ] {
        let err = tokio::time::timeout(
            Duration::from_secs(2),
            connect(sin_broker(Credentials::NkeySeed(malo.to_string()))),
        )
        .await
        .expect("debe fallar antes de intentar conectar")
        .unwrap_err();
        assert!(matches!(err, FluxError::InvalidCredentials(_)), "{err:?}");
        assert_code(&err, "INVALID_CREDENTIALS");
        if !malo.is_empty() {
            assert!(
                !err.to_string().contains(malo),
                "no debe volcar el valor: {err}"
            );
        }
    }
    let err = connect(sin_broker(Credentials::NkeyFile("/no/existe.nk".into())))
        .await
        .unwrap_err();
    assert!(matches!(err, FluxError::InvalidCredentials(_)), "{err:?}");
    assert_code(&err, "INVALID_CREDENTIALS");
}

#[tokio::test]
async fn f_token() {
    let bin = broker_bin!();
    let broker = Broker::start(&bin, "authorization { token: \"s3cr3to\" }", "");
    let url = broker.url();
    let o = |c: Credentials| {
        ConnectOptions::new(&url, "pedidos-api", "test", "1.0.0")
            .with_credentials(c)
            .with_tenant_id("acme")
            .with_schema_base_url("https://schemas.internal")
    };
    connect(o(Credentials::Token("s3cr3to".into())))
        .await
        .expect("token correcto");
    let err = connect_debe_fallar(o(Credentials::Token("equivocado".into()))).await;
    assert_rejected(&err, RejectionReason::Auth);
    assert!(err.to_string().contains("rechazó la credencial"), "{err}");
}

/// Un `.creds` ilegible, o sin las dos secciones, falla en alto antes de tocar la red.
#[tokio::test]
async fn f_un_creds_mal_formado_falla_en_alto() {
    let dir = Tmp::new("creds");
    let cursed = |nombre: &str, contenido: &str| {
        let p = dir.join(nombre);
        std::fs::write(&p, contenido).expect("creds");
        p
    };
    let sin_broker = |p: PathBuf| {
        ConnectOptions::new("nats://127.0.0.1:1", "pedidos-api", "test", "1.0.0")
            .with_credentials(Credentials::CredsFile(p))
    };
    for p in [
        PathBuf::from("/no/existe.creds"),
        cursed("vacio.creds", ""),
        cursed("basura.creds", "esto no es un .creds"),
        cursed(
            "sin-semilla.creds",
            "-----BEGIN NATS USER JWT-----\neyJhbGciOiJlZDI1NTE5LW5rZXkifQ.e30.AAAA\n------END NATS USER JWT------\n",
        ),
    ] {
        let err = tokio::time::timeout(Duration::from_secs(2), connect(sin_broker(p.clone())))
            .await
            .expect("debe fallar antes de intentar conectar")
            .unwrap_err();
        assert!(
            matches!(err, FluxError::InvalidCredentials(_)),
            "{}: {err:?}",
            p.display()
        );
        assert_code(&err, "INVALID_CREDENTIALS");
        assert!(err.to_string().contains(".creds"), "{err}");
    }
}

// ─── Modo operador / JWT: el `.creds` real ───────────────────────────────────

fn b64url(data: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for c in data.chunks(3) {
        let b = [c[0], *c.get(1).unwrap_or(&0), *c.get(2).unwrap_or(&0)];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(A[(n >> 18) as usize & 63] as char);
        out.push(A[(n >> 12) as usize & 63] as char);
        if c.len() > 1 {
            out.push(A[(n >> 6) as usize & 63] as char);
        }
        if c.len() > 2 {
            out.push(A[n as usize & 63] as char);
        }
    }
    out
}

/// Un JWT de NATS (`alg: ed25519-nkey`) firmado por `issuer`. Se construye a mano para no
/// depender de `nsc` en CI: el servidor valida la firma y la cadena operador → cuenta →
/// usuario, que es lo que hay que ejercitar.
fn jwt(issuer: &nkeys::KeyPair, claims: &serde_json::Value) -> String {
    let h = b64url(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
    let c = b64url(claims.to_string().as_bytes());
    let sig = issuer.sign(format!("{h}.{c}").as_bytes()).expect("firma");
    format!("{h}.{c}.{}", b64url(&sig))
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("reloj")
        .as_secs()
}

/// El texto de un `.creds` (JWT de usuario + semilla), con los permisos DENTRO del JWT.
/// `firma` es la clave de la cuenta que emite el JWT; `jwt_de` y `semilla_de`, de quién es
/// la identidad y de quién la semilla — distintas, para fabricar una credencial inválida.
fn creds_text(
    cuenta: &nkeys::KeyPair,
    firma: &nkeys::KeyPair,
    jwt_de: &nkeys::KeyPair,
    semilla_de: &nkeys::KeyPair,
    perms: Option<&Perms>,
) -> String {
    let (p, s) = perms.map_or((json!({}), json!({})), |p| {
        (
            json!({ "allow": p.publish }),
            json!({ "allow": p.subscribe }),
        )
    });
    let token = jwt(
        firma,
        &json!({
            "jti": "x", "iat": now_secs(), "iss": firma.public_key(), "name": "u",
            "sub": jwt_de.public_key(),
            "nats": { "pub": p, "sub": s, "subs": -1, "data": -1, "payload": -1,
                      "type": "user", "version": 2, "issuer_account": cuenta.public_key() }
        }),
    );
    format!(
        "-----BEGIN NATS USER JWT-----\n{token}\n------END NATS USER JWT------\n\n\
         -----BEGIN USER NKEY SEED-----\n{}\n------END USER NKEY SEED------\n",
        semilla_de.seed().expect("semilla")
    )
}

/// **`.creds` contra un broker en modo operador/JWT**: la cadena completa —operador, cuenta
/// con JetStream, usuario con los permisos exactos dentro de su JWT— y el SDK publicando con
/// el inbox privado. Más lo incorrecto: una semilla que no es la del JWT, y un JWT emitido
/// por una cuenta que el operador no conoce.
#[tokio::test]
async fn f_creds_contra_un_broker_en_modo_operador_jwt() {
    let bin = broker_bin!();
    let op = nkeys::KeyPair::new_operator();
    let cuenta = nkeys::KeyPair::new_account();
    let sistema = nkeys::KeyPair::new_account();
    let ajena = nkeys::KeyPair::new_account(); // el operador NO la conoce
    let limites = json!({ "subs": -1, "data": -1, "payload": -1, "imports": -1, "exports": -1,
        "wildcards": true, "conn": -1, "leaf": -1, "mem_storage": -1, "disk_storage": -1,
        "streams": -1, "consumer": -1 });
    // La cuenta de sistema NO puede llevar JetStream: el servidor se niega a arrancar.
    let sin_js = json!({ "subs": -1, "data": -1, "payload": -1, "imports": -1, "exports": -1,
        "wildcards": true, "conn": -1, "leaf": -1 });
    let firmar_cuenta = |k: &nkeys::KeyPair, nombre: &str, limits: &serde_json::Value| {
        jwt(
            &op,
            &json!({ "jti": "x", "iat": now_secs(), "iss": op.public_key(), "name": nombre,
                     "sub": k.public_key(),
                     "nats": { "limits": limits, "type": "account", "version": 2 } }),
        )
    };
    let op_jwt = jwt(
        &op,
        &json!({ "jti": "x", "iat": now_secs(), "iss": op.public_key(), "name": "op",
                 "sub": op.public_key(), "nats": { "type": "operator", "version": 2 } }),
    );
    let top = format!(
        "operator: {op_jwt}\nsystem_account: {}\nresolver: MEMORY\n\
         resolver_preload: {{ {}: {}, {}: {} }}",
        sistema.public_key(),
        cuenta.public_key(),
        firmar_cuenta(&cuenta, "PROD", &limites),
        sistema.public_key(),
        firmar_cuenta(&sistema, "SYS", &sin_js),
    );
    let broker = Broker::start(&bin, &top, "");

    let dir = Tmp::new("opjwt");
    let escribir = |nombre: &str, texto: String| {
        let p = dir.join(nombre);
        std::fs::write(&p, texto).expect("creds");
        p
    };

    // `ops` sin restricciones, para aprovisionar.
    let u_ops = nkeys::KeyPair::new_user();
    let ops = async_nats::ConnectOptions::with_credentials(&creds_text(
        &cuenta, &cuenta, &u_ops, &u_ops, None,
    ))
    .expect("creds de ops")
    .connect(broker.url())
    .await
    .expect("ops en modo operador");
    provision_streams(&ops, &["pedidos"]).await;

    // El servicio, con los permisos exactos en su JWT.
    let perms = Perms::owner("pedidos-api", "pedidos");
    let u = nkeys::KeyPair::new_user();
    let buenas = escribir(
        "pedidos-api.creds",
        creds_text(&cuenta, &cuenta, &u, &u, Some(&perms)),
    );
    let (log, lines) = capture_log();
    let bus = connect(
        ConnectOptions::new(broker.url(), "pedidos-api", "test", "1.0.0")
            .with_credentials(Credentials::CredsFile(buenas))
            .with_tenant_id("acme")
            .with_schema_base_url("https://schemas.internal")
            .with_logger(log),
    )
    .await
    .expect("el `.creds` válido autentica contra el operador");
    bus.publish(SUBJECT, &json!({ "pedidoId": "p", "aggregateVersion": 1 }))
        .await
        .expect("publicar con la identidad del `.creds`, bajo los permisos de su JWT");
    assert!(denials(&lines).is_empty(), "{:?}", denials(&lines));
    // Y los permisos del JWT confinan igual que los de la configuración: otro dominio, no.
    let err = bus
        .publish(
            "facturacion.factura.v1.emitida",
            &json!({ "facturaId": "f" }),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, FluxError::PermissionDenied { .. }), "{err:?}");

    // Lo incorrecto 1: el JWT es del usuario A pero la semilla es de B → la firma del
    // nonce no encaja con la identidad del JWT.
    let otro = nkeys::KeyPair::new_user();
    let mezclada = escribir(
        "mezclada.creds",
        creds_text(&cuenta, &cuenta, &u, &otro, Some(&perms)),
    );
    let o = |p: PathBuf| {
        ConnectOptions::new(broker.url(), "pedidos-api", "test", "1.0.0")
            .with_credentials(Credentials::CredsFile(p))
    };
    let err = connect_debe_fallar(o(mezclada)).await;
    assert_rejected(&err, RejectionReason::Auth);
    assert!(err.to_string().contains("rechazó la credencial"), "{err}");

    // Lo incorrecto 2: un JWT emitido por una cuenta que el operador no conoce.
    let ajeno = escribir(
        "ajena.creds",
        creds_text(&ajena, &ajena, &u, &u, Some(&perms)),
    );
    let err = connect_debe_fallar(o(ajeno)).await;
    assert_rejected(&err, RejectionReason::Auth);
    assert!(err.to_string().contains("rechazó la credencial"), "{err}");
}

// ═════════════════════════════════════════════════════════════════════════════
//  TLS
// ═════════════════════════════════════════════════════════════════════════════

/// Una CA propia y los certificados de servidor y de cliente que firma, en ficheros.
struct Pki {
    dir: PathBuf,
}

impl Pki {
    fn new() -> Self {
        use rcgen::{
            BasicConstraints, CertificateParams, DistinguishedName, DnType,
            ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType,
        };
        let dir = std::env::temp_dir().join(format!(
            "flux-pki-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).expect("dir");

        let ca_key = KeyPair::generate().expect("clave CA");
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("params CA");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.distinguished_name = DistinguishedName::new();
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "flux-test-ca");
        let ca = ca_params.self_signed(&ca_key).expect("CA");

        let firmar = |nombre: &str, sans: Vec<SanType>, eku: ExtendedKeyUsagePurpose| {
            let key = KeyPair::generate().expect("clave");
            let mut p = CertificateParams::new(Vec::<String>::new()).expect("params");
            p.subject_alt_names = sans;
            p.extended_key_usages = vec![eku];
            p.distinguished_name = DistinguishedName::new();
            p.distinguished_name.push(DnType::CommonName, nombre);
            let cert = p.signed_by(&key, &ca, &ca_key).expect("firma");
            std::fs::write(dir.join(format!("{nombre}.pem")), cert.pem()).expect("cert");
            std::fs::write(dir.join(format!("{nombre}.key")), key.serialize_pem()).expect("key");
        };
        firmar(
            "servidor",
            vec![
                SanType::IpAddress("127.0.0.1".parse().expect("ip")),
                SanType::DnsName("localhost".try_into().expect("dns")),
            ],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        firmar("cliente", vec![], ExtendedKeyUsagePurpose::ClientAuth);
        // Firmados por la MISMA CA, pero que no valen para este broker: uno de otro host y otro
        // caducado. La CA que los firmó es de fiar, así que el fallo es del certificado, no de
        // la cadena: es lo que distingue «host fuera de los SAN» y «caducado» de «CA ausente».
        firmar(
            "servidor-otro-host",
            vec![SanType::DnsName(
                "otro-host.example".try_into().expect("dns"),
            )],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        {
            let key = KeyPair::generate().expect("clave");
            let mut p = CertificateParams::new(Vec::<String>::new()).expect("params");
            p.subject_alt_names = vec![SanType::IpAddress("127.0.0.1".parse().expect("ip"))];
            p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
            p.not_before = rcgen::date_time_ymd(2020, 1, 1);
            p.not_after = rcgen::date_time_ymd(2021, 1, 1);
            p.distinguished_name = DistinguishedName::new();
            p.distinguished_name
                .push(DnType::CommonName, "servidor-caducado");
            let cert = p.signed_by(&key, &ca, &ca_key).expect("firma");
            std::fs::write(dir.join("servidor-caducado.pem"), cert.pem()).expect("cert");
            std::fs::write(dir.join("servidor-caducado.key"), key.serialize_pem()).expect("key");
        }
        std::fs::write(dir.join("ca.pem"), ca.pem()).expect("ca");

        // Una SEGUNDA CA, ajena: para comprobar que una CA que no firmó el certificado del
        // broker se rechaza.
        let otra_key = KeyPair::generate().expect("clave");
        let mut otra_params = CertificateParams::new(Vec::<String>::new()).expect("params");
        otra_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        otra_params.distinguished_name = DistinguishedName::new();
        otra_params
            .distinguished_name
            .push(DnType::CommonName, "otra-ca");
        std::fs::write(
            dir.join("otra-ca.pem"),
            otra_params.self_signed(&otra_key).expect("otra").pem(),
        )
        .expect("otra-ca");
        Self { dir }
    }

    fn f(&self, nombre: &str) -> PathBuf {
        self.dir.join(nombre)
    }

    /// Configuración TLS del servidor; `verify` exige certificado de cliente (mTLS).
    fn server_conf(&self, verify: bool) -> String {
        self.server_conf_con("servidor", verify)
    }

    /// Como [`Self::server_conf`], con el certificado de servidor `cert` (`servidor`,
    /// `servidor-otro-host`, `servidor-caducado`).
    fn server_conf_con(&self, cert: &str, verify: bool) -> String {
        format!(
            "tls {{ cert_file: \"{}\", key_file: \"{}\", ca_file: \"{}\", verify: {verify}, timeout: 2 }}",
            self.f(&format!("{cert}.pem")).display(),
            self.f(&format!("{cert}.key")).display(),
            self.f("ca.pem").display()
        )
    }
}

impl Drop for Pki {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn tls_opts(url: &str) -> ConnectOptions {
    ConnectOptions::new(url, "pedidos-api", "test", "1.0.0")
        .with_credentials(Credentials::UserPassword {
            user: "pedidos-api".into(),
            password: "pw".into(),
        })
        .with_tenant_id("acme")
        .with_schema_base_url("https://schemas.internal")
}

#[tokio::test]
async fn tls_con_ca_propia() {
    let bin = broker_bin!();
    let pki = Pki::new();
    let perms = Perms::owner("pedidos-api", "pedidos");
    let broker = Broker::start(
        &bin,
        &pki.server_conf(false),
        &accounts(&[pw_user("pedidos-api", &perms)]),
    );
    // El provisionamiento de ops usa TLS también.
    let ops_tls = async_nats::ConnectOptions::with_user_and_password("ops".into(), "pw".into())
        .require_tls(true)
        .add_root_certificates(pki.f("ca.pem"))
        .connect(broker.url())
        .await
        .expect("ops con TLS");
    provision_streams(&ops_tls, &["pedidos"]).await;

    // Lo correcto: CA propia + TLS exigido, con publicación de extremo a extremo.
    let bus =
        connect(tls_opts(&broker.url()).with_tls(TlsOptions::new().with_ca_file(pki.f("ca.pem"))))
            .await
            .expect("con la CA que firma el certificado del broker");
    bus.publish(
        "pedidos.pedido.v1.creado",
        &json!({ "pedidoId": "p", "aggregateVersion": 1 }),
    )
    .await
    .expect("publicar por TLS");

    // Lo incorrecto 1: sin CA propia, el certificado del broker no es de fiar.
    let err = connect_debe_fallar(tls_opts(&broker.url()).with_tls(TlsOptions::new())).await;
    assert_rejected(&err, RejectionReason::Tls);
    assert!(err.to_string().contains("TLS"), "{err}");
    // La causa de verdad viaja en el mensaje: el certificado de un emisor desconocido.
    assert!(err.to_string().contains("UnknownIssuer"), "{err}");

    // Lo incorrecto 2: una CA que no firmó ese certificado.
    let err = connect_debe_fallar(
        tls_opts(&broker.url()).with_tls(TlsOptions::new().with_ca_file(pki.f("otra-ca.pem"))),
    )
    .await;
    assert_rejected(&err, RejectionReason::Tls);
    assert!(err.to_string().contains("TLS"), "{err}");

    // Lo incorrecto 3: la CA no existe / no es un PEM → INVALID_TLS_OPTIONS, sin red. Es una
    // opción mal formada, no un rechazo del broker: son dos códigos y dos remedios.
    for malo in [pki.f("no-existe.pem"), pki.f("servidor.key")] {
        let err =
            connect(tls_opts("nats://127.0.0.1:1").with_tls(TlsOptions::new().with_ca_file(malo)))
                .await
                .unwrap_err();
        assert!(matches!(err, FluxError::InvalidTlsOptions(_)), "{err:?}");
        assert_code(&err, "INVALID_TLS_OPTIONS");
    }
}

/// `with_tls` EXIGE TLS: contra un broker que solo habla texto plano falla en `connect()`,
/// no degrada en silencio —que enviaría la credencial en claro—.
#[tokio::test]
async fn tls_exigido_contra_un_broker_sin_tls_falla() {
    let bin = broker_bin!();
    let pki = Pki::new();
    let perms = Perms::owner("pedidos-api", "pedidos");
    let broker = Broker::start(&bin, "", &accounts(&[pw_user("pedidos-api", &perms)]));

    let err = connect_debe_fallar(
        tls_opts(&broker.url()).with_tls(TlsOptions::new().with_ca_file(pki.f("ca.pem"))),
    )
    .await;
    assert_rejected(&err, RejectionReason::Tls);

    // El contrario: sin `with_tls` el mismo broker en texto plano funciona (desarrollo).
    connect(tls_opts(&broker.url()))
        .await
        .expect("texto plano sin TLS exigido");
    // Y `with_required(false)` lo permite explícitamente.
    connect(tls_opts(&broker.url()).with_tls(TlsOptions::new().with_required(false)))
        .await
        .expect("TLS no exigido");
}

#[tokio::test]
async fn tls_mutuo_con_certificado_de_cliente() {
    let bin = broker_bin!();
    let pki = Pki::new();
    let perms = Perms::owner("pedidos-api", "pedidos");
    let broker = Broker::start(
        &bin,
        &pki.server_conf(true),
        &accounts(&[pw_user("pedidos-api", &perms)]),
    );
    let ops_tls = async_nats::ConnectOptions::with_user_and_password("ops".into(), "pw".into())
        .require_tls(true)
        .add_root_certificates(pki.f("ca.pem"))
        .add_client_certificate(pki.f("cliente.pem"), pki.f("cliente.key"))
        .connect(broker.url())
        .await
        .expect("ops con mTLS");
    provision_streams(&ops_tls, &["pedidos"]).await;

    // Lo correcto: CA + certificado y clave de cliente.
    let tls = TlsOptions::new()
        .with_ca_file(pki.f("ca.pem"))
        .with_client_cert(pki.f("cliente.pem"), pki.f("cliente.key"));
    let bus = connect(tls_opts(&broker.url()).with_tls(tls))
        .await
        .expect("mTLS");
    bus.publish(
        "pedidos.pedido.v1.creado",
        &json!({ "pedidoId": "p", "aggregateVersion": 1 }),
    )
    .await
    .expect("publicar por mTLS");

    // Lo incorrecto: el broker exige certificado de cliente y no se presenta ninguno.
    let err = connect_debe_fallar(
        tls_opts(&broker.url()).with_tls(TlsOptions::new().with_ca_file(pki.f("ca.pem"))),
    )
    .await;
    assert_rejected(&err, RejectionReason::Tls);
}

// ═════════════════════════════════════════════════════════════════════════════
//  Esperar al broker sigue funcionando
// ═════════════════════════════════════════════════════════════════════════════

/// El reintento propio de `connect()` conserva lo que prometía `retry_on_initial_connect`:
/// un servicio que arranca ANTES que el broker no muere, y conecta cuando éste aparece.
#[tokio::test]
async fn connect_espera_al_broker_que_aun_no_ha_arrancado() {
    let bin = broker_bin!();
    let port = free_port();
    let url = format!("nats://127.0.0.1:{port}");
    let conectando = tokio::spawn(connect(
        ConnectOptions::new(&url, "pedidos-api", "test", "1.0.0").with_credentials(
            Credentials::UserPassword {
                user: "pedidos-api".into(),
                password: "pw".into(),
            },
        ),
    ));

    // El broker no existe todavía: connect() sigue esperando, no ha fallado.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(
        !conectando.is_finished(),
        "connect() no debe rendirse porque el broker tarde"
    );

    // Aparece el broker EN ESE PUERTO.
    let dir = Tmp::new("late");
    let conf = dir.join("nats.conf");
    std::fs::write(
        &conf,
        format!(
            "host: 127.0.0.1\nport: {port}\njetstream {{ store_dir: \"{}\" }}\n{}\n",
            dir.join("js").display(),
            accounts(&[pw_user(
                "pedidos-api",
                &Perms::owner("pedidos-api", "pedidos")
            )])
        ),
    )
    .expect("conf");
    let mut server = Command::new(&bin)
        .arg("-c")
        .arg(&conf)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("nats-server");

    let r = tokio::time::timeout(Duration::from_secs(15), conectando).await;
    let _ = server.kill();
    let _ = server.wait();
    r.expect("connect() debe terminar al aparecer el broker")
        .expect("join")
        .expect("y conectar");
}

// ═════════════════════════════════════════════════════════════════════════════
//  (g) Un pull denegado hace fallar `subscribe()` — 06-security.md §3.7
// ═════════════════════════════════════════════════════════════════════════════

/// Sin permiso de pull (`$JS.API.CONSUMER.MSG.NEXT.<stream>.<durable>`) el broker descarta la
/// petición y no contesta: el consumidor quedaría inerte y PARECERÍA sano. `subscribe()` debe
/// fallar —con `PERMISSION_DENIED`, nombrando el subject— en vez de devolver `Ok`.
///
/// Antes de este cambio devolvía `Ok` a los ~4 ms y la denegación solo llegaba por el logger,
/// ~2 ms después (medido contra nats-server 2.14.5).
#[tokio::test]
async fn g_sin_permiso_de_pull_subscribe_falla_con_permission_denied() {
    let bin = broker_bin!();
    let pedidos = Perms::owner("pedidos-api", "pedidos");
    // TODO lo demás concedido: lo único que falta es pedir mensajes.
    let facturacion =
        Perms::consumer("facturacion-api", SUBJECT).without("$JS.API.CONSUMER.MSG.NEXT.");
    let broker = Broker::start(
        &bin,
        "",
        &accounts(&[
            pw_user("pedidos-api", &pedidos),
            pw_user("facturacion-api", &facturacion),
        ]),
    );
    let ops = ops(&broker).await;
    provision_streams(&ops, &["pedidos"]).await;

    let (log, lines) = capture_log();
    let productor = connect(opts(&broker, "pedidos-api"))
        .await
        .expect("conexión");
    let consumidor = connect(opts(&broker, "facturacion-api").with_logger(log))
        .await
        .expect("conexión");
    let calls: Calls = Arc::new(Mutex::new(Vec::new()));

    let t = Instant::now();
    let err = tokio::time::timeout(
        Duration::from_secs(4),
        consumidor.subscribe(SUBJECT, handler(&calls)),
    )
    .await
    .expect("NO debe colgarse")
    .expect_err("subscribe() debe fallar: un pull denegado deja el consumidor inerte");
    assert!(
        t.elapsed() < Duration::from_secs(3),
        "tardó {:?}",
        t.elapsed()
    );

    // PERMISSION_DENIED, PERMANENT, y NO NOT_PROVISIONED: el recurso existe, falta el permiso.
    assert!(matches!(err, FluxError::PermissionDenied { .. }), "{err:?}");
    assert_eq!(err.code(), Some("PERMISSION_DENIED"));
    assert_eq!(err.class(), Some(ErrorClass::Permanent));

    // Accionable: el subject denegado COMPLETO y sin normalizar, lo que se intentaba y dónde
    // se arregla.
    let durable = flux::durable_name("facturacion-api", SUBJECT).expect("durable");
    let denegado = format!("$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.{durable}");
    let texto = err.to_string();
    assert!(
        texto.contains(&format!("Publish to \"{denegado}\"")),
        "{texto}"
    );
    assert!(texto.contains("pedir mensajes del consumidor"), "{texto}");
    assert!(texto.contains("inerte"), "{texto}");
    assert!(texto.contains("06-security.md"), "{texto}");

    // El logger lo registra igual (§3.7): es la única salida de un pull que se deniega DESPUÉS
    // de suscribirse, y la sonda de `subscribe()` no la quita.
    assert!(
        wait_for(Duration::from_secs(2), || denials(&lines)
            .iter()
            .any(|l| l.contains(&denegado)))
        .await,
        "{:?}",
        lines.lock().unwrap()
    );
    // Y el servidor lo confirma: es una violación de permisos real, no un fallo del SDK.
    assert!(
        broker
            .violations()
            .iter()
            .any(|l| l.contains(&format!("Publish Violation - Subject \"{denegado}\""))),
        "{:?}",
        broker.violations()
    );

    // No quedó ningún bucle de consumo: un evento publicado ahora no llega a ningún handler.
    productor
        .publish(
            SUBJECT,
            &json!({ "pedidoId": "ped-1", "caso": "ok", "aggregateVersion": 1 }),
        )
        .await
        .expect("publicación");
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(calls.lock().unwrap().is_empty(), "{:?}", calls.lock());
    let (pendientes, suelo, _) = consumer_state(&ops, "facturacion-api", SUBJECT).await;
    assert_eq!((pendientes, suelo), (0, 0), "nada se entregó");
}

/// El contrario, y sin él el anterior no demuestra nada: con el permiso concedido, el MISMO
/// servicio se suscribe y consume. Además, con un evento ya pendiente **antes** de suscribirse:
/// la sonda del permiso no puede llevárselo —si lo hiciera, el handler no lo vería hasta
/// `ack_wait` (30 s) y esta espera de 5 s fallaría—.
#[tokio::test]
async fn g_con_el_permiso_de_pull_subscribe_funciona_y_la_sonda_no_se_lleva_ningun_mensaje() {
    let bin = broker_bin!();
    let pedidos = Perms::owner("pedidos-api", "pedidos");
    let facturacion = Perms::consumer("facturacion-api", SUBJECT);
    let broker = Broker::start(
        &bin,
        "",
        &accounts(&[
            pw_user("pedidos-api", &pedidos),
            pw_user("facturacion-api", &facturacion),
        ]),
    );
    let ops = ops(&broker).await;
    provision_streams(&ops, &["pedidos"]).await;
    // El consumidor existe ANTES de que llegue el evento: así el mensaje queda pendiente para
    // él, esperando a quien lo pida.
    provision_consumer(&ops, "facturacion-api", SUBJECT).await;

    let productor = connect(opts(&broker, "pedidos-api"))
        .await
        .expect("conexión");
    productor
        .publish(
            SUBJECT,
            &json!({ "pedidoId": "ped-1", "caso": "ok", "aggregateVersion": 1 }),
        )
        .await
        .expect("publicación");

    let (log, lines) = capture_log();
    let consumidor = connect(opts(&broker, "facturacion-api").with_logger(log))
        .await
        .expect("conexión");
    let calls: Calls = Arc::new(Mutex::new(Vec::new()));
    let sub = consumidor
        .subscribe(SUBJECT, handler(&calls))
        .await
        .expect("con el permiso de pull concedido, subscribe() funciona");

    assert!(
        wait_for(Duration::from_secs(5), || !calls.lock().unwrap().is_empty()).await,
        "el evento pendiente debe llegar al handler: la sonda no puede habérselo llevado"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        calls.lock().unwrap().clone(),
        vec![("ok".to_string(), 1)],
        "una sola entrega, a la primera"
    );
    let (pendientes, suelo, reentregas) = consumer_state(&ops, "facturacion-api", SUBJECT).await;
    assert_eq!(
        (pendientes, suelo, reentregas),
        (0, 1, 0),
        "confirmado y sin reentregas"
    );
    assert!(denials(&lines).is_empty(), "{:?}", denials(&lines));
    sub.unsubscribe();
}

/// La premisa del broker en la que descansa la sonda, medida contra el servidor real: un pull
/// con el cuerpo truncado `{` se contesta `400 Bad Request` **sin** sacar ningún mensaje ni
/// apuntarse como petición pendiente… y un pull VÁLIDO sí se lleva el mensaje. Esa diferencia
/// es la razón de que la sonda sea un JSON roto y no un pull de verdad.
///
/// Si una versión futura de nats-server cambiara cualquiera de las dos cosas, falla ESTE test
/// —con el motivo— antes que el de `subscribe()`.
#[tokio::test]
async fn g_la_sonda_del_pull_es_inocua_y_un_pull_valido_no_lo_seria() {
    let bin = broker_bin!();
    let broker = Broker::start(&bin, "", &accounts(&[]));
    let ops = ops(&broker).await;
    provision_streams(&ops, &["pedidos"]).await;
    provision_consumer(&ops, "facturacion-api", SUBJECT).await;
    let js = jetstream::new(ops.clone());
    js.publish(SUBJECT, r#"{"x":1}"#.into())
        .await
        .expect("publicar")
        .await
        .expect("acuse");
    let durable = flux::durable_name("facturacion-api", SUBJECT).expect("durable");
    let pull = format!("$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.{durable}");
    let info = || async {
        js.get_stream("EVT_PEDIDOS")
            .await
            .expect("stream")
            .consumer_info(&durable)
            .await
            .expect("info del consumidor")
    };

    // 1. La sonda: 400, y el consumidor ni se entera.
    let r = tokio::time::timeout(
        Duration::from_secs(2),
        ops.request(pull.clone(), "{".into()),
    )
    .await
    .expect("el servidor contesta a la sonda")
    .expect("respuesta");
    assert_eq!(r.status.map(|s| s.as_u16()), Some(400), "{r:?}");
    assert!(
        r.description
            .as_deref()
            .is_some_and(|d| d.starts_with("Bad Request")),
        "{r:?}"
    );
    assert!(r.payload.is_empty());
    let i = info().await;
    assert_eq!(
        (i.num_pending, i.num_ack_pending, i.num_waiting),
        (1, 0, 0),
        "la sonda no saca el mensaje ni queda esperando"
    );

    // 2. Un pull válido, en cambio, se lleva el mensaje (y queda sin ack). Se recibe por una
    // suscripción propia y no con `request()`: las entregas de un pull llegan con el subject
    // ORIGINAL del evento (A-22), que el multiplexor de respuestas de `async-nats` no sabe a
    // qué petición atribuir.
    let inbox = ops.new_inbox();
    let mut entregas = ops.subscribe(inbox.clone()).await.expect("suscripción");
    ops.publish_with_reply(pull, inbox, r#"{"batch":1,"no_wait":true}"#.into())
        .await
        .expect("pull válido");
    let entrega = tokio::time::timeout(Duration::from_secs(2), entregas.next())
        .await
        .expect("un pull válido recibe el mensaje")
        .expect("suscripción abierta");
    assert!(
        entrega.status.is_none() && !entrega.payload.is_empty(),
        "un pull válido entrega el mensaje: {entrega:?}"
    );
    assert_eq!(info().await.num_ack_pending, 1);
}

// ═════════════════════════════════════════════════════════════════════════════
//  (h) Los tres códigos de `connect()` — 04-errors.md §5.3
// ═════════════════════════════════════════════════════════════════════════════

/// Lo que debe producir cada situación.
#[derive(Debug, Clone, Copy)]
enum Espera {
    /// `CONNECTION_REJECTED`, con este motivo: el broker contestó y lo rechazó.
    Rechazo(RejectionReason),
    /// `INVALID_CREDENTIALS`: opción mal formada, sin red.
    Credenciales,
    /// `INVALID_TLS_OPTIONS`: opción mal formada, sin red.
    OpcionesTls,
    /// Un error de `connect()` que NO es ninguno de los tres: sin código estable.
    SinCodigo,
}

impl Espera {
    fn codigo(self) -> Option<&'static str> {
        match self {
            Self::Rechazo(_) => Some("CONNECTION_REJECTED"),
            Self::Credenciales => Some("INVALID_CREDENTIALS"),
            Self::OpcionesTls => Some("INVALID_TLS_OPTIONS"),
            Self::SinCodigo => None,
        }
    }
}

/// Cada código de `connect()` se produce en su situación **y en ninguna otra**: la tabla
/// recorre TODAS las situaciones de 03-delivery.md §6.1 contra brokers reales y exige, de
/// cada una, exactamente su código —ni uno de los otros dos, ni ninguno—. Una situación que
/// diera el código equivocado hace fallar la fila que la nombra.
///
/// La fila que falta, a propósito, es «el broker aún no ha arrancado»: no es un error, se
/// espera (`connect_espera_al_broker_que_aun_no_ha_arrancado`).
#[tokio::test]
async fn h_cada_codigo_de_connect_se_produce_en_su_situacion_y_en_ninguna_otra() {
    use RejectionReason::{Auth, Tls};
    let bin = broker_bin!();
    let pki = Pki::new();
    let perms = Perms::owner("pedidos-api", "pedidos");
    let users = accounts(&[pw_user("pedidos-api", &perms)]);
    let plano = Broker::start(&bin, "", &users);
    let tls = Broker::start(&bin, &pki.server_conf(false), &users);
    let mtls = Broker::start(&bin, &pki.server_conf(true), &users);
    let otro_host = Broker::start(
        &bin,
        &pki.server_conf_con("servidor-otro-host", false),
        &users,
    );
    let caducado = Broker::start(
        &bin,
        &pki.server_conf_con("servidor-caducado", false),
        &users,
    );

    let con_ca = || TlsOptions::new().with_ca_file(pki.f("ca.pem"));
    // `nats://127.0.0.1:1`: no hay nadie. Las opciones mal formadas deben fallar ANTES de
    // intentar conectar, así que a quien contestara habría que esperarlo (y la fila fallaría
    // por tiempo, no por código).
    let sin_red = |c: Credentials, t: Option<TlsOptions>| {
        let o = ConnectOptions::new("nats://127.0.0.1:1", "pedidos-api", "test", "1.0.0")
            .with_credentials(c);
        match t {
            Some(t) => o.with_tls(t),
            None => o,
        }
    };
    let usuario = |pw: &str| Credentials::UserPassword {
        user: "pedidos-api".into(),
        password: pw.into(),
    };
    let ok_cred = || usuario("pw");

    let filas: Vec<(&str, ConnectOptions, Espera)> = vec![
        // ── CONNECTION_REJECTED, motivo `auth`: el broker contestó y no acepta la credencial.
        (
            "contraseña equivocada",
            tls_opts(&plano.url()).with_credentials(usuario("equivocada")),
            Espera::Rechazo(Auth),
        ),
        (
            "usuario que el broker no conoce",
            tls_opts(&plano.url()).with_credentials(Credentials::UserPassword {
                user: "nadie".into(),
                password: "pw".into(),
            }),
            Espera::Rechazo(Auth),
        ),
        (
            "sin credencial en un broker que la exige",
            tls_opts(&plano.url()).with_credentials(Credentials::None),
            Espera::Rechazo(Auth),
        ),
        (
            "token que el broker no acepta",
            tls_opts(&plano.url()).with_credentials(Credentials::Token("tok".into())),
            Espera::Rechazo(Auth),
        ),
        // ── CONNECTION_REJECTED, motivo `tls`: el handshake no encaja.
        (
            "certificado de servidor sin CA propia (raíces del sistema)",
            tls_opts(&tls.url()).with_tls(TlsOptions::new()),
            Espera::Rechazo(Tls),
        ),
        (
            "una CA que no firmó el certificado del servidor",
            tls_opts(&tls.url()).with_tls(TlsOptions::new().with_ca_file(pki.f("otra-ca.pem"))),
            Espera::Rechazo(Tls),
        ),
        (
            "host fuera de los SAN del certificado",
            tls_opts(&otro_host.url()).with_tls(con_ca()),
            Espera::Rechazo(Tls),
        ),
        (
            "certificado de servidor caducado",
            tls_opts(&caducado.url()).with_tls(con_ca()),
            Espera::Rechazo(Tls),
        ),
        (
            "TLS exigido y el broker no lo ofrece",
            tls_opts(&plano.url()).with_tls(con_ca()),
            Espera::Rechazo(Tls),
        ),
        (
            "mTLS: el broker exige certificado de cliente y no se presenta ninguno",
            tls_opts(&mtls.url()).with_tls(con_ca()),
            Espera::Rechazo(Tls),
        ),
        // ── INVALID_CREDENTIALS: la opción está mal formada y se sabe sin red.
        (
            "token vacío",
            sin_red(Credentials::Token(String::new()), None),
            Espera::Credenciales,
        ),
        (
            "contraseña en blanco",
            sin_red(usuario("  "), None),
            Espera::Credenciales,
        ),
        (
            "semilla nkey mal formada",
            sin_red(Credentials::NkeySeed("SUMAL".into()), None),
            Espera::Credenciales,
        ),
        (
            "semilla nkey de cuenta, no de usuario",
            sin_red(
                Credentials::NkeySeed(nkeys::KeyPair::new_account().seed().expect("semilla")),
                None,
            ),
            Espera::Credenciales,
        ),
        (
            "fichero de semilla que no existe",
            sin_red(Credentials::NkeyFile("/no/existe.nk".into()), None),
            Espera::Credenciales,
        ),
        (
            ".creds que no existe",
            sin_red(Credentials::CredsFile("/no/existe.creds".into()), None),
            Espera::Credenciales,
        ),
        // ── INVALID_TLS_OPTIONS: la opción está mal formada y se sabe sin red.
        (
            "CA que no existe",
            sin_red(
                ok_cred(),
                Some(TlsOptions::new().with_ca_file("/no/existe/ca.pem")),
            ),
            Espera::OpcionesTls,
        ),
        (
            "CA que no es un PEM (una clave)",
            sin_red(
                ok_cred(),
                Some(TlsOptions::new().with_ca_file(pki.f("servidor.key"))),
            ),
            Espera::OpcionesTls,
        ),
        (
            "certificado de cliente sin su clave",
            sin_red(
                ok_cred(),
                Some(TlsOptions {
                    client_cert: Some(pki.f("cliente.pem")),
                    ..TlsOptions::new()
                }),
            ),
            Espera::OpcionesTls,
        ),
        (
            "clave de cliente sin su certificado",
            sin_red(
                ok_cred(),
                Some(TlsOptions {
                    client_key: Some(pki.f("cliente.key")),
                    ..TlsOptions::new()
                }),
            ),
            Espera::OpcionesTls,
        ),
        // ── Ninguno de los tres: errores de `connect()` que NO deben llevar uno de estos
        // códigos, porque no son estas situaciones.
        (
            "falta el nombre de servicio",
            ConnectOptions::new(plano.url(), "", "test", "1.0.0"),
            Espera::SinCodigo,
        ),
        (
            "nombre de servicio que no es kebab-case",
            ConnectOptions::new(plano.url(), "PedidosAPI", "test", "1.0.0"),
            Espera::SinCodigo,
        ),
    ];

    let mut vistos = BTreeSet::new();
    let mut desviaciones = Vec::new();
    for (situacion, opciones, espera) in filas {
        let err = connect_debe_fallar(opciones).await;
        if err.code() != espera.codigo() {
            desviaciones.push(format!(
                "«{situacion}»: esperaba {:?} y dio {:?} ({err:?})",
                espera.codigo(),
                err.code()
            ));
            continue;
        }
        match (espera, &err) {
            (Espera::Rechazo(r), FluxError::ConnectionRejected { reason, .. }) => {
                if *reason != r {
                    desviaciones.push(format!("«{situacion}»: esperaba motivo {r} y dio {reason}"));
                }
            }
            (Espera::Credenciales, FluxError::InvalidCredentials(_))
            | (Espera::OpcionesTls, FluxError::InvalidTlsOptions(_))
            | (Espera::SinCodigo, _) => {}
            (e, otro) => desviaciones.push(format!(
                "«{situacion}»: código {:?} pero variante {e:?} ≠ {otro:?}",
                espera.codigo()
            )),
        }
        // PERMANENT, siempre que hay código: reintentar no lo arregla.
        if espera.codigo().is_some() {
            assert_eq!(err.class(), Some(ErrorClass::Permanent), "«{situacion}»");
        }
        vistos.insert(err.code().map(str::to_string));
    }
    assert!(
        desviaciones.is_empty(),
        "situaciones que dieron un código que no les toca:\n  {}",
        desviaciones.join("\n  ")
    );
    // Y los tres códigos aparecieron: una tabla que no produjera uno de ellos no lo probaría.
    for codigo in [
        "CONNECTION_REJECTED",
        "INVALID_CREDENTIALS",
        "INVALID_TLS_OPTIONS",
    ] {
        assert!(vistos.contains(&Some(codigo.to_string())), "{codigo}");
    }
}

/// El mensaje de un rechazo dice la causa de verdad y no vuelca la credencial, y lo que cada
/// causa pide revisar es distinto: eso es lo que justifica dos motivos.
#[tokio::test]
async fn h_el_rechazo_dice_la_causa_y_que_revisar() {
    let bin = broker_bin!();
    let pki = Pki::new();
    let perms = Perms::owner("pedidos-api", "pedidos");
    let users = accounts(&[pw_user("pedidos-api", &perms)]);
    let plano = Broker::start(&bin, "", &users);
    let otro_host = Broker::start(
        &bin,
        &pki.server_conf_con("servidor-otro-host", false),
        &users,
    );
    let caducado = Broker::start(
        &bin,
        &pki.server_conf_con("servidor-caducado", false),
        &users,
    );
    let con_ca = || TlsOptions::new().with_ca_file(pki.f("ca.pem"));

    let err = connect_debe_fallar(tls_opts(&plano.url()).with_credentials(
        Credentials::UserPassword {
            user: "pedidos-api".into(),
            password: "contraseña-secreta".into(),
        },
    ))
    .await;
    let texto = err.to_string();
    assert!(texto.contains("rechazó la credencial"), "{texto}");
    assert!(texto.contains(&plano.url()), "nombra el broker: {texto}");
    assert!(texto.contains("Reintentar no lo arregla"), "{texto}");
    assert!(!texto.contains("contraseña-secreta"), "{texto}");
    assert!(!texto.contains("SAN"), "no mezcla las causas: {texto}");

    // Host fuera de los SAN, y caducado: dos causas de TLS que el mensaje distingue por la
    // causa de la librería, y que piden revisar cosas distintas.
    let err = connect_debe_fallar(tls_opts(&otro_host.url()).with_tls(con_ca())).await;
    let texto = err.to_string();
    assert!(texto.contains("not valid for name"), "{texto}");
    assert!(texto.contains("SAN"), "{texto}");
    let err = connect_debe_fallar(tls_opts(&caducado.url()).with_tls(con_ca())).await;
    let texto = err.to_string();
    assert!(texto.to_lowercase().contains("expired"), "{texto}");
    assert!(!texto.contains("rechazó la credencial"), "{texto}");
}
