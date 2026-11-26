//! Estado replicado ejecutado contra un `nats-server` REAL — plan v1.1, Fase 2.
//!
//! `specification/10-state.md` es el contrato; este fichero lo ejecuta con la API de Rust
//! (`Bus::state()`), con tres entradas distintas:
//!
//! 1. **Los escenarios de `conformance/cases/state-semantics.json` como DATOS** (`semantica_*`):
//!    el fichero que ejecuta la implementación de referencia en JavaScript lo ejecuta aquí, paso
//!    a paso, este SDK. Lo esperado es lo del fichero, no lo que este SDK prefiera.
//! 2. **Permisos exactos** (`acl_*`): una configuración de broker escrita a mano, modelada en
//!    `protocol.json` → `state.acl` —dueño que escribe, observador de **un tenant**, sin
//!    `STREAM.INFO.STATE_<DOM>`— y el ciclo de aceptación de la fase: el dueño escribe 100 claves,
//!    un observador **nuevo** las recibe todas y recibe `on_synced`, se borra una, el observador
//!    recibe la lápida y `get` de esa clave da `None`. **En los dos sentidos**: quitar un permiso
//!    se nota.
//! 3. **Comportamiento** (`dlq_*`, `validacion_*`, `metricas_*`…): lo que el SDK promete y que
//!    ningún otro test puede ver, como que una clave inválida falla **antes de la red**.
//!
//! ```text
//! NATS_SERVER=/ruta/a/nats-server cargo test --all-features --test state
//! ```
//!
//! Sin binario, los tests se **saltan con un aviso**… salvo que `FLUX_REQUIRE_BROKER=1`: en CI eso
//! los convierte en un fallo. Un fixture que nadie ejecuta aparenta cobertura.
//!
//! Los puertos salen del rango `FLUX_TEST_PORT_BASE` (default 4500) + 50. Se usan hasta 6 a la
//! vez (el cluster de tres nodos).
//!
//! Los seis códigos de estado —`INVALID_TENANT_FOR_STATE`, `INVALID_STATE_KEY`,
//! `MISSING_AGGREGATE_VERSION`, `STATE_SUBJECT_MISMATCH`, `STATE_CONFLICT` y `RESERVED_DOMAIN`—
//! se comprueban por su literal en `codigos_estables`, que es lo que `scripts/check-sdk-codes.mjs`
//! exige de la evidencia de la capacidad.

use std::collections::{BTreeSet, HashMap};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_nats::jetstream::{self, consumer, stream};
use flux::state::{AfterOpen, WriteHook};
use flux::{
    connect, Bus, ConnectOptions, Credentials, DataClassification, DeleteOptions, ErrorClass,
    FluxError, GetOptions, InMemoryMetrics, LogFn, LogLevel, PutOptions, StateEntry, StateWrite,
    TenantIsolation, WatchOptions, WriteOutcome,
};
use serde_json::{json, Value};

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
         (o ponlo en el PATH). Sin esto los tests de estado no se ejecutan, y un fixture que \
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

/// Un puerto libre del rango propio, comprobado con un `bind` real: un servidor anterior que no
/// terminó de morir no debe romper este.
fn free_port() -> u16 {
    let base: u16 = std::env::var("FLUX_TEST_PORT_BASE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4500);
    const SPAN: u16 = 50;
    for _ in 0..SPAN {
        let port = base + NEXT_PORT.fetch_add(1, Ordering::SeqCst) % SPAN;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("no queda ningún puerto libre en {base}..{}", base + SPAN);
}

fn temp_dir(prefijo: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "flux-{prefijo}-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).expect("directorio temporal");
    dir
}

fn espera_puerto(child: &mut Child, port: u16, dir: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if std::net::TcpStream::connect_timeout(
            &([127, 0, 0, 1], port).into(),
            Duration::from_millis(200),
        )
        .is_ok()
        {
            return;
        }
        if let Ok(Some(status)) = child.try_wait() {
            let log = std::fs::read_to_string(dir.join("server.log")).unwrap_or_default();
            let err = std::fs::read_to_string(dir.join("server.err")).unwrap_or_default();
            panic!("nats-server terminó ({status}) antes de aceptar conexiones:\n{log}\n{err}");
        }
        assert!(
            Instant::now() < deadline,
            "nats-server no abrió el puerto {port}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Un `nats-server` con JetStream, su configuración y su directorio temporal. Al soltarlo mata
/// SOLO su proceso y borra su almacén.
struct Broker {
    child: Child,
    port: u16,
    dir: PathBuf,
}

impl Broker {
    /// `auth` es el bloque `authorization {…}` (o vacío).
    fn start(bin: &Path, auth: &str) -> Self {
        let port = free_port();
        let dir = temp_dir("state");
        let conf = format!(
            "host: 127.0.0.1\nport: {port}\njetstream {{ store_dir: \"{}\" }}\n{auth}\n",
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
            .stderr(std::fs::File::create(dir.join("server.err")).expect("stderr"))
            .spawn()
            .expect("no se pudo lanzar nats-server");
        espera_puerto(&mut child, port, &dir);
        Self { child, port, dir }
    }

    fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }

    /// Las violaciones de permisos que el servidor registró. Formato observado en nats-server
    /// 2.14.5: `Publish Violation - Subject "…"` y `Subscription Violation - Subject "…"`.
    fn violations(&self) -> Vec<String> {
        std::fs::read_to_string(self.dir.join("server.log"))
            .unwrap_or_default()
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

/// Un cluster de tres nodos locales: es donde el conflicto de secuencia es el **10164** (A-49).
struct Cluster {
    children: Vec<Child>,
    ports: Vec<u16>,
    dir: PathBuf,
}

impl Cluster {
    fn start(bin: &Path) -> Self {
        let dir = temp_dir("cluster");
        let nodes: Vec<(u16, u16)> = (0..3).map(|_| (free_port(), free_port())).collect();
        let mut children = Vec::new();
        for (i, (client, cluster)) in nodes.iter().enumerate() {
            let routes: Vec<String> = nodes
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, (_, c))| format!("nats://127.0.0.1:{c}"))
                .collect();
            let node_dir = dir.join(format!("n{i}"));
            std::fs::create_dir_all(node_dir.join("js")).expect("directorio del nodo");
            let conf = format!(
                "server_name: n{i}\nport: {client}\nhost: 127.0.0.1\n\
                 jetstream {{ store_dir: \"{}\" }}\n\
                 cluster {{ name: fluxtest\n listen: 127.0.0.1:{cluster}\n routes: [{}]\n}}\n",
                node_dir.join("js").display(),
                routes.join(", ")
            );
            let conf_path = node_dir.join("n.conf");
            std::fs::write(&conf_path, conf).expect("config");
            children.push(
                Command::new(bin)
                    .arg("-c")
                    .arg(&conf_path)
                    .arg("-l")
                    .arg(node_dir.join("server.log"))
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .expect("no se pudo lanzar nats-server"),
            );
        }
        for (client, _) in &nodes {
            let deadline = Instant::now() + Duration::from_secs(10);
            while std::net::TcpStream::connect_timeout(
                &([127, 0, 0, 1], *client).into(),
                Duration::from_millis(200),
            )
            .is_err()
            {
                assert!(Instant::now() < deadline, "el nodo {client} no arrancó");
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        // La elección de líder de la capa de metadatos.
        std::thread::sleep(Duration::from_millis(3500));
        Self {
            children,
            ports: nodes.iter().map(|(c, _)| *c).collect(),
            dir,
        }
    }

    /// El primer nodo. **No** una lista separada por comas: `ConnectOptions::servers` lo
    /// documenta, pero `async-nats` 0.50 interpreta un `&str` como UNA dirección y una lista da
    /// «NATS server URL is invalid: invalid port number». El stream es R3 y las escrituras
    /// atraviesan el cluster igualmente.
    fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.ports[0])
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for c in &mut self.children {
            let _ = c.kill();
            let _ = c.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// ═════════════════════════════════════════════════════════════════════════════
//  Utilidades
// ═════════════════════════════════════════════════════════════════════════════

/// Espera a que `cond` se cumpla, o falla con `msg`.
async fn espera<F: Fn() -> bool>(cond: F, hasta: Duration, msg: &str) {
    let deadline = Instant::now() + hasta;
    while !cond() {
        assert!(Instant::now() < deadline, "{msg}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Un logger que recoge lo que el SDK registra.
fn logger_espia() -> (LogFn, Arc<Mutex<Vec<String>>>) {
    let lineas = Arc::new(Mutex::new(Vec::new()));
    let destino = Arc::clone(&lineas);
    let log: LogFn = Arc::new(move |_nivel: LogLevel, msg: &str| {
        lock(&destino).push(msg.to_string());
    });
    (log, lineas)
}

const CONTRATO: &str = "clientes.cliente.v1";
const STREAM: &str = "STATE_CLIENTES";
const DLQ: &str = "DLQ_CLIENTES";

/// Una conexión del SDK. El usuario del broker es el nombre del servicio.
async fn bus(url: &str, servicio: &str, tenant: Option<&str>) -> Bus {
    bus_con(url, servicio, tenant, |o| o).await
}

async fn bus_con(
    url: &str,
    servicio: &str,
    tenant: Option<&str>,
    ajusta: impl FnOnce(ConnectOptions) -> ConnectOptions,
) -> Bus {
    let mut opts = ConnectOptions::new(url, servicio, "test", "1.0.0")
        .with_credentials(Credentials::UserPassword {
            user: servicio.to_string(),
            password: "pw".to_string(),
        })
        .with_schema_base_url("https://schemas.internal");
    if let Some(t) = tenant {
        opts = opts.with_tenant_id(t);
    }
    connect(ajusta(opts)).await.expect("conexión")
}

/// Un cliente con permisos totales: es quien aprovisiona (`flux provision`) y quien mira.
struct Ops {
    client: async_nats::Client,
    js: jetstream::Context,
}

impl Ops {
    async fn new(url: &str) -> Self {
        let client = async_nats::ConnectOptions::with_user_and_password("ops".into(), "pw".into())
            .connect(url)
            .await
            .expect("conexión de ops");
        let js = jetstream::new(client.clone());
        Self { client, js }
    }

    /// `STATE_<DOM>` con la configuración de 10-state.md §2.1.
    async fn stream_de_estado(&self, name: &str, dominio: &str, replicas: usize) {
        self.js
            .create_stream(stream::Config {
                name: name.to_string(),
                subjects: vec![format!("state.{dominio}.>")],
                storage: stream::StorageType::File,
                retention: stream::RetentionPolicy::Limits,
                max_messages_per_subject: 1,
                allow_direct: true,
                max_age: Duration::ZERO,
                discard: stream::DiscardPolicy::Old,
                duplicate_window: Duration::from_secs(120),
                num_replicas: replicas,
                ..Default::default()
            })
            .await
            .expect("stream de estado");
    }

    /// `DLQ_<DOM>` de un dominio **con** estado: `dlq.<dom>.>` y `dlq.state.<dom>.>`.
    async fn dlq(&self, name: &str, dominio: &str) {
        self.js
            .create_stream(stream::Config {
                name: name.to_string(),
                subjects: vec![format!("dlq.{dominio}.>"), format!("dlq.state.{dominio}.>")],
                storage: stream::StorageType::File,
                ..Default::default()
            })
            .await
            .expect("DLQ");
    }

    /// El durable que `flux provision` crearía para un observador: la configuración canónica de
    /// 03-delivery.md §2, `last_per_subject` y el filtro exacto.
    async fn durable(&self, stream_name: &str, durable: &str, filtro: &str) {
        self.js
            .create_consumer_on_stream(
                consumer::pull::Config {
                    durable_name: Some(durable.to_string()),
                    filter_subject: filtro.to_string(),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ack_wait: flux::DEFAULT_ACK_WAIT,
                    max_deliver: i64::from(flux::DEFAULT_MAX_DELIVER),
                    backoff: flux::CANONICAL_BACKOFF.to_vec(),
                    max_ack_pending: i64::from(flux::DEFAULT_MAX_ACK_PENDING),
                    deliver_policy: consumer::DeliverPolicy::LastPerSubject,
                    replay_policy: consumer::ReplayPolicy::Instant,
                    ..Default::default()
                },
                stream_name,
            )
            .await
            .expect("durable aprovisionado");
    }

    async fn mensajes(&self, stream_name: &str) -> u64 {
        self.js
            .get_stream(stream_name)
            .await
            .expect("stream")
            .cached_info()
            .state
            .messages
    }

    /// `DIRECT.GET` por subject, a pelo: el mensaje tal cual lo guarda el stream.
    async fn leer(&self, stream_name: &str, subject: &str) -> Option<(u64, Value, String)> {
        let reply = self
            .client
            .request(
                format!("$JS.API.DIRECT.GET.{stream_name}.{subject}"),
                Vec::new().into(),
            )
            .await
            .expect("DIRECT.GET");
        if reply.status.map(|s| s.as_u16()) == Some(404) {
            return None;
        }
        let seq = reply
            .headers
            .as_ref()
            .and_then(|h| h.get("Nats-Sequence"))
            .and_then(|v| v.to_string().parse().ok())
            .expect("Nats-Sequence");
        let msg_id = reply
            .headers
            .as_ref()
            .and_then(|h| h.get("Nats-Msg-Id"))
            .map(ToString::to_string)
            .unwrap_or_default();
        Some((
            seq,
            serde_json::from_slice(&reply.payload).expect("JSON"),
            msg_id,
        ))
    }

    /// El mensaje `seq` de un stream, por `STREAM.MSG.GET` (solo ops puede).
    async fn mensaje(&self, stream_name: &str, seq: u64) -> Option<Vec<u8>> {
        let s = self.js.get_stream(stream_name).await.expect("stream");
        s.get_raw_message(seq)
            .await
            .ok()
            .map(|m| m.payload.to_vec())
    }
}

fn pii(i: usize) -> Value {
    json!({
        "clienteId": format!("cli-{i}"),
        "nombre": "María Peña",
        "email": "maria@example.com",
        "aggregateVersion": 1,
    })
}

fn con_tenant(t: &str) -> PutOptions {
    PutOptions::default().with_tenant_id(t)
}

fn codigo(r: &Result<impl std::fmt::Debug, FluxError>) -> Option<String> {
    r.as_ref().err().and_then(FluxError::code).map(String::from)
}

// ═════════════════════════════════════════════════════════════════════════════
//  1. Los escenarios de `state-semantics.json`, como datos
// ═════════════════════════════════════════════════════════════════════════════

const SEMANTICA: &str = include_str!("../../conformance/cases/state-semantics.json");

/// El hook del escenario: `beforePublish` hace escribir a un rival entre la lectura y la
/// publicación de un intento, y `lostAckOnce` hace que el primer publish se aplique pero quien lo
/// hizo vea un error de secuencia.
struct HookDelEscenario {
    bus: Bus,
    contrato: String,
    tenant: String,
    clave: String,
    rivales: Vec<(u64, u64, u64)>, // (intento, desde la versión, cuántas)
    pierde_el_primer_acuse: bool,
}

impl WriteHook for HookDelEscenario {
    fn before_publish(
        &self,
        attempt: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        let rivales: Vec<(u64, u64)> = self
            .rivales
            .iter()
            .filter(|(a, _, _)| *a == u64::from(attempt))
            .map(|(_, desde, cuantas)| (*desde, *cuantas))
            .collect();
        Box::pin(async move {
            for (desde, cuantas) in rivales {
                for v in desde..desde + cuantas {
                    let _ = self
                        .bus
                        .state()
                        .put_with(
                            &self.contrato,
                            &self.clave,
                            &json!({ "clienteId": self.clave, "aggregateVersion": v }),
                            con_tenant(&self.tenant),
                        )
                        .await;
                }
            }
        })
    }

    fn lose_ack(&self, attempt: u32) -> bool {
        self.pierde_el_primer_acuse && attempt == 1
    }
}

/// `{repeat:a:129}` dentro de una cadena.
fn expande(s: &str) -> String {
    let Some(i) = s.find("{repeat:") else {
        return s.to_string();
    };
    let resto = &s[i + "{repeat:".len()..];
    let fin = resto.find('}').expect("}");
    let (texto, n) = resto[..fin].split_once(':').expect("texto:n");
    format!(
        "{}{}{}",
        &s[..i],
        texto.repeat(n.parse().expect("n")),
        &resto[fin + 1..]
    )
}

fn txt(step: &Value, k: &str) -> String {
    expande(
        step[k]
            .as_str()
            .unwrap_or_else(|| panic!("el paso no tiene `{k}`: {step}")),
    )
}

/// La ruta con puntos de una expectativa: `entries.0.data.clienteId`.
fn camino<'a>(v: &'a Value, ruta: &str) -> Option<&'a Value> {
    ruta.split('.').try_fold(v, |actual, k| match actual {
        Value::Array(a) => k.parse::<usize>().ok().and_then(|i| a.get(i)),
        Value::Object(o) => o.get(k),
        _ => None,
    })
}

/// Un número aleatorio barato: no hace falta más para repartir versiones entre instancias.
struct Azar(u64);

impl Azar {
    fn nuevo(semilla: u64) -> Self {
        Self(semilla.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn siguiente(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    #[allow(clippy::cast_precision_loss)]
    fn unidad(&mut self) -> f64 {
        (self.siguiente() % 10_000) as f64 / 10_000.0
    }
}

/// Un caso de `state-semantics.json` ejecutado con la API del SDK.
struct Caso {
    url: String,
    ops: Ops,
    buses: Mutex<HashMap<String, Bus>>,
}

impl Caso {
    async fn bus(&self, servicio: &str) -> Bus {
        if let Some(b) = lock(&self.buses).get(servicio) {
            return b.clone();
        }
        let b = connect(
            ConnectOptions::new(&self.url, servicio, "test", "1.0.0")
                .with_schema_base_url("https://schemas.internal"),
        )
        .await
        .expect("conexión");
        lock(&self.buses).insert(servicio.to_string(), b.clone());
        b
    }

    /// El dueño: quien escribe y lee en los escenarios.
    async fn dueno(&self) -> Bus {
        self.bus("crm-api").await
    }

    async fn limpia(&self) {
        let mut nombres = self.ops.js.stream_names();
        let mut todos = Vec::new();
        while let Some(Ok(n)) = futures::StreamExt::next(&mut nombres).await {
            todos.push(n);
        }
        for n in todos {
            let _ = self.ops.js.delete_stream(&n).await;
        }
    }

    async fn paso(&self, step: &Value) -> Value {
        match step["op"].as_str().expect("op") {
            "stream_add" => {
                let replicas = step["config"]["num_replicas"].as_u64().unwrap_or(1);
                let dominio = txt(step, "name")
                    .strip_prefix("STATE_")
                    .map(str::to_lowercase)
                    .expect("STATE_<DOM>");
                self.ops
                    .stream_de_estado(
                        &txt(step, "name"),
                        &dominio,
                        usize::try_from(replicas).unwrap(),
                    )
                    .await;
                json!({ "outcome": "ok", "config": { "num_replicas": replicas } })
            }
            "stream_state" => {
                let mut s = self
                    .ops
                    .js
                    .get_stream(txt(step, "name"))
                    .await
                    .expect("stream");
                let info = s.info().await.expect("info");
                json!({ "messages": info.state.messages, "lastSeq": info.state.last_sequence })
            }
            "state_put" => self.put_o_delete(step, false).await,
            "state_delete" => self.put_o_delete(step, true).await,
            "state_get" => self.get(step).await,
            "state_watch" => self.watch(step).await,
            "state_race" => self.carrera(step).await,
            otro => panic!("operación desconocida «{otro}»"),
        }
    }

    async fn put_o_delete(&self, step: &Value, borrar: bool) -> Value {
        let bus = self.dueno().await;
        let contrato = txt(step, "contract");
        let tenant = txt(step, "tenant");
        let clave = txt(step, "key");
        let version = step["version"].clone();

        // Los escenarios con `hook` usan el del SDK; sin él, la llamada normal.
        let hook: Option<Arc<dyn WriteHook>> = step.get("hook").map(|h| {
            let rivales = h["beforePublish"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|r| {
                            (
                                r["attempt"].as_u64().expect("attempt"),
                                r["fromVersion"].as_u64().expect("fromVersion"),
                                r["count"].as_u64().unwrap_or(1),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            Arc::new(HookDelEscenario {
                bus: bus.clone(),
                contrato: contrato.clone(),
                tenant: tenant.clone(),
                clave: clave.clone(),
                rivales,
                pierde_el_primer_acuse: h["lostAckOnce"].as_bool().unwrap_or(false),
            }) as Arc<dyn WriteHook>
        });

        let resultado = if borrar {
            let v = version
                .as_u64()
                .expect("una versión de lápida entera y positiva");
            let mut o = DeleteOptions::default().with_tenant_id(&tenant);
            if let Some(h) = hook {
                o = o.with_hook(h);
            }
            bus.state().delete_with(&contrato, &clave, v, o).await
        } else {
            // La misma `data` que escribe la implementación de referencia.
            let data = json!({ "clienteId": clave, "aggregateVersion": version });
            let mut o = con_tenant(&tenant);
            if let Some(h) = hook {
                o = o.with_hook(h);
            }
            bus.state().put_with(&contrato, &clave, &data, o).await
        };
        observa_escritura(&resultado)
    }

    async fn get(&self, step: &Value) -> Value {
        let bus = self.dueno().await;
        let (contrato, tenant, clave) =
            (txt(step, "contract"), txt(step, "tenant"), txt(step, "key"));
        let entrada = bus
            .state()
            .get_with(
                &contrato,
                &clave,
                GetOptions::default().with_tenant_id(&tenant),
            )
            .await
            .expect("get");
        if let Some(e) = entrada {
            return json!({
                "isNull": false, "revision": e.revision, "aggregateVersion": e.aggregate_version,
                "deleted": false,
                "entry": { "key": e.key, "tenantId": e.tenant_id, "deleted": e.deleted,
                           "aggregateVersion": e.aggregate_version, "revision": e.revision }
            });
        }
        // `None`: o no existe, o es una lápida. La API no distingue —`get` devuelve `None` en los
        // dos casos, y es lo que manda la spec—; el escenario sí, y se mira con una lectura cruda.
        let subject = flux::state_subject(&contrato, &tenant, &clave).expect("subject");
        match self.ops.leer(&stream_de(&contrato), &subject).await {
            None => json!({ "entry": null, "isNull": true }),
            Some((seq, ev, _)) => json!({
                "entry": null, "isNull": true, "revision": seq,
                "aggregateVersion": ev["data"]["aggregateVersion"],
                "deleted": ev["type"].as_str().is_some_and(|t| t.contains(".deleted.")),
            }),
        }
    }

    async fn watch(&self, step: &Value) -> Value {
        let contrato = txt(step, "contract");
        let durable = txt(step, "durable");
        // El servicio sale del durable de la spec: `<servicio>__state_…`.
        let servicio = durable.split("__").next().expect("servicio").to_string();
        let bus = self.bus(&servicio).await;
        let tenant = step.get("tenant").map(|_| txt(step, "tenant"));

        let entradas: Arc<Mutex<Vec<StateEntry>>> = Arc::default();
        let sincronizado_en: Arc<Mutex<Option<i64>>> = Arc::default();

        let destino = Arc::clone(&entradas);
        let manejador = move |e: StateEntry, _d| {
            let destino = Arc::clone(&destino);
            async move {
                lock(&destino).push(e);
                Ok(())
            }
        };
        let (n, marca) = (Arc::clone(&entradas), Arc::clone(&sincronizado_en));
        let mut opts = WatchOptions::default().on_synced(move || {
            *lock(&marca) = Some(i64::try_from(lock(&n).len()).unwrap() - 1);
        });
        if let Some(t) = &tenant {
            opts = opts.with_tenant_id(t);
        }
        // `afterCreate`: escribe tras abrir y ANTES del primer pull.
        if let Some(despues) = step.get("afterCreate").and_then(Value::as_array) {
            let escrituras: Vec<(String, u64)> = despues
                .iter()
                .map(|w| (txt(w, "key"), w["version"].as_u64().expect("version")))
                .collect();
            let (dueno, c, t) = (
                self.dueno().await,
                contrato.clone(),
                tenant.clone().unwrap_or_else(|| "acme".into()),
            );
            let hook: AfterOpen = Arc::new(move || {
                let (dueno, c, t, escrituras) =
                    (dueno.clone(), c.clone(), t.clone(), escrituras.clone());
                Box::pin(async move {
                    for (clave, version) in escrituras {
                        dueno
                            .state()
                            .put_with(
                                &c,
                                &clave,
                                &json!({ "clienteId": clave, "aggregateVersion": version }),
                                con_tenant(&t),
                            )
                            .await
                            .expect("escritura de afterCreate");
                    }
                })
            });
            opts = opts.with_after_open(hook);
        }

        let vigilante = bus
            .state()
            .watch_with(&contrato, manejador, opts)
            .await
            .expect("watch");
        // El durable que deriva el SDK es el de la spec, no uno parecido.
        assert_eq!(vigilante.durable, durable, "el durable de §5.2");

        // Lo que llegue, llega: se espera a que no lleguen entradas nuevas.
        tokio::time::timeout(Duration::from_secs(10), vigilante.wait_synced())
            .await
            .expect("on_synced");
        let mut visto = usize::MAX;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(60)).await;
            let ahora = lock(&entradas).len();
            if ahora == visto {
                break;
            }
            visto = ahora;
        }
        vigilante.unsubscribe();

        let entradas: Vec<StateEntry> = lock(&entradas).clone();
        let sincronizado = lock(&sincronizado_en).unwrap_or(-2);
        json!({
            "count": entradas.len(),
            "keys": entradas.iter().map(|e| e.key.clone()).collect::<Vec<_>>(),
            "entries": entradas.iter().map(|e| json!({
                "key": e.key, "tenantId": e.tenant_id, "deleted": e.deleted,
                "data": e.data.as_ref().map(|d| serde_json::from_str::<Value>(d.get()).unwrap()),
                "aggregateVersion": e.aggregate_version, "revision": e.revision,
            })).collect::<Vec<_>>(),
            "syncedImmediately": sincronizado == -1,
            "syncedAtIndex": sincronizado,
        })
    }

    /// `instances` escritores concurrentes sobre una clave, con versiones repartidas al azar.
    async fn carrera(&self, step: &Value) -> Value {
        let bus = self.dueno().await;
        let (contrato, tenant, clave) =
            (txt(step, "contract"), txt(step, "tenant"), txt(step, "key"));
        let instancias = step["instances"].as_u64().expect("instances");
        let (desde, hasta) = (step["from"].as_u64().unwrap(), step["to"].as_u64().unwrap());
        let (ok, stale, conflicto, otro) = (
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        let mut tareas = Vec::new();
        for inst in 0..instancias {
            let (bus, contrato, tenant, clave) =
                (bus.clone(), contrato.clone(), tenant.clone(), clave.clone());
            let (ok, stale, conflicto, otro) =
                (ok.clone(), stale.clone(), conflicto.clone(), otro.clone());
            tareas.push(tokio::spawn(async move {
                let mut azar = Azar::nuevo(inst + 1);
                for v in desde..=hasta {
                    if !(v % instancias == inst || azar.unidad() < 0.3) {
                        continue;
                    }
                    tokio::time::sleep(Duration::from_micros(azar.siguiente() % 4000)).await;
                    let r = bus
                        .state()
                        .put_with(
                            &contrato,
                            &clave,
                            &json!({ "clienteId": clave, "aggregateVersion": v }),
                            con_tenant(&tenant),
                        )
                        .await;
                    let contador = match &r {
                        Ok(w) if w.applied => &ok,
                        Ok(_) => &stale,
                        Err(FluxError::StateConflict { .. }) => &conflicto,
                        Err(_) => &otro,
                    };
                    contador.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
        for t in tareas {
            t.await.expect("instancia");
        }
        // Un conflicto es RETRYABLE: la aplicación reintenta hasta que no lo haya.
        let leer = || async {
            bus.state()
                .get_with(
                    &contrato,
                    &clave,
                    GetOptions::default().with_tenant_id(&tenant),
                )
                .await
                .expect("get")
                .map(|e| e.aggregate_version)
        };
        let mut final_ = leer().await;
        for _ in 0..10 {
            if final_ == Some(hasta) {
                break;
            }
            let _ = bus
                .state()
                .put_with(
                    &contrato,
                    &clave,
                    &json!({ "clienteId": clave, "aggregateVersion": hasta }),
                    con_tenant(&tenant),
                )
                .await;
            final_ = leer().await;
        }
        json!({
            "finalVersion": final_, "ok": ok.load(Ordering::SeqCst),
            "stale": stale.load(Ordering::SeqCst), "conflict": conflicto.load(Ordering::SeqCst),
            "other": otro.load(Ordering::SeqCst),
        })
    }
}

fn stream_de(contrato: &str) -> String {
    flux::state_stream_name(contrato.split('.').next().expect("dominio"))
}

fn observa_escritura(r: &Result<StateWrite, FluxError>) -> Value {
    match r {
        Ok(w) => json!({
            "outcome": w.outcome.as_str(), "applied": w.applied, "result": w.outcome.as_str(),
            "revision": w.revision, "attempts": w.attempts, "ownWrite": w.own_write,
        }),
        Err(e) => json!({ "outcome": "error", "errCode": e.code(), "text": e.to_string() }),
    }
}

/// Lo esperado de un paso: igualdad exacta de JSON sobre la ruta indicada.
fn comprueba(caso: &str, i: usize, step: &Value, obs: &Value, fallos: &mut Vec<String>) {
    let Some(expect) = step.get("expect").and_then(Value::as_object) else {
        return;
    };
    for (ruta, esperado) in expect {
        let visto = camino(obs, ruta).cloned().unwrap_or(Value::Null);
        // `null` y «ausente» son lo mismo en el vocabulario del fichero (`isNull`).
        if &visto != esperado {
            fallos.push(format!(
                "{caso}, paso {} ({}): «{ruta}» = {visto}, se esperaba {esperado}\n      observado: {obs}",
                i + 1,
                step["title"].as_str().unwrap_or_else(|| step["op"].as_str().unwrap_or("?"))
            ));
        }
    }
}

/// Ejecuta los casos de `state-semantics.json` de una topología con un broker concreto.
async fn corre_semantica(url: &str, topologia: &str) -> Vec<String> {
    let doc: Value = serde_json::from_str(SEMANTICA).expect("state-semantics.json");
    let ops = Ops::new(url).await;
    let caso = Caso {
        url: url.to_string(),
        ops,
        buses: Mutex::default(),
    };
    let mut fallos = Vec::new();
    let mut ejecutados = 0;
    for c in doc["cases"].as_array().expect("cases") {
        if c.get("topology")
            .and_then(Value::as_str)
            .unwrap_or("single")
            != topologia
        {
            continue;
        }
        let id = c["id"].as_str().expect("id");
        caso.limpia().await;
        for (i, step) in c["steps"].as_array().expect("steps").iter().enumerate() {
            let obs = caso.paso(step).await;
            comprueba(id, i, step, &obs, &mut fallos);
        }
        ejecutados += 1;
        eprintln!("  semántica [{topologia}]: {id}");
    }
    assert!(ejecutados > 0, "no hay casos de la topología {topologia}");
    fallos
}

/// Los nueve casos de un nodo de `state-semantics.json`, con la API de Rust.
#[tokio::test]
async fn semantica_de_un_nodo() {
    let bin = broker_bin!();
    let broker = Broker::start(&bin, "");
    let fallos = corre_semantica(&broker.url(), "single").await;
    assert!(fallos.is_empty(), "\n{}", fallos.join("\n"));
}

/// El caso de cluster: bajo carrera el conflicto es el **10164**, y la clave converge igualmente
/// en la 40. Un SDK que solo reconozca el 10071 pasa el caso de un nodo y falla aquí.
#[tokio::test]
async fn semantica_de_un_cluster_de_tres_nodos() {
    let bin = broker_bin!();
    let cluster = Cluster::start(&bin);
    let fallos = corre_semantica(&cluster.url(), "cluster3").await;
    assert!(fallos.is_empty(), "\n{}", fallos.join("\n"));
}

// ═════════════════════════════════════════════════════════════════════════════
//  2. Permisos exactos
// ═════════════════════════════════════════════════════════════════════════════
//
// Lo que un servicio necesita para el estado, **medido**: la configuración de abajo está escrita a
// mano a partir de `protocol.json` → `state.acl` y es la que el SDK usa de verdad. Cada permiso se
// comprueba en los dos sentidos: con él funciona, sin él falla con un error que lo nombra.

/// Permisos de publicación (con su etiqueta, para poder quitar uno a uno) y de suscripción.
#[derive(Clone)]
struct Perms {
    publish: Vec<(&'static str, String)>,
    subscribe: Vec<String>,
}

impl Perms {
    fn sin(&self, etiqueta: &str) -> Self {
        let mut p = self.clone();
        p.publish.retain(|(e, _)| *e != etiqueta);
        if etiqueta == "inbox" {
            // Un `subscribe: { allow: [] }` NO deniega: una lista vacía es «sin restricción»
            // (A-25). Se sustituye por un inbox ajeno, que es lo que deniega de verdad.
            p.subscribe = vec!["_INBOX.otro.>".to_string()];
        }
        p
    }
}

/// **Dueño** del dominio `clientes`: escribe en `state.clientes.>` y lee lo suyo con `DIRECT.GET`
/// por subject (`ownerPublish`, `ownerDirectGet`). Sin `STREAM.INFO`: el SDK no lo usa. Los tres
/// permisos del durable `<servicio>__dlq_state_<dom>` que `protocol.json` concede al dueño sirven
/// para LEER su DLQ de estado, y este SDK no implementa ese lector: no se piden.
fn dueno(servicio: &str) -> Perms {
    Perms {
        publish: vec![
            ("publish", "state.clientes.>".to_string()),
            (
                "directget",
                format!("$JS.API.DIRECT.GET.{STREAM}.state.clientes.>"),
            ),
        ],
        subscribe: vec![format!("{}.>", flux::inbox_prefix(servicio))],
    }
}

/// **Observador** de `clientes.cliente.v1` para **un** tenant: `consumerInfo`, `consumerNext`,
/// `ack`, `observerDirectGet` acotado al tenant, `dlqPublish` y `STREAM.INFO.DLQ_<DOM>`. **Sin**
/// `STREAM.INFO.STATE_<DOM>`, que lista todas las claves y todos los tenants (A-39).
fn observador(servicio: &str, tenant: &str) -> Perms {
    let durable = flux::state_durable_name(servicio, CONTRATO, Some(tenant)).expect("durable");
    Perms {
        publish: vec![
            ("info", format!("$JS.API.CONSUMER.INFO.{STREAM}.{durable}")),
            (
                "next",
                format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{durable}"),
            ),
            ("ack", format!("$JS.ACK.{STREAM}.{durable}.>")),
            (
                "directget",
                format!("$JS.API.DIRECT.GET.{STREAM}.state.clientes.cliente.v1.{tenant}.*"),
            ),
            ("dlq", "dlq.state.clientes.cliente.v1".to_string()),
            ("dlq-stream-info", format!("$JS.API.STREAM.INFO.{DLQ}")),
        ],
        subscribe: vec![format!("{}.>", flux::inbox_prefix(servicio))],
    }
}

fn lista(a: &[String]) -> String {
    a.iter()
        .map(|x| format!("\"{x}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

/// El bloque `authorization` con `ops` (sin límites: es quien aprovisiona) y los usuarios dados.
fn autorizacion(usuarios: &[(&str, &Perms)]) -> String {
    let mut out =
        String::from("authorization {\n  users: [\n    { user: \"ops\", password: \"pw\" }\n");
    for (nombre, p) in usuarios {
        let publicar: Vec<String> = p.publish.iter().map(|(_, s)| s.clone()).collect();
        out.push_str(&format!(
            "    {{ user: \"{nombre}\", password: \"pw\", permissions: {{ publish: {{ allow: [{}] }}, \
             subscribe: {{ allow: [{}] }} }} }}\n",
            lista(&publicar),
            lista(&p.subscribe)
        ));
    }
    out.push_str("  ]\n}\n");
    out
}

/// Un broker con un dueño (`crm-api`) y un observador del tenant `acme` (`tienda-web`), y la
/// infraestructura que `flux provision` crearía.
struct Mundo {
    broker: Broker,
    ops: Ops,
    durable: String,
}

struct Aprovisionado {
    stream: bool,
    durable: bool,
}

impl Mundo {
    async fn nuevo(bin: &Path, dueno_: &Perms, observador_: &Perms, p: Aprovisionado) -> Self {
        let broker = Broker::start(
            bin,
            &autorizacion(&[("crm-api", dueno_), ("tienda-web", observador_)]),
        );
        let ops = Ops::new(&broker.url()).await;
        let durable = flux::state_durable_name("tienda-web", CONTRATO, Some("acme")).unwrap();
        if p.stream {
            ops.stream_de_estado(STREAM, "clientes", 1).await;
            ops.dlq(DLQ, "clientes").await;
            if p.durable {
                ops.durable(STREAM, &durable, "state.clientes.cliente.v1.acme.*")
                    .await;
            }
        }
        Self {
            broker,
            ops,
            durable,
        }
    }

    /// El caso normal: todo aprovisionado y todos los permisos.
    async fn completo(bin: &Path) -> Self {
        Self::nuevo(
            bin,
            &dueno("crm-api"),
            &observador("tienda-web", "acme"),
            Aprovisionado {
                stream: true,
                durable: true,
            },
        )
        .await
    }

    async fn dueno(&self) -> Bus {
        bus(&self.broker.url(), "crm-api", Some("acme")).await
    }

    async fn observador(&self) -> Bus {
        bus(&self.broker.url(), "tienda-web", None).await
    }

    async fn observador_con_log(&self) -> (Bus, Arc<Mutex<Vec<String>>>) {
        let (log, lineas) = logger_espia();
        (
            bus_con(&self.broker.url(), "tienda-web", None, |o| {
                o.with_logger(log)
            })
            .await,
            lineas,
        )
    }

    /// `ack_pending` del durable del observador, mirado por `ops`.
    async fn ack_pending(&self) -> usize {
        let s = self.ops.js.get_stream(STREAM).await.expect("stream");
        let c: consumer::PullConsumer = s.get_consumer(&self.durable).await.expect("consumidor");
        c.cached_info().num_ack_pending
    }
}

/// Un `watch` que recoge las entradas, y una cuenta de `on_synced`.
struct Observacion {
    entradas: Arc<Mutex<Vec<StateEntry>>>,
    sincronizado: Arc<AtomicUsize>,
    al_sincronizar: Arc<Mutex<Option<usize>>>,
}

impl Observacion {
    fn nueva() -> Self {
        Self {
            entradas: Arc::default(),
            sincronizado: Arc::default(),
            al_sincronizar: Arc::default(),
        }
    }

    fn opciones(&self, tenant: Option<&str>) -> WatchOptions {
        let (n, c, marca) = (
            Arc::clone(&self.entradas),
            Arc::clone(&self.sincronizado),
            Arc::clone(&self.al_sincronizar),
        );
        let mut o = WatchOptions::default().on_synced(move || {
            c.fetch_add(1, Ordering::SeqCst);
            *lock(&marca) = Some(lock(&n).len());
        });
        if let Some(t) = tenant {
            o = o.with_tenant_id(t);
        }
        o
    }

    fn manejador(
        &self,
    ) -> impl Fn(StateEntry, flux::Delivery) -> std::future::Ready<flux::HandlerResult>
           + Send
           + Sync
           + 'static {
        let destino = Arc::clone(&self.entradas);
        move |e, _d| {
            lock(&destino).push(e);
            std::future::ready(Ok(()))
        }
    }

    fn claves(&self) -> BTreeSet<String> {
        lock(&self.entradas).iter().map(|e| e.key.clone()).collect()
    }
}

/// **El criterio de aceptación de la fase** (10-state.md §10, punto 6): el dueño escribe 100
/// claves, un observador **nuevo** las recibe todas y recibe `on_synced`, se borra una, el
/// observador recibe la lápida y `get` de esa clave da `None` — con los permisos exactos y **sin
/// una sola violación** del broker: ni falta nada ni se pide nada que no esté concedido.
#[tokio::test]
async fn acl_ciclo_de_100_claves_con_permisos_exactos() {
    let bin = broker_bin!();
    let mundo = Mundo::completo(&bin).await;
    let dueno = mundo.dueno().await;

    // 1. El dueño escribe 100 claves.
    for i in 1..=100 {
        let r = dueno
            .state()
            .put(CONTRATO, &format!("cli-{i}"), &pii(i))
            .await
            .unwrap_or_else(|e| panic!("put cli-{i}: {e}"));
        assert!(r.applied && r.outcome == WriteOutcome::Ok, "{r:?}");
    }
    assert_eq!(mundo.ops.mensajes(STREAM).await, 100, "un valor por clave");

    // 2. Un observador NUEVO las recibe todas y recibe `on_synced`.
    let observador = mundo.observador().await;
    let obs = Observacion::nueva();
    let vigilante = observador
        .state()
        .watch_with(CONTRATO, obs.manejador(), obs.opciones(Some("acme")))
        .await
        .expect("watch bajo permisos exactos");
    assert_eq!(vigilante.durable, mundo.durable);
    tokio::time::timeout(Duration::from_secs(10), vigilante.wait_synced())
        .await
        .expect("on_synced");
    assert_eq!(obs.claves().len(), 100, "la foto trae las 100 claves");
    assert_eq!(
        obs.claves(),
        (1..=100)
            .map(|i| format!("cli-{i}"))
            .collect::<BTreeSet<_>>()
    );
    assert_eq!(
        obs.sincronizado.load(Ordering::SeqCst),
        1,
        "on_synced, UNA vez"
    );
    assert_eq!(
        *lock(&obs.al_sincronizar),
        Some(100),
        "cuando el handler ya había terminado con las 100"
    );
    for e in lock(&obs.entradas).iter() {
        assert_eq!(e.tenant_id, "acme");
        assert!(!e.deleted && e.data.is_some() && e.aggregate_version == 1);
    }

    // `get` de una clave viva.
    let viva = observador
        .state()
        .get_with(
            CONTRATO,
            "cli-7",
            GetOptions::default().with_tenant_id("acme"),
        )
        .await
        .expect("get bajo permisos exactos")
        .expect("la clave existe");
    assert_eq!(viva.key, "cli-7");
    let dato: Value = viva.data_as().unwrap().unwrap();
    assert_eq!(dato["email"], "maria@example.com");

    // 3. Se borra una; el observador recibe la lápida.
    let borrado = dueno
        .state()
        .delete(CONTRATO, "cli-42", 2)
        .await
        .expect("delete");
    assert!(borrado.applied);
    espera(
        || lock(&obs.entradas).iter().any(|e| e.deleted),
        Duration::from_secs(10),
        "el observador no recibió la lápida",
    )
    .await;
    let lapida = lock(&obs.entradas)
        .iter()
        .find(|e| e.deleted)
        .cloned()
        .unwrap();
    assert_eq!(
        (lapida.key.as_str(), lapida.tenant_id.as_str()),
        ("cli-42", "acme")
    );
    assert_eq!(lapida.aggregate_version, 2, "la versión de la lápida");
    assert!(lapida.data.is_none(), "la lápida no lleva dato");
    assert_eq!(lapida.revision, borrado.revision);
    assert_eq!(
        obs.sincronizado.load(Ordering::SeqCst),
        1,
        "y on_synced sigue siendo UNA vez"
    );

    // 4. `get` de esa clave da `None`, y el resto sigue.
    let tras = observador
        .state()
        .get_with(
            CONTRATO,
            "cli-42",
            GetOptions::default().with_tenant_id("acme"),
        )
        .await
        .expect("get");
    assert!(tras.is_none(), "una lápida es `None` en `get`");
    assert!(observador
        .state()
        .get_with(
            CONTRATO,
            "cli-43",
            GetOptions::default().with_tenant_id("acme")
        )
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        mundo.ops.mensajes(STREAM).await,
        100,
        "la lápida SUSTITUYE al valor"
    );

    // El dato personal ya no está en el stream.
    let (_, en_el_stream, _) = mundo
        .ops
        .leer(STREAM, "state.clientes.cliente.v1.acme.cli-42")
        .await
        .expect("la lápida");
    assert!(!en_el_stream.to_string().contains("maria@example.com"));

    // 5. Con los permisos EXACTOS no hubo ni una violación: el SDK no pide nada más (en
    //    particular `STREAM.INFO.STATE_CLIENTES`, que no está concedido).
    vigilante.unsubscribe();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let violaciones = mundo.broker.violations();
    assert!(
        violaciones.is_empty(),
        "el SDK pidió algo no concedido: {violaciones:#?}"
    );
}

/// **En los dos sentidos** (observador): cada permiso concedido es necesario, y quitarlo se nota
/// con un error que nombra el subject, no con un timeout ni —peor— en silencio.
#[tokio::test]
async fn acl_quitar_un_permiso_del_observador_se_nota() {
    let bin = broker_bin!();
    let completo = observador("tienda-web", "acme");

    // Lo que falla al ABRIR o al LEER: `PERMISSION_DENIED` nombrando el subject.
    for (quita, contiene, via_get) in [
        ("info", "CONSUMER.INFO", false),
        ("next", "CONSUMER.MSG.NEXT", false),
        ("dlq-stream-info", "STREAM.INFO.DLQ_CLIENTES", false),
        ("directget", "DIRECT.GET", true),
        ("inbox", "", true),
    ] {
        let mundo = Mundo::nuevo(
            &bin,
            &dueno("crm-api"),
            &completo.sin(quita),
            Aprovisionado {
                stream: true,
                durable: true,
            },
        )
        .await;
        mundo
            .dueno()
            .await
            .state()
            .put(CONTRATO, "cli-1", &pii(1))
            .await
            .expect("put");
        let observador = mundo.observador().await;
        let inicio = Instant::now();
        let resultado = if via_get {
            observador
                .state()
                .get_with(
                    CONTRATO,
                    "cli-1",
                    GetOptions::default().with_tenant_id("acme"),
                )
                .await
                .map(|_| ())
        } else {
            let obs = Observacion::nueva();
            observador
                .state()
                .watch_with(CONTRATO, obs.manejador(), obs.opciones(Some("acme")))
                .await
                .map(|_| ())
        };
        let err = resultado.expect_err(&format!("sin `{quita}` debería fallar"));
        assert!(
            matches!(err, FluxError::PermissionDenied { .. }),
            "sin `{quita}`: {err:?}"
        );
        assert_eq!(err.code(), Some("PERMISSION_DENIED"));
        assert_eq!(err.class(), Some(ErrorClass::Permanent));
        assert!(err.to_string().contains(contiene), "sin `{quita}`: {err}");
        // Un error, no un timeout: la denegación llega en milisegundos.
        assert!(
            inicio.elapsed() < Duration::from_secs(4),
            "sin `{quita}` tardó {:?}: parece un timeout",
            inicio.elapsed()
        );
    }
}

/// Sin permiso de ACK las entradas se entregan, se procesan y **nunca se confirman**: se
/// reentregarían para siempre sin ninguna excepción (06-security.md §3.7, A-19). No se puede
/// devolver como `Result`, pero **no** puede pasar en silencio: el `logger` recibe la denegación.
#[tokio::test]
async fn acl_sin_permiso_de_ack_se_registra_y_no_se_confirma() {
    let bin = broker_bin!();
    let mundo = Mundo::nuevo(
        &bin,
        &dueno("crm-api"),
        &observador("tienda-web", "acme").sin("ack"),
        Aprovisionado {
            stream: true,
            durable: true,
        },
    )
    .await;
    mundo
        .dueno()
        .await
        .state()
        .put(CONTRATO, "cli-1", &pii(1))
        .await
        .expect("put");

    let (observador, lineas) = mundo.observador_con_log().await;
    let obs = Observacion::nueva();
    let _w = observador
        .state()
        .watch_with(CONTRATO, obs.manejador(), obs.opciones(Some("acme")))
        .await
        .expect("sin ACK el watch ABRE: el pull y el info sí están concedidos");
    espera(
        || obs.claves().len() == 1,
        Duration::from_secs(10),
        "la entrada no llegó",
    )
    .await;

    espera(
        || {
            lock(&lineas)
                .iter()
                .any(|l| l.contains("$JS.ACK.STATE_CLIENTES."))
        },
        Duration::from_secs(5),
        "el logger no recibió la denegación del ACK",
    )
    .await;
    assert!(mundo
        .broker
        .violations()
        .iter()
        .any(|v| v.contains("$JS.ACK.")));
    assert_eq!(mundo.ack_pending().await, 1, "entregada y sin confirmar");
}

/// Sin permiso para publicar en `dlq.state.<contrato>`, lo que no se puede enrutar **no se
/// termina**: es preferible reprocesar una entrada que perderla sin rastro.
#[tokio::test]
async fn acl_sin_permiso_de_dlq_no_se_termina_lo_que_no_se_guardo() {
    let bin = broker_bin!();
    let mundo = Mundo::nuevo(
        &bin,
        &dueno("crm-api"),
        &observador("tienda-web", "acme").sin("dlq"),
        Aprovisionado {
            stream: true,
            durable: true,
        },
    )
    .await;
    mundo
        .dueno()
        .await
        .state()
        .put(CONTRATO, "cli-1", &pii(1))
        .await
        .expect("put");

    let (observador, lineas) = mundo.observador_con_log().await;
    let _w = observador
        .state()
        .watch_with(
            CONTRATO,
            |_e: StateEntry, _d| async { Err(FluxError::permanent("PAIS_NO_SOPORTADO").into()) },
            WatchOptions::default().with_tenant_id("acme"),
        )
        .await
        .expect("watch");
    espera(
        || {
            lock(&lineas)
                .iter()
                .any(|l| l.contains("no se pudo enrutar a la DLQ de estado"))
        },
        Duration::from_secs(10),
        "el SDK no dijo que no pudo enrutar a la DLQ",
    )
    .await;
    assert_eq!(mundo.ack_pending().await, 1, "no se hizo `term()`");
    assert_eq!(mundo.ops.mensajes(DLQ).await, 0);
}

/// **En los dos sentidos** (dueño): `ownerPublish` y `ownerDirectGet` son necesarios.
#[tokio::test]
async fn acl_quitar_un_permiso_del_dueno_se_nota() {
    let bin = broker_bin!();
    let completo = dueno("crm-api");
    for (quita, contiene) in [
        (
            "directget",
            "DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.cli-1",
        ),
        ("publish", "state.clientes.cliente.v1.acme.cli-1"),
        ("inbox", ""),
    ] {
        let mundo = Mundo::nuevo(
            &bin,
            &completo.sin(quita),
            &observador("tienda-web", "acme"),
            Aprovisionado {
                stream: true,
                durable: true,
            },
        )
        .await;
        let err = mundo
            .dueno()
            .await
            .state()
            .put(CONTRATO, "cli-1", &pii(1))
            .await
            .expect_err(&format!("sin `{quita}` debería fallar"));
        assert!(
            matches!(err, FluxError::PermissionDenied { .. }),
            "sin `{quita}`: {err:?}"
        );
        assert!(err.to_string().contains(contiene), "sin `{quita}`: {err}");
        assert_eq!(mundo.ops.mensajes(STREAM).await, 0, "no se escribió nada");
    }
}

/// «Solo el dueño escribe» y «un tenant no lee el de otro» **los aplica el broker**, no el SDK.
#[tokio::test]
async fn acl_el_observador_no_escribe_y_el_dueno_no_sale_de_su_dominio() {
    let bin = broker_bin!();
    let mundo = Mundo::completo(&bin).await;
    let observador = mundo.observador().await;

    // El observador puede LEER su tenant, y no puede escribir: el SDK lee primero (concedido) y
    // el broker deniega la publicación.
    let err = observador
        .state()
        .put_with(CONTRATO, "k", &pii(1), con_tenant("acme"))
        .await
        .expect_err("un observador no escribe");
    assert!(matches!(err, FluxError::PermissionDenied { .. }), "{err:?}");
    assert!(
        err.to_string().contains("state.clientes.cliente.v1.acme.k"),
        "{err}"
    );
    assert_eq!(mundo.ops.mensajes(STREAM).await, 0);

    // El dueño de `clientes` no escribe estado de otro dominio, aunque su código lo intente.
    let dueno = mundo.dueno().await;
    let err = dueno
        .state()
        .put_with(
            "pedidos.pedido.v1",
            "p-1",
            &json!({ "aggregateVersion": 1 }),
            con_tenant("acme"),
        )
        .await
        .expect_err("dominio ajeno");
    assert_eq!(err.code(), Some("PERMISSION_DENIED"), "{err:?}");
    assert!(err.to_string().contains("STATE_PEDIDOS"), "{err}");
}

/// El aislamiento por tenant está en el subject y lo aplica el **broker** (A-38): con el permiso
/// de `acme`, `get` de una clave de `globex` se deniega, y un `watch` de `globex` o sin tenant no
/// puede ni consultar su durable.
#[tokio::test]
async fn acl_un_tenant_no_lee_el_de_otro() {
    let bin = broker_bin!();
    let mundo = Mundo::completo(&bin).await;
    let dueno = mundo.dueno().await;
    dueno
        .state()
        .put_with(CONTRATO, "cli-1", &pii(1), con_tenant("acme"))
        .await
        .unwrap();
    dueno
        .state()
        .put_with(CONTRATO, "cli-1", &pii(1), con_tenant("globex"))
        .await
        .unwrap();
    let observador = mundo.observador().await;

    // Lo suyo, sí.
    assert!(observador
        .state()
        .get_with(
            CONTRATO,
            "cli-1",
            GetOptions::default().with_tenant_id("acme")
        )
        .await
        .unwrap()
        .is_some());
    // Lo de globex, no: es una denegación del broker, no un `None`.
    let err = observador
        .state()
        .get_with(
            CONTRATO,
            "cli-1",
            GetOptions::default().with_tenant_id("globex"),
        )
        .await
        .expect_err("globex");
    assert!(matches!(err, FluxError::PermissionDenied { .. }), "{err:?}");
    assert!(err.to_string().contains("globex"), "{err}");

    // Un `watch` de globex, o sin tenant: ni siquiera puede consultar un durable que no es suyo.
    for tenant in [Some("globex"), None] {
        let obs = Observacion::nueva();
        let err = observador
            .state()
            .watch_with(CONTRATO, obs.manejador(), obs.opciones(tenant))
            .await
            .expect_err("un durable ajeno");
        assert!(
            matches!(err, FluxError::PermissionDenied { .. }),
            "{tenant:?}: {err:?}"
        );
        assert!(err.to_string().contains("CONSUMER.INFO"), "{err}");
    }
}

/// `NOT_PROVISIONED` por las dos señales medidas (A-32, A-48), nombrando el recurso que falta y,
/// para el stream, **las dos causas** (no existe / existe sin `allow_direct`).
#[tokio::test]
async fn acl_no_provisioned_distingue_el_stream_y_el_durable() {
    let bin = broker_bin!();

    // Sin STATE_CLIENTES: `get`, `put` y `watch` dicen NOT_PROVISIONED del STREAM.
    let sin_stream = Mundo::nuevo(
        &bin,
        &dueno("crm-api"),
        &observador("tienda-web", "acme"),
        Aprovisionado {
            stream: false,
            durable: false,
        },
    )
    .await;
    let observador_ = sin_stream.observador().await;
    let err = observador_
        .state()
        .get_with(
            CONTRATO,
            "cli-1",
            GetOptions::default().with_tenant_id("acme"),
        )
        .await
        .expect_err("get sin stream");
    assert!(
        matches!(
            err,
            FluxError::NotProvisioned {
                kind: flux::ResourceKind::Stream,
                ..
            }
        ),
        "{err:?}"
    );
    assert_eq!(err.code(), Some("NOT_PROVISIONED"));
    let texto = err.to_string();
    assert!(texto.contains("STATE_CLIENTES"), "{texto}");
    assert!(
        texto.contains("no existe, o existe sin `allow_direct`"),
        "{texto}"
    );
    assert!(
        texto.contains("flux provision") && texto.contains("flux doctor"),
        "{texto}"
    );

    let err = sin_stream
        .dueno()
        .await
        .state()
        .put(CONTRATO, "cli-1", &pii(1))
        .await
        .expect_err("put sin stream");
    assert_eq!(err.code(), Some("NOT_PROVISIONED"), "{err:?}");
    let obs = Observacion::nueva();
    let err = observador_
        .state()
        .watch_with(CONTRATO, obs.manejador(), obs.opciones(Some("acme")))
        .await
        .expect_err("watch sin stream");
    assert!(
        matches!(
            err,
            FluxError::NotProvisioned {
                kind: flux::ResourceKind::Stream,
                ..
            }
        ),
        "{err:?}"
    );

    // Con el stream pero sin el durable, y sin permiso de crearlo: NOT_PROVISIONED del DURABLE.
    let sin_durable = Mundo::nuevo(
        &bin,
        &dueno("crm-api"),
        &observador("tienda-web", "acme"),
        Aprovisionado {
            stream: true,
            durable: false,
        },
    )
    .await;
    let obs = Observacion::nueva();
    let err = sin_durable
        .observador()
        .await
        .state()
        .watch_with(CONTRATO, obs.manejador(), obs.opciones(Some("acme")))
        .await
        .expect_err("watch sin durable");
    assert!(
        matches!(
            err,
            FluxError::NotProvisioned {
                kind: flux::ResourceKind::Consumer,
                ..
            }
        ),
        "{err:?}"
    );
    assert!(err.to_string().contains(&sin_durable.durable), "{err}");
}

/// Un durable aprovisionado que no es el que el SDK pediría —aquí `deliver_policy: all`, que
/// entregaría la historia en vez de la foto— se rechaza: el servidor devuelve lo que quiere.
#[tokio::test]
async fn acl_un_durable_aprovisionado_distinto_se_rechaza() {
    let bin = broker_bin!();
    let mundo = Mundo::nuevo(
        &bin,
        &dueno("crm-api"),
        &observador("tienda-web", "acme"),
        Aprovisionado {
            stream: true,
            durable: false,
        },
    )
    .await;
    mundo
        .ops
        .js
        .create_consumer_on_stream(
            consumer::pull::Config {
                durable_name: Some(mundo.durable.clone()),
                filter_subject: "state.clientes.cliente.v1.acme.*".to_string(),
                ack_policy: consumer::AckPolicy::Explicit,
                ack_wait: flux::DEFAULT_ACK_WAIT,
                max_deliver: i64::from(flux::DEFAULT_MAX_DELIVER),
                backoff: flux::CANONICAL_BACKOFF.to_vec(),
                max_ack_pending: i64::from(flux::DEFAULT_MAX_ACK_PENDING),
                deliver_policy: consumer::DeliverPolicy::All,
                ..Default::default()
            },
            STREAM,
        )
        .await
        .expect("durable distinto");
    let obs = Observacion::nueva();
    let err = mundo
        .observador()
        .await
        .state()
        .watch_with(CONTRATO, obs.manejador(), obs.opciones(Some("acme")))
        .await
        .expect_err("config distinta");
    let FluxError::ConsumerConfigMismatch { differences, .. } = err else {
        panic!("{err:?}")
    };
    assert!(
        differences.iter().any(|d| d.field == "deliver_policy"),
        "{differences:?}"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
//  Antes de la red
// ═════════════════════════════════════════════════════════════════════════════

/// Una clave, un tenant, una versión o un dominio inválidos fallan **antes de abrir ninguna
/// petición** (10-state.md §3.6). Se prueba con un usuario al que el broker **deniega todo**: si
/// la validación fuera después de la lectura, el error sería `PERMISSION_DENIED`, y no lo es.
/// El control, al final: la llamada válida sí toca la red, y entonces sí se deniega.
#[tokio::test]
async fn validacion_antes_de_la_red() {
    let bin = broker_bin!();
    let nada = Perms {
        publish: vec![("x", "nada.>".to_string())],
        subscribe: vec!["_INBOX.nadie.>".to_string()],
    };
    let broker = Broker::start(&bin, &autorizacion(&[("nadie", &nada)]));
    let b = bus(&broker.url(), "nadie", None).await;
    let st = b.state();

    let validos = json!({ "clienteId": "cli-1", "aggregateVersion": 1 });
    let acme = || con_tenant("acme");
    let casos: Vec<(&str, Result<StateWrite, FluxError>)> = vec![
        (
            "INVALID_STATE_KEY",
            st.put_with(CONTRATO, "a.b", &validos, acme()).await,
        ),
        (
            "INVALID_STATE_KEY",
            st.put_with(CONTRATO, "cli-1\n", &validos, acme()).await,
        ),
        (
            "INVALID_STATE_KEY",
            st.put_with(CONTRATO, "_reservada", &validos, acme()).await,
        ),
        (
            "INVALID_STATE_KEY",
            st.put_with(CONTRATO, "", &validos, acme()).await,
        ),
        (
            "INVALID_TENANT_FOR_STATE",
            st.put_with(CONTRATO, "cli-1", &validos, con_tenant("ACME"))
                .await,
        ),
        // Sin tenant en la llamada ni en `connect()`: un estado no cae en `system`.
        (
            "INVALID_TENANT_FOR_STATE",
            st.put(CONTRATO, "cli-1", &validos).await,
        ),
        (
            "MISSING_AGGREGATE_VERSION",
            st.put_with(CONTRATO, "cli-1", &json!({ "clienteId": "x" }), acme())
                .await,
        ),
        (
            "MISSING_AGGREGATE_VERSION",
            st.put_with(
                CONTRATO,
                "cli-1",
                &json!({ "aggregateVersion": 7.5 }),
                acme(),
            )
            .await,
        ),
        (
            "MISSING_AGGREGATE_VERSION",
            st.put_with(
                CONTRATO,
                "cli-1",
                &json!({ "aggregateVersion": "7" }),
                acme(),
            )
            .await,
        ),
        (
            "MISSING_AGGREGATE_VERSION",
            st.put_with(CONTRATO, "cli-1", &json!([1, 2]), acme()).await,
        ),
        (
            "MISSING_AGGREGATE_VERSION",
            st.delete_with(
                CONTRATO,
                "cli-1",
                0,
                DeleteOptions::default().with_tenant_id("acme"),
            )
            .await,
        ),
        (
            "RESERVED_DOMAIN",
            st.put_with("state.cliente.v1", "cli-1", &validos, acme())
                .await,
        ),
        (
            "RESERVED_DOMAIN",
            st.put_with("system.cliente.v1", "cli-1", &validos, acme())
                .await,
        ),
        (
            "INVALID_DATACLASSIFICATION",
            st.put_with(
                CONTRATO,
                "cli-1",
                &validos,
                acme().with_classification(DataClassification::Restricted),
            )
            .await,
        ),
    ];
    for (esperado, r) in &casos {
        assert_eq!(codigo(r).as_deref(), Some(*esperado), "{r:?}");
    }
    let acme_get = || GetOptions::default().with_tenant_id("acme");
    assert_eq!(
        codigo(&st.get_with(CONTRATO, "a.b", acme_get()).await).as_deref(),
        Some("INVALID_STATE_KEY")
    );
    assert_eq!(
        codigo(&st.get_with("dlq.cliente.v1", "k", acme_get()).await).as_deref(),
        Some("RESERVED_DOMAIN")
    );
    assert_eq!(
        codigo(
            &st.watch("rpc.cliente.v1", |_e: StateEntry, _d| async { Ok(()) })
                .await
        )
        .as_deref(),
        Some("RESERVED_DOMAIN")
    );
    assert_eq!(
        codigo(
            &st.watch_with(
                CONTRATO,
                |_e: StateEntry, _d| async { Ok(()) },
                WatchOptions::default().with_tenant_id("Acme"),
            )
            .await
        )
        .as_deref(),
        Some("INVALID_TENANT_FOR_STATE")
    );
    // Y el broker no vio NADA: ninguna violación, porque no se publicó nada.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(broker.violations().is_empty(), "{:?}", broker.violations());

    // El control: lo válido SÍ toca la red, y entonces el broker lo deniega.
    let r = st.put_with(CONTRATO, "cli-1", &validos, acme()).await;
    assert_eq!(codigo(&r).as_deref(), Some("PERMISSION_DENIED"), "{r:?}");
}

/// `publish()` y `subscribe()` rechazan los cuatro dominios reservados (10-state.md §2.7): un
/// `publish()` a `state.…` llegaría al stream de estado SIN el chequeo de secuencia (A-45).
#[tokio::test]
async fn codigos_estables_dominios_reservados_en_publish_y_subscribe() {
    let bin = broker_bin!();
    let broker = Broker::start(&bin, "");
    let b = bus_con(&broker.url(), "svc", Some("acme"), |o| o).await;
    for subject in [
        "state.cliente.v1.creado",
        "dlq.cliente.v1.creado",
        "rpc.cliente.v1.creado",
        "system.cliente.v1.creado",
        "dlq.pedidos.pedido.v1.creado",
    ] {
        let r = b.publish(subject, &json!({ "aggregateVersion": 1 })).await;
        assert_eq!(
            codigo(&r).as_deref(),
            Some("RESERVED_DOMAIN"),
            "publish {subject}: {r:?}"
        );
        let r = b
            .subscribe(subject, |_e: flux::Event, _d| async { Ok(()) })
            .await;
        assert_eq!(
            codigo(&r).as_deref(),
            Some("RESERVED_DOMAIN"),
            "subscribe {subject}: {r:?}"
        );
        assert_eq!(r.unwrap_err().class(), Some(ErrorClass::Permanent));
    }
    // Y nada llegó a ningún stream: ni siquiera se creó uno.
    let ops = Ops::new(&broker.url()).await;
    let mut nombres = ops.js.stream_names();
    assert!(
        futures::StreamExt::next(&mut nombres).await.is_none(),
        "no debería existir ningún stream"
    );

    // El control: un dominio normal sí publica.
    assert!(b
        .publish(
            "pedidos.pedido.v1.creado",
            &json!({ "pedidoId": "p", "aggregateVersion": 1 })
        )
        .await
        .is_ok());
}

/// Los seis códigos de `04-errors.md` §6, por su literal y con su clase: es lo que
/// `scripts/check-sdk-codes.mjs` exige de la evidencia de la capacidad `state`.
#[tokio::test]
async fn codigos_estables() {
    let casos: [(FluxError, &str, ErrorClass); 6] = [
        (
            flux::validate_state_tenant("ACME").unwrap_err(),
            "INVALID_TENANT_FOR_STATE",
            ErrorClass::Permanent,
        ),
        (
            flux::validate_state_key("a.b").unwrap_err(),
            "INVALID_STATE_KEY",
            ErrorClass::Permanent,
        ),
        (
            flux::aggregate_version_of(&json!({})).unwrap_err(),
            "MISSING_AGGREGATE_VERSION",
            ErrorClass::Permanent,
        ),
        (
            FluxError::StateSubjectMismatch {
                attribute: "tenantid".into(),
                envelope: "globex".into(),
                subject: "acme".into(),
            },
            "STATE_SUBJECT_MISMATCH",
            ErrorClass::Poison,
        ),
        (
            FluxError::StateConflict {
                key: "cli-1".into(),
                attempts: 3,
            },
            "STATE_CONFLICT",
            ErrorClass::Retryable,
        ),
        (
            flux::state_subject("state.cliente.v1", "acme", "k").unwrap_err(),
            "RESERVED_DOMAIN",
            ErrorClass::Permanent,
        ),
    ];
    for (err, code, class) in casos {
        assert_eq!(err.code(), Some(code), "{err:?}");
        assert_eq!(err.class(), Some(class), "{code}");
    }
    assert_eq!(
        flux::INVALID_TENANT_FOR_STATE_CODE,
        "INVALID_TENANT_FOR_STATE"
    );
    assert_eq!(flux::INVALID_STATE_KEY_CODE, "INVALID_STATE_KEY");
    assert_eq!(
        flux::MISSING_AGGREGATE_VERSION_CODE,
        "MISSING_AGGREGATE_VERSION"
    );
    assert_eq!(flux::STATE_SUBJECT_MISMATCH_CODE, "STATE_SUBJECT_MISMATCH");
    assert_eq!(flux::STATE_CONFLICT_CODE, "STATE_CONFLICT");
    assert_eq!(flux::RESERVED_DOMAIN_CODE, "RESERVED_DOMAIN");
}

// ═════════════════════════════════════════════════════════════════════════════
//  3. Comportamiento
// ═════════════════════════════════════════════════════════════════════════════

/// Un broker sin permisos con `STATE_CLIENTES` y su `DLQ_CLIENTES` ya aprovisionados.
struct Plano {
    broker: Broker,
    ops: Ops,
}

impl Plano {
    async fn nuevo(bin: &Path) -> Self {
        let broker = Broker::start(bin, "");
        let ops = Ops::new(&broker.url()).await;
        ops.stream_de_estado(STREAM, "clientes", 1).await;
        ops.dlq(DLQ, "clientes").await;
        Self { broker, ops }
    }

    async fn bus(&self, servicio: &str, tenant: Option<&str>) -> Bus {
        bus(&self.broker.url(), servicio, tenant).await
    }

    /// Los eventos de la DLQ, ya parseados, **esperando** a que haya `n`.
    async fn dlq(&self, n: u64) -> Vec<(Value, String)> {
        let inicio = Instant::now();
        while self.ops.mensajes(DLQ).await < n {
            assert!(
                inicio.elapsed() < Duration::from_secs(15),
                "la DLQ no llegó a {n} mensajes"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let mut out = Vec::new();
        for seq in 1..=n {
            let bytes = self.ops.mensaje(DLQ, seq).await.expect("mensaje de la DLQ");
            let texto = String::from_utf8(bytes).expect("UTF-8");
            out.push((serde_json::from_str(&texto).expect("JSON"), texto));
        }
        out
    }
}

/// Un mensaje de estado construido a mano, para publicarlo **saltándose el SDK** (como lo haría un
/// productor roto u hostil).
fn evento_crudo(tenant: &str, key: &str, version: u64) -> Vec<u8> {
    let id = uuid::Uuid::now_v7().to_string();
    let data = serde_json::value::to_raw_value(&json!({
        "clienteId": key, "nombre": "Ana Secreta", "email": "ana@example.com",
        "aggregateVersion": version,
    }))
    .unwrap();
    flux::serialize(
        &flux::build_state_event(flux::StateEventInput {
            contract: CONTRATO.to_string(),
            tenant_id: tenant.to_string(),
            key: key.to_string(),
            id: id.clone(),
            source: "/test/hostil".to_string(),
            time: chrono::Utc::now(),
            correlationid: id,
            producerversion: "9.9.9".to_string(),
            classification: DataClassification::Confidential,
            dataschema: flux::state_dataschema("https://schemas.internal", CONTRATO).unwrap(),
            causationid: None,
            traceparent: None,
            tracestate: None,
            data,
        })
        .expect("evento crudo"),
    )
    .expect("bytes")
}

async fn publica_crudo(ops: &Ops, subject: &str, bytes: Vec<u8>) {
    ops.js
        .publish(subject.to_string(), bytes.into())
        .await
        .expect("publicar")
        .await
        .expect("acuse");
}

/// 10-state.md §6: la entrada que un observador **no pudo aplicar** va a `dlq.state.<contrato>`
/// con `data` sustituido por `{ key, revision }`. La DLQ guarda 90 días: un dato personal que el
/// dueño ya borró seguiría vivo ahí tres meses y el derecho de supresión no se cumpliría.
#[tokio::test]
async fn dlq_una_entrada_rechazada_va_a_la_dlq_de_estado_sin_el_dato() {
    let bin = broker_bin!();
    let plano = Plano::nuevo(&bin).await;
    let dueno = plano.bus("crm-api", Some("acme")).await;
    let put = dueno.state().put(CONTRATO, "cli-1", &pii(1)).await.unwrap();

    let metricas = Arc::new(InMemoryMetrics::new());
    let en_dlq: Arc<Mutex<Vec<flux::DlqEventInfo>>> = Arc::default();
    let destino = Arc::clone(&en_dlq);
    let observador = bus_con(&plano.broker.url(), "tienda-web", None, |o| {
        o.with_metrics(metricas.clone())
            .on_dlq(Arc::new(move |i| lock(&destino).push(i)))
    })
    .await;
    let _w = observador
        .state()
        .watch_with(
            CONTRATO,
            |_e: StateEntry, _d| async {
                Err(FluxError::permanent(
                    "PAIS_NO_SOPORTADO: el cliente cli-1 tiene un país fuera del catálogo",
                )
                .with_code("PAIS_NO_SOPORTADO")
                .into())
            },
            WatchOptions::default().with_tenant_id("acme"),
        )
        .await
        .expect("watch");

    let dlq = plano.dlq(1).await;
    let (evento, texto) = &dlq[0];
    // `data` es `{ key, revision }` y NADA más.
    assert_eq!(
        evento["data"],
        json!({ "key": "cli-1", "revision": put.revision })
    );
    assert!(
        texto.ends_with(&format!(
            "\"data\":{{\"key\":\"cli-1\",\"revision\":{}}}}}",
            put.revision
        )),
        "data va el último: {texto}"
    );
    for filtrado in ["maria@example.com", "María", "Peña", "nombre", "email"] {
        assert!(
            !texto.contains(filtrado),
            "la DLQ de estado filtra `{filtrado}`: {texto}"
        );
    }
    // El resto del sobre queda como estaba, también `dataclassification` y el `id` original.
    let (_, original, _) = plano
        .ops
        .leer(STREAM, "state.clientes.cliente.v1.acme.cli-1")
        .await
        .expect("el valor sigue en el estado");
    assert_eq!(evento["id"], original["id"], "la DLQ conserva el `id`");
    assert_eq!(evento["dataclassification"], "confidential");
    assert_eq!(evento["subject"], "cli-1");
    assert_eq!(evento["tenantid"], "acme");
    assert_eq!(evento["type"], "com.flux.state.clientes.cliente.v1");
    assert_eq!(evento["dlqreason"], "permanent");
    assert_eq!(
        evento["dlqattempts"], 1,
        "un PERMANENT va a la DLQ en la primera entrega"
    );
    assert_eq!(
        evento["dlqconsumer"],
        "tienda-web__state_clientes_cliente_v1__acme"
    );
    assert!(evento["dlqerror"]
        .as_str()
        .unwrap()
        .starts_with("PAIS_NO_SOPORTADO: "));
    assert!(evento.get("signature").is_none() && evento.get("signkeyid").is_none());
    // `data` es lo último de TODAS las claves.
    assert_eq!(
        evento.as_object().unwrap().keys().next_back().unwrap(),
        "data"
    );

    // `term()`: la entrada no se reentrega, y el callback y las métricas lo cuentan.
    espera(
        || !lock(&en_dlq).is_empty(),
        Duration::from_secs(5),
        "on_dlq",
    )
    .await;
    assert_eq!(lock(&en_dlq)[0].classification.code, "PAIS_NO_SOPORTADO");
    let consumer = "tienda-web__state_clientes_cliente_v1";
    let subject = "state.clientes.cliente.v1";
    assert_eq!(
        metricas.counter(&format!(
            "flux_events_consumed_total{{subject=\"{subject}\",consumer=\"{consumer}\",outcome=\"permanent\"}}"
        )),
        Some(1)
    );
    assert_eq!(
        metricas.counter(&format!(
            "flux_events_dlq_total{{subject=\"{subject}\",consumer=\"{consumer}\",reason=\"permanent\",code=\"PAIS_NO_SOPORTADO\"}}"
        )),
        Some(1)
    );
    // Nunca el tenant (ni la clave) como etiqueta: el `consumer` es el durable SIN el sufijo.
    let salida = metricas.render();
    for prohibida in ["acme", "cli-1", "tenant"] {
        assert!(
            !salida.contains(prohibida),
            "{prohibida} en las métricas:\n{salida}"
        );
    }
}

/// Lo que miente, lo ilegible y lo que no se puede aplicar **no llega al handler** y deja la
/// clave con su valor anterior: POISON (`term()` + DLQ de estado + alerta) o PERMANENT.
#[tokio::test]
async fn dlq_poison_y_permanent_de_consumo_nunca_llegan_al_handler() {
    let bin = broker_bin!();
    let plano = Plano::nuevo(&bin).await;
    let dueno = plano.bus("crm-api", Some("acme")).await;
    dueno.state().put(CONTRATO, "cli-1", &pii(1)).await.unwrap();

    // Un evento distinto por clave: el `Nats-Msg-Id` de la DLQ es `dlq-<id>-<consumidor>`, y dos
    // mensajes con el mismo `id` se deduplicarían (por diseño: es el mismo evento).
    let valido = |clave: &str| String::from_utf8(evento_crudo("acme", clave, 1)).unwrap();
    let en_poison: Arc<Mutex<Vec<flux::PoisonInfo>>> = Arc::default();
    let destino = Arc::clone(&en_poison);
    let metricas = Arc::new(InMemoryMetrics::new());
    let observador = bus_con(&plano.broker.url(), "tienda-web", None, |o| {
        o.with_metrics(metricas.clone())
            .on_poison(Arc::new(move |i| lock(&destino).push(i)))
    })
    .await;
    let obs = Observacion::nueva();
    let w = observador
        .state()
        .watch_with(CONTRATO, obs.manejador(), obs.opciones(Some("acme")))
        .await
        .expect("watch");
    w.wait_synced().await;

    let base = "state.clientes.cliente.v1.acme";
    // 1. `tenantid` del sobre ≠ tenant del subject: STATE_SUBJECT_MISMATCH.
    publica_crudo(
        &plano.ops,
        &format!("{base}.cli-9"),
        evento_crudo("globex", "cli-9", 1),
    )
    .await;
    // 2. Basura (con algo parecido a un dato personal): ilegible. Su cuerpo NO se copia.
    publica_crudo(
        &plano.ops,
        &format!("{base}.cli-10"),
        br#"{"nombre":"Ana Secreta""#.to_vec(),
    )
    .await;
    // 3. `restricted` en estado: POISON.
    let restringido = valido("cli-11").replace("\"confidential\"", "\"restricted\"");
    publica_crudo(
        &plano.ops,
        &format!("{base}.cli-11"),
        restringido.clone().into_bytes(),
    )
    .await;
    // 4. Sin `aggregateVersion` válido: PERMANENT.
    let sin_version =
        valido("cli-12").replace("\"aggregateVersion\":1}", "\"aggregateVersion\":\"1\"}");
    publica_crudo(
        &plano.ops,
        &format!("{base}.cli-12"),
        sin_version.into_bytes(),
    )
    .await;

    let dlq = plano.dlq(4).await;
    let por_clave = |k: &str| {
        dlq.iter()
            .find(|(e, _)| e["data"]["key"] == k)
            .unwrap_or_else(|| panic!("la DLQ no tiene la clave {k}"))
    };

    // El handler SOLO vio la entrada buena.
    assert_eq!(obs.claves(), BTreeSet::from(["cli-1".to_string()]));

    let (mismatch, _) = por_clave("cli-9");
    assert_eq!(mismatch["dlqreason"], "poison");
    assert!(mismatch["dlqerror"]
        .as_str()
        .unwrap()
        .starts_with("STATE_SUBJECT_MISMATCH: "));
    // La clave de la DLQ es la del SUBJECT, y el sobre original queda como estaba (con su mentira).
    assert_eq!(mismatch["tenantid"], "globex");
    assert!(mismatch["data"].get("clienteId").is_none(), "sin el dato");

    let (ilegible, texto) = por_clave("cli-10");
    assert_eq!(ilegible["dlqreason"], "poison");
    assert!(ilegible["dlqerror"]
        .as_str()
        .unwrap()
        .starts_with("MALFORMED_JSON: "));
    assert!(
        !texto.contains("Ana Secreta"),
        "el cuerpo ilegible se copió: {texto}"
    );
    // El sobre se SINTETIZA con lo único fiable: el subject, y lo del servicio que consume.
    assert_eq!(ilegible["source"], "/test/tienda-web");
    assert_eq!(ilegible["producerversion"], "1.0.0");
    assert_eq!(ilegible["tenantid"], "acme");
    assert_eq!(ilegible["subject"], "cli-10");
    assert_eq!(ilegible["partitionkey"], "cli-10");
    assert_eq!(ilegible["dataclassification"], "internal");
    assert_eq!(ilegible["type"], "com.flux.state.clientes.cliente.v1");
    assert_eq!(ilegible["id"], ilegible["correlationid"]);
    assert_eq!(ilegible["id"].as_str().unwrap().len(), 36, "un UUID");
    assert_eq!(ilegible["dlqattempts"], 1);

    let (restringido, _) = por_clave("cli-11");
    assert_eq!(restringido["dlqreason"], "poison");
    assert!(restringido["dlqerror"]
        .as_str()
        .unwrap()
        .starts_with("INVALID_DATACLASSIFICATION: "));
    assert_eq!(restringido["dataclassification"], "restricted");

    let (permanente, _) = por_clave("cli-12");
    assert_eq!(
        permanente["dlqreason"], "permanent",
        "legible y con identidad correcta"
    );
    assert!(permanente["dlqerror"]
        .as_str()
        .unwrap()
        .starts_with("MISSING_AGGREGATE_VERSION: "));
    assert!(!permanente["dlqerror"].as_str().unwrap().contains("Ana"));

    // La alerta: tres POISON, y el callback NO recibe el cuerpo (es dato personal).
    espera(
        || lock(&en_poison).len() == 3,
        Duration::from_secs(5),
        "on_poison tres veces",
    )
    .await;
    let codigos: BTreeSet<String> = lock(&en_poison)
        .iter()
        .map(|p| p.error.code().unwrap().to_string())
        .collect();
    assert_eq!(
        codigos,
        BTreeSet::from([
            "STATE_SUBJECT_MISMATCH".to_string(),
            "MALFORMED_JSON".to_string(),
            "INVALID_DATACLASSIFICATION".to_string()
        ])
    );
    assert!(lock(&en_poison).iter().all(|p| p.raw.is_empty()));

    // Las métricas: outcome="poison" tres veces, "permanent" una.
    let (consumer, subject) = (
        "tienda-web__state_clientes_cliente_v1",
        "state.clientes.cliente.v1",
    );
    let cuenta = |o: &str| {
        metricas.counter(&format!(
            "flux_events_consumed_total{{subject=\"{subject}\",consumer=\"{consumer}\",outcome=\"{o}\"}}"
        ))
    };
    assert_eq!(cuenta("poison"), Some(3));
    assert_eq!(cuenta("permanent"), Some(1));
    assert_eq!(cuenta("ok"), Some(1), "la buena sí se aplicó");

    // `get` también las rechaza en vez de devolverlas: un mensaje que miente es un error.
    let st = observador.state();
    let acme = || GetOptions::default().with_tenant_id("acme");
    let err = st
        .get_with(CONTRATO, "cli-9", acme())
        .await
        .expect_err("miente");
    assert_eq!(err.code(), Some("STATE_SUBJECT_MISMATCH"));
    assert_eq!(err.class(), Some(ErrorClass::Poison));
    assert!(
        err.to_string().contains("globex") && err.to_string().contains("acme"),
        "{err}"
    );
    let err = st
        .get_with(CONTRATO, "cli-10", acme())
        .await
        .expect_err("ilegible");
    assert_eq!(err.class(), Some(ErrorClass::Poison), "{err:?}");
    let err = st
        .get_with(CONTRATO, "cli-12", acme())
        .await
        .expect_err("sin versión");
    assert_eq!(err.code(), Some("MISSING_AGGREGATE_VERSION"));
    assert_eq!(err.class(), Some(ErrorClass::Permanent));
}

/// Un error RETRYABLE de un handler se reintenta, y el `retry_after` acorta el **primer**
/// reintento (03-delivery.md §2.2): la entrada llega dos veces con `attempt` 1 y 2.
#[tokio::test]
async fn watch_reintenta_un_error_retryable() {
    let bin = broker_bin!();
    let plano = Plano::nuevo(&bin).await;
    plano
        .bus("crm-api", Some("acme"))
        .await
        .state()
        .put(CONTRATO, "cli-1", &pii(1))
        .await
        .unwrap();
    let intentos: Arc<Mutex<Vec<u32>>> = Arc::default();
    let destino = Arc::clone(&intentos);
    let metricas = Arc::new(InMemoryMetrics::new());
    let observador = bus_con(&plano.broker.url(), "tienda-web", None, |o| {
        o.with_metrics(metricas.clone())
    })
    .await;
    let _w = observador
        .state()
        .watch_with(
            CONTRATO,
            move |_e: StateEntry, d: flux::Delivery| {
                let destino = Arc::clone(&destino);
                async move {
                    lock(&destino).push(d.attempt);
                    if d.attempt == 1 {
                        return Err(FluxError::retryable("la base de datos no contesta")
                            .with_code("DB_DOWN")
                            .with_retry_after(Duration::from_millis(100))
                            .into());
                    }
                    Ok(())
                }
            },
            WatchOptions::default().with_tenant_id("acme"),
        )
        .await
        .expect("watch");
    espera(
        || lock(&intentos).len() == 2,
        Duration::from_secs(10),
        "no se reintentó",
    )
    .await;
    assert_eq!(*lock(&intentos), vec![1, 2]);
    espera(
        || {
            metricas
                .counter("flux_events_retried_total{subject=\"state.clientes.cliente.v1\",consumer=\"tienda-web__state_clientes_cliente_v1\",attempt=\"1\"}")
                == Some(1)
        },
        Duration::from_secs(5),
        "la métrica de reintento",
    )
    .await;
    assert_eq!(
        plano.ops.mensajes(DLQ).await,
        0,
        "un RETRYABLE que se recupera no llega a la DLQ"
    );
}

/// `flux_state_writes_total{contract,outcome}` — 08-observability.md §2.4: cuenta `ok`, `stale`,
/// `conflict` e `invalid_schema`, por contrato y **nunca** por clave ni por tenant; los errores
/// de quien llama (clave, tenant, dominio) no se cuentan.
#[tokio::test]
async fn metricas_de_escrituras_de_estado() {
    let bin = broker_bin!();
    let plano = Plano::nuevo(&bin).await;
    let metricas = Arc::new(InMemoryMetrics::new());
    let dueno = bus_con(&plano.broker.url(), "crm-api", Some("acme"), |o| {
        o.with_metrics(metricas.clone())
    })
    .await;
    let rival = plano.bus("rival", Some("acme")).await;
    let st = dueno.state();
    let cuenta = |o: &str| {
        metricas.counter(&format!(
            "flux_state_writes_total{{contract=\"clientes.cliente.v1\",outcome=\"{o}\"}}"
        ))
    };

    st.put(CONTRATO, "cli-1", &pii(1)).await.unwrap(); // ok
    st.put_with(CONTRATO, "cli-2", &pii(2), con_tenant("globex"))
        .await
        .unwrap(); // ok, otro tenant
    assert_eq!(
        st.put(CONTRATO, "cli-1", &pii(1)).await.unwrap().outcome,
        WriteOutcome::Stale
    );
    st.delete(CONTRATO, "cli-1", 2).await.unwrap(); // ok
    assert!(!st.delete(CONTRATO, "cli-1", 2).await.unwrap().applied); // stale
    assert_eq!((cuenta("ok"), cuenta("stale")), (Some(3), Some(2)));

    // `invalid_schema`: la versión inválida, y SE EMITE aunque la llamada lance.
    let r = st
        .put(CONTRATO, "cli-3", &json!({ "clienteId": "cli-3" }))
        .await;
    assert_eq!(codigo(&r).as_deref(), Some("MISSING_AGGREGATE_VERSION"));
    assert_eq!(cuenta("invalid_schema"), Some(1));

    // `conflict`: tres rivales entre la lectura y la publicación de cada intento.
    let hook = Arc::new(HookDelEscenario {
        bus: rival,
        contrato: CONTRATO.to_string(),
        tenant: "acme".to_string(),
        clave: "cli-4".to_string(),
        rivales: vec![(1, 1, 1), (2, 2, 1), (3, 3, 1)],
        pierde_el_primer_acuse: false,
    });
    let r = st
        .put_with(
            CONTRATO,
            "cli-4",
            // Una versión MAYOR que la de los rivales (1, 2 y 3): con una menor no habría conflicto
            // sino `stale`.
            &json!({ "clienteId": "cli-4", "aggregateVersion": 100 }),
            con_tenant("acme").with_hook(hook),
        )
        .await;
    assert_eq!(codigo(&r).as_deref(), Some("STATE_CONFLICT"), "{r:?}");
    assert_eq!(r.unwrap_err().class(), Some(ErrorClass::Retryable));
    assert_eq!(cuenta("conflict"), Some(1));

    // Lo que NO se cuenta.
    let antes = metricas.render();
    for r in [
        st.put(CONTRATO, "a.b", &pii(1)).await,
        st.put_with(CONTRATO, "cli-1", &pii(1), con_tenant("ACME"))
            .await,
        st.put("state.cliente.v1", "cli-1", &pii(1)).await,
    ] {
        assert!(r.is_err());
    }
    assert_eq!(
        metricas.render(),
        antes,
        "los errores de quien llama no se cuentan"
    );
    // Y ninguna etiqueta lleva la clave ni el tenant.
    for prohibida in ["cli-1", "cli-4", "tenant=", "key=", "globex"] {
        assert!(!antes.contains(prohibida), "{prohibida}:\n{antes}");
    }
}

/// El sobre de un `put`, tal cual lo guarda el stream (10-state.md §2.5): clasificación
/// `confidential` por defecto, `subject` y `partitionkey` = la clave, `Nats-Msg-Id` = `id`, y la
/// lápida `internal` con su `dataschema` fijo.
#[tokio::test]
async fn el_sobre_que_guarda_el_stream() {
    let bin = broker_bin!();
    let plano = Plano::nuevo(&bin).await;
    let dueno = plano.bus("crm-api", Some("acme")).await;
    let st = dueno.state();

    let put = st.put(CONTRATO, "cli-1", &pii(1)).await.unwrap();
    let (seq, e, msg_id) = plano
        .ops
        .leer(STREAM, "state.clientes.cliente.v1.acme.cli-1")
        .await
        .unwrap();
    assert_eq!(seq, put.revision);
    // Siempre por JetStream y con `Nats-Msg-Id` = `id` (el broker conserva las cabeceras: A-32).
    assert_eq!(msg_id, e["id"].as_str().unwrap());
    assert_eq!(e["id"].as_str().unwrap().len(), 36);
    assert_eq!(e["specversion"], "1.0");
    assert_eq!(e["source"], "/test/crm-api");
    assert_eq!(e["type"], "com.flux.state.clientes.cliente.v1");
    assert_eq!(e["datacontenttype"], "application/json");
    assert_eq!(
        e["dataschema"],
        "https://schemas.internal/state/clientes/cliente/1.0.0.json"
    );
    assert_eq!(e["subject"], "cli-1");
    assert_eq!(e["partitionkey"], "cli-1");
    assert_eq!(e["tenantid"], "acme");
    assert_eq!(e["producerversion"], "1.0.0");
    assert_eq!(
        e["correlationid"], e["id"],
        "un estado que no nace de un evento"
    );
    assert_eq!(
        e["dataclassification"], "confidential",
        "el default de un estado"
    );
    assert_eq!(e["time"].as_str().unwrap().len(), 24, "3 decimales y Z");
    assert!(e.get("causationid").is_none() && e.get("traceparent").is_none());
    assert_eq!(e.as_object().unwrap().keys().next_back().unwrap(), "data");
    assert_eq!(e["data"]["aggregateVersion"], 1);

    // Una clasificación explícita se respeta (menos `restricted`).
    st.put_with(
        CONTRATO,
        "pub-1",
        &json!({ "clienteId": "pub-1", "aggregateVersion": 1 }),
        con_tenant("acme").with_classification(DataClassification::Public),
    )
    .await
    .unwrap();
    let (_, p, _) = plano
        .ops
        .leer(STREAM, "state.clientes.cliente.v1.acme.pub-1")
        .await
        .unwrap();
    assert_eq!(p["dataclassification"], "public");

    // La lápida.
    let borrado = st.delete(CONTRATO, "cli-1", 2).await.unwrap();
    let (seq, l, _) = plano
        .ops
        .leer(STREAM, "state.clientes.cliente.v1.acme.cli-1")
        .await
        .unwrap();
    assert_eq!(seq, borrado.revision);
    assert_eq!(l["type"], "com.flux.state.clientes.cliente.deleted.v1");
    assert_eq!(
        l["dataschema"],
        "https://schemas.internal/system/state/eliminado/1.0.0.json"
    );
    assert_eq!(l["dataclassification"], "internal");
    assert_eq!(l["data"], json!({ "aggregateVersion": 2 }));
    assert_eq!(l["subject"], "cli-1");
    // Borrar una clave que NUNCA existió escribe la lápida.
    assert!(st.delete(CONTRATO, "fantasma", 1).await.unwrap().applied);
    assert!(plano
        .ops
        .leer(STREAM, "state.clientes.cliente.v1.acme.fantasma")
        .await
        .is_some());
    // Un valor por clave: pub-1, cli-1 (lápida) y fantasma.
    assert_eq!(plano.ops.mensajes(STREAM).await, 3);
}

/// El tenant de un estado derivado dentro de un handler es el **del evento que lo causó** (§2.4):
/// el contexto del evento en curso gana sobre el de `connect()`, y se hereda la correlación.
#[tokio::test]
async fn contexto_un_estado_derivado_hereda_el_tenant_y_la_correlacion() {
    let bin = broker_bin!();
    let plano = Plano::nuevo(&bin).await;
    let sin_tenant = plano.bus("crm-api", None).await;
    let con_tenant_de_conexion = plano.bus("otro-crm", Some("acme")).await;

    // Sin tenant en ninguna parte: un estado no cae en `system`.
    let r = sin_tenant.state().put(CONTRATO, "cli-1", &pii(1)).await;
    assert_eq!(
        codigo(&r).as_deref(),
        Some("INVALID_TENANT_FOR_STATE"),
        "{r:?}"
    );

    let ctx = flux::EventContext {
        correlation_id: "corr-1".to_string(),
        causation_id: "ev-1".to_string(),
        tenant_id: "globex".to_string(),
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
        tracestate: None,
    };
    // Dentro del evento en curso, el tenant sale de él…
    flux::context::scope(Some(ctx.clone()), async {
        sin_tenant
            .state()
            .put(CONTRATO, "cli-1", &pii(1))
            .await
            .unwrap();
        // …y GANA sobre el de la conexión (`acme`).
        con_tenant_de_conexion
            .state()
            .put(CONTRATO, "cli-2", &pii(2))
            .await
            .unwrap();
    })
    .await;
    let (_, e, _) = plano
        .ops
        .leer(STREAM, "state.clientes.cliente.v1.globex.cli-1")
        .await
        .unwrap();
    assert_eq!(e["tenantid"], "globex");
    assert_eq!(e["correlationid"], "corr-1");
    assert_eq!(e["causationid"], "ev-1");
    assert_eq!(
        e["traceparent"],
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    );
    assert!(plano
        .ops
        .leer(STREAM, "state.clientes.cliente.v1.globex.cli-2")
        .await
        .is_some());
    assert!(plano
        .ops
        .leer(STREAM, "state.clientes.cliente.v1.acme.cli-2")
        .await
        .is_none());

    // La opción de la llamada gana sobre las dos.
    flux::context::scope(Some(ctx), async {
        con_tenant_de_conexion
            .state()
            .put_with(CONTRATO, "cli-3", &pii(3), con_tenant("initech"))
            .await
            .unwrap();
    })
    .await;
    assert!(plano
        .ops
        .leer(STREAM, "state.clientes.cliente.v1.initech.cli-3")
        .await
        .is_some());
}

/// 09-multitenancy.md §3 en estado: con `strict`, un `watch` sin tenant es un error de
/// configuración y falla antes de la red; con tenant en `connect()` **siempre** filtra, también
/// con `system`, que aquí es un tenant como cualquier otro (decisión provisional 9).
#[tokio::test]
async fn watch_con_strict_exige_tenant_y_system_cuenta() {
    let bin = broker_bin!();
    let plano = Plano::nuevo(&bin).await;
    let estricto = bus_con(&plano.broker.url(), "tienda-web", None, |o| {
        o.with_tenant_isolation(TenantIsolation::Strict)
    })
    .await;
    let obs = Observacion::nueva();
    let err = estricto
        .state()
        .watch_with(CONTRATO, obs.manejador(), obs.opciones(None))
        .await
        .expect_err("strict sin tenant");
    assert!(matches!(err, FluxError::TenantIsolation { .. }), "{err:?}");
    assert!(
        err.to_string().contains("state.clientes.cliente.v1"),
        "{err}"
    );
    // Falló ANTES de la red: no se creó ningún durable.
    assert_eq!(
        plano
            .ops
            .js
            .get_stream(STREAM)
            .await
            .unwrap()
            .cached_info()
            .state
            .consumer_count,
        0
    );

    // Con tenant, sí: y `system` cuenta (no es «la ausencia de tenant» como en los eventos).
    let con_system = bus_con(&plano.broker.url(), "tienda-web", Some("system"), |o| {
        o.with_tenant_isolation(TenantIsolation::Strict)
    })
    .await;
    let obs = Observacion::nueva();
    let w = con_system
        .state()
        .watch_with(CONTRATO, obs.manejador(), obs.opciones(None))
        .await
        .expect("`system` es un tenant");
    assert_eq!(w.tenant_id.as_deref(), Some("system"));
    assert_eq!(w.durable, "tienda-web__state_clientes_cliente_v1__system");
    w.unsubscribe();
}

/// Dos `watch` del mismo contrato para dos tenants, en un mismo servicio: cada uno ve solo lo suyo
/// (el tenant va en el durable: uno compartido **repartiría** los mensajes, A-47), y las métricas
/// de consumo usan el durable **sin** sufijo —ni un tenant en una etiqueta— con el gauge de
/// pendientes como la suma de los dos.
#[tokio::test]
async fn watch_de_dos_tenants_no_se_pisan_y_las_metricas_no_llevan_el_tenant() {
    let bin = broker_bin!();
    let plano = Plano::nuevo(&bin).await;
    let dueno = plano.bus("crm-api", None).await;
    for (t, claves) in [("acme", [1, 2, 3]), ("globex", [1, 2, 9])] {
        for i in claves {
            dueno
                .state()
                .put_with(CONTRATO, &format!("cli-{i}"), &pii(i), con_tenant(t))
                .await
                .unwrap();
        }
    }
    let metricas = Arc::new(InMemoryMetrics::new());
    let observador = bus_con(&plano.broker.url(), "tienda-web", None, |o| {
        o.with_metrics(metricas.clone())
    })
    .await;
    let (a, g) = (Observacion::nueva(), Observacion::nueva());
    let wa = observador
        .state()
        .watch_with(CONTRATO, a.manejador(), a.opciones(Some("acme")))
        .await
        .unwrap();
    let wg = observador
        .state()
        .watch_with(CONTRATO, g.manejador(), g.opciones(Some("globex")))
        .await
        .unwrap();
    assert_ne!(wa.durable, wg.durable);
    wa.wait_synced().await;
    wg.wait_synced().await;
    assert_eq!(
        a.claves(),
        BTreeSet::from(["cli-1".into(), "cli-2".into(), "cli-3".into()])
    );
    assert_eq!(
        g.claves(),
        BTreeSet::from(["cli-1".into(), "cli-2".into(), "cli-9".into()])
    );
    // `cli-1` llegó a los dos con SU tenant: la copia local no los mezcla.
    assert!(lock(&a.entradas).iter().all(|e| e.tenant_id == "acme"));
    assert!(lock(&g.entradas).iter().all(|e| e.tenant_id == "globex"));

    let gauge = "flux_consumer_pending{subject=\"state.clientes.cliente.v1\",consumer=\"tienda-web__state_clientes_cliente_v1\"}";
    assert_eq!(
        metricas.gauge(gauge),
        Some(0.0),
        "la suma de los dos durables"
    );
    let salida = metricas.render();
    assert!(
        !salida.contains("acme") && !salida.contains("globex"),
        "{salida}"
    );
    wa.unsubscribe();
    wg.unsubscribe();
}

/// `Bus::close()` detiene los `watch` y quien espera `wait_synced` no se queda colgado.
#[tokio::test]
async fn close_detiene_los_watch() {
    let bin = broker_bin!();
    let plano = Plano::nuevo(&bin).await;
    let observador = plano.bus("tienda-web", None).await;
    let obs = Observacion::nueva();
    let w = observador
        .state()
        .watch_with(CONTRATO, obs.manejador(), obs.opciones(Some("acme")))
        .await
        .unwrap();
    // Vacío: sincronizado de inmediato, antes de entregar nada.
    assert!(w.is_synced());
    assert_eq!(*lock(&obs.al_sincronizar), Some(0));
    observador.close().await.expect("close");
    tokio::time::timeout(Duration::from_secs(5), w.wait_synced())
        .await
        .expect("wait_synced no debe colgarse");
}

/// L3 en `put` (detrás de la feature `validation`): `data` contra el esquema del contrato, que
/// resuelve el índice `state` del **bundle** —sin URL base y sin red—, y la lápida contra su
/// esquema fijo. Falla **antes** de leer ni escribir, reporta todos los errores y **cita la ruta
/// del campo, no su valor** (10-state.md §3.6).
#[cfg(feature = "validation")]
#[tokio::test]
async fn validacion_l3_en_put_y_en_la_lapida() {
    use flux::validation::{SchemaBundle, ValidationOptions};

    let bin = broker_bin!();
    let plano = Plano::nuevo(&bin).await;
    let bundle = || SchemaBundle::from_json(include_str!("../../schemas/bundle.json")).unwrap();
    let metricas = Arc::new(InMemoryMetrics::new());
    // SIN `with_schema_base_url`: el dataschema sale del índice `state` del bundle.
    let dueno = connect(
        ConnectOptions::new(plano.broker.url(), "crm-api", "test", "1.0.0")
            .with_tenant_id("acme")
            .with_metrics(metricas.clone())
            .with_validation(ValidationOptions::strict(bundle())),
    )
    .await
    .expect("conexión");
    let st = dueno.state();

    // Válido: pasa, y el `dataschema` es el del índice del bundle.
    st.put(
        CONTRATO,
        "cli-1",
        &json!({ "clienteId": "cli-1", "nombre": "Ana", "email": "ana@example.com", "aggregateVersion": 1 }),
    )
    .await
    .expect("válido");
    let (_, e, _) = plano
        .ops
        .leer(STREAM, "state.clientes.cliente.v1.acme.cli-1")
        .await
        .unwrap();
    assert_eq!(
        e["dataschema"],
        "https://schemas.internal/state/clientes/cliente/1.0.0.json"
    );

    // Inválido: falta `nombre`, el email no es un email y `pais` no es ISO. TODOS los errores, y
    // ningún valor.
    let antes = plano.ops.mensajes(STREAM).await;
    let err = st
        .put(
            CONTRATO,
            "cli-2",
            &json!({ "clienteId": "cli-2", "email": "esto-no-es-un-email", "pais": "españa", "aggregateVersion": 1 }),
        )
        .await
        .expect_err("inválido");
    assert_eq!(err.code(), Some("INVALID_SCHEMA"), "{err:?}");
    assert_eq!(err.class(), Some(ErrorClass::Permanent));
    let texto = err.to_string();
    assert!(texto.contains("`nombre`"), "{texto}");
    assert!(texto.contains("/email"), "{texto}");
    assert!(texto.contains("/pais"), "{texto}");
    for valor in ["esto-no-es-un-email", "españa"] {
        assert!(
            !texto.contains(valor),
            "el mensaje filtra un valor: {texto}"
        );
    }
    assert_eq!(
        plano.ops.mensajes(STREAM).await,
        antes,
        "falla ANTES de escribir"
    );
    assert_eq!(
        metricas.counter(
            "flux_state_writes_total{contract=\"clientes.cliente.v1\",outcome=\"invalid_schema\"}"
        ),
        Some(1)
    );

    // La lápida valida contra el esquema fijo, y pasa.
    assert!(st.delete(CONTRATO, "cli-1", 2).await.unwrap().applied);

    // Un servicio desplegado con un bundle SIN el esquema de la lápida no puede borrar: es
    // SCHEMA_NOT_FOUND, no un borrado sin validar.
    let mut sin_lapida = bundle();
    sin_lapida
        .schemas
        .remove("https://schemas.internal/system/state/eliminado/1.0.0.json");
    let viejo = connect(
        ConnectOptions::new(plano.broker.url(), "crm-viejo", "test", "1.0.0")
            .with_tenant_id("acme")
            .with_validation(ValidationOptions::strict(sin_lapida)),
    )
    .await
    .expect("conexión");
    let r = viejo.state().delete(CONTRATO, "cli-1", 3).await;
    assert_eq!(codigo(&r).as_deref(), Some("SCHEMA_NOT_FOUND"), "{r:?}");
}

/// La firma (07-signing.md) también vale en estado: «la misma firma» (10-state.md §2.5). Un `put`
/// firmado se guarda con su `signkeyid` y su firma, un observador en modo `require` lo verifica, y
/// un mensaje **sin** firma es POISON — y la DLQ de estado no conserva la firma, que firmaría un
/// `data` que ya no está.
#[cfg(feature = "signing")]
#[tokio::test]
async fn firma_en_estado_se_firma_y_se_verifica() {
    let bin = broker_bin!();
    let plano = Plano::nuevo(&bin).await;
    let (privada, publica) = flux::generate_key_pair();
    let dueno = bus_con(&plano.broker.url(), "crm-api", Some("acme"), |o| {
        o.with_signing(flux::SigningOptions::default().with_private_key(&privada, "crm-api-1"))
    })
    .await;
    dueno.state().put(CONTRATO, "cli-1", &pii(1)).await.unwrap();
    let (_, e, _) = plano
        .ops
        .leer(STREAM, "state.clientes.cliente.v1.acme.cli-1")
        .await
        .unwrap();
    assert_eq!(e["signkeyid"], "crm-api-1");
    assert!(e["signature"].as_str().is_some_and(|s| !s.is_empty()));

    let observador = bus_con(&plano.broker.url(), "tienda-web", None, |o| {
        o.with_signing(
            flux::SigningOptions::default()
                .with_public_key("crm-api-1", &publica)
                .with_verify(flux::VerificationMode::Require),
        )
    })
    .await;
    let obs = Observacion::nueva();
    let w = observador
        .state()
        .watch_with(CONTRATO, obs.manejador(), obs.opciones(Some("acme")))
        .await
        .unwrap();
    w.wait_synced().await;
    assert_eq!(
        obs.claves(),
        BTreeSet::from(["cli-1".to_string()]),
        "la firmada verifica"
    );

    // Sin firma: POISON, y no llega al handler.
    publica_crudo(
        &plano.ops,
        "state.clientes.cliente.v1.acme.cli-9",
        evento_crudo("acme", "cli-9", 1),
    )
    .await;
    let dlq = plano.dlq(1).await;
    assert_eq!(dlq[0].0["dlqreason"], "poison");
    assert!(dlq[0].0["dlqerror"]
        .as_str()
        .unwrap()
        .starts_with("MISSING_SIGNATURE: "));
    assert!(dlq[0].0.get("signature").is_none());
    assert_eq!(obs.claves().len(), 1);
}

// ═════════════════════════════════════════════════════════════════════════════
//  Los permisos de arriba, contra `protocol.json` → `state.acl`
// ═════════════════════════════════════════════════════════════════════════════

/// Cada permiso de la configuración de broker de este fichero —la que el SDK usa de verdad— encaja
/// en un patrón de `protocol.json` → `state.acl` (o en el de `security.acl` para la DLQ y el
/// inbox): lo que el SDK necesita **no** excede lo que la spec concede, ni lo que el generador de
/// permisos debe producir. Y al revés: lo que la spec **no** concede (`STREAM.INFO.STATE_<DOM>`,
/// la forma de cuerpo de `DIRECT.GET`) no lo pide el SDK.
#[test]
fn los_permisos_del_sdk_son_los_de_protocol_json_state_acl() {
    let protocolo: Value = serde_json::from_str(include_str!("../../protocol.json")).unwrap();
    let acl = &protocolo["state"]["acl"];
    let patron = |v: &Value| regex::Regex::new(v.as_str().expect("patrón")).expect("regex");
    let encaja = |re: &regex::Regex, s: &str| re.is_match(s);

    let [dueno_publicar, dueno_leer] = ["ownerPublish", "ownerDirectGet"].map(|k| patron(&acl[k]));
    let (info, siguiente, ack, directo, dlq) = (
        patron(&acl["consumerInfo"]),
        patron(&acl["consumerNext"]),
        patron(&acl["ack"]),
        patron(&acl["observerDirectGet"]),
        patron(&acl["dlqPublish"]),
    );
    let stream_info = patron(&protocolo["security"]["acl"]["permissionPatterns"]["streamInfo"]);
    let inbox = patron(&protocolo["security"]["acl"]["inbox"]["permissionPattern"]);

    let d = dueno("crm-api");
    let permiso = |p: &Perms, e: &str| {
        p.publish
            .iter()
            .find(|(x, _)| *x == e)
            .unwrap_or_else(|| panic!("falta {e}"))
            .1
            .clone()
    };
    assert!(
        encaja(&dueno_publicar, &permiso(&d, "publish")),
        "{}",
        permiso(&d, "publish")
    );
    assert!(
        encaja(&dueno_leer, &permiso(&d, "directget")),
        "{}",
        permiso(&d, "directget")
    );
    assert!(encaja(&inbox, &d.subscribe[0]), "{}", d.subscribe[0]);

    let o = observador("tienda-web", "acme");
    for (etiqueta, re) in [
        ("info", &info),
        ("next", &siguiente),
        ("ack", &ack),
        ("directget", &directo),
        ("dlq", &dlq),
        ("dlq-stream-info", &stream_info),
    ] {
        let p = permiso(&o, etiqueta);
        assert!(encaja(re, &p), "`{etiqueta}`: {p}");
    }
    assert!(encaja(&inbox, &o.subscribe[0]), "{}", o.subscribe[0]);

    // Lo que NO se concede a un observador: `STREAM.INFO.STATE_<DOM>` (lista todas las claves y
    // todos los tenants, A-39) y `DIRECT.GET` en forma de cuerpo o por encima del tenant.
    assert!(!encaja(&stream_info, "$JS.API.STREAM.INFO.STATE_CLIENTES"));
    assert!(!encaja(&directo, "$JS.API.DIRECT.GET.STATE_CLIENTES"));
    assert!(!encaja(
        &directo,
        "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.>"
    ));

    // Los patrones de crear un durable son SOLO de desarrollo, y el SDK en producción no los usa
    // (`acl_ciclo_de_100_claves_con_permisos_exactos` no los concede y no hay ninguna violación).
    assert_eq!(
        acl["devOnlyPatterns"],
        json!(["consumerCreate", "dlqReaderCreate"])
    );
    assert_eq!(acl["observerStreamInfoState"], "never");
}

impl std::fmt::Debug for Perms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Perms")
            .field("publish", &self.publish)
            .field("subscribe", &self.subscribe)
            .finish()
    }
}
