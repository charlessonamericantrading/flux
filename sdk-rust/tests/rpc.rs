//! Petición/respuesta ejecutada contra un `nats-server` REAL — plan v1.1, Fase 3.
//!
//! `specification/11-request-reply.md` es el contrato; este fichero lo ejecuta con la API de Rust
//! (`Bus::request` y `Bus::respond`), con tres entradas distintas:
//!
//! 1. **Permisos exactos** (`acl_*`): la ACL de `conformance/cases/rpc-semantics.json` → `acls.rpc`
//!    —`llama` (quien llama), `crm` (el dueño de `clientes`) y sus variantes— se carga **como datos** en el
//!    `nats-server`, y el ciclo de aceptación de la fase se ejecuta con ella: **una operación atendida
//!    por dos instancias reparte las peticiones; al parar ambas, quien llama recibe `RPC_UNAVAILABLE`
//!    en menos de 100 ms; y quitar un permiso se nota** (§12.9).
//! 2. **Lo que solo se ve contra un broker** (`broker_*`): la petición denegada que `async-nats` NO
//!    entrega como error (A-72), el dueño sin permiso de suscripción (A-73), el acuse de un stream que
//!    captura `rpc.>` (A-67), la respuesta que no cabe (A-65), la parada ordenada (A-62).
//! 3. **Comportamiento** (`algoritmo_*`, `metricas_*`…): lo que el SDK promete y que ningún otro test
//!    puede ver, con respondedores *en bruto* que fabrican respuestas falsas.
//!
//! ```text
//! NATS_SERVER=/ruta/a/nats-server FLUX_REQUIRE_BROKER=1 cargo test --all-features --test rpc
//! ```
//!
//! Sin binario, los tests se **saltan con un aviso**… salvo que `FLUX_REQUIRE_BROKER=1`: en CI eso los
//! convierte en un fallo. Un fixture que nadie ejecuta aparenta cobertura.
//!
//! Los puertos salen de `FLUX_TEST_PORTS=desde-hasta` (rango propio de este fichero) o, si no está, de
//! `FLUX_TEST_PORT_BASE` (default 4750) + 50. Se usan hasta 6 a la vez (el cluster de tres nodos).
//!
//! Los seis códigos de la capacidad —`RPC_UNAVAILABLE`, `RPC_TIMEOUT`, `STALE_REQUEST`,
//! `RPC_INVALID_REPLY`, `RPC_MESSAGE_TOO_LARGE` e `INVALID_RPC_CALL`— más `RESERVED_DOMAIN` y
//! `PERMISSION_DENIED` se comprueban por su literal en `codigos_estables`, que es lo que
//! `scripts/check-sdk-codes.mjs` exige de la evidencia de la capacidad.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flux::{
    connect, Bus, ConnectOptions, Credentials, DataClassification, ErrorClass, FluxError,
    InMemoryMetrics, LogFn, LogLevel, RequestOptions, RespondOptions, RpcContext, RpcRequest,
};
use futures::StreamExt;
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
         (o ponlo en el PATH). Sin esto los tests de petición/respuesta no se ejecutan, y un fixture \
         que nadie ejecuta aparenta cobertura."
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

/// El rango de puertos propio: `FLUX_TEST_PORTS=desde-hasta`, o `FLUX_TEST_PORT_BASE` + 50.
fn port_range() -> (u16, u16) {
    if let Some(rango) = std::env::var("FLUX_TEST_PORTS")
        .ok()
        .filter(|v| !v.is_empty())
    {
        let (desde, hasta) = rango
            .split_once('-')
            .unwrap_or_else(|| panic!("FLUX_TEST_PORTS debe ser `desde-hasta`, y es `{rango}`"));
        return (
            desde
                .trim()
                .parse()
                .expect("FLUX_TEST_PORTS: puerto inicial"),
            hasta.trim().parse().expect("FLUX_TEST_PORTS: puerto final"),
        );
    }
    let base: u16 = std::env::var("FLUX_TEST_PORT_BASE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4750);
    (base, base + 49)
}

/// Un puerto libre del rango propio, comprobado con un `bind` real: un servidor anterior que no
/// terminó de morir no debe romper este.
fn free_port() -> u16 {
    let (desde, hasta) = port_range();
    let span = hasta - desde + 1;
    for _ in 0..span {
        let port = desde + NEXT_PORT.fetch_add(1, Ordering::SeqCst) % span;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("no queda ningún puerto libre en {desde}..={hasta}");
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

/// La ACL de `conformance/cases/rpc-semantics.json` → `acls.rpc`, como **datos**: lo que ejecuta el
/// runner de la suite de conformidad, cargado aquí en el `nats-server` de cada test.
fn acl_rpc() -> Vec<Value> {
    let texto = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/cases/rpc-semantics.json"),
    )
    .expect("rpc-semantics.json");
    let doc: Value = serde_json::from_str(&texto).expect("JSON válido");
    doc["acls"]["rpc"]["users"]
        .as_array()
        .expect("acls.rpc.users")
        .clone()
}

/// El bloque `permissions { … }` de un usuario de `acls.rpc`, en la sintaxis de `nats-server`.
fn permisos_de(usuario: &Value) -> String {
    let lista = |k: &str| -> String {
        usuario[k]
            .as_array()
            .map(|v| {
                v.iter()
                    .map(|s| format!("\"{}\"", s.as_str().unwrap()))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default()
    };
    let responses = match &usuario["allowResponses"] {
        Value::Null => String::new(),
        Value::Bool(true) => "allow_responses: true".to_string(),
        Value::Object(o) => {
            let mut campos = Vec::new();
            if let Some(max) = o.get("max") {
                campos.push(format!("max: {max}"));
            }
            if let Some(expires) = o.get("expires") {
                campos.push(format!("expires: \"{}\"", expires.as_str().unwrap()));
            }
            format!("allow_responses: {{ {} }}", campos.join(", "))
        }
        otro => panic!("allowResponses inesperado: {otro}"),
    };
    format!(
        "permissions: {{ publish: [{}], subscribe: [{}], {responses} }}",
        lista("publish"),
        lista("subscribe")
    )
}

/// Usuarios que `acls.rpc` no trae y que algunos tests necesitan: un servicio **intermedio** que a la vez
/// atiende una operación de otro dominio y llama a una del de `crm` (la cadena `llamador → intermedio →
/// crm`), y quien llama a la del intermedio.
const USUARIOS_EXTRA: &str = r#"
  { user: "llamador", password: "pw", permissions: {
      publish: ["rpc.pedidos.pedido.v1.cotizar"], subscribe: ["_INBOX.llamador.>"] } },
  { user: "intermedio", password: "pw", permissions: {
      publish: ["rpc.clientes.cliente.v1.obtener"],
      subscribe: ["_INBOX.intermedio.>", "rpc.pedidos.>"], allow_responses: true } },"#;

/// El bloque `authorization` completo: los usuarios de `acls.rpc` —con el permiso que se **quite** si se
/// pide— y `ops`, sin restricciones, para armar y vigilar lo que el SDK no debe poder hacer.
fn authorization(quitar: Option<(&str, &str)>) -> String {
    let mut usuarios: Vec<String> = acl_rpc()
        .iter()
        .map(|u| {
            let mut u = u.clone();
            if let Some((nombre, permiso)) = quitar {
                if u["name"] == nombre {
                    match permiso {
                        "publish" | "subscribe" => u[permiso] = json!([]),
                        "allowResponses" => {
                            u.as_object_mut().unwrap().remove("allowResponses");
                        }
                        otro => panic!("no sé quitar `{otro}`"),
                    }
                }
            }
            // Una lista VACÍA no deniega (A-25): sin concesión, el usuario queda sin esa lista y el
            // servidor lo trata como «sin restricción». Se sustituye por un subject que nadie usa.
            let permisos = permisos_de(&u).replace("[]", "[\"nadie.usa.esto\"]");
            format!(
                "{{ user: \"{}\", password: \"pw\", {permisos} }}",
                u["name"].as_str().unwrap()
            )
        })
        .collect();
    usuarios.push("{ user: \"ops\", password: \"pw\" }".to_string());
    format!(
        "authorization {{ users = [{USUARIOS_EXTRA}\n  {}\n] }}",
        usuarios.join(",\n  ")
    )
}

/// Un `nats-server` con JetStream y la ACL de `acls.rpc`, su configuración y su directorio temporal. Al
/// soltarlo mata SOLO su proceso y borra su almacén.
struct Broker {
    child: Child,
    port: u16,
    dir: PathBuf,
}

impl Broker {
    fn start(bin: &Path, auth: &str) -> Self {
        let port = free_port();
        let dir = temp_dir("rpc");
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

    /// El broker de siempre: la ACL de `acls.rpc` tal cual.
    fn con_acl(bin: &Path) -> Self {
        Self::start(bin, &authorization(None))
    }

    /// Con un permiso quitado a un usuario: «quitar un permiso se nota».
    fn sin_permiso(bin: &Path, usuario: &str, permiso: &str) -> Self {
        Self::start(bin, &authorization(Some((usuario, permiso))))
    }

    fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }

    /// Las violaciones de permisos que el servidor registró. Formato observado en nats-server 2.14.5:
    /// `Publish Violation - Subject "…"` y `Subscription Violation - Subject "…"`.
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

// ═════════════════════════════════════════════════════════════════════════════
//  Utilidades
// ═════════════════════════════════════════════════════════════════════════════

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Espera a que `cond` se cumpla, o falla con `msg`.
async fn espera<F: Fn() -> bool>(cond: F, hasta: Duration, msg: &str) {
    let deadline = Instant::now() + hasta;
    while !cond() {
        assert!(Instant::now() < deadline, "{msg}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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

const BASE: &str = "https://schemas.internal";
const OBTENER: &str = "clientes.cliente.v1.obtener";
const DESCUENTO: &str = "clientes.cliente.v1.calcular-descuento";

/// Una conexión del SDK. El usuario del broker es el nombre del servicio: `_INBOX.<servicio>.>` de la ACL.
async fn bus(url: &str, servicio: &str) -> Bus {
    bus_con(url, servicio, |o| o).await
}

async fn bus_con(
    url: &str,
    servicio: &str,
    ajusta: impl FnOnce(ConnectOptions) -> ConnectOptions,
) -> Bus {
    let opts = ConnectOptions::new(url, servicio, "produccion", "1.0.0")
        .with_credentials(Credentials::UserPassword {
            user: servicio.to_string(),
            password: "pw".to_string(),
        })
        .with_schema_base_url(BASE);
    connect(ajusta(opts))
        .await
        .unwrap_or_else(|e| panic!("connect({servicio}): {e}"))
}

/// El dueño de `clientes` con un handler fijo.
async fn dueno_responde(bus: &Bus, operacion: &str, data: Value) -> flux::Responder {
    bus.respond(operacion, move |_: RpcRequest, _: RpcContext| {
        let data = data.clone();
        async move { Ok::<Value, flux::HandlerError>(data) }
    })
    .await
    .unwrap_or_else(|e| panic!("respond({operacion}): {e}"))
}

fn code(e: &FluxError) -> String {
    e.code().unwrap_or("<sin código>").to_string()
}

/// Una conexión **en bruto** de `async-nats`, sin SDK, con el usuario `ops` (sin restricciones): para
/// fabricar lo que el SDK no produce —respuestas falsas, peticiones rotas— y para vigilar el cable.
async fn bruto(url: &str) -> async_nats::Client {
    async_nats::ConnectOptions::with_user_and_password("ops".into(), "pw".into())
        .connect(url)
        .await
        .expect("conexión en bruto")
}

/// Un viaje de ida y vuelta REAL al servidor. El `flush()` de `async-nats` solo vacía el búfer de
/// escritura, no espera a que el servidor haya procesado lo enviado: una petición a un subject sin nadie
/// contesta `no responders`, y esa respuesta llega **después** de procesar todo lo que la precedía —los
/// `SUB` de antes—. Sin esto, un test que se suscribe y llama a continuación compite con el servidor.
async fn barrera(cliente: &async_nats::Client) {
    let _ = cliente
        .send_request(
            "barrera.sin.nadie",
            async_nats::Request::new().timeout(Some(Duration::from_secs(2))),
        )
        .await;
}

fn peticion_basica() -> Value {
    json!({ "clienteId": "cli-1", "totalCents": 9990, "moneda": "EUR", "campana": "Otoño" })
}

// ═════════════════════════════════════════════════════════════════════════════
//  Ciclo de aceptación — 11-request-reply.md §12.9
// ═════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn acl_ciclo_de_aceptacion_dos_instancias_reparten_y_al_parar_ambas_rpc_unavailable() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    // Dos instancias del MISMO servicio (`crm`): el grupo de cola es el nombre del servicio.
    let (log_a, _) = logger_espia();
    let (log_b, _) = logger_espia();
    let crm_a = bus_con(&broker.url(), "crm", |o| o.with_logger(log_a)).await;
    let crm_b = bus_con(&broker.url(), "crm", |o| o.with_logger(log_b)).await;
    let cuenta_a = Arc::new(AtomicUsize::new(0));
    let cuenta_b = Arc::new(AtomicUsize::new(0));
    let cuentas = [(&crm_a, &cuenta_a), (&crm_b, &cuenta_b)];
    let mut responders = Vec::new();
    for (b, cuenta) in cuentas {
        let cuenta = Arc::clone(cuenta);
        responders.push(
            b.respond(DESCUENTO, move |_: RpcRequest, _: RpcContext| {
                let cuenta = Arc::clone(&cuenta);
                async move {
                    cuenta.fetch_add(1, Ordering::SeqCst);
                    Ok::<Value, flux::HandlerError>(
                        json!({ "descuentoCents": 500, "moneda": "EUR" }),
                    )
                }
            })
            .await
            .expect("respond bajo la ACL de `acls.rpc`"),
        );
    }

    let llama = bus(&broker.url(), "llama").await;
    const N: usize = 200;
    for i in 0..N {
        let r = llama
            .request_with(
                DESCUENTO,
                &json!({ "clienteId": "cli-1", "totalCents": 9990, "moneda": "EUR" }),
                RequestOptions::default()
                    .with_timeout_ms(2_000)
                    .with_aggregate_id("cli-1"),
            )
            .await
            .unwrap_or_else(|e| panic!("petición {i}: {e}"));
        assert_eq!(r.data_as::<Value>().unwrap()["descuentoCents"], 500);
    }
    // Se reparten: cada petición la atiende UNA, y las dos reciben (la probabilidad de que una se quede
    // sin ninguna de 200 es 2^-199).
    let (a, b) = (
        cuenta_a.load(Ordering::SeqCst),
        cuenta_b.load(Ordering::SeqCst),
    );
    assert_eq!(
        a + b,
        N,
        "cada petición la atiende exactamente UNA instancia: {a} + {b}"
    );
    assert!(
        a > 40 && b > 40,
        "el reparto es al azar pero no degenera: {a} / {b}"
    );

    // Parar ambas: ordenado, y desde ese instante `no responders`, EN MENOS DE 100 ms.
    for r in &responders {
        r.stop().await;
    }
    for r in &responders {
        assert!(r.is_stopped());
    }
    let t = Instant::now();
    let e = llama
        .request_with(
            DESCUENTO,
            &peticion_basica(),
            RequestOptions::default().with_timeout_ms(5_000),
        )
        .await
        .expect_err("nadie escucha");
    let tardo = t.elapsed();
    assert_eq!(code(&e), "RPC_UNAVAILABLE", "{e}");
    assert_eq!(e.class(), Some(ErrorClass::Retryable));
    assert!(
        tardo < Duration::from_millis(100),
        "RPC_UNAVAILABLE llegó a los {tardo:?}, no antes de 100 ms"
    );

    // Sin una sola violación de la ACL en todo el ciclo: la que se concede es la que se usa.
    assert!(
        broker.violations().is_empty(),
        "violaciones: {:?}",
        broker.violations()
    );
}

// ═════════════════════════════════════════════════════════════════════════════
//  Quitar un permiso se nota — §12.9, §7.1, §7.2
// ═════════════════════════════════════════════════════════════════════════════

/// Una petición **denegada** NO falla rápido con `async-nats`: el servidor descarta el mensaje, el `-ERR`
/// llega solo por el evento de la conexión y la petición espera su timeout (A-72). El SDK lo escucha y lo
/// compite con la respuesta: `PERMISSION_DENIED` en milisegundos, no `RPC_TIMEOUT` a los 5 s.
#[tokio::test]
async fn acl_una_llamada_no_concedida_falla_rapido_con_permission_denied() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let crm = bus(&broker.url(), "crm").await;
    let _r = dueno_responde(&crm, OBTENER, json!({ "clienteId": "cli-1" })).await;

    // `ajeno` solo publica en `chismes.>`: la operación EXISTE y hay quien la atiende, y aun así.
    let (log, _) = logger_espia();
    let ajeno = bus_con(&broker.url(), "ajeno", |o| o.with_logger(log)).await;
    let t = Instant::now();
    let e = ajeno
        .request_with(
            OBTENER,
            &json!({ "clienteId": "cli-1" }),
            RequestOptions::default().with_timeout_ms(5_000),
        )
        .await
        .expect_err("la ACL lo deniega");
    let tardo = t.elapsed();
    assert_eq!(code(&e), "PERMISSION_DENIED", "{e}");
    assert_eq!(e.class(), Some(ErrorClass::Permanent));
    assert!(
        tardo < Duration::from_secs(1),
        "con `async-nats` la petición denegada solo da un timeout (5 s): el SDK debe competir el -ERR; \
         tardó {tardo:?}"
    );
    // Nombra el subject denegado y dónde se arregla.
    let msg = e.to_string();
    assert!(msg.contains("rpc.clientes.cliente.v1.obtener"), "{msg}");
    assert!(msg.contains("06-security.md"), "{msg}");
    // El broker lo registró: es una violación de verdad, no un timeout.
    espera(
        || !broker.violations().is_empty(),
        Duration::from_secs(2),
        "el broker no registró la violación",
    )
    .await;
    assert!(broker
        .violations()
        .iter()
        .any(|v| v.contains("rpc.clientes.cliente.v1.obtener")));
}

#[tokio::test]
async fn acl_sin_permiso_de_publicacion_o_de_inbox_quien_llama_ve_permission_denied() {
    let bin = broker_bin!();
    for (permiso, subject_en_el_mensaje) in [
        ("publish", "rpc.clientes.cliente.v1.calcular-descuento"),
        // Sin suscripción a su inbox privado ninguna respuesta puede llegar: la ACL de la v1 no basta.
        ("subscribe", "_INBOX.llama"),
    ] {
        let broker = Broker::sin_permiso(&bin, "llama", permiso);
        let crm = bus(&broker.url(), "crm").await;
        let _r = dueno_responde(&crm, DESCUENTO, json!({ "descuentoCents": 1 })).await;
        let llama = bus(&broker.url(), "llama").await;
        let t = Instant::now();
        let e = llama
            .request_with(
                DESCUENTO,
                &peticion_basica(),
                RequestOptions::default().with_timeout_ms(5_000),
            )
            .await
            .expect_err(permiso);
        assert_eq!(code(&e), "PERMISSION_DENIED", "{permiso}: {e}");
        assert!(
            t.elapsed() < Duration::from_secs(1),
            "{permiso}: tardó {:?}",
            t.elapsed()
        );
        assert!(
            e.to_string().contains(subject_en_el_mensaje),
            "{permiso}: {e}"
        );
    }
}

/// Un dueño sin permiso de suscripción parece un servicio caído para quien llama (`no responders`, A-73), y
/// el dueño arrancaría «sano»: `respond()` **DEBE** fallar al arrancar con `PERMISSION_DENIED`.
#[tokio::test]
async fn acl_un_dueno_sin_permiso_de_suscripcion_falla_al_arrancar_y_quien_llama_ve_unavailable() {
    let bin = broker_bin!();
    let broker = Broker::sin_permiso(&bin, "crm", "subscribe");
    let (log, lineas) = logger_espia();
    let crm = bus_con(&broker.url(), "crm", |o| o.with_logger(log)).await;
    let t = Instant::now();
    let e = crm
        .respond(DESCUENTO, |_: RpcRequest, _: RpcContext| async move {
            Ok::<Value, flux::HandlerError>(json!({}))
        })
        .await
        .expect_err("arrancaría mudo");
    assert_eq!(code(&e), "PERMISSION_DENIED", "{e}");
    assert_eq!(e.class(), Some(ErrorClass::Permanent));
    let msg = e.to_string();
    assert!(
        msg.contains("rpc.clientes.cliente.v1.calcular-descuento"),
        "nombra el subject: {msg}"
    );
    assert!(
        msg.contains("RPC_UNAVAILABLE"),
        "dice qué ve quien llama: {msg}"
    );
    // Falla en tiempo útil (el tope de espera del aviso), no tras un timeout de 5 s.
    assert!(
        t.elapsed() < Duration::from_millis(1_500),
        "tardó {:?}",
        t.elapsed()
    );
    // Registrado también en el log de la conexión, como cualquier denegación.
    assert!(
        lock(&lineas).iter().any(|l| l.contains("Subscription to")),
        "{:?}",
        lock(&lineas)
    );

    // Y no quedó nada registrado: se puede volver a intentar cuando se arregle la ACL.
    let e2 = crm
        .respond(DESCUENTO, |_: RpcRequest, _: RpcContext| async move {
            Ok::<Value, flux::HandlerError>(json!({}))
        })
        .await
        .expect_err("sigue sin permiso");
    assert_eq!(
        code(&e2),
        "PERMISSION_DENIED",
        "no se queda atascado en «ya hay un respond»: {e2}"
    );

    // Quien llama ve `no responders`: el problema se ve en el dueño, no aquí.
    let llama = bus(&broker.url(), "llama").await;
    let e = llama
        .request_with(
            DESCUENTO,
            &peticion_basica(),
            RequestOptions::default().with_timeout_ms(5_000),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "RPC_UNAVAILABLE", "{e}");
}

/// Sin `allow_responses` el dueño no puede contestar y quien llama ve un timeout (A-68): el aviso queda en
/// el log del dueño, con la pista de a qué le falta el permiso.
#[tokio::test]
async fn acl_un_dueno_sin_allow_responses_no_puede_contestar_y_lo_registra() {
    let bin = broker_bin!();
    let broker = Broker::sin_permiso(&bin, "crm", "allowResponses");
    let (log, lineas) = logger_espia();
    let crm = bus_con(&broker.url(), "crm", |o| o.with_logger(log)).await;
    let _r = dueno_responde(&crm, DESCUENTO, json!({ "descuentoCents": 1 })).await;
    let llama = bus(&broker.url(), "llama").await;
    let e = llama
        .request_with(
            DESCUENTO,
            &peticion_basica(),
            RequestOptions::default().with_timeout_ms(400),
        )
        .await
        .unwrap_err();
    assert_eq!(
        code(&e),
        "RPC_TIMEOUT",
        "quien llama solo ve un timeout: {e}"
    );
    espera(
        || lock(&lineas).iter().any(|l| l.contains("allow_responses")),
        Duration::from_secs(2),
        "el dueño no registró que no puede contestar",
    )
    .await;
    assert!(
        broker
            .violations()
            .iter()
            .any(|v| v.contains("Publish Violation") && v.contains("_INBOX.llama")),
        "{:?}",
        broker.violations()
    );
}

/// `{ max: 1, expires: 1s }` (crm-exp) y `{ max: 3 }` (crm-max3): el SDK contesta UNA vez por petición, así
/// que ninguna de las dos variantes provoca una sola violación.
#[tokio::test]
async fn acl_allow_responses_acotado_basta_porque_se_contesta_una_vez() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let llama = bus(&broker.url(), "llama").await;
    for servicio in ["crm-exp", "crm-max3"] {
        let dueno = bus(&broker.url(), servicio).await;
        let r = dueno_responde(&dueno, OBTENER, json!({ "clienteId": "cli-1" })).await;
        for _ in 0..5 {
            let respuesta = llama
                .request_with(
                    OBTENER,
                    &json!({ "clienteId": "cli-1" }),
                    RequestOptions::default().with_timeout_ms(2_000),
                )
                .await
                .unwrap_or_else(|e| panic!("{servicio}: {e}"));
            assert_eq!(respuesta.source, format!("/produccion/{servicio}"));
        }
        r.stop().await;
    }
    assert!(broker.violations().is_empty(), "{:?}", broker.violations());
}

// ═════════════════════════════════════════════════════════════════════════════
//  Una suscripción exacta por operación, y el grupo de cola es el servicio — §2.3, §2.5
// ═════════════════════════════════════════════════════════════════════════════

/// Con la suscripción exacta, una petición a una operación que el servicio **no atiende** (una errata, una
/// versión aún no desplegada) falla en ~1 ms con `no responders`; con `rpc.<dominio>.>` el servicio tendría
/// interés y quien llama esperaría su timeout (A-64).
#[tokio::test]
async fn broker_una_operacion_que_el_dueno_no_atiende_da_unavailable_no_timeout() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let crm = bus(&broker.url(), "crm").await;
    // El dueño atiende `obtener`; su permiso es `rpc.clientes.>`, pero la suscripción es la exacta.
    let _r = dueno_responde(&crm, OBTENER, json!({ "clienteId": "cli-1" })).await;
    let llama = bus(&broker.url(), "llama").await;
    let t = Instant::now();
    let e = llama
        .request_with(
            DESCUENTO,
            &peticion_basica(),
            RequestOptions::default().with_timeout_ms(5_000),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "RPC_UNAVAILABLE", "{e}");
    assert!(
        t.elapsed() < Duration::from_millis(100),
        "con un comodín esto sería un timeout: {:?}",
        t.elapsed()
    );
    // Y la atendida sí responde.
    assert!(llama
        .request(OBTENER, &json!({ "clienteId": "cli-1" }))
        .await
        .is_ok());
}

/// Dos SERVICIOS (dos grupos de cola) sobre la misma operación: cada uno recibe y ejecuta la petición, y
/// quien llama ve UNA respuesta (A-60). Por eso solo el dueño responde: es lo que la ACL impide.
#[tokio::test]
async fn broker_dos_servicios_sobre_una_operacion_ejecutan_los_dos() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let cuentas = Arc::new(Mutex::new(BTreeMap::<String, usize>::new()));
    for servicio in ["crm", "crm-exp"] {
        let b = bus(&broker.url(), servicio).await;
        let cuentas = Arc::clone(&cuentas);
        b.respond(OBTENER, move |_: RpcRequest, _: RpcContext| {
            let cuentas = Arc::clone(&cuentas);
            async move {
                *lock(&cuentas).entry(servicio.to_string()).or_default() += 1;
                Ok::<Value, flux::HandlerError>(json!({ "clienteId": "cli-1" }))
            }
        })
        .await
        .unwrap();
    }
    let llama = bus(&broker.url(), "llama").await;
    for _ in 0..10 {
        llama
            .request(OBTENER, &json!({ "clienteId": "cli-1" }))
            .await
            .expect("una respuesta");
    }
    espera(
        || lock(&cuentas).values().sum::<usize>() >= 20,
        Duration::from_secs(2),
        "los dos ejecutan cada petición",
    )
    .await;
    let c = lock(&cuentas).clone();
    assert_eq!(c.get("crm"), Some(&10), "{c:?}");
    assert_eq!(c.get("crm-exp"), Some(&10), "{c:?}");
}

#[tokio::test]
async fn broker_una_segunda_respond_sobre_la_misma_operacion_en_un_bus_se_rechaza() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let crm = bus(&broker.url(), "crm").await;
    let primero = dueno_responde(&crm, OBTENER, json!({})).await;
    let e = crm
        .respond(OBTENER, |_: RpcRequest, _: RpcContext| async move {
            Ok::<Value, flux::HandlerError>(json!({}))
        })
        .await
        .expect_err("dos handlers bajo el mismo grupo se repartirían las peticiones");
    assert_eq!(code(&e), "INVALID_RPC_CALL", "{e}");
    // Tras parar el primero, se puede volver a registrar.
    primero.stop().await;
    let _otra = dueno_responde(&crm, OBTENER, json!({})).await;
}

// ═════════════════════════════════════════════════════════════════════════════
//  Qué ve quien llama — §4.3
// ═════════════════════════════════════════════════════════════════════════════

/// El responder lento ejecuta y la respuesta llega tarde; quien llama ve `RPC_TIMEOUT`, **ambiguo** (A-61).
/// La respuesta tardía llega al inbox y se ignora: la conexión sigue sana.
#[tokio::test]
async fn broker_un_responder_lento_da_timeout_ejecuta_y_su_respuesta_tardia_se_ignora() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let crm = bus(&broker.url(), "crm").await;
    let ejecuciones = Arc::new(AtomicUsize::new(0));
    let e2 = Arc::clone(&ejecuciones);
    crm.respond(DESCUENTO, move |_: RpcRequest, _: RpcContext| {
        let e2 = Arc::clone(&e2);
        async move {
            e2.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(600)).await;
            Ok::<Value, flux::HandlerError>(json!({ "descuentoCents": 500, "moneda": "EUR" }))
        }
    })
    .await
    .unwrap();
    let llama = bus(&broker.url(), "llama").await;
    let t = Instant::now();
    let e = llama
        .request_with(
            DESCUENTO,
            &peticion_basica(),
            RequestOptions::default().with_timeout_ms(300),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "RPC_TIMEOUT", "{e}");
    assert_eq!(e.class(), Some(ErrorClass::Retryable));
    assert!(
        t.elapsed() >= Duration::from_millis(300) && t.elapsed() < Duration::from_millis(550),
        "{:?}",
        t.elapsed()
    );
    let msg = e.to_string();
    assert!(msg.contains("PUEDE haberse ejecutado"), "{msg}");
    assert!(msg.contains("300"), "{msg}");
    assert!(msg.contains(DESCUENTO), "{msg}");
    // La operación SE ejecutó, y la respuesta tardía llegó al inbox sin que nadie la espere.
    espera(
        || ejecuciones.load(Ordering::SeqCst) == 1,
        Duration::from_secs(1),
        "no se ejecutó",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    // Y la conexión de quien llamó sigue sana: la siguiente llamada (con tiempo) funciona.
    let ok = llama
        .request_with(
            DESCUENTO,
            &peticion_basica(),
            RequestOptions::default().with_timeout_ms(2_000),
        )
        .await
        .expect("tras la respuesta tardía, la conexión sigue sana");
    assert_eq!(ok.data_as::<Value>().unwrap()["descuentoCents"], 500);
    assert_eq!(ejecuciones.load(Ordering::SeqCst), 2);
}

/// La respuesta que no cabe en `max_payload` falla EN SILENCIO con `async-nats`: lanza en quien responde y
/// quien llama solo vería un timeout (A-65). El que responde lo comprueba y contesta con un error.
#[tokio::test]
async fn broker_una_respuesta_que_no_cabe_se_contesta_con_rpc_message_too_large_no_con_un_timeout()
{
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let (log, lineas) = logger_espia();
    let crm = bus_con(&broker.url(), "crm", |o| o.with_logger(log)).await;
    let enorme = "x".repeat(1_048_576);
    crm.respond(DESCUENTO, move |_: RpcRequest, _: RpcContext| {
        let enorme = enorme.clone();
        async move {
            Ok::<Value, flux::HandlerError>(
                json!({ "descuentoCents": 1, "moneda": "EUR", "motivo": enorme }),
            )
        }
    })
    .await
    .unwrap();
    let llama = bus(&broker.url(), "llama").await;
    let t = Instant::now();
    let e = llama
        .request_with(
            DESCUENTO,
            &peticion_basica(),
            RequestOptions::default().with_timeout_ms(5_000),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "RPC_MESSAGE_TOO_LARGE", "{e}");
    assert_eq!(e.class(), Some(ErrorClass::Permanent));
    assert!(
        t.elapsed() < Duration::from_secs(1),
        "un timeout sería el síntoma de A-65: {:?}",
        t.elapsed()
    );
    let msg = e.to_string();
    assert!(
        msg.contains("1048576") && msg.contains("claim-check"),
        "dice el límite y apunta al claim-check: {msg}"
    );
    // El responder lo registró, con el id de la petición no; con la operación.
    assert!(
        lock(&lineas).iter().any(|l| l.contains("no cabe")),
        "{:?}",
        lock(&lineas)
    );
}

/// La petición que no cabe falla antes de abrir nada: la librería la rechazaría al instante, y un cliente
/// en bruto perdería la conexión (A-65). EXACTAMENTE `max_payload` sí se publica.
#[tokio::test]
async fn broker_una_peticion_que_no_cabe_falla_antes_de_la_red_y_el_limite_exacto_pasa() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let llama = bus(&broker.url(), "llama").await;
    let espia = bruto(&broker.url()).await;
    let mut sub = espia
        .subscribe("rpc.clientes.cliente.v1.obtener")
        .await
        .unwrap();
    barrera(&espia).await;
    let con_relleno = |n: usize| json!({ "clienteId": "x".repeat(n) });

    // Calibración: cuánto ocupa el sobre sin relleno.
    let _ = llama
        .request_with(
            OBTENER,
            &con_relleno(1_000),
            RequestOptions::default().with_timeout_ms(50),
        )
        .await;
    let visto = tokio::time::timeout(Duration::from_secs(1), sub.next())
        .await
        .unwrap()
        .unwrap();
    let sobre = visto.payload.len() - 1_000;
    let limite = 1_048_576;

    // Justo en el límite: se publica (nadie responde → RPC_TIMEOUT o UNAVAILABLE, no RPC_MESSAGE_TOO_LARGE).
    let e = llama
        .request_with(
            OBTENER,
            &con_relleno(limite - sobre),
            RequestOptions::default().with_timeout_ms(50),
        )
        .await
        .unwrap_err();
    assert_ne!(
        code(&e),
        "RPC_MESSAGE_TOO_LARGE",
        "exactamente `max_payload` pasa: {e}"
    );
    let visto = tokio::time::timeout(Duration::from_secs(1), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(visto.payload.len(), limite);

    // Un byte más: RPC_MESSAGE_TOO_LARGE, sin red.
    let t = Instant::now();
    let e = llama
        .request_with(
            OBTENER,
            &con_relleno(limite - sobre + 1),
            RequestOptions::default().with_timeout_ms(5_000),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "RPC_MESSAGE_TOO_LARGE", "{e}");
    assert!(
        t.elapsed() < Duration::from_secs(1),
        "no espera a ningún timeout: {:?}",
        t.elapsed()
    );
    let msg = e.to_string();
    assert!(
        msg.contains(&(limite + 1).to_string()) && msg.contains("1048576"),
        "bytes y límite: {msg}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), sub.next())
            .await
            .is_err(),
        "no salió nada al cable"
    );
}

/// Lo que `async-nats` pone en el cable: el subject de respuesta de la petición es el inbox privado del servicio,
/// `_INBOX.<servicio>.<aleatorio>.<id>` —cuatro tokens—, que cubre el `_INBOX.<servicio>.>` de la ACL (A-94).
#[tokio::test]
async fn broker_el_subject_de_respuesta_es_el_inbox_privado_del_servicio() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let llama = bus(&broker.url(), "llama").await;
    let espia = bruto(&broker.url()).await;
    let mut cable = espia.subscribe(format!("rpc.{OBTENER}")).await.unwrap();
    barrera(&espia).await;
    for _ in 0..3 {
        let _ = llama
            .request_with(
                OBTENER,
                &json!({ "clienteId": "cli-1" }),
                RequestOptions::default().with_timeout_ms(50),
            )
            .await;
        let visto = tokio::time::timeout(Duration::from_secs(1), cable.next())
            .await
            .unwrap()
            .unwrap();
        let reply = visto
            .reply
            .expect("una petición lleva su subject de respuesta")
            .to_string();
        let tokens: Vec<&str> = reply.split('.').collect();
        assert_eq!(tokens.len(), 4, "{reply}");
        assert_eq!(
            (tokens[0], tokens[1]),
            ("_INBOX", "llama"),
            "el inbox es privado: {reply}"
        );
    }
    // Sin una violación: el multiplexor de la librería se suscribe bajo `_INBOX.llama.>`.
    assert!(broker.violations().is_empty(), "{:?}", broker.violations());
}

/// El acuse de un stream que captura `rpc.>` llega como si fuera la respuesta (A-18, A-67). Sin nadie
/// escuchando, además, la petición «se responde» sola: lo que ve quien llama es `RPC_INVALID_REPLY`.
#[tokio::test]
async fn broker_un_stream_que_captura_rpc_da_rpc_invalid_reply_con_la_pista_del_doctor() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let ops = bruto(&broker.url()).await;
    let js = async_nats::jetstream::new(ops.clone());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: "TRAMPA".into(),
        subjects: vec!["rpc.>".into()],
        ..Default::default()
    })
    .await
    .expect("el stream trampa");

    let llama = bus(&broker.url(), "llama").await;
    let t = Instant::now();
    let e = llama
        .request_with(
            OBTENER,
            &json!({ "clienteId": "cli-1" }),
            RequestOptions::default().with_timeout_ms(5_000),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "RPC_INVALID_REPLY", "{e}");
    assert_eq!(e.class(), Some(ErrorClass::Permanent));
    assert!(
        t.elapsed() < Duration::from_millis(500),
        "{:?}",
        t.elapsed()
    );
    let msg = e.to_string();
    assert!(msg.contains("flux doctor"), "apunta al doctor: {msg}");
    assert!(msg.contains("rpc.>"), "{msg}");
    assert!(msg.contains(OBTENER), "{msg}");

    // Con un responder real, es una carrera (A-67: entre 18 y 20 de 20 «respuestas» son el acuse): jamás
    // es una respuesta de OTRO tipo, y el que responde ejecuta todas igualmente.
    let crm = bus(&broker.url(), "crm").await;
    let ejecutadas = Arc::new(AtomicUsize::new(0));
    let e2 = Arc::clone(&ejecutadas);
    crm.respond(DESCUENTO, move |_: RpcRequest, _: RpcContext| {
        let e2 = Arc::clone(&e2);
        async move {
            e2.fetch_add(1, Ordering::SeqCst);
            Ok::<Value, flux::HandlerError>(json!({ "descuentoCents": 1, "moneda": "EUR" }))
        }
    })
    .await
    .unwrap();
    let mut invalidas = 0;
    for _ in 0..20 {
        match llama
            .request_with(
                DESCUENTO,
                &peticion_basica(),
                RequestOptions::default().with_timeout_ms(2_000),
            )
            .await
        {
            Ok(_) => {}
            Err(e) => {
                assert_eq!(code(&e), "RPC_INVALID_REPLY", "{e}");
                invalidas += 1;
            }
        }
    }
    assert!(
        invalidas >= 10,
        "el acuse gana casi siempre: {invalidas} de 20"
    );
    espera(
        || ejecutadas.load(Ordering::SeqCst) == 20,
        Duration::from_secs(2),
        "el responder ejecuta las 20, con el stream o sin él",
    )
    .await;
}

/// `no_responders` exige `headers` y `no_responders` en CONNECT: sin ellos, silencio y timeout (A-57). Se
/// comprueba lo que la conexión del SDK ANUNCIA de verdad, con un servidor de mentira que lee el CONNECT.
#[tokio::test]
async fn broker_la_conexion_anuncia_headers_y_no_responders_en_connect() {
    use std::io::{BufRead, BufReader, Write};

    let puerto = free_port();
    let escucha = TcpListener::bind(("127.0.0.1", puerto)).unwrap();
    let capturado = Arc::new(Mutex::new(None::<String>));
    let c2 = Arc::clone(&capturado);
    let hilo = std::thread::spawn(move || {
        let (mut socket, _) = escucha.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let info = format!(
            "INFO {{\"server_id\":\"falso\",\"server_name\":\"falso\",\"version\":\"2.14.5\",\"proto\":1,\"headers\":true,\"max_payload\":1048576,\"host\":\"127.0.0.1\",\"port\":{puerto}}}\r\n"
        );
        socket.write_all(info.as_bytes()).unwrap();
        let mut lector = BufReader::new(socket.try_clone().unwrap());
        let mut linea = String::new();
        while lector.read_line(&mut linea).is_ok_and(|n| n > 0) {
            if let Some(json) = linea.trim().strip_prefix("CONNECT ") {
                *lock(&c2) = Some(json.to_string());
            }
            if linea.trim() == "PING" {
                socket.write_all(b"PONG\r\n").unwrap();
                return;
            }
            linea.clear();
        }
    });
    let _ = tokio::time::timeout(
        Duration::from_secs(3),
        connect(ConnectOptions::new(
            format!("nats://127.0.0.1:{puerto}"),
            "llama",
            "produccion",
            "1.0.0",
        )),
    )
    .await;
    hilo.join().unwrap();
    let connect_json: Value =
        serde_json::from_str(&lock(&capturado).clone().expect("el SDK envió CONNECT")).unwrap();
    assert_eq!(
        connect_json["headers"], true,
        "sin `headers` no hay mensaje de estado 503: {connect_json}"
    );
    assert_eq!(
        connect_json["no_responders"], true,
        "sin `no_responders` quien llama solo ve un timeout (A-57): {connect_json}"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
//  Parar un responder — §5.5
// ═════════════════════════════════════════════════════════════════════════════

/// **Cerrar la conexión de golpe pierde lo que se atiende** (A-62). Parar con el orden de §5.5 —dejar de
/// escuchar, esperar a lo que está en vuelo, cerrar— no pierde nada: la petición en vuelo termina con su
/// respuesta y la siguiente falla en ~1 ms con `no responders`.
#[tokio::test]
async fn broker_parar_en_orden_no_pierde_lo_que_esta_en_vuelo() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let crm = bus(&broker.url(), "crm").await;
    let empezadas = Arc::new(AtomicUsize::new(0));
    let e2 = Arc::clone(&empezadas);
    let responder = crm
        .respond(DESCUENTO, move |_: RpcRequest, _: RpcContext| {
            let e2 = Arc::clone(&e2);
            async move {
                e2.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(400)).await;
                Ok::<Value, flux::HandlerError>(json!({ "descuentoCents": 500, "moneda": "EUR" }))
            }
        })
        .await
        .unwrap();
    let llama = bus(&broker.url(), "llama").await;
    let en_vuelo = {
        let llama = llama.clone();
        tokio::spawn(async move {
            llama
                .request_with(
                    DESCUENTO,
                    &peticion_basica(),
                    RequestOptions::default().with_timeout_ms(3_000),
                )
                .await
        })
    };
    espera(
        || empezadas.load(Ordering::SeqCst) == 1,
        Duration::from_secs(1),
        "el handler no empezó",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A los 100 ms: parar. `stop()` no vuelve hasta que lo que está en vuelo termina.
    let parada = {
        let r = responder.clone();
        // Lo que tarda `stop()` MISMO, medido dentro de su tarea: medirlo fuera lo confunde con la
        // petición en vuelo, que tarda lo suyo aunque `stop()` no esperase a nadie.
        tokio::spawn(async move {
            let t = Instant::now();
            r.stop().await;
            t.elapsed()
        })
    };
    tokio::time::sleep(Duration::from_millis(60)).await;
    // Desde que deja de escuchar, la petición NUEVA da `no responders` al instante (no cae en la que se va).
    let t2 = Instant::now();
    let e = llama
        .request_with(
            DESCUENTO,
            &peticion_basica(),
            RequestOptions::default().with_timeout_ms(3_000),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "RPC_UNAVAILABLE", "{e}");
    assert!(
        t2.elapsed() < Duration::from_millis(100),
        "{:?}",
        t2.elapsed()
    );

    // La que estaba en vuelo TERMINA con su respuesta.
    let r = en_vuelo
        .await
        .unwrap()
        .expect("la petición en vuelo no se pierde");
    assert_eq!(r.data_as::<Value>().unwrap()["descuentoCents"], 500);
    let tardo_stop = parada.await.unwrap();
    assert!(
        tardo_stop >= Duration::from_millis(200),
        "stop() debe esperar a lo que está en vuelo (le quedaban ~300 ms al handler): {tardo_stop:?}"
    );
    assert!(responder.is_stopped());
    assert_eq!(
        empezadas.load(Ordering::SeqCst),
        1,
        "la que llegó tras parar no se atendió"
    );
}

/// `Bus::close()` para los responders en orden antes de drenar la conexión: lo que está en vuelo termina.
#[tokio::test]
async fn broker_bus_close_espera_a_los_responders_en_vuelo() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let crm = bus(&broker.url(), "crm").await;
    let empezadas = Arc::new(AtomicUsize::new(0));
    let e2 = Arc::clone(&empezadas);
    crm.respond(DESCUENTO, move |_: RpcRequest, _: RpcContext| {
        let e2 = Arc::clone(&e2);
        async move {
            e2.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok::<Value, flux::HandlerError>(json!({ "descuentoCents": 7, "moneda": "EUR" }))
        }
    })
    .await
    .unwrap();
    let llama = bus(&broker.url(), "llama").await;
    let en_vuelo = {
        let llama = llama.clone();
        tokio::spawn(async move {
            llama
                .request_with(
                    DESCUENTO,
                    &peticion_basica(),
                    RequestOptions::default().with_timeout_ms(3_000),
                )
                .await
        })
    };
    espera(
        || empezadas.load(Ordering::SeqCst) == 1,
        Duration::from_secs(1),
        "el handler no empezó",
    )
    .await;
    // Cerrar el bus del dueño con la petición a medias.
    crm.close().await.expect("close");
    let r = en_vuelo
        .await
        .unwrap()
        .expect("close() no pierde lo que se atiende");
    assert_eq!(r.data_as::<Value>().unwrap()["descuentoCents"], 7);
}

// ═════════════════════════════════════════════════════════════════════════════
//  El algoritmo de quien responde y de quien llama, de extremo a extremo
// ═════════════════════════════════════════════════════════════════════════════

/// Qué sale de quien llama cuando el dueño falla de cada manera — §3.3, §5.3. Un handler de eventos que
/// llama a una operación hereda la clasificación **sin hacer nada**.
#[tokio::test]
async fn algoritmo_los_errores_del_handler_llegan_clasificados_a_quien_llama() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let (log, lineas) = logger_espia();
    let crm = bus_con(&broker.url(), "crm", |o| o.with_logger(log)).await;
    crm.respond(
        DESCUENTO,
        |peticion: RpcRequest, _: RpcContext| async move {
            let datos: Value = peticion.data_as()?;
            let resultado: Result<Value, flux::HandlerError> =
                match datos["campana"].as_str().unwrap_or("") {
                    "retryable" => Err(flux::FluxError::retryable(
                        "el servicio de tarifas no responde",
                    )
                    .with_code("HTTP_503")
                    .with_retry_after(Duration::from_secs(5))
                    .into()),
                    "permanent" => Err(flux::FluxError::permanent("el pedido ya está cancelado")
                        .with_code("PEDIDO_YA_CANCELADO")
                        .into()),
                    "desconocido" => Err("fallo con el dato secreto ana@example.com".into()),
                    "http" => {
                        Err(flux::HttpError::new(503, "GET /tarifas?email=ana@example.com").into())
                    }
                    "no-objeto" => Ok(json!([1, 2, 3])),
                    "panico" => panic!("bug con el dato secreto ana@example.com"),
                    _ => Ok(json!({ "descuentoCents": 1, "moneda": "EUR" })),
                };
            resultado
        },
    )
    .await
    .unwrap();
    let llama = bus(&broker.url(), "llama").await;
    let llamada = |campana: &'static str| {
        let llama = llama.clone();
        async move {
            llama
                .request_with(
                    DESCUENTO,
                    &json!({ "clienteId": "cli-1", "campana": campana }),
                    RequestOptions::default().with_timeout_ms(2_000),
                )
                .await
        }
    };

    // RetryableError, con su código, su mensaje y el `retryAfterMs` como `retry_after`.
    let e = llamada("retryable").await.unwrap_err();
    assert_eq!(
        (e.class(), e.code()),
        (Some(ErrorClass::Retryable), Some("HTTP_503")),
        "{e}"
    );
    assert_eq!(e.to_string(), "el servicio de tarifas no responde");
    assert_eq!(e.retry_after(), Some(Duration::from_secs(5)));
    // PermanentError.
    let e = llamada("permanent").await.unwrap_err();
    assert_eq!(
        (e.class(), e.code()),
        (Some(ErrorClass::Permanent), Some("PEDIDO_YA_CANCELADO")),
        "{e}"
    );
    // Desconocido: permanent, UNKNOWN y SIN el texto de la excepción.
    let e = llamada("desconocido").await.unwrap_err();
    assert_eq!(
        (e.class(), e.code()),
        (Some(ErrorClass::Permanent), Some("UNKNOWN")),
        "{e}"
    );
    assert!(!e.to_string().contains("ana@example.com"), "{e}");
    // Un error que el clasificador reconoce: su clase y su código, mensaje genérico.
    let e = llamada("http").await.unwrap_err();
    assert_eq!(
        (e.class(), e.code()),
        (Some(ErrorClass::Retryable), Some("HTTP_503")),
        "{e}"
    );
    assert!(!e.to_string().contains("ana@example.com"), "{e}");
    // Una respuesta que no es un objeto.
    let e = llamada("no-objeto").await.unwrap_err();
    assert_eq!(
        (e.class(), e.code()),
        (Some(ErrorClass::Permanent), Some("INVALID_SCHEMA")),
        "{e}"
    );
    assert!(
        e.to_string().contains("la respuesta no es un objeto"),
        "{e}"
    );
    // Un pánico no mata al responder: lo desconocido, y la siguiente llamada se atiende.
    let e = llamada("panico").await.unwrap_err();
    assert_eq!(
        (e.class(), e.code()),
        (Some(ErrorClass::Permanent), Some("UNKNOWN")),
        "{e}"
    );
    assert!(
        llamada("ok").await.is_ok(),
        "el responder sigue vivo tras un pánico"
    );
    // El texto secreto SÍ está en el log de quien responde, y solo ahí.
    assert!(lock(&lineas).iter().any(|l| l.contains("ana@example.com")));
}

/// Lo que se hace desde un handler de operación continúa la cadena: `correlationid` y `tenantid` se heredan y
/// el `causationid` es el `id` de lo que está en curso — §3.1, §5.1.
#[tokio::test]
async fn algoritmo_una_llamada_dentro_de_un_handler_continua_la_cadena() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    // Al final de la cadena: `crm` atiende `obtener` y anota lo que ve.
    let visto = Arc::new(Mutex::new(None::<RpcContext>));
    let v2 = Arc::clone(&visto);
    let crm = bus(&broker.url(), "crm").await;
    crm.respond(OBTENER, move |_: RpcRequest, ctx: RpcContext| {
        let v2 = Arc::clone(&v2);
        async move {
            *lock(&v2) = Some(ctx);
            Ok::<Value, flux::HandlerError>(json!({ "clienteId": "cli-1" }))
        }
    })
    .await
    .unwrap();
    // En medio: `intermedio` atiende `cotizar` y, para contestar, llama a `obtener`.
    let medio_bus = bus(&broker.url(), "intermedio").await;
    let medio = medio_bus.clone();
    let id_de_la_cotizacion = Arc::new(Mutex::new(String::new()));
    let id2 = Arc::clone(&id_de_la_cotizacion);
    medio_bus
        .respond(
            "pedidos.pedido.v1.cotizar",
            move |_: RpcRequest, ctx: RpcContext| {
                let medio = medio.clone();
                let id2 = Arc::clone(&id2);
                async move {
                    *lock(&id2) = ctx.id.clone();
                    let r = medio
                        .request(OBTENER, &json!({ "clienteId": "cli-1" }))
                        .await?;
                    Ok::<Value, flux::HandlerError>(
                        json!({ "totalCents": 1, "cliente": r.data_as::<Value>()?["clienteId"] }),
                    )
                }
            },
        )
        .await
        .unwrap();
    let llamador = bus(&broker.url(), "llamador").await;
    let raiz = llamador
        .request_with(
            "pedidos.pedido.v1.cotizar",
            &json!({ "pedidoId": "ped-1" }),
            RequestOptions::default()
                .with_timeout_ms(3_000)
                .with_tenant_id("acme")
                .with_aggregate_id("ped-1"),
        )
        .await
        .expect("la cadena completa");

    let id_raiz = lock(&id_de_la_cotizacion).clone();
    // La respuesta a la petición raíz: hereda y su causationid es el id de la petición.
    assert_eq!(raiz.causationid.as_deref(), Some(id_raiz.as_str()));
    assert_eq!(raiz.tenantid, "acme");
    assert_eq!(raiz.aggregate_id.as_deref(), Some("ped-1"));
    assert_eq!(raiz.source, "/produccion/intermedio");
    assert_eq!(raiz.dataclassification, DataClassification::Internal);
    // Lo que vio `crm`, al final: la MISMA correlación (el id de la raíz: nació allí), el MISMO tenant —que
    // `intermedio` no tiene configurado— y la causa es la petición que la provocó.
    let ctx = lock(&visto)
        .clone()
        .expect("crm atendió la petición anidada");
    assert_eq!(
        ctx.correlation_id, id_raiz,
        "una sola correlación a través de las dos operaciones"
    );
    assert_eq!(
        ctx.tenant_id, "acme",
        "el tenant se hereda: el de `intermedio` no se usa"
    );
    assert_eq!(ctx.event.causationid.as_deref(), Some(id_raiz.as_str()));
    assert_eq!(ctx.event.source, "/produccion/intermedio");
    assert_ne!(ctx.id, id_raiz);
}

/// El tenant de la llamada: la opción, el evento en curso, el de `connect()` y, si no hay, `system` — §3.1.
#[tokio::test]
async fn algoritmo_el_tenant_de_la_llamada_se_resuelve_como_en_los_eventos() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let crm = bus(&broker.url(), "crm").await;
    let tenants = Arc::new(Mutex::new(Vec::<String>::new()));
    let t2 = Arc::clone(&tenants);
    crm.respond(OBTENER, move |_: RpcRequest, ctx: RpcContext| {
        let t2 = Arc::clone(&t2);
        async move {
            lock(&t2).push(ctx.tenant_id.clone());
            Ok::<Value, flux::HandlerError>(json!({}))
        }
    })
    .await
    .unwrap();
    let peticion = json!({ "clienteId": "cli-1" });
    // Sin tenant en `connect()`: `system`.
    let sin = bus(&broker.url(), "llama").await;
    let r = sin.request(OBTENER, &peticion).await.unwrap();
    assert_eq!(r.tenantid, "system");
    // Con tenant en `connect()`.
    let con = bus_con(&broker.url(), "llama", |o| o.with_tenant_id("acme")).await;
    assert_eq!(
        con.request(OBTENER, &peticion).await.unwrap().tenantid,
        "acme"
    );
    // La opción de la llamada gana.
    let r = con
        .request_with(
            OBTENER,
            &peticion,
            RequestOptions::default().with_tenant_id("globex"),
        )
        .await
        .unwrap();
    assert_eq!(r.tenantid, "globex");
    // El responder NO filtra por su propio tenant: una instancia atada a `acme` atiende `globex` (§5.6).
    assert_eq!(*lock(&tenants), ["system", "acme", "globex"]);
}

/// Cada reintento reutiliza el **mismo `id` y los mismos bytes**, `time` incluido (§4.5): es lo que permite que
/// quien responde deduplique. Solo ante `RPC_UNAVAILABLE` y `RPC_TIMEOUT`.
#[tokio::test]
async fn algoritmo_los_reintentos_reutilizan_el_mismo_id_y_los_mismos_bytes() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let crm = bus(&broker.url(), "crm").await;
    let vistos = Arc::new(Mutex::new(Vec::<(String, Vec<u8>)>::new()));
    let v2 = Arc::clone(&vistos);
    crm.respond(DESCUENTO, move |_: RpcRequest, ctx: RpcContext| {
        let v2 = Arc::clone(&v2);
        async move {
            let primera = {
                let mut v = lock(&v2);
                v.push((ctx.id.clone(), serde_json::to_vec(&ctx.event).unwrap()));
                v.len() == 1
            };
            if primera {
                // La primera ejecución es lenta: quien llama agota su timeout y reintenta.
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Ok::<Value, flux::HandlerError>(json!({ "descuentoCents": 1, "moneda": "EUR" }))
        }
    })
    .await
    .unwrap();
    let llama = bus(&broker.url(), "llama").await;
    let r = llama
        .request_with(
            DESCUENTO,
            &peticion_basica(),
            RequestOptions::default()
                .with_timeout_ms(200)
                .with_retries(2)
                .with_retry_delay(Duration::from_millis(10)),
        )
        .await
        .expect("el segundo intento responde");
    assert_eq!(r.data_as::<Value>().unwrap()["descuentoCents"], 1);
    espera(
        || lock(&vistos).len() == 2,
        Duration::from_secs(2),
        "quien responde ve los dos intentos",
    )
    .await;
    let v = lock(&vistos).clone();
    assert_eq!(v[0].0, v[1].0, "el MISMO id");
    assert_eq!(
        String::from_utf8_lossy(&v[0].1),
        String::from_utf8_lossy(&v[1].1),
        "los mismos bytes, time incluido"
    );
    // Sin reintentos (el default), el mismo caso falla: UN intento.
    lock(&vistos).clear();
    let _ = llama
        .request_with(
            DESCUENTO,
            &peticion_basica(),
            RequestOptions::default().with_timeout_ms(100),
        )
        .await;
    assert_eq!(lock(&vistos).len(), 1);
}

/// `RPC_UNAVAILABLE` se reintenta hasta que el servicio vuelve: el hueco de un despliegue — §4.5.
#[tokio::test]
async fn algoritmo_un_reintento_cubre_el_hueco_de_un_despliegue() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let llama = bus(&broker.url(), "llama").await;
    let url = broker.url();
    let arranque = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let crm = bus(&url, "crm").await;
        let r = dueno_responde(&crm, OBTENER, json!({ "clienteId": "cli-1" })).await;
        (crm, r)
    });
    let t = Instant::now();
    let r = llama
        .request_with(
            OBTENER,
            &json!({ "clienteId": "cli-1" }),
            RequestOptions::default()
                .with_retries(5)
                .with_retry_delay(Duration::from_millis(100)),
        )
        .await
        .expect("al tercer o cuarto intento el servicio ya escucha");
    assert_eq!(r.data_as::<Value>().unwrap()["clienteId"], "cli-1");
    assert!(
        t.elapsed() >= Duration::from_millis(250),
        "{:?}",
        t.elapsed()
    );
    let _ = arranque.await.unwrap();
    // Y lo permanente NO se reintenta: un solo intento aunque se pidan tres reintentos.
    let intentos = Arc::new(AtomicUsize::new(0));
    let otro = bus(&broker.url(), "crm-exp").await;
    let i2 = Arc::clone(&intentos);
    otro.respond(DESCUENTO, move |_: RpcRequest, _: RpcContext| {
        let i2 = Arc::clone(&i2);
        async move {
            i2.fetch_add(1, Ordering::SeqCst);
            Err::<Value, flux::HandlerError>(Box::new(
                flux::FluxError::permanent("no").with_code("NO"),
            ))
        }
    })
    .await
    .unwrap();
    let e = llama
        .request_with(
            DESCUENTO,
            &peticion_basica(),
            RequestOptions::default().with_retries(3),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "NO");
    assert_eq!(intentos.load(Ordering::SeqCst), 1);
}

/// Las llamadas mal formadas fallan **antes de tocar la red**: un espía en bruto suscrito a `rpc.>` no ve
/// pasar ni una petición, y el broker no registra ninguna violación — §4.1 paso 0.
#[tokio::test]
async fn algoritmo_lo_mal_formado_falla_antes_de_la_red_y_con_su_codigo() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let metricas = Arc::new(InMemoryMetrics::new());
    let llama = bus_con(&broker.url(), "llama", |o| o.with_metrics(metricas.clone())).await;
    let espia = bruto(&broker.url()).await;
    let mut cable = espia.subscribe("rpc.>").await.unwrap();
    barrera(&espia).await;
    let datos = json!({ "clienteId": "cli-1" });
    for (operacion, esperado) in [
        ("rpc.clientes.cliente.v1.obtener", "RESERVED_DOMAIN"),
        ("state.clientes.cliente.v1.obtener", "RESERVED_DOMAIN"),
        ("dlq.clientes.cliente.v1.obtener", "RESERVED_DOMAIN"),
        ("system.clientes.cliente.v1.obtener", "RESERVED_DOMAIN"),
        ("clientes.cliente.v1", "INVALID_RPC_CALL"),
        ("clientes.cliente.v1.obtener.x", "INVALID_RPC_CALL"),
        ("Clientes.cliente.v1.obtener", "INVALID_RPC_CALL"),
        ("clientes.cliente.v1.obtener_cliente", "INVALID_RPC_CALL"),
        ("clientes.cliente.v1.*", "INVALID_RPC_CALL"),
        ("clientes.cliente.v1.>", "INVALID_RPC_CALL"),
        ("clientes.cliente.v0.obtener", "INVALID_RPC_CALL"),
        ("clientes.cliente.v1.obtener\n", "INVALID_RPC_CALL"),
        ("", "INVALID_RPC_CALL"),
    ] {
        let e = llama.request(operacion, &datos).await.unwrap_err();
        assert_eq!(code(&e), esperado, "{operacion:?}: {e}");
        assert_eq!(e.class(), Some(ErrorClass::Permanent));
    }
    // El timeout fuera de rango, o que no es un entero de ms.
    for malo in [
        Duration::ZERO,
        Duration::from_millis(25_001),
        Duration::from_secs(60),
        Duration::from_micros(1_500),
    ] {
        let e = llama
            .request_with(
                OBTENER,
                &datos,
                RequestOptions::default().with_timeout(malo),
            )
            .await
            .unwrap_err();
        assert_eq!(code(&e), "INVALID_RPC_CALL", "{malo:?}: {e}");
    }
    // `data` que no es un objeto.
    for malo in [json!([1]), json!("texto"), json!(7), json!(null)] {
        let e = llama.request(OBTENER, &malo).await.unwrap_err();
        assert_eq!(code(&e), "INVALID_RPC_CALL", "{malo}: {e}");
    }
    // Demasiados reintentos.
    let e = llama
        .request_with(OBTENER, &datos, RequestOptions::default().with_retries(99))
        .await
        .unwrap_err();
    assert_eq!(code(&e), "INVALID_RPC_CALL");
    // Nada de lo anterior salió al cable.
    barrera(&espia).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(150), cable.next())
            .await
            .is_err(),
        "una llamada mal formada llegó al broker"
    );
    // Y una bien formada SÍ sale: la prueba de que el espía ve lo que pasa. El espía tiene interés en
    // `rpc.>` —un comodín, justo lo que `respond` no hace—, así que a quien llama le da un timeout en vez
    // de `no responders` (A-64).
    let e = llama
        .request_with(
            OBTENER,
            &datos,
            RequestOptions::default().with_timeout_ms(200),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "RPC_TIMEOUT", "{e}");
    let visto = tokio::time::timeout(Duration::from_secs(1), cable.next())
        .await
        .expect("el espía ve la buena")
        .unwrap();
    assert_eq!(visto.subject.as_str(), "rpc.clientes.cliente.v1.obtener");
    drop(cable);
    barrera(&espia).await;
    // Los errores de programación no se cuentan: sería una serie por cada error de tipeo.
    let render = metricas.render();
    assert!(!render.contains("operation=\"rpc."), "{render}");
    assert!(!render.contains("Clientes"), "{render}");
    assert_eq!(
        metricas.counter(&format!(
            "flux_rpc_client_requests_total{{operation=\"{OBTENER}\",outcome=\"timeout\"}}"
        )),
        Some(1)
    );

    // `respond` rechaza lo mismo, también antes de la red: nada queda suscrito.
    let crm = bus(&broker.url(), "crm").await;
    for (operacion, esperado) in [
        ("rpc.clientes.cliente.v1.obtener", "RESERVED_DOMAIN"),
        ("state.clientes.cliente.v1.obtener", "RESERVED_DOMAIN"),
        ("clientes.cliente.v1", "INVALID_RPC_CALL"),
        ("clientes.cliente.v1.*", "INVALID_RPC_CALL"),
        ("clientes.cliente.v1.obtener_cliente", "INVALID_RPC_CALL"),
    ] {
        let e = crm
            .respond(operacion, |_: RpcRequest, _: RpcContext| async move {
                Ok::<Value, flux::HandlerError>(json!({}))
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), esperado, "{operacion:?}: {e}");
    }
    let e = crm
        .respond_with(OBTENER, |_: RpcRequest, _: RpcContext| async move { Ok::<Value, flux::HandlerError>(json!({})) }, RespondOptions::default().with_max_concurrent(0))
        .await
        .unwrap_err();
    assert_eq!(code(&e), "INVALID_RPC_CALL");
    let e = llama.request(OBTENER, &datos).await.unwrap_err();
    assert_eq!(
        code(&e),
        "RPC_UNAVAILABLE",
        "ningún `respond` mal formado dejó una suscripción"
    );
    assert!(
        broker.violations().is_empty(),
        "nada de esto salió al cable: {:?}",
        broker.violations()
    );
}

/// `publish()` no puede escribir en `rpc.`: publicaría una petición sin respuesta posible, y por core.
#[tokio::test]
async fn algoritmo_publish_no_puede_escribir_en_rpc() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let crm = bus(&broker.url(), "crm").await;
    let e = crm
        .publish(
            "rpc.clientes.cliente.v1.obtener",
            &json!({ "clienteId": "cli-1" }),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "RESERVED_DOMAIN", "{e}");
}

// ── Respondedores en bruto: lo que el SDK no produce ────────────────────────

/// Un responder **en bruto** sobre una operación: para cada petición llama a `fabrica(petición)` y publica lo
/// que devuelva —bytes arbitrarios— en el subject de respuesta.
async fn responder_en_bruto<F>(
    url: &str,
    operacion: &str,
    fabrica: F,
) -> tokio::task::JoinHandle<()>
where
    F: Fn(&flux::Event) -> Vec<u8> + Send + 'static,
{
    let cliente = bruto(url).await;
    let mut sub = cliente.subscribe(format!("rpc.{operacion}")).await.unwrap();
    barrera(&cliente).await;
    tokio::spawn(async move {
        while let Some(m) = sub.next().await {
            let Some(reply) = m.reply else { continue };
            let peticion = flux::parse_event(&m.payload).expect("la petición del SDK es legible");
            let _ = cliente.publish(reply, fabrica(&peticion).into()).await;
            let _ = cliente.flush().await;
        }
    })
}

fn respuesta_buena(peticion: &flux::Event) -> flux::Event {
    let mut e = peticion.clone();
    e.id = uuid_nuevo();
    // Una respuesta NO hereda la firma de la petición: firma el que responde, con lo que él diga.
    e.signkeyid = None;
    e.signature = None;
    e.source = "/produccion/falso".into();
    e.event_type = "com.flux.rpc.clientes.cliente.obtener.reply.v1".into();
    e.dataschema = "https://schemas.internal/rpc/clientes/cliente/obtener/reply/1.0.0.json".into();
    e.causationid = Some(peticion.id.clone());
    e.data = serde_json::value::to_raw_value(&json!({ "clienteId": "cli-1" })).unwrap();
    e
}

fn uuid_nuevo() -> String {
    static N: AtomicUsize = AtomicUsize::new(0);
    format!(
        "01924f91-5d0b-7c1a-8e22-{:012x}",
        N.fetch_add(1, Ordering::SeqCst)
    )
}

fn bytes_de(e: &flux::Event) -> Vec<u8> {
    serde_json::to_vec(e).unwrap()
}

/// Las cinco comprobaciones de §4.3: lo que llega y no es la respuesta a ESTA petición es `RPC_INVALID_REPLY`.
#[tokio::test]
async fn algoritmo_una_respuesta_que_no_es_de_esta_peticion_es_rpc_invalid_reply() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let llama = bus(&broker.url(), "llama").await;
    type Rompe = Box<dyn Fn(&mut flux::Event) + Send + Sync>;
    let variantes: Vec<(&str, Option<&str>, Rompe)> = vec![
        ("control: una respuesta buena", None, Box::new(|_| {})),
        (
            "type de otra operación",
            Some("su `type`"),
            Box::new(|e| {
                e.event_type = "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1".into()
            }),
        ),
        (
            "type de una petición",
            Some("su `type`"),
            Box::new(|e| e.event_type = "com.flux.rpc.clientes.cliente.obtener.request.v1".into()),
        ),
        (
            "type de un evento",
            Some("su `type`"),
            Box::new(|e| e.event_type = "com.flux.clientes.cliente.creado.v1".into()),
        ),
        (
            "causationid de otra petición",
            Some("causationid"),
            Box::new(|e| e.causationid = Some("otra".into())),
        ),
        (
            "sin causationid",
            Some("causationid"),
            Box::new(|e| e.causationid = None),
        ),
        (
            "correlationid ajeno",
            Some("correlationid"),
            Box::new(|e| e.correlationid = "ajeno".into()),
        ),
        (
            "tenantid ajeno",
            Some("tenantid"),
            Box::new(|e| e.tenantid = "globex".into()),
        ),
        (
            "error con class poison",
            Some("esquema"),
            Box::new(|e| {
                e.event_type = "com.flux.rpc.error.v1".into();
                e.data = serde_json::value::to_raw_value(
                    &json!({ "class": "poison", "code": "X", "message": "m" }),
                )
                .unwrap();
            }),
        ),
        (
            "error sin message",
            Some("esquema"),
            Box::new(|e| {
                e.event_type = "com.flux.rpc.error.v1".into();
                e.data =
                    serde_json::value::to_raw_value(&json!({ "class": "permanent", "code": "X" }))
                        .unwrap();
            }),
        ),
    ];
    for (nombre, trozo, rompe) in variantes {
        let rompe = Arc::new(rompe);
        let r2 = Arc::clone(&rompe);
        let tarea = responder_en_bruto(&broker.url(), OBTENER, move |peticion| {
            let mut e = respuesta_buena(peticion);
            r2(&mut e);
            bytes_de(&e)
        })
        .await;
        let resultado = llama
            .request_with(
                OBTENER,
                &json!({ "clienteId": "cli-1" }),
                RequestOptions::default().with_timeout_ms(2_000),
            )
            .await;
        tarea.abort();
        match trozo {
            None => assert!(resultado.is_ok(), "{nombre}: {resultado:?}"),
            Some(trozo) => {
                let e = resultado.expect_err(nombre);
                assert_eq!(code(&e), "RPC_INVALID_REPLY", "{nombre}: {e}");
                assert_eq!(e.class(), Some(ErrorClass::Permanent));
                assert!(e.to_string().contains(trozo), "{nombre}: {e}");
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Lo que ni siquiera es un CloudEvent.
    for basura in [
        &b"esto no es json"[..],
        b"[]",
        b"{}",
        br#"{"stream":"X","seq":1}"#,
    ] {
        let b = basura.to_vec();
        let tarea = responder_en_bruto(&broker.url(), OBTENER, move |_| b.clone()).await;
        let e = llama
            .request_with(
                OBTENER,
                &json!({ "clienteId": "cli-1" }),
                RequestOptions::default().with_timeout_ms(2_000),
            )
            .await
            .unwrap_err();
        tarea.abort();
        assert_eq!(
            code(&e),
            "RPC_INVALID_REPLY",
            "{}: {e}",
            String::from_utf8_lossy(basura)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Un error con `retryAfterMs` en 5000.0 (lo escribe un serializador ajeno) sigue siendo un entero.
#[tokio::test]
async fn algoritmo_un_error_ajeno_bien_formado_se_traduce_a_su_clase() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let llama = bus(&broker.url(), "llama").await;
    let tarea = responder_en_bruto(&broker.url(), OBTENER, |peticion| {
        let mut e = respuesta_buena(peticion);
        e.event_type = "com.flux.rpc.error.v1".into();
        e.dataschema = "https://schemas.internal/system/rpc/error/1.0.0.json".into();
        e.data = serde_json::from_str(r#"{"class":"retryable","code":"CUPO_AGOTADO","message":"sin cupo","retryAfterMs":2500}"#).unwrap();
        bytes_de(&e)
    })
    .await;
    let e = llama
        .request(OBTENER, &json!({ "clienteId": "cli-1" }))
        .await
        .unwrap_err();
    tarea.abort();
    assert_eq!(
        (e.class(), e.code()),
        (Some(ErrorClass::Retryable), Some("CUPO_AGOTADO")),
        "{e}"
    );
    assert_eq!(e.retry_after(), Some(Duration::from_millis(2_500)));
}

/// Una petición ilegible recibe un error de plataforma: `correlationid` propio y `tenantid: system` (§5.2), y
/// una sin subject de respuesta no se atiende. Las dos cuentan como `poison`: es lo que debe despertar a alguien.
#[tokio::test]
async fn algoritmo_una_peticion_ilegible_o_sin_reply_es_poison() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let metricas = Arc::new(InMemoryMetrics::new());
    let (log, lineas) = logger_espia();
    let crm = bus_con(&broker.url(), "crm", |o| {
        o.with_metrics(metricas.clone()).with_logger(log)
    })
    .await;
    let ejecutadas = Arc::new(AtomicUsize::new(0));
    let e2 = Arc::clone(&ejecutadas);
    crm.respond(DESCUENTO, move |_: RpcRequest, _: RpcContext| {
        let e2 = Arc::clone(&e2);
        async move {
            e2.fetch_add(1, Ordering::SeqCst);
            Ok::<Value, flux::HandlerError>(json!({ "descuentoCents": 1, "moneda": "EUR" }))
        }
    })
    .await
    .unwrap();
    let ops = bruto(&broker.url()).await;
    let mut bandeja = ops.subscribe("_INBOX.ops.pruebas.>").await.unwrap();
    barrera(&ops).await;

    // Ilegible, con reply: un error de plataforma.
    let subject = format!("rpc.{DESCUENTO}");
    ops.publish_with_reply(
        subject.clone(),
        "_INBOX.ops.pruebas.uno".to_string(),
        r#"{"id":"peti-9","specversion":"1.0"}"#.into(),
    )
    .await
    .unwrap();
    let m = tokio::time::timeout(Duration::from_secs(2), bandeja.next())
        .await
        .expect("se contesta, no en silencio")
        .unwrap();
    let error = flux::parse_event(&m.payload).expect("el error es un CloudEvent válido");
    assert_eq!(error.event_type, "com.flux.rpc.error.v1");
    assert_eq!(error.tenantid, "system");
    assert_eq!(
        error.correlationid, error.id,
        "sin petición que heredar, su propio id"
    );
    assert_eq!(
        error.causationid.as_deref(),
        Some("peti-9"),
        "el id legible de la petición ilegible"
    );
    let datos: Value = serde_json::from_str(error.data.get()).unwrap();
    assert_eq!(datos["class"], "permanent", "poison no viaja");
    assert!(
        datos["code"]
            .as_str()
            .unwrap()
            .starts_with("MISSING_REQUIRED"),
        "{datos}"
    );

    // Sin subject de respuesta: no se atiende.
    ops.publish(subject, "{}".into()).await.unwrap();
    ops.flush().await.unwrap();
    espera(
        || metricas.counter("flux_rpc_server_requests_total{operation=\"clientes.cliente.v1.calcular-descuento\",outcome=\"poison\"}") == Some(2),
        Duration::from_secs(2),
        "las dos peticiones cuentan como poison",
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), bandeja.next())
            .await
            .is_err(),
        "no se contesta a quien no espera"
    );
    assert_eq!(ejecutadas.load(Ordering::SeqCst), 0, "el handler no corrió");
    assert!(
        lock(&lineas)
            .iter()
            .any(|l| l.contains("SIN subject de respuesta")),
        "{:?}",
        lock(&lineas)
    );
}

/// Hasta ~256 handlers a la vez por operación, y el exceso espera en la cola de la suscripción — §5.6.
#[tokio::test]
async fn algoritmo_los_handlers_concurrentes_estan_acotados() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let llama = bus(&broker.url(), "llama").await;
    for (limite, peticiones) in [(MAX_POR_DEFECTO, 400_usize), (4, 24)] {
        let crm = bus(&broker.url(), "crm").await;
        let en_vuelo = Arc::new(AtomicUsize::new(0));
        let maximo = Arc::new(AtomicUsize::new(0));
        let (v2, m2) = (Arc::clone(&en_vuelo), Arc::clone(&maximo));
        let opciones = if limite == MAX_POR_DEFECTO {
            RespondOptions::default()
        } else {
            RespondOptions::default().with_max_concurrent(limite)
        };
        crm.respond_with(
            OBTENER,
            move |_: RpcRequest, _: RpcContext| {
                let (v2, m2) = (Arc::clone(&v2), Arc::clone(&m2));
                async move {
                    let ahora = v2.fetch_add(1, Ordering::SeqCst) + 1;
                    m2.fetch_max(ahora, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    v2.fetch_sub(1, Ordering::SeqCst);
                    Ok::<Value, flux::HandlerError>(json!({ "clienteId": "cli-1" }))
                }
            },
            opciones,
        )
        .await
        .unwrap();
        let tareas: Vec<_> = (0..peticiones)
            .map(|_| {
                let llama = llama.clone();
                tokio::spawn(async move {
                    llama
                        .request_with(
                            OBTENER,
                            &json!({ "clienteId": "cli-1" }),
                            RequestOptions::default().with_timeout_ms(20_000),
                        )
                        .await
                })
            })
            .collect();
        for t in tareas {
            t.await
                .unwrap()
                .expect("todas se atienden: el exceso espera, no falla");
        }
        let visto = maximo.load(Ordering::SeqCst);
        assert!(
            visto <= limite,
            "límite {limite}: hubo {visto} handlers a la vez"
        );
        assert!(
            visto >= limite.min(peticiones) / 2,
            "el límite se usa de verdad: {visto} de {limite}"
        );
        crm.close().await.unwrap();
    }
}

const MAX_POR_DEFECTO: usize = flux::MAX_CONCURRENT_HANDLERS;

/// Las tres métricas, con las etiquetas de `protocol.json`, y **nunca** el tenant, el `id` ni quién llama.
#[tokio::test]
async fn metricas_las_tres_series_con_sus_outcomes_y_sin_etiquetas_prohibidas() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let del_dueno = Arc::new(InMemoryMetrics::new());
    let del_cliente = Arc::new(InMemoryMetrics::new());
    let crm = bus_con(&broker.url(), "crm", |o| o.with_metrics(del_dueno.clone())).await;
    crm.respond(
        DESCUENTO,
        |peticion: RpcRequest, _: RpcContext| async move {
            let d: Value = peticion.data_as()?;
            let resultado: Result<Value, flux::HandlerError> = match d["campana"].as_str() {
                Some("retryable") => Err(flux::FluxError::retryable("luego")
                    .with_code("OCUPADO")
                    .into()),
                Some("permanent") => Err(flux::FluxError::permanent("no").with_code("NO").into()),
                Some("desconocido") => Err("raro".into()),
                _ => Ok(json!({ "descuentoCents": 1, "moneda": "EUR" })),
            };
            resultado
        },
    )
    .await
    .unwrap();
    let llama = bus_con(&broker.url(), "llama", |o| {
        o.with_metrics(del_cliente.clone()).with_tenant_id("acme")
    })
    .await;
    let ok = |c: &'static str| json!({ "clienteId": "cli-1", "campana": c });
    llama.request(DESCUENTO, &ok("normal")).await.unwrap();
    llama.request(DESCUENTO, &ok("normal")).await.unwrap();
    let _ = llama.request(DESCUENTO, &ok("retryable")).await;
    let _ = llama.request(DESCUENTO, &ok("permanent")).await;
    let _ = llama.request(DESCUENTO, &ok("desconocido")).await;
    let _ = llama
        .request(OBTENER, &json!({ "clienteId": "cli-1" }))
        .await; // nadie: unavailable
    let _ = llama
        .request_with(
            DESCUENTO,
            &ok("normal"),
            RequestOptions::default().with_timeout_ms(1),
        )
        .await; // el 1 ms casi siempre agota: timeout u ok
    let _ = bus(&broker.url(), "ajeno")
        .await
        .request(OBTENER, &json!({ "clienteId": "cli-1" }))
        .await;

    let c = |m: &InMemoryMetrics, serie: &str| m.counter(serie).unwrap_or(0);
    let cliente = |outcome: &str| {
        format!("flux_rpc_client_requests_total{{operation=\"{DESCUENTO}\",outcome=\"{outcome}\"}}")
    };
    let servidor = |outcome: &str| {
        format!("flux_rpc_server_requests_total{{operation=\"{DESCUENTO}\",outcome=\"{outcome}\"}}")
    };
    espera(
        || c(&del_dueno, &servidor("ok")) >= 2,
        Duration::from_secs(2),
        "el dueño cuenta los ok",
    )
    .await;
    assert!(c(&del_cliente, &cliente("ok")) >= 2);
    assert_eq!(c(&del_cliente, &cliente("retryable")), 1);
    assert_eq!(
        c(&del_cliente, &cliente("permanent")),
        2,
        "el permanent del dueño y el desconocido, que es permanent"
    );
    assert_eq!(c(&del_dueno, &servidor("retryable")), 1);
    assert_eq!(
        c(&del_dueno, &servidor("permanent")),
        2,
        "el desconocido también es permanent"
    );
    assert_eq!(
        c(
            &del_cliente,
            &format!(
                "flux_rpc_client_requests_total{{operation=\"{OBTENER}\",outcome=\"unavailable\"}}"
            )
        ),
        1
    );
    let render = del_cliente.render();
    let render_dueno = del_dueno.render();
    for texto in [&render, &render_dueno] {
        assert!(
            texto.contains("# TYPE flux_rpc_duration_seconds histogram"),
            "{texto}"
        );
        // Nunca el tenant, el id, el correlationid ni quién llama como etiqueta.
        for prohibida in [
            "tenant",
            "acme",
            "correlation",
            "caller",
            "llama\"",
            "crm\"",
            "01924",
        ] {
            assert!(
                !texto
                    .lines()
                    .filter(|l| l.contains("flux_rpc_"))
                    .any(|l| l.contains(prohibida)),
                "etiqueta prohibida `{prohibida}`:\n{texto}"
            );
        }
    }
    assert!(
        render.contains(&format!(
            "flux_rpc_duration_seconds_count{{operation=\"{DESCUENTO}\",role=\"client\"}}"
        )),
        "{render}"
    );
    assert!(
        render_dueno.contains(&format!(
            "flux_rpc_duration_seconds_count{{operation=\"{DESCUENTO}\",role=\"server\"}}"
        )),
        "{render_dueno}"
    );
    // El denegado es `permanent` en el cliente y NO cuenta como `timeout`.
    assert_eq!(
        c(
            &del_cliente,
            &format!(
                "flux_rpc_client_requests_total{{operation=\"{OBTENER}\",outcome=\"timeout\"}}"
            )
        ),
        0
    );
}

/// Los códigos de la capacidad, por su literal: es lo que `scripts/check-sdk-codes.mjs` exige de la evidencia.
#[test]
fn codigos_estables() {
    assert_eq!(flux::RPC_UNAVAILABLE_CODE, "RPC_UNAVAILABLE");
    assert_eq!(flux::RPC_TIMEOUT_CODE, "RPC_TIMEOUT");
    assert_eq!(flux::STALE_REQUEST_CODE, "STALE_REQUEST");
    assert_eq!(flux::RPC_INVALID_REPLY_CODE, "RPC_INVALID_REPLY");
    assert_eq!(flux::RPC_MESSAGE_TOO_LARGE_CODE, "RPC_MESSAGE_TOO_LARGE");
    assert_eq!(flux::INVALID_RPC_CALL_CODE, "INVALID_RPC_CALL");
    // Los que reutiliza y no define.
    assert_eq!(flux::RESERVED_DOMAIN_CODE, "RESERVED_DOMAIN");
    assert_eq!(flux::PERMISSION_DENIED_CODE, "PERMISSION_DENIED");
    assert_eq!(flux::INVALID_SCHEMA_CODE, "INVALID_SCHEMA");
    // Y las constantes de la capacidad.
    assert_eq!(
        (
            flux::DEFAULT_TIMEOUT_MS,
            flux::MIN_TIMEOUT_MS,
            flux::MAX_TIMEOUT_MS
        ),
        (5_000, 1, 25_000)
    );
    assert_eq!(flux::STALE_REQUEST_WINDOW_SECS, 300);
}

#[allow(dead_code)]
fn sin_usar(_: HashMap<String, String>, _: HashSet<String>) {}

// ═════════════════════════════════════════════════════════════════════════════
//  Firma Ed25519 y ventana de 300 s — §7.4 (feature `signing`)
// ═════════════════════════════════════════════════════════════════════════════

#[cfg(feature = "signing")]
mod firma {
    use super::*;
    use flux::{RpcRequestInput, RpcSide, Signer, SigningOptions, VerificationMode};

    /// Las claves de dos servicios y lo que cada uno necesita saber del otro.
    struct Claves {
        llama_priv: String,
        llama_pub: String,
        crm_priv: String,
        crm_pub: String,
    }

    fn claves() -> Claves {
        let (llama_priv, llama_pub) = flux::generate_key_pair();
        let (crm_priv, crm_pub) = flux::generate_key_pair();
        Claves {
            llama_priv,
            llama_pub,
            crm_priv,
            crm_pub,
        }
    }

    fn firma_de(
        servicio: &str,
        propia: &str,
        ajena: (&str, &str),
        modo: VerificationMode,
    ) -> SigningOptions {
        SigningOptions::default()
            .with_private_key(propia, format!("{servicio}-1"))
            .with_public_key(ajena.0, ajena.1)
            .with_verify(modo)
    }

    /// Un `llama` y un `crm` que se firman y se verifican en `require`.
    async fn par(broker: &Broker, k: &Claves, metricas_crm: Arc<InMemoryMetrics>) -> (Bus, Bus) {
        let crm_firma = firma_de(
            "crm",
            &k.crm_priv,
            ("llama-1", &k.llama_pub),
            VerificationMode::Require,
        );
        let crm = bus_con(&broker.url(), "crm", |o| {
            o.with_signing(crm_firma).with_metrics(metricas_crm)
        })
        .await;
        let llama_firma = firma_de(
            "llama",
            &k.llama_priv,
            ("crm-1", &k.crm_pub),
            VerificationMode::Require,
        );
        let llama = bus_con(&broker.url(), "llama", |o| o.with_signing(llama_firma)).await;
        (llama, crm)
    }

    /// Una petición **firmada a mano** con el reloj que se quiera —lo que el SDK no hace: su `time` es el de
    /// construirla—, publicada en bruto, y el error o la respuesta que contesta quien responde.
    async fn peticion_firmada_a_mano(
        broker: &Broker,
        k: &Claves,
        segundos: i64,
        firmar: bool,
    ) -> flux::Event {
        let ops = bruto(&broker.url()).await;
        let mut bandeja = ops.subscribe("_INBOX.ops.firma.>").await.unwrap();
        barrera(&ops).await;
        let id = uuid_nuevo();
        let event = flux::build_rpc_request(RpcRequestInput {
            operation: DESCUENTO.into(),
            tenant_id: "acme".into(),
            aggregate_id: None,
            id: id.clone(),
            source: "/produccion/llama".into(),
            time: chrono_ahora(segundos),
            correlationid: id,
            causationid: None,
            producerversion: "1.0.0".into(),
            classification: DataClassification::Internal,
            dataschema: flux::rpc_dataschema(BASE, DESCUENTO, RpcSide::Request).unwrap(),
            traceparent: None,
            tracestate: None,
            data: serde_json::value::to_raw_value(
                &json!({ "clienteId": "cli-1", "totalCents": 1, "moneda": "EUR" }),
            )
            .unwrap(),
        })
        .unwrap();
        let event = if firmar {
            Signer::new(&SigningOptions::default().with_private_key(&k.llama_priv, "llama-1"))
                .unwrap()
                .expect("firmante")
                .sign(event)
                .unwrap()
        } else {
            event
        };
        ops.publish_with_reply(
            format!("rpc.{DESCUENTO}"),
            "_INBOX.ops.firma.una".to_string(),
            flux::serialize(&event).unwrap().into(),
        )
        .await
        .unwrap();
        let m = tokio::time::timeout(Duration::from_secs(2), bandeja.next())
            .await
            .expect("quien responde contesta SIEMPRE")
            .unwrap();
        flux::parse_event(&m.payload).expect("lo que contesta es un CloudEvent válido")
    }

    fn chrono_ahora(desplazamiento_s: i64) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now() + chrono::Duration::seconds(desplazamiento_s)
    }

    fn error_de(e: &flux::Event) -> Value {
        assert_eq!(e.event_type, "com.flux.rpc.error.v1", "{e:?}");
        serde_json::from_str(e.data.get()).unwrap()
    }

    async fn dueno_ok(crm: &Bus) -> Arc<AtomicUsize> {
        let ejecutadas = Arc::new(AtomicUsize::new(0));
        let e2 = Arc::clone(&ejecutadas);
        crm.respond(DESCUENTO, move |_: RpcRequest, _: RpcContext| {
            let e2 = Arc::clone(&e2);
            async move {
                e2.fetch_add(1, Ordering::SeqCst);
                Ok::<Value, flux::HandlerError>(json!({ "descuentoCents": 5, "moneda": "EUR" }))
            }
        })
        .await
        .unwrap();
        ejecutadas
    }

    /// Los dos sentidos firmados y verificados en `require`: la respuesta y el error van firmados por quien
    /// responde, y quien llama los verifica.
    #[tokio::test]
    async fn firma_el_ciclo_completo_en_require_firma_y_verifica_en_los_dos_sentidos() {
        let bin = broker_bin!();
        let broker = Broker::con_acl(&bin);
        let k = claves();
        let metricas = Arc::new(InMemoryMetrics::new());
        let (llama, crm) = par(&broker, &k, metricas.clone()).await;
        dueno_ok(&crm).await;
        let r = llama
            .request(
                DESCUENTO,
                &json!({ "clienteId": "cli-1", "totalCents": 1, "moneda": "EUR" }),
            )
            .await
            .expect("firmado y verificado en los dos sentidos");
        assert_eq!(
            r.signkeyid.as_deref(),
            Some("crm-1"),
            "la respuesta la firma quien responde"
        );
        assert!(r.signature.is_some());
        // Y un error del handler también viaja firmado y se verifica.
        let otro = bus_con(&broker.url(), "crm-exp", |o| {
            o.with_signing(firma_de(
                "crm-exp",
                &k.crm_priv,
                ("llama-1", &k.llama_pub),
                VerificationMode::Require,
            ))
        })
        .await;
        otro.respond(OBTENER, |_: RpcRequest, _: RpcContext| async move {
            Err::<Value, flux::HandlerError>(
                flux::FluxError::permanent("no").with_code("NO").into(),
            )
        })
        .await
        .unwrap();
        // `crm-exp` firma con `crm-exp-1`, que `llama` no conoce: UNKNOWN_SIGNING_KEY, con el código de la firma.
        let e = llama
            .request(OBTENER, &json!({ "clienteId": "cli-1" }))
            .await
            .unwrap_err();
        assert_eq!(code(&e), "UNKNOWN_SIGNING_KEY", "{e}");
        assert_eq!(
            e.class(),
            Some(ErrorClass::Permanent),
            "para quien llama un fallo de firma es permanent, no poison"
        );
        assert_eq!(
            metricas.counter(&format!(
                "flux_rpc_server_requests_total{{operation=\"{DESCUENTO}\",outcome=\"ok\"}}"
            )),
            Some(1)
        );
    }

    /// En `require`, quien responde rechaza una petición sin firma o con una firma que no es de quien dice.
    #[tokio::test]
    async fn firma_una_peticion_sin_firma_o_con_firma_ajena_se_rechaza_con_el_codigo_de_la_firma() {
        let bin = broker_bin!();
        let broker = Broker::con_acl(&bin);
        let k = claves();
        let metricas = Arc::new(InMemoryMetrics::new());
        let (_llama, crm) = par(&broker, &k, metricas.clone()).await;
        let ejecutadas = dueno_ok(&crm).await;
        let sin_firma = peticion_firmada_a_mano(&broker, &k, 0, false).await;
        let datos = error_de(&sin_firma);
        assert_eq!(
            (datos["class"].as_str(), datos["code"].as_str()),
            (Some("permanent"), Some("MISSING_SIGNATURE")),
            "{datos}"
        );
        assert_eq!(ejecutadas.load(Ordering::SeqCst), 0, "el handler no corrió");
        espera(
            || metricas.counter(&format!("flux_rpc_server_requests_total{{operation=\"{DESCUENTO}\",outcome=\"invalid_signature\"}}")) == Some(1),
            Duration::from_secs(2),
            "el dueño no contó invalid_signature",
        )
        .await;
        // Firmada con la clave de OTRO servicio pero con el signkeyid de `llama`: la firma no verifica.
        let k2 = claves();
        let con_otra_clave = Claves {
            llama_priv: k2.llama_priv.clone(),
            ..claves()
        };
        let falsa = peticion_firmada_a_mano(&broker, &con_otra_clave, 0, true).await;
        assert_eq!(error_de(&falsa)["code"], "INVALID_SIGNATURE");
    }

    /// La ventana de 300 s, simétrica y solo con `require`: una petición firmada pero vieja —una captura
    /// reenviada— se rechaza con `STALE_REQUEST`; una de 290 s se acepta; una del futuro también se rechaza.
    #[tokio::test]
    async fn firma_la_ventana_de_300_s_rechaza_lo_viejo_y_lo_futuro_y_acepta_lo_reciente() {
        let bin = broker_bin!();
        let broker = Broker::con_acl(&bin);
        let k = claves();
        let metricas = Arc::new(InMemoryMetrics::new());
        let (_llama, crm) = par(&broker, &k, metricas.clone()).await;
        let ejecutadas = dueno_ok(&crm).await;
        for (segundos, esperado_stale) in [
            (-600, true),
            (-310, true),
            (400, true),
            (-290, false),
            (250, false),
            (0, false),
        ] {
            let respuesta = peticion_firmada_a_mano(&broker, &k, segundos, true).await;
            if esperado_stale {
                let datos = error_de(&respuesta);
                assert_eq!(
                    (datos["class"].as_str(), datos["code"].as_str()),
                    (Some("permanent"), Some("STALE_REQUEST")),
                    "{segundos} s: {datos}"
                );
                let m = datos["message"].as_str().unwrap();
                assert!(m.contains("300") && m.contains("NTP"), "{m}");
            } else {
                assert_eq!(
                    respuesta.event_type,
                    "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1",
                    "{segundos} s dentro de la ventana: se atiende"
                );
                assert_eq!(
                    respuesta.signkeyid.as_deref(),
                    Some("crm-1"),
                    "la respuesta va firmada"
                );
            }
        }
        assert_eq!(
            ejecutadas.load(Ordering::SeqCst),
            3,
            "solo lo que estaba dentro de la ventana llegó al handler"
        );
        let stale = format!(
            "flux_rpc_server_requests_total{{operation=\"{DESCUENTO}\",outcome=\"stale_request\"}}"
        );
        espera(
            || metricas.counter(&stale) == Some(3),
            Duration::from_secs(2),
            "el dueño no contó stale_request",
        )
        .await;
    }

    /// Sin `require` no hay ventana: un `time` es un campo que cualquiera escribe y no protegería nada.
    #[tokio::test]
    async fn firma_sin_require_no_hay_ventana() {
        let bin = broker_bin!();
        let broker = Broker::con_acl(&bin);
        let k = claves();
        for modo in [VerificationMode::Warn, VerificationMode::Off] {
            let firma = firma_de("crm", &k.crm_priv, ("llama-1", &k.llama_pub), modo);
            let crm = bus_con(&broker.url(), "crm", |o| o.with_signing(firma)).await;
            let r = crm
                .respond(DESCUENTO, |_: RpcRequest, _: RpcContext| async move {
                    Ok::<Value, flux::HandlerError>(json!({ "descuentoCents": 5, "moneda": "EUR" }))
                })
                .await
                .unwrap();
            let vieja = peticion_firmada_a_mano(&broker, &k, -3_600, false).await;
            assert_eq!(
                vieja.event_type, "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1",
                "{modo:?}"
            );
            r.stop().await;
            crm.close().await.unwrap();
        }
    }

    /// Quien llama verifica la respuesta: una sin firma, con firma de otra clave o con una clave desconocida
    /// no se acepta, y el código es el de la firma.
    #[tokio::test]
    async fn firma_quien_llama_rechaza_una_respuesta_sin_firma_o_con_firma_falsa() {
        let bin = broker_bin!();
        let broker = Broker::con_acl(&bin);
        let k = claves();
        let llama_firma = firma_de(
            "llama",
            &k.llama_priv,
            ("crm-1", &k.crm_pub),
            VerificationMode::Require,
        );
        let llama = bus_con(&broker.url(), "llama", |o| o.with_signing(llama_firma)).await;
        let (otra_priv, _) = flux::generate_key_pair();
        type Caso<'a> = (&'a str, Option<(String, &'a str)>, &'a str);
        let casos: Vec<Caso<'_>> = vec![
            ("sin firma", None, "MISSING_SIGNATURE"),
            (
                "firmada con otra clave bajo el id de `crm`",
                Some((otra_priv.clone(), "crm-1")),
                "INVALID_SIGNATURE",
            ),
            (
                "firmada con una clave que no se conoce",
                Some((otra_priv, "intruso-1")),
                "UNKNOWN_SIGNING_KEY",
            ),
            (
                "firmada bien por `crm`",
                Some((k.crm_priv.clone(), "crm-1")),
                "",
            ),
        ];
        for (nombre, firmante, esperado) in casos {
            let tarea = responder_en_bruto(&broker.url(), OBTENER, move |peticion| {
                let e = respuesta_buena(peticion);
                let e = match &firmante {
                    Some((privada, id)) => {
                        Signer::new(&SigningOptions::default().with_private_key(privada, *id))
                            .unwrap()
                            .unwrap()
                            .sign(e)
                            .unwrap()
                    }
                    None => e,
                };
                bytes_de(&e)
            })
            .await;
            let r = llama
                .request_with(
                    OBTENER,
                    &json!({ "clienteId": "cli-1" }),
                    RequestOptions::default().with_timeout_ms(2_000),
                )
                .await;
            tarea.abort();
            if esperado.is_empty() {
                assert!(r.is_ok(), "{nombre}: {r:?}");
            } else {
                let e = r.expect_err(nombre);
                assert_eq!(code(&e), esperado, "{nombre}: {e}");
                assert_eq!(e.class(), Some(ErrorClass::Permanent), "{nombre}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

// ═════════════════════════════════════════════════════════════════════════════
//  Validación L3 con el bundle del repositorio — §5.2, §11 (feature `validation`)
// ═════════════════════════════════════════════════════════════════════════════

#[cfg(feature = "validation")]
mod esquemas {
    use super::*;
    use flux::validation::{SchemaBundle, ValidationMode, ValidationOptions};

    /// El bundle REAL del repositorio, con su índice `rpc`: si `bundle-schemas.mjs` cambia de forma, esto se entera.
    fn bundle() -> SchemaBundle {
        SchemaBundle::from_json(include_str!("../../schemas/bundle.json")).expect("bundle válido")
    }

    fn estricto() -> ValidationOptions {
        ValidationOptions::strict(bundle())
    }

    #[test]
    fn el_bundle_trae_el_indice_rpc_con_el_par_de_cada_operacion() {
        let b = bundle();
        let u = b.rpc_uris_for(DESCUENTO).expect("índice `rpc`");
        assert_eq!(
            u.request,
            "https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json"
        );
        assert_eq!(
            u.reply,
            "https://schemas.internal/rpc/clientes/cliente/calcular-descuento/reply/1.0.0.json"
        );
        assert!(b
            .schemas
            .contains_key("https://schemas.internal/system/rpc/error/1.0.0.json"));
        assert!(b.rpc_uris_for("clientes.cliente.v1.nueva").is_none());
        assert!(
            b.rpc.keys().all(|k| !k.starts_with("rpc.")),
            "el índice usa el nombre de la operación, sin `rpc.`"
        );
    }

    fn valida(
        servicio: &str,
        url: &str,
        modo: ValidationMode,
        metricas: Option<Arc<InMemoryMetrics>>,
    ) -> (String, String, ValidationMode, Option<Arc<InMemoryMetrics>>) {
        (servicio.to_string(), url.to_string(), modo, metricas)
    }

    async fn con_validacion(
        v: (String, String, ValidationMode, Option<Arc<InMemoryMetrics>>),
    ) -> Bus {
        let (servicio, url, modo, metricas) = v;
        let opciones = match modo {
            ValidationMode::Strict => estricto(),
            ValidationMode::Warn => ValidationOptions::warn(bundle()),
            ValidationMode::Off => ValidationOptions::default(),
        };
        bus_con(&url, &servicio, |o| {
            let o = o.with_validation(opciones);
            match metricas {
                Some(m) => o.with_metrics(m),
                None => o,
            }
        })
        .await
    }

    /// Quien llama valida ANTES de construir el sobre y de abrir nada: todos los errores, ningún valor.
    #[tokio::test]
    async fn validacion_una_peticion_invalida_falla_en_quien_llama_con_todas_las_rutas_y_ningun_valor(
    ) {
        let bin = broker_bin!();
        let broker = Broker::con_acl(&bin);
        let metricas = Arc::new(InMemoryMetrics::new());
        let llama = con_validacion(valida(
            "llama",
            &broker.url(),
            ValidationMode::Strict,
            Some(metricas.clone()),
        ))
        .await;
        let espia = bruto(&broker.url()).await;
        let mut cable = espia.subscribe("rpc.>").await.unwrap();
        barrera(&espia).await;
        let e = llama
            .request(DESCUENTO, &json!({ "clienteId": "cli-1", "totalCents": "SECRETO-9990", "extra": "SECRETO-X" }))
            .await
            .unwrap_err();
        assert_eq!(code(&e), "INVALID_SCHEMA", "{e}");
        assert_eq!(e.class(), Some(ErrorClass::Permanent));
        let msg = e.to_string();
        assert!(msg.contains("/totalCents"), "cita la ruta: {msg}");
        assert!(msg.contains("moneda"), "y TODAS: falta `moneda`: {msg}");
        assert!(!msg.contains("SECRETO"), "nunca un valor: {msg}");
        assert!(
            tokio::time::timeout(Duration::from_millis(150), cable.next())
                .await
                .is_err(),
            "no salió nada al cable"
        );
        assert_eq!(
            metricas.counter(&format!("flux_rpc_client_requests_total{{operation=\"{DESCUENTO}\",outcome=\"invalid_schema\"}}")),
            Some(1)
        );
    }

    /// Quien responde rechaza con TODAS las rutas y SIN invocar al handler; un valor sensible no llega al log del que llama.
    #[tokio::test]
    async fn validacion_quien_responde_rechaza_una_peticion_invalida_sin_invocar_al_handler() {
        let bin = broker_bin!();
        let broker = Broker::con_acl(&bin);
        let metricas = Arc::new(InMemoryMetrics::new());
        let crm = con_validacion(valida(
            "crm",
            &broker.url(),
            ValidationMode::Strict,
            Some(metricas.clone()),
        ))
        .await;
        let ejecutadas = Arc::new(AtomicUsize::new(0));
        let e2 = Arc::clone(&ejecutadas);
        crm.respond(DESCUENTO, move |_: RpcRequest, _: RpcContext| {
            let e2 = Arc::clone(&e2);
            async move {
                e2.fetch_add(1, Ordering::SeqCst);
                Ok::<Value, flux::HandlerError>(json!({ "descuentoCents": 1, "moneda": "EUR" }))
            }
        })
        .await
        .unwrap();
        // `llama` sin validación: la petición inválida SALE, y es quien responde quien la para.
        let llama = bus(&broker.url(), "llama").await;
        let e = llama
            .request(
                DESCUENTO,
                &json!({ "clienteId": "cli-1", "totalCents": "SECRETO-9990", "moneda": "eur" }),
            )
            .await
            .unwrap_err();
        assert_eq!(
            (e.class(), e.code()),
            (Some(ErrorClass::Permanent), Some("INVALID_SCHEMA")),
            "{e}"
        );
        let msg = e.to_string();
        assert!(msg.starts_with("la petición no cumple https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json"), "{msg}");
        assert!(
            msg.contains("/totalCents") && msg.contains("/moneda"),
            "TODAS las rutas: {msg}"
        );
        assert!(
            !msg.contains("SECRETO") && !msg.contains("eur"),
            "ningún valor: {msg}"
        );
        assert_eq!(
            ejecutadas.load(Ordering::SeqCst),
            0,
            "el handler NO se invoca"
        );
        espera(
            || metricas.counter(&format!("flux_rpc_server_requests_total{{operation=\"{DESCUENTO}\",outcome=\"invalid_schema\"}}")) == Some(1),
            Duration::from_secs(2),
            "el dueño no contó invalid_schema",
        )
        .await;
    }

    /// La respuesta del handler que no cumple SU esquema es un fallo de quien responde, y el mensaje lo dice.
    #[tokio::test]
    async fn validacion_una_respuesta_invalida_es_un_fallo_de_quien_responde() {
        let bin = broker_bin!();
        let broker = Broker::con_acl(&bin);
        let crm = con_validacion(valida("crm", &broker.url(), ValidationMode::Strict, None)).await;
        crm.respond(DESCUENTO, |_: RpcRequest, _: RpcContext| async move {
            Ok::<Value, flux::HandlerError>(json!({ "descuentoCents": "mucho", "moneda": "EUR" }))
        })
        .await
        .unwrap();
        let llama = bus(&broker.url(), "llama").await;
        let e = llama
            .request(
                DESCUENTO,
                &json!({ "clienteId": "cli-1", "totalCents": 9990, "moneda": "EUR" }),
            )
            .await
            .unwrap_err();
        assert_eq!(
            (e.class(), e.code()),
            (Some(ErrorClass::Permanent), Some("INVALID_SCHEMA")),
            "{e}"
        );
        let msg = e.to_string();
        assert!(
            msg.starts_with("la respuesta no cumple"),
            "dice de QUIÉN es el fallo: {msg}"
        );
        assert!(
            msg.contains("/descuentoCents") && !msg.contains("mucho"),
            "{msg}"
        );
    }

    /// El ciclo bueno, con L3 en los dos lados, y los `dataschema` que salen del índice `rpc` del bundle.
    #[tokio::test]
    async fn validacion_el_ciclo_valido_usa_los_dataschema_del_indice_rpc() {
        let bin = broker_bin!();
        let broker = Broker::con_acl(&bin);
        let crm = con_validacion(valida("crm", &broker.url(), ValidationMode::Strict, None)).await;
        crm.respond(DESCUENTO, |_: RpcRequest, _: RpcContext| async move {
            Ok::<Value, flux::HandlerError>(
                json!({ "descuentoCents": 500, "moneda": "EUR", "motivo": "Campaña de otoño" }),
            )
        })
        .await
        .unwrap();
        let llama =
            con_validacion(valida("llama", &broker.url(), ValidationMode::Strict, None)).await;
        let espia = bruto(&broker.url()).await;
        let mut cable = espia.subscribe(format!("rpc.{DESCUENTO}")).await.unwrap();
        barrera(&espia).await;
        let r = llama
            .request(DESCUENTO, &json!({ "clienteId": "cli-1", "totalCents": 9990, "moneda": "EUR", "campana": "Otoño" }))
            .await
            .expect("válido en los dos lados");
        assert_eq!(r.data_as::<Value>().unwrap()["descuentoCents"], 500);
        assert_eq!(
            r.dataschema,
            "https://schemas.internal/rpc/clientes/cliente/calcular-descuento/reply/1.0.0.json"
        );
        let visto = tokio::time::timeout(Duration::from_secs(1), cable.next())
            .await
            .unwrap()
            .unwrap();
        let peticion = flux::parse_event(&visto.payload).unwrap();
        assert_eq!(
            peticion.dataschema,
            "https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json"
        );
    }

    /// Quien llama valida lo que recibe contra el esquema de la respuesta, aunque el mensaje traiga otro `dataschema`.
    #[tokio::test]
    async fn validacion_quien_llama_valida_la_respuesta_ajena_contra_el_esquema_de_la_operacion() {
        let bin = broker_bin!();
        let broker = Broker::con_acl(&bin);
        let llama =
            con_validacion(valida("llama", &broker.url(), ValidationMode::Strict, None)).await;
        let tarea = responder_en_bruto(&broker.url(), DESCUENTO, |peticion| {
            let mut e = respuesta_buena(peticion);
            e.event_type = "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1".into();
            // Un `dataschema` que NO es el de la operación: se valida contra el índice, no contra lo que diga el mensaje.
            e.dataschema = "https://schemas.internal/otra/cosa/1.0.0.json".into();
            e.data =
                serde_json::value::to_raw_value(&json!({ "descuentoCents": -5, "moneda": "EUR" }))
                    .unwrap();
            bytes_de(&e)
        })
        .await;
        let e = llama
            .request(
                DESCUENTO,
                &json!({ "clienteId": "cli-1", "totalCents": 9990, "moneda": "EUR" }),
            )
            .await
            .unwrap_err();
        tarea.abort();
        assert_eq!(code(&e), "INVALID_SCHEMA", "{e}");
        assert!(matches!(e, FluxError::SchemaValidation(_)), "{e:?}");
        assert!(e.to_string().contains("/descuentoCents"), "{e}");
    }

    /// Un servicio desplegado con un bundle que no trae su operación falla AL ARRANCAR, no con la primera petición.
    #[tokio::test]
    async fn validacion_respond_con_un_bundle_sin_la_operacion_falla_al_arrancar() {
        let bin = broker_bin!();
        let broker = Broker::con_acl(&bin);
        let crm = con_validacion(valida("crm", &broker.url(), ValidationMode::Strict, None)).await;
        let e = crm
            .respond("clientes.cliente.v1.inventada", |_: RpcRequest, _: RpcContext| async move {
                Ok::<Value, flux::HandlerError>(json!({}))
            })
            .await
            .unwrap_err();
        assert_eq!(code(&e), "SCHEMA_NOT_FOUND", "{e}");
        assert!(
            e.to_string().contains("clientes.cliente.v1.inventada"),
            "{e}"
        );
        // En `warn` arranca, y avisa.
        let (log, lineas) = logger_espia();
        let b = bus_con(&broker.url(), "crm-exp", |o| {
            o.with_validation(ValidationOptions::warn(bundle()))
                .with_logger(log)
        })
        .await;
        b.respond("clientes.cliente.v1.inventada", |_: RpcRequest, _: RpcContext| async move { Ok::<Value, flux::HandlerError>(json!({})) })
            .await
            .expect("en warn arranca");
        assert!(
            lock(&lineas)
                .iter()
                .any(|l| l.contains("clientes.cliente.v1.inventada")),
            "{:?}",
            lock(&lineas)
        );
    }

    /// Con `warn`, el handler corre aunque la petición no cumpla su esquema, y el fallo se cuenta igual.
    #[tokio::test]
    async fn validacion_con_warn_el_handler_corre_y_se_cuenta_como_invalid_schema() {
        let bin = broker_bin!();
        let broker = Broker::con_acl(&bin);
        let metricas = Arc::new(InMemoryMetrics::new());
        let crm = con_validacion(valida(
            "crm",
            &broker.url(),
            ValidationMode::Warn,
            Some(metricas.clone()),
        ))
        .await;
        crm.respond(DESCUENTO, |_: RpcRequest, _: RpcContext| async move {
            Ok::<Value, flux::HandlerError>(json!({ "descuentoCents": 1, "moneda": "EUR" }))
        })
        .await
        .unwrap();
        let llama = bus(&broker.url(), "llama").await;
        let r = llama
            .request(
                DESCUENTO,
                &json!({ "clienteId": "cli-1", "totalCents": "no" }),
            )
            .await;
        assert!(r.is_ok(), "con warn la petición se atiende: {r:?}");
        let serie = format!("flux_rpc_server_requests_total{{operation=\"{DESCUENTO}\",outcome=\"invalid_schema\"}}");
        espera(
            || metricas.counter(&serie) == Some(1),
            Duration::from_secs(2),
            "el fallo de esquema no se contó",
        )
        .await;
    }
}

// ═════════════════════════════════════════════════════════════════════════════
//  Medidas: lo que la spec no había medido y el SDK usa (se ejecutan con `-- --ignored --nocapture`)
// ═════════════════════════════════════════════════════════════════════════════

fn percentil(mut v: Vec<f64>, p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i]
}

/// A-90: cuánto tarda en llegar el aviso de una suscripción denegada. `respond()` espera ese aviso hasta
/// `SUBSCRIBE_SETTLE` (100 ms): si llegara más tarde, un dueño sin permiso arrancaría «sano». El tiempo que
/// tarda `respond()` en FALLAR es exactamente la latencia del aviso (no espera el tope entero).
#[tokio::test]
async fn medida_la_denegacion_de_una_suscripcion_llega_en_milisegundos() {
    let bin = broker_bin!();
    let broker = Broker::sin_permiso(&bin, "crm", "subscribe");
    let crm = bus(&broker.url(), "crm").await;
    let mut tiempos = Vec::new();
    for _ in 0..100 {
        let t = Instant::now();
        let e = crm
            .respond(DESCUENTO, |_: RpcRequest, _: RpcContext| async move {
                Ok::<Value, flux::HandlerError>(json!({}))
            })
            .await
            .expect_err("denegado");
        assert_eq!(code(&e), "PERMISSION_DENIED", "{e}");
        tiempos.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    eprintln!(
        "A-90: aviso de suscripción denegada, 100 intentos: p50 {:.2} ms, p99 {:.2} ms, máx {:.2} ms (tope de espera: 100 ms)",
        percentil(tiempos.clone(), 0.5),
        percentil(tiempos.clone(), 0.99),
        percentil(tiempos.clone(), 1.0)
    );
    assert!(
        percentil(tiempos, 1.0) < 100.0,
        "el aviso tardó más que el tope de espera: respond() habría arrancado «sano»"
    );
}

/// A-72 (SDK): una petición denegada llega como `PERMISSION_DENIED` en milisegundos, con `async-nats`, que
/// por sí solo la dejaría en un timeout de 5 s.
#[tokio::test]
async fn medida_una_peticion_denegada_llega_como_permission_denied_en_milisegundos() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let ajeno = bus(&broker.url(), "ajeno").await;
    let mut tiempos = Vec::new();
    for _ in 0..100 {
        let t = Instant::now();
        let e = ajeno
            .request_with(
                OBTENER,
                &json!({ "clienteId": "cli-1" }),
                RequestOptions::default().with_timeout_ms(5_000),
            )
            .await
            .expect_err("denegado");
        assert_eq!(code(&e), "PERMISSION_DENIED", "{e}");
        tiempos.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    eprintln!(
        "A-72 (SDK): petición denegada → PERMISSION_DENIED, 100 intentos: p50 {:.2} ms, p99 {:.2} ms, máx {:.2} ms (el timeout era 5000 ms)",
        percentil(tiempos.clone(), 0.5),
        percentil(tiempos.clone(), 0.99),
        percentil(tiempos.clone(), 1.0)
    );
    assert!(percentil(tiempos, 1.0) < 1_000.0);
}

/// Copia de `origen` a `destino` retrasando cada trozo `retardo`, **conservando el orden**.
async fn copia_con_latencia<R, W>(mut origen: R, mut destino: W, retardo: Duration)
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
    tokio::spawn(async move {
        let mut b = vec![0u8; 65_536];
        loop {
            match origen.read(&mut b).await {
                Ok(n) if n > 0 => {
                    if tx
                        .send((tokio::time::Instant::now() + retardo, b[..n].to_vec()))
                        .is_err()
                    {
                        return;
                    }
                }
                _ => return,
            }
        }
    });
    while let Some((vence, datos)) = rx.recv().await {
        tokio::time::sleep_until(vence).await;
        if destino.write_all(&datos).await.is_err() {
            return;
        }
    }
}

/// Un proxy TCP que añade `un_sentido` de latencia en cada dirección entre un cliente y el servidor: es
/// lo que un nats-server local no puede dar. Devuelve el puerto en el que escucha.
async fn proxy_con_latencia(destino: u16, un_sentido: Duration) -> u16 {
    let puerto = free_port();
    let escucha = tokio::net::TcpListener::bind(("127.0.0.1", puerto))
        .await
        .unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((cliente, _)) = escucha.accept().await else {
                return;
            };
            let Ok(servidor) = tokio::net::TcpStream::connect(("127.0.0.1", destino)).await else {
                return;
            };
            let (cr, cw) = cliente.into_split();
            let (sr, sw) = servidor.into_split();
            tokio::spawn(copia_con_latencia(cr, sw, un_sentido));
            tokio::spawn(copia_con_latencia(sr, cw, un_sentido));
        }
    });
    puerto
}

/// A-91: parar un responder con carga CONTINUA y latencia de red real. `async-nats` 0.50 implementa
/// `Subscriber::drain` retirando la suscripción de su tabla en cuanto el `UNSUB` sale al socket, sin esperar
/// al `PONG` (`lib.rs`, `drain_pings`): lo que el servidor ya envió y aún viaja se descarta. Con un servidor
/// local no se ve; con latencia, cada petición en ese viaje de ida y vuelta es un timeout para quien llama.
///
/// La carga lleva *jitter*: con los ocho bucles sincronizados todas las peticiones salen a la vez y parar
/// entre dos ráfagas no pierde nada (así lo hizo la primera versión de esta medida).
///
/// `cargo test --test rpc -- --ignored --nocapture medida_parar`: imprime cuántas peticiones se pierden con
/// 0, 5 y 15 ms de latencia en cada sentido, sumando 10 paradas de cada una.
#[tokio::test]
#[ignore = "medida: imprime resultados, no es una prueba de regresión"]
async fn medida_parar_con_carga_y_latencia_no_pierde_peticiones() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let llama = bus(&broker.url(), "llama").await;
    for un_sentido in [0_u64, 5, 15] {
        let via = if un_sentido == 0 {
            broker.port
        } else {
            proxy_con_latencia(broker.port, Duration::from_millis(un_sentido)).await
        };
        let (ok, no_hay, perdidas, otras) = (
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        for repeticion in 0..10_u64 {
            let crm = bus(&format!("nats://127.0.0.1:{via}"), "crm").await;
            let r = dueno_responde(&crm, OBTENER, json!({ "clienteId": "cli-1" })).await;
            let parar = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let tareas: Vec<_> = (0..16_u64)
                .map(|n| {
                    let (llama, parar) = (llama.clone(), Arc::clone(&parar));
                    let (ok, no_hay, perdidas, otras) = (
                        Arc::clone(&ok),
                        Arc::clone(&no_hay),
                        Arc::clone(&perdidas),
                        Arc::clone(&otras),
                    );
                    tokio::spawn(async move {
                        let mut estado = n * 7919 + repeticion * 104_729 + 1;
                        while !parar.load(Ordering::SeqCst) {
                            // Un generador de congruencia lineal: pausas de 0 a 9 ms, distintas por bucle.
                            estado = estado
                                .wrapping_mul(6_364_136_223_846_793_005)
                                .wrapping_add(1_442_695_040_888_963_407);
                            tokio::time::sleep(Duration::from_millis((estado >> 33) % 10)).await;
                            match llama
                                .request_with(
                                    OBTENER,
                                    &json!({ "clienteId": "cli-1" }),
                                    RequestOptions::default().with_timeout_ms(400),
                                )
                                .await
                            {
                                Ok(_) => ok.fetch_add(1, Ordering::SeqCst),
                                Err(e) if code(&e) == "RPC_UNAVAILABLE" => {
                                    no_hay.fetch_add(1, Ordering::SeqCst)
                                }
                                Err(e) if code(&e) == "RPC_TIMEOUT" => {
                                    perdidas.fetch_add(1, Ordering::SeqCst)
                                }
                                Err(_) => otras.fetch_add(1, Ordering::SeqCst),
                            };
                        }
                    })
                })
                .collect();
            tokio::time::sleep(Duration::from_millis(250 + repeticion * 13)).await;
            r.stop().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            parar.store(true, Ordering::SeqCst);
            for t in tareas {
                t.await.unwrap();
            }
            crm.close().await.unwrap();
        }
        eprintln!(
            "A-91: latencia {un_sentido} ms por sentido, 10 paradas: {} ok, {} no responders tras parar, {} PERDIDAS (RPC_TIMEOUT), {} otras",
            ok.load(Ordering::SeqCst),
            no_hay.load(Ordering::SeqCst),
            perdidas.load(Ordering::SeqCst),
            otras.load(Ordering::SeqCst)
        );
    }
}

/// A-92: la cola de la suscripción es acotada. §5.6 dice que el exceso de peticiones «espera en la cola de la
/// suscripción»: `async-nats` 0.50 la limita a `subscription_capacity` (65 536 mensajes por defecto) y, llena,
/// **descarta** lo que llega (evento `SlowConsumer`) sin que quien llama sepa más que `RPC_TIMEOUT`. El
/// responder se queda a 1 handler y un handler bloqueado mientras ~70 000 peticiones llegan de golpe.
///
/// `cargo test --test rpc -- --ignored --nocapture medida_cola`
#[tokio::test]
#[ignore = "medida: imprime resultados, no es una prueba de regresión"]
async fn medida_cola_de_la_suscripcion_acotada() {
    let bin = broker_bin!();
    let broker = Broker::con_acl(&bin);
    let (log, lineas) = logger_espia();
    let crm = bus_con(&broker.url(), "crm", |o| o.with_logger(log)).await;
    let liberar = Arc::new(tokio::sync::Notify::new());
    let atendidas = Arc::new(AtomicUsize::new(0));
    let (l2, a2) = (Arc::clone(&liberar), Arc::clone(&atendidas));
    crm.respond_with(
        OBTENER,
        move |_: RpcRequest, _: RpcContext| {
            let (l2, a2) = (Arc::clone(&l2), Arc::clone(&a2));
            async move {
                if a2.fetch_add(1, Ordering::SeqCst) == 0 {
                    l2.notified().await; // el primero bloquea: el resto se acumula
                }
                Ok::<Value, flux::HandlerError>(json!({ "clienteId": "cli-1" }))
            }
        },
        RespondOptions::default().with_max_concurrent(1),
    )
    .await
    .unwrap();
    let ops = bruto(&broker.url()).await;
    let mut respuestas = ops.subscribe("_INBOX.ops.cola.>").await.unwrap();
    barrera(&ops).await;
    let contadas = Arc::new(AtomicUsize::new(0));
    let c2 = Arc::clone(&contadas);
    tokio::spawn(async move {
        while respuestas.next().await.is_some() {
            c2.fetch_add(1, Ordering::SeqCst);
        }
    });
    const ENVIADAS: usize = 70_000;
    let peticion = {
        let mut e = respuesta_buena(
            &flux::build_rpc_request(flux::RpcRequestInput {
                operation: OBTENER.into(),
                tenant_id: "acme".into(),
                aggregate_id: None,
                id: uuid_nuevo(),
                source: "/produccion/llama".into(),
                time: chrono::Utc::now(),
                correlationid: uuid_nuevo(),
                causationid: None,
                producerversion: "1.0.0".into(),
                classification: DataClassification::Internal,
                dataschema: flux::rpc_dataschema(BASE, OBTENER, flux::RpcSide::Request).unwrap(),
                traceparent: None,
                tracestate: None,
                data: serde_json::value::to_raw_value(&json!({ "clienteId": "cli-1" })).unwrap(),
            })
            .unwrap(),
        );
        e.event_type = "com.flux.rpc.clientes.cliente.obtener.request.v1".into();
        e.causationid = None;
        bytes_de(&e)
    };
    for i in 0..ENVIADAS {
        ops.publish_with_reply(
            format!("rpc.{OBTENER}"),
            format!("_INBOX.ops.cola.{i}"),
            peticion.clone().into(),
        )
        .await
        .unwrap();
    }
    ops.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    liberar.notify_one();
    tokio::time::sleep(Duration::from_secs(20)).await;
    let avisos = lock(&lineas)
        .iter()
        .filter(|l| l.contains("DESCARTANDO"))
        .count();
    eprintln!(
        "A-92: {ENVIADAS} peticiones con un handler bloqueado y max_concurrent 1: {} atendidas, {} respuestas recibidas ({} descartadas por la cola de la suscripción); el SDK avisó {avisos} vez/veces por el logger",
        atendidas.load(Ordering::SeqCst),
        contadas.load(Ordering::SeqCst),
        ENVIADAS - atendidas.load(Ordering::SeqCst)
    );
    assert!(
        avisos >= 1,
        "un responder desbordado no puede parecer sano: {:?}",
        lock(&lineas).len()
    );
}
