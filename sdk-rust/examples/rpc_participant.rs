//! El participante del SDK de RUST en los escenarios de petición/respuesta con SDKs reales
//! (`sdk_script` de `conformance/security/interprete.mjs`: casos S30 en adelante de `security-acl.json` y los de
//! `conformance/cases/interop-rpc.json`).
//!
//! No es un test: es un proceso que un orquestador lanza y gobierna por STDIN, con UNA línea JSON por orden, y que
//! contesta por STDOUT con UNA línea `{"id":n,…}` por orden. Habla con el broker SOLO por la API pública del SDK
//! (`connect`, `Bus::request_with`, `Bus::respond`, `Bus::subscribe`, `State::watch_with`, `Bus::publish_with`): lo que el
//! SDK haga por debajo —sus suscripciones, sus permisos, sus tiempos— es justo lo que el escenario mide. El otro
//! participante, `conformance/security/participante-node.mjs`, habla el MISMO protocolo con el SDK de Node: el
//! orquestador no sabe cuál es cuál. Va como ejemplo y no como `tests/*.rs` por la misma razón que `acl_cycle`: `cargo
//! test` no ejecuta los ejemplos, así que un job de Rust que no levanta la suite de seguridad no puede ponerse rojo ni,
//! peor, verde sin haberlo ejecutado.
//!
//! ```text
//! entra  FLUX_P_URL        `tls://127.0.0.1:<puerto>`
//!        FLUX_P_CA         CA que firma el certificado del broker
//!        FLUX_P_SERVICE    el nombre del servicio (su identidad y su inbox privado `_INBOX.<servicio>`)
//!        FLUX_P_SEED       su semilla nkey
//!        FLUX_P_TENANT     el tenant de `connect()` (por defecto `acme`)
//!        FLUX_P_LABEL      cómo se llama esta INSTANCIA en `instancia` de las respuestas (por defecto, el servicio)
//!        FLUX_P_SIGNING    JSON opcional: `{ keyId, privateKey, publicKeys: {id: PEM}, verify: off|warn|require }`
//!                          (solo con `--features signing`; sin ella, pedirla es un error y no se ignora)
//! ```
//!
//! Si el SDK necesita un permiso que el generador no concede, o un código no sale, aquí se cuenta tal cual: este
//! fichero no parchea ni el SDK ni la ACL.

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flux::{
    connect, Bus, ConnectOptions, Credentials, Delivery, ErrorClass, Event, FluxError,
    HandlerError, LogFn, LogLevel, PublishOptions, RequestOptions, Responder, RpcContext,
    RpcRequest, StateEntry, TlsOptions, WatchOptions,
};
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;

fn env(name: &str, por_defecto: Option<&str>) -> String {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v,
        _ => por_defecto.map(str::to_string).unwrap_or_else(|| {
            eprintln!("falta la variable de entorno {name}");
            std::process::exit(2);
        }),
    }
}

/// Lo que cuenta un `call_many`: cuántas se contestaron, quién las atendió y qué falló, por código.
type Cuentas = (u64, HashMap<String, u64>, HashMap<String, u64>);

/// Lo que comparten las órdenes: el bus y lo que el participante cuenta de sí mismo.
struct Compartido {
    etiqueta: String,
    bus: Bus,
    logs: Arc<Mutex<Vec<String>>>,
    comportamientos: Mutex<HashMap<String, Value>>,
    atendidas: Mutex<HashMap<String, u64>>,
    ids_vistos: Mutex<HashMap<String, Vec<String>>>,
    responders: Mutex<HashMap<String, Responder>>,
    consumidos: AtomicU64,
    observados: AtomicU64,
}

/// Un error del SDK en el formato del protocolo del participante: lo mismo que sacaría el de Node.
fn describir_error(e: &FluxError, ms: Option<f64>) -> Value {
    let clase = match e.class() {
        Some(ErrorClass::Retryable) => "retryable",
        Some(ErrorClass::Permanent) => "permanent",
        Some(ErrorClass::Poison) => "poison",
        None => "desconocida",
    };
    let mut o = Map::new();
    o.insert("name".into(), json!(nombre_de(e)));
    o.insert("code".into(), e.code().map_or(Value::Null, |c| json!(c)));
    o.insert("class".into(), json!(clase));
    o.insert("message".into(), json!(e.to_string()));
    if let Some(d) = e.retry_after() {
        o.insert("retryAfterMs".into(), json!(d.as_millis()));
    }
    if let Some(ms) = ms {
        o.insert("ms".into(), json!(ms));
    }
    Value::Object(o)
}

/// El nombre de la variante, que es lo que en Node es `error.name`.
fn nombre_de(e: &FluxError) -> String {
    let d = format!("{e:?}");
    d.split(|c: char| !c.is_alphanumeric())
        .next()
        .unwrap_or("Error")
        .to_string()
}

fn mediana(xs: &[f64]) -> f64 {
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn ms(t0: Instant) -> f64 {
    t0.elapsed().as_secs_f64() * 1000.0
}

fn opciones_de_llamada(o: &Value) -> RequestOptions {
    let mut r = RequestOptions::default();
    if let Some(t) = o["timeoutMs"].as_u64() {
        r = r.with_timeout_ms(t);
    }
    if let Some(t) = o["tenantId"].as_str() {
        r = r.with_tenant_id(t);
    }
    if let Some(a) = o["aggregateId"].as_str() {
        r = r.with_aggregate_id(a);
    }
    if let Some(n) = o["retries"].as_u64() {
        r = r.with_retries(u32::try_from(n).unwrap_or(0));
    }
    if let Some(c) = o["classification"].as_str() {
        let c = match c {
            "public" => Some(flux::DataClassification::Public),
            "internal" => Some(flux::DataClassification::Internal),
            "confidential" => Some(flux::DataClassification::Confidential),
            "restricted" => Some(flux::DataClassification::Restricted),
            _ => None,
        };
        if let Some(c) = c {
            r = r.with_classification(c);
        }
    }
    r
}

/// UNA llamada: `{ ok, data, ms }` o `{ ok: false, error, ms }`.
async fn llamar(s: &Compartido, operacion: &str, data: &Value, opciones: &Value) -> Value {
    let t0 = Instant::now();
    match s
        .bus
        .request_with(operacion, data, opciones_de_llamada(opciones))
        .await
    {
        Ok(ev) => {
            let d: Value = serde_json::from_str(ev.data.get()).unwrap_or(Value::Null);
            json!({ "ok": true, "data": d, "ms": ms(t0) })
        }
        Err(e) => json!({ "ok": false, "error": describir_error(&e, Some(ms(t0))), "ms": ms(t0) }),
    }
}

/// El handler de una operación: hace lo que el comportamiento VIGENTE diga (el escenario lo cambia con `behavior`).
async fn atender(
    s: Arc<Compartido>,
    operacion: String,
    peticion: RpcRequest,
    ctx: RpcContext,
) -> Result<Value, HandlerError> {
    *s.atendidas
        .lock()
        .unwrap()
        .entry(operacion.clone())
        .or_insert(0) += 1;
    s.ids_vistos
        .lock()
        .unwrap()
        .entry(operacion.clone())
        .or_default()
        .push(ctx.id.clone());
    let b = s
        .comportamientos
        .lock()
        .unwrap()
        .get(&operacion)
        .cloned()
        .unwrap_or_else(|| json!({ "kind": "echo" }));
    if let Some(espera) = b["afterMs"].as_u64() {
        tokio::time::sleep(Duration::from_millis(espera)).await;
    }
    match b["kind"].as_str().unwrap_or("echo") {
        "echo" => {
            let eco: Value = serde_json::from_str(peticion.raw().get()).unwrap_or(Value::Null);
            let mut o = Map::new();
            o.insert("instancia".into(), json!(s.etiqueta));
            o.insert("tenant".into(), json!(ctx.tenant_id));
            o.insert("correlationId".into(), json!(ctx.correlation_id));
            o.insert(
                "correlacionEsElId".into(),
                json!(ctx.correlation_id == ctx.id),
            );
            o.insert("peticionId".into(), json!(ctx.id));
            if let Some(a) = &ctx.aggregate_id {
                o.insert("aggregateId".into(), json!(a));
            }
            o.insert("eco".into(), eco);
            Ok(Value::Object(o))
        }
        "error" => {
            let mensaje = b["message"].as_str().map(str::to_string);
            let code = b["code"].as_str().map(str::to_string);
            let e = if b["class"].as_str() == Some("retryable") {
                let mut e = FluxError::retryable(
                    mensaje.unwrap_or_else(|| "el servicio de tarifas no responde".into()),
                );
                if let Some(c) = code {
                    e = e.with_code(c);
                }
                if let Some(ms) = b["retryAfterMs"].as_u64() {
                    e = e.with_retry_after(Duration::from_millis(ms));
                }
                e
            } else {
                let mut e =
                    FluxError::permanent(mensaje.unwrap_or_else(|| "pedido ya cancelado".into()));
                if let Some(c) = code {
                    e = e.with_code(c);
                }
                e
            };
            Err(Box::new(e))
        }
        // Una excepción que NO lanzó a propósito la aplicación: su texto puede llevar valores de `data`, rutas…
        "unknown" => Err(Box::new(std::io::Error::other(
            b["text"]
                .as_str()
                .unwrap_or("SECRETO-DE-LA-EXCEPCION")
                .to_string(),
        ))),
        // Lo que el clasificador RECONOCE (un 503 de una dependencia, `ECONNRESET`), con el texto de la excepción por medio.
        "http" => Err(Box::new(flux::HttpError::new(
            u16::try_from(b["status"].as_u64().unwrap_or(503)).unwrap_or(503),
            b["text"]
                .as_str()
                .unwrap_or("SECRETO-DEL-PROVEEDOR")
                .to_string(),
        ))),
        "econnreset" => Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            b["text"]
                .as_str()
                .unwrap_or("SECRETO-DEL-PROVEEDOR")
                .to_string(),
        ))),
        "big" => Ok(json!({
            "instancia": s.etiqueta,
            "relleno": "x".repeat(usize::try_from(b["bytes"].as_u64().unwrap_or(1_100_000)).unwrap_or(1_100_000)),
        })),
        "no-object" => Ok(json!("esto no es un objeto")),
        otro => Err(format!("comportamiento desconocido «{otro}»").into()),
    }
}

/// Ejecuta UNA orden y devuelve su respuesta (sin el `id`, que pone el bucle).
async fn ejecutar(s: Arc<Compartido>, o: Value) -> Value {
    let cmd = o["cmd"].as_str().unwrap_or("").to_string();
    let operacion = o["operation"].as_str().unwrap_or("").to_string();
    match cmd.as_str() {
        // Atiende una operación con un comportamiento. Falla con el error del SDK (p. ej. PERMISSION_DENIED por falta de permiso).
        "respond" => {
            let b = if o["behavior"].is_null() {
                json!({ "kind": "echo" })
            } else {
                o["behavior"].clone()
            };
            s.comportamientos
                .lock()
                .unwrap()
                .insert(operacion.clone(), b);
            let (s2, op2) = (Arc::clone(&s), operacion.clone());
            let handler = move |req: RpcRequest, ctx: RpcContext| {
                let (s3, op3) = (Arc::clone(&s2), op2.clone());
                async move { atender(s3, op3, req, ctx).await }
            };
            match s.bus.respond(&operacion, handler).await {
                Ok(r) => {
                    let subject = r.subject.clone();
                    s.responders.lock().unwrap().insert(operacion, r);
                    json!({ "ok": true, "subject": subject })
                }
                Err(e) => json!({ "ok": false, "error": describir_error(&e, None) }),
            }
        }
        // Cambia el comportamiento de una operación YA atendida.
        "behavior" => {
            s.comportamientos
                .lock()
                .unwrap()
                .insert(operacion, o["behavior"].clone());
            json!({ "ok": true })
        }
        // Parada ordenada de UNA operación (`Responder::stop`).
        "stop" => {
            let r = s.responders.lock().unwrap().remove(&operacion);
            match r {
                None => {
                    json!({ "ok": false, "error": { "name": "Error", "message": format!("no se atiende {operacion}") } })
                }
                Some(r) => {
                    let t0 = Instant::now();
                    r.stop().await;
                    json!({ "ok": true, "ms": ms(t0) })
                }
            }
        }
        "call" => llamar(&s, &operacion, &o["data"], &o["options"]).await,
        // `repeat` llamadas seguidas: la mediana y el máximo de los tiempos y los códigos que salieron (una sola medición engaña).
        "call_repeat" => {
            let n = o["repeat"].as_u64().unwrap_or(5);
            let mut tiempos = Vec::new();
            let mut codigos: Map<String, Value> = Map::new();
            let mut ultimo = Value::Null;
            for _ in 0..n {
                let r = llamar(&s, &operacion, &o["data"], &o["options"]).await;
                tiempos.push(r["ms"].as_f64().unwrap_or(0.0));
                let codigo = if r["ok"] == json!(true) {
                    "OK".to_string()
                } else {
                    r["error"]["code"]
                        .as_str()
                        .or(r["error"]["name"].as_str())
                        .unwrap_or("?")
                        .to_string()
                };
                let c = codigos.get(&codigo).and_then(Value::as_u64).unwrap_or(0);
                codigos.insert(codigo, json!(c + 1));
                ultimo = r;
            }
            json!({
                "ok": true, "calls": tiempos.len(),
                "p50Ms": mediana(&tiempos),
                "maxMs": tiempos.iter().cloned().fold(0.0, f64::max),
                "minMs": tiempos.iter().cloned().fold(f64::MAX, f64::min),
                "codes": codigos, "last": ultimo,
            })
        }
        // `n` llamadas con `concurrency` a la vez: cuántas se contestaron, quién las atendió y qué falló.
        "call_many" => {
            let n = o["n"].as_u64().unwrap_or(100);
            let concurrencia = o["concurrency"].as_u64().unwrap_or(20).max(1);
            let siguiente = Arc::new(AtomicU64::new(0));
            let resultados: Arc<Mutex<Cuentas>> =
                Arc::new(Mutex::new((0, HashMap::new(), HashMap::new())));
            let mut tareas = Vec::new();
            for _ in 0..concurrencia {
                let (s, siguiente, resultados) = (
                    Arc::clone(&s),
                    Arc::clone(&siguiente),
                    Arc::clone(&resultados),
                );
                let (operacion, data, opciones) =
                    (operacion.clone(), o["data"].clone(), o["options"].clone());
                tareas.push(tokio::spawn(async move {
                    loop {
                        let i = siguiente.fetch_add(1, Ordering::SeqCst);
                        if i >= n {
                            break;
                        }
                        let mut d = if data.is_object() {
                            data.clone()
                        } else {
                            json!({})
                        };
                        d["n"] = json!(i);
                        let r = llamar(&s, &operacion, &d, &opciones).await;
                        let mut g = resultados.lock().unwrap();
                        if r["ok"] == json!(true) {
                            g.0 += 1;
                            let quien = r["data"]["instancia"].as_str().unwrap_or("?").to_string();
                            *g.1.entry(quien).or_insert(0) += 1;
                        } else {
                            let c = r["error"]["code"]
                                .as_str()
                                .or(r["error"]["name"].as_str())
                                .unwrap_or("?")
                                .to_string();
                            *g.2.entry(c).or_insert(0) += 1;
                        }
                    }
                }));
            }
            for t in tareas {
                let _ = t.await;
            }
            let g = resultados.lock().unwrap();
            let min_share = g.1.values().copied().min().unwrap_or(0);
            json!({
                "ok": true, "answered": g.0, "failed": n - g.0,
                "byInstance": g.1, "minShare": min_share, "errors": g.2,
            })
        }
        // Consume un subject de eventos por pull (`Bus::subscribe`): el servicio que consume y llama por la MISMA conexión.
        "consume" => {
            let subject = o["subject"].as_str().unwrap_or("").to_string();
            let s2 = Arc::clone(&s);
            let handler = move |_ev: Event, _d: Delivery| {
                let s3 = Arc::clone(&s2);
                async move {
                    s3.consumidos.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            };
            match s.bus.subscribe(&subject, handler).await {
                Ok(sub) => json!({ "ok": true, "durable": sub.durable }),
                Err(e) => json!({ "ok": false, "error": describir_error(&e, None) }),
            }
        }
        // `State::watch_with` de un contrato de estado.
        "watch" => {
            let contract = o["contract"].as_str().unwrap_or("").to_string();
            let s2 = Arc::clone(&s);
            let handler = move |_e: StateEntry, _d: Delivery| {
                let s3 = Arc::clone(&s2);
                async move {
                    s3.observados.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            };
            let mut opts = WatchOptions::default();
            if let Some(t) = o["tenantId"].as_str() {
                opts = opts.with_tenant_id(t);
            }
            match s.bus.state().watch_with(&contract, handler, opts).await {
                Ok(w) => json!({ "ok": true, "durable": w.durable }),
                Err(e) => json!({ "ok": false, "error": describir_error(&e, None) }),
            }
        }
        // Publica `n` EVENTOS (para que el que consume tenga algo que recibir).
        "publish" => {
            let subject = o["subject"].as_str().unwrap_or("").to_string();
            let n = o["n"].as_u64().unwrap_or(1);
            let mut primero = Value::Null;
            let mut publicados = 0u64;
            for i in 0..n {
                let mut data = o["data"].clone();
                if n > 1 {
                    data["pedidoId"] = json!(format!("ped-{i}"));
                }
                let mut opts = PublishOptions::default();
                if let Some(a) = o["options"]["aggregateId"].as_str() {
                    opts = opts.with_aggregate_id(a);
                } else if n > 1 {
                    opts = opts.with_aggregate_id(format!("ped-{i}"));
                }
                match s.bus.publish_with(&subject, &data, opts).await {
                    Ok(e) => {
                        if primero.is_null() {
                            primero = json!(e.id);
                        }
                        publicados += 1;
                    }
                    Err(e) => return json!({ "ok": false, "error": describir_error(&e, None) }),
                }
            }
            json!({ "ok": true, "eventId": primero, "published": publicados })
        }
        // Escribe una entrada de ESTADO (para que el `watch` tenga algo que observar).
        "state_put" => {
            let contract = o["contract"].as_str().unwrap_or("").to_string();
            let key = o["key"].as_str().unwrap_or("").to_string();
            match s.bus.state().put(&contract, &key, &o["data"]).await {
                Ok(w) => {
                    json!({ "ok": true, "outcome": format!("{:?}", w.outcome).to_lowercase(), "revision": w.revision })
                }
                Err(e) => json!({ "ok": false, "error": describir_error(&e, None) }),
            }
        }
        // Lo que el participante vio: peticiones por operación, eventos consumidos, entradas observadas y su log.
        "stats" => {
            let atendidas = s.atendidas.lock().unwrap().clone();
            let total: u64 = atendidas.values().sum();
            let logs = s.logs.lock().unwrap().clone();
            json!({
                "ok": true, "handled": atendidas, "total": total,
                "requestIds": s.ids_vistos.lock().unwrap().clone(),
                "uniqueRequestIds": s.ids_vistos.lock().unwrap().values().flatten().collect::<std::collections::HashSet<_>>().len(),
                "consumed": s.consumidos.load(Ordering::SeqCst),
                "watched": s.observados.load(Ordering::SeqCst),
                "logCount": logs.len(), "logs": logs,
            })
        }
        "close" => {
            let t0 = Instant::now();
            let _ = s.bus.close().await;
            json!({ "ok": true, "ms": ms(t0) })
        }
        otro => {
            json!({ "ok": false, "error": { "name": "Error", "message": format!("orden desconocida «{otro}»") } })
        }
    }
}

fn salir(o: &Value) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{o}");
    let _ = out.flush();
}

#[tokio::main]
async fn main() {
    let servicio = env("FLUX_P_SERVICE", None);
    let etiqueta = env("FLUX_P_LABEL", Some(&servicio));
    let logs: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&logs);
    let log: LogFn = Arc::new(move |level: LogLevel, msg: &str| {
        sink.lock()
            .unwrap()
            .push(format!("{} {msg}", format!("{level:?}").to_uppercase()));
    });

    // `with_tls` EXIGE TLS: el esquema solo importa para el host y el puerto.
    let url = env("FLUX_P_URL", None).replacen("tls://", "nats://", 1);
    #[allow(unused_mut)]
    let mut opts = ConnectOptions::new(url, &servicio, "test", "1.0.0")
        .with_credentials(Credentials::NkeySeed(env("FLUX_P_SEED", None)))
        .with_tls(TlsOptions::new().with_ca_file(env("FLUX_P_CA", None)))
        .with_tenant_id(env("FLUX_P_TENANT", Some("acme")))
        .with_schema_base_url("https://schemas.internal")
        .with_pending_poll(Duration::from_secs(3600))
        .with_logger(Arc::clone(&log));

    if let Ok(firma) = std::env::var("FLUX_P_SIGNING") {
        let f: Value = serde_json::from_str(&firma).expect("FLUX_P_SIGNING debe ser JSON");
        #[cfg(feature = "signing")]
        {
            let mut so = flux::SigningOptions::default().with_private_key(
                f["privateKey"].as_str().unwrap_or(""),
                f["keyId"].as_str().unwrap_or(""),
            );
            if let Some(claves) = f["publicKeys"].as_object() {
                for (id, pem) in claves {
                    so = so.with_public_key(id, pem.as_str().unwrap_or(""));
                }
            }
            so = so.with_verify(match f["verify"].as_str().unwrap_or("off") {
                "require" => flux::VerificationMode::Require,
                "warn" => flux::VerificationMode::Warn,
                _ => flux::VerificationMode::Off,
            });
            opts = opts.with_signing(so);
        }
        #[cfg(not(feature = "signing"))]
        {
            let _ = f;
            eprintln!(
                "FLUX_P_SIGNING pide la firma y este ejemplo se compiló sin `--features signing`"
            );
            std::process::exit(2);
        }
    }

    let bus = match connect(opts).await {
        Ok(b) => b,
        Err(e) => {
            salir(
                &json!({ "id": 0, "ready": true, "sdk": "rust", "service": servicio, "label": etiqueta,
                           "ok": false, "error": describir_error(&e, None) }),
            );
            return;
        }
    };
    let s = Arc::new(Compartido {
        etiqueta: etiqueta.clone(),
        bus,
        logs,
        comportamientos: Mutex::new(HashMap::new()),
        atendidas: Mutex::new(HashMap::new()),
        ids_vistos: Mutex::new(HashMap::new()),
        responders: Mutex::new(HashMap::new()),
        consumidos: AtomicU64::new(0),
        observados: AtomicU64::new(0),
    });
    salir(
        &json!({ "id": 0, "ready": true, "sdk": "rust", "service": servicio, "label": etiqueta }),
    );

    // stdin es bloqueante: en un hilo aparte, y las órdenes llegan por un canal.
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    std::thread::spawn(move || {
        for linea in std::io::stdin().lock().lines() {
            let Ok(linea) = linea else { break };
            if tx.send(linea).is_err() {
                break;
            }
        }
    });

    // Las órdenes se atienden EN PARALELO (un `call_many` no bloquea un `stats`), salvo `close`, que espera a las demás.
    let pendientes: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
    while let Some(linea) = rx.recv().await {
        let Ok(o) = serde_json::from_str::<Value>(&linea) else {
            salir(
                &json!({ "id": -1, "ok": false, "error": { "name": "Error", "message": "línea que no es JSON" } }),
            );
            continue;
        };
        let id = o["id"].clone();
        if o["cmd"] == json!("close") {
            let hs: Vec<_> = pendientes.lock().unwrap().drain(..).collect();
            for h in hs {
                let _ = h.await;
            }
            let mut r = ejecutar(Arc::clone(&s), o).await;
            r["id"] = id;
            salir(&r);
            return;
        }
        let s2 = Arc::clone(&s);
        let h = tokio::spawn(async move {
            let mut r = ejecutar(s2, o).await;
            r["id"] = id;
            salir(&r);
        });
        pendientes.lock().unwrap().push(h);
    }
    let _ = s.bus.close().await;
}
