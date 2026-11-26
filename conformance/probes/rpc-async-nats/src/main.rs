//! Sonda de la Fase 3 con `async-nats` 0.50: qué ve quien llama en cinco situaciones.
//!
//! Habla con la ACL «rpc» de `conformance/cases/rpc-semantics.json` (usuarios `llama`, `crm` y `ajeno`) y
//! escribe una línea JSON por sonda: `{"probe": "...", "kind": "...", "ms": n, ...}`. `rpc-broker.mjs` la lee y
//! compara con los `expect` de los casos. NO usa ningún SDK de flux: igual que el resto de la suite, un runner
//! construido sobre un SDK heredaría las suposiciones que debe verificar.
use async_nats::{ConnectOptions, Event, Request};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Errores = Arc<Mutex<Vec<String>>>;

async fn conecta(url: &str, user: &str) -> (async_nats::Client, Errores) {
    let errores: Errores = Arc::new(Mutex::new(vec![]));
    let e2 = errores.clone();
    let client = ConnectOptions::with_user_and_password(user.into(), "pw".into())
        .custom_inbox_prefix(format!("_INBOX.{user}"))
        .event_callback(move |ev| {
            let e3 = e2.clone();
            async move {
                // El broker NO contesta a lo que deniega: avisa aparte, por la conexión (A-28).
                if let Event::ServerError(e) = ev {
                    e3.lock().unwrap().push(format!("{e}"));
                }
            }
        })
        .connect(url)
        .await
        .expect("conecta");
    (client, errores)
}

fn linea(v: serde_json::Value) {
    println!("{v}");
}

fn peticion(payload: Bytes, ms: u64) -> Request {
    Request::new().payload(payload).timeout(Some(Duration::from_millis(ms)))
}

#[tokio::main]
async fn main() {
    let url = std::env::args().nth(1).expect("uso: rpc-async-nats-probe <host:puerto>");
    let (llama, _) = conecta(&url, "llama").await;
    let (ajeno, errs_ajeno) = conecta(&url, "ajeno").await;
    let (crm, errs_crm) = conecta(&url, "crm").await;
    let max = llama.server_info().max_payload;
    linea(json!({"probe": "server-info", "maxPayload": max}));

    // 1. Nadie escucha: el broker contesta `no responders` y la librería lo devuelve al instante.
    let t = Instant::now();
    let r = llama.send_request("rpc.clientes.cliente.v1.obtener", peticion(Bytes::from("{}"), 5000)).await;
    let e = r.err().map(|e| e.kind());
    linea(json!({"probe": "no-responders", "kind": format!("{:?}", e), "ms": t.elapsed().as_millis()}));

    // El dueño atiende `calcular-descuento`: una vez rápido, otra lento, otra con una respuesta que no cabe.
    let mut sub = crm.queue_subscribe("rpc.clientes.cliente.v1.calcular-descuento", "crm".into()).await.unwrap();
    let crm2 = crm.clone();
    let modo = Arc::new(Mutex::new("rapido".to_string()));
    let modo2 = modo.clone();
    let fallo_responder: Errores = Arc::new(Mutex::new(vec![]));
    let fallo2 = fallo_responder.clone();
    tokio::spawn(async move {
        while let Some(m) = sub.next().await {
            let modo = modo2.lock().unwrap().clone();
            let Some(reply) = m.reply else { continue };
            match modo.as_str() {
                "lento" => {
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    let _ = crm2.publish(reply, Bytes::from("{\"tarde\":true}")).await;
                }
                "grande" => {
                    if let Err(e) = crm2.publish(reply, Bytes::from(vec![b'a'; max + 1])).await {
                        fallo2.lock().unwrap().push(format!("{:?}", e.kind()));
                    }
                }
                _ => {
                    let _ = crm2.publish(reply, Bytes::from("{\"ok\":true}")).await;
                }
            }
        }
    });
    crm.flush().await.unwrap();

    // 2. Petición atendida: latencia.
    let t = Instant::now();
    let r = llama.send_request("rpc.clientes.cliente.v1.calcular-descuento", peticion(Bytes::from("{}"), 1000)).await;
    linea(json!({"probe": "answered", "ok": r.is_ok(), "ms": t.elapsed().as_secs_f64() * 1000.0}));

    // 3. El que responde tarda más que el timeout.
    *modo.lock().unwrap() = "lento".into();
    let t = Instant::now();
    let r = llama.send_request("rpc.clientes.cliente.v1.calcular-descuento", peticion(Bytes::from("{}"), 300)).await;
    let e = r.err().map(|e| e.kind());
    linea(json!({"probe": "slow-responder", "kind": format!("{:?}", e), "ms": t.elapsed().as_millis()}));
    tokio::time::sleep(Duration::from_millis(800)).await;

    // 4. Una petición DENEGADA por la ACL: ¿error rápido o timeout? (A-17 lo midió con Node; A-28, con async-nats.)
    let t = Instant::now();
    let r = ajeno.send_request("rpc.clientes.cliente.v1.obtener", peticion(Bytes::from("{}"), 2500)).await;
    let e = r.err().map(|e| e.kind());
    let ms = t.elapsed().as_millis();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let vistos = errs_ajeno.lock().unwrap().clone();
    linea(json!({"probe": "denied", "kind": format!("{:?}", e), "ms": ms, "serverErrors": vistos, "sawPermissionsViolation": vistos.iter().any(|x| x.contains("Permissions Violation"))}));

    // 5. Una petición mayor que `max_payload`, y una respuesta mayor.
    let t = Instant::now();
    let r = llama.send_request("rpc.clientes.cliente.v1.obtener", peticion(Bytes::from(vec![b'a'; max + 1]), 1000)).await;
    let e = r.err().map(|e| e.kind());
    linea(json!({"probe": "oversize-request", "kind": format!("{:?}", e), "ms": t.elapsed().as_millis()}));
    let r = llama.publish("rpc.clientes.cliente.v1.obtener", Bytes::from(vec![b'a'; max])).await;
    linea(json!({"probe": "exact-max-payload-publishes", "ok": r.is_ok()}));

    *modo.lock().unwrap() = "grande".into();
    let t = Instant::now();
    let r = llama.send_request("rpc.clientes.cliente.v1.calcular-descuento", peticion(Bytes::from("{}"), 1000)).await;
    let e = r.err().map(|e| e.kind());
    let ms = t.elapsed().as_millis();
    tokio::time::sleep(Duration::from_millis(100)).await;
    linea(json!({"probe": "oversize-reply", "callerKind": format!("{:?}", e), "callerMs": ms, "responderSaw": fallo_responder.lock().unwrap().clone(), "responderViolations": errs_crm.lock().unwrap().clone()}));

}
