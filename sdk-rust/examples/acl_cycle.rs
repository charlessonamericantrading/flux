//! S11 — el SDK de Rust, de extremo a extremo, contra la ACL que genera `scripts/gen-acl.mjs`.
//!
//! No es un test: es el proceso que `conformance/security.mjs` lanza (`cargo run --example
//! acl_cycle`), gobernado SOLO por el entorno. Va como ejemplo y no como `tests/*.rs` a
//! propósito: `cargo test` no ejecuta los ejemplos, así que un job de Rust que no levanta la
//! suite de seguridad no puede ponerse rojo ni, peor, verde sin haberlo ejecutado. El runner
//! comprueba después, desde el broker, lo que de verdad quedó (acks, DLQ, violaciones en el log
//! del servidor): lo que el SDK diga de sí mismo no basta.
//!
//! ```text
//! entra  FLUX_ACL_URL               nats://… o tls://… del broker
//!        FLUX_ACL_CA                CA que firma su certificado
//!        FLUX_ACL_PRODUCER/_SEED    servicio que publica y su semilla nkey
//!        FLUX_ACL_CONSUMER/_SEED    servicio que consume y su semilla nkey
//!        FLUX_ACL_SUBJECT           el subject que se publica y se consume
//!        FLUX_ACL_EVENTS            JSON: [{ caso, calls, data }]
//! sale   FLUX_ACL_REPORT {json}
//! ```
//!
//! El comportamiento del handler lo decide `caso`, igual que en el ciclo de Node: `ok`
//! confirma, `permanente` falla con un error PERMANENT (acaba en la DLQ) y `reintento` falla
//! la primera entrega con un error RETRYABLE y acepta la segunda.
//!
//! Si el SDK necesita un permiso que el generador no concede, aquí falla, y eso es el
//! hallazgo: este fichero no parchea ni el SDK ni la ACL.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flux::{
    connect, ConnectOptions, Credentials, Delivery, Event, FluxError, LogFn, LogLevel,
    PublishOptions, TlsOptions,
};
use serde_json::{json, Value};

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        eprintln!("falta la variable de entorno {name}");
        std::process::exit(2);
    })
}

fn opciones(url: &str, ca: &str, servicio: &str, semilla: String, log: &LogFn) -> ConnectOptions {
    // `with_tls` EXIGE TLS: el esquema solo importa para el host y el puerto.
    ConnectOptions::new(
        url.replacen("tls://", "nats://", 1),
        servicio,
        "test",
        "1.0.0",
    )
    .with_credentials(Credentials::NkeySeed(semilla))
    .with_tls(TlsOptions::new().with_ca_file(ca))
    .with_tenant_id("acme")
    .with_schema_base_url("https://schemas.internal")
    .with_logger(Arc::clone(log))
}

#[tokio::main]
async fn main() {
    let url = env("FLUX_ACL_URL");
    let ca = env("FLUX_ACL_CA");
    let subject = env("FLUX_ACL_SUBJECT");
    let eventos: Vec<Value> = serde_json::from_str(&env("FLUX_ACL_EVENTS"))
        .expect("FLUX_ACL_EVENTS debe ser un array JSON");
    let esperadas: usize = eventos
        .iter()
        .map(|e| usize::try_from(e["calls"].as_u64().unwrap_or(1)).unwrap_or(1))
        .sum();

    let logs: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&logs);
    let log: LogFn = Arc::new(move |level: LogLevel, msg: &str| {
        sink.lock().unwrap().push(format!("{level:?} {msg}"));
    });
    let calls: Arc<Mutex<Vec<(String, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let mut errores: Vec<String> = Vec::new();
    let mut publicados: Vec<Value> = Vec::new();

    let consumidor = connect(opciones(
        &url,
        &ca,
        &env("FLUX_ACL_CONSUMER"),
        env("FLUX_ACL_CONSUMER_SEED"),
        &log,
    ))
    .await;
    let productor = connect(opciones(
        &url,
        &ca,
        &env("FLUX_ACL_PRODUCER"),
        env("FLUX_ACL_PRODUCER_SEED"),
        &log,
    ))
    .await;

    match (&consumidor, &productor) {
        (Ok(consumidor), Ok(productor)) => {
            let vistas = Arc::clone(&calls);
            let sub = consumidor
                .subscribe(&subject, move |ev: Event, d: Delivery| {
                    let vistas = Arc::clone(&vistas);
                    async move {
                        let data: Value =
                            serde_json::from_str(ev.data.get()).unwrap_or(Value::Null);
                        let caso = data["caso"].as_str().unwrap_or("ok").to_string();
                        vistas.lock().unwrap().push((caso.clone(), d.attempt));
                        match caso.as_str() {
                            "permanente" => Err(FluxError::permanent("pedido ya cancelado")
                                .with_code("PEDIDO_YA_CANCELADO")
                                .into()),
                            "reintento" if d.attempt == 1 => {
                                Err(FluxError::retryable("proveedor 503")
                                    .with_code("HTTP_503")
                                    .with_retry_after(Duration::from_millis(300))
                                    .into())
                            }
                            _ => Ok(()),
                        }
                    }
                })
                .await;

            match sub {
                Err(e) => errores.push(format!("subscribe: {e}")),
                Ok(sub) => {
                    for (i, e) in eventos.iter().enumerate() {
                        let caso = e["caso"].as_str().unwrap_or("ok");
                        let mut data = e["data"].clone();
                        data["caso"] = json!(caso);
                        data["aggregateVersion"] = json!(1);
                        let agregado = data["pedidoId"]
                            .as_str()
                            .map_or_else(|| format!("ped-{i}"), str::to_string);
                        match productor
                            .publish_with(
                                &subject,
                                &data,
                                PublishOptions::default().with_aggregate_id(agregado),
                            )
                            .await
                        {
                            Ok(publicado) => {
                                publicados.push(json!({ "caso": caso, "id": publicado.id }));
                            }
                            Err(err) => errores.push(format!("publish: {err}")),
                        }
                    }

                    // Hasta que el handler haya visto todas las entregas esperadas (con un tope).
                    let limite = Instant::now() + Duration::from_secs(25);
                    while calls.lock().unwrap().len() < esperadas && Instant::now() < limite {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    let vistas = calls.lock().unwrap().len();
                    if vistas < esperadas {
                        errores.push(format!(
                            "el handler vio {vistas} de {esperadas} entregas en 25 s"
                        ));
                    }
                    // Un poco más para que los acks y la DLQ salgan antes de cerrar.
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    sub.unsubscribe();
                }
            }
        }
        _ => {
            for (que, r) in [("consumidor", &consumidor), ("productor", &productor)] {
                if let Err(e) = r {
                    errores.push(format!("connect ({que}): {e}"));
                }
            }
        }
    }

    if let Ok(c) = &consumidor {
        let _ = c.close().await;
    }
    if let Ok(p) = &productor {
        let _ = p.close().await;
    }

    let handler_calls: Vec<Value> = calls
        .lock()
        .unwrap()
        .iter()
        .map(|(caso, attempt)| json!({ "caso": caso, "attempt": attempt }))
        .collect();
    let informe = json!({
        "sdk": "rust",
        "published": publicados,
        "handlerCalls": handler_calls,
        "errors": errores,
        "logs": logs.lock().unwrap().clone(),
    });
    println!("FLUX_ACL_REPORT {informe}");
    std::process::exit(i32::from(!errores.is_empty()));
}
