//! Conformidad contra un NATS real.
//!
//! Estos tests **requieren un broker**: `docker compose up -d` en la raíz del repo, o un
//! `nats-server --jetstream` local. Sin él se ignoran solos, para que `cargo test` siga
//! siendo verde en una máquina sin Docker.
//!
//! Cubren lo que los tests unitarios no pueden: que la configuración canónica de
//! consumidor sobreviva al servidor, que el ack explícito funcione, y que el subject
//! fantasma de 02-naming.md §1.1 falle de verdad al publicar por JetStream.
//!
//!     FLUX_NATS_URL=nats://127.0.0.1:4222 cargo test --test integration

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flux::{connect, ConnectOptions, DataClassification, Event, FluxError, PublishOptions};
use serde_json::json;

/// URL del broker, o `None` para saltarse el test.
fn nats_url() -> Option<String> {
    let url = std::env::var("FLUX_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let addr = url.trim_start_matches("nats://");
    std::net::TcpStream::connect_timeout(&addr.parse().ok()?, Duration::from_millis(500))
        .ok()
        .map(|_| url)
}

macro_rules! broker {
    () => {
        match nats_url() {
            Some(u) => u,
            None => {
                eprintln!("saltado: no hay NATS en FLUX_NATS_URL");
                return;
            }
        }
    };
}

/// Un dominio distinto por test, para que no se pisen los streams entre ejecuciones.
fn subject(dominio: &str) -> String {
    format!("{dominio}.pedido.v1.creado")
}

async fn bus(url: &str, servicio: &str) -> flux::Bus {
    connect(
        ConnectOptions::new(url, servicio, "test", "1.0.0")
            .with_tenant_id("acme")
            .with_classification(DataClassification::Confidential)
            .with_schema_base_url("https://schemas.internal"),
    )
    .await
    .expect("conexión")
}

/// L1 completo: publicar, consumir, ack explícito, y el envelope relleno por el SDK.
#[tokio::test]
async fn publicar_y_consumir_con_ack_explicito() {
    let url = broker!();
    let bus = bus(&url, "pedidos-api").await;
    let subject = subject("itrust1");

    let recibido: Arc<Mutex<Option<Event>>> = Arc::new(Mutex::new(None));
    let sink = recibido.clone();

    let sub = bus
        .subscribe(&subject, move |ev: Event, _d| {
            let sink = sink.clone();
            async move {
                *sink.lock().unwrap() = Some(ev);
                Ok(()) // Ok(()) == ack explícito
            }
        })
        .await
        .expect("suscripción");

    // El durable se deriva solo del servicio y el subject — 02-naming.md §4.
    assert_eq!(
        sub.durable,
        format!("pedidos-api__{}", subject.replace('.', "_"))
    );

    let publicado = bus
        .publish_with(
            &subject,
            &json!({ "pedidoId": "ped-123", "aggregateVersion": 1, "totalCents": 9990 }),
            PublishOptions::default().with_aggregate_id("ped-123"),
        )
        .await
        .expect("publicación");

    let ev = esperar(&recibido).await.expect("el evento debería llegar");

    assert_eq!(ev.id, publicado.id, "el id no se regenera");
    assert_eq!(ev.specversion, "1.0");
    assert_eq!(ev.event_type, format!("com.flux.itrust1.pedido.creado.v1"));
    assert_eq!(ev.source, "/test/pedidos-api");
    assert_eq!(ev.tenantid, "acme");
    assert_eq!(ev.producerversion, "1.0.0");
    assert_eq!(ev.dataclassification, DataClassification::Confidential);
    assert_eq!(ev.aggregate_id.as_deref(), Some("ped-123"));
    // Un evento que no nace de otro inicializa correlationid con su propio id — §3.1.
    assert_eq!(ev.correlationid, ev.id);
    assert!(ev.causationid.is_none());
    // `time` con exactamente 3 decimales y sufijo Z, tras el round-trip por el broker.
    assert_eq!(ev.time.len(), 24, "{}", ev.time);
    assert!(ev.time.ends_with('Z'));

    sub.unsubscribe();
    bus.close().await.expect("close");
}

/// La única red de seguridad automática contra una errata de subject — 02-naming.md §1.1.
#[tokio::test]
async fn un_subject_con_mayusculas_se_rechaza_antes_de_la_red() {
    let url = broker!();
    let bus = bus(&url, "pedidos-api").await;

    let err = bus
        .publish("Pedidos.pedido.v1.creado", &json!({}))
        .await
        .unwrap_err();
    assert!(matches!(err, FluxError::InvalidSubject { .. }), "{err:?}");
    assert!(err.to_string().contains("nadie está suscrito"));

    bus.close().await.expect("close");
}

/// Requisito L2: la config que el servidor devuelve DEBE coincidir con la solicitada, y
/// `ack_wait` DEBE seguir siendo `backoff[0]` — 03-delivery.md §2.1.
///
/// Éste es el test que justifica todo el diseño del backoff canónico: si alguien cambiase
/// `CANONICAL_BACKOFF[0]` a 1 s, el servidor sobrescribiría `ack_wait` y esto lo cazaría.
#[tokio::test]
async fn la_config_canonica_sobrevive_al_servidor() {
    let url = broker!();
    let bus = bus(&url, "facturacion-api").await;
    let subject = subject("itrust2");

    // Si el servidor devolviera algo distinto, subscribe fallaría con
    // ConsumerConfigMismatch. Que esto pase es la comprobación.
    let sub = bus
        .subscribe(&subject, |_ev: Event, _d| async { Ok(()) })
        .await
        .expect("el servidor debería honrar la config canónica");

    sub.unsubscribe();
    bus.close().await.expect("close");
}

/// Un PERMANENT no gasta reintentos: `term()` + DLQ inmediato — 04-errors.md §1.2.
#[tokio::test]
async fn un_permanent_va_a_la_dlq_sin_reintentos() {
    let url = broker!();
    let bus = bus(&url, "pedidos-api").await;
    let subject = subject("itrust3");

    let intentos = Arc::new(AtomicU32::new(0));
    let contador = intentos.clone();

    let sub = bus
        .subscribe(&subject, move |_ev: Event, _d| {
            let contador = contador.clone();
            async move {
                contador.fetch_add(1, Ordering::SeqCst);
                Err(FluxError::permanent("pedido ya cancelado")
                    .with_code("PEDIDO_YA_CANCELADO")
                    .into())
            }
        })
        .await
        .expect("suscripción");

    let publicado = bus
        .publish(&subject, &json!({ "pedidoId": "ped-1" }))
        .await
        .expect("publicación");

    // Se espera bastante más de lo que tardaría un reintento inmediato, pero mucho menos
    // que el primer backoff (30 s): si el SDK reintentase, el contador subiría de 1.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        intentos.load(Ordering::SeqCst),
        1,
        "un PERMANENT no gasta ni un reintento"
    );

    // ── Lo que quedó escrito en la DLQ, byte a byte ──
    //
    // Se lee con async-nats crudo a propósito: el SDK no expone consumo de DLQ, y este
    // test comprueba precisamente los BYTES que produjo, no su interpretación.
    let raw = leer_ultimo_de_dlq(&url, "itrust3", &format!("dlq.{subject}"))
        .await
        .expect("el evento debería estar en la DLQ");
    let texto = String::from_utf8(raw).expect("UTF-8");

    // El mensaje de DLQ es el CloudEvent original ÍNTEGRO — 04-errors.md §3.
    let dlq_event: Event =
        flux::parse_event(texto.as_bytes()).expect("la DLQ guarda un evento válido");
    assert_eq!(dlq_event.id, publicado.id, "el id original se conserva");
    assert_eq!(dlq_event.dlqreason, Some(flux::DlqReason::Permanent));
    assert_eq!(
        dlq_event.dlqattempts,
        Some(1),
        "un PERMANENT muere en la 1ª entrega"
    );
    assert_eq!(dlq_event.dlqconsumer.as_deref(), Some(sub.durable.as_str()));
    assert!(dlq_event
        .dlqerror
        .as_deref()
        .is_some_and(|e| e.starts_with("PEDIDO_YA_CANCELADO: ")));

    // ORDEN NORMATIVO: las extensiones dlq* van ANTES de `data` — 01-envelope.md §6.
    // Es la divergencia real que tuvo el SDK de Node con `{...event, dlq*}`.
    let pos_data = texto.find(r#""data":"#).expect("data");
    for ext in [
        "dlqreason",
        "dlqattempts",
        "dlqconsumer",
        "dlqerror",
        "dlqtime",
    ] {
        let pos = texto.find(&format!("\"{ext}\":")).expect(ext);
        assert!(pos < pos_data, "{ext} debería ir antes de data:\n{texto}");
    }

    sub.unsubscribe();
    bus.close().await.expect("close");
}

/// El **presupuesto acotado** de 04-errors.md §2.1, extremo a extremo: un error con
/// `max_attempts = 2` reintenta UNA vez y a la segunda entrega va a la DLQ, sin gastar
/// las 6 del consumidor.
///
/// El presupuesto lo pone una regla del clasificador —que es como el SDK lo aplica a los
/// errores desconocidos— y el `retry_after` comprime a 300 ms el primer reintento. Ver
/// la nota sobre `retry_after` en el README: el segundo ya lo gobernaría el backoff.
#[tokio::test]
async fn un_presupuesto_acotado_va_a_la_dlq_en_la_segunda_entrega() {
    let url = broker!();
    let dominio = format!("itrust5x{}", std::process::id());
    let subject = subject(&dominio);

    let bus = connect(
        ConnectOptions::new(&url, "pedidos-api", "test", "1.0.0")
            .with_schema_base_url("https://schemas.internal")
            .with_classifier(flux::ClassifierOptions {
                rules: vec![Box::new(|_e| {
                    Some(
                        flux::Classification::new(flux::ErrorClass::Retryable, "PROVEEDOR_CAIDO")
                            .with_retry_after(Some(Duration::from_millis(300)))
                            .with_max_attempts(Some(2)),
                    )
                })],
                ..Default::default()
            }),
    )
    .await
    .expect("conexión");

    let intentos = Arc::new(AtomicU32::new(0));
    let contador = intentos.clone();

    let sub = bus
        .subscribe(&subject, move |_ev: Event, d: flux::Delivery| {
            let contador = contador.clone();
            async move {
                contador.fetch_add(1, Ordering::SeqCst);
                // El techo del CONSUMIDOR sigue siendo 6; el recorte es por error.
                assert_eq!(d.max_attempts, 6);
                // Un error CUALQUIERA de la aplicación, no un FluxError tipado: los
                // tipados ganan al clasificador (paso 1) y no llegarían a la regla.
                Err("proveedor caído".into())
            }
        })
        .await
        .expect("suscripción");

    bus.publish(&subject, &json!({ "pedidoId": "ped-2" }))
        .await
        .expect("publicación");

    let raw = esperar_dlq(&url, &dominio, &format!("dlq.{subject}"))
        .await
        .expect("debería acabar en la DLQ al agotar su presupuesto");
    let dlq_event = flux::parse_event(&raw).expect("evento de DLQ");

    assert_eq!(dlq_event.dlqreason, Some(flux::DlqReason::Retryable));
    assert_eq!(
        dlq_event.dlqattempts,
        Some(2),
        "dlqattempts es la entrega en que murió, no una propiedad de la clase"
    );
    assert!(dlq_event
        .dlqerror
        .as_deref()
        .is_some_and(|e| e.starts_with("PROVEEDOR_CAIDO: ")));
    assert_eq!(
        intentos.load(Ordering::SeqCst),
        2,
        "presupuesto acotado: 1 entrega + 1 reintento, no las 6 del consumidor"
    );

    sub.unsubscribe();
    bus.close().await.expect("close");
}

/// POISON: el mensaje no es interpretable, así que el handler **nunca llega a verlo**
/// — 04-errors.md §1.3.
#[tokio::test]
async fn un_mensaje_ilegible_no_llega_al_handler_y_va_a_la_dlq() {
    let url = broker!();
    let subject = subject("itrust6");

    let poisons = Arc::new(AtomicU32::new(0));
    let contador_poison = poisons.clone();
    let codigo: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sink_codigo = codigo.clone();

    let bus = connect(
        ConnectOptions::new(&url, "pedidos-api", "test", "1.0.0")
            .with_schema_base_url("https://schemas.internal")
            .on_poison(Arc::new(move |info: flux::PoisonInfo| {
                contador_poison.fetch_add(1, Ordering::SeqCst);
                *sink_codigo.lock().unwrap() = info.error.code().map(str::to_string);
            })),
    )
    .await
    .expect("conexión");

    let handler_llamado = Arc::new(AtomicU32::new(0));
    let contador_handler = handler_llamado.clone();
    let sub = bus
        .subscribe(&subject, move |_ev: Event, _d| {
            let contador = contador_handler.clone();
            async move {
                contador.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .expect("suscripción");

    // Se publica saltándose el SDK: es la única forma de meter algo que el SDK jamás
    // produciría — un productor roto, o alguien publicando a mano en el subject.
    publicar_crudo(
        &url,
        &subject,
        br#"{"specversion":"1.0","esto":"no es un CloudEvent"}"#,
    )
    .await
    .expect("publicación cruda");

    let raw = esperar_dlq(&url, "itrust6", &format!("dlq.{subject}"))
        .await
        .expect("el POISON debería acabar en la DLQ");
    let capturado = flux::parse_event(&raw).expect("el envelope sintético es válido");

    assert_eq!(capturado.dlqreason, Some(flux::DlqReason::Poison));
    assert_eq!(capturado.event_type, "com.flux.system.poison.capturado.v1");
    assert_eq!(capturado.tenantid, "system");
    // El cuerpo original se preserva para el forense.
    let data: serde_json::Value = capturado.data_as().expect("payload del captura-poison");
    assert_eq!(data["originalSubject"], json!(subject));
    assert!(data["rawBytes"].as_u64().unwrap() > 0);

    assert_eq!(
        poisons.load(Ordering::SeqCst),
        1,
        "on_poison debe despertar a alguien"
    );
    assert_eq!(
        codigo.lock().unwrap().as_deref(),
        Some("MISSING_REQUIRED_ATTRIBUTE")
    );
    assert_eq!(
        handler_llamado.load(Ordering::SeqCst),
        0,
        "el handler NUNCA ve un mensaje ilegible"
    );

    sub.unsubscribe();
    bus.close().await.expect("close");
}

/// Publica bytes arbitrarios saltándose el envelope del SDK.
async fn publicar_crudo(url: &str, subject: &str, payload: &[u8]) -> Result<(), String> {
    let client = async_nats::connect(url).await.map_err(|e| e.to_string())?;
    let js = async_nats::jetstream::new(client);
    let ack = js
        .publish(subject.to_string(), payload.to_vec().into())
        .await
        .map_err(|e| e.to_string())?;
    ack.await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Espera a que aparezca algo en la DLQ, con un techo razonable.
async fn esperar_dlq(url: &str, dominio: &str, subject: &str) -> Option<Vec<u8>> {
    for _ in 0..100 {
        if let Some(raw) = leer_ultimo_de_dlq(url, dominio, subject).await {
            return Some(raw);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

/// Lee el último mensaje de un subject de DLQ con la API cruda de NATS.
async fn leer_ultimo_de_dlq(url: &str, dominio: &str, subject: &str) -> Option<Vec<u8>> {
    use async_nats::jetstream::consumer::pull;
    use async_nats::jetstream::consumer::DeliverPolicy;
    use futures::StreamExt;

    let client = async_nats::connect(url).await.ok()?;
    let js = async_nats::jetstream::new(client);
    let stream = js.get_stream(flux::dlq_stream_name(dominio)).await.ok()?;
    let consumer = stream
        .create_consumer(pull::Config {
            // `LastPerSubject`, NO `All`. Con `All` + fetch(10) se leen los diez
            // mensajes MÁS ANTIGUOS del stream y se toma el décimo, así que en cuanto
            // una ejecución previa deja más de diez muertos en el subject, el test
            // compara contra un evento de otra ejecución y falla sin motivo aparente.
            //
            // Es la clase de test que solo falla en la máquina de alguien que ya lo
            // había ejecutado antes, que es la peor.
            deliver_policy: DeliverPolicy::LastPerSubject,
            filter_subject: subject.to_string(),
            ..Default::default()
        })
        .await
        .ok()?;
    let mut batch = consumer.fetch().max_messages(1).messages().await.ok()?;
    let mut ultimo = None;
    while let Some(Ok(m)) = batch.next().await {
        ultimo = Some(m.payload.to_vec());
        let _ = m.ack().await;
    }
    ultimo
}

/// El contexto se propaga solo: un publish dentro del handler hereda correlationid y
/// causationid sin que nadie los escriba — 01-envelope.md §5.
#[tokio::test]
async fn el_contexto_se_propaga_del_evento_entrante_al_derivado() {
    let url = broker!();
    let bus = bus(&url, "pedidos-api").await;
    let entrante = subject("itrust4");
    let derivado = "itrust4b.factura.v1.emitida".to_string();

    let derivado_recibido: Arc<Mutex<Option<Event>>> = Arc::new(Mutex::new(None));
    let sink = derivado_recibido.clone();
    let sub_derivado = bus
        .subscribe(&derivado, move |ev: Event, _d| {
            let sink = sink.clone();
            async move {
                *sink.lock().unwrap() = Some(ev);
                Ok(())
            }
        })
        .await
        .expect("suscripción derivada");

    let publicador = bus.clone();
    let destino = derivado.clone();
    let sub = bus
        .subscribe(&entrante, move |_ev: Event, _d| {
            let bus = publicador.clone();
            let destino = destino.clone();
            async move {
                // Ni correlationid ni causationid ni tenantid se escriben aquí.
                bus.publish(&destino, &json!({ "facturaId": "fac-1" }))
                    .await?;
                Ok(())
            }
        })
        .await
        .expect("suscripción entrante");

    let origen = bus
        .publish(&entrante, &json!({ "pedidoId": "ped-9" }))
        .await
        .expect("publicación");

    let hijo = esperar(&derivado_recibido)
        .await
        .expect("el evento derivado debería llegar");

    // correlationid se propaga SIN MODIFICAR por toda la cadena.
    assert_eq!(hijo.correlationid, origen.correlationid);
    // causationid es el id del evento en curso, no el suyo propio.
    assert_eq!(hijo.causationid.as_deref(), Some(origen.id.as_str()));
    // El tenant se hereda del evento que lo causó.
    assert_eq!(hijo.tenantid, origen.tenantid);
    assert_ne!(hijo.id, origen.id);

    sub.unsubscribe();
    sub_derivado.unsubscribe();
    bus.close().await.expect("close");
}

/// Espera a que el sink reciba algo, con un techo razonable.
async fn esperar(sink: &Arc<Mutex<Option<Event>>>) -> Option<Event> {
    for _ in 0..100 {
        if let Some(ev) = sink.lock().unwrap().clone() {
            return Some(ev);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

// ─── Fase 5: firma, métricas y aislamiento de tenant ─────────────────────────

/// 09-multitenancy.md §3.3: en `strict`, suscribirse sin filtro de tenant es un **error
/// de configuración**, no un descuido silencioso.
///
/// Esto importa más de lo que parece: un filtro que hay que acordarse de poner es un
/// filtro que alguien olvidará, y el fallo —ver los datos de otro tenant— **no produce
/// ningún error**: produce un incidente de privacidad que se descubre semanas después.
#[tokio::test]
async fn strict_sin_tenant_es_un_error_de_configuracion() {
    let url = broker!();
    let bus = connect(
        ConnectOptions::new(&url, "pedidos-api", "test", "1.0.0")
            .with_tenant_isolation(flux::TenantIsolation::Strict)
            .with_schema_base_url("https://schemas.internal"),
    )
    .await
    .expect("conexión");

    let err = bus
        .subscribe(&subject("itrust8"), |_ev: Event, _d| async { Ok(()) })
        .await
        .unwrap_err();

    assert!(matches!(err, FluxError::TenantIsolation { .. }), "{err:?}");
    assert!(err.to_string().contains("TODOS los tenants"), "{err}");

    bus.close().await.expect("close");
}

/// `"system"` NO cuenta como filtro de tenant: es la AUSENCIA de tenant, reservada a los
/// eventos de plataforma — 09-multitenancy.md §5.
#[tokio::test]
async fn strict_rechaza_system_como_filtro() {
    let url = broker!();
    let bus = connect(
        ConnectOptions::new(&url, "pedidos-api", "test", "1.0.0")
            .with_tenant_id("system")
            .with_tenant_isolation(flux::TenantIsolation::Strict)
            .with_schema_base_url("https://schemas.internal"),
    )
    .await
    .expect("conexión");

    let err = bus
        .subscribe(&subject("itrust9"), |_ev: Event, _d| async { Ok(()) })
        .await
        .unwrap_err();
    assert!(matches!(err, FluxError::TenantIsolation { .. }), "{err:?}");
    assert!(err.to_string().contains("AUSENCIA de tenant"), "{err}");

    bus.close().await.expect("close");
}

/// El evento de otro tenant se **confirma y se descarta antes del handler**: no es un
/// fallo, no es para nosotros — 09-multitenancy.md §3.2.
#[tokio::test]
async fn el_evento_de_otro_tenant_no_llega_al_handler() {
    let url = broker!();
    let subject = subject("itrust10");

    let consumidor = connect(
        ConnectOptions::new(&url, "facturacion-api", "test", "1.0.0")
            .with_tenant_id("acme")
            .with_tenant_isolation(flux::TenantIsolation::Strict)
            .with_schema_base_url("https://schemas.internal"),
    )
    .await
    .expect("conexión");

    let vistos: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = vistos.clone();
    let sub = consumidor
        .subscribe(&subject, move |ev: Event, _d| {
            let sink = sink.clone();
            async move {
                sink.lock().unwrap().push(ev.tenantid.clone());
                Ok(())
            }
        })
        .await
        .expect("suscripción");

    let productor = bus(&url, "pedidos-api").await;
    productor
        .publish_with(
            &subject,
            &json!({ "pedidoId": "de-globex" }),
            PublishOptions::default().with_tenant_id("globex"),
        )
        .await
        .expect("publicación ajena");
    productor
        .publish_with(
            &subject,
            &json!({ "pedidoId": "de-acme" }),
            PublishOptions::default().with_tenant_id("acme"),
        )
        .await
        .expect("publicación propia");

    for _ in 0..100 {
        if !vistos.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Margen para que un ajeno mal filtrado tuviese tiempo de aparecer.
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(
        *vistos.lock().unwrap(),
        vec!["acme".to_string()],
        "el evento de globex debería haberse ACKeado y descartado antes del handler"
    );

    sub.unsubscribe();
    consumidor.close().await.expect("close");
    productor.close().await.expect("close");
}

/// 08-observability.md: los nombres y las etiquetas son contrato entre SDKs, así que se
/// comprueban sobre la salida real de un ciclo publicar → consumir.
#[tokio::test]
async fn las_metricas_del_protocolo_se_emiten_al_publicar_y_consumir() {
    let url = broker!();
    let subject = subject("itrust11");
    let metrics = Arc::new(flux::InMemoryMetrics::new());

    let bus = connect(
        ConnectOptions::new(&url, "pedidos-api", "test", "1.0.0")
            .with_tenant_id("acme")
            .with_metrics(metrics.clone())
            .with_schema_base_url("https://schemas.internal"),
    )
    .await
    .expect("conexión");

    let recibido: Arc<Mutex<Option<Event>>> = Arc::new(Mutex::new(None));
    let sink = recibido.clone();
    let sub = bus
        .subscribe(&subject, move |ev: Event, _d| {
            let sink = sink.clone();
            async move {
                *sink.lock().unwrap() = Some(ev);
                Ok(())
            }
        })
        .await
        .expect("suscripción");

    bus.publish(&subject, &json!({ "pedidoId": "ped-1" }))
        .await
        .expect("publicación");
    esperar(&recibido).await.expect("el evento debería llegar");

    let salida = metrics.render();
    assert!(
        salida.contains(&format!(
            "flux_events_published_total{{subject=\"{subject}\",outcome=\"ok\"}} 1"
        )),
        "{salida}"
    );
    assert!(
        salida.contains(&format!(
            "flux_events_consumed_total{{subject=\"{subject}\",consumer=\"{}\",outcome=\"ok\"}} 1",
            sub.durable
        )),
        "{salida}"
    );
    assert!(salida.contains("flux_connection_state 1"), "{salida}");
    assert!(salida.contains("flux_consumer_pending{"), "{salida}");
    assert!(
        salida.contains("flux_event_handler_duration_seconds_bucket{"),
        "{salida}"
    );
    // §2.2: NUNCA se etiqueta por tenant, aunque el bus tenga uno configurado.
    assert!(!salida.contains("acme"), "{salida}");

    sub.unsubscribe();
    bus.close().await.expect("close");

    // close() marca la conexión como caída: sin esto, un panel no distingue "sano" de
    // "el proceso se fue".
    assert!(metrics.render().contains("flux_connection_state 0"));
}

/// El ciclo completo de 07-signing.md contra un broker real: firmar al publicar,
/// verificar al consumir, y que la firma sobreviva al viaje por JetStream.
#[cfg(feature = "signing")]
#[tokio::test]
async fn un_evento_firmado_viaja_y_verifica_end_to_end() {
    let url = broker!();
    let subject = subject("itrust12");
    let (priv_pem, pub_pem) = flux::generate_key_pair();

    let productor = connect(
        ConnectOptions::new(&url, "pedidos-api", "test", "1.0.0")
            .with_tenant_id("acme")
            .with_schema_base_url("https://schemas.internal")
            .with_signing(
                flux::SigningOptions::default().with_private_key(&priv_pem, "pedidos-api-1"),
            ),
    )
    .await
    .expect("conexión del productor");

    let consumidor = connect(
        ConnectOptions::new(&url, "facturacion-api", "test", "1.0.0")
            .with_tenant_id("acme")
            .with_schema_base_url("https://schemas.internal")
            .with_signing(
                flux::SigningOptions::default()
                    .with_public_key("pedidos-api-1", &pub_pem)
                    .with_verify(flux::VerificationMode::Require),
            ),
    )
    .await
    .expect("conexión del consumidor");

    let recibido: Arc<Mutex<Option<Event>>> = Arc::new(Mutex::new(None));
    let sink = recibido.clone();
    let sub = consumidor
        .subscribe(&subject, move |ev: Event, _d| {
            let sink = sink.clone();
            async move {
                *sink.lock().unwrap() = Some(ev);
                Ok(())
            }
        })
        .await
        .expect("suscripción");

    let publicado = productor
        .publish(&subject, &json!({ "pedidoId": "ped-firmado" }))
        .await
        .expect("publicación");
    assert_eq!(publicado.signkeyid.as_deref(), Some("pedidos-api-1"));

    let ev = esperar(&recibido)
        .await
        .expect("el evento firmado debería llegar y verificar");
    assert_eq!(ev.signature, publicado.signature, "la firma no se altera");

    sub.unsubscribe();
    productor.close().await.expect("close");
    consumidor.close().await.expect("close");
}

/// En modo `require`, un evento **sin firma** es POISON y acaba en la DLQ con
/// `MISSING_SIGNATURE` — 07-signing.md §7.
#[cfg(feature = "signing")]
#[tokio::test]
async fn require_manda_a_la_dlq_un_evento_sin_firma() {
    let url = broker!();
    let dominio = "itrust13";
    let subject = subject(dominio);
    let (_, pub_pem) = flux::generate_key_pair();

    let consumidor = connect(
        ConnectOptions::new(&url, "facturacion-api", "test", "1.0.0")
            .with_tenant_id("acme")
            .with_schema_base_url("https://schemas.internal")
            .with_signing(
                flux::SigningOptions::default()
                    .with_public_key("pedidos-api-1", &pub_pem)
                    .with_verify(flux::VerificationMode::Require),
            ),
    )
    .await
    .expect("conexión del consumidor");

    let llego = Arc::new(AtomicU32::new(0));
    let contador = llego.clone();
    let sub = consumidor
        .subscribe(&subject, move |_ev: Event, _d| {
            let contador = contador.clone();
            async move {
                contador.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .expect("suscripción");

    // El productor NO firma.
    let productor = bus(&url, "pedidos-api").await;
    productor
        .publish(&subject, &json!({ "pedidoId": "sin-firma" }))
        .await
        .expect("publicación");

    let raw = esperar_dlq(&url, dominio, &format!("dlq.{subject}"))
        .await
        .expect("el evento sin firma debería acabar en la DLQ");
    let texto = String::from_utf8_lossy(&raw);
    assert!(texto.contains("MISSING_SIGNATURE"), "{texto}");
    // El handler nunca lo vio: la firma se comprueba ANTES.
    assert_eq!(llego.load(Ordering::SeqCst), 0);

    sub.unsubscribe();
    productor.close().await.expect("close");
    consumidor.close().await.expect("close");
}

/// 07-signing.md §7.1: **`warn` DEBE ser observable.** El evento se acepta y llega al
/// handler, pero `flux_events_consumed_total{outcome="invalid_signature"}` se emite igual.
///
/// Sin esa métrica, `warn` es inútil para lo único que existe —pilotar la migración—: la
/// pregunta "¿cuántos eventos siguen sin firma y de qué productores?" habría que buscarla
/// a mano en los logs de siete servicios.
#[cfg(feature = "signing")]
#[tokio::test]
async fn warn_acepta_el_evento_pero_lo_cuenta_como_invalid_signature() {
    let url = broker!();
    let subject = subject("itrust14");
    let (_, pub_pem) = flux::generate_key_pair();
    let metrics = Arc::new(flux::InMemoryMetrics::new());

    let consumidor = connect(
        ConnectOptions::new(&url, "facturacion-api", "test", "1.0.0")
            .with_tenant_id("acme")
            .with_metrics(metrics.clone())
            .with_schema_base_url("https://schemas.internal")
            .with_signing(
                flux::SigningOptions::default()
                    .with_public_key("pedidos-api-1", &pub_pem)
                    .with_verify(flux::VerificationMode::Warn),
            ),
    )
    .await
    .expect("conexión del consumidor");

    let recibido: Arc<Mutex<Option<Event>>> = Arc::new(Mutex::new(None));
    let sink = recibido.clone();
    let sub = consumidor
        .subscribe(&subject, move |ev: Event, _d| {
            let sink = sink.clone();
            async move {
                *sink.lock().unwrap() = Some(ev);
                Ok(())
            }
        })
        .await
        .expect("suscripción");

    // El productor NO firma.
    let productor = bus(&url, "pedidos-api").await;
    productor
        .publish(&subject, &json!({ "pedidoId": "sin-firma" }))
        .await
        .expect("publicación");

    // `warn` acepta: el handler SÍ lo ve, y no hay nada en la DLQ.
    esperar(&recibido)
        .await
        .expect("en warn el evento debe llegar al handler");

    let salida = metrics.render();
    assert!(
        salida.contains(&format!(
            "flux_events_consumed_total{{subject=\"{subject}\",consumer=\"{}\",outcome=\"invalid_signature\"}} 1",
            sub.durable
        )),
        "§7.1 exige la métrica, no solo el log:\n{salida}"
    );
    // Y NO se cuenta además como `ok`: si se contase dos veces, `sum by (outcome)` dejaría
    // de cuadrar con el total de eventos consumidos.
    assert!(
        !salida.contains(&format!(
            "flux_events_consumed_total{{subject=\"{subject}\",consumer=\"{}\",outcome=\"ok\"}}",
            sub.durable
        )),
        "{salida}"
    );

    sub.unsubscribe();
    productor.close().await.expect("close");
    consumidor.close().await.expect("close");
}
