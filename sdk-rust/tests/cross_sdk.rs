//! Los fixtures compartidos de `conformance/cases/`, ejecutados contra este SDK.
//!
//! No son tests inventados para Rust: son **los mismos ficheros** que verifican Node,
//! Python, Go, Java y .NET. Si este SDK produce otros bytes, el ecosistema deja de ser
//! reproducible y se rompen el replay verbatim, la firma futura del evento, la
//! deduplicación por hash y esta misma suite.
//!
//! No requieren broker: son puro envelope.

use std::path::PathBuf;

use chrono::{DateTime, TimeZone, Utc};
use flux::{
    build_event, data_to_raw, serialize, strip_dlq_extensions, to_dlq_event, BuildEventInput,
    DataClassification, DlqInfo, DlqReason, ALLOWED_ROOT_ATTRIBUTES,
};
use serde_json::Value;

fn fixture(nombre: &str) -> Value {
    let ruta = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../conformance/cases")
        .join(nombre);
    let texto = std::fs::read_to_string(&ruta)
        .unwrap_or_else(|e| panic!("no se pudo leer {}: {e}", ruta.display()));
    serde_json::from_str(&texto).expect("el fixture es JSON válido")
}

/// Las claves de la raíz, **en orden**. Depende de la feature `preserve_order` de
/// serde_json: sin ella este test no podría existir.
fn claves(json: &str) -> Vec<String> {
    let raiz: serde_json::Map<String, Value> = serde_json::from_str(json).expect("objeto JSON");
    raiz.keys().cloned().collect()
}

fn millis_a_utc(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).single().expect("instante")
}

/// `conformance/cases/cross-sdk-envelope.json`
#[test]
fn cross_sdk_envelope() {
    let f = fixture("cross-sdk-envelope.json");
    let input = &f["fixture"]["input"];
    let esperado = &f["fixture"]["expectedAttributes"];

    let evento = build_event(BuildEventInput {
        subject: input["subject"].as_str().unwrap().to_string(),
        data: data_to_raw(&input["data"]).unwrap(),
        id: input["id"].as_str().unwrap().to_string(),
        source: input["source"].as_str().unwrap().to_string(),
        producerversion: input["producerversion"].as_str().unwrap().to_string(),
        tenantid: input["tenantid"].as_str().unwrap().to_string(),
        dataclassification: DataClassification::from_str_exact(
            input["dataclassification"].as_str().unwrap(),
        )
        .unwrap(),
        dataschema: input["dataschema"].as_str().unwrap().to_string(),
        correlationid: input["correlationid"].as_str().unwrap().to_string(),
        time: Some(millis_a_utc(input["timeMillis"].as_i64().unwrap())),
        aggregate_id: Some(input["aggregateId"].as_str().unwrap().to_string()),
        causationid: None,
        partitionkey: None,
        traceparent: None,
        tracestate: None,
    })
    .expect("el fixture debe construir un evento válido");

    // ── expectedAttributes ──
    assert_eq!(
        evento.specversion,
        esperado["specversion"].as_str().unwrap()
    );
    assert_eq!(evento.event_type, esperado["type"].as_str().unwrap());
    assert_eq!(
        evento.datacontenttype,
        esperado["datacontenttype"].as_str().unwrap()
    );
    assert_eq!(evento.aggregate_id.as_deref(), esperado["subject"].as_str());
    assert_eq!(
        evento.partitionkey.as_deref(),
        esperado["partitionkey"].as_str()
    );

    // La divergencia `time-default-formatters-disagree`: Go recortaba a `.41Z` y Python
    // daba `.410000+00:00`. Los seis SDKs producen ahora esta cadena exacta.
    assert_eq!(evento.time, esperado["time"].as_str().unwrap());

    // ── assertions ──
    let json = String::from_utf8(serialize(&evento).unwrap()).unwrap();
    let raiz: serde_json::Map<String, Value> = serde_json::from_str(&json).unwrap();

    // time-exactly-three-decimals
    let (fecha, resto) = evento.time.split_once('T').expect("separador T");
    assert_eq!(fecha.len(), 10);
    assert!(resto.ends_with('Z') && resto.len() == 13, "{}", evento.time);
    assert_eq!(&resto[8..9], ".");
    assert!(
        resto[9..12].chars().all(|c| c.is_ascii_digit()),
        "{}",
        evento.time
    );

    // no-null-attributes — §3.3: un opcional se OMITE, nunca vale null.
    assert!(raiz.values().all(|v| !v.is_null()), "{json}");

    // no-unknown-root-attributes
    for k in raiz.keys() {
        assert!(
            ALLOWED_ROOT_ATTRIBUTES.contains(&k.as_str()),
            "atributo raíz desconocido: {k}"
        );
    }

    // data-is-last — 01-envelope.md §6
    assert_eq!(claves(&json).last().unwrap(), "data");
}

/// `conformance/cases/cross-sdk-dlq-envelope.json`
///
/// Éste es el fixture que nació de una divergencia real: Node construía el evento de DLQ
/// con `{...event, dlq*}` y dejaba las extensiones **después** de `data`. Lo encontró el
/// port a Java, no la suite.
#[test]
fn cross_sdk_dlq_envelope() {
    let f = fixture("cross-sdk-dlq-envelope.json");
    let input = &f["fixture"]["input"];
    let dlq = &input["dlq"];

    let hora = DateTime::parse_from_rfc3339(input["time"].as_str().unwrap())
        .unwrap()
        .with_timezone(&Utc);

    let original = build_event(BuildEventInput {
        subject: input["subject"].as_str().unwrap().to_string(),
        data: data_to_raw(&input["data"]).unwrap(),
        id: input["id"].as_str().unwrap().to_string(),
        source: input["source"].as_str().unwrap().to_string(),
        producerversion: input["producerversion"].as_str().unwrap().to_string(),
        tenantid: input["tenantid"].as_str().unwrap().to_string(),
        dataclassification: DataClassification::from_str_exact(
            input["dataclassification"].as_str().unwrap(),
        )
        .unwrap(),
        dataschema: input["dataschema"].as_str().unwrap().to_string(),
        correlationid: input["correlationid"].as_str().unwrap().to_string(),
        time: Some(hora),
        // El fixture de DLQ no lleva aggregateId: así el orden esperado no incluye
        // `subject` ni `partitionkey`, y se comprueba que los opcionales se OMITEN.
        aggregate_id: None,
        causationid: None,
        partitionkey: None,
        traceparent: None,
        tracestate: None,
    })
    .expect("el fixture debe construir un evento válido");

    let dlq_event = to_dlq_event(
        original.clone(),
        &DlqInfo {
            reason: match dlq["reason"].as_str().unwrap() {
                "retryable" => DlqReason::Retryable,
                "permanent" => DlqReason::Permanent,
                _ => DlqReason::Poison,
            },
            attempts: u32::try_from(dlq["attempts"].as_u64().unwrap()).unwrap(),
            consumer: dlq["consumer"].as_str().unwrap().to_string(),
            error: dlq["error"].as_str().unwrap().to_string(),
        },
        Utc::now(),
    );

    let json = String::from_utf8(serialize(&dlq_event).unwrap()).unwrap();

    // ── expectedKeyOrder: la secuencia EXACTA que producen los otros cinco SDKs ──
    let esperado: Vec<String> = f["fixture"]["expectedKeyOrder"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        claves(&json),
        esperado,
        "orden de claves del evento de DLQ:\n{json}"
    );

    // ── assertions ──
    let claves = claves(&json);
    // data-is-last
    assert_eq!(claves.last().unwrap(), "data");
    // dlq-extensions-before-data
    let pos_data = claves.iter().position(|k| k == "data").unwrap();
    for ext in [
        "dlqreason",
        "dlqattempts",
        "dlqconsumer",
        "dlqerror",
        "dlqtime",
    ] {
        let pos = claves.iter().position(|k| k == ext).unwrap();
        assert!(pos < pos_data, "{ext} debería ir antes de data");
    }
    // id-preserved
    assert_eq!(dlq_event.id, original.id);
    // strip-yields-original — si no vuelve al original exacto, el replay no es verbatim.
    let devuelto = strip_dlq_extensions(dlq_event);
    assert_eq!(devuelto, original);
    assert_eq!(serialize(&devuelto).unwrap(), serialize(&original).unwrap());
}

/// `conformance/cases/consumer-config.json`: la config canónica, contrastada contra las
/// constantes de este SDK.
#[test]
fn consumer_config_canonica() {
    let f = fixture("consumer-config.json");
    let pedida = &f["when"]["requestedConfig"];
    let efectiva = &f["then"]["serverEffectiveConfig"];

    // El fixture está en NANOSEGUNDOS, que es la unidad de la API de JetStream.
    let nanos = |v: &Value| v.as_u64().unwrap();
    let lista = |v: &Value| -> Vec<u64> { v.as_array().unwrap().iter().map(nanos).collect() };

    // Lo que este SDK pide DEBE ser exactamente lo que el fixture verificó.
    assert_eq!(
        u64::try_from(flux::DEFAULT_ACK_WAIT.as_nanos()).unwrap(),
        nanos(&pedida["ack_wait"])
    );
    assert_eq!(
        u64::from(flux::DEFAULT_MAX_DELIVER),
        nanos(&pedida["max_deliver"])
    );
    assert_eq!(
        u64::from(flux::DEFAULT_MAX_ACK_PENDING),
        nanos(&pedida["max_ack_pending"])
    );
    assert_eq!(
        flux::CANONICAL_BACKOFF
            .iter()
            .map(|d| u64::try_from(d.as_nanos()).unwrap())
            .collect::<Vec<_>>(),
        lista(&pedida["backoff"])
    );
    assert_eq!(pedida["ack_policy"], "explicit");

    // Y el servidor devolvió lo mismo: por eso `assert_config_honored` puede exigirlo.
    assert_eq!(nanos(&efectiva["ack_wait"]), nanos(&pedida["ack_wait"]));
    assert_eq!(lista(&efectiva["backoff"]), lista(&pedida["backoff"]));

    // El contraejemplo del fixture es la trampa: pidiendo backoff[0] = 1 s, el servidor
    // devuelve ack_wait = 1 s sin avisar. Este SDK no puede caer en ella porque
    // CANONICAL_BACKOFF es const y un test fija la invariante.
    let trampa = &f["counterExample"];
    assert_ne!(
        nanos(&trampa["observedEffectiveConfig"]["ack_wait"]),
        nanos(&trampa["requestedConfig"]["ack_wait"]),
        "el contraejemplo debe seguir mostrando la sobrescritura silenciosa"
    );
    assert_eq!(
        nanos(&trampa["observedEffectiveConfig"]["ack_wait"]),
        lista(&trampa["requestedConfig"]["backoff"])[0],
        "ack_wait efectivo == backoff[0] solicitado"
    );
    // La invariante: ack_wait DEBE ser backoff[0], porque el servidor lo sobrescribe.
    assert_eq!(flux::DEFAULT_ACK_WAIT, flux::CANONICAL_BACKOFF[0]);
    // 1 entrega + una por entrada de backoff.
    assert_eq!(
        flux::DEFAULT_MAX_DELIVER as usize,
        flux::CANONICAL_BACKOFF.len() + 1
    );
    assert_eq!(flux::total_time_to_dlq().as_secs(), 3090);
}
