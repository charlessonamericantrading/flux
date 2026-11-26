//! Arnés de conformidad cruzada — SDK de Rust.
//! Contrato: `conformance/harness/README.md`.
//!
//! Lee **UNA** operación por stdin, escribe **UN** objeto JSON por stdout y sale con **0
//! siempre**: un exit distinto de 0 significa que el arnés está roto, no que el caso
//! falló. Los diagnósticos —incluido el ruido de `cargo run`— van a stderr, que el runner
//! ignora.
//!
//! Deliberadamente delgado: **no rellena nada**. `id`, `time` y `dlqtime` vienen del
//! vector; si los generase el SDK, los bytes no serían comparables entre lenguajes.
//!
//! Además de `build`, `dlq`, `sign`, `verify` y `parse`, implementa las cuatro operaciones de la
//! capacidad `state` (`state-build`, `state-tombstone`, `state-subject` y `state-dlq`) y las cinco de
//! la capacidad `rpc` (`rpc-request`, `rpc-reply`, `rpc-error`, `rpc-operation` y `rpc-timeout`:
//! `conformance/cases/cross-sdk-rpc-envelope.json`): el runner las ejecuta solo en los SDKs que las
//! declaran en `protocol.json`.
//!
//! ```text
//! echo '{"op":"build","event":{…}}' | cargo run --quiet --bin conformance-harness --features signing
//! ```

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::io::Read;

use chrono::{DateTime, Utc};
use flux::signing::{Signer, SigningOptions, VerificationMode, Verifier};
use flux::{
    build_event, build_rpc_error, build_rpc_reply, build_rpc_request, build_state_event,
    build_state_tombstone, data_to_raw, parse_event, resolve_timeout_ms, rpc_dataschema,
    rpc_subject, serialize, state_dataschema, state_subject, to_dlq_event, to_state_dlq_event,
    BuildEventInput, DataClassification, DlqInfo, DlqReason, ErrorClass, Event, FluxError, ReplyTo,
    RpcErrorBody, RpcErrorInput, RpcReplyInput, RpcRequestInput, RpcRequestRef, RpcSide,
    StateEventInput, StateTombstoneInput,
};
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{json, Value};

// ─── Entrada ─────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct Entrada {
    op: String,
    #[serde(default)]
    event: Option<EventoEntrada>,
    #[serde(default)]
    dlq: Option<DlqEntrada>,
    #[serde(default)]
    signing: Option<FirmaEntrada>,
    #[serde(default, rename = "signFirst")]
    sign_first: bool,
    #[serde(default)]
    bytes: Option<String>,
    #[serde(default, rename = "publicKeys")]
    public_keys: HashMap<String, String>,
    #[serde(default)]
    mode: Option<String>,
}

/// Los atributos llegan **ya decididos**. `data` se recoge como `RawValue` para que los
/// bytes del payload sean los del vector y no una reserialización con otro orden de claves
/// — 01-envelope.md §6.
#[derive(Deserialize)]
struct EventoEntrada {
    subject: String,
    id: String,
    source: String,
    time: String,
    dataschema: String,
    correlationid: String,
    tenantid: String,
    producerversion: String,
    dataclassification: String,
    #[serde(default, rename = "aggregateId")]
    aggregate_id: Option<String>,
    #[serde(default)]
    causationid: Option<String>,
    #[serde(default)]
    partitionkey: Option<String>,
    #[serde(default)]
    traceparent: Option<String>,
    #[serde(default)]
    tracestate: Option<String>,
    data: Box<RawValue>,
}

#[derive(Deserialize)]
struct DlqEntrada {
    reason: DlqReason,
    attempts: u32,
    consumer: String,
    error: String,
    /// Lo fija el vector: si lo pusiera el SDK, los bytes no serían comparables ni entre
    /// ejecuciones ni entre lenguajes.
    dlqtime: String,
}

// ─── Entrada de estado (capacidad `state`) ───────────────────────────────────
//
// PLANA, como el `input` de los fixtures de `cross-sdk-state-envelope.json` más el `op`. Se
// parsea del mismo texto que `Entrada` (los campos que una no conoce, la otra los ignora) y
// `data` sigue siendo `RawValue`: los bytes del payload son los del vector.

/// `state-subject`: solo lo que forma el subject de NATS.
#[derive(Deserialize)]
struct SujetoEntrada {
    contract: String,
    #[serde(rename = "tenantId")]
    tenant_id: String,
    key: String,
}

/// `state-build`, y el `original` de `state-dlq`. `dataschema` NO viene: lo deriva el SDK.
#[derive(Deserialize)]
struct PutEntrada {
    contract: String,
    #[serde(rename = "tenantId")]
    tenant_id: String,
    key: String,
    id: String,
    source: String,
    time: String,
    correlationid: String,
    producerversion: String,
    classification: String,
    #[serde(default)]
    causationid: Option<String>,
    #[serde(default)]
    traceparent: Option<String>,
    #[serde(default)]
    tracestate: Option<String>,
    data: Box<RawValue>,
}

/// `state-tombstone`. La lápida no lleva `classification` ni `data`: son fijos.
#[derive(Deserialize)]
struct LapidaEntrada {
    contract: String,
    #[serde(rename = "tenantId")]
    tenant_id: String,
    key: String,
    id: String,
    source: String,
    time: String,
    correlationid: String,
    producerversion: String,
    #[serde(rename = "aggregateVersion")]
    aggregate_version: u64,
    #[serde(default)]
    causationid: Option<String>,
    #[serde(default)]
    traceparent: Option<String>,
    #[serde(default)]
    tracestate: Option<String>,
}

/// `state-dlq`: el original (una entrada de `state-build`) y la revisión del stream. Los `dlq*`
/// se leen de `Entrada::dlq`, igual que en `dlq`.
#[derive(Deserialize)]
struct DlqEstadoEntrada {
    original: PutEntrada,
    revision: u64,
}

// ─── Entrada de petición/respuesta (capacidad `rpc`) ─────────────────────────
//
// PLANA, como el `input` de los fixtures de `cross-sdk-rpc-envelope.json` más el `op`. `data` sigue
// siendo `RawValue`: los bytes del payload son los del vector. `dataschema` NO viene: lo deriva el SDK
// de la operación, porque es parte de lo que se compara.

/// `rpc-request`.
#[derive(Deserialize)]
struct RpcPeticionEntrada {
    operation: String,
    #[serde(rename = "tenantId")]
    tenant_id: String,
    #[serde(default, rename = "aggregateId")]
    aggregate_id: Option<String>,
    id: String,
    source: String,
    time: String,
    correlationid: String,
    producerversion: String,
    classification: String,
    #[serde(default)]
    causationid: Option<String>,
    #[serde(default)]
    traceparent: Option<String>,
    #[serde(default)]
    tracestate: Option<String>,
    data: Box<RawValue>,
}

/// La petición a la que contestan `rpc-reply` y `rpc-error`: lo que la respuesta **hereda**. El
/// `correlationid` de primer nivel que traen los vectores es redundante y aquí no se usa.
#[derive(Deserialize)]
struct RpcReferenciaEntrada {
    operation: String,
    id: String,
    correlationid: String,
    tenantid: String,
    #[serde(default)]
    subject: Option<String>,
    #[serde(default)]
    partitionkey: Option<String>,
}

impl From<RpcReferenciaEntrada> for RpcRequestRef {
    fn from(r: RpcReferenciaEntrada) -> Self {
        Self {
            operation: r.operation,
            id: r.id,
            correlationid: r.correlationid,
            tenantid: r.tenantid,
            subject: r.subject,
            partitionkey: r.partitionkey,
        }
    }
}

/// `rpc-reply`.
#[derive(Deserialize)]
struct RpcRespuestaEntrada {
    request: RpcReferenciaEntrada,
    id: String,
    source: String,
    time: String,
    producerversion: String,
    classification: String,
    #[serde(default)]
    traceparent: Option<String>,
    #[serde(default)]
    tracestate: Option<String>,
    data: Box<RawValue>,
}

/// El `error` de `rpc-error`.
#[derive(Deserialize)]
struct RpcCuerpoErrorEntrada {
    class: String,
    code: String,
    message: String,
    /// Como `timeoutMs`: un entero escrito con `.0` o con exponente es un entero para JavaScript.
    #[serde(default, rename = "retryAfterMs")]
    retry_after_ms: Option<Value>,
}

/// `rpc-error`.
#[derive(Deserialize)]
struct RpcErrorEntrada {
    request: RpcReferenciaEntrada,
    id: String,
    source: String,
    time: String,
    producerversion: String,
    #[serde(default)]
    traceparent: Option<String>,
    #[serde(default)]
    tracestate: Option<String>,
    error: RpcCuerpoErrorEntrada,
}

#[derive(Deserialize)]
struct FirmaEntrada {
    #[serde(rename = "privateKeyPem")]
    private_key_pem: String,
    #[serde(rename = "keyId")]
    key_id: String,
}

// ─── Fallos ──────────────────────────────────────────────────────────────────

/// Un fallo **reportado**, nunca propagado: el contrato exige exit 0 y el error en el JSON.
struct Fallo {
    code: String,
    detail: String,
}

impl Fallo {
    fn nuevo(code: &str, detail: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            detail: detail.into(),
        }
    }
}

impl From<FluxError> for Fallo {
    fn from(e: FluxError) -> Self {
        Self {
            // El código estable del SDK es lo que se compara entre lenguajes; el texto no.
            code: e.code().unwrap_or("ERROR").to_string(),
            detail: e.to_string(),
        }
    }
}

// ─── Operaciones ─────────────────────────────────────────────────────────────

fn evento(e: &Entrada) -> Result<&EventoEntrada, Fallo> {
    e.event
        .as_ref()
        .ok_or_else(|| Fallo::nuevo("HARNESS_INPUT", "la operación requiere `event`"))
}

fn cuerpo(e: &Entrada) -> Result<Vec<u8>, Fallo> {
    let b64 = e
        .bytes
        .as_deref()
        .ok_or_else(|| Fallo::nuevo("HARNESS_INPUT", "la operación requiere `bytes`"))?;
    b64_decode(b64).map_err(|m| Fallo::nuevo("HARNESS_INPUT", m))
}

/// Una clasificación fuera del enum es `INVALID_DATACLASSIFICATION`, como en el SDK.
fn clasificacion(texto: &str) -> Result<DataClassification, Fallo> {
    DataClassification::from_str_exact(texto).ok_or_else(|| {
        Fallo::nuevo(
            "INVALID_DATACLASSIFICATION",
            format!("dataclassification fuera del enum: {texto}"),
        )
    })
}

/// El arnés **NO** rellena nada: todos los atributos salen del vector.
fn construir(e: &EventoEntrada) -> Result<Event, Fallo> {
    let dataclassification = clasificacion(&e.dataclassification)?;

    Ok(build_event(BuildEventInput {
        subject: e.subject.clone(),
        data: data_to_raw(&e.data)?,
        id: e.id.clone(),
        source: e.source.clone(),
        producerversion: e.producerversion.clone(),
        tenantid: e.tenantid.clone(),
        dataclassification,
        dataschema: e.dataschema.clone(),
        correlationid: e.correlationid.clone(),
        time: Some(instante(&e.time)?),
        aggregate_id: e.aggregate_id.clone(),
        causationid: e.causationid.clone(),
        partitionkey: e.partitionkey.clone(),
        traceparent: e.traceparent.clone(),
        tracestate: e.tracestate.clone(),
    })?)
}

fn instante(s: &str) -> Result<DateTime<Utc>, Fallo> {
    DateTime::parse_from_rfc3339(s)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| Fallo::nuevo("HARNESS_INPUT", format!("`{s}` no es RFC 3339: {e}")))
}

/// Un número JSON que se escribió con parte decimal pero es un entero (`5000.0`, `2e3`): para
/// JavaScript, que no distingue, lo es. `None` si tiene parte fraccionaria o no cabe en un `i64`.
#[allow(clippy::cast_possible_truncation)]
fn entero_de_decimal(valor: Option<f64>) -> Option<i64> {
    let v = valor?;
    // 2^63 como f64 es exacto; `i64::MAX as f64` redondea hacia arriba, así que se compara con `<`.
    (v.fract() == 0.0 && v >= -9_223_372_036_854_775_808.0 && v < 9_223_372_036_854_775_808.0)
        .then_some(v as i64)
}

/// La base de los esquemas del repo: `schemas/state/<dom>/<agr>/<semver>.json` cuelga de aquí. Es
/// la misma que el SDK de Node llama `STATE_SCHEMA_BASE`; el arnés la fija porque ningún vector
/// trae `dataschema`: lo que se compara es lo que el SDK deriva del contrato.
const BASE_ESQUEMAS: &str = "https://schemas.internal";

/// El arnés **NO** rellena nada de estado: todo sale del vector, y `dataschema` del SDK.
fn construir_estado(e: &PutEntrada) -> Result<Event, Fallo> {
    Ok(build_state_event(StateEventInput {
        contract: e.contract.clone(),
        tenant_id: e.tenant_id.clone(),
        key: e.key.clone(),
        id: e.id.clone(),
        source: e.source.clone(),
        time: instante(&e.time)?,
        correlationid: e.correlationid.clone(),
        producerversion: e.producerversion.clone(),
        classification: clasificacion(&e.classification)?,
        dataschema: state_dataschema(BASE_ESQUEMAS, &e.contract)?,
        causationid: e.causationid.clone(),
        traceparent: e.traceparent.clone(),
        tracestate: e.tracestate.clone(),
        data: e.data.clone(),
    })?)
}

fn entrada_de_estado<'a, T: Deserialize<'a>>(texto: &'a str) -> Result<T, Fallo> {
    serde_json::from_str(texto).map_err(|e| Fallo::nuevo("HARNESS_INPUT", e.to_string()))
}

fn firmante(e: &Entrada) -> Result<Signer, Fallo> {
    let f = e
        .signing
        .as_ref()
        .ok_or_else(|| Fallo::nuevo("HARNESS_INPUT", "la operación requiere `signing`"))?;
    Signer::new(
        &SigningOptions::default().with_private_key(f.private_key_pem.as_str(), f.key_id.as_str()),
    )?
    .ok_or_else(|| Fallo::nuevo("HARNESS_INPUT", "`signing.privateKeyPem` vacío"))
}

fn modo_verificacion(mode: Option<&str>) -> Result<VerificationMode, Fallo> {
    match mode.unwrap_or("require") {
        "off" => Ok(VerificationMode::Off),
        "warn" => Ok(VerificationMode::Warn),
        "require" => Ok(VerificationMode::Require),
        otro => Err(Fallo::nuevo(
            "HARNESS_INPUT",
            format!("`mode` desconocido: {otro}"),
        )),
    }
}

fn ejecutar(e: &Entrada, texto: &str) -> Result<Value, Fallo> {
    match e.op.as_str() {
        "build" => {
            let bytes = serialize(&construir(evento(e)?)?)?;
            Ok(json!({ "ok": true, "bytes": b64_encode(&bytes) }))
        }

        "dlq" => {
            let mut ev = construir(evento(e)?)?;
            if e.sign_first {
                // Firmar ANTES de las `dlq*` es lo que fija la posición de
                // signkeyid/signature respecto a ellas — 07-signing.md §4.1, §5.1.
                ev = firmante(e)?.sign(ev)?;
            }
            let d = e
                .dlq
                .as_ref()
                .ok_or_else(|| Fallo::nuevo("HARNESS_INPUT", "la operación requiere `dlq`"))?;
            let con_dlq = to_dlq_event(
                ev,
                &DlqInfo {
                    reason: d.reason,
                    attempts: d.attempts,
                    consumer: d.consumer.clone(),
                    error: d.error.clone(),
                },
                // `dlqtime` viene del vector, no del reloj.
                instante(&d.dlqtime)?,
            );
            let bytes = serialize(&con_dlq)?;
            Ok(json!({ "ok": true, "bytes": b64_encode(&bytes) }))
        }

        "sign" => {
            let firmado = firmante(e)?.sign(construir(evento(e)?)?)?;
            let bytes = serialize(&firmado)?;
            Ok(json!({ "ok": true, "bytes": b64_encode(&bytes) }))
        }

        "verify" => {
            let ev = parse_event(&cuerpo(e)?)?;
            let mut opciones =
                SigningOptions::default().with_verify(modo_verificacion(e.mode.as_deref())?);
            for (id, material) in &e.public_keys {
                opciones = opciones.with_public_key(id.as_str(), material.as_str());
            }
            // `None` en modo `off`: no se paga lo que no se usa, y entonces no hay nada
            // que comprobar. En `warn`, `check` acepta y devuelve el código: sigue siendo
            // ok, igual que en el arnés de Node.
            if let Some(v) = Verifier::new(&opciones, None)? {
                v.check(&ev)?;
            }
            Ok(json!({ "ok": true }))
        }

        "parse" => {
            parse_event(&cuerpo(e)?)?;
            Ok(json!({ "ok": true }))
        }

        // ─── Estado replicado (capacidad `state`) ────────────────────────────────────────
        "state-build" => {
            let p: PutEntrada = entrada_de_estado(texto)?;
            let bytes = serialize(&construir_estado(&p)?)?;
            Ok(json!({ "ok": true, "bytes": b64_encode(&bytes) }))
        }

        "state-tombstone" => {
            let l: LapidaEntrada = entrada_de_estado(texto)?;
            let lapida = build_state_tombstone(StateTombstoneInput {
                contract: l.contract,
                tenant_id: l.tenant_id,
                key: l.key,
                id: l.id,
                source: l.source,
                time: instante(&l.time)?,
                correlationid: l.correlationid,
                producerversion: l.producerversion,
                aggregate_version: l.aggregate_version,
                causationid: l.causationid,
                traceparent: l.traceparent,
                tracestate: l.tracestate,
            })?;
            Ok(json!({ "ok": true, "bytes": b64_encode(&serialize(&lapida)?) }))
        }

        "state-subject" => {
            let s: SujetoEntrada = entrada_de_estado(texto)?;
            let subject = state_subject(&s.contract, &s.tenant_id, &s.key)?;
            Ok(json!({ "ok": true, "subject": subject }))
        }

        "state-dlq" => {
            let d: DlqEstadoEntrada = entrada_de_estado(texto)?;
            let info = e
                .dlq
                .as_ref()
                .ok_or_else(|| Fallo::nuevo("HARNESS_INPUT", "la operación requiere `dlq`"))?;
            let con_dlq = to_state_dlq_event(
                construir_estado(&d.original)?,
                // La clave del SUBJECT de NATS, que aquí es la del original.
                &d.original.key,
                d.revision,
                &DlqInfo {
                    reason: info.reason,
                    attempts: info.attempts,
                    consumer: info.consumer.clone(),
                    error: info.error.clone(),
                },
                // `dlqtime` viene del vector, no del reloj.
                instante(&info.dlqtime)?,
            );
            Ok(json!({ "ok": true, "bytes": b64_encode(&serialize(&con_dlq)?) }))
        }

        // ─── Petición/respuesta (capacidad `rpc`) ────────────────────────────────────────
        "rpc-request" => {
            let p: RpcPeticionEntrada = entrada_de_estado(texto)?;
            let dataschema = rpc_dataschema(BASE_ESQUEMAS, &p.operation, RpcSide::Request)?;
            let evento = build_rpc_request(RpcRequestInput {
                operation: p.operation,
                tenant_id: p.tenant_id,
                aggregate_id: p.aggregate_id,
                id: p.id,
                source: p.source,
                time: instante(&p.time)?,
                correlationid: p.correlationid,
                causationid: p.causationid,
                producerversion: p.producerversion,
                classification: clasificacion(&p.classification)?,
                dataschema,
                traceparent: p.traceparent,
                tracestate: p.tracestate,
                data: p.data,
            })?;
            Ok(json!({ "ok": true, "bytes": b64_encode(&serialize(&evento)?) }))
        }

        "rpc-reply" => {
            let r: RpcRespuestaEntrada = entrada_de_estado(texto)?;
            let dataschema = rpc_dataschema(BASE_ESQUEMAS, &r.request.operation, RpcSide::Reply)?;
            let evento = build_rpc_reply(RpcReplyInput {
                request: r.request.into(),
                id: r.id,
                source: r.source,
                time: instante(&r.time)?,
                producerversion: r.producerversion,
                classification: clasificacion(&r.classification)?,
                dataschema,
                traceparent: r.traceparent,
                tracestate: r.tracestate,
                data: r.data,
            })?;
            Ok(json!({ "ok": true, "bytes": b64_encode(&serialize(&evento)?) }))
        }

        "rpc-error" => {
            let r: RpcErrorEntrada = entrada_de_estado(texto)?;
            let clase = match r.error.class.as_str() {
                "retryable" => ErrorClass::Retryable,
                "permanent" => ErrorClass::Permanent,
                // `poison` no viaja, y cualquier otra cosa tampoco: el SDK no construye ese error.
                otra => {
                    return Err(Fallo::nuevo(
                        "INVALID_RPC_CALL",
                        format!("`class` del error debe ser retryable o permanent: {otra}"),
                    ))
                }
            };
            let retry_after_ms = match &r.error.retry_after_ms {
                None => None,
                Some(Value::Number(n)) => Some(
                    n.as_u64()
                        .or_else(|| {
                            entero_de_decimal(n.as_f64()).and_then(|v| u64::try_from(v).ok())
                        })
                        .ok_or_else(|| {
                            Fallo::nuevo(
                                "INVALID_RPC_CALL",
                                "`retryAfterMs` debe ser un entero >= 0",
                            )
                        })?,
                ),
                Some(_) => {
                    return Err(Fallo::nuevo(
                        "INVALID_RPC_CALL",
                        "`retryAfterMs` debe ser un entero >= 0",
                    ))
                }
            };
            let evento = build_rpc_error(RpcErrorInput {
                reply_to: ReplyTo::Request(r.request.into()),
                id: r.id,
                source: r.source,
                time: instante(&r.time)?,
                producerversion: r.producerversion,
                traceparent: r.traceparent,
                tracestate: r.tracestate,
                error: RpcErrorBody {
                    class: clase,
                    code: r.error.code,
                    message: r.error.message,
                    retry_after_ms,
                },
            })?;
            Ok(json!({ "ok": true, "bytes": b64_encode(&serialize(&evento)?) }))
        }

        // El subject de NATS, o el código estable del rechazo: INVALID_RPC_CALL o RESERVED_DOMAIN.
        "rpc-operation" => {
            let v: Value = entrada_de_estado(texto)?;
            let Some(operacion) = v.get("operation").and_then(Value::as_str) else {
                return Err(Fallo::nuevo(
                    "INVALID_RPC_CALL",
                    "`operation` debe ser una cadena",
                ));
            };
            Ok(json!({ "ok": true, "subject": rpc_subject(operacion)? }))
        }

        // Los milisegundos efectivos, o INVALID_RPC_CALL. `timeoutMs` ausente es 5000; `null` NO es
        // «ausente»: es inválido. Un número entero con `.0` (o `2e3`) es un entero, como en JavaScript.
        "rpc-timeout" => {
            let v: Value = entrada_de_estado(texto)?;
            let pedido = match v.get("timeoutMs") {
                None => None,
                Some(Value::Number(n)) => Some(
                    n.as_i64()
                        .or_else(|| entero_de_decimal(n.as_f64()))
                        .ok_or_else(|| {
                            Fallo::nuevo("INVALID_RPC_CALL", "`timeoutMs` debe ser un entero")
                        })?,
                ),
                Some(_) => {
                    return Err(Fallo::nuevo(
                        "INVALID_RPC_CALL",
                        "`timeoutMs` debe ser un número entero",
                    ))
                }
            };
            Ok(json!({ "ok": true, "effectiveMs": resolve_timeout_ms(pedido)? }))
        }

        otro => Ok(json!({ "ok": false, "code": "UNSUPPORTED_OP", "detail": otro })),
    }
}

// ─── base64 estándar (RFC 4648 §4) ───────────────────────────────────────────
//
// Es el sobre del propio arnés, no lógica del protocolo: `bytes` viaja en base64 para que
// ningún paso intermedio pueda reescribir el UTF-8 o los saltos de línea. Va aquí y no
// como dependencia porque la firma —lo único del SDK que usa base64— lo tiene privado
// tras la feature `signing`, y añadir un crate a `[dependencies]` se lo cobraría a todo
// servicio del ecosistema por algo que solo usa este binario. Un fallo aquí lo detecta el
// propio runner al instante: los bytes dejarían de coincidir con los de Node.

const ALFABETO: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn b64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for trozo in input.chunks(3) {
        let n = (u32::from(trozo[0]) << 16)
            | (u32::from(trozo.get(1).copied().unwrap_or(0)) << 8)
            | u32::from(trozo.get(2).copied().unwrap_or(0));
        out.push(char::from(ALFABETO[(n >> 18) as usize & 63]));
        out.push(char::from(ALFABETO[(n >> 12) as usize & 63]));
        out.push(if trozo.len() > 1 {
            char::from(ALFABETO[(n >> 6) as usize & 63])
        } else {
            '='
        });
        out.push(if trozo.len() > 2 {
            char::from(ALFABETO[n as usize & 63])
        } else {
            '='
        });
    }
    out
}

fn b64_decode(input: &str) -> Result<Vec<u8>, String> {
    let mut acumulado: u32 = 0;
    let mut bits: u32 = 0;
    let mut out = Vec::with_capacity(input.len() / 4 * 3);
    for c in input.bytes() {
        if matches!(c, b'=' | b'\n' | b'\r') {
            continue;
        }
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return Err(format!("`bytes` no es base64 estándar: byte {c:#04x}")),
        };
        acumulado = (acumulado << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(u8::try_from((acumulado >> bits) & 0xFF).expect("8 bits"));
        }
    }
    Ok(out)
}

// ─── Entrada del programa ────────────────────────────────────────────────────

fn main() {
    let mut texto = String::new();
    let salida = match std::io::stdin().read_to_string(&mut texto) {
        Err(e) => json!({ "ok": false, "code": "HARNESS_STDIN", "detail": e.to_string() }),
        Ok(_) => match serde_json::from_str::<Entrada>(&texto) {
            Err(e) => json!({ "ok": false, "code": "HARNESS_INPUT", "detail": e.to_string() }),
            // Un fallo de la operación se REPORTA, no se propaga: exit != 0 significaría
            // que el arnés está roto, no que el caso falló.
            Ok(entrada) => ejecutar(&entrada, &texto)
                .unwrap_or_else(|f| json!({ "ok": false, "code": f.code, "detail": f.detail })),
        },
    };
    println!("{salida}");
}
