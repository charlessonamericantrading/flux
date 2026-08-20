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
//! ```text
//! echo '{"op":"build","event":{…}}' | cargo run --quiet --bin conformance-harness --features signing
//! ```

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::io::Read;

use chrono::{DateTime, Utc};
use flux::signing::{Signer, SigningOptions, VerificationMode, Verifier};
use flux::{
    build_event, data_to_raw, parse_event, serialize, to_dlq_event, BuildEventInput,
    DataClassification, DlqInfo, DlqReason, Event, FluxError,
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

/// El arnés **NO** rellena nada: todos los atributos salen del vector.
fn construir(e: &EventoEntrada) -> Result<Event, Fallo> {
    let dataclassification =
        DataClassification::from_str_exact(&e.dataclassification).ok_or_else(|| {
            Fallo::nuevo(
                "INVALID_DATACLASSIFICATION",
                format!(
                    "dataclassification fuera del enum: {}",
                    e.dataclassification
                ),
            )
        })?;

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

fn ejecutar(e: &Entrada) -> Result<Value, Fallo> {
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
            Ok(entrada) => ejecutar(&entrada)
                .unwrap_or_else(|f| json!({ "ok": false, "code": f.code, "detail": f.detail })),
        },
    };
    println!("{salida}");
}
