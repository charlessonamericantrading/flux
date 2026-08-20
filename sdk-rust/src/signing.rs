//! Firma de eventos con Ed25519. **Extensión OPCIONAL de v1.**
//!
//! Contrato normativo: `specification/07-signing.md`
//!
//! Detrás de la feature `signing`, que **no está activa por defecto**: un evento sin firma
//! sigue siendo válido y un SDK conforme no necesita implementarla, así que imponer
//! `ed25519-dalek` a todo el que use el SDK sería cobrar a todos por lo que usan unos
//! pocos.
//!
//! # Qué problema resuelve
//!
//! Hoy la autenticidad de un evento la garantiza la ACL del broker: un servicio no puede
//! publicar en un dominio ajeno. Eso deja tres huecos —un evento sacado del stream y
//! reinyectado, un evento exportado a un data lake donde ya no hay ACL, y un broker
//! comprometido que fabrica eventos— y la firma los cierra trasladando la autenticidad
//! **del canal al evento**: un evento firmado sigue siendo verificable dentro de un
//! fichero, un backup o un correo.
//!
//! # Por qué se puede firmar
//!
//! Firmar exige que el evento tenga **una única representación en bytes**, y las tres
//! reglas que la fijan —UTF-8 literal (§1.1), `time` con 3 decimales (§2.2) y `data`
//! siempre el último (§6)— se escribieron para otros problemas. Juntas resultaron ser
//! exactamente la canonicalización que la firma necesita: **no hay una forma canónica
//! aparte para firmar, es el mismo [`serialize`] que usa el productor**.
//!
//! # Formato de clave
//!
//! PEM: **PKCS#8** para la privada y **SPKI** para la pública, que es lo que exportan
//! `node:crypto`, `cryptography`, `crypto/ed25519`, `java.security` y `openssl genpkey
//! -algorithm ed25519`. Con eso, una clave generada por cualquier SDK del ecosistema vale
//! en todos los demás. También se acepta la clave **cruda en base64** (32 bytes de
//! semilla o de clave pública, y los 64 bytes de "secret key" de libsodium), porque es la
//! forma en que la entregan algunos gestores de secretos.

use std::collections::HashMap;

use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};

use crate::client::{LogFn, LogLevel};
use crate::envelope::{serialize, strip_dlq_extensions, Event};
use crate::errors::FluxError;

/// Qué hacer con un evento cuya firma falta o no verifica — 07-signing.md §7.
///
/// `Warn` no es un adorno: adoptar la firma en un ecosistema en marcha exige un periodo
/// en el que unos productores firman y otros no. Pasar directo a `Require` convierte en
/// POISON todo evento de un servicio aún no migrado.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VerificationMode {
    /// **Default.** No se mira la firma. Un evento sin firma es válido.
    #[default]
    Off,
    /// Se registra y se acepta. El modo de la migración.
    Warn,
    /// Falta la firma o no verifica → **POISON**.
    Require,
}

/// Configuración de la firma.
#[derive(Debug, Clone, Default)]
pub struct SigningOptions {
    /// Clave privada Ed25519 para firmar al publicar: PEM PKCS#8, o base64 cruda.
    /// `None` para solo verificar. **NUNCA se versiona** — 06-security.md §2.
    pub private_key: Option<String>,

    /// Id de la clave, formato `<servicio>-<n>`. Obligatorio si se firma: sin él un
    /// verificador no sabe qué clave pública usar.
    pub key_id: Option<String>,

    /// Claves públicas conocidas: `signkeyid` → PEM SPKI o base64 cruda.
    ///
    /// ⚠️ **Incluye aquí las claves RETIRADAS mientras exista algún evento firmado con
    /// ellas** (mínimo 90 días, la retención de la DLQ). Retirar una clave impide
    /// **emitir** con ella, no **verificar** lo ya emitido: tratar una clave retirada como
    /// inválida convierte una rotación rutinaria en la invalidación retroactiva de todo el
    /// historial — 07-signing.md §6.
    pub public_keys: HashMap<String, String>,

    /// Política de verificación. Default: [`VerificationMode::Off`].
    pub verify: VerificationMode,
}

impl SigningOptions {
    /// Configura la clave con la que se firmará al publicar.
    #[must_use]
    pub fn with_private_key(
        mut self,
        pem_or_base64: impl Into<String>,
        key_id: impl Into<String>,
    ) -> Self {
        self.private_key = Some(pem_or_base64.into());
        self.key_id = Some(key_id.into());
        self
    }

    /// Registra una clave pública conocida. Llámalo también con las **retiradas**.
    #[must_use]
    pub fn with_public_key(
        mut self,
        key_id: impl Into<String>,
        pem_or_base64: impl Into<String>,
    ) -> Self {
        self.public_keys.insert(key_id.into(), pem_or_base64.into());
        self
    }

    /// Política de verificación al consumir.
    #[must_use]
    pub fn with_verify(mut self, mode: VerificationMode) -> Self {
        self.verify = mode;
        self
    }
}

// ─── Bytes canónicos ─────────────────────────────────────────────────────────

/// Los bytes que se firman: el evento **sin `signature` y sin las extensiones `dlq*`**
/// — 07-signing.md §5.
///
/// - `signkeyid` **SÍ** entra. Si quedara fuera, un atacante podría cambiarlo para que la
///   verificación buscara otra clave.
/// - `signature` no puede firmarse a sí misma.
/// - Las `dlq*` se añaden **después** de firmar, así que quitarlas es exactamente lo que
///   hace el replay: por eso **un evento reproducido conserva su firma válida**, que es lo
///   correcto —el replay redistribuye un hecho ya emitido, no crea uno nuevo (§5.1).
fn signable_payload(event: &Event) -> Result<Vec<u8>, FluxError> {
    let mut canonical = strip_dlq_extensions(event.clone());
    canonical.signature = None;
    serialize(&canonical)
}

// ─── Firmar ──────────────────────────────────────────────────────────────────

/// Firma eventos al publicar.
pub struct Signer {
    key: SigningKey,
    key_id: String,
}

impl std::fmt::Debug for Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // La clave privada NUNCA se imprime: un Debug acaba en un log.
        f.debug_struct("Signer")
            .field("key_id", &self.key_id)
            .field("key", &"<oculto>")
            .finish()
    }
}

impl Signer {
    /// Construye el firmante, o `None` si no hay clave privada configurada.
    ///
    /// # Errores
    ///
    /// [`FluxError::Signing`] si hay clave pero no `key_id`, o si la clave no es una
    /// Ed25519 reconocible.
    pub fn new(options: &SigningOptions) -> Result<Option<Self>, FluxError> {
        let Some(material) = options.private_key.as_deref() else {
            return Ok(None);
        };
        let Some(key_id) = options.key_id.clone() else {
            return Err(FluxError::Signing(
                "signing.private_key requiere signing.key_id: sin él, un verificador no sabe \
                 qué clave pública usar (07-signing.md §4)"
                    .to_string(),
            ));
        };
        Ok(Some(Self {
            key: SigningKey::from_bytes(&parse_private_key(material)?),
            key_id,
        }))
    }

    /// El `signkeyid` que este firmante escribe en cada evento.
    #[must_use]
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// Devuelve el evento con `signkeyid` y `signature` puestos.
    ///
    /// Firmar es **lo último antes de serializar**: la firma cubre el envelope completo,
    /// así que cualquier atributo añadido después la invalidaría — 07-signing.md §5.
    ///
    /// Es determinista: Ed25519 no usa aleatoriedad por firma, así que el mismo evento
    /// produce siempre la misma firma. Eso solo es cierto porque 01-envelope.md §1.1,
    /// §2.2 y §6 fijan una única representación en bytes.
    ///
    /// # Errores
    ///
    /// [`FluxError::Envelope`] si el evento no se puede serializar.
    pub fn sign(&self, mut event: Event) -> Result<Event, FluxError> {
        // `signkeyid` va DENTRO de lo firmado, así que se pone ANTES de calcular los bytes.
        event.signkeyid = Some(self.key_id.clone());
        event.signature = None;
        let firma = self.key.sign(&signable_payload(&event)?);
        event.signature = Some(base64url_encode(&firma.to_bytes()));
        Ok(event)
    }
}

// ─── Verificar ───────────────────────────────────────────────────────────────

/// Verifica la firma de los eventos consumidos, según [`VerificationMode`].
pub struct Verifier {
    keys: HashMap<String, VerifyingKey>,
    mode: VerificationMode,
    logger: Option<LogFn>,
}

impl std::fmt::Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier")
            .field("mode", &self.mode)
            .field("known_key_ids", &self.keys.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl Verifier {
    /// Construye el verificador, o `None` en modo `off` — no se paga lo que no se usa.
    ///
    /// # Errores
    ///
    /// [`FluxError::Signing`] si el modo no es `off` y no hay claves públicas, o si alguna
    /// no es una Ed25519 reconocible.
    pub fn new(options: &SigningOptions, logger: Option<LogFn>) -> Result<Option<Self>, FluxError> {
        if options.verify == VerificationMode::Off {
            return Ok(None);
        }

        let mut keys = HashMap::with_capacity(options.public_keys.len());
        for (id, material) in &options.public_keys {
            let bytes = parse_public_key(material)
                .map_err(|e| FluxError::Signing(format!("clave pública `{id}` inválida: {e}")))?;
            let key = VerifyingKey::from_bytes(&bytes).map_err(|e| {
                FluxError::Signing(format!(
                    "clave pública `{id}` no es un punto Ed25519 válido: {e}"
                ))
            })?;
            keys.insert(id.clone(), key);
        }

        if keys.is_empty() {
            return Err(FluxError::Signing(format!(
                "signing.verify = {:?} requiere signing.public_keys. Incluye también las \
                 claves RETIRADAS mientras existan eventos firmados con ellas: retirar una \
                 clave impide EMITIR con ella, no VERIFICAR lo ya emitido (07-signing.md §6)",
                options.verify
            )));
        }

        Ok(Some(Self {
            keys,
            mode: options.verify,
            logger,
        }))
    }

    /// Aplica la política de verificación al evento.
    ///
    /// `Ok(None)` significa que la firma verificó. **`Ok(Some(code))` significa modo
    /// `warn`: el evento se acepta, pero el llamante DEBE contarlo** como
    /// `flux_events_consumed_total{outcome="invalid_signature"}` — 07-signing.md §7.1.
    ///
    /// Que el código salga por el valor de retorno y no se quede en un log es
    /// deliberado: sin esa métrica, `warn` es inútil para lo único que existe, **pilotar
    /// la migración**. La pregunta que hay que poder responder antes de pasar a `require`
    /// es "¿cuántos eventos siguen sin firma y de qué productores?", y un log no la
    /// contesta — hay que buscarla a mano en siete servicios.
    ///
    /// # Errores
    ///
    /// En modo `require`, [`FluxError::Poison`] con uno de los tres códigos de
    /// 07-signing.md §7: `MISSING_SIGNATURE`, `INVALID_SIGNATURE` o `UNKNOWN_SIGNING_KEY`.
    pub fn check(&self, event: &Event) -> Result<Option<&'static str>, FluxError> {
        let Some(firma_b64) = event.signature.as_deref() else {
            return self.fail(
                "MISSING_SIGNATURE",
                format!("el evento {} no está firmado", event.id),
            );
        };

        let Some(key_id) = event.signkeyid.as_deref() else {
            return self.fail(
                "UNKNOWN_SIGNING_KEY",
                format!(
                    "el evento {} trae `signature` pero no `signkeyid`, así que no hay forma \
                     de saber con qué clave verificarlo (07-signing.md §4)",
                    event.id
                ),
            );
        };

        let Some(key) = self.keys.get(key_id) else {
            // Una clave DESCONOCIDA no es lo mismo que una RETIRADA: si el id no está en
            // el mapa, o el operador la retiró sin conservar la pública —y eso invalida
            // retroactivamente el historial— o el evento viene de fuera del ecosistema.
            return self.fail(
                "UNKNOWN_SIGNING_KEY",
                format!(
                    "el evento {} está firmado con signkeyid=`{key_id}`, que no está entre \
                     las claves conocidas. ¿Se retiró sin conservar la pública? Las públicas \
                     retiradas DEBEN conservarse mientras exista algún evento firmado con \
                     ellas (07-signing.md §6)",
                    event.id
                ),
            );
        };

        let ok = base64url_decode(firma_b64)
            .ok()
            .filter(|b| b.len() == Signature::BYTE_SIZE)
            .and_then(|b| <[u8; Signature::BYTE_SIZE]>::try_from(b.as_slice()).ok())
            .map(|b: [u8; Signature::BYTE_SIZE]| Signature::from_bytes(&b))
            .zip(signable_payload(event).ok())
            // `verify_strict` y no `verify`: rechaza las claves y firmas de orden pequeño,
            // que permiten construir una firma que verifica bajo varias claves distintas.
            // No afecta a nada legítimo y cierra una ambigüedad conocida de Ed25519.
            .is_some_and(|(sig, payload)| key.verify_strict(&payload, &sig).is_ok());

        if ok {
            return Ok(None);
        }

        self.fail(
            "INVALID_SIGNATURE",
            format!(
                "la firma del evento {} no verifica con la clave `{key_id}`. El evento fue \
                 alterado después de firmarse, o no lo emitió quien dice",
                event.id
            ),
        )
    }

    /// En `require` lanza; en `warn` registra, **devuelve el código para que el llamante
    /// lo cuente** (§7.1) y acepta.
    fn fail(&self, code: &'static str, message: String) -> Result<Option<&'static str>, FluxError> {
        let err = FluxError::poison(message).with_code(code);
        if self.mode == VerificationMode::Require {
            return Err(err);
        }
        // `off` no llega aquí: en ese modo no se construye verificador. Queda `warn`, que
        // registra y acepta — es lo que permite migrar un ecosistema en el que unos
        // productores ya firman y otros no (07-signing.md §7).
        //
        // El log es un extra y cada plataforma lo resuelve a su manera; **la parte
        // normativa es la métrica**, y por eso el código sale por el valor de retorno.
        if let Some(log) = &self.logger {
            log(LogLevel::Warn, &format!("[flux] {code}: {err}"));
        }
        Ok(Some(code))
    }
}

// ─── Claves ──────────────────────────────────────────────────────────────────

/// Cabecera DER fija de una clave privada Ed25519 en PKCS#8 v1 — RFC 8410 §7.
///
/// La estructura entera son 48 bytes y los 16 primeros son **constantes** (versión 0, OID
/// 1.3.101.112, y las dos capas de OCTET STRING), así que extraer la semilla es comparar
/// un prefijo. Se hace a mano en vez de arrastrar `pkcs8` + `pem-rfc7468` por 16 bytes
/// conocidos, igual que el `base64` de `client.rs`, y así el SDK de PHP —que no tiene
/// alternativa— hace exactamente lo mismo.
const PKCS8_ED25519_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

/// Cabecera DER fija de una clave pública Ed25519 en SPKI — RFC 8410 §4. 44 bytes en total.
const SPKI_ED25519_PREFIX: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
];

/// Extrae los 32 bytes de semilla de una clave privada.
///
/// # Errores
///
/// [`FluxError::Signing`] si el material no es PEM PKCS#8 de Ed25519 ni base64 de 32 o 64
/// bytes. El mensaje nombra el algoritmo esperado: **el protocolo no negocia algoritmo a
/// propósito**, porque los formatos con algoritmo negociable acumulan una familia de
/// vulnerabilidades —de `alg: none` a la confusión HMAC/RSA— que solo existe porque hay
/// algo que negociar (07-signing.md §3).
fn parse_private_key(material: &str) -> Result<[u8; 32], FluxError> {
    let der = decode_key_material(material, "PRIVATE KEY")?;

    if der.len() == PKCS8_ED25519_PREFIX.len() + 32 && der.starts_with(&PKCS8_ED25519_PREFIX) {
        return Ok(fixed32(&der[PKCS8_ED25519_PREFIX.len()..]));
    }
    // Semilla cruda (32 B) o "secret key" de libsodium (64 B = semilla ‖ pública). De la
    // de 64 solo se toman los 32 primeros: la pública se deriva, no se lee, así que una
    // clave con la mitad pública manipulada no puede colar.
    if der.len() == 32 || der.len() == 64 {
        return Ok(fixed32(&der[..32]));
    }

    Err(FluxError::Signing(format!(
        "clave privada no reconocida ({} bytes). Se espera PEM PKCS#8 de Ed25519 \
         (`openssl genpkey -algorithm ed25519`), o base64 de 32 bytes de semilla / 64 de \
         secret key de libsodium. El protocolo NO negocia algoritmo: si esto es una RSA o \
         una EC, no hay nada que ajustar (07-signing.md §3)",
        der.len()
    )))
}

/// Extrae los 32 bytes de una clave pública.
fn parse_public_key(material: &str) -> Result<[u8; 32], String> {
    let der = decode_key_material(material, "PUBLIC KEY").map_err(|e| e.to_string())?;

    if der.len() == SPKI_ED25519_PREFIX.len() + 32 && der.starts_with(&SPKI_ED25519_PREFIX) {
        return Ok(fixed32(&der[SPKI_ED25519_PREFIX.len()..]));
    }
    if der.len() == 32 {
        return Ok(fixed32(&der));
    }
    Err(format!(
        "no reconocida ({} bytes). Se espera PEM SPKI de Ed25519 o base64 de 32 bytes",
        der.len()
    ))
}

fn fixed32(slice: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(&slice[..32]);
    out
}

/// PEM (con o sin la armadura esperada) o base64 suelto → bytes.
fn decode_key_material(material: &str, label: &str) -> Result<Vec<u8>, FluxError> {
    let trimmed = material.trim();
    let body = if trimmed.starts_with("-----BEGIN") {
        let begin = format!("-----BEGIN {label}-----");
        if !trimmed.starts_with(&begin) {
            return Err(FluxError::Signing(format!(
                "se esperaba un bloque PEM `{begin}` y llegó `{}`",
                trimmed.lines().next().unwrap_or_default()
            )));
        }
        trimmed
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect::<String>()
    } else {
        trimmed.split_whitespace().collect::<String>()
    };

    base64_decode(&body)
        .map_err(|e| FluxError::Signing(format!("la clave no es base64 válido: {e}")))
}

/// Genera un par Ed25519 en PEM (PKCS#8 + SPKI).
///
/// Comodidad para tests y para `flux keygen`. El PEM es el formato **interoperable**: la
/// clave que sale de aquí la lee tal cual el SDK de Node, el de Python, el de Go, el de
/// Java, el de .NET y el de PHP.
///
/// El `signkeyid` **DEBE** cambiar en cada rotación: nunca se reutiliza un id con una
/// clave distinta, porque eso convertiría la verificación de eventos históricos en un
/// juego de azar — 07-signing.md §6.
#[must_use]
pub fn generate_key_pair() -> (String, String) {
    let key = SigningKey::generate(&mut rand_core::OsRng);
    (
        pem(
            "PRIVATE KEY",
            &[&PKCS8_ED25519_PREFIX[..], &key.to_bytes()[..]].concat(),
        ),
        pem(
            "PUBLIC KEY",
            &[
                &SPKI_ED25519_PREFIX[..],
                &key.verifying_key().to_bytes()[..],
            ]
            .concat(),
        ),
    )
}

fn pem(label: &str, der: &[u8]) -> String {
    use std::fmt::Write as _;
    let b64 = base64_encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).unwrap_or_default());
        out.push('\n');
    }
    // El write! sobre un String no puede fallar.
    let _ = writeln!(out, "-----END {label}-----");
    out
}

// ─── base64 ──────────────────────────────────────────────────────────────────

const STD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn encode_with(alphabet: &[u8; 64], input: &[u8], pad: bool) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(alphabet[(n >> 18) as usize & 63] as char);
        out.push(alphabet[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(alphabet[(n >> 6) as usize & 63] as char);
        } else if pad {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(alphabet[n as usize & 63] as char);
        } else if pad {
            out.push('=');
        }
    }
    out
}

fn base64_encode(input: &[u8]) -> String {
    encode_with(STD, input, true)
}

/// **base64url SIN padding**, que es lo que exige 07-signing.md §4 para `signature`.
///
/// No es un detalle cosmético: con padding, la misma firma tendría dos representaciones
/// posibles y dos eventos byte-distintos serían el mismo evento.
fn base64url_encode(input: &[u8]) -> String {
    encode_with(URL, input, false)
}

fn decode_with(input: &str, url_safe: bool) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut acc: u32 = 0;
    let mut bits = 0;
    for c in input.chars() {
        if c == '=' || c.is_whitespace() {
            continue;
        }
        let v = match c {
            'A'..='Z' => c as u32 - 'A' as u32,
            'a'..='z' => c as u32 - 'a' as u32 + 26,
            '0'..='9' => c as u32 - '0' as u32 + 52,
            '+' if !url_safe => 62,
            '/' if !url_safe => 63,
            '-' if url_safe => 62,
            '_' if url_safe => 63,
            other => return Err(format!("carácter inesperado {other:?}")),
        };
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((acc >> bits) & 0xFF) as u8);
        }
    }
    Ok(out)
}

fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    decode_with(input, false)
}

fn base64url_decode(input: &str) -> Result<Vec<u8>, String> {
    decode_with(input, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{
        build_event, data_to_raw, parse_event, to_dlq_event, BuildEventInput, DataClassification,
        DlqInfo, DlqReason,
    };
    use chrono::{DateTime, Utc};
    use serde_json::json;

    const KEY_ID: &str = "pedidos-api-1";

    // ── Vector de interoperabilidad FIJO ──────────────────────────────────────
    //
    // Semilla del TEST 1 de RFC 8032, para que cualquiera pueda reproducirlo. La firma de
    // abajo la producen —y la aceptan— este SDK, el de Node (`node:crypto`) y el de PHP
    // (`sodium_crypto_sign_detached`) sobre los MISMOS bytes. Es lo que fija la
    // interoperabilidad de verdad: si alguien toca el serializador, el orden de claves o
    // el formato de `time`, este test cae antes que cualquier despliegue.

    const SEED_HEX: &str = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";

    const PRIVATE_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
        MC4CAQAwBQYDK2VwBCIEIJ1hsZ3v/VpguoRK9JLsLMREScVpezJpGXA7rAMcrn9g\n\
        -----END PRIVATE KEY-----\n";

    const PUBLIC_PEM: &str = "-----BEGIN PUBLIC KEY-----\n\
        MCowBQYDK2VwAyEA11qYAYKxCrfVS/7TyWQHOg7hcvPapiMlrwIaaPcHURo=\n\
        -----END PUBLIC KEY-----\n";

    /// Los bytes exactos que se firman en el vector.
    const VECTOR_PAYLOAD: &str = concat!(
        r#"{"specversion":"1.0","id":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55","#,
        r#""source":"/produccion/pedidos-api","#,
        r#""type":"com.flux.pedidos.pedido.creado.v1","#,
        r#""time":"2025-08-20T10:25:39.410Z","datacontenttype":"application/json","#,
        r#""dataschema":"https://schemas.internal/pedidos/pedido/creado/1.0.0.json","#,
        r#""correlationid":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55","tenantid":"acme","#,
        r#""producerversion":"3.4.1","dataclassification":"internal","#,
        r#""signkeyid":"pedidos-api-1","data":{"pedidoId":"ped-123"}}"#
    );

    /// El evento de arriba tras pasar por la DLQ, byte a byte. Generado con el SDK de PHP
    /// (`sodium`) y verificado contra el orden de claves de Node, Python, Go, Java y .NET:
    /// la firma va ANTES de las `dlq*`.
    const DLQ_VECTOR: &str = concat!(
        r#"{"specversion":"1.0","id":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55","#,
        r#""source":"/produccion/pedidos-api","#,
        r#""type":"com.flux.pedidos.pedido.creado.v1","#,
        r#""time":"2025-08-20T10:25:39.410Z","datacontenttype":"application/json","#,
        r#""dataschema":"https://schemas.internal/pedidos/pedido/creado/1.0.0.json","#,
        r#""correlationid":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55","tenantid":"acme","#,
        r#""producerversion":"3.4.1","dataclassification":"internal","#,
        r#""signkeyid":"pedidos-api-1","#,
        r#""signature":"Yhv5dV5yVxHz7w2fDuFQodUMhLoB8oPITBDA9t7Y3gAvc0sERbCew_L2JUK7Zy32ZmW3vmfzSPh7RvCY7dCaBA","#,
        r#""dlqreason":"permanent","dlqattempts":1,"#,
        r#""dlqconsumer":"facturacion-api__pedidos_pedido_v1_creado","#,
        r#""dlqerror":"PEDIDO_YA_CANCELADO","dlqtime":"2025-08-20T10:26:00.000Z","#,
        r#""data":{"pedidoId":"ped-123"}}"#
    );

    const VECTOR_SIGNATURE: &str =
        "Yhv5dV5yVxHz7w2fDuFQodUMhLoB8oPITBDA9t7Y3gAvc0sERbCew_L2JUK7Zy32ZmW3vmfzSPh7RvCY7dCaBA";

    fn vector_event() -> Event {
        build_event(BuildEventInput {
            subject: "pedidos.pedido.v1.creado".into(),
            data: data_to_raw(&json!({ "pedidoId": "ped-123" })).unwrap(),
            id: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55".into(),
            source: "/produccion/pedidos-api".into(),
            producerversion: "3.4.1".into(),
            tenantid: "acme".into(),
            dataclassification: DataClassification::Internal,
            dataschema: "https://schemas.internal/pedidos/pedido/creado/1.0.0.json".into(),
            correlationid: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55".into(),
            time: Some(
                DateTime::parse_from_rfc3339("2025-08-20T10:25:39.410Z")
                    .unwrap()
                    .with_timezone(&Utc),
            ),
            aggregate_id: None,
            causationid: None,
            partitionkey: None,
            traceparent: None,
            tracestate: None,
        })
        .unwrap()
    }

    fn signer() -> Signer {
        Signer::new(&SigningOptions::default().with_private_key(PRIVATE_PEM, KEY_ID))
            .unwrap()
            .unwrap()
    }

    fn verifier(mode: VerificationMode) -> Verifier {
        Verifier::new(
            &SigningOptions::default()
                .with_public_key(KEY_ID, PUBLIC_PEM)
                .with_verify(mode),
            None,
        )
        .unwrap()
        .unwrap()
    }

    fn code_of(err: &FluxError) -> String {
        err.code().unwrap_or("<sin código>").to_string()
    }

    // ── interoperabilidad ─────────────────────────────────────────────────────

    /// **El test que fija la interoperabilidad entre los seis SDKs.**
    #[test]
    fn el_vector_fijo_produce_la_misma_firma_que_node_y_php() {
        let firmado = signer().sign(vector_event()).unwrap();

        assert_eq!(
            String::from_utf8(signable_payload(&firmado).unwrap()).unwrap(),
            VECTOR_PAYLOAD,
            "los bytes canónicos han cambiado: cualquier firma emitida antes deja de verificar"
        );
        assert_eq!(firmado.signature.as_deref(), Some(VECTOR_SIGNATURE));
    }

    /// Comprobación cruzada con el SDK de PHP: los bytes del evento de DLQ FIRMADO son
    /// idénticos. Si algún día dejan de serlo, el replay verbatim y los fixtures
    /// compartidos dejan de comparar — 01-envelope.md §6.
    #[test]
    fn el_evento_de_dlq_firmado_es_byte_a_byte_el_de_php_y_node() {
        let firmado = signer().sign(vector_event()).unwrap();
        let en_dlq = to_dlq_event(
            firmado,
            &DlqInfo {
                reason: DlqReason::Permanent,
                attempts: 1,
                consumer: "facturacion-api__pedidos_pedido_v1_creado".into(),
                error: "PEDIDO_YA_CANCELADO".into(),
            },
            DateTime::parse_from_rfc3339("2025-08-20T10:26:00.000Z")
                .unwrap()
                .with_timezone(&Utc),
        );

        assert_eq!(
            String::from_utf8(serialize(&en_dlq).unwrap()).unwrap(),
            DLQ_VECTOR,
        );
    }

    /// Y al revés: la firma que produjeron Node y PHP verifica aquí.
    #[test]
    fn el_vector_fijo_verifica_venga_de_donde_venga() {
        let mut ajeno = vector_event();
        ajeno.signkeyid = Some(KEY_ID.to_string());
        ajeno.signature = Some(VECTOR_SIGNATURE.to_string());
        verifier(VerificationMode::Require).check(&ajeno).unwrap();
    }

    /// El PEM y la semilla cruda son la misma clave: un gestor de secretos que entregue
    /// una u otra produce firmas idénticas.
    #[test]
    fn pem_y_base64_crudo_son_la_misma_clave() {
        let semilla = hex_to_bytes(SEED_HEX);
        let crudo = base64_encode(&semilla);

        let a = signer().sign(vector_event()).unwrap();
        let b = Signer::new(&SigningOptions::default().with_private_key(crudo, KEY_ID))
            .unwrap()
            .unwrap()
            .sign(vector_event())
            .unwrap();
        assert_eq!(a.signature, b.signature);
    }

    /// La "secret key" de 64 bytes de libsodium (semilla ‖ pública) también.
    #[test]
    fn la_secret_key_de_64_bytes_de_libsodium_tambien_vale() {
        let mut sk = hex_to_bytes(SEED_HEX);
        sk.extend_from_slice(&parse_public_key(PUBLIC_PEM).unwrap());
        assert_eq!(sk.len(), 64);

        let firmado =
            Signer::new(&SigningOptions::default().with_private_key(base64_encode(&sk), KEY_ID))
                .unwrap()
                .unwrap()
                .sign(vector_event())
                .unwrap();
        assert_eq!(firmado.signature.as_deref(), Some(VECTOR_SIGNATURE));
    }

    // ── firma ─────────────────────────────────────────────────────────────────

    #[test]
    fn una_firma_valida_verifica() {
        let firmado = signer().sign(vector_event()).unwrap();
        verifier(VerificationMode::Require).check(&firmado).unwrap();
    }

    /// §4: ambas van entre las extensiones, **antes de `data`**.
    #[test]
    fn signkeyid_y_signature_van_antes_de_data() {
        let firmado = signer().sign(vector_event()).unwrap();
        let json = String::from_utf8(serialize(&firmado).unwrap()).unwrap();

        let data_pos = json.find(r#""data":"#).unwrap();
        let keyid_pos = json.find(r#""signkeyid":"#).unwrap();
        let sig_pos = json.find(r#""signature":"#).unwrap();
        assert!(
            keyid_pos < sig_pos,
            "signkeyid va antes que signature: {json}"
        );
        assert!(sig_pos < data_pos, "signature va antes de data: {json}");

        let raiz: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(raiz.keys().next_back().map(String::as_str), Some("data"));
    }

    #[test]
    fn la_firma_es_base64url_sin_padding() {
        let firmado = signer().sign(vector_event()).unwrap();
        let s = firmado.signature.unwrap();
        assert!(
            s.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "{s}"
        );
        assert!(!s.contains('='), "sin padding — 07-signing.md §4: {s}");
        // 64 bytes en base64 sin padding.
        assert_eq!(s.len(), 86);
    }

    /// Ed25519 no usa aleatoriedad por firma: el mismo evento da siempre la misma firma.
    #[test]
    fn la_firma_es_determinista() {
        let s = signer();
        assert_eq!(
            s.sign(vector_event()).unwrap().signature,
            s.sign(vector_event()).unwrap().signature
        );
    }

    /// Si el round-trip no conservara los bytes, la firma no sobreviviría al broker.
    #[test]
    fn sobrevive_al_round_trip_de_serializacion() {
        let firmado = signer().sign(vector_event()).unwrap();
        let vuelto = parse_event(&serialize(&firmado).unwrap()).unwrap();
        assert_eq!(vuelto.signature, firmado.signature);
        verifier(VerificationMode::Require).check(&vuelto).unwrap();
    }

    // ── detección de manipulación ─────────────────────────────────────────────

    #[test]
    fn alterar_data_invalida_la_firma() {
        let mut firmado = signer().sign(vector_event()).unwrap();
        firmado.data = data_to_raw(&json!({ "pedidoId": "ped-999" })).unwrap();

        let err = verifier(VerificationMode::Require)
            .check(&firmado)
            .unwrap_err();
        assert_eq!(code_of(&err), "INVALID_SIGNATURE");
    }

    /// El caso que la ACL del broker **no** cubre: un evento sacado del stream, editado y
    /// reinyectado. Con la firma activa, el `tenantid` queda ligado criptográficamente a
    /// la clave del productor — 09-multitenancy.md §4.
    #[test]
    fn alterar_el_tenantid_invalida_la_firma() {
        let mut firmado = signer().sign(vector_event()).unwrap();
        firmado.tenantid = "otro-tenant".to_string();

        let err = verifier(VerificationMode::Require)
            .check(&firmado)
            .unwrap_err();
        assert_eq!(code_of(&err), "INVALID_SIGNATURE");
    }

    /// `signkeyid` va DENTRO de lo firmado justo para esto — §5.
    #[test]
    fn cambiar_signkeyid_no_permite_eludir_la_verificacion() {
        let mut firmado = signer().sign(vector_event()).unwrap();
        firmado.signkeyid = Some("otro-1".to_string());

        let err = verifier(VerificationMode::Require)
            .check(&firmado)
            .unwrap_err();
        assert_eq!(code_of(&err), "UNKNOWN_SIGNING_KEY");
    }

    /// Y si el atacante lo cambia por un id que SÍ está registrado, tampoco: los bytes
    /// firmados incluían el id viejo.
    #[test]
    fn cambiar_signkeyid_por_uno_conocido_da_firma_invalida() {
        let otra = generate_key_pair();
        let mut firmado = signer().sign(vector_event()).unwrap();
        firmado.signkeyid = Some("pedidos-api-2".to_string());

        let v = Verifier::new(
            &SigningOptions::default()
                .with_public_key(KEY_ID, PUBLIC_PEM)
                .with_public_key("pedidos-api-2", otra.1)
                .with_verify(VerificationMode::Require),
            None,
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            code_of(&v.check(&firmado).unwrap_err()),
            "INVALID_SIGNATURE"
        );
    }

    #[test]
    fn una_firma_de_otra_clave_no_verifica() {
        let (priv_pem, _) = generate_key_pair();
        let impostor = Signer::new(&SigningOptions::default().with_private_key(priv_pem, KEY_ID))
            .unwrap()
            .unwrap();

        let err = verifier(VerificationMode::Require)
            .check(&impostor.sign(vector_event()).unwrap())
            .unwrap_err();
        assert_eq!(code_of(&err), "INVALID_SIGNATURE");
    }

    #[test]
    fn una_firma_que_no_es_base64url_es_invalida() {
        let mut firmado = signer().sign(vector_event()).unwrap();
        firmado.signature = Some("no es base64 ¡¡¡".to_string());
        let err = verifier(VerificationMode::Require)
            .check(&firmado)
            .unwrap_err();
        assert_eq!(code_of(&err), "INVALID_SIGNATURE");
    }

    #[test]
    fn una_signature_sin_signkeyid_no_pasa_por_valida() {
        let mut firmado = signer().sign(vector_event()).unwrap();
        firmado.signkeyid = None;
        let err = verifier(VerificationMode::Require)
            .check(&firmado)
            .unwrap_err();
        assert_eq!(code_of(&err), "UNKNOWN_SIGNING_KEY");
    }

    // ── DLQ y replay ──────────────────────────────────────────────────────────

    /// Si la verificación no ignorase las `dlq*`, **todo evento en la DLQ parecería
    /// manipulado** — 07-signing.md §5.
    #[test]
    fn la_firma_sigue_verificando_tras_pasar_por_la_dlq() {
        let firmado = signer().sign(vector_event()).unwrap();
        let en_dlq = to_dlq_event(
            firmado,
            &DlqInfo {
                reason: DlqReason::Permanent,
                attempts: 1,
                consumer: "facturacion-api__pedidos_pedido_v1_creado".into(),
                error: "PEDIDO_YA_CANCELADO: el pedido ped-123 estaba cancelado".into(),
            },
            Utc::now(),
        );

        verifier(VerificationMode::Require).check(&en_dlq).unwrap();
    }

    /// El replay redistribuye un hecho ya emitido, no crea uno nuevo — §5.1.
    #[test]
    fn un_evento_reproducido_conserva_su_firma_valida() {
        let firmado = signer().sign(vector_event()).unwrap();
        let en_dlq = to_dlq_event(
            firmado,
            &DlqInfo {
                reason: DlqReason::Retryable,
                attempts: 6,
                consumer: "c".into(),
                error: "HTTP_503".into(),
            },
            Utc::now(),
        );
        let reproducido = parse_event(&serialize(&en_dlq).unwrap()).unwrap();
        let limpio = strip_dlq_extensions(reproducido);

        verifier(VerificationMode::Require).check(&limpio).unwrap();
    }

    // ── modos ─────────────────────────────────────────────────────────────────

    #[test]
    fn require_rechaza_un_evento_sin_firma() {
        let err = verifier(VerificationMode::Require)
            .check(&vector_event())
            .unwrap_err();
        assert_eq!(code_of(&err), "MISSING_SIGNATURE");
    }

    #[test]
    fn warn_registra_pero_acepta() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let avisos = std::sync::Arc::new(AtomicUsize::new(0));
        let contador = avisos.clone();

        let v = Verifier::new(
            &SigningOptions::default()
                .with_public_key(KEY_ID, PUBLIC_PEM)
                .with_verify(VerificationMode::Warn),
            Some(std::sync::Arc::new(move |_, _: &str| {
                contador.fetch_add(1, Ordering::SeqCst);
            })),
        )
        .unwrap()
        .unwrap();

        // Sin firma y con firma rota: los dos se aceptan, y los dos devuelven el código
        // para que el llamante lo cuente — §7.1.
        assert_eq!(v.check(&vector_event()).unwrap(), Some("MISSING_SIGNATURE"));
        let mut roto = signer().sign(vector_event()).unwrap();
        roto.tenantid = "otro".into();
        assert_eq!(v.check(&roto).unwrap(), Some("INVALID_SIGNATURE"));

        assert_eq!(avisos.load(Ordering::SeqCst), 2);
    }

    /// §7.1: **`warn` DEBE ser observable.** El código sale por el valor de retorno, no
    /// solo por el log, para que el runtime pueda emitir
    /// `flux_events_consumed_total{outcome="invalid_signature"}`.
    ///
    /// Sin esa métrica, `warn` es inútil para lo único que existe —pilotar la migración—:
    /// la pregunta "¿cuántos eventos siguen sin firma y de qué productores?" habría que
    /// buscarla a mano en los logs de siete servicios.
    #[test]
    fn warn_devuelve_el_codigo_aunque_no_haya_logger() {
        let v = Verifier::new(
            &SigningOptions::default()
                .with_public_key(KEY_ID, PUBLIC_PEM)
                .with_verify(VerificationMode::Warn),
            None, // sin logger: la métrica es la parte normativa, el log es un extra
        )
        .unwrap()
        .unwrap();

        assert_eq!(v.check(&vector_event()).unwrap(), Some("MISSING_SIGNATURE"));

        let mut ajeno = vector_event();
        ajeno.signkeyid = Some("desconocida-9".into());
        ajeno.signature = Some(VECTOR_SIGNATURE.into());
        assert_eq!(v.check(&ajeno).unwrap(), Some("UNKNOWN_SIGNING_KEY"));

        // Y un evento correctamente firmado no genera aviso ninguno.
        assert_eq!(
            v.check(&signer().sign(vector_event()).unwrap()).unwrap(),
            None
        );
    }

    /// `off` no construye verificador: no se paga lo que no se usa, y un evento firmado
    /// se consume igual (adopción gradual — §7).
    #[test]
    fn off_no_construye_verificador() {
        assert!(Verifier::new(&SigningOptions::default(), None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn sin_clave_privada_no_hay_firmante() {
        assert!(Signer::new(&SigningOptions::default()).unwrap().is_none());
    }

    // ── gestión de claves ─────────────────────────────────────────────────────

    #[test]
    fn firmar_sin_key_id_falla_con_un_mensaje_accionable() {
        let opts = SigningOptions {
            private_key: Some(PRIVATE_PEM.to_string()),
            ..SigningOptions::default()
        };
        let err = Signer::new(&opts).unwrap_err().to_string();
        assert!(err.contains("key_id"), "{err}");
        assert!(err.contains("07-signing.md"), "{err}");
    }

    #[test]
    fn verificar_sin_claves_publicas_falla_explicando_la_retencion() {
        let err = Verifier::new(
            &SigningOptions::default().with_verify(VerificationMode::Require),
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("RETIRADAS"), "{err}");
    }

    /// Una clave de otro algoritmo no es "una clave que hay que convertir": el protocolo
    /// no negocia algoritmo — §3.
    #[test]
    fn rechaza_una_clave_que_no_sea_ed25519() {
        // Cabecera SPKI de una P-256, con su OID: ni el tamaño ni el prefijo encajan.
        let p256 = "-----BEGIN PUBLIC KEY-----\n\
            MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEfoo5Rr3z2/g6rCzPoRnKSIVBcjWM\n\
            YjOl6Y/gPWPYt+Fd8ZLKJ0uv5rBXwCkPU9WQiVQdRlVLPO9EGxYQpxCFMA==\n\
            -----END PUBLIC KEY-----";
        let err = Verifier::new(
            &SigningOptions::default()
                .with_public_key("p256-1", p256)
                .with_verify(VerificationMode::Require),
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("p256-1"), "{err}");
    }

    /// **La regla que más se equivoca**: retirar una clave impide EMITIR con ella, no
    /// VERIFICAR lo ya emitido. Tratar una retirada como inválida convierte una rotación
    /// rutinaria en la invalidación retroactiva de todo el historial — §6.
    #[test]
    fn una_clave_retirada_sigue_verificando_si_se_conserva_la_publica() {
        let (vieja_priv, vieja_pub) = generate_key_pair();
        let (_, nueva_pub) = generate_key_pair();

        let firmado_con_la_vieja =
            Signer::new(&SigningOptions::default().with_private_key(vieja_priv, "pedidos-api-1"))
                .unwrap()
                .unwrap()
                .sign(vector_event())
                .unwrap();

        let v = Verifier::new(
            &SigningOptions::default()
                .with_public_key("pedidos-api-1", vieja_pub) // RETIRADA, conservada
                .with_public_key("pedidos-api-2", nueva_pub) // activa
                .with_verify(VerificationMode::Require),
            None,
        )
        .unwrap()
        .unwrap();

        v.check(&firmado_con_la_vieja).unwrap();
    }

    #[test]
    fn el_par_generado_es_pem_interoperable() {
        let (priv_pem, pub_pem) = generate_key_pair();
        assert!(priv_pem.starts_with("-----BEGIN PRIVATE KEY-----"));
        assert!(pub_pem.starts_with("-----BEGIN PUBLIC KEY-----"));

        // La pública del PEM es la que se deriva de la privada.
        let derivada = SigningKey::from_bytes(&parse_private_key(&priv_pem).unwrap())
            .verifying_key()
            .to_bytes();
        assert_eq!(parse_public_key(&pub_pem).unwrap(), derivada);
    }

    #[test]
    fn una_etiqueta_pem_equivocada_se_rechaza() {
        let err = parse_private_key(PUBLIC_PEM).unwrap_err().to_string();
        assert!(err.contains("PRIVATE KEY"), "{err}");
    }

    // ── base64 ────────────────────────────────────────────────────────────────

    #[test]
    fn base64url_ida_y_vuelta() {
        for n in 0..70usize {
            let bytes: Vec<u8> = (0..n)
                .map(|i| u8::try_from((i * 7 + 3) % 256).unwrap())
                .collect();
            let enc = base64url_encode(&bytes);
            assert!(!enc.contains('='));
            assert_eq!(base64url_decode(&enc).unwrap(), bytes);
        }
    }

    #[test]
    fn base64_estandar_coincide_con_los_vectores_de_rfc4648() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64_decode("Zm9vYmFy").unwrap(), b"foobar");
    }

    #[test]
    fn base64url_no_acepta_el_alfabeto_estandar() {
        assert!(base64url_decode("ab+/").is_err());
    }

    fn hex_to_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }
}
