//! Envelope CloudEvents 1.0 — construcción, serialización y parseo.
//!
//! Contrato normativo: `specification/01-envelope.md`
//!
//! flux v1 usa **structured mode**: el CloudEvent completo viaja serializado como JSON
//! en el cuerpo del mensaje NATS. Un mensaje **es** un fichero JSON completo — lo
//! copias, lo pegas en un test, lo reproduces. Esa propiedad vale más que los bytes que
//! ahorraría binary mode repartiendo los atributos entre cabeceras `ce-*` y cuerpo.

use std::collections::BTreeMap;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::errors::FluxError;
use crate::protocol::{subject_to_type, DATA_CONTENT_TYPE, MAX_MESSAGE_BYTES, SPEC_VERSION};

/// Clasificación del dato transportado — 06-security.md §5. Determina la retención
/// efectiva del evento.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataClassification {
    /// Retención 30 d.
    Public,
    /// Retención 30 d. Es el default de `connect()` cuando no se declara otro.
    Internal,
    /// Retención 7 d.
    Confidential,
    /// Retención 24 h.
    Restricted,
}

impl DataClassification {
    /// El literal que viaja en el envelope.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Confidential => "confidential",
            Self::Restricted => "restricted",
        }
    }

    /// Interpreta el literal del envelope. `None` si está fuera del enum → POISON.
    #[must_use]
    pub fn from_str_exact(s: &str) -> Option<Self> {
        match s {
            "public" => Some(Self::Public),
            "internal" => Some(Self::Internal),
            "confidential" => Some(Self::Confidential),
            "restricted" => Some(Self::Restricted),
            _ => None,
        }
    }
}

impl std::fmt::Display for DataClassification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Por qué un evento acabó en la DLQ — 04-errors.md §3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DlqReason {
    /// Agotó los reintentos del backoff canónico.
    Retryable,
    /// El consumidor lo rechazó de forma definitiva. No gastó reintentos.
    Permanent,
    /// El mensaje no era interpretable. Requiere alerta inmediata.
    Poison,
}

impl From<crate::errors::ErrorClass> for DlqReason {
    fn from(c: crate::errors::ErrorClass) -> Self {
        match c {
            crate::errors::ErrorClass::Retryable => Self::Retryable,
            crate::errors::ErrorClass::Permanent => Self::Permanent,
            crate::errors::ErrorClass::Poison => Self::Poison,
        }
    }
}

/// Formatea un instante como exige el protocolo: RFC 3339 UTC con **exactamente 3
/// decimales** y sufijo `Z` — 01-envelope.md §2.2.
///
/// No se usa el formateador por defecto de ningún lenguaje a propósito: los tres
/// primeros SDKs producían cosas distintas, **todas válidas según RFC 3339**. Go
/// recortaba ceros (`…39.41Z`), Python daba microsegundos y offset
/// (`…39.410000+00:00`), Node acertaba por casualidad. El resultado es que dos
/// servicios del mismo ecosistema emiten `time` distintos para el mismo instante, y eso
/// rompe el replay verbatim, la firma futura, la deduplicación por hash y los fixtures
/// compartidos de la suite de conformidad.
///
/// `SecondsFormat::Millis` de chrono fija los 3 decimales; `true` fuerza `Z` en vez de
/// `+00:00`.
#[must_use]
pub fn format_time(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

// ─── El evento ───────────────────────────────────────────────────────────────

/// Un evento de flux: un CloudEvent 1.0 con el perfil de extensiones de flux.
///
/// **El orden de los campos de este struct es normativo.** `serde` serializa en orden de
/// declaración, así que la secuencia de bytes sale de aquí: primero el núcleo de
/// CloudEvents, luego las extensiones obligatorias, luego las opcionales, luego las de
/// DLQ y **`data` siempre el último** — 01-envelope.md §6. Insertar un campo nuevo en
/// medio cambia los bytes de todos los eventos del ecosistema.
///
/// Los nombres de las extensiones van en minúscula pegada (`correlationid`, no
/// `correlation_id`) porque CloudEvents restringe los nombres de extensión a minúsculas
/// y dígitos ASCII sin separadores. No es estilo: es la especificación (§3). De ahí que
/// los `rename` de serde no sigan ninguna convención de Rust.
///
/// `deny_unknown_fields` implementa la regla de **atributos raíz cerrados** (§3.4) en el
/// propio tipo. `parse_event` la comprueba además antes de deserializar, para poder
/// enumerar *todos* los atributos desconocidos en el mensaje de error en vez de abortar
/// en el primero.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    // ── Núcleo CloudEvents — 01-envelope.md §2 ──
    /// Siempre `"1.0"`.
    pub specversion: String,
    /// UUIDv7 canónico. Lo genera el SDK.
    pub id: String,
    /// `/<entorno>/<servicio>`. Junto a `id` es la clave de deduplicación del ecosistema.
    pub source: String,
    /// Reverse-DNS derivado del subject de NATS.
    #[serde(rename = "type")]
    pub event_type: String,
    /// RFC 3339 UTC con exactamente 3 decimales. Ver [`format_time`].
    ///
    /// Es `String` y no `DateTime<Utc>` a propósito: el evento tiene que poder
    /// reserializarse **byte a byte igual** para el replay verbatim, y cualquier
    /// round-trip por un tipo temporal reformatea. Para trabajar con el instante,
    /// [`Event::event_time`].
    pub time: String,
    /// `application/json` en v1.
    pub datacontenttype: String,
    /// URI absoluta al JSON Schema, con SemVer.
    pub dataschema: String,

    /// El atributo `subject` de CloudEvents.
    ///
    /// ⚠️ **NO es el subject de NATS.** El de NATS es una dirección de enrutado
    /// (`pedidos.pedido.v1.creado`); éste es la identidad del agregado dentro de la
    /// fuente (`ped-123`). Confundirlos es el error más frecuente al adoptar CloudEvents
    /// sobre NATS, así que el SDK los nombra distinto y solo los une al serializar
    /// — 01-envelope.md §2.1.
    #[serde(rename = "subject", skip_serializing_if = "Option::is_none")]
    pub aggregate_id: Option<String>,

    // ── Extensiones obligatorias — 01-envelope.md §3.1 ──
    /// Identifica el flujo de negocio completo y se propaga **sin modificar**. Si el
    /// evento no nace de otro, el SDK lo inicializa con su propio `id`.
    pub correlationid: String,
    /// Tenant propietario. `"system"` para eventos de plataforma.
    pub tenantid: String,
    /// SemVer del servicio emisor. Sin esto, un bug de payload en producción no se puede
    /// acotar a un despliegue.
    pub producerversion: String,
    /// Una de las cuatro clasificaciones. Determina la retención.
    pub dataclassification: DataClassification,

    // ── Extensiones opcionales — 01-envelope.md §3.2 ──
    /// `id` del evento concreto que provocó éste. `correlationid` responde "¿de qué
    /// flujo forma parte?"; `causationid`, "¿quién lo causó exactamente?".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causationid: Option<String>,
    /// Clave de ordenación. Por convención, igual al `aggregate_id`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitionkey: Option<String>,
    /// W3C Trace Context.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// W3C Trace Context.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,

    // ── Extensiones de DLQ — solo en dlq.<subject> — 04-errors.md §3 ──
    /// Solo presente en la DLQ.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dlqreason: Option<DlqReason>,
    /// Número de entrega en que el evento murió. **No** es una propiedad de su clase: un
    /// PERMANENT lanzado en la 3ª entrega registra 3.
    ///
    /// Es `Option<u32>` y no `u32`: aquí es donde Rust expresa la regla que Go y C# solo
    /// cumplen por casualidad. Con `omitempty`, un `dlqattempts` de 0 desaparecería del
    /// JSON y reaparecería como "ausente" al parsear — hoy es inocuo únicamente porque
    /// el mínimo legal resulta ser 1. `Option` convierte esa coincidencia en una regla
    /// del tipo: ausente ≠ vacío (01-envelope.md §3.3).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dlqattempts: Option<u32>,
    /// Durable del consumidor que lo mató.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dlqconsumer: Option<String>,
    /// Código y texto del error, truncado a 1024 bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dlqerror: Option<String>,
    /// Instante del enrutado a la DLQ, en el formato de [`format_time`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dlqtime: Option<String>,

    // ── Payload — 01-envelope.md §4 ──
    /// El payload, **sin interpretar**.
    ///
    /// `Box<RawValue>` y no `serde_json::Value` a propósito: mantiene el payload como
    /// los bytes exactos que llegaron, de modo que un evento parseado y vuelto a
    /// serializar es byte a byte el mismo. Eso es lo que hace que el replay desde la DLQ
    /// sea `del(.dlq*)` y republicar, sin reordenar claves (§6) ni normalizar
    /// `4995.00` a `4995.0` (§2.5). Para tiparlo, [`Event::data_as`].
    ///
    /// **Va el último y debe seguir yéndolo.**
    pub data: Box<RawValue>,
}

/// Comparación por valor.
///
/// El payload se compara **byte a byte, no semánticamente**: dos eventos con el mismo
/// contenido JSON pero distinto orden de claves NO son iguales. Es lo correcto aquí,
/// porque el protocolo transporta bytes y el replay debe preservarlos.
///
/// Se implementa a mano —y no con `#[derive(PartialEq)]`— porque `RawValue` no compara,
/// exactamente igual que el `json.RawMessage` de Go hace no comparable a su `Event`. Es
/// consecuencia directa de que el payload sea JSON arbitrario: no hay forma de tenerlo
/// sin interpretar Y comparable a la vez.
impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.specversion == other.specversion
            && self.id == other.id
            && self.source == other.source
            && self.event_type == other.event_type
            && self.time == other.time
            && self.datacontenttype == other.datacontenttype
            && self.dataschema == other.dataschema
            && self.aggregate_id == other.aggregate_id
            && self.correlationid == other.correlationid
            && self.tenantid == other.tenantid
            && self.producerversion == other.producerversion
            && self.dataclassification == other.dataclassification
            && self.causationid == other.causationid
            && self.partitionkey == other.partitionkey
            && self.traceparent == other.traceparent
            && self.tracestate == other.tracestate
            && self.dlqreason == other.dlqreason
            && self.dlqattempts == other.dlqattempts
            && self.dlqconsumer == other.dlqconsumer
            && self.dlqerror == other.dlqerror
            && self.dlqtime == other.dlqtime
            && self.data.get() == other.data.get()
    }
}

impl Eq for Event {}

impl Event {
    /// Interpreta el atributo `time` como instante.
    ///
    /// # Errores
    ///
    /// [`FluxError::Envelope`] si el `time` no es RFC 3339 — algo que `parse_event` no
    /// comprueba, igual que los demás SDKs.
    pub fn event_time(&self) -> Result<DateTime<Utc>, FluxError> {
        DateTime::parse_from_rfc3339(&self.time)
            .map(|t| t.with_timezone(&Utc))
            .map_err(|e| FluxError::Envelope(format!("`time` no es RFC 3339: {}: {e}", self.time)))
    }

    /// Decodifica el payload en el tipo de la aplicación.
    ///
    /// `data` se guarda sin interpretar para preservar la fidelidad del replay, así que
    /// éste es el punto donde el evento se convierte en algo tipado:
    ///
    /// ```no_run
    /// # use serde::Deserialize;
    /// #[derive(Deserialize)]
    /// #[serde(rename_all = "camelCase")]
    /// struct PedidoCreado {
    ///     pedido_id: String,
    ///     aggregate_version: u64,
    ///     total_cents: i64,
    /// }
    /// # fn f(evento: &flux::Event) -> Result<(), flux::FluxError> {
    /// let pedido = evento.data_as::<PedidoCreado>()?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errores
    ///
    /// Un fallo aquí **NO** es POISON: el envelope era correcto y el mensaje llegó al
    /// handler. Es un desajuste de contrato entre productor y consumidor, es decir
    /// PERMANENT — por eso devuelve [`FluxError::Permanent`] (04-errors.md §1.2).
    pub fn data_as<T: DeserializeOwned>(&self) -> Result<T, FluxError> {
        serde_json::from_str(self.data.get()).map_err(|e| {
            FluxError::permanent(format!(
                "el payload del evento {} no encaja en el tipo esperado",
                self.id
            ))
            .with_code("DATA_SCHEMA_MISMATCH")
            .with_source(e)
        })
    }

    /// Deriva los tokens del subject de NATS a partir del `type`.
    ///
    /// Útil en un handler que se suscribió con wildcard y necesita saber qué evento
    /// concreto llegó.
    ///
    /// # Errores
    ///
    /// [`FluxError::InvalidSubject`] si el `type` no sigue
    /// `com.flux.<dominio>.<agregado>.<evento>.v<major>`.
    pub fn parsed_type(&self) -> Result<crate::protocol::ParsedSubject, FluxError> {
        let invalid = || FluxError::InvalidSubject {
            subject: self.event_type.clone(),
            reason: "el `type` no sigue com.flux.<dominio>.<agregado>.<evento>.v<major>"
                .to_string(),
        };
        let rest = self
            .event_type
            .strip_prefix("com.flux.")
            .ok_or_else(invalid)?;
        let t: Vec<&str> = rest.split('.').collect();
        if t.len() != 4 {
            return Err(invalid());
        }
        // El `type` lleva la versión al final y el subject en la posición 3.
        crate::protocol::parse_subject(&format!("{}.{}.{}.{}", t[0], t[1], t[3], t[2]))
    }
}

/// Los ÚNICOS atributos admitidos en la raíz del envelope. Todo lo demás va dentro de
/// `data` — 01-envelope.md §3.4.
///
/// La lista es cerrada y eso es deliberado: las extensiones de CloudEvents solo admiten
/// `String`, `Integer`, `Boolean`, `Binary`, `URI` y `Timestamp`, nunca objetos ni
/// arrays. Un equipo que empieza a colgar metadatos de la raíz acaba serializando JSON
/// dentro de un string, y a partir de ahí el envelope deja de ser interoperable.
pub const ALLOWED_ROOT_ATTRIBUTES: [&str; 22] = [
    "specversion",
    "id",
    "source",
    "type",
    "time",
    "datacontenttype",
    "dataschema",
    "subject",
    "correlationid",
    "tenantid",
    "producerversion",
    "dataclassification",
    "causationid",
    "partitionkey",
    "traceparent",
    "tracestate",
    "dlqreason",
    "dlqattempts",
    "dlqconsumer",
    "dlqerror",
    "dlqtime",
    "data",
];

/// Atributos que, si faltan, hacen POISON al mensaje — 04-errors.md §1.3.
const REQUIRED_ATTRIBUTES: [&str; 8] = [
    "specversion",
    "id",
    "source",
    "type",
    "time",
    "datacontenttype",
    "dataschema",
    "data",
];

/// Extensiones del perfil flux cuya ausencia es POISON — 01-envelope.md §3.1.
///
/// Se exigen **de verdad**, no solo en la prosa. Si no se exigieran no serían
/// obligatorias, serían recomendadas, y cada consumidor tendría que tratar el caso
/// ausente — que es exactamente lo que declararlas obligatorias pretendía evitar.
///
/// Además, asumir un valor por defecto es peligroso en las cuatro: un
/// `dataclassification` ausente tomado como `internal` hace que PII circule con 30 días
/// de retención en vez de 7, y un `tenantid` ausente tomado como `system` cruza
/// fronteras de tenant. Ver 06-security.md §4 y §5.
const REQUIRED_EXTENSIONS: [&str; 4] = [
    "correlationid",
    "tenantid",
    "producerversion",
    "dataclassification",
];

/// Atributos que **DEBEN** llegar como cadena JSON — 01-envelope.md §2.4.
///
/// `serde` ya rechazaría un número en un campo `String`, pero lo haría al deserializar
/// el struct entero y el error acabaría con el código genérico de desajuste de tipo, el
/// mismo que un `{"dlqattempts": "seis"}`. Se comprueban aquí para que el operador
/// distinga "el productor mandó el tipo equivocado en un atributo de identidad" de un
/// desajuste cualquiera, y para que el código coincida con el de los demás SDKs ante el
/// mismo cuerpo.
const STRING_ATTRIBUTES: [&str; 7] = [
    "id",
    "source",
    "type",
    "time",
    "correlationid",
    "tenantid",
    "producerversion",
];

// ─── Construcción ────────────────────────────────────────────────────────────

/// Todo lo que hace falta para construir un evento.
///
/// El desarrollador de la aplicación **NO** rellena esto: lo rellena `Bus::publish` a
/// partir de la configuración de `connect()` y del contexto del evento en curso. Se
/// expone para tests y para quien construya un transporte propio — 01-envelope.md §5.
#[derive(Debug)]
pub struct BuildEventInput {
    /// Subject de NATS. Determina el `type`.
    pub subject: String,
    /// Payload ya serializado y validado con [`data_to_raw`].
    pub data: Box<RawValue>,

    /// UUIDv7 generado por el SDK.
    pub id: String,
    /// `/<entorno>/<servicio>`.
    pub source: String,
    /// SemVer del servicio emisor.
    pub producerversion: String,
    /// Tenant propietario.
    pub tenantid: String,
    /// Clasificación del dato.
    pub dataclassification: DataClassification,
    /// URI del JSON Schema.
    pub dataschema: String,
    /// Correlación heredada, o el propio `id` si el evento no nace de otro.
    pub correlationid: String,

    /// Instante en que **ocurrió** el hecho, no el de publicación. `None` = ahora.
    pub time: Option<DateTime<Utc>>,

    /// Va al atributo `subject` de CloudEvents. Ver [`Event::aggregate_id`].
    pub aggregate_id: Option<String>,
    /// `id` del evento que provocó éste.
    pub causationid: Option<String>,
    /// Por defecto, igual al `aggregate_id`.
    pub partitionkey: Option<String>,
    /// W3C Trace Context.
    pub traceparent: Option<String>,
    /// W3C Trace Context.
    pub tracestate: Option<String>,
}

/// Serializa el payload y exige que sea un **objeto** JSON en la raíz.
///
/// Ni array ni escalar: con un array o un escalar arriba no se pueden añadir campos
/// después sin romper el esquema, y toda la política de compatibilidad de
/// 05-compatibility.md se apoya en poder hacerlo — 01-envelope.md §4.
///
/// La comprobación es "serializa y mira el primer byte no blanco" porque `T: Serialize`
/// no dice nada sobre la FORMA JSON del valor: un struct, un `HashMap` y un `Value::Object`
/// producen todos un objeto, y un `Vec<T>` o un `String` producen algo que no lo es.
///
/// # Errores
///
/// [`FluxError::Envelope`] si el valor no serializa o no produce un objeto.
pub fn data_to_raw<T: Serialize + ?Sized>(data: &T) -> Result<Box<RawValue>, FluxError> {
    let raw = serde_json::value::to_raw_value(data)
        .map_err(|e| FluxError::Envelope(format!("`data` no es serializable a JSON: {e}")))?;
    if !raw.get().trim_start().starts_with('{') {
        return Err(FluxError::Envelope(
            "`data` debe ser un objeto JSON en la raíz (ni array ni escalar), para poder \
             añadir campos sin romper el contrato (01-envelope.md §4)"
                .to_string(),
        ));
    }
    Ok(raw)
}

/// Construye un evento válido a partir de la entrada.
///
/// Valida en tiempo de ejecución lo que el sistema de tipos no puede: que las cuatro
/// extensiones obligatorias no lleguen vacías. `String` vacío y `String` ausente son
/// indistinguibles en cualquier lenguaje, así que la comprobación es explícita.
///
/// # Errores
///
/// [`FluxError::InvalidSubject`] si el subject no cumple el naming;
/// [`FluxError::Envelope`] si falta algún atributo obligatorio.
pub fn build_event(input: BuildEventInput) -> Result<Event, FluxError> {
    let event_type = subject_to_type(&input.subject)?;

    for (name, value) in [
        ("id", &input.id),
        ("source", &input.source),
        ("dataschema", &input.dataschema),
        ("correlationid", &input.correlationid),
        ("tenantid", &input.tenantid),
        ("producerversion", &input.producerversion),
    ] {
        if value.is_empty() {
            return Err(FluxError::Envelope(format!(
                "`{name}` es obligatorio y llegó vacío. Lo rellena el SDK a partir de \
                 ConnectOptions; si estás llamando a build_event a mano, rellénalo \
                 (01-envelope.md §5)"
            )));
        }
    }

    // Por convención partitionkey = aggregateId, salvo que se pida otra cosa
    // explícitamente — 01-envelope.md §3.2.
    let partitionkey = input.partitionkey.or_else(|| input.aggregate_id.clone());

    Ok(Event {
        specversion: SPEC_VERSION.to_string(),
        id: input.id,
        source: input.source,
        event_type,
        time: format_time(input.time.unwrap_or_else(Utc::now)),
        datacontenttype: DATA_CONTENT_TYPE.to_string(),
        dataschema: input.dataschema,
        aggregate_id: input.aggregate_id,
        correlationid: input.correlationid,
        tenantid: input.tenantid,
        producerversion: input.producerversion,
        dataclassification: input.dataclassification,
        causationid: input.causationid,
        partitionkey,
        traceparent: input.traceparent,
        tracestate: input.tracestate,
        dlqreason: None,
        dlqattempts: None,
        dlqconsumer: None,
        dlqerror: None,
        dlqtime: None,
        data: input.data,
    })
}

// ─── Serialización ───────────────────────────────────────────────────────────

/// Convierte el evento en los bytes que viajan por NATS.
///
/// `serde_json` emite UTF-8 con los caracteres no-ASCII **literales** y no escapa
/// `<`, `>` ni `&`, que es justo lo que exige 01-envelope.md §1.1 y lo que hacen Node,
/// Python y Go. (.NET necesita `UnsafeRelaxedJsonEscaping` para lo mismo; aquí sale
/// gratis, pero hay un test que lo fija por si alguien cambia el serializador.)
///
/// # Errores
///
/// [`FluxError::Envelope`] al superar 1 MiB. Falla aquí y no en el broker: el broker
/// devuelve "maximum payload exceeded", que no dice qué hacer al respecto
/// — 01-envelope.md §7.
pub fn serialize(event: &Event) -> Result<Vec<u8>, FluxError> {
    let bytes = serde_json::to_vec(event)
        .map_err(|e| FluxError::Envelope(format!("el evento no es serializable a JSON: {e}")))?;
    if bytes.len() > MAX_MESSAGE_BYTES {
        return Err(FluxError::Envelope(format!(
            "evento de {} bytes supera el límite de {MAX_MESSAGE_BYTES} (1 MiB). Aplica \
             claim-check: sube el contenido a almacenamiento de objetos y publica \
             {{uri, sha256, bytes}} en su lugar (01-envelope.md §7). El sha256 no es \
             decorativo: sin él el consumidor no puede distinguir \"aún no replicado\" de \
             \"fue sustituido\"",
            bytes.len()
        )));
    }
    Ok(bytes)
}

// ─── Parseo ──────────────────────────────────────────────────────────────────

fn raw_string(map: &BTreeMap<String, Box<RawValue>>, key: &str) -> Option<String> {
    serde_json::from_str::<String>(map.get(key)?.get()).ok()
}

fn raw_or_absent(map: &BTreeMap<String, Box<RawValue>>, key: &str) -> String {
    map.get(key)
        .map_or_else(|| "<ausente>".to_string(), |v| v.get().to_string())
}

/// Informa si un atributo falta o llega como el literal `null`.
///
/// Un atributo obligatorio a `null` cuenta como AUSENTE: 01-envelope.md §4 prohíbe usar
/// `null` para "no aplica", así que aceptarlo daría dos formas distintas de decir "no
/// está" y solo una de ellas sería POISON.
fn null_or_absent(map: &BTreeMap<String, Box<RawValue>>, key: &str) -> bool {
    map.get(key).is_none_or(|v| v.get().trim() == "null")
}

/// Interpreta el cuerpo de un mensaje como CloudEvent de flux.
///
/// # Errores
///
/// Todo fallo aquí es **POISON**: el mensaje ni siquiera es interpretable, así que nunca
/// llega al handler y va a la DLQ con alerta inmediata — 04-errors.md §1.3.
///
/// El orden de las comprobaciones es el mismo que en los SDKs de Node y Go, para que los
/// tres den el mismo `code` ante el mismo mensaje corrupto. Ese código acaba en la
/// extensión `dlqerror` y en las métricas: si dos SDKs emiten códigos distintos para el
/// mismo fallo, agrupar por causa deja de funcionar en cuanto el ecosistema es polyglot
/// — que es siempre.
pub fn parse_event(bytes: &[u8]) -> Result<Event, FluxError> {
    // Se decodifica primero a un mapa de valores CRUDOS y no directamente al struct por
    // dos razones:
    //
    //  1. Los atributos raíz son una lista CERRADA y hay que poder enumerar los
    //     desconocidos en el mensaje de error. `deny_unknown_fields` aborta en el primero
    //     y no dice cuáles son los demás.
    //  2. El orden de los códigos de error es parte del contrato entre SDKs, y un
    //     `from_slice` al struct lo decidiría por nosotros según el orden de los campos.
    let raw: BTreeMap<String, Box<RawValue>> = match serde_json::from_slice(bytes) {
        Ok(m) => m,
        Err(e) => {
            // Distinguir "no es JSON" de "es JSON pero no es un objeto" — son dos
            // códigos distintos en todos los SDKs.
            return Err(if serde_json::from_slice::<&RawValue>(bytes).is_ok() {
                FluxError::poison("el cuerpo del mensaje no es un objeto JSON")
                    .with_code("NOT_AN_OBJECT")
                    .with_source(e)
            } else {
                FluxError::poison("el cuerpo del mensaje no es JSON válido")
                    .with_code("MALFORMED_JSON")
                    .with_source(e)
            });
        }
    };

    if raw_string(&raw, "specversion").as_deref() != Some(SPEC_VERSION) {
        return Err(FluxError::poison(format!(
            "specversion no soportada: {} (se esperaba \"{SPEC_VERSION}\")",
            raw_or_absent(&raw, "specversion")
        ))
        .with_code("UNSUPPORTED_SPECVERSION"));
    }

    let missing: Vec<&str> = REQUIRED_ATTRIBUTES
        .iter()
        .copied()
        .filter(|a| null_or_absent(&raw, a))
        .collect();
    if !missing.is_empty() {
        return Err(FluxError::poison(format!(
            "faltan atributos obligatorios de CloudEvents: {}",
            missing.join(", ")
        ))
        .with_code("MISSING_REQUIRED_ATTRIBUTE"));
    }

    // `""` cuenta como ausente igual que `null`: 01-envelope.md §3.3 prohíbe darle
    // significado propio a un valor vacío, así que un `tenantid: ""` no es un tenant.
    // El vacío se evalúa ANTES que el enum: un `dataclassification: ""` es
    // MISSING_REQUIRED_EXTENSION, no INVALID_DATACLASSIFICATION (§3.1, tabla de códigos).
    let missing_ext: Vec<&str> = REQUIRED_EXTENSIONS
        .iter()
        .copied()
        .filter(|a| {
            null_or_absent(&raw, a) || raw.get(*a).is_some_and(|v| v.get().trim() == "\"\"")
        })
        .collect();
    if !missing_ext.is_empty() {
        return Err(FluxError::poison(format!(
            "faltan extensiones obligatorias del perfil flux: {}. Su ausencia no es \
             recuperable asumiendo un valor: un dataclassification ausente tomado como \
             \"internal\" haría circular PII con la retención larga (01-envelope.md §3.1)",
            missing_ext.join(", ")
        ))
        .with_code("MISSING_REQUIRED_EXTENSION"));
    }

    // Un valor fuera del enum no se puede degradar a algo seguro: la retención y las ACLs
    // de 06-security.md §5 se derivan de él, así que inventarle un sentido es peor que
    // rechazar el mensaje. `raw_string` devuelve None si el valor no es una cadena, de
    // modo que un `"dataclassification": 42` cae aquí y no en la comprobación de tipos.
    let classification_ok = raw_string(&raw, "dataclassification")
        .and_then(|s| DataClassification::from_str_exact(&s))
        .is_some();
    if !classification_ok {
        return Err(FluxError::poison(format!(
            "dataclassification inválido: {}. Valores permitidos: public, internal, \
             confidential, restricted",
            raw_or_absent(&raw, "dataclassification")
        ))
        .with_code("INVALID_DATACLASSIFICATION"));
    }

    // Los tipos son exactos: {"tenantid": 42} es POISON, no el tenant "42". Los
    // deserializadores discrepan por defecto (Jackson coacciona, Node propagaba el
    // número) y el que "funciona" es el peor — 01-envelope.md §2.4.
    for a in STRING_ATTRIBUTES {
        if raw_string(&raw, a).is_none() {
            return Err(FluxError::poison(format!(
                "{a} debe ser una cadena, llegó {}",
                raw_or_absent(&raw, a)
            ))
            .with_code("WRONG_ATTRIBUTE_TYPE"));
        }
    }

    if raw_string(&raw, "datacontenttype").as_deref() != Some(DATA_CONTENT_TYPE) {
        return Err(FluxError::poison(format!(
            "datacontenttype no soportado: {}",
            raw_or_absent(&raw, "datacontenttype")
        ))
        .with_code("UNSUPPORTED_CONTENT_TYPE"));
    }

    // La comparación de nombres de atributo es CASE-SENSITIVE: `{"ID": "..."}` no es el
    // atributo `id`, es un atributo raíz desconocido — 01-envelope.md §2.3. En Rust serde
    // ya distingue mayúsculas (a diferencia de `encoding/json` de Go, que empareja sin
    // distinguirlas), pero la regla se comprueba aquí igualmente para no depender de esa
    // propiedad del deserializador.
    let unknown: Vec<&str> = raw
        .keys()
        .map(String::as_str)
        .filter(|k| !ALLOWED_ROOT_ATTRIBUTES.contains(k))
        .collect();
    if !unknown.is_empty() {
        return Err(FluxError::poison(format!(
            "atributos raíz desconocidos: {}. Todo metadato adicional va dentro de `data`",
            unknown.join(", ")
        ))
        .with_code("UNKNOWN_ROOT_ATTRIBUTE"));
    }

    serde_json::from_slice(bytes).map_err(|e| {
        // Llegar aquí significa que un atributo conocido tiene el tipo equivocado
        // (p. ej. `"dlqattempts": "seis"`). Sigue siendo un mensaje ininterpretable.
        FluxError::poison("un atributo del envelope tiene un tipo incompatible con el contrato")
            .with_code("INVALID_ATTRIBUTE_TYPE")
            .with_source(e)
    })
}

// ─── DLQ ─────────────────────────────────────────────────────────────────────

/// Por qué y cómo un evento acabó en la DLQ.
#[derive(Debug, Clone)]
pub struct DlqInfo {
    /// Clase que lo mató.
    pub reason: DlqReason,
    /// Número de entrega en que murió.
    pub attempts: u32,
    /// Durable del consumidor.
    pub consumer: String,
    /// Texto del error, ya con su código delante.
    pub error: String,
}

/// Prepara un evento para la DLQ: el CloudEvent original **íntegro** más las extensiones
/// `dlq*`. Sin wrapper.
///
/// Sin wrapper porque así reproducir un mensaje de la DLQ es borrar las extensiones
/// `dlq*` y republicar (`jq 'del(.dlq*)'`). Con un wrapper habría que desenvolver, y cada
/// consumidor de la DLQ tendría que conocer dos formatos — 04-errors.md §3.
///
/// El evento entra por valor: la copia es explícita en la firma y el original del
/// llamante nunca se muta.
///
/// ⚠️ Las extensiones `dlq*` quedan **antes** de `data`, porque en este struct están
/// declaradas antes. Esa es exactamente la divergencia que el SDK de Node introdujo con
/// `{...event, dlq*}` —dejándolas después— y que produjo dos secuencias de bytes para el
/// mismo evento. Aquí no puede repetirse sin reordenar el struct — 01-envelope.md §6.
#[must_use]
pub fn to_dlq_event(mut event: Event, info: &DlqInfo, now: DateTime<Utc>) -> Event {
    event.dlqreason = Some(info.reason);
    event.dlqattempts = Some(info.attempts);
    event.dlqconsumer = Some(info.consumer.clone());
    event.dlqerror = Some(truncate_chars(&info.error, 1024));
    event.dlqtime = Some(format_time(now));
    event
}

/// Quita las extensiones `dlq*` para reproducir un evento.
///
/// El `id` original se **CONSERVA**. Regenerarlo rompe la idempotencia de todos los
/// consumidores aguas abajo y convierte una recuperación en un incidente nuevo
/// — 04-errors.md §4.1.
#[must_use]
pub fn strip_dlq_extensions(mut event: Event) -> Event {
    event.dlqreason = None;
    event.dlqattempts = None;
    event.dlqconsumer = None;
    event.dlqerror = None;
    event.dlqtime = None;
    event
}

/// Recorta a `n` bytes sin partir un carácter multibyte.
///
/// En Rust cortar un `&str` por un índice que no es frontera de carácter **entra en
/// pánico**, así que aquí no hay opción: hay que buscar la frontera. El SDK de Node hace
/// `.slice(0, 1024)` sobre unidades UTF-16 y Go corta por bytes UTF-8, de modo que el
/// punto de corte de un mensaje con acentos puede diferir entre los tres. Es intencionado:
/// partir un carácter produciría un `dlqerror` con U+FFFD, que es peor que un mensaje
/// unos bytes más corto.
fn truncate_chars(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut cut = n;
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    s[..cut].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn input() -> BuildEventInput {
        BuildEventInput {
            subject: "pedidos.pedido.v1.creado".into(),
            data: data_to_raw(&json!({ "pedidoId": "ped-123", "totalCents": 9990 })).unwrap(),
            id: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55".into(),
            source: "/produccion/pedidos-api".into(),
            producerversion: "3.4.1".into(),
            tenantid: "acme".into(),
            dataclassification: DataClassification::Confidential,
            dataschema: "https://schemas.internal/pedidos/pedido/creado/1.2.0.json".into(),
            correlationid: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55".into(),
            time: Some(
                DateTime::parse_from_rfc3339("2026-08-20T10:25:39.412Z")
                    .unwrap()
                    .with_timezone(&Utc),
            ),
            aggregate_id: Some("ped-123".into()),
            causationid: None,
            partitionkey: None,
            traceparent: None,
            tracestate: None,
        }
    }

    // ── time ──────────────────────────────────────────────────────────────────

    /// Los cinco SDKs producen exactamente esta cadena para este instante.
    #[test]
    fn time_lleva_exactamente_tres_decimales_y_z() {
        let t = DateTime::parse_from_rfc3339("2025-08-20T10:25:39.410Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(format_time(t), "2025-08-20T10:25:39.410Z");
    }

    #[test]
    fn time_no_recorta_ceros_finales() {
        for (input, expected) in [
            ("2025-08-20T10:25:39.400Z", "2025-08-20T10:25:39.400Z"),
            ("2025-08-20T10:25:39.000Z", "2025-08-20T10:25:39.000Z"),
            // Un offset distinto de Z se normaliza a UTC.
            ("2025-08-20T12:25:39.410+02:00", "2025-08-20T10:25:39.410Z"),
        ] {
            let t = DateTime::parse_from_rfc3339(input)
                .unwrap()
                .with_timezone(&Utc);
            assert_eq!(format_time(t), expected);
        }
    }

    #[test]
    fn time_trunca_los_microsegundos_no_los_arrastra() {
        let t = DateTime::parse_from_rfc3339("2025-08-20T10:25:39.410999Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(format_time(t), "2025-08-20T10:25:39.410Z");
    }

    // ── orden de claves ───────────────────────────────────────────────────────

    /// §6 es normativo: `data` va SIEMPRE el último y el resto en orden de declaración.
    ///
    /// La comprobación se hace sobre las claves de la RAÍZ y no buscando en el texto:
    /// el payload también tiene claves y un `find` ingenuo las contaría. Que este test
    /// pueda escribirse así depende de la feature `preserve_order` de serde_json — sin
    /// ella, releer el JSON reordenaría las claves alfabéticamente y el test mentiría.
    #[test]
    fn data_va_el_ultimo() {
        let ev = build_event(input()).unwrap();
        let json = String::from_utf8(serialize(&ev).unwrap()).unwrap();
        assert!(json.starts_with(r#"{"specversion":"1.0","id":"#), "{json}");

        let raiz: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(
            raiz.keys().next_back().map(String::as_str),
            Some("data"),
            "data debe ser la ÚLTIMA clave de la raíz: {json}"
        );
    }

    #[test]
    fn el_orden_es_el_de_la_spec() {
        let ev = build_event(input()).unwrap();
        let json = String::from_utf8(serialize(&ev).unwrap()).unwrap();
        let orden = [
            "specversion",
            "id",
            "source",
            "type",
            "time",
            "datacontenttype",
            "dataschema",
            "subject",
            "correlationid",
            "tenantid",
            "producerversion",
            "dataclassification",
            "partitionkey",
            "data",
        ];
        let mut last = 0;
        for key in orden {
            let pos = json
                .find(&format!("\"{key}\":"))
                .unwrap_or_else(|| panic!("falta {key} en {json}"));
            assert!(pos > last, "{key} está fuera de orden en {json}");
            last = pos;
        }
    }

    /// En el evento de DLQ las extensiones `dlq*` van ANTES de `data`. Es la divergencia
    /// real que tuvo el SDK de Node — 01-envelope.md §6.
    #[test]
    fn en_la_dlq_las_extensiones_van_antes_de_data() {
        let ev = build_event(input()).unwrap();
        let dlq = to_dlq_event(
            ev,
            &DlqInfo {
                reason: DlqReason::Permanent,
                attempts: 1,
                consumer: "facturacion-api__pedidos_pedido_v1_creado".into(),
                error: "PEDIDO_YA_CANCELADO: el pedido ped-123 estaba cancelado".into(),
            },
            Utc::now(),
        );
        let json = String::from_utf8(serialize(&dlq).unwrap()).unwrap();
        let data_pos = json.find(r#""data":"#).unwrap();
        for ext in [
            "dlqreason",
            "dlqattempts",
            "dlqconsumer",
            "dlqerror",
            "dlqtime",
        ] {
            let pos = json.find(&format!("\"{ext}\":")).unwrap();
            assert!(pos < data_pos, "{ext} debería ir antes de data: {json}");
        }
    }

    // ── UTF-8 literal ─────────────────────────────────────────────────────────

    /// §1.1: no-ASCII LITERAL, sin escapes `\u`. `serde_json` lo hace bien por defecto;
    /// este test lo fija para que un cambio de serializador no lo rompa en silencio.
    #[test]
    fn utf8_literal_sin_escapes_unicode() {
        let mut inp = input();
        inp.data =
            data_to_raw(&json!({ "descripcion": "café con leche ñ €", "emoji": "🚀" })).unwrap();
        let ev = build_event(inp).unwrap();
        let bytes = serialize(&ev).unwrap();
        let json = String::from_utf8(bytes.clone()).unwrap();

        assert!(json.contains("café con leche ñ €"), "{json}");
        assert!(json.contains('🚀'), "{json}");
        assert!(!json.contains("\\u"), "no debe haber escapes \\u: {json}");
        // Y los bytes son UTF-8 de verdad: 'é' es 0xC3 0xA9.
        assert!(bytes.windows(2).any(|w| w == [0xC3, 0xA9]));
    }

    /// Go necesita `SetEscapeHTML(false)` y .NET `UnsafeRelaxedJsonEscaping` para esto.
    #[test]
    fn no_se_escapan_los_caracteres_html() {
        let mut inp = input();
        inp.data = data_to_raw(&json!({ "nota": "a < b & c > d" })).unwrap();
        let ev = build_event(inp).unwrap();
        let json = String::from_utf8(serialize(&ev).unwrap()).unwrap();
        assert!(json.contains("a < b & c > d"), "{json}");
    }

    // ── fidelidad del payload ─────────────────────────────────────────────────

    /// §2.5: un SDK NO DEBE reescribir los números de `data` al deserializar y volver a
    /// serializar. Con `Box<RawValue>` sale gratis.
    #[test]
    fn el_payload_sobrevive_al_round_trip_byte_a_byte() {
        let mut inp = input();
        inp.data = RawValue::from_string(
            r#"{"zeta":1,"alfa":2,"precio":4995.00,"grande":123456789012345678901}"#.to_string(),
        )
        .unwrap();
        let ev = build_event(inp).unwrap();
        let bytes = serialize(&ev).unwrap();

        let vuelto = parse_event(&bytes).unwrap();
        assert_eq!(
            vuelto.data.get(),
            r#"{"zeta":1,"alfa":2,"precio":4995.00,"grande":123456789012345678901}"#
        );
        assert_eq!(serialize(&vuelto).unwrap(), bytes, "replay verbatim");
        assert_eq!(vuelto, ev);
    }

    // ── construcción ──────────────────────────────────────────────────────────

    #[test]
    fn build_event_deriva_type_y_partitionkey() {
        let ev = build_event(input()).unwrap();
        assert_eq!(ev.event_type, "com.flux.pedidos.pedido.creado.v1");
        assert_eq!(ev.specversion, "1.0");
        assert_eq!(ev.datacontenttype, "application/json");
        assert_eq!(ev.aggregate_id.as_deref(), Some("ped-123"));
        // Por convención partitionkey = aggregateId.
        assert_eq!(ev.partitionkey.as_deref(), Some("ped-123"));
        assert_eq!(ev.time, "2026-08-20T10:25:39.412Z");
    }

    #[test]
    fn build_event_rechaza_data_que_no_sea_objeto() {
        assert!(data_to_raw(&json!([1, 2, 3])).is_err());
        assert!(data_to_raw(&json!("un escalar")).is_err());
        assert!(data_to_raw(&json!(42)).is_err());
        assert!(data_to_raw(&json!(null)).is_err());
        assert!(data_to_raw(&json!({})).is_ok());
    }

    #[test]
    fn build_event_exige_las_extensiones_obligatorias() {
        for mutate in [
            (|i: &mut BuildEventInput| i.tenantid.clear()) as fn(&mut BuildEventInput),
            |i: &mut BuildEventInput| i.correlationid.clear(),
            |i: &mut BuildEventInput| i.producerversion.clear(),
            |i: &mut BuildEventInput| i.dataschema.clear(),
            |i: &mut BuildEventInput| i.id.clear(),
            |i: &mut BuildEventInput| i.source.clear(),
        ] {
            let mut inp = input();
            mutate(&mut inp);
            assert!(matches!(build_event(inp), Err(FluxError::Envelope(_))));
        }
    }

    #[test]
    fn serialize_rechaza_por_encima_de_1_mib() {
        let mut inp = input();
        inp.data = data_to_raw(&json!({ "relleno": "x".repeat(MAX_MESSAGE_BYTES) })).unwrap();
        let ev = build_event(inp).unwrap();
        let err = serialize(&ev).unwrap_err().to_string();
        assert!(err.contains("claim-check"), "{err}");
        assert!(err.contains("sha256"), "{err}");
    }

    // ── parseo / POISON ───────────────────────────────────────────────────────

    fn valido() -> serde_json::Value {
        json!({
            "specversion": "1.0",
            "id": "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
            "source": "/produccion/pedidos-api",
            "type": "com.flux.pedidos.pedido.creado.v1",
            "subject": "ped-123",
            "time": "2026-08-20T10:25:39.412Z",
            "datacontenttype": "application/json",
            "dataschema": "https://schemas.internal/pedidos/pedido/creado/1.2.0.json",
            "correlationid": "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
            "tenantid": "acme",
            "producerversion": "3.4.1",
            "dataclassification": "confidential",
            "data": { "pedidoId": "ped-123", "totalCents": 9990 }
        })
    }

    fn code_of(v: &serde_json::Value) -> String {
        let bytes = serde_json::to_vec(v).unwrap();
        let err = parse_event(&bytes).expect_err("debería ser POISON");
        assert_eq!(err.class(), Some(crate::errors::ErrorClass::Poison));
        err.code().unwrap().to_string()
    }

    #[test]
    fn parsea_un_evento_valido() {
        let ev = parse_event(&serde_json::to_vec(&valido()).unwrap()).unwrap();
        assert_eq!(ev.tenantid, "acme");
        assert_eq!(ev.dataclassification, DataClassification::Confidential);
        assert_eq!(ev.aggregate_id.as_deref(), Some("ped-123"));
        assert_eq!(ev.dlqattempts, None);
    }

    #[test]
    fn json_malformado_y_no_objeto() {
        let err = parse_event(b"{no soy json").unwrap_err();
        assert_eq!(err.code(), Some("MALFORMED_JSON"));
        let err = parse_event(b"[1,2,3]").unwrap_err();
        assert_eq!(err.code(), Some("NOT_AN_OBJECT"));
        let err = parse_event(b"\"una cadena\"").unwrap_err();
        assert_eq!(err.code(), Some("NOT_AN_OBJECT"));
    }

    #[test]
    fn specversion_desconocida() {
        let mut v = valido();
        v["specversion"] = json!("0.3");
        assert_eq!(code_of(&v), "UNSUPPORTED_SPECVERSION");
    }

    #[test]
    fn falta_un_atributo_obligatorio() {
        for attr in ["id", "source", "type", "time", "dataschema", "data"] {
            let mut v = valido();
            v.as_object_mut().unwrap().remove(attr);
            assert_eq!(code_of(&v), "MISSING_REQUIRED_ATTRIBUTE", "quitando {attr}");
        }
    }

    /// La tabla de códigos de 01-envelope.md §3.1, literal.
    #[test]
    fn tabla_de_codigos_de_extensiones_obligatorias() {
        for ext in REQUIRED_EXTENSIONS {
            // Ausente.
            let mut v = valido();
            v.as_object_mut().unwrap().remove(ext);
            assert_eq!(code_of(&v), "MISSING_REQUIRED_EXTENSION", "{ext} ausente");

            // null.
            let mut v = valido();
            v[ext] = json!(null);
            assert_eq!(code_of(&v), "MISSING_REQUIRED_EXTENSION", "{ext} null");

            // "".
            let mut v = valido();
            v[ext] = json!("");
            assert_eq!(code_of(&v), "MISSING_REQUIRED_EXTENSION", "{ext} vacío");
        }
    }

    #[test]
    fn tenantid_numerico_es_wrong_attribute_type() {
        let mut v = valido();
        v["tenantid"] = json!(42);
        // No es el tenant "42": los tipos son exactos — §2.4.
        assert_eq!(code_of(&v), "WRONG_ATTRIBUTE_TYPE");
    }

    #[test]
    fn todos_los_atributos_de_texto_rechazan_la_coercion() {
        for attr in STRING_ATTRIBUTES {
            let mut v = valido();
            v[attr] = json!(42);
            assert_eq!(code_of(&v), "WRONG_ATTRIBUTE_TYPE", "{attr}");
        }
    }

    #[test]
    fn dataclassification_fuera_del_enum() {
        let mut v = valido();
        v["dataclassification"] = json!("secreto");
        assert_eq!(code_of(&v), "INVALID_DATACLASSIFICATION");

        // Un número tampoco es válido, y cae aquí y no en la comprobación de tipos.
        let mut v = valido();
        v["dataclassification"] = json!(42);
        assert_eq!(code_of(&v), "INVALID_DATACLASSIFICATION");
    }

    /// El vacío se evalúa ANTES que el enum — §3.1, última fila de la tabla.
    #[test]
    fn dataclassification_vacia_es_missing_no_invalid() {
        let mut v = valido();
        v["dataclassification"] = json!("");
        assert_eq!(code_of(&v), "MISSING_REQUIRED_EXTENSION");
    }

    #[test]
    fn datacontenttype_no_soportado() {
        let mut v = valido();
        v["datacontenttype"] = json!("application/xml");
        assert_eq!(code_of(&v), "UNSUPPORTED_CONTENT_TYPE");
    }

    #[test]
    fn atributo_raiz_desconocido() {
        let mut v = valido();
        v["miMetadato"] = json!("algo");
        assert_eq!(code_of(&v), "UNKNOWN_ROOT_ATTRIBUTE");
    }

    /// §2.3: los nombres se comparan respetando mayúsculas. `{"ID": …}` no es `id`.
    #[test]
    fn la_comparacion_de_nombres_es_case_sensitive() {
        let mut v = valido();
        v["ID"] = json!("otro");
        assert_eq!(code_of(&v), "UNKNOWN_ROOT_ATTRIBUTE");
    }

    #[test]
    fn un_atributo_conocido_con_tipo_incompatible() {
        let mut v = valido();
        v["dlqattempts"] = json!("seis");
        assert_eq!(code_of(&v), "INVALID_ATTRIBUTE_TYPE");
    }

    // ── DLQ ───────────────────────────────────────────────────────────────────

    #[test]
    fn el_evento_de_dlq_es_el_original_integro() {
        let ev = build_event(input()).unwrap();
        let dlq = to_dlq_event(
            ev.clone(),
            &DlqInfo {
                reason: DlqReason::Retryable,
                attempts: 6,
                consumer: "facturacion-api__pedidos_pedido_v1_creado".into(),
                error: "HTTP_503: proveedor caído".into(),
            },
            Utc::now(),
        );
        assert_eq!(dlq.id, ev.id, "el id original se conserva");
        assert_eq!(dlq.data.get(), ev.data.get());
        assert_eq!(dlq.dlqattempts, Some(6));
        assert_eq!(dlq.dlqreason, Some(DlqReason::Retryable));

        // Y el replay es exactamente "quitar dlq* y republicar".
        let replay = strip_dlq_extensions(dlq);
        assert_eq!(replay, ev);
    }

    #[test]
    fn dlqerror_se_trunca_sin_partir_caracteres() {
        let ev = build_event(input()).unwrap();
        let dlq = to_dlq_event(
            ev,
            &DlqInfo {
                reason: DlqReason::Permanent,
                attempts: 1,
                consumer: "c".into(),
                error: "ñ".repeat(2000), // 4000 bytes
            },
            Utc::now(),
        );
        let err = dlq.dlqerror.unwrap();
        assert!(err.len() <= 1024);
        // Si se hubiese partido un carácter, esto entraría en pánico o daría U+FFFD.
        assert!(err.chars().all(|c| c == 'ñ'));
    }

    #[test]
    fn el_evento_de_dlq_se_reparsea() {
        let ev = build_event(input()).unwrap();
        let dlq = to_dlq_event(
            ev,
            &DlqInfo {
                reason: DlqReason::Poison,
                attempts: 1,
                consumer: "c".into(),
                error: "MALFORMED_JSON".into(),
            },
            Utc::now(),
        );
        let bytes = serialize(&dlq).unwrap();
        let vuelto = parse_event(&bytes).unwrap();
        assert_eq!(vuelto, dlq);
    }

    // ── varios ────────────────────────────────────────────────────────────────

    #[test]
    fn data_as_da_permanent_no_poison() {
        #[derive(Debug, serde::Deserialize)]
        struct Esperado {
            #[allow(dead_code)]
            campo_que_no_existe: String,
        }
        let ev = build_event(input()).unwrap();
        let err = ev.data_as::<Esperado>().unwrap_err();
        assert_eq!(err.class(), Some(crate::errors::ErrorClass::Permanent));
        assert_eq!(err.code(), Some("DATA_SCHEMA_MISMATCH"));
    }

    #[test]
    fn parsed_type_recupera_los_tokens() {
        let ev = build_event(input()).unwrap();
        let p = ev.parsed_type().unwrap();
        assert_eq!(p.subject(), "pedidos.pedido.v1.creado");
    }

    #[test]
    fn event_time_interpreta_el_instante() {
        let ev = build_event(input()).unwrap();
        assert_eq!(format_time(ev.event_time().unwrap()), ev.time);
    }
}
