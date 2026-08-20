//! Validación del payload contra su JSON Schema. **Nivel L3** — 00-protocol.md §5.
//!
//! Detrás de la feature `validation`, que **no está activa por defecto**, igual que
//! `signing` y por el mismo motivo: L3 es opt-in, así que su coste también debe serlo. Un
//! servicio que se conforma con L2 no tiene por qué arrastrar un validador de JSON Schema
//! —con su tiempo de compilación y su superficie de auditoría— para no ejecutarlo nunca.
//!
//! ```toml
//! flux = { path = "../sdk-rust", features = ["validation"] }
//! ```
//!
//! # Qué problema resuelve
//!
//! Sin esto, un productor puede publicar un payload que viola su propio `dataschema` y
//! nadie se entera hasta que un consumidor —posiblemente de otro equipo, en otro lenguaje
//! y otra semana— se atraganta. El error aparece lejísimos de su causa: el que lo ve no
//! puede arreglarlo y el que puede arreglarlo no lo ve.
//!
//! Validar en `publish()` lo convierte en un fallo del servicio que lo generó.
//!
//! # Resolución de esquemas: bundle, no HTTP
//!
//! El `dataschema` es una URI, pero un SDK L3 **NO DEBE** resolverla por red al publicar.
//! Validar está en la ruta caliente —una petición HTTP por evento es inaceptable— y una
//! caché con TTL es peor que el problema: abre una ventana en la que dos servicios validan
//! el mismo subject contra versiones distintas del mismo esquema y ninguno de los dos
//! falla.
//!
//! En su lugar el bundle llega **como dato** ([`SchemaBundle`]), se genera con
//! `scripts/bundle-schemas.mjs` y se despliega **con el servicio**. Así la versión del
//! esquema queda clavada a la versión del servicio, que es justo lo que `producerversion`
//! promete poder acotar. Por eso este módulo compila el crate `jsonschema` **sin sus
//! features por defecto**: las de serie traen `resolve-http` y con él `reqwest` y `rustls`
//! para ir a buscar `$ref`s por red, que es exactamente lo que el protocolo prohíbe.
//!
//! El bundle resuelve además el `dataschema` exacto de cada subject: dentro de un mayor
//! todo es BACKWARD-compatible, así que el MINOR más alto acepta todo lo que aceptan los
//! anteriores — 05-compatibility.md.
//!
//! # La trampa de la versión del draft
//!
//! Los esquemas de flux declaran `$schema: https://json-schema.org/draft/2020-12/schema`.
//! Un validador configurado para draft-07 **no falla con un error de versión**: falla con
//! `no schema with key or ref ".../draft/2020-12/schema"`, que no dice nada útil y manda a
//! quien lo lee a buscar un fichero que no falta. Aquí el draft se detecta del propio
//! `$schema` y no se fuerza ninguno, que es la forma de que esa confusión no pueda ocurrir.

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;
use serde_json::Value;

use crate::client::{LogFn, LogLevel};
use crate::envelope::Event;
use crate::errors::{FluxError, SchemaNotFoundError, SchemaValidationError};

/// Qué hacer con un payload que no cumple su esquema — 00-protocol.md §5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValidationMode {
    /// **Default.** No se valida nada. Nivel L2, sin coste.
    #[default]
    Off,

    /// Se registra y se publica igual.
    ///
    /// No es un adorno: introducir validación en un ecosistema en marcha exige un periodo
    /// en el que se sabe qué se rompería antes de romperlo. Pasar directo a
    /// [`ValidationMode::Strict`] convierte en fallo de publicación todo payload que
    /// llevaba meses circulando mal sin que nadie lo supiera.
    Warn,

    /// `publish()` **falla**. Es lo que convierte un contrato roto en un fallo del
    /// productor y no en un misterio del consumidor.
    Strict,
}

/// Los esquemas del ecosistema, empaquetados y desplegados **con el servicio**.
///
/// Lo genera `scripts/bundle-schemas.mjs`; su forma en JSON es exactamente la de este
/// struct, así que se lee con [`SchemaBundle::from_json`] o [`SchemaBundle::from_file`].
///
/// Incluirlo en el binario con `include_str!` es lo más fiel al protocolo: el esquema
/// viaja con el artefacto, no con el entorno, y no hay forma de desplegar un servicio con
/// un bundle que no le corresponde.
///
/// ```no_run
/// # fn ejemplo() -> Result<(), flux::FluxError> {
/// let bundle = flux::validation::SchemaBundle::from_json(include_str!(
///     "../../schemas/bundle.json"
/// ))?;
/// # let _ = bundle;
/// # Ok(()) }
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SchemaBundle {
    /// subject → URI del esquema con el MINOR más alto de su mayor.
    #[serde(default)]
    pub subjects: HashMap<String, String>,

    /// URI (`$id`) → JSON Schema.
    #[serde(default)]
    pub schemas: HashMap<String, Value>,
}

impl SchemaBundle {
    /// Lee un bundle desde JSON.
    ///
    /// # Errores
    ///
    /// [`FluxError::Config`] si el JSON no tiene la forma del bundle. Es un error de
    /// **arranque** a propósito: un bundle ilegible descubierto con el primer evento sería
    /// un servicio que pasa el healthcheck y no puede publicar.
    pub fn from_json(json: &str) -> Result<Self, FluxError> {
        serde_json::from_str(json).map_err(|e| {
            FluxError::Config(format!(
                "el bundle de esquemas no se pudo leer: {e}. Se espera el JSON que genera \
                 `node scripts/bundle-schemas.mjs`: {{\"subjects\": {{…}}, \"schemas\": {{…}}}}"
            ))
        })
    }

    /// Lee un bundle desde un fichero.
    ///
    /// # Errores
    ///
    /// [`FluxError::Config`] si el fichero no se puede leer o no tiene la forma esperada.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, FluxError> {
        let path = path.as_ref();
        let json = std::fs::read_to_string(path).map_err(|e| {
            FluxError::Config(format!(
                "no se pudo leer el bundle de esquemas `{}`: {e}",
                path.display()
            ))
        })?;
        Self::from_json(&json)
    }

    /// El `dataschema` exacto de un subject, si el bundle lo conoce.
    #[must_use]
    pub fn uri_for(&self, subject: &str) -> Option<&str> {
        self.subjects.get(subject).map(String::as_str)
    }

    /// Cuántos esquemas trae. Útil para un log de arranque: un bundle vacío es un fallo de
    /// despliegue silencioso, porque `warn` no lo distinguiría de "todo válido".
    #[must_use]
    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    /// ¿Está vacío?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }
}

/// Configuración de la validación L3.
#[derive(Debug, Clone, Default)]
pub struct ValidationOptions {
    /// Qué hacer con un payload inválido. Default: [`ValidationMode::Off`].
    pub mode: ValidationMode,

    /// Los esquemas. Obligatorio si `mode` no es `Off`.
    pub bundle: Option<SchemaBundle>,

    /// Validar también **al consumir**.
    ///
    /// Un fallo ahí se clasifica **PERMANENT**: el evento es sintácticamente correcto pero
    /// incumple su contrato, y reintentarlo dará exactamente el mismo resultado
    /// — 04-errors.md §1.2.
    ///
    /// Default: `false`. Validar al consumir es defensa en profundidad y cuesta CPU en la
    /// ruta caliente de cada evento; el sitio donde un contrato roto se arregla es el
    /// productor.
    pub on_consume: bool,
}

impl ValidationOptions {
    /// Modo `strict` con el bundle dado: publicar un payload inválido falla.
    #[must_use]
    pub fn strict(bundle: SchemaBundle) -> Self {
        Self {
            mode: ValidationMode::Strict,
            bundle: Some(bundle),
            on_consume: false,
        }
    }

    /// Modo `warn` con el bundle dado: se registra y se publica igual.
    #[must_use]
    pub fn warn(bundle: SchemaBundle) -> Self {
        Self {
            mode: ValidationMode::Warn,
            bundle: Some(bundle),
            on_consume: false,
        }
    }

    /// Valida también al consumir. Un fallo se clasifica PERMANENT.
    #[must_use]
    pub fn with_on_consume(mut self, on_consume: bool) -> Self {
        self.on_consume = on_consume;
        self
    }
}

/// Los validadores del bundle, ya compilados.
///
/// Se construye **una vez en `connect()`** y nunca por evento: compilar un JSON Schema es
/// caro —se convierte en un árbol de nodos— y hacerlo en la ruta caliente tiraría el
/// throughput de publicación.
pub struct Validator {
    mode: ValidationMode,
    compiled: HashMap<String, jsonschema::Validator>,
    logger: Option<LogFn>,
}

impl std::fmt::Debug for Validator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Validator")
            .field("mode", &self.mode)
            .field("schemas", &self.compiled.len())
            .finish_non_exhaustive()
    }
}

impl Validator {
    /// Compila los esquemas del bundle. `Ok(None)` si el modo es `Off`.
    ///
    /// # Errores
    ///
    /// [`FluxError::Config`] si el modo no es `Off` y no hay bundle, o si alguno de los
    /// esquemas del bundle no compila. Las dos cosas son fallos de **arranque**: un bundle
    /// roto descubierto con el primer evento sería un servicio que pasa el healthcheck y
    /// no puede publicar nada.
    pub fn new(
        options: &ValidationOptions,
        logger: Option<LogFn>,
    ) -> Result<Option<Self>, FluxError> {
        if options.mode == ValidationMode::Off {
            return Ok(None);
        }

        let Some(bundle) = options.bundle.as_ref() else {
            return Err(FluxError::Config(
                "la validación L3 necesita un bundle de esquemas. Genéralo con \
                 `node scripts/bundle-schemas.mjs` y pásalo en \
                 `ConnectOptions::with_validation`: el `dataschema` NO se resuelve por red \
                 (00-protocol.md §5)"
                    .to_string(),
            ));
        };

        let mut compiled = HashMap::with_capacity(bundle.schemas.len());
        for (uri, schema) in &bundle.schemas {
            // Sin `with_draft`: el draft sale del `$schema` de cada esquema. Forzarlo aquí
            // es justo lo que produce el `no schema with key or ref …/2020-12/schema` que
            // no dice nada — 00-protocol.md §5.
            let validator = jsonschema::options().build(schema).map_err(|e| {
                FluxError::Config(format!(
                    "el esquema `{uri}` del bundle no compila: {e}. Regenera el bundle con \
                     `node scripts/bundle-schemas.mjs`"
                ))
            })?;
            compiled.insert(uri.clone(), validator);
        }

        Ok(Some(Self {
            mode: options.mode,
            compiled,
            logger,
        }))
    }

    /// Valida el payload del evento contra el esquema que su `dataschema` declara.
    ///
    /// # Errores
    ///
    /// En modo `strict`, [`FluxError::SchemaValidation`] si el payload incumple el esquema
    /// y [`FluxError::SchemaNotFound`] si el `dataschema` no está en el bundle. En modo
    /// `warn` ninguna de las dos: se registran y se devuelve `Ok`.
    pub fn validate(&self, event: &Event, subject: &str) -> Result<(), FluxError> {
        let Some(validator) = self.compiled.get(&event.dataschema) else {
            return self.report(FluxError::SchemaNotFound(SchemaNotFoundError {
                subject: subject.to_string(),
                dataschema: event.dataschema.clone(),
            }));
        };

        // El payload viaja como `RawValue` para conservar los bytes exactos, así que hay
        // que interpretarlo para validarlo. Es el coste real de L3 y por eso L3 es opt-in.
        let instance: Value = match serde_json::from_str(event.data.get()) {
            Ok(v) => v,
            Err(e) => {
                // Aquí no llega un payload no-JSON: `parse_event` ya lo habría rechazado
                // como POISON y `build_event` no lo habría construido. Si llegara, decirlo
                // como incumplimiento de esquema es más útil que un panic.
                return self.report(FluxError::SchemaValidation(SchemaValidationError {
                    subject: subject.to_string(),
                    dataschema: event.dataschema.clone(),
                    errors: vec![format!("(raíz) el payload no es JSON válido: {e}")],
                }));
            }
        };

        // `iter_errors` y NO `validate`: éste devuelve solo el primero. Reportar de uno en
        // uno convierte arreglar un payload con tres campos mal en tres despliegues, y por
        // eso 00-protocol.md §5 lo exige explícitamente.
        let errors: Vec<String> = validator
            .iter_errors(&instance)
            .map(|e| {
                let path = e.instance_path().to_string();
                let donde = if path.is_empty() { "(raíz)" } else { &path };
                format!("{donde} {e}")
            })
            .collect();

        if errors.is_empty() {
            return Ok(());
        }

        self.report(FluxError::SchemaValidation(SchemaValidationError {
            subject: subject.to_string(),
            dataschema: event.dataschema.clone(),
            errors,
        }))
    }

    /// En `strict` devuelve el error; en `warn` lo registra y sigue.
    fn report(&self, error: FluxError) -> Result<(), FluxError> {
        if self.mode == ValidationMode::Strict {
            return Err(error);
        }
        if let Some(log) = &self.logger {
            log(LogLevel::Warn, &format!("[flux] {error}"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{build_event, data_to_raw, BuildEventInput, DataClassification};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    const SUBJECT: &str = "pedidos.pedido.v1.creado";

    /// El bundle real del repositorio, no uno de juguete: si `bundle-schemas.mjs` cambia
    /// de forma, estos tests tienen que enterarse.
    fn bundle() -> SchemaBundle {
        SchemaBundle::from_json(include_str!("../../schemas/bundle.json")).expect("bundle válido")
    }

    fn uri() -> String {
        bundle()
            .uri_for(SUBJECT)
            .expect("el bundle debe resolver el subject de ejemplo")
            .to_string()
    }

    fn valido() -> Value {
        json!({
            "pedidoId": "ped-123",
            "clienteId": "cli-987",
            "aggregateVersion": 1,
            "totalCents": 9990,
            "moneda": "EUR",
            "lineas": [{ "sku": "ABC-1", "cantidad": 2, "precioUnitarioCents": 4995 }],
        })
    }

    fn evento(data: &Value) -> Event {
        build_event(BuildEventInput {
            subject: SUBJECT.to_string(),
            data: data_to_raw(data).expect("data serializable"),
            id: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55".to_string(),
            source: "/produccion/pedidos-api".to_string(),
            producerversion: "3.4.1".to_string(),
            tenantid: "acme".to_string(),
            dataclassification: DataClassification::Internal,
            dataschema: uri(),
            correlationid: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55".to_string(),
            time: None,
            aggregate_id: None,
            causationid: None,
            partitionkey: None,
            traceparent: None,
            tracestate: None,
        })
        .expect("envelope válido")
    }

    fn strict() -> Validator {
        Validator::new(&ValidationOptions::strict(bundle()), None)
            .expect("compila")
            .expect("modo strict devuelve validador")
    }

    /// Captura lo que el SDK registra, para poder afirmar que `warn` avisa **y** publica.
    fn logger_espia() -> (LogFn, Arc<Mutex<Vec<String>>>) {
        let avisos = Arc::new(Mutex::new(Vec::new()));
        let capturados = Arc::clone(&avisos);
        let log: LogFn = Arc::new(move |_level, msg: &str| {
            capturados
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(msg.to_string());
        });
        (log, avisos)
    }

    // ── el bundle ─────────────────────────────────────────────────────────────

    #[test]
    fn el_bundle_indexa_el_subject_hacia_su_uri_de_dataschema() {
        let uri = uri();
        assert!(
            uri.starts_with("https://schemas.internal/pedidos/pedido/creado/"),
            "{uri}"
        );
        // El fichero es `<major>.<minor>.<patch>.json`: la URI del esquema lleva la versión
        // SemVer, y es de ahí de donde sale la promesa de que el MINOR más alto acepta lo
        // que aceptan los anteriores — 05-compatibility.md.
        let fichero = uri.rsplit('/').next().unwrap_or_default();
        let partes: Vec<&str> = fichero.split('.').collect();
        assert_eq!(partes.len(), 4, "{uri}");
        assert_eq!(partes[3], "json", "{uri}");
        assert!(
            partes[..3]
                .iter()
                .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit())),
            "{uri}"
        );
    }

    /// La clave del bundle y el `$id` del esquema tienen que ser el mismo texto: el
    /// validador se busca por el `dataschema` del evento, que es esa clave.
    #[test]
    fn el_id_del_esquema_coincide_con_la_clave_del_bundle() {
        let b = bundle();
        let uri = uri();
        assert_eq!(b.schemas[&uri]["$id"], json!(uri));
    }

    // ── strict ────────────────────────────────────────────────────────────────

    #[test]
    fn un_payload_valido_pasa() {
        assert!(strict().validate(&evento(&valido()), SUBJECT).is_ok());
    }

    #[test]
    fn falta_un_campo_requerido_y_falla() {
        let mut data = valido();
        data.as_object_mut().expect("objeto").remove("totalCents");
        let err = strict().validate(&evento(&data), SUBJECT).unwrap_err();
        assert!(matches!(err, FluxError::SchemaValidation(_)), "{err:?}");
    }

    /// El caso que la spec llama el más peligroso: `"9990"` en vez de `9990`. Pasa
    /// cualquier revisión visual y rompe toda aritmética aguas abajo.
    #[test]
    fn un_tipo_incorrecto_falla() {
        let mut data = valido();
        data["totalCents"] = json!("9990");
        let err = strict().validate(&evento(&data), SUBJECT).unwrap_err();
        assert!(matches!(err, FluxError::SchemaValidation(_)), "{err:?}");
    }

    /// `additionalProperties: false`: un campo mal escrito debe fallar, no colarse en
    /// silencio y llegar como `undefined` al consumidor.
    #[test]
    fn un_campo_desconocido_falla() {
        let mut data = valido();
        data["totalCemts"] = json!(9990);
        let err = strict().validate(&evento(&data), SUBJECT).unwrap_err();
        assert!(matches!(err, FluxError::SchemaValidation(_)), "{err:?}");
    }

    #[test]
    fn un_patron_incumplido_falla() {
        let mut data = valido();
        data["moneda"] = json!("euros");
        let err = strict().validate(&evento(&data), SUBJECT).unwrap_err();
        assert!(matches!(err, FluxError::SchemaValidation(_)), "{err:?}");
    }

    /// La exigencia explícita de 00-protocol.md §5: de uno en uno, arreglar un payload con
    /// tres campos mal cuesta tres despliegues.
    #[test]
    fn reporta_todos_los_errores_no_solo_el_primero() {
        let mut data = valido();
        data["totalCents"] = json!("x");
        data["moneda"] = json!("euros");
        data["cantidad"] = json!(1);

        let err = strict().validate(&evento(&data), SUBJECT).unwrap_err();
        let FluxError::SchemaValidation(detalle) = err else {
            panic!("se esperaba SchemaValidation, llegó {err:?}");
        };
        assert!(
            detalle.errors.len() >= 2,
            "esperaba ≥2 errores, hubo {}: {:?}",
            detalle.errors.len(),
            detalle.errors
        );
        // Y el mensaje los lleva todos, que es lo que acaba leyendo una persona.
        let texto = FluxError::SchemaValidation(detalle).to_string();
        assert!(
            texto.contains("moneda") || texto.contains("euros"),
            "{texto}"
        );
    }

    #[test]
    fn un_esquema_ausente_del_bundle_falla_distinto() {
        let mut e = evento(&valido());
        e.dataschema = "https://schemas.internal/no/existe/1.0.0.json".to_string();
        let err = strict().validate(&e, SUBJECT).unwrap_err();
        assert!(matches!(err, FluxError::SchemaNotFound(_)), "{err:?}");
        // El mensaje tiene que decir qué hacer, no solo qué pasó.
        assert!(err.to_string().contains("bundle-schemas.mjs"), "{err}");
    }

    // ── warn y off ────────────────────────────────────────────────────────────

    #[test]
    fn warn_registra_pero_no_falla() {
        let (log, avisos) = logger_espia();
        let v = Validator::new(&ValidationOptions::warn(bundle()), Some(log))
            .expect("compila")
            .expect("modo warn devuelve validador");

        let mut data = valido();
        data["totalCents"] = json!("x");
        assert!(v.validate(&evento(&data), SUBJECT).is_ok());

        let avisos = avisos
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(avisos.len(), 1, "{avisos:?}");
        assert!(avisos[0].contains("no cumple su esquema"), "{avisos:?}");
    }

    /// L2 no paga el coste de L3: en `off` no se compila absolutamente nada.
    #[test]
    fn off_no_compila_nada() {
        let sin_nada = Validator::new(&ValidationOptions::default(), None).expect("no falla");
        assert!(sin_nada.is_none());

        let con_bundle = Validator::new(
            &ValidationOptions {
                mode: ValidationMode::Off,
                bundle: Some(bundle()),
                on_consume: false,
            },
            None,
        )
        .expect("no falla");
        assert!(con_bundle.is_none());
    }

    /// Un modo distinto de `off` sin bundle es un error de configuración, y falla al
    /// arrancar: si esperase al primer evento, el servicio pasaría el healthcheck.
    #[test]
    fn strict_sin_bundle_falla_con_un_mensaje_accionable() {
        let err = Validator::new(
            &ValidationOptions {
                mode: ValidationMode::Strict,
                bundle: None,
                on_consume: false,
            },
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("bundle-schemas.mjs"), "{err}");
    }

    #[test]
    fn un_bundle_ilegible_falla_al_leerlo() {
        let err = SchemaBundle::from_json("{").unwrap_err();
        assert!(matches!(err, FluxError::Config(_)), "{err:?}");
    }

    /// El bundle es dato, no red: si el esquema declara un `$ref` remoto, compilarlo falla
    /// al arrancar en vez de intentar una petición HTTP en la ruta caliente.
    #[test]
    fn un_ref_remoto_no_se_resuelve_por_red() {
        let bundle = SchemaBundle {
            subjects: HashMap::new(),
            schemas: HashMap::from([(
                "https://schemas.internal/x/1.0.0.json".to_string(),
                json!({
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "$ref": "https://ejemplo.invalido/otro.json",
                }),
            )]),
        };
        let err = Validator::new(&ValidationOptions::strict(bundle), None).unwrap_err();
        assert!(matches!(err, FluxError::Config(_)), "{err:?}");
    }

    /// La trampa del draft: el esquema declara 2020-12 y el validador **debe** honrarlo.
    /// Con un validador de draft-07, `prefixItems` se ignoraría en silencio y un payload
    /// inválido pasaría.
    #[test]
    fn el_draft_2020_12_se_detecta_del_propio_esquema() {
        let uri = "https://schemas.internal/x/1.0.0.json".to_string();
        let bundle = SchemaBundle {
            subjects: HashMap::from([(SUBJECT.to_string(), uri.clone())]),
            schemas: HashMap::from([(
                uri.clone(),
                json!({
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "$id": uri,
                    "type": "object",
                    "properties": { "lista": { "prefixItems": [{ "type": "integer" }] } },
                }),
            )]),
        };
        let v = Validator::new(&ValidationOptions::strict(bundle), None)
            .expect("compila")
            .expect("strict devuelve validador");

        let mut e = evento(&json!({ "lista": ["no es un entero"] }));
        e.dataschema = uri;
        let err = v.validate(&e, SUBJECT).unwrap_err();
        assert!(matches!(err, FluxError::SchemaValidation(_)), "{err:?}");
    }
}
