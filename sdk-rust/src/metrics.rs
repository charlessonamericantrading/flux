//! Métricas del SDK.
//!
//! Contrato normativo: `specification/08-observability.md`
//!
//! Los nombres y las etiquetas **son parte del contrato**, no una decisión de cada SDK.
//! Si el de Rust y el de Go nombran distinto la tasa de DLQ, la de los servicios Rust no
//! se puede sumar con la de los de Go y un panel del ecosistema es imposible. Es el mismo
//! argumento que los códigos POISON de 01-envelope.md §3.1: **si dos SDKs emiten nombres
//! distintos para lo mismo, agrupar deja de funcionar en cuanto el ecosistema es polyglot
//! — que es siempre.**
//!
//! Por eso viven aquí y no en la aplicación. Lo que la aplicación elige es el *backend*:
//! implementa [`MetricsSink`] contra `prometheus`, `metrics` u OpenTelemetry y recibe los
//! mismos nombres. El default es [`NoMetrics`]: un SDK no debe imponer un backend.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;

/// Buckets del histograma de duración, en segundos — 08-observability.md §3.
///
/// El último es `30` **a propósito: es el `ack_wait`** (03-delivery.md §2). Un handler que
/// cae en el bucket superior está a punto de que su mensaje se reentregue mientras aún se
/// ejecuta, así que `flux_event_handler_duration_seconds_bucket{le="30"}` frente al total
/// mide directamente cuántos eventos rozan la ejecución concurrente.
///
/// **Este bucket DEBE moverse si se cambia `ack_wait`.** Un bucket que no coincide con el
/// plazo real mide algo que no le importa a nadie. El test
/// `el_ultimo_bucket_es_el_ack_wait` fija la invariante contra
/// [`crate::protocol::DEFAULT_ACK_WAIT`].
pub const DURATION_BUCKETS: [f64; 12] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Valor de la etiqueta `outcome` de `flux_events_published_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PublishOutcome {
    /// El stream confirmó la publicación.
    Ok,
    /// El payload no cumple su JSON Schema (L3). El SDK de Rust es L2 y no lo emite
    /// todavía; el valor existe porque la etiqueta es del protocolo, no del SDK.
    InvalidSchema,
    /// El broker rechazó la publicación o no confirmó.
    Error,
}

/// Valor de la etiqueta `outcome` de `flux_events_consumed_total` — 08-observability.md §2.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConsumeOutcome {
    /// El handler devolvió sin fallar.
    Ok,
    /// Fallo transitorio: se reintentará, o agotó los reintentos.
    Retryable,
    /// El consumidor lo rechazó de forma definitiva.
    Permanent,
    /// El mensaje no era interpretable.
    Poison,
    /// El payload no cumple su JSON Schema.
    InvalidSchema,
    /// Falta la firma, no verifica, o el `signkeyid` es desconocido — 07-signing.md §7.
    InvalidSignature,
}

/// Motivo por el que un evento acabó en la DLQ. Etiqueta `reason` — 04-errors.md §1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DlqReasonLabel {
    /// Agotó los reintentos.
    Retryable,
    /// Rechazo definitivo del consumidor.
    Permanent,
    /// Mensaje ininterpretable.
    Poison,
}

/// Valor de `flux_connection_state` — 08-observability.md §2.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    /// `0`
    Disconnected,
    /// `1`
    Connected,
    /// `2`
    Reconnecting,
}

impl PublishOutcome {
    /// El literal que va en la etiqueta.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::InvalidSchema => "invalid_schema",
            Self::Error => "error",
        }
    }
}

impl ConsumeOutcome {
    /// El literal que va en la etiqueta.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
            Self::Poison => "poison",
            Self::InvalidSchema => "invalid_schema",
            Self::InvalidSignature => "invalid_signature",
        }
    }
}

impl DlqReasonLabel {
    /// El literal que va en la etiqueta.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
            Self::Poison => "poison",
        }
    }
}

impl From<crate::envelope::DlqReason> for DlqReasonLabel {
    fn from(r: crate::envelope::DlqReason) -> Self {
        match r {
            crate::envelope::DlqReason::Retryable => Self::Retryable,
            crate::envelope::DlqReason::Permanent => Self::Permanent,
            crate::envelope::DlqReason::Poison => Self::Poison,
        }
    }
}

impl ConnectionState {
    /// El número que expone el gauge.
    #[must_use]
    pub const fn as_f64(self) -> f64 {
        match self {
            Self::Disconnected => 0.0,
            Self::Connected => 1.0,
            Self::Reconnecting => 2.0,
        }
    }
}

/// Destino de las métricas. Impleméntalo para enchufar el backend que uses.
///
/// ⚠️ **Un método por métrica, con parámetros propios — no un `labels: HashMap` genérico.**
/// No es estilo: un mapa de etiquetas es exactamente el agujero por el que se cuela un
/// `tenantid` que multiplica las series temporales. Con esta forma, etiquetar por tenant
/// exige cambiar la firma del trait, y eso se ve en una revisión. La cardinalidad **no
/// avisa**: el sistema funciona en desarrollo con tres tenants y muere en producción con
/// diez mil, y el fallo se manifiesta como "Prometheus se ha quedado sin memoria", no como
/// "alguien etiquetó por tenant" — 08-observability.md §2.2.
///
/// Por la misma razón `code` es un identificador estable (`HTTP_503`,
/// `PEDIDO_YA_CANCELADO`) y **nunca** el mensaje de error: un mensaje lleva ids,
/// timestamps y rutas, y su cardinalidad es infinita.
pub trait MetricsSink: Send + Sync {
    /// `flux_events_published_total{subject, outcome}`
    fn event_published(&self, subject: &str, outcome: PublishOutcome);

    /// `flux_events_consumed_total{subject, consumer, outcome}`
    fn event_consumed(&self, subject: &str, consumer: &str, outcome: ConsumeOutcome);

    /// `flux_event_handler_duration_seconds{subject, consumer}`
    fn handler_duration(&self, subject: &str, consumer: &str, seconds: f64);

    /// `flux_events_dlq_total{subject, consumer, reason, code}`
    fn event_dlq(&self, subject: &str, consumer: &str, reason: DlqReasonLabel, code: &str);

    /// `flux_events_retried_total{subject, consumer, attempt}`
    fn event_retried(&self, subject: &str, consumer: &str, attempt: u32);

    /// `flux_consumer_pending{subject, consumer}`
    ///
    /// Es la métrica que delata un consumidor cuyo bucle murió: **sigue reportando la
    /// conexión como sana** y solo el crecimiento de `pending` lo evidencia
    /// — 08-observability.md §4.
    fn consumer_pending(&self, subject: &str, consumer: &str, pending: u64);

    /// `flux_connection_state` — sin etiquetas.
    fn connection_state(&self, state: ConnectionState);
}

/// No-op. **El default**: un SDK no debe imponer un backend de métricas.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoMetrics;

impl MetricsSink for NoMetrics {
    fn event_published(&self, _subject: &str, _outcome: PublishOutcome) {}
    fn event_consumed(&self, _subject: &str, _consumer: &str, _outcome: ConsumeOutcome) {}
    fn handler_duration(&self, _subject: &str, _consumer: &str, _seconds: f64) {}
    fn event_dlq(&self, _subject: &str, _consumer: &str, _reason: DlqReasonLabel, _code: &str) {}
    fn event_retried(&self, _subject: &str, _consumer: &str, _attempt: u32) {}
    fn consumer_pending(&self, _subject: &str, _consumer: &str, _pending: u64) {}
    fn connection_state(&self, _state: ConnectionState) {}
}

// ─── Recolector en memoria ───────────────────────────────────────────────────

/// Un histograma: cuentas acumuladas por bucket, suma y total.
#[derive(Debug, Clone)]
struct Histogram {
    buckets: [u64; DURATION_BUCKETS.len()],
    sum: f64,
    count: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: [0; DURATION_BUCKETS.len()],
            sum: 0.0,
            count: 0,
        }
    }
}

#[derive(Debug, Default)]
struct Store {
    // BTreeMap y no HashMap: la salida sale ordenada sin ordenarla, y un `/metrics`
    // estable entre scrapes es mucho más fácil de diffear cuando algo va mal.
    counters: BTreeMap<String, u64>,
    gauges: BTreeMap<String, f64>,
    histograms: BTreeMap<String, Histogram>,
}

/// Recolector **sin dependencias** que expone el formato de texto de Prometheus.
///
/// Suficiente para servir un `/metrics` real. Si ya usas un cliente de Prometheus,
/// implementa [`MetricsSink`] contra él en vez de esto — lo que importa es conservar los
/// nombres y las etiquetas.
#[derive(Debug, Default)]
pub struct InMemoryMetrics {
    // Un Mutex y no atómicos por serie: las métricas están fuera de la ruta caliente
    // (una toma por evento, frente a una publicación por red) y la simplicidad vale más
    // aquí que los nanosegundos.
    store: Mutex<Store>,
}

/// Toma el mutex tolerando el envenenamiento: la sección crítica es un `insert` en un
/// mapa y no puede dejar el estado a medias, así que recuperar el valor es siempre
/// correcto y evita que un pánico en otro sitio del proceso tumbe las métricas.
fn lock(m: &Mutex<Store>) -> std::sync::MutexGuard<'_, Store> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Escapa un valor de etiqueta según el formato de exposición de Prometheus.
///
/// No es cosmético: un `code` con una comilla rompería la línea y **Prometheus descarta
/// el scrape entero, no solo esa línea**. Los `code` salen de mensajes de error de
/// terceros, así que la comilla llega antes o después.
fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// `nombre{k="v",…}` con las etiquetas en el orden dado.
///
/// El orden lo fija el llamante y es siempre el mismo para una métrica dada, así que la
/// clave de una serie temporal es estable. Sin esa estabilidad, la misma serie aparecería
/// bajo dos claves según el orden en que se construyó el mapa.
fn key(name: &str, labels: &[(&str, &str)]) -> String {
    if labels.is_empty() {
        return name.to_string();
    }
    let mut s = String::with_capacity(name.len() + labels.len() * 16);
    s.push_str(name);
    s.push('{');
    for (i, (k, v)) in labels.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(s, "{k}=\"{}\"", escape(v));
    }
    s.push('}');
    s
}

impl InMemoryMetrics {
    /// Un recolector vacío.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn inc(&self, name: &str, labels: &[(&str, &str)]) {
        *lock(&self.store)
            .counters
            .entry(key(name, labels))
            .or_insert(0) += 1;
    }

    fn set(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        lock(&self.store).gauges.insert(key(name, labels), value);
    }

    fn observe(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        let mut store = lock(&self.store);
        let h = store.histograms.entry(key(name, labels)).or_default();
        h.sum += value;
        h.count += 1;
        for (i, limit) in DURATION_BUCKETS.iter().enumerate() {
            if value <= *limit {
                h.buckets[i] += 1;
            }
        }
    }

    /// Formato de exposición de Prometheus. Sírvelo tal cual en `/metrics`.
    ///
    /// Los buckets son **acumulativos** (`le` = "less or equal"), que es lo que
    /// `histogram_quantile` espera; emitir cuentas por bucket en vez de acumuladas daría
    /// cuantiles silenciosamente equivocados.
    #[must_use]
    pub fn render(&self) -> String {
        let store = lock(&self.store);
        let mut out = String::new();

        for name in [
            "flux_events_published_total",
            "flux_events_consumed_total",
            "flux_events_dlq_total",
            "flux_events_retried_total",
        ] {
            let _ = writeln!(out, "# TYPE {name} counter");
        }
        for (k, v) in &store.counters {
            let _ = writeln!(out, "{k} {v}");
        }

        for name in ["flux_consumer_pending", "flux_connection_state"] {
            let _ = writeln!(out, "# TYPE {name} gauge");
        }
        for (k, v) in &store.gauges {
            let _ = writeln!(out, "{k} {v}");
        }

        let _ = writeln!(out, "# TYPE flux_event_handler_duration_seconds histogram");
        for (k, h) in &store.histograms {
            let (base, labels) = match k.find('{') {
                Some(i) => (&k[..i], &k[i + 1..k.len() - 1]),
                None => (k.as_str(), ""),
            };
            let sep = if labels.is_empty() { "" } else { "," };
            for (i, limit) in DURATION_BUCKETS.iter().enumerate() {
                let _ = writeln!(
                    out,
                    "{base}_bucket{{{labels}{sep}le=\"{}\"}} {}",
                    format_bucket(*limit),
                    h.buckets[i]
                );
            }
            let _ = writeln!(out, "{base}_bucket{{{labels}{sep}le=\"+Inf\"}} {}", h.count);
            let _ = writeln!(out, "{base}_sum{{{labels}}} {}", h.sum);
            let _ = writeln!(out, "{base}_count{{{labels}}} {}", h.count);
        }

        out
    }

    /// El valor de un contador por su clave completa. Solo para tests.
    #[must_use]
    pub fn counter(&self, key: &str) -> Option<u64> {
        lock(&self.store).counters.get(key).copied()
    }

    /// El valor de un gauge por su clave completa. Solo para tests.
    #[must_use]
    pub fn gauge(&self, key: &str) -> Option<f64> {
        lock(&self.store).gauges.get(key).copied()
    }
}

/// `1.0` se imprime como `1`, no como `1.0`.
///
/// El `le` de un bucket es texto para Prometheus y **la etiqueta `le="1"` y la `le="1.0"`
/// son dos series distintas**. Los buckets de la spec se escriben `1`, `5`, `10`, `30`, y
/// esos son los que tienen que salir para que el dashboard del ecosistema agrupe.
fn format_bucket(v: f64) -> String {
    // Los buckets están entre 0.005 y 30, así que el truncado no puede perder nada; la
    // conversión existe solo para elegir el formato sin decimales.
    #[allow(clippy::cast_possible_truncation)]
    if v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

impl MetricsSink for InMemoryMetrics {
    fn event_published(&self, subject: &str, outcome: PublishOutcome) {
        self.inc(
            "flux_events_published_total",
            &[("subject", subject), ("outcome", outcome.as_str())],
        );
    }

    fn event_consumed(&self, subject: &str, consumer: &str, outcome: ConsumeOutcome) {
        self.inc(
            "flux_events_consumed_total",
            &[
                ("subject", subject),
                ("consumer", consumer),
                ("outcome", outcome.as_str()),
            ],
        );
    }

    fn handler_duration(&self, subject: &str, consumer: &str, seconds: f64) {
        self.observe(
            "flux_event_handler_duration_seconds",
            &[("subject", subject), ("consumer", consumer)],
            seconds,
        );
    }

    fn event_dlq(&self, subject: &str, consumer: &str, reason: DlqReasonLabel, code: &str) {
        self.inc(
            "flux_events_dlq_total",
            &[
                ("subject", subject),
                ("consumer", consumer),
                ("reason", reason.as_str()),
                ("code", code),
            ],
        );
    }

    fn event_retried(&self, subject: &str, consumer: &str, attempt: u32) {
        self.inc(
            "flux_events_retried_total",
            &[
                ("subject", subject),
                ("consumer", consumer),
                ("attempt", &attempt.to_string()),
            ],
        );
    }

    fn consumer_pending(&self, subject: &str, consumer: &str, pending: u64) {
        #[allow(clippy::cast_precision_loss)]
        self.set(
            "flux_consumer_pending",
            &[("subject", subject), ("consumer", consumer)],
            pending as f64,
        );
    }

    fn connection_state(&self, state: ConnectionState) {
        self.set("flux_connection_state", &[], state.as_f64());
    }
}

#[cfg(test)]
// Los buckets y los estados de conexión son literales EXACTOS del protocolo, no
// resultados de un cálculo: compararlos con `==` es justamente lo que se quiere: si
// alguien escribe `0.0051`, el test tiene que caer.
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;
    use crate::protocol::DEFAULT_ACK_WAIT;

    const SUBJECT: &str = "pedidos.pedido.v1.creado";
    const CONSUMER: &str = "facturacion-api__pedidos_pedido_v1_creado";

    // ── buckets — 08-observability.md §3 ──────────────────────────────────────

    /// La invariante que da sentido al histograma entero.
    #[test]
    fn el_ultimo_bucket_es_el_ack_wait() {
        // Un handler en el bucket superior está a punto de que su mensaje se reentregue
        // mientras aún se ejecuta. Si el bucket no coincide con el plazo real, mide algo
        // que no le importa a nadie — y esa desincronización solo se detecta aquí.
        let ultimo = DURATION_BUCKETS[DURATION_BUCKETS.len() - 1];
        assert_eq!(ultimo, DEFAULT_ACK_WAIT.as_secs_f64());
    }

    #[test]
    fn los_buckets_son_los_de_la_spec_y_ascienden() {
        assert_eq!(
            DURATION_BUCKETS,
            [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
        );
        for w in DURATION_BUCKETS.windows(2) {
            assert!(w[1] > w[0], "los buckets deben ascender: {w:?}");
        }
    }

    // ── recolector ────────────────────────────────────────────────────────────

    #[test]
    fn cuenta_publicaciones_por_subject_y_resultado() {
        let m = InMemoryMetrics::new();
        m.event_published(SUBJECT, PublishOutcome::Ok);
        m.event_published(SUBJECT, PublishOutcome::Ok);
        m.event_published(SUBJECT, PublishOutcome::Error);

        assert_eq!(
            m.counter(&format!(
                "flux_events_published_total{{subject=\"{SUBJECT}\",outcome=\"ok\"}}"
            )),
            Some(2)
        );
        assert_eq!(
            m.counter(&format!(
                "flux_events_published_total{{subject=\"{SUBJECT}\",outcome=\"error\"}}"
            )),
            Some(1)
        );
    }

    #[test]
    fn la_clave_de_una_serie_es_estable_entre_recolectores() {
        // Sin orden estable, la misma serie temporal aparecería con dos claves según el
        // orden en que se construyeron las etiquetas.
        let a = InMemoryMetrics::new();
        let b = InMemoryMetrics::new();
        a.event_dlq(SUBJECT, CONSUMER, DlqReasonLabel::Permanent, "HTTP_404");
        b.event_dlq(SUBJECT, CONSUMER, DlqReasonLabel::Permanent, "HTTP_404");
        assert_eq!(a.render(), b.render());
    }

    #[test]
    fn el_histograma_acumula_en_todos_los_buckets_que_superan_el_valor() {
        let m = InMemoryMetrics::new();
        m.handler_duration(SUBJECT, CONSUMER, 0.03); // cae por encima de 0.025
        let salida = m.render();

        assert!(salida.contains("le=\"0.025\"} 0"), "{salida}");
        assert!(salida.contains("le=\"0.05\"} 1"), "{salida}");
        assert!(salida.contains("le=\"+Inf\"} 1"), "{salida}");
        assert!(salida.contains("_count{"), "{salida}");
    }

    /// `le="30"`, no `le="30.0"`: son dos series distintas para Prometheus y el dashboard
    /// del ecosistema agrupa por la primera.
    #[test]
    fn el_bucket_entero_se_imprime_sin_decimales() {
        let m = InMemoryMetrics::new();
        m.handler_duration(SUBJECT, CONSUMER, 0.4);
        let salida = m.render();
        assert!(salida.contains("le=\"30\"}"), "{salida}");
        assert!(!salida.contains("le=\"30.0\"}"), "{salida}");
        assert!(salida.contains("le=\"0.005\"}"), "{salida}");
    }

    #[test]
    fn un_gauge_sin_etiquetas_no_deja_llaves_vacias() {
        let m = InMemoryMetrics::new();
        m.connection_state(ConnectionState::Connected);
        assert!(
            m.render().lines().any(|l| l == "flux_connection_state 1"),
            "{}",
            m.render()
        );
    }

    #[test]
    fn los_tres_estados_de_conexion_son_los_de_la_spec() {
        assert_eq!(ConnectionState::Disconnected.as_f64(), 0.0);
        assert_eq!(ConnectionState::Connected.as_f64(), 1.0);
        assert_eq!(ConnectionState::Reconnecting.as_f64(), 2.0);
    }

    /// Cada línea del formato de exposición, una a una.
    #[test]
    fn las_lineas_tienen_forma_valida_de_prometheus() {
        let m = InMemoryMetrics::new();
        m.event_published(SUBJECT, PublishOutcome::Ok);
        m.event_consumed(SUBJECT, CONSUMER, ConsumeOutcome::Ok);
        m.event_dlq(SUBJECT, CONSUMER, DlqReasonLabel::Permanent, "HTTP_404");
        m.event_retried(SUBJECT, CONSUMER, 3);
        m.consumer_pending(SUBJECT, CONSUMER, 42);
        m.connection_state(ConnectionState::Connected);
        m.handler_duration(SUBJECT, CONSUMER, 0.4);

        for linea in m.render().lines() {
            if linea.is_empty() || linea.starts_with('#') {
                continue;
            }
            let (nombre_y_etiquetas, valor) = linea
                .rsplit_once(' ')
                .unwrap_or_else(|| panic!("línea sin valor: {linea:?}"));
            assert!(
                valor.parse::<f64>().is_ok(),
                "valor no numérico en {linea:?}"
            );
            let nombre = nombre_y_etiquetas
                .split_once('{')
                .map_or(nombre_y_etiquetas, |(n, _)| n);
            assert!(
                nombre.starts_with("flux_")
                    && nombre
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c == '_' || c.is_ascii_digit()),
                "nombre de métrica no válido en {linea:?}"
            );
            if let Some((_, etiquetas)) = nombre_y_etiquetas.split_once('{') {
                assert!(etiquetas.ends_with('}'), "llaves sin cerrar: {linea:?}");
            }
        }
    }

    /// Un `code` con comillas rompería el formato y Prometheus descartaría **el scrape
    /// entero**, no solo esa línea.
    #[test]
    fn escapa_las_comillas_de_los_valores_de_etiqueta() {
        let m = InMemoryMetrics::new();
        m.event_dlq(
            SUBJECT,
            CONSUMER,
            DlqReasonLabel::Permanent,
            "con \"comillas\"",
        );

        let linea = m
            .render()
            .lines()
            .find(|l| l.starts_with("flux_events_dlq_total"))
            .expect("falta la línea de DLQ")
            .to_string();

        assert!(linea.contains("code=\"con \\\"comillas\\\"\""), "{linea}");
        // Las comillas sin escapar quedan balanceadas: 2 por etiqueta, 4 etiquetas.
        let sin_escapar = linea
            .as_bytes()
            .windows(2)
            .filter(|w| w[1] == b'"' && w[0] != b'\\')
            .count()
            + usize::from(linea.starts_with('"'));
        assert_eq!(sin_escapar % 2, 0, "comillas desbalanceadas: {linea}");
    }

    #[test]
    fn escapa_barras_y_saltos_de_linea() {
        assert_eq!(escape("a\\b"), "a\\\\b");
        assert_eq!(escape("a\nb"), "a\\nb");
    }

    /// La etiqueta `attempt` es 1..max_deliver y va como texto, no como número.
    #[test]
    fn el_intento_se_etiqueta_como_texto() {
        let m = InMemoryMetrics::new();
        m.event_retried(SUBJECT, CONSUMER, 3);
        assert_eq!(
            m.counter(&format!(
                "flux_events_retried_total{{subject=\"{SUBJECT}\",consumer=\"{CONSUMER}\",attempt=\"3\"}}"
            )),
            Some(1)
        );
    }

    /// 08-observability.md §2.2: **NUNCA** se etiqueta con `tenantid`, `id` ni
    /// `correlationid`. Aquí no puede pasar porque no hay dónde meterlos —las firmas del
    /// trait no lo permiten—, pero el test lo fija sobre la salida por si alguien añade
    /// una etiqueta de más.
    #[test]
    fn ninguna_etiqueta_prohibida_llega_a_la_salida() {
        let m = InMemoryMetrics::new();
        m.event_published(SUBJECT, PublishOutcome::Ok);
        m.event_consumed(SUBJECT, CONSUMER, ConsumeOutcome::Ok);
        m.event_dlq(SUBJECT, CONSUMER, DlqReasonLabel::Poison, "MALFORMED_JSON");
        m.event_retried(SUBJECT, CONSUMER, 1);
        m.consumer_pending(SUBJECT, CONSUMER, 7);
        m.handler_duration(SUBJECT, CONSUMER, 1.0);
        m.connection_state(ConnectionState::Reconnecting);

        let salida = m.render();
        for prohibida in ["tenantid=", "id=", "correlationid="] {
            assert!(
                !salida.contains(prohibida),
                "etiqueta prohibida {prohibida} en la salida:\n{salida}"
            );
        }
    }

    #[test]
    fn el_gauge_de_pendientes_se_sobrescribe_no_se_acumula() {
        let m = InMemoryMetrics::new();
        m.consumer_pending(SUBJECT, CONSUMER, 10);
        m.consumer_pending(SUBJECT, CONSUMER, 3);
        assert_eq!(
            m.gauge(&format!(
                "flux_consumer_pending{{subject=\"{SUBJECT}\",consumer=\"{CONSUMER}\"}}"
            )),
            Some(3.0)
        );
    }

    #[test]
    fn no_metrics_no_guarda_nada_y_no_lanza() {
        // Un SDK no debe imponer un backend de métricas: el default no hace nada.
        let m = NoMetrics;
        m.event_published(SUBJECT, PublishOutcome::Ok);
        m.event_consumed(SUBJECT, CONSUMER, ConsumeOutcome::Poison);
        m.event_dlq(SUBJECT, CONSUMER, DlqReasonLabel::Poison, "X");
        m.event_retried(SUBJECT, CONSUMER, 1);
        m.consumer_pending(SUBJECT, CONSUMER, 0);
        m.handler_duration(SUBJECT, CONSUMER, 0.1);
        m.connection_state(ConnectionState::Disconnected);
    }

    #[test]
    fn los_literales_de_las_etiquetas_son_los_de_la_spec() {
        assert_eq!(
            [
                ConsumeOutcome::Ok.as_str(),
                ConsumeOutcome::Retryable.as_str(),
                ConsumeOutcome::Permanent.as_str(),
                ConsumeOutcome::Poison.as_str(),
                ConsumeOutcome::InvalidSchema.as_str(),
                ConsumeOutcome::InvalidSignature.as_str(),
            ],
            [
                "ok",
                "retryable",
                "permanent",
                "poison",
                "invalid_schema",
                "invalid_signature"
            ]
        );
        assert_eq!(
            [
                DlqReasonLabel::Retryable.as_str(),
                DlqReasonLabel::Permanent.as_str(),
                DlqReasonLabel::Poison.as_str()
            ],
            ["retryable", "permanent", "poison"]
        );
        assert_eq!(PublishOutcome::InvalidSchema.as_str(), "invalid_schema");
    }
}
