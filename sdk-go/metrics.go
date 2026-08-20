// Métricas del SDK.
// Contrato normativo: specification/08-observability.md
//
// Los nombres y las etiquetas son parte del CONTRATO, no una decisión de cada SDK: si el
// de Go y el de Node nombran distinto la tasa de DLQ, un panel del ecosistema es
// imposible. Por eso viven aquí y no en la aplicación.
//
// La implementación es un recolector en memoria SIN dependencias. Quien use
// prometheus/client_golang, OpenTelemetry o lo que sea implementa MetricsSink contra su
// backend y conserva los mismos nombres — que es lo único que el protocolo exige.

package flux

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Nombres de las siete métricas obligatorias — 08-observability.md §2.
//
// Son constantes y no literales sueltos porque un typo en una de ellas no rompe nada
// visible: el SDK sigue funcionando y el panel se queda vacío.
const (
	MetricEventsPublished = "flux_events_published_total"
	MetricEventsConsumed  = "flux_events_consumed_total"
	MetricHandlerDuration = "flux_event_handler_duration_seconds"
	MetricEventsDLQ       = "flux_events_dlq_total"
	MetricEventsRetried   = "flux_events_retried_total"
	MetricConsumerPending = "flux_consumer_pending"
	MetricConnectionState = "flux_connection_state"
)

// PublishOutcome es el valor de la etiqueta `outcome` al publicar.
type PublishOutcome string

const (
	PublishOK            PublishOutcome = "ok"
	PublishInvalidSchema PublishOutcome = "invalid_schema"
	PublishError         PublishOutcome = "error"
)

// ConsumeOutcome es el valor de la etiqueta `outcome` al consumir — §2.1.
type ConsumeOutcome string

const (
	ConsumeOK               ConsumeOutcome = "ok"
	ConsumeRetryable        ConsumeOutcome = "retryable"
	ConsumePermanent        ConsumeOutcome = "permanent"
	ConsumePoison           ConsumeOutcome = "poison"
	ConsumeInvalidSchema    ConsumeOutcome = "invalid_schema"
	ConsumeInvalidSignature ConsumeOutcome = "invalid_signature"
)

// ConnState es el valor de `flux_connection_state`.
//
// Se llama ConnState y no ConnectionState porque MetricsSink ya tiene un método con ese
// nombre y leerlos juntos confundiría más de lo que el nombre largo aporta.
type ConnState int

const (
	StateDisconnected ConnState = 0
	StateConnected    ConnState = 1
	StateReconnecting ConnState = 2
)

// DurationBuckets devuelve los buckets del histograma, en segundos — §3.
//
// El último es 30 PORQUE ES el ack_wait (03-delivery.md §2). Un handler que cae en el
// bucket superior está a punto de que su mensaje se reentregue mientras aún se ejecuta,
// así que ese bucket mide directamente cuántos eventos rozan la ejecución concurrente.
// DEBE moverse si se cambia ack_wait: un bucket que no coincide con el plazo real mide
// algo que no le importa a nadie. Hay un test que lo vigila.
//
// Devuelve una copia nueva en cada llamada por la misma razón que CanonicalBackOff: un
// slice a nivel de paquete sería mutable desde fuera.
func DurationBuckets() []float64 {
	return []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
}

// MetricsSink es el destino de las métricas. Impleméntalo para enchufar tu backend.
//
// Las firmas fuerzan las etiquetas del protocolo: NO hay un `labels map[string]string`
// genérico a propósito, porque es justo por ahí por donde se cuela un `tenantid` que
// multiplica las series temporales. Y la cardinalidad no avisa — el sistema funciona en
// desarrollo con tres tenants y muere en producción con diez mil, y el fallo se
// manifiesta como "Prometheus se ha quedado sin memoria" (§2.2).
type MetricsSink interface {
	EventPublished(subject string, outcome PublishOutcome)
	EventConsumed(subject, consumer string, outcome ConsumeOutcome)
	HandlerDuration(subject, consumer string, seconds float64)
	// code DEBE ser un identificador estable, nunca el mensaje del error: un mensaje
	// lleva ids, timestamps y rutas, y su cardinalidad infinita tumba el almacenamiento.
	EventDLQ(subject, consumer string, reason DLQReason, code string)
	EventRetried(subject, consumer string, attempt int)
	ConsumerPending(subject, consumer string, pending int)
	ConnectionState(state ConnState)
}

// NoMetrics descarta todo. Es el default: un SDK no debe imponer un backend de métricas.
type NoMetrics struct{}

func (NoMetrics) EventPublished(string, PublishOutcome)        {}
func (NoMetrics) EventConsumed(string, string, ConsumeOutcome) {}
func (NoMetrics) HandlerDuration(string, string, float64)      {}
func (NoMetrics) EventDLQ(string, string, DLQReason, string)   {}
func (NoMetrics) EventRetried(string, string, int)             {}
func (NoMetrics) ConsumerPending(string, string, int)          {}
func (NoMetrics) ConnectionState(ConnState)                    {}

// ─── Recolector en memoria ───────────────────────────────────────────────────

type histogram struct {
	buckets []int
	sum     float64
	count   int
}

// InMemoryMetrics es un recolector sin dependencias que expone el formato de texto de
// Prometheus. Suficiente para servir un /metrics real.
//
// Si ya usas prometheus/client_golang, implementa MetricsSink contra él en vez de esto:
// lo que importa es conservar nombres y etiquetas.
//
// Es seguro desde varios goroutines — Render puede servirse desde el handler HTTP
// mientras el bucle de consumo sigue registrando.
type InMemoryMetrics struct {
	mu         sync.Mutex
	counters   map[string]float64
	gauges     map[string]float64
	histograms map[string]*histogram
}

// NewInMemoryMetrics crea un recolector vacío.
func NewInMemoryMetrics() *InMemoryMetrics {
	return &InMemoryMetrics{
		counters:   map[string]float64{},
		gauges:     map[string]float64{},
		histograms: map[string]*histogram{},
	}
}

type label struct{ name, value string }

// seriesKey devuelve `nombre{etiqueta="valor",…}` con las etiquetas ORDENADAS.
//
// Sin orden estable la misma serie temporal aparecería con dos claves distintas según el
// orden en que se construyó la lista.
func seriesKey(name string, labels []label) string {
	if len(labels) == 0 {
		// Sin llaves vacías: `flux_connection_state{} 1` no es formato válido.
		return name
	}
	sort.Slice(labels, func(i, j int) bool { return labels[i].name < labels[j].name })
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, l := range labels {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.name)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(l.value))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// escapeLabelValue escapa un valor según el formato de exposición de Prometheus.
//
// No es cosmética: un `code` con comillas partiría la línea y Prometheus descartaría EL
// SCRAPE ENTERO, no solo esa serie. Y el `code` sale de la clasificación de un error,
// que es exactamente el sitio donde alguien acabará metiendo un mensaje con comillas.
func escapeLabelValue(v string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v)
}

// formatValue evita la notación exponencial: `1e-05` no es un valor válido de
// Prometheus, y una duración acumulada pequeña llega ahí sin avisar.
func formatValue(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func (m *InMemoryMetrics) inc(name string, labels []label) {
	key := seriesKey(name, labels)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[key]++
}

func (m *InMemoryMetrics) set(name string, labels []label, value float64) {
	key := seriesKey(name, labels)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[key] = value
}

func (m *InMemoryMetrics) observe(name string, labels []label, value float64) {
	key := seriesKey(name, labels)
	buckets := DurationBuckets()

	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.histograms[key]
	if !ok {
		h = &histogram{buckets: make([]int, len(buckets))}
		m.histograms[key] = h
	}
	h.sum += value
	h.count++
	for i, limit := range buckets {
		if value <= limit {
			h.buckets[i]++
		}
	}
}

// EventPublished implementa MetricsSink.
func (m *InMemoryMetrics) EventPublished(subject string, outcome PublishOutcome) {
	m.inc(MetricEventsPublished, []label{{"subject", subject}, {"outcome", string(outcome)}})
}

// EventConsumed implementa MetricsSink.
func (m *InMemoryMetrics) EventConsumed(subject, consumer string, outcome ConsumeOutcome) {
	m.inc(MetricEventsConsumed, []label{
		{"subject", subject}, {"consumer", consumer}, {"outcome", string(outcome)}})
}

// HandlerDuration implementa MetricsSink.
func (m *InMemoryMetrics) HandlerDuration(subject, consumer string, seconds float64) {
	m.observe(MetricHandlerDuration, []label{{"subject", subject}, {"consumer", consumer}}, seconds)
}

// EventDLQ implementa MetricsSink.
func (m *InMemoryMetrics) EventDLQ(subject, consumer string, reason DLQReason, code string) {
	m.inc(MetricEventsDLQ, []label{
		{"subject", subject}, {"consumer", consumer},
		{"reason", string(reason)}, {"code", code}})
}

// EventRetried implementa MetricsSink.
func (m *InMemoryMetrics) EventRetried(subject, consumer string, attempt int) {
	m.inc(MetricEventsRetried, []label{
		{"subject", subject}, {"consumer", consumer}, {"attempt", strconv.Itoa(attempt)}})
}

// ConsumerPending implementa MetricsSink.
//
// Es la métrica de la cuarta alerta mínima (§4) y la que más importa: un consumidor cuyo
// bucle murió sigue reportando la conexión como sana, y solo el crecimiento de `pending`
// lo delata.
func (m *InMemoryMetrics) ConsumerPending(subject, consumer string, pending int) {
	m.set(MetricConsumerPending, []label{{"subject", subject}, {"consumer", consumer}},
		float64(pending))
}

// ConnectionState implementa MetricsSink.
func (m *InMemoryMetrics) ConnectionState(state ConnState) {
	m.set(MetricConnectionState, nil, float64(state))
}

// Render devuelve el formato de exposición de Prometheus. Sírvelo en /metrics.
func (m *InMemoryMetrics) Render() string {
	m.mu.Lock()
	counters := copyFloatMap(m.counters)
	gauges := copyFloatMap(m.gauges)
	histos := make(map[string]histogram, len(m.histograms))
	for k, h := range m.histograms {
		buckets := make([]int, len(h.buckets))
		copy(buckets, h.buckets)
		histos[k] = histogram{buckets: buckets, sum: h.sum, count: h.count}
	}
	m.mu.Unlock()

	var out []string
	familia := func(kind string, names []string, series map[string]float64) {
		for _, name := range names {
			keys := keysWithBase(series, name)
			if len(keys) == 0 {
				continue
			}
			out = append(out, "# TYPE "+name+" "+kind)
			for _, k := range keys {
				out = append(out, k+" "+formatValue(series[k]))
			}
		}
	}
	familia("counter", []string{
		MetricEventsPublished, MetricEventsConsumed, MetricEventsDLQ, MetricEventsRetried,
	}, counters)
	familia("gauge", []string{MetricConsumerPending, MetricConnectionState}, gauges)

	if len(histos) > 0 {
		out = append(out, "# TYPE "+MetricHandlerDuration+" histogram")
		keys := make([]string, 0, len(histos))
		for k := range histos {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buckets := DurationBuckets()
		for _, k := range keys {
			h := histos[k]
			labels := strings.TrimSuffix(strings.TrimPrefix(k, MetricHandlerDuration+"{"), "}")
			if labels == MetricHandlerDuration { // sin etiquetas: TrimPrefix no cortó nada
				labels = ""
			}
			sep := ""
			if labels != "" {
				sep = ","
			}
			for i, limit := range buckets {
				out = append(out, MetricHandlerDuration+`_bucket{`+labels+sep+
					`le="`+formatValue(limit)+`"} `+strconv.Itoa(h.buckets[i]))
			}
			out = append(out, MetricHandlerDuration+`_bucket{`+labels+sep+`le="+Inf"} `+
				strconv.Itoa(h.count))
			suffix := ""
			if labels != "" {
				suffix = "{" + labels + "}"
			}
			out = append(out, MetricHandlerDuration+"_sum"+suffix+" "+formatValue(h.sum))
			out = append(out, MetricHandlerDuration+"_count"+suffix+" "+strconv.Itoa(h.count))
		}
	}

	if len(out) == 0 {
		// Ni cabeceras `# TYPE` de métricas que nunca se han observado: un scrape con
		// familias vacías confunde más de lo que informa.
		return ""
	}
	return strings.Join(out, "\n") + "\n"
}

// Snapshot devuelve una copia de contadores y gauges. Solo para tests.
func (m *InMemoryMetrics) Snapshot() (counters, gauges map[string]float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return copyFloatMap(m.counters), copyFloatMap(m.gauges)
}

func copyFloatMap(src map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// keysWithBase devuelve, ordenadas, las claves de series de una misma métrica.
func keysWithBase(series map[string]float64, name string) []string {
	var keys []string
	for k := range series {
		if k == name || strings.HasPrefix(k, name+"{") {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}
