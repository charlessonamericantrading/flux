package flux

import (
	"encoding/json"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
)

// Réplica de sdk-node/test/metrics.test.ts más los casos que el contrato pide y que solo
// un test puede sostener: que los nombres y las etiquetas son los de protocol.json, y que
// ninguna etiqueta es `tenantid`.

// lineaPrometheus es la gramática del formato de exposición: `nombre{etiqueta="valor",…}
// valor`. Las comillas dentro de un valor solo valen escapadas — de ahí `(?:[^"\\]|\\.)*`.
var lineaPrometheus = regexp.MustCompile(
	`^[a-zA-Z_:][a-zA-Z0-9_:]*` +
		`(\{[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\]|\\.)*"` +
		`(,[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\]|\\.)*")*\})?` +
		` -?[0-9.]+$`)

var serieRe = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})? `)
var etiquetaRe = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)="`)

const (
	subjectPrueba  = "pedidos.pedido.v1.creado"
	consumerPrueba = "facturacion-api__pedidos_pedido_v1_creado"
)

// protocolJSON lee el contrato desde la raíz del repo.
//
// El SDK y protocol.json pueden divergir en un rename descuidado, y el síntoma sería un
// panel vacío, no un error. Mejor que falle aquí.
func protocolJSON(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("../protocol.json")
	if err != nil {
		t.Fatalf("no se pudo leer protocol.json: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("protocol.json no es JSON válido: %v", err)
	}
	return doc
}

func observabilidad(t *testing.T) map[string]any {
	t.Helper()
	return protocolJSON(t)["observability"].(map[string]any)
}

// conTodasLasMetricas registra una muestra de cada una de las siete.
func conTodasLasMetricas() *InMemoryMetrics {
	m := NewInMemoryMetrics()
	m.EventPublished(subjectPrueba, PublishOK)
	m.EventConsumed(subjectPrueba, consumerPrueba, ConsumeOK)
	m.HandlerDuration(subjectPrueba, consumerPrueba, 0.4)
	m.EventDLQ(subjectPrueba, consumerPrueba, DLQReasonPermanent, "HTTP_404")
	m.EventRetried(subjectPrueba, consumerPrueba, 3)
	m.ConsumerPending(subjectPrueba, consumerPrueba, 42)
	m.ConnectionState(StateConnected)
	return m
}

// series convierte el texto expuesto en {nombre: {etiquetas}}.
func series(t *testing.T, render string) map[string]map[string]bool {
	t.Helper()
	out := map[string]map[string]bool{}
	for _, linea := range strings.Split(strings.TrimRight(render, "\n"), "\n") {
		if linea == "" || strings.HasPrefix(linea, "#") {
			continue
		}
		m := serieRe.FindStringSubmatch(linea)
		if m == nil {
			t.Fatalf("línea no parseable: %q", linea)
		}
		if out[m[1]] == nil {
			out[m[1]] = map[string]bool{}
		}
		for _, e := range etiquetaRe.FindAllStringSubmatch(m[2], -1) {
			out[m[1]][e[1]] = true
		}
	}
	return out
}

func claves(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ─── El contrato ─────────────────────────────────────────────────────────────

func TestNombresDeMetricasSonLosDeProtocolJSON(t *testing.T) {
	// Si dos SDKs nombran distinto lo mismo, agrupar deja de funcionar en cuanto el
	// ecosistema es polyglot — que es siempre (08-observability.md §1).
	expuestas := map[string]bool{}
	for nombre := range series(t, conTodasLasMetricas().Render()) {
		// El histograma se expone en tres familias derivadas del mismo nombre.
		nombre = strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(
			nombre, "_bucket"), "_sum"), "_count")
		expuestas[nombre] = true
	}

	esperadas := map[string]bool{}
	for nombre := range observabilidad(t)["metrics"].(map[string]any) {
		esperadas[nombre] = true
	}

	if strings.Join(claves(expuestas), ",") != strings.Join(claves(esperadas), ",") {
		t.Errorf("métricas expuestas %v, protocol.json declara %v",
			claves(expuestas), claves(esperadas))
	}
}

func TestEtiquetasSonLasDeProtocolJSON(t *testing.T) {
	expuestas := series(t, conTodasLasMetricas().Render())
	for nombre, def := range observabilidad(t)["metrics"].(map[string]any) {
		d := def.(map[string]any)
		var esperadas []string
		for _, l := range d["labels"].([]any) {
			esperadas = append(esperadas, l.(string))
		}
		sort.Strings(esperadas)

		clave := nombre
		if d["type"] == "histogram" {
			clave = nombre + "_count" // _bucket añade `le`, que es del formato, no una etiqueta
		}
		got := claves(expuestas[clave])
		if strings.Join(got, ",") != strings.Join(esperadas, ",") {
			t.Errorf("%s expone %v, protocol.json declara %v", nombre, got, esperadas)
		}
	}
}

func TestNingunaEtiquetaEsDeAltaCardinalidad(t *testing.T) {
	// NUNCA se etiqueta por tenantid, id ni correlationid: un tenant nuevo no debe crear
	// series temporales nuevas. Para eso están las trazas, donde la cardinalidad de un
	// evento individual no es un problema — 08-observability.md §2.2.
	prohibidas := map[string]bool{}
	for _, l := range observabilidad(t)["forbiddenLabels"].([]any) {
		prohibidas[l.(string)] = true
	}
	for nombre, etiquetas := range series(t, conTodasLasMetricas().Render()) {
		for etiqueta := range etiquetas {
			if prohibidas[etiqueta] {
				t.Errorf("%s está etiquetada por %q", nombre, etiqueta)
			}
		}
	}
}

// ─── Buckets ─────────────────────────────────────────────────────────────────

func TestUltimoBucketEsElAckWait(t *testing.T) {
	// 08-observability.md §3: un handler en el bucket superior está a punto de que su
	// mensaje se reentregue mientras aún se ejecuta. Si el bucket deja de coincidir con
	// el plazo real, mide algo que no le importa a nadie — y el día que alguien cambie
	// DefaultAckWait nadie se acordará de este número si no falla un test.
	buckets := DurationBuckets()
	if got := buckets[len(buckets)-1]; got != DefaultAckWait.Seconds() {
		t.Errorf("último bucket = %v s, ack_wait = %v s", got, DefaultAckWait.Seconds())
	}
}

func TestBucketsCoincidenConProtocolJSON(t *testing.T) {
	esperados := observabilidad(t)["durationBucketsSeconds"].([]any)
	buckets := DurationBuckets()
	if len(buckets) != len(esperados) {
		t.Fatalf("%d buckets, protocol.json declara %d", len(buckets), len(esperados))
	}
	for i, want := range esperados {
		if buckets[i] != want.(float64) {
			t.Errorf("bucket[%d] = %v, se esperaba %v", i, buckets[i], want)
		}
	}
}

func TestBucketsAscendentes(t *testing.T) {
	buckets := DurationBuckets()
	for i := 1; i < len(buckets); i++ {
		if buckets[i] <= buckets[i-1] {
			t.Errorf("bucket[%d]=%v no es mayor que bucket[%d]=%v",
				i, buckets[i], i-1, buckets[i-1])
		}
	}
}

func TestDurationBucketsDevuelveUnaCopia(t *testing.T) {
	// Un slice a nivel de paquete sería mutable desde fuera y una entrada alterada
	// cambiaría en silencio el histograma de todo consumidor creado después.
	DurationBuckets()[0] = 999
	if DurationBuckets()[0] != 0.005 {
		t.Error("DurationBuckets comparte el array subyacente entre llamadas")
	}
}

// ─── Recolector ──────────────────────────────────────────────────────────────

func TestCuentaPublicacionesPorSubjectYResultado(t *testing.T) {
	m := NewInMemoryMetrics()
	m.EventPublished(subjectPrueba, PublishOK)
	m.EventPublished(subjectPrueba, PublishOK)
	m.EventPublished(subjectPrueba, PublishInvalidSchema)

	counters, _ := m.Snapshot()
	if got := counters[`flux_events_published_total{outcome="ok",subject="`+subjectPrueba+`"}`]; got != 2 {
		t.Errorf("ok = %v, se esperaba 2", got)
	}
	if got := counters[`flux_events_published_total{outcome="invalid_schema",subject="`+subjectPrueba+`"}`]; got != 1 {
		t.Errorf("invalid_schema = %v, se esperaba 1", got)
	}
}

func TestEtiquetasOrdenadasParaQueLaClaveSeaEstable(t *testing.T) {
	// Sin orden estable, la misma serie temporal aparecería con dos claves según el orden
	// en que se construyó la lista de etiquetas.
	a, b := NewInMemoryMetrics(), NewInMemoryMetrics()
	a.EventDLQ("s", "c", DLQReasonPermanent, "X")
	b.EventDLQ("s", "c", DLQReasonPermanent, "X")
	ca, _ := a.Snapshot()
	cb, _ := b.Snapshot()
	for k := range ca {
		if _, ok := cb[k]; !ok {
			t.Errorf("clave inestable: %q", k)
		}
	}
}

func TestHistogramaAcumulaEnTodosLosBucketsQueSuperanElValor(t *testing.T) {
	m := NewInMemoryMetrics()
	m.HandlerDuration("s", "c", 0.03) // cae por encima de 0.025
	salida := m.Render()

	for _, want := range []string{`le="0.025"} 0`, `le="0.05"} 1`, `le="+Inf"} 1`} {
		if !strings.Contains(salida, want) {
			t.Errorf("falta %q en:\n%s", want, salida)
		}
	}
}

func TestHandlerLentoCaeEnElBucketDelAckWait(t *testing.T) {
	// La señal que el bucket superior existe para dar: 29 s cuenta en le="30" y nada
	// antes; el evento está a un segundo de ejecutarse consigo mismo.
	m := NewInMemoryMetrics()
	m.HandlerDuration("s", "c", 29)
	salida := m.Render()
	for _, want := range []string{`le="10"} 0`, `le="30"} 1`} {
		if !strings.Contains(salida, want) {
			t.Errorf("falta %q en:\n%s", want, salida)
		}
	}
}

func TestGaugeSinEtiquetasNoDejaLlavesVacias(t *testing.T) {
	m := NewInMemoryMetrics()
	m.ConnectionState(StateConnected)
	if !strings.Contains(m.Render(), "flux_connection_state 1\n") {
		t.Errorf("se esperaba `flux_connection_state 1`, se obtuvo:\n%s", m.Render())
	}
}

func TestLineasTienenFormaValidaDePrometheus(t *testing.T) {
	for _, linea := range strings.Split(strings.TrimRight(conTodasLasMetricas().Render(), "\n"), "\n") {
		if linea == "" || strings.HasPrefix(linea, "#") {
			continue
		}
		if !lineaPrometheus.MatchString(linea) {
			t.Errorf("línea no válida para Prometheus: %q", linea)
		}
	}
}

func TestEscapaLasComillasDeLosValoresDeEtiqueta(t *testing.T) {
	// Un `code` con comillas rompería el formato de exposición y Prometheus descartaría
	// el scrape ENTERO, no solo esa línea.
	m := NewInMemoryMetrics()
	m.EventDLQ("s", "c", DLQReasonPermanent, `con "comillas" y \ barra`)
	m.EventDLQ("s", "c", DLQReasonPoison, "dos\nlíneas")

	lineas := 0
	for _, linea := range strings.Split(strings.TrimRight(m.Render(), "\n"), "\n") {
		if strings.HasPrefix(linea, "#") {
			continue
		}
		lineas++
		if !lineaPrometheus.MatchString(linea) {
			t.Errorf("el escapado no produjo una línea válida: %q", linea)
		}
	}
	// Dos series, dos líneas: el salto de línea del `code` no puede partir la exposición.
	if lineas != 2 {
		t.Errorf("%d líneas, se esperaban 2", lineas)
	}
}

func TestRecolectorVacioNoExponeNada(t *testing.T) {
	// Ni cabeceras `# TYPE` de métricas que nunca se han observado: un scrape con
	// familias vacías confunde más de lo que informa.
	if got := NewInMemoryMetrics().Render(); got != "" {
		t.Errorf("un recolector vacío expuso:\n%s", got)
	}
}

func TestRecolectorEsSeguroDesdeVariosGoroutines(t *testing.T) {
	// El bucle de consumo registra mientras el handler de /metrics renderiza. Sin
	// candado, `go test -race` lo caza aquí y no en producción.
	m := NewInMemoryMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				m.EventPublished(subjectPrueba, PublishOK)
				m.HandlerDuration(subjectPrueba, consumerPrueba, 0.01)
				m.ConnectionState(StateReconnecting)
				_ = m.Render()
			}
		}()
	}
	wg.Wait()

	counters, _ := m.Snapshot()
	if got := counters[`flux_events_published_total{outcome="ok",subject="`+subjectPrueba+`"}`]; got != 400 {
		t.Errorf("contador = %v, se esperaban 400", got)
	}
}

func TestNoMetricsNoLanzaYNoGuardaNada(t *testing.T) {
	// Es el default: un SDK no debe imponer un backend de métricas.
	var sink MetricsSink = NoMetrics{}
	sink.EventPublished("s", PublishOK)
	sink.EventConsumed("s", "c", ConsumePoison)
	sink.HandlerDuration("s", "c", 1)
	sink.EventDLQ("s", "c", DLQReasonPoison, "X")
	sink.EventRetried("s", "c", 1)
	sink.ConsumerPending("s", "c", 0)
	sink.ConnectionState(StateDisconnected)
}

// InMemoryMetrics DEBE satisfacer la interfaz: si le faltara un método, el fallo
// aparecería al enchufarla en Connect y no aquí.
var _ MetricsSink = (*InMemoryMetrics)(nil)
var _ MetricsSink = NoMetrics{}
