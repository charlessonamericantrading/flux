package flux

// El estado replicado ejecutado contra un `nats-server` REAL, con TLS, los permisos que genera `scripts/gen-acl.mjs` y `flux
// provision` (testenv_test.go): el ciclo del criterio de aceptación de 10-state.md §10 punto 6 —el dueño escribe 100 claves, un
// observador NUEVO las recibe todas y recibe `onSynced`, se borra una, el observador recibe la lápida y `Get` da «no existe»—, y,
// en los dos sentidos, quitar un permiso SE NOTA.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const contratoClientes = "clientes.cliente.v1"

// sinTenant: un observador de TODOS los tenants (informes-api) no fija el tenant en la conexión: con él, su Watch filtraría por
// ese tenant, y su ACL no concede el durable de uno.
func sinTenant(o *ConnectOptions) { o.TenantID = "" }

// perm devuelve el permiso EXACTO de `service` (de publicación) que empieza por `prefix`: los nombres los pone el generador, y un
// test que los reescribiera a mano concedería, si el SDK nombrara mal un durable, exactamente el permiso equivocado.
func perm(t *testing.T, env *testEnv, service, prefix string) string {
	t.Helper()
	var hallados []string
	for _, p := range env.Perms[service].Publish {
		if strings.HasPrefix(p, prefix) {
			hallados = append(hallados, p)
		}
	}
	if len(hallados) != 1 {
		t.Fatalf("el generador da %d permisos de %s que empiezan por %q: %v", len(hallados), service, prefix, hallados)
	}
	return hallados[0]
}

// recoge acumula las entradas que recibe un Watch, de forma segura entre goroutines.
type recoge struct {
	mu      sync.Mutex
	entries []StateEntry
	synced  atomic.Int32
	// vistoAlSincronizar es cuántas entradas había cuando se llamó a onSynced.
	vistoAlSincronizar atomic.Int32
}

func (r *recoge) handler(context.Context, StateEntry, StateDelivery) error { return nil }

func (r *recoge) Handler() StateHandler {
	return func(_ context.Context, e StateEntry, _ StateDelivery) error {
		r.mu.Lock()
		r.entries = append(r.entries, e)
		r.mu.Unlock()
		return nil
	}
}

func (r *recoge) OnSynced() StateOption {
	return WithOnSynced(func() {
		r.mu.Lock()
		n := len(r.entries)
		r.mu.Unlock()
		r.vistoAlSincronizar.Store(int32(n))
		r.synced.Add(1)
	})
}

func (r *recoge) snapshot() []StateEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]StateEntry(nil), r.entries...)
}

func (r *recoge) espera(t *testing.T, cond func([]StateEntry) bool, que string) []StateEntry {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if es := r.snapshot(); cond(es) {
			return es
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("no llegó: %s (llevo %d entradas)", que, len(r.snapshot()))
	return nil
}

func datosCliente(i, version int) map[string]any {
	return map[string]any{"clienteId": fmt.Sprintf("cli-%d", i), "nombre": "María Peña", "email": fmt.Sprintf("cli%d@example.com", i), "aggregateVersion": version}
}

func TestEstadoCicloDe100ClavesBajoLaAclGenerada(t *testing.T) {
	env := startEnv(t, envOptions{})
	crm, _, err := env.connectAs("crm-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	metrics := NewInMemoryMetrics()
	obs, sink, err := env.connectAs("informes-api", func(o *ConnectOptions) { sinTenant(o); o.Metrics = metrics })
	if err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t, 60*time.Second)

	for i := 1; i <= 100; i++ {
		res, err := crm.State().Put(ctx, contratoClientes, fmt.Sprintf("cli-%d", i), datosCliente(i, 1))
		if err != nil || !res.Applied || res.Outcome != "ok" || res.Revision == 0 {
			t.Fatalf("Put %d: %+v, %v", i, res, err)
		}
	}

	// Un observador NUEVO recibe la foto entera y `onSynced`.
	rec := &recoge{}
	sub, err := obs.State().Watch(ctx, contratoClientes, rec.Handler(), rec.OnSynced())
	if err != nil {
		t.Fatalf("Watch bajo la ACL generada falló: %v", err)
	}
	defer sub.Unsubscribe()
	if sub.Durable != "informes-api__state_clientes_cliente_v1" || sub.TenantID != "" {
		t.Errorf("durable/tenant = %q/%q", sub.Durable, sub.TenantID)
	}
	rec.espera(t, func(es []StateEntry) bool { return len(es) >= 100 }, "las 100 claves")
	deadline := time.Now().Add(10 * time.Second)
	for rec.synced.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if rec.synced.Load() != 1 {
		t.Fatalf("onSynced se llamó %d veces, se esperaba 1", rec.synced.Load())
	}
	if rec.vistoAlSincronizar.Load() != 100 {
		t.Errorf("onSynced llegó con %d entradas procesadas: debe llegar cuando el handler procesó TODO lo que había (100)", rec.vistoAlSincronizar.Load())
	}
	vistas := map[string]StateEntry{}
	for _, e := range rec.snapshot() {
		if e.Deleted || e.TenantID != "acme" || e.AggregateVersion != 1 || e.Revision == 0 {
			t.Errorf("entrada inesperada: %+v", e)
		}
		vistas[e.Key] = e
	}
	if len(vistas) != 100 {
		t.Errorf("claves distintas = %d", len(vistas))
	}

	// Se borra una: el observador recibe la lápida y `Get` da «no existe».
	res, err := crm.State().Delete(ctx, contratoClientes, "cli-7", 2)
	if err != nil || !res.Applied {
		t.Fatalf("Delete: %+v, %v", res, err)
	}
	rec.espera(t, func(es []StateEntry) bool {
		for _, e := range es {
			if e.Key == "cli-7" && e.Deleted {
				return e.Data == nil && e.AggregateVersion == 2
			}
		}
		return false
	}, "la lápida de cli-7")
	if _, found, err := obs.State().Get(ctx, contratoClientes, "cli-7", WithStateTenant("acme")); err != nil || found {
		t.Errorf("Get de una clave borrada: found=%v, %v", found, err)
	}
	got, found, err := obs.State().Get(ctx, contratoClientes, "cli-8", WithStateTenant("acme"))
	if err != nil || !found {
		t.Fatalf("Get de una clave viva: %v, %v", found, err)
	}
	dato, err := UnmarshalStateData[map[string]any](got)
	if err != nil || dato["clienteId"] != "cli-8" || got.AggregateVersion != 1 {
		t.Errorf("Get: %+v / %v / %v", got, dato, err)
	}
	// El valor anterior SALE del stream: la lápida lo sustituye (el dato personal ya no está).
	_, ops := env.opsConn()
	st, err := ops.Stream(ctx, "STATE_CLIENTES")
	if err != nil {
		t.Fatal(err)
	}
	last, err := st.GetLastMsgForSubject(ctx, "state.clientes.cliente.v1.acme.cli-7")
	if err != nil || strings.Contains(string(last.Data), "cli7@example.com") {
		t.Errorf("tras el borrado el valor anterior sigue en el stream: %v / %s", err, last.Data)
	}

	// Las métricas: un contador por contrato y resultado, sin clave ni tenant.
	time.Sleep(100 * time.Millisecond)
	if v := env.violations(); len(v) != 0 {
		t.Errorf("el ciclo dejó %d violaciones de permiso en el log del servidor:\n%s\nlogger:\n%s", len(v), strings.Join(v, "\n"), sink.text())
	}
	out := metrics.Render()
	if !strings.Contains(out, `flux_consumer_pending{consumer="informes-api__state_clientes_cliente_v1",subject="state.clientes.cliente.v1"}`) {
		t.Errorf("falta flux_consumer_pending de estado (la etiqueta consumer va sin tenant):\n%s", out)
	}
}

func TestEstadoStaleIdempotenciaYLaVersionSobreviveAlBorrado(t *testing.T) {
	env := startEnv(t, envOptions{})
	metrics := NewInMemoryMetrics()
	crm, _, err := env.connectAs("crm-api", func(o *ConnectOptions) { o.Metrics = metrics })
	if err != nil {
		t.Fatal(err)
	}
	st := crm.State()
	ctx := ctxT(t, 30*time.Second)
	put := func(v int) StateWriteResult {
		t.Helper()
		r, err := st.Put(ctx, contratoClientes, "k", map[string]any{"aggregateVersion": v, "x": v})
		if err != nil {
			t.Fatalf("Put v%d: %v", v, err)
		}
		return r
	}
	r5 := put(5)
	if !r5.Applied || r5.Outcome != "ok" {
		t.Fatalf("v5: %+v", r5)
	}
	// La misma versión NO avanza: `stale`, que no es un error, con la revisión del mensaje ACTUAL.
	if r := put(5); r.Applied || r.Outcome != "stale" || r.Revision != r5.Revision {
		t.Errorf("v5 otra vez: %+v (revisión actual %d)", r, r5.Revision)
	}
	if r := put(3); r.Applied || r.Outcome != "stale" || r.Revision != r5.Revision {
		t.Errorf("v3 (retrocede): %+v", r)
	}
	r6 := put(6)
	if !r6.Applied || r6.Revision <= r5.Revision {
		t.Errorf("v6: %+v", r6)
	}
	// El contador SOBREVIVE a los borrados: una clave borrada y vuelta a crear continúa desde la versión de su lápida.
	if r, err := st.Delete(ctx, contratoClientes, "k", 7); err != nil || !r.Applied {
		t.Fatalf("Delete v7: %+v, %v", r, err)
	}
	if r := put(7); r.Applied {
		t.Errorf("Put v7 tras la lápida v7 (una igual no avanza): %+v", r)
	}
	if r := put(8); !r.Applied {
		t.Errorf("Put v8 tras la lápida v7: %+v", r)
	}
	// Borrar una clave que nunca existió escribe la lápida.
	if r, err := st.Delete(ctx, contratoClientes, "nunca-existio", 1); err != nil || !r.Applied {
		t.Errorf("Delete de una clave inexistente: %+v, %v", r, err)
	}
	e, found, err := st.Get(ctx, contratoClientes, "k")
	if err != nil || !found || e.AggregateVersion != 8 {
		t.Errorf("Get final: %+v, %v, %v", e, found, err)
	}
	out := metrics.Render()
	for _, quiero := range []string{`flux_state_writes_total{contract="clientes.cliente.v1",outcome="ok"} 5`, `flux_state_writes_total{contract="clientes.cliente.v1",outcome="stale"} 3`} {
		if !strings.Contains(out, quiero) {
			t.Errorf("falta %s en:\n%s", quiero, out)
		}
	}
	if strings.Contains(out, `"k"`) || strings.Contains(out, "acme") {
		t.Errorf("la clave y el tenant NUNCA son etiquetas (una serie por cada agregado y por cada cliente):\n%s", out)
	}
}

// Un acuse perdido NO convierte una escritura buena en un `stale` con su propio dato (§3.2, paso 1b): el reintento relee, reconoce
// SU `id` y devuelve `ok`.
func TestEstadoUnAcusePerdidoNoSeConvierteEnStale(t *testing.T) {
	env := startEnv(t, envOptions{})
	crm, _, err := env.connectAs("crm-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	st := crm.State()
	ctx := ctxT(t, 30*time.Second)
	c, _ := ParseStateContract(contratoClientes)
	ev, err := BuildStateEvent(StateEventInput{Contract: contratoClientes, TenantID: "acme", Key: "ack", ID: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55", Source: "/test/crm-api",
		Time: "2026-08-20T10:25:39.412Z", CorrelationID: "c", ProducerVersion: "1", Data: map[string]any{"aggregateVersion": 4}})
	if err != nil {
		t.Fatal(err)
	}
	subject, _ := StateSubject(contratoClientes, "acme", "ack")
	primera, err := st.write(ctx, c, "acme", "ack", subject, 4, ev)
	if err != nil || !primera.Applied {
		t.Fatalf("primer intento: %+v, %v", primera, err)
	}
	// El MISMO evento otra vez (mismo `id`, misma versión): lo que ve el SDK tras perder el acuse.
	segunda, err := st.write(ctx, c, "acme", "ack", subject, 4, ev)
	if err != nil || !segunda.Applied || segunda.Outcome != "ok" || segunda.Revision != primera.Revision {
		t.Errorf("el reintento tras un acuse perdido: %+v (primera %+v), %v", segunda, primera, err)
	}
	// Otro `id` con la misma versión SÍ es `stale`.
	ev.ID = "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e99"
	if tercera, err := st.write(ctx, c, "acme", "ack", subject, 4, ev); err != nil || tercera.Applied {
		t.Errorf("otro id con la misma versión debe ser stale: %+v, %v", tercera, err)
	}
}

// Dos instancias del dueño no pisan un valor nuevo con uno viejo (§3.1): el chequeo de secuencia del broker.
func TestEstadoEscritoresConcurrentesNoPisanUnValorNuevo(t *testing.T) {
	env := startEnv(t, envOptions{})
	a, _, err := env.connectAs("crm-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := env.connectAs("crm-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t, 90*time.Second)
	const versiones = 60
	orden := rand.New(rand.NewSource(7)).Perm(versiones)
	var wg sync.WaitGroup
	var conflictos, ok, stale atomic.Int32
	trabajo := make(chan int, versiones)
	for _, v := range orden {
		trabajo <- v + 1
	}
	close(trabajo)
	for w, bus := range []*Bus{a, b, a, b, a, b} {
		wg.Add(1)
		go func(w int, bus *Bus) {
			defer wg.Done()
			for v := range trabajo {
				for {
					r, err := bus.State().Put(ctx, contratoClientes, "carrera", map[string]any{"aggregateVersion": v})
					var sc *StateConflictError
					if errors.As(err, &sc) {
						conflictos.Add(1)
						if sc.FluxClass() != ClassRetryable || sc.Attempts != StateMaxAttempts {
							t.Errorf("STATE_CONFLICT: %+v", sc)
						}
						continue // RETRYABLE: un nuevo intento relee
					}
					if err != nil {
						t.Errorf("Put v%d: %v", v, err)
						return
					}
					if r.Applied {
						ok.Add(1)
					} else {
						stale.Add(1)
					}
					break
				}
			}
		}(w, bus)
	}
	wg.Wait()
	e, found, err := a.State().Get(ctx, contratoClientes, "carrera")
	if err != nil || !found || e.AggregateVersion != versiones {
		t.Fatalf("la versión final debe ser la MÁS ALTA (%d): %+v, %v, %v", versiones, e, found, err)
	}
	if ok.Load()+stale.Load() != versiones {
		t.Errorf("ok %d + stale %d != %d", ok.Load(), stale.Load(), versiones)
	}
	t.Logf("60 escrituras desde 6 goroutines y 2 conexiones: %d ok, %d stale, %d STATE_CONFLICT reintentados", ok.Load(), stale.Load(), conflictos.Load())
}

// §3.2 paso 4, de forma DETERMINISTA: otra instancia escribe la clave entre la lectura y la publicación. Con el chequeo de
// secuencia el SDK lo nota (10071 o 10164), relee y reintenta; sin él, pisaría el valor nuevo con uno viejo.
func TestEstadoUnConflictoDeSecuenciaSeReintentaYAgotadosLosIntentosEsStateConflict(t *testing.T) {
	env := startEnv(t, envOptions{})
	metrics := NewInMemoryMetrics()
	crm, _, _ := env.connectAs("crm-api", func(o *ConnectOptions) { o.Metrics = metrics })
	_, ops := env.opsConn()
	ctx := ctxT(t, 30*time.Second)
	subject, _ := StateSubject(contratoClientes, "acme", "k")
	otraInstancia := func(version int) {
		ev, _ := BuildStateEvent(StateEventInput{Contract: contratoClientes, TenantID: "acme", Key: "k", ID: fmt.Sprintf("otra-%d", version), Source: "/otra/crm-api",
			Time: "2026-08-20T10:25:39.412Z", CorrelationID: "c", ProducerVersion: "1", Data: map[string]any{"aggregateVersion": version, "de": "otra"}})
		b, _ := Serialize(ev)
		if _, err := ops.PublishMsg(ctx, &nats.Msg{Subject: subject, Data: b}); err != nil {
			t.Error(err)
		}
	}
	// (a) Se cuela UNA vez, con una versión MENOR: el SDK relee, ve que su versión sigue avanzando y la escribe.
	var intentos atomic.Int32
	crm.State().afterRead = func(n int) {
		intentos.Store(int32(n))
		if n == 1 {
			otraInstancia(1)
		}
	}
	r, err := crm.State().Put(ctx, contratoClientes, "k", map[string]any{"aggregateVersion": 5})
	if err != nil || !r.Applied || intentos.Load() != 2 {
		t.Fatalf("un conflicto se reintenta: %+v, %v, intentos %d", r, err, intentos.Load())
	}
	// (b) Se cuela con una versión MAYOR: al releer, su versión ya no avanza: `stale`, no un error.
	crm.State().afterRead = func(n int) {
		if n == 1 {
			otraInstancia(9)
		}
	}
	r, err = crm.State().Put(ctx, contratoClientes, "k", map[string]any{"aggregateVersion": 6})
	if err != nil || r.Applied || r.Outcome != "stale" {
		t.Errorf("si lo que se coló es más nuevo, el resultado es stale: %+v, %v", r, err)
	}
	// (c) Se cuela EN CADA intento: agotados los 3, STATE_CONFLICT (RETRYABLE).
	v := 20
	crm.State().afterRead = func(n int) { otraInstancia(v + n) }
	_, err = crm.State().Put(ctx, contratoClientes, "k", map[string]any{"aggregateVersion": 1000})
	var sc *StateConflictError
	if !errors.As(err, &sc) || sc.FluxClass() != ClassRetryable || sc.FluxCode() != "STATE_CONFLICT" || sc.Attempts != 3 || sc.Key != "k" {
		t.Fatalf("agotados los 3 intentos: %v", err)
	}
	crm.State().afterRead = nil
	// El valor que quedó es el de la OTRA instancia: el SDK no lo pisó.
	e, _, _ := crm.State().Get(ctx, contratoClientes, "k")
	if e.AggregateVersion != int64(v+3) {
		t.Errorf("el SDK pisó el valor de la otra instancia: versión %d", e.AggregateVersion)
	}
	out := metrics.Render()
	if !strings.Contains(out, `outcome="conflict"} 1`) {
		t.Errorf("flux_state_writes_total debe contar el conflicto:\n%s", out)
	}
}

// Con versiones CRECIENTES por escritor casi todas se aplican, y el chequeo de secuencia del broker rechaza las que se cruzan: es
// donde se ejercitan los intentos de §3.2 (la prueba anterior da casi todo `stale`).
func TestEstadoEscritoresConVersionesCrecientesSeCruzanYElBrokerLosOrdena(t *testing.T) {
	env := startEnv(t, envOptions{})
	a, _, _ := env.connectAs("crm-api", nil)
	b, _, _ := env.connectAs("crm-api", nil)
	ctx := ctxT(t, 90*time.Second)
	const escritores, porEscritor = 6, 12
	var wg sync.WaitGroup
	var conflictos, ok, stale atomic.Int32
	for w := 0; w < escritores; w++ {
		bus := a
		if w%2 == 1 {
			bus = b
		}
		wg.Add(1)
		go func(w int, bus *Bus) {
			defer wg.Done()
			for i := 0; i < porEscritor; i++ {
				v := 1 + w + i*escritores // escritor w: w+1, w+1+6, … (creciente)
				for {
					r, err := bus.State().Put(ctx, contratoClientes, "cruce", map[string]any{"aggregateVersion": v})
					var sc *StateConflictError
					if errors.As(err, &sc) {
						conflictos.Add(1)
						continue
					}
					if err != nil {
						t.Errorf("Put v%d: %v", v, err)
						return
					}
					if r.Applied {
						ok.Add(1)
					} else {
						stale.Add(1)
					}
					break
				}
			}
		}(w, bus)
	}
	wg.Wait()
	e, found, err := a.State().Get(ctx, contratoClientes, "cruce")
	if err != nil || !found || e.AggregateVersion != escritores*porEscritor {
		t.Fatalf("la versión final debe ser la más alta (%d): %+v, %v, %v", escritores*porEscritor, e, found, err)
	}
	if ok.Load() < 12 {
		t.Errorf("solo %d escrituras se aplicaron: con versiones crecientes deberían ser bastantes", ok.Load())
	}
	t.Logf("%d escrituras: %d ok, %d stale, %d STATE_CONFLICT reintentados", escritores*porEscritor, ok.Load(), stale.Load(), conflictos.Load())
}

// Un observador que arranca tarde recibe solo el ÚLTIMO valor de cada clave (`last_per_subject`).
func TestEstadoUnObservadorNuevoRecibeSoloElUltimoValor(t *testing.T) {
	env := startEnv(t, envOptions{})
	crm, _, _ := env.connectAs("crm-api", nil)
	obs, _, err := env.connectAs("informes-api", sinTenant)
	if err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t, 30*time.Second)
	for v := 1; v <= 3; v++ {
		if _, err := crm.State().Put(ctx, contratoClientes, "k", map[string]any{"aggregateVersion": v}); err != nil {
			t.Fatal(err)
		}
	}
	rec := &recoge{}
	sub, err := obs.State().Watch(ctx, contratoClientes, rec.Handler(), rec.OnSynced())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	rec.espera(t, func(es []StateEntry) bool { return len(es) >= 1 }, "el valor")
	time.Sleep(500 * time.Millisecond)
	es := rec.snapshot()
	if len(es) != 1 || es[0].AggregateVersion != 3 {
		t.Errorf("recibió %d entradas, se esperaba UNA con la versión 3: %+v", len(es), es)
	}
}

// El estado vacío: `onSynced` de INMEDIATO, antes de entregar nada (§5.5, punto 2).
func TestEstadoOnSyncedConElContratoVacioEsInmediato(t *testing.T) {
	env := startEnv(t, envOptions{})
	obs, _, err := env.connectAs("informes-api", sinTenant)
	if err != nil {
		t.Fatal(err)
	}
	rec := &recoge{}
	sub, err := obs.State().Watch(ctxT(t, 20*time.Second), contratoClientes, rec.Handler(), rec.OnSynced())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if rec.synced.Load() != 1 {
		t.Errorf("onSynced = %d llamadas al volver Watch con el contrato vacío, se esperaba 1 (inmediato)", rec.synced.Load())
	}
}

// ─── Los permisos: quitar uno SE NOTA ────────────────────────────────────────

func TestEstadoQuitarUnPermisoSeNota(t *testing.T) {
	probe := func(t *testing.T, env *testEnv) (string, string, string, string, string) {
		return perm(t, env, "crm-api", "state.clientes."),
			perm(t, env, "crm-api", "$JS.API.DIRECT.GET.STATE_CLIENTES."),
			perm(t, env, "tienda-web", "$JS.API.CONSUMER.MSG.NEXT."),
			perm(t, env, "tienda-web", "$JS.API.CONSUMER.INFO."),
			perm(t, env, "tienda-web", "$JS.API.DIRECT.GET.")
	}
	// Los nombres salen del generador: se piden con un entorno previo y se usan para declarar las variantes del definitivo.
	probeEnv := &testEnv{Perms: generatedPerms(t, filepath.Join(repoRoot(t), "examples", "services.json"))}
	escribe, directoDueno, pullObs, infoObs, directoObs := probe(t, probeEnv)
	streamInfoDLQ := "$JS.API.STREAM.INFO.DLQ_CLIENTES"

	env := startEnv(t, envOptions{Variants: []variant{
		{Name: "crm-sin-escribir", Base: "crm-api", Drop: []string{escribe}},
		{Name: "crm-sin-direct", Base: "crm-api", Drop: []string{directoDueno}},
		{Name: "tienda-sin-pull", Base: "tienda-web", Drop: []string{pullObs}},
		{Name: "tienda-sin-info", Base: "tienda-web", Drop: []string{infoObs}},
		{Name: "tienda-sin-direct", Base: "tienda-web", Drop: []string{directoObs}},
		{Name: "tienda-sin-dlq", Base: "tienda-web", Drop: []string{streamInfoDLQ}},
	}})
	ctx := ctxT(t, 90*time.Second)
	rapido := func(t *testing.T, t0 time.Time) {
		t.Helper()
		if d := time.Since(t0); d > 4*time.Second {
			t.Errorf("tardó %v: una denegación llega en ms, no por el timeout de la petición", d)
		}
	}

	t.Run("el dueño sin permiso de escritura: Put falla con el subject de la clave", func(t *testing.T) {
		b, _, err := env.connectAs("crm-sin-escribir", nil)
		if err != nil {
			t.Fatal(err)
		}
		t0 := time.Now()
		_, err = b.State().Put(ctx, contratoClientes, "k", map[string]any{"aggregateVersion": 1})
		quierePermisoDenegado(t, err, "state.clientes.cliente.v1.acme.k")
		rapido(t, t0)
	})
	t.Run("el dueño sin DIRECT.GET: Put y Get fallan (el paso 1 de put lee)", func(t *testing.T) {
		b, _, err := env.connectAs("crm-sin-direct", nil)
		if err != nil {
			t.Fatal(err)
		}
		t0 := time.Now()
		_, err = b.State().Put(ctx, contratoClientes, "k", map[string]any{"aggregateVersion": 1})
		quierePermisoDenegado(t, err, "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.k")
		_, _, err = b.State().Get(ctx, contratoClientes, "k")
		quierePermisoDenegado(t, err, "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.k")
		rapido(t, t0)
	})
	t.Run("el observador sin pull: Watch FALLA (no queda inerte)", func(t *testing.T) {
		b, sink, err := env.connectAs("tienda-sin-pull", nil)
		if err != nil {
			t.Fatal(err)
		}
		t0 := time.Now()
		sub, err := b.State().Watch(ctx, contratoClientes, func(context.Context, StateEntry, StateDelivery) error { return nil })
		if err == nil {
			sub.Unsubscribe()
			t.Fatal("Watch devolvió nil con el pull denegado: un consumidor inerte que parece sano")
		}
		quierePermisoDenegado(t, err, pullObs)
		rapido(t, t0)
		time.Sleep(200 * time.Millisecond)
		if !sink.Contains("parece sano") {
			t.Errorf("la denegación debe quedar en el logger:\n%s", sink.text())
		}
	})
	t.Run("el observador sin CONSUMER.INFO: PERMISSION_DENIED y no NOT_PROVISIONED", func(t *testing.T) {
		b, _, _ := env.connectAs("tienda-sin-info", nil)
		_, err := b.State().Watch(ctx, contratoClientes, func(context.Context, StateEntry, StateDelivery) error { return nil })
		quierePermisoDenegado(t, err, infoObs)
	})
	t.Run("el observador sin DIRECT.GET: Get falla, Watch funciona", func(t *testing.T) {
		b, _, _ := env.connectAs("tienda-sin-direct", nil)
		_, _, err := b.State().Get(ctx, contratoClientes, "k")
		quierePermisoDenegado(t, err, "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.k")
		sub, err := b.State().Watch(ctx, contratoClientes, func(context.Context, StateEntry, StateDelivery) error { return nil })
		if err != nil {
			t.Fatalf("Watch no usa DIRECT.GET: %v", err)
		}
		sub.Unsubscribe()
	})
	t.Run("el observador sin STREAM.INFO de la DLQ: Watch falla al abrir, no cuando muera el primer mensaje", func(t *testing.T) {
		b, _, _ := env.connectAs("tienda-sin-dlq", nil)
		_, err := b.State().Watch(ctx, contratoClientes, func(context.Context, StateEntry, StateDelivery) error { return nil })
		quierePermisoDenegado(t, err, streamInfoDLQ)
	})
	t.Run("con todos los permisos, lo mismo funciona (el otro sentido)", func(t *testing.T) {
		dueno, _, _ := env.connectAs("crm-api", nil)
		obs, _, _ := env.connectAs("tienda-web", nil)
		if _, err := dueno.State().Put(ctx, contratoClientes, "k", map[string]any{"aggregateVersion": 1}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if _, found, err := obs.State().Get(ctx, contratoClientes, "k"); err != nil || !found {
			t.Fatalf("Get: %v, %v", found, err)
		}
		sub, err := obs.State().Watch(ctx, contratoClientes, func(context.Context, StateEntry, StateDelivery) error { return nil })
		if err != nil {
			t.Fatalf("Watch: %v", err)
		}
		sub.Unsubscribe()
	})
}

// El confinamiento por tenant lo aplica el BROKER, no el SDK (10-state.md §3.8).
func TestEstadoElConfinamientoPorTenantLoAplicaElBroker(t *testing.T) {
	env := startEnv(t, envOptions{})
	crm, _, _ := env.connectAs("crm-api", nil)
	tienda, _, _ := env.connectAs("tienda-web", nil)           // confinada a acme
	informes, _, _ := env.connectAs("informes-api", sinTenant) // todos los tenants
	ctx := ctxT(t, 60*time.Second)
	for _, tenant := range []string{"acme", "globex"} {
		if _, err := crm.State().Put(ctx, contratoClientes, "cli-1", map[string]any{"aggregateVersion": 1}, WithStateTenant(tenant)); err != nil {
			t.Fatalf("Put %s: %v", tenant, err)
		}
	}
	if _, found, err := tienda.State().Get(ctx, contratoClientes, "cli-1"); err != nil || !found {
		t.Errorf("tienda-web lee SU tenant: %v, %v", found, err)
	}
	_, _, err := tienda.State().Get(ctx, contratoClientes, "cli-1", WithStateTenant("globex"))
	quierePermisoDenegado(t, err, "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.globex.cli-1")
	// Un watch sobre otro tenant apunta a un durable que no existe para ella: la ACL no concede ni su INFO.
	_, err = tienda.State().Watch(ctx, contratoClientes, func(context.Context, StateEntry, StateDelivery) error { return nil }, WithStateTenant("globex"))
	var pd *PermissionDeniedError
	if !errors.As(err, &pd) || !strings.Contains(pd.Subject, "tienda-web__state_clientes_cliente_v1__globex") {
		t.Errorf("Watch de otro tenant: %v", err)
	}
	for _, tenant := range []string{"acme", "globex"} {
		if _, found, err := informes.State().Get(ctx, contratoClientes, "cli-1", WithStateTenant(tenant)); err != nil || !found {
			t.Errorf("informes-api observa todos los tenants (%s): %v, %v", tenant, found, err)
		}
	}
	// Y ve las entradas de los dos tenants por su watch, con su `TenantID`.
	rec := &recoge{}
	sub, err := informes.State().Watch(ctx, contratoClientes, rec.Handler(), rec.OnSynced())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	es := rec.espera(t, func(es []StateEntry) bool { return len(es) >= 2 }, "los dos tenants")
	tenants := []string{es[0].TenantID, es[1].TenantID}
	sort.Strings(tenants)
	if tenants[0] != "acme" || tenants[1] != "globex" {
		t.Errorf("tenants = %v: un watch sin tenant mezcla los de todos, y la entrada debe decir de cuál es", tenants)
	}
}

func TestEstadoSinAprovisionarEsNotProvisionedConLasDosCausas(t *testing.T) {
	env := startEnv(t, envOptions{NoProvision: true})
	crm, _, _ := env.connectAs("crm-api", nil)
	obs, _, _ := env.connectAs("informes-api", sinTenant)
	ctx := ctxT(t, 30*time.Second)

	t0 := time.Now()
	_, err := crm.State().Put(ctx, contratoClientes, "k", map[string]any{"aggregateVersion": 1})
	var np *NotProvisionedError
	if !errors.As(err, &np) || np.FluxCode() != "NOT_PROVISIONED" {
		t.Fatalf("Put sin stream: %v", err)
	}
	// Un stream que no existe y uno SIN `allow_direct` se ven igual (A-32): el mensaje DEBE nombrar las dos causas.
	for _, quiero := range []string{"STATE_CLIENTES", "allow_direct", "no existe", "flux provision", "flux doctor"} {
		if !strings.Contains(np.Error(), quiero) {
			t.Errorf("el mensaje debe mencionar %q: %v", quiero, np)
		}
	}
	if d := time.Since(t0); d > 3*time.Second {
		t.Errorf("tardó %v: `no responders` llega en ms", d)
	}
	if _, _, err := crm.State().Get(ctx, contratoClientes, "k"); !errors.As(err, &np) {
		t.Errorf("Get sin stream: %v", err)
	}
	if _, err := obs.State().Watch(ctx, contratoClientes, func(context.Context, StateEntry, StateDelivery) error { return nil }); !errors.As(err, &np) {
		t.Errorf("Watch sin stream: %v", err)
	}
}

// ─── Lo que llega mal: POISON, DLQ de estado ─────────────────────────────────

func leeDLQDeEstado(t *testing.T, env *testEnv, esperadas int) []Event {
	t.Helper()
	_, ops := env.opsConn()
	ctx := ctxT(t, 20*time.Second)
	deadline := time.Now().Add(15 * time.Second)
	var out []Event
	for time.Now().Before(deadline) {
		st, err := ops.Stream(ctx, "DLQ_CLIENTES")
		if err == nil {
			info, _ := st.Info(ctx)
			if info != nil && info.State.Msgs >= uint64(esperadas) {
				out = nil
				for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
					m, err := st.GetMsg(ctx, seq)
					if err != nil {
						continue
					}
					e, err := ParseEvent(m.Data)
					if err != nil {
						t.Fatalf("un evento de DLQ no parsea: %v / %s", err, m.Data)
					}
					out = append(out, e)
				}
				if len(out) >= esperadas {
					return out
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("la DLQ de estado no llegó a tener %d mensajes", esperadas)
	return nil
}

func TestEstadoUnMensajeQueMienteSobreSuIdentidadEsPoisonYNoSeAplica(t *testing.T) {
	env := startEnv(t, envOptions{})
	_, ops := env.opsConn()
	var poison atomic.Int32
	var ultimoCodigo atomic.Value
	obs, _, err := env.connectAs("informes-api", func(o *ConnectOptions) {
		sinTenant(o)
		o.OnPoison = func(p PoisonInfo) {
			poison.Add(1)
			if ce, ok := AsClassified(p.Err); ok {
				ultimoCodigo.Store(ce.FluxCode())
			}
			if len(p.Raw) != 0 {
				t.Errorf("`Raw` va vacío en estado: el cuerpo de una entrada puede llevar datos personales (%d bytes)", len(p.Raw))
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t, 60*time.Second)
	rec := &recoge{}
	sub, err := obs.State().Watch(ctx, contratoClientes, rec.Handler(), rec.OnSynced())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	// El sobre dice `globex` y el subject dice `acme`: uno miente.
	mentiroso, _ := BuildStateEvent(StateEventInput{Contract: contratoClientes, TenantID: "globex", Key: "cli-9", ID: "id-1", Source: "/x/y",
		Time: "2026-08-20T10:25:39.412Z", CorrelationID: "c", ProducerVersion: "1",
		Data: map[string]any{"email": "ana@example.com", "aggregateVersion": 1}})
	bytes, _ := Serialize(mentiroso)
	ack, err := ops.PublishMsg(ctx, &nats.Msg{Subject: "state.clientes.cliente.v1.acme.cli-9", Data: bytes})
	if err != nil {
		t.Fatal(err)
	}
	// Y uno bueno detrás, para saber que el bucle sigue vivo.
	bueno, _ := BuildStateEvent(StateEventInput{Contract: contratoClientes, TenantID: "acme", Key: "cli-10", ID: "id-2", Source: "/x/y",
		Time: "2026-08-20T10:25:39.412Z", CorrelationID: "c", ProducerVersion: "1", Data: map[string]any{"aggregateVersion": 1}})
	bytes, _ = Serialize(bueno)
	if _, err := ops.PublishMsg(ctx, &nats.Msg{Subject: "state.clientes.cliente.v1.acme.cli-10", Data: bytes}); err != nil {
		t.Fatal(err)
	}
	es := rec.espera(t, func(es []StateEntry) bool { return len(es) >= 1 }, "la entrada buena")
	if es[0].Key != "cli-10" {
		t.Errorf("la entrada que miente NO debe aplicarse ni llegar al handler: %+v", es)
	}
	dlq := leeDLQDeEstado(t, env, 1)[0]
	if poison.Load() != 1 || ultimoCodigo.Load() != "STATE_SUBJECT_MISMATCH" {
		t.Errorf("OnPoison: %d llamadas, código %v", poison.Load(), ultimoCodigo.Load())
	}
	if dlq.DLQReason != DLQReasonPoison || !strings.HasPrefix(dlq.DLQError, "STATE_SUBJECT_MISMATCH: ") || dlq.DLQAttempts != 1 {
		t.Errorf("DLQ: %s %q %d", dlq.DLQReason, dlq.DLQError, dlq.DLQAttempts)
	}
	if string(dlq.Data) != fmt.Sprintf(`{"key":"cli-9","revision":%d}`, ack.Sequence) {
		t.Errorf("la DLQ de estado no conserva el dato: %s", dlq.Data)
	}
	if strings.Contains(string(dlq.Data), "ana@example.com") || dlq.Signature != "" {
		t.Errorf("la DLQ filtró el dato o una firma: %+v", dlq)
	}
	if dlq.DLQConsumer != "informes-api__state_clientes_cliente_v1" {
		t.Errorf("dlqconsumer = %q", dlq.DLQConsumer)
	}
	// Terminado (`term`): no se reentrega.
	time.Sleep(500 * time.Millisecond)
	if n := len(rec.snapshot()); n != 1 {
		t.Errorf("entradas = %d", n)
	}
}

// `restricted` (24 h de retención) no cabe en un almacén que no caduca: lo que se publicó saltándose al SDK es POISON al consumir
// y no se aplica (10-state.md §7).
func TestEstadoUnaEntradaRestrictedEsPoisonAlConsumirYNoSeAplica(t *testing.T) {
	env := startEnv(t, envOptions{})
	_, ops := env.opsConn()
	var codigo atomic.Value
	obs, _, err := env.connectAs("informes-api", func(o *ConnectOptions) {
		sinTenant(o)
		o.OnPoison = func(p PoisonInfo) {
			if ce, ok := AsClassified(p.Err); ok {
				codigo.Store(ce.FluxCode())
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t, 60*time.Second)
	rec := &recoge{}
	sub, err := obs.State().Watch(ctx, contratoClientes, rec.Handler(), rec.OnSynced())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	ev, err := BuildStateEvent(StateEventInput{Contract: contratoClientes, TenantID: "acme", Key: "cli-20", ID: "id-r", Source: "/x/y",
		Time: "2026-08-20T10:25:39.412Z", CorrelationID: "c", ProducerVersion: "1", Data: map[string]any{"aggregateVersion": 1}})
	if err != nil {
		t.Fatal(err)
	}
	ev.DataClassification = ClassificationRestricted
	bytes, _ := Serialize(ev)
	if _, err := ops.PublishMsg(ctx, &nats.Msg{Subject: "state.clientes.cliente.v1.acme.cli-20", Data: bytes}); err != nil {
		t.Fatal(err)
	}
	dlq := leeDLQDeEstado(t, env, 1)[0]
	if dlq.DLQReason != DLQReasonPoison || !strings.HasPrefix(dlq.DLQError, "INVALID_DATACLASSIFICATION: ") {
		t.Errorf("DLQ: %s %q", dlq.DLQReason, dlq.DLQError)
	}
	if codigo.Load() != "INVALID_DATACLASSIFICATION" {
		t.Errorf("OnPoison: %v", codigo.Load())
	}
	time.Sleep(300 * time.Millisecond)
	if n := len(rec.snapshot()); n != 0 {
		t.Errorf("una entrada `restricted` NO se aplica ni llega al handler: %d entradas", n)
	}
}

func TestEstadoElErrorDelHandlerVaALaDLQSinCopiarDatosPersonales(t *testing.T) {
	env := startEnv(t, envOptions{})
	crm, _, _ := env.connectAs("crm-api", nil)
	obs, _, err := env.connectAs("informes-api", func(o *ConnectOptions) {
		sinTenant(o)
		o.Classifier = ClassifierOptions{UnknownErrorPolicy: UnknownPermanent}
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t, 60*time.Second)
	if _, err := crm.State().Put(ctx, contratoClientes, "cli-1", map[string]any{"aggregateVersion": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := crm.State().Put(ctx, contratoClientes, "cli-2", map[string]any{"aggregateVersion": 1}); err != nil {
		t.Fatal(err)
	}
	var visto atomic.Int32
	sub, err := obs.State().Watch(ctx, contratoClientes, func(_ context.Context, e StateEntry, d StateDelivery) error {
		visto.Add(1)
		if e.Key == "cli-1" {
			return NewPermanentError("el cliente cli-1 tiene un país fuera del catálogo", WithCode("PAIS_NO_SOPORTADO"))
		}
		// Un error que NO es de flux: el mensaje de un driver de base de datos puede citar la fila entera.
		return errors.New(`pq: duplicate key value violates unique constraint (email)=(ana@example.com)`)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	dlq := leeDLQDeEstado(t, env, 2)
	porClave := map[string]Event{}
	for _, e := range dlq {
		porClave[e.AggregateID] = e
	}
	if e := porClave["cli-1"]; e.DLQReason != DLQReasonPermanent || e.DLQError != "PAIS_NO_SOPORTADO: el cliente cli-1 tiene un país fuera del catálogo" || e.DLQAttempts != 1 {
		t.Errorf("DLQ de cli-1: %s %q %d", e.DLQReason, e.DLQError, e.DLQAttempts)
	}
	e2 := porClave["cli-2"]
	if !strings.HasPrefix(e2.DLQError, "UNKNOWN: ") || !strings.Contains(e2.DLQError, "10-state.md §6") {
		t.Errorf("un error que no es de flux deja solo su código y una frase fija: %q", e2.DLQError)
	}
	for _, e := range dlq {
		if strings.Contains(string(e.Data)+e.DLQError, "ana@example.com") {
			t.Errorf("la DLQ de estado guarda 90 días: el dato personal no puede acabar ahí: %+v", e)
		}
	}
}

// `flux state resync` borra el durable y lo recrea: el Watch vivo REABRE su consumo, sin CONSUMER.CREATE (§5.2, decisión 22).
func TestEstadoElWatchReabreElConsumoSiSeBorraElDurable(t *testing.T) {
	env := startEnv(t, envOptions{})
	_, ops := env.opsConn()
	crm, _, _ := env.connectAs("crm-api", nil)
	obs, sink, _ := env.connectAs("informes-api", sinTenant)
	ctx := ctxT(t, 90*time.Second)
	if _, err := crm.State().Put(ctx, contratoClientes, "antes", map[string]any{"aggregateVersion": 1}); err != nil {
		t.Fatal(err)
	}
	rec := &recoge{}
	sub, err := obs.State().Watch(ctx, contratoClientes, rec.Handler(), rec.OnSynced())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	rec.espera(t, func(es []StateEntry) bool { return len(es) >= 1 }, "la primera entrada")

	// resync: borrar y recrear con la configuración del aprovisionamiento.
	durable := sub.Durable
	cfg := jetstream.ConsumerConfig{Durable: durable, FilterSubject: "state.clientes.cliente.v1.*.*", AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: DefaultAckWait, MaxDeliver: DefaultMaxDeliver, BackOff: CanonicalBackOff(), MaxAckPending: DefaultMaxAckPending,
		DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy, ReplayPolicy: jetstream.ReplayInstantPolicy}
	if err := ops.DeleteConsumer(ctx, "STATE_CLIENTES", durable); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // el watch está reintentando: no existe
	if _, err := ops.CreateConsumer(ctx, "STATE_CLIENTES", cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := crm.State().Put(ctx, contratoClientes, "despues", map[string]any{"aggregateVersion": 1}); err != nil {
		t.Fatal(err)
	}
	es := rec.espera(t, func(es []StateEntry) bool {
		n := 0
		for _, e := range es {
			if e.Key == "despues" {
				n++
			}
		}
		return n >= 1
	}, "la entrada posterior al resync")
	antes := 0
	for _, e := range es {
		if e.Key == "antes" {
			antes++
		}
	}
	if antes != 2 {
		t.Errorf("tras recrear el durable se entrega la foto OTRA VEZ: «antes» llegó %d veces, se esperaban 2", antes)
	}
	if rec.synced.Load() != 1 {
		t.Errorf("onSynced no se invoca de nuevo tras un resync: %d", rec.synced.Load())
	}
	if !sink.Contains("se volvió a abrir") && !sink.Contains("reabierto") {
		t.Errorf("el logger debe decir que reabrió el consumo:\n%s", sink.text())
	}
}

func TestEstadoWatchSeParaConUnsubscribeYConClose(t *testing.T) {
	env := startEnv(t, envOptions{})
	crm, _, _ := env.connectAs("crm-api", nil)
	obs, _, _ := env.connectAs("informes-api", sinTenant)
	ctx := ctxT(t, 60*time.Second)
	rec := &recoge{}
	sub, err := obs.State().Watch(ctx, contratoClientes, rec.Handler())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := crm.State().Put(ctx, contratoClientes, "a", map[string]any{"aggregateVersion": 1}); err != nil {
		t.Fatal(err)
	}
	rec.espera(t, func(es []StateEntry) bool { return len(es) >= 1 }, "a")
	t0 := time.Now()
	sub.Unsubscribe()
	sub.Unsubscribe() // idempotente
	if d := time.Since(t0); d > 5*time.Second {
		t.Errorf("Unsubscribe tardó %v", d)
	}
	n := len(rec.snapshot())
	if _, err := crm.State().Put(ctx, contratoClientes, "b", map[string]any{"aggregateVersion": 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if len(rec.snapshot()) != n {
		t.Error("un watch parado sigue entregando")
	}
	// Close con un watch vivo no se cuelga.
	sub2, err := obs.State().Watch(ctx, contratoClientes, rec.Handler())
	if err != nil {
		t.Fatal(err)
	}
	_ = sub2
	t0 = time.Now()
	if err := obs.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if d := time.Since(t0); d > 5*time.Second {
		t.Errorf("Close tardó %v", d)
	}
}

var _ = json.Marshal

// L3 en estado: el contrato se resuelve en el índice `state` del bundle —sin red— y `data` se valida ANTES de leer ni escribir
// (10-state.md §3.6, §10 punto 5).
func TestEstadoL3ValidaContraElEsquemaDelContratoYNoCopiaValores(t *testing.T) {
	bundle, err := LoadSchemaBundle("../schemas/bundle.json")
	if err != nil {
		t.Fatal(err)
	}
	if got := StateSchemaURIFor(bundle, contratoClientes); got != "https://schemas.internal/state/clientes/cliente/1.0.0.json" {
		t.Fatalf("el índice `state` del bundle: %q", got)
	}
	env := startEnv(t, envOptions{})
	metrics := NewInMemoryMetrics()
	crm, _, err := env.connectAs("crm-api", func(o *ConnectOptions) {
		o.SchemaBaseURL = ""
		o.Metrics = metrics
		o.Validation = ValidationOptions{Mode: ValidationStrict, Bundle: bundle}
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := ctxT(t, 30*time.Second)
	valido := map[string]any{"clienteId": "cli-1", "nombre": "María Peña", "email": "maria@example.com", "aggregateVersion": 1}
	if _, err := crm.State().Put(ctx, contratoClientes, "cli-1", valido); err != nil {
		t.Fatalf("un valor válido: %v", err)
	}
	// `dataschema` es el del bundle.
	_, ops := env.opsConn()
	st, _ := ops.Stream(ctx, "STATE_CLIENTES")
	m, err := st.GetLastMsgForSubject(ctx, "state.clientes.cliente.v1.acme.cli-1")
	if err != nil || !strings.Contains(string(m.Data), `"dataschema":"https://schemas.internal/state/clientes/cliente/1.0.0.json"`) {
		t.Errorf("dataschema: %v / %s", err, m.Data)
	}

	// Inválido: reporta la ruta, NUNCA el valor (acabaría en logs y en dlqerror, y en estado es dato personal).
	malo := map[string]any{"clienteId": "cli-2", "nombre": "Ana Secreta", "email": "sin-arroba", "aggregateVersion": 1}
	_, err = crm.State().Put(ctx, contratoClientes, "cli-2", malo)
	var sve *SchemaValidationError
	if !errors.As(err, &sve) || sve.FluxCode() != "INVALID_SCHEMA" || sve.FluxClass() != ClassPermanent {
		t.Fatalf("un valor que no cumple su esquema: %v", err)
	}
	if strings.Contains(err.Error(), "sin-arroba") || strings.Contains(err.Error(), "Ana Secreta") {
		t.Errorf("el mensaje de validación NO debe citar valores: %v", err)
	}
	if !strings.Contains(err.Error(), "email") {
		t.Errorf("debe citar la RUTA del campo: %v", err)
	}
	if _, found, _ := crm.State().Get(ctx, contratoClientes, "cli-2"); found {
		t.Error("L3 falla ANTES de escribir: la clave no debe existir")
	}
	// La lápida valida contra su esquema fijo (que está en `schemas`, no en el índice `state`).
	if _, err := crm.State().Delete(ctx, contratoClientes, "cli-1", 2); err != nil {
		t.Errorf("la lápida debe validar: %v", err)
	}
	// Un contrato sin dataschema posible (ni mapa, ni bundle, ni SchemaBaseURL) falla antes de la red, diciendo qué declarar.
	_, err = crm.State().Put(ctx, "clientes.otro.v1", "k", map[string]any{"aggregateVersion": 1})
	if err == nil || !strings.Contains(err.Error(), "no hay dataschema") {
		t.Errorf("contrato sin dataschema: %v", err)
	}
	// Con dataschema declarado pero ausente del bundle, en modo estricto es SCHEMA_NOT_FOUND (nunca se resuelve por red).
	otro, _, err := env.connectAs("crm-api", func(o *ConnectOptions) {
		o.SchemaBaseURL = ""
		o.Schemas = map[string]string{"clientes.otro.v1": "https://schemas.internal/state/clientes/otro/1.0.0.json"}
		o.Validation = ValidationOptions{Mode: ValidationStrict, Bundle: bundle}
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = otro.State().Put(ctx, "clientes.otro.v1", "k", map[string]any{"aggregateVersion": 1})
	var snf *SchemaNotFoundError
	if !errors.As(err, &snf) {
		t.Errorf("dataschema fuera del bundle en modo estricto: %v", err)
	}
	// `MISSING_AGGREGATE_VERSION` y los fallos de L3 se cuentan como `invalid_schema`; los errores de la llamada, no.
	out := metrics.Render()
	if !strings.Contains(out, `flux_state_writes_total{contract="clientes.cliente.v1",outcome="invalid_schema"} 1`) {
		t.Errorf("falta invalid_schema en:\n%s", out)
	}
}
