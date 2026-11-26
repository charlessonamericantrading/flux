// Petición/respuesta contra un nats-server REAL, con los permisos que genera `scripts/gen-acl.mjs` y `flux provision`.
// Contrato normativo: specification/11-request-reply.md
//
// Un broker abierto no prueba nada de seguridad, y un doble de `nats.go` no prueba nada de petición/respuesta: aquí quien llama
// es `pedidos-api`, quien responde es `crm-api`, y lo que decide el broker (la ACL, `no responders`, el interés) lo decide el
// broker. Sin `nats-server` las pruebas se saltan con un aviso, salvo con FLUX_REQUIRE_BROKER=1 (un salto no es un pase).

package flux

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	opCalcular = "clientes.cliente.v1.calcular-descuento"
	opObtener  = "clientes.cliente.v1.obtener"
)

func peticionCalcular() map[string]any {
	return map[string]any{"clienteId": "cli-1", "totalCents": 9990, "moneda": "EUR"}
}

func respuestaCalcular() map[string]any {
	return map[string]any{"descuentoCents": 500, "moneda": "EUR"}
}

// codigo es el código estable de un error de flux, o "" si no lo es.
func codigo(err error) string {
	if ce, ok := AsClassified(err); ok {
		return ce.FluxCode()
	}
	return ""
}

func claseDe(err error) ErrorClass {
	if ce, ok := AsClassified(err); ok {
		return ce.FluxClass()
	}
	return ""
}

// responde registra un `Respond` y lo para al acabar el test.
func responde(t *testing.T, b *Bus, op string, h RPCHandler, opts ...RespondOption) *Responder {
	t.Helper()
	r, err := b.Respond(ctxT(t, 20*time.Second), op, h, opts...)
	if err != nil {
		t.Fatalf("Respond(%s): %v", op, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.Stop(ctx)
	})
	return r
}

func calcula(_ context.Context, _ RPCRequest) (any, error) { return respuestaCalcular(), nil }

// ─── el ciclo de aceptación ──────────────────────────────────────────────────

// El ciclo de §12.9: dos instancias del dueño reparten (el grupo de cola es el nombre del servicio); al parar AMBAS, quien llama ve
// RPC_UNAVAILABLE en < 100 ms —no un timeout—, también con un `Subscribe` de eventos vivo en su MISMA conexión (A-101: el SDK no abre
// una segunda suscripción a su propio inbox) y también cuando lo que se cierra es el Bus.
func TestRPCCicloDosInstanciasRepartenYAlPararAmbasRPCUnavailable(t *testing.T) {
	env := startEnv(t, envOptions{})
	var n1, n2 atomic.Int32
	a1, _, err := env.connectAs("crm-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	a2, _, err := env.connectAs("crm-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	r1 := responde(t, a1, opCalcular, func(ctx context.Context, req RPCRequest) (any, error) { n1.Add(1); return respuestaCalcular(), nil })
	r2 := responde(t, a2, opCalcular, func(ctx context.Context, req RPCRequest) (any, error) { n2.Add(1); return respuestaCalcular(), nil })
	if r1.Subject != "rpc."+opCalcular {
		t.Errorf("Subject = %q: la suscripción es al subject EXACTO", r1.Subject)
	}

	caller, _, err := env.connectAs("pedidos-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Un consumidor de eventos vivo en la misma conexión que llama.
	sub, err := caller.Subscribe(ctxT(t, 10*time.Second), "inventario.stock.v1.ajustado",
		func(context.Context, Event, Delivery) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	ctx := ctxT(t, 60*time.Second)
	for i := 0; i < 40; i++ {
		raw, err := caller.Request(ctx, opCalcular, peticionCalcular(), WithRPCAggregateID("cli-1"))
		if err != nil {
			t.Fatalf("petición %d: %v", i, err)
		}
		if string(raw) != `{"descuentoCents":500,"moneda":"EUR"}` {
			t.Fatalf("data de la respuesta = %s", raw)
		}
	}
	if n1.Load() == 0 || n2.Load() == 0 || n1.Load()+n2.Load() != 40 {
		t.Errorf("las dos instancias DEBEN repartirse las 40 peticiones (grupo de cola = el servicio): %d y %d", n1.Load(), n2.Load())
	}

	// Parar la primera: la segunda atiende TODO.
	if err := r1.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	antes := n2.Load()
	for i := 0; i < 5; i++ {
		if _, err := caller.Request(ctx, opCalcular, peticionCalcular()); err != nil {
			t.Fatalf("con una instancia: %v", err)
		}
	}
	if n2.Load()-antes != 5 {
		t.Errorf("la instancia que queda debe atender las 5: %d", n2.Load()-antes)
	}

	// Parar la segunda: nadie escucha. RPC_UNAVAILABLE, rápido y RETRYABLE.
	if err := r2.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	inicio := time.Now()
	_, err = caller.Request(ctx, opCalcular, peticionCalcular())
	if d := time.Since(inicio); d > 100*time.Millisecond {
		t.Errorf("RPC_UNAVAILABLE tardó %s: debe ser `no responders`, en milisegundos, no el timeout de 5 s", d)
	}
	t.Logf("MEDIDA RPC_UNAVAILABLE tras parar las dos instancias: %s", time.Since(inicio))
	var un *RPCUnavailableError
	if !errors.As(err, &un) || codigo(err) != "RPC_UNAVAILABLE" || claseDe(err) != ClassRetryable {
		t.Fatalf("con todas paradas: %v", err)
	}
	if !strings.Contains(err.Error(), opCalcular) {
		t.Errorf("el error debe decir QUÉ operación: %v", err)
	}

	// Y a través de Close(): un dueño que cierra su bus también deja de contestar.
	r3, err := a1.Respond(ctx, opCalcular, calcula)
	if err != nil {
		t.Fatal(err)
	}
	_ = r3
	if _, err := caller.Request(ctx, opCalcular, peticionCalcular()); err != nil {
		t.Fatalf("tras volver a registrar: %v", err)
	}
	if err := a1.Close(); err != nil {
		t.Fatal(err)
	}
	inicio = time.Now()
	_, err = caller.Request(ctx, opCalcular, peticionCalcular())
	if d := time.Since(inicio); d > 100*time.Millisecond || codigo(err) != "RPC_UNAVAILABLE" {
		t.Errorf("tras Close() del dueño: %s, %v", d, err)
	}
}

// Una operación que el servicio NO atiende da RPC_UNAVAILABLE en ms, aunque su permiso sea `rpc.clientes.>`: la suscripción es
// EXACTA (A-64). Con un comodín, el interés existiría y quien llama esperaría su timeout.
func TestRPCUnaOperacionQueElDuenoNoAtiendeDaUnavailableNoTimeout(t *testing.T) {
	env := startEnv(t, envOptions{})
	crm, _, _ := env.connectAs("crm-api", nil)
	responde(t, crm, opCalcular, calcula)
	caller, _, _ := env.connectAs("facturacion-api", nil) // llama a las dos
	ctx := ctxT(t, 30*time.Second)
	inicio := time.Now()
	_, err := caller.Request(ctx, opObtener, map[string]any{"clienteId": "cli-1"})
	if d := time.Since(inicio); d > 200*time.Millisecond || codigo(err) != "RPC_UNAVAILABLE" {
		t.Errorf("una operación sin atender: %s, %v", d, err)
	}
}

// ─── lo que viaja ────────────────────────────────────────────────────────────

// Lo que viaja por el cable, visto por un espía en bruto: el sobre completo SIN cabeceras de NATS, la petición por el subject
// `rpc.<operación>` (cinco tokens) y la respuesta al inbox PRIVADO del servicio; la respuesta hereda lo que debe.
func TestRPCLoQueViajaPorElCable(t *testing.T) {
	env := startEnv(t, envOptions{})
	spy, _ := env.opsConn()
	pet, err := spy.SubscribeSync("rpc." + opCalcular)
	if err != nil {
		t.Fatal(err)
	}
	res, err := spy.SubscribeSync("_INBOX.pedidos-api.>")
	if err != nil {
		t.Fatal(err)
	}
	if err := spy.Flush(); err != nil {
		t.Fatal(err)
	}
	crm, _, _ := env.connectAs("crm-api", func(o *ConnectOptions) { o.Version = "2.1.0" })
	responde(t, crm, opCalcular, calcula, WithRespondClassification(ClassificationConfidential))
	caller, _, _ := env.connectAs("pedidos-api", func(o *ConnectOptions) { o.Version = "3.4.1" })
	ctx := ctxT(t, 30*time.Second)
	if _, err := caller.Request(ctx, opCalcular, peticionCalcular(), WithRPCAggregateID("cli-1")); err != nil {
		t.Fatal(err)
	}

	mp, err := pet.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(mp.Header) != 0 {
		t.Errorf("la petición NO lleva cabeceras de NATS (todo va en el sobre): %v", mp.Header)
	}
	if !strings.HasPrefix(mp.Reply, "_INBOX.pedidos-api.") {
		t.Errorf("el subject de respuesta es el inbox PRIVADO del servicio, no `_INBOX.<aleatorio>`: %q", mp.Reply)
	}
	peticion, err := ParseEvent(mp.Data)
	if err != nil {
		t.Fatal(err)
	}
	if peticion.Type != "com.flux.rpc.clientes.cliente.calcular-descuento.request.v1" || peticion.AggregateID != "cli-1" ||
		peticion.PartitionKey != "cli-1" || peticion.TenantID != "acme" || peticion.ProducerVersion != "3.4.1" ||
		peticion.DataClassification != ClassificationInternal || peticion.Source != "/test/pedidos-api" ||
		peticion.CorrelationID != peticion.ID || peticion.CausationID != "" ||
		peticion.DataSchema != "https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json" {
		t.Errorf("sobre de la petición: %+v", peticion)
	}
	if !strings.HasSuffix(string(mp.Data), `"data":{"clienteId":"cli-1","moneda":"EUR","totalCents":9990}}`) {
		t.Errorf("`data` va SIEMPRE el último atributo: %s", mp.Data)
	}

	mr, err := res.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	respuesta, err := ParseEvent(mr.Data)
	if err != nil {
		t.Fatal(err)
	}
	if respuesta.Type != "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1" ||
		respuesta.CorrelationID != peticion.CorrelationID || respuesta.CausationID != peticion.ID ||
		respuesta.TenantID != "acme" || respuesta.AggregateID != "cli-1" || respuesta.PartitionKey != "cli-1" ||
		respuesta.ID == peticion.ID || respuesta.ProducerVersion != "2.1.0" ||
		respuesta.DataClassification != ClassificationConfidential || respuesta.Source != "/test/crm-api" {
		t.Errorf("sobre de la respuesta: %+v", respuesta)
	}
	if len(mr.Header) != 0 {
		t.Errorf("la respuesta no lleva cabeceras: %v", mr.Header)
	}

	// Con una cadena en curso el `correlationid` NO es el `id` de la petición: la respuesta hereda el primero, y su `causationid`
	// es el segundo (con los dos iguales, como en una llamada que abre cadena, un error de herencia no se vería).
	cadena := WithEventContext(ctx, EventContext{CorrelationID: "corr-7", CausationID: "evt-3"})
	if _, err := caller.Request(cadena, opCalcular, peticionCalcular(), WithRPCAggregateID("cli-1")); err != nil {
		t.Fatal(err)
	}
	mp, _ = pet.NextMsg(5 * time.Second)
	mr, _ = res.NextMsg(5 * time.Second)
	peticion, _ = ParseEvent(mp.Data)
	respuesta, _ = ParseEvent(mr.Data)
	if peticion.CorrelationID != "corr-7" || peticion.CausationID != "evt-3" || respuesta.CorrelationID != "corr-7" ||
		respuesta.CausationID != peticion.ID || respuesta.CausationID == "evt-3" {
		t.Errorf("la respuesta hereda el correlationid de la petición y su causationid es el `id` de la petición: petición %+v / respuesta %+v", peticion, respuesta)
	}
}

// Una llamada hecha dentro de un handler continúa la cadena: el `correlationid` es el de la primera petición y cada `causationid`
// apunta a su padre. Y el tenant de la llamada se resuelve como el de los eventos: opción > el del contexto > el de la conexión.
func TestRPCUnaLlamadaDentroDeUnHandlerContinuaLaCadenaYElTenantSeResuelve(t *testing.T) {
	env := startEnv(t, envOptions{})
	spy, _ := env.opsConn()
	pet, _ := spy.SubscribeSync("rpc." + opCalcular)
	spy.Flush()
	crm, _, _ := env.connectAs("crm-api", nil)
	responde(t, crm, opCalcular, calcula)
	caller, _, _ := env.connectAs("pedidos-api", nil)
	ctx := ctxT(t, 30*time.Second)

	// El contexto de un evento en curso: el tenant y la correlación se heredan.
	cadena := WithEventContext(ctx, EventContext{CorrelationID: "corr-1", CausationID: "evt-0", TenantID: "globex",
		TraceParent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"})
	if _, err := caller.Request(cadena, opCalcular, peticionCalcular()); err != nil {
		t.Fatal(err)
	}
	m, _ := pet.NextMsg(5 * time.Second)
	e, _ := ParseEvent(m.Data)
	if e.TenantID != "globex" || e.CorrelationID != "corr-1" || e.CausationID != "evt-0" ||
		e.TraceParent != "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" {
		t.Errorf("hereda del contexto: tenant=%q correlationid=%q causationid=%q traceparent=%q", e.TenantID, e.CorrelationID, e.CausationID, e.TraceParent)
	}
	// La opción gana al contexto.
	if _, err := caller.Request(cadena, opCalcular, peticionCalcular(), WithRPCTenant("initech")); err != nil {
		t.Fatal(err)
	}
	m, _ = pet.NextMsg(5 * time.Second)
	if e, _ = ParseEvent(m.Data); e.TenantID != "initech" {
		t.Errorf("la opción gana: %q", e.TenantID)
	}
	// Sin nada, el de la conexión; y sin ninguno, `system` (a diferencia del estado: una petición no es un dato guardado).
	if _, err := caller.Request(ctx, opCalcular, peticionCalcular()); err != nil {
		t.Fatal(err)
	}
	m, _ = pet.NextMsg(5 * time.Second)
	if e, _ = ParseEvent(m.Data); e.TenantID != "acme" {
		t.Errorf("el de la conexión: %q", e.TenantID)
	}
	sin, _, _ := env.connectAs("pedidos-api", func(o *ConnectOptions) { o.TenantID = "" })
	if _, err := sin.Request(ctx, opCalcular, peticionCalcular()); err != nil {
		t.Fatal(err)
	}
	m, _ = pet.NextMsg(5 * time.Second)
	if e, _ = ParseEvent(m.Data); e.TenantID != "system" {
		t.Errorf("sin tenant: %q", e.TenantID)
	}

	// Dentro del handler de quien responde, `ctx` lleva la petición: una llamada ahí continúa la cadena.
	var visto atomic.Value
	crm2, _, _ := env.connectAs("crm-api", nil)
	responde(t, crm2, opObtener, func(ctx context.Context, req RPCRequest) (any, error) {
		ec, _ := EventContextFrom(ctx)
		visto.Store(ec)
		return map[string]any{"clienteId": "cli-1", "nombre": "Ana", "email": "ana@example.com"}, nil
	})
	fac, _, _ := env.connectAs("facturacion-api", nil)
	spy2, _ := env.opsConn()
	pet2, _ := spy2.SubscribeSync("rpc." + opObtener)
	spy2.Flush()
	if _, err := fac.Request(ctx, opObtener, map[string]any{"clienteId": "cli-1"}); err != nil {
		t.Fatal(err)
	}
	m, _ = pet2.NextMsg(5 * time.Second)
	original, _ := ParseEvent(m.Data)
	ec, _ := visto.Load().(EventContext)
	if ec.CorrelationID != original.CorrelationID || ec.CausationID != original.ID || ec.TenantID != "acme" {
		t.Errorf("el handler ve la cadena de la petición: %+v (petición %s)", ec, original.ID)
	}
}

// ─── errores ─────────────────────────────────────────────────────────────────

// El error del handler cruza el broker con su CLASE y su CÓDIGO: NewPermanentError / NewRetryableError los eligen; uno que no es de
// flux es `permanent` UNKNOWN con un mensaje genérico (su texto puede llevar datos) y su detalle va al log de quien responde.
func TestRPCLosErroresDelHandlerLlegananClasificados(t *testing.T) {
	env := startEnv(t, envOptions{})
	crm, sink, _ := env.connectAs("crm-api", nil)
	var cual atomic.Int32
	responde(t, crm, opCalcular, func(ctx context.Context, req RPCRequest) (any, error) {
		switch cual.Add(1) {
		case 1:
			return nil, NewPermanentError("el cliente cli-1 no existe", WithCode("CLIENTE_NO_ENCONTRADO"))
		case 2:
			return nil, NewRetryableError("la base de tarifas no responde", WithCode("TARIFAS_NO_DISPONIBLES"), WithRetryAfter(5*time.Second))
		case 3:
			return nil, errors.New("pq: duplicate key value (email)=(ana@example.com)")
		case 4:
			return nil, NewPermanentError("código malo", WithCode("minusculas"))
		case 5:
			panic("¡bum!")
		case 6:
			return []int{1, 2}, nil // no es un objeto
		}
		return respuestaCalcular(), nil
	})
	caller, _, _ := env.connectAs("pedidos-api", nil)
	ctx := ctxT(t, 30*time.Second)

	_, err := caller.Request(ctx, opCalcular, peticionCalcular())
	var rr *RPCRemoteError
	if !errors.As(err, &rr) || rr.FluxCode() != "CLIENTE_NO_ENCONTRADO" || rr.FluxClass() != ClassPermanent ||
		!strings.Contains(rr.Message, "cli-1 no existe") || rr.Operation != opCalcular {
		t.Fatalf("permanent con su código: %#v", err)
	}
	_, err = caller.Request(ctx, opCalcular, peticionCalcular())
	if !errors.As(err, &rr) || rr.FluxCode() != "TARIFAS_NO_DISPONIBLES" || rr.FluxClass() != ClassRetryable || rr.RetryAfter() != 5*time.Second {
		t.Fatalf("retryable con retryAfterMs: %#v", err)
	}
	_, err = caller.Request(ctx, opCalcular, peticionCalcular())
	if !errors.As(err, &rr) || rr.FluxCode() != "UNKNOWN" || rr.FluxClass() != ClassPermanent {
		t.Fatalf("un error desconocido es permanent UNKNOWN: %#v", err)
	}
	if strings.Contains(rr.Message, "ana@example.com") || strings.Contains(rr.Message, "duplicate") {
		t.Errorf("el mensaje de lo desconocido NO cita el texto de la excepción: %q", rr.Message)
	}
	if !sink.Contains("duplicate key") {
		t.Errorf("el detalle va al log de quien responde:\n%s", sink.text())
	}
	_, err = caller.Request(ctx, opCalcular, peticionCalcular())
	if !errors.As(err, &rr) || rr.FluxCode() != "PERMANENT" {
		t.Fatalf("un código que no cumple el patrón cae al de la clase: %#v", err)
	}
	_, err = caller.Request(ctx, opCalcular, peticionCalcular())
	if !errors.As(err, &rr) || rr.FluxClass() != ClassPermanent {
		t.Fatalf("un pánico en el handler es una respuesta, no un timeout: %#v", err)
	}
	_, err = caller.Request(ctx, opCalcular, peticionCalcular())
	if !errors.As(err, &rr) || rr.FluxCode() != "INVALID_SCHEMA" || !strings.Contains(rr.Message, "objeto") {
		t.Fatalf("una respuesta que no es un objeto: %#v", err)
	}
	if _, err = caller.Request(ctx, opCalcular, peticionCalcular()); err != nil {
		t.Fatalf("y el servicio sigue vivo tras todo eso: %v", err)
	}
}

// RPC_TIMEOUT: el responder ejecuta pero tarda más que el timeout; la respuesta TARDÍA llega al inbox y se ignora, y la conexión sigue
// sana (A-61). Y `WithRPCRetries` reutiliza el MISMO `id` y los mismos bytes: quien responde puede reconocer la orden repetida.
func TestRPCTimeoutLaRespuestaTardiaSeIgnoraYLosReintentosReutilizanElId(t *testing.T) {
	env := startEnv(t, envOptions{})
	crm, _, _ := env.connectAs("crm-api", nil)
	var mu sync.Mutex
	var ids []string
	var llamadas atomic.Int32
	responde(t, crm, opCalcular, func(ctx context.Context, req RPCRequest) (any, error) {
		mu.Lock()
		ids = append(ids, req.ID)
		mu.Unlock()
		if llamadas.Add(1) <= 2 {
			time.Sleep(400 * time.Millisecond)
		}
		return respuestaCalcular(), nil
	})
	caller, _, _ := env.connectAs("pedidos-api", nil)
	ctx := ctxT(t, 30*time.Second)

	inicio := time.Now()
	_, err := caller.Request(ctx, opCalcular, peticionCalcular(), WithRPCTimeout(150*time.Millisecond))
	var to *RPCTimeoutError
	if !errors.As(err, &to) || codigo(err) != "RPC_TIMEOUT" || claseDe(err) != ClassRetryable {
		t.Fatalf("un handler lento: %v", err)
	}
	if d := time.Since(inicio); d < 140*time.Millisecond || d > 600*time.Millisecond {
		t.Errorf("el timeout dura el timeout pedido: %s", d)
	}
	time.Sleep(500 * time.Millisecond) // la respuesta tardía llega al inbox y se ignora
	if _, err := caller.Request(ctx, opCalcular, peticionCalcular()); err != nil {
		t.Fatalf("tras una respuesta tardía la conexión sigue sana: %v", err)
	}

	// Con reintentos: el primer intento (el 2.º del handler) tarda 400 ms y vence; el segundo acaba bien.
	mu.Lock()
	ids = nil
	mu.Unlock()
	llamadas.Store(1)
	if _, err := caller.Request(ctx, opCalcular, peticionCalcular(), WithRPCTimeout(150*time.Millisecond), WithRPCRetries(1)); err != nil {
		t.Fatalf("con un reintento: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Errorf("el reintento llega con el MISMO `id` (quien responde puede deduplicar): %v", ids)
	}
}

// `WithRPCRetries` cubre el hueco de un despliegue: RPC_UNAVAILABLE se reintenta; un error permanente, nunca.
func TestRPCUnReintentoCubreElHuecoDeUnDespliegueYUnPermanenteNoSeReintenta(t *testing.T) {
	env := startEnv(t, envOptions{})
	crm, _, _ := env.connectAs("crm-api", nil)
	caller, _, _ := env.connectAs("pedidos-api", nil)
	ctx := ctxT(t, 30*time.Second)
	go func() {
		time.Sleep(120 * time.Millisecond)
		_, _ = crm.Respond(ctx, opCalcular, calcula)
	}()
	if _, err := caller.Request(ctx, opCalcular, peticionCalcular(), WithRPCRetries(3)); err != nil {
		t.Fatalf("el servicio arranca entre intentos: %v", err)
	}

	var n atomic.Int32
	crm2, _, _ := env.connectAs("crm-api", nil)
	responde(t, crm2, opObtener, func(context.Context, RPCRequest) (any, error) {
		n.Add(1)
		return nil, NewPermanentError("no", WithCode("NO"))
	})
	fac, _, _ := env.connectAs("facturacion-api", nil)
	if _, err := fac.Request(ctx, opObtener, map[string]any{"clienteId": "cli-1"}, WithRPCRetries(3)); codigo(err) != "NO" || n.Load() != 1 {
		t.Errorf("un error permanente no se reintenta: %v (%d intentos)", err, n.Load())
	}
}

// ─── lo mal formado falla ANTES de la red ────────────────────────────────────

func TestRPCLoMalFormadoFallaAntesDeLaRedYConSuCodigo(t *testing.T) {
	env := startEnv(t, envOptions{})
	caller, _, _ := env.connectAs("pedidos-api", nil)
	spy, _ := env.opsConn()
	visto, _ := spy.SubscribeSync("rpc.>")
	spy.Flush()
	ctx := ctxT(t, 30*time.Second)
	casos := []struct {
		nombre string
		op     string
		data   any
		opts   []RequestOption
		codigo string
	}{
		{"prefijo rpc.", "rpc." + opCalcular, peticionCalcular(), nil, "RESERVED_DOMAIN"},
		{"dominio state", "state.cliente.v1.obtener", peticionCalcular(), nil, "RESERVED_DOMAIN"},
		{"mayúsculas", "Clientes.cliente.v1.obtener", peticionCalcular(), nil, "INVALID_RPC_CALL"},
		{"comodín", "clientes.cliente.v1.*", peticionCalcular(), nil, "INVALID_RPC_CALL"},
		{"tres tokens", "clientes.cliente.obtener", peticionCalcular(), nil, "INVALID_RPC_CALL"},
		{"data no es un objeto", opCalcular, []int{1}, nil, "INVALID_RPC_CALL"},
		{"timeout 0", opCalcular, peticionCalcular(), []RequestOption{WithRPCTimeout(0)}, "INVALID_RPC_CALL"},
		{"timeout de 26 s", opCalcular, peticionCalcular(), []RequestOption{WithRPCTimeout(26 * time.Second)}, "INVALID_RPC_CALL"},
		{"timeout con fracción", opCalcular, peticionCalcular(), []RequestOption{WithRPCTimeout(1500 * time.Microsecond)}, "INVALID_RPC_CALL"},
		{"reintentos negativos", opCalcular, peticionCalcular(), []RequestOption{WithRPCRetries(-1)}, "INVALID_RPC_CALL"},
		{"clasificación", opCalcular, peticionCalcular(), []RequestOption{WithRPCClassification("secreto")}, "INVALID_DATACLASSIFICATION"},
	}
	for _, c := range casos {
		inicio := time.Now()
		_, err := caller.Request(ctx, c.op, c.data, c.opts...)
		if codigo(err) != c.codigo || claseDe(err) != ClassPermanent {
			t.Errorf("%s: %v (se esperaba %s PERMANENT)", c.nombre, err, c.codigo)
		}
		if d := time.Since(inicio); d > 100*time.Millisecond {
			t.Errorf("%s tardó %s: debe fallar SIN abrir la petición", c.nombre, d)
		}
	}
	if m, err := visto.NextMsg(300 * time.Millisecond); err == nil {
		t.Errorf("nada de eso debe llegar a la red: %q", m.Subject)
	}
	// Un handler que no es una función, o una operación con prefijo, tampoco registra nada.
	if _, err := caller.Respond(ctx, "rpc."+opCalcular, calcula); codigo(err) != "RESERVED_DOMAIN" {
		t.Errorf("Respond con prefijo rpc.: %v", err)
	}
	if _, err := caller.Respond(ctx, opCalcular, nil); codigo(err) != "INVALID_RPC_CALL" {
		t.Errorf("Respond sin handler: %v", err)
	}
	// `Publish` no puede escribir en `rpc.`: un evento ahí no pasaría por quien responde.
	_, err := caller.Publish(ctx, "rpc.clientes.cliente.v1.obtener", map[string]any{})
	if codigo(err) != "RESERVED_DOMAIN" {
		t.Errorf("Publish en rpc.: %v", err)
	}
	// Dos `Respond` de la misma operación en un bus se repartirían las peticiones entre dos handlers: se rechaza.
	crm, _, _ := env.connectAs("crm-api", nil)
	responde(t, crm, opCalcular, calcula)
	if _, err := crm.Respond(ctx, opCalcular, calcula); codigo(err) != "INVALID_RPC_CALL" {
		t.Errorf("segundo Respond de la misma operación: %v", err)
	}
}

// ─── los permisos que se quitan SE NOTAN ─────────────────────────────────────

// Quien llama sin permiso de PUBLICAR en la operación recibe PERMISSION_DENIED en milisegundos, con el subject EXACTO y la pista de
// dónde arreglarlo; no un RPC_TIMEOUT, que es RETRYABLE y haría que el handler que llama reintentase algo que nunca va a funcionar
// (11-request-reply.md §7.1). Sin inbox propio pasa lo mismo con la SUSCRIPCIÓN a las respuestas.
func TestRPCSinPermisoQuienLlamaVePermissionDenied(t *testing.T) {
	env := startEnv(t, envOptions{Variants: []variant{
		{Name: "pedidos-sin-publicar", Base: "pedidos-api", Drop: []string{"rpc." + opCalcular}},
		{Name: "pedidos-sin-inbox", Base: "pedidos-api", Subscribe: []string{"_INBOX.otro-servicio.>"}},
	}})
	crm, _, _ := env.connectAs("crm-api", nil)
	var ejecuciones atomic.Int32
	responde(t, crm, opCalcular, func(context.Context, RPCRequest) (any, error) { ejecuciones.Add(1); return respuestaCalcular(), nil })
	ctx := ctxT(t, 60*time.Second)

	sinPublicar, _, err := env.connectAs("pedidos-sin-publicar", nil)
	if err != nil {
		t.Fatal(err)
	}
	inicio := time.Now()
	_, err = sinPublicar.Request(ctx, opCalcular, peticionCalcular())
	var pd *PermissionDeniedError
	if !errors.As(err, &pd) || codigo(err) != "PERMISSION_DENIED" || claseDe(err) != ClassPermanent || pd.Subject != "rpc."+opCalcular ||
		pd.Operation != "publish" {
		t.Fatalf("sin permiso de publicar: %v", err)
	}
	if d := time.Since(inicio); d > time.Second {
		t.Errorf("PERMISSION_DENIED tardó %s: debe llegar en milisegundos, no esperar el timeout de 5 s", d)
	}
	t.Logf("MEDIDA PERMISSION_DENIED de una publicación denegada: %s", time.Since(inicio))
	if !strings.Contains(err.Error(), "calls") {
		t.Errorf("la pista dice dónde se arregla (`calls` del manifiesto): %v", err)
	}
	if ejecuciones.Load() != 0 {
		t.Errorf("una petición denegada no llega al dueño: %d ejecuciones", ejecuciones.Load())
	}

	sinInbox, _, err := env.connectAs("pedidos-sin-inbox", nil)
	if err != nil {
		t.Fatal(err)
	}
	inicio = time.Now()
	_, err = sinInbox.Request(ctx, opCalcular, peticionCalcular())
	if !errors.As(err, &pd) || codigo(err) != "PERMISSION_DENIED" || pd.Operation != "subscribe" ||
		!strings.HasPrefix(pd.Subject, "_INBOX.pedidos-api.") {
		t.Fatalf("sin permiso sobre su inbox: %v", err)
	}
	if d := time.Since(inicio); d > time.Second {
		t.Errorf("PERMISSION_DENIED del inbox tardó %s", d)
	}
	t.Logf("MEDIDA PERMISSION_DENIED de la suscripción al inbox: %s", time.Since(inicio))
	if !strings.Contains(err.Error(), "_INBOX.pedidos-api.>") {
		t.Errorf("la pista nombra el inbox privado: %v", err)
	}
}

// Un dueño sin permiso de suscripción parece un servicio CAÍDO —quien llama ve `no responders`—: `Respond` FALLA al arrancar con
// PERMISSION_DENIED, nombrando el subject, en vez de arrancar mudo (A-73). Y uno sin `allow_responses` ejecuta pero no puede
// contestar: quien llama ve un RPC_TIMEOUT, y el dueño lo dice en su log UNA vez (las respuestas se deniegan de una en una).
func TestRPCUnDuenoSinSuscripcionFallaAlArrancarYSinAllowResponsesLoRegistra(t *testing.T) {
	env := startEnv(t, envOptions{Variants: []variant{
		{Name: "crm-sin-suscripcion", Base: "crm-api", Drop: []string{"rpc.clientes.>"}},
		{Name: "crm-sin-allow-responses", Base: "crm-api", DropAllowResponses: true},
	}})
	ctx := ctxT(t, 60*time.Second)
	caller, _, _ := env.connectAs("pedidos-api", nil)

	sin, _, err := env.connectAs("crm-sin-suscripcion", nil)
	if err != nil {
		t.Fatal(err)
	}
	inicio := time.Now()
	_, err = sin.Respond(ctx, opCalcular, calcula)
	var pd *PermissionDeniedError
	if !errors.As(err, &pd) || pd.Operation != "subscribe" || pd.Subject != "rpc."+opCalcular {
		t.Fatalf("Respond sin permiso de suscripción: %v", err)
	}
	if d := time.Since(inicio); d > 2*time.Second {
		t.Errorf("Respond tardó %s en fallar", d)
	}
	t.Logf("MEDIDA Respond sin permiso de suscripción: %s", time.Since(inicio))
	if !strings.Contains(err.Error(), "rpc.<dominio>.>") || !strings.Contains(err.Error(), "owns") {
		t.Errorf("la pista dice qué permiso falta y dónde se concede: %v", err)
	}
	// Y como no se registró, quien llama ve que no hay nadie.
	if _, err := caller.Request(ctx, opCalcular, peticionCalcular()); codigo(err) != "RPC_UNAVAILABLE" {
		t.Errorf("sin responder: %v", err)
	}

	noContesta, sink, err := env.connectAs("crm-sin-allow-responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	var ejecuciones atomic.Int32
	responde(t, noContesta, opCalcular, func(context.Context, RPCRequest) (any, error) { ejecuciones.Add(1); return respuestaCalcular(), nil })
	for i := 0; i < 3; i++ {
		_, err = caller.Request(ctx, opCalcular, peticionCalcular(), WithRPCTimeout(700*time.Millisecond))
		if codigo(err) != "RPC_TIMEOUT" {
			t.Fatalf("sin allow_responses quien llama solo puede ver un timeout: %v", err)
		}
	}
	if ejecuciones.Load() != 3 {
		t.Errorf("el handler SÍ se ejecutó: %d", ejecuciones.Load())
	}
	espera(t, func() bool { return sink.Contains("allow_responses") }, "el log del dueño que no puede contestar")
	if n := sink.Count("allow_responses"); n != 1 {
		t.Errorf("el aviso se da UNA vez, no una por respuesta: %d\n%s", n, sink.text())
	}
	if !strings.Contains(sink.text(), "_INBOX.pedidos-api.>") || !strings.Contains(sink.text(), "RPC_TIMEOUT") {
		t.Errorf("el aviso dice qué se denegó y qué verá quien llama:\n%s", sink.text())
	}
}

func espera(t *testing.T, cond func() bool, que string) {
	t.Helper()
	fin := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(fin) {
			t.Fatalf("no ocurrió a tiempo: %s", que)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ─── la parada ordenada y la concurrencia ────────────────────────────────────

// PARADA ORDENADA (A-83): con un handler de 400 ms, parar a los 100 ms deja que la petición en vuelo TERMINE (Drain de ESA suscripción)
// y la siguiente falla en milisegundos con RPC_UNAVAILABLE. Y `Bus.Close()` con un handler en vuelo tampoco pierde su respuesta:
// `nc.Drain()` de la conexión entera la perdería (`connection closed`).
func TestRPCPararEnOrdenNoPierdeLoQueEstaEnVuelo(t *testing.T) {
	env := startEnv(t, envOptions{})
	caller, _, _ := env.connectAs("pedidos-api", nil)
	ctx := ctxT(t, 60*time.Second)
	lento := func(context.Context, RPCRequest) (any, error) {
		time.Sleep(400 * time.Millisecond)
		return respuestaCalcular(), nil
	}

	for _, porClose := range []bool{false, true} {
		crm, _, _ := env.connectAs("crm-api", nil)
		r, err := crm.Respond(ctx, opCalcular, lento)
		if err != nil {
			t.Fatal(err)
		}
		resultado := make(chan error, 1)
		go func() {
			_, err := caller.Request(ctx, opCalcular, peticionCalcular())
			resultado <- err
		}()
		time.Sleep(100 * time.Millisecond)
		inicio := time.Now()
		if porClose {
			if err := crm.Close(); err != nil {
				t.Fatal(err)
			}
		} else if err := r.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(inicio); d < 200*time.Millisecond {
			t.Errorf("(Close=%v) la parada debe ESPERAR al handler en vuelo: volvió en %s", porClose, d)
		}
		select {
		case err := <-resultado:
			if err != nil {
				t.Fatalf("(Close=%v) la petición en vuelo debe acabar bien, y fue: %v", porClose, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("(Close=%v) la petición en vuelo no acabó", porClose)
		}
		inicio = time.Now()
		_, err = caller.Request(ctx, opCalcular, peticionCalcular())
		if d := time.Since(inicio); d > 100*time.Millisecond || codigo(err) != "RPC_UNAVAILABLE" {
			t.Errorf("(Close=%v) la petición NUEVA tras parar: %s, %v", porClose, d, err)
		}
	}
}

// Lo que ya llegó a la cola de la suscripción cuando se para TAMBIÉN se atiende: `Drain` de la suscripción entrega lo pendiente, un
// `Unsubscribe` lo tiraría y quien llamó vería un timeout. Con un solo handler a la vez y cinco peticiones, cuatro esperan en la cola.
func TestRPCPararConPeticionesEnLaColaLasAtiendeTodas(t *testing.T) {
	env := startEnv(t, envOptions{})
	crm, _, _ := env.connectAs("crm-api", nil)
	var atendidas atomic.Int32
	r, err := crm.Respond(ctxT(t, 20*time.Second), opCalcular, func(context.Context, RPCRequest) (any, error) {
		time.Sleep(100 * time.Millisecond)
		atendidas.Add(1)
		return respuestaCalcular(), nil
	}, WithMaxConcurrent(1))
	if err != nil {
		t.Fatal(err)
	}
	caller, _, _ := env.connectAs("pedidos-api", nil)
	ctx := ctxT(t, 60*time.Second)
	var wg sync.WaitGroup
	var fallos atomic.Int32
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := caller.Request(ctx, opCalcular, peticionCalcular(), WithRPCTimeout(10*time.Second)); err != nil {
				fallos.Add(1)
			}
		}()
	}
	time.Sleep(60 * time.Millisecond) // una en el handler y cuatro en la cola de la suscripción
	if err := r.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if atendidas.Load() != 5 || fallos.Load() != 0 {
		t.Errorf("atendidas %d de 5, fallos %d: parar no debe perder lo que ya llegó", atendidas.Load(), fallos.Load())
	}
}

// Los handlers concurrentes están acotados (A-89): con `WithMaxConcurrent(4)` y 12 peticiones de 100 ms nunca hay más de 4 a la vez
// y las 12 se contestan; por defecto son 256, y 300 peticiones de 50 ms dan un pico de 256 con las 300 contestadas (el exceso espera
// en la cola de la suscripción, sin perderse).
func TestRPCLosHandlersConcurrentesEstanAcotados(t *testing.T) {
	env := startEnv(t, envOptions{})
	caller, _, _ := env.connectAs("pedidos-api", nil)
	ctx := ctxT(t, 120*time.Second)

	medir := func(max *int, opts []RespondOption, n int, dur time.Duration) (pico int32, ok int32) {
		crm, _, _ := env.connectAs("crm-api", nil)
		var enVuelo, p atomic.Int32
		r, err := crm.Respond(ctx, opCalcular, func(context.Context, RPCRequest) (any, error) {
			v := enVuelo.Add(1)
			for {
				actual := p.Load()
				if v <= actual || p.CompareAndSwap(actual, v) {
					break
				}
			}
			time.Sleep(dur)
			enVuelo.Add(-1)
			return respuestaCalcular(), nil
		}, opts...)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Stop(ctx) }()
		var wg sync.WaitGroup
		var contestadas atomic.Int32
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := caller.Request(ctx, opCalcular, peticionCalcular(), WithRPCTimeout(20*time.Second)); err == nil {
					contestadas.Add(1)
				}
			}()
		}
		wg.Wait()
		return p.Load(), contestadas.Load()
	}

	pico, ok := medir(nil, []RespondOption{WithMaxConcurrent(4)}, 12, 100*time.Millisecond)
	if pico > 4 || pico < 2 || ok != 12 {
		t.Errorf("WithMaxConcurrent(4): pico %d, contestadas %d de 12", pico, ok)
	}
	pico, ok = medir(nil, nil, 300, 50*time.Millisecond)
	if pico > int32(RPCMaxConcurrentHandlers) || pico < 128 || ok != 300 {
		t.Errorf("por defecto: pico %d (tope %d), contestadas %d de 300", pico, RPCMaxConcurrentHandlers, ok)
	}
	if _, err := caller.Respond(ctx, opCalcular, calcula, WithMaxConcurrent(0)); codigo(err) != "INVALID_RPC_CALL" {
		t.Errorf("WithMaxConcurrent(0): %v", err)
	}
}

// ─── el tamaño ───────────────────────────────────────────────────────────────

// Una PETICIÓN de más de 1 MiB falla ANTES de salir (RPC_MESSAGE_TOO_LARGE) y una RESPUESTA que no cabe la contesta quien responde con
// ese mismo código: con `nats.go` la publicación fallaría EN QUIEN RESPONDE y quien llama solo vería un timeout (A-65).
func TestRPCLoQueNoCabeDaRPCMessageTooLargeNoUnTimeout(t *testing.T) {
	env := startEnv(t, envOptions{})
	spy, _ := env.opsConn()
	visto, _ := spy.SubscribeSync("rpc." + opCalcular)
	spy.Flush()
	crm, sink, _ := env.connectAs("crm-api", nil)
	grande := strings.Repeat("a", MaxMessageBytes)
	var modo atomic.Int32
	responde(t, crm, opCalcular, func(context.Context, RPCRequest) (any, error) {
		if modo.Load() == 1 {
			return map[string]any{"descuentoCents": 1, "moneda": "EUR", "motivo": grande}, nil
		}
		return respuestaCalcular(), nil
	})
	caller, _, _ := env.connectAs("pedidos-api", nil)
	ctx := ctxT(t, 60*time.Second)

	inicio := time.Now()
	_, err := caller.Request(ctx, opCalcular, map[string]any{"clienteId": "cli-1", "totalCents": 1, "moneda": "EUR", "x": grande})
	var tl *RPCMessageTooLargeError
	if !errors.As(err, &tl) || codigo(err) != "RPC_MESSAGE_TOO_LARGE" || claseDe(err) != ClassPermanent {
		t.Fatalf("una petición que no cabe: %v", err)
	}
	if d := time.Since(inicio); d > time.Second {
		t.Errorf("debe fallar antes de salir: %s", d)
	}
	if m, err := visto.NextMsg(200 * time.Millisecond); err == nil {
		t.Errorf("la petición demasiado grande NO debe llegar a la red: %d bytes", len(m.Data))
	}

	modo.Store(1)
	inicio = time.Now()
	_, err = caller.Request(ctx, opCalcular, peticionCalcular())
	var rr *RPCRemoteError
	if !errors.As(err, &rr) || rr.FluxCode() != "RPC_MESSAGE_TOO_LARGE" || rr.FluxClass() != ClassPermanent {
		t.Fatalf("una respuesta que no cabe: %v", err)
	}
	if d := time.Since(inicio); d > time.Second {
		t.Errorf("quien llama lo ve en milisegundos, NO tras el timeout: %s", d)
	}
	if !sink.Contains("no cabe") {
		t.Errorf("quien responde lo dice en su log:\n%s", sink.text())
	}
}

// ─── la firma ────────────────────────────────────────────────────────────────

func firmadoPor(clave KeyPair, id string, publicas map[string]string) func(*ConnectOptions) {
	return func(o *ConnectOptions) {
		o.Signing = SigningOptions{PrivateKeyPEM: clave.PrivateKeyPEM, KeyID: id, PublicKeys: publicas, Verify: VerifyRequire}
	}
}

// peticionBruta construye a mano el sobre de una petición (lo que haría un cliente que no usa el SDK) con el `time` que se pida.
func peticionBruta(t *testing.T, op, id, tiempo string, data any) Event {
	t.Helper()
	e, err := BuildRPCRequest(RPCRequestInput{Operation: op, TenantID: "acme", ID: id, Source: "/test/otro", Time: tiempo,
		CorrelationID: id, ProducerVersion: "1.0.0", Classification: ClassificationInternal, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func datosDeError(t *testing.T, e Event) RPCErrorData {
	t.Helper()
	if e.Type != RPCErrorType {
		t.Fatalf("no es un error: %s", e.Type)
	}
	return parseRPCErrorData(e.Data)
}

// Con la firma en `require` a los dos lados: la ida y la vuelta van firmadas; una petición SIN firmar o con el `time` fuera de la
// ventana de 300 s la rechaza quien responde con su error, y quien llama verifica la respuesta (11-request-reply.md §7.4).
func TestRPCFirmaRequireALosDosLadosYUnaPeticionVieja(t *testing.T) {
	env := startEnv(t, envOptions{})
	kp1, _ := GenerateKeyPair()
	kp2, _ := GenerateKeyPair()
	publicas := map[string]string{"pedidos-api-1": kp1.PublicKeyPEM, "crm-api-1": kp2.PublicKeyPEM}
	metricas := NewInMemoryMetrics()
	var ejecuciones atomic.Int32
	crm, _, _ := env.connectAs("crm-api", func(o *ConnectOptions) { firmadoPor(kp2, "crm-api-1", publicas)(o); o.Metrics = metricas })
	responde(t, crm, opCalcular, func(context.Context, RPCRequest) (any, error) { ejecuciones.Add(1); return respuestaCalcular(), nil })
	spy, _ := env.opsConn()
	pet, _ := spy.SubscribeSync("rpc." + opCalcular)
	res, _ := spy.SubscribeSync("_INBOX.pedidos-api.>")
	spy.Flush()
	ctx := ctxT(t, 60*time.Second)

	caller, _, _ := env.connectAs("pedidos-api", firmadoPor(kp1, "pedidos-api-1", publicas))
	if _, err := caller.Request(ctx, opCalcular, peticionCalcular()); err != nil {
		t.Fatalf("firmada de ida y de vuelta: %v", err)
	}
	m, _ := pet.NextMsg(5 * time.Second)
	if e, _ := ParseEvent(m.Data); e.SignKeyID != "pedidos-api-1" || e.Signature == "" {
		t.Errorf("la petición va firmada: %+v", e)
	}
	m, _ = res.NextMsg(5 * time.Second)
	if e, _ := ParseEvent(m.Data); e.SignKeyID != "crm-api-1" || e.Signature == "" {
		t.Errorf("la respuesta va firmada: %+v", e)
	}

	// Sin firma: la rechaza quien responde, y el handler NO se ejecuta.
	sinFirma, _, _ := env.connectAs("pedidos-api", nil)
	antes := ejecuciones.Load()
	_, err := sinFirma.Request(ctx, opCalcular, peticionCalcular())
	var rr *RPCRemoteError
	if !errors.As(err, &rr) || rr.FluxCode() != "MISSING_SIGNATURE" || rr.FluxClass() != ClassPermanent {
		t.Fatalf("petición sin firma: %v", err)
	}
	if ejecuciones.Load() != antes {
		t.Error("una petición sin firma NO ejecuta el handler")
	}

	// Una petición vieja pero bien firmada: STALE_REQUEST (la firma por sí sola no impide reenviarla).
	firmante, err := NewSigner(SigningOptions{PrivateKeyPEM: kp1.PrivateKeyPEM, KeyID: "pedidos-api-1"})
	if err != nil {
		t.Fatal(err)
	}
	vieja := peticionBruta(t, opCalcular, "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10", FormatTime(time.Now().Add(-10*time.Minute)), peticionCalcular())
	vieja, _ = firmante.Sign(vieja)
	bytes, _ := Serialize(vieja)
	respuesta, err := spy.Request("rpc."+opCalcular, bytes, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := ParseEvent(respuesta.Data)
	if err != nil {
		t.Fatal(err)
	}
	if d := datosDeError(t, ev); d.Code != "STALE_REQUEST" || d.Class != "permanent" {
		t.Errorf("una petición de hace 10 minutos: %+v", d)
	}
	if ev.CausationID != vieja.ID {
		t.Errorf("el error apunta a la petición: causationid=%q", ev.CausationID)
	}
	if ejecuciones.Load() != antes {
		t.Error("STALE_REQUEST no ejecuta el handler")
	}
	// Y una petición RECIENTE bien firmada, hecha a mano, sí pasa (la ventana es simétrica, no un «siempre rechazar»).
	reciente := peticionBruta(t, opCalcular, "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e11", FormatTime(time.Now()), peticionCalcular())
	reciente, _ = firmante.Sign(reciente)
	bytes, _ = Serialize(reciente)
	if respuesta, err = spy.Request("rpc."+opCalcular, bytes, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if ev, _ = ParseEvent(respuesta.Data); ev.Type == RPCErrorType {
		t.Errorf("una petición reciente y firmada no es un error: %+v", datosDeError(t, ev))
	}

	out := metricas.Render()
	for _, linea := range []string{
		`flux_rpc_server_requests_total{operation="` + opCalcular + `",outcome="invalid_signature"} 1`,
		`flux_rpc_server_requests_total{operation="` + opCalcular + `",outcome="stale_request"} 1`,
	} {
		if !strings.Contains(out, linea) {
			t.Errorf("falta %s en:\n%s", linea, out)
		}
	}

	// Quien llama VERIFICA la respuesta: una respuesta sin firma de un responder en bruto es un fallo PERMANENT.
	raw, _ := env.opsConn()
	if _, err := raw.Subscribe("rpc."+opObtener, func(m *nats.Msg) {
		req, _ := ParseEvent(m.Data)
		r, _ := BuildRPCReply(NewRPCReplyInput(refOf(opObtener, req), "01924f91-5d0f-7e03-9b6a-1c2d3e4f5a67", "/x/y", FormatTime(time.Now()), "1", "",
			ClassificationInternal, map[string]any{"clienteId": "cli-1", "nombre": "Ana", "email": "ana@example.com"}, ""))
		b, _ := Serialize(r)
		_ = m.Respond(b)
	}); err != nil {
		t.Fatal(err)
	}
	raw.Flush()
	fac, _, _ := env.connectAs("facturacion-api", firmadoPor(kp1, "pedidos-api-1", publicas))
	_, err = fac.Request(ctx, opObtener, map[string]any{"clienteId": "cli-1"})
	if !errors.As(err, &rr) || rr.FluxCode() != "MISSING_SIGNATURE" || rr.FluxClass() != ClassPermanent {
		t.Errorf("una respuesta sin firma con `require`: %v", err)
	}
}

// ─── lo que quien responde no puede leer ─────────────────────────────────────

// Una petición SIN subject de respuesta (`nats pub` a mano) NO se atiende —nadie espera el resultado de una orden— y es POISON; una
// que no es un CloudEvent se contesta con un error `permanent` con el código del sobre, y una de otro `type` con INVALID_RPC_CALL.
func TestRPCUnaPeticionIlegibleOSinReplyEsPoison(t *testing.T) {
	env := startEnv(t, envOptions{})
	var poison []string
	var mu sync.Mutex
	metricas := NewInMemoryMetrics()
	crm, _, _ := env.connectAs("crm-api", func(o *ConnectOptions) {
		o.Metrics = metricas
		o.OnPoison = func(p PoisonInfo) {
			mu.Lock()
			defer mu.Unlock()
			if len(p.Raw) != 0 {
				t.Errorf("`Raw` va vacío: una petición puede llevar datos personales")
			}
			poison = append(poison, p.Subject+" "+codigo(p.Err))
		}
	})
	var ejecuciones atomic.Int32
	responde(t, crm, opCalcular, func(context.Context, RPCRequest) (any, error) { ejecuciones.Add(1); return respuestaCalcular(), nil })
	spy, _ := env.opsConn()

	// Sin reply.
	valida := peticionBruta(t, opCalcular, "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10", FormatTime(time.Now()), peticionCalcular())
	bytes, _ := Serialize(valida)
	if err := spy.Publish("rpc."+opCalcular, bytes); err != nil {
		t.Fatal(err)
	}
	spy.Flush()
	espera(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(poison) == 1 }, "POISON de la petición sin reply")
	if ejecuciones.Load() != 0 {
		t.Error("una petición sin subject de respuesta NO se atiende")
	}

	// No es un CloudEvent: error `permanent` con el código del sobre; `tenantid: system` y `correlationid` propio.
	_, esperado := ParseEvent([]byte("esto no es json"))
	m, err := spy.Request("rpc."+opCalcular, []byte("esto no es json"), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := ParseEvent(m.Data)
	if err != nil {
		t.Fatalf("el error es un CloudEvent válido: %v", err)
	}
	d := datosDeError(t, ev)
	if d.Class != "permanent" || d.Code != codigo(esperado) || ev.TenantID != "system" || ev.CorrelationID != ev.ID || ev.CausationID != "" {
		t.Errorf("error de una petición ilegible: %+v / tenant=%q correlationid=%q causationid=%q (código esperado %s)", d, ev.TenantID, ev.CorrelationID, ev.CausationID, codigo(esperado))
	}
	// Con un `id` legible lo cita como `causationid`: es lo que quien llama reconoce como suyo.
	m, err = spy.Request("rpc."+opCalcular, []byte(`{"id":"mi-peticion-7","nada":"más"}`), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ev, _ = ParseEvent(m.Data); ev.CausationID != "mi-peticion-7" || ev.TenantID != "system" {
		t.Errorf("el `id` legible es el `causationid`: %q", ev.CausationID)
	}
	// Otro `type`: INVALID_RPC_CALL, y ESE error sí hereda la identidad de la petición.
	otra := valida
	otra.Type = "com.flux.rpc.clientes.cliente.obtener.request.v1"
	bytes, _ = Serialize(otra)
	if m, err = spy.Request("rpc."+opCalcular, bytes, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	ev, _ = ParseEvent(m.Data)
	if d := datosDeError(t, ev); d.Code != "INVALID_RPC_CALL" || d.Class != "permanent" || ev.CausationID != valida.ID || ev.TenantID != "acme" {
		t.Errorf("otro `type`: %+v causationid=%q", d, ev.CausationID)
	}
	if ejecuciones.Load() != 0 {
		t.Error("nada de eso ejecuta el handler")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(poison) != 4 || !strings.HasPrefix(poison[0], "rpc."+opCalcular+" NO_REPLY_SUBJECT") {
		t.Errorf("OnPoison: %v", poison)
	}
	if out := metricas.Render(); !strings.Contains(out, `flux_rpc_server_requests_total{operation="`+opCalcular+`",outcome="poison"} 4`) {
		t.Errorf("el servidor cuenta 4 `poison`:\n%s", out)
	}
}

// ─── lo que llega a quien llama y no es SU respuesta ─────────────────────────

// Las cinco comprobaciones de §4.3: lo que llega como respuesta y no es la respuesta a ESTA petición es RPC_INVALID_REPLY —PERMANENT,
// un incidente— y no un dato; con la ÚNICA excepción del error de una petición que quien responde no pudo leer.
func TestRPCUnaRespuestaQueNoEsDeEstaPeticionEsRPCInvalidReply(t *testing.T) {
	env := startEnv(t, envOptions{})
	spy, _ := env.opsConn()
	var mutar atomic.Value
	if _, err := spy.Subscribe("rpc."+opObtener, func(m *nats.Msg) {
		req, err := ParseEvent(m.Data)
		if err != nil {
			return
		}
		f := mutar.Load().(func(req Event) []byte)
		if b := f(req); b != nil {
			_ = m.Respond(b)
		}
	}); err != nil {
		t.Fatal(err)
	}
	spy.Flush()
	caller, _, _ := env.connectAs("facturacion-api", nil)
	ctx := ctxT(t, 60*time.Second)
	nuevoID := func() string { return "01924f91-5d0f-7e03-9b6a-1c2d3e4f5a67" }
	cuerpo := map[string]any{"clienteId": "cli-1", "nombre": "Ana", "email": "ana@example.com"}
	buena := func(req Event) Event {
		r, err := BuildRPCReply(NewRPCReplyInput(refOf(opObtener, req), nuevoID(), "/x/y", FormatTime(time.Now()), "1", "", ClassificationInternal, cuerpo, ""))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	conError := func(req Event, d RPCErrorData) Event {
		r, err := BuildRPCError(NewRPCErrorInput(refOf(opObtener, req), nuevoID(), "/x/y", FormatTime(time.Now()), "1", "", d))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	ser := func(e Event) []byte { b, _ := Serialize(e); return b }

	llamar := func(f func(req Event) []byte) error {
		mutar.Store(f)
		_, err := caller.Request(ctx, opObtener, map[string]any{"clienteId": "cli-1"}, WithRPCTimeout(2*time.Second))
		return err
	}
	if err := llamar(func(req Event) []byte { return ser(buena(req)) }); err != nil {
		t.Fatalf("la respuesta buena: %v", err)
	}

	malas := []struct {
		nombre string
		f      func(req Event) []byte
		pista  string
	}{
		{"no es un CloudEvent", func(Event) []byte { return []byte("hola") }, "CloudEvent"},
		{"el type es de otra operación", func(req Event) []byte {
			r := buena(req)
			r.Type = "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1"
			return ser(r)
		}, "type"},
		{"el type es el de la petición", func(req Event) []byte { return ser(req) }, "type"},
		{"el causationid no es el id de la petición", func(req Event) []byte {
			r := buena(req)
			r.CausationID = "otra-peticion"
			return ser(r)
		}, "causationid"},
		{"sin causationid", func(req Event) []byte {
			r := buena(req)
			r.CausationID = ""
			return ser(r)
		}, "causationid"},
		{"otro correlationid", func(req Event) []byte {
			r := buena(req)
			r.CorrelationID = "otra-cadena"
			return ser(r)
		}, "correlationid"},
		{"otro tenantid", func(req Event) []byte {
			r := buena(req)
			r.TenantID = "globex"
			return ser(r)
		}, "tenantid"},
		{"un error cuyo data no cumple su esquema (poison no viaja)", func(req Event) []byte {
			r := conError(req, RPCErrorData{Class: "permanent", Code: "X", Message: "m"})
			r.Data = []byte(`{"class":"poison","code":"X","message":"m"}`)
			return ser(r)
		}, "esquema"},
		{"un error con un atributo de más", func(req Event) []byte {
			r := conError(req, RPCErrorData{Class: "permanent", Code: "X", Message: "m"})
			r.Data = []byte(`{"class":"permanent","code":"X","message":"m","extra":1}`)
			return ser(r)
		}, "esquema"},
		{"el acuse de un stream (algún stream captura rpc.>)", func(Event) []byte { return []byte(`{"stream":"EVT_X","seq":42}`) }, "flux doctor"},
		{"tenantid system sin ser el error de una petición ilegible", func(req Event) []byte {
			r := conError(req, RPCErrorData{Class: "permanent", Code: "X", Message: "m"})
			r.TenantID = "system" // pero su correlationid NO es su propio id
			return ser(r)
		}, "tenantid"},
	}
	for _, c := range malas {
		err := llamar(c.f)
		var ir *RPCInvalidReplyError
		if !errors.As(err, &ir) || codigo(err) != "RPC_INVALID_REPLY" || claseDe(err) != ClassPermanent {
			t.Errorf("%s: %v", c.nombre, err)
			continue
		}
		if !strings.Contains(err.Error(), c.pista) {
			t.Errorf("%s: el mensaje debe decir por qué (%q): %v", c.nombre, c.pista, err)
		}
		if strings.Contains(err.Error(), "ana@example.com") {
			t.Errorf("%s: el mensaje NO cita datos", c.nombre)
		}
	}

	// LA EXCEPCIÓN ÚNICA (§4.3.4): el error de una petición que quien responde no pudo leer no tiene nada que heredar —tenantid
	// `system`, correlationid = su propio id— y se atribuye por el `causationid`: quien llama ve su CÓDIGO, no RPC_INVALID_REPLY.
	err := llamar(func(req Event) []byte {
		ilegible := ref(req)
		ilegible.TenantID, ilegible.CorrelationID = "system", nuevoID()
		r, _ := BuildRPCError(NewRPCErrorInput(ilegible, nuevoID(), "/x/y", FormatTime(time.Now()), "1", "",
			RPCErrorData{Class: "permanent", Code: "MALFORMED_JSON", Message: "la petición no se pudo leer"}))
		return ser(r)
	})
	var rr *RPCRemoteError
	if !errors.As(err, &rr) || rr.FluxCode() != "MALFORMED_JSON" || rr.FluxClass() != ClassPermanent {
		t.Errorf("el error de una petición ilegible: %v", err)
	}
	// Un error ajeno bien formado se traduce a su clase y su código, con `retryAfterMs`.
	cero := int64(0)
	err = llamar(func(req Event) []byte {
		return ser(conError(req, RPCErrorData{Class: "retryable", Code: "HTTP_503", Message: "no responde", RetryAfterMs: &cero}))
	})
	if !errors.As(err, &rr) || rr.FluxCode() != "HTTP_503" || rr.FluxClass() != ClassRetryable {
		t.Errorf("un error retryable ajeno: %v", err)
	}
}

func ref(req Event) RPCRequestRef { return refOf(opObtener, req) }

// ─── las métricas ────────────────────────────────────────────────────────────

// Cliente y servidor cuentan lo mismo, con `operation` como única etiqueta de identidad: nunca el tenant, el `id`, el
// `correlationid` ni quién llama (una serie por cada uno).
func TestRPCLasMetricasDeUnCicloReal(t *testing.T) {
	env := startEnv(t, envOptions{})
	mc, ms := NewInMemoryMetrics(), NewInMemoryMetrics()
	crm, _, _ := env.connectAs("crm-api", func(o *ConnectOptions) { o.Metrics = ms })
	var n atomic.Int32
	responde(t, crm, opCalcular, func(context.Context, RPCRequest) (any, error) {
		switch n.Add(1) {
		case 1, 2:
			return respuestaCalcular(), nil
		case 3:
			return nil, NewPermanentError("no", WithCode("NO"))
		case 4:
			return nil, NewRetryableError("luego", WithCode("LUEGO"))
		}
		time.Sleep(300 * time.Millisecond)
		return respuestaCalcular(), nil
	})
	caller, _, _ := env.connectAs("pedidos-api", func(o *ConnectOptions) { o.Metrics = mc })
	ctx := ctxT(t, 60*time.Second)
	for i := 0; i < 4; i++ {
		_, _ = caller.Request(ctx, opCalcular, peticionCalcular())
	}
	_, _ = caller.Request(ctx, opCalcular, peticionCalcular(), WithRPCTimeout(100*time.Millisecond)) // timeout
	time.Sleep(400 * time.Millisecond)
	fac, _, _ := env.connectAs("facturacion-api", func(o *ConnectOptions) { o.Metrics = mc })
	_, _ = fac.Request(ctx, opObtener, map[string]any{"clienteId": "x"}) // nadie la atiende: unavailable
	_, _ = caller.Request(ctx, "rpc."+opCalcular, peticionCalcular())    // INVALID: no se cuenta

	cl, sv := mc.Render(), ms.Render()
	op := `operation="` + opCalcular + `"`
	for _, linea := range []string{
		`flux_rpc_client_requests_total{` + op + `,outcome="ok"} 2`,
		`flux_rpc_client_requests_total{` + op + `,outcome="permanent"} 1`,
		`flux_rpc_client_requests_total{` + op + `,outcome="retryable"} 1`,
		`flux_rpc_client_requests_total{` + op + `,outcome="timeout"} 1`,
		`flux_rpc_client_requests_total{operation="` + opObtener + `",outcome="unavailable"} 1`,
		`flux_rpc_duration_seconds_count{` + op + `,role="client"} 5`,
		"# TYPE flux_rpc_duration_seconds histogram",
	} {
		if !strings.Contains(cl, linea) {
			t.Errorf("cliente: falta %s en:\n%s", linea, cl)
		}
	}
	for _, linea := range []string{
		`flux_rpc_server_requests_total{` + op + `,outcome="ok"} 3`, // dos buenas y la lenta, que acaba aunque quien llama ya no espere
		`flux_rpc_server_requests_total{` + op + `,outcome="permanent"} 1`,
		`flux_rpc_server_requests_total{` + op + `,outcome="retryable"} 1`,
		`flux_rpc_duration_seconds_count{` + op + `,role="server"} 5`,
	} {
		if !strings.Contains(sv, linea) {
			t.Errorf("servidor: falta %s en:\n%s", linea, sv)
		}
	}
	if strings.Contains(cl, "RESERVED") || strings.Contains(cl, "invalid_rpc") {
		t.Errorf("INVALID_RPC_CALL y RESERVED_DOMAIN no se cuentan: son de programación:\n%s", cl)
	}
	for _, prohibida := range []string{"tenant", "correlation", "caller", `service=`, `client=`, `id=`} {
		if strings.Contains(cl, prohibida) || strings.Contains(sv, prohibida) {
			t.Errorf("etiqueta prohibida %q:\n%s\n%s", prohibida, cl, sv)
		}
	}
}

// ─── L3 ──────────────────────────────────────────────────────────────────────

// L3 en petición/respuesta: la petición se valida ANTES de salir, la respuesta al recibirla, y quien responde valida lo que recibe y
// lo que devuelve; el mensaje cita la RUTA y la regla —nunca el valor: una petición puede llevar datos personales— y dice de quién es
// el fallo.
func TestRPCL3ValidaLaPeticionYLaRespuestaYNoCopiaValores(t *testing.T) {
	env := startEnv(t, envOptions{})
	bundle, err := LoadSchemaBundle("../schemas/bundle.json")
	if err != nil {
		t.Fatal(err)
	}
	estricto := func(o *ConnectOptions) {
		o.SchemaBaseURL = ""
		o.Validation = ValidationOptions{Mode: ValidationStrict, Bundle: bundle}
	}
	spy, _ := env.opsConn()
	visto, _ := spy.SubscribeSync("rpc." + opCalcular)
	spy.Flush()
	var ejecuciones atomic.Int32
	var devolver atomic.Value
	devolver.Store(map[string]any{"descuentoCents": 500, "moneda": "EUR"})
	crm, _, _ := env.connectAs("crm-api", estricto)
	responde(t, crm, opCalcular, func(context.Context, RPCRequest) (any, error) {
		ejecuciones.Add(1)
		return devolver.Load(), nil
	})
	caller, _, _ := env.connectAs("pedidos-api", estricto)
	laxo, _, _ := env.connectAs("pedidos-api", nil)
	ctx := ctxT(t, 60*time.Second)

	if _, err := caller.Request(ctx, opCalcular, peticionCalcular()); err != nil {
		t.Fatalf("una petición válida: %v", err)
	}
	m, _ := visto.NextMsg(2 * time.Second)
	if e, _ := ParseEvent(m.Data); e.DataSchema != "https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json" {
		t.Errorf("el dataschema sale del índice `rpc` del bundle: %s", e.DataSchema)
	}

	malo := map[string]any{"clienteId": "cliente con espacios SECRETO", "totalCents": -5, "moneda": "euro", "sobra": "x"}
	// 1. Quien llama valida ANTES de salir: INVALID_SCHEMA, todas las rutas, ningún valor.
	inicio := time.Now()
	_, err = caller.Request(ctx, opCalcular, malo)
	var sve *SchemaValidationError
	if !errors.As(err, &sve) || codigo(err) != "INVALID_SCHEMA" || claseDe(err) != ClassPermanent {
		t.Fatalf("petición inválida con L3 en quien llama: %v", err)
	}
	if time.Since(inicio) > time.Second {
		t.Error("L3 falla antes de la red")
	}
	if m, err := visto.NextMsg(200 * time.Millisecond); err == nil {
		t.Errorf("una petición inválida NO sale: %s", m.Data)
	}
	for _, valor := range []string{"SECRETO", "euro", "-5"} {
		if strings.Contains(err.Error(), valor) {
			t.Errorf("el mensaje cita el valor %q: %v", valor, err)
		}
	}
	for _, ruta := range []string{"/clienteId", "/totalCents", "/moneda", "quien llama"} {
		if !strings.Contains(err.Error(), ruta) {
			t.Errorf("debe citar %s (todas las rutas, y de quién es el fallo): %v", ruta, err)
		}
	}

	// 2. Quien llama NO valida y quien responde sí: la rechaza con su error `permanent` INVALID_SCHEMA, sin ejecutar el handler.
	antes := ejecuciones.Load()
	_, err = laxo.Request(ctx, opCalcular, malo)
	var rr *RPCRemoteError
	if !errors.As(err, &rr) || rr.FluxCode() != "INVALID_SCHEMA" || rr.FluxClass() != ClassPermanent || strings.Contains(rr.Message, "SECRETO") {
		t.Fatalf("quien responde valida la petición: %v", err)
	}
	if ejecuciones.Load() != antes {
		t.Error("una petición inválida NO ejecuta el handler")
	}

	// 3. El handler devuelve algo que incumple el esquema de su respuesta: es SU fallo, y se dice.
	devolver.Store(map[string]any{"descuentoCents": "mucho", "moneda": "EUR"})
	_, err = laxo.Request(ctx, opCalcular, peticionCalcular())
	if !errors.As(err, &rr) || rr.FluxCode() != "INVALID_SCHEMA" || !strings.Contains(rr.Message, "quien responde") || strings.Contains(rr.Message, "mucho") {
		t.Fatalf("una respuesta que incumple su esquema: %v", err)
	}
	devolver.Store(map[string]any{"descuentoCents": 500, "moneda": "EUR"})

	// 4. Quien llama valida la respuesta de un responder que no lo hace (en bruto): INVALID_SCHEMA de quien responde.
	raw, _ := env.opsConn()
	if _, err := raw.Subscribe("rpc."+opObtener, func(m *nats.Msg) {
		req, _ := ParseEvent(m.Data)
		r, _ := BuildRPCReply(NewRPCReplyInput(refOf(opObtener, req), "01924f91-5d0f-7e03-9b6a-1c2d3e4f5a67", "/x/y", FormatTime(time.Now()), "1", "",
			ClassificationInternal, map[string]any{"clienteId": "cli-1", "nombre": "Ana Secreta"}, "")) // falta `email`
		b, _ := Serialize(r)
		_ = m.Respond(b)
	}); err != nil {
		t.Fatal(err)
	}
	raw.Flush()
	fac, _, _ := env.connectAs("facturacion-api", estricto)
	_, err = fac.Request(ctx, opObtener, map[string]any{"clienteId": "cli-1"})
	if !errors.As(err, &sve) || codigo(err) != "INVALID_SCHEMA" || !strings.Contains(err.Error(), "quien responde") || strings.Contains(err.Error(), "Ana Secreta") ||
		!strings.Contains(err.Error(), "email") {
		t.Errorf("la respuesta inválida, vista por quien llama: %v", err)
	}

	// 5. En modo estricto, un servicio al que le falta el esquema de una operación que atiende falla AL ARRANCAR.
	sinIndice := *bundle
	sinIndice.RPC = map[string]RPCSchemas{}
	sinIndice.Schemas = map[string]json.RawMessage{}
	for uri, doc := range bundle.Schemas {
		if !strings.Contains(uri, "/rpc/clientes/cliente/obtener/") {
			sinIndice.Schemas[uri] = doc
		}
	}
	sinEsquema, _, err := env.connectAs("crm-api", func(o *ConnectOptions) {
		o.SchemaBaseURL = ""
		o.Validation = ValidationOptions{Mode: ValidationStrict, Bundle: &sinIndice}
	})
	if err != nil {
		t.Fatal(err)
	}
	var snf *SchemaNotFoundError
	if _, err := sinEsquema.Respond(ctx, opObtener, calcula); !errors.As(err, &snf) {
		t.Errorf("sin los esquemas en el bundle: %v", err)
	}
}
