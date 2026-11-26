package flux

// Seguridad ejecutada contra un `nats-server` REAL, con TLS, la ACL que genera `scripts/gen-acl.mjs` y `flux provision`
// (ver testenv_test.go) — plan v1.1, Fase 1 y Fase 6.
//
// Cada comprobación se hace en los DOS SENTIDOS: lo concedido funciona y lo no concedido falla, y el código que sale es
// el de SU situación y no el de otra (`h_*`: los códigos de Connect(); `g_*`: las denegaciones). Un test que solo se ha
// visto en verde no demuestra que el broker confine.

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	subjectCreado = "pedidos.pedido.v1.creado"
	pullCreado    = "$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado"
	ackCreado     = "$JS.ACK.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.>"
	infoCreado    = "$JS.API.CONSUMER.INFO.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado"
)

func ctxT(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestSeguridadCicloDeEventosBajoLaAclGenerada(t *testing.T) {
	env := startEnv(t, envOptions{})

	pedidos, _, err := env.connectAs("pedidos-api", nil)
	if err != nil {
		t.Fatalf("pedidos-api no conectó: %v", err)
	}
	facturacion, sink, err := env.connectAs("facturacion-api", nil)
	if err != nil {
		t.Fatalf("facturacion-api no conectó: %v", err)
	}

	recibidos := make(chan Event, 4)
	sub, err := facturacion.Subscribe(ctxT(t, 10*time.Second), subjectCreado,
		func(_ context.Context, e Event, d Delivery) error { recibidos <- e; return nil })
	if err != nil {
		t.Fatalf("Subscribe bajo la ACL generada falló: %v", err)
	}
	defer sub.Unsubscribe()

	if _, err := pedidos.Publish(ctxT(t, 10*time.Second), subjectCreado, map[string]any{"pedidoId": "ped-1"},
		WithAggregateID("ped-1")); err != nil {
		t.Fatalf("Publish bajo la ACL generada falló: %v", err)
	}
	select {
	case e := <-recibidos:
		if e.AggregateID != "ped-1" {
			t.Errorf("subject del sobre = %q", e.AggregateID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("el evento no llegó")
	}
	time.Sleep(300 * time.Millisecond)
	if v := env.violations(); len(v) != 0 {
		t.Errorf("el ciclo completo dejó %d violaciones de permiso en el log del servidor (una por arranque es ruido que tapa "+
			"las reales):\n%s\nlogger del SDK:\n%s", len(v), strings.Join(v, "\n"), sink.text())
	}
}

func TestSeguridadSubscribeConElConsumidorAusenteEsNotProvisioned(t *testing.T) {
	env := startEnv(t, envOptions{NoProvision: true})
	b, _, err := env.connectAs("pedidos-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	_, err = b.Subscribe(ctxT(t, 20*time.Second), "inventario.stock.v1.ajustado",
		func(context.Context, Event, Delivery) error { return nil })
	var np *NotProvisionedError
	if !errors.As(err, &np) {
		t.Fatalf("err = %v (%T), se esperaba *NotProvisionedError", err, err)
	}
	if np.FluxCode() != "NOT_PROVISIONED" || np.FluxClass() != ClassPermanent {
		t.Errorf("code/class = %s/%s", np.FluxCode(), np.FluxClass())
	}
	if !strings.Contains(np.Error(), "flux provision") || !strings.Contains(np.Error(), "$JS.API.STREAM.CREATE.EVT_INVENTARIO") {
		t.Errorf("el mensaje debe nombrar el recurso, el subject denegado y `flux provision`: %s", np.Error())
	}
	if d := time.Since(t0); d > 3*time.Second {
		t.Errorf("tardó %v: un NOT_PROVISIONED llega en milisegundos, no por timeout", d)
	}
}

var _ = atomic.Int32{}

// ─── Las denegaciones: g_* ───────────────────────────────────────────────────

func quierePermisoDenegado(t *testing.T, err error, subject string) *PermissionDeniedError {
	t.Helper()
	var pd *PermissionDeniedError
	if !errors.As(err, &pd) {
		t.Fatalf("err = %v (%T), se esperaba *PermissionDeniedError", err, err)
	}
	if pd.FluxCode() != "PERMISSION_DENIED" || pd.FluxClass() != ClassPermanent {
		t.Errorf("code/class = %s/%s", pd.FluxCode(), pd.FluxClass())
	}
	if pd.Subject != subject {
		t.Errorf("subject denegado = %q, se esperaba %q (completo, sin normalizar)", pd.Subject, subject)
	}
	if !strings.Contains(pd.Error(), subject) || !strings.Contains(pd.Error(), "06-security.md") && !strings.Contains(pd.Error(), "sdk-go/README") {
		t.Errorf("el mensaje debe nombrar el subject y apuntar a la ACL: %v", pd)
	}
	var np *NotProvisionedError
	if errors.As(err, &np) {
		t.Errorf("una denegación SIN saber si el recurso falta NO es NOT_PROVISIONED: %v", err)
	}
	return pd
}

func TestG_SubscribeFallaSiSeDeniegaElPullYLoDiceElSubjectCompleto(t *testing.T) {
	env := startEnv(t, envOptions{Variants: []variant{{Name: "fact-sin-pull", Base: "facturacion-api", Drop: []string{pullCreado}}}})

	// En su situación: sin el permiso, `Subscribe` FALLA antes de devolver (no deja un consumidor inerte que parece sano).
	b, sink, err := env.connectAs("fact-sin-pull", nil)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	sub, err := b.Subscribe(ctxT(t, 20*time.Second), subjectCreado, func(context.Context, Event, Delivery) error { return nil })
	if err == nil {
		sub.Unsubscribe()
		t.Fatal("Subscribe devolvió nil con el pull denegado: un consumidor inerte que parece sano")
	}
	pd := quierePermisoDenegado(t, err, pullCreado)
	if !strings.Contains(pd.Error(), "inerte") {
		t.Errorf("debe explicar que el consumidor quedaría inerte: %v", pd)
	}
	if d := time.Since(t0); d > 4*time.Second {
		t.Errorf("tardó %v: la detección NO debe depender de un temporizador largo", d)
	}
	// Y además se registra en el logger, como cualquier otra denegación (06-security.md §3.7).
	time.Sleep(200 * time.Millisecond)
	if !sink.Contains(pullCreado) || !sink.Contains("parece sano") {
		t.Errorf("la denegación debe quedar en el logger con su consecuencia:\n%s", sink.text())
	}
	// No queda nada vivo: la suscripción que falló no se registró.
	b.mu.Lock()
	n := len(b.subscriptions)
	b.mu.Unlock()
	if n != 0 {
		t.Errorf("tras el fallo quedan %d suscripciones registradas", n)
	}

	// En ninguna otra: con el permiso (la identidad sin tocar), el mismo Subscribe funciona.
	ok, _, err := env.connectAs("facturacion-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := ok.Subscribe(ctxT(t, 20*time.Second), subjectCreado, func(context.Context, Event, Delivery) error { return nil })
	if err != nil {
		t.Fatalf("con el permiso concedido Subscribe debe funcionar: %v", err)
	}
	s2.Unsubscribe()
}

func TestG_SinPermisoParaConsultarElConsumidorEsPermissionDeniedYNoNotProvisioned(t *testing.T) {
	env := startEnv(t, envOptions{Variants: []variant{{Name: "fact-sin-info", Base: "facturacion-api", Drop: []string{infoCreado}}}})
	b, _, err := env.connectAs("fact-sin-info", nil)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	_, err = b.Subscribe(ctxT(t, 20*time.Second), subjectCreado, func(context.Context, Event, Delivery) error { return nil })
	quierePermisoDenegado(t, err, infoCreado)
	if d := time.Since(t0); d > 4*time.Second {
		t.Errorf("tardó %v: una denegación llega en ms, no por timeout", d)
	}
}

func TestG_SinPermisoParaConsultarElStreamDeLaDLQEsPermissionDenied(t *testing.T) {
	// A-8b: con la ACL de antes de la 1.1 la consulta del stream de la DLQ no estaba concedida.
	const dlqInfo = "$JS.API.STREAM.INFO.DLQ_PEDIDOS"
	env := startEnv(t, envOptions{Variants: []variant{{Name: "fact-sin-dlq", Base: "facturacion-api", Drop: []string{dlqInfo}}}})
	b, _, err := env.connectAs("fact-sin-dlq", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Subscribe(ctxT(t, 20*time.Second), subjectCreado, func(context.Context, Event, Delivery) error { return nil })
	quierePermisoDenegado(t, err, dlqInfo)
}

func TestG_UnConsumidorQueFaltaYNoSePuedeCrearEsNotProvisioned_ConDevSeCrea(t *testing.T) {
	env := startEnv(t, envOptions{Variants: []variant{{Name: "fact-dev", Base: "facturacion-api", Dev: true}}})
	_, ops := env.opsConn()
	if err := ops.DeleteConsumer(ctxT(t, 10*time.Second), "EVT_PEDIDOS", "facturacion-api__pedidos_pedido_v1_creado"); err != nil {
		t.Fatalf("no se pudo borrar el durable aprovisionado: %v", err)
	}

	// En su situación: el durable falta, y crearlo está denegado (producción): NOT_PROVISIONED, que nombra el consumidor
	// Y su stream y apunta a `flux provision`.
	b, _, err := env.connectAs("facturacion-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	_, err = b.Subscribe(ctxT(t, 20*time.Second), subjectCreado, func(context.Context, Event, Delivery) error { return nil })
	var np *NotProvisionedError
	if !errors.As(err, &np) {
		t.Fatalf("err = %v (%T), se esperaba *NotProvisionedError", err, err)
	}
	if np.Resource != ResourceConsumer || np.Name != "facturacion-api__pedidos_pedido_v1_creado" || np.Stream != "EVT_PEDIDOS" {
		t.Errorf("recurso = %s %q en %q", np.Resource, np.Name, np.Stream)
	}
	if !strings.Contains(np.DeniedSubject, "$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado") {
		t.Errorf("subject denegado = %q", np.DeniedSubject)
	}
	if !strings.Contains(np.Error(), "flux provision") || !strings.Contains(np.Error(), "operación") {
		t.Errorf("debe apuntar a `flux provision` con credenciales de operación: %v", np)
	}
	if d := time.Since(t0); d > 4*time.Second {
		t.Errorf("tardó %v: no es un timeout", d)
	}
	// Un durable que falta NO se recrea en producción (reentregaría hasta 30 días de eventos con deliver_policy all).
	if _, err := ops.Consumer(ctxT(t, 5*time.Second), "EVT_PEDIDOS", "facturacion-api__pedidos_pedido_v1_creado"); err == nil {
		t.Error("el SDK recreó un durable sin permiso para ello")
	}

	// En ninguna otra: con `--dev` (CONSUMER.CREATE del propio durable) el mismo arranque lo crea y funciona.
	dev, _, err := env.connectAs("fact-dev", nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := dev.Subscribe(ctxT(t, 20*time.Second), subjectCreado, func(context.Context, Event, Delivery) error { return nil })
	if err != nil {
		t.Fatalf("con CONSUMER.CREATE concedido (--dev) debe crearlo: %v", err)
	}
	s.Unsubscribe()
	if _, err := ops.Consumer(ctxT(t, 5*time.Second), "EVT_PEDIDOS", "facturacion-api__pedidos_pedido_v1_creado"); err != nil {
		t.Errorf("el durable no se creó: %v", err)
	}
}

func TestG_PublicarEnUnDominioAjenoEsPermissionDeniedRapido(t *testing.T) {
	env := startEnv(t, envOptions{})
	b, _, err := env.connectAs("pedidos-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	_, err = b.Publish(ctxT(t, 20*time.Second), "facturacion.factura.v2.emitida", map[string]any{"x": 1})
	quierePermisoDenegado(t, err, "$JS.API.STREAM.INFO.EVT_FACTURACION")
	if d := time.Since(t0); d > 4*time.Second {
		t.Errorf("tardó %v: una denegación llega en ms, no por el timeout de la API", d)
	}
}

func TestG_UnPublishDenegadoDeSuPropioDominioTambienEsPermissionDeniedRapido(t *testing.T) {
	// El stream propio se consulta bien, pero el permiso de publicar en `pedidos.>` falta: la librería esperaría el
	// timeout de la API (5 s) sin decir nada.
	env := startEnv(t, envOptions{Variants: []variant{{Name: "pedidos-sin-pub", Base: "pedidos-api", Drop: []string{"pedidos.>"}}}})
	b, _, err := env.connectAs("pedidos-sin-pub", nil)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	_, err = b.Publish(ctxT(t, 20*time.Second), subjectCreado, map[string]any{"x": 1})
	quierePermisoDenegado(t, err, subjectCreado)
	if d := time.Since(t0); d > 4*time.Second {
		t.Errorf("tardó %v", d)
	}
}

func TestG_UnAckDenegadoSoloDejaRastroEnElLogger_UnaVezPorSubject(t *testing.T) {
	env := startEnv(t, envOptions{Variants: []variant{{Name: "fact-sin-ack", Base: "facturacion-api", Drop: []string{ackCreado}}}})
	pedidos, _, err := env.connectAs("pedidos-api", nil)
	if err != nil {
		t.Fatal(err)
	}
	fact, sink, err := env.connectAs("fact-sin-ack", nil)
	if err != nil {
		t.Fatal(err)
	}
	var vistos atomic.Int32
	s, err := fact.Subscribe(ctxT(t, 20*time.Second), subjectCreado, func(context.Context, Event, Delivery) error {
		vistos.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe no debe fallar por un ack: el ack no tiene respuesta que pueda fallar: %v", err)
	}
	defer s.Unsubscribe()
	for i := 0; i < 3; i++ {
		if _, err := pedidos.Publish(ctxT(t, 10*time.Second), subjectCreado, map[string]any{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for vistos.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if vistos.Load() < 3 {
		t.Fatalf("el handler vio %d eventos, se esperaban 3", vistos.Load())
	}
	time.Sleep(500 * time.Millisecond)

	if n := sink.Count("$JS.ACK.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.>"); n != 1 {
		t.Errorf("el ack denegado debe avisarse UNA vez por subject (3 eventos): %d líneas:\n%s", n, sink.text())
	}
	if !sink.Contains("REENTREGA") || !sink.Contains("ERROR") {
		t.Errorf("debe registrarse a nivel de error y con su consecuencia (reentrega sin fin):\n%s", sink.text())
	}
}
