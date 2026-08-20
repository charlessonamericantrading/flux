package flux

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestParseSubjectValido(t *testing.T) {
	casos := []struct {
		subject string
		want    ParsedSubject
	}{
		{"pedidos.pedido.v1.creado", ParsedSubject{"pedidos", "pedido", 1, "creado"}},
		{"logistica.envio.v1.entrega-fallida", ParsedSubject{"logistica", "envio", 1, "entrega-fallida"}},
		{"facturacion.factura.v2.emitida", ParsedSubject{"facturacion", "factura", 2, "emitida"}},
		{"pedidos.linea-envio.v10.direccion-envio-cambiada",
			ParsedSubject{"pedidos", "linea-envio", 10, "direccion-envio-cambiada"}},
	}
	for _, c := range casos {
		got, err := ParseSubject(c.subject)
		if err != nil {
			t.Fatalf("ParseSubject(%q) devolvió error: %v", c.subject, err)
		}
		if got != c.want {
			t.Errorf("ParseSubject(%q) = %+v, se esperaba %+v", c.subject, got, c.want)
		}
		// La transformación es biyectiva: reconstruir debe devolver el original.
		if got.Subject() != c.subject {
			t.Errorf("Subject() = %q, se esperaba %q", got.Subject(), c.subject)
		}
	}
}

func TestParseSubjectRechazaMayusculas(t *testing.T) {
	// NATS es case-sensitive: una mayúscula crea un subject fantasma SIN error, así
	// que el mensaje tiene que explicarlo — 02-naming.md §1.1.
	_, err := ParseSubject("Pedidos.Pedido.V1.Creado")
	if err == nil {
		t.Fatal("se esperaba error para un subject con mayúsculas")
	}
	var ise *InvalidSubjectError
	if !errors.As(err, &ise) {
		t.Fatalf("se esperaba *InvalidSubjectError, se obtuvo %T", err)
	}
	if !strings.Contains(err.Error(), "case-sensitive") {
		t.Errorf("el error debe explicar que NATS es case-sensitive, se obtuvo: %v", err)
	}
}

func TestParseSubjectRechazaTokensIncorrectos(t *testing.T) {
	casos := map[string]string{
		"pedidos.crear-pedido":               "2",
		"pedidos.pedido.v1.creado.retry":     "5", // un 5º token rompe todos los wildcards
		"pedidos.pedido.v1":                  "3",
		"pedidos.pedido.v1.creado.extra.mas": "6",
	}
	for subject, tokens := range casos {
		_, err := ParseSubject(subject)
		if err == nil {
			t.Fatalf("se esperaba error para %q", subject)
		}
		if !strings.Contains(err.Error(), "tiene "+tokens) {
			t.Errorf("el error de %q debería indicar que tiene %s tokens, se obtuvo: %v",
				subject, tokens, err)
		}
	}
}

func TestParseSubjectRechazaFormatosInvalidos(t *testing.T) {
	invalidos := []string{
		"pedidos.pedido.1.creado",        // falta la 'v'
		"pedidos.pedido.v0.creado",       // v0 no existe: el major empieza en 1
		"pedidos.pedido.v1.creado_x",     // guion bajo prohibido
		"pedidos.pedido.v1.creado nuevo", // espacio prohibido
		"pedidos.pedido.v1.*",            // wildcard no es un subject
		"pedidos..v1.creado",             // token vacío
		"",
	}
	for _, s := range invalidos {
		if IsValidSubject(s) {
			t.Errorf("IsValidSubject(%q) = true, se esperaba false", s)
		}
	}
}

func TestSubjectToType(t *testing.T) {
	// La versión pasa de la posición 3 (enrutado, wildcards) al final (catálogo).
	got, err := SubjectToType("pedidos.pedido.v1.creado")
	if err != nil {
		t.Fatal(err)
	}
	if want := "com.flux.pedidos.pedido.creado.v1"; got != want {
		t.Errorf("SubjectToType = %q, se esperaba %q", got, want)
	}

	if _, err := SubjectToType("Pedidos.pedido.v1.creado"); err == nil {
		t.Error("SubjectToType debe propagar el error de ParseSubject")
	}
}

func TestStreamName(t *testing.T) {
	// NATS no admite puntos en nombres de stream — 02-naming.md §3.
	casos := map[string]string{
		"pedidos":       "EVT_PEDIDOS",
		"facturacion":   "EVT_FACTURACION",
		"gestion-stock": "EVT_GESTION_STOCK",
	}
	for domain, want := range casos {
		if got := StreamName(domain); got != want {
			t.Errorf("StreamName(%q) = %q, se esperaba %q", domain, got, want)
		}
		if strings.ContainsAny(StreamName(domain), ".*>/\\ ") {
			t.Errorf("StreamName(%q) contiene un carácter prohibido por NATS", domain)
		}
	}
	if got, want := DLQStreamName("pedidos"), "DLQ_PEDIDOS"; got != want {
		t.Errorf("DLQStreamName = %q, se esperaba %q", got, want)
	}
}

func TestDurableName(t *testing.T) {
	got, err := DurableName("facturacion-api", "pedidos.pedido.v1.creado")
	if err != nil {
		t.Fatal(err)
	}
	want := "facturacion-api__pedidos_pedido_v1_creado"
	if got != want {
		t.Errorf("DurableName = %q, se esperaba %q", got, want)
	}
	// NATS rechaza estos caracteres en nombres de durable — 02-naming.md §4.
	if strings.ContainsAny(got, ".*>/\\ ") {
		t.Errorf("DurableName produjo un carácter prohibido: %q", got)
	}
	// Reversibilidad: partir por "__" recupera el servicio.
	if servicio, _, _ := strings.Cut(got, "__"); servicio != "facturacion-api" {
		t.Errorf("el durable no es reversible: servicio recuperado = %q", servicio)
	}
	if _, err := DurableName("svc", "pedidos.crear"); err == nil {
		t.Error("DurableName debe validar el subject")
	}
}

func TestDLQSubjectEsPrefijo(t *testing.T) {
	// Un sufijo encajaría con `pedidos.>` y el stream principal capturaría sus
	// propios muertos — 02-naming.md §3.1.
	got := DLQSubject("pedidos.pedido.v1.creado")
	if want := "dlq.pedidos.pedido.v1.creado"; got != want {
		t.Errorf("DLQSubject = %q, se esperaba %q", got, want)
	}
	if !strings.HasPrefix(got, "dlq.") {
		t.Error("DLQSubject DEBE ser un prefijo, nunca un sufijo")
	}
	if strings.HasSuffix(got, ".dlq") {
		t.Error("DLQSubject como sufijo encajaría con <dominio>.> — prohibido")
	}
	if !IsDLQSubject(got) || IsDLQSubject("pedidos.pedido.v1.creado") {
		t.Error("IsDLQSubject no distingue el espacio de nombres de la DLQ")
	}
}

func TestSourceURI(t *testing.T) {
	if got, want := SourceURI("produccion", "pedidos-api"), "/produccion/pedidos-api"; got != want {
		t.Errorf("SourceURI = %q, se esperaba %q", got, want)
	}
}

func TestInvarianteAckWaitIgualBackoffCero(t *testing.T) {
	// LA trampa verificada contra NATS 2.14.5: el servidor sobrescribe ack_wait con
	// backoff[0] sin dar error. Si estos dos valores divergen, todo handler que dure
	// más que backoff[0] se ejecuta en concurrencia consigo mismo
	// — 03-delivery.md §2.1, conformance/cases/consumer-config.json.
	backoff := CanonicalBackOff()
	if DefaultAckWait != backoff[0] {
		t.Fatalf("DefaultAckWait (%s) DEBE ser igual a CanonicalBackOff()[0] (%s)",
			DefaultAckWait, backoff[0])
	}
	// Aserción handler-budget-sane del caso de conformidad.
	if backoff[0] < 30*time.Second {
		t.Fatalf("backoff[0] = %s: menos de 30s provoca reentrega concurrente", backoff[0])
	}
	// Aserción backoff-count-matches-retries: 1 entrega inicial + N reintentos.
	if len(backoff) != DefaultMaxDeliver-1 {
		t.Fatalf("len(backoff) = %d y MaxDeliver = %d: la última entrada nunca se aplicaría",
			len(backoff), DefaultMaxDeliver)
	}
	if want := []time.Duration{30 * time.Second, time.Minute, 5 * time.Minute,
		15 * time.Minute, 30 * time.Minute}; !equalDurations(backoff, want) {
		t.Errorf("backoff canónico alterado: %v", backoff)
	}
	if got, want := TotalTimeToDLQ(), 51*time.Minute+30*time.Second; got != want {
		t.Errorf("TotalTimeToDLQ = %s, la spec dice %s", got, want)
	}
}

func TestCanonicalBackOffNoEsMutableDesdeFuera(t *testing.T) {
	// Si devolviese el mismo slice, alterar [0] cambiaría el ack_wait efectivo de
	// todo consumidor creado después.
	b := CanonicalBackOff()
	b[0] = time.Second
	if CanonicalBackOff()[0] != 30*time.Second {
		t.Fatal("CanonicalBackOff devuelve un slice compartido: mutarlo rompe ack_wait")
	}
}

// ─── Verificación de la config devuelta por el servidor (requisito L2) ───────

func canonicalConsumerConfig() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       "facturacion-api__pedidos_pedido_v1_creado",
		FilterSubject: "pedidos.pedido.v1.creado",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       DefaultAckWait,
		MaxDeliver:    DefaultMaxDeliver,
		BackOff:       CanonicalBackOff(),
		MaxAckPending: DefaultMaxAckPending,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
	}
}

func TestAssertConfigHonoredAceptaLaConfigCanonica(t *testing.T) {
	req := canonicalConsumerConfig()
	if err := assertConfigHonored(req.Durable, req, canonicalConsumerConfig()); err != nil {
		t.Fatalf("la config canónica debería pasar la verificación: %v", err)
	}
}

func TestAssertConfigHonoredDetectaLaSobrescrituraDeAckWait(t *testing.T) {
	// El contraejemplo de conformance/cases/consumer-config.json: se pide ack_wait 30s
	// con un backoff que empieza en 1s y el servidor devuelve ack_wait 1s SIN error.
	// Sin esta verificación, todo handler de más de un segundo se ejecutaría en
	// concurrencia consigo mismo — 03-delivery.md §2.1.
	req := canonicalConsumerConfig()
	req.BackOff = []time.Duration{time.Second, 5 * time.Second, 30 * time.Second,
		2 * time.Minute, 10 * time.Minute}

	efectiva := req
	efectiva.AckWait = time.Second // lo que hace el servidor de verdad

	err := assertConfigHonored(req.Durable, req, efectiva)
	if err == nil {
		t.Fatal("la sobrescritura silenciosa de ack_wait DEBE fallar en alto")
	}
	var mismatch *ConsumerConfigMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("se esperaba *ConsumerConfigMismatchError, se obtuvo %T", err)
	}
	campos := map[string]bool{}
	for _, d := range mismatch.Differences {
		campos[d.Field] = true
	}
	if !campos["ack_wait"] {
		t.Errorf("no se reportó la diferencia en ack_wait: %+v", mismatch.Differences)
	}
	if !strings.Contains(err.Error(), "backoff[0]") {
		t.Errorf("el error debe apuntar a backoff[0] como causa: %v", err)
	}
}

func TestAssertConfigHonoredDetectaOtrosCamposAlterados(t *testing.T) {
	casos := map[string]func(*jetstream.ConsumerConfig){
		"max_deliver":     func(c *jetstream.ConsumerConfig) { c.MaxDeliver = 3 },
		"max_ack_pending": func(c *jetstream.ConsumerConfig) { c.MaxAckPending = 1000 },
		"ack_policy":      func(c *jetstream.ConsumerConfig) { c.AckPolicy = jetstream.AckNonePolicy },
		"backoff":         func(c *jetstream.ConsumerConfig) { c.BackOff = CanonicalBackOff()[:3] },
	}
	for campo, romper := range casos {
		req := canonicalConsumerConfig()
		efectiva := canonicalConsumerConfig()
		romper(&efectiva)

		err := assertConfigHonored(req.Durable, req, efectiva)
		if err == nil {
			t.Fatalf("una alteración de %s debe detectarse", campo)
		}
		if !strings.Contains(err.Error(), campo) {
			t.Errorf("el error de %s no menciona el campo: %v", campo, err)
		}
	}
}

func TestAssertConfigHonoredValidaLaInvarianteSobreLaConfigEfectiva(t *testing.T) {
	// Aunque el servidor devuelva exactamente lo pedido, ack_wait y backoff[0] tienen
	// que coincidir. Esto caza que alguien cambie CanonicalBackOff y olvide
	// DefaultAckWait, aquí y no en producción.
	req := canonicalConsumerConfig()
	req.AckWait = 10 * time.Second
	req.BackOff = []time.Duration{10 * time.Second, time.Minute}

	// El servidor "honra" lo pedido, pero la invariante del protocolo se cumple.
	if err := assertConfigHonored(req.Durable, req, req); err != nil {
		t.Fatalf("ack_wait == backoff[0] debería pasar: %v", err)
	}

	req.AckWait = 10 * time.Second
	req.BackOff = []time.Duration{30 * time.Second, time.Minute}
	if err := assertConfigHonored(req.Durable, req, req); err == nil {
		t.Fatal("ack_wait != backoff[0] DEBE fallar aunque el servidor honre la petición")
	}
}

// ─── Propagación de contexto ────────────────────────────────────────────────

func TestContextFromEventEncadenaCausalidad(t *testing.T) {
	origen := Event{
		ID:            "evento-2",
		CorrelationID: "flujo-1",
		CausationID:   "evento-1",
		TenantID:      "acme",
		TraceParent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	ec := ContextFromEvent(origen)

	// correlationid se propaga SIN modificar: identifica el flujo completo.
	if ec.CorrelationID != "flujo-1" {
		t.Errorf("correlationid = %q, debe propagarse sin modificar", ec.CorrelationID)
	}
	// causationid pasa a ser el ID de ESTE evento, no el causationid heredado.
	if ec.CausationID != "evento-2" {
		t.Errorf("causationid = %q, se esperaba el id del evento en curso", ec.CausationID)
	}
	if ec.TenantID != "acme" || ec.TraceParent != origen.TraceParent {
		t.Errorf("contexto incompleto: %+v", ec)
	}
}

func TestEventContextViajaEnElContext(t *testing.T) {
	ec := EventContext{CorrelationID: "flujo-1", CausationID: "evento-2", TenantID: "acme"}
	ctx := WithEventContext(context.Background(), ec)

	got, ok := EventContextFrom(ctx)
	if !ok {
		t.Fatal("EventContextFrom no encontró el contexto inyectado")
	}
	if got != ec {
		t.Errorf("contexto alterado en el viaje: %+v", got)
	}

	// Un ctx limpio es el caso normal de un publish desde una ruta HTTP o un cron:
	// el evento nace de cero y Publish inicializa correlationid con su propio id.
	if _, ok := EventContextFrom(context.Background()); ok {
		t.Error("un context.Background() no debe llevar contexto de evento")
	}
}
