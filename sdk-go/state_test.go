package flux

// Estado replicado sin broker: las funciones puras (los MISMOS bytes que 10-state.md §2.6), las validaciones y su orden (§3.6),
// los siete códigos con su clase (04-errors.md §6) y las piezas de lógica que el broker no ejercita bien por sí solo (los dos
// códigos del conflicto de secuencia, la suma de `flux_consumer_pending`, `onSynced`). Lo que habla con el broker, en
// state_integration_test.go.

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

type casoEstado struct {
	Fixtures []struct {
		ID        string          `json:"id"`
		HarnessOp string          `json:"harnessOp"`
		Input     json.RawMessage `json:"input"`
		Base64    string          `json:"base64"`
		SHA256    string          `json:"sha256"`
	} `json:"fixtures"`
	SubjectVectors []struct {
		Contract    string `json:"contract"`
		TenantID    string `json:"tenantId"`
		Key         string `json:"key"`
		NatsSubject string `json:"natsSubject"`
		ErrorCode   string `json:"errorCode"`
	} `json:"subjectVectors"`
}

func cargaCasoDeEstado(t *testing.T) casoEstado {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "conformance", "cases", "cross-sdk-state-envelope.json"))
	if err != nil {
		t.Fatal(err)
	}
	var c casoEstado
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

// entradaDeEstado lee la entrada plana de un fixture, como la lee el arnés.
func entradaDeEstado(t *testing.T, raw json.RawMessage) (StateEventInput, json.RawMessage) {
	t.Helper()
	var e struct {
		Contract, TenantID, Key, ID, Source, Time, Correlationid, Producerversion, Classification string
		Causationid, Traceparent, Tracestate                                                      string
		Data, AggregateVersion                                                                    json.RawMessage
		Original                                                                                  json.RawMessage
		Revision                                                                                  uint64
		DLQ                                                                                       struct {
			Reason, Consumer, Error, Dlqtime string
			Attempts                         int
		} `json:"dlq"`
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatal(err)
	}
	return StateEventInput{Contract: e.Contract, TenantID: e.TenantID, Key: e.Key, ID: e.ID, Source: e.Source, Time: e.Time,
		CorrelationID: e.Correlationid, ProducerVersion: e.Producerversion, Classification: DataClassification(e.Classification),
		Data: e.Data, CausationID: e.Causationid, TraceParent: e.Traceparent, TraceState: e.Tracestate}, e.AggregateVersion
}

func TestLosTresEjemplosDeLaSpecSonByteAByte(t *testing.T) {
	c := cargaCasoDeEstado(t)
	if len(c.Fixtures) != 3 {
		t.Fatalf("el caso trae %d fixtures, se esperaban 3 (put, lápida y DLQ)", len(c.Fixtures))
	}
	for _, f := range c.Fixtures {
		var (
			e   Event
			err error
		)
		switch f.HarnessOp {
		case "state-build":
			in, _ := entradaDeEstado(t, f.Input)
			e, err = BuildStateEvent(in)
		case "state-tombstone":
			in, version := entradaDeEstado(t, f.Input)
			v, verr := AggregateVersionFromJSON(version)
			if verr != nil {
				t.Fatal(verr)
			}
			e, err = BuildStateTombstone(StateTombstoneInput{Contract: in.Contract, TenantID: in.TenantID, Key: in.Key, ID: in.ID,
				Source: in.Source, Time: in.Time, CorrelationID: in.CorrelationID, ProducerVersion: in.ProducerVersion, AggregateVersion: v})
		case "state-dlq":
			var raw struct {
				Original json.RawMessage `json:"original"`
				Revision uint64          `json:"revision"`
				DLQ      struct {
					Reason, Consumer, Error, Dlqtime string
					Attempts                         int
				} `json:"dlq"`
			}
			if err := json.Unmarshal(f.Input, &raw); err != nil {
				t.Fatal(err)
			}
			orig, _ := entradaDeEstado(t, raw.Original)
			e, err = BuildStateDLQEvent(StateDLQBuildInput{Original: orig, Revision: raw.Revision, DLQ: StateDLQInfo{
				Reason: DLQReason(raw.DLQ.Reason), Attempts: raw.DLQ.Attempts, Consumer: raw.DLQ.Consumer, Error: raw.DLQ.Error, DLQTime: raw.DLQ.Dlqtime}})
		default:
			t.Fatalf("operación %q", f.HarnessOp)
		}
		if err != nil {
			t.Fatalf("%s: %v", f.ID, err)
		}
		b, err := Serialize(e)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		quiero, err := base64.StdEncoding.DecodeString(f.Base64)
		if err != nil {
			t.Fatal(err)
		}
		if hex.EncodeToString(sum[:]) != f.SHA256 || string(b) != string(quiero) {
			t.Errorf("%s: los bytes no son los de la spec\n  tengo:   %s\n  spec:    %s", f.ID, b, quiero)
		}
	}
}

func TestElSubjectDeEstadoYSusSeisTokens(t *testing.T) {
	for _, v := range cargaCasoDeEstado(t).SubjectVectors {
		got, err := StateSubject(v.Contract, v.TenantID, v.Key)
		if v.ErrorCode != "" {
			if ce, ok := AsClassified(err); !ok || ce.FluxCode() != v.ErrorCode {
				t.Errorf("StateSubject(%q,%q,%q): err = %v, se esperaba %s", v.Contract, v.TenantID, v.Key, err, v.ErrorCode)
			}
			continue
		}
		if err != nil || got != v.NatsSubject {
			t.Errorf("StateSubject(%q,%q,%q) = %q, %v; se esperaba %q", v.Contract, v.TenantID, v.Key, got, err, v.NatsSubject)
		}
	}
}

// §3.6: el primero que falla gana. Cada caso rompe DOS cosas a la vez: dos SDKs que validan en otro orden dan el mismo código ante
// cada defecto suelto y distinto ante dos.
func TestElOrdenDeValidacionDeEstado(t *testing.T) {
	bueno := StateEventInput{Contract: "clientes.cliente.v1", TenantID: "acme", Key: "cli-1", ID: "i", Source: "/p/s",
		Time: "2026-08-20T10:25:39.412Z", CorrelationID: "c", ProducerVersion: "1", Data: json.RawMessage(`{"aggregateVersion":7}`)}
	casos := []struct {
		nombre string
		mut    func(*StateEventInput)
		code   string
	}{
		{"dominio reservado gana a todo", func(i *StateEventInput) { i.Contract, i.Key, i.TenantID = "state.cliente.v1", "a.b", "A" }, "RESERVED_DOMAIN"},
		{"contrato antes que clave", func(i *StateEventInput) { i.Contract, i.Key = "clientes.cliente", "a.b" }, "INVALID_STATE_CONTRACT"},
		{"clave antes que tenant", func(i *StateEventInput) { i.Key, i.TenantID = "a.b", "ACME" }, "INVALID_STATE_KEY"},
		{"tenant antes que versión", func(i *StateEventInput) { i.TenantID, i.Data = "ACME", json.RawMessage(`{}`) }, "INVALID_TENANT_FOR_STATE"},
		{"versión antes que clasificación", func(i *StateEventInput) { i.Data, i.Classification = json.RawMessage(`{}`), "restricted" }, "MISSING_AGGREGATE_VERSION"},
		{"clasificación antes que los atributos vacíos", func(i *StateEventInput) { i.Classification, i.ID = "restricted", "" }, "INVALID_DATACLASSIFICATION"},
		{"id vacío", func(i *StateEventInput) { i.ID = "" }, "MISSING_REQUIRED_ATTRIBUTE"},
		{"correlationid vacío es una extensión", func(i *StateEventInput) { i.CorrelationID = "" }, "MISSING_REQUIRED_EXTENSION"},
		{"time que no es un instante", func(i *StateEventInput) { i.Time = "ayer" }, "WRONG_ATTRIBUTE_TYPE"},
	}
	for _, c := range casos {
		in := bueno
		c.mut(&in)
		_, err := BuildStateEvent(in)
		ce, ok := AsClassified(err)
		if !ok || ce.FluxCode() != c.code || ce.FluxClass() != ClassPermanent {
			t.Errorf("%s: err = %v, se esperaba %s PERMANENT", c.nombre, err, c.code)
		}
	}
	if _, err := BuildStateEvent(bueno); err != nil {
		t.Fatalf("la entrada buena no debe fallar: %v", err)
	}
}

func TestAggregateVersion(t *testing.T) {
	buenos := map[string]int64{"1": 1, "7": 7, "9007199254740991": AggregateVersionMax, "  42 ": 42}
	for lit, quiero := range buenos {
		if got, err := AggregateVersionFromJSON([]byte(lit)); err != nil || got != quiero {
			t.Errorf("%q = %d, %v; se esperaba %d", lit, got, err, quiero)
		}
	}
	// Ausente, null, cadena, decimal, `7.0` y `1e0` (en Go el parser separa el entero del flotante: 10-state.md §3.4), 0, negativo,
	// uno mayor que 2^53 − 1, un entero con ceros a la izquierda o signo.
	for _, lit := range []string{"", "null", `"7"`, "7.5", "7.0", "1e0", "0", "-1", "9007199254740992", "007", "+7", "true", "[7]", "{}", "1E1", "99999999999999999999"} {
		_, err := AggregateVersionFromJSON([]byte(lit))
		var mv *MissingAggregateVersionError
		if !errors.As(err, &mv) || mv.FluxCode() != "MISSING_AGGREGATE_VERSION" || mv.FluxClass() != ClassPermanent {
			t.Errorf("%q: err = %v, se esperaba MISSING_AGGREGATE_VERSION", lit, err)
		}
	}
	// Un `data` que no es un objeto tampoco tiene versión.
	for _, d := range []string{`[1]`, `7`, `null`, `"x"`, ``} {
		if _, err := aggregateVersionOfData([]byte(d)); err == nil {
			t.Errorf("data %q debía fallar", d)
		}
	}
}

func TestLosSieteCodigosDeEstadoConSuClase(t *testing.T) {
	casos := []struct {
		err   ClassifiedError
		code  string
		clase ErrorClass
	}{
		{&InvalidTenantForStateError{baseError: baseError{CodeInvalidTenantForState, ClassPermanent, nil}}, "INVALID_TENANT_FOR_STATE", ClassPermanent},
		{&InvalidStateContractError{baseError: baseError{CodeInvalidStateContract, ClassPermanent, nil}}, "INVALID_STATE_CONTRACT", ClassPermanent},
		{&InvalidStateKeyError{baseError{CodeInvalidStateKey, ClassPermanent, nil}, "k"}, "INVALID_STATE_KEY", ClassPermanent},
		{&MissingAggregateVersionError{baseError{CodeMissingAggregateVer, ClassPermanent, nil}, "x"}, "MISSING_AGGREGATE_VERSION", ClassPermanent},
		{&StateSubjectMismatchError{baseError{CodeStateSubjectMismatch, ClassPoison, nil}, "tenantid", "a", "b"}, "STATE_SUBJECT_MISMATCH", ClassPoison},
		{&StateConflictError{baseError: baseError{CodeStateConflict, ClassRetryable, nil}}, "STATE_CONFLICT", ClassRetryable},
		{&ReservedDomainError{baseError{CodeReservedDomain, ClassPermanent, nil}, "state", "contrato"}, "RESERVED_DOMAIN", ClassPermanent},
	}
	for _, c := range casos {
		if c.err.FluxCode() != c.code || c.err.FluxClass() != c.clase {
			t.Errorf("%T: %s/%s, se esperaba %s/%s", c.err, c.err.FluxCode(), c.err.FluxClass(), c.code, c.clase)
		}
		if got := Classify(c.err); got.Code != c.code || got.Class != c.clase {
			t.Errorf("%T: el clasificador dice %+v", c.err, got)
		}
		if strings.TrimSpace(c.err.Error()) == "" {
			t.Errorf("%T: mensaje vacío", c.err)
		}
	}
}

func TestElTenantDeEstadoNoTieneDefaultNiCaeEnSystem(t *testing.T) {
	b := &Bus{opts: ConnectOptions{Service: "crm-api"}}
	st := newState(b)
	_, err := st.tenantFor(context.Background(), stateConfig{})
	var it *InvalidTenantForStateError
	if !errors.As(err, &it) || !it.Missing {
		t.Fatalf("sin tenant, err = %v: en estado no hay tenant por defecto ni `system` implícito", err)
	}
	// El tenant de la conexión, y el del evento en curso GANA a ése en Put/Delete/Get…
	b.opts.TenantID = "acme"
	if got, err := st.tenantFor(context.Background(), stateConfig{}); err != nil || got != "acme" {
		t.Errorf("el de la conexión: %q, %v", got, err)
	}
	ctx := WithEventContext(context.Background(), EventContext{TenantID: "globex"})
	if got, _ := st.tenantFor(ctx, stateConfig{}); got != "globex" {
		t.Errorf("el evento en curso debía ganar a la conexión: %q", got)
	}
	if got, _ := st.tenantFor(ctx, stateConfig{tenantID: "initech"}); got != "initech" {
		t.Errorf("la opción de la llamada gana a todo: %q", got)
	}
	// …pero NUNCA en `Watch`: no corre dentro de un handler, y heredar el tenant del evento que se esté procesando haría que
	// su filtro dependiera de qué evento llegó primero (10-state.md §2.4). Solo la opción y la conexión.
	if got, _ := st.watchTenant(stateConfig{}); got != "acme" {
		t.Errorf("watchTenant debe ignorar el contexto: %q", got)
	}
	b.opts.TenantID = ""
	if got, err := st.watchTenant(stateConfig{}); err != nil || got != "" {
		t.Errorf("sin tenant un watch observa todos: %q, %v", got, err)
	}
	// `system` NO es un tenant implícito: es uno como cualquier otro, y un tenant inválido se rechaza.
	for _, malo := range []string{"ACME", "a b", "-a", "a-", "a--b", strings.Repeat("a", 65), "a.b", "*", ">"} {
		if _, err := st.tenantFor(context.Background(), stateConfig{tenantID: malo}); err == nil {
			t.Errorf("el tenant %q debía rechazarse", malo)
		}
	}
	if _, err := st.tenantFor(context.Background(), stateConfig{tenantID: strings.Repeat("a", 64)}); err != nil {
		t.Errorf("64 caracteres sí caben: %v", err)
	}
}

func TestLosNombresDeEstado(t *testing.T) {
	if got, _ := StateDurableName("tienda-web", "clientes.cliente.v1", "acme"); got != "tienda-web__state_clientes_cliente_v1__acme" {
		t.Errorf("durable con tenant: %q", got)
	}
	if got, _ := StateDurableName("informes-api", "clientes.cliente.v1", ""); got != "informes-api__state_clientes_cliente_v1" {
		t.Errorf("durable sin tenant: %q", got)
	}
	if got, _ := StateDurableName("x", "linea-envios.linea-envio.v2", "acme-corp"); got != "x__state_linea_envios_linea_envio_v2__acme_corp" {
		t.Errorf("los guiones del contrato y del tenant pasan a `_`: %q", got)
	}
	if got, _ := StateFilterSubject("clientes.cliente.v1", "acme"); got != "state.clientes.cliente.v1.acme.*" {
		t.Errorf("filtro con tenant: %q", got)
	}
	if got, _ := StateFilterSubject("clientes.cliente.v1", ""); got != "state.clientes.cliente.v1.*.*" {
		t.Errorf("filtro sin tenant: %q", got)
	}
	if got, _ := StateDLQSubject("clientes.cliente.v1"); got != "dlq.state.clientes.cliente.v1" {
		t.Errorf("subject de DLQ (sin tenant ni clave): %q", got)
	}
	if got := StateStreamName("linea-envios"); got != "STATE_LINEA_ENVIOS" {
		t.Errorf("stream: %q", got)
	}
	if got, _ := StateDataSchema("clientes.cliente.v2", ""); got != "https://schemas.internal/state/clientes/cliente/2.0.0.json" {
		t.Errorf("dataschema: %q", got)
	}
	if _, err := StateDurableName("Servicio.Malo", "clientes.cliente.v1", ""); err == nil {
		t.Error("un nombre de servicio inválido debía rechazarse")
	}
}

// §3.2 paso 4: DOS códigos, no uno. Un SDK que solo reconozca el 10071 pasa sus pruebas contra un nodo y, en producción, trata un
// conflicto corriente como un error desconocido y no reintenta.
func TestElConflictoDeSecuenciaTieneDosCodigos(t *testing.T) {
	for _, code := range []int{10071, 10164} {
		api := &jetstream.APIError{Code: 400, ErrorCode: jetstream.ErrorCode(code), Description: "wrong last sequence"}
		if !isSequenceConflict(api) {
			t.Errorf("el %d es un conflicto de secuencia", code)
		}
		// Llega envuelto en el error de la petición.
		if !isSequenceConflict(fmt.Errorf("fallo al publicar: %w", api)) {
			t.Errorf("el %d, envuelto con %%w, también", code)
		}
	}
	for _, code := range []int{10059, 10014, 10077, 0} {
		if isSequenceConflict(&jetstream.APIError{ErrorCode: jetstream.ErrorCode(code)}) {
			t.Errorf("el %d NO es un conflicto de secuencia", code)
		}
	}
	if isSequenceConflict(errors.New("wrong last sequence: 5")) {
		t.Error("se reconoce el CÓDIGO, no el texto: el 10164 llega sin número")
	}
}

func TestLaSumaDeFluxConsumerPending(t *testing.T) {
	var p pendingSum
	if got := p.note("s", "d1", 5); got != 5 {
		t.Errorf("un durable: %d", got)
	}
	if got := p.note("s", "d2", 7); got != 12 {
		t.Errorf("dos durables SUMAN (un watch por tenant): %d", got)
	}
	if got := p.note("s", "d1", 1); got != 8 {
		t.Errorf("un durable se actualiza, no se acumula: %d", got)
	}
	if got := p.note("otra", "d1", 100); got != 100 {
		t.Errorf("series distintas no se mezclan: %d", got)
	}
	if got, ok := p.drop("s", "d1"); !ok || got != 7 {
		t.Errorf("al parar un watch su durable deja de sumar: %d, %v", got, ok)
	}
	if _, ok := p.drop("s", "d1"); ok {
		t.Error("soltar dos veces no debe contar")
	}
}

func TestOnSynced(t *testing.T) {
	llamadas := func() (*syncTracker, *atomic.Int32) {
		var n atomic.Int32
		return &syncTracker{fire: func() { n.Add(1) }}, &n
	}
	// Vacío, o al día: de INMEDIATO, y solo una vez.
	tr, n := llamadas()
	tr.immediate()
	tr.immediate()
	if n.Load() != 1 {
		t.Errorf("immediate: %d llamadas", n.Load())
	}
	tr.started(0)
	tr.finished()
	if n.Load() != 1 {
		t.Errorf("tras immediate no se vuelve a llamar: %d", n.Load())
	}
	// La foto: onSynced cuando la ÚLTIMA (pending 0) y todo lo en vuelo terminó. No antes.
	tr, n = llamadas()
	tr.started(2)
	tr.finished()
	tr.started(1)
	tr.finished()
	if n.Load() != 0 {
		t.Errorf("faltaba la última entrega: %d", n.Load())
	}
	tr.started(0)
	if n.Load() != 0 {
		t.Errorf("la última entrega aún está en vuelo: %d", n.Load())
	}
	tr.finished()
	if n.Load() != 1 {
		t.Errorf("tras la última y con todo terminado: %d", n.Load())
	}
	// Una entrada RETRYABLE que falla NO retrasa onSynced: `finished` se llama con éxito o con error.
	tr, n = llamadas()
	tr.started(0)
	tr.finished()
	if n.Load() != 1 {
		t.Errorf("una entrada que falla debe contar como terminada: %d", n.Load())
	}
}

func TestSubjectMatches(t *testing.T) {
	casos := []struct {
		p, s string
		ok   bool
	}{
		{"dlq.state.clientes.>", "dlq.state.clientes.x.v1", true},
		{"dlq.clientes.>", "dlq.state.clientes.x.v1", false},
		{"dlq.>", "dlq.state.clientes.x.v1", true},
		{"a.*.c", "a.b.c", true},
		{"a.*.c", "a.b.c.d", false},
		{"a.b.>", "a.b", false},
		{"a.b", "a.b", true},
		{"a.b.c", "a.b", false},
	}
	for _, c := range casos {
		if got := subjectMatches(c.p, c.s); got != c.ok {
			t.Errorf("subjectMatches(%q,%q) = %v", c.p, c.s, got)
		}
	}
}

// Lo que el estado NO conserva en la DLQ (10-state.md §6): ni el dato, ni la firma.
func TestLaDLQDeEstadoNoConservaElDatoNiLaFirma(t *testing.T) {
	ev, err := BuildStateEvent(StateEventInput{Contract: "clientes.cliente.v1", TenantID: "acme", Key: "cli-1", ID: "i", Source: "/p/s",
		Time: "2026-08-20T10:25:39.412Z", CorrelationID: "c", ProducerVersion: "1",
		Data: json.RawMessage(`{"email":"ana@example.com","aggregateVersion":7}`)})
	if err != nil {
		t.Fatal(err)
	}
	ev.SignKeyID, ev.Signature = "k-1", "firma"
	d, err := ToStateDLQEvent(ev, StateDLQInfo{Key: "cli-1", Revision: 41, Reason: DLQReasonPermanent, Attempts: 1, Consumer: "x__state_a_b_v1",
		Error: strings.Repeat("é", 1500), DLQTime: "2026-08-20T10:25:40.117Z"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Serialize(d)
	s := string(b)
	for _, prohibido := range []string{"ana@example.com", "signature", "signkeyid", "aggregateVersion"} {
		if strings.Contains(s, prohibido) {
			t.Errorf("la DLQ de estado no debe conservar %q: %s", prohibido, s)
		}
	}
	if !strings.HasSuffix(s, `"data":{"key":"cli-1","revision":41}}`) {
		t.Errorf("`data` es el último atributo y solo lleva { key, revision }: %s", s)
	}
	if len(d.DLQError) != 1024 {
		t.Errorf("dlqerror = %d bytes, se esperaban 1024 (512 «é»)", len(d.DLQError))
	}
	// Un mensaje ILEGIBLE tampoco copia su cuerpo: se sintetiza un sobre con lo único fiable, el subject.
	u, err := BuildUnreadableStateDLQEvent(UnreadableStateDLQInput{Contract: "clientes.cliente.v1", TenantID: "acme", Key: "cli-1", Revision: 9,
		Source: "/p/s", ProducerVersion: "1", ID: "id-1", Time: "2026-08-20T10:25:39.412Z",
		DLQ: StateDLQInfo{Reason: DLQReasonPoison, Attempts: 1, Consumer: "c", Error: "MALFORMED_JSON: x", DLQTime: "2026-08-20T10:25:40.117Z"}})
	if err != nil {
		t.Fatal(err)
	}
	if ub, _ := Serialize(u); !strings.Contains(string(ub), `"data":{"key":"cli-1","revision":9}`) || strings.Contains(string(ub), "rawBase64") {
		t.Errorf("DLQ de un mensaje ilegible: %s", ub)
	}
}

func TestAssertStateConfigHonoredNotaElFiltroYLaPoliticaDeEntrega(t *testing.T) {
	pedido := jetstream.ConsumerConfig{Durable: "d", FilterSubject: "state.clientes.cliente.v1.acme.*", AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: DefaultAckWait, MaxDeliver: DefaultMaxDeliver, BackOff: CanonicalBackOff(), MaxAckPending: DefaultMaxAckPending,
		DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy}
	if err := assertStateConfigHonored("d", pedido, pedido); err != nil {
		t.Fatalf("la misma config no es una diferencia: %v", err)
	}
	// Un durable aprovisionado con otro filtro entregaría las claves de OTRO tenant sin ningún error.
	otro := pedido
	otro.FilterSubject = "state.clientes.cliente.v1.globex.*"
	var m *ConsumerConfigMismatchError
	if err := assertStateConfigHonored("d", pedido, otro); !errors.As(err, &m) || !strings.Contains(err.Error(), "filter_subject") {
		t.Errorf("otro filtro: %v", err)
	}
	todos := pedido
	todos.FilterSubject = "state.clientes.cliente.v1.*.*"
	if err := assertStateConfigHonored("d", pedido, todos); err == nil {
		t.Error("todos los tenants en vez de uno debía detectarse")
	}
	todo := pedido
	todo.DeliverPolicy = jetstream.DeliverAllPolicy
	if err := assertStateConfigHonored("d", pedido, todo); !errors.As(err, &m) || !strings.Contains(err.Error(), "deliver_policy") {
		t.Errorf("otra política de entrega: %v", err)
	}
	// También la comprobación de siempre (ack_wait).
	lento := pedido
	lento.AckWait = DefaultAckWait + 1
	if err := assertStateConfigHonored("d", pedido, lento); err == nil {
		t.Error("ack_wait distinto debía detectarse")
	}
}

func TestPutRechazaSinRedLoQueEsDeQuienLlama(t *testing.T) {
	// Un bus sin conexión: nada de esto toca la red, que es el punto (10-state.md §3.6: «antes de abrir ninguna petición»).
	b := &Bus{opts: ConnectOptions{Service: "crm-api", TenantID: "acme", Version: "1", SchemaBaseURL: "https://schemas.internal"}, source: "/p/crm-api"}
	b.state = newState(b)
	st := b.State()
	ctx := context.Background()
	datos := map[string]any{"aggregateVersion": 7}
	casos := []struct {
		nombre string
		fn     func() error
		code   string
	}{
		{"dominio reservado", func() error { _, e := st.Put(ctx, "state.cliente.v1", "k", datos); return e }, "RESERVED_DOMAIN"},
		{"contrato mal formado", func() error { _, e := st.Put(ctx, "clientes.cliente", "k", datos); return e }, "INVALID_STATE_CONTRACT"},
		{"clave con punto", func() error { _, e := st.Put(ctx, "clientes.cliente.v1", "a.b", datos); return e }, "INVALID_STATE_KEY"},
		{"clave que empieza por _", func() error { _, e := st.Put(ctx, "clientes.cliente.v1", "_x", datos); return e }, "INVALID_STATE_KEY"},
		{"tenant inválido", func() error {
			_, e := st.Put(ctx, "clientes.cliente.v1", "k", datos, WithStateTenant("ACME"))
			return e
		}, "INVALID_TENANT_FOR_STATE"},
		{"sin versión", func() error { _, e := st.Put(ctx, "clientes.cliente.v1", "k", map[string]any{"x": 1}); return e }, "MISSING_AGGREGATE_VERSION"},
		{"versión 0", func() error {
			_, e := st.Put(ctx, "clientes.cliente.v1", "k", map[string]any{"aggregateVersion": 0})
			return e
		}, "MISSING_AGGREGATE_VERSION"},
		{"versión decimal", func() error {
			_, e := st.Put(ctx, "clientes.cliente.v1", "k", map[string]any{"aggregateVersion": 7.5})
			return e
		}, "MISSING_AGGREGATE_VERSION"},
		{"restricted", func() error {
			_, e := st.Put(ctx, "clientes.cliente.v1", "k", datos, WithStateClassification(ClassificationRestricted))
			return e
		}, "INVALID_DATACLASSIFICATION"},
		{"clasificación fuera del enum", func() error {
			_, e := st.Put(ctx, "clientes.cliente.v1", "k", datos, WithStateClassification("secreto"))
			return e
		}, "INVALID_DATACLASSIFICATION"},
		{"delete sin versión", func() error { _, e := st.Delete(ctx, "clientes.cliente.v1", "k", 0); return e }, "MISSING_AGGREGATE_VERSION"},
		{"delete con clave inválida", func() error { _, e := st.Delete(ctx, "clientes.cliente.v1", "", 3); return e }, "INVALID_STATE_KEY"},
		{"get con dominio reservado", func() error { _, _, e := st.Get(ctx, "dlq.cliente.v1", "k"); return e }, "RESERVED_DOMAIN"},
		{"watch con contrato mal formado", func() error {
			_, e := st.Watch(ctx, "clientes", func(context.Context, StateEntry, StateDelivery) error { return nil })
			return e
		}, "INVALID_STATE_CONTRACT"},
	}
	for _, c := range casos {
		err := c.fn()
		ce, ok := AsClassified(err)
		if !ok || ce.FluxCode() != c.code {
			t.Errorf("%s: err = %v, se esperaba %s", c.nombre, err, c.code)
		}
	}
}

func TestPublishRechazaLosDominiosReservados(t *testing.T) {
	// 10-state.md §2.7: `publish()` no puede escribir en `state.`, `rpc.` ni `dlq.`: un evento ahí no pasaría por el chequeo de
	// secuencia, y uno de core sobrescribe sin acuse (A-45).
	b := &Bus{opts: ConnectOptions{Service: "x", Environment: "t", Version: "1"}}
	for _, s := range []string{"state.clientes.cliente.v1.creado", "rpc.clientes.cliente.v1.creado", "dlq.pedidos.pedido.v1.creado", "system.poison.v1.capturado"} {
		_, err := b.Publish(context.Background(), s, map[string]any{})
		var rd *ReservedDomainError
		if !errors.As(err, &rd) {
			t.Errorf("Publish(%q): err = %v, se esperaba RESERVED_DOMAIN", s, err)
		}
		if _, err := b.Subscribe(context.Background(), s, func(context.Context, Event, Delivery) error { return nil }); !errors.As(err, &rd) {
			t.Errorf("Subscribe(%q): err = %v, se esperaba RESERVED_DOMAIN", s, err)
		}
	}
	// La coincidencia es EXACTA: `states` no es `state`.
	if _, err := ParseSubject("states.cliente.v1.creado"); err != nil {
		t.Errorf("`states` no es un dominio reservado: %v", err)
	}
}

// El mensaje de L3 de un estado cita RUTA y REGLA, nunca el valor — y reporta TODOS los incumplimientos (10-state.md §3.6).
func TestElMensajeDeL3DeEstadoNoCopiaValoresYReportaTodos(t *testing.T) {
	bundle := bundleDePrueba(t)
	v, err := newSchemaValidator(ValidationOptions{Mode: ValidationStrict, Bundle: bundle}, nil)
	if err != nil || v == nil {
		t.Fatal(err)
	}
	uri := StateSchemaURIFor(bundle, "clientes.cliente.v1")
	ev := Event{DataSchema: uri, Data: []byte(`{"clienteId":"cli-1","nombre":"Ana Secreta","email":"sin-arroba","aggregateVersion":1,"extra":"dato-extra"}`)}
	for _, redactado := range []bool{true, false} {
		err := v.run(ev, "state.clientes.cliente.v1.acme.cli-1", redactado)
		var sve *SchemaValidationError
		if !errors.As(err, &sve) {
			t.Fatalf("debía incumplir el esquema: %v", err)
		}
		texto := err.Error()
		for _, valor := range []string{"sin-arroba", "Ana Secreta", "dato-extra"} {
			if redactado && strings.Contains(texto, valor) {
				t.Errorf("el mensaje redactado filtra %q: %s", valor, texto)
			}
		}
		if !redactado && !strings.Contains(texto, "sin-arroba") {
			t.Errorf("el de un EVENTO sí cita el valor (es lo que se defiende): %s", texto)
		}
		if redactado && (!strings.Contains(texto, "/email incumple la regla `pattern`") ||
			!strings.Contains(texto, "(raíz) lleva propiedades que el esquema no declara")) {
			t.Errorf("debe citar ruta y regla de TODOS los incumplimientos: %s", texto)
		}
	}
	// `required` nombra la propiedad que falta: sale del esquema, no del dato.
	ev.Data = []byte(`{"clienteId":"cli-1","aggregateVersion":1}`)
	err = v.checkRedacted(ev, "state.clientes.cliente.v1.acme.cli-1")
	if err == nil || !strings.Contains(err.Error(), "(raíz) no tiene la propiedad obligatoria `") {
		t.Errorf("required: %v", err)
	}
}
