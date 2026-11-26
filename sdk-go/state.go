// Estado replicado — el cliente (`bus.State()`): put, delete, get y watch.
// Contrato normativo: specification/10-state.md, specification/04-errors.md §6
//
// Las funciones puras que construyen el sobre, la lápida, el subject y la DLQ están en state_pure.go.
//
// ⚠️ Lo que se MIDIÓ de `nats.go` v1.37.0 contra nats-server 2.14.5 para esto (PLAN.md, A-190 a A-199):
//
//   - `PublishMsg` con `WithExpectLastSequencePerSubject(n)` devuelve un `*jetstream.APIError` con `ErrorCode`
//     10071 («wrong last sequence: N»); 239 de 240 intentos concurrentes sobre una clave dieron ese código, con un
//     nodo. El 10164 (cluster) lo reconoce igual: se comprueba el código, no el texto.
//   - `DIRECT.GET` por subject de una clave AUSENTE contesta un mensaje con cabecera `Status: 404`; de un stream que
//     no existe (o sin `allow_direct`: se ven igual, A-32) no contesta nadie (`ErrNoResponders`, 0 ms).
//   - Un consumidor BORRADO bajo un `Consume` lo nota la librería en ~1 ms (`ErrConsumerDeleted` al `ErrHandler`) y
//     PARA el consumo ella misma; en Node tarda 43 s. Aquí se reabre al recibirlo (10-state.md §5.2).

package flux

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ─── Opciones ────────────────────────────────────────────────────────────────

type stateConfig struct {
	tenantID       string
	classification DataClassification
	when           time.Time
	onSynced       func()
}

// StateOption ajusta una llamada de `State`. Cada opción vale para unas llamadas y las demás la ignoran.
type StateOption func(*stateConfig)

// WithStateTenant fija el tenant (`Put`, `Delete`, `Get` y `Watch`). Sin él: el del evento en curso (no en `Watch`: un
// `Watch` no corre dentro de un handler y heredar el tenant del evento que se esté procesando haría que su filtro
// dependiera de qué evento llegó primero, 10-state.md §2.4) y el de ConnectOptions. En estado NO hay tenant por defecto
// ni `system` implícito: sin ninguno, INVALID_TENANT_FOR_STATE.
func WithStateTenant(tenantID string) StateOption {
	return func(c *stateConfig) { c.tenantID = tenantID }
}

// WithStateClassification fija la clasificación de un `Put`: `public`, `internal` o `confidential` (el default: NO el de
// la conexión). `restricted` se rechaza.
func WithStateClassification(c DataClassification) StateOption {
	return func(cfg *stateConfig) { cfg.classification = c }
}

// WithStateTime fija el instante en que OCURRIÓ el hecho (`Put` y `Delete`), si no es «ahora».
func WithStateTime(t time.Time) StateOption {
	return func(c *stateConfig) { c.when = t }
}

// WithOnSynced se invoca UNA vez por `Watch` —no una por reconexión, ni de nuevo tras un `flux state resync`— cuando el
// handler ya ha procesado todo lo que había pendiente al abrirlo: la señal de «mi réplica local ya puede contestar
// lecturas». Con el contrato vacío (o el durable al día), de inmediato y antes de entregar nada. NO promete que no
// lleguen entradas después (10-state.md §5.5).
func WithOnSynced(fn func()) StateOption {
	return func(c *stateConfig) { c.onSynced = fn }
}

func applyStateOptions(opts []StateOption) stateConfig {
	var c stateConfig
	for _, o := range opts {
		if o != nil {
			o(&c)
		}
	}
	return c
}

// ─── Tipos públicos ──────────────────────────────────────────────────────────

// StateWriteResult es el resultado de `Put` y `Delete`. No hay un tercer `Outcome`: un conflicto agotado es un error.
type StateWriteResult struct {
	Applied bool
	// Outcome es "ok" o "stale". `stale` NO es un error: la versión no avanza.
	Outcome string
	// Revision: con `ok`, la secuencia del mensaje nuevo; con `stale`, la del mensaje ACTUAL, el que la hizo obsoleta.
	Revision uint64
}

// StateEntry es una entrada de estado, tal como la ve quien la lee (`Get`, handler de `Watch`).
type StateEntry struct {
	// Key es la clave.
	Key string
	// TenantID es el tenant del subject. No estaba en el plan y es necesario: un `Watch` sin tenant recibe `cli-1` de
	// varios, y la clave sola mezclaría los tenants en la copia local.
	TenantID string
	// Deleted es true si es una lápida.
	Deleted bool
	// Data es el `data` del evento; nil si Deleted. Se tipa con UnmarshalStateData.
	Data json.RawMessage
	// AggregateVersion es la versión del valor (o la de la lápida).
	AggregateVersion int64
	// Revision es la secuencia del stream: dice QUÉ mensaje es; no ordena versiones ni se compara entre contratos.
	Revision uint64
}

// UnmarshalStateData decodifica el `data` de una entrada en el tipo de la aplicación.
func UnmarshalStateData[T any](e StateEntry) (T, error) {
	var out T
	if e.Deleted {
		return out, errors.New("flux: una lápida no tiene `data`")
	}
	if err := json.Unmarshal(e.Data, &out); err != nil {
		return out, NewPermanentError(fmt.Sprintf("el `data` de la clave %q no encaja en el tipo esperado", e.Key),
			WithCode("DATA_SCHEMA_MISMATCH"), WithCause(err))
	}
	return out, nil
}

// StateDelivery son los metadatos de ESTA entrega concreta de una entrada.
type StateDelivery struct {
	// Attempt es el número de entrega, empezando en 1.
	Attempt     int
	MaxAttempts int
	// Subject es el subject de NATS de la entrada (`state.clientes.cliente.v1.acme.cli-1`).
	Subject string
	// Contract es el contrato observado (`clientes.cliente.v1`).
	Contract string
}

// StateHandler procesa una entrada. Devolver nil la confirma. Un error se clasifica como el de un evento. DEBE ser
// idempotente y no asumir orden (`UPDATE … WHERE aggregate_version < $n`) y, ante `entry.Deleted`, DEBE borrar su copia
// local: es lo que hace real el derecho de supresión aguas abajo (06-security.md §5.1).
type StateHandler func(ctx context.Context, entry StateEntry, d StateDelivery) error

// StateSubscription es un `Watch` vivo.
type StateSubscription struct {
	Contract string
	Durable  string
	// TenantID es el tenant por el que filtra, si filtra.
	TenantID string

	stop func()
	once sync.Once
}

// Unsubscribe detiene el `Watch`.
func (s *StateSubscription) Unsubscribe() { s.once.Do(s.stop) }

// StateMetricsSink es la extensión OPCIONAL de MetricsSink de la capacidad `state` (08-observability.md §2.4): un
// MetricsSink escrito antes de la capacidad sigue compilando y funcionando. `contract` es lo ÚNICO que el estado
// añade como etiqueta: NUNCA la clave (una serie por cada agregado del dominio) ni el tenant (una por cada cliente).
type StateMetricsSink interface {
	// StateWrite cuenta un `Put` o un `Delete`: outcome es ok, stale, conflict o invalid_schema.
	StateWrite(contract, outcome string)
}

// ─── El cliente ──────────────────────────────────────────────────────────────

// directGetTimeout es el plazo de una lectura puntual: no hay consumidor ni acuse, y un stream que no existe no contesta.
const directGetTimeout = 5 * time.Second

// State es la capacidad `state`: put, delete, get y watch. Se obtiene con Bus.State().
type State struct {
	bus *Bus

	mu         sync.Mutex
	dlqChecked map[string]struct{}
	watches    map[*StateSubscription]struct{}
	pending    pendingSum

	// afterRead es un gancho SOLO de pruebas: se llama tras leer la clave y antes de publicar, que es donde otra instancia del
	// dueño puede colarse (10-state.md §3.2, paso 4). Nil en producción.
	afterRead func(attempt int)
}

func newState(b *Bus) *State {
	return &State{bus: b, dlqChecked: map[string]struct{}{}, watches: map[*StateSubscription]struct{}{}}
}

// State devuelve el cliente de estado replicado.
func (b *Bus) State() *State { return b.state }

func (s *State) metrics() StateMetricsSink {
	if m, ok := s.bus.metrics.(StateMetricsSink); ok {
		return m
	}
	return nil
}

func (s *State) countWrite(contract, outcome string) {
	if m := s.metrics(); m != nil {
		m.StateWrite(contract, outcome)
	}
}

// stopAll para todos los `Watch` vivos: lo que hace Bus.Close() antes de drenar.
func (s *State) stopAll() {
	s.mu.Lock()
	ws := make([]*StateSubscription, 0, len(s.watches))
	for w := range s.watches {
		ws = append(ws, w)
	}
	s.mu.Unlock()
	for _, w := range ws {
		w.Unsubscribe()
	}
}

// tenantFor resuelve el tenant de una escritura o una lectura: la opción de la llamada, el evento en curso y
// ConnectOptions. Sin ninguno, error.
func (s *State) tenantFor(ctx context.Context, cfg stateConfig) (string, error) {
	t := cfg.tenantID
	if t == "" {
		if ec, ok := EventContextFrom(ctx); ok && ec.TenantID != "" {
			t = ec.TenantID
		} else {
			t = s.bus.opts.TenantID
		}
	}
	if t == "" {
		return "", missingTenant()
	}
	if err := assertStateTenant(t); err != nil {
		return "", err
	}
	return t, nil
}

// watchTenant: la opción y ConnectOptions, SIN el evento en curso. Sin ninguno es legítimo (se observan todos), salvo
// TenantIsolationStrict.
func (s *State) watchTenant(cfg stateConfig) (string, error) {
	t := cfg.tenantID
	if t == "" {
		t = s.bus.opts.TenantID
	}
	if t == "" {
		return "", nil
	}
	if err := assertStateTenant(t); err != nil {
		return "", err
	}
	return t, nil
}

// schemaFor es el `dataschema` del contrato: mapa explícito, índice `state` del bundle o SchemaBaseURL. Nunca por red.
func (s *State) schemaFor(contract string) (string, error) {
	o := s.bus.opts
	if v := o.Schemas[contract]; v != "" {
		return v, nil
	}
	if v := StateSchemaURIFor(o.Validation.Bundle, contract); v != "" {
		return v, nil
	}
	if o.SchemaBaseURL == "" {
		return "", fmt.Errorf("flux: no hay dataschema para el estado %q. Declara Schemas[%q], SchemaBaseURL, o pasa un "+
			"bundle con el índice `state` en Validation.Bundle (ConnectOptions)", contract, contract)
	}
	return StateDataSchema(contract, o.SchemaBaseURL)
}

func (s *State) schemaForSafe(contract string) string {
	if v, err := s.schemaFor(contract); err == nil {
		return v
	}
	v, _ := StateDataSchema(contract, "")
	return v
}

// ─── escribir ────────────────────────────────────────────────────────────────

// Put escribe el valor actual de una clave. CONDICIONAL: no escribe una versión que no avanza (`stale`, que NO es un
// error) y publica con `Nats-Expected-Last-Subject-Sequence`, de modo que dos instancias del dueño no pisan un valor
// nuevo con uno viejo (10-state.md §3).
//
// `data` DEBE llevar `aggregateVersion` (entero ≥ 1): lo pone el dueño, no el SDK.
func (s *State) Put(ctx context.Context, contract, key string, data any, opts ...StateOption) (StateWriteResult, error) {
	cfg := applyStateOptions(opts)
	// §3.6, en este orden y SIN red: el primero que falla gana.
	c, err := ParseStateContract(contract)
	if err != nil {
		return StateWriteResult{}, err
	}
	if err := assertStateKey(key); err != nil {
		return StateWriteResult{}, err
	}
	tenant, err := s.tenantFor(ctx, cfg)
	if err != nil {
		return StateWriteResult{}, err
	}
	canon, err := dataBytes(data)
	if err != nil {
		return StateWriteResult{}, err
	}
	version, err := aggregateVersionOfData(canon)
	if err != nil {
		s.countWrite(c.Contract, "invalid_schema")
		return StateWriteResult{}, err
	}
	classification, err := stateClassification(cfg.classification)
	if err != nil {
		return StateWriteResult{}, err
	}
	schema, err := s.schemaFor(c.Contract)
	if err != nil {
		return StateWriteResult{}, err
	}

	id, err := uuid.NewV7()
	if err != nil {
		return StateWriteResult{}, fmt.Errorf("flux: no se pudo generar el UUIDv7 de la entrada: %w", err)
	}
	subject, _ := StateSubject(c.Contract, tenant, key)
	ev, err := BuildStateEvent(s.eventInput(ctx, c.Contract, tenant, key, id.String(), classification, canon, schema, cfg.when))
	if err != nil {
		return StateWriteResult{}, err
	}
	// L3 ANTES de leer ni escribir; reporta TODOS los errores y no copia valores a su mensaje.
	if err := s.validate(ev, subject); err != nil {
		s.countWrite(c.Contract, "invalid_schema")
		return StateWriteResult{}, err
	}
	return s.write(ctx, c, tenant, key, subject, version, ev)
}

// eventInput arma la entrada de BuildStateEvent con el contexto heredado: un estado derivado dentro de un handler
// hereda el contexto del evento que lo causó.
func (s *State) eventInput(ctx context.Context, contract, tenant, key, id string, classification DataClassification,
	data []byte, schema string, when time.Time) StateEventInput {
	in := StateEventInput{
		Contract: contract, TenantID: tenant, Key: key, ID: id, Source: s.bus.source,
		Time: nowOr(when), CorrelationID: id, ProducerVersion: s.bus.opts.Version,
		Classification: classification, Data: json.RawMessage(data), DataSchema: schema,
	}
	if ec, ok := EventContextFrom(ctx); ok {
		if ec.CorrelationID != "" {
			in.CorrelationID = ec.CorrelationID
		}
		in.CausationID, in.TraceParent, in.TraceState = ec.CausationID, ec.TraceParent, ec.TraceState
	}
	if in.TraceParent == "" && s.bus.opts.Traceparent != nil {
		in.TraceParent = s.bus.opts.Traceparent(ctx)
	}
	return in
}

func nowOr(t time.Time) string {
	if t.IsZero() {
		t = time.Now()
	}
	return FormatTime(t)
}

func (s *State) validate(ev Event, subject string) error {
	if v := s.bus.validate; v != nil {
		return v.checkRedacted(ev, subject)
	}
	return nil
}

// Delete borra una clave: publica una LÁPIDA en su subject, que sustituye al valor (el dato personal ya no está en el
// stream). Es `Put` de una lápida: mismo algoritmo, mismas reglas de versión y mismo resultado. Borrar una clave que
// nunca existió escribe la lápida. `aggregateVersion` es la de la LÁPIDA y debe superar la del valor que borra.
func (s *State) Delete(ctx context.Context, contract, key string, aggregateVersion int64, opts ...StateOption) (StateWriteResult, error) {
	cfg := applyStateOptions(opts)
	c, err := ParseStateContract(contract)
	if err != nil {
		return StateWriteResult{}, err
	}
	if err := assertStateKey(key); err != nil {
		return StateWriteResult{}, err
	}
	tenant, err := s.tenantFor(ctx, cfg)
	if err != nil {
		return StateWriteResult{}, err
	}
	if aggregateVersion < 1 || aggregateVersion > AggregateVersionMax {
		s.countWrite(c.Contract, "invalid_schema")
		_, err := AggregateVersionFromJSON([]byte(strconv.FormatInt(aggregateVersion, 10)))
		return StateWriteResult{}, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return StateWriteResult{}, fmt.Errorf("flux: no se pudo generar el UUIDv7 de la lápida: %w", err)
	}
	subject, _ := StateSubject(c.Contract, tenant, key)
	in := s.eventInput(ctx, c.Contract, tenant, key, id.String(), "", nil, "", cfg.when)
	ev, err := BuildStateTombstone(StateTombstoneInput{
		Contract: c.Contract, TenantID: tenant, Key: key, ID: in.ID, Source: in.Source, Time: in.Time,
		CorrelationID: in.CorrelationID, ProducerVersion: in.ProducerVersion, AggregateVersion: aggregateVersion,
		CausationID: in.CausationID, TraceParent: in.TraceParent, TraceState: in.TraceState,
	})
	if err != nil {
		return StateWriteResult{}, err
	}
	// La lápida valida contra su esquema fijo (en `schemas` del bundle, no en el índice `state`).
	if err := s.validate(ev, subject); err != nil {
		s.countWrite(c.Contract, "invalid_schema")
		return StateWriteResult{}, err
	}
	return s.write(ctx, c, tenant, key, subject, aggregateVersion, ev)
}

// write es el algoritmo de 10-state.md §3.2. El evento se construyó UNA vez: `id`, `time` y bytes son los mismos en
// todos los intentos. Con un `id` nuevo por intento, el paso 1b no reconocería nunca la propia escritura.
func (s *State) write(ctx context.Context, c StateContract, tenant, key, subject string, version int64, toSend Event) (StateWriteResult, error) {
	stream := StateStreamName(c.Domain)
	if s.bus.signer != nil {
		var err error
		if toSend, err = s.bus.signer.Sign(toSend); err != nil {
			return StateWriteResult{}, err
		}
	}
	bytes, err := Serialize(toSend)
	if err != nil {
		return StateWriteResult{}, err
	}
	id := toSend.ID

	for attempt := 1; attempt <= StateMaxAttempts; attempt++ {
		// 1. Leer el último mensaje de la clave. 404 → S = 0 y no hay versión.
		actual, err := s.readRaw(ctx, stream, subject)
		if err != nil {
			return StateWriteResult{}, err
		}
		var seq uint64
		if actual != nil {
			seq = actual.seq
		}

		// 1b. ¿El mensaje leído es EL MÍO? Entonces ya se aplicó y solo se perdió el acuse. Sin este paso, un acuse perdido
		// convierte una escritura buena en un `stale` con su propio dato: el reenvío da «wrong last sequence» porque lo que
		// cambió la secuencia fue el propio mensaje (A-33). Se reconoce por el `id` porque el broker conserva las cabeceras
		// (A-32).
		if actual != nil && actual.hasID(id) {
			s.countWrite(c.Contract, "ok")
			return StateWriteResult{Applied: true, Outcome: "ok", Revision: actual.seq}, nil
		}

		// 2. Una versión que no avanza NO se escribe, y una igual tampoco avanza. No es un error.
		if actual != nil && actual.version > 0 && version <= actual.version {
			s.countWrite(c.Contract, "stale")
			return StateWriteResult{Applied: false, Outcome: "stale", Revision: actual.seq}, nil
		}

		// 3. Siempre por JetStream y siempre con la cabecera de secuencia: un publish de CORE a un subject de estado
		// sobrescribe sin acuse ni chequeo (A-45). `S = 0` = «la clave no existe»: crea si el subject no tiene mensaje y
		// falla si ya lo tiene, y eso es lo que hace que dos escritores concurrentes sobre una clave nueva no se pisen (A-33).
		if s.afterRead != nil {
			s.afterRead(attempt)
		}
		gctx, done := s.bus.guard(ctx, subject)
		ack, err := s.bus.js.PublishMsg(gctx, &nats.Msg{Subject: subject, Data: bytes},
			jetstream.WithMsgID(id), jetstream.WithExpectLastSequencePerSubject(seq))
		done()
		if err == nil {
			s.countWrite(c.Contract, "ok")
			return StateWriteResult{Applied: true, Outcome: "ok", Revision: ack.Sequence}, nil
		}
		// 4. Alguien escribió entre el 1 y el 3: volver a 1. DOS códigos, no uno.
		if isSequenceConflict(err) {
			// Un poco de jitter es opcional (§3.3) y barato: sin él, tres instancias que chocan vuelven a chocar a la vez.
			if attempt < StateMaxAttempts {
				select {
				case <-time.After(time.Duration(rand.Float64()*12*float64(attempt)) * time.Millisecond):
				case <-ctx.Done():
					return StateWriteResult{}, ctx.Err()
				}
			}
			continue
		}
		if pd := deniedCause(gctx); pd != nil {
			return StateWriteResult{}, pd
		}
		if errors.Is(err, jetstream.ErrNoStreamResponse) || errors.Is(err, nats.ErrNoResponders) {
			return StateWriteResult{}, s.notProvisioned(stream, subject, err)
		}
		return StateWriteResult{}, err
	}
	s.countWrite(c.Contract, "conflict")
	return StateWriteResult{}, &StateConflictError{
		baseError: baseError{CodeStateConflict, ClassRetryable, nil}, Contract: c.Contract, Key: key, Attempts: StateMaxAttempts}
}

// isSequenceConflict dice si `err`, o su cadena de causas, es un conflicto de secuencia (10071 o 10164). Se comprueba el
// CÓDIGO de la API de JetStream, no el texto: el 10164 llega SIN número.
func isSequenceConflict(err error) bool {
	var api *jetstream.APIError
	if errors.As(err, &api) {
		for _, c := range stateConflictCodes {
			if int(api.ErrorCode) == c {
				return true
			}
		}
	}
	return false
}

// ─── leer ────────────────────────────────────────────────────────────────────

// Get hace una lectura puntual por `DIRECT.GET` sobre el subject exacto: no hay consumidor ni acuse. Devuelve found=false
// si la clave no existe O es una lápida. Es una lectura de RÉPLICA (§3.7): con `replicas: 3` PUEDE devolver el valor
// anterior justo después de un `Put`; quien necesite su propia escritura la deriva de `Revision`, no relee.
func (s *State) Get(ctx context.Context, contract, key string, opts ...StateOption) (StateEntry, bool, error) {
	cfg := applyStateOptions(opts)
	c, err := ParseStateContract(contract)
	if err != nil {
		return StateEntry{}, false, err
	}
	if err := assertStateKey(key); err != nil {
		return StateEntry{}, false, err
	}
	tenant, err := s.tenantFor(ctx, cfg)
	if err != nil {
		return StateEntry{}, false, err
	}
	subject, _ := StateSubject(c.Contract, tenant, key)
	stored, err := s.readRaw(ctx, StateStreamName(c.Domain), subject)
	if err != nil {
		return StateEntry{}, false, err
	}
	if stored == nil {
		return StateEntry{}, false, nil // 404 `Message Not Found`
	}
	d, err := s.decode(stored.data, decodeCtx{contract: c.Contract, tenant: tenant, key: key, revision: stored.seq, subject: subject}, nil)
	if err != nil {
		return StateEntry{}, false, err
	}
	if d.entry.Deleted {
		return StateEntry{}, false, nil
	}
	return d.entry, true, nil
}

// storedState es lo que DIRECT.GET devolvió de una clave.
type storedState struct {
	seq     uint64
	data    []byte
	ids     []string
	version int64
}

func (st *storedState) hasID(id string) bool {
	for _, x := range st.ids {
		if x == id {
			return true
		}
	}
	return false
}

// readRaw es `DIRECT.GET` por SUBJECT: la forma que se puede confinar por tenant con una ACL. La de cuerpo (`{"seq": n}`)
// lee cualquier mensaje y ninguna ACL la ve: no se usa. Devuelve nil si la clave no existe (404).
//
// Un stream que no existe, o sin `allow_direct`, NO da 404: no contesta nadie, y los dos casos se ven igual (A-32) →
// NOT_PROVISIONED, con las dos causas en el mensaje.
func (s *State) readRaw(ctx context.Context, stream, subject string) (*storedState, error) {
	api := "$JS.API.DIRECT.GET." + stream + "." + subject
	gctx, done := s.bus.guard(ctx, api)
	defer done()
	rctx, cancel := context.WithTimeout(gctx, directGetTimeout)
	defer cancel()
	r, err := s.bus.nc.RequestWithContext(rctx, api, nil)
	if err != nil {
		if pd := deniedCause(gctx); pd != nil {
			return nil, pd
		}
		if errors.Is(err, nats.ErrNoResponders) {
			return nil, s.notProvisioned(stream, subject, err)
		}
		return nil, err
	}
	switch status := r.Header.Get("Status"); status {
	case "":
	case "404":
		return nil, nil
	default:
		return nil, fmt.Errorf("flux: DIRECT.GET de %s contestó %s %s", subject, status, r.Header.Get("Description"))
	}
	seq, _ := strconv.ParseUint(r.Header.Get("Nats-Sequence"), 10, 64)
	st := &storedState{seq: seq, data: r.Data}
	if id := r.Header.Get("Nats-Msg-Id"); id != "" {
		st.ids = append(st.ids, id)
	}
	// Un valor ILEGIBLE no bloquea la clave (§3.3): si el cuerpo no es JSON, o no tiene un `aggregateVersion` válido, se
	// trata como «sin versión» y se sobrescribe con el chequeo de secuencia sobre SU `S`. De lo contrario una entrada rota
	// dejaría la clave inescribible para siempre, porque ningún servicio puede purgar el stream (06-security.md §3.5).
	var doc struct {
		ID   json.RawMessage `json:"id"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(r.Data, &doc) != nil {
		return st, nil
	}
	var id string
	if json.Unmarshal(doc.ID, &id) == nil && id != "" {
		st.ids = append(st.ids, id)
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(doc.Data, &obj) == nil {
		if v, ok := parseVersionLiteral(strings.TrimSpace(string(obj["aggregateVersion"]))); ok {
			st.version = v
		}
	}
	return st, nil
}

// notProvisioned: nadie contesta. El stream no existe, O existe sin `allow_direct: true`, y los dos casos se ven
// exactamente igual (A-32). El mensaje DEBE nombrar las dos causas y apuntar a `flux provision` (lo crea) y a `flux doctor`
// (lo audita). Si el broker DENIEGA la operación, es un permiso y no un recurso: eso es PERMISSION_DENIED.
func (s *State) notProvisioned(stream, sinRespuesta string, cause error) error {
	api := "$JS.API.DIRECT.GET." + stream + "." + sinRespuesta
	service := s.bus.opts.Service
	return newNotProvisioned(ResourceStream, stream, "", service, api, fmt.Sprintf(
		"nadie contestó en %q: el stream %q no existe, o existe sin `allow_direct: true`, y los dos casos se ven exactamente "+
			"igual (no hay un 404: no responde nadie). %q no lo crea ni lo consulta —los SDKs no usan `STREAM.INFO` sobre el "+
			"estado, porque lista todas las claves y todos los tenants (A-39)—. Aprovisiónalo con `flux provision`, con "+
			"credenciales de operación, y compruébalo con `flux doctor` (10-state.md §5.1, 04-errors.md §6).",
		api, stream, service), cause)
}

// ─── lo que comprueba el SDK al consumir ─────────────────────────────────────

type decodeCtx struct {
	contract string
	tenant   string
	key      string
	revision uint64
	subject  string
}

type decoded struct {
	event            Event
	entry            StateEntry
	signatureWarning bool
}

// decode es lo que comprueba el SDK al consumir, en el orden de 10-state.md §5.4. Lo que falla NUNCA se aplica y NUNCA
// llega al handler. `holder`, si no es nil, se rellena en cuanto el sobre es un CloudEvent válido, para que la DLQ pueda
// partir de él.
func (s *State) decode(raw []byte, c decodeCtx, holder *Event) (decoded, error) {
	// 1. Un CloudEvent válido con las cuatro extensiones (POISON, los códigos de siempre).
	ev, err := ParseEvent(raw)
	if err != nil {
		return decoded{}, err
	}
	if holder != nil {
		*holder = ev
	}

	// 2. La identidad está en el subject; el sobre la repite. Si discrepan, uno miente: POISON.
	mismatch := func(attr, envelope, subject string) error {
		return &StateSubjectMismatchError{baseError{CodeStateSubjectMismatch, ClassPoison, nil}, attr, envelope, subject}
	}
	if ev.TenantID != c.tenant {
		return decoded{}, mismatch("tenantid", ev.TenantID, c.tenant)
	}
	if ev.AggregateID != c.key {
		return decoded{}, mismatch("subject", ev.AggregateID, c.key)
	}
	put, _ := StateType(c.contract)
	tomb, _ := StateTombstoneType(c.contract)
	deleted := ev.Type == tomb
	if !deleted && ev.Type != put {
		return decoded{}, mismatch("type", ev.Type, put)
	}

	// 3. `restricted` no se admite en estado: en consumo es POISON.
	if ev.DataClassification == ClassificationRestricted {
		return decoded{}, &RestrictedStateEntryError{baseError{CodeInvalidDataClassif, ClassPoison, nil}}
	}

	// 4. `aggregateVersion` válido: sin él no se puede aplicar de forma condicional (PERMANENT).
	version, err := aggregateVersionOfData(ev.Data)
	if err != nil {
		return decoded{}, err
	}

	// La firma antes que el esquema: un evento manipulado puede validar perfectamente y no ser del productor que dice
	// (07-signing.md §5).
	warning := false
	if s.bus.verifier != nil {
		if err := s.bus.verifier.Check(ev); err != nil {
			return decoded{}, err
		}
	}

	// 5. Si hay L3 al consumir: `data` contra su esquema.
	if s.bus.opts.Validation.OnConsume {
		if err := s.validate(ev, c.subject); err != nil {
			return decoded{}, err
		}
	}

	entry := StateEntry{Key: c.key, TenantID: c.tenant, Deleted: deleted, AggregateVersion: version, Revision: c.revision}
	if !deleted {
		entry.Data = ev.Data
	}
	return decoded{event: ev, entry: entry, signatureWarning: warning}, nil
}

// ─── watch ───────────────────────────────────────────────────────────────────

// watchState es lo que un `Watch` necesita en cada entrega.
type watchState struct {
	contract string
	tenant   string
	stream   string
	durable  string
	// consumerLabel es el durable SIN el sufijo de tenant: la etiqueta `consumer` de las métricas (08-observability.md §2.4).
	consumerLabel string
	// subjectLabel es `state.<contrato>`: la etiqueta `subject` de las métricas.
	subjectLabel string
	handler      StateHandler
	sync         *syncTracker
	requested    jetstream.ConsumerConfig
}

// syncTracker cuenta cuántas entregas de un `Watch` siguen en vuelo y cuándo se alcanzó «lo que había al abrir».
type syncTracker struct {
	mu       sync.Mutex
	fire     func()
	done     bool
	inflight int
	sawLast  bool
}

// immediate: al abrir, nada pendiente.
func (t *syncTracker) immediate() {
	t.mu.Lock()
	already := t.done
	t.done = true
	t.mu.Unlock()
	if !already {
		t.fire()
	}
}

// started anota una entrega. `pending` es el número que el broker pone en los metadatos de cada entrega: cuántas quedan
// por entregar. 0 = esta es la última de la foto (A-35).
func (t *syncTracker) started(pending uint64) {
	t.mu.Lock()
	t.inflight++
	if pending == 0 {
		t.sawLast = true
	}
	t.mu.Unlock()
}

// finished: con éxito o con error —una entrada RETRYABLE que falla NO retrasa `onSynced` (10-state.md §5.5)—.
func (t *syncTracker) finished() {
	t.mu.Lock()
	t.inflight--
	fire := !t.done && t.sawLast && t.inflight == 0
	if fire {
		t.done = true
	}
	t.mu.Unlock()
	if fire {
		t.fire()
	}
}

// pendingSum: `flux_consumer_pending` de un contrato de estado es la SUMA de los durables del servicio, no el valor del
// último sondeado (08-observability.md §2.4). Un `Watch` por tenant crea un durable por tenant, y todos publican las
// MISMAS etiquetas —el `consumer` va SIN el sufijo del tenant, o cada tenant nuevo crearía series nuevas—: sin sumar, se
// pisarían entre sí.
type pendingSum struct {
	mu       sync.Mutex
	porSerie map[string]map[string]int
}

func (p *pendingSum) note(serie, durable string, pending int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.porSerie == nil {
		p.porSerie = map[string]map[string]int{}
	}
	d := p.porSerie[serie]
	if d == nil {
		d = map[string]int{}
		p.porSerie[serie] = d
	}
	d[durable] = pending
	return sumValues(d)
}

// drop: un durable deja de sumar (su `Watch` paró). Devuelve la suma que queda y ok=false si no había nada.
func (p *pendingSum) drop(serie, durable string) (int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	d := p.porSerie[serie]
	if _, ok := d[durable]; !ok {
		return 0, false
	}
	delete(d, durable)
	if len(d) == 0 {
		delete(p.porSerie, serie)
	}
	return sumValues(d), true
}

func sumValues(d map[string]int) int {
	n := 0
	for _, v := range d {
		n += v
	}
	return n
}

// Watch entrega la foto completa y después los cambios, por el MISMO canal: un consumidor durable de `STATE_<DOM>` con
// `deliver_policy: last_per_subject`. Un consumidor que estuvo caído recibe solo el último valor de cada clave; un valor
// reemplazado mientras estaba en vuelo NO se reentrega (A-35, A-36). El handler DEBE ser idempotente y no asumir orden y,
// ante `entry.Deleted`, DEBE borrar su copia local.
//
// Un tenant por `Watch`, y cada llamada es un durable: un servicio que observa N tenants llama N veces, y cada llamada
// recibe su `WithOnSynced` (10-state.md §5.5).
func (s *State) Watch(ctx context.Context, contract string, handler StateHandler, opts ...StateOption) (*StateSubscription, error) {
	cfg := applyStateOptions(opts)
	c, err := ParseStateContract(contract)
	if err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("flux: el handler de Watch no puede ser nil")
	}
	o := s.bus.opts
	tenant, err := s.watchTenant(cfg)
	if err != nil {
		return nil, err
	}
	if o.TenantIsolation == TenantIsolationStrict && tenant == "" {
		return nil, &TenantIsolationError{Subject: "state." + c.Contract,
			Reason: "no hay TenantID ni en ConnectOptions ni en WithStateTenant"}
	}
	stream := StateStreamName(c.Domain)
	durable, err := StateDurableName(o.Service, c.Contract, tenant)
	if err != nil {
		return nil, err
	}
	label, _ := StateDurableName(o.Service, c.Contract, "")
	filter, _ := StateFilterSubject(c.Contract, tenant)
	fire := func() {
		if cfg.onSynced != nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						s.bus.logf(slog.LevelError, "WithOnSynced entró en pánico: %v", r)
					}
				}()
				cfg.onSynced()
			}()
		}
	}
	w := &watchState{
		contract: c.Contract, tenant: tenant, stream: stream, durable: durable, consumerLabel: label,
		subjectLabel: "state." + c.Contract, handler: handler, sync: &syncTracker{fire: fire},
		requested: jetstream.ConsumerConfig{
			Durable: durable, FilterSubject: filter, AckPolicy: jetstream.AckExplicitPolicy,
			// ack_wait DEBE ser backoff[0]: JetStream lo sobrescribe con ese valor sin avisar.
			AckWait: DefaultAckWait, MaxDeliver: DefaultMaxDeliver, BackOff: CanonicalBackOff(),
			MaxAckPending: DefaultMaxAckPending, DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy,
			ReplayPolicy: jetstream.ReplayInstantPolicy,
		},
	}

	// El SDK comprueba la DLQ al ARRANCAR, no cuando llega el primer mensaje envenenado.
	if err := s.ensureStateDLQ(ctx, c.Domain); err != nil {
		return nil, err
	}
	consumer, effective, err := s.ensureConsumer(ctx, stream, w.requested)
	if err != nil {
		return nil, err
	}
	if err := assertStateConfigHonored(durable, w.requested, effective); err != nil {
		return nil, err
	}
	if err := s.bus.ensurePullAllowed(ctx, stream, durable); err != nil {
		return nil, err
	}

	// Stream vacío, o durable al día: `onSynced` de INMEDIATO, antes de entregar nada y aunque no llegue ninguna entrada
	// después. Esperar a una que quizá no llegue nunca dejaría al servicio «sincronizando» para siempre (10-state.md §5.5,
	// punto 2). `NumAckPending` también: lo entregado y sin confirmar sigue pendiente de procesar.
	info := consumer.CachedInfo()
	if info.NumPending == 0 && info.NumAckPending == 0 {
		w.sync.immediate()
	}

	runCtx, cancel := context.WithCancel(context.Background())
	sub := &StateSubscription{Contract: c.Contract, Durable: durable, TenantID: tenant}
	done := make(chan struct{})
	sub.stop = func() {
		cancel()
		<-done
		s.dropPending(w)
		s.mu.Lock()
		delete(s.watches, sub)
		s.mu.Unlock()
	}
	s.mu.Lock()
	s.watches[sub] = struct{}{}
	s.mu.Unlock()

	var pollStop chan struct{}
	if every := s.bus.pendingPollInterval(); every > 0 {
		pollStop = make(chan struct{})
		go pollPending(every, pollStop,
			func() (uint64, error) {
				pctx, pcancel := context.WithTimeout(context.Background(), every)
				defer pcancel()
				i, err := consumer.Info(pctx)
				if err != nil {
					return 0, err
				}
				return i.NumPending, nil
			},
			func(p uint64) { s.notePending(w, int(p)) },
			func(err error) { s.bus.logf(slog.LevelWarn, "no se pudo sondear num_pending de %s: %v", durable, err) })
	}
	go func() {
		defer close(done)
		if pollStop != nil {
			defer close(pollStop)
		}
		s.consumeLoop(runCtx, w, consumer)
	}()
	return sub, nil
}

// consumeLoop es el bucle de un `Watch`: entrega cada mensaje a `process` y, si el durable se BORRA mientras el watch vive
// (`flux state resync`: borrar y recrear), REABRE el consumo.
//
// Por qué hace falta. Al borrar el durable, el broker contesta al pull pendiente `409 Consumer Deleted` y la librería
// PARA el consumo ella misma (`ErrConsumerDeleted` al `ErrHandler`, medido en ~1 ms): no vuelve a pedir. Aquí se vuelve a
// consultar el durable por su nombre —`CONSUMER.INFO`, que el observador ya tiene; NO hace falta `CONSUMER.CREATE`— y se
// vuelve a consumir. `onSynced` no se invoca de nuevo.
func (s *State) consumeLoop(ctx context.Context, w *watchState, consumer jetstream.Consumer) {
	for ctx.Err() == nil {
		deleted := make(chan struct{}, 1)
		cc, err := consumer.Consume(func(m jetstream.Msg) { s.dispatch(m, w) },
			jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
				if errors.Is(err, jetstream.ErrConsumerDeleted) {
					select {
					case deleted <- struct{}{}:
					default:
					}
					return
				}
				s.bus.logf(slog.LevelWarn, "el consumo de %s avisó de: %v", w.subjectLabel, err)
			}))
		if err != nil {
			s.bus.logf(slog.LevelError, "no se pudo iniciar el consumo de %s: %v", w.durable, err)
			if next := s.reopen(ctx, w); next != nil {
				consumer = next
				continue
			}
			return
		}
		select {
		case <-ctx.Done():
			// Drain y no Stop: lo que ya está en el búfer se entrega antes de parar, y no se reentrega.
			cc.Drain()
			<-cc.Closed()
			return
		case <-deleted:
			cc.Stop()
			next := s.reopen(ctx, w)
			if next == nil {
				return
			}
			consumer = next
		case <-cc.Closed():
			// La librería paró el consumo por su cuenta (no por haberse borrado el durable): se reabre.
			if ctx.Err() != nil {
				return
			}
			next := s.reopen(ctx, w)
			if next == nil {
				return
			}
			consumer = next
		}
	}
}

// reopen vuelve a abrir el consumo de un durable que se borró bajo el watch. Reintenta con backoff (100 ms … 5 s) hasta que
// el durable exista —`flux state resync` lo recrea a continuación— o el watch pare. Comprueba la configuración como al abrir:
// un durable recreado con otro filtro entregaría las claves de otro tenant sin ningún error, y ese no se consume.
func (s *State) reopen(ctx context.Context, w *watchState) jetstream.Consumer {
	s.bus.logf(slog.LevelWarn, "el durable %s se borró con el watch de %s abierto (¿`flux state resync`?): se vuelve a abrir el "+
		"consumo cuando exista de nuevo.", w.durable, w.subjectLabel)
	ultimo := ""
	for intento := 1; ctx.Err() == nil; intento++ {
		c, err := s.bus.js.Consumer(ctx, w.stream, w.durable) // CONSUMER.INFO
		if err == nil {
			err = assertStateConfigHonored(w.durable, w.requested, c.CachedInfo().Config)
		}
		if err == nil {
			s.bus.logf(slog.LevelWarn, "consumo de %s reabierto (intento %d): se entregará la foto otra vez.", w.durable, intento)
			return c
		}
		// `consumer not found` (10014): el resync está entre el borrado y la creación. Se espera. Se avisa la primera vez, cada
		// 10 intentos y SIEMPRE que la causa cambia (un durable recreado con otra configuración no es «aún no existe»).
		if motivo := err.Error(); intento == 1 || intento%10 == 0 || motivo != ultimo {
			s.bus.logf(slog.LevelWarn, "no se pudo reabrir el consumo de %s (intento %d): %s. Se reintenta.", w.durable, intento, motivo)
			ultimo = motivo
		}
		espera := 100 * time.Millisecond << min(intento-1, 6)
		if espera > 5*time.Second {
			espera = 5 * time.Second
		}
		select {
		case <-time.After(espera):
		case <-ctx.Done():
		}
	}
	s.bus.logf(slog.LevelWarn, "el watch de %s se detuvo sin haber podido reabrir el consumo de %s", w.subjectLabel, w.durable)
	return nil
}

// ─── despacho de una entrada ─────────────────────────────────────────────────

func (s *State) dispatch(m jetstream.Msg, w *watchState) {
	md, mdErr := m.Metadata()
	pending := uint64(1)
	if mdErr == nil {
		pending = md.NumPending
		s.notePending(w, int(md.NumPending))
	}
	w.sync.started(pending)
	defer w.sync.finished()
	defer func() {
		// Un fallo inesperado de `process` rompería el consumo y el watch dejaría de consumir EN SILENCIO, con la conexión
		// viva: un consumidor muerto que parece vivo.
		if r := recover(); r != nil {
			s.bus.logf(slog.LevelError, "fallo inesperado despachando el estado %s: %v. El mensaje no se confirma y será reentregado.", w.subjectLabel, r)
		}
	}()
	s.process(m, md, mdErr, w)
}

func (s *State) process(m jetstream.Msg, md *jetstream.MsgMetadata, mdErr error, w *watchState) {
	b := s.bus
	attempt := 1
	var revision uint64
	if mdErr == nil {
		if md.NumDelivered > 0 {
			attempt = int(md.NumDelivered)
		}
		revision = md.Sequence.Stream
	}
	// `state.<dom>.<agr>.v<M>.<tenant>.<clave>`: la identidad la decide EL BROKER, no el sobre.
	partes := strings.Split(m.Subject(), ".")
	var tenant, key string
	if len(partes) == 6 {
		tenant, key = partes[4], partes[5]
	}
	inicio := time.Now()

	// WIP mientras el handler vive: evita que la misma entrada se ejecute en concurrencia consigo misma.
	stopWIP := make(chan struct{})
	go func() {
		t := time.NewTicker(DefaultAckWait / 2)
		defer t.Stop()
		for {
			select {
			case <-stopWIP:
				return
			case <-t.C:
				_ = m.InProgress()
			}
		}
	}()
	defer close(stopWIP)

	var parsed Event
	d, err := s.decode(m.Data(), decodeCtx{contract: w.contract, tenant: tenant, key: key, revision: revision, subject: m.Subject()}, &parsed)
	if err == nil {
		hctx := WithEventContext(context.Background(), ContextFromEvent(d.event))
		err = s.invokeStateHandler(hctx, w, d.entry, StateDelivery{
			Attempt: attempt, MaxAttempts: DefaultMaxDeliver, Subject: m.Subject(), Contract: w.contract})
	}
	if err == nil {
		_ = m.Ack()
		b.metrics.EventConsumed(w.subjectLabel, w.consumerLabel, ConsumeOK)
		b.metrics.HandlerDuration(w.subjectLabel, w.consumerLabel, time.Since(inicio).Seconds())
		return
	}

	c := b.classify(err)
	budget := DefaultMaxDeliver
	if c.MaxAttempts > 0 && c.MaxAttempts < budget {
		budget = c.MaxAttempts
	}
	if c.Class == ClassRetryable && attempt < budget {
		delay := c.RetryAfter
		if delay <= 0 {
			bo := CanonicalBackOff()
			delay = bo[min(attempt-1, len(bo)-1)]
		}
		b.logf(slog.LevelWarn, "RETRYABLE %s en %s (clave %s, intento %d/%d), reintento en %s", c.Code, w.subjectLabel, key, attempt, budget, delay)
		b.metrics.EventRetried(w.subjectLabel, w.consumerLabel, attempt)
		_ = m.NakWithDelay(delay)
		return
	}

	reason := DLQReasonPermanent
	switch c.Class {
	case ClassPoison:
		reason = DLQReasonPoison
	case ClassRetryable:
		reason = DLQReasonRetryable
	}
	// `dlqerror` NO DEBE contener valores de `data` (10-state.md §6). Un error de flux lleva su código y un mensaje que no
	// cita valores. Uno que no sea de flux —una excepción de un driver de base de datos, que puede citar la fila entera— se
	// reduce a su código: la DLQ guarda 90 días y en estado `data` es dato personal.
	dlqError := c.Code + ": " + UnclassifiedDLQError
	if IsClassified(err) {
		dlqError = c.Code + ": " + err.Error()
	}
	info := StateDLQInfo{Key: key, Revision: revision, Reason: reason, Attempts: attempt, Consumer: w.durable,
		Error: dlqError, DLQTime: FormatTime(time.Now())}
	var dlqEvent Event
	var bErr error
	if parsed.ID != "" {
		dlqEvent, bErr = ToStateDLQEvent(parsed, info)
	} else {
		id, _ := uuid.NewV7()
		dlqEvent, bErr = BuildUnreadableStateDLQEvent(UnreadableStateDLQInput{
			Contract: w.contract, TenantID: tenant, Key: key, Revision: revision, Source: b.source,
			ProducerVersion: b.opts.Version, ID: id.String(), Time: FormatTime(time.Now()),
			DataSchema: s.schemaForSafe(w.contract), DLQ: info})
	}
	if bErr != nil {
		b.logf(slog.LevelError, "no se pudo construir el evento de DLQ de estado de %s (clave %s): %v. El mensaje NO se termina; se reentregará.", w.subjectLabel, key, bErr)
		return
	}
	payload, bErr := Serialize(dlqEvent)
	if bErr == nil {
		dlqSubject, _ := StateDLQSubject(w.contract)
		pctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		gctx, done := b.guard(pctx, dlqSubject)
		_, bErr = b.js.Publish(gctx, dlqSubject, payload, jetstream.WithMsgID("dlq-"+dlqEvent.ID+"-"+w.durable))
		done()
		cancel()
	}
	if bErr != nil {
		// NO se hace term(): terminar aquí borraría la entrada sin haberla guardado en ningún sitio.
		b.logf(slog.LevelError, "no se pudo publicar en la DLQ de estado para %s (clave %s): %v. El mensaje NO se termina; se reentregará.", w.subjectLabel, key, bErr)
		return
	}

	outcome := outcomeFor(reason, c.Code)
	if c.Class == ClassPoison {
		// POISON es lo que despierta a alguien: term() + DLQ + ALERTA. `Raw` va vacío a propósito: el cuerpo de una entrada de
		// estado puede llevar datos personales (10-state.md §6).
		if b.opts.OnPoison != nil {
			b.opts.OnPoison(PoisonInfo{Subject: m.Subject(), Err: err})
		}
		b.logf(slog.LevelError, "POISON %s en %s (clave %s): %s", c.Code, w.subjectLabel, key, c.Code)
	} else {
		b.logf(slog.LevelError, "DLQ de estado (%s) %s en %s (clave %s) tras %d intento(s)", reason, c.Code, w.subjectLabel, key, attempt)
	}
	b.metrics.EventConsumed(w.subjectLabel, w.consumerLabel, outcome)
	b.metrics.EventDLQ(w.subjectLabel, w.consumerLabel, reason, c.Code)
	b.metrics.HandlerDuration(w.subjectLabel, w.consumerLabel, time.Since(inicio).Seconds())
	if b.opts.OnDLQ != nil {
		b.opts.OnDLQ(DLQEventInfo{Subject: m.Subject(), Event: dlqEvent, Classification: c})
	}
	_ = m.Term()
}

// invokeStateHandler aísla el pánico del handler: se convierte en PERMANENT, como en los eventos.
func (s *State) invokeStateHandler(ctx context.Context, w *watchState, e StateEntry, d StateDelivery) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = NewPermanentError(fmt.Sprintf("pánico en el handler de %s: %v", w.subjectLabel, r), WithCode("HANDLER_PANIC"))
		}
	}()
	return w.handler(ctx, e, d)
}

// ─── consumidor y DLQ ────────────────────────────────────────────────────────

// ensureConsumer toma el consumidor durable si existe y, solo si no existe, lo crea (desarrollo). `Watch` NO necesita
// `STREAM.INFO.STATE_<DOM>` —que lista todas las claves y todos los tenants (A-39)—: `CONSUMER.INFO` distingue «no existe el
// consumidor» (10014) de «no existe el stream» (10059) (A-48), que es todo lo que hace falta para dar NOT_PROVISIONED o
// PERMISSION_DENIED.
func (s *State) ensureConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, jetstream.ConsumerConfig, error) {
	return s.bus.ensureConsumer(ctx, stream, cfg)
}

// assertStateConfigHonored es assertConfigHonored más lo propio del estado: la política de entrega y el filtro. Un durable
// aprovisionado con otro filtro entregaría un conjunto distinto del pedido (otro tenant, todos los tenants) sin ningún error.
func assertStateConfigHonored(durable string, requested, effective jetstream.ConsumerConfig) error {
	err := assertConfigHonored(durable, requested, effective)
	var mismatch *ConsumerConfigMismatchError
	if err != nil && !errors.As(err, &mismatch) {
		return err
	}
	var diffs []ConfigDifference
	if mismatch != nil {
		diffs = mismatch.Differences
	}
	if effective.DeliverPolicy != requested.DeliverPolicy {
		diffs = append(diffs, ConfigDifference{"deliver_policy", requested.DeliverPolicy, effective.DeliverPolicy})
	}
	efectivo := effective.FilterSubject
	if efectivo == "" && len(effective.FilterSubjects) == 1 {
		efectivo = effective.FilterSubjects[0]
	}
	if efectivo != requested.FilterSubject || len(effective.FilterSubjects) > 1 {
		diffs = append(diffs, ConfigDifference{"filter_subject", requested.FilterSubject, effective.FilterSubjects})
	}
	if len(diffs) > 0 {
		return &ConsumerConfigMismatchError{Durable: durable, Differences: diffs}
	}
	return nil
}

// ensureStateDLQ comprueba que `DLQ_<DOM>` captura `dlq.state.<dom>.>`. Un dominio solo de eventos conserva su `DLQ_<DOM>`
// de la v1.0 sin tocarlo: ampliarlo es posible y no pierde mensajes (A-44), pero no se le pide a quien no usa estado, y NO lo
// hace el SDK sobre uno existente: lo hace `flux provision`. Si no captura, se AVISA al abrir, y no cuando el primer mensaje
// muera.
func (s *State) ensureStateDLQ(ctx context.Context, domain string) error {
	name := DLQStreamName(domain)
	s.mu.Lock()
	_, ok := s.dlqChecked[name]
	s.mu.Unlock()
	if ok {
		return nil
	}
	b := s.bus
	muestra := "dlq.state." + domain + ".x.v1"
	gctx, done := b.guard(ctx, "$JS.API.STREAM.INFO."+name)
	st, err := b.js.Stream(gctx, name)
	done()
	switch {
	case err == nil:
		captura := false
		for _, subj := range st.CachedInfo().Config.Subjects {
			captura = captura || subjectMatches(subj, muestra)
		}
		if !captura {
			b.logf(slog.LevelWarn, "%s no captura \"dlq.state.%s.>\": una entrada de estado que muera no se podrá guardar y se "+
				"reentregará. Un dominio con estado lo necesita; lo amplía `flux provision` y lo audita `flux doctor` (10-state.md §6).", name, domain)
		}
	case errors.Is(err, jetstream.ErrStreamNotFound):
		// Desarrollo: se crea con LOS DOS subjects. En producción lo aprovisiona `flux provision`.
		cctx, cdone := b.guard(ctx, "$JS.API.STREAM.CREATE."+name)
		_, err = b.js.CreateStream(cctx, jetstream.StreamConfig{
			Name: name, Subjects: []string{"dlq." + domain + ".>", "dlq.state." + domain + ".>"},
			Storage: jetstream.FileStorage, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardOld, MaxAge: DLQStreamMaxAge,
		})
		cdone()
		if pd := deniedCause(cctx); pd != nil {
			return newNotProvisioned(ResourceStream, name, "", b.opts.Service, pd.Subject, "", pd)
		}
		if err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			return fmt.Errorf("flux: no se pudo crear el stream %q: %w", name, err)
		}
	default:
		if pd := deniedCause(gctx); pd != nil {
			return pd
		}
		return fmt.Errorf("flux: no se pudo consultar el stream %q: %w", name, err)
	}
	s.mu.Lock()
	s.dlqChecked[name] = struct{}{}
	s.mu.Unlock()
	return nil
}

// subjectMatches compara un patrón de subject de NATS (`*`, `>`) con un subject concreto.
func subjectMatches(pattern, subject string) bool {
	p, t := strings.Split(pattern, "."), strings.Split(subject, ".")
	for i := range p {
		if p[i] == ">" {
			return len(t) > i
		}
		if i >= len(t) {
			return false
		}
		if p[i] != "*" && p[i] != t[i] {
			return false
		}
	}
	return len(p) == len(t)
}

// ─── métricas ────────────────────────────────────────────────────────────────

// notePending: `flux_consumer_pending` es la SUMA de los durables del servicio para ese contrato.
func (s *State) notePending(w *watchState, pending int) {
	sum := s.pending.note(w.subjectLabel+"|"+w.consumerLabel, w.durable, pending)
	s.bus.metrics.ConsumerPending(w.subjectLabel, w.consumerLabel, sum)
}

// dropPending: al parar un `Watch`, su durable deja de sumar.
func (s *State) dropPending(w *watchState) {
	if sum, ok := s.pending.drop(w.subjectLabel+"|"+w.consumerLabel, w.durable); ok {
		s.bus.metrics.ConsumerPending(w.subjectLabel, w.consumerLabel, sum)
	}
}
