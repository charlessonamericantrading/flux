// Petición/respuesta — el cliente: `bus.Request` y `bus.Respond`.
// Contrato normativo: specification/11-request-reply.md, specification/04-errors.md §7
//
// Las funciones puras que construyen el sobre, el subject y el timeout están en rpc_pure.go.
//
// ⚠️ Lo que se MIDIÓ de `nats.go` v1.37.0 contra nats-server 2.14.5 para esto (PLAN.md, A-190 a A-199):
//
//   - `nc.Request` a un subject denegado es un `ErrTimeout` tras su plazo: la denegación llega por el `ErrorHandler` de la
//     conexión (~1 ms) y NO por la petición. Por eso `Request` COMPITE la respuesta con el `-ERR` (denials.go): sin ello, un
//     permiso que falta se vería como RPC_TIMEOUT (RETRYABLE) y el handler que llama reintentaría 51 minutos algo que nunca
//     va a funcionar (11-request-reply.md §7.1).
//   - Con un inbox privado cuya suscripción de respuestas está denegada la petición TAMBIÉN espera su timeout (la
//     suscripción denegada solo llega al `ErrorHandler`): se vigila también `_INBOX.<servicio>.*` (A-107).
//   - `no responders` llega como `ErrNoResponders` en 0 ms: `nats.go` envía `headers` y `no_responders` en CONNECT sin que haya
//     opción para quitarlos, y `nc.Request` usa UNA suscripción de buzón por conexión. El SDK NO abre una segunda suscripción a
//     su propio inbox por la misma conexión (A-101).
//   - Una suscripción denegada (`respond`) no devuelve error: `QueueSubscribe` devuelve nil y el `-ERR` solo queda en
//     `nc.LastError()` tras un `Flush()` (determinista) y, más tarde, en el `ErrorHandler`. Ver denials.go.
//   - `Drain` POR SUSCRIPCIÓN: con handlers en vuelo, 3 de 3 respuestas llegan y una petición nueva da `no responders` al
//     instante. `nc.Drain()` de la conexión entera pierde las 3 (`connection closed`, A-83).

package flux

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// ─── Opciones ────────────────────────────────────────────────────────────────

type requestConfig struct {
	timeout        time.Duration
	hasTimeout     bool
	tenantID       string
	aggregateID    string
	classification DataClassification
	retries        int
}

// RequestOption ajusta una llamada de Request.
type RequestOption func(*requestConfig)

// WithRPCTimeout fija cuánto esperar a la respuesta de UN intento: un número ENTERO de milisegundos entre 1 y 25 000 (5 000 por
// defecto). Fuera de rango es INVALID_RPC_CALL: no se recorta en silencio.
func WithRPCTimeout(d time.Duration) RequestOption {
	return func(c *requestConfig) { c.timeout, c.hasTimeout = d, true }
}

// WithRPCTenant fija el tenant. Si no, el del evento (o la petición) en curso, el de ConnectOptions y, si no hay, `system`.
func WithRPCTenant(tenantID string) RequestOption {
	return func(c *requestConfig) { c.tenantID = tenantID }
}

// WithRPCAggregateID fija la identidad del agregado sobre el que se pide (`cli-1`): va al atributo `subject` del sobre. NO es
// el subject de NATS.
func WithRPCAggregateID(id string) RequestOption {
	return func(c *requestConfig) { c.aggregateID = id }
}

// WithRPCClassification fija la clasificación: `public`, `internal`, `confidential` o `restricted`. Si no, la de ConnectOptions.
func WithRPCClassification(cl DataClassification) RequestOption {
	return func(c *requestConfig) { c.classification = cl }
}

// WithRPCRetries pide reintentos ante RPC_UNAVAILABLE y RPC_TIMEOUT —solo esos: nunca ante un error permanente—, por defecto 0.
// CADA REINTENTO REUTILIZA EL MISMO `id` Y LOS MISMOS BYTES (`time` incluido): con un `id` nuevo por intento, quien responde no
// podría reconocer la orden repetida, y RPC_TIMEOUT NO dice que la operación no se ejecutó (11-request-reply.md §4.5).
func WithRPCRetries(n int) RequestOption {
	return func(c *requestConfig) { c.retries = n }
}

type respondConfig struct {
	classification DataClassification
	maxConcurrent  int
}

// RespondOption ajusta un Respond.
type RespondOption func(*respondConfig)

// WithRespondClassification fija la clasificación de las respuestas de esta operación. Si no, la de ConnectOptions. El error es
// siempre `internal`.
func WithRespondClassification(c DataClassification) RespondOption {
	return func(cfg *respondConfig) { cfg.classification = c }
}

// WithMaxConcurrent fija los handlers concurrentes de ESTA operación (256 por defecto: el orden de `max_ack_pending`). El exceso
// espera en la cola de la suscripción: core NATS no tiene acuse, y un responder sobrecargado no puede decir «ahora no»
// (11-request-reply.md §5.6, §13).
func WithMaxConcurrent(n int) RespondOption {
	return func(c *respondConfig) { c.maxConcurrent = n }
}

// ─── Lo que ve el handler ────────────────────────────────────────────────────

// RPCRequest es lo que recibe un handler: los datos y el sobre de la petición.
type RPCRequest struct {
	// Operation es `clientes.cliente.v1.calcular-descuento`.
	Operation string
	// Event es el sobre COMPLETO de la petición.
	Event Event
	// ID es el `id` de la petición: LA CLAVE DE DEDUPLICACIÓN (una orden puede llegar dos veces: 11-request-reply.md §4.5).
	ID            string
	CorrelationID string
	// TenantID es el de la petición. ES DEL HANDLER AUTORIZARLO: quien responde no filtra por el tenant de ConnectOptions (§5.6).
	TenantID string
	// AggregateID es el atributo `subject` del sobre, si la petición lo traía.
	AggregateID string
	Time        string
	// Data es el `data` de la petición. Se tipa con UnmarshalRPCData.
	Data json.RawMessage
}

// UnmarshalRPCData decodifica el `data` de una petición en el tipo de la aplicación.
func UnmarshalRPCData[T any](r RPCRequest) (T, error) {
	var out T
	if err := json.Unmarshal(r.Data, &out); err != nil {
		return out, NewPermanentError(fmt.Sprintf("la petición %s a %s no encaja en el tipo esperado", r.ID, r.Operation),
			WithCode("DATA_SCHEMA_MISMATCH"), WithCause(err))
	}
	return out, nil
}

// RPCHandler atiende una petición. Devuelve la respuesta —un objeto JSON— o un error: NewPermanentError / NewRetryableError
// responden un error con su clase, su código y su mensaje; cualquier otro, un `permanent` UNKNOWN genérico (11-request-reply.md
// §5.3). El ctx lleva el contexto de la petición: una llamada hecha desde el handler continúa la cadena de correlación.
type RPCHandler func(ctx context.Context, req RPCRequest) (any, error)

// ─── Métricas ────────────────────────────────────────────────────────────────

// RPCMetricsSink es la extensión OPCIONAL de MetricsSink de la capacidad `rpc` (08-observability.md §2.5; `protocol.json` →
// `rpc.metrics`): un MetricsSink escrito antes de la capacidad sigue compilando. `operation` es
// `<dominio>.<agregado>.v<major>.<operacion>` —sin `rpc.`—, acotado por las operaciones declaradas. NUNCA el tenant, el `id`, el
// `correlationid` ni quién llama.
type RPCMetricsSink interface {
	// RPCClientRequest cuenta una llamada: ok, unavailable, timeout, invalid_reply, invalid_schema, retryable o permanent.
	RPCClientRequest(operation, outcome string)
	// RPCServerRequest cuenta una petición recibida: ok, retryable, permanent, poison, invalid_schema, invalid_signature o stale_request.
	RPCServerRequest(operation, outcome string)
	// RPCDuration mide una llamada (`client`) o lo que tarda el handler (`server`).
	RPCDuration(operation, role string, seconds float64)
}

func (c *rpcClient) metrics() RPCMetricsSink {
	if m, ok := c.bus.metrics.(RPCMetricsSink); ok {
		return m
	}
	return nil
}

// ─── El cliente ──────────────────────────────────────────────────────────────

type rpcClient struct {
	bus *Bus

	mu         sync.Mutex
	responders map[string]*Responder
}

func newRPCClient(b *Bus) *rpcClient {
	return &rpcClient{bus: b, responders: map[string]*Responder{}}
}

// Responder es una operación atendida. Se obtiene con Bus.Respond.
type Responder struct {
	// Operation es `clientes.cliente.v1.calcular-descuento`.
	Operation string
	// Subject es el subject EXACTO al que se suscribió: `rpc.clientes.cliente.v1.calcular-descuento`. Nunca `rpc.<dominio>.>`.
	Subject string

	c              *rpcClient
	handler        RPCHandler
	classification DataClassification
	requestURI     string
	replyURI       string
	sub            *nats.Subscription
	sem            chan struct{}
	inflight       sync.WaitGroup

	stopOnce sync.Once
	stopped  chan struct{}
	closed   chan struct{}
}

// ─── llamar ──────────────────────────────────────────────────────────────────

type preparedRequest struct {
	operation string
	subject   string
	bytes     []byte
	event     Event
	replyURI  string
	timeout   time.Duration
	retries   int
}

// Request hace una **orden** o un **cálculo** con respuesta inmediata a otro servicio (11-request-reply.md). Devuelve el `data`
// de la respuesta. Hace UN intento, salvo WithRPCRetries.
//
// Todo lo que se puede comprobar sin red se comprueba ANTES, en este orden y el primero que falla gana: RESERVED_DOMAIN →
// INVALID_RPC_CALL → INVALID_DATACLASSIFICATION → L3 (INVALID_SCHEMA) → construir el sobre UNA vez → tamaño
// (RPC_MESSAGE_TOO_LARGE).
func (b *Bus) Request(ctx context.Context, operation string, data any, opts ...RequestOption) (json.RawMessage, error) {
	return b.rpc.request(ctx, operation, data, opts)
}

// RequestAs es Request con la respuesta ya decodificada en el tipo de la aplicación.
func RequestAs[T any](ctx context.Context, b *Bus, operation string, data any, opts ...RequestOption) (T, error) {
	var out T
	raw, err := b.Request(ctx, operation, data, opts...)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, NewPermanentError(fmt.Sprintf("la respuesta a %s no encaja en el tipo esperado", operation),
			WithCode("DATA_SCHEMA_MISMATCH"), WithCause(err))
	}
	return out, nil
}

func (c *rpcClient) request(ctx context.Context, operation string, data any, opts []RequestOption) (json.RawMessage, error) {
	prep, err := c.prepare(ctx, operation, data, opts)
	if err != nil {
		return nil, err
	}
	m := c.metrics()
	inicio := time.Now()
	for intento := 1; ; intento++ {
		out, err := c.attempt(ctx, prep)
		if err == nil {
			if m != nil {
				m.RPCClientRequest(prep.operation, "ok")
				m.RPCDuration(prep.operation, "client", time.Since(inicio).Seconds())
			}
			return out, nil
		}
		// Solo RPC_UNAVAILABLE y RPC_TIMEOUT se reintentan, con el MISMO `id` y los mismos bytes.
		var un *RPCUnavailableError
		var to *RPCTimeoutError
		isUn, isTo := errors.As(err, &un), errors.As(err, &to)
		if intento <= prep.retries && (isUn || isTo) {
			if isUn {
				// RPC_UNAVAILABLE falla al instante: una pausa evita martillear a un servicio que arranca.
				select {
				case <-time.After(50 * time.Millisecond * time.Duration(intento)):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			continue
		}
		if m != nil {
			if outcome := clientOutcome(err); outcome != "" {
				m.RPCClientRequest(prep.operation, outcome)
			}
			m.RPCDuration(prep.operation, "client", time.Since(inicio).Seconds())
		}
		return nil, err
	}
}

// prepare es el paso 0 de §4.1: validar y construir SIN red.
func (c *rpcClient) prepare(ctx context.Context, operation string, data any, opts []RequestOption) (*preparedRequest, error) {
	b := c.bus
	var cfg requestConfig
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	// RESERVED_DOMAIN → INVALID_RPC_CALL (operación, timeout, data objeto, y las opciones propias)
	p, err := ParseRPCOperation(operation)
	if err != nil {
		return nil, err
	}
	timeout := RPCTimeoutDefault
	if cfg.hasTimeout {
		if timeout, err = ResolveRPCTimeout(cfg.timeout); err != nil {
			return nil, err
		}
	}
	canon, err := dataBytes(data)
	if err != nil {
		return nil, invalidRPCCall(err.Error(), nil)
	}
	if err := requireObject(canon, "`data`"); err != nil {
		return nil, err
	}
	if cfg.retries < 0 {
		return nil, invalidRPCCall("los reintentos deben ser un entero ≥ 0 (0 por defecto: `Request` hace UN intento).", nil)
	}
	// INVALID_DATACLASSIFICATION
	classification := cfg.classification
	if classification == "" {
		classification = b.opts.Classification
	}
	if classification == "" {
		classification = ClassificationInternal
	}
	if _, err := rpcClassification(classification); err != nil {
		return nil, err
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("flux: no se pudo generar el UUIDv7 de la petición: %w", err)
	}
	in := RPCRequestInput{
		Operation: p.Operation, AggregateID: cfg.aggregateID, ID: id.String(), Source: b.source,
		Time: FormatTime(time.Now()), CorrelationID: id.String(), ProducerVersion: b.opts.Version,
		Classification: classification, Data: json.RawMessage(canon),
	}
	// El contexto heredado gana sobre el default de conexión (09-multitenancy.md §5): una llamada hecha dentro de un handler
	// continúa la cadena. Un estado exige un tenant explícito; la petición, no: como los eventos, cae en `system`.
	inherited, hasCtx := EventContextFrom(ctx)
	switch {
	case cfg.tenantID != "":
		in.TenantID = cfg.tenantID
	case hasCtx && inherited.TenantID != "":
		in.TenantID = inherited.TenantID
	case b.opts.TenantID != "":
		in.TenantID = b.opts.TenantID
	default:
		in.TenantID = "system"
	}
	if hasCtx {
		if inherited.CorrelationID != "" {
			in.CorrelationID = inherited.CorrelationID
		}
		in.CausationID, in.TraceParent, in.TraceState = inherited.CausationID, inherited.TraceParent, inherited.TraceState
	}
	if in.TraceParent == "" && b.opts.Traceparent != nil {
		in.TraceParent = b.opts.Traceparent(ctx)
	}
	if in.DataSchema, err = c.schemaFor(p.Operation, "request"); err != nil {
		return nil, err
	}
	replyURI, err := c.schemaFor(p.Operation, "reply")
	if err != nil {
		return nil, err
	}
	event, err := BuildRPCRequest(in)
	if err != nil {
		return nil, err
	}

	// L3: la petición contra su esquema, ANTES de abrir nada: reporta TODAS las rutas y ningún valor.
	if b.validate != nil {
		if err := b.validate.checkRedacted(event, p.Operation); err != nil {
			if m := c.metrics(); m != nil {
				m.RPCClientRequest(p.Operation, "invalid_schema")
			}
			return nil, c.asRPCSchemaError(err, "petición", p.Operation)
		}
	}

	// El sobre se construye UNA vez: `id`, `time` y bytes son los mismos en todos los intentos.
	bytes, err := c.encode(event, "petición", p.Operation)
	if err != nil {
		var tl *RPCMessageTooLargeError
		if errors.As(err, &tl) {
			if m := c.metrics(); m != nil {
				m.RPCClientRequest(p.Operation, "permanent")
			}
		}
		return nil, err
	}
	return &preparedRequest{operation: p.Operation, subject: "rpc." + p.Operation, bytes: bytes, event: event,
		replyURI: replyURI, timeout: timeout, retries: cfg.retries}, nil
}

// attempt hace UN intento: publica por core NATS con el inbox privado y espera, lo primero que ocurra: `no responders` →
// RPC_UNAVAILABLE; un `-ERR Permissions Violation` → PERMISSION_DENIED; una respuesta → se comprueba; el timeout → RPC_TIMEOUT.
func (c *rpcClient) attempt(ctx context.Context, p *preparedRequest) (json.RawMessage, error) {
	b := c.bus
	// El `-ERR` se compite con la respuesta: con una librería que no entrega la denegación en la propia petición quien llama solo
	// vería un timeout, y un permiso que falta se vería como RPC_TIMEOUT (RETRYABLE) y el handler que llama reintentaría 51 minutos
	// algo que nunca va a funcionar (§7.1). También la suscripción de su inbox: la petición ya salió y llegaría al dueño (A-107).
	inboxPrefix := "_INBOX." + b.opts.Service + "."
	gctx, done := b.guardMatch(ctx, func(d denial) bool {
		return (d.op == "publish" && d.subject == p.subject) || (d.op == "subscribe" && strings.HasPrefix(d.subject, inboxPrefix))
	})
	defer done()
	rctx, cancel := context.WithTimeout(gctx, p.timeout)
	defer cancel()

	msg, err := b.nc.RequestWithContext(rctx, p.subject, p.bytes)
	if err != nil {
		return nil, c.translate(ctx, gctx, rctx, err, p)
	}

	// 3. ¿Es la respuesta a ESTA petición? (§4.3) Si no, RPC_INVALID_REPLY.
	event, esError, err := c.checkReply(p, msg.Data)
	if err != nil {
		return nil, err
	}

	// Con la firma en `require`: verificar. Es un fallo PERMANENT: reintentar da lo mismo.
	if b.verifier != nil {
		if err := b.verifier.Check(event); err != nil {
			code := "INVALID_SIGNATURE"
			var pe *PoisonError
			if errors.As(err, &pe) && pe.Code != "" {
				code = pe.Code
			}
			return nil, newRPCRemoteError(p.operation, "permanent", code, fmt.Sprintf(
				"la respuesta a %q no pasó la verificación de su firma (%s): %v. Con la firma en `require`, quien llama verifica la "+
					"respuesta y los errores (11-request-reply.md §7.4).", p.operation, code, err), 0)
		}
	}

	// 4. Un error: RETRYABLE o PERMANENT según su `class`, con su `code`.
	if esError {
		d := parseRPCErrorData(event.Data)
		var retry time.Duration
		if d.RetryAfterMs != nil {
			retry = time.Duration(*d.RetryAfterMs) * time.Millisecond
		}
		return nil, newRPCRemoteError(p.operation, d.Class, d.Code, d.Message, retry)
	}

	// 5. Una respuesta: con L3, `data` contra el esquema de la respuesta; se devuelve el `data`.
	if b.validate != nil {
		ev := event
		ev.DataSchema = p.replyURI
		if err := b.validate.checkRedacted(ev, p.operation); err != nil {
			return nil, c.asRPCSchemaError(err, "respuesta", p.operation)
		}
	}
	return event.Data, nil
}

// translate traduce lo que devuelve la librería a un código estable.
func (c *rpcClient) translate(ctx, gctx, rctx context.Context, err error, p *preparedRequest) error {
	b := c.bus
	if pd := deniedCause(gctx); pd != nil {
		return newPermissionDenied(b.opts.Service, pd.Operation, pd.Subject, c.hintCaller(pd), nil)
	}
	switch {
	case errors.Is(err, nats.ErrNoResponders):
		return &RPCUnavailableError{baseError{CodeRPCUnavailable, ClassRetryable, err}, p.operation}
	case ctx.Err() != nil:
		// Quien llamó canceló o venció SU contexto antes que el timeout de la llamada: no es un RPC_TIMEOUT.
		return fmt.Errorf("flux: la petición a %q se interrumpió: %w", p.operation, ctx.Err())
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) || rctx.Err() != nil:
		return &RPCTimeoutError{baseError{CodeRPCTimeout, ClassRetryable, err}, p.operation, p.timeout}
	}
	return err
}

func (c *rpcClient) hintCaller(d *PermissionDeniedError) string {
	if d.Operation == "publish" {
		return fmt.Sprintf("Quien llama necesita permiso de PUBLICACIÓN sobre el subject EXACTO de cada operación concedida (`%s`; "+
			"nunca `rpc.>` ni `rpc.<dominio>.>`): es la clave `calls` del manifiesto de `gen-acl.mjs`. La petición se descartó y el "+
			"broker lo avisó aparte (11-request-reply.md §7.1).", d.Subject)
	}
	return fmt.Sprintf("Quien llama necesita permiso de SUSCRIPCIÓN sobre su inbox privado `_INBOX.%s.>`: por ahí llegan las respuestas, "+
		"y sin él ninguna puede llegar (11-request-reply.md §7.3, 06-security.md §3.3).", c.bus.opts.Service)
}

// asRPCSchemaError dice, en el mensaje, de quién es el fallo de esquema: quien llama recibe el mismo INVALID_SCHEMA en los dos
// casos, y debe saber a quién arreglar (§5.2). Cita rutas, nunca valores.
func (c *rpcClient) asRPCSchemaError(err error, lado, operation string) error {
	var sve *SchemaValidationError
	if !errors.As(err, &sve) {
		return err
	}
	quien := "El fallo es de quien responde (o los dos tienen bundles de esquemas distintos): el orden de despliegue es primero quien responde."
	if lado == "petición" {
		quien = "El fallo es de quien llama: arregla los datos de la petición, o actualiza su bundle de esquemas."
	}
	e := *sve
	e.Subject = operation
	return &rpcSchemaError{SchemaValidationError: e, lado: lado, quien: quien}
}

// rpcSchemaError es INVALID_SCHEMA de una petición o de una respuesta: el mensaje DICE cuál.
type rpcSchemaError struct {
	SchemaValidationError
	lado  string
	quien string
}

func (e *rpcSchemaError) Error() string {
	return fmt.Sprintf("la %s de %q no cumple %s: %s. %s", e.lado, e.Subject, e.DataSchema, strings.Join(e.Errors, ", "), e.quien)
}

func (e *rpcSchemaError) Unwrap() error { return &e.SchemaValidationError }

// clientOutcome es la etiqueta `outcome` de una llamada que falló ("" si no se cuenta).
func clientOutcome(err error) string {
	var (
		un *RPCUnavailableError
		to *RPCTimeoutError
		ir *RPCInvalidReplyError
		sv *SchemaValidationError
		sn *SchemaNotFoundError
		rr *RPCRemoteError
		pd *PermissionDeniedError
		tl *RPCMessageTooLargeError
	)
	switch {
	case errors.As(err, &un):
		return "unavailable"
	case errors.As(err, &to):
		return "timeout"
	case errors.As(err, &ir):
		return "invalid_reply"
	case errors.As(err, &sv), errors.As(err, &sn):
		return "invalid_schema"
	case errors.As(err, &rr):
		if rr.FluxClass() == ClassRetryable {
			return "retryable"
		}
		return "permanent"
	case errors.As(err, &pd), errors.As(err, &tl):
		return "permanent"
	}
	return "" // un fallo de transporte lo cuenta la conexión; INVALID_RPC_CALL no se cuenta: es de programación
}

// checkReply son las cinco comprobaciones de §4.3. Cualquiera que falle es RPC_INVALID_REPLY.
func (c *rpcClient) checkReply(p *preparedRequest, raw []byte) (Event, bool, error) {
	invalida := func(motivo string, cause error) error {
		return &RPCInvalidReplyError{baseError{CodeRPCInvalidReply, ClassPermanent, cause}, p.operation, motivo, pareceAcuseDeStream(raw)}
	}
	// 1. Es un CloudEvent válido (parsea, con las cuatro extensiones y los tipos exactos).
	event, err := ParseEvent(raw)
	if err != nil {
		code := "POISON"
		var pe *PoisonError
		if errors.As(err, &pe) && pe.Code != "" {
			code = pe.Code
		}
		return Event{}, false, invalida(fmt.Sprintf("no es un CloudEvent válido de flux (%s)", code), err)
	}
	// 2. Su `type` es el de la respuesta de esa operación, o el del error.
	esperado, _ := RPCReplyType(p.operation)
	if event.Type != esperado && event.Type != RPCErrorType {
		return Event{}, false, invalida(fmt.Sprintf("su `type` es %s y se esperaba %s o %s",
			citar(truncateCodePoints(event.Type, 100), 130), jsonString(esperado), jsonString(RPCErrorType)), nil)
	}
	// 3. Su `causationid` es el `id` de la petición.
	if event.CausationID != p.event.ID {
		visto := "no lo trae"
		if event.CausationID != "" {
			visto = citar(truncateCodePoints(event.CausationID, 80), 100)
		}
		return Event{}, false, invalida(fmt.Sprintf("su `causationid` no es el `id` de la petición (%s): %s", p.event.ID, visto), nil)
	}
	// 4. Su `correlationid` y su `tenantid` son los de la petición. EXCEPCIÓN ÚNICA (§4.3.4): el error de una petición que quien
	// responde no pudo leer (§5.2) no tiene nada que heredar y lleva `correlationid` propio y `tenantid: system`; a quien llama le
	// basta la comprobación 3 (su `causationid` es el `id` de la petición, que solo él conoce) para atribuirlo y ver el código del
	// sobre (`INVALID_RPC_CALL`, `MALFORMED_JSON`…) en vez de RPC_INVALID_REPLY.
	esError := event.Type == RPCErrorType
	if !isErrorOfUnreadableRequest(event, esError) {
		if event.CorrelationID != p.event.CorrelationID {
			return Event{}, false, invalida("su `correlationid` no es el de la petición: una respuesta hereda el de la petición", nil)
		}
		if event.TenantID != p.event.TenantID {
			return Event{}, false, invalida("su `tenantid` no es el de la petición: una respuesta hereda el de la petición, sin modificar", nil)
		}
	}
	// 5. Si es un error, su `data` cumple el esquema del error.
	if esError {
		if problema := rpcErrorDataProblemRaw(event.Data); problema != "" {
			return Event{}, false, invalida(fmt.Sprintf("el error no cumple su esquema (%s)", problema), nil)
		}
	}
	return event, esError, nil
}

// isErrorOfUnreadableRequest: ¿es el error que contesta quien responde a una petición que NO pudo leer (§5.2)? Sin sobre no hay
// `correlationid` ni `tenantid` que heredar: el error lleva `correlationid` igual a SU PROPIO `id` y `tenantid: system`. Es la única
// respuesta a la que quien llama no le exige heredar los dos (§4.3, comprobación 4): la forma es estrecha a propósito —un
// `tenantid: system` cualquiera, o un `correlationid` cualquiera, no basta—, y la comprobación 3 sigue en pie y es la que la atribuye.
func isErrorOfUnreadableRequest(event Event, esError bool) bool {
	return esError && event.TenantID == "system" && event.CorrelationID == event.ID
}

// pareceAcuseDeStream: el cuerpo, ¿parece el acuse de un stream de JetStream (`{"stream":…,"seq":…}`)?
func pareceAcuseDeStream(raw []byte) bool {
	var o struct {
		Stream *string  `json:"stream"`
		Seq    *float64 `json:"seq"`
	}
	return json.Unmarshal(raw, &o) == nil && o.Stream != nil && o.Seq != nil
}

// parseRPCErrorData lee el `data` de un error YA validado por rpcErrorDataProblemRaw.
func parseRPCErrorData(raw []byte) RPCErrorData {
	var o struct {
		Class        string `json:"class"`
		Code         string `json:"code"`
		Message      string `json:"message"`
		RetryAfterMs *int64 `json:"retryAfterMs"`
	}
	_ = json.Unmarshal(raw, &o)
	return RPCErrorData{Class: o.Class, Code: o.Code, Message: o.Message, RetryAfterMs: o.RetryAfterMs}
}

// rpcErrorDataProblemRaw valida el `data` de un error recibido contra su esquema.
func rpcErrorDataProblemRaw(raw []byte) string {
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil {
		return "`data` del error no es un objeto"
	}
	var desconocidas []string
	for k := range obj {
		switch k {
		case "class", "code", "message", "retryAfterMs":
		default:
			desconocidas = append(desconocidas, k)
		}
	}
	if len(desconocidas) > 0 {
		return "el error lleva atributos que su esquema no admite (" + strings.Join(desconocidas, ", ") + ")"
	}
	var d RPCErrorData
	var class, code, message string
	if json.Unmarshal(obj["class"], &class) != nil {
		return "`class` del error debe ser `retryable` o `permanent` (`poison` no viaja)"
	}
	if json.Unmarshal(obj["code"], &code) != nil {
		return "`code` del error debe ser un identificador estable en MAYÚSCULAS y guiones bajos (máx. 64)"
	}
	if json.Unmarshal(obj["message"], &message) != nil {
		return "`message` del error debe ser un texto de 1 a 1000 caracteres"
	}
	d.Class, d.Code, d.Message = class, code, message
	if rawRetry, ok := obj["retryAfterMs"]; ok {
		lit := strings.TrimSpace(string(rawRetry))
		n, okN := parseNonNegativeInt(lit)
		if !okN {
			return "`retryAfterMs` del error debe ser un entero ≥ 0"
		}
		d.RetryAfterMs = &n
	}
	return rpcErrorDataProblem(d)
}

func parseNonNegativeInt(lit string) (int64, bool) {
	if lit == "" || len(lit) > 18 {
		return 0, false
	}
	var n int64
	for _, c := range lit {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	return n, true
}

// ─── tamaño, esquema ─────────────────────────────────────────────────────────

// encode devuelve los bytes del mensaje, firmado si procede, y su tamaño COMPROBADO antes de publicar: el mínimo entre 1 MiB y el
// `max_payload` que anuncia el broker, LAS CABECERAS INCLUIDAS (no usamos ninguna). Si no cabe, RPC_MESSAGE_TOO_LARGE.
func (c *rpcClient) encode(event Event, que, operation string) ([]byte, error) {
	limite := MaxMessageBytes
	if mp := c.bus.nc; mp != nil {
		if max := int(mp.MaxPayload()); max > 0 && max < limite {
			limite = max
		}
	}
	tooLarge := func(n int) error {
		return &RPCMessageTooLargeError{baseError{CodeRPCMessageTooLarge, ClassPermanent, nil}, n, limite, operation, que}
	}
	sinFirma := appendEvent(nil, event)
	if len(sinFirma) > limite {
		return nil, tooLarge(len(sinFirma))
	}
	if c.bus.signer == nil {
		return sinFirma, nil
	}
	firmado, err := c.bus.signer.Sign(event)
	if err != nil {
		var ee *EnvelopeError
		if errors.As(err, &ee) {
			// El firmante serializa con el límite de 1 MiB de los eventos: un mensaje en el borde, con la firma encima, puede pasarse.
			return nil, tooLarge(len(sinFirma) + 200)
		}
		return nil, err
	}
	bytes := appendEvent(nil, firmado)
	if len(bytes) > limite {
		return nil, tooLarge(len(bytes))
	}
	return bytes, nil
}

// schemaFor es el `dataschema`: índice `rpc` del bundle (sin red) o SchemaBaseURL (`<major>.0.0`). Nunca por red.
func (c *rpcClient) schemaFor(operation, side string) (string, error) {
	o := c.bus.opts
	if v := RPCSchemaURIFor(o.Validation.Bundle, operation, side); v != "" {
		return v, nil
	}
	// Con L3 activa y un bundle, una operación que NO está en su índice se dice como lo que es —SCHEMA_NOT_FOUND, que lanza el
	// validador al no encontrar la URI derivada—, y no como «no hay dataschema»: el bundle ya es la fuente de los esquemas.
	if o.SchemaBaseURL == "" && o.Validation.Bundle != nil && o.Validation.Mode != "" && o.Validation.Mode != ValidationOff {
		return RPCDataSchema(operation, side, "")
	}
	if o.SchemaBaseURL == "" {
		return "", fmt.Errorf("flux: no hay dataschema para la operación %q (%s). Pasa un bundle con el índice `rpc` en "+
			"Validation.Bundle (ConnectOptions), o fija SchemaBaseURL para derivar el `<major>.0.0`: ni el nombre de la operación ni "+
			"nada de lo que hay en el sobre permiten adivinar el host de tus esquemas", operation, side)
	}
	return RPCDataSchema(operation, side, o.SchemaBaseURL)
}

// ─── responder ───────────────────────────────────────────────────────────────

// Respond atiende una operación: UNA SUSCRIPCIÓN AL SUBJECT EXACTO (`rpc.<operación>`, nunca `rpc.<dominio>.>`: con el comodín, una
// operación que el servicio no atiende recibe interés y quien llama espera su timeout en vez de `no responders`, A-64) con el
// NOMBRE DEL SERVICIO COMO GRUPO DE COLA: dos instancias se reparten las peticiones (A-58).
//
// Un dueño sin permiso de suscripción parece un servicio CAÍDO (`no responders`, A-73): Respond FALLA con PERMISSION_DENIED,
// nombrando el subject, en vez de arrancar mudo. Y cada petición corre en su goroutine, con un tope de handlers concurrentes
// (256 por defecto): el exceso espera en la cola de la suscripción.
func (b *Bus) Respond(ctx context.Context, operation string, handler RPCHandler, opts ...RespondOption) (*Responder, error) {
	return b.rpc.respond(ctx, operation, handler, opts)
}

func (c *rpcClient) respond(ctx context.Context, operation string, handler RPCHandler, opts []RespondOption) (*Responder, error) {
	b := c.bus
	p, err := ParseRPCOperation(operation)
	if err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, invalidRPCCall("el handler de `Respond` debe ser una función `func(ctx, req) (any, error)`.", nil)
	}
	cfg := respondConfig{maxConcurrent: RPCMaxConcurrentHandlers}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	if cfg.maxConcurrent < 1 {
		return nil, invalidRPCCall("`WithMaxConcurrent` debe ser un entero ≥ 1 (256 por defecto).", nil)
	}
	if cfg.classification != "" {
		if _, err := rpcClassification(cfg.classification); err != nil {
			return nil, err
		}
	}
	c.mu.Lock()
	_, dup := c.responders[p.Operation]
	c.mu.Unlock()
	if dup {
		return nil, invalidRPCCall(fmt.Sprintf(
			"ya hay un `Respond` de %q en este servicio. Dos suscripciones del mismo grupo de cola en una conexión se repartirían las "+
				"peticiones entre dos handlers: registra la operación una vez, o llama a `Stop` del responder anterior.", p.Operation), nil)
	}
	requestURI, err := c.schemaFor(p.Operation, "request")
	if err != nil {
		return nil, err
	}
	replyURI, err := c.schemaFor(p.Operation, "reply")
	if err != nil {
		return nil, err
	}
	// En modo estricto una operación sin esquema es un fallo de configuración: se dice al arrancar, no en cada petición.
	if b.opts.Validation.Mode == ValidationStrict && b.opts.Validation.Bundle != nil {
		for lado, uri := range map[string]string{"petición": requestURI, "respuesta": replyURI} {
			if _, ok := b.opts.Validation.Bundle.Schemas[uri]; !ok {
				return nil, &SchemaNotFoundError{Subject: p.Operation + " (" + lado + ")", DataSchema: uri}
			}
		}
	}

	subject := "rpc." + p.Operation
	r := &Responder{
		Operation: p.Operation, Subject: subject, c: c, handler: handler, classification: cfg.classification,
		requestURI: requestURI, replyURI: replyURI, sem: make(chan struct{}, cfg.maxConcurrent),
		stopped: make(chan struct{}), closed: make(chan struct{}),
	}
	sub, err := b.nc.QueueSubscribe(subject, b.opts.Service, r.onMessage)
	if err != nil {
		return nil, fmt.Errorf("flux: no se pudo suscribir a %q: %w", subject, err)
	}
	r.sub = sub
	sub.SetClosedHandler(func(string) { close(r.closed) })

	// Una suscripción denegada NO devuelve error: `QueueSubscribe` devuelve nil y el `-ERR` llega por la conexión. El `Flush` es un
	// PING/PONG y el protocolo es ordenado: el `-ERR` llega antes que el PONG, y `nc.err` ya lo tiene cuando vuelve (denials.go).
	if err := flushWithin(ctx, b.nc, 10*time.Second); err != nil {
		_ = sub.Unsubscribe()
		return nil, fmt.Errorf("flux: no se pudo confirmar la suscripción a %q: %w", subject, err)
	}
	if _, denied := lastDenial(b.nc, "subscribe", subject); denied {
		_ = sub.Unsubscribe()
		return nil, newPermissionDenied(b.opts.Service, "subscribe", subject,
			"El dueño de un dominio necesita permiso de SUSCRIPCIÓN sobre `rpc.<dominio>.>` (es la clave `owns` del manifiesto de "+
				"`gen-acl.mjs`; el SDK se suscribe al subject exacto de cada operación, pero lo que se CONCEDE no es lo que se USA). "+
				"Sin él la operación parecería un servicio caído —quien llama vería `RPC_UNAVAILABLE`— y por eso `Respond` falla al "+
				"arrancar y no arranca mudo (11-request-reply.md §7.1, A-73).", nil)
	}
	c.mu.Lock()
	c.responders[p.Operation] = r
	c.mu.Unlock()
	return r, nil
}

// onMessage es el callback de la suscripción: nats.go lo invoca en SERIE, uno por mensaje, desde un solo goroutine. Aquí se toma
// un cupo —espera si hay `maxConcurrent` handlers en vuelo: el exceso queda en la cola de la propia suscripción— y cada petición
// corre en su goroutine.
func (r *Responder) onMessage(m *nats.Msg) {
	r.sem <- struct{}{}
	r.inflight.Add(1)
	go func() {
		defer func() {
			<-r.sem
			r.inflight.Done()
		}()
		r.c.serve(r, m)
	}()
}

// Stop es la parada ordenada (11-request-reply.md §5.5): DEJA DE ESCUCHAR (`Drain` de ESTA suscripción: UNSUB y espera a lo que ya
// viaja: las peticiones nuevas dan `no responders` al instante), ESPERA A LOS HANDLERS EN VUELO —con un tope de 25 s o el del ctx— y
// retorna. Cerrar la conexión de golpe pierde lo que se atiende (A-83).
func (r *Responder) Stop(ctx context.Context) error {
	r.stopOnce.Do(func() { go r.stop(ctx) })
	select {
	case <-r.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Responder) stop(ctx context.Context) {
	defer close(r.stopped)
	c := r.c
	c.mu.Lock()
	if c.responders[r.Operation] == r {
		delete(c.responders, r.Operation)
	}
	c.mu.Unlock()

	limite := time.NewTimer(RPCStopTimeout)
	defer limite.Stop()
	// 1. Dejar de escuchar con el DRENAJE de la suscripción: UNSUB, y un PING/PONG que espera a lo que ya viaja. Las peticiones
	// nuevas dan `no responders` al instante; una baja que descartase lo que aún llega la perdería (A-83).
	if err := r.sub.Drain(); err == nil {
		select {
		case <-r.closed:
		case <-limite.C:
		case <-ctx.Done():
		}
	}
	// 2. Esperar a los handlers en vuelo, con un tope: una petición más lenta ya la dio por perdida quien llamó. Un margen breve
	// cubre la propagación asíncrona del interés en un cluster (A-70).
	hechos := make(chan struct{})
	go func() { r.inflight.Wait(); close(hechos) }()
	select {
	case <-hechos:
		select {
		case <-time.After(RPCStopMargin):
		case <-ctx.Done():
		}
	case <-limite.C:
		c.bus.logf(slog.LevelWarn, "%q: siguen handlers en vuelo tras %s; se deja de esperar. Una petición más lenta que el timeout máximo (25 s) ya la dio por perdida quien llamó.", r.Operation, RPCStopTimeout)
	case <-ctx.Done():
	}
}

// stopAll para a todos los responders: lo que hace Bus.Close() ANTES de cerrar la conexión.
func (c *rpcClient) stopAll(ctx context.Context) {
	c.mu.Lock()
	rs := make([]*Responder, 0, len(c.responders))
	for _, r := range c.responders {
		rs = append(rs, r)
	}
	c.mu.Unlock()
	var wg sync.WaitGroup
	for _, r := range rs {
		wg.Add(1)
		go func(r *Responder) { defer wg.Done(); _ = r.Stop(ctx) }(r)
	}
	wg.Wait()
}

// ─── atender una petición ────────────────────────────────────────────────────

// serve atiende un mensaje de la suscripción. NUNCA entra en pánico: lo que falle aquí va al log, y el bucle sigue.
func (c *rpcClient) serve(r *Responder, m *nats.Msg) {
	b := c.bus
	inicio := time.Now()
	var outcome string
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				b.logf(slog.LevelError, "fallo inesperado atendiendo %q: %v. Quien llamó verá un timeout.", r.Operation, rec)
			}
		}()
		outcome = c.handle(r, m)
	}()
	if mt := c.metrics(); mt != nil && outcome != "" {
		mt.RPCServerRequest(r.Operation, outcome)
		mt.RPCDuration(r.Operation, "server", time.Since(inicio).Seconds())
	}
}

// handle son los pasos de §5.1. Devuelve el `outcome` de la petición.
func (c *rpcClient) handle(r *Responder, m *nats.Msg) string {
	b := c.bus
	op := r.Operation
	reply := m.Reply

	// 2. Sin subject de respuesta no se atiende: nadie espera el resultado de una orden (`nats pub` a mano).
	if reply == "" {
		c.poison(op, "NO_REPLY_SUBJECT", fmt.Sprintf(
			"llegó una petición a %q SIN subject de respuesta (¿un `nats pub` a mano?): no se atiende, porque nadie espera el resultado de "+
				"una orden y ejecutarla a ciegas la aplicaría sin que nadie lo sepa (11-request-reply.md §5.1).", op))
		return "poison"
	}

	// 3. Interpretar el sobre. Falla → un error `permanent` con el código del sobre. Sin sobre no hay nada que heredar.
	event, err := ParseEvent(m.Data)
	if err != nil {
		code := "MALFORMED_JSON"
		var pe *PoisonError
		if errors.As(err, &pe) && pe.Code != "" {
			code = pe.Code
		}
		c.poison(op, code, fmt.Sprintf("una petición a %q no es un CloudEvent válido (%s): se contesta con un error permanent.", op, code))
		c.sendError(r, reply, nil, idLegible(m.Data), RPCErrorData{Class: "permanent", Code: code, Message: "la petición no es un CloudEvent " +
			"válido de flux (" + code + "): se espera JSON con specversion, id, source, type, time, datacontenttype, dataschema y data, más " +
			"las cuatro extensiones obligatorias (correlationid, tenantid, producerversion, dataclassification) con tipos exactos (01-envelope.md §3.1)"})
		return "poison"
	}

	// 4. `type` = el de la petición de ESTA operación.
	tipo, _ := RPCRequestType(op)
	if event.Type != tipo {
		c.poison(op, "INVALID_RPC_CALL", fmt.Sprintf("una petición a %q trae `type` %s y se esperaba %s: se contesta con INVALID_RPC_CALL.",
			op, citar(truncateCodePoints(event.Type, 100), 130), jsonString(tipo)))
		c.sendError(r, reply, &event, "", RPCErrorData{Class: "permanent", Code: CodeInvalidRPCCall, Message: fmt.Sprintf(
			"la petición llegó al subject de %q con `type` %s, y el de la petición de esta operación es %s. Quien llama debe construirla con el SDK (11-request-reply.md §5.2)",
			op, citar(truncateCodePoints(event.Type, 100), 130), jsonString(tipo))})
		return "poison"
	}

	// 5. Con la firma en `require`: verificar, y la ventana de 300 s.
	if b.verifier != nil {
		if err := b.verifier.Check(event); err != nil {
			code := "INVALID_SIGNATURE"
			var pe *PoisonError
			if errors.As(err, &pe) && pe.Code != "" {
				code = pe.Code
			}
			b.logf(slog.LevelError, "la petición a %q (%s) no pasó la verificación de su firma (%s).", op, event.ID, code)
			c.sendError(r, reply, &event, "", RPCErrorData{Class: "permanent", Code: code, Message: fmt.Sprintf(
				"la petición a %q no pasó la verificación de su firma (%s): con la firma en `require`, quien responde exige una petición firmada "+
					"con una clave que conozca (07-signing.md §7)", op, code)})
			return "invalid_signature"
		}
	}
	if b.opts.Signing.Verify == VerifyRequire {
		ahora := time.Now()
		t, err := time.Parse(time.RFC3339, event.Time)
		if err != nil || ahora.Sub(t).Abs() > RPCStaleWindowSeconds*time.Second {
			e := &StaleRequestError{baseError{CodeStaleRequest, ClassPermanent, nil}, event.Time, RPCStaleWindowSeconds, FormatTime(ahora)}
			b.logf(slog.LevelError, "STALE_REQUEST en %q (%s): `time` fuera de la ventana de %d s.", op, event.ID, RPCStaleWindowSeconds)
			c.sendError(r, reply, &event, "", RPCErrorData{Class: "permanent", Code: CodeStaleRequest, Message: e.Error()})
			return "stale_request"
		}
	}

	// 6. Con L3: `data` contra el esquema de la petición. Falla → error `permanent` INVALID_SCHEMA con TODAS las rutas.
	if b.validate != nil {
		ev := event
		ev.DataSchema = r.requestURI
		if err := b.validate.checkRedacted(ev, op); err != nil {
			var sve *SchemaValidationError
			var snf *SchemaNotFoundError
			msg := ""
			code := CodeSchemaInvalid
			switch {
			case errors.As(err, &sve):
				msg = fmt.Sprintf("la petición no cumple %s: %s", sve.DataSchema, strings.Join(sve.Errors, ", "))
			case errors.As(err, &snf):
				code = CodeSchemaNotFound
				msg = fmt.Sprintf("no hay esquema de la petición de %q (%s) en el bundle de quien responde", op, r.requestURI)
			default:
				msg = "no se pudo validar la petición contra su esquema"
			}
			c.sendError(r, reply, &event, "", RPCErrorData{Class: "permanent", Code: code, Message: msg})
			return "invalid_schema"
		}
	}

	// 7. Invocar el handler con (ctx, petición). Una llamada hecha desde él continúa la cadena.
	req := RPCRequest{Operation: op, Event: event, ID: event.ID, CorrelationID: event.CorrelationID, TenantID: event.TenantID,
		AggregateID: event.AggregateID, Time: event.Time, Data: event.Data}
	hctx := WithEventContext(context.Background(), ContextFromEvent(event))
	result, herr := c.invokeHandler(hctx, r, req)
	if herr != nil {
		e := c.errorOf(op, event, herr)
		c.sendError(r, reply, &event, "", e)
		return e.Class
	}

	// 8. Su resultado es un objeto. Con L3: contra el esquema de la respuesta.
	data, derr := dataBytes(result)
	if derr != nil || len(data) == 0 || data[0] != '{' {
		c.sendError(r, reply, &event, "", RPCErrorData{Class: "permanent", Code: CodeSchemaInvalid, Message: fmt.Sprintf(
			"la respuesta no es un objeto: el handler de %q devolvió algo que no lo es. El fallo es de quien responde: la respuesta es un "+
				"objeto JSON en la raíz (01-envelope.md §4)", op)})
		return "invalid_schema"
	}
	if b.validate != nil {
		cand := event
		cand.DataSchema, cand.Data = r.replyURI, data
		if err := b.validate.checkRedacted(cand, op); err != nil {
			var sve *SchemaValidationError
			var snf *SchemaNotFoundError
			code, msg := CodeSchemaInvalid, "no se pudo validar la respuesta contra su esquema"
			switch {
			case errors.As(err, &sve):
				msg = fmt.Sprintf("la respuesta no cumple %s: %s. El fallo es de quien responde, no de quien llama", sve.DataSchema, strings.Join(sve.Errors, ", "))
			case errors.As(err, &snf):
				code = CodeSchemaNotFound
				msg = fmt.Sprintf("no hay esquema de la respuesta de %q (%s) en el bundle de quien responde", op, r.replyURI)
			}
			c.sendError(r, reply, &event, "", RPCErrorData{Class: "permanent", Code: code, Message: msg})
			return "invalid_schema"
		}
	}

	// 9. Construir la respuesta (§3.2), firmarla si procede, comprobar el tamaño y publicarla al `reply` de la petición.
	classification := r.classification
	if classification == "" {
		classification = b.opts.Classification
	}
	if classification == "" {
		classification = ClassificationInternal
	}
	id, _ := uuid.NewV7()
	respuesta, err := BuildRPCReply(NewRPCReplyInput(refOf(op, event), id.String(), b.source, FormatTime(time.Now()), b.opts.Version,
		c.replyTraceparent(hctx, event), classification, json.RawMessage(data), r.replyURI))
	if err != nil {
		b.logf(slog.LevelError, "no se pudo construir la respuesta de %q: %v. Quien llamó verá un timeout.", op, err)
		return ""
	}
	bytes, err := c.encode(respuesta, "respuesta", op)
	if err != nil {
		var tl *RPCMessageTooLargeError
		if !errors.As(err, &tl) {
			b.logf(slog.LevelError, "no se pudo codificar la respuesta de %q: %v. Quien llamó verá un timeout.", op, err)
			return ""
		}
		// Si la respuesta no cabe, la librería lanza EN QUIEN RESPONDE y quien llama solo ve un timeout (A-65): se contesta con un error.
		b.logf(slog.LevelError, "la respuesta de %q a la petición %s no cabe: %v", op, event.ID, tl)
		c.sendError(r, reply, &event, "", RPCErrorData{Class: "permanent", Code: CodeRPCMessageTooLarge, Message: truncateCodePoints(tl.Error(), rpcErrorMessageMax)})
		return "permanent"
	}
	c.publish(reply, bytes, op)
	return "ok"
}

// replyTraceparent es el `traceparent` de la respuesta: el del que responde, de la MISMA traza; sin span activo, el de la petición.
func (c *rpcClient) replyTraceparent(ctx context.Context, request Event) string {
	if f := c.bus.opts.Traceparent; f != nil {
		if tp := f(ctx); tp != "" {
			return tp
		}
	}
	return request.TraceParent
}

func (c *rpcClient) invokeHandler(ctx context.Context, r *Responder, req RPCRequest) (res any, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = NewPermanentError(fmt.Sprintf("pánico en el handler de %s: %v", r.Operation, rec), WithCode("HANDLER_PANIC"))
		}
	}()
	return r.handler(ctx, req)
}

// refOf es lo que la respuesta hereda de un sobre de petición ya leído.
func refOf(operation string, e Event) RPCRequestRef {
	return RPCRequestRef{Operation: operation, ID: e.ID, CorrelationID: e.CorrelationID, TenantID: e.TenantID,
		Subject: e.AggregateID, PartitionKey: e.PartitionKey}
}

// soloBlancos: un texto que es solo espacio en blanco (`White_Space` de Unicode) no dice nada.
func soloBlancos(s string) bool { return strings.TrimFunc(s, unicode.IsSpace) == "" }

// errorOf convierte un error del handler en el `data` de `com.flux.rpc.error.v1` (§5.3):
//   - NewRetryableError / NewPermanentError (y PoisonError) → su clase, SU código y SU mensaje (la aplicación los eligió);
//   - uno que el clasificador RECONOCE (un 503, `ECONNRESET`) → su clase y el código del clasificador, con un mensaje GENÉRICO;
//   - uno DESCONOCIDO → `permanent`, `UNKNOWN`, «error interno del servicio». Su texto va al log, con el `id` de la petición.
//
// Lo desconocido es `permanent` y no «retryable acotado»: el presupuesto de 04-errors.md §2.1 se cuenta por entregas de un
// consumidor, y una llamada no tiene número de entrega. `poison` no viaja.
func (c *rpcClient) errorOf(op string, event Event, err error) RPCErrorData {
	b := c.bus
	codigoValido := func(s string) bool { return len(s) <= rpcErrorCodeMax && rpcErrorCodePattern.MatchString(s) }
	mensaje := func(m, porDefecto string) string {
		if soloBlancos(m) {
			m = porDefecto
		}
		return truncateCodePoints(m, rpcErrorMessageMax)
	}
	reservado := event.DataClassification == ClassificationConfidential || event.DataClassification == ClassificationRestricted

	var re *RetryableError
	var pe *PermanentError
	var po *PoisonError
	switch {
	case errors.As(err, &re), errors.As(err, &pe), errors.As(err, &po):
		clase, porDefecto, dado, msg := "permanent", "PERMANENT", "", ""
		var retryAfter time.Duration
		switch {
		case re != nil:
			clase, porDefecto, dado, msg, retryAfter = "retryable", "RETRYABLE", re.Code, re.Error(), re.RetryAfter
		case pe != nil:
			dado, msg = pe.Code, pe.Error()
		default:
			// Un PoisonError es `permanent` (`poison` no viaja): su código por defecto es el de la clase con la que viaja.
			dado, msg = po.Code, po.Error()
		}
		code := porDefecto
		if dado != "" {
			if codigoValido(dado) {
				code = dado
			} else {
				b.logf(slog.LevelWarn, "el handler de %q lanzó un error con el código %s, que no cumple ^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*$ "+
					"(04-errors.md §7): se responde con %s. Petición %s.", op, citar(truncateCodePoints(dado, 70), 90), porDefecto, event.ID)
			}
		}
		out := RPCErrorData{Class: clase, Code: code, Message: mensaje(msg, "el servicio rechazó la petición")}
		if clase == "retryable" && retryAfter > 0 {
			ms := retryAfter.Milliseconds()
			out.RetryAfterMs = &ms
		}
		return out
	}

	cl := b.classify(err)
	if cl.Code != "UNKNOWN" {
		// Reconocido: un 503, `ECONNRESET`, un timeout o un error tipado de otra capacidad.
		clase := "permanent"
		if cl.Class == ClassRetryable {
			clase = "retryable"
		}
		msg := "una dependencia rechazó la operación"
		code := "PERMANENT"
		if clase == "retryable" {
			msg, code = "dependencia no disponible", "RETRYABLE"
		}
		if codigoValido(cl.Code) {
			code = cl.Code
		}
		b.logf(slog.LevelError, "el handler de %q falló con %s (%s) en la petición %s%s", op, cl.Code, clase, event.ID,
			map[bool]string{true: "", false: ": " + err.Error()}[reservado])
		out := RPCErrorData{Class: clase, Code: code, Message: msg}
		if clase == "retryable" && cl.RetryAfter > 0 {
			ms := cl.RetryAfter.Milliseconds()
			out.RetryAfterMs = &ms
		}
		return out
	}

	// DESCONOCIDO. La política configurable de 04-errors.md §2.1 se aplica así: `permanent` y `retryable-bounded` dan `permanent`;
	// `retryable` da `retryable`. El texto de la excepción puede llevar valores de `data`, rutas del servidor o datos personales.
	clase := "permanent"
	if cl.Class == ClassRetryable && cl.MaxAttempts == 0 {
		clase = "retryable"
	}
	detalle := fmt.Sprintf("%T: %v", err, err)
	omitido := ""
	if reservado {
		detalle = fmt.Sprintf("%T", err)
		omitido = " (el mensaje se omite: la petición es confidential o restricted)"
	}
	b.logf(slog.LevelError, "el handler de %q lanzó un error sin clasificar en la petición %s: %s%s. Quien llamó recibió `%s` UNKNOWN con un mensaje genérico.",
		op, event.ID, detalle, omitido, clase)
	return RPCErrorData{Class: clase, Code: "UNKNOWN", Message: "error interno del servicio"}
}

// sendError contesta con un error `com.flux.rpc.error.v1`, SIEMPRE: callar dejaría a quien llama esperando su timeout para un fallo
// que no se arregla esperando (§5.2). `request` es el sobre de la petición, o nil si no se pudo leer: entonces no hay `correlationid`
// ni `tenantid` que heredar, y el error lleva `correlationid` = su propio `id`, `tenantid` = `system` y `causationid` = el `id` de la
// petición solo si es una cadena válida (§5.2, decidido provisionalmente).
func (c *rpcClient) sendError(r *Responder, replySubject string, request *Event, idDeLaPeticion string, e RPCErrorData) {
	b := c.bus
	op := r.Operation
	id, _ := uuid.NewV7()
	e.Message = mensajeSeguro(e.Message)
	var ref RPCRequestRef
	if request != nil {
		ref = refOf(op, *request)
	} else {
		// Sin sobre que leer no hay nada que heredar (§5.2).
		ref = RPCRequestRef{Operation: op, ID: idDeLaPeticion, CorrelationID: id.String(), TenantID: "system"}
		if ref.ID == "" {
			ref.ID = id.String()
		}
	}
	tp := ""
	if request != nil {
		tp = c.replyTraceparent(context.Background(), *request)
	}
	ev, err := BuildRPCError(NewRPCErrorInput(ref, id.String(), b.source, FormatTime(time.Now()), b.opts.Version, tp, e))
	if err != nil {
		b.logf(slog.LevelError, "no se pudo construir el error de %q: %v", op, err)
		return
	}
	if request == nil && idDeLaPeticion == "" {
		ev.CausationID = ""
	}
	bytes, err := c.encode(ev, "respuesta", op)
	if err != nil {
		b.logf(slog.LevelError, "no se pudo contestar con un error a %q: %v", op, err)
		return
	}
	c.publish(replySubject, bytes, op)
}

func mensajeSeguro(m string) string {
	if soloBlancos(m) {
		m = "el servicio rechazó la petición"
	}
	return truncateCodePoints(m, rpcErrorMessageMax)
}

func (c *rpcClient) publish(replySubject string, bytes []byte, op string) {
	if err := c.bus.nc.Publish(replySubject, bytes); err != nil {
		// La conexión se cerró (o se está drenando) con la petición en la mano: quien llamó verá un timeout.
		c.bus.logf(slog.LevelError, "no se pudo publicar la respuesta de %q: %v. Quien llamó verá un timeout.", op, err)
	}
}

func (c *rpcClient) poison(op, code, log string) {
	b := c.bus
	b.logf(slog.LevelError, "POISON en %q: %s", op, log)
	// `Raw` va vacío a propósito: una petición PUEDE llevar datos personales (11-request-reply.md §3.6).
	if b.opts.OnPoison != nil {
		b.opts.OnPoison(PoisonInfo{Subject: "rpc." + op, Err: NewPoisonError(log, WithCode(code))})
	}
}

// idLegible es el `id` de un mensaje que no se pudo leer como CloudEvent, SOLO si es una cadena válida (§5.2): no vacía, de 128
// caracteres como máximo (puntos de código) y sin caracteres de control. Es lo que el error cita como `causationid`: un valor que
// quien llama no puede reconocer como suyo, o que llena un log, no se copia.
func idLegible(raw []byte) string {
	var o struct {
		ID json.RawMessage `json:"id"`
	}
	if json.Unmarshal(raw, &o) != nil {
		return ""
	}
	var id string
	if json.Unmarshal(o.ID, &id) != nil || id == "" || codePointLength(id) > 128 {
		return ""
	}
	for _, r := range id {
		if unicode.IsControl(r) {
			return ""
		}
	}
	return id
}
