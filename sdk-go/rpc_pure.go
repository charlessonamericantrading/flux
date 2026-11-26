// Petición/respuesta — las funciones PURAS y deterministas de la extensión opcional `rpc`.
// Contrato normativo: specification/11-request-reply.md, specification/04-errors.md §7
//
// Un EVENTO dice qué pasó; el ESTADO es el dato; la petición/respuesta es LA ORDEN QUE NECESITA RESPUESTA YA y el
// CÁLCULO. Core NATS, sin stream ni DLQ: si nadie escucha, quien llama lo sabe en milisegundos.
//
// Este fichero es la mitad que NO habla con la red: el subject, los tipos y el sobre de la petición, de la respuesta y
// del error. Producen los MISMOS BYTES en todos los SDKs (11-request-reply.md §3.5; arnés cruzado `rpc-request`,
// `rpc-reply`, `rpc-error`, `rpc-operation` y `rpc-timeout`). Lo que las alimenta —`id`, `time`— se pasa DE FUERA: si la
// función los inventara, dos ejecuciones no serían comparables. La otra mitad, `Request` y `Respond`, está en rpc.go.

package flux

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ─── Constantes — protocol.json → `rpc` ──────────────────────────────────────

const (
	// RPCTimeoutDefault es el timeout por defecto de una llamada: 5 s.
	RPCTimeoutDefault = 5 * time.Second
	// RPCTimeoutMin es el mínimo: 1 ms.
	RPCTimeoutMin = time.Millisecond
	// RPCTimeoutMax es el máximo: 25 s, POR DEBAJO del `ack_wait` de 30 s (03-delivery.md §2): para que una llamada hecha
	// desde un handler de eventos no provoque su reentrega mientras espera.
	RPCTimeoutMax = 25 * time.Second
	// RPCStaleWindowSeconds es la ventana simétrica de STALE_REQUEST (solo con la firma en `require`).
	RPCStaleWindowSeconds = 300
	// RPCMaxConcurrentHandlers son los handlers concurrentes por operación: el mismo orden que `max_ack_pending`
	// (11-request-reply.md §5.6).
	RPCMaxConcurrentHandlers = 256
	// RPCStopTimeout es cuánto espera Stop a los handlers en vuelo: el timeout máximo (§5.5). Una petición más lenta ya la
	// dio por perdida quien llamó.
	RPCStopTimeout = RPCTimeoutMax
	// RPCStopMargin es el margen tras dejar de escuchar y antes de cerrar (§5.5): en un cluster el interés se propaga de
	// forma asíncrona, y la petición que sale justo cuando se va el último puede agotar su timeout en vez de recibir `no
	// responders` (A-70: 4 de 10 sin esperar, 0 de 10 a los 5 ms).
	RPCStopMargin = 25 * time.Millisecond

	// RPCErrorType es el `type` del error: el mismo para toda operación.
	RPCErrorType = "com.flux.rpc.error.v1"
	// RPCErrorDataSchema es su `dataschema`: fijo. Su esquema está en `schemas/system/rpc/error/1.0.0.json`.
	RPCErrorDataSchema = "https://schemas.internal/system/rpc/error/1.0.0.json"
	// RPCSchemaBase es la base de los esquemas del repo: `schemas/rpc/<dom>/<agr>/<op>/{request,reply}/…` cuelga de aquí.
	RPCSchemaBase = "https://schemas.internal"

	rpcErrorCodeMax    = 64
	rpcErrorMessageMax = 1000
)

var (
	// `rpc.operation.pattern`: el nombre de la operación —el subject sin `rpc.`— tiene la MISMA forma sintáctica que el
	// subject de un evento. Coincidencia COMPLETA: en RE2 `$` solo casa al final del texto, no antes de un `\n` final.
	rpcOperationPattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]{0,3}\.[a-z0-9]+(-[a-z0-9]+)*$`)
	// `errors.codePattern` y el `pattern` del esquema del error.
	rpcErrorCodePattern = regexp.MustCompile(`^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*$`)
)

// ─── Validación — se hace SIEMPRE, antes de abrir ninguna petición ───────────────────────────────────────────────

// RPCOperation son los cuatro tokens de una operación ya validada.
type RPCOperation struct {
	// Operation es `clientes.cliente.v1.calcular-descuento`: lo que reciben Request y Respond.
	Operation string
	Domain    string
	Aggregate string
	Major     int
	// Name es `calcular-descuento`: el último token, en infinitivo.
	Name string
}

// ParseRPCOperation valida el nombre de una operación. Primero el dominio reservado (RESERVED_DOMAIN: también `rpc.` puesto
// por quien llama), después la forma (INVALID_RPC_CALL). Siempre antes de tocar la red.
func ParseRPCOperation(operation string) (RPCOperation, error) {
	if err := assertNotReservedDomain(operation, "operación"); err != nil {
		return RPCOperation{}, err
	}
	if !rpcOperationPattern.MatchString(operation) {
		tokens := strings.Split(operation, ".")
		porQue := "la versión mayor es `v` y un entero de 1 a 4 dígitos sin ceros a la izquierda (`v1`…`v9999`), y cada token va en " +
			"kebab-case (letras minúsculas, dígitos y guiones)"
		switch {
		case operation != strings.ToLower(operation):
			porQue = "debe ir todo en minúsculas —NATS distingue mayúsculas, y una errata crea un subject al que nadie está suscrito, sin error—"
		case strings.ContainsAny(operation, "*>"):
			porQue = "no admite comodines (`*`, `>`): el broker los aceptaría, y es una operación concreta la que se llama o se atiende"
		case len(tokens) != 4:
			porQue = fmt.Sprintf("debe tener exactamente 4 tokens, tiene %d", len(tokens))
		}
		return RPCOperation{}, invalidRPCCall(fmt.Sprintf(
			"la operación %s no es válida: %s. El nombre es `<dominio>.<agregado>.v<major>.<operacion>` —el subject sin `rpc.`—, con la "+
				"operación en infinitivo y kebab-case: `clientes.cliente.v1.calcular-descuento` (11-request-reply.md §2.2).",
			citar(operation, 90), porQue), nil)
	}
	t := strings.Split(operation, ".")
	major, _ := strconv.Atoi(t[2][1:])
	return RPCOperation{Operation: operation, Domain: t[0], Aggregate: t[1], Major: major, Name: t[3]}, nil
}

// ResolveRPCTimeout valida el timeout de UNA llamada: un entero de milisegundos entre 1 y 25 000. Fuera de rango es
// INVALID_RPC_CALL: no se recorta ni se redondea (11-request-reply.md §4.2).
func ResolveRPCTimeout(d time.Duration) (time.Duration, error) {
	if d%time.Millisecond != 0 || d < RPCTimeoutMin || d > RPCTimeoutMax {
		return 0, invalidRPCCall(fmt.Sprintf(
			"el timeout debe ser un número ENTERO de milisegundos entre %d y %d (%d por defecto) y llegó %s. No se recorta en silencio: "+
				"un valor fuera de rango es un error. El máximo está por debajo del `ack_wait` de 30 s para que una llamada hecha desde un "+
				"handler de eventos no provoque su reentrega (11-request-reply.md §4.2).",
			RPCTimeoutMin.Milliseconds(), RPCTimeoutMax.Milliseconds(), RPCTimeoutDefault.Milliseconds(), d), nil)
	}
	return d, nil
}

func requireObject(data []byte, que string) error {
	if len(data) == 0 || data[0] != '{' {
		llego := "nada"
		switch {
		case len(data) > 0 && data[0] == '[':
			llego = "un array"
		case len(data) > 0:
			llego = "un valor que no es un objeto"
		}
		return invalidRPCCall(fmt.Sprintf(
			"%s debe ser un objeto JSON en la raíz (ni array ni escalar), para poder añadir campos sin romper el contrato "+
				"(01-envelope.md §4); llegó %s.", que, llego), nil)
	}
	return nil
}

func rpcClassification(c DataClassification) (DataClassification, error) {
	if !c.IsValid() {
		return "", &InvalidDataClassificationError{baseError{CodeInvalidDataClassif, ClassPermanent, nil}, string(c), "rpc"}
	}
	return c, nil
}

// rpcInputError: todo lo que un constructor de petición/respuesta rechaza de los atributos que el SDK rellena es
// INVALID_RPC_CALL: el conjunto de códigos de rpc es cerrado (04-errors.md §7) y agrupa «las equivocaciones de quien programa».
func rpcInputError(_ string, detail string) error { return invalidRPCCall(detail, nil) }

// ─── Nombres — 11-request-reply.md §2.2, §3 ──────────────────────────────────────────────────────────────────────

// RPCSubject es `rpc.clientes.cliente.v1.calcular-descuento`: el subject de NATS. Cinco tokens. NO es el atributo `subject`
// del sobre.
func RPCSubject(operation string) (string, error) {
	p, err := ParseRPCOperation(operation)
	if err != nil {
		return "", err
	}
	return "rpc." + p.Operation, nil
}

// RPCRequestType es `com.flux.rpc.<dominio>.<agregado>.<operacion>.request.v<major>`.
func RPCRequestType(operation string) (string, error) {
	p, err := ParseRPCOperation(operation)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("com.flux.rpc.%s.%s.%s.request.v%d", p.Domain, p.Aggregate, p.Name, p.Major), nil
}

// RPCReplyType es `com.flux.rpc.<dominio>.<agregado>.<operacion>.reply.v<major>`.
func RPCReplyType(operation string) (string, error) {
	p, err := ParseRPCOperation(operation)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("com.flux.rpc.%s.%s.%s.reply.v%d", p.Domain, p.Aggregate, p.Name, p.Major), nil
}

// RPCDataSchema es `https://schemas.internal/rpc/<dominio>/<agregado>/<operacion>/{request,reply}/<major>.0.0.json` (o con otra
// base): lo que se puede asumir SIN bundle. El bundle conoce el MINOR real y lo resuelve RPCSchemaURIFor. `side` es `request` o
// `reply`.
func RPCDataSchema(operation, side, base string) (string, error) {
	p, err := ParseRPCOperation(operation)
	if err != nil {
		return "", err
	}
	if base == "" {
		base = RPCSchemaBase
	}
	return fmt.Sprintf("%s/rpc/%s/%s/%s/%s/%d.0.0.json", strings.TrimRight(base, "/"), p.Domain, p.Aggregate, p.Name, side, p.Major), nil
}

// ─── El sobre — funciones PURAS (11-request-reply.md §3) ─────────────────────────────────────────────────────────

// RPCRequestInput es lo que alimenta el sobre de una petición.
type RPCRequestInput struct {
	// Operation es `clientes.cliente.v1.calcular-descuento`.
	Operation string
	TenantID  string
	// AggregateID va al atributo `subject` del sobre (y a `partitionkey`): la identidad del agregado. NO es el subject de
	// NATS. Vacío = ausente: el sobre no lleva ninguno de los dos (ni `null`).
	AggregateID string
	ID          string
	Source      string
	// Time es ISO-8601; se normaliza a la forma canónica (3 decimales y `Z`) o se rechaza.
	Time string
	// CorrelationID es el del evento o la petición en curso; si la llamada no nace de otro, EL `id` PROPIO.
	CorrelationID string
	// CausationID es el `id` del evento o la petición en curso; vacío si no hay.
	CausationID     string
	ProducerVersion string
	Classification  DataClassification
	// Data debe ser un objeto JSON; se escribe en su forma canónica.
	Data any
	// DataSchema: por defecto, `https://schemas.internal/rpc/<dom>/<agr>/<op>/request/<major>.0.0.json`.
	DataSchema  string
	TraceParent string
	TraceState  string
}

// BuildRPCRequest es el sobre de una petición: un CloudEvent normal con `type` `com.flux.rpc.….request.v<M>`, `subject` y
// `partitionkey` = AggregateID si se da, y `data` el ÚLTIMO atributo.
//
// PURA y determinista: `id` y `time` los da quien llama. Valida lo mismo que Request (§4.1, paso 0, salvo L3), en este orden y
// el primero que falla gana: RESERVED_DOMAIN → INVALID_RPC_CALL (operación, o `data` que no es un objeto) →
// INVALID_DATACLASSIFICATION → los atributos vacíos y el `time`.
func BuildRPCRequest(in RPCRequestInput) (Event, error) {
	p, err := ParseRPCOperation(in.Operation)
	if err != nil {
		return Event{}, err
	}
	data, err := dataBytes(in.Data)
	if err != nil {
		return Event{}, invalidRPCCall(err.Error(), nil)
	}
	if err := requireObject(data, "`data`"); err != nil {
		return Event{}, err
	}
	classification, err := rpcClassification(in.Classification)
	if err != nil {
		return Event{}, err
	}
	canonTime, err := requireEnvelopeInput(rpcInputError, [][3]string{
		{"id", in.ID, ""}, {"source", in.Source, ""}, {"correlationid", in.CorrelationID, "ext"},
		{"tenantid", in.TenantID, "ext"}, {"producerversion", in.ProducerVersion, "ext"},
	}, in.Time)
	if err != nil {
		return Event{}, err
	}
	typ, _ := RPCRequestType(p.Operation)
	schema := in.DataSchema
	if schema == "" {
		schema, _ = RPCDataSchema(p.Operation, "request", "")
	}
	return Event{
		SpecVersion: SpecVersion, ID: in.ID, Source: in.Source, Type: typ, Time: canonTime,
		DataContentType: DataContentType, DataSchema: schema, AggregateID: in.AggregateID,
		CorrelationID: in.CorrelationID, TenantID: in.TenantID, ProducerVersion: in.ProducerVersion,
		DataClassification: classification, CausationID: in.CausationID, PartitionKey: in.AggregateID,
		TraceParent: in.TraceParent, TraceState: in.TraceState, Data: data,
	}, nil
}

// RPCRequestRef es lo que la respuesta HEREDA de la petición. Es el arnés quien lo da plano; el SDK lo saca del sobre recibido.
type RPCRequestRef struct {
	Operation     string
	ID            string
	CorrelationID string
	TenantID      string
	// Subject: el `subject` de la petición; vacío si no lo traía.
	Subject string
	// PartitionKey: si la petición lo traía; si no, el `Subject`, que es la misma convención que en los eventos.
	PartitionKey string
}

func (r RPCRequestRef) partition() string {
	if r.PartitionKey != "" {
		return r.PartitionKey
	}
	return r.Subject
}

type rpcReplyBase struct {
	Request         RPCRequestRef
	ID              string
	Source          string
	Time            string
	ProducerVersion string
	// TraceParent es el del que responde: «de la misma traza» que la petición, con su propio span.
	TraceParent string
}

// RPCReplyInput es lo que alimenta el sobre de una respuesta.
type RPCReplyInput struct {
	rpcReplyBase
	// Classification es la de la operación; si no, la de la conexión.
	Classification DataClassification
	Data           any
	DataSchema     string
}

// NewRPCReplyInput agrupa la entrada de BuildRPCReply (los campos comunes con el error no se exportan sueltos).
func NewRPCReplyInput(request RPCRequestRef, id, source, time, producerVersion, traceParent string, classification DataClassification, data any, dataSchema string) RPCReplyInput {
	return RPCReplyInput{
		rpcReplyBase:   rpcReplyBase{request, id, source, time, producerVersion, traceParent},
		Classification: classification, Data: data, DataSchema: dataSchema,
	}
}

func replyAttrs(b rpcReplyBase) [][3]string {
	return [][3]string{
		{"id", b.ID, ""}, {"source", b.Source, ""}, {"producerversion", b.ProducerVersion, "ext"},
		{"request.id", b.Request.ID, ""}, {"request.correlationid", b.Request.CorrelationID, "ext"},
		{"request.tenantid", b.Request.TenantID, "ext"},
	}
}

// BuildRPCReply es el sobre de una respuesta: HEREDA `correlationid`, `tenantid`, `subject` y `partitionkey` de la petición, y
// su `causationid` es el `id` de la petición. `id` NUEVO: nunca el de la petición. `source`, `producerversion` y `traceparent` son
// del que responde.
func BuildRPCReply(in RPCReplyInput) (Event, error) {
	p, err := ParseRPCOperation(in.Request.Operation)
	if err != nil {
		return Event{}, err
	}
	data, err := dataBytes(in.Data)
	if err != nil {
		return Event{}, invalidRPCCall(err.Error(), nil)
	}
	if err := requireObject(data, "la respuesta"); err != nil {
		return Event{}, err
	}
	classification, err := rpcClassification(in.Classification)
	if err != nil {
		return Event{}, err
	}
	canonTime, err := requireEnvelopeInput(rpcInputError, replyAttrs(in.rpcReplyBase), in.Time)
	if err != nil {
		return Event{}, err
	}
	typ, _ := RPCReplyType(p.Operation)
	schema := in.DataSchema
	if schema == "" {
		schema, _ = RPCDataSchema(p.Operation, "reply", "")
	}
	return Event{
		SpecVersion: SpecVersion, ID: in.ID, Source: in.Source, Type: typ, Time: canonTime,
		DataContentType: DataContentType, DataSchema: schema, AggregateID: in.Request.Subject,
		CorrelationID: in.Request.CorrelationID, TenantID: in.Request.TenantID, ProducerVersion: in.ProducerVersion,
		DataClassification: classification, CausationID: in.Request.ID, PartitionKey: in.Request.partition(),
		TraceParent: in.TraceParent, Data: data,
	}, nil
}

// RPCErrorData es el `data` de `com.flux.rpc.error.v1`: `class` binaria (`poison` no viaja), `code` estable, `message` y
// `retryAfterMs`.
type RPCErrorData struct {
	// Class es `retryable` o `permanent`.
	Class   string
	Code    string
	Message string
	// RetryAfterMs es una sugerencia para el PRIMER reintento, solo con `retryable`. Nil = ausente: `0` SE ESCRIBE.
	RetryAfterMs *int64
}

// rpcErrorDataProblem dice si `d` cumple el esquema del error (`schemas/system/rpc/error/1.0.0.json`): devuelve el primer
// problema, o "". Es la quinta comprobación de quien llama (§4.3) y lo que valida el constructor: se escribe a mano y no con un
// validador porque L3 es opcional y esta comprobación no lo es.
func rpcErrorDataProblem(d RPCErrorData) string {
	if d.Class != "retryable" && d.Class != "permanent" {
		return "`class` del error debe ser `retryable` o `permanent` (`poison` no viaja)"
	}
	if len(d.Code) > rpcErrorCodeMax || !rpcErrorCodePattern.MatchString(d.Code) {
		return "`code` del error debe ser un identificador estable en MAYÚSCULAS y guiones bajos (máx. 64)"
	}
	// `maxLength` de JSON Schema cuenta caracteres Unicode (puntos de código), no unidades UTF-16 ni bytes: mil emojis caben.
	if n := codePointLength(d.Message); n < 1 || n > rpcErrorMessageMax {
		return "`message` del error debe ser un texto de 1 a 1000 caracteres"
	}
	if d.RetryAfterMs != nil {
		if *d.RetryAfterMs < 0 {
			return "`retryAfterMs` del error debe ser un entero ≥ 0"
		}
		if d.Class != "retryable" {
			return "`retryAfterMs` solo tiene sentido con `class: retryable`"
		}
	}
	return ""
}

// RPCErrorInput es lo que alimenta el sobre de un error.
type RPCErrorInput struct {
	rpcReplyBase
	Error RPCErrorData
}

// NewRPCErrorInput agrupa la entrada de BuildRPCError.
func NewRPCErrorInput(request RPCRequestRef, id, source, time, producerVersion, traceParent string, e RPCErrorData) RPCErrorInput {
	return RPCErrorInput{rpcReplyBase: rpcReplyBase{request, id, source, time, producerVersion, traceParent}, Error: e}
}

// BuildRPCError es el sobre de un error `com.flux.rpc.error.v1`: lo que el que responde devuelve EN LUGAR de la respuesta, tanto el
// de un handler que lanzó como el que fabrica su SDK. `type`, `dataschema` y `dataclassification: internal` son FIJOS —el error
// no depende del contrato de la operación—, y `data` lleva las claves en el orden `class, code, message, retryAfterMs`.
func BuildRPCError(in RPCErrorInput) (Event, error) {
	if _, err := ParseRPCOperation(in.Request.Operation); err != nil {
		return Event{}, err
	}
	// Un error que no cumple su esquema es INVALID_RPC_CALL (04-errors.md §7): un código estable, nunca una excepción sin código.
	if problema := rpcErrorDataProblem(in.Error); problema != "" {
		return Event{}, invalidRPCCall("el error que se iba a contestar no cumple su esquema: "+problema, nil)
	}
	canonTime, err := requireEnvelopeInput(rpcInputError, replyAttrs(in.rpcReplyBase), in.Time)
	if err != nil {
		return Event{}, err
	}
	data := []byte(`{"class":` + jsonString(in.Error.Class) + `,"code":` + jsonString(in.Error.Code) + `,"message":` + jsonString(in.Error.Message))
	if in.Error.RetryAfterMs != nil {
		data = append(data, `,"retryAfterMs":`+strconv.FormatInt(*in.Error.RetryAfterMs, 10)...)
	}
	data = append(data, '}')
	return Event{
		SpecVersion: SpecVersion, ID: in.ID, Source: in.Source, Type: RPCErrorType, Time: canonTime,
		DataContentType: DataContentType, DataSchema: RPCErrorDataSchema, AggregateID: in.Request.Subject,
		CorrelationID: in.Request.CorrelationID, TenantID: in.Request.TenantID, ProducerVersion: in.ProducerVersion,
		DataClassification: ClassificationInternal, CausationID: in.Request.ID, PartitionKey: in.Request.partition(),
		TraceParent: in.TraceParent, Data: data,
	}, nil
}
