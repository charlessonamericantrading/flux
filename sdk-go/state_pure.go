// Estado replicado — las funciones PURAS y deterministas de la extensión opcional `state`.
// Contrato normativo: specification/10-state.md, specification/04-errors.md §6
//
// Un EVENTO dice qué pasó; el ESTADO es el valor ACTUAL de un agregado, que su dueño publica y los demás
// observan. Sustituye a `GET /clientes/123`, a la carga inicial y al polling de datos.
//
// Este fichero es la mitad que NO habla con la red: el subject, el sobre de un `put`, la lápida y el evento de
// DLQ de estado. Producen los MISMOS BYTES en todos los SDKs (10-state.md §2.6; arnés cruzado `state-build`,
// `state-tombstone`, `state-subject` y `state-dlq`). Lo que las alimenta —`id`, `time`— se pasa DE FUERA: si la
// función los inventara, dos ejecuciones no serían comparables. La otra mitad, `State` (put, delete, get y
// watch), está en state.go.

package flux

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// ─── Constantes y reglas — protocol.json → `state` ───────────────────────────────────────────────────────────────

const (
	// StateTenantMaxLength: NATS limita los nombres de consumidor a 255 y el durable de un observador lleva el
	// tenant (A-42).
	StateTenantMaxLength = 64
	// AggregateVersionMax es 2^53 − 1: el mayor entero que Node, y todo lo que use `double`, representa sin perder
	// precisión (10-state.md §3.4).
	AggregateVersionMax = 1<<53 - 1
	// StateMaxAttempts son los intentos de un `Put` o `Delete` ante un conflicto de secuencia (10-state.md §3.2, paso 4).
	StateMaxAttempts = 3
	// TombstoneDataSchema es el `dataschema` FIJO de toda lápida, de cualquier contrato.
	TombstoneDataSchema = "https://schemas.internal/system/state/eliminado/1.0.0.json"
	// StateSchemaBase es la base de los esquemas del repo: `schemas/state/<dom>/<agr>/<semver>.json` cuelga de aquí.
	StateSchemaBase = "https://schemas.internal"
	// DefaultStateClassification es la de un `Put` sin clasificación explícita: NO la de la conexión (10-state.md §3.6).
	DefaultStateClassification = ClassificationConfidential
	// UnclassifiedDLQError es lo que sigue al código en el `dlqerror` de un error que NO es de flux (10-state.md §6): el
	// mensaje de un driver de base de datos puede citar la fila entera, y la DLQ guarda 90 días.
	UnclassifiedDLQError = "error sin clasificar de la aplicación; el mensaje se omite para no copiar datos a la DLQ (10-state.md §6)"
)

// Los DOS códigos del conflicto de secuencia (A-33, A-49): 10071 «wrong last sequence: N» (un nodo, o un cluster
// sin carrera) y 10164 «wrong last sequence», SIN número (cluster, bajo carrera: 210 de 210 rechazos de 8
// escritores). Un SDK que solo reconozca el 10071 pasa sus pruebas contra un nodo y, en producción, trata un
// conflicto corriente como un error desconocido y no reintenta.
var stateConflictCodes = [...]int{10071, 10164}

var (
	// `^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`: un único token de NATS. En RE2 `$` solo casa al final del texto: un `\n`
	// final NO pasa el patrón, a diferencia de Python (`re.match`) y PHP (`preg_match` sin `D`).
	stateKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
	// `^[a-z0-9]+(-[a-z0-9]+)*$` y, aparte (un patrón puro, sin lookahead), 64 como máximo.
	stateTenantPattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	// `<dominio>.<agregado>.v<major>`: el subject sin `state.`, sin tenant y sin clave.
	stateContractPattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]{0,3}$`)
)

// ─── Validación — se hace SIEMPRE, antes de abrir ninguna petición ───────────────────────────────────────────────

// StateContract son los tres tokens de un contrato ya validado.
type StateContract struct {
	Contract  string
	Domain    string
	Aggregate string
	Major     int
}

// ParseStateContract valida un contrato. Primero el dominio reservado (RESERVED_DOMAIN), después la forma
// (INVALID_STATE_CONTRACT): el orden de 10-state.md §3.6.
func ParseStateContract(contract string) (StateContract, error) {
	if err := assertNotReservedDomain(contract, "contrato"); err != nil {
		return StateContract{}, err
	}
	if !stateContractPattern.MatchString(contract) {
		n := len(strings.Split(contract, "."))
		motivo := "formato esperado <dominio>.<agregado>.v<major> (la versión mayor, de 1 a 4 dígitos: v1…v9999)"
		if n != 3 {
			motivo = fmt.Sprintf("debe tener exactamente 3 tokens, tiene %d", n)
		}
		return StateContract{}, &InvalidStateContractError{
			baseError: baseError{CodeInvalidStateContract, ClassPermanent, nil}, Contract: contract, detail: motivo,
		}
	}
	t := strings.Split(contract, ".")
	major, _ := strconv.Atoi(t[2][1:])
	return StateContract{Contract: contract, Domain: t[0], Aggregate: t[1], Major: major}, nil
}

func assertStateKey(key string) error {
	if !stateKeyPattern.MatchString(key) {
		return &InvalidStateKeyError{baseError{CodeInvalidStateKey, ClassPermanent, nil}, key}
	}
	return nil
}

func assertStateTenant(tenant string) error {
	if len(tenant) > StateTenantMaxLength || !stateTenantPattern.MatchString(tenant) {
		return &InvalidTenantForStateError{baseError: baseError{CodeInvalidTenantForState, ClassPermanent, nil}, Tenant: tenant}
	}
	return nil
}

func missingTenant() error {
	return &InvalidTenantForStateError{baseError: baseError{CodeInvalidTenantForState, ClassPermanent, nil}, Missing: true}
}

// AggregateVersionFromJSON interpreta un valor JSON como un `aggregateVersion`: un ENTERO ≥ 1 y ≤ 2^53 − 1. El nombre
// del error cubre todo lo que no sea eso: ausente, null, una cadena, un decimal (`7.5`, y `7.0` o `1e0`: en Go el
// parser separa el entero del flotante y los rechaza, 10-state.md §3.4), 0, un negativo o uno mayor.
func AggregateVersionFromJSON(raw []byte) (int64, error) {
	lit := strings.TrimSpace(string(raw))
	if v, ok := parseVersionLiteral(lit); ok {
		return v, nil
	}
	recibido := "llegó " + citarJSON(lit)
	if lit == "" {
		recibido = "no hay ninguno"
	}
	return 0, &MissingAggregateVersionError{baseError{CodeMissingAggregateVer, ClassPermanent, nil}, recibido}
}

// parseVersionLiteral acepta solo el literal de un entero ≥ 1 y ≤ 2^53 − 1 escrito como tal.
func parseVersionLiteral(lit string) (int64, bool) {
	if lit == "" || len(lit) > 16 || lit[0] < '1' || lit[0] > '9' {
		return 0, false
	}
	for i := 1; i < len(lit); i++ {
		if lit[i] < '0' || lit[i] > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseInt(lit, 10, 64)
	if err != nil || v < 1 || v > AggregateVersionMax {
		return 0, false
	}
	return v, true
}

func citarJSON(lit string) string {
	switch {
	case lit == "null":
		return "null"
	case strings.HasPrefix(lit, `"`):
		return "una cadena"
	case lit == "true" || lit == "false":
		return "un booleano"
	case strings.HasPrefix(lit, "{") || strings.HasPrefix(lit, "["):
		return "un objeto o un array"
	}
	if len(lit) > 24 {
		lit = lit[:24] + "…"
	}
	return lit
}

// aggregateVersionOfData lee el `aggregateVersion` de un `data` ya canónico (un objeto), o devuelve
// MISSING_AGGREGATE_VERSION. Se comprueba SIEMPRE, con o sin L3: es una regla del protocolo.
func aggregateVersionOfData(data []byte) (int64, error) {
	var obj map[string]json.RawMessage
	if len(data) == 0 || data[0] != '{' || json.Unmarshal(data, &obj) != nil {
		return 0, &MissingAggregateVersionError{baseError{CodeMissingAggregateVer, ClassPermanent, nil}, "`data` no es un objeto"}
	}
	return AggregateVersionFromJSON(obj["aggregateVersion"])
}

// stateClassification: vacío → `confidential`; `restricted` o una fuera del enum → INVALID_DATACLASSIFICATION.
func stateClassification(c DataClassification) (DataClassification, error) {
	switch c {
	case "":
		return DefaultStateClassification, nil
	case ClassificationPublic, ClassificationInternal, ClassificationConfidential:
		return c, nil
	}
	return "", &InvalidDataClassificationError{
		baseError{CodeInvalidDataClassif, ClassPermanent, nil}, string(c), "estado"}
}

// requireEnvelopeInput comprueba lo que todo constructor de estado hace además de la forma del contrato, la clave y
// el tenant: que los atributos que el SDK rellena no lleguen vacíos y que `time` sea un instante (se normaliza a la
// forma canónica). El mismo código que daría el parser ante el mismo defecto: el conjunto de estado (04-errors.md §6)
// es cerrado y ninguno de sus siete encaja.
func requireEnvelopeInput(rechaza func(code, detail string) error, campos [][3]string, timeText string) (string, error) {
	for _, c := range campos {
		if c[1] == "" {
			code := CodeMissingRequiredAttr
			if c[2] == "ext" {
				code = CodeMissingRequiredExt
			}
			return "", rechaza(code, fmt.Sprintf("`%s` es obligatorio y llegó vacío. Lo rellena el SDK a partir de Connect() y del "+
				"evento en curso; si estás llamando a un constructor a mano, rellénalo con una cadena no vacía (01-envelope.md §5).", c[0]))
		}
	}
	canon, ok := canonicalTime(timeText)
	if !ok {
		return "", rechaza(CodeWrongAttributeType, "`time` debe ser un instante RFC 3339 (`2026-08-20T10:41:07.318Z`; se normaliza "+
			"a 3 decimales y `Z`) y llegó un texto que no lo es (01-envelope.md §2.2).")
	}
	return canon, nil
}

func stateInputError(code, detail string) error { return invalidInput(code, detail) }

// ─── Nombres — 02-naming.md, 10-state.md §2 ──────────────────────────────────────────────────────────────────────

// StateStreamName es `STATE_CLIENTES`. NATS no admite puntos en nombres de stream.
func StateStreamName(domain string) string {
	return "STATE_" + strings.ToUpper(strings.ReplaceAll(domain, "-", "_"))
}

// StateSubject es `state.<dominio>.<agregado>.v<major>.<tenant>.<clave>`: el subject de NATS de una entrada. Seis
// tokens, siempre. NO es el atributo `subject` de CloudEvents, que aquí vale la clave.
//
// Valida en el orden de 10-state.md §3.6 —dominio reservado, forma del contrato, clave, tenant— y devuelve el código
// estable de cada rechazo.
func StateSubject(contract, tenantID, key string) (string, error) {
	c, err := ParseStateContract(contract)
	if err != nil {
		return "", err
	}
	if err := assertStateKey(key); err != nil {
		return "", err
	}
	if err := assertStateTenant(tenantID); err != nil {
		return "", err
	}
	return "state." + c.Contract + "." + tenantID + "." + key, nil
}

// StateType es `com.flux.state.<dominio>.<agregado>.v<major>`.
func StateType(contract string) (string, error) {
	c, err := ParseStateContract(contract)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("com.flux.state.%s.%s.v%d", c.Domain, c.Aggregate, c.Major), nil
}

// StateTombstoneType es `com.flux.state.<dominio>.<agregado>.deleted.v<major>`.
func StateTombstoneType(contract string) (string, error) {
	c, err := ParseStateContract(contract)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("com.flux.state.%s.%s.deleted.v%d", c.Domain, c.Aggregate, c.Major), nil
}

// StateDataSchema es `https://schemas.internal/state/<dominio>/<agregado>/<major>.0.0.json` (o con otra base).
func StateDataSchema(contract, base string) (string, error) {
	c, err := ParseStateContract(contract)
	if err != nil {
		return "", err
	}
	if base == "" {
		base = StateSchemaBase
	}
	return fmt.Sprintf("%s/state/%s/%s/%d.0.0.json", strings.TrimRight(base, "/"), c.Domain, c.Aggregate, c.Major), nil
}

// StateDurableName es `<servicio>__state_<dom>_<agr>_v<M>` y, si filtra por tenant, el sufijo `__<tenant>`. Los puntos y
// los guiones pasan a `_`. El tenant va en el nombre porque UN DURABLE COMPARTIDO REPARTE LOS MENSAJES ENTRE QUIENES LO
// USAN (A-47): dos instancias del mismo servicio, una por tenant, con un durable sin sufijo, se quitarían la mitad de la
// foto cada una.
func StateDurableName(service, contract, tenantID string) (string, error) {
	if !ServicePattern.MatchString(service) {
		return "", &InvalidServiceNameError{Service: service}
	}
	c, err := ParseStateContract(contract)
	if err != nil {
		return "", err
	}
	flat := strings.ReplaceAll
	// El servicio conserva sus guiones (`tienda-web__…`); lo que viene después, no.
	base := fmt.Sprintf("%s__state_%s_%s_v%d", service, flat(c.Domain, "-", "_"), flat(c.Aggregate, "-", "_"), c.Major)
	if tenantID == "" {
		return base, nil
	}
	if err := assertStateTenant(tenantID); err != nil {
		return "", err
	}
	return base + "__" + flat(tenantID, "-", "_"), nil
}

// StateFilterSubject es `state.<dom>.<agr>.v<M>.<tenant>.*`, o `….*.*` sin tenant.
func StateFilterSubject(contract, tenantID string) (string, error) {
	c, err := ParseStateContract(contract)
	if err != nil {
		return "", err
	}
	if tenantID != "" {
		if err := assertStateTenant(tenantID); err != nil {
			return "", err
		}
	} else {
		tenantID = "*"
	}
	return "state." + c.Contract + "." + tenantID + ".*", nil
}

// StateDLQSubject es `dlq.state.<dom>.<agr>.v<M>`: SIN tenant ni clave, para que no se multiplique por ellos.
func StateDLQSubject(contract string) (string, error) {
	c, err := ParseStateContract(contract)
	if err != nil {
		return "", err
	}
	return "dlq.state." + c.Contract, nil
}

// ─── El sobre — funciones PURAS (10-state.md §2.5, §2.6, §4, §6) ─────────────────────────────────────────────────

// StateEventInput es lo que alimenta el sobre de un `Put`.
type StateEventInput struct {
	// Contract es `clientes.cliente.v1`.
	Contract string
	TenantID string
	Key      string
	ID       string
	Source   string
	// Time es ISO-8601; se normaliza a la forma canónica (3 decimales y `Z`) o se rechaza.
	Time            string
	CorrelationID   string
	ProducerVersion string
	// Classification: por defecto `confidential`.
	Classification DataClassification
	// Data DEBE llevar `aggregateVersion` (entero ≥ 1). Se acepta un json.RawMessage o cualquier valor serializable a un
	// objeto JSON; se escribe en su forma canónica.
	Data any
	// DataSchema: por defecto, `https://schemas.internal/state/<dom>/<agr>/<major>.0.0.json`.
	DataSchema  string
	CausationID string
	TraceParent string
	TraceState  string
}

// dataBytes serializa `data` y lo lleva a su forma canónica. Devuelve el texto canónico del objeto.
func dataBytes(data any) ([]byte, error) {
	var raw []byte
	switch d := data.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		if len(d) == 0 {
			return nil, nil
		}
		raw = d
	case []byte:
		if len(d) == 0 {
			return nil, nil
		}
		raw = d
	default:
		b, err := marshalNoHTMLEscape(d)
		if err != nil {
			return nil, &EnvelopeError{Message: "`data` no es serializable a JSON: " + err.Error()}
		}
		raw = b
	}
	canon, err := canonicalizeJSON(raw)
	if err != nil {
		return nil, &EnvelopeError{Message: "`data` no se puede escribir en su forma canónica: " + err.Error() +
			" (01-envelope.md §4.1)"}
	}
	return canon, nil
}

func stateEnvelope(contract StateContract, typ, dataschema, tenant, key, id, source, time, correlation, producer string,
	classification DataClassification, causation, traceparent, tracestate string, data []byte) Event {
	return Event{
		SpecVersion: SpecVersion, ID: id, Source: source, Type: typ, Time: time,
		DataContentType: DataContentType, DataSchema: dataschema,
		AggregateID: key, CorrelationID: correlation, TenantID: tenant, ProducerVersion: producer,
		DataClassification: classification, CausationID: causation, PartitionKey: key,
		TraceParent: traceparent, TraceState: tracestate, Data: data,
	}
}

// BuildStateEvent es el sobre de un `put`: un CloudEvent normal con `subject` = la clave, `tenantid` = el token de
// tenant del subject de NATS, `partitionkey` = la clave, y `data` el ÚLTIMO atributo.
//
// PURA y determinista: `id` y `time` los da quien llama. Valida lo mismo que `Put` (§3.6, salvo L3), así que lo que
// construye es una entrada legal. El orden de las validaciones está en los vectores: el primero que falla gana.
func BuildStateEvent(in StateEventInput) (Event, error) {
	c, err := ParseStateContract(in.Contract)
	if err != nil {
		return Event{}, err
	}
	if err := assertStateKey(in.Key); err != nil {
		return Event{}, err
	}
	if err := assertStateTenant(in.TenantID); err != nil {
		return Event{}, err
	}
	data, err := dataBytes(in.Data)
	if err != nil {
		return Event{}, err
	}
	if _, err := aggregateVersionOfData(data); err != nil {
		return Event{}, err
	}
	classification, err := stateClassification(in.Classification)
	if err != nil {
		return Event{}, err
	}
	time, err := requireEnvelopeInput(stateInputError, [][3]string{
		{"id", in.ID, ""}, {"source", in.Source, ""},
		{"correlationid", in.CorrelationID, "ext"}, {"producerversion", in.ProducerVersion, "ext"},
	}, in.Time)
	if err != nil {
		return Event{}, err
	}
	typ, _ := StateType(c.Contract)
	schema := in.DataSchema
	if schema == "" {
		schema, _ = StateDataSchema(c.Contract, "")
	}
	return stateEnvelope(c, typ, schema, in.TenantID, in.Key, in.ID, in.Source, time, in.CorrelationID, in.ProducerVersion,
		classification, in.CausationID, in.TraceParent, in.TraceState, data), nil
}

// StateTombstoneInput es lo que alimenta la lápida de un `Delete`.
type StateTombstoneInput struct {
	Contract        string
	TenantID        string
	Key             string
	ID              string
	Source          string
	Time            string
	CorrelationID   string
	ProducerVersion string
	// AggregateVersion es la versión DE LA LÁPIDA: debe superar la del valor que borra.
	AggregateVersion int64
	CausationID      string
	TraceParent      string
	TraceState       string
}

// BuildStateTombstone es la lápida de un `delete`: el mismo subject de NATS que el valor, `type` con `.deleted.`,
// `dataschema` FIJO, `dataclassification: "internal"` y `data: { aggregateVersion }` y nada más. Al entrar, el valor
// anterior SALE del stream (A-34). Lleva versión porque `put` la lee para decidir si una escritura nueva es `stale`.
func BuildStateTombstone(in StateTombstoneInput) (Event, error) {
	c, err := ParseStateContract(in.Contract)
	if err != nil {
		return Event{}, err
	}
	if err := assertStateKey(in.Key); err != nil {
		return Event{}, err
	}
	if err := assertStateTenant(in.TenantID); err != nil {
		return Event{}, err
	}
	if in.AggregateVersion < 1 || in.AggregateVersion > AggregateVersionMax {
		return Event{}, &MissingAggregateVersionError{baseError{CodeMissingAggregateVer, ClassPermanent, nil},
			"llegó " + strconv.FormatInt(in.AggregateVersion, 10)}
	}
	time, err := requireEnvelopeInput(stateInputError, [][3]string{
		{"id", in.ID, ""}, {"source", in.Source, ""},
		{"correlationid", in.CorrelationID, "ext"}, {"producerversion", in.ProducerVersion, "ext"},
	}, in.Time)
	if err != nil {
		return Event{}, err
	}
	typ, _ := StateTombstoneType(c.Contract)
	data := []byte(`{"aggregateVersion":` + strconv.FormatInt(in.AggregateVersion, 10) + `}`)
	return stateEnvelope(c, typ, TombstoneDataSchema, in.TenantID, in.Key, in.ID, in.Source, time, in.CorrelationID, in.ProducerVersion,
		ClassificationInternal, in.CausationID, in.TraceParent, in.TraceState, data), nil
}

// StateDLQInfo es lo que lleva el evento de DLQ de estado.
type StateDLQInfo struct {
	// Key es la clave del SUBJECT de NATS —la que decidió el broker—, no la del sobre.
	Key string
	// Revision es la secuencia del stream del mensaje que falló.
	Revision uint64
	Reason   DLQReason
	Attempts int
	Consumer string
	Error    string
	// DLQTime lo da quien llama: si lo pusiera esta función, los bytes no serían comparables.
	DLQTime string
}

// ToStateDLQEvent es el evento de DLQ de estado: el sobre ORIGINAL con las extensiones `dlq*` y SIN su dato: `data`
// pasa a `{ key, revision }`.
//
//   - La DLQ guarda 90 días: un dato personal que el dueño ya borró seguiría vivo ahí tres meses y el derecho de
//     supresión no se cumpliría (10-state.md §6).
//   - NO conserva `signkeyid` ni `signature`: firman un `data` que ya no está, y una firma que no verifica sobre un
//     sobre que parece el original es una afirmación falsa (07-signing.md §5).
//   - `data` sigue siendo el ÚLTIMO atributo, con las `dlq*` justo antes.
func ToStateDLQEvent(event Event, info StateDLQInfo) (Event, error) {
	dlqTime, ok := canonicalTime(info.DLQTime)
	if !ok {
		return Event{}, invalidInput(CodeWrongAttributeType, "`dlqtime` debe ser un instante RFC 3339 (01-envelope.md §2.2).")
	}
	event.SignKeyID, event.Signature = "", ""
	event.DLQReason = info.Reason
	event.DLQAttempts = info.Attempts
	event.DLQConsumer = info.Consumer
	// 1024 BYTES UTF-8 sin partir un carácter (04-errors.md §3).
	event.DLQError = truncateUTF8Bytes(info.Error, 1024)
	event.DLQTime = dlqTime
	event.Data = []byte(`{"key":` + jsonString(info.Key) + `,"revision":` + strconv.FormatUint(info.Revision, 10) + `}`)
	return event, nil
}

// StateDLQBuildInput es lo que el arnés cruzado le pasa a `state-dlq`: el original (la entrada de `state-build`), la
// revisión y los `dlq*`.
type StateDLQBuildInput struct {
	Original StateEventInput
	Revision uint64
	DLQ      StateDLQInfo
}

// BuildStateDLQEvent es ToStateDLQEvent sobre el sobre que BuildStateEvent construye de `Original`. Pura.
func BuildStateDLQEvent(in StateDLQBuildInput) (Event, error) {
	e, err := BuildStateEvent(in.Original)
	if err != nil {
		return Event{}, err
	}
	info := in.DLQ
	info.Key, info.Revision = in.Original.Key, in.Revision
	return ToStateDLQEvent(e, info)
}

// UnreadableStateDLQInput es lo que alimenta el evento de DLQ de un mensaje ILEGIBLE.
type UnreadableStateDLQInput struct {
	Contract string
	TenantID string
	Key      string
	Revision uint64
	// Source y ProducerVersion son los del servicio que consume.
	Source          string
	ProducerVersion string
	ID              string
	Time            string
	DataSchema      string
	DLQ             StateDLQInfo
}

// BuildUnreadableStateDLQEvent: un mensaje ILEGIBLE (JSON roto, sin sobre) no se copia a la DLQ de estado: no se guarda
// su cuerpo en ninguna forma —ni los `rawBase64` de los eventos—, porque podría contener el dato. No hay sobre original
// que conservar, así que se sintetiza uno con lo único fiable, EL SUBJECT (10-state.md §6).
func BuildUnreadableStateDLQEvent(in UnreadableStateDLQInput) (Event, error) {
	c, err := ParseStateContract(in.Contract)
	if err != nil {
		return Event{}, err
	}
	time, err := requireEnvelopeInput(stateInputError, [][3]string{
		{"id", in.ID, ""}, {"source", in.Source, ""}, {"producerversion", in.ProducerVersion, "ext"},
	}, in.Time)
	if err != nil {
		return Event{}, err
	}
	typ, _ := StateType(c.Contract)
	schema := in.DataSchema
	if schema == "" {
		schema, _ = StateDataSchema(c.Contract, "")
	}
	synth := stateEnvelope(c, typ, schema, in.TenantID, in.Key, in.ID, in.Source, time, in.ID, in.ProducerVersion,
		ClassificationInternal, "", "", "", []byte("{}"))
	info := in.DLQ
	info.Key, info.Revision = in.Key, in.Revision
	return ToStateDLQEvent(synth, info)
}
