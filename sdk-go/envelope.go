// Envelope CloudEvents 1.0 — construcción, serialización y parseo.
// Contrato normativo: specification/01-envelope.md
//
// flux v1 usa structured mode: el CloudEvent completo viaja serializado como JSON en
// el cuerpo del mensaje NATS. Un mensaje ES un fichero JSON completo — lo copias, lo
// pegas en un test, lo reproduces. Esa propiedad vale más que los bytes que ahorraría
// binary mode repartiendo los atributos entre cabeceras `ce-*` y cuerpo.

package flux

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// DataClassification es la clasificación del dato transportado — 06-security.md §5.
// Determina la retención efectiva del evento.
type DataClassification string

const (
	ClassificationPublic       DataClassification = "public"
	ClassificationInternal     DataClassification = "internal"
	ClassificationConfidential DataClassification = "confidential"
	ClassificationRestricted   DataClassification = "restricted"
)

// IsValid informa si la clasificación es una de las cuatro del protocolo.
func (c DataClassification) IsValid() bool {
	switch c {
	case ClassificationPublic, ClassificationInternal,
		ClassificationConfidential, ClassificationRestricted:
		return true
	}
	return false
}

// DLQReason indica por qué un evento acabó en la DLQ — 04-errors.md §3.
type DLQReason string

const (
	DLQReasonRetryable DLQReason = "retryable"
	DLQReasonPermanent DLQReason = "permanent"
	DLQReasonPoison    DLQReason = "poison"
)

// TimeFormat es RFC 3339 UTC con precisión de milisegundos EXACTA — 01-envelope.md §2.
//
// No se usa time.RFC3339Nano a propósito: recorta los ceros finales, así que
// 10:25:39.410Z se serializaría como 10:25:39.41Z. Sería válido como RFC 3339 pero
// dejaría de ser byte a byte igual al `toISOString()` de Node y al de .NET, y el
// envelope tiene que ser idéntico en los cuatro lenguajes.
const TimeFormat = "2006-01-02T15:04:05.000Z07:00"

// FormatTime serializa un instante en el formato del protocolo (UTC, 3 decimales).
func FormatTime(t time.Time) string {
	return t.UTC().Format(TimeFormat)
}

// ─── El evento ───────────────────────────────────────────────────────────────

// Event es un evento de flux: un CloudEvent 1.0 con el perfil de extensiones de flux.
//
// Los nombres de las extensiones van en minúscula pegada (`correlationid`, no
// `correlation_id`) porque CloudEvents restringe los nombres de extensión a
// minúsculas y dígitos ASCII sin separadores. No es estilo: es la especificación
// (01-envelope.md §3). De ahí que los tags json no sigan ninguna convención de Go.
//
// ⚠️ Data es json.RawMessage y no `any` a propósito: mantiene el payload sin
// interpretar, de modo que un evento parseado y vuelto a serializar es byte a byte
// el mismo. Eso es lo que hace que el replay desde la DLQ sea `del(.dlq*)` y
// republicar, sin reordenar claves ni convertir enteros grandes a float64. Para
// tiparlo, usa UnmarshalData.
type Event struct {
	// ── Núcleo CloudEvents — 01-envelope.md §2 ──
	SpecVersion     string `json:"specversion"`
	ID              string `json:"id"`
	Source          string `json:"source"`
	Type            string `json:"type"`
	Time            string `json:"time"`
	DataContentType string `json:"datacontenttype"`
	DataSchema      string `json:"dataschema"`

	// AggregateID es el atributo `subject` de CloudEvents.
	//
	// ⚠️ NO es el subject de NATS. El de NATS es una dirección de enrutado
	// ("pedidos.pedido.v1.creado"); éste es la identidad del agregado dentro de la
	// fuente ("ped-123"). Confundirlos es el error más frecuente al adoptar
	// CloudEvents sobre NATS, así que el SDK los nombra distinto y solo los une al
	// serializar — 01-envelope.md §2.1.
	AggregateID string `json:"subject,omitempty"`

	// ── Extensiones obligatorias — 01-envelope.md §3.1 ──

	// CorrelationID identifica el flujo de negocio completo y se propaga sin
	// modificar. Si el evento no nace de otro, el SDK lo inicializa con su propio ID.
	CorrelationID string `json:"correlationid"`
	// TenantID: "system" para eventos de plataforma.
	TenantID string `json:"tenantid"`
	// ProducerVersion es el SemVer del servicio emisor. Sin esto, un bug de payload
	// en producción no se puede acotar a un despliegue.
	ProducerVersion    string             `json:"producerversion"`
	DataClassification DataClassification `json:"dataclassification"`

	// ── Extensiones opcionales — 01-envelope.md §3.2 ──

	// CausationID es el ID del evento concreto que provocó éste. CorrelationID
	// responde "¿de qué flujo forma parte?"; CausationID, "¿quién lo causó?".
	CausationID string `json:"causationid,omitempty"`
	// PartitionKey: por convención, igual al AggregateID.
	PartitionKey string `json:"partitionkey,omitempty"`
	TraceParent  string `json:"traceparent,omitempty"`
	TraceState   string `json:"tracestate,omitempty"`

	// ── Extensiones de DLQ — solo presentes en dlq.<subject> — 04-errors.md §3 ──

	DLQReason DLQReason `json:"dlqreason,omitempty"`
	// DLQAttempts distingue de un vistazo un PERMANENT (1) de un RETRYABLE
	// agotado (6).
	//
	// ⚠️ `omitempty` colapsa 0 y "ausente" en Go. Aquí es inocuo porque el valor
	// mínimo legal es 1 —siempre hubo al menos una entrega—, pero es la clase de
	// ambigüedad que un lenguaje con tipos no nulos introduce y conviene tener
	// presente. Ver README §"Fricciones".
	DLQAttempts int    `json:"dlqattempts,omitempty"`
	DLQConsumer string `json:"dlqconsumer,omitempty"`
	DLQError    string `json:"dlqerror,omitempty"`
	DLQTime     string `json:"dlqtime,omitempty"`

	// ── Payload — 01-envelope.md §4 ──
	Data json.RawMessage `json:"data"`
}

// EventTime interpreta el atributo `time` como instante.
func (e Event) EventTime() (time.Time, error) {
	return time.Parse(time.RFC3339, e.Time)
}

// Equal compara dos eventos por valor.
//
// Existe porque Event NO es comparable con `==`: contiene un json.RawMessage, que es
// un slice. Es una consecuencia directa de que el payload del envelope sea un objeto
// JSON arbitrario — no hay forma de tenerlo sin interpretar Y comparable a la vez.
// Sin este método, cada aplicación acabaría llamando a reflect.DeepEqual sobre el
// evento entero. Ver README §"Fricciones".
//
// La comparación del payload es byte a byte, no semántica: dos eventos con el mismo
// contenido JSON pero distinto orden de claves NO son iguales. Es lo correcto aquí,
// porque el protocolo transporta bytes y el replay debe preservarlos.
//
// Los campos se comparan uno a uno en vez de con reflect.DeepEqual para que añadir un
// atributo al envelope sin tocar esta función sea un fallo visible en revisión, y no
// una comparación que empieza a mentir en silencio.
func (e Event) Equal(other Event) bool {
	return e.SpecVersion == other.SpecVersion &&
		e.ID == other.ID &&
		e.Source == other.Source &&
		e.Type == other.Type &&
		e.Time == other.Time &&
		e.DataContentType == other.DataContentType &&
		e.DataSchema == other.DataSchema &&
		e.AggregateID == other.AggregateID &&
		e.CorrelationID == other.CorrelationID &&
		e.TenantID == other.TenantID &&
		e.ProducerVersion == other.ProducerVersion &&
		e.DataClassification == other.DataClassification &&
		e.CausationID == other.CausationID &&
		e.PartitionKey == other.PartitionKey &&
		e.TraceParent == other.TraceParent &&
		e.TraceState == other.TraceState &&
		e.DLQReason == other.DLQReason &&
		e.DLQAttempts == other.DLQAttempts &&
		e.DLQConsumer == other.DLQConsumer &&
		e.DLQError == other.DLQError &&
		e.DLQTime == other.DLQTime &&
		bytes.Equal(e.Data, other.Data)
}

// ParsedType deriva los tokens del subject de NATS a partir del `type`. Útil en un
// handler que se suscribió con wildcard y necesita saber qué evento concreto llegó.
func (e Event) ParsedType() (ParsedSubject, error) {
	rest, ok := strings.CutPrefix(e.Type, "com.flux.")
	if !ok {
		return ParsedSubject{}, fmt.Errorf("type %q no sigue el formato com.flux.<dominio>.<agregado>.<evento>.v<major>", e.Type)
	}
	t := strings.Split(rest, ".")
	if len(t) != 4 {
		return ParsedSubject{}, fmt.Errorf("type %q no tiene los 4 tokens esperados tras com.flux.", e.Type)
	}
	return ParseSubject(fmt.Sprintf("%s.%s.%s.%s", t[0], t[1], t[3], t[2]))
}

// AllowedRootAttributes son los ÚNICOS atributos admitidos en la raíz del envelope.
// Todo lo demás va dentro de `data` — 01-envelope.md §3.3.
//
// La lista es cerrada y eso es deliberado: las extensiones de CloudEvents solo
// admiten String, Integer, Boolean, Binary, URI y Timestamp, nunca objetos ni arrays.
// Un equipo que empieza a colgar metadatos de la raíz acaba serializando JSON dentro
// de un string, y a partir de ahí el envelope deja de ser interoperable.
var AllowedRootAttributes = map[string]struct{}{
	"specversion": {}, "id": {}, "source": {}, "type": {}, "time": {},
	"datacontenttype": {}, "dataschema": {}, "subject": {},
	"correlationid": {}, "tenantid": {}, "producerversion": {}, "dataclassification": {},
	"causationid": {}, "partitionkey": {}, "traceparent": {}, "tracestate": {},
	"dlqreason": {}, "dlqattempts": {}, "dlqconsumer": {}, "dlqerror": {}, "dlqtime": {},
	"data": {},
}

// requiredAttributes son los que, si faltan, hacen POISON al mensaje — 04-errors.md §1.3.
var requiredAttributes = []string{
	"specversion", "id", "source", "type", "time",
	"datacontenttype", "dataschema", "data",
}

// EnvelopeError señala un envelope que el SDK se niega a construir o serializar.
// A diferencia de PoisonError, se produce del lado del productor: es un bug local,
// no un mensaje ajeno corrupto.
type EnvelopeError struct {
	Message string
}

func (e *EnvelopeError) Error() string { return e.Message }

// ─── Construcción ────────────────────────────────────────────────────────────

// BuildEventInput reúne todo lo que hace falta para construir un evento.
//
// El desarrollador de la aplicación NO rellena esto: lo rellena Bus.Publish a partir
// de la configuración de Connect y del contexto del evento en curso. Se expone para
// tests y para quien construya un transporte propio — 01-envelope.md §5.
type BuildEventInput struct {
	// Subject es el subject de NATS. Determina `type`.
	Subject string
	// Data debe serializar a un OBJETO JSON en la raíz.
	Data any

	ID                 string
	Source             string
	ProducerVersion    string
	TenantID           string
	DataClassification DataClassification
	DataSchema         string
	CorrelationID      string

	// Time es el instante en que OCURRIÓ el hecho, no el de publicación. El cero de
	// time.Time significa "ahora".
	Time time.Time

	// AggregateID va al atributo `subject` de CloudEvents. Ver Event.AggregateID.
	AggregateID  string
	CausationID  string
	PartitionKey string
	TraceParent  string
	TraceState   string
}

// BuildEvent construye un evento válido a partir de la entrada.
//
// Valida lo que en TypeScript garantizaba el sistema de tipos y en Go no puede: el
// cero de un string es "" y es indistinguible de "ausente", así que las cuatro
// extensiones obligatorias se comprueban en tiempo de ejecución.
func BuildEvent(in BuildEventInput) (Event, error) {
	eventType, err := SubjectToType(in.Subject)
	if err != nil {
		return Event{}, err
	}

	// `data` DEBE ser un objeto JSON en la raíz. Ni array ni escalar: con un array o
	// un escalar arriba no se pueden añadir campos después sin romper el esquema, y
	// toda la política de compatibilidad de 05-compatibility.md se apoya en poder
	// hacerlo — 01-envelope.md §4.
	//
	// En Go la comprobación es "serializa y mira el primer byte" porque `any` no
	// dice nada sobre la FORMA JSON del valor: un map, un struct y un *struct son
	// tipos distintos que producen todos un objeto, y un []T o un string producen
	// algo que no lo es.
	raw, err := marshalDataObject(in.Data)
	if err != nil {
		return Event{}, err
	}

	for _, req := range []struct{ name, value string }{
		{"ID", in.ID},
		{"Source", in.Source},
		{"DataSchema", in.DataSchema},
		{"CorrelationID", in.CorrelationID},
		{"TenantID", in.TenantID},
		{"ProducerVersion", in.ProducerVersion},
	} {
		if req.value == "" {
			return Event{}, &EnvelopeError{Message: fmt.Sprintf(
				"%s es obligatorio y llegó vacío. Lo rellena el SDK a partir de ConnectOptions; "+
					"si estás llamando a BuildEvent a mano, rellénalo (01-envelope.md §5)", req.name)}
		}
	}
	if !in.DataClassification.IsValid() {
		return Event{}, &EnvelopeError{Message: fmt.Sprintf(
			"dataclassification %q no es válida: debe ser public, internal, confidential o restricted (06-security.md §5)",
			in.DataClassification)}
	}

	t := in.Time
	if t.IsZero() {
		t = time.Now()
	}

	// Por convención partitionkey = aggregateId, salvo que se pida otra cosa
	// explícitamente — 01-envelope.md §3.2.
	partitionKey := in.PartitionKey
	if partitionKey == "" {
		partitionKey = in.AggregateID
	}

	return Event{
		SpecVersion:        SpecVersion,
		ID:                 in.ID,
		Source:             in.Source,
		Type:               eventType,
		Time:               FormatTime(t),
		DataContentType:    DataContentType,
		DataSchema:         in.DataSchema,
		AggregateID:        in.AggregateID,
		CorrelationID:      in.CorrelationID,
		TenantID:           in.TenantID,
		ProducerVersion:    in.ProducerVersion,
		DataClassification: in.DataClassification,
		CausationID:        in.CausationID,
		PartitionKey:       partitionKey,
		TraceParent:        in.TraceParent,
		TraceState:         in.TraceState,
		Data:               raw,
	}, nil
}

// marshalDataObject serializa el payload y exige que sea un objeto JSON en la raíz.
func marshalDataObject(data any) (json.RawMessage, error) {
	if data == nil {
		return nil, &EnvelopeError{Message: "`data` es obligatorio y llegó nil"}
	}

	// Un json.RawMessage que ya viene del transporte se respeta tal cual: volver a
	// pasarlo por Marshal reordenaría claves y rompería la fidelidad del replay.
	var raw json.RawMessage
	if r, ok := data.(json.RawMessage); ok {
		raw = r
	} else {
		encoded, err := marshalNoHTMLEscape(data)
		if err != nil {
			return nil, &EnvelopeError{Message: "`data` no es serializable a JSON: " + err.Error()}
		}
		raw = encoded
	}

	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, &EnvelopeError{Message: "`data` debe ser un objeto JSON en la raíz (ni array ni escalar), " +
			"para poder añadir campos sin romper el contrato (01-envelope.md §4)"}
	}
	return raw, nil
}

// ─── Serialización ───────────────────────────────────────────────────────────

// Serialize convierte el evento en los bytes que viajan por NATS.
//
// Falla aquí y no en el broker al superar 1 MiB: el broker devuelve "maximum payload
// exceeded", que no dice qué hacer al respecto — 01-envelope.md §6.
func Serialize(e Event) ([]byte, error) {
	b, err := marshalNoHTMLEscape(e)
	if err != nil {
		return nil, &EnvelopeError{Message: "el evento no es serializable a JSON: " + err.Error()}
	}
	if len(b) > MaxMessageBytes {
		return nil, &EnvelopeError{Message: fmt.Sprintf(
			"evento de %d bytes supera el límite de %d (1 MiB). Aplica claim-check: sube el "+
				"contenido a almacenamiento de objetos y publica {uri, sha256, bytes} en su lugar "+
				"(01-envelope.md §6). El sha256 no es decorativo: sin él el consumidor no puede "+
				"distinguir \"aún no replicado\" de \"fue sustituido\"",
			len(b), MaxMessageBytes)}
	}
	return b, nil
}

// marshalNoHTMLEscape serializa sin escapar <, > ni &.
//
// encoding/json los sustituye por <, > y & por defecto, pensando en
// incrustar JSON dentro de HTML. Aquí eso solo produce un envelope distinto byte a
// byte del que emiten Node, Java y .NET para el mismo evento, lo que rompe cualquier
// comparación de fixtures entre SDKs y ensucia todo `nats stream view`.
func marshalNoHTMLEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	// Encode añade un salto de línea final que Marshal no añade.
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// ─── Parseo ──────────────────────────────────────────────────────────────────

// ParseEvent interpreta el cuerpo de un mensaje como CloudEvent de flux.
//
// Todo fallo aquí es POISON: el mensaje ni siquiera es interpretable, así que nunca
// llega al handler y va a la DLQ con alerta inmediata — 04-errors.md §1.3.
//
// El orden de las comprobaciones es el mismo que en el SDK de Node, para que los dos
// den el mismo `code` ante el mismo mensaje corrupto.
func ParseEvent(b []byte) (Event, error) {
	if !json.Valid(b) {
		return Event{}, NewPoisonError("el cuerpo del mensaje no es JSON válido",
			WithCode("MALFORMED_JSON"))
	}

	// Se decodifica primero a map[string]json.RawMessage y no directamente al struct
	// por dos razones:
	//
	//  1. Los atributos raíz son una lista CERRADA y hay que poder enumerar los
	//     desconocidos en el mensaje de error. Decoder.DisallowUnknownFields aborta
	//     en el primero y no dice cuáles son los demás.
	//  2. encoding/json empareja campos SIN distinguir mayúsculas: `{"ID": "..."}`
	//     poblaría Event.ID en silencio, y `Id`, `iD` y `ID` acabarían siendo el
	//     mismo atributo. Comparar contra AllowedRootAttributes con las claves
	//     exactas cierra ese agujero antes de decodificar.
	if trimmed := bytes.TrimLeft(b, " \t\r\n"); len(trimmed) == 0 || trimmed[0] != '{' {
		return Event{}, NewPoisonError("el cuerpo del mensaje no es un objeto JSON",
			WithCode("NOT_AN_OBJECT"))
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return Event{}, NewPoisonError("el cuerpo del mensaje no es un objeto JSON",
			WithCode("NOT_AN_OBJECT"), WithCause(err))
	}

	if sv := rawString(raw, "specversion"); sv != SpecVersion {
		return Event{}, NewPoisonError(fmt.Sprintf(
			"specversion no soportada: %s (se esperaba %q)", rawOrAbsent(raw, "specversion"), SpecVersion),
			WithCode("UNSUPPORTED_SPECVERSION"))
	}

	var missing []string
	for _, a := range requiredAttributes {
		if _, ok := raw[a]; !ok {
			missing = append(missing, a)
		}
	}
	if len(missing) > 0 {
		return Event{}, NewPoisonError(
			"faltan atributos obligatorios de CloudEvents: "+strings.Join(missing, ", "),
			WithCode("MISSING_REQUIRED_ATTRIBUTE"))
	}

	if ct := rawString(raw, "datacontenttype"); ct != DataContentType {
		return Event{}, NewPoisonError(fmt.Sprintf(
			"datacontenttype no soportado: %s", rawOrAbsent(raw, "datacontenttype")),
			WithCode("UNSUPPORTED_CONTENT_TYPE"))
	}

	var unknown []string
	for k := range raw {
		if _, ok := AllowedRootAttributes[k]; !ok {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) > 0 {
		// Estricto a propósito. Ver AllowedRootAttributes — 01-envelope.md §3.3.
		sort.Strings(unknown) // el recorrido de un map en Go no es determinista
		return Event{}, NewPoisonError(fmt.Sprintf(
			"atributos raíz desconocidos: %s. Todo metadato adicional va dentro de `data`",
			strings.Join(unknown, ", ")),
			WithCode("UNKNOWN_ROOT_ATTRIBUTE"))
	}

	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		// Llegar aquí significa que un atributo conocido tiene el tipo equivocado
		// (p. ej. `"dlqattempts": "seis"`). Sigue siendo un mensaje ininterpretable.
		return Event{}, NewPoisonError(
			"un atributo del envelope tiene un tipo incompatible con el contrato",
			WithCode("INVALID_ATTRIBUTE_TYPE"), WithCause(err))
	}
	return e, nil
}

// rawString extrae un atributo raíz como string, o "" si falta o no es string.
func rawString(raw map[string]json.RawMessage, key string) string {
	v, ok := raw[key]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		return ""
	}
	return s
}

// rawOrAbsent devuelve el JSON crudo del atributo para el mensaje de error, de modo
// que el operador vea exactamente qué llegó y no una interpretación.
func rawOrAbsent(raw map[string]json.RawMessage, key string) string {
	v, ok := raw[key]
	if !ok {
		return "<ausente>"
	}
	return string(v)
}

// UnmarshalData decodifica el payload del evento en el tipo de la aplicación.
//
// Data se guarda sin interpretar para preservar la fidelidad del replay, así que
// éste es el punto donde el evento se convierte en algo tipado:
//
//	type PedidoCreado struct {
//	    PedidoID         string `json:"pedidoId"`
//	    AggregateVersion int    `json:"aggregateVersion"`
//	    TotalCents       int64  `json:"totalCents"`
//	}
//	p, err := flux.UnmarshalData[PedidoCreado](evento)
//
// Un fallo aquí NO es POISON: el envelope era correcto y el mensaje llegó al handler.
// Es un desajuste de contrato entre productor y consumidor, es decir PERMANENT — por
// eso devuelve un PermanentError y no un PoisonError (04-errors.md §1.2).
func UnmarshalData[T any](e Event) (T, error) {
	var out T
	if err := json.Unmarshal(e.Data, &out); err != nil {
		return out, NewPermanentError(
			fmt.Sprintf("el payload del evento %s no encaja en el tipo esperado", e.ID),
			WithCode("DATA_SCHEMA_MISMATCH"), WithCause(err))
	}
	return out, nil
}

// ─── DLQ ─────────────────────────────────────────────────────────────────────

// DLQInfo describe por qué y cómo un evento acabó en la DLQ.
type DLQInfo struct {
	Reason   DLQReason
	Attempts int
	Consumer string
	Error    string
}

// ToDLQEvent prepara un evento para la DLQ: el CloudEvent original ÍNTEGRO más las
// extensiones dlq*. Sin wrapper.
//
// Sin wrapper porque así reproducir un mensaje de la DLQ es borrar las extensiones
// dlq* y republicar (`jq 'del(.dlq*)'`). Con un wrapper habría que desenvolver, y
// cada consumidor de la DLQ tendría que conocer dos formatos — 04-errors.md §3.
//
// Event se pasa y devuelve por valor: en Go eso ya es una copia, así que el evento
// original nunca se muta (el `...event` de Node hace lo mismo explícitamente).
func ToDLQEvent(e Event, info DLQInfo) Event {
	e.DLQReason = info.Reason
	e.DLQAttempts = info.Attempts
	e.DLQConsumer = info.Consumer
	e.DLQError = truncateRunes(info.Error, 1024)
	e.DLQTime = FormatTime(time.Now())
	return e
}

// StripDLQExtensions quita las extensiones dlq* para reproducir un evento.
//
// El `id` original se CONSERVA. Regenerarlo rompe la idempotencia de todos los
// consumidores aguas abajo y convierte una recuperación en un incidente nuevo
// — 04-errors.md §4.1.
func StripDLQExtensions(e Event) Event {
	e.DLQReason = ""
	e.DLQAttempts = 0
	e.DLQConsumer = ""
	e.DLQError = ""
	e.DLQTime = ""
	return e
}

// truncateRunes recorta a n bytes sin partir un carácter multibyte.
//
// El SDK de Node hace `.slice(0, 1024)` sobre unidades UTF-16 y aquí se corta por
// bytes UTF-8, así que el punto de corte de un mensaje con acentos puede diferir.
// Es intencionado: partir un rune produciría un JSON con U+FFFD en el `dlqerror`,
// que es peor que un mensaje unos bytes más corto.
func truncateRunes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
