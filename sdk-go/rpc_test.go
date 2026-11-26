// Petición/respuesta — lo que NO necesita un broker: los bytes del sobre, los nombres, los límites, las cinco comprobaciones de quien
// llama y la traducción de errores. Contrato normativo: specification/11-request-reply.md, 04-errors.md §7.
//
// Lo que sí lo necesita (la ACL, `no responders`, la parada ordenada, la concurrencia) está en rpc_integration_test.go.

package flux

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type casoRPC struct {
	Fixtures []struct {
		ID        string          `json:"id"`
		HarnessOp string          `json:"harnessOp"`
		Input     json.RawMessage `json:"input"`
		Bytes     int             `json:"bytes"`
		SHA256    string          `json:"sha256"`
		Base64    string          `json:"base64"`
	} `json:"fixtures"`
	SubjectVectors []struct {
		Operation   string `json:"operation"`
		NatsSubject string `json:"natsSubject"`
		ErrorCode   string `json:"errorCode"`
	} `json:"subjectVectors"`
	TimeoutVectors []struct {
		TimeoutMs   json.RawMessage `json:"timeoutMs"`
		EffectiveMs int64           `json:"effectiveMs"`
		ErrorCode   string          `json:"errorCode"`
	} `json:"timeoutVectors"`
}

func cargaCasoRPC(t *testing.T) casoRPC {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "conformance", "cases", "cross-sdk-rpc-envelope.json"))
	if err != nil {
		t.Fatal(err)
	}
	var c casoRPC
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

// entradaRPC es la entrada PLANA de un fixture, como la lee el arnés.
type entradaRPC struct {
	Operation       string `json:"operation"`
	TenantID        string `json:"tenantId"`
	AggregateID     string `json:"aggregateId"`
	ID              string `json:"id"`
	Source          string `json:"source"`
	Time            string `json:"time"`
	CorrelationID   string `json:"correlationid"`
	ProducerVersion string `json:"producerversion"`
	Classification  string `json:"classification"`
	CausationID     string `json:"causationid"`
	TraceParent     string `json:"traceparent"`
	TraceState      string `json:"tracestate"`
	Data            json.RawMessage
	Request         *struct {
		Operation     string `json:"operation"`
		ID            string `json:"id"`
		CorrelationID string `json:"correlationid"`
		TenantID      string `json:"tenantid"`
		Subject       string `json:"subject"`
	} `json:"request"`
	Error *struct {
		Class        string `json:"class"`
		Code         string `json:"code"`
		Message      string `json:"message"`
		RetryAfterMs *int64 `json:"retryAfterMs"`
	} `json:"error"`
}

func (e entradaRPC) ref() RPCRequestRef {
	return RPCRequestRef{Operation: e.Request.Operation, ID: e.Request.ID, CorrelationID: e.Request.CorrelationID,
		TenantID: e.Request.TenantID, Subject: e.Request.Subject}
}

// Los CUATRO ejemplos de la spec (§3.5) son byte a byte los de la spec: la petición, la respuesta, el error de un handler y el que
// fabrica el SDK. Es lo que hace que un servicio escrito en Go y uno escrito en Node se entiendan.
func TestRPCLosCuatroEjemplosDeLaSpecSonByteAByte(t *testing.T) {
	c := cargaCasoRPC(t)
	if len(c.Fixtures) != 4 {
		t.Fatalf("el caso trae %d fixtures, se esperaban 4", len(c.Fixtures))
	}
	longitudes := map[string]int{"rpc-request": 668, "rpc-reply": 703, "rpc-error-handler": 677, "rpc-error-sdk": 791}
	for _, f := range c.Fixtures {
		var in entradaRPC
		if err := json.Unmarshal(f.Input, &in); err != nil {
			t.Fatal(err)
		}
		var in2 struct{ Data json.RawMessage }
		_ = json.Unmarshal(f.Input, &in2)
		var (
			e   Event
			err error
		)
		switch f.HarnessOp {
		case "rpc-request":
			e, err = BuildRPCRequest(RPCRequestInput{Operation: in.Operation, TenantID: in.TenantID, AggregateID: in.AggregateID, ID: in.ID,
				Source: in.Source, Time: in.Time, CorrelationID: in.CorrelationID, CausationID: in.CausationID, ProducerVersion: in.ProducerVersion,
				Classification: DataClassification(in.Classification), Data: in2.Data, TraceParent: in.TraceParent, TraceState: in.TraceState})
		case "rpc-reply":
			e, err = BuildRPCReply(NewRPCReplyInput(in.ref(), in.ID, in.Source, in.Time, in.ProducerVersion, in.TraceParent,
				DataClassification(in.Classification), in2.Data, ""))
		case "rpc-error":
			e, err = BuildRPCError(NewRPCErrorInput(in.ref(), in.ID, in.Source, in.Time, in.ProducerVersion, in.TraceParent,
				RPCErrorData{Class: in.Error.Class, Code: in.Error.Code, Message: in.Error.Message, RetryAfterMs: in.Error.RetryAfterMs}))
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
		quiero, _ := base64.StdEncoding.DecodeString(f.Base64)
		if hex.EncodeToString(sum[:]) != f.SHA256 || string(b) != string(quiero) {
			t.Errorf("%s: los bytes no son los de la spec\n  tengo: %s\n  spec:  %s", f.ID, b, quiero)
		}
		if len(b) != longitudes[f.ID] || len(b) != f.Bytes {
			t.Errorf("%s: %d bytes, la spec dice %d (y la tabla %d)", f.ID, len(b), f.Bytes, longitudes[f.ID])
		}
		// `data` es el ÚLTIMO atributo (01-envelope.md §1): el orden es normativo.
		if i := strings.LastIndex(string(b), `,"data":`); i < 0 || strings.Contains(string(b)[i+8:], `"specversion"`) {
			t.Errorf("%s: `data` no es el último atributo", f.ID)
		}
	}
}

func TestRPCLosVectoresDeNombreYDeTimeout(t *testing.T) {
	c := cargaCasoRPC(t)
	if len(c.SubjectVectors) != 20 || len(c.TimeoutVectors) != 14 {
		t.Fatalf("el caso trae %d vectores de subject y %d de timeout", len(c.SubjectVectors), len(c.TimeoutVectors))
	}
	for _, v := range c.SubjectVectors {
		subject, err := RPCSubject(v.Operation)
		switch {
		case v.ErrorCode != "":
			if codigo(err) != v.ErrorCode {
				t.Errorf("%q: se esperaba %s y fue %v", v.Operation, v.ErrorCode, err)
			}
		case err != nil || subject != v.NatsSubject:
			t.Errorf("%q: %q, %v (se esperaba %q)", v.Operation, subject, err, v.NatsSubject)
		}
	}
	for _, v := range c.TimeoutVectors {
		ms := RPCTimeoutDefault
		if v.TimeoutMs != nil {
			var f float64
			if json.Unmarshal(v.TimeoutMs, &f) != nil || string(v.TimeoutMs) == "null" {
				// Una cadena, un booleano, un array, un objeto o `null`: el tipo de la API (`time.Duration`) NO los admite —es un error
				// de compilación—, y lo que el arnés cruzado comprueba de ellos es la entrada JSON, no esta API.
				if v.ErrorCode != "INVALID_RPC_CALL" {
					t.Errorf("%s: un valor que no es un número debe ser INVALID_RPC_CALL", v.TimeoutMs)
				}
				continue
			}
			if f > 1e9 { // 1e21 ms no cabe en un time.Duration: basta con uno fuera de rango
				f = 1e9
			}
			ms = time.Duration(f * float64(time.Millisecond))
		}
		d, err := ResolveRPCTimeout(ms)
		switch {
		case v.ErrorCode != "":
			if codigo(err) != v.ErrorCode {
				t.Errorf("%s: se esperaba %s y fue %v", v.TimeoutMs, v.ErrorCode, err)
			}
		case err != nil || d.Milliseconds() != v.EffectiveMs:
			t.Errorf("%s: %v, %v (se esperaban %d ms)", v.TimeoutMs, d, err, v.EffectiveMs)
		}
	}
}

// Los códigos de §7 de 04-errors.md, con su clase: lo que agrupa el panel y lo que decide si el handler que llama reintenta.
func TestRPCLosCodigosConSuClase(t *testing.T) {
	casos := []struct {
		err    error
		codigo string
		clase  ErrorClass
	}{
		{&RPCUnavailableError{baseError{CodeRPCUnavailable, ClassRetryable, nil}, "a.b.v1.c"}, "RPC_UNAVAILABLE", ClassRetryable},
		{&RPCTimeoutError{baseError{CodeRPCTimeout, ClassRetryable, nil}, "a.b.v1.c", time.Second}, "RPC_TIMEOUT", ClassRetryable},
		{&StaleRequestError{baseError{CodeStaleRequest, ClassPermanent, nil}, "x", 300, "y"}, "STALE_REQUEST", ClassPermanent},
		{&RPCInvalidReplyError{baseError{CodeRPCInvalidReply, ClassPermanent, nil}, "a.b.v1.c", "x", false}, "RPC_INVALID_REPLY", ClassPermanent},
		{&RPCMessageTooLargeError{baseError{CodeRPCMessageTooLarge, ClassPermanent, nil}, 2, 1, "a.b.v1.c", "petición"}, "RPC_MESSAGE_TOO_LARGE", ClassPermanent},
		{invalidRPCCall("x", nil), "INVALID_RPC_CALL", ClassPermanent},
		{&PermissionDeniedError{baseError: baseError{CodePermissionDenied, ClassPermanent, nil}}, "PERMISSION_DENIED", ClassPermanent},
		{&SchemaValidationError{}, "INVALID_SCHEMA", ClassPermanent},
		{newRPCRemoteError("a.b.v1.c", "retryable", "HTTP_503", "x", time.Second), "HTTP_503", ClassRetryable},
		{newRPCRemoteError("a.b.v1.c", "permanent", "NO", "x", 0), "NO", ClassPermanent},
	}
	for _, c := range casos {
		ce, ok := AsClassified(errors.Join(errors.New("envuelto"), c.err))
		if !ok || ce.FluxCode() != c.codigo || ce.FluxClass() != c.clase {
			t.Errorf("%T: %v / %v (se esperaba %s %s)", c.err, ce, ok, c.codigo, c.clase)
		}
	}
	// Todos dicen QUÉ está mal y CÓMO se arregla, y nunca citan `data`.
	for _, c := range casos[:6] {
		if len(c.err.Error()) < 40 {
			t.Errorf("%T: el mensaje no dice nada: %q", c.err, c.err.Error())
		}
	}
}

// El orden de §4.1, paso 0: el primero que falla gana, y todo sin red.
func TestRPCElOrdenDeValidacionDeUnaPeticion(t *testing.T) {
	buena := RPCRequestInput{Operation: opCalcular, TenantID: "acme", ID: "id-1", Source: "/x/y", Time: "2026-08-20T10:41:07.318Z",
		CorrelationID: "id-1", ProducerVersion: "1", Classification: ClassificationInternal, Data: map[string]any{"a": 1}}
	casos := []struct {
		nombre string
		mut    func(*RPCRequestInput)
		codigo string
	}{
		{"todo bien", func(*RPCRequestInput) {}, ""},
		{"dominio reservado ANTES que la forma", func(i *RPCRequestInput) { i.Operation = "rpc.Mal"; i.Data = "x"; i.Classification = "z" }, "RESERVED_DOMAIN"},
		{"la forma antes que data", func(i *RPCRequestInput) { i.Operation = "Mal"; i.Data = "x"; i.Classification = "z" }, "INVALID_RPC_CALL"},
		{"data antes que la clasificación", func(i *RPCRequestInput) { i.Data = []int{1}; i.Classification = "z" }, "INVALID_RPC_CALL"},
		{"clasificación antes que los atributos", func(i *RPCRequestInput) { i.Classification = "z"; i.ID = "" }, "INVALID_DATACLASSIFICATION"},
		{"id vacío", func(i *RPCRequestInput) { i.ID = "" }, "INVALID_RPC_CALL"},
		{"tenant vacío", func(i *RPCRequestInput) { i.TenantID = "" }, "INVALID_RPC_CALL"},
		{"time que no es un instante", func(i *RPCRequestInput) { i.Time = "ayer" }, "INVALID_RPC_CALL"},
		{"data null", func(i *RPCRequestInput) { i.Data = nil }, "INVALID_RPC_CALL"},
	}
	for _, c := range casos {
		in := buena
		c.mut(&in)
		_, err := BuildRPCRequest(in)
		if codigo(err) != c.codigo {
			t.Errorf("%s: %v (se esperaba %q)", c.nombre, err, c.codigo)
		}
	}
	// `time` se NORMALIZA a la forma canónica; sin aggregateId no hay `subject` ni `partitionkey` (ni `null`).
	in := buena
	in.Time = "2026-08-20T12:41:07+02:00"
	e, err := BuildRPCRequest(in)
	if err != nil || e.Time != "2026-08-20T10:41:07.000Z" {
		t.Errorf("time canónico: %q %v", e.Time, err)
	}
	if b, _ := Serialize(e); strings.Contains(string(b), `"subject"`) || strings.Contains(string(b), `"partitionkey"`) || strings.Contains(string(b), "null") {
		t.Errorf("sin aggregateId no hay subject ni partitionkey: %s", b)
	}
}

// Un error que no cumple su esquema es INVALID_RPC_CALL (un código estable, nunca una excepción sin código), y `retryAfterMs: 0` SE ESCRIBE.
func TestRPCElErrorValidaSuEsquemaYEscribeElCero(t *testing.T) {
	ref := RPCRequestRef{Operation: opCalcular, ID: "i", CorrelationID: "c", TenantID: "acme"}
	mil := strings.Repeat("😀", 1000)
	cero, menos, tres := int64(0), int64(-1), int64(3)
	casos := []struct {
		nombre string
		d      RPCErrorData
		ok     bool
	}{
		{"retryable", RPCErrorData{Class: "retryable", Code: "HTTP_503", Message: "m"}, true},
		{"retryAfterMs 0", RPCErrorData{Class: "retryable", Code: "X", Message: "m", RetryAfterMs: &cero}, true},
		{"mil emojis caben (puntos de código, no unidades UTF-16 ni bytes)", RPCErrorData{Class: "permanent", Code: "X", Message: mil}, true},
		{"mil y uno no", RPCErrorData{Class: "permanent", Code: "X", Message: mil + "😀"}, false},
		{"poison no viaja", RPCErrorData{Class: "poison", Code: "X", Message: "m"}, false},
		{"código en minúsculas", RPCErrorData{Class: "permanent", Code: "mal", Message: "m"}, false},
		{"código de 65", RPCErrorData{Class: "permanent", Code: "A" + strings.Repeat("B", 64), Message: "m"}, false},
		{"mensaje vacío", RPCErrorData{Class: "permanent", Code: "X", Message: ""}, false},
		{"retryAfterMs negativo", RPCErrorData{Class: "retryable", Code: "X", Message: "m", RetryAfterMs: &menos}, false},
		{"retryAfterMs con permanent", RPCErrorData{Class: "permanent", Code: "X", Message: "m", RetryAfterMs: &tres}, false},
	}
	for _, c := range casos {
		e, err := BuildRPCError(NewRPCErrorInput(ref, "id-2", "/x/y", "2026-08-20T10:41:07.331Z", "1", "", c.d))
		if c.ok != (err == nil) || (err != nil && codigo(err) != "INVALID_RPC_CALL") {
			t.Errorf("%s: %v", c.nombre, err)
		}
		if err == nil && c.d.RetryAfterMs != nil && *c.d.RetryAfterMs == 0 && !strings.Contains(string(e.Data), `"retryAfterMs":0}`) {
			t.Errorf("`retryAfterMs: 0` se escribe: %s", e.Data)
		}
		if err == nil && (e.Type != RPCErrorType || e.DataSchema != RPCErrorDataSchema || e.DataClassification != ClassificationInternal) {
			t.Errorf("%s: type, dataschema y clasificación del error son FIJOS: %+v", c.nombre, e)
		}
	}
}

// Las cinco comprobaciones de §4.3, sin red.
func TestRPCLasCincoComprobacionesDeQuienLlama(t *testing.T) {
	c := &rpcClient{bus: &Bus{}}
	peticion, err := BuildRPCRequest(RPCRequestInput{Operation: opObtener, TenantID: "acme", ID: "id-peticion", Source: "/x/y",
		Time: "2026-08-20T10:41:07.318Z", CorrelationID: "corr-1", ProducerVersion: "1", Classification: ClassificationInternal,
		Data: map[string]any{"clienteId": "cli-1"}})
	if err != nil {
		t.Fatal(err)
	}
	p := &preparedRequest{operation: opObtener, event: peticion}
	ref := refOf(opObtener, peticion)
	respuesta := func(mut func(*Event)) []byte {
		e, err := BuildRPCReply(NewRPCReplyInput(ref, "id-resp", "/x/y", "2026-08-20T10:41:07.329Z", "1", "", ClassificationInternal,
			map[string]any{"clienteId": "cli-1", "nombre": "A", "email": "a@b.c"}, ""))
		if err != nil {
			t.Fatal(err)
		}
		if mut != nil {
			mut(&e)
		}
		b, _ := Serialize(e)
		return b
	}
	error_ := func(d RPCErrorData, mut func(*Event)) []byte {
		e, err := BuildRPCError(NewRPCErrorInput(ref, "id-err", "/x/y", "2026-08-20T10:41:07.329Z", "1", "", d))
		if err != nil {
			t.Fatal(err)
		}
		if mut != nil {
			mut(&e)
		}
		b, _ := Serialize(e)
		return b
	}
	buenError := RPCErrorData{Class: "permanent", Code: "NO", Message: "no"}

	if _, esError, err := c.checkReply(p, respuesta(nil)); err != nil || esError {
		t.Fatalf("la respuesta buena: %v %v", esError, err)
	}
	if _, esError, err := c.checkReply(p, error_(buenError, nil)); err != nil || !esError {
		t.Fatalf("el error bueno: %v %v", esError, err)
	}
	malos := []struct {
		nombre string
		raw    []byte
	}{
		{"1. no es un CloudEvent", []byte("hola")},
		{"1. sin extensiones", []byte(`{"specversion":"1.0","id":"x"}`)},
		{"2. type de otra operación", respuesta(func(e *Event) { e.Type = "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1" })},
		{"2. el type de la petición", respuesta(func(e *Event) { e.Type = peticion.Type })},
		{"3. otro causationid", respuesta(func(e *Event) { e.CausationID = "otra" })},
		{"3. sin causationid", respuesta(func(e *Event) { e.CausationID = "" })},
		{"4. otro correlationid", respuesta(func(e *Event) { e.CorrelationID = "otra" })},
		{"4. otro tenantid", respuesta(func(e *Event) { e.TenantID = "globex" })},
		{"4. un error con otro correlationid", error_(buenError, func(e *Event) { e.CorrelationID = "otra" })},
		{"4. tenant system pero correlationid que no es su id", error_(buenError, func(e *Event) { e.TenantID = "system" })},
		{"4. correlationid igual a su id pero con tenant de otro", error_(buenError, func(e *Event) { e.CorrelationID = e.ID; e.TenantID = "acme" })},
		{"5. un error con class poison", error_(buenError, func(e *Event) { e.Data = []byte(`{"class":"poison","code":"NO","message":"no"}`) })},
		{"5. un error sin message", error_(buenError, func(e *Event) { e.Data = []byte(`{"class":"permanent","code":"NO"}`) })},
		{"5. un error con un atributo de más", error_(buenError, func(e *Event) { e.Data = []byte(`{"class":"permanent","code":"NO","message":"m","x":1}`) })},
		{"5. un error con retryAfterMs decimal", error_(buenError, func(e *Event) {
			e.Data = []byte(`{"class":"retryable","code":"NO","message":"m","retryAfterMs":1.5}`)
		})},
		{"5. un error con data que no es un objeto", error_(buenError, func(e *Event) { e.Data = []byte(`[1]`) })},
	}
	for _, m := range malos {
		_, _, err := c.checkReply(p, m.raw)
		if codigo(err) != "RPC_INVALID_REPLY" || claseDe(err) != ClassPermanent {
			t.Errorf("%s: %v", m.nombre, err)
		}
	}
	// LA EXCEPCIÓN ÚNICA (§4.3.4): tenantid `system` Y correlationid = su propio id Y un causationid que es el `id` de la petición.
	excepcion := error_(RPCErrorData{Class: "permanent", Code: "MALFORMED_JSON", Message: "no se pudo leer"}, func(e *Event) {
		e.TenantID, e.CorrelationID = "system", e.ID
	})
	if _, esError, err := c.checkReply(p, excepcion); err != nil || !esError {
		t.Errorf("el error de una petición ilegible: %v %v", esError, err)
	}
	// Pero NO sin la comprobación 3: sin su causationid no se atribuye a esta petición.
	sinAtribuir := error_(buenError, func(e *Event) { e.TenantID, e.CorrelationID, e.CausationID = "system", e.ID, "otra" })
	if _, _, err := c.checkReply(p, sinAtribuir); codigo(err) != "RPC_INVALID_REPLY" {
		t.Errorf("la excepción no exime de la comprobación 3: %v", err)
	}
	// Y no vale para una RESPUESTA (solo para un error).
	respuestaSystem := respuesta(func(e *Event) { e.TenantID, e.CorrelationID = "system", e.ID })
	if _, _, err := c.checkReply(p, respuestaSystem); codigo(err) != "RPC_INVALID_REPLY" {
		t.Errorf("la excepción es solo del error: %v", err)
	}
	// El acuse de un stream se reconoce y lo dice.
	_, _, err = c.checkReply(p, []byte(`{"stream":"EVT_X","seq":42}`))
	if err == nil || !strings.Contains(err.Error(), "flux doctor") {
		t.Errorf("el acuse de un stream apunta a `flux doctor`: %v", err)
	}
	// El mensaje no cita lo que llegó (puede ser hostil o llevar datos).
	_, _, err = c.checkReply(p, respuesta(func(e *Event) { e.CausationID = "SECRETO-123" }))
	if err == nil || strings.Contains(err.Error(), "SECRETO-123") && len(err.Error()) > 700 {
		t.Errorf("el mensaje cita lo que llegó sin acotarlo: %v", err)
	}
}

// El `id` de una petición ilegible se cita como `causationid` SOLO si es una cadena válida: lo que quien llama no reconoce como suyo, o
// que llena un log, no se copia (§5.2).
func TestRPCIdLegibleDeUnaPeticionQueNoSePudoLeer(t *testing.T) {
	largo := strings.Repeat("a", 129)
	casos := map[string]string{
		`{"id":"abc-123","x":1}`: "abc-123",
		`{"id":""}`:              "",
		`{"id":5}`:               "",
		`{"id":"con\nsalto"}`:    "",
		`{"id":"` + largo + `"}`: "",
		`{"id":"` + strings.Repeat("😀", 128) + `"}`: strings.Repeat("😀", 128),
		`no es json`:   "",
		`[1,2]`:        "",
		`{"sin":"id"}`: "",
	}
	for raw, quiero := range casos {
		if got := idLegible([]byte(raw)); got != quiero {
			t.Errorf("%.40q: %q (se esperaba %q)", raw, got, quiero)
		}
	}
}

// La traducción del error de un handler (§5.3): su clase, SU código y SU mensaje si es de flux; el del clasificador con un mensaje
// GENÉRICO si lo reconoce; y `permanent` UNKNOWN con un mensaje genérico si no — su texto puede llevar valores de `data`.
type error503 struct{}

func (error503) Error() string {
	return "503 Service Unavailable: https://interno.local/tarifas?cliente=ana@example.com"
}
func (error503) HTTPStatus() int { return 503 }

func TestRPCLaTraduccionDelErrorDelHandler(t *testing.T) {
	b := &Bus{classify: NewClassifier(ClassifierOptions{})}
	c := &rpcClient{bus: b}
	ev := Event{ID: "id-1", DataClassification: ClassificationInternal}
	casos := []struct {
		nombre string
		err    error
		quiero RPCErrorData
	}{
		{"permanent propio", NewPermanentError("no existe", WithCode("NO_EXISTE")), RPCErrorData{Class: "permanent", Code: "NO_EXISTE", Message: "no existe"}},
		{"retryable propio", NewRetryableError("luego", WithCode("LUEGO")), RPCErrorData{Class: "retryable", Code: "LUEGO", Message: "luego"}},
		{"sin código: el de la clase", NewPermanentError("x"), RPCErrorData{Class: "permanent", Code: "PERMANENT", Message: "x"}},
		{"código inválido: el de la clase", NewRetryableError("x", WithCode("mal")), RPCErrorData{Class: "retryable", Code: "RETRYABLE", Message: "x"}},
		{"poison viaja como permanent", NewPoisonError("x", WithCode("RARO")), RPCErrorData{Class: "permanent", Code: "RARO", Message: "x"}},
		{"mensaje en blanco", NewPermanentError("   ", WithCode("NO")), RPCErrorData{Class: "permanent", Code: "NO", Message: "el servicio rechazó la petición"}},
		{"desconocido", errors.New("pq: duplicate key (email)=(ana@example.com)"), RPCErrorData{Class: "permanent", Code: "UNKNOWN", Message: "error interno del servicio"}},
	}
	for _, c0 := range casos {
		got := c.errorOf(opCalcular, ev, c0.err)
		if got.Class != c0.quiero.Class || got.Code != c0.quiero.Code || got.Message != c0.quiero.Message || got.RetryAfterMs != nil {
			t.Errorf("%s: %+v (se esperaba %+v)", c0.nombre, got, c0.quiero)
		}
	}
	// RetryAfter de un error retryable viaja en milisegundos.
	got := c.errorOf(opCalcular, ev, NewRetryableError("x", WithCode("LUEGO"), WithRetryAfter(1500*time.Millisecond)))
	if got.RetryAfterMs == nil || *got.RetryAfterMs != 1500 {
		t.Errorf("retryAfterMs: %+v", got.RetryAfterMs)
	}
	// Uno que el clasificador reconoce: su clase, su código y un mensaje GENÉRICO (el texto lleva una URL y un email).
	got = c.errorOf(opCalcular, ev, error503{})
	if got.Class != "retryable" || strings.Contains(got.Message, "ana@example.com") || strings.Contains(got.Message, "interno.local") {
		t.Errorf("un 503: %+v", got)
	}
	// Un mensaje de más de 1000 caracteres se acota en puntos de código.
	got = c.errorOf(opCalcular, ev, NewPermanentError(strings.Repeat("😀", 1500), WithCode("NO")))
	if n := codePointLength(got.Message); n != 1000 || !strings.HasSuffix(got.Message, "…") {
		t.Errorf("mensaje acotado: %d puntos de código", n)
	}
	if rpcErrorDataProblem(got) != "" {
		t.Errorf("lo que se contesta siempre cumple el esquema del error: %s", rpcErrorDataProblem(got))
	}
}

// Las métricas de rpc son del InMemoryMetrics y una extensión OPCIONAL de MetricsSink: un sink escrito antes no las tiene y compila.
func TestRPCLasMetricasSonOpcionales(t *testing.T) {
	var _ MetricsSink = NoMetrics{}
	if _, ok := MetricsSink(NoMetrics{}).(RPCMetricsSink); ok {
		t.Error("NoMetrics no tiene métricas de rpc: la extensión es opcional")
	}
	m := NewInMemoryMetrics()
	var sink MetricsSink = m
	r, ok := sink.(RPCMetricsSink)
	if !ok {
		t.Fatal("InMemoryMetrics implementa RPCMetricsSink")
	}
	r.RPCClientRequest(opCalcular, "ok")
	r.RPCServerRequest(opCalcular, "poison")
	r.RPCDuration(opCalcular, "client", 0.02)
	out := m.Render()
	for _, linea := range []string{
		"# TYPE flux_rpc_client_requests_total counter",
		`flux_rpc_client_requests_total{operation="` + opCalcular + `",outcome="ok"} 1`,
		`flux_rpc_server_requests_total{operation="` + opCalcular + `",outcome="poison"} 1`,
		"# TYPE flux_rpc_duration_seconds histogram",
		`flux_rpc_duration_seconds_bucket{operation="` + opCalcular + `",role="client",le="0.025"} 1`,
		`flux_rpc_duration_seconds_count{operation="` + opCalcular + `",role="client"} 1`,
	} {
		if !strings.Contains(out, linea) {
			t.Errorf("falta %q en:\n%s", linea, out)
		}
	}
}
