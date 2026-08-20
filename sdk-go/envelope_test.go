package flux

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// entradaValida es la base de los tests de construcción: lo que Bus.Publish arma a
// partir de ConnectOptions más el contexto del evento en curso.
func entradaValida() BuildEventInput {
	return BuildEventInput{
		Subject:            "pedidos.pedido.v1.creado",
		Data:               map[string]any{"pedidoId": "ped-123", "totalCents": 9990},
		ID:                 "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
		Source:             "/produccion/pedidos-api",
		ProducerVersion:    "3.4.1",
		TenantID:           "acme",
		DataClassification: ClassificationConfidential,
		DataSchema:         "https://schemas.internal/pedidos/pedido/creado/1.2.0.json",
		CorrelationID:      "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
		Time:               time.Date(2026, 8, 20, 10, 25, 39, 412_000_000, time.UTC),
	}
}

func TestBuildEventRellenaLoDerivable(t *testing.T) {
	in := entradaValida()
	in.AggregateID = "ped-123"

	e, err := BuildEvent(in)
	if err != nil {
		t.Fatal(err)
	}

	if e.SpecVersion != "1.0" {
		t.Errorf("specversion = %q", e.SpecVersion)
	}
	if e.DataContentType != "application/json" {
		t.Errorf("datacontenttype = %q", e.DataContentType)
	}
	// El `type` se DERIVA del subject; nunca se pide al desarrollador — 02-naming.md §2.
	if want := "com.flux.pedidos.pedido.creado.v1"; e.Type != want {
		t.Errorf("type = %q, se esperaba %q", e.Type, want)
	}
	// El AggregateID va al atributo `subject` de CloudEvents, no al subject de NATS.
	if e.AggregateID != "ped-123" {
		t.Errorf("AggregateID = %q", e.AggregateID)
	}
	// partitionkey = aggregateId por convención — 01-envelope.md §3.2.
	if e.PartitionKey != "ped-123" {
		t.Errorf("partitionkey = %q, se esperaba que heredase el aggregateId", e.PartitionKey)
	}
	// RFC 3339 UTC con EXACTAMENTE 3 decimales.
	if want := "2026-08-20T10:25:39.412Z"; e.Time != want {
		t.Errorf("time = %q, se esperaba %q", e.Time, want)
	}
}

func TestFormatTimeSiempreTresDecimales(t *testing.T) {
	// time.RFC3339Nano recortaría los ceros finales y el envelope dejaría de ser
	// byte a byte igual al de Node/.NET.
	casos := map[time.Time]string{
		time.Date(2026, 8, 20, 10, 25, 39, 410_000_000, time.UTC): "2026-08-20T10:25:39.410Z",
		time.Date(2026, 8, 20, 10, 25, 39, 0, time.UTC):           "2026-08-20T10:25:39.000Z",
		// Un instante con offset se normaliza a UTC.
		time.Date(2026, 8, 20, 12, 25, 39, 412_000_000,
			time.FixedZone("CEST", 2*3600)): "2026-08-20T10:25:39.412Z",
	}
	for in, want := range casos {
		if got := FormatTime(in); got != want {
			t.Errorf("FormatTime(%v) = %q, se esperaba %q", in, got, want)
		}
	}
}

func TestBuildEventExigeObjetoEnLaRaizDeData(t *testing.T) {
	// Un array o un escalar arriba impide añadir campos después sin romper el
	// esquema, y toda la política de compatibilidad depende de poder hacerlo
	// — 01-envelope.md §4.
	for _, data := range []any{
		[]int{1, 2, 3},
		"una cadena",
		42,
		json.RawMessage(`[{"a":1}]`),
	} {
		in := entradaValida()
		in.Data = data
		if _, err := BuildEvent(in); err == nil {
			t.Errorf("se esperaba error para data = %#v", data)
		} else if !strings.Contains(err.Error(), "objeto JSON en la raíz") {
			t.Errorf("el error debe explicar el porqué, se obtuvo: %v", err)
		}
	}

	// Un struct de Go sí produce un objeto: es el caso normal en un servicio real.
	in := entradaValida()
	in.Data = struct {
		PedidoID string `json:"pedidoId"`
	}{"ped-123"}
	if _, err := BuildEvent(in); err != nil {
		t.Errorf("un struct debería aceptarse: %v", err)
	}
}

func TestBuildEventExigeLasExtensionesObligatorias(t *testing.T) {
	// En TypeScript esto lo garantizaba el sistema de tipos; en Go el cero de un
	// string es "" y es indistinguible de "ausente", así que se valida en runtime.
	for _, campo := range []string{"ID", "CorrelationID", "TenantID", "ProducerVersion", "DataSchema"} {
		in := entradaValida()
		switch campo {
		case "ID":
			in.ID = ""
		case "CorrelationID":
			in.CorrelationID = ""
		case "TenantID":
			in.TenantID = ""
		case "ProducerVersion":
			in.ProducerVersion = ""
		case "DataSchema":
			in.DataSchema = ""
		}
		if _, err := BuildEvent(in); err == nil {
			t.Errorf("se esperaba error al omitir %s", campo)
		}
	}

	in := entradaValida()
	in.DataClassification = "secreto"
	if _, err := BuildEvent(in); err == nil {
		t.Error("se esperaba error para una dataclassification fuera del enum")
	}
}

func TestSerializeUsaLosNombresDeExtensionDeCloudEvents(t *testing.T) {
	in := entradaValida()
	in.AggregateID = "ped-123"
	in.CausationID = "01924f8d-1a2b-7c3d-8e4f-5a6b7c8d9e01"
	in.TraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	e, err := BuildEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Serialize(e)
	if err != nil {
		t.Fatal(err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}

	// CloudEvents solo admite [a-z0-9] en nombres de extensión: minúscula pegada.
	for _, k := range []string{"correlationid", "tenantid", "producerversion",
		"dataclassification", "causationid", "partitionkey", "traceparent"} {
		if _, ok := raw[k]; !ok {
			t.Errorf("falta la extensión %q en el JSON serializado", k)
		}
	}
	// El aggregateId se serializa como `subject`, no como `aggregateid`.
	if _, ok := raw["aggregateid"]; ok {
		t.Error("el AggregateID no debe serializarse como `aggregateid`")
	}
	if string(raw["subject"]) != `"ped-123"` {
		t.Errorf("subject = %s, se esperaba \"ped-123\"", raw["subject"])
	}
	// Todo atributo emitido debe estar en la lista cerrada.
	for k := range raw {
		if _, ok := AllowedRootAttributes[k]; !ok {
			t.Errorf("el SDK emitió un atributo raíz no permitido: %q", k)
		}
	}
}

func TestSerializeNoEscapaHTML(t *testing.T) {
	// encoding/json convierte < > & en < > & por defecto y el envelope
	// dejaría de ser byte a byte igual al de los demás SDKs.
	in := entradaValida()
	in.Data = map[string]any{"nota": "a < b && c > d"}
	e, err := BuildEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Serialize(e)
	if err != nil {
		t.Fatal(err)
	}
	// Si encoding/json hubiese escapado, los caracteres literales NO estarían en los
	// bytes: habrían sido sustituidos por sus secuencias <, > y &.
	// Comprobar que sobreviven literales es por tanto la aserción completa.
	if !strings.Contains(string(b), `a < b && c > d`) {
		t.Errorf("el JSON escapó HTML en vez de dejar los caracteres literales: %s", b)
	}
	// Y aun escapado seguiría siendo JSON válido, así que el round-trip no bastaría
	// por sí solo — pero confirma que no se rompió nada por el camino.
	parsed, err := ParseEvent(b)
	if err != nil {
		t.Fatal(err)
	}
	nota, err := UnmarshalData[struct {
		Nota string `json:"nota"`
	}](parsed)
	if err != nil {
		t.Fatal(err)
	}
	if nota.Nota != `a < b && c > d` {
		t.Errorf("nota = %q", nota.Nota)
	}
}

func TestSerializeRechazaMasDeUnMiB(t *testing.T) {
	in := entradaValida()
	in.Data = map[string]any{"blob": strings.Repeat("x", MaxMessageBytes)}
	e, err := BuildEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Serialize(e)
	if err == nil {
		t.Fatal("se esperaba error por superar 1 MiB")
	}
	// El error debe ser accionable: fallar aquí en vez de en el broker solo sirve si
	// el mensaje dice qué hacer — 01-envelope.md §6.
	if !strings.Contains(err.Error(), "claim-check") {
		t.Errorf("el error debe mencionar claim-check, se obtuvo: %v", err)
	}
}

// ─── Parseo ──────────────────────────────────────────────────────────────────

const envelopeValido = `{
  "specversion": "1.0",
  "id": "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  "source": "/produccion/pedidos-api",
  "type": "com.flux.pedidos.pedido.creado.v1",
  "subject": "ped-123",
  "time": "2026-08-20T10:25:39.412Z",
  "datacontenttype": "application/json",
  "dataschema": "https://schemas.internal/pedidos/pedido/creado/1.2.0.json",
  "correlationid": "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  "tenantid": "acme",
  "producerversion": "3.4.1",
  "dataclassification": "confidential",
  "data": { "pedidoId": "ped-123", "totalCents": 9990 }
}`

func TestParseEventValido(t *testing.T) {
	e, err := ParseEvent([]byte(envelopeValido))
	if err != nil {
		t.Fatal(err)
	}
	if e.ID != "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55" {
		t.Errorf("id = %q", e.ID)
	}
	if e.AggregateID != "ped-123" {
		t.Errorf("el atributo `subject` debe mapear a AggregateID, se obtuvo %q", e.AggregateID)
	}
	if e.DataClassification != ClassificationConfidential {
		t.Errorf("dataclassification = %q", e.DataClassification)
	}

	tipo, err := e.ParsedType()
	if err != nil {
		t.Fatal(err)
	}
	if tipo.Subject() != "pedidos.pedido.v1.creado" {
		t.Errorf("ParsedType no reconstruye el subject: %q", tipo.Subject())
	}
}

func TestUnmarshalData(t *testing.T) {
	e, err := ParseEvent([]byte(envelopeValido))
	if err != nil {
		t.Fatal(err)
	}
	type PedidoCreado struct {
		PedidoID   string `json:"pedidoId"`
		TotalCents int64  `json:"totalCents"`
	}
	p, err := UnmarshalData[PedidoCreado](e)
	if err != nil {
		t.Fatal(err)
	}
	if p.PedidoID != "ped-123" || p.TotalCents != 9990 {
		t.Errorf("payload mal decodificado: %+v", p)
	}

	// Un desajuste de tipo es PERMANENT, no POISON: el envelope era correcto.
	type Incompatible struct {
		TotalCents string `json:"totalCents"`
	}
	if _, err := UnmarshalData[Incompatible](e); err == nil {
		t.Fatal("se esperaba error de decodificación")
	} else if c, ok := AsClassified(err); !ok || c.FluxClass() != ClassPermanent {
		t.Errorf("un desajuste de payload debe ser PERMANENT, se obtuvo %v", err)
	}
}

func TestParseEventDetectaPoison(t *testing.T) {
	casos := []struct {
		nombre string
		cuerpo string
		code   string
	}{
		{"json malformado", `{"specversion":`, "MALFORMED_JSON"},
		{"no es objeto", `[1,2,3]`, "NOT_AN_OBJECT"},
		{"escalar", `"hola"`, "NOT_AN_OBJECT"},
		{"specversion ausente", `{"id":"x","data":{}}`, "UNSUPPORTED_SPECVERSION"},
		{"specversion desconocida", `{"specversion":"0.3","id":"x","data":{}}`, "UNSUPPORTED_SPECVERSION"},
		{"falta atributo obligatorio", `{"specversion":"1.0","id":"x","data":{}}`, "MISSING_REQUIRED_ATTRIBUTE"},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			_, err := ParseEvent([]byte(c.cuerpo))
			if err == nil {
				t.Fatal("se esperaba POISON")
			}
			var pe *PoisonError
			if !errors.As(err, &pe) {
				t.Fatalf("se esperaba *PoisonError, se obtuvo %T", err)
			}
			if pe.Code != c.code {
				t.Errorf("code = %q, se esperaba %q", pe.Code, c.code)
			}
			if pe.FluxClass() != ClassPoison {
				t.Errorf("clase = %q", pe.FluxClass())
			}
		})
	}
}

func TestParseEventRechazaContentTypeNoSoportado(t *testing.T) {
	cuerpo := strings.Replace(envelopeValido,
		`"datacontenttype": "application/json"`,
		`"datacontenttype": "application/avro"`, 1)
	_, err := ParseEvent([]byte(cuerpo))
	var pe *PoisonError
	if !errors.As(err, &pe) || pe.Code != "UNSUPPORTED_CONTENT_TYPE" {
		t.Fatalf("se esperaba UNSUPPORTED_CONTENT_TYPE, se obtuvo %v", err)
	}
}

func TestParseEventRechazaAtributosRaizDesconocidos(t *testing.T) {
	// Los atributos raíz son una lista CERRADA: permitir metadatos ad-hoc lleva a
	// serializar JSON dentro de un string y el envelope deja de ser interoperable
	// — 01-envelope.md §3.3.
	cuerpo := strings.Replace(envelopeValido,
		`"data": {`, `"metadatos": {"foo":"bar"}, "reintentos": 3, "data": {`, 1)
	_, err := ParseEvent([]byte(cuerpo))
	var pe *PoisonError
	if !errors.As(err, &pe) || pe.Code != "UNKNOWN_ROOT_ATTRIBUTE" {
		t.Fatalf("se esperaba UNKNOWN_ROOT_ATTRIBUTE, se obtuvo %v", err)
	}
	// Deben listarse TODOS, y en orden estable: el recorrido de un map en Go no lo es.
	if !strings.Contains(err.Error(), "metadatos, reintentos") {
		t.Errorf("el error debe listar los atributos desconocidos ordenados: %v", err)
	}
}

func TestParseEventEsSensibleAMayusculasEnLosAtributos(t *testing.T) {
	// encoding/json empareja campos SIN distinguir mayúsculas, así que `"ID"`
	// poblaría Event.ID en silencio. La lista cerrada de atributos raíz cierra ese
	// agujero antes de decodificar.
	cuerpo := strings.Replace(envelopeValido, `"id":`, `"ID":`, 1)
	_, err := ParseEvent([]byte(cuerpo))
	if err == nil {
		t.Fatal("un atributo con mayúsculas debe rechazarse, no aceptarse en silencio")
	}
}

func TestParseEventPreservaElPayloadSinInterpretar(t *testing.T) {
	// Data es json.RawMessage para que parsear y volver a serializar sea idempotente:
	// es lo que hace fiable el replay desde la DLQ. Un entero de 64 bits no debe
	// pasar por float64.
	cuerpo := strings.Replace(envelopeValido,
		`"totalCents": 9990`, `"totalCents": 9007199254740993`, 1)
	e, err := ParseEvent([]byte(cuerpo))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(e.Data), "9007199254740993") {
		t.Errorf("el payload perdió precisión al parsear: %s", e.Data)
	}
}

// ─── DLQ ─────────────────────────────────────────────────────────────────────

func TestToDLQEventConservaElOriginalIntegro(t *testing.T) {
	original, err := ParseEvent([]byte(envelopeValido))
	if err != nil {
		t.Fatal(err)
	}

	dlq := ToDLQEvent(original, DLQInfo{
		Reason:   DLQReasonPermanent,
		Attempts: 1,
		Consumer: "facturacion-api__pedidos_pedido_v1_creado",
		Error:    "PEDIDO_YA_CANCELADO: el pedido ped-123 estaba cancelado",
	})

	// Sin wrapper: el CloudEvent original íntegro más las extensiones dlq*
	// — 04-errors.md §3.
	if dlq.ID != original.ID {
		t.Error("el id original DEBE conservarse: regenerarlo rompe la idempotencia aguas abajo")
	}
	if string(dlq.Data) != string(original.Data) {
		t.Error("el payload original debe llegar íntegro a la DLQ")
	}
	if dlq.DLQReason != DLQReasonPermanent || dlq.DLQAttempts != 1 {
		t.Errorf("extensiones dlq mal puestas: %+v", dlq)
	}
	if dlq.DLQTime == "" {
		t.Error("falta dlqtime")
	}
	// Go pasa el struct por valor: el original no se muta.
	if original.DLQReason != "" {
		t.Error("ToDLQEvent mutó el evento original")
	}
}

func TestStripDLQExtensionsDevuelveElEventoReproducible(t *testing.T) {
	original, err := ParseEvent([]byte(envelopeValido))
	if err != nil {
		t.Fatal(err)
	}
	dlq := ToDLQEvent(original, DLQInfo{Reason: DLQReasonPoison, Attempts: 6, Consumer: "c", Error: "e"})
	limpio := StripDLQExtensions(dlq)

	// El replay es `del(.dlq*)` y republicar — 04-errors.md §4.1.
	// Se usa Equal y no `==`: Event contiene un json.RawMessage y Go no lo considera
	// comparable. Ver Event.Equal.
	if !limpio.Equal(original) {
		t.Errorf("el evento limpio no coincide con el original:\n got %+v\nwant %+v", limpio, original)
	}
	if limpio.ID != original.ID {
		t.Error("el replay DEBE conservar el id original")
	}

	b, err := Serialize(limpio)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	for k := range raw {
		if strings.HasPrefix(k, "dlq") {
			t.Errorf("quedó una extensión de DLQ tras el strip: %q", k)
		}
	}
}

func TestDLQErrorSeRecortaSinPartirCaracteres(t *testing.T) {
	e, err := ParseEvent([]byte(envelopeValido))
	if err != nil {
		t.Fatal(err)
	}
	// "ñ" ocupa 2 bytes en UTF-8: cortar a 1024 bytes cae en mitad de uno.
	dlq := ToDLQEvent(e, DLQInfo{Reason: DLQReasonPermanent, Attempts: 1,
		Consumer: "c", Error: strings.Repeat("ñ", 2000)})
	if len(dlq.DLQError) > 1024 {
		t.Errorf("dlqerror = %d bytes, máximo 1024", len(dlq.DLQError))
	}
	if strings.ContainsRune(dlq.DLQError, '�') {
		t.Error("el recorte partió un carácter multibyte")
	}
	if _, err := Serialize(dlq); err != nil {
		t.Fatalf("el evento de DLQ debe seguir siendo serializable: %v", err)
	}
}

func TestRoundTripParseSerialize(t *testing.T) {
	// Publicar → parsear → serializar debe dar exactamente los mismos bytes. Es la
	// propiedad que hace que un mensaje sea "un fichero JSON completo" reproducible.
	in := entradaValida()
	in.AggregateID = "ped-123"
	e, err := BuildEvent(in)
	if err != nil {
		t.Fatal(err)
	}
	b1, err := Serialize(e)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseEvent(b1)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := Serialize(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if string(b1) != string(b2) {
		t.Errorf("round-trip no idempotente:\n%s\n%s", b1, b2)
	}
}
