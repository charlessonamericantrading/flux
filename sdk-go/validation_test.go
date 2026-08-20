package flux

// Validación L3 — specification/00-protocol.md §5.
//
// Réplica de sdk-node/test/validation.test.ts: los mismos casos, porque un payload que el
// SDK de Node rechaza y el de Go acepta convierte el contrato en una sugerencia. Más los
// casos propios del port: que el bundle se resuelve SIN red y que un fallo se clasifica
// PERMANENT.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

const subjectValidacion = "pedidos.pedido.v1.creado"

// bundleDePrueba carga el bundle real del repo: si el generador y el SDK divergen, este
// test lo caza antes que un despliegue.
func bundleDePrueba(t *testing.T) *SchemaBundle {
	t.Helper()
	b, err := LoadSchemaBundle("../schemas/bundle.json")
	if err != nil {
		t.Fatalf("no se pudo cargar el bundle: %v", err)
	}
	return b
}

func uriDePrueba(t *testing.T) string {
	t.Helper()
	uri := SchemaURIFor(bundleDePrueba(t), subjectValidacion)
	if uri == "" {
		t.Fatalf("el bundle no resuelve %q", subjectValidacion)
	}
	return uri
}

func payloadValido() map[string]any {
	return map[string]any{
		"pedidoId":         "ped-123",
		"clienteId":        "cli-987",
		"aggregateVersion": 1,
		"totalCents":       9990,
		"moneda":           "EUR",
		"lineas": []any{
			map[string]any{"sku": "ABC-1", "cantidad": 2, "precioUnitarioCents": 4995},
		},
	}
}

// eventoConPayload construye un evento completo. Nada de atributos a mano: si el envelope
// no fuera válido, el fallo se confundiría con uno de validación de esquema.
func eventoConPayload(t *testing.T, data map[string]any) Event {
	t.Helper()
	e, err := BuildEvent(BuildEventInput{
		Subject:            subjectValidacion,
		Data:               data,
		ID:                 "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
		Source:             "/produccion/pedidos-api",
		ProducerVersion:    "3.4.1",
		TenantID:           "acme",
		DataClassification: ClassificationInternal,
		DataSchema:         uriDePrueba(t),
		CorrelationID:      "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
	})
	if err != nil {
		t.Fatalf("no se pudo construir el evento: %v", err)
	}
	return e
}

func validadorEstricto(t *testing.T) *schemaValidator {
	t.Helper()
	v, err := newSchemaValidator(
		ValidationOptions{Mode: ValidationStrict, Bundle: bundleDePrueba(t)}, nil)
	if err != nil {
		t.Fatalf("no se pudo compilar el bundle: %v", err)
	}
	if v == nil {
		t.Fatal("el modo strict debe devolver un validador")
	}
	return v
}

// ─── El bundle ───────────────────────────────────────────────────────────────

func TestElBundleIndexaElSubjectHaciaSuDataschema(t *testing.T) {
	uri := uriDePrueba(t)
	patron := regexp.MustCompile(
		`^https://schemas\.internal/pedidos/pedido/creado/\d+\.\d+\.\d+\.json$`)
	if !patron.MatchString(uri) {
		t.Errorf("uri = %q, no encaja con la forma esperada", uri)
	}
}

func TestElIDDelEsquemaCoincideConLaClaveDelBundle(t *testing.T) {
	b := bundleDePrueba(t)
	for uri, raw := range b.Schemas {
		var doc struct {
			ID     string `json:"$id"`
			Schema string `json:"$schema"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("%s: %v", uri, err)
		}
		if doc.ID != uri {
			t.Errorf("$id = %q, se esperaba %q", doc.ID, uri)
		}
		// ⚠️ La trampa que documenta 00-protocol.md §5: un validador configurado para
		// draft-07 NO falla con un error de versión, falla con `no schema with key or ref
		// ".../2020-12/schema"`, que no dice nada. Si algún día un esquema declarara otro
		// draft, mejor enterarse aquí.
		if doc.Schema != "https://json-schema.org/draft/2020-12/schema" {
			t.Errorf("%s declara $schema=%q, se esperaba draft 2020-12", uri, doc.Schema)
		}
	}
}

func TestUnSubjectDesconocidoNoResuelve(t *testing.T) {
	if uri := SchemaURIFor(bundleDePrueba(t), "pedidos.pedido.v9.inventado"); uri != "" {
		t.Errorf("uri = %q, se esperaba vacío", uri)
	}
	if uri := SchemaURIFor(nil, subjectValidacion); uri != "" {
		t.Errorf("un bundle nil no resuelve nada, devolvió %q", uri)
	}
}

// ─── strict ──────────────────────────────────────────────────────────────────

func TestStrictAceptaUnPayloadValido(t *testing.T) {
	if err := validadorEstricto(t).check(eventoConPayload(t, payloadValido()), subjectValidacion); err != nil {
		t.Errorf("un payload válido no debe fallar: %v", err)
	}
}

func TestStrictRechazaLosPayloadsInvalidos(t *testing.T) {
	casos := []struct {
		nombre  string
		mutar   func(map[string]any)
		esperar string
	}{
		{"falta un campo requerido", func(p map[string]any) { delete(p, "totalCents") }, "totalCents"},
		// El caso que la spec llama el más peligroso: "9990" en vez de 9990.
		{"tipo incorrecto", func(p map[string]any) { p["totalCents"] = "9990" }, "totalCents"},
		// additionalProperties: false. Un campo mal escrito debe fallar, no colarse.
		{"campo desconocido", func(p map[string]any) { p["totalCemts"] = 9990 }, "totalCemts"},
		{"patrón incumplido", func(p map[string]any) { p["moneda"] = "euros" }, "moneda"},
		{"mínimo incumplido", func(p map[string]any) { p["aggregateVersion"] = 0 }, "aggregateVersion"},
		{"array vacío", func(p map[string]any) { p["lineas"] = []any{} }, "lineas"},
	}
	v := validadorEstricto(t)
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			p := payloadValido()
			c.mutar(p)
			err := v.check(eventoConPayload(t, p), subjectValidacion)
			var sve *SchemaValidationError
			if !errors.As(err, &sve) {
				t.Fatalf("err = %v, se esperaba *SchemaValidationError", err)
			}
			if !strings.Contains(sve.Error(), c.esperar) {
				t.Errorf("el mensaje no menciona %q: %s", c.esperar, sve.Error())
			}
		})
	}
}

func TestStrictReportaTodosLosErroresNoSoloElPrimero(t *testing.T) {
	// Reportar de uno en uno convierte arreglarlo en tres despliegues.
	p := payloadValido()
	p["totalCents"] = "x"
	p["moneda"] = "euros"
	p["cantidad"] = 1

	err := validadorEstricto(t).check(eventoConPayload(t, p), subjectValidacion)
	var sve *SchemaValidationError
	if !errors.As(err, &sve) {
		t.Fatalf("err = %v, se esperaba *SchemaValidationError", err)
	}
	if len(sve.Errors) < 3 {
		t.Errorf("Errors = %v, se esperaban al menos 3", sve.Errors)
	}
	// Y el mensaje los lleva todos: un error que hay que sacar del slice a mano no
	// aparece en el log del despliegue que falló.
	for _, detalle := range sve.Errors {
		if !strings.Contains(sve.Error(), detalle) {
			t.Errorf("el mensaje no contiene %q", detalle)
		}
	}
}

func TestStrictValidaDentroDeLosArrays(t *testing.T) {
	p := payloadValido()
	p["lineas"] = []any{map[string]any{"sku": "ABC-1", "cantidad": 0, "precioUnitarioCents": 1}}

	err := validadorEstricto(t).check(eventoConPayload(t, p), subjectValidacion)
	var sve *SchemaValidationError
	if !errors.As(err, &sve) {
		t.Fatalf("err = %v, se esperaba *SchemaValidationError", err)
	}
	if !strings.Contains(sve.Error(), "/lineas/0") {
		t.Errorf("el error no localiza la línea que falla: %s", sve.Error())
	}
}

func TestUnEsquemaAusenteDelBundleEsSchemaNotFound(t *testing.T) {
	e := eventoConPayload(t, payloadValido())
	e.DataSchema = "https://schemas.internal/no/existe/1.0.0.json"

	err := validadorEstricto(t).check(e, subjectValidacion)
	var snf *SchemaNotFoundError
	if !errors.As(err, &snf) {
		t.Fatalf("err = %v, se esperaba *SchemaNotFoundError", err)
	}
	if !strings.Contains(err.Error(), "bundle-schemas.mjs") {
		t.Errorf("el mensaje no dice cómo arreglarlo: %v", err)
	}
}

// ─── warn y off ──────────────────────────────────────────────────────────────

func TestWarnRegistraPeroNoFalla(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	v, err := newSchemaValidator(
		ValidationOptions{Mode: ValidationWarn, Bundle: bundleDePrueba(t)}, logger)
	if err != nil {
		t.Fatal(err)
	}

	p := payloadValido()
	p["totalCents"] = "x"
	if err := v.check(eventoConPayload(t, p), subjectValidacion); err != nil {
		t.Errorf("warn no debe fallar, devolvió %v", err)
	}
	if !strings.Contains(buf.String(), "no cumple su esquema") {
		t.Errorf("warn no registró nada; log = %q", buf.String())
	}
}

func TestOffNoCompilaNada(t *testing.T) {
	// L2 no paga el coste de L3.
	for _, opts := range []ValidationOptions{
		{Mode: ValidationOff, Bundle: bundleDePrueba(t)},
		{}, // el cero del struct
	} {
		v, err := newSchemaValidator(opts, nil)
		if err != nil {
			t.Fatalf("modo off no debe fallar: %v", err)
		}
		if v != nil {
			t.Error("modo off debe devolver un validador nil")
		}
	}
}

func TestStrictSinBundleFallaConUnMensajeAccionable(t *testing.T) {
	_, err := newSchemaValidator(ValidationOptions{Mode: ValidationStrict}, nil)
	if err == nil || !strings.Contains(err.Error(), "bundle-schemas.mjs") {
		t.Errorf("err = %v, se esperaba una pista sobre cómo generar el bundle", err)
	}
}

func TestUnModoDesconocidoFallaAlArrancar(t *testing.T) {
	// Un typo en el modo no puede significar "no valides nada en silencio": ese fallo
	// solo se ve el día que alguien publica basura y nadie la para.
	_, err := newSchemaValidator(ValidationOptions{Mode: "estricto"}, nil)
	if err == nil || !strings.Contains(err.Error(), "strict") {
		t.Errorf("err = %v, se esperaba el listado de modos válidos", err)
	}
}

// ─── Sin red ─────────────────────────────────────────────────────────────────

func TestUnRefFueraDelBundleNoSaleARed(t *testing.T) {
	// El bundle se despliega CON el servicio y nunca se resuelve el `dataschema` por
	// HTTP: validar está en la ruta caliente, y una caché con TTL abriría una ventana en
	// la que dos servicios validan contra versiones distintas del mismo esquema
	// (00-protocol.md §5). Un $ref que no esté en el bundle debe fallar AL ARRANCAR y
	// diciendo qué regenerar.
	uri := uriDePrueba(t)
	cojo := &SchemaBundle{
		Subjects: map[string]string{subjectValidacion: uri},
		Schemas: map[string]json.RawMessage{
			uri: json.RawMessage(`{
				"$schema": "https://json-schema.org/draft/2020-12/schema",
				"$id": "` + uri + `",
				"$ref": "https://schemas.internal/no/empaquetado/1.0.0.json"
			}`),
		},
	}
	_, err := newSchemaValidator(ValidationOptions{Mode: ValidationStrict, Bundle: cojo}, nil)
	if err == nil {
		t.Fatal("un $ref sin empaquetar debe fallar")
	}
	if !strings.Contains(err.Error(), "bundle-schemas.mjs") {
		t.Errorf("el mensaje no dice cómo arreglarlo: %v", err)
	}
}

func TestUnBundleVacioSeRechaza(t *testing.T) {
	if _, err := ParseSchemaBundle([]byte(`{"subjects":{},"schemas":{}}`)); err == nil {
		t.Error("un bundle sin esquemas debe rechazarse: validar contra nada es no validar")
	}
	if _, err := ParseSchemaBundle([]byte(`no soy json`)); err == nil {
		t.Error("un bundle que no es JSON debe rechazarse")
	}
}

// ─── Clasificación ───────────────────────────────────────────────────────────

func TestUnFalloDeEsquemaEsPermanent(t *testing.T) {
	// 00-protocol.md §5: el evento es sintácticamente correcto pero incumple su
	// contrato, y reintentarlo dará exactamente el mismo resultado. La clase la declara
	// el propio error, así que no depende de UnknownErrorPolicy.
	classify := NewClassifier(ClassifierOptions{})

	casos := []struct {
		err  error
		code string
	}{
		{&SchemaValidationError{Subject: subjectValidacion, DataSchema: "u", Errors: []string{"x"}}, CodeSchemaInvalid},
		{&SchemaNotFoundError{Subject: subjectValidacion, DataSchema: "u"}, CodeSchemaNotFound},
	}
	for _, c := range casos {
		got := classify(c.err)
		if got.Class != ClassPermanent {
			t.Errorf("%T: Class = %v, se esperaba PERMANENT", c.err, got.Class)
		}
		if got.Code != c.code {
			t.Errorf("%T: Code = %q, se esperaba %q", c.err, got.Code, c.code)
		}
	}
}

func TestLaMetricaDistingueUnFalloDeEsquema(t *testing.T) {
	// "Este consumidor rechaza el evento" y "el productor publica payloads que violan su
	// contrato" son dos incidentes con dos dueños distintos — 08-observability.md §2.1.
	if got := outcomeFor(DLQReasonPermanent, CodeSchemaInvalid); got != ConsumeInvalidSchema {
		t.Errorf("outcome = %q, se esperaba %q", got, ConsumeInvalidSchema)
	}
	if got := outcomeFor(DLQReasonPermanent, "PEDIDO_YA_CANCELADO"); got != ConsumePermanent {
		t.Errorf("outcome = %q, se esperaba %q", got, ConsumePermanent)
	}
}

// ─── El cabo suelto que L3 existe para atar ──────────────────────────────────

// jetStreamFalso es lo mínimo que toca Publish. Embebe la interfaz para satisfacerla sin
// implementar sus treinta métodos: cualquiera que este test no use y alguien llame en el
// futuro entrará en pánico, que es mejor que devolver un cero silencioso.
type jetStreamFalso struct {
	jetstream.JetStream
	publicados [][]byte
}

func (j *jetStreamFalso) Publish(
	_ context.Context, _ string, payload []byte, _ ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	j.publicados = append(j.publicados, payload)
	return &jetstream.PubAck{Stream: StreamName("pedidos"), Sequence: uint64(len(j.publicados))}, nil
}

func busDePrueba(t *testing.T, mode ValidationMode) (*Bus, *jetStreamFalso, *InMemoryMetrics) {
	t.Helper()
	v, err := newSchemaValidator(ValidationOptions{Mode: mode, Bundle: bundleDePrueba(t)}, nil)
	if err != nil {
		t.Fatalf("no se pudo compilar el bundle: %v", err)
	}
	js := &jetStreamFalso{}
	metrics := NewInMemoryMetrics()
	bus := &Bus{
		js: js,
		opts: ConnectOptions{
			Service: "pedidos-api", Environment: "produccion", Version: "3.4.1",
			TenantID:   "acme",
			Validation: ValidationOptions{Mode: mode, Bundle: bundleDePrueba(t)},
			Metrics:    metrics,
		},
		classify: NewClassifier(ClassifierOptions{}),
		source:   SourceURI("produccion", "pedidos-api"),
		validate: v,
		metrics:  metrics,
		// Pre-sembrado: el stream ya existe, así que Publish no toca la administración.
		ensured:       map[string]struct{}{StreamName("pedidos"): {}},
		subscriptions: map[*Subscription]struct{}{},
	}
	return bus, js, metrics
}

func TestPublishNoDejaSalirUnPayloadInvalido(t *testing.T) {
	// El requisito entero de L3: el fallo ocurre en el servicio que lo provocó, no en un
	// consumidor de otro equipo la semana que viene — 00-protocol.md §5.
	bus, js, metrics := busDePrueba(t, ValidationStrict)

	p := payloadValido()
	p["totalCents"] = "9990"
	_, err := bus.Publish(context.Background(), subjectValidacion, p)

	var sve *SchemaValidationError
	if !errors.As(err, &sve) {
		t.Fatalf("err = %v, se esperaba *SchemaValidationError", err)
	}
	if len(js.publicados) != 0 {
		t.Errorf("se publicaron %d mensajes; no debe salir nada que no valide", len(js.publicados))
	}
	if !strings.Contains(metrics.Render(), `outcome="invalid_schema"`) {
		t.Errorf("la publicación fallida no se contó como invalid_schema:\n%s", metrics.Render())
	}
}

func TestPublishEnWarnPublicaIgual(t *testing.T) {
	// `warn` existe para introducir validación en un ecosistema en marcha sin romper nada
	// el primer día.
	bus, js, _ := busDePrueba(t, ValidationWarn)

	p := payloadValido()
	p["totalCents"] = "9990"
	if _, err := bus.Publish(context.Background(), subjectValidacion, p); err != nil {
		t.Fatalf("warn no debe fallar: %v", err)
	}
	if len(js.publicados) != 1 {
		t.Errorf("se publicaron %d mensajes, se esperaba 1", len(js.publicados))
	}
}

func TestPublishTomaElDataschemaDelBundle(t *testing.T) {
	// Sin bundle el SDK solo puede asumir el `<major>.0.0`; con él usa el MINOR real, que
	// es contra el que valida. Un evento que apunta a un esquema distinto del que se
	// comprobó es peor que no validar: miente sobre su propio contrato.
	bus, _, _ := busDePrueba(t, ValidationStrict)

	e, err := bus.Publish(context.Background(), subjectValidacion, payloadValido())
	if err != nil {
		t.Fatalf("un payload válido no debe fallar: %v", err)
	}
	if want := uriDePrueba(t); e.DataSchema != want {
		t.Errorf("dataschema = %q, se esperaba %q", e.DataSchema, want)
	}
}
