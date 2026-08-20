// Validación L3 contra el JSON Schema del evento.
// Contrato normativo: specification/00-protocol.md §5 (nivel L3)
//
// Cierra el hueco más grande que quedaba: sin esto, un productor puede publicar un
// payload que viola su propio `dataschema` y nadie se entera hasta que un consumidor
// —posiblemente en otro equipo, otro lenguaje y otra semana— se atraganta. El error
// aparece lejísimos de su causa.
//
// Validar en Publish lo convierte en un fallo del servicio que lo provocó.
//
// Port de sdk-node/src/validation.ts: misma semántica, mismos modos, mismos mensajes.

package flux

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// ValidationMode gobierna qué pasa cuando un payload no cumple su esquema.
type ValidationMode string

const (
	// ValidationOff es el default: nivel L2, sin coste.
	ValidationOff ValidationMode = "off"
	// ValidationWarn registra y publica igual. Existe para introducir validación en un
	// ecosistema en marcha sin romper nada el primer día.
	ValidationWarn ValidationMode = "warn"
	// ValidationStrict hace que Publish DEVUELVA ERROR si el payload no valida. Es lo
	// que convierte un contrato roto en un fallo del productor y no en un misterio del
	// consumidor.
	ValidationStrict ValidationMode = "strict"
)

// Códigos estables de la validación, para métricas y alertas — 08-observability.md §2.2.
const (
	CodeSchemaInvalid  = "SCHEMA_INVALID"
	CodeSchemaNotFound = "SCHEMA_NOT_FOUND"
)

// SchemaBundle son los esquemas empaquetados, tal cual los genera
// scripts/bundle-schemas.mjs.
//
// Se pasa como DATO, no como URL: validar está en la ruta caliente, una petición de red
// por evento es inaceptable y una caché con TTL abre una ventana en la que dos servicios
// validan contra versiones distintas del mismo esquema. El bundle se despliega CON el
// servicio, así que la versión del esquema queda clavada a la del servicio — que es justo
// lo que producerversion promete poder acotar (00-protocol.md §5).
//
// En Go lo natural es incrustarlo en el binario:
//
//	//go:embed schemas/bundle.json
//	var bundleJSON []byte
//	bundle, err := flux.ParseSchemaBundle(bundleJSON)
type SchemaBundle struct {
	// Subjects mapea subject → URI del esquema con el MINOR más alto de su mayor.
	// Dentro de un mayor todo es BACKWARD-compatible, así que el más alto acepta lo
	// que aceptan los anteriores — 05-compatibility.md §2.
	Subjects map[string]string `json:"subjects"`
	// Schemas mapea URI → JSON Schema sin interpretar.
	Schemas map[string]json.RawMessage `json:"schemas"`
}

// ParseSchemaBundle interpreta el JSON del bundle.
//
// Ignora las claves de metadatos ($comment, generatedFrom, count) en vez de rechazarlas:
// son documentación del fichero, no del contrato, y añadir una no debe romper a los siete
// SDKs.
func ParseSchemaBundle(raw []byte) (*SchemaBundle, error) {
	var b SchemaBundle
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, fmt.Errorf("flux: el bundle de esquemas no es JSON válido: %w", err)
	}
	if len(b.Schemas) == 0 {
		return nil, fmt.Errorf("flux: el bundle no contiene ningún esquema. " +
			"Genéralo con `node scripts/bundle-schemas.mjs`")
	}
	return &b, nil
}

// LoadSchemaBundle lee schemas/bundle.json del disco.
//
// Para un binario que se despliega solo, prefiere ParseSchemaBundle con go:embed: un
// fichero que hay que copiar junto al binario es un fichero que algún despliegue olvidará,
// y entonces el servicio arranca sin poder validar nada.
func LoadSchemaBundle(path string) (*SchemaBundle, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("flux: no se pudo leer el bundle de esquemas %q: %w", path, err)
	}
	return ParseSchemaBundle(raw)
}

// SchemaURIFor resuelve el `dataschema` EXACTO de un subject desde el bundle. Devuelve ""
// si el bundle no lo conoce.
//
// Sin bundle, el SDK solo puede asumir el `<major>.0.0` del mayor (ver Bus.schemaFor):
// suficiente para L2 —el atributo es informativo— pero no para L3, donde el evento debe
// apuntar al esquema contra el que se valida de verdad.
func SchemaURIFor(b *SchemaBundle, subject string) string {
	if b == nil {
		return ""
	}
	return b.Subjects[subject]
}

// ValidationOptions configura la validación L3. El cero del struct no valida nada, que
// es el default del protocolo.
type ValidationOptions struct {
	// Mode: "" y ValidationOff son lo mismo.
	Mode ValidationMode
	// Bundle generado por scripts/bundle-schemas.mjs. Obligatorio si Mode != off.
	Bundle *SchemaBundle
	// OnConsume valida también al CONSUMIR. Un fallo se clasifica PERMANENT: el evento
	// es sintácticamente correcto pero incumple su contrato, y reintentarlo dará
	// exactamente el mismo resultado — 04-errors.md §1.2.
	OnConsume bool
}

// ─── Errores ─────────────────────────────────────────────────────────────────
//
// Los dos implementan FluxClass/FluxCode, así que el clasificador los reconoce con
// errors.As y los manda a la DLQ como PERMANENT sin gastar reintentos. Es deliberado y
// es una divergencia con el SDK de Node, donde son errores genéricos y la clase acaba
// dependiendo de unknownErrorPolicy: un evento que NUNCA podrá validar no debe consumir
// presupuesto de reintentos antes de morir.

// SchemaValidationError: el payload no cumple el JSON Schema que su dataschema declara.
type SchemaValidationError struct {
	Subject    string
	DataSchema string
	// Errors son TODOS los fallos, no solo el primero: de uno en uno, arreglar un
	// payload con tres campos mal cuesta tres despliegues (00-protocol.md §5).
	Errors []string
}

func (e *SchemaValidationError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "el payload de %q no cumple su esquema (%s):", e.Subject, e.DataSchema)
	for _, detalle := range e.Errors {
		fmt.Fprintf(&b, "\n  · %s", detalle)
	}
	return b.String()
}

// FluxClass implementa la interfaz que lee el clasificador — 04-errors.md §1.2.
func (e *SchemaValidationError) FluxClass() ErrorClass { return ClassPermanent }

// FluxCode devuelve el código estable para métricas y alertas.
func (e *SchemaValidationError) FluxCode() string { return CodeSchemaInvalid }

// SchemaNotFoundError: el dataschema del evento no está en el bundle desplegado.
type SchemaNotFoundError struct {
	Subject    string
	DataSchema string
}

func (e *SchemaNotFoundError) Error() string {
	return fmt.Sprintf(
		"no hay esquema para %q (%s) en el bundle. Regenera con "+
			"`node scripts/bundle-schemas.mjs`, o baja Validation.Mode a \"warn\"",
		e.Subject, e.DataSchema)
}

// FluxClass implementa la interfaz que lee el clasificador.
func (e *SchemaNotFoundError) FluxClass() ErrorClass { return ClassPermanent }

// FluxCode devuelve el código estable para métricas y alertas.
func (e *SchemaNotFoundError) FluxCode() string { return CodeSchemaNotFound }

// ─── Compilación ─────────────────────────────────────────────────────────────

// bundleOnlyLoader rechaza cualquier URL que no estuviera ya en el bundle.
//
// El default de la biblioteca es un FileLoader, que ya no sale a la red. Este loader
// existe para que el mensaje diga qué hacer en vez de "invalid file url", y para que la
// garantía "el bundle es la única fuente" esté escrita y no dependa del default de una
// dependencia (00-protocol.md §5).
type bundleOnlyLoader struct{}

func (bundleOnlyLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf(
		"%q no está en el bundle. Regenera con `node scripts/bundle-schemas.mjs`. El SDK "+
			"NO resuelve `dataschema` por red: validar está en la ruta caliente y una "+
			"caché con TTL abriría una ventana en la que dos servicios validan contra "+
			"versiones distintas del mismo esquema (00-protocol.md §5)", url)
}

// schemaValidator es el validador compilado. Nil = modo off.
type schemaValidator struct {
	mode    ValidationMode
	schemas map[string]*jsonschema.Schema
	// logger solo se usa en modo warn. Nil significa silencio, como en ConnectOptions y
	// como en Verifier — con la consecuencia de que `warn` sin Logger no hace nada
	// observable, igual que la firma.
	logger *slog.Logger
}

// newSchemaValidator compila los validadores del bundle, o devuelve nil en modo off.
//
// Se llama UNA vez en Connect y no por evento: compilar un JSON Schema en la ruta caliente
// sería tirar el throughput por comodidad de escritura. Y un bundle ausente o un esquema
// roto rompen el ARRANQUE, que es donde debe verse un fallo de configuración.
func newSchemaValidator(opts ValidationOptions, logger *slog.Logger) (*schemaValidator, error) {
	mode := opts.Mode
	if mode == "" {
		mode = ValidationOff
	}
	switch mode {
	case ValidationOff:
		return nil, nil
	case ValidationWarn, ValidationStrict:
	default:
		return nil, fmt.Errorf("flux: Validation.Mode = %q; los valores válidos son "+
			"\"off\", \"warn\" y \"strict\"", mode)
	}

	if opts.Bundle == nil {
		return nil, fmt.Errorf("flux: Validation.Mode = %q requiere Validation.Bundle. "+
			"Genera el bundle con `node scripts/bundle-schemas.mjs` e incrústalo con "+
			"go:embed + flux.ParseSchemaBundle", mode)
	}

	c := jsonschema.NewCompiler()
	// ⚠️ Los esquemas de flux declaran `$schema: draft/2020-12` y la biblioteca lo lee
	// de cada uno. Este default solo actúa si alguno no lo declarara — y entonces manda
	// el draft del protocolo, no el que la biblioteca considere "el último" el día de
	// mañana. Un validador fijado a draft-07 NO fallaría con un error de versión: fallaría
	// con `no schema with key or ref ".../2020-12/schema"`, que no dice nada
	// (00-protocol.md §5).
	c.DefaultDraft(jsonschema.Draft2020)
	c.UseLoader(bundleOnlyLoader{})

	// Ordenado: si dos esquemas del bundle chocan, el error debe ser el mismo en cada
	// arranque. El recorrido de un map en Go es aleatorio a propósito.
	uris := make([]string, 0, len(opts.Bundle.Schemas))
	for uri := range opts.Bundle.Schemas {
		uris = append(uris, uri)
	}
	sort.Strings(uris)

	for _, uri := range uris {
		// UnmarshalJSON y no json.Unmarshal: decodifica los números como json.Number,
		// que es lo que necesita `type: integer` para no aceptar 1.5 ni perder precisión
		// en un entero grande.
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(opts.Bundle.Schemas[uri]))
		if err != nil {
			return nil, fmt.Errorf("flux: el esquema %q del bundle no es JSON válido: %w", uri, err)
		}
		if err := c.AddResource(uri, doc); err != nil {
			return nil, fmt.Errorf("flux: no se pudo añadir el esquema %q al compilador: %w", uri, err)
		}
	}

	compilados := make(map[string]*jsonschema.Schema, len(uris))
	for _, uri := range uris {
		sch, err := c.Compile(uri)
		if err != nil {
			return nil, fmt.Errorf("flux: no se pudo compilar el esquema %q: %w", uri, err)
		}
		compilados[uri] = sch
	}

	return &schemaValidator{mode: mode, schemas: compilados, logger: logger}, nil
}

// check valida el payload del evento contra su dataschema.
//
// Devuelve error solo en modo strict; en warn registra y devuelve nil, igual que hace
// Verifier.Check con la firma.
func (v *schemaValidator) check(event Event, subject string) error {
	sch, ok := v.schemas[event.DataSchema]
	if !ok {
		err := &SchemaNotFoundError{Subject: subject, DataSchema: event.DataSchema}
		if v.mode == ValidationStrict {
			return err
		}
		v.warn(err.Error())
		return nil
	}

	instancia, err := jsonschema.UnmarshalJSON(bytes.NewReader(event.Data))
	if err != nil {
		// No debería ocurrir: ParseEvent ya rechaza el JSON inválido como POISON, y al
		// publicar el payload lo acaba de serializar BuildEvent.
		return &SchemaValidationError{
			Subject: subject, DataSchema: event.DataSchema,
			Errors: []string{fmt.Sprintf("el payload no es JSON válido: %v", err)},
		}
	}

	if err := sch.Validate(instancia); err != nil {
		var verr *jsonschema.ValidationError
		if !errors.As(err, &verr) {
			// Un fallo del validador que no es del payload —un $ref sin resolver, por
			// ejemplo— no es culpa del evento. Se propaga tal cual.
			return fmt.Errorf("flux: no se pudo validar %q contra %s: %w",
				subject, event.DataSchema, err)
		}
		e := &SchemaValidationError{
			Subject: subject, DataSchema: event.DataSchema, Errors: leafErrors(verr),
		}
		if v.mode == ValidationStrict {
			return e
		}
		// `warn` existe porque adoptar la validación en un ecosistema en marcha exige un
		// periodo en el que unos productores ya cumplen y otros todavía no.
		v.warn(e.Error())
	}
	return nil
}

func (v *schemaValidator) warn(message string) {
	if v.logger != nil {
		v.logger.Warn("[flux] " + message)
	}
}

// leafErrors aplana el árbol de errores del validador a sus HOJAS.
//
// Solo las hojas: los nodos intermedios dicen "esto no valida contra aquel subesquema",
// que es ruido para quien tiene que arreglar el payload. Y todas las hojas, no la
// primera, porque de una en una arreglar un payload con tres campos mal cuesta tres
// despliegues (00-protocol.md §5).
func leafErrors(e *jsonschema.ValidationError) []string {
	var out []string
	var walk func(*jsonschema.ValidationError)
	walk = func(n *jsonschema.ValidationError) {
		if len(n.Causes) == 0 {
			out = append(out, n.Error())
			return
		}
		for _, c := range n.Causes {
			walk(c)
		}
	}
	walk(e)
	// Ordenados para que el mismo payload produzca el mismo mensaje en cada ejecución:
	// un mensaje que cambia de orden es un diff inútil en los logs y en los tests.
	sort.Strings(out)
	return out
}
