// Evaluador de JSON Schema para la validación L3.
// Contrato normativo: specification/00-protocol.md §5 (nivel L3).
//
// Es lo ÚNICO del SDK que depende de JsonSchema.Net. El contrato —modo, excepciones,
// interfaz, bundle— vive en el paquete base (Flux/Validation.cs), así que un servicio en L2
// no instala nada de esto. Ver sdk-dotnet/README.md §"Validación L3".
//
// ⚠️ Versión del meta-esquema. Los esquemas de flux declaran
// `$schema: https://json-schema.org/draft/2020-12/schema`. Un validador configurado para
// draft-07 NO falla con un error de versión: falla diciendo que no encuentra un esquema con
// esa referencia, que no dice nada útil y manda al operador a buscar un fichero que no
// existe. Por eso el dialecto se fija explícitamente abajo (`Dialect.Draft202012`) en vez de
// dejarlo al default de la librería.

using Json.Schema;

namespace Flux;

/// <summary>Construye el <see cref="IEventValidator"/> de la validación L3.</summary>
public static class SchemaValidator
{
    /// <summary>
    /// Compila los esquemas del bundle UNA vez y devuelve el validador.
    /// </summary>
    /// <remarks>
    /// Se llama al arrancar y no en la ruta caliente: compilar un JSON Schema por evento
    /// tiraría el throughput y no aportaría nada, porque el bundle es inmutable durante la
    /// vida del proceso. Un esquema corrupto rompe aquí el arranque del servicio, que es
    /// donde debe romper.
    /// </remarks>
    /// <param name="options">Modo, bundle y logger.</param>
    /// <returns>
    /// <see langword="null"/> en <see cref="ValidationMode.Off"/> — es la forma de que L2 no
    /// pague absolutamente nada por L3.
    /// </returns>
    /// <exception cref="ArgumentException">El modo no es Off y falta el bundle.</exception>
    public static IEventValidator? Create(ValidationOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.Mode == ValidationMode.Off)
        {
            return null;
        }

        if (options.Bundle is null)
        {
            throw new ArgumentException(
                $"el modo de validación {options.Mode} exige un bundle. Genéralo con " +
                "`node scripts/bundle-schemas.mjs` y pásalo en " +
                "`ValidationOptions.Bundle = SchemaBundle.FromFile(...)`",
                nameof(options));
        }

        return new JsonSchemaEventValidator(options.Mode, options.Bundle, options.Logger);
    }

    /// <summary>
    /// Devuelve las mismas opciones con <see cref="ValidationOptions.Validator"/> ya puesto.
    /// </summary>
    /// <remarks>
    /// Azúcar para no tener que repetir el modo y el bundle en dos sitios:
    /// <code>
    /// Validation = new ValidationOptions { Mode = ValidationMode.Strict, Bundle = bundle }
    ///     .WithSchemaValidator(),
    /// </code>
    /// </remarks>
    /// <param name="options">Modo, bundle y logger.</param>
    /// <returns>Las opciones, con el validador construido.</returns>
    public static ValidationOptions WithSchemaValidator(this ValidationOptions options) =>
        options with { Validator = Create(options) };
}

/// <summary>Implementación de <see cref="IEventValidator"/> sobre JsonSchema.Net.</summary>
internal sealed class JsonSchemaEventValidator : IEventValidator
{
    /// <summary>
    /// `List` y no `Flag`: hace falta la lista COMPLETA de fallos.
    /// </summary>
    /// <remarks>
    /// Es requisito explícito de L3 (00-protocol.md §5). Con <c>Flag</c> solo se sabría que
    /// el payload no vale, y arreglar uno con tres campos mal costaría tres despliegues.
    /// </remarks>
    private static readonly EvaluationOptions Evaluation = new()
    {
        OutputFormat = OutputFormat.List,
    };

    private readonly ValidationMode _mode;
    private readonly IFluxLogger? _logger;
    private readonly Dictionary<string, JsonSchema> _compiled = new(StringComparer.Ordinal);

    internal JsonSchemaEventValidator(ValidationMode mode, SchemaBundle bundle, IFluxLogger? logger)
    {
        _mode = mode;
        _logger = logger;

        // Registro LOCAL y no el global (`SchemaRegistry.Global`, que es el default de
        // BuildOptions): dos buses en el mismo proceso —o un test tras otro— no deben
        // pisarse los esquemas entre sí por el hecho de compartir una URI.
        //
        // `Fetch` se deja como viene: por defecto es `(_, _) => null`, es decir que la
        // librería NUNCA descarga un esquema por su cuenta. Eso es justo lo que 00-protocol.md
        // §5 exige ("NO DEBE resolverla por red"), así que aquí no hay nada que apagar; lo
        // que habría que hacer para violar la spec es asignarle un descargador.
        var buildOptions = new BuildOptions
        {
            SchemaRegistry = new SchemaRegistry(),
            // El `$schema` de cada documento manda sobre esto, y todos los esquemas de flux
            // lo declaran. Se fija de todos modos para que un esquema que lo omitiera no
            // acabara evaluado con un dialecto distinto según la versión de la librería.
            Dialect = Dialect.Draft202012,
        };

        foreach (var (uri, text) in bundle.Schemas)
        {
            // FromText registra el esquema por su `$id` en el registro de buildOptions, así
            // que un `$ref` entre esquemas del bundle se resuelve DENTRO del bundle.
            _compiled[uri] = JsonSchema.FromText(text, buildOptions);
        }
    }

    /// <inheritdoc />
    public void Check(FluxEvent evento, string subject)
    {
        ArgumentNullException.ThrowIfNull(evento);

        if (!_compiled.TryGetValue(evento.DataSchema, out var schema))
        {
            Fail(new SchemaNotFoundException(subject, evento.DataSchema));
            return;
        }

        var results = schema.Evaluate(evento.Data, Evaluation);
        if (results.IsValid)
        {
            return;
        }

        Fail(new SchemaValidationException(subject, evento.DataSchema, Format(results)));
    }

    /// <summary>Lanza en <c>Strict</c>, registra en <c>Warn</c>.</summary>
    private void Fail(ClassifiedException error)
    {
        if (_mode == ValidationMode.Strict)
        {
            throw error;
        }

        // Warn sin logger no avisaría de nada y sería indistinguible de Off. Un modo que se
        // llama `warn` y calla es peor que no tenerlo: quien lo configura cree que está
        // viendo los incumplimientos.
        _logger?.Warn("[flux] " + error.Message);
    }

    /// <summary>
    /// TODOS los fallos, ordenados y sin repetidos.
    /// </summary>
    /// <remarks>
    /// El orden es alfabético y no el del recorrido interno del evaluador: ese orden depende
    /// de la versión de la librería, y un test que compare mensajes empezaría a fallar sin
    /// que nada hubiera cambiado de verdad.
    /// </remarks>
    private static IReadOnlyList<string> Format(EvaluationResults results)
    {
        var mensajes = new SortedSet<string>(StringComparer.Ordinal);

        void Recoger(EvaluationResults nodo)
        {
            if (nodo.Errors is { Count: > 0 })
            {
                var ubicacion = nodo.InstanceLocation.ToString();
                foreach (var (palabraClave, mensaje) in nodo.Errors)
                {
                    // La ubicación va DELANTE porque es lo que el operador necesita para
                    // saber qué campo arreglar; la palabra clave del esquema (`required`,
                    // `type`, `pattern`) explica por qué.
                    mensajes.Add(
                        $"{(ubicacion.Length == 0 ? "(raíz)" : ubicacion)} [{palabraClave}] {mensaje}");
                }
            }

            if (nodo.Details is null)
            {
                return;
            }

            // Con OutputFormat.List los detalles vienen ya aplanados en un solo nivel, pero
            // se recorre en profundidad de todos modos: es correcto en los tres formatos de
            // salida y no depende de un detalle de la librería.
            foreach (var detalle in nodo.Details)
            {
                Recoger(detalle);
            }
        }

        Recoger(results);

        if (mensajes.Count == 0)
        {
            // No debería ocurrir —si no es válido hay algún error—, pero un mensaje vacío
            // dejaría al operador sin nada que mirar.
            mensajes.Add("el payload no cumple el esquema (el evaluador no detalló por qué)");
        }

        return mensajes.ToList();
    }
}
