// Validación L3: contrato y política. El compilador de JSON Schema vive en el paquete
// Flux.Validation.
// Contrato normativo: specification/00-protocol.md §5 (nivel L3).
//
// L3 cierra el hueco más grande que quedaba en L2: sin esto, un productor puede publicar un
// payload que viola su propio `dataschema` y nadie se entera hasta que un consumidor
// —posiblemente en otro equipo, otro lenguaje y otra semana— se atraganta. El error aparece
// lejísimos de su causa. Validar en `PublishAsync()` lo convierte en un fallo del servicio
// que lo provocó.
//
// ⚠️ `System.Text.Json` NO valida JSON Schema. Trae `JsonDocument`, `JsonNode` y
// serialización, pero no un evaluador de esquemas: hace falta un paquete. Es la misma
// situación que con Ed25519 (ver Signing.cs), y se resuelve igual — este fichero contiene
// TODO menos el evaluador: el modo, las excepciones y la interfaz. El paquete
// `Flux.Validation` —que sí arrastra JsonSchema.Net y sus tres dependencias transitivas—
// implementa la interfaz. Así el paquete `Flux`, que es el que instala todo servicio que
// solo quiere publicar un evento, sigue dependiendo únicamente de NATS.Net y
// System.Text.Json. Ver sdk-dotnet/README.md §"Validación L3".

namespace Flux;

/// <summary>Qué hacer cuando el payload no cumple su esquema — 00-protocol.md §5.</summary>
public enum ValidationMode
{
    /// <summary>Default: nivel L2. No se compila nada y no se paga nada.</summary>
    Off,

    /// <summary>
    /// Se registra y se publica igual.
    /// </summary>
    /// <remarks>
    /// Existe por la misma razón que <see cref="VerificationMode.Warn"/>: introducir
    /// validación en un ecosistema en marcha exige un periodo en el que se ve el
    /// incumplimiento sin romper a nadie el primer día.
    /// </remarks>
    Warn,

    /// <summary>
    /// <see cref="FluxBus.PublishAsync"/> LANZA si el payload no valida. Es el nivel L3 de
    /// verdad: un contrato roto pasa a ser un fallo del productor.
    /// </summary>
    Strict,
}

/// <summary>Valida el <c>data</c> de un evento contra el esquema que declara.</summary>
/// <remarks>
/// La implementación vive en el paquete <c>Flux.Validation</c>. Esta interfaz está aquí
/// para que <see cref="ConnectOptions"/> pueda referirla sin que el paquete base dependa de
/// ninguna librería de JSON Schema.
/// </remarks>
public interface IEventValidator
{
    /// <summary>Comprueba el payload según el modo configurado.</summary>
    /// <param name="evento">El evento ya construido (al publicar) o ya parseado (al consumir).</param>
    /// <param name="subject">Subject del evento, para el mensaje de error.</param>
    /// <exception cref="SchemaValidationException">
    /// En <see cref="ValidationMode.Strict"/>, si el payload no cumple.
    /// </exception>
    /// <exception cref="SchemaNotFoundException">
    /// En <see cref="ValidationMode.Strict"/>, si el bundle no trae su esquema.
    /// </exception>
    void Check(FluxEvent evento, string subject);
}

/// <summary>Configuración de la validación L3.</summary>
/// <remarks>
/// El <see cref="Validator"/> se construye aparte porque el evaluador de esquemas vive en
/// otro paquete. La forma corta, con la extensión que ofrece <c>Flux.Validation</c>:
/// <code>
/// var options = new ConnectOptions
/// {
///     // …
///     Validation = new ValidationOptions
///     {
///         Mode      = ValidationMode.Strict,
///         Bundle    = SchemaBundle.FromFile("schemas/bundle.json"),
///         OnConsume = true,
///     }.WithSchemaValidator(),
/// };
/// </code>
/// </remarks>
public sealed record ValidationOptions
{
    /// <summary>Default <see cref="ValidationMode.Off"/>, es decir L2.</summary>
    public ValidationMode Mode { get; init; } = ValidationMode.Off;

    /// <summary>
    /// El bundle generado por <c>scripts/bundle-schemas.mjs</c>.
    /// </summary>
    /// <remarks>
    /// Se pasa como DATO: el SDK no resuelve el <c>dataschema</c> por HTTP ni con el modo
    /// estricto puesto — 00-protocol.md §5. Además de alimentar la validación, resuelve el
    /// <c>dataschema</c> exacto de cada subject al publicar.
    /// </remarks>
    public SchemaBundle? Bundle { get; init; }

    /// <summary>
    /// Validar también al CONSUMIR. Default <see langword="false"/>.
    /// </summary>
    /// <remarks>
    /// Un fallo se clasifica PERMANENT: el evento es sintácticamente correcto —ha llegado a
    /// parsearse, así que no es POISON— pero incumple su contrato, y reintentarlo dará
    /// exactamente el mismo resultado — 04-errors.md §1.2.
    /// </remarks>
    public bool OnConsume { get; init; }

    /// <summary>
    /// El evaluador de esquemas. Lo construye <c>Flux.Validation</c>.
    /// </summary>
    /// <remarks>
    /// <see cref="FluxBus.ConnectAsync"/> exige que no sea <see langword="null"/> cuando
    /// <see cref="Mode"/> no es <see cref="ValidationMode.Off"/>, y falla en el arranque
    /// diciendo qué paquete falta. Descubrir en la primera publicación que la validación
    /// que creías tener encendida no existía es peor que no tenerla: se cree que sí.
    /// </remarks>
    public IEventValidator? Validator { get; init; }

    /// <summary>
    /// Dónde van los avisos de <see cref="ValidationMode.Warn"/>.
    /// </summary>
    /// <remarks>
    /// <see langword="null"/> usa el <see cref="ConnectOptions.Logger"/> del bus. Si tampoco
    /// hay, el modo <c>Warn</c> no avisaría de nada y sería indistinguible de <c>Off</c>,
    /// así que <c>Flux.Validation</c> exige uno de los dos al construir el validador.
    /// </remarks>
    public IFluxLogger? Logger { get; init; }
}

/// <summary>
/// El <c>data</c> de un evento no valida contra su esquema.
/// </summary>
/// <remarks>
/// Al PUBLICAR sale de <see cref="FluxBus.PublishAsync"/> y aborta la publicación: es lo que
/// convierte un contrato roto en un fallo del servicio que lo provocó, en vez de un misterio
/// que aparece la semana que viene en un consumidor de otro equipo y otro lenguaje.
/// <para>
/// Al CONSUMIR se clasifica <see cref="ErrorClass.Permanent"/>: el evento es sintácticamente
/// correcto pero incumple su contrato, y reintentarlo seis veces daría exactamente el mismo
/// resultado bloqueando la cola 51 minutos para nada.
/// </para>
/// <para>
/// <see cref="Errors"/> trae TODOS los fallos, no solo el primero. Reportar de uno en uno
/// convierte arreglar un payload con tres campos mal en tres despliegues — 00-protocol.md §5.
/// </para>
/// </remarks>
public sealed class SchemaValidationException : ClassifiedException
{
    /// <summary>Código estable para métricas y alertas — 08-observability.md §2.2.</summary>
    public const string CodeValue = "SCHEMA_VALIDATION_FAILED";

    /// <summary>Construye la excepción.</summary>
    /// <param name="subject">Subject del evento.</param>
    /// <param name="dataSchema">URI del esquema contra el que se validó.</param>
    /// <param name="errors">TODOS los fallos encontrados, en orden estable.</param>
    public SchemaValidationException(string subject, string dataSchema, IReadOnlyList<string> errors)
        : base(BuildMessage(subject, dataSchema, errors), CodeValue, innerException: null)
    {
        Subject = subject;
        DataSchema = dataSchema;
        Errors = errors;
    }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Permanent;

    /// <summary>Subject del evento que no validó.</summary>
    public string Subject { get; }

    /// <summary>URI del esquema contra el que se validó.</summary>
    public string DataSchema { get; }

    /// <summary>TODOS los fallos de validación, no solo el primero.</summary>
    public IReadOnlyList<string> Errors { get; }

    private static string BuildMessage(string subject, string dataSchema, IReadOnlyList<string> errors)
    {
        var detalle = errors is null || errors.Count == 0
            ? string.Empty
            : "\n  · " + string.Join("\n  · ", errors);
        return $"el payload de \"{subject}\" no cumple su esquema ({dataSchema}):{detalle}";
    }
}

/// <summary>
/// El bundle no contiene el esquema que el evento dice cumplir.
/// </summary>
/// <remarks>
/// En <see cref="ValidationMode.Strict"/> es un error y no un aviso, y esa severidad es
/// deliberada: si "no lo encuentro" pasara en silencio, un bundle que se quedó atrás
/// convertiría la validación L3 en un no-op — el servicio seguiría arrancando, el panel
/// seguiría verde y NADA se estaría validando. Un fallo silencioso de la validación es peor
/// que no validar, porque se cree que sí.
/// </remarks>
public sealed class SchemaNotFoundException : ClassifiedException
{
    /// <summary>Código estable para métricas y alertas — 08-observability.md §2.2.</summary>
    public const string CodeValue = "SCHEMA_NOT_FOUND";

    /// <summary>Construye la excepción.</summary>
    /// <param name="subject">Subject del evento.</param>
    /// <param name="dataSchema">La URI que el evento declara y que el bundle no conoce.</param>
    public SchemaNotFoundException(string subject, string dataSchema)
        : base(
            $"no hay esquema para \"{subject}\" ({dataSchema}) en el bundle. " +
            "Regenéralo con `node scripts/bundle-schemas.mjs`, o baja el modo de validación a Warn",
            CodeValue,
            innerException: null)
    {
        Subject = subject;
        DataSchema = dataSchema;
    }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Permanent;

    /// <summary>Subject del evento.</summary>
    public string Subject { get; }

    /// <summary>La URI que no está en el bundle.</summary>
    public string DataSchema { get; }
}
