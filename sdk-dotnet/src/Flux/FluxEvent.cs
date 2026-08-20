// El evento de flux: un CloudEvent 1.0 con el perfil de extensiones de flux.
// Contrato normativo: specification/01-envelope.md

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Flux;

/// <summary>
/// Un evento de flux.
/// </summary>
/// <remarks>
/// Los nombres de las extensiones van en minúscula pegada (<c>correlationid</c>, no
/// <c>correlation_id</c>) porque CloudEvents restringe los nombres de extensión a
/// minúsculas y dígitos ASCII sin separadores. No es estilo: es la especificación
/// (01-envelope.md §3). De ahí que los <see cref="JsonPropertyNameAttribute"/> no sigan
/// ninguna convención de .NET.
/// <para>
/// <b>Ausente ≠ vacío (01-envelope.md §3.3).</b> Todos los atributos opcionales son
/// anulables —<c>int?</c> y nunca <c>int</c> en <see cref="DlqAttempts"/>— y la
/// serialización usa <see cref="JsonIgnoreCondition.WhenWritingNull"/>. Nunca
/// <c>WhenWritingDefault</c>: con esa condición un <c>dlqattempts</c> de 0 desaparecería
/// del JSON, que es exactamente el colapso entre cero y ausente que la spec prohíbe
/// citando por su nombre al default de <c>System.Text.Json</c>.
/// </para>
/// <para>
/// <b>El orden de los atributos es explícito</b> vía <see cref="JsonPropertyOrderAttribute"/>.
/// System.Text.Json serializa en orden de declaración, que es estable en la práctica pero
/// no está garantizado por contrato; fijarlo hace que el JSON sea byte a byte comparable
/// con el de Node, Python, Go y Java — que es el requisito de
/// <c>conformance/cases/cross-sdk-envelope.json</c>.
/// </para>
/// <para>
/// <b>Atributos raíz cerrados.</b> <see cref="JsonUnmappedMemberHandling.Disallow"/> hace
/// que un atributo desconocido reviente la deserialización. Es una segunda línea de
/// defensa: <see cref="Envelope.ParseEvent(ReadOnlyMemory{byte})"/> ya compara las claves
/// contra <see cref="Envelope.AllowedRootAttributes"/> antes de deserializar, porque solo
/// así se pueden ENUMERAR todos los atributos desconocidos en el mensaje de error
/// — 01-envelope.md §3.3.
/// </para>
/// </remarks>
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record FluxEvent
{
    // ── Núcleo CloudEvents — 01-envelope.md §2 ──

    /// <summary>DEBE ser exactamente <c>"1.0"</c>.</summary>
    [JsonPropertyName("specversion")]
    [JsonPropertyOrder(0)]
    public required string SpecVersion { get; init; }

    /// <summary>UUIDv7 en formato canónico, generado por el SDK — 01-envelope.md §2.4.</summary>
    [JsonPropertyName("id")]
    [JsonPropertyOrder(1)]
    public required string Id { get; init; }

    /// <summary><c>/&lt;entorno&gt;/&lt;servicio&gt;</c>. Junto a <see cref="Id"/> es la clave de deduplicación del ecosistema.</summary>
    [JsonPropertyName("source")]
    [JsonPropertyOrder(2)]
    public required string Source { get; init; }

    /// <summary>Reverse-DNS derivado del subject de NATS — 02-naming.md §2.</summary>
    [JsonPropertyName("type")]
    [JsonPropertyOrder(3)]
    public required string Type { get; init; }

    /// <summary>
    /// RFC 3339 UTC con EXACTAMENTE 3 decimales y sufijo <c>Z</c>.
    /// </summary>
    /// <remarks>
    /// Es <see langword="string"/> y no <see cref="DateTimeOffset"/> a propósito. El
    /// formateador natural de .NET (<c>ToString("O")</c>) emite 7 decimales
    /// (<c>…39.4100000+00:00</c>), lo que sigue siendo RFC 3339 válido pero deja de ser
    /// byte a byte igual al <c>toISOString()</c> de Node. Como el replay desde la DLQ
    /// exige preservar el evento verbatim, el atributo se guarda tal cual llegó y se
    /// ofrece <see cref="EventTime"/> para interpretarlo — 01-envelope.md §2.2.
    /// </remarks>
    [JsonPropertyName("time")]
    [JsonPropertyOrder(4)]
    public required string Time { get; init; }

    /// <summary><c>application/json</c> en v1.</summary>
    [JsonPropertyName("datacontenttype")]
    [JsonPropertyOrder(5)]
    public required string DataContentType { get; init; }

    /// <summary>URI absoluta al JSON Schema, con SemVer — 05-compatibility.md.</summary>
    [JsonPropertyName("dataschema")]
    [JsonPropertyOrder(6)]
    public required string DataSchema { get; init; }

    /// <summary>
    /// El atributo <c>subject</c> de CloudEvents: el ID DEL AGREGADO.
    /// </summary>
    /// <remarks>
    /// ⚠️ NO es el subject de NATS. El de NATS es una dirección de enrutado
    /// (<c>pedidos.pedido.v1.creado</c>); éste es la identidad del agregado dentro de la
    /// fuente (<c>ped-123</c>). Confundirlos es el error más frecuente al adoptar
    /// CloudEvents sobre NATS, así que el SDK los nombra distinto y solo los une al
    /// serializar — 01-envelope.md §2.1.
    /// <para>
    /// Aquí duele igual que en Go y Java: la propiedad se llama <c>AggregateId</c> y el
    /// atributo JSON <c>subject</c>, así que el nombre del modelo y el del cable NO
    /// coinciden justo en el atributo más confundible del protocolo. Es la mejor solución
    /// disponible —renombrar el atributo de CloudEvents no es opción— pero es un sitio
    /// donde un lector desprevenido se equivocará. Ver README §"Fricciones".
    /// </para>
    /// </remarks>
    [JsonPropertyName("subject")]
    [JsonPropertyOrder(7)]
    public string? AggregateId { get; init; }

    // ── Extensiones obligatorias — 01-envelope.md §3.1 ──

    /// <summary>
    /// Identifica el flujo de negocio completo y se propaga sin modificar. Si el evento
    /// no nace de otro, el SDK lo inicializa con su propio <see cref="Id"/>.
    /// </summary>
    /// <remarks>
    /// ⚠️ Es obligatoria por 01-envelope.md §3.1 y aun así se declara ANULABLE. Motivo:
    /// <see cref="Envelope.ParseEvent(ReadOnlyMemory{byte})"/> —igual que el SDK de Node,
    /// que es la referencia semántica— no la exige para declarar POISON: la lista de
    /// atributos obligatorios del parser son los ocho del núcleo. Marcarla
    /// <c>required</c> aquí haría que System.Text.Json lanzase al deserializar y este SDK
    /// clasificaría como POISON un mensaje que Node, Python y Go entregan al handler. Un
    /// protocolo polyglot no puede permitirse esa divergencia, así que gana la
    /// compatibilidad y el <c>?</c> queda como cicatriz visible. Ver README §"Fricciones".
    /// <para>
    /// Todo evento construido por este SDK la lleva: <see cref="Envelope.BuildEvent"/> la
    /// exige.
    /// </para>
    /// </remarks>
    [JsonPropertyName("correlationid")]
    [JsonPropertyOrder(8)]
    public string? CorrelationId { get; init; }

    /// <summary>Tenant propietario del dato. <c>"system"</c> para eventos de plataforma.</summary>
    /// <remarks>Anulable por el mismo motivo que <see cref="CorrelationId"/>.</remarks>
    [JsonPropertyName("tenantid")]
    [JsonPropertyOrder(9)]
    public string? TenantId { get; init; }

    /// <summary>
    /// SemVer del servicio emisor. Sin esto, un bug de payload en producción no se puede
    /// acotar a un despliegue.
    /// </summary>
    /// <remarks>Anulable por el mismo motivo que <see cref="CorrelationId"/>.</remarks>
    [JsonPropertyName("producerversion")]
    [JsonPropertyOrder(10)]
    public string? ProducerVersion { get; init; }

    /// <summary>Una de public/internal/confidential/restricted — 06-security.md §5.</summary>
    /// <remarks>Anulable por el mismo motivo que <see cref="CorrelationId"/>.</remarks>
    [JsonPropertyName("dataclassification")]
    [JsonPropertyOrder(11)]
    public DataClassification? DataClassification { get; init; }

    // ── Extensiones opcionales — 01-envelope.md §3.2 ──

    /// <summary>
    /// <see cref="Id"/> del evento concreto que provocó éste. <see cref="CorrelationId"/>
    /// responde "¿de qué flujo forma parte?"; éste, "¿quién lo causó exactamente?".
    /// </summary>
    [JsonPropertyName("causationid")]
    [JsonPropertyOrder(12)]
    public string? CausationId { get; init; }

    /// <summary>Clave de ordenación. Por convención, igual a <see cref="AggregateId"/> — 03-delivery.md §5.</summary>
    [JsonPropertyName("partitionkey")]
    [JsonPropertyOrder(13)]
    public string? PartitionKey { get; init; }

    /// <summary>W3C Trace Context.</summary>
    [JsonPropertyName("traceparent")]
    [JsonPropertyOrder(14)]
    public string? TraceParent { get; init; }

    /// <summary>W3C Trace Context.</summary>
    [JsonPropertyName("tracestate")]
    [JsonPropertyOrder(15)]
    public string? TraceState { get; init; }

    // ── Firma Ed25519 — extensión OPCIONAL — 07-signing.md §4 ──

    /// <summary>
    /// Identifica la clave pública con la que verificar. Formato
    /// <c>&lt;servicio&gt;-&lt;n&gt;</c>, p. ej. <c>pedidos-api-3</c>.
    /// </summary>
    /// <remarks>
    /// <b>Va DENTRO de lo firmado.</b> Si quedara fuera, un atacante podría cambiarlo por
    /// el id de una clave suya y la firma "verificaría" — 07-signing.md §5.
    /// <para>
    /// DEBE cambiar en cada rotación: reutilizar un id con una clave distinta convierte la
    /// verificación de eventos históricos en un juego de azar (§6).
    /// </para>
    /// </remarks>
    [JsonPropertyName("signkeyid")]
    [JsonPropertyOrder(16)]
    public string? SignKeyId { get; init; }

    /// <summary>Firma Ed25519 en base64url SIN padding — 07-signing.md §4.</summary>
    /// <remarks>
    /// Es el único atributo que NO va firmado: no puede firmarse a sí mismo. Todo lo demás
    /// —incluidos <c>data</c> y <see cref="SignKeyId"/>— sí está cubierto; firmar solo unos
    /// atributos elegidos dejaría el resto manipulable.
    /// <para>
    /// Va antes que las extensiones <c>dlq*</c> en el orden de serialización, igual que en
    /// Node: las <c>dlq*</c> se añaden DESPUÉS de firmar, y aunque la verificación las
    /// ignore, el evento de DLQ tiene que producir la misma secuencia de bytes en los dos
    /// SDKs o el replay verbatim deja de serlo — 01-envelope.md §6.
    /// </para>
    /// </remarks>
    [JsonPropertyName("signature")]
    [JsonPropertyOrder(17)]
    public string? Signature { get; init; }

    // ── Extensiones de DLQ — solo en dlq.<subject> — 04-errors.md §3 ──

    /// <summary>Por qué acabó en la DLQ. Solo presente en <c>dlq.&lt;subject&gt;</c>.</summary>
    [JsonPropertyName("dlqreason")]
    [JsonPropertyOrder(18)]
    public DlqReason? DlqReason { get; init; }

    /// <summary>
    /// Número de entrega en la que el evento murió, NO una propiedad de su clase.
    /// </summary>
    /// <remarks>
    /// Un PERMANENT suele registrar <c>1</c> porque no gasta reintentos; un RETRYABLE
    /// agotado registra siempre <c>max_deliver</c> (6). Y si el handler falló dos veces
    /// con errores transitorios y a la tercera lanzó un PERMANENT, registra <c>3</c> —
    /// eso es información útil, no una incoherencia.
    /// <para>
    /// <c>int?</c> y NUNCA <c>int</c>: con <c>int</c> y una condición de omisión por
    /// valor por defecto, un <c>dlqattempts</c> de 0 desaparecería del JSON. Hoy el
    /// mínimo legal es 1 —siempre hubo al menos una entrega— así que sería inocuo, pero
    /// el envelope dependería de una coincidencia en vez de una regla
    /// — 01-envelope.md §3.3.
    /// </para>
    /// </remarks>
    [JsonPropertyName("dlqattempts")]
    [JsonPropertyOrder(19)]
    public int? DlqAttempts { get; init; }

    /// <summary>Durable del consumidor que lo enrutó.</summary>
    [JsonPropertyName("dlqconsumer")]
    [JsonPropertyOrder(20)]
    public string? DlqConsumer { get; init; }

    /// <summary>Mensaje del error, recortado a 1024 caracteres.</summary>
    [JsonPropertyName("dlqerror")]
    [JsonPropertyOrder(21)]
    public string? DlqError { get; init; }

    /// <summary>Instante del enrutado a la DLQ, en el formato de <see cref="Time"/>.</summary>
    [JsonPropertyName("dlqtime")]
    [JsonPropertyOrder(22)]
    public string? DlqTime { get; init; }

    // ── Payload — 01-envelope.md §4 ──

    /// <summary>
    /// El payload, sin interpretar.
    /// </summary>
    /// <remarks>
    /// <see cref="JsonElement"/> a propósito: un evento parseado y vuelto a serializar
    /// conserva claves, orden y precisión de enteros, que es lo que hace que el replay
    /// desde la DLQ sea "borrar las extensiones <c>dlq*</c> y republicar". Un
    /// <c>Dictionary&lt;string, object&gt;</c> convertiría los enteros grandes en
    /// <c>double</c> y perdería el orden. Para tiparlo, <see cref="Envelope.DataAs{T}"/>.
    /// <para>
    /// Matiz frente a Go: allí <c>json.RawMessage</c> se reemite byte a byte, incluidos
    /// los espacios. <see cref="JsonElement"/> reemite token a token, así que un
    /// <c>data</c> que llegase con espacios se reemitiría minificado. Los eventos de flux
    /// siempre viajan minificados, así que en la práctica no cambia nada; conviene saberlo
    /// si alguien compara con un fichero formateado a mano.
    /// </para>
    /// </remarks>
    [JsonPropertyName("data")]
    [JsonPropertyOrder(23)]
    public required JsonElement Data { get; init; }

    // ── Utilidades ──

    /// <summary>Interpreta el atributo <see cref="Time"/> como instante.</summary>
    public DateTimeOffset EventTime() => Envelope.ParseTime(Time);

    /// <summary>
    /// Deriva los tokens del subject de NATS a partir del <see cref="Type"/>. Útil en un
    /// handler suscrito con wildcard que necesita saber qué evento concreto llegó.
    /// </summary>
    public Protocol.ParsedSubject ParsedType() => Protocol.ParseType(Type);

    /// <summary>Informa si el evento lleva las extensiones <c>dlq*</c>.</summary>
    [JsonIgnore]
    public bool IsDlq => DlqReason is not null;

    /// <summary>
    /// Compara dos eventos por valor, incluido el payload.
    /// </summary>
    /// <remarks>
    /// Está escrito a mano —en vez de dejar el <c>Equals</c> que genera el
    /// <see langword="record"/>— porque <see cref="JsonElement"/> es un struct cuya
    /// igualdad por defecto compara la referencia al documento subyacente y el índice del
    /// token: dos eventos con exactamente el mismo JSON saldrían DISTINTOS. Es la misma
    /// fricción que en Go obliga a escribir <c>Event.Equal</c> (allí <c>json.RawMessage</c>
    /// es un slice y hace todo el struct no comparable); Java se libra porque
    /// <c>JsonNode.equals</c> es estructural. Ver README §"Fricciones".
    /// <para>
    /// La comparación del payload es TEXTUAL, no semántica: dos eventos con el mismo
    /// contenido JSON pero distinto orden de claves NO son iguales. Es lo correcto aquí,
    /// porque el protocolo transporta bytes y el replay debe preservarlos — igual que en
    /// Go, y a diferencia de Java.
    /// </para>
    /// <para>
    /// Los campos se comparan uno a uno a propósito: añadir un atributo al envelope sin
    /// tocar este método es un fallo visible en revisión, y no una comparación que empieza
    /// a mentir en silencio.
    /// </para>
    /// </remarks>
    public bool Equals(FluxEvent? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return string.Equals(SpecVersion, other.SpecVersion, StringComparison.Ordinal)
            && string.Equals(Id, other.Id, StringComparison.Ordinal)
            && string.Equals(Source, other.Source, StringComparison.Ordinal)
            && string.Equals(Type, other.Type, StringComparison.Ordinal)
            && string.Equals(Time, other.Time, StringComparison.Ordinal)
            && string.Equals(DataContentType, other.DataContentType, StringComparison.Ordinal)
            && string.Equals(DataSchema, other.DataSchema, StringComparison.Ordinal)
            && string.Equals(AggregateId, other.AggregateId, StringComparison.Ordinal)
            && string.Equals(CorrelationId, other.CorrelationId, StringComparison.Ordinal)
            && string.Equals(TenantId, other.TenantId, StringComparison.Ordinal)
            && string.Equals(ProducerVersion, other.ProducerVersion, StringComparison.Ordinal)
            && DataClassification == other.DataClassification
            && string.Equals(CausationId, other.CausationId, StringComparison.Ordinal)
            && string.Equals(PartitionKey, other.PartitionKey, StringComparison.Ordinal)
            && string.Equals(TraceParent, other.TraceParent, StringComparison.Ordinal)
            && string.Equals(TraceState, other.TraceState, StringComparison.Ordinal)
            && string.Equals(SignKeyId, other.SignKeyId, StringComparison.Ordinal)
            && string.Equals(Signature, other.Signature, StringComparison.Ordinal)
            && DlqReason == other.DlqReason
            && DlqAttempts == other.DlqAttempts
            && string.Equals(DlqConsumer, other.DlqConsumer, StringComparison.Ordinal)
            && string.Equals(DlqError, other.DlqError, StringComparison.Ordinal)
            && string.Equals(DlqTime, other.DlqTime, StringComparison.Ordinal)
            && string.Equals(RawData(this), RawData(other), StringComparison.Ordinal);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Solo <see cref="Id"/> y <see cref="Source"/>: son la clave de deduplicación del
    /// ecosistema (01-envelope.md §2.4), así que reparten bien, y dos eventos iguales
    /// según <see cref="Equals(FluxEvent)"/> los tienen iguales por construcción. Meter el
    /// payload obligaría a serializarlo en cada inserción en un diccionario.
    /// </remarks>
    public override int GetHashCode() => HashCode.Combine(Id, Source);

    /// <summary>El texto JSON del payload, o <c>null</c> si el evento no tiene payload.</summary>
    private static string? RawData(FluxEvent e) =>
        e.Data.ValueKind == JsonValueKind.Undefined ? null : e.Data.GetRawText();
}

/// <summary>
/// Clasificación del dato transportado — 06-security.md §5. Determina la retención
/// efectiva del evento.
/// </summary>
[JsonConverter(typeof(DataClassificationJsonConverter))]
public enum DataClassification
{
    /// <summary>Publicable. Retención 30 d.</summary>
    Public,

    /// <summary>Interno de la organización. Retención 30 d. Es el default del SDK.</summary>
    Internal,

    /// <summary>Confidencial. Retención 7 d.</summary>
    Confidential,

    /// <summary>Restringido. Retención 24 h.</summary>
    Restricted,
}

/// <summary>Por qué un evento acabó en la DLQ — 04-errors.md §3.</summary>
[JsonConverter(typeof(DlqReasonJsonConverter))]
public enum DlqReason
{
    /// <summary>Agotó su presupuesto de reintentos.</summary>
    Retryable,

    /// <summary>El consumidor lo rechazó de forma definitiva.</summary>
    Permanent,

    /// <summary>El mensaje no era interpretable. Debe disparar alerta inmediata.</summary>
    Poison,
}

/// <summary>
/// Convierte <see cref="DataClassification"/> a y desde el literal del protocolo.
/// </summary>
/// <remarks>
/// Se escribe a mano en vez de usar <c>JsonStringEnumConverter</c> con una política de
/// nombres por dos razones: la comparación es CASE-SENSITIVE
/// (<c>"Confidential"</c> no es un valor del protocolo — 01-envelope.md §2.3) y el
/// mensaje de error dice qué valores son válidos, que es lo que ve el operador cuando un
/// productor roto manda basura. Lanzar aquí hace que el mensaje acabe clasificado como
/// POISON, que es lo correcto.
/// </remarks>
internal sealed class DataClassificationJsonConverter : JsonConverter<DataClassification>
{
    /// <inheritdoc />
    public override DataClassification Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var value = reader.TokenType == JsonTokenType.String ? reader.GetString() : null;
        return value switch
        {
            "public" => DataClassification.Public,
            "internal" => DataClassification.Internal,
            "confidential" => DataClassification.Confidential,
            "restricted" => DataClassification.Restricted,
            _ => throw new JsonException(
                $"dataclassification \"{value}\" no es válida: debe ser public, internal, " +
                "confidential o restricted (06-security.md §5)"),
        };
    }

    /// <inheritdoc />
    public override void Write(Utf8JsonWriter writer, DataClassification value, JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteStringValue(value switch
        {
            DataClassification.Public => "public",
            DataClassification.Internal => "internal",
            DataClassification.Confidential => "confidential",
            DataClassification.Restricted => "restricted",
            _ => throw new JsonException($"dataclassification desconocida: {value}"),
        });
    }
}

/// <summary>Convierte <see cref="DlqReason"/> a y desde el literal del protocolo.</summary>
internal sealed class DlqReasonJsonConverter : JsonConverter<DlqReason>
{
    /// <inheritdoc />
    public override DlqReason Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var value = reader.TokenType == JsonTokenType.String ? reader.GetString() : null;
        return value switch
        {
            "retryable" => DlqReason.Retryable,
            "permanent" => DlqReason.Permanent,
            "poison" => DlqReason.Poison,
            _ => throw new JsonException(
                $"dlqreason \"{value}\" no es válida: debe ser retryable, permanent o poison " +
                "(04-errors.md §3)"),
        };
    }

    /// <inheritdoc />
    public override void Write(Utf8JsonWriter writer, DlqReason value, JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteStringValue(value switch
        {
            DlqReason.Retryable => "retryable",
            DlqReason.Permanent => "permanent",
            DlqReason.Poison => "poison",
            _ => throw new JsonException($"dlqreason desconocida: {value}"),
        });
    }
}
