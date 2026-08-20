// Envelope CloudEvents 1.0 — construcción, serialización y parseo.
// Contrato normativo: specification/01-envelope.md
//
// flux v1 usa structured mode: el CloudEvent completo viaja serializado como JSON en el
// cuerpo del mensaje NATS. Un mensaje ES un fichero JSON completo — lo copias, lo pegas
// en un test, lo reproduces. Esa propiedad vale más que los bytes que ahorraría binary
// mode repartiendo los atributos entre cabeceras `ce-*` y cuerpo.

using System.Globalization;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Flux;

/// <summary>Construcción, serialización y parseo del envelope de flux.</summary>
public static class Envelope
{
    /// <summary>
    /// RFC 3339 UTC con precisión de milisegundos EXACTA — 01-envelope.md §2.2.
    /// </summary>
    /// <remarks>
    /// ⚠️ NO se usa <c>ToString("O")</c> (el round-trip de .NET) a propósito: emite SIETE
    /// decimales y offset (<c>2025-08-20T10:25:39.4100000+00:00</c>). Sigue siendo RFC
    /// 3339 válido, pero deja de ser byte a byte igual al <c>toISOString()</c> de Node, al
    /// <c>Format("…05.000Z07:00")</c> de Go y al de Python — y el envelope tiene que ser
    /// idéntico en los cinco lenguajes o se rompen el replay verbatim desde la DLQ, la
    /// firma criptográfica del evento (fase 4), la deduplicación por hash de contenido y
    /// los fixtures compartidos de la suite de conformidad.
    /// <para>
    /// La <c>T</c> y la <c>Z</c> van entrecomilladas: en las cadenas de formato
    /// personalizadas de .NET no son especificadores, pero entrecomillarlas las declara
    /// literales sin depender de esa lectura. <c>.fff</c> TRUNCA la parte submilisegundo,
    /// no redondea — que es justo lo que hacen los otros cuatro SDKs.
    /// </para>
    /// </remarks>
    public const string TimeFormat = "yyyy-MM-dd'T'HH:mm:ss.fff'Z'";

    /// <summary>Longitud máxima de <c>dlqerror</c>, en caracteres.</summary>
    public const int MaxDlqErrorChars = 1024;

    /// <summary>Serializa un instante en el formato del protocolo (UTC, 3 decimales).</summary>
    public static string FormatTime(DateTimeOffset instant) =>
        instant.ToUniversalTime().ToString(TimeFormat, CultureInfo.InvariantCulture);

    /// <summary>Interpreta un <c>time</c> del protocolo como instante.</summary>
    public static DateTimeOffset ParseTime(string time) =>
        DateTimeOffset.Parse(time, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

    /// <summary>
    /// Las opciones de serialización del protocolo. Cambiarlas cambia el cable.
    /// </summary>
    /// <remarks>
    /// Cuatro decisiones, todas cargadas de consecuencias:
    /// <list type="number">
    /// <item>
    /// <description>
    /// <see cref="JsonIgnoreCondition.WhenWritingNull"/> y NUNCA <c>WhenWritingDefault</c>:
    /// "ausente ≠ vacío" (01-envelope.md §3.3). Con <c>WhenWritingDefault</c> un
    /// <c>dlqattempts</c> de 0 desaparecería del JSON — la spec cita a System.Text.Json
    /// por su nombre al explicar esta trampa.
    /// </description>
    /// </item>
    /// <item>
    /// <description>
    /// <c>PropertyNameCaseInsensitive = false</c>: la comparación de nombres de atributo
    /// es CASE-SENSITIVE (01-envelope.md §2.3). <c>{"ID": …}</c> no es <c>id</c>: es un
    /// atributo raíz desconocido, y por tanto POISON. Es el default de
    /// <see cref="JsonSerializerDefaults.General"/>, pero se escribe explícito porque
    /// <see cref="JsonSerializerDefaults.Web"/> —que es lo que inyecta ASP.NET Core y lo
    /// que un desarrollador copia por costumbre— lo pone en <see langword="true"/> y
    /// resucitaría el mismo fantasma que Go tiene por defecto en <c>encoding/json</c>.
    /// </description>
    /// </item>
    /// <item>
    /// <description>
    /// <see cref="JavaScriptEncoder.UnsafeRelaxedJsonEscaping"/>: el encoder por defecto
    /// de System.Text.Json escapa <c>&lt;</c>, <c>&gt;</c>, <c>&amp;</c>, <c>'</c>, <c>+</c>
    /// y TODO lo que no sea ASCII (una <c>é</c> saldría como <c>é</c>). Node y Go
    /// —que llama a <c>SetEscapeHTML(false)</c> por lo mismo— emiten esos caracteres tal
    /// cual, así que sin esto el envelope de .NET NO sería byte a byte igual al de los
    /// demás para cualquier payload en español. "Unsafe" se refiere a incrustar el JSON
    /// dentro de HTML sin escapar, cosa que aquí no ocurre: el JSON viaja por NATS.
    /// </description>
    /// </item>
    /// <item>
    /// <description>
    /// <see cref="JsonNumberHandling.Strict"/>: un número que llegue como string
    /// (<c>"dlqattempts": "3"</c>) no se acepta en silencio. Es un envelope mal formado y
    /// debe ser POISON.
    /// </description>
    /// </item>
    /// </list>
    /// </remarks>
    public static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.General)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNameCaseInsensitive = false,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        NumberHandling = JsonNumberHandling.Strict,
        WriteIndented = false,
    };

    /// <summary>
    /// Los ÚNICOS atributos admitidos en la raíz del envelope. Todo lo demás va dentro de
    /// <c>data</c> — 01-envelope.md §3.3.
    /// </summary>
    /// <remarks>
    /// La lista es cerrada y eso es deliberado: las extensiones de CloudEvents solo
    /// admiten String, Integer, Boolean, Binary, URI y Timestamp, nunca objetos ni arrays.
    /// Un equipo que empieza a colgar metadatos de la raíz acaba serializando JSON dentro
    /// de un string, y a partir de ahí el envelope deja de ser interoperable.
    /// <para>
    /// El comparador es <see cref="StringComparer.Ordinal"/> explícito: es lo que convierte
    /// la regla de §2.3 (comparación case-sensitive) en código, en vez de dejarla a merced
    /// del comparador por defecto.
    /// </para>
    /// </remarks>
    public static readonly IReadOnlySet<string> AllowedRootAttributes = new HashSet<string>(
        StringComparer.Ordinal)
    {
        "specversion", "id", "source", "type", "time", "datacontenttype", "dataschema",
        "subject", "correlationid", "tenantid", "producerversion", "dataclassification",
        "causationid", "partitionkey", "traceparent", "tracestate",
        // Extensión OPCIONAL de firma — 07-signing.md §4. Se admiten SIEMPRE, con la
        // verificación encendida o apagada: un consumidor que no verifica tiene que poder
        // LEER un evento firmado, o adoptar la firma de forma gradual convertiría en POISON
        // los eventos de los productores ya migrados.
        "signkeyid", "signature",
        "dlqreason", "dlqattempts", "dlqconsumer", "dlqerror", "dlqtime", "data",
    };

    /// <summary>
    /// Atributos cuya ausencia hace POISON al mensaje — 04-errors.md §1.3.
    /// </summary>
    /// <remarks>
    /// Son los ocho del núcleo, exactamente los mismos que comprueban Node, Python y Go.
    /// Las cuatro extensiones que 01-envelope.md §3.1 llama obligatorias NO están aquí, y
    /// esa omisión es del SDK de referencia, no de este port: replicarla es lo que impide
    /// que .NET mande a la DLQ mensajes que los demás SDKs entregan. Ver README
    /// §"Fricciones".
    /// </remarks>
    public static readonly IReadOnlyList<string> RequiredAttributes = new[]
    {
        "specversion", "id", "source", "type", "time", "datacontenttype", "dataschema", "data",
    };

    // ─── Construcción ────────────────────────────────────────────────────────

    /// <summary>
    /// Todo lo que hace falta para construir un evento.
    /// </summary>
    /// <remarks>
    /// El desarrollador de la aplicación NO rellena esto: lo rellena
    /// <see cref="FluxBus.PublishAsync"/> a partir de la configuración de
    /// <see cref="FluxBus.ConnectAsync"/> y del contexto del evento en curso. Se expone
    /// para tests y para quien construya un transporte propio — 01-envelope.md §5.
    /// </remarks>
    public sealed record BuildEventInput
    {
        /// <summary>Subject de NATS. Determina el <c>type</c>.</summary>
        public required string Subject { get; init; }

        /// <summary>
        /// Payload. Debe serializar a un OBJETO JSON en la raíz. Un
        /// <see cref="JsonElement"/> se toma tal cual (clonado).
        /// </summary>
        public required object Data { get; init; }

        /// <summary>UUIDv7 del evento.</summary>
        public required string Id { get; init; }

        /// <summary><c>/&lt;entorno&gt;/&lt;servicio&gt;</c>.</summary>
        public required string Source { get; init; }

        /// <summary>SemVer del servicio emisor.</summary>
        public required string ProducerVersion { get; init; }

        /// <summary>Tenant propietario. <c>"system"</c> para eventos de plataforma.</summary>
        public required string TenantId { get; init; }

        /// <summary>Clasificación del dato — 06-security.md §5.</summary>
        public required DataClassification DataClassification { get; init; }

        /// <summary>URI absoluta al JSON Schema.</summary>
        public required string DataSchema { get; init; }

        /// <summary>Flujo de negocio. Si el evento no nace de otro, el <see cref="Id"/> propio.</summary>
        public required string CorrelationId { get; init; }

        /// <summary>
        /// Instante en que OCURRIÓ el hecho, no el de publicación. <see langword="null"/>
        /// significa "ahora" — 01-envelope.md §2.
        /// </summary>
        public DateTimeOffset? Time { get; init; }

        /// <summary>Va al atributo <c>subject</c> de CloudEvents. Ver <see cref="FluxEvent.AggregateId"/>.</summary>
        public string? AggregateId { get; init; }

        /// <summary><c>id</c> del evento que provocó éste.</summary>
        public string? CausationId { get; init; }

        /// <summary>Por defecto, igual al <see cref="AggregateId"/>.</summary>
        public string? PartitionKey { get; init; }

        /// <summary>W3C Trace Context.</summary>
        public string? TraceParent { get; init; }

        /// <summary>W3C Trace Context.</summary>
        public string? TraceState { get; init; }
    }

    /// <summary>
    /// Construye un evento válido a partir de la entrada.
    /// </summary>
    /// <remarks>
    /// Las cuatro extensiones obligatorias se comprueban aquí y no en el sistema de tipos
    /// porque una cadena vacía es un valor legal de <see langword="string"/> y el
    /// compilador no distingue <c>""</c> de "presente". Es la misma comprobación en
    /// tiempo de ejecución que hacen Go y Java por el mismo motivo.
    /// </remarks>
    /// <exception cref="EnvelopeException">La entrada no permite construir un envelope válido.</exception>
    /// <exception cref="InvalidSubjectException">El subject no cumple 02-naming.md §1.</exception>
    public static FluxEvent BuildEvent(BuildEventInput input)
    {
        ArgumentNullException.ThrowIfNull(input);

        var type = Protocol.SubjectToType(input.Subject);
        var data = NormalizeData(input.Data);

        foreach (var (name, value) in new[]
                 {
                     ("Id", input.Id),
                     ("Source", input.Source),
                     ("DataSchema", input.DataSchema),
                     ("CorrelationId", input.CorrelationId),
                     ("TenantId", input.TenantId),
                     ("ProducerVersion", input.ProducerVersion),
                 })
        {
            if (string.IsNullOrEmpty(value))
            {
                throw new EnvelopeException(
                    $"{name} es obligatorio y llegó vacío. Lo rellena el SDK a partir de " +
                    "ConnectOptions; si estás llamando a BuildEvent a mano, rellénalo " +
                    "(01-envelope.md §5)");
            }
        }

        // Por convención partitionkey = aggregateId, salvo que se pida otra cosa
        // explícitamente — 01-envelope.md §3.2.
        var partitionKey = input.PartitionKey ?? input.AggregateId;

        return new FluxEvent
        {
            SpecVersion = Protocol.SpecVersion,
            Id = input.Id,
            Source = input.Source,
            Type = type,
            Time = FormatTime(input.Time ?? DateTimeOffset.UtcNow),
            DataContentType = Protocol.DataContentType,
            DataSchema = input.DataSchema,
            AggregateId = input.AggregateId,
            CorrelationId = input.CorrelationId,
            TenantId = input.TenantId,
            ProducerVersion = input.ProducerVersion,
            DataClassification = input.DataClassification,
            CausationId = input.CausationId,
            PartitionKey = partitionKey,
            TraceParent = input.TraceParent,
            TraceState = input.TraceState,
            Data = data,
        };
    }

    /// <summary>
    /// Serializa el payload y exige que sea un objeto JSON en la raíz.
    /// </summary>
    /// <remarks>
    /// Ni array ni escalar arriba: con un array o un escalar en la raíz no se pueden
    /// añadir campos después sin romper el esquema, y toda la política de compatibilidad
    /// de 05-compatibility.md se apoya en poder hacerlo — 01-envelope.md §4.
    /// </remarks>
    private static JsonElement NormalizeData(object data)
    {
        var element = data switch
        {
            // Clone() desacopla el elemento de su JsonDocument de origen. Sin esto, un
            // payload tomado de otro evento dejaría de ser legible en cuanto ese documento
            // se liberase — y el fallo aparecería al SERIALIZAR, lejos de la causa.
            JsonElement je => je.Clone(),
            JsonDocument jd => jd.RootElement.Clone(),
            _ => JsonSerializer.SerializeToElement(data, JsonOptions),
        };

        if (element.ValueKind != JsonValueKind.Object)
        {
            throw new EnvelopeException(
                "`data` debe ser un objeto JSON en la raíz (ni array ni escalar), para poder " +
                "añadir campos sin romper el contrato (01-envelope.md §4)");
        }

        return element;
    }

    // ─── Serialización ───────────────────────────────────────────────────────

    /// <summary>
    /// Convierte el evento en los bytes que viajan por NATS.
    /// </summary>
    /// <remarks>
    /// Falla aquí y no en el broker al superar 1 MiB: el broker devuelve "maximum payload
    /// exceeded", que no dice qué hacer al respecto — 01-envelope.md §6.
    /// </remarks>
    /// <exception cref="EnvelopeException">El evento no cabe en 1 MiB.</exception>
    public static byte[] Serialize(FluxEvent evento)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(evento, JsonOptions);
        if (bytes.Length > Protocol.MaxMessageBytes)
        {
            throw new EnvelopeException(
                "evento de " + bytes.Length.ToString(CultureInfo.InvariantCulture) +
                " bytes supera el límite de " +
                Protocol.MaxMessageBytes.ToString(CultureInfo.InvariantCulture) +
                " (1 MiB). Aplica claim-check: sube el contenido a almacenamiento de objetos y " +
                "publica {uri, sha256, bytes} en su lugar (01-envelope.md §6). El sha256 no es " +
                "decorativo: sin él el consumidor no puede distinguir \"aún no replicado\" de " +
                "\"fue sustituido\"");
        }

        return bytes;
    }

    // ─── Parseo ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Interpreta el cuerpo de un mensaje como CloudEvent de flux.
    /// </summary>
    /// <remarks>
    /// Todo fallo aquí es POISON: el mensaje ni siquiera es interpretable, así que nunca
    /// llega al handler y va a la DLQ con alerta inmediata — 04-errors.md §1.3.
    /// <para>
    /// El orden de las comprobaciones es el mismo que en Node, Python y Go, para que los
    /// cinco SDKs den el mismo <c>code</c> ante el mismo mensaje corrupto.
    /// </para>
    /// <para>
    /// Se recorre primero el documento con <see cref="JsonDocument"/> y solo después se
    /// deserializa, por dos razones: los atributos raíz son una lista CERRADA y hay que
    /// poder ENUMERAR todos los desconocidos en el mensaje de error
    /// (<see cref="JsonUnmappedMemberHandling.Disallow"/> aborta en el primero), y las
    /// claves se comparan de forma ordinal contra <see cref="AllowedRootAttributes"/>
    /// ANTES de decodificar, que es lo que hace explícita la regla case-sensitive de
    /// 01-envelope.md §2.3.
    /// </para>
    /// </remarks>
    /// <exception cref="PoisonException">El mensaje no es un CloudEvent de flux.</exception>
    public static FluxEvent ParseEvent(ReadOnlyMemory<byte> body)
    {
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(body);
        }
        catch (JsonException e)
        {
            throw new PoisonException(
                "el cuerpo del mensaje no es JSON válido", code: "MALFORMED_JSON", innerException: e);
        }

        using (document)
        {
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                throw new PoisonException(
                    "el cuerpo del mensaje no es un objeto JSON", code: "NOT_AN_OBJECT");
            }

            if (!string.Equals(RawString(root, "specversion"), Protocol.SpecVersion, StringComparison.Ordinal))
            {
                throw new PoisonException(
                    $"specversion no soportada: {RawOrAbsent(root, "specversion")} " +
                    $"(se esperaba \"{Protocol.SpecVersion}\")",
                    code: "UNSUPPORTED_SPECVERSION");
            }

            var missing = RequiredAttributes.Where(a => !root.TryGetProperty(a, out _)).ToList();
            if (missing.Count > 0)
            {
                throw new PoisonException(
                    "faltan atributos obligatorios de CloudEvents: " + string.Join(", ", missing),
                    code: "MISSING_REQUIRED_ATTRIBUTE");
            }

            if (!string.Equals(RawString(root, "datacontenttype"), Protocol.DataContentType, StringComparison.Ordinal))
            {
                throw new PoisonException(
                    $"datacontenttype no soportado: {RawOrAbsent(root, "datacontenttype")}",
                    code: "UNSUPPORTED_CONTENT_TYPE");
            }

            var unknown = new List<string>();
            foreach (var property in root.EnumerateObject())
            {
                if (!AllowedRootAttributes.Contains(property.Name))
                {
                    unknown.Add(property.Name);
                }
            }

            if (unknown.Count > 0)
            {
                // Estricto a propósito. Ver AllowedRootAttributes — 01-envelope.md §3.3.
                unknown.Sort(StringComparer.Ordinal);
                throw new PoisonException(
                    $"atributos raíz desconocidos: {string.Join(", ", unknown)}. " +
                    "Todo metadato adicional va dentro de `data`",
                    code: "UNKNOWN_ROOT_ATTRIBUTE");
            }
        }

        try
        {
            // El deserializador vuelve a leer los bytes originales. Es una segunda pasada,
            // sí, pero preserva el payload sin interpretar y mantiene el parseo del
            // envelope en un único sitio con tipos.
            return JsonSerializer.Deserialize<FluxEvent>(body.Span, JsonOptions)
                   ?? throw new PoisonException("el cuerpo del mensaje es `null`", code: "NOT_AN_OBJECT");
        }
        catch (JsonException e)
        {
            // Llegar aquí significa que un atributo conocido tiene el tipo equivocado
            // (p. ej. `"dlqattempts": "seis"` o una dataclassification fuera del enum).
            // Sigue siendo un mensaje ininterpretable.
            throw new PoisonException(
                "un atributo del envelope tiene un tipo o un valor incompatible con el contrato",
                code: "INVALID_ATTRIBUTE_TYPE",
                innerException: e);
        }
    }

    /// <summary>Sobrecarga de conveniencia para el cuerpo tal cual lo entrega NATS.</summary>
    public static FluxEvent ParseEvent(byte[] body)
    {
        ArgumentNullException.ThrowIfNull(body);
        return ParseEvent(new ReadOnlyMemory<byte>(body));
    }

    /// <summary>Extrae un atributo raíz como string, o <see langword="null"/> si falta o no es string.</summary>
    private static string? RawString(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    /// <summary>
    /// El JSON crudo del atributo, para que el operador vea exactamente qué llegó y no
    /// una interpretación.
    /// </summary>
    private static string RawOrAbsent(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) ? value.GetRawText() : "<ausente>";

    /// <summary>
    /// Decodifica el payload del evento en el tipo de la aplicación.
    /// </summary>
    /// <remarks>
    /// <c>data</c> se guarda sin interpretar para preservar la fidelidad del replay, así
    /// que éste es el punto donde el evento se convierte en algo tipado.
    /// <para>
    /// Un fallo aquí NO es POISON: el envelope era correcto y el mensaje llegó al handler.
    /// Es un desajuste de contrato entre productor y consumidor, es decir PERMANENT — por
    /// eso lanza <see cref="PermanentException"/> y no <see cref="PoisonException"/>
    /// (04-errors.md §1.2).
    /// </para>
    /// </remarks>
    /// <typeparam name="T">El tipo del payload en la aplicación.</typeparam>
    /// <exception cref="PermanentException">El payload no encaja en <typeparamref name="T"/>.</exception>
    public static T DataAs<T>(this FluxEvent evento)
    {
        ArgumentNullException.ThrowIfNull(evento);
        try
        {
            return evento.Data.Deserialize<T>(JsonOptions)
                   ?? throw new PermanentException(
                       $"el payload del evento {evento.Id} se deserializó a null",
                       code: "DATA_SCHEMA_MISMATCH");
        }
        catch (JsonException e)
        {
            throw new PermanentException(
                $"el payload del evento {evento.Id} no encaja en el tipo esperado",
                code: "DATA_SCHEMA_MISMATCH",
                innerException: e);
        }
    }

    // ─── DLQ ─────────────────────────────────────────────────────────────────

    /// <summary>Por qué y cómo un evento acabó en la DLQ.</summary>
    /// <param name="Reason">Clase del fallo que lo mató.</param>
    /// <param name="Attempts">Número de entrega en que murió. Nunca menor que 1.</param>
    /// <param name="Consumer">Durable que lo enrutó.</param>
    /// <param name="Error">Mensaje del error. Se recorta a <see cref="MaxDlqErrorChars"/>.</param>
    public sealed record DlqInfo(DlqReason Reason, int Attempts, string Consumer, string Error);

    /// <summary>
    /// Prepara un evento para la DLQ: el CloudEvent original ÍNTEGRO más las extensiones
    /// <c>dlq*</c>. Sin wrapper.
    /// </summary>
    /// <remarks>
    /// Sin wrapper porque así reproducir un mensaje de la DLQ es borrar las extensiones
    /// <c>dlq*</c> y republicar (<c>jq 'del(.dlq*)'</c>). Con un wrapper habría que
    /// desenvolver, y cada consumidor de la DLQ tendría que conocer dos formatos
    /// — 04-errors.md §3.
    /// <para>
    /// La expresión <c>with</c> de C# devuelve una copia y deja intacto el original, igual
    /// que el <c>...event</c> de Node y el paso por valor de Go.
    /// </para>
    /// </remarks>
    public static FluxEvent ToDlqEvent(FluxEvent evento, DlqInfo info)
    {
        ArgumentNullException.ThrowIfNull(evento);
        ArgumentNullException.ThrowIfNull(info);

        return evento with
        {
            DlqReason = info.Reason,
            DlqAttempts = info.Attempts,
            DlqConsumer = info.Consumer,
            DlqError = Truncate(info.Error, MaxDlqErrorChars),
            DlqTime = FormatTime(DateTimeOffset.UtcNow),
        };
    }

    /// <summary>
    /// Quita las extensiones <c>dlq*</c> para reproducir un evento.
    /// </summary>
    /// <remarks>
    /// El <c>id</c> original se CONSERVA. Regenerarlo rompe la idempotencia de todos los
    /// consumidores aguas abajo y convierte una recuperación en un incidente nuevo
    /// — 04-errors.md §4.1.
    /// </remarks>
    public static FluxEvent StripDlqExtensions(FluxEvent evento)
    {
        ArgumentNullException.ThrowIfNull(evento);

        return evento with
        {
            DlqReason = null,
            DlqAttempts = null,
            DlqConsumer = null,
            DlqError = null,
            DlqTime = null,
        };
    }

    /// <summary>
    /// Recorta a <paramref name="maxChars"/> caracteres sin partir un par sustituto.
    /// </summary>
    /// <remarks>
    /// Node hace <c>.slice(0, 1024)</c> sobre unidades UTF-16 y aquí se corta igual, así
    /// que el punto de corte coincide con el del SDK de referencia (Go corta por bytes
    /// UTF-8 y puede diferir). El guardia del sustituto alto es un extra: partir un par
    /// produciría un <c>dlqerror</c> con U+FFFD, que es peor que un mensaje un carácter
    /// más corto.
    /// </remarks>
    public static string Truncate(string value, int maxChars)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (value.Length <= maxChars)
        {
            return value;
        }

        var cut = maxChars;
        if (cut > 0 && char.IsHighSurrogate(value[cut - 1]))
        {
            cut--;
        }

        return value[..cut];
    }
}
