// Estado replicado — las funciones PURAS de la capacidad `state`.
// Contrato normativo: specification/10-state.md, specification/04-errors.md §6.
//
// Un EVENTO dice qué pasó; el ESTADO es el valor ACTUAL de un agregado, que su dueño publica y los demás observan.
// Este fichero es la mitad pura y determinista (sin reloj, sin UUID, sin red): el subject, el sobre de un `put`, la
// lápida y el evento de DLQ de estado. Producen los MISMOS BYTES en todos los SDKs (10-state.md §2.6; arnés cruzado
// `state-build`, `state-tombstone`, `state-subject` y `state-dlq`). Lo que las alimenta —`id`, `time`— se pasa DE FUERA:
// si la función los inventara, dos ejecuciones no serían comparables. La otra mitad, que habla con el broker, es
// StateClient.cs.

using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Flux;

/// <summary>Reglas, nombres y constructores del sobre de estado — protocol.json → <c>state</c>.</summary>
public static partial class StateProtocol
{
    /// <summary>Intentos de un <c>put</c>/<c>delete</c> ante un conflicto de secuencia (10-state.md §3.2, paso 4).</summary>
    public const int MaxAttempts = 3;

    /// <summary>NATS limita los nombres de consumidor a 255 y el durable de un observador lleva el tenant (A-42).</summary>
    public const int TenantMaxLength = 64;

    /// <summary>2^53 − 1: el mayor entero que Node, y todo lo que use <c>double</c>, representa sin perder precisión.</summary>
    public const long AggregateVersionMax = 9_007_199_254_740_991L;

    /// <summary>El <c>dataschema</c> FIJO de toda lápida, de cualquier contrato.</summary>
    public const string TombstoneDataSchema = "https://schemas.internal/system/state/eliminado/1.0.0.json";

    /// <summary>Base de los esquemas del repo: <c>schemas/state/&lt;dom&gt;/&lt;agr&gt;/&lt;semver&gt;.json</c> cuelga de aquí.</summary>
    public const string SchemaBase = "https://schemas.internal";

    /// <summary>
    /// Los DOS códigos del conflicto de secuencia (A-33, A-49): 10071 «wrong last sequence: N» (un nodo, o un cluster sin
    /// carrera) y 10164 «wrong last sequence», SIN número (cluster, bajo carrera). Un SDK que solo reconozca el 10071 pasa
    /// sus pruebas contra un nodo y, en producción, trata un conflicto corriente como un error desconocido y no reintenta.
    /// </summary>
    public static readonly IReadOnlyList<int> ConflictCodes = new[] { 10071, 10164 };

    /// <summary>
    /// Lo que sigue al código en el <c>dlqerror</c> de un error que <b>no es de flux</b> (10-state.md §6): el mensaje de un
    /// driver de base de datos puede citar la fila entera, y la DLQ guarda 90 días.
    /// </summary>
    public const string UnclassifiedDlqError =
        "error sin clasificar de la aplicación; el mensaje se omite para no copiar datos a la DLQ (10-state.md §6)";

    // Coincidencia COMPLETA (\z y no $): $ acepta un salto de línea final. [0-9] y no \d: \d casa con dígitos de otros alfabetos.
    [GeneratedRegex(@"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}\z", RegexOptions.CultureInvariant)]
    private static partial Regex KeyRegex();

    [GeneratedRegex(@"^[a-z0-9]+(-[a-z0-9]+)*\z", RegexOptions.CultureInvariant)]
    private static partial Regex TenantRegex();

    [GeneratedRegex(@"^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]{0,3}\z", RegexOptions.CultureInvariant)]
    private static partial Regex ContractRegex();

    /// <summary>Un contrato de estado ya validado.</summary>
    public readonly record struct ParsedContract(string Contract, string Domain, string Aggregate, int Major);

    /// <summary>
    /// Valida un contrato. Primero el dominio reservado (<c>RESERVED_DOMAIN</c>), después la forma
    /// (<c>INVALID_STATE_CONTRACT</c>): el orden de 10-state.md §3.6.
    /// </summary>
    public static ParsedContract ParseContract(string? contract)
    {
        if (contract is null)
        {
            throw new InvalidStateContractException(null, "debe ser una cadena");
        }

        ReservedDomainException.AssertNot(contract, "contrato");
        if (!ContractRegex().IsMatch(contract))
        {
            var n = contract.Split('.').Length;
            throw new InvalidStateContractException(
                contract,
                n != 3
                    ? $"debe tener exactamente 3 tokens, tiene {n}"
                    : "formato esperado <dominio>.<agregado>.v<major> (la versión mayor, de 1 a 4 dígitos: v1…v9999)");
        }

        var partes = contract.Split('.');
        return new ParsedContract(contract, partes[0], partes[1], int.Parse(partes[2][1..], CultureInfo.InvariantCulture));
    }

    /// <summary>Valida una clave (un único token de NATS). <c>INVALID_STATE_KEY</c>.</summary>
    public static string AssertKey(string? key)
    {
        if (key is null || !KeyRegex().IsMatch(key))
        {
            throw new InvalidStateKeyException(key);
        }

        return key;
    }

    /// <summary>Valida un tenant de estado. <c>INVALID_TENANT_FOR_STATE</c>.</summary>
    public static string AssertTenant(string? tenant)
    {
        if (tenant is null || tenant.Length > TenantMaxLength || !TenantRegex().IsMatch(tenant))
        {
            throw new InvalidTenantForStateException(tenant);
        }

        return tenant;
    }

    /// <summary>
    /// Lee el <c>aggregateVersion</c> de un <c>data</c> o lanza <c>MISSING_AGGREGATE_VERSION</c>. El nombre cubre todo lo
    /// que no sea un entero ≥ 1: ausente, null, una cadena, un decimal, 0, un negativo o uno mayor que 2^53 − 1. Se comprueba
    /// SIEMPRE, con o sin L3: es una regla del protocolo.
    /// </summary>
    /// <remarks>
    /// <c>7.0</c> depende del lenguaje (10-state.md §3.4): un parser que separa el entero del flotante DEBE rechazarlo. El
    /// token de <see cref="JsonElement"/> conserva cómo se escribió, así que aquí <c>7.0</c> y <c>1e0</c> se rechazan, como en Rust.
    /// </remarks>
    public static long AggregateVersionOf(JsonElement data)
    {
        if (data.ValueKind != JsonValueKind.Object ||
            !TryLast(data, "aggregateVersion", out var v))
        {
            throw new MissingAggregateVersionException("no hay ninguno");
        }

        switch (v.ValueKind)
        {
            case JsonValueKind.Number:
                var raw = v.GetRawText();
                if (!IntegerToken().IsMatch(raw))
                {
                    throw new MissingAggregateVersionException("llegó " + raw.Replace('\n', ' '));
                }

                if (!long.TryParse(raw, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var n) ||
                    n < 1 || n > AggregateVersionMax)
                {
                    throw new MissingAggregateVersionException("llegó " + (raw.Length > 30 ? raw[..30] + "…" : raw));
                }

                return n;
            case JsonValueKind.Null:
                throw new MissingAggregateVersionException("llegó null");
            case JsonValueKind.String:
                throw new MissingAggregateVersionException("llegó una cadena");
            default:
                throw new MissingAggregateVersionException("llegó un valor de tipo " + v.ValueKind.ToString().ToLowerInvariant());
        }
    }

    [GeneratedRegex(@"^-?(0|[1-9][0-9]*)\z", RegexOptions.CultureInvariant)]
    private static partial Regex IntegerToken();

    /// <summary>El valor de una propiedad; con una clave repetida, el de la última aparición (como la forma canónica).</summary>
    internal static bool TryLast(JsonElement obj, string name, out JsonElement value)
    {
        var encontrado = false;
        value = default;
        foreach (var p in obj.EnumerateObject())
        {
            if (p.Name == name)
            {
                value = p.Value;
                encontrado = true;
            }
        }

        return encontrado;
    }

    /// <summary><c>null</c> → <c>confidential</c>; <c>restricted</c> → <c>INVALID_DATACLASSIFICATION</c>.</summary>
    public static DataClassification AssertClassification(DataClassification? c)
    {
        if (c is null)
        {
            return DataClassification.Confidential;
        }

        if (c is not (DataClassification.Public or DataClassification.Internal or DataClassification.Confidential))
        {
            throw new InvalidDataClassificationException(c.Value.ToString().ToLowerInvariant());
        }

        return c.Value;
    }

    // ─── nombres ─────────────────────────────────────────────────────────────

    /// <summary><c>STATE_CLIENTES</c>. NATS no admite puntos en nombres de stream.</summary>
    public static string StreamName(string domain) => "STATE_" + domain.Replace('-', '_').ToUpperInvariant();

    /// <summary>
    /// <c>state.&lt;dominio&gt;.&lt;agregado&gt;.v&lt;major&gt;.&lt;tenant&gt;.&lt;clave&gt;</c>: el subject de NATS de una entrada. Seis tokens,
    /// siempre. <b>No</b> es el atributo <c>subject</c> de CloudEvents, que aquí vale la clave. Valida en el orden de
    /// 10-state.md §3.6: dominio reservado, forma del contrato, clave, tenant.
    /// </summary>
    public static string Subject(string contract, string tenantId, string key)
    {
        var c = ParseContract(contract);
        AssertKey(key);
        AssertTenant(tenantId);
        return $"state.{c.Contract}.{tenantId}.{key}";
    }

    /// <summary><c>com.flux.state.&lt;dominio&gt;.&lt;agregado&gt;.v&lt;major&gt;</c>.</summary>
    public static string Type(string contract)
    {
        var c = ParseContract(contract);
        return $"com.flux.state.{c.Domain}.{c.Aggregate}.v{c.Major}";
    }

    /// <summary><c>com.flux.state.&lt;dominio&gt;.&lt;agregado&gt;.deleted.v&lt;major&gt;</c>.</summary>
    public static string TombstoneType(string contract)
    {
        var c = ParseContract(contract);
        return $"com.flux.state.{c.Domain}.{c.Aggregate}.deleted.v{c.Major}";
    }

    /// <summary><c>https://schemas.internal/state/&lt;dominio&gt;/&lt;agregado&gt;/&lt;major&gt;.0.0.json</c> (o con otra base).</summary>
    public static string DataSchema(string contract, string baseUrl = SchemaBase)
    {
        var c = ParseContract(contract);
        return $"{baseUrl.TrimEnd('/')}/state/{c.Domain}/{c.Aggregate}/{c.Major}.0.0.json";
    }

    /// <summary>
    /// <c>&lt;servicio&gt;__state_&lt;dom&gt;_&lt;agr&gt;_v&lt;M&gt;</c> y, si filtra por tenant, el sufijo <c>__&lt;tenant&gt;</c>. Los puntos y los
    /// guiones pasan a <c>_</c>; el nombre del servicio conserva los suyos. El tenant va en el nombre porque un durable
    /// compartido reparte los mensajes entre quienes lo usan (A-47).
    /// </summary>
    public static string DurableName(string service, string contract, string? tenantId = null)
    {
        Protocol.ValidateService(service);
        var c = ParseContract(contract);
        var baseName = $"{service}__state_{c.Domain.Replace('-', '_')}_{c.Aggregate.Replace('-', '_')}_v{c.Major}";
        if (tenantId is null)
        {
            return baseName;
        }

        AssertTenant(tenantId);
        return $"{baseName}__{tenantId.Replace('-', '_')}";
    }

    /// <summary><c>state.&lt;dom&gt;.&lt;agr&gt;.v&lt;M&gt;.&lt;tenant&gt;.*</c>, o <c>….*.*</c> sin tenant.</summary>
    public static string FilterSubject(string contract, string? tenantId = null)
    {
        var c = ParseContract(contract);
        if (tenantId is not null)
        {
            AssertTenant(tenantId);
        }

        return $"state.{c.Contract}.{tenantId ?? "*"}.*";
    }

    /// <summary><c>dlq.state.&lt;dom&gt;.&lt;agr&gt;.v&lt;M&gt;</c>: SIN tenant ni clave, para que no se multiplique por ellos.</summary>
    public static string DlqSubject(string contract) => "dlq.state." + ParseContract(contract).Contract;

    // ─── el sobre ────────────────────────────────────────────────────────────

    /// <summary>Lo que alimenta el sobre de un <c>put</c>. Todo viene de fuera: <c>id</c>, <c>time</c>…</summary>
    public sealed record EventInput
    {
        /// <summary><c>clientes.cliente.v1</c>.</summary>
        public required string Contract { get; init; }

        /// <summary>El tenant.</summary>
        public required string TenantId { get; init; }

        /// <summary>La clave.</summary>
        public required string Key { get; init; }

        /// <summary>UUIDv7.</summary>
        public required string Id { get; init; }

        /// <summary><c>/&lt;entorno&gt;/&lt;servicio&gt;</c>.</summary>
        public required string Source { get; init; }

        /// <summary>Se normaliza a la forma canónica (3 decimales y <c>Z</c>) o se rechaza.</summary>
        public required string Time { get; init; }

        /// <summary>El del flujo; si no nace de otro, el <c>id</c> propio.</summary>
        public required string CorrelationId { get; init; }

        /// <summary>SemVer del servicio.</summary>
        public required string ProducerVersion { get; init; }

        /// <summary>Por defecto <c>confidential</c>.</summary>
        public DataClassification? Classification { get; init; }

        /// <summary>El valor: un objeto con <c>aggregateVersion</c>.</summary>
        public required JsonElement Data { get; init; }

        /// <summary>Por defecto <c>https://schemas.internal/state/&lt;dom&gt;/&lt;agr&gt;/&lt;major&gt;.0.0.json</c>.</summary>
        public string? DataSchema { get; init; }

        /// <summary><c>causationid</c>.</summary>
        public string? CausationId { get; init; }

        /// <summary><c>traceparent</c>.</summary>
        public string? TraceParent { get; init; }

        /// <summary><c>tracestate</c>.</summary>
        public string? TraceState { get; init; }
    }

    /// <summary>
    /// El sobre de un <c>put</c>: un CloudEvent normal, con <c>subject</c> = la clave, <c>tenantid</c> = el token de tenant del
    /// subject de NATS, <c>partitionkey</c> = la clave, y <c>data</c> el ÚLTIMO atributo. <b>Pura y determinista</b>: valida lo
    /// mismo que <c>put</c> (§3.6, salvo L3), así que lo que construye es una entrada legal.
    /// </summary>
    public static FluxEvent BuildEvent(EventInput input)
    {
        var c = ParseContract(input.Contract);
        AssertKey(input.Key);
        AssertTenant(input.TenantId);
        AggregateVersionOf(input.Data);
        var classification = AssertClassification(input.Classification);
        var time = EnvelopeInput.Check(input.Id, input.Source, input.CorrelationId, input.ProducerVersion, input.Time);
        return new FluxEvent
        {
            SpecVersion = Protocol.SpecVersion,
            Id = input.Id,
            Source = input.Source,
            Type = Type(c.Contract),
            Time = time,
            DataContentType = Protocol.DataContentType,
            DataSchema = input.DataSchema ?? DataSchema(c.Contract),
            AggregateId = input.Key,
            CorrelationId = input.CorrelationId,
            TenantId = input.TenantId,
            ProducerVersion = input.ProducerVersion,
            DataClassification = classification,
            CausationId = input.CausationId,
            PartitionKey = input.Key,
            TraceParent = input.TraceParent,
            TraceState = input.TraceState,
            Data = input.Data,
        };
    }

    /// <summary>Lo que alimenta la lápida de un <c>delete</c>.</summary>
    public sealed record TombstoneInput
    {
        /// <summary>El contrato.</summary>
        public required string Contract { get; init; }

        /// <summary>El tenant.</summary>
        public required string TenantId { get; init; }

        /// <summary>La clave.</summary>
        public required string Key { get; init; }

        /// <summary>UUIDv7.</summary>
        public required string Id { get; init; }

        /// <summary><c>/&lt;entorno&gt;/&lt;servicio&gt;</c>.</summary>
        public required string Source { get; init; }

        /// <summary>Se normaliza o se rechaza.</summary>
        public required string Time { get; init; }

        /// <summary>El del flujo.</summary>
        public required string CorrelationId { get; init; }

        /// <summary>SemVer del servicio.</summary>
        public required string ProducerVersion { get; init; }

        /// <summary>La versión DE LA LÁPIDA: debe superar la del valor que borra.</summary>
        public required JsonElement AggregateVersion { get; init; }

        /// <summary><c>causationid</c>.</summary>
        public string? CausationId { get; init; }

        /// <summary><c>traceparent</c>.</summary>
        public string? TraceParent { get; init; }

        /// <summary><c>tracestate</c>.</summary>
        public string? TraceState { get; init; }
    }

    /// <summary>
    /// La lápida de un <c>delete</c>: el mismo subject de NATS que el valor, <c>type</c> con <c>.deleted.</c>, <c>dataschema</c> FIJO,
    /// <c>dataclassification: "internal"</c> y <c>data: { aggregateVersion }</c> y nada más. Al entrar, el valor anterior SALE del
    /// stream (A-34).
    /// </summary>
    public static FluxEvent BuildTombstone(TombstoneInput input)
    {
        var c = ParseContract(input.Contract);
        AssertKey(input.Key);
        AssertTenant(input.TenantId);
        var version = AggregateVersionOf(JsonSerializer.SerializeToElement(
            new Dictionary<string, JsonElement> { ["aggregateVersion"] = input.AggregateVersion }));
        var time = EnvelopeInput.Check(input.Id, input.Source, input.CorrelationId, input.ProducerVersion, input.Time);
        return new FluxEvent
        {
            SpecVersion = Protocol.SpecVersion,
            Id = input.Id,
            Source = input.Source,
            Type = TombstoneType(c.Contract),
            Time = time,
            DataContentType = Protocol.DataContentType,
            DataSchema = TombstoneDataSchema,
            AggregateId = input.Key,
            CorrelationId = input.CorrelationId,
            TenantId = input.TenantId,
            ProducerVersion = input.ProducerVersion,
            DataClassification = DataClassification.Internal,
            CausationId = input.CausationId,
            PartitionKey = input.Key,
            TraceParent = input.TraceParent,
            TraceState = input.TraceState,
            Data = JsonSerializer.SerializeToElement(new Dictionary<string, long> { ["aggregateVersion"] = version }),
        };
    }

    /// <summary>Los <c>dlq*</c> de una entrada de estado, más su clave y su revisión.</summary>
    public sealed record DlqInfo(
        string Key,
        long Revision,
        DlqReason Reason,
        int Attempts,
        string Consumer,
        string Error,
        string DlqTime);

    /// <summary>
    /// El evento de DLQ de estado: el sobre ORIGINAL con las extensiones <c>dlq*</c> y <b>sin su dato</b>: <c>data</c> pasa a
    /// <c>{ key, revision }</c>. La DLQ guarda 90 días: un dato personal que el dueño ya borró seguiría vivo ahí tres meses y el
    /// derecho de supresión no se cumpliría (10-state.md §6). <b>No conserva <c>signkeyid</c> ni <c>signature</c></b>: firman un
    /// <c>data</c> que ya no está. <c>data</c> sigue siendo el ÚLTIMO atributo, con las <c>dlq*</c> justo antes.
    /// </summary>
    public static FluxEvent ToDlqEvent(FluxEvent original, DlqInfo info)
    {
        var dlqTime = CanonicalTime.Normalize(info.DlqTime) ??
                      throw new EnvelopeInputException(
                          "`dlqtime` no es un instante RFC 3339 válido (se escribe con 3 decimales y `Z`)", "WRONG_ATTRIBUTE_TYPE");
        return original with
        {
            SignKeyId = null,
            Signature = null,
            DlqReason = info.Reason,
            DlqAttempts = info.Attempts,
            DlqConsumer = info.Consumer,
            // 1024 BYTES UTF-8 sin partir un carácter (04-errors.md §3).
            DlqError = Text.TruncateUtf8Bytes(info.Error, Envelope.MaxDlqErrorBytes),
            DlqTime = dlqTime,
            Data = JsonSerializer.SerializeToElement(
                new Dictionary<string, object> { ["key"] = info.Key, ["revision"] = info.Revision }, Envelope.JsonOptions),
        };
    }

    /// <summary>
    /// Un mensaje ILEGIBLE (JSON roto, sin sobre) no se copia a la DLQ de estado: no se guarda su cuerpo en ninguna forma, porque
    /// podría contener el dato. No hay sobre original que conservar, así que se sintetiza uno con lo único fiable, <b>el
    /// subject</b> (10-state.md §6). <c>id</c> y <c>time</c> los da quien llama.
    /// </summary>
    public static FluxEvent BuildUnreadableDlqEvent(
        string contract,
        string tenantId,
        string key,
        long revision,
        string source,
        string producerVersion,
        string id,
        string time,
        string? dataSchema,
        DlqReason reason,
        int attempts,
        string consumer,
        string error,
        string dlqTime)
    {
        var c = ParseContract(contract);
        var tiempo = EnvelopeInput.Check(id, source, id, producerVersion, time);
        var sintetico = new FluxEvent
        {
            SpecVersion = Protocol.SpecVersion,
            Id = id,
            Source = source,
            Type = Type(c.Contract),
            Time = tiempo,
            DataContentType = Protocol.DataContentType,
            DataSchema = dataSchema ?? DataSchema(c.Contract),
            AggregateId = key,
            CorrelationId = id,
            TenantId = tenantId,
            ProducerVersion = producerVersion,
            DataClassification = DataClassification.Internal,
            PartitionKey = key,
            Data = JsonSerializer.SerializeToElement(new Dictionary<string, object>()),
        };
        return ToDlqEvent(sintetico, new DlqInfo(key, revision, reason, attempts, consumer, error, dlqTime));
    }
}

/// <summary>
/// Un constructor se niega a construir un sobre: falta (o está vacío) un atributo que el SDK rellena, o <c>time</c> no es un
/// instante. Lleva el <b>mismo código estable</b> que daría el parser ante el mismo defecto (01-envelope.md §3.1).
/// </summary>
public sealed class EnvelopeInputException : EnvelopeException
{
    /// <summary>Construye la excepción.</summary>
    /// <param name="message">Qué está mal.</param>
    /// <param name="code"><c>MISSING_REQUIRED_ATTRIBUTE</c>, <c>MISSING_REQUIRED_EXTENSION</c> o <c>WRONG_ATTRIBUTE_TYPE</c>.</param>
    public EnvelopeInputException(string message, string code)
        : base(message)
    {
        Code = code;
    }

    /// <summary>El código estable del defecto.</summary>
    public string Code { get; }
}

/// <summary>Lo que comprueba todo constructor de estado además de la forma del contrato, la clave y el tenant.</summary>
internal static class EnvelopeInput
{
    /// <summary>
    /// Los atributos que el SDK rellena NO pueden llegar vacíos: una cadena vacía y una ausente son lo mismo (01-envelope.md
    /// §3.1, §3.3), y un evento que su propio parser rechazaría no se construye. Devuelve <c>time</c> canónico.
    /// </summary>
    public static string Check(string id, string source, string correlationId, string producerVersion, string time)
    {
        if (string.IsNullOrEmpty(id))
        {
            throw new EnvelopeInputException("`id` es obligatorio y llegó vacío (01-envelope.md §3.1)", "MISSING_REQUIRED_ATTRIBUTE");
        }

        if (string.IsNullOrEmpty(source))
        {
            throw new EnvelopeInputException("`source` es obligatorio y llegó vacío (01-envelope.md §3.1)", "MISSING_REQUIRED_ATTRIBUTE");
        }

        if (string.IsNullOrEmpty(correlationId))
        {
            throw new EnvelopeInputException("`correlationid` es obligatorio y llegó vacío (01-envelope.md §3.1)", "MISSING_REQUIRED_EXTENSION");
        }

        if (string.IsNullOrEmpty(producerVersion))
        {
            throw new EnvelopeInputException("`producerversion` es obligatorio y llegó vacío (01-envelope.md §3.1)", "MISSING_REQUIRED_EXTENSION");
        }

        return CanonicalTime.Normalize(time) ??
               throw new EnvelopeInputException(
                   "`time` no es un instante RFC 3339 válido: se escribe con exactamente 3 decimales y `Z` (01-envelope.md §2.2)",
                   "WRONG_ATTRIBUTE_TYPE");
    }
}
