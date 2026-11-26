// Estado replicado — `bus.State`: `put`, `delete`, `get` y `watch`.
// Contrato normativo: specification/10-state.md, specification/04-errors.md §6.
//
// La mitad que habla con el broker. Las funciones puras (subject, sobre, lápida, DLQ) viven en StateProtocol.cs y producen
// los mismos bytes en todos los SDKs.
//
// Regla de diseño heredada de FluxBus.cs: ningún tipo de NATS en la API pública.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Primitives;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Flux;

/// <summary>Una entrada de estado, tal como la ve quien la lee (<c>GetAsync</c>, handler de <c>WatchAsync</c>).</summary>
/// <param name="Key">La clave.</param>
/// <param name="TenantId">
/// El tenant del subject. No estaba en el plan y es necesario: un <c>watch</c> sin tenant recibe <c>cli-1</c> de varios, y la
/// clave sola mezclaría los tenants en la copia local.
/// </param>
/// <param name="Deleted"><see langword="true"/> si es una lápida.</param>
/// <param name="Data">El <c>data</c> del evento; <see langword="null"/> si <paramref name="Deleted"/>.</param>
/// <param name="AggregateVersion">La versión del valor (o la de la lápida).</param>
/// <param name="Revision">La secuencia del stream: dice QUÉ mensaje es; no ordena versiones ni se compara entre contratos.</param>
public sealed record StateEntry(
    string Key,
    string TenantId,
    bool Deleted,
    JsonElement? Data,
    long AggregateVersion,
    long Revision)
{
    /// <summary>Decodifica <see cref="Data"/> en el tipo de la aplicación (<see langword="default"/> si es una lápida).</summary>
    public T? DataAs<T>() => Data is { } d ? d.Deserialize<T>(Envelope.JsonOptions) : default;
}

/// <summary>El resultado de <c>put</c> y <c>delete</c>. No hay un tercer <c>outcome</c>: un conflicto agotado es un error.</summary>
public enum StateOutcome
{
    /// <summary>Se aplicó.</summary>
    Ok,

    /// <summary>La versión no avanzaba: NO es un error.</summary>
    Stale,
}

/// <summary>Resultado de <c>put</c> y <c>delete</c> — 10-state.md §3.5.</summary>
/// <param name="Applied"><see langword="true"/> si se escribió.</param>
/// <param name="Outcome"><c>ok</c> o <c>stale</c>.</param>
/// <param name="Revision">
/// <c>ok</c>: la secuencia del mensaje nuevo. <c>stale</c>: la del mensaje ACTUAL, el que la hizo obsoleta.
/// </param>
public sealed record StateWriteResult(bool Applied, StateOutcome Outcome, long Revision);

/// <summary>Ajustes de un <c>put</c>.</summary>
public sealed record StatePutOptions
{
    /// <summary>Tenant. Si no, el del evento en curso y, si no, el de <c>ConnectAsync</c>. Sin ninguno: <c>INVALID_TENANT_FOR_STATE</c>.</summary>
    public string? TenantId { get; init; }

    /// <summary><c>public</c>, <c>internal</c> o <c>confidential</c> (el default: NO el de la conexión). <c>restricted</c> se rechaza.</summary>
    public DataClassification? Classification { get; init; }

    /// <summary>Instante en que ocurrió el hecho, si no es «ahora».</summary>
    public DateTimeOffset? Time { get; init; }
}

/// <summary>Ajustes de un <c>delete</c>.</summary>
public sealed record StateDeleteOptions
{
    /// <summary>La versión DE LA LÁPIDA. Obligatoria: debe superar la del valor que borra.</summary>
    public required long AggregateVersion { get; init; }

    /// <summary>Tenant. Como en <see cref="StatePutOptions.TenantId"/>.</summary>
    public string? TenantId { get; init; }

    /// <summary>Instante en que ocurrió el hecho, si no es «ahora».</summary>
    public DateTimeOffset? Time { get; init; }
}

/// <summary>Ajustes de un <c>get</c>.</summary>
public sealed record StateGetOptions
{
    /// <summary>Tenant. Como en <see cref="StatePutOptions.TenantId"/>.</summary>
    public string? TenantId { get; init; }
}

/// <summary>Ajustes de un <c>watch</c>.</summary>
public sealed record StateWatchOptions
{
    /// <summary>
    /// Filtra por este tenant (y el durable lleva su sufijo). Si no, el de <c>ConnectAsync</c> —que <b>siempre</b> filtra, también
    /// si vale <c>system</c>: aquí es un tenant como cualquier otro—. Sin ninguno se observan todos, salvo con
    /// <see cref="TenantIsolation.Strict"/>. <b>Nunca el del evento en curso</b> (a diferencia de <c>put</c>, <c>delete</c> y <c>get</c>):
    /// un <c>watch</c> no corre dentro de un handler (10-state.md §2.4). Un tenant por <c>watch</c>: cada llamada es un durable.
    /// </summary>
    public string? TenantId { get; init; }

    /// <summary>
    /// Se invoca UNA vez por <c>watch</c> —no una por reconexión, ni de nuevo tras un <c>flux state resync</c>— cuando el handler ya
    /// ha procesado todo lo que había pendiente al abrirlo. Con el contrato vacío, de inmediato y antes de entregar nada.
    /// </summary>
    public Action? OnSynced { get; init; }
}

/// <summary>Metadatos de ESTA entrega de una entrada de estado.</summary>
/// <param name="Attempt">Número de entrega, empezando en 1.</param>
/// <param name="MaxAttempts">El techo del consumidor.</param>
/// <param name="Subject">El subject de NATS de la entrada (<c>state.clientes.cliente.v1.acme.cli-1</c>).</param>
/// <param name="Contract">El contrato observado (<c>clientes.cliente.v1</c>).</param>
public sealed record StateDelivery(int Attempt, int MaxAttempts, string Subject, string Contract);

/// <summary>
/// Procesa una entrada de estado. Volver sin excepción la ACK-ea; lanzar la clasifica (04-errors.md). El handler DEBE ser
/// idempotente y no asumir orden (<c>UPDATE … WHERE aggregate_version &lt; $n</c>) y, ante <see cref="StateEntry.Deleted"/>, DEBE
/// borrar su copia local (10-state.md §5.3).
/// </summary>
public delegate Task StateHandler(StateEntry entry, StateDelivery delivery, CancellationToken cancellationToken);

/// <summary>Un <c>watch</c> vivo.</summary>
public interface IStateSubscription : IAsyncDisposable
{
    /// <summary>El contrato observado.</summary>
    string Contract { get; }

    /// <summary>Nombre del durable.</summary>
    string Durable { get; }

    /// <summary>El tenant por el que filtra, si filtra.</summary>
    string? TenantId { get; }

    /// <summary>Se completa cuando se vio todo lo que había al abrir (<see cref="StateWatchOptions.OnSynced"/>).</summary>
    Task Synced { get; }

    /// <summary>Detiene la entrega. Lo en vuelo se reentregará (la idempotencia lo cubre).</summary>
    Task UnsubscribeAsync();
}

/// <summary>
/// <c>flux_consumer_pending</c> de un contrato de estado es la SUMA de los durables del servicio, no el valor del último
/// sondeado (08-observability.md §2.4): un <c>watch</c> por tenant crea un durable por tenant, y todos publican las MISMAS
/// etiquetas —el <c>consumer</c> va SIN el sufijo del tenant—: sin sumar, se pisarían entre sí.
/// </summary>
internal sealed class PendingSum
{
    private readonly object _gate = new();
    private readonly Dictionary<string, Dictionary<string, long>> _series = new(StringComparer.Ordinal);

    /// <summary>Anota el <c>pending</c> de un durable y devuelve la suma de la serie.</summary>
    public long Note(string serie, string durable, long pending)
    {
        lock (_gate)
        {
            if (!_series.TryGetValue(serie, out var d))
            {
                _series[serie] = d = new Dictionary<string, long>(StringComparer.Ordinal);
            }

            d[durable] = pending;
            return d.Values.Sum();
        }
    }

    /// <summary>Un durable deja de sumar (su <c>watch</c> paró). Devuelve la suma que queda, o <see langword="null"/>.</summary>
    public long? Drop(string serie, string durable)
    {
        lock (_gate)
        {
            if (!_series.TryGetValue(serie, out var d) || !d.Remove(durable))
            {
                return null;
            }

            if (d.Count == 0)
            {
                _series.Remove(serie);
            }

            return d.Values.Sum();
        }
    }
}

/// <summary>Cuenta cuántas entregas de un <c>watch</c> siguen en vuelo y cuándo se alcanzó «lo que había al abrir».</summary>
internal sealed class SyncTracker
{
    private readonly object _gate = new();
    private readonly Action _fire;
    private bool _done;
    private int _inflight;
    private bool _sawLast;

    public SyncTracker(Action fire) => _fire = fire;

    /// <summary>Al abrir: nada pendiente.</summary>
    public void Immediate()
    {
        lock (_gate)
        {
            if (_done)
            {
                return;
            }

            _done = true;
        }

        _fire();
    }

    /// <summary><c>pending</c> es el número que el broker pone en los metadatos de cada entrega: cuántas quedan por entregar. 0 = la última de la foto (A-35).</summary>
    public void Started(ulong pending)
    {
        lock (_gate)
        {
            _inflight++;
            if (pending == 0)
            {
                _sawLast = true;
            }
        }
    }

    /// <summary>Con éxito o con error: una entrada RETRYABLE que falla NO retrasa <c>onSynced</c> (10-state.md §5.5).</summary>
    public void Finished()
    {
        var dispara = false;
        lock (_gate)
        {
            _inflight--;
            if (!_done && _sawLast && _inflight == 0)
            {
                _done = true;
                dispara = true;
            }
        }

        if (dispara)
        {
            _fire();
        }
    }
}

/// <summary>
/// Estado replicado: <c>PutAsync</c>, <c>DeleteAsync</c>, <c>GetAsync</c> y <c>WatchAsync</c> — el valor ACTUAL de un agregado,
/// que su dueño publica y los demás observan. Sustituye a <c>GET /clientes/123</c>, a la carga inicial y al polling de datos.
/// </summary>
public sealed class StateClient
{
    private static readonly TimeSpan DirectGetTimeout = TimeSpan.FromSeconds(5);

    private readonly FluxBus _bus;
    private readonly PendingSum _pending = new();
    private readonly ConcurrentDictionary<string, byte> _dlqChecked = new(StringComparer.Ordinal);

    internal StateClient(FluxBus bus) => _bus = bus;

    /// <summary>Un gancho para pruebas: ver <see cref="WriteHook"/>.</summary>
    internal WriteHook? Hook { get; set; }

    // ─── escribir ────────────────────────────────────────────────────────────

    /// <summary>
    /// Escribe el valor actual de una clave. <b>Condicional</b>: no escribe una versión que no avanza (<c>stale</c>, que NO es un
    /// error) y publica con <c>Nats-Expected-Last-Subject-Sequence</c>, de modo que dos instancias del dueño no pisan un valor
    /// nuevo con uno viejo. <paramref name="data"/> DEBE llevar <c>aggregateVersion</c> (entero ≥ 1): lo pone el dueño, no el SDK.
    /// </summary>
    /// <exception cref="ClassifiedException">Los siete códigos de estado, <c>RESERVED_DOMAIN</c> y <c>INVALID_DATACLASSIFICATION</c>.</exception>
    public async Task<StateWriteResult> PutAsync(
        string contract,
        string key,
        object data,
        StatePutOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new StatePutOptions();
        // §3.6, en este orden y SIN red: el primero que falla gana.
        var c = StateProtocol.ParseContract(contract);
        StateProtocol.AssertKey(key);
        var tenant = TenantFor(options.TenantId);
        var element = Envelope.ToElement(data, nonObject: false);
        var version = CountingInvalid(c.Contract, () => StateProtocol.AggregateVersionOf(element));
        var classification = StateProtocol.AssertClassification(options.Classification);

        var id = Hook?.IdOverride ?? Protocol.NewEventId().ToString();
        var inherited = FluxContext.CurrentContext;
        var subject = StateProtocol.Subject(c.Contract, tenant, key);
        var evento = StateProtocol.BuildEvent(new StateProtocol.EventInput
        {
            Contract = c.Contract,
            TenantId = tenant,
            Key = key,
            Id = id,
            Source = _bus.Source,
            Time = Envelope.FormatTime(options.Time ?? DateTimeOffset.UtcNow),
            // Un estado derivado dentro de un handler hereda el contexto del evento que lo causó.
            CorrelationId = inherited?.CorrelationId ?? id,
            ProducerVersion = _bus.Options.Version,
            Classification = classification,
            Data = element,
            DataSchema = SchemaFor(c.Contract),
            CausationId = inherited?.CausationId,
            TraceParent = inherited?.TraceParent ?? _bus.CurrentTraceparent(),
            TraceState = inherited?.TraceState ?? FluxContext.ActiveTracestate(),
        });

        // L3 ANTES de leer ni escribir; reporta TODOS los errores y no copia valores a su mensaje.
        CountingInvalid(c.Contract, () => _bus.Options.Validation?.Validator?.Check(evento, subject));
        return await WriteAsync(c, tenant, key, subject, version, evento, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Borra una clave: publica una <b>lápida</b> en su subject, que sustituye al valor (el dato personal ya no está en el
    /// stream). Es <c>put</c> de una lápida: mismo algoritmo, mismas reglas de versión y mismo resultado. Borrar una clave que
    /// nunca existió escribe la lápida.
    /// </summary>
    public async Task<StateWriteResult> DeleteAsync(
        string contract,
        string key,
        StateDeleteOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        var c = StateProtocol.ParseContract(contract);
        StateProtocol.AssertKey(key);
        var tenant = TenantFor(options.TenantId);
        var versionElement = JsonSerializer.SerializeToElement(options.AggregateVersion);
        var version = CountingInvalid(c.Contract, () => StateProtocol.AggregateVersionOf(
            JsonSerializer.SerializeToElement(new Dictionary<string, JsonElement> { ["aggregateVersion"] = versionElement })));

        var id = Hook?.IdOverride ?? Protocol.NewEventId().ToString();
        var inherited = FluxContext.CurrentContext;
        var subject = StateProtocol.Subject(c.Contract, tenant, key);
        var evento = StateProtocol.BuildTombstone(new StateProtocol.TombstoneInput
        {
            Contract = c.Contract,
            TenantId = tenant,
            Key = key,
            Id = id,
            Source = _bus.Source,
            Time = Envelope.FormatTime(options.Time ?? DateTimeOffset.UtcNow),
            CorrelationId = inherited?.CorrelationId ?? id,
            ProducerVersion = _bus.Options.Version,
            AggregateVersion = versionElement,
            CausationId = inherited?.CausationId,
            TraceParent = inherited?.TraceParent ?? _bus.CurrentTraceparent(),
            TraceState = inherited?.TraceState ?? FluxContext.ActiveTracestate(),
        });
        // La lápida valida contra su esquema fijo (en `schemas` del bundle, no en el índice `state`).
        CountingInvalid(c.Contract, () => _bus.Options.Validation?.Validator?.Check(evento, subject));
        return await WriteAsync(c, tenant, key, subject, version, evento, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// El algoritmo de 10-state.md §3.2. El evento se construyó UNA vez: <c>id</c>, <c>time</c> y bytes son los mismos en todos
    /// los intentos. Con un <c>id</c> nuevo por intento, el paso 1b no reconocería nunca la propia escritura.
    /// </summary>
    private async Task<StateWriteResult> WriteAsync(
        StateProtocol.ParsedContract c,
        string tenant,
        string key,
        string subject,
        long version,
        FluxEvent toSend,
        CancellationToken ct)
    {
        _ = tenant;
        var stream = StateProtocol.StreamName(c.Domain);
        var firmado = _bus.Options.Signer?.Sign(toSend) ?? toSend;
        var bytes = Envelope.Serialize(firmado);
        var id = firmado.Id;

        for (var intento = 1; intento <= StateProtocol.MaxAttempts; intento++)
        {
            // 1. Leer el último mensaje de la clave. 404 → S = 0 y no hay versión.
            var actual = await ReadRawAsync(stream, subject, ct).ConfigureAwait(false);
            var s = actual?.Seq ?? 0UL;

            // 1b. ¿El mensaje leído es EL MÍO? Entonces ya se aplicó y solo se perdió el acuse. Sin este paso, un acuse perdido
            // convierte una escritura buena en un `stale` con su propio dato: el reenvío da «wrong last sequence» porque lo que
            // cambió la secuencia fue el propio mensaje (A-33). Se reconoce por el `id` porque el broker conserva las cabeceras.
            if (actual is not null && actual.Ids.Contains(id))
            {
                _bus.Metrics.StateWrite(c.Contract, StateWriteOutcome.Ok);
                Hook?.OnOwnWrite?.Invoke(intento);
                Hook?.OnDone?.Invoke(intento);
                return new StateWriteResult(true, StateOutcome.Ok, (long)actual.Seq);
            }

            // 2. Una versión que no avanza NO se escribe, y una igual tampoco avanza. No es un error.
            if (actual?.Version is { } v && version <= v)
            {
                _bus.Metrics.StateWrite(c.Contract, StateWriteOutcome.Stale);
                Hook?.OnDone?.Invoke(intento);
                return new StateWriteResult(false, StateOutcome.Stale, (long)actual.Seq);
            }

            if (Hook?.BeforePublish is { } antes)
            {
                await antes(intento, subject).ConfigureAwait(false);
            }

            // 3. Siempre por JetStream y siempre con la cabecera de secuencia: un publish de CORE a un subject de estado
            // sobrescribe sin acuse ni chequeo (A-45). `S = 0` = «la clave no existe»: crea si el subject no tiene mensaje y falla
            // si ya lo tiene, y eso es lo que hace que dos escritores concurrentes sobre una clave nueva no se pisen (A-33).
            try
            {
                if (Hook?.FailPublish?.Invoke(intento) is { } fallo)
                {
                    // Un error de secuencia que NO se aplicó: el que un R3 da bajo carrera (10164) y que un solo nodo no produce.
                    throw new NatsJSApiException(new ApiError { Code = 400, ErrCode = fallo, Description = "simulado" });
                }

                var ack = await _bus.GuardAsync(
                    new[] { subject },
                    token => _bus.JetStream.PublishAsync(
                        subject,
                        bytes,
                        opts: new NatsJSPubOpts { MsgId = id, ExpectedLastSubjectSequence = s },
                        cancellationToken: token).AsTask(),
                    "escribir el estado",
                    ct).ConfigureAwait(false);

                if (Hook?.LoseAck?.Invoke(intento) == true)
                {
                    // El publish SE APLICÓ, pero quien lo hizo ve un error de secuencia: lo que ocurre cuando la librería reenvía
                    // el publish tras perder el acuse.
                    throw new NatsJSApiException(
                        new ApiError { Code = 400, ErrCode = Hook.LoseAckErrCode, Description = "wrong last sequence: simulado" });
                }

                if (ack.Error is { } e)
                {
                    throw new NatsJSApiException(e);
                }

                _bus.Metrics.StateWrite(c.Contract, StateWriteOutcome.Ok);
                Hook?.OnDone?.Invoke(intento);
                return new StateWriteResult(true, StateOutcome.Ok, (long)ack.Seq);
            }
            catch (NatsJSApiException e) when (IsSequenceConflict(e))
            {
                // 4. Alguien escribió entre el 1 y el 3: volver a 1. DOS códigos, no uno. Un poco de jitter es opcional (§3.3) y
                // barato: sin él, tres instancias que chocan vuelven a chocar a la vez.
                if (intento < StateProtocol.MaxAttempts)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(Random.Shared.NextDouble() * 12 * intento), ct).ConfigureAwait(false);
                }
            }
            catch (NatsNoRespondersException e)
            {
                throw NotProvisioned(stream, subject, e);
            }
        }

        _bus.Metrics.StateWrite(c.Contract, StateWriteOutcome.Conflict);
        Hook?.OnDone?.Invoke(StateProtocol.MaxAttempts);
        throw new StateConflictException(c.Contract, key, StateProtocol.MaxAttempts);
    }

    /// <summary>¿Es el error de la API de JetStream un conflicto de secuencia (10071 o 10164)?</summary>
    internal static bool IsSequenceConflict(NatsJSApiException e) =>
        StateProtocol.ConflictCodes.Contains(e.Error.ErrCode);

    // ─── leer ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Una lectura puntual por <c>DIRECT.GET</c> sobre el subject exacto: no hay consumidor ni acuse. <see langword="null"/> si la
    /// clave no existe <b>o</b> es una lápida. Es una lectura de RÉPLICA (§3.7): con <c>replicas: 3</c> PUEDE devolver el valor
    /// anterior justo después de un <c>put</c>; quien necesite su propia escritura la deriva de <c>Revision</c>, no relee.
    /// </summary>
    public async Task<StateEntry?> GetAsync(
        string contract,
        string key,
        StateGetOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var c = StateProtocol.ParseContract(contract);
        StateProtocol.AssertKey(key);
        var tenant = TenantFor(options?.TenantId);
        var subject = StateProtocol.Subject(c.Contract, tenant, key);
        var stored = await ReadRawAsync(StateProtocol.StreamName(c.Domain), subject, cancellationToken).ConfigureAwait(false);
        if (stored is null)
        {
            return null; // 404 `Message Not Found`
        }

        var (_, entry, _) = Decode(stored.Data, c.Contract, tenant, key, (long)stored.Seq, subject, holder: null);
        return entry.Deleted ? null : entry;
    }

    private sealed record Stored(ulong Seq, byte[] Data, IReadOnlyList<string> Ids, long? Version);

    /// <summary>
    /// <c>DIRECT.GET</c> por subject — la forma por SUBJECT, que se puede confinar por tenant con una ACL. La de cuerpo
    /// (<c>{"seq": n}</c>) lee cualquier mensaje y ninguna ACL la ve: no se usa. Un stream que no existe, o sin
    /// <c>allow_direct</c>, NO da 404: no contesta nadie, y los dos casos se ven igual (A-32) → <c>NOT_PROVISIONED</c>.
    /// </summary>
    private async Task<Stored?> ReadRawAsync(string stream, string subject, CancellationToken ct)
    {
        var direct = $"$JS.API.DIRECT.GET.{stream}.{subject}";
        NatsMsg<byte[]> r;
        try
        {
            r = await _bus.GuardAsync(
                new[] { direct },
                token => _bus.Connection.RequestAsync<byte[], byte[]>(
                    direct,
                    Array.Empty<byte>(),
                    replyOpts: new NatsSubOpts { Timeout = DirectGetTimeout },
                    cancellationToken: token).AsTask(),
                "leer el estado",
                ct).ConfigureAwait(false);
        }
        catch (NatsNoRespondersException e)
        {
            throw NotProvisioned(stream, subject, e);
        }

        var h = r.Headers;
        if (h?.Code == 404)
        {
            return null;
        }

        if (h is { Code: >= 400 })
        {
            throw new InvalidOperationException($"DIRECT.GET de {subject} contestó {h.Code} {h.MessageText}");
        }

        var seq = h is not null && h.TryGetValue("Nats-Sequence", out var sv) && ulong.TryParse(sv.ToString(), out var s) ? s : 0UL;
        var ids = new List<string>();
        if (h is not null && h.TryGetValue("Nats-Msg-Id", out var mid) && !StringValues.IsNullOrEmpty(mid))
        {
            ids.Add(mid.ToString());
        }

        var data = r.Data ?? Array.Empty<byte>();

        // Un valor ILEGIBLE no bloquea la clave (§3.3): si el cuerpo no es JSON, o no tiene un `aggregateVersion` válido, se trata
        // como «sin versión» y se sobrescribe con el chequeo de secuencia sobre SU `S`. De lo contrario una entrada rota dejaría la
        // clave inescribible para siempre, porque ningún servicio puede purgar el stream (06-security.md §3.5).
        try
        {
            using var doc = JsonDocument.Parse(data);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                return new Stored(seq, data, ids, null);
            }

            if (StateProtocol.TryLast(doc.RootElement, "id", out var idEl) && idEl.ValueKind == JsonValueKind.String)
            {
                ids.Add(idEl.GetString()!);
            }

            long? version = null;
            if (StateProtocol.TryLast(doc.RootElement, "data", out var d) && d.ValueKind == JsonValueKind.Object)
            {
                try
                {
                    version = StateProtocol.AggregateVersionOf(d);
                }
                catch (MissingAggregateVersionException)
                {
                    // sin versión válida: se sobrescribe
                }
            }

            return new Stored(seq, data, ids, version);
        }
        catch (JsonException)
        {
            return new Stored(seq, data, ids, null);
        }
    }

    // ─── watch ───────────────────────────────────────────────────────────────

    private sealed record WatchState(
        string Contract,
        string? Tenant,
        string Stream,
        string Durable,
        string ConsumerLabel,
        string SubjectLabel,
        StateHandler Handler,
        SyncTracker Sync);

    /// <summary>
    /// Foto completa y después los cambios, por el MISMO canal: un consumidor durable de <c>STATE_&lt;DOM&gt;</c> con
    /// <c>deliver_policy: last_per_subject</c>. Un consumidor que estuvo caído recibe solo el último valor de cada clave que cambió;
    /// un valor reemplazado mientras estaba en vuelo NO se reentrega (A-35, A-36).
    /// </summary>
    public async Task<IStateSubscription> WatchAsync(
        string contract,
        StateHandler handler,
        StateWatchOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        options ??= new StateWatchOptions();
        var c = StateProtocol.ParseContract(contract);
        var o = _bus.Options;
        var tenant = WatchTenant(options.TenantId);
        if (o.TenantIsolation == TenantIsolation.Strict && tenant is null)
        {
            throw new TenantIsolationException(
                "con TenantIsolation.Strict, todo `watch` filtra por tenant, y no hay TenantId ni en ConnectAsync ni en WatchAsync " +
                $"(state.{c.Contract}). Un filtro que hay que acordarse de poner es un filtro que alguien olvidará (09-multitenancy.md §3)");
        }

        var stream = StateProtocol.StreamName(c.Domain);
        var durable = StateProtocol.DurableName(o.Service, c.Contract, tenant);
        var sincronizado = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var w = new WatchState(
            c.Contract,
            tenant,
            stream,
            durable,
            StateProtocol.DurableName(o.Service, c.Contract),
            "state." + c.Contract,
            handler,
            new SyncTracker(() => FireSynced(options.OnSynced, sincronizado)));

        // El SDK comprueba la DLQ al ARRANCAR, no cuando llega el primer mensaje envenenado.
        await EnsureStateDlqAsync(c.Domain, cancellationToken).ConfigureAwait(false);

        var requested = new ConsumerConfig(durable)
        {
            DurableName = durable,
            FilterSubject = StateProtocol.FilterSubject(c.Contract, tenant),
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            // ack_wait DEBE ser backoff[0]: JetStream lo sobrescribe con ese valor sin avisar.
            AckWait = Protocol.DefaultAckWait,
            MaxDeliver = Protocol.DefaultMaxDeliver,
            Backoff = Protocol.CanonicalBackoff.Select(t => t.Ticks * 100L).ToArray(),
            MaxAckPending = Protocol.DefaultMaxAckPending,
            DeliverPolicy = ConsumerConfigDeliverPolicy.LastPerSubject,
            ReplayPolicy = ConsumerConfigReplayPolicy.Instant,
        };

        var consumer = await EnsureWatchConsumerAsync(stream, durable, requested, cancellationToken).ConfigureAwait(false);
        AssertWatchConfigHonored(durable, requested, consumer.Info.Config);

        // El pull denegado NO da error y deja un watch inerte que parece sano: se comprueba antes de devolver (06-security.md §3.7).
        await _bus.EnsurePullAllowedAsync(stream, durable, cancellationToken).ConfigureAwait(false);

        var watch = new Watch(this, w, requested);
        if (Hook?.AfterOpen is { } despues)
        {
            await despues().ConfigureAwait(false);
        }

        // Stream vacío, o durable al día: `onSynced` de INMEDIATO, antes de entregar nada y aunque no llegue ninguna entrada
        // después. Esperar a una que quizá no llegue nunca dejaría al servicio «sincronizando» para siempre (10-state.md §5.5,
        // punto 2). `num_ack_pending` también: lo entregado y sin confirmar sigue pendiente de procesar.
        if (consumer.Info.NumPending == 0 && consumer.Info.NumAckPending == 0)
        {
            w.Sync.Immediate();
        }

        watch.Start(consumer, sincronizado.Task);
        _bus.Track(watch, watch.StopAsync);
        return watch;
    }

    /// <summary>
    /// El mismo <c>watch</c> como flujo asíncrono: <c>await foreach (var e in bus.State.WatchEntriesAsync(contrato)) { … }</c>. La entrada se
    /// <b>confirma cuando el cuerpo del bucle vuelve a pedir la siguiente</b>: si el cuerpo lanza o se sale con <c>break</c>, no se confirma y se
    /// reentrega (la idempotencia del consumidor lo cubre). Con contrapresión: el SDK no entrega la siguiente hasta que se pide.
    /// </summary>
    /// <remarks>
    /// El <paramref name="cancellationToken"/> cancela la espera y el <c>watch</c> se cierra al terminar el bucle, por cualquier camino.
    /// <see cref="StateWatchOptions.OnSynced"/> sigue disponible; <see cref="IStateSubscription.Synced"/> no se expone aquí.
    /// </remarks>
    public async IAsyncEnumerable<StateEntry> WatchEntriesAsync(
        string contract,
        StateWatchOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var canal = Channel.CreateBounded<(StateEntry Entry, TaskCompletionSource Hecho)>(
            new BoundedChannelOptions(1) { SingleReader = true, FullMode = BoundedChannelFullMode.Wait });

        // El handler entrega la entrada al bucle y ESPERA a que su cuerpo termine: así el `ack` llega después del cuerpo. Si el bucle se
        // abandona, el cierre del watch cancela esa espera con el token del watch, y el mensaje no se confirma ni se rechaza.
        await using var watch = await WatchAsync(
            contract,
            async (entry, _, ct) =>
            {
                var hecho = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                await canal.Writer.WriteAsync((entry, hecho), ct).ConfigureAwait(false);
                await hecho.Task.WaitAsync(ct).ConfigureAwait(false);
            },
            options,
            cancellationToken).ConfigureAwait(false);

        while (await canal.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (canal.Reader.TryRead(out var item))
            {
                yield return item.Entry;
                item.Hecho.TrySetResult();
            }
        }
    }

    private void FireSynced(Action? callback, TaskCompletionSource sincronizado)
    {
        sincronizado.TrySetResult();
        if (callback is null)
        {
            return;
        }

        try
        {
            callback();
        }
        catch (Exception e)
        {
            _bus.Options.Logger?.Error($"[flux] OnSynced lanzó: {e.Message}");
        }
    }

    private async Task<INatsJSConsumer> EnsureWatchConsumerAsync(
        string stream,
        string durable,
        ConsumerConfig requested,
        CancellationToken ct)
    {
        // `watch` NO necesita `STREAM.INFO.STATE_<DOM>` —que lista todas las claves y todos los tenants (A-39)—: `CONSUMER.INFO`
        // distingue «el consumidor no existe» (10014) de «el stream no existe» (10059) (A-48), que es todo lo que hace falta para
        // dar `NOT_PROVISIONED` o `PERMISSION_DENIED`.
        try
        {
            return await _bus.EnsureConsumerAsync(stream, durable, requested, ct).ConfigureAwait(false);
        }
        catch (NatsJSApiException e) when (e.Error.ErrCode == 10059)
        {
            throw NotProvisionedStream(stream, $"$JS.API.CONSUMER.INFO.{stream}.{durable}", e);
        }
    }

    /// <summary>
    /// Como <see cref="ConsumerConfigVerifier"/>, más lo propio del estado: la política de entrega y el filtro. Un durable
    /// aprovisionado con otro filtro entregaría un conjunto distinto del pedido (otro tenant, todos los tenants) sin ningún error.
    /// </summary>
    private void AssertWatchConfigHonored(string durable, ConsumerConfig requested, ConsumerConfig effective)
    {
        try
        {
            ConsumerConfigVerifier.AssertHonored(
                durable,
                ConsumerConfigSnapshot.Canonical(Protocol.DefaultMaxAckPending),
                FluxBus.Snapshot(effective));
        }
        catch (ConsumerConfigMismatchException e)
        {
            throw Extend(durable, requested, effective, e.Differences);
        }

        var extra = ExtraDifferences(requested, effective);
        if (extra.Count > 0)
        {
            throw new ConsumerConfigMismatchException(durable, extra);
        }
    }

    private static ConsumerConfigMismatchException Extend(
        string durable,
        ConsumerConfig requested,
        ConsumerConfig effective,
        IReadOnlyList<ConfigDifference> diffs)
    {
        var todas = diffs.Concat(ExtraDifferences(requested, effective)).ToList();
        return new ConsumerConfigMismatchException(durable, todas);
    }

    private static List<ConfigDifference> ExtraDifferences(ConsumerConfig requested, ConsumerConfig effective)
    {
        var diffs = new List<ConfigDifference>();
        if (effective.DeliverPolicy != requested.DeliverPolicy)
        {
            diffs.Add(new ConfigDifference("deliver_policy", requested.DeliverPolicy.ToString(), effective.DeliverPolicy.ToString()));
        }

        var filtros = effective.FilterSubjects?.ToList() ?? new List<string>();
        var efectivo = effective.FilterSubject ?? (filtros.Count == 1 ? filtros[0] : null);
        if (!string.Equals(efectivo, requested.FilterSubject, StringComparison.Ordinal) || filtros.Count > 1)
        {
            diffs.Add(new ConfigDifference(
                "filter_subject", requested.FilterSubject ?? "-", efectivo ?? string.Join(",", filtros)));
        }

        return diffs;
    }

    /// <summary>
    /// <c>DLQ_&lt;DOM&gt;</c> debe capturar <c>dlq.state.&lt;dom&gt;.&gt;</c>. Un dominio solo de eventos conserva su <c>DLQ_&lt;DOM&gt;</c> de la
    /// v1.0 sin tocarlo, y NO lo hace el SDK sobre uno existente: lo hace <c>flux provision</c>. Si no captura, se AVISA al abrir,
    /// y no cuando el primer mensaje muera.
    /// </summary>
    private async Task EnsureStateDlqAsync(string domain, CancellationToken ct)
    {
        var name = Protocol.DlqStreamName(domain);
        if (_dlqChecked.ContainsKey(name))
        {
            return;
        }

        var o = _bus.Options;
        var muestra = $"dlq.state.{domain}.x.v1";
        try
        {
            var stream = await _bus.GuardAsync(
                new[] { "$JS.API.STREAM.INFO." + name },
                token => _bus.JetStream.GetStreamAsync(name, cancellationToken: token).AsTask(),
                "abrir el stream `" + name + "`",
                ct).ConfigureAwait(false);
            var subjects = stream.Info.Config.Subjects ?? new List<string>();
            if (!subjects.Any(s => SubjectMatches(s, muestra)))
            {
                o.Logger?.Warn(
                    $"[flux] {name} no captura \"dlq.state.{domain}.>\": una entrada de estado que muera no se podrá guardar y se " +
                    "reentregará. Un dominio con estado lo necesita; lo amplía `flux provision` y lo audita `flux doctor` (10-state.md §6).");
            }
        }
        catch (NatsJSApiException e) when (e.Error.ErrCode == 10059 || (e.Error.Code == 404 && e.Error.ErrCode == 0))
        {
            // Desarrollo: se crea con LOS DOS subjects. En producción lo aprovisiona `flux provision`.
            var createSubject = "$JS.API.STREAM.CREATE." + name;
            try
            {
                await _bus.GuardAsync(
                    new[] { createSubject },
                    token => _bus.JetStream.CreateStreamAsync(
                        new StreamConfig(name, new[] { $"dlq.{domain}.>", $"dlq.state.{domain}.>" })
                        {
                            Storage = StreamConfigStorage.File,
                            Retention = StreamConfigRetention.Limits,
                            Discard = StreamConfigDiscard.Old,
                            MaxAge = Protocol.DlqStreamMaxAge,
                            NumReplicas = o.StreamReplicas,
                        },
                        token).AsTask(),
                    "crear el stream `" + name + "`",
                    ct).ConfigureAwait(false);
            }
            catch (PermissionDeniedException denied) when (denied.DeniedSubject == createSubject)
            {
                throw new NotProvisionedException(ResourceKind.Stream, name, null, o.Service, denied.DeniedSubject, denied);
            }
        }

        _dlqChecked[name] = 0;
    }

    /// <summary>Un patrón de subject de NATS (<c>*</c>, <c>&gt;</c>) contra un subject concreto.</summary>
    internal static bool SubjectMatches(string pattern, string subject)
    {
        var p = pattern.Split('.');
        var s = subject.Split('.');
        for (var i = 0; i < p.Length; i++)
        {
            if (p[i] == ">")
            {
                return s.Length > i;
            }

            if (i >= s.Length)
            {
                return false;
            }

            if (p[i] != "*" && p[i] != s[i])
            {
                return false;
            }
        }

        return p.Length == s.Length;
    }

    // ─── un watch vivo ───────────────────────────────────────────────────────

    private sealed class Watch : IStateSubscription
    {
        private readonly StateClient _client;
        private readonly WatchState _w;
        private readonly ConsumerConfig _requested;
        private readonly CancellationTokenSource _cts = new();
        private Task? _loop;
        private Task? _poll;
        private int _stopped;

        public Watch(StateClient client, WatchState w, ConsumerConfig requested)
        {
            _client = client;
            _w = w;
            _requested = requested;
        }

        public string Contract => _w.Contract;

        public string Durable => _w.Durable;

        public string? TenantId => _w.Tenant;

        public Task Synced { get; private set; } = Task.CompletedTask;

        public void Start(INatsJSConsumer consumer, Task synced)
        {
            Synced = synced;
            _loop = Task.Run(() => ConsumeAsync(consumer), CancellationToken.None);
            if (FluxBus.PendingPollEnabled(_client._bus.Options.PendingPollInterval, _client._bus.Metrics))
            {
                _poll = Task.Run(PollPendingAsync, CancellationToken.None);
            }
        }

        private async Task PollPendingAsync()
        {
            try
            {
                using var timer = new PeriodicTimer(_client._bus.Options.PendingPollInterval);
                while (await timer.WaitForNextTickAsync(_cts.Token).ConfigureAwait(false))
                {
                    try
                    {
                        var c = await _client._bus.JetStream.GetConsumerAsync(_w.Stream, _w.Durable, _cts.Token).ConfigureAwait(false);
                        _client.NotePending(_w, (long)c.Info.NumPending);
                    }
                    catch (Exception e) when (e is not OperationCanceledException)
                    {
                        // La telemetría no debe afectar al consumo.
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        /// <summary>
        /// El bucle de un <c>watch</c>: entrega cada mensaje a <c>Dispatch</c> y, si el durable se <b>borra</b> mientras el watch vive
        /// (<c>flux state resync</c>: borrar y recrear), <b>reabre el consumo</b> — 10-state.md §5.2.
        /// </summary>
        private async Task ConsumeAsync(INatsJSConsumer first)
        {
            var consumer = first;
            var logger = _client._bus.Options.Logger;
            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    await foreach (var m in consumer.ConsumeAsync<byte[]>(cancellationToken: _cts.Token).ConfigureAwait(false))
                    {
                        try
                        {
                            await _client.DispatchAsync(m, _w, _cts.Token).ConfigureAwait(false);
                        }
                        catch (Exception e) when (e is not OperationCanceledException)
                        {
                            // Un fallo inesperado rompería el bucle y el watch dejaría de consumir EN SILENCIO, con la conexión
                            // viva: un consumidor muerto que parece vivo.
                            logger?.Error(
                                $"[flux] fallo inesperado despachando el estado {_w.SubjectLabel}: {e.Message}. " +
                                "El mensaje no se confirma y será reentregado.");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception e)
                {
                    logger?.Warn($"[flux] el consumo de {_w.Durable} terminó con un error ({e.GetType().Name}: {e.Message}).");
                }

                if (_cts.IsCancellationRequested)
                {
                    return;
                }

                // El flujo terminó sin que nadie lo pidiese: el durable se borró (`flux state resync`) o la librería dio por
                // terminado el consumo. Se reabre cuando el durable exista, comprobando su configuración.
                var reabierto = await ReopenAsync().ConfigureAwait(false);
                if (reabierto is null)
                {
                    return;
                }

                consumer = reabierto;
            }
        }

        /// <summary>
        /// Vuelve a abrir el consumo de un durable que se borró bajo el watch. Reintenta con backoff (100 ms … 5 s) hasta que el
        /// durable exista —<c>flux state resync</c> lo recrea a continuación— o el watch pare. Comprueba la configuración como al
        /// abrir: un durable recreado con otro filtro entregaría las claves de otro tenant sin ningún error, y ese no se consume.
        /// </summary>
        private async Task<INatsJSConsumer?> ReopenAsync()
        {
            var logger = _client._bus.Options.Logger;
            logger?.Warn(
                $"[flux] el durable {_w.Durable} se borró con el watch de {_w.SubjectLabel} abierto (¿`flux state resync`?): " +
                "se vuelve a abrir el consumo cuando exista de nuevo.");
            var ultimo = string.Empty;
            for (var intento = 1; !_cts.IsCancellationRequested; intento++)
            {
                try
                {
                    var consumer = await _client._bus.JetStream.GetConsumerAsync(_w.Stream, _w.Durable, _cts.Token).ConfigureAwait(false);
                    _client.AssertWatchConfigHonored(_w.Durable, _requested, consumer.Info.Config);
                    logger?.Warn($"[flux] consumo de {_w.Durable} reabierto (intento {intento}): se entregará la foto otra vez.");
                    return consumer;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception e)
                {
                    // `consumer not found` (10014): el resync está entre el borrado y la creación. Se espera. Se avisa la primera
                    // vez, cada 10 intentos y SIEMPRE que la causa cambia.
                    var motivo = e.Message;
                    if (intento == 1 || intento % 10 == 0 || motivo != ultimo)
                    {
                        logger?.Warn($"[flux] no se pudo reabrir el consumo de {_w.Durable} (intento {intento}): {motivo}. Se reintenta.");
                        ultimo = motivo;
                    }

                    try
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(5_000, 100 * Math.Pow(2, Math.Min(intento - 1, 6)))), _cts.Token)
                            .ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }

            logger?.Warn($"[flux] el watch de {_w.SubjectLabel} se detuvo sin haber podido reabrir el consumo de {_w.Durable}");
            return null;
        }

        public async Task StopAsync()
        {
            if (Interlocked.Exchange(ref _stopped, 1) == 1)
            {
                return;
            }

            await _cts.CancelAsync().ConfigureAwait(false);
            foreach (var t in new[] { _loop, _poll })
            {
                if (t is null)
                {
                    continue;
                }

                try
                {
                    await t.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }

            _client.DropPending(_w);
            _client._bus.Untrack(this);
            _cts.Dispose();
        }

        public Task UnsubscribeAsync() => StopAsync();

        public async ValueTask DisposeAsync() => await StopAsync().ConfigureAwait(false);
    }

    // ─── despacho de una entrada ─────────────────────────────────────────────

    private async Task DispatchAsync(NatsJSMsg<byte[]> m, WatchState w, CancellationToken ct)
    {
        if (m.Metadata is { } meta)
        {
            NotePending(w, (long)meta.NumPending);
            w.Sync.Started(meta.NumPending);
        }
        else
        {
            w.Sync.Started(1);
        }

        try
        {
            await ProcessAsync(m, w, ct).ConfigureAwait(false);
        }
        finally
        {
            w.Sync.Finished();
        }
    }

    private async Task ProcessAsync(NatsJSMsg<byte[]> m, WatchState w, CancellationToken ct)
    {
        var bus = _bus;
        var attempt = m.Metadata?.NumDelivered is { } nd && nd >= 1 ? (int)nd : 1;
        var maxAttempts = Protocol.DefaultMaxDeliver;
        // `state.<dom>.<agr>.v<M>.<tenant>.<clave>`: la identidad la decide EL BROKER, no el sobre.
        var partes = m.Subject.Split('.');
        var tenant = partes.Length > 4 ? partes[4] : string.Empty;
        var key = partes.Length > 5 ? partes[5] : string.Empty;
        var revision = (long)(m.Metadata?.Sequence.Stream ?? 0UL);
        var inicio = Stopwatch.GetTimestamp();
        var holder = new DecodeHolder();

        // WIP mientras el handler vive: evita que la misma entrada se ejecute en concurrencia consigo misma.
        using var wipCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var wip = KeepAliveAsync(m, wipCts.Token);
        try
        {
            var (evento, entry, advertenciaFirma) = Decode(m.Data ?? Array.Empty<byte>(), w.Contract, tenant, key, revision, m.Subject, holder);
            if (advertenciaFirma)
            {
                bus.Metrics.EventConsumed(w.SubjectLabel, w.ConsumerLabel, ConsumeOutcome.InvalidSignature);
            }

            using (FluxContext.Push(FluxContext.FromEvent(evento)))
            {
                await w.Handler(entry, new StateDelivery(attempt, maxAttempts, m.Subject, w.Contract), ct).ConfigureAwait(false);
            }

            await m.AckAsync(cancellationToken: ct).ConfigureAwait(false);
            bus.Metrics.EventConsumed(w.SubjectLabel, w.ConsumerLabel, ConsumeOutcome.Ok);
            bus.Metrics.HandlerDuration(w.SubjectLabel, w.ConsumerLabel, Stopwatch.GetElapsedTime(inicio).TotalSeconds);
        }
        catch (Exception e) when (e is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            await ResolveFailureAsync(m, w, attempt, maxAttempts, key, tenant, revision, holder.Event, e, inicio, ct).ConfigureAwait(false);
        }
        finally
        {
            await wipCts.CancelAsync().ConfigureAwait(false);
            await wip.ConfigureAwait(false);
        }
    }

    private static async Task KeepAliveAsync(NatsJSMsg<byte[]> m, CancellationToken ct)
    {
        try
        {
            using var timer = new PeriodicTimer(Protocol.WorkInProgressInterval);
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                await m.AckProgressAsync(cancellationToken: ct).ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
            // El handler terminó o el mensaje ya está resuelto.
        }
    }

    private async Task ResolveFailureAsync(
        NatsJSMsg<byte[]> m,
        WatchState w,
        int attempt,
        int maxAttempts,
        string key,
        string tenant,
        long revision,
        FluxEvent? parsed,
        Exception error,
        long inicio,
        CancellationToken ct)
    {
        var bus = _bus;
        var classification = bus.Classifier.Classify(error);
        var budget = Classifier.EffectiveBudget(maxAttempts, classification);

        if (classification.Class == ErrorClass.Retryable && attempt < budget)
        {
            var delay = classification.RetryAfter ??
                        Protocol.CanonicalBackoff[Math.Min(attempt - 1, Protocol.CanonicalBackoff.Count - 1)];
            bus.Options.Logger?.Warn(
                $"[flux] RETRYABLE {classification.Code} en {w.SubjectLabel} (clave {key}, intento {attempt}/{budget}), reintento en {delay}");
            bus.Metrics.EventRetried(w.SubjectLabel, w.ConsumerLabel, attempt);
            await m.NakAsync(delay: delay, cancellationToken: ct).ConfigureAwait(false);
            return;
        }

        var reason = classification.Class.ToDlqReason();

        // `dlqerror` NO DEBE contener valores de `data` (10-state.md §6). Un error de flux lleva su código y un mensaje que no cita
        // valores. Uno que no sea de flux —una excepción de un driver de base de datos, que puede citar la fila entera— se reduce a
        // su código: la DLQ guarda 90 días y en estado `data` es dato personal.
        var dlqError = error is ClassifiedException
            ? classification.Code + ": " + error.Message
            : classification.Code + ": " + StateProtocol.UnclassifiedDlqError;
        var ahora = Envelope.FormatTime(DateTimeOffset.UtcNow);
        var info = new StateProtocol.DlqInfo(key, revision, reason, attempt, w.Durable, dlqError, ahora);
        // Un mensaje ilegible no se copia; el resto, su sobre ORIGINAL sin el dato.
        var dlqEvent = parsed is not null
            ? StateProtocol.ToDlqEvent(parsed, info)
            : StateProtocol.BuildUnreadableDlqEvent(
                w.Contract, tenant, key, revision, bus.Source, bus.Options.Version, Protocol.NewEventId().ToString(), ahora,
                SchemaForSafe(w.Contract), reason, attempt, w.Durable, dlqError, ahora);

        try
        {
            await bus.PublishToDlqRawAsync(StateProtocol.DlqSubject(w.Contract), dlqEvent, $"dlq-{dlqEvent.Id}-{w.Durable}", ct)
                .ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // NO se hace term(): terminar aquí borraría la entrada sin haberla guardado en ningún sitio.
            bus.Options.Logger?.Error(
                $"[flux] no se pudo publicar en la DLQ de estado para {w.SubjectLabel} (clave {key}): {e.Message}. " +
                "El mensaje NO se termina; se reentregará.");
            return;
        }

        if (classification.Class == ErrorClass.Poison)
        {
            // POISON es lo que despierta a alguien: term() + DLQ + ALERTA. `Raw` va vacío a propósito: el cuerpo de una entrada de
            // estado puede llevar datos personales (10-state.md §6).
            bus.Options.OnPoison?.Invoke(new PoisonInfo(m.Subject, error, Array.Empty<byte>()));
            bus.Options.Logger?.Error(
                $"[flux] POISON {classification.Code} en {w.SubjectLabel} (clave {key}): " +
                (error is ClassifiedException ? error.Message : classification.Code));
        }
        else
        {
            bus.Options.Logger?.Error(
                $"[flux] DLQ de estado ({reason.ToString().ToLowerInvariant()}) {classification.Code} en {w.SubjectLabel} " +
                $"(clave {key}) tras {attempt} intento(s)");
        }

        bus.Metrics.EventConsumed(w.SubjectLabel, w.ConsumerLabel, MetricLabels.ConsumeOutcomeFor(reason, classification.Code));
        bus.Metrics.EventDlq(w.SubjectLabel, w.ConsumerLabel, reason, classification.Code);
        bus.Metrics.HandlerDuration(w.SubjectLabel, w.ConsumerLabel, Stopwatch.GetElapsedTime(inicio).TotalSeconds);
        bus.Options.OnDlq?.Invoke(new DlqEventInfo(m.Subject, dlqEvent, classification));
        await m.AckTerminateAsync(cancellationToken: ct).ConfigureAwait(false);
    }

    private sealed class DecodeHolder
    {
        public FluxEvent? Event { get; set; }
    }

    /// <summary>
    /// Lo que comprueba el SDK al consumir, en el orden de 10-state.md §5.4. Lo que falla <b>nunca</b> se aplica y <b>nunca</b>
    /// llega al handler. <c>holder.Event</c> se rellena en cuanto el sobre es un CloudEvent válido, para que la DLQ pueda partir de él.
    /// </summary>
    private (FluxEvent Event, StateEntry Entry, bool SignatureWarning) Decode(
        byte[] bytes,
        string contract,
        string tenant,
        string key,
        long revision,
        string subject,
        DecodeHolder? holder)
    {
        // 1. Un CloudEvent válido con las cuatro extensiones (POISON, los códigos de siempre).
        var evento = Envelope.ParseEvent(bytes);
        if (holder is not null)
        {
            holder.Event = evento;
        }

        // 2. La identidad está en el subject; el sobre la repite. Si discrepan, uno miente: POISON.
        if (!string.Equals(evento.TenantId, tenant, StringComparison.Ordinal))
        {
            throw new StateSubjectMismatchException("tenantid", evento.TenantId, tenant);
        }

        if (!string.Equals(evento.AggregateId, key, StringComparison.Ordinal))
        {
            throw new StateSubjectMismatchException("subject", evento.AggregateId, key);
        }

        var put = StateProtocol.Type(contract);
        var deleted = evento.Type == StateProtocol.TombstoneType(contract);
        if (!deleted && evento.Type != put)
        {
            throw new StateSubjectMismatchException("type", evento.Type, put);
        }

        // 3. `restricted` no se admite en estado: en consumo es POISON.
        if (evento.DataClassification == DataClassification.Restricted)
        {
            throw new RestrictedStateEntryException();
        }

        // 4. `aggregateVersion` válido: sin él no se puede aplicar de forma condicional (PERMANENT).
        var version = StateProtocol.AggregateVersionOf(evento.Data);

        // La firma antes que el esquema: un evento manipulado puede validar perfectamente y no ser del productor que dice.
        // En `warn` la propia verificación registra el fallo y sigue; en `require` lanza (PoisonException) y recorre la misma ruta
        // que cualquier otro fallo: clasificación, DLQ de estado, métricas y term.
        _bus.Options.Verifier?.Check(evento);
        const bool advertencia = false;

        // 5. Si hay L3 al consumir: `data` contra su esquema.
        if (_bus.Options.Validation is { OnConsume: true, Validator: { } validador })
        {
            validador.Check(evento, subject);
        }

        return (
            evento,
            new StateEntry(key, tenant, deleted, deleted ? null : evento.Data, version, revision),
            advertencia);
    }

    // ─── tenant, esquema, errores y métricas ─────────────────────────────────

    /// <summary>Escritura y lectura: la opción de la llamada, el evento en curso y <c>ConnectAsync</c>. Sin ninguno, error.</summary>
    private string TenantFor(string? option)
    {
        var t = option ?? FluxContext.CurrentContext?.TenantId ?? _bus.Options.TenantId;
        return StateProtocol.AssertTenant(t);
    }

    /// <summary>
    /// <c>watch</c>: la opción y <c>ConnectAsync</c>, <b>sin el evento en curso</b> (un <c>watch</c> no corre dentro de un handler:
    /// 10-state.md §2.4). Sin ninguno es legítimo (se observan todos), salvo <c>Strict</c>.
    /// </summary>
    private string? WatchTenant(string? option)
    {
        var t = option ?? _bus.Options.TenantId;
        return t is null ? null : StateProtocol.AssertTenant(t);
    }

    /// <summary>El <c>dataschema</c> del contrato: mapa explícito, índice <c>state</c> del bundle o <c>SchemaBaseUrl</c>. Nunca por red.</summary>
    private string SchemaFor(string contract)
    {
        var o = _bus.Options;
        if (o.Schemas is not null && o.Schemas.TryGetValue(contract, out var explicito) && !string.IsNullOrEmpty(explicito))
        {
            return explicito;
        }

        if (o.Validation?.Bundle?.StateSchemaUriFor(contract) is { Length: > 0 } delBundle)
        {
            return delBundle;
        }

        if (string.IsNullOrEmpty(o.SchemaBaseUrl))
        {
            throw new EnvelopeException(
                $"no hay dataschema para el estado \"{contract}\". Declara Schemas[\"{contract}\"], SchemaBaseUrl, o pasa un bundle " +
                "con el índice `state` en Validation.Bundle (ConnectAsync).");
        }

        return StateProtocol.DataSchema(contract, o.SchemaBaseUrl);
    }

    private string SchemaForSafe(string contract)
    {
        try
        {
            return SchemaFor(contract);
        }
        catch (EnvelopeException)
        {
            return StateProtocol.DataSchema(contract);
        }
    }

    private NotProvisionedException NotProvisioned(string stream, string subject, Exception cause) =>
        NotProvisionedStream(stream, $"$JS.API.DIRECT.GET.{stream}.{subject}", cause);

    /// <summary>
    /// Nadie contesta: el stream no existe, <b>o</b> existe sin <c>allow_direct</c>, y los dos casos se ven exactamente igual (A-32).
    /// El mensaje DEBE nombrar las dos causas y apuntar a <c>flux provision</c> (lo crea) y a <c>flux doctor</c> (lo audita).
    /// </summary>
    private NotProvisionedException NotProvisionedStream(string stream, string sinRespuesta, Exception cause) =>
        new(
            ResourceKind.Stream,
            stream,
            null,
            _bus.Options.Service,
            sinRespuesta,
            cause,
            detail:
            $"nadie contestó en \"{sinRespuesta}\": el stream \"{stream}\" no existe, o existe sin `allow_direct: true`, y los dos " +
            "casos se ven exactamente igual (no hay un 404: no responde nadie). \"" + _bus.Options.Service + "\" no lo crea ni lo " +
            "consulta —los SDKs no usan `STREAM.INFO` sobre el estado, porque lista todas las claves y todos los tenants (A-39)—. " +
            "Aprovisiónalo con `flux provision`, con credenciales de operación, y compruébalo con `flux doctor` " +
            "(10-state.md §5.1, 04-errors.md §6).");

    private T CountingInvalid<T>(string contract, Func<T> fn)
    {
        try
        {
            return fn();
        }
        catch (Exception e) when (e is MissingAggregateVersionException or SchemaValidationException or SchemaNotFoundException)
        {
            // `invalid_schema` se emite también cuando la llamada lanza: una excepción sirve a quien la provoca, y la métrica a
            // quien opera el ecosistema. Los demás errores de la llamada (contrato, clave, tenant, dominio reservado,
            // clasificación) son de programación y NO se cuentan.
            _bus.Metrics.StateWrite(contract, StateWriteOutcome.InvalidSchema);
            throw;
        }
    }

    private void CountingInvalid(string contract, Action fn) =>
        CountingInvalid<object?>(contract, () =>
        {
            fn();
            return null;
        });

    private void NotePending(WatchState w, long pending)
    {
        var suma = _pending.Note($"{w.SubjectLabel}|{w.ConsumerLabel}", w.Durable, pending);
        _bus.Metrics.ConsumerPending(w.SubjectLabel, w.ConsumerLabel, suma);
    }

    private void DropPending(WatchState w)
    {
        var suma = _pending.Drop($"{w.SubjectLabel}|{w.ConsumerLabel}", w.Durable);
        if (suma is { } s)
        {
            _bus.Metrics.ConsumerPending(w.SubjectLabel, w.ConsumerLabel, s);
        }
    }
}

/// <summary>
/// Un gancho para las pruebas contra un broker real: hace escribir a un rival entre la lectura y la publicación de un intento, pierde el
/// acuse del primer publish o fija el <c>id</c> (<c>conformance/cases/state-semantics.json</c>, <c>hook</c>). No es API pública.
/// </summary>
internal sealed class WriteHook
{
    /// <summary>Se invoca antes de publicar (intento, subject): permite que un rival escriba en medio.</summary>
    public Func<int, string, Task>? BeforePublish { get; init; }

    /// <summary>Si devuelve <see langword="true"/>, el publish se aplicó pero quien lo hizo ve un error de secuencia.</summary>
    public Func<int, bool>? LoseAck { get; init; }

    /// <summary>El <c>err_code</c> con el que se simula el acuse perdido de <see cref="LoseAck"/>: 10071 (un nodo) o 10164 (R3).</summary>
    public int LoseAckErrCode { get; init; } = 10071;

    /// <summary>Si devuelve un <c>err_code</c>, el intento falla con ese error SIN publicar (un conflicto que no se aplicó).</summary>
    public Func<int, int?>? FailPublish { get; init; }

    /// <summary>Se invoca cuando el paso 1b reconoce la propia escritura.</summary>
    public Action<int>? OnOwnWrite { get; init; }

    /// <summary>Se invoca al terminar una escritura (con éxito, <c>stale</c> o conflicto) con los intentos gastados.</summary>
    public Action<int>? OnDone { get; init; }

    /// <summary>El <c>id</c> del evento, en vez de uno nuevo.</summary>
    public string? IdOverride { get; init; }

    /// <summary>Se invoca en <c>WatchAsync</c> tras crear el durable y antes de empezar a consumir: permite escribir «durante la sincronización».</summary>
    public Func<Task>? AfterOpen { get; init; }
}
