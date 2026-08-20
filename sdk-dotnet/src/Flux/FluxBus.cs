// Cliente de flux. Nivel de conformidad: L2.
// Contrato normativo: specification/00-protocol.md §5
//
// Regla de diseño: este fichero es el ÚNICO del SDK que conoce NATS, y NO expone ningún
// tipo de NATS en su API pública. Si lo hiciera, sustituir el broker dejaría de ser un
// cambio de capa 0-1 y pasaría a tocar las aplicaciones — que es exactamente lo que flux
// existe para evitar (00-protocol.md §3).

using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Flux;

/// <summary>
/// Logger mínimo del SDK.
/// </summary>
/// <remarks>
/// Una interfaz de dos métodos y no <c>ILogger</c> de
/// <c>Microsoft.Extensions.Logging</c>: un SDK de protocolo no debería imponer un
/// contenedor de dependencias ni un paquete de logging a quien solo quiere publicar un
/// evento. Adaptarlo a <c>ILogger</c>, Serilog o <c>Console</c> son tres líneas en la
/// aplicación.
/// </remarks>
public interface IFluxLogger
{
    /// <summary>Algo que merece atención pero no ha roto nada (un RETRYABLE, p. ej.).</summary>
    void Warn(string message);

    /// <summary>Algo que ha roto: un evento a la DLQ, un POISON, un despacho fallido.</summary>
    void Error(string message);
}

/// <summary>Credenciales de NATS. NUNCA versionadas — 06-security.md §2.</summary>
public sealed record NatsCredentials
{
    /// <summary>Ruta a un fichero <c>.creds</c>. Es la forma recomendada.</summary>
    public string? CredsFile { get; init; }

    /// <summary>Token de autenticación.</summary>
    public string? Token { get; init; }

    /// <summary>Usuario.</summary>
    public string? User { get; init; }

    /// <summary>Contraseña.</summary>
    public string? Password { get; init; }
}

/// <summary>Un mensaje que no se pudo interpretar.</summary>
/// <param name="Subject">Subject de NATS por el que llegó.</param>
/// <param name="Error">Por qué no se pudo interpretar.</param>
/// <param name="Raw">El cuerpo tal cual llegó. Es lo único que queda para el forense.</param>
public sealed record PoisonInfo(string Subject, Exception Error, byte[] Raw);

/// <summary>Un evento enrutado a la DLQ.</summary>
/// <param name="Subject">Subject original, sin el prefijo <c>dlq.</c>.</param>
/// <param name="Event">El evento, ya con las extensiones <c>dlq*</c>.</param>
/// <param name="Classification">Cómo se clasificó el error que lo mató.</param>
public sealed record DlqEventInfo(string Subject, FluxEvent Event, Classification Classification);

/// <summary>Configuración de la conexión al bus.</summary>
public sealed record ConnectOptions
{
    /// <summary><c>nats://host:4222</c>, o varios separados por coma para HA.</summary>
    public required string Servers { get; init; }

    /// <summary>
    /// Nombre del servicio. Alimenta <c>source</c> y los nombres de durable consumer, así
    /// que se valida contra <see cref="Protocol.ServicePattern"/> en
    /// <see cref="FluxBus.ConnectAsync"/> — 02-naming.md §4.
    /// </summary>
    public required string Service { get; init; }

    /// <summary><c>produccion</c>, <c>staging</c>, <c>dev</c>. Alimenta <c>source</c>.</summary>
    public required string Environment { get; init; }

    /// <summary>SemVer del servicio. Va en <c>producerversion</c>.</summary>
    public required string Version { get; init; }

    /// <summary>Tenant por defecto de los eventos publicados. Vacío significa <c>"system"</c>.</summary>
    public string? TenantId { get; init; }

    /// <summary>Clasificación por defecto. <see langword="null"/> significa <c>internal</c> — 06-security.md §5.</summary>
    public DataClassification? Classification { get; init; }

    /// <summary>Mapa exacto subject → URI de <c>dataschema</c>. Gana sobre <see cref="SchemaBaseUrl"/>.</summary>
    public IReadOnlyDictionary<string, string>? Schemas { get; init; }

    /// <summary>Base para derivar <c>dataschema</c> cuando el subject no está en <see cref="Schemas"/>.</summary>
    public string? SchemaBaseUrl { get; init; }

    /// <summary>Política de clasificación de errores. Ver <see cref="ClassifierOptions"/>.</summary>
    public ClassifierOptions? Classifier { get; init; }

    /// <summary>
    /// Réplicas de los streams que el SDK crea si no existen.
    /// </summary>
    /// <remarks>
    /// El default es 1 porque el auto-provisionado es una comodidad de desarrollo: con 3 no
    /// arrancaría contra un NATS de un solo nodo. <b>En producción los streams se
    /// provisionan por IaC con <c>replicas: 3</c></b> (02-naming.md §3.2), no desde el SDK
    /// — dejar que un servicio cualquiera cree el stream compartido de su dominio con la
    /// configuración que le apetezca es una carrera esperando a ocurrir.
    /// </remarks>
    public int StreamReplicas { get; init; } = 1;

    /// <summary>Credenciales. NUNCA versionadas — 06-security.md §2.</summary>
    public NatsCredentials? Credentials { get; init; }

    /// <summary>
    /// Origen del <c>traceparent</c> W3C. <see langword="null"/> usa
    /// <see cref="FluxContext.ActiveTraceparent"/>, que lee <c>Activity.Current</c>.
    /// </summary>
    public Func<string?>? TraceparentProvider { get; init; }

    /// <summary>
    /// Se invoca ante un POISON. Es el único caso que debe despertar a alguien: casi
    /// siempre significa que un productor está roto — 04-errors.md §1.3.
    /// </summary>
    public Action<PoisonInfo>? OnPoison { get; init; }

    /// <summary>Se invoca al enrutar cualquier evento a la DLQ.</summary>
    public Action<DlqEventInfo>? OnDlq { get; init; }

    /// <summary>Logger opcional. <see langword="null"/> significa silencio.</summary>
    public IFluxLogger? Logger { get; init; }
}

/// <summary>Ajustes de una publicación concreta.</summary>
public sealed record PublishOptions
{
    /// <summary>
    /// Id del agregado. Va al atributo <c>subject</c> de CloudEvents, NO al subject de
    /// NATS — 01-envelope.md §2.1.
    /// </summary>
    public string? AggregateId { get; init; }

    /// <summary>Sobrescribe el tenant para esta publicación.</summary>
    public string? TenantId { get; init; }

    /// <summary>Sobrescribe la clasificación para esta publicación.</summary>
    public DataClassification? Classification { get; init; }

    /// <summary>
    /// Instante en que OCURRIÓ el hecho, si no es "ahora". El <c>time</c> de CloudEvents no
    /// es el de publicación — 01-envelope.md §2.
    /// </summary>
    public DateTimeOffset? Time { get; init; }

    /// <summary>Sobrescribe la clave de partición, que por defecto es el <see cref="AggregateId"/>.</summary>
    public string? PartitionKey { get; init; }
}

/// <summary>Ajustes de una suscripción.</summary>
public sealed record SubscribeOptions
{
    /// <summary>
    /// Nombre de durable explícito. Por defecto se deriva del servicio y el subject con
    /// <see cref="Protocol.DurableName"/>.
    /// </summary>
    public string? Durable { get; init; }

    /// <summary>Ventana de mensajes sin confirmar. Cero usa <see cref="Protocol.DefaultMaxAckPending"/>.</summary>
    public int MaxAckPending { get; init; }

    /// <summary>
    /// Descarta —con ack— los eventos de otros tenants antes de invocar al handler
    /// — 06-security.md §4.
    /// </summary>
    public string? TenantId { get; init; }
}

/// <summary>
/// Metadatos de ESTA entrega concreta del evento.
/// </summary>
/// <param name="Attempt">
/// Número de entrega, empezando en 1. Si es &gt; 1, este evento YA se intentó procesar
/// antes: la idempotencia no es opcional — 03-delivery.md §4.
/// </param>
/// <param name="MaxAttempts">
/// El techo del consumidor (<see cref="Protocol.DefaultMaxDeliver"/>). Al alcanzarlo, el
/// SDK enruta a la DLQ — antes si la clasificación del error impone un presupuesto menor,
/// cosa que aquí todavía no se sabe porque el handler aún no ha fallado — 04-errors.md §2.1.
/// </param>
/// <param name="Subject">Subject de NATS por el que llegó el evento.</param>
/// <param name="Durable">Nombre del consumidor. Aparece como <c>dlqconsumer</c> en la DLQ.</param>
public sealed record Delivery(int Attempt, int MaxAttempts, string Subject, string Durable);

/// <summary>
/// Procesa un evento.
/// </summary>
/// <remarks>
/// <b>Volver del handler sin excepción ACK-ea el evento.</b> Lanzar lo clasifica según
/// <see cref="ClassifierOptions"/> y produce <c>nak</c>, <c>term</c> o <c>term</c>+alerta.
/// <para>
/// ⚠️ Divergencia con Node: allí el handler recibe un <c>ctx</c> con un método
/// <c>ack()</c> explícito que además es un no-op (volver del handler ya hace ack). Aquí,
/// como en Go, volver ES el ack explícito. El requisito del protocolo —nunca auto-ack— se
/// cumple igual: el SDK jamás confirma un mensaje antes de que el handler termine.
/// </para>
/// <para>
/// El handler DEBE ser idempotente. La garantía es <i>at-least-once</i>: los duplicados
/// llegan, no son un fallo — 03-delivery.md §1.
/// </para>
/// </remarks>
/// <param name="evento">El evento entrante.</param>
/// <param name="entrega">Metadatos de esta entrega.</param>
/// <param name="cancellationToken">Se cancela al cerrar la suscripción o el bus.</param>
public delegate Task FluxHandler(FluxEvent evento, Delivery entrega, CancellationToken cancellationToken);

/// <summary>Una suscripción viva.</summary>
public interface IFluxSubscription : IAsyncDisposable
{
    /// <summary>Subject de NATS suscrito.</summary>
    string Subject { get; }

    /// <summary>Nombre del durable consumer.</summary>
    string Durable { get; }

    /// <summary>
    /// Detiene la entrega.
    /// </summary>
    /// <remarks>
    /// Los mensajes ya en el búfer se descartan y se reentregarán: eso es correcto, y es
    /// exactamente el caso que cubre la idempotencia — 03-delivery.md §6.
    /// </remarks>
    Task UnsubscribeAsync();
}

/// <summary>
/// El cliente de flux: publica y consume eventos. Es seguro usarlo desde varios hilos.
/// </summary>
public sealed class FluxBus : IAsyncDisposable
{
    private const string PoisonType = "com.flux.system.poison.capturado.v1";
    private const string PoisonSchema = "https://schemas.internal/system/poison/capturado/1.0.0.json";

    /// <summary>Tope del base64 del cuerpo crudo en la DLQ, para que el envelope quepa en 1 MiB.</summary>
    private const int MaxRawBase64Chars = 700_000;

    private readonly NatsConnection _connection;
    private readonly NatsJSContext _jetStream;
    private readonly ConnectOptions _options;
    private readonly Classifier _classifier;
    private readonly string _source;
    private readonly ConcurrentDictionary<Subscription, byte> _subscriptions = new();
    private readonly ConcurrentDictionary<string, byte> _ensuredStreams = new(StringComparer.Ordinal);

    private FluxBus(NatsConnection connection, NatsJSContext jetStream, ConnectOptions options)
    {
        _connection = connection;
        _jetStream = jetStream;
        _options = options;
        _classifier = new Classifier(options.Classifier);
        _source = Protocol.SourceUri(options.Environment, options.Service);
    }

    /// <summary>
    /// Abre la conexión al bus.
    /// </summary>
    /// <remarks>
    /// Reconecta indefinidamente con backoff y JITTER: sin jitter, mil servicios reconectan
    /// en el mismo milisegundo y tiran el cluster que acaba de levantarse — 03-delivery.md §6.
    /// </remarks>
    /// <exception cref="ArgumentException">Falta algún dato obligatorio del envelope.</exception>
    /// <exception cref="InvalidServiceNameException">El nombre de servicio no cumple 02-naming.md §4.</exception>
    public static async Task<FluxBus> ConnectAsync(ConnectOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrEmpty(options.Service) ||
            string.IsNullOrEmpty(options.Environment) ||
            string.IsNullOrEmpty(options.Version))
        {
            throw new ArgumentException(
                "Service, Environment y Version son obligatorios: alimentan `source` y " +
                "`producerversion`, que son atributos obligatorios del envelope (01-envelope.md §2)",
                nameof(options));
        }

        // Se valida aquí y no solo al derivar el durable: NATS aceptaría sin rechistar un
        // consumidor llamado `FacturacionAPI__pedidos_…`, y el incumplimiento del patrón
        // solo se descubriría al parsear nombres de consumidor en una herramienta
        // — protocol.json naming.service.
        Protocol.ValidateService(options.Service);

        var natsOptions = NatsOpts.Default with
        {
            Url = options.Servers,
            Name = options.Service + "@" + options.Environment,
            // -1 = reconexión indefinida. Un corte largo del broker no debe matar el
            // servicio: los acks pendientes se reentregan y la idempotencia los cubre.
            MaxReconnectRetry = -1,
            ReconnectWaitMin = TimeSpan.FromSeconds(1),
            ReconnectWaitMax = TimeSpan.FromSeconds(30),
            ReconnectJitter = TimeSpan.FromSeconds(1),
            AuthOpts = BuildAuthOptions(options.Credentials),
        };

        var connection = new NatsConnection(natsOptions);
        await connection.ConnectAsync().ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();

        return new FluxBus(connection, new NatsJSContext(connection), options);
    }

    private static NatsAuthOpts BuildAuthOptions(NatsCredentials? credentials)
    {
        if (credentials is null)
        {
            return NatsAuthOpts.Default;
        }

        return NatsAuthOpts.Default with
        {
            CredsFile = credentials.CredsFile,
            Token = credentials.Token,
            Username = credentials.User,
            Password = credentials.Password,
        };
    }

    /// <summary>
    /// Informa si la conexión sigue viva. Expuesto para el healthcheck que exige
    /// 03-delivery.md §6.
    /// </summary>
    public bool Connected => _connection.ConnectionState == NatsConnectionState.Open;

    // ─── publish ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Construye el envelope y publica el evento.
    /// </summary>
    /// <remarks>
    /// El desarrollador solo escribe subject, <paramref name="data"/> y opcionalmente
    /// <see cref="PublishOptions.AggregateId"/>. Todo lo demás —<c>id</c>, <c>source</c>,
    /// <c>time</c>, <c>specversion</c>, <c>type</c>, <c>dataschema</c>,
    /// <c>correlationid</c>, <c>causationid</c>, <c>producerversion</c>,
    /// <c>traceparent</c>— lo rellena el SDK. Si tu código asigna alguno de esos a mano,
    /// está mal — 01-envelope.md §5.
    /// <para>
    /// Se publica SIEMPRE por JetStream y nunca por core NATS. Verificado contra NATS
    /// 2.14.5: un publish de core a un subject que ningún stream captura no da ningún error
    /// y el evento se evapora sin rastro; el de JetStream sí falla, porque espera el acuse
    /// del stream. Es la única red de seguridad automática contra una errata de subject
    /// — 02-naming.md §1.1.
    /// </para>
    /// </remarks>
    /// <param name="subject">Subject de NATS, 4 tokens en minúsculas.</param>
    /// <param name="data">Payload. Debe ser un objeto JSON en la raíz.</param>
    /// <param name="options">Ajustes opcionales de esta publicación.</param>
    /// <param name="cancellationToken">Cancelación.</param>
    /// <returns>El evento tal y como se publicó.</returns>
    public async Task<FluxEvent> PublishAsync(
        string subject,
        object data,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var parsed = Protocol.ParseSubject(subject); // falla temprano y con un mensaje útil
        var publishOptions = options ?? new PublishOptions();

        await EnsureStreamAsync(parsed.Domain, cancellationToken).ConfigureAwait(false);

        // UUIDv7: monotónico en el tiempo, así que ordenar por `id` dentro de un mismo
        // `source` equivale a ordenar por instante de generación — útil al reconstruir
        // historiales desde la DLQ (01-envelope.md §2.4).
        var eventId = Protocol.NewEventId().ToString();
        var inherited = FluxContext.CurrentContext;

        var traceparent = inherited?.TraceParent
                          ?? _options.TraceparentProvider?.Invoke()
                          ?? FluxContext.ActiveTraceparent();

        var evento = Envelope.BuildEvent(new Envelope.BuildEventInput
        {
            Subject = subject,
            Data = data,
            Id = eventId,
            Source = _source,
            ProducerVersion = _options.Version,
            // El contexto heredado gana sobre el default de la conexión: un evento derivado
            // pertenece al tenant del evento que lo causó, no al del servicio.
            TenantId = publishOptions.TenantId
                       ?? inherited?.TenantId
                       ?? _options.TenantId
                       ?? "system",
            DataClassification = publishOptions.Classification
                                 ?? _options.Classification
                                 ?? DataClassification.Internal,
            DataSchema = SchemaFor(subject, parsed),
            // Si el evento no nace de otro, correlationid se inicializa con su propio id
            // — 01-envelope.md §3.1.
            CorrelationId = inherited?.CorrelationId ?? eventId,
            CausationId = inherited?.CausationId,
            Time = publishOptions.Time,
            AggregateId = publishOptions.AggregateId,
            PartitionKey = publishOptions.PartitionKey,
            TraceParent = traceparent,
            TraceState = inherited?.TraceState ?? FluxContext.ActiveTracestate(),
        });

        var payload = Envelope.Serialize(evento);

        // MsgId pone la cabecera Nats-Msg-Id. Deduplica reintentos de PUBLICACIÓN dentro de
        // duplicate_window: un publish que no recibe el ACK del broker por un corte de red
        // y se reintenta no deja dos copias en el stream.
        //
        // NO deduplica reentregas de consumo. Un nak reentrega el mismo mensaje con el
        // mismo Nats-Msg-Id y eso es correcto y deseado. Nunca sustituye a la idempotencia
        // del consumidor — 03-delivery.md §3.
        var ack = await _jetStream.PublishAsync(
            subject: subject,
            data: payload,
            opts: new NatsJSPubOpts { MsgId = evento.Id },
            cancellationToken: cancellationToken).ConfigureAwait(false);

        ack.EnsureSuccess();
        return evento;
    }

    /// <summary>Resuelve la URI de <c>dataschema</c> del subject.</summary>
    private string SchemaFor(string subject, Protocol.ParsedSubject parsed)
    {
        if (_options.Schemas is not null &&
            _options.Schemas.TryGetValue(subject, out var explicitSchema) &&
            !string.IsNullOrEmpty(explicitSchema))
        {
            return explicitSchema;
        }

        if (string.IsNullOrEmpty(_options.SchemaBaseUrl))
        {
            throw new EnvelopeException(
                $"no hay dataschema para \"{subject}\". Declara Schemas[\"{subject}\"] o " +
                "SchemaBaseUrl en ConnectOptions (01-envelope.md §2)");
        }

        // Sin registro exacto solo se puede asumir el .0.0 del mayor. Un SDK L3 resolverá
        // el minor real contra el Schema Registry — 00-protocol.md §5.
        var baseUrl = _options.SchemaBaseUrl.TrimEnd('/');
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{baseUrl}/{parsed.Domain}/{parsed.Aggregate}/{parsed.Event}/{parsed.Major}.0.0.json");
    }

    // ─── subscribe ───────────────────────────────────────────────────────────

    /// <summary>
    /// Crea (o reutiliza) el durable consumer canónico y empieza a entregar.
    /// </summary>
    /// <param name="subject">Subject de NATS, 4 tokens en minúsculas.</param>
    /// <param name="handler">Handler idempotente. Volver sin excepción ACK-ea.</param>
    /// <param name="options">Ajustes opcionales de la suscripción.</param>
    /// <param name="cancellationToken">Gobierna la creación del consumidor, no la entrega.</param>
    /// <exception cref="ConsumerConfigMismatchException">
    /// El servidor aplicó una configuración distinta de la solicitada — requisito L2,
    /// 03-delivery.md §2.1.
    /// </exception>
    public async Task<IFluxSubscription> SubscribeAsync(
        string subject,
        FluxHandler handler,
        SubscribeOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(handler);

        var parsed = Protocol.ParseSubject(subject);
        var subscribeOptions = options ?? new SubscribeOptions();

        await EnsureStreamAsync(parsed.Domain, cancellationToken).ConfigureAwait(false);
        await EnsureDlqStreamAsync(parsed.Domain, cancellationToken).ConfigureAwait(false);

        var durable = subscribeOptions.Durable ?? Protocol.DurableName(_options.Service, subject);
        var maxAckPending = subscribeOptions.MaxAckPending > 0
            ? subscribeOptions.MaxAckPending
            : Protocol.DefaultMaxAckPending;

        var requested = new ConsumerConfig(durable)
        {
            DurableName = durable,
            FilterSubject = subject,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            // ack_wait DEBE ser backoff[0]: JetStream lo sobrescribe con ese valor sin
            // avisar. Ver 03-delivery.md §2.1 y Protocol.CanonicalBackoff.
            AckWait = Protocol.DefaultAckWait,
            MaxDeliver = Protocol.DefaultMaxDeliver,
            Backoff = Protocol.CanonicalBackoff.Select(ToNanos).ToArray(),
            MaxAckPending = maxAckPending,
            DeliverPolicy = ConsumerConfigDeliverPolicy.All,
            ReplayPolicy = ConsumerConfigReplayPolicy.Instant,
        };

        var stream = Protocol.StreamName(parsed.Domain);
        var consumer = await _jetStream
            .CreateOrUpdateConsumerAsync(stream, requested, cancellationToken)
            .ConfigureAwait(false);

        // El servidor no siempre devuelve lo que se le pide. Requisito L2 — 03-delivery.md §2.1.
        ConsumerConfigVerifier.AssertHonored(
            durable,
            ConsumerConfigSnapshot.Canonical(maxAckPending),
            Snapshot(consumer.Info.Config));

        var subscription = new Subscription(this, subject, durable);
        _subscriptions[subscription] = 0;
        subscription.Start(consumer, handler, subscribeOptions);
        return subscription;
    }

    /// <summary>
    /// Traduce la config que devuelve el servidor al snapshot que sabe verificar
    /// <see cref="ConsumerConfigVerifier"/>.
    /// </summary>
    /// <remarks>
    /// Este método es la frontera: a partir de aquí no hay tipos de NATS. Es lo que permite
    /// probar la verificación L2 en un test unitario, sin broker.
    /// </remarks>
    // ⚠️ NATS.Net tipa las dos duraciones del mismo objeto de forma DISTINTA:
    //   ConsumerConfig.AckWait  → TimeSpan
    //   ConsumerConfig.Backoff  → ICollection<long> en NANOSEGUNDOS
    //
    // Y son precisamente los dos campos que el protocolo obliga a mantener iguales
    // (ack_wait == backoff[0], 03-delivery.md §2.1). Comparar un TimeSpan con un long
    // de nanos "parece" correcto y da 30 vs 30000000000 sin que nada avise, así que la
    // conversión se hace explícita y vive solo aquí: es la frontera de la capa 1 del
    // protocolo (00-protocol.md §3).
    private static long ToNanos(TimeSpan t) => t.Ticks * 100L;

    private static TimeSpan FromNanos(long nanos) => TimeSpan.FromTicks(nanos / 100L);

    private static ConsumerConfigSnapshot Snapshot(ConsumerConfig config) => new()
    {
        AckWait = config.AckWait,
        MaxDeliver = (int)config.MaxDeliver,
        MaxAckPending = (int)config.MaxAckPending,
        AckPolicy = config.AckPolicy switch
        {
            ConsumerConfigAckPolicy.Explicit => "explicit",
            ConsumerConfigAckPolicy.All => "all",
            ConsumerConfigAckPolicy.None => "none",
            _ => "desconocida",
        },
        Backoff = config.Backoff?.Select(FromNanos).ToArray() ?? Array.Empty<TimeSpan>(),
    };

    // ─── despacho ────────────────────────────────────────────────────────────

    private async Task DispatchAsync(
        NatsJSMsg<byte[]> message,
        string subject,
        string durable,
        FluxHandler handler,
        SubscribeOptions options,
        CancellationToken cancellationToken)
    {
        // NumDelivered empieza en 1 en la primera entrega, no en 0.
        var delivered = message.Metadata?.NumDelivered ?? 1;
        var attempt = delivered < 1 ? 1 : (int)delivered;
        var raw = message.Data ?? Array.Empty<byte>();

        // POISON se detecta ANTES del handler: el mensaje no es interpretable, así que el
        // handler nunca llega a verlo — 04-errors.md §1.3.
        FluxEvent evento;
        try
        {
            evento = Envelope.ParseEvent(raw);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // Cualquier fallo al interpretar el mensaje es, por definición, POISON: no hay
            // CloudEvent que entregar al handler — 04-errors.md §1.3.
            await HandlePoisonAsync(message, subject, durable, raw, e, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (!string.IsNullOrEmpty(options.TenantId) &&
            !string.Equals(evento.TenantId, options.TenantId, StringComparison.Ordinal))
        {
            await message.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false); // no es para nosotros
            return;
        }

        // WIP mientras el handler vive: resetea el temporizador de reentrega en el servidor
        // y evita que el mismo evento se ejecute en concurrencia consigo mismo. Cada
        // ack_wait/2 para que un ciclo perdido no agote el plazo — 03-delivery.md §2.1.
        using var wipCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var wip = KeepAliveAsync(message, wipCts.Token);

        Exception? handlerError = null;
        try
        {
            var delivery = new Delivery(attempt, Protocol.DefaultMaxDeliver, subject, durable);

            // El contexto del evento entrante se instala aquí. En Go esto obliga a pasar el
            // ctx a mano hasta el Publish; AsyncLocal lo propaga solo, igual que el
            // AsyncLocalStorage de Node. Ver EventContext.cs.
            using (FluxContext.Push(FluxContext.FromEvent(evento)))
            {
                await handler(evento, delivery, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            // Se captura TODO a propósito. En Go hace falta `recover` porque un pánico
            // mataría el proceso; en .NET cualquier excepción del handler es capturable, y
            // dejarla escapar mataría el bucle de consumo de ESTA suscripción. Un
            // consumidor muerto que parece vivo es peor que uno que se cae.
            handlerError = e;
        }
        finally
        {
            await wipCts.CancelAsync().ConfigureAwait(false);
            await wip.ConfigureAwait(false);
        }

        if (handlerError is null)
        {
            await message.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            return;
        }

        await ResolveFailureAsync(
            message, subject, durable, evento, attempt, handlerError, cancellationToken).ConfigureAwait(false);
    }

    private async Task ResolveFailureAsync(
        NatsJSMsg<byte[]> message,
        string subject,
        string durable,
        FluxEvent evento,
        int attempt,
        Exception handlerError,
        CancellationToken cancellationToken)
    {
        var classification = _classifier.Classify(handlerError);

        // El presupuesto efectivo es el menor entre el del consumidor y el que la
        // clasificación imponga para ESTE error. Así un error desconocido agota su
        // presupuesto acotado sin recortar los 6 intentos de un ECONNRESET reconocido
        // — 04-errors.md §2.1.
        var budget = Classifier.EffectiveBudget(Protocol.DefaultMaxDeliver, classification);

        if (classification.Class == ErrorClass.Retryable && attempt < budget)
        {
            // Retraso explícito según el backoff canónico en vez de dejar expirar ack_wait:
            // no retiene la ranura de max_ack_pending 30 s de más.
            var delay = classification.RetryAfter
                        ?? Protocol.CanonicalBackoff[Math.Min(attempt - 1, Protocol.CanonicalBackoff.Count - 1)];

            _options.Logger?.Warn(
                "[flux] RETRYABLE " + classification.Code + " en " + subject +
                " (intento " + attempt.ToString(CultureInfo.InvariantCulture) + "/" +
                budget.ToString(CultureInfo.InvariantCulture) + "), reintento en " +
                delay.ToString(null, CultureInfo.InvariantCulture));

            await message.NakAsync(delay: delay, cancellationToken: cancellationToken).ConfigureAwait(false);
            return;
        }

        var reason = classification.Class.ToDlqReason();
        var dlqEvent = Envelope.ToDlqEvent(evento, new Envelope.DlqInfo(
            reason, attempt, durable, classification.Code + ": " + handlerError.Message));

        try
        {
            await PublishToDlqAsync(subject, dlqEvent, "dlq-" + evento.Id + "-" + durable, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception e)
        {
            // NO se hace term(): terminar aquí borraría el evento sin haberlo guardado en
            // ningún sitio. Se deja reentregar — es preferible reprocesar un evento que
            // perderlo sin rastro. La causa suele ser un problema del broker, no del evento.
            _options.Logger?.Error(
                $"[flux] no se pudo publicar en la DLQ para {subject} (evento {evento.Id}): " +
                $"{e.Message}. El mensaje NO se termina; se reentregará.");
            return;
        }

        _options.OnDlq?.Invoke(new DlqEventInfo(subject, dlqEvent, classification));
        _options.Logger?.Error(
            "[flux] DLQ (" + reason.ToString().ToLowerInvariant() + ") " + classification.Code +
            " en " + subject + " tras " + attempt.ToString(CultureInfo.InvariantCulture) +
            " intento(s): " + handlerError.Message);

        await message.AckTerminateAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Envía <c>work-in-progress</c> cada <c>ack_wait/2</c> mientras el handler viva.
    /// </summary>
    /// <remarks>
    /// Es lo único que extiende de verdad el plazo de confirmación. Un handler que supere
    /// los 30 s sin WIP recibe el mismo mensaje reentregado MIENTRAS aún se está
    /// ejecutando — 03-delivery.md §2.1.
    /// </remarks>
    private static async Task KeepAliveAsync(NatsJSMsg<byte[]> message, CancellationToken cancellationToken)
    {
        try
        {
            using var timer = new PeriodicTimer(Protocol.WorkInProgressInterval);
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                await message.AckProgressAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // El handler terminó. Es la salida normal.
        }
        catch (Exception)
        {
            // El mensaje ya está resuelto o la conexión se cayó. No hay nada que hacer aquí
            // y desde luego no hay que tumbar el despacho por ello.
        }
    }

    private async Task HandlePoisonAsync(
        NatsJSMsg<byte[]> message,
        string subject,
        string durable,
        byte[] raw,
        Exception error,
        CancellationToken cancellationToken)
    {
        _options.OnPoison?.Invoke(new PoisonInfo(subject, error, raw));
        _options.Logger?.Error($"[flux] POISON en {subject}: {error.Message}");

        try
        {
            await PublishRawToDlqAsync(subject, raw, durable, error.Message, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception e)
        {
            // Igual que en la DLQ normal: no se termina lo que no se ha llegado a guardar.
            _options.Logger?.Error(
                $"[flux] no se pudo guardar el POISON de {subject} en la DLQ: {e.Message}. Se reentregará.");
            return;
        }

        await message.AckTerminateAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    private async Task PublishToDlqAsync(
        string subject,
        FluxEvent dlqEvent,
        string msgId,
        CancellationToken cancellationToken)
    {
        var payload = Envelope.Serialize(dlqEvent);
        var ack = await _jetStream.PublishAsync(
            subject: Protocol.DlqSubject(subject),
            data: payload,
            // El msgID incluye el consumidor: dos consumidores distintos del mismo evento
            // producen dos entradas de DLQ, y son dos hechos distintos.
            opts: new NatsJSPubOpts { MsgId = msgId },
            cancellationToken: cancellationToken).ConfigureAwait(false);
        ack.EnsureSuccess();
    }

    /// <summary>
    /// Guarda un mensaje ininterpretable.
    /// </summary>
    /// <remarks>
    /// Un POISON no tiene CloudEvent que preservar, así que se envuelve el cuerpo crudo en
    /// un envelope sintético del dominio <c>system</c>. Es la única excepción a la regla de
    /// "el mensaje de DLQ es el original íntegro": no había original que preservar.
    /// </remarks>
    private async Task PublishRawToDlqAsync(
        string subject,
        byte[] raw,
        string consumer,
        string error,
        CancellationToken cancellationToken)
    {
        var eventId = Protocol.NewEventId().ToString();
        var now = Envelope.FormatTime(DateTimeOffset.UtcNow);

        var encoded = Convert.ToBase64String(raw);
        if (encoded.Length > MaxRawBase64Chars)
        {
            encoded = encoded[..MaxRawBase64Chars];
        }

        var data = JsonSerializer.SerializeToElement(
            new Dictionary<string, object>(StringComparer.Ordinal)
            {
                ["originalSubject"] = subject,
                ["rawBase64"] = encoded,
                ["rawBytes"] = raw.Length,
            },
            Envelope.JsonOptions);

        var envelope = new FluxEvent
        {
            SpecVersion = Protocol.SpecVersion,
            Id = eventId,
            Source = _source,
            Type = PoisonType,
            Time = now,
            DataContentType = Protocol.DataContentType,
            DataSchema = PoisonSchema,
            CorrelationId = eventId,
            TenantId = "system",
            ProducerVersion = _options.Version,
            DataClassification = DataClassification.Internal,
            DlqReason = DlqReason.Poison,
            DlqAttempts = 1,
            DlqConsumer = consumer,
            DlqError = Envelope.Truncate(error, Envelope.MaxDlqErrorChars),
            DlqTime = now,
            Data = data,
        };

        await PublishToDlqAsync(subject, envelope, "poison-" + eventId, cancellationToken).ConfigureAwait(false);
    }

    // ─── provisión de streams ────────────────────────────────────────────────

    private Task EnsureStreamAsync(string domain, CancellationToken cancellationToken)
    {
        var name = Protocol.StreamName(domain);
        return EnsureStreamAsync(
            name,
            () => new StreamConfig(name, new[] { domain + ".>" })
            {
                Storage = StreamConfigStorage.File,
                // LimitsPolicy: el stream retiene por edad, no hasta que alguien consuma. Un
                // consumidor lento no debe poder hacer crecer el disco sin límite.
                Retention = StreamConfigRetention.Limits,
                Discard = StreamConfigDiscard.Old,
                MaxAge = Protocol.StreamMaxAge,
                // Duplicates solo aplica a PUBLICACIONES. Ver Protocol.DuplicateWindow.
                DuplicateWindow = Protocol.DuplicateWindow,
                NumReplicas = _options.StreamReplicas,
            },
            cancellationToken);
    }

    private Task EnsureDlqStreamAsync(string domain, CancellationToken cancellationToken)
    {
        // Prefijo `dlq.`, nunca sufijo: con sufijo encajaría con `<domain>.>` y el stream
        // principal capturaría sus propios muertos — 02-naming.md §3.1.
        var name = Protocol.DlqStreamName(domain);
        return EnsureStreamAsync(
            name,
            () => new StreamConfig(name, new[] { "dlq." + domain + ".>" })
            {
                Storage = StreamConfigStorage.File,
                Retention = StreamConfigRetention.Limits,
                Discard = StreamConfigDiscard.Old,
                // Más larga a propósito: la DLQ es material forense — 02-naming.md §3.3.
                MaxAge = Protocol.DlqStreamMaxAge,
                NumReplicas = _options.StreamReplicas,
            },
            cancellationToken);
    }

    /// <summary>
    /// Crea el stream si no existe, y si existe lo deja TAL CUAL.
    /// </summary>
    /// <remarks>
    /// No se actualiza a propósito: un SDK que actualiza streams ajenos en cada arranque
    /// puede pisar en silencio la configuración que puso el equipo de plataforma (réplicas,
    /// límites, mirrors). En producción los streams los provisiona infraestructura; esto es
    /// una comodidad de desarrollo — 02-naming.md §3.2.
    /// </remarks>
    private async Task EnsureStreamAsync(
        string name,
        Func<StreamConfig> configFactory,
        CancellationToken cancellationToken)
    {
        if (_ensuredStreams.ContainsKey(name))
        {
            return;
        }

        try
        {
            await _jetStream.GetStreamAsync(name, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (NatsJSApiException e) when (e.Error.Code == 404)
        {
            try
            {
                await _jetStream.CreateStreamAsync(configFactory(), cancellationToken).ConfigureAwait(false);
            }
            catch (NatsJSApiException)
            {
                // Otro proceso lo creó entre el GET y el CREATE. No es un error: la carrera
                // es benigna porque ambos piden la misma configuración.
            }
        }

        _ensuredStreams[name] = 0;
    }

    // ─── ciclo de vida ───────────────────────────────────────────────────────

    internal void Forget(Subscription subscription) => _subscriptions.TryRemove(subscription, out _);

    /// <summary>Detiene todas las suscripciones y cierra la conexión.</summary>
    /// <remarks>
    /// Los mensajes sin ack en el momento del cierre se reentregarán, y eso es correcto: es
    /// exactamente el caso que cubre la idempotencia — 03-delivery.md §6.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        foreach (var subscription in _subscriptions.Keys)
        {
            await subscription.UnsubscribeAsync().ConfigureAwait(false);
        }

        _subscriptions.Clear();
        await _connection.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Una suscripción viva y su bucle de consumo.</summary>
    internal sealed class Subscription : IFluxSubscription
    {
        private readonly FluxBus _bus;
        private readonly CancellationTokenSource _cts = new();
        private Task? _loop;
        private int _stopped;

        internal Subscription(FluxBus bus, string subject, string durable)
        {
            _bus = bus;
            Subject = subject;
            Durable = durable;
        }

        /// <inheritdoc />
        public string Subject { get; }

        /// <inheritdoc />
        public string Durable { get; }

        internal void Start(INatsJSConsumer consumer, FluxHandler handler, SubscribeOptions options)
        {
            _loop = Task.Run(() => ConsumeAsync(consumer, handler, options), CancellationToken.None);
        }

        private async Task ConsumeAsync(INatsJSConsumer consumer, FluxHandler handler, SubscribeOptions options)
        {
            try
            {
                await foreach (var message in consumer
                                   .ConsumeAsync<byte[]>(cancellationToken: _cts.Token)
                                   .ConfigureAwait(false))
                {
                    try
                    {
                        await _bus.DispatchAsync(message, Subject, Durable, handler, options, _cts.Token)
                            .ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        // Sin este catch, un fallo inesperado del despacho —por ejemplo que
                        // la publicación en la DLQ falle de una forma no prevista— rompe el
                        // `await foreach` y el consumidor deja de consumir EN SILENCIO: sin
                        // log, y con Connected == true, así que el healthcheck sigue
                        // diciendo que todo va bien. Un consumidor muerto que parece vivo es
                        // peor que uno que se cae. El mensaje no se confirma y se reentrega.
                        _bus._options.Logger?.Error(
                            $"[flux] fallo inesperado despachando {Subject}: {e.Message}. " +
                            "El mensaje no se confirma y será reentregado.");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Unsubscribe o cierre del bus. Salida normal.
            }
            catch (Exception e)
            {
                // El propio bucle murió (conexión perdida de forma irrecuperable, consumidor
                // borrado en el servidor…). Se registra en alto: a partir de aquí este
                // subject ya NO se está consumiendo y alguien tiene que enterarse.
                _bus._options.Logger?.Error(
                    $"[flux] el bucle de consumo de {Subject} ({Durable}) ha terminado con error: " +
                    $"{e.Message}. ESTE SUBJECT YA NO SE ESTÁ CONSUMIENDO.");
            }
        }

        /// <inheritdoc />
        public async Task UnsubscribeAsync()
        {
            if (Interlocked.Exchange(ref _stopped, 1) == 1)
            {
                return;
            }

            await _cts.CancelAsync().ConfigureAwait(false);

            if (_loop is not null)
            {
                try
                {
                    await _loop.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Esperado.
                }
            }

            _cts.Dispose();
            _bus.Forget(this);
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync() => await UnsubscribeAsync().ConfigureAwait(false);
    }
}
