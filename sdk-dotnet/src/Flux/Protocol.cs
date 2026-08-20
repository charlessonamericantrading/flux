// Constantes verificadas y derivaciones de naming del flux Event Protocol v1.
// Contrato normativo: specification/02-naming.md, specification/03-delivery.md
//
// Todo lo de aquí sale de protocol.json. Si divergen, protocol.json manda: es lo que
// consumen los demás SDKs y los agentes de IA.

using System.Collections.ObjectModel;
using System.Globalization;
using System.Security.Cryptography;
using System.Text.RegularExpressions;

namespace Flux;

/// <summary>
/// Constantes del protocolo y transformaciones de nombres.
/// </summary>
/// <remarks>
/// Clase estática: no se instancia. Todas las funciones son puras salvo
/// <see cref="NewEventId"/>, que consulta el reloj.
/// </remarks>
public static class Protocol
{
    // ─── Constantes del protocolo ────────────────────────────────────────────

    /// <summary>Identifica el contrato, no este SDK.</summary>
    public const string ProtocolName = "flux";

    /// <summary>Versión del contrato, no de este paquete.</summary>
    public const string ProtocolVersion = "1.0.0";

    /// <summary>Literal exigido por CloudEvents — 01-envelope.md §2.</summary>
    public const string SpecVersion = "1.0";

    /// <summary>flux v1 solo admite JSON — 01-envelope.md §2.</summary>
    public const string DataContentType = "application/json";

    /// <summary>Nivel de conformidad declarado por este SDK — 00-protocol.md §5.</summary>
    public const string ConformanceLevel = "L2";

    /// <summary>
    /// Techo del mensaje serializado. Por encima, claim-check: se publica
    /// <c>{uri, sha256, bytes}</c>, no el contenido — 01-envelope.md §6.
    /// </summary>
    public const int MaxMessageBytes = 1_048_576;

    // ─── Configuración canónica de consumidor — 03-delivery.md §2 ────────────

    /// <summary>
    /// DEBE coincidir con <c>CanonicalBackoff[0]</c>.
    /// </summary>
    /// <remarks>
    /// ⚠️ JetStream SOBRESCRIBE <c>ack_wait</c> con <c>backoff[0]</c> y no devuelve
    /// error: pides 30 s con un backoff que empieza en 1 s y obtienes un
    /// <c>ack_wait</c> efectivo de 1 s. Cualquier handler que toque una base de datos
    /// se ejecuta entonces en concurrencia consigo mismo, en cada mensaje, sin ninguna
    /// señal visible. Verificado contra nats-server 2.14.5 — ver 03-delivery.md §2.1 y
    /// <c>conformance/cases/consumer-config.json</c>.
    /// <para>
    /// Consecuencia de diseño: <c>backoff[0]</c> ES el presupuesto de duración del
    /// handler. Por eso el backoff canónico empieza en 30 s y no en 1 s; un primer
    /// reintento rápido es imposible por construcción, y buscarlo es lo que rompe la
    /// configuración.
    /// </para>
    /// </remarks>
    public static readonly TimeSpan DefaultAckWait = TimeSpan.FromSeconds(30);

    /// <summary>
    /// 1 entrega inicial + 5 reintentos, uno por entrada de <see cref="CanonicalBackoff"/>.
    /// Si fuese 5, la última entrada (30 m) no se aplicaría nunca y la configuración
    /// mentiría sobre su propio comportamiento.
    /// </summary>
    public const int DefaultMaxDeliver = 6;

    /// <summary>
    /// Ojo: un mensaje esperando reintento ocupa una ranura, así que con backoffs
    /// largos y mucho fallo simultáneo esta ventana se llena — 03-delivery.md §2.1,
    /// nota final.
    /// </summary>
    public const int DefaultMaxAckPending = 256;

    /// <summary>
    /// Backoff canónico <c>[30s, 1m, 5m, 15m, 30m]</c>. Tiempo total hasta la DLQ
    /// ≈ 51 min 30 s.
    /// </summary>
    /// <remarks>
    /// Es una decisión de producto: cuánto tiempo aceptas que un fallo transitorio siga
    /// reintentando antes de que un humano se entere. Solo lo recorren los RETRYABLE;
    /// un PERMANENT no gasta ni un reintento.
    /// <para>
    /// Diferencia de port con Go: allí <c>CanonicalBackOff()</c> construye un slice nuevo
    /// en cada llamada porque un slice a nivel de paquete sería mutable desde fuera, y
    /// una entrada [0] alterada cambiaría en silencio el <c>ack_wait</c> efectivo de todo
    /// consumidor creado después. <see cref="ReadOnlyCollection{T}"/> encapsula el array
    /// —el llamante no puede recuperarlo con un cast— así que aquí se comparte una única
    /// instancia sin riesgo, igual que hizo Java con <c>List.of</c>.
    /// </para>
    /// </remarks>
    public static readonly ReadOnlyCollection<TimeSpan> CanonicalBackoff = Array.AsReadOnly(
        new[]
        {
            TimeSpan.FromSeconds(30),
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(5),
            TimeSpan.FromMinutes(15),
            TimeSpan.FromMinutes(30),
        });

    static Protocol()
    {
        // La invariante más cara del protocolo no puede expresarse en el sistema de
        // tipos de ningún lenguaje (ver README §"Fricciones", G). Aquí se comprueba al
        // inicializar la clase: si alguien toca el backoff y olvida DefaultAckWait, el
        // proceso no arranca —con TypeInitializationException— en vez de fallar en
        // producción a las 3 de la mañana con reentregas concurrentes y ningún error.
        if (DefaultAckWait != CanonicalBackoff[0])
        {
            throw new InvalidOperationException(
                $"invariante rota: ack_wait ({DefaultAckWait}) debe ser igual a backoff[0] " +
                $"({CanonicalBackoff[0]}) — JetStream sobrescribe ack_wait con backoff[0] sin " +
                "avisar (03-delivery.md §2.1)");
        }

        if (CanonicalBackoff.Count != DefaultMaxDeliver - 1)
        {
            throw new InvalidOperationException(
                $"invariante rota: max_deliver ({DefaultMaxDeliver}) = 1 entrega inicial + " +
                $"{CanonicalBackoff.Count} reintentos; con otro número la última entrada de " +
                "backoff nunca se aplicaría (03-delivery.md §2)");
        }
    }

    /// <summary>
    /// Cada cuánto se emite <c>work-in-progress</c> mientras el handler vive.
    /// </summary>
    /// <remarks>
    /// La mitad de <see cref="DefaultAckWait"/> para que un ciclo perdido —un GC largo, un
    /// hilo hambriento— no agote el plazo. Un handler que supere los 30 s sin WIP recibe el
    /// mismo mensaje reentregado mientras aún se está ejecutando — 03-delivery.md §2.1.
    /// </remarks>
    public static readonly TimeSpan WorkInProgressInterval = TimeSpan.FromTicks(DefaultAckWait.Ticks / 2);

    /// <summary>Suma del backoff canónico: lo que tarda un RETRYABLE en caer en la DLQ.</summary>
    public static TimeSpan TotalTimeToDlq()
    {
        var total = TimeSpan.Zero;
        foreach (var d in CanonicalBackoff)
        {
            total += d;
        }

        return total;
    }

    // ─── Configuración canónica de stream — 02-naming.md §3.2 ────────────────

    /// <summary>Retención del stream de eventos.</summary>
    public static readonly TimeSpan StreamMaxAge = TimeSpan.FromDays(30);

    /// <summary>
    /// Más larga a propósito: la DLQ es material forense. Pero es un límite real, no un
    /// archivo — a los 90 días el evento desaparece.
    /// </summary>
    public static readonly TimeSpan DlqStreamMaxAge = TimeSpan.FromDays(90);

    /// <summary>
    /// Deduplica PUBLICACIONES con el mismo <c>Nats-Msg-Id</c>. NO deduplica reentregas
    /// de consumo, y nunca sustituye a la idempotencia del consumidor: es el
    /// malentendido más frecuente del protocolo — 03-delivery.md §3.
    /// </summary>
    public static readonly TimeSpan DuplicateWindow = TimeSpan.FromMinutes(2);

    // ─── Naming ──────────────────────────────────────────────────────────────

    /// <summary>Valida <c>&lt;dominio&gt;.&lt;agregado&gt;.v&lt;major&gt;.&lt;evento&gt;</c> — 02-naming.md §1.</summary>
    public static readonly Regex SubjectPattern = new(
        @"^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]*\.[a-z0-9]+(-[a-z0-9]+)*$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>
    /// Nombre de servicio válido: kebab-case en minúsculas.
    /// </summary>
    /// <remarks>
    /// NATS aceptaría sin rechistar un durable como <c>FacturacionAPI__pedidos_…</c>, así
    /// que el incumplimiento del patrón solo se descubriría al intentar parsear nombres
    /// de consumidor en una herramienta — protocol.json <c>naming.service</c>.
    /// </remarks>
    public static readonly Regex ServicePattern = new(
        @"^[a-z0-9]+(-[a-z0-9]+)*$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>Los cuatro tokens de un subject ya validado.</summary>
    /// <param name="Domain">Bounded context. Sustantivo plural en kebab-case.</param>
    /// <param name="Aggregate">Raíz de agregado. Sustantivo singular en kebab-case.</param>
    /// <param name="Major">Versión mayor del contrato, entero ≥ 1.</param>
    /// <param name="Event">Hecho en pasado, kebab-case.</param>
    public readonly record struct ParsedSubject(string Domain, string Aggregate, int Major, string Event)
    {
        /// <summary>Reconstruye el subject original. La transformación es biyectiva.</summary>
        public string ToSubject() =>
            string.Create(CultureInfo.InvariantCulture, $"{Domain}.{Aggregate}.v{Major}.{Event}");
    }

    /// <summary>
    /// Valida y descompone un subject de NATS.
    /// </summary>
    /// <remarks>
    /// ⚠️ No confundir con el atributo <c>subject</c> de CloudEvents, que es el ID DEL
    /// AGREGADO (<c>"ped-123"</c>). En este SDK ese atributo se llama <c>AggregateId</c>
    /// y solo se mapea a <c>subject</c> al serializar — 01-envelope.md §2.1.
    /// </remarks>
    /// <exception cref="InvalidSubjectException">El subject no cumple 02-naming.md §1.</exception>
    public static ParsedSubject ParseSubject(string? subject)
    {
        if (string.IsNullOrEmpty(subject))
        {
            throw new InvalidSubjectException(subject ?? "null", "el subject es obligatorio");
        }

        // La comprobación de minúsculas va ANTES que el patrón para poder dar un mensaje
        // útil: los subjects de NATS son case-sensitive, así que "Pedidos.pedido.v1.creado"
        // crea un subject fantasma al que nadie está suscrito y no produce ningún error.
        // Sin este mensaje, el desarrollador solo ve "no llegan mis eventos".
        //
        // ToLowerInvariant y NO ToLower(): con la cultura del proceso en turco,
        // "I".ToLower() da "ı" (i sin punto) y el subject resultante sería exactamente el
        // fantasma que esta comprobación existe para evitar. Es la misma trampa que Java
        // documenta con Locale.ROOT.
        if (!string.Equals(subject, subject.ToLowerInvariant(), StringComparison.Ordinal))
        {
            throw new InvalidSubjectException(
                subject,
                "debe ir todo en minúsculas — NATS es case-sensitive y una mayúscula crea un " +
                "subject al que nadie está suscrito, sin producir error");
        }

        if (!SubjectPattern.IsMatch(subject))
        {
            var tokens = subject.Split('.').Length;
            var reason = tokens != 4
                ? $"debe tener exactamente 4 tokens (<dominio>.<agregado>.v<major>.<evento>), tiene {tokens}"
                : "formato esperado <dominio>.<agregado>.v<major>.<evento> en kebab-case";
            throw new InvalidSubjectException(subject, reason);
        }

        var t = subject.Split('.');
        // El patrón ya garantizó `v` + entero ≥ 1, así que el Parse no puede fallar.
        var major = int.Parse(t[2].AsSpan(1), CultureInfo.InvariantCulture);
        return new ParsedSubject(t[0], t[1], major, t[3]);
    }

    /// <summary>Informa si el subject cumple el contrato, sin exponer el motivo.</summary>
    public static bool IsValidSubject(string? subject)
    {
        try
        {
            ParseSubject(subject);
            return true;
        }
        catch (InvalidSubjectException)
        {
            return false;
        }
    }

    /// <summary>
    /// Deriva el <c>type</c> de CloudEvents:
    /// <c>pedidos.pedido.v1.creado</c> → <c>com.flux.pedidos.pedido.creado.v1</c>
    /// — 02-naming.md §2.
    /// </summary>
    /// <remarks>
    /// Los dos formatos existen porque sirven a consumidores distintos: el subject enruta
    /// y necesita la versión en posición fija para que los wildcards funcionen; el
    /// <c>type</c> identifica el contrato en un catálogo y ahí lee mejor con la versión al
    /// final. La transformación es mecánica, así que jamás se le pide al desarrollador.
    /// </remarks>
    public static string SubjectToType(string subject)
    {
        var p = ParseSubject(subject);
        return string.Create(
            CultureInfo.InvariantCulture,
            $"com.flux.{p.Domain}.{p.Aggregate}.{p.Event}.v{p.Major}");
    }

    /// <summary>
    /// Inversa de <see cref="SubjectToType"/>. Útil en un handler suscrito con wildcard
    /// que necesita saber qué evento concreto llegó.
    /// </summary>
    public static ParsedSubject ParseType(string? type)
    {
        const string prefix = "com.flux.";
        if (type is null || !type.StartsWith(prefix, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"type \"{type}\" no sigue el formato com.flux.<dominio>.<agregado>.<evento>.v<major>",
                nameof(type));
        }

        var t = type[prefix.Length..].Split('.');
        if (t.Length != 4)
        {
            throw new ArgumentException(
                $"type \"{type}\" no tiene los 4 tokens esperados tras com.flux.",
                nameof(type));
        }

        // El `type` lleva la versión al final y el subject en tercera posición: se
        // reordena antes de validar.
        return ParseSubject($"{t[0]}.{t[1]}.{t[3]}.{t[2]}");
    }

    /// <summary>
    /// <c>EVT_PEDIDOS</c> para el dominio <c>pedidos</c>.
    /// </summary>
    /// <remarks>
    /// NATS no admite <c>.</c>, <c>*</c>, <c>&gt;</c>, <c>/</c>, <c>\</c> ni espacios en
    /// nombres de stream: de ahí el guion bajo. Las mayúsculas son convención, para
    /// distinguir de un vistazo un stream de un subject en los logs — 02-naming.md §3.
    /// </remarks>
    public static string StreamName(string domain) =>
        "EVT_" + domain.Replace('-', '_').ToUpperInvariant();

    /// <summary><c>DLQ_PEDIDOS</c> — 02-naming.md §3.</summary>
    public static string DlqStreamName(string domain) =>
        "DLQ_" + domain.Replace('-', '_').ToUpperInvariant();

    /// <summary>
    /// <c>facturacion-api__pedidos_pedido_v1_creado</c>.
    /// </summary>
    /// <remarks>
    /// NATS tampoco admite <c>.</c> en nombres de durable consumer. Separar el servicio
    /// con <c>__</c> y los tokens con <c>_</c> mantiene la reversibilidad: partiendo por
    /// <c>__</c> recuperas servicio y subject exactos. Un nombre de consumidor que no dice
    /// qué servicio lo tiene abierto es inútil en <c>nats consumer ls</c> a las 3 de la
    /// mañana — 02-naming.md §4.
    /// <para>
    /// Se valida TAMBIÉN el nombre de servicio, no solo el subject: NATS aceptaría
    /// <c>FacturacionAPI__pedidos_…</c> sin error y el incumplimiento solo se descubriría
    /// al parsear nombres en una herramienta.
    /// </para>
    /// </remarks>
    public static string DurableName(string service, string subject)
    {
        ValidateService(service);
        ParseSubject(subject);
        return service + "__" + subject.Replace('.', '_').Replace('-', '_');
    }

    /// <summary>Valida el nombre de servicio contra <see cref="ServicePattern"/>.</summary>
    /// <exception cref="InvalidServiceNameException">El nombre no es kebab-case en minúsculas.</exception>
    public static void ValidateService(string? service)
    {
        if (service is null || !ServicePattern.IsMatch(service))
        {
            throw new InvalidServiceNameException(service ?? "null");
        }
    }

    /// <summary>
    /// Antepone <c>dlq.</c> al subject original.
    /// </summary>
    /// <remarks>
    /// PREFIJO, nunca sufijo. Un sufijo (<c>pedidos.pedido.v1.creado.dlq</c>) encajaría
    /// con <c>pedidos.&gt;</c> y el stream EVT_PEDIDOS capturaría sus propios muertos:
    /// contarían contra su retención, un consumidor de <c>pedidos.pedido.v1.&gt;</c> los
    /// recibiría, y un replay masivo podría reinyectarse en su propia DLQ
    /// — 02-naming.md §3.1.
    /// </remarks>
    public static string DlqSubject(string subject) => "dlq." + subject;

    /// <summary>Informa si el subject pertenece al espacio de nombres de la DLQ.</summary>
    public static bool IsDlqSubject(string? subject) =>
        subject is not null && subject.StartsWith("dlq.", StringComparison.Ordinal);

    /// <summary>
    /// <c>/produccion/pedidos-api</c> — 01-envelope.md §2.
    /// </summary>
    /// <remarks>
    /// <c>id</c> + <c>source</c> son la clave de deduplicación del ecosistema entero, así
    /// que el <c>source</c> tiene que identificar de forma estable entorno y servicio.
    /// </remarks>
    public static string SourceUri(string environment, string service) =>
        "/" + environment + "/" + service;

    // ─── UUIDv7 ──────────────────────────────────────────────────────────────

    private static readonly object UuidLock = new();
    private static long _lastTimestampMillis = -1L;
    private static int _sequence;

    /// <summary>
    /// Genera el <c>id</c> del evento: un UUIDv7 (RFC 9562) en formato canónico.
    /// </summary>
    /// <remarks>
    /// Hay que implementarlo: <see cref="Guid.NewGuid"/> es v4 —aleatorio puro— y
    /// <c>Guid.CreateVersion7()</c> solo existe a partir de .NET 9. Este SDK apunta a
    /// .NET 8, así que se genera aquí. Cuando el consumidor mueva el TFM a net9.0 puede
    /// sustituirse por la API del framework sin cambiar el formato del <c>id</c>.
    /// <para>
    /// El protocolo se apoya en que el <c>id</c> sea monotónico en el tiempo: ordenar por
    /// <c>id</c> dentro de un mismo <c>source</c> equivale a ordenar por instante de
    /// generación, que es lo que permite reconstruir historiales desde la DLQ
    /// — 01-envelope.md §2.4.
    /// </para>
    /// <para>
    /// Disposición de bits: 48 bits de milisegundos Unix, 4 de versión (7), 12 de
    /// <c>rand_a</c>, 2 de variante (10b) y 62 de <c>rand_b</c>. Los 12 bits de
    /// <c>rand_a</c> se usan como CONTADOR dentro del mismo milisegundo (método 2 de la
    /// RFC) en vez de como aleatorio: sin eso, dos eventos publicados en el mismo
    /// milisegundo pueden salir desordenados y la propiedad de arriba deja de cumplirse
    /// justo cuando más se usa (una ráfaga).
    /// </para>
    /// <para>
    /// El Guid se construye por componentes y no desde un <c>byte[]</c> a propósito: el
    /// constructor <c>Guid(byte[])</c> interpreta los tres primeros campos en
    /// little-endian, así que un array en orden RFC saldría con el timestamp con los bytes
    /// invertidos en la representación canónica — y el id dejaría de ser ordenable como
    /// texto, que es justo lo que se busca.
    /// </para>
    /// </remarks>
    public static Guid NewEventId()
    {
        long now;
        int sequence;

        lock (UuidLock)
        {
            now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            if (now == _lastTimestampMillis)
            {
                _sequence++;
                if (_sequence > 0xFFF)
                {
                    // Contador agotado (> 4 M eventos/s en un solo proceso): esperar al
                    // siguiente milisegundo es preferible a emitir un id que rompa el orden.
                    while (now == _lastTimestampMillis)
                    {
                        Thread.SpinWait(1);
                        now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    }

                    _lastTimestampMillis = now;
                    _sequence = 0;
                }
            }
            else if (now > _lastTimestampMillis)
            {
                _lastTimestampMillis = now;
                _sequence = 0;
            }
            else
            {
                // El reloj ha ido hacia atrás (NTP). Se mantiene el último timestamp y se
                // sigue contando: un id ligeramente adelantado es mucho menos dañino que
                // una secuencia de ids que retrocede.
                _sequence++;
                now = _lastTimestampMillis;
                if (_sequence > 0xFFF)
                {
                    _lastTimestampMillis++;
                    now = _lastTimestampMillis;
                    _sequence = 0;
                }
            }

            sequence = _sequence;
        }

        Span<byte> random = stackalloc byte[8];
        RandomNumberGenerator.Fill(random);

        // Campo a (8 hex) = bits 47..16 del timestamp; campo b (4 hex) = bits 15..0.
        // Guid.ToString() imprime a, b y c en big-endian, así que el orden canónico del
        // texto coincide con el orden temporal.
        var a = unchecked((int)(uint)((now >> 16) & 0xFFFF_FFFFL));
        var b = unchecked((short)(ushort)(now & 0xFFFFL));
        var c = unchecked((short)(ushort)(0x7000 | (sequence & 0x0FFF)));

        // Variante RFC 9562: los dos bits altos del primer byte del cuarto campo son 10b.
        var d = (byte)(0x80 | (random[0] & 0x3F));

        return new Guid(a, b, c, d, random[1], random[2], random[3], random[4], random[5], random[6], random[7]);
    }
}

/// <summary>
/// Subject que no cumple 02-naming.md §1.
/// </summary>
/// <remarks>
/// Deriva de <see cref="ArgumentException"/> y no de una excepción propia de dominio a
/// propósito: un subject mal escrito es un bug del código que llama, no una condición de
/// ejecución que la aplicación deba manejar. Se detecta en el primer test.
/// </remarks>
public sealed class InvalidSubjectException : ArgumentException
{
    /// <summary>Construye la excepción con el subject ofensivo y el motivo.</summary>
    public InvalidSubjectException(string subject, string reason)
        : base($"subject inválido \"{subject}\": {reason}")
    {
        Subject = subject;
        Reason = reason;
    }

    /// <summary>El subject que se rechazó.</summary>
    public string Subject { get; }

    /// <summary>Por qué se rechazó.</summary>
    public string Reason { get; }
}

/// <summary>Nombre de servicio que no cumple el patrón del protocolo.</summary>
public sealed class InvalidServiceNameException : ArgumentException
{
    /// <summary>Construye la excepción con el nombre de servicio ofensivo.</summary>
    public InvalidServiceNameException(string service)
        : base(
            $"nombre de servicio inválido \"{service}\": debe ser kebab-case en minúsculas " +
            "([a-z0-9-]). NATS aceptaría el durable resultante, pero incumpliría el patrón del " +
            "protocolo y rompería el filtrado por servicio en `nats consumer ls` (02-naming.md §4).")
    {
        Service = service;
    }

    /// <summary>El nombre de servicio que se rechazó.</summary>
    public string Service { get; }
}
