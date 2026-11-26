// Las denegaciones de permisos del broker, que NATS.Net no entrega.
// Contrato normativo: specification/06-security.md §3.7
//
// Cuando una ACL deniega una publicación o una suscripción, el servidor la descarta y manda un
// `-ERR 'Permissions Violation for Publish to "…"'` aparte, por la conexión. No hay respuesta
// que pueda fallar. Medido contra nats-server 2.14.5 con NATS.Net 2.5.16 (A-210):
//
//   · una petición de la API de JetStream denegada NO falla: espera su timeout (5 s) y lanza un
//     NatsJSApiNoResponseException que no dice nada de permisos; un `PublishAsync` de JetStream
//     denegado espera 10,5 s (dos intentos);
//   · un publish o una suscripción de core denegados no lanzan nada;
//   · el `-ERR` no llega a ningún evento ni excepción de la librería: solo se escribe en su
//     ILogger, a nivel Error, con el texto sin comillas (`Server error {Error}`).
//
// La situación es la de `async-nats` en el SDK de Rust, no la de `@nats-io/*` en el de Node, y la
// respuesta es la misma: el SDK ESCUCHA esos avisos (aquí, como el ILoggerFactory que le da a la
// librería) y, mientras espera una respuesta del broker, TAMBIÉN espera una denegación del
// subject al que acaba de publicar. Gana la primera, y una denegación llega en milisegundos.
//
// A diferencia de async-nats, NATS.Net sí tiene `PingAsync` (PING/PONG): como el protocolo es
// ordenado, el `-ERR` de una publicación anterior llega ANTES que el PONG, así que tras un ping
// ya se sabe si algo se denegó. Medido en A-210; no hace falta ningún temporizador.

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Flux;

/// <summary>Qué intentó hacer el cliente cuando el broker denegó.</summary>
internal enum DeniedAction
{
    Publish,
    Subscribe,
}

/// <summary>Una denegación de permisos que el broker comunicó por la conexión.</summary>
internal sealed record Denial(DeniedAction Action, string Subject)
{
    /// <summary>
    /// Interpreta un <c>-ERR</c> del servidor. Devuelve <see langword="null"/> si no es una
    /// denegación de permisos.
    /// </summary>
    /// <remarks>
    /// Formato observado en nats-server 2.14.5, con las comillas simples ya recortadas por
    /// NATS.Net:
    /// <code>
    /// Permissions Violation for Publish to "$JS.API.STREAM.INFO.EVT_PEDIDOS"
    /// Permissions Violation for Subscription to "_INBOX.otro.>"
    /// Permissions Violation for Subscription to "rpc.x.y" using queue "svc"
    /// </code>
    /// </remarks>
    public static Denial? Parse(string? text)
    {
        if (text is null)
        {
            return null;
        }

        text = text.Trim().Trim('\'');
        const string prefijo = "Permissions Violation for ";
        if (!text.StartsWith(prefijo, StringComparison.Ordinal))
        {
            return null;
        }

        var resto = text[prefijo.Length..];
        var corte = resto.IndexOf(" to \"", StringComparison.Ordinal);
        if (corte < 0)
        {
            return null;
        }

        var verbo = resto[..corte];
        var subject = resto[(corte + 5)..];
        var cierre = subject.IndexOf('"');
        if (cierre < 0)
        {
            return null;
        }

        var action = verbo switch
        {
            "Publish" => (DeniedAction?)DeniedAction.Publish,
            "Subscription" => DeniedAction.Subscribe,
            _ => null,
        };

        return action is null ? null : new Denial(action.Value, subject[..cierre]);
    }

    /// <summary>
    /// El subject normalizado para avisar UNA vez: el de un ack (lleva secuencia y marca de tiempo)
    /// pasa a <c>$JS.ACK.&lt;stream&gt;.&lt;durable&gt;.&gt;</c> y el de una respuesta a una petición
    /// (<c>_INBOX.&lt;servicio&gt;.&lt;nuid&gt;.&lt;token&gt;</c>, distinto en cada una) a
    /// <c>_INBOX.&lt;servicio&gt;.&gt;</c>. Sin esto, un dueño sin <c>allow_responses</c> registraría una
    /// línea por cada respuesta denegada y el aviso de «una vez por subject» no avisaría de nada.
    /// </summary>
    public string Normalized()
    {
        if (Subject.StartsWith("$JS.ACK.", StringComparison.Ordinal))
        {
            return string.Join('.', Subject.Split('.').Take(4)) + ".>";
        }

        if (Subject.StartsWith("_INBOX.", StringComparison.Ordinal) && Subject.Split('.').Length > 3)
        {
            return string.Join('.', Subject.Split('.').Take(2)) + ".>";
        }

        return Subject;
    }
}

/// <summary>
/// Registro de las denegaciones del broker, con la capacidad de <b>esperar</b> una. Es el
/// <see cref="ILoggerFactory"/> que se le da a NATS.Net: la librería escribe ahí los <c>-ERR</c>.
/// </summary>
internal sealed class DenialLog : ILoggerFactory
{
    private readonly object _gate = new();
    private readonly List<Watch> _watches = new();
    private readonly ConcurrentDictionary<string, byte> _reported = new(StringComparer.Ordinal);
    private Denial? _inboxDenial;

    /// <summary>Se invoca con cada denegación nueva, en el hilo de lectura de la conexión.</summary>
    public event Action<Denial>? Recorded;

    /// <summary>
    /// La suscripción a <c>_INBOX.&lt;servicio&gt;.…</c> fue denegada: sin ella ninguna respuesta
    /// puede llegar. Es permanente (se queda anotada), porque la suscripción al inbox se hace
    /// una vez, al conectar, y una operación posterior debe enterarse de algo que pasó antes.
    /// </summary>
    public Denial? InboxDenial
    {
        get { lock (_gate) { return _inboxDenial; } }
    }

    /// <summary>Prefijo del inbox privado (<c>_INBOX.&lt;servicio&gt;</c>), para reconocer su suscripción.</summary>
    public string InboxPrefix { get; set; } = "_INBOX.";

    /// <summary>
    /// Anota una denegación, despierta a quien espere y la notifica a <see cref="Recorded"/>.
    /// </summary>
    public void Record(Denial denial)
    {
        List<Watch> afectadas;
        lock (_gate)
        {
            if (denial.Action == DeniedAction.Subscribe &&
                denial.Subject.StartsWith(InboxPrefix + ".", StringComparison.Ordinal))
            {
                _inboxDenial ??= denial;
            }

            afectadas = _watches.Where(w => w.Relevant(denial)).ToList();
        }

        foreach (var watch in afectadas)
        {
            watch.Complete(denial);
        }

        try
        {
            Recorded?.Invoke(denial);
        }
        catch
        {
            // Quien escucha no puede romper el hilo de lectura de la conexión.
        }
    }

    /// <summary>
    /// Si es la primera vez que se ve una denegación de este subject normalizado. Para avisar
    /// UNA vez por subject — 06-security.md §3.7.
    /// </summary>
    public bool FirstTime(Denial denial) => _reported.TryAdd(denial.Normalized(), 0);

    /// <summary>
    /// Olvida lo que se avisó de un subject: una denegación de un intento anterior sobre este
    /// durable no debe condenar al siguiente.
    /// </summary>
    public void Forget(string normalizedSubject) => _reported.TryRemove(normalizedSubject, out _);

    /// <summary>
    /// Empieza a vigilar denegaciones que cumplan <paramref name="relevant"/>. Se crea
    /// <b>antes</b> de la operación cuya denegación se quiere ver: así no hay hueco por el que se
    /// cuele un aviso, y una denegación anterior a la llamada no se le achaca a esta operación.
    /// </summary>
    public Watch StartWatch(Func<Denial, bool> relevant)
    {
        var watch = new Watch(this, relevant);
        lock (_gate)
        {
            _watches.Add(watch);
        }

        return watch;
    }

    internal void Remove(Watch watch)
    {
        lock (_gate)
        {
            _watches.Remove(watch);
        }
    }

    // ─── ILoggerFactory ──────────────────────────────────────────────────────

    /// <inheritdoc />
    public void AddProvider(ILoggerProvider provider)
    {
    }

    /// <inheritdoc />
    public ILogger CreateLogger(string categoryName) => new DenialLogger(this);

    /// <inheritdoc />
    public void Dispose()
    {
    }

    private sealed class DenialLogger(DenialLog log) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        // Solo interesa el nivel Error: es el que NATS.Net usa para los `-ERR` del servidor. La
        // librería también llama a Log con niveles menores aunque IsEnabled diga que no, así que
        // Log lo vuelve a comprobar.
        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Error;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel < LogLevel.Error)
            {
                return;
            }

            // NATS.Net escribe `Server error {Error}` con `Error` ya sin comillas. Se mira el
            // valor estructurado y, por si una versión futura cambia la plantilla, también el
            // texto formateado: es el único sitio donde interpretar texto está justificado.
            string? texto = null;
            if (state is IReadOnlyList<KeyValuePair<string, object?>> pares)
            {
                foreach (var par in pares)
                {
                    if (par.Key == "Error" && par.Value is string valor)
                    {
                        texto = valor;
                        break;
                    }
                }
            }

            var denial = Denial.Parse(texto) ?? Denial.Parse(Extrae(formatter(state, exception)));
            if (denial is not null)
            {
                log.Record(denial);
            }
        }

        private static string? Extrae(string formateado)
        {
            var corte = formateado.IndexOf("Permissions Violation", StringComparison.Ordinal);
            return corte < 0 ? null : formateado[corte..];
        }
    }
}

/// <summary>
/// Una vigilancia de denegaciones: se completa con la primera que cumpla el predicado. Se
/// libera (<see cref="IDisposable.Dispose"/>) al terminar la operación.
/// </summary>
internal sealed class Watch : IDisposable
{
    private readonly DenialLog _log;
    private readonly TaskCompletionSource<Denial> _denied =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Watch(DenialLog log, Func<Denial, bool> relevant)
    {
        _log = log;
        Relevant = relevant;
    }

    /// <summary>Si una denegación invalida lo que se espera.</summary>
    public Func<Denial, bool> Relevant { get; }

    /// <summary>Se completa con la primera denegación relevante.</summary>
    public Task<Denial> Denied => _denied.Task;

    public void Complete(Denial denial) => _denied.TrySetResult(denial);

    public void Dispose() => _log.Remove(this);
}
