// Clasificación de errores del handler.
// Contrato normativo: specification/04-errors.md §2
//
// Este fichero es el punto donde el protocolo se encuentra con la realidad operativa del
// ecosistema. Todo lo demás en el SDK es mecánica; esto es política — y por eso la
// política es un parámetro, no una constante.

using System.Net.Http;
using System.Net.Sockets;

namespace Flux;

/// <summary>
/// Qué hacer con un error que no encaja en ninguna regla conocida — 04-errors.md §2.1.
/// </summary>
/// <remarks>
/// Es un tipo propio y no un <see cref="ErrorClass"/> porque "retryable acotado" NO es una
/// clase del protocolo: es la clase RETRYABLE con un presupuesto distinto. Meterlo en
/// <see cref="ErrorClass"/> contaminaría el valor que acaba escrito en <c>dlqreason</c>.
/// <para>
/// <see cref="RetryableBounded"/> vale 0 para que <c>default(ClassifierOptions)</c>
/// signifique el default de la spec.
/// </para>
/// </remarks>
public enum UnknownPolicy
{
    /// <summary>
    /// El default de la spec: RETRYABLE con presupuesto reducido
    /// (<see cref="ClassifierOptions.UnknownRetryBudget"/>, 2 entregas) en vez de los 6
    /// completos. Un transitorio se recupera en el segundo intento; un sistemático llega a
    /// la DLQ en ~30 s sin atascar la cola.
    /// </summary>
    RetryableBounded = 0,

    /// <summary>
    /// A la DLQ sin gastar reintentos. Falla rápido, pero un hipo de red manda a la DLQ un
    /// evento perfectamente válido y alguien lo reproduce a mano cada mañana.
    /// </summary>
    Permanent = 1,

    /// <summary>
    /// Backoff completo, 51 minutos. Elígelo solo si vuestras dependencias internas tienen
    /// hipos frecuentes y podéis asumir que un modo de fallo nuevo atasque la cola y se
    /// amplifique con cada mensaje que falle igual.
    /// </summary>
    Retryable = 2,
}

/// <summary>La política de clasificación de errores del consumidor.</summary>
public sealed record ClassifierOptions
{
    /// <summary>
    /// Qué hacer con un error que no encaja en ninguna regla conocida. Ver
    /// <see cref="UnknownPolicy"/>.
    /// </summary>
    public UnknownPolicy UnknownErrorPolicy { get; init; } = UnknownPolicy.RetryableBounded;

    /// <summary>
    /// Entregas máximas de un error desconocido cuando la política es
    /// <see cref="UnknownPolicy.RetryableBounded"/>. Incluye la primera entrega, así que
    /// 2 = un reintento. Cero significa el default de la spec
    /// (<see cref="Classifier.DefaultUnknownRetryBudget"/>).
    /// </summary>
    /// <remarks>
    /// NO se traduce a <c>max_deliver</c> del consumidor: eso es por consumidor, no por
    /// mensaje, y recortaría también los reintentos de los RETRYABLE reconocidos. Viaja en
    /// <see cref="Classification.MaxAttempts"/> y lo aplica el runtime a ese error concreto
    /// — 04-errors.md §2.1.
    /// </remarks>
    public int UnknownRetryBudget { get; init; }

    /// <summary>
    /// Un timeout, ¿es "el mundo va lento" o "esta operación no cabe en la ventana"?
    /// </summary>
    /// <remarks>
    /// El default es <see cref="ErrorClass.Retryable"/>: un timeout suele indicar saturación
    /// transitoria. Si vuestros timeouts son casi siempre consultas que nunca van a
    /// terminar, <see cref="ErrorClass.Permanent"/> evita reintentar lo imposible.
    /// </remarks>
    public ErrorClass TimeoutPolicy { get; init; } = ErrorClass.Retryable;

    /// <summary>
    /// Reglas propias, evaluadas antes que todo lo demás salvo los errores que ya declaran
    /// su clase. Devolver <see langword="null"/> cede a la siguiente regla.
    /// </summary>
    public IReadOnlyList<Func<Exception, Classification?>> Rules { get; init; } =
        Array.Empty<Func<Exception, Classification?>>();
}

/// <summary>
/// Traduce un error cualquiera a una de las tres clases del protocolo.
/// </summary>
/// <remarks>
/// El runtime del consumidor usa el resultado así:
/// <list type="bullet">
/// <item><description><see cref="ErrorClass.Retryable"/> → <c>nak(RetryAfter ?? backoff canónico)</c></description></item>
/// <item><description><see cref="ErrorClass.Permanent"/> → <c>term()</c> + publicar en <c>dlq.&lt;subject&gt;</c></description></item>
/// <item><description><see cref="ErrorClass.Poison"/> → <c>term()</c> + publicar en <c>dlq.&lt;subject&gt;</c> + alerta inmediata</description></item>
/// </list>
/// El orden de evaluación es deliberado: lo más específico primero y el default al final.
/// Esa última línea es la decisión de política de verdad.
/// </remarks>
public sealed class Classifier
{
    /// <summary>
    /// Entregas que gasta un error desconocido bajo la política acotada. Incluye la primera
    /// entrega, así que 2 = un reintento.
    /// </summary>
    public const int DefaultUnknownRetryBudget = 2;

    /// <summary>
    /// Status HTTP que merecen reintento — 04-errors.md §1.1.
    /// </summary>
    /// <remarks>
    /// Nótese qué NO está aquí: 400, 403, 404 y 422 son PERMANENT. Reintentarlos es gastar
    /// 51 minutos para obtener exactamente la misma respuesta.
    /// </remarks>
    public static readonly IReadOnlySet<int> RetryableHttpStatus = new HashSet<int> { 429, 502, 503, 504 };

    /// <summary>
    /// Errores de socket inequívocamente transitorios, con su nombre POSIX.
    /// </summary>
    /// <remarks>
    /// La clasificación se hace por <see cref="SocketError"/>, que es el mecanismo
    /// idiomático de .NET, y NO por coincidencia de subcadenas sobre el mensaje del error
    /// — que es justo lo que invita a hacer la lista de códigos de <c>protocol.json</c> y
    /// lo que 04-errors.md §1.1 prohíbe explícitamente.
    /// <para>
    /// Esto además inmuniza al SDK contra el bug real que produjo el port literal de la
    /// lista de Node: en Windows los códigos de libuv llevan prefijo <c>WSA</c>
    /// (<c>WSAECONNRESET</c>), así que el mismo corte de red se clasificaba PERMANENT en
    /// Windows y RETRYABLE en Linux. <see cref="SocketError"/> ya normaliza esa diferencia
    /// en el BCL: el enum es el mismo en las dos plataformas.
    /// </para>
    /// <para>
    /// El nombre POSIX se emite como <see cref="Classification.Code"/> —y no
    /// <c>"ConnectionReset"</c>— para que las métricas de un consumidor .NET se puedan
    /// agregar con las de uno de Node o Go: es el mismo hecho operativo y merece la misma
    /// etiqueta.
    /// </para>
    /// <para>
    /// Nótese qué NO está aquí: <see cref="SocketError.HostNotFound"/>,
    /// <see cref="SocketError.NoData"/> y <see cref="SocketError.NoRecovery"/> son
    /// respuestas DEFINITIVAS del resolutor ("no existe"), no temporales
    /// (<see cref="SocketError.TryAgain"/>, el <c>EAI_AGAIN</c> de la spec). Reintentar un
    /// nombre que no existe son 51 minutos para llegar al mismo sitio.
    /// </para>
    /// </remarks>
    public static readonly IReadOnlyDictionary<SocketError, string> TransientSocketErrors =
        new Dictionary<SocketError, string>
        {
            [SocketError.ConnectionReset] = "ECONNRESET",
            [SocketError.ConnectionRefused] = "ECONNREFUSED",
            [SocketError.ConnectionAborted] = "ECONNABORTED",
            [SocketError.TimedOut] = "ETIMEDOUT",
            [SocketError.HostUnreachable] = "EHOSTUNREACH",
            [SocketError.NetworkUnreachable] = "ENETUNREACH",
            [SocketError.NetworkDown] = "ENETDOWN",
            [SocketError.NetworkReset] = "ENETRESET",
            [SocketError.HostDown] = "EHOSTDOWN",
            [SocketError.TryAgain] = "EAI_AGAIN",
        };

    /// <summary>Profundidad máxima al recorrer la cadena de errores. Corta ciclos.</summary>
    private const int MaxChainDepth = 32;

    private readonly ErrorClass _unknownClass;
    private readonly int _unknownBudget;
    private readonly ErrorClass _timeoutClass;
    private readonly Func<Exception, Classification?>[] _rules;

    /// <summary>Construye un clasificador con la política indicada.</summary>
    /// <param name="options">
    /// Política. <see langword="null"/> significa los defaults de la spec: desconocido →
    /// RETRYABLE acotado a 2 entregas, timeout → RETRYABLE.
    /// </param>
    public Classifier(ClassifierOptions? options = null)
    {
        var opts = options ?? new ClassifierOptions();

        _unknownClass = opts.UnknownErrorPolicy == UnknownPolicy.Permanent
            ? ErrorClass.Permanent
            : ErrorClass.Retryable;

        // Solo la política acotada impone un tope propio; las otras dos dejan mandar al
        // max_deliver del consumidor.
        _unknownBudget = opts.UnknownErrorPolicy == UnknownPolicy.RetryableBounded
            ? (opts.UnknownRetryBudget > 0 ? opts.UnknownRetryBudget : DefaultUnknownRetryBudget)
            : 0;

        _timeoutClass = opts.TimeoutPolicy == ErrorClass.Permanent
            ? ErrorClass.Permanent
            : ErrorClass.Retryable;

        // Copia: la política no cambia bajo los pies si el llamante muta su lista.
        _rules = opts.Rules.ToArray();
    }

    /// <summary>Clasificador con los defaults de la spec.</summary>
    public static Classifier Default { get; } = new();

    /// <summary>
    /// Entregas que el runtime concede a ESTE error concreto.
    /// </summary>
    /// <remarks>
    /// <c>budget = min(consumerMaxDeliver, classification.MaxAttempts &gt; 0 ? MaxAttempts :
    /// consumerMaxDeliver)</c> — 04-errors.md §2.1.
    /// <para>
    /// Es lo que permite que un error DESCONOCIDO se agote en 2 entregas (~30 s) mientras un
    /// RETRYABLE RECONOCIDO (ECONNRESET, HTTP 503) conserva sus 6 entregas y sus 51 minutos:
    /// el tope viaja en la clasificación del error, no en el <c>max_deliver</c> del
    /// consumidor, que es por consumidor y recortaría a los dos por igual.
    /// </para>
    /// <para>
    /// Vive aquí y no dentro del bucle de consumo para que la regla más importante del
    /// runtime se pueda probar sin levantar un broker.
    /// </para>
    /// </remarks>
    /// <param name="consumerMaxDeliver">El <c>max_deliver</c> del consumidor.</param>
    /// <param name="classification">La clasificación del error que acaba de fallar.</param>
    public static int EffectiveBudget(int consumerMaxDeliver, Classification classification)
    {
        ArgumentNullException.ThrowIfNull(classification);
        return classification.MaxAttempts > 0
            ? Math.Min(consumerMaxDeliver, classification.MaxAttempts)
            : consumerMaxDeliver;
    }

    /// <summary>Clasifica un error del handler.</summary>
    public Classification Classify(Exception? error)
    {
        if (error is null)
        {
            // No debería ocurrir —el runtime solo clasifica errores— pero devolver una
            // clase falsa sería peor que devolver algo inerte.
            return new Classification(ErrorClass.Permanent, "NIL_ERROR");
        }

        // 1. Un error tipado de flux siempre gana: la aplicación sabe más que el SDK.
        //    Se busca en toda la cadena, así que un RetryableException envuelto por una
        //    capa intermedia sigue clasificándose bien — cosa que el `instanceof` de Node,
        //    que solo mira el error de arriba, no consigue.
        var classified = FindInChain<ClassifiedException>(error);
        if (classified is not null)
        {
            var result = new Classification(classified.FluxClass, classified.FluxCode);
            return classified.FluxClass == ErrorClass.Retryable
                ? result with { RetryAfter = FindInChain<IRetryAfterError>(error)?.RetryAfter }
                : result;
        }

        // 2. Reglas de la aplicación.
        foreach (var rule in _rules)
        {
            var r = rule(error);
            if (r is not null)
            {
                return r;
            }
        }

        // 3. Status HTTP: la señal más fiable que da una dependencia.
        //    Primero el contrato explícito del SDK, y si no, el del BCL: desde .NET 5
        //    HttpRequestException lleva el StatusCode, así que la mayoría de aplicaciones
        //    no tienen que implementar nada.
        var status = FindInChain<IHttpStatusError>(error)?.HttpStatus
                     ?? (int?)FindInChain<HttpRequestException>(error)?.StatusCode;
        if (status is not null)
        {
            var code = "HTTP_" + status.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            if (!RetryableHttpStatus.Contains(status.Value))
            {
                return new Classification(ErrorClass.Permanent, code);
            }

            return new Classification(ErrorClass.Retryable, code)
            {
                RetryAfter = FindInChain<IRetryAfterError>(error)?.RetryAfter,
            };
        }

        // 4. Errores de red: transitorios por semántica, no por lista de códigos.
        var socket = FindInChain<SocketException>(error);
        if (socket is not null && TransientSocketErrors.TryGetValue(socket.SocketErrorCode, out var posix))
        {
            return new Classification(ErrorClass.Retryable, posix);
        }

        // 5. Fallo de transporte HTTP sin respuesta: la petición nunca llegó a obtener un
        //    status (DNS, conexión, TLS). Es "el mundo ahora mismo", no el evento.
        if (FindInChain<HttpRequestException>(error) is not null)
        {
            return new Classification(ErrorClass.Retryable, "HTTP_TRANSPORT");
        }

        // 6. Timeouts — política configurable.
        //
        //    ⚠️ Fricción de .NET: OperationCanceledException NO distingue "se agotó el
        //    plazo" de "el llamante canceló". HttpClient lanza TaskCanceledException en
        //    ambos casos. Se tratan todos como timeout porque en un handler de flux la
        //    cancelación viene del apagado del proceso, y entonces no confirmar el mensaje
        //    y dejar que se reentregue es exactamente lo correcto.
        if (FindInChain<TimeoutException>(error) is not null ||
            FindInChain<OperationCanceledException>(error) is not null)
        {
            return new Classification(_timeoutClass, "TIMEOUT");
        }

        // 7. Lo desconocido. Aquí se decide el comportamiento del ecosistema ante lo que
        //    nadie previó. El default acotado da al transitorio una segunda oportunidad sin
        //    regalarle 51 minutos de cola al sistemático — y el tope viaja en la
        //    clasificación, no en max_deliver, para no recortar los reintentos de los
        //    RETRYABLE reconocidos — 04-errors.md §2.1.
        return new Classification(_unknownClass, "UNKNOWN") { MaxAttempts = _unknownBudget };
    }

    /// <summary>
    /// Busca el primer error de tipo <typeparamref name="T"/> en la cadena.
    /// </summary>
    /// <remarks>
    /// Equivale al <c>errors.As</c> de Go y atraviesa tanto
    /// <see cref="Exception.InnerException"/> como las ramas de
    /// <see cref="AggregateException"/>, que es como llegan los fallos de un
    /// <c>Task.WhenAll</c> dentro de un handler.
    /// </remarks>
    private static T? FindInChain<T>(Exception? error, int depth = 0)
        where T : class
    {
        if (error is null || depth > MaxChainDepth)
        {
            return null;
        }

        if (error is T match)
        {
            return match;
        }

        if (error is AggregateException aggregate)
        {
            foreach (var inner in aggregate.InnerExceptions)
            {
                var found = FindInChain<T>(inner, depth + 1);
                if (found is not null)
                {
                    return found;
                }
            }

            return null;
        }

        return FindInChain<T>(error.InnerException, depth + 1);
    }
}
