// Taxonomía de errores de flux.
// Contrato normativo: specification/04-errors.md
//
// La distinción entre RETRYABLE y PERMANENT es la decisión de diseño más importante de
// un consumidor. Equivocarla en un sentido atasca la cola; equivocarla en el otro tira
// eventos buenos a la basura por un hipo de red.

namespace Flux;

/// <summary>
/// Un error que ya declara su propia clase de flux.
/// </summary>
/// <remarks>
/// La aplicación lo lanza —normalmente vía los tipos de abajo— para anular al
/// clasificador, porque solo ella conoce sus dependencias — 04-errors.md §2.
/// <para>
/// Es una clase base abstracta y no una interfaz a propósito: en .NET lo que se lanza
/// son <see cref="Exception"/>, y una jerarquía permite escribir
/// <c>catch (ClassifiedException)</c> en la aplicación. El clasificador la busca además a
/// través de <see cref="Exception.InnerException"/> y de
/// <see cref="AggregateException"/>, cosa que un <c>is</c> sobre el error de arriba —lo
/// que hace el <c>instanceof</c> de Node— no consigue.
/// </para>
/// </remarks>
public abstract class ClassifiedException : Exception
{
    /// <summary>Construye la excepción.</summary>
    /// <param name="message">Mensaje para el operador.</param>
    /// <param name="code">Código estable para métricas y alertas.</param>
    /// <param name="innerException">Error subyacente, si lo hay.</param>
    protected ClassifiedException(string message, string? code, Exception? innerException)
        : base(message, innerException)
    {
        Code = code;
    }

    /// <summary>La clase declarada por el error.</summary>
    public abstract ErrorClass FluxClass { get; }

    /// <summary>
    /// Código estable para métricas y alertas (<c>"HTTP_503"</c>,
    /// <c>"PEDIDO_YA_CANCELADO"</c>). El mensaje del error cambia; el código no debería.
    /// </summary>
    public string? Code { get; }

    /// <summary>
    /// El código efectivo. Sin código explícito se cae al nombre del tipo, para que las
    /// métricas nunca queden sin etiqueta.
    /// </summary>
    public string FluxCode => string.IsNullOrEmpty(Code) ? GetType().Name : Code;
}

/// <summary>
/// La aplicación lo lanza para forzar un reintento. Úsalo cuando SABES que el fallo es
/// transitorio: timeout de red, 503 de un proveedor, deadlock de BD.
/// </summary>
/// <example>
/// <code>
/// throw new RetryableException("proveedor 503", retryAfter: TimeSpan.FromSeconds(5));
/// </code>
/// </example>
public sealed class RetryableException : ClassifiedException, IRetryAfterError
{
    /// <summary>Construye la excepción.</summary>
    /// <param name="message">Mensaje para el operador.</param>
    /// <param name="code">Código estable para métricas y alertas.</param>
    /// <param name="retryAfter">
    /// <b>Sugerencia para el PRIMER reintento</b>, no un control del calendario de
    /// reintentos. Úsalo cuando la dependencia dice cuánto esperar (cabecera
    /// <c>Retry-After</c>). <see langword="null"/> significa "usa el backoff canónico".
    /// ⚠️ Con <c>backoff</c> configurado —y flux lo configura SIEMPRE— JetStream honra el
    /// delay del <c>nak</c> solo en la primera reentrega; a partir de la segunda manda el
    /// array <c>backoff</c> y el delay solicitado se ignora sin ningún aviso. Medido contra
    /// NATS 2.14.5 — 03-delivery.md §2.2.
    /// </param>
    /// <param name="innerException">Error subyacente, si lo hay.</param>
    public RetryableException(
        string message,
        string? code = null,
        TimeSpan? retryAfter = null,
        Exception? innerException = null)
        : base(message, code, innerException)
    {
        RetryAfter = retryAfter;
    }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Retryable;

    /// <inheritdoc />
    public TimeSpan? RetryAfter { get; }
}

/// <summary>
/// La aplicación lo lanza para ir directo a la DLQ sin gastar reintentos. Úsalo cuando el
/// evento es válido pero tu lógica lo rechaza de forma definitiva: reintentarlo son 51
/// minutos de cola bloqueada para llegar al mismo sitio.
/// </summary>
/// <example>
/// <code>
/// throw new PermanentException("pedido ya cancelado", code: "PEDIDO_YA_CANCELADO");
/// </code>
/// </example>
public sealed class PermanentException : ClassifiedException
{
    /// <summary>Construye la excepción.</summary>
    /// <param name="message">Mensaje para el operador.</param>
    /// <param name="code">Código estable para métricas y alertas.</param>
    /// <param name="innerException">Error subyacente, si lo hay.</param>
    public PermanentException(string message, string? code = null, Exception? innerException = null)
        : base(message, code, innerException)
    {
    }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Permanent;
}

/// <summary>
/// Lo produce el SDK, no la aplicación: el mensaje no pudo interpretarse como CloudEvent,
/// así que el handler nunca llega a verlo — 04-errors.md §1.3.
/// </summary>
/// <remarks>
/// Un POISON casi siempre significa que un productor está roto o que alguien publicó a
/// mano en el subject equivocado. Es el único caso que DEBE despertar a alguien.
/// </remarks>
public sealed class PoisonException : ClassifiedException
{
    /// <summary>Construye la excepción.</summary>
    /// <param name="message">Mensaje para el operador.</param>
    /// <param name="code">Código estable para métricas y alertas.</param>
    /// <param name="innerException">Error subyacente, si lo hay.</param>
    public PoisonException(string message, string? code = null, Exception? innerException = null)
        : base(message, code, innerException)
    {
    }

    /// <inheritdoc />
    public override ErrorClass FluxClass => ErrorClass.Poison;
}

/// <summary>
/// Un envelope que el SDK se niega a construir o serializar.
/// </summary>
/// <remarks>
/// A diferencia de <see cref="PoisonException"/>, se produce del lado del PRODUCTOR: es un
/// bug local, no un mensaje ajeno corrupto. No tiene clase de flux porque nunca llega a
/// haber un mensaje que clasificar.
/// </remarks>
public sealed class EnvelopeException : Exception
{
    /// <summary>Construye la excepción.</summary>
    public EnvelopeException(string message)
        : base(message)
    {
    }

    /// <summary>Construye la excepción encadenando la causa.</summary>
    public EnvelopeException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Un error que sabe con qué status HTTP falló una dependencia. El status es la señal más
/// fiable que da una dependencia sobre si merece la pena reintentar.
/// </summary>
/// <remarks>
/// En .NET esta interfaz es un complemento, no la vía principal: el clasificador lee
/// primero <c>HttpRequestException.StatusCode</c>, que existe en el BCL desde .NET 5 y que
/// ya rellena <c>HttpClient</c>. Implementa esta interfaz solo si envuelves los fallos de
/// tu cliente HTTP en un tipo propio. Es más limpio que la reflexión sobre
/// <c>err.status</c> / <c>err.statusCode</c> / <c>err.response.status</c> que hace el SDK
/// de Node por no tener tipos.
/// </remarks>
public interface IHttpStatusError
{
    /// <summary>El status HTTP con el que falló la dependencia.</summary>
    int HttpStatus { get; }
}

/// <summary>
/// Un error que sabe cuánto pide esperar la dependencia (la cabecera <c>Retry-After</c> de
/// una respuesta 429 o 503).
/// </summary>
public interface IRetryAfterError
{
    /// <summary>Retraso anunciado por la dependencia, o <see langword="null"/> si no lo anunció.</summary>
    TimeSpan? RetryAfter { get; }
}

/// <summary>
/// Implementación de conveniencia de <see cref="IHttpStatusError"/>, para no obligar a
/// cada aplicación a escribir la suya.
/// </summary>
/// <example>
/// <code>
/// if (!response.IsSuccessStatusCode)
///     throw new HttpStatusException((int)response.StatusCode, "POST /v1/charges");
/// </code>
/// </example>
public sealed class HttpStatusException : Exception, IHttpStatusError, IRetryAfterError
{
    /// <summary>Construye la excepción.</summary>
    /// <param name="httpStatus">Status devuelto por la dependencia.</param>
    /// <param name="message">Qué se estaba haciendo cuando falló.</param>
    /// <param name="retryAfter">Retraso anunciado por la dependencia, si lo anunció.</param>
    /// <param name="innerException">Error subyacente, si lo hay.</param>
    public HttpStatusException(
        int httpStatus,
        string? message = null,
        TimeSpan? retryAfter = null,
        Exception? innerException = null)
        : base(message is null ? $"HTTP {httpStatus}" : $"HTTP {httpStatus}: {message}", innerException)
    {
        HttpStatus = httpStatus;
        RetryAfter = retryAfter;
    }

    /// <inheritdoc />
    public int HttpStatus { get; }

    /// <inheritdoc />
    public TimeSpan? RetryAfter { get; }
}

/// <summary>
/// El resultado de clasificar un error: lo que el runtime del consumidor consume para
/// decidir entre <c>nak</c>, <c>term</c> y alerta.
/// </summary>
/// <param name="Class">Determina la acción sobre el mensaje.</param>
/// <param name="Code">Código estable para métricas y alertas.</param>
public sealed record Classification(ErrorClass Class, string Code)
{
    /// <summary>
    /// Solo aplica a <see cref="ErrorClass.Retryable"/>: <b>sugerencia para el PRIMER
    /// reintento</b>. <see langword="null"/> significa "usa el backoff canónico".
    /// </summary>
    /// <remarks>
    /// ⚠️ <b>NO sobrescribe el calendario de reintentos, aunque lo parezca.</b> Con
    /// <c>backoff</c> configurado —y flux lo configura siempre— JetStream aplica el delay
    /// del <c>nak</c> solo en la primera reentrega, y a partir de la segunda impone el array
    /// <c>backoff</c> sin devolver error ni avisar de nada. Medido contra NATS 2.14.5
    /// pidiendo <c>nak(300ms)</c> en todas las reentregas — 03-delivery.md §2.2:
    /// <code>
    /// SIN backoff:  0ms → 300ms → 600ms  → 900ms     ← el delay se honra siempre
    /// CON backoff:  0ms → 300ms → 5300ms → 15300ms   ← solo la primera vez
    /// </code>
    /// Es decir: un <c>Retry-After: 5</c> de un proveedor acorta el primer reintento y nada
    /// más; los siguientes seguirán el backoff canónico (1 m, 5 m, 15 m, 30 m). No
    /// construyas lógica que dependa de que se respete más allá de la primera vez.
    /// <para>
    /// Divergencia deliberada con Go, donde este campo es un <c>time.Duration</c> y el cero
    /// significa "sin especificar". En C# <c>TimeSpan?</c> distingue "no me han dicho
    /// cuánto esperar" de "reintenta ya mismo", y AMBOS son valores con sentido: un
    /// <c>nak</c> con retraso cero es una petición legítima. Go paga el colapso porque no
    /// tiene opcionales baratos; aquí no hay motivo para pagarlo.
    /// </para>
    /// </remarks>
    public TimeSpan? RetryAfter { get; init; }

    /// <summary>
    /// Solo aplica a <see cref="ErrorClass.Retryable"/>: entregas máximas para ESTE error,
    /// por debajo del <c>max_deliver</c> del consumidor. Cero significa "sin tope propio",
    /// es decir, manda el del consumidor.
    /// </summary>
    /// <remarks>
    /// Existe porque <c>max_deliver</c> es por consumidor, no por mensaje: bajarlo a 2 para
    /// acotar los errores desconocidos recortaría también los reintentos de los que sí
    /// sabemos transitorios (ECONNRESET, HTTP 503), que deben conservar sus 6 intentos
    /// — 04-errors.md §2.1.
    /// <para>
    /// Es <see langword="int"/> y no <c>int?</c> —al revés que <see cref="RetryAfter"/>— y
    /// la asimetría es deliberada: un presupuesto de 0 entregas NO EXISTE, porque todo
    /// mensaje se entrega al menos una vez. El mínimo con sentido es 1, así que el cero
    /// queda libre para significar "sin tope propio" sin colapsar ningún valor legítimo.
    /// Mismo convenio que en Go.
    /// </para>
    /// </remarks>
    public int MaxAttempts { get; init; }
}
