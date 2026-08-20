// Las tres clases de error del protocolo.
// Contrato normativo: specification/04-errors.md §1
//
// El error más caro de un sistema de eventos no es perder un mensaje: es reintentar
// durante 51 minutos algo que nunca va a funcionar mientras los eventos sanos se
// acumulan detrás. Por eso flux no tiene "una política de reintentos": tiene una
// taxonomía de tres clases, y cada clase determina una acción distinta sobre el
// mensaje de NATS.

namespace Flux;

/// <summary>
/// Una de las tres clases del protocolo — 04-errors.md §1.
/// </summary>
/// <remarks>
/// El valor 0 es <see cref="Retryable"/> a propósito: es el default de la spec para la
/// política de timeouts, así que un <c>default(ErrorClass)</c> en
/// <see cref="ClassifierOptions"/> significa exactamente lo que dice 04-errors.md §2.1
/// y no un tercer estado sin sentido. Es el mismo convenio de "el cero es el default de
/// la spec" que usan <c>ClassifierOptions</c> en Go y los campos opcionales de
/// <see cref="Classification"/>.
/// </remarks>
public enum ErrorClass
{
    /// <summary>
    /// El fallo es del entorno y podría desaparecer solo.
    /// → <c>nak(delay)</c> y reintento con el backoff canónico.
    /// </summary>
    Retryable = 0,

    /// <summary>
    /// El evento es válido pero este consumidor nunca podrá procesarlo por mucho que
    /// espere. → <c>term()</c> + DLQ inmediato, sin reintentos.
    /// </summary>
    Permanent = 1,

    /// <summary>
    /// El mensaje ni siquiera es interpretable. → <c>term()</c> + DLQ + alerta.
    /// Lo detecta el SDK antes del handler; casi siempre significa que un productor está
    /// roto. Es el único caso que DEBE despertar a alguien.
    /// </summary>
    Poison = 2,
}

/// <summary>Conversión entre <see cref="ErrorClass"/> y los literales del protocolo.</summary>
public static class ErrorClassExtensions
{
    /// <summary>
    /// El literal que viaja en <c>dlqreason</c>. Siempre en minúsculas: es el valor de la
    /// spec, no el nombre del símbolo de C#.
    /// </summary>
    public static string ToWire(this ErrorClass value) => value switch
    {
        ErrorClass.Retryable => "retryable",
        ErrorClass.Permanent => "permanent",
        ErrorClass.Poison => "poison",
        _ => throw new ArgumentOutOfRangeException(nameof(value), value, "clase de error desconocida"),
    };

    /// <summary>
    /// Traduce la clase del error a la razón que se escribe en la DLQ.
    /// </summary>
    /// <remarks>
    /// Un RETRYABLE solo llega a la DLQ cuando ha agotado su presupuesto de entregas, y
    /// entonces <c>dlqreason</c> vale <c>retryable</c>: es información útil para el
    /// forense —"esto se reintentó y siguió fallando"— y no una contradicción
    /// — 04-errors.md §3.
    /// </remarks>
    public static DlqReason ToDlqReason(this ErrorClass value) => value switch
    {
        ErrorClass.Retryable => DlqReason.Retryable,
        ErrorClass.Permanent => DlqReason.Permanent,
        ErrorClass.Poison => DlqReason.Poison,
        _ => throw new ArgumentOutOfRangeException(nameof(value), value, "clase de error desconocida"),
    };
}
