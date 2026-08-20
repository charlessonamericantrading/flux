// Propagación de contexto entre eventos.
// Contrato normativo: specification/01-envelope.md §5
//
// .NET tiene AsyncLocal<T>, que es el equivalente exacto del AsyncLocalStorage de Node:
// un valor ligado al flujo de ejecución asíncrono, que fluye a las continuaciones de un
// await y a las tareas hijas sin que nadie lo pase por parámetro.
//
// Eso significa que este SDK puede reproducir la semántica del SDK de referencia TAL
// CUAL, sin la divergencia que Go se vio obligado a introducir: allí no hay
// almacenamiento ligado a la goroutine, así que el contexto viaja explícito en el
// context.Context y una llamada a Publish con el ctx equivocado rompe la cadena de
// correlación en silencio. Aquí un publish() en cualquier punto de la pila de llamadas
// de un handler hereda el contexto del evento entrante sin que nadie pase nada.

using System.Diagnostics;

namespace Flux;

/// <summary>Lo que un evento entrante lega a los eventos que provoque.</summary>
public sealed record EventContext
{
    /// <summary>
    /// Se propaga SIN MODIFICAR por toda la cadena. Es la respuesta a "¿de qué flujo de
    /// negocio forma parte esto?".
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// El <c>id</c> del evento en curso, que pasa a ser el <c>causationid</c> de lo que se
    /// publique desde este handler: "¿quién causó esto exactamente?".
    /// </summary>
    public string? CausationId { get; init; }

    /// <summary>
    /// Tenant del evento en curso. Gana sobre el default de la conexión: un evento derivado
    /// pertenece al tenant del evento que lo causó, no al del servicio.
    /// </summary>
    public string? TenantId { get; init; }

    /// <summary>W3C Trace Context heredado del evento entrante.</summary>
    public string? TraceParent { get; init; }

    /// <summary>W3C Trace Context heredado del evento entrante.</summary>
    public string? TraceState { get; init; }
}

/// <summary>
/// El contexto del evento en curso, ligado al flujo de ejecución asíncrono.
/// </summary>
/// <remarks>
/// <see cref="AsyncLocal{T}"/> fluye a través de <c>await</c>, de <c>Task.Run</c> y de
/// cualquier tarea hija: un <c>PublishAsync</c> en el fondo de la pila de llamadas de un
/// handler hereda el contexto sin que ninguna firma lo mencione. Es la misma magia que
/// <c>AsyncLocalStorage</c> en Node, y el precio es el mismo: la propagación no es visible
/// en la firma, así que no se audita leyendo el código. A cambio no se puede romper por
/// olvido.
/// </remarks>
public static class FluxContext
{
    private static readonly AsyncLocal<EventContext?> Current = new();

    /// <summary>
    /// El contexto del evento que se está procesando, o <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// <see langword="null"/> es el caso normal de un publish desde una ruta HTTP o un
    /// cron: el evento nace de cero y <c>PublishAsync</c> inicializará su
    /// <c>correlationid</c> con su propio <c>id</c>.
    /// </remarks>
    public static EventContext? CurrentContext => Current.Value;

    /// <summary>
    /// Instala un contexto de evento hasta que se libere el valor devuelto.
    /// </summary>
    /// <remarks>
    /// Lo llama el SDK antes de invocar al handler. Una aplicación solo lo necesita para
    /// tests o para reanudar una cadena de correlación desde un trabajo diferido (por
    /// ejemplo, un <c>BackgroundService</c> que recoge trabajo de una tabla).
    /// </remarks>
    /// <example>
    /// <code>
    /// using (FluxContext.Push(FluxContext.FromEvent(evento)))
    /// {
    ///     await bus.PublishAsync("facturacion.factura.v1.emitida", payload);
    /// }
    /// </code>
    /// </example>
    public static IDisposable Push(EventContext context)
    {
        var previous = Current.Value;
        Current.Value = context;
        return new Scope(previous);
    }

    /// <summary>
    /// Deriva el contexto que un evento lega a sus descendientes.
    /// </summary>
    /// <remarks>
    /// Nótese que <see cref="EventContext.CausationId"/> toma el <c>id</c> del evento, no su
    /// <c>causationid</c>: la causa de lo que se publique ahora es ESTE evento, no el que
    /// lo causó a él — 01-envelope.md §3.2.
    /// </remarks>
    public static EventContext FromEvent(FluxEvent evento)
    {
        ArgumentNullException.ThrowIfNull(evento);

        return new EventContext
        {
            CorrelationId = evento.CorrelationId,
            CausationId = evento.Id,
            TenantId = evento.TenantId,
            TraceParent = evento.TraceParent,
            TraceState = evento.TraceState,
        };
    }

    /// <summary>
    /// El <c>traceparent</c> W3C del span activo, o <see langword="null"/> si no hay
    /// ninguno.
    /// </summary>
    /// <remarks>
    /// Tercera ventaja de .NET sobre los otros ports: el trace context vive en el BCL
    /// (<see cref="Activity"/>), que es lo que instrumentan OpenTelemetry, ASP.NET Core y
    /// <c>HttpClient</c>. No hace falta ni el <c>import()</c> dinámico de
    /// <c>@opentelemetry/api</c> que hace Node —y que falla en silencio si no está
    /// instalado— ni la inyección explícita que Go se vio obligado a pedir para no
    /// imponer una dependencia dura de OpenTelemetry a todo servicio.
    /// <para>
    /// Se comprueba el formato: <see cref="Activity.Id"/> solo es un <c>traceparent</c>
    /// válido cuando el <see cref="Activity.IdFormat"/> es
    /// <see cref="ActivityIdFormat.W3C"/>. Es el default desde .NET 5, pero una aplicación
    /// puede haberlo cambiado a jerárquico y entonces el id NO es un traceparent: emitirlo
    /// produciría un atributo sintácticamente inválido, que es peor que omitirlo.
    /// </para>
    /// </remarks>
    public static string? ActiveTraceparent()
    {
        var activity = Activity.Current;
        return activity is not null && activity.IdFormat == ActivityIdFormat.W3C ? activity.Id : null;
    }

    /// <summary>El <c>tracestate</c> W3C del span activo, o <see langword="null"/>.</summary>
    public static string? ActiveTracestate()
    {
        var activity = Activity.Current;
        return string.IsNullOrEmpty(activity?.TraceStateString) ? null : activity.TraceStateString;
    }

    private sealed class Scope : IDisposable
    {
        private readonly EventContext? _previous;
        private bool _disposed;

        public Scope(EventContext? previous) => _previous = previous;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Current.Value = _previous;
        }
    }
}
