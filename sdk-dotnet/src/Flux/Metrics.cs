// Métricas del SDK.
// Contrato normativo: specification/08-observability.md
//
// Los nombres y las etiquetas son parte del CONTRATO, no una decisión de cada SDK: si el de
// .NET y el de Go nombran distinto la tasa de DLQ, la de los servicios .NET no se puede
// sumar con la de los de Go y un panel del ecosistema es imposible. Es el mismo argumento
// que el de los códigos POISON de 01-envelope.md §3.1. Por eso esto vive aquí y no en la
// aplicación.

using System.Collections.ObjectModel;
using System.Globalization;
using System.Text;

namespace Flux;

/// <summary>Etiqueta <c>outcome</c> de <c>flux_events_published_total</c>.</summary>
public enum PublishOutcome
{
    /// <summary>El broker acusó la publicación.</summary>
    Ok,

    /// <summary>El payload no cumple su JSON Schema y el productor L3 rechazó publicarlo.</summary>
    InvalidSchema,

    /// <summary>El broker rechazó la publicación.</summary>
    Error,
}

/// <summary>Etiqueta <c>outcome</c> de <c>flux_events_consumed_total</c> — §2.1.</summary>
public enum ConsumeOutcome
{
    /// <summary>El handler terminó sin excepción.</summary>
    Ok,

    /// <summary>Agotó su presupuesto de reintentos y murió en la DLQ.</summary>
    Retryable,

    /// <summary>El consumidor lo rechazó de forma definitiva.</summary>
    Permanent,

    /// <summary>El mensaje no era interpretable.</summary>
    Poison,

    /// <summary>El payload no cumple su JSON Schema al consumir.</summary>
    InvalidSchema,

    /// <summary>
    /// Un fallo de firma: <c>MISSING_SIGNATURE</c>, <c>INVALID_SIGNATURE</c> o
    /// <c>UNKNOWN_SIGNING_KEY</c> — 07-signing.md §7.
    /// </summary>
    /// <remarks>
    /// Se separa del <see cref="Poison"/> común aunque el <c>dlqreason</c> del evento siga
    /// siendo <c>poison</c>: son dos incidentes distintos —basura frente a suplantación—
    /// con dos respuestas distintas. Un pico de firmas rotas apunta a un productor con la
    /// clave equivocada o a alguien reinyectando eventos; un pico de JSON corrupto, a un
    /// productor roto. Confundirlos hace que la alerta no diga qué hacer.
    /// </remarks>
    InvalidSignature,
}

/// <summary>Valores de <c>flux_connection_state</c> — §2.1.</summary>
public enum ConnectionState
{
    /// <summary>Sin conexión con el broker.</summary>
    Disconnected = 0,

    /// <summary>Conectado.</summary>
    Connected = 1,

    /// <summary>Reconectando.</summary>
    Reconnecting = 2,
}

/// <summary>
/// Traduce el resultado de una clasificación a los valores de etiqueta del protocolo.
/// </summary>
/// <remarks>
/// Vive fuera de <see cref="FluxBus"/> —igual que <see cref="TenantFilterPolicy"/> y
/// <see cref="ConsumerConfigVerifier"/>— porque es contrato del protocolo y no tiene nada
/// que ver con NATS: así se puede probar sin broker. En Java el equivalente es un método
/// package-private del propio bus; C# no tiene ese nivel de visibilidad, y ampliar la API
/// pública es preferible a no poder testearlo.
/// </remarks>
public static class MetricLabels
{
    /// <summary>
    /// El valor de la etiqueta <c>outcome</c> de <c>flux_events_consumed_total</c> para un
    /// evento que muere.
    /// </summary>
    /// <remarks>
    /// <b>La firma inválida se separa del POISON común</b> aunque su <c>reason</c> siga
    /// siendo <c>poison</c>: son dos incidentes distintos —basura frente a suplantación—
    /// con dos respuestas distintas, y 08-observability.md §2.1 declara la etiqueta justo
    /// para eso. Un pico de firmas rotas no debe confundirse con un pico de JSON corrupto.
    /// <para>
    /// El <c>reason</c> de la DLQ NO cambia, porque ése sí es el enum cerrado de
    /// 04-errors.md §1: meter <c>invalid_signature</c> en el atributo <c>dlqreason</c> de un
    /// evento produciría un envelope que ningún otro SDK sabe leer.
    /// </para>
    /// <para>
    /// Es el mismo criterio que aplican los SDKs de Go, Rust y PHP, y por tanto el valor que
    /// un panel del ecosistema espera encontrar.
    /// </para>
    /// </remarks>
    /// <param name="reason">Clase del fallo que mató al evento.</param>
    /// <param name="code">Código estable de la clasificación.</param>
    /// <returns>El valor de la etiqueta <c>outcome</c>.</returns>
    public static ConsumeOutcome ConsumeOutcomeFor(DlqReason reason, string? code)
    {
        if (IsSignatureCode(code))
        {
            return ConsumeOutcome.InvalidSignature;
        }

        return reason switch
        {
            DlqReason.Retryable => ConsumeOutcome.Retryable,
            DlqReason.Permanent => ConsumeOutcome.Permanent,
            _ => ConsumeOutcome.Poison,
        };
    }

    /// <summary>Los tres códigos POISON de la extensión de firma — 07-signing.md §7.</summary>
    /// <param name="code">Código estable de la clasificación.</param>
    /// <returns><see langword="true"/> si el fallo es de firma.</returns>
    public static bool IsSignatureCode(string? code) =>
        string.Equals(code, EventSigning.MissingSignature, StringComparison.Ordinal) ||
        string.Equals(code, EventSigning.InvalidSignature, StringComparison.Ordinal) ||
        string.Equals(code, EventSigning.UnknownSigningKey, StringComparison.Ordinal);
}

/// <summary>
/// Dónde van las métricas del SDK. Impleméntalo para enchufar
/// <c>System.Diagnostics.Metrics</c>, prometheus-net o lo que uses; el default es
/// <see cref="NoMetrics.Instance"/>.
/// </summary>
/// <remarks>
/// <b>Las firmas fuerzan las etiquetas del protocolo.</b> No hay un
/// <c>IDictionary&lt;string,string&gt; labels</c> genérico a propósito: es justo por ahí por
/// donde se cuela un <c>tenantid</c> que multiplica las series temporales. Un tenant nuevo
/// NO debe crear series nuevas — para eso están las trazas, donde el tenant sí se etiqueta
/// (08-observability.md §2.2 y §5).
/// <para>
/// La cardinalidad no avisa: el sistema funciona en desarrollo con tres tenants y muere en
/// producción con diez mil. Y el fallo se manifiesta como "Prometheus se ha quedado sin
/// memoria", no como "alguien etiquetó por tenant".
/// </para>
/// </remarks>
public interface IMetricsSink
{
    /// <summary><c>flux_events_published_total{subject,outcome}</c>.</summary>
    /// <param name="subject">Subject de NATS. Es ACOTADO —hay tantos como eventos declarados— así que sirve como etiqueta.</param>
    /// <param name="outcome">Resultado de la publicación.</param>
    void EventPublished(string subject, PublishOutcome outcome);

    /// <summary><c>flux_events_consumed_total{subject,consumer,outcome}</c>.</summary>
    /// <param name="subject">Subject de NATS.</param>
    /// <param name="consumer">Nombre del durable consumer.</param>
    /// <param name="outcome">Cómo terminó el evento.</param>
    void EventConsumed(string subject, string consumer, ConsumeOutcome outcome);

    /// <summary><c>flux_event_handler_duration_seconds{subject,consumer}</c>.</summary>
    /// <param name="subject">Subject de NATS.</param>
    /// <param name="consumer">Nombre del durable consumer.</param>
    /// <param name="seconds">Duración del handler, en segundos.</param>
    void HandlerDuration(string subject, string consumer, double seconds);

    /// <summary><c>flux_events_dlq_total{subject,consumer,reason,code}</c>.</summary>
    /// <param name="subject">Subject de NATS.</param>
    /// <param name="consumer">Nombre del durable consumer.</param>
    /// <param name="reason">Clase del fallo que lo mató.</param>
    /// <param name="code">
    /// El código ESTABLE de la clasificación (<c>"HTTP_503"</c>,
    /// <c>"PEDIDO_YA_CANCELADO"</c>), nunca el mensaje del error: un mensaje lleva ids,
    /// timestamps y rutas, su cardinalidad es infinita y tumba el almacenamiento de
    /// métricas — §2.2.
    /// </param>
    void EventDlq(string subject, string consumer, DlqReason reason, string code);

    /// <summary><c>flux_events_retried_total{subject,consumer,attempt}</c>.</summary>
    /// <param name="subject">Subject de NATS.</param>
    /// <param name="consumer">Nombre del durable consumer.</param>
    /// <param name="attempt">Número de entrega, de 1 a <c>max_deliver</c>.</param>
    void EventRetried(string subject, string consumer, int attempt);

    /// <summary><c>flux_consumer_pending{subject,consumer}</c>.</summary>
    /// <remarks>
    /// La alimenta el despacho en cada entrega, con el <c>NumPending</c> que ya viene en los
    /// metadatos del mensaje de JetStream: no hace falta sondear al servidor.
    /// <para>
    /// Es la <b>única</b> señal que delata a un consumidor cuyo bucle murió. La conexión
    /// sigue reportándose sana y el healthcheck dice que todo va bien; solo el crecimiento
    /// de pending lo evidencia — es el bug que apareció de verdad en el SDK de Node, y de
    /// ahí la cuarta alerta de §4.
    /// </para>
    /// <para>
    /// Limitación: si el consumidor deja de recibir mensajes <b>del todo</b>, tampoco se
    /// actualiza este gauge — se queda en su último valor. Para eso sirve la alerta de §4
    /// sobre el valor sostenido, no sobre su derivada.
    /// </para>
    /// </remarks>
    /// <param name="subject">Subject de NATS.</param>
    /// <param name="consumer">Nombre del durable consumer.</param>
    /// <param name="pending">Mensajes pendientes de entregar.</param>
    void ConsumerPending(string subject, string consumer, long pending);

    /// <summary><c>flux_connection_state</c>. Sin etiquetas — §2.</summary>
    /// <param name="state">Estado de la conexión.</param>
    void ConnectionState(ConnectionState state);
}

/// <summary>
/// No-op. Es el DEFAULT: un SDK de protocolo no debe imponer un backend de métricas a quien
/// solo quiere publicar un evento.
/// </summary>
public sealed class NoMetrics : IMetricsSink
{
    /// <summary>La única instancia: no tiene estado.</summary>
    public static readonly NoMetrics Instance = new();

    private NoMetrics()
    {
    }

    /// <inheritdoc />
    public void EventPublished(string subject, PublishOutcome outcome)
    {
    }

    /// <inheritdoc />
    public void EventConsumed(string subject, string consumer, ConsumeOutcome outcome)
    {
    }

    /// <inheritdoc />
    public void HandlerDuration(string subject, string consumer, double seconds)
    {
    }

    /// <inheritdoc />
    public void EventDlq(string subject, string consumer, DlqReason reason, string code)
    {
    }

    /// <inheritdoc />
    public void EventRetried(string subject, string consumer, int attempt)
    {
    }

    /// <inheritdoc />
    public void ConsumerPending(string subject, string consumer, long pending)
    {
    }

    /// <inheritdoc />
    public void ConnectionState(ConnectionState state)
    {
    }
}

/// <summary>
/// Recolector en memoria, seguro entre hilos, que renderiza el formato de exposición de
/// texto de Prometheus.
/// </summary>
/// <remarks>
/// Sin dependencias: ni prometheus-net ni OpenTelemetry. Es suficiente para servir un
/// <c>/metrics</c> real, y quien ya use otro backend implementa <see cref="IMetricsSink"/>
/// contra él — lo que importa es conservar nombres y etiquetas, que es lo que fija el
/// contrato.
/// </remarks>
public sealed class InMemoryMetrics : IMetricsSink
{
    /// <summary>
    /// Buckets obligatorios del histograma, en segundos — 08-observability.md §3.
    /// </summary>
    /// <remarks>
    /// El último es <c>30</c> a propósito: <b>es el <c>ack_wait</c></b>
    /// (<see cref="Protocol.DefaultAckWait"/>). Un handler que cae en el bucket superior
    /// está a punto de que su mensaje se reentregue mientras aún se ejecuta, así que
    /// <c>flux_event_handler_duration_seconds_bucket{le="30"}</c> frente al total mide
    /// directamente cuántos eventos rozan la ejecución concurrente.
    /// <para>
    /// Ese bucket DEBE moverse si se cambia <c>ack_wait</c>. Un bucket que no coincide con
    /// el plazo real mide algo que no le importa a nadie. Lo fija
    /// <c>MetricsTests.ElUltimoBucketEsElAckWait</c>.
    /// </para>
    /// </remarks>
    public static readonly ReadOnlyCollection<double> DurationBuckets = Array.AsReadOnly(
        new[] { 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0 });

    // SortedDictionary y no Dictionary: sin un orden estable, un scrape difiere del
    // siguiente sin que haya cambiado nada, y comparar dos volcados deja de servir.
    private readonly SortedDictionary<string, long> _counters = new(StringComparer.Ordinal);
    private readonly SortedDictionary<string, double> _gauges = new(StringComparer.Ordinal);
    private readonly SortedDictionary<string, Histogram> _histograms = new(StringComparer.Ordinal);
    private readonly object _lock = new();

    /// <summary>Un histograma acumulativo con los buckets de <see cref="DurationBuckets"/>.</summary>
    private sealed class Histogram
    {
        public long[] Buckets { get; } = new long[DurationBuckets.Count];

        public double Sum { get; set; }

        public long Count { get; set; }
    }

    /// <inheritdoc />
    public void EventPublished(string subject, PublishOutcome outcome) =>
        Increment("flux_events_published_total", ("outcome", Wire(outcome)), ("subject", subject));

    /// <inheritdoc />
    public void EventConsumed(string subject, string consumer, ConsumeOutcome outcome) =>
        Increment(
            "flux_events_consumed_total",
            ("consumer", consumer),
            ("outcome", Wire(outcome)),
            ("subject", subject));

    /// <inheritdoc />
    public void HandlerDuration(string subject, string consumer, double seconds) =>
        Observe(
            "flux_event_handler_duration_seconds",
            seconds,
            ("consumer", consumer),
            ("subject", subject));

    /// <inheritdoc />
    public void EventDlq(string subject, string consumer, DlqReason reason, string code) =>
        Increment(
            "flux_events_dlq_total",
            ("code", code),
            ("consumer", consumer),
            ("reason", Wire(reason)),
            ("subject", subject));

    /// <inheritdoc />
    public void EventRetried(string subject, string consumer, int attempt) =>
        Increment(
            "flux_events_retried_total",
            ("attempt", attempt.ToString(CultureInfo.InvariantCulture)),
            ("consumer", consumer),
            ("subject", subject));

    /// <inheritdoc />
    public void ConsumerPending(string subject, string consumer, long pending) =>
        Set("flux_consumer_pending", pending, ("consumer", consumer), ("subject", subject));

    /// <inheritdoc />
    public void ConnectionState(ConnectionState state) =>
        Set("flux_connection_state", (int)state);

    /// <summary>
    /// El formato de exposición de texto de Prometheus. Sírvelo tal cual en
    /// <c>/metrics</c>.
    /// </summary>
    /// <remarks>
    /// El orden de las líneas —cada familia precedida de su <c># TYPE</c>— y el formato de
    /// los números son los mismos que los del SDK de Node, así que la salida de un servicio
    /// .NET y la de uno de Node son byte a byte comparables cuando alguien depura por qué
    /// dos servicios no cuadran.
    /// </remarks>
    /// <returns>El cuerpo completo de un scrape.</returns>
    public string Render()
    {
        var sb = new StringBuilder();

        lock (_lock)
        {
            sb.Append("# TYPE flux_events_published_total counter\n");
            sb.Append("# TYPE flux_events_consumed_total counter\n");
            sb.Append("# TYPE flux_events_dlq_total counter\n");
            sb.Append("# TYPE flux_events_retried_total counter\n");
            foreach (var (key, value) in _counters)
            {
                sb.Append(key).Append(' ').Append(value.ToString(CultureInfo.InvariantCulture)).Append('\n');
            }

            sb.Append("# TYPE flux_consumer_pending gauge\n");
            sb.Append("# TYPE flux_connection_state gauge\n");
            foreach (var (key, value) in _gauges)
            {
                // Un gauge sin etiquetas no debe dejar unas llaves vacías en la salida.
                sb.Append(key.Replace("{}", string.Empty, StringComparison.Ordinal))
                  .Append(' ').Append(Number(value)).Append('\n');
            }

            sb.Append("# TYPE flux_event_handler_duration_seconds histogram\n");
            foreach (var (key, histogram) in _histograms)
            {
                // IndexOf(char) es ordinal por definición: no admite —ni necesita— un
                // StringComparison, a diferencia de la sobrecarga que recibe una cadena.
                var brace = key.IndexOf('{');
                var name = key[..brace];
                var labels = key[(brace + 1)..^1];
                var separator = labels.Length == 0 ? string.Empty : ",";

                for (var i = 0; i < DurationBuckets.Count; i++)
                {
                    sb.Append(name).Append("_bucket{").Append(labels).Append(separator)
                      .Append("le=\"").Append(Number(DurationBuckets[i])).Append("\"} ")
                      .Append(histogram.Buckets[i].ToString(CultureInfo.InvariantCulture)).Append('\n');
                }

                // +Inf es obligatorio y su valor es el total: sin él, Prometheus no puede
                // calcular cuántas observaciones quedaron por encima del último bucket.
                sb.Append(name).Append("_bucket{").Append(labels).Append(separator)
                  .Append("le=\"+Inf\"} ")
                  .Append(histogram.Count.ToString(CultureInfo.InvariantCulture)).Append('\n');
                sb.Append(name).Append("_sum{").Append(labels).Append("} ")
                  .Append(Number(histogram.Sum)).Append('\n');
                sb.Append(name).Append("_count{").Append(labels).Append("} ")
                  .Append(histogram.Count.ToString(CultureInfo.InvariantCulture)).Append('\n');
            }
        }

        return sb.ToString();
    }

    /// <summary>Copia de los contadores por clave de serie. Para tests y depuración.</summary>
    /// <returns>Un diccionario nuevo; mutarlo no afecta al recolector.</returns>
    public IReadOnlyDictionary<string, long> Counters()
    {
        lock (_lock)
        {
            return new Dictionary<string, long>(_counters, StringComparer.Ordinal);
        }
    }

    /// <summary>Copia de los gauges por clave de serie. Para tests y depuración.</summary>
    /// <returns>Un diccionario nuevo; mutarlo no afecta al recolector.</returns>
    public IReadOnlyDictionary<string, double> Gauges()
    {
        lock (_lock)
        {
            return new Dictionary<string, double>(_gauges, StringComparer.Ordinal);
        }
    }

    // ─── Interno ─────────────────────────────────────────────────────────────

    private void Increment(string name, params (string Name, string Value)[] labels)
    {
        var key = Key(name, labels);
        lock (_lock)
        {
            _counters[key] = _counters.TryGetValue(key, out var current) ? current + 1 : 1;
        }
    }

    private void Set(string name, double value, params (string Name, string Value)[] labels)
    {
        var key = Key(name, labels);
        lock (_lock)
        {
            _gauges[key] = value;
        }
    }

    private void Observe(string name, double value, params (string Name, string Value)[] labels)
    {
        var key = Key(name, labels);
        lock (_lock)
        {
            if (!_histograms.TryGetValue(key, out var histogram))
            {
                histogram = new Histogram();
                _histograms[key] = histogram;
            }

            histogram.Sum += value;
            histogram.Count++;
            for (var i = 0; i < DurationBuckets.Count; i++)
            {
                // Acumulativo: un valor cae en SU bucket y en todos los superiores, que es
                // lo que exige el formato de Prometheus (`le` = less or equal).
                if (value <= DurationBuckets[i])
                {
                    histogram.Buckets[i]++;
                }
            }
        }
    }

    /// <summary>
    /// <c>nombre{k="v",...}</c> con las etiquetas ORDENADAS por nombre.
    /// </summary>
    /// <remarks>
    /// Sin orden estable, la misma serie temporal aparecería con dos claves distintas según
    /// el orden en que se construyeran las etiquetas, y el contador se repartiría entre
    /// ambas sin que nada avisara. Los llamantes ya las pasan ordenadas; esto lo garantiza.
    /// </remarks>
    private static string Key(string name, (string Name, string Value)[] labels)
    {
        var sb = new StringBuilder(name).Append('{');
        var first = true;
        foreach (var (labelName, labelValue) in labels.OrderBy(l => l.Name, StringComparer.Ordinal))
        {
            if (!first)
            {
                sb.Append(',');
            }

            first = false;
            sb.Append(labelName).Append("=\"").Append(Escape(labelValue)).Append('"');
        }

        return sb.Append('}').ToString();
    }

    /// <summary>
    /// Neutraliza comillas, barras invertidas y saltos de línea en un valor de etiqueta.
    /// </summary>
    /// <remarks>
    /// No es cosmética: un <c>code</c> con una comilla rompe el formato de exposición y
    /// Prometheus descarta el <b>scrape entero</b>, no solo esa línea. Es decir, un mensaje
    /// de error mal formado de un servicio apagaría las métricas de todo el proceso.
    /// </remarks>
    private static string Escape(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return string.Empty;
        }

        var sb = new StringBuilder(value.Length);
        foreach (var c in value)
        {
            sb.Append(c is '"' or '\\' or '\n' ? '_' : c);
        }

        return sb.ToString();
    }

    /// <summary>
    /// Renderiza un <see langword="double"/> como lo haría JavaScript: sin ceros finales.
    /// </summary>
    /// <remarks>
    /// <c>le="30.0"</c> y <c>le="30"</c> son etiquetas DISTINTAS: al agregar el histograma
    /// de un servicio .NET con el de uno de Node saldrían dos series donde hay una. El
    /// <c>ToString</c> de .NET Core 3.0+ ya emite la representación más corta que
    /// round-trip, que coincide con la de JavaScript.
    /// </remarks>
    private static string Number(double value) => value.ToString(CultureInfo.InvariantCulture);

    private static string Wire(PublishOutcome outcome) => outcome switch
    {
        PublishOutcome.Ok => "ok",
        PublishOutcome.InvalidSchema => "invalid_schema",
        PublishOutcome.Error => "error",
        _ => "desconocido",
    };

    private static string Wire(ConsumeOutcome outcome) => outcome switch
    {
        ConsumeOutcome.Ok => "ok",
        ConsumeOutcome.Retryable => "retryable",
        ConsumeOutcome.Permanent => "permanent",
        ConsumeOutcome.Poison => "poison",
        ConsumeOutcome.InvalidSchema => "invalid_schema",
        ConsumeOutcome.InvalidSignature => "invalid_signature",
        _ => "desconocido",
    };

    private static string Wire(DlqReason reason) => reason switch
    {
        DlqReason.Retryable => "retryable",
        DlqReason.Permanent => "permanent",
        DlqReason.Poison => "poison",
        _ => "desconocido",
    };
}
