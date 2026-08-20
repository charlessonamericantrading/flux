// Verificación de la configuración efectiva del consumidor.
// Contrato normativo: specification/03-delivery.md §2.1 — requisito L2.
//
// "El servidor devuelve lo que le pides" es falso. JetStream SOBRESCRIBE ack_wait con
// backoff[0], acepta la petición, no avisa, y devuelve una configuración distinta de la
// enviada. Comprobado contra nats-server 2.14.5 vía $JS.API.CONSUMER.DURABLE.CREATE.
//
// Este fichero NO conoce NATS a propósito: trabaja sobre un snapshot de tipos del BCL,
// así que la única defensa contra esa sobrescritura silenciosa se puede probar en un test
// unitario sin levantar un broker. En Go y Node esta comprobación habla directamente con
// los tipos del cliente y solo se ejerce con un NATS delante.

using System.Globalization;
using System.Text;

namespace Flux;

/// <summary>Un campo en el que el servidor no honró lo solicitado.</summary>
/// <param name="Field">Nombre del campo, tal y como lo llama NATS (<c>ack_wait</c>, …).</param>
/// <param name="Requested">Lo que pidió el SDK.</param>
/// <param name="Effective">Lo que devolvió el servidor.</param>
public sealed record ConfigDifference(string Field, string Requested, string Effective);

/// <summary>
/// El servidor aplicó una configuración de consumidor distinta de la solicitada.
/// </summary>
/// <remarks>
/// Requisito L2 — 03-delivery.md §2.1. Es la ÚNICA defensa contra la sobrescritura
/// silenciosa de <c>ack_wait</c> por <c>backoff[0]</c>. Sin esta comprobación, un handler
/// de más de un segundo se ejecuta en concurrencia consigo mismo bajo carga y nada lo
/// indica: ni un error, ni un log, ni una métrica.
/// </remarks>
public sealed class ConsumerConfigMismatchException : Exception
{
    /// <summary>Construye la excepción con las diferencias detectadas.</summary>
    public ConsumerConfigMismatchException(string durable, IReadOnlyList<ConfigDifference> differences)
        : base(BuildMessage(durable, differences))
    {
        Durable = durable;
        Differences = differences;
    }

    /// <summary>El durable cuyo consumidor no se creó como se pidió.</summary>
    public string Durable { get; }

    /// <summary>Los campos que difieren.</summary>
    public IReadOnlyList<ConfigDifference> Differences { get; }

    private static string BuildMessage(string durable, IReadOnlyList<ConfigDifference>? differences)
    {
        var sb = new StringBuilder()
            .Append(CultureInfo.InvariantCulture, $"el servidor devolvió una configuración distinta de la solicitada para \"{durable}\":")
            .AppendLine();

        foreach (var d in differences ?? Array.Empty<ConfigDifference>())
        {
            sb.Append(CultureInfo.InvariantCulture, $"  {d.Field}: solicitado {d.Requested}, efectivo {d.Effective}")
              .AppendLine();
        }

        sb.Append(
            "JetStream sobrescribe algunos campos en silencio (03-delivery.md §2.1). " +
            "Si el campo es ack_wait, comprueba que backoff[0] valga exactamente lo mismo: " +
            "backoff[0] ES el presupuesto de duración del handler.");

        return sb.ToString();
    }
}

/// <summary>
/// Los campos de configuración de consumidor que el SDK pide explícitamente.
/// </summary>
/// <remarks>
/// Solo estos: el resto lo rellena el servidor con sus defaults y compararlos daría falsos
/// positivos.
/// </remarks>
public sealed record ConsumerConfigSnapshot
{
    /// <summary>Plazo de confirmación. DEBE coincidir con <c>Backoff[0]</c>.</summary>
    public required TimeSpan AckWait { get; init; }

    /// <summary>Entregas máximas: 1 inicial + una por entrada de <see cref="Backoff"/>.</summary>
    public required int MaxDeliver { get; init; }

    /// <summary>Ventana de mensajes sin confirmar.</summary>
    public required int MaxAckPending { get; init; }

    /// <summary>
    /// Política de confirmación. Se compara como texto (<c>"explicit"</c>) a propósito: el
    /// enum de cada cliente de NATS numera los valores a su manera y el protocolo habla del
    /// literal del cable, no del ordinal.
    /// </summary>
    public required string AckPolicy { get; init; }

    /// <summary>Backoff por reintento.</summary>
    public required IReadOnlyList<TimeSpan> Backoff { get; init; }

    /// <summary>La configuración canónica del protocolo — 03-delivery.md §2.</summary>
    /// <param name="maxAckPending">Ventana de mensajes sin confirmar.</param>
    public static ConsumerConfigSnapshot Canonical(int maxAckPending = Protocol.DefaultMaxAckPending) =>
        new()
        {
            AckWait = Protocol.DefaultAckWait,
            MaxDeliver = Protocol.DefaultMaxDeliver,
            MaxAckPending = maxAckPending,
            AckPolicy = "explicit",
            Backoff = Protocol.CanonicalBackoff,
        };
}

/// <summary>Comprueba que el servidor aplicó la configuración solicitada.</summary>
public static class ConsumerConfigVerifier
{
    /// <summary>
    /// Lanza <see cref="ConsumerConfigMismatchException"/> si la config efectiva difiere de
    /// la solicitada, o si rompe la invariante <c>ack_wait == backoff[0]</c>.
    /// </summary>
    /// <param name="durable">Nombre del durable, para el mensaje de error.</param>
    /// <param name="requested">Lo que el SDK pidió.</param>
    /// <param name="effective">Lo que el servidor devolvió al leer el consumidor.</param>
    /// <exception cref="ConsumerConfigMismatchException">La config efectiva no es la pedida.</exception>
    public static void AssertHonored(
        string durable,
        ConsumerConfigSnapshot requested,
        ConsumerConfigSnapshot effective)
    {
        ArgumentNullException.ThrowIfNull(requested);
        ArgumentNullException.ThrowIfNull(effective);

        var diffs = new List<ConfigDifference>();

        if (effective.AckWait != requested.AckWait)
        {
            diffs.Add(new ConfigDifference("ack_wait", Fmt(requested.AckWait), Fmt(effective.AckWait)));
        }

        if (effective.MaxDeliver != requested.MaxDeliver)
        {
            diffs.Add(new ConfigDifference("max_deliver", Fmt(requested.MaxDeliver), Fmt(effective.MaxDeliver)));
        }

        if (effective.MaxAckPending != requested.MaxAckPending)
        {
            diffs.Add(new ConfigDifference(
                "max_ack_pending", Fmt(requested.MaxAckPending), Fmt(effective.MaxAckPending)));
        }

        // El protocolo exige ack explícito SIEMPRE: nunca auto-ack, ni AckAll, ni AckNone.
        if (!string.Equals(effective.AckPolicy, requested.AckPolicy, StringComparison.Ordinal))
        {
            diffs.Add(new ConfigDifference("ack_policy", requested.AckPolicy, effective.AckPolicy));
        }

        if (!effective.Backoff.SequenceEqual(requested.Backoff))
        {
            diffs.Add(new ConfigDifference("backoff", Fmt(requested.Backoff), Fmt(effective.Backoff)));
        }

        // Invariante del protocolo, no solo del SDK: aunque el servidor haya devuelto lo
        // mismo que se le pidió, ack_wait y backoff[0] tienen que coincidir. Si alguien
        // cambia el backoff canónico y olvida DefaultAckWait, esto lo caza aquí y no en
        // producción a las 3 de la mañana con reentregas concurrentes.
        if (effective.Backoff.Count > 0 && effective.AckWait != effective.Backoff[0])
        {
            diffs.Add(new ConfigDifference(
                "ack_wait == backoff[0]", Fmt(effective.Backoff[0]), Fmt(effective.AckWait)));
        }

        if (diffs.Count > 0)
        {
            throw new ConsumerConfigMismatchException(durable, diffs);
        }
    }

    private static string Fmt(TimeSpan value) =>
        value.TotalSeconds.ToString("0.###", CultureInfo.InvariantCulture) + "s";

    private static string Fmt(int value) => value.ToString(CultureInfo.InvariantCulture);

    private static string Fmt(IReadOnlyList<TimeSpan> values) =>
        "[" + string.Join(", ", values.Select(Fmt)) + "]";
}
