// El `id` DETERMINISTA de un hecho importado de un sistema externo.
// Contrato normativo: specification/01-envelope.md §2.6.1.
//
// Un proveedor (una pasarela de pagos, un sistema de terceros) reenvía el mismo aviso durante horas. Si cada reenvío
// recibe un `id` nuevo, ningún consumidor puede deduplicarlo: el `id` es la clave de deduplicación del ecosistema. Para
// un hecho importado, el `id` se DERIVA de lo que identifica al hecho, y es siempre el mismo:
//
//   id = UUIDv7 cuyos 48 bits de tiempo son los milisegundos del `time` del hecho externo y cuyos 74 bits restantes
//        (`rand_a` y `rand_b`) son los 74 primeros bits de SHA-256( <source> "|" <clave externa> ).
//
// Es un UUIDv7 VÁLIDO (versión 7, variante 10): cualquier consumidor que valide el formato del `id` lo acepta, y
// ordenar por `id` dentro de un `source` sigue siendo ordenar por el instante del hecho. Es una función PURA: no usa el
// reloj ni ninguna fuente de azar, que es lo que el arnés cruzado compara byte a byte entre SDKs.

using System.Numerics;
using System.Security.Cryptography;
using System.Text;

namespace Flux;

/// <summary>El <c>id</c> determinista de un hecho importado.</summary>
public static class ExternalId
{
    /// <summary>2^48 − 1: lo que cabe en el campo de tiempo de un UUIDv7.</summary>
    public const long Uuidv7MaxMs = (1L << 48) - 1;

    /// <summary>Longitud máxima de una clave externa, en bytes UTF-8: es un identificador, no un documento.</summary>
    public const int ExternalKeyMaxBytes = 512;

    /// <summary>
    /// <c>SHA-256(&lt;source&gt; "|" &lt;externalKey&gt;)</c>, y de ahí el UUIDv7.
    /// </summary>
    /// <param name="source">El <c>source</c> del que publica: <c>/&lt;entorno&gt;/&lt;servicio&gt;</c>. No lleva <c>|</c>.</param>
    /// <param name="externalKey">La clave del hecho en el sistema externo. No vacía, como máximo 512 bytes UTF-8.</param>
    /// <param name="time">El instante del hecho externo.</param>
    /// <exception cref="EnvelopeException">Clave vacía o demasiado larga, <c>source</c> con <c>|</c> o un instante que no cabe.</exception>
    public static string Of(string source, string externalKey, DateTimeOffset time)
    {
        var ms = time.ToUnixTimeMilliseconds();
        return Of(source, externalKey, ms);
    }

    /// <summary>Como <see cref="Of(string, string, DateTimeOffset)"/>, con el instante en milisegundos desde la época.</summary>
    public static string Of(string source, string externalKey, long ms)
    {
        if (string.IsNullOrEmpty(source) || source.Contains('|', StringComparison.Ordinal))
        {
            throw new EnvelopeException(
                "`source` debe ser una cadena no vacía y sin `|` (es el separador de la derivación del id determinista)");
        }

        if (string.IsNullOrEmpty(externalKey))
        {
            throw new EnvelopeException(
                "`ExternalKey` debe ser una cadena no vacía: es la clave del hecho en el sistema externo (el id del evento " +
                "del proveedor). Sin ella no hay nada de lo que derivar un `id` estable (01-envelope.md §2.6.1)");
        }

        if (Encoding.UTF8.GetByteCount(externalKey) > ExternalKeyMaxBytes)
        {
            throw new EnvelopeException(
                $"`ExternalKey` ocupa más de {ExternalKeyMaxBytes} bytes UTF-8: es un identificador, no un documento. " +
                "Usa el id del hecho en el sistema externo");
        }

        if (ms < 0 || ms > Uuidv7MaxMs)
        {
            throw new EnvelopeException(
                $"`time` no es un instante válido para el id determinista (se necesitan milisegundos enteros entre 0 y {Uuidv7MaxMs}). " +
                "Pasa el instante del hecho externo (01-envelope.md §2.6.1)");
        }

        var digest = SHA256.HashData(Encoding.UTF8.GetBytes(source + "|" + externalKey));
        // Los 74 primeros bits del digest: 80 bits (10 bytes) menos los 6 últimos.
        var r = new BigInteger(digest.AsSpan(0, 10), isUnsigned: true, isBigEndian: true) >> 6;
        var randA = r >> 62; // bits 0..11
        var randB = r & ((BigInteger.One << 62) - 1); // bits 12..73
        var uuid = (new BigInteger(ms) << 80) | (new BigInteger(7) << 76) | (randA << 64) | (new BigInteger(2) << 62) | randB;
        var hex = uuid.ToString("x", System.Globalization.CultureInfo.InvariantCulture).PadLeft(32, '0');
        // ToString("x") de un BigInteger positivo puede anteponer un 0 de signo: se queda con los 32 últimos.
        hex = hex[^32..];
        return $"{hex[..8]}-{hex[8..12]}-{hex[12..16]}-{hex[16..20]}-{hex[20..]}";
    }
}
