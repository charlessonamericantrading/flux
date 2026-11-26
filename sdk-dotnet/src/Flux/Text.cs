// Cómo se miden y se cortan los textos del protocolo — la misma regla en todos los SDKs.
// Contrato normativo: specification/02-naming.md y 04-errors.md §3.
//
// .NET cuenta UNIDADES UTF-16 (`string.Length`): un emoji son dos, y mil emojis «miden» 2000. JSON Schema
// (`maxLength`) cuenta CARACTERES UNICODE (puntos de código). Para que dos SDKs acepten y rechacen lo mismo, todo
// límite de longitud del protocolo se cuenta EN PUNTOS DE CÓDIGO (`message` de un error de rpc, `code`…), y lo único
// que se acota en BYTES es `dlqerror` (1024 bytes UTF-8, sin partir un carácter): es un texto que acaba en un
// almacén con un tope de tamaño, no un campo con esquema.

namespace Flux;

/// <summary>Medida y corte de textos según el protocolo.</summary>
public static class Text
{
    /// <summary>Longitud en puntos de código Unicode: lo que cuenta <c>maxLength</c> de JSON Schema. Un sustituto suelto cuenta como uno.</summary>
    public static int CodePointLength(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var n = 0;
        for (var i = 0; i < value.Length; i++)
        {
            if (char.IsHighSurrogate(value[i]) && i + 1 < value.Length && char.IsLowSurrogate(value[i + 1]))
            {
                i++;
            }

            n++;
        }

        return n;
    }

    /// <summary>
    /// Corta a <paramref name="max"/> puntos de código como mucho: si no cabe, <c>max − 1</c> más <c>…</c>. Nunca parte
    /// un par sustituto (una mitad se serializaría como un escape de sustituto suelto, que no es UTF-8 válido).
    /// </summary>
    public static string TruncateCodePoints(string value, int max)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value.Length <= max || CodePointLength(value) <= max)
        {
            return value;
        }

        var i = 0;
        var cuenta = 0;
        while (i < value.Length && cuenta < max - 1)
        {
            i += char.IsHighSurrogate(value[i]) && i + 1 < value.Length && char.IsLowSurrogate(value[i + 1]) ? 2 : 1;
            cuenta++;
        }

        return value[..i] + "…";
    }

    /// <summary>
    /// Corta a <paramref name="maxBytes"/> bytes UTF-8 como mucho <b>sin partir un carácter</b>: el corte cae en la frontera
    /// del último carácter entero que cabe. Es la regla de <c>dlqerror</c> (1024 bytes) — 04-errors.md §3.
    /// </summary>
    public static string TruncateUtf8Bytes(string value, int maxBytes)
    {
        ArgumentNullException.ThrowIfNull(value);
        var bytes = 0;
        var i = 0;
        while (i < value.Length)
        {
            var largo = char.IsHighSurrogate(value[i]) && i + 1 < value.Length && char.IsLowSurrogate(value[i + 1]) ? 2 : 1;
            // Un sustituto suelto se codifica como U+FFFD (3 bytes): es lo que escribiría UTF8.GetBytes.
            var b = largo == 2 ? 4 : value[i] < 0x80 ? 1 : value[i] < 0x800 ? 2 : 3;
            if (bytes + b > maxBytes)
            {
                break;
            }

            bytes += b;
            i += largo;
        }

        return value[..i];
    }
}
