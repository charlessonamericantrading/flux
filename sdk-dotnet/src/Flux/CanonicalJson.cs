// La forma canónica de un mensaje — 01-envelope.md §4.1.
//
// Dos SDKs que reciben el MISMO `data` deben escribir LOS MISMOS bytes: el orden de claves y los bytes son normativos
// para el replay, la firma y la dedupe por hash. La forma canónica es la de `JSON.stringify` con UTF-8 literal:
//
//   · SIN espacios ni saltos de línea entre elementos.
//   · Las claves EN EL ORDEN en que se dan; una clave REPETIDA conserva la posición de la primera aparición y el valor
//     de la última (`{"a":1,"b":3,"a":2}` es `{"a":2,"b":3}`).
//   · Las cadenas, UTF-8 literal salvo lo que ordena §1.1: `ñ` es `ñ`, `\/` es `/`, `\u007f` es DEL, un par
//     sustituto escapado es el carácter; `"`, `\` y U+0000–U+001F siguen escapados (`\b \f \n \r \t`, y el resto como
//     `\u00XX` en minúscula, como `JSON.stringify`).
//   · Los números, como los escribe cada lenguaje, SIN tocarlos (§2.5): `4995` sale `4995`.
//
// Por qué no basta con el encoder de System.Text.Json: escapa los escalares fuera del BMP como pareja de escapes (un
// cohete U+1F680 sale como doce caracteres ASCII), escribe los hex en MAYÚSCULA, y no deduplica claves. Ninguna de las
// tres cosas es configurable. Aquí se recorre el documento y se escribe a mano.

using System.Text;
using System.Text.Json;

namespace Flux;

/// <summary>Escritura de JSON en forma canónica.</summary>
public static class CanonicalJson
{
    /// <summary>Reescribe unos bytes JSON a su forma canónica.</summary>
    /// <exception cref="JsonException">No es JSON válido.</exception>
    public static byte[] Canonicalize(ReadOnlySpan<byte> json)
    {
        using var documento = JsonDocument.Parse(json.ToArray());
        return Write(documento.RootElement);
    }

    /// <summary>La forma canónica de un valor JSON, en UTF-8.</summary>
    public static byte[] Write(JsonElement value)
    {
        var salida = new MemoryStream();
        WriteValue(salida, value);
        return salida.ToArray();
    }

    /// <summary>La forma canónica de un valor JSON, como texto.</summary>
    public static string WriteString(JsonElement value) => Encoding.UTF8.GetString(Write(value));

    private static void WriteValue(Stream o, JsonElement v)
    {
        switch (v.ValueKind)
        {
            case JsonValueKind.Object:
                WriteObject(o, v);
                break;
            case JsonValueKind.Array:
                o.WriteByte((byte)'[');
                var primero = true;
                foreach (var e in v.EnumerateArray())
                {
                    if (!primero)
                    {
                        o.WriteByte((byte)',');
                    }

                    primero = false;
                    WriteValue(o, e);
                }

                o.WriteByte((byte)']');
                break;
            case JsonValueKind.String:
                WriteString(o, v.GetString()!);
                break;
            case JsonValueKind.Number:
                // Sin tocarlos: `GetRawText` es el token tal cual llegó.
                Raw(o, v.GetRawText());
                break;
            case JsonValueKind.True:
                Raw(o, "true");
                break;
            case JsonValueKind.False:
                Raw(o, "false");
                break;
            default:
                Raw(o, "null");
                break;
        }
    }

    private static void WriteObject(Stream o, JsonElement v)
    {
        // Posición de la primera aparición, valor de la última.
        var nombres = new List<string>();
        var valores = new List<JsonElement>();
        var indice = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var p in v.EnumerateObject())
        {
            if (indice.TryGetValue(p.Name, out var i))
            {
                valores[i] = p.Value;
            }
            else
            {
                indice[p.Name] = nombres.Count;
                nombres.Add(p.Name);
                valores.Add(p.Value);
            }
        }

        o.WriteByte((byte)'{');
        for (var i = 0; i < nombres.Count; i++)
        {
            if (i > 0)
            {
                o.WriteByte((byte)',');
            }

            WriteString(o, nombres[i]);
            o.WriteByte((byte)':');
            WriteValue(o, valores[i]);
        }

        o.WriteByte((byte)'}');
    }

    private static void Raw(Stream o, string texto)
    {
        foreach (var c in texto)
        {
            o.WriteByte((byte)c);
        }
    }

    private static void WriteString(Stream o, string s)
    {
        o.WriteByte((byte)'"');
        Span<byte> buffer = stackalloc byte[4];
        for (var i = 0; i < s.Length; i++)
        {
            var c = s[i];
            switch (c)
            {
                case '"':
                    Raw(o, "\\\"");
                    continue;
                case '\\':
                    Raw(o, "\\\\");
                    continue;
                case '\b':
                    Raw(o, "\\b");
                    continue;
                case '\f':
                    Raw(o, "\\f");
                    continue;
                case '\n':
                    Raw(o, "\\n");
                    continue;
                case '\r':
                    Raw(o, "\\r");
                    continue;
                case '\t':
                    Raw(o, "\\t");
                    continue;
            }

            if (c < 0x20)
            {
                Raw(o, "\\u00" + ((int)c).ToString("x2", System.Globalization.CultureInfo.InvariantCulture));
                continue;
            }

            if (c < 0x80)
            {
                o.WriteByte((byte)c);
                continue;
            }

            if (char.IsHighSurrogate(c) && i + 1 < s.Length && char.IsLowSurrogate(s[i + 1]))
            {
                var rune = new Rune(c, s[i + 1]);
                var n = rune.EncodeToUtf8(buffer);
                o.Write(buffer[..n]);
                i++;
                continue;
            }

            if (char.IsSurrogate(c))
            {
                // Un sustituto suelto no es UTF-8: JavaScript lo escribe escapado y Rust lo rechaza (01-envelope.md §4.1,
                // «Qué NO se admite»). Aquí se escribe escapado, como Node.
                Raw(o, "\\u" + ((int)c).ToString("x4", System.Globalization.CultureInfo.InvariantCulture));
                continue;
            }

            var bytes = new Rune(c).EncodeToUtf8(buffer);
            o.Write(buffer[..bytes]);
        }

        o.WriteByte((byte)'"');
    }
}
