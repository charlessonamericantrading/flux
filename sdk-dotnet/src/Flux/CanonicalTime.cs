// El `time` que da quien llama a un constructor — 01-envelope.md §2.2.
//
// Los constructores puros de cada SDK reciben `time` de fuera (el arnés de conformidad, un test, quien construya un
// transporte propio). Copiarlo tal cual dejaría pasar `2026-08-20T10:41:07Z` o `…+02:00`, que NO son los mismos bytes
// que la forma canónica y rompen el replay, la firma y la dedupe por hash; y un valor que ni siquiera es un instante
// («ayer») saldría en el sobre. Por eso se NORMALIZA a la forma canónica —exactamente 3 decimales y sufijo `Z`, en
// UTC— o se RECHAZA.
//
// Se admite lo que RFC 3339 llama `date-time` —`T` o `t` (o un espacio) entre la fecha y la hora; `Z` o `z`, o un
// desfase `±hh:mm`; los decimales son opcionales y de cualquier longitud—. Los decimales de más de tres se TRUNCAN (no
// se redondean). No se admiten: un segundo 60, un instante cuyo año en UTC cae fuera de 0000–9999, ni un desfase de 24
// horas o más.
//
// El cálculo es aritmético (días desde la época, sin `DateTime`): `DateTime` no representa el año 0000, que es válido
// aquí, y un SDK no puede aceptar o rechazar lo que acepte su tipo de fecha.

using System.Globalization;
using System.Text.RegularExpressions;

namespace Flux;

/// <summary>La forma canónica de un instante del protocolo.</summary>
public static partial class CanonicalTime
{
    // [0-9] y no \d: en .NET \d casa con los dígitos de otros alfabetos. \z y no $: $ acepta un salto de línea final.
    [GeneratedRegex(@"^([0-9]{4})-([0-9]{2})-([0-9]{2})[Tt ]([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]+))?(?:[Zz]|([+-])([0-9]{2}):([0-9]{2}))\z", RegexOptions.CultureInvariant)]
    private static partial Regex Instante();

    private static readonly int[] Dias = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

    private static bool Bisiesto(int y) => (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;

    /// <summary>La forma canónica de <paramref name="texto"/> (<c>2026-08-20T10:41:07.318Z</c>), o <see langword="null"/> si no es un instante RFC 3339 válido.</summary>
    public static string? Normalize(string? texto)
    {
        if (texto is null)
        {
            return null;
        }

        var m = Instante().Match(texto);
        if (!m.Success)
        {
            return null;
        }

        int N(int g) => int.Parse(m.Groups[g].Value, CultureInfo.InvariantCulture);
        var y = N(1);
        var mo = N(2);
        var d = N(3);
        var h = N(4);
        var mi = N(5);
        var s = N(6);
        if (mo < 1 || mo > 12)
        {
            return null;
        }

        if (d < 1 || d > (mo == 2 && Bisiesto(y) ? 29 : Dias[mo - 1]))
        {
            return null;
        }

        if (h > 23 || mi > 59 || s > 59)
        {
            return null;
        }

        var fraccion = (m.Groups[7].Success ? m.Groups[7].Value : string.Empty) + "000";
        var ms = int.Parse(fraccion[..3], CultureInfo.InvariantCulture);

        long desfaseMin = 0;
        if (m.Groups[8].Success)
        {
            var oh = N(9);
            var om = N(10);
            if (oh > 23 || om > 59)
            {
                return null;
            }

            desfaseMin = (m.Groups[8].Value == "-" ? -1 : 1) * (oh * 60L + om);
        }

        // Segundos desde 0000-03-01 (algoritmo de Howard Hinnant, años proleptas del calendario gregoriano).
        var segundos = DiasDesdeCivil(y, mo, d) * 86_400L + h * 3_600L + mi * 60L + s - desfaseMin * 60L;
        var dias = FloorDiv(segundos, 86_400L);
        var resto = segundos - dias * 86_400L;
        var (anio, mes, dia) = CivilDesdeDias(dias);
        if (anio < 0 || anio > 9999)
        {
            return null;
        }

        return string.Create(
            CultureInfo.InvariantCulture,
            $"{anio:D4}-{mes:D2}-{dia:D2}T{resto / 3600:D2}:{resto % 3600 / 60:D2}:{resto % 60:D2}.{ms:D3}Z");
    }

    private static long FloorDiv(long a, long b) => a >= 0 ? a / b : -((-a + b - 1) / b);

    // Días desde 1970-01-01 de una fecha civil. Válido para cualquier año proléptico.
    private static long DiasDesdeCivil(int y, int m, int d)
    {
        long yy = m <= 2 ? y - 1 : y;
        var era = FloorDiv(yy, 400);
        var yoe = yy - era * 400;
        var doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
        var doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return era * 146_097 + doe - 719_468;
    }

    private static (int Anio, int Mes, int Dia) CivilDesdeDias(long z)
    {
        z += 719_468;
        var era = FloorDiv(z, 146_097);
        var doe = z - era * 146_097;
        var yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
        var y = yoe + era * 400;
        var doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        var mp = (5 * doy + 2) / 153;
        var d = (int)(doy - (153 * mp + 2) / 5 + 1);
        var m = (int)(mp < 10 ? mp + 3 : mp - 9);
        return ((int)(m <= 2 ? y + 1 : y), m, d);
    }
}
