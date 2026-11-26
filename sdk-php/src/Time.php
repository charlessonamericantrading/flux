<?php

declare(strict_types=1);

namespace Flux;

/**
 * El `time` que da quien llama a un constructor — 01-envelope.md §2.2.
 *
 * Los constructores puros de cada SDK reciben `time` de fuera (el arnés de conformidad, un
 * test, quien construya un transporte propio). Copiarlo tal cual dejaría pasar
 * `2026-08-20T10:41:07Z` o `…+02:00`, que **no son los mismos bytes** que la forma canónica y
 * rompen el replay, la firma y la dedupe por hash; y un valor que ni siquiera es un instante
 * («ayer») saldría en el sobre. Por eso se **normaliza** a la forma canónica —exactamente 3
 * decimales y sufijo `Z`, en UTC— o se **rechaza**.
 *
 * Se admite lo que RFC 3339 llama `date-time`: `T` o `t` (o un espacio) entre la fecha y la
 * hora; `Z` o `z`, o un desfase `±hh:mm`; decimales opcionales y de cualquier longitud. Los
 * decimales de más de tres se **truncan** (no se redondean). No se admiten: un segundo 60 (un
 * segundo intercalar no existe en UTC de POSIX), un instante cuyo año en UTC cae fuera de
 * 0000–9999, ni un desfase de 24 horas o más.
 *
 * La aritmética es de enteros (días desde la época por el algoritmo de Hinnant), no de
 * `DateTime`: así los años 0000–0099 no se reinterpretan y el resultado es el mismo que el de
 * Node y el de Rust byte a byte.
 */
final class Time
{
    private const PATTERN = '/^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(?:[Zz]|([+-])(\d{2}):(\d{2}))\z/D';

    private const DAYS_IN_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    private function __construct()
    {
    }

    /** La forma canónica de `$value` (`2026-08-20T10:41:07.318Z`), o `null` si no es un instante RFC 3339 válido. */
    public static function canonical(mixed $value): ?string
    {
        if (!is_string($value) || preg_match(self::PATTERN, $value, $m) !== 1) {
            return null;
        }

        [$y, $mo, $d, $h, $mi, $s] = [(int) $m[1], (int) $m[2], (int) $m[3], (int) $m[4], (int) $m[5], (int) $m[6]];
        if ($mo < 1 || $mo > 12) {
            return null;
        }
        $dim = self::DAYS_IN_MONTH[$mo - 1] + ($mo === 2 && self::isLeap($y) ? 1 : 0);
        if ($d < 1 || $d > $dim || $h > 23 || $mi > 59 || $s > 59) {
            return null;
        }

        $ms = (int) substr(($m[7] ?? '') . '000', 0, 3);

        $offsetMin = 0;
        if (($m[8] ?? '') !== '') {
            $oh = (int) $m[9];
            $om = (int) $m[10];
            if ($oh > 23 || $om > 59) {
                return null;
            }
            $offsetMin = ($m[8] === '-' ? -1 : 1) * ($oh * 60 + $om);
        }

        $seconds = self::daysFromCivil($y, $mo, $d) * 86400 + $h * 3600 + $mi * 60 + $s - $offsetMin * 60;

        return self::format($seconds, $ms);
    }

    /** Milisegundos desde la época de un instante canónico o RFC 3339, o `null` si no es un instante. */
    public static function toEpochMs(mixed $value): ?int
    {
        $c = self::canonical($value);
        if ($c === null) {
            return null;
        }

        [$y, $mo, $d, $h, $mi, $s, $ms] = [
            (int) substr($c, 0, 4), (int) substr($c, 5, 2), (int) substr($c, 8, 2),
            (int) substr($c, 11, 2), (int) substr($c, 14, 2), (int) substr($c, 17, 2), (int) substr($c, 20, 3),
        ];

        return (self::daysFromCivil($y, $mo, $d) * 86400 + $h * 3600 + $mi * 60 + $s) * 1000 + $ms;
    }

    /** `2026-08-20T10:41:07.318Z` de unos milisegundos desde la época. */
    public static function fromEpochMs(int $ms): string
    {
        return self::format(intdiv($ms - (($ms % 1000 + 1000) % 1000), 1000), ($ms % 1000 + 1000) % 1000)
            ?? throw new \RangeException('el instante cae fuera de 0000–9999');
    }

    public static function nowMs(): int
    {
        return (int) floor(microtime(true) * 1000);
    }

    public static function now(): string
    {
        return self::fromEpochMs(self::nowMs());
    }

    private static function isLeap(int $y): bool
    {
        return ($y % 4 === 0 && $y % 100 !== 0) || $y % 400 === 0;
    }

    /** Días desde 1970-01-01 de una fecha del calendario gregoriano proléptico (Hinnant, `days_from_civil`). */
    private static function daysFromCivil(int $y, int $m, int $d): int
    {
        $y -= $m <= 2 ? 1 : 0;
        $era = intdiv($y >= 0 ? $y : $y - 399, 400);
        $yoe = $y - $era * 400;
        $doy = intdiv(153 * ($m + ($m > 2 ? -3 : 9)) + 2, 5) + $d - 1;
        $doe = $yoe * 365 + intdiv($yoe, 4) - intdiv($yoe, 100) + $doy;

        return $era * 146097 + $doe - 719468;
    }

    /** Lo contrario: `[año, mes, día]` de unos días desde la época (`civil_from_days`). */
    private static function civilFromDays(int $z): array
    {
        $z += 719468;
        $era = intdiv($z >= 0 ? $z : $z - 146096, 146097);
        $doe = $z - $era * 146097;
        $yoe = intdiv($doe - intdiv($doe, 1460) + intdiv($doe, 36524) - intdiv($doe, 146096), 365);
        $y = $yoe + $era * 400;
        $doy = $doe - (365 * $yoe + intdiv($yoe, 4) - intdiv($yoe, 100));
        $mp = intdiv(5 * $doy + 2, 153);
        $d = $doy - intdiv(153 * $mp + 2, 5) + 1;
        $m = $mp + ($mp < 10 ? 3 : -9);

        return [$y + ($m <= 2 ? 1 : 0), $m, $d];
    }

    private static function format(int $epochSeconds, int $ms): ?string
    {
        $days = intdiv($epochSeconds - (($epochSeconds % 86400 + 86400) % 86400), 86400);
        $rest = ($epochSeconds % 86400 + 86400) % 86400;
        [$y, $mo, $d] = self::civilFromDays($days);
        if ($y < 0 || $y > 9999) {
            return null; // fuera de 0000–9999: dejaría de ser RFC 3339
        }

        return sprintf(
            '%04d-%02d-%02dT%02d:%02d:%02d.%03dZ',
            $y, $mo, $d, intdiv($rest, 3600), intdiv($rest % 3600, 60), $rest % 60, $ms,
        );
    }
}
