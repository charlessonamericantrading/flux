<?php

declare(strict_types=1);

namespace Flux;

/**
 * Cómo se miden y se cortan los textos del protocolo — la misma regla en todos los SDKs.
 *
 * PHP cuenta **bytes** (`strlen`), JSON Schema (`maxLength`) cuenta **caracteres Unicode**
 * (puntos de código) y JavaScript, unidades UTF-16. Para que dos SDKs acepten y rechacen lo
 * mismo, todo límite de longitud del protocolo se cuenta **en puntos de código**
 * (`message` de un error de rpc, `code`…), y lo único que se acota en **bytes** es `dlqerror`
 * (1024 bytes UTF-8, sin partir un carácter): es un texto que acaba en un almacén con un tope
 * de tamaño, no un campo con esquema. (02-naming.md y 04-errors.md §3; vectores `ajuste-*` de
 * conformance/harness/vectors.json.)
 */
final class Text
{
    private function __construct()
    {
    }

    /** Longitud en puntos de código Unicode: lo que cuenta `maxLength` de JSON Schema. */
    public static function codePointLength(string $s): int
    {
        if (function_exists('mb_strlen')) {
            return mb_strlen($s, 'UTF-8');
        }

        // Sin ext-mbstring (que es solo `suggest`): `/./su` cuenta un punto de código por coincidencia.
        $n = preg_match_all('/./su', $s);

        return $n === false ? strlen($s) : $n;
    }

    /**
     * Corta a `$max` puntos de código como mucho: si no cabe, `$max − 1` más `…`. Nunca parte un
     * carácter multibyte.
     */
    public static function truncateCodePoints(string $s, int $max): string
    {
        if (self::codePointLength($s) <= $max) {
            return $s;
        }

        $head = function_exists('mb_substr')
            ? mb_substr($s, 0, $max - 1, 'UTF-8')
            : (preg_match('/^.{0,' . ($max - 1) . '}/su', $s, $m) === 1 ? $m[0] : substr($s, 0, $max - 1));

        return $head . '…';
    }

    /**
     * Corta a `$maxBytes` bytes UTF-8 como mucho **sin partir un carácter**: el corte cae en la
     * frontera del último carácter entero que cabe. Es la regla de `dlqerror` (1024 bytes).
     *
     * Un texto que no es UTF-8 válido se sanea antes (cada byte inválido pasa a U+FFFD): un
     * `json_encode` posterior fallaría con él, y el evento de DLQ no llegaría a escribirse
     * justo cuando más falta hace.
     */
    public static function truncateUtf8Bytes(string $s, int $maxBytes): string
    {
        if (preg_match('//u', $s) !== 1) {
            $s = self::scrubUtf8($s);
        }
        if (strlen($s) <= $maxBytes) {
            return $s;
        }

        $cut = substr($s, 0, $maxBytes);
        // Un corte a mitad de secuencia multibyte deja como mucho 3 bytes de cola incompletos.
        while ($cut !== '' && preg_match('//u', $cut) !== 1) {
            $cut = substr($cut, 0, -1);
        }

        return $cut;
    }

    /** Sustituye cada byte que no es UTF-8 válido por U+FFFD. */
    public static function scrubUtf8(string $s): string
    {
        if (function_exists('mb_scrub')) {
            return mb_scrub($s, 'UTF-8');
        }

        return (string) preg_replace('/[^\x{0}-\x{10FFFF}]/u', "\u{FFFD}", $s);
    }

    /**
     * ¿Es solo espacio en blanco (`White_Space` de Unicode)? Un mensaje así no dice nada y quien
     * responde lo sustituye por uno fijo (11-request-reply.md §3.3).
     */
    public static function isBlank(string $s): bool
    {
        return preg_match('/^[\t\n\v\f\r \x{85}\x{A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}]*\z/u', $s) === 1;
    }

    /** ¿Tiene algún carácter de control (U+0000–U+001F ni U+007F–U+009F)? */
    public static function hasControlChars(string $s): bool
    {
        return preg_match('/\p{Cc}/u', $s) === 1;
    }
}
