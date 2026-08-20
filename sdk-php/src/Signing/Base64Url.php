<?php

declare(strict_types=1);

namespace Flux\Signing;

/**
 * base64url **sin padding** — el formato que 07-signing.md §4 exige para `signature`.
 *
 * No es un detalle cosmético. Con padding, la misma firma tendría dos representaciones
 * posibles (`…BA` y `…BA==`) y dos eventos byte-distintos serían el mismo evento: se
 * romperían la deduplicación por hash de contenido y los fixtures compartidos entre SDKs,
 * exactamente los mismos cuatro sitios que 01-envelope.md §6 protege con el orden de
 * claves.
 *
 * PHP no trae base64url. `strtr` + `rtrim` sobre `base64_encode` son 2 líneas y evitan
 * añadir una dependencia por eso.
 */
final class Base64Url
{
    private function __construct()
    {
    }

    public static function encode(string $bytes): string
    {
        return rtrim(strtr(base64_encode($bytes), '+/', '-_'), '=');
    }

    /**
     * `null` si la cadena no es base64url válido.
     *
     * Devuelve `null` en vez de lanzar porque el llamante es la verificación de una firma:
     * una `signature` con basura no es un error del programa, es un evento manipulado, y
     * su respuesta correcta es `INVALID_SIGNATURE` (§7), no una excepción distinta.
     */
    public static function decode(string $value): ?string
    {
        // `strict` a propósito: sin él, base64_decode ignora los caracteres inválidos y
        // una firma corrupta produciría bytes plausibles en vez de fallar.
        $decoded = base64_decode(strtr($value, '-_', '+/'), true);

        return $decoded === false ? null : $decoded;
    }
}
