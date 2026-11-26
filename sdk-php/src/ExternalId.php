<?php

declare(strict_types=1);

namespace Flux;

/**
 * El `id` DETERMINISTA de un hecho importado de un sistema externo.
 * Contrato normativo: specification/01-envelope.md §2.6.1.
 *
 * Un proveedor (una pasarela de pagos, un sistema de terceros) reenvía el mismo aviso durante horas.
 * Si cada reenvío recibe un `id` nuevo, ningún consumidor puede deduplicarlo: el `id` es la clave de
 * deduplicación del ecosistema. Para un hecho importado, el `id` se DERIVA de lo que identifica al
 * hecho, y es siempre el mismo:
 *
 *     id = UUIDv7 cuyos 48 bits de tiempo son los milisegundos del `time` del hecho externo y cuyos
 *          74 bits restantes (`rand_a` y `rand_b`) son los 74 primeros bits de
 *          SHA-256( <source> "|" <clave externa> )
 *
 * Es un UUIDv7 VÁLIDO (versión 7, variante 10) y una función PURA: no usa el reloj ni ninguna fuente
 * de azar. Es lo que el arnés cruzado compara byte a byte entre SDKs.
 */
final class ExternalId
{
    /** 2^48 − 1: lo que cabe en el campo de tiempo de un UUIDv7. */
    public const UUIDV7_MAX_MS = 281_474_976_710_655;

    /** Longitud máxima de una clave externa, en bytes UTF-8: es un identificador, no un documento. */
    public const EXTERNAL_KEY_MAX_BYTES = 512;

    private function __construct()
    {
    }

    /**
     * Los milisegundos de un instante —`DateTimeInterface`, un RFC 3339 o los milisegundos desde la época—, o
     * `EnvelopeException` si no es un instante válido que quepa en 48 bits.
     *
     * @throws EnvelopeException
     */
    public static function instantToMs(\DateTimeInterface|string|int $time): int
    {
        $ms = match (true) {
            $time instanceof \DateTimeInterface => (int) $time->format('U') * 1000 + (int) $time->format('v'),
            is_int($time) => $time,
            default => Time::toEpochMs($time),
        };

        if ($ms === null || $ms < 0 || $ms > self::UUIDV7_MAX_MS) {
            throw new EnvelopeException(
                '`time` no es un instante válido para el id determinista (se necesitan milisegundos enteros entre 0 y '
                . self::UUIDV7_MAX_MS . '). Pasa el instante del hecho externo como RFC 3339 UTC o como DateTimeInterface '
                . '(01-envelope.md §2.6.1)'
            );
        }

        return $ms;
    }

    /**
     * `SHA-256(<source> "|" <externalKey>)`, y de ahí el UUIDv7.
     *
     * @throws EnvelopeException ante una clave vacía o demasiado larga, un `source` con `|` o un instante inválido.
     */
    public static function derive(string $source, string $externalKey, \DateTimeInterface|string|int $time): string
    {
        if ($source === '' || str_contains($source, '|')) {
            throw new EnvelopeException(
                '`source` debe ser una cadena no vacía y sin `|` (es el separador de la derivación del id determinista)'
            );
        }
        if ($externalKey === '') {
            throw new EnvelopeException(
                '`externalKey` debe ser una cadena no vacía: es la clave del hecho en el sistema externo (el id del evento del '
                . 'proveedor). Sin ella no hay nada de lo que derivar un `id` estable (01-envelope.md §2.6.1)'
            );
        }
        if (strlen($externalKey) > self::EXTERNAL_KEY_MAX_BYTES) {
            throw new EnvelopeException(
                '`externalKey` ocupa más de ' . self::EXTERNAL_KEY_MAX_BYTES . ' bytes UTF-8: es un identificador, no un documento. '
                . 'Usa el id del hecho en el sistema externo'
            );
        }
        $ms = self::instantToMs($time);

        // Los 74 primeros bits del digest: 80 bits (10 bytes) menos los 6 últimos. PHP no tiene enteros de
        // 128 bits: se trabaja por bytes, igual que `Protocol::uuid7()`.
        $d = hash('sha256', $source . '|' . $externalKey, true);
        $b = array_values(unpack('C10', $d)); // 10 bytes = 80 bits; los 74 primeros son los útiles

        // rand_a = bits 0..11 de H; rand_b = bits 12..73 de H.
        $randA = ($b[0] << 4) | ($b[1] >> 4);                       // 12 bits
        // rand_b: bits 12..73 → 62 bits. Se arma en 8 bytes con la variante `10` delante.
        $hi = (($b[1] & 0x0F) << 4) | ($b[2] >> 4);                 // bits 12..19
        $bytes = [$hi];
        // Los bits 20..73 son 54 bits: 6 bytes completos más 6 bits. Se desplazan 4 bits sobre el byte 2.
        for ($i = 2; $i <= 8; $i++) {
            $bytes[] = (($b[$i] & 0x0F) << 4) | ($b[$i + 1] >> 4);
        }
        // `$bytes` tiene 8 bytes (62 bits útiles + 2 de relleno al final): los 62 bits de rand_b están en los
        // 62 primeros; hay que dejar sitio a los dos bits de variante AL PRINCIPIO.
        $randB = self::shiftRight2($bytes);                         // 8 bytes: 00 + 62 bits de rand_b
        $randB[0] = ($randB[0] & 0x3F) | 0x80;                      // variante 10

        $time48 = pack('n', ($ms >> 32) & 0xFFFF) . pack('N', $ms & 0xFFFF_FFFF);
        $verRandA = chr(0x70 | (($randA >> 8) & 0x0F)) . chr($randA & 0xFF);

        $hex = bin2hex($time48 . $verRandA . implode('', array_map('chr', $randB)));

        return substr($hex, 0, 8) . '-' . substr($hex, 8, 4) . '-' . substr($hex, 12, 4)
            . '-' . substr($hex, 16, 4) . '-' . substr($hex, 20, 12);
    }

    /**
     * Desplaza 2 bits a la derecha una lista de 8 bytes (64 bits), arrastrando entre bytes.
     *
     * @param list<int> $bytes
     * @return list<int>
     */
    private static function shiftRight2(array $bytes): array
    {
        $out = [];
        $carry = 0;
        foreach ($bytes as $byte) {
            $out[] = (($byte >> 2) | ($carry << 6)) & 0xFF;
            $carry = $byte & 0x03;
        }

        return $out;
    }
}
