<?php

declare(strict_types=1);

namespace Flux\Nats;

/**
 * Las nkeys de NATS (Ed25519): decodificar una semilla, derivar la clave pública y firmar el nonce del servidor.
 *
 * Solo lo que la conexión necesita, sobre `ext-sodium`: base32 (RFC 4648, sin relleno), CRC-16/XMODEM y el prefijo de tipo. Una
 * semilla es `S` + el tipo de clave (`SU…` = usuario) y 32 bytes de semilla Ed25519, con un CRC al final. **Es un secreto**: nunca
 * se interpola en un mensaje de error.
 */
final class Nkeys
{
    private const ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
    private const PREFIX_SEED = 18 << 3;
    private const PREFIX_USER = 20 << 3;

    private function __construct()
    {
    }

    /**
     * Decodifica una semilla de USUARIO (`SU…`).
     *
     * @return array{seed:string,public:string} la semilla de 32 bytes y la clave pública (`U…`, base32)
     *
     * @throws \InvalidArgumentException si no es una semilla de usuario bien formada (sin citar su valor)
     */
    public static function fromSeed(string $seed): array
    {
        $raw = self::base32Decode(trim($seed));
        if ($raw === null || strlen($raw) !== 36) {
            throw new \InvalidArgumentException('no es una semilla nkey: debe ser base32 de 36 bytes');
        }
        if (self::crc16(substr($raw, 0, 34)) !== (ord($raw[34]) | (ord($raw[35]) << 8))) {
            throw new \InvalidArgumentException('la semilla nkey no es válida: su CRC no coincide (¿truncada o con un carácter cambiado?)');
        }
        $b1 = ord($raw[0]) & 0xF8;
        $prefix = ((ord($raw[0]) & 0x07) << 5) | ((ord($raw[1]) & 0xF8) >> 3);
        if ($b1 !== self::PREFIX_SEED) {
            throw new \InvalidArgumentException('no es una semilla nkey: no empieza por «S»');
        }
        if ($prefix !== self::PREFIX_USER) {
            throw new \InvalidArgumentException('debe ser la semilla de una nkey de USUARIO (empieza por «SU»)');
        }

        $pair = sodium_crypto_sign_seed_keypair(substr($raw, 2, 32));
        $public = sodium_crypto_sign_publickey($pair);
        $body = chr(self::PREFIX_USER) . $public;
        $crc = self::crc16($body);

        return [
            'seed' => substr($raw, 2, 32),
            'public' => self::base32Encode($body . chr($crc & 0xFF) . chr($crc >> 8)),
        ];
    }

    /**
     * Una semilla de usuario NUEVA (`SU…`). Para tests y herramientas: el SDK no crea identidades, las da quien opera el broker.
     */
    public static function generateSeed(): string
    {
        $body = chr(self::PREFIX_SEED | (self::PREFIX_USER >> 5)) . chr((self::PREFIX_USER & 31) << 3) . random_bytes(32);
        $crc = self::crc16($body);

        return self::base32Encode($body . chr($crc & 0xFF) . chr($crc >> 8));
    }

    /** La firma Ed25519 del nonce del servidor, en base64url sin relleno (lo que `nats-server` espera en `sig`). */
    public static function sign(string $seed32, string $nonce): string
    {
        $secret = sodium_crypto_sign_secretkey(sodium_crypto_sign_seed_keypair($seed32));

        return rtrim(strtr(base64_encode(sodium_crypto_sign_detached($nonce, $secret)), '+/', '-_'), '=');
    }

    /** CRC-16/XMODEM (poly 0x1021, init 0). */
    public static function crc16(string $data): int
    {
        $crc = 0;
        for ($i = 0, $n = strlen($data); $i < $n; $i++) {
            $crc ^= ord($data[$i]) << 8;
            for ($b = 0; $b < 8; $b++) {
                $crc = ($crc & 0x8000) !== 0 ? (($crc << 1) ^ 0x1021) & 0xFFFF : ($crc << 1) & 0xFFFF;
            }
        }

        return $crc;
    }

    public static function base32Encode(string $data): string
    {
        $out = '';
        $buffer = 0;
        $bits = 0;
        for ($i = 0, $n = strlen($data); $i < $n; $i++) {
            $buffer = ($buffer << 8) | ord($data[$i]);
            $bits += 8;
            while ($bits >= 5) {
                $out .= self::ALPHABET[($buffer >> ($bits - 5)) & 31];
                $bits -= 5;
            }
            $buffer &= (1 << $bits) - 1;
        }
        if ($bits > 0) {
            $out .= self::ALPHABET[($buffer << (5 - $bits)) & 31];
        }

        return $out;
    }

    public static function base32Decode(string $text): ?string
    {
        $out = '';
        $buffer = 0;
        $bits = 0;
        for ($i = 0, $n = strlen($text); $i < $n; $i++) {
            $v = strpos(self::ALPHABET, $text[$i]);
            if ($v === false) {
                return null;
            }
            $buffer = ($buffer << 5) | $v;
            $bits += 5;
            if ($bits >= 8) {
                $out .= chr(($buffer >> ($bits - 8)) & 0xFF);
                $bits -= 8;
                $buffer &= (1 << $bits) - 1;
            }
        }

        return $out;
    }
}
