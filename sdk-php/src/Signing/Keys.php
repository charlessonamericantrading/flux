<?php

declare(strict_types=1);

namespace Flux\Signing;

/**
 * Material de clave Ed25519: PEM ↔ bytes crudos, y generación de pares.
 * Contrato normativo: specification/07-signing.md §3 y §6.
 *
 * ## Por qué PEM y no el formato nativo de libsodium
 *
 * `sodium_crypto_sign_*` trabaja con bytes crudos: 32 de clave pública y **64 de "secret
 * key"** (semilla ‖ pública), sin ninguna envoltura. Los otros cinco SDKs usan PEM
 * —PKCS#8 para la privada, SPKI para la pública— porque es lo que exportan `node:crypto`,
 * `cryptography`, `crypto/ed25519`, `java.security` y `System.Security.Cryptography`.
 *
 * Si este SDK hubiese adoptado el formato de libsodium, **una clave generada en PHP no
 * serviría en ningún otro SDK y al revés**, y el operador tendría que convertirla a mano
 * en cada rotación. Eso no es un detalle de implementación: 07-signing.md §6 obliga a
 * conservar las claves públicas retiradas 90 días, así que un formato divergente
 * multiplica por seis el material que hay que custodiar y traducir.
 *
 * La conversión sale gratis porque **la envoltura DER de una clave Ed25519 es de tamaño
 * fijo** (RFC 8410): 16 bytes de cabecera constante + 32 de semilla en PKCS#8, y 12 + 32
 * en SPKI. No hay que parsear ASN.1: hay que comparar un prefijo. El SDK de Rust hace
 * exactamente lo mismo y por la misma razón.
 *
 * También se acepta la clave **cruda en base64**, porque es la forma en que la entregan
 * algunos gestores de secretos y porque es lo que devuelve `sodium_crypto_sign_keypair()`.
 */
final class Keys
{
    /**
     * Cabecera DER fija de una clave privada Ed25519 en PKCS#8 v1 — RFC 8410 §7.
     * `SEQUENCE { INTEGER 0, SEQUENCE { OID 1.3.101.112 }, OCTET STRING { OCTET STRING } }`
     */
    private const PKCS8_PREFIX = "\x30\x2e\x02\x01\x00\x30\x05\x06\x03\x2b\x65\x70\x04\x22\x04\x20";

    /** Cabecera DER fija de una clave pública Ed25519 en SPKI — RFC 8410 §4. */
    private const SPKI_PREFIX = "\x30\x2a\x30\x05\x06\x03\x2b\x65\x70\x03\x21\x00";

    private function __construct()
    {
    }

    /**
     * Comprueba que `ext-sodium` está cargada.
     *
     * Está en el core de PHP desde 7.2, así que **la firma no añade ninguna dependencia de
     * Composer**; pero muchas distribuciones la empaquetan aparte (`php-sodium`) y en
     * Windows viene comentada en `php.ini`. Sin esta comprobación el fallo sería
     * `Call to undefined function sodium_crypto_sign_detached()`, que no dice qué hacer.
     *
     * @throws SigningKeyException
     */
    public static function assertSodiumAvailable(): void
    {
        if (!extension_loaded('sodium')) {
            throw new SigningKeyException(
                'la firma de eventos (07-signing.md) necesita la extensión `sodium`, que forma '
                . 'parte del core de PHP desde 7.2 pero no siempre viene activada. Instálala '
                . '(`apt install php-sodium`, `dnf install php-sodium`) o descoméntala en '
                . 'php.ini (`extension=sodium`). Es la ÚNICA dependencia de la firma: no hace '
                . 'falta ningún paquete de Composer.'
            );
        }
    }

    /**
     * Los 32 bytes de semilla de una clave privada.
     *
     * Acepta PEM PKCS#8, base64 de 32 bytes (semilla) o base64 de 64 (la "secret key" de
     * libsodium, semilla ‖ pública). De la de 64 se toman **solo los 32 primeros**: la
     * mitad pública se deriva, no se lee, así que una clave con la parte pública manipulada
     * no puede colar una verificación.
     *
     * @throws SigningKeyException
     */
    public static function privateKeySeed(string $material): string
    {
        $der = self::decode($material, 'PRIVATE KEY');

        if (strlen($der) === 48 && str_starts_with($der, self::PKCS8_PREFIX)) {
            return substr($der, 16, 32);
        }
        if (strlen($der) === 32 || strlen($der) === 64) {
            return substr($der, 0, 32);
        }

        throw new SigningKeyException(sprintf(
            'clave privada no reconocida (%d bytes). Se espera PEM PKCS#8 de Ed25519 '
            . '(`openssl genpkey -algorithm ed25519`), o base64 de 32 bytes de semilla / 64 de '
            . 'secret key de libsodium. El protocolo NO negocia algoritmo a propósito: los '
            . 'formatos con algoritmo negociable acumulan una familia de vulnerabilidades '
            . 'conocida que solo existe porque hay algo que negociar, así que si esto es una '
            . 'RSA o una EC no hay nada que ajustar (07-signing.md §3).',
            strlen($der)
        ));
    }

    /**
     * Los 32 bytes de una clave pública. Acepta PEM SPKI o base64 crudo.
     *
     * @throws SigningKeyException
     */
    public static function publicKeyBytes(string $material): string
    {
        $der = self::decode($material, 'PUBLIC KEY');

        if (strlen($der) === 44 && str_starts_with($der, self::SPKI_PREFIX)) {
            return substr($der, 12, 32);
        }
        if (strlen($der) === 32) {
            return $der;
        }

        throw new SigningKeyException(sprintf(
            'clave pública no reconocida (%d bytes). Se espera PEM SPKI de Ed25519 o base64 '
            . 'de 32 bytes.',
            strlen($der)
        ));
    }

    /**
     * La "secret key" de 64 bytes que espera `sodium_crypto_sign_detached`.
     *
     * @throws SigningKeyException
     */
    public static function secretKeyFromSeed(string $seed): string
    {
        self::assertSodiumAvailable();

        return sodium_crypto_sign_secretkey(sodium_crypto_sign_seed_keypair($seed));
    }

    /**
     * Genera un par Ed25519 en PEM (PKCS#8 + SPKI).
     *
     * Comodidad para tests y para `flux keygen`. El PEM es el formato **interoperable**: la
     * clave que sale de aquí la leen tal cual los SDKs de Node, Python, Go, Java, .NET y
     * Rust.
     *
     * ⚠️ El `signkeyid` **DEBE** cambiar en cada rotación. Reutilizar un id con una clave
     * distinta convierte la verificación de eventos históricos en un juego de azar
     * — 07-signing.md §6.
     *
     * @return array{privateKeyPem: string, publicKeyPem: string}
     *
     * @throws SigningKeyException
     */
    public static function generateKeyPair(): array
    {
        self::assertSodiumAvailable();

        $pair = sodium_crypto_sign_keypair();
        // La secret key de libsodium es semilla ‖ pública: la semilla son los 32 primeros.
        $seed = substr(sodium_crypto_sign_secretkey($pair), 0, 32);

        return [
            'privateKeyPem' => self::pem(self::PKCS8_PREFIX . $seed, 'PRIVATE KEY'),
            'publicKeyPem' => self::pem(
                self::SPKI_PREFIX . sodium_crypto_sign_publickey($pair),
                'PUBLIC KEY'
            ),
        ];
    }

    /** DER → PEM con líneas de 64 caracteres, como `openssl`. */
    public static function pem(string $der, string $label): string
    {
        return "-----BEGIN {$label}-----\n"
            . chunk_split(base64_encode($der), 64, "\n")
            . "-----END {$label}-----\n";
    }

    /**
     * PEM (con la armadura esperada) o base64 suelto → bytes.
     *
     * @throws SigningKeyException
     */
    private static function decode(string $material, string $label): string
    {
        $trimmed = trim($material);

        if (str_starts_with($trimmed, '-----BEGIN')) {
            $begin = "-----BEGIN {$label}-----";
            if (!str_starts_with($trimmed, $begin)) {
                $primera = strtok($trimmed, "\n");
                throw new SigningKeyException(
                    "se esperaba un bloque PEM `{$begin}` y llegó `{$primera}`"
                );
            }
            $body = implode('', array_filter(
                preg_split('/\R/', $trimmed) ?: [],
                static fn (string $line): bool => !str_starts_with($line, '-----'),
            ));
        } else {
            $body = preg_replace('/\s+/', '', $trimmed) ?? '';
        }

        // `strict` a propósito: sin él, base64_decode ignora los caracteres inválidos en
        // silencio y una clave con una errata produciría bytes plausibles pero
        // equivocados, es decir firmas que nunca verifican y ningún mensaje que lo explique.
        $der = base64_decode($body, true);
        if ($der === false) {
            throw new SigningKeyException('la clave no es base64 válido');
        }

        return $der;
    }
}
