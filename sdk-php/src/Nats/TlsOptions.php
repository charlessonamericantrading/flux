<?php

declare(strict_types=1);

namespace Flux\Nats;

use Flux\InvalidTlsOptionsException;

/**
 * TLS del listener. 06-security.md §2: es obligatorio en producción.
 *
 * - `caFile`: la CA que firma el certificado del servidor NATS, si no es una CA pública. Es la que manda: **no se mezcla** con las
 *   del sistema.
 * - `certFile` + `keyFile`: certificado de cliente (mTLS). Van juntos.
 * - Sin ninguna (`new TlsOptions()` o `[]`): exige TLS y verifica contra las CA del sistema.
 *
 * La verificación es **siempre completa**: la cadena contra la CA **y** el nombre del host contra los SAN del certificado (con la
 * IP, si te conectas por IP). No hay una opción para desactivarla: es la forma más corriente de acabar con TLS «obligatorio» y
 * sin protección.
 *
 * Las rutas son **ficheros** y se comprueban aquí, sin red: una ruta vacía, ilegible o que no es un PEM es `INVALID_TLS_OPTIONS`.
 * Un certificado que el broker no acepta es otra cosa —`CONNECTION_REJECTED`— y solo lo decide el handshake.
 */
final readonly class TlsOptions
{
    private const KEYS = ['caFile', 'certFile', 'keyFile'];

    public function __construct(
        public ?string $caFile = null,
        public ?string $certFile = null,
        public ?string $keyFile = null,
    ) {
        foreach (['caFile' => $caFile, 'certFile' => $certFile, 'keyFile' => $keyFile] as $name => $path) {
            if ($path === null) {
                continue;
            }
            if (trim($path) === '') {
                throw new InvalidTlsOptionsException("`tls.{$name}` debe ser una ruta no vacía");
            }
            if (!is_file($path) || !is_readable($path)) {
                throw new InvalidTlsOptionsException("`tls.{$name}` apunta a \"{$path}\", que no es un fichero legible");
            }
            $head = (string) @file_get_contents($path, false, null, 0, 4096);
            if (!str_contains($head, '-----BEGIN')) {
                throw new InvalidTlsOptionsException("`tls.{$name}` apunta a \"{$path}\", que no es un PEM (no contiene «-----BEGIN»)");
            }
        }
        if (($certFile === null) !== ($keyFile === null)) {
            throw new InvalidTlsOptionsException('`certFile` y `keyFile` van juntos (certificado de cliente)');
        }
    }

    /**
     * @param array<string,mixed>|self|null $options `null` = sin opciones TLS. **`[]` NO es `null`**: exige TLS con las CA del sistema.
     *
     * @throws InvalidTlsOptionsException
     */
    public static function from(array|self|null $options): ?self
    {
        if ($options === null || $options instanceof self) {
            return $options;
        }
        $unknown = array_values(array_diff(array_map('strval', array_keys($options)), self::KEYS));
        if ($unknown !== []) {
            throw new InvalidTlsOptionsException('claves desconocidas: ' . implode(', ', $unknown) . '. Se aceptan: ' . implode(', ', self::KEYS));
        }
        foreach ($options as $k => $v) {
            if (!is_string($v)) {
                throw new InvalidTlsOptionsException("`tls.{$k}` debe ser una ruta (texto), y llegó " . get_debug_type($v));
            }
        }

        return new self($options['caFile'] ?? null, $options['certFile'] ?? null, $options['keyFile'] ?? null);
    }
}
