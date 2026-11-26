<?php

declare(strict_types=1);

namespace Flux;

/**
 * La clave no cumple `^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$` (10-state.md §2.3). PERMANENT.
 */
final class InvalidStateKeyException extends CodedError
{
    public function __construct(mixed $key, ?\Throwable $previous = null)
    {
        $shown = is_string($key) ? json_encode(Text::truncateCodePoints($key, 70), JSON_UNESCAPED_UNICODE) : get_debug_type($key);
        parent::__construct(
            'INVALID_STATE_KEY',
            ErrorClass::Permanent,
            "clave de estado inválida {$shown}: debe cumplir ^[A-Za-z0-9][A-Za-z0-9_-]{0,127}\$ —un único token de NATS, de 1 a 128 "
            . 'caracteres, sin puntos, comodines ni espacios y sin empezar por `_`—. El SDK no la codifica por ti: dos escapes distintos '
            . 'serían dos claves distintas para el mismo agregado (10-state.md §2.3).',
            $previous,
        );
    }
}
