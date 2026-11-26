<?php

declare(strict_types=1);

namespace Flux;

/**
 * El chequeo de secuencia agotó sus intentos: alguien escribió la misma clave entre la lectura y la publicación, todas las veces
 * (10-state.md §3.2). RETRYABLE: el conflicto es del entorno, no de la llamada.
 */
final class StateConflictException extends CodedError
{
    public function __construct(public readonly string $contract, public readonly string $key, int $attempts, ?\Throwable $previous = null)
    {
        parent::__construct(
            'STATE_CONFLICT',
            ErrorClass::Retryable,
            "no se pudo escribir la clave \"{$key}\" de {$contract} tras {$attempts} intentos: otra instancia la escribió entre la lectura y "
            . 'la publicación todas las veces. Es reintentable; si ocurre siempre, dos escritores compiten por la misma clave y '
            . 'conviene serializarlos (10-state.md §3.2).',
            $previous,
        );
    }
}
