<?php

declare(strict_types=1);

namespace Flux;

/**
 * Las opciones de credenciales están **mal formadas** y se sabe sin hablar con el broker (04-errors.md §5.3). PERMANENT. Nunca se
 * interpola el valor de una credencial: este mensaje acaba en logs.
 */
final class InvalidCredentialsException extends CodedError
{
    public function __construct(string $reason, ?\Throwable $previous = null)
    {
        parent::__construct(
            'INVALID_CREDENTIALS',
            ErrorClass::Permanent,
            "credenciales inválidas en connect(): {$reason}. Se detectó sin abrir ningún socket y reintentar no lo arregla: corrige lo que "
            . 'se pasa a `credentials` (06-security.md §2).',
            $previous,
        );
    }
}
