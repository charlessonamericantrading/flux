<?php

declare(strict_types=1);

namespace Flux;

/**
 * Las opciones TLS están **mal formadas** y se sabe sin red (04-errors.md §5.3). PERMANENT.
 */
final class InvalidTlsOptionsException extends CodedError
{
    public function __construct(string $reason, ?\Throwable $previous = null)
    {
        parent::__construct(
            'INVALID_TLS_OPTIONS',
            ErrorClass::Permanent,
            "opciones TLS inválidas en connect(): {$reason}. Se detectó sin abrir ningún socket y reintentar no lo arregla: corrige lo que "
            . 'se pasa a `tls` (06-security.md §2).',
            $previous,
        );
    }
}
