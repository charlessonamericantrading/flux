<?php

declare(strict_types=1);

namespace Flux;

/**
 * `data.aggregateVersion` no es un entero ≥ 1 y ≤ 2^53 − 1 (10-state.md §3.4). PERMANENT. El nombre cubre todo lo que no sea
 * eso, no solo la ausencia.
 */
final class MissingAggregateVersionException extends CodedError
{
    public function __construct(mixed $value = null, ?\Throwable $previous = null)
    {
        parent::__construct(
            'MISSING_AGGREGATE_VERSION',
            ErrorClass::Permanent,
            '`data.aggregateVersion` debe ser un entero entre 1 y 2^53 − 1 (el contador monotónico de tu agregado) y llegó '
            . ($value === null ? 'ausente' : get_debug_type($value)) . '. Lo pone el dueño del agregado, no el SDK; con `7.0` o `"7"` '
            . 'tampoco vale (10-state.md §3.4).',
            $previous,
        );
    }
}
