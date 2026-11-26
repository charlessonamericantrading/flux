<?php

declare(strict_types=1);

namespace Flux;

/**
 * El `tenantid`, el `subject` o el `type` del sobre no coinciden con el subject de NATS en el que llegó la entrada
 * (10-state.md §5.4). POISON: el mensaje es legible pero **miente sobre su identidad**.
 */
final class StateSubjectMismatchException extends CodedError
{
    public function __construct(public readonly string $field, string $got, string $expected, ?\Throwable $previous = null)
    {
        parent::__construct(
            'STATE_SUBJECT_MISMATCH',
            ErrorClass::Poison,
            "la entrada de estado miente sobre su identidad: su `{$field}` es "
            . json_encode(Text::truncateCodePoints($got, 80), JSON_UNESCAPED_UNICODE) . ' y el del subject de NATS en el que llegó es '
            . json_encode(Text::truncateCodePoints($expected, 80), JSON_UNESCAPED_UNICODE) . '. Un productor que escribe así está roto '
            . 'o es hostil: no se aplica (10-state.md §5.4).',
            $previous,
        );
    }
}
