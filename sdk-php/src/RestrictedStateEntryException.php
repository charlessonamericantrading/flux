<?php

declare(strict_types=1);

namespace Flux;

/**
 * Una entrada de estado que llega con `dataclassification: restricted` (10-state.md §7). POISON: `restricted` no se admite en
 * estado, y un mensaje que lo lleve no debería existir.
 */
final class RestrictedStateEntryException extends CodedError
{
    public function __construct(?\Throwable $previous = null)
    {
        parent::__construct(
            'INVALID_DATACLASSIFICATION',
            ErrorClass::Poison,
            'una entrada de estado llegó con `dataclassification: restricted`, que no se admite en estado (su retención de 24 h no cabe en '
            . 'un almacén que no caduca; 10-state.md §7). Quien la escribió no es un SDK de flux.',
            $previous,
        );
    }
}
