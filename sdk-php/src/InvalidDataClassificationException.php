<?php

declare(strict_types=1);

namespace Flux;

/**
 * `classification` fuera del enum —o `restricted` en un estado— (01-envelope.md §3.1, 10-state.md §7). PERMANENT.
 */
final class InvalidDataClassificationException extends CodedError
{
    public function __construct(mixed $value, string $context = 'event', ?\Throwable $previous = null)
    {
        $shown = is_string($value) ? json_encode(Text::truncateCodePoints($value, 40), JSON_UNESCAPED_UNICODE) : get_debug_type($value);
        parent::__construct(
            'INVALID_DATACLASSIFICATION',
            ErrorClass::Permanent,
            "dataclassification inválido: {$shown}. Valores permitidos: "
            . ($context === 'state'
                ? 'public, internal, confidential (`restricted` no se admite en estado: su retención de 24 h no cabe en un almacén que no caduca, 10-state.md §7)'
                : 'public, internal, confidential, restricted'),
            $previous,
        );
    }
}
