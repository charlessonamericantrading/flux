<?php

declare(strict_types=1);

namespace Flux;

/**
 * El contrato no es `<dominio>.<agregado>.v<major>` (10-state.md §2.2, 04-errors.md §6). PERMANENT.
 */
final class InvalidStateContractException extends CodedError
{
    public function __construct(mixed $contract, string $why, ?\Throwable $previous = null)
    {
        $shown = is_string($contract) ? json_encode(Text::truncateCodePoints($contract, 80), JSON_UNESCAPED_UNICODE) : get_debug_type($contract);
        parent::__construct(
            'INVALID_STATE_CONTRACT',
            ErrorClass::Permanent,
            "contrato de estado inválido {$shown}: {$why}. El contrato es el subject sin `state.`, sin tenant y sin clave: "
            . '`clientes.cliente.v1` (10-state.md §2.2).',
            $previous,
        );
    }
}
