<?php

declare(strict_types=1);

namespace Flux;

/**
 * Un subject, un contrato o una operación usa un dominio reservado (`dlq`, `state`, `rpc`, `system`): cada uno es un espacio de
 * nombres del protocolo y un dominio con ese nombre colisionaría con él **en el broker** (10-state.md §2.7, 02-naming.md §1.3).
 * PERMANENT. Se comprueba antes de tocar la red y **antes** que la forma.
 */
final class ReservedDomainException extends CodedError
{
    /** @param list<string> $reserved */
    public function __construct(public readonly string $domain, array $reserved, string $what = 'subject', ?\Throwable $previous = null)
    {
        parent::__construct(
            'RESERVED_DOMAIN',
            ErrorClass::Permanent,
            "el dominio \"{$domain}\" está reservado al protocolo (reservados: " . implode(', ', $reserved) . ') y no puede usarse en '
            . ($what === 'operación' ? 'una' : 'un') . " {$what}. Cada uno es un espacio de nombres propio —`state.`, `dlq.`, `rpc.`—, y un "
            . 'evento publicado ahí no pasaría por el chequeo de secuencia del estado (10-state.md §2.7). Elige otro dominio.'
            . ($what === 'operación' && $domain === 'rpc'
                ? ' Si lo que escribiste es el subject de NATS (`rpc.clientes.cliente.v1.obtener`), quita el prefijo `rpc.`: lo pone el '
                . 'SDK, y el nombre de la operación es `clientes.cliente.v1.obtener` (11-request-reply.md §2.2).'
                : ''),
            $previous,
        );
    }
}
