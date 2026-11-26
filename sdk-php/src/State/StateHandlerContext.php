<?php

declare(strict_types=1);

namespace Flux\State;

/** Lo que recibe un handler de `watch` junto a la entrada. Confirmar es devolver del handler sin error. */
final readonly class StateHandlerContext
{
    public function __construct(
        /** Nº de entrega, empezando en 1. */
        public int $attempt,
        public int $maxAttempts,
        /** El subject de NATS de la entrada (`state.clientes.cliente.v1.acme.cli-1`). */
        public string $subject,
        /** El contrato observado (`clientes.cliente.v1`). */
        public string $contract,
    ) {
    }

    /** Confirmar es devolver del handler sin error; esto no hace nada (existe por simetría con los eventos). */
    public function ack(): void
    {
    }
}
