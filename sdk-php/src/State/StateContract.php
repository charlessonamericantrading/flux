<?php

declare(strict_types=1);

namespace Flux\State;

/** Un contrato de estado ya validado: `clientes.cliente.v1` y sus partes. */
final readonly class StateContract
{
    public function __construct(
        /** `clientes.cliente.v1`: el subject sin `state.`, sin tenant y sin clave. */
        public string $name,
        public string $domain,
        public string $aggregate,
        public int $major,
    ) {
    }
}
