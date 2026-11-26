<?php

declare(strict_types=1);

namespace Flux\Rpc;

/** Una operación ya validada: `clientes.cliente.v1.calcular-descuento` y sus partes. */
final readonly class RpcOperation
{
    public function __construct(
        /** Lo que reciben `request` y `respond`. */
        public string $operation,
        public string $domain,
        public string $aggregate,
        public int $major,
        /** `calcular-descuento`: el último token, en infinitivo. */
        public string $name,
    ) {
    }
}
