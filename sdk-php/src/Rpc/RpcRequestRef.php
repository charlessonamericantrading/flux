<?php

declare(strict_types=1);

namespace Flux\Rpc;

use Flux\FluxEvent;

/**
 * Lo que la respuesta HEREDA de la petición. El arnés cruzado lo da plano; el SDK lo saca del sobre recibido
 * (`RpcRequestRef::fromEvent`).
 */
final readonly class RpcRequestRef
{
    public function __construct(
        public string $operation,
        public string $id,
        public string $correlationid,
        public string $tenantid,
        public ?string $subject = null,
        /** Si la petición lo traía; si no, el `subject`, que es la misma convención que en los eventos. */
        public ?string $partitionkey = null,
    ) {
    }

    public static function fromEvent(string $operation, FluxEvent $e): self
    {
        return new self($operation, $e->id, $e->correlationid, $e->tenantid, $e->subject, $e->partitionkey);
    }

    /** `partitionkey` de la respuesta: el de la petición, o su `subject` (la convención de los eventos). */
    public function partition(): ?string
    {
        return $this->partitionkey ?? $this->subject;
    }
}
