<?php

declare(strict_types=1);

namespace Flux\State;

/**
 * Una entrada de estado, tal como la ve quien la lee (`get`, handler de `watch`) — 10-state.md §5.3.
 */
final readonly class StateEntry
{
    public function __construct(
        /** La clave. */
        public string $key,
        /**
         * El tenant del subject. No estaba en el plan y es necesario: un `watch` sin tenant recibe `cli-1` de
         * varios, y la clave sola mezclaría los tenants en la copia local.
         */
        public string $tenantId,
        /** `true` si es una lápida. */
        public bool $deleted,
        /** El `data` del evento; `null` si `deleted`. */
        public ?array $data,
        /** La versión del valor (o la de la lápida). */
        public int $aggregateVersion,
        /** La secuencia del stream: dice QUÉ mensaje es; no ordena versiones ni se compara entre contratos. */
        public int $revision,
    ) {
    }
}
