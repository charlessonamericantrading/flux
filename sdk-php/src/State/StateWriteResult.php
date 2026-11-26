<?php

declare(strict_types=1);

namespace Flux\State;

/**
 * Resultado de `put` y `delete` — 10-state.md §3.5. No hay un tercer `outcome`: un conflicto agotado es un error.
 */
final readonly class StateWriteResult
{
    public function __construct(
        public bool $applied,
        /** `ok` | `stale` */
        public string $outcome,
        /** `ok`: la secuencia del mensaje nuevo. `stale`: la del mensaje ACTUAL, el que la hizo obsoleta. */
        public int $revision,
    ) {
    }
}
