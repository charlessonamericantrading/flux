<?php

declare(strict_types=1);

namespace Flux\State;

/**
 * Cuenta cuántas entregas de un `watch` siguen en vuelo y cuándo se alcanzó «lo que había al abrir» (10-state.md §5.5).
 *
 * `onSynced` se invoca **una vez**: cuando el handler ha procesado todo lo que había pendiente al abrir. Con el stream vacío (o el durable
 * al día) de inmediato y antes de entregar nada; si no, cuando llega una entrega con `pending == 0` —el número que el broker pone en los
 * metadatos de cada mensaje— **y** todos los handlers lanzados han terminado, con éxito o con error (una entrada RETRYABLE que falla no
 * retrasa `onSynced`).
 */
final class SyncTracker
{
    private bool $done = false;
    private int $inflight = 0;
    private bool $sawLast = false;

    /** @param \Closure():void $fire */
    public function __construct(private readonly \Closure $fire)
    {
    }

    public function isDone(): bool
    {
        return $this->done;
    }

    /** Al abrir: nada pendiente. */
    public function immediate(): void
    {
        if ($this->done) {
            return;
        }
        $this->done = true;
        ($this->fire)();
    }

    /** Una entrega empieza. `$pending` es lo que quedaba por entregar tras ella (0 = la última de la foto: A-35). */
    public function started(?int $pending): void
    {
        $this->inflight++;
        if ($pending === 0) {
            $this->sawLast = true;
        }
    }

    public function finished(): void
    {
        $this->inflight--;
        if (!$this->done && $this->sawLast && $this->inflight === 0) {
            $this->done = true;
            ($this->fire)();
        }
    }
}
