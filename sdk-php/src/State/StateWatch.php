<?php

declare(strict_types=1);

namespace Flux\State;

/**
 * Un `watch` abierto: **una foto completa y después los cambios, por el mismo canal** (un consumidor durable de `STATE_<DOM>`).
 *
 * En PHP no hay bucle de eventos: el consumo lo conduce `FluxBus::run()` (o `StateClient::poll()`), en un worker de larga vida. Un `watch`
 * en un script web no recibe nada. `stop()` deja de pedir mensajes; el durable sigue existiendo (es lo que significa *durable*).
 */
final class StateWatch
{
    public bool $active = true;
    /** El durable se borró bajo el watch (`flux state resync`): se reabre el consumo cuando exista de nuevo. */
    public bool $reopening = false;
    public int $reopenAttempt = 0;
    public float $nextReopenAt = 0.0;
    public string $lastReopenReason = '';
    /** La secuencia de consumidor de la última entrega: si una nueva no es mayor, el durable se recreó (`Ack::consumerSequence`). */
    public ?int $lastConsumerSeq = null;

    /**
     * @param \Closure(StateEntry,StateHandlerContext):void $handler
     * @param array<string,mixed> $requested la configuración pedida del durable, para comprobarla al reabrir
     */
    public function __construct(
        public readonly string $contract,
        public readonly ?string $tenantId,
        public readonly string $stream,
        public readonly string $durable,
        /** El durable SIN el sufijo de tenant: la etiqueta `consumer` de las métricas (08-observability.md §2.4). */
        public readonly string $consumerLabel,
        /** `state.<contrato>`: la etiqueta `subject` de las métricas. */
        public readonly string $subjectLabel,
        public readonly \Closure $handler,
        public readonly SyncTracker $sync,
        public readonly array $requested,
    ) {
    }

    public function stop(): void
    {
        $this->active = false;
    }
}
