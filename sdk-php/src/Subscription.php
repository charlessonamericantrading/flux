<?php

declare(strict_types=1);

namespace Flux;

/**
 * Handle de una suscripción registrada. No expone nada de NATS.
 *
 * "Cancelarla" solo deja de pedir mensajes: el durable consumer sigue existiendo en el
 * servidor, que es precisamente lo que significa *durable*. Los mensajes se acumulan y se
 * entregarán cuando el servicio vuelva; borrarlo desde el SDK tiraría el progreso de todo
 * el consumidor por reiniciar un proceso.
 */
final class Subscription
{
    public bool $active = true;

    /**
     * @param \Closure(FluxEvent,HandlerContext):void $handler
     */
    public function __construct(
        public readonly string $subject,
        public readonly string $durable,
        public readonly string $stream,
        public readonly \Closure $handler,
        /** Si no es `null`, se descartan (con ack) los eventos de otros tenants. */
        public readonly ?string $tenantId = null,
    ) {
    }

    public function cancel(): void
    {
        $this->active = false;
    }
}
