<?php

declare(strict_types=1);

namespace Flux\Transport;

/**
 * Un transporte que sabe comprobar que el servicio PUEDE pedir mensajes de su consumidor.
 *
 * Es el requisito de 06-security.md §3.7 (**DEBE**, no DEBERÍA): un pull denegado no da error —el broker descarta la petición y avisa
 * aparte— y deja un consumidor **inerte que parece sano**. `FluxBus::subscribe()` lo llama antes de devolver y falla con
 * `PERMISSION_DENIED` nombrando el subject completo. Es opcional (una interfaz aparte) para no romper a quien ya implementa
 * `NatsTransport`: los transportes que no la implementan no dan esa garantía, y el README lo dice.
 */
interface ProbesPull
{
    /**
     * @throws \Flux\PermissionDeniedException si el broker deniega `$JS.API.CONSUMER.MSG.NEXT.<stream>.<durable>`
     */
    public function assertPullAllowed(string $stream, string $durable): void;
}
