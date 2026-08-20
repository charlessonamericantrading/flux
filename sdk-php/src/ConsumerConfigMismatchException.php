<?php

declare(strict_types=1);

namespace Flux;

/**
 * El servidor devolvió una configuración de consumidor distinta de la solicitada.
 * Requisito L2 — 03-delivery.md §2.1.
 *
 * No es paranoia: JetStream **sobrescribe `ack_wait` con `backoff[0]`**, acepta la
 * petición, no avisa y devuelve algo distinto de lo enviado (verificado contra NATS
 * 2.14.5 vía `$JS.API.CONSUMER.DURABLE.CREATE`). Con un `ack_wait` efectivo de 1 s,
 * cualquier handler que escriba en una base de datos y llame a una API recibe el mismo
 * mensaje reentregado **mientras aún se está ejecutando**: ejecución concurrente del
 * mismo evento, en cada mensaje, bajo carga.
 *
 * Fallar en el arranque es lo correcto. La alternativa —arrancar y funcionar mal— produce
 * un servicio que parece sano en el healthcheck y corrompe datos bajo carga.
 */
final class ConsumerConfigMismatchException extends \RuntimeException
{
    /**
     * @param list<array{field: string, requested: mixed, effective: mixed}> $differences
     */
    public function __construct(
        public readonly string $durable,
        public readonly array $differences,
    ) {
        $detail = implode("\n", array_map(
            static fn (array $d): string => sprintf(
                '  %s: solicitado %s, efectivo %s',
                $d['field'],
                json_encode($d['requested']),
                json_encode($d['effective']),
            ),
            $differences,
        ));

        parent::__construct(
            "el servidor devolvió una configuración distinta de la solicitada para \"{$durable}\":\n"
            . $detail . "\n"
            . 'JetStream sobrescribe algunos campos en silencio; el más caro es ack_wait, que '
            . 'reemplaza por backoff[0] (03-delivery.md §2.1). Si el desajuste es en ack_wait, '
            . 'revisa que backoff[0] sea el presupuesto de duración de tu handler.'
        );
    }
}
