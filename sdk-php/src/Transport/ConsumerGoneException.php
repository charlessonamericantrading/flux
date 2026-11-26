<?php

declare(strict_types=1);

namespace Flux\Transport;

/**
 * El broker contestó a un pull que el consumidor ya no existe: `409 Consumer Deleted` si se borró con el pull en vuelo, o `503 no responders`
 * si se borró antes de pedir. No es «no hay mensajes» (que es un `404`/`408` y el estado normal de un consumidor ocioso): un consumidor
 * que ya no está **no entrega nunca más**, y tratarlo como ocioso lo dejaría parado, con la conexión sana y sin ninguna señal.
 *
 * Es lo que ocurre con `flux state resync` (borra y recrea el durable) y con un durable que alguien borró.
 */
final class ConsumerGoneException extends \RuntimeException
{
    public function __construct(public readonly string $stream, public readonly string $durable, public readonly int $status, string $statusText)
    {
        parent::__construct("el consumidor \"{$durable}\" del stream \"{$stream}\" ya no existe en el broker (el pull contestó {$status} {$statusText}): se borró, quizá con `flux state resync`");
    }
}
