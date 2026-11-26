<?php

declare(strict_types=1);

namespace Flux\Nats;

/** El broker contestó `no responders` (estado 503): nadie escucha el subject. Con JetStream, un stream que no existe se ve así. */
final class NoRespondersException extends ConnectionException
{
    public function __construct(public readonly string $subject, ?\Throwable $previous = null)
    {
        parent::__construct("nadie escucha \"{$subject}\" (el broker contestó `no responders`)", 0, $previous);
    }
}
