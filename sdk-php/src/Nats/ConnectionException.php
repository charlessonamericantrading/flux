<?php

declare(strict_types=1);

namespace Flux\Nats;

/** La conexión con el broker se perdió o no se pudo abrir (nada que ver con credenciales ni con TLS). Es transitoria. */
class ConnectionException extends \RuntimeException
{
}
