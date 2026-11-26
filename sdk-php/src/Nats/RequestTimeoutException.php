<?php

declare(strict_types=1);

namespace Flux\Nats;

/** Una petición no obtuvo respuesta en su plazo. Por su nombre, el clasificador de errores la trata como un timeout. */
final class RequestTimeoutException extends ConnectionException
{
}
