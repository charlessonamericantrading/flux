<?php

declare(strict_types=1);

namespace Flux;

/**
 * La llamada o el registro de una operación está mal formado y se sabe **sin red** (11-request-reply.md §6): la operación no
 * cumple el patrón, `timeoutMs` no es un entero entre 1 y 25 000, `data` no es un objeto — o un constructor del sobre recibe un atributo
 * vacío, un `time` que no es un instante o un error que no cumple su esquema. PERMANENT.
 */
final class InvalidRpcCallException extends CodedError
{
    public function __construct(string $message, ?\Throwable $previous = null)
    {
        parent::__construct('INVALID_RPC_CALL', ErrorClass::Permanent, $message, $previous);
    }
}
