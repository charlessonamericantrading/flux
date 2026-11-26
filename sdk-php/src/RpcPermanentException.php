<?php

declare(strict_types=1);

namespace Flux;

/**
 * El error que **devolvió quien responde** (`com.flux.rpc.error.v1`, 11-request-reply.md §3.3), con su `code` y su clase
 * (PERMANENT): un handler de eventos que llama a una operación hereda la clasificación sin hacer nada.
 */
final class RpcPermanentException extends CodedError
{
    public function __construct(public readonly string $operation, string $code, string $message, ?\Throwable $previous = null)
    {
        parent::__construct($code, ErrorClass::Permanent, $message, $previous, null);
    }
}
