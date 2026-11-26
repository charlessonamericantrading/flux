<?php

declare(strict_types=1);

namespace Flux;

/**
 * No llegó una respuesta en el timeout (11-request-reply.md §4.3). RETRYABLE y **ambiguo**: la operación PUEDE haberse
 * ejecutado (respondió tarde, o murió con la petición en la mano), de ahí que quien responde deduplique por `id`.
 */
final class RpcTimeoutException extends CodedError
{
    public function __construct(public readonly string $operation, public readonly int $timeoutMs, ?\Throwable $previous = null)
    {
        parent::__construct(
            'RPC_TIMEOUT',
            ErrorClass::Retryable,
            "no llegó respuesta a \"{$operation}\" en {$timeoutMs} ms. **La operación PUEDE haberse ejecutado**: un timeout no dice "
            . 'que no pasó nada (la respuesta pudo llegar tarde, o la instancia morir con la petición). Quien responde deduplica por '
            . 'el `id` de la petición: si reintentas, reutiliza el mismo (`retries`). Si el timeout es corto para esta operación, '
            . 'sube `timeoutMs` (hasta 25 000).',
            $previous,
        );
    }
}
