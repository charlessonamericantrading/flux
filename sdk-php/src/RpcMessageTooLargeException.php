<?php

declare(strict_types=1);

namespace Flux;

/**
 * El mensaje serializado no cabe en el mínimo entre 1 MiB y el `max_payload` del broker (11-request-reply.md §4.4, §5.4).
 * PERMANENT.
 */
final class RpcMessageTooLargeException extends CodedError
{
    public function __construct(public readonly int $bytes, public readonly int $limit, string $what, string $operation, ?\Throwable $previous = null)
    {
        parent::__construct(
            'RPC_MESSAGE_TOO_LARGE',
            ErrorClass::Permanent,
            "la {$what} de \"{$operation}\" ocupa {$bytes} bytes y el límite es {$limit} (el mínimo entre 1 MiB y el `max_payload` del "
            . 'broker, cabeceras incluidas). Aplica claim-check —sube el contenido a un almacenamiento de objetos y envía '
            . '`{ uri, sha256, bytes }`—, o pagina con un cursor en `data` (01-envelope.md §7).',
            $previous,
        );
    }
}
