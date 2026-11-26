<?php

declare(strict_types=1);

namespace Flux;

/**
 * Lo que llegó no es la respuesta a esta petición (11-request-reply.md §4.3): no es un CloudEvent válido, su `type` no es el de
 * la respuesta ni el del error, su `causationid` no es el `id` de la petición, o no hereda su `correlationid` ni su `tenantid`.
 * PERMANENT: es un incidente (un productor roto, un stream mal puesto), no un fallo que esperar.
 */
final class RpcInvalidReplyException extends CodedError
{
    public function __construct(public readonly string $operation, string $why, bool $looksLikeStreamAck = false, ?\Throwable $previous = null)
    {
        parent::__construct(
            'RPC_INVALID_REPLY',
            ErrorClass::Permanent,
            "lo que llegó como respuesta a \"{$operation}\" no es la respuesta a esta petición: {$why}."
            . ($looksLikeStreamAck
                ? ' El cuerpo parece el ACUSE de un stream de JetStream: algún stream captura `rpc.>` y contesta a cada petición con su '
                . 'acuse (y persiste los datos de la petición). Audítalo con `flux doctor` (11-request-reply.md §8).'
                : ' Es un incidente —un productor roto, un stream que captura `rpc.>`, algo hostil—, no un fallo que se arregle '
                . 'reintentando (11-request-reply.md §4.3).'),
            $previous,
        );
    }
}
