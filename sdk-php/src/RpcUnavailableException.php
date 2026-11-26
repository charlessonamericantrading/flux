<?php

declare(strict_types=1);

namespace Flux;

/**
 * Nadie escucha la operación: el broker contestó `no responders` (estado 503) en milisegundos, sin esperar al timeout
 * (11-request-reply.md §4.3). RETRYABLE, inmediato.
 */
final class RpcUnavailableException extends CodedError
{
    public function __construct(public readonly string $operation, ?\Throwable $previous = null)
    {
        parent::__construct(
            'RPC_UNAVAILABLE',
            ErrorClass::Retryable,
            "nadie atiende la operación \"{$operation}\": el broker contestó `no responders` al instante. O el servicio dueño está "
            . 'caído o parado, o no la atiende esta versión (la suscripción es al subject EXACTO de la operación), o falta el permiso '
            . 'de suscripción de su dueño (06-security.md §3.9). Es reintentable: reintenta con el mismo `id`.',
            $previous,
        );
    }
}
