<?php

declare(strict_types=1);

namespace Flux;

/**
 * La aplicación lanza esto para ir directo a la DLQ sin gastar reintentos.
 * Úsalo cuando el evento es válido pero tu lógica lo rechaza definitivamente —
 * 04-errors.md §1.2.
 *
 * ```php
 * throw new PermanentError('pedido ya cancelado', errorCode: 'PEDIDO_YA_CANCELADO');
 * ```
 *
 * Reintentar aquí es puro desperdicio: 51 minutos de cola bloqueada para llegar al mismo
 * sitio.
 */
final class PermanentError extends FluxError
{
    public function fluxClass(): ErrorClass
    {
        return ErrorClass::Permanent;
    }
}
