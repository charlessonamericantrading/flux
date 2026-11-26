<?php

declare(strict_types=1);

namespace Flux;

/**
 * El broker **contestó** y rechazó la primera conexión por algo que esperar no arregla (04-errors.md §5.3): `auth` —rechazó las
 * credenciales— o `tls` —el certificado no verifica, el servidor no ofrece TLS o exige uno de cliente—. PERMANENT.
 */
final class ConnectionRejectedException extends CodedError
{
    /** @param 'auth'|'tls' $reason */
    public function __construct(public readonly string $reason, string $message, ?\Throwable $previous = null)
    {
        parent::__construct('CONNECTION_REJECTED', ErrorClass::Permanent, $message, $previous);
    }
}
