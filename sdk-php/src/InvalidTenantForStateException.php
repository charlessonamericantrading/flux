<?php

declare(strict_types=1);

namespace Flux;

/**
 * El tenant no cumple `^[a-z0-9]+(-[a-z0-9]+)*$` (64 como máximo), o no se puede resolver ninguno: el estado **no** cae en
 * `system` por defecto (10-state.md §2.4). PERMANENT.
 */
final class InvalidTenantForStateException extends CodedError
{
    public function __construct(mixed $tenant, ?\Throwable $previous = null)
    {
        $shown = $tenant === null ? 'ausente' : (is_string($tenant) ? json_encode(Text::truncateCodePoints($tenant, 70), JSON_UNESCAPED_UNICODE) : get_debug_type($tenant));
        parent::__construct(
            'INVALID_TENANT_FOR_STATE',
            ErrorClass::Permanent,
            "tenant inválido para el estado ({$shown}): debe cumplir ^[a-z0-9]+(-[a-z0-9]+)*\$ con 64 caracteres como máximo. El estado "
            . 'NO cae en `system` por defecto: el tenant va en el subject y uno equivocado es un dato guardado en otro sitio. Pásalo en '
            . '`tenantId` de la llamada o en el `tenantId` de ConnectOptions (10-state.md §2.4).',
            $previous,
        );
    }
}
