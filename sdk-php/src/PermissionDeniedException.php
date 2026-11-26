<?php

declare(strict_types=1);

namespace Flux;

/**
 * El broker denegó una operación por permisos (`Permissions Violation`). PERMANENT, como un HTTP 403: reintentar una denegación da la
 * misma denegación (04-errors.md §5). Nombra el subject denegado, completo y sin normalizar.
 */
final class PermissionDeniedException extends CodedError
{
    /** @param 'publish'|'subscribe' $operation */
    public function __construct(
        public readonly string $service,
        public readonly string $operation,
        public readonly string $subject,
        ?string $hint = null,
        ?\Throwable $previous = null,
    ) {
        parent::__construct(
            'PERMISSION_DENIED',
            ErrorClass::Permanent,
            'el broker denegó ' . ($operation === 'publish' ? 'publicar en' : 'suscribirse a') . " \"{$subject}\" a \"{$service}\": su ACL "
            . 'no concede ese permiso. '
            . ($hint !== null
                ? $hint . ' '
                : (str_starts_with($subject, '$JS.')
                    ? 'Es un permiso de JetStream, y se concede exacto por durable: una ACL generada antes de la v1.1 (que solo habla de '
                    . 'subjects de dominio) no los incluye. Ver «Seguridad» en sdk-php/README.md (06-security.md §3).'
                    : 'Ver 06-security.md §3.')),
            $previous,
        );
    }
}
