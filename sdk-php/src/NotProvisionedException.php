<?php

declare(strict_types=1);

namespace Flux;

/**
 * Falta un stream o un consumidor y este servicio no tiene permiso para crearlo (04-errors.md §5, 06-security.md §3.6). PERMANENT.
 * En producción los streams y los consumidores se aprovisionan **fuera** del SDK, con `flux provision` y credenciales de operación.
 */
final class NotProvisionedException extends CodedError
{
    /** @param 'stream'|'consumer' $kind */
    public function __construct(
        public readonly string $kind,
        public readonly string $resource,
        public readonly ?string $stream,
        string $service,
        public readonly string $deniedSubject,
        ?string $detail = null,
        ?\Throwable $previous = null,
    ) {
        $que = $kind === 'stream' ? "el stream \"{$resource}\"" : "el consumidor \"{$resource}\" del stream \"{$stream}\"";
        parent::__construct(
            'NOT_PROVISIONED',
            ErrorClass::Permanent,
            $detail ?? "{$que} no existe y \"{$service}\" no tiene permiso para crearlo (el broker denegó publicar en \"{$deniedSubject}\"). "
            . 'En producción se aprovisiona con `flux provision <services.json>`, usando credenciales de operación, no las del servicio '
            . '(06-security.md §3.6).',
            $previous,
        );
    }
}
