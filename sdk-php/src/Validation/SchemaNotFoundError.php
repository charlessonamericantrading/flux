<?php

declare(strict_types=1);

namespace Flux\Validation;

use Flux\ErrorClass;
use Flux\FluxError;

/**
 * El `dataschema` del evento no está en el bundle desplegado.
 *
 * En modo `Strict` es un error y no un aviso a propósito. La alternativa —dejar pasar lo
 * que no se puede comprobar— convierte el nivel L3 en una promesa condicional: un subject
 * cuyo esquema alguien olvidó empaquetar quedaría **sin validar y sin decirlo**, que es
 * exactamente el estado que L3 existe para eliminar. Un bundle incompleto es un fallo de
 * despliegue, y se descubre al arrancar o al primer evento, no en una auditoría.
 *
 * En modo `Warn` solo se registra: durante una migración es normal que falten esquemas.
 */
final class SchemaNotFoundError extends FluxError
{
    /** Código estable para métricas — distinto de `INVALID_SCHEMA`: son dos causas distintas. */
    public const CODE = 'SCHEMA_NOT_FOUND';

    public function __construct(
        public readonly string $subject,
        public readonly string $dataschema,
        ?\Throwable $previous = null,
    ) {
        parent::__construct(
            "no hay esquema para \"{$subject}\" ({$dataschema}) en el bundle. "
            . 'Regenéralo con `node scripts/bundle-schemas.mjs` y despliégalo con el servicio, '
            . 'o baja validation.mode a "warn" mientras tanto.',
            self::CODE,
            $previous,
        );
    }

    /**
     * PERMANENT, igual que un payload inválido: reintentar no va a hacer aparecer un
     * esquema que no se empaquetó. Lo que falta es un despliegue, no tiempo.
     */
    public function fluxClass(): ErrorClass
    {
        return ErrorClass::Permanent;
    }
}
