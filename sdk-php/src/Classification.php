<?php

declare(strict_types=1);

namespace Flux;

/**
 * Resultado de clasificar un error. Es lo que consume el runtime del consumidor.
 * Contrato normativo: specification/04-errors.md §2
 */
final readonly class Classification
{
    public function __construct(
        /**
         * Se llama `errorClass` y no `class` porque `$c->class` convive mal con
         * `$c::class` (PHP 8 devuelve ahí el nombre de la clase del objeto) y confundir
         * ambos es un error silencioso. Es el único renombrado respecto al SDK de Node,
         * igual que `error_class` en el de Python.
         */
        public ErrorClass $errorClass,
        /** Código estable para métricas y alertas. Ej. `HTTP_503`, `PEDIDO_YA_CANCELADO`. */
        public string $code,
        /**
         * Solo para RETRYABLE: sobrescribe el backoff canónico para ESTE intento.
         * Úsalo cuando la dependencia dice explícitamente cuánto esperar (`Retry-After`).
         */
        public ?float $retryAfterMs = null,
        /**
         * Solo para RETRYABLE: entregas máximas para ESTE error, por debajo del
         * `max_deliver` del consumidor. `null` = sin tope propio, manda el del consumidor.
         *
         * Existe porque `max_deliver` es por CONSUMIDOR, no por mensaje: bajarlo a 2 para
         * acotar los errores desconocidos recortaría también los reintentos de los que sí
         * sabemos transitorios (ECONNRESET, HTTP 503), que deben conservar sus 6 intentos.
         * El runtime aplica `min(maxDeliver, maxAttempts)` — 04-errors.md §2.1.
         */
        public ?int $maxAttempts = null,
    ) {
    }
}
