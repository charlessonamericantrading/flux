<?php

declare(strict_types=1);

namespace Flux;

/**
 * La aplicación lanza esto para forzar reintento. Úsalo cuando SABES que el fallo es
 * transitorio — 04-errors.md §1.1.
 *
 * ```php
 * throw new RetryableError('proveedor de pagos 503', retryAfterMs: 5000);
 * ```
 *
 * Conserva el presupuesto completo del consumidor (6 entregas, hasta ~51 min): declarar
 * un error como transitorio es afirmar que el mundo puede arreglarlo solo, y ahí el
 * presupuesto acotado de lo desconocido no aplica.
 */
final class RetryableError extends FluxError
{
    public function __construct(
        string $message,
        ?string $errorCode = null,
        /**
         * Sobrescribe el backoff canónico para este intento. Úsalo cuando la dependencia
         * dice explícitamente cuánto esperar; si no, mejor `null` y que mande el backoff
         * del protocolo.
         */
        public readonly ?float $retryAfterMs = null,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, $errorCode, $previous);
    }

    public function fluxClass(): ErrorClass
    {
        return ErrorClass::Retryable;
    }
}
