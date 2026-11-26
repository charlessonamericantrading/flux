<?php

declare(strict_types=1);

namespace Flux;

/**
 * Un error de flux con un **código estable** y una clase (04-errors.md §5, §6, §7).
 *
 * El código es fijo y la aplicación no puede sustituirlo: es lo que agrupa el panel (`code` de
 * 08-observability.md §2.2) y lo que ramifica la aplicación. El mensaje nombra **qué** está mal y
 * **cómo se arregla**, y **nunca** lleva credenciales ni valores de `data` (acaba en logs y en
 * `dlqerror`).
 *
 * La clase decide qué hace un handler que lo deja escapar: `PERMANENT` va a la DLQ en la primera
 * entrega; `RETRYABLE` se reentrega con su backoff; `POISON` además despierta a alguien.
 */
abstract class CodedError extends FluxError
{
    public function __construct(
        string $code,
        private readonly ErrorClass $class,
        string $message,
        ?\Throwable $previous = null,
        /** Solo `RETRYABLE`: una sugerencia para el PRIMER reintento (04-errors.md §2.2). */
        public readonly ?float $retryAfterMs = null,
    ) {
        parent::__construct($message, $code, $previous);
    }

    public function fluxClass(): ErrorClass
    {
        return $this->class;
    }
}
