<?php

declare(strict_types=1);

namespace Flux;

/**
 * Base de los errores que ya declaran su propia clase de protocolo.
 * Contrato normativo: specification/04-errors.md
 *
 * El código estable se llama `$errorCode` y no `$code` porque `\Exception::$code` ya
 * existe, es `protected` y es `int`: redeclararlo como `?string` sería un error fatal de
 * compilación. `getCode()` sigue devolviendo `0`; el que interesa para métricas y para
 * la extensión `dlqerror` es `$errorCode`.
 */
abstract class FluxError extends \RuntimeException
{
    public function __construct(
        string $message,
        public readonly ?string $errorCode = null,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, 0, $previous);
    }

    /** La clase de protocolo que esta excepción declara. */
    abstract public function fluxClass(): ErrorClass;
}
