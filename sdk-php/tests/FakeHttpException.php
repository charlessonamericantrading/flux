<?php

declare(strict_types=1);

namespace Flux\Tests;

/**
 * Excepción con la forma de las de Guzzle / PSR-18: el status vive en
 * `getResponse()->getStatusCode()` y las cabeceras en `getHeaderLine()`.
 *
 * Se usa una doble propia y no un mock para dejar explícito **qué forma** de excepción
 * reconoce el clasificador. Con un mock, el contrato quedaría escondido en una cadena de
 * `willReturn`.
 */
final class FakeHttpException extends \RuntimeException
{
    public string $tag = '';

    /** @param array<string,string> $headers */
    public function __construct(
        private readonly int $status,
        private readonly array $headers = [],
    ) {
        parent::__construct("respuesta HTTP {$status}");
    }

    public function getResponse(): object
    {
        return new FakeHttpResponse($this->status, $this->headers);
    }
}
