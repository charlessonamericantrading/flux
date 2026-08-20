<?php

declare(strict_types=1);

namespace Flux\Tests;

/** Respuesta con la forma de PSR-7: `getStatusCode()` + `getHeaderLine()`. */
final class FakeHttpResponse
{
    /** @param array<string,string> $headers */
    public function __construct(
        private readonly int $status,
        private readonly array $headers = [],
    ) {
    }

    public function getStatusCode(): int
    {
        return $this->status;
    }

    public function getHeaderLine(string $name): string
    {
        foreach ($this->headers as $key => $value) {
            if (strcasecmp($key, $name) === 0) {
                return $value;
            }
        }

        return '';
    }
}
