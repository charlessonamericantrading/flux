<?php

declare(strict_types=1);

namespace Flux\Tests;

/** Excepción que expone un errno de socket, como hacen los wrappers de `ext-sockets`. */
final class FakeSocketException extends \RuntimeException
{
    public function __construct(private readonly int $errno)
    {
        parent::__construct("error de socket {$errno}");
    }

    public function getErrno(): int
    {
        return $this->errno;
    }
}
