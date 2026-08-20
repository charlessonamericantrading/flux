<?php

declare(strict_types=1);

namespace Flux\Tests;

/**
 * Error de base de datos con la forma de PDO: `errorInfo[0]` es el SQLSTATE.
 *
 * No extiende `\PDOException` a propósito, para que la suite no necesite `ext-pdo`
 * cargada — y porque el clasificador reconoce la CONVENCIÓN (`errorInfo` / `getSQLState`),
 * no el tipo concreto. Así también funciona con las excepciones de Doctrine DBAL.
 */
final class FakePdoException extends \RuntimeException
{
    /** @var array{0: string, 1: ?int, 2: ?string} */
    public array $errorInfo;

    public static function withState(string $sqlstate): self
    {
        $e = new self("fallo de base de datos ({$sqlstate})");
        $e->errorInfo = [$sqlstate, null, null];

        return $e;
    }
}
