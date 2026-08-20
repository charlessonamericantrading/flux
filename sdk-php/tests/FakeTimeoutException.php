<?php

declare(strict_types=1);

namespace Flux\Tests;

/**
 * PHP no tiene una excepción de timeout estándar: cada librería trae la suya y no
 * comparten jerarquía. Por eso el clasificador mira el nombre corto de la CLASE —API
 * pública y estable— y nunca el mensaje.
 */
final class FakeTimeoutException extends \RuntimeException
{
}
