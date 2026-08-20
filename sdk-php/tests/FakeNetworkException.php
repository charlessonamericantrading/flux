<?php

declare(strict_types=1);

namespace Flux\Tests;

/**
 * Hace de marcador semántico de red, como `Psr\Http\Client\NetworkExceptionInterface`.
 *
 * No se usa el interfaz real de PSR-18 para que la suite no dependa de tenerlo instalado:
 * lo que se prueba es el **mecanismo** (reconocer por tipo, no por texto), no el nombre
 * concreto de la clase.
 */
final class FakeNetworkException extends \RuntimeException
{
}
