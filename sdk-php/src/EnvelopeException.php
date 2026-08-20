<?php

declare(strict_types=1);

namespace Flux;

/**
 * Error al CONSTRUIR o serializar un envelope: `data` que no es un objeto, `time` con un
 * formato que no cumple 01-envelope.md §2.2, o un mensaje por encima de 1 MiB.
 *
 * Es distinto de `PoisonError` a propósito: esto es un bug del productor, se detecta
 * antes de tocar el cable y no tiene nada que ver con la DLQ. Un POISON es un mensaje
 * ajeno ilegible, ya en el stream.
 */
final class EnvelopeException extends \InvalidArgumentException
{
}
