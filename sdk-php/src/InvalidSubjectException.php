<?php

declare(strict_types=1);

namespace Flux;

/**
 * Subject de NATS que no cumple 02-naming.md §1.
 *
 * Falla en `publish()`/`subscribe()` a propósito: es la última red de seguridad antes
 * de que una errata cree un subject al que nadie está suscrito.
 */
final class InvalidSubjectException extends \InvalidArgumentException
{
    public function __construct(
        public readonly string $subject,
        public readonly string $reason,
    ) {
        parent::__construct("subject inválido \"{$subject}\": {$reason}");
    }
}
