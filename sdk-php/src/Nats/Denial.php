<?php

declare(strict_types=1);

namespace Flux\Nats;

/**
 * Una denegación de permisos que el broker avisó **aparte**, por la conexión: `-ERR 'Permissions Violation for Publish to "x"'`.
 *
 * Cuando una ACL deniega una operación, el broker no contesta: descarta el mensaje y manda este aviso (A-28). No es la respuesta de
 * ninguna petición, y por eso hay que escuchar la conexión y competirlo con la respuesta (06-security.md §3.7).
 */
final readonly class Denial
{
    public function __construct(
        /** `publish` | `subscribe` */
        public string $operation,
        /** El subject denegado, completo y sin normalizar. */
        public string $subject,
        /** El grupo de cola de una suscripción denegada, si lo hubo. */
        public ?string $queue,
        public int $sequence,
    ) {
    }
}
