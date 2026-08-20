<?php

declare(strict_types=1);

namespace Flux\Transport;

/**
 * Un mensaje tal y como llega del broker, antes de intentar interpretarlo como CloudEvent.
 *
 * Es un tipo de **flux**, no de NATS: la regla de diseño del SDK es que ningún tipo del
 * cliente de NATS aparezca en la API pública, porque si lo hiciera, sustituir el broker
 * dejaría de ser un cambio de capa 0-1 y pasaría a tocar las aplicaciones — que es
 * exactamente lo que flux existe para evitar.
 */
final readonly class RawMessage
{
    /**
     * @param string      $subject Subject de NATS del que llegó.
     * @param string      $payload Cuerpo crudo. Puede no ser JSON — eso es POISON, y se
     *                             decide más arriba.
     * @param string|null $replyTo Subject de respuesta `$JS.ACK.…`. Es a la vez el canal
     *                             para ack/nak/term/WIP y la ÚNICA fuente del número de
     *                             entrega. Sin él no se puede acusar recibo del mensaje.
     * @param array<string,string> $headers
     */
    public function __construct(
        public string $subject,
        public string $payload,
        public ?string $replyTo = null,
        public array $headers = [],
    ) {
    }
}
