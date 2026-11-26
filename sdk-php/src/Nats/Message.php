<?php

declare(strict_types=1);

namespace Flux\Nats;

/**
 * Un mensaje de NATS (`MSG` o `HMSG`).
 *
 * `status` es el código de la línea de estado de las cabeceras (`NATS/1.0 503`): `503` es `no responders`, `404` «no hay mensaje»,
 * `408` «el pull expiró», `409` «consumidor borrado» y `400` una petición mal formada. Un mensaje con estado y sin cuerpo **no es un
 * dato**: es el broker diciendo algo.
 */
final readonly class Message
{
    public function __construct(
        public string $subject,
        public string $payload,
        public ?string $reply = null,
        /** @var array<string,string> */
        public array $headers = [],
        public ?int $status = null,
        public string $statusText = '',
    ) {
    }

    public function header(string $name): ?string
    {
        foreach ($this->headers as $k => $v) {
            if (strcasecmp($k, $name) === 0) {
                return $v;
            }
        }

        return null;
    }

    /** `no responders`: el broker contestó 503 sin cuerpo porque nadie escucha el subject. */
    public function isNoResponders(): bool
    {
        return $this->status === 503;
    }
}
