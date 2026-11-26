<?php

declare(strict_types=1);

namespace Flux\Nats;

use Psr\Log\LoggerInterface;

/** Lo que la conexión necesita saber. Lo construye `SocketTransport::connect` a partir de `ConnectOptions`. */
final readonly class ConnectionOptions
{
    /**
     * @param list<string> $servers `nats://host:4222`, `tls://host:4222` o `host:4222`
     * @param string $inboxPrefix `_INBOX.<servicio>`: PRIVADO y no configurable por quien usa el SDK (06-security.md §3.3)
     * @param int|null $waitForBrokerMs cuánto esperar a un broker que aún no ha arrancado (`null` = sin límite). Solo cubre «aún no
     *        está» (conexión rechazada, DNS, timeout): las credenciales rechazadas o un TLS que no encaja fallan **al instante**
     */
    public function __construct(
        public array $servers,
        public string $name,
        public string $inboxPrefix,
        public ?Credentials $credentials = null,
        public ?TlsOptions $tls = null,
        public int $connectTimeoutMs = 2_000,
        public ?int $waitForBrokerMs = null,
        public int $reconnectWaitMs = 1_000,
        public int $reconnectJitterMs = 500,
        public ?LoggerInterface $logger = null,
        public int $maxQueued = 65_536,
    ) {
    }
}
