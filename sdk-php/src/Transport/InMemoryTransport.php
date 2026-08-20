<?php

declare(strict_types=1);

namespace Flux\Transport;

/**
 * Transporte en memoria. Sirve para dos cosas:
 *
 * - Los tests del SDK, que ejercen todo el runtime del consumidor —clasificación,
 *   presupuesto de reintentos, enrutado a la DLQ, la regla de "si la DLQ falla NO hagas
 *   term"— sin broker y sin la librería de NATS instalada.
 * - Los tests de TU servicio: inyéctalo en `FluxBus::connect()` y comprueba qué eventos
 *   publica tu handler sin levantar nada.
 *
 * No pretende emular JetStream. Emula lo justo: el subject de respuesta con el número de
 * entrega, la config efectiva del consumidor y el registro de lo publicado.
 */
final class InMemoryTransport implements NatsTransport
{
    /** @var array<string, array{subjects: list<string>, maxAgeMs: int, duplicateWindowMs: ?int}> */
    public array $streams = [];

    /** @var list<array{subject: string, payload: string, headers: array<string,string>}> */
    public array $published = [];

    /** @var list<array{replyTo: string, payload: string}> */
    public array $acks = [];

    /** @var array<string, array<string,mixed>> */
    public array $consumers = [];

    /** @var list<RawMessage> */
    private array $pending = [];

    private bool $connected = true;

    /**
     * Subjects cuyo `publish()` debe fallar. Sirve para el test de "si la publicación en
     * la DLQ falla, NO se hace term" — un bug real del SDK de Node.
     *
     * @var list<string>
     */
    public array $failPublishOn = [];

    /**
     * Config que devolverá `createDurableConsumer` en lugar de la solicitada. Reproduce la
     * trampa de 03-delivery.md §2.1: el servidor sobrescribe `ack_wait` con `backoff[0]`
     * sin avisar y devuelve algo distinto de lo enviado.
     *
     * @var array<string,mixed>|null
     */
    public ?array $consumerConfigOverride = null;

    /**
     * Si es `true`, `createDurableConsumer` devuelve `consumerConfigOverride` **tal cual**
     * en vez de fusionarlo sobre la config solicitada.
     *
     * Sirve para reproducir un servidor que no reporta todos los campos: sin esto, un
     * override de `[]` se fusiona y devuelve la config completa, así que la rama de
     * `assertConfigHonored()` que ignora los campos ausentes nunca se ejercitaría y el test
     * pasaría sin probar nada.
     */
    public bool $replaceConsumerConfig = false;

    /**
     * Tokens de acuse cuyo envío debe fallar. Sirve para el test de "un fallo inesperado
     * del despacho no mata el bucle de consumo".
     *
     * @var list<string>
     */
    public array $failRespondTokens = [];

    /**
     * `num_pending` que devolverá el sondeo, por durable. Un durable sin entrada devuelve
     * `null` — "no lo sabemos", que no es lo mismo que cero.
     *
     * @var array<string,int|null>
     */
    public array $pendingByDurable = [];

    /**
     * Cuántas veces se ha sondeado, por durable. Sin esto, un test no puede distinguir
     * "el sondeo devolvió el valor correcto" de "el sondeo nunca llegó a ejecutarse y el
     * gauge lo puso el metadato del mensaje".
     *
     * @var array<string,int>
     */
    public array $pendingPolls = [];

    /**
     * Si es `true`, `consumerPending()` lanza. Sirve para el test de "un fallo del sondeo
     * NO afecta al consumo": es telemetría, no puede tumbar el worker.
     */
    public bool $failConsumerPending = false;

    public function isConnected(): bool
    {
        return $this->connected;
    }

    public function ensureStream(
        string $name,
        array $subjects,
        int $maxAgeMs,
        ?int $duplicateWindowMs = null,
    ): void {
        $this->streams[$name] ??= [
            'subjects' => $subjects,
            'maxAgeMs' => $maxAgeMs,
            'duplicateWindowMs' => $duplicateWindowMs,
        ];
    }

    public function createDurableConsumer(string $stream, string $durable, array $config): array
    {
        $this->consumers[$durable] = $config;

        if ($this->consumerConfigOverride === null) {
            return $config;
        }

        return $this->replaceConsumerConfig
            ? $this->consumerConfigOverride
            : array_merge($config, $this->consumerConfigOverride);
    }

    public function consumerPending(string $stream, string $durable): ?int
    {
        $this->pendingPolls[$durable] = ($this->pendingPolls[$durable] ?? 0) + 1;

        if ($this->failConsumerPending) {
            throw new \RuntimeException("sondeo de num_pending rechazado para {$durable} (simulado)");
        }

        return $this->pendingByDurable[$durable] ?? null;
    }

    public function fetch(string $stream, string $durable, int $batch, int $timeoutMs): array
    {
        return array_splice($this->pending, 0, $batch);
    }

    public function publish(string $subject, string $payload, array $headers = []): void
    {
        if (in_array($subject, $this->failPublishOn, true)) {
            throw new \RuntimeException("publicación rechazada en {$subject} (simulado)");
        }

        $this->published[] = ['subject' => $subject, 'payload' => $payload, 'headers' => $headers];
    }

    public function respond(string $replyTo, string $payload): void
    {
        if (in_array(explode(' ', $payload)[0], $this->failRespondTokens, true)) {
            throw new \RuntimeException("no se pudo enviar {$payload} (simulado)");
        }

        $this->acks[] = ['replyTo' => $replyTo, 'payload' => $payload];
    }

    public function close(): void
    {
        $this->connected = false;
    }

    // ─── Utilidades de test ───────────────────────────────────────────────────

    /** Encola un mensaje como si lo hubiese entregado JetStream en la entrega `$attempt`. */
    public function deliver(string $subject, string $payload, int $attempt = 1, string $consumer = 'c'): void
    {
        $this->pending[] = new RawMessage(
            subject: $subject,
            payload: $payload,
            // Formato v1 de 9 tokens: $JS.ACK.<stream>.<consumer>.<entregas>.<sseq>.<cseq>.<tm>.<pending>
            replyTo: '$JS.ACK.EVT_TEST.' . $consumer . '.' . $attempt . '.1.1.0.0',
        );
    }

    /** @return list<string> Los tokens de acuse emitidos, en orden. */
    public function ackTokens(): array
    {
        return array_map(
            static fn (array $ack): string => explode(' ', $ack['payload'])[0],
            $this->acks,
        );
    }

    /** @return list<array{subject: string, payload: string, headers: array<string,string>}> */
    public function publishedTo(string $subject): array
    {
        return array_values(array_filter(
            $this->published,
            static fn (array $message): bool => $message['subject'] === $subject,
        ));
    }
}
