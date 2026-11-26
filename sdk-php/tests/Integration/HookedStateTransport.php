<?php

declare(strict_types=1);

namespace Flux\Tests\Integration;

use Flux\Transport\StateTransport;

/**
 * Envuelve el transporte real para MEDIR lo que el SDK hace por el cable (cuántas lecturas y cuántas publicaciones por paso) y para inyectar
 * un fallo en un punto exacto: un rival que escribe entre la lectura y la publicación de un intento, el acuse que se pierde, una escritura
 * que llega al broker mientras `watch` ya creó su consumidor. Ninguno es un asidero dentro del SDK de producción: es un transporte más.
 */
final class HookedStateTransport implements StateTransport
{
    /** Publicaciones con acuse hechas desde el último `resetCounters()`. */
    public int $publications = 0;
    /** Lecturas `DIRECT.GET`, una por intento. */
    public int $reads = 0;
    /** @var (\Closure(int):void)|null Se ejecuta ANTES de la publicación `n` (1-based). */
    public ?\Closure $beforePublish = null;
    /** La primera publicación SE APLICA pero quien la hizo ve «wrong last sequence», como tras un reenvío de la librería. */
    public bool $loseAckOnce = false;
    /** @var (\Closure():void)|null Justo después de crear/leer el consumidor de un `watch`, con su `num_pending` ya leído. */
    public ?\Closure $afterEnsureConsumer = null;

    public function __construct(private readonly StateTransport $inner)
    {
    }

    public function resetCounters(): void
    {
        $this->publications = 0;
        $this->reads = 0;
    }

    public function publishAck(string $subject, string $payload, array $headers = [], int $timeoutMs = 5_000): array
    {
        $n = ++$this->publications;
        if ($this->beforePublish !== null) {
            ($this->beforePublish)($n);
        }
        $ack = $this->inner->publishAck($subject, $payload, $headers, $timeoutMs);
        if ($this->loseAckOnce && $n === 1) {
            return ['error' => ['code' => 400, 'err_code' => 10071, 'description' => 'wrong last sequence: (simulado)']];
        }

        return $ack;
    }

    public function directGet(string $stream, string $subject, int $timeoutMs = 5_000): ?array
    {
        $this->reads++;

        return $this->inner->directGet($stream, $subject, $timeoutMs);
    }

    public function ensureConsumer(string $stream, string $durable, array $config): array
    {
        $r = $this->inner->ensureConsumer($stream, $durable, $config);
        if ($this->afterEnsureConsumer !== null) {
            ($this->afterEnsureConsumer)();
        }

        return $r;
    }

    public function consumerInfo(string $stream, string $durable): ?array
    {
        return $this->inner->consumerInfo($stream, $durable);
    }

    public function streamConfig(string $name): ?array
    {
        return $this->inner->streamConfig($name);
    }

    public function isConnected(): bool
    {
        return $this->inner->isConnected();
    }

    public function ensureStream(string $name, array $subjects, int $maxAgeMs, ?int $duplicateWindowMs = null): void
    {
        $this->inner->ensureStream($name, $subjects, $maxAgeMs, $duplicateWindowMs);
    }

    public function createDurableConsumer(string $stream, string $durable, array $config): array
    {
        return $this->inner->createDurableConsumer($stream, $durable, $config);
    }

    public function consumerPending(string $stream, string $durable): ?int
    {
        return $this->inner->consumerPending($stream, $durable);
    }

    public function fetch(string $stream, string $durable, int $batch, int $timeoutMs): array
    {
        return $this->inner->fetch($stream, $durable, $batch, $timeoutMs);
    }

    public function publish(string $subject, string $payload, array $headers = []): void
    {
        $this->inner->publish($subject, $payload, $headers);
    }

    public function respond(string $replyTo, string $payload): void
    {
        $this->inner->respond($replyTo, $payload);
    }

    public function assertPullAllowed(string $stream, string $durable): void
    {
        $this->inner->assertPullAllowed($stream, $durable);
    }

    public function close(): void
    {
        $this->inner->close();
    }
}
