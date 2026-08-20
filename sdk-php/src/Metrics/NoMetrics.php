<?php

declare(strict_types=1);

namespace Flux\Metrics;

use Flux\ErrorClass;

/**
 * No-op. **El default**: un SDK no debe imponer un backend de métricas.
 *
 * Existe como clase y no como `?MetricsSink $metrics = null` con comprobaciones repartidas
 * por `FluxBus` porque un `null` obliga a poner `?->` en cada punto de instrumentación, y
 * el día que alguien olvide uno la métrica desaparece en silencio.
 */
final class NoMetrics implements MetricsSink
{
    public function eventPublished(string $subject, PublishOutcome $outcome): void
    {
    }

    public function eventConsumed(string $subject, string $consumer, ConsumeOutcome $outcome): void
    {
    }

    public function handlerDuration(string $subject, string $consumer, float $seconds): void
    {
    }

    public function eventDlq(string $subject, string $consumer, ErrorClass $reason, string $code): void
    {
    }

    public function eventRetried(string $subject, string $consumer, int $attempt): void
    {
    }

    public function consumerPending(string $subject, string $consumer, int $pending): void
    {
    }

    public function connectionState(ConnectionState $state): void
    {
    }
}
