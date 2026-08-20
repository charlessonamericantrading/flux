<?php

declare(strict_types=1);

namespace Flux\Metrics;

use Flux\ErrorClass;

/**
 * Recolector **sin dependencias** que expone el formato de texto de Prometheus.
 *
 * Suficiente para servir un `/metrics` real. Si ya usas un cliente de Prometheus,
 * implementa `MetricsSink` contra él en vez de esto — lo que importa es conservar los
 * nombres y las etiquetas.
 *
 * ⚠️ **Vive en memoria del proceso.** En un worker CLI de larga vida (`$bus->run()`) eso es
 * exactamente lo que se quiere. Bajo FPM **no**: cada petición es un proceso nuevo y los
 * contadores nacen a cero, así que un `/metrics` servido desde FPM reportaría siempre casi
 * nada. Para publicar métricas de un publisher web hay que usar un backend con estado
 * compartido (APCu, Redis) implementando `MetricsSink` contra él — es la contrapartida del
 * modelo de ejecución de PHP, no una limitación de esta clase.
 */
final class InMemoryMetrics implements MetricsSink
{
    /** @var array<string,int> */
    private array $counters = [];

    /** @var array<string,float> */
    private array $gauges = [];

    /** @var array<string,array{buckets: list<int>, sum: float, count: int}> */
    private array $histograms = [];

    // ─── Registro ─────────────────────────────────────────────────────────────

    public function eventPublished(string $subject, PublishOutcome $outcome): void
    {
        $this->inc('flux_events_published_total', [
            'subject' => $subject,
            'outcome' => $outcome->value,
        ]);
    }

    public function eventConsumed(string $subject, string $consumer, ConsumeOutcome $outcome): void
    {
        $this->inc('flux_events_consumed_total', [
            'subject' => $subject,
            'consumer' => $consumer,
            'outcome' => $outcome->value,
        ]);
    }

    public function handlerDuration(string $subject, string $consumer, float $seconds): void
    {
        $this->observe(
            'flux_event_handler_duration_seconds',
            ['subject' => $subject, 'consumer' => $consumer],
            $seconds,
        );
    }

    public function eventDlq(string $subject, string $consumer, ErrorClass $reason, string $code): void
    {
        $this->inc('flux_events_dlq_total', [
            'subject' => $subject,
            'consumer' => $consumer,
            'reason' => $reason->value,
            'code' => $code,
        ]);
    }

    public function eventRetried(string $subject, string $consumer, int $attempt): void
    {
        $this->inc('flux_events_retried_total', [
            'subject' => $subject,
            'consumer' => $consumer,
            // Etiqueta de texto: `attempt="3"`, no `attempt=3`. En el formato de exposición
            // todas las etiquetas son cadenas.
            'attempt' => (string) $attempt,
        ]);
    }

    public function consumerPending(string $subject, string $consumer, int $pending): void
    {
        $this->set(
            'flux_consumer_pending',
            ['subject' => $subject, 'consumer' => $consumer],
            (float) $pending,
        );
    }

    public function connectionState(ConnectionState $state): void
    {
        $this->set('flux_connection_state', [], (float) $state->value);
    }

    // ─── Exposición ───────────────────────────────────────────────────────────

    /**
     * Formato de exposición de Prometheus. Sírvelo tal cual en `/metrics`.
     *
     * Los buckets son **acumulativos** (`le` = "less or equal"), que es lo que
     * `histogram_quantile` espera; emitir cuentas por bucket en vez de acumuladas daría
     * cuantiles silenciosamente equivocados — y "silenciosamente" es la parte cara.
     */
    public function render(): string
    {
        $out = [];

        foreach ([
            'flux_events_published_total',
            'flux_events_consumed_total',
            'flux_events_dlq_total',
            'flux_events_retried_total',
        ] as $name) {
            $out[] = "# TYPE {$name} counter";
        }
        ksort($this->counters);
        foreach ($this->counters as $key => $value) {
            $out[] = "{$key} {$value}";
        }

        foreach (['flux_consumer_pending', 'flux_connection_state'] as $name) {
            $out[] = "# TYPE {$name} gauge";
        }
        ksort($this->gauges);
        foreach ($this->gauges as $key => $value) {
            $out[] = "{$key} " . self::number($value);
        }

        $out[] = '# TYPE flux_event_handler_duration_seconds histogram';
        ksort($this->histograms);
        foreach ($this->histograms as $key => $h) {
            $brace = strpos($key, '{');
            $base = $brace === false ? $key : substr($key, 0, $brace);
            $labels = $brace === false ? '' : substr($key, $brace + 1, -1);
            $sep = $labels === '' ? '' : ',';

            foreach (MetricsSink::DURATION_BUCKETS as $i => $limit) {
                $out[] = "{$base}_bucket{{$labels}{$sep}le=\"" . self::number((float) $limit) . '"} '
                    . $h['buckets'][$i];
            }
            $out[] = "{$base}_bucket{{$labels}{$sep}le=\"+Inf\"} " . $h['count'];
            $out[] = "{$base}_sum{{$labels}} " . self::number($h['sum']);
            $out[] = "{$base}_count{{$labels}} " . $h['count'];
        }

        return implode("\n", $out) . "\n";
    }

    /** El valor de un contador por su clave completa. Solo para tests. */
    public function counter(string $key): ?int
    {
        return $this->counters[$key] ?? null;
    }

    /** El valor de un gauge por su clave completa. Solo para tests. */
    public function gauge(string $key): ?float
    {
        return $this->gauges[$key] ?? null;
    }

    // ─── Interno ──────────────────────────────────────────────────────────────

    /** @param array<string,string> $labels */
    private function inc(string $name, array $labels): void
    {
        $key = self::key($name, $labels);
        $this->counters[$key] = ($this->counters[$key] ?? 0) + 1;
    }

    /** @param array<string,string> $labels */
    private function set(string $name, array $labels, float $value): void
    {
        $this->gauges[self::key($name, $labels)] = $value;
    }

    /** @param array<string,string> $labels */
    private function observe(string $name, array $labels, float $value): void
    {
        $key = self::key($name, $labels);
        $this->histograms[$key] ??= [
            'buckets' => array_fill(0, count(MetricsSink::DURATION_BUCKETS), 0),
            'sum' => 0.0,
            'count' => 0,
        ];

        $this->histograms[$key]['sum'] += $value;
        $this->histograms[$key]['count']++;
        foreach (MetricsSink::DURATION_BUCKETS as $i => $limit) {
            if ($value <= $limit) {
                $this->histograms[$key]['buckets'][$i]++;
            }
        }
    }

    /**
     * `nombre{k="v",…}` con las etiquetas en el orden en que las declara el llamante.
     *
     * El orden es siempre el mismo para una métrica dada, así que la clave de una serie
     * temporal es estable. Sin esa estabilidad, la misma serie aparecería bajo dos claves
     * según el orden en que se construyó el array y los contadores se partirían en dos.
     *
     * @param array<string,string> $labels
     */
    private static function key(string $name, array $labels): string
    {
        if ($labels === []) {
            return $name;
        }

        $parts = [];
        foreach ($labels as $k => $v) {
            $parts[] = $k . '="' . self::escape($v) . '"';
        }

        return $name . '{' . implode(',', $parts) . '}';
    }

    /**
     * Escapa un valor de etiqueta según el formato de exposición.
     *
     * No es cosmético: un `code` con una comilla rompería la línea y **Prometheus descarta
     * el scrape entero, no solo esa línea**. Los `code` salen de mensajes de error de
     * terceros, así que la comilla llega antes o después.
     */
    private static function escape(string $value): string
    {
        return str_replace(['\\', '"', "\n"], ['\\\\', '\\"', '\\n'], $value);
    }

    /**
     * Un float en el formato que Prometheus espera, sin notación científica ni el
     * separador decimal del locale.
     *
     * `1.0` se imprime como `1`: la etiqueta `le="1"` y la `le="1.0"` son **dos series
     * distintas**, y los buckets de 08-observability.md §3 se escriben `1`, `5`, `10`,
     * `30`. Si el SDK emitiera `1.0`, el dashboard del ecosistema no agruparía.
     */
    private static function number(float $value): string
    {
        if ($value === floor($value) && abs($value) < 1e15) {
            return (string) (int) $value;
        }

        return rtrim(rtrim(number_format($value, 9, '.', ''), '0'), '.');
    }
}
