<?php

declare(strict_types=1);

namespace Flux\Metrics;

use Flux\ErrorClass;

/**
 * Destino de las métricas del SDK.
 * Contrato normativo: specification/08-observability.md
 *
 * Los **nombres y las etiquetas son parte del contrato**, no una decisión de cada SDK. Si
 * el de PHP y el de Go nombran distinto la tasa de DLQ, la de los servicios PHP no se
 * puede sumar con la de los de Go y un panel del ecosistema es imposible. Es el mismo
 * argumento que los códigos POISON de 01-envelope.md §3.1: **si dos SDKs emiten nombres
 * distintos para lo mismo, agrupar deja de funcionar en cuanto el ecosistema es polyglot
 * — que es siempre.**
 *
 * Lo que la aplicación elige es el *backend*: implementa esta interfaz contra
 * `promphp/prometheus_client_php`, StatsD u OpenTelemetry y recibe los mismos nombres. El
 * default es `NoMetrics`, porque un SDK no debe imponer un backend.
 *
 * ⚠️ **Un método por métrica, con parámetros propios — no un `array $labels` genérico.**
 * No es estilo: un mapa de etiquetas es exactamente el agujero por el que se cuela un
 * `tenantid` que multiplica las series temporales. Con esta forma, etiquetar por tenant
 * exige cambiar la firma de la interfaz, y eso se ve en una revisión. La cardinalidad **no
 * avisa**: el sistema funciona en desarrollo con tres tenants y muere en producción con
 * diez mil, y el fallo se manifiesta como "Prometheus se ha quedado sin memoria", no como
 * "alguien etiquetó por tenant" — 08-observability.md §2.2.
 */
interface MetricsSink
{
    /**
     * Buckets del histograma de duración, en segundos — 08-observability.md §3.
     *
     * El último es `30` **a propósito: es el `ack_wait`** (03-delivery.md §2). Un handler
     * que cae en el bucket superior está a punto de que su mensaje se reentregue mientras
     * aún se ejecuta, así que `flux_event_handler_duration_seconds_bucket{le="30"}` frente
     * al total mide directamente cuántos eventos rozan la ejecución concurrente.
     *
     * **DEBE moverse si se cambia `ack_wait`.** Un bucket que no coincide con el plazo real
     * mide algo que no le importa a nadie. `MetricsTest::testElUltimoBucketEsElAckWait()`
     * fija la invariante contra `Protocol::ACK_WAIT_MS`.
     */
    public const DURATION_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30];

    /** `flux_events_published_total{subject, outcome}` */
    public function eventPublished(string $subject, PublishOutcome $outcome): void;

    /** `flux_events_consumed_total{subject, consumer, outcome}` */
    public function eventConsumed(string $subject, string $consumer, ConsumeOutcome $outcome): void;

    /** `flux_event_handler_duration_seconds{subject, consumer}` */
    public function handlerDuration(string $subject, string $consumer, float $seconds): void;

    /**
     * `flux_events_dlq_total{subject, consumer, reason, code}`
     *
     * `$code` **DEBE** ser un identificador estable (`HTTP_503`, `PEDIDO_YA_CANCELADO`) y
     * **nunca** el mensaje de error: un mensaje lleva ids, timestamps y rutas, su
     * cardinalidad es infinita y tumba el almacenamiento de métricas — §2.2.
     */
    public function eventDlq(string $subject, string $consumer, ErrorClass $reason, string $code): void;

    /** `flux_events_retried_total{subject, consumer, attempt}`. `$attempt` es 1..max_deliver. */
    public function eventRetried(string $subject, string $consumer, int $attempt): void;

    /**
     * `flux_consumer_pending{subject, consumer}`
     *
     * Es la métrica que delata un consumidor cuyo bucle murió: **sigue reportando la
     * conexión como sana** y solo el crecimiento de `pending` lo evidencia. Es el bug que
     * apareció de verdad en el SDK de Node — 08-observability.md §4.
     */
    public function consumerPending(string $subject, string $consumer, int $pending): void;

    /** `flux_connection_state` — sin etiquetas. */
    public function connectionState(ConnectionState $state): void;
}
