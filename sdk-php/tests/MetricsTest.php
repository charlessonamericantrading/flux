<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\ErrorClass;
use Flux\Metrics\ConnectionState;
use Flux\Metrics\ConsumeOutcome;
use Flux\Metrics\InMemoryMetrics;
use Flux\Metrics\MetricsSink;
use Flux\Metrics\NoMetrics;
use Flux\Metrics\PublishOutcome;
use Flux\Protocol;
use PHPUnit\Framework\TestCase;

/**
 * Métricas — specification/08-observability.md.
 *
 * Los nombres y las etiquetas son **contrato entre SDKs**, así que se comprueban sobre la
 * salida real: si el de PHP nombrara distinto la tasa de DLQ, la de los servicios PHP no
 * se podría sumar con la de los de Go y un panel del ecosistema sería imposible.
 */
final class MetricsTest extends TestCase
{
    private const SUBJECT = 'pedidos.pedido.v1.creado';
    private const CONSUMER = 'facturacion-api__pedidos_pedido_v1_creado';

    // ─── Buckets — §3 ─────────────────────────────────────────────────────────

    /** La invariante que da sentido al histograma entero. */
    public function testElUltimoBucketEsElAckWait(): void
    {
        // Un handler en el bucket superior está a punto de que su mensaje se reentregue
        // mientras aún se ejecuta, así que `…_bucket{le="30"}` frente al total mide
        // directamente cuántos eventos rozan la ejecución concurrente. Si el bucket dejara
        // de coincidir con el plazo real, mediría algo que no le importa a nadie — y esa
        // desincronización solo se detecta aquí.
        $buckets = MetricsSink::DURATION_BUCKETS;

        self::assertSame(
            (float) Protocol::ACK_WAIT_MS / 1000.0,
            (float) $buckets[count($buckets) - 1],
        );
    }

    public function testLosBucketsSonLosDelContrato(): void
    {
        // `protocol.json` se lee del repositorio: una divergencia entre el SDK y el
        // contrato falla en CI, no en producción con otro SDK del ecosistema.
        $protocol = ProtocolFixture::protocol();

        self::assertSame(
            array_map(floatval(...), $protocol['observability']['durationBucketsSeconds']),
            array_map(floatval(...), MetricsSink::DURATION_BUCKETS),
        );
    }

    public function testLosBucketsAscienden(): void
    {
        $buckets = MetricsSink::DURATION_BUCKETS;

        for ($i = 1; $i < count($buckets); $i++) {
            self::assertGreaterThan($buckets[$i - 1], $buckets[$i]);
        }
    }

    // ─── Nombres y etiquetas — §2 ─────────────────────────────────────────────

    public function testLasSieteMetricasSonLasDelContrato(): void
    {
        $m = new InMemoryMetrics();
        $m->eventPublished(self::SUBJECT, PublishOutcome::Ok);
        $m->eventConsumed(self::SUBJECT, self::CONSUMER, ConsumeOutcome::Ok);
        $m->handlerDuration(self::SUBJECT, self::CONSUMER, 0.4);
        $m->eventDlq(self::SUBJECT, self::CONSUMER, ErrorClass::Permanent, 'HTTP_404');
        $m->eventRetried(self::SUBJECT, self::CONSUMER, 3);
        $m->consumerPending(self::SUBJECT, self::CONSUMER, 42);
        $m->connectionState(ConnectionState::Connected);

        $salida = $m->render();
        $declaradas = array_keys(ProtocolFixture::protocol()['observability']['metrics']);
        self::assertCount(7, $declaradas);

        foreach ($declaradas as $nombre) {
            self::assertStringContainsString($nombre, $salida, "falta {$nombre}");
        }
    }

    public function testCuentaPublicacionesPorSubjectYResultado(): void
    {
        $m = new InMemoryMetrics();
        $m->eventPublished(self::SUBJECT, PublishOutcome::Ok);
        $m->eventPublished(self::SUBJECT, PublishOutcome::Ok);
        $m->eventPublished(self::SUBJECT, PublishOutcome::Error);

        self::assertSame(
            2,
            $m->counter('flux_events_published_total{subject="' . self::SUBJECT . '",outcome="ok"}'),
        );
        self::assertSame(
            1,
            $m->counter('flux_events_published_total{subject="' . self::SUBJECT . '",outcome="error"}'),
        );
    }

    public function testLaClaveDeUnaSerieEsEstableEntreRecolectores(): void
    {
        // Sin orden estable, la misma serie temporal aparecería con dos claves según el
        // orden en que se construyeron las etiquetas, y los contadores se partirían en dos.
        $a = new InMemoryMetrics();
        $b = new InMemoryMetrics();
        $a->eventDlq(self::SUBJECT, self::CONSUMER, ErrorClass::Permanent, 'HTTP_404');
        $b->eventDlq(self::SUBJECT, self::CONSUMER, ErrorClass::Permanent, 'HTTP_404');

        self::assertSame($a->render(), $b->render());
    }

    /**
     * §2.2: **NUNCA** se etiqueta con `tenantid`, `id` ni `correlationid`.
     *
     * Aquí no puede pasar porque no hay dónde meterlos —las firmas de `MetricsSink` no lo
     * permiten—, pero el test lo fija sobre la salida por si alguien añade una etiqueta de
     * más. La cardinalidad no avisa: funciona con tres tenants y muere con diez mil.
     */
    public function testNingunaEtiquetaProhibidaLlegaALaSalida(): void
    {
        $m = new InMemoryMetrics();
        $m->eventPublished(self::SUBJECT, PublishOutcome::Ok);
        $m->eventConsumed(self::SUBJECT, self::CONSUMER, ConsumeOutcome::Ok);
        $m->eventDlq(self::SUBJECT, self::CONSUMER, ErrorClass::Poison, 'MALFORMED_JSON');
        $m->eventRetried(self::SUBJECT, self::CONSUMER, 1);
        $m->consumerPending(self::SUBJECT, self::CONSUMER, 7);
        $m->handlerDuration(self::SUBJECT, self::CONSUMER, 1.0);
        $m->connectionState(ConnectionState::Reconnecting);

        $salida = $m->render();

        foreach (ProtocolFixture::protocol()['observability']['forbiddenLabels'] as $prohibida) {
            self::assertStringNotContainsString("{$prohibida}=", $salida);
        }
    }

    // ─── Formato de exposición ────────────────────────────────────────────────

    public function testElHistogramaAcumulaEnTodosLosBucketsQueSuperanElValor(): void
    {
        $m = new InMemoryMetrics();
        $m->handlerDuration(self::SUBJECT, self::CONSUMER, 0.03); // por encima de 0.025

        $salida = $m->render();
        self::assertStringContainsString('le="0.025"} 0', $salida);
        self::assertStringContainsString('le="0.05"} 1', $salida);
        self::assertStringContainsString('le="+Inf"} 1', $salida);
        self::assertStringContainsString('_count{', $salida);
    }

    /**
     * `le="30"`, no `le="30.0"`: para Prometheus son **dos series distintas** y el
     * dashboard del ecosistema agrupa por la primera.
     */
    public function testElBucketEnteroSeImprimeSinDecimales(): void
    {
        $m = new InMemoryMetrics();
        $m->handlerDuration(self::SUBJECT, self::CONSUMER, 0.4);

        $salida = $m->render();
        self::assertStringContainsString('le="30"}', $salida);
        self::assertStringNotContainsString('le="30.0"}', $salida);
        self::assertStringContainsString('le="0.005"}', $salida);
    }

    public function testUnGaugeSinEtiquetasNoDejaLlavesVacias(): void
    {
        $m = new InMemoryMetrics();
        $m->connectionState(ConnectionState::Connected);

        self::assertContains('flux_connection_state 1', explode("\n", $m->render()));
    }

    public function testLosTresEstadosDeConexionSonLosDeLaSpec(): void
    {
        $declarados = ProtocolFixture::protocol()['observability']['metrics']
            ['flux_connection_state']['values'];

        self::assertSame('desconectado', $declarados[(string) ConnectionState::Disconnected->value]);
        self::assertSame('conectado', $declarados[(string) ConnectionState::Connected->value]);
        self::assertSame('reconectando', $declarados[(string) ConnectionState::Reconnecting->value]);
    }

    /** Cada línea del formato de exposición, una a una. */
    public function testLasLineasTienenFormaValidaDePrometheus(): void
    {
        $m = new InMemoryMetrics();
        $m->eventPublished(self::SUBJECT, PublishOutcome::Ok);
        $m->eventConsumed(self::SUBJECT, self::CONSUMER, ConsumeOutcome::Ok);
        $m->eventDlq(self::SUBJECT, self::CONSUMER, ErrorClass::Permanent, 'HTTP_404');
        $m->eventRetried(self::SUBJECT, self::CONSUMER, 3);
        $m->consumerPending(self::SUBJECT, self::CONSUMER, 42);
        $m->connectionState(ConnectionState::Connected);
        $m->handlerDuration(self::SUBJECT, self::CONSUMER, 0.4);

        foreach (explode("\n", $m->render()) as $linea) {
            if ($linea === '' || str_starts_with($linea, '#')) {
                continue;
            }

            self::assertMatchesRegularExpression(
                '/^flux_[a-z0-9_]+(\{[^}]*\})? -?[0-9.]+$/',
                $linea,
                "línea no válida para Prometheus: {$linea}"
            );
        }
    }

    /**
     * Un `code` con comillas rompería el formato y **Prometheus descartaría el scrape
     * entero**, no solo esa línea. Los `code` salen de errores de terceros, así que la
     * comilla llega antes o después.
     */
    public function testEscapaLasComillasDeLosValoresDeEtiqueta(): void
    {
        $m = new InMemoryMetrics();
        $m->eventDlq(self::SUBJECT, self::CONSUMER, ErrorClass::Permanent, 'con "comillas"');

        $linea = null;
        foreach (explode("\n", $m->render()) as $l) {
            if (str_starts_with($l, 'flux_events_dlq_total')) {
                $linea = $l;
            }
        }

        self::assertNotNull($linea);
        self::assertStringContainsString('code="con \\"comillas\\""', $linea);
        // Las comillas SIN escapar quedan balanceadas: 2 por cada una de las 4 etiquetas.
        self::assertSame(0, preg_match_all('/(?<!\\\\)"/', $linea) % 2, $linea);
    }

    public function testEscapaBarrasYSaltosDeLinea(): void
    {
        $m = new InMemoryMetrics();
        $m->eventDlq(self::SUBJECT, self::CONSUMER, ErrorClass::Poison, "a\\b\nc");

        self::assertStringContainsString('code="a\\\\b\\nc"', $m->render());
    }

    public function testElIntentoSeEtiquetaComoTexto(): void
    {
        $m = new InMemoryMetrics();
        $m->eventRetried(self::SUBJECT, self::CONSUMER, 3);

        self::assertSame(1, $m->counter(
            'flux_events_retried_total{subject="' . self::SUBJECT . '",consumer="'
            . self::CONSUMER . '",attempt="3"}'
        ));
    }

    public function testElGaugeDePendientesSeSobrescribeNoSeAcumula(): void
    {
        $m = new InMemoryMetrics();
        $m->consumerPending(self::SUBJECT, self::CONSUMER, 10);
        $m->consumerPending(self::SUBJECT, self::CONSUMER, 3);

        self::assertSame(3.0, $m->gauge(
            'flux_consumer_pending{subject="' . self::SUBJECT . '",consumer="' . self::CONSUMER . '"}'
        ));
    }

    // ─── Literales de las etiquetas — §2.1 ────────────────────────────────────

    public function testLosLiteralesDeLasEtiquetasSonLosDeLaSpec(): void
    {
        self::assertSame(
            ['ok', 'retryable', 'permanent', 'poison', 'invalid_schema', 'invalid_signature'],
            array_map(static fn (ConsumeOutcome $o): string => $o->value, ConsumeOutcome::cases()),
        );
        self::assertSame(
            ['ok', 'invalid_schema', 'error'],
            array_map(static fn (PublishOutcome $o): string => $o->value, PublishOutcome::cases()),
        );
        // `reason` reutiliza ErrorClass: su valor ES el literal de `dlqreason`, así que no
        // hay un segundo enum que alguien pueda desincronizar.
        self::assertSame(
            ['retryable', 'permanent', 'poison'],
            array_map(static fn (ErrorClass $c): string => $c->value, ErrorClass::cases()),
        );
    }

    public function testLaClaseDeErrorSeTraduceAlOutcomeCorrespondiente(): void
    {
        self::assertSame(
            ConsumeOutcome::Retryable,
            ConsumeOutcome::fromErrorClass(ErrorClass::Retryable),
        );
        self::assertSame(
            ConsumeOutcome::Permanent,
            ConsumeOutcome::fromErrorClass(ErrorClass::Permanent),
        );
        self::assertSame(
            ConsumeOutcome::Poison,
            ConsumeOutcome::fromErrorClass(ErrorClass::Poison),
        );
    }

    // ─── El default ───────────────────────────────────────────────────────────

    public function testNoMetricsNoGuardaNadaYNoLanza(): void
    {
        // Un SDK no debe imponer un backend de métricas: el default no hace nada.
        $m = new NoMetrics();
        $m->eventPublished(self::SUBJECT, PublishOutcome::Ok);
        $m->eventConsumed(self::SUBJECT, self::CONSUMER, ConsumeOutcome::Poison);
        $m->handlerDuration(self::SUBJECT, self::CONSUMER, 0.1);
        $m->eventDlq(self::SUBJECT, self::CONSUMER, ErrorClass::Poison, 'X');
        $m->eventRetried(self::SUBJECT, self::CONSUMER, 1);
        $m->consumerPending(self::SUBJECT, self::CONSUMER, 0);
        $m->connectionState(ConnectionState::Disconnected);

        $this->addToAssertionCount(1);
    }
}
