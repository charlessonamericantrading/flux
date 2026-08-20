<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\ConnectOptions;
use Flux\Envelope;
use Flux\FluxBus;
use Flux\FluxEvent;
use Flux\Metrics\InMemoryMetrics;
use Flux\Signing\Keys;
use Flux\Signing\SigningOptions;
use Flux\Signing\VerificationMode;
use Flux\TenantIsolation;
use Flux\TenantIsolationException;
use Flux\Transport\Ack;
use Flux\Transport\InMemoryTransport;
use PHPUnit\Framework\TestCase;

/**
 * Aislamiento de tenant, métricas y firma **a través del bus**.
 * Contrato normativo: 09-multitenancy.md §3, 08-observability.md, 07-signing.md.
 *
 * Todo con `InMemoryTransport`: sin broker y sin la librería de NATS instalada.
 */
final class TenantIsolationTest extends TestCase
{
    private const SUBJECT = ProtocolFixture::SUBJECT;
    private const DURABLE = 'facturacion-api__pedidos_pedido_v1_creado';

    private InMemoryTransport $transport;

    protected function setUp(): void
    {
        $this->transport = new InMemoryTransport();
    }

    // ─── Modo estricto — 09-multitenancy.md §3.3 ──────────────────────────────

    /**
     * El punto que importa de toda la sección: consumir sin filtro **DEBE** ser un error de
     * configuración, no un descuido silencioso.
     */
    public function testStrictSinTenantEsUnErrorDeConfiguracion(): void
    {
        $bus = $this->bus(tenantId: '', isolation: TenantIsolation::Strict);

        $this->expectException(TenantIsolationException::class);
        $this->expectExceptionMessageMatches('/TODOS los tenants/u');

        $bus->subscribe(self::SUBJECT, static fn () => null);
    }

    /**
     * `"system"` NO cuenta como filtro: es la AUSENCIA de tenant, reservada a los eventos
     * de plataforma — 09-multitenancy.md §5. Y además es el **default** de
     * `ConnectOptions`, así que sin esta regla el modo estricto no protegería de nada en el
     * caso más probable: el de quien no configuró tenant.
     */
    public function testStrictRechazaSystemComoFiltro(): void
    {
        $bus = $this->bus(tenantId: 'system', isolation: TenantIsolation::Strict);

        $this->expectException(TenantIsolationException::class);
        $this->expectExceptionMessageMatches('/AUSENCIA de tenant/u');

        $bus->subscribe(self::SUBJECT, static fn () => null);
    }

    /** El error llega ANTES de crear el durable: nada queda a medias en el servidor. */
    public function testStrictFallaAntesDeCrearElConsumidor(): void
    {
        $bus = $this->bus(tenantId: 'system', isolation: TenantIsolation::Strict);

        try {
            $bus->subscribe(self::SUBJECT, static fn () => null);
            self::fail('debería haber lanzado');
        } catch (TenantIsolationException) {
            self::assertSame([], $this->transport->consumers);
        }
    }

    public function testStrictAceptaUnFiltroDeSuscripcionAunqueLaConexionNoLoTenga(): void
    {
        $bus = $this->bus(tenantId: 'system', isolation: TenantIsolation::Strict);

        $sub = $bus->subscribe(self::SUBJECT, static fn () => null, tenantId: 'globex');

        self::assertSame('globex', $sub->tenantId);
    }

    public function testElFiltroDeLaSuscripcionGanaAlDeLaConexion(): void
    {
        $bus = $this->bus(tenantId: 'acme', isolation: TenantIsolation::Strict);

        self::assertSame(
            'globex',
            $bus->subscribe(self::SUBJECT, static fn () => null, tenantId: 'globex')->tenantId,
        );
    }

    /** El default es `off`: el filtrado sigue siendo opcional por suscripción. */
    public function testElDefaultEsOff(): void
    {
        $options = new ConnectOptions(service: 'x', environment: 'dev', version: '1.0.0');

        self::assertSame(TenantIsolation::Off, $options->tenantIsolation);
        self::assertNull($options->metrics);
        self::assertNull($options->signing);
    }

    public function testOffPermiteSuscribirseSinFiltro(): void
    {
        $bus = $this->bus(tenantId: 'system');

        self::assertNull($bus->subscribe(self::SUBJECT, static fn () => null)->tenantId);
    }

    // ─── Filtrado — §3.2 ──────────────────────────────────────────────────────

    /**
     * El evento de otro tenant se **confirma y se descarta antes del handler**: no es un
     * fallo, no es para nosotros.
     */
    public function testElEventoDeOtroTenantSeAckeaYNoLlegaAlHandler(): void
    {
        $bus = $this->bus(tenantId: 'acme', isolation: TenantIsolation::Strict);

        $vistos = [];
        $bus->subscribe(self::SUBJECT, static function (FluxEvent $e) use (&$vistos): void {
            $vistos[] = $e->tenantid;
        });

        $this->deliver(ProtocolFixture::event(['tenantid' => 'globex']));
        $this->deliver(ProtocolFixture::event(['tenantid' => 'acme']));
        $bus->run(stopWhenIdle: true);

        self::assertSame(['acme'], $vistos, 'el de globex no debería haber llegado al handler');
        // Los dos se confirman: el ajeno también, porque descartarlo no es un fallo.
        self::assertSame([Ack::ACK, Ack::ACK], $this->transport->ackTokens());
        // Y ninguno de los dos acaba en la DLQ.
        self::assertSame([], $this->transport->publishedTo('dlq.' . self::SUBJECT));
    }

    // ─── Métricas a través del bus — 08-observability.md ──────────────────────

    public function testElCicloPublicarConsumirEmiteLasMetricasDelProtocolo(): void
    {
        $metrics = new InMemoryMetrics();
        $bus = $this->bus(tenantId: 'acme', metrics: $metrics);

        $bus->subscribe(self::SUBJECT, static fn () => null);
        $bus->publish(self::SUBJECT, ['pedidoId' => 'ped-123']);
        $this->deliver(ProtocolFixture::event(['tenantid' => 'acme']));
        $bus->run(stopWhenIdle: true);

        $salida = $metrics->render();

        self::assertStringContainsString(
            'flux_events_published_total{subject="' . self::SUBJECT . '",outcome="ok"} 1',
            $salida,
        );
        self::assertStringContainsString(
            'flux_events_consumed_total{subject="' . self::SUBJECT . '",consumer="'
            . self::DURABLE . '",outcome="ok"} 1',
            $salida,
        );
        self::assertStringContainsString('flux_connection_state 1', $salida);
        self::assertStringContainsString('flux_consumer_pending{', $salida);
        self::assertStringContainsString('flux_event_handler_duration_seconds_bucket{', $salida);
        // §2.2: NUNCA por tenant, aunque el bus tenga uno configurado.
        self::assertStringNotContainsString('acme', $salida);
    }

    public function testUnPermanentCuentaEnDlqConSuCodigoEstable(): void
    {
        $metrics = new InMemoryMetrics();
        $bus = $this->bus(tenantId: 'acme', metrics: $metrics);

        $bus->subscribe(self::SUBJECT, static function (): void {
            throw new \Flux\PermanentError('pedido ya cancelado', 'PEDIDO_YA_CANCELADO');
        });
        $this->deliver(ProtocolFixture::event(['tenantid' => 'acme']));
        $bus->run(stopWhenIdle: true);

        self::assertSame(1, $metrics->counter(
            'flux_events_dlq_total{subject="' . self::SUBJECT . '",consumer="' . self::DURABLE
            . '",reason="permanent",code="PEDIDO_YA_CANCELADO"}'
        ));
        self::assertSame(1, $metrics->counter(
            'flux_events_consumed_total{subject="' . self::SUBJECT . '",consumer="'
            . self::DURABLE . '",outcome="permanent"}'
        ));
    }

    public function testUnRetryableCuentaComoReintento(): void
    {
        $metrics = new InMemoryMetrics();
        $bus = $this->bus(tenantId: 'acme', metrics: $metrics);

        $bus->subscribe(self::SUBJECT, static function (): void {
            throw new \Flux\RetryableError('proveedor 503', 'HTTP_503');
        });
        $this->deliver(ProtocolFixture::event(['tenantid' => 'acme']));
        $bus->run(stopWhenIdle: true);

        self::assertSame(1, $metrics->counter(
            'flux_events_retried_total{subject="' . self::SUBJECT . '",consumer="'
            . self::DURABLE . '",attempt="1"}'
        ));
    }

    public function testUnMensajeIlegibleCuentaComoPoison(): void
    {
        $metrics = new InMemoryMetrics();
        $bus = $this->bus(tenantId: 'acme', metrics: $metrics);

        $bus->subscribe(self::SUBJECT, static fn () => null);
        $this->transport->deliver(self::SUBJECT, '{no soy json', 1, self::DURABLE);
        $bus->run(stopWhenIdle: true);

        self::assertSame(1, $metrics->counter(
            'flux_events_dlq_total{subject="' . self::SUBJECT . '",consumer="' . self::DURABLE
            . '",reason="poison",code="MALFORMED_JSON"}'
        ));
    }

    public function testCloseMarcaLaConexionComoCaida(): void
    {
        // Sin esto, un panel no distingue "el servicio está sano" de "el proceso se fue".
        $metrics = new InMemoryMetrics();
        $bus = $this->bus(tenantId: 'acme', metrics: $metrics);
        self::assertSame(1.0, $metrics->gauge('flux_connection_state'));

        $bus->close();

        self::assertSame(0.0, $metrics->gauge('flux_connection_state'));
    }

    // ─── Firma a través del bus — 07-signing.md ───────────────────────────────

    public function testPublicarFirmaElEventoYConsumirloLoVerifica(): void
    {
        if (!extension_loaded('sodium')) {
            self::markTestSkipped('ext-sodium no está cargada');
        }

        $par = Keys::generateKeyPair();

        $productor = $this->bus(
            tenantId: 'acme',
            signing: new SigningOptions(
                privateKey: $par['privateKeyPem'],
                keyId: 'facturacion-api-1',
            ),
        );

        $publicado = $productor->publish(self::SUBJECT, ['pedidoId' => 'ped-123']);
        self::assertSame('facturacion-api-1', $publicado->signkeyid);
        self::assertNotNull($publicado->signature);

        // El evento que viaja por el cable lleva la firma, y `data` sigue siendo el último.
        $enElCable = $this->transport->publishedTo(self::SUBJECT)[0]['payload'];
        self::assertStringContainsString('"signature":', $enElCable);
        self::assertStringEndsWith('}}', $enElCable);

        $vistos = [];
        $consumidor = $this->bus(
            tenantId: 'acme',
            signing: new SigningOptions(
                publicKeys: ['facturacion-api-1' => $par['publicKeyPem']],
                verify: VerificationMode::Require,
            ),
        );
        $consumidor->subscribe(self::SUBJECT, static function (FluxEvent $e) use (&$vistos): void {
            $vistos[] = $e->id;
        });
        $this->transport->deliver(self::SUBJECT, $enElCable, 1, self::DURABLE);
        $consumidor->run(stopWhenIdle: true);

        self::assertSame([$publicado->id], $vistos);
        self::assertSame([Ack::ACK], $this->transport->ackTokens());
    }

    /**
     * En `require`, un evento **sin firma** es POISON: `term()` + DLQ, y el handler nunca
     * lo ve — 07-signing.md §7.
     */
    public function testRequireMandaALaDlqUnEventoSinFirma(): void
    {
        if (!extension_loaded('sodium')) {
            self::markTestSkipped('ext-sodium no está cargada');
        }

        $metrics = new InMemoryMetrics();
        $par = Keys::generateKeyPair();

        $bus = $this->bus(
            tenantId: 'acme',
            metrics: $metrics,
            signing: new SigningOptions(
                publicKeys: ['pedidos-api-1' => $par['publicKeyPem']],
                verify: VerificationMode::Require,
            ),
        );

        $llego = 0;
        $bus->subscribe(self::SUBJECT, static function () use (&$llego): void {
            $llego++;
        });
        $this->deliver(ProtocolFixture::event(['tenantid' => 'acme']));
        $bus->run(stopWhenIdle: true);

        self::assertSame(0, $llego, 'la firma se comprueba ANTES del handler');
        self::assertSame([Ack::TERM], $this->transport->ackTokens());

        $enDlq = json_decode(
            $this->transport->publishedTo('dlq.' . self::SUBJECT)[0]['payload'],
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        self::assertSame('poison', $enDlq['dlqreason']);
        self::assertStringContainsString('MISSING_SIGNATURE', $enDlq['dlqerror']);

        // §2.1 distingue `invalid_signature` de `poison` para que un pico de firmas rotas
        // no se confunda con un pico de JSON corrupto: causas y respuestas distintas.
        self::assertSame(1, $metrics->counter(
            'flux_events_consumed_total{subject="' . self::SUBJECT . '",consumer="'
            . self::DURABLE . '",outcome="invalid_signature"}'
        ));
        self::assertSame(1, $metrics->counter(
            'flux_events_dlq_total{subject="' . self::SUBJECT . '",consumer="' . self::DURABLE
            . '",reason="poison",code="MISSING_SIGNATURE"}'
        ));
    }

    /**
     * §7.1: **`warn` DEBE ser observable.** El evento se acepta y llega al handler, pero
     * `flux_events_consumed_total{outcome="invalid_signature"}` se emite igual.
     *
     * Sin esa métrica, `warn` es inútil para lo único que existe —pilotar la migración—: la
     * pregunta "¿cuántos eventos siguen sin firma y de qué productores?" habría que
     * buscarla a mano en los logs de siete servicios.
     */
    public function testWarnAceptaElEventoPeroLoCuentaComoInvalidSignature(): void
    {
        if (!extension_loaded('sodium')) {
            self::markTestSkipped('ext-sodium no está cargada');
        }

        $metrics = new InMemoryMetrics();
        $par = Keys::generateKeyPair();

        $bus = $this->bus(
            tenantId: 'acme',
            metrics: $metrics,
            signing: new SigningOptions(
                publicKeys: ['pedidos-api-1' => $par['publicKeyPem']],
                verify: VerificationMode::Warn,
            ),
        );

        $llego = 0;
        $bus->subscribe(self::SUBJECT, static function () use (&$llego): void {
            $llego++;
        });
        $this->deliver(ProtocolFixture::event(['tenantid' => 'acme']));
        $bus->run(stopWhenIdle: true);

        // `warn` acepta: el handler SÍ lo ve, y no hay nada en la DLQ.
        self::assertSame(1, $llego);
        self::assertSame([Ack::ACK], $this->transport->ackTokens());
        self::assertSame([], $this->transport->publishedTo('dlq.' . self::SUBJECT));

        self::assertSame(1, $metrics->counter(
            'flux_events_consumed_total{subject="' . self::SUBJECT . '",consumer="'
            . self::DURABLE . '",outcome="invalid_signature"}'
        ));
        // Y NO se cuenta además como `ok`: contarlo dos veces rompería
        // `sum by (outcome) == total consumido`.
        self::assertNull($metrics->counter(
            'flux_events_consumed_total{subject="' . self::SUBJECT . '",consumer="'
            . self::DURABLE . '",outcome="ok"}'
        ));
    }

    /** Con `off` —el default— un evento firmado se consume igual. Sin eso, §7 sería imposible. */
    public function testConLaVerificacionApagadaUnEventoFirmadoSeConsumeIgual(): void
    {
        if (!extension_loaded('sodium')) {
            self::markTestSkipped('ext-sodium no está cargada');
        }

        $par = Keys::generateKeyPair();
        $firmado = \Flux\Signing\Signer::fromOptions(new SigningOptions(
            privateKey: $par['privateKeyPem'],
            keyId: 'pedidos-api-1',
        ))?->sign(ProtocolFixture::event(['tenantid' => 'acme']));

        $bus = $this->bus(tenantId: 'acme');
        $vistos = [];
        $bus->subscribe(self::SUBJECT, static function (FluxEvent $e) use (&$vistos): void {
            $vistos[] = $e->signkeyid;
        });
        $this->transport->deliver(self::SUBJECT, Envelope::serialize($firmado), 1, self::DURABLE);
        $bus->run(stopWhenIdle: true);

        self::assertSame(['pedidos-api-1'], $vistos);
    }

    // ─── Utilidades ───────────────────────────────────────────────────────────

    private function bus(
        string $tenantId = 'acme',
        TenantIsolation $isolation = TenantIsolation::Off,
        ?InMemoryMetrics $metrics = null,
        ?SigningOptions $signing = null,
    ): FluxBus {
        return FluxBus::connect(
            new ConnectOptions(
                service: 'facturacion-api',
                environment: 'produccion',
                version: '3.4.1',
                tenantId: $tenantId,
                tenantIsolation: $isolation,
                classification: 'confidential',
                schemaBaseUrl: 'https://schemas.internal',
                signing: $signing,
                metrics: $metrics,
            ),
            $this->transport,
        );
    }

    private function deliver(FluxEvent $event, int $attempt = 1): void
    {
        $this->transport->deliver(
            self::SUBJECT,
            Envelope::serialize($event),
            $attempt,
            self::DURABLE,
        );
    }
}
