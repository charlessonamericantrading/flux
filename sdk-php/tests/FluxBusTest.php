<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\ClassifierOptions;
use Flux\ConnectOptions;
use Flux\ConsumerConfigMismatchException;
use Flux\Envelope;
use Flux\EventContext;
use Flux\FluxBus;
use Flux\FluxEvent;
use Flux\HandlerContext;
use Flux\PermanentError;
use Flux\Protocol;
use Flux\RetryableError;
use Flux\Transport\Ack;
use Flux\Transport\InMemoryTransport;
use PHPUnit\Framework\TestCase;

/**
 * El runtime del consumidor, sin broker.
 *
 * Aquí vive lo que de verdad puede romperse en silencio: el presupuesto de reintentos, la
 * verificación de la config del consumidor, y las dos reglas de robustez que salieron de
 * bugs reales del SDK de Node (si la DLQ falla NO se hace `term`; un fallo del despacho no
 * mata el bucle).
 */
final class FluxBusTest extends TestCase
{
    private const SUBJECT = ProtocolFixture::SUBJECT;
    private const DLQ = 'dlq.pedidos.pedido.v1.creado';

    private InMemoryTransport $transport;

    protected function setUp(): void
    {
        $this->transport = new InMemoryTransport();
    }

    // ─── connect ──────────────────────────────────────────────────────────────

    public function testValidaElNombreDeServicioAlConectar(): void
    {
        // NATS aceptaría el durable `FacturacionAPI__pedidos_…` sin error, así que sin esta
        // comprobación el incumplimiento solo se descubre al parsear nombres en una
        // herramienta (protocol.json → naming.service).
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/nombre de servicio inválido/u');

        FluxBus::connect(
            new ConnectOptions(service: 'FacturacionAPI', environment: 'dev', version: '1.0.0'),
            $this->transport,
        );
    }

    public function testRechazaUnaClasificacionPorDefectoInventada(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        FluxBus::connect(
            new ConnectOptions(
                service: 'pedidos-api',
                environment: 'dev',
                version: '1.0.0',
                classification: 'secreto',
            ),
            $this->transport,
        );
    }

    // ─── publish ──────────────────────────────────────────────────────────────

    public function testPublicaConLaCabeceraNatsMsgId(): void
    {
        $event = $this->bus()->publish(self::SUBJECT, ['pedidoId' => 'ped-123'], aggregateId: 'ped-123');

        $published = $this->transport->publishedTo(self::SUBJECT);
        self::assertCount(1, $published);
        // Deduplica reintentos de PUBLICACIÓN dentro de `duplicate_window`; NO deduplica
        // reentregas de consumo — 03-delivery.md §3.
        self::assertSame($event->id, $published[0]['headers'][Protocol::MSG_ID_HEADER]);
        self::assertSame(Envelope::serialize($event), $published[0]['payload']);
    }

    public function testRellenaElEnvelopeEntero(): void
    {
        // El desarrollador solo escribe subject, data y opcionalmente aggregateId.
        $event = $this->bus()->publish(self::SUBJECT, ['pedidoId' => 'ped-123'], aggregateId: 'ped-123');

        self::assertMatchesRegularExpression('/^[0-9a-f-]{36}$/', $event->id);
        // `source` es `/<entorno>/<servicio>` con el servicio de connect(), que aquí es el
        // consumidor `facturacion-api` — no el dominio del subject. Son cosas distintas:
        // un servicio puede publicar en varios dominios.
        self::assertSame('/produccion/facturacion-api', $event->source);
        self::assertSame('com.flux.pedidos.pedido.creado.v1', $event->type);
        self::assertSame('3.4.1', $event->producerversion);
        self::assertSame('acme', $event->tenantid);
        // Si el evento no nace de otro, `correlationid` se inicializa con su propio `id`.
        self::assertSame($event->id, $event->correlationid);
        self::assertNull($event->causationid);
        self::assertSame('ped-123', $event->subject); // ⚠️ id del agregado, no el subject de NATS
    }

    public function testDerivaElDataschemaDelSubject(): void
    {
        $event = $this->bus()->publish(self::SUBJECT, ['pedidoId' => 'ped-123']);

        self::assertSame(
            'https://schemas.internal/pedidos/pedido/creado/1.0.0.json',
            $event->dataschema,
        );
    }

    public function testUnSubjectInvalidoFallaAntesDeTocarElCable(): void
    {
        $this->expectException(\Flux\InvalidSubjectException::class);

        $this->bus()->publish('Pedidos.Pedido.V1.Creado', ['a' => 1]);
    }

    public function testCreaElStreamDelDominioConSuPatronDeSubjects(): void
    {
        $this->bus()->publish(self::SUBJECT, ['pedidoId' => 'ped-123']);

        self::assertArrayHasKey('EVT_PEDIDOS', $this->transport->streams);
        self::assertSame(['pedidos.>'], $this->transport->streams['EVT_PEDIDOS']['subjects']);
    }

    // ─── subscribe: verificación de la config ─────────────────────────────────

    public function testEnviaLaConfiguracionCanonicaDeConsumidor(): void
    {
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static fn () => null);

        $config = $this->transport->consumers['facturacion-api__pedidos_pedido_v1_creado'];

        self::assertSame('explicit', $config['ack_policy']);
        self::assertSame(6, $config['max_deliver']);
        self::assertSame(256, $config['max_ack_pending']);
        // ⚠️ ack_wait DEBE ser backoff[0], porque el servidor lo sobrescribe con ese valor.
        self::assertSame($config['backoff'][0], $config['ack_wait']);
        self::assertSame(Protocol::nanos(30_000), $config['ack_wait']);
    }

    public function testCreaElStreamDeDlqConPrefijoYNoConSufijo(): void
    {
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static fn () => null);

        // Un sufijo encajaría con `pedidos.>` y el stream principal capturaría sus propios
        // muertos — 02-naming.md §3.1.
        self::assertSame(['dlq.pedidos.>'], $this->transport->streams['DLQ_PEDIDOS']['subjects']);
    }

    public function testFallaSiElServidorDevuelveOtraConfiguracion(): void
    {
        // LA trampa: JetStream sobrescribe `ack_wait` con `backoff[0]`, acepta la petición,
        // no avisa y devuelve algo distinto de lo enviado. Con un `ack_wait` efectivo de
        // 1 s, cualquier handler que toque una base de datos se ejecuta en concurrencia
        // consigo mismo — 03-delivery.md §2.1. Requisito L2.
        $this->transport->consumerConfigOverride = ['ack_wait' => Protocol::nanos(1000)];

        $this->expectException(ConsumerConfigMismatchException::class);
        $this->expectExceptionMessageMatches('/ack_wait/');

        $this->bus()->subscribe(self::SUBJECT, static fn () => null);
    }

    public function testUnCampoNoReportadoPorElServidorNoEsUnDesajuste(): void
    {
        // Tratar la ausencia como desajuste haría fallar el arranque de todo servicio contra
        // una versión de NATS que reporte campos ligeramente distintos.
        //
        // `replaceConsumerConfig` es imprescindible aquí: sin él, el doble fusionaría el
        // override sobre la config solicitada y devolvería los campos igualmente, así que la
        // rama de `assertConfigHonored()` que ignora los ausentes nunca se ejercitaría y este
        // test pasaría sin probar nada.
        $this->transport->replaceConsumerConfig = true;
        $this->transport->consumerConfigOverride = ['max_deliver' => Protocol::MAX_DELIVER];

        $subscription = $this->bus()->subscribe(self::SUBJECT, static fn () => null);

        self::assertSame('facturacion-api__pedidos_pedido_v1_creado', $subscription->durable);
    }

    public function testUnAckWaitReportadoDistintoSiEsUnDesajusteAunqueFalteElResto(): void
    {
        // El complemento del test anterior: ignorar lo ausente no puede convertirse en
        // ignorarlo todo. Si el servidor SÍ reporta `ack_wait` y no coincide, falla.
        $this->transport->replaceConsumerConfig = true;
        $this->transport->consumerConfigOverride = ['ack_wait' => Protocol::nanos(1000)];

        $this->expectException(ConsumerConfigMismatchException::class);

        $this->bus()->subscribe(self::SUBJECT, static fn () => null);
    }

    // ─── consumo: camino feliz ────────────────────────────────────────────────

    public function testUnHandlerQueDevuelveNormalmenteHaceAck(): void
    {
        $visto = null;
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, function (FluxEvent $e, HandlerContext $ctx) use (&$visto): void {
            $visto = $e;
            $ctx->ack();
        });

        $this->deliver(ProtocolFixture::event());
        self::assertSame(1, $bus->run(stopWhenIdle: true));

        self::assertInstanceOf(FluxEvent::class, $visto);
        self::assertSame(['+ACK'], $this->transport->ackTokens());
    }

    public function testElHandlerRecibeElNumeroDeEntrega(): void
    {
        $attempt = null;
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, function (FluxEvent $e, HandlerContext $ctx) use (&$attempt): void {
            $attempt = $ctx->attempt;
        });

        $this->deliver(ProtocolFixture::event(), attempt: 4);
        $bus->run(stopWhenIdle: true);

        self::assertSame(4, $attempt);
    }

    public function testElWorkInProgressSeEmiteBajoDemanda(): void
    {
        // En PHP no hay concurrencia en el proceso: entre que el handler empieza y termina,
        // el SDK no recupera el control y no puede emitir WIP por su cuenta. Ver README.
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function (FluxEvent $e, HandlerContext $ctx): void {
            $ctx->workInProgress();
        });

        $this->deliver(ProtocolFixture::event());
        $bus->run(stopWhenIdle: true);

        self::assertSame([Ack::WIP, Ack::ACK], $this->transport->ackTokens());
    }

    public function testFiltraPorTenantSinTocarLaDlq(): void
    {
        $llamado = false;
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function () use (&$llamado): void {
            $llamado = true;
        }, tenantId: 'otro-tenant');

        $this->deliver(ProtocolFixture::event()); // tenantid = acme
        $bus->run(stopWhenIdle: true);

        self::assertFalse($llamado);
        self::assertSame([Ack::ACK], $this->transport->ackTokens());
        self::assertSame([], $this->transport->publishedTo(self::DLQ));
    }

    // ─── consumo: errores ─────────────────────────────────────────────────────

    public function testUnRetryableHaceNakConElBackoffCanonico(): void
    {
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function (): void {
            throw new RetryableError('proveedor 503');
        });

        $this->deliver(ProtocolFixture::event(), attempt: 1);
        $bus->run(stopWhenIdle: true);

        self::assertSame([Ack::NAK], $this->transport->ackTokens());
        self::assertSame(
            Ack::nakWithDelay(Protocol::BACKOFF_MS[0]),
            $this->transport->acks[0]['payload'],
        );
        self::assertSame([], $this->transport->publishedTo(self::DLQ));
    }

    public function testUnRetryableConRetryAfterUsaEseRetraso(): void
    {
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function (): void {
            throw new RetryableError('proveedor 503', retryAfterMs: 5000);
        });

        $this->deliver(ProtocolFixture::event());
        $bus->run(stopWhenIdle: true);

        self::assertSame(Ack::nakWithDelay(5000), $this->transport->acks[0]['payload']);
    }

    public function testUnRetryableAgotadoAcabaEnLaDlqConSeisIntentos(): void
    {
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function (): void {
            throw new RetryableError('proveedor 503');
        });

        $this->deliver(ProtocolFixture::event(), attempt: Protocol::MAX_DELIVER);
        $bus->run(stopWhenIdle: true);

        $dead = $this->deadLetter();
        self::assertSame('retryable', $dead['dlqreason']);
        self::assertSame(6, $dead['dlqattempts']);
        self::assertSame([Ack::TERM], $this->transport->ackTokens());
    }

    public function testUnPermanentVaALaDlqSinGastarReintentos(): void
    {
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function (): void {
            throw new PermanentError('pedido ya cancelado', 'PEDIDO_YA_CANCELADO');
        });

        $this->deliver(ProtocolFixture::event(), attempt: 1);
        $bus->run(stopWhenIdle: true);

        $dead = $this->deadLetter();
        self::assertSame('permanent', $dead['dlqreason']);
        self::assertSame(1, $dead['dlqattempts']);
        self::assertStringStartsWith('PEDIDO_YA_CANCELADO:', $dead['dlqerror']);
        self::assertSame('facturacion-api__pedidos_pedido_v1_creado', $dead['dlqconsumer']);
        self::assertSame([Ack::TERM], $this->transport->ackTokens());
    }

    public function testDlqattemptsRegistraLaEntregaEnQueMurioNoLaClase(): void
    {
        // Un PERMANENT lanzado en la 3ª entrega registra 3, y eso es información útil, no
        // una incoherencia — 04-errors.md §3.
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function (): void {
            throw new PermanentError('regla de negocio');
        });

        $this->deliver(ProtocolFixture::event(), attempt: 3);
        $bus->run(stopWhenIdle: true);

        self::assertSame(3, $this->deadLetter()['dlqattempts']);
    }

    public function testElEventoDeDlqConservaElOriginalIntegro(): void
    {
        $original = ProtocolFixture::event(['aggregateId' => 'ped-123']);

        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function (): void {
            throw new PermanentError('x');
        });

        $this->deliver($original);
        $bus->run(stopWhenIdle: true);

        // Sin wrapper: el replay es quitar las `dlq*` y republicar — 04-errors.md §3.
        $dead = Envelope::parse($this->transport->publishedTo(self::DLQ)[0]['payload']);
        self::assertEquals($original, Envelope::stripDlqExtensions($dead));
    }

    // ─── el presupuesto acotado de lo desconocido ─────────────────────────────

    public function testUnErrorDesconocidoReintentaUnaVezYSeRinde(): void
    {
        // 2 entregas, ~30 s: el transitorio se recupera en el segundo intento y el
        // sistemático llega a la DLQ sin atascar la cola 51 minutos — 04-errors.md §2.1.
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function (): void {
            throw new \RuntimeException('algo que nadie previó');
        });

        $this->deliver(ProtocolFixture::event(), attempt: 1);
        $bus->run(stopWhenIdle: true);
        self::assertSame([Ack::NAK], $this->transport->ackTokens());
        self::assertSame([], $this->transport->publishedTo(self::DLQ));

        $this->deliver(ProtocolFixture::event(), attempt: 2);
        $bus->run(stopWhenIdle: true);
        self::assertSame([Ack::NAK, Ack::TERM], $this->transport->ackTokens());
        self::assertSame('retryable', $this->deadLetter()['dlqreason']);
        self::assertSame(2, $this->deadLetter()['dlqattempts']);
    }

    public function testElPresupuestoAcotadoNoRecortaUnRetryableReconocido(): void
    {
        // El mismo intento nº 2, pero con un error reconocido como transitorio: conserva
        // sus 6 entregas. El presupuesto es por ERROR, no por consumidor.
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function (): void {
            throw new RetryableError('ECONNRESET', 'ECONNRESET');
        });

        $this->deliver(ProtocolFixture::event(), attempt: 2);
        $bus->run(stopWhenIdle: true);

        self::assertSame([Ack::NAK], $this->transport->ackTokens());
        self::assertSame([], $this->transport->publishedTo(self::DLQ));
    }

    public function testLaPoliticaDeLoDesconocidoEsConfigurable(): void
    {
        $bus = $this->bus(new ClassifierOptions(unknownErrorPolicy: 'permanent'));
        $bus->subscribe(self::SUBJECT, static function (): void {
            throw new \RuntimeException('x');
        });

        $this->deliver(ProtocolFixture::event(), attempt: 1);
        $bus->run(stopWhenIdle: true);

        self::assertSame([Ack::TERM], $this->transport->ackTokens());
        self::assertSame('permanent', $this->deadLetter()['dlqreason']);
    }

    // ─── POISON ───────────────────────────────────────────────────────────────

    public function testUnMensajeIlegibleNoLlegaAlHandler(): void
    {
        $llamado = false;
        $alertas = [];

        $bus = $this->bus(onPoison: function (string $subject, \Throwable $e) use (&$alertas): void {
            $alertas[] = $e->getMessage();
        });
        $bus->subscribe(self::SUBJECT, static function () use (&$llamado): void {
            $llamado = true;
        });

        $this->transport->deliver(self::SUBJECT, '{no soy json');
        $bus->run(stopWhenIdle: true);

        self::assertFalse($llamado, 'el handler nunca ve un POISON — 04-errors.md §1.3');
        self::assertCount(1, $alertas);
        self::assertSame([Ack::TERM], $this->transport->ackTokens());
    }

    public function testElPoisonSeGuardaEnvueltoEnUnEventoValido(): void
    {
        // La alternativa —publicar los bytes crudos— haría que la propia DLQ contuviese
        // mensajes que ninguna herramienta del ecosistema puede leer.
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static fn () => null);

        $this->transport->deliver(self::SUBJECT, '{no soy json');
        $bus->run(stopWhenIdle: true);

        $dead = Envelope::parse($this->transport->publishedTo(self::DLQ)[0]['payload']);
        self::assertSame('poison', $dead->dlqreason);
        self::assertSame(self::SUBJECT, $dead->data['originalSubject']);
        self::assertSame('{no soy json', base64_decode($dead->data['rawBase64'], true));
    }

    public function testUnaExtensionObligatoriaAusenteEsPoisonYNoLlegaAlHandler(): void
    {
        $llamado = false;
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function () use (&$llamado): void {
            $llamado = true;
        });

        $raw = ProtocolFixture::rawEvent();
        unset($raw['tenantid']);
        $this->transport->deliver(self::SUBJECT, ProtocolFixture::encode($raw));
        $bus->run(stopWhenIdle: true);

        self::assertFalse($llamado);
        self::assertStringContainsString(
            'MISSING_REQUIRED_EXTENSION',
            Envelope::parse($this->transport->publishedTo(self::DLQ)[0]['payload'])->dlqerror ?? '',
        );
    }

    // ─── Robustez (bugs reales del SDK de Node) ───────────────────────────────

    public function testSiLaPublicacionEnLaDlqFallaNoSeHaceTerm(): void
    {
        // Un `term()` sin registro en la DLQ borra el evento sin dejar rastro. Sin ack ni
        // term, JetStream lo reentrega y el fallo sigue siendo visible.
        $this->transport->failPublishOn = [self::DLQ];

        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function (): void {
            throw new PermanentError('pedido ya cancelado');
        });

        $this->deliver(ProtocolFixture::event());
        $bus->run(stopWhenIdle: true);

        self::assertSame([], $this->transport->ackTokens(), 'ni ack ni term: se reentregará');
    }

    public function testSiLaDlqDelPoisonFallaTampocoSeHaceTerm(): void
    {
        // Un term sin registro borraría la única prueba de que un productor está publicando
        // basura, que es justo lo que el POISON existe para detectar.
        $this->transport->failPublishOn = [self::DLQ];

        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static fn () => null);

        $this->transport->deliver(self::SUBJECT, '{no soy json');
        $bus->run(stopWhenIdle: true);

        self::assertSame([], $this->transport->ackTokens());
    }

    public function testUnFalloInesperadoDelDespachoNoMataElBucle(): void
    {
        // Un consumidor muerto en silencio parece vivo en el healthcheck y no consume nada.
        // Aquí falla el envío del propio ACK, que no es ni un fallo del handler ni algo que
        // el clasificador cubra.
        $this->transport->failRespondTokens = [Ack::ACK];
        $procesados = 0;

        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function () use (&$procesados): void {
            $procesados++;
        });

        $this->deliver(ProtocolFixture::event());
        $this->deliver(ProtocolFixture::event());
        $total = $bus->run(stopWhenIdle: true);

        self::assertSame(2, $procesados, 'el segundo mensaje se procesa pese al fallo del primero');
        self::assertSame(2, $total);
    }

    public function testUnCallbackDeAlertaRotoNoImpideLlegarALaDlq(): void
    {
        $bus = $this->bus(onDlq: static function (): void {
            throw new \LogicException('el sistema de alertas está caído');
        });
        $bus->subscribe(self::SUBJECT, static function (): void {
            throw new PermanentError('x');
        });

        $this->deliver(ProtocolFixture::event());
        $bus->run(stopWhenIdle: true);

        self::assertCount(1, $this->transport->publishedTo(self::DLQ));
        self::assertSame([Ack::TERM], $this->transport->ackTokens());
    }

    // ─── Propagación de contexto ──────────────────────────────────────────────

    public function testUnPublishDentroDelHandlerHeredaLaCorrelacion(): void
    {
        // Si un desarrollador tiene que rellenar `correlationid` a mano, el SDK ha fallado
        // en su único trabajo — 01-envelope.md §5.
        $entrante = ProtocolFixture::event(['aggregateId' => 'ped-123']);

        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function (FluxEvent $e) use ($bus): void {
            $bus->publish('facturacion.factura.v1.emitida', ['facturaId' => 'fac-1']);
        });

        $this->deliver($entrante);
        $bus->run(stopWhenIdle: true);

        $saliente = Envelope::parse($this->transport->publishedTo('facturacion.factura.v1.emitida')[0]['payload']);

        self::assertSame($entrante->correlationid, $saliente->correlationid);
        // El `id` del entrante es la CAUSA del saliente.
        self::assertSame($entrante->id, $saliente->causationid);
        // El evento derivado pertenece al tenant del que lo causó, no al del servicio.
        self::assertSame($entrante->tenantid, $saliente->tenantid);
    }

    public function testElContextoSeRestauraAunqueElHandlerLance(): void
    {
        // Si no, el contexto quedaría pegado al proceso y el siguiente evento heredaría la
        // correlación del anterior — un fallo silencioso y muy difícil de rastrear.
        $bus = $this->bus();
        $bus->subscribe(self::SUBJECT, static function (): void {
            throw new PermanentError('x');
        });

        $this->deliver(ProtocolFixture::event());
        $bus->run(stopWhenIdle: true);

        self::assertNull(EventContext::current());
    }

    // ─── ciclo de vida ────────────────────────────────────────────────────────

    public function testCloseParaLasSuscripcionesYCierraElTransporte(): void
    {
        $bus = $this->bus();
        $subscription = $bus->subscribe(self::SUBJECT, static fn () => null);

        $bus->close();

        self::assertFalse($subscription->active);
        self::assertFalse($bus->isConnected());
    }

    // ─── Utilidades ───────────────────────────────────────────────────────────

    private function bus(
        ?ClassifierOptions $classifier = null,
        ?\Closure $onPoison = null,
        ?\Closure $onDlq = null,
    ): FluxBus {
        return FluxBus::connect(
            new ConnectOptions(
                service: 'facturacion-api',
                environment: 'produccion',
                version: '3.4.1',
                tenantId: 'acme',
                classification: 'confidential',
                schemaBaseUrl: 'https://schemas.internal',
                classifier: $classifier,
                onPoison: $onPoison,
                onDlq: $onDlq,
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
            'facturacion-api__pedidos_pedido_v1_creado',
        );
    }

    /** @return array<string,mixed> El último evento escrito en la DLQ, decodificado. */
    private function deadLetter(): array
    {
        $published = $this->transport->publishedTo(self::DLQ);
        self::assertNotEmpty($published, 'no se escribió nada en la DLQ');

        return json_decode(end($published)['payload'], true, 512, JSON_THROW_ON_ERROR);
    }
}
