<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\Classifier;
use Flux\ConnectOptions;
use Flux\Envelope;
use Flux\ErrorClass;
use Flux\FluxBus;
use Flux\FluxEvent;
use Flux\HandlerContext;
use Flux\Metrics\InMemoryMetrics;
use Flux\Transport\Ack;
use Flux\Transport\InMemoryTransport;
use Flux\Validation\SchemaBundle;
use Flux\Validation\SchemaNotFoundError;
use Flux\Validation\SchemaValidationError;
use Flux\Validation\ValidationMode;
use Flux\Validation\ValidationOptions;
use Flux\Validation\Validator;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;

/**
 * Validación L3 — 00-protocol.md §5.
 *
 * Los casos son los mismos que los del SDK de Node (`test/validation.test.ts`), y lo son a
 * propósito: si los siete SDKs no aceptan y rechazan exactamente los mismos payloads, la
 * validación deja de ser un contrato del ecosistema y pasa a ser una opinión de cada
 * lenguaje. Se valida contra el bundle **real** del repositorio por el mismo motivo por el
 * que `ProtocolFixture` lee `protocol.json` en vez de copiarlo.
 */
final class ValidationTest extends TestCase
{
    private const SUBJECT = ProtocolFixture::SUBJECT;

    private static ?SchemaBundle $bundle = null;

    /** El payload que el esquema del repositorio acepta. */
    private const VALIDO = [
        'pedidoId' => 'ped-123',
        'clienteId' => 'cli-987',
        'aggregateVersion' => 1,
        'totalCents' => 9990,
        'moneda' => 'EUR',
        'lineas' => [['sku' => 'ABC-1', 'cantidad' => 2, 'precioUnitarioCents' => 4995]],
    ];

    private InMemoryTransport $transport;

    protected function setUp(): void
    {
        $this->transport = new InMemoryTransport();
    }

    private static function bundle(): SchemaBundle
    {
        return self::$bundle ??= SchemaBundle::fromFile(dirname(__DIR__, 2) . '/schemas/bundle.json');
    }

    private static function uri(): string
    {
        $uri = self::bundle()->uriFor(self::SUBJECT);
        self::assertIsString($uri, 'el bundle debe resolver el subject de ejemplo');

        return $uri;
    }

    /** @param array<string,mixed> $data */
    private static function evento(array $data): FluxEvent
    {
        return ProtocolFixture::event(['data' => $data, 'dataschema' => self::uri()]);
    }

    // ─── el bundle ────────────────────────────────────────────────────────────

    public function testIndexaElSubjectHaciaSuUriDeDataschema(): void
    {
        self::assertMatchesRegularExpression(
            '#^https://schemas\.internal/pedidos/pedido/creado/\d+\.\d+\.\d+\.json$#',
            self::uri(),
        );
    }

    public function testElIdDelEsquemaCoincideConLaClaveDelBundle(): void
    {
        // Si divergen, un `$ref` interno del bundle resolvería a un esquema distinto del que
        // el `dataschema` del evento anuncia — y sin ruido.
        $schema = self::bundle()->schemaFor(self::uri());

        self::assertIsArray($schema);
        self::assertSame(self::uri(), $schema['$id'] ?? null);
    }

    public function testElBundleDeclaraDraft202012(): void
    {
        // El motivo de que este SDK exija opis ^2.4 y no cualquier validador: un validador
        // de draft-07 NO da un error de versión, da `no schema with key or ref`, que no dice
        // nada útil (00-protocol.md §5, nota de implementación).
        $schema = self::bundle()->schemaFor(self::uri());

        self::assertIsArray($schema);
        self::assertSame('https://json-schema.org/draft/2020-12/schema', $schema['$schema'] ?? null);
    }

    // ─── strict ───────────────────────────────────────────────────────────────

    public function testUnPayloadValidoPasa(): void
    {
        $lanzo = null;

        try {
            $this->validator(ValidationMode::Strict)->validate(self::evento(self::VALIDO), self::SUBJECT);
        } catch (\Throwable $e) {
            $lanzo = $e;
        }

        self::assertNull($lanzo, 'el payload de ejemplo debe cumplir el esquema del repositorio: '
            . $lanzo?->getMessage());
    }

    public function testFaltaUnCampoRequeridoLanza(): void
    {
        $sinTotal = self::VALIDO;
        unset($sinTotal['totalCents']);

        $this->expectException(SchemaValidationError::class);

        $this->validator(ValidationMode::Strict)->validate(self::evento($sinTotal), self::SUBJECT);
    }

    public function testTipoIncorrectoLanza(): void
    {
        // El caso que la spec llama el más peligroso: "9990" en vez de 9990. Pasa cualquier
        // revisión humana y rompe toda suma aguas abajo.
        $this->expectException(SchemaValidationError::class);

        $this->validator(ValidationMode::Strict)
            ->validate(self::evento([...self::VALIDO, 'totalCents' => '9990']), self::SUBJECT);
    }

    public function testCampoDesconocidoLanza(): void
    {
        // `additionalProperties: false`. Un campo mal escrito debe fallar, no colarse en
        // silencio y quedarse sin escribir en el consumidor.
        $this->expectException(SchemaValidationError::class);

        $this->validator(ValidationMode::Strict)
            ->validate(self::evento([...self::VALIDO, 'totalCemts' => 9990]), self::SUBJECT);
    }

    public function testPatronIncumplidoLanza(): void
    {
        $this->expectException(SchemaValidationError::class);

        $this->validator(ValidationMode::Strict)
            ->validate(self::evento([...self::VALIDO, 'moneda' => 'euros']), self::SUBJECT);
    }

    /**
     * El requisito explícito de 00-protocol.md §5, y el que la librería incumple por
     * defecto: opis arranca con `maxErrors = 1`.
     */
    public function testReportaTodosLosErroresNoSoloElPrimero(): void
    {
        $tresMal = [...self::VALIDO, 'totalCents' => 'x', 'moneda' => 'euros', 'cantidad' => 1];

        try {
            $this->validator(ValidationMode::Strict)->validate(self::evento($tresMal), self::SUBJECT);
            self::fail('debería haber lanzado');
        } catch (SchemaValidationError $e) {
            // De uno en uno, arreglar un payload con tres campos mal cuesta tres despliegues.
            self::assertGreaterThanOrEqual(
                2,
                count($e->errors),
                'esperaba ≥2 errores, hubo ' . count($e->errors) . ': ' . $e->getMessage(),
            );
        }
    }

    public function testCadaErrorNombraSuRutaDentroDelPayload(): void
    {
        // Sin la ruta, "must match the type: integer" en un payload con veinte campos es una
        // adivinanza. El anidamiento es donde más se nota.
        $anidado = [...self::VALIDO, 'lineas' => [['sku' => '', 'cantidad' => 0, 'precioUnitarioCents' => -1]]];

        try {
            $this->validator(ValidationMode::Strict)->validate(self::evento($anidado), self::SUBJECT);
            self::fail('debería haber lanzado');
        } catch (SchemaValidationError $e) {
            self::assertStringContainsString('/lineas/0/cantidad', $e->getMessage());
            self::assertCount(3, $e->errors, $e->getMessage());
        }
    }

    public function testEsquemaAusenteDelBundleLanzaSchemaNotFound(): void
    {
        $evento = ProtocolFixture::event([
            'data' => self::VALIDO,
            'dataschema' => 'https://schemas.internal/no/existe/1.0.0.json',
        ]);

        $this->expectException(SchemaNotFoundError::class);
        $this->expectExceptionMessageMatches('/bundle-schemas\.mjs/u');

        $this->validator(ValidationMode::Strict)->validate($evento, self::SUBJECT);
    }

    public function testUnPayloadVacioSeValidaComoObjetoNoComoLista(): void
    {
        // `Envelope::serialize()` publica un `data: []` como `{}` porque el payload es un
        // objeto JSON en la raíz. Si el validador viera una LISTA, reportaría un "must match
        // the type: object" que no se corresponde con nada de lo publicado.
        try {
            $this->validator(ValidationMode::Strict)->validate(self::evento([]), self::SUBJECT);
            self::fail('debería haber lanzado: faltan los campos requeridos');
        } catch (SchemaValidationError $e) {
            self::assertStringNotContainsString('type: object', $e->getMessage());
            self::assertStringContainsString('required', $e->getMessage());
        }
    }

    // ─── warn y off ───────────────────────────────────────────────────────────

    public function testWarnRegistraPeroNoLanza(): void
    {
        $logger = new class extends AbstractLogger {
            /** @var list<string> */
            public array $avisos = [];

            /** @param mixed[] $context */
            public function log(mixed $level, string|\Stringable $message, array $context = []): void
            {
                $this->avisos[] = (string) $message;
            }
        };

        $validator = Validator::fromOptions(
            new ValidationOptions(mode: ValidationMode::Warn, bundle: self::bundle()),
            $logger,
        );

        self::assertInstanceOf(Validator::class, $validator);
        $validator->validate(self::evento([...self::VALIDO, 'totalCents' => 'x']), self::SUBJECT);

        self::assertCount(1, $logger->avisos);
        self::assertStringContainsString('no cumple su esquema', $logger->avisos[0]);
    }

    public function testOffNoCompilaNadaPorqueL2NoPagaElCosteDeL3(): void
    {
        self::assertNull(Validator::fromOptions(null));
        self::assertNull(Validator::fromOptions(new ValidationOptions()));
        self::assertNull(Validator::fromOptions(new ValidationOptions(mode: ValidationMode::Off)));
    }

    public function testStrictSinBundleFallaConUnMensajeAccionable(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/bundle-schemas\.mjs/u');

        Validator::fromOptions(new ValidationOptions(mode: ValidationMode::Strict));
    }

    public function testStrictConUnBundleVacioTambienFalla(): void
    {
        // Un bundle vacío es casi siempre un bundle mal generado, y dejarlo pasar
        // convertiría L3 en "no valido nada y no lo digo".
        $this->expectException(\RuntimeException::class);

        Validator::fromOptions(
            new ValidationOptions(mode: ValidationMode::Strict, bundle: new SchemaBundle()),
        );
    }

    // ─── clasificación — 04-errors.md §1.2 ────────────────────────────────────

    public function testUnFalloDeEsquemaSeClasificaPermanent(): void
    {
        // Reintentarlo cinco veces daría exactamente el mismo resultado mientras los eventos
        // sanos esperan detrás.
        $classification = (new Classifier())->classify(
            new SchemaValidationError(self::SUBJECT, self::uri(), ['/totalCents no es entero']),
        );

        self::assertSame(ErrorClass::Permanent, $classification->errorClass);
        self::assertSame('INVALID_SCHEMA', $classification->code);
    }

    public function testUnEsquemaAusenteTambienEsPermanentPeroConSuPropioCodigo(): void
    {
        $classification = (new Classifier())->classify(
            new SchemaNotFoundError(self::SUBJECT, self::uri()),
        );

        self::assertSame(ErrorClass::Permanent, $classification->errorClass);
        // Código distinto: "el productor publica mal" y "falta un esquema en el despliegue"
        // son dos incidentes con dos dueños distintos.
        self::assertSame('SCHEMA_NOT_FOUND', $classification->code);
    }

    // ─── publish ──────────────────────────────────────────────────────────────

    public function testStrictHaceFallarPublishYNoPublicaNada(): void
    {
        $metrics = new InMemoryMetrics();
        $bus = $this->bus(ValidationMode::Strict, metrics: $metrics);

        try {
            $bus->publish(self::SUBJECT, [...self::VALIDO, 'totalCents' => '9990']);
            self::fail('debería haber lanzado');
        } catch (SchemaValidationError) {
            // Lo esencial: el evento malo NO llegó al stream. Una vez dentro no hay forma de
            // retirarlo, y lo encontraría un consumidor de otro equipo la semana que viene.
            self::assertSame([], $this->transport->published);
        }

        self::assertSame(
            1,
            $metrics->counter(
                'flux_events_published_total{subject="' . self::SUBJECT . '",outcome="invalid_schema"}'
            ),
        );
        // Y NO se cuenta como `ok`.
        self::assertNull(
            $metrics->counter(
                'flux_events_published_total{subject="' . self::SUBJECT . '",outcome="ok"}'
            ),
        );
    }

    public function testUnPayloadValidoSePublicaConNormalidad(): void
    {
        $bus = $this->bus(ValidationMode::Strict);
        $bus->publish(self::SUBJECT, self::VALIDO);

        self::assertCount(1, $this->transport->publishedTo(self::SUBJECT));
    }

    public function testWarnPublicaIgualUnPayloadInvalido(): void
    {
        // El modo de la migración: enterarse sin romper a nadie el primer día.
        $bus = $this->bus(ValidationMode::Warn);
        $bus->publish(self::SUBJECT, [...self::VALIDO, 'totalCents' => '9990']);

        self::assertCount(1, $this->transport->publishedTo(self::SUBJECT));
    }

    public function testElBundleResuelveElDataschemaExactoDelSubject(): void
    {
        // Sin schemaBaseUrl ni mapa explícito: si el evento sale con `dataschema`, solo pudo
        // salir del bundle. Es lo que 00-protocol.md §5 llama "el bundle resuelve además el
        // dataschema exacto".
        $bus = FluxBus::connect(
            new ConnectOptions(
                service: 'pedidos-api',
                environment: 'produccion',
                version: '3.4.1',
                tenantId: 'acme',
                validation: new ValidationOptions(
                    mode: ValidationMode::Strict,
                    bundle: self::bundle(),
                ),
            ),
            $this->transport,
        );

        $event = $bus->publish(self::SUBJECT, self::VALIDO);

        self::assertSame(self::uri(), $event->dataschema);
    }

    // ─── consumo ──────────────────────────────────────────────────────────────

    public function testUnEventoInvalidoVaALaDlqComoPermanentSinReintentos(): void
    {
        $metrics = new InMemoryMetrics();
        $bus = $this->bus(ValidationMode::Strict, onConsume: true, metrics: $metrics);

        $llamado = false;
        $bus->subscribe(self::SUBJECT, function () use (&$llamado): void {
            $llamado = true;
        }, durable: 'c');

        $this->deliver([...self::VALIDO, 'totalCents' => '9990']);
        $bus->run(stopWhenIdle: true);

        // El handler NUNCA lo ve: el evento incumple su contrato antes de llegar a la lógica.
        self::assertFalse($llamado);

        // term, no nak: reintentar un payload que no va a cambiar es gastar la cola.
        self::assertSame([Ack::TERM], $this->transport->ackTokens());

        $dlq = $this->transport->publishedTo('dlq.' . self::SUBJECT);
        self::assertCount(1, $dlq);

        /** @var array<string,mixed> $muerto */
        $muerto = json_decode($dlq[0]['payload'], true, 512, JSON_THROW_ON_ERROR);
        self::assertSame('permanent', $muerto['dlqreason']);
        self::assertSame(1, $muerto['dlqattempts']);
        self::assertStringContainsString('INVALID_SCHEMA', (string) $muerto['dlqerror']);

        // `invalid_schema` y no `permanent`: "un productor publica algo que incumple su
        // propio contrato" no es lo mismo que "mi lógica rechazó el evento".
        self::assertSame(
            1,
            $metrics->counter(
                'flux_events_consumed_total{subject="' . self::SUBJECT . '",consumer="c",outcome="invalid_schema"}'
            ),
        );
        self::assertSame(
            1,
            $metrics->counter(
                'flux_events_dlq_total{subject="' . self::SUBJECT
                . '",consumer="c",reason="permanent",code="INVALID_SCHEMA"}'
            ),
        );
    }

    public function testSinOnConsumeNoSeValidaAlConsumir(): void
    {
        // El default. El sitio donde se arregla un contrato roto es el productor; validar al
        // consumir lo hace visible, no resuelto — y no todo el mundo quiere pagarlo.
        $bus = $this->bus(ValidationMode::Strict, onConsume: false);

        $recibido = null;
        $bus->subscribe(self::SUBJECT, function (FluxEvent $e) use (&$recibido): void {
            $recibido = $e;
        }, durable: 'c');

        $this->deliver([...self::VALIDO, 'totalCents' => '9990']);
        $bus->run(stopWhenIdle: true);

        self::assertInstanceOf(FluxEvent::class, $recibido);
        self::assertSame([Ack::ACK], $this->transport->ackTokens());
    }

    // ─── sondeo de num_pending — 08-observability.md §2.3 ─────────────────────

    public function testElSondeoEmiteElGaugeAunqueNoLleguenMensajes(): void
    {
        // El caso que da sentido a la métrica: la cola crece y NO se entrega nada, así que
        // los metadatos no pueden decir nada. Sin sondeo el panel muestra "sin datos", que
        // es indistinguible de un consumidor sano.
        $metrics = new InMemoryMetrics();
        $bus = $this->bus(metrics: $metrics);
        $bus->subscribe(self::SUBJECT, static fn () => null, durable: 'c');

        $this->transport->pendingByDurable['c'] = 1_234;
        $bus->run(stopWhenIdle: true);

        self::assertSame(1, $this->transport->pendingPolls['c'] ?? 0);
        self::assertSame(
            1234.0,
            $metrics->gauge('flux_consumer_pending{subject="' . self::SUBJECT . '",consumer="c"}'),
        );
    }

    public function testUnCeroDelServidorSiSeEmite(): void
    {
        // Cero pendientes es un dato real y hay que publicarlo; lo que no se emite es el
        // `null` de "no lo sabemos".
        $metrics = new InMemoryMetrics();
        $bus = $this->bus(metrics: $metrics);
        $bus->subscribe(self::SUBJECT, static fn () => null, durable: 'c');

        $this->transport->pendingByDurable['c'] = 0;
        $bus->run(stopWhenIdle: true);

        self::assertSame(
            0.0,
            $metrics->gauge('flux_consumer_pending{subject="' . self::SUBJECT . '",consumer="c"}'),
        );
    }

    public function testPendingPollMsCeroDesactivaElSondeo(): void
    {
        $bus = $this->bus(pendingPollMs: 0);
        $bus->subscribe(self::SUBJECT, static fn () => null, durable: 'c');

        $bus->run(stopWhenIdle: true);

        self::assertSame([], $this->transport->pendingPolls);
    }

    public function testElIntervaloSeRespetaEntreVueltasDelBucle(): void
    {
        // Con un intervalo largo, varias vueltas del bucle sondean UNA vez. Sin esto, un
        // worker ocioso convertiría el sondeo en una tormenta de CONSUMER.INFO.
        $bus = $this->bus(pendingPollMs: 60_000);
        $bus->subscribe(self::SUBJECT, static fn () => null, durable: 'c');

        $this->deliver(self::VALIDO);
        $this->deliver(self::VALIDO);
        $bus->run(stopWhenIdle: true);

        self::assertSame(1, $this->transport->pendingPolls['c'] ?? 0);
    }

    public function testUnFalloDelSondeoNoAfectaAlConsumo(): void
    {
        // Es telemetría. Cambiar una métrica ausente por un worker parado sería un pésimo
        // negocio — 08-observability.md §2.3.
        $this->transport->failConsumerPending = true;
        $bus = $this->bus();

        $procesados = 0;
        $bus->subscribe(self::SUBJECT, function () use (&$procesados): void {
            $procesados++;
        }, durable: 'c');

        $this->deliver(self::VALIDO);
        $bus->run(stopWhenIdle: true);

        self::assertSame(1, $procesados);
        self::assertSame([Ack::ACK], $this->transport->ackTokens());
    }

    // ─── utilidades ───────────────────────────────────────────────────────────

    private function validator(ValidationMode $mode): Validator
    {
        $validator = Validator::fromOptions(new ValidationOptions(mode: $mode, bundle: self::bundle()));
        self::assertInstanceOf(Validator::class, $validator);

        return $validator;
    }

    /** @param array<string,mixed> $data */
    private function deliver(array $data): void
    {
        $this->transport->deliver(
            self::SUBJECT,
            Envelope::serialize(self::evento($data)),
            consumer: 'c',
        );
    }

    private function bus(
        ValidationMode $mode = ValidationMode::Off,
        bool $onConsume = false,
        ?InMemoryMetrics $metrics = null,
        int $pendingPollMs = 15_000,
    ): FluxBus {
        return FluxBus::connect(
            new ConnectOptions(
                service: 'facturacion-api',
                environment: 'produccion',
                version: '3.4.1',
                tenantId: 'acme',
                schemaBaseUrl: 'https://schemas.internal',
                metrics: $metrics,
                validation: $mode === ValidationMode::Off && !$onConsume
                    ? null
                    : new ValidationOptions($mode, self::bundle(), $onConsume),
                pendingPollMs: $pendingPollMs,
            ),
            $this->transport,
        );
    }
}
