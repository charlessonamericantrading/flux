<?php

declare(strict_types=1);

namespace Flux\Tests\Integration;

use Flux\CodedError;
use Flux\ConnectOptions;
use Flux\ConsumerConfigMismatchException;
use Flux\FluxBus;
use Flux\Metrics\InMemoryMetrics;
use Flux\NotProvisionedException;
use Flux\PermanentError;
use Flux\PermissionDeniedException;
use Flux\PoisonError;
use Flux\StateSubjectMismatchException;
use Flux\State\StateEntry;
use Flux\TenantIsolation;
use Flux\TenantIsolationException;
use Flux\Transport\SocketTransport;
use Flux\Validation\SchemaBundle;
use Flux\Validation\SchemaValidationError;
use Flux\Validation\ValidationMode;
use Flux\Validation\ValidationOptions;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;

/**
 * El estado replicado contra un `nats-server` REAL con TLS, nkeys y las ACLs que genera `gen-acl.mjs` para el manifiesto del ejemplo
 * (`crm-api` dueño de `clientes`; `tienda-web`, observador solo de acme; `informes-api`, observador de todos los tenants), aprovisionado con
 * `flux provision`. Es el ciclo del criterio de aceptación (PLAN §2.7, 10-state.md §10.6):
 *
 *   el dueño escribe 100 claves → un observador NUEVO las recibe todas y `onSynced` → se borra una → recibe la lápida → `get` da `null`
 *
 * y, **en los dos sentidos**, que quitar un permiso SE NOTA: cada permiso del dueño y del observador se retira de uno en uno (usuarios
 * `--variante`) y el SDK falla de forma visible y con el error que toca, o —en los dos casos en que el broker calla— con el aviso que promete.
 *
 * `watch` solo REGISTRA y abre el durable; el consumo lo conduce `run()`/`poll()`. Aquí se conduce con `poll()` en el mismo proceso.
 */
#[Group('integration')]
final class StateIntegrationTest extends BrokerTestCase
{
    private const C = 'clientes.cliente.v1';
    private const STREAM = 'STATE_CLIENTES';
    private const SECRETO = 'ana.secreta@example.com';

    /** alias → [servicio, lo que se quita] */
    private const VARIANTES_OBSERVADOR = [
        'tw-info' => '$JS.API.CONSUMER.INFO.STATE_CLIENTES',
        'tw-next' => '$JS.API.CONSUMER.MSG.NEXT.STATE_CLIENTES',
        'tw-ack' => '$JS.ACK.STATE_CLIENTES',
        'tw-direct' => '$JS.API.DIRECT.GET.STATE_CLIENTES',
        'tw-dlqinfo' => '$JS.API.STREAM.INFO.DLQ_CLIENTES',
        'tw-dlqpub' => 'dlq.state.clientes',
    ];
    private const VARIANTES_DUENO = [
        'cr-put' => 'state.clientes.>',
        'cr-get' => '$JS.API.DIRECT.GET.STATE_CLIENTES',
    ];

    private SocketTransport $ops;

    public static function setUpBeforeClass(): void
    {
        $args = [];
        foreach (self::VARIANTES_OBSERVADOR as $alias => $quita) {
            array_push($args, '--variante', "{$alias}:tienda-web:{$quita}");
        }
        foreach (self::VARIANTES_DUENO as $alias => $quita) {
            array_push($args, '--variante', "{$alias}:crm-api:{$quita}");
        }
        self::$broker = SecureBroker::start($args);
    }

    public static function tearDownAfterClass(): void
    {
        self::$broker->stop();
    }

    protected function setUp(): void
    {
        $this->ops = $this->ops();
        $this->limpiar();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->ops->close();
    }

    // ═══ El ciclo de aceptación ═══════════════════════════════════════════════════════════════════

    public function testCicloElDuenoEscribeCienClavesUnObservadorNuevoLasRecibeYLaLapidaLasSustituye(): void
    {
        $logger = new ArrayLogger();
        $dueno = $this->bus('crm-api', ['tenantId' => 'acme']);
        $llaves = [];
        for ($i = 1; $i <= 100; $i++) {
            $llaves[] = sprintf('cli-%03d', $i);
        }

        // 1. el dueño escribe 100 claves
        foreach ($llaves as $k) {
            self::assertSame('ok', $dueno->state->put(self::C, $k, self::dato($k), classification: 'confidential')->outcome);
        }
        // y una del OTRO tenant con el mismo nombre de clave: no se pisan, y no debe llegar al observador de acme
        $dueno->state->put(self::C, 'cli-001', self::dato('cli-001', 5), tenantId: 'globex');
        self::assertSame(1, $dueno->state->get(self::C, 'cli-001')->aggregateVersion);
        self::assertSame(5, $dueno->state->get(self::C, 'cli-001', tenantId: 'globex')->aggregateVersion);

        // 2. un observador NUEVO: recibe la foto completa y `onSynced`
        $this->provisionar('tienda-web', 'acme');
        $nueva = $this->bus('tienda-web', ['tenantId' => 'acme'], null, $logger);
        $recibidas = [];
        $sincronizado = [];
        $watch = $nueva->state->watch(
            self::C,
            function (StateEntry $e) use (&$recibidas): void {
                $recibidas[] = $e;
            },
            onSynced: function () use (&$recibidas, &$sincronizado): void {
                $sincronizado[] = count($recibidas);
            },
        );
        self::assertSame('tienda-web__state_clientes_cliente_v1__acme', $watch->durable);
        self::assertTrue($this->hasta($nueva, function () use (&$recibidas, &$sincronizado): bool { return count($recibidas) >= 100 && $sincronizado !== []; }, 30), 'recibió ' . count($recibidas));
        $nueva->state->poll(300); // por si llegara algo de más

        self::assertCount(100, $recibidas, 'ni una menos ni la de globex');
        self::assertEqualsCanonicalizing($llaves, array_map(static fn (StateEntry $e): string => $e->key, $recibidas));
        foreach ($recibidas as $e) {
            self::assertSame('acme', $e->tenantId);
            self::assertFalse($e->deleted);
            self::assertSame(1, $e->aggregateVersion);
            self::assertSame($e->key, $e->data['clienteId']);
            self::assertGreaterThan(0, $e->revision);
        }
        self::assertSame([100], $sincronizado, 'onSynced se invoca UNA vez, y cuando el handler ya procesó TODA la foto');

        // 3. se borra una: el observador (ya en vivo) recibe la lápida
        $antes = count($recibidas);
        self::assertSame('ok', $dueno->state->delete(self::C, 'cli-042', 2)->outcome);
        self::assertTrue($this->hasta($nueva, function () use (&$recibidas, $antes): bool { return count($recibidas) > $antes; }, 10), 'no llegó la lápida');
        $lapida = $recibidas[count($recibidas) - 1];
        self::assertSame('cli-042', $lapida->key);
        self::assertTrue($lapida->deleted);
        self::assertNull($lapida->data, 'la lápida no lleva dato');
        self::assertSame(2, $lapida->aggregateVersion);
        self::assertSame('acme', $lapida->tenantId);
        self::assertSame([100], $sincronizado, 'ni una segunda vez');

        // 4. `get` de esa clave da null (igual que si nunca hubiera existido); las demás siguen ahí
        self::assertNull($nueva->state->get(self::C, 'cli-042'));
        self::assertNull($dueno->state->get(self::C, 'cli-042'));
        self::assertSame(1, $nueva->state->get(self::C, 'cli-043')->aggregateVersion);
        self::assertNull($nueva->state->get(self::C, 'no-existe'));

        // 5. y la lápida SUSTITUYÓ al valor: el dato personal ya no está en el stream
        $guardado = $this->enElStream('state.clientes.cliente.v1.acme.cli-042');
        self::assertSame('com.flux.state.clientes.cliente.deleted.v1', $guardado['type']);
        self::assertStringNotContainsString(self::SECRETO, json_encode($guardado));
        self::assertSame(101, $this->ops->api('$JS.API.STREAM.INFO.' . self::STREAM)['state']['messages'], '100 claves de acme (una ya es lápida) + 1 de globex: un mensaje por clave');

        // 6. un observador que llega DESPUÉS recibe la lápida en su foto (A-35): `informes-api` observa todos los tenants
        $this->provisionar('informes-api');
        $informes = $this->bus('informes-api', ['tenantId' => null], null, $logger);
        $foto = [];
        $sync2 = 0;
        $informes->state->watch(self::C, function (StateEntry $e) use (&$foto): void {
            $foto[] = $e;
        }, onSynced: function () use (&$sync2): void {
            $sync2++;
        });
        self::assertTrue($this->hasta($informes, function () use (&$foto, &$sync2): bool { return count($foto) >= 101 && $sync2 >= 1; }, 30), 'la foto tardía tiene ' . count($foto));
        self::assertCount(101, $foto, '100 de acme (con la lápida) y 1 de globex');
        self::assertCount(1, array_filter($foto, static fn (StateEntry $e): bool => $e->deleted));
        self::assertSame([], $logger->messages(), 'ningún aviso del SDK en todo el ciclo');
    }

    public function testLasMetricasDeEscrituraCuentanOkStaleYInvalidSchemaSinTenantNiClave(): void
    {
        $metricas = new InMemoryMetrics();
        $dueno = $this->bus('crm-api', [
            'tenantId' => 'acme', 'metrics' => $metricas,
            'validation' => new ValidationOptions(ValidationMode::Strict, SchemaBundle::fromFile(dirname(__DIR__, 3) . '/schemas/bundle.json')),
        ]);
        $dueno->state->put(self::C, 'cli-1', self::dato('cli-1', 2));
        $dueno->state->put(self::C, 'cli-1', self::dato('cli-1', 1)); // stale: no es un error
        $dueno->state->put(self::C, 'cli-1', self::dato('cli-1', 2)); // una versión IGUAL tampoco avanza
        self::assertInstanceOf(SchemaValidationError::class, self::capturar(fn () => $dueno->state->put(self::C, 'cli-2', ['email' => 'x'] + self::dato('cli-2'))));
        self::assertSame(1, $metricas->counter('flux_state_writes_total{contract="clientes.cliente.v1",outcome="ok"}'));
        self::assertSame(2, $metricas->counter('flux_state_writes_total{contract="clientes.cliente.v1",outcome="stale"}'));
        self::assertSame(1, $metricas->counter('flux_state_writes_total{contract="clientes.cliente.v1",outcome="invalid_schema"}'));
        foreach (explode("\n", $metricas->render()) as $linea) {
            if (str_starts_with($linea, 'flux_state_writes_total')) {
                self::assertStringNotContainsString('acme', $linea);
                self::assertStringNotContainsString('cli-1', $linea);
            }
        }
    }

    public function testTresConflictosSeguidosSonStateConflictQueEsReintentableYNoEscribeNada(): void
    {
        // Un rival escribe entre la lectura y la publicación de CADA intento: el SDK relee, ve una versión mayor... y el rival vuelve a adelantarse.
        // Tras tres intentos, `STATE_CONFLICT`, RETRYABLE (la aplicación puede reintentar), y la escritura del llamante NO se aplicó.
        $options = $this->options('crm-api');
        $hook = new HookedStateTransport(SocketTransport::connect($options));
        $propio = FluxBus::connect($options, $hook);
        $rival = $this->bus('crm-api');
        $propio->state->put(self::C, 'k', self::dato('k', 1)); // la clave existe: la secuencia esperada es la de ese mensaje
        $hook->resetCounters();
        $hook->beforePublish = static function (int $n) use ($rival): void {
            // las versiones del rival (2, 3, 4) no avanzan a las del llamante porque el llamante escribe la 100: lo que falla es la SECUENCIA
            $rival->state->put(self::C, 'k', self::dato('k', 1 + $n), tenantId: 'acme');
        };
        $e = self::capturar(fn () => $propio->state->put(self::C, 'k', self::dato('k', 100)));
        self::assertInstanceOf(CodedError::class, $e, $e === null ? 'put devolvió' : $e::class . ': ' . $e->getMessage());
        self::assertSame('STATE_CONFLICT', $e->errorCode);
        self::assertSame(\Flux\ErrorClass::Retryable, $e->fluxClass());
        self::assertSame(3, $hook->publications, 'tres intentos, ni uno más');
        self::assertSame(4, $this->enElStream('state.clientes.cliente.v1.acme.k')['data']['aggregateVersion'], 'lo que quedó es lo del rival: la escritura del llamante no se aplicó');
        $propio->close();
    }

    public function testElSobreQueLlegaAlBrokerLlevaDataElUltimoUtf8LiteralYLosAtributosQueRellenaElSdk(): void
    {
        $dueno = $this->bus('crm-api', ['tenantId' => 'acme']);
        $dueno->state->put(self::C, 'cli-1', self::dato('cli-1', 1, ['nombre' => 'María Peña', 'pais' => 'ES']));
        $raw = $this->ops->directGet(self::STREAM, 'state.clientes.cliente.v1.acme.cli-1');
        self::assertStringContainsString('María Peña', $raw['data']);
        self::assertSame(0, preg_match('/\\\\u[0-9a-fA-F]{4}/', $raw['data']), 'UTF-8 literal, sin escapes \\u');
        $ev = json_decode($raw['data'], true);
        self::assertSame('data', array_key_last($ev));
        self::assertSame('cli-1', $ev['subject']);
        self::assertSame('acme', $ev['tenantid']);
        self::assertSame('cli-1', $ev['partitionkey']);
        self::assertSame('confidential', $ev['dataclassification'], 'sin clasificación, `confidential` (no la de la conexión)');
        self::assertSame('/test/crm-api', $ev['source']);
        // el id viaja también como Nats-Msg-Id y el broker lo conserva en las cabeceras (A-32): es lo que reconoce el paso 1b
        $id = null;
        foreach ($raw['headers'] as $k => $v) {
            if (strcasecmp($k, 'Nats-Msg-Id') === 0) {
                $id = $v;
            }
        }
        self::assertSame($ev['id'], $id);
    }

    // ═══ Tenants: lo hace el BROKER ═══════════════════════════════════════════════════════════════

    public function testUnObservadorSinTenantRecibeLosDosYLaEntradaDiceDeCual(): void
    {
        $dueno = $this->bus('crm-api');
        $dueno->state->put(self::C, 'cli-1', self::dato('cli-1', 1), tenantId: 'acme');
        $dueno->state->put(self::C, 'cli-1', self::dato('cli-1', 7), tenantId: 'globex');
        $this->provisionar('informes-api');
        $logger = new ArrayLogger();
        $t = $this->bus('informes-api', ['tenantId' => null], null, $logger);
        $vistas = [];
        $t->state->watch(self::C, function (StateEntry $e) use (&$vistas): void {
            $vistas[] = "{$e->tenantId}/{$e->key}@{$e->aggregateVersion}";
        });
        self::assertTrue($this->hasta($t, function () use (&$vistas): bool { return count($vistas) >= 2; }, 10), implode(' | ', $logger->messages()) . json_encode($this->ops->api('$JS.API.CONSUMER.INFO.' . self::STREAM . '.informes-api__state_clientes_cliente_v1')));
        sort($vistas);
        self::assertSame(['acme/cli-1@1', 'globex/cli-1@7'], $vistas, 'sin el tenantId de la entrada, una copia local por clave mezclaría los tenants');

        // con `tenantIsolation: strict`, ese mismo watch es un error de configuración
        $estricto = $this->bus('informes-api', ['tenantId' => null, 'tenantIsolation' => TenantIsolation::Strict]);
        $this->expectException(TenantIsolationException::class);
        $estricto->state->watch(self::C, static function (): void {});
    }

    public function testElBrokerConfinaPorTenantYUnObservadorNoEscribe(): void
    {
        $dueno = $this->bus('crm-api');
        $dueno->state->put(self::C, 'cli-1', self::dato('cli-1', 1), tenantId: 'globex');
        $this->provisionar('tienda-web', 'acme');
        $w = $this->bus('tienda-web', ['tenantId' => 'acme']);

        // get de otro tenant: la forma por SUBJECT con el tenant fijado en el permiso lo deniega
        $e = self::capturar(fn () => $w->state->get(self::C, 'cli-1', tenantId: 'globex'));
        self::assertInstanceOf(PermissionDeniedException::class, $e, (string) $e);
        self::assertSame('PERMISSION_DENIED', $e->errorCode);
        self::assertSame('$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.globex.cli-1', $e->subject);
        // watch del durable de otro tenant: CONSUMER.INFO de ese durable no está concedido
        $e = self::capturar(fn () => $w->state->watch(self::C, static function (): void {}, tenantId: 'globex'));
        self::assertInstanceOf(PermissionDeniedException::class, $e, (string) $e);
        self::assertSame('$JS.API.CONSUMER.INFO.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__globex', $e->subject);
        // un observador NO escribe: el broker lo deniega, no el código
        $e = self::capturar(fn () => $w->state->put(self::C, 'cli-9', self::dato('cli-9'), tenantId: 'acme'));
        self::assertInstanceOf(PermissionDeniedException::class, $e, (string) $e);
        self::assertSame('state.clientes.cliente.v1.acme.cli-9', $e->subject);
        self::assertNull($this->enElStream('state.clientes.cliente.v1.acme.cli-9'));
        // y el dueño de OTRO dominio tampoco escribe `clientes`
        $ajeno = $this->bus('pedidos-api', ['tenantId' => 'acme']);
        self::assertInstanceOf(PermissionDeniedException::class, self::capturar(fn () => $ajeno->state->put(self::C, 'cli-9', self::dato('cli-9'))));
        self::assertInstanceOf(PermissionDeniedException::class, self::capturar(fn () => $ajeno->state->get(self::C, 'cli-1')));
        // CONTROL: el mismo observador SÍ lee lo suyo
        $dueno->state->put(self::C, 'cli-1', self::dato('cli-1', 1), tenantId: 'acme');
        self::assertSame('acme', $w->state->get(self::C, 'cli-1')->tenantId);
    }

    public function testLaValidacionPreviaALaRedGanaAUnBrokerQueDenegariaLaPeticion(): void
    {
        // `tienda-web` es un observador: si una escritura llegara al broker, éste la denegaría (PERMISSION_DENIED). Que lo que se lance sea el error
        // de VALIDACIÓN prueba que no se abrió ninguna petición.
        $w = $this->bus('tienda-web', ['tenantId' => 'acme']);
        $codigo = static fn (callable $f): ?string => ($e = self::capturar($f)) instanceof CodedError ? $e->errorCode : ($e === null ? null : $e::class);
        self::assertSame('INVALID_STATE_KEY', $codigo(fn () => $w->state->put(self::C, 'a.b', self::dato('x'))));
        self::assertSame('INVALID_STATE_CONTRACT', $codigo(fn () => $w->state->put('clientes.cliente', 'ok', self::dato('ok'))));
        self::assertSame('INVALID_STATE_CONTRACT', $codigo(fn () => $w->state->get('Clientes.cliente.v1', 'ok')));
        self::assertSame('MISSING_AGGREGATE_VERSION', $codigo(fn () => $w->state->put(self::C, 'ok', ['clienteId' => 'ok'])));
        self::assertSame('MISSING_AGGREGATE_VERSION', $codigo(fn () => $w->state->delete(self::C, 'ok', 0)));
        self::assertSame('INVALID_TENANT_FOR_STATE', $codigo(fn () => $w->state->put(self::C, 'ok', self::dato('ok'), tenantId: 'ACME')));
        self::assertSame('RESERVED_DOMAIN', $codigo(fn () => $w->state->put('state.clientes.v1', 'ok', self::dato('ok'))));
        self::assertSame('RESERVED_DOMAIN', $codigo(fn () => $w->state->get('dlq.cliente.v1', 'ok')));
        self::assertSame('RESERVED_DOMAIN', $codigo(fn () => $w->publish('state.clientes.v1.creado', [])));
        self::assertSame('RESERVED_DOMAIN', $codigo(fn () => $w->publish('system.cosa.v1.hecha', [])));
        // CONTROL (el otro sentido): con todo válido, la MISMA escritura sí llega al broker, y éste la deniega
        self::assertInstanceOf(PermissionDeniedException::class, self::capturar(fn () => $w->state->put(self::C, 'ok', self::dato('ok'))));
    }

    public function testControlElObservadorNoTieneStreamInfoYTodoFuncionaIgual(): void
    {
        $dueno = $this->bus('crm-api');
        $dueno->state->put(self::C, 'cli-1', self::dato('cli-1'), tenantId: 'globex');
        // el broker lo deniega: `STREAM.INFO` del stream de estado lista TODAS las claves y TODOS los tenants (A-39)...
        $crudo = SocketTransport::connect($this->options('tienda-web'));
        try {
            $e = self::capturar(fn () => $crudo->api('$JS.API.STREAM.INFO.' . self::STREAM, ['subjects_filter' => '>'], 1_500));
            self::assertInstanceOf(PermissionDeniedException::class, $e, (string) $e);
            // ...y la forma de CUERPO de `DIRECT.GET`, que lee cualquier mensaje, tampoco
            $e = self::capturar(fn () => $crudo->api('$JS.API.DIRECT.GET.' . self::STREAM, ['seq' => 1], 1_500));
            self::assertInstanceOf(PermissionDeniedException::class, $e, (string) $e);
        } finally {
            $crudo->close();
        }
        // el SDK no lo necesita: put (dueño sin STREAM.INFO), get y watch (observador sin él) funcionan
        $this->provisionar('tienda-web', 'acme');
        $w = $this->bus('tienda-web', ['tenantId' => 'acme']);
        $dueno->state->put(self::C, 'cli-2', self::dato('cli-2'), tenantId: 'acme');
        self::assertSame('cli-2', $w->state->get(self::C, 'cli-2')->key);
        $vistas = [];
        $w->state->watch(self::C, function (StateEntry $e) use (&$vistas): void {
            $vistas[] = $e;
        });
        self::assertTrue($this->hasta($w, function () use (&$vistas): bool { return count($vistas) >= 1; }, 10));
    }

    // ═══ Quitar un permiso SE NOTA ════════════════════════════════════════════════════════════════

    /** @return array<string,array{0:string,1:string}> */
    public static function variantesDelDueno(): array
    {
        $out = [];
        foreach (self::VARIANTES_DUENO as $alias => $quita) {
            $out[$quita] = [$alias, $quita];
        }

        return $out;
    }

    /** @return array<string,array{0:string,1:string}> */
    public static function variantesDelObservador(): array
    {
        $out = [];
        foreach (self::VARIANTES_OBSERVADOR as $alias => $quita) {
            $out[$quita] = [$alias, $quita];
        }

        return $out;
    }

    #[DataProvider('variantesDelDueno')]
    public function testDuenoSinUnPermisoPutFallaDeFormaVisible(string $alias, string $quitado): void
    {
        $bus = $this->bus('crm-api', ['tenantId' => 'acme'], self::$broker->variantSeed($alias));
        // `cr-get` quita la lectura: `put` lee antes de escribir, así que también falla. `cr-put` quita la escritura.
        $e = self::capturar(fn () => $bus->state->put(self::C, 'cli-1', self::dato('cli-1')));
        self::assertInstanceOf(PermissionDeniedException::class, $e, $e === null ? 'put devolvió' : $e::class . ': ' . $e->getMessage());
        self::assertSame('PERMISSION_DENIED', $e->errorCode);
        // lo que falta es justo lo que se quitó (el subject del permiso, con su comodín, cubre el denegado)
        self::assertStringStartsWith(rtrim($quitado, '>'), $e->subject, "se denegó «{$e->subject}» y se quitó «{$quitado}»");
        self::assertNull($this->enElStream('state.clientes.cliente.v1.acme.cli-1'), 'no se escribió nada');
    }

    #[DataProvider('variantesDelObservador')]
    public function testObservadorSinUnPermisoSeNota(string $alias, string $quitado): void
    {
        $dueno = $this->bus('crm-api');
        $dueno->state->put(self::C, 'cli-1', self::dato('cli-1', 1), tenantId: 'acme');
        $durable = $this->provisionar('tienda-web', 'acme');
        $logger = new ArrayLogger();
        $bus = $this->bus('tienda-web', ['tenantId' => 'acme'], self::$broker->variantSeed($alias), $logger);
        $vistas = [];
        $abre = function () use ($bus, &$vistas) {
            return $bus->state->watch(self::C, function (StateEntry $e) use (&$vistas): void {
                $vistas[] = $e;
            });
        };

        if (str_starts_with($quitado, '$JS.API.CONSUMER.INFO.') || str_starts_with($quitado, '$JS.API.CONSUMER.MSG.NEXT.') || str_starts_with($quitado, '$JS.API.STREAM.INFO.DLQ_')) {
            // CONSUMER.INFO y STREAM.INFO.DLQ: peticiones con respuesta → la denegación llega en milisegundos. MSG.NEXT: un publish suelto, sin
            // respuesta → el broker CALLA y el consumidor quedaría inerte, parece sano (A-20); el SDK lo comprueba al abrir y lanza.
            [$ms, $e] = self::medir($abre);
            self::assertInstanceOf(PermissionDeniedException::class, $e, $e === null ? 'watch() devolvió' : $e::class . ': ' . $e->getMessage());
            self::assertSame('PERMISSION_DENIED', $e->errorCode);
            self::assertStringStartsWith($quitado, $e->subject);
            self::assertLessThan(2_000, $ms, 'en milisegundos, no un timeout');
        } elseif (str_starts_with($quitado, '$JS.ACK.')) {
            // El ack es un publish suelto: el evento se procesa y se REENTREGA cada ack_wait, sin fin y sin error (A-19). Lo único observable es
            // el aviso asíncrono del broker: el SDK lo registra.
            $abre();
            self::assertTrue($this->hasta($bus, function () use (&$vistas): bool { return count($vistas) >= 1; }, 10), 'el watch debe seguir entregando');
            self::assertTrue($this->hasta($bus, function () use ($logger, $quitado): bool { return $logger->contains($quitado); }, 5), 'sin aviso del ack denegado: ' . implode(' | ', $logger->messages()));
            $info = $this->ops->api('$JS.API.CONSUMER.INFO.' . self::STREAM . '.' . $durable);
            self::assertSame(1, $info['num_ack_pending'], 'se entregó y nunca se confirmó');
            self::assertLessThan($info['delivered']['stream_seq'], $info['ack_floor']['stream_seq'], 'el suelo de acks no avanzó');
        } elseif (str_contains($quitado, 'DIRECT.GET')) {
            // `get` usa DIRECT.GET; `watch` NO: lo que se quita solo rompe la lectura puntual
            $e = self::capturar(fn () => $bus->state->get(self::C, 'cli-1'));
            self::assertInstanceOf(PermissionDeniedException::class, $e, (string) $e);
            self::assertStringStartsWith('$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.', $e->subject);
            $abre();
            self::assertTrue($this->hasta($bus, function () use (&$vistas): bool { return count($vistas) >= 1; }, 10), 'el watch no necesita DIRECT.GET');
        } elseif (str_starts_with($quitado, 'dlq.state.')) {
            // Publicar en la DLQ: sin el permiso, una entrada que muere NO se pierde —no se termina— y se reentrega.
            $buena = $this->enElStream('state.clientes.cliente.v1.acme.cli-1');
            $this->escribeCrudo('state.clientes.cliente.v1.acme.mala', ['id' => 'sembrado-mala', 'tenantid' => 'globex', 'subject' => 'mala', 'partitionkey' => 'mala'] + $buena);
            $abre();
            self::assertTrue($this->hasta($bus, function () use ($logger): bool { return $logger->contains('no se pudo publicar en la DLQ de estado'); }, 10), 'sin aviso: ' . implode(' | ', $logger->messages()));
            self::assertSame(0, $this->ops->api('$JS.API.STREAM.INFO.DLQ_CLIENTES')['state']['messages'], 'nada llegó a la DLQ');
            self::assertCount(1, $vistas, 'lo bueno sí se entrega');
            self::assertGreaterThanOrEqual(1, $this->ops->api('$JS.API.CONSUMER.INFO.' . self::STREAM . '.' . $durable)['num_ack_pending'], 'lo que no se pudo guardar NO se terminó');
        } else {
            self::fail("permiso sin caso en la matriz: {$quitado}");
        }
    }

    public function testLaMatrizCubreCadaPermisoDelObservadorYDelDueno(): void
    {
        $json = shell_exec('node ' . escapeshellarg(__DIR__ . '/permisos.mjs') . ' crm-api tienda-web 2>/dev/null');
        $perm = json_decode((string) $json, true);
        self::assertIsArray($perm, 'permisos.mjs no dio JSON');
        $cubre = static function (array $publish, array $quitados): array {
            $sinCubrir = [];
            foreach ($publish as $p) {
                // `$JS.API.INFO` es de connect(), y lo prueba SecurityIntegrationTest
                if ($p === '$JS.API.INFO') {
                    continue;
                }
                $hay = array_filter($quitados, static fn (string $q): bool => str_starts_with($p, $q));
                if ($hay === []) {
                    $sinCubrir[] = $p;
                }
            }

            return $sinCubrir;
        };
        // El dueño reparte además permisos de OTRAS capacidades (eventos, la DLQ de eventos, rpc): solo se miden aquí los de estado.
        $duenoEstado = array_values(array_filter($perm['crm-api']['publish'], static fn (string $p): bool => str_contains($p, 'state') && !str_contains($p, 'dlq')));
        self::assertSame([], $cubre($duenoEstado, array_values(self::VARIANTES_DUENO)), 'permisos de estado del dueño sin variante');
        self::assertSame([], $cubre($perm['tienda-web']['publish'], array_values(self::VARIANTES_OBSERVADOR)), 'permisos del observador sin variante');
        self::assertCount(7, $perm['tienda-web']['publish'], 'si el observador gana un permiso, esta matriz debe crecer');
    }

    // ═══ Aprovisionamiento ════════════════════════════════════════════════════════════════════════

    public function testSinElDurableYSinConsumerCreateEsNotProvisionedDeConsumidor(): void
    {
        $w = $this->bus('tienda-web', ['tenantId' => 'acme']);
        [$ms, $e] = self::medir(fn () => $w->state->watch(self::C, static function (): void {}));
        self::assertInstanceOf(NotProvisionedException::class, $e, $e === null ? 'watch() devolvió' : $e::class . ': ' . $e->getMessage());
        self::assertSame('NOT_PROVISIONED', $e->errorCode);
        self::assertSame('consumer', $e->kind);
        self::assertSame('tienda-web__state_clientes_cliente_v1__acme', $e->resource);
        self::assertSame(self::STREAM, $e->stream);
        self::assertStringContainsString('flux provision', $e->getMessage());
        self::assertLessThan(2_000, $ms);
    }

    public function testElStreamDeEstadoNoExisteEsNotProvisionedConLasDosCausas(): void
    {
        $this->ops->api('$JS.API.STREAM.DELETE.' . self::STREAM);
        try {
            $dueno = $this->bus('crm-api', ['tenantId' => 'acme']);
            foreach ([
                fn () => $dueno->state->get(self::C, 'p-1'),
                fn () => $dueno->state->put(self::C, 'p-1', ['clienteId' => 'p-1', 'aggregateVersion' => 1]),
                fn () => $dueno->state->delete(self::C, 'p-1', 2),
            ] as $op) {
                $e = self::capturar($op);
                self::assertInstanceOf(NotProvisionedException::class, $e, $e === null ? 'devolvió' : $e::class . ': ' . $e->getMessage());
                self::assertSame('NOT_PROVISIONED', $e->errorCode);
                foreach (['STATE_CLIENTES', 'no existe, o existe sin', 'allow_direct', 'flux provision', 'flux doctor'] as $texto) {
                    self::assertStringContainsString($texto, $e->getMessage());
                }
            }
            // un watch sobre un contrato sin stream: CONSUMER.INFO contesta «stream not found» (10059), que se distingue de «consumer not found»
            // (10014) sin STREAM.INFO (A-48)
            $w = $this->bus('tienda-web', ['tenantId' => 'acme']);
            $e = self::capturar(fn () => $w->state->watch(self::C, static function (): void {}));
            self::assertInstanceOf(NotProvisionedException::class, $e, $e === null ? 'devolvió' : $e::class . ': ' . $e->getMessage());
            self::assertStringContainsString('STATE_CLIENTES', $e->getMessage());
        } finally {
            // lo que `flux provision` crea para un dominio con estado (10-state.md §2.1)
            $r = $this->ops->api('$JS.API.STREAM.CREATE.' . self::STREAM, [
                'name' => self::STREAM, 'subjects' => ['state.clientes.>'], 'storage' => 'file', 'max_msgs_per_subject' => 1, 'allow_direct' => true,
                'max_age' => 0, 'discard' => 'old', 'duplicate_window' => 120_000_000_000, 'num_replicas' => 1,
            ]);
            self::assertArrayNotHasKey('error', $r, json_encode($r));
        }
    }

    public function testElStreamExisteSinAllowDirectSeVeExactamenteIgualQueUnoQueNoExiste(): void
    {
        $config = [
            'name' => self::STREAM, 'subjects' => ['state.clientes.>'], 'storage' => 'file', 'max_msgs_per_subject' => 1, 'allow_direct' => true,
            'max_age' => 0, 'discard' => 'old', 'duplicate_window' => 120_000_000_000, 'num_replicas' => 1,
        ];
        $r = $this->ops->api('$JS.API.STREAM.UPDATE.' . self::STREAM, ['allow_direct' => false] + $config);
        self::assertArrayNotHasKey('error', $r, json_encode($r));
        try {
            $dueno = $this->bus('crm-api', ['tenantId' => 'acme']);
            $e = self::capturar(fn () => $dueno->state->get(self::C, 'k'));
            self::assertInstanceOf(NotProvisionedException::class, $e, $e === null ? 'devolvió' : $e::class . ': ' . $e->getMessage());
            self::assertStringContainsString('allow_direct', $e->getMessage());
            self::assertInstanceOf(NotProvisionedException::class, self::capturar(fn () => $dueno->state->put(self::C, 'k', ['clienteId' => 'k', 'aggregateVersion' => 1])));
        } finally {
            $this->ops->api('$JS.API.STREAM.UPDATE.' . self::STREAM, $config);
        }
        // CONTROL: un stream con allow_direct SÍ responde (404 → null)
        $dueno = $this->bus('crm-api', ['tenantId' => 'acme']);
        self::assertNull($dueno->state->get(self::C, 'no-existe'));
    }

    public function testUnDurableConOtraPoliticaOOtroFiltroNoSeAceptaConsumerConfigMismatch(): void
    {
        // deliver_policy: all (el broker entregaría lo mismo en este stream, pero la spec dice last_per_subject)
        $this->provisionar('tienda-web', 'acme', ['deliver_policy' => 'all']);
        $w = $this->bus('tienda-web', ['tenantId' => 'acme']);
        $e = self::capturar(fn () => $w->state->watch(self::C, static function (): void {}));
        self::assertInstanceOf(ConsumerConfigMismatchException::class, $e, $e === null ? 'devolvió' : $e::class . ': ' . $e->getMessage());
        self::assertContains('deliver_policy', array_column($e->differences, 'field'));
        // un filtro de TODOS los tenants en un durable que dice ser de acme: entregaría lo de globex sin error
        $this->provisionar('tienda-web', 'acme', ['filter_subject' => 'state.' . self::C . '.*.*']);
        $e = self::capturar(fn () => $w->state->watch(self::C, static function (): void {}));
        self::assertInstanceOf(ConsumerConfigMismatchException::class, $e);
        self::assertContains('filter_subject', array_column($e->differences, 'field'));
        // CONTROL: bien aprovisionado, abre
        $this->provisionar('tienda-web', 'acme');
        self::assertSame('tienda-web__state_clientes_cliente_v1__acme', $w->state->watch(self::C, static function (): void {})->durable);
    }

    public function testSinElTenantEnConnectNiEnLaLlamadaNoSeEscribeEnSystem(): void
    {
        $dueno = $this->bus('crm-api', ['tenantId' => null]);
        foreach ([
            fn () => $dueno->state->put(self::C, 'cli-1', self::dato('cli-1')),
            fn () => $dueno->state->get(self::C, 'cli-1'),
        ] as $op) {
            $e = self::capturar($op);
            self::assertInstanceOf(CodedError::class, $e, $e === null ? 'devolvió' : $e::class);
            self::assertSame('INVALID_TENANT_FOR_STATE', $e->errorCode);
        }
        self::assertSame(0, $this->ops->api('$JS.API.STREAM.INFO.' . self::STREAM)['state']['messages'], 'nada se escribió en `system`');
    }

    // ═══ Entradas rotas, DLQ de estado y PII ══════════════════════════════════════════════════════

    public function testDlqDeEstadoLoPoisonYLoPermanentMuerenSinElDatoYElDuenoLoLeeConSuDurable(): void
    {
        $dueno = $this->bus('crm-api', ['tenantId' => 'acme']);
        $dueno->state->put(self::C, 'bueno-1', self::dato('bueno-1'));
        $bueno = $this->enElStream('state.clientes.cliente.v1.acme.bueno-1');
        $sembrar = fn (string $k, array $cambios): int => $this->escribeCrudo(
            "state.clientes.cliente.v1.acme.{$k}",
            // cada una con SU id: el `Nats-Msg-Id` de la DLQ es `dlq-<id>-<consumer>`, y dos con el mismo id se deduplicarían
            $cambios + ['id' => "sembrado-{$k}", 'subject' => $k, 'partitionkey' => $k, 'data' => self::dato($k)] + $bueno,
        );
        $revMal = $sembrar('mal-1', ['tenantid' => 'globex']); // el sobre MIENTE sobre su tenant
        $revRoto = $this->escribeCrudo('state.clientes.cliente.v1.acme.roto-1', '{"esto no es un sobre": "' . self::SECRETO . '"'); // JSON roto
        $revRestringido = $sembrar('restringido-1', ['dataclassification' => 'restricted']);
        $dueno->state->put(self::C, 'err-tipado', self::dato('err-tipado'));
        $dueno->state->put(self::C, 'err-plano', self::dato('err-plano'));
        $dueno->state->put(self::C, 'bueno-2', self::dato('bueno-2'));

        $this->provisionar('tienda-web', 'acme');
        $metricas = new InMemoryMetrics();
        $logger = new ArrayLogger();
        $poison = [];
        $web = $this->bus('tienda-web', [
            'tenantId' => 'acme',
            'metrics' => $metricas,
            'onPoison' => function (string $subject, PoisonError $e, string $raw) use (&$poison): void {
                $poison[] = [$subject, $e, $raw];
            },
            'classifier' => new \Flux\ClassifierOptions(unknownErrorPolicy: 'permanent'), // un error sin clasificar va a la DLQ ya, sin esperar 30 s
        ], null, $logger);
        $aplicadas = [];
        $web->state->watch(self::C, function (StateEntry $e) use (&$aplicadas): void {
            if ($e->key === 'err-tipado') {
                throw new PermanentError('país fuera del catálogo', 'PAIS_NO_SOPORTADO');
            }
            if ($e->key === 'err-plano') {
                throw new \RuntimeException('duplicate key value violates unique constraint (' . self::SECRETO . ')');
            }
            $aplicadas[] = $e->key;
        });
        $dlq = fn (): int => $this->ops->api('$JS.API.STREAM.INFO.DLQ_CLIENTES')['state']['messages'];
        self::assertTrue($this->hasta($web, function () use ($dlq): bool { return $dlq() >= 5; }, 20), 'deben llegar 5 a la DLQ: ' . $dlq() . ' ' . implode(' | ', $logger->messages()));
        $web->state->poll(300);

        // 3. lo que NO se aplicó, y lo que sí
        sort($aplicadas);
        self::assertSame(['bueno-1', 'bueno-2'], $aplicadas, 'las entradas rotas NUNCA llegan al handler');
        self::assertCount(3, $poison, 'mal-1, roto-1 y restringido-1: ALERTA por cada POISON');
        foreach ($poison as [, , $raw]) {
            self::assertSame('', $raw, '`raw` va vacío: el cuerpo de un estado puede llevar datos personales');
        }
        $subjects = array_map(static fn (array $p): string => $p[0], $poison);
        sort($subjects);
        self::assertSame([
            'state.clientes.cliente.v1.acme.mal-1', 'state.clientes.cliente.v1.acme.restringido-1', 'state.clientes.cliente.v1.acme.roto-1',
        ], $subjects);

        // 4. el dueño lee la DLQ de estado con su durable `<servicio>__dlq_state_<dom>` (los permisos exactos del rol «DLQ»)
        $crm = SocketTransport::connect($this->options('crm-api'));
        $crudoDlq = [];
        try {
            foreach ($crm->fetch('DLQ_CLIENTES', 'crm-api__dlq_state_clientes', 10, 2_000) as $m) {
                self::assertSame('dlq.state.clientes.cliente.v1', $m->subject, 'el subject de la DLQ de estado: sin tenant ni clave');
                $crudoDlq[] = $m->payload;
                $crm->respond($m->replyTo, '+ACK');
            }
            // fetch devuelve de a uno en el SDK de producción; aquí se piden los que falten
            while (count($crudoDlq) < 5 && ($more = $crm->fetch('DLQ_CLIENTES', 'crm-api__dlq_state_clientes', 10, 1_000)) !== []) {
                foreach ($more as $m) {
                    $crudoDlq[] = $m->payload;
                    $crm->respond($m->replyTo, '+ACK');
                }
            }
        } finally {
            $crm->close();
        }
        self::assertCount(5, $crudoDlq);
        $dlqEvents = array_map(static fn (string $s): array => json_decode($s, true), $crudoDlq);
        $de = static function (string $k) use ($dlqEvents): array {
            foreach ($dlqEvents as $d) {
                if ($d['data']['key'] === $k) {
                    return $d;
                }
            }
            self::fail("no hay un evento de DLQ para {$k}");
        };

        // 5. NI UN dato personal en ninguna forma: ni en `data`, ni en `dlqerror`, ni como `rawBase64`
        foreach ($crudoDlq as $crudo) {
            self::assertStringNotContainsString(self::SECRETO, $crudo, 'el dato personal llegó a la DLQ');
            self::assertStringNotContainsString('Ana Secreta', $crudo);
            self::assertStringNotContainsString('rawBase64', $crudo, 'un mensaje ilegible NO se copia en ninguna forma');
        }
        foreach ($dlqEvents as $d) {
            self::assertSame(['key', 'revision'], array_keys($d['data']));
            self::assertSame('data', array_key_last($d));
            self::assertArrayNotHasKey('signature', $d);
            self::assertArrayNotHasKey('signkeyid', $d);
            self::assertSame('tienda-web__state_clientes_cliente_v1__acme', $d['dlqconsumer']);
        }

        // 6. cada una, con su razón, la clave del SUBJECT y la revisión del stream
        self::assertSame('poison', $de('mal-1')['dlqreason']);
        self::assertMatchesRegularExpression('/^STATE_SUBJECT_MISMATCH: /', $de('mal-1')['dlqerror']);
        self::assertSame($revMal, $de('mal-1')['data']['revision']);
        self::assertSame('globex', $de('mal-1')['tenantid'], 'el resto del sobre queda como estaba, mentira incluida');
        self::assertSame('poison', $de('roto-1')['dlqreason']);
        self::assertSame($revRoto, $de('roto-1')['data']['revision']);
        self::assertSame('roto-1', $de('roto-1')['subject'], 'sintetizado con lo único fiable: el subject');
        self::assertSame('acme', $de('roto-1')['tenantid']);
        self::assertSame('roto-1', $de('roto-1')['partitionkey'], 'partitionkey = la clave (10-state.md §6)');
        self::assertMatchesRegularExpression('/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/', $de('roto-1')['time']);
        self::assertLessThan(60, abs(time() - strtotime($de('roto-1')['time'])), 'time = el instante ACTUAL del consumo, no uno inventado');
        self::assertSame('/test/tienda-web', $de('roto-1')['source'], 'el servicio que consume');
        self::assertSame('internal', $de('roto-1')['dataclassification']);
        self::assertSame($de('roto-1')['id'], $de('roto-1')['correlationid']);
        self::assertSame('poison', $de('restringido-1')['dlqreason']);
        self::assertMatchesRegularExpression('/^INVALID_DATACLASSIFICATION: /', $de('restringido-1')['dlqerror']);
        self::assertSame($revRestringido, $de('restringido-1')['data']['revision']);
        self::assertSame('permanent', $de('err-tipado')['dlqreason']);
        self::assertSame('PAIS_NO_SOPORTADO: país fuera del catálogo', $de('err-tipado')['dlqerror']);
        self::assertSame('permanent', $de('err-plano')['dlqreason']);
        $frase = json_decode((string) file_get_contents(dirname(__DIR__, 3) . '/protocol.json'), true)['state']['dlq']['dlqerrorFixedPhrase'];
        self::assertSame(
            "UNKNOWN: {$frase}",
            $de('err-plano')['dlqerror'],
            'el mensaje de un error que no es de flux se omite —podría citar la fila entera—: solo el código y la frase fija de protocol.json',
        );

        // 7. métricas: el consumer SIN el sufijo del tenant; los POISON cuentan como `poison`
        $subject = 'subject="state.clientes.cliente.v1"';
        $consumer = 'consumer="tienda-web__state_clientes_cliente_v1"';
        self::assertSame(3, $metricas->counter("flux_events_consumed_total{{$subject},{$consumer},outcome=\"poison\"}"));
        self::assertSame(2, $metricas->counter("flux_events_consumed_total{{$subject},{$consumer},outcome=\"ok\"}"));
        self::assertSame(2, $metricas->counter("flux_events_consumed_total{{$subject},{$consumer},outcome=\"permanent\"}"));
        self::assertSame(1, $metricas->counter("flux_events_dlq_total{{$subject},{$consumer},reason=\"poison\",code=\"STATE_SUBJECT_MISMATCH\"}"));
        foreach (explode("\n", $metricas->render()) as $linea) {
            if (str_starts_with($linea, 'flux_events_')) {
                self::assertStringNotContainsString('acme', $linea, 'ni el tenant como etiqueta');
                self::assertStringNotContainsString('mal-1', $linea, 'ni la clave como etiqueta');
            }
        }
        self::assertTrue($logger->contains('POISON STATE_SUBJECT_MISMATCH'), 'el POISON se anuncia en el log');
        self::assertFalse($logger->contains(self::SECRETO), 'ningún log lleva el dato');

        // 8. `get` de una entrada que miente: lanza (POISON), no la devuelve
        $mentira = self::capturar(fn () => $web->state->get(self::C, 'mal-1'));
        self::assertInstanceOf(StateSubjectMismatchException::class, $mentira);
        self::assertSame('STATE_SUBJECT_MISMATCH', $mentira->errorCode);
        // 9. y quien NO es dueño no lee la DLQ de otro: tienda-web no tiene CONSUMER.INFO sobre `crm-api__dlq_state_clientes`
        $ajeno = SocketTransport::connect($this->options('tienda-web'));
        try {
            self::assertInstanceOf(PermissionDeniedException::class, self::capturar(fn () => $ajeno->api('$JS.API.CONSUMER.INFO.DLQ_CLIENTES.crm-api__dlq_state_clientes', [], 1_500)));
        } finally {
            $ajeno->close();
        }
    }

    public function testL3ContraElBundleRealPutValidoPasaUnoRotoFallaAntesDeEscribirYLaLapidaValidaContraEliminado(): void
    {
        $bundle = SchemaBundle::fromFile(dirname(__DIR__, 3) . '/schemas/bundle.json');
        $dueno = $this->bus('crm-api', [
            'tenantId' => 'acme', 'validation' => new ValidationOptions(ValidationMode::Strict, $bundle), 'schemaBaseUrl' => null,
        ]);
        self::assertSame('ok', $dueno->state->put(self::C, 'cli-1', self::dato('cli-1'))->outcome);
        self::assertSame(
            'https://schemas.internal/state/clientes/cliente/1.0.0.json',
            $this->enElStream('state.clientes.cliente.v1.acme.cli-1')['dataschema'],
            'el índice `state` del bundle, no la red',
        );
        $e = self::capturar(fn () => $dueno->state->put(self::C, 'cli-2', ['email' => 'no-es-un-email'] + self::dato('cli-2')));
        self::assertInstanceOf(SchemaValidationError::class, $e, $e === null ? 'devolvió' : $e::class . ': ' . $e->getMessage());
        self::assertStringNotContainsString('no-es-un-email', $e->getMessage(), 'no copia valores a su mensaje');
        self::assertNull($this->enElStream('state.clientes.cliente.v1.acme.cli-2'), 'falló ANTES de escribir');
        self::assertSame('ok', $dueno->state->delete(self::C, 'cli-1', 2)->outcome);
        self::assertSame('https://schemas.internal/system/state/eliminado/1.0.0.json', $this->enElStream('state.clientes.cliente.v1.acme.cli-1')['dataschema']);
    }

    // ═══ `flux state resync` con un watch vivo ════════════════════════════════════════════════════

    public function testUnWatchVivoSobreviveAResyncReabreElConsumoYEntregaLaFotoOtraVez(): void
    {
        // El observador no tiene CONSUMER.CREATE (permisos exactos): reabrir usa solo CONSUMER.INFO y MSG.NEXT.
        $dueno = $this->bus('crm-api', ['tenantId' => 'acme']);
        foreach (['r1', 'r2', 'r3', 'r4', 'r5'] as $k) {
            $dueno->state->put(self::C, $k, self::dato($k, 1));
        }
        $this->provisionar('tienda-web', 'acme');
        $logger = new ArrayLogger();
        $web = $this->bus('tienda-web', ['tenantId' => 'acme'], null, $logger);
        $vistas = [];
        $sincronizado = 0;
        $web->state->watch(self::C, function (StateEntry $e) use (&$vistas): void {
            $vistas[] = [$e->key, $e->aggregateVersion];
        }, onSynced: function () use (&$sincronizado): void {
            $sincronizado++;
        });
        self::assertTrue($this->hasta($web, function () use (&$sincronizado, &$vistas): bool { return count($vistas) === 5 && $sincronizado === 1; }, 10), 'la foto inicial');

        // `flux state resync`: borrar y recrear con la configuración canónica. Entre el borrado y la recreación el durable NO existe: un pull a un
        // consumidor que no existe no es «no hay mensajes» (el watch quedaría parado y aparentemente sano), es un aviso y un reintento.
        $durable = 'tienda-web__state_clientes_cliente_v1__acme';
        $this->ops->api('$JS.API.CONSUMER.DELETE.' . self::STREAM . '.' . $durable);
        self::assertTrue($this->hasta($web, function () use ($logger): bool { return $logger->contains('se borró con el watch'); }, 10), 'sin aviso del borrado: ' . implode(' | ', $logger->messages()));
        $t0 = microtime(true);
        $this->provisionar('tienda-web', 'acme');
        self::assertTrue(
            $this->hasta($web, function () use (&$vistas): bool { return count($vistas) >= 10; }, 20),
            'tras el resync debe volver la foto entera (5 más), y llegaron ' . (count($vistas) - 5) . '. Log: ' . implode(' | ', $logger->messages()),
        );
        self::assertLessThan(15, microtime(true) - $t0, 'reabrir debe tardar segundos');
        $segunda = array_map(static fn (array $v): string => $v[0], array_slice($vistas, 5));
        sort($segunda);
        self::assertSame(['r1', 'r2', 'r3', 'r4', 'r5'], $segunda);
        self::assertSame(1, $sincronizado, '`onSynced` NO se vuelve a invocar tras un resync (una vez por watch)');
        self::assertTrue($logger->contains('reabierto'), 'se avisa por el logger: ' . implode(' | ', $logger->messages()));

        // …y el canal en VIVO sigue vivo: un cambio posterior llega
        $dueno->state->put(self::C, 'r1', self::dato('r1', 2));
        self::assertTrue($this->hasta($web, function () use (&$vistas): bool { return in_array(['r1', 2], $vistas, true); }, 10), 'un put en vivo tras el resync');
    }

    public function testUnWatchCuyoDurableSeBorraYNoSeRecreaReintentaConAvisoSinColgarseYCloseLoPara(): void
    {
        $dueno = $this->bus('crm-api', ['tenantId' => 'acme']);
        $dueno->state->put(self::C, 'a1', self::dato('a1', 1));
        $this->provisionar('tienda-web', 'acme');
        $logger = new ArrayLogger();
        $web = $this->bus('tienda-web', ['tenantId' => 'acme'], null, $logger);
        $vistas = [];
        $web->state->watch(self::C, function (StateEntry $e) use (&$vistas): void {
            $vistas[] = $e->key;
        });
        self::assertTrue($this->hasta($web, function () use (&$vistas): bool { return count($vistas) === 1; }, 10));
        $this->ops->api('$JS.API.CONSUMER.DELETE.' . self::STREAM . '.tienda-web__state_clientes_cliente_v1__acme'); // y nadie lo recrea
        self::assertTrue($this->hasta($web, function () use ($logger): bool { return $logger->contains('no se pudo reabrir'); }, 15), 'debe avisar: ' . implode(' | ', $logger->messages()));
        $t0 = microtime(true);
        $web->close();
        self::assertLessThan(5, microtime(true) - $t0, 'close() no espera al durable que no vuelve');
        self::assertFalse($web->state->hasActiveWatches(), 'close() para el watch');
    }

    public function testElDurableRecreadoConOtroFiltroNoSeConsumeTrasUnResync(): void
    {
        $dueno = $this->bus('crm-api');
        $dueno->state->put(self::C, 'a1', self::dato('a1', 1), tenantId: 'acme');
        $dueno->state->put(self::C, 'g1', self::dato('g1', 1), tenantId: 'globex');
        $this->provisionar('tienda-web', 'acme');
        $logger = new ArrayLogger();
        $web = $this->bus('tienda-web', ['tenantId' => 'acme'], null, $logger);
        $vistas = [];
        $web->state->watch(self::C, function (StateEntry $e) use (&$vistas): void {
            $vistas[] = "{$e->tenantId}/{$e->key}";
        });
        self::assertTrue($this->hasta($web, function () use (&$vistas): bool { return count($vistas) === 1; }, 10));
        // alguien lo recrea con el filtro de TODOS los tenants
        $this->provisionar('tienda-web', 'acme', ['filter_subject' => 'state.' . self::C . '.*.*']);
        self::assertTrue(
            $this->hasta($web, function () use ($logger): bool { return $logger->contains('filter_subject') && $logger->contains('no se pudo reabrir'); }, 15),
            'debe avisar: ' . implode(' | ', $logger->messages()),
        );
        $web->state->poll(500);
        self::assertSame(['acme/a1'], $vistas, 'el durable con otro filtro no se consume: globex/g1 no llega');
    }

    // ═══ El tenant de un watch y el modelo de ejecución ═══════════════════════════════════════════

    public function testUnWatchAbiertoDentroDeUnHandlerNoHeredaElTenantDelEventoPeroUnPutSi(): void
    {
        $this->provisionar('tienda-web', 'acme');
        $this->provisionar('informes-api');
        $conTenant = $this->bus('tienda-web', ['tenantId' => 'acme']);
        $sinTenant = $this->bus('informes-api', ['tenantId' => null]);
        $contexto = new \Flux\EventContext(correlationid: 'corr-1', causationid: 'ev-9', tenantid: 'globex');
        $a = \Flux\EventContext::run($contexto, fn () => $conTenant->state->watch(self::C, static function (): void {}));
        self::assertSame('acme', $a->tenantId, 'gana el de connect(), no el del evento en curso');
        self::assertSame('tienda-web__state_clientes_cliente_v1__acme', $a->durable);
        $b = \Flux\EventContext::run($contexto, fn () => $sinTenant->state->watch(self::C, static function (): void {}));
        self::assertNull($b->tenantId, 'sin tenant en la opción ni en connect(), se observan TODOS: no el del evento');
        self::assertSame('informes-api__state_clientes_cliente_v1', $b->durable);
        // CONTROL: `put` dentro del mismo contexto SÍ hereda el tenant del evento (un estado derivado pertenece al del evento que lo causó)
        $dueno = $this->bus('crm-api', ['tenantId' => null]);
        \Flux\EventContext::run($contexto, fn () => $dueno->state->put(self::C, 'derivado-1', self::dato('derivado-1')));
        self::assertNotNull($this->enElStream('state.clientes.cliente.v1.globex.derivado-1'), 'el put heredó `globex`');
        self::assertNull($this->enElStream('state.clientes.cliente.v1.acme.derivado-1'));
        // y el estado derivado hereda también correlationid y causationid del evento que lo causó
        $ev = $this->enElStream('state.clientes.cliente.v1.globex.derivado-1');
        self::assertSame('corr-1', $ev['correlationid']);
        self::assertSame('ev-9', $ev['causationid']);
    }

    public function testRunConduceElWatchSeParaConLaSenalYCloseLoDeja(): void
    {
        $dueno = $this->bus('crm-api', ['tenantId' => 'acme']);
        foreach (['a', 'b', 'c'] as $k) {
            $dueno->state->put(self::C, $k, self::dato($k));
        }
        $this->provisionar('tienda-web', 'acme');
        $web = $this->bus('tienda-web', ['tenantId' => 'acme']);
        $vistas = [];
        $enCurso = 0;
        $solapadas = false;
        $web->state->watch(self::C, function (StateEntry $e) use (&$vistas, &$enCurso, &$solapadas): void {
            $enCurso++;
            $solapadas = $solapadas || $enCurso > 1;
            usleep(20_000);
            $vistas[] = $e->key;
            $enCurso--;
        });
        // `run()` procesa hasta el límite y devuelve: es lo que hace un worker con `maxMessages` (reinicio periódico)
        self::assertSame(3, $web->run(maxMessages: 3, idleTimeoutMs: 1_000));
        self::assertCount(3, $vistas);
        self::assertFalse($solapadas, 'un handler a la vez: el límite de concurrencia es 1');

        // la parada ordenada: una bandera (la que pondría un manejador de SIGTERM) se consulta entre mensajes y `run()` devuelve sin perder nada
        $parar = false;
        $web2 = $this->bus('tienda-web', ['tenantId' => 'acme']);
        $n = 0;
        $web2->state->watch(self::C, function (StateEntry $e) use (&$parar, &$n): void {
            $n++;
            $parar = true; // llega la señal con una entrada en curso: esa termina, y no se pide otra
        });
        // el durable es el mismo y `$web` ya consumió lo anterior: se escriben dos entradas nuevas
        $dueno->state->put(self::C, 'd', self::dato('d'));
        $dueno->state->put(self::C, 'e', self::dato('e'));
        $t0 = microtime(true);
        $web2->run(shouldStop: static function () use (&$parar): bool {
            return $parar;
        }, maxSeconds: 20, idleTimeoutMs: 1_000);
        self::assertSame(1, $n, 'la entrada en curso se termina y no se empieza otra');
        self::assertLessThan(5, microtime(true) - $t0);
        $web2->close(); // cierra tras un PING/PONG: lo último que se escribió —el ack— llegó al broker
        $info = $this->ops->api('$JS.API.CONSUMER.INFO.' . self::STREAM . '.tienda-web__state_clientes_cliente_v1__acme');
        self::assertSame(0, $info['num_ack_pending'], 'la entrada en curso quedó confirmada: la parada no pierde nada');
    }

    // ═══ Utilidades ═══════════════════════════════════════════════════════════════════════════════

    /** @return array<string,mixed> */
    private static function dato(string $key, int $version = 1, array $extra = []): array
    {
        return $extra + ['clienteId' => $key, 'nombre' => 'Ana Secreta', 'email' => self::SECRETO, 'aggregateVersion' => $version];
    }

    private static function capturar(callable $f): ?\Throwable
    {
        try {
            $f();
        } catch (\Throwable $e) {
            return $e;
        }

        return null;
    }

    /** Conduce los `watch` del bus hasta que se cumpla la condición o pase el plazo. */
    private function hasta(FluxBus $bus, callable $cond, float $seconds): bool
    {
        $deadline = microtime(true) + $seconds;
        while (microtime(true) < $deadline) {
            if ($cond()) {
                return true;
            }
            $bus->state->poll(100);
        }

        return (bool) $cond();
    }

    /** Lo que el broker tiene de verdad en el stream, sin pasar por el SDK. */
    private function enElStream(string $subject): ?array
    {
        $r = $this->ops->directGet(self::STREAM, $subject);

        return $r === null ? null : json_decode($r['data'], true);
    }

    /** Un mensaje que escribe `ops` por JetStream, saltándose al SDK: para fabricar lo que un SDK conforme no emite. */
    private function escribeCrudo(string $subject, string|array $cuerpo): int
    {
        $ack = $this->ops->publishAck($subject, is_string($cuerpo) ? $cuerpo : json_encode($cuerpo, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
        self::assertArrayNotHasKey('error', $ack, json_encode($ack));

        return (int) $ack['seq'];
    }

    /** Lo que `flux provision` hará: el durable del observador con la config canónica y `last_per_subject`. */
    private function provisionar(string $service, ?string $tenant = null, array $override = []): string
    {
        $durable = \Flux\State\StateEnvelope::durableName($service, self::C, $tenant);
        $this->ops->api('$JS.API.CONSUMER.DELETE.' . self::STREAM . '.' . $durable);
        $config = $override + [
            'durable_name' => $durable,
            'filter_subject' => 'state.' . self::C . '.' . ($tenant ?? '*') . '.*',
            'deliver_policy' => 'last_per_subject',
            'ack_policy' => 'explicit',
            'ack_wait' => 30_000_000_000,
            'max_deliver' => 6,
            'backoff' => [30_000_000_000, 60_000_000_000, 300_000_000_000, 900_000_000_000, 1_800_000_000_000],
            'max_ack_pending' => 256,
            'replay_policy' => 'instant',
        ];
        $r = $this->ops->api(
            '$JS.API.CONSUMER.CREATE.' . self::STREAM . '.' . $durable . '.' . $config['filter_subject'],
            ['stream_name' => self::STREAM, 'config' => $config, 'action' => 'create'],
        );
        self::assertArrayNotHasKey('error', $r, json_encode($r));

        return $durable;
    }

    private function limpiar(): void
    {
        foreach ([self::STREAM, 'DLQ_CLIENTES'] as $s) {
            $this->ops->api('$JS.API.STREAM.PURGE.' . $s);
        }
        $names = $this->ops->api('$JS.API.CONSUMER.NAMES.' . self::STREAM)['consumers'] ?? [];
        foreach ($names as $d) {
            $this->ops->api('$JS.API.CONSUMER.DELETE.' . self::STREAM . '.' . $d);
        }
    }
}
