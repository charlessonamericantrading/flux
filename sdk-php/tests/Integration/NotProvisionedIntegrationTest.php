<?php

declare(strict_types=1);

namespace Flux\Tests\Integration;

use Flux\FluxEvent;
use Flux\NotProvisionedException;
use Flux\PermissionDeniedException;
use PHPUnit\Framework\Attributes\Group;

/**
 * `NOT_PROVISIONED` (04-errors.md §5, 06-security.md §3.6) contra un broker REAL **sin aprovisionar**: ningún stream ni durable existe y
 * la ACL de un servicio no concede crearlos. En producción los aprovisiona `flux provision` con credenciales de operación.
 *
 * La frontera con `PERMISSION_DENIED` es una PAREJA de señales: crear → denegado **y** consultar → no existe. Una denegación sola no es
 * `NOT_PROVISIONED` (si el recurso existe, un `CREATE` denegado no es un error y el SDK sigue con la consulta), y sin poder consultar
 * tampoco: no se sabe si falta.
 */
#[Group('integration')]
final class NotProvisionedIntegrationTest extends BrokerTestCase
{
    private static SecureBroker $dev;

    public static function setUpBeforeClass(): void
    {
        self::$broker = SecureBroker::start(['--sin-aprovisionar']);
        // El desarrollo: la ACL concede además `CONSUMER.CREATE` de cada durable PROPIO. Los streams NO se crean nunca desde un servicio.
        self::$dev = SecureBroker::start(['--sin-aprovisionar', '--dev']);
    }

    public static function tearDownAfterClass(): void
    {
        self::$broker->stop();
        self::$dev->stop();
    }

    /** Crea un stream como lo haría `flux provision` (con el usuario de operación): solo el recurso, nada más. */
    private function provisionarStream(string $name, string $subjects, ?SecureBroker $broker = null): void
    {
        $broker ??= self::$broker;
        $ops = \Flux\Transport\SocketTransport::connect(new \Flux\ConnectOptions(
            service: 'ops-tests', environment: 'test', version: '1', servers: $broker->url(), credentials: $broker->ops(), tls: ['caFile' => $broker->ca()],
        ));
        $r = $ops->api('$JS.API.STREAM.CREATE.' . $name, ['name' => $name, 'subjects' => [$subjects], 'storage' => 'file', 'retention' => 'limits', 'num_replicas' => 1]);
        $ops->close();
        self::assertArrayNotHasKey('error', $r, json_encode($r));
    }

    public function testUnStreamQueFaltaYNoSePuedeCrearEsNotProvisioned(): void
    {
        $bus = $this->bus('pedidos-api'); // dueño de `pedidos`: puede PREGUNTAR por EVT_PEDIDOS, no crearlo
        [$ms, $error] = self::medir(fn () => $bus->publish('pedidos.pedido.v1.creado', ['pedidoId' => 'x']));

        self::assertInstanceOf(NotProvisionedException::class, $error, (string) $error);
        self::assertSame('NOT_PROVISIONED', $error->errorCode);
        self::assertSame(\Flux\ErrorClass::Permanent, $error->fluxClass());
        self::assertSame('stream', $error->kind);
        self::assertSame('EVT_PEDIDOS', $error->resource, 'DEBE nombrar el recurso que falta');
        self::assertSame('$JS.API.STREAM.CREATE.EVT_PEDIDOS', $error->deniedSubject, 'y el subject denegado (el de la creación), tal como lo comunicó el broker');
        self::assertStringContainsString('flux provision', $error->getMessage(), 'DEBE apuntar a `flux provision`');
        self::assertStringContainsString('operación', $error->getMessage(), '…con credenciales de operación, no las del servicio');
        self::assertLessThan(1_500, $ms, 'en milisegundos, no un timeout de un minuto sin explicación');
    }

    public function testUnConsumidorQueFaltaYNoSePuedeCrearEsNotProvisioned(): void
    {
        $this->provisionarStream('EVT_PEDIDOS', 'pedidos.>');
        $this->provisionarStream('DLQ_PEDIDOS', 'dlq.pedidos.>');
        $bus = $this->bus('inventario-api'); // consume pedidos.pedido.v1.creado, y su ACL no concede CONSUMER.CREATE
        [$ms, $error] = self::medir(fn () => $bus->subscribe('pedidos.pedido.v1.creado', static function (FluxEvent $e): void {}));

        self::assertInstanceOf(NotProvisionedException::class, $error, $error === null ? 'subscribe() devolvió' : get_class($error) . ': ' . $error->getMessage());
        self::assertSame('consumer', $error->kind);
        self::assertSame('inventario-api__pedidos_pedido_v1_creado', $error->resource);
        self::assertSame('EVT_PEDIDOS', $error->stream, 'el consumidor Y su stream');
        self::assertSame('$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.inventario-api__pedidos_pedido_v1_creado.pedidos.pedido.v1.creado', $error->deniedSubject);
        self::assertStringContainsString('flux provision', $error->getMessage());
        self::assertLessThan(1_500, $ms);
    }

    public function testNoSeEmiteSiElRecursoExisteYElCreateEstaDenegado(): void
    {
        // El otro sentido: con el consumidor aprovisionado, que su ACL no conceda CREATE no es un error.
        $this->provisionarStream('EVT_PEDIDOS', 'pedidos.>');
        $this->provisionarStream('DLQ_PEDIDOS', 'dlq.pedidos.>');
        $ops = $this->ops();
        $r = $ops->api('$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.inventario-api__pedidos_pedido_v1_cancelado.pedidos.pedido.v1.cancelado', [
            'stream_name' => 'EVT_PEDIDOS',
            'config' => [
                'durable_name' => 'inventario-api__pedidos_pedido_v1_cancelado', 'filter_subject' => 'pedidos.pedido.v1.cancelado', 'ack_policy' => 'explicit',
                'ack_wait' => 30_000_000_000, 'max_deliver' => 6, 'max_ack_pending' => 256, 'deliver_policy' => 'all', 'replay_policy' => 'instant',
                'backoff' => [30_000_000_000, 60_000_000_000, 300_000_000_000, 900_000_000_000, 1_800_000_000_000],
            ],
        ]);
        $ops->close();
        self::assertArrayNotHasKey('error', $r, json_encode($r));

        $bus = $this->bus('inventario-api');
        $sub = $bus->subscribe('pedidos.pedido.v1.cancelado', static function (): void {});
        self::assertSame('inventario-api__pedidos_pedido_v1_cancelado', $sub->durable);
    }

    public function testSinPoderConsultarTampocoEsNotProvisioned(): void
    {
        // Una ACL sin `STREAM.INFO` no deja ni mirar: no se sabe si falta. Es PERMISSION_DENIED (se mide en SecurityIntegrationTest con el
        // consumidor; aquí, con el stream de otro dominio: un servicio que no tiene concedido `facturacion`).
        $bus = $this->bus('pedidos-api');
        [, $error] = self::medir(fn () => $bus->publish('facturacion.factura.v2.emitida', ['facturaId' => 'f']));

        self::assertInstanceOf(PermissionDeniedException::class, $error);
        self::assertNotInstanceOf(NotProvisionedException::class, $error);
    }

    public function testEnDesarrolloElConsumidorPropioSeCreaPeroElStreamNunca(): void
    {
        $dev = self::$dev;
        // Sin stream: ni siquiera con `--dev` lo crea un servicio.
        $bus = FluxBusFor::connect($dev, 'inventario-api');
        [, $error] = self::medir(fn () => $bus->subscribe('pedidos.pedido.v1.creado', static function (): void {}));
        self::assertInstanceOf(NotProvisionedException::class, $error, (string) $error);
        self::assertSame('stream', $error->kind, 'los streams NO los crea ningún servicio');
        $bus->close();

        // Con el stream aprovisionado, `CONSUMER.CREATE` del durable propio sí está concedido y el SDK lo crea.
        $this->provisionarStream('EVT_PEDIDOS', 'pedidos.>', $dev);
        $this->provisionarStream('DLQ_PEDIDOS', 'dlq.pedidos.>', $dev);
        $bus = FluxBusFor::connect($dev, 'inventario-api');
        $sub = $bus->subscribe('pedidos.pedido.v1.creado', static function (): void {});
        self::assertSame('inventario-api__pedidos_pedido_v1_creado', $sub->durable);
        $bus->close();
    }
}
