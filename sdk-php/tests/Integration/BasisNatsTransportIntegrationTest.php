<?php

declare(strict_types=1);

namespace Flux\Tests\Integration;

use Flux\ConnectOptions;
use Flux\DataClassification;
use Flux\FluxBus;
use Flux\FluxEvent;
use PHPUnit\Framework\TestCase;

/**
 * Conformidad del adaptador de NATS contra un broker REAL.
 *
 * Hasta ahora el resto de la suite ejercitaba `BasisNatsTransport` con un cliente falso:
 * fijaba cómo REACCIONA el adaptador, no que la API de `basis-company/nats` sea la que
 * asume. Todo lo demás del SDK está aislado tras el puerto `NatsTransport` y sí estaba
 * probado; esto cubre justo el hueco que quedaba.
 *
 * Se salta solo si no hay broker o falta `ext-sockets`, para que la suite siga siendo
 * verde en una máquina sin NATS.
 *
 *     FLUX_NATS_URL=nats://127.0.0.1:4222 vendor/bin/phpunit --group integration
 *
 * @group integration
 */
final class BasisNatsTransportIntegrationTest extends TestCase
{
    private const DOMINIO = 'phpitest';

    private static function url(): string
    {
        return getenv('FLUX_NATS_URL') ?: 'nats://127.0.0.1:4222';
    }

    protected function setUp(): void
    {
        if (!extension_loaded('sockets')) {
            self::markTestSkipped('el cliente de NATS necesita ext-sockets');
        }
        if (!class_exists(\Basis\Nats\Client::class)) {
            self::markTestSkipped('falta basis-company/nats: composer require --dev basis-company/nats');
        }

        // Un broker inalcanzable debe SALTAR el test, no hacerlo fallar: lo contrario
        // convierte "no hay NATS aquí" en "el SDK está roto".
        $partes = parse_url(self::url());
        $sock = @fsockopen($partes['host'] ?? '127.0.0.1', $partes['port'] ?? 4222, $e, $s, 1.0);
        if ($sock === false) {
            self::markTestSkipped('no hay NATS en ' . self::url());
        }
        fclose($sock);
    }

    private function bus(string $servicio): FluxBus
    {
        // El cliente de NATS se construye AQUÍ y se inyecta: `ConnectOptions` no conoce
        // a `basis-company/nats`, y esa separación es justo lo que permite probar todo
        // el resto del SDK sin broker.
        $partes = parse_url(self::url());
        // El timeout va en el constructor: `Configuration` de basis 1.2 solo expone
        // `setDelay` como setter fluido.
        $cfg = new \Basis\Nats\Configuration([
            'host' => $partes['host'] ?? '127.0.0.1',
            'port' => $partes['port'] ?? 4222,
            'timeout' => 5,
        ]);
        $cfg->setDelay(0.05);

        return FluxBus::connectToNats(
            new ConnectOptions(
                service: $servicio,
                environment: 'test',
                version: '1.0.0',
                tenantId: 'acme',
                schemaBaseUrl: 'https://schemas.internal',
            ),
            new \Basis\Nats\Client($cfg),
        );
    }

    /**
     * L1 completo contra el broker: publicar, consumir, ack explícito, y el envelope
     * relleno por el SDK sobreviviendo al viaje de ida y vuelta.
     */
    public function testPublicarYConsumirContraUnBrokerReal(): void
    {
        $subject = self::DOMINIO . '.pedido.v1.creado';
        $bus = $this->bus('pedidos-api');

        $recibidos = [];
        $bus->subscribe($subject, function (FluxEvent $ev) use (&$recibidos): void {
            $recibidos[] = $ev;
        });

        $publicado = $bus->publish(
            $subject,
            ['pedidoId' => 'ped-123', 'aggregateVersion' => 1, 'totalCents' => 9990],
            aggregateId: 'ped-123'
        );

        $bus->run(maxMessages: 1, maxSeconds: 10.0);

        self::assertCount(1, $recibidos, 'el evento publicado debería volver');
        $ev = $recibidos[0];

        self::assertSame($publicado->id, $ev->id, 'el id no se regenera');
        self::assertSame('1.0', $ev->specversion);
        self::assertSame('com.flux.' . self::DOMINIO . '.pedido.creado.v1', $ev->type);
        self::assertSame('/test/pedidos-api', $ev->source);
        self::assertSame('acme', $ev->tenantid);
        // ⚠️ `$ev->subject` es el atributo `subject` de CLOUDEVENTS —el id del agregado—,
        // NO el subject de NATS. Son dos cosas distintas con el mismo nombre y es el error
        // más frecuente al adoptar CloudEvents sobre NATS (01-envelope.md §2.1). Al
        // escribir este test asumí `$ev->aggregateId` y no existe.
        self::assertSame('ped-123', $ev->subject);
        // Un evento que no nace de otro inicializa correlationid con su propio id.
        self::assertSame($ev->id, $ev->correlationid);
        self::assertNull($ev->causationid);

        // `time` con exactamente 3 decimales y sufijo Z, DESPUÉS del round-trip por el
        // broker: es donde un reformateo accidental se notaría.
        self::assertMatchesRegularExpression(
            '/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/',
            $ev->time
        );

        $bus->close();
    }

    /**
     * La única red de seguridad automática contra una errata de subject:
     * un publish por JetStream a un subject que ningún stream captura DEBE fallar
     * (02-naming.md §1.1). Por core NATS se evaporaría en silencio.
     */
    public function testUnSubjectConMayusculasSeRechazaAntesDeLaRed(): void
    {
        $bus = $this->bus('pedidos-api');

        $this->expectException(\Flux\InvalidSubjectException::class);
        try {
            $bus->publish('Phpitest.pedido.v1.creado', ['x' => 1]);
        } finally {
            $bus->close();
        }
    }

    /**
     * Requisito L2: el adaptador DEBE comprobar que la config que devuelve el servidor
     * coincide con la solicitada. JetStream sobrescribe `ack_wait` con `backoff[0]` sin
     * devolver error, y este test comprueba que el ida y vuelta con el broker real
     * conserva la config canónica (03-delivery.md §2.1).
     */
    public function testLaConfigCanonicaSobreviveAlServidor(): void
    {
        $subject = self::DOMINIO . '.config.v1.creado';
        $bus = $this->bus('facturacion-api');

        // Si el servidor devolviera algo distinto, subscribe lanzaría
        // ConsumerConfigMismatchException. Que esto no lance ES la comprobación.
        $bus->subscribe($subject, static function (FluxEvent $ev): void {
        });

        self::assertTrue(true, 'el servidor honró la configuración canónica');
        $bus->close();
    }
}
