<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\InvalidSubjectException;
use Flux\Protocol;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Naming, configuración canónica e identidad.
 *
 * Es lo que un agente o un humano puede romper sin que ningún test de integración se
 * entere: un subject con una mayúscula, un `ack_wait` que deja de ser `backoff[0]`, un
 * durable con puntos.
 */
final class ProtocolTest extends TestCase
{
    // ─── Subjects ─────────────────────────────────────────────────────────────

    public function testParseaLosCuatroTokens(): void
    {
        $p = Protocol::parseSubject('logistica.envio.v2.entrega-fallida');

        self::assertSame('logistica', $p->domain);
        self::assertSame('envio', $p->aggregate);
        self::assertSame(2, $p->major);
        self::assertSame('entrega-fallida', $p->event);
    }

    #[DataProvider('subjectsValidos')]
    public function testAceptaLosValidosDeProtocolJson(string $subject): void
    {
        self::assertTrue(Protocol::isValidSubject($subject));
    }

    #[DataProvider('subjectsInvalidos')]
    public function testRechazaLosInvalidosDeProtocolJson(string $subject): void
    {
        self::assertFalse(Protocol::isValidSubject($subject));
    }

    public function testElPatronEsElDeProtocolJson(): void
    {
        // Si divergen, protocol.json manda: es lo que consumen los otros cinco SDKs.
        self::assertSame(
            ProtocolFixture::protocol()['naming']['subject']['pattern'],
            Protocol::SUBJECT_PATTERN,
        );
    }

    public function testLasMayusculasDanUnMensajeSobreElSubjectFantasma(): void
    {
        // El mensaje importa: NATS acepta el publish por core sin error y el evento
        // simplemente no existe para nadie (02-naming.md §1.1).
        $this->expectException(InvalidSubjectException::class);
        $this->expectExceptionMessageMatches('/minúsculas/u');
        $this->expectExceptionMessageMatches('/case-sensitive/u');

        Protocol::parseSubject('Pedidos.Pedido.V1.Creado');
    }

    public function testUnQuintoTokenRompeLosWildcards(): void
    {
        $this->expectException(InvalidSubjectException::class);
        $this->expectExceptionMessageMatches('/4 tokens/');

        Protocol::parseSubject('pedidos.pedido.v1.creado.retry');
    }

    #[DataProvider('formasRotas')]
    public function testRechazaFormasRotas(string $subject): void
    {
        self::assertFalse(Protocol::isValidSubject($subject));
    }

    // ─── Derivaciones ─────────────────────────────────────────────────────────

    public function testTypeEsReverseDnsConLaVersionAlFinal(): void
    {
        $type = Protocol::subjectToType(ProtocolFixture::SUBJECT);

        self::assertSame('com.flux.pedidos.pedido.creado.v1', $type);
        // Delimitador `~` y no `/`: varios patrones de protocol.json llevan barras
        // (`source` es `^/<entorno>/<servicio>$`) y con `/` habría que escaparlos.
        self::assertMatchesRegularExpression(
            '~' . ProtocolFixture::protocol()['naming']['type']['pattern'] . '~',
            $type,
        );
    }

    public function testStreamSinPuntos(): void
    {
        self::assertSame('EVT_PEDIDOS', Protocol::streamName('pedidos'));
        self::assertSame('DLQ_PEDIDOS', Protocol::dlqStreamName('pedidos'));
        self::assertSame('EVT_LINEA_ENVIO', Protocol::streamName('linea-envio'));

        foreach (ProtocolFixture::protocol()['naming']['stream']['forbiddenChars'] as $char) {
            self::assertStringNotContainsString($char, Protocol::streamName('linea-envio'));
        }
    }

    public function testDurableReversibleYSinPuntos(): void
    {
        $durable = Protocol::durableName('facturacion-api', ProtocolFixture::SUBJECT);
        $expected = ProtocolFixture::protocol()['naming']['durableConsumer'];

        self::assertSame($expected['example'], $durable);
        self::assertMatchesRegularExpression('~' . $expected['pattern'] . '~', $durable);

        foreach ($expected['forbiddenChars'] as $char) {
            self::assertStringNotContainsString($char, $durable);
        }

        // Reversible: partiendo por `__` se recuperan servicio y subject exactos, que es
        // lo que hace útil un `nats consumer ls` a las 3 de la mañana.
        [$service, $encoded] = explode('__', $durable, 2);
        self::assertSame('facturacion-api', $service);
        self::assertSame(str_replace('.', '_', ProtocolFixture::SUBJECT), $encoded);
    }

    public function testDurableValidaElSubject(): void
    {
        $this->expectException(InvalidSubjectException::class);

        Protocol::durableName('facturacion-api', 'Pedidos.pedido.v1.creado');
    }

    #[DataProvider('serviciosInvalidos')]
    public function testDurableValidaTambienElNombreDeServicio(string $service): void
    {
        // NATS aceptaría `FacturacionAPI__pedidos_…` sin error, así que si el SDK no lo
        // valida el incumplimiento solo se descubre al parsear nombres en una herramienta
        // (protocol.json → naming.service).
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/nombre de servicio inválido/u');

        Protocol::durableName($service, ProtocolFixture::SUBJECT);
    }

    public function testElPatronDeServicioEsElDeProtocolJson(): void
    {
        self::assertSame(
            ProtocolFixture::protocol()['naming']['service']['pattern'],
            Protocol::SERVICE_PATTERN,
        );
    }

    public function testDlqEsPrefijoYQuedaFueraDelStreamPrincipal(): void
    {
        $dlq = Protocol::dlqSubject(ProtocolFixture::SUBJECT);

        self::assertSame('dlq.pedidos.pedido.v1.creado', $dlq);
        self::assertTrue(Protocol::isDlqSubject($dlq));
        // La razón de que sea PREFIJO: un sufijo encajaría con `pedidos.>` y el stream
        // principal se tragaría sus propios muertos (02-naming.md §3.1).
        self::assertStringStartsNotWith('pedidos.', $dlq);
    }

    public function testSourceUri(): void
    {
        $uri = Protocol::sourceUri('produccion', 'pedidos-api');

        self::assertSame('/produccion/pedidos-api', $uri);
        self::assertMatchesRegularExpression(
            '~' . ProtocolFixture::protocol()['cloudevents']['requiredAttributes']['source']['pattern'] . '~',
            $uri,
        );
    }

    // ─── UUIDv7 ───────────────────────────────────────────────────────────────

    public function testUuid7VersionYVariante(): void
    {
        self::assertMatchesRegularExpression(
            '/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/',
            Protocol::uuid7(),
        );
    }

    public function testUuid7EsMonotonoDentroDelMismoMilisegundo(): void
    {
        // 01-envelope.md §2.6 se apoya en esto: ordenar por `id` dentro de un mismo
        // `source` equivale a ordenar por instante de generación, que es lo que permite
        // reconstruir historiales desde la DLQ. Sin el contador en `rand_a`, dos eventos
        // del mismo milisegundo quedarían en orden aleatorio.
        $ids = [];
        for ($i = 0; $i < 500; $i++) {
            $ids[] = Protocol::uuid7();
        }

        $sorted = $ids;
        sort($sorted, SORT_STRING);

        self::assertSame($sorted, $ids);
        self::assertCount(500, array_unique($ids));
    }

    // ─── Configuración canónica ───────────────────────────────────────────────

    public function testConsumerDefaultsCoincidenConProtocolJson(): void
    {
        $c = ProtocolFixture::protocol()['consumer'];

        self::assertSame($c['ackWaitSeconds'] * 1000, Protocol::ACK_WAIT_MS);
        self::assertSame($c['maxDeliver'], Protocol::MAX_DELIVER);
        self::assertSame($c['backoffMs'], Protocol::BACKOFF_MS);
        self::assertSame($c['maxAckPending'], Protocol::MAX_ACK_PENDING);
        self::assertSame($c['ackPolicy'], Protocol::ACK_POLICY);
    }

    public function testAckWaitEsBackoffCero(): void
    {
        // LA trampa de JetStream: el servidor sobrescribe `ack_wait` con `backoff[0]` sin
        // devolver error (03-delivery.md §2.1). Si esta invariante se rompe, cualquier
        // handler que tarde más que `backoff[0]` se ejecuta en concurrencia consigo mismo.
        self::assertSame(Protocol::BACKOFF_MS[0], Protocol::ACK_WAIT_MS);
        self::assertTrue(ProtocolFixture::protocol()['consumer']['ackWaitEqualsBackoffZero']);
    }

    public function testBackoffYMaxDeliverCuadran(): void
    {
        // 1 entrega inicial + N reintentos, uno por entrada de backoff. Si `max_deliver`
        // fuese 5, la última entrada (30 m) no se aplicaría nunca y la config mentiría
        // sobre su propio comportamiento.
        self::assertCount(Protocol::MAX_DELIVER - 1, Protocol::BACKOFF_MS);
    }

    public function testPresupuestoDeHandlerDeAlMenos30s(): void
    {
        // `backoff[0]` ES el `ack_wait` efectivo, es decir, el presupuesto de duración del
        // handler. Un primer reintento rápido es imposible por construcción.
        self::assertGreaterThanOrEqual(30_000, Protocol::BACKOFF_MS[0]);
    }

    public function testTiempoTotalHastaLaDlq(): void
    {
        // ≈51 min 30 s. Es una decisión de producto —cuánto se tolera reintentar antes de
        // que un humano se entere—, no un detalle técnico (03-delivery.md §2).
        self::assertSame(
            ProtocolFixture::protocol()['consumer']['totalTimeToDlqSeconds'],
            (int) (array_sum(Protocol::BACKOFF_MS) / 1000),
        );
    }

    public function testStreamDefaults(): void
    {
        self::assertSame(120_000, Protocol::DUPLICATE_WINDOW_MS);
        // Más larga: la DLQ es material forense (02-naming.md §3.3).
        self::assertGreaterThan(Protocol::STREAM_MAX_AGE_MS, Protocol::DLQ_STREAM_MAX_AGE_MS);
        self::assertSame(
            ProtocolFixture::protocol()['data']['maxMessageBytes'],
            Protocol::MAX_MESSAGE_BYTES,
        );
        self::assertSame(
            ProtocolFixture::protocol()['stream']['replicasSdkDefault'],
            Protocol::STREAM_REPLICAS_SDK_DEFAULT,
        );
    }

    public function testNanosNoConfundeUnidades(): void
    {
        // Equivocar la unidad no da error: da un `ack_wait` de 950 años, en silencio.
        self::assertSame(30_000_000_000, Protocol::nanos(Protocol::ACK_WAIT_MS));
    }

    // ─── Proveedores ──────────────────────────────────────────────────────────

    /** @return iterable<array{string}> */
    public static function subjectsValidos(): iterable
    {
        foreach (ProtocolFixture::protocol()['naming']['subject']['valid'] as $subject) {
            yield $subject => [$subject];
        }
    }

    /** @return iterable<array{string}> */
    public static function subjectsInvalidos(): iterable
    {
        foreach (ProtocolFixture::protocol()['naming']['subject']['invalid'] as $subject) {
            yield $subject => [$subject];
        }
    }

    /** @return iterable<string,array{string}> */
    public static function formasRotas(): iterable
    {
        yield 'comando sin tokens' => ['pedidos.crear-pedido'];
        yield 'falta la v' => ['pedidos.pedido.1.creado'];
        yield 'el mayor empieza en 1' => ['pedidos.pedido.v0.creado'];
        yield 'guion bajo prohibido' => ['pedidos.pedido_v1.creado'];
        yield 'token vacio' => ['pedidos..v1.creado'];
        yield 'espacio' => ['pedidos.pedido.v1.creado '];
        yield 'wildcard' => ['pedidos.pedido.v1.>'];
    }

    /** @return iterable<string,array{string}> */
    public static function serviciosInvalidos(): iterable
    {
        yield 'PascalCase' => ['FacturacionAPI'];
        yield 'snake_case' => ['facturacion_api'];
        yield 'punto' => ['facturacion.api'];
        yield 'guion al final' => ['facturacion-'];
        yield 'vacio' => [''];
    }
}
