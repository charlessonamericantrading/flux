<?php

declare(strict_types=1);

namespace Flux\Tests\Integration;

use Flux\ConnectionRejectedException;
use Flux\InvalidCredentialsException;
use Flux\InvalidTlsOptionsException;
use Flux\Nats\Connection;
use Flux\Nats\ConnectionException;
use Flux\Nats\ConnectionOptions;
use Flux\Nats\Credentials;
use Flux\Nats\TlsOptions;
use Flux\PermanentError;
use Flux\PermissionDeniedException;
use Flux\FluxEvent;
use Flux\HandlerContext;
use PHPUnit\Framework\Attributes\Group;

/**
 * La capacidad `security` (06-security.md §2 y §3, 04-errors.md §5, 03-delivery.md §6.1) contra un `nats-server` REAL con TLS, nkeys y las
 * ACLs que genera `gen-acl.mjs` —no un broker abierto—, aprovisionado con `flux provision`.
 *
 * Cada código se prueba **en los dos sentidos**: en su situación y en ninguna otra. Y «quitar un permiso se nota»: los usuarios
 * `sin-*` son el de un servicio con UN permiso menos, y la denegación sale por el canal que corresponde a su fila de la tabla de
 * 06-security.md §3.7.
 */
#[Group('integration')]
final class SecurityIntegrationTest extends BrokerTestCase
{
    private const PEDIDOS_CONSUMER_STREAM = 'EVT_INVENTARIO';
    private const PEDIDOS_DURABLE = 'pedidos-api__inventario_stock_v1_ajustado';
    private const SUBJECT_AJUSTADO = 'inventario.stock.v1.ajustado';

    public static function setUpBeforeClass(): void
    {
        self::$broker = SecureBroker::start([
            '--sin-tls', '--altname-malo', '--mtls',
            // Un permiso menos cada uno: pedidos-api, que consume `inventario.stock.v1.ajustado` y llama a una operación.
            '--variante', 'sin-pull:pedidos-api:$JS.API.CONSUMER.MSG.NEXT.EVT_INVENTARIO',
            '--variante', 'sin-ack:pedidos-api:$JS.ACK.EVT_INVENTARIO',
            '--variante', 'sin-info-consumidor:pedidos-api:$JS.API.CONSUMER.INFO.EVT_INVENTARIO',
            '--variante', 'sin-api-info:pedidos-api:$JS.API.INFO',
            '--variante', 'sin-inbox:pedidos-api::_INBOX.pedidos-api',
            '--variante', 'sin-publicar:pedidos-api:pedidos.>',
        ]);
    }

    public static function tearDownAfterClass(): void
    {
        self::$broker->stop();
    }

    // ═══ Los eventos de punta a punta, con los permisos exactos ═══════════════════════════════════

    public function testElCicloDeEventosFuncionaConLosPermisosGenerados(): void
    {
        $productor = $this->bus('pedidos-api');
        $recibidos = [];
        $consumidor = $this->bus('inventario-api');
        $consumidor->subscribe('pedidos.pedido.v1.creado', function (FluxEvent $e, HandlerContext $ctx) use (&$recibidos): void {
            $recibidos[] = $e;
        });

        $evento = $productor->publish('pedidos.pedido.v1.creado', ['pedidoId' => 'ped-1', 'totalCents' => 9990], aggregateId: 'ped-1');
        $consumidor->run(maxMessages: 1, idleTimeoutMs: 3_000);

        self::assertCount(1, $recibidos, 'el consumidor debía recibir el evento');
        self::assertSame($evento->id, $recibidos[0]->id);
        self::assertSame(['pedidoId' => 'ped-1', 'totalCents' => 9990], $recibidos[0]->data);

        // El ack llegó (no quedó pendiente): sin `$JS.ACK…` el mensaje se reentregaría cada ack_wait.
        $ops = $this->ops();
        $info = $ops->api('$JS.API.CONSUMER.INFO.EVT_PEDIDOS.inventario-api__pedidos_pedido_v1_creado');
        self::assertSame(0, $info['num_ack_pending'], 'el evento se procesó y debe estar confirmado');
        self::assertSame(0, $info['num_pending']);
        $ops->close();
    }

    public function testUnEventoPermanenteVaALaDlqSinReintentos(): void
    {
        $productor = $this->bus('pedidos-api');
        $consumidor = $this->bus('facturacion-api');
        $consumidor->subscribe('pedidos.pedido.v1.cancelado', static function (): void {
            throw new PermanentError('pedido ya cancelado', 'PEDIDO_YA_CANCELADO');
        });
        $e = $productor->publish('pedidos.pedido.v1.cancelado', ['pedidoId' => 'ped-dlq'], aggregateId: 'ped-dlq');
        $consumidor->run(maxMessages: 1, idleTimeoutMs: 3_000);

        $ops = $this->ops();
        $info = $ops->api('$JS.API.STREAM.INFO.DLQ_PEDIDOS', ['subjects_filter' => 'dlq.pedidos.pedido.v1.cancelado']);
        self::assertSame(1, $info['state']['subjects']['dlq.pedidos.pedido.v1.cancelado'] ?? 0, 'el evento muerto está en dlq.<subject>');
        $raw = $ops->api('$JS.API.STREAM.MSG.GET.DLQ_PEDIDOS', ['last_by_subj' => 'dlq.pedidos.pedido.v1.cancelado']);
        $muerto = json_decode((string) base64_decode($raw['message']['data'], true), true);
        self::assertSame($e->id, $muerto['id'], 'la DLQ conserva el id original');
        self::assertSame('permanent', $muerto['dlqreason']);
        self::assertSame('facturacion-api__pedidos_pedido_v1_cancelado', $muerto['dlqconsumer']);
        self::assertStringStartsWith('PEDIDO_YA_CANCELADO:', $muerto['dlqerror']);
        self::assertSame('data', array_key_last($muerto), '`data` es el último atributo también en la DLQ');
        $ops->close();
    }

    public function testElMismoHechoImportadoDaElMismoIdYElBrokerLoDeduplica(): void
    {
        $bus = $this->bus('pedidos-api');
        $a = $bus->publish('pedidos.pedido.v1.creado', ['pedidoId' => 'ext-1'], time: '2026-08-20T10:25:39.412Z', externalKey: 'pasarela:evt_1Nx');
        $b = $bus->publish('pedidos.pedido.v1.creado', ['pedidoId' => 'ext-1'], time: '2026-08-20T10:25:39.412Z', externalKey: 'pasarela:evt_1Nx');
        $otro = $bus->publish('pedidos.pedido.v1.creado', ['pedidoId' => 'ext-1'], time: '2026-08-20T10:25:39.413Z', externalKey: 'pasarela:evt_1Nx');

        self::assertSame($a->id, $b->id, 'el reenvío del proveedor da el mismo id');
        self::assertNotSame($a->id, $otro->id, 'otro instante, otro id');
        self::assertSame('/test/pedidos-api', $a->source);
        $this->expectException(\Flux\EnvelopeException::class);
        $this->expectExceptionMessage('exige `time`');
        $bus->publish('pedidos.pedido.v1.creado', ['x' => 1], externalKey: 'sin-time');
    }

    // ═══ Inbox privado (06-security.md §3.3) ═════════════════════════════════════════════════════

    public function testElInboxEsPrivadoYEsLaUnicaSuscripcionDelServicio(): void
    {
        $bus = $this->bus('pedidos-api');
        $bus->publish('pedidos.pedido.v1.creado', ['pedidoId' => 'inbox-1']); // abre el buzón único
        $subs = self::$broker->subscriptionsOf('pedidos-api@test');

        self::assertNotSame([], $subs, 'el servidor debe ver la conexión del servicio');
        foreach ($subs as $s) {
            self::assertMatchesRegularExpression('/^_INBOX\.pedidos-api\.[0-9a-f]{16}\.\*\z/', $s, 'la ÚNICA suscripción es el buzón único `_INBOX.<servicio>.<nuid>.*`');
        }
        self::assertCount(1, $subs, 'una sola suscripción: abrir otra que case con el reply rompe `no responders` (A-101)');
        self::assertNotContains('_INBOX.>', $subs);
    }

    public function testConElInboxComunDeLasLibreriasElBrokerLoDenieg(): void
    {
        // El sentido contrario: lo que haría el SDK si el prefijo fuese `_INBOX` (el de cualquier ejemplo de NATS). No es una opción.
        $c = new Connection(new ConnectionOptions(
            servers: [self::$broker->url()],
            name: 'pedidos-api@test',
            inboxPrefix: '_INBOX',
            credentials: Credentials::from(['nkeySeed' => self::$broker->nkeySeed('pedidos-api')]),
            tls: new TlsOptions(caFile: self::$broker->ca()),
        ));
        $c->connect();
        try {
            $c->request('$JS.API.INFO', '', 2_000);
            self::fail('con `_INBOX.>` común el broker debía denegar la suscripción al buzón');
        } catch (PermissionDeniedException $e) {
            self::assertSame('subscribe', $e->operation);
            self::assertStringStartsWith('_INBOX.', $e->subject);
            self::assertSame('PERMISSION_DENIED', $e->errorCode);
        } finally {
            $c->close();
        }
    }

    public function testSinPermisoSobreSuInboxConnectFallaConPermissionDenied(): void
    {
        // Sin el inbox ninguna respuesta puede llegar: no es un timeout, es PERMISSION_DENIED y nombra el subject.
        [$ms, $error] = self::medir(fn () => $this->bus('pedidos-api', [], self::$broker->variantSeed('sin-inbox')));

        self::assertInstanceOf(PermissionDeniedException::class, $error, (string) $error);
        self::assertSame('subscribe', $error->operation);
        self::assertMatchesRegularExpression('/^_INBOX\.pedidos-api\.[0-9a-f]{16}\.\*\z/', $error->subject);
        self::assertLessThan(1_500, $ms, 'debe fallar en milisegundos, no esperar un timeout');
    }

    public function testConUnaAclAnteriorALaV11ConnectFallaConPermissionDenied(): void
    {
        // Una ACL de la v1 no concede `$JS.API.INFO`: connect() lo pregunta y el broker lo deniega.
        [$ms, $error] = self::medir(fn () => $this->bus('pedidos-api', [], self::$broker->variantSeed('sin-api-info')));

        self::assertInstanceOf(PermissionDeniedException::class, $error, (string) $error);
        self::assertSame('$JS.API.INFO', $error->subject);
        self::assertSame('publish', $error->operation);
        self::assertStringContainsString('ACL generada antes de la v1.1', $error->getMessage());
        self::assertLessThan(1_500, $ms);
    }

    // ═══ PERMISSION_DENIED (04-errors.md §5) ═════════════════════════════════════════════════════

    public function testPublicarEnUnDominioAjenoFallaRapidoConPermissionDenied(): void
    {
        $bus = $this->bus('pedidos-api'); // dueño de `pedidos`, no de `facturacion`
        [$ms, $error] = self::medir(fn () => $bus->publish('facturacion.factura.v2.emitida', ['facturaId' => 'f-1']));

        self::assertInstanceOf(PermissionDeniedException::class, $error, (string) $error);
        self::assertSame('PERMISSION_DENIED', $error->errorCode);
        self::assertSame(\Flux\ErrorClass::Permanent, $error->fluxClass());
        // Lo que intentaba y el subject denegado, completo: `$JS.API.STREAM.INFO.EVT_FACTURACION` (comprobar el stream del dominio).
        self::assertStringContainsString('$JS.API.STREAM.INFO.EVT_FACTURACION', $error->subject);
        self::assertStringContainsString('06-security.md', $error->getMessage());
        self::assertLessThan(1_500, $ms, 'una petición denegada falla en ms, no por timeout');
    }

    public function testElPermisoDePublicacionQuitadoSeNota(): void
    {
        $bus = $this->bus('pedidos-api', [], self::$broker->variantSeed('sin-publicar'));
        [, $error] = self::medir(fn () => $bus->publish('pedidos.pedido.v1.creado', ['pedidoId' => 'x']));

        self::assertInstanceOf(PermissionDeniedException::class, $error, (string) $error);
        self::assertSame('pedidos.pedido.v1.creado', $error->subject);
        self::assertSame('publish', $error->operation);
    }

    // ═══ subscribe() falla si se deniega el pull (06-security.md §3.7) ═══════════════════════════

    public function testSubscribeFallaConPermissionDeniedSiSeDeniegaElPull(): void
    {
        $logger = new ArrayLogger();
        $bus = $this->bus('pedidos-api', [], self::$broker->variantSeed('sin-pull'), $logger);
        [$ms, $error] = self::medir(fn () => $bus->subscribe(self::SUBJECT_AJUSTADO, static function (): void {}));

        self::assertInstanceOf(PermissionDeniedException::class, $error, 'el pull denegado deja un consumidor inerte que parece sano: DEBE fallar');
        self::assertSame('$JS.API.CONSUMER.MSG.NEXT.' . self::PEDIDOS_CONSUMER_STREAM . '.' . self::PEDIDOS_DURABLE, $error->subject, 'el subject COMPLETO, sin normalizar');
        self::assertLessThan(1_500, $ms, 'determinista: PING/PONG, no un temporizador');
        self::assertTrue($logger->contains('CONSUMER.MSG.NEXT', 'error'), 'además se registra en el logger');
        self::assertTrue($logger->contains('parece sano', 'error'));
    }

    public function testSubscribeConTodosSusPermisosNoFalla(): void
    {
        // El otro sentido: ninguna denegación que no existe.
        $logger = new ArrayLogger();
        $bus = $this->bus('pedidos-api', [], null, $logger);
        $sub = $bus->subscribe(self::SUBJECT_AJUSTADO, static function (): void {});

        self::assertSame(self::PEDIDOS_DURABLE, $sub->durable);
        self::assertSame([], $logger->messages('error'));
    }

    public function testElAckDenegadoSeRegistraUnaVezPorSubjectYElEventoNoQuedaConfirmado(): void
    {
        $logger = new ArrayLogger();
        $bus = $this->bus('pedidos-api', [], self::$broker->variantSeed('sin-ack'), $logger);
        $bus->subscribe(self::SUBJECT_AJUSTADO, static function (): void {});
        $inventario = $this->bus('inventario-api');
        $inventario->publish(self::SUBJECT_AJUSTADO, ['sku' => 'A-1']);
        $inventario->publish(self::SUBJECT_AJUSTADO, ['sku' => 'A-2']);

        $bus->run(maxMessages: 2, idleTimeoutMs: 3_000);
        $conn = self::connection($bus);
        $conn->flush();
        $bus->metrics(); // (sin efecto: fuerza a que el SDK haya leído lo pendiente)
        $conn->pump(300);

        $avisos = array_values(array_filter($logger->messages('error'), static fn (string $m): bool => str_contains($m, '$JS.ACK.')));
        self::assertCount(1, $avisos, 'una vez por subject: un log por mensaje entierra la causa. ' . implode(' | ', $logger->messages()));
        self::assertStringContainsString('$JS.ACK.' . self::PEDIDOS_CONSUMER_STREAM . '.' . self::PEDIDOS_DURABLE . '.>', $avisos[0], 'el subject del ack, normalizado a `….>`');
        self::assertStringContainsString('REENTREGA', $avisos[0], 'dice la consecuencia: se reentrega cada ack_wait, para siempre');

        $ops = $this->ops();
        $info = $ops->api('$JS.API.CONSUMER.INFO.' . self::PEDIDOS_CONSUMER_STREAM . '.' . self::PEDIDOS_DURABLE);
        self::assertSame(2, $info['num_ack_pending'], 'sin el permiso de ack los dos eventos siguen pendientes');
        $ops->close();
    }

    public function testSinPermisoDeConsultarElConsumidorEsPermissionDeniedYNoNotProvisioned(): void
    {
        // La frontera de 04-errors.md §5: si ni siquiera se puede CONSULTAR el recurso no se sabe si falta. El consumidor EXISTE.
        $bus = $this->bus('pedidos-api', [], self::$broker->variantSeed('sin-info-consumidor'));
        [, $error] = self::medir(fn () => $bus->subscribe(self::SUBJECT_AJUSTADO, static function (): void {}));

        self::assertInstanceOf(PermissionDeniedException::class, $error, (string) $error);
        self::assertNotInstanceOf(\Flux\NotProvisionedException::class, $error);
        self::assertSame('$JS.API.CONSUMER.INFO.' . self::PEDIDOS_CONSUMER_STREAM . '.' . self::PEDIDOS_DURABLE, $error->subject);
    }

    // ═══ Los tres códigos de connect() (04-errors.md §5.3) ═══════════════════════════════════════

    /** @param array<string,mixed> $extra */
    private function rechazo(array $extra, string $razon): ConnectionRejectedException
    {
        [$ms, $error] = self::medir(fn () => $this->bus('pedidos-api', $extra));
        self::assertInstanceOf(ConnectionRejectedException::class, $error, 'connect() debía fallar con CONNECTION_REJECTED: ' . ($error === null ? 'conectó' : get_class($error) . ': ' . $error->getMessage()));
        self::assertSame('CONNECTION_REJECTED', $error->errorCode);
        self::assertSame($razon, $error->reason);
        self::assertSame(\Flux\ErrorClass::Permanent, $error->fluxClass());
        self::assertLessThan(3_000, $ms, "tardó {$ms} ms: parece que reintenta en vez de fallar");
        self::assertStringContainsString('Reintentar no lo arregla', $error->getMessage(), 'DEBE decir que reintentar no lo arregla, para que nadie envuelva connect() en un bucle');

        return $error;
    }

    public function testUnaCredencialBienFormadaPeroDesconocidaEsConnectionRejectedAuth(): void
    {
        // La semilla de un usuario que no existe en la account: bien formada, equivocada. NUNCA INVALID_CREDENTIALS.
        $desconocida = \Flux\Nats\Nkeys::generateSeed();
        $e = $this->rechazo(['credentials' => ['nkeySeed' => $desconocida]], 'auth');

        self::assertStringContainsString('credenciales', $e->getMessage());
        self::assertStringContainsString('Authorization Violation', $e->getMessage(), 'la causa tal como la da el broker');
        self::assertStringNotContainsString($desconocida, $e->getMessage(), 'nunca el valor de una credencial');
    }

    public function testLasCredencialesDeCreds(): void
    {
        // Un `.creds` (JWT + nkey) —el recomendado— conecta igual que la semilla sola.
        $semilla = self::$broker->nkeySeed('pedidos-api');
        $ruta = self::$broker->dir() . '/pedidos-api.creds';
        file_put_contents($ruta, "-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QifQ.e30.relleno\n------END NATS USER JWT------\n\n-----BEGIN USER NKEY SEED-----\n{$semilla}\n------END USER NKEY SEED------\n");
        $bus = $this->bus('pedidos-api', ['credentials' => ['creds' => $ruta]]);
        self::assertTrue($bus->isConnected());
        self::assertNotNull($bus->publish('pedidos.pedido.v1.creado', ['pedidoId' => 'creds'])->id);

        $contenido = (string) file_get_contents($ruta);
        $bus2 = $this->bus('pedidos-api', ['credentials' => ['credsContent' => $contenido]]);
        self::assertTrue($bus2->isConnected());
    }

    /** @return iterable<string,array{array<string,mixed>,string}> */
    public static function credencialesMalFormadas(): iterable
    {
        yield 'un nombre de opción equivocado no degrada a «sin credenciales»' => [['password' => 'x'], 'claves desconocidas: password'];
        yield 'una variable de entorno sin definir (vacío)' => [['nkeySeed' => ''], 'texto no vacío'];
        yield 'user sin pass' => [['user' => 'a'], '`user` y `pass` van juntos'];
        yield 'pass sin user' => [['pass' => 'a'], '`user` y `pass` van juntos'];
        yield 'más de un método a la vez' => [['nkeySeed' => 'SUAAAA', 'token' => 't'], 'elige UN método'];
        yield 'una semilla que no es de usuario' => [['nkeySeed' => 'SAAIBDPBAUTWCWBKIO6XHQNINK5FWJW4OHLXC3HQ2KFE4PEJUA44CNHTAM'], 'USUARIO'];
        yield 'una semilla truncada' => [['nkeySeed' => 'SUAIBDPBAUTWCWBKIO6XHQ'], 'semilla'];
        yield 'un .creds que no existe' => [['creds' => '/no/existe/servicio.creds'], 'no es un fichero legible'];
    }

    /** @param array<string,mixed> $credenciales */
    #[\PHPUnit\Framework\Attributes\DataProvider('credencialesMalFormadas')]
    public function testUnaCredencialMalFormadaEsInvalidCredentialsAntesDeAbrirNingunSocket(array $credenciales, string $contiene): void
    {
        // Un puerto en el que no escucha nadie y una espera corta: si la validación fuese DESPUÉS de dialar, el fallo sería otro y tardaría.
        [$ms, $error] = self::medir(fn () => $this->bus('pedidos-api', ['credentials' => $credenciales, 'servers' => 'nats://127.0.0.1:1', 'waitForBrokerMs' => 1_500]));

        self::assertInstanceOf(InvalidCredentialsException::class, $error, $error === null ? 'conectó' : get_class($error) . ': ' . $error->getMessage());
        self::assertSame('INVALID_CREDENTIALS', $error->errorCode);
        self::assertStringContainsString($contiene, $error->getMessage());
        self::assertLessThan(300, $ms, 'sin abrir ningún socket: no espera a nadie');
    }

    public function testUnCredsSinSemillaEsInvalidCredentials(): void
    {
        $ruta = self::$broker->dir() . '/sin-semilla.creds';
        file_put_contents($ruta, "-----BEGIN NATS USER JWT-----\neyJ.e30.x\n------END NATS USER JWT------\n");
        [, $error] = self::medir(fn () => $this->bus('pedidos-api', ['credentials' => ['creds' => $ruta]]));

        self::assertInstanceOf(InvalidCredentialsException::class, $error);
        self::assertStringContainsString('semilla de usuario', $error->getMessage());
    }

    public function testSinCaElCertificadoNoVerificaYFallaConConnectionRejectedTls(): void
    {
        // El broker usa una CA propia; sin `tls.caFile` se verifica contra las del sistema, que no la conocen. Antes de la Fase 6 esto
        // dejaba al servicio arrancado y mudo (o, con la librería, conectaba SIN verificar nada).
        $e = $this->rechazo(['tls' => []], 'tls');

        self::assertStringContainsString('tls.caFile', $e->getMessage());
        self::assertStringContainsString('SAN', $e->getMessage());
    }

    public function testConLaCaEquivocadaFalla(): void
    {
        $this->rechazo(['tls' => ['caFile' => self::$broker->otraCa()]], 'tls');
    }

    public function testUnCertificadoQueNoCubreElHostFallaAunqueLaCaSeaLaCorrecta(): void
    {
        $e = $this->rechazo(['tls' => ['caFile' => self::$broker->ca()], 'servers' => self::$broker->extra('altname')['url']], 'tls');

        self::assertStringContainsString('SAN', $e->getMessage());
    }

    public function testSePideTlsYElServidorNoLoOfreceFallaYNoBajaATextoPlano(): void
    {
        $e = $this->rechazo(['tls' => ['caFile' => self::$broker->ca()], 'servers' => self::$broker->extra('plano')['url']], 'tls');

        self::assertStringContainsString('no lo ofrece', $e->getMessage());
        // …y tampoco con el esquema `tls://`.
        $e2 = $this->rechazo(['tls' => null, 'servers' => str_replace('nats://', 'tls://', self::$broker->extra('plano')['url'])], 'tls');
        self::assertStringContainsString('no lo ofrece', $e2->getMessage());
    }

    public function testSinPedirTlsContraUnBrokerSinTlsConectaEnClaro(): void
    {
        // El otro sentido de la fila anterior: no se EXIGE TLS a quien no lo pidió y el servidor no lo ofrece.
        $bus = $this->bus('pedidos-api', ['tls' => null, 'servers' => self::$broker->extra('plano')['url']]);

        self::assertTrue($bus->isConnected());
    }

    public function testElBrokerQueExigeCertificadoDeClienteLoRechazaSiNoSePresentaNinguno(): void
    {
        $mtls = self::$broker->extra('mtls');
        // DEBERÍA: no todos los clientes NATS lo distinguen de un timeout. Éste sí: el handshake falla.
        $this->rechazo(['tls' => ['caFile' => self::$broker->ca()], 'servers' => $mtls['url']], 'tls');

        // Y con certificado y clave de cliente, conecta.
        $bus = $this->bus('pedidos-api', ['tls' => ['caFile' => self::$broker->ca(), 'certFile' => $mtls['certFile'], 'keyFile' => $mtls['keyFile']], 'servers' => $mtls['url']]);
        self::assertTrue($bus->isConnected());
    }

    /** @return iterable<string,array{array<string,mixed>,string}> */
    public static function tlsMalFormado(): iterable
    {
        yield 'una CA que no existe' => [['caFile' => '/no/existe/ca.pem'], 'no es un fichero legible'];
        yield 'una ruta vacía' => [['caFile' => ''], 'ruta no vacía'];
        yield 'un fichero que no es un PEM' => [['caFile' => __DIR__ . '/../../composer.json'], 'no es un PEM'];
        yield 'un nombre de opción desconocido' => [['ca_file' => 'x'], 'claves desconocidas: ca_file'];
    }

    public function testUnCertificadoDeClienteSinSuClaveEsInvalidTlsOptions(): void
    {
        // Un PEM de verdad (el de la CA) como certificado de cliente, y sin `keyFile`: van juntos.
        [, $error] = self::medir(fn () => $this->bus('pedidos-api', ['tls' => ['caFile' => self::$broker->ca(), 'certFile' => self::$broker->ca()], 'servers' => 'nats://127.0.0.1:1', 'waitForBrokerMs' => 1_000]));

        self::assertInstanceOf(InvalidTlsOptionsException::class, $error, $error === null ? 'conectó' : get_class($error));
        self::assertStringContainsString('`certFile` y `keyFile` van juntos', $error->getMessage());
    }

    /** @param array<string,mixed> $tls */
    #[\PHPUnit\Framework\Attributes\DataProvider('tlsMalFormado')]
    public function testUnTlsMalFormadoEsInvalidTlsOptionsAntesDeAbrirNingunSocket(array $tls, string $contiene): void
    {
        [$ms, $error] = self::medir(fn () => $this->bus('pedidos-api', ['tls' => $tls, 'servers' => 'nats://127.0.0.1:1', 'waitForBrokerMs' => 1_500]));

        self::assertInstanceOf(InvalidTlsOptionsException::class, $error, $error === null ? 'conectó' : get_class($error) . ': ' . $error->getMessage());
        self::assertSame('INVALID_TLS_OPTIONS', $error->errorCode);
        self::assertMatchesRegularExpression('/' . $contiene . '/', $error->getMessage());
        self::assertLessThan(300, $ms);
    }

    public function testUnBrokerQueAunNoHaArrancadoSeEsperaYNoEsNingunoDeLosTres(): void
    {
        // Ninguno de los tres códigos: no es un error, se espera (03-delivery.md §6.1, primera fila).
        [$ms, $error] = self::medir(fn () => $this->bus('pedidos-api', ['servers' => 'nats://127.0.0.1:1', 'tls' => null, 'credentials' => null, 'waitForBrokerMs' => 700]));

        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertNotInstanceOf(ConnectionRejectedException::class, $error);
        self::assertGreaterThanOrEqual(650, $ms, 'esperó (con jitter) en vez de morir en el primer intento');
        self::assertNull($error instanceof \Flux\FluxError ? $error->errorCode : null);
    }

    public function testElBrokerQueArrancaTardeSeEsperaYSeConecta(): void
    {
        $nats = getenv('NATS_SERVER') ?: 'nats-server';
        $dir = self::$broker->dir() . '/tarde';
        @mkdir($dir);
        $puerto = 7699;
        $proc = proc_open(
            ['sh', '-c', 'sleep 1.3; exec "$0" -js -a 127.0.0.1 -p ' . $puerto . ' -sd ' . escapeshellarg($dir), $nats],
            [0 => ['pipe', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']],
            $pipes,
        );
        try {
            [$ms, $error, $bus] = self::medir(fn () => $this->bus('pedidos-api', ['servers' => "nats://127.0.0.1:{$puerto}", 'tls' => null, 'credentials' => null, 'waitForBrokerMs' => 15_000]));

            self::assertNull($error, (string) $error);
            self::assertTrue($bus->isConnected());
            self::assertGreaterThanOrEqual(1_200, $ms, 'esperó a que el broker arrancase');
        } finally {
            if (is_resource($proc)) {
                proc_terminate($proc, SIGKILL);
                proc_close($proc);
            }
        }
    }
}
