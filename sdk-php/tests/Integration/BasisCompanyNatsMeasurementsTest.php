<?php

declare(strict_types=1);

namespace Flux\Tests\Integration;

use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

/**
 * LO QUE `basis-company/nats` NO HACE, medido contra un `nats-server` real con TLS, nkeys y las ACLs de `gen-acl.mjs`
 * (Apéndice A del plan v1.1, A-220 a A-228; `conformance/cases/php-nats-client.json`).
 *
 * No son pruebas del SDK: son **la evidencia de por qué el SDK de PHP trae su propio cliente** (`Flux\Nats\Connection`) en lugar de
 * apoyarse en la librería para la capacidad `security`. Cada test **fija un comportamiento medido de la librería (1.2.9)**: si una versión
 * futura lo arregla, el test falla y dice que la decisión se puede revisar. Un hallazgo que nadie ejecuta es una opinión.
 */
#[Group('integration')]
final class BasisCompanyNatsMeasurementsTest extends TestCase
{
    private static SecureBroker $broker;

    public static function setUpBeforeClass(): void
    {
        if (!class_exists(\Basis\Nats\Client::class)) {
            self::markTestSkipped('falta basis-company/nats (composer require --dev basis-company/nats)');
        }
        self::$broker = SecureBroker::start(['--sin-tls', '--altname-malo']);
    }

    public static function tearDownAfterClass(): void
    {
        if (isset(self::$broker)) {
            self::$broker->stop();
        }
    }

    /** @param array<string,mixed> $extra */
    private function client(array $extra = [], string $url = null, string $service = 'pedidos-api'): \Basis\Nats\Client
    {
        $p = parse_url($url ?? self::$broker->url());
        $cfg = new \Basis\Nats\Configuration($extra + [
            'host' => $p['host'], 'port' => $p['port'], 'timeout' => 2, 'reconnect' => true, 'maxReconnectAttempts' => 3,
            'nkey' => self::$broker->nkeySeed($service), 'tlsCaFile' => self::$broker->ca(),
            'inboxPrefix' => "_INBOX.{$service}",
        ]);
        $c = new \Basis\Nats\Client($cfg);
        $c->setName($service);

        return $c;
    }

    /** @return array{0:float,1:?\Throwable,2:mixed} */
    private static function medir(callable $f): array
    {
        $t0 = microtime(true);
        try {
            $v = $f();
            $e = null;
        } catch (\Throwable $x) {
            $v = null;
            $e = $x;
        }

        return [(microtime(true) - $t0) * 1000, $e, $v];
    }

    public function testA220LaPeticionSeSuscribeAUnInboxEscritoAFuegoYLaOpcionInboxPrefixNoLoToca(): void
    {
        // La ACL del servicio solo concede `_INBOX.pedidos-api.>`. La librería se suscribe a `_REQS.<hex>.*` —también para la confirmación de un
        // publish por JetStream y para cada llamada a la API—, así que NINGUNA petición funciona con el inbox privado.
        $c = $this->client();
        [, $error] = self::medir(function () use ($c) {
            $c->request('$JS.API.INFO', '', static function (): void {});
        });

        self::assertInstanceOf(\LogicException::class, $error);
        self::assertStringContainsString('Permissions Violation for Subscription to "_REQS.', $error->getMessage());
    }

    public function testA221LaVerificacionDelNombreDeHostEstaDesactivada(): void
    {
        // CA correcta, y un certificado que NO cubre 127.0.0.1 (SAN `otro.example`): debería fallar. La librería conecta: `verify_peer_name = false`.
        $c = $this->client([], self::$broker->extra('altname')['url']);
        [, $error, $ok] = self::medir(fn () => $c->ping());

        self::assertNull($error, 'la librería ya verifica el host: ' . $error?->getMessage());
        self::assertTrue($ok, 'conectó con un certificado que no cubre el host');
    }

    public function testA222SinTlsCaFileNoVerificaNada(): void
    {
        // Sin `tlsCaFile` la librería pone `verify_peer = false`: acepta el certificado de cualquiera (y `tls: []` —las CA del sistema— no existe).
        $c = $this->client(['tlsCaFile' => null]);
        [, $error, $ok] = self::medir(fn () => $c->ping());

        self::assertNull($error, 'la librería ya verifica sin CA: ' . $error?->getMessage());
        self::assertTrue($ok, 'conectó con un certificado de una CA desconocida, sin verificar');
    }

    public function testA223SePideTlsYElServidorNoLoOfreceBajaATextoPlano(): void
    {
        $c = $this->client(['reconnect' => false], self::$broker->extra('plano')['url']);
        [, $error, $ok] = self::medir(fn () => $c->ping());

        self::assertNull($error);
        self::assertTrue($ok, 'conectó en claro pese a pedir TLS: no hay forma de EXIGIRlo');
    }

    public function testA224UnaCredencialRechazadaYUnaCaAjenaFallanRapidoPeroSinCodigoNiMotivo(): void
    {
        // Lo que SÍ hace bien (y no hace falta reescribirlo): fallar rápido. Lo que no hace: distinguirlo. `LogicException` con el texto del broker y
        // `Exception('Failed to connect: Error enabling TLS')` (más un warning de PHP por stderr): el SDK no puede dar `CONNECTION_REJECTED` con su motivo sin
        // interpretar texto.
        $semilla = \Flux\Nats\Nkeys::generateSeed();
        [$ms1, $e1] = self::medir(fn () => $this->client(['nkey' => $semilla])->ping());
        self::assertInstanceOf(\LogicException::class, $e1);
        self::assertSame('Authorization Violation', $e1->getMessage());
        self::assertLessThan(1_500, $ms1, 'con el reintento por defecto NO se cuelga: falla en ms');

        $ajena = self::$broker->otraCa();
        $prev = set_error_handler(static fn (): bool => true); // el warning de PHP del handshake
        try {
            [$ms2, $e2] = self::medir(fn () => $this->client(['tlsCaFile' => $ajena])->ping());
        } finally {
            restore_error_handler();
            unset($prev);
        }
        self::assertInstanceOf(\Exception::class, $e2);
        self::assertSame('Failed to connect: Error enabling TLS', $e2->getMessage());
        self::assertLessThan(1_500, $ms2);
    }

    public function testA225ElConnectNoAnunciaNoResponders(): void
    {
        $opciones = (new \Basis\Nats\Configuration())->getOptions();

        self::assertTrue($opciones['headers']);
        self::assertArrayNotHasKey('no_responders', $opciones, 'sin `no_responders` en CONNECT, RPC_UNAVAILABLE sería un timeout (A-57)');
    }

    public function testA226UnaDenegacionLlegaComoLogicExceptionEnLaSiguienteLecturaSinDecirDeQueOperacion(): void
    {
        // Con un inbox que SÍ funciona (el usuario de operación) se aísla lo que importa: el `-ERR` de una publicación denegada.
        $c = $this->client(['nkey' => self::$broker->nkeySeed('crm-api'), 'inboxPrefix' => '_INBOX.crm-api'], null, 'crm-api');
        $c->publish('pedidos.pedido.v1.creado', '{}'); // crm-api no es dueño de `pedidos`: la ACL la descarta
        [, $error] = self::medir(fn () => $c->ping());

        self::assertInstanceOf(\LogicException::class, $error, 'la denegación sale en la lectura SIGUIENTE, de otra operación');
        self::assertSame('Permissions Violation for Publish to "pedidos.pedido.v1.creado"', $error->getMessage());
    }

    /**
     * Lo que la librería SÍ hace, contra un broker ABIERTO (sin ACL ni TLS): cabeceras en HPUB, el estado de `DIRECT.GET` (404) en las
     * cabeceras de la respuesta y el error de secuencia como cuerpo JSON. Es lo que habría permitido construir el estado sobre ella, **si** el
     * inbox privado no se lo impidiese (A-220): la decisión de escribir el cliente no es por falta de funciones, es por la seguridad.
     */
    public function testA227ConUnBrokerAbiertoLaLibreriaSiHaceCabecerasDirectGetYElChequeoDeSecuencia(): void
    {
        $nats = getenv('NATS_SERVER') ?: 'nats-server';
        $dir = self::$broker->dir() . '/abierto';
        @mkdir($dir);
        $puerto = 7697;
        $proc = proc_open([$nats, '-js', '-a', '127.0.0.1', '-p', (string) $puerto, '-sd', $dir], [0 => ['pipe', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']], $pipes);
        try {
            for ($i = 0; $i < 50 && @fsockopen('127.0.0.1', $puerto, $e, $m, 0.1) === false; $i++) {
                usleep(100_000);
            }
            $c = new \Basis\Nats\Client(new \Basis\Nats\Configuration(['host' => '127.0.0.1', 'port' => $puerto, 'timeout' => 2]));
            $api = static function (string $subject, array $body) use ($c): array {
                $out = null;
                $c->request($subject, json_encode($body), static function ($p) use (&$out): void {
                    $out = json_decode((string) $p, true);
                });

                return $out ?? [];
            };
            $r = $api('$JS.API.STREAM.CREATE.STATE_X', ['name' => 'STATE_X', 'subjects' => ['state.x.>'], 'max_msgs_per_subject' => 1, 'allow_direct' => true, 'storage' => 'file']);
            self::assertArrayNotHasKey('error', $r, json_encode($r));

            // DIRECT.GET de una clave que no existe: la librería entrega el 404 como `Status-Code` de las cabeceras.
            $status = null;
            $c->request('$JS.API.DIRECT.GET.STATE_X.state.x.cli-1', '', static function ($p) use (&$status): void {
                $status = $p->headers['Status-Code'] ?? null;
            });
            self::assertSame('404', $status);

            // HPUB con `Nats-Expected-Last-Subject-Sequence: 0` crea; repetirlo da el error 10071 como cuerpo JSON.
            $publicar = static function (string $id) use ($c): array {
                $out = null;
                $c->request('state.x.cli-1', new \Basis\Nats\Message\Payload('{"v":1}', ['Nats-Expected-Last-Subject-Sequence' => '0', 'Nats-Msg-Id' => $id]), static function ($p) use (&$out): void {
                    $out = json_decode((string) $p, true);
                });

                return $out ?? [];
            };
            self::assertSame(1, $publicar('a')['seq'] ?? null);
            self::assertSame(10071, $publicar('b')['error']['err_code'] ?? null);
        } finally {
            if (is_resource($proc)) {
                proc_terminate($proc, SIGKILL);
                proc_close($proc);
            }
        }
    }
}
