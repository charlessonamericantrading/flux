<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\InvalidCredentialsException;
use Flux\InvalidTlsOptionsException;
use Flux\Nats\Credentials;
use Flux\Nats\Nkeys;
use Flux\Nats\TlsOptions;
use Flux\Transport\SocketTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Lo del cliente de NATS que no necesita broker: las nkeys (con un vector que produjo **Node**, `@nats-io/nkeys`, de forma independiente),
 * la validación de `credentials` y de `tls` —**antes** de abrir ningún socket— y los nombres. Lo que habla con un broker real está en
 * `tests/Integration/`.
 */
final class NatsClientTest extends TestCase
{
    // Un vector de `@nats-io/nkeys` (Node): la semilla, su clave pública y la firma de un nonce, en base64url sin relleno.
    private const SEED = 'SUADVR4RU2L7G7Z4JY3P5WX42JVXZXRN32MXIBTSLUIPMFCELA56SGYDCM';
    private const PUBLIC = 'UB657ZB3G6YLQ5A4OFMUB4L2ARDOFWMYD7IQN6YNTLJFR26DYFXUY3X6';
    private const NONCE = 'rEqJ2bNZ0aTq1xYk';
    private const SIGNATURE = 'ZOEk_O8AF7j1wlcQcifBuwu-8evsFZO5U2QpfvDgxWFcCRAfcglc3wqVnWkep4wM4ElEjBsjn0z-AdGXk2wwBQ';

    protected function setUp(): void
    {
        if (!extension_loaded('sodium')) {
            self::markTestSkipped('las nkeys necesitan ext-sodium');
        }
    }

    public function testLaClavePublicaYLaFirmaSonLasDeNode(): void
    {
        $k = Nkeys::fromSeed(self::SEED);

        self::assertSame(self::PUBLIC, $k['public']);
        self::assertSame(self::SIGNATURE, Nkeys::sign($k['seed'], self::NONCE), 'Ed25519 es determinista: la misma firma que `@nats-io/nkeys`');
    }

    public function testBase32DaVueltaYElCrcDetectaUnCaracterCambiado(): void
    {
        $bytes = random_bytes(37);
        self::assertSame($bytes, Nkeys::base32Decode(Nkeys::base32Encode($bytes)));

        $cambiada = self::SEED;
        $cambiada[10] = $cambiada[10] === 'A' ? 'B' : 'A';
        $this->expectException(\InvalidArgumentException::class);
        Nkeys::fromSeed($cambiada);
    }

    public function testUnaSemillaGeneradaEsDeUsuarioYSePuedeUsar(): void
    {
        $s = Nkeys::generateSeed();

        self::assertStringStartsWith('SU', $s);
        self::assertStringStartsWith('U', Nkeys::fromSeed($s)['public']);
    }

    public function testElCredsSeLeeDeUnFicheroYSeRelee(): void
    {
        $ruta = sys_get_temp_dir() . '/flux-php-' . bin2hex(random_bytes(4)) . '.creds';
        file_put_contents($ruta, "-----BEGIN NATS USER JWT-----\neyJ.e30.x\n------END NATS USER JWT------\n\n-----BEGIN USER NKEY SEED-----\n" . self::SEED . "\n------END USER NKEY SEED------\n");
        try {
            $c = Credentials::from(['creds' => $ruta]);
            self::assertNotNull($c);
            $f = $c->connectFields(self::NONCE);
            self::assertSame(self::PUBLIC, $f['nkey']);
            self::assertSame(self::SIGNATURE, $f['sig']);
            self::assertSame('eyJ.e30.x', $f['jwt']);

            // «Se relee en cada (re)conexión»: rotar el fichero no exige reiniciar.
            $otra = Nkeys::generateSeed();
            file_put_contents($ruta, "-----BEGIN NATS USER JWT-----\neyJ.e30.y\n------END NATS USER JWT------\n\n-----BEGIN USER NKEY SEED-----\n{$otra}\n------END USER NKEY SEED------\n");
            self::assertSame(Nkeys::fromSeed($otra)['public'], $c->connectFields(self::NONCE)['nkey']);
        } finally {
            @unlink($ruta);
        }
    }

    public function testElTokenYElUsuarioNoNecesitanNonce(): void
    {
        self::assertSame(['auth_token' => 't'], Credentials::from(['token' => 't'])?->connectFields(null));
        self::assertSame(['user' => 'u', 'pass' => 'p'], Credentials::from(['user' => 'u', 'pass' => 'p'])?->connectFields(null));
        self::assertNull(Credentials::from([]), 'ninguna credencial es «sin credenciales», no un error');
        self::assertNull(Credentials::from(null));
    }

    /** @return iterable<string,array{array<string,mixed>,string}> */
    public static function credencialesInvalidas(): iterable
    {
        yield 'una clave desconocida' => [['pass' => 'a', 'password' => 'a'], 'claves desconocidas: password'];
        yield 'un valor que no es texto' => [['token' => 5], 'texto no vacío'];
        yield 'dos métodos' => [['token' => 't', 'user' => 'u', 'pass' => 'p'], 'elige UN método'];
        yield 'una semilla de cuenta' => [['nkeySeed' => 'SAAIBDPBAUTWCWBKIO6XHQNINK5FWJW4OHLXC3HQ2KFE4PEJUA44CNHTAM'], 'USUARIO'];
    }

    /** @param array<string,mixed> $opciones */
    #[DataProvider('credencialesInvalidas')]
    public function testUnaCredencialMalFormadaEsInvalidCredentialsYNoCitaSuValor(array $opciones, string $contiene): void
    {
        try {
            Credentials::from($opciones);
            self::fail('debía rechazarla');
        } catch (InvalidCredentialsException $e) {
            self::assertSame('INVALID_CREDENTIALS', $e->errorCode);
            self::assertSame(\Flux\ErrorClass::Permanent, $e->fluxClass());
            self::assertStringContainsString($contiene, $e->getMessage());
            self::assertStringContainsString('antes de abrir', str_replace('sin abrir ningún socket', 'antes de abrir', $e->getMessage()));
            foreach ($opciones as $v) {
                if (is_string($v) && strlen($v) > 6) {
                    self::assertStringNotContainsString($v, $e->getMessage(), 'nunca el valor de una credencial: el mensaje acaba en logs');
                }
            }
        }
    }

    public function testTlsVacioNoEsNullExigeTlsConLasCaDelSistema(): void
    {
        self::assertNull(TlsOptions::from(null));
        $t = TlsOptions::from([]);

        self::assertNotNull($t, '`[]` exige TLS; solo `null` no lo pide');
        self::assertNull($t->caFile);
    }

    public function testTlsMalFormadoEsInvalidTlsOptions(): void
    {
        $pem = sys_get_temp_dir() . '/flux-php-' . bin2hex(random_bytes(4)) . '.pem';
        file_put_contents($pem, "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n");
        try {
            self::assertSame($pem, (new TlsOptions(caFile: $pem))->caFile);
            foreach ([
                fn () => new TlsOptions(caFile: '/no/existe.pem'),
                fn () => new TlsOptions(caFile: ''),
                fn () => new TlsOptions(caFile: __DIR__ . '/../composer.json'),
                fn () => new TlsOptions(certFile: $pem),
                fn () => new TlsOptions(keyFile: $pem),
                fn () => TlsOptions::from(['ca' => $pem]),
                fn () => TlsOptions::from(['caFile' => 5]),
            ] as $i => $f) {
                try {
                    $f();
                    self::fail("el caso {$i} debía ser INVALID_TLS_OPTIONS");
                } catch (InvalidTlsOptionsException $e) {
                    self::assertSame('INVALID_TLS_OPTIONS', $e->errorCode);
                }
            }
        } finally {
            @unlink($pem);
        }
    }

    public function testElInboxEsPrivadoYSeDerivaDelNombreDeServicio(): void
    {
        self::assertSame('_INBOX.pedidos-api', SocketTransport::inboxPrefixFor('pedidos-api'));
        // El nombre de servicio no admite puntos: el prefijo de uno nunca es prefijo de otro (`_INBOX.pedidos.>` no captura `_INBOX.pedidos-api.x`).
        $this->expectException(\InvalidArgumentException::class);
        SocketTransport::inboxPrefixFor('pedidos.api');
    }

    public function testElSubjectDeUnAckSeNormalizaParaAvisarUnaVezPorSubject(): void
    {
        self::assertSame(
            '$JS.ACK.EVT_PEDIDOS.svc__pedidos_pedido_v1_creado.>',
            SocketTransport::normalizeDeniedSubject('$JS.ACK.EVT_PEDIDOS.svc__pedidos_pedido_v1_creado.1.41.7.1787221539412000000.0'),
        );
        self::assertSame('_INBOX.crm-api.>', SocketTransport::normalizeDeniedSubject('_INBOX.crm-api.abc123.17'));
        self::assertSame('rpc.clientes.cliente.v1.obtener', SocketTransport::normalizeDeniedSubject('rpc.clientes.cliente.v1.obtener'));
    }

    public function testLosTresCodigosDeConnectSonPermanentYTienenSuLiteral(): void
    {
        $e = new \Flux\ConnectionRejectedException('tls', 'x');
        self::assertSame('CONNECTION_REJECTED', $e->errorCode);
        self::assertSame('tls', $e->reason);
        self::assertSame(\Flux\ErrorClass::Permanent, $e->fluxClass());
        self::assertSame(\Flux\ErrorClass::Permanent, (new \Flux\PermissionDeniedException('s', 'publish', 'x.y'))->fluxClass());
        self::assertSame(\Flux\ErrorClass::Permanent, (new \Flux\NotProvisionedException('stream', 'EVT_X', null, 's', '$JS.API.STREAM.CREATE.EVT_X'))->fluxClass());
    }
}
