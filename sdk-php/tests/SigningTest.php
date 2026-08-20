<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\Envelope;
use Flux\ErrorClass;
use Flux\FluxEvent;
use Flux\PoisonError;
use Flux\Signing\Base64Url;
use Flux\Signing\Keys;
use Flux\Signing\Signer;
use Flux\Signing\SigningKeyException;
use Flux\Signing\SigningOptions;
use Flux\Signing\VerificationMode;
use Flux\Signing\Verifier;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;

/**
 * Firma Ed25519 — specification/07-signing.md.
 *
 * Se salta entero sin `ext-sodium`, igual que el test de `SOCKET_ECONNRESET` se salta sin
 * `ext-sockets`: la extensión está en el core de PHP desde 7.2 pero muchas distribuciones
 * la empaquetan aparte y en Windows viene comentada en `php.ini`.
 *
 *     php -d extension=sodium vendor/bin/phpunit
 */
final class SigningTest extends TestCase
{
    private const KEY_ID = 'pedidos-api-1';

    // ─── Vector de interoperabilidad FIJO ─────────────────────────────────────
    //
    // Semilla del TEST 1 de RFC 8032, para que cualquiera pueda reproducirlo. La firma de
    // abajo la producen —y la aceptan— este SDK, el de Node (`node:crypto`) y el de Rust
    // (`ed25519-dalek`) sobre los MISMOS bytes, con la MISMA clave en PEM. Es lo que fija
    // la interoperabilidad de verdad: si alguien toca el serializador, el orden de claves
    // o el formato de `time`, este test cae antes que cualquier despliegue.

    private const SEED_HEX = '9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60';

    private const PRIVATE_PEM = "-----BEGIN PRIVATE KEY-----\n"
        . "MC4CAQAwBQYDK2VwBCIEIJ1hsZ3v/VpguoRK9JLsLMREScVpezJpGXA7rAMcrn9g\n"
        . "-----END PRIVATE KEY-----\n";

    private const PUBLIC_PEM = "-----BEGIN PUBLIC KEY-----\n"
        . "MCowBQYDK2VwAyEA11qYAYKxCrfVS/7TyWQHOg7hcvPapiMlrwIaaPcHURo=\n"
        . "-----END PUBLIC KEY-----\n";

    /** Los bytes exactos que se firman en el vector. */
    private const VECTOR_PAYLOAD = '{"specversion":"1.0","id":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",'
        . '"source":"/produccion/pedidos-api","type":"com.flux.pedidos.pedido.creado.v1",'
        . '"time":"2025-08-20T10:25:39.410Z","datacontenttype":"application/json",'
        . '"dataschema":"https://schemas.internal/pedidos/pedido/creado/1.0.0.json",'
        . '"correlationid":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55","tenantid":"acme",'
        . '"producerversion":"3.4.1","dataclassification":"internal",'
        . '"signkeyid":"pedidos-api-1","data":{"pedidoId":"ped-123"}}';

    private const VECTOR_SIGNATURE =
        'Yhv5dV5yVxHz7w2fDuFQodUMhLoB8oPITBDA9t7Y3gAvc0sERbCew_L2JUK7Zy32ZmW3vmfzSPh7RvCY7dCaBA';

    /**
     * El mismo evento tras pasar por la DLQ, byte a byte. El SDK de Rust lleva este mismo
     * literal en `signing::tests`: la firma va **antes** de las `dlq*`, que es el orden de
     * Node, Python, Go, Java y .NET.
     */
    private const DLQ_VECTOR = '{"specversion":"1.0","id":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",'
        . '"source":"/produccion/pedidos-api","type":"com.flux.pedidos.pedido.creado.v1",'
        . '"time":"2025-08-20T10:25:39.410Z","datacontenttype":"application/json",'
        . '"dataschema":"https://schemas.internal/pedidos/pedido/creado/1.0.0.json",'
        . '"correlationid":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55","tenantid":"acme",'
        . '"producerversion":"3.4.1","dataclassification":"internal",'
        . '"signkeyid":"pedidos-api-1",'
        . '"signature":"' . self::VECTOR_SIGNATURE . '",'
        . '"dlqreason":"permanent","dlqattempts":1,'
        . '"dlqconsumer":"facturacion-api__pedidos_pedido_v1_creado",'
        . '"dlqerror":"PEDIDO_YA_CANCELADO","dlqtime":"2025-08-20T10:26:00.000Z",'
        . '"data":{"pedidoId":"ped-123"}}';

    protected function setUp(): void
    {
        if (!extension_loaded('sodium')) {
            self::markTestSkipped(
                'ext-sodium no está cargada; ejecuta `php -d extension=sodium vendor/bin/phpunit`'
            );
        }
    }

    private static function event(): FluxEvent
    {
        return Envelope::build(
            subject: 'pedidos.pedido.v1.creado',
            data: ['pedidoId' => 'ped-123'],
            id: '01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55',
            source: '/produccion/pedidos-api',
            producerversion: '3.4.1',
            tenantid: 'acme',
            dataclassification: 'internal',
            dataschema: 'https://schemas.internal/pedidos/pedido/creado/1.0.0.json',
            correlationid: '01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55',
            time: '2025-08-20T10:25:39.410Z',
        );
    }

    private static function signer(): Signer
    {
        $signer = Signer::fromOptions(
            new SigningOptions(privateKey: self::PRIVATE_PEM, keyId: self::KEY_ID)
        );
        self::assertNotNull($signer);

        return $signer;
    }

    private static function verifier(
        VerificationMode $mode = VerificationMode::Require,
        ?AbstractLogger $logger = null,
    ): Verifier {
        $verifier = Verifier::fromOptions(
            new SigningOptions(
                publicKeys: [self::KEY_ID => self::PUBLIC_PEM],
                verify: $mode,
            ),
            $logger,
        );
        self::assertNotNull($verifier);

        return $verifier;
    }

    /** @return string El `errorCode` del POISON que lanzó `check()`. */
    private static function codeOf(callable $fn): string
    {
        try {
            $fn();
        } catch (PoisonError $e) {
            self::assertSame(ErrorClass::Poison, $e->fluxClass());

            return $e->errorCode ?? '<sin código>';
        }

        self::fail('se esperaba un PoisonError y no se lanzó ninguno');
    }

    // ─── Interoperabilidad ────────────────────────────────────────────────────

    /**
     * **El test que fija la interoperabilidad entre los seis SDKs.**
     *
     * Si esto pasa, una firma de PHP verifica en Node y en Rust, y al revés — sobre la
     * misma clave PEM y los mismos bytes.
     */
    public function testElVectorFijoProduceLaMismaFirmaQueNodeYRust(): void
    {
        $firmado = self::signer()->sign(self::event());

        self::assertSame(
            self::VECTOR_PAYLOAD,
            Envelope::signablePayload($firmado),
            'los bytes canónicos han cambiado: cualquier firma emitida antes deja de verificar'
        );
        self::assertSame(self::VECTOR_SIGNATURE, $firmado->signature);
    }

    /** Y al revés: la firma que produjeron Node y Rust verifica aquí. */
    public function testElVectorFijoVerificaVengaDeDondeVenga(): void
    {
        $ajeno = self::event()->withSignature(self::KEY_ID, self::VECTOR_SIGNATURE);

        self::assertNull(self::verifier()->check($ajeno));
    }

    /**
     * El PEM y la clave cruda son el mismo material.
     *
     * Importa porque un gestor de secretos entrega lo que le da la gana, y porque
     * `sodium_crypto_sign_keypair()` devuelve bytes crudos: sin esto, el operador tendría
     * que convertir a mano en cada rotación.
     */
    public function testPemYBase64CrudoSonLaMismaClave(): void
    {
        $seed = (string) hex2bin(self::SEED_HEX);

        $conPem = self::signer()->sign(self::event());
        $conCrudo = Signer::fromOptions(
            new SigningOptions(privateKey: base64_encode($seed), keyId: self::KEY_ID)
        )?->sign(self::event());

        self::assertSame($conPem->signature, $conCrudo?->signature);
    }

    /** La "secret key" de 64 bytes de libsodium (semilla ‖ pública) también. */
    public function testLaSecretKeyDe64BytesDeLibsodiumTambienVale(): void
    {
        $sk = (string) hex2bin(self::SEED_HEX) . Keys::publicKeyBytes(self::PUBLIC_PEM);
        self::assertSame(64, strlen($sk));

        $firmado = Signer::fromOptions(
            new SigningOptions(privateKey: base64_encode($sk), keyId: self::KEY_ID)
        )?->sign(self::event());

        self::assertSame(self::VECTOR_SIGNATURE, $firmado?->signature);
    }

    /** El PEM que genera el SDK es exactamente el que produce `openssl genpkey`. */
    public function testElParGeneradoEsPemInteroperable(): void
    {
        $par = Keys::generateKeyPair();

        self::assertStringStartsWith('-----BEGIN PRIVATE KEY-----', $par['privateKeyPem']);
        self::assertStringStartsWith('-----BEGIN PUBLIC KEY-----', $par['publicKeyPem']);

        // La pública del PEM es la que se deriva de la privada.
        $derivada = sodium_crypto_sign_publickey_from_secretkey(
            Keys::secretKeyFromSeed(Keys::privateKeySeed($par['privateKeyPem']))
        );
        self::assertSame($derivada, Keys::publicKeyBytes($par['publicKeyPem']));
    }

    // ─── Firma ────────────────────────────────────────────────────────────────

    public function testUnaFirmaValidaVerifica(): void
    {
        self::assertNull(self::verifier()->check(self::signer()->sign(self::event())));
    }

    /** §4: ambas van entre las extensiones y **antes de `data`**. */
    public function testSignkeyidYSignatureVanAntesDeData(): void
    {
        $firmado = self::signer()->sign(self::event());
        $claves = array_keys($firmado->toArray());

        self::assertSame('data', $claves[count($claves) - 1], '`data` sigue siendo el último');
        self::assertLessThan(
            array_search('signature', $claves, true),
            array_search('signkeyid', $claves, true),
            'signkeyid va antes que signature: el primero se firma, el segundo no'
        );
        self::assertLessThan(
            array_search('data', $claves, true),
            array_search('signature', $claves, true),
        );
    }

    public function testLaFirmaEsBase64UrlSinPadding(): void
    {
        $firma = self::signer()->sign(self::event())->signature ?? '';

        self::assertMatchesRegularExpression('/^[A-Za-z0-9_-]+$/', $firma);
        self::assertStringNotContainsString('=', $firma, 'sin padding — 07-signing.md §4');
        self::assertSame(86, strlen($firma), '64 bytes en base64 sin padding');
    }

    /**
     * Ed25519 no consume aleatoriedad por firma. Solo es cierto porque 01-envelope.md
     * §1.1, §2.2 y §6 fijan una única representación en bytes: sin ellas, firmar entre
     * lenguajes sería imposible.
     */
    public function testLaFirmaEsDeterminista(): void
    {
        $signer = self::signer();

        self::assertSame(
            $signer->sign(self::event())->signature,
            $signer->sign(self::event())->signature,
        );
    }

    /** Si el round-trip no conservara los bytes, la firma no sobreviviría al broker. */
    public function testSobreviveAlRoundTripDeSerializacion(): void
    {
        $firmado = self::signer()->sign(self::event());
        $vuelto = Envelope::parse(Envelope::serialize($firmado));

        self::assertSame($firmado->signature, $vuelto->signature);
        self::assertSame($firmado->signkeyid, $vuelto->signkeyid);
        self::verifier()->check($vuelto);
    }

    // ─── Detección de manipulación ────────────────────────────────────────────

    public function testAlterarDataInvalidaLaFirma(): void
    {
        $firmado = self::signer()->sign(self::event());
        $manipulado = new FluxEvent(
            id: $firmado->id,
            source: $firmado->source,
            type: $firmado->type,
            time: $firmado->time,
            dataschema: $firmado->dataschema,
            correlationid: $firmado->correlationid,
            tenantid: $firmado->tenantid,
            producerversion: $firmado->producerversion,
            dataclassification: $firmado->dataclassification,
            data: ['pedidoId' => 'ped-999'],
            signkeyid: $firmado->signkeyid,
            signature: $firmado->signature,
        );

        self::assertSame(
            'INVALID_SIGNATURE',
            self::codeOf(static fn () => self::verifier()->check($manipulado)),
        );
    }

    /**
     * El caso que la ACL del broker **no** cubre: un evento sacado del stream, editado y
     * reinyectado. Con la firma activa, el `tenantid` queda ligado criptográficamente a la
     * clave del productor — 09-multitenancy.md §4.
     */
    public function testAlterarElTenantidInvalidaLaFirma(): void
    {
        $firmado = self::signer()->sign(self::event());
        $raw = json_decode(Envelope::serialize($firmado), true, 512, JSON_THROW_ON_ERROR);
        $raw['tenantid'] = 'globex';
        $manipulado = Envelope::parse(ProtocolFixture::encode($raw));

        self::assertSame(
            'INVALID_SIGNATURE',
            self::codeOf(static fn () => self::verifier()->check($manipulado)),
        );
    }

    /** `signkeyid` va DENTRO de lo firmado justo para esto — §5. */
    public function testCambiarSignkeyidNoPermiteEludirLaVerificacion(): void
    {
        $firmado = self::signer()->sign(self::event());
        $otro = $firmado->withSignature('otro-1', (string) $firmado->signature);

        self::assertSame(
            'UNKNOWN_SIGNING_KEY',
            self::codeOf(static fn () => self::verifier()->check($otro)),
        );
    }

    /** Y si lo cambia por un id que SÍ está registrado, tampoco: los bytes llevaban el viejo. */
    public function testCambiarSignkeyidPorUnoConocidoDaFirmaInvalida(): void
    {
        $otra = Keys::generateKeyPair();
        $firmado = self::signer()->sign(self::event());
        $suplantado = $firmado->withSignature('pedidos-api-2', (string) $firmado->signature);

        $verifier = Verifier::fromOptions(new SigningOptions(
            publicKeys: [
                self::KEY_ID => self::PUBLIC_PEM,
                'pedidos-api-2' => $otra['publicKeyPem'],
            ],
            verify: VerificationMode::Require,
        ));

        self::assertSame(
            'INVALID_SIGNATURE',
            self::codeOf(static fn () => $verifier?->check($suplantado)),
        );
    }

    public function testUnaFirmaDeOtraClaveNoVerifica(): void
    {
        $otra = Keys::generateKeyPair();
        $impostor = Signer::fromOptions(
            new SigningOptions(privateKey: $otra['privateKeyPem'], keyId: self::KEY_ID)
        );

        self::assertSame(
            'INVALID_SIGNATURE',
            self::codeOf(static fn () => self::verifier()->check($impostor?->sign(self::event()))),
        );
    }

    /**
     * Una firma que no llega a 64 bytes haría lanzar `SodiumException` a libsodium; el SDK
     * la convierte en INVALID_SIGNATURE porque un evento manipulado no es un fallo del
     * programa.
     */
    public function testUnaFirmaCortaOIlegibleEsInvalidSignature(): void
    {
        foreach (['no-es-base64-válido-¡¡', Base64Url::encode('corta')] as $basura) {
            $roto = self::event()->withSignature(self::KEY_ID, $basura);
            self::assertSame(
                'INVALID_SIGNATURE',
                self::codeOf(static fn () => self::verifier()->check($roto)),
                $basura
            );
        }
    }

    public function testUnaSignatureSinSignkeyidNoPasaPorValida(): void
    {
        // Se construye por JSON porque es exactamente como llegaría del cable: nadie
        // produce esto con la API del SDK, pero un productor roto o un atacante sí.
        $raw = json_decode(
            Envelope::serialize(self::signer()->sign(self::event())),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        unset($raw['signkeyid']);
        $roto = Envelope::parse(ProtocolFixture::encode($raw));

        self::assertNull($roto->signkeyid);
        self::assertSame(
            'UNKNOWN_SIGNING_KEY',
            self::codeOf(static fn () => self::verifier()->check($roto)),
        );
    }

    // ─── DLQ y replay ─────────────────────────────────────────────────────────

    /**
     * Si la verificación no ignorase las `dlq*`, **todo evento en la DLQ parecería
     * manipulado** — 07-signing.md §5.
     */
    public function testLaFirmaSigueVerificandoTrasPasarPorLaDlq(): void
    {
        $enDlq = Envelope::toDlqEvent(
            self::signer()->sign(self::event()),
            ErrorClass::Permanent,
            1,
            'facturacion-api__pedidos_pedido_v1_creado',
            'PEDIDO_YA_CANCELADO: el pedido ped-123 estaba cancelado',
        );

        self::assertNotNull($enDlq->dlqtime);
        self::assertNull(self::verifier()->check($enDlq));
    }

    /**
     * En el evento de DLQ, la firma va **antes de las `dlq*`** — que es el orden que emiten
     * Node, Python, Go, Java y .NET.
     *
     * No cambia si la firma verifica (la verificación quita las `dlq*` en cualquier caso),
     * pero sí los BYTES del mensaje que acaba en la DLQ, y de esos dependen el replay
     * verbatim, la deduplicación por hash y los fixtures compartidos — 01-envelope.md §6.
     * Es la misma clase de divergencia que el `{...event, dlq*}` de Node que dio origen a
     * esa sección.
     */
    public function testEnLaDlqLaFirmaVaAntesDeLasExtensionesDlq(): void
    {
        $enDlq = Envelope::toDlqEvent(
            self::signer()->sign(self::event()),
            ErrorClass::Permanent,
            1,
            'c',
            'PEDIDO_YA_CANCELADO',
        );
        $claves = array_keys($enDlq->toArray());

        self::assertLessThan(
            array_search('dlqreason', $claves, true),
            array_search('signature', $claves, true),
        );
        self::assertSame('data', $claves[count($claves) - 1]);

        // Y los bytes exactos, que es lo que de verdad importa: el SDK de Rust lleva este
        // mismo literal.
        $conTiempoFijo = $enDlq->withDlq(
            'permanent',
            1,
            'facturacion-api__pedidos_pedido_v1_creado',
            'PEDIDO_YA_CANCELADO',
            '2025-08-20T10:26:00.000Z',
        );
        self::assertSame(self::DLQ_VECTOR, Envelope::serialize($conTiempoFijo));
    }

    /** El replay redistribuye un hecho ya emitido, no crea uno nuevo — §5.1. */
    public function testUnEventoReproducidoConservaSuFirmaValida(): void
    {
        $enDlq = Envelope::toDlqEvent(
            self::signer()->sign(self::event()),
            ErrorClass::Retryable,
            6,
            'c',
            'HTTP_503: proveedor caído',
        );
        $reproducido = Envelope::stripDlqExtensions(
            Envelope::parse(Envelope::serialize($enDlq))
        );

        self::assertSame(self::VECTOR_SIGNATURE, $reproducido->signature);
        self::verifier()->check($reproducido);
    }

    // ─── Modos ────────────────────────────────────────────────────────────────

    public function testRequireRechazaUnEventoSinFirma(): void
    {
        self::assertSame(
            'MISSING_SIGNATURE',
            self::codeOf(static fn () => self::verifier()->check(self::event())),
        );
    }

    /**
     * `warn` es lo que hace posible migrar: pasar directo a `require` convertiría en POISON
     * todo evento de un servicio aún no migrado — §7.
     */
    public function testWarnRegistraPeroAcepta(): void
    {
        $logger = new class () extends AbstractLogger {
            /** @var list<string> */
            public array $avisos = [];

            public function log($level, \Stringable|string $message, array $context = []): void
            {
                $this->avisos[] = (string) $message;
            }
        };

        $verifier = self::verifier(VerificationMode::Warn, $logger);

        // Y devuelven el código, que es lo que el runtime necesita para contarlos.
        self::assertSame('MISSING_SIGNATURE', $verifier->check(self::event()));
        $ajeno = self::event()->withSignature('desconocida-9', self::VECTOR_SIGNATURE);
        self::assertSame('UNKNOWN_SIGNING_KEY', $verifier->check($ajeno));

        self::assertCount(2, $logger->avisos);
        self::assertStringContainsString('MISSING_SIGNATURE', $logger->avisos[0]);
        self::assertStringContainsString('UNKNOWN_SIGNING_KEY', $logger->avisos[1]);
    }

    /**
     * §7.1: **`warn` DEBE ser observable.** El código sale por el valor de retorno, no solo
     * por el log, para que el runtime pueda emitir
     * `flux_events_consumed_total{outcome="invalid_signature"}`.
     *
     * §7.1 además prohíbe explícitamente imponer una fachada de logging: la parte normativa
     * es la métrica. Por eso esto funciona **sin logger ninguno**.
     */
    public function testWarnDevuelveElCodigoAunqueNoHayaLogger(): void
    {
        $verifier = self::verifier(VerificationMode::Warn);

        self::assertSame('MISSING_SIGNATURE', $verifier->check(self::event()));

        $roto = self::event()->withSignature(self::KEY_ID, self::VECTOR_SIGNATURE);
        $manipulado = $roto->withSignature(self::KEY_ID, Base64Url::encode(random_bytes(64)));
        self::assertSame('INVALID_SIGNATURE', $verifier->check($manipulado));

        // Y un evento correctamente firmado no genera aviso ninguno.
        self::assertNull($verifier->check(self::signer()->sign(self::event())));
    }

    /**
     * `off` no construye verificador: no se paga lo que no se usa, y un evento firmado se
     * consume igual — sin eso, la adopción gradual de §7 sería imposible.
     */
    public function testOffNoConstruyeVerificador(): void
    {
        self::assertNull(Verifier::fromOptions(new SigningOptions()));
        self::assertNull(Verifier::fromOptions(null));
    }

    public function testSinClavePrivadaNoHayFirmante(): void
    {
        self::assertNull(Signer::fromOptions(new SigningOptions()));
        self::assertNull(Signer::fromOptions(null));
    }

    /** Un evento firmado se parsea aunque el SDK no verifique nada. */
    public function testUnEventoFirmadoSeParseaAunqueNoSeVerifique(): void
    {
        $raw = ProtocolFixture::rawEvent([
            'signkeyid' => 'pedidos-api-1',
            'signature' => self::VECTOR_SIGNATURE,
        ]);
        // El orden importa: `data` tiene que seguir siendo el último tras el override.
        $data = $raw['data'];
        unset($raw['data']);
        $raw['data'] = $data;

        $ev = Envelope::parse(ProtocolFixture::encode($raw));

        self::assertSame('pedidos-api-1', $ev->signkeyid);
        self::assertSame(self::VECTOR_SIGNATURE, $ev->signature);
    }

    // ─── Gestión de claves ────────────────────────────────────────────────────

    public function testFirmarSinKeyIdFallaConUnMensajeAccionable(): void
    {
        $this->expectException(SigningKeyException::class);
        $this->expectExceptionMessageMatches('/keyId/u');

        Signer::fromOptions(new SigningOptions(privateKey: self::PRIVATE_PEM));
    }

    public function testVerificarSinClavesPublicasFallaExplicandoLaRetencion(): void
    {
        $this->expectException(SigningKeyException::class);
        $this->expectExceptionMessageMatches('/RETIRADAS/u');

        Verifier::fromOptions(new SigningOptions(verify: VerificationMode::Require));
    }

    /**
     * Una clave de otro algoritmo no es "una clave que hay que convertir": el protocolo no
     * negocia algoritmo a propósito — §3.
     */
    public function testRechazaUnaClaveQueNoSeaEd25519(): void
    {
        // SPKI de una P-256: ni el tamaño ni el prefijo del OID encajan.
        $p256 = "-----BEGIN PUBLIC KEY-----\n"
            . "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEfoo5Rr3z2/g6rCzPoRnKSIVBcjWM\n"
            . "YjOl6Y/gPWPYt+Fd8ZLKJ0uv5rBXwCkPU9WQiVQdRlVLPO9EGxYQpxCFMA==\n"
            . "-----END PUBLIC KEY-----\n";

        $this->expectException(SigningKeyException::class);
        $this->expectExceptionMessageMatches('/p256-1/u');

        Verifier::fromOptions(new SigningOptions(
            publicKeys: ['p256-1' => $p256],
            verify: VerificationMode::Require,
        ));
    }

    public function testUnaEtiquetaPemEquivocadaSeRechaza(): void
    {
        $this->expectException(SigningKeyException::class);
        $this->expectExceptionMessageMatches('/PRIVATE KEY/u');

        Keys::privateKeySeed(self::PUBLIC_PEM);
    }

    /**
     * **La regla que más se equivoca.** Retirar una clave impide EMITIR con ella, no
     * VERIFICAR lo ya emitido: tratar una retirada como inválida convierte una rotación
     * rutinaria en la invalidación retroactiva de todo el historial — §6.
     */
    public function testUnaClaveRetiradaSigueVerificandoSiSeConservaLaPublica(): void
    {
        $vieja = Keys::generateKeyPair();
        $nueva = Keys::generateKeyPair();

        $firmadoConLaVieja = Signer::fromOptions(new SigningOptions(
            privateKey: $vieja['privateKeyPem'],
            keyId: 'pedidos-api-1',
        ))?->sign(self::event());

        $verifier = Verifier::fromOptions(new SigningOptions(
            publicKeys: [
                'pedidos-api-1' => $vieja['publicKeyPem'],  // RETIRADA, conservada
                'pedidos-api-2' => $nueva['publicKeyPem'],  // activa
            ],
            verify: VerificationMode::Require,
        ));

        self::assertNull($verifier?->check($firmadoConLaVieja));
    }

    // ─── base64url ────────────────────────────────────────────────────────────

    public function testBase64UrlIdaYVuelta(): void
    {
        for ($n = 0; $n < 70; $n++) {
            $bytes = $n === 0 ? '' : random_bytes($n);
            $enc = Base64Url::encode($bytes);

            self::assertStringNotContainsString('=', $enc);
            self::assertSame($bytes, Base64Url::decode($enc));
        }
    }

    public function testBase64UrlNoAceptaBasura(): void
    {
        self::assertNull(Base64Url::decode('¡¡ esto no es base64 !!'));
    }
}
