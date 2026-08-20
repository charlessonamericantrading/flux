<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\Classification;
use Flux\Classifier;
use Flux\ClassifierOptions;
use Flux\ErrorClass;
use Flux\PermanentError;
use Flux\PoisonError;
use Flux\Protocol;
use Flux\RetryableError;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ClassifierTest extends TestCase
{
    private Classifier $classifier;

    protected function setUp(): void
    {
        $this->classifier = new Classifier();
    }

    // ─── Errores tipados ──────────────────────────────────────────────────────

    public function testLosErroresTipadosGanan(): void
    {
        $c = $this->classifier->classify(new RetryableError('proveedor 503', retryAfterMs: 5000));
        self::assertSame(ErrorClass::Retryable, $c->errorClass);
        self::assertSame(5000.0, $c->retryAfterMs);

        $c = $this->classifier->classify(new PermanentError('pedido ya cancelado', 'PEDIDO_YA_CANCELADO'));
        self::assertSame(ErrorClass::Permanent, $c->errorClass);
        self::assertSame('PEDIDO_YA_CANCELADO', $c->code);

        self::assertSame(
            ErrorClass::Poison,
            $this->classifier->classify(new PoisonError('json roto'))->errorClass,
        );
    }

    public function testSinCodigoSeUsaElNombreDeLaClase(): void
    {
        self::assertSame('RetryableError', $this->classifier->classify(new RetryableError('x'))->code);
    }

    public function testUnErrorTipadoNoLlevaTopePropio(): void
    {
        // Declarar un error como transitorio es afirmar que el mundo puede arreglarlo solo:
        // conserva las 6 entregas del consumidor.
        self::assertNull($this->classifier->classify(new RetryableError('x'))->maxAttempts);
    }

    // ─── HTTP ─────────────────────────────────────────────────────────────────

    #[DataProvider('statusRetryable')]
    public function testHttpRetryable(int $status): void
    {
        $c = $this->classifier->classify(new FakeHttpException($status));

        self::assertSame(ErrorClass::Retryable, $c->errorClass);
        self::assertSame("HTTP_{$status}", $c->code);
        // Los reconocidos conservan su presupuesto completo.
        self::assertNull($c->maxAttempts);
    }

    #[DataProvider('statusPermanent')]
    public function testHttpPermanent(int $status): void
    {
        // Reintentar un 404 es gastar 51 minutos para obtener exactamente la misma respuesta.
        self::assertSame(
            ErrorClass::Permanent,
            $this->classifier->classify(new FakeHttpException($status))->errorClass,
        );
    }

    public function testLeeRetryAfterDeLaRespuesta(): void
    {
        $c = $this->classifier->classify(new FakeHttpException(429, ['Retry-After' => '12']));

        self::assertSame(ErrorClass::Retryable, $c->errorClass);
        self::assertSame(12_000.0, $c->retryAfterMs);
    }

    public function testRetryAfterConFechaHttpSeIgnora(): void
    {
        // Interpretarla mal daría un retraso absurdo; el backoff canónico es una respuesta
        // segura.
        $c = $this->classifier->classify(
            new FakeHttpException(503, ['Retry-After' => 'Wed, 21 Oct 2026 07:28:00 GMT']),
        );

        self::assertNull($c->retryAfterMs);
    }

    public function testNoConfundeGetCodeConUnStatusHttp(): void
    {
        // `getCode()` es un cero o un código de driver en el 90 % de las excepciones de PHP.
        // Tomarlo por un status HTTP clasificaría mal en silencio.
        $e = new \RuntimeException('algo', 404);

        self::assertSame('UNKNOWN', $this->classifier->classify($e)->code);
    }

    // ─── Semántica por tipo ───────────────────────────────────────────────────

    public function testUnMarcadorDeRedEsRetryable(): void
    {
        // `FakeNetworkException` simula un `Psr\Http\Client\NetworkExceptionInterface`: el
        // marcador estándar de "no se pudo hablar con el servidor". Es la señal más
        // semántica que existe en PHP, y no depende de ningún código numérico.
        $classifier = new Classifier(new ClassifierOptions(
            transientExceptions: [FakeNetworkException::class],
        ));

        self::assertSame(
            ErrorClass::Retryable,
            $classifier->classify(new FakeNetworkException('conexión rechazada'))->errorClass,
        );
    }

    public function testUnMarcadorDePeticionInvalidaEsPermanent(): void
    {
        $classifier = new Classifier(new ClassifierOptions(
            permanentExceptions: [FakeNetworkException::class],
        ));

        self::assertSame(
            ErrorClass::Permanent,
            $classifier->classify(new FakeNetworkException('petición mal formada'))->errorClass,
        );
    }

    public function testLosMarcadoresPorDefectoSonLosDePsr18(): void
    {
        self::assertContains(
            'Psr\\Http\\Client\\NetworkExceptionInterface',
            Classifier::DEFAULT_TRANSIENT_EXCEPTIONS,
        );
        self::assertContains(
            'Psr\\Http\\Client\\RequestExceptionInterface',
            Classifier::DEFAULT_PERMANENT_EXCEPTIONS,
        );
    }

    public function testUnaClaseInexistenteNoRompeLaComprobacion(): void
    {
        // Los marcadores por defecto nombran librerías que pueden no estar instaladas.
        // `instanceof` contra una clase inexistente es `false`, no un error.
        self::assertSame('UNKNOWN', $this->classifier->classify(new \RuntimeException('x'))->code);
    }

    // ─── Sockets ──────────────────────────────────────────────────────────────

    public function testUnErrnoDeSocketSeTraduceANombrePosix(): void
    {
        if (!defined('SOCKET_ECONNRESET')) {
            self::markTestSkipped('ext-sockets no está cargada');
        }

        // ⚠️ Se compara por NOMBRE de constante y no por valor: en Windows
        // `SOCKET_ECONNRESET` vale 10054 (WSAECONNRESET) y en Linux 104. Comparar valores
        // haría que el mismo corte de red fuese RETRYABLE en un sistema operativo y
        // desconocido en el otro — el bug real que documenta 04-errors.md §1.1.
        self::assertSame('ECONNRESET', Classifier::socketErrorName(SOCKET_ECONNRESET));

        $c = $this->classifier->classify(new FakeSocketException(SOCKET_ECONNRESET));
        self::assertSame(ErrorClass::Retryable, $c->errorClass);
        self::assertSame('ECONNRESET', $c->code);
        self::assertNull($c->maxAttempts);
    }

    public function testLosNombresDeSocketSonLosDeProtocolJson(): void
    {
        // Los mismos códigos que emiten Node y Python, para que agrupar por causa funcione
        // en un ecosistema polyglot. `EAI_AGAIN` no tiene constante en PHP y se omite a
        // propósito: la spec lo lista como EJEMPLO, no como norma.
        $ejemplos = ProtocolFixture::protocol()['errors']['classes']['retryable']['syscallCodesExample'];

        foreach (Classifier::TRANSIENT_SOCKET_CODES as $code) {
            self::assertContains($code, $ejemplos);
        }
    }

    // ─── Base de datos ────────────────────────────────────────────────────────

    #[DataProvider('sqlStatesTransitorios')]
    public function testContencionYConexionDeBaseDeDatosSonRetryable(string $sqlstate): void
    {
        // El mecanismo portable de PHP para "contención en base de datos" y "dependencia
        // arrancando" (04-errors.md §1.1) es la CLASE de SQLSTATE, no el código del driver:
        // `1213` es de MySQL, `40P01` de Postgres, y ambos son de la clase `40`.
        $c = $this->classifier->classify(FakePdoException::withState($sqlstate));

        self::assertSame(ErrorClass::Retryable, $c->errorClass);
        self::assertSame("SQLSTATE_{$sqlstate}", $c->code);
    }

    public function testUnaViolacionDeIntegridadEsPermanent(): void
    {
        // `23505` (unique_violation) no mejora esperando: el registro seguirá ahí.
        self::assertSame(
            ErrorClass::Permanent,
            $this->classifier->classify(FakePdoException::withState('23505'))->errorClass,
        );
    }

    public function testUnSqlstateGenericoCaeEnLaPoliticaDeLoDesconocido(): void
    {
        // `HY000` no dice nada útil; fingir que es permanente sería peor que admitir que no
        // se sabe.
        $c = $this->classifier->classify(FakePdoException::withState('HY000'));

        self::assertSame(ErrorClass::Retryable, $c->errorClass);
        self::assertSame(2, $c->maxAttempts);
    }

    // ─── Timeouts ─────────────────────────────────────────────────────────────

    public function testTimeoutPorDefectoRetryable(): void
    {
        $c = $this->classifier->classify(new FakeTimeoutException('se acabó el tiempo'));

        self::assertSame(ErrorClass::Retryable, $c->errorClass);
        self::assertSame('TIMEOUT', $c->code);
    }

    public function testTimeoutPuedeHacersePermanent(): void
    {
        $estricto = new Classifier(new ClassifierOptions(timeoutPolicy: 'permanent'));

        self::assertSame(
            ErrorClass::Permanent,
            $estricto->classify(new FakeTimeoutException('x'))->errorClass,
        );
    }

    public function testElTimeoutSeDetectaPorNombreDeClaseNoPorMensaje(): void
    {
        // Un nombre de clase es API pública y estable; un mensaje de error no lo es ni
        // pretende serlo. Esta es la única concesión a la comparación textual, y va sobre
        // el nombre de la clase — nunca sobre `getMessage()`, que es lo que 04-errors.md
        // §1.1 prohíbe.
        $conMensajeEnganoso = new \RuntimeException('connection timeout after 30s');

        self::assertSame('UNKNOWN', $this->classifier->classify($conMensajeEnganoso)->code);
    }

    // ─── El default: RETRYABLE acotado ────────────────────────────────────────

    public function testDefaultDeLoDesconocidoEsRetryableAcotado(): void
    {
        // Domina a las dos alternativas: el transitorio se recupera en el 2º intento y el
        // sistemático llega a la DLQ en ~30 s sin atascar la cola — 04-errors.md §2.1.
        self::assertSame('retryable-bounded', ProtocolFixture::protocol()['errors']['default']);

        $c = $this->classifier->classify(new \RuntimeException('algo que nadie previó'));

        self::assertSame(ErrorClass::Retryable, $c->errorClass);
        self::assertSame('UNKNOWN', $c->code);
        self::assertSame(2, $c->maxAttempts);
        self::assertSame(ProtocolFixture::protocol()['errors']['unknownRetryBudget'], $c->maxAttempts);
    }

    public function testElPresupuestoAcotadoNoRecortaLosRetryableReconocidos(): void
    {
        // Un HTTP 503 conserva sus 6 intentos: el presupuesto es por ERROR, no por
        // consumidor. Bajarlo en `max_deliver` los habría recortado a todos.
        self::assertNull($this->classifier->classify(new FakeHttpException(503))->maxAttempts);
        self::assertSame(
            ProtocolFixture::protocol()['errors']['budgets']['recognizedRetryable'],
            Protocol::MAX_DELIVER,
        );
    }

    public function testElPresupuestoDeLoDesconocidoEsConfigurable(): void
    {
        $generoso = new Classifier(new ClassifierOptions(unknownRetryBudget: 3));

        self::assertSame(3, $generoso->classify(new \RuntimeException('x'))->maxAttempts);
    }

    public function testPoliticaPermanentNoGastaNingunReintento(): void
    {
        $estricto = new Classifier(new ClassifierOptions(unknownErrorPolicy: 'permanent'));
        $c = $estricto->classify(new \RuntimeException('x'));

        self::assertSame(ErrorClass::Permanent, $c->errorClass);
        self::assertNull($c->maxAttempts);
    }

    public function testPoliticaRetryableDaElBackoffCompleto(): void
    {
        $agresivo = new Classifier(new ClassifierOptions(unknownErrorPolicy: 'retryable'));
        $c = $agresivo->classify(new \RuntimeException('x'));

        self::assertSame(ErrorClass::Retryable, $c->errorClass);
        self::assertNull($c->maxAttempts); // sin tope propio = manda el del consumidor
    }

    // ─── Reglas de la aplicación ──────────────────────────────────────────────

    public function testLasReglasDeLaAplicacionVanPrimero(): void
    {
        $clasificador = new Classifier(new ClassifierOptions(rules: [
            static fn (\Throwable $e): ?Classification => $e instanceof FakeHttpException && $e->tag === 'deadlock'
                ? new Classification(ErrorClass::Retryable, 'DB_DEADLOCK')
                : null,
        ]));

        // La regla gana sobre el status 400, que si no sería PERMANENT.
        $e = new FakeHttpException(400);
        $e->tag = 'deadlock';

        self::assertSame('DB_DEADLOCK', $clasificador->classify($e)->code);
    }

    public function testUnaReglaQueCedeNoAlteraElResultado(): void
    {
        $clasificador = new Classifier(new ClassifierOptions(rules: [
            static fn (\Throwable $e): ?Classification => null,
        ]));

        self::assertSame('HTTP_503', $clasificador->classify(new FakeHttpException(503))->code);
    }

    // ─── Proveedores ──────────────────────────────────────────────────────────

    /** @return iterable<array{int}> */
    public static function statusRetryable(): iterable
    {
        foreach (ProtocolFixture::protocol()['errors']['classes']['retryable']['httpStatus'] as $status) {
            yield (string) $status => [$status];
        }
    }

    /** @return iterable<array{int}> */
    public static function statusPermanent(): iterable
    {
        foreach (ProtocolFixture::protocol()['errors']['classes']['permanent']['httpStatus'] as $status) {
            yield (string) $status => [$status];
        }
    }

    /** @return iterable<string,array{string}> */
    public static function sqlStatesTransitorios(): iterable
    {
        yield 'connection failure' => ['08006'];
        yield 'connection does not exist' => ['08003'];
        yield 'serialization failure' => ['40001'];
        yield 'deadlock detected (postgres)' => ['40P01'];
        yield 'too many connections' => ['53300'];
        yield 'database starting up' => ['57P03'];
    }
}
