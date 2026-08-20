<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\Envelope;
use Flux\EnvelopeException;
use Flux\ErrorClass;
use Flux\PoisonError;
use Flux\Protocol;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class EnvelopeTest extends TestCase
{
    // ─── Construcción ─────────────────────────────────────────────────────────

    public function testRellenaLoQueElDesarrolladorNoEscribe(): void
    {
        $e = ProtocolFixture::event();

        self::assertSame('1.0', $e->specversion);
        self::assertSame('application/json', $e->datacontenttype);
        self::assertSame('com.flux.pedidos.pedido.creado.v1', $e->type);
    }

    public function testTimeTieneExactamenteTresDecimalesYSufijoZ(): void
    {
        // Ni `DateTime::ATOM` ni `format('c')` valen: no dan milisegundos y ponen `+00:00`.
        // Sin esto, dos servicios del mismo ecosistema emiten `time` distintos para el
        // mismo instante y el replay verbatim deja de ser byte a byte idéntico.
        $e = ProtocolFixture::event(['time' => null]);

        self::assertMatchesRegularExpression('/' . Protocol::TIME_PATTERN . '/', $e->time);
    }

    public function testTimeConservaLosCerosFinales(): void
    {
        // El caso que Go rompe por defecto (`RFC3339Nano` recorta ceros → `.4Z`).
        $when = new \DateTimeImmutable('2026-08-20T10:25:39.400000+00:00');

        self::assertSame('2026-08-20T10:25:39.400Z', Envelope::formatTime($when));
    }

    public function testTimeSinMilisegundosSaleConTresCeros(): void
    {
        $when = new \DateTimeImmutable('2026-08-20T10:25:39+00:00');

        self::assertSame('2026-08-20T10:25:39.000Z', Envelope::formatTime($when));
    }

    public function testTimeExplicitoSeConvierteAUtc(): void
    {
        // Madrid en agosto es UTC+2. Un `time` con offset local sería válido en RFC 3339 y
        // rompería la comparación textual entre servicios.
        $when = new \DateTimeImmutable('2026-08-20T12:25:39.412000', new \DateTimeZone('Europe/Madrid'));

        self::assertSame('2026-08-20T10:25:39.412Z', Envelope::formatTime($when));
    }

    #[DataProvider('timesInvalidos')]
    public function testUnTimeMalFormateadoSeRechazaEnElProductor(string $time): void
    {
        // Diferencia deliberada con Node y Python, que dejan pasar cualquier cadena: es más
        // barato fallar aquí que descubrir en el replay que un servicio lleva meses
        // emitiendo `…39.41Z`.
        $this->expectException(EnvelopeException::class);

        ProtocolFixture::event(['time' => $time]);
    }

    public function testAggregateIdVaAlSubjectDeCloudEvents(): void
    {
        // ⚠️ El `subject` del envelope es el ID DEL AGREGADO, no el subject de NATS.
        $e = ProtocolFixture::event(['aggregateId' => 'ped-123']);

        self::assertSame('ped-123', $e->subject);
        self::assertSame('ped-123', $e->partitionkey); // por convención, 01-envelope.md §3.2
    }

    public function testPartitionKeyExplicitaGana(): void
    {
        $e = ProtocolFixture::event(['aggregateId' => 'ped-123', 'partitionkey' => 'cli-987']);

        self::assertSame('cli-987', $e->partitionkey);
    }

    public function testDataDebeSerObjetoEnLaRaiz(): void
    {
        $this->expectException(EnvelopeException::class);
        $this->expectExceptionMessageMatches('/objeto JSON en la raíz/u');

        ProtocolFixture::event(['data' => [['sku' => 'ABC-1']]]);
    }

    public function testUnaClasificacionInventadaSeRechazaAlPublicar(): void
    {
        // Publicarla produciría un POISON en CADA consumidor del subject y el productor no
        // se enteraría nunca.
        $this->expectException(EnvelopeException::class);

        ProtocolFixture::event(['dataclassification' => 'secreto']);
    }

    // ─── Serialización ────────────────────────────────────────────────────────

    public function testOmiteLosAtributosAusentes(): void
    {
        // Ausente ≠ vacío: `null` para significar "no aplica" está prohibido, y un
        // `causationid: null` en el cable obligaría a cada consumidor a distinguir los dos
        // casos — 01-envelope.md §3.3.
        $raw = json_decode(Envelope::serialize(ProtocolFixture::event()), true);

        self::assertArrayNotHasKey('causationid', $raw);
        self::assertArrayNotHasKey('subject', $raw);
        self::assertArrayNotHasKey('dlqreason', $raw);
        self::assertNotContains(null, $raw, 'ningún atributo debe salir como null');
    }

    public function testSoloAtributosPermitidos(): void
    {
        $raw = json_decode(Envelope::serialize(ProtocolFixture::event(['aggregateId' => 'ped-1'])), true);

        self::assertSame([], array_diff(array_keys($raw), Envelope::ALLOWED_ROOT_ATTRIBUTES));
    }

    public function testLimiteDe1MibMencionaClaimCheck(): void
    {
        $this->expectException(EnvelopeException::class);
        $this->expectExceptionMessageMatches('/claim-check/');

        Envelope::serialize(ProtocolFixture::event([
            'data' => ['blob' => str_repeat('x', Protocol::MAX_MESSAGE_BYTES + 10)],
        ]));
    }

    public function testJustoPorDebajoDelLimitePasa(): void
    {
        $json = Envelope::serialize(ProtocolFixture::event([
            'data' => ['blob' => str_repeat('x', Protocol::MAX_MESSAGE_BYTES - 1000)],
        ]));

        self::assertLessThan(Protocol::MAX_MESSAGE_BYTES, strlen($json));
    }

    public function testDataVacioSeSerializaComoObjetoYNoComoArray(): void
    {
        // En PHP `[]` es a la vez el objeto vacío y la lista vacía. `data` DEBE ser un
        // objeto (01-envelope.md §4), así que en la raíz la ambigüedad se resuelve por
        // contrato; si no, `"data":[]` haría POISON el mensaje en los otros cinco SDKs.
        $json = Envelope::serialize(ProtocolFixture::event(['data' => []]));

        self::assertStringEndsWith('"data":{}}', $json);
    }

    // ─── Parseo ───────────────────────────────────────────────────────────────

    public function testRoundTrip(): void
    {
        $original = ProtocolFixture::event(['aggregateId' => 'ped-123']);

        self::assertEquals($original, Envelope::parse(Envelope::serialize($original)));
    }

    public function testJsonMalformado(): void
    {
        $error = $this->poison('{no soy json');

        self::assertSame('MALFORMED_JSON', $error->errorCode);
        self::assertSame(ErrorClass::Poison, $error->fluxClass());
    }

    #[DataProvider('cuerposQueNoSonObjeto')]
    public function testNoEsUnObjeto(string $payload): void
    {
        self::assertSame('NOT_AN_OBJECT', $this->poison($payload)->errorCode);
    }

    public function testSpecversionDesconocida(): void
    {
        $raw = ProtocolFixture::rawEvent(['specversion' => '0.3']);

        self::assertSame('UNSUPPORTED_SPECVERSION', $this->poison(ProtocolFixture::encode($raw))->errorCode);
    }

    #[DataProvider('atributosObligatorios')]
    public function testFaltaUnAtributoObligatorio(string $attribute): void
    {
        $raw = ProtocolFixture::rawEvent();
        unset($raw[$attribute]);

        $error = $this->poison(ProtocolFixture::encode($raw));

        self::assertSame('MISSING_REQUIRED_ATTRIBUTE', $error->errorCode);
        self::assertStringContainsString($attribute, $error->getMessage());
    }

    #[DataProvider('extensionesObligatorias')]
    public function testFaltaUnaExtensionObligatoriaEsPoison(string $extension): void
    {
        // La spec las llamaba obligatorias y ningún parser las exigía. Asumir un default es
        // peligroso en las cuatro: un `dataclassification` ausente tomado como `internal`
        // haría circular PII con 30 días de retención en vez de 7, y un `tenantid` ausente
        // tomado como `system` cruzaría fronteras de tenant — 01-envelope.md §3.1.
        $raw = ProtocolFixture::rawEvent();
        unset($raw[$extension]);

        $error = $this->poison(ProtocolFixture::encode($raw));

        self::assertSame('MISSING_REQUIRED_EXTENSION', $error->errorCode);
        self::assertStringContainsString($extension, $error->getMessage());
    }

    #[DataProvider('valoresVacios')]
    public function testUnaExtensionObligatoriaVaciaCuentaComoAusente(mixed $value): void
    {
        // Ausente ≠ vacío va en las dos direcciones: si `""` colase, el envelope tendría dos
        // formas de decir "no lo sé" y solo una sería POISON — 01-envelope.md §3.3.
        $raw = ProtocolFixture::rawEvent(['tenantid' => $value]);

        self::assertSame('MISSING_REQUIRED_EXTENSION', $this->poison(ProtocolFixture::encode($raw))->errorCode);
    }

    public function testDataclassificationVaciaSeEvaluaAntesQueElEnum(): void
    {
        // La tabla de 01-envelope.md §3.1 lo fija explícitamente: `""` es
        // MISSING_REQUIRED_EXTENSION, no INVALID_DATACLASSIFICATION. Si dos SDKs emiten
        // códigos distintos ante la misma entrada, agrupar por causa en las métricas deja
        // de funcionar en cuanto el ecosistema es polyglot — que es siempre.
        $raw = ProtocolFixture::rawEvent(['dataclassification' => '']);

        self::assertSame('MISSING_REQUIRED_EXTENSION', $this->poison(ProtocolFixture::encode($raw))->errorCode);
    }

    #[DataProvider('clasificacionesInvalidas')]
    public function testDataclassificationFueraDelEnumEsPoison(mixed $value): void
    {
        // La retención y las ACLs se derivan de este valor (06-security.md §5), así que un
        // valor desconocido no se puede degradar a nada seguro.
        $raw = ProtocolFixture::rawEvent(['dataclassification' => $value]);

        self::assertSame('INVALID_DATACLASSIFICATION', $this->poison(ProtocolFixture::encode($raw))->errorCode);
    }

    #[DataProvider('atributosDeTexto')]
    public function testUnTipoCoaccionableEsPoison(string $attribute): void
    {
        // `{"tenantid": 42}` es POISON, no el tenant "42". En PHP hay que comprobarlo con
        // `is_string()` explícito: el lenguaje coacciona con entusiasmo y `"42" == 42` es
        // `true`, así que el entero llegaría al handler y "funcionaría" hasta que dejase de
        // hacerlo — 01-envelope.md §2.4.
        $raw = ProtocolFixture::rawEvent([$attribute => 42]);

        $error = $this->poison(ProtocolFixture::encode($raw));

        self::assertSame('WRONG_ATTRIBUTE_TYPE', $error->errorCode);
        self::assertStringContainsString($attribute, $error->getMessage());
    }

    public function testLaListaDeAtributosDeTextoEsLaDeProtocolJson(): void
    {
        self::assertSame(
            ProtocolFixture::protocol()['cloudevents']['stringAttributes'],
            Envelope::STRING_ATTRIBUTES,
        );
    }

    public function testUnDlqattemptsDeTipoIncorrectoNoTumbaElConsumidor(): void
    {
        // Con `strict_types=1` el constructor lanzaría `TypeError` y, sin capturarlo, el
        // proceso del consumidor se caería en vez de mandar el mensaje a la DLQ.
        $raw = ProtocolFixture::rawEvent(['dlqattempts' => 'tres']);

        self::assertSame('WRONG_ATTRIBUTE_TYPE', $this->poison(ProtocolFixture::encode($raw))->errorCode);
    }

    #[DataProvider('datosQueNoSonObjeto')]
    public function testUnDataQueNoEsObjetoEsPoison(mixed $data): void
    {
        // Simétrico con `build()`, que ya lo rechaza al publicar. Comprobar solo `is_array`
        // no bastaba: en PHP una lista JSON TAMBIÉN es un array, así que un
        // `{"data":[1,2,3]}` de un productor ajeno llegaba al handler como lista en vez de
        // ser POISON — 01-envelope.md §4.
        $raw = ProtocolFixture::rawEvent(['data' => $data]);

        self::assertSame('WRONG_ATTRIBUTE_TYPE', $this->poison(ProtocolFixture::encode($raw))->errorCode);
    }

    public function testUnDataVacioSiEsValido(): void
    {
        // `{}` llega a PHP como `[]`, indistinguible de la lista vacía. Se acepta porque el
        // objeto vacío es legal y la lista vacía no aporta nada que perder.
        $raw = ProtocolFixture::rawEvent(['data' => new \stdClass()]);

        self::assertSame([], Envelope::parse(ProtocolFixture::encode($raw))->data);
    }

    public function testContentTypeNoSoportado(): void
    {
        $raw = ProtocolFixture::rawEvent(['datacontenttype' => 'application/xml']);

        self::assertSame('UNSUPPORTED_CONTENT_TYPE', $this->poison(ProtocolFixture::encode($raw))->errorCode);
    }

    public function testAtributoRaizDesconocidoEsPoison(): void
    {
        // Los atributos raíz están cerrados: permitir metadatos ad-hoc acaba en JSON dentro
        // de un string, porque las extensiones de CloudEvents no admiten objetos ni arrays,
        // y el envelope deja de ser interoperable — 01-envelope.md §3.4.
        $raw = ProtocolFixture::rawEvent(['miMetadato' => 'algo']);

        $error = $this->poison(ProtocolFixture::encode($raw));

        self::assertSame('UNKNOWN_ROOT_ATTRIBUTE', $error->errorCode);
        self::assertStringContainsString('`data`', $error->getMessage());
    }

    public function testLaComparacionDeNombresEsCaseSensitive(): void
    {
        // `{"ID": "..."}` no es el atributo `id`: es un atributo raíz desconocido. La regla
        // existe porque `encoding/json` de Go empareja campos sin distinguir mayúsculas y
        // poblaría `Event.ID` en silencio — 01-envelope.md §2.3.
        $raw = ProtocolFixture::rawEvent();
        $raw['ID'] = $raw['id'];

        self::assertSame('UNKNOWN_ROOT_ATTRIBUTE', $this->poison(ProtocolFixture::encode($raw))->errorCode);
    }

    public function testLasExtensionesDlqSiEstanPermitidas(): void
    {
        // Un mensaje leído de la DLQ tiene que poder parsearse para poder reproducirlo.
        $dead = Envelope::toDlqEvent(ProtocolFixture::event(), ErrorClass::Permanent, 1, 'c__s', 'boom');

        self::assertSame('permanent', Envelope::parse(Envelope::serialize($dead))->dlqreason);
    }

    // ─── DLQ ──────────────────────────────────────────────────────────────────

    public function testConservaElEventoOriginalIntacto(): void
    {
        $original = ProtocolFixture::event(['aggregateId' => 'ped-123']);
        $dead = Envelope::toDlqEvent(
            $original,
            ErrorClass::Permanent,
            1,
            'facturacion-api__pedidos_pedido_v1_creado',
            'PEDIDO_YA_CANCELADO: el pedido ped-123 estaba cancelado',
        );

        self::assertSame('permanent', $dead->dlqreason);
        self::assertSame(1, $dead->dlqattempts);
        self::assertSame('facturacion-api__pedidos_pedido_v1_creado', $dead->dlqconsumer);
        self::assertMatchesRegularExpression('/' . Protocol::TIME_PATTERN . '/', (string) $dead->dlqtime);

        // Sin wrapper: el replay es quitar las `dlq*` y republicar — 04-errors.md §3.
        self::assertEquals($original, Envelope::stripDlqExtensions($dead));
    }

    public function testElReplayConservaElId(): void
    {
        // Regenerarlo rompería la idempotencia de todos los consumidores aguas abajo y
        // convertiría una recuperación en un incidente nuevo — 04-errors.md §4.1.
        $original = ProtocolFixture::event();
        $dead = Envelope::toDlqEvent($original, ErrorClass::Poison, 1, 'c', 'x');

        self::assertSame($original->id, Envelope::stripDlqExtensions($dead)->id);
    }

    public function testElErrorSeRecortaA1Kib(): void
    {
        $dead = Envelope::toDlqEvent(
            ProtocolFixture::event(),
            ErrorClass::Permanent,
            1,
            'c',
            str_repeat('x', 5000),
        );

        self::assertSame(1024, strlen((string) $dead->dlqerror));
    }

    public function testElRecorteNoPartUnCaracterMultibyte(): void
    {
        // Un corte a mitad de secuencia UTF-8 haría que `json_encode` fallase con
        // "Malformed UTF-8 characters" y el evento de DLQ no llegaría a escribirse — justo
        // cuando más falta hace.
        $dead = Envelope::toDlqEvent(
            ProtocolFixture::event(),
            ErrorClass::Permanent,
            1,
            'c',
            str_repeat('é', 2000),
        );

        self::assertNotFalse(json_encode($dead->dlqerror, JSON_UNESCAPED_UNICODE));
        self::assertLessThanOrEqual(1024, strlen((string) $dead->dlqerror));
    }

    public function testDlqattemptsDistinguePermanentDeRetryableAgotado(): void
    {
        // `dlqattempts` es el número de entrega en que el evento murió, NO una propiedad de
        // su clase: un PERMANENT lanzado en la 3ª entrega registra 3 — 04-errors.md §3.
        $event = ProtocolFixture::event();

        self::assertSame(1, Envelope::toDlqEvent($event, ErrorClass::Permanent, 1, 'c', 'x')->dlqattempts);
        self::assertSame(6, Envelope::toDlqEvent($event, ErrorClass::Retryable, 6, 'c', 'x')->dlqattempts);
        self::assertSame(3, Envelope::toDlqEvent($event, ErrorClass::Permanent, 3, 'c', 'x')->dlqattempts);
    }

    // ─── Utilidades ───────────────────────────────────────────────────────────

    private function poison(string $payload): PoisonError
    {
        try {
            Envelope::parse($payload);
        } catch (PoisonError $e) {
            return $e;
        }

        self::fail('se esperaba un PoisonError y el mensaje se parseó sin error');
    }

    /** @return iterable<string,array{string}> */
    public static function timesInvalidos(): iterable
    {
        yield 'ceros recortados' => ['2026-08-20T10:25:39.41Z'];
        yield 'microsegundos y offset' => ['2026-08-20T10:25:39.410000+00:00'];
        yield 'sin decimales' => ['2026-08-20T10:25:39Z'];
        yield 'sin zona' => ['2026-08-20T10:25:39.410'];
    }

    /** @return iterable<string,array{string}> */
    public static function cuerposQueNoSonObjeto(): iterable
    {
        yield 'array' => ['[1,2,3]'];
        yield 'array vacio' => ['[]'];
        yield 'numero' => ['42'];
        yield 'cadena' => ['"texto"'];
        yield 'null' => ['null'];
    }

    /** @return iterable<array{string}> */
    public static function atributosObligatorios(): iterable
    {
        foreach (['id', 'source', 'type', 'time', 'dataschema', 'data'] as $attribute) {
            yield $attribute => [$attribute];
        }
    }

    /** @return iterable<array{string}> */
    public static function extensionesObligatorias(): iterable
    {
        foreach (Envelope::REQUIRED_EXTENSIONS as $extension) {
            yield $extension => [$extension];
        }
    }

    /** @return iterable<string,array{mixed}> */
    public static function valoresVacios(): iterable
    {
        yield 'null' => [null];
        yield 'cadena vacia' => [''];
    }

    /** @return iterable<string,array{mixed}> */
    public static function clasificacionesInvalidas(): iterable
    {
        yield 'fuera del enum' => ['secreto'];
        yield 'case-sensitive' => ['Confidential'];
        yield 'numero' => [42];
        yield 'objeto' => [['a' => 1]];
    }

    /** @return iterable<string,array{mixed}> */
    public static function datosQueNoSonObjeto(): iterable
    {
        yield 'lista' => [[1, 2, 3]];
        yield 'lista de objetos' => [[['sku' => 'ABC-1']]];
        yield 'cadena' => ['texto'];
        yield 'numero' => [42];
        yield 'booleano' => [true];
    }

    /** @return iterable<array{string}> */
    public static function atributosDeTexto(): iterable
    {
        foreach (Envelope::STRING_ATTRIBUTES as $attribute) {
            yield $attribute => [$attribute];
        }
    }
}
