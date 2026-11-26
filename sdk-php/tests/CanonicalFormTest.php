<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\Envelope;
use Flux\Json;
use Flux\PoisonError;
use Flux\Text;
use Flux\Time;
use PHPUnit\Framework\TestCase;

/**
 * Lo decidido en «Ajustes» (conformance/harness/README.md, 01-envelope.md §2.2 y §4.1, 04-errors.md §3) que es de PHP aunque no haya
 * broker: `dlqerror` en BYTES, `maxLength` en PUNTOS DE CÓDIGO, `time` canónico y `data` en su forma canónica.
 */
final class CanonicalFormTest extends TestCase
{
    public function testDlqerrorSeCortaEnBytesSinPartirUnCaracter(): void
    {
        $e = str_repeat('é', 1500); // 2 bytes cada una
        $cut = Text::truncateUtf8Bytes($e, 1024);

        self::assertSame(1024, strlen($cut));
        self::assertSame(512, Text::codePointLength($cut));

        // 1023 bytes de ASCII y un emoji de 4: no cabe entero, se descarta ENTERO.
        $emoji = str_repeat('a', 1023) . '😀';
        self::assertSame(str_repeat('a', 1023), Text::truncateUtf8Bytes($emoji, 1024));
        // «€» son 3 bytes: con 1022 de ASCII, justo uno más cabe.
        self::assertSame(str_repeat('a', 1021) . '€', Text::truncateUtf8Bytes(str_repeat('a', 1021) . '€€', 1024));
    }

    public function testUnTextoQueNoEsUtf8SeSaneaEnLugarDeRomperElEventoDeDlq(): void
    {
        $roto = "error \xFF\xFE de un driver";
        $dlq = Envelope::toDlqEvent(
            \Flux\Tests\ProtocolFixture::event(),
            \Flux\ErrorClass::Permanent,
            1,
            'svc__x',
            $roto,
        );

        // Con `json_encode` sobre bytes inválidos el evento no llegaría a escribirse justo cuando más falta hace.
        self::assertStringContainsString('error ', Envelope::serialize($dlq));
        self::assertSame(1, preg_match('//u', (string) $dlq->dlqerror));
    }

    public function testMaxLengthCuentaPuntosDeCodigoNoBytesNiUnidadesUtf16(): void
    {
        self::assertSame(1000, Text::codePointLength(str_repeat('😀', 1000))); // 4000 bytes, 2000 unidades UTF-16
        self::assertSame(1, Text::codePointLength('é'));
        self::assertSame(str_repeat('😀', 999) . '…', Text::truncateCodePoints(str_repeat('😀', 1001), 1000));
    }

    public function testElPoisonCrudoDeLaDlqTambienCortaEnBytes(): void
    {
        // El `substr` de antes cortaba un emoji por la mitad y `json_encode` fallaba: el POISON no llegaba a la DLQ.
        $texto = Text::truncateUtf8Bytes('POISON: ' . str_repeat('😀', 400), 1024);

        self::assertLessThanOrEqual(1024, strlen($texto));
        self::assertSame(1, preg_match('//u', $texto));
    }

    public function testElTimeSeNormalizaOSeRechazaSinCopiarlo(): void
    {
        self::assertSame('2026-08-20T10:25:39.412Z', Time::canonical('2026-08-20T12:25:39.4129+02:00'));
        self::assertNull(Time::canonical('2026-08-20T10:25:39.412Z '), 'un espacio final no es un instante');
        self::assertNull(Time::canonical(5));
        self::assertNull(Time::canonical(null));
        self::assertSame('2024-02-29T23:59:59.999Z', Time::canonical('2024-02-29T23:59:59.999Z'), '2024 es bisiesto');
        self::assertNull(Time::canonical('2026-02-29T00:00:00Z'), '2026 no lo es');
        self::assertSame(1787221539412, Time::toEpochMs('2026-08-20T10:25:39.412Z'));
        self::assertSame('2026-08-20T10:25:39.412Z', Time::fromEpochMs(1787221539412));
    }

    public function testLaFormaCanonicaDeDataEsLaDeJsonStringify(): void
    {
        // U+2028 y U+2029 literales (PHP los escapa por defecto, `JSON.stringify` no), `/` y UTF-8 sin escapar.
        self::assertSame("{\"a\":\"x\u{2028}y<&>\"}", Json::encode(['a' => "x\u{2028}y<&>"]));
        self::assertSame('{"u":"http://x/y","n":"ñandú"}', Json::encode(['u' => 'http://x/y', 'n' => 'ñandú']));
        // Controles escapados en minúsculas, DEL literal.
        self::assertSame('{"a":"\u0001\n\t\u001f' . "\x7f" . '"}', Json::encode(['a' => "\x01\n\t\x1f\x7f"]));
    }

    public function testUnFlotanteEnteroSaleSinPuntoCeroComoEnLaReferencia(): void
    {
        // `JSON.stringify(4995.0)` es `4995`; con JSON_PRESERVE_ZERO_FRACTION saldría `4995.0` y los bytes divergirían de Node.
        self::assertSame('{"a":4995,"b":0.1,"c":49.5}', Json::encode(['a' => 4995.0, 'b' => 0.1, 'c' => 49.5]));
    }

    public function testElPrecisionDeUnPhpIniExoticoNoCambiaLosBytes(): void
    {
        $antes = ini_get('serialize_precision');
        ini_set('serialize_precision', '17');
        try {
            self::assertSame('{"a":0.1}', Json::encode(['a' => 0.1]), 'con serialize_precision=17 saldría 0.10000000000000001');
            self::assertSame('17', ini_get('serialize_precision'), 'se restaura');
        } finally {
            ini_set('serialize_precision', (string) $antes);
        }
    }

    public function testUnObjetoVacioAnidadoSobreviveAlViajeDeIdaYVuelta(): void
    {
        // Con `assoc = true` el `{}` se colapsaba en `[]` y el evento de la DLQ salía con otros bytes.
        $cuerpo = '{"specversion":"1.0","id":"a","source":"/d/s","type":"com.flux.p.p.c.v1","time":"2026-08-20T10:25:39.412Z",'
            . '"datacontenttype":"application/json","dataschema":"https://s/x","correlationid":"a","tenantid":"t",'
            . '"producerversion":"1","dataclassification":"internal","data":{"a":{},"b":[],"c":{"d":{}},"e":[{}]}}';

        self::assertSame($cuerpo, Envelope::serialize(Envelope::parse($cuerpo)));
    }

    public function testUnaClaveRepetidaConservaLaPosicionDeLaPrimeraYElValorDeLaUltima(): void
    {
        $cuerpo = '{"specversion":"1.0","id":"a","source":"/d/s","type":"com.flux.p.p.c.v1","time":"2026-08-20T10:25:39.412Z",'
            . '"datacontenttype":"application/json","dataschema":"https://s/x","correlationid":"a","tenantid":"t",'
            . '"producerversion":"1","dataclassification":"internal","data":{"a":1,"b":3,"a":2}}';

        self::assertStringContainsString('"data":{"a":2,"b":3}', Envelope::serialize(Envelope::parse($cuerpo)));
    }

    public function testUnCuerpoQueNoEsUtf8OLlevaBomEsMalformedJson(): void
    {
        foreach (["\xEF\xBB\xBF{}", "{\"a\":\"\xFF\"}"] as $malo) {
            try {
                Envelope::parse($malo);
                self::fail('debía ser POISON');
            } catch (PoisonError $e) {
                self::assertSame('MALFORMED_JSON', $e->errorCode);
            }
        }
    }
}
