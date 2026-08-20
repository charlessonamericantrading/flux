<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\Envelope;
use Flux\ErrorClass;
use PHPUnit\Framework\TestCase;

/**
 * Tests **a nivel de bytes**.
 *
 * Existen porque flux no depende solo de que el JSON sea equivalente como dato, sino de la
 * secuencia exacta de BYTES, en cuatro sitios: el replay verbatim desde la DLQ, una firma
 * criptográfica futura sobre el evento serializado, la deduplicación por hash de contenido
 * y los fixtures compartidos entre los seis SDKs (01-envelope.md §1.1 y §6).
 *
 * Y existen especialmente en PHP porque los defaults de `json_encode` violan las dos
 * reglas a la vez: escapa todo lo no-ASCII (una `é` acaba como la secuencia de seis
 * caracteres barra-invertida-u-cero-cero-e-nueve) **y** escapa las barras (una `/` acaba
 * como barra-invertida-barra). Sin `JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES`,
 * cualquier evento
 * con un `dataschema` (es decir, todos) y cualquier texto en español difieren byte a byte
 * de los otros cinco SDKs, y ningún test que compare arrays decodificados lo vería.
 */
final class JsonEncodingTest extends TestCase
{
    public function testNoEscapaLosCaracteresNoAscii(): void
    {
        $json = Envelope::serialize(ProtocolFixture::event([
            'data' => ['nota' => 'café con leche — 5 €', 'cliente' => 'Nuñez & Söhne'],
        ]));

        self::assertStringContainsString('café con leche — 5 €', $json);

        // La secuencia de escape se construye con `chr(92)` en vez de escribirla literal
        // para que nada —ni un editor, ni un `sed`, ni un agente— pueda "arreglarla"
        // convirtiéndola en el carácter que precisamente NO debe aparecer.
        $escape = chr(92) . 'u'; // el prefijo de \uXXXX

        self::assertStringNotContainsString($escape, $json, 'ningún escape uXXXX en el envelope');
    }

    public function testLosFlagsPorDefectoDePhpProducirianBytesDistintos(): void
    {
        // El test de arriba comprueba que hacemos lo correcto; este comprueba que la
        // diferencia es REAL y no una precaución teórica. Si algún día PHP cambiase sus
        // defaults, este test avisaría de que las banderas ya no hacen falta — en vez de
        // dejarlas ahí como cargo cult.
        $event = ProtocolFixture::event(['data' => ['nota' => 'café']]);

        $correcto = Envelope::serialize($event);
        $porDefecto = json_encode($event->toArray());

        self::assertNotSame($porDefecto, $correcto);
        self::assertStringContainsString(chr(92) . 'u00e9', (string) $porDefecto);
        self::assertStringContainsString(chr(92) . '/', (string) $porDefecto);
    }

    public function testNoEscapaLasBarrasDeLasUrls(): void
    {
        // Sin `JSON_UNESCAPED_SLASHES` el `dataschema` saldría como
        // `https:\/\/schemas.internal\/...`. Sigue siendo JSON válido y se parsea al mismo
        // string, pero son bytes distintos de los que emiten Node, Python y Go.
        $json = Envelope::serialize(ProtocolFixture::event());

        self::assertStringContainsString(
            '"dataschema":"https://schemas.internal/pedidos/pedido/creado/1.2.0.json"',
            $json,
        );
        self::assertStringNotContainsString(chr(92) . '/', $json);
        // `source` también lleva barras.
        self::assertStringContainsString('"source":"/produccion/pedidos-api"', $json);
    }

    public function testLosBytesSonExactamenteLosEsperados(): void
    {
        // Fixture byte a byte. Si alguien cambia el orden de las claves, una bandera de
        // `json_encode` o el formato de `time`, esto falla — que es exactamente el punto.
        $event = ProtocolFixture::event([
            'aggregateId' => 'ped-123',
            'data' => ['pedidoId' => 'ped-123', 'moneda' => 'EUR', 'totalCents' => 9990],
        ]);

        $expected = '{"specversion":"1.0"'
            . ',"id":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55"'
            . ',"source":"/produccion/pedidos-api"'
            . ',"type":"com.flux.pedidos.pedido.creado.v1"'
            . ',"time":"2026-08-20T10:25:39.412Z"'
            . ',"datacontenttype":"application/json"'
            . ',"dataschema":"https://schemas.internal/pedidos/pedido/creado/1.2.0.json"'
            . ',"subject":"ped-123"'
            . ',"correlationid":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55"'
            . ',"tenantid":"acme"'
            . ',"producerversion":"3.4.1"'
            . ',"dataclassification":"confidential"'
            . ',"partitionkey":"ped-123"'
            . ',"data":{"pedidoId":"ped-123","moneda":"EUR","totalCents":9990}}';

        self::assertSame($expected, Envelope::serialize($event));
    }

    public function testDataVaSiempreElUltimo(): void
    {
        $keys = array_keys(json_decode(Envelope::serialize(ProtocolFixture::event()), true));

        self::assertSame('data', end($keys));
    }

    public function testEnElEventoDeDlqLasExtensionesVanAntesDeData(): void
    {
        // Esta regla nació de una divergencia real: el SDK de Node construía el evento de
        // DLQ con `{...event, dlq*}`, dejando las extensiones DESPUÉS de `data`, mientras
        // Python, Go y Java las ponían antes. El mismo evento, dos secuencias de bytes. La
        // suite de conformidad no lo veía porque su fixture solo cubría el envelope normal.
        $dead = Envelope::toDlqEvent(ProtocolFixture::event(), ErrorClass::Permanent, 1, 'c__s', 'boom');
        $keys = array_keys(json_decode(Envelope::serialize($dead), true));

        self::assertSame('data', end($keys));

        foreach (['dlqreason', 'dlqattempts', 'dlqconsumer', 'dlqerror', 'dlqtime'] as $extension) {
            self::assertLessThan(
                array_search('data', $keys, true),
                array_search($extension, $keys, true),
                "{$extension} debe ir ANTES de data",
            );
        }
    }

    public function testElOrdenEsElDelEjemploNormativo(): void
    {
        $event = ProtocolFixture::event([
            'aggregateId' => 'ped-123',
            'causationid' => '01924f8d-1a2b-7c3d-8e4f-5a6b7c8d9e01',
            'traceparent' => '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
        ]);

        self::assertSame(
            [
                'specversion', 'id', 'source', 'type', 'time', 'datacontenttype', 'dataschema',
                'subject', 'correlationid', 'tenantid', 'producerversion', 'dataclassification',
                'causationid', 'partitionkey', 'traceparent', 'data',
            ],
            array_keys(json_decode(Envelope::serialize($event), true)),
        );
    }

    public function testUnEventoConAcentosSobreviveAlRoundTrip(): void
    {
        $original = ProtocolFixture::event(['data' => ['descripcion' => 'Envío urgente — Coruña']]);
        $json = Envelope::serialize($original);

        self::assertSame($json, Envelope::serialize(Envelope::parse($json)));
    }
}
