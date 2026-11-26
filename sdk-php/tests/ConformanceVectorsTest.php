<?php

declare(strict_types=1);

namespace Flux\Tests;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Los vectores de conformidad cruzada ejecutados **en proceso**, con la misma lógica que el arnés (`Harness`): los 87 de estado, los
 * 100 de petición/respuesta y los 93 de «Ajustes» de `conformance/harness/vectors.json`, más los ejemplos de la spec con su SHA-256
 * (`conformance/cases/cross-sdk-{state,rpc}-envelope.json`) y los de `id` determinista (`cross-sdk-external-id.json`).
 *
 * Lo que fija un vector es lo que dice la **spec** (`sha256`, `expect`, `expectSubject`, `expectEffectiveMs`), no lo que haga Node:
 * el runner cruzado (`node conformance/cross-sdk.mjs`) compara además PHP contra Node byte a byte. Aquí un vector sin esas claves
 * (los fija «la referencia») se comprueba como bytes válidos y sin más.
 */
final class ConformanceVectorsTest extends TestCase
{
    /**
     * En MODO OBJETO: un `data: {}` de un vector debe llegar al arnés como `{}` y no como `[]` (PHP los colapsa con `assoc = true`),
     * que es lo que el runner cruzado le manda.
     */
    private static function leer(string $ruta): object
    {
        $texto = file_get_contents(dirname(__DIR__, 2) . '/' . $ruta);
        self::assertIsString($texto, "no se pudo leer {$ruta}");

        return json_decode($texto, false, 512, JSON_THROW_ON_ERROR);
    }

    private static function json(mixed $v): string
    {
        return json_encode($v, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION);
    }

    /** @return iterable<string,array{string,object}> */
    public static function vectores(): iterable
    {
        $v = self::leer('conformance/harness/vectors.json');
        foreach (['stateVectors', 'rpcVectors', 'ajustesVectors'] as $bloque) {
            foreach ($v->{$bloque} as $vector) {
                yield "{$bloque}/{$vector->id}" => [$bloque, $vector];
            }
        }
    }

    #[DataProvider('vectores')]
    public function testElVectorDaLoQueDiceLaSpec(string $bloque, object $vector): void
    {
        $entrada = (object) (['op' => $vector->op] + (array) $vector->input);
        $texto = self::json($entrada);
        if (isset($vector->dataTexto)) {
            // `data` como TEXTO CRUDO (espacios, escapes, una clave repetida): se inyecta sin reserializar, como hace el runner.
            $texto = substr($texto, 0, -1) . ',"data":' . $vector->dataTexto . '}';
        }

        $r = Harness::run($texto);

        if (isset($vector->expect)) {
            self::assertFalse($r['ok'], "debía rechazarlo con {$vector->expect}");
            self::assertSame($vector->expect, $r['code'], (string) ($r['detail'] ?? ''));

            return;
        }
        self::assertTrue($r['ok'], 'la operación falló: ' . ($r['code'] ?? '?') . ' ' . ($r['detail'] ?? ''));
        if (isset($vector->expectSubject)) {
            self::assertSame($vector->expectSubject, $r['subject']);
        } elseif (isset($vector->expectEffectiveMs)) {
            self::assertSame($vector->expectEffectiveMs, $r['effectiveMs']);
        } elseif (isset($vector->sha256)) {
            $bytes = base64_decode($r['bytes'], true);
            self::assertIsString($bytes);
            self::assertSame($vector->sha256, hash('sha256', $bytes));
            if (isset($vector->length)) {
                self::assertSame($vector->length, strlen($bytes));
            }
        }
    }

    /** @return iterable<string,array{string,object}> */
    public static function ejemplosDeLaSpec(): iterable
    {
        foreach (['state' => 'conformance/cases/cross-sdk-state-envelope.json', 'rpc' => 'conformance/cases/cross-sdk-rpc-envelope.json'] as $cap => $ruta) {
            foreach (self::leer($ruta)->fixtures as $f) {
                yield "{$cap}/{$f->id}" => [$cap, $f];
            }
        }
    }

    #[DataProvider('ejemplosDeLaSpec')]
    public function testLosBytesSonLosDeLaSpecYDeSuSha256(string $cap, object $fixture): void
    {
        $r = Harness::run(self::json((object) (['op' => $fixture->harnessOp] + (array) $fixture->input)));

        self::assertTrue($r['ok'], ($r['code'] ?? '?') . ' ' . ($r['detail'] ?? ''));
        $bytes = base64_decode($r['bytes'], true);
        self::assertSame(base64_decode($fixture->base64, true), $bytes, 'los bytes difieren de los de la spec');
        self::assertSame($fixture->bytes, strlen($bytes));
        self::assertSame($fixture->sha256, hash('sha256', $bytes));
    }

    /** @return iterable<string,array{object,bool}> */
    public static function idsDeterministas(): iterable
    {
        $caso = self::leer('conformance/cases/cross-sdk-external-id.json');
        foreach ($caso->fixtures as $f) {
            yield "valido/{$f->id}" => [$f, true];
        }
        foreach ($caso->invalid as $f) {
            yield "invalido/{$f->id}" => [$f, false];
        }
    }

    #[DataProvider('idsDeterministas')]
    public function testElIdDeterministaEsElDeLaSpec(object $fixture, bool $valido): void
    {
        $r = Harness::run(self::json(['op' => 'external-id', 'input' => $fixture->input]));

        self::assertSame($valido, $r['ok']);
        if ($valido) {
            self::assertSame($fixture->expected->id, $r['id']);
        }
    }

    public function testElIdDeterministaEsUnUuidV7ValidoYOrdenaPorElInstante(): void
    {
        $ids = [];
        for ($i = 0; $i < 20_000; $i++) {
            $ids[] = \Flux\ExternalId::derive('/dev/a', "evt_{$i}", '2026-08-20T10:25:39.412Z');
        }

        self::assertCount(20_000, array_unique($ids), '74 bits: 20 000 claves distintas dan 20 000 ids distintos');
        foreach (array_slice($ids, 0, 200) as $id) {
            self::assertMatchesRegularExpression('/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/', $id);
            self::assertStringStartsWith('01a01eb4-7a54', $id, 'los 12 primeros dígitos son los milisegundos de `time`');
        }
        self::assertSame($ids[0], \Flux\ExternalId::derive('/dev/a', 'evt_0', '2026-08-20T10:25:39.412Z'), 'determinista');
        self::assertLessThan(
            \Flux\ExternalId::derive('/dev/a', 'evt_0', '2026-08-20T10:25:39.413Z'),
            $ids[0],
            'dos ids con la misma clave y source y distinto instante ordenan como los instantes',
        );
    }
}
