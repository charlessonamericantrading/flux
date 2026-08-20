<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\Envelope;
use Flux\FluxEvent;

/**
 * Utilidades compartidas por los tests.
 *
 * `protocol.json` se lee del repositorio, no se copia: si el SDK y el contrato divergen,
 * la suite falla en CI en vez de descubrirse en producción con otro SDK del ecosistema.
 */
final class ProtocolFixture
{
    public const SUBJECT = 'pedidos.pedido.v1.creado';

    /** @var array<string,mixed>|null */
    private static ?array $protocol = null;

    /** @return array<string,mixed> */
    public static function protocol(): array
    {
        if (self::$protocol === null) {
            $path = dirname(__DIR__, 2) . '/protocol.json';
            $raw = file_get_contents($path);
            if ($raw === false) {
                throw new \RuntimeException("no se pudo leer {$path}");
            }
            /** @var array<string,mixed> $decoded */
            $decoded = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
            self::$protocol = $decoded;
        }

        return self::$protocol;
    }

    /** @param array<string,mixed> $overrides */
    public static function event(array $overrides = []): FluxEvent
    {
        $base = [
            'subject' => self::SUBJECT,
            'data' => ['pedidoId' => 'ped-123', 'aggregateVersion' => 1],
            'id' => '01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55',
            'source' => '/produccion/pedidos-api',
            'producerversion' => '3.4.1',
            'tenantid' => 'acme',
            'dataclassification' => 'confidential',
            'dataschema' => 'https://schemas.internal/pedidos/pedido/creado/1.2.0.json',
            'correlationid' => '01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55',
            'time' => '2026-08-20T10:25:39.412Z',
        ];

        return Envelope::build(...array_merge($base, $overrides));
    }

    /** @return array<string,mixed> */
    public static function rawEvent(array $overrides = []): array
    {
        /** @var array<string,mixed> $raw */
        $raw = json_decode(Envelope::serialize(self::event()), true, 512, JSON_THROW_ON_ERROR);

        foreach ($overrides as $key => $value) {
            $raw[$key] = $value;
        }

        return $raw;
    }

    /** @param array<string,mixed> $raw */
    public static function encode(array $raw): string
    {
        return json_encode($raw, Envelope::JSON_FLAGS);
    }
}
