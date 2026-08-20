<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\Transport\Ack;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * El protocolo de acuse de JetStream.
 *
 * El número de entrega se lee del subject de respuesta, y de él depende **todo** el
 * presupuesto de reintentos de 04-errors.md: si se leyese mal, un error desconocido con
 * presupuesto 2 reintentaría seis veces o ninguna.
 */
final class AckTest extends TestCase
{
    #[DataProvider('subjectsDeAck')]
    public function testExtraeElNumeroDeEntrega(string $replyTo, int $expected): void
    {
        self::assertSame($expected, Ack::deliveryAttempt($replyTo));
    }

    public function testSinSubjectDeRespuestaAsumeLaPrimeraEntrega(): void
    {
        // Default conservador a propósito: asumir el ÚLTIMO intento mandaría a la DLQ un
        // evento que aún tenía reintentos por delante, y hacia ese lado el error no se
        // recupera solo.
        self::assertSame(1, Ack::deliveryAttempt(null));
        self::assertSame(1, Ack::deliveryAttempt('esto.no.es.un.subject.de.ack'));
    }

    public function testElNakLlevaElRetrasoEnNanosegundos(): void
    {
        // La API de JetStream usa nanosegundos. Equivocar la unidad no da error: da un
        // reintento dentro de 30 microsegundos o dentro de 950 años.
        self::assertSame('-NAK {"delay":30000000000}', Ack::nakWithDelay(30_000));
    }

    public function testLosTokensSonLosQueEsperaElServidor(): void
    {
        self::assertSame('+ACK', Ack::ACK);
        self::assertSame('+TERM', Ack::TERM);
        // Sí, `+WPI` con las letras en ese orden: es el literal del servidor, no una
        // errata de "WIP".
        self::assertSame('+WPI', Ack::WIP);
    }

    /** @return iterable<string,array{string,int}> */
    public static function subjectsDeAck(): iterable
    {
        // v1, 9 tokens: $JS.ACK.<stream>.<consumer>.<entregas>.<sseq>.<cseq>.<tm>.<pending>
        yield 'v1 primera entrega' => [
            '$JS.ACK.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.1.42.7.1755689139000000000.0',
            1,
        ];
        yield 'v1 sexta entrega' => [
            '$JS.ACK.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.6.42.7.1755689139000000000.0',
            6,
        ];
        // v2, 12 tokens: añade dominio, hash de cuenta y un token aleatorio al final.
        yield 'v2 con dominio y hash de cuenta' => [
            '$JS.ACK.hub.ACCHASH.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.3.42.7.1755689139000000000.0.rand',
            3,
        ];
    }
}
