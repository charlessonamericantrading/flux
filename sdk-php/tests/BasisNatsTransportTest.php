<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\Transport\BasisNatsTransport;
use Flux\Transport\MissingAckSubjectException;
use PHPUnit\Framework\TestCase;

/**
 * El adaptador de transporte, con un cliente falso.
 *
 * ⚠️ Esto **no** verifica que el adaptador hable bien con NATS de verdad: eso solo lo dice
 * un servidor real y sigue pendiente. Lo que fija es su comportamiento ante las respuestas
 * que importan, y en particular las dos formas de **fallar en silencio** que tenía y que
 * son el peor modo de fallo posible en un bus de eventos: un worker que gira sin consumir
 * y un evento que se evapora sin error.
 */
final class BasisNatsTransportTest extends TestCase
{
    private const SUBJECT = 'pedidos.pedido.v1.creado';
    private const ACK_REPLY = '$JS.ACK.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.3.1.1.0.0';

    // ─── fetch ────────────────────────────────────────────────────────────────

    public function testUnMensajeSinSubjectDeRespuestaFallaEnAlto(): void
    {
        // Sin `$JS.ACK.…` no se puede hacer ack, nak ni term, y el número de entrega es
        // indeterminable — con lo que todo el presupuesto de reintentos de 04-errors.md
        // deja de funcionar. Confundirlo con "cola vacía" dejaría al worker girando
        // eternamente sin consumir nada.
        $client = FakeNatsClient::entregando((object) [
            'body' => '{"hola":1}',
            'subject' => self::SUBJECT,
        ]);

        $this->expectException(MissingAckSubjectException::class);
        $this->expectExceptionMessageMatches('/subject de respuesta/u');

        (new BasisNatsTransport($client))->fetch('EVT_PEDIDOS', 'svc__x', 1, 100);
    }

    public function testUnFetchVacioNoEsUnError(): void
    {
        // Un consumidor ocioso es el estado normal, y en la mayoría de clientes eso llega
        // como un error de timeout. Tratarlo como fallo llenaría los logs de ruido y
        // escondería los fallos de verdad.
        $client = FakeNatsClient::lanzando(new \RuntimeException('nats: timeout'));

        self::assertSame([], (new BasisNatsTransport($client))->fetch('EVT_PEDIDOS', 'svc__x', 1, 100));
    }

    public function testDevuelveElMensajeConSuSubjectDeRespuesta(): void
    {
        $client = FakeNatsClient::entregando((object) [
            'body' => '{"hola":1}',
            'subject' => self::SUBJECT,
            'replyTo' => self::ACK_REPLY,
        ]);

        $mensajes = (new BasisNatsTransport($client))->fetch('EVT_PEDIDOS', 'svc__x', 1, 100);

        self::assertCount(1, $mensajes);
        self::assertSame(self::SUBJECT, $mensajes[0]->subject);
        self::assertSame('{"hola":1}', $mensajes[0]->payload);
        self::assertSame(self::ACK_REPLY, $mensajes[0]->replyTo);
    }

    public function testUnMensajeDeEstadoSinCuerpoNiReplyNoEsUnEvento(): void
    {
        // `404 No Messages` / `408 Request Timeout` son la forma que tiene JetStream de
        // decir "no hay nada", no eventos.
        $client = FakeNatsClient::entregando((object) ['body' => '']);

        self::assertSame([], (new BasisNatsTransport($client))->fetch('EVT_PEDIDOS', 'svc__x', 1, 100));
    }

    // ─── publish ──────────────────────────────────────────────────────────────

    public function testSinPubAckLaPublicacionFalla(): void
    {
        // ⚠️ El caso que este método existe para impedir: un publish de **core** NATS a un
        // subject que ningún stream captura no da NINGÚN error y el evento se evapora sin
        // rastro (verificado contra NATS 2.14.5, 02-naming.md §1.1). Esperar el acuse del
        // stream es la única red de seguridad automática contra una errata de subject; si
        // el silencio se aceptase, esa red no existiría.
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/no acusó recibo/u');

        (new BasisNatsTransport(FakeNatsClient::mudo()))->publish(self::SUBJECT, '{}');
    }

    public function testUnErrorDelStreamHaceFallarLaPublicacion(): void
    {
        $client = FakeNatsClient::respondiendo('{"error":{"code":503,"description":"no responders"}}');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/rechazó la publicación/u');

        (new BasisNatsTransport($client))->publish(self::SUBJECT, '{}');
    }

    public function testUnPubAckCorrectoNoLanza(): void
    {
        $client = FakeNatsClient::respondiendo('{"stream":"EVT_PEDIDOS","seq":42}');

        (new BasisNatsTransport($client))->publish(self::SUBJECT, '{}');

        $this->expectNotToPerformAssertions();
    }

    /**
     * Solo corre cuando `basis-company/nats` NO está instalada, así que se salta en
     * cualquier entorno normal — está en `require-dev` para los tests de integración.
     *
     * Va en su propio grupo para poder excluirlo de la pasada con `--fail-on-skipped`:
     * ese flag existe para cazar tests que se saltan **porque falta una extensión**, y
     * confundir eso con un salto por condición legítima y permanente convierte la
     * comprobación en ruido que alguien acabará desactivando.
     *
     * Para ejecutarlo:  composer remove --dev basis-company/nats && vendor/bin/phpunit
     *
     * @group sin-libreria-nats
     */
    public function testSinLaLibreriaNoSePierdenLasCabecerasEnSilencio(): void
    {
        // Perder las cabeceras sería perder `Nats-Msg-Id`, y con él la deduplicación de
        // publicaciones dentro de `duplicate_window` (03-delivery.md §3). Mejor fallar.
        if (class_exists('Basis\\Nats\\Message\\Payload')) {
            self::markTestSkipped('basis-company/nats está instalada: las cabeceras sí viajan');
        }

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/Nats-Msg-Id/');

        (new BasisNatsTransport(FakeNatsClient::respondiendo('{"seq":1}')))
            ->publish(self::SUBJECT, '{}', ['Nats-Msg-Id' => 'abc']);
    }

    // ─── construcción ─────────────────────────────────────────────────────────

    public function testRechazaUnClienteQueNoSirve(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/NatsTransport/');

        new BasisNatsTransport(new \stdClass());
    }

    public function testCerrarMarcaLaConexionComoCaida(): void
    {
        $transport = new BasisNatsTransport(FakeNatsClient::mudo());
        self::assertTrue($transport->isConnected());

        $transport->close();

        self::assertFalse($transport->isConnected());
    }
}
