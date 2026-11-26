<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\State\PendingSum;
use Flux\State\StateClient;
use Flux\State\SyncTracker;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/** Las piezas del estado que no necesitan broker: la suma de `pending`, el seguimiento de `onSynced` y el patrón de subjects de la DLQ. */
final class StateSupportTest extends TestCase
{
    public function testPendingEsLaSumaDeLosDurablesDeLaSerieNoElValorDelUltimo(): void
    {
        $p = new PendingSum();
        self::assertSame(2, $p->note('state.c|svc', 'svc__acme', 2));
        // un segundo durable (otro tenant) publica las MISMAS etiquetas: sin sumar, pisaría al primero
        self::assertSame(5, $p->note('state.c|svc', 'svc__globex', 3));
        self::assertSame(4, $p->note('state.c|svc', 'svc__globex', 2));
        // otra serie no se mezcla
        self::assertSame(7, $p->note('state.d|svc', 'x', 7));
        self::assertSame(2, $p->drop('state.c|svc', 'svc__globex'));
        self::assertNull($p->drop('state.c|svc', 'no-existe'));
        self::assertSame(0, $p->drop('state.c|svc', 'svc__acme'));
    }

    public function testOnSyncedSeInvocaUnaVezCuandoLaUltimaEntradaYTodosLosHandlersTerminaron(): void
    {
        $n = 0;
        $t = new SyncTracker(function () use (&$n): void {
            $n++;
        });
        $t->started(2);
        $t->finished();
        $t->started(1);
        self::assertSame(0, $n);
        $t->finished();
        self::assertSame(0, $n, 'quedaba una entrada por entregar');
        $t->started(0);
        self::assertSame(0, $n, 'la última entrega empezó, pero su handler no ha terminado');
        $t->finished();
        self::assertSame(1, $n);
        $t->started(0);
        $t->finished();
        self::assertSame(1, $n, 'una sola vez');
    }

    public function testElSincronizadoInmediatoNoSeRepite(): void
    {
        $n = 0;
        $t = new SyncTracker(function () use (&$n): void {
            $n++;
        });
        $t->immediate();
        $t->immediate();
        $t->started(0);
        $t->finished();
        self::assertSame(1, $n);
        self::assertTrue($t->isDone());
    }

    /** @return array<string,array{0:string,1:string,2:bool}> */
    public static function patrones(): array
    {
        return [
            'exacto' => ['dlq.state.clientes.cliente.v1', 'dlq.state.clientes.cliente.v1', true],
            'cola' => ['dlq.state.clientes.>', 'dlq.state.clientes.x.v1', true],
            'cola exige algo' => ['dlq.state.clientes.>', 'dlq.state.clientes', false],
            'otro dominio' => ['dlq.clientes.>', 'dlq.state.clientes.x.v1', false],
            'comodin' => ['dlq.*.clientes.x.v1', 'dlq.state.clientes.x.v1', true],
            'mas largo' => ['dlq.state.clientes', 'dlq.state.clientes.x', false],
            'mas corto' => ['dlq.state.clientes.x', 'dlq.state.clientes', false],
        ];
    }

    #[DataProvider('patrones')]
    public function testSubjectMatches(string $patron, string $subject, bool $esperado): void
    {
        self::assertSame($esperado, StateClient::subjectMatches($patron, $subject));
    }
}
