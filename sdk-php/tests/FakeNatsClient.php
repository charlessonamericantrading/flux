<?php

declare(strict_types=1);

namespace Flux\Tests;

/**
 * Cliente con la forma mínima de `Basis\Nats\Client`: `publish()` y `request()`.
 *
 * Permite ejercer `BasisNatsTransport` sin la librería instalada y sin broker. No prueba
 * que el adaptador hable bien con NATS de verdad —eso solo lo dice un servidor real— pero
 * sí fija su comportamiento ante las tres respuestas que importan: PubAck correcto, error
 * del stream, y **silencio**.
 */
final class FakeNatsClient
{
    /** @var list<array{subject: string, payload: mixed, replyTo: ?string}> */
    public array $published = [];

    /** @param \Closure(string,mixed,?callable):void $onRequest */
    public function __construct(private readonly \Closure $onRequest)
    {
    }

    public function publish(string $subject, mixed $payload, ?string $replyTo = null): void
    {
        $this->published[] = ['subject' => $subject, 'payload' => $payload, 'replyTo' => $replyTo];
    }

    public function request(string $subject, mixed $payload, ?callable $handler = null): mixed
    {
        ($this->onRequest)($subject, $payload, $handler);

        return null;
    }

    /** El stream responde con un PubAck correcto. */
    public static function respondiendo(string $body): self
    {
        return new self(static function (string $s, mixed $p, ?callable $handler) use ($body): void {
            if ($handler !== null) {
                $handler($body);
            }
        });
    }

    /** Nadie responde: ni PubAck ni error. Es el caso peligroso. */
    public static function mudo(): self
    {
        return new self(static fn (): null => null);
    }

    /** El cliente revienta, como cuando un fetch se queda sin mensajes. */
    public static function lanzando(\Throwable $e): self
    {
        return new self(static function () use ($e): void {
            throw $e;
        });
    }

    /** Entrega un mensaje al handler del `fetch`. */
    public static function entregando(object $mensaje): self
    {
        $cliente = new self(static fn (): null => null);
        $cliente->aEntregar = [$mensaje];

        return $cliente;
    }

    // ─── plano de datos: getApi()->getStream()->getConsumer()->handle() ────────
    //
    // `fetch()` NO usa `request()`: JetStream entrega el mensaje extraído en su subject
    // original y no en el inbox de la petición, así que `Client::request()` no sabe
    // enrutarlo. Este falso reproduce la cadena que sí usa el adaptador.

    /** @var list<object> Mensajes que `handle()` entregará, cada uno con su replyTo. */
    public array $aEntregar = [];

    public function getApi(): object
    {
        return new class ($this) {
            public function __construct(private readonly FakeNatsClient $c)
            {
            }

            public function getStream(string $nombre): object
            {
                return new class ($this->c) {
                    public function __construct(private readonly FakeNatsClient $c)
                    {
                    }

                    public function getConsumer(string $durable): object
                    {
                        return new class ($this->c) {
                            public function __construct(private readonly FakeNatsClient $c)
                            {
                            }

                            public function setBatching(int $n): static
                            {
                                return $this;
                            }

                            public function setIterations(int $n): static
                            {
                                return $this;
                            }

                            public function setExpires(float $s): static
                            {
                                return $this;
                            }

                            /**
                             * Imita a `Consumer::handle`: entrega `($payload, $replyTo)` por
                             * SEPARADO —el `Payload` de la librería no lleva el replyTo
                             * dentro— y no confirma, porque el adaptador pide `ack: false`.
                             */
                            public function handle(callable $h, ?callable $vacio = null, bool $ack = true): int
                            {
                                foreach ($this->c->aEntregar as $m) {
                                    $h($m, $m->replyTo ?? null);
                                }

                                return count($this->c->aEntregar);
                            }
                        };
                    }
                };
            }
        };
    }
}
