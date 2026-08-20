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
        return new self(static function (string $s, mixed $p, ?callable $handler) use ($mensaje): void {
            if ($handler !== null) {
                $handler($mensaje);
            }
        });
    }
}
