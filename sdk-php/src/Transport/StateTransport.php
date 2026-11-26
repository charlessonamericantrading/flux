<?php

declare(strict_types=1);

namespace Flux\Transport;

/**
 * Lo que el estado replicado (`StateClient`) necesita del broker además de lo que ya pide un consumidor de eventos: una publicación **con
 * acuse y con cabeceras** (`Nats-Expected-Last-Subject-Sequence`), la lectura directa por subject (`DIRECT.GET`) y la consulta de
 * consumidores y streams que distingue «no existe» de «no tengo permiso».
 *
 * Lo implementa `SocketTransport`, el transporte por defecto. **`BasisNatsTransport` y `InMemoryTransport` no lo implementan** —ni la
 * librería ni un transporte en memoria dan `DIRECT.GET` ni la publicación condicional— y el estado, con ellos, falla de forma explícita
 * en vez de degradarse (A-220). Existe como interfaz para que una prueba pueda envolver el transporte y medir lo que el SDK hace por
 * el cable (cuántas lecturas, cuántas publicaciones) o inyectar un fallo en un punto exacto sin tocar el código de producción.
 */
interface StateTransport extends NatsTransport, ProbesPull
{
    /**
     * Publica por JetStream y devuelve el acuse (`stream`, `seq`, `duplicate`) o `{ error: … }`.
     *
     * @param array<string,string> $headers
     *
     * @return array<string,mixed>
     *
     * @throws \Flux\Nats\NoRespondersException si ningún stream captura el subject
     */
    public function publishAck(string $subject, string $payload, array $headers = [], int $timeoutMs = 5_000): array;

    /**
     * `DIRECT.GET` por subject: el último mensaje, o `null` si la clave no existe (404).
     *
     * @return array{seq:int,data:string,headers:array<string,string>}|null
     *
     * @throws \Flux\NotProvisionedException|\Flux\PermissionDeniedException
     */
    public function directGet(string $stream, string $subject, int $timeoutMs = 5_000): ?array;

    /**
     * Toma el consumidor durable si existe y, si no, lo crea (desarrollo).
     *
     * @param array<string,mixed> $config
     *
     * @return array{config:array<string,mixed>,numPending:int,numAckPending:int}
     */
    public function ensureConsumer(string $stream, string $durable, array $config): array;

    /**
     * El consumidor por su nombre, o `null` si no existe.
     *
     * @return array{config:array<string,mixed>,numPending:int,numAckPending:int}|null
     */
    public function consumerInfo(string $stream, string $durable): ?array;

    /**
     * La configuración de un stream, o `null` si no existe.
     *
     * @return array<string,mixed>|null
     */
    public function streamConfig(string $name): ?array;
}
