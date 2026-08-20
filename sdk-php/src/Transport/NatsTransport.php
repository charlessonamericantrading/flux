<?php

declare(strict_types=1);

namespace Flux\Transport;

/**
 * Puerto hacia el broker. **Ningún tipo de NATS cruza esta frontera.**
 *
 * Esta interfaz es la diferencia de port más visible respecto a los otros cinco SDKs, que
 * hablan directamente con su cliente de NATS. Existe por dos motivos concretos, no por
 * gusto por las abstracciones:
 *
 * 1. **El ecosistema PHP de NATS no está consolidado.** No hay un cliente equivalente a
 *    `nats.js` o `nats-py`, y el más usado (`basis-company/nats`) es síncrono y no expone
 *    con estabilidad ni el subject de respuesta de JetStream ni el WIP. Con un puerto, un
 *    cambio de cliente es un fichero nuevo y no una reescritura del SDK.
 * 2. **Testabilidad real.** Toda la lógica que importa —clasificación, presupuesto de
 *    reintentos, "si la DLQ falla NO hagas term"— se ejerce en los tests con
 *    `InMemoryTransport`, sin broker y sin la librería instalada.
 *
 * Las duraciones van en **milisegundos** en esta interfaz y el adaptador las convierte a
 * los nanosegundos que espera la API `$JS.API.*`.
 */
interface NatsTransport
{
    public function isConnected(): bool;

    /**
     * Crea el stream si no existe. Comodidad de DESARROLLO: en producción los streams se
     * provisionan por IaC, porque son recurso compartido de todo el dominio y dejar que
     * los cree el primer servicio que arranque convierte retención, replicación y ventana
     * de duplicados en una carrera — 02-naming.md §3.2.
     *
     * @param list<string> $subjects
     */
    public function ensureStream(
        string $name,
        array $subjects,
        int $maxAgeMs,
        ?int $duplicateWindowMs = null,
    ): void;

    /**
     * Crea (o lee, si ya existía) el durable consumer y **devuelve la configuración
     * efectiva que reporta el servidor**.
     *
     * Devolverla no es un detalle de API: es la única defensa contra la sobrescritura
     * silenciosa de `ack_wait` por `backoff[0]` que documenta 03-delivery.md §2.1. El
     * servidor acepta la petición, no avisa y devuelve algo distinto de lo enviado.
     *
     * @param array<string,mixed> $config Config en el formato de la API de JetStream
     *                                    (`ack_wait` y `backoff` ya en nanosegundos).
     * @return array<string,mixed> La config efectiva, mismo formato.
     */
    public function createDurableConsumer(string $stream, string $durable, array $config): array;

    /**
     * Mensajes del stream **aún no entregados** a este consumidor (`num_pending`), o
     * `null` si no se puede determinar.
     *
     * Es la segunda fuente obligatoria de `flux_consumer_pending` — 08-observability.md
     * §2.3. La primera (los metadatos del subject de respuesta, ver `Ack::pending()`) es
     * gratis pero **solo llega cuando llegan mensajes**, y el fallo que esta métrica existe
     * para detectar es precisamente que dejen de llegar. Preguntándoselo al servidor el
     * dato sigue apareciendo con el consumidor parado, que es cuando hace falta.
     *
     * Devuelve `null` y no `0` cuando no se sabe: un cero se dibuja en el panel como "no hay
     * nada pendiente", que es justo la conclusión contraria a "no lo sabemos".
     *
     * ⚠️ Un fallo aquí **NO DEBE** afectar al consumo: esto es telemetría. Quien lo llama
     * captura y sigue.
     */
    public function consumerPending(string $stream, string $durable): ?int;

    /**
     * Pide hasta `$batch` mensajes al consumidor pull y espera como mucho `$timeoutMs`.
     *
     * Devuelve una lista vacía si no había nada — no es un error: un consumidor ocioso es
     * el estado normal.
     *
     * @return list<RawMessage>
     */
    public function fetch(string $stream, string $durable, int $batch, int $timeoutMs): array;

    /**
     * @param array<string,string> $headers `Nats-Msg-Id` entre ellas al publicar eventos.
     * @throws \Throwable si el stream no acusa recibo. Publicar por JetStream y NO por
     *         core NATS es lo que convierte una errata de subject en un error visible
     *         (02-naming.md §1.1): un publish de core a un subject que ningún stream
     *         captura no da error y el evento se evapora.
     */
    public function publish(string $subject, string $payload, array $headers = []): void;

    /**
     * Publica un token de acuse en el subject de respuesta de un mensaje.
     * Ver `Ack` para los tokens.
     */
    public function respond(string $replyTo, string $payload): void;

    public function close(): void;
}
