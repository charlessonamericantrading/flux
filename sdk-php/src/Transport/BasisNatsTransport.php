<?php

declare(strict_types=1);

namespace Flux\Transport;

use Flux\Protocol;

/**
 * Adaptador sobre `basis-company/nats`.
 *
 * ✅ **VERIFICADO contra NATS 2.14.5 y basis-company/nats 1.2.3** —
 * `tests/Integration/BasisNatsTransportIntegrationTest.php`, ejecutado con broker real.
 *
 * Lo estuvo sin verificar durante un tiempo y escondía un bug de raíz: `fetch()` usaba
 * `Client::request()`, que registra el handler en un inbox `_REQS.n`, mientras JetStream
 * entrega el mensaje extraído **en su subject original** con `reply-to` de `$JS.ACK…`.
 * El servidor entregaba el mensaje —quedaba en `num_ack_pending`— y nunca llegaba al
 * handler ni se confirmaba: un consumidor que consume sin procesar. Los tests con cliente
 * falso no podían verlo, porque devolvían obedientemente lo que el adaptador esperaba.
 *
 * Decisión de diseño, actualizada tras ese hallazgo: la API `$JS.API.*` por
 * petición/respuesta se usa para el **plano de control** (crear streams y consumidores,
 * leer su config, sondear `num_pending`), donde `request()` sí encaja porque la respuesta
 * llega al inbox. Para el **plano de datos** se usa `Consumer::handle()` con `ack: false`,
 * que es la única primitiva de la librería que enruta correctamente los mensajes de un
 * pull consumer y entrega el `reply-to` — el canal de ack/nak/term/WIP y la única fuente
 * del número de entrega. El `false` es obligatorio: el auto-ack de la librería confirmaría
 * antes de que corriese el handler, que es lo que 03-delivery.md prohíbe.
 *
 * Si tu cliente de NATS es otro, implementa `NatsTransport` y pásalo a `FluxBus::connect()`.
 * Es el motivo de que el puerto exista.
 */
final class BasisNatsTransport implements NatsTransport
{
    /** Prefijo de la API de JetStream. Sin dominio: flux v1 no federa multi-cluster. */
    private const API = '$JS.API';

    /** @var object `Basis\Nats\Client` — sin tipar para no requerir la librería en autoload. */
    private object $client;

    private bool $connected = true;

    /**
     * @param object $client Instancia de `Basis\Nats\Client` ya configurada (servidores,
     *                       credenciales, timeouts). El SDK no la construye para no
     *                       acoplar `ConnectOptions` a la configuración de una librería.
     */
    public function __construct(object $client)
    {
        if (!method_exists($client, 'publish') || !method_exists($client, 'request')) {
            throw new \InvalidArgumentException(
                'el cliente debe exponer publish() y request() (se esperaba Basis\\Nats\\Client). '
                . 'Si usas otro cliente de NATS, implementa Flux\\Transport\\NatsTransport '
                . 'directamente: es exactamente para lo que existe el puerto.'
            );
        }

        $this->client = $client;
    }

    public function isConnected(): bool
    {
        return $this->connected;
    }

    public function ensureStream(
        string $name,
        array $subjects,
        int $maxAgeMs,
        ?int $duplicateWindowMs = null,
    ): void {
        $existing = $this->apiRequest(self::API . '.STREAM.INFO.' . $name, [], allowError: true);
        if (!isset($existing['error'])) {
            return;
        }

        $config = [
            'name' => $name,
            'subjects' => $subjects,
            'storage' => 'file',
            'retention' => 'limits',
            'discard' => 'old',
            'max_age' => Protocol::nanos($maxAgeMs),
            // Con 3 réplicas no arrancaría contra un NATS de un solo nodo, y crear streams
            // desde el SDK es comodidad de desarrollo — 02-naming.md §3.2.
            'num_replicas' => Protocol::STREAM_REPLICAS_SDK_DEFAULT,
        ];

        if ($duplicateWindowMs !== null) {
            $config['duplicate_window'] = Protocol::nanos($duplicateWindowMs);
        }

        $this->apiRequest(self::API . '.STREAM.CREATE.' . $name, $config);
    }

    public function createDurableConsumer(string $stream, string $durable, array $config): array
    {
        $response = $this->apiRequest(
            self::API . '.CONSUMER.DURABLE.CREATE.' . $stream . '.' . $durable,
            ['stream_name' => $stream, 'config' => $config],
            allowError: true,
        );

        if (isset($response['error'])) {
            // El consumidor ya existe con otra config y el servidor rechaza el CREATE. Se
            // lee la que hay para que el desajuste se reporte con detalle
            // (`ConsumerConfigMismatchException`) en vez de con un error opaco del broker.
            $response = $this->apiRequest(
                self::API . '.CONSUMER.INFO.' . $stream . '.' . $durable,
                [],
            );
        }

        $effective = $response['config'] ?? null;

        if (!is_array($effective)) {
            throw new \RuntimeException(
                "el servidor no devolvió la configuración del consumidor \"{$durable}\". "
                . 'Sin ella no se puede verificar que aplicó lo solicitado, que es un '
                . 'requisito L2 (03-delivery.md §2.1).'
            );
        }

        return $effective;
    }

    public function consumerPending(string $stream, string $durable): ?int
    {
        // `allowError: true` porque un consumidor que aún no existe —o que acaba de
        // borrarse— es una respuesta legítima de la API, no un fallo del que avisar. El
        // sondeo es telemetría: no puede convertir un estado transitorio en un error.
        $response = $this->apiRequest(
            self::API . '.CONSUMER.INFO.' . $stream . '.' . $durable,
            [],
            allowError: true,
        );

        $pending = $response['num_pending'] ?? null;

        return is_int($pending) ? $pending : null;
    }

    /**
     * Saca hasta `$batch` mensajes del pull consumer.
     *
     * ⚠️ Usa la API de consumidor de la librería, **NO `Client::request()`**. Esa fue la
     * primera implementación y estaba mal de raíz: `request()` registra el handler en un
     * inbox (`_REQS.n`) y espera la respuesta ahí, pero JetStream entrega el mensaje
     * extraído **en su subject original** con un `reply-to` de `$JS.ACK…`. La librería no
     * sabe enrutarlo y lanza `LogicException: No handler for message _REQS.1 or
     * <subject>`.
     *
     * El síntoma en producción era el peor posible: el servidor SÍ entregaba el mensaje
     * —quedaba como `num_ack_pending: 1`— pero nunca llegaba al handler ni se confirmaba.
     * Un consumidor que consume sin procesar.
     *
     * No lo detectó ningún test porque todos usaban un cliente falso, que devolvía
     * obedientemente lo que el adaptador esperaba. Solo apareció al ejecutarlo contra un
     * broker real.
     *
     * `ack: false` es obligatorio: flux confirma explícitamente según el resultado del
     * handler (03-delivery.md §2), y el auto-ack de la librería confirmaría antes de que
     * el handler corriese.
     */
    public function fetch(string $stream, string $durable, int $batch, int $timeoutMs): array
    {
        $messages = [];

        try {
            $consumer = $this->client->getApi()->getStream($stream)->getConsumer($durable);
            $consumer
                ->setBatching($batch)
                // Una sola ronda: el bucle de reintento y el ritmo los decide `FluxBus::run`,
                // no el transporte.
                ->setIterations(1)
                ->setExpires(max(0.1, ($timeoutMs - 100) / 1000));

            $consumer->handle(
                function (mixed $payload, mixed $replyTo) use (&$messages, $stream): void {
                    $message = $this->toRawMessage($payload, $stream, $replyTo);
                    if ($message !== null) {
                        $messages[] = $message;
                    }
                },
                null,
                false,
            );
        } catch (MissingAckSubjectException $e) {
            // Este NO se traga: un mensaje sin subject de respuesta no se puede acusar, así
            // que confundirlo con "cola vacía" dejaría al worker girando eternamente sin
            // consumir. Va delante del catch genérico a propósito.
            throw $e;
        } catch (\Throwable) {
            // Un fetch vacío llega como error de timeout en la mayoría de clientes. No es
            // un fallo: un consumidor ocioso es el estado normal, y tratarlo como error
            // llenaría los logs de ruido y escondería los fallos de verdad.
            return [];
        }

        return $messages;
    }

    public function publish(string $subject, string $payload, array $headers = []): void
    {
        // Se usa `request()` y no `publish()` a propósito: la respuesta es el PubAck de
        // JetStream. Un publish de **core** NATS a un subject que ningún stream captura no
        // da NINGÚN error y el evento se evapora sin rastro (verificado contra NATS 2.14.5,
        // 02-naming.md §1.1). Esperar el acuse del stream es la única red de seguridad
        // automática contra una errata de subject, y por eso un SDK NO DEBE exponer
        // publicación por core para eventos de dominio.
        $response = null;

        $this->client->request(
            $subject,
            $this->payload($payload, $headers),
            function (mixed $ack) use (&$response): void {
                $response = $this->decode($this->bodyOf($ack));
            },
        );

        if ($response === null) {
            // Sin PubAck no hay garantía de que el evento esté en ningún stream, y devolver
            // en silencio sería justo el fallo que este método existe para impedir: el
            // publish de core a un subject que ningún stream captura no da error y el
            // evento se evapora (02-naming.md §1.1). "No stream response" llega así.
            throw new \RuntimeException(
                "JetStream no acusó recibo de la publicación en \"{$subject}\". Ningún stream "
                . 'captura ese subject, o la petición expiró. Revisa las mayúsculas (NATS es '
                . 'case-sensitive) y que el stream del dominio exista.'
            );
        }

        if (is_array($response) && isset($response['error'])) {
            $error = is_array($response['error'])
                ? json_encode($response['error'], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES)
                : (string) $response['error'];

            throw new \RuntimeException(
                "JetStream rechazó la publicación en \"{$subject}\": {$error}. "
                . 'Si dice "no stream response", ningún stream captura ese subject: revisa '
                . 'las mayúsculas (NATS es case-sensitive) y que el stream del dominio exista.'
            );
        }
    }

    public function respond(string $replyTo, string $payload): void
    {
        // El acuse SÍ va por core: es un mensaje efímero al inbox del consumidor, no un
        // evento de dominio, y no hay stream que deba capturarlo.
        $this->client->publish($replyTo, $payload);
    }

    public function close(): void
    {
        $this->connected = false;

        if (method_exists($this->client, 'disconnect')) {
            $this->client->disconnect();
        }
    }

    // ─── Interior ─────────────────────────────────────────────────────────────

    /**
     * @param array<string,mixed> $body
     * @return array<string,mixed>
     */
    private function apiRequest(string $subject, array $body, bool $allowError = false): array
    {
        $response = null;

        $this->client->request(
            $subject,
            $body === [] ? '' : json_encode($body, JSON_THROW_ON_ERROR),
            function (mixed $payload) use (&$response): void {
                $response = $this->decode($this->bodyOf($payload));
            },
        );

        if (!is_array($response)) {
            throw new \RuntimeException(
                "la API de JetStream no respondió a \"{$subject}\". ¿Está JetStream habilitado "
                . 'en el servidor y la cuenta tiene permiso sobre $JS.API.>?'
            );
        }

        if (isset($response['error']) && !$allowError) {
            $detail = json_encode($response['error'], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
            throw new \RuntimeException("la API de JetStream rechazó \"{$subject}\": {$detail}");
        }

        return $response;
    }

    /**
     * Convierte lo que devuelva la librería en un `RawMessage`.
     *
     * ⚠️ Este es el punto exacto donde el adaptador depende de detalles de
     * `basis-company/nats`. Si tu versión no expone `replyTo` en el objeto que recibe el
     * handler, **el SDK no puede acusar recibo** y aquí se ve en alto en vez de perder
     * mensajes en silencio: sin `replyTo` no hay ack, no hay número de entrega y todo el
     * presupuesto de reintentos de 04-errors.md deja de funcionar.
     */
    /**
     * @param mixed $replyToExplicito El `reply-to` que entrega `Consumer::handle()` como
     *        segundo argumento. La librería lo pasa aparte porque el `Payload` no lo
     *        lleva dentro: buscarlo solo en el payload devolvía siempre `null` y todo
     *        mensaje acababa descartado como "cola vacía".
     */
    private function toRawMessage(mixed $payload, string $stream, mixed $replyToExplicito = null): ?RawMessage
    {
        $body = $this->bodyOf($payload);
        $replyTo = $replyToExplicito
            ?? (is_object($payload) ? ($payload->replyTo ?? null) : null);
        $subject = is_object($payload) ? ($payload->subject ?? $stream) : $stream;

        // Los mensajes de estado (404 No Messages, 408 Request Timeout) llegan con cuerpo
        // vacío y sin reply. No son eventos: son la forma que tiene JetStream de decir
        // "no hay nada".
        if ($body === '' && $replyTo === null) {
            return null;
        }

        if (!is_string($replyTo) || $replyTo === '') {
            throw new MissingAckSubjectException(
                'el mensaje llegó sin subject de respuesta ($JS.ACK.…). Sin él no se puede '
                . 'hacer ack, nak ni term, y el número de entrega es indeterminable. '
                . 'Comprueba que tu versión de basis-company/nats expone `replyTo`, o '
                . 'implementa Flux\\Transport\\NatsTransport sobre otro cliente.'
            );
        }

        /** @var array<string,string> $headers */
        $headers = is_object($payload) && is_array($payload->headers ?? null) ? $payload->headers : [];

        return new RawMessage(
            subject: is_string($subject) ? $subject : $stream,
            payload: $body,
            replyTo: $replyTo,
            headers: $headers,
        );
    }

    /**
     * Envuelve cuerpo y cabeceras en el `Payload` de la librería cuando existe; si no,
     * devuelve el cuerpo pelado.
     *
     * Perder las cabeceras aquí sería perder `Nats-Msg-Id`, y con él la deduplicación de
     * publicaciones dentro de `duplicate_window` (03-delivery.md §3), así que si la clase
     * no está se avisa en alto en lugar de degradar en silencio.
     *
     * @param array<string,string> $headers
     */
    private function payload(string $body, array $headers): mixed
    {
        if ($headers === []) {
            return $body;
        }

        /** @var class-string|string $payloadClass */
        $payloadClass = 'Basis\\Nats\\Message\\Payload';
        if (!class_exists($payloadClass)) {
            throw new \RuntimeException(
                'no se encontró Basis\\Nats\\Message\\Payload, así que no se pueden enviar '
                . 'cabeceras NATS. Sin la cabecera Nats-Msg-Id no hay deduplicación de '
                . 'publicaciones (03-delivery.md §3). Instala basis-company/nats o usa otro '
                . 'transporte.'
            );
        }

        return new $payloadClass($body, $headers);
    }

    private function bodyOf(mixed $payload): string
    {
        if (is_string($payload)) {
            return $payload;
        }

        if (is_object($payload)) {
            foreach (['body', 'payload', 'data'] as $property) {
                if (isset($payload->{$property}) && is_string($payload->{$property})) {
                    return $payload->{$property};
                }
            }

            if (method_exists($payload, '__toString')) {
                return (string) $payload;
            }
        }

        return '';
    }

    /** @return array<string,mixed>|null */
    private function decode(string $body): ?array
    {
        if ($body === '') {
            return null;
        }

        try {
            $decoded = json_decode($body, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            return null;
        }

        return is_array($decoded) ? $decoded : null;
    }
}
