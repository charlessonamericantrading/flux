<?php

declare(strict_types=1);

namespace Flux\Transport;

use Flux\ConnectOptions;
use Flux\NotProvisionedException;
use Flux\PermissionDeniedException;
use Flux\Protocol;
use Flux\Nats\Connection;
use Flux\Nats\ConnectionOptions;
use Flux\Nats\Credentials;
use Flux\Nats\Denial;
use Flux\Nats\NoRespondersException;
use Flux\Nats\TlsOptions;
use Psr\Log\LoggerInterface;

/**
 * El transporte por defecto: `Flux\Nats\Connection` (el protocolo de NATS sobre un socket de PHP, sin dependencias).
 *
 * **Es el único que cumple la capacidad `security`** (06-security.md §3): inbox privado `_INBOX.<servicio>`, TLS con CA propia y
 * verificación de host, credenciales (`.creds` y nkey), `NOT_PROVISIONED` y `PERMISSION_DENIED` rápidos, los tres códigos de
 * `connect()` y un `subscribe()` que falla si se deniega el pull. `BasisNatsTransport` no puede: ver `Connection`.
 */
final class SocketTransport implements NatsTransport, ProbesPull
{
    /** Un timeout por defecto para las peticiones de la API de JetStream (el broker contesta en milisegundos o no contesta). */
    private const API_TIMEOUT_MS = 5_000;
    private const STREAM_NOT_FOUND = 10059;
    private const CONSUMER_NOT_FOUND = 10014;

    /** @var array<string,true> Los subjects denegados ya avisados: se avisa UNA vez por subject (un log por mensaje entierra la causa). */
    private array $warned = [];

    private function __construct(
        private readonly Connection $conn,
        private readonly string $service,
        private readonly ?LoggerInterface $logger,
    ) {
    }

    /**
     * Valida las opciones **sin abrir ningún socket**, abre la conexión y comprueba que la cuenta puede usar JetStream.
     *
     * @throws \Flux\InvalidCredentialsException|\Flux\InvalidTlsOptionsException antes de abrir ningún socket
     * @throws \Flux\ConnectionRejectedException  credenciales rechazadas (`auth`) o un TLS que no encaja (`tls`): al instante
     * @throws PermissionDeniedException          una ACL anterior a la v1.1 no concede `$JS.API.INFO`
     */
    public static function connect(ConnectOptions $options): self
    {
        Protocol::assertServiceName($options->service);
        // Todo lo que se puede comprobar sin red se comprueba AQUÍ, antes de dialar: un valor mal formado falla con su nombre y no se
        // degrada a «sin credenciales» o a «sin TLS».
        $credentials = Credentials::from($options->credentials);
        $tls = TlsOptions::from($options->tls);
        $servers = is_array($options->servers) ? array_values($options->servers) : [$options->servers];

        $conn = new Connection(new ConnectionOptions(
            servers: $servers,
            name: "{$options->service}@{$options->environment}",
            // Inbox PRIVADO, siempre y no configurable: con el `_INBOX.>` común cualquier servicio lee lo que reciben los demás.
            inboxPrefix: self::inboxPrefixFor($options->service),
            credentials: $credentials,
            tls: $tls,
            connectTimeoutMs: $options->connectTimeoutMs,
            waitForBrokerMs: $options->waitForBrokerMs,
            logger: $options->logger,
        ));
        $conn->connect();

        $transport = new self($conn, $options->service, $options->logger);
        try {
            // Lo que hace `jetstreamManager`: preguntar por la cuenta. Con una ACL anterior a la v1.1 el broker lo deniega, y sin este
            // cierre la conexión quedaría abierta tras el fallo.
            $transport->api('$JS.API.INFO');
        } catch (\Throwable $e) {
            $conn->close();
            throw $e;
        }

        return $transport;
    }

    /** `_INBOX.<servicio>`: el servicio pasa a ser un token de subject, no solo parte de un nombre de durable. */
    public static function inboxPrefixFor(string $service): string
    {
        Protocol::assertServiceName($service);

        return "_INBOX.{$service}";
    }

    public function connection(): Connection
    {
        return $this->conn;
    }

    public function isConnected(): bool
    {
        return $this->conn->isConnected();
    }

    public function close(): void
    {
        $this->conn->close();
    }

    // ─── La API de JetStream ──────────────────────────────────────────────────

    /**
     * Una petición a la API de JetStream. **Falla en tiempo útil, no por timeout**: si la ACL la deniega, el broker avisa por la
     * conexión y la petición lanza `PERMISSION_DENIED` en milisegundos nombrando el subject (06-security.md §3.7).
     *
     * @param array<string,mixed> $body
     *
     * @return array<string,mixed> la respuesta decodificada (puede llevar `error`)
     *
     * @throws PermissionDeniedException
     */
    public function api(string $subject, array $body = [], int $timeoutMs = self::API_TIMEOUT_MS): array
    {
        try {
            $reply = $this->conn->request($subject, $body === [] ? '' : json_encode($body, JSON_THROW_ON_ERROR), $timeoutMs);
        } catch (NoRespondersException $e) {
            throw new \RuntimeException(
                "la API de JetStream no responde a \"{$subject}\". ¿Está JetStream habilitado en el servidor y la cuenta tiene permiso "
                . 'sobre `$JS.API`?',
                0,
                $e,
            );
        }
        $decoded = json_decode($reply->payload, true);
        if (!is_array($decoded)) {
            throw new \RuntimeException("la API de JetStream contestó algo que no es JSON a \"{$subject}\"");
        }

        return $decoded;
    }

    /** @param array<string,mixed> $response */
    private static function errorCode(array $response): ?int
    {
        $e = $response['error'] ?? null;

        return is_array($e) && isset($e['err_code']) ? (int) $e['err_code'] : (is_array($e) ? -1 : null);
    }

    /** @param array<string,mixed> $response */
    private static function errorText(array $response): string
    {
        $e = $response['error'] ?? [];

        return is_array($e) ? (($e['description'] ?? 'error') . ' (' . ($e['err_code'] ?? $e['code'] ?? '?') . ')') : (string) $e;
    }

    public function ensureStream(string $name, array $subjects, int $maxAgeMs, ?int $duplicateWindowMs = null): void
    {
        $info = $this->api('$JS.API.STREAM.INFO.' . $name);
        $code = self::errorCode($info);
        if ($code === null) {
            return;
        }
        // Solo «no existe» autoriza a crearlo: cualquier otro fallo de `info` no es un recurso que falta.
        if ($code !== self::STREAM_NOT_FOUND) {
            throw new \RuntimeException("la API de JetStream rechazó consultar el stream \"{$name}\": " . self::errorText($info));
        }

        $config = [
            'name' => $name,
            'subjects' => $subjects,
            'storage' => 'file',
            'retention' => 'limits',
            'discard' => 'old',
            'max_age' => Protocol::nanos($maxAgeMs),
            // Con 3 réplicas no arrancaría contra un NATS de un solo nodo, y crear streams desde el SDK es comodidad de desarrollo.
            'num_replicas' => Protocol::STREAM_REPLICAS_SDK_DEFAULT,
        ];
        if ($duplicateWindowMs !== null) {
            $config['duplicate_window'] = Protocol::nanos($duplicateWindowMs);
        }
        $subject = '$JS.API.STREAM.CREATE.' . $name;
        try {
            $created = $this->api($subject, $config);
        } catch (PermissionDeniedException $e) {
            // Falta y no puede crearlo: nadie lo aprovisionó. No es un timeout ni un fallo de red.
            throw new NotProvisionedException('stream', $name, null, $this->service, $e->subject, null, $e);
        }
        if (self::errorCode($created) !== null) {
            throw new \RuntimeException("no se pudo crear el stream \"{$name}\": " . self::errorText($created));
        }
    }

    public function createDurableConsumer(string $stream, string $durable, array $config): array
    {
        // Se pregunta ANTES de crear: en producción lo aprovisiona `flux provision`, y entonces el SDK no necesita permiso de creación.
        // Intentar crear primero provocaba una denegación espuria en cada arranque.
        $info = $this->api('$JS.API.CONSUMER.INFO.' . $stream . '.' . $durable);
        $code = self::errorCode($info);
        if ($code === null) {
            return $info['config'] ?? [];
        }
        if ($code !== self::CONSUMER_NOT_FOUND) {
            throw new \RuntimeException("la API de JetStream rechazó consultar el consumidor \"{$durable}\" de \"{$stream}\": " . self::errorText($info));
        }

        // El filtro va EN EL SUBJECT: es la única forma de que una ACL vea el filtro (06-security.md §3.4).
        $filter = (string) ($config['filter_subject'] ?? '');
        $subject = '$JS.API.CONSUMER.CREATE.' . $stream . '.' . $durable . ($filter !== '' ? '.' . $filter : '');
        try {
            $created = $this->api($subject, ['stream_name' => $stream, 'config' => $config, 'action' => 'create']);
        } catch (PermissionDeniedException $e) {
            // La pareja medida: crear → denegado Y consultar → no existe. Solo entonces es «falta y no puedo crearlo».
            throw new NotProvisionedException('consumer', $durable, $stream, $this->service, $e->subject, null, $e);
        }
        if (self::errorCode($created) !== null) {
            // Otra instancia del servicio pudo crearlo entre la pregunta y la creación.
            $again = $this->api('$JS.API.CONSUMER.INFO.' . $stream . '.' . $durable);
            if (self::errorCode($again) === null) {
                return $again['config'] ?? [];
            }
            throw new \RuntimeException("no se pudo crear el consumidor \"{$durable}\" de \"{$stream}\": " . self::errorText($created));
        }

        return $created['config'] ?? [];
    }

    public function consumerPending(string $stream, string $durable): ?int
    {
        // Es telemetría: un consumidor que aún no existe, o que acaba de borrarse, es una respuesta legítima y no un fallo.
        $info = $this->api('$JS.API.CONSUMER.INFO.' . $stream . '.' . $durable);

        return isset($info['num_pending']) && is_int($info['num_pending']) ? $info['num_pending'] : null;
    }

    // ─── El pull ──────────────────────────────────────────────────────────────

    /**
     * Comprueba que el servicio PUEDE pedir mensajes de su consumidor (06-security.md §3.7, **DEBE**). Un pull denegado no falla: el
     * broker descarta la petición y avisa aparte, y el consumidor queda inerte y parece sano.
     *
     * No se espera «un poco» y se mira (una carrera): se publica sobre **el mismo subject** una petición con un cuerpo truncado —`{`—,
     * que el servidor contesta `400 Bad Request` sin apuntarla como pendiente y sin sacar ningún mensaje del consumidor, y se hace un
     * PING/PONG. El protocolo es ordenado: si la ACL la denegó, el `-ERR` llega **antes** que el PONG. Determinista, sin temporizador.
     *
     * @throws PermissionDeniedException
     */
    public function assertPullAllowed(string $stream, string $durable): void
    {
        $subject = '$JS.API.CONSUMER.MSG.NEXT.' . $stream . '.' . $durable;
        $mark = $this->conn->denialMark();
        $token = $this->conn->openReply();
        try {
            $this->conn->publish($subject, '{', [], $this->conn->replySubject($token));
            $this->conn->flush();
            $denied = $this->conn->denialSince($mark, 'publish', $subject);
            if ($denied !== null) {
                $this->warnOnce($denied);
                throw new PermissionDeniedException(
                    $this->service,
                    'publish',
                    $subject,
                    "Es pedir mensajes del consumidor \"{$durable}\" del stream \"{$stream}\": sin ese permiso el consumidor se quedaría inerte, "
                    . 'sin recibir nada y sin ningún error (06-security.md §3.7).',
                );
            }
        } finally {
            $this->conn->closeReply($token);
        }
    }

    /**
     * Pide hasta `$batch` mensajes y espera como mucho `$timeoutMs`. Una lista vacía no es un error: un consumidor ocioso es el estado
     * normal. `expires` se fija por debajo de la espera para que sea **el servidor** quien termine el pull (`408`), y no quede una petición
     * viva entregando después a un buzón que ya nadie lee.
     */
    public function fetch(string $stream, string $durable, int $batch, int $timeoutMs): array
    {
        $subject = '$JS.API.CONSUMER.MSG.NEXT.' . $stream . '.' . $durable;
        $expiresMs = max(100, $timeoutMs - 150);
        $mark = $this->conn->denialMark();
        // Un inbox PROPIO del pull (cinco tokens): las entregas llegan con el subject original del evento, no con el del inbox.
        $inbox = $this->conn->pullInbox();
        $sid = $this->conn->subscribe($inbox);
        $messages = [];
        try {
            $this->conn->publish(
                $subject,
                json_encode(['batch' => $batch, 'expires' => Protocol::nanos($expiresMs)], JSON_THROW_ON_ERROR),
                [],
                $inbox,
            );
            $deadline = microtime(true) + ($expiresMs + 400) / 1000;
            while (count($messages) < $batch) {
                $m = $this->conn->nextMessage($sid, max(1, (int) (($deadline - microtime(true)) * 1000)));
                if ($m === null) {
                    break; // el servidor no terminó el pull a tiempo: se deja
                }
                if ($m->status !== null) {
                    // 404 «no hay mensajes», 408 «el pull expiró», 409 «consumidor borrado / límite», 400 «petición mal formada».
                    if ($m->status === 409) {
                        throw new \RuntimeException("el broker cerró el pull del consumidor \"{$durable}\" ({$m->status} {$m->statusText})");
                    }
                    break;
                }
                $messages[] = new RawMessage($m->subject, $m->payload, $m->reply, $m->headers);
            }
            $denied = $this->conn->denialSince($mark, 'publish', $subject);
            if ($denied !== null && $messages === []) {
                $this->warnOnce($denied);
                throw new PermissionDeniedException($this->service, 'publish', $subject);
            }
        } finally {
            $this->conn->unsubscribe($sid);
            $this->reportDenials();
        }

        return $messages;
    }

    // ─── Publicar ─────────────────────────────────────────────────────────────

    /**
     * Publica por JetStream y espera el acuse. Un `publish` de **core** a un subject que ningún stream captura no da error y el evento
     * se evapora (02-naming.md §1.1): por eso se espera el acuse del stream, la única red de seguridad contra una errata de subject.
     *
     * @return array<string,mixed> el acuse (`stream`, `seq`, `duplicate`) o `{ error: … }`
     */
    public function publishAck(string $subject, string $payload, array $headers = [], int $timeoutMs = self::API_TIMEOUT_MS): array
    {
        try {
            $reply = $this->conn->request($subject, $payload, $timeoutMs, $headers);
        } catch (NoRespondersException $e) {
            throw new \RuntimeException(
                "JetStream no acusó recibo de la publicación en \"{$subject}\". Ningún stream captura ese subject. Revisa las mayúsculas "
                . '(NATS es case-sensitive) y que el stream del dominio exista (lo aprovisiona `flux provision`).',
                0,
                $e,
            );
        }
        $decoded = json_decode($reply->payload, true);

        return is_array($decoded) ? $decoded : ['error' => ['description' => 'el acuse no es JSON']];
    }

    public function publish(string $subject, string $payload, array $headers = []): void
    {
        $ack = $this->publishAck($subject, $payload, $headers);
        if (isset($ack['error'])) {
            throw new \RuntimeException(
                "JetStream rechazó la publicación en \"{$subject}\": " . self::errorText($ack) . '. Si dice «no stream», ningún stream captura '
                . 'ese subject: revisa las mayúsculas (NATS es case-sensitive) y que el stream del dominio exista.'
            );
        }
    }

    /**
     * El acuse (`ack`, `nak`, `term`, WIP) es un mensaje efímero al `$JS.ACK…` del mensaje: va por core, sin stream. Es una publicación
     * sin respuesta: si la ACL la deniega no hay excepción ni timeout, el evento se procesa y se **reentrega cada `ack_wait`, para siempre**
     * (A-19). La única salida es el log, que se escribe en cuanto el broker avisa (`reportDenials`).
     */
    public function respond(string $replyTo, string $payload): void
    {
        $this->conn->publish($replyTo, $payload);
        $this->reportDenials();
    }

    // ─── Las denegaciones que no son un error ─────────────────────────────────

    /** Registra, UNA vez por subject, lo que el broker avisó que denegó y que ninguna operación esperaba (un ack, un pull). */
    public function reportDenials(): void
    {
        $this->conn->pump(0);
        foreach ($this->conn->takeDenials() as $d) {
            $this->warnOnce($d);
        }
    }

    private function warnOnce(Denial $d): void
    {
        $subject = self::normalizeDeniedSubject($d->subject);
        if (isset($this->warned[$subject . '|' . $d->operation])) {
            return;
        }
        $this->warned[$subject . '|' . $d->operation] = true;
        $effect = match (true) {
            str_starts_with($subject, '$JS.ACK.') => ' Sin permiso de ack, cada evento se procesa y se REENTREGA cada ack_wait, sin fin y sin error.',
            str_starts_with($subject, '$JS.API.CONSUMER.MSG.NEXT.') => ' Sin permiso de pull, el consumidor no recibe nada y parece sano.',
            str_starts_with($subject, '_INBOX.') && $d->operation === 'publish' => ' Es la RESPUESTA a una petición: sin `allow_responses` en su ACL el dueño no '
                . 'puede contestar, y quien llama solo verá un timeout (RPC_TIMEOUT). Acótalo: `{ max: 1, expires: 30s }` (11-request-reply.md §7.2).',
            default => '',
        };
        $this->logger?->error(
            '[flux] el broker denegó ' . ($d->operation === 'publish' ? 'publicar en' : 'suscribirse a') . " \"{$subject}\" a \"{$this->service}\": "
            . "falta ese permiso en su ACL.{$effect} (06-security.md §3)"
        );
    }

    /**
     * Normaliza el subject de un ack (lleva secuencia y marca de tiempo) a `…<durable>.>`, y el de una respuesta a una petición
     * (`_INBOX.<servicio>.<nuid>.<token>`, distinto en cada petición) a `_INBOX.<servicio>.>`: sin ello, un dueño sin `allow_responses`
     * registraría una línea por cada respuesta denegada, y el aviso de «una vez por subject» no avisaría de nada.
     */
    public static function normalizeDeniedSubject(string $subject): string
    {
        if (str_starts_with($subject, '$JS.ACK.')) {
            return implode('.', array_slice(explode('.', $subject), 0, 4)) . '.>';
        }
        if (str_starts_with($subject, '_INBOX.') && count(explode('.', $subject)) > 3) {
            return implode('.', array_slice(explode('.', $subject), 0, 2)) . '.>';
        }

        return $subject;
    }
}
