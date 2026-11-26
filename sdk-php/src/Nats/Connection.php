<?php

declare(strict_types=1);

namespace Flux\Nats;

use Flux\ConnectionRejectedException;
use Flux\PermissionDeniedException;

/**
 * El cliente de NATS de flux: el protocolo de texto de NATS sobre un socket de PHP, **sin dependencias** (solo `ext-openssl` para TLS y
 * `ext-sodium` para las nkeys).
 *
 * ## Por qué existe, y por qué no `basis-company/nats`
 *
 * Es lo que midió la Fase 6 contra un `nats-server` real con las ACLs que genera `gen-acl.mjs` (ver «Seguridad» en el README y el
 * Apéndice A del plan v1.1, A-220 a A-229). La librería (1.2.9):
 *
 * - **No puede usar el inbox privado**: `request()` se suscribe a `_REQS.<hex>.*`, escrito a fuego, y la opción `inboxPrefix` no lo
 *   toca. Con `_INBOX.<servicio>.>` como única suscripción permitida, **toda petición falla** —también la confirmación de un `publish`
 *   por JetStream y cada llamada a la API—: `Permissions Violation for Subscription to "_REQS…"`.
 * - **No verifica el host** (`verify_peer_name` fijo a `false`), **no verifica nada sin `tlsCaFile`** (`verify_peer` a `false`) y
 *   **baja a texto plano** si se pide TLS y el servidor no lo ofrece.
 * - No anuncia `no_responders` en CONNECT, así que `RPC_UNAVAILABLE` sería un timeout.
 * - Una denegación llega como `LogicException` en **la siguiente lectura**, sin decir de qué operación.
 *
 * Todo eso se escribe aquí, sobre el protocolo, y se mide. Es síncrono: no hay bucle de eventos ni hilos, así que **solo lee del
 * socket cuando se le pide** (`pump`, `nextMessage`, `request`, `flush`): un proceso que no llama no recibe nada, y el broker
 * lo desconecta si no contesta a sus `PING` (≈ 4 min). Ver «El modelo de ejecución de PHP» en el README.
 */
final class Connection
{
    public const LANG_VERSION = '1.1.0';

    /** @var resource|null */
    private $socket = null;
    private string $buf = '';
    /** @var array<string,mixed> */
    private array $info = [];
    private bool $connected = false;
    private bool $closed = false;
    private int $nextSid = 0;
    /**
     * @var array<int,array{subject:string,queue:?string,messages:\SplQueue<Message>,max:?int,received:int,dropped:int}>
     */
    private array $subs = [];
    /** @var list<Denial> */
    private array $denials = [];
    private int $denialSeq = 0;
    private int $pongs = 0;
    private ?string $lastError = null;
    private ?int $muxSid = null;
    private string $muxPrefix;
    private int $nextToken = 0;
    /** @var array<string,\SplQueue<Message>> */
    private array $replies = [];
    private int $generation = 0;
    private ?string $server = null;
    private bool $tlsActive = false;

    public function __construct(private readonly ConnectionOptions $options)
    {
        $this->muxPrefix = $options->inboxPrefix . '.' . bin2hex(random_bytes(8));
    }

    // ─── Conectar ─────────────────────────────────────────────────────────────

    /**
     * Abre la conexión: **espera** a un broker que aún no ha arrancado y **falla al instante** ante lo que esperar no arregla
     * (03-delivery.md §6.1).
     *
     * @throws ConnectionRejectedException credenciales rechazadas (`auth`) o un TLS que no encaja (`tls`)
     * @throws ConnectionException         el broker no apareció en `waitForBrokerMs`
     */
    public function connect(): void
    {
        $this->closed = false;
        $limit = $this->options->waitForBrokerMs;
        $deadline = $limit !== null ? microtime(true) + $limit / 1000 : null;
        $servers = $this->options->servers;
        $last = null;

        for ($attempt = 1;; $attempt++) {
            shuffle($servers);
            foreach ($servers as $server) {
                try {
                    $this->dial($server);

                    return;
                } catch (ConnectionRejectedException $e) {
                    $this->teardown();
                    throw $e; // reintentar no lo arregla
                } catch (ConnectionException $e) {
                    $this->teardown();
                    $last = $e;
                }
            }
            if ($deadline !== null && microtime(true) >= $deadline) {
                throw new ConnectionException(
                    'no se pudo conectar a ' . implode(', ', $this->options->servers) . " en {$limit} ms: " . ($last?->getMessage() ?? 'sin respuesta')
                    . '. El broker no ha arrancado, o la dirección no es la suya.',
                    0,
                    $last,
                );
            }
            // La misma espera con jitter que la reconexión: mil servicios que esperan a un NATS que arranca no deben llamar a la vez.
            $pauseMs = $this->options->reconnectWaitMs + ($this->options->reconnectJitterMs > 0 ? random_int(0, $this->options->reconnectJitterMs) : 0);
            if ($deadline !== null) {
                $pauseMs = (int) min($pauseMs, max(0, ($deadline - microtime(true)) * 1000)); // no se duerme más allá del plazo
            }
            usleep(1000 * $pauseMs);
        }
    }

    /** @throws ConnectionRejectedException|ConnectionException */
    private function dial(string $server): void
    {
        [$scheme, $host, $port] = self::parseServer($server);
        $this->server = "{$host}:{$port}";
        $errno = 0;
        $errstr = '';
        // Un contexto PROPIO de esta conexión. Sin él PHP usa el contexto POR DEFECTO, global al proceso: `stream_context_set_option` sobre
        // el socket escribiría ahí, y la `cafile` o el certificado de una conexión se heredarían en las siguientes (medido: tras una conexión
        // con `tls.caFile`, otra con `tls: []` verificaba contra esa CA y conectaba).
        $context = stream_context_create();
        $sock = @stream_socket_client(
            "tcp://{$host}:{$port}",
            $errno,
            $errstr,
            $this->options->connectTimeoutMs / 1000,
            STREAM_CLIENT_CONNECT,
            $context,
        );
        if ($sock === false) {
            throw new ConnectionException("no se pudo abrir {$host}:{$port}: " . ($errstr !== '' ? $errstr : "errno {$errno}"));
        }
        stream_set_blocking($sock, false);
        $this->socket = $sock;
        $this->buf = '';
        $this->lastError = null;
        $this->pongs = 0;

        // 1. INFO, en claro: el servidor habla primero.
        $deadline = microtime(true) + $this->options->connectTimeoutMs / 1000;
        $this->info = [];
        while ($this->info === []) {
            $line = $this->readLine($deadline);
            if ($line === null) {
                throw new ConnectionException("{$host}:{$port} aceptó la conexión pero no envió su INFO: ¿es un servidor NATS?");
            }
            if (str_starts_with($line, 'INFO ')) {
                $decoded = json_decode(substr($line, 5), true);
                if (!is_array($decoded)) {
                    throw new ConnectionException("{$host}:{$port} envió un INFO ilegible: ¿es un servidor NATS?");
                }
                $this->info = $decoded;
            }
        }

        // 2. TLS. Se pidió `tls` (o `tls://`) y el servidor no lo ofrece: falla y NO baja a texto plano. Si el servidor lo EXIGE, se
        // hace TLS aunque no se pidiera, y se verifica igual (contra las CA del sistema si no hay `caFile`).
        $serverRequires = ($this->info['tls_required'] ?? false) === true || ($this->info['tls_verify'] ?? false) === true;
        $serverOffers = $serverRequires || ($this->info['tls_available'] ?? false) === true;
        $wantsTls = $this->options->tls !== null || $scheme === 'tls';
        if ($wantsTls && !$serverOffers) {
            throw new ConnectionRejectedException(
                'tls',
                "se pidió TLS y el servidor NATS {$host}:{$port} no lo ofrece. Reintentar no lo arregla, y el SDK no baja a texto plano: "
                . 'habilita TLS en el listener (06-security.md §2) o quita `tls`.',
            );
        }
        $tlsUsed = $wantsTls || $serverRequires;
        $this->tlsActive = false;
        if ($tlsUsed) {
            $this->upgradeTls($sock, $context, $host, $port);
            $this->tlsActive = true;
        }

        // 3. CONNECT + PING: la primera conexión se da por buena cuando el broker contesta PONG. Sin esperarlo, una credencial
        // rechazada pasaría inadvertida hasta la primera operación.
        $connect = [
            'verbose' => false,
            'pedantic' => false,
            'tls_required' => $tlsUsed,
            'name' => $this->options->name,
            'lang' => 'php',
            'version' => self::LANG_VERSION,
            'protocol' => 1,
            // `headers` y `no_responders`: sin ellos `RPC_UNAVAILABLE` es un timeout (11-request-reply.md §4.3, A-57).
            'headers' => true,
            'no_responders' => true,
            'echo' => true,
        ];
        if ($this->options->credentials !== null) {
            $connect += $this->options->credentials->connectFields(is_string($this->info['nonce'] ?? null) ? $this->info['nonce'] : null);
        }
        $this->write('CONNECT ' . json_encode($connect, JSON_UNESCAPED_SLASHES) . "\r\nPING\r\n");

        $deadline = microtime(true) + $this->options->connectTimeoutMs / 1000;
        $before = $this->pongs;
        while ($this->pongs === $before) {
            if ($this->lastError !== null) {
                throw $this->rejection($this->lastError, "{$host}:{$port}");
            }
            if (!$this->fill($deadline)) {
                if ($this->lastError !== null) {
                    throw $this->rejection($this->lastError, "{$host}:{$port}");
                }
                // Con TLS 1.3 el handshake «termina» en el cliente ANTES de que el servidor compruebe el certificado de cliente: si lo
                // exigía (`tls_verify`) y no se presentó ninguno, lo único que se ve es que cierra sin contestar al CONNECT. Es el caso en el
                // que no todos los clientes NATS distinguen el mTLS de un timeout; aquí sí se distingue, y esperar no lo arregla.
                if ($tlsUsed && ($this->info['tls_verify'] ?? false) === true && $this->options->tls?->certFile === null) {
                    throw new ConnectionRejectedException(
                        'tls',
                        "el broker {$host}:{$port} exige certificado de cliente (mTLS) y no se presentó ninguno: cerró la conexión sin contestar. "
                        . 'Reintentar no lo arregla: indica `tls.certFile` y `tls.keyFile`, firmados por una CA que el broker acepte.',
                    );
                }
                throw new ConnectionException("{$host}:{$port} cerró la conexión sin contestar al CONNECT");
            }
            $this->parse();
        }
        if ($this->lastError !== null) {
            throw $this->rejection($this->lastError, "{$host}:{$port}");
        }

        $this->connected = true;
        $this->generation++;
        $this->denials = [];

        // Tras una reconexión: las suscripciones y el buzón único vuelven a abrirse con los mismos sid.
        foreach ($this->subs as $sid => $sub) {
            $this->write('SUB ' . $sub['subject'] . ($sub['queue'] !== null ? ' ' . $sub['queue'] : '') . " {$sid}\r\n");
        }
    }

    /** Un `-ERR` durante el CONNECT: qué es lo que el broker rechaza. */
    private function rejection(string $error, string $where): \Throwable
    {
        $e = strtolower($error);
        if (str_contains($e, 'authorization violation') || str_contains($e, 'authentication')) {
            return new ConnectionRejectedException(
                'auth',
                "el broker {$where} rechazó las credenciales ({$error}). Reintentar no lo arregla: revisa `credentials` y que el usuario "
                . 'exista en la account del servicio.',
            );
        }

        return new ConnectionException("el broker {$where} rechazó la conexión: {$error}");
    }

    /**
     * Sube la conexión a TLS **verificando de verdad**: la cadena contra la CA (la de `tls.caFile` y solo esa, o las del sistema) y el
     * nombre del host contra los SAN del certificado. Cualquier fallo es `CONNECTION_REJECTED` (`tls`): el handshake no dice por qué,
     * así que el mensaje lista las causas candidatas.
     *
     * @param resource $sock
     * @param resource $context el contexto de ESTA conexión (no el global del proceso)
     *
     * @throws ConnectionRejectedException
     */
    private function upgradeTls($sock, $context, string $host, int $port): void
    {
        $tls = $this->options->tls;
        $isIp = filter_var($host, FILTER_VALIDATE_IP) !== false;
        $ctx = [
            'verify_peer' => true,
            'verify_peer_name' => true,
            'allow_self_signed' => false,
            'peer_name' => $host,
            'SNI_enabled' => !$isIp,
            'disable_compression' => true,
        ];
        if (!$isIp) {
            $ctx['SNI_server_name'] = $host;
        }
        if ($tls?->caFile !== null) {
            $ctx['cafile'] = $tls->caFile;
        }
        if ($tls?->certFile !== null && $tls->keyFile !== null) {
            $ctx['local_cert'] = $tls->certFile;
            $ctx['local_pk'] = $tls->keyFile;
        }
        foreach ($ctx as $k => $v) {
            stream_context_set_option($context, 'ssl', $k, $v);
        }

        $messages = [];
        set_error_handler(static function (int $no, string $str) use (&$messages): bool {
            $messages[] = trim((string) preg_replace('/^stream_socket_enable_crypto\(\):\s*/', '', $str));

            return true;
        });
        try {
            stream_set_blocking($sock, true);
            stream_set_timeout($sock, max(1, (int) ceil($this->options->connectTimeoutMs / 1000)));
            $ok = stream_socket_enable_crypto($sock, true, STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT | STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT);
            while (($e = openssl_error_string()) !== false) {
                $messages[] = $e;
            }
        } finally {
            restore_error_handler();
            stream_set_blocking($sock, false);
        }

        if ($ok !== true) {
            $cause = $messages === [] ? 'el handshake TLS falló sin decir por qué' : implode(' · ', array_slice(array_unique($messages), 0, 4));
            throw new ConnectionRejectedException(
                'tls',
                "no se pudo establecer TLS con {$host}:{$port} ({$cause}). Reintentar no lo arregla. Causas candidatas: el certificado del "
                . 'servidor no verifica (indica la CA propia en `tls.caFile`, y no es la CA que lo firmó si ya la indicaste); el host al que '
                . 'te conectas no está entre los SAN del certificado; o el broker exige certificado de cliente (mTLS) y no se presentó '
                . 'ninguno (`tls.certFile` y `tls.keyFile`).',
            );
        }
    }

    /** @return array{0:string,1:string,2:int} */
    private static function parseServer(string $server): array
    {
        $url = str_contains($server, '://') ? $server : "nats://{$server}";
        $p = parse_url($url);
        if ($p === false || !isset($p['host'])) {
            throw new ConnectionException("la dirección del servidor NATS \"{$server}\" no se entiende (se espera nats://host:4222)");
        }

        return [strtolower($p['scheme'] ?? 'nats'), $p['host'], $p['port'] ?? 4222];
    }

    public function isConnected(): bool
    {
        return $this->connected && !$this->closed;
    }

    /** La dirección a la que está conectada, para los mensajes de error. */
    public function server(): ?string
    {
        return $this->server;
    }

    /** @return array<string,mixed> el INFO del servidor (`max_payload`, `headers`, `nonce`…). */
    public function info(): array
    {
        return $this->info;
    }

    public function maxPayload(): int
    {
        $m = $this->info['max_payload'] ?? 1_048_576;

        return is_int($m) ? $m : 1_048_576;
    }

    /** Cuántas veces se ha (re)conectado. Quien guarda estado de la sesión (un pull pendiente) lo compara para saber si se perdió. */
    public function generation(): int
    {
        return $this->generation;
    }

    // ─── Publicar y suscribirse ───────────────────────────────────────────────

    /**
     * @param array<string,string> $headers
     *
     * @throws \LengthException si no cabe en `max_payload` (las cabeceras cuentan)
     */
    public function publish(string $subject, string $payload = '', array $headers = [], ?string $reply = null): void
    {
        $this->ensureConnected();
        $this->write($this->frame($subject, $payload, $headers, $reply));
    }

    /**
     * @param array<string,string> $headers
     */
    private function frame(string $subject, string $payload, array $headers, ?string $reply): string
    {
        $head = '';
        if ($headers !== []) {
            $head = "NATS/1.0\r\n";
            foreach ($headers as $name => $value) {
                if (preg_match('/^[!-9;-~]+\z/', (string) $name) !== 1 || preg_match('/[\r\n]/', $value) === 1) {
                    throw new \InvalidArgumentException('cabecera NATS inválida: el nombre no admite espacios ni `:` y el valor no admite saltos de línea');
                }
                $head .= "{$name}: {$value}\r\n";
            }
            $head .= "\r\n";
        }
        $size = strlen($head) + strlen($payload);
        if ($size > $this->maxPayload()) {
            throw new \LengthException("el mensaje ocupa {$size} bytes (cabeceras incluidas) y el broker admite {$this->maxPayload()}");
        }
        if (preg_match('/[\s]/', $subject) === 1 || $subject === '') {
            throw new \InvalidArgumentException('subject de NATS inválido: no puede estar vacío ni contener espacios');
        }
        $target = $subject . ($reply !== null ? " {$reply}" : '');

        return $head === ''
            ? "PUB {$target} " . strlen($payload) . "\r\n{$payload}\r\n"
            : "HPUB {$target} " . strlen($head) . " {$size}\r\n{$head}{$payload}\r\n";
    }

    /** Se suscribe y devuelve el `sid`. */
    public function subscribe(string $subject, ?string $queue = null): int
    {
        $this->ensureConnected();
        $sid = ++$this->nextSid;
        $this->subs[$sid] = ['subject' => $subject, 'queue' => $queue, 'messages' => new \SplQueue(), 'max' => null, 'received' => 0, 'dropped' => 0];
        $this->write("SUB {$subject}" . ($queue !== null ? " {$queue}" : '') . " {$sid}\r\n");

        return $sid;
    }

    /** Se da de baja de golpe: lo que aún viaja se descarta. Para parar sin perder nada, `drain()`. */
    public function unsubscribe(int $sid): void
    {
        if (isset($this->subs[$sid]) && $this->isConnected()) {
            try {
                $this->write("UNSUB {$sid}\r\n");
            } catch (ConnectionException) {
                // ya está cerrada: no hay nada que dar de baja
            }
        }
        unset($this->subs[$sid]);
    }

    /**
     * Drenaje de UNA suscripción (11-request-reply.md §5.5): `UNSUB` y un PING/PONG que espera a lo que ya viaja. Desde ese instante
     * las peticiones nuevas dan `no responders`, y lo que ya estaba en camino **se entrega**: se lee con `nextMessage` y
     * después `release`. Una baja que descartase lo que aún llega lo perdería (A-62).
     */
    public function drain(int $sid, int $timeoutMs = 2_000): void
    {
        if (!isset($this->subs[$sid])) {
            return;
        }
        try {
            $this->write("UNSUB {$sid}\r\n");
            $this->flush($timeoutMs);
        } catch (ConnectionException) {
            // la conexión se cerró: lo que quedaba en la cola local sigue siendo entregable
        }
    }

    /** Olvida una suscripción ya drenada (cuando su cola local está vacía). */
    public function release(int $sid): void
    {
        unset($this->subs[$sid]);
    }

    public function pendingCount(int $sid): int
    {
        return isset($this->subs[$sid]) ? count($this->subs[$sid]['messages']) : 0;
    }

    /** Lo que la cola local de esa suscripción descartó por estar llena: core NATS no tiene acuse y, llena, descarta sin error. */
    public function droppedCount(int $sid): int
    {
        return $this->subs[$sid]['dropped'] ?? 0;
    }

    /**
     * El siguiente mensaje de una suscripción, esperando como mucho `$timeoutMs`. `null` si no llegó ninguno.
     *
     * @throws ConnectionException si la conexión se cerró
     */
    public function nextMessage(int $sid, int $timeoutMs): ?Message
    {
        $deadline = microtime(true) + $timeoutMs / 1000;
        while (true) {
            if (isset($this->subs[$sid]) && !$this->subs[$sid]['messages']->isEmpty()) {
                return $this->subs[$sid]['messages']->dequeue();
            }
            if (!$this->isConnected()) {
                throw new ConnectionException('la conexión con el broker está cerrada');
            }
            if (microtime(true) >= $deadline) {
                return null;
            }
            $this->pumpUntil($deadline);
        }
    }

    // ─── Petición/respuesta ───────────────────────────────────────────────────

    /**
     * Una petición por core NATS con **el buzón único** de la conexión (`_INBOX.<servicio>.<nuid>.*`): una sola suscripción, la que
     * ya tiene el permiso de la ACL. **No abre otra** que case con el `reply` (A-101: una segunda suscripción al propio inbox hace que
     * el `503` de `no responders` no llegue y se convierta en un timeout).
     *
     * Espera lo primero que ocurra: una respuesta; `no responders` (503); un `-ERR Permissions Violation` que nombra el subject
     * **publicado** —la ACL descartó la petición y el broker no contesta nunca—; o el timeout.
     *
     * @param array<string,string> $headers
     *
     * @throws NoRespondersException|RequestTimeoutException|PermissionDeniedException|ConnectionException
     */
    public function request(string $subject, string $payload, int $timeoutMs, array $headers = []): Message
    {
        $token = $this->openReply();
        try {
            $mark = $this->denialSeq;
            $this->publish($subject, $payload, $headers, "{$this->muxPrefix}.{$token}");

            return $this->awaitReply($token, $subject, $mark, $timeoutMs);
        } finally {
            unset($this->replies[$token]);
        }
    }

    /** Reserva un token del buzón único (y abre la suscripción la primera vez). */
    public function openReply(): string
    {
        $this->ensureConnected();
        $this->ensureMux();
        $token = (string) ++$this->nextToken;
        $this->replies[$token] = new \SplQueue();

        return $token;
    }

    public function replySubject(string $token): string
    {
        return "{$this->muxPrefix}.{$token}";
    }

    /**
     * Un subject de respuesta PROPIO para un pull: `_INBOX.<servicio>.<nuid>.pull.<n>`, de cinco tokens, que **no casa** con el buzón único
     * (`…<nuid>.*`, cuatro) y por tanto no toca A-101. Hace falta porque JetStream entrega los mensajes de un pull **con su subject original**
     * (`pedidos.pedido.v1.creado`) y no con el del inbox: el buzón único los repartiría por el último token del subject, y no lo tienen.
     * Se reparten por suscripción (`sid`).
     */
    public function pullInbox(): string
    {
        return "{$this->muxPrefix}.pull." . ++$this->nextToken;
    }

    public function closeReply(string $token): void
    {
        unset($this->replies[$token]);
    }

    /**
     * Espera una respuesta al token. Compite la respuesta con la denegación del subject `$published` (la que llegó después de `$mark`).
     */
    public function awaitReply(string $token, string $published, int $mark, int $timeoutMs): Message
    {
        $deadline = microtime(true) + $timeoutMs / 1000;
        while (true) {
            $q = $this->replies[$token] ?? null;
            if ($q !== null && !$q->isEmpty()) {
                $m = $q->dequeue();
                if ($m->isNoResponders()) {
                    throw new NoRespondersException($published);
                }

                return $m;
            }
            $denied = $this->denialSince($mark, 'publish', $published);
            if ($denied !== null) {
                throw new PermissionDeniedException(
                    $this->serviceName(),
                    'publish',
                    $denied->subject,
                    null,
                );
            }
            if (!$this->isConnected()) {
                throw new ConnectionException('la conexión con el broker se cerró esperando la respuesta');
            }
            if (microtime(true) >= $deadline) {
                throw new RequestTimeoutException("no llegó respuesta de \"{$published}\" en {$timeoutMs} ms");
            }
            $this->pumpUntil($deadline);
        }
    }

    /**
     * El siguiente mensaje de un token (para un pull, que puede dar varios), o `null` al agotarse `$timeoutMs`.
     */
    public function nextReply(string $token, int $timeoutMs): ?Message
    {
        $deadline = microtime(true) + $timeoutMs / 1000;
        while (true) {
            $q = $this->replies[$token] ?? null;
            if ($q !== null && !$q->isEmpty()) {
                return $q->dequeue();
            }
            if (!$this->isConnected()) {
                throw new ConnectionException('la conexión con el broker se cerró esperando la respuesta');
            }
            if (microtime(true) >= $deadline) {
                return null;
            }
            $this->pumpUntil($deadline);
        }
    }

    /**
     * Abre el buzón único. Una suscripción denegada NO lanza al suscribirse: el aviso llega aparte. El PING/PONG ordena: el `-ERR`
     * llega antes que el PONG, así que al volver ya está registrado. Sin permiso sobre el inbox ninguna respuesta puede llegar, y
     * eso es `PERMISSION_DENIED`, no un timeout (06-security.md §3.7).
     */
    private function ensureMux(): void
    {
        if ($this->muxSid !== null && isset($this->subs[$this->muxSid])) {
            return;
        }
        $mark = $this->denialSeq;
        $subject = "{$this->muxPrefix}.*";
        $this->muxSid = $this->subscribe($subject);
        $this->flush($this->options->connectTimeoutMs);
        $denied = $this->denialSince($mark, 'subscribe', $subject);
        if ($denied !== null) {
            unset($this->subs[$this->muxSid]);
            $this->muxSid = null;
            throw new PermissionDeniedException(
                $this->serviceName(),
                'subscribe',
                $denied->subject,
                'Las respuestas llegan por el inbox privado `' . $this->options->inboxPrefix . '.>`: sin ese permiso de SUSCRIPCIÓN ninguna petición '
                . 'puede recibir su respuesta (06-security.md §3.3). Es lo único que un servicio necesita suscrito, salvo el dueño de una operación.',
            );
        }
    }

    private function serviceName(): string
    {
        // `_INBOX.<servicio>`
        return substr($this->options->inboxPrefix, strlen('_INBOX.'));
    }

    // ─── Leer ─────────────────────────────────────────────────────────────────

    /**
     * PING/PONG: espera a que el broker conteste. El protocolo es **ordenado**, así que cualquier `-ERR` que el broker mandase por algo
     * enviado antes del PING llega antes del PONG: es la forma determinista de enterarse de una denegación sin esperar un tiempo fijo.
     *
     * @throws ConnectionException
     */
    public function flush(int $timeoutMs = 5_000): void
    {
        $this->ensureConnected();
        $before = $this->pongs;
        $this->write("PING\r\n");
        $deadline = microtime(true) + $timeoutMs / 1000;
        while ($this->pongs === $before) {
            if (!$this->isConnected()) {
                throw new ConnectionException('la conexión con el broker se cerró esperando el PONG');
            }
            if (microtime(true) >= $deadline) {
                throw new RequestTimeoutException("el broker no contestó al PING en {$timeoutMs} ms");
            }
            $this->pumpUntil($deadline);
        }
    }

    /** Lee lo que haya, hasta `$timeoutMs`, y lo reparte. Devuelve si leyó algo. */
    public function pump(int $timeoutMs = 0): bool
    {
        if (!$this->isConnected()) {
            return false;
        }

        return $this->pumpUntil(microtime(true) + $timeoutMs / 1000);
    }

    private function pumpUntil(float $deadline): bool
    {
        if (!$this->fill($deadline)) {
            if (!$this->isConnected()) {
                return false;
            }

            return false;
        }
        $this->parse();

        return true;
    }

    /**
     * Las denegaciones que el broker avisó desde `$mark` (el valor de `denialMark()` antes de la operación).
     *
     * @return list<Denial>
     */
    public function denialsSince(int $mark): array
    {
        return array_values(array_filter($this->denials, static fn (Denial $d): bool => $d->sequence >= $mark));
    }

    public function denialMark(): int
    {
        return $this->denialSeq;
    }

    public function denialSince(int $mark, string $operation, string $subject): ?Denial
    {
        foreach ($this->denials as $d) {
            if ($d->sequence >= $mark && $d->operation === $operation && $d->subject === $subject) {
                return $d;
            }
        }

        return null;
    }

    /**
     * Toma (y olvida) las denegaciones **no atendidas**: las que el llamador aún no vio. Lo usa quien necesita registrarlas (un `ack`
     * denegado no tiene otra salida que el log).
     *
     * @return list<Denial>
     */
    public function takeDenials(): array
    {
        $out = $this->denials;
        $this->denials = [];

        return $out;
    }

    // ─── Cierre ───────────────────────────────────────────────────────────────

    public function close(): void
    {
        $this->closed = true;
        $this->teardown();
    }

    private function teardown(): void
    {
        $this->connected = false;
        if (is_resource($this->socket)) {
            @fclose($this->socket);
        }
        $this->socket = null;
        $this->buf = '';
    }

    /** Reabre la conexión (con la misma espera con jitter) y vuelve a suscribirse. */
    public function reconnect(): void
    {
        $this->teardown();
        $this->replies = [];
        $this->connect();
    }

    private function ensureConnected(): void
    {
        if ($this->closed) {
            throw new ConnectionException('la conexión está cerrada');
        }
        if (!$this->connected) {
            $this->reconnect();
        }
    }

    // ─── Interior: bytes ──────────────────────────────────────────────────────

    /** @throws ConnectionException */
    private function write(string $data): void
    {
        if (!is_resource($this->socket)) {
            throw new ConnectionException('la conexión con el broker está cerrada');
        }
        $len = strlen($data);
        $off = 0;
        $deadline = microtime(true) + 10.0;
        while ($off < $len) {
            $n = @fwrite($this->socket, substr($data, $off, 65_536));
            if ($n === false || ($n === 0 && feof($this->socket))) {
                $this->teardown();
                throw new ConnectionException('se perdió la conexión con el broker al escribir');
            }
            if ($n === 0) {
                // Búfer de escritura lleno: se espera a que se pueda escribir.
                $w = [$this->socket];
                $r = $e = null;
                if (microtime(true) >= $deadline) {
                    $this->teardown();
                    throw new ConnectionException('el broker no admite más datos (se agotaron 10 s escribiendo)');
                }
                @stream_select($r, $w, $e, 0, 200_000);
                continue;
            }
            $off += $n;
        }
    }

    /**
     * Espera hasta `$deadline` a que haya algo que leer, lo lee y lo añade al búfer. Devuelve si leyó algo; `false` al agotarse el plazo
     * o si el broker cerró (y entonces la conexión queda caída).
     *
     * Que `stream_select` diga «legible» y `fread` devuelva vacío **no** es un cierre: con TLS 1.3 el servidor manda tras el handshake
     * unos registros (los tickets de sesión) que OpenSSL consume sin entregar datos de aplicación. Se sigue esperando hasta el plazo.
     */
    private function fill(float $deadline): bool
    {
        while (true) {
            if (!is_resource($this->socket)) {
                return false;
            }
            // Se intenta leer ANTES de esperar: con TLS, OpenSSL puede tener ya descifrados bytes que `stream_select` no ve.
            $got = $this->readAvailable();
            if ($got !== '') {
                $this->buf .= $got;

                return true;
            }
            if ($this->socket === null) {
                return false; // el broker cerró
            }
            $remaining = $deadline - microtime(true);
            if ($remaining <= 0) {
                return false;
            }
            // Como mucho 1 s por vuelta (así una señal de parada se atiende sin esperar al plazo entero) y 100 ms con TLS, por lo de arriba.
            $wait = min($remaining, $this->tlsActive ? 0.1 : 1.0);
            $sec = (int) $wait;
            $r = [$this->socket];
            $w = $e = null;
            @stream_select($r, $w, $e, $sec, (int) (($wait - $sec) * 1_000_000));
        }
    }

    private function readAvailable(): string
    {
        if (!is_resource($this->socket)) {
            return '';
        }
        $data = @fread($this->socket, 262_144);
        if ($data === false || ($data === '' && feof($this->socket))) {
            $this->teardown();

            return '';
        }

        return $data;
    }

    /** Una línea (sin el CRLF) del búfer, esperando hasta `$deadline`. Solo para el arranque, antes de que haya suscripciones. */
    private function readLine(float $deadline): ?string
    {
        while (true) {
            $nl = strpos($this->buf, "\r\n");
            if ($nl !== false) {
                $line = substr($this->buf, 0, $nl);
                $this->buf = (string) substr($this->buf, $nl + 2);

                return $line;
            }
            if (!$this->fill($deadline)) {
                return null;
            }
        }
    }

    /** Reparte todos los marcos completos del búfer. */
    private function parse(): void
    {
        $pos = 0;
        $len = strlen($this->buf);
        while ($pos < $len) {
            $nl = strpos($this->buf, "\r\n", $pos);
            if ($nl === false) {
                break;
            }
            $line = substr($this->buf, $pos, $nl - $pos);
            $sp = strpos($line, ' ');
            $op = strtoupper($sp === false ? $line : substr($line, 0, $sp));

            if ($op === 'MSG' || $op === 'HMSG') {
                $args = preg_split('/\s+/', trim($line));
                $hmsg = $op === 'HMSG';
                $n = count($args);
                // MSG <subject> <sid> [reply] <len>          HMSG <subject> <sid> [reply] <hlen> <len>
                $need = $hmsg ? 5 : 4;
                if ($n < $need || $n > $need + 1) {
                    $pos = $nl + 2;
                    continue;
                }
                $reply = $n === $need + 1 ? $args[3] : null;
                $hlen = $hmsg ? (int) $args[$n - 2] : 0;
                $total = (int) $args[$n - 1];
                if ($nl + 2 + $total + 2 > $len) {
                    break; // el mensaje aún no ha llegado entero
                }
                $body = substr($this->buf, $nl + 2, $total);
                $pos = $nl + 2 + $total + 2;
                $headers = [];
                $status = null;
                $statusText = '';
                if ($hlen > 0) {
                    [$status, $statusText, $headers] = self::parseHeaders(substr($body, 0, $hlen));
                    $body = substr($body, $hlen);
                }
                $this->dispatch((int) $args[2], new Message($args[1], $body, $reply, $headers, $status, $statusText));

                continue;
            }

            $pos = $nl + 2;
            switch ($op) {
                case 'PING':
                    try {
                        $this->write("PONG\r\n");
                    } catch (ConnectionException) {
                        // el próximo acceso lo notará
                    }
                    break;
                case 'PONG':
                    $this->pongs++;
                    break;
                case '-ERR':
                    $this->onError(trim(substr($line, 5), " '"));
                    break;
                case 'INFO':
                    $decoded = json_decode(substr($line, 5), true);
                    if (is_array($decoded)) {
                        $this->info = array_replace($this->info, $decoded);
                    }
                    break;
                default:
                    break; // `+OK` y lo que no se conoce
            }
        }
        $this->buf = $pos >= $len ? '' : (string) substr($this->buf, $pos);
    }

    /**
     * @return array{0:?int,1:string,2:array<string,string>}
     */
    private static function parseHeaders(string $block): array
    {
        $lines = explode("\r\n", rtrim($block, "\r\n"));
        $first = array_shift($lines) ?? '';
        $status = null;
        $text = '';
        if (preg_match('/^NATS\/1\.0(?:\s+(\d{3}))?(?:\s+(.*))?$/', $first, $m) === 1) {
            $status = isset($m[1]) && $m[1] !== '' ? (int) $m[1] : null;
            $text = $m[2] ?? '';
        }
        $headers = [];
        foreach ($lines as $l) {
            $c = strpos($l, ':');
            if ($c !== false) {
                $headers[substr($l, 0, $c)] = ltrim(substr($l, $c + 1));
            }
        }

        return [$status, $text, $headers];
    }

    private function dispatch(int $sid, Message $m): void
    {
        if ($sid === $this->muxSid) {
            $token = (string) substr($m->subject, strlen($this->muxPrefix) + 1);
            // Una respuesta tardía (tras el timeout) o ajena: el broker no la descarta (A-61), la ignora quien llama.
            if (isset($this->replies[$token])) {
                $this->replies[$token]->enqueue($m);
            }

            return;
        }
        if (!isset($this->subs[$sid])) {
            return;
        }
        $sub = &$this->subs[$sid];
        if (count($sub['messages']) >= $this->options->maxQueued) {
            // Core NATS no tiene acuse: con la cola llena se descarta, y se dice.
            if ($sub['dropped']++ % 1000 === 0) {
                $this->options->logger?->error(
                    "[flux] la cola local de \"{$sub['subject']}\" está llena ({$this->options->maxQueued}): se descartan mensajes. "
                    . 'El proceso consume más despacio de lo que recibe (11-request-reply.md §13).'
                );
            }

            return;
        }
        $sub['messages']->enqueue($m);
        $sub['received']++;
    }

    private function onError(string $text): void
    {
        if (preg_match('/^Permissions Violation for (Publish|Subscription) to "(.*?)"(?: using queue "(.*)")?$/s', $text, $m) === 1) {
            $this->denials[] = new Denial($m[1] === 'Publish' ? 'publish' : 'subscribe', $m[2], $m[3] ?? null, $this->denialSeq++);
            if (count($this->denials) > 1000) {
                array_shift($this->denials);
            }

            return;
        }
        // El resto de los -ERR cierra la conexión (Authorization Violation, Stale Connection, Maximum Payload Violation…).
        $this->lastError = $text;
    }
}
