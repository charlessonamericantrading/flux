<?php

declare(strict_types=1);

namespace Flux;

use Flux\Transport\Ack;
use Flux\Transport\NatsTransport;
use Flux\Transport\RawMessage;

/**
 * Cliente de flux. Nivel de conformidad: **L2**.
 * Contrato normativo: specification/00-protocol.md §5
 *
 * Regla de diseño: este fichero NO expone ningún tipo de NATS en su API pública. Si lo
 * hiciera, sustituir el broker dejaría de ser un cambio de capa 0-1 y pasaría a tocar las
 * aplicaciones — que es exactamente lo que flux existe para evitar.
 *
 * ⚠️ **Modelo de ejecución.** A diferencia de Node, Python o Go, PHP no tiene un bucle de
 * eventos en el que dejar corriendo un consumidor. `subscribe()` **registra** el handler y
 * crea el durable consumer, pero no bloquea; el consumo lo conduce `run()`, que hay que
 * llamar desde un proceso CLI de larga vida (un worker), nunca desde un handler web.
 * `publish()`, en cambio, es directamente utilizable desde cualquier petición. Ver la
 * sección "El modelo de ejecución de PHP" del README.
 */
final class FluxBus
{
    private readonly Classifier $classifier;
    private readonly string $source;

    /** @var list<Subscription> */
    private array $subscriptions = [];

    /** @var array<string,true> Streams ya comprobados en esta conexión. */
    private array $ensured = [];

    private function __construct(
        private readonly NatsTransport $transport,
        private readonly ConnectOptions $options,
    ) {
        $this->classifier = new Classifier($options->classifier);
        $this->source = Protocol::sourceUri($options->environment, $options->service);
    }

    /**
     * Conecta al bus.
     *
     * El nombre de servicio se valida aquí y no al derivar el primer durable: NATS
     * aceptaría `FacturacionAPI__pedidos_…` sin error, y sin esta comprobación el
     * incumplimiento del patrón de `durableConsumer` solo se descubriría al parsear
     * nombres en una herramienta (protocol.json → `naming.service`).
     *
     * @throws \InvalidArgumentException si el nombre de servicio no es kebab-case
     */
    public static function connect(ConnectOptions $options, NatsTransport $transport): self
    {
        Protocol::assertServiceName($options->service);

        if (!in_array($options->classification, Envelope::VALID_CLASSIFICATIONS, true)) {
            throw new \InvalidArgumentException(
                "classification por defecto inválida: \"{$options->classification}\". "
                . 'Valores permitidos: ' . implode(', ', Envelope::VALID_CLASSIFICATIONS)
            );
        }

        return new self($transport, $options);
    }

    /**
     * Atajo para el caso habitual: conectar sobre un cliente de `basis-company/nats` ya
     * construido.
     *
     * El cliente se recibe hecho —con sus servidores, credenciales y timeouts— en vez de
     * construirlo aquí, por dos motivos: acoplar `ConnectOptions` a la configuración de una
     * librería concreta contradiría la regla de no exponer NATS en la API pública, y las
     * credenciales por servicio (06-security.md §2) suelen venir ya resueltas del gestor de
     * secretos de cada casa.
     *
     * ⚠️ El adaptador está sin verificar contra un broker real. Ver
     * `Flux\Transport\BasisNatsTransport`.
     *
     * @param object $natsClient Instancia de `Basis\Nats\Client`.
     */
    public static function connectToNats(ConnectOptions $options, object $natsClient): self
    {
        return self::connect($options, new Transport\BasisNatsTransport($natsClient));
    }

    // ─── publish ──────────────────────────────────────────────────────────────

    /**
     * Publica un evento. El desarrollador solo escribe `$subject`, `$data` y opcionalmente
     * `$aggregateId` — 01-envelope.md §5. Todo lo demás lo rellena el SDK.
     *
     * ⚠️ `$aggregateId` es el atributo `subject` de CloudEvents (`"ped-123"`), **NO** el
     * subject de NATS. Son dos cosas distintas con el mismo nombre y confundirlas es el
     * error más frecuente al adoptar CloudEvents sobre NATS — 01-envelope.md §2.1.
     *
     * @param array<string,mixed> $data Objeto JSON, claves en camelCase, importes como
     *        entero en la unidad mínima, sin PII y sin `null` para "ausente".
     *
     * @throws InvalidSubjectException|EnvelopeException
     */
    public function publish(
        string $subject,
        array $data,
        ?string $aggregateId = null,
        ?string $tenantId = null,
        ?string $classification = null,
        \DateTimeInterface|string|null $time = null,
        ?string $partitionKey = null,
    ): FluxEvent {
        $parsed = Protocol::parseSubject($subject); // falla temprano y con un mensaje útil
        $this->ensureEventStream($parsed->domain);

        $id = Protocol::uuid7();
        $inherited = EventContext::current();

        $event = Envelope::build(
            subject: $subject,
            data: $data,
            id: $id,
            source: $this->source,
            producerversion: $this->options->version,
            // El contexto heredado gana sobre el default de conexión: un evento derivado
            // pertenece al tenant del evento que lo causó, no al del servicio.
            tenantid: $tenantId ?? $inherited?->tenantid ?? $this->options->tenantId,
            dataclassification: $classification ?? $this->options->classification,
            dataschema: $this->schemaFor($subject),
            // Si el evento no nace de otro, `correlationid` se inicializa con su propio
            // `id` — 01-envelope.md §3.1.
            correlationid: $inherited?->correlationid ?? $id,
            time: $time,
            aggregateId: $aggregateId,
            causationid: $inherited?->causationid,
            partitionkey: $partitionKey,
            traceparent: $inherited?->traceparent ?? EventContext::activeTraceparent(),
            tracestate: $inherited?->tracestate,
        );

        // La cabecera `Nats-Msg-Id` deduplica reintentos de PUBLICACIÓN dentro de
        // `duplicate_window`. NO deduplica reentregas de consumo — 03-delivery.md §3. Es
        // el malentendido más común del protocolo y nunca sustituye a la idempotencia del
        // consumidor.
        $this->transport->publish(
            $subject,
            Envelope::serialize($event),
            [Protocol::MSG_ID_HEADER => $event->id],
        );

        return $event;
    }

    private function schemaFor(string $subject): string
    {
        if (isset($this->options->schemas[$subject])) {
            return $this->options->schemas[$subject];
        }

        if ($this->options->schemaBaseUrl === null) {
            throw new EnvelopeException(
                "no hay dataschema para \"{$subject}\". Declara schemas[\"{$subject}\"] o "
                . 'schemaBaseUrl en ConnectOptions.'
            );
        }

        $p = Protocol::parseSubject($subject);

        // Sin registro exacto solo se puede asumir el `.0.0` del mayor. El dataschema NO se
        // resuelve por red en publish(): está en la ruta caliente, y una caché con TTL abre
        // una ventana en la que dos servicios validan contra versiones distintas. Un SDK L3
        // resuelve el minor real desde el bundle desplegado con el servicio.
        return rtrim($this->options->schemaBaseUrl, '/')
            . "/{$p->domain}/{$p->aggregate}/{$p->event}/{$p->major}.0.0.json";
    }

    // ─── subscribe ────────────────────────────────────────────────────────────

    /**
     * Registra un handler y crea (o valida) su durable consumer.
     *
     * **No bloquea.** Llama a `run()` después para procesar mensajes. Ver la nota sobre el
     * modelo de ejecución en la cabecera de esta clase.
     *
     * El durable se deriva del servicio y del subject (02-naming.md §4) salvo que se dé uno
     * explícito. Devolver del handler = ack; lanzar = clasificar y decidir entre nak, DLQ o
     * alerta (04-errors.md).
     *
     * @param callable(FluxEvent,HandlerContext):void $handler
     *
     * @throws InvalidSubjectException|ConsumerConfigMismatchException
     */
    public function subscribe(
        string $subject,
        callable $handler,
        ?string $durable = null,
        ?int $maxAckPending = null,
        ?string $tenantId = null,
    ): Subscription {
        $parsed = Protocol::parseSubject($subject);
        $this->ensureEventStream($parsed->domain);
        $this->ensureDlqStream($parsed->domain);

        $stream = Protocol::streamName($parsed->domain);
        $durable ??= Protocol::durableName($this->options->service, $subject);

        $requested = [
            'durable_name' => $durable,
            'filter_subject' => $subject,
            'ack_policy' => Protocol::ACK_POLICY,
            // ⚠️ ack_wait DEBE ser backoff[0]: JetStream lo sobrescribe con ese valor sin
            // devolver error. Declararlos iguales hace que la config solicitada y la
            // efectiva coincidan — 03-delivery.md §2.1.
            'ack_wait' => Protocol::nanos(Protocol::ACK_WAIT_MS),
            'max_deliver' => Protocol::MAX_DELIVER,
            'backoff' => array_map(Protocol::nanos(...), Protocol::BACKOFF_MS),
            'max_ack_pending' => $maxAckPending ?? Protocol::MAX_ACK_PENDING,
            'deliver_policy' => Protocol::DELIVER_POLICY,
            'replay_policy' => Protocol::REPLAY_POLICY,
        ];

        $effective = $this->transport->createDurableConsumer($stream, $durable, $requested);
        $this->assertConfigHonored($durable, $requested, $effective);

        $subscription = new Subscription(
            subject: $subject,
            durable: $durable,
            stream: $stream,
            // `Closure::fromCallable` y no `$handler(...)`: acepta también un
            // `[$objeto, 'metodo']` o un `'funcion'`, que es como se escriben los handlers
            // en la mayoría de aplicaciones PHP con inyección de dependencias.
            handler: \Closure::fromCallable($handler),
            tenantId: $tenantId,
        );

        $this->subscriptions[] = $subscription;

        return $subscription;
    }

    /**
     * Requisito L2: verificar que el servidor aplicó lo que se le pidió. Es la única
     * defensa contra sobrescrituras silenciosas — 03-delivery.md §2.1.
     *
     * @param array<string,mixed> $requested
     * @param array<string,mixed> $effective
     *
     * @throws ConsumerConfigMismatchException
     */
    private function assertConfigHonored(string $durable, array $requested, array $effective): void
    {
        /** @var list<array{field: string, requested: mixed, effective: mixed}> $differences */
        $differences = [];

        foreach (['ack_wait', 'max_deliver', 'max_ack_pending', 'ack_policy'] as $field) {
            // Un campo ausente en la respuesta se ignora: significa "el servidor no lo
            // reporta", no "lo cambió". Tratar la ausencia como desajuste haría fallar el
            // arranque de todo servicio contra una versión de NATS ligeramente distinta.
            if (!array_key_exists($field, $effective)) {
                continue;
            }
            if ($effective[$field] != $requested[$field]) { // phpcs:ignore
                $differences[] = [
                    'field' => $field,
                    'requested' => $requested[$field],
                    'effective' => $effective[$field],
                ];
            }
        }

        if (array_key_exists('backoff', $effective)) {
            $effectiveBackoff = is_array($effective['backoff']) ? array_values($effective['backoff']) : [];
            if ($effectiveBackoff !== array_values($requested['backoff'])) {
                $differences[] = [
                    'field' => 'backoff',
                    'requested' => $requested['backoff'],
                    'effective' => $effective['backoff'],
                ];
            }
        }

        if ($differences !== []) {
            throw new ConsumerConfigMismatchException($durable, $differences);
        }
    }

    // ─── bucle de consumo ─────────────────────────────────────────────────────

    /**
     * Procesa mensajes hasta agotar alguno de los límites. Devuelve cuántos procesó.
     *
     * Está pensado para el `main()` de un worker CLI:
     *
     * ```php
     * $bus->subscribe('pedidos.pedido.v1.creado', $handler);
     * $bus->run();   // bloquea hasta SIGTERM
     * ```
     *
     * Los tres límites existen porque un worker de PHP tiene una vida útil finita en la
     * práctica —fugas de memoria de extensiones, despliegues, `opcache`— y reiniciarlo
     * cada N mensajes o cada N segundos es la forma normal de operarlo. Un durable consumer
     * retoma exactamente donde lo dejó.
     *
     * @param callable():bool|null $shouldStop Se consulta entre mensajes. Aquí es donde se
     *        engancha el apagado limpio (`pcntl_signal` + una bandera).
     * @param bool $stopWhenIdle Termina en cuanto una vuelta completa no devuelve nada.
     *        Es lo que quiere un worker de tipo cron ("vacía la cola y sal") y lo que
     *        quieren los tests; un worker de larga vida lo deja en `false`.
     */
    public function run(
        ?int $maxMessages = null,
        ?float $maxSeconds = null,
        ?callable $shouldStop = null,
        int $idleTimeoutMs = 5000,
        bool $stopWhenIdle = false,
    ): int {
        $processed = 0;
        $deadline = $maxSeconds !== null ? microtime(true) + $maxSeconds : null;

        while (true) {
            if ($maxMessages !== null && $processed >= $maxMessages) {
                return $processed;
            }
            if ($deadline !== null && microtime(true) >= $deadline) {
                return $processed;
            }
            if ($shouldStop !== null && $shouldStop()) {
                return $processed;
            }

            $active = array_values(array_filter(
                $this->subscriptions,
                static fn (Subscription $s): bool => $s->active,
            ));
            if ($active === []) {
                return $processed;
            }

            $gotSomething = false;

            foreach ($active as $subscription) {
                try {
                    // batch = 1 no es una limitación: es la consecuencia de procesar los
                    // mensajes en serie. Con batch = N, los N-1 que esperan turno gastan su
                    // `ack_wait` sin que nadie emita WIP por ellos y JetStream los reentrega
                    // mientras el primero sigue en el handler — justo la ejecución
                    // concurrente que 03-delivery.md §2.1 obliga a evitar. En PHP esto es
                    // además ineludible: no hay forma de emitir WIP en paralelo.
                    $messages = $this->transport->fetch(
                        $subscription->stream,
                        $subscription->durable,
                        1,
                        $idleTimeoutMs,
                    );
                } catch (\Throwable $e) {
                    // Fallo transitorio del broker o de la conexión. No mata el bucle.
                    $this->log('warning', "[flux] fallo al recibir de {$subscription->subject}: "
                        . $e->getMessage());
                    continue;
                }

                foreach ($messages as $message) {
                    $gotSomething = true;
                    try {
                        $this->dispatch($subscription, $message);
                    } catch (\Throwable $e) {
                        // Un fallo del propio DESPACHO —no del handler: eso ya está
                        // clasificado y resuelto dentro de dispatch()— no puede matar el
                        // bucle. Un consumidor muerto en silencio parece vivo en el
                        // healthcheck y no consume nada. Sin ack ni term: el mensaje se
                        // reentrega, que es correcto.
                        $this->log('error', "[flux] fallo despachando {$subscription->subject}: "
                            . $e->getMessage());
                    }
                    $processed++;
                }
            }

            if (!$gotSomething && $stopWhenIdle) {
                return $processed;
            }
        }
    }

    private function dispatch(Subscription $subscription, RawMessage $message): void
    {
        $subject = $subscription->subject;
        $attempt = Ack::deliveryAttempt($message->replyTo);
        $maxAttempts = Protocol::MAX_DELIVER;

        // POISON se detecta ANTES del handler: el mensaje no es interpretable, así que el
        // handler nunca llega a verlo — 04-errors.md §1.3.
        try {
            $event = Envelope::parse($message->payload);
        } catch (\Throwable $e) {
            $poison = $e instanceof PoisonError
                ? $e
                : new PoisonError($e->getMessage(), 'MALFORMED_MESSAGE', $e);

            $this->notifyPoison($subject, $poison, $message->payload);
            $this->log('error', "[flux] POISON en {$subject}: " . $poison->getMessage());

            // `term()` SOLO si quedó constancia: un term sin registro en la DLQ borraría la
            // única prueba de que un productor está publicando basura.
            if ($this->sendRawToDlq($subject, $message->payload, $subscription->durable, $poison)) {
                $this->respond($message, Ack::TERM);
            }

            return;
        }

        if ($subscription->tenantId !== null && $event->tenantid !== $subscription->tenantId) {
            $this->respond($message, Ack::ACK); // no es para nosotros
            return;
        }

        $context = new HandlerContext(
            attempt: $attempt,
            maxAttempts: $maxAttempts,
            subject: $subject,
            sendWorkInProgress: function () use ($message): void {
                $this->respond($message, Ack::WIP);
            },
        );

        try {
            EventContext::run(
                EventContext::fromEvent($event),
                fn () => ($subscription->handler)($event, $context),
            );
            $this->respond($message, Ack::ACK);

            return;
        } catch (\Throwable $e) {
            $this->handleFailure($subscription, $message, $event, $e, $attempt, $maxAttempts);
        }
    }

    private function handleFailure(
        Subscription $subscription,
        RawMessage $message,
        FluxEvent $event,
        \Throwable $error,
        int $attempt,
        int $maxAttempts,
    ): void {
        $classification = $this->classifier->classify($error);
        $detail = $error->getMessage() !== '' ? $error->getMessage() : $error::class;

        // El presupuesto efectivo es el menor entre el del consumidor y el que la
        // clasificación imponga para ESTE error. Así un error desconocido agota su
        // presupuesto acotado (2 entregas, ~30 s) sin recortar los 6 intentos de un
        // ECONNRESET o un HTTP 503 reconocidos — 04-errors.md §2.1.
        $budget = min($maxAttempts, $classification->maxAttempts ?? $maxAttempts);

        if ($classification->errorClass === ErrorClass::Retryable && $attempt < $budget) {
            $delayMs = $classification->retryAfterMs ?? $this->backoffFor($attempt);

            $this->log('warning', sprintf(
                '[flux] RETRYABLE %s en %s (intento %d/%d), reintento en %d ms',
                $classification->code,
                $subscription->subject,
                $attempt,
                $budget,
                (int) $delayMs,
            ));

            // Retraso explícito según el backoff canónico en vez de dejar expirar
            // `ack_wait`: así no se retiene la ranura de `max_ack_pending` 30 s de más.
            $this->respond($message, Ack::nakWithDelay($delayMs));

            return;
        }

        // La clase de error ES la razón de DLQ: el valor del enum se escribe tal cual en
        // `dlqreason`. Un RETRYABLE que llega hasta aquí es uno que agotó su presupuesto, y
        // registrarlo como `retryable` es correcto: dice qué se intentó, no qué se decidió.
        $reason = $classification->errorClass;

        try {
            $this->sendToDlq(
                $subscription->subject,
                $event,
                $subscription->durable,
                $attempt,
                $reason,
                "{$classification->code}: {$detail}",
            );
        } catch (\Throwable $dlqError) {
            // ⚠️ Si la DLQ no admite el evento, **NO se hace term**. Un term sin registro
            // en la DLQ borra el evento sin dejar rastro. Sin ack ni term, JetStream lo
            // reentrega y el fallo sigue siendo visible. (Bug real corregido en el SDK de
            // Node, replicado aquí a propósito.)
            $this->log('error', sprintf(
                '[flux] no se pudo escribir en la DLQ de %s (%s); el evento %s se reentregará: %s',
                $subscription->subject,
                $reason->value,
                $event->id,
                $dlqError->getMessage(),
            ));

            return;
        }

        $this->notifyDlq($subscription->subject, $event, $classification);
        $this->log('error', sprintf(
            '[flux] DLQ (%s) %s en %s tras %d intento(s): %s',
            $reason->value,
            $classification->code,
            $subscription->subject,
            $attempt,
            $detail,
        ));

        $this->respond($message, Ack::TERM);
    }

    /** Backoff canónico para el intento `$attempt` (1-based). */
    private function backoffFor(int $attempt): float
    {
        $backoff = Protocol::BACKOFF_MS;

        return (float) $backoff[min($attempt - 1, count($backoff) - 1)];
    }

    private function respond(RawMessage $message, string $token): void
    {
        if ($message->replyTo === null) {
            return;
        }

        $this->transport->respond($message->replyTo, $token);
    }

    // ─── DLQ ──────────────────────────────────────────────────────────────────

    private function sendToDlq(
        string $subject,
        FluxEvent $event,
        string $consumer,
        int $attempts,
        ErrorClass $reason,
        string $error,
    ): void {
        $dead = Envelope::toDlqEvent($event, $reason, $attempts, $consumer, $error);

        $this->transport->publish(
            Protocol::dlqSubject($subject),
            Envelope::serialize($dead),
            // El id lleva el consumidor: dos consumidores distintos del mismo evento tienen
            // derecho a un registro cada uno en la DLQ.
            [Protocol::MSG_ID_HEADER => "dlq-{$event->id}-{$consumer}"],
        );
    }

    /**
     * Un POISON no tiene CloudEvent que preservar —por definición no se pudo parsear—, así
     * que se envuelve el cuerpo crudo en un evento de sistema válido. La alternativa
     * (publicar los bytes tal cual) haría que la propia DLQ contuviese mensajes que ninguna
     * herramienta del ecosistema puede leer.
     *
     * @return bool ¿Quedó constancia? Solo entonces procede el `term()`.
     */
    private function sendRawToDlq(string $subject, string $raw, string $consumer, PoisonError $error): bool
    {
        $id = Protocol::uuid7();
        $now = Envelope::now();

        // Se construye a mano y no con Envelope::build porque no hay subject de dominio del
        // que derivar el `type`. El orden de las claves es el normativo: `data` el último y
        // las `dlq*` antes — 01-envelope.md §6.
        $envelope = [
            'specversion' => Protocol::SPEC_VERSION,
            'id' => $id,
            'source' => $this->source,
            'type' => 'com.flux.system.poison.capturado.v1',
            'time' => $now,
            'datacontenttype' => 'application/json',
            'dataschema' => 'https://schemas.internal/system/poison/capturado/1.0.0.json',
            'correlationid' => $id,
            'tenantid' => 'system',
            'producerversion' => $this->options->version,
            'dataclassification' => 'internal',
            'dlqreason' => ErrorClass::Poison->value,
            'dlqattempts' => 1,
            'dlqconsumer' => $consumer,
            'dlqerror' => substr(($error->errorCode ?? 'POISON') . ': ' . $error->getMessage(), 0, 1024),
            'dlqtime' => $now,
            'data' => [
                'originalSubject' => $subject,
                // Recortado para no superar el límite de 1 MiB del propio mensaje de DLQ.
                'rawBase64' => substr(base64_encode($raw), 0, 700_000),
                'rawBytes' => strlen($raw),
            ],
        ];

        try {
            $this->transport->publish(
                Protocol::dlqSubject($subject),
                json_encode($envelope, Envelope::JSON_FLAGS),
                [Protocol::MSG_ID_HEADER => "poison-{$id}"],
            );

            return true;
        } catch (\Throwable $e) {
            $this->log('error', "[flux] no se pudo escribir el POISON de {$subject} en la DLQ: "
                . $e->getMessage());

            return false;
        }
    }

    // ─── provisión de streams ─────────────────────────────────────────────────

    private function ensureEventStream(string $domain): void
    {
        if (!$this->options->createStreams) {
            return;
        }

        $name = Protocol::streamName($domain);
        if (isset($this->ensured[$name])) {
            return;
        }

        $this->transport->ensureStream(
            $name,
            ["{$domain}.>"],
            Protocol::STREAM_MAX_AGE_MS,
            Protocol::DUPLICATE_WINDOW_MS,
        );
        $this->ensured[$name] = true;
    }

    private function ensureDlqStream(string $domain): void
    {
        if (!$this->options->createStreams) {
            return;
        }

        $name = Protocol::dlqStreamName($domain);
        if (isset($this->ensured[$name])) {
            return;
        }

        // Prefijo `dlq.`, no sufijo: un sufijo encajaría con `<dominio>.>` y el stream
        // principal capturaría sus propios muertos — 02-naming.md §3.1.
        $this->transport->ensureStream($name, ["dlq.{$domain}.>"], Protocol::DLQ_STREAM_MAX_AGE_MS);
        $this->ensured[$name] = true;
    }

    // ─── ciclo de vida ────────────────────────────────────────────────────────

    public function isConnected(): bool
    {
        return $this->transport->isConnected();
    }

    /**
     * Cierra la conexión.
     *
     * Los acks que no llegasen provocarán reentrega, y eso es correcto: es exactamente el
     * caso que cubre la idempotencia del consumidor — 03-delivery.md §6.
     */
    public function close(): void
    {
        foreach ($this->subscriptions as $subscription) {
            $subscription->cancel();
        }

        $this->transport->close();
    }

    // ─── observabilidad ───────────────────────────────────────────────────────

    private function notifyPoison(string $subject, PoisonError $error, string $raw): void
    {
        if ($this->options->onPoison === null) {
            return;
        }

        try {
            ($this->options->onPoison)($subject, $error, $raw);
        } catch (\Throwable $e) {
            // Un callback de alerta roto no puede impedir que el mensaje llegue a la DLQ.
            $this->log('warning', '[flux] el callback onPoison lanzó: ' . $e->getMessage());
        }
    }

    private function notifyDlq(string $subject, FluxEvent $event, Classification $classification): void
    {
        if ($this->options->onDlq === null) {
            return;
        }

        try {
            ($this->options->onDlq)($subject, $event, $classification);
        } catch (\Throwable $e) {
            $this->log('warning', '[flux] el callback onDlq lanzó: ' . $e->getMessage());
        }
    }

    private function log(string $level, string $message): void
    {
        $this->options->logger?->log($level, $message);
    }
}
