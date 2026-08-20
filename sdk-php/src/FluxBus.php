<?php

declare(strict_types=1);

namespace Flux;

use Flux\Metrics\ConnectionState;
use Flux\Metrics\ConsumeOutcome;
use Flux\Metrics\MetricsSink;
use Flux\Metrics\NoMetrics;
use Flux\Metrics\PublishOutcome;
use Flux\Signing\Signer;
use Flux\Signing\Verifier;
use Flux\Transport\Ack;
use Flux\Transport\NatsTransport;
use Flux\Transport\RawMessage;
use Flux\Validation\SchemaNotFoundError;
use Flux\Validation\SchemaValidationError;
use Flux\Validation\Validator;

/**
 * Cliente de flux. Nivel de conformidad: **L3** (la validación de esquema es opt-in;
 * sin ella el comportamiento es exactamente el de L2 y no cuesta nada).
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
 *
 * Consecuencia honesta de ese modelo: el **sondeo de `num_pending`** que exige
 * 08-observability.md §2.3 vive dentro de `run()`, porque PHP no tiene temporizadores en
 * segundo plano. Un proceso que solo publica —una petición web— no sondea nada, y no puede.
 * No es una carencia disimulable: sin worker no hay `flux_consumer_pending`, igual que sin
 * worker tampoco hay consumo.
 */
final class FluxBus
{
    private readonly Classifier $classifier;
    private readonly string $source;
    private readonly MetricsSink $metrics;
    private readonly ?Signer $signer;
    private readonly ?Verifier $verifier;
    private readonly ?Validator $validator;

    /** @var list<Subscription> */
    private array $subscriptions = [];

    /** @var array<string,true> Streams ya comprobados en esta conexión. */
    private array $ensured = [];

    /**
     * Instante del último sondeo de `num_pending`, por durable. Ver `pollConsumerPending()`.
     *
     * @var array<string,float>
     */
    private array $lastPendingPoll = [];

    private function __construct(
        private readonly NatsTransport $transport,
        private readonly ConnectOptions $options,
    ) {
        $this->classifier = new Classifier($options->classifier);
        $this->source = Protocol::sourceUri($options->environment, $options->service);
        $this->metrics = $options->metrics ?? new NoMetrics();

        // El firmante y el verificador se construyen al CONECTAR, no al primer evento: una
        // clave mal configurada es un fallo de arranque. Si se construyeran perezosamente,
        // un servicio con la clave equivocada pasaría el healthcheck y solo fallaría al
        // llegar el primer mensaje — 07-signing.md §7.
        $this->signer = Signer::fromOptions($options->signing);
        $this->verifier = Verifier::fromOptions($options->signing, $options->logger);

        // Los esquemas se compilan UNA vez, aquí, y no por evento: parsear un JSON Schema
        // en la ruta caliente del productor tiraría el throughput. Por el mismo motivo que
        // el firmante, un bundle ausente o una librería que falta es un fallo de ARRANQUE:
        // un servicio con `mode: strict` mal configurado no debe pasar el healthcheck y
        // descubrirlo al publicar el primer evento.
        $this->validator = Validator::fromOptions($options->validation, $options->logger);

        $this->metrics->connectionState(
            $transport->isConnected() ? ConnectionState::Connected : ConnectionState::Disconnected
        );
    }

    /**
     * El destino de métricas configurado. Útil para servir `/metrics` desde el worker.
     *
     * ⚠️ Solo tiene sentido en un proceso de larga vida: bajo FPM cada petición arranca con
     * los contadores a cero. Ver la nota de `Flux\Metrics\InMemoryMetrics`.
     */
    public function metrics(): MetricsSink
    {
        return $this->metrics;
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

        // L3: validar ANTES de publicar, y antes de firmar. Un payload que viola su propio
        // contrato debe fallar AQUÍ, en el servicio que lo generó, en vez de aparecer como
        // un misterio en un consumidor de otro equipo la semana que viene — 00-protocol.md
        // §5. Para entonces el evento malo ya estaría en el stream y no hay forma de
        // retirarlo.
        try {
            $this->validator?->validate($event, $subject);
        } catch (SchemaValidationError|SchemaNotFoundError $e) {
            // `invalid_schema` y no `error`: "el broker rechazó la publicación" y "mi
            // servicio intentó publicar basura" son dos problemas distintos con dos dueños
            // distintos, y un panel que los suma no sirve para ninguno de los dos.
            $this->metrics->eventPublished($subject, PublishOutcome::InvalidSchema);

            throw $e;
        }

        // Firmar es LO ÚLTIMO antes de serializar: la firma cubre el envelope completo, así
        // que cualquier atributo añadido después la invalidaría — 07-signing.md §5.
        $event = $this->signer?->sign($event) ?? $event;

        try {
            // La cabecera `Nats-Msg-Id` deduplica reintentos de PUBLICACIÓN dentro de
            // `duplicate_window`. NO deduplica reentregas de consumo — 03-delivery.md §3.
            // Es el malentendido más común del protocolo y nunca sustituye a la
            // idempotencia del consumidor.
            $this->transport->publish(
                $subject,
                Envelope::serialize($event),
                [Protocol::MSG_ID_HEADER => $event->id],
            );
        } catch (\Throwable $e) {
            $this->metrics->eventPublished($subject, PublishOutcome::Error);

            throw $e;
        }

        $this->metrics->eventPublished($subject, PublishOutcome::Ok);

        return $event;
    }

    private function schemaFor(string $subject): string
    {
        // Lo declarado a mano gana siempre: es la única forma de apuntar a un esquema que no
        // está en el bundle.
        if (isset($this->options->schemas[$subject])) {
            return $this->options->schemas[$subject];
        }

        // El bundle L3 conoce el MINOR real de cada subject. Dentro de un mayor todo es
        // BACKWARD-compatible (05-compatibility.md), así que el MINOR más alto acepta todo
        // lo que aceptan los anteriores y es el que corresponde anunciar.
        $fromBundle = $this->options->validation?->bundle?->uriFor($subject);
        if ($fromBundle !== null) {
            return $fromBundle;
        }

        if ($this->options->schemaBaseUrl === null) {
            throw new EnvelopeException(
                "no hay dataschema para \"{$subject}\". Declara schemas[\"{$subject}\"], "
                . 'schemaBaseUrl, o pasa un bundle en validation.bundle (ConnectOptions).'
            );
        }

        $p = Protocol::parseSubject($subject);

        // Sin bundle ni mapa explícito solo se puede asumir el `.0.0` del mayor: es
        // suficiente para L2 —donde el atributo es informativo— pero no para L3, donde se
        // valida contra él. El dataschema NO se resuelve por red en publish(): está en la
        // ruta caliente, y una caché con TTL abre una ventana en la que dos servicios validan
        // contra versiones distintas del mismo esquema (00-protocol.md §5).
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
     * @param string|null $tenantId Filtro de tenant de ESTA suscripción. Gana sobre el de
     *        la conexión — 09-multitenancy.md §3.
     *
     * @throws InvalidSubjectException|ConsumerConfigMismatchException|TenantIsolationException
     */
    public function subscribe(
        string $subject,
        callable $handler,
        ?string $durable = null,
        ?int $maxAckPending = null,
        ?string $tenantId = null,
    ): Subscription {
        $parsed = Protocol::parseSubject($subject);

        // El filtro efectivo se resuelve ANTES de crear nada: si `strict` no encuentra
        // tenant, no tiene sentido dejar un durable consumer huérfano en el servidor.
        $tenantFilter = self::effectiveTenantFilter($tenantId, $this->options->tenantId);
        if ($this->options->tenantIsolation === TenantIsolation::Strict && $tenantFilter === null) {
            throw new TenantIsolationException(
                $subject,
                self::tenantIsolationReason($this->options->tenantId),
            );
        }

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
            tenantId: $tenantFilter,
        );

        $this->subscriptions[] = $subscription;

        return $subscription;
    }

    /**
     * El filtro de tenant que aplicará una suscripción.
     *
     * El de la suscripción gana sobre el de la conexión. Y **`"system"` NO cuenta como
     * filtro**: es la AUSENCIA de tenant, el valor reservado para los eventos de
     * plataforma, y usarlo como comodín o como default cuando el tenant real se desconoce
     * está prohibido (09-multitenancy.md §5). Si contase, un servicio sin tenant real
     * —y `system` es el default de `ConnectOptions`— pasaría la comprobación de `strict`
     * sin filtrar absolutamente nada, que es exactamente el descuido silencioso que §3
     * obliga a convertir en error.
     */
    private static function effectiveTenantFilter(?string $subscription, ?string $connection): ?string
    {
        $tenant = $subscription ?? $connection;

        return ($tenant === null || $tenant === '' || $tenant === 'system') ? null : $tenant;
    }

    /** Por qué `strict` no encontró filtro, dicho de forma accionable. */
    private static function tenantIsolationReason(string $connectionTenant): string
    {
        if ($connectionTenant === 'system') {
            return 'el tenant de la conexión es "system", que es la AUSENCIA de tenant y no un '
                . 'filtro: se reserva para eventos de plataforma y no debe usarse como comodín '
                . '(09-multitenancy.md §5). Pasa un tenant real en ConnectOptions::$tenantId o '
                . 'en el parámetro $tenantId de subscribe()';
        }

        return 'no hay tenant ni en ConnectOptions::$tenantId ni en el parámetro $tenantId de '
            . 'subscribe()';
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
                // Antes de pedir mensajes, no después: si `fetch` se queda bloqueado el
                // `idleTimeoutMs` entero, el sondeo de la vuelta anterior ya emitió su valor
                // y el gauge no se queda sin actualizar mientras la cola está vacía — que es
                // precisamente cuando esta métrica importa.
                $this->pollConsumerPending($subscription);

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

    /**
     * Pregunta al servidor por el `num_pending` del consumidor, como mucho una vez cada
     * `pendingPollMs` — 08-observability.md §2.3.
     *
     * ## Por qué hace falta, si los metadatos ya lo traen
     *
     * `dispatch()` actualiza el gauge desde los metadatos de cada mensaje entregado, que es
     * gratis y fresquísimo. Pero esa fuente falla **exactamente donde importa**: si el bucle
     * del consumidor muere, dejan de entregarse mensajes, así que el gauge se queda PLANO en
     * su último valor en vez de crecer. En un panel eso es una línea horizontal —
     * indistinguible de "no pasa nada"— mientras la cola crece sin techo y la conexión se
     * sigue reportando sana. Es el bug que apareció de verdad en el SDK de Node
     * (08-observability.md §4).
     *
     * El sondeo no depende de que lleguen mensajes, así que reporta el `num_pending`
     * creciente. Las dos fuentes son obligatorias porque cada una cubre el punto ciego de la
     * otra.
     *
     * ## Y por qué un fallo aquí se traga
     *
     * Esto es telemetría. Un `CONSUMER.INFO` que falla —permisos, un servidor recargado, un
     * consumidor recién borrado— no puede impedir que se procesen eventos: sería cambiar una
     * métrica ausente por un worker parado.
     */
    private function pollConsumerPending(Subscription $subscription): void
    {
        $intervalMs = $this->options->pendingPollMs;

        if ($intervalMs <= 0) {
            return;
        }

        $now = microtime(true);
        $last = $this->lastPendingPoll[$subscription->durable] ?? null;

        if ($last !== null && ($now - $last) * 1000 < $intervalMs) {
            return;
        }

        // El instante se apunta ANTES de preguntar: si el sondeo tarda o falla, el siguiente
        // intento sigue respetando el intervalo en vez de reintentar en cada vuelta del
        // bucle y convertir un servidor con problemas en una tormenta de peticiones.
        $this->lastPendingPoll[$subscription->durable] = $now;

        try {
            $pending = $this->transport->consumerPending(
                $subscription->stream,
                $subscription->durable,
            );
        } catch (\Throwable $e) {
            $this->log('warning', sprintf(
                '[flux] falló el sondeo de num_pending de %s: %s. El consumo continúa.',
                $subscription->durable,
                $e->getMessage(),
            ));

            return;
        }

        // `null` es "no lo sabemos" y NO se emite: publicar un 0 se dibujaría como "no hay
        // nada pendiente", que es la conclusión contraria.
        if ($pending !== null) {
            $this->metrics->consumerPending(
                $subscription->subject,
                $subscription->durable,
                $pending,
            );
        }
    }

    private function dispatch(Subscription $subscription, RawMessage $message): void
    {
        $subject = $subscription->subject;
        $attempt = Ack::deliveryAttempt($message->replyTo);
        $maxAttempts = Protocol::MAX_DELIVER;

        // La única señal que delata a un consumidor cuyo bucle murió: la conexión sigue
        // sana y solo el crecimiento de `pending` lo evidencia — 08-observability.md §4.
        $pending = Ack::pending($message->replyTo);
        if ($pending !== null) {
            $this->metrics->consumerPending($subject, $subscription->durable, $pending);
        }

        // POISON se detecta ANTES del handler: el mensaje no es interpretable, así que el
        // handler nunca llega a verlo — 04-errors.md §1.3.
        try {
            $event = Envelope::parse($message->payload);
        } catch (\Throwable $e) {
            $poison = $e instanceof PoisonError
                ? $e
                : new PoisonError($e->getMessage(), 'MALFORMED_MESSAGE', $e);

            $code = $poison->errorCode ?? 'POISON';
            $this->metrics->eventConsumed($subject, $subscription->durable, ConsumeOutcome::Poison);
            $this->metrics->eventDlq($subject, $subscription->durable, ErrorClass::Poison, $code);
            $this->notifyPoison($subject, $poison, $message->payload);
            $this->log('error', "[flux] POISON en {$subject}: " . $poison->getMessage());

            // `term()` SOLO si quedó constancia: un term sin registro en la DLQ borraría la
            // única prueba de que un productor está publicando basura.
            if ($this->sendRawToDlq($subject, $message->payload, $subscription->durable, $poison)) {
                $this->respond($message, Ack::TERM);
            }

            return;
        }

        // Filtrar ANTES del handler: un evento de otro tenant no es un fallo, no es para
        // nosotros. Se ACKea y se descarta — 09-multitenancy.md §3.2.
        if ($subscription->tenantId !== null && $event->tenantid !== $subscription->tenantId) {
            $this->respond($message, Ack::ACK);
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

        $started = microtime(true);
        $signatureWarning = false;

        try {
            // La firma se comprueba ANTES del handler y antes que cualquier validación de
            // payload: un evento manipulado puede tener un `data` impecable y aun así no
            // ser del productor que dice — 07-signing.md §5. Va dentro del `try` para que
            // un POISON de firma recorra el mismo camino que cualquier otro fallo:
            // clasificar, contar y enrutar a la DLQ.
            //
            // En modo `warn` el evento se acepta, pero el fallo **se cuenta igual**: sin
            // esa métrica, `warn` es inútil para lo único que existe —pilotar la
            // migración—, y la pregunta "¿cuántos eventos siguen sin firma?" habría que
            // buscarla a mano en los logs de siete servicios (§7.1).
            $signatureWarning = $this->verifier?->check($event) !== null;

            // L3 al consumir. Va DESPUÉS de la firma y ANTES del handler: un evento
            // manipulado puede tener un `data` impecable, así que comprobar el esquema
            // primero respondería la pregunta equivocada.
            //
            // El fallo se clasifica PERMANENT —`SchemaValidationError::fluxClass()`— y por
            // eso se lanza dentro de este `try`: recorre el mismo camino que cualquier otro
            // fallo del handler (clasificar, contar, DLQ) en vez de tener una rama propia.
            // El evento parseó como CloudEvent, así que no es POISON; y su `data` no va a
            // cambiar entre entregas, así que reintentarlo son 51 minutos de cola bloqueada
            // para llegar al mismo sitio — 04-errors.md §1.2.
            if ($this->options->validation?->onConsume === true) {
                $this->validator?->validate($event, $subject);
            }

            EventContext::run(
                EventContext::fromEvent($event),
                fn () => ($subscription->handler)($event, $context),
            );
            $this->respond($message, Ack::ACK);

            // El aviso de firma GANA sobre el `ok`: el evento se procesó, sí, pero la
            // pregunta que hay que poder responder antes de pasar a `require` es cuántos
            // eventos siguen sin firmar. Contarlo como `ok` la borraría, y contarlo dos
            // veces rompería `sum by (outcome) == total consumido`.
            $this->metrics->eventConsumed(
                $subject,
                $subscription->durable,
                $signatureWarning ? ConsumeOutcome::InvalidSignature : ConsumeOutcome::Ok,
            );
            $this->metrics->handlerDuration(
                $subject,
                $subscription->durable,
                microtime(true) - $started,
            );

            return;
        } catch (\Throwable $e) {
            $this->handleFailure(
                $subscription,
                $message,
                $event,
                $e,
                $attempt,
                $maxAttempts,
                $started,
                $signatureWarning,
            );
        }
    }

    private function handleFailure(
        Subscription $subscription,
        RawMessage $message,
        FluxEvent $event,
        \Throwable $error,
        int $attempt,
        int $maxAttempts,
        float $started,
        bool $signatureWarning = false,
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

            $this->metrics->eventRetried($subscription->subject, $subscription->durable, $attempt);

            // Retraso explícito según el backoff canónico en vez de dejar expirar
            // `ack_wait`: así no se retiene la ranura de `max_ack_pending` 30 s de más.
            //
            // ⚠️ El delay solo se honra en la PRIMERA reentrega: con `backoff` configurado
            // —y flux lo configura siempre— el servidor manda el array a partir de la
            // segunda y el delay pedido se ignora sin aviso (03-delivery.md §2.2).
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

        // `invalid_signature` no es una clase de error aparte —un fallo de firma es POISON,
        // y su `dlqreason` en la DLQ **sí** es `poison`— pero sí es un `outcome` propio:
        // 07-signing.md §7.2 lo exige porque son dos preguntas distintas y las dos importan.
        // `outcome="poison"` es "un productor publica basura", un bug de serialización;
        // `outcome="invalid_signature"` es "alguien publica eventos que no son suyos", un
        // incidente de seguridad o una migración a medias. Mezclarlas hace que
        // `rate(…{outcome="poison"})` mida cosas distintas según el lenguaje del consumidor,
        // que es justo lo que 08-observability.md §1 existe para impedir.
        // Por la misma razón, `invalid_schema` tampoco es una clase de error aparte: su
        // `dlqreason` en la DLQ es `permanent` y así se registra. Pero como `outcome` sí es
        // propio, porque responde otra pregunta: `permanent` es "mi lógica de negocio
        // rechazó este evento" —una decisión— mientras que `invalid_schema` es "alguien
        // publicó algo que incumple su propio contrato" —un bug de un productor, y en otro
        // servicio—. Sumarlos haría que un productor roto se leyese como reglas de negocio
        // funcionando, que es lo contrario de una alerta.
        $this->metrics->eventConsumed(
            $subscription->subject,
            $subscription->durable,
            match (true) {
                $signatureWarning || self::isSignatureCode($classification->code)
                    => ConsumeOutcome::InvalidSignature,
                self::isSchemaCode($classification->code) => ConsumeOutcome::InvalidSchema,
                default => ConsumeOutcome::fromErrorClass($reason),
            },
        );
        $this->metrics->eventDlq(
            $subscription->subject,
            $subscription->durable,
            $reason,
            $classification->code,
        );
        $this->metrics->handlerDuration(
            $subscription->subject,
            $subscription->durable,
            microtime(true) - $started,
        );

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

    /** Los tres códigos POISON de la firma — 07-signing.md §7. */
    private static function isSignatureCode(string $code): bool
    {
        return in_array(
            $code,
            ['MISSING_SIGNATURE', 'INVALID_SIGNATURE', 'UNKNOWN_SIGNING_KEY'],
            true,
        );
    }

    /** Los códigos de la validación L3 — 00-protocol.md §5. */
    private static function isSchemaCode(string $code): bool
    {
        return in_array($code, [SchemaValidationError::CODE, SchemaNotFoundError::CODE], true);
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

        // Sin esto, un panel no distingue "el servicio está sano" de "el proceso se fue"
        // — 08-observability.md §2.1.
        $this->metrics->connectionState(ConnectionState::Disconnected);
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
