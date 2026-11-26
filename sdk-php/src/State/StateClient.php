<?php

declare(strict_types=1);

namespace Flux\State;

use Flux\Classifier;
use Flux\ConnectOptions;
use Flux\ConsumerConfigMismatchException;
use Flux\Envelope;
use Flux\ErrorClass;
use Flux\EventContext;
use Flux\FluxError;
use Flux\FluxEvent;
use Flux\InvalidTenantForStateException;
use Flux\MissingAggregateVersionException;
use Flux\Metrics\ConsumeOutcome;
use Flux\Metrics\MetricsSink;
use Flux\Metrics\StateRpcMetrics;
use Flux\Nats\NoRespondersException;
use Flux\NotProvisionedException;
use Flux\PoisonError;
use Flux\Protocol;
use Flux\RestrictedStateEntryException;
use Flux\Signing\Signer;
use Flux\Signing\Verifier;
use Flux\StateConflictException;
use Flux\StateSubjectMismatchException;
use Flux\TenantIsolation;
use Flux\TenantIsolationException;
use Flux\Transport\Ack;
use Flux\Transport\ConsumerGoneException;
use Flux\Transport\RawMessage;
use Flux\Transport\StateTransport;
use Flux\Validation\SchemaNotFoundError;
use Flux\Validation\SchemaValidationError;
use Flux\Validation\Validator;

/**
 * Estado replicado — la capacidad `state` (specification/10-state.md): `put`, `delete`, `get` y `watch`. El valor ACTUAL de un agregado,
 * que su dueño publica y los demás observan; sustituye a `GET /clientes/123`, a la carga inicial y al polling de datos.
 *
 *     $bus->state->put('clientes.cliente.v1', 'cli-1', ['clienteId' => 'cli-1', 'aggregateVersion' => 7], tenantId: 'acme');
 *     $bus->state->get('clientes.cliente.v1', 'cli-1', tenantId: 'acme');          // StateEntry | null
 *     $bus->state->watch('clientes.cliente.v1', fn (StateEntry $e, $ctx) => …, tenantId: 'acme');
 *
 * ## Qué sirve en un script PHP corriente y qué pide un worker
 *
 * - `put`, `delete` y `get` son **llamadas síncronas puntuales**: sirven en cualquier petición (un `get` de estado sustituye a un
 *   `GET /clientes/123` a otro servicio). Cada una abre —o reutiliza— la conexión del bus.
 * - **`watch` necesita un proceso de larga vida** (un worker CLI, Swoole/RoadRunner…): `watch()` registra y abre el durable, y el consumo lo
 *   conduce `FluxBus::run()`. En hosting compartido, sin worker, el camino es el gateway (`gateway/`, 12-edge.md).
 *
 * Lo que garantiza, y que se ejecuta contra un broker real con los permisos de `gen-acl.mjs`: `put` condicional (`Nats-Expected-Last-Subject-
 * Sequence`, 10071 **y** 10164 como conflicto, paso 1b del acuse perdido), lápidas, los siete códigos de 04-errors.md §6, `watch` con un
 * durable por servicio y tenant, `onSynced`, DLQ de estado **sin el dato**, reapertura del consumo tras un `flux state resync`.
 */
final class StateClient
{
    private const DIRECT_GET_TIMEOUT_MS = 5_000;
    /** Cuánto espera un `watch` entre reintentos de reabrir su durable: 100 ms … 5 s. */
    private const REOPEN_MIN_MS = 100;
    private const REOPEN_MAX_MS = 5_000;

    /** @var list<StateWatch> */
    private array $watches = [];
    /** @var array<string,true> */
    private array $dlqChecked = [];
    private readonly PendingSum $pending;

    public function __construct(
        private readonly ?StateTransport $transport,
        private readonly ConnectOptions $options,
        private readonly string $source,
        private readonly MetricsSink $metrics,
        private readonly Classifier $classifier,
        private readonly ?Validator $validator,
        private readonly ?Signer $signer,
        private readonly ?Verifier $verifier,
    ) {
        $this->pending = new PendingSum();
    }

    private function t(): StateTransport
    {
        return $this->transport ?? throw new \LogicException(
            'el estado replicado necesita un transporte que implemente `StateTransport`: el de por defecto (`SocketTransport`, `FluxBus::connect($opciones)` sin segundo argumento). '
            . 'Con `BasisNatsTransport` o `InMemoryTransport` no hay `DIRECT.GET` ni publicación condicional (A-220).'
        );
    }

    // ─── escribir ─────────────────────────────────────────────────────────────

    /**
     * Escribe el valor actual de una clave. **Condicional**: no escribe una versión que no avanza (`stale`, que NO es un error) y publica con
     * `Nats-Expected-Last-Subject-Sequence`, de modo que dos instancias del dueño no pisan un valor nuevo con uno viejo.
     *
     * `$data` DEBE llevar `aggregateVersion` (entero ≥ 1): lo pone el dueño, no el SDK. Sin `$classification`, `confidential` (no la de la
     * conexión). Sin `$tenantId`: el del evento en curso y, si no, el de `connect()`; sin ninguno, `INVALID_TENANT_FOR_STATE`.
     *
     * @param array<string,mixed> $data
     */
    public function put(
        string $contract,
        string $key,
        array $data,
        ?string $tenantId = null,
        ?string $classification = null,
        \DateTimeInterface|string|null $time = null,
    ): StateWriteResult {
        // §3.6, en este orden y SIN red: el primero que falla gana.
        $c = StateEnvelope::parseContract($contract);
        StateEnvelope::assertKey($key);
        $tenant = $this->tenantFor($tenantId);
        $version = $this->countingInvalid($c->name, static fn (): int => StateEnvelope::aggregateVersionOf($data));
        $classification = StateEnvelope::assertClassification($classification);

        $id = Protocol::uuid7();
        $inherited = EventContext::current();
        $subject = StateEnvelope::subject($c->name, $tenant, $key);
        $event = StateEnvelope::buildEvent(
            contract: $c->name,
            tenantId: $tenant,
            key: $key,
            id: $id,
            source: $this->source,
            time: self::time($time),
            // Un estado derivado dentro de un handler hereda el contexto del evento que lo causó.
            correlationid: $inherited?->correlationid ?? $id,
            producerversion: $this->options->version,
            data: $data,
            classification: $classification,
            dataschema: $this->schemaFor($c->name),
            causationid: $inherited?->causationid,
            traceparent: $inherited?->traceparent ?? EventContext::activeTraceparent(),
            tracestate: $inherited?->tracestate,
        );
        // L3 ANTES de leer ni escribir; reporta TODOS los errores y no copia valores a su mensaje.
        $this->countingInvalid($c->name, fn () => $this->validator?->validate($event, $subject));

        return $this->write($c, $tenant, $key, $subject, $version, $event);
    }

    /**
     * Borra una clave: publica una **lápida** en su subject, que sustituye al valor (el dato personal ya no está en el stream). Es `put` de
     * una lápida: mismo algoritmo, mismas reglas de versión y mismo resultado. Borrar una clave que nunca existió escribe la lápida.
     */
    public function delete(
        string $contract,
        string $key,
        int $aggregateVersion,
        ?string $tenantId = null,
        \DateTimeInterface|string|null $time = null,
    ): StateWriteResult {
        $c = StateEnvelope::parseContract($contract);
        StateEnvelope::assertKey($key);
        $tenant = $this->tenantFor($tenantId);
        $version = $this->countingInvalid($c->name, static fn (): int => StateEnvelope::aggregateVersionOf(['aggregateVersion' => $aggregateVersion]));

        $id = Protocol::uuid7();
        $inherited = EventContext::current();
        $subject = StateEnvelope::subject($c->name, $tenant, $key);
        $event = StateEnvelope::buildTombstone(
            contract: $c->name,
            tenantId: $tenant,
            key: $key,
            id: $id,
            source: $this->source,
            time: self::time($time),
            correlationid: $inherited?->correlationid ?? $id,
            producerversion: $this->options->version,
            aggregateVersion: $version,
            causationid: $inherited?->causationid,
            traceparent: $inherited?->traceparent ?? EventContext::activeTraceparent(),
            tracestate: $inherited?->tracestate,
        );
        // La lápida valida contra su esquema fijo (en `schemas` del bundle, no en el índice `state`).
        $this->countingInvalid($c->name, fn () => $this->validator?->validate($event, $subject));

        return $this->write($c, $tenant, $key, $subject, $version, $event);
    }

    /**
     * El algoritmo de 10-state.md §3.2. El evento se construyó UNA vez: `id`, `time` y bytes son los mismos en todos los intentos. Con un
     * `id` nuevo por intento, el paso 1b no reconocería nunca la propia escritura.
     */
    private function write(StateContract $c, string $tenant, string $key, string $subject, int $version, FluxEvent $toSend): StateWriteResult
    {
        $stream = StateEnvelope::streamName($c->domain);
        $signed = $this->signer?->sign($toSend) ?? $toSend;
        $bytes = Envelope::serialize($signed);
        $id = $signed->id;

        for ($attempt = 1; $attempt <= StateEnvelope::MAX_ATTEMPTS; $attempt++) {
            // 1. Leer el último mensaje de la clave. 404 → S = 0 y no hay versión.
            $actual = $this->readRaw($stream, $subject);
            $s = $actual['seq'] ?? 0;

            // 1b. ¿El mensaje leído es EL MÍO? Entonces ya se aplicó y solo se perdió el acuse. Sin este paso, un acuse perdido convierte una
            // escritura buena en un `stale` con su propio dato: el reenvío da «wrong last sequence» porque lo que cambió la secuencia fue el
            // propio mensaje (A-33). Se reconoce por el `id` porque el broker conserva las cabeceras (A-32).
            if ($actual !== null && in_array($id, $actual['ids'], true)) {
                $this->stateWrite($c->name, 'ok');

                return new StateWriteResult(true, 'ok', $actual['seq']);
            }

            // 2. Una versión que no avanza NO se escribe, y una igual tampoco avanza. No es un error.
            if ($actual !== null && $actual['version'] !== null && $version <= $actual['version']) {
                $this->stateWrite($c->name, 'stale');

                return new StateWriteResult(false, 'stale', $actual['seq']);
            }

            // 3. Siempre por JetStream y siempre con la cabecera de secuencia: un publish de CORE a un subject de estado sobrescribe sin acuse
            // ni chequeo (A-45). `S = 0` = «la clave no existe»: crea si el subject no tiene mensaje y falla si ya lo tiene, y eso es lo que
            // hace que dos escritores concurrentes sobre una clave nueva no se pisen (A-33).
            try {
                $ack = $this->t()->publishAck($subject, $bytes, [
                    Protocol::MSG_ID_HEADER => $id,
                    'Nats-Expected-Last-Subject-Sequence' => (string) $s,
                ]);
            } catch (NoRespondersException $e) {
                throw $this->notProvisioned($stream, '$JS.API.DIRECT.GET.' . $stream . '.' . $subject, $e);
            }
            if (!isset($ack['error'])) {
                $this->stateWrite($c->name, 'ok');

                return new StateWriteResult(true, 'ok', (int) ($ack['seq'] ?? 0));
            }
            // 4. Alguien escribió entre el 1 y el 3: volver a 1. DOS códigos, no uno (A-33, A-49).
            $code = is_array($ack['error']) ? ($ack['error']['err_code'] ?? null) : null;
            if (in_array($code, StateEnvelope::CONFLICT_CODES, true)) {
                // Un poco de jitter es opcional (§3.3) y barato: sin él, tres instancias que chocan vuelven a chocar a la vez.
                if ($attempt < StateEnvelope::MAX_ATTEMPTS) {
                    usleep(random_int(0, 12_000 * $attempt));
                }

                continue;
            }
            throw new \RuntimeException(
                "JetStream rechazó la escritura de \"{$key}\" en {$c->name}: " . json_encode($ack['error'], JSON_UNESCAPED_UNICODE)
                . '. Si dice «no stream», STATE_' . strtoupper(str_replace('-', '_', $c->domain)) . ' no captura ese subject: lo aprovisiona `flux provision`.'
            );
        }
        $this->stateWrite($c->name, 'conflict');

        throw new StateConflictException($c->name, $key, StateEnvelope::MAX_ATTEMPTS);
    }

    // ─── leer ─────────────────────────────────────────────────────────────────

    /**
     * Una lectura puntual por `DIRECT.GET` sobre el subject exacto: no hay consumidor ni acuse. `null` si la clave no existe **o** es una
     * lápida. Es una lectura de RÉPLICA (§3.7): con `replicas: 3` PUEDE devolver el valor anterior justo después de un `put`; quien
     * necesite su propia escritura la deriva de `revision`, no relee.
     */
    public function get(string $contract, string $key, ?string $tenantId = null): ?StateEntry
    {
        $c = StateEnvelope::parseContract($contract);
        StateEnvelope::assertKey($key);
        $tenant = $this->tenantFor($tenantId);
        $subject = StateEnvelope::subject($c->name, $tenant, $key);
        $stored = $this->readRaw(StateEnvelope::streamName($c->domain), $subject);
        if ($stored === null) {
            return null; // 404 `Message Not Found`
        }
        ['entry' => $entry] = $this->decode($stored['data'], $c->name, $tenant, $key, $stored['seq'], $subject);

        return $entry->deleted ? null : $entry;
    }

    /**
     * `DIRECT.GET` por subject — la forma por SUBJECT, que se puede confinar por tenant con una ACL. Un valor ILEGIBLE no bloquea la clave
     * (§3.3): si el cuerpo no es JSON, o no tiene un `aggregateVersion` válido, se trata como «sin versión» y se sobrescribe con el chequeo de
     * secuencia sobre SU `S`. De lo contrario una entrada rota dejaría la clave inescribible para siempre, porque ningún servicio puede
     * purgar el stream (06-security.md §3.5).
     *
     * @return array{seq:int,data:string,ids:list<string>,version:?int}|null
     */
    private function readRaw(string $stream, string $subject): ?array
    {
        $r = $this->t()->directGet($stream, $subject, self::DIRECT_GET_TIMEOUT_MS);
        if ($r === null) {
            return null;
        }
        $ids = [];
        foreach ($r['headers'] as $name => $value) {
            if (strcasecmp((string) $name, Protocol::MSG_ID_HEADER) === 0 && is_string($value) && $value !== '') {
                $ids[] = $value;
            }
        }
        $version = null;
        try {
            $tree = json_decode($r['data'], false, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            return ['seq' => $r['seq'], 'data' => $r['data'], 'ids' => $ids, 'version' => null];
        }
        if ($tree instanceof \stdClass) {
            if (isset($tree->id) && is_string($tree->id)) {
                $ids[] = $tree->id;
            }
            $d = $tree->data ?? null;
            $v = $d instanceof \stdClass ? ($d->aggregateVersion ?? null) : null;
            $version = StateEnvelope::isValidAggregateVersion($v) ? $v : null;
        }

        return ['seq' => $r['seq'], 'data' => $r['data'], 'ids' => $ids, 'version' => $version];
    }

    // ─── observar ─────────────────────────────────────────────────────────────

    /**
     * Foto completa y después los cambios, por el MISMO canal: un consumidor durable de `STATE_<DOM>` con `deliver_policy:
     * last_per_subject`. Un consumidor que estuvo caído recibe solo el último valor de cada clave; un valor reemplazado mientras estaba en
     * vuelo NO se reentrega (A-35, A-36). El handler DEBE ser idempotente y no asumir orden (`UPDATE … WHERE aggregate_version < $n`) y, ante
     * `$entry->deleted`, DEBE borrar su copia local.
     *
     * **Registra y abre el durable; no consume.** El consumo lo conduce `FluxBus::run()` en un worker de larga vida.
     *
     * @param callable(StateEntry,StateHandlerContext):void $handler
     * @param (callable():void)|null $onSynced Se invoca UNA vez por `watch()` —no una por reconexión, ni tras un `resync`— cuando el handler
     *        ya ha procesado todo lo que había pendiente al abrirlo. Con el contrato vacío (o el durable al día), de inmediato.
     */
    public function watch(string $contract, callable $handler, ?string $tenantId = null, ?callable $onSynced = null): StateWatch
    {
        $c = StateEnvelope::parseContract($contract);
        $tenant = $this->watchTenant($tenantId);
        if ($this->options->tenantIsolation === TenantIsolation::Strict && $tenant === null) {
            throw new TenantIsolationException("state.{$c->name}", 'no hay tenantId ni en connect() ni en watch()');
        }
        $stream = StateEnvelope::streamName($c->domain);
        $durable = StateEnvelope::durableName($this->options->service, $c->name, $tenant);

        // El SDK comprueba la DLQ al ARRANCAR, no cuando llega el primer mensaje envenenado.
        $this->ensureStateDlq($c->domain);

        $requested = [
            'durable_name' => $durable,
            'filter_subject' => StateEnvelope::filterSubject($c->name, $tenant),
            'ack_policy' => Protocol::ACK_POLICY,
            // ack_wait DEBE ser backoff[0]: JetStream lo sobrescribe con ese valor sin avisar.
            'ack_wait' => Protocol::nanos(Protocol::ACK_WAIT_MS),
            'max_deliver' => Protocol::MAX_DELIVER,
            'backoff' => array_map(Protocol::nanos(...), Protocol::BACKOFF_MS),
            'max_ack_pending' => Protocol::MAX_ACK_PENDING,
            'deliver_policy' => 'last_per_subject',
            'replay_policy' => Protocol::REPLAY_POLICY,
        ];
        $found = $this->t()->ensureConsumer($stream, $durable, $requested);
        $this->assertConfigHonored($durable, $requested, $found['config']);

        // Un pull denegado no da error y deja el watch inerte, aparentemente sano (A-20): `watch()` DEBE fallar con PERMISSION_DENIED.
        $this->t()->assertPullAllowed($stream, $durable);

        $sync = new SyncTracker($this->syncedCallback($onSynced));
        $w = new StateWatch(
            contract: $c->name,
            tenantId: $tenant,
            stream: $stream,
            durable: $durable,
            consumerLabel: StateEnvelope::durableName($this->options->service, $c->name),
            subjectLabel: "state.{$c->name}",
            handler: \Closure::fromCallable($handler),
            sync: $sync,
            requested: $requested,
        );
        // Stream vacío, o durable al día: `onSynced` de INMEDIATO, antes de entregar nada y aunque no llegue ninguna entrada después. Esperar a
        // una que quizá no llegue nunca dejaría al servicio «sincronizando» para siempre (10-state.md §5.5, punto 2). `num_ack_pending`
        // también: lo entregado y sin confirmar sigue pendiente de procesar.
        if ($found['numPending'] === 0 && $found['numAckPending'] === 0) {
            $sync->immediate();
        }
        $this->watches[] = $w;

        return $w;
    }

    /**
     * Pide mensajes a cada `watch` activo y los procesa, **en serie**. Devuelve cuántos procesó. Lo llama `FluxBus::run()`.
     *
     * @param int $sliceMs cuánto espera cada `watch` a que le llegue algo
     */
    public function poll(int $sliceMs): int
    {
        $processed = 0;
        foreach ($this->watches as $w) {
            if (!$w->active) {
                continue;
            }
            if ($w->reopening && !$this->reopen($w)) {
                continue;
            }
            try {
                $messages = $this->t()->fetch($w->stream, $w->durable, 1, $sliceMs);
            } catch (ConsumerGoneException) {
                // El durable se borró bajo el watch (`flux state resync`): es «reabre cuando exista de nuevo», no un fallo ni el final del
                // watch. Un pull a un consumidor que no existe NO es «no hay mensajes»: tratarlo como ocioso dejaría el watch parado y sano.
                $w->reopening = true;
                $w->reopenAttempt = 0;
                $w->nextReopenAt = 0.0;
                $this->options->logger?->warning(
                    "[flux] el durable {$w->durable} se borró con el watch de {$w->subjectLabel} abierto (¿`flux state resync`?): se vuelve a abrir "
                    . 'el consumo cuando exista de nuevo.'
                );

                continue;
            } catch (\Throwable $e) {
                $this->options->logger?->warning("[flux] fallo al recibir de {$w->subjectLabel}: " . $e->getMessage());
                // Sin esta pausa, un fallo que no espera (la conexión caída) haría girar el bucle a toda velocidad.
                usleep(200_000);

                continue;
            }
            foreach ($messages as $message) {
                try {
                    $this->dispatch($w, $message);
                } catch (\Throwable $e) {
                    // Sin este catch un fallo inesperado rompería el bucle y el watch dejaría de consumir EN SILENCIO, con la conexión viva: un
                    // consumidor muerto que parece vivo. Sin ack ni term: el mensaje se reentrega, que es correcto.
                    $this->options->logger?->error(
                        "[flux] fallo inesperado despachando el estado {$w->subjectLabel}: {$e->getMessage()}. El mensaje no se confirma y será reentregado."
                    );
                }
                $processed++;
            }
        }

        return $processed;
    }

    /** ¿Hay algún `watch` activo? */
    public function hasActiveWatches(): bool
    {
        foreach ($this->watches as $w) {
            if ($w->active) {
                return true;
            }
        }

        return false;
    }

    /** Deja de pedir mensajes a todos los `watch` (`bus->close()`). */
    public function stopAll(): void
    {
        foreach ($this->watches as $w) {
            $w->stop();
            $this->dropPending($w);
        }
    }

    /**
     * Vuelve a abrir el consumo de un durable que se borró bajo el watch. Una vez por llamada de `poll()`, con backoff (100 ms … 5 s) hasta
     * que el durable exista —`flux state resync` lo recrea a continuación—. Comprueba la configuración como al abrir: un durable recreado con
     * otro filtro entregaría las claves de otro tenant sin ningún error, y ese no se consume. **No necesita `CONSUMER.CREATE`.**
     */
    private function reopen(StateWatch $w): bool
    {
        if (microtime(true) < $w->nextReopenAt) {
            usleep(10_000);

            return false;
        }
        $w->reopenAttempt++;
        $failure = $this->checkDurable($w);
        if ($failure !== null) {
            // Se avisa la primera vez, cada 10 intentos y SIEMPRE que la causa cambia (un durable recreado con otra configuración no es «aún
            // no existe»).
            $reason = $failure->getMessage();
            if ($w->reopenAttempt === 1 || $w->reopenAttempt % 10 === 0 || $reason !== $w->lastReopenReason) {
                $this->options->logger?->warning(
                    "[flux] no se pudo reabrir el consumo de {$w->durable} (intento {$w->reopenAttempt}): {$reason}. Se reintenta."
                );
                $w->lastReopenReason = $reason;
            }
            $ms = min(self::REOPEN_MAX_MS, self::REOPEN_MIN_MS * (2 ** min($w->reopenAttempt - 1, 6)));
            $w->nextReopenAt = microtime(true) + $ms / 1000;

            return false;
        }
        $this->options->logger?->warning("[flux] consumo de {$w->durable} reabierto (intento {$w->reopenAttempt}): se entregará la foto otra vez.");
        $w->reopening = false;
        $w->lastConsumerSeq = null;

        return true;
    }

    /** ¿Sigue siendo el durable el que se pidió? `null` si sí; el motivo si no existe, tiene otra configuración o ya no se puede pedir de él. */
    private function checkDurable(StateWatch $w): ?\Throwable
    {
        try {
            $info = $this->t()->consumerInfo($w->stream, $w->durable);
            if ($info === null) {
                return new \RuntimeException("el consumidor {$w->durable} no existe todavía (`consumer not found`)");
            }
            $this->assertConfigHonored($w->durable, $w->requested, $info['config']);
            $this->t()->assertPullAllowed($w->stream, $w->durable);
        } catch (\Throwable $e) {
            return $e;
        }

        return null;
    }

    // ─── despacho de una entrada ──────────────────────────────────────────────

    private function dispatch(StateWatch $w, RawMessage $m): void
    {
        // Un durable borrado y recreado entre dos pulls (`flux state resync` rápido) no da ningún error: el siguiente pull encuentra uno nuevo y
        // entrega su foto. Se nota porque la secuencia del consumidor no crece. Antes de entregar nada de él se comprueba que sigue siendo el
        // pedido: recreado con otro filtro entregaría las claves de otro tenant sin un solo error, y a ese no se le entrega.
        $cseq = Ack::consumerSequence($m->replyTo);
        if ($cseq !== null) {
            if ($w->lastConsumerSeq !== null && $cseq <= $w->lastConsumerSeq) {
                $failure = $this->checkDurable($w);
                if ($failure !== null) {
                    $w->reopening = true;
                    $w->reopenAttempt = 1;
                    $w->nextReopenAt = microtime(true) + self::REOPEN_MIN_MS / 1000;
                    $w->lastReopenReason = $failure->getMessage();
                    $w->lastConsumerSeq = null;
                    $this->options->logger?->warning(
                        "[flux] no se pudo reabrir el consumo de {$w->durable} (intento 1): el durable se recreó y {$failure->getMessage()}. "
                        . 'No se entrega nada de él; se reintenta.'
                    );

                    return; // sin ack ni term: no es de este watch
                }
                $this->options->logger?->warning("[flux] el durable {$w->durable} se recreó con el watch abierto: se entrega la foto otra vez.");
            }
            $w->lastConsumerSeq = $cseq;
        }

        $pending = Ack::pending($m->replyTo);
        if ($pending !== null) {
            $this->notePending($w, $pending);
        }
        $w->sync->started($pending);
        try {
            $this->process($w, $m);
        } finally {
            $w->sync->finished();
        }
    }

    private function process(StateWatch $w, RawMessage $m): void
    {
        $attempt = Ack::deliveryAttempt($m->replyTo);
        $maxAttempts = Protocol::MAX_DELIVER;
        // `state.<dom>.<agr>.v<M>.<tenant>.<clave>`: la identidad la decide EL BROKER, no el sobre.
        $parts = explode('.', $m->subject);
        $tenant = $parts[4] ?? '';
        $key = $parts[5] ?? '';
        $revision = Ack::streamSequence($m->replyTo) ?? 0;
        $started = microtime(true);
        $event = null;
        $warn = false;

        try {
            ['entry' => $entry, 'signatureWarning' => $warn] = $this->decode($m->payload, $w->contract, $tenant, $key, $revision, $m->subject, $event);
            $ctx = new StateHandlerContext($attempt, $maxAttempts, $m->subject, $w->contract);
            EventContext::run(EventContext::fromEvent($event), fn () => ($w->handler)($entry, $ctx));
            $this->respond($m, Ack::ACK);
            // El aviso de firma GANA sobre el `ok` (como en los eventos): se procesó, pero hay que poder contar cuántos siguen sin firma.
            $this->metrics->eventConsumed($w->subjectLabel, $w->consumerLabel, $warn ? ConsumeOutcome::InvalidSignature : ConsumeOutcome::Ok);
            $this->metrics->handlerDuration($w->subjectLabel, $w->consumerLabel, microtime(true) - $started);
        } catch (\Throwable $e) {
            $this->fail($w, $m, $e, $event, $tenant, $key, $revision, $attempt, $maxAttempts, $started, $warn);
        }
    }

    private function fail(
        StateWatch $w,
        RawMessage $m,
        \Throwable $e,
        ?FluxEvent $event,
        string $tenant,
        string $key,
        int $revision,
        int $attempt,
        int $maxAttempts,
        float $started,
        bool $signatureWarning = false,
    ): void {
        $c = $this->classifier->classify($e);
        $budget = min($maxAttempts, $c->maxAttempts ?? $maxAttempts);

        if ($c->errorClass === ErrorClass::Retryable && $attempt < $budget) {
            $delay = $c->retryAfterMs ?? (float) Protocol::BACKOFF_MS[min($attempt - 1, count(Protocol::BACKOFF_MS) - 1)];
            $this->options->logger?->warning(sprintf(
                '[flux] RETRYABLE %s en %s (clave %s, intento %d/%d), reintento en %d ms',
                $c->code, $w->subjectLabel, $key, $attempt, $budget, (int) $delay,
            ));
            $this->metrics->eventRetried($w->subjectLabel, $w->consumerLabel, $attempt);
            $this->respond($m, Ack::nakWithDelay($delay));

            return;
        }

        $reason = $c->errorClass;
        $outcome = match (true) {
            $signatureWarning, in_array($c->code, ['MISSING_SIGNATURE', 'UNKNOWN_SIGNING_KEY', 'INVALID_SIGNATURE'], true) => ConsumeOutcome::InvalidSignature,
            in_array($c->code, [SchemaValidationError::CODE, SchemaNotFoundError::CODE], true) => ConsumeOutcome::InvalidSchema,
            default => ConsumeOutcome::fromErrorClass($reason),
        };

        // `dlqerror` NO DEBE contener valores de `data` (10-state.md §6). Un error de flux lleva su código y un mensaje que no cita valores.
        // Uno que no sea de flux —una excepción de un driver de base de datos, que puede citar la fila entera— se reduce a su código: la DLQ
        // guarda 90 días y en estado `data` es dato personal.
        $dlqerror = $e instanceof FluxError
            ? "{$c->code}: {$e->getMessage()}"
            : "{$c->code}: " . StateEnvelope::UNCLASSIFIED_DLQ_ERROR;
        $dlqtime = Envelope::now();
        $dlqEvent = $event !== null
            ? StateEnvelope::toDlqEvent($event, $key, $revision, $reason, $attempt, $w->durable, $dlqerror, $dlqtime)
            : StateEnvelope::buildUnreadableDlqEvent(
                contract: $w->contract,
                tenantId: $tenant,
                key: $key,
                revision: $revision,
                source: $this->source,
                producerversion: $this->options->version,
                id: Protocol::uuid7(),
                time: $dlqtime,
                reason: $reason,
                attempts: $attempt,
                consumer: $w->durable,
                error: $dlqerror,
                dlqtime: $dlqtime,
                dataschema: $this->schemaForSafe($w->contract),
            );
        try {
            $this->t()->publish(
                StateEnvelope::dlqSubject($w->contract),
                Envelope::serialize($dlqEvent),
                [Protocol::MSG_ID_HEADER => "dlq-{$dlqEvent->id}-{$w->durable}"],
            );
        } catch (\Throwable $dlqError) {
            // NO se hace term(): terminar aquí borraría la entrada sin haberla guardado en ningún sitio.
            $this->options->logger?->error(
                "[flux] no se pudo publicar en la DLQ de estado para {$w->subjectLabel} (clave {$key}): {$dlqError->getMessage()}. "
                . 'El mensaje NO se termina; se reentregará.'
            );

            return;
        }

        if ($c->errorClass === ErrorClass::Poison) {
            // POISON es lo que despierta a alguien: term() + DLQ + ALERTA. El cuerpo crudo NO se entrega: una entrada de estado puede llevar
            // datos personales (10-state.md §6).
            if ($this->options->onPoison !== null) {
                try {
                    ($this->options->onPoison)($m->subject, $e instanceof PoisonError ? $e : new PoisonError($e->getMessage(), $c->code, $e), '');
                } catch (\Throwable $cb) {
                    $this->options->logger?->warning('[flux] el callback onPoison lanzó: ' . $cb->getMessage());
                }
            }
            $this->options->logger?->error("[flux] POISON {$c->code} en {$w->subjectLabel} (clave {$key}): " . ($e instanceof FluxError ? $e->getMessage() : $c->code));
        } else {
            $this->options->logger?->error(
                "[flux] DLQ de estado ({$reason->value}) {$c->code} en {$w->subjectLabel} (clave {$key}) tras {$attempt} intento(s)"
            );
        }
        $this->metrics->eventConsumed($w->subjectLabel, $w->consumerLabel, $outcome);
        $this->metrics->eventDlq($w->subjectLabel, $w->consumerLabel, $reason, $c->code);
        $this->metrics->handlerDuration($w->subjectLabel, $w->consumerLabel, microtime(true) - $started);
        if ($this->options->onDlq !== null) {
            try {
                ($this->options->onDlq)($m->subject, $dlqEvent, $c);
            } catch (\Throwable $cb) {
                $this->options->logger?->warning('[flux] el callback onDlq lanzó: ' . $cb->getMessage());
            }
        }
        $this->respond($m, Ack::TERM);
    }

    private function respond(RawMessage $m, string $token): void
    {
        if ($m->replyTo !== null) {
            $this->t()->respond($m->replyTo, $token);
        }
    }

    /**
     * Lo que comprueba el SDK al consumir, en el orden de 10-state.md §5.4. Lo que falla **nunca** se aplica y **nunca** llega al handler.
     * `$holder` se rellena en cuanto el sobre es un CloudEvent válido, para que la DLQ pueda partir de él.
     *
     * @return array{event:FluxEvent,entry:StateEntry,signatureWarning:bool}
     */
    private function decode(string $bytes, string $contract, string $tenant, string $key, int $revision, string $subject, ?FluxEvent &$holder = null): array
    {
        // 1. Un CloudEvent válido con las cuatro extensiones (POISON, los códigos de siempre).
        $event = Envelope::parse($bytes);
        $holder = $event;

        // 2. La identidad está en el subject; el sobre la repite. Si discrepan, uno miente: POISON.
        if ($event->tenantid !== $tenant) {
            throw new StateSubjectMismatchException('tenantid', $event->tenantid, $tenant);
        }
        if ($event->subject !== $key) {
            throw new StateSubjectMismatchException('subject', (string) $event->subject, $key);
        }
        $put = StateEnvelope::type($contract);
        $deleted = $event->type === StateEnvelope::tombstoneType($contract);
        if (!$deleted && $event->type !== $put) {
            throw new StateSubjectMismatchException('type', $event->type, $put);
        }

        // 3. `restricted` no se admite en estado: en consumo es POISON.
        if ($event->dataclassification === 'restricted') {
            throw new RestrictedStateEntryException();
        }

        // 4. `aggregateVersion` válido: sin él no se puede aplicar de forma condicional (PERMANENT).
        $version = StateEnvelope::aggregateVersionOf($event->data);

        // La firma antes que el esquema: un evento manipulado puede validar perfectamente y no ser del productor que dice (07-signing.md §5).
        $warning = $this->verifier?->check($event) !== null;

        // 5. Si hay L3 al consumir: `data` contra su esquema.
        if ($this->options->validation?->onConsume === true) {
            $this->validator?->validate($event, $subject);
        }

        return [
            'event' => $event,
            'signatureWarning' => $warning,
            'entry' => new StateEntry($key, $tenant, $deleted, $deleted ? null : $event->data, $version, $revision),
        ];
    }

    // ─── consumidor y DLQ ─────────────────────────────────────────────────────

    /**
     * Como `FluxBus::assertConfigHonored`, más lo propio del estado: la política de entrega y el filtro. Un durable aprovisionado con otro
     * filtro entregaría un conjunto distinto del pedido (otro tenant, todos los tenants) sin ningún error.
     *
     * @param array<string,mixed> $requested
     * @param array<string,mixed> $effective
     */
    private function assertConfigHonored(string $durable, array $requested, array $effective): void
    {
        $diffs = [];
        foreach (['ack_wait', 'max_deliver', 'max_ack_pending', 'deliver_policy'] as $f) {
            if (array_key_exists($f, $effective) && $effective[$f] != $requested[$f]) { // phpcs:ignore
                $diffs[] = ['field' => $f, 'requested' => $requested[$f], 'effective' => $effective[$f]];
            }
        }
        $effB = is_array($effective['backoff'] ?? null) ? array_values($effective['backoff']) : [];
        if ($effB !== array_values($requested['backoff'])) {
            $diffs[] = ['field' => 'backoff', 'requested' => $requested['backoff'], 'effective' => $effB];
        }
        $filters = is_array($effective['filter_subjects'] ?? null) ? $effective['filter_subjects'] : [];
        $filter = $effective['filter_subject'] ?? (count($filters) === 1 ? $filters[0] : null);
        if ($filter !== $requested['filter_subject'] || count($filters) > 1) {
            $diffs[] = ['field' => 'filter_subject', 'requested' => $requested['filter_subject'], 'effective' => $filter ?? $filters];
        }
        if ($diffs !== []) {
            throw new ConsumerConfigMismatchException($durable, $diffs);
        }
    }

    /**
     * `DLQ_<DOM>` debe capturar `dlq.state.<dom>.>`. Un dominio solo de eventos conserva su `DLQ_<DOM>` de la v1.0 sin tocarlo: ampliarlo es
     * posible y no pierde mensajes (A-44), pero no se le pide a quien no usa estado, y NO lo hace el SDK sobre uno existente: lo hace `flux
     * provision`. Si no captura, se AVISA al abrir, y no cuando el primer mensaje muera.
     */
    private function ensureStateDlq(string $domain): void
    {
        $name = Protocol::dlqStreamName($domain);
        if (isset($this->dlqChecked[$name])) {
            return;
        }
        $sample = "dlq.state.{$domain}.x.v1";
        $config = $this->t()->streamConfig($name);
        if ($config === null) {
            // Desarrollo: se crea con LOS DOS subjects. En producción lo aprovisiona `flux provision` (y sin permiso de crear es NOT_PROVISIONED).
            $this->t()->ensureStream($name, ["dlq.{$domain}.>", "dlq.state.{$domain}.>"], Protocol::DLQ_STREAM_MAX_AGE_MS);
        } else {
            $subjects = is_array($config['subjects'] ?? null) ? $config['subjects'] : [];
            $captures = false;
            foreach ($subjects as $s) {
                if (is_string($s) && self::subjectMatches($s, $sample)) {
                    $captures = true;
                }
            }
            if (!$captures) {
                $this->options->logger?->warning(
                    "[flux] {$name} no captura \"dlq.state.{$domain}.>\": una entrada de estado que muera no se podrá guardar y se reentregará. "
                    . 'Un dominio con estado lo necesita; lo amplía `flux provision` y lo audita `flux doctor` (10-state.md §6).'
                );
            }
        }
        $this->dlqChecked[$name] = true;
    }

    /** Un patrón de subject de NATS (`*`, `>`) contra un subject concreto. */
    public static function subjectMatches(string $pattern, string $subject): bool
    {
        $p = explode('.', $pattern);
        $s = explode('.', $subject);
        foreach ($p as $i => $token) {
            if ($token === '>') {
                return count($s) > $i;
            }
            if ($i >= count($s)) {
                return false;
            }
            if ($token !== '*' && $token !== $s[$i]) {
                return false;
            }
        }

        return count($p) === count($s);
    }

    // ─── tenant, esquema, errores y métricas ──────────────────────────────────

    /** Escritura y lectura: la opción de la llamada, el evento en curso y `connect()`. Sin ninguno, error. */
    private function tenantFor(?string $option): string
    {
        $t = $option ?? EventContext::current()?->tenantid ?? $this->options->tenantId;
        if ($t === null) {
            throw new InvalidTenantForStateException(null);
        }

        return StateEnvelope::assertTenant($t);
    }

    /**
     * `watch`: la opción y `connect()`, **sin el evento en curso** (un `watch` no corre dentro de un handler: 10-state.md §2.4). Sin ninguno es
     * legítimo (se observan todos), salvo `strict`.
     */
    private function watchTenant(?string $option): ?string
    {
        $t = $option ?? $this->options->tenantId;

        return $t === null ? null : StateEnvelope::assertTenant($t);
    }

    /** El `dataschema` del contrato: mapa explícito, índice `state` del bundle o `schemaBaseUrl`. Nunca por red. */
    private function schemaFor(string $contract): string
    {
        $o = $this->options;
        if (isset($o->schemas[$contract])) {
            return $o->schemas[$contract];
        }
        $fromBundle = $o->validation?->bundle?->stateUriFor($contract);
        if ($fromBundle !== null) {
            return $fromBundle;
        }
        if ($o->schemaBaseUrl === null) {
            throw new \Flux\EnvelopeException(
                "no hay dataschema para el estado \"{$contract}\". Declara schemas[\"{$contract}\"], schemaBaseUrl, o pasa un bundle con el índice "
                . '`state` en validation.bundle (ConnectOptions).'
            );
        }

        return StateEnvelope::dataschema($contract, $o->schemaBaseUrl);
    }

    /** Para la DLQ: si no se puede resolver, el default del repo; un fallo de configuración no debe perder el aviso. */
    private function schemaForSafe(string $contract): string
    {
        try {
            return $this->schemaFor($contract);
        } catch (\Throwable) {
            return StateEnvelope::dataschema($contract);
        }
    }

    private static function time(\DateTimeInterface|string|null $t): string
    {
        return $t === null ? Envelope::now() : ($t instanceof \DateTimeInterface ? Envelope::formatTime($t) : $t);
    }

    private function notProvisioned(string $stream, string $silent, \Throwable $cause): NotProvisionedException
    {
        return new NotProvisionedException(
            'stream',
            $stream,
            null,
            $this->options->service,
            $silent,
            "nadie contestó en \"{$silent}\": el stream \"{$stream}\" no existe, o existe sin `allow_direct: true`, y los dos casos se ven exactamente igual "
            . "(no hay un 404: no responde nadie). \"{$this->options->service}\" no lo crea ni lo consulta. Aprovisiónalo con `flux provision`, con credenciales "
            . 'de operación, y compruébalo con `flux doctor` (10-state.md §5.1, 04-errors.md §6).',
            $cause,
        );
    }

    /**
     * `invalid_schema` se emite también cuando la llamada lanza: una excepción sirve a quien la provoca, y la métrica a quien opera el ecosistema.
     * Es todo fallo de L3 —también el esquema ausente del bundle en modo estricto— y `MISSING_AGGREGATE_VERSION`. Los demás errores de la
     * llamada (contrato, clave, tenant, dominio reservado, clasificación) son de programación y NO se cuentan.
     *
     * @template R
     * @param callable():R $fn
     * @return R
     */
    private function countingInvalid(string $contract, callable $fn): mixed
    {
        try {
            return $fn();
        } catch (MissingAggregateVersionException|SchemaValidationError|SchemaNotFoundError $e) {
            $this->stateWrite($contract, 'invalid_schema');

            throw $e;
        }
    }

    private function stateWrite(string $contract, string $outcome): void
    {
        if ($this->metrics instanceof StateRpcMetrics) {
            $this->metrics->stateWrite($contract, $outcome);
        }
    }

    /** @return \Closure():void */
    private function syncedCallback(?callable $cb): \Closure
    {
        return function () use ($cb): void {
            if ($cb === null) {
                return;
            }
            try {
                $cb();
            } catch (\Throwable $e) {
                $this->options->logger?->error('[flux] onSynced lanzó: ' . $e->getMessage());
            }
        };
    }

    private function notePending(StateWatch $w, int $pending): void
    {
        $sum = $this->pending->note("{$w->subjectLabel}|{$w->consumerLabel}", $w->durable, $pending);
        $this->metrics->consumerPending($w->subjectLabel, $w->consumerLabel, $sum);
    }

    private function dropPending(StateWatch $w): void
    {
        $sum = $this->pending->drop("{$w->subjectLabel}|{$w->consumerLabel}", $w->durable);
        if ($sum !== null) {
            $this->metrics->consumerPending($w->subjectLabel, $w->consumerLabel, $sum);
        }
    }
}
