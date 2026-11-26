/*
 * Estado replicado — la extension opcional `state`.
 * Contrato normativo: specification/10-state.md, specification/04-errors.md §6.
 *
 * Un EVENTO dice que paso; el ESTADO es el valor ACTUAL de un agregado, que su dueno publica y los demas observan. Sustituye a
 * `GET /clientes/123`, a la carga inicial y al polling de datos. Este fichero es la mitad que habla con el broker (`bus.state()`):
 * `put`, `delete`, `get` y `watch`. La mitad pura —el subject, el sobre, la lapida, la DLQ— es {@link StateEnvelope}.
 *
 * Regla de diseno heredada de FluxBus: ningun tipo de NATS en la API publica.
 */
package com.flux;

import com.fasterxml.jackson.databind.JsonNode;
import com.flux.FluxErrors.Classification;
import com.flux.SecurityErrors.NotProvisionedException;
import com.flux.SecurityErrors.PermissionDeniedException;
import com.flux.SecurityErrors.ResourceKind;
import com.flux.StateErrors.InvalidTenantForStateException;
import com.flux.StateErrors.MissingAggregateVersionException;
import com.flux.StateErrors.RestrictedStateEntryException;
import com.flux.StateErrors.StateConflictException;
import com.flux.StateErrors.StateSubjectMismatchException;
import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.JetStreamStatusException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.ReplayPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** El cliente de estado de un {@link FluxBus}. Se obtiene con {@link FluxBus#state()}. */
public final class FluxState {

    // ─── Tipos publicos ──────────────────────────────────────────────────────

    /** El {@code outcome} de un {@code put} o un {@code delete}. No hay un tercero: un conflicto agotado es un error. */
    public enum Outcome {
        OK("ok"),
        STALE("stale");

        private final String wire;

        Outcome(String wire) {
            this.wire = wire;
        }

        public String wire() {
            return wire;
        }
    }

    /**
     * Resultado de {@code put} y {@code delete} — 10-state.md §3.5.
     *
     * @param applied  {@code true} si se escribio. {@code stale} NO es un error: es la respuesta normal de un dueno que
     *                 reproduce un evento viejo o de dos instancias que compiten.
     * @param revision {@code ok}: la secuencia del mensaje nuevo. {@code stale}: la del mensaje ACTUAL, el que la hizo obsoleta.
     */
    public record WriteResult(boolean applied, Outcome outcome, long revision) {
    }

    /**
     * Una entrada de estado, tal como la ve quien la lee ({@code get}, handler de {@code watch}) — 10-state.md §5.3.
     *
     * @param tenantId         el tenant del subject. No estaba en el plan y es necesario: un {@code watch} sin tenant recibe
     *                         {@code cli-1} de varios, y la clave sola mezclaria los tenants en la copia local.
     * @param deleted          {@code true} si es una lapida: el handler DEBE borrar o anonimizar su copia local.
     * @param data             el {@code data} del evento; {@code null} si {@code deleted}.
     * @param aggregateVersion la version del valor (o la de la lapida).
     * @param revision         la secuencia del stream: dice QUE mensaje es; no ordena versiones ni se compara entre contratos.
     */
    public record Entry(String key, String tenantId, boolean deleted, JsonNode data, long aggregateVersion,
                        long revision) {

        /** El {@code data} como el tipo de la aplicacion. Un desajuste es PERMANENT ({@code DATA_SCHEMA_MISMATCH}). */
        public <T> T dataAs(Class<T> type) {
            if (data == null) {
                return null;
            }
            try {
                return Envelope.mapper().treeToValue(data, type);
            } catch (Exception e) {
                throw new FluxErrors.PermanentException("el dato de la entrada \"" + key + "\" no encaja en el tipo esperado",
                        "DATA_SCHEMA_MISMATCH", e);
            }
        }
    }

    /**
     * Metadatos de ESTA entrega.
     *
     * @param attempt numero de entrega, empezando en 1. Si es &gt; 1 la entrada YA se intento procesar: el handler es
     *                idempotente (03-delivery.md §4).
     * @param subject el subject de NATS de la entrada ({@code state.clientes.cliente.v1.acme.cli-1}).
     * @param contract el contrato observado ({@code clientes.cliente.v1}).
     */
    public record Delivery(int attempt, int maxAttempts, String subject, String contract) {
    }

    /**
     * Procesa una entrada. Volver sin lanzar la confirma ({@code ack}); lanzar clasifica segun 04-errors.md.
     *
     * <p>El handler DEBE ser idempotente y NO asumir orden ({@code UPDATE … WHERE aggregate_version < $n}) y, ante
     * {@code entry.deleted()}, DEBE borrar su copia local (10-state.md §5.3).
     */
    @FunctionalInterface
    public interface Handler {
        void handle(EventContext context, Entry entry, Delivery delivery) throws Exception;
    }

    public static final class PutOptions {
        private String tenantId;
        private FluxEvent.DataClassification classification;
        private Instant time;

        /** Tenant. Si no, el del evento en curso y, si no, el de {@code connect()}. Sin ninguno: {@code INVALID_TENANT_FOR_STATE}. */
        public PutOptions tenantId(String v) {
            this.tenantId = v;
            return this;
        }

        /** {@code public}, {@code internal} o {@code confidential} (el default: NO el de la conexion). {@code restricted} se rechaza. */
        public PutOptions classification(FluxEvent.DataClassification v) {
            this.classification = v;
            return this;
        }

        /** Instante en que ocurrio el hecho, si no es «ahora». */
        public PutOptions time(Instant v) {
            this.time = v;
            return this;
        }
    }

    public static final class DeleteOptions {
        private String tenantId;
        private Instant time;

        public DeleteOptions tenantId(String v) {
            this.tenantId = v;
            return this;
        }

        public DeleteOptions time(Instant v) {
            this.time = v;
            return this;
        }
    }

    public static final class GetOptions {
        private String tenantId;

        public GetOptions tenantId(String v) {
            this.tenantId = v;
            return this;
        }
    }

    public static final class WatchOptions {
        private String tenantId;
        private Runnable onSynced;

        /**
         * Filtra por este tenant (y el durable lleva su sufijo). Si no, el de {@code connect()} —que SIEMPRE filtra, tambien si
         * vale {@code system}—. Sin ninguno se observan todos, salvo con {@code TenantIsolation.STRICT}. NUNCA el del evento en
         * curso (a diferencia de {@code put}, {@code delete} y {@code get}): un {@code watch} no corre dentro de un handler
         * (10-state.md §2.4). Un tenant por {@code watch}: cada llamada es un durable.
         */
        public WatchOptions tenantId(String v) {
            this.tenantId = v;
            return this;
        }

        /**
         * Se invoca UNA vez por {@code watch()} —no una por reconexion, ni de nuevo tras un {@code flux state resync}— cuando el
         * handler ya ha procesado todo lo que habia pendiente al abrirlo: la senal de «mi replica local ya puede contestar
         * lecturas». Con el contrato vacio (o el durable al dia), de inmediato y antes de entregar nada. NO promete que no
         * lleguen entradas despues (10-state.md §5.5).
         */
        public WatchOptions onSynced(Runnable v) {
            this.onSynced = v;
            return this;
        }
    }

    /** Un {@code watch} vivo. */
    public final class Subscription implements AutoCloseable {
        private final Watch watch;

        private Subscription(Watch watch) {
            this.watch = watch;
        }

        public String contract() {
            return watch.contract;
        }

        public String durable() {
            return watch.durable;
        }

        /** El tenant por el que filtra, si filtra. */
        public Optional<String> tenantId() {
            return Optional.ofNullable(watch.tenant);
        }

        /** Detiene la entrega. */
        public void unsubscribe() {
            watch.stop();
            watches.remove(watch);
        }

        @Override
        public void close() {
            unsubscribe();
        }
    }

    // ─── Constantes ──────────────────────────────────────────────────────────

    /**
     * Los DOS codigos del conflicto de secuencia (A-33, A-49): 10071 «wrong last sequence: N» (un nodo, o un cluster sin carrera)
     * y 10164 «wrong last sequence», SIN numero (cluster, bajo carrera: 210 de 210 rechazos de 8 escritores). Un SDK que solo
     * reconozca el 10071 pasa sus pruebas contra un nodo y, en produccion, trata un conflicto corriente como un error desconocido
     * y no reintenta.
     */
    static final List<Integer> CONFLICT_CODES = List.of(10071, 10164);

    private static final long DIRECT_GET_TIMEOUT_MILLIS = 5_000;

    /** Cuantos mensajes pide cada lote del consumo y cuanto vive un lote que no se llena (el silencio normal de un contrato quieto). */
    private static final int PULL_BATCH = 100;
    private static final java.time.Duration PULL_EXPIRES = java.time.Duration.ofSeconds(10);
    private static final int JS_STREAM_NOT_FOUND = 10059;
    private static final int JS_CONSUMER_NOT_FOUND = 10014;

    /**
     * SOLO PARA LAS PRUEBAS: se invoca entre la lectura y la publicacion de cada intento, con su numero. Una prueba mete ahi la escritura
     * de «otra instancia» y fabrica el conflicto de secuencia de forma determinista, en vez de esperar a que una carrera lo produzca.
     */
    static volatile java.util.function.IntConsumer beforePublish;

    /** ¿Es {@code apiErrorCode}, o algun error de la cadena de causas, un conflicto de secuencia (10071 o 10164)? */
    static boolean isSequenceConflict(int apiErrorCode) {
        return CONFLICT_CODES.contains(apiErrorCode);
    }

    static boolean isSequenceConflict(Throwable e) {
        for (Throwable t = e, i = null; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof JetStreamApiException api && isSequenceConflict(api.getApiErrorCode())) {
                return true;
            }
        }
        return false;
    }

    // ─── Estado ──────────────────────────────────────────────────────────────

    private final FluxBus bus;
    private final PendingSum pending = new PendingSum();
    private final Set<String> dlqChecked = ConcurrentHashMap.newKeySet();
    private final Set<Watch> watches = ConcurrentHashMap.newKeySet();

    FluxState(FluxBus bus) {
        this.bus = bus;
    }

    private FluxBus.ConnectOptions options() {
        return bus.options;
    }

    private void log(System.Logger.Level level, String message) {
        bus.log(level, message);
    }

    // ─── escribir ────────────────────────────────────────────────────────────

    /** {@code put} de un evento que nace de cero. Ver {@link #put(EventContext, String, String, Object, PutOptions)}. */
    public WriteResult put(String contract, String key, Object data) {
        return put(EventContext.ROOT, contract, key, data, new PutOptions());
    }

    public WriteResult put(String contract, String key, Object data, PutOptions options) {
        return put(EventContext.ROOT, contract, key, data, options);
    }

    /**
     * Escribe el valor actual de una clave. <b>Condicional</b>: no escribe una version que no avanza ({@code stale}, que NO es un
     * error) y publica con {@code Nats-Expected-Last-Subject-Sequence}, de modo que dos instancias del dueno no pisan un valor
     * nuevo con uno viejo (10-state.md §3.2).
     *
     * <p>{@code data} DEBE llevar {@code aggregateVersion} (entero &gt;= 1): lo pone el dueno, no el SDK.
     *
     * @param parent el evento en curso si se escribe desde un handler: el estado derivado hereda su tenant y su cadena de
     *               correlacion. {@link EventContext#ROOT} si nace de cero.
     */
    public WriteResult put(EventContext parent, String contract, String key, Object data, PutOptions options) {
        PutOptions po = options != null ? options : new PutOptions();
        EventContext ctx = parent != null ? parent : EventContext.ROOT;
        // §3.6, en este orden y SIN red: el primero que falla gana.
        StateEnvelope.ParsedContract c = StateEnvelope.parseContract(contract);
        StateEnvelope.assertKey(key);
        String tenant = tenantFor(po.tenantId, ctx);
        JsonNode dataNode = dataNode(data);
        long version = countingInvalid(c.contract(), () -> StateEnvelope.aggregateVersionOf(dataNode));
        String classification = StateEnvelope.assertClassification(po.classification == null ? null : po.classification.wire());

        String id = Protocol.uuidV7();
        String subject = StateEnvelope.subject(c.contract(), tenant, key);
        FluxEvent event = StateEnvelope.buildEvent(new StateEnvelope.EventInput()
                .contract(c.contract()).tenantId(tenant).key(key).id(id).source(bus.source)
                .time(Envelope.formatTime(po.time != null ? po.time : Instant.now()))
                // Un estado derivado dentro de un handler hereda el contexto del evento que lo causo.
                .correlationid(ctx.isRoot() ? id : ctx.correlationId())
                .producerversion(options().version)
                .classification(classification)
                .data(dataNode)
                .dataschema(schemaFor(c.contract()))
                .causationid(ctx.isRoot() ? null : ctx.causationId())
                .traceparent(traceparentFor(ctx))
                .tracestate(ctx.traceparent() != null ? ctx.tracestate() : null));
        // L3 ANTES de leer ni escribir; reporta TODOS los errores y no copia valores a su mensaje.
        countingInvalid(c.contract(), () -> validate(event, subject));
        return write(c, tenant, key, subject, version, event);
    }

    public WriteResult delete(String contract, String key, long aggregateVersion) {
        return delete(EventContext.ROOT, contract, key, aggregateVersion, new DeleteOptions());
    }

    public WriteResult delete(String contract, String key, long aggregateVersion, DeleteOptions options) {
        return delete(EventContext.ROOT, contract, key, aggregateVersion, options);
    }

    /**
     * Borra una clave: publica una <b>lapida</b> en su subject, que sustituye al valor (el dato personal ya no esta en el
     * stream). Es {@code put} de una lapida: mismo algoritmo, mismas reglas de version y mismo resultado. Borrar una clave que
     * nunca existio escribe la lapida (10-state.md §4).
     *
     * @param aggregateVersion la version DE LA LAPIDA: debe superar la del valor que borra.
     */
    public WriteResult delete(EventContext parent, String contract, String key, long aggregateVersion,
                              DeleteOptions options) {
        DeleteOptions dop = options != null ? options : new DeleteOptions();
        EventContext ctx = parent != null ? parent : EventContext.ROOT;
        StateEnvelope.ParsedContract c = StateEnvelope.parseContract(contract);
        StateEnvelope.assertKey(key);
        String tenant = tenantFor(dop.tenantId, ctx);
        long version = countingInvalid(c.contract(), () -> {
            JsonNode d = Envelope.mapper().createObjectNode().put("aggregateVersion", aggregateVersion);
            return StateEnvelope.aggregateVersionOf(d);
        });

        String id = Protocol.uuidV7();
        String subject = StateEnvelope.subject(c.contract(), tenant, key);
        FluxEvent event = StateEnvelope.buildTombstone(new StateEnvelope.TombstoneInput()
                .contract(c.contract()).tenantId(tenant).key(key).id(id).source(bus.source)
                .time(Envelope.formatTime(dop.time != null ? dop.time : Instant.now()))
                .correlationid(ctx.isRoot() ? id : ctx.correlationId())
                .producerversion(options().version)
                .aggregateVersion(version)
                .causationid(ctx.isRoot() ? null : ctx.causationId())
                .traceparent(traceparentFor(ctx))
                .tracestate(ctx.traceparent() != null ? ctx.tracestate() : null));
        // La lapida valida contra su esquema fijo (en `schemas` del bundle, no en el indice `state`).
        countingInvalid(c.contract(), () -> validate(event, subject));
        return write(c, tenant, key, subject, version, event);
    }

    /**
     * El algoritmo de 10-state.md §3.2. El evento se construyo UNA vez: {@code id}, {@code time} y bytes son los mismos en todos
     * los intentos. Con un {@code id} nuevo por intento, el paso 1b no reconoceria nunca la propia escritura.
     */
    WriteResult write(StateEnvelope.ParsedContract c, String tenant, String key, String subject, long version,
                              FluxEvent toSend) {
        String stream = StateEnvelope.streamName(c.domain());
        FluxEvent signed = bus.sign(toSend);
        byte[] bytes = Envelope.serialize(signed);
        String id = signed.id();

        for (int attempt = 1; attempt <= StateEnvelope.MAX_ATTEMPTS; attempt++) {
            // 1. Leer el ultimo mensaje de la clave. 404 → S = 0 y no hay version.
            Stored actual = readRaw(stream, subject);
            long s = actual == null ? 0 : actual.seq;

            // 1b. ¿El mensaje leido es EL MIO? Entonces ya se aplico y solo se perdio el acuse. Sin este paso, un acuse perdido
            // convierte una escritura buena en un `stale` con su propio dato: el reenvio da «wrong last sequence» porque lo que
            // cambio la secuencia fue el propio mensaje (A-33). Se reconoce por el `id` porque el broker conserva las cabeceras.
            if (actual != null && actual.ids.contains(id)) {
                bus.metrics.stateWrite(c.contract(), MetricsSink.StateWriteOutcome.OK);
                return new WriteResult(true, Outcome.OK, actual.seq);
            }

            // 2. Una version que no avanza NO se escribe, y una igual tampoco avanza. No es un error.
            if (actual != null && actual.version != null && version <= actual.version) {
                bus.metrics.stateWrite(c.contract(), MetricsSink.StateWriteOutcome.STALE);
                return new WriteResult(false, Outcome.STALE, actual.seq);
            }

            java.util.function.IntConsumer race = beforePublish;
            if (race != null) {
                race.accept(attempt);
            }

            // 3. Siempre por JetStream y siempre con la cabecera de secuencia: un publish de CORE a un subject de estado
            // sobrescribe sin acuse ni chequeo (A-45). `S = 0` = «la clave no existe»: crea si el subject no tiene mensaje y falla
            // si ya lo tiene, y eso es lo que hace que dos escritores concurrentes sobre una clave nueva no se pisen (A-33).
            try {
                final long expected = s;
                io.nats.client.api.PublishAck result = bus.listener.denials.race(options().service, subject, null,
                        () -> bus.jetStream.publishAsync(subject, bytes,
                                io.nats.client.PublishOptions.builder().messageId(id).expectedLastSubjectSequence(expected).build()),
                        5_000);
                bus.metrics.stateWrite(c.contract(), MetricsSink.StateWriteOutcome.OK);
                return new WriteResult(true, Outcome.OK, result.getSeqno());
            } catch (PermissionDeniedException denied) {
                throw denied;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FluxBus.FluxBusException("flux: escritura de estado interrumpida en \"" + subject + "\"", e);
            } catch (Exception e) {
                Throwable cause = e instanceof java.util.concurrent.ExecutionException && e.getCause() != null ? e.getCause() : e;
                // 4. Alguien escribio entre el 1 y el 3: volver a 1. DOS codigos, no uno.
                if (isSequenceConflict(cause)) {
                    // Un poco de jitter es opcional (§3.3) y barato: sin el, tres instancias que chocan vuelven a chocar a la vez.
                    if (attempt < StateEnvelope.MAX_ATTEMPTS) {
                        sleep(ThreadLocalRandom.current().nextInt(1, 12 * attempt + 1));
                    }
                    continue;
                }
                if (cause instanceof FluxErrors.FluxException fe) {
                    throw fe;
                }
                bus.metrics.stateWrite(c.contract(), MetricsSink.StateWriteOutcome.CONFLICT);
                throw new FluxBus.FluxBusException("flux: fallo al escribir el estado \"" + subject + "\"", cause);
            }
        }
        bus.metrics.stateWrite(c.contract(), MetricsSink.StateWriteOutcome.CONFLICT);
        throw new StateConflictException(c.contract(), key, StateEnvelope.MAX_ATTEMPTS);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ─── leer ────────────────────────────────────────────────────────────────

    public Optional<Entry> get(String contract, String key) {
        return get(EventContext.ROOT, contract, key, new GetOptions());
    }

    public Optional<Entry> get(String contract, String key, GetOptions options) {
        return get(EventContext.ROOT, contract, key, options);
    }

    /**
     * Una lectura puntual por {@code DIRECT.GET} sobre el subject exacto: no hay consumidor ni acuse. Vacio si la clave no existe
     * <b>o</b> es una lapida. Es una lectura de REPLICA (10-state.md §3.7): con {@code replicas: 3} PUEDE devolver el valor
     * anterior justo despues de un {@code put}; quien necesite su propia escritura la deriva de {@code revision}, no relee.
     */
    public Optional<Entry> get(EventContext parent, String contract, String key, GetOptions options) {
        GetOptions go = options != null ? options : new GetOptions();
        StateEnvelope.ParsedContract c = StateEnvelope.parseContract(contract);
        StateEnvelope.assertKey(key);
        String tenant = tenantFor(go.tenantId, parent != null ? parent : EventContext.ROOT);
        String subject = StateEnvelope.subject(c.contract(), tenant, key);
        Stored stored = readRaw(StateEnvelope.streamName(c.domain()), subject);
        if (stored == null) {
            return Optional.empty(); // 404 `Message Not Found`
        }
        Decoded d = decode(stored.data, c.contract(), tenant, key, stored.seq, subject, null);
        return d.entry.deleted() ? Optional.empty() : Optional.of(d.entry);
    }

    private static final class Stored {
        final long seq;
        final byte[] data;
        /** El {@code id} del sobre, o el de la cabecera {@code Nats-Msg-Id} (el broker las conserva: A-32). */
        final List<String> ids = new ArrayList<>();
        /** Su version, si el cuerpo es legible y la tiene valida. */
        Long version;

        Stored(long seq, byte[] data) {
            this.seq = seq;
            this.data = data;
        }
    }

    /**
     * {@code DIRECT.GET} por subject — la forma por SUBJECT, que se puede confinar por tenant con una ACL. La de cuerpo
     * ({@code {"seq": n}}) lee cualquier mensaje y ninguna ACL la ve: no se usa.
     *
     * <p>Un stream que no existe, o sin {@code allow_direct}, NO da 404: no contesta nadie, y los dos casos se ven igual (A-32)
     * → {@code NOT_PROVISIONED}, con las dos causas en el mensaje.
     */
    private Stored readRaw(String stream, String subject) {
        String api = "$JS.API.DIRECT.GET." + stream + "." + subject;
        Message r;
        try {
            r = bus.listener.denials.race(options().service, api, null, () -> bus.connection.request(api, new byte[0]),
                    DIRECT_GET_TIMEOUT_MILLIS);
        } catch (PermissionDeniedException denied) {
            throw denied;
        } catch (CancellationException noResponders) {
            // `no responders`: nadie contesta. Se pidio con una limpieza de 60 s, asi que no es un timeout de la libreria.
            throw notProvisioned(stream, api, noResponders);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FluxBus.FluxBusException("flux: lectura de estado interrumpida en \"" + subject + "\"", e);
        } catch (TimeoutException e) {
            throw new FluxBus.FluxBusException("flux: DIRECT.GET de \"" + subject + "\" no contesto en "
                    + DIRECT_GET_TIMEOUT_MILLIS + " ms", e);
        } catch (Exception e) {
            throw new FluxBus.FluxBusException("flux: fallo al leer el estado \"" + subject + "\"", e);
        }
        if (r.isStatusMessage()) {
            if (r.getStatus().getCode() == 404) {
                return null;
            }
            throw new FluxBus.FluxBusException("flux: DIRECT.GET de \"" + subject + "\" contesto " + r.getStatus().getCode() + " "
                    + r.getStatus().getMessage());
        }
        long seq = r.getHeaders() != null && r.getHeaders().getFirst("Nats-Sequence") != null
                ? Long.parseLong(r.getHeaders().getFirst("Nats-Sequence")) : 0;
        Stored stored = new Stored(seq, r.getData());
        String headerId = r.getHeaders() == null ? null : r.getHeaders().getFirst("Nats-Msg-Id");
        if (headerId != null) {
            stored.ids.add(headerId);
        }
        // Un valor ILEGIBLE no bloquea la clave (§3.3): si el cuerpo no es JSON, o no tiene un `aggregateVersion` valido, se trata
        // como «sin version» y se sobrescribe con el chequeo de secuencia sobre SU `S`. De lo contrario una entrada rota dejaria la
        // clave inescribible para siempre, porque ningun servicio puede purgar el stream (06-security.md §3.5).
        try {
            JsonNode ev = Envelope.mapper().readTree(r.getData());
            if (ev != null && ev.isObject()) {
                if (ev.path("id").isTextual()) {
                    stored.ids.add(ev.get("id").textValue());
                }
                JsonNode v = ev.path("data").path("aggregateVersion");
                if (StateEnvelope.isValidAggregateVersion(v)) {
                    stored.version = v.longValue();
                }
            }
        } catch (IOException | RuntimeException e) {
            // ilegible: sin version
        }
        return stored;
    }

    /**
     * Nadie contesta: el stream no existe, <b>o</b> existe sin {@code allow_direct}, y los dos casos se ven exactamente igual (A-32).
     * El mensaje DEBE nombrar las dos causas y apuntar a {@code flux provision} (lo crea) y a {@code flux doctor} (lo audita).
     */
    private NotProvisionedException notProvisioned(String stream, String noReply, Throwable cause) {
        String service = options().service;
        return new NotProvisionedException(ResourceKind.STREAM, stream, null, service, noReply,
                "nadie contesto en \"" + noReply + "\": el stream \"" + stream + "\" no existe, o existe sin `allow_direct: true`, y los "
                        + "dos casos se ven exactamente igual (no hay un 404: no responde nadie). \"" + service + "\" no lo crea ni lo "
                        + "consulta —los SDKs no usan `STREAM.INFO` sobre el estado, porque lista todas las claves y todos los tenants "
                        + "(A-39)—. Aprovisionalo con `flux provision`, con credenciales de operacion, y compruebalo con `flux doctor` "
                        + "(10-state.md §5.1, 04-errors.md §6).", cause);
    }

    // ─── observar ────────────────────────────────────────────────────────────

    public Subscription watch(String contract, Handler handler) {
        return watch(contract, handler, new WatchOptions());
    }

    /**
     * Foto completa y despues los cambios, por el MISMO canal: un consumidor durable de {@code STATE_<DOM>} con
     * {@code deliver_policy: last_per_subject}. Un consumidor que estuvo caido recibe solo el ultimo valor de cada clave; un valor
     * reemplazado mientras estaba en vuelo NO se reentrega (A-35, A-36).
     */
    public Subscription watch(String contract, Handler handler, WatchOptions options) {
        WatchOptions wo = options != null ? options : new WatchOptions();
        StateEnvelope.ParsedContract c = StateEnvelope.parseContract(contract);
        FluxBus.ConnectOptions o = options();
        String tenant = watchTenant(wo.tenantId);
        if (o.tenantIsolation == FluxBus.TenantIsolation.STRICT && tenant == null) {
            throw new TenantIsolationException("state." + c.contract(), null, o.tenantId);
        }
        String stream = StateEnvelope.streamName(c.domain());
        String durable = StateEnvelope.durableName(o.service, c.contract(), tenant);
        Watch w = new Watch(c.contract(), tenant, stream, durable, StateEnvelope.durableName(o.service, c.contract(), null),
                "state." + c.contract(), handler, wo.onSynced, c.domain());
        try {
            // El SDK comprueba la DLQ al ARRANCAR, no cuando llega el primer mensaje envenenado.
            ensureStateDlq(c.domain());

            ConsumerConfiguration requested = ConsumerConfiguration.builder()
                    .durable(durable)
                    .filterSubject(StateEnvelope.filterSubject(c.contract(), tenant))
                    .ackPolicy(AckPolicy.Explicit)
                    // ack_wait DEBE ser backoff[0]: JetStream lo sobrescribe con ese valor sin avisar.
                    .ackWait(Protocol.DEFAULT_ACK_WAIT)
                    .maxDeliver(Protocol.DEFAULT_MAX_DELIVER)
                    .backoff(Protocol.canonicalBackoff().toArray(new Duration[0]))
                    .maxAckPending(Protocol.DEFAULT_MAX_ACK_PENDING)
                    .deliverPolicy(DeliverPolicy.LastPerSubject)
                    .replayPolicy(ReplayPolicy.Instant)
                    .build();
            w.requested = requested;

            ConsumerInfo found = bus.ensureConsumer(stream, durable, StateEnvelope.filterSubject(c.contract(), tenant), requested);
            assertStateConfigHonored(durable, requested, found.getConsumerConfiguration());

            String pull = "$JS.API.CONSUMER.MSG.NEXT." + stream + "." + durable;
            bus.listener.denials.forget(pull);
            bus.ensurePullAllowed(stream, durable);

            // Stream vacio, o durable al dia: `onSynced` de INMEDIATO, antes de entregar nada y aunque no llegue ninguna entrada
            // despues. Esperar a una que quiza no llegue nunca dejaria al servicio «sincronizando» para siempre (10-state.md §5.5,
            // punto 2). `num_ack_pending` tambien: lo entregado y sin confirmar sigue pendiente de procesar.
            // Se decide ANTES de abrir el consumo: nada se ha entregado todavia.
            if (found.getNumPending() == 0 && found.getNumAckPending() == 0) {
                w.sync.immediate();
            }
            // Una suscripcion de PULL ligada al durable (`fastBind`): ni crea ni consulta nada. El consumo simplificado de la libreria
            // (`getConsumerContext`) pide `STREAM.INFO.STATE_<DOM>` para construirse, que lista todas las claves y todos los tenants y
            // NINGUN observador tiene concedido (A-39): bajo los permisos de produccion el watch no arrancaria (A-206).
            w.sub = bus.jetStream.subscribe(null, PullSubscribeOptions.fastBind(stream, durable));
            w.start();
            long poll = options().pendingPollMillis;
            ScheduledExecutorService scheduler = bus.pendingScheduler;
            if (scheduler != null && poll > 0) {
                w.poll = scheduler.scheduleAtFixedRate(() -> {
                    try {
                        notePending(w, bus.jsm.getConsumerInfo(stream, durable).getCalculatedPending());
                    } catch (Exception | Error e) {
                        // Un fallo del sondeo NO debe afectar al consumo: es telemetria.
                    }
                }, poll, poll, TimeUnit.MILLISECONDS);
            }
        } catch (RuntimeException | InterruptedException | IOException | JetStreamApiException e) {
            // Un watch que no llega a abrirse no debe quedarse consumiendo a espaldas de quien recibe el error.
            w.stop();
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new FluxBus.FluxBusException("flux: no se pudo abrir el watch de \"" + c.contract() + "\"", e);
        }
        watches.add(w);
        return new Subscription(w);
    }

    /**
     * Como {@code FluxBus.assertConfigHonored}, mas lo propio del estado: la politica de entrega y el filtro. Un durable
     * aprovisionado con otro filtro entregaria un conjunto distinto del pedido (otro tenant, todos los tenants) sin ningun error.
     */
    private static void assertStateConfigHonored(String durable, ConsumerConfiguration requested, ConsumerConfiguration effective) {
        FluxBus.assertConfigHonored(durable, requested, effective, extraDifferences(requested, effective));
    }

    private static List<ConsumerConfigMismatchException.Difference> extraDifferences(ConsumerConfiguration requested,
                                                                                    ConsumerConfiguration effective) {
        List<ConsumerConfigMismatchException.Difference> diffs = new ArrayList<>();
        if (requested.getDeliverPolicy() != effective.getDeliverPolicy()) {
            diffs.add(new ConsumerConfigMismatchException.Difference("deliver_policy", requested.getDeliverPolicy(),
                    effective.getDeliverPolicy()));
        }
        List<String> filters = effective.getFilterSubjects();
        String eff = effective.getFilterSubject() != null ? effective.getFilterSubject()
                : filters != null && filters.size() == 1 ? filters.get(0) : null;
        if (!java.util.Objects.equals(requested.getFilterSubject(), eff) || (filters != null && filters.size() > 1)) {
            diffs.add(new ConsumerConfigMismatchException.Difference("filter_subject", requested.getFilterSubject(),
                    eff != null ? eff : filters));
        }
        return diffs;
    }

    /**
     * {@code DLQ_<DOM>} debe capturar {@code dlq.state.<dom>.>}. Un dominio solo de eventos conserva su {@code DLQ_<DOM>} de la v1.0
     * sin tocarlo: ampliarlo es posible y no pierde mensajes (A-44), pero no se le pide a quien no usa estado, y NO lo hace el SDK
     * sobre uno existente: lo hace {@code flux provision}. Si no captura, se AVISA al abrir, y no cuando el primer mensaje muera.
     */
    private void ensureStateDlq(String domain) throws IOException, JetStreamApiException {
        String name = Protocol.dlqStreamName(domain);
        if (dlqChecked.contains(name)) {
            return;
        }
        String sample = "dlq.state." + domain + ".x.v1";
        try {
            io.nats.client.api.StreamInfo info = bus.api(List.of("$JS.API.STREAM.INFO." + name), null,
                    () -> bus.jsm.getStreamInfo(name));
            boolean captures = false;
            for (String pattern : info.getConfiguration().getSubjects()) {
                if (subjectMatches(pattern, sample)) {
                    captures = true;
                }
            }
            if (!captures) {
                log(System.Logger.Level.WARNING, name + " no captura \"dlq.state." + domain + ".>\": una entrada de estado que muera "
                        + "no se podra guardar y se reentregara. Un dominio con estado lo necesita; lo amplia `flux provision` y lo "
                        + "audita `flux doctor` (10-state.md §6).");
            }
        } catch (JetStreamApiException e) {
            if (e.getApiErrorCode() != JS_STREAM_NOT_FOUND) {
                throw e;
            }
            // Desarrollo: se crea con LOS DOS subjects. En produccion lo aprovisiona `flux provision`.
            try {
                StreamConfiguration config = StreamConfiguration.builder().name(name)
                        .subjects("dlq." + domain + ".>", "dlq.state." + domain + ".>")
                        .storageType(StorageType.File).retentionPolicy(RetentionPolicy.Limits)
                        .discardPolicy(DiscardPolicy.Old).replicas(options().streamReplicas)
                        .maxAge(Protocol.DLQ_STREAM_MAX_AGE).build();
                bus.api(List.of("$JS.API.STREAM.CREATE." + name), null, () -> bus.jsm.addStream(config));
            } catch (PermissionDeniedException denied) {
                throw new NotProvisionedException(ResourceKind.STREAM, name, null, options().service, denied.subject(), null, denied);
            }
        }
        dlqChecked.add(name);
    }

    /** Un patron de subject de NATS ({@code *}, {@code >}) contra un subject concreto. */
    static boolean subjectMatches(String pattern, String subject) {
        String[] p = pattern.split("\\.", -1);
        String[] s = subject.split("\\.", -1);
        for (int i = 0; i < p.length; i++) {
            if (p[i].equals(">")) {
                return s.length > i;
            }
            if (i >= s.length) {
                return false;
            }
            if (!p[i].equals("*") && !p[i].equals(s[i])) {
                return false;
            }
        }
        return p.length == s.length;
    }

    // ─── el consumo de un watch ──────────────────────────────────────────────

    /** Cuenta cuantas entregas de un {@code watch} siguen en vuelo y cuando se alcanzo «lo que habia al abrir». */
    static final class SyncTracker {
        private boolean done;
        private int inflight;
        private boolean sawLast;
        private final Runnable fire;

        SyncTracker(Runnable fire) {
            this.fire = fire;
        }

        /** Al abrir: nada pendiente. */
        void immediate() {
            boolean fireNow;
            synchronized (this) {
                fireNow = !done;
                done = true;
            }
            if (fireNow) {
                fire.run();
            }
        }

        /** {@code pending}: el numero que el broker pone en los metadatos de cada entrega. 0 = esta es la ultima de la foto (A-35). */
        void started(long pending) {
            synchronized (this) {
                inflight++;
                if (pending == 0) {
                    sawLast = true;
                }
            }
        }

        /** Con exito o con error: una entrada RETRYABLE que falla NO retrasa {@code onSynced} (10-state.md §5.5). */
        void finished() {
            boolean fireNow = false;
            synchronized (this) {
                inflight--;
                if (!done && sawLast && inflight == 0) {
                    done = true;
                    fireNow = true;
                }
            }
            if (fireNow) {
                fire.run();
            }
        }
    }

    /** {@code flux_consumer_pending} de un contrato es la SUMA de los durables del servicio (un durable por tenant), no el ultimo. */
    static final class PendingSum {
        private final Map<String, Map<String, Long>> bySeries = new HashMap<>();

        synchronized long note(String series, String durable, long pending) {
            Map<String, Long> d = bySeries.computeIfAbsent(series, k -> new HashMap<>());
            d.put(durable, pending);
            return d.values().stream().mapToLong(Long::longValue).sum();
        }

        /** Un durable deja de sumar. Devuelve la suma que queda, o {@code null} si no habia nada. */
        synchronized Long drop(String series, String durable) {
            Map<String, Long> d = bySeries.get(series);
            if (d == null || d.remove(durable) == null) {
                return null;
            }
            if (d.isEmpty()) {
                bySeries.remove(series);
            }
            return d.values().stream().mapToLong(Long::longValue).sum();
        }
    }

    private void notePending(Watch w, long pendingCount) {
        long sum = pending.note(w.subjectLabel + "|" + w.consumerLabel, w.durable, pendingCount);
        bus.metrics.consumerPending(w.subjectLabel, w.consumerLabel, sum);
    }

    private final class Watch {
        final String contract;
        final String tenant;
        final String stream;
        final String durable;
        /** El durable SIN el sufijo de tenant: la etiqueta {@code consumer} de las metricas (08-observability.md §2.4). */
        final String consumerLabel;
        /** {@code state.<contrato>}: la etiqueta {@code subject} de las metricas. */
        final String subjectLabel;
        final Handler handler;
        final String domain;
        final SyncTracker sync;
        volatile ConsumerConfiguration requested;
        volatile JetStreamSubscription sub;
        volatile Thread loop;
        volatile ScheduledFuture<?> poll;
        volatile boolean stopped;

        Watch(String contract, String tenant, String stream, String durable, String consumerLabel, String subjectLabel,
              Handler handler, Runnable onSynced, String domain) {
            this.contract = contract;
            this.tenant = tenant;
            this.stream = stream;
            this.durable = durable;
            this.consumerLabel = consumerLabel;
            this.subjectLabel = subjectLabel;
            this.handler = handler;
            this.domain = domain;
            this.sync = new SyncTracker(() -> fireSynced(onSynced));
        }

        void start() {
            Thread t = new Thread(this::run, "flux-state-watch-" + durable);
            t.setDaemon(true);
            loop = t;
            t.start();
        }

        void stop() {
            stopped = true;
            ScheduledFuture<?> p = poll;
            if (p != null) {
                p.cancel(false);
            }
            JetStreamSubscription s = sub;
            if (s != null) {
                try {
                    s.unsubscribe();
                } catch (RuntimeException ignored) {
                    // la conexion ya esta cerrada
                }
            }
            Thread t = loop;
            if (t != null && t != Thread.currentThread()) {
                t.interrupt();
                try {
                    t.join(1_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            Long sum = pending.drop(subjectLabel + "|" + consumerLabel, durable);
            if (sum != null) {
                bus.metrics.consumerPending(subjectLabel, consumerLabel, sum);
            }
        }

        /**
         * El bucle del consumo: pide un lote y entrega lo que llega, en orden y de uno en uno. Un lote que expira sin mensajes
         * (silencio normal) se vuelve a pedir; si el DURABLE se borro (`flux state resync`: borrar y recrear), la libreria lo dice con
         * una excepcion en ESE momento —`409 Consumer Deleted` a los ~25 ms, y `503 No Responders` mientras no exista— y se reabre.
         */
        private void run() {
            while (!stopped) {
                try {
                    Iterator<Message> batch = sub.iterate(PULL_BATCH, PULL_EXPIRES);
                    while (!stopped && batch.hasNext()) {
                        dispatch(batch.next());
                    }
                } catch (JetStreamStatusException status) {
                    if (stopped) {
                        return;
                    }
                    reopen(status);
                } catch (RuntimeException e) {
                    if (stopped) {
                        return;
                    }
                    // Una excepcion inesperada NO debe dejar al watch muerto con la conexion viva.
                    log(System.Logger.Level.ERROR, "el consumo de " + subjectLabel + " fallo: " + e + ". Se reintenta.");
                    sleep(500);
                }
            }
        }

        /**
         * El durable se borro con el watch abierto. Se vuelve a consultar por su nombre —{@code CONSUMER.INFO}; NO hace falta
         * {@code CONSUMER.CREATE}— con backoff (100 ms … 5 s) hasta que exista, se COMPRUEBA su configuracion como al abrir (un durable
         * recreado con otro filtro entregaria las claves de otro tenant sin ningun error, y ese no se consume) y se vuelve a ligar.
         * {@code onSynced} no se invoca de nuevo.
         */
        private void reopen(JetStreamStatusException cause) {
            log(System.Logger.Level.WARNING, "el durable " + durable + " se borro con el watch de " + subjectLabel + " abierto "
                    + "(¿`flux state resync`?; el broker contesto " + cause.getStatus().getCode() + " " + cause.getStatus().getMessage()
                    + "): se vuelve a abrir el consumo cuando exista de nuevo.");
            String last = "";
            for (int attempt = 1; !stopped; attempt++) {
                try {
                    ConsumerInfo info = bus.api(List.of("$JS.API.CONSUMER.INFO." + stream + "." + durable), null,
                            () -> bus.jsm.getConsumerInfo(stream, durable));
                    assertStateConfigHonored(durable, requested, info.getConsumerConfiguration());
                    JetStreamSubscription old = sub;
                    sub = bus.jetStream.subscribe(null, PullSubscribeOptions.fastBind(stream, durable));
                    if (old != null) {
                        try {
                            old.unsubscribe();
                        } catch (RuntimeException ignored) {
                            // ya cerrada
                        }
                    }
                    log(System.Logger.Level.WARNING, "consumo de " + durable + " reabierto (intento " + attempt
                            + "): se entregara la foto otra vez.");
                    return;
                } catch (Exception e) {
                    // `consumer not found` (10014): el resync esta entre el borrado y la creacion. Se espera. Se avisa la primera vez,
                    // cada 10 intentos y SIEMPRE que la causa cambia (un durable recreado con otra config no es «aun no existe»).
                    String reason = e.getMessage() == null ? e.toString() : e.getMessage();
                    if (attempt == 1 || attempt % 10 == 0 || !reason.equals(last)) {
                        log(System.Logger.Level.WARNING, "no se pudo reabrir el consumo de " + durable + " (intento " + attempt + "): "
                                + reason + ". Se reintenta.");
                        last = reason;
                    }
                    sleep(Math.min(5_000, 100L << Math.min(attempt - 1, 6)));
                }
            }
            log(System.Logger.Level.WARNING, "el watch de " + subjectLabel + " se detuvo sin haber podido reabrir el consumo de " + durable);
        }

        void dispatch(Message msg) {
            long delivered = 1;
            long pendingCount = -1;
            long revision = 0;
            try {
                io.nats.client.impl.NatsJetStreamMetaData meta = msg.metaData();
                delivered = Math.max(1, meta.deliveredCount());
                pendingCount = meta.pendingCount();
                revision = meta.streamSequence();
            } catch (RuntimeException e) {
                log(System.Logger.Level.WARNING, "mensaje sin metadatos de JetStream en " + subjectLabel);
            }
            if (pendingCount >= 0) {
                notePending(this, pendingCount);
            }
            sync.started(pendingCount);
            try {
                process(msg, (int) delivered, revision);
            } catch (Throwable t) {
                // Un fallo inesperado del despacho rompe la entrega EN SILENCIO con la conexion viva: un consumidor muerto que parece
                // vivo. El mensaje no se confirma ni se termina: se deja reentregar.
                log(System.Logger.Level.ERROR, "fallo inesperado despachando el estado " + subjectLabel + ": " + t
                        + ". El mensaje no se confirma y sera reentregado.");
            } finally {
                sync.finished();
            }
        }

        private void process(Message msg, int attempt, long revision) {
            FluxBus.ConnectOptions o = options();
            int maxAttempts = Protocol.DEFAULT_MAX_DELIVER;
            // `state.<dom>.<agr>.v<M>.<tenant>.<clave>`: la identidad la decide EL BROKER, no el sobre.
            String[] parts = msg.getSubject().split("\\.", -1);
            String msgTenant = parts[4];
            String key = parts[5];
            long start = System.nanoTime();
            Decoded[] decoded = new Decoded[1];

            // WIP mientras el handler vive: evita que la misma entrada se ejecute en concurrencia consigo misma.
            long wipPeriod = Protocol.DEFAULT_ACK_WAIT.toMillis() / 2;
            ScheduledFuture<?> wip = bus.wipScheduler.scheduleAtFixedRate(() -> {
                try {
                    msg.inProgress();
                } catch (RuntimeException ignored) {
                    // el mensaje ya esta resuelto
                }
            }, wipPeriod, wipPeriod, TimeUnit.MILLISECONDS);

            try {
                try {
                    decoded[0] = decode(msg.getData(), contract, msgTenant, key, revision, msg.getSubject(), this);
                    if (decoded[0].signatureWarning) {
                        bus.metrics.eventConsumed(subjectLabel, consumerLabel, MetricsSink.ConsumeOutcome.INVALID_SIGNATURE);
                    }
                    handler.handle(EventContext.fromEvent(decoded[0].event), decoded[0].entry,
                            new Delivery(attempt, maxAttempts, msg.getSubject(), contract));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
                msg.ack();
                bus.metrics.eventConsumed(subjectLabel, consumerLabel, MetricsSink.ConsumeOutcome.OK);
                bus.metrics.handlerDuration(subjectLabel, consumerLabel, (System.nanoTime() - start) / 1e9);
            } catch (Throwable e) {
                fail(msg, e, attempt, maxAttempts, msgTenant, key, revision, start, decoded[0] == null ? null : decoded[0].event, o);
            } finally {
                wip.cancel(false);
            }
        }

        private void fail(Message msg, Throwable e, int attempt, int maxAttempts, String msgTenant, String key, long revision,
                          long start, FluxEvent parsed, FluxBus.ConnectOptions o) {
            Classification c = bus.classifier.classify(e);
            int budget = FluxBus.effectiveBudget(maxAttempts, c.maxAttempts());

            if (c.errorClass() == ErrorClass.RETRYABLE && attempt < budget) {
                Duration delay = c.retryAfter() != null ? c.retryAfter()
                        : Protocol.canonicalBackoff().get(Math.min(attempt - 1, Protocol.canonicalBackoff().size() - 1));
                log(System.Logger.Level.WARNING, "RETRYABLE " + c.code() + " en " + subjectLabel + " (clave " + key + ", intento "
                        + attempt + "/" + budget + "), reintento en " + delay);
                bus.metrics.eventRetried(subjectLabel, consumerLabel, attempt);
                msg.nakWithDelay(delay);
                return;
            }

            FluxEvent.DlqReason reason = c.errorClass() == ErrorClass.POISON ? FluxEvent.DlqReason.POISON
                    : c.errorClass() == ErrorClass.RETRYABLE ? FluxEvent.DlqReason.RETRYABLE : FluxEvent.DlqReason.PERMANENT;
            MetricsSink.ConsumeOutcome outcome = FluxBus.consumeOutcome(reason, c.code());

            // `dlqerror` NO DEBE contener valores de `data` (10-state.md §6). Un error de flux lleva su codigo y un mensaje que no
            // cita valores. Uno que no sea de flux —una excepcion de un driver de base de datos, que puede citar la fila entera— se
            // reduce a su codigo: la DLQ guarda 90 dias y en estado `data` es dato personal. Tampoco se concatena la cadena de causas.
            String dlqerror = e instanceof FluxErrors.FluxException fe ? c.code() + ": " + fe.getMessage()
                    : c.code() + ": " + StateEnvelope.UNCLASSIFIED_DLQ_ERROR;
            String dlqtime = Envelope.formatTime(Instant.now());
            FluxEvent dlqEvent;
            try {
                // Un mensaje ilegible no se copia; el resto, su sobre ORIGINAL sin el dato.
                dlqEvent = parsed != null
                        ? StateEnvelope.toDlqEvent(parsed, new StateEnvelope.DlqInfo(key, revision, reason, attempt, durable, dlqerror, dlqtime))
                        : StateEnvelope.buildUnreadableDlqEvent(contract, msgTenant, key, revision, bus.source, o.version,
                                Protocol.uuidV7(), Envelope.formatTime(Instant.now()), schemaForSafe(contract), reason, attempt, durable,
                                dlqerror, dlqtime);
                bus.jetStream.publish(StateEnvelope.dlqSubject(contract), Envelope.serialize(dlqEvent),
                        io.nats.client.PublishOptions.builder().messageId("dlq-" + dlqEvent.id() + "-" + durable).build());
            } catch (Exception dlqError) {
                // NO se hace term(): terminar aqui borraria la entrada sin haberla guardado en ningun sitio.
                log(System.Logger.Level.ERROR, "no se pudo publicar en la DLQ de estado para " + subjectLabel + " (clave " + key + "): "
                        + dlqError + ". El mensaje NO se termina; se reentregara.");
                return;
            }

            if (c.errorClass() == ErrorClass.POISON) {
                // POISON es lo que despierta a alguien: term() + DLQ + ALERTA. `raw` va vacio a proposito: el cuerpo de una entrada de
                // estado puede llevar datos personales (10-state.md §6).
                if (o.onPoison != null) {
                    o.onPoison.onPoison(new FluxBus.PoisonInfo(msg.getSubject(), e, new byte[0]));
                }
                log(System.Logger.Level.ERROR, "POISON " + c.code() + " en " + subjectLabel + " (clave " + key + "): "
                        + (e instanceof FluxErrors.FluxException fe ? fe.getMessage() : c.code()));
            } else {
                log(System.Logger.Level.ERROR, "DLQ de estado (" + reason.wire() + ") " + c.code() + " en " + subjectLabel + " (clave "
                        + key + ") tras " + attempt + " intento(s)");
            }
            bus.metrics.eventConsumed(subjectLabel, consumerLabel, outcome);
            bus.metrics.eventDlq(subjectLabel, consumerLabel, reason, c.code());
            bus.metrics.handlerDuration(subjectLabel, consumerLabel, (System.nanoTime() - start) / 1e9);
            if (o.onDlq != null) {
                o.onDlq.onDlq(new FluxBus.DlqEventInfo(msg.getSubject(), dlqEvent, c));
            }
            msg.term();
        }
    }

    private void fireSynced(Runnable callback) {
        if (callback == null) {
            return;
        }
        try {
            callback.run();
        } catch (RuntimeException e) {
            log(System.Logger.Level.ERROR, "onSynced lanzo: " + e);
        }
    }

    /** Para todos los {@code watch}: lo que hace {@code bus.close()} ANTES de cerrar la conexion. */
    void stopAll() {
        for (Watch w : new ArrayList<>(watches)) {
            w.stop();
        }
        watches.clear();
    }

    // ─── lo que comprueba el SDK al consumir ─────────────────────────────────

    private static final class Decoded {
        FluxEvent event;
        Entry entry;
        boolean signatureWarning;
    }

    /**
     * Lo que comprueba el SDK al consumir, en el orden de 10-state.md §5.4. Lo que falla NUNCA se aplica y NUNCA llega al handler.
     */
    private Decoded decode(byte[] bytes, String contract, String tenant, String key, long revision, String subject, Watch w) {
        // 1. Un CloudEvent valido con las cuatro extensiones (POISON, los codigos de siempre).
        FluxEvent event = Envelope.parseEvent(bytes);

        // 2. La identidad esta en el subject; el sobre la repite. Si discrepan, uno miente: POISON.
        if (!tenant.equals(event.tenantid())) {
            throw new StateSubjectMismatchException("tenantid", event.tenantid(), tenant);
        }
        if (!key.equals(event.aggregateId())) {
            throw new StateSubjectMismatchException("subject", event.aggregateId(), key);
        }
        String put = StateEnvelope.type(contract);
        boolean deleted = event.type().equals(StateEnvelope.tombstoneType(contract));
        if (!deleted && !event.type().equals(put)) {
            throw new StateSubjectMismatchException("type", event.type(), put);
        }

        // 3. `restricted` no se admite en estado: en consumo es POISON.
        if (event.dataclassification() == FluxEvent.DataClassification.RESTRICTED) {
            throw new RestrictedStateEntryException();
        }

        // 4. `aggregateVersion` valido: sin el no se puede aplicar de forma condicional (PERMANENT).
        long aggregateVersion = StateEnvelope.aggregateVersionOf(event.data());

        Decoded d = new Decoded();
        d.event = event;
        // La firma antes que el esquema: un evento manipulado puede validar perfectamente y no ser del productor que dice
        // (07-signing.md §5). `warn` anota el fallo y sigue; `require` lanza.
        d.signatureWarning = bus.verify(event);

        // 5. Si hay L3 al consumir: `data` contra su esquema.
        if (bus.validateOnConsume) {
            validate(event, subject);
        }
        d.entry = new Entry(key, tenant, deleted, deleted ? null : event.data(), aggregateVersion, revision);
        return d;
    }

    // ─── tenant, esquema y metricas ──────────────────────────────────────────

    /** Escritura y lectura: la opcion de la llamada, el evento en curso y {@code connect()}. Sin ninguno, error. */
    private String tenantFor(String option, EventContext ctx) {
        String t = option != null ? option : firstNonEmpty(ctx.tenantId(), options().tenantId);
        if (t == null) {
            throw new InvalidTenantForStateException(null);
        }
        StateEnvelope.assertTenant(t);
        return t;
    }

    /**
     * {@code watch}: la opcion y {@code connect()}, SIN el evento en curso (un {@code watch} no corre dentro de un handler:
     * 10-state.md §2.4). Sin ninguno es legitimo (se observan todos), salvo {@code strict}.
     */
    private String watchTenant(String option) {
        String t = option != null ? option : emptyToNull(options().tenantId);
        if (t == null) {
            return null;
        }
        StateEnvelope.assertTenant(t);
        return t;
    }

    private static String firstNonEmpty(String a, String b) {
        return a != null && !a.isEmpty() ? a : emptyToNull(b);
    }

    private static String emptyToNull(String v) {
        return v == null || v.isEmpty() ? null : v;
    }

    /** L3, si esta configurada: lanza en {@code strict}, registra en {@code warn}. */
    private void validate(FluxEvent event, String subject) {
        if (bus.validator != null) {
            bus.validator.check(event, subject);
        }
    }

    private String traceparentFor(EventContext ctx) {
        if (ctx.traceparent() != null) {
            return ctx.traceparent();
        }
        EventContext.TraceparentSupplier supplier = options().traceparentSupplier;
        return supplier == null ? null : emptyToNull(supplier.get());
    }

    private JsonNode dataNode(Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof JsonNode n) {
            return n;
        }
        try {
            return Envelope.mapper().valueToTree(data);
        } catch (IllegalArgumentException e) {
            throw new Envelope.EnvelopeException("`data` no es serializable a JSON: " + e.getMessage(), e);
        }
    }

    /** El {@code dataschema} del contrato: mapa explicito, indice {@code state} del bundle o {@code schemaBaseUrl}. Nunca por red. */
    private String schemaFor(String contract) {
        FluxBus.ConnectOptions o = options();
        String explicit = o.schemas.get(contract);
        if (explicit != null && !explicit.isEmpty()) {
            return explicit;
        }
        if (o.validation != null && o.validation.bundle() != null) {
            String fromBundle = o.validation.bundle().stateSchemaUriFor(contract);
            if (fromBundle != null) {
                return fromBundle;
            }
        }
        if (o.schemaBaseUrl == null || o.schemaBaseUrl.isEmpty()) {
            throw new FluxBus.FluxBusException("flux: no hay dataschema para el estado \"" + contract + "\". Declara schema(\"" + contract
                    + "\", …), schemaBaseUrl, o pasa un bundle con el indice `state` en validation(...) (ConnectOptions)");
        }
        return StateEnvelope.dataschema(contract, o.schemaBaseUrl);
    }

    /** Para la DLQ: si no se puede resolver, el default del repo; un fallo de configuracion no debe perder el aviso. */
    private String schemaForSafe(String contract) {
        try {
            return schemaFor(contract);
        } catch (RuntimeException e) {
            return StateEnvelope.dataschema(contract, StateEnvelope.SCHEMA_BASE);
        }
    }

    /**
     * {@code invalid_schema} se emite tambien cuando la llamada lanza: una excepcion sirve a quien la provoca, y la metrica a quien
     * opera el ecosistema. Es todo fallo de L3 —tambien el esquema ausente del bundle en modo estricto— y
     * {@code MISSING_AGGREGATE_VERSION}. Los demas errores de la llamada (contrato, clave, tenant, dominio reservado, clasificacion)
     * son de programacion y NO se cuentan.
     */
    private <R> R countingInvalid(String contract, java.util.function.Supplier<R> fn) {
        try {
            return fn.get();
        } catch (MissingAggregateVersionException | SchemaValidationException | SchemaNotFoundException e) {
            bus.metrics.stateWrite(contract, MetricsSink.StateWriteOutcome.INVALID_SCHEMA);
            throw e;
        }
    }

    private void countingInvalid(String contract, Runnable fn) {
        countingInvalid(contract, () -> {
            fn.run();
            return null;
        });
    }
}
