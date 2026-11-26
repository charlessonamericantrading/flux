/*
 * Peticion/respuesta — la extension opcional `rpc`.
 * Contrato normativo: specification/11-request-reply.md, specification/04-errors.md §7.
 *
 * Un EVENTO dice que paso, a quien quiera oirlo; una PETICION pide algo a un unico responsable y espera la respuesta. Va por CORE
 * NATS, no por JetStream: no se guarda, no se reentrega y no la ve ningun otro servicio. Este fichero es la mitad que habla con el
 * broker (`bus.request` y `bus.respond`); el sobre, los nombres y los codigos son {@link RpcEnvelope} y {@link RpcErrors}.
 *
 * Lo que la libreria (jnats 2.20.5) hace por su cuenta y por lo que aqui se rodea (Apendice A, A-200 a A-209):
 *   · una peticion DENEGADA no falla: espera su timeout. Se compite con el aviso de denegacion (`Denials.race`);
 *   · una suscripcion DENEGADA no lanza al suscribirse: `respond()` hace un PING/PONG, espera a que los callbacks alcancen los
 *     `-ERR` ya leidos (`Denials.fence`) y falla con PERMISSION_DENIED en vez de arrancar mudo;
 *   · un `no responders` y el timeout de limpieza de la libreria son la MISMA excepcion (`CancellationException`): se fija la
 *     limpieza en 60 s (> los 25 s maximos de una llamada) para que el unico timeout sea el de la llamada;
 *   · el bloque de un `Dispatcher` se ejecuta en UN hilo: los handlers corren en un pool propio, con tope por operacion.
 */
package com.flux;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flux.FluxErrors.Classification;
import com.flux.RpcErrors.InvalidRpcCallException;
import com.flux.RpcErrors.RpcInvalidReplyException;
import com.flux.RpcErrors.RpcMessageTooLargeException;
import com.flux.RpcErrors.RpcPermanentException;
import com.flux.RpcErrors.RpcRetryableException;
import com.flux.RpcErrors.RpcSchemaException;
import com.flux.RpcErrors.RpcTimeoutException;
import com.flux.RpcErrors.RpcUnavailableException;
import com.flux.RpcErrors.StaleRequestException;
import com.flux.SecurityErrors.Operation;
import com.flux.SecurityErrors.PermissionDeniedException;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/** El cliente de peticion/respuesta de un {@link FluxBus}. Se usa con {@link FluxBus#request} y {@link FluxBus#respond}. */
public final class FluxRpc {

    // ─── Constantes ──────────────────────────────────────────────────────────

    /** Handlers concurrentes por operacion si no se dice otra cosa: el orden de {@code max_ack_pending} (11-request-reply.md §5.6). */
    public static final int MAX_CONCURRENT_HANDLERS = 256;

    /** Cuanto espera {@link Responder#stop()} a los handlers en vuelo: el timeout maximo de una llamada (§5.5). */
    public static final int STOP_TIMEOUT_MS = RpcEnvelope.TIMEOUT_MAX_MS;

    /** Margen tras vaciar lo en vuelo: cubre la propagacion asincrona del interes en un cluster (A-70). */
    static final int STOP_MARGIN_MS = 25;

    /** Ventana de {@code STALE_REQUEST}: solo con la firma en {@code require} (§5.1, paso 5). */
    public static final int STALE_WINDOW_SECONDS = 300;

    private static final int RETRY_PAUSE_MS = 50;

    // ─── Tipos publicos ──────────────────────────────────────────────────────

    /** Opciones de una llamada. Todas son opcionales. */
    public static final class RequestOptions {
        private Integer timeoutMs;
        private String tenantId;
        private String aggregateId;
        private FluxEvent.DataClassification classification;
        private int retries;

        /**
         * Cuanto esperar a la respuesta, en ms: un entero entre 1 y 25 000 (5 000 por defecto). Fuera de rango es
         * {@code INVALID_RPC_CALL}: no se recorta en silencio. Es el de UN intento.
         */
        public RequestOptions timeoutMs(int v) {
            this.timeoutMs = v;
            return this;
        }

        /** Tenant. Si no, el del evento (o la peticion) en curso, el de {@code connect()} y, si no hay, {@code system}. */
        public RequestOptions tenantId(String v) {
            this.tenantId = v;
            return this;
        }

        /** La identidad del agregado sobre el que se pide ({@code cli-1}): va al atributo {@code subject} del sobre. NO es el subject de NATS. */
        public RequestOptions aggregateId(String v) {
            this.aggregateId = v;
            return this;
        }

        /** {@code public}, {@code internal}, {@code confidential} o {@code restricted}. Si no, la de {@code connect()}. */
        public RequestOptions classification(FluxEvent.DataClassification v) {
            this.classification = v;
            return this;
        }

        /**
         * Reintentos ante {@code RPC_UNAVAILABLE} y {@code RPC_TIMEOUT} —solo esos: nunca ante un error permanente—, por
         * defecto 0. <b>Cada reintento reutiliza el mismo {@code id} y los mismos bytes</b> ({@code time} incluido): con un
         * {@code id} nuevo por intento, quien responde no podria reconocer la orden repetida, y {@code RPC_TIMEOUT} NO dice que
         * la operacion no se ejecuto.
         */
        public RequestOptions retries(int v) {
            this.retries = v;
            return this;
        }
    }

    /** Lo que recibe un handler junto a los datos: el sobre de la peticion y lo que mas se usa de el. */
    public record RpcContext(String operation, FluxEvent event, String id, String correlationId, String tenantId,
                             String aggregateId, String time) {

        /**
         * El contexto para lo que haga el handler: pasalo a {@code bus.publish(ctx.eventContext(), …)} o a
         * {@code bus.request(ctx.eventContext(), …)} y la cadena de correlacion, la causa y la traza continuan solas.
         */
        public EventContext eventContext() {
            return EventContext.fromEvent(event);
        }

        /** Los datos de la peticion como el tipo de la aplicacion. Un desajuste es PERMANENT ({@code DATA_SCHEMA_MISMATCH}). */
        public <T> T dataAs(Class<T> type) {
            try {
                return Envelope.mapper().treeToValue(event.data(), type);
            } catch (Exception e) {
                throw new FluxErrors.PermanentException("los datos de la peticion no encajan en el tipo esperado",
                        "DATA_SCHEMA_MISMATCH", e);
            }
        }
    }

    /**
     * Atiende una peticion. El resultado es un objeto (un registro, un {@code Map}, un {@code JsonNode}) que viaja como el
     * {@code data} de la respuesta. Lanzar {@code FluxErrors.PermanentException} o {@code RetryableException} responde un error
     * con SU codigo; cualquier otra excepcion, un {@code permanent UNKNOWN} generico (11-request-reply.md §5.3).
     *
     * <p>El handler DEBE ser idempotente: una orden puede llegar dos veces (§4.5), y {@code ctx.id()} es la clave de deduplicacion.
     * Y es suyo autorizar el tenant: quien responde NO filtra por el tenant de {@code connect()} (§5.6).
     */
    @FunctionalInterface
    public interface Handler {
        Object handle(JsonNode data, RpcContext ctx) throws Exception;
    }

    public static final class RespondOptions {
        private FluxEvent.DataClassification classification;
        private int maxConcurrent = MAX_CONCURRENT_HANDLERS;

        /** La clasificacion de las respuestas de esta operacion. Si no, la de {@code connect()}. El error es siempre {@code internal}. */
        public RespondOptions classification(FluxEvent.DataClassification v) {
            this.classification = v;
            return this;
        }

        /**
         * Handlers concurrentes de ESTA operacion (256 por defecto). El exceso espera en la cola de la suscripcion: core NATS
         * no tiene acuse, y un responder sobrecargado no puede decir «ahora no» (§5.6, §13).
         */
        public RespondOptions maxConcurrent(int v) {
            this.maxConcurrent = v;
            return this;
        }
    }

    /** Una operacion atendida. */
    public interface Responder extends AutoCloseable {
        /** {@code clientes.cliente.v1.calcular-descuento}. */
        String operation();

        /** El subject EXACTO al que se suscribio: {@code rpc.clientes.cliente.v1.calcular-descuento}. Nunca {@code rpc.<dominio>.>}. */
        String subject();

        /**
         * Parada ordenada (§5.5): DEJA DE ESCUCHAR (UNSUB y espera a lo que ya viaja: las peticiones nuevas dan {@code no
         * responders} al instante), ESPERA A LOS HANDLERS EN VUELO —con un tope de 25 s— y retorna. Cerrar la conexion de golpe
         * pierde lo que se atiende (A-62).
         */
        void stop();

        /** Como {@link #stop()} con otro tope de espera. */
        void stop(Duration timeout);

        @Override
        default void close() {
            stop();
        }
    }

    // ─── Estado ──────────────────────────────────────────────────────────────

    private final FluxBus bus;
    private final Map<String, ResponderState> responders = new ConcurrentHashMap<>();

    FluxRpc(FluxBus bus) {
        this.bus = bus;
    }

    private FluxBus.ConnectOptions options() {
        return bus.options;
    }

    private void log(System.Logger.Level level, String message) {
        bus.log(level, message);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // El cliente
    // ═════════════════════════════════════════════════════════════════════════

    /**
     * El algoritmo de 11-request-reply.md §4.1. Devuelve el {@code data} de la respuesta. Hace UN intento, salvo {@code retries}.
     *
     * <p>Todo lo que se puede comprobar sin red se comprueba ANTES, en este orden y el primero que falla gana:
     * {@code RESERVED_DOMAIN} → {@code INVALID_RPC_CALL} → {@code INVALID_DATACLASSIFICATION} → L3 ({@code INVALID_SCHEMA}) →
     * construir el sobre UNA vez → tamano ({@code RPC_MESSAGE_TOO_LARGE}).
     *
     * @param parent el evento o la peticion en curso si se llama desde un handler: la llamada hereda su tenant y su cadena de
     *               correlacion. {@link EventContext#ROOT} si nace de cero.
     */
    public JsonNode request(EventContext parent, String operation, Object data, RequestOptions options) {
        Prepared prep = prepare(parent != null ? parent : EventContext.ROOT, operation, data,
                options != null ? options : new RequestOptions());
        long start = System.nanoTime();
        MetricsSink m = bus.metrics;
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                JsonNode out = attemptOnce(prep);
                m.rpcClientRequest(prep.operation, MetricsSink.RpcClientOutcome.OK);
                m.rpcDuration(prep.operation, "client", seconds(start));
                return out;
            } catch (RuntimeException e) {
                // Solo RPC_UNAVAILABLE y RPC_TIMEOUT se reintentan, con el MISMO id y los mismos bytes.
                if (attempt <= prep.retries && (e instanceof RpcUnavailableException || e instanceof RpcTimeoutException)) {
                    if (e instanceof RpcUnavailableException) {
                        sleep((long) RETRY_PAUSE_MS * attempt);
                    }
                    continue;
                }
                MetricsSink.RpcClientOutcome outcome = clientOutcome(e);
                if (outcome != null) {
                    m.rpcClientRequest(prep.operation, outcome);
                }
                m.rpcDuration(prep.operation, "client", seconds(start));
                throw e;
            }
        }
    }

    /** Lo mismo, con la respuesta convertida a {@code type}. Un desajuste es PERMANENT ({@code DATA_SCHEMA_MISMATCH}). */
    public <R> R request(EventContext parent, String operation, Object data, RequestOptions options, Class<R> type) {
        JsonNode reply = request(parent, operation, data, options);
        try {
            return Envelope.mapper().treeToValue(reply, type);
        } catch (Exception e) {
            throw new FluxErrors.PermanentException("la respuesta de \"" + operation + "\" no encaja en el tipo esperado",
                    "DATA_SCHEMA_MISMATCH", e);
        }
    }

    private static double seconds(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000_000.0;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class Prepared {
        final String operation;
        final String subject;
        final byte[] bytes;
        final FluxEvent event;
        final String replyUri;
        final int timeoutMs;
        final int retries;

        Prepared(String operation, String subject, byte[] bytes, FluxEvent event, String replyUri, int timeoutMs, int retries) {
            this.operation = operation;
            this.subject = subject;
            this.bytes = bytes;
            this.event = event;
            this.replyUri = replyUri;
            this.timeoutMs = timeoutMs;
            this.retries = retries;
        }
    }

    /** Paso 0 de §4.1: validar y construir SIN red. */
    private Prepared prepare(EventContext ctx, String operation, Object data, RequestOptions options) {
        FluxBus.ConnectOptions o = options();
        // RESERVED_DOMAIN → INVALID_RPC_CALL (operacion, timeoutMs, data objeto y las opciones propias)
        RpcEnvelope.ParsedOperation p = RpcEnvelope.parseOperation(operation);
        int timeoutMs = RpcEnvelope.resolveTimeout(options.timeoutMs);
        JsonNode dataNode = RpcEnvelope.dataObject(data, "`data`");
        if (options.retries < 0) {
            throw new InvalidRpcCallException("`retries` debe ser un entero >= 0 (0 por defecto: `request` hace UN intento).");
        }
        if (options.aggregateId != null && options.aggregateId.isEmpty()) {
            throw new InvalidRpcCallException("`aggregateId` debe ser un texto no vacio: la identidad del agregado (`cli-1`).");
        }
        if (options.tenantId != null && options.tenantId.isEmpty()) {
            throw new InvalidRpcCallException("`tenantId` debe ser un texto no vacio.");
        }
        // INVALID_DATACLASSIFICATION
        FluxEvent.DataClassification classification = options.classification != null ? options.classification
                : o.classification != null ? o.classification : FluxEvent.DataClassification.INTERNAL;

        String id = Protocol.uuidV7();
        String requestUri = schemaFor(p.operation(), RpcEnvelope.Side.REQUEST);
        String replyUri = schemaFor(p.operation(), RpcEnvelope.Side.REPLY);
        // El contexto heredado gana sobre el default de conexion (09-multitenancy.md §5): una llamada hecha dentro de un handler
        // continua la cadena. Un estado exige un tenant explicito; la peticion, no: como los eventos, cae en `system`.
        String tenant = firstNonEmpty(options.tenantId, ctx.tenantId(), o.tenantId, "system");
        RpcEnvelope.RequestInput input = new RpcEnvelope.RequestInput()
                .operation(p.operation())
                .tenantId(tenant)
                .id(id)
                .source(bus.source)
                .time(Envelope.formatTime(Instant.now()))
                .correlationid(ctx.isRoot() ? id : ctx.correlationId())
                .producerversion(o.version)
                .classification(classification.wire())
                .data(dataNode)
                .dataschema(requestUri);
        if (options.aggregateId != null) {
            input.aggregateId(options.aggregateId);
        }
        if (!ctx.isRoot() && ctx.causationId() != null) {
            input.causationid(ctx.causationId());
        }
        if (ctx.traceparent() != null) {
            input.traceparent(ctx.traceparent());
            if (ctx.tracestate() != null) {
                input.tracestate(ctx.tracestate());
            }
        } else {
            String tp = activeTraceparent();
            if (tp != null) {
                input.traceparent(tp);
            }
        }
        FluxEvent event = RpcEnvelope.buildRequest(input);

        // L3: la peticion contra su esquema, ANTES de abrir nada: reporta TODAS las rutas y ningun valor.
        Validation.Validator v = bus.validator;
        if (v != null) {
            RuntimeException failure = v.failure(event, p.operation());
            if (failure != null) {
                bus.metrics.rpcClientRequest(p.operation(), MetricsSink.RpcClientOutcome.INVALID_SCHEMA);
                if (v.mode() == Validation.Mode.STRICT) {
                    throw asRpcSchemaError(failure, "peticion", p.operation());
                }
                v.check(event, p.operation()); // warn: lo registra la propia validacion, y se sigue
            }
        }

        // El sobre se construye UNA vez: `id`, `time` y bytes son los mismos en todos los intentos.
        byte[] bytes;
        try {
            bytes = encode(event, "peticion", p.operation());
        } catch (RpcMessageTooLargeException e) {
            bus.metrics.rpcClientRequest(p.operation(), MetricsSink.RpcClientOutcome.PERMANENT);
            throw e;
        }
        return new Prepared(p.operation(), "rpc." + p.operation(), bytes, event, replyUri, timeoutMs, options.retries);
    }

    /**
     * Un intento: publica por core NATS con el inbox privado y espera, lo primero que ocurra: {@code no responders} →
     * {@code RPC_UNAVAILABLE}; un {@code -ERR Permissions Violation} → {@code PERMISSION_DENIED}; una respuesta → se comprueba;
     * el timeout → {@code RPC_TIMEOUT}.
     */
    private JsonNode attemptOnce(Prepared p) {
        // Con jnats la denegacion no llega en la propia peticion (A-201): quien llama solo veria un timeout, y un permiso que falta
        // se veria como RPC_TIMEOUT (RETRYABLE) y el handler que llama reintentaria 51 minutos algo que nunca va a funcionar
        // (11-request-reply.md §7.1). Se compite el aviso con la respuesta.
        Message reply;
        try {
            reply = bus.listener.denials.race(options().service, p.subject, hintCaller(p.subject), "_INBOX." + options().service + ".",
                    hintInbox(), () -> bus.connection.request(p.subject, p.bytes), p.timeoutMs);
        } catch (PermissionDeniedException denied) {
            throw denied;
        } catch (CancellationException noResponders) {
            // jnats cancela el futuro con un `no responders` (503). La limpieza propia de la libreria esta puesta en 60 s, mas que
            // el timeout maximo de una llamada, asi que aqui no puede ser un timeout (A-203).
            throw new RpcUnavailableException(p.operation, noResponders);
        } catch (TimeoutException timeout) {
            throw new RpcTimeoutException(p.operation, p.timeoutMs, timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FluxBus.FluxBusException("flux: llamada a \"" + p.operation + "\" interrumpida", e);
        } catch (ExecutionException e) {
            throw new FluxBus.FluxBusException("flux: fallo al llamar a \"" + p.operation + "\"", e.getCause());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FluxBus.FluxBusException("flux: fallo al llamar a \"" + p.operation + "\"", e);
        }

        // 3. ¿Es la respuesta a ESTA peticion? (§4.3) Si no, RPC_INVALID_REPLY.
        FluxEvent event = checkReply(p, reply.getData());
        boolean isError = RpcEnvelope.ERROR_TYPE.equals(event.type());

        // Con la firma en `require`: verificar. Es un fallo PERMANENT: reintentar da lo mismo.
        try {
            bus.verify(event); // `warn` registra el fallo y sigue; `require` lanza
        } catch (FluxErrors.FluxException e) {
            String code = e.code() != null && !e.code().equals(e.getClass().getSimpleName()) ? e.code() : "INVALID_SIGNATURE";
            throw new RpcPermanentException(p.operation, code, "la respuesta a \"" + p.operation + "\" no paso la verificacion de su "
                    + "firma (" + code + "): " + e.getMessage() + ". Con la firma en `require`, quien llama verifica la respuesta y los "
                    + "errores (11-request-reply.md §7.4).");
        }

        // 4. Un error: RpcRetryableException o RpcPermanentException segun su `class`, con su `code`.
        if (isError) {
            RpcEnvelope.ErrorData d = RpcEnvelope.ErrorData.fromJson(event.data());
            throw d.errorClass().equals("retryable")
                    ? new RpcRetryableException(p.operation, d.code(), d.message(), d.retryAfterMs())
                    : new RpcPermanentException(p.operation, d.code(), d.message());
        }

        // 5. Una respuesta: con L3, `data` contra el esquema de la respuesta; se devuelve el `data`.
        Validation.Validator v = bus.validator;
        if (v != null) {
            FluxEvent candidate = event.withSchemaAndData(p.replyUri, event.data());
            RuntimeException failure = v.failure(candidate, p.operation);
            if (failure != null) {
                if (v.mode() == Validation.Mode.STRICT) {
                    throw asRpcSchemaError(failure, "respuesta", p.operation);
                }
                v.check(candidate, p.operation);
                bus.metrics.rpcClientRequest(p.operation, MetricsSink.RpcClientOutcome.INVALID_SCHEMA);
            }
        }
        return event.data();
    }

    /** Las cinco comprobaciones de §4.3. Cualquiera que falle es {@code RPC_INVALID_REPLY}. */
    private FluxEvent checkReply(Prepared p, byte[] bytes) {
        boolean streamHint = looksLikeStreamAck(bytes);

        // 1. Es un CloudEvent valido (parsea, con las cuatro extensiones y los tipos exactos).
        FluxEvent event;
        try {
            event = Envelope.parseEvent(bytes);
        } catch (RuntimeException e) {
            String code = e instanceof FluxErrors.FluxException fe ? fe.code() : "POISON";
            throw new RpcInvalidReplyException(p.operation, "no es un CloudEvent valido de flux (" + code + ")", streamHint, e);
        }
        // 2. Su `type` es el de la respuesta de esa operacion, o el del error.
        String expected = RpcEnvelope.replyType(p.operation);
        if (!expected.equals(event.type()) && !RpcEnvelope.ERROR_TYPE.equals(event.type())) {
            throw new RpcInvalidReplyException(p.operation, "su `type` es " + Envelope.jsonString(Text.truncateCodePoints(event.type(), 100))
                    + " y se esperaba " + Envelope.jsonString(expected) + " o " + Envelope.jsonString(RpcEnvelope.ERROR_TYPE), streamHint, null);
        }
        // 3. Su `causationid` es el `id` de la peticion.
        if (!p.event.id().equals(event.causationid())) {
            throw new RpcInvalidReplyException(p.operation, "su `causationid` no es el `id` de la peticion (" + p.event.id() + "): "
                    + (event.causationid() != null ? Envelope.jsonString(Text.truncateCodePoints(event.causationid(), 80)) : "no lo trae"),
                    streamHint, null);
        }
        // 4. Su `correlationid` y su `tenantid` son los de la peticion. EXCEPCION UNICA (§4.3.4): el error de una peticion que quien
        // responde no pudo leer (§5.2) no tiene nada que heredar y lleva `correlationid` propio y `tenantid: system`; a quien llama le
        // basta la comprobacion 3 (su `causationid` es el `id` de la peticion, que solo el conoce) para atribuirlo.
        boolean isError = RpcEnvelope.ERROR_TYPE.equals(event.type());
        if (!isUnreadableRequestError(event, isError)) {
            if (!p.event.correlationid().equals(event.correlationid())) {
                throw new RpcInvalidReplyException(p.operation, "su `correlationid` no es el de la peticion: una respuesta hereda el de la peticion",
                        streamHint, null);
            }
            if (!p.event.tenantid().equals(event.tenantid())) {
                throw new RpcInvalidReplyException(p.operation, "su `tenantid` no es el de la peticion: una respuesta hereda el de la peticion, sin modificar",
                        streamHint, null);
            }
        }
        // 5. Si es un error, su `data` cumple el esquema del error.
        if (isError) {
            String problem = RpcEnvelope.errorDataProblem(event.data());
            if (problem != null) {
                throw new RpcInvalidReplyException(p.operation, "el error no cumple su esquema (" + problem + ")", streamHint, null);
            }
        }
        return event;
    }

    /**
     * ¿Es el error que contesta quien responde a una peticion que NO pudo leer (§5.2)? Sin sobre no hay {@code correlationid} ni
     * {@code tenantid} que heredar: el error lleva {@code correlationid} igual a su propio {@code id} y {@code tenantid: system}. Es
     * la unica respuesta a la que quien llama no le exige heredar los dos: la forma es estrecha a proposito, y la comprobacion 3 sigue
     * en pie y es la que la atribuye.
     */
    static boolean isUnreadableRequestError(FluxEvent event, boolean isError) {
        return isError && "system".equals(event.tenantid()) && event.id() != null && event.id().equals(event.correlationid());
    }

    /** El cuerpo, ¿parece el acuse de un stream de JetStream ({@code {"stream":…,"seq":…}})? */
    private static boolean looksLikeStreamAck(byte[] bytes) {
        try {
            JsonNode o = Envelope.mapper().readTree(bytes);
            return o != null && o.isObject() && o.path("stream").isTextual() && o.path("seq").isNumber();
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    private String hintCaller(String subject) {
        return "Quien llama necesita permiso de PUBLICACION sobre el subject EXACTO de cada operacion concedida (`" + subject + "`; "
                + "nunca `rpc.>` ni `rpc.<dominio>.>`): es la clave `calls` del manifiesto de `gen-acl.mjs`. Sin el la peticion se "
                + "descarta y, con una libreria que no entrega la denegacion en la propia peticion, solo se veria un timeout "
                + "(11-request-reply.md §7.1).";
    }

    private String hintInbox() {
        return "Quien llama necesita permiso de SUSCRIPCION sobre su inbox privado `_INBOX." + options().service + ".>`: por ahi llegan "
                + "las respuestas, y sin el ninguna puede llegar —core NATS no avisa— y quien llama solo veria un timeout "
                + "(11-request-reply.md §7.3, 06-security.md §3.3).";
    }

    private RuntimeException asRpcSchemaError(RuntimeException failure, String side, String operation) {
        if (failure instanceof SchemaValidationException sv && !(failure instanceof RpcSchemaException)) {
            return new RpcSchemaException(side, operation, sv.dataschema(), sv.errors());
        }
        return failure;
    }

    private static MetricsSink.RpcClientOutcome clientOutcome(RuntimeException e) {
        if (e instanceof RpcUnavailableException) {
            return MetricsSink.RpcClientOutcome.UNAVAILABLE;
        }
        if (e instanceof RpcTimeoutException) {
            return MetricsSink.RpcClientOutcome.TIMEOUT;
        }
        if (e instanceof RpcInvalidReplyException) {
            return MetricsSink.RpcClientOutcome.INVALID_REPLY;
        }
        if (e instanceof SchemaValidationException || e instanceof SchemaNotFoundException) {
            return MetricsSink.RpcClientOutcome.INVALID_SCHEMA;
        }
        if (e instanceof RpcRetryableException) {
            return MetricsSink.RpcClientOutcome.RETRYABLE;
        }
        // `permanent`: un error permanente del que responde, PERMISSION_DENIED, STALE_REQUEST (si lo contesta el responder es un
        // RpcPermanentException con ese codigo), RPC_MESSAGE_TOO_LARGE y un fallo de firma.
        if (e instanceof RpcPermanentException || e instanceof PermissionDeniedException || e instanceof RpcMessageTooLargeException) {
            return MetricsSink.RpcClientOutcome.PERMANENT;
        }
        return null; // un fallo de transporte lo cuenta la conexion; INVALID_RPC_CALL no se cuenta: es de programacion
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Quien responde
    // ═════════════════════════════════════════════════════════════════════════

    private final class ResponderState implements Responder {
        final String operation;
        final String subject;
        final Handler handler;
        final FluxEvent.DataClassification classification;
        final String requestUri;
        final String replyUri;
        final Semaphore slots;
        final AtomicInteger inFlight = new AtomicInteger();
        final Object idle = new Object();
        final ExecutorService executor;
        volatile Dispatcher dispatcher;
        volatile CompletableFuture<Void> stopping;
        /** {@code respond()} ya devolvio: una denegacion posterior no puede ser un error de {@code respond()}, solo un log. */
        volatile boolean started;

        ResponderState(String operation, Handler handler, FluxEvent.DataClassification classification, String requestUri,
                       String replyUri, int maxConcurrent) {
            this.operation = operation;
            this.subject = "rpc." + operation;
            this.handler = handler;
            this.classification = classification;
            this.requestUri = requestUri;
            this.replyUri = replyUri;
            this.slots = new Semaphore(maxConcurrent);
            this.executor = Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "flux-rpc-" + operation);
                t.setDaemon(true);
                return t;
            });
        }

        @Override
        public String operation() {
            return operation;
        }

        @Override
        public String subject() {
            return subject;
        }

        @Override
        public void stop() {
            stop(Duration.ofMillis(STOP_TIMEOUT_MS));
        }

        @Override
        public void stop(Duration timeout) {
            stopAsync(timeout).join();
        }

        /** La parada ordenada de §5.5: dejar de escuchar → esperar a lo que esta en vuelo (con tope) → el llamante cierra. */
        synchronized CompletableFuture<Void> stopAsync(Duration timeout) {
            if (stopping == null) {
                responders.remove(operation, this);
                stopping = CompletableFuture.runAsync(() -> doStop(timeout), STOPPER);
            }
            return stopping;
        }

        private void doStop(Duration timeout) {
            long deadline = System.nanoTime() + timeout.toNanos();
            // 1. Dejar de escuchar con el DRENAJE de la libreria: UNSUB y un PING/PONG que espera a lo que ya viaja, y despacha lo que
            // quedaba en la cola. Las peticiones nuevas dan `no responders` al instante; una baja que descartase lo que aun llega la
            // perdería (A-62). Es el drenaje DE LA SUSCRIPCION: `Connection.drain` cerraria la conexion con handlers en vuelo (A-83).
            Dispatcher d = dispatcher;
            if (d != null) {
                try {
                    d.drain(timeout).get(Math.max(1, timeout.toMillis()), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    // ya cerrada (la conexion se cerro, o el broker la denego): no hay nada que dejar de escuchar
                }
            }
            // 2. Esperar a los handlers en vuelo, con un tope: una peticion mas lenta ya la dio por perdida quien llamo. Un margen
            // breve cubre la propagacion asincrona del interes en un cluster (A-70).
            boolean timedOut = false;
            synchronized (idle) {
                while (inFlight.get() > 0) {
                    long left = deadline - System.nanoTime();
                    if (left <= 0) {
                        timedOut = true;
                        break;
                    }
                    try {
                        TimeUnit.NANOSECONDS.timedWait(idle, left);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        timedOut = true;
                        break;
                    }
                }
            }
            if (timedOut) {
                log(System.Logger.Level.WARNING, "\"" + operation + "\": " + inFlight.get() + " handler(s) siguen en vuelo tras "
                        + timeout.toMillis() + " ms; se deja de esperar. Una peticion mas lenta que el timeout maximo (25 s) ya la "
                        + "dio por perdida quien llamo.");
            } else {
                sleep(STOP_MARGIN_MS);
            }
            executor.shutdown();
        }

        void accept(Message msg) {
            // El `Dispatcher` entrega en UN hilo: los handlers corren aparte, con tope. El exceso espera en la cola de la propia
            // suscripcion (este hilo se bloquea aqui) y no se pierde mientras quepa en ella.
            try {
                slots.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            inFlight.incrementAndGet();
            try {
                executor.execute(() -> {
                    try {
                        serve(this, msg);
                    } finally {
                        slots.release();
                        synchronized (idle) {
                            inFlight.decrementAndGet();
                            idle.notifyAll();
                        }
                    }
                });
            } catch (RuntimeException rejected) {
                slots.release();
                synchronized (idle) {
                    inFlight.decrementAndGet();
                    idle.notifyAll();
                }
                log(System.Logger.Level.ERROR, "no se pudo atender una peticion de \"" + operation + "\": " + rejected
                        + ". Quien llamo vera un timeout.");
            }
        }
    }

    /** Los hilos de las paradas: una por responder, en paralelo. */
    private static final ExecutorService STOPPER = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "flux-rpc-stop");
        t.setDaemon(true);
        return t;
    });

    /**
     * Atiende una operacion: UNA suscripcion al subject EXACTO ({@code rpc.<operacion>}, nunca {@code rpc.<dominio>.>}: con el
     * comodin, una operacion que el servicio no atiende recibe interes y quien llama espera su timeout en vez de {@code no
     * responders}, A-64) con el NOMBRE DEL SERVICIO como grupo de cola: dos instancias se reparten las peticiones (A-58).
     *
     * <p>Un dueno sin permiso de suscripcion parece un servicio CAIDO ({@code no responders}, A-73): {@code respond()} FALLA con
     * {@code PERMISSION_DENIED}, nombrando el subject, en vez de arrancar mudo.
     */
    public Responder respond(String operation, Handler handler, RespondOptions options) {
        FluxBus.ConnectOptions o = options();
        RpcEnvelope.ParsedOperation p = RpcEnvelope.parseOperation(operation);
        if (handler == null) {
            throw new InvalidRpcCallException("el handler de `respond()` debe ser una funcion `(datos, ctx) -> resultado`.");
        }
        RespondOptions ro = options != null ? options : new RespondOptions();
        if (ro.maxConcurrent < 1) {
            throw new InvalidRpcCallException("`maxConcurrent` debe ser un entero >= 1 (256 por defecto).");
        }
        if (responders.containsKey(p.operation())) {
            throw new InvalidRpcCallException("ya hay un `respond()` de \"" + p.operation() + "\" en este servicio. Dos suscripciones del "
                    + "mismo grupo de cola en una conexion se repartirian las peticiones entre dos handlers: registra la operacion una "
                    + "vez, o llama a `stop()` del responder anterior.");
        }
        String requestUri = schemaFor(p.operation(), RpcEnvelope.Side.REQUEST);
        String replyUri = schemaFor(p.operation(), RpcEnvelope.Side.REPLY);
        // En modo estricto una operacion sin esquema es un fallo de configuracion: se dice al arrancar, no en cada peticion.
        Validation.Validator v = bus.validator;
        if (v != null && v.mode() == Validation.Mode.STRICT) {
            SchemaBundle bundle = o.validation != null ? o.validation.bundle() : null;
            String[][] sides = {{requestUri, "peticion"}, {replyUri, "respuesta"}};
            for (String[] side : sides) {
                if (bundle == null || bundle.schema(side[0]) == null) {
                    throw new SchemaNotFoundException(p.operation() + " (" + side[1] + ")", side[0]);
                }
            }
        }

        ResponderState state = new ResponderState(p.operation(), handler, ro.classification, requestUri, replyUri, ro.maxConcurrent);
        responders.put(p.operation(), state);
        // Una denegacion de un intento anterior sobre este subject no debe condenar a este.
        bus.listener.denials.forget(state.subject);

        Dispatcher dispatcher = bus.connection.createDispatcher();
        state.dispatcher = dispatcher;
        try {
            dispatcher.subscribe(state.subject, o.service, state::accept);
            // Una suscripcion denegada NO lanza al suscribirse: el `-ERR` llega por la conexion y la suscripcion queda activa y
            // muda (A-8f, A-73). El PING/PONG ordena: el `-ERR` se lee ANTES del PONG; y su callback va por detras del hilo
            // lector, asi que se espera a que los callbacks alcancen la cuenta de `-ERR` leidos (`Statistics.getErrs`) en vez de
            // adivinar un tiempo.
            bus.connection.flush(Duration.ofSeconds(5));
            bus.listener.denials.fence(() -> bus.connection.getStatistics().getErrs());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            abandon(state);
            throw new FluxBus.FluxBusException("flux: respond(\"" + p.operation() + "\") interrumpido", e);
        } catch (TimeoutException | RuntimeException e) {
            abandon(state);
            throw new FluxBus.FluxBusException("flux: no se pudo suscribir a \"" + state.subject + "\"", e);
        }
        Operation denied = bus.listener.denials.deniedOperation(state.subject);
        if (denied != null) {
            abandon(state);
            throw new PermissionDeniedException(o.service, Operation.SUBSCRIBE, state.subject,
                    "El dueno de un dominio necesita permiso de SUSCRIPCION sobre `rpc.<dominio>.>` (es la clave `owns` del manifiesto "
                            + "de `gen-acl.mjs`; el SDK se suscribe al subject exacto de cada operacion, pero lo que se CONCEDE no es "
                            + "lo que se USA). Sin el la operacion parecia un servicio caido —quien llama veria `RPC_UNAVAILABLE`— y por "
                            + "eso `respond()` falla al arrancar y no arranca mudo (11-request-reply.md §7.1, A-73).", null);
        }
        state.started = true;
        return state;
    }

    /** Deshace un `respond()` que no llego a arrancar: la operacion no figura como atendida y no queda ningun hilo. */
    private void abandon(ResponderState state) {
        responders.remove(state.operation, state);
        try {
            Dispatcher d = state.dispatcher;
            if (d != null) {
                bus.connection.closeDispatcher(d);
            }
        } catch (RuntimeException ignored) {
            // la conexion ya esta cerrada
        }
        state.executor.shutdownNow();
    }

    /** Para todos los responders, en paralelo: lo que hace {@code bus.close()} ANTES de cerrar la conexion. */
    void stopAll() {
        List<CompletableFuture<Void>> all = new java.util.ArrayList<>();
        for (ResponderState r : responders.values()) {
            all.add(r.stopAsync(Duration.ofMillis(STOP_TIMEOUT_MS)));
        }
        for (CompletableFuture<Void> f : all) {
            f.join();
        }
    }

    // ─── atender una peticion ────────────────────────────────────────────────

    /** Un mensaje de la suscripcion. NUNCA lanza: lo que falle aqui va al log, y el bucle sigue. */
    private void serve(ResponderState r, Message msg) {
        long start = System.nanoTime();
        MetricsSink.RpcServerOutcome outcome = null;
        try {
            outcome = handle(r, msg);
        } catch (Throwable e) {
            log(System.Logger.Level.ERROR, "fallo inesperado atendiendo \"" + r.operation + "\": " + e
                    + ". Quien llamo vera un timeout.");
        }
        if (outcome != null) {
            bus.metrics.rpcServerRequest(r.operation, outcome);
            bus.metrics.rpcDuration(r.operation, "server", seconds(start));
        }
    }

    /** Los pasos de §5.1. Devuelve el {@code outcome} de la peticion. */
    private MetricsSink.RpcServerOutcome handle(ResponderState r, Message msg) {
        FluxBus.ConnectOptions o = options();
        String op = r.operation;
        String reply = msg.getReplyTo();

        // 2. Sin subject de respuesta no se atiende: nadie espera el resultado de una orden (`nats pub` a mano).
        if (reply == null || reply.isEmpty()) {
            poison(op, new FluxErrors.PoisonException("la peticion no trae subject de respuesta", "NO_REPLY_SUBJECT"),
                    "llego una peticion a \"" + op + "\" SIN subject de respuesta (¿un `nats pub` a mano?): no se atiende, porque nadie "
                            + "espera el resultado de una orden y ejecutarla a ciegas la aplicaria sin que nadie lo sepa (11-request-reply.md §5.1).");
            return MetricsSink.RpcServerOutcome.POISON;
        }

        // 3. Interpretar el sobre. Falla → un error `permanent` con el codigo del sobre. Sin sobre no hay nada que heredar.
        FluxEvent event;
        try {
            event = Envelope.parseEvent(msg.getData());
        } catch (RuntimeException e) {
            String code = e instanceof FluxErrors.FluxException fe ? fe.code() : "MALFORMED_JSON";
            poison(op, e instanceof FluxErrors.PoisonException pe ? pe : new FluxErrors.PoisonException(String.valueOf(e), code),
                    "una peticion a \"" + op + "\" no es un CloudEvent valido (" + code + "): se contesta con un error permanent.");
            sendError(reply, r, null, readableId(msg.getData()), new RpcEnvelope.ErrorData("permanent", code,
                    "la peticion no es un CloudEvent valido de flux (" + code + "): se espera JSON con specversion, id, source, type, time, "
                            + "datacontenttype, dataschema y data, mas las cuatro extensiones obligatorias (correlationid, tenantid, "
                            + "producerversion, dataclassification) con tipos exactos (01-envelope.md §3.1)", null));
            return MetricsSink.RpcServerOutcome.POISON;
        }

        // 4. `type` = el de la peticion de ESTA operacion.
        String type = RpcEnvelope.requestType(op);
        if (!type.equals(event.type())) {
            poison(op, new FluxErrors.PoisonException("type inesperado en una peticion a \"" + op + "\"", "INVALID_RPC_CALL"),
                    "una peticion a \"" + op + "\" trae `type` " + Envelope.jsonString(Text.truncateCodePoints(event.type(), 100))
                            + " y se esperaba " + Envelope.jsonString(type) + ": se contesta con INVALID_RPC_CALL.");
            sendError(reply, r, event, null, new RpcEnvelope.ErrorData("permanent", "INVALID_RPC_CALL",
                    "la peticion llego al subject de \"" + op + "\" con `type` " + Envelope.jsonString(Text.truncateCodePoints(event.type(), 100))
                            + ", y el de la peticion de esta operacion es " + Envelope.jsonString(type)
                            + ". Quien llama debe construirla con el SDK (11-request-reply.md §5.2)", null));
            return MetricsSink.RpcServerOutcome.POISON;
        }

        // 5. Con la firma en `require`: verificar, y la ventana de 300 s.
        try {
            bus.verify(event); // `warn` registra el fallo y sigue; `require` lanza
        } catch (FluxErrors.FluxException e) {
            String code = e.code() != null && !e.code().equals(e.getClass().getSimpleName()) ? e.code() : "INVALID_SIGNATURE";
            log(System.Logger.Level.ERROR, "la peticion a \"" + op + "\" (" + event.id() + ") no paso la verificacion de su firma (" + code + ").");
            sendError(reply, r, event, null, new RpcEnvelope.ErrorData("permanent", code,
                    "la peticion a \"" + op + "\" no paso la verificacion de su firma (" + code + "): con la firma en `require`, quien "
                            + "responde exige una peticion firmada con una clave que conozca (07-signing.md §7)", null));
            return MetricsSink.RpcServerOutcome.INVALID_SIGNATURE;
        }
        if (o.signing != null && o.signing.verifyMode() == Signing.VerificationMode.REQUIRE) {
            Instant now = Instant.now();
            Long sent = epochMillis(event.time());
            if (sent == null || Math.abs(now.toEpochMilli() - sent) > STALE_WINDOW_SECONDS * 1000L) {
                StaleRequestException stale = new StaleRequestException(event.time(), STALE_WINDOW_SECONDS, Envelope.formatTime(now));
                log(System.Logger.Level.ERROR, "STALE_REQUEST en \"" + op + "\" (" + event.id() + "): `time` fuera de la ventana de "
                        + STALE_WINDOW_SECONDS + " s.");
                sendError(reply, r, event, null, new RpcEnvelope.ErrorData("permanent", "STALE_REQUEST", stale.getMessage(), null));
                return MetricsSink.RpcServerOutcome.STALE_REQUEST;
            }
        }

        // 6. Con L3: `data` contra el esquema de la peticion. Falla → error `permanent` INVALID_SCHEMA con TODAS las rutas.
        Validation.Validator v = bus.validator;
        if (v != null) {
            FluxEvent candidate = event.withSchemaAndData(r.requestUri, event.data());
            RuntimeException failure = v.failure(candidate, op);
            if (failure != null) {
                if (v.mode() == Validation.Mode.STRICT) {
                    String message = failure instanceof SchemaValidationException sv
                            ? "la peticion no cumple " + sv.dataschema() + ": " + String.join(", ", sv.errors())
                            : "no hay esquema de la peticion de \"" + op + "\" (" + r.requestUri + ") en el bundle de quien responde";
                    sendError(reply, r, event, null, new RpcEnvelope.ErrorData("permanent", codeOf(failure), message, null));
                    return MetricsSink.RpcServerOutcome.INVALID_SCHEMA;
                }
                // `warn`: queda en el log y en la metrica, y el handler SE EJECUTA.
                v.check(candidate, op);
                bus.metrics.rpcServerRequest(op, MetricsSink.RpcServerOutcome.INVALID_SCHEMA);
            }
        }

        // 7. Invocar el handler con (data, ctx). Una llamada hecha desde el continua la cadena.
        RpcContext ctx = new RpcContext(op, event, event.id(), event.correlationid(), event.tenantid(), event.aggregateId(), event.time());
        Object result;
        try {
            result = r.handler.handle(event.data(), ctx);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            RpcEnvelope.ErrorData err = errorOf(op, event, e);
            sendError(reply, r, event, null, err);
            return err.errorClass().equals("retryable") ? MetricsSink.RpcServerOutcome.RETRYABLE : MetricsSink.RpcServerOutcome.PERMANENT;
        } catch (Throwable e) {
            RpcEnvelope.ErrorData err = errorOf(op, event, e);
            sendError(reply, r, event, null, err);
            return err.errorClass().equals("retryable") ? MetricsSink.RpcServerOutcome.RETRYABLE : MetricsSink.RpcServerOutcome.PERMANENT;
        }

        // 8. Su resultado es un objeto. Con L3: contra el esquema de la respuesta.
        JsonNode replyData = asObject(result);
        if (replyData == null) {
            sendError(reply, r, event, null, new RpcEnvelope.ErrorData("permanent", "INVALID_SCHEMA",
                    "la respuesta no es un objeto: el handler de \"" + op + "\" devolvio "
                            + (result == null ? "null" : result instanceof JsonNode n && n.isArray() ? "un array" : result.getClass().getSimpleName())
                            + ". El fallo es de quien responde: la respuesta es un objeto JSON en la raiz (01-envelope.md §4)", null));
            return MetricsSink.RpcServerOutcome.INVALID_SCHEMA;
        }
        if (v != null) {
            // El validador mira `dataschema` y `data` y nada mas: se le da la respuesta con el esquema de ESTA operacion.
            FluxEvent candidate = event.withSchemaAndData(r.replyUri, replyData);
            RuntimeException failure = v.failure(candidate, op);
            if (failure != null) {
                if (v.mode() == Validation.Mode.STRICT) {
                    String message = failure instanceof SchemaValidationException sv
                            ? "la respuesta no cumple " + sv.dataschema() + ": " + String.join(", ", sv.errors())
                                    + ". El fallo es de quien responde, no de quien llama"
                            : "no hay esquema de la respuesta de \"" + op + "\" (" + r.replyUri + ") en el bundle de quien responde";
                    sendError(reply, r, event, null, new RpcEnvelope.ErrorData("permanent", codeOf(failure), message, null));
                    return MetricsSink.RpcServerOutcome.INVALID_SCHEMA;
                }
                v.check(candidate, op);
                bus.metrics.rpcServerRequest(op, MetricsSink.RpcServerOutcome.INVALID_SCHEMA);
            }
        }

        // 9. Construir la respuesta (§3.2), firmarla si procede, comprobar el tamano y publicarla al `reply` de la peticion.
        FluxEvent response = RpcEnvelope.buildReply(new RpcEnvelope.ReplyInput()
                .request(refOf(op, event))
                .id(Protocol.uuidV7())
                .source(bus.source)
                .time(Envelope.formatTime(Instant.now()))
                .producerversion(o.version)
                .classification((r.classification != null ? r.classification
                        : o.classification != null ? o.classification : FluxEvent.DataClassification.INTERNAL).wire())
                .data(replyData)
                .dataschema(r.replyUri)
                .traceparent(replyTraceparent(event)));
        byte[] bytes;
        try {
            bytes = encode(response, "respuesta", op);
        } catch (RpcMessageTooLargeException e) {
            // Si la respuesta no cabe, la libreria lanza EN QUIEN RESPONDE y quien llama solo ve un timeout (A-65): se contesta con un error.
            log(System.Logger.Level.ERROR, "la respuesta de \"" + op + "\" a la peticion " + event.id() + " no cabe: " + e.getMessage());
            sendError(reply, r, event, null, new RpcEnvelope.ErrorData("permanent", "RPC_MESSAGE_TOO_LARGE", e.getMessage(), null));
            return MetricsSink.RpcServerOutcome.PERMANENT;
        }
        publish(reply, bytes, op);
        return MetricsSink.RpcServerOutcome.OK;
    }

    private static String codeOf(RuntimeException failure) {
        return failure instanceof FluxErrors.FluxException fe ? fe.code() : "INVALID_SCHEMA";
    }

    /** El resultado del handler como un objeto JSON, o {@code null} si no lo es (un escalar, un array, nada, algo no serializable). */
    private static JsonNode asObject(Object result) {
        if (result == null) {
            return null;
        }
        try {
            JsonNode node = result instanceof JsonNode n ? n : Envelope.mapper().valueToTree(result);
            return node != null && node.isObject() ? node : null;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** {@code time} como milisegundos desde 1970, o {@code null} si no es un instante. */
    private static Long epochMillis(String time) {
        try {
            return OffsetDateTime.parse(time).toInstant().toEpochMilli();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** El {@code traceparent} de la respuesta: el del que responde, de la MISMA traza; sin span activo, el de la peticion. */
    private String replyTraceparent(FluxEvent request) {
        String active = activeTraceparent();
        return active != null ? active : request.traceparent();
    }

    private String activeTraceparent() {
        EventContext.TraceparentSupplier supplier = options().traceparentSupplier;
        if (supplier == null) {
            return null;
        }
        String tp = supplier.get();
        return tp == null || tp.isEmpty() ? null : tp;
    }

    /** Lo que la respuesta hereda de un sobre de peticion ya leido. */
    private static RpcEnvelope.RequestRef refOf(String operation, FluxEvent e) {
        return new RpcEnvelope.RequestRef(operation, e.id(), e.correlationid(), e.tenantid(), e.aggregateId(), e.partitionkey());
    }

    /**
     * Un error del handler → el {@code data} de {@code com.flux.rpc.error.v1} (§5.3):
     * <ul>
     *   <li>{@code RetryableException} / {@code PermanentException} / {@code PoisonException} → su clase, SU codigo y SU mensaje
     *       (la aplicacion los eligio);
     *   <li>uno que el clasificador RECONOCE (un 503, un {@code SocketException}) → su clase y el codigo del clasificador, con un
     *       mensaje GENERICO;
     *   <li>uno DESCONOCIDO → {@code permanent}, {@code UNKNOWN}, «error interno del servicio». Su texto va al log, con el {@code id}
     *       de la peticion. {@code poison} no viaja.
     * </ul>
     * Lo desconocido es {@code permanent} y no «retryable acotado»: el presupuesto de 04-errors.md §2.1 se cuenta por entregas de
     * un consumidor, y una llamada no tiene numero de entrega.
     */
    private RpcEnvelope.ErrorData errorOf(String op, FluxEvent event, Throwable e) {
        boolean reserved = event.dataclassification() == FluxEvent.DataClassification.CONFIDENTIAL
                || event.dataclassification() == FluxEvent.DataClassification.RESTRICTED;

        if (e instanceof FluxErrors.RetryableException || e instanceof FluxErrors.PermanentException
                || e instanceof FluxErrors.PoisonException) {
            FluxErrors.FluxException fe = (FluxErrors.FluxException) e;
            boolean retryable = e instanceof FluxErrors.RetryableException;
            // §5.3: sin codigo propio, RETRYABLE o PERMANENT. Un PoisonException es `permanent` (`poison` no viaja): su codigo por
            // defecto es el de la clase con la que viaja.
            String byDefault = retryable ? "RETRYABLE" : "PERMANENT";
            String given = fe.code();
            String code = byDefault;
            if (given != null && !given.equals(e.getClass().getSimpleName())) {
                if (validCode(given)) {
                    code = given;
                } else {
                    log(System.Logger.Level.WARNING, "el handler de \"" + op + "\" lanzo un error con el codigo "
                            + Envelope.jsonString(Text.truncateCodePoints(given, 70)) + ", que no cumple ^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*$ "
                            + "(04-errors.md §7): se responde con " + byDefault + ". Peticion " + event.id() + ".");
                }
            }
            Integer retryAfterMs = null;
            if (e instanceof FluxErrors.RetryableException re) {
                retryAfterMs = re.retryAfter().map(d -> (int) Math.min(Integer.MAX_VALUE, d.toMillis())).orElse(null);
            }
            return new RpcEnvelope.ErrorData(retryable ? "retryable" : "permanent", code,
                    message(e.getMessage(), "el servicio rechazo la peticion"), retryAfterMs);
        }

        Classifier.Detailed detailed = bus.classifier.classifyDetailed(e);
        Classification c = detailed.classification();
        if (detailed.recognized()) {
            boolean retryable = c.errorClass() == ErrorClass.RETRYABLE;
            log(System.Logger.Level.ERROR, "el handler de \"" + op + "\" fallo con " + c.code() + " (" + (retryable ? "retryable" : "permanent")
                    + ") en la peticion " + event.id() + (reserved ? "" : ": " + e));
            Integer retryAfterMs = retryable && c.retryAfter() != null
                    ? (int) Math.min(Integer.MAX_VALUE, c.retryAfter().toMillis()) : null;
            return new RpcEnvelope.ErrorData(retryable ? "retryable" : "permanent",
                    validCode(c.code()) ? c.code() : retryable ? "RETRYABLE" : "PERMANENT",
                    retryable ? "dependencia no disponible" : "una dependencia rechazo la operacion", retryAfterMs);
        }

        // DESCONOCIDO. La politica configurable de 04-errors.md §2.1 se aplica asi: `permanent` y `retryable-bounded` dan `permanent`;
        // `retryable` da `retryable`. El texto de la excepcion puede llevar valores de `data`, rutas del servidor o datos personales.
        boolean retryable = c.errorClass() == ErrorClass.RETRYABLE && c.maxAttempts() == null;
        log(System.Logger.Level.ERROR, "el handler de \"" + op + "\" lanzo un error sin clasificar en la peticion " + event.id() + ": "
                + (reserved ? e.getClass().getName() : e.toString())
                + (reserved ? " (el mensaje se omite: la peticion es confidential o restricted)" : "")
                + ". Quien llamo recibio `" + (retryable ? "retryable" : "permanent") + "` UNKNOWN con un mensaje generico.");
        return new RpcEnvelope.ErrorData(retryable ? "retryable" : "permanent", "UNKNOWN", "error interno del servicio", null);
    }

    private static boolean validCode(String code) {
        return code != null && code.length() <= RpcEnvelope.ERROR_CODE_MAX && RpcEnvelope.ERROR_CODE_PATTERN.matcher(code).matches();
    }

    private static String message(String m, String byDefault) {
        String text = m == null || Text.isBlank(m) ? byDefault : m;
        return Text.truncateCodePoints(text, RpcEnvelope.ERROR_MESSAGE_MAX);
    }

    /**
     * Contesta con un error {@code com.flux.rpc.error.v1}, SIEMPRE: callar dejaria a quien llama esperando su timeout para un fallo
     * que no se arregla esperando (§5.2). {@code request} es el sobre de la peticion, o {@code null} si no se pudo leer: entonces no
     * hay {@code correlationid} ni {@code tenantid} que heredar, y el error lleva {@code correlationid} = su propio {@code id},
     * {@code tenantid} = {@code system} y {@code causationid} = el {@code id} de la peticion solo si es una cadena valida (§5.2).
     */
    private void sendError(String replySubject, ResponderState r, FluxEvent request, String requestId, RpcEnvelope.ErrorData error) {
        FluxBus.ConnectOptions o = options();
        String op = r.operation;
        String id = Protocol.uuidV7();
        RpcEnvelope.ErrorData truncated = new RpcEnvelope.ErrorData(error.errorClass(), error.code(),
                message(error.message(), "el servicio rechazo la peticion"), error.retryAfterMs());
        // Sin sobre que leer no hay nada que heredar (§5.2): `correlationid` propio, `system`, y el `id` de la peticion solo si es valido.
        RpcEnvelope.RequestRef ref = request != null
                ? refOf(op, request)
                : new RpcEnvelope.RequestRef(op, requestId != null ? requestId : id, id, "system", null, null);
        FluxEvent event = RpcEnvelope.buildError(new RpcEnvelope.ErrorInput()
                .request(ref)
                .id(id)
                .source(bus.source)
                .time(Envelope.formatTime(Instant.now()))
                .producerversion(o.version)
                .error(truncated)
                .traceparent(request != null ? replyTraceparent(request) : null));
        if (request == null && requestId == null) {
            event = event.withoutCausationid();
        }
        byte[] bytes;
        try {
            bytes = encode(event, "respuesta", op);
        } catch (RuntimeException e) {
            log(System.Logger.Level.ERROR, "no se pudo contestar con un error a \"" + op + "\": " + e.getMessage());
            return;
        }
        publish(replySubject, bytes, op);
    }

    private void publish(String replySubject, byte[] bytes, String op) {
        try {
            bus.connection.publish(replySubject, bytes);
        } catch (RuntimeException e) {
            // La conexion se cerro (o se esta drenando) con la peticion en la mano: quien llamo vera un timeout.
            log(System.Logger.Level.ERROR, "no se pudo publicar la respuesta de \"" + op + "\": " + e.getMessage()
                    + ". Quien llamo vera un timeout.");
        }
    }

    private void poison(String op, FluxErrors.PoisonException error, String logLine) {
        log(System.Logger.Level.ERROR, "POISON en \"" + op + "\": " + logLine);
        // `raw` va vacio a proposito: una peticion PUEDE llevar datos personales (11-request-reply.md §3.6).
        FluxBus.PoisonListener listener = options().onPoison;
        if (listener != null) {
            listener.onPoison(new FluxBus.PoisonInfo("rpc." + op, error, new byte[0]));
        }
    }

    /**
     * El {@code id} de un mensaje que no se pudo leer como CloudEvent, SOLO si es una cadena valida (§5.2): no vacia, de 128
     * caracteres como maximo (puntos de codigo) y sin caracteres de control (U+0000–U+001F ni U+007F–U+009F). Es lo que el error
     * cita como {@code causationid}: un valor que quien llama no puede reconocer como suyo, o que llena un log, no se copia.
     */
    static String readableId(byte[] bytes) {
        try {
            JsonNode node = Envelope.mapper().readTree(bytes);
            JsonNode id = node != null && node.isObject() ? node.get("id") : null;
            if (id == null || !id.isTextual()) {
                return null;
            }
            String s = id.textValue();
            if (s.isEmpty() || Text.codePointLength(s) > 128) {
                return null;
            }
            for (int i = 0; i < s.length(); ) {
                int cp = s.codePointAt(i);
                if (cp <= 0x1F || (cp >= 0x7F && cp <= 0x9F)) {
                    return null;
                }
                i += Character.charCount(cp);
            }
            return s;
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    // ─── sobre, tamano, esquema ──────────────────────────────────────────────

    /**
     * Los bytes del mensaje, firmado si procede, y su tamano COMPROBADO antes de publicar: el minimo entre 1 MiB y el
     * {@code max_payload} que anuncia el broker, las cabeceras incluidas (no usamos ninguna). Si no cabe, {@code RPC_MESSAGE_TOO_LARGE}.
     */
    private byte[] encode(FluxEvent event, String what, String operation) {
        long maxPayload = bus.connection.getMaxPayload();
        int limit = (int) Math.min(Protocol.MAX_MESSAGE_BYTES, maxPayload > 0 ? maxPayload : Protocol.MAX_MESSAGE_BYTES);
        byte[] unsigned = Envelope.serializeUnbounded(event);
        if (unsigned.length > limit) {
            throw new RpcMessageTooLargeException(unsigned.length, limit, what, operation);
        }
        if (bus.signer == null) {
            return unsigned;
        }
        FluxEvent signed;
        try {
            signed = bus.signer.sign(event);
        } catch (Envelope.EnvelopeException e) {
            // El firmante serializa con el limite de 1 MiB de los eventos: un mensaje en el borde, con la firma encima, puede pasarse.
            throw new RpcMessageTooLargeException(unsigned.length + 200, limit, what, operation);
        }
        byte[] bytes = Envelope.serializeUnbounded(signed);
        if (bytes.length > limit) {
            throw new RpcMessageTooLargeException(bytes.length, limit, what, operation);
        }
        return bytes;
    }

    /** El {@code dataschema}: indice {@code rpc} del bundle (sin red) o {@code schemaBaseUrl} ({@code <major>.0.0}). Nunca por red. */
    private String schemaFor(String operation, RpcEnvelope.Side side) {
        FluxBus.ConnectOptions o = options();
        String fromBundle = o.validation != null && o.validation.bundle() != null
                ? o.validation.bundle().rpcSchemaUriFor(operation, side) : null;
        if (fromBundle != null && !fromBundle.isEmpty()) {
            return fromBundle;
        }
        boolean noBase = o.schemaBaseUrl == null || o.schemaBaseUrl.isEmpty();
        // Con L3 activa y un bundle, una operacion que NO esta en su indice se dice como lo que es —`SCHEMA_NOT_FOUND`, que lanza el
        // validador al no encontrar la URI derivada—, y no como «no hay dataschema»: el bundle ya es la fuente de los esquemas.
        if (noBase && o.validation != null && o.validation.bundle() != null && o.validation.mode() != Validation.Mode.OFF) {
            return RpcEnvelope.dataschema(operation, side, RpcEnvelope.SCHEMA_BASE);
        }
        if (noBase) {
            throw new FluxBus.FluxBusException("flux: no hay dataschema para la operacion \"" + operation + "\" (" + side.wire()
                    + "). Pasa un bundle con el indice `rpc` en validation(...) (ConnectOptions), o fija schemaBaseUrl para derivar el "
                    + "`<major>.0.0`: ni el nombre de la operacion ni nada de lo que hay en el sobre permiten adivinar el host de tus esquemas.");
        }
        return RpcEnvelope.dataschema(operation, side, o.schemaBaseUrl);
    }

    private static String firstNonEmpty(String... candidates) {
        for (String candidate : candidates) {
            if (candidate != null && !candidate.isEmpty()) {
                return candidate;
            }
        }
        return null;
    }
}
