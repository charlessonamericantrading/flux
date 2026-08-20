/*
 * Cliente de flux. Nivel de conformidad: L2.
 * Contrato normativo: specification/00-protocol.md §5
 *
 * Regla de diseno: este fichero NO expone ningun tipo de NATS en su API publica. Si lo
 * hiciera, sustituir el broker dejaria de ser un cambio de capa 0-1 y pasaria a tocar las
 * aplicaciones — que es exactamente lo que flux existe para evitar (00-protocol.md §3).
 * Los tipos de io.nats.client viven detras de campos y metodos privados; los que aparecen
 * en firmas package-private (assertConfigHonored) estan ahi para poder testear la
 * verificacion de config sin un broker, y no forman parte de la API publica.
 */
package com.flux;

import com.flux.FluxErrors.Classification;
import io.nats.client.Connection;
import io.nats.client.ConsumerContext;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.MessageConsumer;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.StreamContext;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.ReplayPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * El cliente de flux: publica y consume eventos.
 *
 * <p>Es seguro usarlo desde varios hilos.
 *
 * <pre>{@code
 * FluxBus bus = FluxBus.connect(new FluxBus.ConnectOptions()
 *         .servers("nats://localhost:4222")
 *         .service("pedidos-api")
 *         .environment("produccion")
 *         .version("3.4.1")
 *         .tenantId("acme")
 *         .schemaBaseUrl("https://schemas.internal"));
 * }</pre>
 */
public final class FluxBus implements AutoCloseable {

    // ─── Errores del cliente ─────────────────────────────────────────────────

    /**
     * Fallo de infraestructura al hablar con el broker.
     *
     * <p>Envuelve las excepciones comprobadas de jnats ({@code IOException},
     * {@code JetStreamApiException}) para que ningun tipo de NATS aparezca en una firma
     * publica. La causa original sigue disponible en {@link #getCause()} para el
     * diagnostico.
     */
    public static final class FluxBusException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public FluxBusException(String message, Throwable cause) {
            super(message, cause);
        }

        public FluxBusException(String message) {
            super(message);
        }
    }

    // ─── Opciones de conexion ────────────────────────────────────────────────

    /** Credenciales de NATS. NUNCA versionadas — 06-security.md §2. */
    public static final class Credentials {
        private String credsFile;
        private String token;
        private String user;
        private String password;

        /** Ruta a un fichero {@code .creds}. Es la forma recomendada. */
        public Credentials credsFile(String v) {
            this.credsFile = v;
            return this;
        }

        public Credentials token(String v) {
            this.token = v;
            return this;
        }

        public Credentials userPassword(String user, String password) {
            this.user = user;
            this.password = password;
            return this;
        }
    }

    /** Se invoca ante un POISON. Es el unico caso que debe despertar a alguien. */
    @FunctionalInterface
    public interface PoisonListener {
        void onPoison(PoisonInfo info);
    }

    /** Se invoca al enrutar cualquier evento a la DLQ. */
    @FunctionalInterface
    public interface DlqListener {
        void onDlq(DlqEventInfo info);
    }

    /** Un mensaje que no se pudo interpretar. {@code raw} es lo unico que queda al forense. */
    public record PoisonInfo(String subject, Throwable error, byte[] raw) {
    }

    /** Un evento enrutado a la DLQ. */
    public record DlqEventInfo(String subject, FluxEvent event, Classification classification) {
    }

    /** Configuracion de la conexion al bus. */
    public static final class ConnectOptions {
        private String servers = "nats://localhost:4222";
        private String service;
        private String environment;
        private String version;
        private String tenantId;
        private FluxEvent.DataClassification classification;
        private final Map<String, String> schemas = new HashMap<>();
        private String schemaBaseUrl;
        private Classifier.ClassifierOptions classifierOptions;
        private int streamReplicas = 1;
        private Credentials credentials;
        private EventContext.TraceparentSupplier traceparentSupplier;
        private PoisonListener onPoison;
        private DlqListener onDlq;
        private System.Logger logger;

        /** {@code nats://host:4222}, o varios separados por coma para HA. */
        public ConnectOptions servers(String v) {
            this.servers = v;
            return this;
        }

        /** Nombre del servicio. Alimenta {@code source} y los nombres de durable. */
        public ConnectOptions service(String v) {
            this.service = v;
            return this;
        }

        /** {@code produccion}, {@code staging}, {@code dev}. Alimenta {@code source}. */
        public ConnectOptions environment(String v) {
            this.environment = v;
            return this;
        }

        /** SemVer del servicio. Va en {@code producerversion}. */
        public ConnectOptions version(String v) {
            this.version = v;
            return this;
        }

        /** Tenant por defecto de los eventos publicados. Vacio significa {@code "system"}. */
        public ConnectOptions tenantId(String v) {
            this.tenantId = v;
            return this;
        }

        /** Clasificacion por defecto. Vacia significa {@code internal} — 06-security.md §5. */
        public ConnectOptions classification(FluxEvent.DataClassification v) {
            this.classification = v;
            return this;
        }

        /** Mapa exacto subject → URI de {@code dataschema}. Gana sobre {@code schemaBaseUrl}. */
        public ConnectOptions schema(String subject, String uri) {
            this.schemas.put(subject, uri);
            return this;
        }

        /** Base para derivar {@code dataschema} cuando el subject no esta en el mapa. */
        public ConnectOptions schemaBaseUrl(String v) {
            this.schemaBaseUrl = v;
            return this;
        }

        /** Politica de clasificacion de errores. Ver {@link Classifier}. */
        public ConnectOptions classifier(Classifier.ClassifierOptions v) {
            this.classifierOptions = v;
            return this;
        }

        /**
         * Replicas de los streams que el SDK crea si no existen.
         *
         * <p>El default es 1 porque el auto-provisionado es una comodidad de desarrollo:
         * con 3 no arrancaria contra un NATS de un solo nodo. <b>En produccion los streams
         * se provisionan por IaC con {@code replicas: 3}</b> (02-naming.md §3.2), no desde
         * el SDK — dejar que un servicio cualquiera cree el stream compartido de su dominio
         * con la configuracion que le apetezca es una carrera esperando a ocurrir.
         */
        public ConnectOptions streamReplicas(int v) {
            this.streamReplicas = v;
            return this;
        }

        public ConnectOptions credentials(Credentials v) {
            this.credentials = v;
            return this;
        }

        /** Ver {@link EventContext.TraceparentSupplier}. */
        public ConnectOptions traceparentSupplier(EventContext.TraceparentSupplier v) {
            this.traceparentSupplier = v;
            return this;
        }

        public ConnectOptions onPoison(PoisonListener v) {
            this.onPoison = v;
            return this;
        }

        public ConnectOptions onDlq(DlqListener v) {
            this.onDlq = v;
            return this;
        }

        /**
         * Logger opcional. {@code null} significa silencio.
         *
         * <p>{@link System.Logger} y no SLF4J: el SDK no debe imponer una fachada de logging
         * a toda aplicacion que lo use, y {@code System.Logger} tiene puentes a todas.
         */
        public ConnectOptions logger(System.Logger v) {
            this.logger = v;
            return this;
        }
    }

    // ─── Opciones de publicacion y suscripcion ───────────────────────────────

    /** Ajustes de una publicacion concreta. */
    public static final class PublishOptions {
        private String aggregateId;
        private String tenantId;
        private FluxEvent.DataClassification classification;
        private Instant time;
        private String partitionKey;

        /**
         * Id del agregado. Va al atributo {@code subject} de CloudEvents, NO al subject de
         * NATS — 01-envelope.md §2.1.
         */
        public PublishOptions aggregateId(String v) {
            this.aggregateId = v;
            return this;
        }

        public PublishOptions tenantId(String v) {
            this.tenantId = v;
            return this;
        }

        public PublishOptions classification(FluxEvent.DataClassification v) {
            this.classification = v;
            return this;
        }

        /**
         * Instante en que OCURRIO el hecho, si no es "ahora". El {@code time} de
         * CloudEvents no es el de publicacion — 01-envelope.md §2.
         */
        public PublishOptions time(Instant v) {
            this.time = v;
            return this;
        }

        /** Clave de particion. Por defecto, el {@code aggregateId} — 03-delivery.md §5. */
        public PublishOptions partitionKey(String v) {
            this.partitionKey = v;
            return this;
        }
    }

    /** Ajustes de una suscripcion. */
    public static final class SubscribeOptions {
        private String durable;
        private Integer maxAckPending;
        private String tenantId;

        /** Durable explicito. Por defecto se deriva del servicio y el subject. */
        public SubscribeOptions durable(String v) {
            this.durable = v;
            return this;
        }

        public SubscribeOptions maxAckPending(int v) {
            this.maxAckPending = v;
            return this;
        }

        /** Descarta —con ack— los eventos de otros tenants antes del handler — 06-security.md §4. */
        public SubscribeOptions tenantId(String v) {
            this.tenantId = v;
            return this;
        }
    }

    // ─── Handler ─────────────────────────────────────────────────────────────

    /**
     * Metadatos de ESTA entrega concreta del evento.
     *
     * @param attempt     numero de entrega, empezando en 1. Si es &gt; 1, este evento YA se
     *                    intento procesar antes: la idempotencia no es opcional
     *                    — 03-delivery.md §4.
     * @param maxAttempts {@link Protocol#DEFAULT_MAX_DELIVER}. Al alcanzarlo, el SDK enruta
     *                    a la DLQ.
     * @param subject     subject de NATS por el que llego el evento.
     * @param durable     nombre del consumidor. Aparece como {@code dlqconsumer} en la DLQ.
     */
    public record Delivery(int attempt, int maxAttempts, String subject, String durable) {
    }

    /**
     * Procesa un evento.
     *
     * <p>Volver del metodo sin lanzar ACK-ea el evento. Lanzar clasifica el error segun
     * {@link Classifier} y produce nak, term o term+alerta.
     *
     * <p>⚠️ Divergencia con Node: alli el handler recibe un {@code ctx} con un metodo
     * {@code ack()} explicito que ademas es un no-op (devolver del handler ya hace ack).
     * Aqui volver sin excepcion ES el ack explicito. El requisito del protocolo —nunca
     * auto-ack— se cumple igual: el SDK jamas confirma un mensaje antes de que el handler
     * termine.
     *
     * <p>El {@link EventContext} que llega es el del evento entrante; pasalo a
     * {@link #publish(EventContext, String, Object)} para que {@code correlationid},
     * {@code causationid} y {@code traceparent} se propaguen solos. Ver
     * {@link EventContext}.
     *
     * <p>El handler DEBE ser idempotente. La garantia es at-least-once: los duplicados
     * llegan, no son un fallo — 03-delivery.md §1.
     *
     * <p>{@code throws Exception} en la firma es deliberado: obliga a que el codigo del
     * handler no tenga que envolver cada {@code IOException} en un
     * {@code RuntimeException} solo para poder propagarla, y deja que el clasificador vea
     * el tipo original — que es justo lo que necesita para reconocer un
     * {@code SocketException}.
     */
    @FunctionalInterface
    public interface Handler {
        void handle(EventContext context, FluxEvent event, Delivery delivery) throws Exception;
    }

    /** Una suscripcion viva. */
    public final class Subscription implements AutoCloseable {
        private final String subject;
        private final String durable;
        private final MessageConsumer consumer;
        private volatile boolean stopped;

        private Subscription(String subject, String durable, MessageConsumer consumer) {
            this.subject = subject;
            this.durable = durable;
            this.consumer = consumer;
        }

        public String subject() {
            return subject;
        }

        public String durable() {
            return durable;
        }

        /**
         * Detiene la entrega. Los mensajes ya en el bufer se descartan y se reentregaran:
         * eso es correcto, y es exactamente el caso que cubre la idempotencia.
         */
        public void unsubscribe() {
            if (stopped) {
                return;
            }
            stopped = true;
            consumer.stop();
            subscriptions.remove(this);
        }

        @Override
        public void close() {
            unsubscribe();
        }
    }

    // ─── Estado ──────────────────────────────────────────────────────────────

    private final Connection connection;
    private final JetStream jetStream;
    private final JetStreamManagement jsm;
    private final ConnectOptions options;
    private final Classifier classifier;
    private final String source;
    private final Set<Subscription> subscriptions =
            Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<String> ensuredStreams = ConcurrentHashMap.newKeySet();

    /**
     * Hilos que emiten los WIP.
     *
     * <p>Son daemon para que un {@code close()} olvidado no impida que la JVM termine. Son
     * dos y no uno porque {@code inProgress()} publica un mensaje y puede bloquearse si el
     * bufer de salida esta lleno: con un solo hilo, un WIP atascado dejaria sin renovar el
     * plazo a TODAS las suscripciones a la vez, que es justo la reentrega concurrente que el
     * WIP existe para evitar — 03-delivery.md §2.1.
     */
    private final ScheduledExecutorService wipScheduler = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "flux-wip");
        t.setDaemon(true);
        return t;
    });

    private FluxBus(Connection connection, JetStream jetStream, JetStreamManagement jsm,
                    ConnectOptions options) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.jsm = jsm;
        this.options = options;
        this.classifier = new Classifier(options.classifierOptions);
        this.source = Protocol.sourceUri(options.environment, options.service);
    }

    // ─── connect ─────────────────────────────────────────────────────────────

    /**
     * Conecta al bus.
     *
     * <p>Reconecta indefinidamente con backoff y JITTER: sin jitter, mil servicios
     * reconectan en el mismo milisegundo y tiran el cluster que acaba de levantarse
     * — 03-delivery.md §6.
     */
    public static FluxBus connect(ConnectOptions options) {
        if (options == null || options.service == null || options.environment == null
                || options.version == null) {
            throw new IllegalArgumentException("flux: service, environment y version son obligatorios: "
                    + "alimentan `source` y `producerversion`, que son atributos obligatorios del envelope");
        }
        // El nombre de servicio se valida AQUI y no al suscribirse: es lo que exige
        // protocol.json (naming.service). Si se validara solo en durableName(), un servicio
        // que solo publica nunca descubriria que su nombre incumple el patron.
        Protocol.validateService(options.service);

        Options.Builder builder = new Options.Builder()
                .servers(options.servers.split(","))
                .connectionName(options.service + "@" + options.environment)
                .maxReconnects(-1)
                .reconnectWait(Duration.ofSeconds(1))
                .reconnectJitter(Duration.ofMillis(500))
                .reconnectJitterTls(Duration.ofSeconds(1));

        Credentials credentials = options.credentials;
        if (credentials != null) {
            if (credentials.credsFile != null) {
                builder.credentialPath(credentials.credsFile);
            } else if (credentials.token != null) {
                builder.token(credentials.token.toCharArray());
            } else if (credentials.user != null) {
                builder.userInfo(credentials.user, credentials.password);
            }
        }

        try {
            // connectReconnectOnConnect y no connect: un servicio que arranca antes que el
            // broker no debe morir en el primer intento. Es el equivalente del
            // waitOnFirstConnect de Node y del RetryOnFailedConnect de Go.
            Connection connection = Nats.connectReconnectOnConnect(builder.build());
            return new FluxBus(connection, connection.jetStream(), connection.jetStreamManagement(),
                    options);
        } catch (InterruptedException e) {
            // Restaurar el flag es obligatorio: tragarselo deja el hilo marcado como no
            // interrumpido y rompe cualquier apagado ordenado aguas arriba.
            Thread.currentThread().interrupt();
            throw new FluxBusException("flux: conexion interrumpida", e);
        } catch (IOException e) {
            throw new FluxBusException("flux: no se pudo conectar a \"" + options.servers + "\"", e);
        }
    }

    /** Informa si la conexion sigue viva. Es el healthcheck que exige 03-delivery.md §6. */
    public boolean isConnected() {
        return connection.getStatus() == Connection.Status.CONNECTED;
    }

    /** Detiene las suscripciones y drena la conexion. */
    @Override
    public void close() {
        for (Subscription s : new ArrayList<>(subscriptions)) {
            s.unsubscribe();
        }
        subscriptions.clear();
        wipScheduler.shutdownNow();
        try {
            // drain y no close: los acks pendientes no se pierden en silencio.
            connection.drain(Duration.ofSeconds(10)).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log(System.Logger.Level.WARNING, "fallo al drenar la conexion: " + e.getMessage());
        }
    }

    // ─── publish ─────────────────────────────────────────────────────────────

    /** Publica un evento que nace de cero (una ruta HTTP, un cron). */
    public FluxEvent publish(String subject, Object data) {
        return publish(EventContext.ROOT, subject, data, new PublishOptions());
    }

    /** Publica un evento que nace de cero, con opciones. */
    public FluxEvent publish(String subject, Object data, PublishOptions publishOptions) {
        return publish(EventContext.ROOT, subject, data, publishOptions);
    }

    /** Publica un evento derivado del que se esta procesando. */
    public FluxEvent publish(EventContext parent, String subject, Object data) {
        return publish(parent, subject, data, new PublishOptions());
    }

    /**
     * Construye el envelope y publica el evento.
     *
     * <p>El desarrollador solo escribe subject, {@code data} y opcionalmente
     * {@code aggregateId}. Todo lo demas —{@code id}, {@code source}, {@code time},
     * {@code specversion}, {@code type}, {@code dataschema}, {@code correlationid},
     * {@code causationid}, {@code producerversion}, {@code traceparent}— lo rellena el
     * SDK. Si tu codigo asigna alguno de esos a mano, esta mal — 01-envelope.md §5.
     *
     * @param parent contexto del evento en curso cuando se publica desde un handler.
     *               {@link EventContext#ROOT} si el evento nace de cero. Pasar ROOT dentro
     *               de un handler ROMPE la cadena de correlacion en silencio.
     */
    public FluxEvent publish(EventContext parent, String subject, Object data,
                             PublishOptions publishOptions) {
        Protocol.ParsedSubject parsed = Protocol.parseSubject(subject); // falla temprano y con mensaje util
        PublishOptions po = publishOptions != null ? publishOptions : new PublishOptions();
        EventContext ctx = parent != null ? parent : EventContext.ROOT;

        ensureStream(parsed.domain());

        // UUIDv7: monotonico en el tiempo, asi que ordenar por `id` dentro de un mismo
        // `source` equivale a ordenar por instante de generacion — util al reconstruir
        // historiales desde la DLQ (01-envelope.md §2.4).
        String id = Protocol.uuidV7();

        Envelope.BuildEventInput input = new Envelope.BuildEventInput()
                .subject(subject)
                .data(data)
                .id(id)
                .source(source)
                .producerVersion(options.version)
                .dataSchema(schemaFor(subject, parsed))
                .time(po.time)
                .aggregateId(po.aggregateId)
                .partitionKey(po.partitionKey);

        // El contexto heredado gana sobre el default de la conexion: un evento derivado
        // pertenece al tenant del evento que lo causo, no al del servicio.
        input.tenantId(firstNonEmpty(po.tenantId, ctx.tenantId(), options.tenantId, "system"));

        input.dataClassification(po.classification != null
                ? po.classification
                : (options.classification != null
                        ? options.classification
                        : FluxEvent.DataClassification.INTERNAL));

        // Si el evento no nace de otro, correlationid se inicializa con su propio id
        // — 01-envelope.md §3.1.
        if (!ctx.isRoot()) {
            input.correlationId(ctx.correlationId());
            input.causationId(ctx.causationId());
        } else {
            input.correlationId(id);
        }

        if (ctx.traceparent() != null) {
            input.traceparent(ctx.traceparent());
            input.tracestate(ctx.tracestate());
        } else if (options.traceparentSupplier != null) {
            input.traceparent(emptyToNull(options.traceparentSupplier.get()));
        }

        FluxEvent event = Envelope.buildEvent(input);
        byte[] payload = Envelope.serialize(event);

        try {
            // messageId pone la cabecera Nats-Msg-Id. Deduplica reintentos de PUBLICACION
            // dentro de duplicate_window: un publish que no recibe el ACK del broker por un
            // corte de red y se reintenta no deja dos copias en el stream.
            //
            // NO deduplica reentregas de consumo. Un nak reentrega el mismo mensaje con el
            // mismo Nats-Msg-Id y eso es correcto y deseado. Nunca sustituye a la
            // idempotencia del consumidor — 03-delivery.md §3.
            jetStream.publish(subject, payload,
                    io.nats.client.PublishOptions.builder().messageId(event.id()).build());
        } catch (IOException | JetStreamApiException e) {
            throw new FluxBusException("flux: fallo al publicar en \"" + subject + "\"", e);
        }
        return event;
    }

    /** Resuelve la URI de {@code dataschema} del subject. */
    private String schemaFor(String subject, Protocol.ParsedSubject parsed) {
        String explicit = options.schemas.get(subject);
        if (explicit != null && !explicit.isEmpty()) {
            return explicit;
        }
        if (options.schemaBaseUrl == null || options.schemaBaseUrl.isEmpty()) {
            throw new FluxBusException("flux: no hay dataschema para \"" + subject
                    + "\". Declara schema(\"" + subject + "\", …) o schemaBaseUrl en ConnectOptions");
        }
        String base = options.schemaBaseUrl.endsWith("/")
                ? options.schemaBaseUrl.substring(0, options.schemaBaseUrl.length() - 1)
                : options.schemaBaseUrl;
        // Sin registro exacto solo se puede asumir el .0.0 del mayor. Un SDK L3 resolvera el
        // minor real contra el Schema Registry — 00-protocol.md §5.
        return base + "/" + parsed.domain() + "/" + parsed.aggregate() + "/" + parsed.event()
                + "/" + parsed.major() + ".0.0.json";
    }

    // ─── subscribe ───────────────────────────────────────────────────────────

    /** Crea (o reutiliza) el durable consumer canonico y empieza a entregar. */
    public Subscription subscribe(String subject, Handler handler) {
        return subscribe(subject, handler, new SubscribeOptions());
    }

    /**
     * Crea (o reutiliza) el durable consumer canonico y empieza a entregar.
     *
     * @throws ConsumerConfigMismatchException si el servidor no honro la config solicitada.
     *                                         Requisito L2 — 03-delivery.md §2.1.
     */
    public Subscription subscribe(String subject, Handler handler, SubscribeOptions subscribeOptions) {
        Protocol.ParsedSubject parsed = Protocol.parseSubject(subject);
        SubscribeOptions so = subscribeOptions != null ? subscribeOptions : new SubscribeOptions();

        ensureStream(parsed.domain());
        ensureDlqStream(parsed.domain());

        String durable = so.durable != null ? so.durable
                : Protocol.durableName(options.service, subject);
        int maxAckPending = so.maxAckPending != null ? so.maxAckPending
                : Protocol.DEFAULT_MAX_ACK_PENDING;

        ConsumerConfiguration requested = ConsumerConfiguration.builder()
                .durable(durable)
                .filterSubject(subject)
                .ackPolicy(AckPolicy.Explicit)
                // ack_wait DEBE ser backoff[0]: JetStream lo sobrescribe con ese valor sin
                // avisar. Ver 03-delivery.md §2.1 y Protocol.canonicalBackoff().
                .ackWait(Protocol.DEFAULT_ACK_WAIT)
                .maxDeliver(Protocol.DEFAULT_MAX_DELIVER)
                .backoff(Protocol.canonicalBackoff().toArray(new Duration[0]))
                .maxAckPending(maxAckPending)
                .deliverPolicy(DeliverPolicy.All)
                .replayPolicy(ReplayPolicy.Instant)
                .build();

        String stream = Protocol.streamName(parsed.domain());
        try {
            StreamContext streamContext = jetStream.getStreamContext(stream);
            ConsumerContext consumerContext = streamContext.createOrUpdateConsumer(requested);

            // El servidor no siempre devuelve lo que se le pide. Requisito L2.
            ConsumerConfiguration effective = consumerContext.getConsumerInfo().getConsumerConfiguration();
            assertConfigHonored(durable, requested, effective);

            MessageConsumer consumer = consumerContext.consume(msg -> {
                try {
                    dispatch(msg, subject, durable, handler, so);
                } catch (Throwable t) {
                    // Sin este catch, un fallo inesperado del despacho —por ejemplo que la
                    // publicacion en la DLQ falle— sube a la libreria y puede detener la
                    // entrega EN SILENCIO: sin log, y con la conexion en CONNECTED, asi que
                    // el healthcheck sigue diciendo que todo va bien. Un consumidor muerto
                    // que parece vivo es peor que uno que se cae.
                    //
                    // El mensaje NO se confirma ni se termina: se deja reentregar.
                    log(System.Logger.Level.ERROR, "fallo inesperado despachando " + subject + ": "
                            + t + ". El mensaje no se confirma y sera reentregado.");
                }
            });

            Subscription subscription = new Subscription(subject, durable, consumer);
            subscriptions.add(subscription);
            return subscription;
        } catch (IOException | JetStreamApiException e) {
            throw new FluxBusException(
                    "flux: no se pudo crear el consumidor \"" + durable + "\" en \"" + stream + "\"", e);
        }
    }

    /**
     * Verifica que el servidor aplico lo solicitado.
     *
     * <p>Requisito L2 y unica defensa contra sobrescrituras silenciosas — 03-delivery.md
     * §2.1. Se comprueban solo los campos que el SDK pide explicitamente: el resto lo
     * rellena el servidor con sus defaults y compararlos daria falsos positivos.
     *
     * <p>Package-private para poder testearla sin broker, igual que en Go.
     */
    static void assertConfigHonored(String durable, ConsumerConfiguration requested,
                                    ConsumerConfiguration effective) {
        List<ConsumerConfigMismatchException.Difference> diffs = new ArrayList<>();

        if (!equalDurations(requested.getAckWait(), effective.getAckWait())) {
            diffs.add(new ConsumerConfigMismatchException.Difference(
                    "ack_wait", requested.getAckWait(), effective.getAckWait()));
        }
        if (requested.getMaxDeliver() != effective.getMaxDeliver()) {
            diffs.add(new ConsumerConfigMismatchException.Difference(
                    "max_deliver", requested.getMaxDeliver(), effective.getMaxDeliver()));
        }
        if (requested.getMaxAckPending() != effective.getMaxAckPending()) {
            diffs.add(new ConsumerConfigMismatchException.Difference(
                    "max_ack_pending", requested.getMaxAckPending(), effective.getMaxAckPending()));
        }
        if (requested.getAckPolicy() != effective.getAckPolicy()) {
            diffs.add(new ConsumerConfigMismatchException.Difference(
                    "ack_policy", requested.getAckPolicy(), effective.getAckPolicy()));
        }
        if (!java.util.Objects.equals(requested.getBackoff(), effective.getBackoff())) {
            diffs.add(new ConsumerConfigMismatchException.Difference(
                    "backoff", requested.getBackoff(), effective.getBackoff()));
        }

        // Invariante del protocolo, no solo del SDK: aunque el servidor haya devuelto lo
        // mismo que se le pidio, ack_wait y backoff[0] tienen que coincidir. Si alguien
        // cambia el backoff canonico y olvida DEFAULT_ACK_WAIT, esto lo caza aqui —sobre la
        // config EFECTIVA— y no en produccion a las 3 de la manana.
        List<Duration> effectiveBackoff = effective.getBackoff();
        if (effectiveBackoff != null && !effectiveBackoff.isEmpty()
                && !equalDurations(effective.getAckWait(), effectiveBackoff.get(0))) {
            diffs.add(new ConsumerConfigMismatchException.Difference(
                    "ack_wait == backoff[0]", effectiveBackoff.get(0), effective.getAckWait()));
        }

        if (!diffs.isEmpty()) {
            throw new ConsumerConfigMismatchException(durable, diffs);
        }
    }

    private static boolean equalDurations(Duration a, Duration b) {
        return a == null ? b == null : a.equals(b);
    }

    // ─── despacho ────────────────────────────────────────────────────────────

    private void dispatch(io.nats.client.Message msg, String subject, String durable,
                          Handler handler, SubscribeOptions so) {
        // deliveredCount empieza en 1 en la primera entrega, no en 0.
        int attempt = 1;
        try {
            long delivered = msg.metaData().deliveredCount();
            if (delivered > 0) {
                attempt = (int) delivered;
            }
        } catch (RuntimeException e) {
            // Un mensaje sin metadatos de JetStream no deberia llegar aqui; si llega, se
            // trata como primera entrega en vez de tirar el despacho.
            log(System.Logger.Level.WARNING, "mensaje sin metadatos de JetStream en " + subject);
        }

        // POISON se detecta ANTES del handler: el mensaje no es interpretable, asi que el
        // handler nunca llega a verlo — 04-errors.md §1.3.
        FluxEvent event;
        try {
            event = Envelope.parseEvent(msg.getData());
        } catch (RuntimeException e) {
            handlePoison(subject, durable, msg, e);
            return;
        }

        if (so.tenantId != null && !so.tenantId.equals(event.tenantid())) {
            msg.ack(); // no es para nosotros
            return;
        }

        // WIP mientras el handler vive: resetea el temporizador de reentrega en el servidor
        // y evita que el mismo evento se ejecute en concurrencia consigo mismo. Cada
        // ack_wait/2 para que un ciclo perdido no agote el plazo — 03-delivery.md §2.1.
        long wipPeriodMillis = Protocol.DEFAULT_ACK_WAIT.toMillis() / 2;
        ScheduledFuture<?> wip = wipScheduler.scheduleAtFixedRate(() -> {
            try {
                msg.inProgress();
            } catch (RuntimeException ignored) {
                // El mensaje ya esta resuelto o la conexion se cayo; el siguiente ciclo
                // tampoco hara nada y el finally cancelara la tarea.
            }
        }, wipPeriodMillis, wipPeriodMillis, TimeUnit.MILLISECONDS);

        Delivery delivery = new Delivery(attempt, Protocol.DEFAULT_MAX_DELIVER, subject, durable);

        try {
            Throwable handlerError = invoke(handler, event, delivery);
            if (handlerError == null) {
                msg.ack();
                return;
            }

            Classification classification = classifier.classify(handlerError);

            // El presupuesto efectivo es el menor entre el del consumidor y el que la
            // clasificacion imponga para ESTE error. Asi un error desconocido agota su
            // presupuesto acotado (2 entregas) sin recortar los 6 intentos de un
            // SocketException reconocido — 04-errors.md §2.1.
            int budget = effectiveBudget(Protocol.DEFAULT_MAX_DELIVER, classification.maxAttempts());

            if (classification.errorClass() == ErrorClass.RETRYABLE && attempt < budget) {
                // Retraso explicito segun el backoff canonico en vez de dejar expirar
                // ack_wait: no retiene la ranura de max_ack_pending 30 s de mas.
                Duration delay = classification.retryAfter() != null
                        ? classification.retryAfter()
                        : backoffFor(attempt);
                log(System.Logger.Level.WARNING, "RETRYABLE " + classification.code() + " en " + subject
                        + " (intento " + attempt + "/" + budget + "), reintento en " + delay);
                msg.nakWithDelay(delay);
                return;
            }

            FluxEvent.DlqReason reason = classification.errorClass() == ErrorClass.RETRYABLE
                    ? FluxEvent.DlqReason.RETRYABLE // agoto los reintentos
                    : classification.errorClass().toDlqReason();

            try {
                sendToDlq(subject, event, durable, attempt, reason,
                        classification.code() + ": " + messageOf(handlerError));
            } catch (Exception dlqError) {
                // NO se hace term(): terminar aqui borraria el evento sin haberlo guardado
                // en ningun sitio. Se deja reentregar — es preferible reprocesar un evento
                // que perderlo sin rastro. La causa suele ser un problema del broker, no del
                // evento.
                log(System.Logger.Level.ERROR, "no se pudo publicar en la DLQ para " + subject
                        + " (evento " + event.id() + "): " + dlqError
                        + ". El mensaje NO se termina; se reentregara.");
                return;
            }

            if (options.onDlq != null) {
                options.onDlq.onDlq(new DlqEventInfo(subject, event, classification));
            }
            log(System.Logger.Level.ERROR, "DLQ (" + reason.wire() + ") " + classification.code()
                    + " en " + subject + " tras " + attempt + " intento(s): " + messageOf(handlerError));
            msg.term();
        } finally {
            wip.cancel(false);
        }
    }

    /**
     * Presupuesto efectivo de entregas para un error.
     *
     * <p>{@code min(max_deliver del consumidor, maxAttempts de la clasificacion)}. El
     * segundo es {@code null} salvo para la politica RETRYABLE_BOUNDED, porque
     * {@code max_deliver} es por consumidor y no por mensaje — 04-errors.md §2.1.
     *
     * <p>Package-private para poder testear la aritmetica sin broker.
     */
    static int effectiveBudget(int consumerMaxDeliver, Integer classificationMaxAttempts) {
        if (classificationMaxAttempts == null) {
            return consumerMaxDeliver;
        }
        return Math.min(consumerMaxDeliver, classificationMaxAttempts);
    }

    /** Entrada de backoff que toca para este intento, saturando en la ultima. */
    private static Duration backoffFor(int attempt) {
        List<Duration> backoff = Protocol.canonicalBackoff();
        int index = Math.min(attempt - 1, backoff.size() - 1);
        return backoff.get(Math.max(index, 0));
    }

    /**
     * Invoca al handler y devuelve el error, o {@code null} si termino bien.
     *
     * <p>Captura {@link Throwable} y no {@link Exception}: un {@code StackOverflowError} o
     * un {@code NoClassDefFoundError} dentro del handler mataria el hilo de despacho y con
     * el la suscripcion entera. Es el equivalente del {@code recover()} que el SDK de Go
     * pone alrededor del handler. Un {@code InterruptedException} se trata aparte para no
     * tragarse la senal de apagado.
     *
     * <p>⚠️ Divergencia con Go: alli un panico se convierte en PERMANENT ("un panico es un
     * bug del consumidor y reintentarlo cinco veces no lo arregla"). Aqui el error pasa por
     * el clasificador como cualquier otro, porque en Java lanzar es el canal normal de
     * error —un NullPointerException no es excepcional como un panico de Go— y porque el
     * default de la spec para lo desconocido ya es el correcto para un bug: RETRYABLE
     * acotado a 2 entregas, es decir DLQ en ~30 s (04-errors.md §2.1).
     */
    private static Throwable invoke(Handler handler, FluxEvent event, Delivery delivery) {
        try {
            handler.handle(EventContext.fromEvent(event), event, delivery);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return e;
        } catch (Throwable t) {
            return t;
        }
    }

    private static String messageOf(Throwable t) {
        String message = t.getMessage();
        return message != null ? message : t.getClass().getName();
    }

    private void handlePoison(String subject, String durable, io.nats.client.Message msg,
                              RuntimeException error) {
        if (options.onPoison != null) {
            options.onPoison.onPoison(new PoisonInfo(subject, error, msg.getData()));
        }
        log(System.Logger.Level.ERROR, "POISON en " + subject + ": " + error.getMessage());
        try {
            sendRawToDlq(subject, msg.getData(), durable, messageOf(error));
        } catch (Exception e) {
            // Igual que en el despacho: no se termina lo que no se ha llegado a guardar.
            log(System.Logger.Level.ERROR, "no se pudo guardar el POISON de " + subject
                    + " en la DLQ; se reentregara: " + e);
            return;
        }
        msg.term();
    }

    /** Publica el CloudEvent original integro mas las extensiones {@code dlq*}. */
    private void sendToDlq(String subject, FluxEvent event, String consumer, int attempts,
                           FluxEvent.DlqReason reason, String errorText)
            throws IOException, JetStreamApiException {
        FluxEvent dlqEvent = Envelope.toDlqEvent(event,
                new Envelope.DlqInfo(reason, attempts, consumer, errorText));
        // El msgID incluye el consumidor: dos consumidores distintos del mismo evento
        // producen dos entradas de DLQ, y son dos hechos distintos.
        jetStream.publish(Protocol.dlqSubject(subject), Envelope.serialize(dlqEvent),
                io.nats.client.PublishOptions.builder()
                        .messageId("dlq-" + event.id() + "-" + consumer)
                        .build());
    }

    /**
     * Guarda un mensaje ininterpretable.
     *
     * <p>Un POISON no tiene CloudEvent que preservar, asi que se envuelve el cuerpo crudo
     * en un envelope sintetico del dominio {@code system}. Es la unica excepcion a la regla
     * de "el mensaje de DLQ es el original integro": no habia original que preservar.
     */
    private void sendRawToDlq(String subject, byte[] raw, String consumer, String errorText)
            throws IOException, JetStreamApiException {
        String id = Protocol.uuidV7();
        String now = Envelope.formatTime(Instant.now());

        // El base64 se recorta para que el envelope quepa en 1 MiB con margen para el resto
        // de atributos.
        String encoded = Base64.getEncoder().encodeToString(raw);
        if (encoded.length() > 700_000) {
            encoded = encoded.substring(0, 700_000);
        }
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("originalSubject", subject);
        data.put("rawBase64", encoded);
        data.put("rawBytes", raw.length);

        FluxEvent envelope = new Envelope.BuildEventInput()
                .subject("system.poison.v1.capturado")
                .data(data)
                .id(id)
                .source(source)
                .producerVersion(options.version)
                .dataSchema("https://schemas.internal/system/poison/capturado/1.0.0.json")
                .correlationId(id)
                .tenantId("system")
                .dataClassification(FluxEvent.DataClassification.INTERNAL)
                .build()
                .withDlq(FluxEvent.DlqReason.POISON, 1, consumer,
                        Envelope.truncate(errorText, 1024), now);

        jetStream.publish(Protocol.dlqSubject(subject), Envelope.serialize(envelope),
                io.nats.client.PublishOptions.builder().messageId("poison-" + id).build());
    }

    // ─── provision de streams ────────────────────────────────────────────────

    private void ensureStream(String domain) {
        String name = Protocol.streamName(domain);
        ensure(name, StreamConfiguration.builder()
                .name(name)
                .subjects(domain + ".>")
                .storageType(StorageType.File)
                // Limits: el stream retiene por edad, no hasta que alguien consuma. Un
                // consumidor lento no debe poder hacer crecer el disco sin limite.
                .retentionPolicy(RetentionPolicy.Limits)
                .discardPolicy(DiscardPolicy.Old)
                .maxAge(Protocol.STREAM_MAX_AGE)
                // duplicateWindow solo aplica a publicaciones. Ver Protocol.DUPLICATE_WINDOW.
                .duplicateWindow(Protocol.DUPLICATE_WINDOW)
                .replicas(options.streamReplicas)
                .build());
    }

    private void ensureDlqStream(String domain) {
        String name = Protocol.dlqStreamName(domain);
        // Prefijo `dlq.`, nunca sufijo: con sufijo encajaria con `<domain>.>` y el stream
        // principal capturaria sus propios muertos — 02-naming.md §3.1.
        ensure(name, StreamConfiguration.builder()
                .name(name)
                .subjects("dlq." + domain + ".>")
                .storageType(StorageType.File)
                .retentionPolicy(RetentionPolicy.Limits)
                .discardPolicy(DiscardPolicy.Old)
                .maxAge(Protocol.DLQ_STREAM_MAX_AGE)
                .replicas(options.streamReplicas)
                .build());
    }

    /**
     * Crea el stream si no existe, y si existe lo deja tal cual.
     *
     * <p>No se usa {@code updateStream} a proposito: un SDK que actualiza streams ajenos en
     * cada arranque puede pisar en silencio la configuracion que puso el equipo de
     * plataforma (replicas, limites, mirrors). En produccion los streams los provisiona
     * infraestructura; esto es una comodidad de desarrollo — 02-naming.md §3.2.
     */
    private void ensure(String name, StreamConfiguration config) {
        if (ensuredStreams.contains(name)) {
            return;
        }
        try {
            jsm.getStreamInfo(name);
        } catch (JetStreamApiException e) {
            // Se distingue "no existe" de cualquier otro fallo de la API por el codigo y no
            // por "hubo excepcion": un error de permisos tambien llega aqui, y tratarlo como
            // "no existe" produciria un intento de creacion que falla con un mensaje que no
            // menciona los permisos.
            if (e.getApiErrorCode() != JS_STREAM_NOT_FOUND) {
                throw new FluxBusException("flux: no se pudo consultar el stream \"" + name + "\"", e);
            }
            createStream(name, config);
        } catch (IOException e) {
            throw new FluxBusException("flux: no se pudo consultar el stream \"" + name + "\"", e);
        }
        ensuredStreams.add(name);
    }

    private void createStream(String name, StreamConfiguration config) {
        try {
            jsm.addStream(config);
        } catch (JetStreamApiException e) {
            if (e.getApiErrorCode() != JS_STREAM_NAME_IN_USE) {
                throw new FluxBusException("flux: no se pudo crear el stream \"" + name + "\"", e);
            }
            // Otro proceso lo creo entre el getStreamInfo y el addStream. No es un error.
            log(System.Logger.Level.WARNING, "el stream " + name + " ya existia al crearlo");
        } catch (IOException e) {
            throw new FluxBusException("flux: no se pudo crear el stream \"" + name + "\"", e);
        }
    }

    /**
     * Codigos de error de la API de JetStream. jnats no los expone como constantes, asi que
     * se declaran aqui en vez de tratar cualquier excepcion como "el stream no existe".
     */
    private static final int JS_STREAM_NOT_FOUND = 10059;

    private static final int JS_STREAM_NAME_IN_USE = 10058;

    // ─── utilidades ──────────────────────────────────────────────────────────

    private static String firstNonEmpty(String... candidates) {
        for (String candidate : candidates) {
            if (candidate != null && !candidate.isEmpty()) {
                return candidate;
            }
        }
        return null;
    }

    private static String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }

    private void log(System.Logger.Level level, String message) {
        if (options.logger != null) {
            options.logger.log(level, "[flux] " + message);
        }
    }
}
