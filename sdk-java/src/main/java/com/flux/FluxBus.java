/*
 * Cliente de flux. Nivel de conformidad: L3 (la validacion de esquema es opt-in).
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
import com.flux.SecurityErrors.NotProvisionedException;
import com.flux.SecurityErrors.PermissionDeniedException;
import com.flux.SecurityErrors.ResourceKind;
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
import io.nats.client.api.ConsumerInfo;
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
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;

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

    /**
     * Credenciales de NATS. NUNCA versionadas — 06-security.md §2. Se admite UN solo metodo:
     *
     * <table>
     *   <caption>Metodos</caption>
     *   <tr><td>{@code .creds} (JWT + nkey), el recomendado</td><td>{@link #credsFile(String)} o {@link #credsContent(String)}</td></tr>
     *   <tr><td>nkey</td><td>{@link #nkeySeed(String)}</td></tr>
     *   <tr><td>token</td><td>{@link #token(String)}</td></tr>
     *   <tr><td>usuario y contrasena</td><td>{@link #userPassword(String, String)}</td></tr>
     * </table>
     *
     * <p>Todo lo mal formado lanza {@code INVALID_CREDENTIALS} en {@code connect()}, ANTES de abrir ningun
     * socket: un valor vacio (casi siempre una variable de entorno sin definir) no degrada a «sin
     * credenciales».
     */
    public static final class Credentials {
        private String credsFile;
        private String credsContent;
        private String nkeySeed;
        private String token;
        private String user;
        private String password;

        /** Ruta a un fichero {@code .creds}. Es la forma recomendada: se relee en cada (re)conexion, y rotar no exige reiniciar. */
        public Credentials credsFile(String v) {
            this.credsFile = v;
            return this;
        }

        /** El contenido de un {@code .creds}, por ejemplo leido de un gestor de secretos. */
        public Credentials credsContent(String v) {
            this.credsContent = v;
            return this;
        }

        /** Semilla de una nkey de USUARIO ({@code SU…}). */
        public Credentials nkeySeed(String v) {
            this.nkeySeed = v;
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

        String credsFile() {
            return credsFile;
        }

        String credsContent() {
            return credsContent;
        }

        String nkeySeed() {
            return nkeySeed;
        }

        String token() {
            return token;
        }

        String user() {
            return user;
        }

        String password() {
            return password;
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

    /**
     * Aislamiento entre tenants — 09-multitenancy.md §3.
     *
     * <p>flux v1 implementa el <b>Modelo A</b>: un stream por dominio con todos los tenants
     * mezclados, y el SDK filtra antes del handler. Eso protege contra el consumidor que
     * olvida filtrar, no contra un adversario: todo servicio con acceso al dominio SIGUE
     * pudiendo leer los datos de todos los tenants. El aislamiento duro exige una account
     * de NATS por tenant (Modelo B), y eso es topologia, no SDK.
     */
    public enum TenantIsolation {
        /** Default. El filtrado es opcional y se decide por suscripcion. */
        OFF,
        /**
         * Toda suscripcion filtra, y suscribirse sin tenant configurado es un ERROR de
         * configuracion.
         *
         * <p>Es el punto que importa de §3. Un filtro que hay que acordarse de poner es un
         * filtro que alguien olvidara, y el fallo —ver los datos de otro tenant— <b>no
         * produce ningun error</b>: produce un incidente de privacidad que se descubre
         * semanas despues.
         */
        STRICT
    }

    /** Configuracion de la conexion al bus. */
    public static final class ConnectOptions {
        String servers = "nats://localhost:4222";
        String service;
        String environment;
        String version;
        String tenantId;
        TenantIsolation tenantIsolation = TenantIsolation.OFF;
        Validation.Options validation;
        long pendingPollMillis = DEFAULT_PENDING_POLL_MILLIS;
        Signing.SigningOptions signing;
        MetricsSink metrics = MetricsSink.NONE;
        FluxEvent.DataClassification classification;
        final Map<String, String> schemas = new HashMap<>();
        String schemaBaseUrl;
        Classifier.ClassifierOptions classifierOptions;
        int streamReplicas = 1;
        Credentials credentials;
        TlsOptions tls;
        EventContext.TraceparentSupplier traceparentSupplier;
        PoisonListener onPoison;
        DlqListener onDlq;
        System.Logger logger;

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

        /**
         * Aislamiento entre tenants. Default {@link TenantIsolation#OFF}
         * — 09-multitenancy.md §3.
         */
        public ConnectOptions tenantIsolation(TenantIsolation v) {
            this.tenantIsolation = v != null ? v : TenantIsolation.OFF;
            return this;
        }

        /**
         * Validacion L3 contra el JSON Schema del evento — 00-protocol.md §5.
         *
         * <p>Con {@link Validation.Mode#STRICT}, publicar un payload que viola su contrato
         * falla en el productor en vez de aparecer como un misterio en un consumidor de otro
         * equipo la semana que viene. Default: sin validacion, es decir L2.
         *
         * <p>El bundle se pasa como DATO: el SDK NO resuelve el {@code dataschema} por HTTP.
         * Ver {@link SchemaBundle}.
         */
        public ConnectOptions validation(Validation.Options v) {
            this.validation = v;
            return this;
        }

        /**
         * Cada cuanto sondear el {@code num_pending} de cada consumidor, en milisegundos.
         * {@code 0} lo desactiva. Default {@value #DEFAULT_PENDING_POLL_MILLIS}.
         *
         * <p>No es un capricho de configuracion: {@code flux_consumer_pending} tiene DOS
         * fuentes y hacen falta las dos (08-observability.md §2.3). Los metadatos del
         * mensaje entregado son gratis y frescos, pero <b>si el bucle del consumidor muere
         * dejan de llegar mensajes</b> y el gauge se queda PLANO en su ultimo valor en vez
         * de crecer — una linea horizontal indistinguible de "no pasa nada". El sondeo sigue
         * corriendo y reporta el pending creciente. Es la senal.
         */
        public ConnectOptions pendingPollMillis(long v) {
            this.pendingPollMillis = Math.max(v, 0);
            return this;
        }

        /** El periodo configurado. Package-private: es para los tests, no API publica. */
        long pendingPollMillis() {
            return pendingPollMillis;
        }

        /**
         * Firma Ed25519 de eventos. Extension OPCIONAL — 07-signing.md.
         *
         * <p>Traslada la autenticidad del canal al evento: hoy la garantiza la ACL del
         * broker, que deja de aplicar en cuanto el evento sale del stream. Un evento
         * firmado sigue siendo verificable dentro de un fichero, un backup o un correo.
         */
        public ConnectOptions signing(Signing.SigningOptions v) {
            this.signing = v;
            return this;
        }

        /**
         * Destino de las metricas. Default {@link MetricsSink#NONE}.
         *
         * <p>Los nombres y las etiquetas los fija el protocolo (08-observability.md), no la
         * aplicacion: si cada SDK nombrara a su manera, un panel del ecosistema seria
         * imposible.
         */
        public ConnectOptions metrics(MetricsSink v) {
            this.metrics = v != null ? v : MetricsSink.NONE;
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

        /**
         * TLS — 06-security.md §2: es OBLIGATORIO en produccion. {@code new TlsOptions()} exige TLS con las CA
         * del sistema; {@link TlsOptions#caFile(String)} fija una CA propia. El SDK siempre verifica el host
         * (los SAN del certificado). Sin esta opcion no hay TLS, y un broker que lo exige se rechaza con
         * {@code CONNECTION_REJECTED}.
         */
        public ConnectOptions tls(TlsOptions v) {
            this.tls = v;
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
        private String externalKey;

        /**
         * La clave del hecho en el SISTEMA EXTERNO que lo origino (el id del evento de una pasarela de pagos).
         * El {@code id} del evento se DERIVA de ella y de {@link #time(Instant)} en vez de generarse: es siempre
         * el mismo, y por eso un consumidor puede deduplicar los reenvios de un tercero durante horas
         * — 01-envelope.md §2.6.1. Exige {@code time}: el instante del hecho EXTERNO, no «ahora».
         */
        public PublishOptions externalKey(String v) {
            this.externalKey = v;
            return this;
        }

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
        /** {@code null} si el sondeo esta desactivado. Ver {@link #startPendingPoll}. */
        private final ScheduledFuture<?> pendingPoll;
        private volatile boolean stopped;

        private Subscription(String subject, String durable, MessageConsumer consumer,
                             ScheduledFuture<?> pendingPoll) {
            this.subject = subject;
            this.durable = durable;
            this.consumer = consumer;
            this.pendingPoll = pendingPoll;
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
            if (pendingPoll != null) {
                pendingPoll.cancel(false);
            }
            consumer.stop();
            subscriptions.remove(this);
        }

        @Override
        public void close() {
            unsubscribe();
        }
    }

    // ─── Estado ──────────────────────────────────────────────────────────────

    /** Periodo por defecto del sondeo de {@code num_pending} — 08-observability.md §2.3. */
    public static final long DEFAULT_PENDING_POLL_MILLIS = 15_000L;

    final Connection connection;
    final JetStream jetStream;
    final JetStreamManagement jsm;
    final ConnectOptions options;
    /** Lo que jnats notifica: denegaciones de permisos, errores del broker, durables borrados. */
    final FluxListener listener;
    final Classifier classifier;
    final MetricsSink metrics;
    final Signing.Signer signer;
    final Signing.Verifier verifier;
    /** {@code null} salvo en L3. Ver {@link Validation#create}. */
    final Validation.Validator validator;
    final boolean validateOnConsume;
    final String source;
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
    final ScheduledExecutorService wipScheduler = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "flux-wip");
        t.setDaemon(true);
        return t;
    });

    /**
     * Hilo del sondeo de {@code num_pending}, o {@code null} si esta desactivado.
     *
     * <p>Es un hilo APARTE del de los WIP y no una tarea mas en {@link #wipScheduler} por la
     * misma razon por la que aquel tiene dos: una peticion al servidor puede tardar, y un
     * sondeo lento no debe retrasar la renovacion del plazo de ack de ninguna suscripcion
     * — eso seria provocar la reentrega concurrente que el WIP existe para evitar.
     *
     * <p>Si el sondeo esta desactivado no se crea el hilo: quien no lo usa no lo paga.
     */
    final ScheduledExecutorService pendingScheduler;

    private FluxBus(Connection connection, JetStream jetStream, JetStreamManagement jsm,
                    ConnectOptions options, FluxListener listener) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.jsm = jsm;
        this.options = options;
        this.listener = listener;
        this.classifier = new Classifier(options.classifierOptions);
        this.metrics = options.metrics;
        // Las claves se decodifican UNA vez, en connect(): un PEM mal escrito debe romper
        // el arranque del servicio y no la primera publicacion a las 3 de la manana.
        this.signer = Signing.createSigner(options.signing);
        this.verifier = Signing.createVerifier(options.signing);
        // Los esquemas se compilan UNA vez, por el mismo motivo y ademas por throughput:
        // compilar un JSON Schema por evento esta en la ruta caliente. En modo OFF esto
        // devuelve null y no se carga ni la libreria de validacion — 00-protocol.md §5.
        Validation.Options validationOptions = options.validation;
        this.validator = Validation.create(validationOptions != null
                ? validationOptions.logger(validationOptions.logger() != null
                        ? validationOptions.logger() : options.logger)
                : null);
        this.validateOnConsume = validationOptions != null && validationOptions.onConsume();
        this.source = Protocol.sourceUri(options.environment, options.service);
        // Sin sink de metricas el sondeo no tendria donde escribir, asi que ni se crea el
        // hilo: es la misma condicion que aplica el SDK de Node.
        this.pendingScheduler =
                pendingPollEnabled(options.pendingPollMillis, this.metrics)
                        ? Executors.newSingleThreadScheduledExecutor(r -> {
                            Thread t = new Thread(r, "flux-pending-poll");
                            t.setDaemon(true);
                            return t;
                        })
                        : null;
        this.metrics.connectionState(MetricsSink.ConnectionState.CONNECTED);
    }

    // ─── connect ─────────────────────────────────────────────────────────────

    /**
     * Conecta al bus.
     *
     * <p>Reconecta indefinidamente con backoff y JITTER: sin jitter, mil servicios reconectan en el mismo
     * milisegundo y tiran el cluster que acaba de levantarse — 03-delivery.md §6.
     *
     * <p>La PRIMERA conexion es otro caso (03-delivery.md §6.1): espera a un broker que aun no ha arrancado,
     * pero FALLA de inmediato ante lo que esperar no arregla —credenciales rechazadas, un certificado que no
     * verifica, un TLS exigido que el servidor no ofrece— con uno de los tres codigos de 04-errors.md §5.3
     * ({@code CONNECTION_REJECTED}, {@code INVALID_CREDENTIALS}, {@code INVALID_TLS_OPTIONS}), todos
     * PERMANENT.
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

        // Todo lo que se puede comprobar SIN RED se comprueba aqui, ANTES de abrir ningun socket: un valor mal
        // formado debe fallar con su nombre, y no degradarse a «sin credenciales» o a «sin TLS».
        String inboxPrefix = Security.inboxPrefixFor(options.service);
        Security.Auth auth = Security.credentials(options.credentials);
        SSLContext sslContext = Security.sslContext(options.tls);

        FluxListener listener = new FluxListener(options.service, options.logger);
        Options.Builder builder = new Options.Builder()
                .servers(serverList(options.servers))
                .connectionName(options.service + "@" + options.environment)
                .maxReconnects(-1)
                .reconnectWait(RECONNECT_WAIT)
                .reconnectJitter(RECONNECT_JITTER)
                .reconnectJitterTls(RECONNECT_JITTER_TLS)
                // Inbox PRIVADO, siempre y no configurable: ver Security.inboxPrefixFor().
                .inboxPrefix(inboxPrefix)
                // Una peticion sin respuesta se cancela a los 5 s por defecto —con la MISMA excepcion que un
                // `no responders`—: con un timeout de 25 s la confundiriamos con RPC_UNAVAILABLE (A-203).
                .requestCleanupInterval(Duration.ofSeconds(60))
                .errorListener(listener)
                .connectionListener(listener);
        if (auth != null) {
            auth.apply(builder);
        }
        if (sslContext != null) {
            builder.sslContext(sslContext);
        }

        Connection connection = connectWhenAvailable(builder, listener, options.servers, sslContext);
        try {
            return new FluxBus(connection, connection.jetStream(), connection.jetStreamManagement(), options,
                    listener);
        } catch (IOException e) {
            try {
                connection.close();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            throw new FluxBusException("flux: no se pudo abrir JetStream en \"" + options.servers + "\"", e);
        }
    }

    private static final Duration RECONNECT_WAIT = Duration.ofSeconds(1);
    private static final Duration RECONNECT_JITTER = Duration.ofMillis(500);
    private static final Duration RECONNECT_JITTER_TLS = Duration.ofSeconds(1);

    private static String[] serverList(String servers) {
        List<String> out = new ArrayList<>();
        for (String s : servers.split(",")) {
            if (!s.trim().isEmpty()) {
                out.add(s.trim());
            }
        }
        return out.toArray(new String[0]);
    }

    /**
     * Espera a que el broker este disponible —un servicio que arranca antes que NATS es lo normal en un
     * compose— pero NO espera lo que no va a cambiar.
     *
     * <p>{@code Nats.connectReconnectOnConnect} reintenta cualquier error, sin limite y sin avisar. Medido contra
     * NATS 2.14.5 con jnats 2.20.5 (A-200): con una CA que no firmo el certificado, un servidor sin TLS ante un
     * {@code tls} pedido o un TLS exigido que no se configuro, {@code connect()} no falla: se CUELGA; y con una
     * credencial rechazada devuelve una conexion CERRADA, sin excepcion, que el servicio usaria sin saberlo.
     * Aqui el bucle es del SDK, con {@code Nats.connect} (un solo intento) y la clasificacion de
     * {@link Security#permanentConnectError}. Las reconexiones posteriores siguen siendo de la libreria
     * ({@code maxReconnects(-1)}).
     */
    private static Connection connectWhenAvailable(Options.Builder builder, FluxListener listener, String servers,
                                                   SSLContext tls) {
        Options built = builder.build();
        while (true) {
            FluxListener.Attempt attempt = listener.beginAttempt();
            try {
                return Nats.connect(built);
            } catch (InterruptedException e) {
                // Restaurar el flag es obligatorio: tragarselo deja el hilo marcado como no
                // interrumpido y rompe cualquier apagado ordenado aguas arriba.
                Thread.currentThread().interrupt();
                throw new FluxBusException("flux: conexion interrumpida", e);
            } catch (IOException e) {
                try {
                    // La causa llega por el oyente, y la libreria la notifica ANTES del cierre del intento.
                    attempt.awaitClosed(1500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new FluxBusException("flux: conexion interrumpida", ie);
                }
                SecurityErrors.ConnectionRejectedException permanent =
                        Security.permanentConnectError(e, attempt.exceptions(), servers, tls != null);
                if (permanent == null && tls != null && attempt.exceptions().stream()
                        .anyMatch(t -> t instanceof TimeoutException)) {
                    // Solo un timeout, con TLS configurado: puede ser un broker mudo (se espera) o uno que exige un
                    // certificado de cliente que no se presento (no se espera). La libreria no los distingue.
                    javax.net.ssl.SSLException rejection = Security.probeTlsRejection(tls, serverList(servers)[0]);
                    if (rejection != null) {
                        permanent = Security.permanentConnectError(e, List.of(rejection), servers, true);
                    }
                }
                if (permanent != null) {
                    throw permanent;
                }
                listener.log(System.Logger.Level.DEBUG, "el broker aun no esta disponible (" + e.getMessage()
                        + "): se reintenta");
                try {
                    // Con jitter: mil servicios que esperan a un NATS que arranca no deben llamar a la vez
                    // — 03-delivery.md §6.
                    long jitter = (long) (Math.random() * RECONNECT_JITTER.toMillis());
                    Thread.sleep(RECONNECT_WAIT.toMillis() + jitter);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new FluxBusException("flux: conexion interrumpida", ie);
                }
            }
        }
    }

    /** Cuantas suscripciones de eventos tiene abiertas. Package-private: es para los tests. */
    int subscriptionCount() {
        return subscriptions.size();
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
        if (pendingScheduler != null) {
            pendingScheduler.shutdownNow();
        }
        metrics.connectionState(MetricsSink.ConnectionState.DISCONNECTED);
        try {
            // drain y no close: los acks pendientes no se pierden en silencio.
            connection.drain(Duration.ofSeconds(10)).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log(System.Logger.Level.WARNING, "fallo al drenar la conexion: " + e.getMessage());
        } finally {
            listener.denials.close();
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
        // historiales desde la DLQ (01-envelope.md §2.4). De un hecho IMPORTADO el id se DERIVA de la
        // clave externa y del instante del hecho: es siempre el mismo (01-envelope.md §2.6.1).
        String id = po.externalKey != null
                ? ExternalId.eventId(source, po.externalKey, po.time)
                : Protocol.uuidV7();

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

        // L3: validar ANTES de publicar. Un payload que viola su contrato debe fallar aqui,
        // en el servicio que lo genero, y no aparecer como un misterio en un consumidor de
        // otro equipo la semana que viene — 00-protocol.md §5.
        if (validator != null) {
            try {
                validator.check(event, subject);
            } catch (RuntimeException e) {
                // `invalid_schema` y no `error`: "el broker rechazo la publicacion" y "el
                // productor publico basura" son dos incidentes distintos con dos respuestas
                // distintas, y la etiqueta existe justo para separarlos
                // — 08-observability.md §2.1.
                metrics.eventPublished(subject, MetricsSink.PublishOutcome.INVALID_SCHEMA);
                throw e;
            }
        }

        // Firmar es lo ULTIMO antes de serializar: la firma cubre el envelope COMPLETO, asi
        // que cualquier atributo anadido despues la invalidaria — 07-signing.md §5.
        if (signer != null) {
            event = signer.sign(event);
        }
        byte[] payload = Envelope.serialize(event);

        try {
            // messageId pone la cabecera Nats-Msg-Id. Deduplica reintentos de PUBLICACION
            // dentro de duplicate_window: un publish que no recibe el ACK del broker por un
            // corte de red y se reintenta no deja dos copias en el stream.
            //
            // NO deduplica reentregas de consumo. Un nak reentrega el mismo mensaje con el
            // mismo Nats-Msg-Id y eso es correcto y deseado. Nunca sustituye a la
            // idempotencia del consumidor — 03-delivery.md §3.
            //
            // Asincrona y competida con la denegacion: una publicacion denegada NO falla en jnats, espera su
            // timeout (A-201). Un publish a un dominio ajeno lo deniega el broker, no este codigo, y es
            // PERMANENT: como un 403, no debe reintentarse.
            final byte[] body = payload;
            final String messageId = event.id();
            listener.denials.race(options.service, subject, null, () -> jetStream.publishAsync(subject, body,
                    io.nats.client.PublishOptions.builder().messageId(messageId).build()), PUBLISH_ACK_TIMEOUT_MILLIS);
        } catch (PermissionDeniedException denied) {
            metrics.eventPublished(subject, MetricsSink.PublishOutcome.ERROR);
            throw denied;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.eventPublished(subject, MetricsSink.PublishOutcome.ERROR);
            throw new FluxBusException("flux: publicacion interrumpida en \"" + subject + "\"", e);
        } catch (Exception e) {
            metrics.eventPublished(subject, MetricsSink.PublishOutcome.ERROR);
            throw new FluxBusException("flux: fallo al publicar en \"" + subject + "\"", unwrap(e));
        }
        metrics.eventPublished(subject, MetricsSink.PublishOutcome.OK);
        return event;
    }

    /** Cuanto se espera el acuse de una publicacion antes de darla por fallida. */
    private static final long PUBLISH_ACK_TIMEOUT_MILLIS = 5_000;

    private static Throwable unwrap(Throwable e) {
        return e instanceof java.util.concurrent.ExecutionException && e.getCause() != null ? e.getCause() : e;
    }

    /** Resuelve la URI de {@code dataschema} del subject. */
    private String schemaFor(String subject, Protocol.ParsedSubject parsed) {
        String explicit = options.schemas.get(subject);
        if (explicit != null && !explicit.isEmpty()) {
            return explicit;
        }
        // El bundle L3 conoce el MINOR real de cada subject: dentro de un mayor todo es
        // BACKWARD-compatible, asi que el mas alto acepta lo que aceptan los anteriores.
        // Sin el, mas abajo solo se puede asumir el .0.0 — 00-protocol.md §5.
        if (options.validation != null && options.validation.bundle() != null) {
            String fromBundle = options.validation.bundle().schemaUriFor(subject);
            if (fromBundle != null && !fromBundle.isEmpty()) {
                return fromBundle;
            }
        }
        if (options.schemaBaseUrl == null || options.schemaBaseUrl.isEmpty()) {
            throw new FluxBusException("flux: no hay dataschema para \"" + subject
                    + "\". Declara schema(\"" + subject + "\", …), schemaBaseUrl, o pasa un bundle "
                    + "en validation(...) (ConnectOptions)");
        }
        String base = options.schemaBaseUrl.endsWith("/")
                ? options.schemaBaseUrl.substring(0, options.schemaBaseUrl.length() - 1)
                : options.schemaBaseUrl;
        // Sin bundle ni mapa explicito solo se puede asumir el .0.0 del mayor. Es suficiente
        // para L2 —el atributo es informativo— pero no para L3 — 00-protocol.md §5.
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

        // El filtro efectivo se resuelve ANTES de tocar el broker: si el aislamiento es
        // estricto y falta el tenant, la suscripcion no debe llegar ni a crear el durable.
        String tenantFilter = requiredTenantFilter(
                subject, options.tenantIsolation, so.tenantId, options.tenantId);

        ensureStream(parsed.domain());
        // La DLQ se comprueba AL ARRANCAR, no cuando llega el primer mensaje envenenado: descubrirlo entonces es
        // el peor momento. Sin el permiso de consulta, la comprobacion falla con PERMISSION_DENIED.
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
            // INFO antes de crear (06-security.md §3.4): en produccion lo aprovisiona `flux provision` y la ACL del
            // servicio NO concede CONSUMER.CREATE.
            ConsumerInfo found = ensureConsumer(stream, durable, subject, requested);

            // El servidor no siempre devuelve lo que se le pide. Requisito L2.
            assertConfigHonored(durable, requested, found.getConsumerConfiguration());

            // Un pull denegado no da error: el consumidor quedaria inerte y parece sano (A-20). Se comprueba
            // ANTES de devolver, no por la ausencia de eventos semanas despues (06-security.md §3.7).
            String pull = "$JS.API.CONSUMER.MSG.NEXT." + stream + "." + durable;
            listener.denials.forget(pull);
            ensurePullAllowed(stream, durable);

            ConsumerContext consumerContext = api(List.of("$JS.API.CONSUMER.INFO." + stream + "." + durable), null,
                    () -> jetStream.getConsumerContext(stream, durable));
            MessageConsumer consumer = consumerContext.consume(msg -> {
                try {
                    dispatch(msg, subject, durable, handler, tenantFilter);
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

            Subscription subscription =
                    new Subscription(subject, durable, consumer, startPendingPoll(stream, subject, durable));
            subscriptions.add(subscription);
            return subscription;
        } catch (PermissionDeniedException | NotProvisionedException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FluxBusException("flux: suscripcion interrumpida en \"" + subject + "\"", e);
        } catch (IOException | JetStreamApiException e) {
            throw new FluxBusException(
                    "flux: no se pudo crear el consumidor \"" + durable + "\" en \"" + stream + "\"", e);
        }
    }

    /**
     * Toma el consumidor durable si existe y, solo si no existe, lo crea.
     *
     * <p>Se pregunta ANTES de crear: en produccion lo aprovisiona {@code flux provision}, y entonces el SDK no
     * necesita permiso de creacion. Crear primero provocaba una denegacion espuria en cada arranque (una
     * {@code Publish Violation} en el log del servidor) y obligaba a conceder {@code CONSUMER.CREATE} a quien no
     * lo usa nunca. Que el consumidor ya exista con otra config no es un error de aqui: lo decide
     * {@link #assertConfigHonored}.
     *
     * <ul>
     *   <li>{@code INFO} denegado → {@code PERMISSION_DENIED}: ni siquiera se sabe si falta.
     *   <li>{@code INFO} dice «no existe» y {@code CREATE} esta denegado → {@code NOT_PROVISIONED}: falta y no
     *       puedo crearlo. Esa PAREJA, y no la denegacion sola, es lo que significa (04-errors.md §5).
     * </ul>
     */
    ConsumerInfo ensureConsumer(String stream, String durable, String filter, ConsumerConfiguration requested)
            throws IOException, JetStreamApiException {
        String info = "$JS.API.CONSUMER.INFO." + stream + "." + durable;
        try {
            return api(List.of(info), null, () -> jsm.getConsumerInfo(stream, durable));
        } catch (JetStreamApiException e) {
            if (e.getApiErrorCode() == JS_STREAM_NOT_FOUND) {
                throw new NotProvisionedException(ResourceKind.STREAM, stream, null, options.service, info,
                        "el stream \"" + stream + "\" no existe (el broker contesto `stream not found` al consultar el "
                                + "consumidor \"" + durable + "\"). En produccion se aprovisiona con `flux provision "
                                + "<services.json>`, usando credenciales de operacion, no las del servicio "
                                + "(06-security.md §3.6).", e);
            }
            if (e.getApiErrorCode() != JS_CONSUMER_NOT_FOUND) {
                throw e;
            }
        }
        try {
            return api(List.of("$JS.API.CONSUMER.CREATE." + stream + "." + durable + "." + filter,
                    "$JS.API.CONSUMER.DURABLE.CREATE." + stream + "." + durable), null,
                    () -> jsm.addOrUpdateConsumer(stream, requested));
        } catch (PermissionDeniedException denied) {
            throw new NotProvisionedException(ResourceKind.CONSUMER, durable, stream, options.service,
                    denied.subject(), null, denied);
        } catch (JetStreamApiException addError) {
            // Otra instancia del servicio pudo crearlo entre la pregunta y la creacion.
            try {
                return api(List.of(info), null, () -> jsm.getConsumerInfo(stream, durable));
            } catch (JetStreamApiException | IOException again) {
                throw addError;
            }
        }
    }

    /**
     * Comprueba que el servicio PUEDE pedir mensajes de su consumidor, o falla con
     * {@code PERMISSION_DENIED} nombrando el subject denegado.
     *
     * <p>Un pull denegado no da error: el servidor descarta el mensaje y manda un {@code -ERR} aparte
     * (A-20). Con jnats, el primer pull lo emite la propia libreria al consumir y su denegacion llega por el
     * oyente DESPUES; esperar «un poco» y mirar seria una carrera. Lo determinista es no depender del pull de
     * la libreria: se pregunta lo mismo sobre EL MISMO subject con una peticion propia y se compite su
     * respuesta con la denegacion. El cuerpo es un JSON truncado, {@code {}, a proposito: el servidor lo
     * contesta {@code 400 Bad Request} al instante, sin apuntar la peticion como pendiente y sin sacar ningun
     * mensaje del consumidor —cualquier pull valido podria llevarse uno, que quedaria sin ack hasta
     * {@code ack_wait}—. Medido (A-204): {@code 400 Bad Request - unexpected end of JSON input} en ~7 ms.
     *
     * <p>Si la sonda no obtiene respuesta ni denegacion no se falla: se avisa por el logger y se sigue. No poder
     * comprobarlo no es lo mismo que estar denegado.
     */
    void ensurePullAllowed(String stream, String durable) throws InterruptedException {
        String subject = "$JS.API.CONSUMER.MSG.NEXT." + stream + "." + durable;
        String hint = "Es pedir mensajes del consumidor \"" + durable + "\" del stream \"" + stream + "\": sin ese "
                + "permiso el consumidor se quedaria inerte, sin recibir nada y sin ningun error, y por eso "
                + "subscribe() falla en vez de arrancar (06-security.md §3.7).";
        try {
            io.nats.client.Message reply = listener.denials.race(options.service, subject, hint,
                    () -> connection.request(subject, "{".getBytes()), PULL_PROBE_TIMEOUT_MILLIS);
            if (reply.getStatus() == null || reply.getStatus().getCode() != 400) {
                log(System.Logger.Level.WARNING, "no se pudo comprobar el permiso de pull de \"" + durable
                        + "\" en \"" + stream + "\": la sonda no obtuvo el 400 esperado ("
                        + reply.getStatus() + "). Si la ACL no concede `" + subject + "`, el consumidor se quedara "
                        + "inerte y solo lo dira este logger");
            }
        } catch (PermissionDeniedException denied) {
            throw denied;
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            log(System.Logger.Level.WARNING, "no se pudo comprobar el permiso de pull de \"" + durable + "\" en \""
                    + stream + "\": " + unwrap(e) + ". Si la ACL no concede `" + subject + "`, el consumidor se "
                    + "quedara inerte y solo lo dira este logger");
        }
    }

    private static final long PULL_PROBE_TIMEOUT_MILLIS = 5_000;

    @FunctionalInterface
    interface Api<T> {
        T call() throws IOException, JetStreamApiException;
    }

    /**
     * Una peticion a la API de JetStream, vigilada: si el broker deniega alguno de {@code subjects} falla en
     * milisegundos con {@code PERMISSION_DENIED} en vez de esperar el timeout de la libreria (A-201).
     */
    <T> T api(List<String> subjects, String hint, Api<T> call) throws IOException, JetStreamApiException {
        try {
            return listener.denials.guard(options.service, subjects, hint, call::call);
        } catch (IOException | JetStreamApiException | RuntimeException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FluxBusException("flux: peticion a JetStream interrumpida", e);
        } catch (TimeoutException e) {
            throw new IOException("flux: la peticion a JetStream no termino", e);
        } catch (Exception e) {
            throw new FluxBusException("flux: fallo inesperado en una peticion a JetStream", e);
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
        assertConfigHonored(durable, requested, effective, List.of());
    }

    /** Como la anterior, mas las diferencias propias de quien llama (el estado: politica de entrega y filtro). */
    static void assertConfigHonored(String durable, ConsumerConfiguration requested,
                                    ConsumerConfiguration effective,
                                    List<ConsumerConfigMismatchException.Difference> extra) {
        List<ConsumerConfigMismatchException.Difference> diffs = new ArrayList<>(extra);

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

    /**
     * Arranca el sondeo periodico de {@code num_pending} para una suscripcion.
     *
     * <p>{@code flux_consumer_pending} tiene DOS fuentes y un SDK DEBE usar las dos, porque
     * cada una falla justo donde la otra sirve (08-observability.md §2.3):
     *
     * <ul>
     *   <li>Los <b>metadatos</b> del mensaje entregado son gratis y frescos en cada evento,
     *       pero fallan cuando <b>no llegan mensajes</b> — que es exactamente el caso que
     *       importa. Si el bucle del consumidor muere, el gauge se queda plano en su ultimo
     *       valor: un panel mostraria una linea horizontal, indistinguible de "no pasa
     *       nada".</li>
     *   <li>El <b>sondeo</b> cuesta una peticion cada ~15 s y sigue corriendo aunque no se
     *       entregue nada. Es el que reporta el pending creciente. Es la senal.</li>
     * </ul>
     *
     * @return la tarea, o {@code null} si el sondeo esta desactivado.
     */
    private ScheduledFuture<?> startPendingPoll(String stream, String subject, String durable) {
        if (pendingScheduler == null) {
            return null;
        }
        long period = options.pendingPollMillis;
        Runnable task = pendingPollTask(subject, durable, metrics,
                () -> jsm.getConsumerInfo(stream, durable).getCalculatedPending(),
                message -> log(System.Logger.Level.DEBUG, message));
        return pendingScheduler.scheduleAtFixedRate(task, period, period, TimeUnit.MILLISECONDS);
    }

    /**
     * Si hay que crear el hilo del sondeo.
     *
     * <p>{@code 0} lo desactiva —lo permite explicitamente 08-observability.md §2.3— y sin
     * sink de metricas no habria donde escribir el gauge, asi que el hilo tampoco se crea:
     * quien no usa la metrica no paga un hilo por ella. Es la misma condicion que aplica el
     * SDK de Node.
     *
     * <p>Package-private para poder testear la decision sin broker.
     */
    static boolean pendingPollEnabled(long pollMillis, MetricsSink metrics) {
        return pollMillis > 0 && metrics != MetricsSink.NONE;
    }

    /** De donde sale el {@code num_pending}. Existe para poder testear el sondeo sin broker. */
    @FunctionalInterface
    interface PendingSource {
        long pending() throws Exception;
    }

    /**
     * El cuerpo de UNA iteracion del sondeo.
     *
     * <p>Package-private para poder testear sin broker lo unico que puede salir mal aqui:
     * que un fallo del sondeo se lleve por delante algo mas que la propia telemetria.
     */
    static Runnable pendingPollTask(String subject, String durable, MetricsSink metrics,
                                    PendingSource source, java.util.function.Consumer<String> log) {
        return () -> {
            try {
                metrics.consumerPending(subject, durable, source.pending());
            } catch (Exception | Error e) {
                // Un fallo del sondeo NO DEBE afectar al consumo: esto es telemetria
                // (08-observability.md §2.3). Se traga TODO —incluido un Error— porque una
                // excepcion que escape de una tarea de scheduleAtFixedRate CANCELA la tarea
                // en silencio: un corte de red de dos segundos apagaria el sondeo para
                // siempre y la metrica dejaria de emitirse sin que nada lo dijera. Ese
                // fallo se manifiesta como "el panel lleva horas sin datos", que es
                // exactamente lo que esta metrica existe para que no ocurra.
                log.accept("no se pudo sondear num_pending de " + durable + ": " + e);
            }
        };
    }

    // ─── despacho ────────────────────────────────────────────────────────────

    /**
     * El tenant por el que hay que filtrar, o {@code null} si no hay ninguno.
     *
     * <p>El de la suscripcion gana sobre el de la conexion.
     *
     * <p>{@code "system"} NO cuenta: se reserva para eventos de plataforma sin tenant y
     * <b>no debe usarse como comodin ni como valor por defecto</b> cuando el tenant real se
     * desconoce. Un consumidor configurado con {@code tenantId="system"} no esta aislado de
     * nadie, asi que devolver aqui {@code "system"} haria que el modo estricto diese por
     * bueno justo el caso que existe para cazar — 09-multitenancy.md §5.
     *
     * <p>Package-private para poder testear la politica sin broker, igual que
     * {@link #assertConfigHonored} y {@link #effectiveBudget}.
     */
    static String tenantFilter(String subscriptionTenantId, String connectionTenantId) {
        String candidate = firstNonEmpty(subscriptionTenantId, connectionTenantId);
        return candidate == null || "system".equals(candidate) ? null : candidate;
    }

    /**
     * Igual que {@link #tenantFilter}, pero en {@link TenantIsolation#STRICT} exige que
     * haya filtro.
     *
     * @throws TenantIsolationException si el aislamiento es estricto y no hay tenant.
     */
    static String requiredTenantFilter(String subject, TenantIsolation isolation,
                                       String subscriptionTenantId, String connectionTenantId) {
        String filter = tenantFilter(subscriptionTenantId, connectionTenantId);
        if (isolation == TenantIsolation.STRICT && filter == null) {
            throw new TenantIsolationException(subject, subscriptionTenantId, connectionTenantId);
        }
        return filter;
    }

    private void dispatch(io.nats.client.Message msg, String subject, String durable,
                          Handler handler, String tenantFilter) {
        // deliveredCount empieza en 1 en la primera entrega, no en 0.
        int attempt = 1;
        try {
            io.nats.client.impl.NatsJetStreamMetaData meta = msg.metaData();
            long delivered = meta.deliveredCount();
            if (delivered > 0) {
                attempt = (int) delivered;
            }
            // `pending` viene en los metadatos del propio mensaje: gratis y mas fresco que
            // el sondeo, porque se actualiza en CADA entrega. Es una de las dos fuentes
            // obligatorias de la metrica.
            //
            // Pero NO sustituye al sondeo de startPendingPoll(): si el bucle del consumidor
            // muere dejan de llegar mensajes, y una metrica alimentada solo desde aqui se
            // queda PLANA en su ultimo valor en vez de crecer — que es exactamente lo
            // contrario de la senal que hace falta (08-observability.md §2.3).
            metrics.consumerPending(subject, durable, meta.pendingCount());
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

        // Filtrar ANTES del handler: un evento de otro tenant no es un fallo y no es para
        // nosotros. Se confirma y se descarta — 09-multitenancy.md §3, punto 2. Tampoco se
        // contabiliza como consumido: no lo hemos consumido, lo hemos ignorado.
        if (tenantFilter != null && !tenantFilter.equals(event.tenantid())) {
            msg.ack();
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
        long inicio = System.nanoTime();

        try {
            // La firma se comprueba ANTES que nada: si el evento fue manipulado, su payload
            // puede validar perfectamente y aun asi no ser del productor que dice ser. En
            // modo `require` esto lanza una PoisonException y cae por la rama de error de
            // abajo, que la clasifica como POISON y la enruta a la DLQ con alerta
            // — 07-signing.md §7.
            Throwable handlerError = verifierError(event);
            // L3 al consumir (opt-in): el evento es sintacticamente valido —ha llegado a
            // parsearse, asi que no es POISON— pero incumple su contrato. Reintentarlo dara
            // exactamente el mismo resultado, asi que la clasificacion correcta es PERMANENT
            // y la da la propia SchemaValidationException — 04-errors.md §1.2.
            //
            // Va DESPUES de la firma: si el evento fue manipulado, su payload puede validar
            // perfectamente y aun asi no ser del productor que dice ser.
            if (handlerError == null && validateOnConsume) {
                handlerError = validationError(event, subject);
            }
            if (handlerError == null) {
                handlerError = invoke(handler, event, delivery);
            }
            if (handlerError == null) {
                msg.ack();
                metrics.eventConsumed(subject, durable, MetricsSink.ConsumeOutcome.OK);
                metrics.handlerDuration(subject, durable, elapsedSeconds(inicio));
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
                metrics.eventRetried(subject, durable, attempt);
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

            metrics.eventConsumed(subject, durable, consumeOutcome(reason, classification.code()));
            metrics.eventDlq(subject, durable, reason, classification.code());
            metrics.handlerDuration(subject, durable, elapsedSeconds(inicio));
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

    /**
     * Comprueba la firma, o {@code null} si no hay verificador o el evento pasa.
     *
     * <p>Devuelve el error en vez de lanzarlo para que recorra exactamente la misma ruta
     * que un fallo del handler: clasificacion, DLQ, metricas y {@code term}. Un camino
     * aparte seria un segundo sitio donde arreglar el enrutado a la DLQ.
     */
    private Throwable verifierError(FluxEvent event) {
        if (verifier == null) {
            return null;
        }
        try {
            verifier.check(event);
            return null;
        } catch (RuntimeException e) {
            return e;
        }
    }

    /**
     * Valida el payload contra su esquema, o {@code null} si no hay validador o pasa.
     *
     * <p>Devuelve el error en vez de lanzarlo por la misma razon que
     * {@link #verifierError}: asi recorre exactamente la misma ruta que un fallo del
     * handler —clasificacion, DLQ, metricas y {@code term}— en vez de abrir un segundo
     * sitio donde arreglar el enrutado a la DLQ.
     */
    private Throwable validationError(FluxEvent event, String subject) {
        if (validator == null) {
            return null;
        }
        try {
            validator.check(event, subject);
            return null;
        } catch (RuntimeException e) {
            return e;
        }
    }

    private static double elapsedSeconds(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000_000.0;
    }

    /**
     * La etiqueta {@code outcome} de un evento que muere.
     *
     * <p><b>La firma invalida se separa del POISON comun</b> aunque su {@code reason} siga
     * siendo {@code poison}: son dos incidentes distintos —basura frente a suplantacion— con
     * dos respuestas distintas, y la etiqueta existe para eso (08-observability.md §2.1). El
     * {@code reason} de la DLQ no cambia porque ese si es el enum cerrado de 04-errors.md
     * §1.
     *
     * <p>Package-private para poder testear la traduccion sin broker.
     */
    static MetricsSink.ConsumeOutcome consumeOutcome(FluxEvent.DlqReason reason, String code) {
        if (isSignatureCode(code)) {
            return MetricsSink.ConsumeOutcome.INVALID_SIGNATURE;
        }
        // Mismo argumento que con la firma: el `reason` de la DLQ sigue siendo `permanent`
        // —es el enum cerrado de 04-errors.md §1— pero "un productor incumple su esquema" y
        // "mi logica rechaza este evento" son dos preguntas distintas, y la etiqueta
        // `invalid_schema` existe para separarlas — 08-observability.md §2.1.
        if (isSchemaCode(code)) {
            return MetricsSink.ConsumeOutcome.INVALID_SCHEMA;
        }
        return switch (reason) {
            case RETRYABLE -> MetricsSink.ConsumeOutcome.RETRYABLE;
            case PERMANENT -> MetricsSink.ConsumeOutcome.PERMANENT;
            case POISON -> MetricsSink.ConsumeOutcome.POISON;
        };
    }

    /** Los dos codigos de la validacion L3 — 00-protocol.md §5. */
    private static boolean isSchemaCode(String code) {
        return SchemaValidationException.CODE.equals(code)
                || SchemaNotFoundException.CODE.equals(code);
    }

    /** Los tres codigos POISON de la extension de firma — 07-signing.md §7. */
    private static boolean isSignatureCode(String code) {
        return Signing.CODE_MISSING_SIGNATURE.equals(code)
                || Signing.CODE_INVALID_SIGNATURE.equals(code)
                || Signing.CODE_UNKNOWN_SIGNING_KEY.equals(code);
    }

    private void handlePoison(String subject, String durable, io.nats.client.Message msg,
                              RuntimeException error) {
        if (options.onPoison != null) {
            options.onPoison.onPoison(new PoisonInfo(subject, error, msg.getData()));
        }
        // El `code` es el estable de la clasificacion (MALFORMED_JSON, WRONG_ATTRIBUTE_TYPE…),
        // nunca el mensaje: un mensaje lleva ids y timestamps y su cardinalidad infinita
        // tumba el almacenamiento de metricas — 08-observability.md §2.2.
        String code = error instanceof FluxErrors.PoisonException poison ? poison.code() : "POISON";
        metrics.eventConsumed(subject, durable, consumeOutcome(FluxEvent.DlqReason.POISON, code));
        metrics.eventDlq(subject, durable, FluxEvent.DlqReason.POISON, code);
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
                .allowReservedDomain()
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
            api(List.of("$JS.API.STREAM.INFO." + name), null, () -> jsm.getStreamInfo(name));
        } catch (PermissionDeniedException e) {
            // Ni siquiera se puede CONSULTAR: no se sabe si falta, asi que no es NOT_PROVISIONED (04-errors.md §5).
            throw e;
        } catch (JetStreamApiException e) {
            // Se distingue "no existe" de cualquier otro fallo de la API por el codigo y no
            // por "hubo excepcion": tratarlo como "no existe" produciria un intento de creacion que falla con un
            // mensaje que no explica lo que paso de verdad.
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
            api(List.of("$JS.API.STREAM.CREATE." + name), null, () -> jsm.addStream(config));
        } catch (PermissionDeniedException denied) {
            // «Falta y no puedo crearlo»: el stream no existe (STREAM.INFO dijo 10059) y el broker deniega crearlo.
            // En produccion se aprovisiona FUERA del SDK, y sin este error lo que se veria es un timeout.
            throw new NotProvisionedException(ResourceKind.STREAM, name, null, options.service, denied.subject(), null,
                    denied);
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

    private static final int JS_CONSUMER_NOT_FOUND = 10014;

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

    void log(System.Logger.Level level, String message) {
        if (options.logger != null) {
            options.logger.log(level, "[flux] " + message);
        }
    }
}
