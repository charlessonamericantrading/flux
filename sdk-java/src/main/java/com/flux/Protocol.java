/*
 * Constantes verificadas y derivaciones de naming del flux Event Protocol v1.
 * Contrato normativo: specification/02-naming.md, specification/03-delivery.md
 *
 * Todo lo de aqui sale de protocol.json. Si divergen, protocol.json manda: es lo que
 * consumen los demas SDKs y los agentes de IA.
 */
package com.flux;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Constantes del protocolo y transformaciones de nombres.
 *
 * <p>Clase de utilidad: no se instancia. Todas las funciones son puras salvo
 * {@link #uuidV7()}, que consulta el reloj.
 */
public final class Protocol {

    private Protocol() {
        throw new AssertionError("clase de utilidad");
    }

    // ─── Constantes del protocolo ────────────────────────────────────────────

    /** Identifican el contrato, no este SDK. */
    public static final String PROTOCOL_NAME = "flux";

    public static final String PROTOCOL_VERSION = "1.0.0";

    /** Literal exigido por CloudEvents — 01-envelope.md §2. */
    public static final String SPEC_VERSION = "1.0";

    /** flux v1 solo admite JSON — 01-envelope.md §2. */
    public static final String DATA_CONTENT_TYPE = "application/json";

    /** Nivel de conformidad declarado por este SDK — 00-protocol.md §5. */
    public static final String CONFORMANCE_LEVEL = "L2";

    /**
     * Techo del mensaje serializado. Por encima, claim-check: se publica
     * {@code {uri, sha256, bytes}}, no el contenido — 01-envelope.md §6.
     */
    public static final int MAX_MESSAGE_BYTES = 1_048_576;

    // ─── Configuracion canonica de consumidor — 03-delivery.md §2 ────────────

    /**
     * DEBE coincidir con {@code canonicalBackoff().get(0)}.
     *
     * <p>⚠️ JetStream SOBRESCRIBE {@code ack_wait} con {@code backoff[0]} y no devuelve
     * error: pides 30 s con un backoff que empieza en 1 s y obtienes un {@code ack_wait}
     * efectivo de 1 s. Cualquier handler que toque una base de datos se ejecuta entonces
     * en concurrencia consigo mismo, en cada mensaje, sin ninguna senal visible.
     * Verificado contra nats-server 2.14.5 — ver 03-delivery.md §2.1 y
     * {@code conformance/cases/consumer-config.json}.
     *
     * <p>Consecuencia de diseno: {@code backoff[0]} ES el presupuesto de duracion del
     * handler. Por eso el backoff canonico empieza en 30 s y no en 1 s; un primer
     * reintento rapido es imposible por construccion, y buscarlo es lo que rompe la
     * configuracion.
     */
    public static final Duration DEFAULT_ACK_WAIT = Duration.ofSeconds(30);

    /**
     * 1 entrega inicial + 5 reintentos, uno por entrada de {@link #canonicalBackoff()}.
     * Si fuese 5, la ultima entrada (30 m) no se aplicaria nunca y la configuracion
     * mentiria sobre su propio comportamiento.
     */
    public static final int DEFAULT_MAX_DELIVER = 6;

    /**
     * Ojo: un mensaje esperando reintento ocupa una ranura, asi que con backoffs largos
     * y mucho fallo simultaneo esta ventana se llena — 03-delivery.md §2.1, nota final.
     */
    public static final int DEFAULT_MAX_ACK_PENDING = 256;

    /**
     * Backoff canonico {@code [30s, 1m, 5m, 15m, 30m]}.
     *
     * <p>Diferencia de port con Go: alli {@code CanonicalBackOff()} construye un slice
     * nuevo en cada llamada porque un slice a nivel de paquete seria mutable desde fuera
     * y una entrada [0] alterada cambiaria en silencio el {@code ack_wait} efectivo de
     * todo consumidor creado despues. {@link List#of} ya es inmutable, asi que aqui se
     * comparte la misma instancia sin riesgo.
     */
    private static final List<Duration> CANONICAL_BACKOFF = List.of(
            Duration.ofSeconds(30),
            Duration.ofMinutes(1),
            Duration.ofMinutes(5),
            Duration.ofMinutes(15),
            Duration.ofMinutes(30));

    static {
        // La invariante mas cara del protocolo no puede expresarse en el sistema de
        // tipos de ningun lenguaje (ver README §"Fricciones", G). Aqui se comprueba al
        // cargar la clase: si alguien toca el backoff y olvida DEFAULT_ACK_WAIT, el
        // proceso no arranca — en vez de fallar en produccion a las 3 de la manana con
        // reentregas concurrentes y ningun error.
        if (!DEFAULT_ACK_WAIT.equals(CANONICAL_BACKOFF.get(0))) {
            throw new ExceptionInInitializerError(
                    "invariante rota: ack_wait (" + DEFAULT_ACK_WAIT + ") debe ser igual a backoff[0] ("
                            + CANONICAL_BACKOFF.get(0) + ") — JetStream sobrescribe ack_wait con backoff[0] "
                            + "sin avisar (03-delivery.md §2.1)");
        }
        if (CANONICAL_BACKOFF.size() != DEFAULT_MAX_DELIVER - 1) {
            throw new ExceptionInInitializerError(
                    "invariante rota: max_deliver (" + DEFAULT_MAX_DELIVER + ") = 1 entrega inicial + "
                            + CANONICAL_BACKOFF.size() + " reintentos; con otro numero la ultima entrada de "
                            + "backoff nunca se aplicaria (03-delivery.md §2)");
        }
    }

    /**
     * Backoff canonico. Tiempo total hasta la DLQ ≈ 51 min 30 s.
     *
     * <p>Es una decision de producto: cuanto tiempo aceptas que un fallo transitorio siga
     * reintentando antes de que un humano se entere. Solo lo recorren los RETRYABLE; un
     * PERMANENT no gasta ni un reintento.
     */
    public static List<Duration> canonicalBackoff() {
        return CANONICAL_BACKOFF;
    }

    /** Suma del backoff canonico: lo que tarda un RETRYABLE en caer en la DLQ. */
    public static Duration totalTimeToDlq() {
        Duration total = Duration.ZERO;
        for (Duration d : CANONICAL_BACKOFF) {
            total = total.plus(d);
        }
        return total;
    }

    // ─── Configuracion canonica de stream — 02-naming.md §3.2 ───────────────

    public static final Duration STREAM_MAX_AGE = Duration.ofDays(30);

    /**
     * Mas larga a proposito: la DLQ es material forense. Pero es un limite real, no un
     * archivo — a los 90 dias el evento desaparece.
     */
    public static final Duration DLQ_STREAM_MAX_AGE = Duration.ofDays(90);

    /**
     * Deduplica PUBLICACIONES con el mismo {@code Nats-Msg-Id}. NO deduplica reentregas
     * de consumo, y nunca sustituye a la idempotencia del consumidor: es el malentendido
     * mas frecuente del protocolo — 03-delivery.md §3.
     */
    public static final Duration DUPLICATE_WINDOW = Duration.ofMinutes(2);

    // ─── Naming ──────────────────────────────────────────────────────────────

    /** Valida {@code <dominio>.<agregado>.v<major>.<evento>} — 02-naming.md §1. */
    public static final Pattern SUBJECT_PATTERN = Pattern.compile(
            "^[a-z0-9]+(-[a-z0-9]+)*\\.[a-z0-9]+(-[a-z0-9]+)*\\.v[1-9][0-9]*\\.[a-z0-9]+(-[a-z0-9]+)*$");

    /**
     * Nombre de servicio valido: kebab-case en minusculas.
     *
     * <p>NATS aceptaria sin rechistar un durable como {@code FacturacionAPI__pedidos_…},
     * asi que el incumplimiento del patron solo se descubriria al intentar parsear
     * nombres de consumidor en una herramienta — protocol.json {@code naming.service}.
     */
    public static final Pattern SERVICE_PATTERN = Pattern.compile("^[a-z0-9]+(-[a-z0-9]+)*$");

    /** Los cuatro tokens de un subject ya validado. */
    public record ParsedSubject(String domain, String aggregate, int major, String event) {

        /** Reconstruye el subject original. La transformacion es biyectiva. */
        public String toSubject() {
            return domain + "." + aggregate + ".v" + major + "." + event;
        }
    }

    /**
     * Subject que no cumple 02-naming.md §1.
     *
     * <p>Es {@link IllegalArgumentException} y no una excepcion comprobada a proposito:
     * un subject mal escrito es un bug del codigo que llama, no una condicion de
     * ejecucion que la aplicacion deba manejar. Se detecta en el primer test.
     */
    public static final class InvalidSubjectException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;
        private final transient String subject;

        public InvalidSubjectException(String subject, String reason) {
            super("subject invalido \"" + subject + "\": " + reason);
            this.subject = subject;
        }

        public String subject() {
            return subject;
        }
    }

    /** Nombre de servicio que no cumple el patron del protocolo. */
    public static final class InvalidServiceNameException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        public InvalidServiceNameException(String service) {
            super("nombre de servicio invalido \"" + service + "\": debe ser kebab-case en minusculas "
                    + "([a-z0-9-]). NATS aceptaria el durable resultante, pero incumpliria el patron del "
                    + "protocolo y romperia el filtrado por servicio en `nats consumer ls` (02-naming.md §4).");
        }
    }

    /**
     * Valida y descompone un subject de NATS.
     *
     * <p>⚠️ No confundir con el atributo {@code subject} de CloudEvents, que es el ID DEL
     * AGREGADO ({@code "ped-123"}). En este SDK ese atributo se llama {@code aggregateId}
     * y solo se mapea a {@code subject} al serializar — 01-envelope.md §2.1.
     */
    public static ParsedSubject parseSubject(String subject) {
        if (subject == null) {
            throw new InvalidSubjectException("null", "el subject es obligatorio");
        }
        // La comprobacion de minusculas va ANTES que el patron para poder dar un mensaje
        // util: los subjects de NATS son case-sensitive, asi que "Pedidos.pedido.v1.creado"
        // crea un subject fantasma al que nadie esta suscrito y no produce ningun error.
        // Sin este mensaje, el desarrollador solo ve "no llegan mis eventos".
        //
        // Locale.ROOT y no el locale por defecto: con la JVM en turco, "pedidos".toUpperCase()
        // da "PEDİDOS" y "I".toLowerCase() da "ı". Es una trampa especifica de Java y
        // produciria exactamente el subject fantasma que esta comprobacion existe para evitar.
        if (!subject.equals(subject.toLowerCase(Locale.ROOT))) {
            throw new InvalidSubjectException(subject,
                    "debe ir todo en minusculas — NATS es case-sensitive y una mayuscula crea un subject "
                            + "al que nadie esta suscrito, sin producir error");
        }

        if (!SUBJECT_PATTERN.matcher(subject).matches()) {
            int n = subject.split("\\.", -1).length;
            String reason = n != 4
                    ? "debe tener exactamente 4 tokens (<dominio>.<agregado>.v<major>.<evento>), tiene " + n
                    : "formato esperado <dominio>.<agregado>.v<major>.<evento> en kebab-case";
            throw new InvalidSubjectException(subject, reason);
        }

        String[] tokens = subject.split("\\.", -1);
        // El patron ya garantizo `v` + entero >= 1, asi que el parseInt no puede fallar.
        int major = Integer.parseInt(tokens[2].substring(1));
        return new ParsedSubject(tokens[0], tokens[1], major, tokens[3]);
    }

    /** Informa si el subject cumple el contrato, sin exponer el motivo. */
    public static boolean isValidSubject(String subject) {
        try {
            parseSubject(subject);
            return true;
        } catch (InvalidSubjectException e) {
            return false;
        }
    }

    /**
     * Deriva el {@code type} de CloudEvents:
     * {@code pedidos.pedido.v1.creado} → {@code com.flux.pedidos.pedido.creado.v1}
     * — 02-naming.md §2.
     *
     * <p>Los dos formatos existen porque sirven a consumidores distintos: el subject
     * enruta y necesita la version en posicion fija para que los wildcards funcionen; el
     * {@code type} identifica el contrato en un catalogo y ahi lee mejor con la version
     * al final. La transformacion es mecanica, asi que jamas se le pide al desarrollador.
     */
    public static String subjectToType(String subject) {
        ParsedSubject p = parseSubject(subject);
        return "com.flux." + p.domain() + "." + p.aggregate() + "." + p.event() + ".v" + p.major();
    }

    /**
     * Inversa de {@link #subjectToType}. Util en un handler suscrito con wildcard que
     * necesita saber que evento concreto llego.
     */
    public static ParsedSubject parseType(String type) {
        if (type == null || !type.startsWith("com.flux.")) {
            throw new IllegalArgumentException("type \"" + type
                    + "\" no sigue el formato com.flux.<dominio>.<agregado>.<evento>.v<major>");
        }
        String[] t = type.substring("com.flux.".length()).split("\\.", -1);
        if (t.length != 4) {
            throw new IllegalArgumentException(
                    "type \"" + type + "\" no tiene los 4 tokens esperados tras com.flux.");
        }
        return parseSubject(t[0] + "." + t[1] + "." + t[3] + "." + t[2]);
    }

    /**
     * {@code EVT_PEDIDOS} para el dominio {@code pedidos}.
     *
     * <p>NATS no admite {@code .}, {@code *}, {@code >}, {@code /}, {@code \} ni espacios
     * en nombres de stream: de ahi el guion bajo. Las mayusculas son convencion, para
     * distinguir de un vistazo un stream de un subject en los logs — 02-naming.md §3.
     */
    public static String streamName(String domain) {
        return "EVT_" + domain.replace('-', '_').toUpperCase(Locale.ROOT);
    }

    /** {@code DLQ_PEDIDOS} — 02-naming.md §3. */
    public static String dlqStreamName(String domain) {
        return "DLQ_" + domain.replace('-', '_').toUpperCase(Locale.ROOT);
    }

    /**
     * {@code facturacion-api__pedidos_pedido_v1_creado}.
     *
     * <p>NATS tampoco admite {@code .} en nombres de durable consumer. Separar el
     * servicio con {@code __} y los tokens con {@code _} mantiene la reversibilidad:
     * partiendo por {@code __} recuperas servicio y subject exactos. Un nombre de
     * consumidor que no dice que servicio lo tiene abierto es inutil en
     * {@code nats consumer ls} a las 3 de la manana — 02-naming.md §4.
     *
     * <p>Se valida TAMBIEN el nombre de servicio, no solo el subject: NATS aceptaria
     * {@code FacturacionAPI__pedidos_…} sin error y el incumplimiento solo se descubriria
     * al parsear nombres en una herramienta.
     */
    public static String durableName(String service, String subject) {
        validateService(service);
        parseSubject(subject);
        return service + "__" + subject.replace('.', '_').replace('-', '_');
    }

    /** Valida el nombre de servicio contra {@link #SERVICE_PATTERN}. */
    public static void validateService(String service) {
        if (service == null || !SERVICE_PATTERN.matcher(service).matches()) {
            throw new InvalidServiceNameException(String.valueOf(service));
        }
    }

    /**
     * Antepone {@code dlq.} al subject original.
     *
     * <p>PREFIJO, nunca sufijo. Un sufijo ({@code pedidos.pedido.v1.creado.dlq})
     * encajaria con {@code pedidos.>} y el stream EVT_PEDIDOS capturaria sus propios
     * muertos: contarian contra su retencion, un consumidor de {@code pedidos.pedido.v1.>}
     * los recibiria, y un replay masivo podria reinyectarse en su propia DLQ
     * — 02-naming.md §3.1.
     */
    public static String dlqSubject(String subject) {
        return "dlq." + subject;
    }

    /** Informa si el subject pertenece al espacio de nombres de la DLQ. */
    public static boolean isDlqSubject(String subject) {
        return subject != null && subject.startsWith("dlq.");
    }

    /**
     * {@code /produccion/pedidos-api} — 01-envelope.md §2.
     *
     * <p>{@code id} + {@code source} son la clave de deduplicacion del ecosistema entero,
     * asi que el {@code source} tiene que identificar de forma estable entorno y servicio.
     */
    public static String sourceUri(String environment, String service) {
        return "/" + environment + "/" + service;
    }

    // ─── UUIDv7 ──────────────────────────────────────────────────────────────

    private static final SecureRandom RANDOM = new SecureRandom();
    private static long lastTimestampMillis = -1L;
    private static int sequence = 0;

    /**
     * Genera un UUIDv7 (RFC 9562) en formato canonico.
     *
     * <p>Hay que implementarlo: {@link UUID} no genera v7 ni en Java 21.
     * {@code UUID.randomUUID()} es v4 —aleatorio puro— y no sirve aqui, porque el
     * protocolo se apoya en que el {@code id} sea monotonico en el tiempo: ordenar por
     * {@code id} dentro de un mismo {@code source} equivale a ordenar por instante de
     * generacion, que es lo que permite reconstruir historiales desde la DLQ
     * — 01-envelope.md §2.4.
     *
     * <p>Disposicion de bits: 48 bits de milisegundos Unix, 4 de version (7), 12 de
     * {@code rand_a}, 2 de variante (10b) y 62 de {@code rand_b}.
     *
     * <p>Los 12 bits de {@code rand_a} se usan como CONTADOR dentro del mismo
     * milisegundo (metodo 2 de la RFC) en vez de como aleatorio: sin eso, dos eventos
     * publicados en el mismo milisegundo pueden salir desordenados y la propiedad de
     * arriba deja de cumplirse justo cuando mas se usa (una rafaga). Con 4096 valores por
     * milisegundo, desbordar exige > 4 M eventos/s en un solo proceso; si ocurre, se
     * espera al siguiente milisegundo en vez de reutilizar un contador.
     */
    public static synchronized String uuidV7() {
        long now = System.currentTimeMillis();
        if (now == lastTimestampMillis) {
            sequence++;
            if (sequence > 0xFFF) {
                // Contador agotado: esperar activamente al siguiente milisegundo es
                // preferible a emitir un id que rompa el orden.
                while (now == lastTimestampMillis) {
                    Thread.onSpinWait();
                    now = System.currentTimeMillis();
                }
                lastTimestampMillis = now;
                sequence = 0;
            }
        } else if (now > lastTimestampMillis) {
            lastTimestampMillis = now;
            sequence = 0;
        } else {
            // El reloj ha ido hacia atras (NTP). Se mantiene el ultimo timestamp y se
            // sigue contando: un id ligeramente adelantado es mucho menos danino que una
            // secuencia de ids que retrocede.
            sequence++;
            now = lastTimestampMillis;
            if (sequence > 0xFFF) {
                lastTimestampMillis++;
                now = lastTimestampMillis;
                sequence = 0;
            }
        }

        long msb = ((now & 0xFFFF_FFFF_FFFFL) << 16) | (0x7L << 12) | (sequence & 0xFFFL);
        long lsb = (RANDOM.nextLong() & 0x3FFF_FFFF_FFFF_FFFFL) | 0x8000_0000_0000_0000L;
        return new UUID(msb, lsb).toString();
    }
}
