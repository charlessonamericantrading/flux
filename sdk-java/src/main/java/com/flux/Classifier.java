/*
 * Clasificacion de errores del handler.
 * Contrato normativo: specification/04-errors.md §2
 *
 * Este fichero es el punto donde el protocolo se encuentra con la realidad operativa del
 * ecosistema. Todo lo demas en el SDK es mecanica; esto es politica — y por eso la
 * politica es un parametro, no una constante.
 */
package com.flux;

import com.flux.FluxErrors.Classification;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Traduce un error cualquiera a una de las tres clases del protocolo.
 *
 * <p>El runtime del consumidor usa el resultado asi:
 *
 * <pre>
 *   RETRYABLE  → msg.nakWithDelay(retryAfter ?: backoff canonico)
 *   PERMANENT  → msg.term() + publicar en dlq.&lt;subject&gt; con dlqattempts = intento
 *   POISON     → msg.term() + publicar en dlq.&lt;subject&gt; + alerta inmediata
 * </pre>
 */
public final class Classifier {

    // ─── Politicas ───────────────────────────────────────────────────────────

    /**
     * Que hacer con un error que no encaja en ninguna regla conocida — 04-errors.md §2.1.
     */
    public enum UnknownErrorPolicy {
        /**
         * A la DLQ sin gastar reintentos. Falla rapido, pero un hipo de red manda a la DLQ
         * un evento perfectamente valido y alguien lo reproduce a mano cada manana.
         */
        PERMANENT,

        /**
         * Backoff completo: 51 minutos. Solo si vuestras dependencias tienen hipos muy
         * frecuentes y podeis asumir que un modo de fallo nuevo atasque la cola — y se
         * amplifique con cada mensaje que falle igual.
         */
        RETRYABLE,

        /**
         * DEFAULT. Reintenta con presupuesto reducido ({@link #DEFAULT_UNKNOWN_RETRY_BUDGET}
         * entregas) en vez de las 6 completas.
         *
         * <p>Domina a las otras dos: un transitorio desconocido se recupera en el segundo
         * intento y un sistematico desconocido llega a la DLQ en ~30 s sin atascar la cola.
         * Cuesta 30 segundos de latencia sobre los errores genuinamente permanentes y a
         * cambio elimina los dos modos de fallo. No es un punto medio, es estrictamente
         * mejor — 04-errors.md §2.1.
         */
        RETRYABLE_BOUNDED
    }

    /** Un timeout, ¿es "el mundo va lento" o "esta operacion no cabe en la ventana"? */
    public enum TimeoutPolicy {
        /** Evita reintentar lo imposible, si vuestros timeouts son consultas que nunca terminan. */
        PERMANENT,
        /** DEFAULT: un timeout suele indicar saturacion transitoria. */
        RETRYABLE
    }

    /**
     * Entregas maximas para un error desconocido bajo {@link UnknownErrorPolicy#RETRYABLE_BOUNDED}.
     * Incluye la primera entrega, asi que 2 = un reintento — protocol.json
     * {@code errors.unknownRetryBudget}.
     */
    public static final int DEFAULT_UNKNOWN_RETRY_BUDGET = 2;

    /**
     * Status HTTP que merecen reintento — 04-errors.md §1.1.
     *
     * <p>Notese que NO esta aqui: 400, 403, 404 y 422 son PERMANENT. Reintentarlos es
     * gastar 51 minutos para obtener exactamente la misma respuesta.
     */
    public static final Set<Integer> RETRYABLE_HTTP_STATUS = Set.of(429, 502, 503, 504);

    // ─── Contratos que la aplicacion puede implementar ───────────────────────

    /**
     * Error que sabe decir con que status HTTP fallo una dependencia. El status es la
     * senal mas fiable que da una dependencia sobre si merece la pena reintentar.
     *
     * <p>⚠️ Divergencia con Node, y es de fondo: alli el clasificador hurga en
     * {@code err.status}, {@code err.statusCode} y {@code err.response.status} porque en
     * JavaScript cualquier objeto puede tener cualquier propiedad. En Java eso exigiria
     * reflexion sobre nombres de campo, que es adivinar; el contrato se hace explicito.
     * Implementa esta interfaz —o usa {@link HttpException}— y el clasificador la
     * encuentra recorriendo la cadena de causas.
     */
    public interface HttpStatusAware {
        int httpStatus();
    }

    /** Error que sabe cuanto pide esperar la dependencia (cabecera {@code Retry-After}). */
    public interface RetryAfterAware {
        Duration retryAfter();
    }

    /**
     * Implementacion de conveniencia de {@link HttpStatusAware}, para no obligar a cada
     * aplicacion a escribir la suya.
     *
     * <pre>{@code
     * if (response.statusCode() >= 400) {
     *     throw new Classifier.HttpException(response.statusCode(), "POST /v1/charges", retryAfter);
     * }
     * }</pre>
     */
    public static class HttpException extends RuntimeException implements HttpStatusAware, RetryAfterAware {
        private static final long serialVersionUID = 1L;

        private final int status;
        private final transient Duration retryAfter;

        public HttpException(int status, String message) {
            this(status, message, null, null);
        }

        public HttpException(int status, String message, Duration retryAfter) {
            this(status, message, retryAfter, null);
        }

        public HttpException(int status, String message, Duration retryAfter, Throwable cause) {
            super(message == null ? "HTTP " + status : "HTTP " + status + ": " + message, cause);
            this.status = status;
            this.retryAfter = retryAfter;
        }

        @Override
        public int httpStatus() {
            return status;
        }

        /** {@code null} si la dependencia no anuncio {@code Retry-After}. */
        @Override
        public Duration retryAfter() {
            return retryAfter;
        }
    }

    /**
     * Regla de clasificacion de la aplicacion. Devuelve {@link Optional#empty()} para
     * ceder al resto de la cadena. Se evaluan antes que todo lo demas salvo los errores
     * que ya declaran su clase.
     */
    @FunctionalInterface
    public interface Rule extends Function<Throwable, Optional<Classification>> {
    }

    // ─── Errores transitorios, por SEMANTICA ─────────────────────────────────

    /**
     * Tipos de excepcion que significan "la red no esta disponible o se interrumpio", y
     * "la resolucion de nombres fallo de forma temporal" — 04-errors.md §1.1.
     *
     * <p>⚠️ La lista de {@code protocol.json} ({@code ECONNRESET}, {@code EAI_AGAIN}…) son
     * codigos de libuv y NO son normativos: la clasificacion se define por semantica. En
     * Java el mecanismo idiomatico es el TIPO de la excepcion, que el propio JDK ya elige
     * segun el errno. Nunca se hace matching de substrings sobre el mensaje: el mensaje de
     * {@code SocketException} lo escribe el sistema operativo y difiere entre Windows y
     * Linux, que es justo el bug que la spec documenta (un port literal de la lista de
     * Node clasificaba PERMANENT en Windows y RETRYABLE en Linux el mismo corte de red).
     *
     * <p>El orden importa: {@link ConnectException}, {@link NoRouteToHostException} y
     * {@link PortUnreachableException} extienden {@link SocketException}, asi que la
     * superclase va la ultima para que el {@code code} de la metrica sea el tipo mas
     * especifico.
     *
     * <p>Cobertura por categoria de la spec:
     * <ul>
     *   <li>Conexion rechazada / dependencia arrancando → {@link ConnectException}</li>
     *   <li>Ruta inalcanzable → {@link NoRouteToHostException},
     *       {@link PortUnreachableException}</li>
     *   <li>Conexion reseteada, tuberia rota, red caida → {@link SocketException}</li>
     *   <li>Resolucion de nombres → {@link UnknownHostException}</li>
     * </ul>
     */
    private static final List<Class<? extends Throwable>> TRANSIENT_NETWORK = List.of(
            ConnectException.class,
            NoRouteToHostException.class,
            PortUnreachableException.class,
            UnknownHostException.class,
            SocketException.class);

    /**
     * Tipos que significan "se agoto el plazo".
     *
     * <p>{@link SocketTimeoutException} extiende {@link InterruptedIOException} y NO
     * {@link SocketException}, asi que no lo captura la lista de red: la separacion entre
     * "la red fallo" y "la operacion no cabio en la ventana" —que la spec trata con
     * politicas distintas— la da el propio arbol de tipos del JDK.
     */
    private static final List<Class<? extends Throwable>> TIMEOUTS = List.of(
            SocketTimeoutException.class,
            HttpTimeoutException.class,
            TimeoutException.class,
            InterruptedIOException.class);

    // ─── Opciones ────────────────────────────────────────────────────────────

    /**
     * Politica de clasificacion del consumidor.
     *
     * <p>Builder mutable por la misma razon que {@link Envelope.BuildEventInput}: todos
     * los campos son opcionales y tienen un default de la spec.
     */
    public static final class ClassifierOptions {
        private UnknownErrorPolicy unknownErrorPolicy = UnknownErrorPolicy.RETRYABLE_BOUNDED;
        private int unknownRetryBudget = DEFAULT_UNKNOWN_RETRY_BUDGET;
        private TimeoutPolicy timeoutPolicy = TimeoutPolicy.RETRYABLE;
        private final List<Rule> rules = new ArrayList<>();

        public ClassifierOptions unknownErrorPolicy(UnknownErrorPolicy policy) {
            this.unknownErrorPolicy = policy;
            return this;
        }

        /** Entregas maximas de un error desconocido bajo RETRYABLE_BOUNDED. Minimo 1. */
        public ClassifierOptions unknownRetryBudget(int budget) {
            if (budget < 1) {
                throw new IllegalArgumentException("el presupuesto minimo es 1 entrega (la inicial)");
            }
            this.unknownRetryBudget = budget;
            return this;
        }

        public ClassifierOptions timeoutPolicy(TimeoutPolicy policy) {
            this.timeoutPolicy = policy;
            return this;
        }

        /** Regla propia, evaluada antes que todo lo demas salvo los errores tipados. */
        public ClassifierOptions addRule(Rule rule) {
            this.rules.add(rule);
            return this;
        }
    }

    // ─── Clasificador ────────────────────────────────────────────────────────

    private final UnknownErrorPolicy unknownErrorPolicy;
    private final int unknownRetryBudget;
    private final TimeoutPolicy timeoutPolicy;
    private final List<Rule> rules;

    /** Clasificador con los defaults de la spec. */
    public Classifier() {
        this(new ClassifierOptions());
    }

    public Classifier(ClassifierOptions options) {
        ClassifierOptions o = options != null ? options : new ClassifierOptions();
        this.unknownErrorPolicy = o.unknownErrorPolicy;
        this.unknownRetryBudget = o.unknownRetryBudget;
        this.timeoutPolicy = o.timeoutPolicy;
        // Copia defensiva: la politica no debe poder cambiar bajo los pies del consumidor
        // una vez creado. Es lo mismo que hace el clasificador de Go con su slice de reglas.
        this.rules = List.copyOf(o.rules);
    }

    /** Clasificador con los defaults de la spec: desconocido → RETRYABLE acotado (2). */
    public static final Classifier DEFAULT = new Classifier();

    /**
     * Clasifica un error.
     *
     * <p>El orden de evaluacion es deliberado: lo mas especifico primero y el default al
     * final. Esa ultima linea es la decision de politica de verdad.
     */
    public Classification classify(Throwable error) {
        if (error == null) {
            // No deberia ocurrir —el runtime solo clasifica errores— pero devolver una
            // clase falsa seria peor que devolver algo inerte.
            return new Classification(ErrorClass.PERMANENT, "NIL_ERROR");
        }

        // 1. Un error tipado de flux siempre gana: la aplicacion sabe mas que el SDK.
        Optional<FluxErrors.FluxException> typed = FluxErrors.asClassified(error);
        if (typed.isPresent()) {
            FluxErrors.FluxException fe = typed.get();
            Duration retryAfter = fe instanceof FluxErrors.RetryableException re
                    ? re.retryAfter().orElse(null)
                    : null;
            return new Classification(fe.errorClass(), fe.code(), retryAfter, null);
        }

        // 2. Reglas de la aplicacion.
        for (Rule rule : rules) {
            Optional<Classification> result = rule.apply(error);
            if (result.isPresent()) {
                return result.get();
            }
        }

        // 3. Status HTTP: la senal mas fiable que da una dependencia.
        Optional<HttpStatusAware> http = FluxErrors.findCause(error, HttpStatusAware.class);
        if (http.isPresent()) {
            int status = http.get().httpStatus();
            boolean retryable = RETRYABLE_HTTP_STATUS.contains(status);
            if (!retryable) {
                return new Classification(ErrorClass.PERMANENT, "HTTP_" + status);
            }
            Duration retryAfter = FluxErrors.findCause(error, RetryAfterAware.class)
                    .map(RetryAfterAware::retryAfter)
                    .orElse(null);
            return new Classification(ErrorClass.RETRYABLE, "HTTP_" + status, retryAfter, null);
        }

        // 4. Errores de sistema: red y DNS son transitorios por definicion.
        //
        // UnknownHostException entra aqui, y con una imprecision que conviene conocer: el
        // JDK usa el mismo tipo para "el nombre no existe" (NXDOMAIN, permanente) y para
        // "el resolutor no pudo contestar ahora" (SERVFAIL, transitorio). La spec pide
        // reintentar solo el segundo. Java no expone la diferencia —no hay equivalente al
        // *net.DNSError.IsTemporary de Go—, asi que se reintenta en ambos casos: el coste
        // de reintentar un host inexistente esta acotado por max_deliver, y el de NO
        // reintentar un SERVFAIL es tirar un evento bueno a la DLQ. Ver README §"Fricciones".
        for (Class<? extends Throwable> type : TRANSIENT_NETWORK) {
            Optional<? extends Throwable> found = FluxErrors.findCause(error, type);
            if (found.isPresent()) {
                return new Classification(ErrorClass.RETRYABLE, found.get().getClass().getSimpleName());
            }
        }

        // 5. Timeouts — politica configurable.
        for (Class<? extends Throwable> type : TIMEOUTS) {
            if (FluxErrors.findCause(error, type).isPresent()) {
                ErrorClass timeoutClass = timeoutPolicy == TimeoutPolicy.PERMANENT
                        ? ErrorClass.PERMANENT
                        : ErrorClass.RETRYABLE;
                return new Classification(timeoutClass, "TIMEOUT");
            }
        }

        // 6. Lo desconocido. Aqui se decide el comportamiento del ecosistema ante lo que
        //    nadie previo. El default acotado da al transitorio una segunda oportunidad sin
        //    regalarle 51 minutos de cola al sistematico — 04-errors.md §2.1.
        return switch (unknownErrorPolicy) {
            case PERMANENT -> new Classification(ErrorClass.PERMANENT, "UNKNOWN");
            case RETRYABLE -> new Classification(ErrorClass.RETRYABLE, "UNKNOWN");
            case RETRYABLE_BOUNDED -> Classification.retryableBounded("UNKNOWN", unknownRetryBudget);
        };
    }
}
