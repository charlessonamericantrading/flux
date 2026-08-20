/*
 * Taxonomia de errores de flux: excepciones tipadas y resultado de la clasificacion.
 * Contrato normativo: specification/04-errors.md
 *
 * ⚠️ NOMBRES: la spec y los SDKs de Node/Go llaman a estos tipos RetryableError,
 * PermanentError y PoisonError. Aqui son ...Exception. No es un capricho de estilo: en
 * Java, `Error` es la rama de Throwable reservada a fallos de los que no se espera
 * recuperacion (OutOfMemoryError, StackOverflowError). Una clase llamada RetryableError
 * que extienda RuntimeException invitaria a `catch (Error e)` —que captura otra cosa— y
 * a que los linters marquen su captura como sospechosa. Es la unica divergencia de
 * nombres del port; ver README §"Fricciones", H.
 */
package com.flux;

import java.time.Duration;
import java.util.Optional;

/**
 * Contenedor de las excepciones tipadas de flux y de {@link Classification}.
 *
 * <p>Una aplicacion lanza {@link RetryableException} o {@link PermanentException} para
 * anular al clasificador, porque solo ella conoce sus dependencias — 04-errors.md §2.
 * {@link PoisonException} la produce el SDK, no la aplicacion.
 */
public final class FluxErrors {

    private FluxErrors() {
        throw new AssertionError("clase de utilidad");
    }

    /**
     * Profundidad maxima al recorrer la cadena de causas.
     *
     * <p>Existe porque en Java una cadena de causas puede ser ciclica
     * ({@code a.initCause(b); b.initCause(a)}) y recorrerla sin limite colgaria el
     * despacho. Go no necesita esto: {@code errors.As} recorre {@code Unwrap} y una
     * cadena ciclica alli es igual de posible pero mucho menos frecuente.
     */
    private static final int MAX_CAUSE_DEPTH = 32;

    // ─── Base ────────────────────────────────────────────────────────────────

    /**
     * Error que ya declara su propia clase de flux.
     *
     * <p>Es {@link RuntimeException} y no comprobada: obligar a declarar
     * {@code throws RetryableException} en cada firma de la aplicacion no aporta nada
     * —el SDK captura {@link Throwable} igualmente— y contaminaria toda la pila de
     * llamadas del handler.
     */
    public abstract static class FluxException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private final String code;

        protected FluxException(String message, String code, Throwable cause) {
            super(message, cause);
            this.code = code;
        }

        /** Clase declarada por este error. */
        public abstract ErrorClass errorClass();

        /**
         * Codigo estable para metricas y alertas ({@code "HTTP_503"},
         * {@code "PEDIDO_YA_CANCELADO"}). El mensaje del error cambia; el codigo no
         * deberia. Sin codigo explicito se cae al nombre simple de la clase, para que las
         * metricas nunca queden sin etiqueta.
         */
        public String code() {
            return code != null ? code : getClass().getSimpleName();
        }
    }

    // ─── RETRYABLE ───────────────────────────────────────────────────────────

    /**
     * La lanza la aplicacion para forzar un reintento. Usala cuando SABES que el fallo es
     * transitorio: timeout de red, 503 de un proveedor, deadlock de BD.
     *
     * <pre>{@code
     * throw new FluxErrors.RetryableException("proveedor 503", "PROVEEDOR_503", Duration.ofSeconds(5), causa);
     * }</pre>
     */
    public static final class RetryableException extends FluxException {
        private static final long serialVersionUID = 1L;

        private final transient Duration retryAfter;

        public RetryableException(String message) {
            this(message, null, null, null);
        }

        public RetryableException(String message, String code) {
            this(message, code, null, null);
        }

        public RetryableException(String message, String code, Throwable cause) {
            this(message, code, null, cause);
        }

        /**
         * @param retryAfter <b>sugerencia para el PRIMER reintento</b>, no un control del
         *                   calendario de reintentos. Usalo cuando la dependencia dice
         *                   explicitamente cuanto esperar (cabecera {@code Retry-After}).
         *                   {@code null} significa "usa el backoff canonico".
         *                   <p>⚠️ Con {@code backoff} configurado —y flux lo configura
         *                   SIEMPRE— JetStream honra el delay del {@code nak} solo en la
         *                   primera reentrega; a partir de la segunda manda el array
         *                   {@code backoff} y el delay solicitado <b>se ignora sin ningun
         *                   aviso</b>. Medido contra NATS 2.14.5 — 03-delivery.md §2.2.
         *                   Un {@code Retry-After: 5} acorta el primer reintento y nada
         *                   mas: los siguientes seguiran 1 m, 5 m, 15 m, 30 m.
         */
        public RetryableException(String message, String code, Duration retryAfter, Throwable cause) {
            super(message, code, cause);
            this.retryAfter = retryAfter;
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.RETRYABLE;
        }

        /** Retraso pedido para el siguiente intento, o vacio si no se pidio ninguno. */
        public Optional<Duration> retryAfter() {
            return Optional.ofNullable(retryAfter);
        }
    }

    // ─── PERMANENT ───────────────────────────────────────────────────────────

    /**
     * La lanza la aplicacion para ir directo a la DLQ sin gastar reintentos. Usala cuando
     * el evento es valido pero tu logica lo rechaza de forma definitiva: reintentarlo son
     * 51 minutos de cola bloqueada para llegar al mismo sitio.
     */
    public static final class PermanentException extends FluxException {
        private static final long serialVersionUID = 1L;

        public PermanentException(String message) {
            this(message, null, null);
        }

        public PermanentException(String message, String code) {
            this(message, code, null);
        }

        public PermanentException(String message, String code, Throwable cause) {
            super(message, code, cause);
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    // ─── POISON ──────────────────────────────────────────────────────────────

    /**
     * La produce el SDK, no la aplicacion: el mensaje no pudo interpretarse como
     * CloudEvent, asi que el handler nunca llega a verlo — 04-errors.md §1.3.
     */
    public static final class PoisonException extends FluxException {
        private static final long serialVersionUID = 1L;

        public PoisonException(String message) {
            this(message, null, null);
        }

        public PoisonException(String message, String code) {
            this(message, code, null);
        }

        public PoisonException(String message, String code, Throwable cause) {
            super(message, code, cause);
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.POISON;
        }
    }

    // ─── Resultado de la clasificacion ───────────────────────────────────────

    /**
     * Lo que el runtime del consumidor consume para decidir entre nak, term y alerta.
     *
     * <p>El componente se llama {@code errorClass} y no {@code class} porque
     * {@code class} es palabra reservada en Java. En Node es {@code class} y en Go
     * {@code Class}; es la unica diferencia de nombre del tipo.
     *
     * @param errorClass  determina la accion sobre el mensaje.
     * @param code        codigo estable para metricas y alertas.
     * @param retryAfter  solo para RETRYABLE: <b>sugerencia para el PRIMER reintento</b>.
     *                    {@code null} = usa el backoff canonico.
     *                    <p>⚠️ NO sobrescribe el calendario de reintentos, aunque lo
     *                    parezca. Con {@code backoff} configurado —y flux lo configura
     *                    siempre— JetStream aplica el delay del {@code nak} solo en la
     *                    primera reentrega y a partir de la segunda impone el array
     *                    {@code backoff}, sin devolver error ni avisar de nada. Verificado
     *                    contra NATS 2.14.5 — 03-delivery.md §2.2. No construyas logica que
     *                    dependa de que se respete mas alla de la primera vez.
     * @param maxAttempts solo para RETRYABLE: numero maximo de entregas para ESTE error,
     *                    por debajo del {@code max_deliver} del consumidor. Existe porque
     *                    {@code max_deliver} es por consumidor, no por mensaje: bajarlo a
     *                    2 para acotar los errores desconocidos recortaria tambien los
     *                    reintentos de los que si sabemos transitorios — 04-errors.md
     *                    §2.1. {@code null} = el presupuesto completo del consumidor.
     */
    public record Classification(
            ErrorClass errorClass,
            String code,
            Duration retryAfter,
            Integer maxAttempts) {

        public Classification(ErrorClass errorClass, String code) {
            this(errorClass, code, null, null);
        }

        /** RETRYABLE con presupuesto acotado — el default de lo desconocido. */
        public static Classification retryableBounded(String code, int maxAttempts) {
            return new Classification(ErrorClass.RETRYABLE, code, null, maxAttempts);
        }

        public Optional<Duration> retryAfterOpt() {
            return Optional.ofNullable(retryAfter);
        }

        public Optional<Integer> maxAttemptsOpt() {
            return Optional.ofNullable(maxAttempts);
        }
    }

    // ─── Inspeccion ──────────────────────────────────────────────────────────

    /**
     * Recorre la cadena de causas buscando un error que declare su clase.
     *
     * <p>Equivalente de {@code errors.As} de Go: funciona a traves de envoltorios
     * ({@link java.util.concurrent.CompletionException},
     * {@link java.lang.reflect.InvocationTargetException}, o cualquier excepcion de la
     * aplicacion que envuelva la causa). Es una mejora sobre el {@code instanceof} del
     * SDK de Node, que solo mira el objeto de arriba: un RetryableException envuelto por
     * una capa intermedia sigue clasificandose bien.
     */
    public static Optional<FluxException> asClassified(Throwable error) {
        Throwable t = error;
        for (int depth = 0; t != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (t instanceof FluxException fe) {
                return Optional.of(fe);
            }
            if (t.getCause() == t) {
                break;
            }
            t = t.getCause();
        }
        return Optional.empty();
    }

    /** Informa si el error (o alguno de los que envuelve) declara su clase. */
    public static boolean isClassified(Throwable error) {
        return asClassified(error).isPresent();
    }

    /**
     * Busca en la cadena de causas el primer elemento del tipo dado.
     *
     * <p>Es el {@code errors.As} generico de Go. Lo usa el clasificador y esta expuesto
     * porque una regla de la aplicacion casi siempre necesita lo mismo.
     *
     * <p>{@code <T>} no esta acotado a {@link Throwable} a proposito: el tipo buscado suele
     * ser una INTERFAZ que la excepcion implementa ({@link Classifier.HttpStatusAware}), y
     * una interfaz no extiende Throwable. Es el equivalente de que {@code errors.As} de Go
     * acepte cualquier target que implemente la interfaz buscada.
     */
    public static <T> Optional<T> findCause(Throwable error, Class<T> type) {
        Throwable t = error;
        for (int depth = 0; t != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (type.isInstance(t)) {
                return Optional.of(type.cast(t));
            }
            if (t.getCause() == t) {
                break;
            }
            t = t.getCause();
        }
        return Optional.empty();
    }
}
