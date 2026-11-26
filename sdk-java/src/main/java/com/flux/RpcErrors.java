/*
 * Errores de la capacidad `rpc` (peticion/respuesta).
 * Contrato normativo: specification/04-errors.md §7 y specification/11-request-reply.md §6.
 *
 * La lista es CERRADA: los seis codigos de abajo y ninguno mas. Un caso que no encaje en
 * ninguno es un hueco de la spec, no un codigo nuevo que este SDK pueda inventar. Lo que se
 * reutiliza (`RESERVED_DOMAIN`, `INVALID_SCHEMA`, `INVALID_DATACLASSIFICATION`,
 * `PERMISSION_DENIED`, los de la firma y los del sobre) vive donde ya estaba.
 *
 * Reglas comunes (04-errors.md §7, «Que DEBE decir el mensaje»):
 *   · el `code` es fijo y la aplicacion no puede sustituirlo: es lo que agrupa el panel;
 *   · el mensaje dice QUE esta mal y COMO se arregla;
 *   · NINGUN mensaje lleva valores de `data`: acaba en logs, y una peticion puede llevar datos
 *     personales (11-request-reply.md §3.6). Una operacion, un `id` y un tenant si pueden citarse.
 */
package com.flux;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/** Contenedor de las excepciones de peticion/respuesta. Clase de utilidad: no se instancia. */
public final class RpcErrors {

    private RpcErrors() {
        throw new AssertionError("clase de utilidad");
    }

    /**
     * Nadie escucha la operacion: el broker contesto {@code no responders} (estado 503) en
     * milisegundos. <b>RETRYABLE</b>, inmediato: no gasta el timeout. Es tambien lo que ve quien
     * llama durante el hueco de un despliegue, o con una errata en la operacion (la suscripcion de
     * quien responde es al subject EXACTO, asi que una operacion que no atiende nadie no tiene
     * interes: A-64).
     */
    public static final class RpcUnavailableException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;
        private final String operation;

        public RpcUnavailableException(String operation, Throwable cause) {
            super("nadie escucha la operacion \"" + operation + "\": el broker contesto `no responders`. "
                    + "Ningun servicio la atiende ahora mismo (¿esta desplegado y arrancado el servicio "
                    + "dueno? ¿el nombre y la version mayor son los de su contrato? ¿hay un respond() para "
                    + "esta operacion EXACTA?). Es reintentable, y falla al instante: no ha esperado al "
                    + "timeout (11-request-reply.md §4.3).", "RPC_UNAVAILABLE", cause);
            this.operation = operation;
        }

        public String operation() {
            return operation;
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.RETRYABLE;
        }
    }

    /**
     * No llego una respuesta en el timeout. <b>RETRYABLE</b> y <b>AMBIGUO</b>: la operacion PUEDE
     * haberse ejecutado (un responder lento la ejecuta y su respuesta llega tarde, A-61; uno que
     * muere con la peticion, A-62; una instancia colgada, A-63; la ventana de un cluster al irse el
     * ultimo, A-70).
     */
    public static final class RpcTimeoutException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;
        private final String operation;
        private final int timeoutMs;

        public RpcTimeoutException(String operation, int timeoutMs, Throwable cause) {
            super("no llego una respuesta a \"" + operation + "\" en " + timeoutMs + " ms. La operacion PUEDE "
                    + "haberse ejecutado: el que responde pudo ser mas lento que el timeout, o morir con la "
                    + "peticion en la mano. Un reintento debe ser seguro: usa `retries` (reutiliza el MISMO "
                    + "id) y que quien responde deduplique por el id de la peticion (11-request-reply.md "
                    + "§4.5). Si es un calculo lento, sube `timeoutMs` (maximo 25000).",
                    "RPC_TIMEOUT", cause);
            this.operation = operation;
            this.timeoutMs = timeoutMs;
        }

        public String operation() {
            return operation;
        }

        public int timeoutMs() {
            return timeoutMs;
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.RETRYABLE;
        }
    }

    /**
     * Con la firma en {@code require}, el {@code time} de la peticion se aleja mas de 300 s del reloj
     * de quien responde. <b>PERMANENT</b>. Lo lanza quien responde, DENTRO de un error
     * ({@code com.flux.rpc.error.v1}); quien llama lo recibe como un {@link RpcPermanentException}
     * con este codigo.
     */
    public static final class StaleRequestException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;

        public StaleRequestException(String time, int windowSeconds, String nowIso) {
            super("la peticion lleva `time` " + StateErrors.quote(time) + " y la hora local de quien responde "
                    + "es " + nowIso + ": se aleja mas de " + windowSeconds + " s (por delante o por detras). "
                    + "Con la firma en modo `require` la ventana acota cuanto tiempo sirve una peticion "
                    + "capturada, porque la firma por si sola no impide reenviarla. Sincroniza los relojes "
                    + "(NTP) de los dos servicios; reintentar la misma peticion da lo mismo "
                    + "(11-request-reply.md §7.4).", "STALE_REQUEST", null);
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /**
     * Lo que llego NO es la respuesta a esta peticion: no es un CloudEvent valido, su {@code type} no
     * es el de la respuesta ni el del error, su {@code causationid} no es el {@code id} de la
     * peticion, su {@code correlationid} o su {@code tenantid} no los hereda, o el {@code data} del
     * error no cumple su esquema. <b>PERMANENT</b>: es un incidente (un productor roto, un stream que
     * captura {@code rpc.>}, algo hostil), no un fallo que esperar.
     */
    public static final class RpcInvalidReplyException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;
        private final String operation;

        public RpcInvalidReplyException(String operation, String reason, boolean streamHint, Throwable cause) {
            super("lo que llego como respuesta a \"" + operation + "\" no es la respuesta a esta peticion: "
                    + reason + ". "
                    + (streamHint
                            ? "El cuerpo parece el acuse de un stream de JetStream (`{\"stream\":…,\"seq\":…}`): "
                                    + "algun stream captura `rpc.>` y contesta a cada peticion con su acuse "
                                    + "—y guarda cada peticion—. Ejecuta `flux doctor` (11-request-reply.md §8). "
                            : "Una respuesta que no es de esta peticion es un incidente (un productor roto, un "
                                    + "stream mal puesto o algo hostil), no un fallo que esperar: reintentar no "
                                    + "lo arregla (11-request-reply.md §4.3). ")
                    + "Se cuenta como `invalid_reply`: es la alerta que debe despertar a alguien.",
                    "RPC_INVALID_REPLY", cause);
            this.operation = operation;
        }

        public String operation() {
            return operation;
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /**
     * El mensaje serializado no cabe en el minimo entre 1 MiB y el {@code max_payload} del broker.
     * <b>PERMANENT</b>. En la peticion lo lanza quien llama ANTES de abrir nada; en la respuesta lo
     * detecta quien responde y contesta con un error de este codigo: publicarla lanzaria en quien
     * responde y quien llama solo veria un timeout (A-65).
     */
    public static final class RpcMessageTooLargeException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;
        private final int bytes;
        private final int limit;

        public RpcMessageTooLargeException(int bytes, int limit, String what, String operation) {
            super("la " + what + " de \"" + operation + "\" serializa a " + bytes + " bytes y el limite es "
                    + limit + " (el minimo entre 1 MiB y el max_payload que anuncia el broker; las cabeceras "
                    + "cuentan). Aplica claim-check: guarda el contenido en un almacen de objetos y envia "
                    + "`{ uri, sha256, bytes }`, o pagina con un cursor en `data` (01-envelope.md §7, "
                    + "11-request-reply.md §4.4).", "RPC_MESSAGE_TOO_LARGE", null);
            this.bytes = bytes;
            this.limit = limit;
        }

        public int bytes() {
            return bytes;
        }

        public int limit() {
            return limit;
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /**
     * La llamada, o el registro de quien responde, esta mal formada y se sabe SIN RED: la operacion
     * no cumple el patron (cuatro tokens, sin el prefijo {@code rpc.}, minusculas y kebab-case),
     * {@code timeoutMs} no es un entero entre 1 y 25 000, o {@code data} no es un objeto. En quien
     * responde, ademas, un {@code type} que no es el de la peticion de esa operacion.
     * <b>PERMANENT</b>: un error de programacion, no se cuenta.
     */
    public static final class InvalidRpcCallException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;

        public InvalidRpcCallException(String reason) {
            this(reason, null);
        }

        public InvalidRpcCallException(String reason, Throwable cause) {
            super("llamada de peticion/respuesta invalida: " + reason, "INVALID_RPC_CALL", cause);
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /**
     * La respuesta del que contesta ya traia un error {@code com.flux.rpc.error.v1} con
     * {@code class: retryable}: se convierte en un error RETRYABLE que CONSERVA su {@code code} y su
     * {@code retryAfterMs}. Un handler de eventos que llama a una operacion hereda la clasificacion
     * sin hacer nada: si el otro servicio dice {@code retryable}, el evento se reentrega con su
     * backoff (11-request-reply.md §3.3).
     */
    public static final class RpcRetryableException extends FluxErrors.FluxException
            implements Classifier.RetryAfterAware {
        private static final long serialVersionUID = 1L;
        private final String operation;
        private final transient Duration retryAfter;

        public RpcRetryableException(String operation, String code, String message, Integer retryAfterMs) {
            super(message, code, null);
            this.operation = operation;
            this.retryAfter = retryAfterMs == null ? null : Duration.ofMillis(retryAfterMs);
        }

        public String operation() {
            return operation;
        }

        /** El {@code retryAfterMs} del error, si lo traia. */
        public Optional<Duration> retryAfterOpt() {
            return Optional.ofNullable(retryAfter);
        }

        @Override
        public Duration retryAfter() {
            return retryAfter;
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.RETRYABLE;
        }
    }

    /**
     * Un error {@code com.flux.rpc.error.v1} con {@code class: permanent} —o un fallo de firma de la
     * respuesta, que para quien llama es lo mismo: reintentar da lo mismo—, convertido en un error
     * PERMANENT que CONSERVA su {@code code}. Va a la DLQ en la primera entrega si ocurre en un
     * handler de eventos.
     */
    public static final class RpcPermanentException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;
        private final String operation;

        public RpcPermanentException(String operation, String code, String message) {
            super(message, code, null);
            this.operation = operation;
        }

        public String operation() {
            return operation;
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /** Los seis codigos propios, en el orden de 04-errors.md §7. */
    static final List<String> CODES = List.of(
            "RPC_UNAVAILABLE", "RPC_TIMEOUT", "STALE_REQUEST", "RPC_INVALID_REPLY",
            "RPC_MESSAGE_TOO_LARGE", "INVALID_RPC_CALL");
}
