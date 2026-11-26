/*
 * Peticion/respuesta: nombres, validacion y sobre — funciones PURAS y deterministas.
 * Contrato normativo: specification/11-request-reply.md, specification/04-errors.md §7.
 *
 * Un EVENTO dice que paso; el ESTADO es el dato; la peticion/respuesta es la orden que necesita
 * respuesta ya y el calculo. Este fichero es la mitad pura de la capacidad (sin reloj, sin UUID, sin
 * red): el subject, los tipos y el sobre de la peticion, de la respuesta y del error. Producen los
 * MISMOS BYTES en todos los SDKs (11-request-reply.md §3.5; arnes cruzado `rpc-request`,
 * `rpc-reply`, `rpc-error`, `rpc-operation` y `rpc-timeout`). Lo que las alimenta —`id`, `time`— se
 * pasa DE FUERA. La otra mitad, la que habla con el broker, es {@link FluxRpc}.
 *
 * No importa nada de io.nats: el arnes de conformidad se compila con este fichero y solo con Jackson.
 */
package com.flux;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flux.RpcErrors.InvalidRpcCallException;
import com.flux.StateErrors.InvalidDataClassificationException;
import java.util.Iterator;
import java.util.regex.Pattern;

/** Clase de utilidad: no se instancia. */
public final class RpcEnvelope {

    private RpcEnvelope() {
        throw new AssertionError("clase de utilidad");
    }

    // ─── Constantes — protocol.json → `rpc` ──────────────────────────────────

    /** {@code rpc.timeout}: 5 s por defecto. */
    public static final int TIMEOUT_DEFAULT_MS = 5_000;

    /** {@code rpc.timeout.minMs}. */
    public static final int TIMEOUT_MIN_MS = 1;

    /**
     * {@code rpc.timeout.maxMs}: 25 s, POR DEBAJO del {@code ack_wait} de 30 s (03-delivery.md §2): para
     * que una llamada hecha desde un handler de eventos no provoque su reentrega mientras espera.
     */
    public static final int TIMEOUT_MAX_MS = 25_000;

    /** {@code rpc.type.error}: el mismo tipo para toda operacion. */
    public static final String ERROR_TYPE = "com.flux.rpc.error.v1";

    /** {@code dataschema} fijo del error. Su esquema esta en {@code schemas/system/rpc/error/1.0.0.json}. */
    public static final String ERROR_DATASCHEMA = "https://schemas.internal/system/rpc/error/1.0.0.json";

    /** Base de los esquemas del repo: {@code schemas/rpc/<dom>/<agr>/<op>/{request,reply}/…} cuelga de aqui. */
    public static final String SCHEMA_BASE = "https://schemas.internal";

    /**
     * {@code rpc.operation.pattern}: el nombre de la operacion —el subject sin {@code rpc.}— tiene la
     * MISMA forma sintactica que el subject de un evento. Coincidencia COMPLETA ({@code matches()}): el
     * {@code $} de una regex de Java acepta UN {@code \n} final.
     */
    public static final Pattern OPERATION_PATTERN = Pattern.compile(
            "[a-z0-9]+(-[a-z0-9]+)*\\.[a-z0-9]+(-[a-z0-9]+)*\\.v[1-9][0-9]{0,3}\\.[a-z0-9]+(-[a-z0-9]+)*");

    /** {@code errors.codePattern} y el {@code pattern} del esquema del error. */
    public static final Pattern ERROR_CODE_PATTERN = Pattern.compile("[A-Z][A-Z0-9]*(_[A-Z0-9]+)*");

    static final int ERROR_CODE_MAX = 64;
    static final int ERROR_MESSAGE_MAX = 1000;

    // ─── Validacion — se hace SIEMPRE, antes de abrir ninguna peticion ───────

    /** Una operacion ya validada. */
    public record ParsedOperation(String operation, String domain, String aggregate, int major, String name) {
    }

    /**
     * Valida el nombre de una operacion. Primero el dominio reservado ({@code RESERVED_DOMAIN}: tambien
     * {@code rpc.} puesto por quien llama), despues la forma ({@code INVALID_RPC_CALL}). Siempre antes de
     * tocar la red.
     */
    public static ParsedOperation parseOperation(String operation) {
        if (operation == null) {
            throw new InvalidRpcCallException("la operacion debe ser una cadena "
                    + "`<dominio>.<agregado>.v<major>.<operacion>`, y llego null.");
        }
        ReservedDomainException.assertNotReserved(operation, "operacion");
        if (!OPERATION_PATTERN.matcher(operation).matches()) {
            int n = operation.split("\\.", -1).length;
            String why = !operation.equals(operation.toLowerCase(java.util.Locale.ROOT))
                    ? "debe ir todo en minusculas —NATS distingue mayusculas, y una errata crea un subject al que "
                            + "nadie esta suscrito, sin error—"
                    : operation.indexOf('*') >= 0 || operation.indexOf('>') >= 0
                            ? "no admite comodines (`*`, `>`): el broker los aceptaria, y es una operacion concreta "
                                    + "la que se llama o se atiende"
                            : n != 4
                                    ? "debe tener exactamente 4 tokens, tiene " + n
                                    : "la version mayor es `v` y un entero de 1 a 4 digitos sin ceros a la "
                                            + "izquierda (`v1`…`v9999`), y cada token va en kebab-case (letras "
                                            + "minusculas, digitos y guiones)";
            String shown = operation.length() > 90 ? operation.substring(0, 87) + "…" : operation;
            throw new InvalidRpcCallException("la operacion " + Envelope.jsonString(shown) + " no es valida: " + why
                    + ". El nombre es `<dominio>.<agregado>.v<major>.<operacion>` —el subject sin `rpc.`—, con "
                    + "la operacion en infinitivo y kebab-case: `clientes.cliente.v1.calcular-descuento` "
                    + "(11-request-reply.md §2.2).");
        }
        String[] t = operation.split("\\.", -1);
        return new ParsedOperation(operation, t[0], t[1], Integer.parseInt(t[2].substring(1)), t[3]);
    }

    /**
     * El {@code timeoutMs} efectivo: ausente → 5000; un entero entre 1 y 25 000; cualquier otra cosa,
     * {@code INVALID_RPC_CALL} (no se recorta).
     *
     * @param timeoutMs {@code null} es «ausente» en esta sobrecarga tipada.
     */
    public static int resolveTimeout(Integer timeoutMs) {
        if (timeoutMs == null) {
            return TIMEOUT_DEFAULT_MS;
        }
        return checkTimeout(timeoutMs);
    }

    /**
     * Lo mismo para el JSON del arnes: {@code null} NO es «ausente» y es invalido; un entero JSON de 1 a
     * 25 000 y nada mas: no se recorta, no se redondea y una cadena, un booleano, un array o un objeto no
     * se convierten (11-request-reply.md §4.2).
     */
    static int resolveTimeout(JsonNode timeoutMs) {
        if (timeoutMs == null || timeoutMs.isMissingNode()) {
            return TIMEOUT_DEFAULT_MS;
        }
        if (!timeoutMs.isIntegralNumber() || !timeoutMs.canConvertToLong()) {
            throw new InvalidRpcCallException("`timeoutMs` debe ser un entero entre " + TIMEOUT_MIN_MS + " y "
                    + TIMEOUT_MAX_MS + " (milisegundos; " + TIMEOUT_DEFAULT_MS + " por defecto) y llego "
                    + (timeoutMs.isNull() ? "null" : timeoutMs.isNumber() ? timeoutMs.asText() : "un valor de otro tipo")
                    + ". No se recorta en silencio: un valor fuera de rango es un error "
                    + "(11-request-reply.md §4.2).");
        }
        long v = timeoutMs.longValue();
        if (v < TIMEOUT_MIN_MS || v > TIMEOUT_MAX_MS) {
            return checkTimeout(v < Integer.MIN_VALUE ? Integer.MIN_VALUE : v > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) v);
        }
        return (int) v;
    }

    private static int checkTimeout(int timeoutMs) {
        if (timeoutMs < TIMEOUT_MIN_MS || timeoutMs > TIMEOUT_MAX_MS) {
            throw new InvalidRpcCallException("`timeoutMs` debe ser un entero entre " + TIMEOUT_MIN_MS + " y "
                    + TIMEOUT_MAX_MS + " (milisegundos; " + TIMEOUT_DEFAULT_MS + " por defecto) y llego "
                    + timeoutMs + ". No se recorta en silencio: un valor fuera de rango es un error. El "
                    + "maximo esta por debajo del `ack_wait` de 30 s para que una llamada hecha desde un handler "
                    + "de eventos no provoque su reentrega (11-request-reply.md §4.2).");
        }
        return timeoutMs;
    }

    /** {@code data} como un objeto JSON, o {@code INVALID_RPC_CALL}. */
    static JsonNode dataObject(Object data, String what) {
        JsonNode node;
        if (data == null) {
            node = null;
        } else if (data instanceof JsonNode n) {
            node = n;
        } else {
            try {
                node = Envelope.mapper().valueToTree(data);
            } catch (IllegalArgumentException e) {
                throw new InvalidRpcCallException(what + " no es serializable a JSON: " + e.getMessage(), e);
            }
        }
        if (node == null || !node.isObject()) {
            throw new InvalidRpcCallException(what + " debe ser un objeto JSON en la raiz (ni array ni escalar), "
                    + "para poder anadir campos sin romper el contrato (01-envelope.md §4); llego "
                    + (node == null || node.isNull() ? "null" : node.isArray() ? "un array" : "un escalar") + ".");
        }
        return node;
    }

    /** Una clasificacion de las cuatro; cualquier otra, {@code INVALID_DATACLASSIFICATION}. */
    static FluxEvent.DataClassification classification(String c) {
        if (c == null) {
            throw new InvalidDataClassificationException(null, false);
        }
        for (FluxEvent.DataClassification candidate : FluxEvent.DataClassification.values()) {
            if (candidate.wire().equals(c)) {
                return candidate;
            }
        }
        throw new InvalidDataClassificationException(c, false);
    }

    /**
     * Lo que comprueba todo constructor de peticion/respuesta ademas de la operacion y {@code data}: que
     * los atributos que el SDK rellena no lleguen vacios y que {@code time} sea un instante (se
     * normaliza a la forma canonica). Todo es {@code INVALID_RPC_CALL}: el conjunto de codigos de rpc es
     * cerrado (04-errors.md §7). Devuelve {@code time} canonico.
     */
    private static String envelopeInput(String time, String... namesAndValues) {
        for (int i = 0; i < namesAndValues.length; i += 2) {
            String v = namesAndValues[i + 1];
            if (v == null || v.isEmpty()) {
                throw new InvalidRpcCallException("`" + namesAndValues[i] + "` es obligatorio y llego "
                        + (v == null ? "ausente" : "vacio") + ". Lo rellena el SDK a partir de connect() y del "
                        + "evento en curso; si estas llamando a un constructor a mano, rellenalo con una cadena "
                        + "no vacia (01-envelope.md §5).");
            }
        }
        String canonical = CanonicalTime.canonical(time);
        if (canonical == null) {
            throw new InvalidRpcCallException("`time` debe ser un instante RFC 3339 "
                    + "(`2026-08-20T10:41:07.318Z`; se normaliza a 3 decimales y `Z`) y llego "
                    + (time == null ? "ausente" : "un texto que no lo es") + " (01-envelope.md §2.2).");
        }
        return canonical;
    }

    // ─── Nombres — 11-request-reply.md §2.2, §3 ──────────────────────────────

    /**
     * {@code rpc.clientes.cliente.v1.calcular-descuento}: el subject de NATS. Cinco tokens. NO es el
     * atributo {@code subject} del sobre.
     */
    public static String subject(String operation) {
        return "rpc." + parseOperation(operation).operation();
    }

    /** {@code com.flux.rpc.<dominio>.<agregado>.<operacion>.request.v<major>}. */
    public static String requestType(String operation) {
        ParsedOperation p = parseOperation(operation);
        return "com.flux.rpc." + p.domain() + "." + p.aggregate() + "." + p.name() + ".request.v" + p.major();
    }

    /** {@code com.flux.rpc.<dominio>.<agregado>.<operacion>.reply.v<major>}. */
    public static String replyType(String operation) {
        ParsedOperation p = parseOperation(operation);
        return "com.flux.rpc." + p.domain() + "." + p.aggregate() + "." + p.name() + ".reply.v" + p.major();
    }

    /** Un lado de la operacion. */
    public enum Side {
        REQUEST("request"),
        REPLY("reply");

        private final String wire;

        Side(String wire) {
            this.wire = wire;
        }

        public String wire() {
            return wire;
        }
    }

    /**
     * {@code https://schemas.internal/rpc/<dominio>/<agregado>/<operacion>/{request,reply}/<major>.0.0.json}
     * (o con otra base): lo que se puede asumir SIN bundle. El bundle conoce el MINOR real.
     */
    public static String dataschema(String operation, Side side, String base) {
        ParsedOperation p = parseOperation(operation);
        String b = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        return b + "/rpc/" + p.domain() + "/" + p.aggregate() + "/" + p.name() + "/" + side.wire() + "/"
                + p.major() + ".0.0.json";
    }

    // ─── El sobre — funciones PURAS (11-request-reply.md §3) ─────────────────

    /** Lo que alimenta el sobre de una peticion. {@code id} y {@code time} los da quien llama. */
    public static final class RequestInput {
        String operation;
        String tenantId;
        String aggregateId;
        String id;
        String source;
        String time;
        String correlationid;
        String causationid;
        String producerversion;
        String classification;
        Object data;
        String dataschema;
        String traceparent;
        String tracestate;

        public RequestInput operation(String v) {
            this.operation = v;
            return this;
        }

        public RequestInput tenantId(String v) {
            this.tenantId = v;
            return this;
        }

        /** Va al atributo {@code subject} del sobre (y a {@code partitionkey}). NO es el subject de NATS. */
        public RequestInput aggregateId(String v) {
            this.aggregateId = v;
            return this;
        }

        public RequestInput id(String v) {
            this.id = v;
            return this;
        }

        public RequestInput source(String v) {
            this.source = v;
            return this;
        }

        public RequestInput time(String v) {
            this.time = v;
            return this;
        }

        /** El del evento o la peticion en curso; si la llamada no nace de otro, EL {@code id} PROPIO. */
        public RequestInput correlationid(String v) {
            this.correlationid = v;
            return this;
        }

        public RequestInput causationid(String v) {
            this.causationid = v;
            return this;
        }

        public RequestInput producerversion(String v) {
            this.producerversion = v;
            return this;
        }

        public RequestInput classification(String v) {
            this.classification = v;
            return this;
        }

        public RequestInput data(Object v) {
            this.data = v;
            return this;
        }

        public RequestInput dataschema(String v) {
            this.dataschema = v;
            return this;
        }

        public RequestInput traceparent(String v) {
            this.traceparent = v;
            return this;
        }

        public RequestInput tracestate(String v) {
            this.tracestate = v;
            return this;
        }
    }

    /**
     * El sobre de una peticion: un CloudEvent normal con {@code type} {@code com.flux.rpc.….request.v<M>},
     * {@code subject} y {@code partitionkey} = {@code aggregateId} si se da, y {@code data} el ULTIMO
     * atributo.
     *
     * <p><b>Pura y determinista</b>: {@code id} y {@code time} los da quien llama. Valida lo mismo que
     * {@code request} (§4.1, paso 0, salvo L3), asi que lo que construye es una peticion legal.
     */
    public static FluxEvent buildRequest(RequestInput in) {
        ParsedOperation p = parseOperation(in.operation);
        JsonNode data = dataObject(in.data, "`data`");
        FluxEvent.DataClassification classification = classification(in.classification);
        String time = envelopeInput(in.time, "id", in.id, "source", in.source, "correlationid", in.correlationid,
                "tenantid", in.tenantId, "producerversion", in.producerversion);
        return new FluxEvent(
                Protocol.SPEC_VERSION, in.id, in.source, requestType(p.operation()), time,
                Protocol.DATA_CONTENT_TYPE,
                in.dataschema != null ? in.dataschema : dataschema(p.operation(), Side.REQUEST, SCHEMA_BASE),
                in.aggregateId, in.correlationid, in.tenantId, in.producerversion, classification,
                in.causationid, in.aggregateId, in.traceparent, in.tracestate,
                null, null, null, null, null, null, null, data);
    }

    /** Lo que la respuesta HEREDA de la peticion. El arnes lo da plano; el SDK lo saca del sobre recibido. */
    public record RequestRef(String operation, String id, String correlationid, String tenantid, String subject,
                             String partitionkey) {

        /** {@code partitionkey} de la respuesta: el de la peticion, o su {@code subject} (la convencion de los eventos). */
        String partition() {
            return partitionkey != null ? partitionkey : subject;
        }
    }

    /** Lo que alimenta el sobre de una respuesta. */
    public static final class ReplyInput {
        RequestRef request;
        String id;
        String source;
        String time;
        String producerversion;
        String traceparent;
        String classification;
        Object data;
        String dataschema;

        public ReplyInput request(RequestRef v) {
            this.request = v;
            return this;
        }

        /** UUIDv7 NUEVO: nunca el de la peticion. */
        public ReplyInput id(String v) {
            this.id = v;
            return this;
        }

        public ReplyInput source(String v) {
            this.source = v;
            return this;
        }

        public ReplyInput time(String v) {
            this.time = v;
            return this;
        }

        public ReplyInput producerversion(String v) {
            this.producerversion = v;
            return this;
        }

        /** El del que responde: «de la misma traza» que la peticion, con su propio span. */
        public ReplyInput traceparent(String v) {
            this.traceparent = v;
            return this;
        }

        public ReplyInput classification(String v) {
            this.classification = v;
            return this;
        }

        public ReplyInput data(Object v) {
            this.data = v;
            return this;
        }

        public ReplyInput dataschema(String v) {
            this.dataschema = v;
            return this;
        }
    }

    /**
     * El sobre de una respuesta: HEREDA {@code correlationid}, {@code tenantid}, {@code subject} y
     * {@code partitionkey} de la peticion, y su {@code causationid} es el {@code id} de la peticion.
     * {@code id} NUEVO. {@code source}, {@code producerversion} y {@code traceparent} son del que
     * responde. La respuesta y el error llevan solo {@code traceparent}, nunca {@code tracestate}.
     */
    public static FluxEvent buildReply(ReplyInput in) {
        ParsedOperation p = parseOperation(in.request.operation());
        JsonNode data = dataObject(in.data, "la respuesta");
        FluxEvent.DataClassification classification = classification(in.classification);
        String time = envelopeInput(in.time, "id", in.id, "source", in.source, "producerversion", in.producerversion,
                "request.id", in.request.id(), "request.correlationid", in.request.correlationid(),
                "request.tenantid", in.request.tenantid());
        return new FluxEvent(
                Protocol.SPEC_VERSION, in.id, in.source, replyType(p.operation()), time, Protocol.DATA_CONTENT_TYPE,
                in.dataschema != null ? in.dataschema : dataschema(p.operation(), Side.REPLY, SCHEMA_BASE),
                in.request.subject(), in.request.correlationid(), in.request.tenantid(), in.producerversion,
                classification, in.request.id(), in.request.partition(), in.traceparent, null,
                null, null, null, null, null, null, null, data);
    }

    /** {@code data} de {@code com.flux.rpc.error.v1}: {@code class} binaria ({@code poison} no viaja), {@code code}, {@code message}, {@code retryAfterMs}. */
    public record ErrorData(String errorClass, String code, String message, Integer retryAfterMs) {

        /** Lo que ve quien llama: el {@code data} del error, ya validado por {@link RpcEnvelope#errorDataProblem}. */
        static ErrorData fromJson(JsonNode d) {
            JsonNode ra = d.get("retryAfterMs");
            return new ErrorData(d.get("class").textValue(), d.get("code").textValue(), d.get("message").textValue(),
                    ra == null ? null : ra.intValue());
        }
    }

    /**
     * ¿{@code d} cumple el esquema del error ({@code schemas/system/rpc/error/1.0.0.json})? Devuelve el
     * primer problema, o {@code null}. Es la quinta comprobacion de quien llama (§4.3) y lo que valida el
     * constructor: se escribe a mano y no con el validador de esquemas porque L3 es opcional y esta
     * comprobacion no lo es.
     */
    public static String errorDataProblem(JsonNode d) {
        if (d == null || !d.isObject()) {
            return "`data` del error no es un objeto";
        }
        for (Iterator<String> it = d.fieldNames(); it.hasNext(); ) {
            String name = it.next();
            if (!name.equals("class") && !name.equals("code") && !name.equals("message")
                    && !name.equals("retryAfterMs")) {
                return "el error lleva atributos que su esquema no admite (" + name + ")";
            }
        }
        JsonNode cls = d.get("class");
        if (cls == null || !cls.isTextual()
                || !(cls.textValue().equals("retryable") || cls.textValue().equals("permanent"))) {
            return "`class` del error debe ser `retryable` o `permanent` (`poison` no viaja)";
        }
        JsonNode code = d.get("code");
        if (code == null || !code.isTextual() || code.textValue().length() > ERROR_CODE_MAX
                || !ERROR_CODE_PATTERN.matcher(code.textValue()).matches()) {
            return "`code` del error debe ser un identificador estable en MAYUSCULAS y guiones bajos (max. 64)";
        }
        // `maxLength` de JSON Schema cuenta caracteres Unicode (puntos de codigo), no unidades UTF-16: mil emojis caben.
        JsonNode message = d.get("message");
        if (message == null || !message.isTextual() || message.textValue().isEmpty()
                || Text.codePointLength(message.textValue()) > ERROR_MESSAGE_MAX) {
            return "`message` del error debe ser un texto de 1 a 1000 caracteres";
        }
        if (d.has("retryAfterMs")) {
            JsonNode ra = d.get("retryAfterMs");
            if (!ra.isIntegralNumber() || !ra.canConvertToLong() || ra.longValue() < 0
                    || ra.longValue() > Integer.MAX_VALUE) {
                return "`retryAfterMs` del error debe ser un entero >= 0";
            }
            if (!cls.textValue().equals("retryable")) {
                return "`retryAfterMs` solo tiene sentido con `class: retryable`";
            }
        }
        return null;
    }

    /** Lo que alimenta el sobre de un error. */
    public static final class ErrorInput {
        RequestRef request;
        String id;
        String source;
        String time;
        String producerversion;
        String traceparent;
        JsonNode error;

        public ErrorInput request(RequestRef v) {
            this.request = v;
            return this;
        }

        public ErrorInput id(String v) {
            this.id = v;
            return this;
        }

        public ErrorInput source(String v) {
            this.source = v;
            return this;
        }

        public ErrorInput time(String v) {
            this.time = v;
            return this;
        }

        public ErrorInput producerversion(String v) {
            this.producerversion = v;
            return this;
        }

        public ErrorInput traceparent(String v) {
            this.traceparent = v;
            return this;
        }

        /** El {@code data} del error: {@code { class, code, message, retryAfterMs? }}. */
        public ErrorInput error(JsonNode v) {
            this.error = v;
            return this;
        }

        /** Lo mismo desde un valor tipado. */
        public ErrorInput error(ErrorData v) {
            ObjectNode n = Envelope.mapper().createObjectNode();
            n.put("class", v.errorClass());
            n.put("code", v.code());
            n.put("message", v.message());
            if (v.retryAfterMs() != null) {
                n.put("retryAfterMs", v.retryAfterMs());
            }
            this.error = n;
            return this;
        }
    }

    /**
     * El sobre de un error {@code com.flux.rpc.error.v1}: lo que el que responde devuelve EN LUGAR de la
     * respuesta, tanto el de un handler que lanzo como el que fabrica su SDK. {@code type},
     * {@code dataschema} y {@code dataclassification: internal} son FIJOS —el error no depende del
     * contrato de la operacion—, y {@code data} lleva las claves en el orden {@code class, code, message,
     * retryAfterMs}.
     */
    public static FluxEvent buildError(ErrorInput in) {
        parseOperation(in.request.operation());
        // Un error que no cumple su esquema es `INVALID_RPC_CALL` (04-errors.md §7): un codigo estable.
        String problem = errorDataProblem(in.error);
        if (problem != null) {
            throw new InvalidRpcCallException("el error que se iba a contestar no cumple su esquema: " + problem);
        }
        String time = envelopeInput(in.time, "id", in.id, "source", in.source, "producerversion", in.producerversion,
                "request.id", in.request.id(), "request.correlationid", in.request.correlationid(),
                "request.tenantid", in.request.tenantid());
        ErrorData e = ErrorData.fromJson(in.error);
        ObjectNode data = Envelope.mapper().createObjectNode();
        data.put("class", e.errorClass());
        data.put("code", e.code());
        data.put("message", e.message());
        if (e.retryAfterMs() != null) {
            data.put("retryAfterMs", e.retryAfterMs());
        }
        return new FluxEvent(
                Protocol.SPEC_VERSION, in.id, in.source, ERROR_TYPE, time, Protocol.DATA_CONTENT_TYPE,
                ERROR_DATASCHEMA, in.request.subject(), in.request.correlationid(), in.request.tenantid(),
                in.producerversion, FluxEvent.DataClassification.INTERNAL, in.request.id(), in.request.partition(),
                in.traceparent, null,
                null, null, null, null, null, null, null, data);
    }
}
