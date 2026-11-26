/*
 * Estado replicado: nombres, validacion y sobre — funciones PURAS y deterministas.
 * Contrato normativo: specification/10-state.md, specification/04-errors.md §6.
 *
 * Un EVENTO dice que paso; el ESTADO es el valor ACTUAL de un agregado, que su dueno publica y los
 * demas observan. Este fichero es la mitad pura de la capacidad (sin reloj, sin UUID, sin red): el
 * subject, el sobre de un `put`, la lapida y el evento de DLQ de estado. Producen los MISMOS BYTES
 * en todos los SDKs (10-state.md §2.6; arnes cruzado `state-build`, `state-tombstone`,
 * `state-subject` y `state-dlq`). Lo que las alimenta —`id`, `time`— se pasa DE FUERA: si la funcion
 * los inventara, dos ejecuciones no serian comparables. La otra mitad, la que habla con el broker,
 * es {@link FluxState}.
 *
 * No importa nada de io.nats: el arnes de conformidad se compila con este fichero y solo con Jackson.
 */
package com.flux;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flux.StateErrors.InvalidDataClassificationException;
import com.flux.StateErrors.InvalidStateContractException;
import com.flux.StateErrors.InvalidStateKeyException;
import com.flux.StateErrors.InvalidTenantForStateException;
import com.flux.StateErrors.MissingAggregateVersionException;
import java.util.Locale;
import java.util.regex.Pattern;

/** Clase de utilidad: no se instancia. */
public final class StateEnvelope {

    private StateEnvelope() {
        throw new AssertionError("clase de utilidad");
    }

    // ─── Constantes y reglas del protocolo — protocol.json → `state` ─────────

    /**
     * {@code ^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$}: un unico token de NATS. Coincidencia COMPLETA
     * ({@code matches()}): el {@code $} de una regex de Java acepta UN {@code \n} final, asi que
     * {@code "cli-1\n"} pasaria un {@code find()} (10-state.md §2.3).
     */
    public static final Pattern KEY_PATTERN = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_-]{0,127}");

    /** {@code ^[a-z0-9]+(-[a-z0-9]+)*$} y, aparte, 64 como maximo. */
    public static final Pattern TENANT_PATTERN = Pattern.compile("[a-z0-9]+(-[a-z0-9]+)*");

    /** NATS limita los nombres de consumidor a 255 y el durable de un observador lleva el tenant (A-42). */
    public static final int TENANT_MAX_LENGTH = 64;

    /** {@code <dominio>.<agregado>.v<major>}: el subject sin {@code state.}, sin tenant y sin clave. */
    public static final Pattern CONTRACT_PATTERN =
            Pattern.compile("[a-z0-9]+(-[a-z0-9]+)*\\.[a-z0-9]+(-[a-z0-9]+)*\\.v[1-9][0-9]{0,3}");

    /** 2^53 − 1: el mayor entero que Node, y todo lo que use {@code double}, representa sin perder precision. */
    public static final long AGGREGATE_VERSION_MAX = 9_007_199_254_740_991L;

    /** Intentos de un {@code put}/{@code delete} ante un conflicto de secuencia (10-state.md §3.2, paso 4). */
    public static final int MAX_ATTEMPTS = 3;

    /**
     * El {@code dataschema} FIJO de toda lapida, de cualquier contrato.
     */
    public static final String TOMBSTONE_DATASCHEMA = "https://schemas.internal/system/state/eliminado/1.0.0.json";

    /** Base de los esquemas del repo: {@code schemas/state/<dom>/<agr>/<semver>.json} cuelga de aqui. */
    public static final String SCHEMA_BASE = "https://schemas.internal";

    /** Un {@code put} sin {@code classification} usa ESTA, no la de la conexion (10-state.md §3.6). */
    public static final String DEFAULT_CLASSIFICATION = "confidential";

    /**
     * Lo que sigue al codigo en el {@code dlqerror} de un error que NO es de flux (10-state.md §6): el
     * mensaje de un driver de base de datos puede citar la fila entera, y la DLQ guarda 90 dias. La
     * misma frase que Node, Rust y {@code protocol.json → state.dlq.dlqerrorFixedPhrase}.
     */
    public static final String UNCLASSIFIED_DLQ_ERROR = "error sin clasificar de la aplicación; el mensaje se omite "
            + "para no copiar datos a la DLQ (10-state.md §6)";

    // ─── Validacion — se hace SIEMPRE, antes de abrir ninguna peticion ───────

    /** Un contrato ya validado. */
    public record ParsedContract(String contract, String domain, String aggregate, int major) {
    }

    /**
     * Valida un contrato. Primero el dominio reservado ({@code RESERVED_DOMAIN}), despues la forma
     * ({@code INVALID_STATE_CONTRACT}): el orden de 10-state.md §3.6.
     */
    public static ParsedContract parseContract(String contract) {
        if (contract == null) {
            throw new InvalidStateContractException(null, "debe ser una cadena");
        }
        ReservedDomainException.assertNotReserved(contract, "contrato");
        if (!CONTRACT_PATTERN.matcher(contract).matches()) {
            int n = contract.split("\\.", -1).length;
            throw new InvalidStateContractException(contract, n != 3
                    ? "debe tener exactamente 3 tokens, tiene " + n
                    : "formato esperado <dominio>.<agregado>.v<major> (la version mayor, de 1 a 4 digitos: "
                            + "v1…v9999)");
        }
        String[] t = contract.split("\\.", -1);
        return new ParsedContract(contract, t[0], t[1], Integer.parseInt(t[2].substring(1)));
    }

    public static void assertKey(String key) {
        if (key == null || !KEY_PATTERN.matcher(key).matches()) {
            throw new InvalidStateKeyException(key);
        }
    }

    public static void assertTenant(String tenant) {
        // {@code length()} cuenta unidades UTF-16: el patron solo admite ASCII, asi que es lo mismo.
        if (tenant == null || tenant.length() > TENANT_MAX_LENGTH || !TENANT_PATTERN.matcher(tenant).matches()) {
            throw new InvalidTenantForStateException(tenant);
        }
    }

    /**
     * ¿Es {@code v} un {@code aggregateVersion} valido? Un entero >= 1 y <= 2^53 − 1. En Java
     * {@code 7.0} NO es un entero (Jackson lo lee como decimal): se rechaza, como en Rust y a
     * diferencia de Node, donde es indistinguible de {@code 7} (10-state.md §3.4).
     */
    static boolean isValidAggregateVersion(JsonNode v) {
        return v != null && v.isIntegralNumber() && v.canConvertToLong()
                && v.longValue() >= 1 && v.longValue() <= AGGREGATE_VERSION_MAX;
    }

    /**
     * Lee el {@code aggregateVersion} de un {@code data} o lanza {@code MISSING_AGGREGATE_VERSION}. El
     * nombre cubre todo lo que no sea un entero >= 1: ausente, null, una cadena, un decimal, 0, un
     * negativo o uno mayor que 2^53 − 1. Se comprueba SIEMPRE, con o sin L3: es una regla del
     * protocolo, no del esquema.
     */
    public static long aggregateVersionOf(JsonNode data) {
        JsonNode v = data != null && data.isObject() ? data.get("aggregateVersion") : null;
        if (!isValidAggregateVersion(v)) {
            throw new MissingAggregateVersionException(describe(v));
        }
        return v.longValue();
    }

    /** Que llego, SIN citar el valor de un campo de texto: {@code data} es dato personal. */
    private static String describe(JsonNode v) {
        if (v == null) {
            return "no hay ninguno";
        }
        if (v.isNull()) {
            return "llego null";
        }
        if (v.isNumber()) {
            return "llego " + v.asText();
        }
        if (v.isTextual()) {
            return "llego una cadena";
        }
        return "llego un valor de tipo " + (v.isBoolean() ? "booleano" : v.isArray() ? "array" : "objeto");
    }

    /** {@code null} → {@code confidential}; {@code restricted} o una fuera del enum → {@code INVALID_DATACLASSIFICATION}. */
    public static String assertClassification(String c) {
        if (c == null) {
            return DEFAULT_CLASSIFICATION;
        }
        if (!c.equals("public") && !c.equals("internal") && !c.equals("confidential")) {
            throw new InvalidDataClassificationException(c, true);
        }
        return c;
    }

    // ─── Nombres — 02-naming.md, 10-state.md §2 ──────────────────────────────

    /** {@code STATE_CLIENTES}. NATS no admite puntos en nombres de stream. */
    public static String streamName(String domain) {
        return "STATE_" + domain.replace('-', '_').toUpperCase(Locale.ROOT);
    }

    /**
     * {@code state.<dominio>.<agregado>.v<major>.<tenant>.<clave>}: el subject de NATS de una entrada.
     * Seis tokens, siempre. NO es el atributo {@code subject} de CloudEvents, que aqui vale la clave.
     *
     * <p>Valida en el orden de 10-state.md §3.6 —dominio reservado, forma del contrato, clave, tenant—
     * y lanza con el codigo estable de cada rechazo.
     */
    public static String subject(String contract, String tenantId, String key) {
        ParsedContract c = parseContract(contract);
        assertKey(key);
        assertTenant(tenantId);
        return "state." + c.contract() + "." + tenantId + "." + key;
    }

    /** {@code com.flux.state.<dominio>.<agregado>.v<major>}. */
    public static String type(String contract) {
        ParsedContract c = parseContract(contract);
        return "com.flux.state." + c.domain() + "." + c.aggregate() + ".v" + c.major();
    }

    /** {@code com.flux.state.<dominio>.<agregado>.deleted.v<major>}. */
    public static String tombstoneType(String contract) {
        ParsedContract c = parseContract(contract);
        return "com.flux.state." + c.domain() + "." + c.aggregate() + ".deleted.v" + c.major();
    }

    /** {@code https://schemas.internal/state/<dominio>/<agregado>/<major>.0.0.json} (o con otra base). */
    public static String dataschema(String contract, String base) {
        ParsedContract c = parseContract(contract);
        String b = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        return b + "/state/" + c.domain() + "/" + c.aggregate() + "/" + c.major() + ".0.0.json";
    }

    /**
     * {@code <servicio>__state_<dom>_<agr>_v<M>} y, si filtra por tenant, el sufijo {@code __<tenant>}.
     * Los puntos y los guiones pasan a {@code _}. El tenant va en el nombre porque UN DURABLE COMPARTIDO
     * REPARTE LOS MENSAJES entre quienes lo usan (A-47): dos instancias del mismo servicio, una por
     * tenant, con un durable sin sufijo, se quitarian la mitad de la foto cada una.
     */
    public static String durableName(String service, String contract, String tenantId) {
        Protocol.validateService(service);
        ParsedContract c = parseContract(contract);
        // El servicio conserva sus guiones (`tienda-web__…`); lo que viene despues, no.
        String base = service + "__state_" + c.domain().replace('-', '_') + "_" + c.aggregate().replace('-', '_')
                + "_v" + c.major();
        if (tenantId == null) {
            return base;
        }
        assertTenant(tenantId);
        return base + "__" + tenantId.replace('-', '_');
    }

    /** {@code state.<dom>.<agr>.v<M>.<tenant>.*}, o {@code ….*.*} sin tenant. */
    public static String filterSubject(String contract, String tenantId) {
        ParsedContract c = parseContract(contract);
        if (tenantId != null) {
            assertTenant(tenantId);
        }
        return "state." + c.contract() + "." + (tenantId == null ? "*" : tenantId) + ".*";
    }

    /** {@code dlq.state.<dom>.<agr>.v<M>}: SIN tenant ni clave, para que no se multiplique por ellos. */
    public static String dlqSubject(String contract) {
        return "dlq.state." + parseContract(contract).contract();
    }

    // ─── El sobre — funciones PURAS (10-state.md §2.5, §2.6, §4, §6) ─────────

    /** Lo que comprueba todo constructor de estado ademas de la forma: que lo que rellena el SDK no llegue vacio. */
    private static String envelopeInput(String id, String source, String correlationid, String producerversion,
                                        String time) {
        Envelope.requireNonEmpty("id", id, false);
        Envelope.requireNonEmpty("source", source, false);
        Envelope.requireNonEmpty("correlationid", correlationid, true);
        Envelope.requireNonEmpty("producerversion", producerversion, true);
        return canonicalTimeOrThrow(time);
    }

    /** {@code time} en su forma canonica, o {@code WRONG_ATTRIBUTE_TYPE} si no es un instante RFC 3339. */
    static String canonicalTimeOrThrow(String time) {
        String canonical = CanonicalTime.canonical(time);
        if (canonical == null) {
            throw new Envelope.EnvelopeException("`time` debe ser un instante RFC 3339 "
                    + "(`2026-08-20T10:41:07.318Z`; se normaliza a 3 decimales y `Z`) y llego "
                    + (time == null ? "ausente" : "un texto que no lo es") + " (01-envelope.md §2.2).",
                    "WRONG_ATTRIBUTE_TYPE");
        }
        return canonical;
    }

    /** Lo que alimenta el sobre de un {@code put}. {@code id} y {@code time} los da quien llama. */
    public static final class EventInput {
        String contract;
        String tenantId;
        String key;
        String id;
        String source;
        String time;
        String correlationid;
        String producerversion;
        String classification;
        Object data;
        String dataschema;
        String causationid;
        String traceparent;
        String tracestate;

        public EventInput contract(String v) {
            this.contract = v;
            return this;
        }

        public EventInput tenantId(String v) {
            this.tenantId = v;
            return this;
        }

        public EventInput key(String v) {
            this.key = v;
            return this;
        }

        public EventInput id(String v) {
            this.id = v;
            return this;
        }

        public EventInput source(String v) {
            this.source = v;
            return this;
        }

        /** ISO-8601; se normaliza a 3 decimales y {@code Z}, o se rechaza. */
        public EventInput time(String v) {
            this.time = v;
            return this;
        }

        public EventInput correlationid(String v) {
            this.correlationid = v;
            return this;
        }

        public EventInput producerversion(String v) {
            this.producerversion = v;
            return this;
        }

        /** {@code public}, {@code internal} o {@code confidential} (el default: NO el de la conexion). */
        public EventInput classification(String v) {
            this.classification = v;
            return this;
        }

        /** Debe ser un objeto con {@code aggregateVersion}. */
        public EventInput data(Object v) {
            this.data = v;
            return this;
        }

        /** Por defecto, {@code https://schemas.internal/state/<dom>/<agr>/<major>.0.0.json}. */
        public EventInput dataschema(String v) {
            this.dataschema = v;
            return this;
        }

        public EventInput causationid(String v) {
            this.causationid = v;
            return this;
        }

        public EventInput traceparent(String v) {
            this.traceparent = v;
            return this;
        }

        public EventInput tracestate(String v) {
            this.tracestate = v;
            return this;
        }
    }

    /**
     * El sobre de un {@code put}: un CloudEvent normal, con {@code subject} = la clave, {@code tenantid}
     * = el token de tenant del subject de NATS, {@code partitionkey} = la clave, y {@code data} el
     * ULTIMO atributo.
     *
     * <p><b>Pura y determinista</b>: {@code id} y {@code time} los da quien llama. Valida lo mismo que
     * {@code put} (§3.6, salvo L3), asi que lo que construye es una entrada legal.
     */
    public static FluxEvent buildEvent(EventInput in) {
        ParsedContract c = parseContract(in.contract);
        assertKey(in.key);
        assertTenant(in.tenantId);
        JsonNode data = dataNodeOrNull(in.data);
        aggregateVersionOf(data);
        String classification = assertClassification(in.classification);
        String time = envelopeInput(in.id, in.source, in.correlationid, in.producerversion, in.time);
        return new FluxEvent(
                Protocol.SPEC_VERSION, in.id, in.source, type(c.contract()), time, Protocol.DATA_CONTENT_TYPE,
                in.dataschema != null ? in.dataschema : dataschema(c.contract(), SCHEMA_BASE),
                in.key, in.correlationid, in.tenantId, in.producerversion,
                FluxEvent.DataClassification.fromWire(classification),
                in.causationid, in.key, in.traceparent, in.tracestate,
                null, null, null, null, null, null, null, data);
    }

    /** {@code data} como JSON si ya es un objeto; {@code null} si es {@code null}; si no, el arbol que le salga. */
    private static JsonNode dataNodeOrNull(Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof JsonNode node) {
            return node;
        }
        try {
            return Envelope.mapper().valueToTree(data);
        } catch (IllegalArgumentException e) {
            throw new Envelope.EnvelopeException("`data` no es serializable a JSON: " + e.getMessage(), e);
        }
    }

    /** Lo que alimenta la lapida de un {@code delete}. */
    public static final class TombstoneInput {
        String contract;
        String tenantId;
        String key;
        String id;
        String source;
        String time;
        String correlationid;
        String producerversion;
        Object aggregateVersion;
        String causationid;
        String traceparent;
        String tracestate;

        public TombstoneInput contract(String v) {
            this.contract = v;
            return this;
        }

        public TombstoneInput tenantId(String v) {
            this.tenantId = v;
            return this;
        }

        public TombstoneInput key(String v) {
            this.key = v;
            return this;
        }

        public TombstoneInput id(String v) {
            this.id = v;
            return this;
        }

        public TombstoneInput source(String v) {
            this.source = v;
            return this;
        }

        public TombstoneInput time(String v) {
            this.time = v;
            return this;
        }

        public TombstoneInput correlationid(String v) {
            this.correlationid = v;
            return this;
        }

        public TombstoneInput producerversion(String v) {
            this.producerversion = v;
            return this;
        }

        /** La version DE LA LAPIDA: debe superar la del valor que borra. Un entero, o un {@link JsonNode} numerico. */
        public TombstoneInput aggregateVersion(Object v) {
            this.aggregateVersion = v;
            return this;
        }

        public TombstoneInput causationid(String v) {
            this.causationid = v;
            return this;
        }

        public TombstoneInput traceparent(String v) {
            this.traceparent = v;
            return this;
        }

        public TombstoneInput tracestate(String v) {
            this.tracestate = v;
            return this;
        }
    }

    /**
     * La lapida de un {@code delete}: el mismo subject de NATS que el valor, {@code type} con
     * {@code .deleted.}, {@code dataschema} FIJO, {@code dataclassification: "internal"} y
     * {@code data: { aggregateVersion }} y nada mas. Al entrar, el valor anterior SALE del stream
     * (A-34). Lleva version porque {@code put} la lee para decidir si una escritura nueva es
     * {@code stale}.
     */
    public static FluxEvent buildTombstone(TombstoneInput in) {
        ParsedContract c = parseContract(in.contract);
        assertKey(in.key);
        assertTenant(in.tenantId);
        ObjectNode data = Envelope.mapper().createObjectNode();
        if (in.aggregateVersion instanceof JsonNode node) {
            data.set("aggregateVersion", node);
        } else if (in.aggregateVersion instanceof Long l) {
            data.put("aggregateVersion", l);
        } else if (in.aggregateVersion instanceof Integer i) {
            data.put("aggregateVersion", i);
        } else if (in.aggregateVersion != null) {
            data.set("aggregateVersion", Envelope.mapper().valueToTree(in.aggregateVersion));
        }
        long version = aggregateVersionOf(data);
        String time = envelopeInput(in.id, in.source, in.correlationid, in.producerversion, in.time);
        ObjectNode tombstone = Envelope.mapper().createObjectNode();
        tombstone.put("aggregateVersion", version);
        return new FluxEvent(
                Protocol.SPEC_VERSION, in.id, in.source, tombstoneType(c.contract()), time,
                Protocol.DATA_CONTENT_TYPE, TOMBSTONE_DATASCHEMA, in.key, in.correlationid, in.tenantId,
                in.producerversion, FluxEvent.DataClassification.INTERNAL,
                in.causationid, in.key, in.traceparent, in.tracestate,
                null, null, null, null, null, null, null, tombstone);
    }

    /** Lo que se registra de un fallo al consumir una entrada. */
    public record DlqInfo(String key, long revision, FluxEvent.DlqReason reason, int attempts, String consumer,
                          String error, String dlqtime) {
    }

    /**
     * El evento de DLQ de estado: el sobre ORIGINAL con las extensiones {@code dlq*} y <b>sin su
     * dato</b>: {@code data} pasa a {@code { key, revision }}.
     *
     * <ul>
     *   <li>La DLQ guarda 90 dias: un dato personal que el dueno ya borro seguiria vivo ahi tres meses
     *       y el derecho de supresion no se cumpliria (10-state.md §6).
     *   <li><b>No conserva {@code signkeyid} ni {@code signature}</b>: firman un {@code data} que ya
     *       no esta, y una firma que no verifica sobre un sobre que parece el original es una
     *       afirmacion falsa (07-signing.md §5).
     *   <li>{@code data} sigue siendo el ULTIMO atributo, con las {@code dlq*} justo antes.
     * </ul>
     */
    public static FluxEvent toDlqEvent(FluxEvent event, DlqInfo info) {
        ObjectNode data = Envelope.mapper().createObjectNode();
        data.put("key", info.key());
        data.put("revision", info.revision());
        return new FluxEvent(
                event.specversion(), event.id(), event.source(), event.type(), event.time(),
                event.datacontenttype(), event.dataschema(), event.aggregateId(), event.correlationid(),
                event.tenantid(), event.producerversion(), event.dataclassification(), event.causationid(),
                event.partitionkey(), event.traceparent(), event.tracestate(),
                null, null,
                info.reason(), info.attempts(), info.consumer(),
                // 1024 BYTES UTF-8 sin partir un caracter (04-errors.md §3).
                Text.truncateUtf8Bytes(info.error(), 1024),
                canonicalTimeOrThrow(info.dlqtime()), data);
    }

    /**
     * Un mensaje ILEGIBLE (JSON roto, sin sobre) no se copia a la DLQ de estado: no se guarda su cuerpo
     * en ninguna forma —ni los {@code rawBase64} de los eventos—, porque podria contener el dato. No hay
     * sobre original que conservar, asi que se sintetiza uno con lo unico fiable, EL SUBJECT
     * (10-state.md §6). {@code id} y {@code time} los da quien llama.
     */
    public static FluxEvent buildUnreadableDlqEvent(String contract, String tenantId, String key, long revision,
                                                    String source, String producerversion, String id, String time,
                                                    String dataschema, FluxEvent.DlqReason reason, int attempts,
                                                    String consumer, String error, String dlqtime) {
        ParsedContract c = parseContract(contract);
        String canonical = envelopeInput(id, source, id, producerversion, time);
        FluxEvent synthetic = new FluxEvent(
                Protocol.SPEC_VERSION, id, source, type(c.contract()), canonical, Protocol.DATA_CONTENT_TYPE,
                dataschema != null ? dataschema : dataschema(c.contract(), SCHEMA_BASE), key, id, tenantId,
                producerversion, FluxEvent.DataClassification.INTERNAL, null, key, null, null,
                null, null, null, null, null, null, null, Envelope.mapper().createObjectNode());
        return toDlqEvent(synthetic, new DlqInfo(key, revision, reason, attempts, consumer, error, dlqtime));
    }
}
