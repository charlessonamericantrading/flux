/*
 * Envelope CloudEvents 1.0 — construccion, serializacion, parseo y DLQ.
 * Contrato normativo: specification/01-envelope.md
 *
 * flux v1 usa structured mode: el CloudEvent completo viaja serializado como JSON en el
 * cuerpo del mensaje NATS. Un mensaje ES un fichero JSON completo — lo copias, lo pegas
 * en un test, lo reproduces. Esa propiedad vale mas que los bytes que ahorraria binary
 * mode repartiendo los atributos entre cabeceras `ce-*` y cuerpo.
 */
package com.flux;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.type.LogicalType;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Construccion, serializacion y parseo del envelope.
 *
 * <p>Clase de utilidad: no se instancia.
 */
public final class Envelope {

    private Envelope() {
        throw new AssertionError("clase de utilidad");
    }

    // ─── Jackson ─────────────────────────────────────────────────────────────

    /**
     * El unico {@link ObjectMapper} del SDK, configurado para el contrato.
     *
     * <p>Cada opcion esta puesta a proposito y varias son "el default de Jackson"; se
     * escriben igualmente porque el contrato depende de ellas y un cambio de default en
     * una version futura seria una regresion silenciosa del protocolo:
     *
     * <ul>
     *   <li>{@code ACCEPT_CASE_INSENSITIVE_PROPERTIES} DESACTIVADO — la comparacion de
     *       nombres de atributo es case-sensitive (01-envelope.md §2.3). Con esto
     *       activado, {@code {"ID": "..."}} poblaria {@code id} en silencio: exactamente
     *       el fantasma que la spec combate en los subjects de NATS y el mismo agujero
     *       que {@code encoding/json} de Go tiene por defecto.</li>
     *   <li>{@code ACCEPT_CASE_INSENSITIVE_ENUMS} DESACTIVADO — {@code "Confidential"} no
     *       es un valor del protocolo.</li>
     *   <li>{@code FAIL_ON_UNKNOWN_PROPERTIES} ACTIVADO — los atributos raiz son una
     *       lista CERRADA (01-envelope.md §3.3). {@link #parseEvent} ademas los enumera
     *       antes de deserializar, para poder decir cuales sobran.</li>
     *   <li>{@code FAIL_ON_TRAILING_TOKENS} ACTIVADO — {@code {"a":1} basura} no es un
     *       mensaje valido. Sin esto Jackson lee el primer valor y descarta el resto.</li>
     *   <li>{@code NON_NULL} — ausente != vacio (01-envelope.md §3.3). Aplica tambien al
     *       payload: un campo a {@code null} dentro de {@code data} se omite, que es lo
     *       que pide 01-envelope.md §4 ("NO DEBE contener null para significar
     *       ausente").</li>
     *   <li>{@code USE_BIG_DECIMAL_FOR_FLOATS} + {@code STRIP_TRAILING_BIGDECIMAL_ZEROES}
     *       desactivado — al reparsear un evento, {@code 4995.00} se conserva como
     *       {@code 4995.00} y no se normaliza a {@code 4995.0} ni a {@code 4995}. Es lo que
     *       permite que el replay desde la DLQ devuelva el payload como llego. Ver README
     *       §"Fricciones", C.</li>
     * </ul>
     *
     * <p>No se toca el escapado: Jackson NO escapa {@code <}, {@code >}, {@code &} ni
     * caracteres no ASCII, que es justo lo que hace {@code JSON.stringify} de Node. Go
     * necesita desactivar el escapado de HTML explicitamente; aqui el default ya coincide.
     * Que coincida por defecto no lo hace opcional: 01-envelope.md §1.1 exige UTF-8 literal
     * y prohibe los escapes {@code \\uXXXX} —el encoder por defecto de
     * {@code System.Text.Json} los emite y rompe la paridad byte a byte—, asi que
     * {@code EnvelopeTest.serializeEmiteUtf8Literal} lo fija en vez de confiar en el default.
     */
    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .disable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
            .disable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false)
            .serializationInclusion(JsonInclude.Include.NON_NULL)
            // Un atributo de texto DEBE llegar como texto. Sin esto, Jackson convierte por
            // su cuenta un `"tenantid": 42` en la cadena "42" y un `true` en "true": el
            // mensaje se acepta con un valor que el productor nunca escribio, y el mismo
            // cuerpo seria POISON en Go (encoding/json no coacciona escalares a string).
            // Un envelope que significa cosas distintas en dos SDKs deja de ser un contrato.
            .withCoercionConfig(LogicalType.Textual, config -> config
                    .setCoercion(CoercionInputShape.Integer, CoercionAction.Fail)
                    .setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
                    .setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail))
            .build();

    /**
     * El mapper configurado, para los tests que necesitan comprobar la configuracion en si
     * (que {@code ACCEPT_CASE_INSENSITIVE_PROPERTIES} sigue desactivado, por ejemplo).
     *
     * <p>Package-private a proposito: exponer el mapper en la API publica invitaria a que
     * las aplicaciones lo reconfiguren, y cualquier cambio en el se convierte en un cambio
     * del formato del envelope.
     */
    static ObjectMapper mapper() {
        return MAPPER;
    }

    // ─── Tiempo ──────────────────────────────────────────────────────────────

    /**
     * RFC 3339 UTC con precision de milisegundos EXACTA — 01-envelope.md §2.2.
     *
     * <p>No se usa {@link DateTimeFormatter#ISO_INSTANT} a proposito: recorta los ceros
     * finales, asi que {@code 10:25:39.410Z} se serializaria como {@code 10:25:39.41Z}.
     * Seria RFC 3339 valido pero dejaria de ser byte a byte igual al {@code toISOString()}
     * de Node, al de Python y al de Go, y el envelope tiene que ser identico en los cuatro
     * lenguajes: de ello dependen el replay verbatim desde la DLQ, la firma criptografica
     * del evento y los fixtures compartidos de la suite de conformidad.
     *
     * <p>{@code uuuu} y no {@code yyyy}: {@code yyyy} es "year-of-era" y exige una era
     * para resolverse en modo estricto. Con {@code uuuu} el ano es el proleptico y el
     * formateador funciona sin ambiguedad.
     */
    public static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    /** Serializa un instante en el formato del protocolo (UTC, exactamente 3 decimales). */
    public static String formatTime(Instant instant) {
        return TIME_FORMATTER.format(instant);
    }

    /**
     * Interpreta un {@code time} del envelope.
     *
     * <p>Acepta tambien offsets distintos de {@code Z} aunque el SDK nunca los emita: un
     * consumidor debe poder leer lo que otro productor haya escrito, y rechazar aqui
     * convertiria un evento procesable en POISON.
     */
    public static Instant parseTime(String value) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            return OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant();
        }
    }

    // ─── Listas del contrato ─────────────────────────────────────────────────

    /**
     * Los UNICOS atributos admitidos en la raiz del envelope. Todo lo demas va dentro de
     * {@code data} — 01-envelope.md §3.3.
     *
     * <p>La lista es cerrada y eso es deliberado: las extensiones de CloudEvents solo
     * admiten String, Integer, Boolean, Binary, URI y Timestamp, nunca objetos ni arrays.
     * Un equipo que empieza a colgar metadatos de la raiz acaba serializando JSON dentro
     * de un string, y a partir de ahi el envelope deja de ser interoperable.
     */
    public static final Set<String> ALLOWED_ROOT_ATTRIBUTES = Set.of(
            "specversion", "id", "source", "type", "time", "datacontenttype", "dataschema", "subject",
            "correlationid", "tenantid", "producerversion", "dataclassification",
            "causationid", "partitionkey", "traceparent", "tracestate",
            // Extension OPCIONAL de firma — 07-signing.md §4. Van en la lista aunque el
            // SDK tenga la verificacion en `off`: un consumidor que no verifica DEBE aun
            // asi poder LEER un evento firmado, o la adopcion gradual de la firma
            // convertiria en POISON todo evento de un productor ya migrado.
            "signkeyid", "signature",
            "dlqreason", "dlqattempts", "dlqconsumer", "dlqerror", "dlqtime",
            "data");

    /** Los que, si faltan, hacen POISON al mensaje — 04-errors.md §1.3. */
    private static final List<String> REQUIRED_ATTRIBUTES = List.of(
            "specversion", "id", "source", "type", "time", "datacontenttype", "dataschema", "data");

    /**
     * Extensiones del perfil flux cuya ausencia es POISON — 01-envelope.md §3.1.
     *
     * <p>Se exigen de verdad, no solo en la prosa. Si no se exigieran no serian
     * obligatorias, serian recomendadas, y cada consumidor tendria que tratar el caso
     * ausente — que es exactamente lo que declararlas obligatorias pretendia evitar.
     *
     * <p>Ademas, asumir un valor por defecto es peligroso en las cuatro: un
     * {@code dataclassification} ausente tomado como {@code internal} hace que PII circule
     * con 30 dias de retencion en vez de 7, y un {@code tenantid} ausente tomado como
     * {@code system} cruza fronteras de tenant. Ver 06-security.md §4 y §5.
     */
    private static final List<String> REQUIRED_EXTENSIONS = List.of(
            "correlationid", "tenantid", "producerversion", "dataclassification");

    /** Los cuatro valores del enum de 01-envelope.md §3.1. */
    private static final Set<String> VALID_CLASSIFICATIONS =
            Set.of("public", "internal", "confidential", "restricted");

    /**
     * Atributos que DEBEN llegar como cadena JSON — 01-envelope.md §2.4.
     *
     * <p>La configuracion de coercion del mapper ya impide que Jackson convierta un
     * {@code 42} en {@code "42"}, pero ese fallo llega envuelto en un error de
     * deserializacion generico e indistinguible de un {@code "dlqattempts": "seis"}. Se
     * comprueban aqui, sobre el {@link JsonNode}, para que el codigo sea el mismo que dan
     * Node, Python y Go ante el mismo cuerpo, y para que el operador vea que atributo de
     * identidad llego con el tipo equivocado.
     */
    private static final List<String> STRING_ATTRIBUTES = List.of(
            "id", "source", "type", "time", "correlationid", "tenantid", "producerversion");

    /** Limite del texto de {@code dlqerror}. */
    private static final int DLQ_ERROR_MAX_CHARS = 1024;

    /**
     * Envelope que el SDK se niega a construir o serializar.
     *
     * <p>A diferencia de {@link FluxErrors.PoisonException}, se produce del lado del
     * productor: es un bug local, no un mensaje ajeno corrupto.
     */
    public static final class EnvelopeException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public EnvelopeException(String message) {
            super(message);
        }

        public EnvelopeException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // ─── Construccion ────────────────────────────────────────────────────────

    /**
     * Todo lo que hace falta para construir un evento.
     *
     * <p>El desarrollador de la aplicacion NO rellena esto: lo rellena
     * {@link FluxBus#publish} a partir de la configuracion de
     * {@link FluxBus#connect} y del contexto del evento en curso. Se expone para tests y
     * para quien construya un transporte propio — 01-envelope.md §5.
     *
     * <p>Es un builder mutable y no un record porque son 15 campos de los que 6 son
     * opcionales: el constructor canonico equivalente seria una fila de {@code null}s
     * imposible de leer en la llamada. Go resuelve lo mismo con un struct de campos con
     * nombre; Java no tiene literales de struct.
     */
    public static final class BuildEventInput {
        private String subject;
        private Object data;
        private String id;
        private String source;
        private String producerVersion;
        private String tenantId;
        private FluxEvent.DataClassification dataClassification;
        private String dataSchema;
        private String correlationId;
        private Instant time;
        private String aggregateId;
        private String causationId;
        private String partitionKey;
        private String traceparent;
        private String tracestate;

        /** Subject de NATS. Determina el {@code type}. */
        public BuildEventInput subject(String v) {
            this.subject = v;
            return this;
        }

        /** Debe serializar a un OBJETO JSON en la raiz. */
        public BuildEventInput data(Object v) {
            this.data = v;
            return this;
        }

        public BuildEventInput id(String v) {
            this.id = v;
            return this;
        }

        public BuildEventInput source(String v) {
            this.source = v;
            return this;
        }

        public BuildEventInput producerVersion(String v) {
            this.producerVersion = v;
            return this;
        }

        public BuildEventInput tenantId(String v) {
            this.tenantId = v;
            return this;
        }

        public BuildEventInput dataClassification(FluxEvent.DataClassification v) {
            this.dataClassification = v;
            return this;
        }

        public BuildEventInput dataSchema(String v) {
            this.dataSchema = v;
            return this;
        }

        public BuildEventInput correlationId(String v) {
            this.correlationId = v;
            return this;
        }

        /**
         * Instante en que OCURRIO el hecho, no el de publicacion — 01-envelope.md §2.
         * {@code null} significa "ahora".
         */
        public BuildEventInput time(Instant v) {
            this.time = v;
            return this;
        }

        /** Va al atributo {@code subject} de CloudEvents. Ver {@link FluxEvent}. */
        public BuildEventInput aggregateId(String v) {
            this.aggregateId = v;
            return this;
        }

        public BuildEventInput causationId(String v) {
            this.causationId = v;
            return this;
        }

        public BuildEventInput partitionKey(String v) {
            this.partitionKey = v;
            return this;
        }

        public BuildEventInput traceparent(String v) {
            this.traceparent = v;
            return this;
        }

        public BuildEventInput tracestate(String v) {
            this.tracestate = v;
            return this;
        }

        /** Atajo para {@code Envelope.buildEvent(this)}. */
        public FluxEvent build() {
            return buildEvent(this);
        }
    }

    /**
     * Construye un evento valido a partir de la entrada.
     *
     * <p>Valida en tiempo de ejecucion lo que en TypeScript garantizaba el sistema de
     * tipos: aqui cualquier String puede ser {@code null} o {@code ""}, asi que las cuatro
     * extensiones obligatorias se comprueban una por una.
     */
    public static FluxEvent buildEvent(BuildEventInput in) {
        // Valida el subject y deriva el type. Va primero para que un subject mal escrito
        // falle con su mensaje propio y no con "dataschema es obligatorio".
        String type = Protocol.subjectToType(in.subject);

        JsonNode data = toDataObject(in.data);

        requireNonEmpty("id", in.id);
        requireNonEmpty("source", in.source);
        requireNonEmpty("dataschema", in.dataSchema);
        requireNonEmpty("correlationid", in.correlationId);
        requireNonEmpty("tenantid", in.tenantId);
        requireNonEmpty("producerversion", in.producerVersion);
        if (in.dataClassification == null) {
            throw new EnvelopeException("dataclassification es obligatoria: debe ser public, internal, "
                    + "confidential o restricted (06-security.md §5)");
        }

        Instant when = in.time != null ? in.time : Instant.now();

        // Por convencion partitionkey = aggregateId, salvo que se pida otra cosa
        // explicitamente — 01-envelope.md §3.2.
        String partitionKey = in.partitionKey != null ? in.partitionKey : in.aggregateId;

        return new FluxEvent(
                Protocol.SPEC_VERSION,
                in.id,
                in.source,
                type,
                formatTime(when),
                Protocol.DATA_CONTENT_TYPE,
                in.dataSchema,
                in.aggregateId,
                in.correlationId,
                in.tenantId,
                in.producerVersion,
                in.dataClassification,
                in.causationId,
                partitionKey,
                in.traceparent,
                in.tracestate,
                // signkeyid + signature: los pone {@link Signing} DESPUES de construir el
                // evento, porque la firma cubre el envelope completo — 07-signing.md §5.
                null, null,
                null, null, null, null, null,
                data);
    }

    private static void requireNonEmpty(String name, String value) {
        if (value == null || value.isEmpty()) {
            throw new EnvelopeException(name + " es obligatorio y llego vacio. Lo rellena el SDK a partir "
                    + "de ConnectOptions; si estas llamando a buildEvent a mano, rellenalo "
                    + "(01-envelope.md §5)");
        }
    }

    /**
     * Convierte el payload a JSON y exige que sea un objeto en la raiz.
     *
     * <p>Ni array ni escalar: con un array o un escalar arriba no se pueden anadir campos
     * despues sin romper el esquema, y toda la politica de compatibilidad de
     * 05-compatibility.md se apoya en poder hacerlo — 01-envelope.md §4.
     */
    private static JsonNode toDataObject(Object data) {
        if (data == null) {
            throw new EnvelopeException("`data` es obligatorio y llego null");
        }
        JsonNode node;
        if (data instanceof JsonNode existing) {
            // Un JsonNode que ya viene del transporte se respeta tal cual: volver a
            // pasarlo por el mapper reordenaria claves y romperia la fidelidad del replay.
            node = existing;
        } else {
            try {
                node = MAPPER.valueToTree(data);
            } catch (IllegalArgumentException e) {
                throw new EnvelopeException("`data` no es serializable a JSON: " + e.getMessage(), e);
            }
        }
        if (node == null || !node.isObject()) {
            throw new EnvelopeException("`data` debe ser un objeto JSON en la raiz (ni array ni escalar), "
                    + "para poder anadir campos sin romper el contrato (01-envelope.md §4)");
        }
        return node;
    }

    // ─── Serializacion ───────────────────────────────────────────────────────

    /**
     * Convierte el evento en los bytes que viajan por NATS.
     *
     * <p>Falla aqui y no en el broker al superar 1 MiB: el broker devuelve "maximum
     * payload exceeded", que no dice que hacer al respecto — 01-envelope.md §6.
     */
    public static byte[] serialize(FluxEvent event) {
        byte[] bytes;
        try {
            bytes = MAPPER.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new EnvelopeException("el evento no es serializable a JSON: " + e.getMessage(), e);
        }
        if (bytes.length > Protocol.MAX_MESSAGE_BYTES) {
            throw new EnvelopeException("evento de " + bytes.length + " bytes supera el limite de "
                    + Protocol.MAX_MESSAGE_BYTES + " (1 MiB). Aplica claim-check: sube el contenido a "
                    + "almacenamiento de objetos y publica {uri, sha256, bytes} en su lugar "
                    + "(01-envelope.md §6). El sha256 no es decorativo: sin el, el consumidor no puede "
                    + "distinguir \"aun no replicado\" de \"fue sustituido\"");
        }
        return bytes;
    }

    /** El evento serializado como texto UTF-8. Util en tests y en logs de depuracion. */
    public static String serializeToString(FluxEvent event) {
        return new String(serialize(event), StandardCharsets.UTF_8);
    }

    // ─── Parseo ──────────────────────────────────────────────────────────────

    /**
     * Interpreta el cuerpo de un mensaje como CloudEvent de flux.
     *
     * <p>Todo fallo aqui es POISON: el mensaje ni siquiera es interpretable, asi que nunca
     * llega al handler y va a la DLQ con alerta inmediata — 04-errors.md §1.3.
     *
     * <p>El orden de las comprobaciones es el mismo que en los SDKs de Node y Go, para que
     * los tres den el mismo {@code code} ante el mismo mensaje corrupto.
     */
    public static FluxEvent parseEvent(byte[] body) {
        JsonNode root;
        try {
            root = MAPPER.readTree(body);
        } catch (Exception e) {
            throw new FluxErrors.PoisonException(
                    "el cuerpo del mensaje no es JSON valido", "MALFORMED_JSON", e);
        }
        if (root == null || root.isMissingNode()) {
            throw new FluxErrors.PoisonException(
                    "el cuerpo del mensaje esta vacio", "MALFORMED_JSON");
        }
        if (!root.isObject()) {
            throw new FluxErrors.PoisonException(
                    "el cuerpo del mensaje no es un objeto JSON", "NOT_AN_OBJECT");
        }

        JsonNode specversion = root.get("specversion");
        if (specversion == null || !specversion.isTextual()
                || !Protocol.SPEC_VERSION.equals(specversion.textValue())) {
            throw new FluxErrors.PoisonException(
                    "specversion no soportada: " + rawOrAbsent(root, "specversion")
                            + " (se esperaba \"" + Protocol.SPEC_VERSION + "\")",
                    "UNSUPPORTED_SPECVERSION");
        }

        List<String> missing = new ArrayList<>();
        for (String attribute : REQUIRED_ATTRIBUTES) {
            // hasNonNull y no has: un obligatorio a `null` cuenta como AUSENTE, porque
            // 01-envelope.md §4 prohibe usar null para "no aplica" y aceptarlo daria dos
            // formas distintas de decir "no esta" de las que solo una seria POISON.
            if (!root.hasNonNull(attribute)) {
                missing.add(attribute);
            }
        }
        if (!missing.isEmpty()) {
            throw new FluxErrors.PoisonException(
                    "faltan atributos obligatorios de CloudEvents: " + String.join(", ", missing),
                    "MISSING_REQUIRED_ATTRIBUTE");
        }

        List<String> missingExtensions = new ArrayList<>();
        for (String extension : REQUIRED_EXTENSIONS) {
            JsonNode value = root.get(extension);
            // `""` cuenta como ausente igual que `null`: 01-envelope.md §3.3 prohibe darle
            // significado propio a un valor vacio, asi que un `tenantid: ""` no es un tenant.
            if (value == null || value.isNull() || (value.isTextual() && value.textValue().isEmpty())) {
                missingExtensions.add(extension);
            }
        }
        if (!missingExtensions.isEmpty()) {
            throw new FluxErrors.PoisonException(
                    "faltan extensiones obligatorias del perfil flux: "
                            + String.join(", ", missingExtensions)
                            + ". Su ausencia no es recuperable asumiendo un valor: un "
                            + "dataclassification ausente tomado como \"internal\" haria circular PII "
                            + "con la retencion larga (01-envelope.md §3.1)",
                    "MISSING_REQUIRED_EXTENSION");
        }

        // Un valor fuera del enum no se puede degradar a algo seguro: la retencion y las ACLs
        // de 06-security.md §5 se derivan de el, asi que inventarle un sentido es peor que
        // rechazar el mensaje. Se comprueba sobre el JsonNode y no se deja al @JsonCreator del
        // enum porque este llega antes y da un codigo propio, en vez del generico de
        // deserializacion que daria treeToValue.
        JsonNode classification = root.get("dataclassification");
        if (!classification.isTextual() || !VALID_CLASSIFICATIONS.contains(classification.textValue())) {
            throw new FluxErrors.PoisonException(
                    "dataclassification invalido: " + rawOrAbsent(root, "dataclassification")
                            + ". Valores permitidos: public, internal, confidential, restricted",
                    "INVALID_DATACLASSIFICATION");
        }

        // Los tipos son exactos: {"tenantid": 42} es POISON, no el tenant "42". Jackson lo
        // coacciona en silencio salvo que se le desactive (ver MAPPER), y un envelope que
        // significa cosas distintas en dos SDKs deja de ser un contrato — 01-envelope.md §2.4.
        for (String attribute : STRING_ATTRIBUTES) {
            if (!root.get(attribute).isTextual()) {
                throw new FluxErrors.PoisonException(
                        attribute + " debe ser una cadena, llego " + rawOrAbsent(root, attribute),
                        "WRONG_ATTRIBUTE_TYPE");
            }
        }

        JsonNode contentType = root.get("datacontenttype");
        if (contentType == null || !contentType.isTextual()
                || !Protocol.DATA_CONTENT_TYPE.equals(contentType.textValue())) {
            throw new FluxErrors.PoisonException(
                    "datacontenttype no soportado: " + rawOrAbsent(root, "datacontenttype"),
                    "UNSUPPORTED_CONTENT_TYPE");
        }

        // Los atributos raiz son una lista CERRADA. Se enumeran los desconocidos aqui, y
        // no se deja a FAIL_ON_UNKNOWN_PROPERTIES, porque Jackson aborta en el primero y
        // el operador necesita ver todos. Comparar las claves EXACTAS ademas es lo que
        // hace efectiva la regla case-sensitive de 01-envelope.md §2.3.
        List<String> unknown = new ArrayList<>();
        for (Iterator<String> it = root.fieldNames(); it.hasNext(); ) {
            String name = it.next();
            if (!ALLOWED_ROOT_ATTRIBUTES.contains(name)) {
                unknown.add(name);
            }
        }
        if (!unknown.isEmpty()) {
            Collections.sort(unknown); // mensaje determinista
            throw new FluxErrors.PoisonException(
                    "atributos raiz desconocidos: " + String.join(", ", unknown)
                            + ". Todo metadato adicional va dentro de `data`",
                    "UNKNOWN_ROOT_ATTRIBUTE");
        }

        try {
            return MAPPER.treeToValue(root, FluxEvent.class);
        } catch (Exception e) {
            // Llegar aqui significa que un atributo conocido tiene el tipo equivocado
            // (p. ej. `"dlqattempts": "seis"`) o un enum fuera de rango. Sigue siendo un
            // mensaje ininterpretable.
            throw new FluxErrors.PoisonException(
                    "un atributo del envelope tiene un tipo incompatible con el contrato: " + e.getMessage(),
                    "INVALID_ATTRIBUTE_TYPE", e);
        }
    }

    /** El JSON crudo del atributo, para que el operador vea que llego y no una interpretacion. */
    private static String rawOrAbsent(JsonNode root, String field) {
        JsonNode node = root.get(field);
        return node == null ? "<ausente>" : node.toString();
    }

    /**
     * Decodifica el payload del evento en el tipo de la aplicacion.
     *
     * <p>{@code data} se guarda sin interpretar para preservar la fidelidad del replay,
     * asi que este es el punto donde el evento se convierte en algo tipado:
     *
     * <pre>{@code
     * record PedidoCreado(String pedidoId, int aggregateVersion, long totalCents) {}
     * PedidoCreado p = Envelope.dataAs(evento, PedidoCreado.class);
     * }</pre>
     *
     * <p>Un fallo aqui NO es POISON: el envelope era correcto y el mensaje llego al
     * handler. Es un desajuste de contrato entre productor y consumidor, es decir
     * PERMANENT — por eso lanza {@link FluxErrors.PermanentException} y no
     * {@link FluxErrors.PoisonException} (04-errors.md §1.2).
     */
    public static <T> T dataAs(FluxEvent event, Class<T> type) {
        try {
            return MAPPER.treeToValue(event.data(), type);
        } catch (Exception e) {
            throw dataMismatch(event, e);
        }
    }

    /** Variante para tipos genericos: {@code new TypeReference<Map<String, Object>>() {}}. */
    public static <T> T dataAs(FluxEvent event, TypeReference<T> type) {
        try {
            return MAPPER.readValue(MAPPER.treeAsTokens(event.data()), type);
        } catch (Exception e) {
            throw dataMismatch(event, e);
        }
    }

    private static FluxErrors.PermanentException dataMismatch(FluxEvent event, Exception cause) {
        return new FluxErrors.PermanentException(
                "el payload del evento " + event.id() + " no encaja en el tipo esperado",
                "DATA_SCHEMA_MISMATCH", cause);
    }

    // ─── DLQ ─────────────────────────────────────────────────────────────────

    /**
     * Por que y como un evento acabo en la DLQ.
     *
     * @param attempts numero de entrega en la que murio. DEBE ser >= 1: un {@code 0}
     *                 significaria "cero entregas", que es imposible, y ademas seria el
     *                 valor vacio que 01-envelope.md §3.3 prohibe usar con significado.
     */
    public record DlqInfo(FluxEvent.DlqReason reason, int attempts, String consumer, String error) {
        public DlqInfo {
            if (reason == null) {
                throw new IllegalArgumentException("dlqreason es obligatoria");
            }
            if (attempts < 1) {
                throw new IllegalArgumentException("dlqattempts debe ser >= 1: es el numero de entrega en "
                        + "la que el evento murio, y siempre hubo al menos una (04-errors.md §3)");
            }
        }
    }

    /**
     * Prepara un evento para la DLQ: el CloudEvent original INTEGRO mas las extensiones
     * {@code dlq*}. Sin wrapper.
     *
     * <p>Sin wrapper porque asi reproducir un mensaje de la DLQ es borrar las extensiones
     * {@code dlq*} y republicar ({@code jq 'del(.dlq*)'}). Con un wrapper habria que
     * desenvolver, y cada consumidor de la DLQ tendria que conocer dos formatos
     * — 04-errors.md §3.
     *
     * <p>El evento original nunca se muta: {@link FluxEvent} es un record inmutable y esto
     * devuelve una copia.
     */
    public static FluxEvent toDlqEvent(FluxEvent event, DlqInfo info) {
        return event.withDlq(
                info.reason(),
                info.attempts(),
                info.consumer(),
                truncate(info.error(), DLQ_ERROR_MAX_CHARS),
                formatTime(Instant.now()));
    }

    /**
     * Quita las extensiones {@code dlq*} para reproducir un evento.
     *
     * <p>El {@code id} original se CONSERVA. Regenerarlo rompe la idempotencia de todos
     * los consumidores aguas abajo y convierte una recuperacion en un incidente nuevo
     * — 04-errors.md §4.1.
     */
    public static FluxEvent stripDlqExtensions(FluxEvent event) {
        return event.withoutDlq();
    }

    /**
     * Recorta sin partir un par suplente.
     *
     * <p>El SDK de Node hace {@code .slice(0, 1024)} sobre unidades UTF-16 y puede partir
     * un emoji por la mitad; Go corta por bytes respetando runas. Aqui se corta por
     * unidades UTF-16 —lo natural en Java— pero retrocediendo una posicion si el corte
     * cae entre los dos suplentes de un caracter, porque un {@code dlqerror} con un
     * suplente huerfano produce un JSON con U+FFFD al reparsear.
     */
    static String truncate(String value, int maxChars) {
        if (value == null || value.length() <= maxChars) {
            return value;
        }
        int cut = maxChars;
        if (Character.isHighSurrogate(value.charAt(cut - 1))) {
            cut--;
        }
        return value.substring(0, cut);
    }
}
