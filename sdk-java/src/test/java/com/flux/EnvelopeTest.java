/*
 * Envelope: construccion, serializacion, parseo y DLQ.
 * Contrato normativo: specification/01-envelope.md
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EnvelopeTest {

    private static final ObjectMapper PLAIN = new ObjectMapper();

    /** El envelope de referencia de 01-envelope.md §4, en una sola linea. */
    private static final String ENVELOPE_VALIDO = """
            {"specversion":"1.0","id":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",\
            "source":"/produccion/pedidos-api","type":"com.flux.pedidos.pedido.creado.v1",\
            "time":"2026-08-20T10:25:39.412Z","datacontenttype":"application/json",\
            "dataschema":"https://schemas.internal/pedidos/pedido/creado/1.2.0.json",\
            "subject":"ped-123","correlationid":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",\
            "tenantid":"acme","producerversion":"3.4.1","dataclassification":"confidential",\
            "data":{"pedidoId":"ped-123","totalCents":9990}}""";

    private static Envelope.BuildEventInput entradaValida() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("pedidoId", "ped-123");
        data.put("totalCents", 9990);
        return new Envelope.BuildEventInput()
                .subject("pedidos.pedido.v1.creado")
                .data(data)
                .id("01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55")
                .source("/produccion/pedidos-api")
                .producerVersion("3.4.1")
                .tenantId("acme")
                .dataClassification(FluxEvent.DataClassification.CONFIDENTIAL)
                .dataSchema("https://schemas.internal/pedidos/pedido/creado/1.2.0.json")
                .correlationId("01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55");
    }

    // ─── time ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("formatTime emite SIEMPRE tres decimales y sufijo Z")
    void formatTimeSiempreTresDecimales() {
        // ISO_INSTANT recortaria los ceros finales y el envelope dejaria de ser byte a byte
        // igual al de Node, Python y Go — 01-envelope.md §2.2.
        assertEquals("2026-08-20T10:25:39.410Z",
                Envelope.formatTime(Instant.parse("2026-08-20T10:25:39.410Z")));
        assertEquals("2026-08-20T10:25:39.400Z",
                Envelope.formatTime(Instant.parse("2026-08-20T10:25:39.400Z")));
        assertEquals("2026-08-20T10:25:39.000Z",
                Envelope.formatTime(Instant.parse("2026-08-20T10:25:39Z")));

        // Un instante con offset se normaliza a UTC.
        Instant conOffset = ZonedDateTime.of(2026, 8, 20, 12, 25, 39, 412_000_000,
                ZoneOffset.ofHours(2)).toInstant();
        assertEquals("2026-08-20T10:25:39.412Z", Envelope.formatTime(conOffset));

        // Los nanosegundos por debajo del milisegundo se truncan, no se emiten.
        assertEquals("2026-08-20T10:25:39.410Z",
                Envelope.formatTime(Instant.parse("2026-08-20T10:25:39.410999Z")));
    }

    @Test
    @DisplayName("el time del evento cumple el patron exacto del protocolo")
    void timeDelEventoCumpleElPatron() {
        FluxEvent event = entradaValida().build();
        assertTrue(event.time().matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$"),
                "time = " + event.time());
        assertNotNull(event.eventTime());
    }

    // ─── buildEvent ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("buildEvent rellena todo lo derivable y no pide nada de eso al desarrollador")
    void buildEventRellenaLoDerivable() {
        FluxEvent event = entradaValida().aggregateId("ped-123").build();

        assertEquals("1.0", event.specversion());
        assertEquals("com.flux.pedidos.pedido.creado.v1", event.type());
        assertEquals("application/json", event.datacontenttype());
        assertEquals("ped-123", event.aggregateId());
        // Por convencion partitionkey = aggregateId — 01-envelope.md §3.2.
        assertEquals("ped-123", event.partitionkey());
        assertEquals(FluxEvent.DataClassification.CONFIDENTIAL, event.dataclassification());
    }

    @Test
    @DisplayName("`data` debe ser un objeto JSON en la raiz")
    void dataDebeSerObjetoEnLaRaiz() {
        // Con un array o un escalar arriba no se pueden anadir campos despues sin romper el
        // esquema, y toda la politica de compatibilidad se apoya en poder hacerlo
        // — 01-envelope.md §4.
        assertThrows(Envelope.EnvelopeException.class, () -> entradaValida().data(List.of(1, 2)).build());
        assertThrows(Envelope.EnvelopeException.class, () -> entradaValida().data("texto").build());
        assertThrows(Envelope.EnvelopeException.class, () -> entradaValida().data(42).build());
        assertThrows(Envelope.EnvelopeException.class, () -> entradaValida().data(null).build());
    }

    @Test
    @DisplayName("las extensiones obligatorias se exigen en tiempo de ejecucion")
    void extensionesObligatorias() {
        assertThrows(Envelope.EnvelopeException.class, () -> entradaValida().tenantId(null).build());
        assertThrows(Envelope.EnvelopeException.class, () -> entradaValida().tenantId("").build());
        assertThrows(Envelope.EnvelopeException.class, () -> entradaValida().producerVersion(null).build());
        assertThrows(Envelope.EnvelopeException.class, () -> entradaValida().correlationId(null).build());
        assertThrows(Envelope.EnvelopeException.class, () -> entradaValida().dataSchema(null).build());
        assertThrows(Envelope.EnvelopeException.class, () -> entradaValida().id(null).build());
        assertThrows(Envelope.EnvelopeException.class,
                () -> entradaValida().dataClassification(null).build());
    }

    @Test
    @DisplayName("un subject invalido falla antes que cualquier otra validacion")
    void subjectInvalidoFallaPrimero() {
        assertThrows(Protocol.InvalidSubjectException.class,
                () -> entradaValida().subject("Pedidos.pedido.v1.creado").tenantId(null).build());
    }

    // ─── serializacion ───────────────────────────────────────────────────────

    @Test
    @DisplayName("se serializan los nombres de extension de CloudEvents, no los de Java")
    void serializeUsaLosNombresDeCloudEvents() throws Exception {
        FluxEvent event = entradaValida()
                .aggregateId("ped-123")
                .causationId("01924f8d-1a2b-7c3d-8e4f-5a6b7c8d9e01")
                .traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .build();

        JsonNode raw = PLAIN.readTree(Envelope.serialize(event));

        // CloudEvents solo admite [a-z0-9] en nombres de extension: minuscula pegada.
        for (String key : List.of("correlationid", "tenantid", "producerversion",
                "dataclassification", "causationid", "partitionkey", "traceparent")) {
            assertTrue(raw.has(key), "falta la extension " + key + " en el JSON serializado");
        }

        // El aggregateId se serializa como `subject`, no como `aggregateid`.
        assertFalse(raw.has("aggregateid"), "el aggregateId no debe serializarse como `aggregateid`");
        assertEquals("ped-123", raw.get("subject").textValue());

        // Todo atributo emitido debe estar en la lista cerrada. Este bucle no es teorico:
        // caza que un metodo `isDlq()` del record se convierta en el atributo `"dlq": false`
        // por la convencion de beans de Jackson — ver FluxEvent.isDlq().
        for (Iterator<String> it = raw.fieldNames(); it.hasNext(); ) {
            String name = it.next();
            assertTrue(Envelope.ALLOWED_ROOT_ATTRIBUTES.contains(name),
                    "el SDK emitio un atributo raiz no permitido: " + name);
        }
        assertFalse(raw.has("dlq"), "los metodos de conveniencia del record no son atributos");
    }

    @Test
    @DisplayName("ausente != vacio: los opcionales se omiten, no salen como null ni como 0")
    void ausenteNoEsVacio() throws Exception {
        // 01-envelope.md §3.3. En Java el riesgo concreto es dlqattempts como int primitivo:
        // valdria 0 y se emitiria como 0, colapsando cero y ausente.
        FluxEvent event = entradaValida().build();
        JsonNode raw = PLAIN.readTree(Envelope.serialize(event));

        for (String opcional : List.of("subject", "causationid", "partitionkey", "traceparent",
                "tracestate", "dlqreason", "dlqattempts", "dlqconsumer", "dlqerror", "dlqtime")) {
            assertFalse(raw.has(opcional), opcional + " deberia estar AUSENTE, no presente y vacio");
        }
        for (Iterator<JsonNode> it = raw.elements(); it.hasNext(); ) {
            assertFalse(it.next().isNull(), "ningun atributo del envelope puede valer null");
        }
        assertNull(event.dlqattempts(), "dlqattempts debe ser Integer nulo, no int a 0");
    }

    @Test
    @DisplayName("no se escapan <, > ni & (paridad byte a byte con Node y Go)")
    void serializeNoEscapaHtml() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nota", "a<b & c>d");
        String json = Envelope.serializeToString(entradaValida().data(data).build());
        assertTrue(json.contains("\"a<b & c>d\""), json);
        assertFalse(json.contains("\\u003c"), "Go tiene que desactivar el escapado de HTML; "
                + "Jackson no lo hace por defecto y aqui se comprueba que sigue siendo asi");
    }

    @Test
    @DisplayName("no se escapan los caracteres no ASCII")
    void serializeNoEscapaNoAscii() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nota", "envío ñ €");
        String json = Envelope.serializeToString(entradaValida().data(data).build());
        assertTrue(json.contains("envío ñ €"), json);
    }

    @Test
    @DisplayName("serialize rechaza mas de 1 MiB con un error accionable")
    void serializeRechazaMasDeUnMiB() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("blob", "x".repeat(Protocol.MAX_MESSAGE_BYTES));
        Envelope.EnvelopeException e = assertThrows(Envelope.EnvelopeException.class,
                () -> Envelope.serialize(entradaValida().data(data).build()));
        // El broker diria "maximum payload exceeded", que no dice que hacer
        // — 01-envelope.md §6.
        assertTrue(e.getMessage().contains("claim-check"), e.getMessage());
        assertTrue(e.getMessage().contains("sha256"), e.getMessage());
    }

    @Test
    @DisplayName("fixture cross-SDK: el JSON es byte a byte el de Node, Python y Go")
    void fixtureCrossSdk() {
        // conformance/cases/cross-sdk-envelope.json. Un protocolo polyglot cuyo envelope no
        // es reproducible entre lenguajes no es un protocolo: es una convencion informal.
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("pedidoId", "ped-123");
        data.put("aggregateVersion", 1);
        data.put("totalCents", 9990);
        data.put("moneda", "EUR");

        FluxEvent event = new Envelope.BuildEventInput()
                .subject("pedidos.pedido.v1.creado")
                .data(data)
                .id("01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55")
                .source("/produccion/pedidos-api")
                .time(Instant.ofEpochMilli(1755685539410L))
                .aggregateId("ped-123")
                .dataSchema("https://schemas.internal/pedidos/pedido/creado/1.0.0.json")
                .correlationId("01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55")
                .tenantId("acme")
                .producerVersion("3.4.1")
                .dataClassification(FluxEvent.DataClassification.CONFIDENTIAL)
                .build();

        String esperado = """
                {"specversion":"1.0","id":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",\
                "source":"/produccion/pedidos-api","type":"com.flux.pedidos.pedido.creado.v1",\
                "time":"2025-08-20T10:25:39.410Z","datacontenttype":"application/json",\
                "dataschema":"https://schemas.internal/pedidos/pedido/creado/1.0.0.json",\
                "subject":"ped-123","correlationid":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",\
                "tenantid":"acme","producerversion":"3.4.1","dataclassification":"confidential",\
                "partitionkey":"ped-123","data":{"pedidoId":"ped-123","aggregateVersion":1,\
                "totalCents":9990,"moneda":"EUR"}}""";

        assertEquals(esperado, Envelope.serializeToString(event));
    }

    // ─── parseo ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("parseEvent interpreta un envelope valido")
    void parseEventValido() {
        FluxEvent event = Envelope.parseEvent(ENVELOPE_VALIDO.getBytes(StandardCharsets.UTF_8));
        assertEquals("01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55", event.id());
        assertEquals("ped-123", event.aggregateId());
        assertEquals(FluxEvent.DataClassification.CONFIDENTIAL, event.dataclassification());
        assertEquals("ped-123", event.data().get("pedidoId").textValue());
        assertEquals(9990, event.data().get("totalCents").intValue());
        assertFalse(event.isDlq());
        assertEquals("pedidos.pedido.v1.creado", event.parsedType().toSubject());
    }

    @Test
    @DisplayName("todo fallo de parseo es POISON, con el mismo code que Node y Go")
    void parseEventDetectaPoison() {
        record Caso(String descripcion, String cuerpo, String code) {
        }
        List<Caso> casos = List.of(
                new Caso("JSON malformado", "{no es json", "MALFORMED_JSON"),
                new Caso("cuerpo vacio", "", "MALFORMED_JSON"),
                new Caso("basura despues del objeto", ENVELOPE_VALIDO + " basura", "MALFORMED_JSON"),
                new Caso("array en la raiz", "[1,2,3]", "NOT_AN_OBJECT"),
                new Caso("escalar en la raiz", "\"hola\"", "NOT_AN_OBJECT"),
                new Caso("specversion desconocida",
                        ENVELOPE_VALIDO.replace("\"specversion\":\"1.0\"", "\"specversion\":\"0.3\""),
                        "UNSUPPORTED_SPECVERSION"),
                new Caso("falta un atributo obligatorio",
                        ENVELOPE_VALIDO.replace("\"dataschema\":", "\"dataschemaX\":")
                                .replace("\"dataschemaX\":\"https://schemas.internal/pedidos/pedido/creado/1.2.0.json\",", ""),
                        "MISSING_REQUIRED_ATTRIBUTE"),
                new Caso("datacontenttype no soportado",
                        ENVELOPE_VALIDO.replace("application/json", "application/xml"),
                        "UNSUPPORTED_CONTENT_TYPE"),
                new Caso("atributo raiz desconocido",
                        ENVELOPE_VALIDO.replace("\"data\":{", "\"metadatos\":{\"a\":1},\"data\":{"),
                        "UNKNOWN_ROOT_ATTRIBUTE"),
                new Caso("un atributo de texto llega como numero",
                        // Sin la config de coercion, Jackson lo aceptaria como la cadena
                        // "42" y el mismo cuerpo seria POISON en Go: divergencia silenciosa.
                        ENVELOPE_VALIDO.replace("\"tenantid\":\"acme\"", "\"tenantid\":42"),
                        "WRONG_ATTRIBUTE_TYPE"),
                new Caso("dlqattempts no numerico",
                        ENVELOPE_VALIDO.replace("\"data\":{", "\"dlqattempts\":\"seis\",\"data\":{"),
                        "INVALID_ATTRIBUTE_TYPE"),
                new Caso("dataclassification fuera del enum",
                        ENVELOPE_VALIDO.replace("\"confidential\"", "\"secretisimo\""),
                        "INVALID_DATACLASSIFICATION"),
                new Caso("falta una extension obligatoria del perfil flux",
                        ENVELOPE_VALIDO.replace("\"tenantid\":\"acme\",", ""),
                        "MISSING_REQUIRED_EXTENSION"),
                new Caso("un atributo obligatorio del nucleo a null cuenta como ausente",
                        // 01-envelope.md §4 prohibe usar null para "no aplica": aceptarlo
                        // daria dos formas de decir "no esta" y solo una seria POISON.
                        ENVELOPE_VALIDO.replace("\"source\":\"/produccion/pedidos-api\"",
                                "\"source\":null"),
                        "MISSING_REQUIRED_ATTRIBUTE"));

        for (Caso caso : casos) {
            FluxErrors.PoisonException e = assertThrows(FluxErrors.PoisonException.class,
                    () -> Envelope.parseEvent(caso.cuerpo().getBytes(StandardCharsets.UTF_8)),
                    caso.descripcion());
            assertEquals(caso.code(), e.code(), caso.descripcion() + " → " + e.getMessage());
            assertEquals(ErrorClass.POISON, e.errorClass());
        }
    }

    @Test
    @DisplayName("falta una extension obligatoria del perfil flux → POISON")
    void faltaUnaExtensionObligatoria() throws Exception {
        // La spec las llamaba obligatorias y ningun parser las exigia. Asumir un default es
        // peligroso en las cuatro: un dataclassification ausente tomado como "internal" haria
        // circular PII con 30 dias de retencion en vez de 7, y un tenantid ausente tomado como
        // "system" cruzaria fronteras de tenant — 01-envelope.md §3.1.
        for (String extension : List.of("correlationid", "tenantid", "producerversion",
                "dataclassification")) {
            // Ausente, `null` y `""` son el mismo caso: ausente != vacio va en las dos
            // direcciones, o el envelope tendria tres formas de decir "no lo se" y solo una
            // seria POISON — 01-envelope.md §3.3.
            for (JsonNode valor : Arrays.asList(null, NullNode.getInstance(), TextNode.valueOf(""))) {
                ObjectNode raw = (ObjectNode) PLAIN.readTree(ENVELOPE_VALIDO);
                if (valor == null) {
                    raw.remove(extension);
                } else {
                    raw.set(extension, valor);
                }
                byte[] cuerpo = PLAIN.writeValueAsBytes(raw);
                String caso = extension + " = " + valor;

                FluxErrors.PoisonException e = assertThrows(FluxErrors.PoisonException.class,
                        () -> Envelope.parseEvent(cuerpo), caso);
                assertEquals("MISSING_REQUIRED_EXTENSION", e.code(), caso);
                assertEquals(ErrorClass.POISON, e.errorClass(), caso);
                assertTrue(e.getMessage().contains(extension), e.getMessage());
            }
        }
    }

    @Test
    @DisplayName("dataclassification fuera del enum → POISON con codigo propio")
    void dataclassificationFueraDelEnum() {
        // La retencion y las ACLs se derivan de este valor, asi que un valor desconocido no se
        // puede degradar a nada seguro — 01-envelope.md §3.1 y 06-security.md §5. El
        // @JsonCreator del enum ya lanzaria, pero con el codigo generico de deserializacion:
        // el contrato pide este.
        // "Confidential" esta en la lista porque la comparacion es case-sensitive
        // (01-envelope.md §2.3), y los dos ultimos porque un valor que ni siquiera es una
        // cadena tiene que dar ESTE codigo y no el generico de desajuste de tipo.
        for (String valor : List.of("\"secreto\"", "\"Confidential\"", "42", "{\"a\":1}")) {
            String cuerpo = ENVELOPE_VALIDO.replace("\"confidential\"", valor);
            FluxErrors.PoisonException e = assertThrows(FluxErrors.PoisonException.class,
                    () -> Envelope.parseEvent(cuerpo.getBytes(StandardCharsets.UTF_8)), valor);
            assertEquals("INVALID_DATACLASSIFICATION", e.code(), valor);
            assertEquals(ErrorClass.POISON, e.errorClass(), valor);
        }
    }

    @Test
    @DisplayName("un tipo coaccionable → POISON, no la coercion silenciosa de Jackson")
    void tiposExactosSinCoercion() throws Exception {
        // Jackson convierte {"tenantid": 42} en "42" sin avisar: el mensaje se aceptaria con un
        // valor que el productor nunca escribio, y el mismo cuerpo seria POISON en Go, que no
        // coacciona escalares a string. Un envelope que significa cosas distintas en dos SDKs
        // deja de ser un contrato — 01-envelope.md §2.4.
        for (String attribute : List.of("id", "source", "type", "time", "correlationid",
                "tenantid", "producerversion")) {
            ObjectNode raw = (ObjectNode) PLAIN.readTree(ENVELOPE_VALIDO);
            raw.put(attribute, 42);
            byte[] cuerpo = PLAIN.writeValueAsBytes(raw);

            FluxErrors.PoisonException e = assertThrows(FluxErrors.PoisonException.class,
                    () -> Envelope.parseEvent(cuerpo), attribute);
            assertEquals("WRONG_ATTRIBUTE_TYPE", e.code(), attribute);
            assertTrue(e.getMessage().contains(attribute), e.getMessage());
        }

        // Y el mapper tampoco coacciona por su cuenta: la comprobacion sobre el JsonNode y la
        // configuracion de coercion se cubren la una a la otra, porque cualquiera de las dos
        // podria retirarse por descuido dejando el agujero abierto.
        assertThrows(Exception.class, () -> Envelope.mapper().readValue(
                ENVELOPE_VALIDO.replace("\"tenantid\":\"acme\"", "\"tenantid\":42"),
                FluxEvent.class));
    }

    @Test
    @DisplayName("el JSON viaja en UTF-8 literal, sin escapes \\uXXXX")
    void serializeEmiteUtf8Literal() {
        // Ambas formas son JSON valido y se parsean al mismo String, pero NO son los mismos
        // BYTES, y de la secuencia de bytes dependen el replay verbatim desde la DLQ, la firma
        // criptografica futura y los fixtures cross-SDK — 01-envelope.md §1.1. Jackson acierta
        // por defecto; el encoder por defecto de System.Text.Json escapa todo lo no ASCII, asi
        // que la regla es normativa y este test la fija en vez de dejarla al default.
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("nota", "café ñandú 東京 €");
        byte[] bytes = Envelope.serialize(entradaValida().data(data).build());

        assertTrue(new String(bytes, StandardCharsets.UTF_8).contains("café ñandú 東京 €"));

        // A nivel de BYTES: si Jackson hubiese escapado, la salida seria ASCII puro y la 'e'
        // acentuada apareceria como los seis caracteres "é" en vez de los dos octetos
        // 0xC3 0xA9 que le corresponden en UTF-8.
        assertFalse(new String(bytes, StandardCharsets.US_ASCII).contains("\\u"),
                "no debe salir ningun escape \\uXXXX");
        assertTrue(contieneSecuencia(bytes, "café".getBytes(StandardCharsets.UTF_8)),
                "los caracteres no ASCII deben viajar como sus octetos UTF-8 literales");
    }

    /** Busca una subsecuencia de bytes. Sin esto, la asercion recaeria en un String. */
    private static boolean contieneSecuencia(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    @Test
    @DisplayName("los nombres de atributo se comparan RESPETANDO mayusculas")
    void parseEventEsCaseSensitive() {
        // 01-envelope.md §2.3. `encoding/json` de Go empareja campos sin distinguir
        // mayusculas y Jackson lo haria tambien si alguien activase
        // ACCEPT_CASE_INSENSITIVE_PROPERTIES: `{"ID": …}` poblaria `id` en silencio, y `Id`,
        // `iD` e `ID` acabarian siendo el mismo atributo. Es el mismo fantasma que la spec
        // combate en los subjects de NATS.
        String conMayusculas = ENVELOPE_VALIDO.replace("\"id\":", "\"ID\":");
        FluxErrors.PoisonException e = assertThrows(FluxErrors.PoisonException.class,
                () -> Envelope.parseEvent(conMayusculas.getBytes(StandardCharsets.UTF_8)));
        // `id` desaparecio (falta un obligatorio) y ademas aparecio `ID` (desconocido).
        assertEquals("MISSING_REQUIRED_ATTRIBUTE", e.code());

        // Con el `id` correcto presente, un `Id` extra es un atributo raiz desconocido.
        String conDuplicado = ENVELOPE_VALIDO.replace("\"data\":{", "\"Id\":\"otro\",\"data\":{");
        FluxErrors.PoisonException dup = assertThrows(FluxErrors.PoisonException.class,
                () -> Envelope.parseEvent(conDuplicado.getBytes(StandardCharsets.UTF_8)));
        assertEquals("UNKNOWN_ROOT_ATTRIBUTE", dup.code());
        assertTrue(dup.getMessage().contains("Id"), dup.getMessage());
    }

    @Test
    @DisplayName("el mapper tiene desactivado el emparejamiento insensible a mayusculas")
    void mapperEsCaseSensitive() {
        // La lista cerrada de atributos raiz tapa el agujero, pero el envelope no debe
        // depender de esa casualidad: la propiedad se comprueba tambien en el mapper.
        assertFalse(Envelope.mapper().isEnabled(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES),
                "ACCEPT_CASE_INSENSITIVE_PROPERTIES DEBE estar desactivado (01-envelope.md §2.3)");
        assertFalse(Envelope.mapper().isEnabled(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS));

        // Y se verifica de verdad, no solo por el flag: `{"ID": …}` no puebla `id`.
        assertThrows(Exception.class, () -> Envelope.mapper()
                .readValue(ENVELOPE_VALIDO.replace("\"id\":", "\"ID\":"), FluxEvent.class));
    }

    @Test
    @DisplayName("el payload se conserva sin interpretar")
    void payloadSeConserva() {
        // Es lo que hace que el replay desde la DLQ sea "borrar dlq* y republicar": ni se
        // reordenan claves, ni se convierten enteros grandes a coma flotante, ni se
        // normalizan los ceros finales de un decimal.
        String cuerpo = ENVELOPE_VALIDO.replace(
                "\"data\":{\"pedidoId\":\"ped-123\",\"totalCents\":9990}",
                "\"data\":{\"z\":1,\"a\":2,\"grande\":9007199254740993,\"decimal\":4995.00}");

        FluxEvent event = Envelope.parseEvent(cuerpo.getBytes(StandardCharsets.UTF_8));
        String reserializado = Envelope.serializeToString(event);

        assertTrue(reserializado.contains("\"z\":1,\"a\":2"), "el orden de claves debe conservarse");
        assertTrue(reserializado.contains("9007199254740993"),
                "un entero mayor que 2^53 no debe pasar por coma flotante");
        assertTrue(reserializado.contains("4995.00"),
                "los ceros finales de un decimal deben conservarse: " + reserializado);
    }

    @Test
    @DisplayName("parse + serialize es idempotente byte a byte")
    void roundTripParseSerialize() {
        byte[] original = ENVELOPE_VALIDO.getBytes(StandardCharsets.UTF_8);
        FluxEvent event = Envelope.parseEvent(original);
        byte[] reserializado = Envelope.serialize(event);
        assertEquals(ENVELOPE_VALIDO, new String(reserializado, StandardCharsets.UTF_8));
        assertEquals(event, Envelope.parseEvent(reserializado),
                "el record debe comparar por valor, payload incluido");
    }

    // ─── dataAs ──────────────────────────────────────────────────────────────

    record PedidoCreado(String pedidoId, long totalCents) {
    }

    @Test
    @DisplayName("dataAs tipa el payload")
    void dataAsTipaElPayload() {
        FluxEvent event = Envelope.parseEvent(ENVELOPE_VALIDO.getBytes(StandardCharsets.UTF_8));
        PedidoCreado pedido = Envelope.dataAs(event, PedidoCreado.class);
        assertEquals("ped-123", pedido.pedidoId());
        assertEquals(9990L, pedido.totalCents());
    }

    record PedidoIncompatible(String pedidoId, List<String> lineas) {
    }

    @Test
    @DisplayName("un payload que no encaja es PERMANENT, no POISON")
    void dataAsIncompatibleEsPermanent() {
        // El envelope era correcto y el mensaje llego al handler: es un desajuste de
        // contrato entre productor y consumidor — 04-errors.md §1.2.
        FluxEvent event = Envelope.parseEvent(
                ENVELOPE_VALIDO.replace("\"totalCents\":9990", "\"lineas\":\"no-soy-un-array\"")
                        .getBytes(StandardCharsets.UTF_8));
        FluxErrors.PermanentException e = assertThrows(FluxErrors.PermanentException.class,
                () -> Envelope.dataAs(event, PedidoIncompatible.class));
        assertEquals("DATA_SCHEMA_MISMATCH", e.code());
        assertEquals(ErrorClass.PERMANENT, e.errorClass());
    }

    // ─── DLQ ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("el mensaje de DLQ es el original INTEGRO mas las extensiones dlq*")
    void toDlqEventConservaElOriginal() {
        FluxEvent original = Envelope.parseEvent(ENVELOPE_VALIDO.getBytes(StandardCharsets.UTF_8));
        FluxEvent dlq = Envelope.toDlqEvent(original, new Envelope.DlqInfo(
                FluxEvent.DlqReason.PERMANENT, 1, "facturacion-api__pedidos_pedido_v1_creado",
                "PEDIDO_YA_CANCELADO: el pedido ped-123 estaba cancelado"));

        // Sin wrapper: el replay es borrar dlq* y republicar — 04-errors.md §3.
        assertEquals(original.id(), dlq.id());
        assertEquals(original.time(), dlq.time());
        assertEquals(original.data(), dlq.data());
        assertEquals(FluxEvent.DlqReason.PERMANENT, dlq.dlqreason());
        assertEquals(1, dlq.dlqattempts());
        assertEquals("facturacion-api__pedidos_pedido_v1_creado", dlq.dlqconsumer());
        assertNotNull(dlq.dlqtime());
        assertTrue(dlq.isDlq());

        // El original no se muta: FluxEvent es inmutable.
        assertFalse(original.isDlq());

        // Y el replay devuelve exactamente el evento original.
        assertEquals(original, Envelope.stripDlqExtensions(dlq));
    }

    @Test
    @DisplayName("dlqattempts no admite 0: seria el valor vacio con significado que prohibe la spec")
    void dlqAttemptsMinimoUno() {
        FluxEvent original = Envelope.parseEvent(ENVELOPE_VALIDO.getBytes(StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class, () -> Envelope.toDlqEvent(original,
                new Envelope.DlqInfo(FluxEvent.DlqReason.PERMANENT, 0, "c", "e")));
    }

    @Test
    @DisplayName("dlqerror se recorta a 1024 sin partir un par suplente")
    void dlqErrorSeRecorta() {
        FluxEvent original = Envelope.parseEvent(ENVELOPE_VALIDO.getBytes(StandardCharsets.UTF_8));

        FluxEvent dlq = Envelope.toDlqEvent(original,
                new Envelope.DlqInfo(FluxEvent.DlqReason.PERMANENT, 1, "c", "x".repeat(5000)));
        assertEquals(1024, dlq.dlqerror().length());

        // 1023 'x' + un emoji (2 unidades UTF-16): el corte caeria entre los dos suplentes.
        String conEmoji = "x".repeat(1023) + "🚀";
        FluxEvent conSuplente = Envelope.toDlqEvent(original,
                new Envelope.DlqInfo(FluxEvent.DlqReason.PERMANENT, 1, "c", conEmoji));
        assertEquals(1023, conSuplente.dlqerror().length(),
                "debe retroceder una posicion antes que dejar un suplente huerfano");
        assertFalse(Character.isHighSurrogate(
                conSuplente.dlqerror().charAt(conSuplente.dlqerror().length() - 1)));

        // Y el resultado sigue siendo serializable sin U+FFFD.
        assertFalse(Envelope.serializeToString(conSuplente).contains("�"));
    }

    @Test
    @DisplayName("un evento de DLQ vuelve a parsearse y conserva las extensiones")
    void dlqRoundTrip() {
        FluxEvent original = Envelope.parseEvent(ENVELOPE_VALIDO.getBytes(StandardCharsets.UTF_8));
        FluxEvent dlq = Envelope.toDlqEvent(original,
                new Envelope.DlqInfo(FluxEvent.DlqReason.RETRYABLE, 6, "consumidor", "se agotaron"));

        FluxEvent releido = Envelope.parseEvent(Envelope.serialize(dlq));
        assertEquals(dlq, releido);
        assertEquals(FluxEvent.DlqReason.RETRYABLE, releido.dlqreason());
        assertEquals(6, releido.dlqattempts());
    }

    @Test
    @DisplayName("un JsonNode pasado como data se respeta tal cual")
    void dataJsonNodeSeRespeta() throws Exception {
        JsonNode node = PLAIN.readTree("{\"z\":1,\"a\":2}");
        FluxEvent event = entradaValida().data(node).build();
        assertSame(node, event.data());
        assertTrue(Envelope.serializeToString(event).contains("\"data\":{\"z\":1,\"a\":2}"));
    }

    @Test
    @DisplayName("un null dentro de data se omite, como pide 01-envelope.md §4")
    void nullEnDataSeOmite() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("pedidoId", "ped-123");
        data.put("descuento", null);
        String json = Envelope.serializeToString(entradaValida().data(data).build());
        assertFalse(json.contains("descuento"),
                "`data` NO DEBE contener null para significar ausente: se omite el campo");
    }

    @Test
    @DisplayName("la lista de atributos raiz permitidos es exactamente la de la spec")
    void listaCerradaDeAtributos() {
        List<String> esperados = new ArrayList<>(List.of(
                "specversion", "id", "source", "type", "time", "datacontenttype", "dataschema",
                "subject", "correlationid", "tenantid", "producerversion", "dataclassification",
                "causationid", "partitionkey", "traceparent", "tracestate",
                "dlqreason", "dlqattempts", "dlqconsumer", "dlqerror", "dlqtime", "data"));
        assertEquals(esperados.size(), Envelope.ALLOWED_ROOT_ATTRIBUTES.size());
        assertTrue(Envelope.ALLOWED_ROOT_ATTRIBUTES.containsAll(esperados));
    }
}
