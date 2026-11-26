/*
 * Lo decidido en «Ajustes» (conformance/harness/README.md) que NO tiene vector en el runner cruzado,
 * porque la operacion del arnes es general (`build`, `parse`): el parseo estricto, el dominio
 * reservado, el `time` canonico y el corte de textos. Los 93 vectores de `ajustesVectors` los ejecuta
 * VectorsTest.
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class AjustesTest {

    private static final String VALIDO = "{\"specversion\":\"1.0\",\"id\":\"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55\","
            + "\"source\":\"/produccion/pedidos-api\",\"type\":\"com.flux.pedidos.pedido.creado.v1\","
            + "\"time\":\"2025-08-20T10:25:39.410Z\",\"datacontenttype\":\"application/json\","
            + "\"dataschema\":\"https://schemas.internal/pedidos/pedido/creado/1.0.0.json\","
            + "\"correlationid\":\"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55\",\"tenantid\":\"acme\","
            + "\"producerversion\":\"3.4.1\",\"dataclassification\":\"internal\",\"data\":{\"a\":1}}";

    private static String codigoAlParsear(byte[] bytes) {
        return assertThrows(FluxErrors.PoisonException.class, () -> Envelope.parseEvent(bytes)).code();
    }

    // ─── parseo estricto ─────────────────────────────────────────────────────

    @Test
    @DisplayName("parse: un BOM no es JSON (RFC 8259 §8.1) → MALFORMED_JSON")
    void bomEsMalformedJson() {
        byte[] json = VALIDO.getBytes(StandardCharsets.UTF_8);
        byte[] conBom = new byte[json.length + 3];
        conBom[0] = (byte) 0xEF;
        conBom[1] = (byte) 0xBB;
        conBom[2] = (byte) 0xBF;
        System.arraycopy(json, 0, conBom, 3, json.length);
        assertEquals("MALFORMED_JSON", codigoAlParsear(conBom));
        // Y sin BOM, el mismo mensaje se acepta: es lo unico que cambia.
        assertEquals("1.0", Envelope.parseEvent(json).specversion());
    }

    @Test
    @DisplayName("parse: un byte que no es UTF-8 → MALFORMED_JSON, no U+FFFD")
    void utf8InvalidoEsMalformedJson() {
        byte[] json = VALIDO.replace("acme", "acéme").getBytes(StandardCharsets.ISO_8859_1);
        assertEquals("MALFORMED_JSON", codigoAlParsear(json));
        // UTF-16 con BOM tampoco: Jackson lo detectaria solo y daria un veredicto distinto al de los demas SDKs.
        assertEquals("MALFORMED_JSON", codigoAlParsear(VALIDO.getBytes(StandardCharsets.UTF_16)));
    }

    static Stream<Arguments> tiposDeLoOpcional() {
        return Stream.of(
                Arguments.of("\"subject\":5", "\"data\""),
                Arguments.of("\"causationid\":7", "\"data\""),
                Arguments.of("\"partitionkey\":true", "\"data\""),
                Arguments.of("\"dlqattempts\":\"1\"", "\"data\""),
                Arguments.of("\"dlqattempts\":1.5", "\"data\""),
                Arguments.of("\"dlqattempts\":-1", "\"data\""),
                Arguments.of("\"dlqreason\":\"otra\"", "\"data\""),
                Arguments.of("\"traceparent\":{}", "\"data\""));
    }

    @ParameterizedTest(name = "{0} → INVALID_ATTRIBUTE_TYPE")
    @MethodSource("tiposDeLoOpcional")
    @DisplayName("parse: los tipos son exactos TAMBIEN en lo no obligatorio")
    void tiposExactosEnLoOpcional(String atributo, String antesDe) {
        String json = VALIDO.replace(antesDe, atributo + "," + antesDe);
        assertEquals("INVALID_ATTRIBUTE_TYPE", codigoAlParsear(json.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("parse: un opcional a null es «ausente», como en Node y Rust")
    void opcionalNullEsAusente() {
        FluxEvent e = Envelope.parseEvent(VALIDO.replace("\"data\"", "\"subject\":null,\"dlqattempts\":null,\"data\"")
                .getBytes(StandardCharsets.UTF_8));
        assertNull(e.aggregateId());
        assertNull(e.dlqattempts());
    }

    // ─── dominios reservados ─────────────────────────────────────────────────

    @ParameterizedTest(name = "{0}")
    @MethodSource("dominiosReservados")
    @DisplayName("un subject de un dominio reservado se rechaza antes de la forma: RESERVED_DOMAIN")
    void dominioReservado(String subject) {
        ReservedDomainException e = assertThrows(ReservedDomainException.class, () -> Protocol.parseSubject(subject));
        assertEquals("RESERVED_DOMAIN", e.code());
        assertEquals(ErrorClass.PERMANENT, e.errorClass());
        // `build` (un constructor puro) tampoco escribe en `rpc.`, `state.`, `dlq.` ni `system.`.
        assertThrows(ReservedDomainException.class, () -> new Envelope.BuildEventInput().subject(subject).build());
    }

    static Stream<String> dominiosReservados() {
        return Stream.of("rpc.pedido.v1.creado", "state.pedido.v1.creado", "dlq.pedidos.pedido.v1.creado",
                "system.pedido.v1.creado", "rpc", "state");
    }

    @Test
    @DisplayName("State.cliente.v1.x (mayuscula) NO es un dominio reservado: es un subject mal formado")
    void mayusculaNoEsReservado() {
        assertThrows(Protocol.InvalidSubjectException.class, () -> Protocol.parseSubject("State.cliente.v1.creado"));
        // La coincidencia con un dominio reservado es EXACTA: `states` no es `state` y es un dominio como cualquiera.
        assertEquals("states", Protocol.parseSubject("states.cliente.v1.creado").domain());
        // `dlq` solo es reservado como PRIMER token: como agregado es un nombre cualquiera.
        assertEquals("com.flux.pedidos.dlq.creado.v1", Protocol.subjectToType("pedidos.dlq.v1.creado"));
    }

    // ─── time canonico ───────────────────────────────────────────────────────

    static Stream<Arguments> instantes() {
        return Stream.of(
                Arguments.of("2026-08-20T10:41:07.318Z", "2026-08-20T10:41:07.318Z"),
                Arguments.of("2026-08-20T10:41:07Z", "2026-08-20T10:41:07.000Z"),
                Arguments.of("2026-08-20T10:41:07.3Z", "2026-08-20T10:41:07.300Z"),
                Arguments.of("2026-08-20T10:41:07.3189Z", "2026-08-20T10:41:07.318Z"),
                Arguments.of("2026-08-20T10:41:07.412345Z", "2026-08-20T10:41:07.412Z"),
                Arguments.of("2026-08-20T12:41:07.318+02:00", "2026-08-20T10:41:07.318Z"),
                Arguments.of("2026-08-20T06:11:07.318-04:30", "2026-08-20T10:41:07.318Z"),
                Arguments.of("2026-08-20t10:41:07.318z", "2026-08-20T10:41:07.318Z"),
                Arguments.of("2026-08-20 10:41:07.318Z", "2026-08-20T10:41:07.318Z"),
                Arguments.of("2024-02-29T23:59:59.999Z", "2024-02-29T23:59:59.999Z"),
                Arguments.of("0000-01-01T00:00:00.000Z", "0000-01-01T00:00:00.000Z"),
                Arguments.of("9999-12-31T23:59:59.999Z", "9999-12-31T23:59:59.999Z"),
                // Rechazos
                Arguments.of("ayer", null),
                Arguments.of("2026-13-20T10:41:07Z", null),
                Arguments.of("2026-02-30T10:41:07Z", null),
                Arguments.of("2025-02-29T10:41:07Z", null),
                Arguments.of("2026-08-20T10:41:60Z", null),
                Arguments.of("2026-08-20T24:00:00Z", null),
                Arguments.of("2026-08-20T10:41:07", null),
                Arguments.of("2026-08-20T10:41:07+24:00", null),
                Arguments.of("2026-08-20T10:41:07+02:60", null),
                Arguments.of("0000-01-01T00:00:00+01:00", null),
                Arguments.of("9999-12-31T23:59:59-01:00", null),
                Arguments.of("2026-08-20T10:41:07.318Z\n", null),
                Arguments.of("2026-08-20T10:41:07.318Z ", null),
                Arguments.of("٢٠٢٦-08-20T10:41:07Z", null),
                Arguments.of("", null));
    }

    @ParameterizedTest(name = "{0} → {1}")
    @MethodSource("instantes")
    @DisplayName("time: se normaliza a 3 decimales y Z (UTC, truncando) o se rechaza")
    void timeCanonico(String entrada, String esperado) {
        assertEquals(esperado, CanonicalTime.canonical(entrada));
    }

    @Test
    @DisplayName("time: null no es un instante")
    void timeNull() {
        assertNull(CanonicalTime.canonical(null));
    }

    // ─── textos ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("textos: puntos de codigo (maxLength) y bytes UTF-8 (dlqerror) son medidas distintas")
    void textos() {
        String emojis = "🚀".repeat(1000);
        assertEquals(2000, emojis.length(), "UTF-16");
        assertEquals(1000, Text.codePointLength(emojis), "mil emojis son mil caracteres para JSON Schema");
        assertEquals(emojis, Text.truncateCodePoints(emojis, 1000));
        // 1001 emojis: 999 + «…», sin partir ningun par suplente.
        String corto = Text.truncateCodePoints("🚀".repeat(1001), 1000);
        assertEquals(1000, Text.codePointLength(corto));
        assertTrue(corto.endsWith("…"));
        assertFalse(corto.contains("�"));
        // Un caracter de 3 bytes en el borde (€): no cabe medio.
        String euros = "x".repeat(1022) + "€";
        assertEquals("x".repeat(1022), Text.truncateUtf8Bytes(euros, 1024));
        assertEquals(euros, Text.truncateUtf8Bytes(euros, 1025));
        assertTrue(Text.isBlank(" \t 　"));
        assertFalse(Text.isBlank(" a "));
    }
}
