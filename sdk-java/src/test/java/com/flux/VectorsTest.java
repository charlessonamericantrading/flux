/*
 * Los mismos vectores que ejecuta el runner cruzado (`node conformance/cross-sdk.mjs`), en proceso.
 *
 * `conformance/harness/vectors.json` es la fuente: este test NO copia ninguno. Cada vector dice que
 * espera —`sha256` (los bytes de la spec), `expectSubject`, `expect` (el codigo estable del rechazo) o
 * `differsFrom` (control)—; lo que no lleva ninguna de esas cosas lo fija el SDK de referencia (Node),
 * y eso solo lo puede comparar el runner cruzado. Aqui se comprueba todo lo que NO depende de Node:
 * lo que dice la spec. Con esto `mvn test` solo ya caza una regresion de bytes o de codigos, sin
 * esperar a la integracion.
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class VectorsTest {

    private static final JsonNode VECTORS = Repo.json("conformance/harness/vectors.json");

    private static String sha256(String base64) throws Exception {
        byte[] bytes = java.util.Base64.getDecoder().decode(base64);
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    /** La entrada del arnes: `op` + `input`, y `dataTexto` inyectado como TEXTO CRUDO (como hace `ajustes.mjs`). */
    private static JsonNode entradaDe(JsonNode v) throws Exception {
        ObjectNode input = ((ObjectNode) v.get("input")).deepCopy();
        input.put("op", v.has("op") ? v.get("op").textValue() : v.get("input").path("op").asText());
        if (!v.has("dataTexto")) {
            return input;
        }
        String resto = Envelope.mapper().writeValueAsString(input);
        return Envelope.mapper().readTree(resto.substring(0, resto.length() - 1) + ",\"data\":"
                + v.get("dataTexto").textValue() + "}");
    }

    /** Comprueba un vector de estado, de rpc o de ajustes; devuelve los bytes si los hay. */
    private static String comprobar(JsonNode v, Map<String, String> bytesPorId, String donde) throws Exception {
        JsonNode entrada = entradaDe(v);
        ObjectNode r = ConformanceHarness.procesar(entrada);
        String id = v.get("id").textValue();
        if (v.has("expect")) {
            assertFalse(r.get("ok").booleanValue(), donde + " " + id + ": esperaba rechazo " + v.get("expect").textValue()
                    + " y se acepto");
            assertEquals(v.get("expect").textValue(), r.get("code").textValue(), donde + " " + id);
            return null;
        }
        assertTrue(r.get("ok").booleanValue(), donde + " " + id + ": la operacion fallo: " + r);
        if (v.has("expectSubject")) {
            assertEquals(v.get("expectSubject").textValue(), r.get("subject").textValue(), donde + " " + id);
            return null;
        }
        if (v.has("expectEffectiveMs")) {
            assertEquals(v.get("expectEffectiveMs").intValue(), r.get("effectiveMs").intValue(), donde + " " + id);
            return null;
        }
        String bytes = r.get("bytes").textValue();
        bytesPorId.put(id, bytes);
        if (v.has("sha256")) {
            assertEquals(v.get("sha256").textValue(), sha256(bytes), donde + " " + id + ": no son los bytes de la spec");
            if (v.has("length")) {
                assertEquals(v.get("length").intValue(), java.util.Base64.getDecoder().decode(bytes).length,
                        donde + " " + id + ": longitud");
            }
        }
        if (v.has("differsFrom")) {
            assertNotEquals(bytesPorId.get(v.get("differsFrom").textValue()), bytes,
                    donde + " " + id + ": CONTROL: los bytes son iguales a los de " + v.get("differsFrom").textValue());
        }
        return bytes;
    }

    private static void correr(String bloque) throws Exception {
        Map<String, String> bytes = new HashMap<>();
        int n = 0;
        for (JsonNode v : VECTORS.get(bloque)) {
            comprobar(v, bytes, bloque);
            n++;
        }
        assertTrue(n > 0, bloque + " sin vectores");
    }

    @Test
    @DisplayName("los 87 vectores de estado: bytes de la spec (SHA-256), subjects y codigos de rechazo")
    void vectoresDeEstado() throws Exception {
        assertEquals(87, VECTORS.get("stateVectors").size(), "cambiaron los vectores: revisa este test");
        correr("stateVectors");
    }

    @Test
    @DisplayName("los 100 vectores de rpc: bytes de la spec (SHA-256 y longitud), operaciones y timeouts")
    void vectoresDeRpc() throws Exception {
        assertEquals(100, VECTORS.get("rpcVectors").size(), "cambiaron los vectores: revisa este test");
        correr("rpcVectors");
    }

    @Test
    @DisplayName("los 93 vectores de «Ajustes»: dlqerror en bytes, maxLength en puntos de codigo, time canonico, data canonico…")
    void vectoresDeAjustes() throws Exception {
        assertEquals(93, VECTORS.get("ajustesVectors").size(), "cambiaron los vectores: revisa este test");
        correr("ajustesVectors");
    }

    @Test
    @DisplayName("los vectores de poison: el mismo codigo que dan los demas SDKs ante el mismo mensaje corrupto")
    void vectoresDePoison() throws Exception {
        // Como el runner cruzado: se construye un envelope valido, se muta y se compara el CODIGO.
        ObjectNode base = Envelope.mapper().createObjectNode();
        base.put("op", "build");
        base.set("event", VECTORS.get("vectors").get(0).get("event"));
        ObjectNode construido = ConformanceHarness.procesar(base);
        assertTrue(construido.get("ok").booleanValue(), construido.toString());
        JsonNode valido = Envelope.mapper().readTree(java.util.Base64.getDecoder().decode(construido.get("bytes").textValue()));
        int n = 0;
        for (JsonNode v : VECTORS.get("poisonVectors")) {
            byte[] raw;
            if (v.has("rawUtf8")) {
                raw = v.get("rawUtf8").textValue().getBytes(StandardCharsets.UTF_8);
            } else {
                ObjectNode obj = ((ObjectNode) valido).deepCopy();
                if (v.has("remove")) {
                    for (JsonNode k : v.get("remove")) {
                        obj.remove(k.textValue());
                    }
                }
                if (v.has("mutate")) {
                    obj.setAll((ObjectNode) v.get("mutate"));
                }
                raw = Envelope.mapper().writeValueAsBytes(obj);
            }
            ObjectNode in = Envelope.mapper().createObjectNode();
            in.put("op", "parse");
            in.put("bytes", java.util.Base64.getEncoder().encodeToString(raw));
            ObjectNode r = ConformanceHarness.procesar(in);
            assertFalse(r.get("ok").booleanValue(), v.get("id").textValue() + ": deberia ser POISON");
            assertEquals(v.get("expect").textValue(), r.get("code").textValue(), v.get("id").textValue());
            n++;
        }
        assertEquals(12, n);
    }
}
