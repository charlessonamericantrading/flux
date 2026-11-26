/*
 * El `id` determinista de un hecho importado — 01-envelope.md §2.6.1.
 * Los vectores son los de conformance/cases/cross-sdk-external-id.json (6 validos y 5 invalidos): no se copian.
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ExternalIdTest {

    private static final JsonNode CASO = Repo.json("conformance/cases/cross-sdk-external-id.json");
    private static final Pattern UUID_V7 = Pattern.compile(
            "^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$");

    private static String derivar(JsonNode input) {
        JsonNode k = input.get("externalKey");
        String clave = k != null && k.isObject() ? k.get("repeat").textValue().repeat(k.get("times").intValue())
                : k == null ? null : k.textValue();
        Instant t = input.has("time") ? CanonicalTime.instant(input.get("time").textValue()) : null;
        return ExternalId.eventId(input.path("source").textValue(), clave, t);
    }

    @Test
    @DisplayName("los 6 vectores validos dan el id de la spec (calculados con otra implementacion, no con esta)")
    void validos() {
        assertEquals(6, CASO.get("fixtures").size());
        for (JsonNode f : CASO.get("fixtures")) {
            assertEquals(f.get("expected").get("id").textValue(), derivar(f.get("input")), f.get("id").textValue());
        }
    }

    @Test
    @DisplayName("los 5 vectores invalidos se rechazan en vez de producir un id que no es el de los demas")
    void invalidos() {
        assertEquals(5, CASO.get("invalid").size());
        for (JsonNode f : CASO.get("invalid")) {
            assertThrows(Envelope.EnvelopeException.class, () -> derivar(f.get("input")), f.get("id").textValue());
        }
    }

    @Test
    @DisplayName("propiedades: UUIDv7 valido, determinista, 12 primeros digitos = milisegundos, inyectivo, ordenado por tiempo")
    void propiedades() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 20_000; i++) {
            String id = ExternalId.eventId("/dev/a", "evt_" + i, Instant.parse("2026-08-20T10:25:39.412Z"));
            assertTrue(UUID_V7.matcher(id).matches(), id);
            assertEquals(id, ExternalId.eventId("/dev/a", "evt_" + i, Instant.parse("2026-08-20T10:25:39.412Z")));
            ids.add(id);
        }
        assertEquals(20_000, ids.size(), "74 bits: ni una colision");
        Instant t = Instant.parse("2026-08-20T10:25:39.412Z");
        String id = ExternalId.eventId("/dev/a", "k", t);
        assertEquals(String.format("%012x", t.toEpochMilli()), id.replace("-", "").substring(0, 12));
        assertTrue(ExternalId.eventId("/dev/a", "k", t.plusMillis(1)).compareTo(id) > 0);
        assertNotEquals(id, ExternalId.eventId("/dev/b", "k", t));
        // El hash es sobre UTF-8: la clave con ñ no se confunde con la de otra codificacion.
        assertEquals("018df74f-83ff-72cc-9c87-4843a505450d", ExternalId.eventId("/dev/tienda-web",
                "pedido-ñandú-ß-日本", Instant.parse("2024-02-29T23:59:59.999Z")));
    }
}
