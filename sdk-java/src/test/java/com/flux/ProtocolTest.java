/*
 * Naming, constantes e invariantes del protocolo.
 * Contrato normativo: specification/02-naming.md, specification/03-delivery.md
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ProtocolTest {

    @Test
    @DisplayName("parseSubject descompone los cuatro tokens y la transformacion es biyectiva")
    void parseSubjectValido() {
        Protocol.ParsedSubject p = Protocol.parseSubject("pedidos.pedido.v1.creado");
        assertEquals("pedidos", p.domain());
        assertEquals("pedido", p.aggregate());
        assertEquals(1, p.major());
        assertEquals("creado", p.event());
        assertEquals("pedidos.pedido.v1.creado", p.toSubject());

        Protocol.ParsedSubject kebab = Protocol.parseSubject("logistica.envio.v2.entrega-fallida");
        assertEquals("entrega-fallida", kebab.event());
        assertEquals(2, kebab.major());
    }

    @Test
    @DisplayName("una mayuscula se rechaza con el motivo del subject fantasma")
    void parseSubjectRechazaMayusculas() {
        // NATS es case-sensitive: "Pedidos.pedido.v1.creado" crea un subject al que nadie
        // esta suscrito y no produce ningun error — 02-naming.md §1.1.
        Protocol.InvalidSubjectException e = assertThrows(Protocol.InvalidSubjectException.class,
                () -> Protocol.parseSubject("Pedidos.pedido.v1.creado"));
        assertTrue(e.getMessage().contains("minusculas"), e.getMessage());
        assertTrue(e.getMessage().contains("nadie esta suscrito"),
                "el mensaje debe explicar el subject fantasma, no solo decir 'invalido': " + e.getMessage());
    }

    @Test
    @DisplayName("el numero de tokens aparece en el mensaje de error")
    void parseSubjectRechazaTokensIncorrectos() {
        Protocol.InvalidSubjectException pocos = assertThrows(Protocol.InvalidSubjectException.class,
                () -> Protocol.parseSubject("pedidos.crear-pedido"));
        assertTrue(pocos.getMessage().contains("tiene 2"), pocos.getMessage());

        Protocol.InvalidSubjectException muchos = assertThrows(Protocol.InvalidSubjectException.class,
                () -> Protocol.parseSubject("pedidos.pedido.v1.creado.retry"));
        assertTrue(muchos.getMessage().contains("tiene 5"), muchos.getMessage());
    }

    @Test
    @DisplayName("los antipatrones de la spec no pasan la validacion")
    void parseSubjectRechazaFormatosInvalidos() {
        List<String> invalidos = List.of(
                "pedidos.pedido.V1.creado",   // version en mayuscula
                "pedidos.pedido.v0.creado",   // major debe ser >= 1
                "pedidos.pedido.1.creado",    // falta la 'v'
                "pedidos.pedido_v1.creado",   // guion bajo prohibido
                "pedidos..v1.creado",         // token vacio
                "pedidos.pedido.v1.",         // evento vacio
                "pedidos pedido v1 creado");  // espacios
        for (String subject : invalidos) {
            assertFalse(Protocol.isValidSubject(subject), "deberia rechazarse: " + subject);
        }
    }

    @Test
    @DisplayName("subjectToType mueve la version al final y parseType lo deshace")
    void subjectToType() {
        assertEquals("com.flux.pedidos.pedido.creado.v1",
                Protocol.subjectToType("pedidos.pedido.v1.creado"));
        assertEquals("com.flux.logistica.envio.entrega-fallida.v2",
                Protocol.subjectToType("logistica.envio.v2.entrega-fallida"));

        // La transformacion es mecanica y biyectiva en ambos sentidos — 02-naming.md §2.
        assertEquals("pedidos.pedido.v1.creado",
                Protocol.parseType("com.flux.pedidos.pedido.creado.v1").toSubject());
    }

    @Test
    @DisplayName("los nombres de stream no llevan puntos y van en mayusculas")
    void streamName() {
        assertEquals("EVT_PEDIDOS", Protocol.streamName("pedidos"));
        assertEquals("DLQ_PEDIDOS", Protocol.dlqStreamName("pedidos"));
        // NATS no admite '.' ni '-' comodos en nombres de stream — 02-naming.md §3.
        assertEquals("EVT_GESTION_ALMACEN", Protocol.streamName("gestion-almacen"));
    }

    @Test
    @DisplayName("el durable es reversible y valida tambien el nombre de servicio")
    void durableName() {
        assertEquals("facturacion-api__pedidos_pedido_v1_creado",
                Protocol.durableName("facturacion-api", "pedidos.pedido.v1.creado"));

        // Partiendo por "__" se recuperan servicio y subject exactos — 02-naming.md §4.
        String[] partes = Protocol.durableName("facturacion-api", "pedidos.pedido.v1.creado").split("__");
        assertEquals("facturacion-api", partes[0]);
        assertEquals("pedidos_pedido_v1_creado", partes[1]);

        // NATS aceptaria "FacturacionAPI__…" sin error: el SDK DEBE validarlo
        // (protocol.json naming.service).
        assertThrows(Protocol.InvalidServiceNameException.class,
                () -> Protocol.durableName("FacturacionAPI", "pedidos.pedido.v1.creado"));
        assertThrows(Protocol.InvalidServiceNameException.class,
                () -> Protocol.durableName("facturacion_api", "pedidos.pedido.v1.creado"));
        assertThrows(Protocol.InvalidServiceNameException.class,
                () -> Protocol.durableName("facturacion api", "pedidos.pedido.v1.creado"));

        // Y el subject tambien.
        assertThrows(Protocol.InvalidSubjectException.class,
                () -> Protocol.durableName("facturacion-api", "Pedidos.pedido.v1.creado"));
    }

    @Test
    @DisplayName("la DLQ va por PREFIJO para quedar fuera de <dominio>.>")
    void dlqSubjectEsPrefijo() {
        String subject = "pedidos.pedido.v1.creado";
        String dlq = Protocol.dlqSubject(subject);
        assertEquals("dlq.pedidos.pedido.v1.creado", dlq);

        // Si fuese sufijo, encajaria con "pedidos.>" y EVT_PEDIDOS capturaria sus propios
        // muertos — 02-naming.md §3.1.
        assertFalse(dlq.startsWith("pedidos."),
                "el subject de DLQ no debe caer dentro del espacio del dominio");
        assertTrue(Protocol.isDlqSubject(dlq));
        assertFalse(Protocol.isDlqSubject(subject));
    }

    @Test
    @DisplayName("source identifica entorno y servicio")
    void sourceUri() {
        assertEquals("/produccion/pedidos-api", Protocol.sourceUri("produccion", "pedidos-api"));
    }

    // ─── Invariantes de la configuracion canonica ────────────────────────────

    @Test
    @DisplayName("ack_wait == backoff[0]: JetStream lo sobrescribe sin avisar")
    void invarianteAckWaitIgualBackoffCero() {
        // Es LA trampa del protocolo. Verificada contra nats-server 2.14.5 — 03-delivery.md
        // §2.1 y conformance/cases/consumer-config.json.
        assertEquals(Protocol.DEFAULT_ACK_WAIT, Protocol.canonicalBackoff().get(0),
                "backoff[0] ES el ack_wait efectivo; si difieren, el handler se ejecuta en "
                        + "concurrencia consigo mismo sin ningun error visible");
        assertEquals(Duration.ofSeconds(30), Protocol.DEFAULT_ACK_WAIT);
    }

    @Test
    @DisplayName("max_deliver = 1 entrega + una por entrada de backoff")
    void invarianteMaxDeliverCuadraConBackoff() {
        // Si max_deliver fuese 5, la ultima entrada (30 m) no se aplicaria nunca y la
        // configuracion mentiria sobre su propio comportamiento — 03-delivery.md §2.
        assertEquals(Protocol.DEFAULT_MAX_DELIVER - 1, Protocol.canonicalBackoff().size());
        assertEquals(6, Protocol.DEFAULT_MAX_DELIVER);
    }

    @Test
    @DisplayName("el backoff canonico coincide con protocol.json y suma 51 min 30 s")
    void backoffCanonico() {
        assertEquals(
                List.of(Duration.ofSeconds(30), Duration.ofSeconds(60), Duration.ofSeconds(300),
                        Duration.ofSeconds(900), Duration.ofSeconds(1800)),
                Protocol.canonicalBackoff());
        // totalTimeToDlqSeconds de protocol.json.
        assertEquals(Duration.ofSeconds(3090), Protocol.totalTimeToDlq());
    }

    @Test
    @DisplayName("el backoff canonico no es mutable desde fuera")
    void backoffNoMutable() {
        // Una entrada [0] alterada cambiaria en silencio el ack_wait efectivo de todo
        // consumidor creado despues. En Go se resuelve devolviendo una copia; aqui la lista
        // es inmutable y el intento falla en alto.
        assertThrows(UnsupportedOperationException.class,
                () -> Protocol.canonicalBackoff().set(0, Duration.ofSeconds(1)));
    }

    @Test
    @DisplayName("las constantes de mensaje y envelope son las de protocol.json")
    void constantes() {
        assertEquals("1.0", Protocol.SPEC_VERSION);
        assertEquals("application/json", Protocol.DATA_CONTENT_TYPE);
        assertEquals(1_048_576, Protocol.MAX_MESSAGE_BYTES);
        assertEquals(256, Protocol.DEFAULT_MAX_ACK_PENDING);
        assertEquals(Duration.ofDays(30), Protocol.STREAM_MAX_AGE);
        assertEquals(Duration.ofDays(90), Protocol.DLQ_STREAM_MAX_AGE);
        assertEquals(Duration.ofMinutes(2), Protocol.DUPLICATE_WINDOW);
    }

    // ─── UUIDv7 ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("uuidV7 produce version 7 y variante RFC 4122")
    void uuidV7EsV7() {
        // java.util.UUID no genera v7: randomUUID() es v4. Si esto falla, el `id` deja de
        // ser monotonico y el orden por id dentro de un source deja de significar nada
        // — 01-envelope.md §2.4.
        UUID uuid = UUID.fromString(Protocol.uuidV7());
        assertEquals(7, uuid.version(), "debe ser UUIDv7");
        assertEquals(2, uuid.variant(), "variante RFC 4122 (10b)");
    }

    @Test
    @DisplayName("uuidV7 lleva el instante actual en los 48 bits altos")
    void uuidV7LlevaElTiempo() {
        long antes = System.currentTimeMillis();
        UUID uuid = UUID.fromString(Protocol.uuidV7());
        long despues = System.currentTimeMillis();

        long timestamp = uuid.getMostSignificantBits() >>> 16;
        assertTrue(timestamp >= antes && timestamp <= despues,
                "el timestamp embebido (" + timestamp + ") debe caer entre " + antes + " y " + despues);
    }

    @Test
    @DisplayName("uuidV7 es monotonico incluso dentro del mismo milisegundo")
    void uuidV7EsMonotonico() {
        // El contador de rand_a existe justo para esto: sin el, una rafaga de eventos en el
        // mismo milisegundo saldria desordenada y la propiedad de "ordenar por id equivale a
        // ordenar por instante" se perderia cuando mas se usa.
        int n = 5_000;
        List<String> ids = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ids.add(Protocol.uuidV7());
        }

        Set<String> unicos = new HashSet<>(ids);
        assertEquals(n, unicos.size(), "los ids deben ser unicos");

        for (int i = 1; i < n; i++) {
            assertTrue(ids.get(i - 1).compareTo(ids.get(i)) < 0,
                    "el id " + i + " (" + ids.get(i) + ") no es mayor que el anterior ("
                            + ids.get(i - 1) + "): el orden lexicografico debe seguir al temporal");
        }
        assertNotEquals(ids.get(0), ids.get(n - 1));
    }
}
