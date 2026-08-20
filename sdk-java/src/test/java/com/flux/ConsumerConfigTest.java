/*
 * Verificacion de la configuracion efectiva del consumidor y presupuesto de reintentos.
 * Contrato normativo: specification/03-delivery.md §2.1, specification/04-errors.md §2.1
 *
 * Estos tests no necesitan broker: comprueban la logica que protege del unico fallo del
 * protocolo que NO produce ningun error visible en produccion.
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConsumerConfigTest {

    private static final String DURABLE = "facturacion-api__pedidos_pedido_v1_creado";

    /** La misma configuracion que envia FluxBus.subscribe. */
    private static ConsumerConfiguration canonica() {
        return ConsumerConfiguration.builder()
                .durable(DURABLE)
                .filterSubject("pedidos.pedido.v1.creado")
                .ackPolicy(AckPolicy.Explicit)
                .ackWait(Protocol.DEFAULT_ACK_WAIT)
                .maxDeliver(Protocol.DEFAULT_MAX_DELIVER)
                .backoff(Protocol.canonicalBackoff().toArray(new Duration[0]))
                .maxAckPending(Protocol.DEFAULT_MAX_ACK_PENDING)
                .deliverPolicy(DeliverPolicy.All)
                .replayPolicy(ReplayPolicy.Instant)
                .build();
    }

    @Test
    @DisplayName("la config canonica se acepta sin diferencias")
    void aceptaLaConfigCanonica() {
        assertDoesNotThrow(() -> FluxBus.assertConfigHonored(DURABLE, canonica(), canonica()));
    }

    @Test
    @DisplayName("detecta la sobrescritura silenciosa de ack_wait por backoff[0]")
    void detectaLaSobrescrituraDeAckWait() {
        // El contraejemplo de conformance/cases/consumer-config.json: se pide ack_wait 30 s
        // con un backoff que empieza en 1 s, y el servidor devuelve ack_wait 1 s sin error.
        // Con 1 s efectivo, cualquier handler que escriba en BD se reentrega mientras la
        // primera ejecucion sigue viva.
        ConsumerConfiguration efectiva = ConsumerConfiguration.builder(canonica())
                .ackWait(Duration.ofSeconds(1))
                .backoff(Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofSeconds(30),
                        Duration.ofMinutes(2), Duration.ofMinutes(10))
                .build();

        ConsumerConfigMismatchException e = assertThrows(ConsumerConfigMismatchException.class,
                () -> FluxBus.assertConfigHonored(DURABLE, canonica(), efectiva));

        List<String> campos = e.differences().stream()
                .map(ConsumerConfigMismatchException.Difference::field).toList();
        assertTrue(campos.contains("ack_wait"), campos.toString());
        assertTrue(campos.contains("backoff"), campos.toString());
        assertTrue(e.getMessage().contains("backoff[0]"),
                "el mensaje debe apuntar a la causa real: " + e.getMessage());
        assertEquals(DURABLE, e.durable());
    }

    @Test
    @DisplayName("detecta el resto de campos alterados")
    void detectaOtrosCamposAlterados() {
        ConsumerConfiguration menosEntregas = ConsumerConfiguration.builder(canonica())
                .maxDeliver(3).build();
        assertThrows(ConsumerConfigMismatchException.class,
                () -> FluxBus.assertConfigHonored(DURABLE, canonica(), menosEntregas));

        ConsumerConfiguration otraVentana = ConsumerConfiguration.builder(canonica())
                .maxAckPending(1).build();
        assertThrows(ConsumerConfigMismatchException.class,
                () -> FluxBus.assertConfigHonored(DURABLE, canonica(), otraVentana));

        // El protocolo exige ack explicito SIEMPRE: nunca auto-ack.
        ConsumerConfiguration otroAck = ConsumerConfiguration.builder(canonica())
                .ackPolicy(AckPolicy.None).build();
        assertThrows(ConsumerConfigMismatchException.class,
                () -> FluxBus.assertConfigHonored(DURABLE, canonica(), otroAck));
    }

    @Test
    @DisplayName("valida la invariante sobre la config EFECTIVA, no solo sobre la solicitada")
    void validaLaInvarianteSobreLaConfigEfectiva() {
        // Aunque el servidor devuelva exactamente lo que se le pidio, si ack_wait y
        // backoff[0] no coinciden la configuracion es incorrecta. Este caso caza el error
        // de mantenimiento: alguien cambia el backoff canonico y olvida DEFAULT_ACK_WAIT.
        ConsumerConfiguration incoherente = ConsumerConfiguration.builder(canonica())
                .ackWait(Duration.ofSeconds(10))
                .backoff(Duration.ofSeconds(30), Duration.ofMinutes(1))
                .build();

        ConsumerConfigMismatchException e = assertThrows(ConsumerConfigMismatchException.class,
                () -> FluxBus.assertConfigHonored(DURABLE, incoherente, incoherente));
        assertTrue(e.differences().stream()
                        .anyMatch(d -> d.field().equals("ack_wait == backoff[0]")),
                e.getMessage());
    }

    // ─── Presupuesto de reintentos ───────────────────────────────────────────

    @Test
    @DisplayName("el presupuesto efectivo es el minimo entre el del consumidor y el del error")
    void presupuestoEfectivo() {
        // Un error desconocido agota su presupuesto acotado sin recortar los 6 intentos de
        // un transitorio reconocido — 04-errors.md §2.1.
        assertEquals(6, FluxBus.effectiveBudget(6, null), "sin maxAttempts, el del consumidor");
        assertEquals(2, FluxBus.effectiveBudget(6, 2), "desconocido acotado");
        assertEquals(3, FluxBus.effectiveBudget(3, 6),
                "la clasificacion nunca puede AMPLIAR el presupuesto del consumidor");
        assertEquals(1, FluxBus.effectiveBudget(6, 1));
    }
}
