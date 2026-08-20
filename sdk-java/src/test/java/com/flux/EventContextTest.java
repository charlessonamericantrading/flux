/*
 * Propagacion de contexto entre eventos.
 * Contrato normativo: specification/01-envelope.md §5
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EventContextTest {

    private static final String ENVELOPE = """
            {"specversion":"1.0","id":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",\
            "source":"/produccion/pedidos-api","type":"com.flux.pedidos.pedido.creado.v1",\
            "time":"2026-08-20T10:25:39.412Z","datacontenttype":"application/json",\
            "dataschema":"https://schemas.internal/pedidos/pedido/creado/1.2.0.json",\
            "correlationid":"01924f00-0000-7000-8000-000000000000",\
            "causationid":"01924f11-1111-7111-8111-111111111111",\
            "tenantid":"acme","producerversion":"3.4.1","dataclassification":"confidential",\
            "traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",\
            "data":{"pedidoId":"ped-123"}}""";

    @Test
    @DisplayName("el causationId hereda el id del evento en curso, no su causationid")
    void contextFromEventEncadenaCausalidad() {
        // La causa de lo que se publique ahora es ESTE evento, no el que lo causo a el
        // — 01-envelope.md §3.2.
        FluxEvent event = Envelope.parseEvent(ENVELOPE.getBytes(StandardCharsets.UTF_8));
        EventContext ctx = EventContext.fromEvent(event);

        assertEquals(event.id(), ctx.causationId());
        assertNotEquals(event.causationid(), ctx.causationId());

        // El correlationid se propaga SIN MODIFICAR por toda la cadena.
        assertEquals("01924f00-0000-7000-8000-000000000000", ctx.correlationId());
        assertEquals("acme", ctx.tenantId());
        assertEquals("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", ctx.traceparent());
        assertFalse(ctx.isRoot());
        assertTrue(ctx.traceparentOpt().isPresent());
    }

    @Test
    @DisplayName("ROOT significa 'el evento nace de cero'")
    void rootEsElEventoQueNaceDeCero() {
        // Publish inicializara el correlationid con el id del propio evento
        // — 01-envelope.md §3.1.
        assertTrue(EventContext.ROOT.isRoot());
        assertNull(EventContext.ROOT.correlationId());
        assertNull(EventContext.ROOT.causationId());
        assertTrue(EventContext.ROOT.traceparentOpt().isEmpty());
    }

    @Test
    @DisplayName("dos eventos del mismo flujo comparten correlationid y encadenan causationid")
    void cadenaDeDosSaltos() {
        FluxEvent primero = Envelope.parseEvent(ENVELOPE.getBytes(StandardCharsets.UTF_8));
        EventContext ctx1 = EventContext.fromEvent(primero);

        // Lo que publicaria un handler a partir de ctx1.
        FluxEvent segundo = new Envelope.BuildEventInput()
                .subject("facturacion.factura.v1.emitida")
                .data(java.util.Map.of("facturaId", "fac-1"))
                .id("01924f22-2222-7222-8222-222222222222")
                .source("/produccion/facturacion-api")
                .producerVersion("1.0.0")
                .dataSchema("https://schemas.internal/facturacion/factura/emitida/1.0.0.json")
                .tenantId(ctx1.tenantId())
                .dataClassification(FluxEvent.DataClassification.INTERNAL)
                .correlationId(ctx1.correlationId())
                .causationId(ctx1.causationId())
                .traceparent(ctx1.traceparent())
                .build();

        assertEquals(primero.correlationid(), segundo.correlationid());
        assertEquals(primero.id(), segundo.causationid());

        // Y el contexto del segundo apunta al segundo, no al primero.
        EventContext ctx2 = EventContext.fromEvent(segundo);
        assertEquals(segundo.id(), ctx2.causationId());
        assertEquals(primero.correlationid(), ctx2.correlationId());
    }
}
