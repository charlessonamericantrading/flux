/*
 * Estado replicado y peticion/respuesta SIN broker: las piezas puras que no cubre el arnes cruzado (que compara bytes y codigos de
 * las funciones puras) y las decisiones del cliente que se pueden fabricar sin red.
 *
 * Lo que necesita un broker (la carrera de escritores, el watch, la reapertura, los permisos) esta en StateIntegrationTest y
 * RpcIntegrationTest.
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.nats.client.JetStreamApiException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StateUnitTest {

    /** Un error de la API de JetStream con ese codigo: el constructor de {@code Error} no es publico, como en cualquier respuesta real. */
    private static JetStreamApiException apiError(int code, int apiCode, String description) throws Exception {
        Constructor<io.nats.client.api.Error> c = io.nats.client.api.Error.class.getDeclaredConstructor(int.class, int.class,
                String.class);
        c.setAccessible(true);
        return new JetStreamApiException(c.newInstance(code, apiCode, description));
    }

    // ─── el conflicto de secuencia: DOS codigos ──────────────────────────────

    @Test
    @DisplayName("el conflicto de secuencia es el 10071 Y el 10164 (un SDK que solo conozca el primero no reintenta bajo carrera: A-33, A-49)")
    void losDosCodigosDelConflicto() throws Exception {
        assertTrue(FluxState.isSequenceConflict(apiError(400, 10071, "wrong last sequence: 5")));
        assertTrue(FluxState.isSequenceConflict(apiError(400, 10164, "wrong last sequence")), "SIN numero: el que da un cluster bajo carrera");
        assertFalse(FluxState.isSequenceConflict(apiError(400, 10058, "stream name already in use")));
        assertFalse(FluxState.isSequenceConflict(apiError(503, 10077, "maximum messages exceeded")));
        assertFalse(FluxState.isSequenceConflict(new RuntimeException("wrong last sequence")), "no se reconoce por el texto");
        assertFalse(FluxState.isSequenceConflict((Throwable) null));
    }

    @Test
    @DisplayName("el conflicto se reconoce a traves de la cadena de causas (ExecutionException de publishAsync)")
    void elConflictoSeReconoceEnLaCausa() throws Exception {
        Exception wrapped = new java.util.concurrent.ExecutionException(apiError(400, 10164, "wrong last sequence"));
        assertTrue(FluxState.isSequenceConflict(wrapped));
        assertTrue(FluxState.isSequenceConflict(new java.util.concurrent.CompletionException(new RuntimeException(apiError(400, 10071, "x")))));
    }

    // ─── subjectMatches: lo que decide si DLQ_<DOM> captura dlq.state ────────

    @Test
    @DisplayName("subjectMatches: comodines de NATS (* un token, > el resto, y > exige al menos uno)")
    void subjectMatches() {
        assertTrue(FluxState.subjectMatches("dlq.state.clientes.>", "dlq.state.clientes.x.v1"));
        assertTrue(FluxState.subjectMatches("dlq.>", "dlq.state.clientes.x.v1"), "el DLQ de la v1.0 de un dominio de eventos NO captura estado: dlq.clientes.>");
        assertFalse(FluxState.subjectMatches("dlq.clientes.>", "dlq.state.clientes.x.v1"));
        assertFalse(FluxState.subjectMatches("dlq.state.clientes.>", "dlq.state.clientes"), "`>` exige al menos un token");
        assertTrue(FluxState.subjectMatches("state.clientes.*.v1", "state.clientes.cliente.v1"));
        assertFalse(FluxState.subjectMatches("state.clientes.*", "state.clientes.cliente.v1"));
    }

    // ─── onSynced ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("SyncTracker: dispara UNA vez, con la ultima de la foto ya procesada y nada en vuelo; un error no la retrasa")
    void syncTracker() {
        AtomicInteger fired = new AtomicInteger();
        FluxState.SyncTracker t = new FluxState.SyncTracker(fired::incrementAndGet);
        t.started(2);
        t.finished();
        assertEquals(0, fired.get(), "quedaban 2 por entregar: aun no esta al dia");
        t.started(1);
        t.started(0); // la ultima de la foto, con la anterior aun en vuelo
        t.finished();
        assertEquals(0, fired.get(), "hay una entrega en vuelo");
        t.finished();
        assertEquals(1, fired.get());
        t.started(0);
        t.finished();
        assertEquals(1, fired.get(), "UNA vez: no se repite con cada entrada posterior");
    }

    @Test
    @DisplayName("SyncTracker.immediate: contrato vacio o durable al dia, de inmediato y una sola vez")
    void syncImmediate() {
        AtomicInteger fired = new AtomicInteger();
        FluxState.SyncTracker t = new FluxState.SyncTracker(fired::incrementAndGet);
        t.immediate();
        t.immediate();
        t.started(0);
        t.finished();
        assertEquals(1, fired.get());
    }

    // ─── flux_consumer_pending = SUMA de los durables del contrato ───────────

    @Test
    @DisplayName("PendingSum: la serie suma los durables (uno por tenant) y al parar uno deja de sumar")
    void pendingSum() {
        FluxState.PendingSum p = new FluxState.PendingSum();
        assertEquals(5, p.note("s|c", "d1", 5));
        assertEquals(12, p.note("s|c", "d2", 7), "no el ultimo sondeado: la suma");
        assertEquals(9, p.note("s|c", "d1", 2));
        assertEquals(Long.valueOf(7), p.drop("s|c", "d1"));
        assertEquals(Long.valueOf(0), p.drop("s|c", "d2"));
        assertNull(p.drop("s|c", "d2"), "ya no suma");
    }

    // ─── metricas ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("InMemoryMetrics: las metricas de state y rpc salen con los nombres y etiquetas del protocolo, y las familias en el orden de Node")
    void metricas() {
        InMemoryMetrics m = new InMemoryMetrics();
        m.stateWrite("clientes.cliente.v1", MetricsSink.StateWriteOutcome.OK);
        m.stateWrite("clientes.cliente.v1", MetricsSink.StateWriteOutcome.STALE);
        m.rpcClientRequest("clientes.cliente.v1.obtener", MetricsSink.RpcClientOutcome.UNAVAILABLE);
        m.rpcServerRequest("clientes.cliente.v1.obtener", MetricsSink.RpcServerOutcome.POISON);
        m.rpcDuration("clientes.cliente.v1.obtener", "client", 0.2);
        m.rpcDuration("clientes.cliente.v1.obtener", "server", 40);
        String out = m.render();
        assertTrue(out.contains("flux_state_writes_total{contract=\"clientes.cliente.v1\",outcome=\"ok\"} 1"), out);
        assertTrue(out.contains("flux_state_writes_total{contract=\"clientes.cliente.v1\",outcome=\"stale\"} 1"), out);
        assertTrue(out.contains("flux_rpc_client_requests_total{operation=\"clientes.cliente.v1.obtener\",outcome=\"unavailable\"} 1"), out);
        assertTrue(out.contains("flux_rpc_server_requests_total{operation=\"clientes.cliente.v1.obtener\",outcome=\"poison\"} 1"), out);
        assertTrue(out.contains("flux_rpc_duration_seconds_bucket{operation=\"clientes.cliente.v1.obtener\",role=\"client\",le=\"0.25\"} 1"), out);
        assertTrue(out.contains("flux_rpc_duration_seconds_bucket{operation=\"clientes.cliente.v1.obtener\",role=\"server\",le=\"30\"} 0"), out);
        assertTrue(out.contains("flux_rpc_duration_seconds_bucket{operation=\"clientes.cliente.v1.obtener\",role=\"server\",le=\"+Inf\"} 1"), out);
        // El orden de las familias (el de Node, para que la salida de los dos se pueda comparar de un vistazo)
        int a = out.indexOf("# TYPE flux_events_retried_total");
        int b = out.indexOf("# TYPE flux_state_writes_total");
        int c = out.indexOf("# TYPE flux_rpc_client_requests_total");
        int d = out.indexOf("# TYPE flux_rpc_server_requests_total");
        assertTrue(a < b && b < c && c < d, out);
        assertTrue(out.indexOf("# TYPE flux_event_handler_duration_seconds histogram")
                < out.indexOf("# TYPE flux_rpc_duration_seconds histogram"), out);
    }

    @Test
    @DisplayName("un MetricsSink escrito antes de las capacidades state y rpc sigue compilando y funcionando (los metodos son opcionales)")
    void metricasOpcionales() {
        MetricsSink viejo = new MetricsSink() {
            @Override public void eventPublished(String subject, PublishOutcome outcome) { }
            @Override public void eventConsumed(String subject, String consumer, ConsumeOutcome outcome) { }
            @Override public void handlerDuration(String subject, String consumer, double seconds) { }
            @Override public void eventDlq(String subject, String consumer, FluxEvent.DlqReason reason, String code) { }
            @Override public void eventRetried(String subject, String consumer, int attempt) { }
            @Override public void consumerPending(String subject, String consumer, long pending) { }
            @Override public void connectionState(ConnectionState state) { }
        };
        viejo.stateWrite("a.b.v1", MetricsSink.StateWriteOutcome.OK);
        viejo.rpcClientRequest("a.b.v1.c", MetricsSink.RpcClientOutcome.OK);
        viejo.rpcServerRequest("a.b.v1.c", MetricsSink.RpcServerOutcome.OK);
        viejo.rpcDuration("a.b.v1.c", "client", 1);
    }

    // ─── el bundle trae los indices state y rpc ──────────────────────────────

    @Test
    @DisplayName("SchemaBundle: resuelve el dataschema de un contrato de estado y de una operacion sin red; un bundle sin esos indices sigue valiendo")
    void bundleConIndices() {
        SchemaBundle b = SchemaBundle.fromPath(Repo.root().resolve("schemas/bundle.json"));
        assertEquals("https://schemas.internal/state/clientes/cliente/1.0.0.json", b.stateSchemaUriFor("clientes.cliente.v1"));
        assertEquals("https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json",
                b.rpcSchemaUriFor("clientes.cliente.v1.calcular-descuento", RpcEnvelope.Side.REQUEST));
        assertEquals("https://schemas.internal/rpc/clientes/cliente/calcular-descuento/reply/1.0.0.json",
                b.rpcSchemaUriFor("clientes.cliente.v1.calcular-descuento", RpcEnvelope.Side.REPLY));
        assertNull(b.stateSchemaUriFor("clientes.cliente.v9"));
        assertNull(b.rpcSchemaUriFor("clientes.cliente.v1.no-existe", RpcEnvelope.Side.REPLY));
        // Un bundle generado antes de esas capacidades
        SchemaBundle viejo = SchemaBundle.fromJson("{\"schemas\":{\"https://x/y.json\":{\"type\":\"object\"}},\"subjects\":{}}");
        assertNull(viejo.stateSchemaUriFor("clientes.cliente.v1"));
        assertNull(viejo.rpcSchemaUriFor("clientes.cliente.v1.obtener", RpcEnvelope.Side.REQUEST));
    }




    @Test
    @DisplayName("los codigos de las metricas del protocolo son los del contrato (el wire de cada enum)")
    void codigosDeMetricas() {
        List<String> client = new ArrayList<>();
        for (MetricsSink.RpcClientOutcome o : MetricsSink.RpcClientOutcome.values()) {
            client.add(o.wire());
        }
        assertEquals(List.of("ok", "unavailable", "timeout", "retryable", "permanent", "invalid_schema", "invalid_reply"), client);
        List<String> server = new ArrayList<>();
        for (MetricsSink.RpcServerOutcome o : MetricsSink.RpcServerOutcome.values()) {
            server.add(o.wire());
        }
        assertEquals(List.of("ok", "retryable", "permanent", "poison", "invalid_schema", "invalid_signature", "stale_request"), server);
        List<String> state = new ArrayList<>();
        for (MetricsSink.StateWriteOutcome o : MetricsSink.StateWriteOutcome.values()) {
            state.add(o.wire());
        }
        assertEquals(List.of("ok", "stale", "conflict", "invalid_schema"), state);
    }

    @SuppressWarnings("unused")
    private static JsonNode unused() {
        return null;
    }
}
