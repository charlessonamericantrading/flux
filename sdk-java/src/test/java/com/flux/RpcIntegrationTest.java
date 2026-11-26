/*
 * Peticion/respuesta EJECUTADA contra un nats-server REAL, bajo los permisos EXACTOS que genera `scripts/gen-acl.mjs` de
 * `examples/services.json` (la capacidad `rpc`, 11-request-reply.md): quien llama publica en el subject EXACTO de cada operacion que
 * se le concede; el dueno se suscribe a `rpc.<dominio>.>` y responde con `allow_responses`, sin ningun permiso de publicacion sobre inboxes.
 *
 *   NATS_SERVER=/ruta/a/nats-server FLUX_REQUIRE_BROKER=1 mvn test -Dtest=RpcIntegrationTest
 */
package com.flux;

import static com.flux.TestSupport.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flux.RpcErrors.InvalidRpcCallException;
import com.flux.RpcErrors.RpcInvalidReplyException;
import com.flux.RpcErrors.RpcMessageTooLargeException;
import com.flux.RpcErrors.RpcPermanentException;
import com.flux.RpcErrors.RpcRetryableException;
import com.flux.RpcErrors.RpcTimeoutException;
import com.flux.RpcErrors.RpcUnavailableException;
import com.flux.SecurityErrors.Operation;
import com.flux.SecurityErrors.PermissionDeniedException;
import com.flux.TestSupport.CapturingLogger;
import com.flux.broker.SecureEnv;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RpcIntegrationTest {

    private static final String DESCUENTO = "clientes.cliente.v1.calcular-descuento";
    private static final String OBTENER = "clientes.cliente.v1.obtener";
    private static final String SUBJECT_DESCUENTO = "rpc." + DESCUENTO;
    private static final String SUBJECT_OBTENER = "rpc." + OBTENER;

    private static SecureEnv env;
    private final List<AutoCloseable> abiertos = new ArrayList<>();

    @BeforeAll
    static void start() throws Exception {
        env = SecureEnv.builder()
                // el dueno de `clientes` SIN permiso de suscripcion sobre rpc.clientes.>: parece un servicio caido (A-73)
                .variant("crm-sin-suscribir", "crm-api", List.of(), List.of("rpc.clientes.>"))
                // facturacion-api sin poder llamar a `obtener`
                .variant("fact-sin-obtener", "facturacion-api", List.of(SUBJECT_OBTENER), List.of())
                // facturacion-api sin su inbox privado: las respuestas no pueden llegar
                .variant("fact-sin-inbox", "facturacion-api", List.of(), List.of("_INBOX.facturacion-api.>"))
                .build();
    }

    @AfterAll
    static void stop() {
        if (env != null) {
            env.close();
        }
    }

    @AfterEach
    void cierra() throws Exception {
        for (Tap t : taps) {
            t.dispatcher.unsubscribe(t.subject);
        }
        taps.clear();
        for (Object[] f : fakes) {
            ((Dispatcher) f[0]).unsubscribe((String) f[1]);
        }
        fakes.clear();
        env.ops().flush(Duration.ofSeconds(2));
        for (AutoCloseable c : abiertos) {
            try {
                c.close();
            } catch (Exception ignored) {
                // ya cerrado
            }
        }
        abiertos.clear();
    }

    private <T extends AutoCloseable> T track(T c) {
        abiertos.add(c);
        return c;
    }

    private FluxBus bus(String id) {
        return track(FluxBus.connect(env.options(id).tenantId("acme")));
    }

    private FluxBus bus(FluxBus.ConnectOptions o) {
        return track(FluxBus.connect(o));
    }

    /** Espera a que un contador alcance ese valor: el que responde lo anota DESPUES de publicar la respuesta, y quien llama ya la tiene. */
    private static void counter(InMemoryMetrics m, String key, long expected) throws Exception {
        await(key + " = " + expected, 5_000, () -> Long.valueOf(expected).equals(m.counters().get(key)));
    }

    private static Map<String, Object> peticion() {
        return Map.of("clienteId", "cli-1", "totalCents", 10_000, "moneda", "EUR");
    }

    private static FluxRpc.Handler descuento() {
        return (data, ctx) -> Map.of("descuentoCents", data.get("totalCents").intValue() / 10, "moneda", data.get("moneda").textValue());
    }

    /** Un suscriptor SIN cola (ve una copia de cada mensaje) con la conexion de operacion: lo que viaja de verdad. */
    private final class Tap {
        final List<Message> messages = new CopyOnWriteArrayList<>();
        final Dispatcher dispatcher;
        final String subject;

        Tap(String subject) throws Exception {
            this.subject = subject;
            Connection ops = env.ops();
            dispatcher = ops.createDispatcher(messages::add);
            dispatcher.subscribe(subject);
            ops.flush(Duration.ofSeconds(2));
            taps.add(this);
        }
    }

    /** Lo que se dejo suscrito con la conexion de operacion: TIENE que soltarse. Un suscriptor olvidado da interes al subject y el broker deja de contestar `no responders`. */
    private final List<Tap> taps = new ArrayList<>();
    private final List<Object[]> fakes = new ArrayList<>();

    /** Un responder FALSO con la conexion de operacion: contesta lo que se le diga (o nada). */
    private Dispatcher fake(String subject, Function<Message, byte[]> reply) throws Exception {
        Connection ops = env.ops();
        Dispatcher d = ops.createDispatcher(msg -> {
            byte[] bytes = reply.apply(msg);
            if (bytes != null && msg.getReplyTo() != null) {
                ops.publish(msg.getReplyTo(), bytes);
            }
        });
        d.subscribe(subject);
        ops.flush(Duration.ofSeconds(2));
        fakes.add(new Object[] {d, subject});
        return d;
    }

    private void unfake(Dispatcher d) throws Exception {
        for (Object[] f : new ArrayList<>(fakes)) {
            if (f[0] == d) {
                fakes.remove(f);
                d.unsubscribe((String) f[1]);
            }
        }
        env.ops().flush(Duration.ofSeconds(2));
    }

    // ═════════════════════════════════════════════════════════════════════════
    // El ciclo
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("el ciclo bajo permisos exactos: llamar, atender y recibir la respuesta, SIN una violacion en el log del servidor (allow_responses: el dueno no publica en inboxes)")
    void cicloBasico() throws Exception {
        int antes = env.server.violations().size();
        InMemoryMetrics clientMetrics = new InMemoryMetrics();
        InMemoryMetrics serverMetrics = new InMemoryMetrics();
        CapturingLogger log = new CapturingLogger();
        Tap peticiones = new Tap(SUBJECT_DESCUENTO);
        Tap respuestas = new Tap("_INBOX.>");
        FluxBus crm = bus(env.options("crm-api").tenantId("acme").metrics(serverMetrics).logger(log));
        FluxBus pedidos = bus(env.options("pedidos-api").tenantId("acme").metrics(clientMetrics).logger(log));
        FluxRpc.Responder r = crm.respond(DESCUENTO, descuento());
        assertEquals(SUBJECT_DESCUENTO, r.subject(), "la suscripcion es al subject EXACTO, nunca rpc.<dominio>.>");

        JsonNode out = pedidos.request(DESCUENTO, peticion());
        assertEquals(1000, out.get("descuentoCents").intValue());
        assertEquals("EUR", out.get("moneda").textValue());

        // Lo que viajo de verdad
        await("la peticion", 5_000, () -> !peticiones.messages.isEmpty() && !respuestas.messages.isEmpty());
        String req = new String(peticiones.messages.get(0).getData(), StandardCharsets.UTF_8);
        assertTrue(req.indexOf("\"data\":") > req.indexOf("\"dataclassification\""), "data el ultimo: " + req);
        FluxEvent rq = Envelope.parseEvent(peticiones.messages.get(0).getData());
        assertEquals("com.flux.rpc.clientes.cliente.calcular-descuento.request.v1", rq.type());
        assertEquals("acme", rq.tenantid());
        assertEquals(rq.id(), rq.correlationid(), "una peticion que nace de cero inicia su cadena");
        assertEquals("/produccion/pedidos-api", rq.source());
        assertNull(rq.causationid());
        assertTrue(peticiones.messages.get(0).getReplyTo().startsWith("_INBOX.pedidos-api."), peticiones.messages.get(0).getReplyTo());
        FluxEvent rp = Envelope.parseEvent(respuestas.messages.get(0).getData());
        assertEquals("com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1", rp.type());
        assertEquals(rq.id(), rp.causationid(), "la respuesta dice a que peticion contesta");
        assertEquals(rq.correlationid(), rp.correlationid());
        assertEquals(rq.tenantid(), rp.tenantid());
        assertEquals("/produccion/crm-api", rp.source());
        assertTrue(!rp.id().equals(rq.id()), "id NUEVO");

        Map<String, Long> c = clientMetrics.counters();
        assertEquals(1L, c.get("flux_rpc_client_requests_total{operation=\"" + DESCUENTO + "\",outcome=\"ok\"}"));
        counter(serverMetrics, "flux_rpc_server_requests_total{operation=\"" + DESCUENTO + "\",outcome=\"ok\"}", 1);
        assertTrue(clientMetrics.render().contains("flux_rpc_duration_seconds_count{operation=\"" + DESCUENTO + "\",role=\"client\"} 1"));
        assertTrue(serverMetrics.render().contains("flux_rpc_duration_seconds_count{operation=\"" + DESCUENTO + "\",role=\"server\"} 1"));
        r.stop();
        assertEquals(antes, env.server.violations().size(), "violaciones nuevas: " + env.server.violations());
        assertTrue(log.lines.stream().noneMatch(l -> l.startsWith("ERROR")), log.lines.toString());
    }

    @Test
    @DisplayName("la cadena de correlacion continua dentro de un handler: tenant, correlationid y causationid del evento en curso van en la peticion, y la respuesta los hereda")
    void contextoHeredado() throws Exception {
        Tap peticiones = new Tap(SUBJECT_DESCUENTO);
        Tap respuestas = new Tap("_INBOX.>");
        FluxBus crm = bus("crm-api");
        FluxBus pedidos = bus("pedidos-api");
        crm.respond(DESCUENTO, (data, ctx) -> {
            assertEquals("corr-77", ctx.correlationId());
            assertEquals("globex", ctx.tenantId());
            assertNotNull(ctx.eventContext().causationId());
            return Map.of("descuentoCents", 1, "moneda", "EUR");
        });
        EventContext padre = new EventContext("corr-77", "evt-5", "globex", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", null);
        pedidos.request(padre, DESCUENTO, peticion(), new FluxRpc.RequestOptions().aggregateId("cli-1"));
        await("lo que viajo", 5_000, () -> !peticiones.messages.isEmpty() && !respuestas.messages.isEmpty());
        FluxEvent rq = Envelope.parseEvent(peticiones.messages.get(0).getData());
        assertEquals("corr-77", rq.correlationid());
        assertEquals("evt-5", rq.causationid());
        assertEquals("globex", rq.tenantid());
        assertEquals("cli-1", rq.aggregateId(), "el atributo `subject` del sobre es el agregado, NO el subject de NATS");
        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", rq.traceparent());
        FluxEvent rp = Envelope.parseEvent(respuestas.messages.get(0).getData());
        assertEquals("corr-77", rp.correlationid());
        assertEquals("globex", rp.tenantid());
        assertEquals("cli-1", rp.aggregateId());
        assertEquals(rq.id(), rp.causationid());
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Instancias del dueno: reparto, parada, nadie escucha
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("dos instancias del dueno se reparten las peticiones (grupo de cola = el servicio); parada una, sigue la otra; paradas las dos, RPC_UNAVAILABLE al instante")
    void dosInstancias() throws Exception {
        AtomicInteger a = new AtomicInteger();
        AtomicInteger b = new AtomicInteger();
        FluxBus crmA = bus("crm-api");
        FluxBus crmB = bus("crm-api");
        FluxBus pedidos = bus("pedidos-api");
        FluxRpc.Responder ra = crmA.respond(DESCUENTO, (d, c) -> {
            a.incrementAndGet();
            return Map.of("descuentoCents", 1, "moneda", "EUR");
        });
        FluxRpc.Responder rb = crmB.respond(DESCUENTO, (d, c) -> {
            b.incrementAndGet();
            return Map.of("descuentoCents", 2, "moneda", "EUR");
        });
        for (int i = 0; i < 60; i++) {
            pedidos.request(DESCUENTO, peticion());
        }
        assertEquals(60, a.get() + b.get(), "cada peticion la atiende UNA instancia, no las dos");
        assertTrue(a.get() > 0 && b.get() > 0, "las dos reciben trabajo: a=" + a + " b=" + b);

        ra.stop();
        int bAntes = b.get();
        for (int i = 0; i < 10; i++) {
            assertEquals(2, pedidos.request(DESCUENTO, peticion()).get("descuentoCents").intValue());
        }
        assertEquals(bAntes + 10, b.get(), "parada la A, todo va a la B");

        rb.stop();
        long t0 = System.nanoTime();
        RpcUnavailableException e = assertThrows(RpcUnavailableException.class, () -> pedidos.request(DESCUENTO, peticion()));
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms < 100, "`no responders` es inmediato; no gasta el timeout: " + ms + " ms");
        assertEquals("RPC_UNAVAILABLE", e.code());
        assertEquals(ErrorClass.RETRYABLE, e.errorClass());
        assertEquals(DESCUENTO, e.operation());
    }

    @Test
    @DisplayName("una operacion que nadie atiende da RPC_UNAVAILABLE (retryable) y se cuenta; con `retries` reintenta SOLO esto, y el error final sigue siendo ese")
    void nadieEscucha() throws Exception {
        InMemoryMetrics m = new InMemoryMetrics();
        FluxBus pedidos = bus(env.options("pedidos-api").tenantId("acme").metrics(m));
        long t0 = System.nanoTime();
        assertThrows(RpcUnavailableException.class, () -> pedidos.request(DESCUENTO, peticion(), new FluxRpc.RequestOptions().retries(2)));
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms >= 140 && ms < 1500, "3 intentos con una pausa corta entre ellos: " + ms + " ms");
        assertEquals(1L, m.counters().get("flux_rpc_client_requests_total{operation=\"" + DESCUENTO + "\",outcome=\"unavailable\"}"),
                "UNA salida por llamada, no una por intento");
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Permisos
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("llamar a lo que el ACL no concede falla en milisegundos con PERMISSION_DENIED (nombra el subject y la clave `calls`), no tras un timeout; con la concesion, funciona")
    void llamadaDenegada() throws Exception {
        InMemoryMetrics m = new InMemoryMetrics();
        FluxBus crm = bus("crm-api");
        crm.respond(OBTENER, (d, c) -> Map.of("clienteId", "cli-1", "nombre", "Ana", "email", "ana@example.com"));
        // pedidos-api no tiene `obtener` en `calls`
        FluxBus pedidos = bus(env.options("pedidos-api").tenantId("acme").metrics(m));
        long t0 = System.nanoTime();
        PermissionDeniedException e = assertThrows(PermissionDeniedException.class,
                () -> pedidos.request(OBTENER, Map.of("clienteId", "cli-1")));
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms < 1500, "la denegacion se COMPITE con la respuesta: " + ms + " ms, no el timeout de 5 s");
        assertEquals(SUBJECT_OBTENER, e.subject());
        assertEquals(Operation.PUBLISH, e.operation());
        assertEquals(ErrorClass.PERMANENT, e.errorClass());
        assertTrue(e.getMessage().contains("calls"), e.getMessage());
        counter(m, "flux_rpc_client_requests_total{operation=\"" + OBTENER + "\",outcome=\"permanent\"}", 1);
        // El mismo caso con la concesion
        FluxBus facturacion = bus("facturacion-api");
        assertEquals("cli-1", facturacion.request(OBTENER, Map.of("clienteId", "cli-1")).get("clienteId").textValue());
        // Y la variante de facturacion-api sin ese permiso lo nota
        FluxBus sin = bus(env.options("fact-sin-obtener").tenantId("acme"));
        assertThrows(PermissionDeniedException.class, () -> sin.request(OBTENER, Map.of("clienteId", "cli-1")));
    }

    @Test
    @DisplayName("la deteccion de `respond()` sin permiso NO es una carrera: 25 arranques seguidos, todos fallan con PERMISSION_DENIED (el contador de callbacks se anota despues de registrar la denegacion)")
    void respondSinPermisoNoEsUnaCarrera() throws Exception {
        for (int i = 0; i < 25; i++) {
            try (FluxBus sin = FluxBus.connect(env.options("crm-sin-suscribir").tenantId("acme"))) {
                int n = i;
                assertThrows(PermissionDeniedException.class, () -> sin.respond(OBTENER, (d, c) -> Map.of()), "arranque " + n);
            }
        }
    }

    @Test
    @DisplayName("sin permiso de suscripcion al inbox privado las respuestas nunca llegan (core NATS no avisa): la llamada falla en milisegundos con PERMISSION_DENIED (subscribe), tambien la SEGUNDA")
    void sinPermisoDeInbox() throws Exception {
        FluxBus crm = bus("crm-api");
        crm.respond(OBTENER, (d, c) -> Map.of("clienteId", "cli-1", "nombre", "Ana", "email", "ana@example.com"));
        FluxBus sin = bus(env.options("fact-sin-inbox").tenantId("acme"));
        for (int i = 0; i < 2; i++) {
            long t0 = System.nanoTime();
            PermissionDeniedException e = assertThrows(PermissionDeniedException.class, () -> sin.request(OBTENER, Map.of("clienteId", "cli-1")));
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 1500, "no espera el timeout de 5 s: llamada " + (i + 1));
            assertEquals(Operation.SUBSCRIBE, e.operation());
            assertTrue(e.subject().startsWith("_INBOX.facturacion-api."), e.subject());
            assertTrue(e.getMessage().contains("inbox privado"), e.getMessage());
        }
    }

    @Test
    @DisplayName("un dueno SIN permiso de suscripcion no arranca mudo: respond() falla con PERMISSION_DENIED nombrando el subject, y la operacion NO queda atendida")
    void respondSinPermiso() throws Exception {
        CapturingLogger log = new CapturingLogger();
        FluxBus sin = bus(env.options("crm-sin-suscribir").tenantId("acme").logger(log));
        long t0 = System.nanoTime();
        PermissionDeniedException e = assertThrows(PermissionDeniedException.class, () -> sin.respond(OBTENER, (d, c) -> Map.of()));
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 3000);
        assertEquals(SUBJECT_OBTENER, e.subject());
        assertEquals(Operation.SUBSCRIBE, e.operation());
        assertTrue(e.getMessage().contains("owns") && e.getMessage().contains("RPC_UNAVAILABLE"), e.getMessage());
        // No quedo nada atendido: quien llama ve que no hay nadie
        FluxBus facturacion = bus("facturacion-api");
        assertThrows(RpcUnavailableException.class, () -> facturacion.request(OBTENER, Map.of("clienteId", "cli-1")));
        // Y se puede reintentar con el permiso arreglado (otra identidad) sin que la denegacion anterior lo condene
        FluxBus crm = bus("crm-api");
        crm.respond(OBTENER, (d, c) -> Map.of("clienteId", "cli-1", "nombre", "Ana", "email", "ana@example.com"));
        assertEquals("Ana", facturacion.request(OBTENER, Map.of("clienteId", "cli-1")).get("nombre").textValue());
    }

    @Test
    @DisplayName("sin el permiso de suscripcion al inbox privado las respuestas no llegan: el SDK nunca se suscribe a un inbox ajeno ni comun (A-101), y el broker lo confirma")
    void soloElInboxPropio() throws Exception {
        FluxBus crm = bus("crm-api");
        crm.respond(DESCUENTO, descuento());
        FluxBus pedidos = bus("pedidos-api");
        pedidos.request(DESCUENTO, peticion());
        JsonNode connz = env.connz();
        int vistos = 0;
        for (JsonNode c : connz.get("connections")) {
            String nombre = c.path("name").asText();
            for (JsonNode sub : c.path("subscriptions_list")) {
                String s = sub.asText();
                if (nombre.startsWith("pedidos-api@")) {
                    assertTrue(s.startsWith("_INBOX.pedidos-api."), "quien llama solo se suscribe a SU inbox: " + s);
                    assertFalse(s.equals("_INBOX.>") || s.equals("_INBOX.pedidos-api.>"), "ni un comodin comun: " + s);
                    vistos++;
                }
                if (nombre.startsWith("crm-api@")) {
                    assertFalse(s.startsWith("_INBOX."), "quien responde no se suscribe a ningun inbox: " + s);
                    assertFalse(s.contains(">") || s.contains("*"), "el subject EXACTO de la operacion, nunca rpc.<dominio>.>: " + s);
                    assertEquals(SUBJECT_DESCUENTO, s);
                    vistos++;
                }
            }
        }
        assertEquals(2, vistos, "una suscripcion de quien llama (su inbox) y una de quien responde (la operacion)");
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Lo que se comprueba de una respuesta (§4.3)
    // ═════════════════════════════════════════════════════════════════════════

    private static FluxEvent leer(Message m) {
        return Envelope.parseEvent(m.getData());
    }

    private static RpcEnvelope.RequestRef ref(FluxEvent e) {
        return new RpcEnvelope.RequestRef(OBTENER, e.id(), e.correlationid(), e.tenantid(), e.aggregateId(), e.partitionkey());
    }

    private static byte[] reply(FluxEvent request, String id) {
        return Envelope.serializeUnbounded(RpcEnvelope.buildReply(new RpcEnvelope.ReplyInput().request(ref(request)).id(id)
                .source("/produccion/crm-api").time(Envelope.formatTime(Instant.now())).producerversion("1.0.0").classification("internal")
                .data(Map.of("clienteId", "cli-1", "nombre", "Ana", "email", "ana@example.com"))
                .dataschema("https://schemas.internal/rpc/clientes/cliente/obtener/reply/1.0.0.json")));
    }

    private static byte[] error(RpcEnvelope.RequestRef ref, String id, RpcEnvelope.ErrorData data) {
        return Envelope.serializeUnbounded(RpcEnvelope.buildError(new RpcEnvelope.ErrorInput().request(ref).id(id)
                .source("/produccion/crm-api").time(Envelope.formatTime(Instant.now())).producerversion("1.0.0").error(data)));
    }

    /** Quien llama recibe `bytes` como respuesta y falla con un RPC_INVALID_REPLY cuyo mensaje contiene `fragmento`. */
    private void invalidReply(Function<FluxEvent, byte[]> respuesta, String fragmento) throws Exception {
        Dispatcher d = fake(SUBJECT_OBTENER, m -> {
            try {
                return respuesta.apply(leer(m));
            } catch (RuntimeException e) {
                return respuesta.apply(null);
            }
        });
        try {
            InMemoryMetrics metrics = new InMemoryMetrics();
            try (FluxBus facturacion = FluxBus.connect(env.options("facturacion-api").tenantId("acme").metrics(metrics))) {
                RpcInvalidReplyException e = assertThrows(RpcInvalidReplyException.class, () -> facturacion.request(OBTENER, Map.of("clienteId", "cli-1")),
                        fragmento);
                assertEquals("RPC_INVALID_REPLY", e.code());
                assertEquals(ErrorClass.PERMANENT, e.errorClass(), "PERMANENT: es un incidente, no se reintenta");
                assertTrue(e.getMessage().contains(fragmento), "esperaba `" + fragmento + "` en: " + e.getMessage());
                counter(metrics, "flux_rpc_client_requests_total{operation=\"" + OBTENER + "\",outcome=\"invalid_reply\"}", 1);
            }
        } finally {
            unfake(d);
        }
    }

    @Test
    @DisplayName("las cinco comprobaciones de una respuesta: no es un CloudEvent, otro type, otro causationid, otro correlationid, otro tenantid, error que no cumple su esquema → RPC_INVALID_REPLY")
    void cincoComprobaciones() throws Exception {
        // 1. No es un CloudEvent valido
        invalidReply(r -> "esto no es un evento".getBytes(), "no es un CloudEvent valido");
        invalidReply(r -> "{\"specversion\":\"1.0\"}".getBytes(), "no es un CloudEvent valido");
        // y el cuerpo que parece el acuse de un stream de JetStream se dice como lo que es (A: un stream que captura rpc.>)
        invalidReply(r -> "{\"stream\":\"EVT_X\",\"seq\":7}".getBytes(), "acuse de un stream");
        // 2. type
        invalidReply(r -> Envelope.serializeUnbounded(RpcEnvelope.buildReply(new RpcEnvelope.ReplyInput().request(
                        new RpcEnvelope.RequestRef("clientes.cliente.v1.calcular-descuento", r.id(), r.correlationid(), r.tenantid(), null, null))
                .id("r-1").source("/x").time(Envelope.formatTime(Instant.now())).producerversion("1.0.0").classification("internal")
                .data(Map.of("a", 1)))), "`type`");
        // 3. causationid
        invalidReply(r -> Envelope.serializeUnbounded(RpcEnvelope.buildReply(new RpcEnvelope.ReplyInput().request(
                        new RpcEnvelope.RequestRef(OBTENER, "otra-peticion", r.correlationid(), r.tenantid(), null, null))
                .id("r-1").source("/x").time(Envelope.formatTime(Instant.now())).producerversion("1.0.0").classification("internal")
                .data(Map.of("a", 1)))), "causationid");
        // 4. correlationid y tenantid
        invalidReply(r -> Envelope.serializeUnbounded(RpcEnvelope.buildReply(new RpcEnvelope.ReplyInput().request(
                        new RpcEnvelope.RequestRef(OBTENER, r.id(), "otra-cadena", r.tenantid(), null, null))
                .id("r-1").source("/x").time(Envelope.formatTime(Instant.now())).producerversion("1.0.0").classification("internal")
                .data(Map.of("a", 1)))), "correlationid");
        invalidReply(r -> Envelope.serializeUnbounded(RpcEnvelope.buildReply(new RpcEnvelope.ReplyInput().request(
                        new RpcEnvelope.RequestRef(OBTENER, r.id(), r.correlationid(), "globex", null, null))
                .id("r-1").source("/x").time(Envelope.formatTime(Instant.now())).producerversion("1.0.0").classification("internal")
                .data(Map.of("a", 1)))), "tenantid");
        // 5. un error cuyo data no cumple su esquema (class: poison no viaja)
        invalidReply(r -> {
            // el sobre de un error valido, con el data estropeado a mano
            byte[] ok = error(ref(r), "e-1", new RpcEnvelope.ErrorData("permanent", "X", "m", null));
            return new String(ok, StandardCharsets.UTF_8).replace("\"class\":\"permanent\"", "\"class\":\"poison\"").getBytes(StandardCharsets.UTF_8);
        }, "esquema");
    }

    @Test
    @DisplayName("EXCEPCION unica (§4.3.4): el error de una peticion ILEGIBLE —tenantid system y correlationid = su propio id— se acepta si su causationid es el id de la peticion; con otro causationid es RPC_INVALID_REPLY")
    void errorDePeticionIlegible() throws Exception {
        Dispatcher d = fake(SUBJECT_OBTENER, m -> {
            FluxEvent r = leer(m);
            String id = "e-" + System.nanoTime();
            return error(new RpcEnvelope.RequestRef(OBTENER, r.id(), id, "system", null, null), id,
                    new RpcEnvelope.ErrorData("permanent", "MALFORMED_JSON", "la peticion no es un CloudEvent valido", null));
        });
        try {
            FluxBus facturacion = bus("facturacion-api");
            RpcPermanentException e = assertThrows(RpcPermanentException.class, () -> facturacion.request(OBTENER, Map.of("clienteId", "cli-1")));
            assertEquals("MALFORMED_JSON", e.code(), "quien llama ve el codigo del sobre y no RPC_INVALID_REPLY");
        } finally {
            unfake(d);
        }
        // Mismo sobre pero atribuido a OTRA peticion: no se acepta
        Dispatcher d2 = fake(SUBJECT_OBTENER, m -> {
            String id = "e-" + System.nanoTime();
            return error(new RpcEnvelope.RequestRef(OBTENER, "no-es-mi-peticion", id, "system", null, null), id,
                    new RpcEnvelope.ErrorData("permanent", "MALFORMED_JSON", "x", null));
        });
        try {
            FluxBus facturacion = bus("facturacion-api");
            assertThrows(RpcInvalidReplyException.class, () -> facturacion.request(OBTENER, Map.of("clienteId", "cli-1")));
        } finally {
            unfake(d2);
        }
    }

    @Test
    @DisplayName("lo que NO es la excepcion sigue siendo invalido: tenantid system pero correlationid de la peticion; o correlationid propio con otro tenant")
    void laExcepcionEsEstrecha() throws Exception {
        invalidReply(r -> error(new RpcEnvelope.RequestRef(OBTENER, r.id(), r.correlationid(), "system", null, null), "e-1",
                new RpcEnvelope.ErrorData("permanent", "X", "m", null)), "tenantid");
        invalidReply(r -> error(new RpcEnvelope.RequestRef(OBTENER, r.id(), "e-2", "acme", null, null), "e-2",
                new RpcEnvelope.ErrorData("permanent", "X", "m", null)), "correlationid");
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Errores del handler: la clase y el codigo viajan
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("PermanentException y RetryableException viajan con SU clase, SU codigo, SU mensaje y retryAfterMs; un codigo mal formado se degrada a PERMANENT con aviso")
    void erroresDeLaAplicacion() throws Exception {
        CapturingLogger log = new CapturingLogger();
        InMemoryMetrics serverMetrics = new InMemoryMetrics();
        FluxBus crm = bus(env.options("crm-api").tenantId("acme").logger(log).metrics(serverMetrics));
        AtomicReference<RuntimeException> aLanzar = new AtomicReference<>();
        crm.respond(DESCUENTO, (d, c) -> {
            throw aLanzar.get();
        });
        FluxBus pedidos = bus("pedidos-api");

        aLanzar.set(new FluxErrors.PermanentException("el pedido ya esta cancelado", "PEDIDO_YA_CANCELADO"));
        RpcPermanentException p = assertThrows(RpcPermanentException.class, () -> pedidos.request(DESCUENTO, peticion()));
        assertEquals("PEDIDO_YA_CANCELADO", p.code());
        assertEquals("el pedido ya esta cancelado", p.getMessage());
        assertEquals(ErrorClass.PERMANENT, p.errorClass());

        aLanzar.set(new FluxErrors.RetryableException("proveedor caido", "PROVEEDOR_503", Duration.ofMillis(750), null));
        RpcRetryableException r = assertThrows(RpcRetryableException.class, () -> pedidos.request(DESCUENTO, peticion()));
        assertEquals("PROVEEDOR_503", r.code());
        assertEquals(Duration.ofMillis(750), r.retryAfter(), "retryAfterMs viaja");
        assertEquals(ErrorClass.RETRYABLE, r.errorClass());

        // Un handler de EVENTOS que llama a una operacion hereda la clasificacion sin hacer nada
        assertEquals(ErrorClass.RETRYABLE, new Classifier().classify(r).errorClass());
        assertEquals(Duration.ofMillis(750), new Classifier().classify(r).retryAfter());

        aLanzar.set(new FluxErrors.PermanentException("x", "codigo en minusculas"));
        assertEquals("PERMANENT", assertThrows(RpcPermanentException.class, () -> pedidos.request(DESCUENTO, peticion())).code());
        assertFalse(log.containing("no cumple").isEmpty(), "se AVISA al que responde: " + log.lines);

        aLanzar.set(new FluxErrors.PermanentException("   "));
        assertEquals("el servicio rechazo la peticion", assertThrows(RpcPermanentException.class, () -> pedidos.request(DESCUENTO, peticion())).getMessage());

        counter(serverMetrics, "flux_rpc_server_requests_total{operation=\"" + DESCUENTO + "\",outcome=\"permanent\"}", 3);
        counter(serverMetrics, "flux_rpc_server_requests_total{operation=\"" + DESCUENTO + "\",outcome=\"retryable\"}", 1);
    }

    @Test
    @DisplayName("una excepcion DESCONOCIDA responde permanent UNKNOWN con un mensaje generico (su texto va al log, NO a quien llama: puede llevar datos personales); una RECONOCIDA, su clase")
    void erroresDesconocidos() throws Exception {
        CapturingLogger log = new CapturingLogger();
        FluxBus crm = bus(env.options("crm-api").tenantId("acme").logger(log));
        AtomicReference<RuntimeException> aLanzar = new AtomicReference<>();
        crm.respond(DESCUENTO, (d, c) -> {
            throw aLanzar.get();
        });
        FluxBus pedidos = bus("pedidos-api");

        aLanzar.set(new IllegalStateException("fila duplicada: ana@example.com"));
        RpcPermanentException u = assertThrows(RpcPermanentException.class, () -> pedidos.request(DESCUENTO, peticion()));
        assertEquals("UNKNOWN", u.code());
        assertEquals("error interno del servicio", u.getMessage());
        assertFalse(u.getMessage().contains("ana@example.com"));
        assertTrue(log.lines.stream().anyMatch(l -> l.contains("ana@example.com")), "el texto SI va al log del que responde");

        // Una HTTP 503 que el clasificador RECONOCE: retryable, con mensaje generico
        aLanzar.set(new Classifier.HttpException(503, "upstream dijo ana@example.com", Duration.ofSeconds(2)));
        RpcRetryableException h = assertThrows(RpcRetryableException.class, () -> pedidos.request(DESCUENTO, peticion()));
        assertEquals("HTTP_503", h.code());
        assertEquals("dependencia no disponible", h.getMessage());
        assertEquals(Duration.ofSeconds(2), h.retryAfter());
        aLanzar.set(new Classifier.HttpException(404, "no esta"));
        RpcPermanentException n = assertThrows(RpcPermanentException.class, () -> pedidos.request(DESCUENTO, peticion()));
        assertEquals("HTTP_404", n.code());
        assertEquals("una dependencia rechazo la operacion", n.getMessage());
    }

    @Test
    @DisplayName("una respuesta que no es un objeto JSON es un fallo de quien responde: permanent INVALID_SCHEMA, no una respuesta rota")
    void respuestaQueNoEsUnObjeto() throws Exception {
        FluxBus crm = bus("crm-api");
        AtomicInteger cual = new AtomicInteger();
        crm.respond(DESCUENTO, (d, c) -> cual.getAndIncrement() == 0 ? "una cadena" : null);
        FluxBus pedidos = bus("pedidos-api");
        for (int i = 0; i < 2; i++) {
            RpcPermanentException e = assertThrows(RpcPermanentException.class, () -> pedidos.request(DESCUENTO, peticion()));
            assertEquals("INVALID_SCHEMA", e.code());
            assertTrue(e.getMessage().contains("no es un objeto"), e.getMessage());
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Plazos, reintentos
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("RPC_TIMEOUT: retryable y AMBIGUO (la operacion pudo ejecutarse), a los timeoutMs pedidos; la respuesta tardia se ignora sin ruido")
    void timeout() throws Exception {
        InMemoryMetrics m = new InMemoryMetrics();
        CapturingLogger log = new CapturingLogger();
        FluxBus crm = bus(env.options("crm-api").tenantId("acme").logger(log));
        AtomicInteger ejecuciones = new AtomicInteger();
        crm.respond(DESCUENTO, (d, c) -> {
            ejecuciones.incrementAndGet();
            Thread.sleep(900);
            return Map.of("descuentoCents", 1, "moneda", "EUR");
        });
        FluxBus pedidos = bus(env.options("pedidos-api").tenantId("acme").metrics(m));
        long t0 = System.nanoTime();
        RpcTimeoutException e = assertThrows(RpcTimeoutException.class,
                () -> pedidos.request(DESCUENTO, peticion(), new FluxRpc.RequestOptions().timeoutMs(300)));
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms >= 290 && ms < 700, "a su timeout, no al de la libreria: " + ms + " ms");
        assertEquals("RPC_TIMEOUT", e.code());
        assertEquals(300, e.timeoutMs());
        assertEquals(ErrorClass.RETRYABLE, e.errorClass());
        await("la operacion SI se ejecuto", 3_000, () -> ejecuciones.get() == 1);
        Thread.sleep(900); // la respuesta tardia llega a un inbox que ya nadie espera
        counter(m, "flux_rpc_client_requests_total{operation=\"" + DESCUENTO + "\",outcome=\"timeout\"}", 1);
        assertTrue(log.lines.stream().noneMatch(l -> l.startsWith("ERROR")), log.lines.toString());
    }

    @Test
    @DisplayName("un timeout MAYOR que la limpieza por defecto de la libreria (5 s) sigue siendo RPC_TIMEOUT, no RPC_UNAVAILABLE: la libreria cancela con la MISMA excepcion que un `no responders` (A-203)")
    void timeoutMayorQueLaLimpiezaDeLaLibreria() throws Exception {
        FluxBus crm = bus("crm-api");
        crm.respond(DESCUENTO, (d, c) -> {
            Thread.sleep(8000);
            return Map.of("descuentoCents", 1, "moneda", "EUR");
        });
        FluxBus pedidos = bus("pedidos-api");
        long t0 = System.nanoTime();
        RpcTimeoutException e = assertThrows(RpcTimeoutException.class,
                () -> pedidos.request(DESCUENTO, peticion(), new FluxRpc.RequestOptions().timeoutMs(7000)));
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms >= 6900 && ms < 8000, "el timeout es el de la llamada: " + ms + " ms");
        assertEquals(7000, e.timeoutMs());
    }

    @Test
    @DisplayName("cada reintento reutiliza el MISMO id y los MISMOS bytes (time incluido): RPC_TIMEOUT no dice que la operacion no se ejecuto")
    void reintentosConElMismoId() throws Exception {
        List<byte[]> vistas = new CopyOnWriteArrayList<>();
        Dispatcher d = fake(SUBJECT_OBTENER, m -> {
            vistas.add(m.getData());
            return vistas.size() < 3 ? null : reply(leer(m), "r-1");
        });
        try {
            FluxBus facturacion = bus("facturacion-api");
            JsonNode out = facturacion.request(OBTENER, Map.of("clienteId", "cli-1"), new FluxRpc.RequestOptions().timeoutMs(300).retries(2));
            assertEquals("cli-1", out.get("clienteId").textValue());
            assertEquals(3, vistas.size());
            assertTrue(java.util.Arrays.equals(vistas.get(0), vistas.get(1)) && java.util.Arrays.equals(vistas.get(1), vistas.get(2)),
                    "los tres intentos son BYTE A BYTE el mismo mensaje");
            // Un error permanente NO se reintenta
            vistas.clear();
            unfake(d);
            Dispatcher d2 = fake(SUBJECT_OBTENER, m -> {
                vistas.add(m.getData());
                FluxEvent r = leer(m);
                return error(ref(r), "e-1", new RpcEnvelope.ErrorData("permanent", "NO", "no", null));
            });
            assertThrows(RpcPermanentException.class, () -> facturacion.request(OBTENER, Map.of("clienteId", "cli-1"), new FluxRpc.RequestOptions().retries(5)));
            assertEquals(1, vistas.size(), "un error permanente no se reintenta");
            unfake(d2);
        } catch (Throwable t) {
            throw t;
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Tamanos
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("una peticion que no cabe (min(1 MiB, max_payload)) falla ANTES de abrir nada con RPC_MESSAGE_TOO_LARGE; una respuesta que no cabe, la contesta quien responde con ese error")
    void tamanos() throws Exception {
        InMemoryMetrics m = new InMemoryMetrics();
        Tap peticiones = new Tap(SUBJECT_DESCUENTO);
        FluxBus crm = bus("crm-api");
        AtomicInteger recibidas = new AtomicInteger();
        crm.respond(DESCUENTO, (d, c) -> {
            recibidas.incrementAndGet();
            // totalCents == 7 pide una respuesta enorme
            return Map.of("relleno", "x".repeat(d.get("totalCents").intValue() == 7 ? 1_100_000 : 10), "descuentoCents", 1, "moneda", "EUR");
        });
        FluxBus pedidos = bus(env.options("pedidos-api").tenantId("acme").metrics(m));
        RpcMessageTooLargeException e = assertThrows(RpcMessageTooLargeException.class,
                () -> pedidos.request(DESCUENTO, Map.of("clienteId", "cli-1", "totalCents", 1, "moneda", "EUR", "relleno", "x".repeat(1_100_000))));
        assertEquals("RPC_MESSAGE_TOO_LARGE", e.code());
        assertEquals(ErrorClass.PERMANENT, e.errorClass());
        assertEquals(1_048_576, e.limit());
        Thread.sleep(300);
        assertTrue(peticiones.messages.isEmpty() && recibidas.get() == 0, "no salio nada a la red");
        counter(m, "flux_rpc_client_requests_total{operation=\"" + DESCUENTO + "\",outcome=\"permanent\"}", 1);
        // Una grande que SI cabe (900 KB) viaja
        JsonNode ok = pedidos.request(DESCUENTO, Map.of("clienteId", "cli-1", "totalCents", 1, "moneda", "EUR", "relleno", "x".repeat(900_000)));
        assertEquals(10, ok.get("relleno").textValue().length());
        assertEquals(1, recibidas.get());
        // La respuesta que no cabe: la contesta quien responde con el error
        RpcPermanentException r = assertThrows(RpcPermanentException.class,
                () -> pedidos.request(DESCUENTO, Map.of("clienteId", "cli-1", "totalCents", 7, "moneda", "EUR")));
        assertEquals("RPC_MESSAGE_TOO_LARGE", r.code(), "quien responde contesta con el error: publicarla lanzaria en el y quien llama solo veria un timeout (A-65)");
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Firma y ventana de 300 s
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("con la firma en `require`: la peticion y la respuesta verifican; una peticion con `time` a mas de 300 s es STALE_REQUEST, y una SIN firma, MISSING_SIGNATURE")
    void conFirma() throws Exception {
        Signing.KeyPairPem ped = Signing.generateKeyPair();
        Signing.KeyPairPem crmKey = Signing.generateKeyPair();
        InMemoryMetrics serverMetrics = new InMemoryMetrics();
        FluxBus crm = bus(env.options("crm-api").tenantId("acme").metrics(serverMetrics)
                .signing(new Signing.SigningOptions().privateKeyPem(crmKey.privateKeyPem()).keyId("crm-1")
                        .publicKey("ped-1", ped.publicKeyPem()).verify(Signing.VerificationMode.REQUIRE)));
        crm.respond(DESCUENTO, descuento());
        Signing.SigningOptions pedSigning = new Signing.SigningOptions().privateKeyPem(ped.privateKeyPem()).keyId("ped-1")
                .publicKey("crm-1", crmKey.publicKeyPem()).verify(Signing.VerificationMode.REQUIRE);
        FluxBus pedidos = bus(env.options("pedidos-api").tenantId("acme").signing(pedSigning));
        assertEquals(1000, pedidos.request(DESCUENTO, peticion()).get("descuentoCents").intValue());

        // Una peticion firmada pero VIEJA (se construye a mano y se firma con la clave de quien llama)
        Signing.Signer firmante = Signing.createSigner(new Signing.SigningOptions().privateKeyPem(ped.privateKeyPem()).keyId("ped-1"));
        String id = Protocol.uuidV7();
        FluxEvent vieja = RpcEnvelope.buildRequest(new RpcEnvelope.RequestInput().operation(DESCUENTO).tenantId("acme").id(id)
                .source("/produccion/pedidos-api").time(Envelope.formatTime(Instant.now().minusSeconds(600))).correlationid(id)
                .producerversion("1.0.0").classification("internal").data(peticion())
                .dataschema("https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json"));
        Message m = env.ops().request(SUBJECT_DESCUENTO, Envelope.serializeUnbounded(firmante.sign(vieja)), Duration.ofSeconds(5));
        FluxEvent e = Envelope.parseEvent(m.getData());
        assertEquals("com.flux.rpc.error.v1", e.type());
        assertEquals("STALE_REQUEST", e.data().get("code").textValue());
        assertEquals("permanent", e.data().get("class").textValue());
        assertEquals(id, e.causationid());
        assertTrue(e.signature() != null, "con la firma en require, quien responde FIRMA tambien sus errores");
        counter(serverMetrics, "flux_rpc_server_requests_total{operation=\"" + DESCUENTO + "\",outcome=\"stale_request\"}", 1);

        // El futuro lejano tambien (la ventana es +-300 s)
        FluxEvent futura = RpcEnvelope.buildRequest(new RpcEnvelope.RequestInput().operation(DESCUENTO).tenantId("acme").id(Protocol.uuidV7())
                .source("/produccion/pedidos-api").time(Envelope.formatTime(Instant.now().plusSeconds(600))).correlationid("c-1")
                .producerversion("1.0.0").classification("internal").data(peticion())
                .dataschema("https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json"));
        FluxEvent e2 = Envelope.parseEvent(env.ops().request(SUBJECT_DESCUENTO, Envelope.serializeUnbounded(firmante.sign(futura)), Duration.ofSeconds(5)).getData());
        assertEquals("STALE_REQUEST", e2.data().get("code").textValue());

        // Sin firma
        FluxEvent sinFirma = RpcEnvelope.buildRequest(new RpcEnvelope.RequestInput().operation(DESCUENTO).tenantId("acme").id(Protocol.uuidV7())
                .source("/produccion/pedidos-api").time(Envelope.formatTime(Instant.now())).correlationid("c-2")
                .producerversion("1.0.0").classification("internal").data(peticion())
                .dataschema("https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json"));
        FluxEvent e3 = Envelope.parseEvent(env.ops().request(SUBJECT_DESCUENTO, Envelope.serializeUnbounded(sinFirma), Duration.ofSeconds(5)).getData());
        assertEquals("MISSING_SIGNATURE", e3.data().get("code").textValue());
        counter(serverMetrics, "flux_rpc_server_requests_total{operation=\"" + DESCUENTO + "\",outcome=\"invalid_signature\"}", 1);
    }

    @Test
    @DisplayName("SIN firma en `require`, la ventana de 300 s NO se aplica (solo con `require`): una peticion vieja se atiende")
    void sinRequireNoHayVentana() throws Exception {
        FluxBus crm = bus("crm-api");
        crm.respond(DESCUENTO, descuento());
        String id = Protocol.uuidV7();
        FluxEvent vieja = RpcEnvelope.buildRequest(new RpcEnvelope.RequestInput().operation(DESCUENTO).tenantId("acme").id(id)
                .source("/produccion/pedidos-api").time(Envelope.formatTime(Instant.now().minusSeconds(3600))).correlationid(id)
                .producerversion("1.0.0").classification("internal").data(peticion())
                .dataschema("https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json"));
        FluxEvent r = Envelope.parseEvent(env.ops().request(SUBJECT_DESCUENTO, Envelope.serializeUnbounded(vieja), Duration.ofSeconds(5)).getData());
        assertEquals("com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1", r.type());
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Peticiones que no se pueden atender
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("una peticion SIN subject de respuesta es POISON (nadie espera el resultado de una orden) y NO se ejecuta; una ILEGIBLE se contesta con un error permanent y se cita su id si es valido")
    void peticionesQueNoSePuedenAtender() throws Exception {
        List<String> poison = new CopyOnWriteArrayList<>();
        InMemoryMetrics m = new InMemoryMetrics();
        FluxBus crm = bus(env.options("crm-api").tenantId("acme").metrics(m).onPoison(info -> poison.add(info.subject() + ":" + ((FluxErrors.FluxException) info.error()).code() + ":" + info.raw().length)));
        AtomicInteger ejecuciones = new AtomicInteger();
        crm.respond(DESCUENTO, (d, c) -> {
            ejecuciones.incrementAndGet();
            return Map.of("descuentoCents", 1, "moneda", "EUR");
        });
        Connection ops = env.ops();
        // Sin reply
        ops.publish(SUBJECT_DESCUENTO, "{}".getBytes());
        await("POISON", 5_000, () -> poison.size() == 1);
        assertEquals("rpc." + DESCUENTO + ":NO_REPLY_SUBJECT:0", poison.get(0), "`raw` va vacio: una peticion puede llevar datos personales");
        assertEquals(0, ejecuciones.get());
        // Ilegible, con id citable
        FluxEvent e = Envelope.parseEvent(ops.request(SUBJECT_DESCUENTO, "{\"id\":\"mi-id-7\",\"no-es\":\"un cloudevent\"}".getBytes(), Duration.ofSeconds(5)).getData());
        assertEquals("com.flux.rpc.error.v1", e.type());
        assertEquals("mi-id-7", e.causationid(), "su id, porque es una cadena valida");
        assertEquals("system", e.tenantid());
        assertEquals(e.id(), e.correlationid(), "sin sobre no hay nada que heredar");
        assertEquals("permanent", e.data().get("class").textValue());
        assertEquals("UNSUPPORTED_SPECVERSION", e.data().get("code").textValue(), "el codigo del sobre: el mismo que da parseEvent");
        // Ilegible, sin id citable: no hay causationid
        FluxEvent e2 = Envelope.parseEvent(ops.request(SUBJECT_DESCUENTO, "no es json".getBytes(), Duration.ofSeconds(5)).getData());
        assertNull(e2.causationid());
        assertEquals("MALFORMED_JSON", e2.data().get("code").textValue());
        assertEquals(3, poison.size(), poison.toString());
        assertEquals(0, ejecuciones.get(), "nada de esto se ejecuto");
        counter(m, "flux_rpc_server_requests_total{operation=\"" + DESCUENTO + "\",outcome=\"poison\"}", 3);
        // Un type que no es el de la peticion de esta operacion: INVALID_RPC_CALL
        String id = Protocol.uuidV7();
        FluxEvent otra = RpcEnvelope.buildRequest(new RpcEnvelope.RequestInput().operation(OBTENER).tenantId("acme").id(id)
                .source("/x").time(Envelope.formatTime(Instant.now())).correlationid(id).producerversion("1.0.0").classification("internal")
                .data(Map.of("clienteId", "c")).dataschema("https://schemas.internal/rpc/clientes/cliente/obtener/request/1.0.0.json"));
        FluxEvent e3 = Envelope.parseEvent(ops.request(SUBJECT_DESCUENTO, Envelope.serializeUnbounded(otra), Duration.ofSeconds(5)).getData());
        assertEquals("INVALID_RPC_CALL", e3.data().get("code").textValue());
        assertEquals(id, e3.causationid());
        assertEquals(0, ejecuciones.get());
        counter(m, "flux_rpc_server_requests_total{operation=\"" + DESCUENTO + "\",outcome=\"poison\"}", 4);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Parada ordenada
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("stop(): deja de escuchar al instante (las nuevas dan RPC_UNAVAILABLE), ESPERA al handler en vuelo y su respuesta llega; no se pierde lo que se atiende (A-62)")
    void paradaOrdenada() throws Exception {
        FluxBus crm = bus("crm-api");
        CountDownLatch dentro = new CountDownLatch(1);
        FluxRpc.Responder r = crm.respond(DESCUENTO, (d, c) -> {
            dentro.countDown();
            Thread.sleep(1500);
            return Map.of("descuentoCents", 77, "moneda", "EUR");
        });
        FluxBus pedidos = bus("pedidos-api");
        AtomicReference<Object> resultado = new AtomicReference<>();
        Thread llamada = new Thread(() -> {
            try {
                resultado.set(pedidos.request(DESCUENTO, peticion(), new FluxRpc.RequestOptions().timeoutMs(10_000)));
            } catch (Throwable t) {
                resultado.set(t);
            }
        });
        llamada.start();
        assertTrue(dentro.await(5, TimeUnit.SECONDS));
        long t0 = System.nanoTime();
        AtomicReference<Long> paro = new AtomicReference<>();
        Thread parada = new Thread(() -> {
            r.stop();
            paro.set((System.nanoTime() - t0) / 1_000_000);
        });
        parada.start();
        Thread.sleep(300);
        long u0 = System.nanoTime();
        assertThrows(RpcUnavailableException.class, () -> pedidos.request(DESCUENTO, peticion()),
                "parandose, las peticiones nuevas dan `no responders` al instante");
        assertTrue((System.nanoTime() - u0) / 1_000_000 < 500);
        llamada.join(10_000);
        parada.join(10_000);
        assertInstanceOf(JsonNode.class, resultado.get(), "la que estaba en vuelo SE ATIENDE: " + resultado.get());
        assertEquals(77, ((JsonNode) resultado.get()).get("descuentoCents").intValue());
        assertTrue(paro.get() >= 1300, "stop() espero al handler en vuelo: " + paro.get() + " ms");
    }

    @Test
    @DisplayName("bus.close() hace lo mismo: primero para a los que responden y espera lo que esta en vuelo, y SOLO entonces cierra la conexion")
    void cerrarElBusEsperaLoEnVuelo() throws Exception {
        FluxBus crm = FluxBus.connect(env.options("crm-api").tenantId("acme"));
        CountDownLatch dentro = new CountDownLatch(1);
        crm.respond(DESCUENTO, (d, c) -> {
            dentro.countDown();
            Thread.sleep(1000);
            return Map.of("descuentoCents", 5, "moneda", "EUR");
        });
        FluxBus pedidos = bus("pedidos-api");
        AtomicReference<Object> resultado = new AtomicReference<>();
        Thread llamada = new Thread(() -> {
            try {
                resultado.set(pedidos.request(DESCUENTO, peticion(), new FluxRpc.RequestOptions().timeoutMs(10_000)));
            } catch (Throwable t) {
                resultado.set(t);
            }
        });
        llamada.start();
        assertTrue(dentro.await(5, TimeUnit.SECONDS));
        crm.close();
        llamada.join(10_000);
        assertInstanceOf(JsonNode.class, resultado.get(), "close() con un handler en vuelo NO pierde su respuesta: " + resultado.get());
    }

    @Test
    @DisplayName("mas de maxConcurrent peticiones a la vez: el exceso espera en la cola y se atiende despues (no se pierde ni se rechaza)")
    void topeDeConcurrencia() throws Exception {
        FluxBus crm = bus("crm-api");
        AtomicInteger dentro = new AtomicInteger();
        AtomicInteger maximo = new AtomicInteger();
        crm.respond(DESCUENTO, (d, c) -> {
            int n = dentro.incrementAndGet();
            maximo.accumulateAndGet(n, Math::max);
            Thread.sleep(150);
            dentro.decrementAndGet();
            return Map.of("descuentoCents", 1, "moneda", "EUR");
        }, new FluxRpc.RespondOptions().maxConcurrent(3));
        FluxBus pedidos = bus("pedidos-api");
        List<Thread> hilos = new ArrayList<>();
        AtomicInteger bien = new AtomicInteger();
        for (int i = 0; i < 12; i++) {
            Thread t = new Thread(() -> {
                try {
                    pedidos.request(DESCUENTO, peticion(), new FluxRpc.RequestOptions().timeoutMs(10_000));
                    bien.incrementAndGet();
                } catch (Throwable ignored) {
                    // se cuenta por `bien`
                }
            });
            hilos.add(t);
            t.start();
        }
        for (Thread t : hilos) {
            t.join(15_000);
        }
        assertEquals(12, bien.get());
        assertTrue(maximo.get() <= 3, "nunca mas de 3 handlers a la vez: " + maximo.get());
        assertTrue(maximo.get() >= 2, "y si hay concurrencia: " + maximo.get());
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Validacion previa a la red
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("lo que se sabe sin red falla sin red y en orden: RESERVED_DOMAIN, INVALID_RPC_CALL (operacion, timeoutMs, data), registro doble; nada sale al broker")
    void validacionPrevia() throws Exception {
        Tap todo = new Tap("rpc.>");
        FluxBus pedidos = bus("pedidos-api");
        FluxBus crm = bus("crm-api");
        assertEquals("RESERVED_DOMAIN", assertThrows(ReservedDomainException.class, () -> pedidos.request("rpc.clientes.cliente.v1.obtener", Map.of())).code());
        assertEquals("RESERVED_DOMAIN", assertThrows(ReservedDomainException.class, () -> pedidos.request("system.cliente.v1.obtener", Map.of())).code());
        for (String mala : List.of("clientes.cliente.obtener", "Clientes.cliente.v1.obtener", "clientes.cliente.v1.obtener.mas", "clientes.cliente.v1.*", "clientes.cliente.v0.obtener")) {
            assertEquals("INVALID_RPC_CALL", assertThrows(InvalidRpcCallException.class, () -> pedidos.request(mala, Map.of())).code(), mala);
        }
        for (int t : new int[] {0, -5, 25_001}) {
            assertEquals("INVALID_RPC_CALL", assertThrows(InvalidRpcCallException.class,
                    () -> pedidos.request(DESCUENTO, peticion(), new FluxRpc.RequestOptions().timeoutMs(t))).code(), "timeoutMs " + t);
        }
        assertThrows(InvalidRpcCallException.class, () -> pedidos.request(DESCUENTO, List.of(1, 2)));
        assertThrows(InvalidRpcCallException.class, () -> pedidos.request(DESCUENTO, "texto"));
        assertThrows(InvalidRpcCallException.class, () -> pedidos.request(DESCUENTO, peticion(), new FluxRpc.RequestOptions().retries(-1)));
        assertThrows(InvalidRpcCallException.class, () -> pedidos.request(DESCUENTO, peticion(), new FluxRpc.RequestOptions().aggregateId("")));
        assertThrows(InvalidRpcCallException.class, () -> crm.respond("clientes.cliente.obtener", (d, c) -> Map.of()));
        assertThrows(InvalidRpcCallException.class, () -> crm.respond(DESCUENTO, null));
        crm.respond(DESCUENTO, descuento());
        InvalidRpcCallException doble = assertThrows(InvalidRpcCallException.class, () -> crm.respond(DESCUENTO, descuento()));
        assertTrue(doble.getMessage().contains("ya hay un `respond()`"), doble.getMessage());
        Thread.sleep(300);
        assertTrue(todo.messages.isEmpty(), "nada de lo anterior salio a la red: " + todo.messages.size());
    }

    // ═════════════════════════════════════════════════════════════════════════
    // L3 con el bundle real
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("L3 estricta con el bundle REAL: una peticion invalida falla ANTES de salir (INVALID_SCHEMA, dice «peticion»); una respuesta invalida del handler llega como error permanent INVALID_SCHEMA que dice de quien es el fallo")
    void validacionL3() throws Exception {
        SchemaBundle bundle = SchemaBundle.fromPath(Repo.root().resolve("schemas/bundle.json"));
        Validation.Options v = new Validation.Options().mode(Validation.Mode.STRICT).bundle(bundle);
        InMemoryMetrics client = new InMemoryMetrics();
        InMemoryMetrics server = new InMemoryMetrics();
        Tap tap = new Tap(SUBJECT_DESCUENTO);
        FluxBus crm = bus(env.options("crm-api").tenantId("acme").validation(v).metrics(server));
        AtomicInteger malas = new AtomicInteger();
        crm.respond(DESCUENTO, (d, c) -> malas.incrementAndGet() == 1
                ? Map.of("descuentoCents", "no es un entero", "moneda", "EUR")
                : Map.of("descuentoCents", 10, "moneda", "EUR"));
        FluxBus pedidos = bus(env.options("pedidos-api").tenantId("acme").validation(v).metrics(client));

        // Peticion invalida: no sale
        SchemaValidationException e = assertThrows(SchemaValidationException.class,
                () -> pedidos.request(DESCUENTO, Map.of("clienteId", "cli-1", "totalCents", -5, "moneda", "euro", "sobra", 1)));
        assertEquals("INVALID_SCHEMA", e.code());
        assertInstanceOf(RpcErrors.RpcSchemaException.class, e);
        assertTrue(e.getMessage().contains("la peticion"), e.getMessage());
        assertTrue(e.errors().size() >= 3, "TODOS los errores: " + e.errors());
        assertFalse(e.getMessage().contains("euro"), "rutas, nunca valores: " + e.getMessage());
        Thread.sleep(200);
        assertTrue(tap.messages.isEmpty());
        counter(client, "flux_rpc_client_requests_total{operation=\"" + DESCUENTO + "\",outcome=\"invalid_schema\"}", 1);

        // Respuesta invalida del handler: el que responde la valida y contesta con el error
        RpcPermanentException r = assertThrows(RpcPermanentException.class, () -> pedidos.request(DESCUENTO, peticion()));
        assertEquals("INVALID_SCHEMA", r.code());
        assertTrue(r.getMessage().contains("la respuesta no cumple") && r.getMessage().contains("de quien responde"), r.getMessage());
        counter(server, "flux_rpc_server_requests_total{operation=\"" + DESCUENTO + "\",outcome=\"invalid_schema\"}", 1);
        // Y la siguiente, valida, pasa
        assertEquals(10, pedidos.request(DESCUENTO, peticion()).get("descuentoCents").intValue());

        // Una peticion invalida que llega por otro lado (sin L3 en quien llama) la rechaza quien responde con el mismo codigo
        FluxBus sinL3 = bus("pedidos-api");
        RpcPermanentException p = assertThrows(RpcPermanentException.class,
                () -> sinL3.request(DESCUENTO, Map.of("clienteId", "cli-1", "totalCents", -5, "moneda", "EUR")));
        assertEquals("INVALID_SCHEMA", p.code());
        assertTrue(p.getMessage().contains("la peticion no cumple"), p.getMessage());
        // En modo estricto una operacion que NO esta en el bundle de quien responde se dice al ARRANCAR
        assertThrows(SchemaNotFoundException.class, () -> crm.respond("clientes.cliente.v1.no-existe", (d, c) -> Map.of()));
    }

    @Test
    @DisplayName("sin bundle ni schemaBaseUrl no hay dataschema que derivar: se dice, en vez de inventar un host")
    void sinEsquema() throws Exception {
        FluxBus.ConnectOptions o = env.options("pedidos-api").tenantId("acme");
        FluxBus sin = bus(new FluxBus.ConnectOptions().servers(env.url()).service("pedidos-api").environment("produccion").version("1.0.0")
                .tls(new TlsOptions().caFile(env.pki.ca.toString()))
                .credentials(new FluxBus.Credentials().nkeySeed(env.identity("pedidos-api").seed())));
        FluxBus.FluxBusException e = assertThrows(FluxBus.FluxBusException.class, () -> sin.request(DESCUENTO, peticion()));
        assertTrue(e.getMessage().contains("dataschema"), e.getMessage());
        assertNotNull(o);
    }

    @SuppressWarnings("unused")
    private static void unused(ObjectNode n) {
        assertNotNull(n);
    }
}
