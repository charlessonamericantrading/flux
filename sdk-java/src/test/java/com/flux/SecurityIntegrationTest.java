/*
 * Seguridad EJECUTADA contra un nats-server REAL — plan v1.1, Fase 1 (y Fase 6, «Seguridad en Java»).
 *
 * El SDK vive bajo los permisos EXACTOS que genera `scripts/gen-acl.mjs` de `examples/services.json`, con los streams y los
 * durables que crea `flux provision` con credenciales de operacion: no bajo un broker abierto, que no probaria nada de
 * seguridad. Cada comprobacion se hace EN LOS DOS SENTIDOS: lo concedido funciona y lo que se quita se NOTA, por el canal que
 * corresponde a su fila de 06-security.md §3.7.
 *
 *   NATS_SERVER=/ruta/a/nats-server FLUX_REQUIRE_BROKER=1 mvn test -Dtest=SecurityIntegrationTest
 *
 * (necesita `node`, `openssl` y `npm install` en `cli/`). Sin broker las pruebas se saltan con aviso, salvo FLUX_REQUIRE_BROKER=1.
 */
package com.flux;

import static com.flux.TestSupport.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.flux.SecurityErrors.NotProvisionedException;
import com.flux.SecurityErrors.PermissionDeniedException;
import com.flux.SecurityErrors.ResourceKind;
import com.flux.TestSupport.CapturingLogger;
import com.flux.broker.SecureEnv;
import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerInfo;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SecurityIntegrationTest {

    private static final String SUBJECT = "pedidos.pedido.v1.creado";
    private static final String DURABLE = "facturacion-api__pedidos_pedido_v1_creado";
    private static final String PULL = "$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS." + DURABLE;
    private static final String ACK = "$JS.ACK.EVT_PEDIDOS." + DURABLE + ".>";
    private static final String CONSUMER_INFO = "$JS.API.CONSUMER.INFO.EVT_PEDIDOS." + DURABLE;
    private static final String DLQ_INFO = "$JS.API.STREAM.INFO.DLQ_PEDIDOS";

    private static SecureEnv env;

    @BeforeAll
    static void start() throws Exception {
        env = SecureEnv.builder()
                // facturacion-api, cada vez con UN permiso de menos
                .variant("fact-sin-pull", "facturacion-api", List.of(PULL), List.of())
                .variant("fact-sin-ack", "facturacion-api", List.of(ACK), List.of())
                .variant("fact-sin-info-consumidor", "facturacion-api", List.of(CONSUMER_INFO), List.of())
                .variant("fact-sin-info-dlq", "facturacion-api", List.of(DLQ_INFO), List.of())
                // pedidos-api sin poder PUBLICAR en su propio dominio (pero si consultar su stream)
                .variant("ped-sin-publicar", "pedidos-api", List.of("pedidos.>"), List.of())
                .build();
    }

    @AfterAll
    static void stop() {
        if (env != null) {
            env.close();
        }
    }

    private static FluxEvent publishEvent(FluxBus bus, String subject) {
        return bus.publish(subject, Map.of("pedidoId", "ped-" + System.nanoTime()));
    }

    // ─── a. el ciclo, de extremo a extremo, bajo permisos exactos ────────────

    @Test
    @DisplayName("a. publicar y consumir de extremo a extremo con inbox privado y permisos exactos, SIN CREATE y sin ninguna violacion en el log del servidor")
    void cicloDeExtremoAExtremo() throws Exception {
        int antes = env.server.violations().size();
        CapturingLogger log = new CapturingLogger();
        List<FluxEvent> recibidos = new CopyOnWriteArrayList<>();
        try (FluxBus pedidos = FluxBus.connect(env.options("pedidos-api").logger(log));
             FluxBus facturacion = FluxBus.connect(env.options("facturacion-api").logger(log))) {
            facturacion.subscribe(SUBJECT, (ctx, event, delivery) -> recibidos.add(event));
            FluxEvent enviado = publishEvent(pedidos, SUBJECT);
            await("el evento llega", 10_000, () -> !recibidos.isEmpty());
            assertEquals(enviado.id(), recibidos.get(0).id());
            assertEquals("/produccion/pedidos-api", recibidos.get(0).source());

            // El inbox PRIVADO, mirado EN EL BROKER: ninguna suscripcion de la conexion cae fuera de `_INBOX.<servicio>.`
            JsonNode connz = env.connz();
            int mirados = 0;
            for (JsonNode c : connz.get("connections")) {
                String nombre = c.path("name").asText();
                if (!nombre.startsWith("facturacion-api@") && !nombre.startsWith("pedidos-api@")) {
                    continue;
                }
                mirados++;
                String servicio = nombre.substring(0, nombre.indexOf('@'));
                for (JsonNode sub : c.get("subscriptions_list")) {
                    assertTrue(sub.asText().startsWith("_INBOX." + servicio + "."),
                            servicio + " tiene una suscripcion fuera de su inbox privado: " + sub);
                }
            }
            assertEquals(2, mirados, "las dos conexiones del SDK figuran en el broker");
        }
        // «Info antes de crear»: con los permisos de produccion (sin CREATE) cada arranque NO deja una violacion.
        assertEquals(antes, env.server.violations().size(), "violaciones nuevas: " + env.server.violations());
        assertTrue(log.lines.stream().noneMatch(l -> l.startsWith("ERROR")), "el SDK no debe quejarse: " + log.lines);
    }

    @Test
    @DisplayName("a. un POISON o un error PERMANENT va a la DLQ bajo los permisos exactos (el dueño de la DLQ la lee con su durable)")
    void laDlqFuncionaBajoPermisosExactos() throws Exception {
        List<FluxEvent> vistos = new CopyOnWriteArrayList<>();
        try (FluxBus pedidos = FluxBus.connect(env.options("pedidos-api"));
             FluxBus facturacion = FluxBus.connect(env.options("facturacion-api"))) {
            // Un consumidor de otro durable para no mezclar con los demas tests: `cancelado`.
            facturacion.subscribe("pedidos.pedido.v1.cancelado", (ctx, event, delivery) -> {
                vistos.add(event);
                throw new FluxErrors.PermanentException("pedido ya cancelado", "PEDIDO_YA_CANCELADO");
            });
            FluxEvent e = publishEvent(pedidos, "pedidos.pedido.v1.cancelado");
            await("el handler corre", 10_000, () -> !vistos.isEmpty());
            // pedidos-api lee DLQ_PEDIDOS con su durable `pedidos-api__dlq_pedidos`
            Connection ops = env.ops();
            JetStreamManagement jsm = ops.jetStreamManagement();
            await("el evento llega a la DLQ", 10_000, () -> {
                try {
                    return jsm.getStreamInfo("DLQ_PEDIDOS").getStreamState().getMsgCount() >= 1;
                } catch (Exception ex) {
                    return false;
                }
            });
            var msg = jsm.getLastMessage("DLQ_PEDIDOS", "dlq.pedidos.pedido.v1.cancelado");
            FluxEvent dlq = Envelope.parseEvent(msg.getData());
            assertEquals(e.id(), dlq.id(), "la DLQ conserva el id original");
            assertEquals(FluxEvent.DlqReason.PERMANENT, dlq.dlqreason());
            assertTrue(dlq.dlqerror().startsWith("PEDIDO_YA_CANCELADO:"), dlq.dlqerror());
        }
    }

    // ─── b. publicar en un dominio ajeno ─────────────────────────────────────

    @Test
    @DisplayName("b. publicar en el dominio de otro falla en milisegundos con PERMISSION_DENIED que nombra el subject, no tras un timeout")
    void publicarEnUnDominioAjeno() throws Exception {
        CapturingLogger log = new CapturingLogger();
        try (FluxBus pedidos = FluxBus.connect(env.options("pedidos-api").logger(log))) {
            // pedidos-api NO es dueño de `facturacion`: ni siquiera puede consultar su stream.
            long t0 = System.nanoTime();
            PermissionDeniedException e = assertThrows(PermissionDeniedException.class,
                    () -> pedidos.publish("facturacion.factura.v2.emitida", Map.of("facturaId", "f-1")));
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms < 1500, "debe fallar en tiempo util, no por el timeout de 2 s de la libreria: " + ms + " ms");
            assertEquals("PERMISSION_DENIED", e.code());
            assertEquals(ErrorClass.PERMANENT, e.errorClass());
            assertTrue(e.subject().startsWith("$JS.API.STREAM.INFO.EVT_FACTURACION"), e.subject());
            assertTrue(e.getMessage().contains("pedidos-api"), "nombra al servicio: " + e.getMessage());
            assertTrue(e.getMessage().contains("$JS.API.STREAM.INFO.EVT_FACTURACION"), e.getMessage());
            // Un handler que la provoque va a la DLQ en la primera entrega: es PERMANENT, no «desconocido».
            assertEquals(ErrorClass.PERMANENT, new Classifier().classify(e).errorClass());
        }
    }

    @Test
    @DisplayName("b. un publish denegado (con el stream consultable) tambien falla en milisegundos con PERMISSION_DENIED y el subject del evento")
    void publishDenegado() throws Exception {
        try (FluxBus pedidos = FluxBus.connect(env.options("ped-sin-publicar"))) {
            long t0 = System.nanoTime();
            PermissionDeniedException e = assertThrows(PermissionDeniedException.class,
                    () -> pedidos.publish(SUBJECT, Map.of("pedidoId", "p-1")));
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertEquals(SUBJECT, e.subject());
            assertTrue(ms < 1500, "no espera el timeout del acuse: " + ms + " ms");
        }
        // Y el MISMO servicio con su permiso de publicar publica (el sentido contrario).
        try (FluxBus pedidos = FluxBus.connect(env.options("pedidos-api"))) {
            assertNotNull(pedidos.publish(SUBJECT, Map.of("pedidoId", "p-2")));
        }
    }

    @Test
    @DisplayName("b. sin concesion alguna sobre un dominio, ni se puede suscribir a el")
    void suscribirseSinConcesion() throws Exception {
        try (FluxBus informes = FluxBus.connect(env.options("informes-api"))) {
            PermissionDeniedException e = assertThrows(PermissionDeniedException.class,
                    () -> informes.subscribe(SUBJECT, (ctx, event, delivery) -> { }));
            assertEquals("PERMISSION_DENIED", e.code());
            assertTrue(e.subject().startsWith("$JS.API."), e.subject());
        }
    }

    // ─── c. NOT_PROVISIONED: el recurso falta y no se puede crear ────────────

    @Test
    @DisplayName("c. un stream que falta y que el servicio no puede crear es NOT_PROVISIONED (no un timeout), y dice que lo crea `flux provision`")
    void streamSinAprovisionar() throws Exception {
        Connection ops = env.ops();
        JetStreamManagement jsm = ops.jetStreamManagement();
        jsm.deleteStream("EVT_LOGISTICA");
        try (FluxBus logistica = FluxBus.connect(env.options("logistica-api"))) {
            long t0 = System.nanoTime();
            NotProvisionedException e = assertThrows(NotProvisionedException.class,
                    () -> logistica.publish("logistica.envio.v1.despachado", Map.of("envioId", "e-1")));
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms < 1500, "NOT_PROVISIONED en milisegundos, no tras un timeout: " + ms + " ms");
            assertEquals("NOT_PROVISIONED", e.code());
            assertEquals(ErrorClass.PERMANENT, e.errorClass());
            assertEquals(ResourceKind.STREAM, e.kind());
            assertEquals("EVT_LOGISTICA", e.resource());
            assertEquals("$JS.API.STREAM.CREATE.EVT_LOGISTICA", e.deniedSubject());
            assertTrue(e.getMessage().contains("EVT_LOGISTICA"), "nombra el recurso: " + e.getMessage());
            assertTrue(e.getMessage().contains("flux provision"), "apunta a flux provision: " + e.getMessage());
            assertTrue(e.getMessage().contains("credenciales de operacion"), e.getMessage());
        } finally {
            // lo vuelve a crear `flux provision`: aqui, con ops, para no romper a los demas tests
            jsm.addStream(io.nats.client.api.StreamConfiguration.builder().name("EVT_LOGISTICA").subjects("logistica.>")
                    .storageType(io.nats.client.api.StorageType.File).build());
        }
    }

    @Test
    @DisplayName("c. un consumidor que falta y que el servicio no puede crear es NOT_PROVISIONED y nombra el consumidor Y su stream")
    void consumidorSinAprovisionar() throws Exception {
        Connection ops = env.ops();
        JetStreamManagement jsm = ops.jetStreamManagement();
        String durable = "inventario-api__pedidos_pedido_v1_cancelado";
        ConsumerInfo original = jsm.getConsumerInfo("EVT_PEDIDOS", durable);
        jsm.deleteConsumer("EVT_PEDIDOS", durable);
        try (FluxBus inventario = FluxBus.connect(env.options("inventario-api"))) {
            NotProvisionedException e = assertThrows(NotProvisionedException.class,
                    () -> inventario.subscribe("pedidos.pedido.v1.cancelado", (ctx, event, delivery) -> { }));
            assertEquals("NOT_PROVISIONED", e.code());
            assertEquals(ResourceKind.CONSUMER, e.kind());
            assertEquals(durable, e.resource());
            assertEquals("EVT_PEDIDOS", e.stream());
            assertTrue(e.deniedSubject().startsWith("$JS.API.CONSUMER.CREATE.EVT_PEDIDOS." + durable), e.deniedSubject());
            assertTrue(e.getMessage().contains(durable) && e.getMessage().contains("EVT_PEDIDOS"), e.getMessage());
            assertTrue(e.getMessage().contains("flux provision"));
        } finally {
            jsm.addOrUpdateConsumer("EVT_PEDIDOS", original.getConsumerConfiguration());
        }
    }

    @Test
    @DisplayName("c. sin permiso ni de CONSULTAR el consumidor no se sabe si falta: PERMISSION_DENIED, nunca NOT_PROVISIONED")
    void sinPermisoDeConsultarEsPermissionDenied() throws Exception {
        try (FluxBus fact = FluxBus.connect(env.options("fact-sin-info-consumidor"))) {
            PermissionDeniedException e = assertThrows(PermissionDeniedException.class,
                    () -> fact.subscribe(SUBJECT, (ctx, event, delivery) -> { }));
            assertEquals(CONSUMER_INFO, e.subject(), "el subject denegado, completo y sin normalizar");
        }
    }

    // ─── d. la DLQ y el pull: lo que el SDK comprueba AL ARRANCAR ────────────

    @Test
    @DisplayName("d. sin permiso de consultar la DLQ del stream consumido, subscribe() falla al arrancar y nombra el subject")
    void sinPermisoDeLaDlq() throws Exception {
        try (FluxBus fact = FluxBus.connect(env.options("fact-sin-info-dlq"))) {
            PermissionDeniedException e = assertThrows(PermissionDeniedException.class,
                    () -> fact.subscribe(SUBJECT, (ctx, event, delivery) -> { }));
            assertEquals(DLQ_INFO, e.subject());
        }
    }

    @Test
    @DisplayName("d. PULL DENEGADO: subscribe() FALLA con PERMISSION_DENIED nombrando MSG.NEXT completo, ANTES de devolver (06-security.md §3.7, DEBE)")
    void pullDenegadoHaceFallarSubscribe() throws Exception {
        CapturingLogger log = new CapturingLogger();
        try (FluxBus fact = FluxBus.connect(env.options("fact-sin-pull").logger(log))) {
            long t0 = System.nanoTime();
            PermissionDeniedException e = assertThrows(PermissionDeniedException.class,
                    () -> fact.subscribe(SUBJECT, (ctx, event, delivery) -> { }));
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertEquals(PULL, e.subject(), "el subject de pull COMPLETO: stream y durable");
            assertTrue(e.getMessage().contains("inerte"), "dice por que importa: " + e.getMessage());
            assertTrue(ms < 3000, "falla en tiempo util: " + ms + " ms");
            // No quedo ningun consumidor abierto a espaldas de quien recibe el error.
            assertEquals(0, fact.subscriptionCount());
        }
    }

    @Test
    @DisplayName("d. y la MISMA conexion con el permiso de pull concedido SI se suscribe, y la sonda no se lleva ningun mensaje")
    void conPermisoDePullSeSuscribeYLaSondaEsInocua() throws Exception {
        List<FluxEvent> recibidos = new CopyOnWriteArrayList<>();
        try (FluxBus pedidos = FluxBus.connect(env.options("pedidos-api"));
             FluxBus fact = FluxBus.connect(env.options("facturacion-api"))) {
            // El evento ya esta en el stream ANTES de suscribirse: si la sonda se llevara un mensaje, quedaria sin ack.
            FluxEvent antes = publishEvent(pedidos, SUBJECT);
            fact.subscribe(SUBJECT, (ctx, event, delivery) -> recibidos.add(event));
            await("llega", 10_000, () -> recibidos.stream().anyMatch(x -> x.id().equals(antes.id())));
            Connection ops = env.ops();
            await("sin pendientes de ack: ni la sonda ni nadie dejo un mensaje en vuelo", 10_000, () -> {
                try {
                    return ops.jetStreamManagement().getConsumerInfo("EVT_PEDIDOS", DURABLE).getNumAckPending() == 0;
                } catch (Exception ex) {
                    return false;
                }
            });
            assertEquals(1, recibidos.stream().filter(x -> x.id().equals(antes.id())).count(), "entregado UNA vez");
        }
    }

    // ─── e. el ack denegado: silencio, y solo el log lo dice ─────────────────

    @Test
    @DisplayName("e. sin permiso de ack el handler corre pero el evento NO se confirma; el SDK lo dice por el logger, UNA vez, con la consecuencia")
    void ackDenegadoSoloTieneElLog() throws Exception {
        CapturingLogger log = new CapturingLogger();
        List<FluxEvent> recibidos = new CopyOnWriteArrayList<>();
        try (FluxBus pedidos = FluxBus.connect(env.options("pedidos-api"));
             FluxBus fact = FluxBus.connect(env.options("fact-sin-ack").logger(log))) {
            fact.subscribe(SUBJECT, (ctx, event, delivery) -> recibidos.add(event));
            publishEvent(pedidos, SUBJECT);
            publishEvent(pedidos, SUBJECT);
            await("los dos eventos corren", 10_000, () -> recibidos.size() >= 2);
            await("el log lo dice", 5_000, () -> !log.containing("falta ese permiso").isEmpty());
            List<String> avisos = log.containing("falta ese permiso");
            assertEquals(1, avisos.size(), "UNA vez por subject, no una por mensaje: " + avisos);
            String aviso = avisos.get(0);
            assertTrue(aviso.startsWith("ERROR"), "a nivel de error: " + aviso);
            assertTrue(aviso.contains(ACK), "el subject normalizado a `…<durable>.>`: " + aviso);
            assertTrue(aviso.contains("REENTREGA"), "dice la CONSECUENCIA: " + aviso);
            // Y es verdad: el broker NO recibio los acks.
            Connection ops = env.ops();
            ConsumerInfo info = ops.jetStreamManagement().getConsumerInfo("EVT_PEDIDOS", DURABLE);
            assertTrue(info.getNumAckPending() >= 2, "los eventos siguen sin confirmar: " + info.getNumAckPending());
        }
    }

    // ─── f. lo que el modo desarrollo cambia ─────────────────────────────────

    @Test
    @DisplayName("f. con `gen-acl.mjs --dev` (CONSUMER.CREATE concedido) el SDK crea el durable que falta: lo contrario de c, en el mismo escenario")
    void conCreateElSdkAprovisiona() throws Exception {
        try (SecureEnv dev = SecureEnv.builder().dev(true).build()) {
            Connection ops = dev.ops();
            // El mismo escenario de `consumidorSinAprovisionar`: el durable se borra. Con CREATE el SDK lo recrea.
            ops.jetStreamManagement().deleteConsumer("EVT_PEDIDOS", DURABLE);
            List<FluxEvent> recibidos = new CopyOnWriteArrayList<>();
            try (FluxBus pedidos = FluxBus.connect(dev.options("pedidos-api"));
                 FluxBus fact = FluxBus.connect(dev.options("facturacion-api"))) {
                fact.subscribe(SUBJECT, (ctx, event, delivery) -> recibidos.add(event));
                FluxEvent e = publishEvent(pedidos, SUBJECT);
                await("el durable recreado entrega", 10_000, () -> recibidos.stream().anyMatch(x -> x.id().equals(e.id())));
            }
            assertEquals(List.of(), dev.server.violations(), "con el permiso concedido no hay ninguna violacion");
            ConsumerInfo recreado = ops.jetStreamManagement().getConsumerInfo("EVT_PEDIDOS", DURABLE);
            assertEquals(Protocol.DEFAULT_MAX_DELIVER, recreado.getConsumerConfiguration().getMaxDeliver(),
                    "lo crea con la configuracion canonica");
        }
    }
}
