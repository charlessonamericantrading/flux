/*
 * Estado replicado EJECUTADO contra un nats-server REAL, bajo los permisos EXACTOS que genera `scripts/gen-acl.mjs` de
 * `examples/services.json` y con los streams y durables que crea `flux provision` (la capacidad `state`, 10-state.md).
 *
 *   NATS_SERVER=/ruta/a/nats-server FLUX_REQUIRE_BROKER=1 mvn test -Dtest=StateIntegrationTest
 *
 * Cada prueba parte de un estado LIMPIO (el stream vaciado, los durables recreados con la config aprovisionada) y comprueba lo
 * concedido Y lo que se quita: una variante del servicio con un permiso de menos tiene que NOTARSE.
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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flux.FluxState.Outcome;
import com.flux.FluxState.WriteResult;
import com.flux.SecurityErrors.NotProvisionedException;
import com.flux.SecurityErrors.PermissionDeniedException;
import com.flux.StateErrors.InvalidStateKeyException;
import com.flux.StateErrors.InvalidStateContractException;
import com.flux.StateErrors.InvalidTenantForStateException;
import com.flux.StateErrors.MissingAggregateVersionException;
import com.flux.TestSupport.CapturingLogger;
import com.flux.broker.SecureEnv;
import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.MessageInfo;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StateIntegrationTest {

    private static final String CONTRACT = "clientes.cliente.v1";
    private static final String STREAM = "STATE_CLIENTES";
    private static final String DLQ_STREAM = "DLQ_CLIENTES";
    private static final String INFORMES_DURABLE = "informes-api__state_clientes_cliente_v1";
    private static final String TIENDA_DURABLE = "tienda-web__state_clientes_cliente_v1__acme";
    private static final String PUT_ACL = "state.clientes.>";
    private static final String DIRECT_ACL = "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.>";
    private static final String INFORMES_PULL = "$JS.API.CONSUMER.MSG.NEXT." + STREAM + "." + INFORMES_DURABLE;
    private static final String INFORMES_ACK = "$JS.ACK." + STREAM + "." + INFORMES_DURABLE + ".>";
    private static final String INFORMES_DLQ_PUBLISH = "dlq.state.clientes.cliente.v1";

    private static SecureEnv env;
    private static SecureEnv sinProvisionar;
    private static final Map<String, ConsumerConfiguration> PROVISIONED = new ConcurrentHashMap<>();

    @BeforeAll
    static void start() throws Exception {
        env = SecureEnv.builder()
                .variant("crm-sin-publicar", "crm-api", List.of(PUT_ACL), List.of())
                .variant("crm-sin-direct-get", "crm-api", List.of(DIRECT_ACL), List.of())
                .variant("informes-sin-pull", "informes-api", List.of(INFORMES_PULL), List.of())
                .variant("informes-sin-ack", "informes-api", List.of(INFORMES_ACK), List.of())
                .variant("informes-sin-dlq", "informes-api", List.of(INFORMES_DLQ_PUBLISH), List.of())
                .build();
        JetStreamManagement jsm = env.ops().jetStreamManagement();
        for (String d : List.of(INFORMES_DURABLE, TIENDA_DURABLE)) {
            PROVISIONED.put(d, jsm.getConsumerInfo(STREAM, d).getConsumerConfiguration());
        }
        sinProvisionar = SecureEnv.builder().provision(false).build();
    }

    @AfterAll
    static void stop() {
        if (env != null) {
            env.close();
        }
        if (sinProvisionar != null) {
            sinProvisionar.close();
        }
    }

    /** Vacia los streams y recrea los durables con la config aprovisionada: cada prueba parte de cero. */
    @BeforeEach
    void limpio() throws Exception {
        JetStreamManagement jsm = env.ops().jetStreamManagement();
        jsm.purgeStream(STREAM);
        jsm.purgeStream(DLQ_STREAM);
        for (Map.Entry<String, ConsumerConfiguration> e : PROVISIONED.entrySet()) {
            try {
                jsm.deleteConsumer(STREAM, e.getKey());
            } catch (Exception notThere) {
                // ya no estaba
            }
            jsm.addOrUpdateConsumer(STREAM, e.getValue());
        }
    }

    // ─── utilidades ──────────────────────────────────────────────────────────

    private static ObjectNode cliente(String id, long version) {
        ObjectNode n = Envelope.mapper().createObjectNode();
        n.put("clienteId", id);
        n.put("nombre", "Ana " + id);
        n.put("email", id + "@example.com");
        n.put("aggregateVersion", version);
        return n;
    }

    private static FluxBus connect(String id) {
        return FluxBus.connect(env.options(id));
    }

    private static FluxBus crm() {
        return FluxBus.connect(env.options("crm-api").tenantId("acme"));
    }

    private static MessageInfo raw(String key) throws Exception {
        return env.ops().jetStreamManagement().getLastMessage(STREAM, "state." + CONTRACT + ".acme." + key);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // El ciclo completo
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("100 claves: put → watch con onSynced (UNA vez, con la foto entera ya procesada) → get; bajo permisos exactos y SIN una violacion en el log del servidor")
    void cicloDe100Claves() throws Exception {
        int antes = env.server.violations().size();
        Map<String, FluxState.Entry> vistas = new ConcurrentHashMap<>();
        AtomicInteger sincronizado = new AtomicInteger();
        AtomicInteger alSincronizar = new AtomicInteger(-1);
        CapturingLogger log = new CapturingLogger();
        try (FluxBus crm = crm();
             FluxBus informes = FluxBus.connect(env.options("informes-api").logger(log))) {
            for (int i = 0; i < 100; i++) {
                WriteResult r = crm.state().put(CONTRACT, "cli-" + i, cliente("cli-" + i, 1));
                assertTrue(r.applied());
                assertEquals(Outcome.OK, r.outcome());
                assertTrue(r.revision() > 0);
            }
            informes.state().watch(CONTRACT, (ctx, entry, delivery) -> vistas.put(entry.key(), entry),
                    new FluxState.WatchOptions().onSynced(() -> {
                        alSincronizar.set(vistas.size());
                        sincronizado.incrementAndGet();
                    }));
            await("onSynced", 15_000, () -> sincronizado.get() > 0);
            // Con la foto entera YA procesada: es lo que significa «mi replica local ya puede contestar lecturas»
            assertEquals(100, alSincronizar.get(), "al sincronizar, las 100 entradas ya estaban procesadas");
            // Y una entrada posterior NO vuelve a disparar onSynced
            crm.state().put(CONTRACT, "cli-extra", cliente("cli-extra", 1));
            await("la entrada posterior", 10_000, () -> vistas.containsKey("cli-extra"));
            assertEquals(1, sincronizado.get(), "UNA vez por watch()");
            assertEquals(101, vistas.size());
            FluxState.Entry e = vistas.get("cli-42");
            assertEquals("acme", e.tenantId());
            assertFalse(e.deleted());
            assertEquals(1, e.aggregateVersion());
            assertEquals("Ana cli-42", e.data().get("nombre").textValue());

            // get: una lectura puntual por DIRECT.GET
            FluxState.Entry g = crm.state().get(CONTRACT, "cli-7").orElseThrow();
            assertEquals("cli-7", g.key());
            assertEquals("cli-7@example.com", g.data().get("email").textValue());
            assertTrue(g.revision() > 0);
            assertTrue(crm.state().get(CONTRACT, "no-existe").isEmpty(), "404 → vacio");
        }
        assertEquals(antes, env.server.violations().size(), "violaciones nuevas: " + env.server.violations());
        assertTrue(log.lines.stream().noneMatch(l -> l.startsWith("ERROR")), log.lines.toString());
    }

    @Test
    @DisplayName("el sobre que llega al broker: data el ULTIMO, UTF-8 literal, subject = la clave, clasificacion confidential por defecto, firma ausente")
    void elSobreEnElBroker() throws Exception {
        try (FluxBus crm = crm()) {
            ObjectNode d = cliente("cli-1", 1);
            d.put("nombre", "José ñ 🚀");
            crm.state().put(CONTRACT, "cli-1", d);
            String body = new String(raw("cli-1").getData(), StandardCharsets.UTF_8);
            assertTrue(body.contains("José ñ 🚀"), "UTF-8 literal, sin escapes \\u: " + body);
            assertFalse(body.contains("\\u"), body);
            assertTrue(body.indexOf("\"data\":") > body.indexOf("\"dataclassification\""), "data el ultimo");
            assertTrue(body.endsWith("}}"), body);
            JsonNode e = Envelope.mapper().readTree(body);
            assertEquals("cli-1", e.get("subject").textValue(), "el atributo `subject` es la CLAVE, no el subject de NATS");
            assertEquals("acme", e.get("tenantid").textValue());
            assertEquals("confidential", e.get("dataclassification").textValue());
            assertEquals("com.flux.state.clientes.cliente.v1", e.get("type").textValue());
            assertEquals("1.0.0", e.get("producerversion").textValue());
            assertEquals("/produccion/crm-api", e.get("source").textValue());
            assertEquals(e.get("id").textValue(), e.get("correlationid").textValue());
            assertFalse(e.has("signature"));
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // La version manda: stale, lapidas
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("una version que no avanza NO se escribe (stale, que NO es un error) y su revision es la del mensaje ACTUAL; una igual tampoco avanza")
    void stale() throws Exception {
        InMemoryMetrics metrics = new InMemoryMetrics();
        try (FluxBus crm = FluxBus.connect(env.options("crm-api").tenantId("acme").metrics(metrics))) {
            WriteResult v3 = crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 3));
            assertTrue(v3.applied());
            WriteResult v2 = crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 2));
            assertFalse(v2.applied());
            assertEquals(Outcome.STALE, v2.outcome());
            assertEquals(v3.revision(), v2.revision(), "stale: la secuencia del mensaje que la hizo obsoleta");
            WriteResult v3bis = crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 3));
            assertFalse(v3bis.applied(), "una igual no avanza");
            assertEquals(3, crm.state().get(CONTRACT, "cli-1").orElseThrow().aggregateVersion());
            assertTrue(crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 4)).applied());
            Map<String, Long> c = metrics.counters();
            assertEquals(2L, c.get("flux_state_writes_total{contract=\"clientes.cliente.v1\",outcome=\"ok\"}"));
            assertEquals(2L, c.get("flux_state_writes_total{contract=\"clientes.cliente.v1\",outcome=\"stale\"}"));
        }
    }

    @Test
    @DisplayName("delete: la lapida sustituye al valor (el dato personal ya no esta en el stream), get da vacio, y la version continua desde la lapida")
    void lapidas() throws Exception {
        List<FluxState.Entry> vistas = new CopyOnWriteArrayList<>();
        try (FluxBus crm = crm(); FluxBus informes = connect("informes-api")) {
            crm.state().put(CONTRACT, "cli-9", cliente("cli-9", 1));
            informes.state().watch(CONTRACT, (ctx, entry, delivery) -> vistas.add(entry));
            await("el valor", 10_000, () -> vistas.size() == 1);

            WriteResult borrado = crm.state().delete(CONTRACT, "cli-9", 2);
            assertTrue(borrado.applied());
            assertTrue(crm.state().get(CONTRACT, "cli-9").isEmpty(), "una lapida se lee como «no existe»");
            await("la lapida llega al observador", 10_000, () -> vistas.size() == 2);
            FluxState.Entry lapida = vistas.get(1);
            assertTrue(lapida.deleted(), "el handler DEBE borrar su copia local");
            assertEquals(null, lapida.data());
            assertEquals(2, lapida.aggregateVersion());
            // El dato personal ya no esta en el stream
            String body = new String(raw("cli-9").getData(), StandardCharsets.UTF_8);
            assertFalse(body.contains("cli-9@example.com"), body);
            assertTrue(body.contains("com.flux.state.clientes.cliente.deleted.v1"), body);

            // Una lapida de version <= la actual no escribe; y recrear exige superar la de la lapida
            assertFalse(crm.state().delete(CONTRACT, "cli-9", 2).applied());
            assertFalse(crm.state().put(CONTRACT, "cli-9", cliente("cli-9", 2)).applied(), "continua desde la lapida: 2 no avanza");
            assertTrue(crm.state().put(CONTRACT, "cli-9", cliente("cli-9", 3)).applied());
            assertEquals(3, crm.state().get(CONTRACT, "cli-9").orElseThrow().aggregateVersion());
            // Borrar una clave que nunca existio escribe la lapida
            assertTrue(crm.state().delete(CONTRACT, "nunca-existio", 1).applied());
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Escritores concurrentes
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("tres instancias del dueño escribiendo las mismas claves convergen en la version mas alta; STATE_CONFLICT es RETRYABLE y se reintenta")
    void escritoresConcurrentes() throws Exception {
        int claves = 30;
        int versiones = 6;
        AtomicInteger conflictos = new AtomicInteger();
        List<FluxBus> buses = new ArrayList<>();
        List<Thread> hilos = new ArrayList<>();
        AtomicReference<Throwable> fallo = new AtomicReference<>();
        try {
            for (int w = 0; w < 3; w++) {
                buses.add(crm());
            }
            for (int w = 0; w < 3; w++) {
                FluxBus bus = buses.get(w);
                int quien = w;
                Thread t = new Thread(() -> {
                    try {
                        for (int k = 0; k < claves; k++) {
                            // Cada escritor recorre las versiones en SU orden: unos suben, otros bajan
                            for (int i = 0; i < versiones; i++) {
                                int v = quien == 1 ? versiones - i : quien == 2 ? ((i * 5) % versiones) + 1 : i + 1;
                                while (true) {
                                    try {
                                        bus.state().put(CONTRACT, "carrera-" + k, cliente("carrera-" + k, v));
                                        break;
                                    } catch (StateErrors.StateConflictException e) {
                                        conflictos.incrementAndGet();
                                        assertEquals(ErrorClass.RETRYABLE, e.errorClass());
                                    }
                                }
                            }
                        }
                    } catch (Throwable e) {
                        fallo.compareAndSet(null, e);
                    }
                });
                hilos.add(t);
                t.start();
            }
            for (Thread t : hilos) {
                t.join(60_000);
            }
            assertEquals(null, fallo.get());
            FluxBus lector = buses.get(0);
            for (int k = 0; k < claves; k++) {
                // En el valor que quedo NUNCA hay una version vieja pisando a una nueva
                assertEquals(versiones, lector.state().get(CONTRACT, "carrera-" + k).orElseThrow().aggregateVersion(), "carrera-" + k);
            }
        } finally {
            for (FluxBus b : buses) {
                b.close();
            }
        }
    }

    @Test
    @DisplayName("un acuse perdido NO convierte una escritura buena en `stale` con su propio dato (paso 1b): el mensaje leido es el mio, se reconoce por el id")
    void elPropioMensajeSeReconoce() throws Exception {
        try (FluxBus crm = crm()) {
            StateEnvelope.ParsedContract c = StateEnvelope.parseContract(CONTRACT);
            String id = Protocol.uuidV7();
            String subject = StateEnvelope.subject(CONTRACT, "acme", "cli-1");
            FluxEvent ev = StateEnvelope.buildEvent(new StateEnvelope.EventInput().contract(CONTRACT).tenantId("acme").key("cli-1")
                    .id(id).source(crm.source).time(Envelope.formatTime(java.time.Instant.now())).correlationid(id)
                    .producerversion("1.0.0").classification("confidential").data(cliente("cli-1", 1))
                    .dataschema("https://schemas.internal/state/clientes/cliente/1.0.0.json"));
            // «El acuse se perdio»: el mensaje YA esta en el stream, con ese id. El reintento lo lee y es EL SUYO.
            long seq = env.ops().jetStream().publish(subject, Envelope.serialize(ev),
                    io.nats.client.PublishOptions.builder().messageId(id).build()).getSeqno();
            WriteResult r = crm.state().write(c, "acme", "cli-1", subject, 1, ev);
            assertTrue(r.applied(), "es mi propio mensaje: ya se aplico, NO es un stale");
            assertEquals(Outcome.OK, r.outcome());
            assertEquals(seq, r.revision());
            // Con OTRO id y la misma version, en cambio, SI es stale
            FluxEvent otro = StateEnvelope.buildEvent(new StateEnvelope.EventInput().contract(CONTRACT).tenantId("acme").key("cli-1")
                    .id(Protocol.uuidV7()).source(crm.source).time(Envelope.formatTime(java.time.Instant.now())).correlationid(id)
                    .producerversion("1.0.0").classification("confidential").data(cliente("cli-1", 1))
                    .dataschema("https://schemas.internal/state/clientes/cliente/1.0.0.json"));
            assertFalse(crm.state().write(c, "acme", "cli-1", subject, 1, otro).applied());
        }
    }

    /** El sobre de un put de «otra instancia del dueno» para esa clave, con esa version. */
    private static byte[] deOtraInstancia(String key, long version) {
        String id = Protocol.uuidV7();
        return Envelope.serialize(StateEnvelope.buildEvent(new StateEnvelope.EventInput().contract(CONTRACT).tenantId("acme").key(key)
                .id(id).source("/produccion/crm-api").time(Envelope.formatTime(java.time.Instant.now())).correlationid(id)
                .producerversion("1.0.0").classification("confidential").data(cliente(key, version))
                .dataschema("https://schemas.internal/state/clientes/cliente/1.0.0.json")));
    }

    @Test
    @DisplayName("el conflicto de secuencia, FABRICADO: otra instancia escribe entre la lectura y la publicacion → el SDK relee y decide (stale si la otra va por delante, aplica si va por detras)")
    void conflictoDeterminista() throws Exception {
        InMemoryMetrics metrics = new InMemoryMetrics();
        List<Integer> intentos = new CopyOnWriteArrayList<>();
        try (FluxBus crm = FluxBus.connect(env.options("crm-api").tenantId("acme").metrics(metrics))) {
            String subject = "state." + CONTRACT + ".acme.k-1";
            // 1. La otra instancia va por DELANTE (version 5): tras el conflicto el SDK relee y ve que su 3 ya no avanza
            FluxState.beforePublish = attempt -> {
                intentos.add(attempt);
                if (attempt == 1) {
                    try {
                        env.ops().jetStream().publish(subject, deOtraInstancia("k-1", 5));
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
            };
            WriteResult r = crm.state().put(CONTRACT, "k-1", cliente("k-1", 3));
            assertFalse(r.applied(), "la otra va por delante: ya no avanza");
            assertEquals(Outcome.STALE, r.outcome());
            assertEquals(List.of(1), intentos, "el segundo intento ya no llega a publicar: lo para el paso 2");
            assertEquals(5, crm.state().get(CONTRACT, "k-1").orElseThrow().aggregateVersion());

            // 2. La otra va por DETRAS (version 1): el SDK reintenta con la secuencia nueva y SI escribe
            intentos.clear();
            FluxState.beforePublish = attempt -> {
                intentos.add(attempt);
                if (attempt == 1) {
                    try {
                        env.ops().jetStream().publish("state." + CONTRACT + ".acme.k-2", deOtraInstancia("k-2", 1));
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
            };
            WriteResult r2 = crm.state().put(CONTRACT, "k-2", cliente("k-2", 3));
            assertTrue(r2.applied());
            assertEquals(List.of(1, 2), intentos, "un conflicto, un reintento");
            assertEquals(3, crm.state().get(CONTRACT, "k-2").orElseThrow().aggregateVersion());

            // 3. Otra instancia gana CADA vez (siempre con una version menor): al tercer intento STATE_CONFLICT, RETRYABLE
            intentos.clear();
            AtomicInteger v = new AtomicInteger(1);
            FluxState.beforePublish = attempt -> {
                intentos.add(attempt);
                try {
                    env.ops().jetStream().publish("state." + CONTRACT + ".acme.k-3", deOtraInstancia("k-3", v.getAndIncrement()));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            };
            StateErrors.StateConflictException c = assertThrows(StateErrors.StateConflictException.class,
                    () -> crm.state().put(CONTRACT, "k-3", cliente("k-3", 9)));
            assertEquals(List.of(1, 2, 3), intentos, "exactamente 3 intentos");
            assertEquals("STATE_CONFLICT", c.code());
            assertEquals(ErrorClass.RETRYABLE, c.errorClass());
            assertEquals(1L, metrics.counters().get("flux_state_writes_total{contract=\"clientes.cliente.v1\",outcome=\"conflict\"}"));
            assertEquals(1L, metrics.counters().get("flux_state_writes_total{contract=\"clientes.cliente.v1\",outcome=\"stale\"}"));
            assertEquals(1L, metrics.counters().get("flux_state_writes_total{contract=\"clientes.cliente.v1\",outcome=\"ok\"}"));
        } finally {
            FluxState.beforePublish = null;
        }
    }

    @Test
    @DisplayName("CONTROL: el broker de verdad contesta el 10071 a una escritura con la secuencia vieja (el 10164 es el de un cluster bajo carrera, y el SDK reconoce los dos)")
    void elCodigoDelBroker() throws Exception {
        Connection ops = env.ops();
        ops.jetStream().publish("state." + CONTRACT + ".acme.k-1", deOtraInstancia("k-1", 1));
        io.nats.client.JetStreamApiException e = assertThrows(io.nats.client.JetStreamApiException.class,
                () -> ops.jetStream().publish("state." + CONTRACT + ".acme.k-1", deOtraInstancia("k-1", 2),
                        io.nats.client.PublishOptions.builder().expectedLastSubjectSequence(999).build()));
        assertTrue(List.of(10071, 10164).contains(e.getApiErrorCode()), "codigo " + e.getApiErrorCode() + ": " + e.getErrorDescription());
        assertTrue(FluxState.isSequenceConflict(e));
        // `0` = «la clave no existe»: tiene que ENVIARSE la cabecera con valor 0 (y fallar si ya hay un mensaje)
        io.nats.client.JetStreamApiException cero = assertThrows(io.nats.client.JetStreamApiException.class,
                () -> ops.jetStream().publish("state." + CONTRACT + ".acme.k-1", deOtraInstancia("k-1", 2),
                        io.nats.client.PublishOptions.builder().expectedLastSubjectSequence(0).build()));
        assertTrue(FluxState.isSequenceConflict(cero), "la cabecera con 0 se envia: " + cero.getErrorDescription());
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Validacion antes de la red
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("la validacion de la llamada va ANTES de la red y en orden: dominio reservado, contrato, clave, tenant, version, clasificacion")
    void validacionPrevia() throws Exception {
        try (FluxBus crm = crm(); FluxBus sinTenant = connect("crm-api")) {
            assertEquals("RESERVED_DOMAIN", assertThrows(ReservedDomainException.class, () -> crm.state().put("state.clientes.v1", "k", cliente("k", 1))).code());
            assertEquals("INVALID_STATE_CONTRACT", assertThrows(InvalidStateContractException.class, () -> crm.state().put("clientes.cliente", "k", cliente("k", 1))).code());
            assertEquals("INVALID_STATE_KEY", assertThrows(InvalidStateKeyException.class, () -> crm.state().put(CONTRACT, "con.punto", cliente("k", 1))).code());
            assertEquals("INVALID_TENANT_FOR_STATE", assertThrows(InvalidTenantForStateException.class,
                    () -> sinTenant.state().put(CONTRACT, "k", cliente("k", 1))).code(),
                    "sin tenant en connect() ni en la llamada, NO se escribe en `system`");
            assertEquals("INVALID_TENANT_FOR_STATE", assertThrows(InvalidTenantForStateException.class,
                    () -> crm.state().put(CONTRACT, "k", cliente("k", 1), new FluxState.PutOptions().tenantId("ACME"))).code());
            ObjectNode sinVersion = cliente("k", 1);
            sinVersion.remove("aggregateVersion");
            assertEquals("MISSING_AGGREGATE_VERSION", assertThrows(MissingAggregateVersionException.class, () -> crm.state().put(CONTRACT, "k", sinVersion)).code());
            assertEquals("INVALID_DATACLASSIFICATION", assertThrows(StateErrors.InvalidDataClassificationException.class,
                    () -> crm.state().put(CONTRACT, "k", cliente("k", 1), new FluxState.PutOptions().classification(FluxEvent.DataClassification.RESTRICTED))).code(),
                    "restricted no se admite en estado");
            // Nada de esto toco el stream
            assertEquals(0, env.ops().jetStreamManagement().getStreamInfo(STREAM).getStreamState().getMsgCount());
        }
        // Y con un tenant en la llamada, sin tenant en connect(), SI se escribe
        try (FluxBus sinTenant = connect("crm-api")) {
            assertTrue(sinTenant.state().put(CONTRACT, "k", cliente("k", 1), new FluxState.PutOptions().tenantId("globex")).applied());
            assertTrue(sinTenant.state().get(CONTRACT, "k", new FluxState.GetOptions().tenantId("globex")).isPresent());
            assertTrue(sinTenant.state().get(CONTRACT, "k", new FluxState.GetOptions().tenantId("acme")).isEmpty());
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Permisos: lo concedido funciona y lo que se quita se NOTA
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("sin permiso de publicar el estado: put falla en milisegundos con PERMISSION_DENIED que nombra el subject; con el permiso, escribe")
    void putSinPermiso() throws Exception {
        try (FluxBus sin = FluxBus.connect(env.options("crm-sin-publicar").tenantId("acme"))) {
            long t0 = System.nanoTime();
            PermissionDeniedException e = assertThrows(PermissionDeniedException.class, () -> sin.state().put(CONTRACT, "cli-1", cliente("cli-1", 1)));
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertEquals("state." + CONTRACT + ".acme.cli-1", e.subject());
            assertTrue(ms < 2500, "no espera el timeout del acuse: " + ms + " ms");
            assertEquals(0, env.ops().jetStreamManagement().getStreamInfo(STREAM).getStreamState().getMsgCount());
        }
        try (FluxBus crm = crm()) {
            assertTrue(crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 1)).applied());
        }
    }

    @Test
    @DisplayName("sin permiso de DIRECT.GET, tanto get como put (que lee antes de escribir) fallan con PERMISSION_DENIED, no con NOT_PROVISIONED ni un timeout")
    void leerSinPermiso() throws Exception {
        try (FluxBus sin = FluxBus.connect(env.options("crm-sin-direct-get").tenantId("acme"))) {
            long t0 = System.nanoTime();
            PermissionDeniedException e = assertThrows(PermissionDeniedException.class, () -> sin.state().get(CONTRACT, "cli-1"));
            assertTrue(e.subject().startsWith("$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.cli-1"), e.subject());
            assertThrows(PermissionDeniedException.class, () -> sin.state().put(CONTRACT, "cli-1", cliente("cli-1", 1)));
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 4000);
        }
    }

    @Test
    @DisplayName("el broker confina por tenant: tienda-web (solo acme) no lee ni observa globex, y un observador no escribe")
    void confinamientoPorTenant() throws Exception {
        try (FluxBus crm = FluxBus.connect(env.options("crm-api"));
             FluxBus tienda = FluxBus.connect(env.options("tienda-web").tenantId("acme"))) {
            crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 1), new FluxState.PutOptions().tenantId("acme"));
            crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 1), new FluxState.PutOptions().tenantId("globex"));
            assertTrue(tienda.state().get(CONTRACT, "cli-1").isPresent(), "su tenant");
            assertThrows(PermissionDeniedException.class,
                    () -> tienda.state().get(CONTRACT, "cli-1", new FluxState.GetOptions().tenantId("globex")), "otro tenant: el broker lo deniega");
            assertThrows(PermissionDeniedException.class,
                    () -> tienda.state().watch(CONTRACT, (ctx, entry, d) -> { }, new FluxState.WatchOptions().tenantId("globex")));
            assertThrows(PermissionDeniedException.class,
                    () -> tienda.state().put(CONTRACT, "cli-2", cliente("cli-2", 1)), "un observador no escribe");
            // Y SI observa el suyo
            List<String> claves = new CopyOnWriteArrayList<>();
            tienda.state().watch(CONTRACT, (ctx, entry, d) -> claves.add(entry.tenantId() + "/" + entry.key()));
            await("su foto", 10_000, () -> claves.size() == 1);
            assertEquals("acme/cli-1", claves.get(0));
        }
    }

    @Test
    @DisplayName("sin el permiso de pull el watch FALLA al abrirse (no queda inerte parece sano); sin el de ack o sin el de la DLQ, solo el log lo dice")
    void watchSinPermisos() throws Exception {
        try (FluxBus sin = connect("informes-sin-pull")) {
            PermissionDeniedException e = assertThrows(PermissionDeniedException.class,
                    () -> sin.state().watch(CONTRACT, (ctx, entry, d) -> { }));
            assertEquals(INFORMES_PULL, e.subject());
        }
        // Sin ack: el watch abre y procesa, pero el SDK AVISA, y por el log (un ack no tiene respuesta)
        CapturingLogger log = new CapturingLogger();
        try (FluxBus crm = crm(); FluxBus sin = FluxBus.connect(env.options("informes-sin-ack").logger(log))) {
            crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 1));
            List<String> claves = new CopyOnWriteArrayList<>();
            sin.state().watch(CONTRACT, (ctx, entry, d) -> claves.add(entry.key()));
            await("procesa", 10_000, () -> !claves.isEmpty());
            await("el aviso del ack denegado", 10_000, () -> !log.containing("REENTREGA").isEmpty());
        }
    }

    @Test
    @DisplayName("un durable que NO esta aprovisionado y sin permiso de crearlo es NOT_PROVISIONED (consumidor), PERMANENT, y apunta a `flux provision`")
    void durableSinAprovisionar() throws Exception {
        JetStreamManagement jsm = env.ops().jetStreamManagement();
        jsm.deleteConsumer(STREAM, INFORMES_DURABLE);
        try (FluxBus informes = connect("informes-api")) {
            NotProvisionedException e = assertThrows(NotProvisionedException.class,
                    () -> informes.state().watch(CONTRACT, (ctx, entry, d) -> { }));
            assertEquals("NOT_PROVISIONED", e.code());
            assertEquals(ErrorClass.PERMANENT, e.errorClass());
            assertTrue(e.getMessage().contains("flux provision"), e.getMessage());
        }
    }

    @Test
    @DisplayName("el stream de estado NO existe: get y put dan NOT_PROVISIONED con las DOS causas (no existe / sin allow_direct), sin un timeout")
    void streamSinAprovisionar() throws Exception {
        try (FluxBus crm = FluxBus.connect(sinProvisionar.options("crm-api").tenantId("acme"))) {
            long t0 = System.nanoTime();
            NotProvisionedException e = assertThrows(NotProvisionedException.class, () -> crm.state().get(CONTRACT, "cli-1"));
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 3000, "`no responders` llega al instante");
            assertEquals("NOT_PROVISIONED", e.code());
            assertTrue(e.getMessage().contains("allow_direct"), e.getMessage());
            assertTrue(e.getMessage().contains("flux provision") && e.getMessage().contains("flux doctor"), e.getMessage());
            assertThrows(NotProvisionedException.class, () -> crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 1)));
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // Consumir: lo que se comprueba y a donde va lo que falla
    // ═════════════════════════════════════════════════════════════════════════

    private static FluxEvent dlqEvent(String key) throws Exception {
        return Envelope.parseEvent(env.ops().jetStreamManagement()
                .getLastMessage(DLQ_STREAM, "dlq.state." + CONTRACT).getData());
    }

    @Test
    @DisplayName("un handler que falla va a la DLQ de estado con el sobre ORIGINAL y data = {key, revision}: sin el dato, sin la firma, y data sigue el ultimo")
    void dlqDeEstado() throws Exception {
        List<String> poison = new CopyOnWriteArrayList<>();
        InMemoryMetrics metrics = new InMemoryMetrics();
        try (FluxBus crm = crm();
             FluxBus informes = FluxBus.connect(env.options("informes-api").metrics(metrics)
                     .onPoison(info -> poison.add(info.subject())))) {
            WriteResult w = crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 1));
            AtomicInteger veces = new AtomicInteger();
            informes.state().watch(CONTRACT, (ctx, entry, d) -> {
                veces.incrementAndGet();
                throw new FluxErrors.PermanentException("el cliente cli-1@example.com ya existe", "CLIENTE_DUPLICADO");
            });
            await("a la DLQ", 10_000, () -> {
                try {
                    return env.ops().jetStreamManagement().getStreamInfo(DLQ_STREAM).getStreamState().getMsgCount() == 1;
                } catch (Exception e) {
                    return false;
                }
            });
            assertEquals(1, veces.get(), "PERMANENT: a la DLQ en la primera entrega, sin reintentos");
            FluxEvent dlq = dlqEvent("cli-1");
            assertEquals(FluxEvent.DlqReason.PERMANENT, dlq.dlqreason());
            assertEquals(1, dlq.dlqattempts());
            assertEquals(INFORMES_DURABLE, dlq.dlqconsumer());
            assertTrue(dlq.dlqerror().startsWith("CLIENTE_DUPLICADO: "), dlq.dlqerror());
            assertEquals("cli-1", dlq.data().get("key").textValue());
            assertEquals(w.revision(), dlq.data().get("revision").longValue());
            assertEquals(2, dlq.data().size(), "ni el nombre ni el email: la DLQ guarda 90 dias");
            assertEquals(null, dlq.signature());
            assertEquals("cli-1", dlq.aggregateId());
            String raw = new String(env.ops().jetStreamManagement().getLastMessage(DLQ_STREAM, "dlq.state." + CONTRACT).getData(), StandardCharsets.UTF_8);
            assertFalse(raw.contains("example.com") && raw.contains("nombre"), raw);
            assertTrue(raw.endsWith("\"data\":{\"key\":\"cli-1\",\"revision\":" + w.revision() + "}}"), raw);
            assertTrue(poison.isEmpty(), "PERMANENT no es POISON");
            Map<String, Long> c = metrics.counters();
            assertEquals(1L, c.get("flux_events_dlq_total{code=\"CLIENTE_DUPLICADO\",consumer=\"informes-api__state_clientes_cliente_v1\",reason=\"permanent\",subject=\"state.clientes.cliente.v1\"}"), c.toString());
        }
    }

    @Test
    @DisplayName("un error que no es de flux (una excepcion de un driver) NO cita su mensaje en la DLQ: solo el codigo y la frase fija")
    void dlqSinElMensajeAjeno() throws Exception {
        CapturingLogger log = new CapturingLogger();
        try (FluxBus crm = crm();
             FluxBus informes = FluxBus.connect(env.options("informes-api").logger(log)
                     .classifier(new Classifier.ClassifierOptions().unknownErrorPolicy(Classifier.UnknownErrorPolicy.PERMANENT)))) {
            crm.state().put(CONTRACT, "cli-2", cliente("cli-2", 1));
            informes.state().watch(CONTRACT, (ctx, entry, d) -> {
                throw new java.sql.SQLException("fila duplicada: (cli-2, cli-2@example.com)");
            });
            await("a la DLQ", 10_000, () -> {
                try {
                    return env.ops().jetStreamManagement().getStreamInfo(DLQ_STREAM).getStreamState().getMsgCount() == 1;
                } catch (Exception e) {
                    return false;
                }
            });
            FluxEvent dlq = dlqEvent("cli-2");
            assertEquals("UNKNOWN: " + StateEnvelope.UNCLASSIFIED_DLQ_ERROR, dlq.dlqerror());
            assertFalse(dlq.dlqerror().contains("example.com"));
            assertTrue(log.lines.stream().noneMatch(l -> l.contains("example.com")), "ni en el log: " + log.lines);
        }
    }

    private static void limpioDurables() throws Exception {
        JetStreamManagement jsm = env.ops().jetStreamManagement();
        for (Map.Entry<String, ConsumerConfiguration> e : PROVISIONED.entrySet()) {
            try {
                jsm.deleteConsumer(STREAM, e.getKey());
            } catch (Exception notThere) {
                // ya no estaba
            }
            jsm.addOrUpdateConsumer(STREAM, e.getValue());
        }
    }

    @Test
    @DisplayName("el broker decide la identidad: un mensaje cuyo sobre miente (otro tenant, otra clave, otro tipo, restricted) es POISON y no llega al handler")
    void sobreQueMiente() throws Exception {
        List<String> poison = new CopyOnWriteArrayList<>();
        List<String> codigos = new CopyOnWriteArrayList<>();
        List<String> vistas = new CopyOnWriteArrayList<>();
        Connection ops = env.ops();
        try (FluxBus crm = crm();
             FluxBus informes = FluxBus.connect(env.options("informes-api").onPoison(info -> {
                 poison.add(info.error().getMessage());
                 codigos.add(((FluxErrors.FluxException) info.error()).code());
             }))) {
            crm.state().put(CONTRACT, "bueno", cliente("bueno", 1));
            // Un productor que publica a mano (con credenciales de operacion) sobres que mienten sobre su subject
            FluxEvent base = Envelope.parseEvent(raw("bueno").getData());
            publishRaw(ops, "state." + CONTRACT + ".acme.otra-clave", base);                       // subject != clave del sobre
            publishRaw(ops, "state." + CONTRACT + ".globex.bueno", base);                          // tenant != tenant del subject
            informes.state().watch(CONTRACT, (ctx, entry, d) -> vistas.add(entry.tenantId() + "/" + entry.key()));
            await("los dos POISON", 15_000, () -> poison.size() == 2);
            await("el bueno", 10_000, () -> vistas.contains("acme/bueno"));
            assertEquals(List.of("acme/bueno"), vistas, "lo que miente NO llega al handler");
            assertEquals(List.of("STATE_SUBJECT_MISMATCH", "STATE_SUBJECT_MISMATCH"), codigos, "POISON, con el codigo estable de la spec");
            assertTrue(poison.stream().anyMatch(m -> m.contains("subject")), poison.toString());
            assertTrue(poison.stream().anyMatch(m -> m.contains("tenantid")), poison.toString());
            await("a la DLQ", 10_000, () -> {
                try {
                    return env.ops().jetStreamManagement().getStreamInfo(DLQ_STREAM).getStreamState().getMsgCount() == 2;
                } catch (Exception e) {
                    return false;
                }
            });
        }
    }

    private static void publishRaw(Connection ops, String subject, FluxEvent e) throws Exception {
        ops.jetStream().publish(subject, Envelope.serialize(e));
    }

    @Test
    @DisplayName("un mensaje ILEGIBLE en el stream va a la DLQ sintetizado desde el subject (no se copia su cuerpo) y se marca POISON")
    void mensajeIlegible() throws Exception {
        List<String> poison = new CopyOnWriteArrayList<>();
        Connection ops = env.ops();
        ops.jetStream().publish("state." + CONTRACT + ".acme.roto", "{ esto no es json con un secreto: ana@example.com".getBytes());
        try (FluxBus informes = FluxBus.connect(env.options("informes-api").onPoison(info -> poison.add(info.subject())))) {
            informes.state().watch(CONTRACT, (ctx, entry, d) -> { });
            await("POISON", 10_000, () -> poison.size() == 1);
            await("a la DLQ", 10_000, () -> {
                try {
                    return env.ops().jetStreamManagement().getStreamInfo(DLQ_STREAM).getStreamState().getMsgCount() == 1;
                } catch (Exception e) {
                    return false;
                }
            });
            String raw = new String(env.ops().jetStreamManagement().getLastMessage(DLQ_STREAM, "dlq.state." + CONTRACT).getData(), StandardCharsets.UTF_8);
            assertFalse(raw.contains("ana@example.com"), "el cuerpo ilegible no se copia: " + raw);
            FluxEvent dlq = Envelope.parseEvent(raw.getBytes(StandardCharsets.UTF_8));
            assertEquals(FluxEvent.DlqReason.POISON, dlq.dlqreason());
            assertEquals("roto", dlq.aggregateId());
            assertEquals("acme", dlq.tenantid());
            assertEquals("roto", dlq.data().get("key").textValue());
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // onSynced, tenants y el durable
    // ═════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("onSynced con el contrato VACIO se invoca de inmediato, antes de ninguna entrada; y una sola vez")
    void onSyncedConContratoVacio() throws Exception {
        AtomicInteger sync = new AtomicInteger();
        List<String> orden = new CopyOnWriteArrayList<>();
        try (FluxBus crm = crm(); FluxBus informes = connect("informes-api")) {
            informes.state().watch(CONTRACT, (ctx, entry, d) -> orden.add("entrada:" + entry.key()),
                    new FluxState.WatchOptions().onSynced(() -> {
                        sync.incrementAndGet();
                        orden.add("sync");
                    }));
            assertEquals(1, sync.get(), "ya al volver de watch(): no espera a una entrada que quiza no llegue");
            crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 1));
            await("la entrada", 10_000, () -> orden.contains("entrada:cli-1"));
            assertEquals(List.of("sync", "entrada:cli-1"), orden);
            assertEquals(1, sync.get());
        }
    }

    @Test
    @DisplayName("un watch con tenant filtra por el (y el durable lleva su sufijo); sin tenant recibe `cli-1` de los DOS tenants y la entrada dice de cual")
    void tenantsEnElWatch() throws Exception {
        List<String> todos = new CopyOnWriteArrayList<>();
        try (FluxBus crm = FluxBus.connect(env.options("crm-api"));
             FluxBus informes = connect("informes-api")) {
            crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 1), new FluxState.PutOptions().tenantId("acme"));
            crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 1), new FluxState.PutOptions().tenantId("globex"));
            FluxState.Subscription s = informes.state().watch(CONTRACT, (ctx, entry, d) -> todos.add(entry.tenantId() + "/" + entry.key()));
            assertEquals(INFORMES_DURABLE, s.durable());
            assertTrue(s.tenantId().isEmpty());
            await("los dos", 10_000, () -> todos.size() == 2);
            assertTrue(todos.contains("acme/cli-1") && todos.contains("globex/cli-1"), todos.toString());
        }
        assertEquals("tienda-web__state_clientes_cliente_v1__acme", StateEnvelope.durableName("tienda-web", CONTRACT, "acme"));
    }

    @Test
    @DisplayName("con tenantIsolation STRICT, un watch sin tenant en connect() ni en watch() es un error de configuracion (no observa a todos)")
    void tenantIsolationEstricto() throws Exception {
        try (FluxBus informes = FluxBus.connect(env.options("informes-api").tenantIsolation(FluxBus.TenantIsolation.STRICT))) {
            assertThrows(TenantIsolationException.class, () -> informes.state().watch(CONTRACT, (ctx, entry, d) -> { }));
        }
    }

    @Test
    @DisplayName("un durable aprovisionado con otro filtro NO se acepta (ConsumerConfigMismatchException): entregaria claves de otro tenant sin ningun error")
    void durableConOtroFiltro() throws Exception {
        JetStreamManagement jsm = env.ops().jetStreamManagement();
        jsm.deleteConsumer(STREAM, INFORMES_DURABLE);
        ConsumerConfiguration otro = ConsumerConfiguration.builder(PROVISIONED.get(INFORMES_DURABLE))
                .filterSubject("state.clientes.cliente.v1.globex.*").build();
        jsm.addOrUpdateConsumer(STREAM, otro);
        try (FluxBus informes = connect("informes-api")) {
            ConsumerConfigMismatchException e = assertThrows(ConsumerConfigMismatchException.class,
                    () -> informes.state().watch(CONTRACT, (ctx, entry, d) -> { }));
            assertTrue(e.differences().stream().anyMatch(x -> x.field().equals("filter_subject")), e.getMessage());
        }
        jsm.deleteConsumer(STREAM, INFORMES_DURABLE);
        ConsumerConfiguration politica = ConsumerConfiguration.builder(PROVISIONED.get(INFORMES_DURABLE))
                .deliverPolicy(io.nats.client.api.DeliverPolicy.All).build();
        jsm.addOrUpdateConsumer(STREAM, politica);
        try (FluxBus informes = connect("informes-api")) {
            ConsumerConfigMismatchException e = assertThrows(ConsumerConfigMismatchException.class,
                    () -> informes.state().watch(CONTRACT, (ctx, entry, d) -> { }));
            assertTrue(e.differences().stream().anyMatch(x -> x.field().equals("deliver_policy")), e.getMessage());
        }
    }

    @Test
    @DisplayName("un watch cuyo durable se BORRA y se recrea (flux state resync) reabre el consumo en segundos —no en 89 s— y entrega la foto otra vez")
    void resync() throws Exception {
        List<String> vistas = new CopyOnWriteArrayList<>();
        AtomicInteger sync = new AtomicInteger();
        CapturingLogger log = new CapturingLogger();
        try (FluxBus crm = crm(); FluxBus informes = FluxBus.connect(env.options("informes-api").logger(log))) {
            crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 1));
            informes.state().watch(CONTRACT, (ctx, entry, d) -> vistas.add(entry.key() + "@" + entry.aggregateVersion()),
                    new FluxState.WatchOptions().onSynced(sync::incrementAndGet));
            await("la foto", 10_000, () -> vistas.size() == 1);

            JetStreamManagement jsm = env.ops().jetStreamManagement();
            long t0 = System.nanoTime();
            jsm.deleteConsumer(STREAM, INFORMES_DURABLE);
            crm.state().put(CONTRACT, "cli-2", cliente("cli-2", 1));
            Thread.sleep(500);
            jsm.addOrUpdateConsumer(STREAM, PROVISIONED.get(INFORMES_DURABLE));
            // Recreado: last_per_subject → la foto ENTERA otra vez (cli-1 y cli-2)
            await("la foto nueva", 30_000, () -> vistas.contains("cli-2@1") && vistas.size() >= 3);
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms < 20_000, "sin la reapertura serian ~89 s (heartbeat de la libreria): " + ms + " ms");
            assertEquals(1, sync.get(), "onSynced no se invoca de nuevo tras un resync");
            assertFalse(log.containing("se borro con el watch").isEmpty(), "se avisa: " + log.lines);
        }
    }

    @Test
    @DisplayName("un watch cuyo durable se borra y NO se recrea reintenta con aviso, sin colgarse, y close() lo para")
    void durableBorradoSinRecrear() throws Exception {
        CapturingLogger log = new CapturingLogger();
        FluxBus informes = FluxBus.connect(env.options("informes-api").logger(log));
        informes.state().watch(CONTRACT, (ctx, entry, d) -> { });
        env.ops().jetStreamManagement().deleteConsumer(STREAM, INFORMES_DURABLE);
        await("el aviso de reapertura", 15_000, () -> !log.containing("no se pudo reabrir").isEmpty() || !log.containing("se borro con el watch").isEmpty());
        long t0 = System.nanoTime();
        informes.close();
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 15_000, "close() no espera a un durable que no vuelve");
    }

    @Test
    @DisplayName("L3 contra el bundle REAL: un put valido pasa, uno roto falla ANTES de escribir con INVALID_SCHEMA (y se cuenta), y la lapida valida contra `eliminado`")
    void validacionL3() throws Exception {
        SchemaBundle bundle = SchemaBundle.fromPath(Repo.root().resolve("schemas/bundle.json"));
        InMemoryMetrics metrics = new InMemoryMetrics();
        try (FluxBus crm = FluxBus.connect(env.options("crm-api").tenantId("acme").metrics(metrics)
                .validation(new Validation.Options().mode(Validation.Mode.STRICT).bundle(bundle)))) {
            assertTrue(crm.state().put(CONTRACT, "cli-1", cliente("cli-1", 1)).applied());
            ObjectNode roto = cliente("cli-2", 1);
            roto.put("email", "no es un correo");
            roto.put("extra", "no permitido");
            SchemaValidationException e = assertThrows(SchemaValidationException.class, () -> crm.state().put(CONTRACT, "cli-2", roto));
            assertEquals("INVALID_SCHEMA", e.code());
            assertTrue(e.errors().size() >= 2, "TODOS los errores, no el primero: " + e.errors());
            assertFalse(e.getMessage().contains("no es un correo"), "no copia VALORES a su mensaje: " + e.getMessage());
            assertTrue(env.ops().jetStreamManagement().getStreamInfo(STREAM).getStreamState().getMsgCount() == 1, "nada se escribio");
            assertTrue(crm.state().delete(CONTRACT, "cli-1", 2).applied(), "la lapida valida contra su esquema fijo");
            assertEquals(1L, metrics.counters().get("flux_state_writes_total{contract=\"clientes.cliente.v1\",outcome=\"invalid_schema\"}"));
            // MISSING_AGGREGATE_VERSION tambien se cuenta
            ObjectNode sinVersion = cliente("cli-3", 1);
            sinVersion.remove("aggregateVersion");
            assertThrows(MissingAggregateVersionException.class, () -> crm.state().put(CONTRACT, "cli-3", sinVersion));
            assertEquals(2L, metrics.counters().get("flux_state_writes_total{contract=\"clientes.cliente.v1\",outcome=\"invalid_schema\"}"));
            // Un contrato que no esta en el bundle, en modo estricto: SCHEMA_NOT_FOUND
            assertThrows(SchemaNotFoundException.class, () -> crm.state().put("clientes.sin-esquema.v1", "k", cliente("k", 1)));
        }
    }

    @Test
    @DisplayName("la firma Ed25519 de un put verifica en quien lo observa (require), y un valor SIN firma es POISON para un observador que la exige")
    void conFirma() throws Exception {
        Signing.KeyPairPem par = Signing.generateKeyPair();
        List<String> vistas = new CopyOnWriteArrayList<>();
        List<String> poison = new CopyOnWriteArrayList<>();
        try (FluxBus crm = FluxBus.connect(env.options("crm-api").tenantId("acme")
                .signing(new Signing.SigningOptions().privateKeyPem(par.privateKeyPem()).keyId("crm-1")));
             FluxBus sinFirmar = crm();
             FluxBus informes = FluxBus.connect(env.options("informes-api")
                     .signing(new Signing.SigningOptions().publicKey("crm-1", par.publicKeyPem()).verify(Signing.VerificationMode.REQUIRE))
                     .onPoison(info -> poison.add(((FluxErrors.FluxException) info.error()).code())))) {
            crm.state().put(CONTRACT, "firmado", cliente("firmado", 1));
            sinFirmar.state().put(CONTRACT, "sin-firma", cliente("sin-firma", 1));
            informes.state().watch(CONTRACT, (ctx, entry, d) -> vistas.add(entry.key()));
            await("el firmado", 10_000, () -> vistas.contains("firmado"));
            await("el POISON", 10_000, () -> !poison.isEmpty());
            assertEquals(List.of("firmado"), vistas);
            assertEquals(List.of("MISSING_SIGNATURE"), poison);
        }
    }

    @Test
    @DisplayName("un put dentro de un handler hereda el tenant y la cadena de correlacion del evento en curso (EventContext explicito)")
    void contextoHeredado() throws Exception {
        try (FluxBus crm = FluxBus.connect(env.options("crm-api"))) {
            EventContext padre = new EventContext("corr-123", "evt-9", "globex", null, null);
            crm.state().put(padre, CONTRACT, "cli-1", cliente("cli-1", 1), new FluxState.PutOptions());
            MessageInfo m = env.ops().jetStreamManagement().getLastMessage(STREAM, "state." + CONTRACT + ".globex.cli-1");
            JsonNode e = Envelope.mapper().readTree(m.getData());
            assertEquals("corr-123", e.get("correlationid").textValue());
            assertEquals("evt-9", e.get("causationid").textValue());
            assertEquals("globex", e.get("tenantid").textValue());
        }
    }

    @SuppressWarnings("unused")
    private static void unused(CountDownLatch l, MessageInfo m) {
        assertNotNull(m);
        assertInstanceOf(Object.class, l);
    }
}
