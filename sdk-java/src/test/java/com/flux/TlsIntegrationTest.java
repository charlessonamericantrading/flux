/*
 * La conexion contra un nats-server REAL: TLS con CA propia, credenciales y la PRIMERA conexion.
 *
 * Cada fila de 03-delivery.md §6.1 tiene su caso, y cada caso comprueba QUE codigo sale, no solo que sale rapido: una fila que
 * fallara «rapido» pero con el codigo de otra mandaria a quien opera a buscar donde no es (04-errors.md §5.3). Tambien se
 * prueba lo contrario —que lo que SI se arregla esperando se espera— y que la libreria sola NO lo hace (A-200).
 *
 *   NATS_SERVER=/ruta/a/nats-server FLUX_REQUIRE_BROKER=1 mvn test -Dtest=TlsIntegrationTest
 */
package com.flux;

import static com.flux.TestSupport.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flux.SecurityErrors.ConnectionRejectedException;
import com.flux.SecurityErrors.InvalidCredentialsException;
import com.flux.SecurityErrors.InvalidTlsOptionsException;
import com.flux.SecurityErrors.RejectionReason;
import com.flux.broker.NatsServer;
import com.flux.broker.Pki;
import com.flux.broker.SecureEnv;
import io.nats.client.Connection;
import io.nats.client.NKey;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TlsIntegrationTest {

    private static Pki pki;
    private static NatsServer tls;
    private static NatsServer plain;
    private static NatsServer anyHost;
    private static Path tmp;

    @BeforeAll
    static void start() throws Exception {
        NatsServer.requireBinary();
        NatsServer.requireTool("openssl", "version");
        tmp = Files.createTempDirectory("flux-java-tls-");
        pki = Pki.create(tmp);
        String tlsBlock = "tls {\n  cert_file: \"" + pki.serverCert + "\"\n  key_file: \"" + pki.serverKey + "\"\n  timeout: 2\n}\n"
                + "authorization { user: u, password: p }\n";
        tls = NatsServer.start("127.0.0.1", tlsBlock, false);
        plain = NatsServer.start("127.0.0.1", "authorization { user: u, password: p }\n", false);
        // Escucha en todas las interfaces solo para poder llegar por `127.0.0.2`, un host que el certificado NO cubre.
        anyHost = NatsServer.start("0.0.0.0", tlsBlock, false);
    }

    @AfterAll
    static void stop() {
        for (NatsServer s : new NatsServer[] {tls, plain, anyHost}) {
            if (s != null) {
                s.close();
            }
        }
        if (tmp != null) {
            NatsServer.deleteRecursively(tmp);
        }
    }

    private static FluxBus.ConnectOptions options(String url) {
        return new FluxBus.ConnectOptions().servers(url).service("svc").environment("dev").version("1.0.0")
                .schemaBaseUrl("https://schemas.internal")
                .credentials(new FluxBus.Credentials().userPassword("u", "p"));
    }

    private static TlsOptions ca() {
        return new TlsOptions().caFile(pki.ca.toString());
    }

    /** `connect()` debe FALLAR, y rapido: devuelve el error y lo que tardo. */
    private static ConnectionRejectedException rejected(FluxBus.ConnectOptions o, long maxMillis) {
        long t0 = System.nanoTime();
        ConnectionRejectedException e = assertThrows(ConnectionRejectedException.class, () -> FluxBus.connect(o));
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms < maxMillis, "debe fallar de inmediato, no esperar: " + ms + " ms");
        assertEquals("CONNECTION_REJECTED", e.code());
        assertEquals(ErrorClass.PERMANENT, e.errorClass());
        assertTrue(e.getMessage().contains("Reintentar no lo arregla") || e.getMessage().contains("Reintentar no lo"),
                "dice que reintentar no lo arregla: " + e.getMessage());
        return e;
    }

    // ─── TLS con CA propia ───────────────────────────────────────────────────

    @Test
    @DisplayName("TLS con una CA propia: conecta y verifica el host; el control de todos los casos de abajo")
    void tlsConCaPropia() throws Exception {
        try (FluxBus bus = FluxBus.connect(options("tls://127.0.0.1:" + tls.port()).tls(ca()))) {
            assertTrue(bus.isConnected());
        }
        // `nats://` con `tls`: la libreria sube a TLS y el resultado es el mismo
        try (FluxBus bus = FluxBus.connect(options("nats://127.0.0.1:" + tls.port()).tls(ca()))) {
            assertTrue(bus.isConnected());
        }
    }

    @Test
    @DisplayName("una CA que NO firmo el certificado → CONNECTION_REJECTED (tls), no INVALID_TLS_OPTIONS ni un cuelgue")
    void caQueNoFirmo() {
        ConnectionRejectedException e = rejected(
                options("tls://127.0.0.1:" + tls.port()).tls(new TlsOptions().caFile(pki.otherCa.toString())), 5000);
        assertEquals(RejectionReason.TLS, e.reason());
        assertTrue(e.getMessage().contains("127.0.0.1:" + tls.port()), "nombra el broker: " + e.getMessage());
        assertTrue(e.getMessage().contains("caFile"), "dice que revisar: " + e.getMessage());
        assertTrue(e.getMessage().contains("SAN"), e.getMessage());
    }

    @Test
    @DisplayName("sin `caFile` la CA propia no esta en el almacen del sistema: tambien es CONNECTION_REJECTED (tls)")
    void sinLaCaDelBroker() {
        assertEquals(RejectionReason.TLS, rejected(options("tls://127.0.0.1:" + tls.port()).tls(new TlsOptions()), 5000).reason());
    }

    @Test
    @DisplayName("un host fuera de los SAN del certificado → CONNECTION_REJECTED (tls): la libreria sola conecta, el SDK NO")
    void hostFueraDeLosSan() throws Exception {
        // Control: el MISMO broker por un host que el certificado cubre.
        try (FluxBus bus = FluxBus.connect(options("tls://127.0.0.1:" + anyHost.port()).tls(ca()))) {
            assertTrue(bus.isConnected());
        }
        ConnectionRejectedException e = rejected(options("tls://127.0.0.2:" + anyHost.port()).tls(ca()), 5000);
        assertEquals(RejectionReason.TLS, e.reason());
        assertTrue(e.getMessage().contains("127.0.0.2"), e.getMessage());

        // La libreria SOLA no verifica el host (A-200): con el contexto de la CA, conecta a un host fuera de los SAN.
        Options sola = new Options.Builder().server("tls://127.0.0.2:" + anyHost.port()).userInfo("u", "p")
                .sslContext(SecureEnv.trusting(pki.ca)).maxReconnects(0).build();
        try (Connection c = Nats.connect(sola)) {
            assertEquals(Connection.Status.CONNECTED, c.getStatus(), "jnats no verifica el host por si sola: por eso lo impone el SDK");
        }
    }

    @Test
    @DisplayName("TLS pedido y el broker NO lo ofrece → CONNECTION_REJECTED (tls) y NO baja a texto plano")
    void tlsPedidoSinTlsEnElBroker() {
        ConnectionRejectedException e = rejected(options("tls://127.0.0.1:" + plain.port()).tls(ca()), 5000);
        assertEquals(RejectionReason.TLS, e.reason());
        assertTrue(e.getMessage().contains("no ofrece TLS"), e.getMessage());
        // Con `nats://` y `tls` tambien: el SDK no degrada.
        assertEquals(RejectionReason.TLS, rejected(options("nats://127.0.0.1:" + plain.port()).tls(ca()), 5000).reason());
    }

    @Test
    @DisplayName("el broker EXIGE TLS y el servicio no lo configuro → CONNECTION_REJECTED (tls), con el remedio en el mensaje")
    void tlsExigidoYNoConfigurado() {
        ConnectionRejectedException e = rejected(options("nats://127.0.0.1:" + tls.port()), 5000);
        assertEquals(RejectionReason.TLS, e.reason());
        assertTrue(e.getMessage().contains("exige TLS"), e.getMessage());
        assertTrue(e.getMessage().contains("TlsOptions"), e.getMessage());
    }

    // ─── mTLS ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("mTLS: con certificado y clave de cliente (PKCS#8 y SEC1) conecta; la clave se lee en los dos formatos")
    void tlsMutuo() throws Exception {
        // Sin `flux provision`: la CLI no habla mTLS. Aqui solo importa que la conexion se establezca.
        try (SecureEnv env = SecureEnv.builder().mtls(true).provision(false).build()) {
            for (Path key : new Path[] {pki().clientKeyPkcs8, pki().clientKey}) {
                // La CA de este entorno es la suya: se firma el certificado de cliente con ELLA (la de `env.pki`).
                FluxBus.ConnectOptions o = env.options("pedidos-api").tls(new TlsOptions().caFile(env.pki.ca.toString())
                        .certFile(env.pki.clientCert.toString())
                        .keyFile(key == pki().clientKeyPkcs8 ? env.pki.clientKeyPkcs8.toString() : env.pki.clientKey.toString()));
                try (FluxBus bus = FluxBus.connect(o)) {
                    assertTrue(bus.isConnected(), "mTLS con " + key.getFileName());
                    // Y habla de verdad por el canal: lo que contesta el broker (falta el stream) llega como el error de siempre.
                    assertThrows(SecurityErrors.NotProvisionedException.class,
                            () -> bus.publish("pedidos.pedido.v1.creado", Map.of("pedidoId", "mtls")));
                }
            }
        }
    }

    private static Pki pki() {
        return pki;
    }

    @Test
    @DisplayName("mTLS sin certificado de cliente: el broker lo rechaza y el SDK lo dice, o espera sin colgar para siempre")
    void tlsMutuoSinCertificadoDeCliente() throws Exception {
        try (SecureEnv env = SecureEnv.builder().mtls(true).provision(false).build()) {
            FluxBus.ConnectOptions o = env.options("pedidos-api");
            long t0 = System.nanoTime();
            ConnectionRejectedException e = assertThrows(ConnectionRejectedException.class, () -> FluxBus.connect(o));
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertEquals(RejectionReason.TLS, e.reason());
            assertTrue(ms < 15_000, "no se cuelga: " + ms + " ms");
            assertTrue(e.getMessage().contains("mTLS"), "lista la causa candidata: " + e.getMessage());
        }
    }

    // ─── credenciales ────────────────────────────────────────────────────────

    @Test
    @DisplayName("usuario y contraseña: la correcta conecta; una BIEN FORMADA pero equivocada es CONNECTION_REJECTED (auth), nunca INVALID_CREDENTIALS")
    void usuarioYContrasena() throws Exception {
        try (FluxBus bus = FluxBus.connect(options("tls://127.0.0.1:" + tls.port()).tls(ca()))) {
            assertTrue(bus.isConnected());
        }
        ConnectionRejectedException e = rejected(options("tls://127.0.0.1:" + tls.port()).tls(ca())
                .credentials(new FluxBus.Credentials().userPassword("u", "otra")), 8000);
        assertEquals(RejectionReason.AUTH, e.reason());
        assertTrue(e.getMessage().contains("credentials"), "dice que revisar: " + e.getMessage());
        assertFalse(e.getMessage().contains("otra"), "ninguna credencial en el mensaje");
    }

    @Test
    @DisplayName("token y nkey: la buena conecta, la equivocada es CONNECTION_REJECTED (auth)")
    void tokenYNkey() throws Exception {
        try (NatsServer tokenBroker = NatsServer.start("127.0.0.1", "authorization { token: \"s3cret\" }\n", false)) {
            try (FluxBus bus = FluxBus.connect(options("nats://127.0.0.1:" + tokenBroker.port())
                    .credentials(new FluxBus.Credentials().token("s3cret")))) {
                assertTrue(bus.isConnected());
            }
            ConnectionRejectedException e = rejected(options("nats://127.0.0.1:" + tokenBroker.port())
                    .credentials(new FluxBus.Credentials().token("equivocado")), 8000);
            assertEquals(RejectionReason.AUTH, e.reason());
        }
        NKey bueno = NKey.createUser(new SecureRandom());
        NKey otro = NKey.createUser(new SecureRandom());
        try (NatsServer nkeyBroker = NatsServer.start("127.0.0.1",
                "authorization { users: [ { nkey: \"" + new String(bueno.getPublicKey()) + "\" } ] }\n", false)) {
            try (FluxBus bus = FluxBus.connect(options("nats://127.0.0.1:" + nkeyBroker.port())
                    .credentials(new FluxBus.Credentials().nkeySeed(new String(bueno.getSeed()))))) {
                assertTrue(bus.isConnected());
            }
            // El mismo formato, otra clave: bien formada, rechazada.
            assertEquals(RejectionReason.AUTH, rejected(options("nats://127.0.0.1:" + nkeyBroker.port())
                    .credentials(new FluxBus.Credentials().nkeySeed(new String(otro.getSeed()))), 8000).reason());
            // Y un `.creds` con la semilla buena: se relee de fichero y funciona.
            Path creds = tmp.resolve("bueno.creds");
            Files.writeString(creds, "-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QifQ.e30.relleno\n------END NATS USER JWT------\n\n"
                    + "-----BEGIN USER NKEY SEED-----\n" + new String(bueno.getSeed()) + "\n------END USER NKEY SEED------\n");
            try (FluxBus bus = FluxBus.connect(options("nats://127.0.0.1:" + nkeyBroker.port())
                    .credentials(new FluxBus.Credentials().credsFile(creds.toString())))) {
                assertTrue(bus.isConnected());
            }
        }
    }

    @Test
    @DisplayName("cada codigo de connect() sale en su situacion y en NINGUNA otra")
    void cadaCodigoEnSuSituacion() {
        // INVALID_CREDENTIALS: sin hablar con el broker (ni siquiera existe el puerto)
        InvalidCredentialsException ic = assertThrows(InvalidCredentialsException.class, () -> FluxBus.connect(
                options("nats://127.0.0.1:1").credentials(new FluxBus.Credentials().credsFile(tmp.resolve("no-existe.creds").toString()))));
        assertEquals("INVALID_CREDENTIALS", ic.code());
        // INVALID_TLS_OPTIONS: sin red tampoco, y NO es CONNECTION_REJECTED
        InvalidTlsOptionsException it = assertThrows(InvalidTlsOptionsException.class, () -> FluxBus.connect(
                options("tls://127.0.0.1:1").tls(new TlsOptions().caFile(tmp.resolve("no-existe.pem").toString()))));
        assertEquals("INVALID_TLS_OPTIONS", it.code());
        // Los otros fallos de connect() NO llevan ninguno de los tres: no son esas situaciones.
        for (FluxBus.ConnectOptions mal : new FluxBus.ConnectOptions[] {
                options("nats://127.0.0.1:1").service("Servicio.Con.Puntos"),
                options("nats://127.0.0.1:1").service(null)}) {
            Exception e = assertThrows(IllegalArgumentException.class, () -> FluxBus.connect(mal));
            assertFalse(e instanceof FluxErrors.FluxException, "no lleva un codigo de seguridad: " + e);
        }
    }

    // ─── la primera conexion: esperar al broker, y solo a el ─────────────────

    @Test
    @DisplayName("un broker que AUN no ha arrancado se espera (no es ninguno de los tres codigos) y se conecta cuando arranca")
    void seEsperaAlBrokerQueNoHaArrancado() throws Exception {
        int port = NatsServer.freePort();
        AtomicReference<Object> result = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                result.set(FluxBus.connect(options("nats://127.0.0.1:" + port)));
            } catch (Throwable t) {
                result.set(t);
            }
        });
        thread.setDaemon(true);
        thread.start();
        try {
            // Mientras no hay broker: no falla, no se rinde, y no lanza ninguno de los tres codigos.
            thread.join(2500);
            assertTrue(thread.isAlive(), "sigue esperando al broker; ha terminado con " + result.get());
            assertEquals(null, result.get());
            try (NatsServer late = NatsServer.startOn(port, "127.0.0.1", "authorization { user: u, password: p }\n", false, Map.of())) {
                thread.join(15_000);
                assertFalse(thread.isAlive(), "debe conectar en cuanto el broker arranca");
                assertInstanceOf(FluxBus.class, result.get(), String.valueOf(result.get()));
                ((FluxBus) result.get()).close();
            }
        } finally {
            thread.interrupt();
        }
    }

    @Test
    @DisplayName("esperar al broker se puede interrumpir: restaura el flag y lanza un error SIN codigo de seguridad")
    void esperarSePuedeInterrumpir() throws Exception {
        int port = NatsServer.freePort();
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicReference<Boolean> flag = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                FluxBus.connect(options("nats://127.0.0.1:" + port));
            } catch (Throwable t) {
                error.set(t);
                flag.set(Thread.currentThread().isInterrupted());
            }
        });
        thread.setDaemon(true);
        thread.start();
        thread.join(1500);
        assertTrue(thread.isAlive());
        thread.interrupt();
        thread.join(5000);
        assertFalse(thread.isAlive());
        assertInstanceOf(FluxBus.FluxBusException.class, error.get());
        assertFalse(error.get() instanceof FluxErrors.FluxException);
        assertEquals(Boolean.TRUE, flag.get(), "tragarse el flag rompe el apagado ordenado");
    }

    /** Ejecuta jnats SOLA en otra JVM y devuelve lo que dijo. Se mata el proceso entero: un cuelgue deja hilos no-daemon. */
    private static String jnatsSola(String scenario, int port, Path otherCa) throws Exception {
        Process p = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
                System.getProperty("java.class.path"), "com.flux.JnatsAlone", scenario, "tls://127.0.0.1:" + port,
                pki.ca.toString(), otherCa.toString()).redirectErrorStream(true).start();
        try {
            if (!p.waitFor(60, TimeUnit.SECONDS)) {
                return "SIN TERMINAR";
            }
            String out = new String(p.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            for (String line : out.split("\n")) {
                if (line.startsWith("HANG") || line.startsWith("RETURNED") || line.startsWith("THROWS")) {
                    return line.trim();
                }
            }
            return out;
        } finally {
            p.destroyForcibly();
        }
    }

    @Test
    @DisplayName("A-200: `Nats.connectReconnectOnConnect` de la libreria SE CUELGA con una CA que no firmo, y con una credencial rechazada devuelve una conexion CERRADA")
    void laLibreriaSolaNoFallaRapido() throws Exception {
        assertEquals("HANG", jnatsSola("ca", tls.port(), pki.otherCa),
                "si deja de colgarse, el bucle propio del SDK ya no haria falta: revisa A-200");
        assertEquals("RETURNED CLOSED", jnatsSola("credenciales", tls.port(), pki.otherCa),
                "la libreria devuelve una conexion cerrada y ninguna excepcion: el servicio la usaria sin saberlo");
    }
}
