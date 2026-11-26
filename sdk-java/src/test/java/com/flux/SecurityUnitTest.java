/*
 * Seguridad de la conexion SIN broker: lo que se valida antes de abrir ningun socket.
 *   · INVALID_CREDENTIALS: credenciales mal formadas, y que el mensaje NUNCA cite su valor.
 *   · INVALID_TLS_OPTIONS: opciones TLS mal formadas.
 *   · el inbox privado, las denegaciones (como se leen y se normalizan) y la clasificacion de los fallos de la
 *     primera conexion.
 * El comportamiento contra un broker real lo prueban SecurityIntegrationTest y TlsIntegrationTest.
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flux.SecurityErrors.ConnectionRejectedException;
import com.flux.SecurityErrors.InvalidCredentialsException;
import com.flux.SecurityErrors.InvalidTlsOptionsException;
import com.flux.SecurityErrors.RejectionReason;
import io.nats.client.AuthenticationException;
import io.nats.client.NKey;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.util.List;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SecurityUnitTest {

    @TempDir
    Path tmp;

    private static String seed() throws Exception {
        return new String(NKey.createUser(new SecureRandom()).getSeed());
    }

    private static final String JWT_FILLER = "eyJ0eXAiOiJKV1QifQ.e30.relleno";

    private static String creds(String seed) {
        return "-----BEGIN NATS USER JWT-----\n" + JWT_FILLER + "\n------END NATS USER JWT------\n\n"
                + "-----BEGIN USER NKEY SEED-----\n" + seed + "\n------END USER NKEY SEED------\n";
    }

    private static InvalidCredentialsException invalid(FluxBus.Credentials c) {
        return assertThrows(InvalidCredentialsException.class, () -> Security.credentials(c));
    }

    // ─── inbox privado ───────────────────────────────────────────────────────

    @Test
    @DisplayName("el inbox es _INBOX.<servicio>, derivado del nombre y no configurable")
    void inboxPrivado() {
        assertEquals("_INBOX.facturacion-api", Security.inboxPrefixFor("facturacion-api"));
        assertThrows(Protocol.InvalidServiceNameException.class, () -> Security.inboxPrefixFor("Facturacion.API"));
        // El nombre no admite puntos: el prefijo de un servicio nunca puede ser prefijo del de otro.
        assertThrows(Protocol.InvalidServiceNameException.class, () -> Security.inboxPrefixFor("a.b"));
    }

    // ─── INVALID_CREDENTIALS ─────────────────────────────────────────────────

    @Test
    @DisplayName("sin credenciales no hay autenticacion; con un objeto vacio tampoco")
    void sinCredenciales() {
        assertNull(Security.credentials(null));
        assertNull(Security.credentials(new FluxBus.Credentials()));
    }

    @Test
    @DisplayName("cada metodo valido produce una autenticacion")
    void metodosValidos() throws Exception {
        assertNotNull(Security.credentials(new FluxBus.Credentials().token("t0ken")));
        assertNotNull(Security.credentials(new FluxBus.Credentials().userPassword("u", "p")));
        String s = seed();
        Security.Auth nkey = Security.credentials(new FluxBus.Credentials().nkeySeed(s));
        assertNotNull(nkey.handler);
        assertEquals(new String(NKey.fromSeed(s.toCharArray()).getPublicKey()), new String(nkey.handler.getID()));
        assertNull(nkey.handler.getJWT(), "una nkey sin JWT: el broker la trata como nkey");
        assertNotNull(Security.credentials(new FluxBus.Credentials().credsContent(creds(s))));
        Path f = tmp.resolve("ok.creds");
        Files.writeString(f, creds(s));
        assertNotNull(Security.credentials(new FluxBus.Credentials().credsFile(f.toString())));
    }

    @Test
    @DisplayName("un valor vacio o en blanco (una variable de entorno sin definir) es INVALID_CREDENTIALS, no «sin credenciales»")
    void valoresVacios() {
        for (FluxBus.Credentials c : List.of(
                new FluxBus.Credentials().token(""),
                new FluxBus.Credentials().token("   "),
                new FluxBus.Credentials().nkeySeed(""),
                new FluxBus.Credentials().credsFile(""),
                new FluxBus.Credentials().credsContent(" "),
                new FluxBus.Credentials().userPassword("", "p"),
                new FluxBus.Credentials().userPassword("u", ""))) {
            assertEquals("INVALID_CREDENTIALS", invalid(c).code());
        }
    }

    @Test
    @DisplayName("user sin password, o al reves")
    void usuarioSinContrasena() {
        assertEquals("INVALID_CREDENTIALS", invalid(new FluxBus.Credentials().userPassword("u", null)).code());
        assertEquals("INVALID_CREDENTIALS", invalid(new FluxBus.Credentials().userPassword(null, "p")).code());
    }

    @Test
    @DisplayName("mas de un metodo es ambiguo: el servidor aceptaria uno y el otro seria ruido")
    void masDeUnMetodo() throws Exception {
        InvalidCredentialsException e = invalid(new FluxBus.Credentials().token("t").nkeySeed(seed()));
        assertTrue(e.getMessage().contains("elige UN metodo"), e.getMessage());
        assertEquals("INVALID_CREDENTIALS", invalid(new FluxBus.Credentials().token("t").userPassword("u", "p")).code());
    }

    @Test
    @DisplayName("la semilla debe ser de USUARIO (SU…) y valida")
    void semillaDeUsuario() throws Exception {
        String cuenta = new String(NKey.createAccount(new SecureRandom()).getSeed());
        assertTrue(cuenta.startsWith("SA"));
        assertEquals("INVALID_CREDENTIALS", invalid(new FluxBus.Credentials().nkeySeed(cuenta)).code());
        assertEquals("INVALID_CREDENTIALS", invalid(new FluxBus.Credentials().nkeySeed("SUNOESUNASEMILLA")).code());
    }

    @Test
    @DisplayName("un .creds ilegible, sin JWT o sin semilla, o con una semilla que no es de usuario")
    void credsMalFormado() throws Exception {
        assertEquals("INVALID_CREDENTIALS",
                invalid(new FluxBus.Credentials().credsFile(tmp.resolve("no-existe.creds").toString())).code());
        assertEquals("INVALID_CREDENTIALS", invalid(new FluxBus.Credentials().credsFile(tmp.toString())).code(),
                "un directorio no es un fichero");
        Path sinNada = tmp.resolve("vacio.creds");
        Files.writeString(sinNada, "hola");
        assertEquals("INVALID_CREDENTIALS", invalid(new FluxBus.Credentials().credsFile(sinNada.toString())).code());
        // Sin JWT
        String sinJwt = creds(seed()).replaceAll("(?s)-----BEGIN NATS USER JWT-----.*?------END NATS USER JWT------", "");
        assertEquals("INVALID_CREDENTIALS", invalid(new FluxBus.Credentials().credsContent(sinJwt)).code());
        // Sin semilla
        assertEquals("INVALID_CREDENTIALS", invalid(new FluxBus.Credentials().credsContent(
                "-----BEGIN NATS USER JWT-----\n" + JWT_FILLER + "\n------END NATS USER JWT------\n")).code());
        // Semilla que no es de usuario
        String cuenta = new String(NKey.createAccount(new SecureRandom()).getSeed());
        assertEquals("INVALID_CREDENTIALS", invalid(new FluxBus.Credentials().credsContent(creds(cuenta))).code());
    }

    @Test
    @DisplayName("el mensaje NO cita el valor de ninguna credencial (acaba en logs) pero SI la ruta")
    void elMensajeNoFiltraSecretos() throws Exception {
        String secreto = "s3cr3t-NO-DEBE-SALIR";
        for (FluxBus.Credentials c : List.of(
                new FluxBus.Credentials().token(secreto).nkeySeed(secreto),
                new FluxBus.Credentials().nkeySeed(secreto),
                new FluxBus.Credentials().credsContent(secreto),
                new FluxBus.Credentials().userPassword("usuario", secreto).token(secreto))) {
            assertFalse(invalid(c).getMessage().contains(secreto), "filtra una credencial");
        }
        Path ruta = tmp.resolve("falta.creds");
        assertTrue(invalid(new FluxBus.Credentials().credsFile(ruta.toString())).getMessage().contains(ruta.toString()));
    }

    @Test
    @DisplayName("INVALID_CREDENTIALS es PERMANENT y lleva un codigo fijo")
    void esPermanente() {
        InvalidCredentialsException e = invalid(new FluxBus.Credentials().token(" "));
        assertEquals(ErrorClass.PERMANENT, e.errorClass());
        assertEquals("INVALID_CREDENTIALS", e.code());
    }

    // ─── INVALID_TLS_OPTIONS ─────────────────────────────────────────────────

    private static InvalidTlsOptionsException invalidTls(TlsOptions t) {
        return assertThrows(InvalidTlsOptionsException.class, () -> Security.sslContext(t));
    }

    @Test
    @DisplayName("sin tls no hay contexto; con `new TlsOptions()` hay TLS con las CA del sistema")
    void tlsPorDefecto() {
        assertNull(Security.sslContext(null));
        assertNotNull(Security.sslContext(new TlsOptions()), "`tls: {}` EXIGE TLS");
    }

    @Test
    @DisplayName("una ruta vacia, que no existe o que no es un PEM: INVALID_TLS_OPTIONS, y la ruta se cita")
    void rutasMalas() throws Exception {
        assertEquals("INVALID_TLS_OPTIONS", invalidTls(new TlsOptions().caFile("")).code());
        Path falta = tmp.resolve("ca-que-no-existe.pem");
        assertTrue(invalidTls(new TlsOptions().caFile(falta.toString())).getMessage().contains(falta.toString()));
        Path noPem = tmp.resolve("no-es-pem.pem");
        Files.writeString(noPem, "esto no es un certificado");
        assertEquals("INVALID_TLS_OPTIONS", invalidTls(new TlsOptions().caFile(noPem.toString())).code());
        Path pemRoto = tmp.resolve("roto.pem");
        Files.writeString(pemRoto, "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n");
        assertEquals("INVALID_TLS_OPTIONS", invalidTls(new TlsOptions().caFile(pemRoto.toString())).code());
        assertEquals("INVALID_TLS_OPTIONS", invalidTls(new TlsOptions().caFile(tmp.toString())).code(),
                "un directorio no es un fichero");
    }

    @Test
    @DisplayName("certFile y keyFile van juntos")
    void certYClaveJuntos() throws Exception {
        Path f = tmp.resolve("x.pem");
        Files.writeString(f, "x");
        assertTrue(invalidTls(new TlsOptions().certFile(f.toString())).getMessage().contains("van juntos"));
        assertTrue(invalidTls(new TlsOptions().keyFile(f.toString())).getMessage().contains("van juntos"));
    }

    @Test
    @DisplayName("INVALID_TLS_OPTIONS es PERMANENT")
    void tlsEsPermanente() {
        assertEquals(ErrorClass.PERMANENT, invalidTls(new TlsOptions().caFile("")).errorClass());
    }

    // ─── denegaciones ────────────────────────────────────────────────────────

    @Test
    @DisplayName("una denegacion se lee del -ERR de la libreria: operacion y subject COMPLETO")
    void leerDenegaciones() {
        Denials.Denial d = Denials.parse("Permissions Violation for Publish to \"$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.fx__p\"");
        assertEquals(SecurityErrors.Operation.PUBLISH, d.operation());
        assertEquals("$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.fx__p", d.subject());
        Denials.Denial s = Denials.parse("Permissions Violation for Subscription to \"rpc.clientes.>\"");
        assertEquals(SecurityErrors.Operation.SUBSCRIBE, s.operation());
        assertEquals("rpc.clientes.>", s.subject());
        assertNull(Denials.parse("Authorization Violation"));
        assertNull(Denials.parse("Slow Consumer"));
        assertNull(Denials.parse(null));
    }

    @Test
    @DisplayName("el subject de un ack o de una respuesta se normaliza para avisar UNA vez, no una por mensaje")
    void normalizar() {
        assertEquals("$JS.ACK.EVT_PEDIDOS.fx__p.>", Denials.normalize("$JS.ACK.EVT_PEDIDOS.fx__p.1.1.1.1700000000.0"));
        assertEquals("_INBOX.crm-api.>", Denials.normalize("_INBOX.crm-api.AbC123.xYz"));
        assertEquals("pedidos.pedido.v1.creado", Denials.normalize("pedidos.pedido.v1.creado"));
        assertEquals("$JS.API.INFO", Denials.normalize("$JS.API.INFO"));

        Denials denials = new Denials();
        try {
            String ack = "Permissions Violation for Publish to \"$JS.ACK.EVT_PEDIDOS.fx__p.1.1.1.1.0\"";
            assertNotNull(denials.onError(ack), "la primera vez se registra");
            assertNull(denials.onError(ack.replace(".1.1.1.1.0", ".2.2.2.2.0")), "la segunda del mismo durable no");
            assertNull(denials.onError("Slow Consumer"), "otro -ERR no es una denegacion");
            assertTrue(denials.wasDenied("$JS.ACK.EVT_PEDIDOS.fx__p.9.9.9.9.9"));
            denials.forget("$JS.ACK.EVT_PEDIDOS.fx__p.>");
            assertFalse(denials.wasDenied("$JS.ACK.EVT_PEDIDOS.fx__p.9.9.9.9.9"));
        } finally {
            denials.close();
        }
    }

    @Test
    @DisplayName("quien espera un subject se entera de SU denegacion y de ninguna otra")
    void esperarUnaDenegacion() throws Exception {
        Denials denials = new Denials();
        try {
            var mine = denials.watch("rpc.a.b.v1.c");
            var other = denials.watch("rpc.x.y.v1.z");
            denials.onError("Permissions Violation for Publish to \"rpc.a.b.v1.c\"");
            assertTrue(mine.isDone());
            assertFalse(other.isDone(), "la denegacion de otro subject no es culpa de esta operacion");
            assertEquals("rpc.a.b.v1.c", mine.get().subject());
        } finally {
            denials.close();
        }
    }

    @Test
    @DisplayName("guard: la denegacion gana al timeout de la libreria; sin denegacion gana el resultado")
    void guard() throws Exception {
        Denials denials = new Denials();
        try {
            // Una llamada que tardaria 30 s (el timeout de la libreria) y el broker deniega a los 50 ms.
            long t0 = System.nanoTime();
            Thread aviso = new Thread(() -> {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ignored) {
                    return;
                }
                denials.onError("Permissions Violation for Publish to \"$JS.API.STREAM.INFO.EVT_X\"");
            });
            aviso.start();
            SecurityErrors.PermissionDeniedException e = assertThrows(SecurityErrors.PermissionDeniedException.class,
                    () -> denials.guard("svc", List.of("$JS.API.STREAM.INFO.EVT_X"), null, () -> {
                        Thread.sleep(30_000);
                        return "nunca";
                    }));
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms < 2000, "debe fallar en tiempo util, no por timeout: " + ms + " ms");
            assertEquals("PERMISSION_DENIED", e.code());
            assertEquals("$JS.API.STREAM.INFO.EVT_X", e.subject());
            assertEquals("svc", e.service());

            // Sin denegacion, el resultado.
            assertEquals("ok", denials.guard("svc", List.of("$JS.API.STREAM.INFO.EVT_Y"), null, () -> "ok"));
            // Una excepcion de la llamada se propaga TAL CUAL, no se disfraza de denegacion.
            IOException io = assertThrows(IOException.class, () -> denials.guard("svc", List.of("x"), null, () -> {
                throw new IOException("la propia llamada fallo");
            }));
            assertEquals("la propia llamada fallo", io.getMessage());
        } finally {
            denials.close();
        }
    }

    // ─── la PRIMERA conexion: que espera y que no ────────────────────────────

    private static ConnectionRejectedException rejected(Throwable thrown, Throwable... seen) {
        return Security.permanentConnectError(thrown, List.of(seen), "tls://broker:4222", true);
    }

    @Test
    @DisplayName("credencial rechazada → CONNECTION_REJECTED (auth); no es INVALID_CREDENTIALS")
    void rechazoAuth() {
        ConnectionRejectedException e = rejected(new AuthenticationException("Authorization Violation"));
        assertNotNull(e);
        assertEquals(RejectionReason.AUTH, e.reason());
        assertEquals("CONNECTION_REJECTED", e.code());
        assertEquals(ErrorClass.PERMANENT, e.errorClass());
        assertTrue(e.getMessage().contains("tls://broker:4222"), "nombra el broker");
        assertTrue(e.getMessage().contains("Reintentar no lo arregla"), "dice que reintentar no lo arregla");
    }

    @Test
    @DisplayName("un certificado que no verifica, en cualquiera de sus formas → CONNECTION_REJECTED (tls)")
    void rechazoTls() {
        for (Throwable causa : List.<Throwable>of(
                new SSLHandshakeException("(certificate_unknown) PKIX path building failed: unable to find valid certification path"),
                new SSLHandshakeException("No subject alternative names matching IP address 127.0.0.2 found"),
                new SSLHandshakeException("(bad_certificate) Received fatal alert: bad_certificate"),
                new SSLHandshakeException("Received fatal alert: certificate_required"),
                new SSLHandshakeException("PKIX path validation failed: java.security.cert.CertPathValidatorException: validity check failed"),
                new CertificateException("NotAfter: Thu Nov 26 00:00:00 UTC 2026"))) {
            ConnectionRejectedException e = rejected(new IOException("Unable to connect to NATS servers"), causa);
            assertNotNull(e, causa.toString());
            assertEquals(RejectionReason.TLS, e.reason(), causa.toString());
        }
        // «SSL connection wanted by client»: se pidio tls y el servidor no lo ofrece.
        ConnectionRejectedException sinTls = rejected(new IOException("Unable to connect"),
                new IOException("SSL connection wanted by client."));
        assertEquals(RejectionReason.TLS, sinTls.reason());
        assertTrue(sinTls.getMessage().contains("no ofrece TLS"), sinTls.getMessage());
        assertTrue(sinTls.getMessage().contains("no baja a texto plano"));
        // «SSL required by server»: el servidor lo exige y no se configuro.
        ConnectionRejectedException exigido = rejected(new IOException("Unable to connect"),
                new IOException("SSL required by server."));
        assertEquals(RejectionReason.TLS, exigido.reason());
        assertTrue(exigido.getMessage().contains("exige TLS"), exigido.getMessage());
    }

    @Test
    @DisplayName("un broker que aun no ha arrancado NO es ninguno de los tres: se espera")
    void seEsperaAlBroker() {
        for (Throwable causa : List.<Throwable>of(
                new ConnectException("Connection refused"),
                new UnknownHostException("nats.interno"),
                new java.net.SocketTimeoutException("connect timed out"),
                new java.util.concurrent.TimeoutException(),
                new SSLHandshakeException("Remote host terminated the handshake"),
                new java.net.SocketException("Connection reset"))) {
            assertNull(rejected(new IOException("Unable to connect to NATS servers"), causa), causa.toString());
        }
        assertNull(rejected(new IOException("Unable to connect to NATS servers")), "sin ninguna causa conocida, se espera");
    }

    @Test
    @DisplayName("la causa se busca en toda la cadena de causas")
    void causaEnLaCadena() {
        Throwable envuelta = new IOException("x", new IOException("y", new SSLHandshakeException("(unknown_ca) unknown_ca")));
        assertEquals(RejectionReason.TLS, rejected(new IOException("Unable to connect"), envuelta).reason());
    }

    @Test
    @DisplayName("las clases de error de seguridad son todas PERMANENT, con un codigo fijo")
    void clasesYCodigos() {
        var denied = new SecurityErrors.PermissionDeniedException("svc", SecurityErrors.Operation.PUBLISH, "a.b", null, null);
        var notProvisioned = new SecurityErrors.NotProvisionedException(SecurityErrors.ResourceKind.STREAM, "EVT_A", null,
                "svc", "$JS.API.STREAM.CREATE.EVT_A", null, null);
        for (FluxErrors.FluxException e : List.of(denied, notProvisioned,
                rejected(new AuthenticationException("x")), invalid(new FluxBus.Credentials().token(" ")),
                invalidTls(new TlsOptions().caFile("")))) {
            assertEquals(ErrorClass.PERMANENT, e.errorClass(), e.code());
            assertInstanceOf(RuntimeException.class, e);
        }
        assertEquals(List.of("NOT_PROVISIONED", "PERMISSION_DENIED", "CONNECTION_REJECTED", "INVALID_CREDENTIALS",
                "INVALID_TLS_OPTIONS"), SecurityErrors.CODES);
        // El clasificador respeta la clase declarada: un handler que la provoque va a la DLQ en la primera entrega.
        assertEquals(ErrorClass.PERMANENT, new Classifier().classify(denied).errorClass());
        assertEquals("NOT_PROVISIONED", new Classifier().classify(notProvisioned).code());
        assertTrue(notProvisioned.getMessage().contains("flux provision"));
        assertTrue(notProvisioned.getMessage().contains("$JS.API.STREAM.CREATE.EVT_A"));
    }

    // ─── Denials.race: el aviso se registra ANTES de lanzar la operacion ─────

    @Test
    @DisplayName("Denials.race: una denegacion que llega ANTES de empezar a esperar (el broker contesta en microsegundos) no se pierde: falla al instante, no tras el timeout")
    void laDenegacionQueLlegaAntesDeEsperarNoSePierde() throws Exception {
        Denials d = new Denials();
        long t0 = System.nanoTime();
        SecurityErrors.PermissionDeniedException e = assertThrows(SecurityErrors.PermissionDeniedException.class,
                () -> d.race("svc", "state.clientes.cliente.v1.acme.cli-1", null, () -> {
                    // lo que pasa DENTRO de la operacion: el -ERR llega antes de que quien la lanzo se ponga a esperar
                    d.onError("Permissions Violation for Publish to \"state.clientes.cliente.v1.acme.cli-1\"");
                    return new java.util.concurrent.CompletableFuture<String>();
                }, 5_000));
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 1_000, "no espero el timeout");
        assertEquals("state.clientes.cliente.v1.acme.cli-1", e.subject());
        // Y si la operacion contesta, gana la respuesta aunque haya una denegacion de OTRO subject
        String ok = d.race("svc", "a.b", null, () -> {
            d.onError("Permissions Violation for Publish to \"otro.subject\"");
            return java.util.concurrent.CompletableFuture.completedFuture("respuesta");
        }, 1_000);
        assertEquals("respuesta", ok);
    }
}
