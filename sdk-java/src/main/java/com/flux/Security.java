/*
 * Seguridad de la conexion: inbox privado, TLS, credenciales y fallos de la PRIMERA conexion.
 * Contrato normativo: specification/06-security.md §2 y §3, specification/04-errors.md §5.3,
 * specification/03-delivery.md §6.1.
 *
 * Este fichero valida opciones y las traduce a lo que entiende jnats, para que un valor mal formado
 * falle en `connect()` —con su codigo— y no se ignore en silencio. Nada de lo que valida abre un socket.
 */
package com.flux;

import com.flux.SecurityErrors.ConnectionRejectedException;
import com.flux.SecurityErrors.InvalidCredentialsException;
import com.flux.SecurityErrors.InvalidTlsOptionsException;
import com.flux.SecurityErrors.RejectionReason;
import io.nats.client.AuthHandler;
import io.nats.client.AuthenticationException;
import io.nats.client.NKey;
import io.nats.client.Nats;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/** Clase de utilidad: no se instancia. */
final class Security {

    private Security() {
        throw new AssertionError("clase de utilidad");
    }

    // ─── Inbox privado ───────────────────────────────────────────────────────

    /**
     * {@code _INBOX.<servicio>}. Las respuestas a las peticiones de JetStream —y las entregas de un pull
     * consumer— viajan por el inbox. Con el {@code _INBOX.>} comun, un servicio que se suscribe ahi lee
     * los eventos de dominios que no tiene concedidos (06-security.md §3.3, hallazgo H2).
     *
     * <p>No es una opcion: lo deriva el SDK del nombre del servicio. Si fuera configurable, volver al
     * {@code _INBOX.>} comun seria un valor por defecto olvidado. El nombre de servicio no admite puntos,
     * asi que el prefijo de uno nunca puede ser un prefijo de otro. jnats aniade el punto y el nuid:
     * {@code _INBOX.<servicio>.<nuid>} (medido).
     */
    static String inboxPrefixFor(String service) {
        Protocol.validateService(service);
        return "_INBOX." + service;
    }

    // ─── Credenciales ────────────────────────────────────────────────────────

    /** Como autenticarse, ya validado. Ninguno abre un socket. */
    static final class Auth {
        /** {@code .creds}, nkey: lo que firma el nonce. {@code null} si es token o usuario/contrasena. */
        final AuthHandler handler;
        final char[] token;
        final String user;
        final char[] password;

        Auth(AuthHandler handler, char[] token, String user, char[] password) {
            this.handler = handler;
            this.token = token;
            this.user = user;
            this.password = password;
        }

        void apply(io.nats.client.Options.Builder builder) {
            if (handler != null) {
                builder.authHandler(handler);
            } else if (token != null) {
                builder.token(token);
            } else if (user != null) {
                builder.userInfo(user.toCharArray(), password);
            }
        }
    }

    private static final Pattern CREDS_JWT = Pattern.compile(
            "-----BEGIN NATS USER JWT-----\\s+([A-Za-z0-9._\\-]+)\\s+------END NATS USER JWT------");
    private static final Pattern CREDS_SEED = Pattern.compile(
            "-----BEGIN USER NKEY SEED-----\\s+(\\S+)\\s+------END USER NKEY SEED------");

    private static String nonEmpty(String option, String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new InvalidCredentialsException("`" + option + "` debe ser un texto no vacio (casi siempre es una "
                    + "variable de entorno sin definir)");
        }
        return value;
    }

    /** Un {@link AuthHandler} de nkey: firma el nonce con la semilla; sin JWT, el broker lo trata como nkey. */
    private static AuthHandler nkeyHandler(String seed, String option) {
        if (!seed.startsWith("SU")) {
            throw new InvalidCredentialsException("`" + option + "` debe ser la semilla de una nkey de USUARIO "
                    + "(empieza por «SU»)");
        }
        NKey key;
        char[] publicKey;
        try {
            key = NKey.fromSeed(seed.toCharArray());
            publicKey = key.getPublicKey();
        } catch (Exception | Error e) {
            throw new InvalidCredentialsException("`" + option + "` no es una semilla nkey valida", e);
        }
        return new AuthHandler() {
            @Override
            public byte[] sign(byte[] nonce) {
                try {
                    return key.sign(nonce);
                } catch (GeneralSecurityException | IOException e) {
                    throw new IllegalStateException("no se pudo firmar el nonce con la nkey", e);
                }
            }

            @Override
            public char[] getID() {
                return publicKey;
            }

            @Override
            public char[] getJWT() {
                return null;
            }
        };
    }

    /**
     * Traduce {@code credentials} a la autenticacion de jnats, o {@code null} si no hay. Falla ANTES de abrir
     * un socket ante cualquier valor que la libreria ignoraria o que no podra usar.
     */
    static Auth credentials(FluxBus.Credentials c) {
        if (c == null) {
            return null;
        }
        List<String> methods = new ArrayList<>();
        if (c.credsFile() != null) {
            methods.add("credsFile");
        }
        if (c.credsContent() != null) {
            methods.add("credsContent");
        }
        if (c.nkeySeed() != null) {
            methods.add("nkeySeed");
        }
        if (c.token() != null) {
            methods.add("token");
        }
        if (c.user() != null || c.password() != null) {
            methods.add("user+password");
        }
        if (methods.isEmpty()) {
            return null;
        }
        if (methods.size() > 1) {
            // Combinarlos es ambiguo: el servidor aceptaria uno y el otro quedaria como ruido.
            throw new InvalidCredentialsException("elige UN metodo y no " + String.join(" + ", methods));
        }
        if (c.credsFile() != null) {
            Path path = Path.of(nonEmpty("credsFile", c.credsFile()));
            String content;
            try {
                if (!Files.isRegularFile(path)) {
                    throw new IOException("no es un fichero");
                }
                content = Files.readString(path, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new InvalidCredentialsException("`credsFile` apunta a \"" + path + "\", que no se puede leer ("
                        + e.getClass().getSimpleName() + ")", e);
            }
            checkCredsText(content, "credsFile");
            // Se pasa la RUTA: jnats la relee en cada (re)conexion y rotar no exige reiniciar.
            return new Auth(Nats.credentials(path.toString()), null, null, null);
        }
        if (c.credsContent() != null) {
            String content = nonEmpty("credsContent", c.credsContent());
            checkCredsText(content, "credsContent");
            return new Auth(Nats.staticCredentials(content.getBytes(StandardCharsets.UTF_8)), null, null, null);
        }
        if (c.nkeySeed() != null) {
            return new Auth(nkeyHandler(nonEmpty("nkeySeed", c.nkeySeed()), "nkeySeed"), null, null, null);
        }
        if (c.token() != null) {
            return new Auth(null, nonEmpty("token", c.token()).toCharArray(), null, null);
        }
        if (c.user() == null || c.password() == null) {
            throw new InvalidCredentialsException("`user` y `password` van juntos");
        }
        return new Auth(null, null, nonEmpty("user", c.user()), nonEmpty("password", c.password()).toCharArray());
    }

    /** Un {@code .creds} trae un JWT y una semilla de USUARIO. */
    private static void checkCredsText(String content, String option) {
        Matcher jwt = CREDS_JWT.matcher(content);
        Matcher seed = CREDS_SEED.matcher(content);
        if (!jwt.find() || !seed.find()) {
            throw new InvalidCredentialsException("`" + option + "` no tiene la forma de un .creds: faltan el bloque "
                    + "`BEGIN NATS USER JWT` o el `BEGIN USER NKEY SEED`");
        }
        nkeyHandler(seed.group(1), option); // la semilla del .creds se valida como cualquier otra
    }

    // ─── TLS ─────────────────────────────────────────────────────────────────

    private static Path readableFile(String option, String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new InvalidTlsOptionsException("`" + option + "` debe ser una ruta no vacia");
        }
        Path path = Path.of(value);
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new InvalidTlsOptionsException("`" + option + "` apunta a \"" + value + "\", que no es un fichero legible");
        }
        return path;
    }

    /** El contexto TLS, o {@code null} si no se pidio TLS. */
    static SSLContext sslContext(TlsOptions tls) {
        if (tls == null) {
            return null;
        }
        if ((tls.certFile() == null) != (tls.keyFile() == null)) {
            throw new InvalidTlsOptionsException("`certFile` y `keyFile` van juntos (certificado de cliente)");
        }
        // Se leen todas las rutas ANTES de construir nada: un error de la segunda no debe quedar tapado por la primera.
        Path ca = tls.caFile() == null ? null : readableFile("caFile", tls.caFile());
        Path cert = tls.certFile() == null ? null : readableFile("certFile", tls.certFile());
        Path key = tls.keyFile() == null ? null : readableFile("keyFile", tls.keyFile());
        try {
            TrustManager[] trust = trustManagers(ca);
            KeyManager[] keys = cert == null ? null : keyManagers(cert, key);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys, trust, new SecureRandom());
            return new StrictSslContext(context);
        } catch (GeneralSecurityException | IOException e) {
            throw new InvalidTlsOptionsException("no se pudo construir el contexto TLS (" + e.getClass().getSimpleName()
                    + ": " + e.getMessage() + ")", e);
        }
    }

    private static TrustManager[] trustManagers(Path ca) throws GeneralSecurityException, IOException {
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        if (ca == null) {
            factory.init((KeyStore) null); // las CA del sistema
            return factory.getTrustManagers();
        }
        String text = Files.readString(ca, StandardCharsets.UTF_8);
        if (!text.contains("-----BEGIN CERTIFICATE-----")) {
            throw new InvalidTlsOptionsException("`caFile` (\"" + ca + "\") no es un PEM: no tiene ningun bloque "
                    + "`BEGIN CERTIFICATE`");
        }
        KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
        store.load(null, null);
        int i = 0;
        try (InputStream in = Files.newInputStream(ca)) {
            for (Certificate c : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
                store.setCertificateEntry("ca" + i++, c);
            }
        } catch (CertificateException e) {
            throw new InvalidTlsOptionsException("`caFile` (\"" + ca + "\") no es un PEM de certificados valido", e);
        }
        if (i == 0) {
            throw new InvalidTlsOptionsException("`caFile` (\"" + ca + "\") no contiene ningun certificado");
        }
        factory.init(store);
        return factory.getTrustManagers();
    }

    private static KeyManager[] keyManagers(Path cert, Path key) throws GeneralSecurityException, IOException {
        List<Certificate> chain = new ArrayList<>();
        try (InputStream in = Files.newInputStream(cert)) {
            chain.addAll(CertificateFactory.getInstance("X.509").generateCertificates(in));
        } catch (CertificateException e) {
            throw new InvalidTlsOptionsException("`certFile` (\"" + cert + "\") no es un PEM de certificados valido", e);
        }
        if (chain.isEmpty()) {
            throw new InvalidTlsOptionsException("`certFile` (\"" + cert + "\") no contiene ningun certificado");
        }
        PrivateKey privateKey = readPrivateKey(key);
        KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
        store.load(null, null);
        char[] pass = new char[0];
        store.setKeyEntry("client", privateKey, pass, chain.toArray(new Certificate[0]));
        KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        factory.init(store, pass);
        return factory.getKeyManagers();
    }

    // ─── Clave privada PEM (PKCS#8, PKCS#1 y SEC1) ───────────────────────────

    private static final Pattern PEM_BLOCK = Pattern.compile(
            "-----BEGIN ([A-Z ]+)-----([A-Za-z0-9+/=\\s]+)-----END \\1-----");

    static PrivateKey readPrivateKey(Path file) throws IOException, GeneralSecurityException {
        Matcher m = PEM_BLOCK.matcher(Files.readString(file, StandardCharsets.UTF_8));
        while (m.find()) {
            String kind = m.group(1);
            byte[] der = Base64.getMimeDecoder().decode(m.group(2));
            switch (kind) {
                case "PRIVATE KEY":
                    return privateKeyFromPkcs8(der, file);
                case "RSA PRIVATE KEY":
                    return privateKeyFromPkcs8(wrapRsa(der), file);
                case "EC PRIVATE KEY":
                    return privateKeyFromPkcs8(wrapEc(der), file);
                case "ENCRYPTED PRIVATE KEY":
                    throw new InvalidTlsOptionsException("`keyFile` (\"" + file + "\") esta cifrada con contrasena: "
                            + "el SDK no la admite. Entregala sin cifrar al servicio (con permisos 0600) o desde un "
                            + "gestor de secretos");
                default:
                    break;
            }
        }
        throw new InvalidTlsOptionsException("`keyFile` (\"" + file + "\") no es un PEM de clave privada: no tiene ningun "
                + "bloque `BEGIN PRIVATE KEY`, `BEGIN RSA PRIVATE KEY` ni `BEGIN EC PRIVATE KEY`");
    }

    private static PrivateKey privateKeyFromPkcs8(byte[] der, Path file) throws GeneralSecurityException {
        for (String algorithm : new String[] {"RSA", "EC", "Ed25519"}) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(der));
            } catch (GeneralSecurityException ignored) {
                // otro algoritmo
            }
        }
        throw new InvalidTlsOptionsException("`keyFile` (\"" + file + "\") no es una clave privada RSA, EC ni Ed25519 valida");
    }

    private static final byte[] OID_RSA = {0x06, 0x09, 0x2A, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xF7, 0x0D, 0x01, 0x01, 0x01};
    private static final byte[] OID_EC_PUBLIC_KEY = {0x06, 0x07, 0x2A, (byte) 0x86, 0x48, (byte) 0xCE, 0x3D, 0x02, 0x01};

    /** PKCS#1 → PKCS#8: {@code SEQ { INT 0, SEQ { OID rsaEncryption, NULL }, OCTET STRING { pkcs1 } }}. */
    private static byte[] wrapRsa(byte[] pkcs1) {
        byte[] algorithm = der(0x30, concat(OID_RSA, new byte[] {0x05, 0x00}));
        return der(0x30, concat(new byte[] {0x02, 0x01, 0x00}, algorithm, der(0x04, pkcs1)));
    }

    /** SEC1 → PKCS#8: la curva sale del {@code [0] parameters} de la propia clave. */
    private static byte[] wrapEc(byte[] sec1) {
        byte[] curve = null;
        int[] pos = {0};
        if (sec1.length > 1 && (sec1[0] & 0xFF) == 0x30) {
            pos[0] = 1;
            readLength(sec1, pos);
            while (pos[0] < sec1.length) {
                int tag = sec1[pos[0]++] & 0xFF;
                int len = readLength(sec1, pos);
                if (tag == 0xA0) {
                    curve = java.util.Arrays.copyOfRange(sec1, pos[0], pos[0] + len);
                    break;
                }
                pos[0] += len;
            }
        }
        if (curve == null) {
            throw new InvalidTlsOptionsException("`keyFile` es una clave EC sin los parametros de la curva: conviertela con "
                    + "`openssl pkcs8 -topk8 -nocrypt`");
        }
        byte[] algorithm = der(0x30, concat(OID_EC_PUBLIC_KEY, curve));
        return der(0x30, concat(new byte[] {0x02, 0x01, 0x00}, algorithm, der(0x04, sec1)));
    }

    private static int readLength(byte[] b, int[] pos) {
        int first = b[pos[0]++] & 0xFF;
        if (first < 0x80) {
            return first;
        }
        int n = first & 0x7F;
        int len = 0;
        for (int i = 0; i < n; i++) {
            len = (len << 8) | (b[pos[0]++] & 0xFF);
        }
        return len;
    }

    private static byte[] der(int tag, byte[] content) {
        byte[] length;
        if (content.length < 0x80) {
            length = new byte[] {(byte) content.length};
        } else if (content.length < 0x100) {
            length = new byte[] {(byte) 0x81, (byte) content.length};
        } else {
            length = new byte[] {(byte) 0x82, (byte) (content.length >> 8), (byte) content.length};
        }
        return concat(new byte[] {(byte) tag}, length, content);
    }

    private static byte[] concat(byte[]... parts) {
        int total = 0;
        for (byte[] p : parts) {
            total += p.length;
        }
        byte[] out = new byte[total];
        int at = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, at, p.length);
            at += p.length;
        }
        return out;
    }

    // ─── Verificacion de host ────────────────────────────────────────────────

    /**
     * Un {@link SSLContext} cuyos sockets verifican que el certificado cubre el host ({@code HTTPS} como
     * algoritmo de identificacion). jnats NO lo hace por si sola: medido, conecta a {@code 127.0.0.2} con un
     * certificado cuyos SAN son {@code localhost} e {@code 127.0.0.1}. Sin esto, «CA propia» seria «cualquier
     * certificado firmado por esa CA, de cualquier host».
     */
    static final class StrictSslContext extends SSLContext {
        StrictSslContext(SSLContext delegate) {
            super(new Spi(delegate), delegate.getProvider(), delegate.getProtocol());
        }

        private static final class Spi extends SSLContextSpi {
            private final SSLContext delegate;

            Spi(SSLContext delegate) {
                this.delegate = delegate;
            }

            @Override
            protected void engineInit(KeyManager[] km, TrustManager[] tm, SecureRandom sr) {
                throw new UnsupportedOperationException("el contexto ya esta inicializado");
            }

            @Override
            protected SSLSocketFactory engineGetSocketFactory() {
                return new StrictSocketFactory(delegate.getSocketFactory());
            }

            @Override
            protected SSLServerSocketFactory engineGetServerSocketFactory() {
                return delegate.getServerSocketFactory();
            }

            @Override
            protected SSLEngine engineCreateSSLEngine() {
                return strict(delegate.createSSLEngine());
            }

            @Override
            protected SSLEngine engineCreateSSLEngine(String host, int port) {
                return strict(delegate.createSSLEngine(host, port));
            }

            @Override
            protected SSLSessionContext engineGetServerSessionContext() {
                return delegate.getServerSessionContext();
            }

            @Override
            protected SSLSessionContext engineGetClientSessionContext() {
                return delegate.getClientSessionContext();
            }

            private static SSLEngine strict(SSLEngine engine) {
                SSLParameters p = engine.getSSLParameters();
                p.setEndpointIdentificationAlgorithm("HTTPS");
                engine.setSSLParameters(p);
                return engine;
            }
        }
    }

    private static final class StrictSocketFactory extends SSLSocketFactory {
        private final SSLSocketFactory delegate;

        StrictSocketFactory(SSLSocketFactory delegate) {
            this.delegate = delegate;
        }

        private static Socket strict(Socket socket) {
            if (socket instanceof SSLSocket ssl) {
                SSLParameters p = ssl.getSSLParameters();
                p.setEndpointIdentificationAlgorithm("HTTPS");
                ssl.setSSLParameters(p);
            }
            return socket;
        }

        @Override
        public String[] getDefaultCipherSuites() {
            return delegate.getDefaultCipherSuites();
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }

        @Override
        public Socket createSocket(Socket s, String host, int port, boolean autoClose) throws IOException {
            return strict(delegate.createSocket(s, host, port, autoClose));
        }

        @Override
        public Socket createSocket() throws IOException {
            return strict(delegate.createSocket());
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            return strict(delegate.createSocket(host, port));
        }

        @Override
        public Socket createSocket(String host, int port, java.net.InetAddress localHost, int localPort)
                throws IOException {
            return strict(delegate.createSocket(host, port, localHost, localPort));
        }

        @Override
        public Socket createSocket(java.net.InetAddress host, int port) throws IOException {
            return strict(delegate.createSocket(host, port));
        }

        @Override
        public Socket createSocket(java.net.InetAddress address, int port, java.net.InetAddress localAddress,
                                   int localPort) throws IOException {
            return strict(delegate.createSocket(address, port, localAddress, localPort));
        }
    }

    // ─── La PRIMERA conexion: esperar al broker, no a lo que no cambia ───────

    /**
     * Un fallo de la primera conexion que ESPERAR NO ARREGLA, ya explicado; o {@code null} si es transitorio
     * (broker caido, DNS, timeout) y merece otro intento.
     *
     * <p>Por que existe: {@code Nats.connectReconnectOnConnect} reintenta TODO sin limite. Medido contra
     * nats-server 2.14.5 con jnats 2.20.5: con una CA que no firmo el certificado, un servidor sin TLS ante un
     * {@code tls} pedido, o un TLS exigido que no se configuro, {@code connect()} NO falla: se cuelga; con una
     * credencial rechazada devuelve una conexion CERRADA, sin excepcion (A-200). Aqui el bucle es del SDK
     * (03-delivery.md §6.1).
     *
     * <p>{@code Nats.connect} lanza un {@code IOException} generico («Unable to connect to NATS servers»)
     * sin causa: lo que dice POR QUE solo llega por {@code ErrorListener.exceptionOccurred}. Se reciben aqui
     * las dos cosas.
     *
     * @param thrown lo que lanzo {@code Nats.connect}.
     * @param seen   las excepciones que la libreria notifico durante el intento.
     * @param servers a que se intentaba conectar (para el mensaje).
     * @param tlsRequested si el servicio configuro {@code tls}.
     */
    static ConnectionRejectedException permanentConnectError(Throwable thrown, List<Throwable> seen, String servers,
                                                             boolean tlsRequested) {
        if (thrown instanceof AuthenticationException) {
            return new ConnectionRejectedException(RejectionReason.AUTH, "el broker (" + servers + ") rechazo las "
                    + "credenciales (" + thrown.getMessage() + "). Reintentar no lo arregla: revisa `credentials` y que "
                    + "el usuario exista en la account del servicio (04-errors.md §5.3).", thrown);
        }
        List<Throwable> all = new ArrayList<>(seen);
        all.add(thrown);
        for (Throwable t : all) {
            for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
                String message = String.valueOf(c.getMessage());
                if (message.contains("SSL connection wanted by client")) {
                    return tls("se pidio `tls` pero el servidor NATS (" + servers + ") no ofrece TLS. Reintentar no lo "
                            + "arregla, y el SDK no baja a texto plano: habilita TLS en el listener "
                            + "(06-security.md §2) o quita `tls`.", c);
                }
                if (message.contains("SSL required by server")) {
                    return tls("el servidor NATS (" + servers + ") exige TLS y este servicio no configuro `tls`. "
                            + "Reintentar no lo arregla: pasa `tls(new TlsOptions())` (CA del sistema) o "
                            + "`tls(new TlsOptions().caFile(...))` si usa una CA propia (06-security.md §2).", c);
                }
                if (isCertificateProblem(c)) {
                    return tls("no se pudo verificar el certificado del servidor NATS (" + servers + "): "
                            + c.getClass().getSimpleName() + (c.getMessage() != null ? " — " + c.getMessage() : "")
                            + ". Reintentar no lo arregla. Si el servidor usa una CA propia, indicala en "
                            + "`tls.caFile`; si el certificado no cubre el host al que te conectas, revisa sus SAN; "
                            + "y si el broker exige un certificado de cliente (mTLS), pasa `certFile` y `keyFile` "
                            + "(04-errors.md §5.3).", c);
                }
            }
        }
        return null;
    }

    private static final Pattern TLS_ALERT = Pattern.compile(
            "certificate_unknown|bad_certificate|unknown_ca|certificate_expired|certificate_revoked|"
                    + "certificate_required|unsupported_certificate|handshake_failure|protocol_version|"
                    + "No subject alternative|No name matching|PKIX path|unable to find valid certification path|"
                    + "unable to verify|certificate verify failed|hostname", Pattern.CASE_INSENSITIVE);

    /**
     * ¿Es una causa de TLS que esperar no arregla? Un certificado que no verifica, un host fuera de los SAN, un
     * certificado caducado o una alerta de la otra parte. «Remote host terminated the handshake» o una conexion
     * reseteada NO lo son: es lo que se ve con un broker que se esta reiniciando.
     */
    private static boolean isCertificateProblem(Throwable t) {
        if (t instanceof CertificateException) {
            return true;
        }
        if (t instanceof javax.net.ssl.SSLPeerUnverifiedException) {
            return true;
        }
        if (t instanceof SSLException) {
            return TLS_ALERT.matcher(String.valueOf(t.getMessage())).find();
        }
        return false;
    }

    private static ConnectionRejectedException tls(String message, Throwable cause) {
        return new ConnectionRejectedException(RejectionReason.TLS, message, cause);
    }

    /**
     * Un diagnostico para el fallo que jnats NO distingue de un servidor que no contesta: el broker exige un
     * CERTIFICADO DE CLIENTE (mTLS) y no se presento ninguno. Medido (A-200): el unico rastro es un
     * {@code TimeoutException} tras 2 s, igual que un broker que acepta la conexion y se queda mudo
     * (03-delivery.md §6.1: «DEBERIA fallar de inmediato: no todos los clientes NATS lo distinguen de un timeout»).
     *
     * <p>Se repite el handshake con el MISMO contexto TLS sobre un socket propio: con TLS 1.3 el servidor valida el
     * certificado de cliente DESPUES del handshake del cliente, y su alerta ({@code certificate_required}) llega con la
     * primera lectura; con TLS 1.2 el propio handshake falla. Un servidor que simplemente no contesta da un timeout de
     * lectura, y eso NO es un rechazo: se sigue esperando.
     *
     * @return la excepcion TLS que explica el rechazo, o {@code null} si no hay rastro de uno.
     */
    static SSLException probeTlsRejection(SSLContext context, String serverUri) {
        try {
            java.net.URI uri = new java.net.URI(serverUri.contains("://") ? serverUri : "nats://" + serverUri);
            String host = uri.getHost();
            int port = uri.getPort() > 0 ? uri.getPort() : 4222;
            try (Socket raw = new Socket()) {
                raw.connect(new java.net.InetSocketAddress(host, port), 1500);
                raw.setSoTimeout(1500);
                // En NATS el servidor manda su INFO en texto plano antes de que el cliente suba a TLS.
                InputStream plain = raw.getInputStream();
                int b;
                do {
                    b = plain.read();
                } while (b != '\n' && b != -1);
                try (SSLSocket ssl = (SSLSocket) context.getSocketFactory().createSocket(raw, host, port, false)) {
                    ssl.startHandshake();
                    ssl.getInputStream().read();
                }
            }
        } catch (SSLException e) {
            return e;
        } catch (Exception e) {
            // Un timeout de lectura, una conexion rechazada o una URI rara no son un rechazo TLS: no se sabe nada.
        }
        return null;
    }
}
