/*
 * El entorno SEGURO de las pruebas de integracion: un nats-server con TLS, los permisos que genera
 * `scripts/gen-acl.mjs` de un manifiesto, una identidad nkey por servicio y `flux provision` con credenciales de
 * operacion. Es lo mismo que monta `conformance/security/entorno.mjs` para Node y Rust.
 *
 * Un broker abierto no prueba nada de seguridad: aqui el SDK vive bajo los permisos EXACTOS que el generador da a cada
 * servicio, y quitar uno de ellos (una VARIANTE) tiene que notarse.
 */
package com.flux.broker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flux.FluxBus;
import com.flux.TlsOptions;
import io.nats.client.AuthHandler;
import io.nats.client.Connection;
import io.nats.client.NKey;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public final class SecureEnv implements AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Una identidad nkey: la semilla es el secreto (solo en pruebas), la publica va en la ACL. */
    public record Identity(String name, String service, String seed, String pub) {
    }

    /** Un permiso que se QUITA de los generados: una variante del servicio con un permiso de menos. */
    public record Variant(String id, String service, List<String> dropPublish, List<String> dropSubscribe) {
    }

    public static final class Builder {
        private Path manifest = Repo.path("examples/services.json");
        private boolean dev;
        private boolean mtls;
        private boolean provision = true;
        private final List<Variant> variants = new ArrayList<>();

        /** Otro manifiesto (el de una prueba concreta). Ruta absoluta. */
        public Builder manifest(Path v) {
            this.manifest = v;
            return this;
        }

        /** {@code gen-acl.mjs --dev}: concede {@code CONSUMER.CREATE} de cada durable propio. */
        public Builder dev(boolean v) {
            this.dev = v;
            return this;
        }

        /** El broker exige certificado de cliente. */
        public Builder mtls(boolean v) {
            this.mtls = v;
            return this;
        }

        /** Sin {@code flux provision}: nada existe todavia. */
        public Builder provision(boolean v) {
            this.provision = v;
            return this;
        }

        /**
         * Una VARIANTE de {@code service}: sus permisos generados, menos los de {@code dropPublish} y
         * {@code dropSubscribe} (subjects EXACTOS tal y como los genera el generador).
         */
        public Builder variant(String id, String service, List<String> dropPublish, List<String> dropSubscribe) {
            variants.add(new Variant(id, service, dropPublish, dropSubscribe));
            return this;
        }

        public SecureEnv build() throws Exception {
            return new SecureEnv(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public final NatsServer server;
    public final Pki pki;
    public final Path manifest;
    /** Los permisos que el generador afirma, por servicio ({@code gen-acl.mjs --json}). */
    public final Map<String, JsonNode> permissions = new LinkedHashMap<>();
    private final Map<String, Identity> identities = new LinkedHashMap<>();
    private final Identity ops;
    private final Path opsCreds;
    /** Salida de {@code flux provision}. */
    public final String provisionOutput;
    private final List<Connection> opened = new ArrayList<>();

    private SecureEnv(Builder b) throws Exception {
        NatsServer.requireBinary();
        NatsServer.requireTool("node", "--version");
        NatsServer.requireTool("openssl", "version");
        this.manifest = b.manifest;
        Path tmp = Files.createTempDirectory("flux-java-env-");
        this.pki = Pki.create(tmp);

        JsonNode all = node("--json", b.dev);
        for (JsonNode s : all) {
            permissions.put(s.get("name").textValue(), s);
            identities.put(s.get("name").textValue(), identity(s.get("name").textValue(), s.get("name").textValue()));
        }
        this.ops = identity("ops", "ops");
        Map<String, String> nkeys = new LinkedHashMap<>();
        for (Identity i : identities.values()) {
            nkeys.put(i.name(), i.pub());
        }
        Path nkeysFile = tmp.resolve("nkeys.json");
        Files.writeString(nkeysFile, JSON.writeValueAsString(nkeys));
        String acl = run(b.dev ? new String[] {"--nkeys", nkeysFile.toString(), "--dev"}
                : new String[] {"--nkeys", nkeysFile.toString()});

        StringBuilder extra = new StringBuilder();
        for (Variant v : b.variants) {
            JsonNode base = permissions.get(v.service());
            if (base == null) {
                throw new IllegalArgumentException("la variante " + v.id() + " usa un servicio que no esta en el manifiesto: " + v.service());
            }
            Identity id = identity(v.id(), v.service());
            identities.put(v.id(), id);
            extra.append(userConf(id.pub(), minus(base.get("publish"), v.dropPublish(), v.id()),
                    minus(base.get("subscribe"), v.dropSubscribe(), v.id()), base.get("allowResponses")));
        }
        extra.append("      { nkey: \"").append(ops.pub()).append("\" },\n");
        String marker = "    users: [";
        int at = acl.indexOf(marker);
        if (at < 0) {
            throw new IllegalStateException("la config generada no tiene `users: [` donde se esperaba");
        }
        acl = acl.substring(0, at + marker.length()) + "\n" + extra + acl.substring(at + marker.length());

        String tls = "tls {\n  cert_file: \"" + pki.serverCert + "\"\n  key_file: \"" + pki.serverKey + "\"\n  timeout: 2\n"
                + (b.mtls ? "  ca_file: \"" + pki.ca + "\"\n  verify: true\n" : "") + "}\n";
        this.server = NatsServer.start("127.0.0.1", tls + "include \"acl.conf\"\n", true, Map.of("acl.conf", acl));

        this.opsCreds = tmp.resolve("ops.creds");
        Files.writeString(opsCreds, "-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QifQ.e30.relleno\n------END NATS USER JWT------\n\n"
                + "-----BEGIN USER NKEY SEED-----\n" + ops.seed() + "\n------END USER NKEY SEED------\n");
        this.provisionOutput = b.provision ? provision() : "";
    }

    private static Identity identity(String id, String service) throws Exception {
        NKey key = NKey.createUser(new SecureRandom());
        return new Identity(id, service, new String(key.getSeed()), new String(key.getPublicKey()));
    }

    private static List<String> minus(JsonNode list, List<String> drop, String variant) {
        List<String> out = new ArrayList<>();
        for (JsonNode s : list) {
            out.add(s.textValue());
        }
        for (String d : drop) {
            if (!out.remove(d)) {
                // Una variante imposible no se degrada a «sin cambios»: si el generador deja de conceder el permiso, la
                // prueba tiene que decirlo, no pasar por no haber quitado nada.
                throw new IllegalStateException("la variante " + variant + " quita `" + d + "`, que el generador no concede");
            }
        }
        return out;
    }

    private static String userConf(String pub, List<String> publish, List<String> subscribe, JsonNode allowResponses) {
        StringBuilder sb = new StringBuilder("      { nkey: \"" + pub + "\"\n        permissions: {\n");
        sb.append("          publish: ").append(permission(publish)).append("\n");
        sb.append("          subscribe: ").append(permission(subscribe)).append("\n");
        if (allowResponses != null && !allowResponses.isNull()) {
            sb.append("          allow_responses: { max: ").append(allowResponses.path("max").asInt(1))
                    .append(", expires: \"").append(allowResponses.path("expires").asText("30s")).append("\" }\n");
        }
        return sb.append("        } },\n").toString();
    }

    /**
     * Una lista de permisos. VACIA no es «ninguno»: en nats-server un `allow: []` es «sin restriccion», y una variante a la que se
     * le quita su ultimo permiso quedaria con TODOS. Se dice con un `deny: [">"]`.
     */
    private static String permission(List<String> list) {
        return list.isEmpty() ? "{ deny: [\">\"] }" : "{ allow: [" + quoted(list) + "] }";
    }

    private static String quoted(List<String> list) {
        List<String> q = new ArrayList<>();
        for (String s : list) {
            q.add("\"" + s + "\"");
        }
        return String.join(", ", q);
    }

    // ─── node: el generador y la CLI ─────────────────────────────────────────

    private JsonNode node(String flag, boolean dev) throws Exception {
        return JSON.readTree(run(dev ? new String[] {flag, "--dev"} : new String[] {flag}));
    }

    private String run(String[] args) throws IOException, InterruptedException {
        List<String> cmd = new ArrayList<>(List.of("node", Repo.path("scripts/gen-acl.mjs").toString(), manifest.toString()));
        cmd.addAll(List.of(args));
        Process p = new ProcessBuilder(cmd).directory(Repo.root().toFile()).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String err = new String(p.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        if (p.waitFor() != 0) {
            throw new IllegalStateException("gen-acl.mjs fallo:\n" + err);
        }
        return out;
    }

    /** {@code flux provision} con credenciales de OPERACION, por TLS con la CA efimera (la CLI no tiene `--ca`). */
    private String provision() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("node", Repo.path("cli/src/index.mjs").toString(), "provision",
                manifest.toString(), "--server", "tls://127.0.0.1:" + server.port(), "--creds", opsCreds.toString())
                .directory(Repo.root().toFile()).redirectErrorStream(true);
        pb.environment().put("NO_COLOR", "1");
        pb.environment().put("NODE_EXTRA_CA_CERTS", pki.ca.toString());
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (p.waitFor() != 0) {
            throw new IllegalStateException("flux provision fallo (¿falta `npm install` en cli/?):\n" + out);
        }
        return out;
    }

    // ─── lo que las pruebas usan ─────────────────────────────────────────────

    public Identity identity(String id) {
        Identity i = identities.get(id);
        if (i == null) {
            throw new IllegalArgumentException("no hay identidad " + id + " (hay: " + identities.keySet() + ")");
        }
        return i;
    }

    /** {@code tls://127.0.0.1:<puerto>}: el host esta en los SAN del certificado. */
    public String url() {
        return "tls://127.0.0.1:" + server.port();
    }

    /** Las opciones del SDK de una identidad: TLS con la CA propia y la nkey. {@code id} es un servicio o una variante. */
    public FluxBus.ConnectOptions options(String id) {
        Identity i = identity(id);
        return new FluxBus.ConnectOptions()
                .servers(url())
                .service(i.service())
                .environment("produccion")
                .version("1.0.0")
                .schemaBaseUrl("https://schemas.internal")
                .tls(new TlsOptions().caFile(pki.ca.toString()))
                .credentials(new FluxBus.Credentials().nkeySeed(i.seed()));
    }

    /** Una conexion de OPERACION, sin restricciones, para provisionar y mirar. Se cierra con el entorno. */
    public Connection ops() throws Exception {
        AuthHandler auth = new AuthHandler() {
            @Override
            public byte[] sign(byte[] nonce) {
                try {
                    return NKey.fromSeed(ops.seed().toCharArray()).sign(nonce);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public char[] getID() {
                try {
                    return NKey.fromSeed(ops.seed().toCharArray()).getPublicKey();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public char[] getJWT() {
                return null;
            }
        };
        Connection c = Nats.connect(new Options.Builder().server(url()).sslContext(trusting(pki.ca)).authHandler(auth)
                .maxReconnects(-1).build());
        opened.add(c);
        return c;
    }

    /** Un contexto TLS que confia en esa CA (la de las conexiones de operacion de las pruebas). */
    public static SSLContext trusting(Path ca) throws Exception {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        try (InputStream in = Files.newInputStream(ca)) {
            int i = 0;
            for (Certificate c : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
                ks.setCertificateEntry("ca" + i++, c);
            }
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }

    /**
     * Lo que el BROKER dice de las conexiones y sus suscripciones ({@code /connz?subs=1}): lo que ningun cliente puede
     * falsear. Devuelve el JSON.
     */
    public JsonNode connz() throws Exception {
        HttpResponse<String> r = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + server.monitorPort() + "/connz?subs=1&auth=1"))
                .timeout(Duration.ofSeconds(5)).build(), HttpResponse.BodyHandlers.ofString());
        return JSON.readTree(r.body());
    }

    @Override
    public void close() {
        for (Connection c : opened) {
            try {
                c.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        server.close();
        NatsServer.deleteRecursively(pki.dir);
    }
}
