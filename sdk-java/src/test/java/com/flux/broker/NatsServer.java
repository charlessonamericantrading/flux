/*
 * Un nats-server REAL para las pruebas: se arranca con la configuracion que escribe cada prueba, se espera a que
 * su PROPIO log diga «Server is ready» y se mata solo ese proceso al soltarlo.
 *
 * Sin binario, las pruebas se SALTAN con un aviso... salvo que FLUX_REQUIRE_BROKER=1: en CI eso las convierte en un
 * fallo. Un fixture que nadie ejecuta aparenta cobertura.
 *
 *   NATS_SERVER=/ruta/a/nats-server mvn test
 *   FLUX_TEST_PORTS=7400-7499         el rango de puertos propio (por defecto 7400-7499)
 */
package com.flux.broker;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Assumptions;

public final class NatsServer implements AutoCloseable {

    private static final AtomicInteger NEXT = new AtomicInteger();

    private final Process process;
    private final Path dir;
    private final Path log;
    private final int port;
    private final int monitorPort;

    private NatsServer(Process process, Path dir, Path log, int port, int monitorPort) {
        this.process = process;
        this.dir = dir;
        this.log = log;
        this.port = port;
        this.monitorPort = monitorPort;
    }

    // ─── Entorno ─────────────────────────────────────────────────────────────

    /** {@code FLUX_REQUIRE_BROKER} definida y distinta de {@code 0}/vacio. */
    public static boolean required() {
        String v = System.getenv("FLUX_REQUIRE_BROKER");
        return v != null && !v.isEmpty() && !v.equals("0");
    }

    /** El binario de nats-server: {@code NATS_SERVER}, o el del {@code PATH}. */
    public static Path binary() {
        String env = System.getenv("NATS_SERVER");
        if (env != null && !env.isEmpty()) {
            Path p = Path.of(env);
            return Files.isRegularFile(p) ? p : null;
        }
        String path = System.getenv("PATH");
        if (path != null) {
            for (String d : path.split(java.io.File.pathSeparator)) {
                Path p = Path.of(d, "nats-server");
                if (Files.isRegularFile(p)) {
                    return p;
                }
            }
        }
        return null;
    }

    /**
     * Sin binario (o sin lo que la prueba necesite): salta con aviso, o FALLA si el entorno exige broker. Se llama
     * al empezar la prueba.
     */
    public static Path requireBinary() {
        Path bin = binary();
        if (bin == null) {
            if (required()) {
                fail("FLUX_REQUIRE_BROKER=1 y no hay nats-server: define NATS_SERVER con la ruta del binario (o ponlo "
                        + "en el PATH). Sin esto las pruebas con broker no se ejecutan, y un fixture que nadie "
                        + "ejecuta aparenta cobertura.");
            }
            Assumptions.assumeTrue(false, "saltado: no hay nats-server (define NATS_SERVER). Con FLUX_REQUIRE_BROKER=1 fallaria.");
        }
        return bin;
    }

    /** Una herramienta externa (`node`, `openssl`): igual que el binario, salta o falla. */
    public static void requireTool(String name, String... versionArgs) {
        try {
            String[] cmd = new String[versionArgs.length + 1];
            cmd[0] = name;
            System.arraycopy(versionArgs, 0, cmd, 1, versionArgs.length);
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            p.getInputStream().readAllBytes();
            if (p.waitFor() == 0) {
                return;
            }
        } catch (IOException | InterruptedException ignored) {
            // no esta
        }
        if (required()) {
            fail("FLUX_REQUIRE_BROKER=1 y no hay `" + name + "` en el PATH: las pruebas de seguridad lo necesitan "
                    + "(genera los permisos con scripts/gen-acl.mjs y los certificados con openssl).");
        }
        Assumptions.assumeTrue(false, "saltado: no hay `" + name + "` en el PATH");
    }

    /** Un puerto libre del rango propio ({@code FLUX_TEST_PORTS}, por defecto 7400-7499), comprobado con un bind real. */
    public static int freePort() {
        String range = System.getenv("FLUX_TEST_PORTS");
        int from = 7400;
        int to = 7499;
        if (range != null && range.matches("\\d+-\\d+")) {
            String[] r = range.split("-");
            from = Integer.parseInt(r[0]);
            to = Integer.parseInt(r[1]);
        }
        int span = to - from + 1;
        for (int i = 0; i < span; i++) {
            int port = from + Math.floorMod(NEXT.getAndIncrement(), span);
            try (ServerSocket s = new ServerSocket()) {
                s.setReuseAddress(true);
                s.bind(new InetSocketAddress("127.0.0.1", port));
                return port;
            } catch (IOException busy) {
                // otro servidor, aun cerrandose
            }
        }
        throw new IllegalStateException("no queda ningun puerto libre en " + from + "-" + to);
    }

    // ─── Arranque ────────────────────────────────────────────────────────────

    /**
     * Arranca un servidor con {@code config} (que NO define puerto ni JetStream: lo pone aqui). {@code topLevel}
     * es configuracion de nivel superior ({@code tls {…}}, {@code authorization {…}}…).
     *
     * @param jetStream con JetStream y su almacen en el temporal.
     */
    public static NatsServer start(String host, String topLevel, boolean jetStream) throws IOException {
        return start(host, topLevel, jetStream, java.util.Map.of());
    }

    /**
     * Como {@link #start(String, String, boolean)}, con ficheros que la config incluye ({@code include "acl.conf"}):
     * nats-server resuelve un {@code include} relativo al directorio de la propia config, tambien una ruta absoluta.
     */
    public static NatsServer start(String host, String topLevel, boolean jetStream,
                                   java.util.Map<String, String> files) throws IOException {
        return startOn(freePort(), host, topLevel, jetStream, files);
    }

    /** Como {@link #start(String, String, boolean, java.util.Map)}, en un puerto ya elegido (reservado antes de arrancar). */
    public static NatsServer startOn(int port, String host, String topLevel, boolean jetStream,
                                     java.util.Map<String, String> files) throws IOException {
        Path bin = requireBinary();
        Path dir = Files.createTempDirectory("flux-java-");
        int monitor = freePort();
        String conf = "host: \"" + host + "\"\nport: " + port + "\nhttp: \"127.0.0.1:" + monitor + "\"\n"
                + (jetStream ? "jetstream { store_dir: \"" + dir.resolve("js") + "\" }\n" : "") + topLevel + "\n";
        Path file = dir.resolve("server.conf");
        Files.writeString(file, conf, StandardCharsets.UTF_8);
        for (java.util.Map.Entry<String, String> f : files.entrySet()) {
            Files.writeString(dir.resolve(f.getKey()), f.getValue(), StandardCharsets.UTF_8);
        }
        Path log = dir.resolve("server.log");
        Process process = new ProcessBuilder(bin.toString(), "-c", file.toString())
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
        NatsServer server = new NatsServer(process, dir, log, port, monitor);
        try {
            for (int i = 0; i < 100; i++) {
                if (!process.isAlive()) {
                    throw new IllegalStateException("nats-server no cargo la config:\n" + server.log());
                }
                if (server.log().contains("Server is ready")) {
                    return server;
                }
                Thread.sleep(100);
            }
            throw new IllegalStateException("nats-server no llego a estar listo:\n" + server.log());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.close();
            throw new IllegalStateException(e);
        } catch (RuntimeException e) {
            server.close();
            throw e;
        }
    }

    public int port() {
        return port;
    }

    public int monitorPort() {
        return monitorPort;
    }

    public Path dir() {
        return dir;
    }

    /** Lo que el servidor ha escrito en su log. */
    public String log() {
        try {
            return Files.readString(log, StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "";
        }
    }

    /**
     * Las violaciones de permiso que el servidor ANOTO (`Publish Violation` / `Subscription Violation`): lo que ningun
     * cliente puede falsear. Una por arranque seria ruido que tapa las reales (06-security.md §3.4).
     */
    public java.util.List<String> violations() {
        java.util.List<String> out = new java.util.ArrayList<>();
        Pattern p = Pattern.compile("\\b(Publish|Subscription) Violation\\b.*");
        for (String line : log().split("\n")) {
            if (p.matcher(line).find()) {
                out.add(line.trim());
            }
        }
        return out;
    }

    /** Mata el proceso (SIGKILL): lo que ve el resto es un broker que desaparece. */
    public void kill() {
        process.destroyForcibly();
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean alive() {
        return process.isAlive();
    }

    @Override
    public void close() {
        process.destroy();
        try {
            if (!process.waitFor(3, java.util.concurrent.TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
        deleteRecursively(dir);
    }

    public static void deleteRecursively(Path dir) {
        try (var walk = Files.walk(dir)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        } catch (IOException ignored) {
            // el temporal no debe romper la prueba
        }
    }
}
