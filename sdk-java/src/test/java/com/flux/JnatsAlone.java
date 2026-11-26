/*
 * jnats SOLA, en OTRA JVM: lo que hace `Nats.connectReconnectOnConnect` (la forma «esperar al broker» de la libreria) ante un fallo
 * que esperar no arregla. Se ejecuta aparte porque un cuelgue de la libreria deja hilos de reconexion no-daemon que impedirian
 * terminar a la JVM de las pruebas; aqui se mata el proceso entero.
 *
 *   java -cp … com.flux.JnatsAlone ca|credenciales <url> <ca.pem> [<ca.pem de otra CA>]
 */
package com.flux;

import com.flux.broker.SecureEnv;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class JnatsAlone {

    private JnatsAlone() {
    }

    public static void main(String[] args) throws Exception {
        String scenario = args[0];
        String url = args[1];
        Path ca = Path.of(args[2]);
        Options.Builder b = new Options.Builder().server(url).userInfo("u", scenario.equals("credenciales") ? "equivocada" : "p")
                .maxReconnects(-1).reconnectWait(Duration.ofMillis(200));
        b.sslContext(SecureEnv.trusting(scenario.equals("ca") ? Path.of(args[3]) : ca));
        Options options = b.build();
        CompletableFuture<Connection> f = new CompletableFuture<>();
        Thread t = new Thread(() -> {
            try {
                f.complete(Nats.connectReconnectOnConnect(options));
            } catch (Throwable e) {
                f.completeExceptionally(e);
            }
        });
        t.setDaemon(true);
        t.start();
        try {
            Connection c = f.get(scenario.equals("ca") ? 4 : 30, TimeUnit.SECONDS);
            System.out.println("RETURNED " + c.getStatus());
        } catch (TimeoutException e) {
            System.out.println("HANG");
        } catch (Exception e) {
            System.out.println("THROWS " + e.getClass().getSimpleName());
        }
        System.out.flush();
        System.exit(0);
    }
}
