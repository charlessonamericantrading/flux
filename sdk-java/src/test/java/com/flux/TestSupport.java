/*
 * Utilidades de las pruebas de integracion: un logger que captura, esperas con condicion (no con sleeps a ciegas) y
 * lo que las pruebas comparten.
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

final class TestSupport {

    private TestSupport() {
        throw new AssertionError("clase de utilidad");
    }

    /** Un {@link System.Logger} que guarda lo que se le escribe, para comprobar lo que el SDK DICE. */
    static final class CapturingLogger implements System.Logger {
        final List<String> lines = new CopyOnWriteArrayList<>();

        @Override
        public String getName() {
            return "flux-test";
        }

        @Override
        public boolean isLoggable(Level level) {
            return true;
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String msg, Throwable thrown) {
            lines.add(level + " " + msg);
        }

        @Override
        public void log(Level level, ResourceBundle bundle, String format, Object... params) {
            lines.add(level + " " + format);
        }

        /** Las lineas de nivel ERROR o WARNING que contienen {@code text}. */
        List<String> containing(String text) {
            return lines.stream().filter(l -> l.contains(text)).toList();
        }
    }

    /** Espera a que se cumpla la condicion (hasta {@code millis}); devuelve si se cumplio. */
    static boolean until(long millis, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + millis * 1_000_000;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(20);
        }
        return condition.getAsBoolean();
    }

    static void await(String what, long millis, BooleanSupplier condition) throws InterruptedException {
        if (!until(millis, condition)) {
            fail("no se cumplio en " + millis + " ms: " + what);
        }
    }

    /** Cuanto tarda algo, en ms. */
    static long timed(Runnable r) {
        long t0 = System.nanoTime();
        r.run();
        return (System.nanoTime() - t0) / 1_000_000;
    }
}
