/*
 * Clasificacion de errores del handler.
 * Contrato normativo: specification/04-errors.md §2
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flux.FluxErrors.Classification;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ClassifierTest {

    private final Classifier classifier = Classifier.DEFAULT;

    @Test
    @DisplayName("una excepcion tipada de flux siempre gana: la aplicacion sabe mas que el SDK")
    void erroresTipadosGanan() {
        Classification retryable = classifier.classify(
                new FluxErrors.RetryableException("proveedor 503", "PROVEEDOR_503"));
        assertEquals(ErrorClass.RETRYABLE, retryable.errorClass());
        assertEquals("PROVEEDOR_503", retryable.code());
        assertNull(retryable.maxAttempts(), "un RETRYABLE declarado conserva los 6 intentos");

        Classification permanent = classifier.classify(
                new FluxErrors.PermanentException("pedido ya cancelado", "PEDIDO_YA_CANCELADO"));
        assertEquals(ErrorClass.PERMANENT, permanent.errorClass());
        assertEquals("PEDIDO_YA_CANCELADO", permanent.code());

        Classification poison = classifier.classify(new FluxErrors.PoisonException("roto", "MALFORMED_JSON"));
        assertEquals(ErrorClass.POISON, poison.errorClass());
    }

    @Test
    @DisplayName("sin code explicito, el codigo cae al nombre del tipo para no perder la metrica")
    void codigoPorDefecto() {
        assertEquals("RetryableException",
                classifier.classify(new FluxErrors.RetryableException("algo")).code());
    }

    @Test
    @DisplayName("retryAfter viaja en la clasificacion")
    void retryAfterSePropaga() {
        Classification c = classifier.classify(new FluxErrors.RetryableException(
                "espera", "LENTO", Duration.ofSeconds(5), null));
        assertEquals(Duration.ofSeconds(5), c.retryAfter());
    }

    @Test
    @DisplayName("la clasificacion atraviesa errores envueltos")
    void atraviesaErroresEnvueltos() {
        // El instanceof de Node solo mira el objeto de arriba; aqui se recorre la cadena de
        // causas, igual que errors.As en Go. Un CompletionException de un CompletableFuture
        // es el envoltorio mas comun en Java.
        Throwable envuelto = new CompletionException(
                new IllegalStateException("capa intermedia",
                        new FluxErrors.RetryableException("la de dentro", "DENTRO")));
        Classification c = classifier.classify(envuelto);
        assertEquals(ErrorClass.RETRYABLE, c.errorClass());
        assertEquals("DENTRO", c.code());
    }

    @Test
    @DisplayName("una cadena de causas ciclica no cuelga el despacho")
    void cadenaCiclicaNoCuelga() {
        // En Java se puede construir un ciclo con initCause; recorrerlo sin limite colgaria
        // el hilo de despacho de la suscripcion entera.
        Exception a = new Exception("a");
        Exception b = new Exception("b", a);
        a.initCause(b);
        assertEquals(ErrorClass.RETRYABLE, classifier.classify(a).errorClass());
    }

    @Test
    @DisplayName("los status HTTP siguen la tabla de la spec")
    void statusHttp() {
        for (int status : new int[] {429, 502, 503, 504}) {
            Classification c = classifier.classify(new Classifier.HttpException(status, "dependencia"));
            assertEquals(ErrorClass.RETRYABLE, c.errorClass(), "HTTP " + status);
            assertEquals("HTTP_" + status, c.code());
        }
        // Reintentar un 400 o un 422 es gastar 51 minutos para obtener la misma respuesta.
        for (int status : new int[] {400, 403, 404, 422, 501}) {
            Classification c = classifier.classify(new Classifier.HttpException(status, "dependencia"));
            assertEquals(ErrorClass.PERMANENT, c.errorClass(), "HTTP " + status);
            assertEquals("HTTP_" + status, c.code());
        }
    }

    @Test
    @DisplayName("se lee el Retry-After que anuncia la dependencia")
    void retryAfterDeLaDependencia() {
        Classification c = classifier.classify(
                new Classifier.HttpException(503, "no disponible", Duration.ofSeconds(12)));
        assertEquals(Duration.ofSeconds(12), c.retryAfter());

        // En un PERMANENT no hay reintento del que hablar.
        assertNull(classifier.classify(
                new Classifier.HttpException(400, "mal", Duration.ofSeconds(12))).retryAfter());
    }

    @Test
    @DisplayName("los fallos de red se reconocen por TIPO, nunca por el texto del mensaje")
    void erroresDeRed() {
        // 04-errors.md §1.1: la clasificacion se define por semantica. El mensaje de un
        // SocketException lo escribe el sistema operativo y difiere entre Windows y Linux;
        // un port literal de la lista de codigos de Node produjo exactamente ese bug.
        record Caso(Throwable error, String code) {
        }
        for (Caso caso : new Caso[] {
                new Caso(new ConnectException("Connection refused"), "ConnectException"),
                new Caso(new SocketException("Connection reset"), "SocketException"),
                new Caso(new SocketException("Broken pipe"), "SocketException"),
                new Caso(new UnknownHostException("api.interna"), "UnknownHostException"),
        }) {
            Classification c = classifier.classify(caso.error());
            assertEquals(ErrorClass.RETRYABLE, c.errorClass(), caso.error().toString());
            assertEquals(caso.code(), c.code());
            assertNull(c.maxAttempts(),
                    "un transitorio RECONOCIDO conserva los 6 intentos completos — 04-errors.md §2.1");
        }
    }

    @Test
    @DisplayName("el error de red se reconoce aunque venga envuelto en un IOException de la app")
    void erroresDeRedEnvueltos() {
        Throwable envuelto = new UncheckedIOException(
                new IOException("fallo llamando al proveedor", new ConnectException("refused")));
        Classification c = classifier.classify(envuelto);
        assertEquals(ErrorClass.RETRYABLE, c.errorClass());
        assertEquals("ConnectException", c.code());
    }

    @Test
    @DisplayName("el codigo es el tipo mas especifico, no la superclase")
    void codigoEsElTipoMasEspecifico() {
        // ConnectException extiende SocketException: si la superclase se comprobase primero,
        // toda la telemetria de red se colapsaria en "SocketException".
        assertEquals("ConnectException", classifier.classify(new ConnectException("x")).code());
    }

    @Test
    @DisplayName("los timeouts siguen su propia politica")
    void timeoutsSegunPolitica() {
        // SocketTimeoutException extiende InterruptedIOException, NO SocketException: el
        // propio arbol de tipos del JDK separa "la red fallo" de "no cupo en la ventana".
        Classification porDefecto = classifier.classify(new SocketTimeoutException("Read timed out"));
        assertEquals(ErrorClass.RETRYABLE, porDefecto.errorClass());
        assertEquals("TIMEOUT", porDefecto.code());

        Classifier permanente = new Classifier(new Classifier.ClassifierOptions()
                .timeoutPolicy(Classifier.TimeoutPolicy.PERMANENT));
        assertEquals(ErrorClass.PERMANENT,
                permanente.classify(new SocketTimeoutException("Read timed out")).errorClass());

        assertEquals("TIMEOUT",
                classifier.classify(new java.util.concurrent.TimeoutException("plazo")).code());
    }

    // ─── El default: RETRYABLE acotado ───────────────────────────────────────

    @Test
    @DisplayName("lo desconocido es RETRYABLE con presupuesto de 2 entregas")
    void desconocidoEsRetryableAcotado() {
        // 04-errors.md §2.1. Domina a las dos alternativas: un transitorio desconocido se
        // recupera en el 2º intento y un sistematico llega a la DLQ en ~30 s.
        Classification c = classifier.classify(new IllegalStateException("algo que nadie previo"));
        assertEquals(ErrorClass.RETRYABLE, c.errorClass());
        assertEquals("UNKNOWN", c.code());
        assertEquals(2, c.maxAttempts());
        assertEquals(2, Classifier.DEFAULT_UNKNOWN_RETRY_BUDGET);
    }

    @Test
    @DisplayName("la politica de lo desconocido es configurable")
    void politicaDeLoDesconocidoConfigurable() {
        Classifier permanente = new Classifier(new Classifier.ClassifierOptions()
                .unknownErrorPolicy(Classifier.UnknownErrorPolicy.PERMANENT));
        Classification p = permanente.classify(new IllegalStateException("x"));
        assertEquals(ErrorClass.PERMANENT, p.errorClass());
        assertNull(p.maxAttempts());

        Classifier completo = new Classifier(new Classifier.ClassifierOptions()
                .unknownErrorPolicy(Classifier.UnknownErrorPolicy.RETRYABLE));
        Classification r = completo.classify(new IllegalStateException("x"));
        assertEquals(ErrorClass.RETRYABLE, r.errorClass());
        assertNull(r.maxAttempts(), "RETRYABLE completo = los 6 intentos del consumidor");

        Classifier acotadoA3 = new Classifier(new Classifier.ClassifierOptions()
                .unknownErrorPolicy(Classifier.UnknownErrorPolicy.RETRYABLE_BOUNDED)
                .unknownRetryBudget(3));
        assertEquals(3, acotadoA3.classify(new IllegalStateException("x")).maxAttempts());
    }

    @Test
    @DisplayName("el presupuesto acotado NO recorta los reintentos de un transitorio reconocido")
    void presupuestoNoAfectaALoReconocido() {
        // El presupuesto va por error via Classification.maxAttempts y no en max_deliver,
        // que es por consumidor — 04-errors.md §2.1.
        assertNull(classifier.classify(new ConnectException("refused")).maxAttempts());
        assertNull(classifier.classify(new Classifier.HttpException(503, "x")).maxAttempts());
        assertEquals(2, classifier.classify(new RuntimeException("?")).maxAttempts());
    }

    // ─── Reglas de la aplicacion ─────────────────────────────────────────────

    @Test
    @DisplayName("las reglas de la aplicacion se evaluan antes que la heuristica del SDK")
    void reglasDeLaAplicacion() {
        Classifier conRegla = new Classifier(new Classifier.ClassifierOptions()
                .addRule(e -> e.getMessage() != null && e.getMessage().contains("deadlock")
                        ? Optional.of(new Classification(ErrorClass.RETRYABLE, "DB_DEADLOCK"))
                        : Optional.empty()));

        Classification c = conRegla.classify(new RuntimeException("deadlock detected"));
        assertEquals(ErrorClass.RETRYABLE, c.errorClass());
        assertEquals("DB_DEADLOCK", c.code());

        // Pero un error tipado de flux sigue ganando a la regla.
        Classification tipado = conRegla.classify(
                new FluxErrors.PermanentException("deadlock detected", "MIO"));
        assertEquals("MIO", tipado.code());

        // Y si la regla cede, se sigue con el resto de la cadena.
        assertEquals("UNKNOWN", conRegla.classify(new RuntimeException("otra cosa")).code());
    }

    @Test
    @DisplayName("las reglas se copian: la politica no cambia bajo los pies del consumidor")
    void reglasSeCopian() {
        Classifier.ClassifierOptions options = new Classifier.ClassifierOptions();
        Classifier creado = new Classifier(options);
        options.addRule(e -> Optional.of(new Classification(ErrorClass.PERMANENT, "TARDE")));
        assertEquals("UNKNOWN", creado.classify(new RuntimeException("x")).code());
    }

    @Test
    @DisplayName("clasificar null devuelve algo inerte en vez de una clase falsa")
    void nullEsInerte() {
        Classification c = classifier.classify(null);
        assertEquals(ErrorClass.PERMANENT, c.errorClass());
        assertEquals("NIL_ERROR", c.code());
    }

    @Test
    @DisplayName("las tres clases se traducen a la razon de DLQ de la spec")
    void clasesADlqReason() {
        assertEquals(FluxEvent.DlqReason.RETRYABLE, ErrorClass.RETRYABLE.toDlqReason());
        assertEquals(FluxEvent.DlqReason.PERMANENT, ErrorClass.PERMANENT.toDlqReason());
        assertEquals(FluxEvent.DlqReason.POISON, ErrorClass.POISON.toDlqReason());
        assertEquals("retryable", ErrorClass.RETRYABLE.wire());
    }

    @Test
    @DisplayName("asClassified y findCause recorren la cadena")
    void inspeccion() {
        Throwable envuelto = new RuntimeException("fuera",
                new FluxErrors.PermanentException("dentro", "X"));
        assertTrue(FluxErrors.isClassified(envuelto));
        assertEquals("X", FluxErrors.asClassified(envuelto).orElseThrow().code());
        assertTrue(FluxErrors.findCause(envuelto, FluxErrors.PermanentException.class).isPresent());
        assertTrue(FluxErrors.findCause(envuelto, ConnectException.class).isEmpty());
    }
}
