/*
 * Validacion L3 contra el JSON Schema del evento.
 * Contrato normativo: specification/00-protocol.md §5 (nivel L3).
 *
 * Los casos son los mismos que fija sdk-node/test/validation.test.ts, que es la
 * implementacion de referencia. Que un payload sea rechazado en Node y aceptado en Java
 * seria peor que no validar: convertiria el nivel de conformidad en una propiedad del
 * lenguaje del productor.
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ValidationTest {

    private static final String SUBJECT = "pedidos.pedido.v1.creado";

    /** La serie temporal tal y como la indexa {@link InMemoryMetrics}: etiquetas ordenadas. */
    private static final String GAUGE =
            "flux_consumer_pending{consumer=\"durable\",subject=\"" + SUBJECT + "\"}";

    /**
     * La raiz se busca por un marcador y no contando {@code ../}: surefire corre con el
     * directorio de trabajo en {@code sdk-java/}, pero un IDE puede correr desde la raiz
     * del repositorio, y cualquier ruta relativa fija es correcta en uno de los dos casos y
     * silenciosamente incorrecta en el otro.
     */
    private static Path repoRoot() {
        Path dir = Path.of("").toAbsolutePath();
        for (int i = 0; i < 6 && dir != null; i++) {
            if (Files.exists(dir.resolve("protocol.json"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("no se encontro la raiz del repo (protocol.json)");
    }

    private static final SchemaBundle BUNDLE =
            SchemaBundle.fromPath(repoRoot().resolve("schemas").resolve("bundle.json"));

    private static final String URI = BUNDLE.schemaUriFor(SUBJECT);

    /** El payload de ejemplo de AGENTS.md §2, que cumple el esquema. */
    private static Map<String, Object> valido() {
        Map<String, Object> linea = new LinkedHashMap<>();
        linea.put("sku", "ABC-1");
        linea.put("cantidad", 2);
        linea.put("precioUnitarioCents", 4995);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("pedidoId", "ped-123");
        data.put("clienteId", "cli-987");
        data.put("aggregateVersion", 1);
        data.put("totalCents", 9990);
        data.put("moneda", "EUR");
        data.put("lineas", List.of(linea));
        return data;
    }

    private static FluxEvent evento(Map<String, Object> data) {
        return evento(data, URI);
    }

    private static FluxEvent evento(Map<String, Object> data, String dataschema) {
        return new Envelope.BuildEventInput()
                .subject(SUBJECT)
                .data(data)
                .id("01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55")
                .source("/produccion/pedidos-api")
                .producerVersion("3.4.1")
                .tenantId("acme")
                .dataClassification(FluxEvent.DataClassification.INTERNAL)
                .dataSchema(dataschema)
                .correlationId("01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55")
                .build();
    }

    private static Validation.Validator validador(Validation.Mode mode) {
        return Validation.create(new Validation.Options().mode(mode).bundle(BUNDLE));
    }

    /** Cuenta los avisos de {@link Validation.Mode#WARN} sin ensuciar la salida del test. */
    private static final class LoggerDePrueba implements System.Logger {
        private final List<String> avisos = new ArrayList<>();

        @Override
        public String getName() {
            return "test";
        }

        @Override
        public boolean isLoggable(Level level) {
            return true;
        }

        @Override
        public void log(Level level, java.util.ResourceBundle bundle, String msg, Throwable thrown) {
            if (level == Level.WARNING) {
                avisos.add(msg);
            }
        }

        @Override
        public void log(Level level, java.util.ResourceBundle bundle, String format, Object... params) {
            if (level == Level.WARNING) {
                avisos.add(format);
            }
        }
    }

    // ─── El bundle ───────────────────────────────────────────────────────────

    @Nested
    @DisplayName("bundle de esquemas")
    class Bundle {

        @Test
        @DisplayName("indexa el subject hacia su URI de dataschema")
        void indexaElSubject() {
            assertNotNull(URI, "el bundle debe resolver el subject de ejemplo");
            assertTrue(URI.matches(
                    "^https://schemas\\.internal/pedidos/pedido/creado/\\d+\\.\\d+\\.\\d+\\.json$"),
                    "URI inesperada: " + URI);
        }

        @Test
        @DisplayName("el $id del esquema coincide con la clave del bundle")
        void idCoincideConLaClave() {
            assertEquals(URI, BUNDLE.schema(URI).get("$id").textValue());
        }

        @Test
        @DisplayName("un bundle que no es JSON falla con un mensaje accionable")
        void bundleCorrupto() {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> SchemaBundle.fromJson("{no es json"));
            assertTrue(e.getMessage().contains("bundle-schemas.mjs"), e.getMessage());
        }

        @Test
        @DisplayName("un bundle sin esquemas no se acepta: en STRICT fallaria TODO evento")
        void bundleVacio() {
            assertThrows(IllegalArgumentException.class,
                    () -> SchemaBundle.fromJson("{\"subjects\":{},\"schemas\":{}}"));
        }
    }

    // ─── STRICT ──────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("validacion L3 — strict")
    class Strict {

        @Test
        @DisplayName("un payload valido pasa")
        void payloadValido() {
            Validation.Validator v = validador(Validation.Mode.STRICT);
            assertDoesNotThrow(() -> v.check(evento(valido()), SUBJECT));
        }

        @Test
        @DisplayName("falta un campo requerido → lanza")
        void faltaRequerido() {
            Validation.Validator v = validador(Validation.Mode.STRICT);
            Map<String, Object> sinTotal = valido();
            sinTotal.remove("totalCents");
            assertThrows(SchemaValidationException.class, () -> v.check(evento(sinTotal), SUBJECT));
        }

        @Test
        @DisplayName("tipo incorrecto → lanza")
        void tipoIncorrecto() {
            // El caso que la spec llama el mas peligroso: "9990" en vez de 9990. El importe
            // sigue "estando", y un consumidor descuidado lo concatena en vez de sumarlo.
            Validation.Validator v = validador(Validation.Mode.STRICT);
            Map<String, Object> data = valido();
            data.put("totalCents", "9990");
            assertThrows(SchemaValidationException.class, () -> v.check(evento(data), SUBJECT));
        }

        @Test
        @DisplayName("campo desconocido → lanza (additionalProperties: false)")
        void campoDesconocido() {
            // Un campo mal escrito debe fallar, no colarse en silencio: `totalCemts` sin
            // esta regla se publicaria y el consumidor leeria 0.
            Validation.Validator v = validador(Validation.Mode.STRICT);
            Map<String, Object> data = valido();
            data.put("totalCemts", 9990);
            assertThrows(SchemaValidationException.class, () -> v.check(evento(data), SUBJECT));
        }

        @Test
        @DisplayName("patron incumplido → lanza")
        void patronIncumplido() {
            Validation.Validator v = validador(Validation.Mode.STRICT);
            Map<String, Object> data = valido();
            data.put("moneda", "euros");
            assertThrows(SchemaValidationException.class, () -> v.check(evento(data), SUBJECT));
        }

        @Test
        @DisplayName("reporta TODOS los errores, no solo el primero")
        void reportaTodosLosErrores() {
            // Requisito explicito de L3 (00-protocol.md §5): de uno en uno, arreglar un
            // payload con tres campos mal cuesta tres despliegues.
            Validation.Validator v = validador(Validation.Mode.STRICT);
            Map<String, Object> data = valido();
            data.put("totalCents", "x");
            data.put("moneda", "euros");
            data.put("cantidad", 1);

            SchemaValidationException e = assertThrows(SchemaValidationException.class,
                    () -> v.check(evento(data), SUBJECT));
            assertTrue(e.errors().size() >= 2,
                    "esperaba >= 2 errores, hubo " + e.errors().size() + ": " + e.errors());
            assertEquals(SUBJECT, e.subject());
            assertEquals(URI, e.dataschema());
        }

        @Test
        @DisplayName("esquema ausente del bundle → SchemaNotFoundException")
        void esquemaAusente() {
            Validation.Validator v = validador(Validation.Mode.STRICT);
            assertThrows(SchemaNotFoundException.class, () -> v.check(
                    evento(valido(), "https://schemas.internal/no/existe/1.0.0.json"), SUBJECT));
        }
    }

    // ─── WARN y OFF ──────────────────────────────────────────────────────────

    @Nested
    @DisplayName("validacion L3 — warn y off")
    class WarnYOff {

        @Test
        @DisplayName("warn registra pero no lanza")
        void warnNoLanza() {
            LoggerDePrueba logger = new LoggerDePrueba();
            Validation.Validator v = Validation.create(new Validation.Options()
                    .mode(Validation.Mode.WARN).bundle(BUNDLE).logger(logger));
            Map<String, Object> data = valido();
            data.put("totalCents", "x");

            assertDoesNotThrow(() -> v.check(evento(data), SUBJECT));
            assertEquals(1, logger.avisos.size());
            assertTrue(logger.avisos.get(0).contains("no cumple su esquema"), logger.avisos.get(0));
        }

        @Test
        @DisplayName("warn tambien avisa —sin lanzar— cuando el esquema no esta en el bundle")
        void warnEsquemaAusente() {
            LoggerDePrueba logger = new LoggerDePrueba();
            Validation.Validator v = Validation.create(new Validation.Options()
                    .mode(Validation.Mode.WARN).bundle(BUNDLE).logger(logger));

            assertDoesNotThrow(() -> v.check(
                    evento(valido(), "https://schemas.internal/no/existe/1.0.0.json"), SUBJECT));
            assertEquals(1, logger.avisos.size());
        }

        @Test
        @DisplayName("off no compila nada — L2 no paga el coste de L3")
        void offNoCompilaNada() {
            assertNull(Validation.create(new Validation.Options()));
            assertNull(Validation.create(new Validation.Options().mode(Validation.Mode.OFF)));
            // Un null tambien es OFF: es lo que recibe FluxBus cuando nadie configuro nada.
            assertNull(Validation.create(null));
        }

        @Test
        @DisplayName("strict sin bundle falla con un mensaje accionable")
        void strictSinBundle() {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> Validation.create(new Validation.Options().mode(Validation.Mode.STRICT)));
            assertTrue(e.getMessage().contains("bundle-schemas.mjs"), e.getMessage());
        }
    }

    // ─── Consumo ─────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("al consumir")
    class AlConsumir {

        @Test
        @DisplayName("un fallo de esquema se clasifica PERMANENT, no RETRYABLE")
        void esPermanente() {
            // El evento es sintacticamente correcto —ha llegado a parsearse, asi que no es
            // POISON— pero incumple su contrato: reintentarlo seis veces da exactamente el
            // mismo resultado y bloquea la cola 51 minutos para nada — 04-errors.md §1.2.
            FluxErrors.Classification c = Classifier.DEFAULT.classify(
                    new SchemaValidationException(SUBJECT, URI, List.of("$.totalCents: …")));
            assertEquals(ErrorClass.PERMANENT, c.errorClass());
            assertEquals(SchemaValidationException.CODE, c.code());
        }

        @Test
        @DisplayName("un esquema ausente tambien es PERMANENT")
        void esquemaAusenteEsPermanente() {
            FluxErrors.Classification c = Classifier.DEFAULT.classify(
                    new SchemaNotFoundException(SUBJECT, URI));
            assertEquals(ErrorClass.PERMANENT, c.errorClass());
            assertEquals(SchemaNotFoundException.CODE, c.code());
        }

        @Test
        @DisplayName("la metrica lo etiqueta invalid_schema, no permanent")
        void outcomeInvalidSchema() {
            // "un productor incumple su esquema" y "mi logica rechaza este evento" son dos
            // preguntas distintas con dos respuestas distintas — 08-observability.md §2.1.
            // El dlqreason sigue siendo `permanent`: ese es el enum cerrado de 04-errors.md.
            assertEquals(MetricsSink.ConsumeOutcome.INVALID_SCHEMA,
                    FluxBus.consumeOutcome(FluxEvent.DlqReason.PERMANENT,
                            SchemaValidationException.CODE));
            assertEquals(MetricsSink.ConsumeOutcome.PERMANENT,
                    FluxBus.consumeOutcome(FluxEvent.DlqReason.PERMANENT, "PEDIDO_YA_CANCELADO"));
        }
    }

    // ─── Errores ─────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("mensajes de error")
    class Mensajes {

        @Test
        @DisplayName("el mensaje enumera todos los fallos, uno por linea")
        void mensajeEnumeraFallos() {
            SchemaValidationException e = new SchemaValidationException(
                    SUBJECT, URI, List.of("$.totalCents: string found, integer expected",
                            "$.moneda: does not match the regex pattern ^[A-Z]{3}$"));
            assertTrue(e.getMessage().contains(SUBJECT));
            assertTrue(e.getMessage().contains(URI));
            assertEquals(2, e.getMessage().split("\n  · ").length - 1);
        }

        @Test
        @DisplayName("SchemaNotFound dice como regenerar el bundle")
        void mensajeSchemaNotFound() {
            SchemaNotFoundException e = new SchemaNotFoundException(SUBJECT, URI);
            assertTrue(e.getMessage().contains("bundle-schemas.mjs"), e.getMessage());
        }
    }

    // ─── Sondeo de num_pending — 08-observability.md §2.3 ─────────────────────

    @Nested
    @DisplayName("sondeo de flux_consumer_pending")
    class SondeoDePending {

        @Test
        @DisplayName("el default es 15 s y `0` lo desactiva")
        void defaultYDesactivacion() {
            assertEquals(15_000L, FluxBus.DEFAULT_PENDING_POLL_MILLIS);
            assertEquals(FluxBus.DEFAULT_PENDING_POLL_MILLIS,
                    new FluxBus.ConnectOptions().pendingPollMillis());
            assertEquals(0L, new FluxBus.ConnectOptions().pendingPollMillis(0).pendingPollMillis());

            InMemoryMetrics metrics = new InMemoryMetrics();
            assertTrue(FluxBus.pendingPollEnabled(15_000L, metrics));
            assertFalse(FluxBus.pendingPollEnabled(0L, metrics), "`0` DEBE desactivarlo");
            // Sin sink no hay donde escribir el gauge: no se crea el hilo.
            assertFalse(FluxBus.pendingPollEnabled(15_000L, MetricsSink.NONE),
                    "sin metricas no hace falta sondear");
        }

        @Test
        @DisplayName("cada sondeo actualiza el gauge")
        void actualizaElGauge() {
            InMemoryMetrics metrics = new InMemoryMetrics();
            FluxBus.pendingPollTask(SUBJECT, "durable", metrics, () -> 42L, m -> { }).run();
            assertEquals(42.0, metrics.gauges().get(GAUGE));
        }

        @Test
        @DisplayName("un fallo del sondeo NO afecta al consumo ni cancela la tarea")
        void falloDelSondeoNoRompeNada() {
            // Es telemetria. Y ademas: si la excepcion escapara, scheduleAtFixedRate
            // CANCELA la tarea en silencio y la metrica dejaria de emitirse para siempre
            // tras un corte de red de dos segundos — 08-observability.md §2.3.
            InMemoryMetrics metrics = new InMemoryMetrics();
            AtomicInteger logs = new AtomicInteger();
            Runnable task = FluxBus.pendingPollTask(SUBJECT, "durable", metrics,
                    () -> {
                        throw new java.io.IOException("broker no disponible");
                    },
                    m -> logs.incrementAndGet());

            assertDoesNotThrow(task::run);
            assertEquals(1, logs.get());
            assertNull(metrics.gauges().get(GAUGE));

            // Y el siguiente ciclo sigue funcionando: la tarea no se ha envenenado.
            FluxBus.pendingPollTask(SUBJECT, "durable", metrics, () -> 7L, m -> { }).run();
            assertEquals(7.0, metrics.gauges().get(GAUGE));
        }
    }
}
