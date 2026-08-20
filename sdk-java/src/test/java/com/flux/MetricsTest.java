/*
 * Metricas del SDK.
 * Contrato normativo: specification/08-observability.md
 */
package com.flux;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class MetricsTest {

    /** Una linea de exposicion valida: nombre, etiquetas opcionales y un numero. */
    private static final Pattern LINEA_PROMETHEUS = Pattern.compile("^[a-z_]+(\\{[^}]*\\})? -?[0-9.]+$");

    @Nested
    @DisplayName("buckets del histograma")
    class Buckets {

        @Test
        @DisplayName("el ultimo bucket ES el ack_wait canonico")
        void ultimoBucketEsAckWait() {
            // 08-observability.md §3: un handler en el bucket superior esta a punto de que
            // su mensaje se reentregue MIENTRAS aun se ejecuta. Si el bucket no coincide
            // con el plazo real, mide algo que no le importa a nadie — y el bucket DEBE
            // moverse si alguien cambia ack_wait. Este test es lo que lo obliga.
            double ultimo = MetricsSink.DURATION_BUCKETS.get(MetricsSink.DURATION_BUCKETS.size() - 1);
            assertEquals(Protocol.DEFAULT_ACK_WAIT.toSeconds(), (long) ultimo);
        }

        @Test
        @DisplayName("son exactamente los doce de la spec, en orden ascendente")
        void bucketsDeLaSpec() {
            assertEquals(
                    List.of(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
                    MetricsSink.DURATION_BUCKETS);
            for (int i = 1; i < MetricsSink.DURATION_BUCKETS.size(); i++) {
                assertTrue(MetricsSink.DURATION_BUCKETS.get(i) > MetricsSink.DURATION_BUCKETS.get(i - 1));
            }
        }
    }

    @Nested
    @DisplayName("nombres y etiquetas del contrato")
    class Contrato {

        @Test
        @DisplayName("las siete metricas usan los nombres y las etiquetas de la spec")
        void sieteMetricas() {
            // Son un CONTRATO con los dashboards y las alertas, no una decision de este
            // SDK: si Java y Go nombran distinto la tasa de DLQ, no se pueden sumar y un
            // panel del ecosistema es imposible — §1.
            InMemoryMetrics m = todasLasMetricas();
            String salida = m.render();

            assertTrue(salida.contains(
                    "flux_events_published_total{outcome=\"ok\",subject=\"pedidos.pedido.v1.creado\"} 1"));
            assertTrue(salida.contains("flux_events_consumed_total{consumer=\"facturacion-api__x\","
                    + "outcome=\"ok\",subject=\"pedidos.pedido.v1.creado\"} 1"));
            assertTrue(salida.contains("flux_events_dlq_total{code=\"HTTP_404\","
                    + "consumer=\"facturacion-api__x\",reason=\"permanent\","
                    + "subject=\"pedidos.pedido.v1.creado\"} 1"));
            assertTrue(salida.contains("flux_events_retried_total{attempt=\"3\","
                    + "consumer=\"facturacion-api__x\",subject=\"pedidos.pedido.v1.creado\"} 1"));
            assertTrue(salida.contains("flux_consumer_pending{consumer=\"facturacion-api__x\","
                    + "subject=\"pedidos.pedido.v1.creado\"} 42"));
            assertTrue(salida.contains("flux_connection_state 1"));
            assertTrue(salida.contains("flux_event_handler_duration_seconds_bucket{"));
        }

        @Test
        @DisplayName("ninguna etiqueta es tenantid, id ni correlationid")
        void sinEtiquetasDeAltaCardinalidad() {
            // §2.2: un tenant nuevo NO debe crear series temporales nuevas. El fallo no
            // avisa —funciona con tres tenants en desarrollo y muere con diez mil en
            // produccion— y se manifiesta como "Prometheus se ha quedado sin memoria", no
            // como "alguien etiqueto por tenant". Por eso MetricsSink no tiene un mapa
            // generico de etiquetas: no hay por donde colarlo.
            String salida = todasLasMetricas().render();
            for (String prohibida : List.of("tenantid=", "id=", "correlationid=")) {
                assertFalse(salida.contains(prohibida),
                        "etiqueta prohibida por 08-observability.md §2.2: " + prohibida);
            }
        }
    }

    @Nested
    @DisplayName("recolector")
    class Recolector {

        @Test
        @DisplayName("cuenta publicaciones por subject y resultado")
        void cuentaPublicaciones() {
            InMemoryMetrics m = new InMemoryMetrics();
            m.eventPublished("pedidos.pedido.v1.creado", MetricsSink.PublishOutcome.OK);
            m.eventPublished("pedidos.pedido.v1.creado", MetricsSink.PublishOutcome.OK);
            m.eventPublished("pedidos.pedido.v1.creado", MetricsSink.PublishOutcome.ERROR);

            Map<String, Long> counters = m.counters();
            assertEquals(2L, counters.get(
                    "flux_events_published_total{outcome=\"ok\",subject=\"pedidos.pedido.v1.creado\"}"));
            assertEquals(1L, counters.get(
                    "flux_events_published_total{outcome=\"error\",subject=\"pedidos.pedido.v1.creado\"}"));
        }

        @Test
        @DisplayName("las etiquetas se ordenan para que la clave sea estable")
        void clavesEstables() {
            // Sin orden estable, la MISMA serie temporal apareceria con dos claves segun el
            // orden en que se construyo el mapa, y el contador se repartiria entre ambas.
            InMemoryMetrics a = new InMemoryMetrics();
            InMemoryMetrics b = new InMemoryMetrics();
            a.eventDlq("s", "c", FluxEvent.DlqReason.PERMANENT, "X");
            b.eventDlq("s", "c", FluxEvent.DlqReason.PERMANENT, "X");
            assertEquals(a.counters().keySet(), b.counters().keySet());
        }

        @Test
        @DisplayName("el histograma acumula en todos los buckets que superan el valor")
        void histogramaAcumulativo() {
            InMemoryMetrics m = new InMemoryMetrics();
            m.handlerDuration("s", "c", 0.03); // cae por encima de 0.025
            String salida = m.render();
            assertTrue(salida.contains("le=\"0.025\"} 0"));
            assertTrue(salida.contains("le=\"0.05\"} 1"));
            assertTrue(salida.contains("le=\"+Inf\"} 1"));
            assertTrue(salida.contains("_count{subject=\"s\",consumer=\"c\"} 1")
                    || salida.contains("_count{consumer=\"c\",subject=\"s\"} 1"));
        }

        @Test
        @DisplayName("los buckets se emiten sin ceros finales, como en Node")
        void bucketsSinCerosFinales() {
            // `le="30.0"` y `le="30"` son etiquetas DISTINTAS: al agregar el histograma de
            // un servicio Java con el de uno de Node saldrian dos series donde hay una.
            InMemoryMetrics m = new InMemoryMetrics();
            m.handlerDuration("s", "c", 0.4);
            String salida = m.render();
            assertTrue(salida.contains("le=\"30\"}"));
            assertTrue(salida.contains("le=\"1\"}"));
            assertFalse(salida.contains("le=\"30.0\"}"));
            assertFalse(salida.contains("le=\"1.0\"}"));
        }

        @Test
        @DisplayName("un gauge sin etiquetas no deja llaves vacias en la salida")
        void gaugeSinEtiquetas() {
            InMemoryMetrics m = new InMemoryMetrics();
            m.connectionState(MetricsSink.ConnectionState.CONNECTED);
            assertTrue(m.render().contains("\nflux_connection_state 1\n"));
        }

        @Test
        @DisplayName("todas las lineas tienen forma valida de Prometheus")
        void formatoDeExposicionValido() {
            for (String linea : todasLasMetricas().render().split("\n")) {
                if (linea.isEmpty() || linea.startsWith("#")) {
                    continue;
                }
                assertTrue(LINEA_PROMETHEUS.matcher(linea).matches(),
                        "linea no valida para Prometheus: " + linea);
            }
        }

        @Test
        @DisplayName("escapa las comillas de los valores de etiqueta")
        void escapaComillas() {
            // Un `code` con comillas rompe el formato y Prometheus descarta el SCRAPE
            // ENTERO, no solo esa linea: un mensaje de error mal formado de un servicio
            // apagaria las metricas de todo el proceso.
            InMemoryMetrics m = new InMemoryMetrics();
            m.eventDlq("s", "c", FluxEvent.DlqReason.PERMANENT, "con \"comillas\" y \\barra");
            for (String linea : m.render().split("\n")) {
                if (linea.startsWith("flux_events_dlq_total")) {
                    assertEquals(0, linea.chars().filter(c -> c == '"').count() % 2,
                            "comillas desbalanceadas: " + linea);
                    assertFalse(linea.contains("\\"), "una barra invertida rompe el escapado");
                }
            }
        }
    }

    @Nested
    @DisplayName("outcome de los fallos de firma")
    class FirmaComoOutcome {

        @Test
        @DisplayName("los tres codigos de firma producen outcome=invalid_signature")
        void codigosDeFirma() {
            // La firma invalida se separa del POISON comun aunque su `reason` siga siendo
            // `poison`: son dos incidentes distintos —basura frente a suplantacion— con dos
            // respuestas distintas, y §2.1 declara la etiqueta justo para eso. Un pico de
            // firmas rotas no debe confundirse con un pico de JSON corrupto.
            for (String code : List.of(Signing.CODE_MISSING_SIGNATURE,
                    Signing.CODE_INVALID_SIGNATURE, Signing.CODE_UNKNOWN_SIGNING_KEY)) {
                assertEquals(MetricsSink.ConsumeOutcome.INVALID_SIGNATURE,
                        FluxBus.consumeOutcome(FluxEvent.DlqReason.POISON, code),
                        "codigo " + code);
            }
        }

        @Test
        @DisplayName("el reason de la DLQ NO cambia: sigue siendo el enum cerrado de 04-errors.md")
        void elReasonNoCambia() {
            // El outcome tiene seis valores; dlqreason tiene tres y es un enum cerrado del
            // envelope. Mezclarlos metaria "invalid_signature" en el atributo `dlqreason` de
            // un evento, que ningun otro SDK sabria leer.
            InMemoryMetrics m = new InMemoryMetrics();
            m.eventDlq("s", "c", FluxEvent.DlqReason.POISON, Signing.CODE_INVALID_SIGNATURE);
            assertTrue(m.render().contains("reason=\"poison\""));
            assertFalse(m.render().contains("reason=\"invalid_signature\""));
        }

        @Test
        @DisplayName("cualquier otro codigo conserva el outcome de su reason")
        void otrosCodigos() {
            assertEquals(MetricsSink.ConsumeOutcome.POISON,
                    FluxBus.consumeOutcome(FluxEvent.DlqReason.POISON, "MALFORMED_JSON"));
            assertEquals(MetricsSink.ConsumeOutcome.PERMANENT,
                    FluxBus.consumeOutcome(FluxEvent.DlqReason.PERMANENT, "HTTP_404"));
            assertEquals(MetricsSink.ConsumeOutcome.RETRYABLE,
                    FluxBus.consumeOutcome(FluxEvent.DlqReason.RETRYABLE, "ECONNRESET"));
        }
    }

    @Nested
    @DisplayName("MetricsSink.NONE")
    class SinMetricas {

        @Test
        @DisplayName("no lanza y no guarda nada — es el default")
        void noOp() {
            // Un SDK de protocolo no debe imponer un backend de metricas a quien solo
            // quiere publicar un evento.
            assertDoesNotThrow(() -> {
                MetricsSink.NONE.eventPublished("s", MetricsSink.PublishOutcome.OK);
                MetricsSink.NONE.eventConsumed("s", "c", MetricsSink.ConsumeOutcome.OK);
                MetricsSink.NONE.handlerDuration("s", "c", 1.0);
                MetricsSink.NONE.eventDlq("s", "c", FluxEvent.DlqReason.POISON, "X");
                MetricsSink.NONE.eventRetried("s", "c", 1);
                MetricsSink.NONE.consumerPending("s", "c", 0);
                MetricsSink.NONE.connectionState(MetricsSink.ConnectionState.DISCONNECTED);
            });
        }
    }

    private static InMemoryMetrics todasLasMetricas() {
        InMemoryMetrics m = new InMemoryMetrics();
        String subject = "pedidos.pedido.v1.creado";
        String consumer = "facturacion-api__x";
        m.eventPublished(subject, MetricsSink.PublishOutcome.OK);
        m.eventConsumed(subject, consumer, MetricsSink.ConsumeOutcome.OK);
        m.eventDlq(subject, consumer, FluxEvent.DlqReason.PERMANENT, "HTTP_404");
        m.eventRetried(subject, consumer, 3);
        m.consumerPending(subject, consumer, 42);
        m.connectionState(MetricsSink.ConnectionState.CONNECTED);
        m.handlerDuration(subject, consumer, 0.4);
        return m;
    }
}
