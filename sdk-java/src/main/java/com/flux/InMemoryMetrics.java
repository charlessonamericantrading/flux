/*
 * Recolector de metricas en memoria con salida en formato Prometheus.
 * Contrato normativo: specification/08-observability.md
 *
 * Sin dependencias: no hay ni Micrometer ni el cliente de Prometheus. Es suficiente para
 * servir un `/metrics` real, y quien ya use otro backend implementa MetricsSink contra el
 * — lo que importa es conservar nombres y etiquetas, que es lo que fija el contrato.
 */
package com.flux;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Recolector en memoria, seguro entre hilos, que renderiza el formato de exposicion de
 * texto de Prometheus.
 *
 * <pre>{@code
 * InMemoryMetrics metrics = new InMemoryMetrics();
 * FluxBus bus = FluxBus.connect(new FluxBus.ConnectOptions()
 *         .service("pedidos-api")
 *         .metrics(metrics));
 * // en tu servidor HTTP:  responder(metrics.render());
 * }</pre>
 */
public final class InMemoryMetrics implements MetricsSink {

    /** Series ordenadas por clave: sin orden estable, un scrape difiere del siguiente. */
    private final Map<String, AtomicLong> counters = new TreeMap<>();

    private final Map<String, Double> gauges = new TreeMap<>();

    private final Map<String, Histogram> histograms = new TreeMap<>();

    /** Un histograma acumulativo con los buckets de {@link MetricsSink#DURATION_BUCKETS}. */
    private static final class Histogram {
        private final long[] buckets = new long[DURATION_BUCKETS.size()];
        private double sum;
        private long count;
    }

    // ─── Claves de serie ─────────────────────────────────────────────────────

    /**
     * {@code nombre{k="v",...}} con las etiquetas ORDENADAS por nombre.
     *
     * <p>Sin orden estable, la misma serie temporal apareceria con dos claves distintas
     * segun el orden en que se construyo el mapa, y los contadores se repartirian entre
     * ambas sin que nada avisara.
     */
    private static String key(String name, Map<String, String> labels) {
        StringBuilder sb = new StringBuilder(name).append('{');
        boolean first = true;
        for (Map.Entry<String, String> e : new TreeMap<>(labels).entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append(e.getKey()).append("=\"").append(escape(e.getValue())).append('"');
        }
        return sb.append('}').toString();
    }

    /**
     * Neutraliza comillas, barras invertidas y saltos de linea en un valor de etiqueta.
     *
     * <p>No es cosmetica: un {@code code} con una comilla rompe el formato de exposicion y
     * Prometheus descarta el <b>scrape entero</b>, no solo esa linea. Es decir, un mensaje
     * de error mal formado de un servicio apagaria las metricas de todo el proceso.
     */
    private static String escape(String value) {
        String v = value == null ? "" : value;
        StringBuilder sb = new StringBuilder(v.length());
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            sb.append(c == '"' || c == '\\' || c == '\n' ? '_' : c);
        }
        return sb.toString();
    }

    private static Map<String, String> labels(String... pairs) {
        Map<String, String> map = new TreeMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put(pairs[i], pairs[i + 1]);
        }
        return map;
    }

    // ─── Registro ────────────────────────────────────────────────────────────

    private void inc(String name, Map<String, String> labels) {
        String k = key(name, labels);
        synchronized (this) {
            counters.computeIfAbsent(k, unused -> new AtomicLong()).incrementAndGet();
        }
    }

    private void set(String name, Map<String, String> labels, double value) {
        String k = key(name, labels);
        synchronized (this) {
            gauges.put(k, value);
        }
    }

    private void observe(String name, Map<String, String> labels, double value) {
        String k = key(name, labels);
        synchronized (this) {
            Histogram h = histograms.computeIfAbsent(k, unused -> new Histogram());
            h.sum += value;
            h.count++;
            for (int i = 0; i < DURATION_BUCKETS.size(); i++) {
                // Acumulativo: un valor cae en SU bucket y en todos los superiores, que es
                // lo que exige el formato de Prometheus (`le` = less or equal).
                if (value <= DURATION_BUCKETS.get(i)) {
                    h.buckets[i]++;
                }
            }
        }
    }

    @Override
    public void eventPublished(String subject, PublishOutcome outcome) {
        inc("flux_events_published_total", labels("subject", subject, "outcome", outcome.wire()));
    }

    @Override
    public void eventConsumed(String subject, String consumer, ConsumeOutcome outcome) {
        inc("flux_events_consumed_total",
                labels("subject", subject, "consumer", consumer, "outcome", outcome.wire()));
    }

    @Override
    public void handlerDuration(String subject, String consumer, double seconds) {
        observe("flux_event_handler_duration_seconds", labels("subject", subject, "consumer", consumer),
                seconds);
    }

    @Override
    public void eventDlq(String subject, String consumer, FluxEvent.DlqReason reason, String code) {
        inc("flux_events_dlq_total", labels(
                "subject", subject, "consumer", consumer, "reason", reason.wire(), "code", code));
    }

    @Override
    public void eventRetried(String subject, String consumer, int attempt) {
        inc("flux_events_retried_total", labels(
                "subject", subject, "consumer", consumer, "attempt", Integer.toString(attempt)));
    }

    @Override
    public void consumerPending(String subject, String consumer, long pending) {
        set("flux_consumer_pending", labels("subject", subject, "consumer", consumer), pending);
    }

    @Override
    public void connectionState(ConnectionState state) {
        set("flux_connection_state", labels(), state.value());
    }

    // ─── Exposicion ──────────────────────────────────────────────────────────

    /**
     * El formato de exposicion de texto de Prometheus. Sirvelo tal cual en {@code /metrics}.
     *
     * <p>Los numeros se emiten sin ceros finales ({@code 30}, no {@code 30.0}) para que la
     * salida sea byte a byte comparable con la del SDK de Node, que es la referencia.
     */
    public String render() {
        List<String> out = new ArrayList<>();

        synchronized (this) {
            // El orden de las lineas es el mismo que el del SDK de Node —cada familia
            // precedida de su `# TYPE`— para que la salida de los dos sea comparable de un
            // vistazo cuando alguien depura por que dos servicios no cuadran.
            out.add("# TYPE flux_events_published_total counter");
            out.add("# TYPE flux_events_consumed_total counter");
            out.add("# TYPE flux_events_dlq_total counter");
            out.add("# TYPE flux_events_retried_total counter");
            for (Map.Entry<String, AtomicLong> e : counters.entrySet()) {
                out.add(e.getKey() + " " + e.getValue().get());
            }

            out.add("# TYPE flux_consumer_pending gauge");
            out.add("# TYPE flux_connection_state gauge");
            for (Map.Entry<String, Double> e : gauges.entrySet()) {
                // Un gauge sin etiquetas no debe dejar unas llaves vacias en la salida.
                out.add(e.getKey().replace("{}", "") + " " + number(e.getValue()));
            }

            out.add("# TYPE flux_event_handler_duration_seconds histogram");
            for (Map.Entry<String, Histogram> e : histograms.entrySet()) {
                String k = e.getKey();
                Histogram h = e.getValue();
                String base = k.substring(0, k.indexOf('{'));
                String labels = k.substring(k.indexOf('{') + 1, k.length() - 1);
                String sep = labels.isEmpty() ? "" : ",";
                for (int i = 0; i < DURATION_BUCKETS.size(); i++) {
                    out.add(base + "_bucket{" + labels + sep + "le=\"" + number(DURATION_BUCKETS.get(i))
                            + "\"} " + h.buckets[i]);
                }
                // +Inf es obligatorio y su valor es el total: sin el, Prometheus no puede
                // calcular cuantas observaciones quedaron por encima del ultimo bucket.
                out.add(base + "_bucket{" + labels + sep + "le=\"+Inf\"} " + h.count);
                out.add(base + "_sum{" + labels + "} " + number(h.sum));
                out.add(base + "_count{" + labels + "} " + h.count);
            }
        }

        return String.join("\n", out) + "\n";
    }

    /**
     * Renderiza un double como lo haria JavaScript: sin ceros finales.
     *
     * <p>{@code Double.toString(30.0)} da {@code "30.0"} y el SDK de Node emite
     * {@code "30"}. Prometheus acepta las dos, pero un {@code le="30.0"} y un
     * {@code le="30"} son etiquetas DISTINTAS: al agregar el histograma de un servicio Java
     * con el de uno de Node saldrian dos series donde hay una.
     */
    private static String number(double value) {
        if (value == Math.rint(value) && !Double.isInfinite(value)) {
            return Long.toString((long) value);
        }
        return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    }

    /** Copia de los contadores, para tests. */
    public Map<String, Long> counters() {
        Map<String, Long> copy = new TreeMap<>();
        synchronized (this) {
            counters.forEach((k, v) -> copy.put(k, v.get()));
        }
        return copy;
    }

    /** Copia de los gauges, para tests. */
    public Map<String, Double> gauges() {
        synchronized (this) {
            return new TreeMap<>(gauges);
        }
    }
}
