/**
 * Métricas del SDK.
 * Contrato normativo: specification/08-observability.md
 *
 * Los nombres y etiquetas son parte del contrato, no una decisión de cada SDK: si el
 * de Node y el de Go nombran distinto la tasa de DLQ, un panel del ecosistema es
 * imposible. Por eso viven aquí y no en la aplicación.
 *
 * La implementación es un recolector en memoria sin dependencias. Quien use
 * prom-client, OpenTelemetry o lo que sea implementa `MetricsSink` y recibe los mismos
 * nombres.
 */

/** Buckets del histograma, en segundos — 08-observability.md §3. */
export const DURATION_BUCKETS = [
  0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
] as const;

export type PublishOutcome = "ok" | "invalid_schema" | "error";
export type ConsumeOutcome =
  | "ok"
  | "retryable"
  | "permanent"
  | "poison"
  | "invalid_schema"
  | "invalid_signature";

export type ConnectionState = 0 | 1 | 2; // desconectado | conectado | reconectando

/**
 * Destino de las métricas. Impleméntalo para enchufar el backend que uses.
 *
 * Las firmas fuerzan las etiquetas del protocolo: no hay un `labels: object` genérico
 * a propósito, porque es justo por ahí por donde se cuela un `tenantid` que multiplica
 * las series temporales (§2.2).
 */
export interface MetricsSink {
  eventPublished(subject: string, outcome: PublishOutcome): void;
  eventConsumed(subject: string, consumer: string, outcome: ConsumeOutcome): void;
  handlerDuration(subject: string, consumer: string, seconds: number): void;
  eventDlq(
    subject: string,
    consumer: string,
    reason: "retryable" | "permanent" | "poison",
    code: string,
  ): void;
  eventRetried(subject: string, consumer: string, attempt: number): void;
  consumerPending(subject: string, consumer: string, pending: number): void;
  connectionState(state: ConnectionState): void;
}

/** No-op. El default: un SDK no debe imponer un backend de métricas. */
export const NO_METRICS: MetricsSink = {
  eventPublished() {},
  eventConsumed() {},
  handlerDuration() {},
  eventDlq() {},
  eventRetried() {},
  consumerPending() {},
  connectionState() {},
};

// ─── Recolector en memoria ───────────────────────────────────────────────────

type Labels = Record<string, string | number>;

/**
 * Escapa un valor de etiqueta según el formato de exposición de Prometheus.
 *
 * Se ESCAPA, no se sustituye: un `code` con comillas rompería el scrape entero, pero
 * reemplazarlas por `_` perdería el valor y dejaría dos errores distintos con la misma
 * etiqueta. El orden importa — la barra invertida primero, o se escaparían las que
 * acabamos de introducir.
 */
const escaparEtiqueta = (v: string): string =>
  v.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\n/g, "\\n");

const clave = (name: string, labels: Labels) =>
  `${name}{${Object.entries(labels)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}="${escaparEtiqueta(String(v))}"`)
    .join(",")}}`;

/**
 * Recolector sin dependencias que expone formato de texto de Prometheus.
 *
 * Suficiente para un `/metrics` real. Si ya usas prom-client, implementa `MetricsSink`
 * contra él en vez de esto — lo que importa es conservar nombres y etiquetas.
 */
export class InMemoryMetrics implements MetricsSink {
  readonly #counters = new Map<string, number>();
  readonly #gauges = new Map<string, number>();
  readonly #histograms = new Map<string, { buckets: number[]; sum: number; count: number }>();

  #inc(name: string, labels: Labels, by = 1): void {
    const k = clave(name, labels);
    this.#counters.set(k, (this.#counters.get(k) ?? 0) + by);
  }

  #set(name: string, labels: Labels, value: number): void {
    this.#gauges.set(clave(name, labels), value);
  }

  #observe(name: string, labels: Labels, value: number): void {
    const k = clave(name, labels);
    let h = this.#histograms.get(k);
    if (!h) {
      h = { buckets: new Array(DURATION_BUCKETS.length).fill(0), sum: 0, count: 0 };
      this.#histograms.set(k, h);
    }
    h.sum += value;
    h.count++;
    for (let i = 0; i < DURATION_BUCKETS.length; i++) {
      if (value <= DURATION_BUCKETS[i]) h.buckets[i]++;
    }
  }

  eventPublished(subject: string, outcome: PublishOutcome): void {
    this.#inc("flux_events_published_total", { subject, outcome });
  }

  eventConsumed(subject: string, consumer: string, outcome: ConsumeOutcome): void {
    this.#inc("flux_events_consumed_total", { subject, consumer, outcome });
  }

  handlerDuration(subject: string, consumer: string, seconds: number): void {
    this.#observe("flux_event_handler_duration_seconds", { subject, consumer }, seconds);
  }

  eventDlq(subject: string, consumer: string, reason: string, code: string): void {
    // `code` es un identificador estable, nunca el mensaje: un mensaje lleva ids y
    // timestamps, y su cardinalidad infinita tumba el almacenamiento — §2.2.
    this.#inc("flux_events_dlq_total", { subject, consumer, reason, code });
  }

  eventRetried(subject: string, consumer: string, attempt: number): void {
    this.#inc("flux_events_retried_total", { subject, consumer, attempt });
  }

  consumerPending(subject: string, consumer: string, pending: number): void {
    this.#set("flux_consumer_pending", { subject, consumer }, pending);
  }

  connectionState(state: ConnectionState): void {
    this.#set("flux_connection_state", {}, state);
  }

  /** Formato de exposición de Prometheus. Sírvelo en `/metrics`. */
  render(): string {
    const out: string[] = [];

    out.push("# TYPE flux_events_published_total counter");
    out.push("# TYPE flux_events_consumed_total counter");
    out.push("# TYPE flux_events_dlq_total counter");
    out.push("# TYPE flux_events_retried_total counter");
    for (const [k, v] of [...this.#counters].sort()) out.push(`${k} ${v}`);

    out.push("# TYPE flux_consumer_pending gauge");
    out.push("# TYPE flux_connection_state gauge");
    for (const [k, v] of [...this.#gauges].sort()) out.push(`${k.replace("{}", "")} ${v}`);

    out.push("# TYPE flux_event_handler_duration_seconds histogram");
    for (const [k, h] of [...this.#histograms].sort()) {
      const base = k.slice(0, k.indexOf("{"));
      const labels = k.slice(k.indexOf("{") + 1, -1);
      const sep = labels ? "," : "";
      for (let i = 0; i < DURATION_BUCKETS.length; i++) {
        out.push(`${base}_bucket{${labels}${sep}le="${DURATION_BUCKETS[i]}"} ${h.buckets[i]}`);
      }
      out.push(`${base}_bucket{${labels}${sep}le="+Inf"} ${h.count}`);
      out.push(`${base}_sum{${labels}} ${h.sum}`);
      out.push(`${base}_count{${labels}} ${h.count}`);
    }

    return out.join("\n") + "\n";
  }

  /** Solo para tests. */
  snapshot(): { counters: Map<string, number>; gauges: Map<string, number> } {
    return { counters: new Map(this.#counters), gauges: new Map(this.#gauges) };
  }
}
