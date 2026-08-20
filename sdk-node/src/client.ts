/**
 * Cliente de flux. Nivel de conformidad: L2.
 * Contrato normativo: specification/00-protocol.md §5
 *
 * Regla de diseño: este fichero NO debe exponer ningún tipo de NATS en su API
 * pública. Si lo hiciera, sustituir el broker dejaría de ser un cambio de capa 0-1
 * y pasaría a tocar las aplicaciones — que es exactamente lo que flux existe para
 * evitar (00-protocol.md §3).
 */

// Paquetes @nats-io/* y no `nats`: el paquete monolítico quedó deprecado en favor de
// estos. Un SDK nuevo no debería nacer sobre una dependencia sin mantenimiento.
import {
  connect as natsConnect,
  type ConnectionOptions,
  type NatsConnection,
} from "@nats-io/transport-node";
import {
  jetstream,
  jetstreamManager,
  AckPolicy,
  DeliverPolicy,
  DiscardPolicy,
  ReplayPolicy,
  RetentionPolicy,
  StorageType,
  type JetStreamClient,
  type JetStreamManager,
  type JsMsg,
} from "@nats-io/jetstream";
import { v7 as uuidv7 } from "uuid";

import {
  buildEvent,
  parseEvent,
  serialize,
  toDlqEvent,
  type DataClassification,
  type FluxEvent,
} from "./envelope.js";
import { ErrorClass, PoisonError, type Classification } from "./errors.js";
import { createClassifier, type ClassifierOptions } from "./classify.js";
import {
  contextFromEvent,
  currentContext,
  runWithContext,
  activeTraceparent,
} from "./context.js";
import {
  CONSUMER_DEFAULTS,
  STREAM_DEFAULTS,
  dlqStreamName,
  dlqSubject,
  durableName,
  nanos,
  parseSubject,
  sourceUri,
  streamName,
} from "./protocol.js";

// ─── Opciones ────────────────────────────────────────────────────────────────

export interface ConnectOptions {
  /** `nats://host:4222`, o varios para HA. */
  servers: string | string[];
  /** Nombre del servicio. Alimenta `source` y los nombres de durable consumer. */
  service: string;
  /** `produccion`, `staging`, `dev`. Alimenta `source`. */
  environment: string;
  /** SemVer del servicio. Va en `producerversion`. */
  version: string;
  /** Tenant por defecto de los eventos publicados. */
  tenantId?: string;
  /** Clasificación por defecto. Ver 06-security.md §5. */
  classification?: DataClassification;
  /** Mapa exacto subject → URI de `dataschema`. Gana sobre `schemaBaseUrl`. */
  schemas?: Record<string, string>;
  /** Base para derivar `dataschema` cuando no está en `schemas`. */
  schemaBaseUrl?: string;
  /** Política de clasificación de errores. Ver classify.ts. */
  classifier?: ClassifierOptions;
  /**
   * Réplicas de los streams que el SDK crea si no existen.
   *
   * El default es 1 porque el auto-provisionado es una comodidad de desarrollo: con 3
   * no arrancaría contra un NATS de un solo nodo. **En producción los streams se
   * provisionan por IaC con `replicas: 3`** (02-naming.md §3.2), no desde el SDK —
   * dejar que un servicio cualquiera cree el stream compartido de su dominio con la
   * configuración que le apetezca es una carrera esperando a ocurrir.
   */
  streamReplicas?: number;
  /** Credenciales NATS. NUNCA versionadas — 06-security.md §2. */
  credentials?: { creds?: string; token?: string; user?: string; pass?: string };
  /** Se invoca ante un POISON. Es el único caso que debe despertar a alguien. */
  onPoison?: (info: { subject: string; error: Error; raw: Uint8Array }) => void;
  /** Se invoca al enrutar cualquier evento a la DLQ. */
  onDlq?: (info: { subject: string; event: FluxEvent; classification: Classification }) => void;
  /** Logger opcional. Por defecto, silencio. */
  logger?: Pick<Console, "warn" | "error">;
}

export interface PublishOptions {
  /** Id del agregado. Va al atributo `subject` de CloudEvents — 01-envelope.md §2.1. */
  aggregateId?: string;
  tenantId?: string;
  classification?: DataClassification;
  /** Instante en que ocurrió el hecho, si no es "ahora". */
  time?: Date | string;
  partitionKey?: string;
}

export interface SubscribeOptions {
  /** Nombre de durable explícito. Por defecto se deriva del servicio y el subject. */
  durable?: string;
  maxAckPending?: number;
  /** Filtra por tenant antes de invocar el handler — 06-security.md §4. */
  tenantId?: string;
}

export interface HandlerContext {
  /** Confirma el evento. Devolver normalmente del handler hace lo mismo. */
  ack(): void;
  /** Nº de entrega, empezando en 1. */
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly subject: string;
}

export type Handler<T = unknown> = (
  event: FluxEvent<T>,
  ctx: HandlerContext,
) => void | Promise<void>;

export interface Subscription {
  readonly subject: string;
  readonly durable: string;
  unsubscribe(): Promise<void>;
}

// ─── Errores del cliente ─────────────────────────────────────────────────────

export class ConsumerConfigMismatchError extends Error {
  constructor(
    readonly durable: string,
    readonly differences: Array<{ field: string; requested: unknown; effective: unknown }>,
  ) {
    super(
      `el servidor devolvió una configuración distinta de la solicitada para "${durable}":\n` +
        differences
          .map((d) => `  ${d.field}: solicitado ${JSON.stringify(d.requested)}, efectivo ${JSON.stringify(d.effective)}`)
          .join("\n") +
        `\nJetStream sobrescribe algunos campos en silencio (03-delivery.md §2.1).`,
    );
    this.name = "ConsumerConfigMismatchError";
  }
}

// ─── Cliente ─────────────────────────────────────────────────────────────────

export class FluxBus {
  readonly #nc: NatsConnection;
  readonly #js: JetStreamClient;
  readonly #jsm: JetStreamManager;
  readonly #opts: ConnectOptions;
  readonly #classify: (e: unknown) => Classification;
  readonly #source: string;
  readonly #subscriptions = new Set<{ stop: () => void }>();
  readonly #ensured = new Set<string>();

  constructor(
    nc: NatsConnection,
    js: JetStreamClient,
    jsm: JetStreamManager,
    opts: ConnectOptions,
  ) {
    this.#nc = nc;
    this.#js = js;
    this.#jsm = jsm;
    this.#opts = opts;
    this.#classify = createClassifier(opts.classifier);
    this.#source = sourceUri(opts.environment, opts.service);
  }

  // ─── publish ───────────────────────────────────────────────────────────────

  async publish<T extends object>(
    subject: string,
    data: T,
    options: PublishOptions = {},
  ): Promise<FluxEvent<T>> {
    parseSubject(subject); // falla temprano y con un mensaje útil
    const { domain } = parseSubject(subject);
    await this.#ensureStream(domain);

    const id = uuidv7();
    const inherited = currentContext();

    const event = buildEvent<T>({
      subject,
      data,
      id,
      source: this.#source,
      producerversion: this.#opts.version,
      // El contexto heredado gana sobre el default de conexión: un evento derivado
      // pertenece al tenant del evento que lo causó, no al del servicio.
      tenantid: options.tenantId ?? inherited?.tenantid ?? this.#opts.tenantId ?? "system",
      dataclassification:
        options.classification ?? this.#opts.classification ?? "internal",
      dataschema: this.#schemaFor(subject),
      // Si el evento no nace de otro, se inicializa con su propio id — 01-envelope.md §3.1.
      correlationid: inherited?.correlationid ?? id,
      ...(inherited?.causationid !== undefined && { causationid: inherited.causationid }),
      ...(options.time !== undefined && {
        time: typeof options.time === "string" ? options.time : options.time.toISOString(),
      }),
      ...(options.aggregateId !== undefined && { aggregateId: options.aggregateId }),
      ...(options.partitionKey !== undefined && { partitionkey: options.partitionKey }),
      ...(inherited?.traceparent !== undefined
        ? { traceparent: inherited.traceparent }
        : await this.#traceparent()),
      ...(inherited?.tracestate !== undefined && { tracestate: inherited.tracestate }),
    });

    // msgID pone la cabecera Nats-Msg-Id: deduplica reintentos de PUBLICACIÓN dentro
    // de duplicate_window. NO deduplica reentregas de consumo — 03-delivery.md §3.
    await this.#js.publish(subject, serialize(event), { msgID: event.id });
    return event;
  }

  async #traceparent(): Promise<{ traceparent?: string }> {
    const tp = await activeTraceparent();
    return tp ? { traceparent: tp } : {};
  }

  #schemaFor(subject: string): string {
    const explicit = this.#opts.schemas?.[subject];
    if (explicit) return explicit;
    const base = this.#opts.schemaBaseUrl;
    if (!base) {
      throw new Error(
        `no hay dataschema para "${subject}". Declara \`schemas["${subject}"]\` o \`schemaBaseUrl\` en connect().`,
      );
    }
    const { domain, aggregate, event, major } = parseSubject(subject);
    // Sin registro exacto solo se puede asumir el .0.0 del mayor. Un SDK L3 resolverá
    // el minor real contra el Schema Registry — 00-protocol.md §5.
    return `${base.replace(/\/$/, "")}/${domain}/${aggregate}/${event}/${major}.0.0.json`;
  }

  // ─── subscribe ─────────────────────────────────────────────────────────────

  async subscribe<T = unknown>(
    subject: string,
    handler: Handler<T>,
    options: SubscribeOptions = {},
  ): Promise<Subscription> {
    const { domain } = parseSubject(subject);
    await this.#ensureStream(domain);
    await this.#ensureDlqStream(domain);

    const stream = streamName(domain);
    const durable = options.durable ?? durableName(this.#opts.service, subject);

    const requested = {
      durable_name: durable,
      filter_subject: subject,
      ack_policy: AckPolicy.Explicit,
      // ack_wait DEBE ser backoff[0]: JetStream lo sobrescribe con ese valor sin
      // avisar. Ver 03-delivery.md §2.1.
      ack_wait: nanos(CONSUMER_DEFAULTS.ackWaitMs),
      max_deliver: CONSUMER_DEFAULTS.maxDeliver,
      backoff: CONSUMER_DEFAULTS.backoffMs.map(nanos),
      max_ack_pending: options.maxAckPending ?? CONSUMER_DEFAULTS.maxAckPending,
      deliver_policy: DeliverPolicy.All,
      replay_policy: ReplayPolicy.Instant,
    };

    let effective;
    try {
      effective = (await this.#jsm.consumers.add(stream, requested)).config;
    } catch {
      effective = (await this.#jsm.consumers.info(stream, durable)).config;
    }

    this.#assertConfigHonored(
      durable,
      requested as unknown as Record<string, unknown>,
      effective as unknown as Record<string, unknown>,
    );

    const consumer = await this.#js.consumers.get(stream, durable);
    const messages = await consumer.consume();

    let stopped = false;
    const entry = {
      stop: () => {
        stopped = true;
        messages.stop();
      },
    };
    this.#subscriptions.add(entry);

    void (async () => {
      for await (const m of messages) {
        if (stopped) break;
        try {
          await this.#dispatch(m, subject, durable, handler as Handler, options);
        } catch (e) {
          // Sin este catch, un fallo inesperado de #dispatch —por ejemplo que la
          // publicación en la DLQ falle— rompe el `for await` y el consumidor deja
          // de consumir EN SILENCIO: sin log, y con connected === true, así que el
          // healthcheck sigue diciendo que todo va bien. Un consumidor muerto que
          // parece vivo es peor que uno que se cae.
          this.#opts.logger?.error(
            `[flux] fallo inesperado despachando ${subject}: ${e instanceof Error ? e.message : String(e)}. ` +
              `El mensaje no se confirma y será reentregado.`,
          );
        }
      }
    })();

    return {
      subject,
      durable,
      unsubscribe: async () => {
        entry.stop();
        this.#subscriptions.delete(entry);
      },
    };
  }

  /**
   * Requisito L2: verificar que el servidor aplicó lo que se le pidió.
   * Es la única defensa contra sobrescrituras silenciosas — 03-delivery.md §2.1.
   */
  #assertConfigHonored(
    durable: string,
    requested: Record<string, unknown>,
    effective: Record<string, unknown>,
  ): void {
    type Diff = { field: string; requested: unknown; effective: unknown };
    const check = ["ack_wait", "max_deliver", "max_ack_pending"];
    const diffs: Diff[] = check
      .filter((f) => effective[f] !== undefined && effective[f] !== requested[f])
      .map((f) => ({ field: f, requested: requested[f], effective: effective[f] }));

    const reqB = requested.backoff as number[];
    const effB = (effective.backoff as number[] | undefined) ?? [];
    if (effB.length !== reqB.length || effB.some((v, i) => v !== reqB[i])) {
      diffs.push({ field: "backoff", requested: reqB, effective: effB });
    }
    if (diffs.length > 0) throw new ConsumerConfigMismatchError(durable, diffs);
  }

  // ─── despacho ──────────────────────────────────────────────────────────────

  async #dispatch(
    m: JsMsg,
    subject: string,
    durable: string,
    handler: Handler,
    options: SubscribeOptions,
  ): Promise<void> {
    // `deliveryCount` empieza en 1 en la primera entrega, no en 0.
    const attempt = m.info.deliveryCount;
    const maxAttempts = CONSUMER_DEFAULTS.maxDeliver;

    // POISON se detecta antes del handler: el mensaje no es interpretable, así que
    // el handler nunca llega a verlo — 04-errors.md §1.3.
    let event: FluxEvent;
    try {
      event = parseEvent(m.data);
    } catch (e) {
      const err = e instanceof PoisonError ? e : new PoisonError(String(e));
      this.#opts.onPoison?.({ subject, error: err, raw: m.data });
      this.#opts.logger?.error(`[flux] POISON en ${subject}: ${err.message}`);
      try {
        await this.#sendRawToDlq(subject, m.data, durable, err.message);
      } catch {
        // Igual que arriba: no se termina lo que no se ha llegado a guardar.
        this.#opts.logger?.error(
          `[flux] no se pudo guardar el POISON de ${subject} en la DLQ; se reentregará`,
        );
        return;
      }
      m.term();
      return;
    }

    if (options.tenantId && event.tenantid !== options.tenantId) {
      m.ack(); // no es para nosotros
      return;
    }

    // WIP mientras el handler vive: extiende ack_wait y evita que el mismo evento se
    // ejecute en concurrencia consigo mismo — 03-delivery.md §2.1.
    const wip = setInterval(() => {
      try {
        m.working();
      } catch {
        /* el mensaje ya está resuelto */
      }
    }, CONSUMER_DEFAULTS.ackWaitMs / 2);

    try {
      await runWithContext(contextFromEvent(event), async () =>
        handler(event, {
          ack: () => {},
          attempt,
          maxAttempts,
          subject,
        }),
      );
      m.ack();
    } catch (e) {
      const c = this.#classify(e);
      const message = e instanceof Error ? e.message : String(e);

      // El presupuesto efectivo es el menor entre el del consumidor y el que la
      // clasificación imponga para ESTE error. Así un error desconocido agota su
      // presupuesto acotado sin recortar los 6 intentos de un ECONNRESET reconocido.
      const budget = Math.min(maxAttempts, c.maxAttempts ?? maxAttempts);

      if (c.class === ErrorClass.RETRYABLE && attempt < budget) {
        // Retraso explícito según el backoff canónico, en vez de dejar expirar
        // ack_wait: no retiene la ranura de ack_pending 30 s de más.
        const delay =
          c.retryAfterMs ??
          CONSUMER_DEFAULTS.backoffMs[
            Math.min(attempt - 1, CONSUMER_DEFAULTS.backoffMs.length - 1)
          ];
        this.#opts.logger?.warn(
          `[flux] RETRYABLE ${c.code} en ${subject} (intento ${attempt}/${budget}), reintento en ${delay}ms`,
        );
        m.nak(delay);
        return;
      }

      const reason =
        c.class === ErrorClass.POISON
          ? "poison"
          : c.class === ErrorClass.RETRYABLE
            ? "retryable" // agotó los reintentos
            : "permanent";

      try {
        await this.#sendToDlq(subject, event, durable, attempt, reason, `${c.code}: ${message}`);
      } catch (dlqError) {
        // NO se hace term(): terminar aquí borraría el evento sin haberlo guardado en
        // ningún sitio. Se deja reentregar — es preferible reprocesar un evento que
        // perderlo sin rastro. La causa suele ser un problema del broker, no del evento.
        this.#opts.logger?.error(
          `[flux] no se pudo publicar en la DLQ para ${subject} (evento ${event.id}): ` +
            `${dlqError instanceof Error ? dlqError.message : String(dlqError)}. ` +
            `El mensaje NO se termina; se reentregará.`,
        );
        return;
      }

      this.#opts.onDlq?.({ subject, event, classification: c });
      this.#opts.logger?.error(
        `[flux] DLQ (${reason}) ${c.code} en ${subject} tras ${attempt} intento(s): ${message}`,
      );
      m.term();
    } finally {
      clearInterval(wip);
    }
  }

  async #sendToDlq(
    subject: string,
    event: FluxEvent,
    consumer: string,
    attempts: number,
    reason: "retryable" | "permanent" | "poison",
    error: string,
  ): Promise<void> {
    const dlqEvent = toDlqEvent(event, { reason, attempts, consumer, error });
    await this.#js.publish(dlqSubject(subject), serialize(dlqEvent), {
      msgID: `dlq-${event.id}-${consumer}`,
    });
  }

  /** Un POISON no tiene CloudEvent que preservar; se guarda el cuerpo crudo. */
  async #sendRawToDlq(
    subject: string,
    raw: Uint8Array,
    consumer: string,
    error: string,
  ): Promise<void> {
    const envelope = {
      specversion: "1.0",
      id: uuidv7(),
      source: this.#source,
      type: "com.flux.system.poison.capturado.v1",
      time: new Date().toISOString(),
      datacontenttype: "application/json",
      dataschema: "https://schemas.internal/system/poison/capturado/1.0.0.json",
      correlationid: uuidv7(),
      tenantid: "system",
      producerversion: this.#opts.version,
      dataclassification: "internal" as const,
      dlqreason: "poison" as const,
      dlqattempts: 1,
      dlqconsumer: consumer,
      dlqerror: error.slice(0, 1024),
      dlqtime: new Date().toISOString(),
      data: {
        originalSubject: subject,
        rawBase64: Buffer.from(raw).toString("base64").slice(0, 700_000),
        rawBytes: raw.byteLength,
      },
    };
    await this.#js.publish(
      dlqSubject(subject),
      new TextEncoder().encode(JSON.stringify(envelope)),
      { msgID: `poison-${envelope.id}` },
    );
  }

  // ─── provisión de streams ──────────────────────────────────────────────────

  async #ensureStream(domain: string): Promise<void> {
    const name = streamName(domain);
    if (this.#ensured.has(name)) return;
    await this.#addStreamIfAbsent(name, [`${domain}.>`], {
      max_age: nanos(STREAM_DEFAULTS.maxAgeMs),
      duplicate_window: nanos(STREAM_DEFAULTS.duplicateWindowMs),
    });
    this.#ensured.add(name);
  }

  async #ensureDlqStream(domain: string): Promise<void> {
    const name = dlqStreamName(domain);
    if (this.#ensured.has(name)) return;
    // Prefijo `dlq.`, no sufijo: si fuese sufijo encajaría con `<domain>.>` y el
    // stream principal capturaría sus propios muertos — 02-naming.md §3.1.
    await this.#addStreamIfAbsent(name, [`dlq.${domain}.>`], {
      max_age: nanos(STREAM_DEFAULTS.dlqMaxAgeMs),
    });
    this.#ensured.add(name);
  }

  async #addStreamIfAbsent(
    name: string,
    subjects: string[],
    extra: Record<string, unknown>,
  ): Promise<void> {
    try {
      await this.#jsm.streams.info(name);
    } catch {
      await this.#jsm.streams.add({
        name,
        subjects,
        storage: StorageType.File,
        retention: RetentionPolicy.Limits,
        discard: DiscardPolicy.Old,
        num_replicas: this.#opts.streamReplicas ?? 1,
        ...extra,
      });
    }
  }

  // ─── ciclo de vida ─────────────────────────────────────────────────────────

  get connected(): boolean {
    return !this.#nc.isClosed();
  }

  async close(): Promise<void> {
    for (const s of this.#subscriptions) s.stop();
    this.#subscriptions.clear();
    await this.#nc.drain();
  }
}

/**
 * Conecta al bus. Reconecta indefinidamente con backoff y jitter: sin jitter, mil
 * servicios reconectan en el mismo milisegundo y tiran el cluster que acaba de
 * levantarse — 03-delivery.md §6.
 */
export async function connect(options: ConnectOptions): Promise<FluxBus> {
  const natsOptions: ConnectionOptions = {
    servers: options.servers,
    name: `${options.service}@${options.environment}`,
    maxReconnectAttempts: -1,
    reconnectTimeWait: 1000,
    reconnectJitter: 500,
    reconnectJitterTLS: 1000,
    waitOnFirstConnect: true,
    ...options.credentials,
  };

  const nc = await natsConnect(natsOptions);
  const jsm = await jetstreamManager(nc);
  const js = jetstream(nc);
  return new FluxBus(nc, js, jsm, options);
}
