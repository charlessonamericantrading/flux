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
  type ConsumerConfig,
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
import { ConsumerConfigMismatchError, TenantIsolationError } from "./client-errors.js";
import { StateClient } from "./state.js";
import {
  RpcClient,
  type RequestOptions,
  type RespondOptions,
  type Responder,
  type RpcHandler,
} from "./rpc.js";
import { createClassifier, createDetailedClassifier, type ClassifierOptions } from "./classify.js";
import { NO_METRICS, type MetricsSink } from "./metrics.js";
import {
  createSigner,
  createVerifier,
  type Signer,
  type Verifier,
  type SigningOptions,
} from "./signing.js";
import {
  createValidator,
  schemaUriFor,
  SchemaValidationError,
  type ValidationOptions,
  type Validator,
} from "./validation.js";
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
import {
  NotProvisionedError,
  PermissionDeniedError,
  buildAuthenticator,
  buildTls,
  inboxPrefixFor,
  isNotFound,
  normalizeDeniedSubject,
  permanentConnectError,
  permissionViolation,
  type Credentials,
  type TlsOptions,
} from "./security.js";

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
  /**
   * Tenant por defecto de los eventos publicados (y, si cumple el patrón de estado, de
   * `bus.state`: ahí NO hay `system` implícito ni se admite cualquier cadena — 10-state.md §2.4).
   */
  tenantId?: string;
  /**
   * Aislamiento entre tenants — 09-multitenancy.md §3.
   *
   * `"strict"`: toda suscripción filtra por `tenantId`, y suscribirse sin tenant
   * configurado es un error de configuración. Un filtro que hay que acordarse de
   * poner es un filtro que alguien olvidará, y el fallo —ver los datos de otro
   * tenant— no produce ningún error: produce un incidente de privacidad que se
   * descubre semanas después.
   *
   * `"off"` (default): el filtrado es opcional por suscripción.
   */
  tenantIsolation?: "off" | "strict";
  /** Clasificación por defecto. Ver 06-security.md §5. */
  classification?: DataClassification;
  /** Mapa exacto subject → URI de `dataschema`. Gana sobre `schemaBaseUrl`. */
  schemas?: Record<string, string>;
  /** Base para derivar `dataschema` cuando no está en `schemas`. */
  schemaBaseUrl?: string;
  /** Política de clasificación de errores. Ver classify.ts. */
  classifier?: ClassifierOptions;
  /**
   * Validación L3 contra el JSON Schema del evento. Ver validation.ts.
   * Con `mode: "strict"`, publicar un payload que viola su contrato falla en el
   * productor en vez de aparecer como un misterio en un consumidor de otro equipo.
   */
  validation?: ValidationOptions;
  /**
   * Firma Ed25519 de eventos. Extensión OPCIONAL — ver specification/07-signing.md.
   * Traslada la autenticidad del canal al evento: un evento firmado sigue siendo
   * verificable dentro de un fichero, un backup o un correo, donde ya no hay ACL.
   */
  signing?: SigningOptions;
  /**
   * Destino de métricas. Los nombres y etiquetas los fija el protocolo
   * (08-observability.md), no la aplicación: si cada SDK nombrara a su manera, un
   * panel del ecosistema sería imposible.
   */
  metrics?: MetricsSink;
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
  /**
   * Credenciales NATS. NUNCA versionadas — 06-security.md §2. Un solo método: `.creds`
   * (ruta o contenido), nkey, token, o usuario y contraseña. Ver `Credentials`.
   *
   * Un valor mal formado, o una clave que no existe, lanza `InvalidCredentialsError` en
   * `connect()`: nunca se ignora.
   */
  credentials?: Credentials;
  /**
   * TLS contra el listener de NATS, con CA propia y certificado de cliente opcionales.
   * Obligatorio en producción — 06-security.md §2. Ver `TlsOptions`.
   */
  tls?: TlsOptions;
  /** Se invoca ante un POISON. Es el único caso que debe despertar a alguien. */
  onPoison?: (info: { subject: string; error: Error; raw: Uint8Array }) => void;
  /** Se invoca al enrutar cualquier evento a la DLQ. */
  onDlq?: (info: { subject: string; event: FluxEvent; classification: Classification }) => void;
  /**
   * Cada cuánto sondear `num_pending` de cada consumidor, en ms.
   *
   * | Valor | Significado |
   * |---|---|
   * | Positivo | Ese intervalo |
   * | `0` | El default (15 s), **no** "desactivado" |
   * | Negativo | Desactivado, explícitamente |
   *
   * Que el `0` sea el default y no "desactivado" viene de 08-observability.md §2.3: en
   * Go, Java y C# un entero sin asignar ES `0`, así que si desactivara, todo servicio
   * que no conozca este campo perdería la métrica en silencio. Desactivar algo debe
   * costar un gesto deliberado, y un negativo no se escribe sin querer.
   *
   * @default 15000
   */
  pendingPollMs?: number;
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
// Definidos en client-errors.ts (los usa también state.ts, sin ciclo) y re-exportados aquí.
export { ConsumerConfigMismatchError, TenantIsolationError };

// ─── Cliente ─────────────────────────────────────────────────────────────────

export class FluxBus {
  readonly #nc: NatsConnection;
  readonly #js: JetStreamClient;
  readonly #jsm: JetStreamManager;
  readonly #opts: ConnectOptions;
  readonly #classify: (e: unknown) => Classification;
  readonly #classifyDetailed: (e: unknown) => { classification: Classification; recognized: boolean };
  readonly #source: string;
  readonly #metrics: MetricsSink;
  readonly #subscriptions = new Set<{ stop: () => void }>();
  readonly #ensured = new Set<string>();
  #validate: Validator | null = null;
  #signer: Signer | null = null;
  #verifier: Verifier | null = null;
  /** Código del último fallo de firma, para que #dispatch sepa clasificar. */
  #pendingSignatureFailure: string | null = null;
  /**
   * Denegaciones de permisos que el broker notificó DE FORMA ASÍNCRONA, por subject
   * normalizado. Ver #watchPermissionViolations.
   */
  readonly #denied = new Map<string, "publish" | "subscribe">();
  /**
   * Quien espera una respuesta y quiere enterarse si el broker le deniega PUBLICAR en su subject, por subject EXACTO
   * (sin normalizar). Ver `#watchDenied`.
   */
  readonly #denialWaiters = new Map<string, Set<(subject: string) => void>>();

  /**
   * Estado replicado — la capacidad `state` (specification/10-state.md): `put`, `delete`, `get`
   * y `watch`. El valor ACTUAL de un agregado, que su dueño publica y los demás observan;
   * sustituye a `GET /clientes/123`, a la carga inicial y al polling de datos.
   */
  readonly state: StateClient;

  /**
   * Petición/respuesta — la capacidad `rpc` (specification/11-request-reply.md): `request` y `respond`.
   * Se expone como los métodos del bus (`bus.request`, `bus.respond`), no como un sub-objeto.
   */
  readonly #rpc: RpcClient;

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
    this.#classifyDetailed = createDetailedClassifier(opts.classifier);
    this.#source = sourceUri(opts.environment, opts.service);
    this.#metrics = opts.metrics ?? NO_METRICS;
    this.#metrics.connectionState(1);
    this.#watchPermissionViolations();
    this.state = new StateClient({
      nc,
      js,
      jsm,
      options: opts,
      source: this.#source,
      metrics: this.#metrics,
      classify: (e) => this.#classify(e),
      // Se resuelven en connect() (`initValidation`), después de construir el bus.
      validator: () => this.#validate,
      signer: () => this.#signer,
      verify: (event) => {
        this.#pendingSignatureFailure = null;
        this.#verifier?.check(event); // `require` lanza; `warn` solo anota el fallo
        const fallo = this.#pendingSignatureFailure;
        this.#pendingSignatureFailure = null;
        return fallo;
      },
      isDenied: (subject) => this.#denied.has(subject),
      clearDenied: (subject) => void this.#denied.delete(subject),
      track: (stop) => {
        const entry = { stop };
        this.#subscriptions.add(entry);
        return () => void this.#subscriptions.delete(entry);
      },
    });
    this.#rpc = new RpcClient({
      nc,
      options: opts,
      source: this.#source,
      metrics: this.#metrics,
      classify: (e) => this.#classifyDetailed(e),
      validator: () => this.#validate,
      signer: () => this.#signer,
      verify: (event) => {
        this.#pendingSignatureFailure = null;
        this.#verifier?.check(event); // `require` lanza; `warn` solo anota el fallo
        const fallo = this.#pendingSignatureFailure;
        this.#pendingSignatureFailure = null;
        return fallo;
      },
      watchDenied: (subject) => this.#watchDenied(subject),
      now: () => Date.now(),
    });
  }

  /**
   * Un `ack`, un WIP o la petición de mensajes de un pull NO son peticiones con
   * respuesta: son publicaciones sueltas. Si el broker las deniega no hay excepción
   * ni timeout que las delate; solo un `-ERR` asíncrono que el cliente reparte como
   * evento de estado (medido contra NATS 2.14.5).
   *
   * Consecuencia sin esta vigilancia: sin permiso de ack, cada evento se procesa bien y
   * se REENTREGA cada `ack_wait` para siempre, sin ningún error; sin permiso de pull, el
   * consumidor no recibe nada y parece sano. Dos fallos de configuración que se
   * manifiestan como «no pasa nada».
   */
  #watchPermissionViolations(): void {
    void (async () => {
      try {
        for await (const s of this.#nc.status()) {
          if (s.type !== "error") continue;
          const v = permissionViolation(s.error);
          if (!v) continue;
          // Quien espera una respuesta se entera ANTES de la deduplicación: cada petición denegada necesita su aviso.
          if (v.operation === "publish") for (const avisa of this.#denialWaiters.get(v.subject) ?? []) avisa(v.subject);
          const subject = normalizeDeniedSubject(v.subject);
          if (this.#denied.has(subject)) continue; // se avisa una vez por subject
          this.#denied.set(subject, v.operation);
          // Las peticiones denegadas ya lanzan su propia excepción; aquí importa el
          // efecto de las dos que NO lo hacen.
          const efecto = subject.startsWith("$JS.ACK.")
            ? " Sin permiso de ack, cada evento se procesa y se REENTREGA cada ack_wait, sin fin y sin error."
            : subject.startsWith("$JS.API.CONSUMER.MSG.NEXT.")
              ? " Sin permiso de pull, el consumidor no recibe nada y parece sano."
              : subject.startsWith("_INBOX.") && v.operation === "publish"
                ? " Es la RESPUESTA a una petición: sin `allow_responses` en su ACL el dueño no puede contestar, y quien llama solo" +
                  " verá un timeout (RPC_TIMEOUT). Acótalo: `{ max: 1, expires: 30s }` (11-request-reply.md §7.2)."
                : "";
          this.#opts.logger?.error(
            `[flux] el broker denegó ${v.operation === "publish" ? "publicar en" : "suscribirse a"} ` +
              `"${subject}" a "${this.#opts.service}": falta ese permiso en su ACL.${efecto} ` +
              `(06-security.md §3)`,
          );
        }
      } catch {
        // El iterador termina con la conexión; no hay nada que vigilar después.
      }
    })();
  }

  /**
   * Un aviso para quien espera una respuesta: se rechaza con `PERMISSION_DENIED` si el broker deniega PUBLICAR en `subject`.
   * Con `@nats-io/*` la propia petición ya falla en ~1 ms (A-66) y esto no hace falta; con una librería que no entrega la
   * denegación —`async-nats`, A-72— quien llama solo vería un timeout (RETRYABLE), y el handler que llama reintentaría
   * 51 minutos algo que nunca va a funcionar. El SDK DEBE escuchar el `-ERR` y competirlo con la respuesta (06-security.md
   * §3.7, 11-request-reply.md §7.1).
   */
  #watchDenied(subject: string): { promise: Promise<never>; cancel(): void } {
    let avisa: ((s: string) => void) | undefined;
    const promise = new Promise<never>((_, reject) => {
      avisa = (denegado) =>
        reject(
          new PermissionDeniedError(this.#opts.service, "publish", denegado, {
            hint:
              `Quien llama necesita permiso de PUBLICACIÓN sobre el subject EXACTO de cada operación concedida (nunca \`rpc.>\` ni ` +
              `\`rpc.<dominio>.>\`): es la clave \`calls\` del manifiesto de \`gen-acl.mjs\`. La petición se descartó y el broker ` +
              `lo avisó aparte (11-request-reply.md §7.1).`,
          }),
        );
    });
    // Si nadie compite con ella, no debe terminar como «rechazo sin atender».
    promise.catch(() => {});
    const waiters = this.#denialWaiters.get(subject) ?? new Set();
    this.#denialWaiters.set(subject, waiters);
    waiters.add(avisa!);
    return {
      promise,
      cancel: () => {
        waiters.delete(avisa!);
        if (waiters.size === 0 && this.#denialWaiters.get(subject) === waiters) this.#denialWaiters.delete(subject);
      },
    };
  }

  // ─── petición/respuesta ────────────────────────────────────────────────────

  /**
   * Una **orden** o un **cálculo** con respuesta inmediata a otro servicio (specification/11-request-reply.md). Resuelve con
   * el `data` de la respuesta; un error del que responde se lanza como `RetryableError` o `PermanentError` con su `code`,
   * de modo que un handler de eventos que llama a una operación hereda la clasificación sin hacer nada.
   *
   * ```ts
   * const r = await bus.request("clientes.cliente.v1.calcular-descuento",
   *   { clienteId: "cli-1", totalCents: 9990, moneda: "EUR" }, { timeoutMs: 2000, aggregateId: "cli-1" });
   * ```
   *
   * **Si lo que pides es un dato que otro sistema guarda, es estado** (`bus.state.get` / `watch`), no una petición.
   * `RPC_TIMEOUT` **no** dice que la operación no se ejecutó: quien responde deduplica por el `id` de la petición.
   */
  request<R = Record<string, unknown>>(operation: string, data: object, options?: RequestOptions): Promise<R> {
    return this.#rpc.request<R>(operation, data, options);
  }

  /**
   * Atiende una operación: una suscripción al subject EXACTO, con el nombre del servicio como grupo de cola (dos instancias se
   * reparten las peticiones). El handler recibe `(petición, ctx)` y devuelve el resultado; lanzar `PermanentError` o
   * `RetryableError` responde un error. **Deduplica por `ctx.id`**: una orden puede llegar dos veces.
   *
   * Falla con `PERMISSION_DENIED` si la ACL no concede la suscripción: un dueño sin permiso parecería un servicio caído.
   * Devuelve un `Responder`; `stop()` para de forma ordenada (`bus.close()` también).
   */
  respond<Req = Record<string, unknown>, Res extends object = Record<string, unknown>>(
    operation: string,
    handler: RpcHandler<Req, Res>,
    options?: RespondOptions,
  ): Promise<Responder> {
    return this.#rpc.respond<Req, Res>(operation, handler, options);
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

    // L3: validar ANTES de publicar. Un payload que viola su contrato debe fallar
    // aquí, en el servicio que lo generó, y no aparecer como un misterio en un
    // consumidor de otro equipo la semana que viene — 00-protocol.md §5.
    try {
      this.#validate?.(event, subject);
    } catch (e) {
      // Se anota ANTES de propagar. Un `publish()` que lanza sin dejar rastro en las
      // métricas hace invisible el volumen de contratos rotos: la aplicación ve una
      // excepción, pero el panel del ecosistema no ve nada y nadie sabe cuántos
      // productores están fallando ni desde cuándo — 08-observability.md §2.1.
      this.#metrics.eventPublished(subject, "invalid_schema");
      throw e;
    }

    // Firmar es lo ULTIMO antes de serializar: la firma cubre el envelope completo,
    // así que cualquier atributo añadido después la invalidaría — 07-signing.md §5.
    const publicado = this.#signer ? this.#signer.sign(event) : event;

    // msgID pone la cabecera Nats-Msg-Id: deduplica reintentos de PUBLICACIÓN dentro
    // de duplicate_window. NO deduplica reentregas de consumo — 03-delivery.md §3.
    try {
      await this.#js.publish(subject, serialize(publicado), { msgID: publicado.id });
    } catch (e) {
      // Un publish a un dominio ajeno lo deniega el broker, no este código — 06-security.md
      // §3. Sin envolverlo llegaría como un `RequestError` genérico, que el clasificador
      // trata como DESCONOCIDO y reintenta; una denegación es PERMANENT, como un 403.
      throw this.#asPermissionDenied(e) ?? e;
    }
    this.#metrics.eventPublished(subject, "ok");
    return publicado as FluxEvent<T>;
  }

  async #traceparent(): Promise<{ traceparent?: string }> {
    const tp = await activeTraceparent();
    return tp ? { traceparent: tp } : {};
  }

  /** El tenant de la conexión, salvo "system", que no es un tenant sino su ausencia. */
  #tenantFilter(): string | undefined {
    const t = this.#opts.tenantId;
    return t && t !== "system" ? t : undefined;
  }

  #schemaFor(subject: string): string {
    const explicit = this.#opts.schemas?.[subject];
    if (explicit) return explicit;

    // El bundle L3 conoce el MINOR real de cada subject: dentro de un mayor todo es
    // BACKWARD-compatible, así que el más alto acepta lo que aceptan los anteriores.
    const fromBundle = this.#opts.validation?.bundle
      ? schemaUriFor(this.#opts.validation.bundle, subject)
      : undefined;
    if (fromBundle) return fromBundle;

    const base = this.#opts.schemaBaseUrl;
    if (!base) {
      throw new Error(
        `no hay dataschema para "${subject}". Declara \`schemas["${subject}"]\`, ` +
          `\`schemaBaseUrl\`, o pasa un bundle en \`validation.bundle\` (connect()).`,
      );
    }
    const { domain, aggregate, event, major } = parseSubject(subject);
    // Sin bundle ni mapa explícito solo se puede asumir el .0.0 del mayor. Es
    // suficiente para L2 —el atributo es informativo— pero no para L3.
    return `${base.replace(/\/$/, "")}/${domain}/${aggregate}/${event}/${major}.0.0.json`;
  }

  // ─── subscribe ─────────────────────────────────────────────────────────────

  async subscribe<T = unknown>(
    subject: string,
    handler: Handler<T>,
    options: SubscribeOptions = {},
  ): Promise<Subscription> {
    const { domain } = parseSubject(subject);

    // El filtro efectivo: el de la suscripción gana sobre el de la conexión.
    const tenantFilter = options.tenantId ?? this.#tenantFilter();
    if (this.#opts.tenantIsolation === "strict" && !tenantFilter) {
      throw new TenantIsolationError(
        subject,
        "no hay tenantId ni en connect() ni en subscribe()",
      );
    }
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

    const effective = await this.#ensureConsumer(stream, durable, requested);

    this.#assertConfigHonored(
      durable,
      requested as unknown as Record<string, unknown>,
      effective as unknown as Record<string, unknown>,
    );

    const consumer = await this.#js.consumers.get(stream, durable);
    // Una denegación de un intento anterior sobre este durable no debe condenar a este.
    const pull = `$JS.API.CONSUMER.MSG.NEXT.${stream}.${durable}`;
    this.#denied.delete(pull);
    const messages = await consumer.consume();

    // Sondeo de num_pending. Sin esto, `flux_consumer_pending` nunca se emite y el
    // panel muestra "sin datos", que es indistinguible de "consumidor sano" — 08 §4.
    const configurado = this.#opts.pendingPollMs ?? 0;
    // 0 (o ausente) → default; negativo → desactivado — 08-observability.md §2.3.
    const pollMs = configurado === 0 ? 15_000 : configurado;
    const poll =
      pollMs > 0 && this.#opts.metrics
        ? setInterval(() => {
            void this.#jsm.consumers
              .info(stream, durable)
              .then((i) => this.#metrics.consumerPending(subject, durable, i.num_pending))
              // Un fallo del sondeo NO debe afectar al consumo: es telemetría.
              .catch(() => {});
          }, pollMs)
        : null;
    if (poll?.unref) poll.unref();

    let stopped = false;
    const entry = {
      stop: () => {
        stopped = true;
        if (poll) clearInterval(poll);
        messages.stop();
      },
    };
    this.#subscriptions.add(entry);

    void (async () => {
      for await (const m of messages) {
        if (stopped) break;
        try {
          await this.#dispatch(m, subject, durable, handler as Handler, tenantFilter);
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

    // La petición de mensajes es un publish sin respuesta: si el broker la deniega, no
    // hay excepción y el consumidor se queda inerte, aparentemente sano. Es mejor fallar
    // aquí que descubrirlo por la ausencia de eventos semanas después.
    //
    // Se comprueba DESPUÉS de arrancar el bucle porque la librería encola el primer pull
    // y no lo publica hasta que alguien itera (medido). Un turno de event loop deja que
    // salga, y el `-ERR` llega antes que el PONG de este flush —el protocolo es
    // ordenado—, así que ya está registrado cuando vuelve.
    await new Promise<void>((r) => setImmediate(r));
    await this.#nc.flush();
    // El estado llega al vigilante por un iterador asíncrono: tras el `-ERR` aún le faltan
    // unos microturnos para anotarlo, y un turno más de event loop los agota todos.
    await new Promise<void>((r) => setImmediate(r));
    if (this.#denied.has(pull)) {
      entry.stop();
      this.#subscriptions.delete(entry);
      throw new PermissionDeniedError(this.#opts.service, "publish", pull);
    }

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
   * Toma el consumidor durable si existe y, solo si no existe, lo crea.
   *
   * Se pregunta ANTES de crear: en producción lo aprovisiona `flux provision`, y
   * entonces el SDK no necesita permiso de creación. Intentar crear primero provocaba
   * una denegación espuria en cada arranque y obligaba a conceder `CONSUMER.CREATE` a
   * quien no lo usa nunca. Que el consumidor ya exista con otra config no es un error
   * de aquí: lo decide `#assertConfigHonored`.
   *
   * Si no existe y crearlo está DENEGADO, no es un fallo de red ni un timeout: es que
   * nadie lo aprovisionó — `NotProvisionedError`.
   */
  async #ensureConsumer(
    stream: string,
    durable: string,
    requested: Partial<ConsumerConfig>,
  ): Promise<ConsumerConfig> {
    try {
      return (await this.#jsm.consumers.info(stream, durable)).config;
    } catch (infoError) {
      if (!isNotFound(infoError, "consumer")) {
        throw this.#asPermissionDenied(infoError) ?? infoError;
      }
    }
    try {
      return (await this.#jsm.consumers.add(stream, requested)).config;
    } catch (addError) {
      const denied = permissionViolation(addError);
      if (denied) {
        throw new NotProvisionedError(
          { kind: "consumer", name: durable, stream },
          this.#opts.service,
          denied.subject,
          { cause: addError },
        );
      }
      // Otra instancia del servicio pudo crearlo entre la pregunta y la creación.
      try {
        return (await this.#jsm.consumers.info(stream, durable)).config;
      } catch {
        throw addError;
      }
    }
  }

  /** Convierte una denegación de la librería en el error accionable del SDK. */
  #asPermissionDenied(e: unknown): PermissionDeniedError | undefined {
    const v = permissionViolation(e);
    return v
      ? new PermissionDeniedError(this.#opts.service, v.operation, v.subject, { cause: e })
      : undefined;
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
    tenantFilter: string | undefined,
  ): Promise<void> {
    // `deliveryCount` empieza en 1 en la primera entrega, no en 0.
    const attempt = m.info.deliveryCount;

    // Los metadatos traen `pending` gratis en cada entrega: más fresco que el sondeo y
    // sin coste. Pero NO sustituye al sondeo — si el bucle muere, dejan de llegar
    // mensajes y el gauge se quedaría plano en vez de crecer, que es exactamente lo
    // contrario de la señal que hace falta (08-observability.md §2.3).
    if (typeof m.info.pending === "number") {
      this.#metrics.consumerPending(subject, durable, m.info.pending);
    }
    const maxAttempts = CONSUMER_DEFAULTS.maxDeliver;

    // POISON se detecta antes del handler: el mensaje no es interpretable, así que
    // el handler nunca llega a verlo — 04-errors.md §1.3.
    let event: FluxEvent;
    try {
      event = parseEvent(m.data);
    } catch (e) {
      const err = e instanceof PoisonError ? e : new PoisonError(String(e));
      this.#opts.onPoison?.({ subject, error: err, raw: m.data });
      this.#metrics.eventConsumed(subject, durable, "poison");
      this.#metrics.eventDlq(subject, durable, "poison", err.opts.code ?? "POISON");
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

    // Filtrar ANTES del handler: un evento de otro tenant no es un fallo, no es para
    // nosotros. Se confirma y se descarta — 09-multitenancy.md §3.
    if (tenantFilter && event.tenantid !== tenantFilter) {
      m.ack();
      return;
    }

    // WIP mientras el handler vive: extiende ack_wait y evita que el mismo evento se
    // ejecute en concurrencia consigo mismo — 03-delivery.md §2.1.
    const inicio = Date.now();
    const wip = setInterval(() => {
      try {
        m.working();
      } catch {
        /* el mensaje ya está resuelto */
      }
    }, CONSUMER_DEFAULTS.ackWaitMs / 2);

    try {
      // L3 en consumo: el evento es sintácticamente válido pero incumple su
      // contrato. Reintentarlo dará exactamente el mismo resultado, así que la
      // clasificación correcta es PERMANENT — 04-errors.md §1.2.
      // La firma se comprueba ANTES que el esquema: si el evento fue manipulado, su
      // payload puede validar perfectamente y aun así no ser del productor que dice.
      this.#pendingSignatureFailure = null;
      this.#verifier?.check(event);
      if (this.#pendingSignatureFailure) {
        // Modo `warn`: check() no lanzó, pero el evento habría muerto en `require`.
        this.#metrics.eventConsumed(subject, durable, "invalid_signature");
        this.#pendingSignatureFailure = null;
      }
      if (this.#opts.validation?.onConsume) this.#validate?.(event, subject);
      await runWithContext(contextFromEvent(event), async () =>
        handler(event, {
          ack: () => {},
          attempt,
          maxAttempts,
          subject,
        }),
      );
      m.ack();
      this.#metrics.eventConsumed(subject, durable, "ok");
      this.#metrics.handlerDuration(subject, durable, (Date.now() - inicio) / 1000);
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
        this.#metrics.eventRetried(subject, durable, attempt);
        m.nak(delay);
        return;
      }

      const reason =
        c.class === ErrorClass.POISON
          ? "poison"
          : c.class === ErrorClass.RETRYABLE
            ? "retryable" // agotó los reintentos
            : "permanent";

      // El `outcome` de la métrica y el `dlqreason` del evento NO son lo mismo cuando
      // muere por firma: en la DLQ es `poison` (el evento no es interpretable como
      // legítimo), pero la métrica debe decir `invalid_signature`. "Un productor
      // publica basura" y "alguien publica eventos que no son suyos" son dos
      // preguntas distintas — 07-signing.md §7.2.
      const outcome = this.#pendingSignatureFailure
        ? "invalid_signature"
        // Un fallo de esquema tiene outcome propio: "el productor rompio el
        // contrato" es otra pregunta que "la regla de negocio rechazo el evento",
        // aunque ambos sean PERMANENT — 08-observability.md §2.1.
        : c.code === "INVALID_SCHEMA" || c.code === "SCHEMA_NOT_FOUND"
          ? "invalid_schema"
          : reason;
      this.#pendingSignatureFailure = null;

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

      this.#metrics.eventConsumed(subject, durable, outcome);
      this.#metrics.eventDlq(subject, durable, reason, c.code);
      this.#metrics.handlerDuration(subject, durable, (Date.now() - inicio) / 1000);
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
      return;
    } catch (infoError) {
      // Solo «no existe» autoriza a crearlo. Antes cualquier fallo de `info` —un permiso
      // denegado, un JetStream apagado— caía en `add`, y el error que llegaba era el de
      // crear, que no explicaba nada de lo que había pasado de verdad.
      if (!isNotFound(infoError, "stream")) {
        throw this.#asPermissionDenied(infoError) ?? infoError;
      }
    }
    try {
      await this.#jsm.streams.add({
        name,
        subjects,
        storage: StorageType.File,
        retention: RetentionPolicy.Limits,
        discard: DiscardPolicy.Old,
        num_replicas: this.#opts.streamReplicas ?? 1,
        ...extra,
      });
    } catch (addError) {
      const denied = permissionViolation(addError);
      if (denied) {
        throw new NotProvisionedError(
          { kind: "stream", name },
          this.#opts.service,
          denied.subject,
          { cause: addError },
        );
      }
      throw addError;
    }
  }

  /** Lo llama connect(). Público solo porque connect() vive fuera de la clase. */
  async initValidation(): Promise<void> {
    this.#validate = await createValidator(this.#opts.validation ?? {});
    this.#signer = createSigner(this.#opts.signing ?? {});
    // Se envuelve para que el fallo de firma cuente como `invalid_signature` y no
    // como `poison`: son dos preguntas distintas —"un productor publica basura" vs
    // "alguien publica eventos que no son suyos"— y mezclarlas hace que la misma
    // consulta mida cosas distintas según el lenguaje. Ver 07-signing.md §7.2.
    this.#verifier = createVerifier({
      ...(this.#opts.signing ?? {}),
      onFailure: (info) => {
        this.#pendingSignatureFailure = info.code;
        this.#opts.signing?.onFailure?.(info);
      },
    });
  }

  // ─── ciclo de vida ─────────────────────────────────────────────────────────

  get connected(): boolean {
    return !this.#nc.isClosed();
  }

  async close(): Promise<void> {
    // Primero los responders: dejar de escuchar, esperar a lo que está en vuelo y SOLO entonces cerrar. Un `drain()` de la
    // conexión con un handler en vuelo pierde su respuesta (la publicación lanza `closed connection`: A-62, A-83).
    await this.#rpc.stopAll();
    for (const s of this.#subscriptions) s.stop();
    this.#subscriptions.clear();
    this.#metrics.connectionState(0);
    await this.#nc.drain();
  }
}

/**
 * Las opciones que `connect()` le pasa a la librería de NATS. Todo lo que se puede comprobar sin red se comprueba
 * AQUÍ, ANTES de dialar: un valor mal formado debe fallar con su nombre, y no degradarse a «sin credenciales» o a «sin
 * TLS» —que es lo que pasaba con `credentials.creds`—.
 *
 * Aparte de `connect()` para que se pueda comprobar QUÉ pide la conexión: la petición/respuesta necesita que la librería
 * anuncie `headers` y `no_responders` en CONNECT, y sin ellos `RPC_UNAVAILABLE` es un timeout (A-57). La librería los
 * pone sola cuando el servidor dice `headers: true` en su INFO, y no hay opción para quitarlos (A-79).
 */
export function natsOptionsFor(options: ConnectOptions): ConnectionOptions {
  const inboxPrefix = inboxPrefixFor(options.service);
  const authenticator = buildAuthenticator(options.credentials);
  const tls = buildTls(options.tls);

  return {
    servers: options.servers,
    name: `${options.service}@${options.environment}`,
    maxReconnectAttempts: -1,
    reconnectTimeWait: 1000,
    reconnectJitter: 500,
    reconnectJitterTLS: 1000,
    // Inbox PRIVADO, siempre y no configurable: ver inboxPrefixFor().
    inboxPrefix,
    ...(authenticator && { authenticator }),
    ...(tls && { tls }),
  };
}

/**
 * Conecta al bus. Reconecta indefinidamente con backoff y jitter: sin jitter, mil
 * servicios reconectan en el mismo milisegundo y tiran el cluster que acaba de
 * levantarse — 03-delivery.md §6.
 */
export async function connect(options: ConnectOptions): Promise<FluxBus> {
  const nc = await connectWhenAvailable(natsOptionsFor(options));
  try {
    const jsm = await jetstreamManager(nc);
    const js = jetstream(nc);
    const bus = new FluxBus(nc, js, jsm, options);
    // Compilar los esquemas UNA vez en connect(), no por evento: ajv compila a
    // función y hacerlo en la ruta caliente tiraría el throughput.
    await bus.initValidation();
    return bus;
  } catch (e) {
    // `jetstreamManager` consulta `$JS.API.INFO`. Con una ACL anterior a la v1.1 el
    // broker lo deniega, y sin este cierre la conexión queda abierta tras el fallo.
    await nc.close().catch(() => {});
    const v = permissionViolation(e);
    throw v ? new PermissionDeniedError(options.service, v.operation, v.subject, { cause: e }) : e;
  }
}

/**
 * Espera a que el broker esté disponible —un servicio que arranca antes que NATS es lo
 * normal en un compose— pero NO espera lo que no va a cambiar.
 *
 * `waitOnFirstConnect: true` de la librería reintenta cualquier error, sin límite y sin
 * avisar. Medido contra NATS 2.14.5: con un certificado de servidor que no verifica,
 * `connect()` no falla, se cuelga. Aquí el bucle es nuestro para poder distinguir.
 * Las reconexiones posteriores siguen siendo de la librería (`maxReconnectAttempts: -1`).
 */
async function connectWhenAvailable(natsOptions: ConnectionOptions): Promise<NatsConnection> {
  const wait = natsOptions.reconnectTimeWait ?? 1000;
  const jitter = natsOptions.reconnectJitter ?? 0;
  for (;;) {
    try {
      return await natsConnect({ ...natsOptions, waitOnFirstConnect: false });
    } catch (e) {
      const permanent = permanentConnectError(e);
      if (permanent) throw permanent;
      // Con jitter: mil servicios que esperan a un NATS que arranca no deben llamar a
      // la vez — 03-delivery.md §6.
      await new Promise((r) => setTimeout(r, wait + Math.random() * jitter));
    }
  }
}
