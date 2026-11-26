/**
 * Petición/respuesta — la extensión opcional `rpc`.
 * Contrato normativo: specification/11-request-reply.md, specification/04-errors.md §7.
 *
 * Un EVENTO dice qué pasó; el ESTADO es el dato; la petición/respuesta es **la orden que necesita
 * respuesta ya** y el **cálculo**. Core NATS, sin stream ni DLQ: si nadie escucha, quien llama lo sabe en
 * milisegundos. Este fichero tiene dos mitades que conviene no mezclar:
 *
 *   1. **Funciones puras y deterministas** (sin reloj, sin UUID, sin red): el subject, los tipos y el sobre
 *      de la petición, de la respuesta y del error. Producen los MISMOS BYTES en todos los SDKs
 *      (11-request-reply.md §3.5; arnés cruzado `rpc-request`, `rpc-reply`, `rpc-error`, `rpc-operation` y
 *      `rpc-timeout`). Lo que las alimenta —`id`, `time`— se pasa DE FUERA: si la función los inventara,
 *      dos ejecuciones no serían comparables.
 *   2. **`RpcClient`** (`bus.request` y `bus.respond`), que las usa y habla con el broker.
 *
 * Regla de diseño heredada de client.ts: ningún tipo de NATS en la API pública. `RpcHost` sí los lleva,
 * pero es interno y no se exporta desde index.ts.
 */

import type { Msg, NatsConnection, Subscription as NatsSubscription } from "@nats-io/transport-node";
import { v7 as uuidv7 } from "uuid";

import type { ConnectOptions } from "./client.js";
import { contextFromEvent, currentContext, runWithContext, activeTraceparent } from "./context.js";
import { parseEvent, type DataClassification, type FluxEvent } from "./envelope.js";
import {
  ErrorClass,
  PermanentError,
  PoisonError,
  RetryableError,
  type Classification,
} from "./errors.js";
import type { MetricsSink, RpcClientOutcome, RpcServerOutcome } from "./metrics.js";
import { MAX_MESSAGE_BYTES, assertNotReservedDomain } from "./protocol.js";
import {
  InvalidRpcCallError,
  RpcInvalidReplyError,
  RpcMessageTooLargeError,
  RpcPermanentError,
  RpcRetryableError,
  RpcTimeoutError,
  RpcUnavailableError,
  StaleRequestError,
} from "./rpc-errors.js";
import {
  PermissionDeniedError,
  isNoResponders,
  permissionViolation,
} from "./security.js";
import type { Signer } from "./signing.js";
import { InvalidDataClassificationError } from "./state-errors.js";
import {
  SchemaNotFoundError,
  SchemaValidationError,
  rpcSchemaUriFor,
  type Validator,
} from "./validation.js";

// ═════════════════════════════════════════════════════════════════════════════════════════
// 1. Constantes — protocol.json → `rpc`
// ═════════════════════════════════════════════════════════════════════════════════════════

/** `rpc.timeout`: 5 s por defecto. */
export const RPC_TIMEOUT_DEFAULT_MS = 5_000;
/** `rpc.timeout.minMs`. */
export const RPC_TIMEOUT_MIN_MS = 1;
/**
 * `rpc.timeout.maxMs`: 25 s, **por debajo del `ack_wait` de 30 s** (03-delivery.md §2): para que una llamada
 * hecha desde un handler de eventos no provoque su reentrega mientras espera.
 */
export const RPC_TIMEOUT_MAX_MS = 25_000;
/** `rpc.signing.staleRequestWindowSeconds`: la ventana simétrica de `STALE_REQUEST` (solo con la firma en `require`). */
export const RPC_STALE_WINDOW_SECONDS = 300;
/** Handlers concurrentes por operación: el mismo orden que `max_ack_pending` (11-request-reply.md §5.6). */
export const RPC_MAX_CONCURRENT_HANDLERS = 256;
/**
 * Cuánto espera `stop()` a los handlers en vuelo: el timeout máximo (11-request-reply.md §5.5). Una petición
 * más lenta ya la dio por perdida quien llamó.
 */
export const RPC_STOP_TIMEOUT_MS = RPC_TIMEOUT_MAX_MS;
/**
 * Margen tras dejar de escuchar y antes de cerrar (11-request-reply.md §5.5, «No medido»): en un cluster el
 * interés se propaga de forma asíncrona, y la petición que sale justo cuando se va el último puede agotar su
 * timeout en vez de recibir `no responders` (A-70: 4 de 10 sin esperar, 0 de 10 a los 5 ms).
 */
export const RPC_STOP_MARGIN_MS = 25;
/** Pausa entre los intentos de un `retries` tras `RPC_UNAVAILABLE` (que falla al instante): no es un temporizador de la spec. */
const RETRY_PAUSE_MS = 50;

/** `rpc.type.error`: el mismo tipo para toda operación. */
export const RPC_ERROR_TYPE = "com.flux.rpc.error.v1";
/** `rpc.type.error`'s `dataschema`: fijo. Su esquema está en `schemas/system/rpc/error/1.0.0.json`. */
export const RPC_ERROR_DATASCHEMA = "https://schemas.internal/system/rpc/error/1.0.0.json";
/** Base de los esquemas del repo: `schemas/rpc/<dom>/<agr>/<op>/{request,reply}/…` cuelga de aquí. */
export const RPC_SCHEMA_BASE = "https://schemas.internal";
/**
 * `rpc.operation.pattern`: el nombre de la operación —el subject sin `rpc.`— tiene la MISMA forma sintáctica
 * que el subject de un evento. Coincidencia COMPLETA: en JS el `$` sin el modificador `m` no acepta un `\n` final,
 * a diferencia de Python y PHP, donde `"…obtener\n"` pasaría el patrón (11-request-reply.md §2.2).
 */
export const RPC_OPERATION_PATTERN =
  /^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]*\.[a-z0-9]+(-[a-z0-9]+)*$/;
/** `errors.codePattern` y el `pattern` del esquema del error. */
export const RPC_ERROR_CODE_PATTERN = /^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*$/;
const RPC_ERROR_CODE_MAX = 64;
const RPC_ERROR_MESSAGE_MAX = 1000;
const CLASSIFICATIONS: readonly DataClassification[] = ["public", "internal", "confidential", "restricted"];

// ═════════════════════════════════════════════════════════════════════════════════════════
// 2. Validación — se hace SIEMPRE, antes de abrir ninguna petición
// ═════════════════════════════════════════════════════════════════════════════════════════

export interface ParsedRpcOperation {
  /** `clientes.cliente.v1.calcular-descuento`: lo que reciben `request` y `respond`. */
  readonly operation: string;
  readonly domain: string;
  readonly aggregate: string;
  readonly major: number;
  /** `calcular-descuento`: el último token, en infinitivo. */
  readonly name: string;
}

/**
 * Valida el nombre de una operación. Primero el dominio reservado (`RESERVED_DOMAIN`: también `rpc.` puesto por
 * quien llama), después la forma (`INVALID_RPC_CALL`). Siempre antes de tocar la red.
 */
export function parseRpcOperation(operation: unknown): ParsedRpcOperation {
  if (typeof operation !== "string") {
    throw new InvalidRpcCallError(
      `la operación debe ser una cadena \`<dominio>.<agregado>.v<major>.<operacion>\`, y llegó ` +
        `${operation === null ? "null" : typeof operation}.`,
    );
  }
  assertNotReservedDomain(operation, "operación");
  if (!RPC_OPERATION_PATTERN.test(operation)) {
    const tokens = operation.split(".");
    const n = tokens.length;
    const porQue =
      operation !== operation.toLowerCase()
        ? "debe ir todo en minúsculas —NATS distingue mayúsculas, y una errata crea un subject al que nadie está suscrito, sin error—"
        : /[*>]/.test(operation)
          ? "no admite comodines (`*`, `>`): el broker los aceptaría, y es una operación concreta la que se llama o se atiende"
          : n !== 4
            ? `debe tener exactamente 4 tokens, tiene ${n}`
            : "la versión es `v` y un entero ≥ 1, y cada token va en kebab-case (letras minúsculas, dígitos y guiones)";
    throw new InvalidRpcCallError(
      `la operación ${JSON.stringify(operation.length > 90 ? `${operation.slice(0, 87)}…` : operation)} no es válida: ` +
        `${porQue}. El nombre es \`<dominio>.<agregado>.v<major>.<operacion>\` —el subject sin \`rpc.\`—, con la ` +
        `operación en infinitivo y kebab-case: \`clientes.cliente.v1.calcular-descuento\` (11-request-reply.md §2.2).`,
    );
  }
  const [domain, aggregate, v, name] = operation.split(".");
  return { operation, domain, aggregate, major: Number(v.slice(1)), name };
}

/** El `timeoutMs` efectivo: `undefined` → 5000; un entero entre 1 y 25 000; cualquier otra cosa, `INVALID_RPC_CALL` (no se recorta). */
export function resolveRpcTimeout(timeoutMs: unknown): number {
  if (timeoutMs === undefined) return RPC_TIMEOUT_DEFAULT_MS;
  if (
    typeof timeoutMs !== "number" ||
    !Number.isInteger(timeoutMs) ||
    timeoutMs < RPC_TIMEOUT_MIN_MS ||
    timeoutMs > RPC_TIMEOUT_MAX_MS
  ) {
    throw new InvalidRpcCallError(
      `\`timeoutMs\` debe ser un entero entre ${RPC_TIMEOUT_MIN_MS} y ${RPC_TIMEOUT_MAX_MS} (milisegundos; ` +
        `${RPC_TIMEOUT_DEFAULT_MS} por defecto) y llegó ${
          typeof timeoutMs === "number" ? timeoutMs : timeoutMs === null ? "null" : `un valor de tipo ${typeof timeoutMs}`
        }. No se recorta en silencio: un valor fuera de rango es un error. El máximo está por debajo del ` +
        `\`ack_wait\` de 30 s para que una llamada hecha desde un handler de eventos no provoque su reentrega ` +
        `(11-request-reply.md §4.2).`,
    );
  }
  return timeoutMs;
}

function assertObject(data: unknown, que: string): asserts data is Record<string, unknown> {
  if (data === null || typeof data !== "object" || Array.isArray(data)) {
    throw new InvalidRpcCallError(
      `${que} debe ser un objeto JSON en la raíz (ni array ni escalar), para poder añadir campos sin romper el ` +
        `contrato (01-envelope.md §4); llegó ${data === null ? "null" : Array.isArray(data) ? "un array" : `un valor de tipo ${typeof data}`}.`,
    );
  }
}

function assertClassification(c: unknown): DataClassification {
  if (!CLASSIFICATIONS.includes(c as DataClassification)) throw new InvalidDataClassificationError(c, "rpc");
  return c as DataClassification;
}

// ═════════════════════════════════════════════════════════════════════════════════════════
// 3. Nombres — 11-request-reply.md §2.2, §3
// ═════════════════════════════════════════════════════════════════════════════════════════

/** `rpc.clientes.cliente.v1.calcular-descuento`: el subject de NATS. Cinco tokens. **No** es el atributo `subject` del sobre. */
export function rpcSubject(operation: string): string {
  return `rpc.${parseRpcOperation(operation).operation}`;
}

/** `com.flux.rpc.<dominio>.<agregado>.<operacion>.request.v<major>`. */
export function rpcRequestType(operation: string): string {
  const { domain, aggregate, name, major } = parseRpcOperation(operation);
  return `com.flux.rpc.${domain}.${aggregate}.${name}.request.v${major}`;
}

/** `com.flux.rpc.<dominio>.<agregado>.<operacion>.reply.v<major>`. */
export function rpcReplyType(operation: string): string {
  const { domain, aggregate, name, major } = parseRpcOperation(operation);
  return `com.flux.rpc.${domain}.${aggregate}.${name}.reply.v${major}`;
}

/**
 * `https://schemas.internal/rpc/<dominio>/<agregado>/<operacion>/{request,reply}/<major>.0.0.json` (o con otra
 * base): lo que se puede asumir SIN bundle. El bundle conoce el MINOR real y lo resuelve `rpcSchemaUriFor`.
 */
export function rpcDataschema(operation: string, lado: "request" | "reply", base: string = RPC_SCHEMA_BASE): string {
  const { domain, aggregate, name, major } = parseRpcOperation(operation);
  return `${base.replace(/\/$/, "")}/rpc/${domain}/${aggregate}/${name}/${lado}/${major}.0.0.json`;
}

// ═════════════════════════════════════════════════════════════════════════════════════════
// 4. El sobre — funciones PURAS (11-request-reply.md §3)
// ═════════════════════════════════════════════════════════════════════════════════════════

export interface RpcRequestInput<T extends object = Record<string, unknown>> {
  /** `clientes.cliente.v1.calcular-descuento`. */
  operation: string;
  tenantId: string;
  /** Va al atributo `subject` del sobre (y a `partitionkey`): la identidad del agregado. **No** es el subject de NATS. */
  aggregateId?: string;
  id: string;
  source: string;
  /** ISO-8601 con exactamente 3 decimales y sufijo `Z`. */
  time: string;
  /** El del evento o la petición en curso; si la llamada no nace de otro, **el `id` propio**. */
  correlationid: string;
  /** El `id` del evento o la petición en curso; ausente si no hay. */
  causationid?: string;
  producerversion: string;
  classification: DataClassification;
  data: T;
  /** Por defecto, `https://schemas.internal/rpc/<dom>/<agr>/<op>/request/<major>.0.0.json`. */
  dataschema?: string;
  traceparent?: string;
  tracestate?: string;
}

/**
 * El sobre de una petición: un CloudEvent normal con `type` `com.flux.rpc.….request.v<M>`, `subject` y
 * `partitionkey` = `aggregateId` si se da, y `data` el ÚLTIMO atributo.
 *
 * **Pura y determinista**: `id` y `time` los da quien llama. Valida lo mismo que `request` (§4.1, paso 0,
 * salvo L3), así que lo que construye es una petición legal.
 */
export function buildRpcRequest<T extends object>(input: RpcRequestInput<T>): FluxEvent<T> {
  const p = parseRpcOperation(input.operation);
  assertObject(input.data, "`data`");
  const classification = assertClassification(input.classification);
  return {
    specversion: "1.0",
    id: input.id,
    source: input.source,
    type: rpcRequestType(p.operation),
    time: input.time,
    datacontenttype: "application/json",
    dataschema: input.dataschema ?? rpcDataschema(p.operation, "request"),
    ...(input.aggregateId !== undefined && { subject: input.aggregateId }),
    correlationid: input.correlationid,
    tenantid: input.tenantId,
    producerversion: input.producerversion,
    dataclassification: classification,
    ...(input.causationid !== undefined && { causationid: input.causationid }),
    ...(input.aggregateId !== undefined && { partitionkey: input.aggregateId }),
    ...(input.traceparent !== undefined && { traceparent: input.traceparent }),
    ...(input.tracestate !== undefined && { tracestate: input.tracestate }),
    data: input.data,
  };
}

/** Lo que la respuesta HEREDA de la petición. Es el arnés quien lo da plano; el SDK lo saca del sobre recibido. */
export interface RpcRequestRef {
  operation: string;
  id: string;
  correlationid: string;
  tenantid: string;
  subject?: string;
  /** Si la petición lo traía; si no, el `subject`, que es la misma convención que en los eventos. */
  partitionkey?: string;
}

interface RpcReplyBase {
  request: RpcRequestRef;
  id: string;
  source: string;
  time: string;
  producerversion: string;
  /** El que responde: «de la misma traza» que la petición, con su propio span. */
  traceparent?: string;
}

export interface RpcReplyInput<T extends object = Record<string, unknown>> extends RpcReplyBase {
  /** La de la operación; si no, la de la conexión. */
  classification: DataClassification;
  data: T;
  dataschema?: string;
}

/** `partitionkey` de la respuesta: el de la petición, o su `subject`: la convención de los eventos (`partitionkey ?? aggregateId`). */
const particion = (r: RpcRequestRef): string | undefined => r.partitionkey ?? r.subject;

/**
 * El sobre de una respuesta: **hereda** `correlationid`, `tenantid`, `subject` y `partitionkey` de la petición, y
 * su `causationid` es el `id` de la petición. `id` NUEVO: nunca el de la petición. `source`, `producerversion` y
 * `traceparent` son del que responde.
 */
export function buildRpcReply<T extends object>(input: RpcReplyInput<T>): FluxEvent<T> {
  const p = parseRpcOperation(input.request.operation);
  assertObject(input.data, "la respuesta");
  const classification = assertClassification(input.classification);
  const key = particion(input.request);
  return {
    specversion: "1.0",
    id: input.id,
    source: input.source,
    type: rpcReplyType(p.operation),
    time: input.time,
    datacontenttype: "application/json",
    dataschema: input.dataschema ?? rpcDataschema(p.operation, "reply"),
    ...(input.request.subject !== undefined && { subject: input.request.subject }),
    correlationid: input.request.correlationid,
    tenantid: input.request.tenantid,
    producerversion: input.producerversion,
    dataclassification: classification,
    causationid: input.request.id,
    ...(key !== undefined && { partitionkey: key }),
    ...(input.traceparent !== undefined && { traceparent: input.traceparent }),
    data: input.data,
  };
}

/** `data` de `com.flux.rpc.error.v1`: `class` binaria (`poison` no viaja), `code` estable, `message` y `retryAfterMs`. */
export interface RpcErrorData {
  class: "retryable" | "permanent";
  code: string;
  message: string;
  /** Solo con `retryable`: una sugerencia para el PRIMER reintento. */
  retryAfterMs?: number;
}

export interface RpcErrorInput extends RpcReplyBase {
  error: RpcErrorData;
}

/**
 * ¿`d` cumple el esquema del error (`schemas/system/rpc/error/1.0.0.json`)? Devuelve el primer problema, o
 * `null`. Es la quinta comprobación de quien llama (§4.3) y lo que valida el constructor: se escribe a mano y
 * no con ajv porque L3 es opcional y esta comprobación no lo es.
 */
export function rpcErrorDataProblem(d: unknown): string | null {
  if (d === null || typeof d !== "object" || Array.isArray(d)) return "`data` del error no es un objeto";
  const o = d as Record<string, unknown>;
  const desconocidas = Object.keys(o).filter((k) => !["class", "code", "message", "retryAfterMs"].includes(k));
  if (desconocidas.length > 0) return `el error lleva atributos que su esquema no admite (${desconocidas.join(", ")})`;
  if (o.class !== "retryable" && o.class !== "permanent") {
    return "`class` del error debe ser `retryable` o `permanent` (`poison` no viaja)";
  }
  if (typeof o.code !== "string" || o.code.length > RPC_ERROR_CODE_MAX || !RPC_ERROR_CODE_PATTERN.test(o.code)) {
    return "`code` del error debe ser un identificador estable en MAYÚSCULAS y guiones bajos (máx. 64)";
  }
  if (typeof o.message !== "string" || o.message.length < 1 || o.message.length > RPC_ERROR_MESSAGE_MAX) {
    return "`message` del error debe ser un texto de 1 a 1000 caracteres";
  }
  if (o.retryAfterMs !== undefined) {
    if (typeof o.retryAfterMs !== "number" || !Number.isInteger(o.retryAfterMs) || o.retryAfterMs < 0) {
      return "`retryAfterMs` del error debe ser un entero ≥ 0";
    }
    if (o.class !== "retryable") return "`retryAfterMs` solo tiene sentido con `class: retryable`";
  }
  return null;
}

/**
 * El sobre de un error `com.flux.rpc.error.v1`: lo que el que responde devuelve EN LUGAR de la respuesta, tanto
 * el de un handler que lanzó como el que fabrica su SDK. `type`, `dataschema` y `dataclassification: internal`
 * son FIJOS —el error no depende del contrato de la operación—, y `data` lleva las claves en el orden
 * `class, code, message, retryAfterMs`.
 */
export function buildRpcError(input: RpcErrorInput): FluxEvent<RpcErrorData> {
  parseRpcOperation(input.request.operation);
  const problema = rpcErrorDataProblem(input.error);
  if (problema) throw new TypeError(`error de petición/respuesta mal formado: ${problema}`);
  const key = particion(input.request);
  const { class: clase, code, message, retryAfterMs } = input.error;
  return {
    specversion: "1.0",
    id: input.id,
    source: input.source,
    type: RPC_ERROR_TYPE,
    time: input.time,
    datacontenttype: "application/json",
    dataschema: RPC_ERROR_DATASCHEMA,
    ...(input.request.subject !== undefined && { subject: input.request.subject }),
    correlationid: input.request.correlationid,
    tenantid: input.request.tenantid,
    producerversion: input.producerversion,
    dataclassification: "internal",
    causationid: input.request.id,
    ...(key !== undefined && { partitionkey: key }),
    ...(input.traceparent !== undefined && { traceparent: input.traceparent }),
    data: { class: clase, code, message, ...(retryAfterMs !== undefined && { retryAfterMs }) },
  };
}

const ENCODER = new TextEncoder();
const DECODER = new TextDecoder();

/** Los bytes del mensaje: `JSON.stringify` + UTF-8, sin espacios y sin escapes `\u` (01-envelope.md §1.1). Sin límite de tamaño: lo pone quien envía. */
export function encodeRpcMessage(event: FluxEvent): Uint8Array {
  return ENCODER.encode(JSON.stringify(event));
}

// ═════════════════════════════════════════════════════════════════════════════════════════
// 5. API pública de `bus.request` y `bus.respond`
// ═════════════════════════════════════════════════════════════════════════════════════════

export interface RequestOptions {
  /**
   * Cuánto esperar a la respuesta, en ms: un entero entre 1 y 25 000 (5 000 por defecto). Fuera de rango es
   * `INVALID_RPC_CALL`: no se recorta en silencio. Es el de **un intento**.
   */
  timeoutMs?: number;
  /** Tenant. Si no, el del evento (o la petición) en curso, el de `connect()` y, si no hay, `system`. */
  tenantId?: string;
  /** La identidad del agregado sobre el que se pide (`cli-1`): va al atributo `subject` del sobre. **No** es el subject de NATS. */
  aggregateId?: string;
  /** `public`, `internal`, `confidential` o `restricted`. Si no, la de `connect()`. */
  classification?: DataClassification;
  /**
   * Reintentos ante `RPC_UNAVAILABLE` y `RPC_TIMEOUT` —solo esos: nunca ante un error permanente—, por defecto **0**.
   * **Cada reintento reutiliza el mismo `id` y los mismos bytes** (`time` incluido): con un `id` nuevo por intento, el
   * que responde no podría reconocer la orden repetida, y `RPC_TIMEOUT` **no** dice que la operación no se ejecutó.
   */
  retries?: number;
}

/** Lo que recibe un handler junto a los datos: el sobre de la petición y lo que más se usa de él. */
export interface RpcHandlerContext {
  /** `clientes.cliente.v1.calcular-descuento`. */
  readonly operation: string;
  /** El sobre COMPLETO de la petición. */
  readonly event: FluxEvent;
  /** El `id` de la petición: **la clave de deduplicación** (una orden puede llegar dos veces: 11-request-reply.md §4.5). */
  readonly id: string;
  readonly correlationId: string;
  /** El tenant de la petición. **Es del handler autorizarlo**: quien responde no filtra por el tenant de `connect()` (§5.6). */
  readonly tenantId: string;
  /** El atributo `subject` del sobre, si la petición lo traía. */
  readonly aggregateId?: string;
  readonly time: string;
}

/** Lanzar `PermanentError` / `RetryableError` responde un error; cualquier otra excepción, un `permanent` `UNKNOWN` genérico. */
export type RpcHandler<Req = Record<string, unknown>, Res extends object = Record<string, unknown>> = (
  request: Req,
  ctx: RpcHandlerContext,
) => Res | Promise<Res>;

export interface RespondOptions {
  /** La clasificación de las respuestas de esta operación. Si no, la de `connect()`. El error es siempre `internal`. */
  classification?: DataClassification;
  /**
   * Handlers concurrentes de ESTA operación (256 por defecto: el orden de `max_ack_pending`). El exceso espera en la
   * cola de la suscripción: core NATS no tiene acuse, y un responder sobrecargado no puede decir «ahora no»
   * (11-request-reply.md §5.6, §13).
   */
  maxConcurrent?: number;
}

export interface Responder {
  /** `clientes.cliente.v1.calcular-descuento`. */
  readonly operation: string;
  /** El subject EXACTO al que se suscribió: `rpc.clientes.cliente.v1.calcular-descuento`. Nunca `rpc.<dominio>.>`. */
  readonly subject: string;
  /**
   * Parada ordenada (11-request-reply.md §5.5): **deja de escuchar** (UNSUB y espera a lo que ya viaja: las peticiones
   * nuevas dan `no responders` al instante), **espera a los handlers en vuelo** —con un tope de 25 s— y retorna.
   * Cerrar la conexión de golpe pierde lo que se atiende (A-62).
   */
  stop(opts?: { timeoutMs?: number }): Promise<void>;
}

// ═════════════════════════════════════════════════════════════════════════════════════════
// 6. El cliente
// ═════════════════════════════════════════════════════════════════════════════════════════

/**
 * Lo que `RpcClient` necesita de `FluxBus`. Interno: lleva tipos de NATS y no se exporta desde index.ts. Es una
 * interfaz y no el bus entero para que los tests puedan darle conexiones falsas.
 */
export interface RpcHost {
  readonly nc: NatsConnection;
  readonly options: ConnectOptions;
  readonly source: string;
  readonly metrics: MetricsSink;
  /** Como `classify`, más si el clasificador RECONOCIÓ el error (un 503, `ECONNRESET`) o cayó en la política de lo desconocido. */
  classify(e: unknown): { classification: Classification; recognized: boolean };
  /** L3 si está configurada. Se resuelve en `connect()`, de ahí que sea una función. */
  validator(): Validator | null;
  signer(): Signer | null;
  /** Verifica la firma. Lanza (`require`) o devuelve el código del fallo (`warn`), o `null`. */
  verify(event: FluxEvent): string | null;
  /**
   * Avisa cuando el broker deniega PUBLICAR en `subject` (el `-ERR` de la conexión). Una librería que no entrega la
   * denegación en la propia petición deja a quien llama esperando su timeout (A-28, A-72): se compite con la respuesta
   * (11-request-reply.md §7.1). `cancel` quita el aviso.
   */
  watchDenied(subject: string): { promise: Promise<never>; cancel(): void };
  /** El reloj (ms desde 1970). Inyectable para probar la ventana de `STALE_REQUEST`. */
  now(): number;
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const aIso = (ms: number) => new Date(ms).toISOString();

/** Corta a `max` UTF-16 sin partir un par sustituto: una mitad se serializaría como `\ud83d`. */
function truncar(s: string, max: number): string {
  if (s.length <= max) return s;
  const corte = /[\ud800-\udbff]/.test(s[max - 2] ?? "") ? max - 2 : max - 1;
  return `${s.slice(0, corte)}…`;
}

/** Un semáforo: `acquire` espera si no quedan cupos, y `release` se lo pasa al siguiente que espera. */
class Cupos {
  #libres: number;
  readonly #cola: Array<() => void> = [];
  constructor(n: number) {
    this.#libres = n;
  }
  async acquire(): Promise<void> {
    if (this.#libres > 0) {
      this.#libres--;
      return;
    }
    await new Promise<void>((r) => this.#cola.push(r));
  }
  release(): void {
    const siguiente = this.#cola.shift();
    if (siguiente) siguiente();
    else this.#libres++;
  }
}

/** ¿`e`, o su cadena de causas, es el timeout de la librería? Por nombre: con dos copias de `@nats-io/*`, `instanceof` falla sin ruido. */
function esTimeout(e: unknown): boolean {
  for (let x: unknown = e, i = 0; typeof x === "object" && x !== null && i < 6; i++) {
    if ((x as { name?: unknown }).name === "TimeoutError") return true;
    x = (x as { cause?: unknown }).cause;
  }
  return false;
}

/** El cuerpo, ¿parece el acuse de un stream de JetStream (`{"stream":…,"seq":…}`)? */
function pareceAcuseDeStream(bytes: Uint8Array): boolean {
  try {
    const o = JSON.parse(DECODER.decode(bytes)) as unknown;
    return (
      typeof o === "object" && o !== null && typeof (o as { stream?: unknown }).stream === "string" &&
      typeof (o as { seq?: unknown }).seq === "number"
    );
  } catch {
    return false;
  }
}

/** El `id` de un mensaje que no se pudo leer como CloudEvent, **solo si es una cadena válida** (§5.2). */
function idLegible(bytes: Uint8Array): string | undefined {
  try {
    const id = (JSON.parse(DECODER.decode(bytes)) as { id?: unknown } | null)?.id;
    return typeof id === "string" && id.length >= 1 && id.length <= 256 ? id : undefined;
  } catch {
    return undefined;
  }
}

interface PreparedRequest {
  readonly operation: string;
  readonly subject: string;
  readonly bytes: Uint8Array;
  readonly event: FluxEvent;
  readonly replyUri: string;
  readonly timeoutMs: number;
  readonly retries: number;
}

interface ResponderState {
  readonly operation: string;
  readonly subject: string;
  readonly handler: RpcHandler;
  readonly classification: DataClassification | undefined;
  readonly requestUri: string;
  readonly replyUri: string;
  readonly sub: NatsSubscription;
  readonly cupos: Cupos;
  readonly enVuelo: Set<Promise<void>>;
  pump: Promise<void>;
  parando?: Promise<void>;
  /** `respond()` ya devolvió: una denegación posterior no puede ser un error de `respond()`, solo un log. */
  arrancado: boolean;
}

export class RpcClient {
  readonly #h: RpcHost;
  readonly #responders = new Map<string, ResponderState>();

  constructor(host: RpcHost) {
    this.#h = host;
  }

  // ─── llamar ────────────────────────────────────────────────────────────────

  /**
   * El algoritmo de 11-request-reply.md §4.1. Resuelve con el `data` de la respuesta. Hace UN intento, salvo `retries`.
   *
   * Todo lo que se puede comprobar sin red se comprueba ANTES, en este orden y el primero que falla gana:
   * `RESERVED_DOMAIN` → `INVALID_RPC_CALL` → `INVALID_DATACLASSIFICATION` → L3 (`INVALID_SCHEMA`) → construir el sobre UNA
   * vez → tamaño (`RPC_MESSAGE_TOO_LARGE`).
   */
  async request<R = Record<string, unknown>>(
    operation: string,
    data: object,
    options: RequestOptions = {},
  ): Promise<R> {
    const prep = await this.#prepare(operation, data, options);
    const inicio = performance.now();
    const m = this.#h.metrics;
    let intento = 0;
    for (;;) {
      intento++;
      try {
        const out = await this.#attempt(prep);
        m.rpcClientRequest?.(prep.operation, "ok");
        m.rpcDuration?.(prep.operation, "client", (performance.now() - inicio) / 1000);
        return out as R;
      } catch (e) {
        // Solo `RPC_UNAVAILABLE` y `RPC_TIMEOUT` se reintentan, con el MISMO `id` y los mismos bytes.
        if (intento <= prep.retries && (e instanceof RpcUnavailableError || e instanceof RpcTimeoutError)) {
          if (e instanceof RpcUnavailableError) await sleep(RETRY_PAUSE_MS * intento);
          continue;
        }
        const outcome = this.#clientOutcome(e);
        if (outcome) m.rpcClientRequest?.(prep.operation, outcome);
        m.rpcDuration?.(prep.operation, "client", (performance.now() - inicio) / 1000);
        throw e;
      }
    }
  }

  /** Paso 0 de §4.1: validar y construir SIN red. */
  async #prepare(operation: string, data: object, options: RequestOptions): Promise<PreparedRequest> {
    const h = this.#h;
    // RESERVED_DOMAIN → INVALID_RPC_CALL (operación, timeoutMs, data objeto, y las opciones propias)
    const p = parseRpcOperation(operation);
    if (options === null || typeof options !== "object") {
      throw new InvalidRpcCallError("`options` debe ser un objeto (`{ timeoutMs, tenantId, aggregateId, classification, retries }`).");
    }
    const timeoutMs = resolveRpcTimeout(options.timeoutMs);
    assertObject(data, "`data`");
    const retries = options.retries ?? 0;
    if (typeof retries !== "number" || !Number.isInteger(retries) || retries < 0) {
      throw new InvalidRpcCallError("`retries` debe ser un entero ≥ 0 (0 por defecto: `request` hace UN intento).");
    }
    if (options.aggregateId !== undefined && (typeof options.aggregateId !== "string" || options.aggregateId === "")) {
      throw new InvalidRpcCallError("`aggregateId` debe ser un texto no vacío: la identidad del agregado (`cli-1`).");
    }
    if (options.tenantId !== undefined && (typeof options.tenantId !== "string" || options.tenantId === "")) {
      throw new InvalidRpcCallError("`tenantId` debe ser un texto no vacío.");
    }
    // INVALID_DATACLASSIFICATION
    const classification = assertClassification(options.classification ?? h.options.classification ?? "internal");

    const id = uuidv7();
    const inherited = currentContext();
    const event = buildRpcRequest({
      operation: p.operation,
      // El contexto heredado gana sobre el default de conexión (09-multitenancy.md §5): una llamada hecha dentro de un
      // handler continúa la cadena. Un estado exige un tenant explícito; la petición, no: como los eventos, cae en `system`.
      tenantId: options.tenantId ?? inherited?.tenantid ?? h.options.tenantId ?? "system",
      id,
      source: h.source,
      time: aIso(h.now()),
      correlationid: inherited?.correlationid ?? id,
      producerversion: h.options.version,
      classification,
      data,
      dataschema: this.#schemaFor(p.operation, "request"),
      ...(options.aggregateId !== undefined && { aggregateId: options.aggregateId }),
      ...(inherited?.causationid !== undefined && { causationid: inherited.causationid }),
      ...(inherited?.traceparent !== undefined
        ? { traceparent: inherited.traceparent }
        : await this.#traceparent()),
      ...(inherited?.tracestate !== undefined && { tracestate: inherited.tracestate }),
    });
    const replyUri = this.#schemaFor(p.operation, "reply");

    // L3: la petición contra su esquema, ANTES de abrir nada: reporta TODAS las rutas y ningún valor.
    const v = h.validator();
    if (v) {
      const fallo = v.check(event, p.operation);
      if (fallo) {
        h.metrics.rpcClientRequest?.(p.operation, "invalid_schema");
        if (v.mode === "strict") throw this.#asRpcSchemaError(fallo, "petición", p.operation);
        v(event, p.operation); // `warn`: lo registra la propia validación, y se sigue
      }
    }

    // El sobre se construye UNA vez: `id`, `time` y bytes son los mismos en todos los intentos.
    let bytes: Uint8Array;
    try {
      bytes = this.#encode(event, "petición", p.operation);
    } catch (e) {
      if (e instanceof RpcMessageTooLargeError) h.metrics.rpcClientRequest?.(p.operation, "permanent");
      throw e;
    }
    return { operation: p.operation, subject: `rpc.${p.operation}`, bytes, event, replyUri, timeoutMs, retries };
  }

  /**
   * Un intento: publica por core NATS con el inbox privado y espera, lo primero que ocurra: `no responders` →
   * `RPC_UNAVAILABLE`; un `-ERR Permissions Violation` → `PERMISSION_DENIED`; una respuesta → se comprueba; el timeout →
   * `RPC_TIMEOUT`.
   */
  async #attempt(p: PreparedRequest): Promise<unknown> {
    const h = this.#h;
    // El `-ERR` se compite con la respuesta: con una librería que no entrega la denegación en la propia petición
    // (async-nats, A-72) quien llama solo vería un timeout, y un permiso que falta se vería como `RPC_TIMEOUT`
    // (RETRYABLE) y el handler que llama reintentaría 51 minutos algo que nunca va a funcionar (§7.1).
    const denegacion = h.watchDenied(p.subject);
    let respuesta: Msg;
    try {
      respuesta = await Promise.race([
        h.nc.request(p.subject, p.bytes, { timeout: p.timeoutMs }),
        denegacion.promise,
      ]);
    } catch (e) {
      throw this.#translate(e, p);
    } finally {
      denegacion.cancel();
    }

    // 3. ¿Es la respuesta a ESTA petición? (§4.3) Si no, `RPC_INVALID_REPLY`.
    const { event, esError } = this.#checkReply(p, respuesta.data);

    // Con la firma en `require`: verificar. Es un fallo PERMANENT: reintentar da lo mismo.
    try {
      h.verify(event); // `warn` registra el fallo y sigue (la propia verificación lo escribe); `require` lanza
    } catch (e) {
      const code = e instanceof PoisonError ? (e.opts.code ?? "INVALID_SIGNATURE") : "INVALID_SIGNATURE";
      throw new RpcPermanentError(
        p.operation,
        code,
        `la respuesta a "${p.operation}" no pasó la verificación de su firma (${code}): ${e instanceof Error ? e.message : String(e)}. ` +
          `Con la firma en \`require\`, quien llama verifica la respuesta y los errores (11-request-reply.md §7.4).`,
      );
    }

    // 4. Un error: RetryableError o PermanentError según su `class`, con su `code`.
    if (esError) {
      const d = event.data as RpcErrorData;
      throw d.class === "retryable"
        ? new RpcRetryableError(p.operation, d.code, d.message, d.retryAfterMs)
        : new RpcPermanentError(p.operation, d.code, d.message);
    }

    // 5. Una respuesta: con L3, `data` contra el esquema de la respuesta; se devuelve el `data`.
    const v = h.validator();
    if (v) {
      const fallo = v.check({ ...event, dataschema: p.replyUri }, p.operation);
      if (fallo) {
        if (v.mode === "strict") throw this.#asRpcSchemaError(fallo, "respuesta", p.operation);
        v({ ...event, dataschema: p.replyUri }, p.operation);
        h.metrics.rpcClientRequest?.(p.operation, "invalid_schema");
      }
    }
    return event.data;
  }

  /** Las cinco comprobaciones de §4.3. Cualquiera que falle es `RPC_INVALID_REPLY`. */
  #checkReply(p: PreparedRequest, bytes: Uint8Array): { event: FluxEvent; esError: boolean } {
    const invalida = (motivo: string, opts: { cause?: unknown } = {}) =>
      new RpcInvalidReplyError(p.operation, motivo, { ...opts, pistaStream: pareceAcuseDeStream(bytes) });

    // 1. Es un CloudEvent válido (parsea, con las cuatro extensiones y los tipos exactos).
    let event: FluxEvent;
    try {
      event = parseEvent(bytes);
    } catch (e) {
      const code = e instanceof PoisonError ? (e.opts.code ?? "POISON") : "POISON";
      throw invalida(`no es un CloudEvent válido de flux (${code})`, { cause: e });
    }
    // 2. Su `type` es el de la respuesta de esa operación, o el del error.
    const esperado = rpcReplyType(p.operation);
    if (event.type !== esperado && event.type !== RPC_ERROR_TYPE) {
      throw invalida(`su \`type\` es ${JSON.stringify(truncar(event.type, 100))} y se esperaba ${JSON.stringify(esperado)} o ${JSON.stringify(RPC_ERROR_TYPE)}`);
    }
    // 3. Su `causationid` es el `id` de la petición.
    if (event.causationid !== p.event.id) {
      throw invalida(
        `su \`causationid\` no es el \`id\` de la petición (${p.event.id}): ` +
          `${typeof event.causationid === "string" ? JSON.stringify(truncar(event.causationid, 80)) : "no lo trae"}`,
      );
    }
    // 4. Su `correlationid` y su `tenantid` son los de la petición. EXCEPCIÓN ÚNICA (§4.3.4): el error de una petición que
    // quien responde no pudo leer (§5.2) no tiene nada que heredar y lleva `correlationid` propio y `tenantid: system`; a
    // quien llama le basta la comprobación 3 (su `causationid` es el `id` de la petición, que solo él conoce) para
    // atribuirlo y ver el código del sobre (`INVALID_RPC_CALL`, `MALFORMED_JSON`…) en vez de `RPC_INVALID_REPLY`.
    const esError = event.type === RPC_ERROR_TYPE;
    if (!esErrorDeUnaPeticionIlegible(event, esError)) {
      if (event.correlationid !== p.event.correlationid) {
        throw invalida("su `correlationid` no es el de la petición: una respuesta hereda el de la petición");
      }
      if (event.tenantid !== p.event.tenantid) {
        throw invalida("su `tenantid` no es el de la petición: una respuesta hereda el de la petición, sin modificar");
      }
    }
    // 5. Si es un error, su `data` cumple el esquema del error.
    if (esError) {
      const problema = rpcErrorDataProblem(event.data);
      if (problema) throw invalida(`el error no cumple su esquema (${problema})`);
    }
    return { event, esError };
  }

  /** Traduce lo que lanza la librería a un código estable. */
  #translate(e: unknown, p: PreparedRequest): unknown {
    if (e instanceof PermissionDeniedError) return e;
    if (isNoResponders(e)) return new RpcUnavailableError(p.operation, { cause: e });
    const v = permissionViolation(e);
    if (v) {
      return new PermissionDeniedError(this.#h.options.service, v.operation, v.subject, {
        cause: e,
        hint: this.#hintCaller(v.operation, p.subject),
      });
    }
    if (esTimeout(e)) return new RpcTimeoutError(p.operation, p.timeoutMs, { cause: e });
    return e;
  }

  #hintCaller(operacion: "publish" | "subscribe", subject: string): string {
    return operacion === "publish"
      ? `Quien llama necesita permiso de PUBLICACIÓN sobre el subject EXACTO de cada operación concedida (\`${subject}\`; ` +
          `nunca \`rpc.>\` ni \`rpc.<dominio>.>\`): es la clave \`calls\` del manifiesto de \`gen-acl.mjs\`. Sin él la petición ` +
          `se descarta y, con una librería que no entrega la denegación, solo se vería un timeout (11-request-reply.md §7.1).`
      : `Quien llama necesita permiso de SUSCRIPCIÓN sobre su inbox privado \`_INBOX.${this.#h.options.service}.>\`: ` +
          `por ahí llegan las respuestas, y sin él ninguna puede llegar (11-request-reply.md §7.3, 06-security.md §3.3).`;
  }

  #asRpcSchemaError(
    e: SchemaValidationError | SchemaNotFoundError,
    lado: "petición" | "respuesta",
    operation: string,
  ): SchemaValidationError | SchemaNotFoundError {
    if (!(e instanceof SchemaValidationError)) return e;
    return new RpcSchemaError(lado, operation, e.dataschema, e.errors);
  }

  #clientOutcome(e: unknown): RpcClientOutcome | undefined {
    if (e instanceof RpcUnavailableError) return "unavailable";
    if (e instanceof RpcTimeoutError) return "timeout";
    if (e instanceof RpcInvalidReplyError) return "invalid_reply";
    if (e instanceof SchemaValidationError || e instanceof SchemaNotFoundError) return "invalid_schema";
    if (e instanceof RpcRetryableError) return "retryable";
    // `permanent`: un error permanente del que responde, `PERMISSION_DENIED`, `STALE_REQUEST`, `RPC_MESSAGE_TOO_LARGE` y un fallo de firma.
    if (e instanceof RpcPermanentError || e instanceof PermissionDeniedError || e instanceof RpcMessageTooLargeError) {
      return "permanent";
    }
    return undefined; // un fallo de transporte lo cuenta la conexión; INVALID_RPC_CALL no se cuenta: es de programación
  }

  // ─── responder ─────────────────────────────────────────────────────────────

  /**
   * Atiende una operación: **una suscripción al subject EXACTO** (`rpc.<operación>`, nunca `rpc.<dominio>.>`: con el comodín,
   * una operación que el servicio no atiende recibe interés y quien llama espera su timeout en vez de `no responders`, A-64)
   * con el **nombre del servicio como grupo de cola**: dos instancias se reparten las peticiones (A-58).
   *
   * Un dueño sin permiso de suscripción parece un servicio CAÍDO (`no responders`, A-73): `respond()` **falla** con
   * `PERMISSION_DENIED`, nombrando el subject, en vez de arrancar mudo.
   */
  async respond<Req = Record<string, unknown>, Res extends object = Record<string, unknown>>(
    operation: string,
    handler: RpcHandler<Req, Res>,
    options: RespondOptions = {},
  ): Promise<Responder> {
    const h = this.#h;
    const p = parseRpcOperation(operation);
    if (typeof handler !== "function") throw new InvalidRpcCallError("el handler de `respond()` debe ser una función `(petición, ctx) => resultado`.");
    if (options === null || typeof options !== "object") throw new InvalidRpcCallError("`options` debe ser un objeto (`{ classification, maxConcurrent }`).");
    const maxConcurrent = options.maxConcurrent ?? RPC_MAX_CONCURRENT_HANDLERS;
    if (typeof maxConcurrent !== "number" || !Number.isInteger(maxConcurrent) || maxConcurrent < 1) {
      throw new InvalidRpcCallError("`maxConcurrent` debe ser un entero ≥ 1 (256 por defecto).");
    }
    const classification = options.classification !== undefined ? assertClassification(options.classification) : undefined;
    if (this.#responders.has(p.operation)) {
      throw new InvalidRpcCallError(
        `ya hay un \`respond()\` de "${p.operation}" en este servicio. Dos suscripciones del mismo grupo de cola en una ` +
          `conexión se repartirían las peticiones entre dos handlers: registra la operación una vez, o llama a ` +
          `\`stop()\` del responder anterior.`,
      );
    }
    const requestUri = this.#schemaFor(p.operation, "request");
    const replyUri = this.#schemaFor(p.operation, "reply");
    // En modo estricto una operación sin esquema es un fallo de configuración: se dice al arrancar, no en cada petición.
    const v = h.validator();
    if (v?.mode === "strict") {
      const bundle = h.options.validation?.bundle;
      for (const [uri, lado] of [[requestUri, "petición"], [replyUri, "respuesta"]] as const) {
        if (!bundle?.schemas[uri]) throw new SchemaNotFoundError(`${p.operation} (${lado})`, uri);
      }
    }

    const subject = `rpc.${p.operation}`;
    const sub = h.nc.subscribe(subject, { queue: h.options.service });
    const estado: ResponderState = {
      operation: p.operation,
      subject,
      handler: handler as unknown as RpcHandler,
      classification,
      requestUri,
      replyUri,
      sub,
      cupos: new Cupos(maxConcurrent),
      enVuelo: new Set(),
      pump: Promise.resolve(),
      arrancado: false,
    };
    this.#responders.set(p.operation, estado);

    const aviso: { denegada?: { operation: "publish" | "subscribe"; subject: string } } = {};
    estado.pump = this.#pump(estado, (d) => void (aviso.denegada = d));

    try {
      // Una suscripción denegada NO lanza al suscribirse: el `-ERR` llega por la conexión y hace que el iterador de esa
      // suscripción lance al consumirla (A-8f, A-73). El `flush` es un PING/PONG y el protocolo es ordenado: el `-ERR`
      // llega antes que el PONG. Un turno más de event loop deja que el iterador lo anote.
      await h.nc.flush();
      await new Promise<void>((r) => setImmediate(r));
    } catch (e) {
      this.#responders.delete(p.operation);
      throw e;
    }
    if (aviso.denegada) {
      this.#responders.delete(p.operation);
      await estado.pump;
      throw new PermissionDeniedError(h.options.service, "subscribe", aviso.denegada.subject, {
        hint:
          `El dueño de un dominio necesita permiso de SUSCRIPCIÓN sobre \`rpc.<dominio>.>\` (es la clave \`owns\` del manifiesto de ` +
          `\`gen-acl.mjs\`; el SDK se suscribe al subject exacto de cada operación, pero lo que se CONCEDE no es lo que se USA). ` +
          `Sin él la operación parecería un servicio caído —quien llama vería \`RPC_UNAVAILABLE\`— y por eso \`respond()\` falla ` +
          `al arrancar y no arranca mudo (11-request-reply.md §7.1, A-73).`,
      });
    }

    estado.arrancado = true;
    return {
      operation: p.operation,
      subject,
      stop: (opts) => this.#stop(estado, opts?.timeoutMs ?? RPC_STOP_TIMEOUT_MS),
    };
  }

  /**
   * Recibe de la suscripción y lanza handlers hasta `maxConcurrent`; el exceso espera en la cola de la propia suscripción.
   * Termina cuando la suscripción se drena (`stop()`), se cierra la conexión o el broker la deniega.
   */
  async #pump(r: ResponderState, alDenegar: (v: { operation: "publish" | "subscribe"; subject: string }) => void): Promise<void> {
    try {
      for await (const m of r.sub) {
        await r.cupos.acquire();
        const tarea: Promise<void> = this.#serve(r, m).finally(() => {
          r.enVuelo.delete(tarea);
          r.cupos.release();
        });
        r.enVuelo.add(tarea);
      }
    } catch (e) {
      const v = permissionViolation(e);
      if (v) {
        alDenegar(v);
        // Un permiso que se pierde DESPUÉS de arrancar (una ACL recargada) no puede devolverse como error: `respond()` ya
        // devolvió. Al arrancar sí se devuelve (falla), y aquí no se registra nada.
        if (r.arrancado) {
          this.#h.options.logger?.error(
            `[flux] el broker denegó ${v.operation === "subscribe" ? "suscribirse a" : "publicar en"} "${v.subject}" a ` +
              `"${this.#h.options.service}": la operación "${r.operation}" ya NO se atiende y quien llame verá RPC_UNAVAILABLE ` +
              `(11-request-reply.md §7.1).`,
          );
        }
      } else {
        this.#h.options.logger?.error(
          `[flux] la suscripción de "${r.operation}" terminó con un error: ${e instanceof Error ? e.message : String(e)}`,
        );
      }
    }
    // Pase lo que pase, una operación que ya no escucha deja de figurar como atendida.
    if (this.#responders.get(r.operation) === r) this.#responders.delete(r.operation);
  }

  /** La parada ordenada de §5.5: dejar de escuchar → esperar a lo que está en vuelo (con tope) → el llamante cierra. */
  #stop(r: ResponderState, timeoutMs: number): Promise<void> {
    r.parando ??= (async () => {
      this.#responders.delete(r.operation);
      // 1. Dejar de escuchar con el DRENAJE de la librería: UNSUB, y un PING/PONG que espera a lo que ya viaja. Las
      // peticiones nuevas dan `no responders` al instante; una baja que descartase lo que aún llega la perdería (A-62).
      try {
        await r.sub.drain();
      } catch {
        // ya cerrada (la conexión se cerró, o el broker la denegó): no hay nada que dejar de escuchar
      }
      // 2. Esperar a los handlers en vuelo, con un tope: una petición más lenta ya la dio por perdida quien llamó. Un margen
      // breve cubre la propagación asíncrona del interés en un cluster (A-70).
      let aviso: ReturnType<typeof setTimeout> | undefined;
      const limite = new Promise<"limite">((res) => {
        aviso = setTimeout(() => res("limite"), timeoutMs);
      });
      const terminado = (async () => {
        await r.pump;
        while (r.enVuelo.size > 0) await Promise.allSettled([...r.enVuelo]);
        await sleep(RPC_STOP_MARGIN_MS);
        return "terminado" as const;
      })();
      const quien = await Promise.race([terminado, limite]);
      clearTimeout(aviso);
      if (quien === "limite") {
        this.#h.options.logger?.warn(
          `[flux] "${r.operation}": ${r.enVuelo.size} handler(s) siguen en vuelo tras ${timeoutMs} ms; se deja de esperar. ` +
            `Una petición más lenta que el timeout máximo (25 s) ya la dio por perdida quien llamó.`,
        );
      }
    })();
    return r.parando;
  }

  /** Para todos los responders: lo que hace `bus.close()` ANTES de cerrar la conexión. */
  async stopAll(timeoutMs: number = RPC_STOP_TIMEOUT_MS): Promise<void> {
    await Promise.all([...this.#responders.values()].map((r) => this.#stop(r, timeoutMs)));
  }

  // ─── atender una petición ──────────────────────────────────────────────────

  /** Un mensaje de la suscripción. NUNCA lanza: lo que falle aquí va al log, y el bucle sigue. */
  async #serve(r: ResponderState, m: Msg): Promise<void> {
    const h = this.#h;
    const inicio = performance.now();
    let outcome: RpcServerOutcome | undefined;
    try {
      outcome = await this.#handle(r, m);
    } catch (e) {
      h.options.logger?.error(
        `[flux] fallo inesperado atendiendo "${r.operation}": ${e instanceof Error ? e.message : String(e)}. ` +
          `Quien llamó verá un timeout.`,
      );
    }
    if (outcome) {
      h.metrics.rpcServerRequest?.(r.operation, outcome);
      h.metrics.rpcDuration?.(r.operation, "server", (performance.now() - inicio) / 1000);
    }
  }

  /** Los pasos de §5.1. Devuelve el `outcome` de la petición. */
  async #handle(r: ResponderState, m: Msg): Promise<RpcServerOutcome> {
    const h = this.#h;
    const op = r.operation;
    const reply = m.reply;

    // 2. Sin subject de respuesta no se atiende: nadie espera el resultado de una orden (`nats pub` a mano).
    if (!reply) {
      this.#poison(op, new PoisonError("la petición no trae subject de respuesta", { code: "NO_REPLY_SUBJECT" }),
        `llegó una petición a "${op}" SIN subject de respuesta (¿un \`nats pub\` a mano?): no se atiende, porque nadie espera el ` +
          `resultado de una orden y ejecutarla a ciegas la aplicaría sin que nadie lo sepa (11-request-reply.md §5.1).`);
      return "poison";
    }

    // 3. Interpretar el sobre. Falla → un error `permanent` con el código del sobre. Sin sobre no hay nada que heredar.
    let event: FluxEvent;
    try {
      event = parseEvent(m.data);
    } catch (e) {
      const code = e instanceof PoisonError ? (e.opts.code ?? "MALFORMED_JSON") : "MALFORMED_JSON";
      this.#poison(op, e instanceof PoisonError ? e : new PoisonError(String(e), { code }),
        `una petición a "${op}" no es un CloudEvent válido (${code}): se contesta con un error permanent.`);
      await this.#sendError(reply, r, null, idLegible(m.data), {
        class: "permanent",
        code,
        message:
          `la petición no es un CloudEvent válido de flux (${code}): se espera JSON con specversion, id, source, type, time, ` +
          `datacontenttype, dataschema y data, más las cuatro extensiones obligatorias (correlationid, tenantid, producerversion, ` +
          `dataclassification) con tipos exactos (01-envelope.md §3.1)`,
      });
      return "poison";
    }

    // 4. `type` = el de la petición de ESTA operación.
    const tipo = rpcRequestType(op);
    if (event.type !== tipo) {
      this.#poison(op, new PoisonError(`type inesperado en una petición a "${op}"`, { code: "INVALID_RPC_CALL" }),
        `una petición a "${op}" trae \`type\` ${JSON.stringify(truncar(event.type, 100))} y se esperaba ${JSON.stringify(tipo)}: ` +
          `se contesta con INVALID_RPC_CALL.`);
      await this.#sendError(reply, r, event, undefined, {
        class: "permanent",
        code: "INVALID_RPC_CALL",
        message:
          `la petición llegó al subject de "${op}" con \`type\` ${JSON.stringify(truncar(event.type, 100))}, y el de la petición de ` +
          `esta operación es ${JSON.stringify(tipo)}. Quien llama debe construirla con el SDK (11-request-reply.md §5.2)`,
      });
      return "poison";
    }

    // 5. Con la firma en `require`: verificar, y la ventana de 300 s.
    try {
      h.verify(event); // `warn` registra el fallo y sigue (la propia verificación lo escribe); `require` lanza
    } catch (e) {
      const code = e instanceof PoisonError ? (e.opts.code ?? "INVALID_SIGNATURE") : "INVALID_SIGNATURE";
      h.options.logger?.error(`[flux] la petición a "${op}" (${event.id}) no pasó la verificación de su firma (${code}).`);
      await this.#sendError(reply, r, event, undefined, {
        class: "permanent",
        code,
        message:
          `la petición a "${op}" no pasó la verificación de su firma (${code}): con la firma en \`require\`, quien responde ` +
          `exige una petición firmada con una clave que conozca (07-signing.md §7)`,
      });
      return "invalid_signature";
    }
    if (h.options.signing?.verify === "require") {
      const ahora = h.now();
      const t = Date.parse(event.time);
      if (!Number.isFinite(t) || Math.abs(ahora - t) > RPC_STALE_WINDOW_SECONDS * 1000) {
        const e = new StaleRequestError(event.time, RPC_STALE_WINDOW_SECONDS, aIso(ahora));
        h.options.logger?.error(`[flux] STALE_REQUEST en "${op}" (${event.id}): \`time\` fuera de la ventana de ${RPC_STALE_WINDOW_SECONDS} s.`);
        await this.#sendError(reply, r, event, undefined, { class: "permanent", code: "STALE_REQUEST", message: e.message });
        return "stale_request";
      }
    }

    // 6. Con L3: `data` contra el esquema de la petición. Falla → error `permanent` INVALID_SCHEMA con TODAS las rutas.
    const v = h.validator();
    if (v) {
      const fallo = v.check({ ...event, dataschema: r.requestUri }, op);
      if (fallo) {
        if (v.mode === "strict") {
          const msg = fallo instanceof SchemaValidationError
            ? `la petición no cumple ${fallo.dataschema}: ${fallo.errors.join(", ")}`
            : `no hay esquema de la petición de "${op}" (${r.requestUri}) en el bundle de quien responde`;
          await this.#sendError(reply, r, event, undefined, { class: "permanent", code: fallo.code, message: msg });
          return "invalid_schema";
        }
        // `warn`: queda en el log y en la métrica, y el handler SE EJECUTA.
        v({ ...event, dataschema: r.requestUri }, op);
        h.metrics.rpcServerRequest?.(op, "invalid_schema");
      }
    }

    // 7. Invocar el handler con (data, ctx). Una llamada hecha desde él continúa la cadena.
    const ctx: RpcHandlerContext = {
      operation: op,
      event,
      id: event.id,
      correlationId: event.correlationid,
      tenantId: event.tenantid,
      ...(event.subject !== undefined && { aggregateId: event.subject }),
      time: event.time,
    };
    let resultado: unknown;
    try {
      resultado = await runWithContext(contextFromEvent(event), async () => r.handler(event.data as Record<string, unknown>, ctx));
    } catch (e) {
      const err = this.#errorOf(op, event, e);
      await this.#sendError(reply, r, event, undefined, err);
      return err.class;
    }

    // 8. Su resultado es un objeto. Con L3: contra el esquema de la respuesta.
    if (resultado === null || typeof resultado !== "object" || Array.isArray(resultado)) {
      await this.#sendError(reply, r, event, undefined, {
        class: "permanent",
        code: "INVALID_SCHEMA",
        message:
          `la respuesta no es un objeto: el handler de "${op}" devolvió ${resultado === null ? "null" : Array.isArray(resultado) ? "un array" : typeof resultado}. ` +
          `El fallo es de quien responde: la respuesta es un objeto JSON en la raíz (01-envelope.md §4)`,
      });
      return "invalid_schema";
    }
    const dataRespuesta = resultado as object;
    if (v) {
      // El validador mira `dataschema` y `data` y nada más: se le da la respuesta con el esquema de ESTA operación.
      const candidato = { ...event, dataschema: r.replyUri, data: dataRespuesta } as FluxEvent;
      const fallo = v.check(candidato, op);
      if (fallo) {
        if (v.mode === "strict") {
          const msg = fallo instanceof SchemaValidationError
            ? `la respuesta no cumple ${fallo.dataschema}: ${fallo.errors.join(", ")}. El fallo es de quien responde, no de quien llama`
            : `no hay esquema de la respuesta de "${op}" (${r.replyUri}) en el bundle de quien responde`;
          await this.#sendError(reply, r, event, undefined, { class: "permanent", code: fallo.code, message: msg });
          return "invalid_schema";
        }
        v(candidato, op);
        h.metrics.rpcServerRequest?.(op, "invalid_schema");
      }
    }

    // 9. Construir la respuesta (§3.2), firmarla si procede, comprobar el tamaño y publicarla al `reply` de la petición.
    const respuesta = buildRpcReply({
      request: refOf(op, event),
      id: uuidv7(),
      source: h.source,
      time: aIso(h.now()),
      producerversion: h.options.version,
      classification: r.classification ?? h.options.classification ?? "internal",
      data: dataRespuesta,
      dataschema: r.replyUri,
      ...(await this.#replyTraceparent(event)),
    });
    let bytes: Uint8Array;
    try {
      bytes = this.#encode(respuesta, "respuesta", op);
    } catch (e) {
      if (!(e instanceof RpcMessageTooLargeError)) throw e;
      // Si la respuesta no cabe, la librería lanza EN QUIEN RESPONDE y quien llama solo ve un timeout (A-65): se contesta con un error.
      h.options.logger?.error(`[flux] la respuesta de "${op}" a la petición ${event.id} no cabe: ${e.message}`);
      await this.#sendError(reply, r, event, undefined, { class: "permanent", code: "RPC_MESSAGE_TOO_LARGE", message: e.message });
      return "permanent";
    }
    this.#publish(reply, bytes, op);
    return "ok";
  }

  /** El `traceparent` de la respuesta: el del que responde, de la MISMA traza; sin span activo, el de la petición. */
  async #replyTraceparent(request: FluxEvent): Promise<{ traceparent?: string }> {
    const activo = await activeTraceparent();
    const tp = activo ?? request.traceparent;
    return tp !== undefined ? { traceparent: tp } : {};
  }

  /**
   * Un error del handler → el `data` de `com.flux.rpc.error.v1` (§5.3):
   *   · `RetryableError` / `PermanentError` → su clase, SU `code` y SU mensaje (la aplicación los eligió);
   *   · uno que el clasificador RECONOCE (un 503, `ECONNRESET`) → su clase y el código del clasificador, con un mensaje GENÉRICO;
   *   · uno DESCONOCIDO → `permanent`, `UNKNOWN`, «error interno del servicio». Su texto va al log, con el `id` de la petición.
   * Lo desconocido es `permanent` y no «retryable acotado»: el presupuesto de 04-errors.md §2.1 se cuenta por entregas de un
   * consumidor, y una llamada no tiene número de entrega. `poison` no viaja.
   */
  #errorOf(op: string, event: FluxEvent, e: unknown): RpcErrorData {
    const h = this.#h;
    const codigoValido = (c: unknown): c is string =>
      typeof c === "string" && c.length <= RPC_ERROR_CODE_MAX && RPC_ERROR_CODE_PATTERN.test(c);
    const mensaje = (m: string, porDefecto: string) => truncar(m.length > 0 ? m : porDefecto, RPC_ERROR_MESSAGE_MAX);
    const reservado = event.dataclassification === "confidential" || event.dataclassification === "restricted";

    if (e instanceof RetryableError || e instanceof PermanentError || e instanceof PoisonError) {
      const clase: "retryable" | "permanent" = e instanceof RetryableError ? "retryable" : "permanent";
      const porDefecto = clase === "retryable" ? "RETRYABLE" : e instanceof PoisonError ? "POISON" : "PERMANENT";
      const dado = (e.opts as { code?: unknown }).code;
      let code = porDefecto;
      if (dado !== undefined) {
        if (codigoValido(dado)) code = dado;
        else {
          h.options.logger?.warn(
            `[flux] el handler de "${op}" lanzó un error con el código ${JSON.stringify(String(dado).slice(0, 70))}, que no cumple ` +
              `^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*$ (04-errors.md §7): se responde con ${porDefecto}. Petición ${event.id}.`,
          );
        }
      }
      const retryAfterMs = (e.opts as { retryAfterMs?: unknown }).retryAfterMs;
      return {
        class: clase,
        code,
        message: mensaje(e.message, "el servicio rechazó la petición"),
        ...(clase === "retryable" &&
          typeof retryAfterMs === "number" && Number.isInteger(retryAfterMs) && retryAfterMs >= 0 && { retryAfterMs }),
      };
    }

    const { classification: c, recognized } = h.classify(e);
    if (recognized) {
      const clase: "retryable" | "permanent" = c.class === ErrorClass.RETRYABLE ? "retryable" : "permanent";
      h.options.logger?.error(
        `[flux] el handler de "${op}" falló con ${c.code} (${clase}) en la petición ${event.id}` +
          `${reservado ? "" : `: ${e instanceof Error ? e.message : String(e)}`}`,
      );
      return {
        class: clase,
        code: codigoValido(c.code) ? c.code : clase === "retryable" ? "RETRYABLE" : "PERMANENT",
        message: clase === "retryable" ? "dependencia no disponible" : "una dependencia rechazó la operación",
        ...(clase === "retryable" && c.retryAfterMs !== undefined && Number.isInteger(c.retryAfterMs) && c.retryAfterMs >= 0 && { retryAfterMs: c.retryAfterMs }),
      };
    }

    // DESCONOCIDO. La política configurable de 04-errors.md §2.1 se aplica así: `permanent` y `retryable-bounded` dan `permanent`;
    // `retryable` da `retryable`. El texto de la excepción puede llevar valores de `data`, rutas del servidor o datos personales.
    const clase: "retryable" | "permanent" =
      c.class === ErrorClass.RETRYABLE && c.maxAttempts === undefined ? "retryable" : "permanent";
    h.options.logger?.error(
      `[flux] el handler de "${op}" lanzó un error sin clasificar en la petición ${event.id}: ` +
        `${e instanceof Error ? (reservado ? e.name : `${e.name}: ${e.message}`) : "una excepción que no es un Error"}` +
        `${reservado ? " (el mensaje se omite: la petición es confidential o restricted)" : ""}. ` +
        `Quien llamó recibió \`${clase}\` UNKNOWN con un mensaje genérico.`,
    );
    return { class: clase, code: "UNKNOWN", message: "error interno del servicio" };
  }

  /**
   * Contesta con un error `com.flux.rpc.error.v1`, **siempre**: callar dejaría a quien llama esperando su timeout para un
   * fallo que no se arregla esperando (§5.2). `request` es el sobre de la petición, o `null` si no se pudo leer: entonces no hay
   * `correlationid` ni `tenantid` que heredar, y el error lleva `correlationid` = su propio `id`, `tenantid` = `system` y
   * `causationid` = el `id` de la petición solo si es una cadena válida (§5.2, decidido provisionalmente).
   */
  async #sendError(
    replySubject: string,
    r: ResponderState,
    request: FluxEvent | null,
    idDeLaPeticion: string | undefined,
    error: RpcErrorData,
  ): Promise<void> {
    const h = this.#h;
    const op = r.operation;
    const id = uuidv7();
    const truncado: RpcErrorData = {
      ...error,
      code: error.code,
      message: truncar(error.message || "el servicio rechazó la petición", RPC_ERROR_MESSAGE_MAX),
    };
    // Sin sobre que leer no hay nada que heredar (§5.2): `correlationid` propio, `system`, y el `id` de la petición solo si es válido.
    const ref: RpcRequestRef = request
      ? refOf(op, request)
      : { operation: op, id: idDeLaPeticion ?? id, correlationid: id, tenantid: "system" };
    let evento = buildRpcError({
      request: ref,
      id,
      source: h.source,
      time: aIso(h.now()),
      producerversion: h.options.version,
      error: truncado,
      ...(request ? await this.#replyTraceparent(request) : {}),
    }) as FluxEvent<RpcErrorData> & { causationid?: string };
    if (!request && idDeLaPeticion === undefined) {
      const { causationid: _sinId, ...resto } = evento;
      evento = resto as typeof evento;
    }
    let bytes: Uint8Array;
    try {
      bytes = this.#encode(evento, "respuesta", op);
    } catch (e) {
      h.options.logger?.error(`[flux] no se pudo contestar con un error a "${op}": ${e instanceof Error ? e.message : String(e)}`);
      return;
    }
    this.#publish(replySubject, bytes, op);
  }

  #publish(replySubject: string, bytes: Uint8Array, op: string): void {
    try {
      this.#h.nc.publish(replySubject, bytes);
    } catch (e) {
      // La conexión se cerró (o se está drenando) con la petición en la mano: quien llamó verá un timeout.
      this.#h.options.logger?.error(
        `[flux] no se pudo publicar la respuesta de "${op}": ${e instanceof Error ? e.message : String(e)}. Quien llamó verá un timeout.`,
      );
    }
  }

  #poison(op: string, error: PoisonError, log: string): void {
    this.#h.options.logger?.error(`[flux] POISON en "${op}": ${log}`);
    // `raw` va vacío a propósito: una petición PUEDE llevar datos personales (11-request-reply.md §3.6).
    this.#h.options.onPoison?.({ subject: `rpc.${op}`, error, raw: new Uint8Array(0) });
  }

  // ─── sobre, tamaño, esquema ────────────────────────────────────────────────

  /**
   * Los bytes del mensaje, firmado si procede, y su tamaño COMPROBADO antes de publicar: el mínimo entre 1 MiB y el
   * `max_payload` que anuncia el broker, **las cabeceras incluidas** (no usamos ninguna). Si no cabe, `RPC_MESSAGE_TOO_LARGE`.
   */
  #encode(event: FluxEvent, que: "petición" | "respuesta", operation: string): Uint8Array {
    const limite = Math.min(MAX_MESSAGE_BYTES, this.#h.nc.info?.max_payload ?? MAX_MESSAGE_BYTES);
    const sinFirma = encodeRpcMessage(event);
    if (sinFirma.byteLength > limite) throw new RpcMessageTooLargeError(sinFirma.byteLength, limite, que, operation);
    const signer = this.#h.signer();
    if (!signer) return sinFirma;
    let firmado: FluxEvent;
    try {
      firmado = signer.sign(event);
    } catch (e) {
      // El firmante serializa con el límite de 1 MiB de los eventos: un mensaje en el borde, con la firma encima, puede pasarse.
      if (e instanceof Error && e.name === "EnvelopeError") {
        throw new RpcMessageTooLargeError(sinFirma.byteLength + 200, limite, que, operation);
      }
      throw e;
    }
    const bytes = encodeRpcMessage(firmado);
    if (bytes.byteLength > limite) throw new RpcMessageTooLargeError(bytes.byteLength, limite, que, operation);
    return bytes;
  }

  /** El `dataschema`: índice `rpc` del bundle (sin red) o `schemaBaseUrl` (`<major>.0.0`). Nunca por red. */
  #schemaFor(operation: string, lado: "request" | "reply"): string {
    const o = this.#h.options;
    const delBundle = o.validation?.bundle ? rpcSchemaUriFor(o.validation.bundle, operation, lado) : undefined;
    if (delBundle) return delBundle;
    // Con L3 activa y un bundle, una operación que NO está en su índice se dice como lo que es —`SCHEMA_NOT_FOUND`, que lanza
    // el validador al no encontrar la URI derivada—, y no como «no hay dataschema»: el bundle ya es la fuente de los esquemas.
    if (!o.schemaBaseUrl && o.validation?.bundle && (o.validation.mode ?? "off") !== "off") return rpcDataschema(operation, lado);
    if (!o.schemaBaseUrl) {
      throw new Error(
        `no hay dataschema para la operación "${operation}" (${lado}). Pasa un bundle con el índice \`rpc\` en ` +
          `\`validation.bundle\` (connect()), o fija \`schemaBaseUrl\` para derivar el \`<major>.0.0\`: ni el nombre de la ` +
          `operación ni nada de lo que hay en el sobre permiten adivinar el host de tus esquemas.`,
      );
    }
    return rpcDataschema(operation, lado, o.schemaBaseUrl);
  }

  async #traceparent(): Promise<{ traceparent?: string }> {
    const tp = await activeTraceparent();
    return tp ? { traceparent: tp } : {};
  }
}

/**
 * ¿Es el error que contesta quien responde a una petición que **no pudo leer** (11-request-reply.md §5.2)? Sin sobre no hay
 * `correlationid` ni `tenantid` que heredar: el error lleva `correlationid` igual a **su propio `id`** y `tenantid: system`.
 * Es la única respuesta a la que quien llama no le exige heredar los dos (§4.3, comprobación 4): la forma es estrecha a
 * propósito —un `tenantid: system` cualquiera, o un `correlationid` cualquiera, no basta—, y la comprobación 3 (`causationid`
 * = el `id` de la petición) sigue en pie y es la que la atribuye.
 */
export function esErrorDeUnaPeticionIlegible(event: FluxEvent, esError: boolean): boolean {
  return esError && event.tenantid === "system" && event.correlationid === event.id;
}

/** Lo que la respuesta hereda de un sobre de petición ya leído. */
function refOf(operation: string, e: FluxEvent): RpcRequestRef {
  return {
    operation,
    id: e.id,
    correlationid: e.correlationid,
    tenantid: e.tenantid,
    ...(e.subject !== undefined && { subject: e.subject }),
    ...(e.partitionkey !== undefined && { partitionkey: e.partitionkey }),
  };
}

/**
 * `INVALID_SCHEMA` de una petición o de una respuesta. El mensaje DICE cuál: quien llama recibe el mismo `INVALID_SCHEMA` en
 * los dos casos, y debe saber a quién arreglar (11-request-reply.md §5.2). Cita rutas, nunca valores.
 */
export class RpcSchemaError extends SchemaValidationError {
  constructor(
    readonly lado: "petición" | "respuesta",
    operation: string,
    dataschema: string,
    errors: string[],
  ) {
    super(operation, dataschema, errors);
    this.message =
      `la ${lado} de "${operation}" no cumple ${dataschema}: ${errors.join(", ")}. ` +
      (lado === "petición"
        ? "El fallo es de quien llama: arregla los datos de la petición, o actualiza su bundle de esquemas."
        : "El fallo es de quien responde (o los dos tienen bundles de esquemas distintos): el orden de despliegue es primero quien responde.");
    this.name = "RpcSchemaError";
  }
}
