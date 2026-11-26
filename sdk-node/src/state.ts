/**
 * Estado replicado — la extensión opcional `state`.
 * Contrato normativo: specification/10-state.md, specification/04-errors.md §6.
 *
 * Un EVENTO dice qué pasó; el ESTADO es el valor ACTUAL de un agregado, que su dueño publica
 * y los demás observan. Sustituye a `GET /clientes/123`, a la carga inicial y al polling de
 * datos. Este fichero tiene dos mitades que conviene no mezclar:
 *
 *   1. **Funciones puras y deterministas** (sin reloj, sin UUID, sin red): el subject, el sobre
 *      de un `put`, la lápida y el evento de DLQ de estado. Producen los MISMOS BYTES en todos
 *      los SDKs (10-state.md §2.6; arnés cruzado `state-build`, `state-tombstone`,
 *      `state-subject` y `state-dlq`). Lo que las alimenta —`id`, `time`— se pasa DE FUERA:
 *      si la función los inventara, dos ejecuciones no serían comparables.
 *   2. **`StateClient`** (`bus.state`): `put`, `delete`, `get` y `watch`, que usan las
 *      anteriores y hablan con el broker.
 *
 * Regla de diseño heredada de client.ts: ningún tipo de NATS en la API pública. `StateHost`
 * sí los lleva, pero es interno y no se exporta desde index.ts.
 */

import type { Msg, NatsConnection } from "@nats-io/transport-node";
import {
  AckPolicy,
  DeliverPolicy,
  DiscardPolicy,
  ReplayPolicy,
  RetentionPolicy,
  StorageType,
  type ConsumerConfig,
  type JetStreamClient,
  type ConsumerMessages,
  type JetStreamManager,
  type JsMsg,
} from "@nats-io/jetstream";
import { v7 as uuidv7 } from "uuid";

import type { ConnectOptions } from "./client.js";
import { ConsumerConfigMismatchError, TenantIsolationError } from "./client-errors.js";
import { contextFromEvent, currentContext, runWithContext, activeTraceparent } from "./context.js";
import {
  InvalidEnvelopeInputError,
  exigirAtributos,
  parseEvent,
  serialize,
  tiempoCanonico,
  type DataClassification,
  type FluxEvent,
} from "./envelope.js";
import { truncateUtf8Bytes } from "./text.js";
import { ErrorClass, PoisonError, isClassifiedError, type Classification } from "./errors.js";
import type { MetricsSink } from "./metrics.js";
import {
  CONSUMER_DEFAULTS,
  STREAM_DEFAULTS,
  InvalidServiceNameError,
  SERVICE_PATTERN,
  assertNotReservedDomain,
  dlqStreamName,
  nanos,
} from "./protocol.js";
import {
  NotProvisionedError,
  PermissionDeniedError,
  isNoResponders,
  isNotFound,
  permissionViolation,
} from "./security.js";
import type { Signer } from "./signing.js";
import {
  InvalidDataClassificationError,
  InvalidStateContractError,
  InvalidStateKeyError,
  InvalidTenantForStateError,
  MissingAggregateVersionError,
  RestrictedStateEntryError,
  StateConflictError,
  StateSubjectMismatchError,
} from "./state-errors.js";
import { SchemaNotFoundError, SchemaValidationError, stateSchemaUriFor } from "./validation.js";

// ═════════════════════════════════════════════════════════════════════════════════════════
// 1. Constantes y reglas del protocolo — protocol.json → `state`
// ═════════════════════════════════════════════════════════════════════════════════════════

/**
 * `^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`: un único token de NATS. Coincidencia COMPLETA: en JS el
 * `$` sin el modificador `m` no acepta un `\n` final, a diferencia de Python (`re.match`) y PHP
 * (`preg_match` sin `D`), donde `"cli-1\n"` pasaría el patrón (10-state.md §2.3).
 */
export const STATE_KEY_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;
/** `^[a-z0-9]+(-[a-z0-9]+)*$` y, aparte (un patrón puro, sin lookahead), 64 como máximo. */
export const STATE_TENANT_PATTERN = /^[a-z0-9]+(-[a-z0-9]+)*$/;
/** NATS limita los nombres de consumidor a 255 y el durable de un observador lleva el tenant (A-42). */
export const STATE_TENANT_MAX_LENGTH = 64;
/** `<dominio>.<agregado>.v<major>`: el subject sin `state.`, sin tenant y sin clave. */
export const STATE_CONTRACT_PATTERN =
  /^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]{0,3}$/;
/** 2^53 − 1: el mayor entero que Node, y todo lo que use `double`, representa sin perder precisión. */
export const AGGREGATE_VERSION_MAX = Number.MAX_SAFE_INTEGER;
/** Intentos de un `put`/`delete` ante un conflicto de secuencia (10-state.md §3.2, paso 4). */
export const STATE_MAX_ATTEMPTS = 3;
/**
 * Los DOS códigos del conflicto de secuencia (A-33, A-49): 10071 «wrong last sequence: N» (un
 * nodo, o un cluster sin carrera) y 10164 «wrong last sequence», SIN número (cluster, bajo
 * carrera: 210 de 210 rechazos de 8 escritores). Un SDK que solo reconozca el 10071 pasa sus
 * pruebas contra un nodo y, en producción, trata un conflicto corriente como un error
 * desconocido y no reintenta.
 */
export const STATE_CONFLICT_CODES: readonly number[] = [10071, 10164];
/** El `dataschema` FIJO de toda lápida, de cualquier contrato. */
export const TOMBSTONE_DATASCHEMA = "https://schemas.internal/system/state/eliminado/1.0.0.json";
/** Base de los esquemas del repo: `schemas/state/<dom>/<agr>/<semver>.json` cuelga de aquí. */
export const STATE_SCHEMA_BASE = "https://schemas.internal";
/** Clasificaciones admitidas en estado. `restricted` NO: 24 h de retención no caben en un almacén sin caducidad. */
export const STATE_CLASSIFICATIONS: readonly DataClassification[] = ["public", "internal", "confidential"];
/** Un `put` sin `classification` usa ESTA, no la de la conexión (10-state.md §3.6). */
export const STATE_DEFAULT_CLASSIFICATION: DataClassification = "confidential";
/**
 * Lo que sigue al código en el `dlqerror` de un error que **no es de flux** (10-state.md §6): el
 * mensaje de un driver de base de datos puede citar la fila entera, y la DLQ guarda 90 días. La
 * misma frase que Rust y que `protocol.json` → `state.dlq.dlqerrorFixedPhrase`.
 */
export const UNCLASSIFIED_DLQ_ERROR =
  "error sin clasificar de la aplicación; el mensaje se omite para no copiar datos a la DLQ (10-state.md §6)";

// ═════════════════════════════════════════════════════════════════════════════════════════
// 2. Validación — se hace SIEMPRE, antes de abrir ninguna petición
// ═════════════════════════════════════════════════════════════════════════════════════════

export interface ParsedStateContract {
  readonly contract: string;
  readonly domain: string;
  readonly aggregate: string;
  readonly major: number;
}

/**
 * Valida un contrato. Primero el dominio reservado (`RESERVED_DOMAIN`), después la forma
 * (`INVALID_STATE_CONTRACT`): el orden de 10-state.md §3.6.
 */
export function parseStateContract(contract: unknown): ParsedStateContract {
  if (typeof contract !== "string") throw new InvalidStateContractError(contract, "debe ser una cadena");
  assertNotReservedDomain(contract, "contrato");
  if (!STATE_CONTRACT_PATTERN.test(contract)) {
    const n = contract.split(".").length;
    throw new InvalidStateContractError(
      contract,
      n !== 3 ? `debe tener exactamente 3 tokens, tiene ${n}` : "formato esperado <dominio>.<agregado>.v<major> (la versión mayor, de 1 a 4 dígitos: v1…v9999)",
    );
  }
  const [domain, aggregate, v] = contract.split(".");
  return { contract, domain, aggregate, major: Number(v.slice(1)) };
}

export function assertStateKey(key: unknown): asserts key is string {
  if (typeof key !== "string" || !STATE_KEY_PATTERN.test(key)) throw new InvalidStateKeyError(key);
}

export function assertStateTenant(tenant: unknown): asserts tenant is string {
  if (typeof tenant !== "string" || tenant.length > STATE_TENANT_MAX_LENGTH || !STATE_TENANT_PATTERN.test(tenant)) {
    throw new InvalidTenantForStateError(tenant);
  }
}

/** ¿Es un `aggregateVersion` válido? Entero ≥ 1 y ≤ 2^53 − 1. En JS `7.0` ES el entero 7. */
export function isValidAggregateVersion(v: unknown): v is number {
  return typeof v === "number" && Number.isSafeInteger(v) && v >= 1;
}

/**
 * Lee el `aggregateVersion` de un `data` o lanza `MISSING_AGGREGATE_VERSION`. El nombre cubre
 * todo lo que no sea un entero ≥ 1: ausente, null, una cadena, un decimal, 0, un negativo o uno
 * mayor que 2^53 − 1. Se comprueba SIEMPRE, con o sin L3: es una regla del protocolo.
 */
export function aggregateVersionOf(data: unknown): number {
  const v =
    data !== null && typeof data === "object" && !Array.isArray(data)
      ? (data as Record<string, unknown>).aggregateVersion
      : undefined;
  if (!isValidAggregateVersion(v)) throw new MissingAggregateVersionError(v);
  return v;
}

/** `undefined` → `confidential`; `restricted` o una fuera del enum → `INVALID_DATACLASSIFICATION`. */
export function assertStateClassification(c: unknown): DataClassification {
  if (c === undefined) return STATE_DEFAULT_CLASSIFICATION;
  if (!STATE_CLASSIFICATIONS.includes(c as DataClassification)) throw new InvalidDataClassificationError(c);
  return c as DataClassification;
}

/**
 * Lo que comprueba todo constructor de estado además de la forma del contrato, la clave y el tenant: que los atributos que el SDK
 * rellena no lleguen vacíos y que `time` sea un instante (se normaliza a la forma canónica). El mismo código que daría el
 * parser ante el mismo defecto: el conjunto de estado (04-errors.md §6) es cerrado y ninguno de sus siete encaja.
 */
function entradaDelSobre(
  input: { id: string; source: string; correlationid: string; producerversion: string; time: string },
): string {
  const rechaza = (m: string, c: "MISSING_REQUIRED_ATTRIBUTE" | "MISSING_REQUIRED_EXTENSION") => new InvalidEnvelopeInputError(m, c);
  exigirAtributos(
    [
      ["id", input.id],
      ["source", input.source],
      ["correlationid", input.correlationid, true],
      ["producerversion", input.producerversion, true],
    ],
    rechaza,
  );
  return tiempoCanonico(input.time, (m) => new InvalidEnvelopeInputError(m, "WRONG_ATTRIBUTE_TYPE"));
}

// ═════════════════════════════════════════════════════════════════════════════════════════
// 3. Nombres — 02-naming.md, 10-state.md §2
// ═════════════════════════════════════════════════════════════════════════════════════════

/** `STATE_CLIENTES`. NATS no admite puntos en nombres de stream. */
export function stateStreamName(domain: string): string {
  return `STATE_${domain.replace(/-/g, "_").toUpperCase()}`;
}

/**
 * `state.<dominio>.<agregado>.v<major>.<tenant>.<clave>`: el subject de NATS de una entrada.
 * Seis tokens, siempre. **No** es el atributo `subject` de CloudEvents, que aquí vale la clave.
 *
 * Valida en el orden de 10-state.md §3.6 —dominio reservado, forma del contrato, clave, tenant—
 * y lanza con el código estable de cada rechazo.
 */
export function stateSubject(contract: string, tenantId: string, key: string): string {
  const c = parseStateContract(contract);
  assertStateKey(key);
  assertStateTenant(tenantId);
  return `state.${c.contract}.${tenantId}.${key}`;
}

/** `com.flux.state.<dominio>.<agregado>.v<major>`. */
export function stateType(contract: string): string {
  const { domain, aggregate, major } = parseStateContract(contract);
  return `com.flux.state.${domain}.${aggregate}.v${major}`;
}

/** `com.flux.state.<dominio>.<agregado>.deleted.v<major>`. */
export function stateTombstoneType(contract: string): string {
  const { domain, aggregate, major } = parseStateContract(contract);
  return `com.flux.state.${domain}.${aggregate}.deleted.v${major}`;
}

/** `https://schemas.internal/state/<dominio>/<agregado>/<major>.0.0.json` (o con otra base). */
export function stateDataschema(contract: string, base: string = STATE_SCHEMA_BASE): string {
  const { domain, aggregate, major } = parseStateContract(contract);
  return `${base.replace(/\/$/, "")}/state/${domain}/${aggregate}/${major}.0.0.json`;
}

/**
 * `<servicio>__state_<dom>_<agr>_v<M>` y, si filtra por tenant, el sufijo `__<tenant>`. Los
 * puntos y los guiones pasan a `_`. El tenant va en el nombre porque **un durable compartido
 * reparte los mensajes entre quienes lo usan** (A-47): dos instancias del mismo servicio, una
 * por tenant, con un durable sin sufijo, se quitarían la mitad de la foto cada una.
 */
export function stateDurableName(service: string, contract: string, tenantId?: string): string {
  if (!SERVICE_PATTERN.test(service)) throw new InvalidServiceNameError(service);
  const { domain, aggregate, major } = parseStateContract(contract);
  // El servicio conserva sus guiones (`tienda-web__…`); lo que viene después, no.
  const base = `${service}__state_${domain.replace(/-/g, "_")}_${aggregate.replace(/-/g, "_")}_v${major}`;
  if (tenantId === undefined) return base;
  assertStateTenant(tenantId);
  return `${base}__${tenantId.replace(/-/g, "_")}`;
}

/** `state.<dom>.<agr>.v<M>.<tenant>.*`, o `….*.*` sin tenant. */
export function stateFilterSubject(contract: string, tenantId?: string): string {
  const c = parseStateContract(contract);
  if (tenantId !== undefined) assertStateTenant(tenantId);
  return `state.${c.contract}.${tenantId ?? "*"}.*`;
}

/** `dlq.state.<dom>.<agr>.v<M>`: SIN tenant ni clave, para que no se multiplique por ellos. */
export function stateDlqSubject(contract: string): string {
  return `dlq.state.${parseStateContract(contract).contract}`;
}

// ═════════════════════════════════════════════════════════════════════════════════════════
// 4. El sobre — funciones PURAS (10-state.md §2.5, §2.6, §4, §6)
// ═════════════════════════════════════════════════════════════════════════════════════════

/** Una entrada de estado, tal como la ve quien la lee (`get`, handler de `watch`). */
export interface StateEntry<T = Record<string, unknown>> {
  /** La clave. */
  readonly key: string;
  /**
   * El tenant del subject. No estaba en el plan y es necesario: un `watch` sin tenant recibe
   * `cli-1` de varios, y la clave sola mezclaría los tenants en la copia local.
   */
  readonly tenantId: string;
  /** `true` si es una lápida. */
  readonly deleted: boolean;
  /** El `data` del evento; `null` si `deleted`. */
  readonly data: T | null;
  /** La versión del valor (o la de la lápida). */
  readonly aggregateVersion: number;
  /** La secuencia del stream: dice QUÉ mensaje es; no ordena versiones ni se compara entre contratos. */
  readonly revision: number;
}

export interface StateEventInput<T extends object = Record<string, unknown>> {
  /** `clientes.cliente.v1`. */
  contract: string;
  tenantId: string;
  key: string;
  id: string;
  source: string;
  /** ISO-8601 con exactamente 3 decimales y sufijo `Z`. */
  time: string;
  correlationid: string;
  producerversion: string;
  /** Por defecto `confidential`. */
  classification?: DataClassification;
  data: T;
  /** Por defecto, `https://schemas.internal/state/<dom>/<agr>/<major>.0.0.json`. */
  dataschema?: string;
  causationid?: string;
  traceparent?: string;
  tracestate?: string;
}

/**
 * El sobre de un `put`: un CloudEvent normal, con `subject` = la clave, `tenantid` = el token de
 * tenant del subject de NATS, `partitionkey` = la clave, y `data` el ÚLTIMO atributo.
 *
 * **Pura y determinista**: `id` y `time` los da quien llama. Valida lo mismo que `put`
 * (§3.6, salvo L3), así que lo que construye es una entrada legal.
 */
export function buildStateEvent<T extends object>(input: StateEventInput<T>): FluxEvent<T> {
  const c = parseStateContract(input.contract);
  assertStateKey(input.key);
  assertStateTenant(input.tenantId);
  aggregateVersionOf(input.data);
  const classification = assertStateClassification(input.classification);
  const time = entradaDelSobre(input);
  return {
    specversion: "1.0",
    id: input.id,
    source: input.source,
    type: stateType(c.contract),
    time,
    datacontenttype: "application/json",
    dataschema: input.dataschema ?? stateDataschema(c.contract),
    subject: input.key,
    correlationid: input.correlationid,
    tenantid: input.tenantId,
    producerversion: input.producerversion,
    dataclassification: classification,
    ...(input.causationid !== undefined && { causationid: input.causationid }),
    partitionkey: input.key,
    ...(input.traceparent !== undefined && { traceparent: input.traceparent }),
    ...(input.tracestate !== undefined && { tracestate: input.tracestate }),
    data: input.data,
  };
}

export interface StateTombstoneInput {
  contract: string;
  tenantId: string;
  key: string;
  id: string;
  source: string;
  time: string;
  correlationid: string;
  producerversion: string;
  /** La versión DE LA LÁPIDA: debe superar la del valor que borra. */
  aggregateVersion: number;
  causationid?: string;
  traceparent?: string;
  tracestate?: string;
}

/**
 * La lápida de un `delete`: el mismo subject de NATS que el valor, `type` con `.deleted.`,
 * `dataschema` FIJO, `dataclassification: "internal"` y `data: { aggregateVersion }` y nada más.
 * Al entrar, el valor anterior SALE del stream (A-34). Lleva versión porque `put` la lee para
 * decidir si una escritura nueva es `stale`.
 */
export function buildStateTombstone(input: StateTombstoneInput): FluxEvent<{ aggregateVersion: number }> {
  const c = parseStateContract(input.contract);
  assertStateKey(input.key);
  assertStateTenant(input.tenantId);
  const aggregateVersion = aggregateVersionOf({ aggregateVersion: input.aggregateVersion });
  const time = entradaDelSobre(input);
  return {
    specversion: "1.0",
    id: input.id,
    source: input.source,
    type: stateTombstoneType(c.contract),
    time,
    datacontenttype: "application/json",
    dataschema: TOMBSTONE_DATASCHEMA,
    subject: input.key,
    correlationid: input.correlationid,
    tenantid: input.tenantId,
    producerversion: input.producerversion,
    dataclassification: "internal",
    ...(input.causationid !== undefined && { causationid: input.causationid }),
    partitionkey: input.key,
    ...(input.traceparent !== undefined && { traceparent: input.traceparent }),
    ...(input.tracestate !== undefined && { tracestate: input.tracestate }),
    data: { aggregateVersion },
  };
}

export interface StateDlqInfo {
  /** La clave del SUBJECT de NATS —la que decidió el broker—, no la del sobre. */
  key: string;
  /** La secuencia del stream del mensaje que falló. */
  revision: number;
  reason: "retryable" | "permanent" | "poison";
  attempts: number;
  consumer: string;
  error: string;
  /** Lo da quien llama: si lo pusiera esta función, los bytes no serían comparables. */
  dlqtime: string;
}

/**
 * El evento de DLQ de estado: el sobre ORIGINAL con las extensiones `dlq*` y **sin su dato**:
 * `data` pasa a `{ key, revision }`.
 *
 *   · La DLQ guarda 90 días: un dato personal que el dueño ya borró seguiría vivo ahí tres
 *     meses y el derecho de supresión no se cumpliría (10-state.md §6).
 *   · **No conserva `signkeyid` ni `signature`**: firman un `data` que ya no está, y una firma
 *     que no verifica sobre un sobre que parece el original es una afirmación falsa (07-signing.md §5).
 *   · `data` sigue siendo el ÚLTIMO atributo, con las `dlq*` justo antes.
 */
export function toStateDlqEvent(event: FluxEvent, info: StateDlqInfo): FluxEvent<{ key: string; revision: number }> {
  const {
    data: _data,
    signkeyid: _k,
    signature: _s,
    dlqreason: _r,
    dlqattempts: _a,
    dlqconsumer: _c,
    dlqerror: _e,
    dlqtime: _t,
    ...rest
  } = event as FluxEvent & { signkeyid?: string; signature?: string };
  return {
    ...rest,
    dlqreason: info.reason,
    dlqattempts: info.attempts,
    dlqconsumer: info.consumer,
    // 1024 BYTES UTF-8 sin partir un carácter (04-errors.md §3).
    dlqerror: truncateUtf8Bytes(info.error, 1024),
    dlqtime: tiempoCanonico(info.dlqtime, (m) => new InvalidEnvelopeInputError(m, "WRONG_ATTRIBUTE_TYPE")),
    data: { key: info.key, revision: info.revision },
  };
}

/** Lo que el arnés cruzado le pasa a `state-dlq`: el original (entrada de `state-build`), la revisión y los `dlq*`. */
export interface StateDlqBuildInput {
  original: StateEventInput;
  revision: number;
  dlq: Omit<StateDlqInfo, "key" | "revision">;
}

/** `toStateDlqEvent` sobre el sobre que `buildStateEvent` construye de `original`. Pura. */
export function buildStateDlqEvent(input: StateDlqBuildInput): FluxEvent<{ key: string; revision: number }> {
  return toStateDlqEvent(buildStateEvent(input.original), {
    ...input.dlq,
    key: input.original.key,
    revision: input.revision,
  });
}

/**
 * Un mensaje ILEGIBLE (JSON roto, sin sobre) no se copia a la DLQ de estado: no se guarda su
 * cuerpo en ninguna forma —ni los `rawBase64` de los eventos—, porque podría contener el dato.
 * No hay sobre original que conservar, así que se sintetiza uno con lo único fiable, **el
 * subject** (10-state.md §6). `id` y `time` los da quien llama.
 */
export function buildUnreadableStateDlqEvent(input: {
  contract: string;
  tenantId: string;
  key: string;
  revision: number;
  /** Servicio que consume: su `source` y su `producerversion`. */
  source: string;
  producerversion: string;
  id: string;
  time: string;
  dataschema?: string;
  dlq: Omit<StateDlqInfo, "key" | "revision">;
}): FluxEvent<{ key: string; revision: number }> {
  const c = parseStateContract(input.contract);
  const time = entradaDelSobre({ ...input, correlationid: input.id });
  const synth: FluxEvent = {
    specversion: "1.0",
    id: input.id,
    source: input.source,
    type: stateType(c.contract),
    time,
    datacontenttype: "application/json",
    dataschema: input.dataschema ?? stateDataschema(c.contract),
    subject: input.key,
    correlationid: input.id,
    tenantid: input.tenantId,
    producerversion: input.producerversion,
    dataclassification: "internal",
    partitionkey: input.key,
    data: {},
  };
  return toStateDlqEvent(synth, { ...input.dlq, key: input.key, revision: input.revision });
}

// ═════════════════════════════════════════════════════════════════════════════════════════
// 5. API pública de `bus.state`
// ═════════════════════════════════════════════════════════════════════════════════════════

export interface StatePutOptions {
  /** Tenant. Si no, el del evento en curso y, si no, el de `connect()`. Sin ninguno: `INVALID_TENANT_FOR_STATE`. */
  tenantId?: string;
  /** `public`, `internal` o `confidential` (el default: NO el de la conexión). `restricted` se rechaza. */
  classification?: DataClassification;
  /** Instante en que ocurrió el hecho, si no es «ahora». */
  time?: Date | string;
}

export interface StateDeleteOptions {
  /** La versión DE LA LÁPIDA. Obligatoria: debe superar la del valor que borra. */
  aggregateVersion: number;
  tenantId?: string;
  time?: Date | string;
}

export interface StateGetOptions {
  tenantId?: string;
}

export interface StateWatchOptions {
  /**
   * Filtra por este tenant (y el durable lleva su sufijo). Si no, el de `connect()` —que
   * **siempre** filtra, también si vale `system`: aquí es un tenant como cualquier otro—. Sin
   * ninguno se observan todos, salvo con `tenantIsolation: "strict"`.
   *
   * **Nunca el del evento en curso** (a diferencia de `put`, `delete` y `get`): un `watch` no corre
   * dentro de un handler, se abre al arrancar y vive tanto como el servicio, y heredar el tenant
   * del evento que se esté procesando haría que su filtro dependiera de qué evento llegó primero
   * (10-state.md §2.4).
   *
   * **Un tenant por `watch`**, y cada llamada es un durable. Un servicio que observa N tenants
   * llama N veces, y cada llamada recibe su `onSynced`; combinarlas es de la aplicación
   * (10-state.md §5.5).
   */
  tenantId?: string;
  /**
   * Se invoca UNA vez por `watch()` —no una por reconexión, ni de nuevo tras un `flux state
   * resync`— cuando el handler ya ha procesado todo lo que había pendiente al abrirlo: la señal
   * de «mi réplica local ya puede contestar lecturas». Con el contrato vacío (o el durable al
   * día), de inmediato y antes de entregar nada. **No** promete que no lleguen entradas después.
   */
  onSynced?: () => void;
}

/** Resultado de `put` y `delete`. No hay un tercer `outcome`: un conflicto agotado es un error. */
export interface StateWriteResult {
  applied: boolean;
  outcome: "ok" | "stale";
  /** `ok`: la secuencia del mensaje nuevo. `stale`: la del mensaje ACTUAL, el que la hizo obsoleta. */
  revision: number;
}

export interface StateHandlerContext {
  /** Confirmar es devolver del handler sin error; esto no hace nada. */
  ack(): void;
  /** Nº de entrega, empezando en 1. */
  readonly attempt: number;
  readonly maxAttempts: number;
  /** El subject de NATS de la entrada (`state.clientes.cliente.v1.acme.cli-1`). */
  readonly subject: string;
  /** El contrato observado (`clientes.cliente.v1`). */
  readonly contract: string;
}

export type StateHandler<T = Record<string, unknown>> = (
  entry: StateEntry<T>,
  ctx: StateHandlerContext,
) => void | Promise<void>;

export interface StateSubscription {
  readonly contract: string;
  readonly durable: string;
  /** El tenant por el que filtra, si filtra. */
  readonly tenantId?: string;
  unsubscribe(): Promise<void>;
}

// ═════════════════════════════════════════════════════════════════════════════════════════
// 6. El cliente
// ═════════════════════════════════════════════════════════════════════════════════════════

/**
 * Lo que `StateClient` necesita de `FluxBus`. Interno: lleva tipos de NATS y no se exporta desde
 * index.ts. Es una interfaz y no el bus entero para que los tests puedan darle conexiones falsas.
 */
export interface StateHost {
  readonly nc: NatsConnection;
  readonly js: JetStreamClient;
  readonly jsm: JetStreamManager;
  readonly options: ConnectOptions;
  readonly source: string;
  readonly metrics: MetricsSink;
  classify(e: unknown): Classification;
  /** L3 si está configurada. Se resuelve en `connect()`, de ahí que sea una función. */
  validator(): ((event: FluxEvent, subject: string) => void) | null;
  signer(): Signer | null;
  /** Verifica la firma. Lanza (`require`) o devuelve el código del fallo (`warn`), o `null`. */
  verify(event: FluxEvent): string | null;
  /** ¿Avisó el broker, de forma asíncrona, de que denegó este subject? Ver `FluxBus.#watchPermissionViolations`. */
  isDenied(subject: string): boolean;
  clearDenied(subject: string): void;
  /** Registra algo que `bus.close()` debe parar. Devuelve cómo quitarlo. */
  track(stop: () => void): () => void;
}

interface StoredState {
  seq: number;
  data: Uint8Array;
  /** El `id` del sobre, o el de la cabecera `Nats-Msg-Id` (el broker las conserva: A-32). */
  ids: string[];
  /** Su versión, si el cuerpo es legible y la tiene válida. */
  version?: number;
  unreadable: boolean;
}

const EMPTY = new Uint8Array(0);
const DECODER = new TextDecoder();
/** Códigos de `signing.poisonCodes`: la métrica de consumo los llama `invalid_signature`. */
const SIGNATURE_CODES = new Set(["MISSING_SIGNATURE", "UNKNOWN_SIGNING_KEY", "INVALID_SIGNATURE"]);
const DIRECT_GET_TIMEOUT_MS = 5_000;
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

const toIso = (t: Date | string | undefined): string =>
  t === undefined ? new Date().toISOString() : typeof t === "string" ? t : t.toISOString();

/** ¿`e` o su cadena de causas es un conflicto de secuencia (10071 o 10164)? */
export function isSequenceConflict(e: unknown): boolean {
  for (let x: unknown = e, i = 0; typeof x === "object" && x !== null && i < 6; i++) {
    const o = x as { code?: unknown; apiError?: () => { err_code?: unknown } };
    // `JetStreamApiError.code` ES el `err_code` (10071); su `status` es el 400 HTTP.
    if (typeof o.code === "number" && STATE_CONFLICT_CODES.includes(o.code)) return true;
    if (typeof o.apiError === "function") {
      const c = o.apiError().err_code;
      if (typeof c === "number" && STATE_CONFLICT_CODES.includes(c)) return true;
    }
    x = (x as { cause?: unknown }).cause;
  }
  return false;
}

/** Un patrón de subject de NATS (`*`, `>`) contra un subject concreto. */
function subjectMatches(pattern: string, subject: string): boolean {
  const p = pattern.split(".");
  const s = subject.split(".");
  for (let i = 0; i < p.length; i++) {
    if (p[i] === ">") return s.length > i;
    if (i >= s.length) return false;
    if (p[i] !== "*" && p[i] !== s[i]) return false;
  }
  return p.length === s.length;
}

/**
 * `flux_consumer_pending` de un contrato de estado es la SUMA de los durables del servicio, no el
 * valor del último sondeado (08-observability.md §2.4). Un `watch` por tenant crea un durable por
 * tenant, y todos publican las MISMAS etiquetas —el `consumer` va SIN el sufijo del tenant, o
 * cada tenant nuevo crearía series nuevas—: sin sumar, se pisarían entre sí.
 */
export class PendingSum {
  readonly #porSerie = new Map<string, Map<string, number>>();

  /** Anota el `pending` de un durable y devuelve la suma de la serie. */
  note(serie: string, durable: string, pending: number): number {
    let d = this.#porSerie.get(serie);
    if (!d) this.#porSerie.set(serie, (d = new Map()));
    d.set(durable, pending);
    return this.#suma(d);
  }

  /** Un durable deja de sumar (su `watch` paró). Devuelve la suma que queda, o `undefined` si no había nada. */
  drop(serie: string, durable: string): number | undefined {
    const d = this.#porSerie.get(serie);
    if (!d?.delete(durable)) return undefined;
    if (d.size === 0) this.#porSerie.delete(serie);
    return this.#suma(d);
  }

  #suma(d: Map<string, number>): number {
    let suma = 0;
    for (const v of d.values()) suma += v;
    return suma;
  }
}

/** Cuenta cuántas entregas de un `watch` siguen en vuelo y cuándo se alcanzó «lo que había al abrir». */
class SyncTracker {
  #done = false;
  #inflight = 0;
  #sawLast = false;
  constructor(private readonly fire: () => void) {}
  /** Al abrir: nada pendiente. */
  immediate(): void {
    this.#done = true;
    this.fire();
  }
  started(pending: unknown): void {
    this.#inflight++;
    // `pending` es el número que el broker pone en los metadatos de cada entrega: cuántas
    // quedan por entregar. 0 = esta es la última de la foto (A-35).
    if (pending === 0) this.#sawLast = true;
  }
  /** Con éxito o con error: una entrada RETRYABLE que falla NO retrasa `onSynced` (10-state.md §5.5). */
  finished(): void {
    this.#inflight--;
    if (!this.#done && this.#sawLast && this.#inflight === 0) {
      this.#done = true;
      this.fire();
    }
  }
}

interface WatchState<T> {
  contract: string;
  tenant: string | undefined;
  stream: string;
  durable: string;
  /** El durable SIN el sufijo de tenant: la etiqueta `consumer` de las métricas (08-observability.md §2.4). */
  consumerLabel: string;
  /** `state.<contrato>`: la etiqueta `subject` de las métricas. */
  subjectLabel: string;
  handler: StateHandler<T>;
  sync: SyncTracker;
}

export class StateClient {
  readonly #h: StateHost;
  readonly #pending = new PendingSum();
  readonly #dlqChecked = new Set<string>();

  constructor(host: StateHost) {
    this.#h = host;
  }

  // ─── escribir ──────────────────────────────────────────────────────────────

  /**
   * Escribe el valor actual de una clave. **Condicional**: no escribe una versión que no avanza
   * (`stale`, que NO es un error) y publica con `Nats-Expected-Last-Subject-Sequence`, de modo
   * que dos instancias del dueño no pisan un valor nuevo con uno viejo.
   *
   * `data` DEBE llevar `aggregateVersion` (entero ≥ 1): lo pone el dueño, no el SDK.
   */
  async put<T extends object>(
    contract: string,
    key: string,
    data: T,
    options: StatePutOptions = {},
  ): Promise<StateWriteResult> {
    // §3.6, en este orden y SIN red: el primero que falla gana.
    const c = parseStateContract(contract);
    assertStateKey(key);
    const tenant = this.#tenantFor(options.tenantId);
    const version = this.#countingInvalid(c.contract, () => aggregateVersionOf(data));
    const classification = assertStateClassification(options.classification);

    const id = uuidv7();
    const inherited = currentContext();
    const subject = stateSubject(c.contract, tenant, key);
    const event = buildStateEvent({
      contract: c.contract,
      tenantId: tenant,
      key,
      id,
      source: this.#h.source,
      time: toIso(options.time),
      // Un estado derivado dentro de un handler hereda el contexto del evento que lo causó.
      correlationid: inherited?.correlationid ?? id,
      producerversion: this.#h.options.version,
      classification,
      data,
      dataschema: this.#schemaFor(c.contract),
      ...(inherited?.causationid !== undefined && { causationid: inherited.causationid }),
      ...(inherited?.traceparent !== undefined
        ? { traceparent: inherited.traceparent }
        : await this.#traceparent()),
      ...(inherited?.tracestate !== undefined && { tracestate: inherited.tracestate }),
    });
    // L3 ANTES de leer ni escribir; reporta TODOS los errores y no copia valores a su mensaje.
    this.#countingInvalid(c.contract, () => this.#h.validator()?.(event, subject));
    return this.#write(c, tenant, key, subject, version, event);
  }

  /**
   * Borra una clave: publica una **lápida** en su subject, que sustituye al valor (el dato
   * personal ya no está en el stream). Es `put` de una lápida: mismo algoritmo, mismas reglas de
   * versión y mismo resultado. Borrar una clave que nunca existió escribe la lápida.
   */
  async delete(contract: string, key: string, options: StateDeleteOptions): Promise<StateWriteResult> {
    const c = parseStateContract(contract);
    assertStateKey(key);
    const tenant = this.#tenantFor(options?.tenantId);
    const version = this.#countingInvalid(c.contract, () =>
      aggregateVersionOf({ aggregateVersion: options?.aggregateVersion }),
    );

    const id = uuidv7();
    const inherited = currentContext();
    const subject = stateSubject(c.contract, tenant, key);
    const event = buildStateTombstone({
      contract: c.contract,
      tenantId: tenant,
      key,
      id,
      source: this.#h.source,
      time: toIso(options.time),
      correlationid: inherited?.correlationid ?? id,
      producerversion: this.#h.options.version,
      aggregateVersion: version,
      ...(inherited?.causationid !== undefined && { causationid: inherited.causationid }),
      ...(inherited?.traceparent !== undefined
        ? { traceparent: inherited.traceparent }
        : await this.#traceparent()),
      ...(inherited?.tracestate !== undefined && { tracestate: inherited.tracestate }),
    });
    // La lápida valida contra su esquema fijo (en `schemas` del bundle, no en el índice `state`).
    this.#countingInvalid(c.contract, () => this.#h.validator()?.(event, subject));
    return this.#write(c, tenant, key, subject, version, event);
  }

  /**
   * El algoritmo de 10-state.md §3.2. El evento se construyó UNA vez: `id`, `time` y bytes son
   * los mismos en todos los intentos. Con un `id` nuevo por intento, el paso 1b no reconocería
   * nunca la propia escritura.
   */
  async #write(
    c: ParsedStateContract,
    tenant: string,
    key: string,
    subject: string,
    version: number,
    toSend: FluxEvent,
  ): Promise<StateWriteResult> {
    const stream = stateStreamName(c.domain);
    const signed = this.#h.signer()?.sign(toSend) ?? toSend;
    const bytes = serialize(signed);
    const id = signed.id;

    for (let attempt = 1; attempt <= STATE_MAX_ATTEMPTS; attempt++) {
      // 1. Leer el último mensaje de la clave. 404 → S = 0 y no hay versión.
      const actual = await this.#readRaw(stream, subject);
      const S = actual?.seq ?? 0;

      // 1b. ¿El mensaje leído es EL MÍO? Entonces ya se aplicó y solo se perdió el acuse. Sin
      // este paso, un acuse perdido convierte una escritura buena en un `stale` con su propio
      // dato: el reenvío da «wrong last sequence» porque lo que cambió la secuencia fue el propio
      // mensaje (A-33). Se reconoce por el `id` porque el broker conserva las cabeceras (A-32).
      if (actual && actual.ids.includes(id)) {
        this.#h.metrics.stateWrite?.(c.contract, "ok");
        return { applied: true, outcome: "ok", revision: actual.seq };
      }

      // 2. Una versión que no avanza NO se escribe, y una igual tampoco avanza. No es un error.
      if (actual?.version !== undefined && version <= actual.version) {
        this.#h.metrics.stateWrite?.(c.contract, "stale");
        return { applied: false, outcome: "stale", revision: actual.seq };
      }

      // 3. Siempre por JetStream y siempre con la cabecera de secuencia: un publish de CORE a un
      // subject de estado sobrescribe sin acuse ni chequeo (A-45). `S = 0` = «la clave no existe»:
      // crea si el subject no tiene mensaje y falla si ya lo tiene, y eso es lo que hace que dos
      // escritores concurrentes sobre una clave nueva no se pisen (A-33).
      try {
        const ack = await this.#h.js.publish(subject, bytes, {
          msgID: id,
          expect: { lastSubjectSequence: S },
        });
        this.#h.metrics.stateWrite?.(c.contract, "ok");
        return { applied: true, outcome: "ok", revision: ack.seq };
      } catch (e) {
        // 4. Alguien escribió entre el 1 y el 3: volver a 1. DOS códigos, no uno.
        if (isSequenceConflict(e)) {
          // Un poco de jitter es opcional (§3.3) y barato: sin él, tres instancias que chocan
          // vuelven a chocar a la vez.
          if (attempt < STATE_MAX_ATTEMPTS) await sleep(Math.random() * 12 * attempt);
          continue;
        }
        throw this.#asDenied(e) ?? this.#asNotProvisioned(e, stream, subject) ?? e;
      }
    }
    this.#h.metrics.stateWrite?.(c.contract, "conflict");
    throw new StateConflictError(c.contract, key, STATE_MAX_ATTEMPTS);
  }

  // ─── leer ──────────────────────────────────────────────────────────────────

  /**
   * Una lectura puntual por `DIRECT.GET` sobre el subject exacto: no hay consumidor ni acuse.
   * `null` si la clave no existe **o** es una lápida. Es una lectura de RÉPLICA (§3.7): con
   * `replicas: 3` PUEDE devolver el valor anterior justo después de un `put`; quien necesite su
   * propia escritura la deriva de `revision`, no relee.
   */
  async get<T = Record<string, unknown>>(
    contract: string,
    key: string,
    options: StateGetOptions = {},
  ): Promise<StateEntry<T> | null> {
    const c = parseStateContract(contract);
    assertStateKey(key);
    const tenant = this.#tenantFor(options?.tenantId);
    const subject = stateSubject(c.contract, tenant, key);
    const stored = await this.#readRaw(stateStreamName(c.domain), subject);
    if (!stored) return null; // 404 `Message Not Found`
    const { entry } = this.#decode<T>(stored.data, { contract: c.contract, tenant, key, revision: stored.seq, subject });
    return entry.deleted ? null : entry;
  }

  /**
   * `DIRECT.GET` por subject — la forma por SUBJECT, que se puede confinar por tenant con una
   * ACL. La de cuerpo (`{"seq": n}`) lee cualquier mensaje y ninguna ACL la ve: no se usa.
   *
   * Un stream que no existe, o sin `allow_direct`, NO da 404: no contesta nadie, y los dos casos
   * se ven igual (A-32) → `NOT_PROVISIONED`, con las dos causas en el mensaje.
   */
  async #readRaw(stream: string, subject: string): Promise<StoredState | null> {
    let r: Msg;
    try {
      r = await this.#h.nc.request(`$JS.API.DIRECT.GET.${stream}.${subject}`, EMPTY, {
        timeout: DIRECT_GET_TIMEOUT_MS,
      });
    } catch (e) {
      throw this.#asDenied(e) ?? this.#asNotProvisioned(e, stream, subject) ?? e;
    }
    const h = r.headers;
    if (h?.code === 404) return null;
    if (h?.hasError) {
      throw new Error(`DIRECT.GET de ${subject} contestó ${h.code} ${h.description}`);
    }
    const seq = Number(h?.get("Nats-Sequence"));
    const ids: string[] = [];
    const headerId = h?.get("Nats-Msg-Id");
    if (headerId) ids.push(headerId);

    // Un valor ILEGIBLE no bloquea la clave (§3.3): si el cuerpo no es JSON, o no tiene un
    // `aggregateVersion` válido, se trata como «sin versión» y se sobrescribe con el chequeo de
    // secuencia sobre SU `S`. De lo contrario una entrada rota dejaría la clave inescribible
    // para siempre, porque ningún servicio puede purgar el stream (06-security.md §3.5).
    let ev: unknown;
    try {
      ev = JSON.parse(DECODER.decode(r.data));
    } catch {
      return { seq, data: r.data, ids, unreadable: true };
    }
    if (ev === null || typeof ev !== "object" || Array.isArray(ev)) {
      return { seq, data: r.data, ids, unreadable: true };
    }
    const o = ev as { id?: unknown; data?: { aggregateVersion?: unknown } | null };
    if (typeof o.id === "string") ids.push(o.id);
    const v = o.data !== null && typeof o.data === "object" ? o.data.aggregateVersion : undefined;
    return { seq, data: r.data, ids, unreadable: false, ...(isValidAggregateVersion(v) && { version: v }) };
  }

  /**
   * Foto completa y después los cambios, por el MISMO canal: un consumidor durable de
   * `STATE_<DOM>` con `deliver_policy: last_per_subject`. Un consumidor que estuvo caído recibe
   * solo el último valor de cada clave; un valor reemplazado mientras estaba en vuelo NO se
   * reentrega (A-35, A-36). El handler DEBE ser idempotente y no asumir orden
   * (`UPDATE … WHERE aggregate_version < $n`) y, ante `entry.deleted`, DEBE borrar su copia local.
   */
  async watch<T = Record<string, unknown>>(
    contract: string,
    handler: StateHandler<T>,
    options: StateWatchOptions = {},
  ): Promise<StateSubscription> {
    const c = parseStateContract(contract);
    const opts = this.#h.options;
    const tenant = this.#watchTenant(options.tenantId);
    if (opts.tenantIsolation === "strict" && tenant === undefined) {
      throw new TenantIsolationError(
        `state.${c.contract}`,
        "no hay tenantId ni en connect() ni en watch()",
      );
    }
    const stream = stateStreamName(c.domain);
    const durable = stateDurableName(opts.service, c.contract, tenant);
    const w: WatchState<T> = {
      contract: c.contract,
      tenant,
      stream,
      durable,
      consumerLabel: stateDurableName(opts.service, c.contract),
      subjectLabel: `state.${c.contract}`,
      handler,
      sync: new SyncTracker(() => this.#fireSynced(options.onSynced)),
    };

    // El SDK comprueba la DLQ al ARRANCAR, no cuando llega el primer mensaje envenenado.
    await this.#ensureStateDlq(c.domain);

    const requested = {
      durable_name: durable,
      filter_subject: stateFilterSubject(c.contract, tenant),
      ack_policy: AckPolicy.Explicit,
      // ack_wait DEBE ser backoff[0]: JetStream lo sobrescribe con ese valor sin avisar.
      ack_wait: nanos(CONSUMER_DEFAULTS.ackWaitMs),
      max_deliver: CONSUMER_DEFAULTS.maxDeliver,
      backoff: CONSUMER_DEFAULTS.backoffMs.map(nanos),
      max_ack_pending: CONSUMER_DEFAULTS.maxAckPending,
      deliver_policy: DeliverPolicy.LastPerSubject,
      replay_policy: ReplayPolicy.Instant,
    };
    const found = await this.#ensureConsumer(stream, durable, requested);
    this.#assertConfigHonored(durable, requested as unknown as Record<string, unknown>, found.config as unknown as Record<string, unknown>);

    const consumer = await this.#h.js.consumers.get(stream, durable);
    // Una denegación de un intento anterior sobre este durable no debe condenar a este.
    const pull = `$JS.API.CONSUMER.MSG.NEXT.${stream}.${durable}`;
    this.#h.clearDenied(pull);
    // `let`: si el durable se borra mientras el watch vive (`flux state resync`), se reabre (#consume).
    let messages = await consumer.consume();

    const configurado = opts.pendingPollMs ?? 0;
    const pollMs = configurado === 0 ? 15_000 : configurado; // 0 → default; negativo → desactivado (08 §2.3)
    const poll =
      pollMs > 0 && opts.metrics
        ? setInterval(() => {
            void this.#h.jsm.consumers
              .info(stream, durable)
              .then((i) => this.#notePending(w, i.num_pending))
              .catch(() => {}); // la telemetría no debe afectar al consumo
          }, pollMs)
        : null;
    if (poll?.unref) poll.unref();

    let stopped = false;
    const stop = () => {
      stopped = true;
      if (poll) clearInterval(poll);
      messages.stop();
      this.#dropPending(w);
    };
    const untrack = this.#h.track(stop);

    // Stream vacío, o durable al día: `onSynced` de INMEDIATO, antes de entregar nada y aunque no
    // llegue ninguna entrada después. Esperar a una que quizá no llegue nunca dejaría al servicio
    // «sincronizando» para siempre (10-state.md §5.5, punto 2). `num_ack_pending` también: lo
    // entregado y sin confirmar sigue pendiente de procesar.
    if (found.numPending === 0 && found.numAckPending === 0) w.sync.immediate();

    void this.#consume(w, requested as unknown as Record<string, unknown>, messages, (m) => (messages = m), () => stopped);

    // La petición de mensajes es un publish sin respuesta: si el broker la deniega no hay
    // excepción y el watch queda inerte, aparentemente sano (A-20). Como en subscribe().
    try {
      await new Promise<void>((r) => setImmediate(r));
      await this.#h.nc.flush();
      await new Promise<void>((r) => setImmediate(r));
      if (this.#h.isDenied(pull)) throw new PermissionDeniedError(opts.service, "publish", pull);
    } catch (e) {
      // Un watch que no llega a abrirse no debe quedarse consumiendo a espaldas de quien recibe el error.
      stop();
      untrack();
      throw e;
    }

    return {
      contract: c.contract,
      durable,
      ...(tenant !== undefined && { tenantId: tenant }),
      unsubscribe: async () => {
        stop();
        untrack();
      },
    };
  }

  // ─── el bucle de consumo, y su reapertura ──────────────────────────────────

  /**
   * El bucle de un `watch`: entrega cada mensaje a `#dispatch` y, si el durable se **borra** mientras
   * el watch vive (`flux state resync`: borrar y recrear), **reabre el consumo**.
   *
   * Por qué hace falta. Al borrar el durable, el broker contesta al pull pendiente `409 Consumer
   * Deleted` y nats.js lo notifica (`consumer_deleted`)… y **no vuelve a pedir** hasta que detecta la
   * falta de latidos (`heartbeats_missed`: dos latidos de 15 s, más el reintento). Medido con
   * nats-server 2.14.5 y nats.js 3.4.0: **43 s** entre recrear el durable y la primera entrada de la
   * foto nueva (10-state.md §5.2). Aquí se repite el pull al recibir `consumer_deleted`: se vuelve a
   * consultar el durable por su nombre —`CONSUMER.INFO`, que el observador ya tiene; **no** hace falta
   * `CONSUMER.CREATE`— y se vuelve a consumir. `onSynced` no se invoca de nuevo.
   */
  async #consume<T>(
    w: WatchState<T>,
    requested: Record<string, unknown>,
    first: ConsumerMessages,
    swap: (m: ConsumerMessages) => void,
    isStopped: () => boolean,
  ): Promise<void> {
    let messages = first;
    while (!isStopped()) {
      let deleted = false;
      const current = messages;
      void (async () => {
        try {
          for await (const s of current.status()) {
            if (s.type === "consumer_deleted") {
              deleted = true;
              current.stop(); // termina el `for await` de abajo: se reabre
              return;
            }
          }
        } catch {
          /* el iterador de estado termina con el consumo */
        }
      })();
      for await (const m of current) {
        if (isStopped()) break;
        try {
          await this.#dispatch(m, w);
        } catch (e) {
          // Un fallo inesperado de #dispatch rompería el `for await` y el watch dejaría de
          // consumir EN SILENCIO, con la conexión viva: un consumidor muerto que parece vivo.
          this.#h.options.logger?.error(
            `[flux] fallo inesperado despachando el estado ${w.subjectLabel}: ${e instanceof Error ? e.message : String(e)}. ` +
              `El mensaje no se confirma y será reentregado.`,
          );
        }
      }
      if (isStopped() || !deleted) return;
      const reopened = await this.#reopen(w, requested, isStopped);
      if (!reopened) return;
      messages = reopened;
      swap(reopened);
    }
  }

  /**
   * Vuelve a abrir el consumo de un durable que se borró bajo el watch. Reintenta con backoff
   * (100 ms … 5 s) hasta que el durable exista —`flux state resync` lo recrea a continuación— o el
   * watch pare. Comprueba la configuración como al abrir: un durable recreado con otro filtro
   * entregaría las claves de otro tenant sin ningún error, y ese no se consume.
   */
  async #reopen<T>(
    w: WatchState<T>,
    requested: Record<string, unknown>,
    isStopped: () => boolean,
  ): Promise<ConsumerMessages | null> {
    const logger = this.#h.options.logger;
    logger?.warn(
      `[flux] el durable ${w.durable} se borró con el watch de ${w.subjectLabel} abierto (¿\`flux state resync\`?): ` +
        `se vuelve a abrir el consumo cuando exista de nuevo.`,
    );
    let ultimo = "";
    for (let intento = 1; !isStopped(); intento++) {
      try {
        const consumer = await this.#h.js.consumers.get(w.stream, w.durable); // CONSUMER.INFO
        const info = await consumer.info(true);
        this.#assertConfigHonored(w.durable, requested, info.config as unknown as Record<string, unknown>);
        const messages = await consumer.consume();
        if (isStopped()) {
          messages.stop();
          return null;
        }
        logger?.warn(`[flux] consumo de ${w.durable} reabierto (intento ${intento}): se entregará la foto otra vez.`);
        return messages;
      } catch (e) {
        // `consumer not found` (10014): el resync está entre el borrado y la creación. Se espera. Se avisa la primera
        // vez, cada 10 intentos y SIEMPRE que la causa cambia (un durable recreado con otra configuración no es «aún no existe»).
        const motivo = e instanceof Error ? e.message : String(e);
        if (intento === 1 || intento % 10 === 0 || motivo !== ultimo) {
          logger?.warn(`[flux] no se pudo reabrir el consumo de ${w.durable} (intento ${intento}): ${motivo}. Se reintenta.`);
          ultimo = motivo;
        }
        // `unref`: un servicio que se cierra con el durable aún sin recrear no debe quedarse hasta 5 s por este temporizador.
        await new Promise<void>((r) => setTimeout(r, Math.min(5_000, 100 * 2 ** Math.min(intento - 1, 6))).unref());
      }
    }
    // Se pidió parar (`close()`, `unsubscribe()`) mientras se esperaba al durable: se dice, para que un reintento
    // que se detiene no sea indistinguible de uno que sigue colgado.
    logger?.warn(`[flux] el watch de ${w.subjectLabel} se detuvo sin haber podido reabrir el consumo de ${w.durable}`);
    return null;
  }

  // ─── despacho de una entrada ───────────────────────────────────────────────

  async #dispatch<T>(m: JsMsg, w: WatchState<T>): Promise<void> {
    if (typeof m.info.pending === "number") this.#notePending(w, m.info.pending);
    w.sync.started(m.info.pending);
    try {
      await this.#process(m, w);
    } finally {
      w.sync.finished();
    }
  }

  async #process<T>(m: JsMsg, w: WatchState<T>): Promise<void> {
    const h = this.#h;
    const attempt = m.info.deliveryCount;
    const maxAttempts = CONSUMER_DEFAULTS.maxDeliver;
    // `state.<dom>.<agr>.v<M>.<tenant>.<clave>`: la identidad la decide EL BROKER, no el sobre.
    const partes = m.subject.split(".");
    const tenant = partes[4];
    const key = partes[5];
    const revision = m.info.streamSequence;
    const inicio = Date.now();
    const parsed: { event?: FluxEvent } = {};

    // WIP mientras el handler vive: evita que la misma entrada se ejecute en concurrencia consigo misma.
    const wip = setInterval(() => {
      try {
        m.working();
      } catch {
        /* el mensaje ya está resuelto */
      }
    }, CONSUMER_DEFAULTS.ackWaitMs / 2);

    try {
      const { event, entry, signatureWarning } = this.#decode<T>(
        m.data,
        { contract: w.contract, tenant, key, revision, subject: m.subject },
        parsed,
      );
      if (signatureWarning) h.metrics.eventConsumed(w.subjectLabel, w.consumerLabel, "invalid_signature");
      await runWithContext(contextFromEvent(event), async () =>
        w.handler(entry, { ack: () => {}, attempt, maxAttempts, subject: m.subject, contract: w.contract }),
      );
      m.ack();
      h.metrics.eventConsumed(w.subjectLabel, w.consumerLabel, "ok");
      h.metrics.handlerDuration(w.subjectLabel, w.consumerLabel, (Date.now() - inicio) / 1000);
    } catch (e) {
      const c = h.classify(e);
      const budget = Math.min(maxAttempts, c.maxAttempts ?? maxAttempts);

      if (c.class === ErrorClass.RETRYABLE && attempt < budget) {
        const delay =
          c.retryAfterMs ?? CONSUMER_DEFAULTS.backoffMs[Math.min(attempt - 1, CONSUMER_DEFAULTS.backoffMs.length - 1)];
        h.options.logger?.warn(
          `[flux] RETRYABLE ${c.code} en ${w.subjectLabel} (clave ${key}, intento ${attempt}/${budget}), reintento en ${delay}ms`,
        );
        h.metrics.eventRetried(w.subjectLabel, w.consumerLabel, attempt);
        m.nak(delay);
        return;
      }

      const reason = c.class === ErrorClass.POISON ? "poison" : c.class === ErrorClass.RETRYABLE ? "retryable" : "permanent";
      const outcome = SIGNATURE_CODES.has(c.code)
        ? "invalid_signature"
        : c.code === "INVALID_SCHEMA" || c.code === "SCHEMA_NOT_FOUND"
          ? "invalid_schema"
          : reason;

      // `dlqerror` NO DEBE contener valores de `data` (10-state.md §6). Un error de flux lleva su
      // código y un mensaje que no cita valores. Uno que no sea de flux —una excepción de un
      // driver de base de datos, que puede citar la fila entera— se reduce a su código: la DLQ
      // guarda 90 días y en estado `data` es dato personal.
      const dlqerror = isClassifiedError(e)
        ? `${c.code}: ${e.message}`
        : `${c.code}: ${UNCLASSIFIED_DLQ_ERROR}`;
      const info = {
        reason,
        attempts: attempt,
        consumer: w.durable,
        error: dlqerror,
        dlqtime: new Date().toISOString(),
      } as const;
      // Un mensaje ilegible no se copia; el resto, su sobre ORIGINAL sin el dato.
      const dlqEvent = parsed.event
        ? toStateDlqEvent(parsed.event, { ...info, key, revision })
        : buildUnreadableStateDlqEvent({
            contract: w.contract,
            tenantId: tenant,
            key,
            revision,
            source: h.source,
            producerversion: h.options.version,
            id: uuidv7(),
            time: new Date().toISOString(),
            dataschema: this.#schemaForSafe(w.contract),
            dlq: info,
          });
      try {
        await h.js.publish(stateDlqSubject(w.contract), serialize(dlqEvent), {
          msgID: `dlq-${dlqEvent.id}-${w.durable}`,
        });
      } catch (dlqError) {
        // NO se hace term(): terminar aquí borraría la entrada sin haberla guardado en ningún sitio.
        h.options.logger?.error(
          `[flux] no se pudo publicar en la DLQ de estado para ${w.subjectLabel} (clave ${key}): ` +
            `${dlqError instanceof Error ? dlqError.message : String(dlqError)}. El mensaje NO se termina; se reentregará.`,
        );
        return;
      }

      if (c.class === ErrorClass.POISON) {
        // POISON es lo que despierta a alguien: term() + DLQ + ALERTA. `raw` va vacío a propósito:
        // el cuerpo de una entrada de estado puede llevar datos personales (10-state.md §6).
        h.options.onPoison?.({ subject: m.subject, error: e instanceof Error ? e : new PoisonError(String(e)), raw: EMPTY });
        h.options.logger?.error(`[flux] POISON ${c.code} en ${w.subjectLabel} (clave ${key}): ${isClassifiedError(e) ? e.message : c.code}`);
      } else {
        h.options.logger?.error(
          `[flux] DLQ de estado (${reason}) ${c.code} en ${w.subjectLabel} (clave ${key}) tras ${attempt} intento(s)`,
        );
      }
      h.metrics.eventConsumed(w.subjectLabel, w.consumerLabel, outcome);
      h.metrics.eventDlq(w.subjectLabel, w.consumerLabel, reason, c.code);
      h.metrics.handlerDuration(w.subjectLabel, w.consumerLabel, (Date.now() - inicio) / 1000);
      h.options.onDlq?.({ subject: m.subject, event: dlqEvent, classification: c });
      m.term();
    } finally {
      clearInterval(wip);
    }
  }

  /**
   * Lo que comprueba el SDK al consumir, en el orden de 10-state.md §5.4. Lo que falla **nunca**
   * se aplica y **nunca** llega al handler. `holder.event` se rellena en cuanto el sobre es un
   * CloudEvent válido, para que la DLQ pueda partir de él.
   */
  #decode<T>(
    bytes: Uint8Array,
    ctx: { contract: string; tenant: string; key: string; revision: number; subject: string },
    holder: { event?: FluxEvent } = {},
  ): { event: FluxEvent; entry: StateEntry<T>; signatureWarning: boolean } {
    // 1. Un CloudEvent válido con las cuatro extensiones (POISON, los códigos de siempre).
    const event = parseEvent(bytes);
    holder.event = event;

    // 2. La identidad está en el subject; el sobre la repite. Si discrepan, uno miente: POISON.
    if (event.tenantid !== ctx.tenant) throw new StateSubjectMismatchError("tenantid", event.tenantid, ctx.tenant);
    if (event.subject !== ctx.key) throw new StateSubjectMismatchError("subject", event.subject, ctx.key);
    const put = stateType(ctx.contract);
    const deleted = event.type === stateTombstoneType(ctx.contract);
    if (!deleted && event.type !== put) throw new StateSubjectMismatchError("type", event.type, put);

    // 3. `restricted` no se admite en estado: en consumo es POISON.
    if (event.dataclassification === "restricted") throw new RestrictedStateEntryError();

    // 4. `aggregateVersion` válido: sin él no se puede aplicar de forma condicional (PERMANENT).
    const aggregateVersion = aggregateVersionOf(event.data);

    // La firma antes que el esquema: un evento manipulado puede validar perfectamente y no ser
    // del productor que dice (07-signing.md §5).
    const signatureWarning = this.#h.verify(event) !== null;

    // 5. Si hay L3 al consumir: `data` contra su esquema.
    if (this.#h.options.validation?.onConsume) this.#h.validator()?.(event, ctx.subject);

    return {
      event,
      signatureWarning,
      entry: {
        key: ctx.key,
        tenantId: ctx.tenant,
        deleted,
        data: deleted ? null : (event.data as T),
        aggregateVersion,
        revision: ctx.revision,
      },
    };
  }

  // ─── consumidor y DLQ ──────────────────────────────────────────────────────

  /**
   * Toma el consumidor durable si existe y, solo si no existe, lo crea (desarrollo). `watch` NO
   * necesita `STREAM.INFO.STATE_<DOM>` —que lista todas las claves y todos los tenants (A-39)—:
   * `CONSUMER.INFO` distingue «el consumidor no existe» (10014) de «el stream no existe»
   * (10059) (A-48), que es todo lo que hace falta para dar `NOT_PROVISIONED` o `PERMISSION_DENIED`.
   */
  async #ensureConsumer(
    stream: string,
    durable: string,
    requested: Partial<ConsumerConfig>,
  ): Promise<{ config: ConsumerConfig; numPending: number; numAckPending: number }> {
    const service = this.#h.options.service;
    try {
      const i = await this.#h.jsm.consumers.info(stream, durable);
      return { config: i.config, numPending: i.num_pending, numAckPending: i.num_ack_pending };
    } catch (infoError) {
      if (isNotFound(infoError, "stream")) {
        throw this.#notProvisioned(stream, `$JS.API.CONSUMER.INFO.${stream}.${durable}`, infoError);
      }
      if (!isNotFound(infoError, "consumer")) throw this.#asDenied(infoError) ?? infoError;
    }
    try {
      const i = await this.#h.jsm.consumers.add(stream, requested);
      return { config: i.config, numPending: i.num_pending, numAckPending: i.num_ack_pending };
    } catch (addError) {
      const denied = permissionViolation(addError);
      if (denied) {
        throw new NotProvisionedError({ kind: "consumer", name: durable, stream }, service, denied.subject, {
          cause: addError,
        });
      }
      // Otra instancia del servicio pudo crearlo entre la pregunta y la creación.
      try {
        const i = await this.#h.jsm.consumers.info(stream, durable);
        return { config: i.config, numPending: i.num_pending, numAckPending: i.num_ack_pending };
      } catch {
        throw addError;
      }
    }
  }

  /**
   * Como `FluxBus.#assertConfigHonored`, más lo propio del estado: la política de entrega y el
   * filtro. Un durable aprovisionado con otro filtro entregaría un conjunto distinto del pedido
   * (otro tenant, todos los tenants) sin ningún error.
   */
  #assertConfigHonored(durable: string, requested: Record<string, unknown>, effective: Record<string, unknown>): void {
    const diffs: Array<{ field: string; requested: unknown; effective: unknown }> = [];
    for (const f of ["ack_wait", "max_deliver", "max_ack_pending", "deliver_policy"]) {
      if (effective[f] !== undefined && effective[f] !== requested[f]) {
        diffs.push({ field: f, requested: requested[f], effective: effective[f] });
      }
    }
    const reqB = requested.backoff as number[];
    const effB = (effective.backoff as number[] | undefined) ?? [];
    if (effB.length !== reqB.length || effB.some((v, i) => v !== reqB[i])) {
      diffs.push({ field: "backoff", requested: reqB, effective: effB });
    }
    const filtros = (effective.filter_subjects as string[] | undefined) ?? [];
    const efectivo = (effective.filter_subject as string | undefined) ?? (filtros.length === 1 ? filtros[0] : undefined);
    if (efectivo !== requested.filter_subject || filtros.length > 1) {
      diffs.push({ field: "filter_subject", requested: requested.filter_subject, effective: efectivo ?? filtros });
    }
    if (diffs.length > 0) throw new ConsumerConfigMismatchError(durable, diffs);
  }

  /**
   * `DLQ_<DOM>` debe capturar `dlq.state.<dom>.>`. Un dominio solo de eventos conserva su
   * `DLQ_<DOM>` de la v1.0 sin tocarlo: ampliarlo es posible y no pierde mensajes (A-44), pero no
   * se le pide a quien no usa estado, y NO lo hace el SDK sobre uno existente: lo hace
   * `flux provision`. Si no captura, se AVISA al abrir, y no cuando el primer mensaje muera.
   */
  async #ensureStateDlq(domain: string): Promise<void> {
    const name = dlqStreamName(domain);
    if (this.#dlqChecked.has(name)) return;
    const opts = this.#h.options;
    const muestra = `dlq.state.${domain}.x.v1`;
    try {
      const info = await this.#h.jsm.streams.info(name);
      if (!(info.config.subjects ?? []).some((s) => subjectMatches(s, muestra))) {
        opts.logger?.warn(
          `[flux] ${name} no captura "dlq.state.${domain}.>": una entrada de estado que muera no se podrá guardar ` +
            `y se reentregará. Un dominio con estado lo necesita; lo amplía \`flux provision\` y lo audita \`flux doctor\` (10-state.md §6).`,
        );
      }
    } catch (infoError) {
      if (!isNotFound(infoError, "stream")) throw this.#asDenied(infoError) ?? infoError;
      // Desarrollo: se crea con LOS DOS subjects. En producción lo aprovisiona `flux provision`.
      try {
        await this.#h.jsm.streams.add({
          name,
          subjects: [`dlq.${domain}.>`, `dlq.state.${domain}.>`],
          storage: StorageType.File,
          retention: RetentionPolicy.Limits,
          discard: DiscardPolicy.Old,
          num_replicas: opts.streamReplicas ?? 1,
          max_age: nanos(STREAM_DEFAULTS.dlqMaxAgeMs),
        });
      } catch (addError) {
        const denied = permissionViolation(addError);
        if (denied) {
          throw new NotProvisionedError({ kind: "stream", name }, opts.service, denied.subject, { cause: addError });
        }
        throw addError;
      }
    }
    this.#dlqChecked.add(name);
  }

  // ─── tenant, esquema, errores y métricas ───────────────────────────────────

  /** Escritura y lectura: la opción de la llamada, el evento en curso y `connect()`. Sin ninguno, error. */
  #tenantFor(option: string | undefined): string {
    const t = option !== undefined ? option : (currentContext()?.tenantid ?? this.#h.options.tenantId);
    if (t === undefined) throw new InvalidTenantForStateError(undefined);
    assertStateTenant(t);
    return t;
  }

  /**
   * `watch`: la opción y `connect()`, **sin el evento en curso** (un `watch` no corre dentro de un
   * handler: 10-state.md §2.4). Sin ninguno es legítimo (se observan todos), salvo `strict`.
   */
  #watchTenant(option: string | undefined): string | undefined {
    const t = option !== undefined ? option : this.#h.options.tenantId;
    if (t === undefined) return undefined;
    assertStateTenant(t);
    return t;
  }

  /** El `dataschema` del contrato: mapa explícito, índice `state` del bundle o `schemaBaseUrl`. Nunca por red. */
  #schemaFor(contract: string): string {
    const o = this.#h.options;
    const explicit = o.schemas?.[contract];
    if (explicit) return explicit;
    const fromBundle = o.validation?.bundle ? stateSchemaUriFor(o.validation.bundle, contract) : undefined;
    if (fromBundle) return fromBundle;
    if (!o.schemaBaseUrl) {
      throw new Error(
        `no hay dataschema para el estado "${contract}". Declara \`schemas["${contract}"]\`, ` +
          `\`schemaBaseUrl\`, o pasa un bundle con el índice \`state\` en \`validation.bundle\` (connect()).`,
      );
    }
    return stateDataschema(contract, o.schemaBaseUrl);
  }

  /** Para la DLQ: si no se puede resolver, el default del repo; un fallo de configuración no debe perder el aviso. */
  #schemaForSafe(contract: string): string {
    try {
      return this.#schemaFor(contract);
    } catch {
      return stateDataschema(contract);
    }
  }

  async #traceparent(): Promise<{ traceparent?: string }> {
    const tp = await activeTraceparent();
    return tp ? { traceparent: tp } : {};
  }

  #asDenied(e: unknown): PermissionDeniedError | undefined {
    const v = permissionViolation(e);
    return v ? new PermissionDeniedError(this.#h.options.service, v.operation, v.subject, { cause: e }) : undefined;
  }

  #asNotProvisioned(e: unknown, stream: string, subject: string): NotProvisionedError | undefined {
    return isNoResponders(e) ? this.#notProvisioned(stream, `$JS.API.DIRECT.GET.${stream}.${subject}`, e) : undefined;
  }

  /**
   * Nadie contesta: el stream no existe, **o** existe sin `allow_direct`, y los dos casos se ven
   * exactamente igual (A-32). El mensaje DEBE nombrar las dos causas y apuntar a `flux provision`
   * (lo crea) y a `flux doctor` (lo audita). Si el broker DENIEGA la operación, es un permiso y no
   * un recurso: eso es `PERMISSION_DENIED`, y no llega aquí.
   */
  #notProvisioned(stream: string, sinRespuesta: string, cause: unknown): NotProvisionedError {
    const service = this.#h.options.service;
    return new NotProvisionedError({ kind: "stream", name: stream }, service, sinRespuesta, {
      cause,
      detail:
        `nadie contestó en "${sinRespuesta}": el stream "${stream}" no existe, o existe sin ` +
        `\`allow_direct: true\`, y los dos casos se ven exactamente igual (no hay un 404: no responde ` +
        `nadie). "${service}" no lo crea ni lo consulta —los SDKs no usan \`STREAM.INFO\` sobre el estado, ` +
        `porque lista todas las claves y todos los tenants (A-39)—. Aprovisiónalo con \`flux provision\`, ` +
        `con credenciales de operación, y compruébalo con \`flux doctor\` (10-state.md §5.1, 04-errors.md §6).`,
    });
  }

  #countingInvalid<R>(contract: string, fn: () => R): R {
    try {
      return fn();
    } catch (e) {
      // `invalid_schema` se emite también cuando la llamada lanza: una excepción sirve a quien la
      // provoca, y la métrica a quien opera el ecosistema. Es todo fallo de L3 —también el esquema
      // ausente del bundle en modo estricto (`SCHEMA_NOT_FOUND`), igual que en `publish()`— y
      // `MISSING_AGGREGATE_VERSION`. Los demás errores de la llamada (contrato, clave, tenant,
      // dominio reservado, clasificación) son de programación y NO se cuentan.
      if (
        e instanceof MissingAggregateVersionError ||
        e instanceof SchemaValidationError ||
        e instanceof SchemaNotFoundError
      ) {
        this.#h.metrics.stateWrite?.(contract, "invalid_schema");
      }
      throw e;
    }
  }

  #fireSynced(cb: (() => void) | undefined): void {
    if (!cb) return;
    try {
      const r = cb() as unknown;
      if (r instanceof Promise) r.catch((e) => this.#h.options.logger?.error(`[flux] onSynced lanzó: ${e instanceof Error ? e.message : String(e)}`));
    } catch (e) {
      this.#h.options.logger?.error(`[flux] onSynced lanzó: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  /**
   * `flux_consumer_pending` es la SUMA de los durables del servicio para ese contrato, no el
   * valor del último sondeado: un durable por tenant publican las mismas etiquetas (el `consumer`
   * va SIN el sufijo del tenant, o cada tenant nuevo crearía series nuevas) y se pisarían.
   */
  #notePending<T>(w: WatchState<T>, pending: number): void {
    const suma = this.#pending.note(`${w.subjectLabel}|${w.consumerLabel}`, w.durable, pending);
    this.#h.metrics.consumerPending(w.subjectLabel, w.consumerLabel, suma);
  }

  /** Al parar un `watch`, su durable deja de sumar. */
  #dropPending<T>(w: WatchState<T>): void {
    const suma = this.#pending.drop(`${w.subjectLabel}|${w.consumerLabel}`, w.durable);
    if (suma !== undefined) this.#h.metrics.consumerPending(w.subjectLabel, w.consumerLabel, suma);
  }
}
