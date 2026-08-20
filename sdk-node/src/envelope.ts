/**
 * Envelope CloudEvents 1.0 — construcción, serialización y parseo.
 * Contrato normativo: specification/01-envelope.md
 */

import { PoisonError } from "./errors.js";
import { MAX_MESSAGE_BYTES, subjectToType } from "./protocol.js";

export type DataClassification =
  | "public"
  | "internal"
  | "confidential"
  | "restricted";

/**
 * Un evento de flux. Los nombres de las extensiones van en minúscula pegada porque
 * CloudEvents restringe los nombres de extensión a `[a-z0-9]` sin separadores —
 * no es estilo, es la especificación (01-envelope.md §3).
 */
export interface FluxEvent<T = unknown> {
  readonly specversion: "1.0";
  readonly id: string;
  readonly source: string;
  readonly type: string;
  readonly time: string;
  readonly datacontenttype: "application/json";
  readonly dataschema: string;
  /** ⚠️ Id del AGREGADO, no el subject de NATS. Ver 01-envelope.md §2.1. */
  readonly subject?: string;

  readonly correlationid: string;
  readonly tenantid: string;
  readonly producerversion: string;
  readonly dataclassification: DataClassification;

  readonly causationid?: string;
  readonly partitionkey?: string;
  readonly traceparent?: string;
  readonly tracestate?: string;

  readonly dlqreason?: "retryable" | "permanent" | "poison";
  readonly dlqattempts?: number;
  readonly dlqconsumer?: string;
  readonly dlqerror?: string;
  readonly dlqtime?: string;

  readonly data: T;
}

/** Atributos raíz permitidos. Todo lo demás va dentro de `data` — 01-envelope.md §3.3. */
export const ALLOWED_ROOT_ATTRIBUTES = new Set([
  "specversion", "id", "source", "type", "time", "datacontenttype",
  "dataschema", "subject", "correlationid", "tenantid", "producerversion",
  "dataclassification", "causationid", "partitionkey", "traceparent",
  "tracestate", "dlqreason", "dlqattempts", "dlqconsumer", "dlqerror",
  "dlqtime", "data",
]);

const REQUIRED_ATTRIBUTES = [
  "specversion", "id", "source", "type", "time",
  "datacontenttype", "dataschema", "data",
] as const;

export class EnvelopeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EnvelopeError";
  }
}

// ─── Construcción ────────────────────────────────────────────────────────────

export interface BuildEventInput<T> {
  subject: string;
  data: T;
  id: string;
  source: string;
  producerversion: string;
  tenantid: string;
  dataclassification: DataClassification;
  dataschema: string;
  correlationid: string;
  time?: string;
  aggregateId?: string;
  causationid?: string;
  partitionkey?: string;
  traceparent?: string;
  tracestate?: string;
}

export function buildEvent<T>(input: BuildEventInput<T>): FluxEvent<T> {
  if (input.data === null || typeof input.data !== "object" || Array.isArray(input.data)) {
    // Un array o escalar en la raíz impide añadir campos después sin romper el
    // esquema — 01-envelope.md §4.
    throw new EnvelopeError(
      "`data` debe ser un objeto JSON en la raíz (ni array ni escalar), para poder añadir campos sin romper el contrato",
    );
  }

  const partitionkey = input.partitionkey ?? input.aggregateId;

  const event: FluxEvent<T> = {
    specversion: "1.0",
    id: input.id,
    source: input.source,
    type: subjectToType(input.subject),
    time: input.time ?? new Date().toISOString(),
    datacontenttype: "application/json",
    dataschema: input.dataschema,
    ...(input.aggregateId !== undefined && { subject: input.aggregateId }),
    correlationid: input.correlationid,
    tenantid: input.tenantid,
    producerversion: input.producerversion,
    dataclassification: input.dataclassification,
    ...(input.causationid !== undefined && { causationid: input.causationid }),
    ...(partitionkey !== undefined && { partitionkey }),
    ...(input.traceparent !== undefined && { traceparent: input.traceparent }),
    ...(input.tracestate !== undefined && { tracestate: input.tracestate }),
    data: input.data,
  };

  return event;
}

const encoder = new TextEncoder();

export function serialize(event: FluxEvent): Uint8Array {
  const bytes = encoder.encode(JSON.stringify(event));
  if (bytes.byteLength > MAX_MESSAGE_BYTES) {
    // Fallar aquí y no en el broker: el mensaje del broker no dice qué hacer.
    throw new EnvelopeError(
      `evento de ${bytes.byteLength} bytes supera el límite de ${MAX_MESSAGE_BYTES} (1 MiB). ` +
        `Aplica claim-check: sube el contenido a almacenamiento de objetos y publica ` +
        `{ uri, sha256, bytes } en su lugar (01-envelope.md §6).`,
    );
  }
  return bytes;
}

// ─── Parseo ──────────────────────────────────────────────────────────────────

const decoder = new TextDecoder();

/**
 * Un fallo aquí es POISON: el mensaje ni siquiera es interpretable, así que nunca
 * llega al handler (04-errors.md §1.3).
 */
export function parseEvent(bytes: Uint8Array): FluxEvent {
  let raw: unknown;
  try {
    raw = JSON.parse(decoder.decode(bytes));
  } catch (cause) {
    throw new PoisonError("el cuerpo del mensaje no es JSON válido", {
      code: "MALFORMED_JSON",
      cause,
    });
  }

  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
    throw new PoisonError("el cuerpo del mensaje no es un objeto JSON", {
      code: "NOT_AN_OBJECT",
    });
  }

  const e = raw as Record<string, unknown>;

  if (e.specversion !== "1.0") {
    throw new PoisonError(
      `specversion no soportada: ${JSON.stringify(e.specversion)} (se esperaba "1.0")`,
      { code: "UNSUPPORTED_SPECVERSION" },
    );
  }

  const missing = REQUIRED_ATTRIBUTES.filter((a) => e[a] === undefined);
  if (missing.length > 0) {
    throw new PoisonError(
      `faltan atributos obligatorios de CloudEvents: ${missing.join(", ")}`,
      { code: "MISSING_REQUIRED_ATTRIBUTE" },
    );
  }

  if (e.datacontenttype !== "application/json") {
    throw new PoisonError(
      `datacontenttype no soportado: ${JSON.stringify(e.datacontenttype)}`,
      { code: "UNSUPPORTED_CONTENT_TYPE" },
    );
  }

  const unknown = Object.keys(e).filter((k) => !ALLOWED_ROOT_ATTRIBUTES.has(k));
  if (unknown.length > 0) {
    // Estricto a propósito: permitir atributos raíz ad-hoc lleva a serializar JSON
    // dentro de un string y el envelope deja de ser interoperable — 01-envelope.md §3.3.
    throw new PoisonError(
      `atributos raíz desconocidos: ${unknown.join(", ")}. Todo metadato adicional va dentro de \`data\``,
      { code: "UNKNOWN_ROOT_ATTRIBUTE" },
    );
  }

  return e as unknown as FluxEvent;
}

/**
 * Prepara un evento para la DLQ: el CloudEvent original íntegro más las extensiones
 * `dlq*`. Sin wrapper — así el replay es `del(.dlq*)` y republicar (04-errors.md §3).
 */
export function toDlqEvent(
  event: FluxEvent,
  info: {
    reason: "retryable" | "permanent" | "poison";
    attempts: number;
    consumer: string;
    error: string;
  },
): FluxEvent {
  // `data` va SIEMPRE al final, también aquí — 01-envelope.md §7.
  //
  // Un `{...event, dlq*}` deja las extensiones DESPUÉS de `data`, y entonces el mismo
  // evento serializado por Node y por Python produce secuencias de bytes distintas.
  // Ninguna es incorrecta como JSON, pero rompe el replay verbatim y cualquier firma
  // futura sobre el evento serializado.
  const { data, ...rest } = event;
  return {
    ...rest,
    dlqreason: info.reason,
    dlqattempts: info.attempts,
    dlqconsumer: info.consumer,
    dlqerror: info.error.slice(0, 1024),
    dlqtime: new Date().toISOString(),
    data,
  };
}

/** Quita las extensiones `dlq*`. El `id` original se conserva — 04-errors.md §4.1. */
export function stripDlqExtensions(event: FluxEvent): FluxEvent {
  const {
    dlqreason: _r, dlqattempts: _a, dlqconsumer: _c,
    dlqerror: _e, dlqtime: _t, ...rest
  } = event;
  return rest as FluxEvent;
}
