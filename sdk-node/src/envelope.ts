/**
 * Envelope CloudEvents 1.0 — construcción, serialización y parseo.
 * Contrato normativo: specification/01-envelope.md
 */

import { PoisonError } from "./errors.js";
import { MAX_MESSAGE_BYTES, subjectToType } from "./protocol.js";
import { InvalidDataClassificationError } from "./state-errors.js";
import { truncateUtf8Bytes } from "./text.js";
import { canonicalTime } from "./time.js";

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
  "dlqtime", "signkeyid", "signature", "data",
]);

const REQUIRED_ATTRIBUTES = [
  "specversion", "id", "source", "type", "time",
  "datacontenttype", "dataschema", "data",
] as const;

/**
 * Extensiones del perfil flux cuya ausencia es POISON — 01-envelope.md §3.1.
 *
 * Se exigen de verdad, no solo en la prosa. Si no se exigieran no serían
 * obligatorias, serían recomendadas, y cada consumidor tendría que tratar el caso
 * ausente — que es exactamente lo que declararlas obligatorias pretendía evitar.
 *
 * Además, asumir un valor por defecto es peligroso en las cuatro: un
 * `dataclassification` ausente tomado como `internal` hace que PII circule con 30
 * días de retención en vez de 7, y un `tenantid` ausente tomado como `system` cruza
 * fronteras de tenant. Ver 06-security.md §4 y §5.
 */
const REQUIRED_EXTENSIONS = [
  "correlationid", "tenantid", "producerversion", "dataclassification",
] as const;

const VALID_CLASSIFICATIONS = new Set([
  "public", "internal", "confidential", "restricted",
]);

export class EnvelopeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EnvelopeError";
  }
}

/**
 * Un constructor se niega a construir un sobre: falta (o está vacío) un atributo que el SDK rellena, o `time` no es un
 * instante. Lleva el **mismo código estable** que daría el parser ante el mismo defecto (01-envelope.md §3.1): un atributo
 * obligatorio vacío es `MISSING_REQUIRED_ATTRIBUTE`; una extensión obligatoria vacía, `MISSING_REQUIRED_EXTENSION`; un `time`
 * que no es un instante RFC 3339, `WRONG_ATTRIBUTE_TYPE` (CloudEvents lo tipa `Timestamp`). El arnés de conformidad compara ese
 * código entre SDKs.
 */
export class InvalidEnvelopeInputError extends EnvelopeError {
  readonly code: string;
  readonly opts: { readonly code: string };
  constructor(message: string, code: "MISSING_REQUIRED_ATTRIBUTE" | "MISSING_REQUIRED_EXTENSION" | "WRONG_ATTRIBUTE_TYPE") {
    super(message);
    this.name = "InvalidEnvelopeInputError";
    this.code = code;
    this.opts = { code };
  }
}

/** `[nombre, valor, esExtension]`: lo que un constructor comprueba que no llegó vacío. */
export type AtributoObligatorio = readonly [nombre: string, valor: unknown, extension?: boolean];

/**
 * Los atributos que el SDK rellena NO pueden llegar vacíos a un constructor: una cadena vacía y una ausente son lo mismo
 * (01-envelope.md §3.1, §3.3), y un evento que su propio parser rechazaría no se construye. `rechaza` decide el error: el de
 * eventos y estado lleva el código del parser; el de petición/respuesta, `INVALID_RPC_CALL` (su conjunto de códigos es cerrado).
 */
export function exigirAtributos(
  campos: readonly AtributoObligatorio[],
  rechaza: (mensaje: string, code: "MISSING_REQUIRED_ATTRIBUTE" | "MISSING_REQUIRED_EXTENSION") => Error,
): void {
  for (const [nombre, valor, extension] of campos) {
    if (typeof valor !== "string" || valor === "") {
      throw rechaza(
        `\`${nombre}\` es obligatorio y llegó ${valor === undefined || valor === null ? "ausente" : typeof valor === "string" ? "vacío" : `como ${typeof valor}`}. ` +
          `Lo rellena el SDK a partir de connect() y del evento en curso; si estás llamando a un constructor a mano, rellénalo ` +
          `con una cadena no vacía (01-envelope.md §5).`,
        extension ? "MISSING_REQUIRED_EXTENSION" : "MISSING_REQUIRED_ATTRIBUTE",
      );
    }
  }
}

/**
 * `time` en su forma canónica (3 decimales y `Z`, UTC), o el error de `rechaza` si no es un instante RFC 3339 válido.
 * Ver time.ts: se admite lo mismo que el SDK de Rust.
 */
export function tiempoCanonico(time: unknown, rechaza: (mensaje: string) => Error): string {
  const c = canonicalTime(time);
  if (c === null) {
    throw rechaza(
      `\`time\` debe ser un instante RFC 3339 (\`2026-08-20T10:41:07.318Z\`; se normaliza a 3 decimales y \`Z\`) y llegó ` +
        `${typeof time === "string" ? "un texto que no lo es" : `un valor de tipo ${time === null ? "null" : typeof time}`} (01-envelope.md §2.2).`,
    );
  }
  return c;
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
  const type = subjectToType(input.subject);
  if (!VALID_CLASSIFICATIONS.has(input.dataclassification)) {
    throw new InvalidDataClassificationError(input.dataclassification, "rpc");
  }
  exigirAtributos(
    [
      ["id", input.id],
      ["source", input.source],
      ["dataschema", input.dataschema],
      ["correlationid", input.correlationid, true],
      ["tenantid", input.tenantid, true],
      ["producerversion", input.producerversion, true],
    ],
    (m, c) => new InvalidEnvelopeInputError(m, c),
  );
  // `time` de quien llama se normaliza a la forma canónica o se rechaza; sin él, el reloj del sistema.
  const time =
    input.time !== undefined
      ? tiempoCanonico(input.time, (m) => new InvalidEnvelopeInputError(m, "WRONG_ATTRIBUTE_TYPE"))
      : new Date().toISOString();

  const partitionkey = input.partitionkey ?? input.aggregateId;

  const event: FluxEvent<T> = {
    specversion: "1.0",
    id: input.id,
    source: input.source,
    type,
    time,
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

/**
 * UTF-8 ESTRICTO. `new TextDecoder()` por defecto sustituye cada byte inválido por U+FFFD y se COME un BOM inicial, así que un
 * mensaje que no es UTF-8 —o que empieza por una marca de orden de bytes— se interpretaba como JSON válido, y el SDK de Rust
 * (que no lo es) lo rechazaba: el mismo mensaje, dos veredictos. Un cuerpo que no es UTF-8 no es JSON (RFC 8259 §8.1) y un BOM
 * tampoco (§8.1: «las implementaciones NO DEBEN añadir una marca»): ambos son `MALFORMED_JSON`.
 */
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

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

  // `null` cuenta como ausente: 01-envelope.md §4 prohíbe usar null para "no aplica",
  // así que un atributo obligatorio a null está igual de mal que no estar.
  const missing = REQUIRED_ATTRIBUTES.filter((a) => e[a] === undefined || e[a] === null);
  if (missing.length > 0) {
    throw new PoisonError(
      `faltan atributos obligatorios de CloudEvents: ${missing.join(", ")}`,
      { code: "MISSING_REQUIRED_ATTRIBUTE" },
    );
  }

  const missingExt = REQUIRED_EXTENSIONS.filter(
    (a) => e[a] === undefined || e[a] === null || e[a] === "",
  );
  if (missingExt.length > 0) {
    throw new PoisonError(
      `faltan extensiones obligatorias del perfil flux: ${missingExt.join(", ")}. ` +
        `Su ausencia no es recuperable asumiendo un valor: un dataclassification ` +
        `ausente tomado como "internal" haría circular PII con la retención larga ` +
        `(01-envelope.md §3.1)`,
      { code: "MISSING_REQUIRED_EXTENSION" },
    );
  }

  if (!VALID_CLASSIFICATIONS.has(e.dataclassification as string)) {
    throw new PoisonError(
      `dataclassification inválido: ${JSON.stringify(e.dataclassification)}. ` +
        `Valores permitidos: ${[...VALID_CLASSIFICATIONS].join(", ")}`,
      { code: "INVALID_DATACLASSIFICATION" },
    );
  }

  // Los tipos son exactos: {"tenantid": 42} es POISON, no el tenant "42".
  // Los deserializadores discrepan por defecto (Jackson coacciona, Go rechaza, Node
  // propaga el número), y el que "funciona" es el peor — 01-envelope.md §2.4.
  for (const a of ["id", "source", "type", "time", "correlationid", "tenantid", "producerversion"]) {
    if (typeof e[a] !== "string") {
      throw new PoisonError(
        `${a} debe ser una cadena, llegó ${typeof e[a]} (${JSON.stringify(e[a])})`,
        { code: "WRONG_ATTRIBUTE_TYPE" },
      );
    }
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

  // Los tipos son exactos TAMBIÉN en lo no obligatorio: `{"subject": 5}` o `{"dlqattempts": "seis"}` no se propagan. Es el último
  // código de `parseEvent` (el orden es parte del contrato entre SDKs) y el mismo de Rust, Java y .NET.
  const mal = atributoConTipoIncompatible(e);
  if (mal !== undefined) {
    throw new PoisonError(
      `${mal} tiene un tipo incompatible con el contrato (01-envelope.md §2.4): ${JSON.stringify(typeof e[mal] === "object" ? "un objeto o un array" : e[mal]).slice(0, 60)}`,
      { code: "INVALID_ATTRIBUTE_TYPE" },
    );
  }

  return e as unknown as FluxEvent;
}

const CADENAS_NO_OBLIGATORIAS = [
  "dataschema", "subject", "causationid", "partitionkey", "traceparent", "tracestate",
  "dlqconsumer", "dlqerror", "dlqtime", "signkeyid", "signature",
] as const;

/**
 * El primer atributo conocido —ni de los siete cadena obligatorios— con un tipo incompatible, o `undefined`. `null` es «ausente»
 * en un opcional (como en Rust). `dlqattempts` es un entero de 0 a 2^32−1 (`u32`) y `dlqreason`, una de las tres clases.
 */
function atributoConTipoIncompatible(e: Record<string, unknown>): string | undefined {
  for (const a of CADENAS_NO_OBLIGATORIAS) {
    if (e[a] !== undefined && e[a] !== null && typeof e[a] !== "string") return a;
  }
  const r = e.dlqreason;
  if (r !== undefined && r !== null && r !== "retryable" && r !== "permanent" && r !== "poison") return "dlqreason";
  const n = e.dlqattempts;
  if (n !== undefined && n !== null && (typeof n !== "number" || !Number.isInteger(n) || n < 0 || n > 4294967295)) return "dlqattempts";
  return undefined;
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
  /** El instante de la muerte, si no es «ahora» (lo fija el arnés de conformidad). Se normaliza a la forma canónica o se rechaza. */
  dlqtime?: string,
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
    // 1024 BYTES UTF-8 sin partir un carácter (04-errors.md §3): no unidades UTF-16, que cortarían un emoji por la mitad.
    dlqerror: truncateUtf8Bytes(info.error, 1024),
    dlqtime:
      dlqtime !== undefined
        ? tiempoCanonico(dlqtime, (m) => new InvalidEnvelopeInputError(m, "WRONG_ATTRIBUTE_TYPE"))
        : new Date().toISOString(),
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
