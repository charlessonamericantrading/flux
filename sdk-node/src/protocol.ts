/**
 * Constantes y derivaciones del protocolo.
 * Contrato normativo: specification/02-naming.md, specification/03-delivery.md
 *
 * Todo lo de aquí sale de protocol.json. Si divergen, protocol.json manda —
 * es lo que consumen los demás SDKs y los agentes.
 */

import { ReservedDomainError } from "./state-errors.js";

const NS = 1_000_000n;
const ns = (ms: number) => Number(BigInt(ms) * NS);

/** Configuración canónica de consumidor. Ver 03-delivery.md §2. */
export const CONSUMER_DEFAULTS = {
  ackPolicy: "explicit",
  /**
   * ⚠️ ack_wait DEBE ser igual a backoff[0]: JetStream lo sobrescribe con ese valor
   * sin devolver error. Ver 03-delivery.md §2.1 y
   * conformance/cases/consumer-config.json.
   */
  ackWaitMs: 30_000,
  maxDeliver: 6,
  backoffMs: [30_000, 60_000, 300_000, 900_000, 1_800_000],
  maxAckPending: 256,
} as const;

export const STREAM_DEFAULTS = {
  maxAgeMs: 30 * 24 * 60 * 60 * 1000,
  dlqMaxAgeMs: 90 * 24 * 60 * 60 * 1000,
  duplicateWindowMs: 120_000,
} as const;

export const MAX_MESSAGE_BYTES = 1_048_576;

export const nanos = ns;

// ─── Naming ──────────────────────────────────────────────────────────────────

/**
 * Dominios que NO son dominios — 02-naming.md §1.3, protocol.json `naming.reservedDomains`.
 *
 * Cada uno es un espacio de nombres del protocolo (`dlq.`, `state.`, `rpc.`), y un dominio con
 * ese nombre colisionaría con él **en el broker**: con `STATE_CLIENTES` (`state.clientes.>`)
 * creado, un `EVT_STATE` (`state.>`) falla con `subjects overlap with an existing stream`
 * (err 10065, A-43). Peor: un evento publicado en `state.` no pasaría por el chequeo de
 * secuencia del estado, y uno de core sobrescribe sin acuse (A-45).
 */
export const RESERVED_DOMAINS: readonly string[] = ["dlq", "state", "rpc", "system"];

/**
 * Lanza `RESERVED_DOMAIN` si el primer token de un subject o un contrato es un dominio
 * reservado. Se comprueba ANTES que la forma: `dlq.pedidos.pedido.v1.creado` tiene cinco tokens
 * y «debe tener cuatro» no diría qué pasa de verdad.
 */
export function assertNotReservedDomain(
  subjectOrContract: string,
  what: "subject" | "contrato" | "operación" = "subject",
): void {
  const domain = String(subjectOrContract).split(".")[0];
  if (RESERVED_DOMAINS.includes(domain)) throw new ReservedDomainError(domain, RESERVED_DOMAINS, what);
}

/** `<dominio>.<agregado>.v<major>.<evento>` — 02-naming.md §1. La versión mayor son de 1 a 4 dígitos (`v1`…`v9999`, sin ceros a la izquierda). */
export const SUBJECT_PATTERN =
  /^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]{0,3}\.[a-z0-9]+(-[a-z0-9]+)*$/;

export interface ParsedSubject {
  readonly domain: string;
  readonly aggregate: string;
  readonly major: number;
  readonly event: string;
}

export class InvalidSubjectError extends Error {
  constructor(subject: string, reason: string) {
    super(`subject inválido "${subject}": ${reason}`);
    this.name = "InvalidSubjectError";
  }
}

export function parseSubject(subject: string): ParsedSubject {
  // Todo lo que recibe un subject —publish, subscribe, durableName…— pasa por aquí: un dominio
  // reservado se rechaza antes de tocar la red (10-state.md §2.7).
  assertNotReservedDomain(subject);
  if (subject !== subject.toLowerCase()) {
    // Se comprueba antes que el patrón para dar un mensaje útil: los subjects de NATS
    // son case-sensitive y una mayúscula crea un subject fantasma SIN ningún error.
    throw new InvalidSubjectError(
      subject,
      "debe ir todo en minúsculas — NATS es case-sensitive y una mayúscula crea un subject al que nadie está suscrito, sin producir error",
    );
  }
  if (!SUBJECT_PATTERN.test(subject)) {
    const n = subject.split(".").length;
    throw new InvalidSubjectError(
      subject,
      n !== 4
        ? `debe tener exactamente 4 tokens (<dominio>.<agregado>.v<major>.<evento>), tiene ${n}`
        : "formato esperado <dominio>.<agregado>.v<major>.<evento> en kebab-case (la versión mayor, de 1 a 4 dígitos: v1…v9999)",
    );
  }
  const [domain, aggregate, v, event] = subject.split(".");
  return { domain, aggregate, major: Number(v.slice(1)), event };
}

export function isValidSubject(subject: string): boolean {
  try {
    parseSubject(subject);
    return true;
  } catch {
    return false;
  }
}

/** `pedidos.pedido.v1.creado` → `com.flux.pedidos.pedido.creado.v1` — 02-naming.md §2 */
export function subjectToType(subject: string): string {
  const { domain, aggregate, major, event } = parseSubject(subject);
  return `com.flux.${domain}.${aggregate}.${event}.v${major}`;
}

/** `EVT_PEDIDOS`. NATS no admite puntos en nombres de stream — 02-naming.md §3. */
export function streamName(domain: string): string {
  return `EVT_${domain.replace(/-/g, "_").toUpperCase()}`;
}

export function dlqStreamName(domain: string): string {
  return `DLQ_${domain.replace(/-/g, "_").toUpperCase()}`;
}

/** Un nombre de servicio válido: `[a-z0-9-]+`. */
export const SERVICE_PATTERN = /^[a-z0-9]+(-[a-z0-9]+)*$/;

export class InvalidServiceNameError extends Error {
  constructor(service: string) {
    super(
      `nombre de servicio inválido "${service}": debe ser kebab-case en minúsculas ([a-z0-9-]). ` +
        `NATS aceptaría el durable resultante, pero incumpliría el patrón del protocolo ` +
        `y rompería el filtrado por servicio en \`nats consumer ls\` (02-naming.md §4).`,
    );
    this.name = "InvalidServiceNameError";
  }
}

/**
 * `facturacion-api__pedidos_pedido_v1_creado`.
 * NATS rechaza `.`, `*` y `>` en nombres de durable — 02-naming.md §4.
 *
 * El servicio se valida aquí y no solo el subject: NATS aceptaría sin rechistar un
 * durable como `FacturacionAPI__pedidos_…`, y el incumplimiento del patrón solo se
 * descubriría al intentar parsear nombres de consumidor en una herramienta.
 */
export function durableName(service: string, subject: string): string {
  if (!SERVICE_PATTERN.test(service)) throw new InvalidServiceNameError(service);
  parseSubject(subject);
  return `${service}__${subject.replace(/\./g, "_").replace(/-/g, "_")}`;
}

/**
 * `dlq.pedidos.pedido.v1.creado`.
 * PREFIJO, no sufijo: un sufijo encajaría con `<dominio>.>` y el stream principal
 * capturaría sus propios muertos — 02-naming.md §3.1.
 */
export function dlqSubject(subject: string): string {
  return `dlq.${subject}`;
}

export function isDlqSubject(subject: string): boolean {
  return subject.startsWith("dlq.");
}

/** `/produccion/pedidos-api` — 01-envelope.md §2 */
export function sourceUri(environment: string, service: string): string {
  return `/${environment}/${service}`;
}
