/**
 * El `id` DETERMINISTA de un hecho importado de un sistema externo.
 * Contrato normativo: specification/01-envelope.md §2.6.1.
 *
 * Un proveedor (una pasarela de pagos, un sistema de terceros) reenvía el mismo aviso durante horas. Si cada reenvío
 * recibe un `id` nuevo, ningún consumidor puede deduplicarlo: el `id` es la clave de deduplicación del ecosistema. Para
 * un hecho importado, el `id` se DERIVA de lo que identifica al hecho, y es siempre el mismo:
 *
 *   id = UUIDv7 cuyos 48 bits de tiempo son los milisegundos del `time` del hecho externo y cuyos 74 bits restantes
 *        (`rand_a` y `rand_b`) son los 74 primeros bits de SHA-256( <source> "|" <clave externa> ).
 *
 * Es un UUIDv7 VÁLIDO (versión 7, variante 10): cualquier consumidor que valide el formato del `id` lo acepta, y ordenar
 * por `id` dentro de un `source` sigue siendo ordenar por el instante del hecho (§2.6). Es una función PURA y no usa el
 * reloj ni ninguna fuente de azar: es lo que el arnés cruzado compara byte a byte entre SDKs.
 */

import { createHash } from "node:crypto";

import { EnvelopeError } from "./envelope.js";

/** 2^48 − 1: lo que cabe en el campo de tiempo de un UUIDv7. */
export const UUIDV7_MAX_MS = 2 ** 48 - 1;

/** Longitud máxima de una clave externa, en bytes UTF-8: es un identificador, no un documento. */
export const EXTERNAL_KEY_MAX_BYTES = 512;

export interface ExternalIdInput {
  /** El `source` del que publica: `/<entorno>/<servicio>`. No lleva `|`, así que el separador no es ambiguo. */
  source: string;
  /** La clave del hecho en el sistema externo (`evt_1Nx…`). No vacía, como máximo 512 bytes UTF-8. */
  externalKey: string;
  /** El instante del hecho externo. Un `Date`, un RFC 3339 o los milisegundos desde la época. */
  time: Date | string | number;
}

/** Los milisegundos de un instante, o `EnvelopeError` si no es un instante válido que quepa en 48 bits. */
export function instantToMs(time: Date | string | number): number {
  const ms = time instanceof Date ? time.getTime() : typeof time === "number" ? time : Date.parse(time);
  if (!Number.isInteger(ms) || ms < 0 || ms > UUIDV7_MAX_MS) {
    throw new EnvelopeError(
      `\`time\` no es un instante válido para el id determinista (se necesitan milisegundos enteros entre 0 y ${UUIDV7_MAX_MS}). ` +
        `Pasa el instante del hecho externo como RFC 3339 UTC o como \`Date\` (01-envelope.md §2.6.1)`,
    );
  }
  return ms;
}

/**
 * `SHA-256(<source> "|" <externalKey>)`, y de ahí el UUIDv7. Falla con `EnvelopeError` ante una clave vacía o demasiado
 * larga, un `source` con `|` o un instante inválido.
 */
export function externalEventId({ source, externalKey, time }: ExternalIdInput): string {
  if (typeof source !== "string" || source === "" || source.includes("|")) {
    throw new EnvelopeError("`source` debe ser una cadena no vacía y sin `|` (es el separador de la derivación del id determinista)");
  }
  if (typeof externalKey !== "string" || externalKey === "") {
    throw new EnvelopeError(
      "`externalKey` debe ser una cadena no vacía: es la clave del hecho en el sistema externo (el id del evento del proveedor). " +
        "Sin ella no hay nada de lo que derivar un `id` estable (01-envelope.md §2.6.1)",
    );
  }
  if (Buffer.byteLength(externalKey, "utf8") > EXTERNAL_KEY_MAX_BYTES) {
    throw new EnvelopeError(
      `\`externalKey\` ocupa más de ${EXTERNAL_KEY_MAX_BYTES} bytes UTF-8: es un identificador, no un documento. Usa el id del hecho en el sistema externo`,
    );
  }
  const ms = instantToMs(time);

  const digest = createHash("sha256").update(`${source}|${externalKey}`, "utf8").digest();
  // Los 74 primeros bits del digest: 80 bits (10 bytes) menos los 6 últimos.
  const r = BigInt(`0x${digest.subarray(0, 10).toString("hex")}`) >> 6n;
  const randA = r >> 62n; // bits 0..11
  const randB = r & ((1n << 62n) - 1n); // bits 12..73
  const uuid = (BigInt(ms) << 80n) | (0x7n << 76n) | (randA << 64n) | (0b10n << 62n) | randB;
  const hex = uuid.toString(16).padStart(32, "0");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
