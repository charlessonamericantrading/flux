/**
 * Clasificación de errores del handler.
 * Contrato normativo: specification/04-errors.md §2
 *
 * Este fichero es el punto donde el protocolo se encuentra con la realidad operativa
 * del ecosistema. Todo lo demás en el SDK es mecánica; esto es política.
 */

import {
  ErrorClass,
  type Classification,
  isClassifiedError,
} from "./errors.js";

/**
 * Códigos de error de Node/libuv que son inequívocamente transitorios.
 * Referencia para la implementación de `classify`.
 */
export const TRANSIENT_SYSCALL_CODES = new Set([
  "ECONNRESET",
  "ECONNREFUSED",
  "ETIMEDOUT",
  "EPIPE",
  "EHOSTUNREACH",
  "ENETUNREACH",
  "EAI_AGAIN", // fallo temporal de DNS
]);

/**
 * Códigos HTTP que merecen reintento. Nótese qué NO está aquui:
 * 400, 403, 404, 422 son PERMANENT — reintentarlos es gastar 12 minutos
 * para obtener exactamente la misma respuesta.
 */
export const RETRYABLE_HTTP_STATUS = new Set([429, 502, 503, 504]);

/** Formas habituales en que una dependencia expone su status HTTP. */
export function extractHttpStatus(e: unknown): number | undefined {
  if (typeof e !== "object" || e === null) return undefined;
  const anyE = e as Record<string, unknown>;
  const candidate =
    anyE.status ??
    anyE.statusCode ??
    (anyE.response as Record<string, unknown> | undefined)?.status;
  return typeof candidate === "number" ? candidate : undefined;
}

/** Extrae el header Retry-After (en segundos) si la dependencia lo expone. */
export function extractRetryAfterMs(e: unknown): number | undefined {
  if (typeof e !== "object" || e === null) return undefined;
  const headers = (e as any)?.response?.headers;
  const raw = headers?.["retry-after"] ?? headers?.get?.("retry-after");
  const seconds = Number(raw);
  return Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : undefined;
}

/** Extrae el `code` de un error de sistema (Node lo pone en `.code`). */
export function extractSyscallCode(e: unknown): string | undefined {
  const code = (e as Record<string, unknown> | null)?.code;
  return typeof code === "string" ? code : undefined;
}

// ─────────────────────────────────────────────────────────────────────────────

/**
 * Traduce cualquier error lanzado por un handler a una de las tres clases del
 * protocolo. El runtime del consumidor usa el resultado así:
 *
 *   RETRYABLE  → msg.nak(retryAfterMs ?? backoff canónico)
 *   PERMANENT  → msg.term() + publicar en dlq.<subject> con dlqattempts = 1
 *   POISON     → msg.term() + publicar en dlq.<subject> + alerta inmediata
 *
 * ── POR QUÉ ESTA DECISIÓN ES TUYA ───────────────────────────────────────────
 *
 * La spec fija el default en PERMANENT (04-errors.md §2): un evento en la DLQ es
 * recuperable, una cola atascada en hora punta no lo es. Pero ese default es
 * deliberadamente conservador, y el equilibrio correcto depende de cómo son
 * REALMENTE vuestras dependencias:
 *
 *   · Si vuestros servicios internos tienen hipos de red frecuentes, un default
 *     PERMANENT demasiado agresivo llenará la DLQ de eventos perfectamente
 *     válidos, y alguien tendrá que reproducirlos a mano cada mañana.
 *
 *   · Si vuestras dependencias fallan de forma limpia y determinista, ser
 *     generoso con RETRYABLE solo sirve para retrasar 12 minutos el momento en
 *     que alguien se entera de que algo está roto.
 *
 * Y hay una tercera opción que la spec no impone: tratar los errores DESCONOCIDOS
 * como RETRYABLE pero con un presupuesto reducido (p.ej. 2 intentos en vez de 5),
 * para no atascar la cola pero tampoco descartar por un hipo. Requiere devolver
 * un `retryAfterMs` y que el runtime respete un `max_deliver` menor.
 *
 * ── QUÉ IMPLEMENTAR ─────────────────────────────────────────────────────────
 *
 * Sustituye el cuerpo del TODO. Unas 8-10 líneas. Orden sugerido:
 *
 *   1. Errores que ya declaran su clase → respetarla (ya resuelto abajo).
 *   2. Status HTTP → RETRYABLE_HTTP_STATUS decide; el resto, PERMANENT.
 *   3. Código syscall → TRANSIENT_SYSCALL_CODES decide.
 *   4. AbortError / TimeoutError → decide tú: ¿un timeout es "el mundo va lento"
 *      (RETRYABLE) o "esta operación no cabe en la ventana" (PERMANENT)?
 *   5. Todo lo demás → tu default. Esta línea es la decisión de verdad.
 *
 * El `code` que devuelvas acaba en la extensión `dlqerror` del evento en la DLQ y
 * en las métricas, así que hazlo estable y agrupable ("HTTP_503", "ECONNRESET",
 * "UNKNOWN") — no metas ahí el mensaje completo, que es de cardinalidad infinita.
 */
export function classify(e: unknown): Classification {
  // Una excepción tipada de flux siempre gana: la aplicación sabe más que nosotros.
  if (isClassifiedError(e)) {
    return {
      class: e.fluxClass,
      code: e.opts.code ?? e.name,
      retryAfterMs:
        e.fluxClass === ErrorClass.RETRYABLE
          ? (e.opts as { retryAfterMs?: number }).retryAfterMs
          : undefined,
    };
  }

  // TODO(equipo): implementar la clasificación de errores no tipados.
  //
  //   const status = extractHttpStatus(e);
  //   if (status !== undefined) { ... }
  //
  //   const syscall = extractSyscallCode(e);
  //   if (syscall !== undefined) { ... }
  //
  //   ¿timeouts?
  //
  //   return { class: ???, code: "UNKNOWN" };   ← la línea que de verdad decide
  //
  throw new Error("classify(): sin implementar — ver la nota de este fichero");
}
