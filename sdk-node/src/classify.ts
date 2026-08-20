/**
 * Clasificación de errores del handler.
 * Contrato normativo: specification/04-errors.md §2
 *
 * Este fichero es el punto donde el protocolo se encuentra con la realidad operativa
 * del ecosistema. Todo lo demás en el SDK es mecánica; esto es política — y por eso
 * la política es un parámetro, no una constante.
 */

import {
  ErrorClass,
  type Classification,
  isClassifiedError,
} from "./errors.js";

/** Códigos de Node/libuv inequívocamente transitorios. */
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
 * Status HTTP que merecen reintento. Nótese qué NO está aquí: 400, 403, 404 y 422
 * son PERMANENT — reintentarlos es gastar 51 minutos para obtener exactamente la
 * misma respuesta.
 */
export const RETRYABLE_HTTP_STATUS = new Set([429, 502, 503, 504]);

export function extractHttpStatus(e: unknown): number | undefined {
  if (typeof e !== "object" || e === null) return undefined;
  const anyE = e as Record<string, unknown>;
  const candidate =
    anyE.status ??
    anyE.statusCode ??
    (anyE.response as Record<string, unknown> | undefined)?.status;
  return typeof candidate === "number" ? candidate : undefined;
}

/** Lee `Retry-After` (segundos) si la dependencia lo expone. */
export function extractRetryAfterMs(e: unknown): number | undefined {
  if (typeof e !== "object" || e === null) return undefined;
  const headers = (e as { response?: { headers?: unknown } })?.response?.headers as
    | Record<string, unknown>
    | { get?: (k: string) => unknown }
    | undefined;
  if (!headers) return undefined;
  const raw =
    (headers as Record<string, unknown>)["retry-after"] ??
    (headers as { get?: (k: string) => unknown }).get?.("retry-after");
  const seconds = Number(raw);
  return Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : undefined;
}

export function extractSyscallCode(e: unknown): string | undefined {
  const code = (e as Record<string, unknown> | null)?.code;
  return typeof code === "string" ? code : undefined;
}

function isTimeout(e: unknown): boolean {
  const name = (e as Error | null)?.name;
  return name === "AbortError" || name === "TimeoutError";
}

// ─────────────────────────────────────────────────────────────────────────────

export interface ClassifierOptions {
  /**
   * Qué hacer con un error que no encaja en ninguna regla conocida.
   *
   * `"retryable-bounded"` (default) — reintenta, pero con un presupuesto reducido
   * (`unknownRetryBudget`, 2 por defecto) en vez de los 6 intentos completos. Un
   * transitorio se recupera en el segundo intento; un sistemático llega a la DLQ en
   * ~30 s sin atascar la cola. Ver 04-errors.md §2.1.
   *
   * `"permanent"` — a la DLQ sin gastar reintentos. Falla rápido, pero un hipo de red
   * manda a la DLQ un evento perfectamente válido y alguien lo reproduce a mano.
   *
   * `"retryable"` — backoff completo, 51 minutos. Solo si vuestras dependencias tienen
   * hipos muy frecuentes y podéis asumir que un modo de fallo nuevo atasque la cola.
   *
   * @default "retryable-bounded"
   */
  unknownErrorPolicy?: "permanent" | "retryable" | "retryable-bounded";

  /**
   * Entregas máximas para un error desconocido cuando la política es
   * `"retryable-bounded"`. Incluye la primera entrega, así que 2 = un reintento.
   *
   * @default 2
   */
  unknownRetryBudget?: number;

  /**
   * Un timeout, ¿es "el mundo va lento" o "esta operación no cabe en la ventana"?
   *
   * El default es `"retryable"`: un timeout suele indicar saturación transitoria. Si
   * vuestros timeouts son casi siempre consultas que nunca van a terminar,
   * `"permanent"` evita reintentar lo imposible.
   *
   * @default "retryable"
   */
  timeoutPolicy?: "permanent" | "retryable";

  /** Reglas propias, evaluadas antes que todo lo demás. Devuelve `undefined` para ceder. */
  rules?: ReadonlyArray<(e: unknown) => Classification | undefined>;
}

/**
 * Construye un clasificador. El runtime del consumidor usa el resultado así:
 *
 *   RETRYABLE  → msg.nak(retryAfterMs ?? backoff canónico)
 *   PERMANENT  → msg.term() + publicar en dlq.<subject> con dlqattempts = 1
 *   POISON     → msg.term() + publicar en dlq.<subject> + alerta inmediata
 *
 * El orden de evaluación es deliberado: lo más específico primero, y el default al
 * final. Esa última línea es la decisión de política de verdad.
 */
export function createClassifier(
  options: ClassifierOptions = {},
): (e: unknown) => Classification {
  const policy = options.unknownErrorPolicy ?? "retryable-bounded";
  const unknownBudget = options.unknownRetryBudget ?? 2;
  const timeoutClass =
    options.timeoutPolicy === "permanent"
      ? ErrorClass.PERMANENT
      : ErrorClass.RETRYABLE;
  const rules = options.rules ?? [];

  return function classify(e: unknown): Classification {
    // 1. Una excepción tipada de flux siempre gana: la aplicación sabe más que el SDK.
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

    // 2. Reglas de la aplicación.
    for (const rule of rules) {
      const r = rule(e);
      if (r) return r;
    }

    // 3. Status HTTP: la señal más fiable que da una dependencia.
    const status = extractHttpStatus(e);
    if (status !== undefined) {
      const retryable = RETRYABLE_HTTP_STATUS.has(status);
      return {
        class: retryable ? ErrorClass.RETRYABLE : ErrorClass.PERMANENT,
        code: `HTTP_${status}`,
        ...(retryable && { retryAfterMs: extractRetryAfterMs(e) }),
      };
    }

    // 4. Errores de sistema: red y DNS son transitorios por definición.
    const syscall = extractSyscallCode(e);
    if (syscall !== undefined && TRANSIENT_SYSCALL_CODES.has(syscall)) {
      return { class: ErrorClass.RETRYABLE, code: syscall };
    }

    // 5. Timeouts — política configurable.
    if (isTimeout(e)) {
      return { class: timeoutClass, code: "TIMEOUT" };
    }

    // 6. Lo desconocido. Aquí se decide el comportamiento del ecosistema ante lo que
    //    nadie previó. El default acotado da al transitorio una segunda oportunidad
    //    sin regalarle 51 minutos de cola al sistemático — 04-errors.md §2.1.
    const code = syscall ?? "UNKNOWN";
    if (policy === "permanent") return { class: ErrorClass.PERMANENT, code };
    if (policy === "retryable") return { class: ErrorClass.RETRYABLE, code };
    return { class: ErrorClass.RETRYABLE, code, maxAttempts: unknownBudget };
  };
}

/** Clasificador con los defaults de la spec. */
export const classify = createClassifier();
