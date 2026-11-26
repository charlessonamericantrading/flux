/**
 * Los errores HTTP del gateway: traducen los CÓDIGOS ESTABLES del protocolo a un estado y a un cuerpo.
 * Contrato (implementación): specification/12-edge.md §6.4 y `protocol.json` → `edge.gateway.errors`.
 *
 * La regla de los estados: 4xx = el cliente lo ha hecho mal y repetir igual da lo mismo (salvo 408, 409 y 429); 5xx = el
 * fallo es del gateway o del ecosistema y `class` dice si reintentar sirve. 502, 503 y 504 son SIEMPRE reintentables: muchos
 * clientes y proxies los reintentan solos.
 *
 * El cuerpo de un error es `{ "error": { code, class, message, retryAfterMs? } }`. El `message` dice qué y cómo se arregla y
 * NUNCA cita valores de `data` (los del SDK ya cumplen 04-errors.md §5.2). Una excepción que el gateway no sabe clasificar
 * se contesta con un mensaje GENÉRICO: no se reenvía su texto, que puede llevar rutas, valores o datos personales.
 */

import {
  EnvelopeError,
  InvalidSubjectError,
  PermanentError,
  RetryableError,
} from "@flux/sdk";

/** Un error ya traducido: lo que se escribe en la respuesta. */
export class ErrorHttp extends Error {
  /**
   * @param {number} status
   * @param {string} code
   * @param {"retryable"|"permanent"} clase
   * @param {string} message
   * @param {{ retryAfterMs?: number, headers?: Record<string,string>, ambiguo?: boolean }} [extra]
   */
  constructor(status, code, clase, message, extra = {}) {
    super(message);
    this.name = "ErrorHttp";
    this.status = status;
    this.code = code;
    this.clase = clase;
    this.retryAfterMs = extra.retryAfterMs;
    this.headers = extra.headers ?? {};
    this.ambiguo = extra.ambiguo === true;
  }
}

/** Los códigos que solo existen en el borde (`edge.gateway.errors.codes`): no son del protocolo. */
export const CODIGOS_EDGE = Object.freeze({
  EDGE_UNAUTHENTICATED: { status: 401, clase: "permanent" },
  EDGE_RATE_LIMITED: { status: 429, clase: "retryable" },
  EDGE_BODY_TOO_LARGE: { status: 413, clase: "permanent" },
  EDGE_INVALID_BODY: { status: 400, clase: "permanent" },
  EDGE_UNSUPPORTED_MEDIA_TYPE: { status: 415, clase: "permanent" },
  EDGE_INVALID_REQUEST: { status: 400, clase: "permanent" },
  EDGE_TENANT_NOT_ALLOWED: { status: 403, clase: "permanent" },
  EDGE_NOT_FOUND: { status: 404, clase: "permanent" },
  EDGE_METHOD_NOT_ALLOWED: { status: 405, clase: "permanent" },
  EDGE_INVALID_WEBHOOK_SIGNATURE: { status: 401, clase: "permanent" },
  EDGE_WEBHOOK_TIMESTAMP_OUT_OF_TOLERANCE: { status: 401, clase: "permanent" },
  EDGE_WEBHOOK_PAYLOAD_INVALID: { status: 422, clase: "permanent" },
  EDGE_SHUTTING_DOWN: { status: 503, clase: "retryable" },
  EDGE_INTERNAL: { status: 500, clase: "permanent" },
});

/** Los códigos del protocolo que pueden salir por HTTP, con su estado y su clase (`edge.gateway.errors.protocolCodes`). */
export const CODIGOS_PROTOCOLO = Object.freeze({
  INVALID_SCHEMA: { status: 422, clase: "permanent" },
  INVALID_DATACLASSIFICATION: { status: 400, clase: "permanent" },
  SCHEMA_NOT_FOUND: { status: 500, clase: "permanent" },
  RESERVED_DOMAIN: { status: 400, clase: "permanent" },
  INVALID_STATE_CONTRACT: { status: 400, clase: "permanent" },
  INVALID_STATE_KEY: { status: 400, clase: "permanent" },
  INVALID_TENANT_FOR_STATE: { status: 400, clase: "permanent" },
  MISSING_AGGREGATE_VERSION: { status: 422, clase: "permanent" },
  STATE_CONFLICT: { status: 409, clase: "retryable" },
  INVALID_RPC_CALL: { status: 400, clase: "permanent" },
  RPC_UNAVAILABLE: { status: 503, clase: "retryable", retryAfterMs: 1000 },
  RPC_TIMEOUT: { status: 504, clase: "retryable", ambiguo: true },
  RPC_MESSAGE_TOO_LARGE: { status: 413, clase: "permanent" },
  RPC_INVALID_REPLY: { status: 500, clase: "permanent" },
  STALE_REQUEST: { status: 400, clase: "permanent" },
  PERMISSION_DENIED: { status: 403, clase: "permanent" },
  NOT_PROVISIONED: { status: 500, clase: "permanent" },
});

/** Un error del borde, por su código. */
export function errorEdge(code, message, extra = {}) {
  const def = CODIGOS_EDGE[code];
  if (!def) throw new Error(`código del borde desconocido: ${code}`);
  return new ErrorHttp(def.status, code, def.clase, message, extra);
}

const aSegundos = (ms) => String(Math.max(1, Math.ceil(ms / 1000)));

/**
 * Traduce lo que lanza el SDK (o el propio gateway) a un `ErrorHttp`. Nunca lanza.
 * @param {unknown} e
 * @returns {ErrorHttp}
 */
export function traducir(e) {
  if (e instanceof ErrorHttp) return e;

  const code = typeof e?.code === "string" ? e.code : typeof e?.opts?.code === "string" ? e.opts.code : undefined;
  const mensaje = typeof e?.message === "string" ? e.message : "error sin mensaje";

  // 1. Un código estable del protocolo: la tabla.
  const def = code ? CODIGOS_PROTOCOLO[code] : undefined;
  if (def) {
    // Un error que llega DEL QUE RESPONDE (RpcRetryableError / RpcPermanentError) con el code de la tabla es el mismo caso: la
    // tabla manda. El mensaje es el suyo: 04-errors.md §7 les prohíbe citar valores de `data`.
    const retryAfterMs = def.retryAfterMs ?? (typeof e?.opts?.retryAfterMs === "number" ? e.opts.retryAfterMs : undefined);
    return new ErrorHttp(def.status, code, def.clase, mensaje, { retryAfterMs, ambiguo: def.ambiguo });
  }

  // 2. Una excepción tipada de flux con un code que NO es de la tabla: viene del que responde (HTTP_503, PEDIDO_YA_CANCELADO…).
  // Conserva su code y su clase.
  if (e instanceof RetryableError) {
    return new ErrorHttp(503, code ?? "RETRYABLE", "retryable", mensaje, {
      retryAfterMs: typeof e.opts?.retryAfterMs === "number" ? e.opts.retryAfterMs : undefined,
    });
  }
  if (e instanceof PermanentError) {
    return new ErrorHttp(422, code ?? "PERMANENT", "permanent", mensaje);
  }

  // 3. Lo que lanza el SDK sin código estable.
  if (e instanceof InvalidSubjectError) {
    return errorEdge("EDGE_INVALID_REQUEST", mensaje);
  }
  if (e instanceof EnvelopeError) {
    // El SDK dice «supera el límite» cuando el EVENTO (el sobre y `data`) pasa de 1 MiB: es un cuerpo grande aunque el HTTP
    // estuviera por debajo del límite (A-152). Un test fija el texto del SDK, para que un cambio no lo convierta en un 422.
    return /supera el l[ií]mite/.test(mensaje) ? errorEdge("EDGE_BODY_TOO_LARGE", mensaje) : errorEdge("EDGE_INVALID_REQUEST", mensaje);
  }

  // 4. Lo desconocido: un mensaje genérico. No se reenvía el texto de una excepción cualquiera.
  return errorEdge("EDGE_INTERNAL", "el gateway no pudo atender la petición por un fallo interno; no es un error de la petición. Reintentarla no lo arregla: avisa a quien opera el gateway (el detalle está en su registro)");
}

/** El cuerpo JSON de un error. */
export function cuerpoDeError(err) {
  return {
    error: {
      code: err.code,
      class: err.clase,
      message: err.message + (err.ambiguo ? " La operación PUEDE haberse ejecutado: reintenta con una clave de negocio en `data` que quien responde deduplique." : ""),
      ...(err.retryAfterMs !== undefined && { retryAfterMs: err.retryAfterMs }),
    },
  };
}

/** Las cabeceras de un error: `Retry-After` en lo reintentable, `WWW-Authenticate` en un 401 de token. */
export function cabecerasDeError(err) {
  const h = { ...err.headers };
  if (err.retryAfterMs !== undefined && err.clase === "retryable") h["retry-after"] = aSegundos(err.retryAfterMs);
  if (err.code === "EDGE_UNAUTHENTICATED") h["www-authenticate"] = "Bearer";
  return h;
}
