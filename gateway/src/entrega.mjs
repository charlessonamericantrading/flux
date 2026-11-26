/**
 * La entrega saliente: flux → HTTP. Contrato NORMATIVO: specification/12-edge.md §4.
 *
 * Por cada entrega, el gateway es un CONSUMIDOR más (su durable, su `ack_wait`, su backoff y su DLQ, los de 03-delivery.md §2) cuyo
 * handler hace un `POST` al receptor:
 *
 *   Content-Type: application/cloudevents+json; charset=utf-8     el CloudEvent tal cual, los mismos bytes en cada intento
 *   Idempotency-Key: <id del evento>                              el mismo en cada intento
 *   Flux-Signature: t=<unix>,v1=<hmac>                            `t` del momento de ESTE intento (firma.mjs)
 *
 * y clasifica la respuesta con la taxonomía de siempre (04-errors.md): 2xx → ack; 429/502/503/504 → RETRYABLE con el presupuesto
 * completo; 408, 425 y el resto de 5xx → RETRYABLE ACOTADO a 2 entregas (un 500 no es un 503); 3xx (no se sigue) y el resto de 4xx →
 * PERMANENT, a la DLQ sin reintentos. Se lo dice al SDK con un error propio (`FalloDeEntrega`) que una regla de `classifier`
 * convierte en `Classification` — incluido el presupuesto acotado (A-160), sin tocar el SDK ni `max_deliver`.
 *
 * Se usa `node:http`/`node:https` y no `fetch`: no sigue redirecciones, el plazo es TOTAL y las cabeceras llegan sin esperar al
 * cuerpo (A-161). El cuerpo de la respuesta se descarta y NUNCA se registra.
 */

import http from "node:http";
import https from "node:https";

import { ErrorClass, serialize } from "@flux/sdk";

import { firmar } from "./firma.mjs";

/** Los códigos de red que `04-errors.md §1.1` da como ejemplos de transitorios: se reintentan con el presupuesto completo. */
const TRANSITORIOS = new Set(["ECONNRESET", "ECONNREFUSED", "ETIMEDOUT", "EPIPE", "EHOSTUNREACH", "ENETUNREACH", "EAI_AGAIN"]);
/** Un certificado del receptor que no verifica: PERMANENT (no se arregla solo en 30 s; el evento espera en la DLQ). */
const TLS_NO_VERIFICA = new Set([
  "DEPTH_ZERO_SELF_SIGNED_CERT", "SELF_SIGNED_CERT_IN_CHAIN", "UNABLE_TO_VERIFY_LEAF_SIGNATURE", "UNABLE_TO_GET_ISSUER_CERT_LOCALLY",
  "CERT_HAS_EXPIRED", "CERT_NOT_YET_VALID", "ERR_TLS_CERT_ALTNAME_INVALID", "HOSTNAME_MISMATCH",
]);
const REINTENTABLES = new Set([429, 502, 503, 504]); // errors.classes.retryable.httpStatus
const RETRY_AFTER_MAX_MS = 300_000;
const PRESUPUESTO_ACOTADO = 2;

/** Lo que el SDK necesita para clasificar: ver `reglaDeClasificacion`. */
export class FalloDeEntrega extends Error {
  /** @param {string} message  @param {{ class: string, code: string, retryAfterMs?: number, maxAttempts?: number }} classification */
  constructor(message, classification, opts = {}) {
    super(message, opts);
    this.name = "FalloDeEntrega";
    this.classification = classification;
  }
}

/** La regla de `ConnectOptions.classifier.rules`: lo que el handler declara, el SDK lo respeta. */
export const reglaDeClasificacion = (e) => (e instanceof FalloDeEntrega ? e.classification : undefined);

/** `Retry-After`: segundos o fecha HTTP. Recortado a 300 s; `undefined` si no dice nada útil. */
export function parseRetryAfter(valor, ahoraMs = Date.now()) {
  if (typeof valor !== "string") return undefined;
  const v = valor.trim();
  let ms;
  if (/^[0-9]{1,9}$/.test(v)) ms = Number(v) * 1000;
  else {
    const t = Date.parse(v);
    if (Number.isNaN(t)) return undefined;
    ms = t - ahoraMs;
  }
  return ms > 0 ? Math.min(ms, RETRY_AFTER_MAX_MS) : undefined;
}

/**
 * La clasificación de un estado HTTP del receptor (12-edge.md §4.4). `null` = ack.
 * @returns {null | { class: string, code: string, retryAfterMs?: number, maxAttempts?: number }}
 */
export function clasificarRespuesta(status, retryAfterMs) {
  if (status >= 200 && status <= 299) return null;
  const code = `HTTP_${status}`;
  if (REINTENTABLES.has(status)) return { class: ErrorClass.RETRYABLE, code, ...(retryAfterMs !== undefined && { retryAfterMs }) };
  // 408, 425 y cualquier otro 5xx: lo desconocido tiene presupuesto acotado (04-errors.md §2.1). Un 500 no es un 503.
  if (status === 408 || status === 425 || (status >= 500 && status <= 599)) {
    return { class: ErrorClass.RETRYABLE, code, maxAttempts: PRESUPUESTO_ACOTADO, ...(retryAfterMs !== undefined && { retryAfterMs }) };
  }
  // 3xx (no se sigue la redirección) y el resto de 4xx: PERMANENT. Una firma rechazada (401/403) también.
  return { class: ErrorClass.PERMANENT, code };
}

/** La clasificación de un fallo de red o de TLS (12-edge.md §4.4). */
export function clasificarFalloDeRed(e) {
  const code = typeof e?.code === "string" ? e.code : undefined;
  if (code && TLS_NO_VERIFICA.has(code)) return { class: ErrorClass.PERMANENT, code: "TLS_VERIFICATION_FAILED" };
  if (code === "PLAZO") return { class: ErrorClass.RETRYABLE, code: "TIMEOUT" };
  if (code && TRANSITORIOS.has(code)) return { class: ErrorClass.RETRYABLE, code };
  // Lo desconocido (ENOTFOUND, EPROTO…): reintentable acotado.
  return { class: ErrorClass.RETRYABLE, code: code ?? "UNKNOWN", maxAttempts: PRESUPUESTO_ACOTADO };
}

/**
 * Un POST con plazo TOTAL. Resuelve en cuanto llegan las cabeceras; el cuerpo de la respuesta se descarta.
 * @returns {Promise<{ status: number, retryAfter?: string }>}
 */
export function enviar({ url, cuerpo, cabeceras, timeoutMs, ca }) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const modulo = u.protocol === "https:" ? https : http;
    let resuelto = false;
    const req = modulo.request(u, { method: "POST", headers: cabeceras, ...(ca && { ca }) }, (res) => {
      resuelto = true;
      resolve({ status: res.statusCode ?? 0, retryAfter: res.headers["retry-after"] });
      res.resume(); // se descarta: nunca se lee ni se registra
      res.once("close", () => clearTimeout(temporizador));
    });
    // El plazo es de TODO el intento, no de inactividad: un receptor que gotea el cuerpo no ocupa la cola eternamente.
    const temporizador = setTimeout(() => req.destroy(Object.assign(new Error("plazo agotado"), { code: "PLAZO" })), timeoutMs);
    req.once("error", (e) => {
      clearTimeout(temporizador);
      if (!resuelto) reject(e);
    });
    req.end(cuerpo);
  });
}

const host = (url) => {
  try { return new URL(url).host; } catch { return "?"; }
};

/**
 * El handler de una entrega de EVENTO.
 * @param {object} o
 * @param {{ url: string, secretos: string[], timeoutMs: number, allowClassifications: string[], caFile?: string }} o.entrega
 * @param {{ info: Function, warn: Function }} o.registro
 * @param {string} o.cliente  el nombre del servicio (para el registro)
 * @param {() => number} [o.reloj]  milisegundos desde la época (inyectable)
 * @param {(o: object) => Promise<{status: number, retryAfter?: string}>} [o.enviarHttp]  (inyectable)
 * @param {Buffer} [o.ca]
 */
export function crearManejadorDeEntrega({ entrega, registro, cliente, reloj = Date.now, enviarHttp = enviar, ca }) {
  return async function entregarEvento(evento, ctx) {
    // `restricted` NUNCA sale por HTTP, y `confidential` solo si la entrega lo permite de forma EXPLÍCITA (12-edge.md §4.7).
    // El mensaje no cita `data`.
    if (evento.dataclassification === "restricted" || !entrega.allowClassifications.includes(evento.dataclassification)) {
      throw new FalloDeEntrega(
        `el evento es \`${evento.dataclassification}\` y esta entrega solo admite ${entrega.allowClassifications.join(", ")}` +
          (evento.dataclassification === "restricted" ? " (`restricted` no sale nunca por HTTP)" : ": añade la clasificación a `allowClassifications` de la entrega si quieres que salga del ecosistema"),
        { class: ErrorClass.PERMANENT, code: "DELIVERY_CLASSIFICATION_DENIED" },
      );
    }
    // El CloudEvent tal cual: la serialización de 01-envelope.md §6. Los MISMOS bytes en cada intento.
    const cuerpo = Buffer.from(serialize(evento));
    return entregar({ entrega, cuerpo, idempotencyKey: evento.id, contentType: "application/cloudevents+json; charset=utf-8", registro, cliente, reloj, enviarHttp, ca, campos: { subject: ctx.subject, id: evento.id, intento: ctx.attempt } });
  };
}

/**
 * El handler de una entrega de ESTADO (implementación, no normativo: 12-edge.md §4.8). `watch` entrega la ENTRADA y no el sobre.
 * Su clasificación es `confidential`: la entrega tiene que permitirlo de forma explícita.
 */
export function crearManejadorDeEntregaDeEstado({ entrega, registro, cliente, reloj = Date.now, enviarHttp = enviar, ca }) {
  return async function entregarEstado(entrada, ctx) {
    if (!entrega.allowClassifications.includes("confidential")) {
      throw new FalloDeEntrega("el estado es `confidential` y esta entrega no lo autoriza (`allowClassifications`)", { class: ErrorClass.PERMANENT, code: "DELIVERY_CLASSIFICATION_DENIED" });
    }
    const cuerpo = Buffer.from(JSON.stringify({
      contract: ctx.contract, tenantId: entrada.tenantId, key: entrada.key, deleted: entrada.deleted,
      aggregateVersion: entrada.aggregateVersion, revision: entrada.revision, data: entrada.data,
    }));
    const clave = `${ctx.contract}/${entrada.tenantId}/${entrada.key}@${entrada.revision}`;
    return entregar({ entrega, cuerpo, idempotencyKey: clave, contentType: "application/json; charset=utf-8", registro, cliente, reloj, enviarHttp, ca, campos: { contrato: ctx.contract, tenant: entrada.tenantId, intento: ctx.attempt } });
  };
}

async function entregar({ entrega, cuerpo, idempotencyKey, contentType, registro, cliente, reloj, enviarHttp, ca, campos }) {
  // `t` es el de ESTE intento: con un `t` fijo, la tolerancia del receptor rechazaría justo los reintentos de 51 minutos.
  const t = Math.floor(reloj() / 1000);
  const cabeceras = {
    "content-type": contentType,
    "content-length": String(cuerpo.length),
    "idempotency-key": idempotencyKey,
    "flux-signature": firmar({ secretos: entrega.secretos, t, cuerpo }),
    "user-agent": "flux-gateway",
  };

  let r;
  try {
    r = await enviarHttp({ url: entrega.url, cuerpo, cabeceras, timeoutMs: entrega.timeoutMs, ...(ca && { ca }) });
  } catch (e) {
    const c = clasificarFalloDeRed(e);
    registro.warn("entrega fallida", { cliente, host: host(entrega.url), code: c.code, ...campos });
    // El mensaje nombra el receptor (su host, no la URL: puede llevar una clave en la ruta) y lo que pasó; nunca `data`.
    throw new FalloDeEntrega(`no se pudo entregar a ${host(entrega.url)}: ${c.code === "TIMEOUT" ? `sin respuesta en ${entrega.timeoutMs} ms` : c.code === "TLS_VERIFICATION_FAILED" ? `el certificado del receptor no verifica (${e.code})` : (e.code ?? "error de red")}`, c, { cause: e });
  }

  const c = clasificarRespuesta(r.status, parseRetryAfter(r.retryAfter, reloj()));
  if (c) {
    registro.warn("entrega rechazada", { cliente, host: host(entrega.url), estado: r.status, code: c.code, clase: c.class, ...campos });
    throw new FalloDeEntrega(`el receptor ${host(entrega.url)} respondió ${r.status}`, c);
  }
  registro.info("entrega", { cliente, host: host(entrega.url), estado: r.status, ...campos });
}
