/**
 * Webhooks de terceros: HTTP → evento. Implementación: specification/12-edge.md §6.3 y §5.
 *
 *   POST /v1/hooks/{nombre}      sin `Authorization`: lo autentica LA FIRMA del proveedor (HMAC-SHA256, configurable)
 *
 * El orden importa y es el de la spec:
 *   1. se verifica la firma sobre los BYTES en bruto, ANTES de interpretar nada;
 *   2. solo entonces se interpreta el JSON, se sacan la clave externa y el instante del hecho (del cuerpo o de una cabecera) y se
 *      construye `data` (el cuerpo entero, o los campos que el propietario elige con `pick`: un cobro trae el email del pagador,
 *      y la regla 5 de AGENTS.md es «sin PII en data»);
 *   3. se publica con `{ externalKey, time }`: el `id` es DETERMINISTA, y **el mismo webhook enviado tres veces produce el mismo
 *      evento tres veces** —y los consumidores, que deduplican por `id`, aplican UN efecto—.
 *
 * La clave externa lleva el NOMBRE DEL HOOK (`<hook>:<id del proveedor>`): dos proveedores pueden numerar sus eventos igual y, con
 * el mismo `source`, producirían el mismo `id`.
 */

import { verificar, verificarHmacSimple } from "./firma.mjs";
import { errorEdge } from "./errores.mjs";

const UTF8 = new TextDecoder("utf-8", { fatal: true });
const MAX_MS = 2 ** 48 - 1;

/** Un valor por JSON Pointer (RFC 6901): `/data/object/id`. `""` es el documento entero. */
export function porPuntero(doc, puntero) {
  if (puntero === "") return doc;
  let v = doc;
  for (const trozo of puntero.slice(1).split("/")) {
    const k = trozo.replaceAll("~1", "/").replaceAll("~0", "~");
    if (v === null || typeof v !== "object" || !Object.hasOwn(v, k)) return undefined;
    v = v[k];
  }
  return v;
}

/** Los milisegundos de un instante del proveedor, o `undefined` si no es un instante válido. */
export function aMilisegundos(valor, unidad) {
  let ms;
  if (unidad === "iso") {
    if (typeof valor !== "string") return undefined;
    ms = Date.parse(valor);
  } else {
    const n = typeof valor === "string" && /^[0-9]{1,16}(\.[0-9]+)?$/.test(valor.trim()) ? Number(valor) : valor;
    if (typeof n !== "number" || !Number.isFinite(n)) return undefined;
    ms = Math.round(unidad === "seconds" ? n * 1000 : n);
  }
  return Number.isInteger(ms) && ms >= 0 && ms <= MAX_MS ? ms : undefined;
}

function leerFuente(fuente, cuerpo, cabeceras) {
  return fuente.from === "header" ? cabeceras[fuente.name] : porPuntero(cuerpo, fuente.pointer);
}

function fuenteDe(fuente) {
  return fuente.from === "header" ? `la cabecera \`${fuente.name}\`` : `el JSON Pointer \`${fuente.pointer}\` del cuerpo`;
}

/**
 * Procesa un webhook ya leído.
 * @param {object} h  el hook normalizado (config.mjs)
 * @param {{ cabeceras: Record<string, string|string[]|undefined>, cuerpo: Buffer }} peticion
 * @param {{ bus: any, tenantId: string, reloj?: () => number }} contexto
 * @returns {Promise<{ id: string }>}
 */
export async function procesarHook(h, { cabeceras, cuerpo }, { bus, tenantId, reloj = Date.now }) {
  // 1. La firma, sobre los bytes en bruto.
  const valor = cabeceras[h.verify.header];
  if (h.verify.formato === "t-v1") {
    const r = verificar({ cabecera: Array.isArray(valor) ? valor[0] : valor, cuerpo, secretos: h.verify.secretos, ahoraMs: reloj(), toleranciaSeg: h.verify.toleranciaSeg });
    if (!r.ok && r.motivo === "tolerancia") {
      throw errorEdge("EDGE_WEBHOOK_TIMESTAMP_OUT_OF_TOLERANCE", `la firma del webhook es correcta pero su instante se aleja más de ${h.verify.toleranciaSeg} s de la hora del gateway (por delante o por detrás): sincroniza los relojes (NTP). Un reenvío con la misma firma no sirve: caduca`);
    }
    if (!r.ok) throw errorEdge("EDGE_INVALID_WEBHOOK_SIGNATURE", `la firma del webhook no verifica (${r.motivo}): revisa el secreto y que el cuerpo no se reserialice por el camino`);
  } else if (!verificarHmacSimple({ valor: Array.isArray(valor) ? valor[0] : valor, formato: h.verify.formato, cuerpo, secretos: h.verify.secretos })) {
    throw errorEdge("EDGE_INVALID_WEBHOOK_SIGNATURE", "la firma del webhook falta o no verifica: revisa el secreto y que el cuerpo no se reserialice por el camino");
  }

  // 2. Solo ahora se interpreta.
  let doc;
  try {
    doc = JSON.parse(UTF8.decode(cuerpo));
  } catch {
    throw errorEdge("EDGE_INVALID_BODY", "el cuerpo del webhook no es JSON válido en UTF-8");
  }

  // `data`: el cuerpo entero, o los campos elegidos. Siempre un OBJETO (01-envelope.md §4).
  let data;
  if (h.datos.modo === "pick") {
    data = {};
    for (const [nombre, puntero] of Object.entries(h.datos.pick)) {
      const v = porPuntero(doc, puntero);
      if (v !== undefined) data[nombre] = v; // ausente, no `null`: 01-envelope.md §3.3
    }
  } else data = doc;
  if (h.datos.wrapAs) data = { [h.datos.wrapAs]: data };
  if (data === null || typeof data !== "object" || Array.isArray(data)) {
    throw errorEdge("EDGE_WEBHOOK_PAYLOAD_INVALID", "el cuerpo del webhook no es un objeto JSON: `data` debe serlo (01-envelope.md §4). Declara `data.wrapAs` en el hook para guardarlo bajo una clave");
  }

  const opciones = { tenantId, classification: h.classification };

  if (h.aggregateId) {
    const a = leerFuente(h.aggregateId, doc, cabeceras);
    if (typeof a === "string" && a !== "") opciones.aggregateId = a;
    else if (typeof a === "number" && Number.isFinite(a)) opciones.aggregateId = String(a);
  }

  // 3. El id determinista: la clave del proveedor, con el nombre del hook, y el instante del hecho.
  if (h.idempotente) {
    const k = leerFuente(h.externalKey, doc, cabeceras);
    if (!(typeof k === "string" && k !== "") && !(typeof k === "number" && Number.isFinite(k))) {
      throw errorEdge("EDGE_WEBHOOK_PAYLOAD_INVALID", `no hay clave del evento del proveedor en ${fuenteDe(h.externalKey)}: sin ella no hay id determinista. Revisa \`externalKey\` del hook`);
    }
    const crudo = leerFuente(h.time, doc, cabeceras);
    const ms = aMilisegundos(Array.isArray(crudo) ? crudo[0] : crudo, h.time.unit);
    if (ms === undefined) {
      throw errorEdge("EDGE_WEBHOOK_PAYLOAD_INVALID", `no hay un instante válido (${h.time.unit}) en ${fuenteDe(h.time)}: es el instante del hecho EXTERNO y es lo que hace estable el id. Revisa \`time\` del hook`);
    }
    opciones.externalKey = `${h.nombre}:${k}`;
    opciones.time = new Date(ms).toISOString();
  }

  const evento = await bus.publish(h.subject, data, opciones);
  return { id: evento.id };
}
