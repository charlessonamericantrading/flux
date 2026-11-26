/**
 * Utilidades de las pruebas de petición/respuesta: un RESPONDER escrito a mano sobre NATS crudo y el
 * constructor de respuestas y errores de 11-request-reply.md §3.2, §3.3.
 *
 * Como el escritor de estado de `helpers.mjs`, **no usa ningún SDK de flux**: la CLI no depende del
 * SDK, y para comprobar lo que ve hay que contestarle por otro camino. El responder cumple §5 en lo
 * que importa a quien llama: suscripción EXACTA a `rpc.<operación>`, grupo de cola = servicio, la
 * respuesta HEREDA `correlationid` y `tenantid` y su `causationid` es el `id` de la petición, y un
 * fallo del handler se contesta con un error `com.flux.rpc.error.v1`, nunca en silencio.
 *
 * Los constructores se comprueban contra los bytes de `conformance/cases/cross-sdk-rpc-envelope.json`
 * (rpc-modelo.test.mjs): un responder que solo coincidiera con la CLI no demostraría nada.
 */

import { uuidv7 } from "../src/rpc-model.mjs";

const enc = new TextEncoder();
const dec = new TextDecoder();

const ERROR_TYPE = "com.flux.rpc.error.v1";
const ERROR_SCHEMA = "https://schemas.internal/system/rpc/error/1.0.0.json";

/**
 * La respuesta de una operación (11-request-reply.md §3.2). Las claves van en el orden de
 * declaración y `data` la última. `peticion` es el sobre recibido; `request` del arnés cruzado
 * (`{ operation, id, correlationid, tenantid, subject? }`) también vale.
 */
export function construirRespuesta({
  operation, request, id, source, time, producerversion, classification, traceparent, dataschema, data, type,
}) {
  const [dom, agr, major, op] = operation.split(".");
  const ev = {
    specversion: "1.0",
    id,
    source,
    type: type ?? `com.flux.rpc.${dom}.${agr}.${op}.reply.${major}`,
    time,
    datacontenttype: "application/json",
    dataschema: dataschema ?? `https://schemas.internal/rpc/${dom}/${agr}/${op}/reply/${major.slice(1)}.0.0.json`,
    ...(request.subject !== undefined && { subject: request.subject }),
    correlationid: request.correlationid,
    tenantid: request.tenantid,
    producerversion,
    dataclassification: classification,
    causationid: request.id,
    ...(request.subject !== undefined && { partitionkey: request.subject }),
    ...(traceparent !== undefined && { traceparent }),
    data,
  };
  return ev;
}

/** El error de una operación (§3.3): `type` y `dataschema` fijos, clasificación `internal`. */
export function construirError({ request, id, source, time, producerversion, traceparent, error }) {
  const ev = {
    specversion: "1.0",
    id,
    source,
    type: ERROR_TYPE,
    time,
    datacontenttype: "application/json",
    dataschema: ERROR_SCHEMA,
    ...(request.subject !== undefined && { subject: request.subject }),
    correlationid: request.correlationid,
    tenantid: request.tenantid,
    producerversion,
    dataclassification: "internal",
    causationid: request.id,
    ...(request.subject !== undefined && { partitionkey: request.subject }),
    ...(traceparent !== undefined && { traceparent }),
    data: { class: error.class, code: error.code, message: error.message, ...(error.retryAfterMs !== undefined && { retryAfterMs: error.retryAfterMs }) },
  };
  return ev;
}

export const bytes = (evento) => enc.encode(JSON.stringify(evento));

/**
 * Atiende `rpc.<operacion>` por core NATS, como lo haría `respond()` de un SDK (§5.1). El `handler`
 * recibe `(sobre, mensaje)` y devuelve:
 *   · `{ data, classification? }`           → la respuesta
 *   · `{ error: { class, code, message, retryAfterMs? } }`  → un error del servicio
 *   · `{ crudo: Uint8Array }`                → esos bytes tal cual (para fabricar una respuesta rota)
 *   · `{ data, cambios: (ev) => ev }`        → la respuesta, con el sobre retocado (para estropearlo)
 *   · `{ callar: true }`                     → no contesta (un responder colgado)
 * `demoraMs` retrasa la respuesta (un responder lento: A-61). `visto` guarda TODO lo recibido: los
 * bytes, las cabeceras y el subject de respuesta, para comprobar qué mandó la CLI.
 */
export function sirve(nc, { operacion, servicio = "crm-api", entorno = "produccion", version = "2.1.0", handler, demoraMs = 0 }) {
  const visto = [];
  const sub = nc.subscribe(`rpc.${operacion}`, { queue: servicio });
  const tarea = (async () => {
    for await (const m of sub) {
      const entrada = { bytes: m.data, headers: m.headers, reply: m.reply, subject: m.subject };
      let evento;
      try {
        evento = JSON.parse(dec.decode(m.data));
      } catch { /* una petición ilegible: se anota y se contesta con un error en un responder real; aquí no la generamos */ }
      entrada.evento = evento;
      visto.push(entrada);
      if (!m.reply) continue; // §5.1 paso 2: sin subject de respuesta no se atiende
      if (demoraMs) await new Promise((r) => setTimeout(r, demoraMs));
      const salida = await handler(evento, m);
      if (salida.callar) continue;
      if (salida.crudo) {
        m.respond(salida.crudo);
        continue;
      }
      const comunes = { request: evento, id: uuidv7(), source: `/${entorno}/${servicio}`, time: new Date().toISOString(), producerversion: version };
      let ev = salida.error
        ? construirError({ ...comunes, error: salida.error })
        : construirRespuesta({ ...comunes, operation: operacion, classification: salida.classification ?? "internal", data: salida.data });
      if (salida.cambios) ev = salida.cambios(ev);
      m.respond(bytes(ev));
    }
  })();
  return {
    visto,
    async parar() {
      await sub.drain();
      await tarea;
    },
  };
}
