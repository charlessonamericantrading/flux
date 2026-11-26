/**
 * El servidor HTTP del gateway: `node:http`, sin framework. Implementación: specification/12-edge.md §6 y §7.
 *
 * Rutas (todas con `Authorization: Bearer <token>` salvo los hooks y /healthz):
 *
 *   POST   /v1/events/{subject}           publica; el cuerpo es `data`                 → 202 { id }
 *   PUT    /v1/state/{contrato}/{clave}   state.put; el cuerpo es `data`               → 200 { applied, outcome, revision }
 *   DELETE /v1/state/{contrato}/{clave}   state.delete (?aggregateVersion=n)           → 200 { applied, outcome, revision }
 *   GET    /v1/state/{contrato}/{clave}   state.get                                    → 200 { key, … } | 404
 *   POST   /v1/rpc/{operacion}            request; el cuerpo es `data`                 → 200 <data de la respuesta>
 *   POST   /v1/hooks/{nombre}             webhook de un tercero (firma, no token)      → 202 { id }
 *   GET    /healthz                       ¿están conectados los clientes?
 *
 * El gateway NO escribe el sobre: `id`, `source`, `time`, `type`, `correlationid` y `traceparent` los pone el SDK. Cada cliente es
 * una identidad NATS propia: lo que el broker no le concede, el broker se lo deniega, aunque este código se equivoque (A-150).
 */

import http from "node:http";
import https from "node:https";
import { readFileSync } from "node:fs";

import { ClienteAbandono, CuerpoDemasiadoGrande, leerCuerpo } from "./cuerpo.mjs";
import { ErrorHttp, cabecerasDeError, cuerpoDeError, errorEdge, traducir } from "./errores.mjs";
import { procesarHook } from "./hooks.mjs";
import { LimitePorClave } from "./limite.mjs";

const JSON_CT = /^application\/json(?:\s*;.*)?$/i;
const INSTANTE = /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$/;
const CLASIFICACIONES = ["public", "internal", "confidential", "restricted"];

/**
 * @param {object} o
 * @param {object} o.config                 la configuración normalizada (config.mjs)
 * @param {Map<string, { cliente: object, bus: any }>} o.clientes  por nombre de servicio
 * @param {{ autenticar(authorization?: string): { cliente: string, tokenId: string } | null }} o.autenticador
 * @param {Map<string, { hook: object, cliente: string }>} o.hooks  por nombre de hook
 * @param {{ info: Function, warn: Function, error: Function }} o.registro
 * @param {() => number} [o.reloj]
 */
export function crearServidor({ config, clientes, autenticador, hooks, registro, reloj = Date.now }) {
  const limites = config.limits;
  const limitePorToken = new LimitePorClave(limites.rateLimit);
  const limitesPropios = new Map();
  for (const { cliente } of clientes.values()) if (cliente.rateLimit) limitesPropios.set(cliente.service, new LimitePorClave(cliente.rateLimit));
  const limiteDeHooks = new LimitePorClave(limites.rateLimit);
  const limitesDeHook = new Map();
  for (const { hook } of hooks.values()) if (hook.rateLimit) limitesDeHook.set(hook.nombre, new LimitePorClave({ ...limites.rateLimit, ...hook.rateLimit }));

  const estado = { parando: false, enVuelo: 0 };

  // ── Respuestas ───────────────────────────────────────────────────────────────────────────────────────────────
  function responder(res, status, cuerpo, cabeceras = {}) {
    if (res.headersSent || res.destroyed) return; // el cliente se fue (A-151): no hay a quién escribir, y escribir no lanza
    const texto = cuerpo === undefined ? "" : JSON.stringify(cuerpo);
    res.writeHead(status, {
      ...(texto && { "content-type": "application/json; charset=utf-8" }),
      "content-length": Buffer.byteLength(texto),
      "cache-control": "no-store",
      ...(estado.parando && { connection: "close" }), // A-162: sin esto la conexión de la última respuesta sigue `keep-alive`
      ...cabeceras,
    });
    res.end(texto);
  }

  function responderError(res, err, ctx) {
    ctx.code = err.code;
    ctx.clase = err.clase;
    responder(res, err.status, cuerpoDeError(err), cabecerasDeError(err));
  }

  // ── Piezas de una petición ───────────────────────────────────────────────────────────────────────────────────
  const cabecera = (req, nombre) => {
    const v = req.headers[nombre];
    return Array.isArray(v) ? v[0] : v;
  };

  function decodificar(segmento, que) {
    try {
      return decodeURIComponent(segmento);
    } catch {
      throw errorEdge("EDGE_INVALID_REQUEST", `${que} no es una URL válida (porcentaje mal formado)`);
    }
  }

  /** El tenant de la petición: el de `Flux-Tenant` si el cliente lo tiene declarado; si no, el suyo por defecto. */
  function tenantDe(req, cliente) {
    const t = cabecera(req, "flux-tenant");
    if (t === undefined) return cliente.tenantId;
    if (!cliente.tenants.includes(t)) {
      throw errorEdge("EDGE_TENANT_NOT_ALLOWED", `el cliente «${cliente.service}» no tiene declarado el tenant de \`Flux-Tenant\`. Los suyos son: ${cliente.tenants.join(", ")}. Añádelo a \`tenants\` del cliente en gateway.json (y al manifiesto, si el broker lo confina)`);
    }
    return t;
  }

  function clasificacionDe(req) {
    const c = cabecera(req, "flux-classification");
    if (c === undefined) return undefined;
    if (!CLASIFICACIONES.includes(c)) throw errorEdge("EDGE_INVALID_REQUEST", `\`Flux-Classification\` debe ser ${CLASIFICACIONES.join(", ")}`);
    return c;
  }

  function instanteDe(req) {
    const t = cabecera(req, "flux-event-time");
    if (t === undefined) return undefined;
    if (!INSTANTE.test(t) || Number.isNaN(Date.parse(t))) throw errorEdge("EDGE_INVALID_REQUEST", "`Flux-Event-Time` debe ser RFC 3339 en UTC con exactamente 3 decimales y sufijo Z (`2026-08-20T10:25:39.412Z`): el instante del hecho (01-envelope.md §2.2)");
    return t;
  }

  /** El cuerpo como objeto JSON: es `data` (01-envelope.md §4). */
  async function cuerpoJson(req) {
    const ct = cabecera(req, "content-type");
    if (ct === undefined || !JSON_CT.test(ct)) throw errorEdge("EDGE_UNSUPPORTED_MEDIA_TYPE", "el `Content-Type` debe ser `application/json`");
    const bytes = await leerCuerpo(req, limites.maxBodyBytes);
    let v;
    try {
      v = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
    } catch {
      throw errorEdge("EDGE_INVALID_BODY", "el cuerpo no es JSON válido en UTF-8");
    }
    if (v === null || typeof v !== "object" || Array.isArray(v)) throw errorEdge("EDGE_INVALID_BODY", "el cuerpo debe ser un objeto JSON (es `data`: ni un array ni un escalar en la raíz, para poder añadir campos sin romper el contrato: 01-envelope.md §4)");
    return v;
  }

  // ── Rutas autenticadas ───────────────────────────────────────────────────────────────────────────────────────
  async function eventos(req, res, partes, c, ctx) {
    if (req.method !== "POST") throw errorEdge("EDGE_METHOD_NOT_ALLOWED", "esta ruta solo admite POST", { headers: { allow: "POST" } });
    if (partes.length !== 3) throw errorEdge("EDGE_NOT_FOUND", "la ruta es `/v1/events/{subject}`");
    const subject = decodificar(partes[2], "el subject");
    ctx.subject = subject;
    const data = await cuerpoJson(req);

    const opciones = { tenantId: tenantDe(req, c.cliente) };
    const agregado = cabecera(req, "flux-aggregate-id");
    if (agregado !== undefined) opciones.aggregateId = agregado;
    const clasificacion = clasificacionDe(req);
    if (clasificacion) opciones.classification = clasificacion;
    const instante = instanteDe(req);
    const clave = cabecera(req, "idempotency-key");
    if (clave !== undefined) {
      // El id DETERMINISTA para quien reintenta (01-envelope.md §2.6.1): exige el instante del hecho, que es lo que lo hace estable.
      if (instante === undefined) throw errorEdge("EDGE_INVALID_REQUEST", "`Idempotency-Key` exige `Flux-Event-Time`: el instante del hecho es lo que hace estable el id (con «ahora», cada reintento tendría el suyo)");
      opciones.externalKey = `${subject}:${clave}`;
    }
    if (instante !== undefined) opciones.time = instante;

    const evento = await c.bus.publish(subject, data, opciones);
    ctx.id = evento.id;
    responder(res, 202, { id: evento.id });
  }

  async function estadoRuta(req, res, partes, c, ctx) {
    if (partes.length !== 4) throw errorEdge("EDGE_NOT_FOUND", "la ruta es `/v1/state/{contrato}/{clave}`");
    const contrato = decodificar(partes[2], "el contrato");
    const clave = decodificar(partes[3], "la clave");
    ctx.contrato = contrato;
    const tenantId = tenantDe(req, c.cliente);
    ctx.tenant = tenantId;

    if (req.method === "GET") {
      const entrada = await c.bus.state.get(contrato, clave, { tenantId });
      if (entrada === null) throw errorEdge("EDGE_NOT_FOUND", "no hay valor para esa clave (nunca se escribió, o está borrada: una lápida lee como ausente)");
      return responder(res, 200, { key: entrada.key, tenantId: entrada.tenantId, deleted: entrada.deleted, data: entrada.data, aggregateVersion: entrada.aggregateVersion, revision: entrada.revision });
    }
    if (req.method === "PUT") {
      const data = await cuerpoJson(req);
      const opciones = { tenantId };
      const clasificacion = clasificacionDe(req);
      if (clasificacion) opciones.classification = clasificacion;
      const instante = instanteDe(req);
      if (instante) opciones.time = instante;
      const r = await c.bus.state.put(contrato, clave, data, opciones);
      ctx.outcome = r.outcome;
      return responder(res, 200, { applied: r.applied, outcome: r.outcome, revision: r.revision });
    }
    if (req.method === "DELETE") {
      const url = new URL(req.url, "http://gateway.local");
      const v = url.searchParams.get("aggregateVersion");
      if (v === null || !/^[0-9]{1,16}$/.test(v)) throw errorEdge("EDGE_INVALID_REQUEST", "`DELETE` necesita `?aggregateVersion=n`: la versión de la LÁPIDA, que debe superar la del valor que borra (10-state.md §4)");
      const opciones = { aggregateVersion: Number(v), tenantId };
      const instante = instanteDe(req);
      if (instante) opciones.time = instante;
      const r = await c.bus.state.delete(contrato, clave, opciones);
      ctx.outcome = r.outcome;
      return responder(res, 200, { applied: r.applied, outcome: r.outcome, revision: r.revision });
    }
    throw errorEdge("EDGE_METHOD_NOT_ALLOWED", "esta ruta admite GET, PUT y DELETE", { headers: { allow: "GET, PUT, DELETE" } });
  }

  async function rpc(req, res, partes, c, ctx) {
    if (req.method !== "POST") throw errorEdge("EDGE_METHOD_NOT_ALLOWED", "esta ruta solo admite POST", { headers: { allow: "POST" } });
    if (partes.length !== 3) throw errorEdge("EDGE_NOT_FOUND", "la ruta es `/v1/rpc/{operacion}`");
    const operacion = decodificar(partes[2], "la operación");
    ctx.operacion = operacion;
    const data = await cuerpoJson(req);

    const opciones = { tenantId: tenantDe(req, c.cliente) };
    const timeout = cabecera(req, "flux-timeout-ms");
    if (timeout !== undefined) {
      if (!/^[0-9]{1,6}$/.test(timeout)) throw errorEdge("EDGE_INVALID_REQUEST", "`Flux-Timeout-Ms` debe ser un entero entre 1 y 25000");
      opciones.timeoutMs = Number(timeout); // el SDK rechaza el rango con INVALID_RPC_CALL: no se recorta en silencio
    }
    const agregado = cabecera(req, "flux-aggregate-id");
    if (agregado !== undefined) opciones.aggregateId = agregado;
    const clasificacion = clasificacionDe(req);
    if (clasificacion) opciones.classification = clasificacion;

    const respuesta = await c.bus.request(operacion, data, opciones);
    responder(res, 200, respuesta);
  }

  async function hook(req, res, partes, ctx) {
    if (req.method !== "POST") throw errorEdge("EDGE_METHOD_NOT_ALLOWED", "los hooks solo admiten POST", { headers: { allow: "POST" } });
    if (partes.length !== 3) throw errorEdge("EDGE_NOT_FOUND", "la ruta es `/v1/hooks/{nombre}`");
    const nombre = decodificar(partes[2], "el nombre del hook");
    const def = hooks.get(nombre);
    if (!def) throw errorEdge("EDGE_NOT_FOUND", "no hay ningún hook con ese nombre");
    ctx.hook = nombre;
    ctx.cliente = def.cliente;

    // Los hooks no llevan token: su cubo es por NOMBRE (un tercero no es un cliente). Antes de leer el cuerpo.
    const lim = (limitesDeHook.get(nombre) ?? limiteDeHooks).tomar(nombre);
    if (!lim.ok) throw errorEdge("EDGE_RATE_LIMITED", "demasiadas peticiones a este hook", { retryAfterMs: lim.retryAfterMs });

    const cuerpo = await leerCuerpo(req, limites.maxBodyBytes);
    const c = clientes.get(def.cliente);
    const r = await procesarHook(def.hook, { cabeceras: req.headers, cuerpo }, { bus: c.bus, tenantId: c.cliente.tenantId, reloj });
    ctx.id = r.id;
    responder(res, 202, { id: r.id });
  }

  function salud(res) {
    const lista = [...clientes.values()];
    const conectados = lista.filter((c) => c.bus.connected).length;
    const ok = conectados === lista.length && !estado.parando;
    responder(res, ok ? 200 : 503, { status: ok ? "ok" : "degraded", clientes: lista.length, conectados });
  }

  // ── El manejador ─────────────────────────────────────────────────────────────────────────────────────────────
  async function manejar(req, res) {
    const t0 = performance.now();
    const ctx = { metodo: req.method, ruta: "?" };
    estado.enVuelo++;
    res.once("close", () => {
      estado.enVuelo--;
      registro.info("petición", { ...ctx, estado: res.statusCode, ms: Math.round(performance.now() - t0) });
    });

    try {
      if (estado.parando) throw errorEdge("EDGE_SHUTTING_DOWN", "el gateway se está parando: reintenta en unos segundos", { retryAfterMs: 1000 });

      const url = new URL(req.url ?? "/", "http://gateway.local");
      const partes = url.pathname.split("/").filter((p) => p !== "");
      ctx.ruta = partes.length >= 2 && partes[0] === "v1" ? `/v1/${partes[1]}` : url.pathname === "/healthz" ? "/healthz" : "?";

      if (url.pathname === "/healthz") return salud(res);

      // Un cuerpo declarado mayor que el límite se rechaza ANTES de leerlo, en cualquier ruta (A-152).
      if (Number(req.headers["content-length"]) > limites.maxBodyBytes) throw new CuerpoDemasiadoGrande(limites.maxBodyBytes, Number(req.headers["content-length"]));

      if (partes[0] !== "v1" || partes.length < 2) throw errorEdge("EDGE_NOT_FOUND", "las rutas son `/v1/events`, `/v1/state`, `/v1/rpc`, `/v1/hooks` y `/healthz`");
      if (partes[1] === "hooks") return await hook(req, res, partes, ctx);

      // Autenticación: el mismo 401 para un token ausente y uno desconocido.
      const quien = autenticador.autenticar(cabecera(req, "authorization"));
      if (!quien) throw errorEdge("EDGE_UNAUTHENTICATED", "falta un token válido: `Authorization: Bearer <token>`");
      ctx.cliente = quien.cliente;
      ctx.tokenId = quien.tokenId;
      const c = clientes.get(quien.cliente);

      const lim = (limitesPropios.get(quien.cliente) ?? limitePorToken).tomar(quien.tokenId);
      if (!lim.ok) throw errorEdge("EDGE_RATE_LIMITED", `se supera el límite de peticiones de este token; reintenta en ${Math.ceil(lim.retryAfterMs / 1000)} s`, { retryAfterMs: lim.retryAfterMs });

      switch (partes[1]) {
        case "events": return await eventos(req, res, partes, c, ctx);
        case "state": return await estadoRuta(req, res, partes, c, ctx);
        case "rpc": return await rpc(req, res, partes, c, ctx);
        default: throw errorEdge("EDGE_NOT_FOUND", "las rutas son `/v1/events`, `/v1/state`, `/v1/rpc` y `/v1/hooks`");
      }
    } catch (e) {
      if (e instanceof CuerpoDemasiadoGrande) {
        // La conexión se CIERRA: sin ello, Node lee y descarta todo lo declarado antes de cerrar (A-152).
        const err = errorEdge("EDGE_BODY_TOO_LARGE", `el cuerpo supera el límite de ${limites.maxBodyBytes} bytes (1 MiB: el del evento, 01-envelope.md §7). Aplica claim-check: sube el contenido a un almacén de objetos y envía \`{ uri, sha256, bytes }\``);
        ctx.code = err.code;
        responder(res, err.status, cuerpoDeError(err), { connection: "close" });
        res.once("finish", () => req.destroy());
        return;
      }
      if (e instanceof ClienteAbandono) {
        ctx.code = "CLIENT_ABORTED";
        return; // no hay a quién contestar
      }
      const err = traducir(e);
      if (err.code === "EDGE_INTERNAL") registro.error("fallo interno", { ...ctx, code: err.code, motivo: e instanceof Error ? `${e.name}: ${e.message}` : String(e) });
      responderError(res, err, ctx);
    }
  }

  // ── El servidor ──────────────────────────────────────────────────────────────────────────────────────────────
  const opciones = {
    // headersTimeout y requestTimeout solo se comprueban cada `connectionsCheckingInterval` (30 s por defecto): A-153.
    connectionsCheckingInterval: limites.connectionsCheckingIntervalMs,
    headersTimeout: limites.headersTimeoutMs,
    requestTimeout: limites.requestTimeoutMs,
    keepAliveTimeout: 5000,
  };
  const tls = config.listen.tls;
  const servidor = tls
    ? https.createServer({ ...opciones, cert: readFileSync(tls.certFile), key: readFileSync(tls.keyFile) }, manejar)
    : http.createServer(opciones, manejar);
  // Mide inactividad del socket MIENTRAS el gateway espera al broker: debe superar el timeout máximo de un rpc (A-153).
  servidor.timeout = limites.serverTimeoutMs;
  servidor.requestTimeout = limites.requestTimeoutMs;
  servidor.headersTimeout = limites.headersTimeoutMs;

  /**
   * La parada ordenada (A-162): dejar de aceptar, contestar lo que vuela con `Connection: close`, cerrar las ociosas y, tras el
   * plazo, lo que quede.
   */
  async function parar(plazoMs) {
    estado.parando = true;
    const cerrado = new Promise((r) => servidor.close(() => r()));
    servidor.closeIdleConnections();
    const limite = Date.now() + plazoMs;
    while (estado.enVuelo > 0 && Date.now() < limite) {
      servidor.closeIdleConnections();
      await new Promise((r) => setTimeout(r, 25));
    }
    servidor.closeIdleConnections();
    if (estado.enVuelo > 0) registro.warn("parada con peticiones en vuelo", { motivo: `${estado.enVuelo} petición(es) sin terminar tras ${plazoMs} ms` });
    servidor.closeAllConnections();
    await cerrado;
  }

  return { servidor, parar, estado };
}
