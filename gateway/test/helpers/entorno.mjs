/**
 * El entorno de las pruebas de extremo a extremo: un nats-server REAL con los permisos GENERADOS (ecosistema.mjs), el gateway
 * arrancado de verdad (`iniciarGateway`, con su servidor HTTP en un puerto libre y una conexión NATS por cliente), servicios
 * nativos que atienden y consumen, y un receptor HTTP de prueba que VERIFICA la firma como lo haría un tercero.
 *
 * Nada de esto es un mock: el gateway habla con el broker y con HTTP reales, y los clientes son los del manifiesto del ejemplo
 * (`examples/services.json`), con `"via": "gateway"`. La CONFIGURACIÓN parte de `examples/gateway.json`: lo que se documenta es lo que
 * se ejecuta, con solo cuatro cosas sustituidas (el broker, el puerto, los tokens y la URL del receptor).
 */

import http from "node:http";
import path from "node:path";
import { readFileSync } from "node:fs";
import { dec } from "../../../scripts/lib/nats-harness.mjs";

import { connect } from "@flux/sdk";

import { cargar } from "./fuente.mjs";
import { EJEMPLOS, brokerObligatorio, levantarEcosistema, natsServer } from "./ecosistema.mjs";

export { brokerObligatorio, natsServer };

const { cargarConfig } = await cargar("config");
const { nuevoToken, sha256Hex } = await cargar("auth");
const { iniciarGateway } = await cargar("gateway");
const { crearRegistro } = await cargar("registro");
const { verificar } = await cargar("firma");

export const dormir = (ms) => new Promise((r) => setTimeout(r, ms));
export const esperarHasta = async (cond, ms = 8000, cada = 25) => {
  const limite = Date.now() + ms;
  while (Date.now() < limite) {
    if (await cond()) return true;
    await dormir(cada);
  }
  return cond();
};

// ─── Un receptor HTTP, como el de un tercero ─────────────────────────────────────────────────────────────────────

/**
 * Un servidor HTTP que registra cada petición y VERIFICA su firma con `verificar` (el código de referencia del receptor).
 * `respuestas` es una cola de respuestas `{ status, headers }`; agotada, contesta `200`.
 */
export async function crearReceptor({ secretos, toleranciaSeg = 300, respuestas = [] }) {
  const peticiones = [];
  const cola = [...respuestas];
  const servidor = http.createServer((req, res) => {
    const trozos = [];
    req.on("data", (t) => trozos.push(t));
    req.on("end", () => {
      const cuerpo = Buffer.concat(trozos);
      const firma = req.headers["flux-signature"];
      const r = {
        metodo: req.method,
        url: req.url,
        cabeceras: req.headers,
        cuerpo,
        recibidoMs: Date.now(),
        verificacion: verificar({ cabecera: firma, cuerpo, secretos, ahoraMs: Date.now(), toleranciaSeg }),
        idempotencyKey: req.headers["idempotency-key"],
      };
      peticiones.push(r);
      const { status = 200, headers = {} } = cola.shift() ?? {};
      res.writeHead(status, headers);
      res.end();
    });
  });
  await new Promise((r) => servidor.listen(0, "127.0.0.1", r));
  const { port } = servidor.address();
  return {
    url: `http://127.0.0.1:${port}/hooks/flux`,
    puerto: port,
    peticiones,
    encola: (...rs) => cola.push(...rs),
    async cerrar() {
      servidor.closeAllConnections();
      await new Promise((r) => servidor.close(r));
    },
  };
}

// ─── El gateway, arrancado de verdad ─────────────────────────────────────────────────────────────────────────────

/** Los valores de los secretos de las pruebas, y los NOMBRES de las variables de entorno que usa `examples/gateway.json`. */
export const SECRETOS = {
  TIENDA_ENTREGA: "whsec_entrega_tienda_0123456789abcdef",
  PASARELA_HOOK: "whsec_pasarela_hook_fedcba9876543210",
  GITHUB_HOOK: "It's a Secret to Everybody",
};
export const VARIABLES = {
  TIENDA_ENTREGA: "TIENDA_WEB_ENTREGA_SECRETO",
  PASARELA_HOOK: "PASARELA_HOOK_SECRETO",
  GITHUB_HOOK: "GITHUB_HOOK_SECRETO",
};

/**
 * Levanta TODO: el broker con la ACL generada y los streams aprovisionados, y el gateway con los tres clientes de borde.
 *
 * @param {object} [o]
 * @param {(cfg: object) => void} [o.ajusta]    para cambiar la configuración antes de validarla
 * @param {string[]} [o.entregas]               NO usado: las entregas se declaran en `ajusta`
 * @param {object} [o.receptor]                 el receptor de las entregas de `tienda-web` (se crea si no se da)
 */
export async function levantarTodo({ ajusta, receptor, limits } = {}) {
  const eco = await levantarEcosistema();
  const recep = receptor ?? (await crearReceptor({ secretos: [SECRETOS.TIENDA_ENTREGA] }));

  const tokens = { "tienda-web": nuevoToken(), "catalogo-web": nuevoToken(), "pasarela-web": nuevoToken() };
  const env = {
    TIENDA_WEB_NKEY_SEED: dec.decode(eco.claves["tienda-web"].seed),
    CATALOGO_WEB_NKEY_SEED: dec.decode(eco.claves["catalogo-web"].seed),
    PASARELA_WEB_NKEY_SEED: dec.decode(eco.claves["pasarela-web"].seed),
    [VARIABLES.TIENDA_ENTREGA]: SECRETOS.TIENDA_ENTREGA,
    [VARIABLES.PASARELA_HOOK]: SECRETOS.PASARELA_HOOK,
    [VARIABLES.GITHUB_HOOK]: SECRETOS.GITHUB_HOOK,
  };

  // La configuración del EJEMPLO, con el broker, el puerto, los tokens y la URL del receptor sustituidos.
  const cfg = JSON.parse(readFileSync(path.join(EJEMPLOS, "gateway.json"), "utf8"));
  cfg.listen = { host: "127.0.0.1", port: 0 };
  cfg.nats = { servers: eco.url };
  if (limits) cfg.limits = limits;
  for (const c of cfg.clients) c.tokens = [{ id: `${c.service}-1`, sha256: sha256Hex(tokens[c.service]) }];
  for (const d of cfg.clients[0].deliveries) Object.assign(d, { url: recep.url, allowInsecureHttp: true, timeoutMs: 3000 });
  ajusta?.(cfg, { eco, env });
  const config = cargarConfig(cfg, { env });

  const lineas = [];
  const registro = crearRegistro({ escribir: (l) => lineas.push(l), nivel: "debug" });
  const gw = await iniciarGateway(config, { registro });
  const url = `http://127.0.0.1:${gw.port}`;

  /** Una petición HTTP al gateway. `cuerpo`: un objeto (se envía como JSON) o una cadena/Buffer tal cual. */
  async function llamar(metodo, ruta, { token, cuerpo, cabeceras = {}, tipo = "application/json" } = {}) {
    const bytes = cuerpo === undefined ? undefined : Buffer.isBuffer(cuerpo) || typeof cuerpo === "string" ? Buffer.from(cuerpo) : Buffer.from(JSON.stringify(cuerpo));
    return new Promise((resolve, reject) => {
      const req = http.request(`${url}${ruta}`, {
        method: metodo,
        agent: false,
        headers: {
          ...(token && { authorization: `Bearer ${token}` }),
          ...(bytes && { "content-type": tipo, "content-length": bytes.length }),
          ...cabeceras,
        },
      }, (res) => {
        const trozos = [];
        res.on("data", (t) => trozos.push(t));
        res.on("end", () => {
          const texto = Buffer.concat(trozos).toString("utf8");
          let json;
          try { json = texto ? JSON.parse(texto) : undefined; } catch { /* no es JSON */ }
          resolve({ status: res.statusCode, cabeceras: res.headers, texto, json });
        });
      });
      req.on("error", reject);
      req.end(bytes);
    });
  }

  /** Un `connect()` NATIVO (no del gateway) de un servicio del manifiesto, con SUS credenciales. */
  const nativo = async (servicio, extra = {}) =>
    connect({
      servers: eco.url, service: servicio, environment: "produccion", version: "1.0.0", tenantId: "acme",
      schemaBaseUrl: "https://schemas.internal", credentials: eco.credenciales(servicio), ...extra,
    });

  return {
    eco, gw, url, config, tokens, receptor: recep, lineas, llamar, nativo, registro,
    /** Todo lo que el gateway escribió, en una cadena: para buscar lo que NO debe estar. */
    registroEntero: () => lineas.join("\n"),
    async parar() {
      await gw.cerrar(3000).catch(() => {});
      if (!receptor) await recep.cerrar().catch(() => {});
      await eco.parar();
    },
  };
}

/** Los mensajes de un stream, parseados (para inspeccionar la DLQ y EVT_). */
export async function mensajesDe(jsm, stream) {
  const info = await jsm.streams.info(stream);
  const out = [];
  if (info.state.messages === 0) return out;
  for (let seq = info.state.first_seq; seq <= info.state.last_seq; seq++) {
    const m = await jsm.streams.getMessage(stream, { seq }).catch(() => null);
    if (m) out.push(JSON.parse(Buffer.from(m.data).toString("utf8")));
  }
  return out;
}

/**
 * Como `describe`, pero con el contrato del broker (el de los demás tests del repo): sin `nats-server` se SALTA con aviso, salvo
 * con FLUX_REQUIRE_BROKER=1, donde FALLA. Un CI verde que no ejecutó nada contra el broker aparenta cobertura.
 */
export function describeBroker(describe, test, nombre, cuerpo) {
  const { motivo } = natsServer();
  if (!motivo) return describe(nombre, cuerpo);
  if (brokerObligatorio()) {
    return describe(nombre, () => test("están todos los requisitos (FLUX_REQUIRE_BROKER está activada)", () => { throw new Error(`${motivo}. Con FLUX_REQUIRE_BROKER las pruebas contra el broker son obligatorias.`); }));
  }
  console.warn(`\nAVISO: se SALTAN las pruebas «${nombre}»: ${motivo}.\n`);
  return describe(nombre, { skip: motivo }, () => test("(saltado)", () => {}));
}
