/**
 * Levanta el ecosistema BAJO LA CONFIGURACIÓN SEGURA, con los mismos comandos que usarás en tu proyecto:
 *
 *   1. `scripts/dev-secure.mjs`  → CA propia, una nkey por servicio y las ACLs que genera `gen-acl.mjs` a partir de services.json
 *                                  (nada de permisos escritos a mano, nada de `--dev`: ningún servicio puede crear streams ni consumidores)
 *   2. `nats-server -c …`        → TLS obligatorio, la ACL generada
 *   3. `flux provision`          → los streams y los durables que esas ACLs presuponen (con las credenciales de OPERACIÓN)
 *   4. el gateway (`gateway/`)   → aloja a `web-php`, que no puede mantener una conexión NATS
 *   5. `php -S`                  → el «hosting compartido»: ficha.php y recibir.php, que solo hablan HTTP con el gateway
 *   6. los servicios nativos     → un PROCESO por servicio, cada uno con SOLO su semilla (el `crm` no ve la de `facturacion`)
 *
 * Lo usan `run.mjs` (el guion) y `test/ecosistema.test.mjs` (el extremo a extremo): lo que ves es lo que se prueba.
 *
 * Entorno: NATS_SERVER (el binario; si no, el del PATH) · FLUX_TEST_PORTS `desde-hasta` (por defecto 7000-7099) ·
 *          ECOSISTEMA_DIR (otra copia del ejemplo: los mutantes) · FLUX_REQUIRE_PHP=1 (sin `php`, es un fallo y no un salto).
 */

import { spawn, spawnSync } from "node:child_process";
import { createHmac, randomBytes } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { connect } from "@flux/sdk";

import { conectar, enc, jetstream, jetstreamManager } from "../../../scripts/lib/nats-harness.mjs";

const AQUI = path.dirname(fileURLToPath(import.meta.url));
export const RAIZ = path.join(AQUI, "..", "..", "..");
/** Las fuentes del ejemplo (services.json, schemas/, servicios/, web-php/, gateway.json): este directorio, u otra copia (los mutantes). */
export const DIR = process.env.ECOSISTEMA_DIR ? path.resolve(process.env.ECOSISTEMA_DIR) : path.join(AQUI, "..");
export const SERVICIOS = ["crm", "facturacion", "tienda-web"];

export const dormir = (ms) => new Promise((r) => setTimeout(r, ms));

/** Espera a que `cond()` sea verdadera. Plazos holgados: la máquina puede ir cargada. */
export async function esperarHasta(cond, ms = 20_000, cada = 40) {
  const limite = Date.now() + ms;
  for (;;) {
    const v = await cond();
    if (v) return v;
    if (Date.now() > limite) return v;
    await dormir(cada);
  }
}

export function natsServer() {
  const bin = process.env.NATS_SERVER || "nats-server";
  const r = spawnSync(bin, ["--version"], { encoding: "utf8" });
  if (r.error || r.status !== 0) {
    return { motivo: `no se puede ejecutar \`${bin} --version\`. Define NATS_SERVER con la ruta del binario (nats-server 2.14: \`GOBIN=$PWD/bin go install github.com/nats-io/nats-server/v2@v2.14.5\`)` };
  }
  return { bin, version: r.stdout.trim() };
}

export function phpDisponible() {
  const r = spawnSync("php", ["-r", "exit(function_exists('curl_init') && function_exists('hash_hmac') ? 0 : 3);"], { encoding: "utf8" });
  return !r.error && r.status === 0;
}

// ─── Procesos ───────────────────────────────────────────────────────────────────────────────────────────────────

/** Un proceso hijo con su salida recogida. Los servicios escriben una línea JSON por evento y aceptan comandos JSON por stdin. */
export class Proceso {
  constructor(nombre, orden, args, { env, cwd } = {}) {
    this.nombre = nombre;
    this.lineas = [];
    this.texto = "";
    this.n = 0;
    this.hijo = spawn(orden, args, { env, cwd, stdio: ["pipe", "pipe", "pipe"] });
    let resto = "";
    this.hijo.stdout.on("data", (d) => {
      resto += d;
      const partes = resto.split("\n");
      resto = partes.pop();
      for (const l of partes) {
        this.texto += `${l}\n`;
        try { this.lineas.push(JSON.parse(l)); } catch { this.lineas.push({ texto: l }); }
      }
    });
    this.hijo.stderr.on("data", (d) => { this.texto += d; });
    this.hijo.stdin.on("error", () => {});
    this.salio = new Promise((r) => this.hijo.once("exit", (codigo, senal) => { this.codigo = codigo ?? senal; r(this.codigo); }));
  }

  get vivo() { return this.hijo.exitCode === null && this.hijo.signalCode === null; }

  /** Espera una línea que cumpla `pred`; si no llega, lanza con TODA la salida del proceso (sin ella, un fallo no se diagnostica). */
  async esperar(pred, ms = 20_000, que = "una línea") {
    const l = await esperarHasta(() => this.lineas.find(pred), ms);
    if (!l) throw new Error(`[${this.nombre}] no apareció ${que} en ${ms} ms. Salida:\n${this.texto.trim()}`);
    return l;
  }

  /** Cuántas líneas cumplen `pred`. */
  cuenta(pred) { return this.lineas.filter(pred).length; }

  /** Un comando del operador: escribe `{ n, cmd, … }` y espera su `resultado`. Lanza (con el `codigo` estable) si el servicio lo rechazó. */
  async comando(cmd, args = {}, ms = 20_000) {
    const n = ++this.n;
    this.hijo.stdin.write(`${JSON.stringify({ n, cmd, ...args })}\n`);
    const r = await this.esperar((l) => l.evento === "resultado" && l.n === n, ms, `el resultado de «${cmd}»`);
    if (!r.ok) throw Object.assign(new Error(`[${this.nombre}] «${cmd}» falló: ${r.codigo}`), { codigo: r.codigo });
    return r;
  }

  async parar(senal = "SIGTERM", ms = 8000) {
    if (!this.vivo) return;
    this.hijo.kill(senal);
    await Promise.race([this.salio, dormir(ms)]);
    if (this.vivo) this.hijo.kill("SIGKILL");
    await this.salio;
  }
}

// ─── Puertos ────────────────────────────────────────────────────────────────────────────────────────────────────

function rangoDePuertos() {
  const m = /^(\d+)-(\d+)$/.exec(process.env.FLUX_TEST_PORTS ?? "7000-7099");
  if (!m) throw new Error("FLUX_TEST_PORTS debe ser `desde-hasta`, por ejemplo 7000-7099");
  return { desde: Number(m[1]), hasta: Number(m[2]) };
}

async function puertoLibre(usados) {
  const { desde, hasta } = rangoDePuertos();
  for (let p = desde; p <= hasta; p++) {
    if (usados.has(p)) continue;
    const libre = await new Promise((ok) => {
      const s = net.createServer();
      s.once("error", () => ok(false));
      s.listen(p, "127.0.0.1", () => s.close(() => ok(true)));
    });
    if (libre) { usados.add(p); return p; }
  }
  throw new Error(`no hay un puerto libre entre ${desde} y ${hasta} (FLUX_TEST_PORTS)`);
}

function ejecutar(orden, args, opciones, que) {
  const r = spawnSync(orden, args, { encoding: "utf8", cwd: RAIZ, ...opciones });
  if (r.error || r.status !== 0) throw new Error(`${que} falló (${r.status ?? r.error?.code}):\n${(r.stdout + r.stderr).trim()}`);
  return r.stdout;
}

// ─── El ecosistema ──────────────────────────────────────────────────────────────────────────────────────────────

/**
 * @param {object} [o]
 * @param {string} [o.dir]        dónde se escribe todo (claves, datos, registros). Por defecto, un temporal.
 * @param {boolean} [o.php=true]  arrancar el «hosting compartido» (ficha.php, recibir.php)
 */
export async function levantar({ dir, php = true } = {}) {
  const { bin, motivo } = natsServer();
  if (!bin) throw new Error(motivo);
  const conPhp = php && phpDisponible();
  if (php && !conPhp && process.env.FLUX_REQUIRE_PHP) throw new Error("FLUX_REQUIRE_PHP está activada y no hay `php` con curl en el PATH");

  const run = dir ? path.resolve(dir) : mkdtempSync(path.join(os.tmpdir(), "flux-eco-"));
  mkdirSync(run, { recursive: true });
  const seguro = path.join(run, "seguro");
  const datos = path.join(run, "datos");
  const manifiesto = path.join(DIR, "services.json");
  const bundle = path.join(DIR, "schemas", "bundle.json");
  const usados = new Set();
  const puertos = { nats: await puertoLibre(usados), gateway: await puertoLibre(usados), php: conPhp ? await puertoLibre(usados) : 0 };
  const hijos = [];
  const procesos = {};
  const limpiar = async () => {
    await Promise.allSettled(hijos.map((h) => h.parar()));
  };

  try {
    // 1. CA, nkeys y ACLs generadas. `--bundle`: los contratos de ESTE ecosistema.
    ejecutar(process.execPath, [path.join(RAIZ, "scripts", "dev-secure.mjs"), manifiesto, "--bundle", bundle, "--out", seguro, "--port", String(puertos.nats)], {}, "dev-secure.mjs");

    // 2. El broker.
    const nats = new Proceso("nats-server", bin, ["-c", path.join(seguro, "nats.conf")], {});
    hijos.push(nats);
    const listo = await esperarHasta(() => nats.texto.includes("Server is ready") || !nats.vivo, 15_000);
    if (!listo || !nats.vivo) throw new Error(`nats-server no arrancó:\n${nats.texto.trim()}`);
    const url = `tls://127.0.0.1:${puertos.nats}`;
    const ca = path.join(seguro, "ca.pem");

    // 3. Streams y durables: con las credenciales de OPERACIÓN, nunca las de un servicio.
    ejecutar(process.execPath, [path.join(RAIZ, "cli", "src", "index.mjs"), "provision", manifiesto, "--bundle", bundle, "--server", url, "--creds", path.join(seguro, "ops.creds")],
      { env: { ...process.env, NODE_EXTRA_CA_CERTS: ca, NO_COLOR: "1" } }, "flux provision");

    const semilla = (servicio) => readFileSync(path.join(seguro, "nkeys", `${servicio}.nk`), "utf8").trim();

    // 4. El gateway, con un token NUEVO por ejecución y los secretos de los webhooks y de las entregas.
    const t = ejecutar(process.execPath, [path.join(RAIZ, "gateway", "src", "index.mjs"), "token"], {}, "flux-gateway token").match(/token:\s+(\S+)\s+sha256:\s+(\S+)/);
    const token = t[1];
    const secretos = { pasarela: randomBytes(32).toString("hex"), entrega: randomBytes(32).toString("hex") };
    const cfg = JSON.parse(readFileSync(path.join(DIR, "gateway.json"), "utf8"));
    cfg.listen = { host: "127.0.0.1", port: puertos.gateway };
    cfg.nats = { servers: [url], tls: { caFile: ca } };
    cfg.bundleFile = bundle;
    cfg.clients[0].tokens = [{ id: "web-php-1", sha256: t[2] }];
    if (conPhp) Object.assign(cfg.clients[0].deliveries[0], { url: `http://127.0.0.1:${puertos.php}/recibir.php`, allowInsecureHttp: true, timeoutMs: 5000 });
    else cfg.clients[0].deliveries = [];
    const cfgRuta = path.join(run, "gateway.json");
    writeFileSync(cfgRuta, JSON.stringify(cfg, null, 2));
    const gateway = new Proceso("gateway", process.execPath, [path.join(RAIZ, "gateway", "src", "index.mjs"), "--config", cfgRuta], {
      cwd: RAIZ,
      env: { ...process.env, WEB_PHP_NKEY_SEED: semilla("web-php"), PASARELA_HOOK_SECRETO: secretos.pasarela, WEB_PHP_ENTREGA_SECRETO: secretos.entrega },
    });
    hijos.push(gateway);
    const gatewayUrl = `http://127.0.0.1:${puertos.gateway}`;
    const sano = await esperarHasta(async () => (await fetch(`${gatewayUrl}/healthz`).catch(() => ({ ok: false }))).ok || !gateway.vivo, 20_000);
    if (!sano || !gateway.vivo) throw new Error(`el gateway no arrancó:\n${gateway.texto.trim()}`);

    // 5. El «hosting compartido»: PHP con su servidor integrado, configurado por el entorno como lo estaría un PHP-FPM.
    let phpUrl;
    if (conPhp) {
      mkdirSync(path.join(run, "datos-web"), { recursive: true });
      const srv = new Proceso("php", "php", ["-S", `127.0.0.1:${puertos.php}`, "-t", path.join(DIR, "web-php")], {
        env: { ...process.env, FLUX_GATEWAY_URL: gatewayUrl, FLUX_GATEWAY_TOKEN: token, FLUX_ENTREGA_SECRETO: secretos.entrega, FLUX_WEB_DATA_DIR: path.join(run, "datos-web") },
      });
      hijos.push(srv);
      phpUrl = `http://127.0.0.1:${puertos.php}`;
      if (!(await esperarHasta(async () => (await fetch(`${phpUrl}/ficha.php`).catch(() => null)) !== null || !srv.vivo, 10_000)) || !srv.vivo) throw new Error(`php -S no arrancó:\n${srv.texto.trim()}`);
    }

    // 6. Los servicios nativos: un proceso cada uno, con SOLO su semilla.
    const arrancar = async (servicio, extra = {}) => {
      const p = new Proceso(servicio, process.execPath, [path.join(DIR, "servicios", `${servicio}.mjs`)], {
        cwd: DIR,
        env: {
          PATH: process.env.PATH, HOME: process.env.HOME,
          NATS_URL: url, NATS_CA_FILE: ca, NKEY_SEED_FILE: path.join(seguro, "nkeys", `${servicio}.nk`),
          DATA_DIR: datos, BUNDLE_FILE: bundle, FLUX_TENANT: "acme", FLUX_ENV: "produccion", ...extra,
        },
      });
      hijos.push(p);
      procesos[servicio] = p;
      await p.esperar((l) => l.evento === "listo" || l.evento === "no-arranca", 25_000, "«listo»");
      if (p.lineas.some((l) => l.evento === "no-arranca")) throw new Error(`[${servicio}] no arranca:\n${p.texto.trim()}`);
      return p;
    };
    /** Para un servicio y lo vuelve a arrancar (conserva su base de datos: es un reinicio, no una instalación nueva). */
    const reiniciar = async (servicio, senal = "SIGTERM") => {
      await procesos[servicio].parar(senal);
      return arrancar(servicio);
    };

    // El usuario de operación y una conexión como cualquier servicio, para las pruebas.
    const ops = await conectar(url, { nombre: "ops", seed: enc.encode(semilla("ops")), tls: { caFile: ca }, etiqueta: "flux-eco" });
    const conectarComo = (servicio, extra = {}) => connect({
      servers: url, service: servicio, environment: "produccion", version: "1.0.0", tenantId: "acme", schemaBaseUrl: "https://schemas.internal",
      credentials: { nkeySeed: semilla(servicio) }, tls: { caFile: ca }, ...extra,
    });
    /** Una conexión NATS en bruto como un servicio (para sondear el broker sin pasar por el SDK). */
    const crudaComo = (servicio) => conectar(url, { nombre: servicio, seed: enc.encode(semilla(servicio)), inboxPrefix: `_INBOX.${servicio}`, tls: { caFile: ca }, etiqueta: "flux-eco" });

    const jsm = await jetstreamManager(ops);
    const js = jetstream(ops);

    return {
      run, datos, seguro, url, ca, puertos, gatewayUrl, phpUrl, token, secretos, conPhp,
      nats, gateway, procesos, ops, jsm, js, arrancar, reiniciar, conectarComo, crudaComo, semilla,
      /**
       * Vuelve a publicar los BYTES del último mensaje de un subject con otro `Nats-Msg-Id`: lo que ve un consumidor tras un replay de la DLQ o un
       * productor que reintenta fuera de `duplicate_window` (A-157). El CloudEvent lleva el mismo `id`: el consumidor idempotente lo ignora.
       */
      async reinyectar(stream, subject) {
        const m = await jsm.streams.getMessage(stream, { last_by_subj: subject });
        await js.publish(subject, m.data, { msgID: `reinyeccion-${Date.now()}-${Math.random()}` });
      },
      /** Un webhook de la pasarela de pagos (un TERCERO) al gateway, firmado como ella: `t=<unix>,v1=<hmac-sha256(secreto, t + "." + cuerpo)>`. */
      webhook(cuerpo, { secreto = secretos.pasarela, t = Math.floor(Date.now() / 1000) } = {}) {
        return fetch(`${gatewayUrl}/v1/hooks/pasarela`, {
          method: "POST",
          headers: { "content-type": "application/json", "x-pasarela-firma": `t=${t},v1=${createHmac("sha256", secreto).update(`${t}.${cuerpo}`).digest("hex")}` },
          body: cuerpo,
        });
      },
      /** Lee la «base de datos» de un servicio (su fichero JSON). */
      leer: (servicio) => JSON.parse(readFileSync(path.join(datos, `${servicio}.json`), "utf8")),
      leerWeb: () => { try { return readFileSync(path.join(run, "datos-web", "recibidas.jsonl"), "utf8").split("\n").filter(Boolean); } catch { return []; } },
      async parar({ conservar = false } = {}) {
        await ops.close().catch(() => {});
        await limpiar();
        if (!conservar && !dir) rmSync(run, { recursive: true, force: true });
      },
    };
  } catch (e) {
    await limpiar();
    if (!dir) rmSync(run, { recursive: true, force: true });
    throw e;
  }
}
