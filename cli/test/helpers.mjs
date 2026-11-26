/**
 * Utilidades de las pruebas de integración de la CLI: un `nats-server` propio, la CLI como
 * proceso (lo que de verdad usa un operador) y un escritor de estado A MANO.
 *
 * El escritor implementa el algoritmo de `put`/`delete` de 10-state.md §3.2 sobre NATS crudo,
 * **sin ningún SDK de flux**: la CLI no depende del SDK, y para comprobar lo que ve hay que
 * escribir el estado por otro camino. Es la misma técnica que `conformance/state-broker.mjs`.
 *
 *   NATS_SERVER=/ruta/a/nats-server npm test
 *   FLUX_TEST_PORTS=desde-hasta        (por defecto 4650-4699; la suite de conformidad usa 4600-4649)
 *   FLUX_REQUIRE_BROKER=1              que falte nats-server es un FALLO, no un salto
 */

import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer, createConnection } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { connect, headers as natsHeaders, nanos } from "@nats-io/transport-node";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";

const AQUI = dirname(fileURLToPath(import.meta.url));
export const CLI = join(AQUI, "..", "src", "index.mjs");
export const BIN = process.env.NATS_SERVER ?? "nats-server";
export const REQUIRE = process.env.FLUX_REQUIRE_BROKER === "1";
const [P0, P1] = (process.env.FLUX_TEST_PORTS ?? "4650-4699").split("-").map(Number);

const enc = new TextEncoder();
const dec = new TextDecoder();
export const dormir = (ms) => new Promise((r) => setTimeout(r, ms));

/** ¿Hay un nats-server ejecutable? Síncrono: decide si las suites con broker corren o se saltan. */
export function detectarBroker() {
  const r = spawnSync(BIN, ["--version"], { encoding: "utf8" });
  if (r.error || r.status !== 0) {
    return { ok: false, motivo: `no se pudo ejecutar «${BIN}» (${r.error?.code ?? `salida ${r.status}`}). Define NATS_SERVER=/ruta/a/nats-server` };
  }
  return { ok: true, version: r.stdout.trim() };
}

// ─── Broker ──────────────────────────────────────────────────────────────────────────────────────

const libre = (puerto) =>
  new Promise((res) => {
    const s = createServer();
    s.once("error", () => res(false));
    s.listen(puerto, "127.0.0.1", () => s.close(() => res(true)));
  });

async function esperaPuerto(puerto, ms = 8000) {
  const fin = Date.now() + ms;
  while (Date.now() < fin) {
    const ok = await new Promise((res) => {
      const s = createConnection(puerto, "127.0.0.1");
      s.on("connect", () => { s.destroy(); res(true); });
      s.on("error", () => res(false));
    });
    if (ok) return;
    await dormir(80);
  }
  throw new Error(`nats-server no escucha en ${puerto}`);
}

/**
 * Levanta un `nats-server` con JetStream en un puerto libre del rango y devuelve
 * `{ url, parar }`. Mata SOLO el proceso que lanzó (nunca `pkill`) y borra su directorio.
 */
export async function arrancaBroker() {
  let puerto = null;
  for (let p = P0; p <= P1 && puerto === null; p++) if (await libre(p)) puerto = p;
  if (puerto === null) throw new Error(`no queda ningún puerto libre en FLUX_TEST_PORTS (${P0}-${P1})`);

  const dir = mkdtempSync(join(tmpdir(), "flux-cli-"));
  const hijo = spawn(BIN, ["-js", "-sd", join(dir, "js"), "-p", String(puerto), "-a", "127.0.0.1"], { stdio: "ignore" });
  const limpiar = () => rmSync(dir, { recursive: true, force: true });
  try {
    await esperaPuerto(puerto);
  } catch (e) {
    hijo.kill();
    limpiar();
    throw e;
  }
  return {
    url: `nats://127.0.0.1:${puerto}`,
    async parar() {
      const salio = new Promise((r) => hijo.once("exit", r));
      hijo.kill();
      await salio;
      limpiar();
    },
  };
}

/** Una conexión de las pruebas, con su `jsm` y `js`. */
export async function conecta(url) {
  const nc = await connect({ servers: url });
  return { nc, jsm: await jetstreamManager(nc), js: jetstream(nc) };
}

// ─── La CLI como proceso ─────────────────────────────────────────────────────────────────────────

/** Ejecuta `flux <args>` contra `url`; devuelve `{ code, out, err }`. Sin terminal: stdin cerrado. */
export function flux(url, args, { env } = {}) {
  return new Promise((resolve) => {
    const h = spawn(process.execPath, [CLI, "-s", url, ...args], {
      env: { ...process.env, NO_COLOR: "1", ...env },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "";
    let err = "";
    h.stdout.on("data", (d) => (out += d));
    h.stderr.on("data", (d) => (err += d));
    h.on("close", (code) => resolve({ code, out, err }));
  });
}

/** Un `flux` de larga vida (`tail`): se mira su salida hasta que aparece algo y se mata. */
export function fluxVivo(url, args) {
  const h = spawn(process.execPath, [CLI, "-s", url, ...args], {
    env: { ...process.env, NO_COLOR: "1" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const o = { out: "", err: "" };
  h.stdout.on("data", (d) => (o.out += d));
  h.stderr.on("data", (d) => (o.err += d));
  return {
    salida: o,
    async hasta(cond, ms = 8000) {
      const fin = Date.now() + ms;
      while (Date.now() < fin) {
        if (cond(o)) return true;
        await dormir(50);
      }
      return false;
    },
    async parar() {
      // Si el proceso ya salió (p.ej. la CLI abortó con un error), `exit` no volverá a emitirse.
      if (h.exitCode !== null || h.signalCode !== null) return;
      const salio = new Promise((r) => h.once("exit", r));
      h.kill("SIGKILL");
      await salio;
    },
  };
}

// ─── Streams de estado, como los crearía `flux provision` ────────────────────────────────────────

/** `STATE_<DOM>` con la configuración de 10-state.md §2.1; `cambios` la estropea a propósito. */
export async function creaStreamEstado(jsm, dom, cambios = {}) {
  const nombre = dom.replace(/-/g, "_").toUpperCase();
  const cfg = {
    name: `STATE_${nombre}`, subjects: [`state.${dom}.>`], storage: "file", retention: "limits",
    max_msgs_per_subject: 1, allow_direct: true, max_age: 0, discard: "old",
    duplicate_window: nanos(120_000), num_replicas: 1, ...cambios,
  };
  for (const k of Object.keys(cfg)) if (cfg[k] === undefined) delete cfg[k];
  return jsm.streams.add(cfg);
}

/** `DLQ_<DOM>` (90 días) y `EVT_<DOM>` (30 días), con los subjects que se pidan. */
export async function creaDlq(jsm, dom, subjects = [`dlq.${dom}.>`, `dlq.state.${dom}.>`]) {
  const nombre = dom.replace(/-/g, "_").toUpperCase();
  return jsm.streams.add({
    name: `DLQ_${nombre}`, subjects, storage: "file", retention: "limits", discard: "old",
    max_age: nanos(90 * 86_400_000), num_replicas: 1,
  });
}
export async function creaEvt(jsm, dom) {
  const nombre = dom.replace(/-/g, "_").toUpperCase();
  return jsm.streams.add({
    name: `EVT_${nombre}`, subjects: [`${dom}.>`], storage: "file", retention: "limits", discard: "old",
    max_age: nanos(30 * 86_400_000), duplicate_window: nanos(120_000), num_replicas: 1,
  });
}

/** Un durable de estado con la config canónica; `cambios` la estropea a propósito. */
export async function creaDurable(jsm, stream, nombre, filtro, cambios = {}) {
  return jsm.consumers.add(stream, {
    durable_name: nombre, filter_subject: filtro, ack_policy: "explicit", ack_wait: nanos(30_000), max_deliver: 6,
    backoff: [30_000, 60_000, 300_000, 900_000, 1_800_000].map((x) => nanos(x)), max_ack_pending: 256,
    deliver_policy: "last_per_subject", replay_policy: "instant", ...cambios,
  });
}

/** Borra todos los streams del broker: cada prueba parte de un broker limpio. */
export async function vaciaBroker(jsm) {
  for await (const s of jsm.streams.list()) await jsm.streams.delete(s.config.name);
}

// ─── Escribir estado a mano (10-state.md §3.2, sin SDK) ──────────────────────────────────────────

let contadorId = 0;
const idNuevo = () => `01924f8e-7c3a-7b2d-9e14-${String(Date.now() % 1_000_000).padStart(6, "0")}${String(++contadorId).padStart(6, "0")}`;

/** El sobre de una entrada de estado (10-state.md §2.5); `data` va el último. */
export function sobreEstado({ tipo = "put", contrato, tenant, clave, version, clasificacion, id, data, cambios = {} }) {
  const [dom, agr, major] = contrato.split(".");
  const i = id ?? idNuevo();
  const put = tipo === "put";
  return {
    specversion: "1.0", id: i, source: "/produccion/crm-api",
    type: put ? `com.flux.state.${dom}.${agr}.${major}` : `com.flux.state.${dom}.${agr}.deleted.${major}`,
    time: "2026-08-20T10:25:39.412Z", datacontenttype: "application/json",
    dataschema: put ? `https://schemas.internal/state/${dom}/${agr}/1.0.0.json` : "https://schemas.internal/system/state/eliminado/1.0.0.json",
    subject: clave, correlationid: i, tenantid: tenant, producerversion: "2.1.0",
    dataclassification: clasificacion ?? (put ? "confidential" : "internal"), partitionkey: clave,
    ...cambios,
    data: data ?? (put ? { clienteId: clave, nombre: `Cliente ${clave}`, email: `${clave}@example.com`, aggregateVersion: version } : { aggregateVersion: version }),
  };
}

/**
 * `put` / `delete` según §3.2, SIN reintentos (no hay carrera): lee la secuencia con DIRECT.GET,
 * descarta lo que no avanza la versión y publica con `Nats-Expected-Last-Subject-Sequence`.
 */
export async function escribeEstado({ nc, js }, { tipo = "put", contrato, tenant, clave, version, ...resto }) {
  const [dom] = contrato.split(".");
  const stream = `STATE_${dom.replace(/-/g, "_").toUpperCase()}`;
  const subject = `state.${contrato}.${tenant}.${clave}`;
  const r = await nc.request(`$JS.API.DIRECT.GET.${stream}.${subject}`, "", { timeout: 2000 });
  let S = 0;
  if (r.headers?.code !== 404) {
    S = Number(r.headers.get("Nats-Sequence"));
    const v = JSON.parse(dec.decode(r.data))?.data?.aggregateVersion;
    if (Number.isInteger(v) && version <= v) return { applied: false, outcome: "stale" };
  }
  const ev = sobreEstado({ tipo, contrato, tenant, clave, version, ...resto });
  const h = natsHeaders();
  h.set("Nats-Msg-Id", ev.id);
  h.set("Nats-Expected-Last-Subject-Sequence", String(S));
  const ack = await js.publish(subject, enc.encode(JSON.stringify(ev)), { headers: h });
  return { applied: true, outcome: "ok", revision: ack.seq };
}

/** Publica un mensaje de estado tal cual, sin pasar por el algoritmo (para fabricar entradas rotas). */
export async function publicaCrudo({ js }, subject, cuerpo) {
  const ack = await js.publish(subject, typeof cuerpo === "string" ? enc.encode(cuerpo) : enc.encode(JSON.stringify(cuerpo)));
  return ack.seq;
}

/** Escribe `n` claves `cli-1..cli-n` de un tenant. */
export async function sembrar(conn, { contrato = "clientes.cliente.v1", tenant = "acme", n = 100, version = 1 } = {}) {
  for (let i = 1; i <= n; i++) await escribeEstado(conn, { contrato, tenant, clave: `cli-${i}`, version });
}

/**
 * Drena un durable con ack y devuelve las claves entregadas. `max` es lo que se ESPERA recibir:
 * el fetch termina en cuanto llegan, y solo espera `expires` si faltan (o si se espera 0 y se
 * quiere comprobar que no llega nada).
 */
export async function drena({ js }, stream, durable, max = 1000, expires = 1500) {
  const con = await js.consumers.get(stream, durable);
  const it = await con.fetch({ max_messages: Math.max(max, 1), expires });
  const claves = [];
  for await (const m of it) {
    claves.push(m.subject.split(".").pop());
    m.ack();
  }
  return claves;
}

/** Publica una entrada en una DLQ (el sobre original con las `dlq*`). */
export async function publicaDlq({ js }, subject, sobre, msgID) {
  await js.publish(subject, enc.encode(JSON.stringify(sobre)), { msgID });
}
