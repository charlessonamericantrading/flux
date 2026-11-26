#!/usr/bin/env node
/**
 * Runner de los casos de PETICIÓN/RESPUESTA — specification/11-request-reply.md.
 *
 *   NATS_SERVER=/ruta/a/nats-server node conformance/rpc-broker.mjs
 *   node conformance/rpc-broker.mjs --only queue-group-splits-requests,no-responders-is-instant -v
 *   node conformance/rpc-broker.mjs --list
 *   node conformance/rpc-broker.mjs --mutant no-queue-group        # DEBE fallar
 *
 * Como `state-broker.mjs` y `security.mjs`, **los casos son DATOS y el runner los ejecuta**: no sabe qué es
 * «un reparto por grupo de cola», solo ejecuta el vocabulario de `cases/rpc-semantics.json` y compara lo
 * observado con `expect`. Y, como `run.mjs`, habla NATS directamente, sin ningún SDK de flux: un runner
 * construido sobre un SDK heredaría las suposiciones que debe verificar.
 *
 * Qué mide (todo contra un nats-server real, y cada caso con su `observed`: versión, fecha, resultado):
 *   · reparto con *queue group* entre instancias, qué pasa si una se va, muere o se cuelga;
 *   · `no responders`: su latencia y la bandera de CONNECT que lo activa;
 *   · qué ve quien llama si el que responde tarda más que el timeout, o si la respuesta no cabe;
 *   · `allow_responses`: cuántas respuestas admite, si caduca, a qué subject deja responder;
 *   · cabeceras, y que el core de NATS NO deduplica por `Nats-Msg-Id`;
 *   · una petición DENEGADA por la ACL, con las DOS librerías de NATS de los SDKs (Node, y `async-nats` de Rust
 *     por una sonda: `probes/rpc-async-nats/`);
 *   · la trampa A-18: un stream que captura `rpc.>` contesta con su acuse, y qué subjects de stream lo hacen;
 *   · un cluster de tres nodos.
 *
 * Y comprueba `cases/cross-sdk-rpc-envelope.json`: reconstruye los bytes de los ejemplos de 11-request-reply.md
 * §3.5 desde sus datos y compara su SHA-256, sin fiarse del texto de la spec; y deriva el código de cada vector
 * de la operación y del timeout desde los patrones de protocol.json.
 *
 * Servidores: levanta y para los suyos (uno suelto, uno con la ACL del caso, y un cluster de tres nodos).
 * Puertos: `FLUX_TEST_PORTS=desde-hasta` (por defecto 4700-4749) — se usan hasta 8—. `FLUX_REQUIRE_BROKER=1`: que
 * falte `nats-server` (o `cargo`, para la sonda de Rust) es un FALLO, no un salto. El cluster se salta con
 * `--no-cluster`, la sonda de Rust con `--no-rust`.
 */

import { readFile } from "node:fs/promises";
import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import net from "node:net";
import { connect, headers as natsHeaders } from "@nats-io/transport-node";
import { jetstreamManager } from "@nats-io/jetstream";

const HERE = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const flag = (f) => args.includes(f);
const opt = (f) => (args.includes(f) ? args[args.indexOf(f) + 1] : undefined);
const VERBOSE = flag("-v");
/**
 * `--mutant <nombre>`: estropea A PROPÓSITO lo que el runner monta para demostrar que los casos DETECTAN el fallo
 * (una comprobación que solo se ha visto en verde no demuestra nada). Con un mutante, el runner debe FALLAR.
 *   no-queue-group         los que responden se suscriben SIN grupo de cola → cada instancia recibe todo
 *   wildcard-subscription  el dueño se suscribe a `rpc.<dominio>.>` en vez de al subject exacto de la operación
 *   no-allow-responses     la ACL del dueño no lleva `allow_responses`
 *   stream-captures-rpc    un stream captura `rpc.>` antes de cada caso (la trampa A-18)
 */
const MUTANT = opt("--mutant");
const MUTANTES = ["no-queue-group", "wildcard-subscription", "no-allow-responses", "stream-captures-rpc"];
if (MUTANT && !MUTANTES.includes(MUTANT)) { console.error(`mutante desconocido «${MUTANT}». Los hay: ${MUTANTES.join(", ")}`); process.exit(2); }
const ONLY = opt("--only")?.split(",");
const REQUIRE = process.env.FLUX_REQUIRE_BROKER === "1";
const [PORT0, PORT1] = (process.env.FLUX_TEST_PORTS ?? "4700-4749").split("-").map(Number);
const BIN = process.env.NATS_SERVER ?? "nats-server";

const enc = new TextEncoder();
const dec = new TextDecoder();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const verde = (s) => `\x1b[32m${s}\x1b[0m`;
const rojo = (s) => `\x1b[31m${s}\x1b[0m`;
const gris = (s) => `\x1b[90m${s}\x1b[0m`;
const ahora = () => process.hrtime.bigint();
const msDesde = (t) => Number(process.hrtime.bigint() - t) / 1e6;

/** Un caso que no se puede ejecutar aquí (falta `cargo`): se salta, y con FLUX_REQUIRE_BROKER=1 es un fallo. */
class Salto extends Error {}

// ─── Carga de los casos ──────────────────────────────────────────────────────────────────────────
const leer = async (f) => JSON.parse(await readFile(join(HERE, "cases", f), "utf8"));
const doc = await leer("rpc-semantics.json");
const envelope = await leer("cross-sdk-rpc-envelope.json");
const protocol = JSON.parse(await readFile(join(HERE, "..", "protocol.json"), "utf8"));

if (flag("--list")) {
  for (const c of doc.cases) console.log(`${c.id.padEnd(58)} ${(c.covers ?? []).join(",").padEnd(10)} ${c.title}`);
  process.exit(0);
}

// ─── El solape de subjects: la comprobación de `flux doctor` (11-request-reply.md §8) ─────────────────
/** La forma de TODO subject de operación (cinco tokens): contra ella marca el doctor. `rpc.>` marcaría de más un filtro de 6 tokens. */
const FORMA_RPC = protocol.rpc.doctor.overlapsWith;
/** ¿Hay algún subject concreto que casen a la vez dos patrones de NATS? `*` casa un token; `>`, uno o más al final. */
export function solapa(a, b) {
  const x = a.split("."), y = b.split(".");
  for (let i = 0; ; i++) {
    if (x[i] === ">") return y.length > i;
    if (y[i] === ">") return x.length > i;
    if (i >= x.length || i >= y.length) return x.length === y.length;
    if (x[i] === "*" || y[i] === "*" || x[i] === y[i]) continue;
    return false;
  }
}

// ─── Servidores ──────────────────────────────────────────────────────────────────────────────────
const tmp = mkdtempSync(join(tmpdir(), "flux-rpc-"));
let nextPort = PORT0;
const hijos = [];
const puerto = () => {
  if (nextPort > PORT1) throw new Error(`se acabaron los puertos de FLUX_TEST_PORTS (${PORT0}-${PORT1})`);
  return nextPort++;
};

async function esperaPuerto(port, ms = 8000) {
  const fin = Date.now() + ms;
  while (Date.now() < fin) {
    const ok = await new Promise((res) => { const s = net.createConnection(port, "127.0.0.1"); s.on("connect", () => { s.destroy(); res(true); }); s.on("error", () => res(false)); });
    if (ok) return;
    await sleep(100);
  }
  throw new Error(`nats-server no escucha en ${port}`);
}

function arranca(nombre, conf) {
  const dir = join(tmp, nombre);
  mkdirSync(join(dir, "js"), { recursive: true });
  const f = join(dir, "n.conf");
  writeFileSync(f, conf(dir));
  const p = spawn(BIN, ["-c", f, "-l", join(dir, "log")], { stdio: "ignore" });
  hijos.push(p);
  return p;
}

const lista = (a) => `[${a.map((x) => JSON.stringify(x)).join(", ")}]`;
/** Los permisos de un usuario de `acls`. Ninguna lista sale vacía: en NATS `allow: []` es «sin restricción» (A-25). */
function permisos(u) {
  let allowResponses = u.allowResponses;
  if (MUTANT === "no-allow-responses") allowResponses = undefined;
  const ar = allowResponses === undefined ? "" : allowResponses === true ? ", allow_responses: true"
    : `, allow_responses: { max: ${allowResponses.max ?? 1}${allowResponses.expires ? `, expires: "${allowResponses.expires}"` : ""} }`;
  return `{ publish: { allow: ${lista(u.publish)} }, subscribe: { allow: ${lista(u.subscribe)} }${ar} }`;
}

async function servidorSuelto(users) {
  const port = puerto();
  const auth = users ? `authorization { users: [ ${users.map((u) => `{ user: "${u.name}", password: "pw"${u.publish ? `, permissions: ${permisos(u)}` : ""} }`).join(", ")} ] }` : "";
  arranca(`n${port}`, (dir) => `port: ${port}\nhost: 127.0.0.1\njetstream { store_dir: "${join(dir, "js")}" }\n${auth}\n`);
  await esperaPuerto(port);
  return { servers: [`127.0.0.1:${port}`], port };
}

async function cluster3() {
  const nodos = [0, 1, 2].map(() => ({ client: puerto(), cluster: puerto() }));
  nodos.forEach((n, i) => {
    const routes = nodos.filter((_, j) => j !== i).map((m) => `nats://127.0.0.1:${m.cluster}`).join(", ");
    arranca(`c${i}`, (dir) => `server_name: n${i}\nport: ${n.client}\nhost: 127.0.0.1\njetstream { store_dir: "${join(dir, "js")}" }\ncluster { name: fluxtest\n listen: 127.0.0.1:${n.cluster}\n routes: [${routes}]\n}\n`);
  });
  for (const n of nodos) await esperaPuerto(n.client);
  await sleep(3500); // elección de líder de la capa de metadatos
  return { servers: nodos.map((n) => `127.0.0.1:${n.client}`), ports: nodos.map((n) => n.client) };
}

// ─── Actores ─────────────────────────────────────────────────────────────────────────────────────
/** Una conexión con su contador de violaciones de permiso (el broker avisa aparte: A-28). */
async function conecta(srv, { user, node } = {}) {
  const servers = node === undefined ? srv.servers : [srv.servers[node]];
  const nc = await connect({ servers, ...(user ? { user, pass: "pw" } : {}), inboxPrefix: `_INBOX.${user ?? "anon"}${node === undefined ? "" : `-n${node}`}`, timeout: 4000 });
  const a = { nc, user, violaciones: [] };
  (async () => { for await (const s of nc.status()) { const m = s.error?.message ?? ""; if (/ermission/.test(m)) a.violaciones.push(m); } })();
  return a;
}

// ─── Un cliente NATS «en bruto»: lo que ninguna librería enseña ──────────────────────────────────────
/**
 * Habla el protocolo a mano por un socket: sirve para lo que las librerías esconden (la bandera `no_responders`
 * de CONNECT, los mensajes de estado 503, el `-ERR` de un PUB demasiado grande, un suscriptor mudo).
 */
function crudo(port, { user, headers = true, noResponders = true, pass = "pw" } = {}) {
  const t0 = ahora();
  const r = { frames: [], serverError: null, closed: false, sock: net.connect(port, "127.0.0.1"), ms: () => msDesde(t0), buf: Buffer.alloc(0) };
  r.sock.on("data", (d) => {
    r.buf = Buffer.concat([r.buf, d]);
    for (;;) {
      const i = r.buf.indexOf("\r\n");
      if (i < 0) return;
      const linea = r.buf.subarray(0, i).toString("latin1");
      const [op, ...rest] = linea.split(" ");
      if (op === "MSG" || op === "HMSG") {
        const total = Number(rest.at(-1));
        const hdr = op === "HMSG" ? Number(rest.at(-2)) : 0;
        if (r.buf.length < i + 2 + total + 2) return;
        const carga = r.buf.subarray(i + 2, i + 2 + total);
        const cabecera = hdr ? carga.subarray(0, hdr).toString("latin1") : "";
        const estado = /^NATS\/1\.0 (\d+)/.exec(cabecera)?.[1];
        r.frames.push({ ms: r.ms(), op, subject: rest[0], status: estado ? Number(estado) : undefined, body: carga.subarray(hdr).toString("utf8") });
        r.buf = r.buf.subarray(i + 2 + total + 2);
        continue;
      }
      r.buf = r.buf.subarray(i + 2);
      if (op === "PING") r.sock.write("PONG\r\n");
      else if (op === "-ERR") { r.serverError = linea; r.frames.push({ ms: r.ms(), op: "-ERR", text: linea }); }
    }
  });
  r.sock.on("close", () => { r.closed = true; });
  r.sock.on("error", () => { r.closed = true; });
  const conn = { verbose: false, pedantic: false, protocol: 1, headers, no_responders: noResponders, ...(user ? { user, pass } : {}) };
  r.sock.write(`CONNECT ${JSON.stringify(conn)}\r\nPING\r\n`);
  return r;
}

// ─── Lenguaje de expectativas ────────────────────────────────────────────────────────────────────
const camino = (o, ruta) => ruta.split(".").reduce((x, k) => (x == null ? undefined : x[k]), o);
function cumple(valor, esperado) {
  if (esperado !== null && typeof esperado === "object" && !Array.isArray(esperado)) {
    if ("match" in esperado) return typeof valor === "string" && new RegExp(esperado.match).test(valor);
    if ("includes" in esperado) return Array.isArray(valor) ? valor.includes(esperado.includes) : String(valor).includes(esperado.includes);
    if ("gte" in esperado) return valor >= esperado.gte;
    if ("lte" in esperado) return valor <= esperado.lte;
    if ("between" in esperado) return valor >= esperado.between[0] && valor <= esperado.between[1];
    if ("not" in esperado) return !cumple(valor, esperado.not);
    if ("present" in esperado) return (valor !== undefined && valor !== null) === esperado.present;
    if ("oneOf" in esperado) return esperado.oneOf.some((e) => cumple(valor, e));
  }
  return JSON.stringify(valor) === JSON.stringify(esperado);
}

// ─── El vocabulario ──────────────────────────────────────────────────────────────────────────────
// Vive en los DATOS (cases/rpc-semantics.json → vocabulary.ops): qué hace cada operación, qué argumentos lleva y
// qué observa. Aquí solo está cómo se ejecuta.
const OPS = doc.vocabulary.ops;

const actor = (ctx, nombre) => {
  const a = nombre === "ops" ? ctx.ops : ctx.actores.get(nombre);
  if (!a) throw new Error(`actor «${nombre}» sin conectar antes (op connect)`);
  return a;
};

function clasificaPeticion(e, t, a, antes) {
  const texto = `${e.name}: ${e.message}`;
  const base = { ms: msDesde(t), text: texto };
  if (e.cause?.name === "PermissionViolationError" || /ermission/.test(e.message) || a.violaciones.length > antes) return { ...base, outcome: "denied", text: a.violaciones.at(-1) ?? texto };
  if (e.name === "InvalidArgumentError" && /max_payload/.test(e.message)) return { ...base, outcome: "too_large" };
  if (e.name === "TimeoutError") return { ...base, outcome: "timeout" };
  if (/no responders/i.test(e.message)) return { ...base, outcome: "no_responders" };
  return { ...base, outcome: "error" };
}

const parse = (s) => { try { return JSON.parse(s); } catch { return undefined; } };

async function haz(ctx, a, antes, p) {
  const nc = a.nc;
  let carga = p.data ?? "{}";
  if (p.bytes !== undefined) {
    const n = p.bytes === "max" ? nc.info.max_payload : p.bytes === "max+1" ? nc.info.max_payload + 1 : p.bytes;
    carga = new Uint8Array(n).fill(97);
  }
  const h = p.headers ? (() => { const x = natsHeaders(); for (const [k, v] of Object.entries(p.headers)) x.set(k, v); return x; })() : undefined;
  const t = ahora();
  try {
    const r = await nc.request(p.subject, carga, { timeout: p.timeoutMs ?? 2000, ...(h ? { headers: h } : {}) });
    const body = dec.decode(r.data);
    const j = parse(body);
    return { outcome: "ok", ms: msDesde(t), reply: body, from: j?.from, replyHeaders: r.headers ? [...r.headers.keys()] : [], isJetstreamAck: j?.stream !== undefined && j?.seq !== undefined, stream: j?.stream };
  } catch (e) { return clasificaPeticion(e, t, a, antes); }
}

/** Sirve una operación: el cuerpo de `respond()` de un SDK, reducido a lo que se mide. */
function sirve(ctx, p) {
  const a = actor(ctx, p.as);
  const nc = a.nc;
  const st = { name: p.name, nc, received: 0, replied: 0, errors: [], headersSeen: [], noReply: 0, inflight: 0, actor: a };
  const queue = MUTANT === "no-queue-group" ? undefined : p.queue;
  const subject = MUTANT === "wildcard-subscription" ? `${p.subject.split(".").slice(0, 2).join(".")}.>` : p.subject;
  const sub = nc.subscribe(subject, queue ? { queue } : {});
  st.sub = sub;
  (async () => {
    try {
      for await (const m of sub) {
        // Un dueño suscrito al comodín no sabe contestar a lo que no atiende: la operación sin handler cae al vacío.
        if (MUTANT === "wildcard-subscription" && m.subject !== p.subject) continue;
        st.received++; st.inflight++;
        try {
          if (!m.reply) st.noReply++;
          if (m.headers) st.headersSeen.push([...m.headers.keys()]);
          const modo = p.mode ?? "echo";
          if (modo === "silent") continue;
          if (modo === "die") { await nc.close().catch(() => {}); continue; }
          if (p.delayMs) await sleep(p.delayMs);
          const cuerpo = JSON.stringify({ from: p.name });
          const respond = (data, o) => { try { m.respond(data, o); st.replied++; } catch (e) { st.errors.push(`${e.name}: ${e.message}`); } };
          if (modo === "echo") respond(enc.encode(cuerpo));
          else if (modo === "multi") for (let i = 1; i <= p.replies; i++) { respond(enc.encode(`r${i}`)); await sleep(40); }
          else if (modo === "big") respond(new Uint8Array(p.replyBytes === "max+1" ? nc.info.max_payload + 1 : p.replyBytes));
          else if (modo === "headers") { const h = natsHeaders(); h.set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01"); h.set("X-Visto", st.headersSeen.at(-1)?.join(",") ?? ""); respond(enc.encode(cuerpo), { headers: h }); }
          else if (modo === "foreign-inbox") { try { nc.publish(p.otherSubject, enc.encode("inbox-ajeno")); } catch (e) { st.errors.push(e.message); } await sleep(30); respond(enc.encode(cuerpo)); }
          else throw new Error(`modo desconocido «${modo}»`);
        } finally { st.inflight--; }
      }
    } catch (e) { st.errors.push(`${e.name}: ${e.message}`); }
  })();
  ctx.resp.set(p.name, st);
  return st;
}

const sirvaStats = (st) => ({ received: st.received, replied: st.replied, errors: [...st.errors], errorCount: st.errors.length, headersSeen: st.headersSeen.map((h) => [...h]), noReply: st.noReply, violations: st.actor?.violaciones?.length ?? 0, violationText: st.actor?.violaciones?.join(" | ") ?? "" });

// ─── La sonda de Rust (async-nats) ───────────────────────────────────────────────────────────────
let sondaRust;
async function sondaDeRust(srv) {
  if (flag("--no-rust")) throw new Salto("--no-rust");
  if (!sondaRust) {
    sondaRust = (async () => {
      const manifest = join(HERE, "probes", "rpc-async-nats", "Cargo.toml");
      const target = process.env.CARGO_TARGET_DIR ?? join(tmp, "cargo-target");
      const v = spawnSync("cargo", ["--version"], { encoding: "utf8" });
      if (v.error || v.status !== 0) throw new Salto("no hay `cargo`");
      const b = spawnSync("cargo", ["build", "--release", "--quiet", "--manifest-path", manifest, "--target-dir", target], { encoding: "utf8", timeout: 600000 });
      if (b.status !== 0) throw new Error(`la sonda de Rust no compila:\n${b.stderr}`);
      const r = spawnSync(join(target, "release", "rpc-async-nats-probe"), [srv.servers[0]], { encoding: "utf8", timeout: 90000 });
      if (r.status !== 0) throw new Error(`la sonda de Rust falla (${r.status}):\n${r.stdout}\n${r.stderr}`);
      const lineas = new Map();
      for (const l of r.stdout.split("\n")) { const j = parse(l); if (j?.probe) lineas.set(j.probe, j); }
      return lineas;
    })();
  }
  return sondaRust;
}

// ─── Ejecutor de un paso ─────────────────────────────────────────────────────────────────────────
async function paso(ctx, st) {
  switch (st.op) {
    case "sleep": await sleep(st.ms); return { outcome: "ok" };

    case "connect": {
      const a = await conecta(ctx.srv, { user: st.user, node: st.node });
      ctx.actores.set(st.name, a);
      return { outcome: "ok", maxPayload: a.nc.info.max_payload, headers: a.nc.info.headers, serverVersion: a.nc.info.version };
    }

    case "serve": {
      const r = sirve(ctx, st);
      await r.nc.flush();      // la suscripción está registrada en el servidor antes de que nadie pregunte
      await sleep(30);         // y se nota un aviso de permiso (llega aparte, por la conexión)
      return { outcome: "ok", ...sirvaStats(r) };
    }
    case "served": return sirvaStats(ctx.resp.get(st.name));

    case "unserve": {
      const r = ctx.resp.get(st.name);
      if (st.how === "unsubscribe") { r.sub.unsubscribe(); await r.nc.flush(); }
      else if (st.how === "close") await r.nc.close();
      else if (st.how === "graceful") {            // dejar de escuchar, esperar a lo que está en vuelo, cerrar
        r.sub.unsubscribe(); await r.nc.flush();
        for (let i = 0; i < 100 && r.inflight > 0; i++) await sleep(50);
        await r.nc.close();
      } else if (st.how === "destroy") r.sock.destroy();
      else throw new Error(`how desconocido «${st.how}»`);
      return { outcome: "ok" };
    }

    case "raw_zombie": {   // conectado, suscrito, mudo: un proceso colgado cuya conexión sigue abierta
      const z = crudo(ctx.srv.port, { user: st.user });
      z.sock.write(`SUB ${st.subject}${st.queue ? ` ${st.queue}` : ""} 1\r\nPING\r\n`);
      await sleep(150);
      ctx.resp.set(st.name, { sock: z.sock, received: 0, inflight: 0 });
      return { outcome: "ok" };
    }

    case "request": {
      const a = actor(ctx, st.as);
      return haz(ctx, a, a.violaciones.length, st);
    }
    case "request_async": {
      const a = actor(ctx, st.as);
      ctx.pendientes.set(st.id, haz(ctx, a, a.violaciones.length, st));
      return { outcome: "started" };
    }
    case "await_request": return await ctx.pendientes.get(st.id);

    case "burst": {
      const a = actor(ctx, st.as);
      const r = { ok: 0, acks: 0, timeouts: 0, noResponders: 0, other: 0, byFrom: {} };
      for (let i = 0; i < st.count; i++) {
        const x = await haz(ctx, a, a.violaciones.length, { subject: st.subject, timeoutMs: st.timeoutMs ?? 1500 });
        if (x.outcome === "ok") { r.ok++; if (x.isJetstreamAck) r.acks++; else r.byFrom[x.from ?? "?"] = (r.byFrom[x.from ?? "?"] ?? 0) + 1; }
        else if (x.outcome === "timeout") r.timeouts++;
        else if (x.outcome === "no_responders") r.noResponders++;
        else r.other++;
      }
      return r;
    }

    case "publish_reply_to": { const a = actor(ctx, st.as); a.nc.publish(st.subject, enc.encode("{}"), { reply: st.reply }); await a.nc.flush(); return { outcome: "sent" }; }

    case "publish_no_reply": { actor(ctx, st.as).nc.publish(st.subject, enc.encode("{}")); await actor(ctx, st.as).nc.flush(); return { outcome: "sent" }; }

    case "listen": {
      const a = actor(ctx, st.as);
      const l = { subject: st.subject, got: [], error: null };
      const sub = a.nc.subscribe(st.subject);
      (async () => { try { for await (const m of sub) l.got.push({ subject: m.subject, body: dec.decode(m.data) }); } catch (e) { l.error = `${e.name}: ${e.message}`; } })();
      await a.nc.flush().catch(() => {});
      ctx.oyentes.set(st.name, l);
      return { outcome: "ok" };
    }
    case "heard": {
      await sleep(80);
      const l = ctx.oyentes.get(st.name);
      return { count: l.got.length, bodies: l.got.map((g) => g.body), subjects: l.got.map((g) => g.subject), error: l.error };
    }

    case "violations": { const a = actor(ctx, st.as); await sleep(60); return { count: a.violaciones.length, text: a.violaciones.join(" | ") }; }

    case "raw_request": {
      const z = crudo(ctx.srv.port, { user: st.user, headers: st.flags?.headers ?? true, noResponders: st.flags?.no_responders ?? true });
      const inbox = `_INBOX.${st.user ?? "anon"}.raw`;
      await sleep(80);
      z.sock.write(`SUB ${inbox} 1\r\nPUB ${st.subject} ${inbox} 2\r\n{}\r\nPING\r\n`);
      const tPub = z.ms();
      await sleep(st.waitMs ?? 600);
      z.sock.destroy();
      const msgs = z.frames.filter((f) => f.op === "MSG" || f.op === "HMSG");
      const primero = msgs[0];
      return { first: !primero ? "silence" : primero.status === 503 ? "status_503" : "msg", firstMs: primero ? primero.ms - tPub : undefined, firstStatus: primero?.status, msgCount: msgs.length, bodies: msgs.map((f) => f.body) };
    }

    case "raw_oversize": {
      const z = crudo(ctx.srv.port, { user: st.user });
      await sleep(100);
      const max = 1048576;
      z.sock.write(`PUB ${st.subject} ${max + 1}\r\n`);
      await sleep(400);
      const out = { serverError: z.serverError, closed: z.closed };
      z.sock.destroy();
      return out;
    }

    case "stream_add": {
      try {
        const s = await ctx.jsm.streams.add({ name: st.name, subjects: st.subjects, storage: "file", num_replicas: 1, ...(st.noAck ? { no_ack: true } : {}) });
        return { outcome: "ok", subjects: s.config.subjects };
      } catch (e) { return { outcome: "error", errCode: e?.api?.err_code ?? e?.code, text: e.message }; }
    }
    case "stream_state": { const i = await ctx.jsm.streams.info(st.name); return { messages: i.state.messages }; }

    case "stream_captures": {
      const nombre = `PROBE_${st.index}`;
      const crea = (noAck) => ctx.jsm.streams.add({ name: nombre, subjects: [st.pattern], storage: "memory", num_replicas: 1, ...(noAck ? { no_ack: true } : {}) });
      let creado = true, needsNoAck = false, errCode, text;
      try { await crea(false); } catch (e) {
        // JetStream solo deja crear sin acuse un stream que captura TODO o que se solapa con su propia API ($JS.API.>)
        if (/no-ack/.test(e.message)) { needsNoAck = true; try { await crea(true); } catch (e2) { creado = false; errCode = e2?.api?.err_code; text = e2.message; } }
        else { creado = false; errCode = e?.api?.err_code; text = e.message; }
      }
      let ack = false, capturesInterest = false, outcome = "n/a", reply;
      if (creado) {
        const x = await haz(ctx, ctx.ops, 0, { subject: st.probe, timeoutMs: 700 });
        outcome = x.outcome; reply = x.reply;
        ack = x.outcome === "ok" && x.isJetstreamAck === true;
        capturesInterest = x.outcome !== "no_responders";   // el stream cuenta como interés: sin `no responders`
        await ctx.jsm.streams.delete(nombre).catch(() => {});
      }
      return { created: creado, needsNoAck, errCode, text, ack, capturesInterest, predicted: solapa(st.pattern, st.probe), overlapsRpc: solapa(st.pattern, FORMA_RPC), outcome, reply };
    }

    case "rust_probe": {
      const lineas = await sondaDeRust(ctx.srv);
      const l = lineas.get(st.probe);
      if (!l) throw new Error(`la sonda de Rust no emitió «${st.probe}» (emitió: ${[...lineas.keys()].join(", ")})`);
      return l;
    }

    default: throw new Error(`operación desconocida «${st.op}»`);
  }
}

// ─── Ejecución de casos ──────────────────────────────────────────────────────────────────────────
async function limpia(ctx) {
  for (const r of ctx.resp.values()) { try { r.sock?.destroy(); } catch { /* ya no está */ } try { await r.nc?.close(); } catch { /* ya no está */ } }
  for (const a of ctx.actores.values()) { try { await a.nc.close(); } catch { /* ya no está */ } }
  ctx.resp.clear(); ctx.actores.clear(); ctx.pendientes.clear(); ctx.oyentes.clear();
  for (const n of await ctx.jsm.streams.list().next().then((l) => l.map((s) => s.config.name)).catch(() => [])) { try { await ctx.jsm.streams.delete(n); } catch { /* ya no está */ } }
  // un instante: el broker procesa los cierres de forma asíncrona, y el caso siguiente no debe verlos
  await sleep(120);
  if (MUTANT === "stream-captures-rpc") await ctx.jsm.streams.add({ name: "TRAMPA_MUTANTE", subjects: ["rpc.>"], storage: "memory", num_replicas: 1 });
}

async function ejecutaCaso(c, ctx) {
  await limpia(ctx);
  const fallos = [];
  const traza = [];
  for (const [i, st] of c.steps.entries()) {
    let obs;
    try { obs = await paso(ctx, st); } catch (e) { if (e instanceof Salto) throw e; fallos.push(`paso ${i + 1} (${st.op}): ${e.message}`); break; }
    traza.push({ i: i + 1, st, obs });
    for (const [ruta, esperado] of Object.entries(st.expect ?? {})) {
      const v = camino(obs, ruta);
      if (!cumple(v, esperado)) fallos.push(`paso ${i + 1} (${st.title ?? st.op}): «${ruta}» = ${JSON.stringify(v)}, se esperaba ${JSON.stringify(esperado)}${st.control ? "  [control]" : ""}`);
    }
  }
  return { fallos, traza };
}

function valida(fichero, d) {
  const conocidos = new Set(["id", "title", "spec", "covers", "rationale", "observed", "counterExample", "topology", "acl", "steps", "note", "decision"]);
  for (const c of d.cases) {
    for (const k of Object.keys(c)) if (!conocidos.has(k)) throw new Error(`${fichero}: el caso «${c.id}» tiene un campo desconocido «${k}»`);
    for (const k of ["spec", "rationale", "observed"]) if (!c[k]) throw new Error(`${fichero}: el caso «${c.id}» no tiene «${k}»: un caso sin norma, sin porqué o sin lo observado no es un caso`);
    if (!c.observed.server || !c.observed.date || !c.observed.result) throw new Error(`${fichero}: «${c.id}»: observed necesita server, date y result`);
    for (const st of c.steps) {
      const def = OPS[st.op];
      if (!def) throw new Error(`${fichero}: «${c.id}» usa una operación desconocida «${st.op}»`);
      const permitidos = new Set(["op", "expect", "title", "control", ...(def.args ?? []), ...(def.optional ?? [])]);
      for (const k of Object.keys(st)) if (!permitidos.has(k)) throw new Error(`${fichero}: «${c.id}», paso «${st.title ?? st.op}»: campo desconocido «${k}»`);
      for (const k of def.args ?? []) if (!(k in st)) throw new Error(`${fichero}: «${c.id}», paso «${st.title ?? st.op}»: falta «${k}»`);
      for (const ruta of Object.keys(st.expect ?? {})) {
        const raiz = ruta.split(".")[0];
        const observables = new Set((def.observes ?? []).map((o) => o.split(".")[0]));
        if (!observables.has(raiz)) throw new Error(`${fichero}: «${c.id}», paso «${st.title ?? st.op}»: la operación ${st.op} no observa «${raiz}» (observa: ${[...observables].join(", ")})`);
      }
    }
  }
}
valida("rpc-semantics.json", doc);

// ─── Principal ───────────────────────────────────────────────────────────────────────────────────
const resultados = [];
const todos = doc.cases.filter((c) => !ONLY || ONLY.includes(c.id));

let sinBinario = false;
try { const p = spawn(BIN, ["--version"], { stdio: "ignore" }); await new Promise((res, rej) => { p.on("exit", res); p.on("error", rej); }); } catch { sinBinario = true; }
if (sinBinario) {
  const m = `no se encuentra nats-server («${BIN}»). NATS_SERVER=/ruta/al/binario`;
  console.error(REQUIRE ? rojo(`✗ ${m}`) : gris(`- ${m}: los casos de broker NO se ejecutan (FLUX_REQUIRE_BROKER=1 lo convierte en fallo)`));
  process.exit(REQUIRE ? 1 : 0);
}

async function conTopologia(topologia, nombreAcl, fn) {
  const users = nombreAcl ? [{ name: "ops" }, ...doc.acls[nombreAcl].users] : null;
  const srv = topologia === "cluster3" ? await cluster3() : await servidorSuelto(users);
  const ops = await conecta(srv, nombreAcl ? { user: "ops" } : {});
  const ctx = { srv, ops, actores: new Map(), resp: new Map(), pendientes: new Map(), oyentes: new Map(), jsm: await jetstreamManager(ops.nc) };
  try { return await fn(ctx); } finally { await limpia(ctx).catch(() => {}); await ops.nc.close().catch(() => {}); }
}

try {
  // 1) un nodo, sin ACL
  const sueltos = todos.filter((c) => (c.topology ?? "single") === "single" && !c.acl);
  if (sueltos.length) {
    await conTopologia("single", null, async (ctx) => {
      console.log(`\n\x1b[1mpetición/respuesta — un nodo\x1b[0m  (11-request-reply.md)`);
      for (const c of sueltos) await informe(c, ctx);
    });
  }
  // 2) un servidor por cada conjunto de usuarios con permisos (`acls`)
  for (const nombreAcl of [...new Set(todos.filter((c) => c.acl).map((c) => c.acl))]) {
    if (!doc.acls[nombreAcl]) throw new Error(`rpc-semantics.json: acls no define «${nombreAcl}»`);
    await conTopologia("single", nombreAcl, async (ctx) => {
      console.log(`\n\x1b[1mpetición/respuesta — ACL «${nombreAcl}»\x1b[0m  ${gris(doc.acls[nombreAcl].title ?? "")}`);
      for (const c of todos.filter((c) => c.acl === nombreAcl)) await informe(c, ctx);
    });
  }
  // 3) el cluster
  const enCluster = todos.filter((c) => c.topology === "cluster3");
  if (enCluster.length && !flag("--no-cluster")) {
    await conTopologia("cluster3", null, async (ctx) => {
      console.log(`\n\x1b[1mpetición/respuesta — cluster de 3 nodos\x1b[0m`);
      for (const c of enCluster) await informe(c, ctx);
    });
  } else if (enCluster.length) {
    for (const c of enCluster) console.log(gris(`  - ${c.id}: saltado (--no-cluster)`));
  }
} finally {
  for (const p of hijos) { try { p.kill("SIGTERM"); } catch { /* ya no está */ } }
  await sleep(300);
  rmSync(tmp, { recursive: true, force: true });
}

async function informe(c, ctx) {
  let r;
  try { r = await ejecutaCaso(c, ctx); } catch (e) {
    if (e instanceof Salto) {
      resultados.push({ id: c.id, ok: !REQUIRE, saltado: true });
      console.log(`  ${REQUIRE ? rojo("✗") : gris("-")} ${c.id.padEnd(58)} ${gris(`saltado: ${e.message}${REQUIRE ? " (FLUX_REQUIRE_BROKER=1: es un fallo)" : ""}`)}`);
      return;
    }
    r = { fallos: [`excepción: ${e.stack ?? e.message}`], traza: [] };
  }
  const ok = r.fallos.length === 0;
  resultados.push({ id: c.id, ok });
  console.log(`  ${ok ? verde("✓") : rojo("✗")} ${c.id.padEnd(58)} ${gris((c.covers ?? []).join(","))}`);
  if (!ok) for (const f of r.fallos) console.log(rojo(`      ${f}`));
  if (VERBOSE || !ok) for (const t of r.traza) console.log(gris(`      ${t.i}. ${t.st.title ?? t.st.op} → ${JSON.stringify(t.obs).slice(0, 300)}`));
}

// ─── Los ejemplos byte a byte de 11-request-reply.md §3.5 y los vectores ─────────────────────────────
if (!ONLY) {
  console.log(`\n\x1b[1mcross-sdk-rpc-envelope\x1b[0m  (11-request-reply.md §3.5)`);
  const ORDEN = envelope.attributeOrder;
  const rpc = protocol.rpc;
  const porId = Object.fromEntries(envelope.fixtures.map((f) => [f.id, f]));
  for (const f of envelope.fixtures) {
    const fallos = [];
    const s = JSON.stringify(f.event);
    const bytes = Buffer.from(s, "utf8");
    if (bytes.length !== f.bytes) fallos.push(`bytes: ${bytes.length}, se esperaban ${f.bytes}`);
    const sha = createHash("sha256").update(bytes).digest("hex");
    if (sha !== f.sha256) fallos.push(`sha256: ${sha}, se esperaba ${f.sha256}`);
    if (/\\u[0-9a-fA-F]{4}/.test(s)) fallos.push("lleva un escape \\u: debe ser UTF-8 literal");
    const idx = Object.keys(f.event).map((k) => ORDEN.indexOf(k));
    if (!idx.every((v, i) => v >= 0 && (i === 0 || v > idx[i - 1]))) fallos.push("las claves no siguen el orden de declaración con data al final");
    if (Object.keys(f.event).at(-1) !== "data") fallos.push("data no es el último atributo");
    // el tipo y el esquema, contra los patrones de protocol.json
    const e = f.event;
    const esError = f.id.startsWith("rpc-error");
    const esPeticion = f.id === "rpc-request";
    if (esError) {
      if (e.type !== rpc.type.error.value) fallos.push(`type ${e.type}: el del error es ${rpc.type.error.value}`);
      if (e.dataschema !== rpc.dataschema.error) fallos.push("dataschema del error distinto del fijo");
      if (!rpc.error.classes.includes(e.data.class)) fallos.push(`class «${e.data.class}» fuera de ${rpc.error.classes}`);
      if (!new RegExp(protocol.errors.codePattern).test(e.data.code)) fallos.push("code no cumple errors.codePattern");
      const ks = Object.keys(e.data);
      if (ks.join() !== rpc.error.dataKeys.filter((k) => ks.includes(k)).join()) fallos.push(`las claves de data (${ks}) no siguen ${rpc.error.dataKeys}`);
      if (e.dataclassification !== rpc.error.dataclassification) fallos.push("el error va siempre `internal`");
    } else {
      const patType = esPeticion ? rpc.type.request.pattern : rpc.type.reply.pattern;
      const patSchema = esPeticion ? rpc.dataschema.request.pattern : rpc.dataschema.reply.pattern;
      if (!new RegExp(patType).test(e.type)) fallos.push(`type ${e.type} no cumple ${patType}`);
      if (!new RegExp(patSchema).test(e.dataschema)) fallos.push(`dataschema ${e.dataschema} no cumple ${patSchema}`);
    }
    if (esPeticion) {
      if (!new RegExp(rpc.subject.pattern).test(f.natsSubject)) fallos.push("el subject de NATS no cumple rpc.subject.pattern");
      if (e.correlationid !== e.id) fallos.push("una petición que no nace de otro evento lleva correlationid = su propio id");
      if ("causationid" in e) fallos.push("una petición que no nace de otro evento no lleva causationid");
    } else {
      const q = porId["rpc-request"].event;
      if (e.causationid !== q.id) fallos.push("causationid no es el id de la petición");
      for (const k of ["correlationid", "tenantid", "subject", "partitionkey"]) if (e[k] !== q[k]) fallos.push(`${k} no se hereda de la petición`);
      if (e.id === q.id) fallos.push("la respuesta lleva el id de la petición");
    }
    resultados.push({ id: `envelope:${f.id}`, ok: fallos.length === 0 });
    console.log(`  ${fallos.length ? rojo("✗") : verde("✓")} ${f.id.padEnd(58)} ${gris(`${bytes.length} bytes  sha256 ${sha.slice(0, 12)}…`)}`);
    for (const x of fallos) console.log(rojo(`      ${x}`));
  }

  // El código de cada vector de la operación, derivado de protocol.json (que es la fuente).
  const derivado = (operacion) => {
    const dom = operacion.split(".")[0];
    if (protocol.naming.reservedDomains.names.includes(dom)) return { code: "RESERVED_DOMAIN" };
    if (!new RegExp(rpc.operation.pattern).test(operacion)) return { code: "INVALID_RPC_CALL" };
    const sub = `rpc.${operacion}`;
    return new RegExp(rpc.subject.pattern).test(sub) ? { subject: sub } : { code: "SUBJECT_DOES_NOT_MATCH_PATTERN" };
  };
  const malos = [];
  for (const v of envelope.subjectVectors) {
    const d = derivado(v.operation);
    const ok = v.natsSubject ? d.subject === v.natsSubject : d.code === v.errorCode;
    if (!ok) malos.push(`${JSON.stringify(v)} → ${JSON.stringify(d)}`);
  }
  resultados.push({ id: "envelope:subject-vectors", ok: malos.length === 0 });
  console.log(`  ${malos.length ? rojo("✗") : verde("✓")} ${`subject-vectors (${envelope.subjectVectors.length})`.padEnd(58)} ${gris("coinciden con los patrones de protocol.json")}`);
  for (const m of malos) console.log(rojo(`      ${m}`));

  // El rango del timeout, derivado de protocol.json.
  const efectivo = (t) => {
    if (t === undefined) return { ms: rpc.timeout.defaultMs };
    if (typeof t !== "number" || !Number.isInteger(t) || t < rpc.timeout.minMs || t > rpc.timeout.maxMs) return { code: rpc.timeout.outOfRangeCode };
    return { ms: t };
  };
  const malosT = [];
  for (const v of envelope.timeoutVectors) {
    const d = efectivo(v.timeoutMs === "NaN" ? NaN : v.timeoutMs);
    const ok = v.effectiveMs !== undefined ? d.ms === v.effectiveMs : d.code === v.errorCode;
    if (!ok) malosT.push(`${JSON.stringify(v)} → ${JSON.stringify(d)}`);
  }
  resultados.push({ id: "envelope:timeout-vectors", ok: malosT.length === 0 });
  console.log(`  ${malosT.length ? rojo("✗") : verde("✓")} ${`timeout-vectors (${envelope.timeoutVectors.length})`.padEnd(58)} ${gris("coinciden con rpc.timeout")}`);
  for (const m of malosT) console.log(rojo(`      ${m}`));

  // en los dos sentidos: una mutación DEBE cambiar el hash
  const f0 = envelope.fixtures[0];
  const roto = JSON.stringify({ ...f0.event, tenantid: "ACME" });
  const okControl = createHash("sha256").update(Buffer.from(roto, "utf8")).digest("hex") !== f0.sha256;
  resultados.push({ id: "envelope:control", ok: okControl });
  console.log(`  ${okControl ? verde("✓") : rojo("✗")} ${"control: otro tenantid cambia el hash".padEnd(58)}`);
}

const mal = resultados.filter((r) => !r.ok);
const saltados = resultados.filter((r) => r.saltado).length;
console.log(`\n${mal.length ? rojo(`✗ ${mal.length} de ${resultados.length} casos fallan: ${mal.map((m) => m.id).join(", ")}`) : verde(`✓ ${resultados.length - saltados} casos de petición/respuesta${saltados ? gris(` (${saltados} saltados)`) : ""}`)}${MUTANT ? gris(`  [mutante: ${MUTANT}]`) : ""}`);
process.exit(mal.length ? 1 : 0);
