#!/usr/bin/env node
/**
 * Runner de los casos de ESTADO REPLICADO — specification/10-state.md.
 *
 *   NATS_SERVER=/ruta/a/nats-server node conformance/state-broker.mjs
 *   node conformance/state-broker.mjs --only one-value-per-key,conditional-write -v
 *   node conformance/state-broker.mjs --list
 *
 * Como `security.mjs`, **los casos son DATOS y el runner los ejecuta**: no sabe qué es «un valor por
 * clave», solo ejecuta el vocabulario de `cases/state-broker.json` y compara lo observado con
 * `expect`. Y, como `run.mjs`, habla NATS directamente, sin ningún SDK de flux: un runner construido
 * sobre un SDK heredaría las suposiciones que debe verificar.
 *
 * Ejecuta DOS ficheros:
 *
 *   cases/state-broker.json      Lo que hace el BROKER (A-31…A-49): un stream de estado, el chequeo de
 *                                secuencia, la lectura directa, la foto de un consumidor, los permisos
 *                                por tenant y un cluster de 3 nodos.
 *   cases/state-semantics.json   Lo que hace el PROTOCOLO encima (A-50): el algoritmo de `put`/`delete`
 *                                de 10-state.md §3.2 y la foto de `watch` de §5.5, escritos aquí como
 *                                implementación de REFERENCIA sobre NATS crudo. Los SDKs de Node y Rust
 *                                ejecutan estos mismos escenarios con su API: el resultado esperado es
 *                                el de aquí.
 *
 * Y comprueba `cases/cross-sdk-state-envelope.json`: reconstruye los bytes de los ejemplos de §2.6
 * desde sus datos y compara su SHA-256, sin fiarse del texto de la spec.
 *
 * Servidores: levanta y para los suyos (uno suelto, uno con la ACL de cada caso que la pide, y un
 * cluster de tres nodos para A-49). Puertos: `FLUX_TEST_PORTS=desde-hasta` (por defecto 4600-4649) —
 * se usan hasta 8—. `FLUX_REQUIRE_BROKER=1`: que falte `nats-server` es un FALLO, no un salto.
 * Los casos de cluster se saltan con `--no-cluster`.
 */

import { readFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, existsSync } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { connect, headers as natsHeaders, nanos } from "@nats-io/transport-node";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";

const HERE = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const flag = (f) => args.includes(f);
const opt = (f) => (args.includes(f) ? args[args.indexOf(f) + 1] : undefined);
const VERBOSE = flag("-v");
/**
 * `--mutant <nombre>`: estropea A PROPÓSITO la configuración del stream o la implementación de referencia
 * para demostrar que los casos DETECTAN el fallo (una comprobación que solo se ha visto en verde no
 * demuestra nada). Con un mutante, el runner debe FALLAR; `--mutant` sin fallos es lo preocupante.
 *   no-max-msgs-per-subject · no-allow-direct · discard-new · only-10071 · no-own-write-check ·
 *   equal-version-applies · publish-via-core · fresh-id-per-attempt · no-cas-header
 */
const MUTANT = opt("--mutant");
const ONLY = opt("--only")?.split(",");
const REQUIRE = process.env.FLUX_REQUIRE_BROKER === "1";
const [PORT0, PORT1] = (process.env.FLUX_TEST_PORTS ?? "4600-4649").split("-").map(Number);
const BIN = process.env.NATS_SERVER ?? "nats-server";

const enc = new TextEncoder();
const dec = new TextDecoder();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const verde = (s) => `\x1b[32m${s}\x1b[0m`;
const rojo = (s) => `\x1b[31m${s}\x1b[0m`;
const gris = (s) => `\x1b[90m${s}\x1b[0m`;

// ─── Carga de los casos ──────────────────────────────────────────────────────────────────────────
const leer = async (f) => JSON.parse(await readFile(join(HERE, "cases", f), "utf8"));
const broker = await leer("state-broker.json");
const semantica = await leer("state-semantics.json");
const envelope = await leer("cross-sdk-state-envelope.json");

if (flag("--list")) {
  for (const c of [...broker.cases, ...semantica.cases]) console.log(`${c.id.padEnd(44)} ${(c.covers ?? []).join(",").padEnd(12)} ${c.title}`);
  process.exit(0);
}

// ─── Servidores ──────────────────────────────────────────────────────────────────────────────────
const tmp = mkdtempSync(join(tmpdir(), "flux-state-"));
let nextPort = PORT0;
const hijos = [];
const puerto = () => {
  if (nextPort > PORT1) throw new Error(`se acabaron los puertos de FLUX_TEST_PORTS (${PORT0}-${PORT1})`);
  return nextPort++;
};

async function esperaPuerto(port, ms = 8000) {
  const { createConnection } = await import("node:net");
  const fin = Date.now() + ms;
  while (Date.now() < fin) {
    const ok = await new Promise((res) => { const s = createConnection(port, "127.0.0.1"); s.on("connect", () => { s.destroy(); res(true); }); s.on("error", () => res(false)); });
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

async function servidorSuelto(users) {
  const port = puerto();
  const auth = users ? `authorization { users: [ ${users.map((u) => `{ user: "${u.name}", password: "pw"${u.permissions ? `, permissions: ${permisos(u.permissions)}` : ""} }`).join(", ")} ] }` : "";
  arranca(`n${port}`, (dir) => `port: ${port}\nhost: 127.0.0.1\njetstream { store_dir: "${join(dir, "js")}" }\n${auth}\n`);
  await esperaPuerto(port);
  return { servers: [`127.0.0.1:${port}`], port };
}
const lista = (a) => `[${a.map((x) => JSON.stringify(x)).join(", ")}]`;
const permisos = (p) => `{ publish: { allow: ${lista(p.publish)} }, subscribe: { allow: ${lista(p.subscribe)} } }`;

async function cluster3() {
  const nodos = [0, 1, 2].map(() => ({ client: puerto(), cluster: puerto() }));
  nodos.forEach((n, i) => {
    const routes = nodos.filter((_, j) => j !== i).map((m) => `nats://127.0.0.1:${m.cluster}`).join(", ");
    arranca(`c${i}`, (dir) => `server_name: n${i}\nport: ${n.client}\nhost: 127.0.0.1\njetstream { store_dir: "${join(dir, "js")}" }\ncluster { name: fluxtest\n listen: 127.0.0.1:${n.cluster}\n routes: [${routes}]\n}\n`);
  });
  for (const n of nodos) await esperaPuerto(n.client);
  await sleep(3500); // elección de líder de la capa de metadatos
  return { servers: nodos.map((n) => `127.0.0.1:${n.client}`) };
}

// ─── Actores ─────────────────────────────────────────────────────────────────────────────────────
/** Una conexión con su contador de violaciones de permiso (el broker avisa aparte: A-28). */
async function conecta(servers, { user, inboxPrefix } = {}) {
  const nc = await connect({ servers, ...(user ? { user, pass: "pw" } : {}), ...(inboxPrefix ? { inboxPrefix } : {}) });
  const a = { nc, js: jetstream(nc), violaciones: [] };
  (async () => { for await (const s of nc.status()) { const m = s.error?.message ?? ""; if (/ermission/.test(m)) a.violaciones.push(m); } })();
  return a;
}

// ─── Lenguaje de expectativas ────────────────────────────────────────────────────────────────────
const camino = (o, ruta) => ruta.split(".").reduce((x, k) => (x == null ? undefined : x[k]), o);
function cumple(valor, esperado) {
  if (esperado !== null && typeof esperado === "object" && !Array.isArray(esperado)) {
    if ("match" in esperado) return typeof valor === "string" && new RegExp(esperado.match).test(valor);
    if ("includes" in esperado) return Array.isArray(valor) ? valor.includes(esperado.includes) : String(valor).includes(esperado.includes);
    if ("gte" in esperado) return valor >= esperado.gte;
    if ("lte" in esperado) return valor <= esperado.lte;
    if ("not" in esperado) return !cumple(valor, esperado.not);
    if ("present" in esperado) return (valor !== undefined && valor !== null) === esperado.present;
    if ("oneOf" in esperado) return esperado.oneOf.some((e) => cumple(valor, e));
  }
  return JSON.stringify(valor) === JSON.stringify(esperado);
}

// ─── Referencias entre pasos: { "ref": "nombre", "plus": n } ──────────────────────────────────────
const resuelve = (v, mem) => {
  if (Array.isArray(v)) return v.map((x) => resuelve(x, mem));
  if (v && typeof v === "object") {
    if ("ref" in v) { if (!(v.ref in mem)) throw new Error(`referencia «${v.ref}» sin guardar antes`); return mem[v.ref] + (v.plus ?? 0); }
    return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, resuelve(x, mem)]));
  }
  if (typeof v === "string") return v.replace(/\{(repeat):([^:}]+):(\d+)\}/g, (_, __, s, n) => s.repeat(Number(n)));
  return v;
};

// ─── El vocabulario ──────────────────────────────────────────────────────────────────────────────
const nombre = (e) => e?.api?.err_code ?? e?.code;
const esNoResponders = (e) => /no responders|jetstream is not enabled|503/i.test(e?.message ?? "") || e?.code === 503;

/** Clasifica un fallo de una operación contra una ACL. */
async function clasifica(actor, antes, fn) {
  let r;
  try { r = await fn(); } catch (e) {
    const t = `${e.name}: ${e.message}`;
    await sleep(60);
    if (actor.violaciones.length > antes || /ermission/.test(t)) return { outcome: "denied", text: actor.violaciones.at(-1) ?? t };
    if (esNoResponders(e)) return { outcome: "no_responders", text: t };
    if (/timeout/i.test(e.message)) return { outcome: "timeout", text: t };
    return { outcome: "error", errCode: nombre(e), text: e.message };
  }
  await sleep(40);
  if (actor.violaciones.length > antes) return { outcome: "denied", text: actor.violaciones.at(-1) };
  return r;
}

function cab(h) { const o = {}; if (h) for (const k of h.keys()) o[k] = h.get(k); return o; }

// El vocabulario vive en los DATOS (cases/state-broker.json → vocabulary.ops): qué hace cada operación, qué
// argumentos lleva y qué observa. Aquí solo está cómo se ejecuta.
const OPS = broker.vocabulary.ops;

const canon = () => ({ ack_policy: "explicit", ack_wait: nanos(30_000), max_deliver: 6, backoff: [30_000, 60_000, 300_000, 900_000, 1_800_000].map(nanos), max_ack_pending: 256, replay_policy: "instant" });
const estadoBase = (name, subjects, extra = {}) => {
  const c = { name, subjects, storage: "file", retention: "limits", max_msgs_per_subject: 1, allow_direct: true, max_age: 0, discard: "old", duplicate_window: nanos(120_000), num_replicas: 1, ...extra };
  if (MUTANT === "no-max-msgs-per-subject") delete c.max_msgs_per_subject;
  if (MUTANT === "no-allow-direct") c.allow_direct = false;
  if (MUTANT === "discard-new") { c.discard = "new"; c.discard_new_per_subject = true; }
  return c;
};

// ─── La implementación de REFERENCIA (10-state.md §3.2) ──────────────────────────────────────────
const CAS = MUTANT === "only-10071" ? new Set([10071]) : new Set([10071, 10164]);
const subjectDe = (c, tenant, key) => { const [d, a, v] = c.split("."); return `state.${d}.${a}.${v}.${tenant}.${key}`; };
const streamDe = (c) => `STATE_${c.split(".")[0].toUpperCase().replace(/-/g, "_")}`;
const KEY = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;

async function leeEstado(nc, stream, subject) {
  const r = await nc.request(`$JS.API.DIRECT.GET.${stream}.${subject}`, "", { timeout: 2000 });
  const h = r.headers;
  if (h?.code === 404) return null;
  if (h?.hasError) throw Object.assign(new Error(`direct get: ${h.code} ${h.description}`), { code: h.code });
  let ev;
  try { ev = JSON.parse(dec.decode(r.data)); } catch { return { seq: Number(h.get("Nats-Sequence")), unreadable: true }; }
  const v = ev?.data?.aggregateVersion;
  return { seq: Number(h.get("Nats-Sequence")), eventId: ev.id, version: Number.isSafeInteger(v) && v >= 1 ? v : undefined, deleted: /\.deleted\.v[1-9][0-9]*$/.test(ev.type ?? ""), event: ev };
}

function eventoDe({ tipo, contract, tenant, key, version, id, classification }) {
  const [dom, agr, v] = contract.split(".");
  return {
    specversion: "1.0", id, source: "/produccion/crm-api",
    type: tipo === "put" ? `com.flux.state.${dom}.${agr}.${v}` : `com.flux.state.${dom}.${agr}.deleted.${v}`,
    time: "2026-08-20T10:25:39.412Z", datacontenttype: "application/json",
    dataschema: tipo === "put" ? `https://schemas.internal/state/${dom}/${agr}/1.0.0.json` : "https://schemas.internal/system/state/eliminado/1.0.0.json",
    subject: key, correlationid: id, tenantid: tenant, producerversion: "2.1.0",
    dataclassification: tipo === "put" ? (classification ?? "confidential") : "internal", partitionkey: key,
    data: tipo === "put" ? { clienteId: key, aggregateVersion: version } : { aggregateVersion: version },
  };
}

let seqId = 0;
const nuevoId = () => `01924f8e-7c3a-7b2d-9e14-${String(++seqId).padStart(12, "0")}`;

async function escribe(actor, tipo, p, hook) {
  const stream = streamDe(p.contract);
  const subject = subjectDe(p.contract, p.tenant, p.key);
  if (!KEY.test(p.key)) return { outcome: "error", errCode: "INVALID_STATE_KEY", text: `clave «${p.key}»` };
  if (!Number.isSafeInteger(p.version) || p.version < 1) return { outcome: "error", errCode: "MISSING_AGGREGATE_VERSION", text: `versión ${p.version}` };
  const id = p.id ?? nuevoId();
  const bytes = enc.encode(JSON.stringify(eventoDe({ tipo, ...p, id })));   // UNA vez: mismos bytes y mismo id en todos los intentos
  for (let intento = 1; intento <= 3; intento++) {
    const actual = await leeEstado(actor.nc, stream, subject);
    const S = actual?.seq ?? 0;
    if (MUTANT !== "no-own-write-check" && actual && !actual.unreadable && actual.eventId === id) return { outcome: "ok", applied: true, result: "ok", revision: actual.seq, attempts: intento, ownWrite: true };
    if (actual?.version !== undefined && (MUTANT === "equal-version-applies" ? p.version < actual.version : p.version <= actual.version)) return { outcome: "stale", applied: false, result: "stale", revision: actual.seq, attempts: intento };
    if (hook?.beforePublish) await rivales(actor, hook.beforePublish, intento, p);
    const h = natsHeaders(); h.set("Nats-Msg-Id", id); if (MUTANT !== "no-cas-header") h.set("Nats-Expected-Last-Subject-Sequence", String(S));
    try {
      if (MUTANT === "publish-via-core") { actor.nc.publish(subject, bytes); await actor.nc.flush(); return { outcome: "ok", applied: true, result: "ok", revision: 0, attempts: intento }; }
      const ack = await actor.js.publish(subject, MUTANT === "fresh-id-per-attempt" ? enc.encode(JSON.stringify({ ...JSON.parse(dec.decode(bytes)), id: nuevoId() })) : bytes, { headers: MUTANT === "fresh-id-per-attempt" ? (() => { const h2 = natsHeaders(); h2.set("Nats-Msg-Id", nuevoId()); h2.set("Nats-Expected-Last-Subject-Sequence", String(S)); return h2; })() : h });
      // Acuse perdido: el publish SE APLICÓ, pero quien lo hizo ve el error del REENVÍO de la librería de
      // NATS tras una reconexión (el servidor, con la S ya vieja, contesta «wrong last sequence»).
      if (hook?.lostAckOnce && intento === 1) throw Object.assign(new Error("wrong last sequence: (simulado)"), { code: 10071 });
      return { outcome: "ok", applied: true, result: "ok", revision: ack.seq, attempts: intento };
    } catch (e) {
      if (CAS.has(nombre(e))) continue;
      return { outcome: "error", errCode: nombre(e), text: e.message };
    }
  }
  return { outcome: "error", errCode: "STATE_CONFLICT", text: "el chequeo de secuencia agotó sus 3 intentos" };
}
async function rivales(actor, lista, intento, p) {
  const r = lista.find((x) => x.attempt === intento);
  if (!r) return;
  for (let v = r.fromVersion; v < r.fromVersion + (r.count ?? 1); v++) {
    await escribe(actor, "put", { contract: p.contract, tenant: p.tenant, key: p.key, version: v });
  }
}

// ─── Ejecutor de un paso ─────────────────────────────────────────────────────────────────────────
async function paso(ctx, st, mem) {
  const actor = st.as && st.as !== "ops" ? ctx.actores[st.as] : ctx.ops;
  if (!actor) throw new Error(`actor «${st.as}» sin declarar en acl.users`);
  const jsm = ctx.jsm;
  const a = resuelve(st, mem);
  const antes = actor.violaciones.length;

  switch (a.op) {
    case "stream_add": {
      try {
        const cfg = a.state ? estadoBase(a.name, a.subjects, a.config) : { name: a.name, subjects: a.subjects, storage: "file", num_replicas: 1, ...(a.config ?? {}) };
        const s = await jsm.streams.add(cfg);
        return { outcome: "ok", config: s.config, leader: s.cluster?.leader };
      } catch (e) { return { outcome: "error", errCode: nombre(e), text: e.message }; }
    }
    case "stream_update": {
      try {
        const i = await jsm.streams.info(a.name);
        const u = await jsm.streams.update(a.name, { ...i.config, subjects: a.subjects });
        return { outcome: "ok", subjects: u.config.subjects };
      } catch (e) { return { outcome: "error", errCode: nombre(e), text: e.message }; }
    }
    case "stream_state": {
      const i = await jsm.streams.info(a.name);
      return { messages: i.state.messages, numSubjects: i.state.num_subjects, firstSeq: i.state.first_seq, lastSeq: i.state.last_seq };
    }
    case "stream_subjects": {
      return clasifica(actor, antes, async () => {
        const r = JSON.parse(dec.decode((await actor.nc.request(`$JS.API.STREAM.INFO.${a.name}`, JSON.stringify({ subjects_filter: a.filter }), { timeout: 2000 })).data));
        const subs = r.state?.subjects ?? {};
        return { outcome: "answered", subjects: Object.keys(subs).sort(), count: Object.keys(subs).length };
      });
    }
    case "js_publish": {
      return clasifica(actor, antes, async () => {
        const h = natsHeaders();
        if (a.msgId) h.set("Nats-Msg-Id", a.msgId);
        if (a.expectedLastSubjectSeq !== undefined) h.set("Nats-Expected-Last-Subject-Sequence", String(a.expectedLastSubjectSeq));
        try {
          const ack = await actor.js.publish(a.subject, enc.encode(a.data ?? "{}"), { headers: h, timeout: 2500 });
          return { outcome: "ok", stream: ack.stream, seq: ack.seq, duplicate: ack.duplicate };
        } catch (e) {
          if (/ermission/.test(`${e.message} ${e.cause?.message ?? ""}`)) throw e;
          if (esNoResponders(e)) return { outcome: "no_responders", text: e.message };
          if (e.name === "InvalidSubjectError") return { outcome: "error", errCode: "illegal_subject", text: e.message };
          return { outcome: "error", errCode: nombre(e), text: e.message };
        }
      });
    }
    case "core_publish": {
      return clasifica(actor, antes, async () => { actor.nc.publish(a.subject, enc.encode(a.data ?? "{}")); await actor.nc.flush(); return { outcome: "sent" }; });
    }
    case "direct_get":
    case "direct_get_body": {
      const subj = a.op === "direct_get" ? `$JS.API.DIRECT.GET.${a.stream}.${a.subject}` : `$JS.API.DIRECT.GET.${a.stream}`;
      const cuerpo = a.op === "direct_get" ? (a.body === undefined ? "" : JSON.stringify(a.body)) : JSON.stringify(a.request);
      return clasifica(actor, antes, async () => {
        const r = await actor.nc.request(subj, cuerpo, { timeout: 2000 });
        const code = r.headers?.code ?? 0;
        if (code === 404) return { outcome: "not_found", status: 404, text: r.headers.description };
        if (code === 408 || code === 400) return { outcome: "bad_request", status: code, text: r.headers.description };
        const h = cab(r.headers);
        return { outcome: "found", status: 200, seq: Number(h["Nats-Sequence"]), subject: h["Nats-Subject"], data: dec.decode(r.data), header: h, headerNames: Object.keys(h).sort() };
      });
    }
    case "api_request": {
      return clasifica(actor, antes, async () => {
        const r = await actor.nc.request(a.subject, a.body === undefined ? "" : JSON.stringify(a.body), { timeout: 2000 });
        return { outcome: "answered", reply: JSON.parse(dec.decode(r.data)) };
      });
    }
    case "consumer_add": {
      try {
        const c = await jsm.consumers.add(a.stream, { durable_name: a.durable, filter_subject: a.filter, deliver_policy: a.deliver ?? "all", ...(a.replicas ? { num_replicas: a.replicas } : {}), ...canon() });
        return { outcome: "ok", numPending: c.num_pending, deliverPolicy: c.config.deliver_policy, numReplicas: c.config.num_replicas };
      } catch (e) { return { outcome: "error", errCode: nombre(e), text: e.message }; }
    }
    case "consumer_delete": { await jsm.consumers.delete(a.stream, a.durable); return { outcome: "ok" }; }
    case "consumer_info": {
      // Una petición cruda a CONSUMER.INFO, como el actor del paso (con ACL, el broker puede denegarla).
      return clasifica(actor, antes, async () => {
        const r = JSON.parse(dec.decode((await actor.nc.request(`$JS.API.CONSUMER.INFO.${a.stream}.${a.durable}`, "", { timeout: 2000 })).data));
        if (r.error) return { outcome: "error", errCode: r.error.err_code, text: r.error.description };
        return { outcome: "ok", numPending: r.num_pending, numAckPending: r.num_ack_pending, numRedelivered: r.num_redelivered, deliveredStreamSeq: r.delivered.stream_seq, ackFloorStreamSeq: r.ack_floor.stream_seq };
      });
    }
    case "pull": return drena(actor, a.stream, a.durable, a.max ?? 100, a.then ?? "ack");
    case "pull_shared": {
      const c1 = await ctx.ops.js.consumers.get(a.stream, a.durable), c2 = await ctx.ops.js.consumers.get(a.stream, a.durable);
      const tomar = async (c) => { const r = []; const it = await c.fetch({ max_messages: a.perClient, expires: 1000 }); for await (const m of it) { r.push(JSON.parse(dec.decode(m.data)).i); m.ack(); } return r.sort((x, y) => x - y); };
      const [x, y] = await Promise.all([tomar(c1), tomar(c2)]);
      return { a: x, b: y, disjoint: x.every((v) => !y.includes(v)), total: x.length + y.length };
    }
    case "cas_race": {
      const ej = { accepted: 0, perRound: [], errCodes: new Set(), errMessages: new Set() };
      for (let r = 0; r < a.rounds; r++) {
        let S = 0;
        const g = await actor.nc.request(`$JS.API.DIRECT.GET.${a.stream}.${a.subject}`, "", { timeout: 2000 });
        if (g.headers?.code !== 404) S = Number(g.headers.get("Nats-Sequence"));
        const res = await Promise.allSettled(Array.from({ length: a.writers }, (_, i) => {
          const h = natsHeaders(); h.set("Nats-Expected-Last-Subject-Sequence", String(S)); h.set("Nats-Msg-Id", `r${r}-${i}-${Math.random()}`);
          return actor.js.publish(a.subject, enc.encode(`{"r":${r},"i":${i}}`), { headers: h });
        }));
        const ok = res.filter((x) => x.status === "fulfilled").length;
        ej.accepted += ok; ej.perRound.push(ok);
        for (const x of res) if (x.status === "rejected") { ej.errCodes.add(nombre(x.reason)); ej.errMessages.add(x.reason.message.replace(/\d+/g, "N")); }
      }
      return { accepted: ej.accepted, perRound: [...new Set(ej.perRound)], errCodes: [...ej.errCodes].sort((x, y) => x - y), errMessages: [...ej.errMessages] };
    }
    case "state_put": return escribe(actor, "put", a, a.hook);
    case "state_delete": return escribe(actor, "delete", a, a.hook);
    case "state_get": {
      const e = await leeEstado(actor.nc, streamDe(a.contract), subjectDe(a.contract, a.tenant, a.key));
      if (!e || e.deleted) return { entry: null, isNull: true, ...(e ? { revision: e.seq, aggregateVersion: e.version, deleted: true } : {}) };
      return { entry: { key: a.key, tenantId: a.tenant, deleted: false, data: e.event.data, aggregateVersion: e.version, revision: e.seq }, isNull: false, revision: e.seq, aggregateVersion: e.version, deleted: false };
    }
    case "state_watch": return vigila(ctx, a);
    case "state_race": {
      const res = { ok: 0, stale: 0, conflict: 0, other: 0 };
      const tareas = [...Array(a.instances).keys()].map(async (inst) => {
        for (let v = a.from; v <= a.to; v++) {
          if (!(v % a.instances === inst || Math.random() < 0.3)) continue;
          await sleep(Math.random() * 4);
          const r = await escribe(actor, "put", { contract: a.contract, tenant: a.tenant, key: a.key, version: v });
          if (r.outcome === "ok") res.ok++; else if (r.outcome === "stale") res.stale++; else if (r.errCode === "STATE_CONFLICT") res.conflict++; else res.other++;
        }
      });
      await Promise.all(tareas);
      // un conflicto es RETRYABLE: la aplicación reintenta hasta que no lo haya
      let fin = await leeEstado(actor.nc, streamDe(a.contract), subjectDe(a.contract, a.tenant, a.key));
      for (let i = 0; i < 10 && fin?.version !== a.to; i++) { await escribe(actor, "put", { contract: a.contract, tenant: a.tenant, key: a.key, version: a.to }); fin = await leeEstado(actor.nc, streamDe(a.contract), subjectDe(a.contract, a.tenant, a.key)); }
      return { finalVersion: fin?.version, ...res };
    }
    case "violations": return { count: actor.violaciones.length, text: actor.violaciones.join(" | ") };
    default: throw new Error(`operación desconocida «${a.op}»`);
  }
}

async function drena(actor, stream, durable, max, entonces) {
  const c = await actor.js.consumers.get(stream, durable);
  const got = [];
  const it = await c.fetch({ max_messages: max, expires: 1000 });
  for await (const m of it) {
    got.push({ subject: m.subject, seq: m.info.streamSequence, pending: m.info.pending, body: dec.decode(m.data) });
    if (entonces === "ack") m.ack(); else if (entonces === "nak") m.nak();
  }
  return {
    count: got.length, keys: got.map((g) => g.subject.split(".").pop()), seqs: got.map((g) => g.seq), pendings: got.map((g) => g.pending),
    bodies: got.map((g) => g.body), lastPending: got.at(-1)?.pending, firstPendingZeroIndex: got.findIndex((g) => g.pending === 0),
  };
}

/** `watch` de REFERENCIA: el durable, la foto y el momento de `onSynced` (10-state.md §5.2, §5.5). */
async function vigila(ctx, a) {
  const stream = streamDe(a.contract);
  const [d, ag, v] = a.contract.split(".");
  const filtro = `state.${d}.${ag}.${v}.${a.tenant ?? "*"}.*`;
  await ctx.jsm.consumers.add(stream, { durable_name: a.durable, filter_subject: filtro, deliver_policy: "last_per_subject", ...canon() });
  const info = await ctx.jsm.consumers.info(stream, a.durable);
  const inmediato = info.num_pending === 0 && info.num_ack_pending === 0;          // §5.5, punto 2
  if (a.afterCreate) for (const w of a.afterCreate) await escribe(ctx.ops, "put", { contract: a.contract, tenant: w.tenant ?? a.tenant ?? "acme", key: w.key, version: w.version });
  const c = await ctx.ops.js.consumers.get(stream, a.durable);
  const entries = [];
  const it = await c.fetch({ max_messages: 500, expires: 1000 });
  for await (const m of it) {
    const ev = JSON.parse(dec.decode(m.data));
    const partes = m.subject.split(".");
    entries.push({ key: partes.at(-1), tenantId: partes.at(-2), deleted: /\.deleted\.v[1-9][0-9]*$/.test(ev.type), data: /\.deleted\./.test(ev.type) ? null : ev.data, aggregateVersion: ev.data.aggregateVersion, revision: m.info.streamSequence, pending: m.info.pending });
    m.ack();
  }
  const i0 = entries.findIndex((e) => e.pending === 0);                              // §5.5, punto 3
  return { count: entries.length, keys: entries.map((e) => e.key), entries: entries.map(({ pending, ...e }) => e), syncedImmediately: inmediato, syncedAtIndex: inmediato ? -1 : i0 };
}

// ─── Ejecución de casos ──────────────────────────────────────────────────────────────────────────
async function limpia(ctx) {
  for (const n of await ctx.jsm.streams.list().next().then((l) => l.map((s) => s.config.name)).catch(() => [])) { try { await ctx.jsm.streams.delete(n); } catch { /* ya no está */ } }
}

async function ejecutaCaso(c, ctx) {
  await limpia(ctx);
  const mem = {};
  const fallos = [];
  const traza = [];
  for (const [i, st] of c.steps.entries()) {
    let obs;
    try { obs = await paso(ctx, st, mem); } catch (e) { fallos.push(`paso ${i + 1} (${st.op}): ${e.message}`); break; }
    if (st.save) for (const [campo, nombre_] of Object.entries(st.save)) mem[nombre_] = camino(obs, campo);
    traza.push({ i: i + 1, st, obs });
    for (const [ruta, esperado] of Object.entries(st.expect ?? {})) {
      const v = camino(obs, ruta);
      if (!cumple(v, resuelve(esperado, mem))) fallos.push(`paso ${i + 1} (${st.title ?? st.op}): «${ruta}» = ${JSON.stringify(v)}, se esperaba ${JSON.stringify(esperado)}${st.control ? "  [control]" : ""}`);
    }
  }
  return { fallos, traza };
}

function valida(fichero, doc) {
  const conocidos = new Set(["id", "title", "spec", "covers", "rationale", "observed", "counterExample", "topology", "acl", "steps", "bugs", "note", "decision"]);
  for (const c of doc.cases) {
    for (const k of Object.keys(c)) if (!conocidos.has(k)) throw new Error(`${fichero}: el caso «${c.id}» tiene un campo desconocido «${k}»`);
    for (const k of ["spec", "rationale", "observed"]) if (!c[k]) throw new Error(`${fichero}: el caso «${c.id}» no tiene «${k}»: un caso sin norma, sin porqué o sin lo observado no es un caso`);
    for (const st of c.steps) {
      const def = OPS[st.op];
      if (!def) throw new Error(`${fichero}: «${c.id}» usa una operación desconocida «${st.op}»`);
      const permitidos = new Set(["op", "as", "expect", "title", "save", "control", ...(def.args ?? []), ...(def.optional ?? [])]);
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
valida("state-broker.json", broker);
valida("state-semantics.json", semantica);

// ─── Principal ───────────────────────────────────────────────────────────────────────────────────
const resultados = [];
const todos = [...broker.cases.map((c) => ({ ...c, fichero: "state-broker" })), ...semantica.cases.map((c) => ({ ...c, fichero: "state-semantics" }))].filter((c) => !ONLY || ONLY.includes(c.id));

let sinBinario = false;
try { const p = spawn(BIN, ["--version"], { stdio: "ignore" }); await new Promise((res, rej) => { p.on("exit", res); p.on("error", rej); }); } catch { sinBinario = true; }
if (sinBinario) {
  const m = `no se encuentra nats-server («${BIN}»). NATS_SERVER=/ruta/al/binario`;
  console.error(REQUIRE ? rojo(`✗ ${m}`) : gris(`- ${m}: los casos de broker NO se ejecutan (FLUX_REQUIRE_BROKER=1 lo convierte en fallo)`));
  process.exit(REQUIRE ? 1 : 0);
}

async function conTopologia(nombreTop, aclUsers, fn) {
  const srv = nombreTop === "cluster3" ? await cluster3() : await servidorSuelto(aclUsers);
  const ops = await conecta(srv.servers, aclUsers ? { user: "ops" } : {});
  const actores = {};
  for (const u of aclUsers ?? []) if (u.name !== "ops") actores[u.name] = await conecta(srv.servers, { user: u.name, inboxPrefix: `_INBOX.${u.name}` });
  const ctx = { ops, actores, jsm: await jetstreamManager(ops.nc) };
  try { return await fn(ctx); } finally { await Promise.all([ops.nc, ...Object.values(actores).map((x) => x.nc)].map((n) => n.close().catch(() => {}))); }
}

try {
  // 1) el broker suelto, sin ACL
  const sueltos = todos.filter((c) => (c.topology ?? "single") === "single" && !c.acl);
  if (sueltos.length) {
    await conTopologia("single", null, async (ctx) => {
      console.log(`\n\x1b[1mestado — un nodo\x1b[0m  (10-state.md)`);
      for (const c of sueltos) await informe(c, ctx);
    });
  }
  // 2) un servidor por cada conjunto de usuarios con permisos (`acls` de state-broker.json)
  for (const nombreAcl of [...new Set(todos.filter((c) => c.acl).map((c) => c.acl))]) {
    const def = broker.acls[nombreAcl];
    if (!def) throw new Error(`state-broker.json: acls no define «${nombreAcl}»`);
    const users = [{ name: "ops" }, ...def.users];
    await conTopologia("single", users, async (ctx) => {
      console.log(`\n\x1b[1mestado — ACL «${nombreAcl}»\x1b[0m  ${gris(def.title ?? "")}`);
      for (const c of todos.filter((c) => c.acl === nombreAcl)) await informe(c, ctx);
    });
  }
  // 3) el cluster
  const enCluster = todos.filter((c) => c.topology === "cluster3");
  if (enCluster.length && !flag("--no-cluster")) {
    await conTopologia("cluster3", null, async (ctx) => {
      console.log(`\n\x1b[1mestado — cluster de 3 nodos\x1b[0m`);
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
  const { fallos, traza } = await ejecutaCaso(c, ctx).catch((e) => ({ fallos: [`excepción: ${e.stack ?? e.message}`], traza: [] }));
  const ok = fallos.length === 0;
  resultados.push({ id: c.id, ok });
  console.log(`  ${ok ? verde("✓") : rojo("✗")} ${c.id.padEnd(44)} ${gris((c.covers ?? []).join(","))}`);
  if (!ok) for (const f of fallos) console.log(rojo(`      ${f}`));
  if (VERBOSE || !ok) for (const t of traza) console.log(gris(`      ${t.i}. ${t.st.title ?? t.st.op} → ${JSON.stringify(t.obs).slice(0, 300)}`));
}

// ─── Los ejemplos byte a byte de 10-state.md §2.6 ────────────────────────────────────────────────
console.log(`\n\x1b[1mcross-sdk-state-envelope\x1b[0m  (10-state.md §2.6)`);
{
  const ORDEN = envelope.attributeOrder;
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
    const partes = f.natsSubject.split(".");
    if (f.event.subject !== partes.at(-1) && !f.natsSubject.startsWith("dlq.")) fallos.push("el atributo subject no es la clave del subject de NATS");
    if (!f.natsSubject.startsWith("dlq.") && f.event.tenantid !== partes.at(-2)) fallos.push("tenantid no coincide con el token de tenant del subject de NATS");
    resultados.push({ id: `envelope:${f.id}`, ok: fallos.length === 0 });
    console.log(`  ${fallos.length ? rojo("✗") : verde("✓")} ${f.id.padEnd(44)} ${gris(`${bytes.length} bytes  sha256 ${sha.slice(0, 12)}…`)}`);
    for (const x of fallos) console.log(rojo(`      ${x}`));
  }
  // Los vectores del subject: el CÓDIGO estable de cada rechazo, derivado de los patrones de protocol.json
  // (que son la fuente) y comprobado contra lo que dicen los vectores. El orden es el de 10-state.md §3.6.
  const protocol = JSON.parse(await readFile(join(HERE, "..", "protocol.json"), "utf8"));
  const st = protocol.state;
  const derivado = (v) => {
    const [dom, agr] = v.contract.split(".");
    if (protocol.naming.reservedDomains.names.includes(dom)) return { code: "RESERVED_DOMAIN" };
    if (!new RegExp(st.contract.pattern).test(v.contract)) return { code: "INVALID_CONTRACT" };
    if (!new RegExp(st.key.pattern).test(v.key)) return { code: "INVALID_STATE_KEY" };
    if (!new RegExp(st.tenant.pattern).test(v.tenantId) || v.tenantId.length > st.tenant.maxLength) return { code: "INVALID_TENANT_FOR_STATE" };
    const sub = `state.${v.contract}.${v.tenantId}.${v.key}`;
    return new RegExp(st.subject.pattern).test(sub) ? { subject: sub } : { code: "SUBJECT_DOES_NOT_MATCH_PATTERN" };
  };
  const malos = [];
  for (const v of envelope.subjectVectors) {
    const d = derivado(v);
    const ok = v.natsSubject ? d.subject === v.natsSubject : d.code === v.errorCode;
    if (!ok) malos.push(`${JSON.stringify(v)} → ${JSON.stringify(d)}`);
  }
  resultados.push({ id: "envelope:subject-vectors", ok: malos.length === 0 });
  console.log(`  ${malos.length ? rojo("✗") : verde("✓")} ${`subject-vectors (${envelope.subjectVectors.length})`.padEnd(44)} ${gris("coinciden con los patrones de protocol.json")}`);
  for (const m of malos) console.log(rojo(`      ${m}`));

  // en los dos sentidos: una mutación DEBE cambiar el hash
  const f0 = envelope.fixtures[0];
  const roto = JSON.stringify({ ...f0.event, data: f0.event.data, tenantid: "ACME" });
  const ok = createHash("sha256").update(Buffer.from(roto, "utf8")).digest("hex") !== f0.sha256;
  resultados.push({ id: "envelope:control", ok });
  console.log(`  ${ok ? verde("✓") : rojo("✗")} ${"control: otro tenantid cambia el hash".padEnd(44)}`);
}

const mal = resultados.filter((r) => !r.ok);
console.log(`\n${mal.length ? rojo(`✗ ${mal.length} de ${resultados.length} casos fallan: ${mal.map((m) => m.id).join(", ")}`) : verde(`✓ ${resultados.length} casos de estado`)}`);
process.exit(mal.length ? 1 : 0);
