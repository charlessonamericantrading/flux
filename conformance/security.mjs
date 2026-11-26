#!/usr/bin/env node
/**
 * Suite de seguridad: las ACLs que genera `scripts/gen-acl.mjs`, ejecutadas contra un
 * nats-server REAL, con TLS y con los SDKs de verdad. Fase 1.5 del plan v1.1.
 *
 *   (cd conformance && npm install) && (cd cli && npm install) && (cd sdk-node && npm install && npm run build)
 *   NATS_SERVER=/ruta/a/nats-server node conformance/security.mjs
 *   node conformance/security.mjs --only S1,S6        # solo esos casos
 *   node conformance/security.mjs --acl conformance/fixtures/acl-v1.conf   # DEBE fallar (S1, S6)
 *   node conformance/security.mjs --list              # los casos y su estado
 *
 * Los casos son DATOS (`cases/security-acl.json`): este fichero no sabe qué es S1. Orquesta el
 * entorno —PKI efímera con openssl, nkeys de prueba, la config que genera `gen-acl.mjs`, un
 * nats-server con TLS, `flux provision`— y delega la ejecución de cada paso en
 * `security/interprete.mjs`. Lo único que hace por su cuenta son las dos operaciones que
 * necesitan procesos aparte: `sdk_cycle` (S10 y S11, los SDKs reales) y `regression` (S9, la ACL
 * v1 congelada en un segundo servidor).
 *
 * Habla NATS directamente, sin ningún SDK de flux (salvo en S10 y S11, que existen justo para
 * lo contrario): un runner construido sobre un SDK heredaría las suposiciones que debe
 * verificar.
 *
 * Entorno
 *   NATS_SERVER            binario de nats-server (por defecto, `nats-server` del PATH)
 *   FLUX_REQUIRE_BROKER=1  la AUSENCIA de nats-server, openssl, sdk-node/dist o cargo es un
 *                          FALLO, no un salto con aviso. En CI siempre: un job verde que no
 *                          ejecutó nada aparenta cobertura.
 *   FLUX_TEST_PORTS        rango de puertos, `desde-hasta` (por defecto 4600-4649)
 *
 * Sale con 0 si todo lo ejecutado cumple, 1 si algo falla, 2 si el entorno está mal montado.
 */

import { spawn, spawnSync } from "node:child_process";
import { statSync } from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { crearPki, opensslDisponible } from "../scripts/lib/pki.mjs";
import {
  Banco,
  CLI,
  EJEMPLO,
  ROOT,
  conUsuarios,
  conectar,
  credsDe,
  dec,
  gen,
  jetstream,
  jetstreamManager,
  nuevaClave,
  permisosJson,
  sleep,
  usuarioConf,
  usuarioOpsConf,
} from "../scripts/lib/nats-harness.mjs";
import {
  Interprete,
  actoresDe,
  claveVariante,
  esVariante,
  evaluar,
  normalizarActor,
  permisosDeVariante,
  validarCaso,
} from "./security/interprete.mjs";

const AQUI = path.dirname(fileURLToPath(import.meta.url));
const CASOS = path.join(AQUI, "cases", "security-acl.json");

// ─── Argumentos ──────────────────────────────────────────────────────────────

const USO = `uso: node conformance/security.mjs [--acl <fichero.conf>] [--only S1,S6] [--list] [--keep] [-v]

  --acl <f>   Sustituye la ACL generada por la de ese fichero (con \`accounts { … }\` incluible, como
              conformance/fixtures/acl-v1.conf: un usuario por servicio, con \`user: "<servicio>"\` y
              \`password: "pw"\`). Sirve para ver qué casos detectan una ACL rota: con la v1 DEBEN
              fallar S1 y S6. S9, S10 y S11 no aplican.
  --only ids  Solo esos casos (separados por comas).
  --list      Lista los casos y sale.
  --keep      No borra el directorio temporal (certificados, config y log del servidor).
  -v          Muestra lo observado en cada paso, no solo en los que fallan.`;

function leerArgumentos(argv) {
  const o = { acl: undefined, only: undefined, list: false, keep: false, verbose: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--acl" || a === "--only") {
      const v = argv[++i];
      if (!v || v.startsWith("--")) usoErroneo(`${a} necesita un valor`);
      if (a === "--acl") o.acl = v;
      else o.only = new Set(v.split(",").map((s) => s.trim()).filter(Boolean));
    } else if (a === "--list") o.list = true;
    else if (a === "--keep") o.keep = true;
    else if (a === "-v" || a === "--verbose") o.verbose = true;
    else if (a === "-h" || a === "--help") {
      console.log(USO);
      process.exit(0);
    } else usoErroneo(`argumento desconocido: ${a}`);
  }
  return o;
}

function usoErroneo(m) {
  console.error(`${m}\n${USO}`);
  process.exit(2);
}

const args = leerArgumentos(process.argv.slice(2));

// ─── Requisitos: se saltan con aviso, o FALLAN con FLUX_REQUIRE_BROKER ───────

/** `FLUX_REQUIRE_BROKER` definida y no vacía, `0` o `false`. */
const exigido = (() => {
  const v = (process.env.FLUX_REQUIRE_BROKER ?? "").trim().toLowerCase();
  return v !== "" && v !== "0" && v !== "false";
})();

/** Qué falta de lo que la suite necesita, o `undefined`. */
function falta(que) {
  switch (que) {
    case "nats-server": {
      const bin = process.env.NATS_SERVER ?? "nats-server";
      const r = spawnSync(bin, ["--version"], { encoding: "utf8" });
      return r.error || r.status !== 0
        ? `no se pudo ejecutar nats-server («${bin}»). Define NATS_SERVER con su ruta; sin Docker: GOBIN=$PWD/bin go install github.com/nats-io/nats-server/v2@v2.14.5`
        : undefined;
    }
    case "openssl":
      return opensslDisponible().ok ? undefined : "no hay `openssl` en el PATH (la PKI efímera de TLS lo necesita)";
    case "sdk-node/dist":
      return statSync(path.join(ROOT, "sdk-node", "dist", "index.js"), { throwIfNoEntry: false })?.isFile()
        ? undefined
        : "no existe sdk-node/dist: ejecuta (cd sdk-node && npm install && npm run build)";
    case "cargo":
      return spawnSync("cargo", ["--version"], { encoding: "utf8" }).status === 0 ? undefined : "no hay `cargo` en el PATH (el SDK de Rust no se puede compilar)";
    default:
      return `requisito desconocido «${que}»`;
  }
}

/** Sale: con aviso y 0, o —con FLUX_REQUIRE_BROKER— con el error y 1. */
function sinRequisitoGlobal(motivo) {
  if (exigido) {
    console.error(
      `✗ ${motivo}.\n  FLUX_REQUIRE_BROKER está activada: la suite de seguridad es obligatoria, y saltarla daría un CI verde sin haber ejecutado nada contra el broker.`,
    );
    process.exit(1);
  }
  console.warn(
    `\nAVISO: se SALTA la suite de seguridad: ${motivo}.\n       Define NATS_SERVER, o FLUX_REQUIRE_BROKER=1 para que su ausencia sea un fallo.\n`,
  );
  process.exit(0);
}

// ─── Carga de los casos ──────────────────────────────────────────────────────

const suite = JSON.parse(await readFile(CASOS, "utf8"));
for (const caso of suite.cases) validarCaso(caso, suite.vocabulary);
const porId = new Map(suite.cases.map((c) => [c.id, c]));

if (args.list) {
  for (const c of suite.cases) {
    console.log(`${c.id.padEnd(4)} ${c.title}${c.covers?.length ? `   [${c.covers.join(", ")}]` : ""}`);
  }
  process.exit(0);
}
for (const id of args.only ?? []) {
  if (!porId.has(id)) usoErroneo(`no existe el caso «${id}». Casos: ${suite.cases.map((c) => c.id).join(", ")}`);
}

const falla = falta("nats-server") ?? falta("openssl");
if (falla) sinRequisitoGlobal(falla);

// ─── Entorno ─────────────────────────────────────────────────────────────────

const [pDesde, pHasta] = (/^(\d+)-(\d+)$/.exec(process.env.FLUX_TEST_PORTS ?? "4600-4649") ?? []).slice(1).map(Number);
if (!pDesde) usoErroneo("FLUX_TEST_PORTS debe ser `desde-hasta`, por ejemplo 4600-4649");

const NATS_SERVER = process.env.NATS_SERVER ?? "nats-server";
const banco = new Banco({
  prefijo: "flux-security-",
  puertoBase: pDesde,
  puertos: pHasta - pDesde + 1,
  natsServer: NATS_SERVER,
});

const R = { casos: [], inicio: Date.now() };
const seccion = (t) => console.log(`\n${t}`);
const nota = (t) => console.log(`  · ${t}`);

/** Lo observado, en una línea. */
function lineaObservado(o) {
  if (!o) return "";
  const partes = [];
  if (o.outcome !== undefined) partes.push(String(o.outcome));
  for (const k of ["stream", "subject", "leaked", "victimRead", "count", "ackPending", "numPending", "ackTokens", "deliveryCount", "violation", "childExit", "serverViolationLines"]) {
    if (o[k] !== undefined && o[k] !== null && o[k] !== "") partes.push(`${k}=${typeof o[k] === "object" ? JSON.stringify(o[k]) : o[k]}`);
  }
  if (o.reply?.error) partes.push(`error ${o.reply.error.err_code ?? ""} «${o.reply.error.description}»`);
  if (o.text) partes.push(`«${String(o.text).split("\n")[0].slice(0, 130)}»`);
  if (typeof o.ms === "number") partes.push(`${o.ms} ms`);
  return partes.join(" · ");
}

function imprimirPaso(p) {
  if (p.omitido) {
    console.log(`  - ${p.descripcion}\n      omitido: ${p.omitido}`);
    return;
  }
  const marca = p.ok ? "✓" : "✗";
  const control = p.control ? " (control)" : "";
  console.log(`  ${marca} ${p.descripcion}${control}`);
  if (p.knownDeviation) console.log(`      ⚠ desviación conocida: ${p.knownDeviation}`);
  if (!p.ok || args.verbose) console.log(`      → ${lineaObservado(p.observado)}`);
  for (const f of p.fallos ?? []) console.log(`      ✗ ${f}`);
  if (args.verbose && p.note) console.log(`      · ${p.note}`);
}

// ─── Servidor, identidades y aprovisionamiento ───────────────────────────────

/**
 * Levanta un nats-server con TLS y la ACL dada, conecta `ops` y aprovisiona con `flux provision`.
 * Devuelve el contexto del intérprete, o lanza.
 */
async function levantar(nombre, { acl, identidades, ops, pki }) {
  const top =
    `tls {\n  cert_file: "${pki.servidorCert}"\n  key_file: "${pki.servidorKey}"\n  timeout: 2\n}\n`;
  const srv = await banco.arrancar(nombre, acl, { top, servidorNombre: `flux-security-${nombre}` });
  const url = `tls://127.0.0.1:${srv.port}`;

  const nc = await conectar(url, { nombre: "ops", seed: ops.seed, tls: { caFile: pki.ca }, etiqueta: "flux-security" });
  const opsConn = { nc, jsm: await jetstreamManager(nc), js: jetstream(nc) };

  // `flux provision` con credenciales de operación, por TLS con la CA efímera. La CLI no tiene
  // una opción `--ca`, así que la CA llega por el mecanismo de Node (NODE_EXTRA_CA_CERTS): un
  // operador con una CA privada tendría que hacer lo mismo.
  const credsOps = path.join(banco.tmp, `ops-${nombre}.creds`);
  await writeFile(credsOps, credsDe(ops.seed), { mode: 0o600 });
  const r = spawnSync(process.execPath, [CLI, "provision", EJEMPLO, "--server", url, "--creds", credsOps], {
    encoding: "utf8",
    cwd: ROOT,
    env: { ...process.env, NO_COLOR: "1", NODE_EXTRA_CA_CERTS: pki.ca },
  });
  const provision = { codigo: r.status, salida: r.stdout, errores: r.stderr };
  return { srv, url, ops: opsConn, provision, identidades, pki };
}

// ─── S10 y S11: los SDKs reales ──────────────────────────────────────────────

/** Las líneas de violación de permiso que anotó el servidor desde `desde` (un offset de su log). */
const violacionesDelServidor = (srv, desde) =>
  srv.salida.slice(desde).split("\n").filter((l) => /\b(?:Publish|Subscription) Violation\b/.test(l));

function ejecutar(cmd, argv, { cwd, env, timeoutMs }) {
  return new Promise((resolver) => {
    const hijo = spawn(cmd, argv, { cwd, env, stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    let err = "";
    hijo.stdout.on("data", (d) => (out += d));
    hijo.stderr.on("data", (d) => (err += d));
    const t = setTimeout(() => {
      err += `\n(se mató al proceso: más de ${timeoutMs} ms)`;
      hijo.kill("SIGKILL");
    }, timeoutMs);
    hijo.on("error", (e) => {
      clearTimeout(t);
      resolver({ codigo: -1, out, err: `${err}\nno se pudo lanzar ${cmd}: ${e.message}` });
    });
    hijo.on("close", (codigo) => {
      clearTimeout(t);
      resolver({ codigo, out, err });
    });
  });
}

/**
 * Lanza el ciclo de un SDK (publicar, consumir, ack, DLQ) CONTRA la ACL generada y mira, desde
 * el broker, lo que dejó: lo que el SDK diga de sí mismo no basta.
 *
 * El contrato con el proceso hijo es solo entorno y una línea de salida:
 *   entra   FLUX_ACL_URL, FLUX_ACL_CA, FLUX_ACL_PRODUCER(_SEED), FLUX_ACL_CONSUMER(_SEED),
 *           FLUX_ACL_SUBJECT, FLUX_ACL_EVENTS (JSON: [{ caso, calls, data }])
 *   sale    una línea `FLUX_ACL_REPORT {json}` con lo publicado y lo que vio el handler
 */
async function cicloSdk(p, ctx, marker) {
  const ident = (s) => {
    const i = ctx.identidades.get(s);
    if (!i?.seed) throw new Error(`${s}: sin semilla nkey (el ciclo de un SDK necesita las identidades generadas)`);
    return dec.decode(i.seed);
  };
  const eventos = p.events.map((e) => JSON.parse(JSON.stringify(e).replaceAll("{marker}", marker)));
  const env = {
    ...process.env,
    FLUX_ACL_URL: ctx.url,
    FLUX_ACL_CA: ctx.pki.ca,
    FLUX_ACL_PRODUCER: p.producer,
    FLUX_ACL_PRODUCER_SEED: ident(p.producer),
    FLUX_ACL_CONSUMER: p.consumer,
    FLUX_ACL_CONSUMER_SEED: ident(p.consumer),
    FLUX_ACL_SUBJECT: p.subject,
    FLUX_ACL_EVENTS: JSON.stringify(eventos),
  };

  // El estado del que partimos: el durable del consumidor limpio y la DLQ del dominio medida.
  const dominio = p.subject.split(".")[0];
  const stream = `EVT_${dominio.toUpperCase().replace(/-/g, "_")}`;
  const dlqStream = `DLQ_${dominio.toUpperCase().replace(/-/g, "_")}`;
  const durable = `${p.consumer}__${p.subject.replace(/[.-]/g, "_")}`;
  const dlqAntes = (await ctx.ops.jsm.streams.info(dlqStream)).state.last_seq;
  const logAntes = ctx.srv.salida.length;

  const [cmd, argv, cwd] =
    p.sdk === "node"
      ? [process.execPath, [path.join(AQUI, "security", "node-cycle.mjs")], ROOT]
      : ["cargo", ["run", "--quiet", "--example", "acl_cycle"], path.join(ROOT, "sdk-rust")];
  const r = await ejecutar(cmd, argv, { cwd, env, timeoutMs: p.timeoutMs ?? 60_000 });

  const linea = r.out.split("\n").find((l) => l.startsWith("FLUX_ACL_REPORT "));
  if (!linea) {
    return {
      outcome: "error",
      childExit: r.codigo,
      text: `el proceso no emitió su informe (exit ${r.codigo}): ${(r.err || r.out).trim().split("\n").slice(-6).join(" | ")}`,
    };
  }
  const informe = JSON.parse(linea.slice("FLUX_ACL_REPORT ".length));
  await sleep(500); // los acks y la DLQ en vuelo

  // Lo que el handler vio, por caso.
  const handlerCalls = {};
  for (const c of informe.handlerCalls ?? []) (handlerCalls[c.caso] ??= []).push(c.attempt);

  // El estado que quedó en el broker, mirado con `ops`.
  const info = await ctx.ops.jsm.consumers.info(stream, durable);
  const dlq = [];
  const ultimo = (await ctx.ops.jsm.streams.info(dlqStream)).state.last_seq;
  for (let seq = dlqAntes + 1; seq <= ultimo; seq++) {
    const m = await ctx.ops.jsm.streams.getMessage(dlqStream, { seq }).catch(() => null);
    if (!m) continue;
    const texto = dec.decode(m.data);
    if (texto.includes(marker)) dlq.push(texto);
  }
  const dlqEvento = dlq[0] ? JSON.parse(dlq[0]) : null;
  const permanente = (informe.published ?? []).find((e) => e.caso === "permanente");
  const violaciones = violacionesDelServidor(ctx.srv, logAntes);

  return {
    outcome: r.codigo === 0 ? "ok" : "error",
    childExit: r.codigo,
    text: r.codigo === 0 ? "" : (informe.errors?.join(" | ") || r.err.trim().split("\n").slice(-4).join(" | ")),
    published: (informe.published ?? []).length,
    handlerCalls,
    ackPending: info.num_ack_pending,
    numPending: info.num_pending,
    dlqCount: dlq.length,
    dlqReason: dlqEvento?.dlqreason ?? null,
    dlqAttempts: dlqEvento?.dlqattempts ?? null,
    dlqIdPreserved: !!dlqEvento && !!permanente && dlqEvento.id === permanente.id,
    dlqDataLast: dlqEvento ? Object.keys(dlqEvento).at(-1) === "data" : false,
    serverViolations: violaciones.length,
    serverViolationLines: violaciones.map((l) => l.replace(/^\[\d+\] \S+ \S+ /, "")),
    serverViolationText: violaciones.map((l) => /Subject "([^"]*)"/.exec(l)?.[0] ?? l).join(" | "),
    sdkLog: informe.logs ?? [],
  };
}

// ─── Ejecución de un caso (con S9 y S10/S11 como operaciones propias) ────────

/** Ejecuta un caso entero: los pasos del intérprete y, si los hay, los de este fichero. */
async function ejecutarCaso(caso, ctx, interprete) {
  const necesidades = (caso.needs ?? []).map((n) => [n, falta(n)]).filter(([, f]) => f);
  if (necesidades.length) {
    const motivo = necesidades.map(([n, f]) => `${n}: ${f}`).join("; ");
    return { id: caso.id, ok: !exigido, omitido: motivo, pasos: [], faltaRequisito: true };
  }

  // Los pasos que solo este fichero sabe ejecutar se separan de los del intérprete.
  const propios = new Set(["sdk_cycle", "regression"]);
  if (!caso.steps.some((s) => propios.has(s.op))) return interprete.ejecutarCaso(caso);

  const pasos = [];
  const estado = { marker: `${caso.id}-${Math.random().toString(16).slice(2, 10)}` };
  for (const paso of caso.steps) {
    if (!propios.has(paso.op)) {
      const r = await interprete.ejecutarCaso({ ...caso, steps: [paso] });
      pasos.push(...r.pasos);
      continue;
    }
    const descripcion = interprete.describirPaso(paso);
    let observado;
    try {
      observado = paso.op === "sdk_cycle" ? await cicloSdk(paso, ctx, estado.marker) : await ejecutarRegresion(paso, ctx);
    } catch (e) {
      observado = { outcome: "error", text: e?.message ?? String(e) };
    }
    const fallos = evaluar(paso.expect, observado);
    pasos.push({ id: paso.id, descripcion, ok: fallos.length === 0, observado, fallos, control: !!paso.control, note: paso.note, knownDeviation: paso.knownDeviation });
  }
  return { id: caso.id, ok: pasos.every((p) => p.ok || p.omitido), pasos, omitidos: pasos.filter((p) => p.omitido).length };
}

// ─── S9: la ACL antigua, congelada, debe seguir detectándose rota ────────────

async function ejecutarRegresion(p, ctx) {
  const fixture = await readFile(path.resolve(ROOT, p.acl), "utf8");
  const ops = nuevaClave();
  const nombres = permisosJson().map((s) => s.name);
  // La v1 no tiene nkeys: cada servicio entra con usuario y contraseña, como en el fixture.
  const identidades = new Map([["ops", ops], ...nombres.map((n) => [n, { user: n, pass: "pw" }])]);
  const acl = conUsuarios(fixture, usuarioOpsConf(ops.pub));
  const v1 = await levantar("v1", { acl, identidades, ops, pki: ctx.pki });
  try {
    // Sin los streams y los durables aprovisionados, S1 fallaría por otra causa y la regresión
    // daría por detectado un H1 que no es el documentado.
    if (v1.provision.codigo !== 0) {
      throw new Error(`flux provision falló contra el servidor con la ACL v1:\n${(v1.provision.salida + v1.provision.errores).trim()}`);
    }
    const interprete = new Interprete({ ...baseCtx(v1, ctx), generada: false });
    const observado = {};
    for (const id of p.reruns) {
      const caso = porId.get(id);
      const r = await interprete.ejecutarCaso(caso);
      observado[id] = {
        passes: r.ok,
        failed: r.pasos.filter((x) => !x.ok && !x.omitido).length,
        steps: Object.fromEntries(r.pasos.filter((x) => x.id).map((x) => [x.id, x.observado])),
      };
    }
    const texto = p.reruns
      .map((id) => `${id}: ${observado[id].passes ? "PASA" : `FALLA (${observado[id].failed} paso(s) ✗)`}`)
      .join(" · ");
    return { outcome: "ok", text: texto, ...observado };
  } finally {
    await v1.ops.nc.close().catch(() => {});
    await banco.parar(v1.srv);
  }
}

const baseCtx = (l, ctxPrincipal) => ({
  url: l.url,
  caFile: l.pki.ca,
  otraCa: l.pki.otraCa,
  ops: l.ops,
  canon: ctxPrincipal.canon,
  identidades: l.identidades,
  variantes: new Map(),
});

// ─── Principal ───────────────────────────────────────────────────────────────

async function main() {
  const version = banco.version();
  const tmp = await banco.iniciar();
  console.log(`flux — suite de seguridad: ACLs generadas, TLS y SDKs contra un nats-server real`);
  console.log(`  ${version} · ${opensslDisponible().version} · temporales en ${tmp}`);
  const medido = [...new Set(suite.cases.map((c) => c.observed.server))];
  const num = (v) => /(\d+\.\d+\.\d+)/.exec(v)?.[1];
  if (!medido.every((m) => num(m) === num(version))) {
    nota(`AVISO: los casos se midieron contra ${medido.join(", ")} y este servidor es ${version}: un fallo puede ser el broker, no la ACL`);
  }
  if (args.acl) nota(`ACL bajo prueba: ${args.acl} (no la generada)`);

  // ── Identidades ────────────────────────────────────────────────────────────
  const generada = !args.acl;
  const prod = permisosJson();
  const dev = permisosJson("--dev");
  const ops = nuevaClave();
  const canon = JSON.parse(await readFile(path.join(ROOT, "protocol.json"), "utf8")).consumer;

  const identidades = new Map([["ops", ops]]);
  const variantes = new Map();
  let acl;
  if (generada) {
    for (const s of prod) identidades.set(s.name, nuevaClave());
    const mapa = path.join(tmp, "nkeys.json");
    await writeFile(mapa, JSON.stringify(Object.fromEntries(prod.map((s) => [s.name, identidades.get(s.name).pub]))));
    const g = gen(EJEMPLO, "--nkeys", mapa);
    if (g.codigo !== 0) throw new Error(`gen-acl.mjs falló:\n${g.errores}`);

    // Las variantes que los casos piden: los permisos generados, estropeados a propósito.
    let extra = "";
    for (const caso of suite.cases) {
      for (const paso of caso.steps) {
        for (const a of actoresDe(paso).map(normalizarActor).filter(esVariante)) {
          const clave = claveVariante(a);
          if (variantes.has(clave)) continue;
          // Una variante imposible (quita un permiso que el generador ya no concede) no aborta la
          // suite: hace fallar los pasos que la usan, con el motivo. Es justo lo que ocurre si
          // alguien quita un permiso del generador.
          try {
            const k = nuevaClave();
            const perms = permisosDeVariante(a, { prod, dev });
            variantes.set(clave, k);
            extra += usuarioConf(k.pub, perms.publish, perms.subscribe);
          } catch (e) {
            variantes.set(clave, { error: e.message });
          }
        }
      }
    }
    extra += usuarioOpsConf(ops.pub);
    acl = conUsuarios(g.salida, extra);
  } else {
    // La ACL antigua entra con usuario y contraseña (no tiene nkeys): `<servicio>` / `pw`.
    for (const s of prod) identidades.set(s.name, { user: s.name, pass: "pw" });
    acl = conUsuarios(await readFile(path.resolve(args.acl), "utf8"), usuarioOpsConf(ops.pub));
  }

  // ── PKI, servidor, aprovisionamiento ───────────────────────────────────────
  const pki = await crearPki(tmp, { dias: 2 });
  const l = await levantar("principal", { acl, identidades, ops, pki });
  nota(`nats-server con TLS en ${l.url}; ${prod.length} servicios${generada ? `, ${[...variantes.values()].filter((v) => !v.error).length} variante(s) de prueba` : ""} + ops`);

  const durables = prod.reduce((n, s) => n + s.durables.length, 0);
  // EVT_ y DLQ_ por dominio, y STATE_ por cada dominio con estado.
  const streams = 2 * new Set(prod.flatMap((s) => s.owns)).size + new Set(prod.flatMap((s) => s.stateOwns)).size;
  const creados = /(\d+) creado/.exec(l.provision.salida)?.[1];
  const provisionOk = l.provision.codigo === 0 && Number(creados) === streams + durables;
  console.log(`  ${provisionOk ? "✓" : "✗"} flux provision por TLS con credenciales de operación: ${creados ?? "0"} recursos creados (esperados: ${streams} streams + ${durables} durables)`);
  if (!provisionOk) {
    console.log(`      ${(l.provision.salida + l.provision.errores).trim().split("\n").join("\n      ")}`);
    R.casos.push({ id: "provision", titulo: "flux provision", ok: false });
    return l;
  }

  const contexto = { ...baseCtx(l, { canon }), generada, ops: l.ops, srv: l.srv, pki, canon, variantes };
  const interprete = new Interprete(contexto);

  // ── Los casos ──────────────────────────────────────────────────────────────
  // Los que no aplican con --acl: S9 (trae su propia ACL) y los de los SDKs (necesitan nkeys).
  const noAplicaConAcl = (c) => c.steps.some((s) => ["regression", "sdk_cycle"].includes(s.op));
  for (const caso of suite.cases) {
    if (args.only && !args.only.has(caso.id)) continue;
    seccion(`${caso.id}  ${caso.title}${caso.covers?.length ? `   [${caso.covers.join(", ")}]` : ""}`);
    if (args.acl && noAplicaConAcl(caso)) {
      console.log(`  - no aplica con --acl: ${caso.steps.some((s) => s.op === "regression") ? "trae su propia ACL" : "el ciclo de un SDK usa las nkeys generadas"}`);
      R.casos.push({ id: caso.id, titulo: caso.title, ok: true, omitido: "no aplica con --acl" });
      continue;
    }
    let r;
    try {
      r = await ejecutarCaso(caso, contexto, interprete);
    } catch (e) {
      r = { id: caso.id, ok: false, pasos: [{ descripcion: "el caso se interrumpió", ok: false, fallos: [e?.stack ?? String(e)] }] };
    }
    if (r.omitido) {
      console.log(`  ${exigido ? "✗" : "-"} ${exigido ? "FALTA un requisito" : "omitido"}: ${r.omitido}`);
    }
    for (const p of r.pasos) imprimirPaso(p);
    R.casos.push({ id: caso.id, titulo: caso.title, ok: r.ok, omitido: r.omitido, omitidos: r.omitidos });
  }
  return l;
}

let principal;
banco.instalarManejadores((e) => console.log(`\n  ✗ error no controlado: ${e?.stack ?? e}`));
try {
  principal = await main();
} catch (e) {
  console.log(`\n  ✗ la suite se interrumpió: ${e?.stack ?? e}`);
  R.casos.push({ id: "suite", titulo: "la suite se interrumpió", ok: false });
} finally {
  await principal?.ops?.nc?.close().catch(() => {});
  if (principal?.srv) await banco.parar(principal.srv);
  await banco.limpiar({ conservar: args.keep });
}

// ─── Resumen ─────────────────────────────────────────────────────────────────

seccion("Resumen");
for (const c of R.casos) {
  const marca = c.omitido ? (c.ok ? "-" : "✗") : c.ok ? "✓" : "✗";
  const extra = c.omitido ? `  (${c.omitido})` : c.omitidos ? `  (${c.omitidos} paso(s) omitido(s))` : "";
  console.log(`  ${marca} ${String(c.id).padEnd(4)} ${c.titulo ?? ""}${extra}`);
}
const falladas = R.casos.filter((c) => !c.ok);
const omitidas = R.casos.filter((c) => c.ok && c.omitido);
const seg = ((Date.now() - R.inicio) / 1000).toFixed(1);
console.log(
  `\n${falladas.length === 0 ? "✓" : "✗"} ${R.casos.length - falladas.length - omitidas.length}/${R.casos.length} casos cumplen` +
    (omitidas.length ? ` · ${omitidas.length} omitido(s): ${omitidas.map((c) => c.id).join(", ")}` : "") +
    (falladas.length ? ` · fallan: ${falladas.map((c) => c.id).join(", ")}` : "") +
    ` · ${seg} s`,
);
if (args.keep) console.log(`  temporales conservados en ${banco.tmp}`);
process.exit(falladas.length === 0 ? 0 : 1);
