#!/usr/bin/env node
/**
 * Mutantes del GENERADOR de ACL: la suite de seguridad debe FALLAR con cada uno.
 *
 *   node conformance/security/mutantes.mjs                 # todos los mutantes
 *   node conformance/security/mutantes.mjs --fast          # sin los lentos (los de `expires`, ~1 min cada uno)
 *   node conformance/security/mutantes.mjs --sdk           # los casos que deben fallar son los de los SDKs REALES (S30 y S34)
 *   node conformance/security/mutantes.mjs --only caller-domain-wildcard,no-allow-responses
 *   node conformance/security/mutantes.mjs --list
 *
 * Una prueba que solo se ha visto en verde no demuestra nada. Los casos S21 a S29 de `security-acl.json` traen
 * sus propios controles (variantes estropeadas A MANO, con `extraPublish`, `without`…), pero eso prueba que la
 * SONDA puede fallar, no que la suite detecte un GENERADOR que concede de más o de menos. Esto último es lo que
 * hace este script: copia `scripts/` a un directorio temporal, le aplica a la copia UN cambio de texto —el que
 * haría un error real: conceder `rpc.<dominio>.>` a quien llama, quitar `allow_responses`, dar `_INBOX.>`…—, y
 * ejecuta `conformance/security.mjs` con `FLUX_GEN_ACL` apuntando a esa copia. Cada mutante declara qué casos DEBEN
 * fallar: la suite tiene que salir con 1 y esos casos tienen que estar entre los que fallan. Antes, el mismo
 * subconjunto de casos se ejecuta SIN mutar y tiene que pasar (si no, un fallo no probaría nada).
 *
 * `--sdk` hace lo mismo con los casos de los SDKs REALES (S30, el ciclo de §12.9 con el SDK de Node, y S34, `allow_responses` con
 * un responder tramposo): un generador estropeado también tiene que romper al SDK de verdad, no solo a los clientes en bruto. Va
 * aparte para no alargar el job de seguridad: lo ejecuta el de interoperabilidad, en paralelo.
 *
 * El repo real no se toca: el mutante vive en `os.tmpdir()` y se borra al terminar.
 *
 * Entorno: el de `security.mjs` (NATS_SERVER, FLUX_TEST_PORTS, FLUX_REQUIRE_BROKER).
 * Sale con 0 si todos los mutantes se detectan, 1 si alguno NO, 2 si el entorno está mal.
 */

import { spawn, spawnSync } from "node:child_process";
import { cp, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const AQUI = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(AQUI, "..", "..");
const SUITE = path.join(ROOT, "conformance", "security.mjs");

const MANIFEST = "scripts/lib/manifest.mjs";
const GEN = "scripts/gen-acl.mjs";

/**
 * Cada mutante: un cambio de TEXTO en un fichero de la copia (`de` debe aparecer EXACTAMENTE una vez: si el generador
 * cambia y el ancla desaparece, el mutante lo dice en vez de «no cambiar nada y pasar»), y los casos que DEBEN fallar.
 */
export const MUTANTES = [
  {
    nombre: "caller-domain-wildcard",
    que: "quien llama recibe `rpc.<dominio>.>` en vez del subject exacto: podría llamar a TODAS las operaciones del dominio",
    archivo: MANIFEST,
    de: "grupo(`Llama a ${op}: publica en el subject EXACTO de la operación`, [`rpc.${op}`]);",
    a: "grupo(`Llama a ${op}: publica en el subject EXACTO de la operación`, [`rpc.${domainOf(op)}.>`]);",
    debenFallar: ["S21"],
    sdk: ["S30"],
  },
  {
    nombre: "caller-rpc-all",
    que: "quien llama recibe `rpc.>`: podría llamar a las operaciones de todos los dominios",
    archivo: MANIFEST,
    de: "grupo(`Llama a ${op}: publica en el subject EXACTO de la operación`, [`rpc.${op}`]);",
    a: "grupo(`Llama a ${op}: publica en el subject EXACTO de la operación`, [`rpc.>`]);",
    debenFallar: ["S21"],
    sdk: ["S30"],
  },
  {
    nombre: "caller-without-permission",
    que: "quien llama no recibe ningún permiso de publicación sobre la operación: sus llamadas se deniegan",
    archivo: MANIFEST,
    de: "grupo(`Llama a ${op}: publica en el subject EXACTO de la operación`, [`rpc.${op}`]);",
    a: "void op;",
    debenFallar: ["S21", "S27"],
    sdk: ["S30"],
  },
  {
    nombre: "owner-rpc-all",
    que: "el dueño se suscribe a `rpc.>`: atendería las operaciones de todos los dominios",
    archivo: MANIFEST,
    de: "const subscribe = [`_INBOX.${servicio.name}.>`, ...dominiosRpc.map((d) => `rpc.${d}.>`)];",
    a: "const subscribe = [`_INBOX.${servicio.name}.>`, ...(dominiosRpc.length ? [\"rpc.>\"] : [])];",
    debenFallar: ["S22"],
    sdk: ["S30"],
  },
  {
    nombre: "owner-without-subscribe",
    que: "el dueño no recibe su suscripción `rpc.<dominio>.>`: parece un servicio caído (A-73)",
    archivo: MANIFEST,
    de: "const subscribe = [`_INBOX.${servicio.name}.>`, ...dominiosRpc.map((d) => `rpc.${d}.>`)];",
    a: "const subscribe = [`_INBOX.${servicio.name}.>`];",
    debenFallar: ["S21", "S26", "S27"],
    sdk: ["S30"],
  },
  {
    nombre: "caller-gets-domain-subscription",
    que: "quien llama recibe también la suscripción `rpc.<dominio>.>` de los dominios que llama: leería las peticiones del dueño",
    archivo: MANIFEST,
    de: "const subscribe = [`_INBOX.${servicio.name}.>`, ...dominiosRpc.map((d) => `rpc.${d}.>`)];",
    a: "const subscribe = [`_INBOX.${servicio.name}.>`, ...[...new Set([...atendidas, ...llamadas].map(domainOf))].sort().map((d) => `rpc.${d}.>`)];",
    debenFallar: ["S22"],
    sdk: ["S30"],
  },
  {
    nombre: "shared-inbox",
    que: "todos se suscriben a `_INBOX.>` en vez de a su inbox privado: cualquiera lee las respuestas ajenas (A-71)",
    archivo: MANIFEST,
    de: "const subscribe = [`_INBOX.${servicio.name}.>`, ...dominiosRpc.map((d) => `rpc.${d}.>`)];",
    a: "const subscribe = [`_INBOX.>`, ...dominiosRpc.map((d) => `rpc.${d}.>`)];",
    debenFallar: ["S23"],
    sdk: ["S30"],
  },
  {
    nombre: "no-allow-responses",
    que: "el dueño no recibe `allow_responses`: recibe y ejecuta, pero su respuesta se deniega",
    archivo: MANIFEST,
    de: "const allowResponses = atendidas.length ? { ...RPC_ALLOW_RESPONSES } : null;",
    a: "const allowResponses = null;",
    debenFallar: ["S21", "S24", "S27", "S28"],
    sdk: ["S30"],
  },
  {
    nombre: "no-allow-responses-in-conf",
    que: "la config de nats-server no lleva `allow_responses` aunque `--json` lo diga: la suite mide lo que se carga, no lo que se afirma",
    archivo: GEN,
    de: "  if (perm.allowResponses) {",
    a: "  if (false) {",
    debenFallar: ["S21", "S24", "S27", "S28"],
    sdk: ["S30"],
  },
  {
    nombre: "allow-responses-max-3",
    que: "`allow_responses` con `max: 3`: el dueño contesta tres veces a la misma petición",
    archivo: MANIFEST,
    de: 'export const RPC_ALLOW_RESPONSES = Object.freeze({ max: 1, expires: "30s" });',
    a: 'export const RPC_ALLOW_RESPONSES = Object.freeze({ max: 3, expires: "30s" });',
    debenFallar: ["S24"],
    sdk: ["S34"],
  },
  {
    nombre: "owner-publishes-on-inboxes",
    que: "el dueño recibe `_INBOX.>` para publicar: escribe en los inboxes de todos y `max: 1` no sirve de nada",
    archivo: MANIFEST,
    de: "  const dominiosRpc = [...new Set(atendidas.map(domainOf))].sort();",
    a: '  const dominiosRpc = [...new Set(atendidas.map(domainOf))].sort();\n  if (atendidas.length) grupo("mutante: publicar en cualquier inbox", ["_INBOX.>"]);',
    debenFallar: ["S24"],
    sdk: ["S34"],
  },
  {
    nombre: "no-expires",
    que: "`allow_responses` sin `expires`: una respuesta a los 31 s, cuando la llamada ya expiró, SE ENTREGA (lento)",
    archivo: GEN,
    de: 'allow_responses: { max: ${perm.allowResponses.max}, expires: "${perm.allowResponses.expires}" }',
    a: "allow_responses: { max: ${perm.allowResponses.max} }",
    debenFallar: ["S25"],
    lento: true,
  },
  {
    nombre: "short-expires",
    que: "`allow_responses` con `expires: 5s`: una respuesta legítima de un handler lento (24 s, dentro del plazo de 25 s) se deniega (lento)",
    archivo: MANIFEST,
    de: 'export const RPC_ALLOW_RESPONSES = Object.freeze({ max: 1, expires: "30s" });',
    a: 'export const RPC_ALLOW_RESPONSES = Object.freeze({ max: 1, expires: "5s" });',
    debenFallar: ["S25"],
    lento: true,
  },
];

// ─── Argumentos ──────────────────────────────────────────────────────────────

const argv = process.argv.slice(2);
const o = { fast: false, sdk: false, only: undefined, list: false, verbose: false };
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (a === "--fast") o.fast = true;
  else if (a === "--sdk") o.sdk = true;
  else if (a === "--list") o.list = true;
  else if (a === "-v" || a === "--verbose") o.verbose = true;
  else if (a === "--only") o.only = new Set((argv[++i] ?? "").split(",").map((s) => s.trim()).filter(Boolean));
  else {
    console.error(`argumento desconocido: ${a}\nuso: node conformance/security/mutantes.mjs [--fast] [--sdk] [--only a,b] [--list] [-v]`);
    process.exit(2);
  }
}
for (const n of o.only ?? []) {
  if (!MUTANTES.some((m) => m.nombre === n)) {
    console.error(`no existe el mutante «${n}». Los hay: ${MUTANTES.map((m) => m.nombre).join(", ")}`);
    process.exit(2);
  }
}
if (o.list) {
  for (const m of MUTANTES) console.log(`${m.nombre.padEnd(34)} ${m.lento ? "[lento] " : "        "}${m.que}  → debe fallar ${m.debenFallar.join(", ")}${m.sdk ? `; con --sdk, ${m.sdk.join(", ")}` : ""}`);
  process.exit(0);
}
// Con `--sdk` los casos que deben fallar son los de los SDKs REALES (`sdk`, S30 y S34), y solo se ejecutan los mutantes que los tienen.
const casosDe = (m) => (o.sdk ? m.sdk : m.debenFallar);
const elegidos = MUTANTES.filter((m) => (o.only ? o.only.has(m.nombre) : !(o.fast && m.lento)) && (!o.sdk || m.sdk));

// ─── Ejecución ───────────────────────────────────────────────────────────────

const bin = process.env.NATS_SERVER ?? "nats-server";
const v = spawnSync(bin, ["--version"], { encoding: "utf8" });
if (v.error || v.status !== 0) {
  const exigido = !["", "0", "false"].includes((process.env.FLUX_REQUIRE_BROKER ?? "").trim().toLowerCase());
  console.error(`${exigido ? "✗" : "AVISO:"} no se pudo ejecutar nats-server («${bin}»): los mutantes necesitan un broker.`);
  process.exit(exigido ? 1 : 0);
}

/** Ejecuta la suite sobre `casos` y devuelve `{ codigo, casos: Map(id → ok), salida }`. */
function ejecutarSuite(casos, env) {
  return new Promise((resolver) => {
    const hijo = spawn(process.execPath, [SUITE, "--only", casos.join(",")], { cwd: ROOT, env: { ...process.env, ...env }, stdio: ["ignore", "pipe", "pipe"] });
    let salida = "";
    hijo.stdout.on("data", (d) => (salida += d));
    hijo.stderr.on("data", (d) => (salida += d));
    hijo.on("close", (codigo) => {
      const resumen = salida.split("\nResumen\n")[1] ?? "";
      const resultados = new Map();
      for (const l of resumen.split("\n")) {
        const m = /^\s+([✓✗-])\s+(S\d+|provision|suite)\s/.exec(l);
        if (m) resultados.set(m[2], m[1] === "✓");
      }
      resolver({ codigo, casos: resultados, salida });
    });
  });
}

/** Copia `scripts/`, el bundle y el manifiesto a un temporal y aplica el mutante. Devuelve la ruta del generador mutado. */
async function prepararMutante(m) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "flux-mutante-"));
  await cp(path.join(ROOT, "scripts"), path.join(dir, "scripts"), { recursive: true });
  await cp(path.join(ROOT, "schemas", "bundle.json"), path.join(dir, "schemas", "bundle.json"));
  await cp(path.join(ROOT, "examples"), path.join(dir, "examples"), { recursive: true });
  const ruta = path.join(dir, m.archivo);
  const texto = await readFile(ruta, "utf8");
  const veces = texto.split(m.de).length - 1;
  if (veces !== 1) {
    await rm(dir, { recursive: true, force: true });
    throw new Error(`el ancla del mutante «${m.nombre}» aparece ${veces} veces en ${m.archivo} (debe ser UNA): el generador cambió y el mutante ya no muta nada`);
  }
  await writeFile(ruta, texto.replace(m.de, () => m.a));
  return { dir, gen: path.join(dir, "scripts", "gen-acl.mjs") };
}

const R = { detectados: 0, escapan: [], total: elegidos.length };
console.log(`flux — mutantes del generador de ACL: la suite de seguridad debe FALLAR con cada uno (${elegidos.length})`);
console.log(`  ${v.stdout.trim()} · ${o.fast ? "sin los lentos" : "todos"}`);

// Sin mutar: el subconjunto debe pasar. Si no, un fallo con un mutante no probaría nada.
const todosLosCasos = [...new Set(elegidos.flatMap(casosDe))].sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)));
console.log(`\nSin mutar: ${todosLosCasos.join(", ")}`);
const base = await ejecutarSuite(todosLosCasos, {});
const baseOk = base.codigo === 0 && todosLosCasos.every((c) => base.casos.get(c) === true);
console.log(`  ${baseOk ? "✓" : "✗"} la suite sin mutar pasa ${todosLosCasos.length} caso(s)`);
if (!baseOk) {
  console.log(base.salida.split("\n").slice(-30).join("\n"));
  console.log("\n✗ la suite falla SIN mutar: no se puede medir nada.");
  process.exit(1);
}

for (const m of elegidos) {
  let prep;
  try {
    prep = await prepararMutante(m);
  } catch (e) {
    console.log(`\n${m.nombre}\n  ✗ ${e.message}`);
    R.escapan.push(m.nombre);
    continue;
  }
  try {
    console.log(`\n${m.nombre}: ${m.que}`);
    const deben = casosDe(m);
    const r = await ejecutarSuite(deben, { FLUX_GEN_ACL: prep.gen });
    const fallan = deben.filter((c) => r.casos.get(c) === false);
    const noFallan = deben.filter((c) => r.casos.get(c) !== false);
    const detectado = r.codigo === 1 && noFallan.length === 0;
    if (detectado) R.detectados++;
    else R.escapan.push(m.nombre);
    console.log(
      `  ${detectado ? "✓ DETECTADO" : "✗ NO DETECTADO"}: fallan ${fallan.length ? fallan.join(", ") : "ninguno"} de los esperados (${deben.join(", ")}); salida ${r.codigo}` +
        (noFallan.length ? `; NO fallan: ${noFallan.join(", ")}` : ""),
    );
    if (!detectado || o.verbose) console.log(r.salida.split("\n").filter((l) => /✗|✓ \d+\/|se interrumpió|Error/.test(l)).slice(0, 14).join("\n"));
  } finally {
    await rm(prep.dir, { recursive: true, force: true });
  }
}

console.log(`\n${R.escapan.length === 0 ? "✓" : "✗"} ${R.detectados}/${R.total} mutantes detectados` + (R.escapan.length ? ` · NO detectados: ${R.escapan.join(", ")}` : ""));
process.exit(R.escapan.length === 0 ? 0 : 1);
