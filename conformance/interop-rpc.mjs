#!/usr/bin/env node
/**
 * Interoperabilidad de la petición/respuesta entre los SDKs REALES de Node, de Rust y de Go, bajo los permisos que genera `gen-acl.mjs`.
 *
 *   (cd conformance && npm install) && (cd cli && npm install) && (cd sdk-node && npm install && npm run build)
 *   NATS_SERVER=/ruta/a/nats-server node conformance/interop-rpc.mjs
 *   node conformance/interop-rpc.mjs --only I2            # solo ese caso (con todas sus combinaciones)
 *   node conformance/interop-rpc.mjs --only "I2[node→rust]"
 *   node conformance/interop-rpc.mjs --list               # los casos, ya expandidos
 *
 * Los casos son DATOS (`cases/interop-rpc.json`): este fichero no sabe qué es I1. Monta el MISMO entorno que la suite de seguridad
 * (`security/entorno.mjs`: PKI efímera, nats-server con TLS, la ACL que genera `gen-acl.mjs` de `examples/services.json`,
 * `flux provision`) y ejecuta cada caso con el intérprete de `security/interprete.mjs`, cuya operación `sdk_script` lanza cada
 * instancia como un PROCESO con un SDK real (`security/participante-node.mjs`, `sdk-rust/examples/rpc_participant.rs` y `sdk-go/cmd/rpc-participant`) y le da
 * órdenes por stdin. El orquestador no sabe qué SDK es cuál: compara lo que cada uno cuenta de sí mismo con lo que el broker anotó.
 *
 * Un caso con `matrix` se expande en uno por combinación (`{caller}` y `{responder}`: `node` | `rust` | `go`), y el id queda
 * `I1[node→rust]`: quien llama es el primero. Las nueve combinaciones incluyen las tres de control (el mismo SDK a los dos lados).
 * Si el SDK de uno de los dos no es el de la combinación, el caso falla: no hay un valor por defecto que lo tape.
 *
 * Entorno
 *   NATS_SERVER            binario de nats-server (por defecto, `nats-server` del PATH)
 *   FLUX_REQUIRE_BROKER=1  la AUSENCIA de nats-server, openssl, sdk-node/dist o cargo es un FALLO, no un salto con aviso
 *   FLUX_TEST_PORTS        rango de puertos, `desde-hasta` (por defecto 4650-4699)
 *   FLUX_SDK_NODE_DIST, FLUX_SDK_RUST_DIR   otro SDK (una copia estropeada a propósito): solo `security/mutantes-sdk.mjs`
 *
 * Sale con 0 si todo lo ejecutado cumple, 1 si algo falla, 2 si el entorno está mal montado.
 */

import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { opensslDisponible } from "../scripts/lib/pki.mjs";
import { Banco } from "../scripts/lib/nats-harness.mjs";
import { crearEntorno } from "./security/entorno.mjs";
import { imprimirPaso } from "./security/informe.mjs";
import { validarCaso } from "./security/interprete.mjs";
import { exigido, falta, sinRequisitoGlobal } from "./security/requisitos.mjs";

const AQUI = path.dirname(fileURLToPath(import.meta.url));

// ─── Argumentos ──────────────────────────────────────────────────────────────

const USO = `uso: node conformance/interop-rpc.mjs [--only I1,I2] [--list] [--keep] [-v]

  --only ids  Solo esos casos (separados por comas). \`I2\` ejecuta todas sus combinaciones; \`I2[node→rust]\`, una.
  --list      Lista los casos expandidos y sale.
  --keep      No borra el directorio temporal (certificados, config y log del servidor).
  -v          Muestra lo observado en cada paso, no solo en los que fallan.`;

function usoErroneo(m) {
  console.error(`${m}\n${USO}`);
  process.exit(2);
}

const args = { only: undefined, list: false, keep: false, verbose: false };
for (let i = 2; i < process.argv.length; i++) {
  const a = process.argv[i];
  if (a === "--only") {
    const v = process.argv[++i];
    if (!v || v.startsWith("--")) usoErroneo("--only necesita un valor");
    args.only = new Set(v.split(",").map((s) => s.trim()).filter(Boolean));
  } else if (a === "--list") args.list = true;
  else if (a === "--keep") args.keep = true;
  else if (a === "-v" || a === "--verbose") args.verbose = true;
  else if (a === "-h" || a === "--help") {
    console.log(USO);
    process.exit(0);
  } else usoErroneo(`argumento desconocido: ${a}`);
}

// ─── Carga y expansión de los casos ──────────────────────────────────────────

/** Sustituye `{nombre}` en todas las cadenas de un valor. */
function sustituir(valor, vars) {
  if (typeof valor === "string") return valor.replace(/\{(\w+)\}/g, (m, k) => (k in vars ? vars[k] : m));
  if (Array.isArray(valor)) return valor.map((v) => sustituir(v, vars));
  if (valor && typeof valor === "object") return Object.fromEntries(Object.entries(valor).map(([k, v]) => [k, sustituir(v, vars)]));
  return valor;
}

/**
 * Un caso con `matrix: { caller: [...], responder: [...] }` pasa a ser uno por combinación. La plantilla `{marker}` NO se toca
 * aquí (la sustituye el intérprete, por caso y ejecución): solo `{<clave de matrix>}`.
 */
export function expandirMatriz(caso) {
  if (!caso.matrix) return [caso];
  const claves = Object.keys(caso.matrix);
  let combos = [{}];
  for (const k of claves) combos = combos.flatMap((c) => caso.matrix[k].map((v) => ({ ...c, [k]: v })));
  return combos.map((vars) => {
    const { matrix, ...resto } = caso;
    const etiqueta = claves.map((k) => vars[k]).join("→");
    return { ...sustituir(resto, vars), id: `${caso.id}[${etiqueta}]`, family: caso.id, combination: vars };
  });
}

const casosDeInterop = JSON.parse(await readFile(path.join(AQUI, "cases", "interop-rpc.json"), "utf8"));
const seguridad = JSON.parse(await readFile(path.join(AQUI, "cases", "security-acl.json"), "utf8"));
const suite = {
  ...casosDeInterop,
  vocabulary: seguridad.vocabulary,
  scenario: casosDeInterop.scenario ?? {},
  cases: casosDeInterop.cases.flatMap(expandirMatriz),
};
for (const caso of suite.cases) validarCaso(caso, suite.vocabulary);

if (args.list) {
  for (const c of suite.cases) console.log(`${c.id.padEnd(16)} ${c.title}${c.covers?.length ? `   [${c.covers.join(", ")}]` : ""}`);
  process.exit(0);
}
const elegido = (c) => !args.only || args.only.has(c.id) || args.only.has(c.family ?? c.id);
for (const id of args.only ?? []) {
  if (!suite.cases.some((c) => c.id === id || c.family === id)) usoErroneo(`no existe el caso «${id}». Casos: ${suite.cases.map((c) => c.id).join(", ")}`);
}

const falla = falta("nats-server") ?? falta("openssl");
if (falla) sinRequisitoGlobal(falla, "la suite de interoperabilidad entre SDKs");

// ─── Entorno ─────────────────────────────────────────────────────────────────

const [pDesde, pHasta] = (/^(\d+)-(\d+)$/.exec(process.env.FLUX_TEST_PORTS ?? "4650-4699") ?? []).slice(1).map(Number);
if (!pDesde) usoErroneo("FLUX_TEST_PORTS debe ser `desde-hasta`, por ejemplo 4650-4699");

const banco = new Banco({
  prefijo: "flux-interop-",
  puertoBase: pDesde,
  puertos: pHasta - pDesde + 1,
  natsServer: process.env.NATS_SERVER ?? "nats-server",
});

const R = { casos: [], inicio: Date.now() };
const seccion = (t) => console.log(`\n${t}`);

async function main() {
  const version = banco.version();
  const tmp = await banco.iniciar();
  console.log("flux — interoperabilidad de petición/respuesta: los SDKs REALES de Node, de Rust y de Go, bajo los permisos generados");
  console.log(`  ${version} · ${opensslDisponible().version} · temporales en ${tmp}`);
  const medido = [...new Set(suite.cases.map((c) => c.observed.server))];
  const num = (v) => /(\d+\.\d+\.\d+)/.exec(v)?.[1];
  if (!medido.every((m) => num(m) === num(version))) {
    console.log(`  · AVISO: los casos se midieron contra ${medido.join(", ")} y este servidor es ${version}: un fallo puede ser el broker, no un SDK`);
  }

  const { l, contexto, interprete, provision } = await crearEntorno({ banco, suite: { ...suite, cases: suite.cases.filter(elegido) } });
  console.log(`  · nats-server con TLS en ${l.url}; ${contexto.generadas.length} servicios + ops`);
  console.log(
    `  ${provision.ok ? "✓" : "✗"} flux provision por TLS con credenciales de operación: ${provision.creados ?? "0"} recursos creados (esperados: ${provision.esperados.streams} streams + ${provision.esperados.durables} durables)`,
  );
  if (!provision.ok) {
    console.log(`      ${(provision.salida + provision.errores).trim().split("\n").join("\n      ")}`);
    R.casos.push({ id: "provision", titulo: "flux provision", ok: false });
    return l;
  }

  for (const caso of suite.cases.filter(elegido)) {
    seccion(`${caso.id}  ${caso.title}${caso.covers?.length ? `   [${caso.covers.join(", ")}]` : ""}`);
    const faltas = (caso.needs ?? []).map((n) => [n, falta(n)]).filter(([, f]) => f);
    if (faltas.length) {
      const motivo = faltas.map(([n, f]) => `${n}: ${f}`).join("; ");
      console.log(`  ${exigido ? "✗" : "-"} ${exigido ? "FALTA un requisito" : "omitido"}: ${motivo}`);
      R.casos.push({ id: caso.id, titulo: caso.title, ok: !exigido, omitido: motivo });
      continue;
    }
    let r;
    try {
      r = await interprete.ejecutarCaso(caso);
    } catch (e) {
      r = { id: caso.id, ok: false, pasos: [{ descripcion: "el caso se interrumpió", ok: false, fallos: [e?.stack ?? String(e)] }] };
    }
    for (const p of r.pasos) imprimirPaso(p, args.verbose);
    R.casos.push({ id: caso.id, titulo: caso.title, ok: r.ok, omitidos: r.omitidos });
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
  console.log(`  ${marca} ${String(c.id).padEnd(16)} ${c.titulo ?? ""}${c.omitido ? `  (${c.omitido})` : ""}`);
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
