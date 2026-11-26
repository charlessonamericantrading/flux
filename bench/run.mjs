#!/usr/bin/env node
/**
 * El runner del banco: `node bench/run.mjs [--rapido]`.
 *
 *   node bench/run.mjs                   la malla completa (cada punto, 5 repeticiones); escribe bench/RESULTS.md y bench/datos/completo.json
 *   node bench/run.mjs --rapido          prueba de humo (< 2 min): escribe bench/resultados/RESULTS.rapido.md; NO toca RESULTS.md
 *   node bench/run.mjs --solo-informe    regenera RESULTS.md desde bench/datos/completo.json, sin medir
 *
 *   --escenario a,b,c     solo esos escenarios
 *   --reps N              repeticiones por punto (5 en la completa, 1 en la rápida)
 *   --reintentos N        cuántas veces se repite una repetición con carga ajena o un proceso saturado (2); 0 = nunca
 *   --salida FICHERO      dónde escribir el informe          --datos FICHERO    dónde guardar los datos
 *   --preparar            instala las dependencias que faltan (npm install en conformance, cli y sdk-node; build del SDK) y sale
 *   --ayuda
 *
 * Requisitos: Node ≥ 20, `nats-server` (NATS_SERVER=/ruta o en el PATH), `openssl` para los puntos con TLS, y las dependencias
 * de conformance/, cli/ y sdk-node/ instaladas (`--preparar`). Puertos: FLUX_TEST_PORTS=desde-hasta (por defecto 4700-4799).
 */

import { execFileSync, spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { RAIZ, BENCH, crearBroker, describirEntorno, natsServerBin } from "./lib/entorno.mjs";
import { fijarReintentos } from "./lib/corrida.mjs";
import { cargaMedia } from "./lib/proc.mjs";
import { latencias } from "./lib/estadistica.mjs";
import { comprobar } from "./lib/comprobar.mjs";
import { plan } from "./lib/plan.mjs";
import { generarInforme } from "./lib/informe.mjs";
import { manifiestoA, puntoA } from "./escenarios/a-enterarse.mjs";
import { manifiestoB, puntoB } from "./escenarios/b-avisar.mjs";
import { manifiestoC, puntoC } from "./escenarios/c-llamada.mjs";
import { crearPki } from "../scripts/lib/pki.mjs";

const args = process.argv.slice(2);
const flag = (f) => args.includes(f);
const opt = (f) => (args.includes(f) ? args[args.indexOf(f) + 1] : undefined);

if (flag("--ayuda") || flag("-h")) {
  console.log(`uso: node bench/run.mjs [--rapido] [--escenario a,b,c] [--reps N] [--reintentos N] [--salida F] [--datos F] [--solo-informe] [--preparar]\nVer bench/README.md.`);
  process.exit(0);
}

// ─── --preparar: instalar lo que falta ───────────────────────────────────────

const DEPENDENCIAS = [
  ["conformance", "node_modules/@nats-io/nkeys"],
  ["cli", "node_modules/@nats-io/jetstream"],
  ["sdk-node", "node_modules/@nats-io/transport-node"],
];

function faltan() {
  const f = [];
  for (const [dir, marca] of DEPENDENCIAS) if (!existsSync(path.join(RAIZ, dir, marca))) f.push(`${dir}/ (npm install)`);
  if (!existsSync(path.join(RAIZ, "sdk-node", "dist", "index.js"))) f.push("sdk-node/dist (npm run build)");
  return f;
}

if (flag("--preparar")) {
  for (const [dir, marca] of DEPENDENCIAS) {
    if (existsSync(path.join(RAIZ, dir, marca))) continue;
    console.log(`→ npm install en ${dir}/`);
    execFileSync("npm", ["install", "--no-audit", "--no-fund"], { cwd: path.join(RAIZ, dir), stdio: "inherit" });
  }
  console.log("→ npm run build en sdk-node/");
  execFileSync("npm", ["run", "build"], { cwd: path.join(RAIZ, "sdk-node"), stdio: "inherit" });
  console.log("✓ listo. Ejecuta: node bench/run.mjs --rapido");
  process.exit(0);
}

const RAPIDO = flag("--rapido");
const DATOS = path.resolve(opt("--datos") ?? path.join(BENCH, "datos", RAPIDO ? "rapido.json" : "completo.json"));
const SALIDA = path.resolve(opt("--salida") ?? (RAPIDO ? path.join(BENCH, "resultados", "RESULTS.rapido.md") : path.join(BENCH, "RESULTS.md")));

// ─── --solo-informe ──────────────────────────────────────────────────────────

if (flag("--solo-informe")) {
  const { readFile } = await import("node:fs/promises");
  const datos = JSON.parse(await readFile(path.resolve(opt("--datos") ?? path.join(BENCH, "datos", "completo.json")), "utf8"));
  await mkdir(path.dirname(SALIDA), { recursive: true });
  await writeFile(SALIDA, generarInforme(datos));
  console.log(`✓ ${path.relative(process.cwd(), SALIDA)} regenerado desde los datos (sin medir)`);
  process.exit(0);
}

// ─── requisitos ──────────────────────────────────────────────────────────────

const falta = faltan();
if (falta.length) {
  console.error(`✗ faltan dependencias: ${falta.join(", ")}\n  Ejecuta: node bench/run.mjs --preparar`);
  process.exit(2);
}
const nats = spawnSync(natsServerBin(), ["--version"], { encoding: "utf8" });
if (nats.error || nats.status !== 0) {
  console.error(`✗ no se puede ejecutar «${natsServerBin()}» (NATS_SERVER=/ruta/a/nats-server o ponlo en el PATH)`);
  process.exit(2);
}

// ─── ejecución ───────────────────────────────────────────────────────────────

const PLAN = plan({ rapido: RAPIDO });
const reps = Number(opt("--reps") ?? PLAN.reps);
if (opt("--reintentos") !== undefined) fijarReintentos(Number(opt("--reintentos")));
else if (RAPIDO) fijarReintentos(0);
const solo = opt("--escenario")?.toUpperCase().split(",");
const grupos = PLAN.grupos.filter((g) => !solo || solo.includes(g.escenario));
const necesitaTls = grupos.some((g) => g.tls);
const nucleos = os.cpus().length;

const t0 = Date.now();
const entorno = describirEntorno();
const datos = {
  meta: { entorno, rapido: RAPIDO, reps, inicio: new Date().toISOString(), cargaInicio: cargaMedia(), nucleos, centroA: PLAN.centroA, centroB: PLAN.centroB },
  puntos: [],
};

const total = grupos.reduce((s, g) => s + g.puntos.length, 0) * reps;
let hechas = 0;
let tmpCerts;
let certs;
let brokerActual;
const limpiar = async () => {
  await brokerActual?.parar().catch(() => {});
  if (tmpCerts) await rm(tmpCerts, { recursive: true, force: true });
};
for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, async () => {
    await limpiar();
    process.exit(130);
  });
}

const resumen = (res) => {
  const { lat, ...resto } = res;
  return { ...resto, latencia: lat ? latencias(lat) : undefined };
};

async function guardar() {
  datos.meta.fin = new Date().toISOString();
  datos.meta.cargaFin = cargaMedia();
  datos.meta.duracionS = (Date.now() - t0) / 1000;
  const m = Math.round(datos.meta.duracionS / 60);
  datos.meta.duracionTexto = datos.meta.duracionS < 120 ? `${Math.round(datos.meta.duracionS)} s` : `${m} min`;
  await mkdir(path.dirname(DATOS), { recursive: true });
  await writeFile(DATOS, JSON.stringify(datos));
}

async function ejecutar(g, punto, rep, broker) {
  const comun = { rep, tls: !!g.tls, broker, certs, ventanaS: punto.ventanaS, calentamientoS: punto.calentamientoS, metodo: punto.metodo };
  if (punto.escenario === "A") return puntoA({ ...comun, n: punto.n, cambiosPorMin: punto.c, periodoS: punto.t });
  if (punto.escenario === "B") return puntoB({ ...comun, n: punto.n, hechosPorMin: punto.h });
  return puntoC({ ...comun, concurrencia: punto.conc });
}

const claveDe = (g, p) => {
  const base = { A: `A ${p.metodo} N=${p.n} C=${p.c} T=${p.t}`, B: `B ${p.metodo} N=${p.n} H=${p.h}`, C: `C ${p.metodo} c=${p.conc}` }[p.escenario];
  return g.tls ? `${base} TLS` : base;
};

try {
  console.log(`bench: ${RAPIDO ? "RÁPIDO" : "COMPLETO"} · ${grupos.length} grupos · ${reps} repetición(es) por punto · ${total} repeticiones · ${nucleos} núcleos · carga ${cargaMedia().join(" ")}`);
  if (necesitaTls) {
    tmpCerts = await mkdtemp(path.join(os.tmpdir(), "flux-bench-certs-"));
    const pki = await crearPki(tmpCerts, { dias: 2, otraCa: false });
    certs = { cert: pki.servidorCert, key: pki.servidorKey, ca: pki.ca };
  }
  for (const g of grupos) {
    const usaBroker = g.puntos.some((p) => ["flux", "flux-otel", "nats-crudo"].includes(p.metodo));
    brokerActual = undefined;
    if (usaBroker) {
      const manifiesto = g.escenario === "A" ? manifiestoA(g.n) : g.escenario === "B" ? manifiestoB(g.n) : manifiestoC();
      process.stdout.write(`\n── grupo ${g.id}: nats-server con la configuración segura (${manifiesto.services.length} servicios${g.tls ? ", TLS" : ""})… `);
      brokerActual = await crearBroker({ manifiesto, tls: !!g.tls, nombre: g.id.toLowerCase() });
      console.log(`aprovisionado en ${brokerActual.msProvision} ms`);
    } else console.log(`\n── grupo ${g.id}`);
    const puntos = g.puntos.map((p) => ({ clave: claveDe(g, p), grupo: g.id, escenario: p.escenario, params: { ...p, tls: !!g.tls }, reps: [] }));
    for (let rep = 1; rep <= reps; rep++) {
      for (const [i, punto] of g.puntos.entries()) {
        const etiqueta = `${puntos[i].clave} · rep ${rep}/${reps}`;
        const ini = Date.now();
        try {
          const r = await ejecutar(g, punto, rep, brokerActual);
          const res = resumen({ ...r.res, intentos: r.intentos, dudosa: !!r.dudosa });
          res.comprobaciones = comprobar(punto.escenario, r.res);
          puntos[i].reps.push(res);
          const mal = res.comprobaciones.filter((c) => !c.ok && !/fiabilidad/.test(c.nombre));
          console.log(`  [${++hechas}/${total}] ${etiqueta} · ${((Date.now() - ini) / 1000).toFixed(0)} s · carga ${res.cargaDespues?.[0]}${r.dudosa ? " · DUDOSA" : ""}${mal.length ? ` · ⚠ ${mal.map((c) => c.nombre).join("; ")}` : ""}`);
        } catch (e) {
          puntos[i].reps.push({ rep, error: String(e?.message ?? e).slice(0, 800) });
          console.error(`  [${++hechas}/${total}] ${etiqueta} · ✗ ${String(e?.message ?? e).split("\n")[0]}`);
        }
      }
      datos.puntos = [...datos.puntos.filter((p) => !puntos.includes(p)), ...puntos];
      await guardar();
    }
    datos.puntos = [...datos.puntos.filter((p) => !puntos.includes(p)), ...puntos];
    await brokerActual?.parar();
    brokerActual = undefined;
  }
} finally {
  await limpiar();
}

await guardar();
await mkdir(path.dirname(SALIDA), { recursive: true });
await writeFile(SALIDA, generarInforme(datos));
const fallidas = datos.puntos.flatMap((p) => p.reps).filter((r) => r.error).length;
console.log(`\n✓ ${datos.meta.duracionTexto}: ${path.relative(process.cwd(), SALIDA)} y ${path.relative(process.cwd(), DATOS)} escritos${fallidas ? ` (${fallidas} repetición(es) fallaron: están en los datos)` : ""}`);
process.exit(fallidas && RAPIDO ? 1 : 0);
