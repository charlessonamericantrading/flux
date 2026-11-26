#!/usr/bin/env node
/**
 * Comprueba la ESTRUCTURA de un `RESULTS.md` generado, no sus valores: lo que debe tener siempre, salga lo que salga de la medida.
 *
 *   node bench/comprobar-resultados.mjs [bench/RESULTS.md]
 *
 * Es lo que ejecuta el CI tras `node bench/run.mjs --rapido`. Una cifra que sube o baja con el ruido de la máquina del CI no es un
 * fallo del banco; que falte un escenario, una tabla, la máquina o las versiones, sí. Por eso aquí no hay ningún umbral numérico.
 */

import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { SECCIONES } from "./lib/informe.mjs";

const BENCH = path.dirname(fileURLToPath(import.meta.url));
const fichero = path.resolve(process.argv[2] ?? path.join(BENCH, "RESULTS.md"));
let md;
try {
  md = readFileSync(fichero, "utf8");
} catch (e) {
  console.error(`✗ no se puede leer ${fichero}: ${e.message}`);
  process.exit(2);
}

const errores = [];
const exige = (cond, que) => cond || errores.push(que);

for (const s of SECCIONES) exige(md.includes(`\n${s}\n`), `falta la sección «${s}»`);

// Una tabla bajo cada subtítulo de escenario, con al menos una fila de datos y sin celdas rotas.
const subtitulos = [/^#### A\.1 /m, /^#### B\.1 /m, /^#### C\.1 /m];
for (const re of subtitulos) {
  const m = re.exec(md);
  if (!exige(m, `falta la tabla ${re.source.replace(/\^|\/m| /g, "")}`)) continue;
  const resto = md.slice(m.index).split("\n").slice(1);
  const filas = [];
  for (const l of resto) {
    if (l.startsWith("|")) filas.push(l);
    else if (filas.length) break;
  }
  exige(filas.length >= 3, `la tabla ${m[0].trim()} no tiene filas de datos`);
}

// Ninguna celda con un valor roto (NaN, undefined, [object Object]).
for (const [i, l] of md.split("\n").entries()) {
  if (l.startsWith("|") && /\bNaN\b|undefined|\[object Object\]|Infinity/.test(l)) errores.push(`línea ${i + 1}: valor roto en una tabla: ${l.slice(0, 120)}`);
}

// La máquina y las versiones.
for (const clave of ["CPU", "nats-server", "Node", "SDK de flux", "@nats-io/", "RAM", "SO"]) exige(md.includes(clave), `falta «${clave}» en la sección de la máquina y las versiones`);
exige(/nats-server: v\d+\.\d+\.\d+/.test(md), "no consta la versión de nats-server");
exige(/Node\s*\| v\d+\.\d+\.\d+/.test(md) || /\| Node \| v\d+/.test(md), "no consta la versión de Node");

// Lo que no se mide, y la lectura.
exige(/No hay red real/.test(md), "falta la advertencia «No hay red real» en «Lo que NO se mide»");
exige(/Un solo nodo/.test(md), "falta la advertencia «Un solo nodo» en «Lo que NO se mide»");
exige(/Cruces calculados con los datos/.test(md), "falta la lectura calculada");

// Las comprobaciones del propio banco: debe haber alguna de cada escenario presente.
for (const e of ["A", "B", "C"]) exige(new RegExp(`^\\| ${e}: `, "m").test(md), `ninguna comprobación del propio banco del escenario ${e}`);

if (errores.length) {
  console.error(`✗ ${path.relative(process.cwd(), fichero)} no tiene la estructura esperada:\n${errores.map((e) => `  - ${e}`).join("\n")}`);
  process.exit(1);
}
console.log(`✓ ${path.relative(process.cwd(), fichero)}: estructura correcta (${SECCIONES.length} secciones, tablas de A, B y C, máquina y versiones, límites)`);
