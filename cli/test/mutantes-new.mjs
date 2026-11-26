#!/usr/bin/env node
/**
 * «Una prueba que pasa con el código roto no vale.» Cada mutante copia `cli/`, `scripts/` y `protocol.json` a un temporal, estropea UNA garantía
 * de `flux new` y ejecuta `test/new.test.mjs` sobre la copia: DEBE fallar, y por la razón correcta (el nombre de la prueba que falla). Antes se
 * ejecuta sin estropear: tiene que pasar.
 *
 *   NATS_SERVER=… FLUX_REQUIRE_BROKER=1 node test/mutantes-new.mjs [--solo <nombre>] [--lista]
 */

import { spawnSync } from "node:child_process";
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const CLI = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const RAIZ = path.join(CLI, "..");
const NEW = "cli/src/new.mjs";

/** `cambios`: [[de, a]] (cada `de` DEBE aparecer exactamente una vez); `falla`: lo que debe contener el nombre de la prueba que falla. */
const MUTANTES = [
  { nombre: "pisa-el-esquema-existente", en: NEW, cambios: [["if (existentes.length) {", "if (false) {"]], falla: "NUNCA pisa un esquema" },
  { nombre: "no-revisa-el-arbol-de-antes", en: NEW, cambios: [["  if (existsSync(dir)) {\n    const antes", "  if (false) {\n    const antes"]], falla: "no se completa a escondidas" },
  { nombre: "no-comprueba-el-contrato-nuevo", en: NEW, cambios: [["if (!despues.ok) throw", "if (false) throw"]], falla: "se DESHACE todo" },
  { nombre: "no-deshace-los-ficheros", en: NEW, cambios: [["for (const f of creados) rmSync(f, { force: true });", ""]], falla: "se DESHACE todo" },
  { nombre: "no-deshace-los-directorios", en: NEW, cambios: [["try { if (readdirSync(d).length === 0) rmdirSync(d); } catch { /* ya no está */ }", ""]], falla: "directorios nuevos incluidos" },
  { nombre: "el-evento-sin-aggregateVersion", en: NEW, cambios: [['required: [referencia, "aggregateVersion"],\n        properties: {\n          [referencia]: idPropiedad("Identificador del agregado; el atributo', 'required: [referencia],\n        properties: {\n          [referencia]: idPropiedad("Identificador del agregado; el atributo']], falla: "aggregateVersion" },
  { nombre: "el-estado-con-version-opcional", en: NEW, cambios: [['required: [referencia, "aggregateVersion"],\n        properties: {\n          [referencia]: idPropiedad("Identificador del agregado: la CLAVE', 'required: [referencia],\n        properties: {\n          [referencia]: idPropiedad("Identificador del agregado: la CLAVE']], falla: "aggregateVersion" },
  { nombre: "id-que-no-es-la-ruta", en: NEW, cambios: [["const id = (ruta) => `https://schemas.internal/${ruta}`;", "const id = (ruta) => `https://schemas.internal/${ruta.replace(/\\.0\\.0\\.json$/, '.0.json')}`;"]], falla: "el `$id` es la ruta" },
  { nombre: "la-operacion-sin-respuesta", en: NEW, cambios: [["    {\n      ruta: respuesta,", "    ...[].concat({\n      ruta: respuesta,"], ["        required: [],\n        properties: {},\n      },\n    },\n  ];", "        required: [],\n        properties: {},\n      },\n    }).slice(1),\n  ];"]], falla: "DOS esquemas" },
  { nombre: "sin-el-esquema-de-plataforma", en: NEW, cambios: [["if (faltaSistema) crea(", "if (false) crea("]], falla: "lápida" },
  { nombre: "el-dry-run-escribe", en: NEW, cambios: [["  if (opts.dryRun) {", "  if (false) {"]], falla: "--dry-run" },
  { nombre: "dominio-reservado-aceptado", en: NEW, cambios: [["if (k.reservados.includes(dominio)) {\n    const resto", "if (false) {\n    const resto"]], falla: "dominios reservados" },
  { nombre: "patron-del-evento-laxo", en: NEW, cambios: [["reEvento: new RegExp(p.naming.subject.pattern),", "reEvento: /.*/,"], ["problemas.push(...formaDelNombre(nombre, 4, \"<dominio>.<agregado>.v<major>.<evento>\", k.ejemplos.event));", ""]], falla: "event: rechaza" },
  { nombre: "acepta-mayusculas", en: NEW, cambios: [['if (/[A-Z]/.test(nombre)) problemas.push(["lleva mayúsculas"', 'if (false) problemas.push(["lleva mayúsculas"']], falla: "confusiones habituales" },
  { nombre: "rechazo-sin-como-arreglarlo", en: NEW, cambios: [['problemas.push(["tiene un guion bajo", `solo kebab-case: «${nombre.replace(/_/g, "-")}»`]);', 'problemas.push(["tiene un guion bajo", ""]);']], falla: "CADA rechazo" },
  { nombre: "el-bundle-no-se-regenera", en: NEW, cambios: [["const b = ejecutarScript(\"bundle-schemas.mjs\", [\"--schemas\", dir]);", "const b = { ok: true, salida: \"\" };"]], falla: "regenera el bundle" },
];

const solo = process.argv.includes("--solo") ? process.argv[process.argv.indexOf("--solo") + 1] : undefined;
if (process.argv.includes("--lista")) {
  for (const m of MUTANTES) console.log(m.nombre);
  process.exit(0);
}

function copia(m) {
  const tmp = mkdtempSync(path.join(os.tmpdir(), "flux-new-mutante-"));
  mkdirSync(path.join(tmp, "cli"));
  for (const f of ["src", "test", "package.json"]) cpSync(path.join(CLI, f), path.join(tmp, "cli", f), { recursive: true });
  symlinkSync(path.join(CLI, "node_modules"), path.join(tmp, "cli", "node_modules"));
  cpSync(path.join(RAIZ, "scripts"), path.join(tmp, "scripts"), { recursive: true });
  cpSync(path.join(RAIZ, "schemas"), path.join(tmp, "schemas"), { recursive: true });
  cpSync(path.join(RAIZ, "protocol.json"), path.join(tmp, "protocol.json"));
  // `scripts/lib/nats-harness.mjs` resuelve las dependencias de NATS desde `conformance/`: no se usa aquí, pero que no falle un import.
  mkdirSync(path.join(tmp, "conformance"));
  if (m) {
    for (const [de, a] of m.cambios) {
      const f = path.join(tmp, m.en);
      const texto = readFileSync(f, "utf8");
      const n = texto.split(de).length - 1;
      if (n !== 1) throw new Error(`${m.nombre}: «${de.slice(0, 70)}…» aparece ${n} veces en ${m.en}; debe aparecer exactamente una`);
      writeFileSync(f, texto.replace(de, () => a));
    }
  }
  return tmp;
}

function ejecutar(tmp) {
  const r = spawnSync(process.execPath, ["--test", "--test-force-exit", "--test-reporter=tap", path.join(tmp, "cli", "test", "new.test.mjs")], {
    encoding: "utf8", cwd: path.join(tmp, "cli"), env: { ...process.env },
  });
  const salida = `${r.stdout}${r.stderr}`;
  return { estado: r.status, fallan: [...salida.matchAll(/^\s*not ok \d+ - (.*)$/gm)].map((x) => x[1]), salida };
}

console.log("Sin estropear: debe PASAR…");
const base = copia(undefined);
const b = ejecutar(base);
rmSync(base, { recursive: true, force: true });
if (b.estado !== 0) {
  console.error(`✗ sin estropear no pasa:\n${b.salida.split("\n").slice(-50).join("\n")}`);
  process.exit(1);
}
console.log("  ✓ pasa");

let mal = 0;
let n = 0;
for (const m of MUTANTES) {
  if (solo && m.nombre !== solo) continue;
  n++;
  const tmp = copia(m);
  const r = ejecutar(tmp);
  rmSync(tmp, { recursive: true, force: true });
  const detectado = r.estado !== 0;
  const porLaRazon = r.fallan.some((f) => f.includes(m.falla));
  if (detectado && porLaRazon) console.log(`  ✓ ${m.nombre}: detectado (falla «${m.falla}»)`);
  else {
    mal++;
    console.log(`  ✗ ${m.nombre}: ${detectado ? `falló, pero no por «${m.falla}»: ${r.fallan.join(" | ")}` : "NO lo detecta la prueba"}`);
  }
}
console.log(mal ? `\n✗ ${mal} de ${n} mutantes NO detectados` : `\n✓ ${n} mutantes: todos detectados`);
process.exit(mal ? 1 : 0);
