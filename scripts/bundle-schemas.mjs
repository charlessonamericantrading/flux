#!/usr/bin/env node
/**
 * Empaqueta schemas/ en un único JSON, indexado por la URI de `dataschema`.
 *
 *   node scripts/bundle-schemas.mjs           → schemas/bundle.json
 *   node scripts/bundle-schemas.mjs --check   → falla si está desactualizado (CI)
 *
 * Por qué un bundle y no resolución HTTP en tiempo de ejecución: validar en
 * `publish()` está en la ruta caliente. Una petición de red por evento es
 * inaceptable, y una caché con TTL introduce una ventana en la que dos servicios
 * validan contra versiones distintas del mismo esquema. El bundle se despliega con
 * el servicio, así que **la versión del esquema queda clavada a la versión del
 * servicio** — que es exactamente lo que `producerversion` promete poder acotar.
 */

import { readFile, writeFile, readdir } from "node:fs/promises";
import { join, dirname, relative } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const SCHEMAS = join(ROOT, "schemas");
const OUT = join(SCHEMAS, "bundle.json");
const BUNDLE_NAME = "bundle.json";

async function walk(dir) {
  const out = [];
  for (const e of await readdir(dir, { withFileTypes: true })) {
    const p = join(dir, e.name);
    if (e.isDirectory()) out.push(...(await walk(p)));
    else if (e.name.endsWith(".json") && e.name !== BUNDLE_NAME) out.push(p);
  }
  return out;
}

const SEMVER = /^(\d+)\.(\d+)\.(\d+)\.json$/;

/** Declarado antes del bucle: `const` no se hoistea, y el cortocircuito de
 *  `!prev ||` escondería el ReferenceError hasta que hubiese dos versiones. */
const cmp = (a, b) => a[0] - b[0] || a[1] - b[1] || a[2] - b[2];

const files = (await walk(SCHEMAS).catch(() => [])).sort();
const schemas = {};
/** subject → la URI de la versión más alta de su mayor. */
const bySubject = {};

for (const abs of files) {
  const rel = relative(SCHEMAS, abs).replace(/\\/g, "/");
  const [domain, aggregate, event, file] = rel.split("/");
  const m = SEMVER.exec(file ?? "");
  if (!m) continue;

  const schema = JSON.parse(await readFile(abs, "utf8"));
  const uri = `https://schemas.internal/${rel}`;
  if (schema.$id !== uri) {
    console.error(`✗ ${rel}: $id="${schema.$id}" no coincide con la ruta`);
    process.exit(1);
  }
  schemas[uri] = schema;

  const subject = `${domain}.${aggregate}.v${m[1]}.${event}`;
  const ver = [+m[1], +m[2], +m[3]];
  const prev = bySubject[subject];
  // El productor publica contra el MINOR más alto de su mayor: dentro de un mayor
  // todo es BACKWARD-compatible, así que el más alto acepta todo lo que aceptan los
  // anteriores. Ver 05-compatibility.md §2.
  if (!prev || cmp(ver, prev.v) > 0) bySubject[subject] = { uri, v: ver };
}

const bundle = {
  $comment:
    "GENERADO por scripts/bundle-schemas.mjs — no editar a mano. Indexa los JSON Schema por la URI que aparece en el atributo dataschema del CloudEvent.",
  generatedFrom: "schemas/",
  count: Object.keys(schemas).length,
  subjects: Object.fromEntries(
    Object.entries(bySubject)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([s, { uri }]) => [s, uri]),
  ),
  schemas,
};

const json = JSON.stringify(bundle, null, 2) + "\n";

if (process.argv.includes("--check")) {
  const current = await readFile(OUT, "utf8").catch(() => "");
  if (current !== json) {
    console.error("✗ schemas/bundle.json está desactualizado.\n  Ejecuta: node scripts/bundle-schemas.mjs");
    process.exit(1);
  }
  console.log(`✓ bundle.json al día (${bundle.count} esquema(s))`);
} else {
  await writeFile(OUT, json, "utf8");
  console.log(`✓ schemas/bundle.json — ${bundle.count} esquema(s), ${Object.keys(bySubject).length} subject(s)`);
}
