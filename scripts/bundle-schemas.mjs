#!/usr/bin/env node
/**
 * Empaqueta schemas/ en un único JSON, indexado por la URI de `dataschema`.
 *
 *   node scripts/bundle-schemas.mjs                     → schemas/bundle.json
 *   node scripts/bundle-schemas.mjs --check             → falla si está desactualizado (CI)
 *   node scripts/bundle-schemas.mjs --schemas <dir>     → el `schemas/` de OTRO proyecto (su `<dir>/bundle.json`): lo que
 *                                                         usan `flux new` y examples/ecosistema/. Sin la opción, el de este repo.
 *
 * Por qué un bundle y no resolución HTTP en tiempo de ejecución: validar en
 * `publish()` está en la ruta caliente. Una petición de red por evento es
 * inaceptable, y una caché con TTL introduce una ventana en la que dos servicios
 * validan contra versiones distintas del mismo esquema. El bundle se despliega con
 * el servicio, así que **la versión del esquema queda clavada a la versión del
 * servicio** — que es exactamente lo que `producerversion` promete poder acotar.
 *
 * Tres índices, porque hay tres formas de nombrar un contrato (la ubicación y las reglas de cada
 * árbol están en scripts/schema-rules.mjs):
 *
 *   subjects  evento: `pedidos.pedido.v1.creado`           → URI del esquema de versión más alta de su mayor
 *   state     estado: `clientes.cliente.v1`                → ídem, de `schemas/state/…` (10-state.md §10)
 *   rpc       operación: `clientes.cliente.v1.obtener`     → { request, reply }: la URI más alta de su mayor, de cada
 *                                                             lado, de `schemas/rpc/…` (11-request-reply.md §11)
 *
 * La lápida de estado (`schemas/system/state/eliminado/…`), el error de rpc (`schemas/system/rpc/error/…`) y
 * el resto de `schemas/system/` van en `schemas`, por su URI exacta, y en NINGÚN índice: no son un subject al
 * que nadie se suscriba, y un `system.state.v1.eliminado` en `subjects` sería un evento en un dominio reservado.
 *
 * Un esquema de estado que no declare `aggregateVersion` como entero `required` (≥ 1), o cuyo `data`
 * no sea un objeto en la raíz, es un ERROR, no un aviso: sin versión, `put` no puede ser condicional.
 * Un esquema de rpc cuyo `data` no sea un objeto en la raíz, que no tenga su PAR (petición y respuesta de la
 * misma operación y el mismo mayor), o que esté fuera de `…/<operacion>/{request,reply}/`, también.
 */

import { readFile, writeFile, readdir } from "node:fs/promises";
import { join, dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  clasifica, nombresDeEstadoValidos, problemasDeEstado, LAPIDA,
  nombresDeRpcValidos, problemasDeRpc, problemasDelError, ERROR_RPC, LADOS_RPC,
} from "./schema-rules.mjs";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
/** El árbol de esquemas: el de este repo, o el que dé `--schemas <dir>` (el `schemas/` de un proyecto que usa flux). */
const iDir = process.argv.indexOf("--schemas");
if (iDir > 0 && (!process.argv[iDir + 1] || process.argv[iDir + 1].startsWith("--"))) {
  console.error("✗ --schemas necesita un directorio. Uso: node scripts/bundle-schemas.mjs [--check] [--schemas <dir>]");
  process.exit(2);
}
const SCHEMAS = iDir > 0 ? resolve(process.argv[iDir + 1]) : join(ROOT, "schemas");
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

/** Declarado antes del bucle: `const` no se hoistea, y el cortocircuito de
 *  `!prev ||` escondería el ReferenceError hasta que hubiese dos versiones. */
const cmp = (a, b) => a[0] - b[0] || a[1] - b[1] || a[2] - b[2];

const files = (await walk(SCHEMAS).catch(() => [])).sort();
const schemas = {};
/** subject → la URI de la versión más alta de su mayor. */
const bySubject = {};
/** contrato de estado → ídem. */
const byState = {};
/** operación rpc → { request, reply }: la versión más alta de su mayor, de cada lado (con la ruta de algún fichero, para los errores). */
const byRpc = {};
const errores = [];

const falla = (rel, msg, hint) => errores.push(`✗ ${rel}: ${msg}${hint ? `\n    ${hint}` : ""}`);

for (const abs of files) {
  const rel = relative(SCHEMAS, abs).replace(/\\/g, "/");
  const meta = clasifica(rel);
  if (meta.tipo === "fuera") continue;
  if (meta.tipo === "rpc-mal-ubicado") {
    falla(rel, meta.motivo, "Una operación de petición/respuesta son DOS esquemas —su petición y su respuesta—: schemas/rpc/<dominio>/<agregado>/<operacion>/{request,reply}/<major>.<minor>.<patch>.json (11-request-reply.md §11)");
    continue;
  }
  if (meta.tipo === "reservado") {
    falla(rel, meta.motivo, "Un dominio reservado colisiona con un espacio de nombres del protocolo en el broker (A-43). Nombra el bounded context real");
    continue;
  }

  const schema = JSON.parse(await readFile(abs, "utf8"));
  if (schema.$id !== meta.expectedId) {
    falla(rel, `$id="${schema.$id}" no coincide con la ruta`);
    continue;
  }
  schemas[meta.expectedId] = schema;

  const ver = [meta.major, meta.minor, meta.patch];
  // El productor publica contra el MINOR más alto de su mayor: dentro de un mayor
  // todo es BACKWARD-compatible, así que el más alto acepta todo lo que aceptan los
  // anteriores. Ver 05-compatibility.md §2.
  if (meta.tipo === "evento") {
    const prev = bySubject[meta.subject];
    if (!prev || cmp(ver, prev.v) > 0) bySubject[meta.subject] = { uri: meta.expectedId, v: ver };
  } else if (meta.tipo === "estado") {
    for (const p of [...nombresDeEstadoValidos(meta), ...problemasDeEstado(schema)]) falla(rel, p.msg, p.hint);
    const prev = byState[meta.contrato];
    if (!prev || cmp(ver, prev.v) > 0) byState[meta.contrato] = { uri: meta.expectedId, v: ver };
  } else if (meta.tipo === "rpc") {
    for (const p of [...nombresDeRpcValidos(meta), ...problemasDeRpc(schema, meta.lado)]) falla(rel, p.msg, p.hint);
    const op = (byRpc[meta.operation] ??= { rel });
    const prev = op[meta.lado];
    if (!prev || cmp(ver, prev.v) > 0) op[meta.lado] = { uri: meta.expectedId, v: ver };
  } else if (meta.rel === LAPIDA.ruta) {
    // La lápida lleva versión como cualquier estado: `put` la lee para decidir si una escritura es stale.
    for (const p of problemasDeEstado(schema)) falla(rel, `la lápida: ${p.msg}`, p.hint);
  } else if (meta.rel === ERROR_RPC.ruta) {
    for (const p of problemasDelError(schema)) falla(rel, `el error de rpc: ${p.msg}`, p.hint);
  }
}

// Una operación es un PAR: sin su respuesta, quien llama no tiene contrato contra el que validar lo que recibe; sin su
// petición, quien atiende no puede rechazar lo que no cumple. Falla por cada operación (y mayor) a la que le falte un lado.
for (const [operation, lados] of Object.entries(byRpc)) {
  const [dom, agr, v, op] = operation.split(".");
  for (const lado of LADOS_RPC) {
    if (lados[lado]) continue;
    const otro = LADOS_RPC.find((l) => l !== lado);
    const nombre = (l) => (l === "request" ? "petición" : "respuesta");
    falla(lados.rel, `la operación ${operation} tiene esquema de ${nombre(otro)} pero no de ${nombre(lado)}`,
      `Una operación de petición/respuesta es un PAR del mismo mayor: añade schemas/rpc/${dom}/${agr}/${op}/${lado}/${v.slice(1)}.<minor>.<patch>.json — 11-request-reply.md §11`);
  }
}

// Con algún contrato de estado, la lápida es obligatoria: `delete` valida contra ella y su URI es fija.
if (Object.keys(byState).length && !schemas[LAPIDA.uri]) {
  falla(`schemas/${LAPIDA.ruta}`, "falta el esquema de la lápida", "Hay contratos de estado y `delete` valida contra esta URI fija (10-state.md §4)");
}
// Con alguna operación rpc, el esquema del error es obligatorio: el que responde valida contra su URI fija.
if (Object.keys(byRpc).length && !schemas[ERROR_RPC.uri]) {
  falla(`schemas/${ERROR_RPC.ruta}`, "falta el esquema del error de rpc", "Hay operaciones rpc y toda respuesta de error valida contra esta URI fija (11-request-reply.md §3.3)");
}

if (errores.length) {
  console.error(errores.join("\n"));
  process.exit(1);
}

const ordenado = (obj) =>
  Object.fromEntries(
    Object.entries(obj)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, { uri }]) => [k, uri]),
  );
const ordenadoRpc = (obj) =>
  Object.fromEntries(
    Object.entries(obj)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, lados]) => [k, { request: lados.request.uri, reply: lados.reply.uri }]),
  );

const bundle = {
  $comment:
    "GENERADO por scripts/bundle-schemas.mjs — no editar a mano. Indexa los JSON Schema por la URI que aparece en el atributo dataschema del CloudEvent. `subjects`, `state` y `rpc` resuelven el esquema de versión más alta de un contrato de evento, de estado y de una operación de petición/respuesta (esta, uno por lado: { request, reply }) respectivamente.",
  generatedFrom: "schemas/",
  count: Object.keys(schemas).length,
  subjects: ordenado(bySubject),
  state: ordenado(byState),
  rpc: ordenadoRpc(byRpc),
  schemas,
};

const json = JSON.stringify(bundle, null, 2) + "\n";

if (process.argv.includes("--check")) {
  const current = await readFile(OUT, "utf8").catch(() => "");
  if (current !== json) {
    const rel = relative(process.cwd(), OUT).replace(/\\/g, "/") || "bundle.json";
    console.error(`✗ ${rel} está desactualizado.\n  Ejecuta: node scripts/bundle-schemas.mjs${iDir > 0 ? ` --schemas ${process.argv[iDir + 1]}` : ""}`);
    process.exit(1);
  }
  console.log(`✓ bundle.json al día (${bundle.count} esquema(s))`);
} else {
  await writeFile(OUT, json, "utf8");
  console.log(
    `✓ ${relative(process.cwd(), OUT).replace(/\\/g, "/")} — ${bundle.count} esquema(s), ${Object.keys(bySubject).length} subject(s), ${Object.keys(byState).length} contrato(s) de estado, ${Object.keys(byRpc).length} operación(es) rpc`,
  );
}
