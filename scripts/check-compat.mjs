#!/usr/bin/env node
/**
 * Verificador de compatibilidad de esquemas.
 * Contrato normativo: specification/05-compatibility.md §5
 *
 *   node scripts/check-compat.mjs            # todos los esquemas
 *   node scripts/check-compat.mjs --base HEAD~1   # solo lo que cambia el diff
 *
 * "Sin (1) y (2) automatizados, el resto de 05-compatibility.md es una sugerencia
 * amable." Esto es (1) y (2).
 *
 * Recorre los cuatro árboles de scripts/schema-rules.mjs: eventos
 * (`schemas/<dominio>/<agregado>/<evento>/…`), estado (`schemas/state/<dominio>/<agregado>/…`),
 * petición/respuesta (`schemas/rpc/<dominio>/<agregado>/<operacion>/{request,reply}/…`) y plataforma
 * (`schemas/system/…`). Las reglas de compatibilidad (05-compatibility.md §2) valen igual en todos. Un
 * esquema de estado debe además declarar `aggregateVersion` como entero `required` (≥ 1) y llevar un
 * objeto en la raíz — 10-state.md §3.4, §10 —, y un dominio reservado (`dlq`, `rpc`, y el propio `state` o
 * `system` como dominio de evento) es un error.
 *
 * Petición/respuesta (11-request-reply.md §11): una operación es un PAR —su petición y su respuesta, del mismo
 * mayor— y un esquema sin su par es un error; y la compatibilidad vale igual **con la dirección invertida
 * donde corresponde**. Un esquema describe datos que fluyen de quien ESCRIBE a quien LEE. En la RESPUESTA
 * escribe el dueño y leen los llamantes —muchos, independientes, como los consumidores de un evento—: rige
 * todo, incluido que un valor nuevo en un enum exija `x-extensible-enum`. En la PETICIÓN escriben los
 * llamantes y lee el dueño —un solo lector, que es quien publica el esquema y lo despliega—: añadir un valor
 * a un enum cerrado NO rompe a nadie, así que ahí no se exige la marca; lo demás (quitar un campo, cambiar un
 * tipo, un requerido nuevo, endurecer una restricción) rompe a los llamantes que no se han actualizado, y
 * sigue prohibido. El orden de despliegue que esto presupone: primero quien responde, después quien llama.
 */

import { readFile, readdir } from "node:fs/promises";
import { join, dirname, relative } from "node:path";
import { fileURLToPath } from "node:url";

import {
  clasifica, nombresDeEstadoValidos, problemasDeEstado, LAPIDA,
  nombresDeRpcValidos, problemasDeRpc, problemasDelError, ERROR_RPC, LADOS_RPC,
} from "./schema-rules.mjs";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const SCHEMAS = join(ROOT, "schemas");
const problemas = [];
const notas = [];

const err = (file, msg, hint) => problemas.push({ file, msg, hint });

// ─── Descubrimiento ──────────────────────────────────────────────────────────

async function walk(dir) {
  const out = [];
  for (const e of await readdir(dir, { withFileTypes: true })) {
    const p = join(dir, e.name);
    if (e.isDirectory()) out.push(...(await walk(p)));
    // bundle.json es generado por scripts/bundle-schemas.mjs, no un esquema.
    else if (e.name.endsWith(".json") && e.name !== "bundle.json") out.push(p);
  }
  return out;
}

/** La ruta, clasificada por scripts/schema-rules.mjs (evento, estado o plataforma). `null` si no
 *  sigue ninguna convención. */
function parsePath(abs) {
  const rel = relative(SCHEMAS, abs).replace(/\\/g, "/");
  const meta = clasifica(rel);
  return meta.tipo === "fuera" ? null : meta;
}

// ─── Comparación de compatibilidad ───────────────────────────────────────────

/** Aplana un JSON Schema a un mapa ruta → propiedades relevantes. */
function flatten(schema, path = "", out = new Map()) {
  if (!schema || typeof schema !== "object") return out;
  const required = new Set(schema.required ?? []);
  for (const [name, sub] of Object.entries(schema.properties ?? {})) {
    const p = path ? `${path}.${name}` : name;
    out.set(p, {
      type: sub.type,
      required: required.has(name),
      enum: sub.enum,
      extensibleEnum: sub["x-extensible-enum"] === true,
      maxLength: sub.maxLength,
      minimum: sub.minimum,
      maximum: sub.maximum,
      pattern: sub.pattern,
      deprecated: sub.deprecated === true,
    });
    flatten(sub, p, out);
    if (sub.items) flatten(sub.items, `${p}[]`, out);
  }
  return out;
}

/**
 * Reglas de 05-compatibility.md §2. `lado` es `"request"` o `"reply"` en un esquema rpc (11-request-reply.md §11):
 * en una PETICIÓN el lector es el dueño, que se despliega con el esquema, así que añadir un valor a un enum cerrado
 * no exige `x-extensible-enum`. Cualquier otro esquema (`undefined`) y toda RESPUESTA lo exigen.
 */
function checkBackward(viejo, nuevo, file, lado) {
  const a = flatten(viejo);
  const b = flatten(nuevo);

  for (const [campo, vo] of a) {
    const nu = b.get(campo);

    if (!nu) {
      err(file, `campo eliminado: ${campo}`,
        "El consumidor viejo lee undefined y falla o, peor, calcula mal en silencio. Requiere MAYOR nuevo");
      continue;
    }
    if (JSON.stringify(vo.type) !== JSON.stringify(nu.type)) {
      err(file, `tipo cambiado en ${campo}: ${vo.type} → ${nu.type}`,
        "Incluye \"9990\" → 9990. Requiere MAYOR nuevo");
    }
    if (!vo.required && nu.required) {
      err(file, `${campo} pasó de opcional a requerido`,
        "Los productores viejos no lo envían. Requiere MAYOR nuevo");
    }
    for (const [prop, cmp] of [
      ["maxLength", (x, y) => y < x],
      ["minimum", (x, y) => y > x],
      ["maximum", (x, y) => y < x],
    ]) {
      if (vo[prop] !== undefined && nu[prop] !== undefined && cmp(vo[prop], nu[prop])) {
        err(file, `restricción endurecida en ${campo}: ${prop} ${vo[prop]} → ${nu[prop]}`,
          "Invalida datos ya emitidos. Requiere MAYOR nuevo");
      }
    }
    if (vo.pattern !== undefined && nu.pattern !== undefined && vo.pattern !== nu.pattern) {
      notas.push(`${file}: pattern cambiado en ${campo} — revisa a mano si es más restrictivo`);
    }
    // Introducir un enum donde no lo había es un ESTRECHAMIENTO: el campo pasa de
    // aceptar cualquier valor de su tipo a aceptar una lista cerrada, y todo dato ya
    // emitido fuera de esa lista deja de validar. Es la misma clase de ruptura que
    // bajar un maxLength, aunque no lo parezca.
    if (!vo.enum && nu.enum) {
      err(file, `enum introducido en ${campo}: [${nu.enum.join(", ")}]`,
        "El campo aceptaba cualquier valor de su tipo y ahora acepta una lista cerrada: los datos ya emitidos fuera de ella dejan de validar. Requiere MAYOR nuevo");
    }

    // La trampa de los enums: 05-compatibility.md §2.3
    if (vo.enum && nu.enum) {
      const quitados = vo.enum.filter((v) => !nu.enum.includes(v));
      const añadidos = nu.enum.filter((v) => !vo.enum.includes(v));
      if (quitados.length) {
        err(file, `valores de enum eliminados en ${campo}: ${quitados.join(", ")}`,
          "Requiere MAYOR nuevo");
      }
      if (añadidos.length && !nu.extensibleEnum && lado !== "request") {
        err(file, `valores añadidos a un enum CERRADO en ${campo}: ${añadidos.join(", ")}`,
          "Un consumidor con switch exhaustivo se rompe. Marca \"x-extensible-enum\": true o sube el MAYOR — 05-compatibility.md §2.3");
      }
    }
  }

  for (const [campo, nu] of b) {
    if (!a.has(campo) && nu.required) {
      err(file, `campo requerido nuevo: ${campo}`,
        "Los productores viejos no lo envían. Los campos añadidos deben ser opcionales");
    }
  }
}

// ─── Main ────────────────────────────────────────────────────────────────────

const files = await walk(SCHEMAS).catch(() => []);
if (files.length === 0) {
  console.log("No hay esquemas en schemas/.");
  process.exit(0);
}

const porMajor = new Map();

for (const abs of files) {
  const meta = parsePath(abs);
  if (!meta) {
    err(relative(ROOT, abs).replace(/\\/g, "/"), "ruta fuera de convención",
      "Formato: schemas/<dominio>/<agregado>/<evento>/<major>.<minor>.<patch>.json (evento), " +
      "schemas/state/<dominio>/<agregado>/<major>.<minor>.<patch>.json (estado) o " +
      "schemas/rpc/<dominio>/<agregado>/<operacion>/{request,reply}/<major>.<minor>.<patch>.json (petición/respuesta)");
    continue;
  }
  if (meta.tipo === "rpc-mal-ubicado") {
    err(meta.rel, meta.motivo,
      "Una operación de petición/respuesta son DOS esquemas —su petición y su respuesta—: schemas/rpc/<dominio>/<agregado>/<operacion>/{request,reply}/<major>.<minor>.<patch>.json (11-request-reply.md §11)");
    continue;
  }
  if (meta.tipo === "reservado") {
    err(meta.rel, meta.motivo,
      "Un dominio reservado colisiona con un espacio de nombres del protocolo en el broker (subjects overlap, A-43). Nombra el bounded context real");
    continue;
  }

  let schema;
  try {
    schema = JSON.parse(await readFile(abs, "utf8"));
  } catch (e) {
    err(meta.rel, `JSON inválido: ${e.message}`, null);
    continue;
  }

  // Un esquema de ESTADO: los nombres van en un subject y `data` lleva la versión (10-state.md §10).
  if (meta.tipo === "estado") {
    for (const p of [...nombresDeEstadoValidos(meta), ...problemasDeEstado(schema)]) err(meta.rel, p.msg, p.hint);
  } else if (meta.tipo === "rpc") {
    // Los nombres van en un subject y `data` es un objeto (11-request-reply.md §11).
    for (const p of [...nombresDeRpcValidos(meta), ...problemasDeRpc(schema, meta.lado)]) err(meta.rel, p.msg, p.hint);
  } else if (meta.rel === LAPIDA.ruta) {
    for (const p of problemasDeEstado(schema)) err(meta.rel, `la lápida: ${p.msg}`, p.hint);
  } else if (meta.rel === ERROR_RPC.ruta) {
    for (const p of problemasDelError(schema)) err(meta.rel, `el error de rpc: ${p.msg}`, p.hint);
  }

  if (schema.$id !== meta.expectedId) {
    err(meta.rel, `$id no coincide con la ruta`,
      `Es "${schema.$id}", debería ser "${meta.expectedId}". Un dataschema que no resuelve deja el evento sin contrato verificable`);
  }
  if (schema.type !== "object") {
    err(meta.rel, `type es "${schema.type}", debe ser "object"`,
      "Un array o escalar en la raíz impide añadir campos sin romper el esquema — 01-envelope.md §4");
  }
  if (schema.additionalProperties !== false) {
    err(meta.rel, "falta additionalProperties: false",
      "Sin esto, un campo mal escrito pasa la validación en silencio");
  }
  if (!schema.description) {
    notas.push(`${meta.rel}: sin description — el catálogo de eventos queda mudo`);
  }
  for (const [name, sub] of Object.entries(schema.properties ?? {})) {
    if (/_/.test(name)) {
      err(meta.rel, `propiedad "${name}" usa snake_case`,
        "El payload va en camelCase — 01-envelope.md §4");
    }
    if (/(^|[^a-z])(euros?|dolares?|precio|importe|total)$/i.test(name) && sub.type === "number") {
      err(meta.rel, `"${name}" es number y parece monetario`,
        "Los importes van como ENTERO en unidad mínima (céntimos) + moneda ISO 4217. Un float pierde precisión — 01-envelope.md §4");
    }
  }

  const lista = porMajor.get(meta.key) ?? [];
  lista.push({ meta, schema });
  porMajor.set(meta.key, lista);
}

// Con algún contrato de estado, la lápida es obligatoria: `delete` valida contra su URI fija
// (10-state.md §4). `bundle-schemas.mjs` lo comprueba también, pero este paso corre antes en CI.
const hayEstado = [...porMajor.values()].some((l) => l[0].meta.tipo === "estado");
if (hayEstado && !files.some((f) => relative(SCHEMAS, f).replace(/\\/g, "/") === LAPIDA.ruta)) {
  err(`schemas/${LAPIDA.ruta}`, "falta el esquema de la lápida",
    "Hay contratos de estado y `delete` valida contra esta URI fija — 10-state.md §4");
}

// Con alguna operación rpc, el esquema del error es obligatorio: toda respuesta de error valida contra su URI fija
// (11-request-reply.md §3.3). `bundle-schemas.mjs` lo comprueba también, pero este paso corre antes en CI.
const hayRpc = [...porMajor.values()].some((l) => l[0].meta.tipo === "rpc");
if (hayRpc && !files.some((f) => relative(SCHEMAS, f).replace(/\\/g, "/") === ERROR_RPC.ruta)) {
  err(`schemas/${ERROR_RPC.ruta}`, "falta el esquema del error de rpc",
    "Hay operaciones rpc y toda respuesta de error valida contra esta URI fija — 11-request-reply.md §3.3");
}

// Una operación rpc es un PAR: su petición y su respuesta, del mismo mayor. Un lado sin el otro deja a quien llama sin
// contrato para lo que recibe, o a quien atiende sin contrato para lo que debe rechazar.
{
  const lados = new Map();   // `<dominio>.<agregado>.v<major>.<operacion>` → { request?, reply?, rel }
  for (const lista of porMajor.values()) {
    const { meta } = lista[0];
    if (meta.tipo !== "rpc") continue;
    const e = lados.get(meta.operation) ?? { rel: meta.rel };
    e[meta.lado] = true;
    lados.set(meta.operation, e);
  }
  const nombre = (l) => (l === "request" ? "petición" : "respuesta");
  for (const [operation, e] of lados) {
    for (const lado of LADOS_RPC) {
      if (e[lado]) continue;
      const otro = LADOS_RPC.find((l) => l !== lado);
      const [dom, agr, v, op] = operation.split(".");
      err(e.rel, `la operación ${operation} tiene esquema de ${nombre(otro)} pero no de ${nombre(lado)}`,
        `Una operación de petición/respuesta es un PAR del mismo mayor: añade schemas/rpc/${dom}/${agr}/${op}/${lado}/${v.slice(1)}.<minor>.<patch>.json — 11-request-reply.md §11`);
    }
  }
}

// Compatibilidad entre versiones consecutivas del mismo mayor (y, en rpc, del mismo lado: la petición y la respuesta
// de una operación evolucionan por separado y en direcciones opuestas).
for (const [key, versiones] of porMajor) {
  versiones.sort((a, b) =>
    a.meta.minor - b.meta.minor || a.meta.patch - b.meta.patch);
  for (let i = 1; i < versiones.length; i++) {
    checkBackward(versiones[i - 1].schema, versiones[i].schema, versiones[i].meta.rel, versiones[i].meta.lado);
  }
  if (versiones.length > 1) {
    notas.push(
      `${key}: ${versiones.length} versiones (${versiones.map((v) => `${v.meta.major}.${v.meta.minor}.${v.meta.patch}`).join(" → ")})`,
    );
  }
}

// ─── Informe ─────────────────────────────────────────────────────────────────

const C = process.stdout.isTTY && !process.env.NO_COLOR;
const red = (s) => (C ? `\x1b[31m${s}\x1b[0m` : s);
const green = (s) => (C ? `\x1b[32m${s}\x1b[0m` : s);
const gray = (s) => (C ? `\x1b[90m${s}\x1b[0m` : s);
const bold = (s) => (C ? `\x1b[1m${s}\x1b[0m` : s);

if (process.argv.includes("-v")) for (const n of notas) console.log(gray(`  i ${n}`));

if (problemas.length === 0) {
  console.log(green(`✓ ${files.length} esquema(s) conformes y compatibles`));
  process.exit(0);
}

console.log(red(bold(`\n✗ ${problemas.length} problema(s)\n`)));
for (const p of problemas) {
  console.log(`  ${red("✗")} ${bold(p.file)}`);
  console.log(`    ${p.msg}`);
  if (p.hint) console.log(gray(`    ${p.hint}`));
}
console.log(gray("\n  Ver specification/05-compatibility.md"));
process.exit(1);
