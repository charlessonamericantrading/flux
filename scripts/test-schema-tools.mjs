#!/usr/bin/env node
/**
 * Pruebas EN NEGATIVO de `bundle-schemas.mjs` y `check-compat.mjs` para los esquemas de petición/respuesta
 * (specification/11-request-reply.md §11).
 *
 *   node scripts/test-schema-tools.mjs
 *
 * Una comprobación que solo se ha visto en verde no demuestra nada. Este script copia `scripts/` y `schemas/` a un
 * directorio temporal, escribe en él esquemas ROTOS —una petición sin su respuesta, un `data` que no es un objeto, un
 * dominio reservado, un esquema suelto…— y ejecuta los dos scripts: cada caso DEBE fallar (o pasar, donde se dice) por la
 * razón que se dice. No toca el repositorio. Cubre también la inversión de la compatibilidad:
 *
 *   · en una PETICIÓN, añadir un valor a un enum cerrado NO exige `x-extensible-enum` (el lector es el dueño);
 *   · en una RESPUESTA sí (leen los llamantes);
 *   · un campo requerido nuevo o un campo eliminado siguen prohibidos en los dos lados.
 */

import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync, cpSync, renameSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const TMP = mkdtempSync(join(tmpdir(), "flux-schema-tools-"));
const S = (r) => join(TMP, "schemas", r);

const sch = (ruta, extra = {}, props = { id: { type: "string" } }, req = ["id"]) => ({
  $schema: "https://json-schema.org/draft/2020-12/schema",
  $id: `https://schemas.internal/${ruta}`,
  description: "prueba",
  type: "object",
  additionalProperties: false,
  required: req,
  properties: props,
  ...extra,
});
const escribe = (ruta, obj) => { mkdirSync(dirname(S(ruta)), { recursive: true }); writeFileSync(S(ruta), JSON.stringify(obj, null, 2) + "\n"); };
const corre = (script) => spawnSync("node", [join(TMP, "scripts", script), ...(script === "bundle-schemas.mjs" ? ["--check"] : [])], { cwd: TMP, encoding: "utf8" });

let fallos = 0;
let total = 0;

/** @param esperado `{ bundle?: "pasa" | "falla:<texto>", compat?: … }` */
function caso(nombre, prepara, esperado) {
  rmSync(join(TMP, "schemas"), { recursive: true, force: true });
  cpSync(join(ROOT, "schemas"), join(TMP, "schemas"), { recursive: true });
  prepara();
  for (const [script, esp] of [["bundle-schemas.mjs", esperado.bundle], ["check-compat.mjs", esperado.compat]]) {
    if (!esp) continue;
    total++;
    const r = corre(script);
    const t = `${r.stdout}${r.stderr}`;
    const [modo, texto] = esp.split(/:(.*)/s);
    // `bundle --check` falla también si el bundle está desactualizado: «pasa» es que NO falló por validación.
    const ok = modo === "pasa" ? r.status === 0 || (script === "bundle-schemas.mjs" && /desactualizado/.test(t)) : r.status !== 0 && t.includes(texto);
    if (!ok) { fallos++; console.log(`  ✗ ${nombre} — ${script}: esperaba ${esp}\n${t.split("\n").map((l) => "      " + l).join("\n")}`); }
    else console.log(`  ✓ ${nombre} — ${script}: ${modo === "pasa" ? "pasa" : `falla por «${texto}»`}`);
  }
}

const D = "rpc/zzprueba/agregado";
const par = (op = "crear", v = "1.0.0", extra = {}) => {
  for (const lado of ["request", "reply"]) escribe(`${D}/${op}/${lado}/${v}.json`, sch(`${D}/${op}/${lado}/${v}.json`, extra));
};
const conEnum = (ruta, valores, extensible = false) => sch(ruta, {}, { id: { type: "string" }, estado: { type: "string", enum: valores, ...(extensible ? { "x-extensible-enum": true } : {}) } });

try {
  mkdirSync(join(TMP, "scripts"), { recursive: true });
  cpSync(join(ROOT, "scripts"), join(TMP, "scripts"), { recursive: true });

  console.log("El par y la ubicación");
  caso("petición sin su respuesta", () => escribe(`${D}/crear/request/1.0.0.json`, sch(`${D}/crear/request/1.0.0.json`)),
    { bundle: "falla:tiene esquema de petición pero no de respuesta", compat: "falla:tiene esquema de petición pero no de respuesta" });
  caso("respuesta sin su petición", () => escribe(`${D}/crear/reply/1.0.0.json`, sch(`${D}/crear/reply/1.0.0.json`)),
    { bundle: "falla:tiene esquema de respuesta pero no de petición", compat: "falla:tiene esquema de respuesta pero no de petición" });
  caso("el par es del mismo mayor: petición v1 y respuesta v2", () => {
    escribe(`${D}/crear/request/1.0.0.json`, sch(`${D}/crear/request/1.0.0.json`));
    escribe(`${D}/crear/reply/2.0.0.json`, sch(`${D}/crear/reply/2.0.0.json`));
  }, { bundle: "falla:pero no de", compat: "falla:pero no de" });
  caso("el par completo pasa", () => par(), { bundle: "pasa", compat: "pasa" });
  caso("un esquema suelto en la carpeta de la operación", () => escribe(`${D}/crear/1.0.0.json`, sch(`${D}/crear/1.0.0.json`)),
    { bundle: "falla:es un PAR", compat: "falla:es un PAR" });
  caso("un .json de rpc que no es semver", () => escribe(`${D}/crear/request/v1.json`, sch(`${D}/crear/request/v1.json`)),
    { bundle: "falla:no es <major>.<minor>.<patch>.json", compat: "falla:no es <major>.<minor>.<patch>.json" });
  caso("un dominio reservado dentro de rpc/", () => escribe("rpc/state/agregado/op/request/1.0.0.json", sch("rpc/state/agregado/op/request/1.0.0.json")),
    { bundle: "falla:está reservado", compat: "falla:está reservado" });
  caso("una operación que no es kebab-case", () => par("Crear_Algo"), { bundle: "falla:no es kebab-case", compat: "falla:no es kebab-case" });

  console.log("La forma");
  caso("`data` que no es un objeto en la raíz", () => {
    escribe(`${D}/crear/request/1.0.0.json`, { ...sch(`${D}/crear/request/1.0.0.json`), type: "array" });
    escribe(`${D}/crear/reply/1.0.0.json`, sch(`${D}/crear/reply/1.0.0.json`));
  }, { bundle: "falla:no es un objeto en la raíz", compat: "falla:no es un objeto en la raíz" });
  caso("sin el esquema del error de rpc", () => renameSync(S("system/rpc/error/1.0.0.json"), join(TMP, "error.bak")),
    { bundle: "falla:falta el esquema del error de rpc", compat: "falla:falta el esquema del error de rpc" });
  caso("un error cuya class admite poison", () => {
    const f = S("system/rpc/error/1.0.0.json");
    const e = JSON.parse(readFileSync(f, "utf8"));
    e.properties.class.enum.push("poison");
    writeFileSync(f, JSON.stringify(e, null, 2));
  }, { bundle: "falla:debe declarar `class` con enum", compat: "falla:debe declarar `class` con enum" });

  console.log("La compatibilidad, con la dirección invertida");
  caso("PETICIÓN: añadir un valor a un enum cerrado NO exige x-extensible-enum", () => {
    for (const lado of ["request", "reply"]) escribe(`${D}/crear/${lado}/1.0.0.json`, conEnum(`${D}/crear/${lado}/1.0.0.json`, ["a", "b"]));
    escribe(`${D}/crear/request/1.1.0.json`, conEnum(`${D}/crear/request/1.1.0.json`, ["a", "b", "c"]));
  }, { compat: "pasa" });
  caso("RESPUESTA: añadir un valor a un enum cerrado SÍ lo exige", () => {
    for (const lado of ["request", "reply"]) escribe(`${D}/crear/${lado}/1.0.0.json`, conEnum(`${D}/crear/${lado}/1.0.0.json`, ["a", "b"]));
    escribe(`${D}/crear/reply/1.1.0.json`, conEnum(`${D}/crear/reply/1.1.0.json`, ["a", "b", "c"]));
  }, { compat: "falla:valores añadidos a un enum CERRADO" });
  caso("RESPUESTA: con x-extensible-enum sí puede añadir un valor", () => {
    for (const lado of ["request", "reply"]) escribe(`${D}/crear/${lado}/1.0.0.json`, conEnum(`${D}/crear/${lado}/1.0.0.json`, ["a", "b"], true));
    escribe(`${D}/crear/reply/1.1.0.json`, conEnum(`${D}/crear/reply/1.1.0.json`, ["a", "b", "c"], true));
  }, { compat: "pasa" });
  caso("PETICIÓN: un campo requerido nuevo sigue prohibido (los llamantes viejos no lo envían)", () => {
    par();
    escribe(`${D}/crear/request/1.1.0.json`, sch(`${D}/crear/request/1.1.0.json`, {}, { id: { type: "string" }, nuevo: { type: "string" } }, ["id", "nuevo"]));
  }, { compat: "falla:campo requerido nuevo" });
  caso("PETICIÓN: quitar un campo sigue prohibido", () => {
    for (const lado of ["request", "reply"]) escribe(`${D}/crear/${lado}/1.0.0.json`, sch(`${D}/crear/${lado}/1.0.0.json`, {}, { id: { type: "string" }, otro: { type: "string" } }));
    escribe(`${D}/crear/request/1.1.0.json`, sch(`${D}/crear/request/1.1.0.json`));
  }, { compat: "falla:campo eliminado" });
  caso("la petición v1.1 se compara con la petición v1.0, no con la respuesta", () => {
    escribe(`${D}/crear/request/1.0.0.json`, sch(`${D}/crear/request/1.0.0.json`, {}, { id: { type: "string" }, solo: { type: "string" } }, ["id"]));
    escribe(`${D}/crear/reply/1.0.0.json`, sch(`${D}/crear/reply/1.0.0.json`));
    escribe(`${D}/crear/request/1.1.0.json`, sch(`${D}/crear/request/1.1.0.json`, {}, { id: { type: "string" }, solo: { type: "string" }, mas: { type: "string" } }, ["id"]));
  }, { compat: "pasa" });
} finally {
  rmSync(TMP, { recursive: true, force: true });
}

console.log(fallos ? `\n✗ ${fallos} de ${total} comprobaciones no se comportan como se esperaba` : `\n✓ ${total} comprobaciones: todas fallan —o pasan— por la razón que dicen`);
process.exit(fallos ? 1 : 0);
