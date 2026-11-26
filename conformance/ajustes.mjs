#!/usr/bin/env node
/**
 * Runner de los AJUSTES: ejecuta `ajustesVectors` (conformance/harness/vectors.json) contra los arneses de los SDKs de
 * `ajustesSdks` —hoy Node y Rust— y comprueba que dan lo DECIDIDO y lo mismo.
 *
 *   node conformance/ajustes.mjs
 *   node conformance/ajustes.mjs --only node,rust
 *   FLUX_HARNESS_RUST=sdk-rust/target/debug/conformance-harness node conformance/ajustes.mjs     # un binario ya compilado
 *
 * Es un runner APARTE de `cross-sdk.mjs` a propósito: los ajustes son de operaciones de DOS capacidades (estado y rpc) y no de una, y
 * necesitan `dataTexto` (que `data` llegue como TEXTO CRUDO: con espacios, escapes o una clave repetida, que un objeto de vectors.json
 * no puede expresar). Cuando `cross-sdk.mjs` lo absorba (ver conformance/harness/README.md → «Ajustes»), este fichero se borra.
 *
 * Cada vector dice qué espera: `sha256` (los bytes DECIDIDOS; los que coinciden con la spec llevan su SHA-256), `expectSubject`,
 * `expect` (el código estable del rechazo) o `differsFrom` (control: los bytes DEBEN ser distintos de los de otro vector).
 * Sale con 0 si todo coincide, 1 si no, 2 si un vector está mal formado.
 */
import { readFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const RAIZ = join(dirname(fileURLToPath(import.meta.url)), "..");
const args = process.argv.slice(2);
const soloIdx = args.indexOf("--only");
const solo = soloIdx >= 0 ? new Set(args[soloIdx + 1].split(",")) : null;
const C = process.stdout.isTTY && !process.env.NO_COLOR;
const col = (n) => (s) => (C ? `\x1b[${n}m${s}\x1b[0m` : String(s));
const [bold, red, green, gray, yellow] = [1, 31, 32, 90, 33].map(col);

const vectores = JSON.parse(await readFile(join(RAIZ, "conformance/harness/vectors.json"), "utf8"));
const registro = JSON.parse(await readFile(join(RAIZ, "conformance/harnesses.json"), "utf8")).harnesses;
const sha256 = (b64) => createHash("sha256").update(Buffer.from(b64, "base64")).digest("hex");

const OPS = new Set([
  "state-build", "state-tombstone", "state-subject", "state-dlq",
  "rpc-request", "rpc-reply", "rpc-error", "rpc-operation", "rpc-timeout",
]);

/** Lo que se manda al arnés. `dataTexto` se inyecta al final del objeto SIN pasar por `JSON.stringify` (ver arriba). */
function textoDeEntrada(entrada) {
  if (entrada.dataTexto === undefined) return JSON.stringify(entrada);
  const { dataTexto, ...resto } = entrada;
  return `${JSON.stringify(resto).slice(0, -1)},"data":${dataTexto}}`;
}

function invocar(h, entrada) {
  const rust = h.sdk === "rust" && process.env.FLUX_HARNESS_RUST;
  return new Promise((res) => {
    const p = spawn(rust || h.cmd, rust ? [] : h.args, { cwd: join(RAIZ, h.cwd ?? ".") });
    let out = "", err = "";
    p.stdout.on("data", (d) => (out += d));
    p.stderr.on("data", (d) => (err += d));
    p.on("error", (e) => res({ __roto: `no se pudo lanzar: ${e.message}` }));
    p.on("close", (code) => {
      if (code !== 0) return res({ __roto: `exit ${code}: ${err.trim().slice(0, 300)}` });
      try { res(JSON.parse(out)); } catch { res({ __roto: `stdout no es JSON: ${out.trim().slice(0, 200)}` }); }
    });
    p.stdin.end(textoDeEntrada(entrada));
  });
}

const sdks = (vectores.ajustesSdks ?? []).filter((s) => !solo || solo.has(s));
const arneses = sdks.map((s) => registro.find((h) => h.sdk === s));
if (!vectores.ajustesVectors?.length || !sdks.length || arneses.some((h) => !h)) {
  console.error(red("no hay vectores de ajustes, o un SDK de `ajustesSdks` no tiene arnés registrado en conformance/harnesses.json"));
  process.exit(2);
}

const ids = new Set();
for (const v of vectores.ajustesVectors) {
  const problemas = [];
  if (!OPS.has(v.op)) problemas.push(`op "${v.op}" no es una operación de estado ni de petición/respuesta`);
  if (ids.has(v.id)) problemas.push("id repetido");
  if ([v.sha256, v.expect, v.expectSubject, v.differsFrom].filter((x) => x !== undefined).length !== 1) problemas.push("debe decir UNA cosa: sha256, expect, expectSubject o differsFrom");
  if (v.differsFrom !== undefined && !ids.has(v.differsFrom)) problemas.push(`\`differsFrom\` apunta a "${v.differsFrom}", que no existe antes que él`);
  if (problemas.length) {
    console.error(red(`vector de ajustes "${v.id}" mal formado: ${problemas.join("; ")}`));
    process.exit(2);
  }
  ids.add(v.id);
}

console.log(bold(`\nAjustes de la spec — ${vectores.ajustesVectors.length} vectores × ${sdks.length} SDK(s): ${sdks.join(", ")}\n`));

const fallos = [];
const bytesPorVector = new Map(); // id → Map(sdk → bytes)
for (const v of vectores.ajustesVectors) {
  const entrada = { op: v.op, ...v.input, ...(v.dataTexto !== undefined && { dataTexto: v.dataTexto }) };
  const rs = await Promise.all(arneses.map((h) => invocar(h, entrada)));
  const marcas = [];
  bytesPorVector.set(v.id, new Map());
  rs.forEach((r, i) => {
    const sdk = sdks[i];
    let ok, detalle = "";
    if (r.__roto) { ok = false; detalle = `arnés roto: ${r.__roto}`; }
    else if (r.ok === false && r.code === "UNSUPPORTED_OP") { ok = false; detalle = `su arnés no implementa ${v.op} (UNSUPPORTED_OP)`; }
    else if (v.expect !== undefined) { ok = r.ok === false && r.code === v.expect; if (!ok) detalle = `esperaba code="${v.expect}", obtuvo ${r.ok ? "ok:true (¡no lo rechazó!)" : `code="${r.code}"`}`; }
    else if (v.expectSubject !== undefined) { ok = r.ok === true && r.subject === v.expectSubject; if (!ok) detalle = r.ok ? `subject ${JSON.stringify(r.subject)}, esperaba ${JSON.stringify(v.expectSubject)}` : `lo rechazó: code="${r.code}"`; }
    else {
      if (r.ok) bytesPorVector.get(v.id).set(sdk, r.bytes);
      if (v.sha256 !== undefined) { ok = r.ok === true && sha256(r.bytes) === v.sha256; if (!ok) detalle = r.ok ? `sha256 ${sha256(r.bytes)}, y el vector fija ${v.sha256}` : `la operación falló: ${r.code} ${r.detail ?? ""}`; }
      else {
        const otro = bytesPorVector.get(v.differsFrom)?.get(sdk);
        ok = r.ok === true && otro !== undefined && r.bytes !== otro;
        if (!ok) detalle = `CONTROL: los bytes son IGUALES a los de "${v.differsFrom}" y deberían diferir`;
      }
    }
    marcas.push(ok ? green("✓") : red("✗"));
    if (!ok) fallos.push({ v, sdk, detalle });
  });
  const que = v.expect !== undefined ? `→ ${v.expect}` : v.expectSubject !== undefined ? `→ ${v.expectSubject}` : v.sha256 !== undefined ? `→ sha256 ${v.sha256.slice(0, 12)}…` : `→ distinto de ${v.differsFrom}`;
  console.log(`  ${marcas.join(" ")}  ${v.id} ${gray(que)}`);
}
console.log(gray(`\n  ${sdks.join("  ")}`));

if (fallos.length) {
  console.log(red(bold(`\n✗ ${fallos.length} fallo(s)\n`)));
  for (const f of fallos) {
    console.log(`  ${red("✗")} ${bold(f.sdk)} · ${f.v.id}  ${gray(f.v.spec ?? "")}\n    ${f.detalle}`);
    if (f.v.why) console.log(gray(`    ${f.v.why}`));
  }
  process.exit(1);
}
console.log(green(bold(`\n✓ ${vectores.ajustesVectors.length} vectores de ajustes: ${sdks.join(" y ")} dan lo decidido`)));
