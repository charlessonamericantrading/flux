#!/usr/bin/env node
/**
 * Runner de conformidad cruzada.
 *
 * Ejecuta los mismos vectores contra el arnés de cada SDK y compara las salidas
 * **byte a byte**. El SDK de referencia (Node) fija el resultado esperado; los demás
 * deben coincidir exactamente.
 *
 *   node conformance/cross-sdk.mjs
 *   node conformance/cross-sdk.mjs --only rust,php
 *   node conformance/cross-sdk.mjs --verbose      (muestra el primer byte que difiere)
 *
 * No necesita broker: esto ejercita envelope, firma y parseo, no NATS.
 */

import { readFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const AQUI = dirname(fileURLToPath(import.meta.url));
const RAIZ = join(AQUI, "..");

const args = process.argv.slice(2);
const verbose = args.includes("--verbose") || args.includes("-v");
const soloIdx = args.indexOf("--only");
const solo = soloIdx >= 0 ? new Set(args[soloIdx + 1].split(",")) : null;

const C = process.stdout.isTTY && !process.env.NO_COLOR;
const col = (n) => (s) => (C ? `\x1b[${n}m${s}\x1b[0m` : String(s));
const [bold, red, green, gray, yellow, cyan] = [1, 31, 32, 90, 33, 36].map(col);

const vectores = JSON.parse(await readFile(join(AQUI, "harness/vectors.json"), "utf8"));
const claves = JSON.parse(await readFile(join(AQUI, "harness/keys.json"), "utf8"));
const registro = JSON.parse(await readFile(join(AQUI, "harnesses.json"), "utf8"));

/** Invoca un arnés con una operación. Devuelve el objeto de salida o un error. */
function invocar(harness, entrada) {
  return new Promise((res) => {
    const p = spawn(harness.cmd, harness.args, {
      cwd: join(RAIZ, harness.cwd ?? "."),
      shell: harness.shell ?? false,
    });
    let out = "", err = "";
    p.stdout.on("data", (d) => (out += d));
    p.stderr.on("data", (d) => (err += d));
    p.on("error", (e) => res({ __arnesRoto: `no se pudo lanzar: ${e.message}` }));
    p.on("close", (code) => {
      // Exit != 0 significa que el ARNÉS está roto, no que el caso falló: el contrato
      // dice que un fallo de operación se reporta en el JSON con exit 0.
      if (code !== 0) return res({ __arnesRoto: `exit ${code}: ${err.trim().slice(0, 300)}` });
      try {
        res(JSON.parse(out));
      } catch {
        res({ __arnesRoto: `stdout no es JSON: ${out.trim().slice(0, 200)}` });
      }
    });
    p.stdin.end(JSON.stringify(entrada));
  });
}

/** Primer byte en que difieren dos base64, con su contexto legible. */
function primeraDiferencia(a, b) {
  const ba = Buffer.from(a, "base64").toString("utf8");
  const bb = Buffer.from(b, "base64").toString("utf8");
  let i = 0;
  while (i < ba.length && i < bb.length && ba[i] === bb[i]) i++;
  const ctx = (s) => JSON.stringify(s.slice(Math.max(0, i - 30), i + 40));
  return `  byte ${i}\n    referencia ${ctx(ba)}\n    este SDK   ${ctx(bb)}`;
}

// ─── Construir la lista de operaciones ───────────────────────────────────────

const operaciones = [];

for (const v of vectores.vectors) {
  const entrada = { op: v.op, event: v.event };
  if (v.dlq) entrada.dlq = v.dlq;
  if (v.op === "sign" || v.signFirst) {
    entrada.signing = { privateKeyPem: claves.privateKeyPem, keyId: claves.keyId };
    if (v.signFirst) entrada.signFirst = true;
  }
  operaciones.push({ id: v.id, spec: v.spec, why: v.why, entrada, tipo: "bytes" });
}

// Los vectores POISON se construyen mutando un envelope válido y comparando el CÓDIGO,
// no los bytes: lo que se verifica es que los siete SDKs clasifiquen igual.
const baseValida = vectores.vectors[0].event;
for (const p of vectores.poisonVectors) {
  let raw;
  if (p.rawUtf8 !== undefined) {
    raw = Buffer.from(p.rawUtf8, "utf8").toString("base64");
  } else {
    const ref = await invocar(registro.harnesses.find((h) => h.reference), {
      op: "build",
      event: baseValida,
    });
    if (ref.__arnesRoto || !ref.ok) {
      console.error(red(`no se pudo construir la base para ${p.id}`));
      process.exit(2);
    }
    const obj = JSON.parse(Buffer.from(ref.bytes, "base64").toString("utf8"));
    for (const k of p.remove ?? []) delete obj[k];
    Object.assign(obj, p.mutate ?? {});
    raw = Buffer.from(JSON.stringify(obj), "utf8").toString("base64");
  }
  operaciones.push({
    id: p.id, spec: p.spec, why: p.why,
    entrada: { op: "parse", bytes: raw },
    tipo: "codigo", esperado: p.expect,
  });
}

// ─── Ejecutar ────────────────────────────────────────────────────────────────

const activos = registro.harnesses.filter((h) => !solo || solo.has(h.sdk));
const saltados = registro.harnesses.filter((h) => solo && !solo.has(h.sdk));
const referencia = activos.find((h) => h.reference) ?? activos[0];

console.log(bold(`\nConformidad cruzada — ${operaciones.length} vectores × ${activos.length} SDK(s)`));
console.log(gray(`referencia: ${referencia.sdk}\n`));

const fallos = [];
const arnesesRotos = [];

for (const op of operaciones) {
  const resultados = new Map();
  for (const h of activos) resultados.set(h.sdk, await invocar(h, op.entrada));

  const ref = resultados.get(referencia.sdk);
  if (ref.__arnesRoto) {
    arnesesRotos.push(`${referencia.sdk} (referencia): ${ref.__arnesRoto}`);
    break; // sin referencia no hay nada que comparar
  }

  const marcas = [];
  for (const h of activos) {
    const r = resultados.get(h.sdk);
    let ok;
    if (r.__arnesRoto) {
      arnesesRotos.push(`${h.sdk} en ${op.id}: ${r.__arnesRoto}`);
      ok = null;
    } else if (op.tipo === "codigo") {
      ok = r.ok === false && r.code === op.esperado;
      if (!ok) {
        fallos.push({ op, sdk: h.sdk,
          detalle: `esperaba code="${op.esperado}", obtuvo ${r.ok ? "ok:true (¡no lo rechazó!)" : `code="${r.code}"`}` });
      }
    } else {
      ok = r.ok === true && r.bytes === ref.bytes;
      if (!ok) {
        fallos.push({ op, sdk: h.sdk,
          detalle: r.ok
            ? `bytes distintos de ${referencia.sdk}` +
              (verbose ? `\n${primeraDiferencia(ref.bytes, r.bytes)}` : "")
            : `la operación falló: ${r.code ?? "?"} ${r.detail ?? ""}` });
      }
    }
    marcas.push(ok === null ? yellow("?") : ok ? green("✓") : red("✗"));
  }

  const nombre = op.tipo === "codigo" ? `${op.id} ${gray(`→ ${op.esperado}`)}` : op.id;
  console.log(`  ${marcas.join(" ")}  ${nombre}`);
}

console.log(gray(`\n  ${activos.map((h) => h.sdk).join("  ")}`));

// ─── Informe ─────────────────────────────────────────────────────────────────

if (saltados.length) {
  console.log(yellow(`\n! ${saltados.length} SDK(s) saltados por --only: ${saltados.map((h) => h.sdk).join(", ")}`));
}

const sinArnes = (registro.pending ?? []);
if (sinArnes.length) {
  // Nunca en silencio: saltarse un SDK sin decirlo es el fallo que este runner existe
  // para evitar.
  console.log(yellow(`\n! ${sinArnes.length} SDK(s) SIN ARNÉS, no verificados: ${sinArnes.join(", ")}`));
}

if (arnesesRotos.length) {
  console.log(red(bold(`\n✗ ${arnesesRotos.length} arnés(es) roto(s)`)));
  for (const a of arnesesRotos) console.log(`  ${red("✗")} ${a}`);
}

if (fallos.length) {
  console.log(red(bold(`\n✗ ${fallos.length} divergencia(s)\n`)));
  for (const f of fallos) {
    console.log(`  ${red("✗")} ${bold(f.sdk)} · ${f.op.id}  ${gray(f.op.spec ?? "")}`);
    console.log(`    ${f.detalle}`);
    if (f.op.why) console.log(gray(`    ${f.op.why}`));
  }
}

if (!fallos.length && !arnesesRotos.length) {
  console.log(green(bold(`\n✓ ${operaciones.length} vectores idénticos en ${activos.length} SDK(s)`)));
}

process.exit(fallos.length || arnesesRotos.length ? 1 : 0);
