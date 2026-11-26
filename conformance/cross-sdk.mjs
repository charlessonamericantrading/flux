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
 *   node conformance/cross-sdk.mjs --mutant rust-un-byte   (DEBE fallar: ver MUTANTES)
 *
 * No necesita broker: esto ejercita envelope, firma, parseo y estado, no NATS.
 *
 * Los vectores de ESTADO (`stateVectors`) solo se ejecutan en los SDKs que declaran la capacidad
 * `state` en protocol.json → capabilities. En los demás se imprime «N/A: no declara state», nunca un
 * salto silencioso; y un SDK que SÍ la declara y no tiene la operación en su arnés FALLA.
 */

import { readFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const AQUI = dirname(fileURLToPath(import.meta.url));
const RAIZ = join(AQUI, "..");

const args = process.argv.slice(2);
const verbose = args.includes("--verbose") || args.includes("-v");
const soloIdx = args.indexOf("--only");
const solo = soloIdx >= 0 ? new Set(args[soloIdx + 1].split(",")) : null;
const mutIdx = args.indexOf("--mutant");
const nombreMutante = mutIdx >= 0 ? args[mutIdx + 1] : null;

const C = process.stdout.isTTY && !process.env.NO_COLOR;
const col = (n) => (s) => (C ? `\x1b[${n}m${s}\x1b[0m` : String(s));
const [bold, red, green, gray, yellow, cyan] = [1, 31, 32, 90, 33, 36].map(col);

const vectores = JSON.parse(await readFile(join(AQUI, "harness/vectors.json"), "utf8"));
const claves = JSON.parse(await readFile(join(AQUI, "harness/keys.json"), "utf8"));
const registro = JSON.parse(await readFile(join(AQUI, "harnesses.json"), "utf8"));
const protocolo = JSON.parse(await readFile(join(RAIZ, "protocol.json"), "utf8"));
const casoEstado = JSON.parse(await readFile(join(AQUI, "cases/cross-sdk-state-envelope.json"), "utf8"));

const sha256 = (b64) => createHash("sha256").update(Buffer.from(b64, "base64")).digest("hex");

// ─── Mutantes ────────────────────────────────────────────────────────────────
//
// Un runner que solo se ha visto en verde no demuestra que detecte nada. Cada mutante estropea A
// PROPÓSITO lo que un SDK entrega o declara, sin tocar su código, y el runner DEBE reaccionar:
//
//   · `falla`: sale con 1 (un byte distinto, un tenant cambiado, una operación ausente…).
//   · `pasa`:  sale con 0 y dice «N/A» (un SDK que no declara `state` no es un fallo).
//
// Solo afectan a las operaciones de estado. CI los ejecuta todos (spec.yml, job `cross-sdk`).

/** Cambia UN byte: el `0` de `"specversion":"1.0"`. Lo que haría un arnés (o un SDK) que se equivoca en una letra. */
function cambiarUnByte(b64) {
  const b = Buffer.from(b64, "base64");
  const i = b.indexOf('"specversion":"1.0"');
  b[i >= 0 ? i + 17 : b.length - 1] ^= 1; // el '0' de "1.0" (0x30) → '1' (0x31)
  return b.toString("base64");
}

/** acme ↔ globex: un SDK que escribe el estado de un tenant en el de otro. */
const intercambiar = (t) => (t === "acme" ? "globex" : t === "globex" ? "acme" : t);

const MUTANTES = {
  "rust-un-byte": {
    esperado: "falla",
    que: "el arnés de Rust devuelve los bytes de estado con UN byte cambiado (`specversion` 1.0 → 1.1)",
    salida: (sdk, op, r) =>
      sdk === "rust" && r.ok && r.bytes ? { ...r, bytes: cambiarUnByte(r.bytes) } : r,
  },
  "rust-tenant-cambiado": {
    esperado: "falla",
    que: "el SDK de Rust intercambia los tenants `acme` y `globex` en toda operación de estado",
    entrada: (sdk, op, e) =>
      sdk !== "rust" ? e : {
        ...e,
        ...(e.tenantId !== undefined && { tenantId: intercambiar(e.tenantId) }),
        ...(e.original && { original: { ...e.original, tenantId: intercambiar(e.original.tenantId) } }),
      },
  },
  "rust-ignora-tenant": {
    esperado: "falla",
    que: "el SDK de Rust ignora el tenant que se le pide y escribe siempre `acme`",
    entrada: (sdk, op, e) =>
      sdk !== "rust" ? e : {
        ...e,
        ...(e.tenantId !== undefined && { tenantId: "acme" }),
        ...(e.original && { original: { ...e.original, tenantId: "acme" } }),
      },
  },
  "rust-sin-state-subject": {
    esperado: "falla",
    que: "Rust declara `state` pero su arnés no implementa `state-subject` (responde UNSUPPORTED_OP)",
    salida: (sdk, op, r) =>
      sdk === "rust" && op.entrada.op === "state-subject"
        ? { ok: false, code: "UNSUPPORTED_OP", detail: "state-subject" }
        : r,
  },
  "python-declara-state": {
    esperado: "falla",
    que: "Python pasa a declarar `state` sin tener ninguna de las operaciones en su arnés (el arnés real responde UNSUPPORTED_OP)",
    declara: (sdk, cap) => (sdk === "python" && cap === "state" ? true : undefined),
  },
  "rust-sin-arnes-registrado": {
    esperado: "falla",
    que: "Rust declara `state` pero su arnés desaparece de conformance/harnesses.json",
    registro: (r) => { r.harnesses = r.harnesses.filter((h) => h.sdk !== "rust"); },
  },
  "rust-no-declara-state": {
    esperado: "pasa",
    que: "Rust deja de declarar `state`: sus vectores de estado salen N/A y NO es un fallo",
    declara: (sdk, cap) => (sdk === "rust" && cap === "state" ? false : undefined),
  },
};

const mutante = nombreMutante ? MUTANTES[nombreMutante] : null;
if (nombreMutante && !mutante) {
  console.error(red(`mutante desconocido "${nombreMutante}". Los hay: ${Object.keys(MUTANTES).join(", ")}`));
  process.exit(2);
}
mutante?.registro?.(registro);

/** ¿Declara este SDK la capacidad en protocol.json → capabilities? (con el mutante, si lo hay). */
function declara(sdk, capacidad) {
  const forzada = mutante?.declara?.(sdk, capacidad);
  if (forzada !== undefined) return forzada;
  return (protocolo.capabilities?.sdks?.[sdk]?.capabilities ?? []).includes(capacidad);
}

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

/** Como `invocar`, pero aplicando el mutante a lo que se manda y a lo que vuelve (solo estado). */
async function ejecutarEn(harness, op) {
  const mutar = mutante && op.capacidad;
  const entrada = mutar && mutante.entrada ? mutante.entrada(harness.sdk, op, op.entrada) : op.entrada;
  const r = await invocar(harness, entrada);
  return mutar && mutante.salida && !r.__arnesRoto ? mutante.salida(harness.sdk, op, r) : r;
}

const abreviar = (s, n) => (s.length > n ? `${s.slice(0, n - 1)}…` : s);

/** Primer byte en que difieren dos base64, con su contexto legible. */
function primeraDiferencia(a, b) {
  const ba = Buffer.from(a, "base64");
  const bb = Buffer.from(b, "base64");
  let i = 0;
  while (i < ba.length && i < bb.length && ba[i] === bb[i]) i++;
  const hex = (buf) => (i < buf.length ? `0x${buf[i].toString(16).padStart(2, "0")}` : "(fin)");
  const ctx = (buf) => JSON.stringify(buf.subarray(Math.max(0, i - 30), i + 40).toString("utf8"));
  return (
    `  byte ${i}: referencia ${hex(ba)}, este SDK ${hex(bb)}  (${ba.length} y ${bb.length} bytes)\n` +
    `    referencia ${ctx(ba)}\n    este SDK   ${ctx(bb)}`
  );
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

// Vectores de `verify`: el SDK de REFERENCIA firma, y cada uno de los demás verifica
// ESA firma. Verificar la propia solo demuestra consistencia consigo mismo; esto es lo
// único que demuestra interoperabilidad.
const refHarness = registro.harnesses.find((h) => h.reference) ?? registro.harnesses[0];
for (const v of vectores.verifyVectors ?? []) {
  const fuente = v.signVector
    ? vectores.vectors.find((x) => x.id === v.signVector)
    : vectores.vectors.find((x) => x.id === v.buildVector);
  if (!fuente) {
    console.error(red(`vector de verify "${v.id}" apunta a uno inexistente`));
    process.exit(2);
  }

  const entradaFirma = { op: v.signVector ? "sign" : "build", event: fuente.event };
  if (v.signVector) {
    entradaFirma.signing = { privateKeyPem: claves.privateKeyPem, keyId: claves.keyId };
  }
  const firmado = await invocar(refHarness, entradaFirma);
  if (firmado.__arnesRoto || !firmado.ok) {
    console.error(red(`no se pudo preparar "${v.id}" con ${refHarness.sdk}`));
    process.exit(2);
  }

  let bytes = firmado.bytes;
  if (v.mutate) {
    // Se manipula DESPUÉS de firmar: es exactamente el ataque que la firma detecta —
    // un evento sacado del stream, editado y reinyectado (07-signing.md §1).
    const obj = JSON.parse(Buffer.from(bytes, "base64").toString("utf8"));
    Object.assign(obj, v.mutate);
    bytes = Buffer.from(JSON.stringify(obj), "utf8").toString("base64");
  }

  operaciones.push({
    id: v.id, spec: v.spec, why: v.why,
    entrada: {
      op: "verify", bytes,
      publicKeys: { [claves.keyId]: claves.publicKeyPem },
      mode: "require",
    },
    tipo: v.expect === "ok" ? "verifyOk" : "codigo",
    esperado: v.expect,
  });
}

// Vectores de ESTADO (capacidad `state`, specification/10-state.md). Cada uno dice qué espera:
//   · `sha256`       → los bytes son los de la spec (no «lo que dice el SDK de referencia»);
//   · `expectSubject`→ el subject de NATS;
//   · `expect`       → el CÓDIGO estable del rechazo;
//   · nada           → los bytes los fija el SDK de referencia, y los demás deben coincidir.
// `differsFrom` es un control: los bytes DEBEN ser distintos de los de otro vector del mismo SDK.
const CAPACIDAD_DE_ESTADO = "state";
const OPS_DE_ESTADO = new Set(["state-build", "state-tombstone", "state-subject", "state-dlq"]);
const idsVistos = new Set(operaciones.map((o) => o.id));
for (const v of vectores.stateVectors ?? []) {
  const problemas = [];
  if (!OPS_DE_ESTADO.has(v.op)) problemas.push(`op "${v.op}" no es una operación de estado`);
  if (idsVistos.has(v.id)) problemas.push("id repetido");
  if (v.expect !== undefined && v.expectSubject !== undefined) problemas.push("`expect` y `expectSubject` a la vez");
  if (v.differsFrom !== undefined && !idsVistos.has(v.differsFrom)) {
    problemas.push(`\`differsFrom\` apunta a "${v.differsFrom}", que no existe antes que él`);
  }
  if (problemas.length) {
    console.error(red(`vector de estado "${v.id}" mal formado: ${problemas.join("; ")}`));
    process.exit(2);
  }
  idsVistos.add(v.id);
  operaciones.push({
    id: v.id, spec: v.spec, why: v.why,
    entrada: { op: v.op, ...v.input },
    tipo: v.expectSubject !== undefined ? "subject" : v.expect !== undefined ? "codigo" : "bytes",
    esperado: v.expect, esperadoSubject: v.expectSubject,
    sha256: v.sha256, differsFrom: v.differsFrom, caseFixture: v.caseFixture,
    capacidad: CAPACIDAD_DE_ESTADO,
  });
}

/**
 * Los vectores de estado repiten los fixtures y los `subjectVectors` de
 * cases/cross-sdk-state-envelope.json (los ejecuta el runner; el caso los documenta). Dos copias
 * se desincronizan en silencio: aquí se comprueba que siguen siendo la misma.
 */
function coherenciaConElCaso() {
  const problemas = [];
  const deEstado = operaciones.filter((o) => o.capacidad === CAPACIDAD_DE_ESTADO);
  const entradaSinOp = (o) => { const { op, ...resto } = o.entrada; return JSON.stringify(resto); };

  for (const f of casoEstado.fixtures) {
    const o = deEstado.find((x) => x.caseFixture === f.id);
    if (!o) { problemas.push(`el fixture "${f.id}" del caso no tiene vector (\`caseFixture\`) en vectors.json`); continue; }
    if (o.entrada.op !== f.harnessOp) problemas.push(`"${o.id}": op ${o.entrada.op}, el caso dice ${f.harnessOp}`);
    if (entradaSinOp(o) !== JSON.stringify(f.input)) problemas.push(`"${o.id}": su entrada ya no es la del fixture "${f.id}"`);
    if (o.sha256 !== f.sha256) problemas.push(`"${o.id}": sha256 ${o.sha256}, el caso dice ${f.sha256}`);
  }
  for (const [i, sv] of casoEstado.subjectVectors.entries()) {
    const o = deEstado.find((x) => x.entrada.op === "state-subject" &&
      x.entrada.contract === sv.contract && x.entrada.tenantId === sv.tenantId && x.entrada.key === sv.key);
    if (!o) { problemas.push(`el vector de subject #${i} del caso (${JSON.stringify(sv)}) no está en vectors.json`); continue; }
    if (o.esperadoSubject !== sv.natsSubject || o.esperado !== sv.errorCode) {
      problemas.push(`"${o.id}": espera ${o.esperadoSubject ?? o.esperado}, el caso dice ${sv.natsSubject ?? sv.errorCode}`);
    }
  }
  return problemas;
}

{
  const incoherencias = coherenciaConElCaso();
  if (incoherencias.length) {
    console.error(red(bold("vectors.json y cases/cross-sdk-state-envelope.json ya no coinciden:")));
    for (const p of incoherencias) console.error(`  ${red("✗")} ${p}`);
    process.exit(2);
  }
}

/**
 * Lo que la SPEC dice de los bytes de estado, comprobado sobre la salida de REFERENCIA. Que todos los
 * SDKs coincidan con Node no prueba que Node acierte: esto sí es independiente de él.
 */
function invariantesDeEstado(op, b64) {
  const problemas = [];
  const texto = Buffer.from(b64, "base64").toString("utf8");
  let ev;
  try { ev = JSON.parse(texto); } catch { return ["los bytes no son JSON"]; }

  const e = op.entrada;
  const origen = e.op === "state-dlq" ? e.original : e;
  const [dom, agr, mayor] = origen.contract.split(".");

  if (Object.keys(ev).at(-1) !== "data") problemas.push("`data` no es el último atributo (01-envelope.md §6)");
  if (ev.subject !== origen.key) problemas.push("el atributo `subject` no es la clave (10-state.md §2.5)");
  if (ev.partitionkey !== origen.key) problemas.push("`partitionkey` no es la clave (10-state.md §2.5)");
  if (ev.tenantid !== origen.tenantId) problemas.push("`tenantid` no es el tenant del subject de NATS (10-state.md §2.5)");
  // Solo los de control (U+0000–U+001F) se escapan: los demás son UTF-8 literal (01-envelope.md §1.1).
  if (/\\u(?!00[01][0-9a-f])[0-9a-f]{4}/i.test(texto)) problemas.push("lleva un escape \\u de un carácter que no es de control: debe ser UTF-8 literal");

  if (e.op === "state-tombstone") {
    if (ev.type !== `com.flux.state.${dom}.${agr}.deleted.${mayor}`) problemas.push(`type ${ev.type} (10-state.md §4)`);
    if (ev.dataschema !== "https://schemas.internal/system/state/eliminado/1.0.0.json") problemas.push("dataschema de la lápida no es el fijo (10-state.md §4)");
    if (ev.dataclassification !== "internal") problemas.push("la lápida no es `internal` (10-state.md §4)");
    if (Object.keys(ev.data).join() !== "aggregateVersion") problemas.push("`data` de la lápida no es solo la versión (10-state.md §4)");
  } else {
    if (ev.type !== `com.flux.state.${dom}.${agr}.${mayor}`) problemas.push(`type ${ev.type} (10-state.md §2.5)`);
    if (ev.dataclassification !== origen.classification) problemas.push("`dataclassification` no es la pedida");
  }
  if (e.op === "state-build" && JSON.stringify(ev.data) !== JSON.stringify(e.data)) {
    problemas.push("`data` no conserva el orden de claves ni los valores del vector (01-envelope.md §6)");
  }
  if (e.op === "state-dlq") {
    if (Object.keys(ev.data).join() !== "key,revision") problemas.push("`data` de la DLQ de estado no es solo { key, revision } (10-state.md §6)");
    if (ev.data.revision !== e.revision) problemas.push("`data.revision` no es la revisión pedida");
    if ("signature" in ev || "signkeyid" in ev) problemas.push("la DLQ de estado conserva la firma (10-state.md §6)");
    const claves = Object.keys(ev);
    if (claves.at(-2) !== "dlqtime") problemas.push("`dlqtime` no es el atributo justo antes de `data`");
  }
  return problemas;
}

// ─── Ejecutar ────────────────────────────────────────────────────────────────

const activos = registro.harnesses.filter((h) => !solo || solo.has(h.sdk));
const saltados = registro.harnesses.filter((h) => solo && !solo.has(h.sdk));
const referencia = activos.find((h) => h.reference) ?? activos[0];

const generales = operaciones.filter((o) => !o.capacidad);
const deEstado = operaciones.filter((o) => o.capacidad === CAPACIDAD_DE_ESTADO);
const conEstado = activos.filter((h) => declara(h.sdk, CAPACIDAD_DE_ESTADO));
const sinEstado = activos.filter((h) => !declara(h.sdk, CAPACIDAD_DE_ESTADO));

// Un SDK que declara `state` y NO tiene arnés registrado (o figura en `pending`) no se ejecutaría
// nunca, ni siquiera para fallar: la capacidad quedaría declarada sin evidencia. Se mira contra el
// registro entero, no contra `--only`: excluir un SDK de una ejecución no es quitarle el arnés.
const declaranSinArnes = Object.keys(protocolo.capabilities?.sdks ?? {}).filter(
  (sdk) =>
    declara(sdk, CAPACIDAD_DE_ESTADO) &&
    (!registro.harnesses.some((h) => h.sdk === sdk) || (registro.pending ?? []).includes(sdk)),
);

console.log(bold(
  `\nConformidad cruzada — ${generales.length} vectores × ${activos.length} SDK(s)` +
  (deEstado.length ? `  +  ${deEstado.length} de estado × ${conEstado.length} SDK(s) con \`state\`` : ""),
));
console.log(gray(`referencia: ${referencia.sdk}`));
if (mutante) {
  console.log(yellow(`! MUTANTE ${nombreMutante}: ${mutante.que}.`));
  console.log(yellow(`  El runner ${mutante.esperado === "falla" ? "DEBE fallar" : "DEBE pasar, con N/A"}.`));
}
console.log("");

const fallos = [];
const arnesesRotos = [];
const porOp = new Map(); // id → Map(sdk → resultado), para `differsFrom`
let encabezadoDeEstado = false;

for (const op of operaciones) {
  if (op.capacidad && !encabezadoDeEstado) {
    encabezadoDeEstado = true;
    console.log(gray(`\n  ── estado replicado (capacidad \`${op.capacidad}\`) · «–» = N/A: no declara ${op.capacidad} ──`));
  }

  // Solo se ejecuta donde aplica: una capacidad que el SDK no declara no se le exige…
  const aplican = activos.filter((h) => !op.capacidad || declara(h.sdk, op.capacidad));
  const resultados = new Map();
  for (const h of aplican) resultados.set(h.sdk, await ejecutarEn(h, op));
  porOp.set(op.id, resultados);

  // …y la referencia de una operación es la del conjunto que la ejecuta.
  const ref0 = aplican.find((h) => h.reference) ?? aplican[0];
  const ref = ref0 ? resultados.get(ref0.sdk) : null;
  if (ref?.__arnesRoto) {
    arnesesRotos.push(`${ref0.sdk} (referencia): ${ref.__arnesRoto}`);
    break; // sin referencia no hay nada que comparar
  }

  const invariantes = ref?.ok && op.capacidad && op.tipo === "bytes" ? invariantesDeEstado(op, ref.bytes) : [];
  for (const p of invariantes) fallos.push({ op, sdk: ref0.sdk, detalle: `invariante de la spec: ${p}` });

  const marcas = [];
  for (const h of activos) {
    if (!aplican.includes(h)) { marcas.push(gray("–")); continue; } // N/A

    const r = resultados.get(h.sdk);
    let ok;
    if (r.__arnesRoto) {
      arnesesRotos.push(`${h.sdk} en ${op.id}: ${r.__arnesRoto}`);
      ok = null;
    } else if (op.capacidad && r.ok === false && r.code === "UNSUPPORTED_OP") {
      // Declarar una capacidad sin tener su operación es EXACTAMENTE lo que este runner impide.
      ok = false;
      fallos.push({ op, sdk: h.sdk,
        detalle: `declara \`${op.capacidad}\` en protocol.json pero su arnés no implementa \`${op.entrada.op}\` (UNSUPPORTED_OP)` });
    } else if (op.tipo === "verifyOk") {
      ok = r.ok === true;
      if (!ok) {
        fallos.push({ op, sdk: h.sdk,
          detalle: `esperaba que verificase, obtuvo code="${r.code ?? "?"}" ${r.detail ?? ""}` });
      }
    } else if (op.tipo === "codigo") {
      ok = r.ok === false && r.code === op.esperado;
      if (!ok) {
        fallos.push({ op, sdk: h.sdk,
          detalle: `esperaba code="${op.esperado}", obtuvo ${r.ok ? "ok:true (¡no lo rechazó!)" : `code="${r.code}"`}` });
      }
    } else if (op.tipo === "subject") {
      ok = r.ok === true && r.subject === op.esperadoSubject;
      if (!ok) {
        fallos.push({ op, sdk: h.sdk,
          detalle: r.ok
            ? `subject ${JSON.stringify(r.subject)}, esperaba ${JSON.stringify(op.esperadoSubject)}`
            : `esperaba el subject ${JSON.stringify(op.esperadoSubject)}, lo rechazó: code="${r.code ?? "?"}"` });
      }
    } else {
      // bytes: con `sha256` el patrón es la SPEC; sin él, el SDK de referencia.
      const objetivo = op.sha256 ? null : ref.bytes;
      const obtenido = r.ok ? sha256(r.bytes) : null;
      ok = r.ok === true && (op.sha256 ? obtenido === op.sha256 : r.bytes === objetivo);
      if (!ok) {
        const difiereDeLaReferencia = r.ok && ref.ok && r.bytes !== ref.bytes && h !== ref0;
        fallos.push({ op, sdk: h.sdk,
          detalle: r.ok
            ? (op.sha256
                ? `sha256 ${obtenido}, y la spec fija ${op.sha256}`
                : `bytes distintos de ${ref0.sdk}`) +
              (verbose && difiereDeLaReferencia ? `\n${primeraDiferencia(ref.bytes, r.bytes)}` : "")
            : `la operación falló: ${r.code ?? "?"} ${r.detail ?? ""}` });
      }
      // El control: otro tenant DEBE dar otros bytes. Se evalúa APARTE de la comparación con la
      // referencia: un SDK que ignorase el tenant la fallaría de todos modos, pero esto lo detectaría
      // aunque la referencia compartiera el error.
      if (op.differsFrom) {
        const otro = porOp.get(op.differsFrom)?.get(h.sdk);
        if (r.ok && otro?.ok && otro.bytes === r.bytes) {
          ok = false;
          fallos.push({ op, sdk: h.sdk,
            detalle: `CONTROL: los bytes son IGUALES a los de "${op.differsFrom}" y deberían diferir` });
        }
      }
      if (ok && h === ref0 && invariantes.length) ok = false;
    }
    marcas.push(ok === null ? yellow("?") : ok ? green("✓") : red("✗"));
  }

  const nombre =
    op.tipo === "codigo" ? `${op.id} ${gray(`→ ${op.esperado}`)}`
    : op.tipo === "verifyOk" ? `${op.id} ${gray("→ verifica")}`
    : op.tipo === "subject" ? `${op.id} ${gray(`→ ${abreviar(op.esperadoSubject, 72)}`)}`
    : op.differsFrom ? `${op.id} ${gray(`→ distinto de ${op.differsFrom}`)}`
    : op.sha256 ? `${op.id} ${gray(`→ sha256 ${op.sha256.slice(0, 12)}…`)}`
    : op.id;
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

if (deEstado.length && sinEstado.length) {
  // Tampoco en silencio: es la CAPACIDAD la que no se declara, no un SDK que se olvidó.
  console.log(yellow(
    `\n! N/A: no declara state — ${sinEstado.map((h) => h.sdk).join(", ")}. ` +
    `Sus ${deEstado.length} vectores de estado no se ejecutan (protocol.json → capabilities); no es un fallo.`,
  ));
}
if (deEstado.length && !conEstado.length) {
  console.log(yellow(`! ningún SDK activo declara state: los ${deEstado.length} vectores de estado NO se ejecutaron`));
}

if (declaranSinArnes.length) {
  console.log(red(bold(`\n✗ ${declaranSinArnes.length} SDK(s) declaran \`state\` sin arnés registrado`)));
  for (const sdk of declaranSinArnes) {
    console.log(`  ${red("✗")} ${sdk}: protocol.json lo declara, pero no está en conformance/harnesses.json (o figura en \`pending\`): sin arnés no hay evidencia`);
  }
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

if (!fallos.length && !arnesesRotos.length && !declaranSinArnes.length) {
  console.log(green(bold(`\n✓ ${generales.length} vectores idénticos en ${activos.length} SDK(s)`)));
  if (deEstado.length && conEstado.length) {
    console.log(
      green(bold(`✓ ${deEstado.length} vectores de estado idénticos en ${conEstado.length} SDK(s): ${conEstado.map((h) => h.sdk).join(", ")}`)) +
      (sinEstado.length ? yellow(`  ·  N/A en ${sinEstado.length}: ${sinEstado.map((h) => h.sdk).join(", ")}`) : ""),
    );
  }
}

process.exit(fallos.length || arnesesRotos.length || declaranSinArnes.length ? 1 : 0);
