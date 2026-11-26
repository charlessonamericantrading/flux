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
 *   node conformance/cross-sdk.mjs --exigir state,rpc      (falla si ningún SDK activo ejecuta esa capacidad)
 *
 * No necesita broker: esto ejercita envelope, firma, parseo, estado y petición/respuesta, no NATS.
 *
 * Los vectores de una CAPACIDAD (`stateVectors`, `rpcVectors`) solo se ejecutan en los SDKs que la declaran
 * en protocol.json → capabilities. En los demás se imprime «N/A: no declara state» (o `rpc`), nunca un
 * salto silencioso; y un SDK que SÍ la declara y no tiene la operación en su arnés —o no tiene arnés
 * registrado— FALLA.
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
const exigIdx = args.indexOf("--exigir");
const exigidas = exigIdx >= 0 ? args[exigIdx + 1].split(",").filter(Boolean) : [];

const C = process.stdout.isTTY && !process.env.NO_COLOR;
const col = (n) => (s) => (C ? `\x1b[${n}m${s}\x1b[0m` : String(s));
const [bold, red, green, gray, yellow, cyan] = [1, 31, 32, 90, 33, 36].map(col);

const vectores = JSON.parse(await readFile(join(AQUI, "harness/vectors.json"), "utf8"));
const claves = JSON.parse(await readFile(join(AQUI, "harness/keys.json"), "utf8"));
const registro = JSON.parse(await readFile(join(AQUI, "harnesses.json"), "utf8"));
const protocolo = JSON.parse(await readFile(join(RAIZ, "protocol.json"), "utf8"));
const casoEstado = JSON.parse(await readFile(join(AQUI, "cases/cross-sdk-state-envelope.json"), "utf8"));
const casoRpc = JSON.parse(await readFile(join(AQUI, "cases/cross-sdk-rpc-envelope.json"), "utf8"));
const especificacionRpc = await readFile(join(RAIZ, "specification/11-request-reply.md"), "utf8");

const sha256 = (b64) => createHash("sha256").update(Buffer.from(b64, "base64")).digest("hex");

// ─── Mutantes ────────────────────────────────────────────────────────────────
//
// Un runner que solo se ha visto en verde no demuestra que detecte nada. Cada mutante estropea A
// PROPÓSITO lo que un SDK entrega o declara, sin tocar su código, y el runner DEBE reaccionar:
//
//   · `falla`: sale con 1 (un byte distinto, un tenant cambiado, una operación ausente…).
//   · `pasa`:  sale con 0 y dice «N/A» (un SDK que no declara una capacidad no es un fallo).
//
// Cada uno afecta a las operaciones de UNA capacidad (`capacidad`: `state` si no dice otra cosa), y solo
// de ella. CI los ejecuta todos (spec.yml, job `cross-sdk`).

/** Cambia UN byte: el `0` de `"specversion":"1.0"`. Lo que haría un arnés (o un SDK) que se equivoca en una letra. */
function cambiarUnByte(b64) {
  const b = Buffer.from(b64, "base64");
  const i = b.indexOf('"specversion":"1.0"');
  b[i >= 0 ? i + 17 : b.length - 1] ^= 1; // el '0' de "1.0" (0x30) → '1' (0x31)
  return b.toString("base64");
}

/** acme ↔ globex: un SDK que escribe el estado de un tenant en el de otro. */
const intercambiar = (t) => (t === "acme" ? "globex" : t === "globex" ? "acme" : t);

/** Lo que hace una respuesta o un error de rpc cuyo `correlationid` no es el de la petición, sino su `id`. */
function conCorrelationidDeLaPeticion(b64, peticionId) {
  const obj = JSON.parse(Buffer.from(b64, "base64").toString("utf8"));
  obj.correlationid = peticionId;
  return Buffer.from(JSON.stringify(obj), "utf8").toString("base64");
}

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
    que: "Rust declara `state` y `rpc` pero su arnés desaparece de conformance/harnesses.json",
    registro: (r) => { r.harnesses = r.harnesses.filter((h) => h.sdk !== "rust"); },
  },
  "rust-no-declara-state": {
    esperado: "pasa",
    que: "Rust deja de declarar `state`: sus vectores de estado salen N/A y NO es un fallo",
    declara: (sdk, cap) => (sdk === "rust" && cap === "state" ? false : undefined),
  },

  // ── petición/respuesta (`rpc`) ──
  "rust-rpc-un-byte": {
    capacidad: "rpc",
    esperado: "falla",
    que: "el arnés de Rust devuelve los bytes de rpc (petición, respuesta y errores) con UN byte cambiado (`specversion` 1.0 → 1.1)",
    salida: (sdk, op, r) =>
      sdk === "rust" && r.ok && r.bytes ? { ...r, bytes: cambiarUnByte(r.bytes) } : r,
  },
  "rust-rpc-timeout-recortado": {
    capacidad: "rpc",
    esperado: "falla",
    que: "Rust ACEPTA un timeout fuera de rango: lo recorta a [1, 25000] en vez de rechazarlo con INVALID_RPC_CALL (§4.2: «no se recorta en silencio»)",
    salida: (sdk, op, r) => {
      const t = op.entrada.timeoutMs;
      return sdk === "rust" && op.entrada.op === "rpc-timeout" && !r.ok && r.code === "INVALID_RPC_CALL" && typeof t === "number"
        ? { ok: true, effectiveMs: Math.min(Math.max(Math.trunc(t), 1), 25000) }
        : r;
    },
  },
  "rust-rpc-dominio-reservado-aceptado": {
    capacidad: "rpc",
    esperado: "falla",
    que: "Rust NO rechaza un dominio reservado en rpc: construye el subject (o el sobre) en vez de contestar RESERVED_DOMAIN",
    salida: (sdk, op, r) =>
      sdk === "rust" && !r.ok && r.code === "RESERVED_DOMAIN"
        ? op.entrada.op === "rpc-operation"
          ? { ok: true, subject: `rpc.${op.entrada.operation}` }
          : { ok: true, bytes: Buffer.from("{}").toString("base64") }
        : r,
  },
  "rust-rpc-correlationid-de-la-peticion": {
    capacidad: "rpc",
    esperado: "falla",
    que: "Rust escribe en la respuesta y en el error el `id` de la petición como `correlationid`, en vez de HEREDAR el suyo (en los ejemplos de la spec valen lo mismo)",
    salida: (sdk, op, r) =>
      sdk === "rust" && r.ok && r.bytes && (op.entrada.op === "rpc-reply" || op.entrada.op === "rpc-error")
        ? { ...r, bytes: conCorrelationidDeLaPeticion(r.bytes, op.entrada.request.id) }
        : r,
  },
  "rust-sin-rpc-operation": {
    capacidad: "rpc",
    esperado: "falla",
    que: "Rust declara `rpc` pero su arnés no implementa `rpc-operation` (responde UNSUPPORTED_OP)",
    salida: (sdk, op, r) =>
      sdk === "rust" && op.entrada.op === "rpc-operation"
        ? { ok: false, code: "UNSUPPORTED_OP", detail: "rpc-operation" }
        : r,
  },
  "python-declara-rpc": {
    esperado: "falla",
    que: "Python pasa a declarar `rpc` sin tener ninguna de las cinco operaciones en su arnés (el arnés real responde UNSUPPORTED_OP)",
    declara: (sdk, cap) => (sdk === "python" && cap === "rpc" ? true : undefined),
  },
  "rust-no-declara-rpc": {
    esperado: "pasa",
    que: "Rust deja de declarar `rpc`: sus vectores de rpc salen N/A y NO es un fallo",
    declara: (sdk, cap) => (sdk === "rust" && cap === "rpc" ? false : undefined),
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

/** Como `invocar`, pero aplicando el mutante a lo que se manda y a lo que vuelve (solo las operaciones de SU capacidad). */
async function ejecutarEn(harness, op) {
  const mutar = mutante && op.capacidad && op.capacidad === (mutante.capacidad ?? "state");
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


// Vectores de una CAPACIDAD: `stateVectors` (specification/10-state.md) y `rpcVectors`
// (specification/11-request-reply.md). Cada uno dice qué espera:
//   · `sha256` (+ `length`) → los bytes son los de la spec (no «lo que dice el SDK de referencia»);
//   · `expectSubject`       → el subject de NATS;
//   · `expectEffectiveMs`   → los milisegundos efectivos de un timeout (`rpc-timeout`);
//   · `expect`              → el CÓDIGO estable del rechazo;
//   · nada                  → los bytes los fija el SDK de referencia, y los demás deben coincidir.
// `differsFrom` es un control: los bytes DEBEN ser distintos de los de otro vector del mismo SDK.
//
// Añadir una capacidad es añadir una entrada aquí: las operaciones que su arnés debe implementar y la
// sección de vectors.json que las ejercita. Quien la declara en protocol.json las ejecuta; quien no, sale N/A.
const CAPACIDADES = {
  state: {
    clave: "stateVectors", etiqueta: "de estado", nombre: "estado replicado",
    ops: new Set(["state-build", "state-tombstone", "state-subject", "state-dlq"]),
  },
  rpc: {
    clave: "rpcVectors", etiqueta: "de rpc", nombre: "petición/respuesta",
    ops: new Set(["rpc-request", "rpc-reply", "rpc-error", "rpc-operation", "rpc-timeout"]),
  },
};
const idsVistos = new Set(operaciones.map((o) => o.id));
for (const [cap, def] of Object.entries(CAPACIDADES)) {
  const lista = vectores[def.clave];
  if (!Array.isArray(lista) || !lista.length) {
    console.error(red(`vectors.json no tiene \`${def.clave}\` (o está vacío): sin vectores, declarar \`${cap}\` no tendría evidencia`));
    process.exit(2);
  }
  for (const v of lista) {
    const problemas = [];
    if (!def.ops.has(v.op)) problemas.push(`op "${v.op}" no es una operación de ${def.nombre}`);
    if (idsVistos.has(v.id)) problemas.push("id repetido");
    const esperas = ["expect", "expectSubject", "expectEffectiveMs"].filter((k) => v[k] !== undefined);
    if (esperas.length > 1) problemas.push(`${esperas.map((k) => `\`${k}\``).join(" y ")} a la vez`);
    if (v.sha256 !== undefined && esperas.length) problemas.push("`sha256` en un vector cuyo resultado no son bytes");
    if (v.length !== undefined && v.sha256 === undefined) problemas.push("`length` sin `sha256`");
    if (v.differsFrom !== undefined && !idsVistos.has(v.differsFrom)) {
      problemas.push(`\`differsFrom\` apunta a "${v.differsFrom}", que no existe antes que él`);
    }
    if (problemas.length) {
      console.error(red(`vector de ${def.nombre} "${v.id}" mal formado: ${problemas.join("; ")}`));
      process.exit(2);
    }
    idsVistos.add(v.id);
    operaciones.push({
      id: v.id, spec: v.spec, why: v.why,
      entrada: { op: v.op, ...v.input },
      tipo: v.expectSubject !== undefined ? "subject"
        : v.expectEffectiveMs !== undefined ? "efectivo"
        : v.expect !== undefined ? "codigo"
        : "bytes",
      esperado: v.expect, esperadoSubject: v.expectSubject, esperadoEfectivo: v.expectEffectiveMs,
      sha256: v.sha256, longitud: v.length, differsFrom: v.differsFrom, caseFixture: v.caseFixture,
      capacidad: cap,
    });
  }
}

/** La entrada de un vector sin su `op`, como texto comparable. */
const entradaSinOp = (o) => { const { op, ...resto } = o.entrada; return JSON.stringify(resto); };

/**
 * Los vectores de estado repiten los fixtures y los `subjectVectors` de
 * cases/cross-sdk-state-envelope.json (los ejecuta el runner; el caso los documenta). Dos copias
 * se desincronizan en silencio: aquí se comprueba que siguen siendo la misma.
 */
function coherenciaConElCaso() {
  const problemas = [];
  const deEstado = operaciones.filter((o) => o.capacidad === "state");

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

/**
 * Lo mismo para rpc, y algo más: aquí hay TRES copias de lo mismo —la spec (§3.5), el caso
 * cases/cross-sdk-rpc-envelope.json y los vectores— más las constantes de protocol.json → rpc. Los vectores del
 * caso (los 4 sobres, los de operación y los de timeout) DEBEN estar en vectors.json con la misma entrada y el
 * mismo resultado; los bytes del caso DEBEN ser los de la spec; y lo que protocol.json declara válido o inválido,
 * o como límite del timeout, DEBE tener su vector.
 */
function coherenciaDeRpc() {
  const problemas = [];
  const deRpc = operaciones.filter((o) => o.capacidad === "rpc");
  const json = JSON.stringify;

  // El caso, consigo mismo y con la spec: de aquí salen los bytes que los vectores fijan con su SHA-256.
  const opsCaso = Object.keys(casoRpc.harness?.ops ?? {}).sort().join();
  const opsRunner = [...CAPACIDADES.rpc.ops].sort().join();
  if (opsCaso !== opsRunner) problemas.push(`el caso lista las operaciones del arnés «${opsCaso}» y el runner ejecuta «${opsRunner}»`);

  for (const f of casoRpc.fixtures) {
    const bytes = Buffer.from(f.base64, "base64");
    if (sha256(f.base64) !== f.sha256) problemas.push(`fixture "${f.id}": el SHA-256 de su \`base64\` no es el que dice`);
    if (bytes.length !== f.bytes) problemas.push(`fixture "${f.id}": su \`base64\` mide ${bytes.length} bytes y dice ${f.bytes}`);
    if (Buffer.compare(Buffer.from(json(f.event), "utf8"), bytes) !== 0) {
      problemas.push(`fixture "${f.id}": su \`event\` serializado no son los bytes de su \`base64\``);
    }
    if (!especificacionRpc.includes(bytes.toString("utf8"))) {
      problemas.push(`fixture "${f.id}": sus bytes no están, tal cual, en specification/11-request-reply.md §3.5`);
    }
    if (!especificacionRpc.includes(f.sha256)) {
      problemas.push(`fixture "${f.id}": su SHA-256 no está en specification/11-request-reply.md §3.5`);
    }
    const o = deRpc.find((x) => x.caseFixture === f.id);
    if (!o) { problemas.push(`el fixture "${f.id}" del caso no tiene vector (\`caseFixture\`) en vectors.json`); continue; }
    if (o.entrada.op !== f.harnessOp) problemas.push(`"${o.id}": op ${o.entrada.op}, el caso dice ${f.harnessOp}`);
    if (entradaSinOp(o) !== json(f.input)) problemas.push(`"${o.id}": su entrada ya no es la del fixture "${f.id}"`);
    if (o.sha256 !== f.sha256) problemas.push(`"${o.id}": sha256 ${o.sha256}, el caso dice ${f.sha256}`);
    if (o.longitud !== f.bytes) problemas.push(`"${o.id}": longitud ${o.longitud}, el caso dice ${f.bytes}`);
  }

  const deOperacion = deRpc.filter((x) => x.entrada.op === "rpc-operation");
  for (const [i, sv] of casoRpc.subjectVectors.entries()) {
    const os = deOperacion.filter((x) => x.entrada.operation === sv.operation);
    if (os.length !== 1) { problemas.push(`el vector de operación #${i} del caso (${json(sv.operation)}) está ${os.length} veces en vectors.json: debe estar una`); continue; }
    const [o] = os;
    if (o.esperadoSubject !== sv.natsSubject || o.esperado !== sv.errorCode) {
      problemas.push(`"${o.id}": espera ${o.esperadoSubject ?? o.esperado}, el caso dice ${sv.natsSubject ?? sv.errorCode}`);
    }
  }

  const deTimeout = deRpc.filter((x) => x.entrada.op === "rpc-timeout");
  for (const [i, tv] of casoRpc.timeoutVectors.entries()) {
    const entrada = Object.hasOwn(tv, "timeoutMs") ? { timeoutMs: tv.timeoutMs } : {};
    const os = deTimeout.filter((x) => entradaSinOp(x) === json(entrada));
    if (os.length !== 1) { problemas.push(`el vector de timeout #${i} del caso (${json(entrada)}) está ${os.length} veces en vectors.json: debe estar una`); continue; }
    const [o] = os;
    if (o.esperadoEfectivo !== tv.effectiveMs || o.esperado !== tv.errorCode) {
      problemas.push(`"${o.id}": espera ${o.esperadoEfectivo ?? o.esperado}, el caso dice ${tv.effectiveMs ?? tv.errorCode}`);
    }
  }

  // protocol.json → rpc: lo que declara válido o inválido, y los límites del timeout, tienen su vector.
  const op = protocolo.rpc?.operation ?? {};
  for (const v of op.valid ?? []) {
    const o = deOperacion.find((x) => x.entrada.operation === v);
    if (o?.esperadoSubject !== `rpc.${v}`) problemas.push(`protocol.json → rpc.operation.valid: «${v}» no tiene un vector que espere \`rpc.${v}\``);
  }
  const reservados = new Set(protocolo.naming?.reservedDomains?.names ?? []);
  for (const v of op.invalid ?? []) {
    const esperado = reservados.has(v.split(".")[0]) ? "RESERVED_DOMAIN" : "INVALID_RPC_CALL";
    const o = deOperacion.find((x) => x.entrada.operation === v);
    if (o?.esperado !== esperado) problemas.push(`protocol.json → rpc.operation.invalid: «${v}» no tiene un vector que espere ${esperado}`);
  }
  const t = protocolo.rpc?.timeout ?? {};
  const porTimeout = (entrada) => deTimeout.find((x) => entradaSinOp(x) === json(entrada));
  const limites = [
    [{}, { efectivo: t.defaultMs }],
    [{ timeoutMs: t.minMs }, { efectivo: t.minMs }],
    [{ timeoutMs: t.maxMs }, { efectivo: t.maxMs }],
    [{ timeoutMs: t.minMs - 1 }, { codigo: t.outOfRangeCode }],
    [{ timeoutMs: t.maxMs + 1 }, { codigo: t.outOfRangeCode }],
  ];
  for (const [entrada, { efectivo, codigo }] of limites) {
    const o = porTimeout(entrada);
    if (!o || o.esperadoEfectivo !== efectivo || o.esperado !== codigo) {
      problemas.push(`protocol.json → rpc.timeout: ${json(entrada)} debería esperar ${efectivo ?? codigo}, y ${o ? `el vector «${o.id}» espera ${o.esperadoEfectivo ?? o.esperado}` : "no hay vector"}`);
    }
  }
  return problemas;
}

{
  const incoherencias = [
    ...coherenciaConElCaso().map((p) => ["cases/cross-sdk-state-envelope.json", p]),
    ...coherenciaDeRpc().map((p) => ["cases/cross-sdk-rpc-envelope.json", p]),
  ];
  if (incoherencias.length) {
    for (const caso of new Set(incoherencias.map(([c]) => c))) {
      console.error(red(bold(`vectors.json y ${caso} ya no coinciden:`)));
      for (const [c, p] of incoherencias) if (c === caso) console.error(`  ${red("✗")} ${p}`);
    }
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

/**
 * Lo que la SPEC dice de los bytes de rpc (11-request-reply.md §3), comprobado sobre la salida de REFERENCIA y
 * calculado aquí SIN llamar a ningún SDK: el orden normativo de los atributos, qué hereda la respuesta, qué es
 * fijo en el error, qué atributos NO aparecen si no se dieron. Es lo que hace que un vector sin `sha256` (cuyos
 * bytes fija Node) no sea «lo que dice Node» sino lo que dice la spec.
 */
function invariantesDeRpc(op, b64) {
  const problemas = [];
  const texto = Buffer.from(b64, "base64").toString("utf8");
  let ev;
  try { ev = JSON.parse(texto); } catch { return ["los bytes no son JSON"]; }
  const e = op.entrada;
  const claves = Object.keys(ev);

  // 01-envelope.md §6: el orden de los atributos es normativo y `data` va el último.
  let previo = -1;
  for (const k of claves) {
    const i = casoRpc.attributeOrder.indexOf(k);
    if (i < 0) problemas.push(`el atributo \`${k}\` no existe en el sobre`);
    else if (i < previo) problemas.push(`\`${k}\` está fuera de su sitio: el orden de los atributos es normativo (01-envelope.md §6)`);
    else previo = i;
  }
  if (claves.at(-1) !== "data") problemas.push("`data` no es el último atributo (01-envelope.md §6)");
  for (const k of ["signature", "signkeyid", "dlqreason", "dlqattempts", "dlqconsumer", "dlqerror", "dlqtime"]) {
    if (k in ev) problemas.push(`lleva \`${k}\`: una petición, una respuesta y un error de rpc no van firmados aquí ni pasan por la DLQ`);
  }
  // Solo los de control (U+0000–U+001F) se escapan: los demás son UTF-8 literal (01-envelope.md §1.1).
  if (/\\u(?!00[01][0-9a-f])[0-9a-f]{4}/i.test(texto)) problemas.push("lleva un escape \\u de un carácter que no es de control: debe ser UTF-8 literal");

  if (ev.specversion !== "1.0") problemas.push(`specversion ${ev.specversion}`);
  if (ev.datacontenttype !== "application/json") problemas.push(`datacontenttype ${ev.datacontenttype}`);
  for (const k of ["id", "source", "time", "producerversion", "traceparent"]) {
    if (ev[k] !== e[k]) problemas.push(`\`${k}\` es ${JSON.stringify(ev[k])} y la entrada dice ${JSON.stringify(e[k])}`);
  }

  const ref = e.op === "rpc-request" ? e : e.request;
  const [dom, agr, mayor, oper] = ref.operation.split(".");
  const dataschema = (lado) => `https://schemas.internal/rpc/${dom}/${agr}/${oper}/${lado}/${mayor.slice(1)}.0.0.json`;
  const mismoData = (esperado) => JSON.stringify(ev.data) === JSON.stringify(esperado);

  if (e.op === "rpc-request") {
    if (ev.type !== `com.flux.rpc.${dom}.${agr}.${oper}.request.${mayor}`) problemas.push(`type ${ev.type} (11-request-reply.md §3.1)`);
    if (ev.dataschema !== dataschema("request")) problemas.push(`dataschema ${ev.dataschema} (11-request-reply.md §3.1)`);
    if (ev.subject !== e.aggregateId) problemas.push("`subject` no es el `aggregateId`, o aparece sin darlo (§3.1)");
    if (ev.partitionkey !== e.aggregateId) problemas.push("`partitionkey` no es el `aggregateId`, o aparece sin darlo (§3.1)");
    if (ev.correlationid !== e.correlationid) problemas.push("`correlationid` no es el pedido");
    if (ev.causationid !== e.causationid) problemas.push("`causationid` no es el pedido, o aparece sin darlo (§3.1)");
    if (ev.tenantid !== e.tenantId) problemas.push("`tenantid` no es el pedido");
    if (ev.dataclassification !== e.classification) problemas.push("`dataclassification` no es la pedida");
    if (ev.tracestate !== e.tracestate) problemas.push("`tracestate` no es el pedido, o aparece sin darlo");
    if (!mismoData(e.data)) problemas.push("`data` no conserva el orden de claves ni los valores del vector (01-envelope.md §6)");
    return problemas;
  }

  // La respuesta y el error HEREDAN de la petición (§3.2) y no escriben a mano nada de eso.
  if (ev.correlationid !== ref.correlationid) problemas.push("`correlationid` no es el de la petición (§3.2)");
  if (ev.tenantid !== ref.tenantid) problemas.push("`tenantid` no es el de la petición (§3.2)");
  if (ev.subject !== ref.subject) problemas.push("`subject` no es el de la petición, o aparece sin traerlo ella (§3.2)");
  if (ev.partitionkey !== (ref.partitionkey ?? ref.subject)) problemas.push("`partitionkey` no es el de la petición, o aparece sin traerlo ella (§3.2)");
  if (ev.causationid !== ref.id) problemas.push("`causationid` no es el `id` de la petición (§3.2)");
  if (ev.id === ref.id) problemas.push("el `id` de la respuesta es el de la petición: debe ser nuevo (§3.2)");
  if ("tracestate" in ev) problemas.push("`tracestate` en una respuesta: §3.2 no lo prevé");

  if (e.op === "rpc-reply") {
    if (ev.type !== `com.flux.rpc.${dom}.${agr}.${oper}.reply.${mayor}`) problemas.push(`type ${ev.type} (§3.2)`);
    if (ev.dataschema !== dataschema("reply")) problemas.push(`dataschema ${ev.dataschema} (§3.2)`);
    if (ev.dataclassification !== e.classification) problemas.push("`dataclassification` no es la pedida");
    if (!mismoData(e.data)) problemas.push("`data` no conserva el orden de claves ni los valores del vector (01-envelope.md §6)");
  } else {
    if (ev.type !== "com.flux.rpc.error.v1") problemas.push(`type ${ev.type} (§3.3)`);
    if (ev.dataschema !== "https://schemas.internal/system/rpc/error/1.0.0.json") problemas.push("dataschema del error no es el fijo (§3.3)");
    if (ev.dataclassification !== "internal") problemas.push("el error no es `internal` (§3.3)");
    const esperadas = ["class", "code", "message", ...(e.error.retryAfterMs !== undefined ? ["retryAfterMs"] : [])];
    if (Object.keys(ev.data).join() !== esperadas.join()) problemas.push(`las claves de \`data\` del error son «${Object.keys(ev.data).join()}» y deben ser «${esperadas.join()}», en ese orden (§3.3)`);
    if (!["retryable", "permanent"].includes(ev.data.class)) problemas.push("`class` del error no es `retryable` ni `permanent` (§3.3)");
    if (!mismoData(e.error)) problemas.push("`data` del error no es el `error` de la entrada, con sus claves en el mismo orden");
  }
  return problemas;
}

// ─── Ejecutar ────────────────────────────────────────────────────────────────

const activos = registro.harnesses.filter((h) => !solo || solo.has(h.sdk));
const saltados = registro.harnesses.filter((h) => solo && !solo.has(h.sdk));
const referencia = activos.find((h) => h.reference) ?? activos[0];

const generales = operaciones.filter((o) => !o.capacidad);
/** cap → { ops, con (los SDKs activos que la declaran), sin (los que no) } */
const porCapacidad = Object.fromEntries(Object.keys(CAPACIDADES).map((cap) => [cap, {
  ops: operaciones.filter((o) => o.capacidad === cap),
  con: activos.filter((h) => declara(h.sdk, cap)),
  sin: activos.filter((h) => !declara(h.sdk, cap)),
}]));

// Un SDK que declara una capacidad y NO tiene arnés registrado (o figura en `pending`) no se ejecutaría
// nunca, ni siquiera para fallar: la capacidad quedaría declarada sin evidencia. Se mira contra el
// registro entero, no contra `--only`: excluir un SDK de una ejecución no es quitarle el arnés.
const declaranSinArnes = [];
for (const cap of Object.keys(CAPACIDADES)) {
  for (const sdk of Object.keys(protocolo.capabilities?.sdks ?? {})) {
    if (declara(sdk, cap) && (!registro.harnesses.some((h) => h.sdk === sdk) || (registro.pending ?? []).includes(sdk))) {
      declaranSinArnes.push({ sdk, cap });
    }
  }
}

// `--exigir`: que la capacidad se haya EJECUTADO en algún SDK. Sin esto, un protocol.json que dejase de
// declararla (o un `--only` que dejase fuera a quien la declara) daría un «N/A» verde en CI.
const exigenciasIncumplidas = [];
for (const cap of exigidas) {
  if (!CAPACIDADES[cap]) {
    console.error(red(`--exigir: la capacidad "${cap}" no existe. Las hay: ${Object.keys(CAPACIDADES).join(", ")}`));
    process.exit(2);
  }
  if (!porCapacidad[cap].con.length) exigenciasIncumplidas.push(cap);
}

console.log(bold(
  `\nConformidad cruzada — ${generales.length} vectores × ${activos.length} SDK(s)` +
  Object.entries(porCapacidad)
    .filter(([, c]) => c.ops.length)
    .map(([cap, c]) => `  +  ${c.ops.length} ${CAPACIDADES[cap].etiqueta} × ${c.con.length} SDK(s) con \`${cap}\``)
    .join(""),
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
const conEncabezado = new Set();

for (const op of operaciones) {
  if (op.capacidad && !conEncabezado.has(op.capacidad)) {
    conEncabezado.add(op.capacidad);
    console.log(gray(`\n  ── ${CAPACIDADES[op.capacidad].nombre} (capacidad \`${op.capacidad}\`) · «–» = N/A: no declara ${op.capacidad} ──`));
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

  const invariantes = ref?.ok && op.capacidad && op.tipo === "bytes"
    ? (op.capacidad === "rpc" ? invariantesDeRpc : invariantesDeEstado)(op, ref.bytes)
    : [];
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
    } else if (op.tipo === "efectivo") {
      ok = r.ok === true && r.effectiveMs === op.esperadoEfectivo;
      if (!ok) {
        fallos.push({ op, sdk: h.sdk,
          detalle: r.ok
            ? `${JSON.stringify(r.effectiveMs)} ms efectivos, esperaba ${op.esperadoEfectivo}`
            : `esperaba ${op.esperadoEfectivo} ms, lo rechazó: code="${r.code ?? "?"}"` });
      }
    } else {
      // bytes: con `sha256` el patrón es la SPEC; sin él, el SDK de referencia.
      const objetivo = op.sha256 ? null : ref.bytes;
      const obtenido = r.ok ? sha256(r.bytes) : null;
      const longitud = r.ok ? Buffer.from(r.bytes, "base64").length : null;
      ok = r.ok === true && (op.sha256 ? obtenido === op.sha256 : r.bytes === objetivo);
      if (ok && op.longitud !== undefined && longitud !== op.longitud) ok = false;
      if (!ok) {
        const difiereDeLaReferencia = r.ok && ref.ok && r.bytes !== ref.bytes && h !== ref0;
        fallos.push({ op, sdk: h.sdk,
          detalle: r.ok
            ? (op.sha256
                ? (obtenido !== op.sha256
                    ? `sha256 ${obtenido}, y la spec fija ${op.sha256}`
                    : `${longitud} bytes, y la spec fija ${op.longitud}`)
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
    : op.tipo === "efectivo" ? `${op.id} ${gray(`→ ${op.esperadoEfectivo} ms`)}`
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

for (const [cap, c] of Object.entries(porCapacidad)) {
  const { etiqueta } = CAPACIDADES[cap];
  if (c.ops.length && c.sin.length) {
    // Tampoco en silencio: es la CAPACIDAD la que no se declara, no un SDK que se olvidó.
    console.log(yellow(
      `\n! N/A: no declara ${cap} — ${c.sin.map((h) => h.sdk).join(", ")}. ` +
      `Sus ${c.ops.length} vectores ${etiqueta} no se ejecutan (protocol.json → capabilities); no es un fallo.`,
    ));
  }
  if (c.ops.length && !c.con.length) {
    console.log(yellow(`! ningún SDK activo declara ${cap}: los ${c.ops.length} vectores ${etiqueta} NO se ejecutaron`));
  }
}

if (declaranSinArnes.length) {
  console.log(red(bold(`\n✗ ${declaranSinArnes.length} declaración(es) de capacidad sin arnés registrado`)));
  for (const { sdk, cap } of declaranSinArnes) {
    console.log(`  ${red("✗")} ${sdk}: protocol.json declara \`${cap}\`, pero no está en conformance/harnesses.json (o figura en \`pending\`): sin arnés no hay evidencia`);
  }
}

if (exigenciasIncumplidas.length) {
  console.log(red(bold(`\n✗ --exigir: ningún SDK activo ejecutó ${exigenciasIncumplidas.map((c) => `\`${c}\``).join(", ")}`)));
  for (const cap of exigenciasIncumplidas) {
    console.log(`  ${red("✗")} ${cap}: ningún SDK de los activos la declara en protocol.json → capabilities, y un «N/A» en todos no demuestra nada`);
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

const fallido = fallos.length || arnesesRotos.length || declaranSinArnes.length || exigenciasIncumplidas.length;
if (!fallido) {
  console.log(green(bold(`\n✓ ${generales.length} vectores idénticos en ${activos.length} SDK(s)`)));
  for (const [cap, c] of Object.entries(porCapacidad)) {
    if (!c.ops.length || !c.con.length) continue;
    console.log(
      green(bold(`✓ ${c.ops.length} vectores ${CAPACIDADES[cap].etiqueta} idénticos en ${c.con.length} SDK(s): ${c.con.map((h) => h.sdk).join(", ")}`)) +
      (c.sin.length ? yellow(`  ·  N/A en ${c.sin.length}: ${c.sin.map((h) => h.sdk).join(", ")}`) : ""),
    );
  }
}

process.exit(fallido ? 1 : 0);
