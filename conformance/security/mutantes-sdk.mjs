#!/usr/bin/env node
/**
 * Mutantes de los SDKs: la suite con SDKs REALES debe FALLAR con cada uno.
 *
 *   node conformance/security/mutantes-sdk.mjs                 # todos
 *   node conformance/security/mutantes-sdk.mjs --only node-inbox-en-connect,rust-inbox-en-connect
 *   node conformance/security/mutantes-sdk.mjs --no-rust       # sin los que compilan Rust (~1 min cada uno)
 *   node conformance/security/mutantes-sdk.mjs --list
 *
 * `security/mutantes.mjs` estropea el GENERADOR de permisos y comprueba que los casos de seguridad lo notan. Esto estropea el
 * SDK: copia `sdk-node/dist` (o `sdk-rust`) a un temporal, le aplica UN cambio de texto —el que haría un error real: que el SDK
 * abra una suscripción al inbox por la conexión con la que pide, en `connect()`, en el pull de `subscribe()` o en `state.watch()`
 * (A-101: el broker deja de mandar `no responders` y `RPC_UNAVAILABLE` pasa a ser un timeout)— y ejecuta la suite con
 * `FLUX_SDK_NODE_DIST` / `FLUX_SDK_RUST_DIR` apuntando a la copia. Cada mutante declara qué casos DEBEN fallar. Antes, esos mismos
 * casos se ejecutan SIN mutar y tienen que pasar: si no, un fallo no probaría nada.
 *
 * El repo real no se toca: el mutante vive en `os.tmpdir()` y se borra al terminar. El de Rust se compila en el MISMO `target/`
 * (`CARGO_TARGET_DIR`, por defecto el de `sdk-rust/`): las dependencias, ya compiladas, no se repiten, y solo se recompila el
 * crate `flux` de la copia (~30 s); la siguiente ejecución normal vuelve a recompilar el suyo.
 *
 * Entorno: el de `security.mjs` (NATS_SERVER, FLUX_TEST_PORTS, FLUX_REQUIRE_BROKER).
 * Sale con 0 si todos los mutantes se detectan, 1 si alguno NO, 2 si el entorno está mal.
 */

import { spawn, spawnSync } from "node:child_process";
import { cp, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const AQUI = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(AQUI, "..", "..");

const RUNNERS = {
  security: path.join(ROOT, "conformance", "security.mjs"),
  interop: path.join(ROOT, "conformance", "interop-rpc.mjs"),
};
const INBOX_NODE = "`_INBOX.${options.service}.>`";

/**
 * Cada mutante: un cambio de TEXTO en un fichero de la copia (`de` debe aparecer EXACTAMENTE una vez: si el SDK cambia y el
 * ancla desaparece, el mutante lo dice en vez de «no cambiar nada y pasar») y los casos que DEBEN fallar.
 */
export const MUTANTES = [
  {
    nombre: "node-inbox-en-connect",
    sdk: "node",
    que: "el SDK de Node se suscribe a `_INBOX.<servicio>.>` al conectar, por la conexión con la que pide: el 503 de `no responders` no llega",
    archivo: "client.js",
    de: "const nc = await connectWhenAvailable(natsOptionsFor(options));",
    a: `const nc = await connectWhenAvailable(natsOptionsFor(options));\n    nc.subscribe(${INBOX_NODE});`,
    debenFallar: ["S30", "S32"],
  },
  {
    nombre: "node-inbox-en-subscribe",
    sdk: "node",
    que: "el pull de `subscribe()` abre una suscripción `_INBOX.<servicio>.>` en la conexión: un servicio que consume y llama recibe timeouts",
    archivo: "client.js",
    de: "    async subscribe(subject, handler, options = {}) {\n        const { domain } = parseSubject(subject);",
    a: "    async subscribe(subject, handler, options = {}) {\n        const { domain } = parseSubject(subject);\n        this.#nc.subscribe(`_INBOX.${this.#opts.service}.>`);",
    debenFallar: ["S32"],
  },
  {
    nombre: "node-inbox-en-watch",
    sdk: "node",
    que: "`state.watch()` abre una suscripción `_INBOX.<servicio>.>` en la conexión: un servicio que observa estado y llama recibe timeouts",
    archivo: "state.js",
    de: "    async watch(contract, handler, options = {}) {\n        const c = parseStateContract(contract);",
    a: "    async watch(contract, handler, options = {}) {\n        const c = parseStateContract(contract);\n        this.#h.nc.subscribe(`_INBOX.${this.#h.options.service}.>`);",
    debenFallar: ["S32"],
  },
  {
    nombre: "rust-inbox-en-connect",
    sdk: "rust",
    lento: true,
    que: "el SDK de Rust se suscribe a `_INBOX.<servicio>.>` al conectar, por la conexión con la que pide",
    archivo: "src/client.rs",
    de: "    let client = open_connection(&nats, &opts, &servers).await?;\n",
    a: "    let client = open_connection(&nats, &opts, &servers).await?;\n    std::mem::forget(client.subscribe(format!(\"{}.>\", inbox)).await.unwrap());\n",
    debenFallar: ["S31", "S33"],
  },
  {
    nombre: "rust-inbox-en-subscribe",
    sdk: "rust",
    lento: true,
    que: "el pull de `Bus::subscribe_with` abre una suscripción `_INBOX.<servicio>.>` en la conexión",
    archivo: "src/client.rs",
    de: "        crate::state::reject_reserved_domain(subject)?;\n        let parsed = parse_subject(subject)?;\n\n        // El filtro efectivo se resuelve ANTES",
    a: "        crate::state::reject_reserved_domain(subject)?;\n        let parsed = parse_subject(subject)?;\n        std::mem::forget(self.inner.client.subscribe(format!(\"{}.>\", self.inner.inbox_prefix)).await.unwrap());\n\n        // El filtro efectivo se resuelve ANTES",
    debenFallar: ["S33"],
  },
  // ── interoperabilidad (`interop-rpc.mjs`): un SDK que se aparta de lo que el otro espera ──
  {
    nombre: "node-respuesta-sin-tenant",
    sdk: "node",
    runner: "interop",
    que: "la respuesta del SDK de Node no hereda el `tenantid` de la petición (pone `system`): quien llama la rechaza como RPC_INVALID_REPLY",
    archivo: "rpc.js",
    de: "        correlationid: input.request.correlationid,\n        tenantid: input.request.tenantid,\n        producerversion: input.producerversion,\n        dataclassification: classification,",
    a: "        correlationid: input.request.correlationid,\n        tenantid: \"system\",\n        producerversion: input.producerversion,\n        dataclassification: classification,",
    debenFallar: ["I1[node→node]", "I1[rust→node]"],
  },
  {
    nombre: "node-error-desconocido-con-texto",
    sdk: "node",
    runner: "interop",
    que: "el SDK de Node reenvía el TEXTO de una excepción desconocida a quien llama (valores de `data`, rutas, datos personales)",
    archivo: "rpc.js",
    de: 'return { class: clase, code: "UNKNOWN", message: "error interno del servicio" };',
    a: 'return { class: clase, code: "UNKNOWN", message: String(e?.message ?? "error interno del servicio") };',
    debenFallar: ["I2[node→node]", "I2[rust→node]"],
  },
  {
    nombre: "node-reintento-con-id-nuevo",
    sdk: "node",
    runner: "interop",
    que: "un reintento del SDK de Node construye una petición NUEVA (otro `id`): quien responde no puede reconocer la orden repetida",
    archivo: "rpc.js",
    de: "const out = await this.#attempt(prep);",
    a: "const out = await this.#attempt(intento > 1 ? await this.#prepare(operation, data, options) : prep);",
    debenFallar: ["I5[node→node]", "I5[node→rust]"],
  },
  {
    nombre: "node-responder-no-verifica-la-firma",
    sdk: "node",
    runner: "interop",
    que: "el SDK de Node, con la firma en `require`, atiende una petición SIN firma: ejecuta lo que debía rechazar",
    archivo: "rpc.js",
    de: "        // 5. Con la firma en `require`: verificar, y la ventana de 300 s.\n        try {\n            h.verify(event); // `warn` registra el fallo y sigue (la propia verificación lo escribe); `require` lanza\n",
    a: "        // 5. Con la firma en `require`: verificar, y la ventana de 300 s.\n        try {\n            void 0;\n",
    debenFallar: ["I3[node→node]", "I3[rust→node]"],
  },
  {
    nombre: "rust-respuesta-sin-tenant",
    sdk: "rust",
    runner: "interop",
    lento: true,
    que: "la respuesta del SDK de Rust no hereda el `tenantid` de la petición (pone `system`)",
    archivo: "src/rpc.rs",
    de: "            tenantid: request.tenantid.clone(),\n            causationid: Some(request.id.clone()),",
    a: "            tenantid: \"system\".to_string(),\n            causationid: Some(request.id.clone()),",
    debenFallar: ["I1[node→rust]", "I1[rust→rust]"],
  },
  {
    nombre: "rust-error-desconocido-con-texto",
    sdk: "rust",
    runner: "interop",
    lento: true,
    que: "el SDK de Rust reenvía el TEXTO de una excepción desconocida a quien llama",
    archivo: "src/rpc.rs",
    de: '            code: "UNKNOWN".to_string(),\n            message: "error interno del servicio".to_string(),\n            retry_after_ms: None,\n        }\n    } else {',
    a: '            code: "UNKNOWN".to_string(),\n            message: describe(error),\n            retry_after_ms: None,\n        }\n    } else {',
    debenFallar: ["I2[node→rust]", "I2[rust→rust]"],
  },
];

// ─── Argumentos ──────────────────────────────────────────────────────────────

const argv = process.argv.slice(2);
const o = { noRust: false, only: undefined, list: false, verbose: false };
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (a === "--no-rust") o.noRust = true;
  else if (a === "--list") o.list = true;
  else if (a === "-v" || a === "--verbose") o.verbose = true;
  else if (a === "--only") o.only = new Set((argv[++i] ?? "").split(",").map((s) => s.trim()).filter(Boolean));
  else {
    console.error(`argumento desconocido: ${a}\nuso: node conformance/security/mutantes-sdk.mjs [--no-rust] [--only a,b] [--list] [-v]`);
    process.exit(2);
  }
}
for (const n of o.only ?? []) {
  if (!MUTANTES.some((m) => m.nombre === n)) {
    console.error(`no existe el mutante «${n}». Los hay: ${MUTANTES.map((m) => m.nombre).join(", ")}`);
    process.exit(2);
  }
}
if (o.list) {
  for (const m of MUTANTES) console.log(`${m.nombre.padEnd(36)} ${m.lento ? "[rust] " : "       "}${m.que}  → debe fallar ${m.debenFallar.join(", ")}`);
  process.exit(0);
}
const elegidos = MUTANTES.filter((m) => (o.only ? o.only.has(m.nombre) : !(o.noRust && m.sdk === "rust")));

// ─── Ejecución ───────────────────────────────────────────────────────────────

const exigido = !["", "0", "false"].includes((process.env.FLUX_REQUIRE_BROKER ?? "").trim().toLowerCase());
const bin = process.env.NATS_SERVER ?? "nats-server";
const v = spawnSync(bin, ["--version"], { encoding: "utf8" });
if (v.error || v.status !== 0) {
  console.error(`${exigido ? "✗" : "AVISO:"} no se pudo ejecutar nats-server («${bin}»): los mutantes necesitan un broker.`);
  process.exit(exigido ? 1 : 0);
}

/** Ejecuta la suite sobre `casos` y devuelve `{ codigo, casos: Map(id → ok), salida }`. `runner`: `security` o `interop`. */
function ejecutarSuite(runner, casos, env) {
  return new Promise((resolver) => {
    const hijo = spawn(process.execPath, [RUNNERS[runner], "--only", casos.join(",")], { cwd: ROOT, env: { ...process.env, ...env }, stdio: ["ignore", "pipe", "pipe"] });
    let salida = "";
    hijo.stdout.on("data", (d) => (salida += d));
    hijo.stderr.on("data", (d) => (salida += d));
    hijo.on("close", (codigo) => {
      const resumen = salida.split("\nResumen\n")[1] ?? "";
      const resultados = new Map();
      for (const l of resumen.split("\n")) {
        const m = /^\s+([✓✗-])\s+(\S+)\s/.exec(l);
        if (m) resultados.set(m[2], m[1] === "✓");
      }
      resolver({ codigo, casos: resultados, salida });
    });
  });
}

/** Copia el SDK a un temporal y aplica el mutante. Devuelve el entorno con el que ejecutar la suite. */
async function prepararMutante(m) {
  const dir = await mkdtemp(path.join(os.tmpdir(), "flux-mutante-sdk-"));
  let ruta;
  let env;
  if (m.sdk === "node") {
    await cp(path.join(ROOT, "sdk-node", "dist"), path.join(dir, "sdk-node", "dist"), { recursive: true });
    await cp(path.join(ROOT, "sdk-node", "package.json"), path.join(dir, "sdk-node", "package.json"));
    // Las dependencias son las del SDK real: el mismo módulo de NATS, la misma instancia.
    await symlink(path.join(ROOT, "sdk-node", "node_modules"), path.join(dir, "sdk-node", "node_modules"));
    ruta = path.join(dir, "sdk-node", "dist", m.archivo);
    env = { FLUX_SDK_NODE_DIST: path.join(dir, "sdk-node", "dist") };
  } else {
    await cp(path.join(ROOT, "sdk-rust"), path.join(dir, "sdk-rust"), {
      recursive: true,
      filter: (p) => !p.split(path.sep).includes("target"),
    });
    ruta = path.join(dir, "sdk-rust", m.archivo);
    env = {
      FLUX_SDK_RUST_DIR: path.join(dir, "sdk-rust"),
      CARGO_TARGET_DIR: process.env.CARGO_TARGET_DIR ?? path.join(ROOT, "sdk-rust", "target"),
    };
  }
  const texto = await readFile(ruta, "utf8");
  const veces = texto.split(m.de).length - 1;
  if (veces !== 1) {
    await rm(dir, { recursive: true, force: true });
    throw new Error(`el ancla del mutante «${m.nombre}» aparece ${veces} veces en ${m.archivo} (debe ser UNA): el SDK cambió y el mutante ya no muta nada`);
  }
  await writeFile(ruta, texto.replace(m.de, () => m.a));
  return { dir, env };
}

const R = { detectados: 0, escapan: [], total: elegidos.length };
console.log(`flux — mutantes de los SDKs: la suite con SDKs reales debe FALLAR con cada uno (${elegidos.length})`);
console.log(`  ${v.stdout.trim()} · ${o.noRust ? "sin los de Rust" : "todos"}`);

// Sin mutar: el subconjunto de cada runner debe pasar. Si no, un fallo con un mutante no probaría nada.
const porRunner = new Map();
for (const m of elegidos) {
  const r = m.runner ?? "security";
  porRunner.set(r, [...new Set([...(porRunner.get(r) ?? []), ...m.debenFallar])]);
}
let baseOk = true;
for (const [runner, casos] of porRunner) {
  console.log(`\nSin mutar (${runner}): ${casos.join(", ")}`);
  const base = await ejecutarSuite(runner, casos, {});
  const ok = base.codigo === 0 && casos.every((c) => base.casos.get(c) === true);
  console.log(`  ${ok ? "✓" : "✗"} la suite sin mutar pasa ${casos.length} caso(s)`);
  if (!ok) {
    console.log(base.salida.split("\n").slice(-30).join("\n"));
    baseOk = false;
  }
}
if (!baseOk) {
  console.log("\n✗ la suite falla SIN mutar: no se puede medir nada.");
  process.exit(1);
}

for (const m of elegidos) {
  let prep;
  try {
    prep = await prepararMutante(m);
  } catch (e) {
    console.log(`\n${m.nombre}\n  ✗ ${e.message}`);
    R.escapan.push(m.nombre);
    continue;
  }
  try {
    console.log(`\n${m.nombre}: ${m.que}`);
    const r = await ejecutarSuite(m.runner ?? "security", m.debenFallar, prep.env);
    const fallan = m.debenFallar.filter((c) => r.casos.get(c) === false);
    const noFallan = m.debenFallar.filter((c) => r.casos.get(c) !== false);
    const detectado = r.codigo === 1 && noFallan.length === 0;
    if (detectado) R.detectados++;
    else R.escapan.push(m.nombre);
    console.log(
      `  ${detectado ? "✓ DETECTADO" : "✗ NO DETECTADO"}: fallan ${fallan.length ? fallan.join(", ") : "ninguno"} de los esperados (${m.debenFallar.join(", ")}); salida ${r.codigo}` +
        (noFallan.length ? `; NO fallan: ${noFallan.join(", ")}` : ""),
    );
    if (!detectado || o.verbose) console.log(r.salida.split("\n").filter((l) => /✗|✓ \d+\/|se interrumpió|Error|error\[/.test(l)).slice(0, 14).join("\n"));
  } finally {
    await rm(prep.dir, { recursive: true, force: true });
  }
}

console.log(`\n${R.escapan.length === 0 ? "✓" : "✗"} ${R.detectados}/${R.total} mutantes detectados` + (R.escapan.length ? ` · NO detectados: ${R.escapan.join(", ")}` : ""));
process.exit(R.escapan.length === 0 ? 0 : 1);
