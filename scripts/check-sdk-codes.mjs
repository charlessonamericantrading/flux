#!/usr/bin/env node
/**
 * Comprueba que cada SDK contiene los códigos de error de las capacidades que declara.
 *
 *   node scripts/check-sdk-codes.mjs
 *
 * Existe por la misma razón que el resto del repo: una regla que solo existe en un SDK es
 * la spec de facto, y una capacidad que solo existe en una tabla del README es una intención.
 * `protocol.json` → `capabilities` dice qué SDK declara qué extensión opcional
 * (specification/00-protocol.md §5); este script lo cruza con lo que hay de verdad:
 *
 *   1. Cada SDK que declara una capacidad contiene, **como literal de cadena**, cada código de
 *      error de esa capacidad. Los códigos salen de `protocol.json` → `errors.codes` (campo
 *      `capability`) y de `signing.poisonCodes` (capacidad `signing`).
 *   2. La **evidencia** de la capacidad existe: `cross-sdk-harness` (el SDK está en
 *      `conformance/harnesses.json` y no en `pending`) o `sdk-files` (los ficheros citados
 *      existen y, entre todos, nombran cada código).
 *   3. Los siete SDKs están declarados (aunque sea con una lista vacía): un SDK nuevo no puede
 *      olvidarse de decir qué capacidades tiene.
 *   4. La columna «Capacidades» de la tabla «Estado de los SDKs» del README coincide con
 *      `protocol.json`: dos fuentes que dicen lo mismo, y una comprobación de que lo dicen.
 *
 * LÍMITE, dicho en voz alta: que un código esté escrito **no prueba que se emita**. Es
 * necesario y no suficiente; lo que prueba que se emite son los tests de integración contra un
 * broker real que la evidencia cita. Por eso (2) exige que esos tests nombren cada código.
 *
 * Se mira solo código de producción: `src/` (Node, .NET, Rust, PHP), `flux/` (Python), la raíz
 * del paquete (Go, sin `_test.go` ni `cmd/`) y `src/main` (Java). En Rust, además, se descarta
 * lo que sigue al primer `#[cfg(test)]` de cada fichero —por convención los tests unitarios
 * van al final—, para que un código que solo aparece en un test no cuente como emitido.
 *
 * La autocomprobación (se ejecuta siempre, en memoria) aplica una serie de mutaciones sobre los
 * datos reales —quitar un código de un SDK, declarar una capacidad que no se tiene, tocar el
 * README…— y exige que **cada una** haga fallar la comprobación. Una comprobación que solo se
 * ha visto en verde no demuestra nada.
 */

import { readFile, readdir } from "node:fs/promises";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

/** Dónde vive el código de producción de cada SDK. */
const SDKS = {
  node: { dir: "sdk-node", fuentes: ["src"], ext: [".ts"] },
  python: { dir: "sdk-python", fuentes: ["flux"], ext: [".py"] },
  go: { dir: "sdk-go", fuentes: ["."], ext: [".go"], plano: true, excluye: /_test\.go$/ },
  java: { dir: "sdk-java", fuentes: ["src/main"], ext: [".java"] },
  dotnet: { dir: "sdk-dotnet", fuentes: ["src"], ext: [".cs"] },
  rust: { dir: "sdk-rust", fuentes: ["src"], ext: [".rs"], cortaEnTests: /^\s*#\[cfg\(test\)\]/m },
  php: { dir: "sdk-php", fuentes: ["src"], ext: [".php"] },
};

const IGNORAR = new Set(["node_modules", "target", "dist", "vendor", "bin", "obj", ".git"]);

// ─── Lectura del disco ───────────────────────────────────────────────────────

async function ficherosDe(dir, { ext, plano, excluye }) {
  const out = [];
  async function recorre(d) {
    let entradas;
    try {
      entradas = await readdir(d, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of entradas) {
      const ruta = join(d, e.name);
      if (e.isDirectory()) {
        if (!plano && !IGNORAR.has(e.name)) await recorre(ruta);
      } else if (ext.some((x) => e.name.endsWith(x)) && !(excluye && excluye.test(e.name))) {
        out.push(ruta);
      }
    }
  }
  await recorre(dir);
  return out.sort();
}

async function fuenteDe(sdk) {
  const cfg = SDKS[sdk];
  const partes = [];
  for (const sub of cfg.fuentes) {
    for (const f of await ficherosDe(join(ROOT, cfg.dir, sub), cfg)) {
      let txt = await readFile(f, "utf8");
      if (cfg.cortaEnTests) {
        const i = txt.search(cfg.cortaEnTests);
        if (i >= 0) txt = txt.slice(0, i);
      }
      partes.push(txt);
    }
  }
  return partes.join("\n");
}

const leeOpcional = (ruta) => readFile(join(ROOT, ruta), "utf8").catch(() => null);

// ─── La comprobación (pura: recibe los datos, devuelve los fallos) ───────────

/** Un código como literal de cadena entre comillas — `"X"`, `'X'` o `` `X` ``. */
const conLiteral = (texto, codigo) => new RegExp(`(["'\`])${codigo}\\1`).test(texto ?? "");
const sinLiteral = (texto, codigo) => texto.replace(new RegExp(`(["'\`])${codigo}\\1`, "g"), "");

/** capacidad → [{ codigo, origen }] */
function codigosPorCapacidad(protocol) {
  const out = new Map();
  const add = (cap, codigo, origen) => {
    if (!out.has(cap)) out.set(cap, []);
    out.get(cap).push({ codigo, origen });
  };
  for (const [codigo, def] of Object.entries(protocol.errors?.codes ?? {})) {
    if (!codigo.startsWith("$")) add(def.capability, codigo, `errors.codes.${codigo}`);
  }
  for (const codigo of Object.keys(protocol.signing?.poisonCodes ?? {})) {
    add("signing", codigo, `signing.poisonCodes.${codigo}`);
  }
  return out;
}

const celdas = (fila) => fila.split("|").slice(1, -1).map((c) => c.trim());

/** README → { filas: [{ sdk, caps: Set }] } o { error } */
function capacidadesDelReadme(readme) {
  const seccion = readme.split(/^## Estado de los SDKs\s*$/m)[1]?.split(/^## /m)[0];
  if (!seccion) return { error: "el README no tiene la sección «Estado de los SDKs»" };
  const filas = seccion.split("\n").filter((l) => l.trim().startsWith("|"));
  if (filas.length < 3) return { error: "la tabla «Estado de los SDKs» está vacía" };
  const iCap = celdas(filas[0]).findIndex((c) => /^capacidades$/i.test(c));
  if (iCap < 0) return { error: "la tabla «Estado de los SDKs» no tiene la columna «Capacidades»" };
  const out = [];
  for (const fila of filas.slice(2)) {
    const c = celdas(fila);
    const sdk = /\]\(sdk-([a-z]+)\/?\)/.exec(c[0] ?? "")?.[1];
    if (!sdk) continue;
    out.push({ sdk, caps: new Set([...(c[iCap] ?? "").matchAll(/`([a-z]+)`/g)].map((m) => m[1])) });
  }
  return { filas: out };
}

/**
 * @param {object} d
 * @param {object} d.protocol   protocol.json ya parseado
 * @param {string} d.readme     README.md
 * @param {Record<string,string>} d.fuentes  sdk → su código de producción, concatenado
 * @param {Map<string,string|null>} d.ficheros  ruta de evidencia → contenido (null si no existe)
 * @param {object} d.harnesses  conformance/harnesses.json ya parseado
 * @returns {string[]} fallos (vacío = todo bien)
 */
export function comprobar({ protocol, readme, fuentes, ficheros, harnesses }) {
  const fallos = [];
  const caps = protocol.capabilities;
  if (!caps?.definitions || !caps?.sdks) return ["protocol.json no tiene `capabilities.definitions` y `capabilities.sdks`"];
  const codigos = codigosPorCapacidad(protocol);

  for (const cap of codigos.keys()) {
    if (!Object.hasOwn(caps.definitions, cap)) fallos.push(`hay códigos de la capacidad "${cap}", que no está en capabilities.definitions`);
  }

  // (3) Los siete SDKs, declarados.
  for (const sdk of Object.keys(SDKS)) {
    if (!Object.hasOwn(caps.sdks, sdk)) fallos.push(`capabilities.sdks no declara el SDK "${sdk}": ni siquiera con una lista vacía`);
  }
  for (const sdk of Object.keys(caps.sdks)) {
    if (!Object.hasOwn(SDKS, sdk)) fallos.push(`capabilities.sdks declara "${sdk}", que este script no sabe dónde buscar (SDKS en scripts/check-sdk-codes.mjs)`);
  }

  // (1) y (2)
  for (const [sdk, decl] of Object.entries(caps.sdks)) {
    if (!Object.hasOwn(SDKS, sdk)) continue;
    for (const cap of decl.capabilities ?? []) {
      const def = caps.definitions[cap];
      if (!def) {
        fallos.push(`${sdk}: declara "${cap}", que no está en capabilities.definitions`);
        continue;
      }
      for (const { codigo, origen } of codigos.get(cap) ?? []) {
        if (!conLiteral(fuentes[sdk], codigo)) {
          fallos.push(`${sdk}: declara "${cap}" pero su código no contiene el literal "${codigo}" (${origen})`);
        }
      }
      if (def.evidence === "cross-sdk-harness") {
        const registrado = harnesses.harnesses?.some((h) => h.sdk === sdk);
        const pendiente = (harnesses.pending ?? []).includes(sdk);
        if (!registrado) fallos.push(`${sdk}: declara "${cap}" pero no está en conformance/harnesses.json, y esa es su evidencia`);
        if (pendiente) fallos.push(`${sdk}: declara "${cap}" pero figura en \`pending\` de conformance/harnesses.json: sin arnés no hay evidencia`);
      } else if (def.evidence === "sdk-files") {
        const rutas = decl.evidence?.[cap] ?? [];
        if (!rutas.length) fallos.push(`${sdk}: declara "${cap}" sin citar su evidencia (capabilities.sdks.${sdk}.evidence.${cap})`);
        const textos = [];
        for (const r of rutas) {
          const t = ficheros.get(r);
          if (t == null) fallos.push(`${sdk}: la evidencia de "${cap}" cita ${r}, que no existe`);
          else textos.push(t);
        }
        if (textos.length === rutas.length) {
          const junto = textos.join("\n");
          for (const { codigo } of codigos.get(cap) ?? []) {
            if (!conLiteral(junto, codigo)) fallos.push(`${sdk}: ningún fichero de evidencia de "${cap}" nombra "${codigo}": un código que ningún test comprueba es una hipótesis`);
          }
        }
      } else {
        fallos.push(`capabilities.definitions.${cap}.evidence debe ser "cross-sdk-harness" o "sdk-files", es ${JSON.stringify(def.evidence)}`);
      }
    }
  }

  // (4) README ↔ protocol.json
  const r = capacidadesDelReadme(readme);
  if (r.error) {
    fallos.push(r.error);
  } else {
    const vistos = new Set();
    for (const { sdk, caps: enReadme } of r.filas) {
      vistos.add(sdk);
      const declaradas = caps.sdks[sdk]?.capabilities;
      if (!declaradas) {
        fallos.push(`README: la tabla lista "${sdk}", que protocol.json no declara`);
        continue;
      }
      const a = [...enReadme].sort().join(", ");
      const b = [...declaradas].sort().join(", ");
      if (a !== b) fallos.push(`README: la fila de ${sdk} dice capacidades [${a}] y protocol.json dice [${b}]`);
    }
    for (const sdk of Object.keys(caps.sdks)) {
      if (!vistos.has(sdk)) fallos.push(`README: la tabla «Estado de los SDKs» no tiene fila para "${sdk}"`);
    }
  }
  return fallos;
}

// ─── Autocomprobación: cada mutación DEBE hacer fallar la comprobación ───────

function mutaciones(real) {
  const { protocol } = real;
  const filaDe = (readme, sdk) => readme.split("\n").find((l) => l.includes(`](sdk-${sdk}/)`));
  const out = [];
  const caso = (nombre, aplica, mencionaTodos) => out.push({ nombre, aplica, mencionaTodos });

  // Quitar de un SDK, uno a uno, cada código de cada capacidad que declara.
  for (const [sdk, decl] of Object.entries(protocol.capabilities.sdks)) {
    for (const cap of decl.capabilities) {
      for (const { codigo } of codigosPorCapacidad(protocol).get(cap) ?? []) {
        caso(`quitar "${codigo}" del código de ${sdk}`, (m) => { m.fuentes[sdk] = sinLiteral(m.fuentes[sdk], codigo); }, [sdk, codigo]);
      }
    }
  }

  // Declarar lo que no se tiene. Se aporta la evidencia de Node y se corrige el README para
  // que NADA salvo el código del propio SDK pueda delatarlo.
  const noDeclaradas = (sdk) => Object.keys(protocol.capabilities.definitions).filter((c) => !protocol.capabilities.sdks[sdk].capabilities.includes(c));
  for (const sdk of Object.keys(protocol.capabilities.sdks)) {
    for (const cap of noDeclaradas(sdk)) {
      caso(`${sdk} declara «${cap}» sin tener su código`, (m) => {
        const decl = m.protocol.capabilities.sdks[sdk];
        decl.capabilities.push(cap);
        decl.evidence = { ...(decl.evidence ?? {}), ...(protocol.capabilities.sdks.node.evidence?.[cap] && { [cap]: protocol.capabilities.sdks.node.evidence[cap] }) };
        m.readme = m.readme.replace(filaDe(m.readme, sdk), (f) => f.replace(/\s*\|\s*$/, ` \`${cap}\` |`));
        // «Sin tener su código»: un SDK que YA lleva los códigos de una capacidad que todavía no declara (en una entrega por pasos, el
        // sobre y los códigos puros llegan antes que la declaración, que espera a la prueba contra un broker real) no sería víctima de
        // esta mutación. Se le quitan, para simular lo que dice el nombre: declarar lo que no se tiene.
        for (const { codigo } of codigosPorCapacidad(protocol).get(cap) ?? []) m.fuentes[sdk] = sinLiteral(m.fuentes[sdk], codigo);
      }, [sdk]);
    }
  }

  // Un código nuevo en protocol.json que ningún SDK de la capacidad tiene.
  caso("añadir un código a `security` que ningún SDK contiene", (m) => {
    m.protocol.errors.codes.CODIGO_QUE_NADIE_TIENE = { class: "permanent", capability: "security" };
  }, ["CODIGO_QUE_NADIE_TIENE"]);
  caso("un código con una capacidad que no existe", (m) => {
    m.protocol.errors.codes.CODIGO_HUERFANO = { class: "permanent", capability: "inexistente" };
  }, ["inexistente"]);

  // Un SDK que se olvida de declararse.
  caso("un SDK sin declarar en capabilities.sdks", (m) => { delete m.protocol.capabilities.sdks.php; }, ["php"]);

  // README que no dice lo que protocol.json.
  caso("el README quita `security` a Node", (m) => {
    m.readme = m.readme.replace(filaDe(m.readme, "node"), (f) => f.replace("`security`", ""));
  }, ["node"]);
  caso("el README añade `security` a Python", (m) => {
    m.readme = m.readme.replace(filaDe(m.readme, "python"), (f) => f.replace(/\s*\|\s*$/, " `security` |"));
  }, ["python"]);
  caso("el README sin la columna «Capacidades»", (m) => { m.readme = m.readme.replaceAll("Capacidades", "Otra cosa"); }, ["Capacidades"]);

  // Evidencia que falta o que no nombra el código.
  const evNode = protocol.capabilities.sdks.node.evidence.security;
  caso("la evidencia de `security` de Node no existe", (m) => { for (const r of evNode) m.ficheros.set(r, null); }, ["node", "no existe"]);
  caso("la evidencia de `security` de Rust no nombra NOT_PROVISIONED", (m) => {
    for (const r of protocol.capabilities.sdks.rust.evidence.security) m.ficheros.set(r, sinLiteral(m.ficheros.get(r), "NOT_PROVISIONED"));
  }, ["rust", "NOT_PROVISIONED"]);
  caso("`security` de Node sin citar evidencia", (m) => { delete m.protocol.capabilities.sdks.node.evidence; }, ["node", "evidencia"]);
  // Lo mismo para `rpc`, la última capacidad declarada: su evidencia son DOS ficheros por SDK (la suite unitaria o el
  // código, y la de integración contra un broker real), y quitar cualquiera de las dos pistas debe notarse.
  for (const sdk of ["node", "rust"]) {
    const ev = protocol.capabilities.sdks[sdk].evidence.rpc;
    caso(`la evidencia de \`rpc\` de ${sdk} no existe`, (m) => { for (const r of ev) m.ficheros.set(r, null); }, [sdk, "no existe"]);
    caso(`la evidencia de \`rpc\` de ${sdk} no nombra RPC_TIMEOUT`, (m) => {
      for (const r of ev) m.ficheros.set(r, sinLiteral(m.ficheros.get(r), "RPC_TIMEOUT"));
    }, [sdk, "RPC_TIMEOUT"]);
    caso(`\`rpc\` de ${sdk} sin citar evidencia`, (m) => { delete m.protocol.capabilities.sdks[sdk].evidence.rpc; }, [sdk, "evidencia"]);
  }
  caso("PHP sin arnés (`pending`), y declara `signing`", (m) => { m.harnesses.pending = ["php"]; }, ["php", "pending"]);
  caso("PHP fuera de conformance/harnesses.json, y declara `signing`", (m) => { m.harnesses.harnesses = m.harnesses.harnesses.filter((h) => h.sdk !== "php"); }, ["php"]);
  return out;
}

function autocomprobacion(real) {
  const problemas = [];
  const base = comprobar(real);
  if (base.length) return { n: 0, problemas: ["la comprobación sobre los datos reales ya falla; no se puede autocomprobar"] };
  const muts = mutaciones(real);
  for (const { nombre, aplica, mencionaTodos } of muts) {
    const m = {
      protocol: structuredClone(real.protocol),
      readme: real.readme,
      fuentes: { ...real.fuentes },
      ficheros: new Map(real.ficheros),
      harnesses: structuredClone(real.harnesses),
    };
    aplica(m);
    const fallos = comprobar(m);
    if (!fallos.length) {
      problemas.push(`mutación NO detectada: ${nombre}`);
    } else if (!fallos.some((f) => mencionaTodos.every((t) => f.includes(t)))) {
      problemas.push(`mutación detectada por la razón equivocada: ${nombre}\n      esperaba un fallo que mencione ${mencionaTodos.map((t) => JSON.stringify(t)).join(" y ")}; obtuvo:\n        ${fallos.join("\n        ")}`);
    }
  }
  return { n: muts.length, problemas };
}

// ─── Programa principal ──────────────────────────────────────────────────────

const protocol = JSON.parse(await readFile(join(ROOT, "protocol.json"), "utf8"));
const readme = await readFile(join(ROOT, "README.md"), "utf8");
const harnesses = JSON.parse(await readFile(join(ROOT, "conformance/harnesses.json"), "utf8"));

const fuentes = {};
for (const sdk of Object.keys(SDKS)) fuentes[sdk] = await fuenteDe(sdk);

const ficheros = new Map();
for (const decl of Object.values(protocol.capabilities?.sdks ?? {})) {
  for (const rutas of Object.values(decl.evidence ?? {})) {
    for (const r of rutas) if (!ficheros.has(r)) ficheros.set(r, await leeOpcional(r));
  }
}

const real = { protocol, readme, fuentes, ficheros, harnesses };
const fallos = comprobar(real);

if (fallos.length) {
  console.error(`\n✗ ${fallos.length} problema(s) en las capacidades de los SDKs\n`);
  for (const f of fallos) console.error(`  ✗ ${f}`);
  process.exit(1);
}

const auto = autocomprobacion(real);
if (auto.problemas.length) {
  console.error(`\n✗ la autocomprobación falla: ${auto.problemas.length} mutación(es) sin detectar bien\n`);
  for (const p of auto.problemas) console.error(`  ✗ ${p}`);
  process.exit(1);
}

const codigos = codigosPorCapacidad(protocol);
let declaraciones = 0;
let verificados = 0;
for (const [sdk, decl] of Object.entries(protocol.capabilities.sdks)) {
  console.log(`  ${sdk.padEnd(7)} ${decl.capabilities.length ? decl.capabilities.join(", ") : "—"}`);
  declaraciones += decl.capabilities.length;
  for (const cap of decl.capabilities) verificados += (codigos.get(cap) ?? []).length;
}
console.log(
  `✓ capacidades de los SDKs coherentes: ${Object.keys(SDKS).length} SDKs, ${declaraciones} declaraciones, ` +
    `${verificados} códigos comprobados en su código · autocomprobación: ${auto.n} mutaciones, todas detectadas`,
);
