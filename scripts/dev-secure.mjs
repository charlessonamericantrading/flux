#!/usr/bin/env node
/**
 * Un NATS de desarrollo con la configuración SEGURA: TLS, una nkey por servicio y las ACLs que
 * genera `scripts/gen-acl.mjs`. Lo mismo que la suite de seguridad levanta efímeramente, pero
 * persistente, para desarrollar contra lo que se va a desplegar y no contra un `nats-server`
 * abierto donde todo funciona porque nada está confinado.
 *
 *   (cd conformance && npm install)
 *   node scripts/dev-secure.mjs [services.json] [--bundle <bundle.json>] [--out .dev-secure] [--port 4223] [--dev] [--force]
 *
 * `services.json` es TU manifiesto y `--bundle` el índice de contratos de TUS esquemas (por defecto, los de este repo:
 * `examples/services.json` y `schemas/bundle.json`). Hasta la v1.1 el script ignoraba el manifiesto que se le daba al
 * calcular las claves, y solo servía para el ecosistema de este repo.
 *
 * Escribe en `.dev-secure/` (ignorado por git: lleva claves privadas):
 *
 *   ca.pem · servidor.pem · servidor.key    TLS: una CA de desarrollo (un año) y su certificado
 *   nkeys/<servicio>.nk · nkeys/ops.nk      las SEMILLAS (secretos). La del servicio es lo que
 *                                           su SDK recibe en `credentials.nkeySeed`
 *   ops.creds                               la de operación, en el formato que lee `--creds`
 *   nkeys.json                              las claves PÚBLICAS (lo que pide `gen-acl --nkeys`)
 *   acl.conf                                la ACL generada, con el usuario de operación añadido
 *   nats.conf                               el servidor para ejecutarlo directamente
 *   nats-compose.conf                       el mismo, con las rutas del contenedor (perfil `secure`)
 *
 * Es IDEMPOTENTE: volver a ejecutarlo tras tocar el manifiesto regenera la ACL y las
 * configuraciones, y NO rota lo que ya existe (la CA y las semillas de los servicios que ya
 * tenían). Rotarlo todo es `--force`: borra el directorio entero y empieza de cero.
 *
 * `--dev` concede además CONSUMER.CREATE de cada durable propio (ver 06-security.md §3.4): con él
 * un servicio puede crear su consumidor sin `flux provision`. Los streams NUNCA los crea un
 * servicio: ese permiso no se concede ni con `--dev`, así que hay que aprovisionar siempre.
 */

import { spawnSync } from "node:child_process";
import { chmod, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";

import { crearPki, opensslDisponible } from "./lib/pki.mjs";
import {
  EJEMPLO,
  GEN,
  ROOT,
  conUsuarios,
  credsDe,
  dec,
  enc,
  gen,
  nkeys,
  nuevaClave,
  usuarioOpsConf,
} from "./lib/nats-harness.mjs";

const USO = "uso: node scripts/dev-secure.mjs [services.json] [--bundle <bundle.json>] [--out <dir>] [--port <n>] [--dev] [--force]";

function leerArgumentos(argv) {
  const o = { manifiesto: EJEMPLO, bundle: undefined, out: path.join(ROOT, ".dev-secure"), port: 4223, dev: false, force: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--out" || a === "--port" || a === "--bundle") {
      const v = argv[++i];
      if (!v || v.startsWith("--")) fallar(`${a} necesita un valor`);
      if (a === "--out") o.out = path.resolve(v);
      else if (a === "--bundle") o.bundle = path.resolve(v);
      else o.port = Number(v);
    } else if (a === "--dev") o.dev = true;
    else if (a === "--force") o.force = true;
    else if (a === "-h" || a === "--help") {
      console.log(USO);
      process.exit(0);
    } else if (a.startsWith("--")) fallar(`opción desconocida: ${a}`);
    else o.manifiesto = path.resolve(a);
  }
  if (!Number.isInteger(o.port) || o.port < 1 || o.port > 65535) fallar(`--port inválido: ${o.port}`);
  return o;
}

function fallar(m, codigo = 2) {
  console.error(`✗ ${m}${codigo === 2 ? `\n${USO}` : ""}`);
  process.exit(codigo);
}

const args = leerArgumentos(process.argv.slice(2));
if (!opensslDisponible().ok) fallar("no hay `openssl` en el PATH (la CA de desarrollo lo necesita)", 1);

// ─── Directorio ──────────────────────────────────────────────────────────────

if (args.force) {
  // Solo se borra lo que ESTE script escribe, nunca un directorio ajeno: `--out .` con --force no
  // debe llevarse por delante un proyecto. Se exige que parezca un directorio de dev-secure.
  const nombre = path.basename(args.out);
  const entradas = await readdir(args.out).catch(() => []);
  if (entradas.length && !entradas.includes("nats.conf") && !entradas.includes("ca.pem")) {
    fallar(`--force borra ${args.out}, que no parece un directorio de dev-secure (sin nats.conf ni ca.pem). No lo toco.`, 1);
  }
  await rm(args.out, { recursive: true, force: true });
  console.log(`  · --force: ${nombre}/ borrado; se regenera todo`);
}
await mkdir(path.join(args.out, "nkeys"), { recursive: true });

const existe = (f) => readFile(f).then(() => true, () => false);

// ─── TLS: solo si falta ──────────────────────────────────────────────────────

let tlsNuevo = false;
if (!(await existe(path.join(args.out, "ca.pem"))) || !(await existe(path.join(args.out, "servidor.pem")))) {
  // `claveLegible`: el proceso del contenedor no es el dueño del fichero (0600 no lo leería).
  // Es una clave de una CA de DESARROLLO en un directorio ignorado por git.
  await crearPki(args.out, { dias: 365, claveLegible: true, otraCa: false });
  tlsNuevo = true;
}

// ─── Claves: una por servicio y la de operación, solo las que faltan ─────────

// Los servicios son los de TU manifiesto (y su bundle): con `permisosJson()` salían siempre los de examples/services.json.
const opcionesGen = [...(args.bundle ? ["--bundle", args.bundle] : []), ...(args.dev ? ["--dev"] : [])];
const lista = spawnSync(process.execPath, [GEN, args.manifiesto, "--json", ...opcionesGen], { encoding: "utf8", cwd: ROOT });
if (lista.status !== 0) fallar(`gen-acl.mjs falló:\n${lista.stderr || lista.stdout}`, 1);
const permisos = JSON.parse(lista.stdout);
const publicas = {};
const semillas = new Map();
const nuevas = [];

async function claveDe(nombre) {
  const f = path.join(args.out, "nkeys", `${nombre}.nk`);
  let semilla;
  try {
    semilla = (await readFile(f, "utf8")).trim();
  } catch {
    const k = nuevaClave();
    semilla = dec.decode(k.seed);
    await writeFile(f, `${semilla}\n`, { mode: 0o600 });
    nuevas.push(nombre);
  }
  await chmod(f, 0o600);
  semillas.set(nombre, semilla);
  return nkeys.fromSeed(enc.encode(semilla)).getPublicKey();
}

for (const s of permisos) publicas[s.name] = await claveDe(s.name);
const opsPub = await claveDe("ops");

await writeFile(path.join(args.out, "nkeys.json"), `${JSON.stringify(publicas, null, 2)}\n`);
await writeFile(path.join(args.out, "ops.creds"), credsDe(semillas.get("ops")), { mode: 0o600 });

// ─── ACL generada + usuario de operación ─────────────────────────────────────

const g = gen(args.manifiesto, "--nkeys", path.join(args.out, "nkeys.json"), ...opcionesGen);
if (g.codigo !== 0) fallar(`gen-acl.mjs falló:\n${g.errores || g.salida}`, 1);
await writeFile(
  path.join(args.out, "acl.conf"),
  conUsuarios(g.salida, `      # Operación: aprovisiona con \`flux provision\`. Sin restricciones.\n${usuarioOpsConf(opsPub)}`),
);

// ─── Configuración del servidor ──────────────────────────────────────────────

/**
 * `dir`: dónde están los ficheros VISTOS POR EL SERVIDOR (la ruta del contenedor o la de este
 * directorio); `store`: el almacén de JetStream. Las rutas de TLS van absolutas: NATS las
 * resuelve contra el directorio de trabajo del proceso, no contra la config (medido).
 */
function configServidor({ dir, store, host, port, http }) {
  return [
    `# GENERADO por scripts/dev-secure.mjs — configuración de DESARROLLO. No es producción.`,
    `#`,
    `# TLS obligatorio (sin allow_non_tls), una nkey por servicio y las ACLs de gen-acl.mjs.`,
    `# Los streams y los consumidores NO los crea ningún servicio: \`flux provision\`.`,
    ``,
    `server_name: "flux-dev-secure"`,
    `host: "${host}"`,
    `port: ${port}`,
    ...(http ? [`http: "${http}"`] : []),
    `jetstream { store_dir: "${store}" }`,
    ``,
    `tls {`,
    `  cert_file: "${dir}/servidor.pem"`,
    `  key_file: "${dir}/servidor.key"`,
    `  timeout: 2`,
    `}`,
    ``,
    `include "acl.conf"`,
    ``,
  ].join("\n");
}

await writeFile(
  path.join(args.out, "nats.conf"),
  configServidor({ dir: args.out, store: path.join(args.out, "js"), host: "127.0.0.1", port: args.port }),
);
await writeFile(
  path.join(args.out, "nats-compose.conf"),
  // Dentro del contenedor el directorio se monta en /etc/flux (docker-compose.yml, perfil `secure`).
  configServidor({ dir: "/etc/flux", store: "/data", host: "0.0.0.0", port: 4222, http: "0.0.0.0:8222" }),
);

// ─── Informe ─────────────────────────────────────────────────────────────────

/** Relativa a la raíz del repo si el directorio está dentro (lo normal); absoluta si no. */
const rel = (f) => {
  const r = path.relative(ROOT, path.join(args.out, f));
  return r.startsWith("..") ? path.join(args.out, f) : r || ".";
};
console.log(`dev-secure — NATS de desarrollo con TLS, nkeys y ACLs generadas`);
console.log(`  ${tlsNuevo ? "✓ CA y certificado de servidor creados (365 días)" : "· CA y certificado ya existían: no se tocan"}`);
console.log(`  ${nuevas.length ? `✓ semillas nuevas: ${nuevas.join(", ")}` : "· las semillas ya existían: no se rotan"}`);
console.log(`  ✓ ${rel("acl.conf")}: ${permisos.length} servicio(s) + operación${args.dev ? " (con --dev: CONSUMER.CREATE del durable propio)" : ""}`);
console.log(`
Arrancarlo, con el binario:

  nats-server -c ${rel("nats.conf")}

…o con Docker (perfil \`secure\` de docker-compose.yml; monta ${rel("")} en /etc/flux):

  docker compose --profile secure up -d nats-secure

Aprovisionar los streams y los durables (con las credenciales de OPERACIÓN y la CA de desarrollo):

  NODE_EXTRA_CA_CERTS=${rel("ca.pem")} node cli/src/index.mjs provision ${path.relative(ROOT, args.manifiesto)}${args.bundle ? ` --bundle ${path.relative(ROOT, args.bundle)}` : ""} \\
      --server tls://localhost:${args.port} --creds ${rel("ops.creds")}

Conectar un servicio con su SDK (Node): su semilla, la CA y el inbox privado, que el SDK fija solo:

  connect({ servers: "tls://localhost:${args.port}", service: "pedidos-api", …,
            credentials: { nkeySeed: readFileSync("${rel("nkeys/pedidos-api.nk")}", "utf8").trim() },
            tls: { caFile: "${rel("ca.pem")}" } })

Las semillas son SECRETOS: ${rel("")} está en .gitignore y no se versiona.`);
