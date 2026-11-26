/**
 * El ecosistema de las pruebas: un nats-server REAL con los permisos que GENERA `scripts/gen-acl.mjs`
 * (a partir de `gateway/examples/services.json`, con clientes de borde `"via": "gateway"`) y los
 * streams y durables que crea `flux provision`.
 *
 * Es la misma receta que `conformance/security.mjs` (S1…): una nkey por servicio, la ACL generada, un usuario de
 * operación sin restricciones que aprovisiona, y NINGÚN permiso a mano. Si el gateway necesitara uno especial,
 * estas pruebas no pasarían: lo que se prueba es que `"via": "gateway"` no concede nada que no concedan `owns`,
 * `consumes`, `watches` y `calls`.
 *
 * Entorno (el de los demás runners del repo):
 *   NATS_SERVER            binario de nats-server (si no, el del PATH)
 *   FLUX_REQUIRE_BROKER=1  su ausencia es un FALLO, no un salto con aviso
 *   FLUX_TEST_PORTS        rango de puertos, `desde-hasta` (por defecto 6900-6999)
 */

import { spawnSync } from "node:child_process";
import { writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  Banco,
  CLI,
  ROOT,
  conUsuarios,
  conectar,
  credsDe,
  dec,
  gen,
  jetstream,
  jetstreamManager,
  nuevaClave,
  usuarioOpsConf,
} from "../../../scripts/lib/nats-harness.mjs";

const AQUI = path.dirname(fileURLToPath(import.meta.url));
/** El ejemplo del gateway (`gateway/examples/`) ES el ecosistema de las pruebas: lo que se documenta es lo que se ejecuta. */
export const EJEMPLOS = path.join(AQUI, "..", "..", "examples");
export const MANIFIESTO = path.join(EJEMPLOS, "services.json");
export const BUNDLE = path.join(EJEMPLOS, "bundle.json");

/** `FLUX_REQUIRE_BROKER` definida y no vacía, `0` o `false`. */
export function brokerObligatorio() {
  const v = (process.env.FLUX_REQUIRE_BROKER ?? "").trim().toLowerCase();
  return v !== "" && v !== "0" && v !== "false";
}

/** El binario de nats-server, o el motivo por el que no está. */
export function natsServer() {
  const bin = process.env.NATS_SERVER || "nats-server";
  const r = spawnSync(bin, ["--version"], { encoding: "utf8" });
  return r.error || r.status !== 0
    ? { motivo: `no se puede ejecutar \`${bin} --version\` (define NATS_SERVER con la ruta del binario)` }
    : { bin, version: r.stdout.trim() };
}

function rangoDePuertos() {
  const m = /^(\d+)-(\d+)$/.exec(process.env.FLUX_TEST_PORTS ?? "6900-6999");
  if (!m) throw new Error("FLUX_TEST_PORTS debe ser `desde-hasta`, por ejemplo 6900-6999");
  return { puertoBase: Number(m[1]), puertos: Number(m[2]) - Number(m[1]) + 1 };
}

/**
 * Levanta el broker, aplica la ACL generada y aprovisiona. Devuelve:
 *   url, banco, claves[servicio] = { pub, seed }, credenciales(servicio) → `{ nkeySeed }` para `connect()`,
 *   ops = { nc, jsm, js } (el usuario de operación), conectar(servicio) → conexión NATS en bruto de ese servicio,
 *   parar().
 */
export async function levantarEcosistema({ manifiesto = MANIFIESTO, bundle = BUNDLE, aprovisionar = true } = {}) {
  const { bin, motivo } = natsServer();
  if (!bin) throw new Error(motivo);
  const { puertoBase, puertos } = rangoDePuertos();
  const banco = new Banco({ prefijo: "flux-gw-", puertoBase, puertos, natsServer: bin });
  await banco.iniciar();

  try {
    const m = JSON.parse(await (await import("node:fs/promises")).readFile(manifiesto, "utf8"));
    const claves = Object.fromEntries(m.services.map((s) => [s.name, nuevaClave()]));
    const ops = nuevaClave();

    const mapa = path.join(banco.tmp, "nkeys.json");
    await writeFile(mapa, JSON.stringify(Object.fromEntries(Object.entries(claves).map(([n, k]) => [n, k.pub]))));
    const g = gen(manifiesto, "--bundle", bundle, "--nkeys", mapa);
    if (g.codigo !== 0) throw new Error(`gen-acl.mjs falló:\n${g.errores || g.salida}`);

    const srv = await banco.arrancar("gw", conUsuarios(g.salida, usuarioOpsConf(ops.pub)));
    const url = srv.url;

    const nc = await conectar(url, { nombre: "ops", seed: ops.seed, etiqueta: "flux-gw" });
    const opsConn = { nc, jsm: await jetstreamManager(nc), js: jetstream(nc) };

    if (aprovisionar) {
      const credsOps = path.join(banco.tmp, "ops.creds");
      await writeFile(credsOps, credsDe(ops.seed), { mode: 0o600 });
      const r = spawnSync(process.execPath, [CLI, "provision", manifiesto, "--bundle", bundle, "--server", url, "--creds", credsOps], {
        encoding: "utf8",
        cwd: ROOT,
        env: { ...process.env, NO_COLOR: "1" },
      });
      if (r.status !== 0) throw new Error(`flux provision falló (${r.status}):\n${(r.stdout + r.stderr).trim()}`);
    }

    return {
      url,
      banco,
      servidor: srv,
      claves,
      ops: opsConn,
      credenciales: (servicio) => ({ nkeySeed: dec.decode(claves[servicio].seed) }),
      conectar: (servicio, extra = {}) => conectar(url, { nombre: servicio, seed: claves[servicio].seed, inboxPrefix: `_INBOX.${servicio}`, etiqueta: "flux-gw", ...extra }),
      async parar() {
        try {
          await opsConn.nc.close();
        } catch {
          /* ya cerrada */
        }
        await banco.limpiar();
      },
    };
  } catch (e) {
    await banco.limpiar();
    throw e;
  }
}
