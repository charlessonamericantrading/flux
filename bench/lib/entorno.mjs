/**
 * El ENTORNO del banco: la máquina, las versiones y un `nats-server` real con la configuración SEGURA.
 *
 * La configuración segura es la de `scripts/dev-secure.mjs` y la de la suite de seguridad, no una inventada
 * para el banco: una nkey por servicio, las ACLs que genera `scripts/gen-acl.mjs` a partir de un manifiesto
 * (con el inbox privado `_INBOX.<servicio>.>` y los permisos EXACTOS de cada durable) y los streams y durables
 * que crea `flux provision` con credenciales de operación. Los servicios no tienen permiso para crear nada.
 * TLS es opcional y se mide aparte (con y sin): ver README.md del banco.
 */

import { execFile, execFileSync, spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { crearPki, opensslDisponible } from "../../scripts/lib/pki.mjs";
import { GEN, CLI, ROOT, conUsuarios, credsDe, dec, nuevaClave, usuarioOpsConf } from "../../scripts/lib/nats-harness.mjs";
import { dormir } from "./proc.mjs";

const execFileP = promisify(execFile);
export const RAIZ = ROOT;
export const BENCH = path.dirname(path.dirname(fileURLToPath(import.meta.url)));

// ─── Puertos ─────────────────────────────────────────────────────────────────

/** `FLUX_TEST_PORTS=desde-hasta` (por defecto 4700-4799). */
export function rangoDePuertos() {
  const m = /^(\d+)-(\d+)$/.exec(process.env.FLUX_TEST_PORTS ?? "4700-4799");
  if (!m) throw new Error("FLUX_TEST_PORTS debe ser `desde-hasta`, por ejemplo 4700-4799");
  return [Number(m[1]), Number(m[2])];
}

const usados = new Set();
/** Un puerto libre del rango, comprobado con un `bind` real, y no repetido en este proceso. */
export async function puertoLibre() {
  const [d, h] = rangoDePuertos();
  for (let p = d; p <= h; p++) {
    if (usados.has(p)) continue;
    const libre = await new Promise((ok) => {
      const s = net.createServer();
      s.once("error", () => ok(false));
      s.listen(p, "127.0.0.1", () => s.close(() => ok(true)));
    });
    if (libre) {
      usados.add(p);
      return p;
    }
  }
  throw new Error(`no hay un puerto libre entre ${d} y ${h} (FLUX_TEST_PORTS)`);
}
export const liberarPuerto = (p) => usados.delete(p);

// ─── La máquina y las versiones ──────────────────────────────────────────────

/** La salida de un comando corto, o `undefined` si no existe o falla: una versión desconocida es un `?`, no un error. */
function sh(cmd, args) {
  try {
    return execFileSync(cmd, args, { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  } catch {
    return undefined;
  }
}

function versionDe(paquete, desde) {
  // `exports` de @nats-io/* no expone package.json: se lee del árbol de node_modules.
  const candidatos = [
    path.join(desde, "node_modules", ...paquete.split("/"), "package.json"),
    path.join(RAIZ, "conformance", "node_modules", ...paquete.split("/"), "package.json"),
    path.join(RAIZ, "cli", "node_modules", ...paquete.split("/"), "package.json"),
  ];
  for (const c of candidatos) {
    if (existsSync(c)) return JSON.parse(readFileSync(c, "utf8")).version;
  }
  return "?";
}

export function natsServerBin() {
  return process.env.NATS_SERVER ?? "nats-server";
}

/** Todo lo que `RESULTS.md` dice de DÓNDE y CON QUÉ se midió. */
export function describirEntorno() {
  const cpus = os.cpus();
  const lscpu = sh("lscpu", []) ?? "";
  const campo = (k) => new RegExp(`^${k}:\\s+(.+)$`, "m").exec(lscpu)?.[1]?.trim();
  const sdkDir = path.join(RAIZ, "sdk-node");
  const sdkPkg = JSON.parse(readFileSync(path.join(sdkDir, "package.json"), "utf8"));
  const osRelease = existsSync("/etc/os-release") ? readFileSync("/etc/os-release", "utf8") : "";
  const gov = (() => {
    try {
      return readFileSync("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor", "utf8").trim();
    } catch {
      return "no expuesto (máquina virtual)";
    }
  })();
  return {
    fecha: new Date().toISOString(),
    cpu: campo("Model name") ?? cpus[0]?.model ?? "?",
    nucleos: cpus.length,
    hilosPorNucleo: campo("Thread\\(s\\) per core"),
    virtualizacion: campo("Hypervisor vendor") ? `${campo("Hypervisor vendor")} (${campo("Virtualization type") ?? "?"})` : "ninguna detectada",
    ramGb: Math.round((os.totalmem() / 2 ** 30) * 10) / 10,
    so: `${/^PRETTY_NAME="?([^"\n]+)"?/m.exec(osRelease)?.[1] ?? os.type()} · kernel ${os.release()}`,
    gobernador: gov,
    node: process.version,
    v8: process.versions.v8,
    natsServer: sh(natsServerBin(), ["--version"]) ?? "?",
    nats: {
      "@nats-io/transport-node": versionDe("@nats-io/transport-node", sdkDir),
      "@nats-io/jetstream": versionDe("@nats-io/jetstream", sdkDir),
      "@nats-io/nkeys": versionDe("@nats-io/nkeys", sdkDir),
    },
    sdkFlux: `@flux/sdk ${sdkPkg.version}`,
    commit: sh("git", ["-C", RAIZ, "rev-parse", "--short", "HEAD"]) ?? "?",
    commitSucio: (sh("git", ["-C", RAIZ, "status", "--porcelain", "--", "sdk-node/src", "scripts", "bench", "cli/src"]) ?? "") !== "",
    openssl: sh("openssl", ["version"]) ?? "?",
  };
}

// ─── Un nats-server real con la configuración segura ─────────────────────────

/**
 * @param {object} o
 * @param {object} o.manifiesto  `{ environment, services: [...] }` (el formato de `examples/services.json`)
 * @param {boolean} [o.tls]      TLS en el listener (certificado de una CA propia, SAN localhost/127.0.0.1)
 * @param {string}  [o.nombre]   para el nombre del directorio temporal
 * @returns un broker ya aprovisionado: `{ url, monitor, pid, caFile, semilla(servicio), parar(), varz() }`
 */
export async function crearBroker({ manifiesto, tls = false, nombre = "bench", prof = false }) {
  if (tls && !opensslDisponible().ok) throw new Error("--tls necesita `openssl` en el PATH");
  const dir = await mkdtemp(path.join(os.tmpdir(), `flux-bench-${nombre}-`));
  const limpiar = () => rm(dir, { recursive: true, force: true });
  try {
    // ── claves: una nkey por servicio y la de operación ──
    const semillas = new Map();
    const publicas = {};
    for (const s of manifiesto.services) {
      const k = nuevaClave();
      publicas[s.name] = k.pub;
      semillas.set(s.name, dec.decode(k.seed));
    }
    const ops = nuevaClave();
    await writeFile(path.join(dir, "manifiesto.json"), JSON.stringify(manifiesto, null, 2));
    await writeFile(path.join(dir, "nkeys.json"), JSON.stringify(publicas));
    await writeFile(path.join(dir, "ops.creds"), credsDe(dec.decode(ops.seed)), { mode: 0o600 });

    // ── la ACL, con el GENERADOR del repo (no se escribe a mano) ──
    // `maxBuffer` alto: con 1 000 servicios la ACL pesa varios MiB y `spawnSync` corta en 1 MiB.
    const { stdout: acl } = await execFileP(
      process.execPath,
      [GEN, path.join(dir, "manifiesto.json"), "--nkeys", path.join(dir, "nkeys.json")],
      { cwd: RAIZ, maxBuffer: 1 << 30 },
    ).catch((e) => {
      throw new Error(`gen-acl.mjs falló: ${e.stderr || e.message}`);
    });
    await writeFile(
      path.join(dir, "acl.conf"),
      conUsuarios(acl, `      # Operación: aprovisiona con \`flux provision\`. Sin restricciones.\n${usuarioOpsConf(ops.pub)}`),
    );

    // ── TLS ──
    if (tls) await crearPki(dir, { dias: 2, claveLegible: false, otraCa: false });

    // ── configuración del servidor ──
    const port = await puertoLibre();
    const monitorPort = await puertoLibre();
    // Diagnóstico: `prof: true` abre el puerto de pprof de nats-server (`go tool pprof`) para ver DÓNDE gasta la CPU.
    const profPort = prof ? await puertoLibre() : undefined;
    const conf = [
      `# GENERADO por bench/lib/entorno.mjs — la misma forma que scripts/dev-secure.mjs.`,
      `server_name: "flux-bench"`,
      `host: "127.0.0.1"`,
      `port: ${port}`,
      `http: "127.0.0.1:${monitorPort}"`,
      ...(profPort ? [`prof_port: ${profPort}`] : []),
      `jetstream { store_dir: "${path.join(dir, "js")}" }`,
      ...(tls
        ? [`tls {`, `  cert_file: "${path.join(dir, "servidor.pem")}"`, `  key_file: "${path.join(dir, "servidor.key")}"`, `  timeout: 5`, `}`]
        : []),
      `include "acl.conf"`,
      ``,
    ].join("\n");
    await writeFile(path.join(dir, "nats.conf"), conf);

    const hijo = spawn(natsServerBin(), ["-c", path.join(dir, "nats.conf")], { stdio: ["ignore", "ignore", "pipe"] });
    const lineas = [];
    let salio = false;
    hijo.once("exit", () => (salio = true));
    // Un binario que no existe emite `error` (ENOENT), no `exit`: sin este manejador el proceso entero caería con un volcado.
    hijo.once("error", (e) => {
      salio = true;
      lineas.push(`no se pudo lanzar ${natsServerBin()}: ${e.message}`);
    });
    hijo.stderr.setEncoding("utf8");
    hijo.stderr.on("data", (d) => {
      lineas.push(...d.split("\n").filter(Boolean));
      if (lineas.length > 300) lineas.splice(0, lineas.length - 300);
    });
    const matar = () => hijo.kill("SIGKILL");
    process.once("exit", matar);

    const limite = Date.now() + 15_000;
    while (!lineas.some((l) => /Server is ready/.test(l))) {
      if (salio || Date.now() > limite) {
        matar();
        throw new Error(`nats-server no arrancó con la config del banco:\n${lineas.join("\n")}`);
      }
      await dormir(25);
    }
    if (!lineas.some((l) => new RegExp(`Listening for client connections on .*:${port}\\b`).test(l))) {
      matar();
      throw new Error(`nats-server no escucha en el puerto ${port} (¿lo ocupa otro proceso?):\n${lineas.join("\n")}`);
    }

    const url = `${tls ? "tls" : "nats"}://127.0.0.1:${port}`;
    const caFile = tls ? path.join(dir, "ca.pem") : undefined;
    const broker = {
      url,
      port,
      monitor: `http://127.0.0.1:${monitorPort}`,
      profPort,
      pid: hijo.pid,
      caFile,
      tls,
      dir,
      semilla: (servicio) => {
        const s = semillas.get(servicio);
        if (!s) throw new Error(`el servicio «${servicio}» no está en el manifiesto del broker`);
        return s;
      },
      log: () => lineas.join("\n"),
      async varz() {
        const r = await fetch(`http://127.0.0.1:${monitorPort}/varz`);
        return r.json();
      },
      async connz() {
        const r = await fetch(`http://127.0.0.1:${monitorPort}/connz?limit=1`);
        return r.json();
      },
      async jsz() {
        const r = await fetch(`http://127.0.0.1:${monitorPort}/jsz`);
        return r.json();
      },
      async parar() {
        process.removeListener("exit", matar);
        if (!salio) {
          const fin = new Promise((r) => hijo.once("exit", r));
          hijo.kill("SIGTERM");
          const dura = setTimeout(() => hijo.kill("SIGKILL"), 5000);
          await fin;
          clearTimeout(dura);
        }
        liberarPuerto(port);
        liberarPuerto(monitorPort);
        await limpiar();
      },
    };

    // ── aprovisionar con `flux provision` y credenciales de operación ──
    const t0 = Date.now();
    try {
      await execFileP(
        process.execPath,
        [CLI, "provision", path.join(dir, "manifiesto.json"), "--server", url, "--creds", path.join(dir, "ops.creds")],
        {
          cwd: RAIZ,
          maxBuffer: 1 << 28,
          env: { ...process.env, ...(caFile && { NODE_EXTRA_CA_CERTS: caFile }) },
          timeout: 180_000,
        },
      );
    } catch (e) {
      await broker.parar();
      throw new Error(`\`flux provision\` falló: ${(e.stdout ?? "").slice(-1500)}${(e.stderr ?? "").slice(-1500)} ${e.message}`);
    }
    broker.msProvision = Date.now() - t0;
    return broker;
  } catch (e) {
    await limpiar();
    throw e;
  }
}

/** Una cadena `service` válida (`^[a-z0-9]+(-[a-z0-9]+)*$`) y ordenada para el servicio nº `i`. */
export const nombreServicio = (prefijo, i) => `${prefijo}-${String(i).padStart(4, "0")}`;

export { readFile, mkdir };
