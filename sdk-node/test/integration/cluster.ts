/**
 * Un cluster de TRES nodos locales de nats-server, para lo que un nodo suelto no demuestra.
 *
 * Existe por una sola observación (A-49): en un stream R3, el conflicto de secuencia bajo carrera
 * da el err **10164** y SIN número, y el 10071 solo sale sin carrera. Un SDK que reconozca solo el
 * 10071 pasa todas las pruebas contra un nodo y, en producción, no reintenta. Con un nodo suelto
 * no hay forma de verlo.
 *
 * Mismo contrato que `broker.ts`: sin binario se SALTA con aviso, y con FLUX_REQUIRE_BROKER=1
 * FALLA. Los puertos salen de FLUX_TEST_PORTS (seis: tres de clientes y tres de rutas).
 */

import { spawn, type ChildProcess } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { localizarNatsServer, rangoDePuertos } from "./broker.js";

export interface Cluster {
  /** Las tres direcciones de cliente. */
  readonly servers: string[];
  stop(): Promise<void>;
}

const libre = (port: number) =>
  new Promise<boolean>((res) => {
    const s = createServer();
    s.once("error", () => res(false));
    s.listen(port, "127.0.0.1", () => s.close(() => res(true)));
  });

/** Seis puertos distintos y libres del rango. */
async function seisPuertos(): Promise<number[]> {
  const [desde, hasta] = rangoDePuertos();
  const elegidos: number[] = [];
  for (let i = 0; i < 300 && elegidos.length < 6; i++) {
    const p = desde + Math.floor(Math.random() * (hasta - desde + 1));
    if (!elegidos.includes(p) && (await libre(p))) elegidos.push(p);
  }
  if (elegidos.length < 6) throw new Error(`no hay seis puertos libres en ${desde}-${hasta}`);
  return elegidos;
}

export async function startCluster3(): Promise<Cluster> {
  const { ruta, motivo } = localizarNatsServer();
  if (!ruta) throw new Error(motivo);
  const dir = mkdtempSync(join(tmpdir(), "flux-cluster-"));
  const hijos: ChildProcess[] = [];
  const logs: string[][] = [[], [], []];

  const matar = () => hijos.forEach((h) => h.kill("SIGKILL"));
  const parar = async () => {
    process.removeListener("exit", matar);
    await Promise.all(
      hijos.map(
        (h) =>
          new Promise<void>((r) => {
            if (h.exitCode !== null) return r();
            h.once("exit", () => r());
            h.kill("SIGTERM");
            setTimeout(() => h.kill("SIGKILL"), 3000).unref();
          }),
      ),
    );
    rmSync(dir, { recursive: true, force: true });
  };
  // Red de seguridad: solo los PID lanzados aquí, nunca por nombre — hay otros nats-server en la máquina.
  process.once("exit", matar);

  try {
    const p = await seisPuertos();
    const nodos = [0, 1, 2].map((i) => ({ client: p[i], cluster: p[3 + i] }));
    nodos.forEach((n, i) => {
      const rutas = nodos.filter((_, j) => j !== i).map((m) => `nats://127.0.0.1:${m.cluster}`).join(", ");
      const d = join(dir, `n${i}`);
      mkdirSync(join(d, "js"), { recursive: true });
      const conf = join(d, "n.conf");
      writeFileSync(
        conf,
        `server_name: n${i}\nlisten: 127.0.0.1:${n.client}\njetstream { store_dir: "${join(d, "js")}" }\n` +
          `cluster { name: fluxtest\n listen: 127.0.0.1:${n.cluster}\n routes: [${rutas}]\n}\n`,
      );
      const h = spawn(ruta, ["-c", conf], { stdio: ["ignore", "ignore", "pipe"] });
      h.stderr!.setEncoding("utf8");
      h.stderr!.on("data", (x: string) => logs[i].push(...x.split("\n").filter(Boolean)));
      hijos.push(h);
    });
    // Los tres escuchan
    const limite = Date.now() + 15_000;
    for (;;) {
      const listos = nodos.every((n, i) => logs[i].some((l) => new RegExp(`Listening for client connections on .*:${n.client}\\b`).test(l)) && logs[i].some((l) => /Server is ready/.test(l)));
      if (listos) break;
      const roto = logs.findIndex((l) => l.some((x) => /address already in use|Error listening/i.test(x)));
      if (roto >= 0) throw new Error(`el nodo ${roto} del cluster no pudo escuchar:\n${logs[roto].join("\n")}`);
      if (Date.now() > limite) throw new Error(`el cluster no arrancó en 15 s:\n${logs.map((l) => l.slice(-5).join("\n")).join("\n---\n")}`);
      await new Promise((r) => setTimeout(r, 50));
    }
    return { servers: nodos.map((n) => `127.0.0.1:${n.client}`), stop: parar };
  } catch (e) {
    await parar();
    throw e;
  }
}
