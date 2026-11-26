#!/usr/bin/env node
/**
 * DIAGNÓSTICO (no forma parte de la malla): un `Authentication Timeout` del broker se trata como credenciales rechazadas.
 *
 *   node bench/diagnostico/auth-timeout.mjs
 *
 * Lo vio el banco una vez, midiendo el escenario B con N = 1000 y la máquina con 19 de carga en 4 núcleos: un proceso de consumidores
 * falló al abrir su conexión nº 251 con «ConnectionRejectedError: el broker rechazó las credenciales (Authentication Timeout). Reintentar
 * no lo arregla». Pero un `Authentication Timeout` no dice que la credencial esté mal: dice que el broker no recibió el `CONNECT` en
 * `auth_timeout` (2 s por defecto), y eso lo causa una máquina sin CPU (o una red lenta), que es **transitorio**. `permanentConnectError`
 * de `sdk-node/src/security.ts` clasifica todo `AuthorizationError` de la librería como PERMANENT, y un servicio que arranca con el broker
 * saturado no reintenta: se cae.
 *
 * Aquí se reproduce sin carga: un proxy retrasa 400 ms lo que el cliente manda (su `CONNECT`) y el broker tiene `auth_timeout: 0.1`.
 * Con el `auth_timeout` por defecto (2 s) el mismo retraso conecta. Ver la fila A-174 del Apéndice A.
 */

import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";

import { natsServerBin, puertoLibre } from "../lib/entorno.mjs";
import { conectarServicio } from "../lib/flux.mjs";
import { nuevaClave, dec } from "../../scripts/lib/nats-harness.mjs";

const k = nuevaClave();
const dir = mkdtempSync(path.join(os.tmpdir(), "flux-authto-"));
const puertoNats = await puertoLibre();
const puertoProxy = await puertoLibre();

let retraso = 400;
// Un proxy que retrasa lo que el cliente manda al servidor (el CONNECT), como lo haría una máquina sin CPU.
const proxy = net
  .createServer((c) => {
    const s = net.connect(puertoNats, "127.0.0.1");
    c.on("data", (d) => setTimeout(() => s.writable && s.write(d), retraso));
    s.on("data", (d) => c.writable && c.write(d));
    for (const [a, b] of [[c, s], [s, c]]) {
      a.on("error", () => b.destroy());
      a.on("close", () => b.destroy());
    }
  })
  .listen(puertoProxy, "127.0.0.1");

let mal = 0;
try {
  for (const [nombre, authTimeout, deberiaConectar] of [
    ["auth_timeout 0.1 s, CONNECT retrasado 400 ms", 0.1, false],
    ["auth_timeout 2 s (el de por defecto), CONNECT retrasado 400 ms", 2, true],
  ]) {
    writeFileSync(
      path.join(dir, "n.conf"),
      `host: "127.0.0.1"\nport: ${puertoNats}\njetstream { store_dir: "${path.join(dir, "js")}" }\nauthorization {\n  timeout: ${authTimeout}\n  users: [ { nkey: "${k.pub}" } ]\n}\n`,
    );
    const nats = spawn(natsServerBin(), ["-c", path.join(dir, "n.conf")], { stdio: "ignore" });
    await new Promise((r) => setTimeout(r, 800));
    const t0 = Date.now();
    let resultado;
    try {
      const espera = new Promise((_, rj) => setTimeout(() => rj(new Error("sigue esperando a los 5 s (reintenta)")), 5000));
      const bus = await Promise.race([conectarServicio({ url: `nats://127.0.0.1:${puertoProxy}`, servicio: "prueba-api", semilla: dec.decode(k.seed) }), espera]);
      await bus.close();
      resultado = { conecta: true, texto: "conectó" };
    } catch (e) {
      resultado = { conecta: false, texto: `${e.name}: ${String(e.message).slice(0, 170)}`, permanente: e.name === "ConnectionRejectedError" };
    }
    console.log(`${nombre}\n   → ${resultado.texto} (${Date.now() - t0} ms)`);
    if (resultado.conecta !== deberiaConectar) mal++;
    // El hallazgo: el primer caso falla como PERMANENT («credenciales rechazadas») aunque sea transitorio.
    if (!deberiaConectar && resultado.permanente) console.log("   ⚠ un timeout de autenticación (transitorio) sale como ConnectionRejectedError (PERMANENT, «revisa credentials»)");
    nats.kill("SIGKILL");
    await new Promise((r) => setTimeout(r, 300));
  }
} finally {
  proxy.close();
  rmSync(dir, { recursive: true, force: true });
}
process.exit(mal ? 1 : 0);
