/**
 * Los AGENTES de los escenarios con SDKs reales: procesos hijos que hablan con el broker SOLO por la API pública de un SDK
 * de flux (`security/participante-node.mjs` con el de Node; `sdk-rust/examples/rpc_participant.rs` con el de Rust;
 * `sdk-go/cmd/rpc-participant` con el de Go) y que un
 * orquestador gobierna por stdin/stdout con líneas JSON.
 *
 * Por qué procesos y no hilos o funciones: lo que mide un escenario es lo que el SDK hace con SU conexión (qué
 * suscripciones abre, qué permisos usa, cuánto tarda en decir `no responders`), y dos instancias de un servicio son dos
 * conexiones independientes, con su reloj y su bucle de eventos. Un orquestador que ejecutara los SDKs en su propio
 * proceso mediría la carga del orquestador.
 *
 * El protocolo es el mismo para los tres SDKs, y el orquestador no sabe cuál es cuál:
 *   entra  {"id": n, "cmd": "<orden>", …}             una línea por orden
 *   sale   {"id": 0, "ready": true, "sdk": …}          cuando ya está conectado (o {"ok": false, "error": …} si no pudo)
 *          {"id": n, …}                                una línea por orden, con el mismo `id`
 */

import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const AQUI = path.dirname(fileURLToPath(import.meta.url));
export const RAIZ = path.join(AQUI, "..", "..");

/** El tope de espera a una orden, salvo que la orden traiga el suyo (`timeoutMs`): ni una espera infinita ni una impaciente. */
const TOPE_ORDEN_MS = 60_000;

/**
 * El binario del participante de Rust, compilado UNA vez por ejecución (cada agente lo lanza directo: `cargo run` por agente
 * serializaría los arranques con el bloqueo del directorio de compilación y gastaría segundos en cada uno).
 *
 * `FLUX_SDK_RUST_DIR` apunta a otra copia de `sdk-rust` (una estropeada a propósito, `security/mutantes-sdk.mjs`); su
 * `target/` es el de `CARGO_TARGET_DIR` si está definida —las dependencias, compiladas, se reutilizan— o el suyo.
 */
let binarioRust;
export function binarioDeRust() {
  if (binarioRust) return binarioRust;
  const dir = process.env.FLUX_SDK_RUST_DIR ? path.resolve(process.env.FLUX_SDK_RUST_DIR) : path.join(RAIZ, "sdk-rust");
  // `--features signing`: el participante firma y verifica si el escenario lo pide; sin la feature, pedirlo es un error.
  const r = spawnSync("cargo", ["build", "--quiet", "--example", "rpc_participant", "--features", "signing", "--message-format=json"], {
    cwd: dir,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
    env: process.env,
  });
  if (r.status !== 0) {
    const msg = (r.stderr || "").trim().split("\n").slice(-12).join("\n");
    throw new Error(`no se pudo compilar el participante de Rust (cargo build --example rpc_participant):\n${msg}`);
  }
  for (const linea of r.stdout.split("\n")) {
    if (!linea.startsWith("{")) continue;
    try {
      const m = JSON.parse(linea);
      if (m.reason === "compiler-artifact" && m.executable && m.target?.name === "rpc_participant") binarioRust = m.executable;
    } catch {
      /* una línea que no es de cargo */
    }
  }
  if (!binarioRust) throw new Error("cargo no dijo dónde dejó el ejecutable del participante de Rust");
  return binarioRust;
}

/**
 * El binario del participante de Go, compilado UNA vez por ejecución (como el de Rust: `go run` por agente gastaría segundos en cada
 * arranque y mediría la compilación). `FLUX_SDK_GO_DIR` apunta a otra copia de `sdk-go` (una estropeada a propósito).
 */
let binarioGo;
export function binarioDeGo() {
  if (binarioGo) return binarioGo;
  const dir = process.env.FLUX_SDK_GO_DIR ? path.resolve(process.env.FLUX_SDK_GO_DIR) : path.join(RAIZ, "sdk-go");
  const salida = path.join(mkdtempSync(path.join(tmpdir(), "flux-participante-go-")), "rpc-participant");
  const r = spawnSync("go", ["build", "-o", salida, "./cmd/rpc-participant"], { cwd: dir, encoding: "utf8", env: process.env });
  if (r.status !== 0) {
    const msg = (r.stderr || "").trim().split("\n").slice(-12).join("\n");
    throw new Error(`no se pudo compilar el participante de Go (go build ./cmd/rpc-participant):\n${msg}`);
  }
  binarioGo = salida;
  return binarioGo;
}

/** Un proceso participante y la conversación con él. */
export class Agente {
  /**
   * @param {object} o
   * @param {string} o.id        cómo lo llama el escenario
   * @param {"node"|"rust"|"go"} o.sdk
   * @param {object} o.env       FLUX_P_*
   */
  constructor({ id, sdk, env }) {
    this.id = id;
    this.sdk = sdk;
    this.env = env;
    this.siguiente = 1;
    this.esperando = new Map();
    this.stderr = "";
    this.cerrado = false;
    this.listo = null;
  }

  /** Lanza el proceso y espera a su línea `ready`. Devuelve `{ ok, error? }`: no lanza si el SDK no pudo CONECTAR (eso es un resultado). */
  async lanzar() {
    const [cmd, argv] =
      this.sdk === "node"
        ? [process.execPath, [path.join(AQUI, "participante-node.mjs")]]
        : this.sdk === "rust"
          ? [binarioDeRust(), []]
          : this.sdk === "go"
            ? [binarioDeGo(), []]
            : (() => {
              throw new Error(`SDK desconocido «${this.sdk}»`);
            })();
    this.hijo = spawn(cmd, argv, { cwd: RAIZ, env: { ...process.env, ...this.env }, stdio: ["pipe", "pipe", "pipe"] });
    let buffer = "";
    this.hijo.stdout.on("data", (d) => {
      buffer += d;
      let i;
      while ((i = buffer.indexOf("\n")) >= 0) {
        const linea = buffer.slice(0, i);
        buffer = buffer.slice(i + 1);
        this.#linea(linea);
      }
    });
    this.hijo.stderr.on("data", (d) => {
      this.stderr = (this.stderr + d).slice(-4000);
      if (process.env.FLUX_P_DEBUG) process.stderr.write(`[${this.id}] ${d}`);
    });
    this.hijo.stdin.on("error", () => {});
    this.salida = new Promise((resolver) => {
      this.hijo.once("close", (codigo, senal) => {
        this.cerrado = true;
        for (const { rechazar } of this.esperando.values()) rechazar(new Error(`el proceso (${this.sdk}) terminó (${senal ?? codigo})\n${this.stderr.trim()}`));
        this.esperando.clear();
        resolver(codigo);
      });
    });
    this.hijo.once("error", (e) => {
      this.cerrado = true;
      for (const { rechazar } of this.esperando.values()) rechazar(e);
    });
    this.listo = new Promise((resolver, rechazar) => {
      this.esperando.set(0, { resolver, rechazar });
      setTimeout(() => rechazar(new Error(`el participante (${this.sdk}) no llegó a estar listo en 60 s\n${this.stderr.trim()}`)), 60_000).unref();
    });
    const r = await this.listo;
    return r;
  }

  #linea(texto) {
    let o;
    try {
      o = JSON.parse(texto);
    } catch {
      this.stderr = (this.stderr + `\n(stdout) ${texto}`).slice(-4000);
      return;
    }
    const e = this.esperando.get(o.id);
    if (e) {
      this.esperando.delete(o.id);
      e.resolver(o);
    }
  }

  /** Manda una orden y espera su respuesta. Lanza si el proceso muere o no contesta en `timeoutMs`. */
  orden(cmd, { timeoutMs = TOPE_ORDEN_MS } = {}) {
    if (this.cerrado) return Promise.reject(new Error(`el proceso (${this.sdk}) ya terminó\n${this.stderr.trim()}`));
    const id = this.siguiente++;
    return new Promise((resolver, rechazar) => {
      const t = setTimeout(() => {
        this.esperando.delete(id);
        rechazar(new Error(`el participante (${this.sdk}) no contestó a «${cmd.cmd}» en ${timeoutMs} ms\n${this.stderr.trim()}`));
      }, timeoutMs);
      this.esperando.set(id, {
        resolver: (r) => (clearTimeout(t), resolver(r)),
        rechazar: (e) => (clearTimeout(t), rechazar(e)),
      });
      this.hijo.stdin.write(`${JSON.stringify({ ...cmd, id })}\n`);
    });
  }

  /** Cierra con `close` (que espera a lo que está en vuelo) y, si no acaba, mata. Nunca lanza. */
  async cerrar() {
    if (!this.hijo || this.cerrado) return;
    try {
      await this.orden({ cmd: "close" }, { timeoutMs: 8000 });
    } catch {
      /* se mata abajo */
    }
    await Promise.race([this.salida, new Promise((r) => setTimeout(r, 3000))]);
    if (!this.cerrado) this.hijo.kill("SIGKILL");
  }

  /** Mata sin contemplaciones (SIGKILL): un servicio que muere. */
  matar() {
    if (this.hijo && !this.cerrado) this.hijo.kill("SIGKILL");
  }
}
