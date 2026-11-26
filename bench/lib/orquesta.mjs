/**
 * El lado del runner de `worker.mjs`: lanza procesos, les habla por IPC y los mata.
 */

import { fork } from "node:child_process";
import path from "node:path";

import { BENCH } from "./entorno.mjs";

const WORKER = path.join(BENCH, "worker.mjs");

export class Proceso {
  /** @param {string} rol  nombre de fichero en `roles/`  @param {string} etiqueta  cómo se llama en los informes */
  constructor(rol, etiqueta = rol) {
    this.rol = rol;
    this.etiqueta = etiqueta;
    this.hijo = undefined;
    this.pid = undefined;
    this.esperas = new Map(); // tipo → [resolver, rechazar]
    this.errores = [];
    this.salio = false;
    this.sigId = 1;
  }

  async lanzar(execArgv = []) {
    this.hijo = fork(WORKER, [this.rol], {
      serialization: "advanced",
      execArgv: ["--max-old-space-size=3072", ...execArgv],
      stdio: ["ignore", "inherit", "inherit", "ipc"],
    });
    this.pid = this.hijo.pid;
    this.hijo.on("message", (m) => this.#alRecibir(m));
    this.hijo.once("exit", (code, signal) => {
      this.salio = true;
      const e = new Error(`el proceso «${this.etiqueta}» (${this.rol}) terminó solo: código ${code}, señal ${signal}`);
      for (const [, [, rj]] of this.esperas) rj(e);
      this.esperas.clear();
      if (!this.cerrando) this.errores.push(e.message);
    });
    await this.#esperar("hola");
    return this;
  }

  #alRecibir(m) {
    if (m.t === "error") {
      this.errores.push(`${m.en}: ${m.mensaje}`);
      // Si alguien espera algo de este proceso, se le rechaza ya: un worker roto no debe colgar el banco.
      for (const [, [, rj]] of this.esperas) rj(new Error(`«${this.etiqueta}»: ${m.mensaje}`));
      this.esperas.clear();
      return;
    }
    const clave = m.id !== undefined ? `${m.t}:${m.id}` : m.t;
    const e = this.esperas.get(clave);
    if (e) {
      this.esperas.delete(clave);
      e[0](m);
    }
  }

  #esperar(clave, ms = 600_000) {
    if (this.salio) return Promise.reject(new Error(`«${this.etiqueta}» ya había terminado`));
    return new Promise((ok, rj) => {
      const t = setTimeout(() => {
        this.esperas.delete(clave);
        rj(new Error(`«${this.etiqueta}»: sin respuesta «${clave}» en ${ms / 1000} s`));
      }, ms);
      this.esperas.set(clave, [(m) => (clearTimeout(t), ok(m)), (e) => (clearTimeout(t), rj(e))]);
    });
  }

  async configurar(cfg, ms) {
    this.hijo.send({ t: "config", cfg });
    return (await this.#esperar("listo", ms)).info;
  }
  async arrancar() {
    this.hijo.send({ t: "arranca" });
    await this.#esperar("arrancado");
  }
  async snap() {
    const id = this.sigId++;
    this.hijo.send({ t: "snap", id });
    return (await this.#esperar(`snap:${id}`, 60_000)).datos;
  }
  async fin() {
    this.hijo.send({ t: "fin" });
    return (await this.#esperar("fin", 120_000)).datos;
  }
  async cerrar() {
    this.cerrando = true;
    if (!this.hijo || this.salio) return;
    const fin = new Promise((r) => this.hijo.once("exit", r));
    try {
      this.hijo.send({ t: "salir" });
    } catch {
      /* ya cerrado */
    }
    const dura = setTimeout(() => this.hijo.kill("SIGKILL"), 8000);
    await fin;
    clearTimeout(dura);
  }
}

/**
 * Lanza todos los procesos a la vez y los CONFIGURA por grupos de `orden` (de menor a mayor; los del mismo grupo, a la vez):
 * el que responde debe estar escuchando antes de que el que llama haga su petición de calibración.
 * Si uno falla, cierra todos y relanza el error.
 */
export async function lanzarTodos(especificaciones) {
  const ps = especificaciones.map((e) => new Proceso(e.rol, e.etiqueta));
  try {
    await Promise.all(ps.map((p, i) => p.lanzar(especificaciones[i].execArgv)));
    const infos = new Array(ps.length);
    const ordenes = [...new Set(especificaciones.map((e) => e.orden ?? 0))].sort((a, b) => a - b);
    for (const o of ordenes) {
      await Promise.all(
        ps.map(async (p, i) => {
          if ((especificaciones[i].orden ?? 0) !== o) return;
          infos[i] = await p.configurar(especificaciones[i].cfg, especificaciones[i].esperaMs);
        }),
      );
    }
    return { ps, infos };
  } catch (e) {
    await Promise.allSettled(ps.map((p) => p.cerrar()));
    throw e;
  }
}
