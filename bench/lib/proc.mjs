/**
 * Lo que el banco lee de `/proc`: CPU y memoria de CADA proceso medido y la ocupación global de la máquina.
 *
 * Se lee `/proc/<pid>/stat` y no `process.cpuUsage()` porque hay procesos que no son Node (`nats-server`) y
 * porque `cpuUsage()` de dentro no ve los hilos de GC de V8 de forma homogénea con el resto: con `/proc` todos
 * los procesos se miden con el mismo instrumento, desde fuera, sumando todos sus hilos.
 *
 * La resolución es un tic (`CLK_TCK`, 100 Hz = 10 ms). En una ventana de 15 s son 1 500 tics: 0,07 % de un
 * núcleo por tic. Un proceso casi ocioso mide «0 tics» y eso es lo que hay: se publica.
 */

import { readFileSync, readdirSync } from "node:fs";
import { spawnSync } from "node:child_process";

export const TIC = (() => {
  const r = spawnSync("getconf", ["CLK_TCK"], { encoding: "utf8" });
  const n = Number((r.stdout ?? "").trim());
  return Number.isFinite(n) && n > 0 ? n : 100;
})();

/**
 * CPU acumulada de un proceso, con DOS instrumentos:
 *
 *   `tic`   `/proc/<pid>/stat` (utime + stime): incluye los hilos que ya terminaron, pero con resolución de un tic (10 ms).
 *   `fino`  la suma de `/proc/<pid>/task/<tid>/schedstat` (primer campo: ns en CPU de cada hilo): resolución de ns, pero
 *           pierde lo que gastó un hilo que ya no existe. `NaN` si el núcleo no lo expone.
 *
 * `cpuDelta` elige entre los dos (ver allí). Los dos devuelven SEGUNDOS; `NaN` si el proceso ya no existe.
 */
export function cpuSegundos(pid) {
  let tic = NaN;
  let uTic = NaN;
  let sTic = NaN;
  try {
    const s = readFileSync(`/proc/${pid}/stat`, "utf8");
    // El segundo campo (`comm`) puede llevar espacios y paréntesis: se parte DESPUÉS del último `)`.
    const campos = s.slice(s.lastIndexOf(")") + 2).split(" ");
    // tras `comm`: estado(0) ppid(1) … utime es el campo 14 de la línea completa = índice 11 aquí; stime, 12.
    uTic = Number(campos[11]) / TIC;
    sTic = Number(campos[12]) / TIC;
    tic = uTic + sTic;
  } catch {
    return { tic: NaN, fino: NaN };
  }
  let fino = 0;
  try {
    for (const t of readdirSync(`/proc/${pid}/task`)) {
      fino += Number(readFileSync(`/proc/${pid}/task/${t}/schedstat`, "utf8").split(" ")[0]);
    }
    fino /= 1e9;
  } catch {
    fino = NaN;
  }
  return { tic, fino, uTic, sTic };
}

/** Qué fracción de la CPU entre dos lecturas se gastó EN EL NÚCLEO (llamadas al sistema: red, epoll, futex, ficheros). */
export function fraccionSistema(a, b) {
  const u = b.uTic - a.uTic;
  const s = b.sTic - a.sTic;
  return u + s > 0 ? s / (u + s) : NaN;
}

/**
 * Los segundos de CPU entre dos lecturas de `cpuSegundos`. Se usa la fina (ns) salvo que se quede CORTA respecto al tic
 * por más de 20 ms + 3 %: señal de que un hilo terminó y se llevó su cuenta, y entonces manda el tic.
 */
export function cpuDelta(a, b) {
  const dTic = b.tic - a.tic;
  const dFino = b.fino - a.fino;
  if (!Number.isFinite(dFino)) return dTic;
  return dTic - dFino > 0.02 + 0.03 * dTic ? dTic : dFino;
}

/** `{ rssMb, picoMb }` de un proceso (`VmRSS`, `VmHWM`). */
export function memoriaMb(pid) {
  try {
    const t = readFileSync(`/proc/${pid}/status`, "utf8");
    const kb = (clave) => Number(new RegExp(`^${clave}:\\s+(\\d+)\\s+kB`, "m").exec(t)?.[1] ?? NaN);
    return { rssMb: kb("VmRSS") / 1024, picoMb: kb("VmHWM") / 1024 };
  } catch {
    return { rssMb: NaN, picoMb: NaN };
  }
}

/** Número de conexiones TCP abiertas por un proceso (descriptores de tipo socket). */
export function descriptoresAbiertos(pid) {
  try {
    return readFileSync(`/proc/${pid}/status`, "utf8").match(/^FDSize:\s+(\d+)/m)?.[1];
  } catch {
    return undefined;
  }
}

/** Segundos de CPU «ocupada» y totales de TODA la máquina (todas las CPU), de `/proc/stat`. */
export function cpuGlobal() {
  const l = readFileSync("/proc/stat", "utf8").split("\n")[0].trim().split(/\s+/).slice(1).map(Number);
  // user nice system idle iowait irq softirq steal …
  const [user, nice, system, idle, iowait = 0, irq = 0, softirq = 0, steal = 0] = l;
  const ocupada = user + nice + system + irq + softirq;
  return { ocupada: ocupada / TIC, total: (ocupada + idle + iowait + steal) / TIC, steal: steal / TIC };
}

/** `[1 min, 5 min, 15 min]` de `/proc/loadavg`. */
export function cargaMedia() {
  return readFileSync("/proc/loadavg", "utf8").trim().split(/\s+/).slice(0, 3).map(Number);
}

/** Todos los hijos (recursivo) de un pid, por si un proceso medido lanza otros. */
export function hijosDe(pid) {
  const out = [];
  const visitar = (p) => {
    let hs = [];
    try {
      hs = readFileSync(`/proc/${p}/task/${p}/children`, "utf8").trim().split(/\s+/).filter(Boolean).map(Number);
    } catch {
      return;
    }
    for (const h of hs) {
      out.push(h);
      visitar(h);
    }
  };
  visitar(pid);
  return out;
}

/**
 * CPU acumulada (s) de CADA proceso de la máquina que NO es del banco: `Map(pid → { cpu, comando })`. Para saber, además de
 * CUÁNTO gastaron los demás (`/proc/stat` menos los nuestros), QUIÉNES y cuántos eran: una medida con `rustc` y `java`
 * compilando al lado no se lee igual que una con un `sshd` ocioso.
 */
export function cpuDeAjenos(propios) {
  const out = new Map();
  let pids = [];
  try {
    pids = readdirSync("/proc").filter((d) => /^\d+$/.test(d)).map(Number);
  } catch {
    return out;
  }
  for (const pid of pids) {
    if (propios.has(pid)) continue;
    try {
      const s = readFileSync(`/proc/${pid}/stat`, "utf8");
      const comando = s.slice(s.indexOf("(") + 1, s.lastIndexOf(")"));
      const campos = s.slice(s.lastIndexOf(")") + 2).split(" ");
      out.set(pid, { cpu: (Number(campos[11]) + Number(campos[12])) / TIC, comando });
    } catch {
      /* el proceso terminó entre listar y leer */
    }
  }
  return out;
}

export const dormir = (ms) => new Promise((r) => setTimeout(r, ms));
/** Reloj monotónico en ms con fracciones: el MISMO en todos los procesos de la máquina (CLOCK_MONOTONIC). */
export const ahoraMs = () => Number(process.hrtime.bigint()) / 1e6;
