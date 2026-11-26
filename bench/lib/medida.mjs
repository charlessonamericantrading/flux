/**
 * Una VENTANA de medida: lo que gastó cada proceso entre dos instantes, medido desde fuera.
 *
 * Qué se toma, y con qué instrumento:
 *
 *   · CPU de cada proceso      `/proc/<pid>/task/<tid>/schedstat` (ns en CPU de cada hilo; usuario + sistema), con `/proc/<pid>/stat`
 *                              (tics de 10 ms) como contraste: ver `cpuDelta`.
 *   · Bytes y mensajes NATS    `/varz` de `nats-server` (`in_msgs`, `out_msgs`, `in_bytes`, `out_bytes`). Son los
 *                              bytes de APLICACIÓN que el servidor leyó y escribió en los sockets de los clientes,
 *                              ya descifrados (con TLS no cuentan la cabecera de los registros TLS).
 *   · Bytes y peticiones HTTP  los cuentan los propios roles con `socket.bytesRead + socket.bytesWritten` del lado
 *                              servidor: lo mismo, bytes de aplicación sin cabeceras TCP/IP, y comparable con el de NATS.
 *   · Carga ajena              CPU total de la máquina (`/proc/stat`) menos la de todos los procesos del banco: lo
 *                              que gastó OTRO. Una medida con carga ajena alta no es de fiar y el runner la repite.
 *   · Saturación               un proceso de Node por encima de ~85 % de un núcleo es un cuello de botella del
 *                              PROPIO banco (un solo hilo de JS): su número deja de medir al sistema bajo prueba.
 *
 * La ventana empieza DESPUÉS del calentamiento: la primera conexión, el JIT y la carga inicial no cuentan.
 */

import { cargaMedia, cpuDeAjenos, cpuDelta, fraccionSistema, cpuGlobal, cpuSegundos, dormir, hijosDe, memoriaMb, ahoraMs } from "./proc.mjs";

/** Límite de CPU de un proceso de Node (1 hilo de JS ≈ 1,0 núcleo; por encima, el banco se estorba a sí mismo). */
export const SATURACION_NODE = 0.85;
/** Carga ajena tolerada, en núcleos: por encima el runner repite la repetición. */
export const AJENA_MAX = 0.25;

const resta = (a, b) => {
  const out = {};
  for (const k of Object.keys(a)) {
    if (typeof a[k] !== "number") continue;
    // `g_*` son medidores (un valor, no un acumulado): se queda el último.
    out[k] = k.startsWith("g_") ? a[k] : a[k] - (b?.[k] ?? 0);
  }
  return out;
};

async function foto({ procesos, broker, workers, runnerPid }) {
  const t = ahoraMs();
  const propios = new Set([runnerPid, ...hijosDe(runnerPid)]);
  const ajenos = cpuDeAjenos(propios);
  const cpu = {};
  for (const p of procesos) cpu[p.etiqueta] = cpuSegundos(p.pid);
  const cpuRunner = cpuSegundos(runnerPid);
  const global = cpuGlobal();
  const [varz, snaps] = await Promise.all([
    broker ? broker.varz() : undefined,
    Promise.all(workers.map(async (w) => [w.etiqueta, await w.snap()])),
  ]);
  return { t, cpu, cpuRunner, global, varz, snaps: Object.fromEntries(snaps), carga: cargaMedia(), ajenos };
}

/**
 * @param {object} o
 * @param {{etiqueta:string,pid:number,grupo:string}[]} o.procesos  los procesos medidos (incluye el `nats-server`)
 * @param {object} [o.broker]   para `/varz`
 * @param {object[]} o.workers  los `Proceso` que contestan `snap()` (con `etiqueta`)
 * @param {number} o.ventanaMs
 * @param {Function} [o.durante]  se llama a mitad de ventana (para sondeos propios)
 */
export async function medirVentana({ procesos, broker, workers, ventanaMs, durante }) {
  const a = await foto({ procesos, broker, workers, runnerPid: process.pid });
  const picoAntes = Object.fromEntries(procesos.map((p) => [p.etiqueta, memoriaMb(p.pid)]));
  const espera = dormir(ventanaMs);
  const extra = durante?.();
  await espera;
  await extra;
  const b = await foto({ procesos, broker, workers, runnerPid: process.pid });

  const durMs = b.t - a.t;
  const durS = durMs / 1000;
  const cpu = {};
  const nucleos = {};
  const sistema = {};
  for (const p of procesos) {
    sistema[p.etiqueta] = fraccionSistema(a.cpu[p.etiqueta], b.cpu[p.etiqueta]);
    cpu[p.etiqueta] = cpuDelta(a.cpu[p.etiqueta], b.cpu[p.etiqueta]);
    nucleos[p.etiqueta] = cpu[p.etiqueta] / durS;
  }
  const cpuRunner = cpuDelta(a.cpuRunner, b.cpuRunner);
  const bench = Object.values(cpu).reduce((x, y) => x + (Number.isFinite(y) ? y : 0), 0) + cpuRunner;
  const ocupada = b.global.ocupada - a.global.ocupada;
  const ajena = Math.max(0, ocupada - bench) / durS; // en núcleos
  const robo = (b.global.steal - a.global.steal) / durS; // CPU que el hipervisor se llevó
  const mem = Object.fromEntries(procesos.map((p) => [p.etiqueta, memoriaMb(p.pid)]));

  // ── QUIÉNES eran los ajenos: los procesos que no son del banco y gastaron algo en la ventana ──
  const porComando = new Map();
  for (const [pid, d1] of b.ajenos) {
    const d0 = a.ajenos.get(pid);
    const gasto = d0 ? d1.cpu - d0.cpu : d1.cpu; // un proceso nacido en la ventana: todo lo suyo es de la ventana
    if (gasto / durS < 0.02) continue; // por debajo del 2 % de un núcleo no cuenta como «ocupado»
    const e = porComando.get(d1.comando) ?? { procesos: 0, nucleos: 0 };
    e.procesos++;
    e.nucleos += gasto / durS;
    porComando.set(d1.comando, e);
  }
  const ajenosOcupados = [...porComando.entries()].map(([comando, e]) => ({ comando, ...e })).sort((x, y) => y.nucleos - x.nucleos);

  const nats = broker
    ? {
        in_msgs: b.varz.in_msgs - a.varz.in_msgs,
        out_msgs: b.varz.out_msgs - a.varz.out_msgs,
        in_bytes: b.varz.in_bytes - a.varz.in_bytes,
        out_bytes: b.varz.out_bytes - a.varz.out_bytes,
        conexiones: b.varz.connections,
        g_slow_consumers: b.varz.slow_consumers,
      }
    : undefined;

  const snaps = {};
  for (const w of workers) snaps[w.etiqueta] = resta(b.snaps[w.etiqueta], a.snaps[w.etiqueta]);

  // ── ¿se puede fiar de esta ventana? ──
  const avisos = [];
  for (const p of procesos) {
    if (p.grupo !== "nats" && nucleos[p.etiqueta] > SATURACION_NODE) {
      avisos.push(`saturado: «${p.etiqueta}» usó ${(nucleos[p.etiqueta] * 100).toFixed(0)} % de un núcleo`);
    }
  }
  const nNucleos = Math.round((b.global.total - a.global.total) / durS);
  if ((ocupada / durS) / Math.max(1, nNucleos) > 0.85) {
    avisos.push(`máquina casi llena: ${((ocupada / durS) / nNucleos * 100).toFixed(0)} % de todas las CPU`);
  }
  if (ajena > AJENA_MAX) avisos.push(`carga ajena: ${ajena.toFixed(2)} núcleos gastados por procesos que no son del banco`);

  return {
    t0: a.t,
    t1: b.t,
    durS,
    cpu,
    nucleos,
    sistema,
    cpuRunner,
    ajena,
    ajenosProcesos: ajenosOcupados.reduce((x, e) => x + e.procesos, 0),
    ajenosTop: ajenosOcupados.slice(0, 4),
    robo,
    mem,
    picoAntes,
    nats,
    snaps,
    cargaAntes: a.carga,
    cargaDespues: b.carga,
    avisos,
  };
}
