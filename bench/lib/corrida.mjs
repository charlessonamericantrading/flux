/**
 * La mecánica común de UNA corrida de cualquier escenario:
 *
 *   lanzar los procesos → que estén listos → arrancar el tráfico → calentar → ventana medida → recoger registros → cerrar
 *
 * y la política de repetición: una repetición con avisos de fiabilidad (un proceso saturado, carga ajena) se repite hasta
 * `reintentos` veces; si sigue mal, se conserva MARCADA, no se esconde.
 */

import { dormir } from "./proc.mjs";
import { lanzarTodos } from "./orquesta.mjs";
import { medirVentana, AJENA_MAX } from "./medida.mjs";

/**
 * @param {object} o
 * @param {{rol:string,etiqueta:string,grupo:string,cfg:object,orden?:number}[]} o.procesos  `orden` menor arranca antes
 * @param {object} [o.broker]
 * @param {number} o.calentamientoS
 * @param {number} o.ventanaS
 * @param {number} [o.drenajeS]   tiempo extra con el tráfico vivo tras la ventana, para que lleguen los últimos registros
 * @returns {Promise<{ medida, registros: Record<string, any>, infos }>}
 */
export async function correr({ procesos, broker, calentamientoS, ventanaS, drenajeS = 1 }) {
  const { ps, infos } = await lanzarTodos(procesos.map((p) => ({ rol: p.rol, etiqueta: p.etiqueta, cfg: p.cfg, esperaMs: p.esperaMs, orden: p.orden, execArgv: p.execArgv })));
  try {
    const medidos = [
      ...ps.map((p, i) => ({ etiqueta: p.etiqueta, pid: p.pid, grupo: procesos[i].grupo })),
      ...(broker ? [{ etiqueta: "nats-server", pid: broker.pid, grupo: "nats" }] : []),
    ];
    // Arrancan en el orden pedido (los que escuchan antes que los que llaman) y SIN esperar a que terminen unos a otros.
    const ordenes = [...new Set(procesos.map((p) => p.orden ?? 0))].sort((a, b) => a - b);
    for (const o of ordenes) {
      await Promise.all(ps.filter((_, i) => (procesos[i].orden ?? 0) === o).map((p) => p.arrancar()));
    }
    await dormir(calentamientoS * 1000);
    const medida = await medirVentana({ procesos: medidos, broker, workers: ps, ventanaMs: ventanaS * 1000 });
    await dormir(drenajeS * 1000);
    const registros = {};
    for (const p of ps) registros[p.etiqueta] = await p.fin();
    for (const p of ps) {
      if (p.errores.length) medida.avisos.push(`errores en «${p.etiqueta}»: ${p.errores.slice(0, 3).join(" | ").slice(0, 400)}`);
    }
    return { medida, registros, infos };
  } finally {
    await Promise.allSettled(ps.map((p) => p.cerrar()));
  }
}

/** Lo que toda repetición guarda de la calidad de su medida (carga de la máquina, quién más estaba gastando CPU). */
export const metaMedida = (m) => ({
  avisos: m.avisos,
  ajena: m.ajena,
  ajenosProcesos: m.ajenosProcesos,
  ajenosTop: m.ajenosTop,
  robo: m.robo,
  cargaAntes: m.cargaAntes,
  cargaDespues: m.cargaDespues,
});

const config = { reintentos: 2 };
/** Cuántas veces se repite una repetición con avisos de fiabilidad (0 = nunca: para medir con la máquina cargada, a sabiendas). */
export const fijarReintentos = (n) => void (config.reintentos = n);

/** Repite `unaVez()` mientras la corrida traiga avisos de FIABILIDAD (no de errores de lógica), hasta `reintentos` veces. */
export async function conReintentos(unaVez, { reintentos = config.reintentos, etiqueta = "", sinSaturacion = false } = {}) {
  let ultimo;
  // `sinSaturacion`: en un bucle cerrado (escenario C) saturar al más lento es el objeto de la medida, no un fallo.
  const dudosaRe = sinSaturacion ? /carga ajena/ : /saturado|carga ajena|máquina casi llena/;
  for (let intento = 0; intento <= reintentos; intento++) {
    ultimo = await unaVez(intento);
    const dudosa = ultimo.medida.avisos.some((a) => dudosaRe.test(a));
    if (!dudosa) return { ...ultimo, intentos: intento + 1 };
    if (intento === reintentos) break;
    console.error(`    ↻ ${etiqueta}: ${ultimo.medida.avisos.join("; ")} — se repite (${intento + 1}/${reintentos + 1})`);
    await dormir(3000);
  }
  return { ...ultimo, intentos: reintentos + 1, dudosa: true };
}

export { AJENA_MAX };
