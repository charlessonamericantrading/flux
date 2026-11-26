/**
 * ESCENARIO B, flux — N CONSUMIDORES: cada uno es un servicio flux del manifiesto (su nkey, su durable sobre
 * `pedidos.pedido.v1.creado`, su inbox privado) con su propia conexión y `bus.subscribe`. El handler hace lo mismo que el
 * receptor de webhook: anotar qué hecho es y cuándo llegó, y deduplicar por `id` (obligatorio en un consumidor flux).
 */

import { ahoraMs } from "../lib/proc.mjs";
import { conLimite, conectarServicio } from "../lib/flux.mjs";
import { SUBJECT_PEDIDO, seqDe } from "../lib/pedido.mjs";

export async function crear(cfg) {
  const c = { entregas: 0, duplicados: 0 };
  const recibidos = []; // por consumidor: [[seq, t], …]
  const buses = [];
  const subs = [];

  await conLimite(cfg.servicios, cfg.conexionesALaVez ?? 25, async (s, i) => {
    const bus = await conectarServicio({ url: cfg.url, caFile: cfg.caFile, servicio: s.name, semilla: s.nkey });
    const log = [];
    const vistos = new Set();
    recibidos[i] = log;
    buses[i] = bus;
    subs[i] = await bus.subscribe(SUBJECT_PEDIDO, async (evento) => {
      const t = ahoraMs();
      c.entregas++;
      if (vistos.has(evento.id)) {
        c.duplicados++;
        return;
      }
      vistos.add(evento.id);
      if (cfg.sabotaje === "descarta" && i === 0) return; // autoprueba: un consumidor que se traga lo que recibe
      log.push([seqDe(evento.data.pedidoId), t]);
    });
  });

  return {
    info: () => ({ pid: process.pid, consumidores: buses.length }),
    arranca() {},
    snap: () => ({ ...c }),
    async fin() {
      return { recibidos };
    },
    async cerrar() {
      await Promise.allSettled(buses.map(async (b, i) => {
        await subs[i]?.unsubscribe?.();
        await b.close();
      }));
    },
  };
}
