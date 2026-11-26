/**
 * ESCENARIO A, flux — N observadores: cada uno es UN servicio flux del manifiesto (su nkey, su durable de estado,
 * su inbox privado) con su propia conexión y un `bus.state.watch` sobre `clientes.cliente.v1`, confinado al tenant `acme`.
 *
 * Registra cuándo vio cada versión de la clave de la ejecución, cuántas veces (un duplicado es un hallazgo) y cuántas
 * entradas ajenas le llegaron.
 */

import { ahoraMs } from "../lib/proc.mjs";
import { conLimite, conectarServicio } from "../lib/flux.mjs";

const CONTRATO = "clientes.cliente.v1";

export async function crear(cfg) {
  const c = { entregas: 0, duplicados: 0, ajenas: 0 };
  const servicios = [];

  await conLimite(cfg.servicios, cfg.conexionesALaVez ?? 25, async (s, i) => {
    const bus = await conectarServicio({ url: cfg.url, caFile: cfg.caFile, servicio: s.name, semilla: s.nkey });
    const estado = { bus, visto: [], versiones: new Set(), sincronizado: false, i };
    let sincronizar;
    const listo = new Promise((r) => (sincronizar = r));
    estado.sub = await bus.state.watch(
      CONTRATO,
      async (entrada) => {
        if (entrada.key !== cfg.clave) {
          c.ajenas++;
          return;
        }
        c.entregas++;
        if (estado.versiones.has(entrada.aggregateVersion)) {
          c.duplicados++;
          return;
        }
        estado.versiones.add(entrada.aggregateVersion);
        if (entrada.aggregateVersion >= 2) estado.visto.push([entrada.aggregateVersion, ahoraMs()]);
      },
      { tenantId: "acme", onSynced: () => ((estado.sincronizado = true), sincronizar()) },
    );
    await Promise.race([listo, new Promise((_, rj) => setTimeout(() => rj(new Error(`${s.name}: onSynced no llegó en 60 s`)), 60_000))]);
    servicios[i] = estado;
  });

  return {
    info: () => ({ pid: process.pid, servicios: servicios.length }),
    arranca() {},
    snap: () => ({ ...c }),
    async fin() {
      return { visto: servicios.map((s) => s.visto) };
    },
    async cerrar() {
      await Promise.allSettled(servicios.map(async (s) => {
        await s.sub.unsubscribe();
        await s.bus.close();
      }));
    },
  };
}
