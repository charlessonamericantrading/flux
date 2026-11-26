/**
 * ESCENARIO B, flux — el PRODUCTOR: UN servicio flux (`pedidos-api`, dueño de `pedidos`) que publica UN evento por hecho con
 * `bus.publish`, sin saber cuántos lo reciben. `publish` espera el acuse de JetStream (el evento ya está en el stream).
 */

import { ahoraMs } from "../lib/proc.mjs";
import { programar } from "../lib/comun.mjs";
import { conectarServicio } from "../lib/flux.mjs";
import { datosPedido, pedidoId, SUBJECT_PEDIDO } from "../lib/pedido.mjs";

export async function crear(cfg) {
  const bus = await conectarServicio({ url: cfg.url, caFile: cfg.caFile, servicio: "pedidos-api", semilla: cfg.nkey });
  const c = { hechos: 0, errores: 0 };
  const enviados = [];
  let seq = 0;
  // Un publish de calibración (hecho nº 0 de la corrida): abre la ruta de publicación antes de medir. El runner lo descarta.
  await bus.publish(SUBJECT_PEDIDO, datosPedido(cfg.base), { aggregateId: pedidoId(cfg.base) });

  let parar;
  return {
    info: () => ({ pid: process.pid }),
    arranca() {
      parar = programar(cfg.hechosPorMin, cfg.semilla, async () => {
        const n = cfg.base + ++seq;
        enviados.push([n, ahoraMs()]);
        c.hechos++;
        try {
          await bus.publish(SUBJECT_PEDIDO, datosPedido(n), { aggregateId: pedidoId(n) });
        } catch (e) {
          c.errores++;
          console.error("[bench] publish falló:", e?.message ?? e);
        }
      });
    },
    snap: () => ({ ...c }),
    async fin() {
      parar?.();
      return { enviados };
    },
    async cerrar() {
      await bus.close();
    },
  };
}
