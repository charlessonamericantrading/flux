/**
 * ESCENARIO A, flux — el CRM: UN servicio flux (`crm-api`, dueño de `clientes`) que escribe el estado de un cliente con
 * `bus.state.put` `cambiosPorMin` veces por minuto. El valor es el mismo que sirve el CRM HTTP.
 */

import { ahoraMs } from "../lib/proc.mjs";
import { programar } from "../lib/comun.mjs";
import { conectarServicio } from "../lib/flux.mjs";

const CONTRATO = "clientes.cliente.v1";
const datosDe = (clave, v) => ({
  clienteId: clave,
  aggregateVersion: v,
  nombre: "Ana García Ruiz",
  email: "ana.garcia@example.com",
  pais: "ES",
});

export async function crear(cfg) {
  const bus = await conectarServicio({ url: cfg.url, caFile: cfg.caFile, servicio: "crm-api", semilla: cfg.nkey });
  let version = 1;
  const cambios = [];
  const c = { puts: 0, stale: 0, errores: 0, cambios: 0 };
  // El valor inicial (versión 1) existe antes de que nadie observe: es la «carga inicial».
  const r0 = await bus.state.put(CONTRATO, cfg.clave, datosDe(cfg.clave, version));
  if (!r0.applied) throw new Error(`el put inicial no se aplicó: ${JSON.stringify(r0)}`);

  let parar;
  return {
    info: () => ({ pid: process.pid }),
    arranca() {
      parar = programar(cfg.cambiosPorMin, cfg.semilla, async () => {
        const v = version + 1;
        const t = ahoraMs(); // el cambio «ocurre» al empezar el put: el coste de escribirlo en el stream es parte de enterarse
        try {
          const r = await bus.state.put(CONTRATO, cfg.clave, datosDe(cfg.clave, v));
          c.puts++;
          if (r.applied) {
            version = v;
            cambios.push({ v, t });
            c.cambios++;
          } else c.stale++;
        } catch (e) {
          c.errores++;
          console.error("[bench] put falló:", e?.message ?? e);
        }
      });
    },
    snap: () => ({ ...c }),
    async fin() {
      parar?.();
      return { cambios };
    },
    async cerrar() {
      await bus.close();
    },
  };
}
