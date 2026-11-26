/**
 * ESCENARIO C, flux — el que LLAMA: `facturacion-api` con `bus.request` a `clientes.cliente.v1.calcular-descuento`, en bucle
 * cerrado con la misma concurrencia que el cliente HTTP.
 */

import { conectarServicio } from "../lib/flux.mjs";
import { cargaCerrada } from "../lib/carga.mjs";
import { OPERACION } from "./c-flux-servidor.mjs";

export async function crear(cfg) {
  const bus = await conectarServicio({ url: cfg.url, caFile: cfg.caFile, servicio: "facturacion-api", semilla: cfg.nkey });
  const hacer = async () => {
    const r = await bus.request(OPERACION, { clienteId: "cli-1", totalCents: 9990, moneda: "EUR" }, { timeoutMs: 10_000 });
    if (r.descuentoCents !== 499) throw new Error("respuesta inesperada");
  };
  await hacer(); // calibración: el camino funciona
  const carga = cargaCerrada({ concurrencia: cfg.concurrencia, hacerPeticion: hacer });
  return {
    info: () => ({ pid: process.pid }),
    arranca: () => carga.arranca(),
    snap: () => carga.snap(),
    async fin() {
      await carga.parar();
      return carga.registros();
    },
    async cerrar() {
      await bus.close();
    },
  };
}
