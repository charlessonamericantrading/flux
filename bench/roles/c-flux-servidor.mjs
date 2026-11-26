/**
 * ESCENARIO C, flux — el que RESPONDE: `crm-api` (dueño de `clientes`) con `bus.respond` sobre
 * `clientes.cliente.v1.calcular-descuento`. El handler hace lo mismo que el servidor HTTP: calcula y devuelve.
 *
 * Un responder real deduplica por el `id` de la petición (11-request-reply.md §4.5). Este cálculo es una función pura
 * (idempotente por naturaleza, estrategia B de AGENTS.md §4): repetirlo no tiene efecto, así que no guarda nada.
 */

import { conectarServicio } from "../lib/flux.mjs";

export const OPERACION = "clientes.cliente.v1.calcular-descuento";

export async function crear(cfg) {
  const bus = await conectarServicio({ url: cfg.url, caFile: cfg.caFile, servicio: "crm-api", semilla: cfg.nkey });
  const c = { atendidas: 0 };
  const responder = await bus.respond(OPERACION, async (peticion) => {
    c.atendidas++;
    return { descuentoCents: Math.floor(peticion.totalCents * 0.05), moneda: peticion.moneda };
  });
  return {
    info: () => ({ pid: process.pid }),
    arranca() {},
    snap: () => ({ ...c }),
    async fin() {
      return {};
    },
    async cerrar() {
      await responder.stop?.();
      await bus.close();
    },
  };
}
