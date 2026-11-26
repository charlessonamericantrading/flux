/**
 * ESCENARIO C, «NATS crudo» — el que responde SIN el SDK de flux: una suscripción core al subject exacto de la operación y
 * `msg.respond` con un JSON pelado (sin sobre CloudEvent). Mismo broker, mismas credenciales y mismos permisos que el servicio
 * flux: lo único que cambia es lo que hay ENCIMA de la librería de NATS. Sirve para repartir el coste entre el transporte y el SDK.
 */

import { conectarCrudo } from "../lib/flux.mjs";

export const SUBJECT = "rpc.clientes.cliente.v1.calcular-descuento";

export async function crear(cfg) {
  const nc = await conectarCrudo({ url: cfg.url, caFile: cfg.caFile, servicio: "crm-api", semilla: cfg.nkey });
  const c = { atendidas: 0 };
  const dec = new TextDecoder();
  const enc = new TextEncoder();
  const sub = nc.subscribe(SUBJECT, {
    callback: (err, msg) => {
      if (err) return;
      c.atendidas++;
      const p = JSON.parse(dec.decode(msg.data));
      msg.respond(enc.encode(JSON.stringify({ descuentoCents: Math.floor(p.totalCents * 0.05), moneda: p.moneda })));
    },
  });
  await nc.flush();
  return {
    info: () => ({ pid: process.pid }),
    arranca() {},
    snap: () => ({ ...c }),
    async fin() {
      return {};
    },
    async cerrar() {
      sub.unsubscribe();
      await nc.drain();
    },
  };
}
