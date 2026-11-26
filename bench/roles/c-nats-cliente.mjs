/**
 * ESCENARIO C, «NATS crudo» — el que llama SIN el SDK de flux: `nc.request` con un JSON pelado, en bucle cerrado.
 */

import { conectarCrudo } from "../lib/flux.mjs";
import { cargaCerrada } from "../lib/carga.mjs";
import { SUBJECT } from "./c-nats-servidor.mjs";

export async function crear(cfg) {
  const nc = await conectarCrudo({ url: cfg.url, caFile: cfg.caFile, servicio: "facturacion-api", semilla: cfg.nkey });
  const enc = new TextEncoder();
  const dec = new TextDecoder();
  const cuerpo = enc.encode(JSON.stringify({ clienteId: "cli-1", totalCents: 9990, moneda: "EUR" }));
  const hacer = async () => {
    const r = await nc.request(SUBJECT, cuerpo, { timeout: 10_000 });
    if (JSON.parse(dec.decode(r.data)).descuentoCents !== 499) throw new Error("respuesta inesperada");
  };
  await hacer();
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
      await nc.drain();
    },
  };
}
