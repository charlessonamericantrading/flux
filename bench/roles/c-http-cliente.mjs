/**
 * ESCENARIO C, línea base — el CLIENTE HTTP: `node:http` con un `Agent`:
 *
 *   keepAlive true    las conexiones se reutilizan (a lo sumo `concurrencia`): la línea base JUSTA.
 *   keepAlive false   una conexión TCP por petición, con `Connection: close`: es lo que hace un cliente sin configurar. Se mide
 *                     porque es habitual, no porque sea justo.
 */

import { agente, cliente, peticion } from "../lib/comun.mjs";
import { cargaCerrada } from "../lib/carga.mjs";

export async function crear(cfg) {
  const modulo = cliente(cfg.tls);
  // `sabotaje: "sin-keepalive"` (solo la autoprueba): el cliente dice ser keep-alive y NO lo es.
  const keepAlive = cfg.keepAlive && cfg.sabotaje !== "sin-keepalive";
  const ag = agente(cfg.tls, { keepAlive, maxSockets: keepAlive ? cfg.concurrencia : Infinity });
  const cuerpo = JSON.stringify({ clienteId: "cli-1", totalCents: 9990, moneda: "EUR" });
  const hacer = async () => {
      const r = await peticion(modulo, {
        host: cfg.host, port: cfg.puerto, path: "/v1/clientes/calcular-descuento", method: "POST", agent: ag,
        headers: {
          "Content-Type": "application/json", "Content-Length": Buffer.byteLength(cuerpo),
          ...(!keepAlive && { Connection: "close" }),
        },
      }, cuerpo);
      if (r.status !== 200) throw new Error(`HTTP ${r.status}`);
      if (JSON.parse(r.cuerpo.toString("utf8")).descuentoCents !== 499) throw new Error("respuesta inesperada");
  };
  // Calibración: una petición para comprobar que el camino funciona (y abrir la primera conexión) antes de medir.
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
      ag.destroy();
    },
  };
}
