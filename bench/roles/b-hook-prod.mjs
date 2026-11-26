/**
 * ESCENARIO B, línea base — el PRODUCTOR de webhooks: por cada hecho hace UN `POST` a CADA receptor, con keep-alive (una
 * conexión por receptor) y reintentos mínimos: hasta 3 intentos ante error de red, timeout o `5xx`/`429` (espera 200 ms,
 * 400 ms), nunca ante otro `4xx`. Sin cola ni persistencia: si el proceso muere, lo no enviado se pierde (es lo que
 * hace un webhook «mínimo»; una cola con reintentos largos costaría más y es justo lo que flux ya trae).
 *
 * Los envía todos a la vez (`Promise.all`), que es lo más rápido posible para el productor; un envío secuencial
 * tardaría más en avisar al último.
 */

import { ahoraMs, dormir } from "../lib/proc.mjs";
import { agente, cliente, peticion, programar } from "../lib/comun.mjs";
import { datosPedido } from "../lib/pedido.mjs";
import { SUBJECT_PEDIDO } from "../lib/pedido.mjs";
import { randomUUID } from "node:crypto";

export async function crear(cfg) {
  const modulo = cliente(cfg.tls);
  const ag = agente(cfg.tls); // un Agent: la conexión se reutiliza por destino (host:puerto), una por receptor
  const c = { hechos: 0, intentos: 0, reintentos: 0, fallidos: 0 };
  const enviados = []; // [[seq, t]]
  let seq = 0;

  const enviar = async (dest, cuerpo, id) => {
    for (let intento = 1; intento <= 3; intento++) {
      c.intentos++;
      if (intento > 1) c.reintentos++;
      try {
        const r = await peticion(modulo, {
          host: dest.host, port: cfg.puerto, path: "/hooks/pedidos", method: "POST", agent: ag,
          ...(cfg.tls && { servername: "localhost" }), // el certificado es de `localhost`; los destinos son 127.x.y.z
          timeout: 5000,
          headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(cuerpo), "Idempotency-Key": id },
        }, cuerpo);
        if (r.status < 300) return true;
        if (r.status >= 400 && r.status < 500 && r.status !== 429) break;
      } catch {
        /* error de red o timeout: se reintenta */
      }
      if (intento < 3) await dormir(200 * intento);
    }
    c.fallidos++;
    return false;
  };

  let parar;
  return {
    info: () => ({ pid: process.pid }),
    arranca() {
      parar = programar(cfg.hechosPorMin, cfg.semilla, async () => {
        const n = cfg.base + ++seq;
        const id = randomUUID();
        const cuerpo = JSON.stringify({ id, type: SUBJECT_PEDIDO, time: new Date().toISOString(), data: datosPedido(n) });
        enviados.push([n, ahoraMs()]);
        c.hechos++;
        await Promise.all(cfg.destinos.map((d) => enviar(d, cuerpo, id)));
      });
    },
    snap: () => ({ ...c }),
    async fin() {
      parar?.();
      return { enviados };
    },
    async cerrar() {
      ag.destroy();
    },
  };
}
