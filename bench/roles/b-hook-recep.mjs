/**
 * ESCENARIO B, línea base — N RECEPTORES de webhook: cada uno es un servidor HTTP propio (su socket de escucha, su
 * dirección `127.x.y.z`: N destinos distintos y N conexiones, como N servicios en N hosts). Cada POST: se lee el
 * cuerpo, se parsea y se anota qué hecho es y cuándo llegó; se responde `200`.
 */

import { ahoraMs } from "../lib/proc.mjs";
import { contarConexiones, crearServidor, escuchar } from "../lib/comun.mjs";
import { seqDe } from "../lib/pedido.mjs";

/** La dirección del receptor nº `i` (1…): `127.0.0.1`, `127.0.0.2`… */
export const ipDe = (i) => `127.${(i >> 16) & 255}.${(i >> 8) & 255}.${i & 255}`;

export async function crear(cfg) {
  const c = { peticiones: 0, invalidas: 0, duplicados: 0, ocupados: 0 };
  const servidores = [];
  const lectores = [];
  const recibidos = []; // por receptor: [[seq, t], …]

  for (let i = cfg.desde; i <= cfg.hasta; i++) {
    const log = [];
    const vistos = new Set();
    recibidos.push(log);
    const s = crearServidor(cfg.tls, (req, res) => {
      const partes = [];
      req.on("data", (p) => partes.push(p));
      req.once("end", () => {
        const t = ahoraMs();
        c.peticiones++;
        if (cfg.sabotaje === "falla-una-de-tres" && c.peticiones % 3 === 0) {
          res.writeHead(503, { "Content-Length": 0 }).end(); // autoprueba: un receptor que falla de vez en cuando
          return;
        }
        try {
          const cuerpo = JSON.parse(Buffer.concat(partes).toString("utf8"));
          const seq = seqDe(cuerpo.data.pedidoId);
          if (vistos.has(seq)) c.duplicados++;
          else {
            vistos.add(seq);
            log.push([seq, t]);
          }
          res.writeHead(200, { "Content-Length": 0 }).end();
        } catch {
          c.invalidas++;
          res.writeHead(400, { "Content-Length": 0 }).end();
        }
      });
    }, { keepAliveS: cfg.keepAliveS });
    lectores.push(contarConexiones(s, cfg.tls ? "secureConnection" : "connection"));
    await escuchar(s, ipDe(i), cfg.puerto);
    servidores.push(s);
  }

  return {
    info: () => ({ pid: process.pid, receptores: servidores.length }),
    arranca() {},
    snap() {
      const a = { ...c, bytes: 0, conexiones: 0, g_abiertas: 0 };
      for (const l of lectores) {
        const x = l();
        a.bytes += x.bytes;
        a.conexiones += x.conexiones;
        a.g_abiertas += x.g_abiertas;
      }
      return a;
    },
    async fin() {
      return { recibidos };
    },
    async cerrar() {
      for (const s of servidores) {
        s.closeAllConnections?.();
        s.close();
      }
    },
  };
}
