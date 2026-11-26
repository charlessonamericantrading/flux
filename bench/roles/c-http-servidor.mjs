/**
 * ESCENARIO C, línea base — el SERVIDOR HTTP de la llamada: `POST /v1/clientes/calcular-descuento` con un JSON y su respuesta
 * en JSON. Hace lo mismo que el handler de flux: lee la petición, calcula y contesta. Cuenta peticiones, conexiones TCP
 * aceptadas (la prueba de que el keep-alive es de verdad) y bytes.
 */

import { contarConexiones, crearServidor, escuchar } from "../lib/comun.mjs";

export async function crear(cfg) {
  const c = { peticiones: 0, invalidas: 0 };
  const servidor = crearServidor(cfg.tls, (req, res) => {
    const partes = [];
    req.on("data", (p) => partes.push(p));
    req.once("end", () => {
      c.peticiones++;
      try {
        const p = JSON.parse(Buffer.concat(partes).toString("utf8"));
        const cuerpo = JSON.stringify({ descuentoCents: Math.floor(p.totalCents * 0.05), moneda: p.moneda });
        res.writeHead(200, { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(cuerpo) });
        res.end(cuerpo);
      } catch {
        c.invalidas++;
        res.writeHead(400, { "Content-Length": 0 }).end();
      }
    });
  }, { keepAliveS: cfg.keepAliveS });
  const leer = contarConexiones(servidor, cfg.tls ? "secureConnection" : "connection");
  await escuchar(servidor, "127.0.0.1", cfg.puerto);
  return {
    info: () => ({ pid: process.pid }),
    arranca() {},
    snap: () => ({ ...c, ...leer() }),
    async fin() {
      return {};
    },
    async cerrar() {
      servidor.closeAllConnections?.();
      servidor.close();
    },
  };
}
