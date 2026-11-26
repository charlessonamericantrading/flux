/**
 * ESCENARIO A, línea base — el CRM: un servidor HTTP con UN recurso (`GET /clientes/cli-1`) que cambia `cambiosPorMin`
 * veces por minuto. Responde con `ETag` y atiende `If-None-Match` (304 sin cuerpo); lo que haga el cliente con ello es
 * cosa suya (sondeo ingenuo o con ETag).
 *
 * Registra el instante de cada cambio, que es lo que el runner cruza con lo que cada sondeador vio.
 */

import { ahoraMs } from "../lib/proc.mjs";
import { contarConexiones, crearServidor, escuchar, programar } from "../lib/comun.mjs";

const cuerpoDe = (v) =>
  JSON.stringify({ clienteId: "cli-1", aggregateVersion: v, nombre: "Ana García Ruiz", email: "ana.garcia@example.com", pais: "ES" });

export async function crear(cfg) {
  let version = 1;
  let cuerpo = cuerpoDe(version);
  const cambios = []; // [{ v, t }]
  const c = { peticiones: 0, r200: 0, r304: 0, otras: 0, cambios: 0 };

  const servidor = crearServidor(cfg.tls, (req, res) => {
    c.peticiones++;
    if (req.method !== "GET" || req.url !== "/clientes/cli-1") {
      c.otras++;
      res.writeHead(404).end();
      return;
    }
    const etag = `"v${version}"`;
    if (cfg.sabotaje !== "ignora-etag" && req.headers["if-none-match"] === etag) {
      c.r304++;
      res.writeHead(304, { ETag: etag }).end();
      return;
    }
    c.r200++;
    res.writeHead(200, { "Content-Type": "application/json", ETag: etag, "Content-Length": Buffer.byteLength(cuerpo) });
    res.end(cuerpo);
  }, { keepAliveS: cfg.keepAliveS });
  const leer = contarConexiones(servidor, cfg.tls ? "secureConnection" : "connection");
  await escuchar(servidor, "127.0.0.1", cfg.puerto);

  let parar;
  return {
    info: () => ({ pid: process.pid }),
    arranca() {
      parar = programar(cfg.cambiosPorMin, cfg.semilla, () => {
        version++;
        cuerpo = cuerpoDe(version);
        cambios.push({ v: version, t: ahoraMs() });
        c.cambios++;
      });
    },
    snap: () => ({ ...c, ...leer() }),
    async fin() {
      parar?.();
      return { cambios };
    },
    async cerrar() {
      servidor.closeAllConnections?.();
      servidor.close();
    },
  };
}
