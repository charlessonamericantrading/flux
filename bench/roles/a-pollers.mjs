/**
 * ESCENARIO A, línea base — N sondeadores HTTP: cada uno es UN servicio, con su PROPIA conexión (un `Agent` por
 * sondeador, con keep-alive y un solo socket: así hay N conexiones, no un pool compartido que escondería el
 * coste de mantener N) que pregunta cada `periodoMs`.
 *
 *   modo "ingenuo"   GET sin condicional: siempre 200 con el cuerpo entero.
 *   modo "etag"      GET con `If-None-Match`: 304 sin cuerpo mientras no cambie. Es la línea base JUSTA.
 *
 * Cada sondeador tiene una FASE aleatoria dentro del periodo (una flota real no está sincronizada) y sondea a
 * ritmo fijo (`inicio + k·T`, no «T tras la respuesta»). Registra cuándo vio cada versión por primera vez.
 */

import { ahoraMs, dormir } from "../lib/proc.mjs";
import { agente, cliente, peticion, prng } from "../lib/comun.mjs";

export async function crear(cfg) {
  const modulo = cliente(cfg.tls);
  const rnd = prng(cfg.semilla);
  const c = { emitidas: 0, r200: 0, r304: 0, otras: 0, errores: 0, omitidas: 0 };

  // `sabotaje` solo lo usa bench/autoprueba.mjs, para demostrar que las comprobaciones DETECTAN un banco estropeado.
  const sinKeepAlive = cfg.sabotaje === "sin-keepalive";
  const sondeadores = Array.from({ length: cfg.n }, (_, i) => ({
    i,
    agente: agente(cfg.tls, sinKeepAlive ? { keepAlive: false, maxSockets: Infinity } : undefined),
    version: 1,
    etag: '"v1"',
    visto: [], // [version, t]
    enVuelo: false,
  }));

  const sondear = async (s) => {
    if (s.enVuelo) {
      c.omitidas++;
      return;
    }
    s.enVuelo = true;
    c.emitidas++;
    try {
      const r = await peticion(modulo, {
        host: cfg.host,
        port: cfg.puerto,
        path: "/clientes/cli-1",
        method: "GET",
        agent: s.agente,
        headers: { Accept: "application/json", ...(cfg.modo === "etag" && { "If-None-Match": s.etag }), ...(sinKeepAlive && { Connection: "close" }) },
      });
      if (r.status === 304) c.r304++;
      else if (r.status === 200) {
        c.r200++;
        // Lo que haría cualquier consumidor: leer el cuerpo y ver si hay algo nuevo.
        const v = JSON.parse(r.cuerpo.toString("utf8")).aggregateVersion;
        if (v > s.version) {
          s.version = v;
          s.visto.push([v, r.fin]);
        }
        s.etag = r.cabeceras.etag;
      } else c.otras++;
    } catch {
      c.errores++;
    } finally {
      s.enVuelo = false;
    }
  };

  // Calibración: una petición por sondeador, para que las N conexiones EXISTAN antes de medir. No se cuenta
  // como sondeo de la ventana (el runner mide por diferencia de contadores) ni como observación.
  for (let i = 0; i < sondeadores.length; i += 50) {
    await Promise.all(sondeadores.slice(i, i + 50).map((s) => sondear(s)));
  }
  for (const s of sondeadores) s.visto.length = 0;

  let activo = false;
  const timers = [];
  return {
    info: () => ({ pid: process.pid }),
    arranca() {
      activo = true;
      if (cfg.sabotaje === "saturar") {
        // Un hilo de JS ocupado casi todo el tiempo: el banco debe AVISAR de que este proceso está saturado.
        setInterval(() => {
          const t = Date.now();
          while (Date.now() - t < 48);
        }, 50);
      }
      const t0 = ahoraMs();
      for (const s of sondeadores) {
        const fase = rnd() * cfg.periodoMs;
        let k = 0;
        const paso = () => {
          if (!activo) return;
          sondear(s);
          k++;
          const proximo = t0 + fase + k * cfg.periodoMs;
          timers[s.i] = setTimeout(paso, Math.max(0, proximo - ahoraMs()));
        };
        timers[s.i] = setTimeout(paso, fase);
      }
    },
    snap: () => ({ ...c }),
    async fin() {
      activo = false;
      for (const t of timers) clearTimeout(t);
      await dormir(200);
      return { visto: sondeadores.map((s) => s.visto) };
    },
    async cerrar() {
      for (const s of sondeadores) s.agente.destroy();
    },
  };
}
