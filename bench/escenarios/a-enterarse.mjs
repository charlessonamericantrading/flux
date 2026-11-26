/**
 * ESCENARIO A — Enterarse de un cambio.
 *
 *   N consumidores quieren saber, cuanto antes, que un dato que guarda otro sistema cambió; C cambios por minuto.
 *
 *   línea base HTTP   cada consumidor sondea `GET /clientes/cli-1` cada T segundos, con keep-alive:
 *                       · «ingenuo»  sin condicional (siempre 200 con el cuerpo)
 *                       · «etag»     con `If-None-Match` (304 sin cuerpo). El sondeo INTELIGENTE: la línea base justa.
 *   flux              cada consumidor es un servicio con su `bus.state.watch` (un durable de estado cada uno).
 *
 * Lo que se mide: peticiones o mensajes, CPU de cada proceso, bytes, y el TIEMPO HASTA ENTERARSE de cada cambio
 * (lo que el sondeo pierde: con periodo T, de media T/2).
 */

import { correr, conReintentos, metaMedida } from "../lib/corrida.mjs";
import { crearBroker, nombreServicio, puertoLibre, liberarPuerto } from "../lib/entorno.mjs";
import { latencias, mediana, suma } from "../lib/estadistica.mjs";

const CONTRATO = "clientes.cliente.v1";
const POR_PROCESO = 250; // consumidores por proceso: ningún proceso de Node debe ir saturado

export function manifiestoA(n) {
  return {
    environment: "bench",
    services: [
      { name: "crm-api", owns: ["clientes"] },
      ...Array.from({ length: n }, (_, i) => ({
        name: nombreServicio("obs", i + 1),
        watches: [{ contract: CONTRATO, tenants: ["acme"] }],
      })),
    ],
  };
}

/** Los consumidores repartidos en procesos de a lo sumo `POR_PROCESO`. */
const reparto = (n) => {
  const k = Math.ceil(n / POR_PROCESO);
  const trozos = [];
  for (let i = 0; i < k; i++) {
    const desde = Math.floor((i * n) / k) + 1;
    const hasta = Math.floor(((i + 1) * n) / k);
    trozos.push([desde, hasta]);
  }
  return trozos;
};

/** Cruza lo que el productor cambió con lo que cada observador vio y calcula el tiempo hasta enterarse. */
function analizar({ cambios, visto, t0, t1, holguraMs }) {
  const validos = cambios.filter((c) => c.t >= t0 && c.t <= t1 - holguraMs);
  const muestras = [];
  let exactas = 0;
  let pares = 0;
  let sinVer = 0;
  for (const obs of visto) {
    // `visto` de un observador: [[versión, t], …] en el orden en que las vio.
    for (const c of validos) {
      pares++;
      const primera = obs.find(([v]) => v >= c.v);
      if (!primera) {
        sinVer++;
        continue;
      }
      muestras.push(primera[1] - c.t);
      if (primera[0] === c.v) exactas++;
    }
  }
  return {
    cambiosContados: validos.length,
    muestras,
    pares,
    sinVer,
    // Fracción de (observador, cambio) cuya versión EXACTA nunca se vio porque otra posterior la tapó antes del siguiente sondeo.
    coalescidas: pares ? (pares - sinVer - exactas) / pares : 0,
  };
}

/**
 * Una repetición de un punto. `metodo`: "ingenuo" | "etag" | "flux".
 * `broker` solo para flux (el mismo para todas las repeticiones de un N).
 */
export async function corridaA({ metodo, n, cambiosPorMin, periodoS, ventanaS, calentamientoS, rep, tls, broker, certs, sabotaje }) {
  const esFlux = metodo === "flux";
  const puerto = esFlux ? undefined : await puertoLibre();
  const clave = `cli-${rep}${Date.now().toString(36).slice(-4)}`;
  try {
    const procesos = [];
    if (esFlux) {
      procesos.push({
        rol: "a-crm-flux", etiqueta: "crm", grupo: "servidor", orden: 0,
        cfg: { url: broker.url, caFile: broker.caFile, nkey: broker.semilla("crm-api"), cambiosPorMin, semilla: rep, clave },
      });
      for (const [desde, hasta] of reparto(n)) {
        procesos.push({
          rol: "a-watchers", etiqueta: `obs-${desde}-${hasta}`, grupo: "clientes", orden: 1, esperaMs: 300_000,
          cfg: {
            url: broker.url, caFile: broker.caFile, clave,
            servicios: Array.from({ length: hasta - desde + 1 }, (_, i) => {
              const name = nombreServicio("obs", desde + i);
              return { name, nkey: broker.semilla(name) };
            }),
          },
        });
      }
    } else {
      procesos.push({
        rol: "a-crm-http", etiqueta: "crm", grupo: "servidor", orden: 0,
        cfg: { puerto, cambiosPorMin, semilla: rep, tls: tls && certs, keepAliveS: 120, sabotaje },
      });
      for (const [desde, hasta] of reparto(n)) {
        procesos.push({
          rol: "a-pollers", etiqueta: `obs-${desde}-${hasta}`, grupo: "clientes", orden: 1,
          cfg: {
            host: "127.0.0.1", puerto, n: hasta - desde + 1, periodoMs: periodoS * 1000, modo: metodo,
            semilla: rep * 1000 + desde, tls: tls && certs, sabotaje,
          },
        });
      }
    }

    const r = await correr({ procesos, broker: esFlux ? broker : undefined, calentamientoS, ventanaS, drenajeS: 1 });
    const { medida, registros } = r;

    const cambios = registros.crm.cambios;
    const visto = Object.entries(registros).filter(([k]) => k.startsWith("obs-")).flatMap(([, v]) => v.visto);
    // Los cambios que CUENTAN para el tiempo hasta enterarse son los de [t0, t1 − holgura]: los posteriores todavía
    // podrían estar sin ver porque aún no tocaba sondear (un periodo entero, con sondeo). La CPU y los bytes, en cambio,
    // se miden en toda la ventana.
    const holguraMs = esFlux ? 1500 : periodoS * 1.05 * 1000 + 500;
    const lat = analizar({ cambios, visto, t0: medida.t0, t1: medida.t1, holguraMs });

    const cl = Object.keys(medida.cpu).filter((k) => k.startsWith("obs-"));
    const snapsCl = cl.map((k) => medida.snaps[k]);
    const crmSnap = medida.snaps.crm;
    const res = {
      metodo, n, cambiosPorMin, periodoS, rep, tls: !!tls,
      durS: medida.durS,
      cpu: {
        servidor: medida.cpu.crm,
        clientes: suma(cl.map((k) => medida.cpu[k])),
        nats: esFlux ? medida.cpu["nats-server"] : 0,
      },
      sistema: {
        servidor: medida.sistema.crm,
        clientes: mediana(cl.map((k) => medida.sistema[k])),
        nats: esFlux ? medida.sistema["nats-server"] : NaN,
      },
      nucleosMax: Math.max(...Object.entries(medida.nucleos).filter(([k]) => k !== "nats-server").map(([, v]) => v)),
      natsNucleos: esFlux ? medida.nucleos["nats-server"] : 0,
      memoriaMb: {
        servidor: medida.mem.crm.picoMb,
        clientes: suma(cl.map((k) => medida.mem[k].picoMb)),
        nats: esFlux ? medida.mem["nats-server"].picoMb : 0,
      },
      cambiosEnVentana: crmSnap.cambios,
      cambiosContados: lat.cambiosContados,
      lat: lat.muestras,
      coalescidas: lat.coalescidas,
      sinVer: lat.sinVer,
      pares: lat.pares,
      ...metaMedida(medida),
    };
    if (esFlux) {
      res.nats = medida.nats;
      res.entregas = suma(snapsCl.map((s) => s.entregas));
      res.duplicados = suma(snapsCl.map((s) => s.duplicados));
      res.puts = crmSnap.puts;
      // Cada observador debe ver cada cambio contado EXACTAMENTE una vez: lo que falte o sobre es un hallazgo.
      res.entregasEsperadas = crmSnap.cambios * n;
    } else {
      res.http = {
        peticiones: crmSnap.peticiones, r200: crmSnap.r200, r304: crmSnap.r304, otras: crmSnap.otras,
        bytes: crmSnap.bytes, conexiones: crmSnap.conexiones, abiertas: crmSnap.g_abiertas,
        emitidas: suma(snapsCl.map((s) => s.emitidas)), errores: suma(snapsCl.map((s) => s.errores)), omitidas: suma(snapsCl.map((s) => s.omitidas)),
      };
    }
    return { medida, res };
  } finally {
    if (puerto) liberarPuerto(puerto);
  }
}

export async function puntoA(args) {
  return conReintentos(() => corridaA(args), { etiqueta: `A ${args.metodo} N=${args.n} C=${args.cambiosPorMin} T=${args.periodoS}` });
}

export { crearBroker, latencias };
