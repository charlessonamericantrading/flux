/**
 * ESCENARIO B — Avisar a muchos.
 *
 *   1 productor, N receptores; un hecho (un pedido creado) cada 60/H segundos.
 *
 *   línea base HTTP   N webhooks por hecho: el productor hace un POST a CADA receptor (keep-alive, hasta 3 intentos).
 *   flux              1 evento publicado; N consumidores durables (un servicio cada uno), cada uno con su conexión.
 *
 * Lo que se mide: peticiones o mensajes, CPU de cada proceso, bytes y la latencia de cada entrega
 * (del instante en que el productor decide avisar al instante en que el receptor tiene el hecho).
 */

import { correr, conReintentos, metaMedida } from "../lib/corrida.mjs";
import { nombreServicio, puertoLibre, liberarPuerto } from "../lib/entorno.mjs";
import { mediana, suma } from "../lib/estadistica.mjs";
import { SUBJECT_PEDIDO } from "../lib/pedido.mjs";
import { ipDe } from "../roles/b-hook-recep.mjs";

const POR_PROCESO = 250;

export function manifiestoB(n) {
  return {
    environment: "bench",
    services: [
      { name: "pedidos-api", owns: ["pedidos"] },
      ...Array.from({ length: n }, (_, i) => ({ name: nombreServicio("rec", i + 1), consumes: [SUBJECT_PEDIDO] })),
    ],
  };
}

const reparto = (n) => {
  const k = Math.ceil(n / POR_PROCESO);
  return Array.from({ length: k }, (_, i) => [Math.floor((i * n) / k) + 1, Math.floor(((i + 1) * n) / k)]);
};

let contadorCorridas = 0;

/**
 * `metodo`: "webhook" | "flux". `hechosPorMin`: hechos por minuto (120 = 2 por segundo).
 */
export async function corridaB({ metodo, n, hechosPorMin, ventanaS, calentamientoS, rep, tls, broker, certs, sabotaje }) {
  const esFlux = metodo === "flux";
  const puerto = esFlux ? undefined : await puertoLibre();
  // Cada corrida numera sus hechos desde su propia base: lo que quedara en un durable de una corrida anterior no se confunde con éste.
  const base = (++contadorCorridas % 3000 + 1) * 1_000_000;
  try {
    const procesos = [];
    if (esFlux) {
      procesos.push({
        rol: "b-evento-prod", etiqueta: "productor", grupo: "servidor", orden: 1,
        cfg: { url: broker.url, caFile: broker.caFile, nkey: broker.semilla("pedidos-api"), hechosPorMin, semilla: rep, base },
      });
      for (const [desde, hasta] of reparto(n)) {
        procesos.push({
          rol: "b-evento-cons", etiqueta: `rec-${desde}-${hasta}`, grupo: "clientes", orden: 0, esperaMs: 300_000,
          cfg: {
            url: broker.url, caFile: broker.caFile, sabotaje,
            servicios: Array.from({ length: hasta - desde + 1 }, (_, i) => {
              const name = nombreServicio("rec", desde + i);
              return { name, nkey: broker.semilla(name) };
            }),
          },
        });
      }
    } else {
      for (const [desde, hasta] of reparto(n)) {
        procesos.push({
          rol: "b-hook-recep", etiqueta: `rec-${desde}-${hasta}`, grupo: "clientes", orden: 0,
          cfg: { desde, hasta, puerto, tls: tls && certs, keepAliveS: 120, sabotaje },
        });
      }
      procesos.push({
        rol: "b-hook-prod", etiqueta: "productor", grupo: "servidor", orden: 1,
        cfg: {
          destinos: Array.from({ length: n }, (_, i) => ({ host: ipDe(i + 1) })), puerto, hechosPorMin, semilla: rep, base,
          tls: tls && certs,
        },
      });
    }

    const { medida, registros } = await correr({ procesos, broker: esFlux ? broker : undefined, calentamientoS, ventanaS, drenajeS: 2 });

    // ── latencia y comprobación de entrega ──
    const enviados = registros.productor.enviados; // [[n, t]]
    const contados = enviados.filter(([, t]) => t >= medida.t0 && t <= medida.t1 - 1500);
    const envT = new Map(enviados);
    const muestras = [];
    let perdidas = 0;
    let ajenos = 0;
    const cl = Object.keys(registros).filter((k) => k.startsWith("rec-"));
    for (const k of cl) {
      const listas = registros[k].recibidos; // por receptor [[seq, t]]
      for (const log of listas) {
        const porSeq = new Map();
        for (const [seq, t] of log) {
          if (seq < base || seq >= base + 1_000_000) ajenos++;
          else porSeq.set(seq, t);
        }
        for (const [seq] of contados) {
          const t = porSeq.get(seq);
          if (t === undefined) perdidas++;
          else muestras.push(t - envT.get(seq));
        }
      }
    }

    const snaps = cl.map((k) => medida.snaps[k]);
    const prod = medida.snaps.productor;
    const res = {
      metodo, n, hechosPorMin, rep, tls: !!tls,
      durS: medida.durS,
      cpu: {
        servidor: medida.cpu.productor,
        clientes: suma(cl.map((k) => medida.cpu[k])),
        nats: esFlux ? medida.cpu["nats-server"] : 0,
      },
      sistema: {
        servidor: medida.sistema.productor,
        clientes: mediana(cl.map((k) => medida.sistema[k])),
        nats: esFlux ? medida.sistema["nats-server"] : NaN,
      },
      nucleos: {
        servidor: medida.nucleos.productor,
        clientesMax: Math.max(...cl.map((k) => medida.nucleos[k])),
        nats: esFlux ? medida.nucleos["nats-server"] : 0,
      },
      memoriaMb: {
        servidor: medida.mem.productor.picoMb,
        clientes: suma(cl.map((k) => medida.mem[k].picoMb)),
        nats: esFlux ? medida.mem["nats-server"].picoMb : 0,
      },
      hechosEnVentana: prod.hechos,
      hechosContados: contados.length,
      lat: muestras,
      perdidas,
      ajenos,
      ...metaMedida(medida),
    };
    if (esFlux) {
      res.nats = medida.nats;
      res.entregas = suma(snaps.map((s) => s.entregas));
      res.duplicados = suma(snaps.map((s) => s.duplicados));
      res.publicaciones = prod.hechos;
      res.erroresProductor = prod.errores;
    } else {
      res.http = {
        // peticiones que llegaron a los receptores (cada una = 1 POST) y lo que el productor tuvo que emitir (con reintentos)
        recibidas: suma(snaps.map((s) => s.peticiones)),
        intentos: prod.intentos, reintentos: prod.reintentos, fallidos: prod.fallidos,
        invalidas: suma(snaps.map((s) => s.invalidas)), duplicados: suma(snaps.map((s) => s.duplicados)),
        bytes: suma(snaps.map((s) => s.bytes)),
        conexiones: suma(snaps.map((s) => s.conexiones)),
        abiertas: suma(snaps.map((s) => s.g_abiertas)),
      };
    }
    return { medida, res };
  } finally {
    if (puerto) liberarPuerto(puerto);
  }
}

export const puntoB = (args) =>
  conReintentos(() => corridaB(args), { etiqueta: `B ${args.metodo} N=${args.n} H=${args.hechosPorMin}` });
