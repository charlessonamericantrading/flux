/**
 * ESCENARIO C — Llamada con respuesta.
 *
 *   Un servicio pide un cálculo a otro y espera la respuesta (`calcular-descuento`: ~60 B de petición, ~35 B de respuesta).
 *
 *   línea base HTTP   `node:http` con keep-alive (la JUSTA) y sin él (una conexión por petición, lo habitual si no se configura).
 *   flux              `bus.request` / `bus.respond` por core NATS, con los permisos de gen-acl y el inbox privado.
 *
 * Bucle CERRADO a concurrencia 1, 16 y 256: latencia p50/p99/máx y peticiones por segundo.
 */

import { fileURLToPath } from "node:url";

import { correr, conReintentos, metaMedida } from "../lib/corrida.mjs";
import { puertoLibre, liberarPuerto } from "../lib/entorno.mjs";
import { suma } from "../lib/estadistica.mjs";

export const OPERACION = "clientes.cliente.v1.calcular-descuento";
const OTEL_STUB = fileURLToPath(new URL("../lib/otel-stub-hook.mjs", import.meta.url));

export function manifiestoC() {
  return {
    environment: "bench",
    services: [
      { name: "crm-api", owns: ["clientes"], serves: [OPERACION] },
      { name: "facturacion-api", owns: ["facturacion"], calls: [OPERACION] },
    ],
  };
}

/** `metodo`: "http-ka" (keep-alive) | "http-cerrar" (sin keep-alive) | "flux" | "nats-crudo" (core NATS sin el SDK). */
export async function corridaC({ metodo, concurrencia, ventanaS, calentamientoS, rep, tls, broker, certs, sabotaje }) {
  const esFlux = metodo === "flux" || metodo === "flux-otel" || metodo === "nats-crudo";
  // «flux-otel»: el mismo SDK, con `@opentelemetry/api` resolviendo (ver lib/otel-stub-hook.mjs y la fila A-170 del Apéndice A).
  const execArgv = metodo === "flux-otel" ? ["--import", OTEL_STUB] : undefined;
  const puerto = esFlux ? undefined : await puertoLibre();
  try {
    const rolNats = metodo === "nats-crudo" ? "c-nats" : "c-flux";
    const procesos = esFlux
      ? [
          { rol: `${rolNats}-servidor`, etiqueta: "servidor", grupo: "servidor", orden: 0, execArgv, cfg: { url: broker.url, caFile: broker.caFile, nkey: broker.semilla("crm-api") } },
          { rol: `${rolNats}-cliente`, etiqueta: "cliente", grupo: "clientes", orden: 1, execArgv, cfg: { url: broker.url, caFile: broker.caFile, nkey: broker.semilla("facturacion-api"), concurrencia } },
        ]
      : [
          { rol: "c-http-servidor", etiqueta: "servidor", grupo: "servidor", orden: 0, cfg: { puerto, tls: tls && certs, keepAliveS: 120 } },
          { rol: "c-http-cliente", etiqueta: "cliente", grupo: "clientes", orden: 1,
            cfg: { host: "127.0.0.1", puerto, concurrencia, keepAlive: metodo === "http-ka", tls: tls && certs, sabotaje } },
        ];
    const { medida, registros } = await correr({ procesos, broker: esFlux ? broker : undefined, calentamientoS, ventanaS, drenajeS: 0.5 });

    // Las peticiones que SALIERON dentro de la ventana medida.
    const { inicios, duraciones } = registros.cliente;
    const lat = [];
    for (let i = 0; i < inicios.length; i++) if (inicios[i] >= medida.t0 && inicios[i] <= medida.t1) lat.push(duraciones[i]);

    const cli = medida.snaps.cliente;
    const srv = medida.snaps.servidor;
    const res = {
      metodo, concurrencia, rep, tls: !!tls,
      durS: medida.durS,
      peticiones: cli.hechas,
      errores: cli.errores,
      porSegundo: cli.hechas / medida.durS,
      lat,
      cpu: { servidor: medida.cpu.servidor, clientes: medida.cpu.cliente, nats: esFlux ? medida.cpu["nats-server"] : 0 },
      sistema: { servidor: medida.sistema.servidor, clientes: medida.sistema.cliente, nats: esFlux ? medida.sistema["nats-server"] : NaN },
      nucleos: { servidor: medida.nucleos.servidor, clientes: medida.nucleos.cliente, nats: esFlux ? medida.nucleos["nats-server"] : 0 },
      memoriaMb: { servidor: medida.mem.servidor.picoMb, clientes: medida.mem.cliente.picoMb, nats: esFlux ? medida.mem["nats-server"].picoMb : 0 },
      ...metaMedida(medida),
    };
    if (esFlux) {
      res.nats = medida.nats;
      res.atendidas = srv.atendidas;
    } else {
      res.http = { atendidas: srv.peticiones, invalidas: srv.invalidas, bytes: srv.bytes, conexiones: srv.conexiones, abiertas: srv.g_abiertas };
    }
    void suma;
    return { medida, res };
  } finally {
    if (puerto) liberarPuerto(puerto);
  }
}

// En C el bucle cerrado satura por diseño al proceso más lento (un hilo de JS por lado): no se repite por «saturado».
export const puntoC = (args) =>
  conReintentos(() => corridaC(args), { etiqueta: `C ${args.metodo} c=${args.concurrencia}`, sinSaturacion: true });
