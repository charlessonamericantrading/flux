#!/usr/bin/env node
/**
 * DIAGNÓSTICO (no forma parte de la malla): ¿cuánto del coste de `nats-server` en el escenario B (1 evento, N consumidores durables)
 * es **persistir el estado de cada consumidor**?
 *
 *   node bench/diagnostico/consumidores-en-memoria.mjs [b|a] [N=100] [hechos o cambios por minuto=600] [repeticiones=3]
 *
 *   b (por defecto)   escenario B: 1 evento y N consumidores de `EVT_PEDIDOS`
 *   a                 escenario A: N observadores de estado (`state.watch`) sobre `STATE_CLIENTES`
 *
 * Contexto: un perfil de CPU de `nats-server` (`prof_port` + `go tool pprof`) durante el escenario B con N = 100 puso ~2/3 de su CPU
 * en `consumerFileStore.flushLoop → writeState → writeAtomically` (abrir un fichero temporal, escribir y renombrarlo, por consumidor),
 * y no en la red. Este script lo comprueba al revés: mide el MISMO escenario con los durables que crea `flux provision` (estado de
 * consumidor en disco, lo que hace el SDK hoy) y con los mismos durables recreados con `mem_storage: true` (estado en memoria), y compara
 * la CPU de `nats-server` por hecho. Las repeticiones se intercalan.
 *
 * `mem_storage` pierde el estado del consumidor si el servidor reinicia (reentregaría desde el principio del stream): este script
 * NO propone cambiarlo, mide cuánto cuesta no hacerlo. Ver la fila A-173 del Apéndice A del plan.
 */

import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import path from "node:path";
import { readFile } from "node:fs/promises";

import { RAIZ, crearBroker } from "../lib/entorno.mjs";
import { cargarNats } from "../lib/flux.mjs";
import { fijarReintentos } from "../lib/corrida.mjs";
import { mediana } from "../lib/estadistica.mjs";
import { corridaA, manifiestoA } from "../escenarios/a-enterarse.mjs";
import { corridaB, manifiestoB } from "../escenarios/b-avisar.mjs";

const esc = (process.argv[2] ?? "b").toLowerCase();
const n = Number(process.argv[3] ?? 100);
const h = Number(process.argv[4] ?? 600);
const reps = Number(process.argv[5] ?? 3);
const STREAM = esc === "a" ? "STATE_CLIENTES" : "EVT_PEDIDOS";
fijarReintentos(0);

const req = createRequire(path.join(RAIZ, "sdk-node", "package.json"));
const js = await import(pathToFileURL(req.resolve("@nats-io/jetstream")).href);
const { connect, credsAuthenticator } = await cargarNats();

/** Recrea todos los durables del stream con `mem_storage: true` (el stream está vacío: no se reentrega nada). */
async function pasarAMemoria(broker) {
  const nc = await connect({
    servers: broker.url,
    authenticator: credsAuthenticator(new TextEncoder().encode(await readFile(path.join(broker.dir, "ops.creds"), "utf8"))),
  });
  const jsm = await js.jetstreamManager(nc);
  const cs = [];
  for await (const c of jsm.consumers.list(STREAM)) cs.push(c.config);
  for (const cfg of cs) {
    await jsm.consumers.delete(STREAM, cfg.durable_name);
    await jsm.consumers.add(STREAM, { ...cfg, mem_storage: true });
  }
  await nc.close();
  return cs.length;
}

const resultados = { disco: [], memoria: [] };
for (const variante of ["disco", "memoria"]) {
  const broker = await crearBroker({ manifiesto: esc === "a" ? manifiestoA(n) : manifiestoB(n), nombre: `diag-${variante}` });
  try {
    if (variante === "memoria") console.log(`  · ${await pasarAMemoria(broker)} durables recreados con mem_storage: true`);
    for (let rep = 1; rep <= reps; rep++) {
      const { res } =
        esc === "a"
          ? await corridaA({ metodo: "flux", n, cambiosPorMin: h, periodoS: 5, ventanaS: 15, calentamientoS: 3, rep, broker })
          : await corridaB({ metodo: "flux", n, hechosPorMin: h, ventanaS: 10, calentamientoS: 3, rep, broker });
      const hechos = esc === "a" ? res.cambiosEnVentana : res.hechosEnVentana;
      const porHecho = (res.cpu.nats * 1000) / hechos;
      resultados[variante].push({ natsMsPorHecho: porHecho, totalMsPorHecho: ((res.cpu.nats + res.cpu.servidor + res.cpu.clientes) * 1000) / hechos, perdidas: res.perdidas ?? res.sinVer, p50: res.latencia?.p50 ?? null, avisos: res.avisos });
      console.log(`  ${variante} rep ${rep}: nats-server ${porHecho.toFixed(1)} ms/hecho · perdidas ${res.perdidas ?? res.sinVer} · avisos ${res.avisos.length}`);
    }
  } finally {
    await broker.parar();
  }
}
const m = (v, k) => mediana(resultados[v].map((r) => r[k]));
console.log(`\nEscenario ${esc.toUpperCase()}: N = ${n}, ${h} hechos o cambios/min, ${reps} repeticiones (mediana):`);
console.log(`  nats-server  disco ${m("disco", "natsMsPorHecho").toFixed(1)} ms/hecho   memoria ${m("memoria", "natsMsPorHecho").toFixed(1)} ms/hecho   → ${(m("memoria", "natsMsPorHecho") / m("disco", "natsMsPorHecho")).toFixed(2)}×`);
console.log(`  total        disco ${m("disco", "totalMsPorHecho").toFixed(1)} ms/hecho   memoria ${m("memoria", "totalMsPorHecho").toFixed(1)} ms/hecho   → ${(m("memoria", "totalMsPorHecho") / m("disco", "totalMsPorHecho")).toFixed(2)}×`);
