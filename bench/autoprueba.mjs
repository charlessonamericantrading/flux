#!/usr/bin/env node
/**
 * La AUTOPRUEBA del banco: estropea el banco a propósito y exige que sus comprobaciones lo DETECTEN.
 *
 *   node bench/autoprueba.mjs
 *
 * Una comprobación que solo se ha visto en verde no demuestra nada (un cliente «keep-alive» que en realidad abre una conexión por
 * petición daría números de HTTP-sin-keep-alive con la etiqueta de la línea base justa, y nadie lo notaría). Cada caso de aquí es un
 * SABOTAJE concreto de un rol del banco y la comprobación que DEBE saltar; y hay controles sin sabotaje que DEBEN pasar.
 * Sale con 1 si un sabotaje pasa desapercibido o si un control falla.
 */

import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { crearBroker } from "./lib/entorno.mjs";
import { comprobar } from "./lib/comprobar.mjs";
import { fijarReintentos } from "./lib/corrida.mjs";
import { crearPki } from "../scripts/lib/pki.mjs";
import { corridaA } from "./escenarios/a-enterarse.mjs";
import { corridaB, manifiestoB } from "./escenarios/b-avisar.mjs";
import { corridaC, manifiestoC } from "./escenarios/c-llamada.mjs";

fijarReintentos(0);
const base = { rep: 1, calentamientoS: 1 };
const A = { ...base, n: 5, cambiosPorMin: 60, periodoS: 1, ventanaS: 6 };
const B = { ...base, n: 5, hechosPorMin: 120, ventanaS: 5 };
const C = { ...base, concurrencia: 4, ventanaS: 2 };

/** Cada caso: qué se estropea, qué debe fallar (regex sobre el nombre de la comprobación; `null` = nada: es un control). */
const CASOS = [
  { nombre: "control A: sondeo con ETag sin sabotaje", esc: "A", correr: () => corridaA({ ...A, metodo: "etag" }), falla: null },
  { nombre: "A: sondeadores sin keep-alive (una conexión por sondeo)", esc: "A", correr: () => corridaA({ ...A, metodo: "etag", sabotaje: "sin-keepalive" }), falla: /keep-alive real|una conexión viva/ },
  { nombre: "A: el servidor ignora If-None-Match (el sondeo «con ETag» es ingenuo)", esc: "A", correr: () => corridaA({ ...A, metodo: "etag", sabotaje: "ignora-etag" }), falla: /304/ },
  { nombre: "A: un cliente saturado (hilo de JS al 96 %)", esc: "A", correr: () => corridaA({ ...A, metodo: "etag", sabotaje: "saturar" }), falla: /fiabilidad/ },
  { nombre: "control B: webhooks sin sabotaje", esc: "B", correr: () => corridaB({ ...B, metodo: "webhook" }), falla: null },
  { nombre: "B: un receptor que falla una de cada tres (reintentos)", esc: "B", correr: () => corridaB({ ...B, metodo: "webhook", sabotaje: "falla-una-de-tres" }), falla: /sin reintentos ni fallos|perdidos/ },
  { nombre: "B: un consumidor flux que se traga los eventos", esc: "B", correr: (b) => corridaB({ ...B, metodo: "flux", broker: b, sabotaje: "descarta" }), falla: /ninguno perdido/, broker: "B" },
  { nombre: "control C: HTTP keep-alive sin sabotaje", esc: "C", correr: () => corridaC({ ...C, metodo: "http-ka" }), falla: null },
  { nombre: "C: «keep-alive» que no lo es", esc: "C", correr: () => corridaC({ ...C, metodo: "http-ka", sabotaje: "sin-keepalive" }), falla: /keep-alive real/ },
];

let mal = 0;
const brokers = {};
try {
  for (const c of CASOS) {
    let broker;
    if (c.broker === "B") broker = brokers.B ??= await crearBroker({ manifiesto: manifiestoB(5), nombre: "auto-b" });
    let r;
    try {
      r = (await c.correr(broker)).res;
    } catch (e) {
      console.log(`✗ ${c.nombre}\n    el caso no pudo ejecutarse: ${e.message.split("\n")[0]}`);
      mal++;
      continue;
    }
    const cs = comprobar(c.esc, r);
    const falladas = cs.filter((x) => !x.ok);
    if (c.falla === null) {
      // El control: NINGUNA comprobación de lógica debe fallar (las de fiabilidad dependen de la carga de la máquina).
      const logicas = falladas.filter((x) => !/fiabilidad/.test(x.nombre));
      if (logicas.length) {
        console.log(`✗ ${c.nombre}\n    el control falló: ${logicas.map((x) => `${x.nombre} (${x.detalle})`).join("; ")}`);
        mal++;
      } else console.log(`✓ ${c.nombre}: todas las comprobaciones pasan`);
    } else {
      const detectadas = falladas.filter((x) => c.falla.test(x.nombre));
      if (detectadas.length) console.log(`✓ ${c.nombre}: DETECTADO → ${detectadas.map((x) => `«${x.nombre}» (${x.detalle})`).join("; ")}`);
      else {
        console.log(`✗ ${c.nombre}: NO se detectó (fallaron: ${falladas.map((x) => x.nombre).join("; ") || "ninguna"})`);
        mal++;
      }
    }
  }
} finally {
  await Promise.allSettled(Object.values(brokers).map((b) => b.parar()));
}
console.log(mal ? `\n✗ ${mal} caso(s) de la autoprueba mal` : `\n✓ la autoprueba pasa: cada sabotaje fue detectado y cada control pasó`);
process.exit(mal ? 1 : 0);
