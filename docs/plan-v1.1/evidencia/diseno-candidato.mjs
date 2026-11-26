#!/usr/bin/env node
/**
 * EVIDENCIA 5 del plan v1.1 — una decisión de diseño, comprobada contra un nats-server real
 * ANTES de escribir la spec.
 *
 *   5. Petición/respuesta por core NATS.
 *
 * La evidencia 3 (la ACL candidata: inbox privado + permisos `$JS.API` exactos) ya es la suite
 * de seguridad: `conformance/cases/security-acl.json`, que ejecuta `conformance/security.mjs`.
 * La evidencia 4 (estado replicado: A-9 … A-14, y lo que midió la fase 2) ya son los casos de
 * `conformance/cases/state-broker.json` y `state-semantics.json`, que ejecuta
 * `conformance/state-broker.mjs`.
 *
 * Experimento ad-hoc. Cada línea de salida debe acabar convertida en un caso de
 * `conformance/cases/` con su `observed` (Fase 3). Se lanza con `ejecutar.sh`.
 */

import { createRequire } from "node:module";

const require = createRequire(new URL("../../../conformance/package.json", import.meta.url));
const { connect } = require("@nats-io/transport-node");
const { jetstream, jetstreamManager } = require("@nats-io/jetstream");

const url = process.env.NATS_URL ?? "nats://127.0.0.1:4234";
const enc = new TextEncoder();
const dec = new TextDecoder();
const esperar = (ms) => new Promise((r) => setTimeout(r, ms));
const conn = (user, extra = {}) =>
  connect({ servers: url, user, pass: "pw", timeout: 3000, ...extra });

async function intento(etiqueta, fn) {
  try {
    const r = await fn();
    console.log("  OK   ", etiqueta, typeof r === "string" ? r : "");
    return r;
  } catch (e) {
    console.log("  FALLA", etiqueta, "→", (e?.message ?? String(e)).slice(0, 130));
  }
}

const ops = await conn("ops");
const jsmOps = await jetstreamManager(ops);

// Dos servicios del experimento (con su inbox privado, como en la ACL generada):
//   fx-fact    no es dueño de `clientes`: llama a su operación `obtener`
//   fx-chis    no tiene concedido nada ajeno: intenta llamar a una operación que no es suya
const fact = await conn("fx-fact", { inboxPrefix: "_INBOX.fx-fact" });
const chis = await conn("fx-chis", { inboxPrefix: "_INBOX.fx-chis" });

// El dueño de `clientes` (la conexión la usa la evidencia 5 para atender la operación).
const cli = await conn("fx-clientes", { inboxPrefix: "_INBOX.fx-clientes" });

// ─────────────────────────────────────────────────────────────────────────────
console.log("\nEVIDENCIA 5 — PETICIÓN/RESPUESTA por core NATS");

const OP = "rpc.clientes.cliente.v1.obtener";
await intento("[debe fallar rápido] petición sin nadie escuchando", async () => {
  const t = Date.now();
  try {
    await fact.request(OP, enc.encode("{}"), { timeout: 5000 });
  } catch (e) {
    throw new Error(`${e.message} (en ${Date.now() - t} ms; el timeout era 5000)`);
  }
});

const servidor = cli.subscribe("rpc.clientes.>", { queue: "fx-clientes" });
(async () => {
  for await (const m of servidor) m.respond(enc.encode('{"clienteId":"cli-1","nombre":"Ana"}'));
})();
await cli.flush();

await intento("petición atendida (allow_responses, sin permiso de publicación en _INBOX)", async () => {
  const t = process.hrtime.bigint();
  const r = await fact.request(OP, enc.encode('{"clienteId":"cli-1"}'), { timeout: 2000 });
  return `${dec.decode(r.data)} (${(Number(process.hrtime.bigint() - t) / 1e6).toFixed(2)} ms)`;
});
await intento("[debe fallar] fx-chis llama a una operación no concedida", async () => {
  await chis.request(OP, enc.encode("{}"), { timeout: 800 });
});

await jsmOps.streams.add({ name: "TRAMPA", subjects: ["rpc.trampa.>"] });
await intento("TRAMPA: petición a un subject que captura un stream", async () => {
  const r = await ops.request("rpc.trampa.x.v1.y", enc.encode("{}"), { timeout: 1500 });
  return `la «respuesta» es el acuse de JetStream → ${dec.decode(r.data)}`;
});

await fact.close();
await chis.close();
await cli.close();
await ops.close();
