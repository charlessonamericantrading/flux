#!/usr/bin/env node
/**
 * EVIDENCIA 1 y 2 del plan v1.1 — la ACL que hoy genera `scripts/gen-acl.mjs`, aplicada
 * a un nats-server real.
 *
 *   1. Un servicio NO puede consumir lo que tiene concedido: la ACL no incluye ningún
 *      permiso sobre `$JS.API.*`, y JetStream se consume por ahí.
 *   2. Un servicio SÍ puede leer un dominio que NO tiene concedido: `_INBOX.>` es común
 *      a todos, y las entregas de un pull consumer viajan por el inbox.
 *
 * Experimento ad-hoc. La Fase 1 lo sustituye por casos de conformidad y borra esta
 * carpeta. Se lanza con `ejecutar.sh`, que prepara el servidor.
 */

import { createRequire } from "node:module";

// Las dependencias de NATS viven en conformance/: este script no añade ninguna.
const require = createRequire(new URL("../../../conformance/package.json", import.meta.url));
const { connect } = require("@nats-io/transport-node");
const { jetstream, jetstreamManager, AckPolicy } = require("@nats-io/jetstream");

const url = process.env.NATS_URL ?? "nats://127.0.0.1:4233";
const enc = new TextEncoder();
const dec = new TextDecoder();
const conn = (user) => connect({ servers: url, user, pass: "pw", timeout: 3000 });

async function intento(etiqueta, fn) {
  try {
    const r = await fn();
    console.log("  OK   ", etiqueta, typeof r === "string" ? r : "");
  } catch (e) {
    console.log("  FALLA", etiqueta, "→", (e?.message ?? String(e)).slice(0, 120));
  }
}

// ── Preparación con un usuario sin restricciones ──────────────────────────────
const ops = await conn("ops");
const jsmOps = await jetstreamManager(ops);
for (const [nombre, subjects] of [
  ["EVT_PEDIDOS", ["pedidos.>"]],
  ["EVT_INVENTARIO", ["inventario.>"]],
  ["EVT_FACTURACION", ["facturacion.>"]],
]) {
  await jsmOps.streams.add({ name: nombre, subjects });
}
const jsOps = jetstream(ops);
await jsOps.publish("pedidos.pedido.v1.creado", enc.encode('{"pedidoId":"ped-1"}'));
await jsOps.publish(
  "inventario.stock.v1.ajustado",
  enc.encode('{"sku":"SECRETO-DE-INVENTARIO","stock":7}'),
);

console.log(
  "\nEVIDENCIA 1 — facturacion-api, con la ACL generada, intenta consumir lo que SÍ tiene concedido",
);
const fact = await conn("facturacion-api");
await intento("jetstreamManager()", async () => {
  await jetstreamManager(fact, { timeout: 1500 });
});
const jsmF = await jetstreamManager(fact, { checkAPI: false, timeout: 1500 });
await intento("streams.info(EVT_PEDIDOS)", async () => {
  await jsmF.streams.info("EVT_PEDIDOS");
});
await intento("consumers.add(durable)", async () => {
  await jsmF.consumers.add("EVT_PEDIDOS", {
    durable_name: "facturacion-api__pedidos_pedido_v1_creado",
    filter_subject: "pedidos.pedido.v1.creado",
    ack_policy: AckPolicy.Explicit,
  });
});
await intento("publicar por JetStream en su propio dominio", async () => {
  const r = await jetstream(fact, { timeout: 1500 }).publish(
    "facturacion.factura.v1.emitida",
    enc.encode("{}"),
  );
  return `seq ${r.seq}`;
});

console.log(
  "\nEVIDENCIA 2 — facturacion-api NO tiene concedido `inventario`. ¿Lo lee espiando _INBOX.>?",
);
const visto = [];
const espia = fact.subscribe("_INBOX.>");
(async () => {
  for await (const m of espia) {
    const texto = dec.decode(m.data);
    if (texto.includes("SECRETO-DE-INVENTARIO")) visto.push(texto);
  }
})();
await fact.flush();

// Un consumidor legítimo lee inventario por un pull consumer normal.
await jsmOps.consumers.add("EVT_INVENTARIO", {
  durable_name: "pedidos-api__inventario_stock_v1_ajustado",
  filter_subject: "inventario.stock.v1.ajustado",
  ack_policy: AckPolicy.Explicit,
});
const consumidor = await jsOps.consumers.get(
  "EVT_INVENTARIO",
  "pedidos-api__inventario_stock_v1_ajustado",
);
const m = await consumidor.next({ expires: 2000 });
m?.ack();
await new Promise((r) => setTimeout(r, 500));

console.log(
  visto.length
    ? `  FUGA: facturacion-api leyó ${visto.length} mensaje(s) de inventario → ${visto[0]}`
    : "  sin fuga: no vio nada",
);

await fact.close();
await ops.close();
