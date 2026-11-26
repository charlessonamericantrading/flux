#!/usr/bin/env node
/**
 * EVIDENCIA 3, 4 y 5 del plan v1.1 — las tres decisiones de diseño, comprobadas contra
 * un nats-server real ANTES de escribir la spec.
 *
 *   3. ACL candidata: inbox privado + permisos `$JS.API` exactos.
 *   4. Estado replicado: stream con un valor por clave.
 *   5. Petición/respuesta por core NATS.
 *
 * Experimento ad-hoc. Cada línea de salida debe acabar convertida en un caso de
 * `conformance/cases/` con su `observed`. Se lanza con `ejecutar.sh`.
 */

import { createRequire } from "node:module";

const require = createRequire(new URL("../../../conformance/package.json", import.meta.url));
const { connect, headers } = require("@nats-io/transport-node");
const {
  jetstream,
  jetstreamManager,
  AckPolicy,
  DeliverPolicy,
} = require("@nats-io/jetstream");

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
const jsOps = jetstream(ops);
await jsmOps.streams.add({ name: "EVT_PEDIDOS", subjects: ["pedidos.>"] });
await jsmOps.streams.add({ name: "EVT_FACTURACION", subjects: ["facturacion.>"] });
await jsOps.publish("pedidos.pedido.v1.creado", enc.encode('{"pedidoId":"ped-1"}'));

const acks = [];
const subAck = ops.subscribe("$JS.ACK.>");
(async () => {
  for await (const m of subAck) acks.push(m.subject);
})();

// ─────────────────────────────────────────────────────────────────────────────
console.log("EVIDENCIA 3 — ACL candidata: inbox privado + permisos $JS.API exactos");

const DURABLE = "fx-fact__pedidos_pedido_v1_creado";
const fact = await conn("fx-fact", { inboxPrefix: "_INBOX.fx-fact" });
const jsmF = await intento("jetstreamManager()", () => jetstreamManager(fact, { timeout: 1500 }));

await intento("crear el consumidor con el filtro concedido", async () => {
  await jsmF.consumers.add("EVT_PEDIDOS", {
    durable_name: DURABLE,
    filter_subject: "pedidos.pedido.v1.creado",
    ack_policy: AckPolicy.Explicit,
  });
});
await intento("consumir + WIP + ack", async () => {
  const c = await jetstream(fact).consumers.get("EVT_PEDIDOS", DURABLE);
  const it = await c.consume({ max_messages: 1 });
  for await (const m of it) {
    m.working();
    m.ack();
    it.stop();
    return dec.decode(m.data);
  }
});
await esperar(300);
await intento("el ack quedó registrado", async () => {
  const i = await jsmOps.consumers.info("EVT_PEDIDOS", DURABLE);
  return `ack_floor=${i.ack_floor.stream_seq} pending=${i.num_ack_pending}`;
});
console.log("     formato del subject de ack:", acks[0]);

await intento("[debe fallar] mismo subject de API, pero filtro más amplio en el cuerpo", async () => {
  const r = await fact.request(
    `$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.${DURABLE}.pedidos.pedido.v1.creado`,
    enc.encode(
      JSON.stringify({
        stream_name: "EVT_PEDIDOS",
        config: { durable_name: DURABLE, filter_subject: "pedidos.>", ack_policy: "explicit" },
      }),
    ),
    { timeout: 1500 },
  );
  const j = JSON.parse(dec.decode(r.data));
  if (j.error) throw new Error(j.error.description);
  return `ACEPTADO con filtro ${j.config?.filter_subject}`;
});
await intento("[debe fallar] API antigua DURABLE.CREATE (el filtro va solo en el cuerpo)", async () => {
  await fact.request("$JS.API.CONSUMER.DURABLE.CREATE.EVT_PEDIDOS.otro", enc.encode("{}"), {
    timeout: 800,
  });
});
await intento("[debe fallar] leer el stream directamente (STREAM.MSG.GET)", async () => {
  await fact.request("$JS.API.STREAM.MSG.GET.EVT_PEDIDOS", enc.encode('{"seq":1}'), {
    timeout: 800,
  });
});

const chis = await conn("fx-chis", { inboxPrefix: "_INBOX.fx-chis" });
const violaciones = [];
(async () => {
  for await (const s of chis.status()) if (s.type === "error") violaciones.push(String(s.error?.message));
})();
chis.subscribe("_INBOX.>");
chis.subscribe("_INBOX.fx-fact.>");
await chis.flush();
await esperar(300);
console.log(
  "  fx-chis espiando inbox:",
  violaciones.length === 2 ? "DENEGADO (2 violaciones)" : `¡¡PERMITIDO!! ${JSON.stringify(violaciones)}`,
);
await intento("[debe fallar] fx-chis pide mensajes del consumidor de fx-fact", async () => {
  await chis.request(`$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.${DURABLE}`, enc.encode('{"batch":1}'), {
    timeout: 800,
  });
});

// ─────────────────────────────────────────────────────────────────────────────
console.log("\nEVIDENCIA 4 — ESTADO: stream con un valor por clave");

await jsmOps.streams.add({
  name: "STATE_CLIENTES",
  subjects: ["state.clientes.>"],
  max_msgs_per_subject: 1,
  allow_direct: true,
});
const cli = await conn("fx-clientes", { inboxPrefix: "_INBOX.fx-clientes" });
const jsC = jetstream(cli);
const subjectDe = (clave) => `state.clientes.cliente.v1.acme.${clave}`;
const put = (clave, valor, secuenciaEsperada) => {
  const h = headers();
  if (secuenciaEsperada !== undefined) {
    h.set("Nats-Expected-Last-Subject-Sequence", String(secuenciaEsperada));
  }
  return jsC.publish(subjectDe(clave), enc.encode(JSON.stringify(valor)), { headers: h });
};

const r1 = await put("cli-1", { v: 1, nombre: "Ana" }, 0);
await put("cli-1", { v: 2, nombre: "Ana María" }, r1.seq);
await put("cli-2", { v: 1, nombre: "Luis" }, 0);

await intento("[debe fallar] escribir con una secuencia esperada vieja (CAS)", async () => {
  await put("cli-1", { v: 1, nombre: "VIEJO" }, r1.seq);
});
await intento("[debe fallar] crear una clave que ya existe (esperada = 0)", async () => {
  await put("cli-2", { v: 9 }, 0);
});
const info = await jsmOps.streams.info("STATE_CLIENTES");
console.log("     mensajes en el stream tras 3 escrituras sobre 2 claves:", info.state.messages);

await intento("lectura puntual al broker (direct get, last_by_subj)", async () => {
  const m = await jsmOps.direct.getMessage("STATE_CLIENTES", { last_by_subj: subjectDe("cli-1") });
  return dec.decode(m.data);
});
await intento("foto completa + cambios en vivo (deliver last_per_subject)", async () => {
  await jsmOps.consumers.add("STATE_CLIENTES", {
    durable_name: "web__state_clientes_cliente_v1",
    filter_subject: "state.clientes.cliente.v1.acme.*",
    deliver_policy: DeliverPolicy.LastPerSubject,
    ack_policy: AckPolicy.Explicit,
  });
  const c = await jsOps.consumers.get("STATE_CLIENTES", "web__state_clientes_cliente_v1");
  const recibido = [];
  const it = await c.consume();
  setTimeout(() => put("cli-3", { v: 1, nombre: "Eva" }, 0), 300);
  for await (const m of it) {
    recibido.push(`${m.subject.split(".").pop()}=${dec.decode(m.data)}`);
    m.ack();
    if (recibido.length === 3) it.stop();
  }
  return recibido.join(" | ");
});
await intento("borrado: la lápida sustituye al valor", async () => {
  await jsC.publish(subjectDe("cli-1"), enc.encode('{"deleted":true}'));
  const m = await jsmOps.direct.getMessage("STATE_CLIENTES", { last_by_subj: subjectDe("cli-1") });
  const i = await jsmOps.streams.info("STATE_CLIENTES");
  return `${dec.decode(m.data)} · mensajes=${i.state.messages}`;
});
await intento("[debe fallar] fx-fact escribe estado de un dominio ajeno", async () => {
  await jetstream(fact, { timeout: 800 }).publish(subjectDe("cli-9"), enc.encode("{}"));
});

// El tenant va en el subject para que sea EL BROKER quien confine, no el SDK.
await jsOps.publish("state.clientes.cliente.v1.globex.cli-2", enc.encode('{"de":"globex"}'));
const web = await conn("fx-web", { inboxPrefix: "_INBOX.fx-web" });
const jsmW = await jetstreamManager(web, { timeout: 1500 });
const DURABLE_WEB = "fx-web__state_clientes_cliente_v1__acme";
const consumidorDeEstado = (filtro) =>
  jsmW.consumers.add("STATE_CLIENTES", {
    durable_name: DURABLE_WEB,
    filter_subject: filtro,
    deliver_policy: DeliverPolicy.LastPerSubject,
    ack_policy: AckPolicy.Explicit,
  });
const directGet = async (subject) => {
  const r = await web.request(`$JS.API.DIRECT.GET.STATE_CLIENTES.${subject}`, new Uint8Array(), {
    timeout: 1000,
  });
  return r.data.length ? dec.decode(r.data) : `${r.headers?.code} ${r.headers?.description}`;
};

await intento("observar el estado de SU tenant (comodín de clave en la ACL)", async () => {
  await consumidorDeEstado("state.clientes.cliente.v1.acme.*");
});
await intento("[debe fallar] observar el estado de TODOS los tenants", async () => {
  await consumidorDeEstado("state.clientes.cliente.v1.*.*");
});
await intento("lectura puntual con la librería bajo ACL por subject", async () => {
  const m = await jsmW.direct.getMessage("STATE_CLIENTES", { last_by_subj: subjectDe("cli-2") });
  return dec.decode(m.data);
});
await intento("[debe fallar] lectura puntual de una clave de otro tenant", async () => {
  return `LEÍDO: ${await directGet("state.clientes.cliente.v1.globex.cli-2")}`;
});
await intento("lectura puntual de una clave que no existe", () =>
  directGet("state.clientes.cliente.v1.acme.no-existe"),
);
await web.close();

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
