/**
 * `publish(…, { externalKey, time })` contra un nats-server REAL — specification/01-envelope.md §2.6.1.
 *
 * Lo que importa: el mismo hecho publicado tres veces lleva el MISMO `id`; dentro de `duplicate_window` JetStream guarda UN
 * mensaje (A-157), y fuera guarda los dos y un consumidor idempotente por `id` aplica UN efecto. Sin `time`, falla con un error que
 * explica por qué.
 *
 * Sin `nats-server` estos tests se SALTAN con aviso, salvo con FLUX_REQUIRE_BROKER=1, donde FALLAN (broker.ts).
 */

import { test, before, after } from "node:test";
import assert from "node:assert/strict";

import { EnvelopeError, connect, externalEventId, type FluxBus } from "../../src/index.js";
import { conectarOps, describeBroker, dormir, esperarHasta, startBroker, type Broker } from "./broker.js";

const T = "2026-08-20T10:25:39.412Z";

describeBroker("SDK Node: publish con `externalKey` (id determinista)", () => {
  let broker: Broker;
  let bus: FluxBus;
  let ops: Awaited<ReturnType<typeof conectarOps>>;

  before(async () => {
    broker = await startBroker(
      ({ port, store }) => `listen: 127.0.0.1:${port}
jetstream { store_dir: "${store}" }
accounts { PROD: { jetstream: enabled, users: [ { user: "ops", password: "pw" }, { user: "pasarela-web", password: "pw" } ] } }
`,
    );
    ops = await conectarOps(broker);
    bus = await connect({
      servers: broker.url,
      service: "pasarela-web",
      environment: "produccion",
      version: "1.0.0",
      tenantId: "acme",
      schemaBaseUrl: "https://schemas.internal",
      credentials: { user: "pasarela-web", pass: "pw" },
    });
  });
  after(async () => {
    await bus?.close().catch(() => {});
    await ops?.nc.close().catch(() => {});
    await broker?.stop();
  });

  test("el id es el de la spec, y es el mismo en cada publicación", async () => {
    const esperado = externalEventId({ source: "/produccion/pasarela-web", externalKey: "pasarela:evt_1", time: T });
    for (let i = 0; i < 3; i++) {
      const ev = await bus.publish("pagos.cobro.v1.recibido", { n: i }, { externalKey: "pasarela:evt_1", time: T });
      assert.equal(ev.id, esperado);
      assert.equal(ev.time, T);
      assert.equal(ev.correlationid, esperado, "sin un evento en curso, correlationid = id");
    }
  });

  test("dentro de duplicate_window JetStream guarda UN mensaje (A-157)", async () => {
    const info = await ops.jsm.streams.info("EVT_PAGOS");
    assert.equal(info.state.messages, 1, "tres publicaciones del mismo hecho = un mensaje");
    assert.equal(info.config.duplicate_window / 1e9, 120);
  });

  test("fuera de la ventana se guardan los dos y un consumidor idempotente por id aplica UN efecto (A-157)", async () => {
    const cfg = (await ops.jsm.streams.info("EVT_PAGOS")).config;
    await ops.jsm.streams.update("EVT_PAGOS", { ...cfg, duplicate_window: 1e9 });
    await dormir(1500);
    await bus.publish("pagos.cobro.v1.recibido", { n: 3 }, { externalKey: "pasarela:evt_1", time: T });
    assert.equal((await ops.jsm.streams.info("EVT_PAGOS")).state.messages, 2, "pasada la ventana, JetStream ya no deduplica");

    const vistos = new Set<string>();
    let entregas = 0;
    let efectos = 0;
    await bus.subscribe("pagos.cobro.v1.recibido", async (ev) => {
      entregas++;
      if (vistos.has(ev.id)) return;
      vistos.add(ev.id);
      efectos++;
    });
    assert.ok(await esperarHasta(() => entregas >= 2, 5000), "llegan las dos copias");
    assert.equal(efectos, 1, "un efecto por id");
  });

  test("un hecho distinto (otra clave) tiene otro id y es un mensaje nuevo", async () => {
    const a = await bus.publish("pagos.cobro.v1.recibido", { n: 1 }, { externalKey: "pasarela:evt_2", time: T });
    assert.notEqual(a.id, externalEventId({ source: "/produccion/pasarela-web", externalKey: "pasarela:evt_1", time: T }));
  });

  test("sin `time` falla, con un error que dice por qué y cómo se arregla", async () => {
    await assert.rejects(
      bus.publish("pagos.cobro.v1.recibido", { n: 1 }, { externalKey: "pasarela:evt_3" }),
      (e: Error) => e instanceof EnvelopeError && /exige `time`/.test(e.message) && /no hay `id` determinista/.test(e.message),
    );
  });

  test("sin `externalKey`, el id sigue siendo un UUIDv7 nuevo cada vez", async () => {
    const a = await bus.publish("pagos.cobro.v1.recibido", { n: 1 }, { time: T });
    const b = await bus.publish("pagos.cobro.v1.recibido", { n: 1 }, { time: T });
    assert.notEqual(a.id, b.id);
  });

  test("una clave vacía o un instante inválido fallan antes de publicar", async () => {
    await assert.rejects(bus.publish("pagos.cobro.v1.recibido", {}, { externalKey: "", time: T }), EnvelopeError);
    await assert.rejects(bus.publish("pagos.cobro.v1.recibido", {}, { externalKey: "k", time: "ayer" }), EnvelopeError);
  });
});
