/**
 * `flux doctor` (ningún stream captura una operación) y `flux call`, CONTRA UN nats-server REAL.
 *
 * La CLI se ejecuta como proceso (lo que usa un operador) y los responders se escriben A MANO sobre
 * NATS crudo (`rpc-helpers.mjs`), sin ningún SDK de flux. Cada comprobación va en los dos sentidos:
 * el doctor sale 0 con un broker sano y 1 con cada stream que captura una operación; `call` da la
 * respuesta con un responder sano y su código estable con cada fallo.
 *
 *   NATS_SERVER=/ruta/a/nats-server npm test
 *   FLUX_REQUIRE_BROKER=1 npm test      → sin nats-server, ESTAS pruebas FALLAN (no se saltan)
 */

import { test, describe, before, after, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { connect } from "@nats-io/transport-node";

import { arrancaBroker, conecta, creaDlq, creaEvt, creaStreamEstado, detectarBroker, dormir, flux, REQUIRE, vaciaBroker } from "./helpers.mjs";
import { sirve } from "./rpc-helpers.mjs";
import { auditarStreamsRpc } from "../src/doctor-rpc.mjs";
import { solapan } from "../src/rpc-model.mjs";

const hay = detectarBroker();
if (!hay.ok) {
  if (REQUIRE) {
    test("hay un nats-server (FLUX_REQUIRE_BROKER=1)", () => assert.fail(hay.motivo));
  } else {
    console.error(`# SALTADO: ${hay.motivo}  (FLUX_REQUIRE_BROKER=1 lo convierte en fallo)`);
  }
}
const suite = hay.ok ? describe : describe.skip;

const RAIZ = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const BUNDLE = JSON.parse(readFileSync(join(RAIZ, "schemas", "bundle.json"), "utf8"));
const VERSION_CLI = JSON.parse(readFileSync(join(RAIZ, "cli", "package.json"), "utf8")).version;
const DESCUENTO = "clientes.cliente.v1.calcular-descuento";
const OBTENER = "clientes.cliente.v1.obtener";
const PETICION = '{"clienteId":"cli-1","totalCents":9990,"moneda":"EUR"}';
const BASE = ["--tenant", "acme", "--env", "produccion"];
const RE_UUIDV7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const DEAD = "nats://127.0.0.1:1"; // nadie escucha: lo que NO toca la red no puede fallar por esto

/** Un broker propio para una suite, con su conexión de pruebas. */
function conBroker(nombre, cuerpo, opciones) {
  suite(nombre, () => {
    const ctx = {};
    before(async () => {
      ctx.broker = await arrancaBroker(opciones);
      ctx.url = ctx.broker.url;
      if (!opciones?.conf) ctx.conn = await conecta(ctx.url);
    });
    after(async () => {
      await ctx.conn?.nc.close();
      await ctx.broker?.parar();
    });
    cuerpo(ctx);
  });
}

const json = (r) => JSON.parse(r.out);

// ─── doctor ──────────────────────────────────────────────────────────────────────────────────────

conBroker("flux doctor: ningún stream captura una operación (11-request-reply.md §8)", (ctx) => {
  beforeEach(async () => {
    await vaciaBroker(ctx.conn.jsm);
  });

  /** Un dominio sano: su stream de eventos y su DLQ. */
  const sano = async (dom = "pedidos") => {
    await creaEvt(ctx.conn.jsm, dom);
    await creaDlq(ctx.conn.jsm, dom, [`dlq.${dom}.>`]);
  };
  const crea = (name, subjects, noAck = false) => ctx.conn.jsm.streams.add({ name, subjects, storage: "file", ...(noAck && { no_ack: true }) });

  test("una account SIN streams: sale 0, y lo dice (no hay nada que capture una operación)", async () => {
    const h = await flux(ctx.url, ["doctor"]);
    assert.equal(h.code, 0, h.err + h.out);
    assert.match(h.out, /No hay streams en este servidor/);
    assert.match(h.out, /Petición\/respuesta/);
    assert.match(h.out, /✓ ningún stream captura rpc\.\*\.\*\.\*\.\* —la forma de toda operación— \(0 stream\(s\) revisados\)/);
    const j = json(await flux(ctx.url, ["doctor", "--json"]));
    assert.equal(j.ok, true);
    assert.equal(j.rpc.ok, true);
    assert.equal(j.rpc.revisados, 0);
    assert.deepEqual(j.findings, []);
  });

  test("un stream `EVT_X` normal (y su DLQ): sale 0, el veredicto de rpc es «ningún stream captura»", async () => {
    await creaEvt(ctx.conn.jsm, "x");
    await creaDlq(ctx.conn.jsm, "x", ["dlq.x.>"]);
    const h = await flux(ctx.url, ["doctor"]);
    assert.equal(h.code, 0, h.out);
    assert.match(h.out, /✓ ningún stream captura rpc\.\*\.\*\.\*\.\* .*\(2 stream\(s\) revisados\)/);
    const j = json(await flux(ctx.url, ["doctor", "--json"]));
    assert.deepEqual([j.ok, j.rpc.ok, j.rpc.revisados, j.streams], [true, true, 2, 2]);
  });

  // Cada uno de los filtros que capturan una operación: el doctor FALLA, dice QUÉ stream y CÓMO se arregla.
  const CAPTURAN = [
    { filtro: "rpc.>", noAck: false },
    { filtro: "rpc.x.>", noAck: false },
    { filtro: "rpc.*.>", noAck: false },
    { filtro: "rpc.clientes.cliente.v1.obtener", noAck: false },
    { filtro: "rpc.*.cliente.v1.*", noAck: false },
    { filtro: "rpc.*.*.*.*", noAck: false },
    { filtro: ">", noAck: true },
    { filtro: "*.>", noAck: true },
    { filtro: "*.*.*.*.*", noAck: true },
  ];
  for (const { filtro, noAck } of CAPTURAN) {
    test(`un stream que captura «${filtro}»${noAck ? " (solo se puede crear sin acuse, A-69)" : ""}: sale 1, nombra el stream y el arreglo`, async () => {
      await crea("TRAMPA", [filtro], noAck);
      const h = await flux(ctx.url, ["doctor"]);
      assert.equal(h.code, 1, h.out);
      assert.match(h.out, /✗ 1 stream\(s\) capturan operaciones: TRAMPA/);
      assert.match(h.out, new RegExp(`✗ TRAMPA {2}captura operaciones de petición/respuesta: \`${filtro.replace(/[.*>]/g, "\\$&")}\` se solapa con rpc\\.\\*\\.\\*\\.\\*\\.\\*`));
      assert.match(h.out, noAck ? /stream sin acuse, no_ack\).*CUENTA COMO INTERÉS/s : /stream con acuse\).*Contesta a cada petición con SU acuse/s);
      assert.match(h.out, /Cómo se arregla: .*nats stream rm TRAMPA/, "es lo único que captura: se borra");
      const j = json(await flux(ctx.url, ["doctor", "--json"]));
      assert.equal(j.ok, false);
      assert.equal(j.rpc.ok, false);
      assert.deepEqual(j.rpc.capturas.map((c) => [c.stream, c.subjects, c.noAck]), [["TRAMPA", [filtro], noAck]]);
      const f = j.findings.find((x) => x.check === "rpc-stream-captura-operaciones");
      assert.deepEqual([f.level, f.target], ["error", "TRAMPA"]);
      assert.match(f.hint, /nats stream rm TRAMPA/);
    });
  }

  test("un stream `no_ack` sobre `rpc.x.>` (A-67): marcado como «sin acuse», y por qué importa", async () => {
    await crea("AUDITORIA", ["rpc.x.>"], true);
    const h = await flux(ctx.url, ["doctor"]);
    assert.equal(h.code, 1);
    assert.match(h.out, /✗ AUDITORIA {2}captura operaciones .*stream sin acuse, no_ack/);
    assert.match(h.out, /no responders.*RPC_UNAVAILABLE.*timeout.*RPC_TIMEOUT/s);
    assert.equal(json(await flux(ctx.url, ["doctor", "--json"])).rpc.capturas[0].noAck, true);
  });

  test("un `no_ack` que NO captura una operación (`*.*.*.*`, `*.*.*.*.*.*`) no se marca: el doctor sale 0", async () => {
    await crea("CUATRO", ["*.*.*.*"], true);
    const h = await flux(ctx.url, ["doctor", "--json"]);
    assert.equal(json(h).rpc.ok, true);
    await vaciaBroker(ctx.conn.jsm);
    await crea("SEIS", ["*.*.*.*.*.*"], true);
    assert.equal(json(await flux(ctx.url, ["doctor", "--json"])).rpc.ok, true);
  });

  test("el stream captura ALGO MÁS: el arreglo conserva lo demás (`nats stream edit … --subjects`), y el resto del broker sigue sano", async () => {
    await sano("pedidos");
    await crea("MIXTO", ["clientes.>", "rpc.x.>", "logistica.>"]);
    const h = await flux(ctx.url, ["doctor"]);
    assert.equal(h.code, 1);
    assert.match(h.out, /✗ MIXTO {2}captura operaciones .*`rpc\.x\.>`/);
    assert.match(h.out, /nats stream edit MIXTO --subjects "clientes\.>,logistica\.>"/);
    assert.doesNotMatch(h.out, /✗ EVT_PEDIDOS|✗ DLQ_PEDIDOS/, "los streams sanos no se marcan");
    assert.match(h.out, /1 error\(es\)/);
    const j = json(await flux(ctx.url, ["doctor", "--json"]));
    assert.deepEqual(j.findings.filter((f) => f.level === "error").map((f) => f.target), ["MIXTO"]);
  });

  test("varios streams que capturan: cada uno con su hallazgo; los de otras familias (EVT/DLQ/STATE) conviven", async () => {
    await sano("pedidos");
    await creaStreamEstado(ctx.conn.jsm, "clientes");
    await crea("UNO", ["rpc.a.>"]);
    await crea("DOS", ["rpc.b.>"], true);
    const j = json(await flux(ctx.url, ["doctor", "--json"]));
    assert.deepEqual(j.rpc.capturas.map((c) => c.stream).sort(), ["DOS", "UNO"]);
    assert.equal(j.rpc.revisados, 5);
    const h = await flux(ctx.url, ["doctor"]);
    assert.match(h.out, /✗ 2 stream\(s\) capturan operaciones: /);
  });

  test("`flux provision` del manifiesto de ejemplo NO crea ningún stream que capture una operación (y el doctor lo confirma)", async () => {
    const p = await flux(ctx.url, ["provision", join(RAIZ, "examples", "services.json")]);
    assert.equal(p.code, 0, p.err + p.out);
    assert.match(p.out, /creado/);
    const j = json(await flux(ctx.url, ["doctor", "--json"]));
    assert.ok(j.rpc.revisados >= 8, `se aprovisionaron streams de las tres familias (${j.rpc.revisados})`);
    assert.equal(j.rpc.ok, true, JSON.stringify(j.rpc.capturas));
    const h = await flux(ctx.url, ["doctor"]);
    assert.match(h.out, /✓ ningún stream captura rpc\.\*\.\*\.\*\.\*/);
    // y se ve TODO lo que hay: ningún stream, de ningún tipo, solapa con la forma de una operación
    const nombres = [];
    for await (const s of ctx.conn.jsm.streams.list()) for (const sj of s.config.subjects) { nombres.push(`${s.config.name}:${sj}`); assert.equal(solapan(sj, "rpc.*.*.*.*"), false, `${s.config.name} ${sj}`); }
    assert.ok(nombres.some((n) => n.startsWith("EVT_")) && nombres.some((n) => n.startsWith("DLQ_")) && nombres.some((n) => n.startsWith("STATE_")));
  });

  test("EL BROKER MANDA: `STREAM.NAMES` con subject=rpc.*.*.*.* devuelve EXACTAMENTE los streams que el doctor marca (25 patrones, A-110)", async () => {
    const patrones = [
      "rpc.>", "rpc.*.>", "rpc.clientes.>", "rpc.clientes.cliente.v1.obtener", "rpc.*.*.*.*", "rpc.*.cliente.v1.*", ">", "*.>", "*.*.*.*.*",
      "clientes.>", "dlq.>", "state.>", "rpc.otro.>", "rpc.clientes.cliente.v1.crear", "*.*.*.*", "*.*.*.*.*.*", "rpc.*.*.*.*.>", "rpc.*.*.*.*.*",
      "*.*.*.*.>", "rpc", "rpc.a.b.c", "rpc.a.b.c.>", "*.a.*.*.*", "x.*.*.*.*", "*.rpc.*.*.*",
    ];
    let marcados = 0;
    for (const p of patrones) {
      await vaciaBroker(ctx.conn.jsm);
      // JetStream solo deja crear SIN acuse un filtro que captura todo o se solapa con su API (A-69)
      const s = await crea("T", [p]).catch(() => crea("T", [p], true));
      const r = await ctx.conn.nc.request("$JS.API.STREAM.NAMES", JSON.stringify({ subject: "rpc.*.*.*.*" }), { timeout: 2000 });
      const delBroker = (JSON.parse(new TextDecoder().decode(r.data)).streams ?? []).includes("T");
      const lista = [];
      for await (const x of ctx.conn.jsm.streams.list()) lista.push(x);
      const delDoctor = !auditarStreamsRpc(lista).ok;
      assert.equal(delDoctor, delBroker, `«${p}»: el doctor ${delDoctor ? "marca" : "no marca"} y el broker ${delBroker ? "lo solapa" : "no"}`);
      assert.equal(s.config.subjects[0], p);
      if (delDoctor) marcados++;
    }
    assert.equal(marcados, 15, "15 de los 25 capturan una operación");
  });
});

// ─── call ────────────────────────────────────────────────────────────────────────────────────────

conBroker("flux call: una petición a mano, con el sobre de un SDK y los códigos estables (11-request-reply.md §4)", (ctx) => {
  const respondes = [];
  const sirvo = (opciones) => {
    const r = sirve(ctx.conn.nc, opciones);
    respondes.push(r);
    return r;
  };
  beforeEach(async () => {
    await vaciaBroker(ctx.conn.jsm);
  });
  // Cada prueba sus propios responders: uno olvidado seguiría en el grupo de cola y se llevaría su parte (A-58).
  afterEach(async () => {
    for (const r of respondes.splice(0)) await r.parar().catch(() => {});
  });
  const llama = (args, o) => flux(ctx.url, ["call", ...args], o);
  /** Un responder sano de `calcular-descuento`. */
  const descuento = (extra = {}) =>
    sirvo({ operacion: DESCUENTO, handler: async (p) => ({ data: { descuentoCents: 500, moneda: p.data.moneda, motivo: "Campaña de otoño" } }), ...extra });
  const parado = async (r) => {
    await r.parar();
    respondes.splice(respondes.indexOf(r), 1);
  };

  test("respuesta OK: el responder ve un sobre de SDK, y la CLI imprime la respuesta", async () => {
    const r = descuento();
    const h = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--aggregate-id", "cli-1"]);
    assert.equal(h.code, 0, h.err + h.out);
    assert.match(h.out, /^✓ clientes\.cliente\.v1\.calcular-descuento → rpc\.clientes\.cliente\.v1\.calcular-descuento {2}[\d.]+ ms/);
    assert.match(h.out, /respuesta {2}com\.flux\.rpc\.clientes\.cliente\.calcular-descuento\.reply\.v1 · de \/produccion\/crm-api · tenant acme/);
    assert.match(h.out, /"descuentoCents": 500/);
    assert.match(h.out, /"motivo": "Campaña de otoño"/);

    // lo que MANDÓ la CLI: un solo mensaje, por core NATS, sin cabeceras, con un inbox privado y por la cola del servicio
    assert.equal(r.visto.length, 1);
    const v = r.visto[0];
    assert.equal(v.headers, undefined, "sin cabeceras de NATS (11-request-reply.md §3.4)");
    assert.match(v.reply, /^_INBOX\.flux-cli\.[A-Za-z0-9]+\.[A-Za-z0-9]+$/, "el inbox PRIVADO de la CLI (06-security.md §3.3)");
    assert.equal(v.subject, "rpc.clientes.cliente.v1.calcular-descuento");
    const e = v.evento;
    // el orden de claves es normativo: el de declaración, con `data` la última (la regla 8)
    assert.deepEqual(Object.keys(e), [
      "specversion", "id", "source", "type", "time", "datacontenttype", "dataschema", "subject", "correlationid", "tenantid", "producerversion",
      "dataclassification", "partitionkey", "data",
    ]);
    assert.equal(e.specversion, "1.0");
    assert.match(e.id, RE_UUIDV7);
    assert.equal(e.source, "/produccion/flux-cli");
    assert.equal(e.type, "com.flux.rpc.clientes.cliente.calcular-descuento.request.v1");
    assert.match(e.time, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
    assert.ok(Math.abs(Date.now() - Date.parse(e.time)) < 10_000, "la hora es la de AHORA");
    assert.equal(e.datacontenttype, "application/json");
    assert.equal(e.dataschema, BUNDLE.rpc[DESCUENTO].request, "el dataschema sale del índice rpc del bundle");
    assert.equal(e.subject, "cli-1");
    assert.equal(e.partitionkey, "cli-1");
    assert.equal(e.correlationid, e.id, "una petición que no nace de otro evento lleva su propio id");
    assert.equal(e.tenantid, "acme");
    assert.equal(e.producerversion, VERSION_CLI);
    assert.equal(e.dataclassification, "internal");
    assert.equal("causationid" in e, false);
    assert.equal("traceparent" in e, false, "sin traza activa, la CLI no inventa un traceparent");
    assert.deepEqual(e.data, { clienteId: "cli-1", totalCents: 9990, moneda: "EUR" });
    // son los bytes de `JSON.stringify` (UTF-8, sin espacios): lo que cualquier SDK produciría
    assert.equal(new TextDecoder().decode(v.bytes), JSON.stringify(e));
    assert.match(h.out, new RegExp(`petición id ${e.id} · correlationid ${e.id} · tenant acme`));
    assert.match(h.out, new RegExp(`causationid = id de la petición ${e.id}`));
  });

  test("sin --aggregate-id no hay `subject` ni `partitionkey`; el entorno puede venir de $FLUX_ENV; el tenant, nunca por defecto", async () => {
    const r = descuento();
    const h = await llama([DESCUENTO, "--tenant", "globex", "--data", PETICION], { env: { FLUX_ENV: "staging" } });
    assert.equal(h.code, 0, h.err);
    const e = r.visto[0].evento;
    assert.equal(e.source, "/staging/flux-cli");
    assert.equal(e.tenantid, "globex");
    assert.equal("subject" in e || "partitionkey" in e, false);
  });

  test("--json: `ok`, la operación, el subject, los ms y los DOS sobres, `data` el último", async () => {
    descuento();
    const j = json(await llama([DESCUENTO, ...BASE, "--data", PETICION, "--json"]));
    assert.equal(j.ok, true);
    assert.equal(j.operation, DESCUENTO);
    assert.equal(j.subject, `rpc.${DESCUENTO}`);
    assert.ok(typeof j.ms === "number" && j.ms < 1000);
    assert.equal(j.request.tenantid, "acme");
    assert.deepEqual(j.reply.data, { descuentoCents: 500, moneda: "EUR", motivo: "Campaña de otoño" });
    assert.equal(j.reply.causationid, j.request.id);
    assert.equal(j.reply.correlationid, j.request.correlationid);
    assert.equal(Object.keys(j.reply).at(-1), "data");
    assert.equal(Object.keys(j.request).at(-1), "data");
  });

  test("un error `permanent` del que responde: su código y su clase, tal cual; sale 1", async () => {
    descuento({ handler: async () => ({ error: { class: "permanent", code: "PEDIDO_YA_CANCELADO", message: "el pedido ya estaba cancelado: no se puede aplicar un descuento" } }) });
    const h = await llama([DESCUENTO, ...BASE, "--data", PETICION]);
    assert.equal(h.code, 1);
    assert.match(h.err, /^✗ PEDIDO_YA_CANCELADO \[PERMANENT\] {2}[\d.]+ ms {2}— error del servicio que responde \(com\.flux\.rpc\.error\.v1\)/);
    assert.match(h.err, /el pedido ya estaba cancelado/);
    assert.match(h.err, /responde \/produccion\/crm-api/);
    assert.equal(h.out, "", "un fallo no imprime en stdout");
    const j = json(await llama([DESCUENTO, ...BASE, "--data", PETICION, "--json"]));
    assert.equal(j.ok, false);
    assert.deepEqual(j.error, { code: "PEDIDO_YA_CANCELADO", class: "permanent", message: "el pedido ya estaba cancelado: no se puede aplicar un descuento", origin: "remote" });
    assert.equal(j.reply.type, "com.flux.rpc.error.v1");
  });

  test("un error `retryable` con `retryAfterMs`: se ve la clase y la sugerencia del primer reintento", async () => {
    descuento({ handler: async () => ({ error: { class: "retryable", code: "HTTP_503", message: "el servicio de tarifas no responde", retryAfterMs: 5000 } }) });
    const h = await llama([DESCUENTO, ...BASE, "--data", PETICION]);
    assert.equal(h.code, 1);
    assert.match(h.err, /^✗ HTTP_503 \[RETRYABLE\]/);
    assert.match(h.err, /retryAfterMs 5000: una sugerencia para el PRIMER reintento/);
    const j = json(await llama([DESCUENTO, ...BASE, "--data", PETICION, "--json"]));
    assert.deepEqual([j.error.class, j.error.code, j.error.retryAfterMs, j.error.origin], ["retryable", "HTTP_503", 5000, "remote"]);
  });

  test("NADIE ESCUCHA: RPC_UNAVAILABLE en milisegundos (no en el timeout), RETRYABLE, con el diagnóstico", async () => {
    const t0 = Date.now();
    const h = await llama([OBTENER, ...BASE, "--data", '{"clienteId":"cli-1"}', "--json", "--timeout", "5000"]);
    assert.ok(Date.now() - t0 < 4000, "no esperó el timeout");
    assert.equal(h.code, 1);
    const j = json(h);
    assert.equal(j.error.code, "RPC_UNAVAILABLE");
    assert.equal(j.error.class, "retryable");
    assert.ok(j.ms < 100, `la respuesta de «no responders» llega en ${j.ms} ms (acepta < 100, A-56)`);
    assert.match(j.error.message, /nadie atiende «clientes\.cliente\.v1\.obtener».*no responders.*dueño del dominio «clientes».*respond\(\).*rpc\.clientes\.>.*A-73.*no ejecutó nada/s);
    const humano = await llama([OBTENER, ...BASE, "--data", '{"clienteId":"cli-1"}']);
    assert.match(humano.err, /^✗ RPC_UNAVAILABLE \[RETRYABLE\] {2}[\d.]+ ms/);
  });

  test("un responder que atiende OTRA operación del servicio no atiende esta: RPC_UNAVAILABLE (suscripción exacta, A-64)", async () => {
    descuento();
    const j = json(await llama([OBTENER, ...BASE, "--data", '{"clienteId":"cli-1"}', "--json"]));
    assert.equal(j.error.code, "RPC_UNAVAILABLE");
    assert.ok(j.ms < 100);
  });

  test("RESPONDER LENTO: RPC_TIMEOUT, y la operación SE EJECUTÓ (el mensaje lo avisa: A-61)", async () => {
    let ejecuciones = 0;
    descuento({ demoraMs: 600, handler: async () => { ejecuciones++; return { data: { descuentoCents: 1, moneda: "EUR" } }; } });
    const h = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--timeout", "200", "--json"]);
    assert.equal(h.code, 1);
    const j = json(h);
    assert.equal(j.error.code, "RPC_TIMEOUT");
    assert.equal(j.error.class, "retryable");
    assert.ok(j.ms >= 190 && j.ms < 1500, `tardó su timeout: ${j.ms} ms`);
    assert.match(j.error.message, /no llegó respuesta en 200 ms.*PUEDE haberse ejecutado.*deduplique por el `id` de la petición \([0-9a-f-]{36}\)/s);
    assert.equal(j.error.message.includes(j.request.id), true, "el mensaje da el id de la petición, para buscarlo en el log del responder");
    // Se espera a que ocurra (con tope), no un tiempo fijo: una máquina cargada retrasa al responder.
    for (let i = 0; i < 100 && ejecuciones === 0; i++) await dormir(50);
    assert.equal(ejecuciones, 1, "el responder ejecutó la orden aunque quien llamó ya no esperaba");
  });

  test("EL ACUSE DE UN STREAM (A-18, A-67): RPC_INVALID_REPLY, PERMANENT, con el stream y la pista de `flux doctor`", async () => {
    await ctx.conn.jsm.streams.add({ name: "TRAMPA", subjects: ["rpc.>"], storage: "file" });
    const h = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--json"]);
    assert.equal(h.code, 1);
    const j = json(h);
    assert.equal(j.error.code, "RPC_INVALID_REPLY");
    assert.equal(j.error.class, "permanent");
    assert.match(j.error.message, /ACUSE del stream TRAMPA de JetStream \(\{"stream":"TRAMPA","seq":1\}\), no la respuesta del servicio/);
    assert.match(j.error.message, /algún stream captura `rpc\.clientes\.cliente\.v1\.calcular-descuento`/);
    assert.match(j.error.message, /guarda la petición \(datos personales incluidos\).*Ejecuta `flux doctor`: dice qué stream es y cómo se arregla/s);
    // y el stream GUARDÓ la petición, con su data: lo que hace la trampa peligrosa
    assert.equal((await ctx.conn.jsm.streams.info("TRAMPA")).state.messages, 1);
    // `flux doctor` lo delata
    assert.equal((await flux(ctx.url, ["doctor"])).code, 1);
  });

  test("un stream `no_ack` que captura la operación: RPC_TIMEOUT, y el mensaje NOMBRA el stream (STREAM.NAMES) en vez de dejar adivinar", async () => {
    await ctx.conn.jsm.streams.add({ name: "AUDITORIA", subjects: ["rpc.>"], storage: "file", no_ack: true });
    const h = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--timeout", "300", "--json"]);
    const j = json(h);
    assert.equal(j.error.code, "RPC_TIMEOUT", "no hay `no responders`: el stream cuenta como interés");
    assert.ok(j.ms >= 290, `esperó su timeout (${j.ms} ms)`);
    assert.match(j.error.message, /MIRA ESTO: el stream AUDITORIA captura rpc\.clientes\.cliente\.v1\.calcular-descuento.*SIN acuse.*no responders.*timeout.*flux doctor/s);
  });

  test("un timeout sin ningún stream de por medio NO acusa a nadie", async () => {
    descuento({ demoraMs: 500 });
    const j = json(await llama([DESCUENTO, ...BASE, "--data", PETICION, "--timeout", "100", "--json"]));
    assert.equal(j.error.code, "RPC_TIMEOUT");
    assert.doesNotMatch(j.error.message, /MIRA ESTO|stream/);
  });

  test("LA RESPUESTA NO CABE: el que responde contesta RPC_MESSAGE_TOO_LARGE (A-65); la CLI lo da con su clase PERMANENT", async () => {
    descuento({ handler: async () => ({ error: { class: "permanent", code: "RPC_MESSAGE_TOO_LARGE", message: "la respuesta ocupa 1048577 bytes y el límite es 1048576: pagina con un cursor en data" } }) });
    const h = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--json"]);
    assert.equal(h.code, 1);
    const j = json(h);
    assert.deepEqual([j.error.code, j.error.class, j.error.origin], ["RPC_MESSAGE_TOO_LARGE", "permanent", "remote"]);
    assert.match(j.error.message, /1048577 bytes.*1048576/);
  });

  test("un responder que NO comprueba el tamaño: la librería lanza en ÉL y quien llama solo ve un timeout (A-65)", async () => {
    let lanzo = null;
    const sub = ctx.conn.nc.subscribe(`rpc.${DESCUENTO}`, { queue: "crm-api" });
    (async () => { for await (const m of sub) { try { m.respond(new Uint8Array(1048577)); } catch (e) { lanzo = e; } } })();
    const j = json(await llama([DESCUENTO, ...BASE, "--data", PETICION, "--timeout", "400", "--json", "--no-validate"]));
    assert.equal(j.error.code, "RPC_TIMEOUT");
    for (let i = 0; i < 100 && lanzo === null; i++) await dormir(20);
    assert.match(lanzo?.message ?? "", /max_payload/, "el error de la librería lo recibe el que responde");
    sub.unsubscribe();
  });

  test("UNA RESPUESTA DE 900 KB sale ENTERA por una tubería (process.exit tras un console.log grande la cortaría en 146 KB)", async () => {
    sirvo({ operacion: DESCUENTO, handler: async () => ({ data: { descuentoCents: 1, moneda: "EUR", motivo: "x".repeat(900_000) } }) });
    const h = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--json", "--no-validate"]);
    assert.equal(h.code, 0, h.err);
    assert.ok(Buffer.byteLength(h.out) > 900_000, `${Buffer.byteLength(h.out)} bytes`);
    assert.equal(json(h).reply.data.motivo.length, 900_000, "el JSON está completo y se puede leer");
    const humano = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--no-validate"]);
    assert.ok(Buffer.byteLength(humano.out) > 900_000 && /"descuentoCents": 1/.test(humano.out));
  });

  test("LA PETICIÓN NO CABE: RPC_MESSAGE_TOO_LARGE antes de tocar la red (no se intenta conectar: -s a un puerto muerto)", async () => {
    const dir = mkdtempSync(join(tmpdir(), "flux-call-"));
    try {
      const f = join(dir, "grande.json");
      writeFileSync(f, JSON.stringify({ clienteId: "cli-1", totalCents: 1, moneda: "EUR", campana: "x".repeat(1_100_000) }));
      const h = await flux(DEAD, ["call", DESCUENTO, ...BASE, "--data", `@${f}`, "--no-validate", "--json"]);
      assert.equal(h.code, 1, "falla con 1, no con 2 (no se pudo conectar): nunca llegó a conectar");
      const j = json(h);
      assert.equal(j.error.code, "RPC_MESSAGE_TOO_LARGE");
      assert.equal(j.error.class, "permanent");
      assert.match(j.error.message, /la petición serializada ocupa 11\d{5} bytes y el límite es 1048576.*1 MiB.*claim-check.*cursor/s);
      assert.doesNotMatch(h.out + h.err, /xxxxxxxx/, "no reproduce el contenido");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("OPERACIÓN MAL FORMADA y DOMINIO RESERVADO: antes de tocar la red (un puerto muerto no cambia nada) y sin enviar nada", async () => {
    const r = descuento();
    for (const [op, codigo] of [
      ["Clientes.cliente.v1.obtener", "INVALID_RPC_CALL"],
      ["clientes.cliente.v1", "INVALID_RPC_CALL"],
      ["clientes.cliente.v1.obtener_cliente", "INVALID_RPC_CALL"],
      ["clientes.cliente.v1.*", "INVALID_RPC_CALL"],
      ["clientes.cliente.v0.obtener", "INVALID_RPC_CALL"],
      ["clientes.cliente.v1.obtener\n", "INVALID_RPC_CALL"],
      ["rpc.clientes.cliente.v1.obtener", "RESERVED_DOMAIN"],
      ["state.cliente.v1.obtener", "RESERVED_DOMAIN"],
      ["dlq.cliente.v1.obtener", "RESERVED_DOMAIN"],
      ["system.cliente.v1.obtener", "RESERVED_DOMAIN"],
    ]) {
      for (const url of [DEAD, ctx.url]) {
        const h = await flux(url, ["call", op, ...BASE, "--data", "{}", "--json"]);
        assert.equal(h.code, 1, `${JSON.stringify(op)} en ${url}`);
        const j = json(h);
        assert.equal(j.error.code, codigo, JSON.stringify(op));
        assert.equal(j.error.class, "permanent");
        assert.equal(j.request, null, "no se construyó ningún sobre");
        assert.equal(j.ms, null, "no se esperó nada");
      }
    }
    assert.equal(r.visto.length, 0, "ninguna petición salió");
    const h = await flux(ctx.url, ["call", "rpc.clientes.cliente.v1.obtener", ...BASE, "--data", "{}"]);
    assert.match(h.err, /RESERVED_DOMAIN.*El prefijo `rpc\.` lo pone la CLI: escribe «clientes\.cliente\.v1\.obtener»/s);
  });

  test("TIMEOUT fuera de 1..25000 ms (o no entero) es INVALID_RPC_CALL y NO se recorta: no se envía nada", async () => {
    const r = descuento();
    for (const t of ["0", "-1", "25001", "30000", "60000", "1.5", "abc", "1e3"]) {
      const h = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--timeout", t, "--json"]);
      assert.equal(h.code, 1, t);
      const j = json(h);
      assert.equal(j.error.code, "INVALID_RPC_CALL", t);
      assert.match(j.error.message, /entero entre 1 y 25000 ms.*No se recorta/, t);
    }
    assert.equal(r.visto.length, 0, "un 30000 NO se recortó a 25000: no hubo petición");
    // los extremos válidos SÍ pasan
    for (const t of ["1", "25000"]) {
      const h = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--timeout", t, "--json"]);
      assert.notEqual(json(h).error?.code, "INVALID_RPC_CALL", t);
    }
  });

  test("--data: falta, no es JSON, no es un objeto: INVALID_RPC_CALL sin reproducir el texto; --tenant y --env obligatorios", async () => {
    const r = descuento();
    const casos = [
      [["--data", '{"clienteId":"secreto-9",}'], /no es JSON válido \(el error está en la posición \d+\)/],
      [["--data", "[1]"], /no un array/],
      [["--data", '"hola"'], /no un texto/],
      [["--data", "null"], /no null/],
      [[], /falta `--data`/],
    ];
    for (const [args, re] of casos) {
      const h = await llama([DESCUENTO, ...BASE, ...args, "--json"]);
      assert.equal(h.code, 1);
      const j = json(h);
      assert.equal(j.error.code, "INVALID_RPC_CALL");
      assert.match(j.error.message, re);
      assert.doesNotMatch(h.out + h.err, /secreto-9/, "los mensajes de error no llevan el contenido de `data`");
    }
    const sinTenant = await llama([DESCUENTO, "--env", "produccion", "--data", PETICION]);
    assert.equal(sinTenant.code, 1);
    assert.match(sinTenant.err, /falta `--tenant <tenant>`: la CLI no asume uno, ni `system`/);
    const sinEnv = await llama([DESCUENTO, "--tenant", "acme", "--data", PETICION]);
    assert.equal(sinEnv.code, 1);
    assert.match(sinEnv.err, /falta el entorno: `--env <entorno>` \(o \$FLUX_ENV\)/);
    const envMalo = await llama([DESCUENTO, "--tenant", "acme", "--env", "Producción", "--data", PETICION]);
    assert.match(envMalo.err, /entorno «Producción» no es válido/);
    const claseMala = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--classification", "secreta", "--json"]);
    assert.equal(json(claseMala).error.code, "INVALID_DATACLASSIFICATION");
    assert.equal(r.visto.length, 0, "nada de lo anterior llegó al broker");
  });

  test("`--data @fichero`: lo lee; uno que no existe lo dice", async () => {
    const r = descuento();
    const dir = mkdtempSync(join(tmpdir(), "flux-call-"));
    try {
      const f = join(dir, "peticion.json");
      writeFileSync(f, PETICION);
      const h = await llama([DESCUENTO, ...BASE, "--data", `@${f}`]);
      assert.equal(h.code, 0, h.err);
      assert.deepEqual(r.visto[0].evento.data, { clienteId: "cli-1", totalCents: 9990, moneda: "EUR" });
      const malo = await llama([DESCUENTO, ...BASE, "--data", `@${join(dir, "no-existe.json")}`]);
      assert.match(malo.err, /no se pudo leer el fichero .* de `--data` \(ENOENT\)/);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("DATO PERSONAL: una respuesta `confidential` NO se imprime sin --show-data (ni en --json); con él, sí; el error nunca se oculta", async () => {
    sirvo({ operacion: OBTENER, handler: async () => ({ classification: "confidential", data: { clienteId: "cli-1", nombre: "Ana Pérez", email: "ana@example.com" } }) });
    const oculto = await llama([OBTENER, ...BASE, "--data", '{"clienteId":"cli-1"}']);
    assert.equal(oculto.code, 0, oculto.err);
    assert.match(oculto.out, /\[oculto: dataclassification=confidential\].*--show-data/);
    assert.doesNotMatch(oculto.out + oculto.err, /example\.com|Ana Pérez/);
    const ocultoJson = await llama([OBTENER, ...BASE, "--data", '{"clienteId":"cli-1"}', "--json"]);
    assert.equal(json(ocultoJson).reply.data, "[oculto: dataclassification=confidential]");
    assert.doesNotMatch(ocultoJson.out, /example\.com|Ana Pérez/);
    assert.equal(Object.keys(json(ocultoJson).reply).at(-1), "data", "el sobre conserva `data` como último atributo también con la marca");
    const visible = await llama([OBTENER, ...BASE, "--data", '{"clienteId":"cli-1"}', "--show-data"]);
    assert.match(visible.out, /"email": "ana@example\.com"/);
    const visibleJson = json(await llama([OBTENER, ...BASE, "--data", '{"clienteId":"cli-1"}', "--show-data", "--json"]));
    assert.equal(visibleJson.reply.data.email, "ana@example.com");
  });

  test("lo `restricted` también se oculta; lo `public` y lo `internal` se ven", async () => {
    for (const [clasificacion, visible] of [["restricted", false], ["public", true]]) {
      const r = sirvo({ operacion: DESCUENTO, handler: async () => ({ classification: clasificacion, data: { descuentoCents: 1, moneda: "EUR" } }) });
      const h = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--no-validate"]);
      assert.equal(/"descuentoCents": 1/.test(h.out), visible, clasificacion);
      await parado(r);
    }
  });

  test("la clasificación de la PETICIÓN va en el sobre, y con `confidential` el `-v` no vuelca su data", async () => {
    const r = descuento();
    const h = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--classification", "confidential", "-v"]);
    assert.equal(r.visto[0].evento.dataclassification, "confidential");
    assert.match(h.out, /petición enviada:/);
    assert.match(h.out, /\[oculto: dataclassification=confidential\]/);
    assert.doesNotMatch(h.out, /"clienteId": "cli-1"/);
    const h2 = await llama([DESCUENTO, ...BASE, "--data", PETICION, "-v"]);
    assert.match(h2.out, /"clienteId": "cli-1"/, "internal: se ve");
  });

  test("L3, SIN RED: `data` que no cumple el esquema del bundle → INVALID_SCHEMA con TODAS las rutas y NINGÚN valor; el responder no ve nada", async () => {
    const r = descuento();
    const h = await flux(DEAD, ["call", DESCUENTO, ...BASE, "--data", '{"clienteId":"cli-1","totalCents":"mucho","extra":"valor-secreto"}', "--json"]);
    assert.equal(h.code, 1, "falla con 1 (no con 2): validó antes de conectar");
    const j = json(h);
    assert.equal(j.error.code, "INVALID_SCHEMA");
    assert.equal(j.error.class, "permanent");
    assert.match(j.error.message, /la petición no cumple https:\/\/schemas\.internal\/rpc\/clientes\/cliente\/calcular-descuento\/request\/1\.0\.0\.json/);
    assert.match(j.error.message, /\/moneda es obligatorio/);
    assert.match(j.error.message, /\/totalCents debe ser de tipo integer/);
    assert.match(j.error.message, /\(raíz\) no admite el campo «extra»/);
    assert.doesNotMatch(h.out, /valor-secreto|mucho/, "rutas, no valores");
    assert.match(j.error.message, /--no-validate/);
    assert.equal(r.visto.length, 0);
  });

  test("`--no-validate` manda lo que no cumple el esquema, para ver qué contesta el responder (INVALID_SCHEMA de SU lado)", async () => {
    const r = descuento({ handler: async () => ({ error: { class: "permanent", code: "INVALID_SCHEMA", message: "la petición no cumple el esquema: /totalCents debe ser un entero" } }) });
    const h = await llama([DESCUENTO, ...BASE, "--data", '{"clienteId":"cli-1","totalCents":"mucho","moneda":"EUR"}', "--no-validate", "--json"]);
    assert.equal(r.visto.length, 1, "la petición salió");
    const j = json(h);
    assert.deepEqual([j.error.code, j.error.origin], ["INVALID_SCHEMA", "remote"]);
  });

  test("una operación que no está en el bundle: se manda igual, con el `dataschema` <major>.0.0, y la nota lo dice", async () => {
    const r = sirvo({ operacion: "logistica.envio.v2.reservar-stock", handler: async () => ({ data: { reservado: true } }) });
    const h = await llama(["logistica.envio.v2.reservar-stock", ...BASE, "--data", '{"sku":"A"}', "--json"]);
    assert.equal(h.code, 0, h.err);
    assert.equal(r.visto[0].evento.dataschema, "https://schemas.internal/rpc/logistica/envio/reservar-stock/request/2.0.0.json");
    assert.match(json(h).notes[0], /no está en el índice rpc del bundle.*no se valida/);
  });

  test("L3 en la RESPUESTA: si no cumple SU esquema, INVALID_SCHEMA (el fallo es del que responde); --no-validate la deja ver", async () => {
    descuento({ handler: async () => ({ data: { descuentoCents: "cinco", moneda: "EUR" } }) });
    const h = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--json"]);
    assert.equal(h.code, 1);
    const j = json(h);
    assert.equal(j.error.code, "INVALID_SCHEMA");
    assert.match(j.error.message, /la RESPUESTA no cumple .*reply\/1\.0\.0\.json: \/descuentoCents debe ser de tipo integer.*del servicio que responde/s);
    assert.equal(j.reply.type, "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1");
    const libre = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--no-validate", "--json"]);
    assert.equal(libre.code, 0);
    assert.equal(json(libre).reply.data.descuentoCents, "cinco");
  });

  // ─── la respuesta que NO es de esta petición: las cinco comprobaciones, por el cable (§4.3) ───

  const INCIDENTES = [
    ["`causationid` de otra petición", (ev) => ({ ...ev, causationid: "01924f91-ffff-7c1a-8e22-9a4b3c2d1e10" }), /causationid.*no es el `id` de esta petición/],
    ["`tenantid` cambiado", (ev) => ({ ...ev, tenantid: "globex" }), /tenantid.*hereda sin modificar/],
    ["`correlationid` cambiado", (ev) => ({ ...ev, correlationid: "otro" }), /correlationid.*HEREDA/],
    ["`type` de otra operación", (ev) => ({ ...ev, type: "com.flux.rpc.clientes.cliente.obtener.reply.v1" }), /no es el de la respuesta de esta operación/],
    ["sin `causationid`", ({ causationid, ...ev }) => ev, /causationid/],
    ["`tenantid` numérico (los tipos son exactos)", (ev) => ({ ...ev, tenantid: 42 }), /`tenantid` debe ser una cadena/],
    ["sin `dataclassification`", ({ dataclassification, ...ev }) => ev, /`dataclassification` falta o está vacío/],
  ];
  for (const [titulo, cambia, esperado] of INCIDENTES) {
    test(`una respuesta con ${titulo} es RPC_INVALID_REPLY (PERMANENT: un incidente, no un fallo que esperar)`, async () => {
      descuento({ handler: async () => ({ data: { descuentoCents: 500, moneda: "EUR" }, cambios: cambia }) });
      const h = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--json"]);
      assert.equal(h.code, 1);
      const j = json(h);
      assert.equal(j.error.code, "RPC_INVALID_REPLY");
      assert.equal(j.error.class, "permanent");
      assert.match(j.error.message, esperado);
    });
  }
  // EXCEPCIÓN ÚNICA de la comprobación 4 (§4.3): el error de una petición que quien responde no pudo leer lleva `correlationid`
  // propio y `tenantid: system` (§5.2). Un responder en bruto contra un broker real lo contesta.
  const ILEGIBLE = ({ error, ...ev }) => ({ ...ev, correlationid: ev.id, tenantid: "system", data: { class: "permanent", code: "MALFORMED_JSON", message: "la petición no es un CloudEvent válido de flux" } });
  test("EL ERROR DE UNA PETICIÓN ILEGIBLE pasa la comprobación 4: la CLI da el código del sobre (MALFORMED_JSON), PERMANENT, no RPC_INVALID_REPLY", async () => {
    descuento({ handler: async () => ({ error: { class: "permanent", code: "X", message: "m" }, cambios: ILEGIBLE }) });
    const h = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--json"]);
    assert.equal(h.code, 1);
    const j = json(h);
    assert.deepEqual([j.error.code, j.error.class, j.error.origin], ["MALFORMED_JSON", "permanent", "remote"]);
    assert.equal(j.reply.type, "com.flux.rpc.error.v1");
    assert.match((await llama([DESCUENTO, ...BASE, "--data", PETICION])).err, /^✗ MALFORMED_JSON \[PERMANENT\]/);
  });
  test("CONTROL: la excepción es estrecha (un `correlationid` que no es el propio, un tenant ajeno, sin `causationid`) → RPC_INVALID_REPLY", async () => {
    for (const cambia of [
      (ev) => ({ ...ILEGIBLE(ev), correlationid: "otro-flujo" }),
      (ev) => ({ ...ILEGIBLE(ev), tenantid: "globex" }),
      (ev) => { const { causationid, ...sin } = ILEGIBLE(ev); return sin; },
    ]) {
      const r = descuento({ handler: async () => ({ error: { class: "permanent", code: "X", message: "m" }, cambios: cambia }) });
      const j = json(await llama([DESCUENTO, ...BASE, "--data", PETICION, "--json"]));
      assert.equal(j.error.code, "RPC_INVALID_REPLY");
      await parado(r);
    }
  });
  test("una «respuesta» que es basura (no JSON), o un error con `class: poison` o sin `code`, es RPC_INVALID_REPLY", async () => {
    const basura = sirvo({ operacion: DESCUENTO, handler: async () => ({ crudo: new TextEncoder().encode("tarjeta 4111-1111") }) });
    const h = await llama([DESCUENTO, ...BASE, "--data", PETICION, "--json"]);
    assert.equal(json(h).error.code, "RPC_INVALID_REPLY");
    assert.doesNotMatch(h.out, /4111/, "el cuerpo de lo que llegó no se reproduce (puede ser un dato personal)");
    assert.match((await llama([DESCUENTO, ...BASE, "--data", PETICION, "--json", "--show-data"])).out, /4111-1111/);
    await parado(basura);
    descuento({ handler: async () => ({ error: { class: "poison", code: "X", message: "m" } }) });
    assert.match(json(await llama([DESCUENTO, ...BASE, "--data", PETICION, "--json"])).error.message, /`poison` no viaja/);
  });

  test("`flux call` no necesita JetStream: funciona contra un servidor sin su API ni su cuenta", async () => {
    // El uso real: quien llama no tiene permiso sobre `$JS.API.>`. Aquí se comprueba que el comando no la usa en el camino feliz.
    const r = descuento();
    const j = json(await llama([DESCUENTO, ...BASE, "--data", PETICION, "--json"]));
    assert.equal(j.ok, true);
    assert.equal(r.visto.length, 1);
  });

  test("sin broker: sale con 2 y dice cómo arreglarlo (no es un error de la llamada)", async () => {
    const h = await flux(DEAD, ["call", DESCUENTO, ...BASE, "--data", PETICION]);
    assert.equal(h.code, 2);
    assert.match(h.err, /no se pudo conectar a nats:\/\/127\.0\.0\.1:1/);
  });
});

// ─── call bajo una ACL: el inbox privado y la denegación que se ve ───────────────────────────────

const ACL = (subscribeLlamante) => `
accounts {
  A {
    users = [
      { user: llama, password: pw, permissions: { publish: { allow: ["rpc.clientes.cliente.v1.calcular-descuento"] }, subscribe: { allow: [${subscribeLlamante}] } } }
      { user: crm, password: pw, permissions: { subscribe: { allow: ["rpc.clientes.>"] }, allow_responses: { max: 1, expires: "30s" } } }
    ]
  }
}
no_auth_user: llama
`;

describe_acl("flux call bajo la ACL de un llamante: publica SOLO en la operación concedida y se suscribe SOLO a `_INBOX.flux-cli.>`", '"_INBOX.flux-cli.>"', (ctx) => {
  test("la operación concedida funciona con el inbox privado (con el `_INBOX.<aleatorio>` por defecto de la librería, no)", async () => {
    const crm = await connect({ servers: ctx.url, user: "crm", pass: "pw" });
    const r = sirve(crm, { operacion: DESCUENTO, handler: async () => ({ data: { descuentoCents: 500, moneda: "EUR" } }) });
    try {
      const h = await flux(ctx.url, ["call", DESCUENTO, ...BASE, "--data", PETICION, "--json"]);
      assert.equal(h.code, 0, h.err + h.out);
      assert.equal(json(h).ok, true);
      assert.match(r.visto[0].reply, /^_INBOX\.flux-cli\./);
    } finally {
      await r.parar();
      await crm.close();
    }
  });

  test("una operación NO concedida: PERMISSION_DENIED en milisegundos (no un timeout), con el subject y cómo arreglarlo", async () => {
    const h = await flux(ctx.url, ["call", OBTENER, ...BASE, "--data", '{"clienteId":"cli-1"}', "--json", "--timeout", "5000"]);
    assert.equal(h.code, 1);
    const j = json(h);
    assert.equal(j.error.code, "PERMISSION_DENIED");
    assert.equal(j.error.class, "permanent");
    assert.ok(j.ms < 500, `falla rápido (${j.ms} ms), no a los 5000 del timeout`);
    assert.match(j.error.message, /deniega a esta conexión publicar en «rpc\.clientes\.cliente\.v1\.obtener».*clientes\.cliente\.v1\.obtener.*manifiesto.*gen-acl\.mjs/s);
  });
});

describe_acl("flux call bajo una ACL SIN el inbox concedido: PERMISSION_DENIED nombrando lo que falta", '"otro.>"', (ctx) => {
  test("sin permiso sobre `_INBOX.flux-cli.>` ninguna respuesta puede llegar: se dice, no se ve como un timeout", async () => {
    const h = await flux(ctx.url, ["call", DESCUENTO, ...BASE, "--data", PETICION, "--json", "--timeout", "5000"]);
    assert.equal(h.code, 1);
    const j = json(h);
    assert.equal(j.error.code, "PERMISSION_DENIED");
    assert.ok(j.ms < 500, `${j.ms} ms`);
    assert.match(j.error.message, /suscribirse a su inbox.*_INBOX\.flux-cli\.>.*06-security\.md §3\.3/s);
  });
});

function describe_acl(nombre, subscribeLlamante, cuerpo) {
  conBroker(nombre, cuerpo, { conf: ACL(subscribeLlamante) });
}
