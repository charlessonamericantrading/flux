/**
 * Webhooks de terceros, de extremo a extremo — specification/12-edge.md §5 y §6.3.
 *
 * Contra un nats-server REAL con los permisos generados. Lo central: **el mismo webhook enviado tres veces produce UN efecto en el
 * consumidor** (con y sin la ventana de `duplicate_window` de JetStream), y todo lo que debe rechazarse (firma inválida, firma
 * correcta fuera de tiempo, cuerpo manipulado) se rechaza SIN publicar nada.
 *
 * Sin `nats-server` estas pruebas se SALTAN con aviso, salvo con FLUX_REQUIRE_BROKER=1, donde FALLAN.
 */

import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";

import { externalEventId } from "@flux/sdk";

import { SECRETOS, describeBroker, dormir, esperarHasta, levantarTodo, mensajesDe } from "./helpers/entorno.mjs";
import { cargar } from "./helpers/fuente.mjs";

const { firmar } = await cargar("firma");

const SOURCE = "/produccion/pasarela-web";
const ahora = () => Math.floor(Date.now() / 1000);

describeBroker(describe, test, "webhooks de terceros: HTTP → evento con id determinista", () => {
  let e;
  let conta; // el consumidor del ecosistema que aplica el efecto de un cobro
  const efectos = new Map(); // id → nº de efectos
  let entregas = 0;

  before(async () => {
    e = await levantarTodo();
    conta = await e.nativo("contabilidad-api");
    await conta.subscribe("pagos.cobro.v1.recibido", async (ev) => {
      entregas++;
      // idempotente por `id`: el efecto se aplica una vez por id
      if (!efectos.has(ev.id)) efectos.set(ev.id, 0);
      if (efectos.get(ev.id) === 0) efectos.set(ev.id, 1);
    });
  });
  after(async () => {
    await conta?.close().catch(() => {});
    await e?.parar();
  });

  const cuerpoDe = (id, extra = {}) => JSON.stringify({ id, created: 1787221600, amount: 9990, currency: "EUR", customer: { email: "ana@example.com", nombre: "Ana" }, ...extra });
  const hook = (cuerpo, { t = ahora(), secreto = SECRETOS.PASARELA_HOOK, cabecera = "x-pasarela-firma", nombre = "pasarela", firma } = {}) =>
    e.llamar("POST", `/v1/hooks/${nombre}`, { cuerpo, cabeceras: { [cabecera]: firma ?? firmar({ secretos: [secreto], t, cuerpo }) } });
  const cobros = async () => mensajesDe(e.eco.ops.jsm, "EVT_PAGOS");
  const cuantos = async () => (await e.eco.ops.jsm.streams.info("EVT_PAGOS")).state.messages;

  describe("el mismo webhook, tres veces: UN efecto", () => {
    test("dentro de `duplicate_window`: tres 202 con el mismo id, UN mensaje en el stream y UN efecto", async () => {
      const cuerpo = cuerpoDe("evt_triple");
      const n0 = await cuantos();
      const rs = [];
      for (let i = 0; i < 3; i++) rs.push(await hook(cuerpo, { t: ahora() + i })); // t distinto en cada reenvío, como en la realidad
      assert.deepEqual(rs.map((r) => r.status), [202, 202, 202]);
      const id = rs[0].json.id;
      assert.deepEqual(rs.map((r) => r.json.id), [id, id, id], "el mismo id las tres veces");
      assert.equal(id, externalEventId({ source: SOURCE, externalKey: "pasarela:evt_triple", time: new Date(1787221600 * 1000).toISOString() }), "es el id DETERMINISTA de la spec");
      assert.equal((await cuantos()) - n0, 1, "JetStream guarda UN mensaje (A-157)");
      assert.ok(await esperarHasta(() => efectos.has(id), 8000));
      await dormir(300);
      assert.equal(efectos.get(id), 1, "UN efecto");
    });

    test("FUERA de `duplicate_window` JetStream guarda los dos, y el consumidor, que deduplica por id, aplica UN efecto (A-157)", async () => {
      const cfg = (await e.eco.ops.jsm.streams.info("EVT_PAGOS")).config;
      await e.eco.ops.jsm.streams.update("EVT_PAGOS", { ...cfg, duplicate_window: 1e9 }); // 1 s
      const cuerpo = cuerpoDe("evt_fuera_de_ventana");
      const a = await hook(cuerpo);
      assert.ok(await esperarHasta(() => efectos.has(a.json.id), 8000));
      await dormir(1500);
      const n0 = await cuantos();
      const entregas0 = entregas;
      const b = await hook(cuerpo);
      assert.equal(b.json.id, a.json.id, "mismo id pasada la ventana");
      assert.equal((await cuantos()) - n0, 1, "pasada la ventana JetStream YA NO deduplica: el mensaje está dos veces en el stream");
      assert.ok(await esperarHasta(() => entregas > entregas0, 8000), "el consumidor recibe la segunda copia");
      await dormir(300);
      assert.equal(efectos.get(a.json.id), 1, "…y aplica UN efecto: la dedupe por id es la que protege, no la ventana");
      await e.eco.ops.jsm.streams.update("EVT_PAGOS", { ...cfg, duplicate_window: 120e9 });
    });

    test("otro hecho del proveedor (otra clave) es otro evento; otro instante con la misma clave, también", async () => {
      const a = await hook(cuerpoDe("evt_a"));
      const b = await hook(cuerpoDe("evt_b"));
      const c = await hook(cuerpoDe("evt_a", { created: 1787221601 }));
      assert.equal(new Set([a.json.id, b.json.id, c.json.id]).size, 3);
    });

    test("dos proveedores con la MISMA clave de evento no chocan: la clave lleva el nombre del hook", async () => {
      const cuerpoGh = JSON.stringify({ hook: { updated_at: "2026-08-20T10:25:39.412Z" }, zen: "x" });
      const gh = await e.llamar("POST", "/v1/hooks/github", {
        cuerpo: cuerpoGh,
        cabeceras: { "x-hub-signature-256": `sha256=${crypto.createHmac("sha256", SECRETOS.GITHUB_HOOK).update(cuerpoGh).digest("hex")}`, "x-github-delivery": "evt_a" },
      });
      assert.equal(gh.status, 202);
      const pas = await hook(cuerpoDe("evt_a", { created: Date.parse("2026-08-20T10:25:39.412Z") / 1000 }));
      assert.notEqual(gh.json.id, pas.json.id, "«github:evt_a» ≠ «pasarela:evt_a»: sin el nombre del hook, mismo source + misma clave + mismo instante darían el MISMO id");
      assert.equal(gh.json.id, externalEventId({ source: SOURCE, externalKey: "github:evt_a", time: "2026-08-20T10:25:39.412Z" }));
    });
  });

  describe("lo que llega a `data`", () => {
    test("solo los campos elegidos (`pick`): el email del pagador NO pasa; clasificación confidential; subject = aggregateId; time = el del hecho", async () => {
      const r = await hook(cuerpoDe("evt_pick"));
      const ev = (await cobros()).find((x) => x.id === r.json.id);
      assert.ok(ev);
      assert.deepEqual(ev.data, { pagoId: "evt_pick", importeCents: 9990, moneda: "EUR" });
      assert.ok(!JSON.stringify(ev).includes("ana@example.com"), "ningún dato personal del cuerpo del tercero llega al evento");
      assert.equal(ev.dataclassification, "confidential");
      assert.equal(ev.subject, "evt_pick");
      assert.equal(ev.time, new Date(1787221600 * 1000).toISOString(), "`time` es el instante del hecho EXTERNO, no el de recepción");
      assert.equal(ev.source, SOURCE);
      assert.equal(Object.keys(ev).at(-1), "data");
    });

    test("`wrapAs` guarda el cuerpo entero bajo una clave (el hook de estilo GitHub), con el instante en ISO", async () => {
      const cuerpoGh = JSON.stringify({ hook: { updated_at: "2026-09-01T08:00:00.123Z" }, action: "push" });
      const r = await e.llamar("POST", "/v1/hooks/github", {
        cuerpo: cuerpoGh,
        cabeceras: { "x-hub-signature-256": `sha256=${crypto.createHmac("sha256", SECRETOS.GITHUB_HOOK).update(cuerpoGh).digest("hex")}`, "x-github-delivery": "gh-1" },
      });
      assert.equal(r.status, 202);
      const ev = (await cobros()).find((x) => x.id === r.json.id);
      assert.deepEqual(ev.data, { payload: { hook: { updated_at: "2026-09-01T08:00:00.123Z" }, action: "push" } });
      assert.equal(ev.time, "2026-09-01T08:00:00.123Z");
    });
  });

  describe("negativas: se rechaza SIN publicar nada", () => {
    const sinPublicar = async (fn) => {
      const n0 = await cuantos();
      const r = await fn();
      await dormir(150);
      assert.equal(await cuantos(), n0, "no se publicó nada");
      return r;
    };

    test("firma inválida: otro secreto, sin cabecera, mal formada, hexadecimal inválido", async () => {
      const cuerpo = cuerpoDe("evt_mala");
      for (const [nombre, mut] of [
        ["otro secreto", () => hook(cuerpo, { secreto: "otro-secreto-que-no-es" })],
        ["sin cabecera", () => e.llamar("POST", "/v1/hooks/pasarela", { cuerpo })],
        ["basura", () => hook(cuerpo, { firma: "basura" })],
        ["sin v1", () => hook(cuerpo, { firma: `t=${ahora()}` })],
        ["v1 corto", () => hook(cuerpo, { firma: `t=${ahora()},v1=abc` })],
        ["v1 en mayúsculas", () => hook(cuerpo, { firma: firmar({ secretos: [SECRETOS.PASARELA_HOOK], t: ahora(), cuerpo }).toUpperCase().replace("T=", "t=").replace(/V1=/g, "v1=") })],
        ["cabecera con otro nombre", () => hook(cuerpo, { cabecera: "x-otra-firma" })],
      ]) {
        const r = await sinPublicar(mut);
        assert.equal(r.status, 401, nombre);
        assert.equal(r.json.error.code, "EDGE_INVALID_WEBHOOK_SIGNATURE", nombre);
        assert.equal(r.json.error.class, "permanent");
      }
    });

    test("el hook de estilo GitHub (`sha256=<hmac>` sobre el cuerpo): sin cabecera, otro secreto, sin el prefijo o cuerpo manipulado, 401 y nada se publica", async () => {
      const cuerpoGh = JSON.stringify({ hook: { updated_at: "2026-09-01T08:00:00.123Z" }, action: "malo" });
      const hmac = (secreto, c = cuerpoGh) => crypto.createHmac("sha256", secreto).update(c).digest("hex");
      const gh = (cabeceras, cuerpo = cuerpoGh) => e.llamar("POST", "/v1/hooks/github", { cuerpo, cabeceras: { "x-github-delivery": "gh-malo", ...cabeceras } });
      for (const [nombre, mut] of [
        ["sin cabecera", () => gh({})],
        ["otro secreto", () => gh({ "x-hub-signature-256": `sha256=${hmac("otro-secreto")}` })],
        ["sin el prefijo sha256=", () => gh({ "x-hub-signature-256": hmac(SECRETOS.GITHUB_HOOK) })],
        ["en mayúsculas", () => gh({ "x-hub-signature-256": `sha256=${hmac(SECRETOS.GITHUB_HOOK).toUpperCase()}` })],
        ["cuerpo manipulado", () => gh({ "x-hub-signature-256": `sha256=${hmac(SECRETOS.GITHUB_HOOK)}` }, `${cuerpoGh} `)],
        ["una firma de t-v1 donde se espera sha256=", () => gh({ "x-hub-signature-256": firmar({ secretos: [SECRETOS.GITHUB_HOOK], t: ahora(), cuerpo: cuerpoGh }) })],
      ]) {
        const r = await sinPublicar(mut);
        assert.equal(r.status, 401, nombre);
        assert.equal(r.json.error.code, "EDGE_INVALID_WEBHOOK_SIGNATURE", nombre);
      }
      // y la buena sí
      assert.equal((await gh({ "x-hub-signature-256": `sha256=${hmac(SECRETOS.GITHUB_HOOK)}` })).status, 202);
    });

    test("cuerpo manipulado tras firmar (un espacio, otro importe): la firma no verifica", async () => {
      const cuerpo = cuerpoDe("evt_manip");
      const firma = firmar({ secretos: [SECRETOS.PASARELA_HOOK], t: ahora(), cuerpo });
      for (const otro of [`${cuerpo} `, cuerpo.replace('"amount":9990', '"amount":1'), cuerpo.replace("EUR", "USD")]) {
        const r = await sinPublicar(() => e.llamar("POST", "/v1/hooks/pasarela", { cuerpo: otro, cabeceras: { "x-pasarela-firma": firma } }));
        assert.equal(r.status, 401);
        assert.equal(r.json.error.code, "EDGE_INVALID_WEBHOOK_SIGNATURE");
      }
    });

    test("firma CORRECTA pero fuera de la tolerancia (una hora por detrás, o por delante): 401 con su propio código; y 300 s dentro sí", async () => {
      const cuerpo = cuerpoDe("evt_tarde");
      for (const delta of [-3600, 3600, -301, 301]) {
        const r = await sinPublicar(() => hook(cuerpo, { t: ahora() + delta }));
        assert.equal(r.status, 401, `t ${delta > 0 ? "+" : ""}${delta}`);
        assert.equal(r.json.error.code, "EDGE_WEBHOOK_TIMESTAMP_OUT_OF_TOLERANCE");
        assert.match(r.json.error.message, /NTP/);
      }
      assert.equal((await hook(cuerpoDe("evt_dentro"), { t: ahora() - 290 })).status, 202);
      assert.equal((await hook(cuerpoDe("evt_dentro2"), { t: ahora() + 290 })).status, 202);
    });

    test("firma válida pero la clave del evento o el instante no están en el cuerpo: 422 (y no se publica)", async () => {
      for (const [cuerpo, re] of [
        [JSON.stringify({ created: 1787221600, amount: 1 }), /clave del evento/],
        [JSON.stringify({ id: "evt_sin_t", amount: 1 }), /instante válido/],
        [JSON.stringify({ id: "evt_t_mala", created: "ayer" }), /instante válido/],
        [JSON.stringify({ id: "", created: 1787221600 }), /clave del evento/],
      ]) {
        const r = await sinPublicar(() => hook(cuerpo));
        assert.equal(r.status, 422, cuerpo);
        assert.equal(r.json.error.code, "EDGE_WEBHOOK_PAYLOAD_INVALID");
        assert.match(r.json.error.message, re);
      }
    });

    test("firma válida pero el cuerpo no es JSON, no es UTF-8 o es un array sin `wrapAs`: 400/422", async () => {
      let r = await sinPublicar(() => hook("{ no es json"));
      assert.deepEqual([r.status, r.json.error.code], [400, "EDGE_INVALID_BODY"]);
      const mal = Buffer.from([0x7b, 0x22, 0x69, 0x64, 0x22, 0x3a, 0x22, 0xff, 0x22, 0x7d]);
      r = await sinPublicar(() => e.llamar("POST", "/v1/hooks/pasarela", { cuerpo: mal, cabeceras: { "x-pasarela-firma": firmar({ secretos: [SECRETOS.PASARELA_HOOK], t: ahora(), cuerpo: mal }) } }));
      assert.deepEqual([r.status, r.json.error.code], [400, "EDGE_INVALID_BODY"]);
    });

    test("la firma se verifica ANTES de interpretar el cuerpo: un cuerpo roto SIN firma válida es 401, no 400 (no se le dice nada a quien no se identifica)", async () => {
      for (const roto of ["{ no es json", "[1,2", Buffer.from([0x7b, 0xff, 0xfe, 0x7d])]) {
        const r = await sinPublicar(() => e.llamar("POST", "/v1/hooks/pasarela", { cuerpo: roto, cabeceras: { "x-pasarela-firma": firmar({ secretos: ["otro-secreto"], t: ahora(), cuerpo: roto }) } }));
        assert.equal(r.status, 401);
        assert.equal(r.json.error.code, "EDGE_INVALID_WEBHOOK_SIGNATURE");
      }
      const gh = await sinPublicar(() => e.llamar("POST", "/v1/hooks/github", { cuerpo: "{ no es json", cabeceras: { "x-hub-signature-256": `sha256=${"0".repeat(64)}`, "x-github-delivery": "x" } }));
      assert.equal(gh.status, 401);
    });

    test("un webhook no lleva token: un Bearer válido de otro cliente no sustituye a la firma", async () => {
      const cuerpo = cuerpoDe("evt_bearer");
      const r = await sinPublicar(() => e.llamar("POST", "/v1/hooks/pasarela", { cuerpo, token: e.tokens["pasarela-web"] }));
      assert.equal(r.status, 401);
      assert.equal(r.json.error.code, "EDGE_INVALID_WEBHOOK_SIGNATURE");
    });

    test("un hook que no existe es 404; con GET, 405; y el cuerpo demasiado grande, 413 sin publicar", async () => {
      assert.equal((await e.llamar("POST", "/v1/hooks/otro", { cuerpo: "{}" })).status, 404);
      assert.equal((await e.llamar("GET", "/v1/hooks/pasarela")).status, 405);
      const grande = JSON.stringify({ id: "evt_grande", created: 1787221600, relleno: "x".repeat(1_100_000) });
      const r = await sinPublicar(() => hook(grande));
      assert.equal(r.status, 413);
      assert.equal(r.json.error.code, "EDGE_BODY_TOO_LARGE");
    });
  });

  test("lo que pasó queda en el registro con el nombre del hook y el id del evento, sin el cuerpo ni la firma", () => {
    const log = e.registroEntero();
    assert.match(log, /"hook":"pasarela"/);
    assert.ok(!log.includes("ana@example.com") && !log.includes(SECRETOS.PASARELA_HOOK) && !log.includes("v1="), "ni el email, ni el secreto, ni la firma");
  });
});
