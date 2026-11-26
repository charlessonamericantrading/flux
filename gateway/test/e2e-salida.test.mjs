/**
 * flux → HTTP, de extremo a extremo — la entrega saliente, specification/12-edge.md §4 (NORMATIVO).
 *
 * Un servicio nativo (`inventario-api`) publica un evento; el gateway, que aloja a `tienda-web` con su durable y su ACL generada, lo
 * entrega por HTTP a un receptor de prueba que VERIFICA la firma como lo haría un tercero. Contra un nats-server REAL. Se comprueba
 * el formato (el CloudEvent tal cual, `Idempotency-Key`, `Flux-Signature` con `t` renovado en cada intento) y toda la taxonomía:
 * 2xx ack, 503/429 con `Retry-After` reintentan, un 4xx permanente va a la DLQ sin reintentos, un 500 tiene presupuesto de 2, una
 * redirección no se sigue, `restricted` nunca sale y `confidential` solo con autorización explícita.
 *
 * Sin `nats-server` estas pruebas se SALTAN con aviso, salvo con FLUX_REQUIRE_BROKER=1, donde FALLAN.
 */

import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import http from "node:http";
import { readFileSync } from "node:fs";

import { SECRETOS, VARIABLES, crearReceptor, describeBroker, dormir, esperarHasta, levantarTodo, mensajesDe } from "./helpers/entorno.mjs";

const protocolo = JSON.parse(readFileSync(new URL("../../protocol.json", import.meta.url), "utf8"));
const PATRON_FIRMA = new RegExp(protocolo.edge.delivery.signature.pattern);
const CANARIO = "CANARIO-entrega-4d2f8b1a";
const SUBJECT = "inventario.stock.v1.ajustado";

describeBroker(describe, test, "entrega saliente: flux → HTTP firmado", () => {
  let e;
  let inv; // el servicio nativo que publica
  let n = 0;

  before(async () => {
    e = await levantarTodo();
    inv = await e.nativo("inventario-api");
  });
  after(async () => {
    await inv?.close().catch(() => {});
    await e?.parar();
  });

  const publicar = (data = {}, opciones = {}) => inv.publish(SUBJECT, { sku: `sku-${++n}`, delta: 1, aggregateVersion: 1, ...data }, { aggregateId: `sku-${n}`, ...opciones });
  const recibidas = (id) => e.receptor.peticiones.filter((p) => p.idempotencyKey === id);
  const dlq = async () => mensajesDe(e.eco.ops.jsm, "DLQ_INVENTARIO");
  const enLaDlq = async (id) => (await dlq()).find((x) => x.id === id);
  const durable = "tienda-web__inventario_stock_v1_ajustado";
  const pendientes = async () => {
    const i = await e.eco.ops.jsm.consumers.info("EVT_INVENTARIO", durable);
    return { ackPending: i.num_ack_pending, pending: i.num_pending };
  };

  describe("el formato (normativo)", () => {
    test("POST, `application/cloudevents+json`, el CloudEvent TAL CUAL, `Idempotency-Key` = id y `Flux-Signature` que verifica", async () => {
      const ev = await publicar({ almacen: "Málaga" });
      assert.ok(await esperarHasta(() => recibidas(ev.id).length >= 1, 10_000), "llegó la entrega");
      const p = recibidas(ev.id)[0];

      assert.equal(p.metodo, "POST");
      assert.equal(p.cabeceras["content-type"], "application/cloudevents+json; charset=utf-8");
      assert.equal(p.idempotencyKey, ev.id, "Idempotency-Key es el id del evento");
      assert.equal(p.cuerpo.toString("utf8"), JSON.stringify(ev), "el cuerpo es el evento publicado, byte a byte (01-envelope.md §6)");
      assert.equal(Object.keys(JSON.parse(p.cuerpo)).at(-1), "data", "`data` el último atributo");
      assert.ok(!/dlq/.test(p.cuerpo.toString()), "sin extensiones dlq*");
      assert.ok(p.cuerpo.toString("utf8").includes('"almacen":"Málaga"') && !p.cuerpo.toString("utf8").includes("\\u00e1"), "UTF-8 literal, sin escapes \\u");

      // la firma: formato, y verifica con el código de referencia del receptor…
      const firma = p.cabeceras["flux-signature"];
      assert.match(firma, PATRON_FIRMA);
      assert.deepEqual(p.verificacion.ok, true);
      // …y con una implementación INDEPENDIENTE del gateway (node:crypto a pelo)
      const t = /^t=(\d+),/.exec(firma)[1];
      const esperado = crypto.createHmac("sha256", SECRETOS.TIENDA_ENTREGA).update(`${t}.`).update(p.cuerpo).digest("hex");
      assert.equal(firma, `t=${t},v1=${esperado}`);
      assert.ok(Math.abs(Number(t) - Date.now() / 1000) < 30, "`t` es el del momento del intento");
    });

    test("un 2xx es ack: el evento no se reentrega y no hay nada pendiente", async () => {
      const ev = await publicar();
      assert.ok(await esperarHasta(() => recibidas(ev.id).length >= 1, 10_000));
      assert.ok(await esperarHasta(async () => (await pendientes()).ackPending === 0, 5000));
      await dormir(500);
      assert.equal(recibidas(ev.id).length, 1);
      assert.equal(await enLaDlq(ev.id), undefined);
    });

    test("el mismo evento reintentado lleva los MISMOS bytes, la MISMA `Idempotency-Key` y un `t` RENOVADO (503 + Retry-After: 1)", async () => {
      e.receptor.encola({ status: 503, headers: { "retry-after": "1" } });
      const ev = await publicar();
      assert.ok(await esperarHasta(() => recibidas(ev.id).length >= 2, 15_000), "el reintento llegó");
      const [a, b] = recibidas(ev.id);
      assert.equal(a.cabeceras["flux-signature"] === b.cabeceras["flux-signature"], false, "otra firma");
      assert.ok(a.cuerpo.equals(b.cuerpo), "los MISMOS bytes");
      assert.equal(a.idempotencyKey, b.idempotencyKey);
      const ta = Number(/^t=(\d+)/.exec(a.cabeceras["flux-signature"])[1]);
      const tb = Number(/^t=(\d+)/.exec(b.cabeceras["flux-signature"])[1]);
      assert.ok(tb > ta, `t se renueva en cada intento (${ta} → ${tb}): con un t fijo, la tolerancia rechazaría los reintentos de 51 min`);
      assert.ok(b.recibidoMs - a.recibidoMs >= 900, `el Retry-After de 1 s se respetó en el primer reintento (A-158): ${b.recibidoMs - a.recibidoMs} ms`);
      assert.ok(a.verificacion.ok && b.verificacion.ok, "las dos verifican en su momento");
      assert.ok(await esperarHasta(async () => (await pendientes()).ackPending === 0, 5000));
      assert.equal(await enLaDlq(ev.id), undefined, "se recuperó: no va a la DLQ");
    });

    test("429 con Retry-After es RETRYABLE (presupuesto completo): se reintenta y se recupera", async () => {
      e.receptor.encola({ status: 429, headers: { "retry-after": "1" } });
      const ev = await publicar();
      assert.ok(await esperarHasta(() => recibidas(ev.id).length >= 2, 15_000));
      assert.ok(await esperarHasta(async () => (await pendientes()).ackPending === 0, 5000));
      assert.equal(await enLaDlq(ev.id), undefined);
    });
  });

  describe("la taxonomía: lo permanente va a la DLQ sin reintentos", () => {
    for (const [status, headers] of [[400, {}], [401, {}], [403, {}], [404, {}], [410, {}], [422, {}]]) {
      test(`un ${status} del receptor es PERMANENT: UNA entrega y a la DLQ, sin reintentos (A-159)`, async () => {
        e.receptor.encola({ status, headers });
        const ev = await publicar({ almacen: "Málaga" });
        assert.ok(await esperarHasta(() => recibidas(ev.id).length >= 1, 10_000));
        const m = await (async () => {
          assert.ok(await esperarHasta(async () => await enLaDlq(ev.id), 10_000), "llegó a la DLQ");
          return enLaDlq(ev.id);
        })();
        assert.equal(m.dlqreason, "permanent");
        assert.equal(m.dlqattempts, 1);
        assert.match(m.dlqerror, new RegExp(`^HTTP_${status}: `));
        assert.equal(m.dlqconsumer, durable);
        assert.equal(m.id, ev.id, "la DLQ conserva el id original");
        assert.equal(Object.keys(m).at(-1), "data");
        assert.ok(!m.dlqerror.includes("Málaga"), "el mensaje de la DLQ no cita `data`");
        await dormir(800);
        assert.equal(recibidas(ev.id).length, 1, "UNA sola entrega: nada de reintentos");
      });
    }

    test("un 302 NO se sigue: es PERMANENT, y la URL de la redirección no recibe nada", async () => {
      let visitas = 0;
      const otro = http.createServer((q, r) => { visitas++; r.end(); });
      await new Promise((r) => otro.listen(0, "127.0.0.1", r));
      try {
        e.receptor.encola({ status: 302, headers: { location: `http://127.0.0.1:${otro.address().port}/robado` } });
        const ev = await publicar();
        assert.ok(await esperarHasta(async () => await enLaDlq(ev.id), 10_000));
        const m = await enLaDlq(ev.id);
        assert.equal(m.dlqreason, "permanent");
        assert.match(m.dlqerror, /^HTTP_302: /);
        await dormir(500);
        assert.equal(visitas, 0, "seguirla enviaría el cuerpo firmado a una URL que el operador no configuró");
      } finally {
        otro.closeAllConnections();
        await new Promise((r) => otro.close(r));
      }
    });

    test("un 500 tiene presupuesto ACOTADO de 2 entregas (no es un 503): dos intentos y a la DLQ como retryable (A-160)", async () => {
      e.receptor.encola({ status: 500, headers: { "retry-after": "1" } }, { status: 500, headers: {} });
      const ev = await publicar();
      assert.ok(await esperarHasta(async () => await enLaDlq(ev.id), 20_000), "llegó a la DLQ");
      const m = await enLaDlq(ev.id);
      assert.equal(m.dlqreason, "retryable", "agotó su presupuesto");
      assert.equal(m.dlqattempts, 2);
      assert.match(m.dlqerror, /^HTTP_500: /);
      await dormir(500);
      assert.equal(recibidas(ev.id).length, 2);
    });
  });

  describe("lo que sale del ecosistema se clasifica (12-edge.md §4.7)", () => {
    test("un evento `confidential` NO se entrega si la entrega no lo autoriza: PERMANENT, el receptor no recibe nada, y la DLQ no cita `data`", async () => {
      const ev = await publicar({ nota: CANARIO }, { classification: "confidential" });
      assert.ok(await esperarHasta(async () => await enLaDlq(ev.id), 10_000));
      const m = await enLaDlq(ev.id);
      assert.equal(m.dlqreason, "permanent");
      assert.equal(m.dlqattempts, 1);
      assert.match(m.dlqerror, /^DELIVERY_CLASSIFICATION_DENIED: /);
      assert.match(m.dlqerror, /allowClassifications/, "dice cómo se arregla");
      assert.ok(!m.dlqerror.includes(CANARIO));
      assert.equal(recibidas(ev.id).length, 0, "el dato NO salió del ecosistema");
      assert.ok(!e.registroEntero().includes(CANARIO), "ni a los registros");
    });

    test("un evento `restricted` no sale NUNCA por HTTP", async () => {
      const ev = await publicar({ nota: CANARIO }, { classification: "restricted" });
      assert.ok(await esperarHasta(async () => await enLaDlq(ev.id), 10_000));
      const m = await enLaDlq(ev.id);
      assert.match(m.dlqerror, /^DELIVERY_CLASSIFICATION_DENIED: .*restricted/);
      assert.equal(recibidas(ev.id).length, 0);
    });
  });
});

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Otros entornos: confidential autorizado, estado hacia HTTP, rotación de secretos, plazo y receptor caído.
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════

describeBroker(describe, test, "entrega saliente: confidential autorizado, estado, rotación y fallos de red", () => {
  let e;
  let inv;
  let receptorEstado;
  const NUEVO = "whsec_entrega_nueva_9876543210fedcba";

  before(async () => {
    // El receptor SOLO conoce el secreto NUEVO: la entrega firma con los dos (rotación sin corte) y basta con uno.
    const receptor = await crearReceptor({ secretos: [NUEVO] });
    receptorEstado = await crearReceptor({ secretos: [SECRETOS.TIENDA_ENTREGA] });
    e = await levantarTodo({
      receptor,
      ajusta(cfg, { env }) {
        env.TIENDA_WEB_ENTREGA_SECRETO_NUEVO = NUEVO;
        const c = cfg.clients[0];
        c.deliveries = [
          { subject: SUBJECT, url: receptor.url, secretEnv: [VARIABLES.TIENDA_ENTREGA, "TIENDA_WEB_ENTREGA_SECRETO_NUEVO"], allowInsecureHttp: true, allowClassifications: ["public", "internal", "confidential"], timeoutMs: 2000 },
          { contract: "catalogo.producto.v1", tenant: "acme", url: receptorEstado.url, secretEnv: VARIABLES.TIENDA_ENTREGA, allowInsecureHttp: true, allowClassifications: ["public", "internal", "confidential"], timeoutMs: 2000 },
        ];
      },
    });
    inv = await e.nativo("inventario-api");
  });
  after(async () => {
    await inv?.close().catch(() => {});
    await e?.parar();
    await receptorEstado?.cerrar();
    await e?.receptor.cerrar().catch(() => {});
  });

  test("un `confidential` AUTORIZADO sale, firmado con los dos secretos (rotación); el receptor, que solo conoce el nuevo, verifica; y el canario no está en ningún registro", async () => {
    const ev = await inv.publish(SUBJECT, { sku: "c-1", nota: CANARIO, aggregateVersion: 1 }, { aggregateId: "c-1", classification: "confidential" });
    assert.ok(await esperarHasta(() => e.receptor.peticiones.some((p) => p.idempotencyKey === ev.id), 10_000));
    const p = e.receptor.peticiones.find((x) => x.idempotencyKey === ev.id);
    assert.equal(p.cabeceras["flux-signature"].match(/v1=/g).length, 2, "un v1 por secreto vigente");
    assert.equal(p.verificacion.ok, true, "el receptor solo conoce el secreto nuevo y verifica igual");
    assert.ok(p.cuerpo.toString().includes(CANARIO), "el dato SÍ llega al receptor (está autorizado)");
    assert.ok(!e.registroEntero().includes(CANARIO), "…pero no queda en ningún registro del gateway (A-163)");
    assert.ok(!e.registroEntero().includes(NUEVO) && !e.registroEntero().includes(SECRETOS.TIENDA_ENTREGA), "ni los secretos");
  });

  test("el ESTADO hacia HTTP: la entrada, firmada, con `Idempotency-Key` = contrato/tenant/clave@revisión; una lápida llega con `deleted: true`", async () => {
    const put = await e.llamar("PUT", "/v1/state/catalogo.producto.v1/est-1", { token: e.tokens["catalogo-web"], cuerpo: { productoId: "est-1", nombre: "Lámpara", aggregateVersion: 1 } });
    assert.equal(put.status, 200);
    assert.ok(await esperarHasta(() => receptorEstado.peticiones.some((p) => p.idempotencyKey === `catalogo.producto.v1/acme/est-1@${put.json.revision}`), 10_000));
    const p = receptorEstado.peticiones.find((x) => x.idempotencyKey === `catalogo.producto.v1/acme/est-1@${put.json.revision}`);
    assert.equal(p.cabeceras["content-type"], "application/json; charset=utf-8", "NO es un CloudEvent: no es normativo (12-edge.md §4.8)");
    assert.equal(p.verificacion.ok, true);
    assert.deepEqual(JSON.parse(p.cuerpo), { contract: "catalogo.producto.v1", tenantId: "acme", key: "est-1", deleted: false, aggregateVersion: 1, revision: put.json.revision, data: { productoId: "est-1", nombre: "Lámpara", aggregateVersion: 1 } });

    const del = await e.llamar("DELETE", "/v1/state/catalogo.producto.v1/est-1?aggregateVersion=2", { token: e.tokens["catalogo-web"] });
    assert.equal(del.status, 200);
    assert.ok(await esperarHasta(() => receptorEstado.peticiones.some((x) => x.idempotencyKey === `catalogo.producto.v1/acme/est-1@${del.json.revision}`), 10_000));
    const l = JSON.parse(receptorEstado.peticiones.find((x) => x.idempotencyKey === `catalogo.producto.v1/acme/est-1@${del.json.revision}`).cuerpo);
    assert.equal(l.deleted, true);
    assert.equal(l.data, null);
  });
});

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Fallos de RED: un receptor mudo (plazo total) y uno caído (conexión rechazada). Son RETRYABLE: no van a la DLQ.
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════

describeBroker(describe, test, "entrega saliente: fallos de red", () => {
  const casos = [
    { nombre: "un receptor que NO CONTESTA: el plazo TOTAL corta el intento (TIMEOUT)", code: "TIMEOUT", prepara: async () => { const s = http.createServer(() => {}); await new Promise((r) => s.listen(0, "127.0.0.1", r)); return { url: `http://127.0.0.1:${s.address().port}/h`, cierra: async () => { s.closeAllConnections(); await new Promise((r) => s.close(r)); } }; } },
    { nombre: "un receptor CAÍDO (conexión rechazada): ECONNREFUSED", code: "ECONNREFUSED", prepara: async () => { const s = http.createServer(); await new Promise((r) => s.listen(0, "127.0.0.1", r)); const url = `http://127.0.0.1:${s.address().port}/h`; await new Promise((r) => s.close(r)); return { url, cierra: async () => {} }; } },
  ];
  for (const caso of casos) {
    test(`${caso.nombre}: RETRYABLE, el evento NO va a la DLQ y espera su backoff`, async () => {
      const destino = await caso.prepara();
      const e = await levantarTodo({
        ajusta(cfg) {
          cfg.clients[0].deliveries = [{ subject: SUBJECT, url: destino.url, secretEnv: VARIABLES.TIENDA_ENTREGA, allowInsecureHttp: true, timeoutMs: 500 }];
        },
      });
      const inv = await e.nativo("inventario-api");
      try {
        const t0 = Date.now();
        const ev = await inv.publish(SUBJECT, { sku: "red-1", aggregateVersion: 1 }, { aggregateId: "red-1" });
        const fila = () => e.lineas.map((l) => JSON.parse(l)).find((f) => f.mensaje === "entrega fallida" && f.code === caso.code);
        assert.ok(await esperarHasta(() => fila(), 10_000), `el registro dice «entrega fallida» con code ${caso.code}`);
        if (caso.code === "TIMEOUT") assert.ok(Date.now() - t0 >= 450, "tardó el plazo");
        assert.equal(fila().cliente, "tienda-web");
        assert.ok(!e.registroEntero().includes("/h?"), "no se registra la URL entera, solo el host");
        await dormir(500);
        assert.deepEqual(await mensajesDe(e.eco.ops.jsm, "DLQ_INVENTARIO"), [], "es reintentable: no va a la DLQ");
        const i = await e.eco.ops.jsm.consumers.info("EVT_INVENTARIO", "tienda-web__inventario_stock_v1_ajustado");
        assert.equal(i.num_ack_pending + i.num_pending >= 1, true, "el evento sigue pendiente de reentrega");
        void ev;
      } finally {
        await inv.close().catch(() => {});
        await e.parar();
        await destino.cierra();
      }
    });
  }
});
