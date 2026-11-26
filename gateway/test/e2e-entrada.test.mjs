/**
 * HTTP → flux, de extremo a extremo: el gateway arrancado DE VERDAD contra un nats-server REAL, con los permisos que GENERA
 * `scripts/gen-acl.mjs` para los clientes de borde (`"via": "gateway"`) y los streams de `flux provision`. Nada de mocks.
 *
 * Es el criterio de aceptación de la Fase 4 (un script con `curl` publica un evento, escribe y lee estado y hace una llamada) y,
 * en negativo, todo lo que el gateway debe RECHAZAR: un token malo, un token válido sobre un recurso que el broker no concede, un
 * cuerpo demasiado grande, `confidential` en los registros…
 *
 * Sin `nats-server` estas pruebas se SALTAN con aviso, salvo con FLUX_REQUIRE_BROKER=1, donde FALLAN.
 */

import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { externalEventId } from "@flux/sdk";

import { SECRETOS, dormir, describeBroker, esperarHasta, levantarTodo, mensajesDe } from "./helpers/entorno.mjs";

const AQUI = path.dirname(fileURLToPath(import.meta.url));
const DEMO = path.join(AQUI, "..", "examples", "demo.sh");
const CANARIO = "CANARIO-confidencial-7c1e9a4b";

describeBroker(describe, test, "gateway de extremo a extremo: HTTP → flux", () => {
  let e;
  let precios; // el dueño NATIVO de `precios.tarifa.v1.calcular`
  let ejecuciones; // cuántas veces se ejecutó la operación
  // El broker anota cada denegación: `[ERR] … "v3.4.0:nats.js:catalogo-web@produccion" … Publish Violation - Subject "rpc.…"`.
  const violaciones = () => e.eco.servidor.salida.split("\n").filter((l) => /\b(?:Publish|Subscription) Violation\b/.test(l));

  before(async () => {
    e = await levantarTodo({
      ajusta(cfg, { env }) {
        // tienda-web acepta un segundo tenant que el BROKER no le concede: lo que el gateway permite y el broker no.
        cfg.clients[0].tenants = ["acme", "globex"];
        // un cliente con un límite propio pequeño, para probar el 429 sin cargar al resto
        cfg.clients[1].rateLimit = { requests: 1000, perSeconds: 60 };
      },
    });
    ejecuciones = [];
    precios = await e.nativo("precios-api");
    await precios.respond("precios.tarifa.v1.calcular", async (req, ctx) => {
      ejecuciones.push(ctx.id);
      if (req.modo === "lento") await dormir(req.ms ?? 1500);
      if (req.modo === "permanente") throw new (await import("@flux/sdk")).PermanentError("la tarifa no existe para ese SKU", { code: "TARIFA_INEXISTENTE" });
      if (req.modo === "reintentable") throw new (await import("@flux/sdk")).RetryableError("el proveedor de tarifas no responde", { code: "HTTP_503", retryAfterMs: 2000 });
      return { totalCents: (req.cantidad ?? 1) * 100, secreto: req.secreto };
    });
  });
  after(async () => {
    await precios?.close().catch(() => {});
    await e?.parar();
  });

  const t = () => e.tokens;
  const eventosEn = async (stream) => (await mensajesDe(e.eco.ops.jsm, stream));

  // ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
  describe("aceptación: un script con `curl`", () => {
    test("publica un evento, escribe y lee estado, hace una llamada, y un webhook ×3 produce UN efecto", async () => {
      // El consumidor del ecosistema que aplica el efecto de los cobros: idempotente por `id`.
      const conta = await e.nativo("contabilidad-api");
      const vistos = new Set();
      let entregas = 0;
      let efectos = 0;
      await conta.subscribe("pagos.cobro.v1.recibido", async (ev) => {
        entregas++;
        if (vistos.has(ev.id)) return;
        vistos.add(ev.id);
        efectos++;
      });

      const r = await new Promise((resolve) => {
        const hijo = spawn("bash", [DEMO], {
          env: { ...process.env, GATEWAY_URL: e.url, TOKEN_TIENDA: t()["tienda-web"], TOKEN_CATALOGO: t()["catalogo-web"], PASARELA_SECRETO: SECRETOS.PASARELA_HOOK },
          stdio: ["ignore", "pipe", "pipe"],
        });
        let out = "";
        let err = "";
        hijo.stdout.on("data", (d) => (out += d));
        hijo.stderr.on("data", (d) => (err += d));
        hijo.on("close", (codigo) => resolve({ codigo, out, err }));
      });
      assert.equal(r.codigo, 0, `el script de curl falló:\n${r.out}\n${r.err}`);
      assert.match(r.out, /✓ aceptación/);

      // 1. el evento llegó al stream, con el sobre que puso el SDK (el script no lo escribió)
      const pedidos = await eventosEn("EVT_PEDIDOS");
      const ev = pedidos.find((x) => x.data?.pedidoId === "ped-123");
      assert.ok(ev, "el evento publicado por curl está en EVT_PEDIDOS");
      assert.equal(ev.source, "/produccion/tienda-web");
      assert.equal(ev.type, "com.flux.pedidos.pedido.creado.v1");
      assert.equal(ev.subject, "ped-123");
      assert.equal(ev.tenantid, "acme");
      assert.match(ev.id, /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
      assert.equal(Object.keys(ev).at(-1), "data", "`data` el último atributo");

      // 2. el estado: lo escribió catalogo-web (dueño) y lo lee tienda-web (observador de acme)
      assert.match(r.out, /== leer estado -> 200\n\{"key":"prod-1","tenantId":"acme","deleted":false,"data":\{"productoId":"prod-1","nombre":"Mesa de roble","aggregateVersion":1\}/);

      // 3. la llamada se atendió, y la no concedida la denegó el broker
      assert.match(r.out, /== llamar a una operación -> 200\n\{"totalCents":300\}/);
      assert.match(r.out, /== una operación no concedida \(la deniega el broker\) -> 403\n\{"error":\{"code":"PERMISSION_DENIED"/);
      assert.ok(violaciones().some((l) => l.includes("rpc.precios.tarifa.v1.calcular")), "el broker registró la violación de permiso de catalogo-web");
      assert.equal(ejecuciones.length >= 1, true);

      // 4. el webhook ×3: tres 202 con el MISMO id, y UN efecto en el consumidor
      const ids = [...r.out.matchAll(/== webhook de la pasarela, envío \d -> 202\n\{"id":"([^"]+)"\}/g)].map((m) => m[1]);
      assert.equal(ids.length, 3);
      assert.equal(new Set(ids).size, 1, "el mismo webhook reenviado da el mismo id");
      assert.equal(ids[0], externalEventId({ source: "/produccion/pasarela-web", externalKey: "pasarela:evt_demo_1", time: new Date(1787221600 * 1000).toISOString() }), "el id es el determinista de la spec, no uno aleatorio");
      assert.ok(await esperarHasta(() => entregas >= 1, 8000), "el consumidor recibe el evento");
      await dormir(500);
      assert.equal(efectos, 1, "UN efecto");
      const cobros = (await eventosEn("EVT_PAGOS")).filter((x) => x.id === ids[0]);
      assert.equal(cobros.length, 1, "dentro de duplicate_window JetStream guarda UN mensaje (A-157)");
      // y lo que llegó no lleva el email del pagador: el hook elige los campos (`pick`)
      assert.deepEqual(cobros[0].data, { pagoId: "evt_demo_1", importeCents: 9990, moneda: "EUR" });
      assert.equal(cobros[0].dataclassification, "confidential", "un hook es confidential por defecto");
      await conta.close();
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
  describe("eventos", () => {
    test("`POST /v1/events/{subject}` publica; la respuesta es 202 { id } y el id es el del evento", async () => {
      const r = await e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { token: t()["tienda-web"], cuerpo: { pedidoId: "ped-ev-1", aggregateVersion: 1 }, cabeceras: { "flux-aggregate-id": "ped-ev-1", "flux-classification": "confidential" } });
      assert.equal(r.status, 202);
      assert.deepEqual(Object.keys(r.json), ["id"]);
      const ev = (await eventosEn("EVT_PEDIDOS")).find((x) => x.id === r.json.id);
      assert.ok(ev);
      assert.equal(ev.dataclassification, "confidential");
      assert.equal(ev.subject, "ped-ev-1");
    });

    test("`Idempotency-Key` + `Flux-Event-Time` dan un id DETERMINISTA: quien reintenta no duplica", async () => {
      const cab = { "idempotency-key": "ped-77-intento", "flux-event-time": "2026-08-20T10:25:39.412Z" };
      const a = await e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { token: t()["tienda-web"], cuerpo: { pedidoId: "ped-77", aggregateVersion: 1 }, cabeceras: cab });
      const b = await e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { token: t()["tienda-web"], cuerpo: { pedidoId: "ped-77", aggregateVersion: 1 }, cabeceras: cab });
      assert.equal(a.status, 202);
      assert.equal(a.json.id, b.json.id);
      assert.equal(a.json.id, externalEventId({ source: "/produccion/tienda-web", externalKey: "pedidos.pedido.v1.creado:ped-77-intento", time: "2026-08-20T10:25:39.412Z" }));
      assert.equal((await eventosEn("EVT_PEDIDOS")).filter((x) => x.id === a.json.id).length, 1);
    });

    test("`Idempotency-Key` sin `Flux-Event-Time` se rechaza: sin el instante del hecho el id no es estable", async () => {
      const r = await e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { token: t()["tienda-web"], cuerpo: { pedidoId: "x" }, cabeceras: { "idempotency-key": "k" } });
      assert.equal(r.status, 400);
      assert.equal(r.json.error.code, "EDGE_INVALID_REQUEST");
      assert.match(r.json.error.message, /Flux-Event-Time/);
    });

    test("las cabeceras `Flux-*` mal formadas dan 400 con el motivo", async () => {
      for (const [cab, re] of [
        [{ "flux-classification": "secreto" }, /Flux-Classification/],
        [{ "flux-event-time": "ayer" }, /3 decimales/],
        [{ "flux-event-time": "2026-08-20T10:25:39Z" }, /3 decimales/],
        [{ "flux-event-time": "2026-13-45T10:25:39.412Z" }, /3 decimales/],
      ]) {
        const r = await e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { token: t()["tienda-web"], cuerpo: { pedidoId: "x" }, cabeceras: cab });
        assert.equal(r.status, 400, JSON.stringify(cab));
        assert.equal(r.json.error.code, "EDGE_INVALID_REQUEST");
        assert.match(r.json.error.message, re);
      }
    });

    test("un subject mal formado o de un dominio reservado es un 400 con un código", async () => {
      let r = await e.llamar("POST", "/v1/events/pedidos.crear", { token: t()["tienda-web"], cuerpo: { x: 1 } });
      assert.equal(r.status, 400);
      assert.equal(r.json.error.code, "EDGE_INVALID_REQUEST");
      r = await e.llamar("POST", "/v1/events/state.clientes.cliente.v1", { token: t()["tienda-web"], cuerpo: { x: 1 } });
      assert.equal(r.status, 400);
      assert.equal(r.json.error.code, "RESERVED_DOMAIN");
      r = await e.llamar("POST", "/v1/events/Pedidos.pedido.v1.creado", { token: t()["tienda-web"], cuerpo: { x: 1 } });
      assert.equal(r.status, 400);
      r = await e.llamar("POST", "/v1/events/%E0%A4%A", { token: t()["tienda-web"], cuerpo: { x: 1 } });
      assert.equal(r.status, 400, "un porcentaje mal formado");
    });

    test("un evento con `restricted` por HTTP se publica como el SDK lo admite (no es del gateway decidirlo): la entrada no es la salida", async () => {
      const r = await e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { token: t()["tienda-web"], cuerpo: { pedidoId: "ped-r" }, cabeceras: { "flux-classification": "restricted" } });
      assert.equal(r.status, 202);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
  describe("estado", () => {
    const ruta = (k) => `/v1/state/catalogo.producto.v1/${k}`;
    const put = (k, version, extra = {}, token = t()["catalogo-web"]) =>
      e.llamar("PUT", ruta(k), { token, cuerpo: { productoId: k, nombre: `Producto ${version}`, aggregateVersion: version, ...extra } });

    test("el ciclo: put, put obsoleto (stale, no es un error), put nuevo, get, delete, get (404), recrear con versión mayor", async () => {
      assert.deepEqual((await put("ciclo", 1)).json, { applied: true, outcome: "ok", revision: (await e.llamar("GET", ruta("ciclo"), { token: t()["tienda-web"] })).json.revision });
      const stale = await put("ciclo", 1);
      assert.equal(stale.status, 200, "un put obsoleto NO es un error");
      assert.equal(stale.json.applied, false);
      assert.equal(stale.json.outcome, "stale");
      assert.equal((await put("ciclo", 2)).json.outcome, "ok");

      const leido = await e.llamar("GET", ruta("ciclo"), { token: t()["tienda-web"] });
      assert.equal(leido.status, 200);
      assert.equal(leido.json.aggregateVersion, 2);
      assert.equal(leido.json.data.nombre, "Producto 2");
      assert.equal(leido.json.deleted, false);

      const borrado = await e.llamar("DELETE", `${ruta("ciclo")}?aggregateVersion=3`, { token: t()["catalogo-web"] });
      assert.equal(borrado.status, 200);
      assert.equal(borrado.json.outcome, "ok");
      const tras = await e.llamar("GET", ruta("ciclo"), { token: t()["tienda-web"] });
      assert.equal(tras.status, 404, "una lápida lee como ausente");
      assert.equal(tras.json.error.code, "EDGE_NOT_FOUND");
      assert.equal((await put("ciclo", 3)).json.outcome, "stale", "recrear con la versión de la lápida es obsoleto");
      assert.equal((await put("ciclo", 4)).json.outcome, "ok", "recrear con una versión mayor que la de la lápida");
      assert.equal((await e.llamar("GET", ruta("ciclo"), { token: t()["tienda-web"] })).json.data.nombre, "Producto 4");
    });

    test("una clave que nunca existió es 404", async () => {
      assert.equal((await e.llamar("GET", ruta("nunca-existio"), { token: t()["tienda-web"] })).status, 404);
    });

    test("quien no es dueño del contrato no escribe: lo deniega el BROKER (tienda-web con su token válido)", async () => {
      const antes = violaciones().length;
      const r = await put("ajeno", 1, {}, t()["tienda-web"]);
      assert.equal(r.status, 403);
      assert.equal(r.json.error.code, "PERMISSION_DENIED");
      assert.ok(violaciones().length > antes, "el broker anotó la denegación: no fue solo el gateway");
      assert.equal((await e.llamar("GET", ruta("ajeno"), { token: t()["catalogo-web"] })).status, 404, "y no se escribió nada");
    });

    test("el tenant: `Flux-Tenant` solo si el cliente lo tiene declarado (gateway); y si el gateway lo admite pero el broker no lo concede, lo deniega el BROKER", async () => {
      // catalogo-web: tenants [acme, globex]. initech no está: lo rechaza el gateway.
      let r = await e.llamar("PUT", ruta("tenants"), { token: t()["catalogo-web"], cuerpo: { productoId: "tenants", aggregateVersion: 1 }, cabeceras: { "flux-tenant": "initech" } });
      assert.equal(r.status, 403);
      assert.equal(r.json.error.code, "EDGE_TENANT_NOT_ALLOWED");
      assert.match(r.json.error.message, /acme, globex/);
      // globex sí lo tiene declarado: el dueño lo escribe en globex
      r = await e.llamar("PUT", ruta("g1"), { token: t()["catalogo-web"], cuerpo: { productoId: "g1", nombre: "Solo globex", aggregateVersion: 1 }, cabeceras: { "flux-tenant": "globex" } });
      assert.equal(r.status, 200);
      // tienda-web: el gateway admite globex (config de esta prueba) pero el manifiesto la confina a acme: lo deniega el broker.
      const antes = violaciones().length;
      r = await e.llamar("GET", ruta("g1"), { token: t()["tienda-web"], cabeceras: { "flux-tenant": "globex" } });
      assert.equal(r.status, 403);
      assert.equal(r.json.error.code, "PERMISSION_DENIED", "el broker confina `watches` por tenant (A-14, A-38)");
      assert.ok(violaciones().length > antes, "con una violación en el log del broker");
      // …y en acme no ve lo de globex
      assert.equal((await e.llamar("GET", ruta("g1"), { token: t()["tienda-web"] })).status, 404);
    });

    test("lo mal formado: clave con punto, sin aggregateVersion, DELETE sin versión", async () => {
      let r = await e.llamar("PUT", ruta("a.b"), { token: t()["catalogo-web"], cuerpo: { aggregateVersion: 1 } });
      assert.equal(r.status, 400);
      assert.equal(r.json.error.code, "INVALID_STATE_KEY");
      r = await e.llamar("PUT", ruta("sinversion"), { token: t()["catalogo-web"], cuerpo: { productoId: "x" } });
      assert.equal(r.status, 422);
      assert.equal(r.json.error.code, "MISSING_AGGREGATE_VERSION");
      r = await e.llamar("DELETE", ruta("x"), { token: t()["catalogo-web"] });
      assert.equal(r.status, 400);
      assert.match(r.json.error.message, /aggregateVersion/);
      r = await e.llamar("PUT", "/v1/state/catalogo.producto/x", { token: t()["catalogo-web"], cuerpo: { aggregateVersion: 1 } });
      assert.equal(r.status, 400);
      assert.equal(r.json.error.code, "INVALID_STATE_CONTRACT");
      r = await e.llamar("PUT", "/v1/state/state.producto.v1/x", { token: t()["catalogo-web"], cuerpo: { aggregateVersion: 1 } });
      assert.equal(r.json.error.code, "RESERVED_DOMAIN");
      assert.equal((await e.llamar("PATCH", ruta("x"), { token: t()["catalogo-web"], cuerpo: {} })).status, 405);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
  describe("peticiones (rpc)", () => {
    const rpc = (cuerpo, cabeceras = {}, token = t()["tienda-web"]) => e.llamar("POST", "/v1/rpc/precios.tarifa.v1.calcular", { token, cuerpo, cabeceras });

    test("200 con el `data` de la respuesta", async () => {
      const r = await rpc({ cantidad: 4 });
      assert.equal(r.status, 200);
      assert.equal(r.json.totalCents, 400);
    });

    test("una llamada que tarda MÁS de 1 s no la mata el `server.timeout` (mide inactividad del socket mientras se espera al broker: A-153)", async () => {
      const t0 = Date.now();
      const r = await rpc({ modo: "lento", ms: 1500, cantidad: 2 });
      assert.equal(r.status, 200);
      assert.equal(r.json.totalCents, 200);
      assert.ok(Date.now() - t0 >= 1400);
    });

    test("un token válido de quien NO tiene concedida la operación: 403 PERMISSION_DENIED, lo deniega el broker y el dueño no ejecuta nada", async () => {
      const antes = ejecuciones.length;
      const v0 = violaciones().length;
      const r = await rpc({ cantidad: 1 }, {}, t()["catalogo-web"]);
      assert.equal(r.status, 403);
      assert.equal(r.json.error.code, "PERMISSION_DENIED");
      assert.equal(r.json.error.class, "permanent");
      assert.ok(violaciones().length > v0, "el broker anotó la denegación");
      await dormir(200);
      assert.equal(ejecuciones.length, antes, "el dueño NO ejecutó nada: la petición nunca llegó");
    });

    test("un error PERMANENT del que responde conserva su code y es 422; uno RETRYABLE es 503 con Retry-After", async () => {
      let r = await rpc({ modo: "permanente" });
      assert.equal(r.status, 422);
      assert.equal(r.json.error.code, "TARIFA_INEXISTENTE");
      assert.equal(r.json.error.class, "permanent");
      r = await rpc({ modo: "reintentable" });
      assert.equal(r.status, 503);
      assert.equal(r.json.error.code, "HTTP_503");
      assert.equal(r.json.error.class, "retryable");
      assert.equal(r.cabeceras["retry-after"], "2");
    });

    test("RPC_TIMEOUT es 504 y el cuerpo dice que la operación PUDO ejecutarse", async () => {
      const antes = ejecuciones.length;
      const r = await rpc({ modo: "lento", ms: 1200 }, { "flux-timeout-ms": "300" });
      assert.equal(r.status, 504);
      assert.equal(r.json.error.code, "RPC_TIMEOUT");
      assert.equal(r.json.error.class, "retryable");
      assert.match(r.json.error.message, /PUEDE haberse ejecutado/);
      assert.ok(await esperarHasta(() => ejecuciones.length > antes, 3000), "y, en efecto, la operación se ejecutó");
    });

    test("`Flux-Timeout-Ms` fuera de rango: INVALID_RPC_CALL (no se recorta en silencio)", async () => {
      for (const v of ["0", "25001", "99999"]) {
        const r = await rpc({ cantidad: 1 }, { "flux-timeout-ms": v });
        assert.equal(r.status, 400, v);
        assert.equal(r.json.error.code, "INVALID_RPC_CALL", v);
      }
      assert.equal((await rpc({ cantidad: 1 }, { "flux-timeout-ms": "abc" })).json.error.code, "EDGE_INVALID_REQUEST");
    });

    test("un cliente HTTP que CORTA a mitad de la llamada no la cancela: la operación se ejecuta UNA vez (A-151) y el gateway sigue en pie", async () => {
      const antes = ejecuciones.length;
      const cuerpo = JSON.stringify({ modo: "lento", ms: 800, cantidad: 1 });
      await new Promise((resolve) => {
        const s = net.connect(Number(new URL(e.url).port), "127.0.0.1", () => {
          s.write(`POST /v1/rpc/precios.tarifa.v1.calcular HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer ${t()["tienda-web"]}\r\nContent-Type: application/json\r\nContent-Length: ${cuerpo.length}\r\n\r\n${cuerpo}`);
          setTimeout(() => { s.destroy(); resolve(); }, 150);
        });
      });
      assert.ok(await esperarHasta(() => ejecuciones.length > antes, 4000));
      await dormir(1200); // la respuesta llega a un socket que ya no existe
      assert.equal(ejecuciones.length, antes + 1, "una vez");
      assert.equal((await e.llamar("GET", "/healthz")).status, 200, "el gateway sigue en pie");
      assert.ok(e.lineas.some((l) => /CLIENT_ABORTED|"estado":200/.test(l)));
    });

    test("nadie escucha la operación: 503 RPC_UNAVAILABLE con Retry-After (a los pocos ms, no tras el timeout)", async () => {
      const t0 = Date.now();
      const r = await e.llamar("POST", "/v1/rpc/precios.tarifa.v1.calcular", { token: t()["tienda-web"], cuerpo: { cantidad: 1 }, cabeceras: {} });
      assert.equal(r.status, 200, "con el responder en marcha");
      await precios.close();
      precios = undefined;
      const t1 = Date.now();
      const r2 = await e.llamar("POST", "/v1/rpc/precios.tarifa.v1.calcular", { token: t()["tienda-web"], cuerpo: { cantidad: 1 } });
      assert.equal(r2.status, 503);
      assert.equal(r2.json.error.code, "RPC_UNAVAILABLE");
      assert.equal(r2.cabeceras["retry-after"], "1");
      assert.ok(Date.now() - t1 < 2500, `falló rápido (${Date.now() - t1} ms), no tras el timeout de 5 s`);
      void t0;
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
  describe("negativas: autenticación y permisos", () => {
    const evento = (token, extra = {}) => e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { token, cuerpo: { pedidoId: "neg" }, ...extra });

    test("un token malo, uno ausente y uno con otro esquema dan el MISMO 401, sin publicar nada", async () => {
      const antes = (await eventosEn("EVT_PEDIDOS")).length;
      const respuestas = [
        await evento("flx_estetokennoexiste0000000000000000000000000"),
        await evento(undefined),
        await e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { cuerpo: { pedidoId: "neg" }, cabeceras: { authorization: `Basic ${t()["tienda-web"]}` } }),
        await e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { cuerpo: { pedidoId: "neg" }, cabeceras: { authorization: `Bearer ${t()["tienda-web"].toUpperCase()}` } }),
      ];
      for (const r of respuestas) {
        assert.equal(r.status, 401);
        assert.equal(r.json.error.code, "EDGE_UNAUTHENTICATED");
        assert.equal(r.cabeceras["www-authenticate"], "Bearer");
      }
      assert.equal(new Set(respuestas.map((r) => r.texto)).size, 1, "el mismo cuerpo exacto: no se distingue «ausente» de «desconocido»");
      assert.equal((await eventosEn("EVT_PEDIDOS")).length, antes, "no se publicó nada");
    });

    test("el token de OTRO cliente sobre un recurso que ese cliente no tiene concedido: lo deniega el BROKER (no solo el gateway)", async () => {
      const antes = (await eventosEn("EVT_PEDIDOS")).length;
      const v0 = violaciones().length;
      // catalogo-web tiene un token válido, pero `pedidos` es de tienda-web
      const r = await evento(t()["catalogo-web"]);
      assert.equal(r.status, 403);
      assert.equal(r.json.error.code, "PERMISSION_DENIED");
      assert.ok(violaciones().length > v0, "violación registrada por el broker");
      assert.ok(violaciones().some((l) => /EVT_PEDIDOS|pedidos\./.test(l)), "sobre el recurso de otro dominio");
      assert.equal((await eventosEn("EVT_PEDIDOS")).length, antes, "nada se publicó");
      // y a la inversa: pasarela-web tampoco publica en `catalogo`
      const r2 = await e.llamar("POST", "/v1/events/catalogo.producto.v1.publicado", { token: t()["pasarela-web"], cuerpo: { x: 1 } });
      assert.equal(r2.status, 403);
      assert.equal(r2.json.error.code, "PERMISSION_DENIED");
    });

    test("un cuerpo que no es JSON, no es un objeto, no es UTF-8 o no declara `application/json`: 400/415 con el motivo", async () => {
      const ruta = "/v1/events/pedidos.pedido.v1.creado";
      const tk = t()["tienda-web"];
      let r = await e.llamar("POST", ruta, { token: tk, cuerpo: "{ no es json" });
      assert.deepEqual([r.status, r.json.error.code], [400, "EDGE_INVALID_BODY"]);
      for (const cuerpo of ["[1,2]", "42", '"hola"', "null", ""]) {
        r = await e.llamar("POST", ruta, { token: tk, cuerpo });
        assert.deepEqual([r.status, r.json.error.code], [400, "EDGE_INVALID_BODY"], cuerpo);
      }
      r = await e.llamar("POST", ruta, { token: tk, cuerpo: Buffer.from([0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0xff, 0xfe, 0x22, 0x7d]) });
      assert.deepEqual([r.status, r.json.error.code], [400, "EDGE_INVALID_BODY"], "UTF-8 inválido");
      r = await e.llamar("POST", ruta, { token: tk, cuerpo: "pedidoId=1", tipo: "application/x-www-form-urlencoded" });
      assert.deepEqual([r.status, r.json.error.code], [415, "EDGE_UNSUPPORTED_MEDIA_TYPE"]);
      r = await e.llamar("POST", ruta, { token: tk, cuerpo: '{"a":1}', tipo: "application/json; charset=utf-8" });
      assert.equal(r.status, 202, "con charset sí");
    });

    test("rutas y métodos: 404 y 405 con su código", async () => {
      assert.equal((await e.llamar("GET", "/v1/events/pedidos.pedido.v1.creado", { token: t()["tienda-web"] })).json.error.code, "EDGE_METHOD_NOT_ALLOWED");
      assert.equal((await e.llamar("POST", "/v1/nada", { token: t()["tienda-web"], cuerpo: {} })).json.error.code, "EDGE_NOT_FOUND");
      assert.equal((await e.llamar("POST", "/otra/cosa", { token: t()["tienda-web"], cuerpo: {} })).status, 404);
      assert.equal((await e.llamar("POST", "/v1/events", { token: t()["tienda-web"], cuerpo: {} })).status, 404);
      assert.equal((await e.llamar("POST", "/v1/events/a/b", { token: t()["tienda-web"], cuerpo: {} })).status, 404);
      assert.equal((await e.llamar("POST", "/v1/hooks/no-existe", { cuerpo: {} })).json.error.code, "EDGE_NOT_FOUND");
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
  describe("negativas: límite de cuerpo (A-152)", () => {
    const antes = async () => (await eventosEn("EVT_PEDIDOS")).length;
    const conexionCerrada = (s) => new Promise((r) => (s.destroyed ? r(true) : s.once("close", () => r(true))));

    test("un `Content-Length` mayor que el límite: 413 SIN leer el cuerpo, con la conexión cerrada, y no se publica nada", async () => {
      const n0 = await antes();
      const { status, leidosPorElServidor, cerrada, cuerpo } = await new Promise((resolve) => {
        const s = net.connect(Number(new URL(e.url).port), "127.0.0.1");
        let resp = "";
        let enviados = 0;
        s.on("data", (d) => (resp += d));
        s.on("error", () => {});
        s.write(`POST /v1/events/pedidos.pedido.v1.creado HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer ${t()["tienda-web"]}\r\nContent-Type: application/json\r\nContent-Length: ${50 * 1024 * 1024}\r\n\r\n`);
        const trozo = Buffer.alloc(64 * 1024, 0x61);
        const bombea = async () => {
          const t0 = Date.now();
          while (Date.now() - t0 < 1500 && !s.destroyed) {
            if (!s.write(trozo)) await new Promise((r) => (s.once("drain", r), setTimeout(r, 100)));
            enviados += trozo.length;
          }
          s.destroy();
        };
        bombea();
        s.once("close", () => resolve({ status: Number(/HTTP\/1.1 (\d+)/.exec(resp)?.[1]), leidosPorElServidor: enviados, cerrada: true, cuerpo: resp.split("\r\n\r\n")[1] }));
      });
      assert.equal(status, 413);
      assert.ok(cerrada, "el servidor cerró la conexión");
      assert.match(cuerpo, /EDGE_BODY_TOO_LARGE/);
      assert.ok(leidosPorElServidor < 20 * 1024 * 1024, `el cliente pudo enviar solo ${leidosPorElServidor} de 52428800 bytes declarados: el servidor no los leyó (sin cerrar, Node los lee y descarta todos)`);
      assert.equal(await antes(), n0, "no se publicó nada");
    });

    test("un cuerpo *chunked* que pasa del límite se corta, con 413 y conexión cerrada", async () => {
      const n0 = await antes();
      const r = await new Promise((resolve) => {
        const s = net.connect(Number(new URL(e.url).port), "127.0.0.1");
        let resp = "";
        s.on("data", (d) => (resp += d));
        s.on("error", () => {});
        s.write(`POST /v1/events/pedidos.pedido.v1.creado HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer ${t()["tienda-web"]}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n`);
        const trozo = Buffer.alloc(64 * 1024, 0x61);
        (async () => {
          const t0 = Date.now();
          while (Date.now() - t0 < 4000 && !s.destroyed) {
            s.write(`${trozo.length.toString(16)}\r\n`);
            if (!s.write(Buffer.concat([trozo, Buffer.from("\r\n")]))) await new Promise((r) => (s.once("drain", r), setTimeout(r, 100)));
          }
          s.destroy();
        })();
        s.once("close", () => resolve(resp));
      });
      assert.match(r, /^HTTP\/1.1 413/);
      assert.match(r, /EDGE_BODY_TOO_LARGE/);
      assert.equal(await antes(), n0);
    });

    test("un cuerpo que cabe en el HTTP pero cuyo EVENTO pasa de 1 MiB (el sobre añade ~425 bytes) es 413 igualmente (A-152)", async () => {
      const n0 = await antes();
      const justo = { pedidoId: "x".repeat(1_048_576 - 100 - '{"pedidoId":""}'.length) };
      assert.ok(Buffer.byteLength(JSON.stringify(justo)) < 1_048_576, "el HTTP cabe");
      const r = await e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { token: t()["tienda-web"], cuerpo: justo });
      assert.equal(r.status, 413);
      assert.equal(r.json.error.code, "EDGE_BODY_TOO_LARGE");
      assert.equal(await antes(), n0);
    });

    test("un cuerpo de 1 MiB menos el sobre sí se publica (el límite es el del evento)", async () => {
      const cabe = { pedidoId: "y".repeat(1_040_000) };
      const r = await e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { token: t()["tienda-web"], cuerpo: cabe });
      assert.equal(r.status, 202);
    });

    test("la conexión de un 413 se cierra; las demás siguen sirviendo", async () => {
      const sock = net.connect(Number(new URL(e.url).port), "127.0.0.1");
      sock.on("error", () => {});
      sock.resume(); // sin leer, el socket no ve el FIN del servidor y no se cierra
      sock.write(`POST /v1/events/pedidos.pedido.v1.creado HTTP/1.1\r\nHost: x\r\nContent-Length: 9999999\r\n\r\n`);
      assert.ok(await Promise.race([conexionCerrada(sock), dormir(5000).then(() => false)]));
      assert.equal((await e.llamar("GET", "/healthz")).status, 200);
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
  describe("registros: `data` no se registra NUNCA (A-163)", () => {
    test("un canario en el `data` de un evento, un put, una llamada y un webhook no aparece en NINGÚN registro; tampoco el token ni la firma", async () => {
      // el responder nativo de precios se cerró en la prueba de RPC_UNAVAILABLE: se abre otro
      precios = await e.nativo("precios-api");
      await precios.respond("precios.tarifa.v1.calcular", async (req) => ({ totalCents: 1, eco: req.secreto }));

      const clasifs = { "flux-classification": "confidential" };
      const r1 = await e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { token: t()["tienda-web"], cuerpo: { pedidoId: "log-1", nota: CANARIO }, cabeceras: clasifs });
      const r2 = await e.llamar("PUT", "/v1/state/catalogo.producto.v1/log-1", { token: t()["catalogo-web"], cuerpo: { productoId: "log-1", nombre: CANARIO, aggregateVersion: 1 }, cabeceras: clasifs });
      const r3 = await e.llamar("POST", "/v1/rpc/precios.tarifa.v1.calcular", { token: t()["tienda-web"], cuerpo: { secreto: CANARIO } });
      const { firmar } = await import("../src/firma.mjs");
      const cuerpoHook = JSON.stringify({ id: "evt_log", created: 1787221600, amount: 1, currency: CANARIO });
      const firma = firmar({ secretos: [SECRETOS.PASARELA_HOOK], t: Math.floor(Date.now() / 1000), cuerpo: cuerpoHook });
      const r4 = await e.llamar("POST", "/v1/hooks/pasarela", { cuerpo: cuerpoHook, cabeceras: { "x-pasarela-firma": firma } });
      // y errores que citan lo que sea
      const r5 = await e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { token: t()["tienda-web"], cuerpo: `{"x":"${CANARIO}` });
      const r6 = await e.llamar("PUT", "/v1/state/catalogo.producto.v1/a.b", { token: t()["catalogo-web"], cuerpo: { nombre: CANARIO } });
      assert.deepEqual([r1.status, r2.status, r3.status, r4.status, r5.status, r6.status], [202, 200, 200, 202, 400, 400]);

      const log = e.registroEntero();
      assert.ok(log.length > 500, "el gateway SÍ registra qué pasó");
      assert.ok(!log.includes(CANARIO), "el canario del `data` NO está en ningún registro");
      for (const [nombre, token] of Object.entries(t())) assert.ok(!log.includes(token), `el token de ${nombre} no está en ningún registro`);
      assert.ok(!log.includes(SECRETOS.PASARELA_HOOK), "ni el secreto del hook");
      assert.ok(!/authorization/i.test(log), "ni la cabecera Authorization");
      assert.ok(!r5.texto.includes(CANARIO) && !r6.texto.includes(CANARIO), "tampoco en el cuerpo de un error");
      // qué SÍ registra
      const fila = e.lineas.map((l) => JSON.parse(l)).find((f) => f.mensaje === "petición" && f.ruta === "/v1/events" && f.estado === 202);
      assert.ok(fila.cliente && fila.metodo && fila.ms !== undefined && fila.id, "cliente, método, duración e id del evento");
      assert.equal(fila.tokenId, "tienda-web-1", "el tokenId (qué token), nunca el token");
    });
  });

  test("el gateway sigue sano al final", async () => {
    const r = await e.llamar("GET", "/healthz");
    assert.equal(r.status, 200);
    assert.deepEqual(r.json, { status: "ok", clientes: 3, conectados: 3 });
  });
});
