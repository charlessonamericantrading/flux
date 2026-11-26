/**
 * El extremo a extremo del ecosistema: un nats-server REAL con TLS y las ACLs GENERADAS de services.json (`dev-secure.mjs`, sin permisos a
 * mano ni `--dev`), `flux provision`, el gateway, PHP con su servidor integrado y tres servicios nativos, cada uno un PROCESO con solo su
 * semilla. Nada es un mock. Recorre, en orden, lo que un desarrollador necesita que funcione para sustituir sus APIs y webhooks:
 *
 *   1. la FOTO inicial de un estado llega a un observador, sin llamar a nadie
 *   2. un cambio llega solo (y la lápida de una baja borra la copia local, y no queda PII en el broker)
 *   3. un pedido → una factura (el CRM responde), y el aviso vuelve; el `correlationid` une las dos puntas (fila 7 del mapa)
 *   4. la MISMA entrega dos veces —también tras un reinicio con `kill -9`— → UN efecto
 *   5. una ORDEN repetida con otro `id` → un efecto (la deduplica el que responde, por la clave de negocio)
 *   6. el webhook de un TERCERO enviado tres veces → un efecto, sin asumir que llega después de la factura
 *   7. PHP habla SOLO con el gateway: lee el estado, llama a una operación y recibe una entrega firmada, deduplicada
 *   8. las NEGATIVAS: un servicio no puede leer, escribir ni llamar lo que su manifiesto no le concede, aunque tenga credenciales válidas
 *   9. ni un solo dato personal en ningún evento, y la DLQ vacía
 *
 * Los mutantes (`test/mutantes.mjs`) rompen a propósito lo importante y exigen que ESTAS pruebas fallen: una que pasa con el código roto no vale.
 */

import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";
import { after, before, describe, test } from "node:test";
import { pathToFileURL } from "node:url";

import { dec } from "../../../scripts/lib/nats-harness.mjs";
import { DIR, RAIZ, dormir, esperarHasta, levantar, natsServer, phpDisponible } from "../lib/entorno.mjs";

// La lógica de la réplica se carga de DIR (el ejemplo, o la copia estropeada de un mutante), no de una ruta fija.
const { aplicarEntrada, aplicarFactura } = await import(pathToFileURL(path.join(DIR, "servicios", "tienda-web.mjs")).href);

const obligatorio = (v) => { const x = (process.env[v] ?? "").trim().toLowerCase(); return x !== "" && x !== "0" && x !== "false"; };
const { motivo } = natsServer();
const PHP = phpDisponible();
if (motivo && !obligatorio("FLUX_REQUIRE_BROKER")) console.warn(`\nAVISO: se SALTAN las pruebas del ecosistema: ${motivo}.\n`);

const PERSONAS = { "cli-1": { nombre: "Ana Pérez", email: "ana.perez@example.com" }, "cli-2": { nombre: "Luis Gómez", email: "luis.gomez@example.com" } };
const LINEAS = [{ sku: "SILLA-1", cantidad: 2, precioUnitarioCents: 5000 }];   // 10 000 céntimos
const PEDIDO_1 = "ped-001";

// ─── pruebas sin broker: la lógica de la réplica local (el `WHERE aggregate_version < $n`) ────────────────────────
describe("la réplica local no asume orden ni entregas únicas", () => {
  const memoria = () => {
    let d = { clientes: {}, versiones: {}, facturas: {} };
    return { leer: () => d, transaccion: (fn) => { const c = structuredClone(d); const r = fn(c); d = c; return r; } };
  };
  const entrada = (v, extra = {}) => ({ key: "cli-1", tenantId: "acme", deleted: false, aggregateVersion: v, data: { nombre: "Ana", email: "a@x.es", plan: "oro", puntos: 0, ...extra } });

  test("una entrada vieja, o repetida, no pisa una nueva", () => {
    const db = memoria();
    assert.equal(aplicarEntrada(db, entrada(3, { plan: "oro" })), "aplicada");
    assert.equal(aplicarEntrada(db, entrada(2, { plan: "bronce" })), "obsoleta", "una versión anterior llegada tarde se descarta");
    assert.equal(aplicarEntrada(db, entrada(3, { plan: "bronce" })), "obsoleta", "la misma versión otra vez (una reentrega) también");
    assert.equal(db.leer().clientes["cli-1"].plan, "oro");
  });

  test("una lápida borra la copia, y una entrada vieja rezagada no resucita al cliente", () => {
    const db = memoria();
    aplicarEntrada(db, entrada(2));
    assert.equal(aplicarEntrada(db, { ...entrada(3), deleted: true, data: null }), "borrada");
    assert.equal(db.leer().clientes["cli-1"], undefined);
    assert.equal(aplicarEntrada(db, entrada(2)), "obsoleta");
    assert.equal(db.leer().clientes["cli-1"], undefined, "no vuelve a aparecer");
  });

  test("el aviso de factura es naturalmente idempotente y solo avanza", () => {
    const db = memoria();
    const f = (v, total) => ({ facturaId: "fac-1", pedidoId: "ped-1", numero: 1, totalCents: total, aggregateVersion: v });
    assert.equal(aplicarFactura(db, f(1, 100)), "aplicada");
    assert.equal(aplicarFactura(db, f(1, 100)), "obsoleta");
    assert.equal(aplicarFactura(db, f(2, 200)), "aplicada");
    assert.equal(aplicarFactura(db, f(1, 100)), "obsoleta");
    assert.equal(db.leer().facturas["ped-1"].totalCents, 200);
  });
});

describe("los ficheros de PHP", () => {
  test("firma.php es, byte a byte, el receptor de referencia del gateway (no divergen)", () => {
    assert.equal(readFileSync(path.join(DIR, "web-php", "firma.php"), "utf8"), readFileSync(path.join(RAIZ, "gateway", "examples", "receptor.php"), "utf8"));
  });
  test("todos pasan `php -l`", { skip: !PHP && !obligatorio("FLUX_REQUIRE_PHP") ? "no hay php" : false }, () => {
    for (const f of ["flux.php", "ficha.php", "recibir.php", "firma.php"]) {
      const r = spawnSync("php", ["-l", path.join(DIR, "web-php", f)], { encoding: "utf8" });
      assert.equal(r.status, 0, `${f}: ${r.stdout}${r.stderr}`);
    }
  });
});

// ─── el extremo a extremo ──────────────────────────────────────────────────────────────────────────────────────────
describe("ecosistema — NATS seguro + gateway + PHP + tres servicios", { skip: motivo && !obligatorio("FLUX_REQUIRE_BROKER") ? motivo : false }, () => {
  let e, crm, facturacion, tienda, jsm;
  const rpcVistos = [];
  const llamadas = (op) => rpcVistos.filter((s) => s === `rpc.clientes.cliente.v1.${op}`).length;

  /** Todos los mensajes de un stream, parseados (los de estado y los eventos son CloudEvents en JSON). */
  async function mensajes(stream) {
    const info = await jsm.streams.info(stream);
    const out = [];
    if (info.state.messages === 0) return out;
    for (let seq = info.state.first_seq; seq <= info.state.last_seq; seq++) {
      const m = await jsm.streams.getMessage(stream, { seq }).catch(() => null);
      if (m) out.push({ seq, subject: m.subject, texto: dec.decode(m.data), json: JSON.parse(dec.decode(m.data)) });
    }
    return out;
  }
  const reinyectar = (stream, subject) => e.reinyectar(stream, subject);
  const webhook = (cuerpo, opciones) => e.webhook(cuerpo, opciones);
  const cobro = (id, pedidoId, extra = {}) => JSON.stringify({ id, created: Math.floor(Date.now() / 1000), amount: 9000, currency: "EUR", metadata: { pedidoId }, customer: { email: "pagador@example.com" }, ...extra });
  const gw = (metodo, ruta, { token = e.token, cuerpo, cabeceras = {} } = {}) =>
    fetch(`${e.gatewayUrl}${ruta}`, { method: metodo, headers: { ...(token && { authorization: `Bearer ${token}` }), ...(cuerpo && { "content-type": "application/json" }), ...cabeceras }, body: cuerpo && JSON.stringify(cuerpo) });
  const ficha = (cliente, total) => fetch(`${e.phpUrl}/ficha.php?cliente=${cliente}&total=${total}`);

  before(async () => {
    e = await levantar();
    jsm = e.jsm;
    // Todo lo que alguien LLAMA, visto desde el broker (el usuario de operación lo ve todo): la prueba de que «no se llamó a nadie».
    e.ops.subscribe("rpc.>", { callback: (err, m) => { if (!err) rpcVistos.push(m.subject); } });
    await e.ops.flush();

    crm = await e.arrancar("crm");
    facturacion = await e.arrancar("facturacion");
    for (const [clienteId, p] of Object.entries(PERSONAS)) await crm.comando("registrar-cliente", { clienteId, ...p, plan: clienteId === "cli-1" ? "plata" : "oro" });
  });
  after(async () => { await e?.parar(); });

  // ── 1 ───────────────────────────────────────────────────────────────────────────────────────────────────────────
  test("1. la tienda arranca DESPUÉS y recibe la foto de los clientes, sin llamar a nadie", async () => {
    tienda = await e.arrancar("tienda-web");
    await tienda.esperar((l) => l.evento === "foto-inicial");
    const local = e.leer("tienda-web").clientes;
    assert.deepEqual(Object.keys(local).sort(), ["cli-1", "cli-2"]);
    assert.equal(local["cli-1"].plan, "plata");
    assert.equal(rpcVistos.length, 0, "ninguna petición: la tienda no preguntó al CRM, lo observa");
    // `get` es la lectura puntual de lo mismo (por el permiso de lectura de su `watches`, fijado a su tenant)
    const bus = await e.conectarComo("tienda-web");
    try {
      assert.equal((await bus.state.get("clientes.cliente.v1", "cli-1", { tenantId: "acme" })).data.nombre, PERSONAS["cli-1"].nombre);
    } finally { await bus.close(); }
  });

  // ── 2 ───────────────────────────────────────────────────────────────────────────────────────────────────────────
  test("2. un cambio en el CRM llega a la tienda SIN llamar; una baja borra la copia local y el valor sale del broker", async () => {
    await crm.comando("cambiar-plan", { clienteId: "cli-1", plan: "oro" });
    await tienda.esperar((l) => l.evento === "cliente-aplicada" && l.clienteId === "cli-1" && l.aggregateVersion === 2);
    assert.equal(e.leer("tienda-web").clientes["cli-1"].plan, "oro");
    assert.equal(rpcVistos.length, 0, "el cambio llegó solo: ni una petición");

    await crm.comando("dar-de-baja", { clienteId: "cli-2" });
    await tienda.esperar((l) => l.evento === "cliente-borrada" && l.clienteId === "cli-2");
    assert.equal(e.leer("tienda-web").clientes["cli-2"], undefined, "la lápida borra la copia local (regla 14)");

    // En el broker: el valor de cli-2 ya no está (lo sustituye una lápida sin dato personal)
    const ultimo = await jsm.streams.getMessage("STATE_CLIENTES", { last_by_subj: "state.clientes.cliente.v1.acme.cli-2" });
    const texto = dec.decode(ultimo.data);
    assert.ok(!texto.includes(PERSONAS["cli-2"].nombre) && !texto.includes(PERSONAS["cli-2"].email), "la lápida no lleva el dato");
    // …y la facturación abrió la cuenta de cada alta con el evento `registrado`, que solo lleva la referencia
    await facturacion.esperar((l) => l.evento === "cuenta-abierta" && l.clienteId === "cli-2");
  });

  // ── 3 ───────────────────────────────────────────────────────────────────────────────────────────────────────────
  test("3. un pedido → facturación llama al CRM y emite UNA factura; el aviso vuelve a la tienda; el correlationid las une", async () => {
    await tienda.comando("crear-pedido", { pedidoId: PEDIDO_1, clienteId: "cli-1", lineas: LINEAS });
    await tienda.esperar((l) => l.evento === "factura-aplicada" && l.pedidoId === PEDIDO_1);

    const { facturas } = e.leer("facturacion");
    assert.equal(facturas.length, 1);
    // El descuento lo CALCULÓ el CRM (plan oro: 10 %): lo prueba que el importe es el de su respuesta, no uno que facturación sepa
    assert.deepEqual({ ...facturas[0] }, { facturaId: "fac-1", numero: 1, pedidoId: PEDIDO_1, clienteId: "cli-1", subtotalCents: 10000, descuentoCents: 1000, totalCents: 9000, moneda: "EUR", estado: "emitida" });
    assert.equal(llamadas("calcular-descuento"), 1);
    assert.equal(llamadas("sumar-puntos"), 1);
    // La orden cambió al CRM, y el cambio volvió por ESTADO a la tienda: 90 puntos
    await tienda.esperar((l) => l.evento === "cliente-aplicada" && l.clienteId === "cli-1" && l.aggregateVersion === 3);
    assert.equal(e.leer("tienda-web").clientes["cli-1"].puntos, 90);
    assert.equal(e.leer("tienda-web").facturas[PEDIDO_1].numero, 1);

    // Fila 7 del mapa (una acción asíncrona): evento del que pide + evento de resultado, unidos por `correlationid`
    const [creado] = (await mensajes("EVT_PEDIDOS")).filter((m) => m.subject === "pedidos.pedido.v1.creado");
    const [emitida] = (await mensajes("EVT_FACTURACION")).filter((m) => m.subject === "facturacion.factura.v1.emitida");
    assert.ok(creado.json.correlationid, "el SDK pone el correlationid");
    assert.equal(emitida.json.correlationid, creado.json.correlationid, "la factura hereda el correlationid del pedido");
    assert.equal(emitida.json.causationid, creado.json.id, "y su causa es el `id` del pedido");
    assert.equal(emitida.json.tenantid, "acme");
    assert.equal(emitida.json.dataclassification !== undefined, true);
  });

  // ── 4 ───────────────────────────────────────────────────────────────────────────────────────────────────────────
  test("4. la MISMA entrega otra vez —y tras un `kill -9` de facturación— produce UN efecto", async () => {
    const antes = { calculos: llamadas("calcular-descuento"), puntos: llamadas("sumar-puntos") };
    await reinyectar("EVT_PEDIDOS", "pedidos.pedido.v1.creado");
    await facturacion.esperar((l) => l.evento === "duplicado-ignorado");
    assert.equal(e.leer("facturacion").facturas.length, 1, "una segunda entrega del mismo `id` no emite otra factura");
    assert.equal(e.leer("facturacion").siguienteNumero, 2, "ni consume un número de factura");
    assert.equal(llamadas("calcular-descuento"), antes.calculos, "ni vuelve a llamar al CRM");
    assert.equal(llamadas("sumar-puntos"), antes.puntos);

    facturacion = await e.reiniciar("facturacion", "SIGKILL");   // sin parada ordenada: lo que hace una caída
    await reinyectar("EVT_PEDIDOS", "pedidos.pedido.v1.creado");
    await facturacion.esperar((l) => l.evento === "duplicado-ignorado");
    assert.equal(e.leer("facturacion").facturas.length, 1, "la marca de «ya procesado» sobrevivió al reinicio: estaba en la misma transacción");
    const emitidas = (await mensajes("EVT_FACTURACION")).filter((m) => m.subject === "facturacion.factura.v1.emitida");
    assert.equal(emitidas.length, 1, "y no se avisó dos veces");
  });

  // ── 5 ───────────────────────────────────────────────────────────────────────────────────────────────────────────
  test("5. una ORDEN repetida con otro `id` (una reentrega del evento que la provoca) no suma dos veces", async () => {
    const bus = await e.conectarComo("facturacion");
    try {
      const r = await bus.request("clientes.cliente.v1.sumar-puntos", { clienteId: "cli-1", puntos: 90, referencia: PEDIDO_1 });
      assert.deepEqual(r, { clienteId: "cli-1", puntos: 90, aggregateVersion: 3 }, "la MISMA respuesta que la primera vez, no puntos = 180");
      const nueva = await bus.request("clientes.cliente.v1.sumar-puntos", { clienteId: "cli-1", puntos: 5, referencia: "ped-otro" });
      assert.deepEqual(nueva, { clienteId: "cli-1", puntos: 95, aggregateVersion: 4 }, "otra referencia sí suma");
    } finally { await bus.close(); }
  });

  // ── 6 ───────────────────────────────────────────────────────────────────────────────────────────────────────────
  test("6. el webhook de un TERCERO enviado tres veces produce UN solo cobro; y un cobro que llega ANTES que su factura", async () => {
    const cuerpo = cobro("evt_pago_1", PEDIDO_1);
    const ids = [];
    for (let i = 0; i < 3; i++) {
      const r = await webhook(cuerpo);
      assert.equal(r.status, 202);
      ids.push((await r.json()).id);
    }
    assert.equal(new Set(ids).size, 1, "el mismo aviso reenviado da el MISMO id (determinista): 01-envelope.md §2.6.1");
    await facturacion.esperar((l) => l.evento === "cobro-registrado" && l.pagoId === "evt_pago_1");
    const pagos = (await mensajes("EVT_PAGOS")).filter((m) => m.subject === "pagos.cobro.v1.recibido");
    assert.equal(pagos.length, 1, "JetStream guardó UN mensaje (los otros dos, dentro de `duplicate_window`, los descartó)");
    assert.ok(!pagos[0].texto.includes("pagador@example.com"), "el correo del pagador NO pasa: `data.pick` (regla 5)");
    assert.deepEqual(pagos[0].json.data, { pagoId: "evt_pago_1", pedidoId: PEDIDO_1, importeCents: 9000, moneda: "EUR" });
    assert.equal(Object.keys(e.leer("facturacion").cobros).length, 1);
    assert.equal(e.leer("facturacion").facturas[0].estado, "pagada");

    // Fuera de `duplicate_window` (A-157) el broker guarda los dos: el consumidor es quien deduplica.
    await reinyectar("EVT_PAGOS", "pagos.cobro.v1.recibido");
    await facturacion.esperar((l) => l.evento === "cobro-repetido");
    assert.equal(Object.keys(e.leer("facturacion").cobros).length, 1, "un efecto");

    // Sin asumir orden: el cobro de un pedido que AÚN NO tiene factura se guarda, y la factura nace pagada.
    assert.equal((await webhook(cobro("evt_pago_2", "ped-002"))).status, 202);
    await facturacion.esperar((l) => l.evento === "cobro-registrado" && l.pagoId === "evt_pago_2");
    await tienda.comando("crear-pedido", { pedidoId: "ped-002", clienteId: "cli-1", lineas: [{ sku: "MESA-1", cantidad: 1, precioUnitarioCents: 10000 }] });
    await esperarHasta(() => e.leer("facturacion").facturas.length === 2, 20_000);
    const f2 = e.leer("facturacion").facturas[1];
    assert.deepEqual([f2.numero, f2.pedidoId, f2.estado], [2, "ped-002", "pagada"]);
  });

  test("6b. el mismo webhook, con `curl` y `openssl` (web-php/webhook-pasarela.sh): lo que haría un cron o la propia pasarela", async () => {
    const r = spawnSync("bash", [path.join(DIR, "web-php", "webhook-pasarela.sh"), "evt_sh_1", "ped-shell"], { encoding: "utf8", env: { ...process.env, FLUX_GATEWAY_URL: e.gatewayUrl, PASARELA_SECRETO: e.secretos.pasarela } });
    assert.equal(r.status, 0, r.stdout + r.stderr);
    assert.match(JSON.parse(r.stdout).id, /^[0-9a-f-]{36}$/);
    await facturacion.esperar((l) => l.evento === "cobro-registrado" && l.pagoId === "evt_sh_1");
  });

  // ── 7 ───────────────────────────────────────────────────────────────────────────────────────────────────────────
  describe("7. PHP, en un hosting compartido: solo habla HTTP con el gateway", { skip: !PHP && !obligatorio("FLUX_REQUIRE_PHP") ? "no hay php" : false }, () => {
    const hayPhp = () => { if (!e.conPhp) throw new Error("no hay `php` con curl (FLUX_REQUIRE_PHP)"); };

    test("ficha.php lee el estado y llama a la operación a través del gateway", async () => {
      hayPhp();
      const r = await ficha("cli-1", 9990);
      assert.equal(r.status, 200);
      const j = await r.json();
      assert.equal(j.cliente.nombre, PERSONAS["cli-1"].nombre);
      assert.equal(j.cliente.plan, "oro");
      assert.equal(j.descuentoCents, 999, "el cálculo es del CRM: oro, 10 % de 9990");
      assert.equal(JSON.stringify(j).includes("@"), false, "el correo no sale de la ficha");
      assert.equal((await ficha("cli-2", 100)).status, 404, "un cliente dado de baja no existe");
      assert.equal((await ficha("no valido", 100)).status, 400);
    });

    test("ficha-curl.sh, el equivalente con `curl`, da lo mismo que ficha.php", () => {
      const r = spawnSync("bash", [path.join(DIR, "web-php", "ficha-curl.sh"), "cli-1", "9990"], { encoding: "utf8", env: { ...process.env, FLUX_GATEWAY_URL: e.gatewayUrl, FLUX_GATEWAY_TOKEN: e.token } });
      assert.equal(r.status, 0, r.stdout + r.stderr);
      const [estado, descuento] = r.stdout.replace(/\}\{/, "}\n{").split("\n").map((l) => JSON.parse(l));
      assert.equal(estado.data.nombre, PERSONAS["cli-1"].nombre);
      assert.equal(descuento.descuentoCents, 999);
    });

    test("recibir.php recibe las facturas por una entrega FIRMADA y las deduplica", async () => {
      hayPhp();
      // Las dos facturas emitidas hasta ahora llegaron por el gateway (flux → HTTP): una entrega por evento
      await esperarHasta(() => e.leerWeb().length >= 2, 20_000);
      assert.equal(e.leerWeb().length, 2);
      assert.equal(new Set(e.leerWeb()).size, 2);

      // Lo que hace el gateway, a mano: la misma `Idempotency-Key` dos veces → UN efecto; una firma mala → 401
      const cuerpo = JSON.stringify({ specversion: "1.0", id: "01a21928-c92d-778f-973f-86e20e3d3f7b", data: {} });
      const t = Math.floor(Date.now() / 1000);
      const firma = (secreto) => `t=${t},v1=${createHmac("sha256", secreto).update(`${t}.${cuerpo}`).digest("hex")}`;
      const entrega = (f, clave) => fetch(`${e.phpUrl}/recibir.php`, { method: "POST", headers: { "content-type": "application/cloudevents+json", "idempotency-key": clave, "flux-signature": f }, body: cuerpo });
      const clave = "01a21928-aaaa-7bbb-8ccc-000000000001";
      assert.equal((await entrega(firma(e.secretos.entrega), clave)).status, 204);
      assert.equal((await entrega(firma(e.secretos.entrega), clave)).status, 204, "repetida: 2xx, sin repetir el efecto");
      assert.equal(e.leerWeb().filter((k) => k === clave).length, 1);
      assert.equal((await entrega(firma("otro-secreto"), "01a21928-aaaa-7bbb-8ccc-000000000002")).status, 401, "la firma que no verifica se rechaza");
      assert.equal(e.leerWeb().includes("01a21928-aaaa-7bbb-8ccc-000000000002"), false, "…y no se ejecuta nada");
    });
  });

  // ── 8 ───────────────────────────────────────────────────────────────────────────────────────────────────────────
  describe("8. negativas de seguridad: lo que el manifiesto no concede, se deniega — con credenciales válidas", () => {
    const denegado = (err) => err?.code === "PERMISSION_DENIED";
    let tiendaBus, factBus, crmBus;
    before(async () => { [tiendaBus, factBus, crmBus] = await Promise.all(["tienda-web", "facturacion", "crm"].map((s) => e.conectarComo(s))); });
    after(async () => { await Promise.allSettled([tiendaBus, factBus, crmBus].map((b) => b?.close())); });

    test("tienda-web NO puede leer los cobros: no los consume", async () => {
      await assert.rejects(() => tiendaBus.subscribe("pagos.cobro.v1.recibido", async () => {}), denegado);
    });
    test("tienda-web NO puede llamar al CRM: no tiene `calls` (y lee los clientes por estado, que sí)", async () => {
      await assert.rejects(() => tiendaBus.request("clientes.cliente.v1.calcular-descuento", { clienteId: "cli-1", totalCents: 100, moneda: "EUR" }, { timeoutMs: 2000 }), denegado);
      await assert.rejects(() => tiendaBus.request("clientes.cliente.v1.sumar-puntos", { clienteId: "cli-1", puntos: 1000, referencia: "x" }, { timeoutMs: 2000 }), denegado);
    });
    test("tienda-web NO puede escribir los clientes (solo el dueño) ni leer los de otro tenant", async () => {
      const datos = { clienteId: "cli-1", nombre: "Mallory", email: "m@x.es", plan: "oro", puntos: 0, aggregateVersion: 99 };
      await assert.rejects(() => tiendaBus.state.put("clientes.cliente.v1", "cli-1", datos), denegado);
      await assert.rejects(() => tiendaBus.state.delete("clientes.cliente.v1", "cli-1", { aggregateVersion: 99 }), denegado);
      await assert.rejects(() => tiendaBus.state.get("clientes.cliente.v1", "cli-1", { tenantId: "globex" }), denegado);
      assert.ok(await tiendaBus.state.get("clientes.cliente.v1", "cli-1", { tenantId: "acme" }), "control positivo: su tenant, sí");
    });
    test("facturacion NO puede leer los clientes (no los observa) ni publicar en un dominio ajeno", async () => {
      await assert.rejects(() => factBus.state.get("clientes.cliente.v1", "cli-1", { tenantId: "acme" }), denegado);
      await assert.rejects(() => factBus.publish("clientes.cliente.v1.registrado", { clienteId: "cli-9", aggregateVersion: 1, plan: "oro" }), denegado);
      await assert.rejects(() => factBus.publish("pedidos.pedido.v1.creado", { pedidoId: "ped-999", clienteId: "cli-1", aggregateVersion: 1, totalCents: 1, moneda: "EUR", lineas: [{ sku: "x", cantidad: 1, precioUnitarioCents: 1 }] }), denegado);
    });
    test("el CRM NO puede leer los pedidos ni llamarse a sí mismo (no tiene `calls`)", async () => {
      await assert.rejects(() => crmBus.subscribe("pedidos.pedido.v1.creado", async () => {}), denegado);
      await assert.rejects(() => crmBus.request("clientes.cliente.v1.calcular-descuento", { clienteId: "cli-1", totalCents: 100, moneda: "EUR" }, { timeoutMs: 2000 }), denegado);
    });
    test("nadie lee el inbox de otro: `_INBOX.>` y `_INBOX.crm.>` están denegados a facturacion, y `rpc.>` también", async () => {
      const nc = await e.crudaComo("facturacion");
      try {
        for (const s of ["_INBOX.>", "_INBOX.crm.>", "rpc.>", "state.>", "pedidos.>"]) nc.subscribe(s);
        await nc.flush();
        await dormir(400);
        for (const s of ["_INBOX.>", "_INBOX.crm.>", "rpc.>", "state.>", "pedidos.>"]) {
          assert.ok(nc.violaciones.some((v) => v.includes(`Subscription to "${s}"`)), `facturacion pudo suscribirse a ${s}`);
        }
        const propio = nc.subscribe("_INBOX.facturacion.x");   // control positivo: el suyo, sí
        await nc.flush(); await dormir(200);
        assert.equal(nc.violaciones.some((v) => v.includes('"_INBOX.facturacion.x"')), false);
        propio.unsubscribe();
      } finally { await nc.close(); }
    });
    test("el CLIENTE DE BORDE no tiene permisos especiales: un token válido sobre lo que no tiene concedido lo deniega el BROKER (403)", async () => {
      const denegadoGw = async (r) => { assert.equal(r.status, 403); assert.equal((await r.json()).error.code, "PERMISSION_DENIED"); };
      await denegadoGw(await gw("POST", "/v1/rpc/clientes.cliente.v1.sumar-puntos", { cuerpo: { clienteId: "cli-1", puntos: 1, referencia: "x" } }));
      await denegadoGw(await gw("PUT", "/v1/state/clientes.cliente.v1/cli-1", { cuerpo: { clienteId: "cli-1", nombre: "M", email: "m@x.es", plan: "oro", puntos: 0, aggregateVersion: 99 } }));
      await denegadoGw(await gw("POST", "/v1/events/pedidos.pedido.v1.creado", { cuerpo: { pedidoId: "ped-777", clienteId: "cli-1", aggregateVersion: 1, totalCents: 1, moneda: "EUR", lineas: [{ sku: "x", cantidad: 1, precioUnitarioCents: 1 }] } }));
      assert.equal((await gw("POST", "/v1/rpc/clientes.cliente.v1.calcular-descuento", { cuerpo: { clienteId: "cli-1", totalCents: 100, moneda: "EUR" } })).status, 200, "control positivo: lo concedido funciona");
    });
    test("sin token, o con uno falso, es 401; un webhook con firma mala, 401 y NO se publica nada", async () => {
      assert.equal((await gw("GET", "/v1/state/clientes.cliente.v1/cli-1", { token: null })).status, 401);
      assert.equal((await gw("GET", "/v1/state/clientes.cliente.v1/cli-1", { token: "flx_falso" })).status, 401);
      const antes = (await mensajes("EVT_PAGOS")).length;
      assert.equal((await webhook(cobro("evt_falso", PEDIDO_1), { secreto: "secreto-equivocado" })).status, 401);
      assert.equal((await webhook(cobro("evt_viejo", PEDIDO_1), { t: Math.floor(Date.now() / 1000) - 3600 })).status, 401, "fuera de la tolerancia de 300 s");
      assert.equal((await mensajes("EVT_PAGOS")).length, antes);
    });
  });

  // ── 9 ───────────────────────────────────────────────────────────────────────────────────────────────────────────
  test("9. ni un dato personal en un evento (solo en el ESTADO de los clientes) y la DLQ vacía", async () => {
    const personales = [...Object.values(PERSONAS).flatMap((p) => [p.nombre, p.email]), "pagador@example.com"];
    const flujos = (await jsm.streams.list().next()).map((s) => s.config.name);
    assert.ok(flujos.includes("STATE_CLIENTES") && flujos.includes("EVT_PEDIDOS") && flujos.includes("DLQ_PEDIDOS"), `streams: ${flujos}`);
    for (const nombre of flujos) {
      for (const m of await mensajes(nombre)) {
        if (nombre === "STATE_CLIENTES" && m.subject.endsWith(".cli-1")) continue;   // el dato personal vive AQUÍ, clasificado y borrable
        for (const p of personales) assert.ok(!m.texto.includes(p), `«${p}» está en ${nombre} (${m.subject})`);
      }
      if (nombre.startsWith("DLQ_")) assert.equal((await mensajes(nombre)).length, 0, `${nombre} no está vacía`);
    }
    // El estado va clasificado y con su versión
    const est = (await mensajes("STATE_CLIENTES")).find((m) => m.subject.endsWith(".cli-1"));
    assert.equal(est.json.dataclassification, "confidential");
    assert.equal(est.json.tenantid, "acme");
  });

  // ── 10 ──────────────────────────────────────────────────────────────────────────────────────────────────────────
  test("10. la tienda pierde su base de datos: `flux state resync` y la foto vuelve (sin llamar a nadie)", async () => {
    const llamadasAntes = rpcVistos.length;
    await tienda.parar();
    const { rmSync } = await import("node:fs");
    rmSync(path.join(e.datos, "tienda-web.json"));
    const r = spawnSync(process.execPath, [path.join(RAIZ, "cli", "src", "index.mjs"), "state", "resync", "tienda-web", "clientes.cliente.v1", "--tenant", "acme", "--confirm", "--yes", "--server", e.url, "--creds", path.join(e.seguro, "ops.creds")],
      { encoding: "utf8", env: { ...process.env, NODE_EXTRA_CA_CERTS: e.ca, NO_COLOR: "1" } });
    assert.equal(r.status, 0, `${r.stdout}${r.stderr}`);
    tienda = await e.arrancar("tienda-web");
    await tienda.esperar((l) => l.evento === "foto-inicial");
    const local = e.leer("tienda-web").clientes;
    assert.deepEqual(Object.keys(local), ["cli-1"], "la foto trae lo que hay AHORA: cli-2 está de baja");
    assert.equal(local["cli-1"].puntos, 185, "95 + los 90 puntos del segundo pedido");
    assert.equal(rpcVistos.length, llamadasAntes, "recuperarse no costó ni una petición");
  });
});
