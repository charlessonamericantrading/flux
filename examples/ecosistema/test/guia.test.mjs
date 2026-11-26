/**
 * La guía de migración (`docs/migracion.md`) y su «antes» en HTTP, SIN broker.
 *
 *  1. Cada bloque de código de la guía es un extracto LITERAL de un fichero real (`scripts/check-migracion.mjs`), y la comprobación PUEDE FALLAR:
 *     se estropea una copia de la guía de cinco maneras y cada una debe rechazarse.
 *  2. El «antes» (`antes-http/`) FUNCIONA: se ejecuta contra servidores `node:http` de verdad. Un «antes» que no corre sería un hombre de paja.
 *  3. Los scripts de `curl` y el PHP del «antes» también se ejecutan (con `php`/`curl` si hay; FLUX_REQUIRE_PHP=1 hace que su ausencia sea un fallo).
 */

import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { describe, test } from "node:test";
import { pathToFileURL } from "node:url";

import { DIR, RAIZ, phpDisponible } from "../lib/entorno.mjs";

const GUIA = path.join(RAIZ, "docs", "migracion.md");
const CHECK = path.join(RAIZ, "scripts", "check-migracion.mjs");
const PHP = phpDisponible();
const exigePhp = !PHP && !["", "0", "false"].includes((process.env.FLUX_REQUIRE_PHP ?? "").trim().toLowerCase());
const importa = (...ruta) => import(pathToFileURL(path.join(DIR, ...ruta)).href);

/**
 * Un servidor `node:http` en un puerto libre; `manejador(req, res, cuerpo)`. Con `{ crudo: true }` el manejador recibe la petición SIN leer
 * (es quien lee el cuerpo, como el receptor del webhook de la fila 8).
 */
async function servidor(manejador, { crudo = false } = {}) {
  const peticiones = [];
  const s = http.createServer((req, res) => {
    if (crudo) return manejador(req, res);
    const trozos = [];
    req.on("data", (t) => trozos.push(t));
    req.on("end", () => {
      const cuerpo = Buffer.concat(trozos);
      peticiones.push({ metodo: req.method, url: req.url, cabeceras: req.headers, cuerpo: cuerpo.toString("utf8") });
      manejador(req, res, cuerpo);
    });
  });
  await new Promise((r) => s.listen(0, "127.0.0.1", r));
  return { url: `http://127.0.0.1:${s.address().port}`, peticiones, cerrar: () => new Promise((r) => { s.closeAllConnections(); s.close(r); }) };
}
const json = (res, estado, cuerpo) => { res.statusCode = estado; res.setHeader("content-type", "application/json"); res.end(JSON.stringify(cuerpo)); };
const dormir = (ms) => new Promise((r) => setTimeout(r, ms));

// ─── 1. la guía ──────────────────────────────────────────────────────────────────────────────────────────────────

describe("docs/migracion.md: el código de la guía es el de los ficheros reales", () => {
  const comprobar = (guia) => spawnSync(process.execPath, [CHECK, guia], { encoding: "utf8" });
  const conCopia = (cambia) => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "flux-guia-"));
    try {
      const f = path.join(dir, "guia.md");
      writeFileSync(f, cambia(readFileSync(GUIA, "utf8")));
      return comprobar(f);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  };

  test("la guía pasa, y tiene fragmentos de verdad (no está vacía)", () => {
    const r = comprobar(GUIA);
    assert.equal(r.status, 0, r.stdout + r.stderr);
    const n = Number(/✓ (\d+) fragmento/.exec(r.stdout)?.[1]);
    assert.ok(n >= 30, `solo ${n} fragmentos`);
  });

  test("un carácter cambiado dentro de un bloque se detecta, y dice dónde", () => {
    const r = conCopia((t) => t.replace('"clientes.cliente.v1.registrado", { clienteId, aggregateVersion', '"clientes.cliente.v1.registrado", { clienteID, aggregateVersion'));
    assert.equal(r.status, 1);
    assert.match(r.stderr, /no coincide con examples\/ecosistema\/servicios\/crm\.mjs#crm-evento/);
    assert.match(r.stderr, /clienteID/);
  });

  test("un bloque de código SIN marca de fuente se rechaza (código escrito a mano)", () => {
    const r = conCopia((t) => t.replace("<!-- fuente: examples/ecosistema/servicios/crm.mjs#crm-calculo -->\n", ""));
    assert.equal(r.status, 1);
    assert.match(r.stderr, /sin marca de fuente/);
  });

  test("una etiqueta que no existe, o un fichero que no existe, se rechaza", () => {
    assert.equal(conCopia((t) => t.replace("#crm-calculo -->", "#no-existe -->")).status, 1);
    assert.equal(conCopia((t) => t.replace("servicios/crm.mjs#crm-calculo", "servicios/otro.mjs#crm-calculo")).status, 1);
  });

  test("un bloque JSON distinto de su subárbol, y un `sin-fuente` sin motivo, se rechazan", () => {
    const r = conCopia((t) => t.replace('"toleranceSeconds": 300', '"toleranceSeconds": 3000'));
    assert.equal(r.status, 1);
    assert.match(r.stderr, /el bloque JSON no es igual/);
    assert.equal(conCopia((t) => t.replace(/<!-- sin-fuente: los pasos 3 a 5[^>]*-->/, "<!-- sin-fuente: x -->")).status, 1);
  });
});

// ─── 2. el «antes» ───────────────────────────────────────────────────────────────────────────────────────────────

describe("antes-http: el «antes» funciona", () => {
  test("fila 1: el webhook llega a cada suscriptor… con el correo del cliente en el cuerpo (el problema)", async () => {
    const { avisarAltaDeCliente } = await importa("antes-http", "http.mjs");
    const a = await servidor((q, r) => r.end());
    const b = await servidor((q, r) => r.end());
    try {
      await avisarAltaDeCliente([a.url, b.url], { clienteId: "cli-1", email: "ana@example.com" });
      for (const s of [a, b]) assert.deepEqual(JSON.parse(s.peticiones[0].cuerpo), { evento: "cliente.registrado", clienteId: "cli-1", email: "ana@example.com" });
    } finally { await a.cerrar(); await b.cerrar(); }
  });

  test("fila 2: el polling entrega cada cambio una vez, preguntando cada T aunque no haya nada", async () => {
    const { cambios, servirCambios, sondear } = await importa("antes-http", "http.mjs");
    const s = await servidor((req, res) => servirCambios(req, res));
    const vistos = [];
    const t = sondear(s.url, 20, (c) => vistos.push(c.seq));
    try {
      await dormir(120);
      const peticionesEnVacio = s.peticiones.length;
      assert.ok(peticionesEnVacio >= 2, "pregunta aunque no haya cambiado nada");
      cambios.push({ seq: 1 }, { seq: 2 });
      await dormir(120);
      assert.deepEqual(vistos, [1, 2], "cada cambio, una vez");
    } finally { clearInterval(t); await s.cerrar(); }
  });

  test("fila 3, 4 y 6: leer un cliente (404 → null; 500 → error), paginar la carga inicial y calcular", async () => {
    const h = await importa("antes-http", "http.mjs");
    const crm = await servidor((req, res) => {
      const u = new URL(req.url, "http://x");
      if (u.pathname === "/clientes/cli-1") return json(res, 200, { clienteId: "cli-1", nombre: "Ana" });
      if (u.pathname === "/clientes/roto") return json(res, 500, {});
      if (u.pathname === "/clientes") return json(res, 200, Number(u.searchParams.get("page")) <= 2 ? [{ n: u.searchParams.get("page") }] : []);
      if (u.pathname === "/descuento") return json(res, 200, { descuentoCents: Number(u.searchParams.get("totalCents")) / 10 });
      return json(res, 404, {});
    });
    try {
      assert.equal((await h.leerCliente(crm.url, "cli-1")).nombre, "Ana");
      assert.equal(await h.leerCliente(crm.url, "nadie"), null);
      await assert.rejects(() => h.leerCliente(crm.url, "roto"), /500/);
      assert.deepEqual(await h.cargarTodos(crm.url), [{ n: "1" }, { n: "2" }]);
      assert.deepEqual(await h.calcularDescuento(crm.url, "cli-1", 9990), { descuentoCents: 999 });
    } finally { await crm.cerrar(); }
  });

  test("fila 5: la orden lleva su Idempotency-Key en la cabecera y un error es un error", async () => {
    const { sumarPuntos } = await importa("antes-http", "http.mjs");
    const crm = await servidor((req, res) => json(res, req.url.includes("roto") ? 503 : 200, { puntos: 90 }));
    try {
      assert.deepEqual(await sumarPuntos(crm.url, "cli-1", 90, "ped-001"), { puntos: 90 });
      assert.equal(crm.peticiones[0].cabeceras["idempotency-key"], "ped-001");
      assert.deepEqual(JSON.parse(crm.peticiones[0].cuerpo), { puntos: 90 });
      await assert.rejects(() => sumarPuntos(crm.url, "roto", 1, "x"), /503/);
    } finally { await crm.cerrar(); }
  });

  test("fila 7: un POST asíncrono se resuelve preguntando hasta que la factura esté lista", async () => {
    const { facturarYEsperar } = await importa("antes-http", "http.mjs");
    let preguntas = 0;
    const fac = await servidor((req, res) => {
      if (req.method === "POST") return json(res, 202, { location: "/facturas/1" });
      return json(res, 200, { estado: ++preguntas < 3 ? "en-curso" : "lista", numero: 1 });
    });
    try {
      assert.deepEqual(await facturarYEsperar(fac.url, { pedidoId: "ped-001" }, { cadaMs: 10 }), { estado: "lista", numero: 1 });
      assert.equal(preguntas, 3);
    } finally { await fac.cerrar(); }
  });

  test("fila 8: el webhook de la pasarela verifica la firma, la tolerancia y deduplica", async () => {
    const { recibirCobro } = await importa("antes-http", "http.mjs");
    const yaVistos = new Set();
    const guardados = [];
    const s = await servidor(recibirCobro({ secreto: "s3cret", yaVistos, guardar: (c) => guardados.push(c) }), { crudo: true });
    const envia = (cuerpo, { secreto = "s3cret", t = Math.floor(Date.now() / 1000) } = {}) =>
      fetch(s.url, { method: "POST", headers: { "x-pasarela-firma": `t=${t},v1=${createHmac("sha256", secreto).update(`${t}.${cuerpo}`).digest("hex")}` }, body: cuerpo });
    try {
      const cuerpo = JSON.stringify({ id: "evt_1", amount: 9000 });
      assert.equal((await envia(cuerpo)).status, 200);
      assert.equal((await envia(cuerpo)).status, 200);
      assert.equal(guardados.length, 1, "el mismo aviso, un efecto");
      assert.equal((await envia(JSON.stringify({ id: "evt_2" }), { secreto: "otro" })).status, 401);
      assert.equal((await envia(JSON.stringify({ id: "evt_3" }), { t: Math.floor(Date.now() / 1000) - 3600 })).status, 401);
      assert.equal(guardados.length, 1);
    } finally { await s.cerrar(); }
  });

  test("fila 9: la entrega saliente reintenta un 503 y no reintenta un 400", async () => {
    const { entregarConReintentos } = await importa("antes-http", "http.mjs");
    let n = 0;
    const flaky = await servidor((req, res) => { res.statusCode = ++n < 3 ? 503 : 200; res.end(); });
    const rechaza = await servidor((req, res) => { res.statusCode = 400; res.end(); });
    try {
      assert.equal(await entregarConReintentos(flaky.url, { id: "ev-1" }, { esperaMs: 5 }), 3);
      assert.equal(flaky.peticiones[0].cabeceras["idempotency-key"], "ev-1");
      await assert.rejects(() => entregarConReintentos(rechaza.url, { id: "ev-2" }, { esperaMs: 5 }), /rechazó/);
      assert.equal(rechaza.peticiones.length, 1);
    } finally { await flaky.cerrar(); await rechaza.cerrar(); }
  });

  test("la migración gradual: el productor hace las DOS cosas, y se cuenta quién usa el endpoint viejo", async () => {
    const { registrarCliente, contarUso } = await importa("antes-http", "migracion-gradual.mjs");
    const publicados = [];
    const receptor = await servidor((q, r) => r.end());
    try {
      await registrarCliente({ bus: { publish: async (...a) => publicados.push(a) }, suscriptoresHttp: [receptor.url], cliente: { clienteId: "cli-1", email: "a@b.c", plan: "oro" } });
      assert.deepEqual(publicados, [["clientes.cliente.v1.registrado", { clienteId: "cli-1", aggregateVersion: 1, plan: "oro" }, { aggregateId: "cli-1" }]]);
      assert.equal(JSON.parse(receptor.peticiones[0].cuerpo).evento, "cliente.registrado", "y el webhook de siempre");
      assert.ok(!JSON.stringify(publicados).includes("a@b.c"), "el evento NO lleva el correo");

      const { servirCambios, cambios } = await importa("antes-http", "http.mjs");
      cambios.length = 0;
      const { medido, uso } = contarUso(servirCambios);
      const viejo = await servidor((req, res) => medido(req, res));
      try {
        await fetch(`${viejo.url}/cambios?desde=0`);
        await fetch(`${viejo.url}/cambios?desde=0`);
        assert.equal([...uso.values()][0].peticiones, 2);
      } finally { await viejo.cerrar(); }
    } finally { await receptor.cerrar(); }
  });
});

// ─── 3. PHP y curl ───────────────────────────────────────────────────────────────────────────────────────────────

describe("el «antes» en PHP", { skip: !PHP && !exigePhp ? "no hay php" : false }, () => {
  test("ficha-antes.php lee las dos APIs del CRM (y `php -l` pasa)", async () => {
    if (exigePhp) assert.fail("FLUX_REQUIRE_PHP: no hay php con curl");
    const l = spawnSync("php", ["-l", path.join(DIR, "antes-http", "ficha-antes.php")], { encoding: "utf8" });
    assert.equal(l.status, 0, l.stdout + l.stderr);
    const crm = await servidor((req, res) => json(res, 200, req.url.startsWith("/clientes/") ? { nombre: "Ana" } : { descuentoCents: 999 }));
    try {
      // Asíncrono: el servidor de mentira vive en ESTE proceso, y un `spawnSync` lo bloquearía mientras PHP espera su respuesta.
      const r = await new Promise((resolve) => {
        const h = spawn("php", [path.join(DIR, "antes-http", "ficha-antes.php"), "cli-1", "9990"], { env: { ...process.env, CRM_URL: crm.url } });
        let out = "";
        h.stdout.on("data", (d) => (out += d));
        h.stderr.on("data", (d) => (out += d));
        h.on("close", (status) => resolve({ status, out }));
      });
      assert.equal(r.status, 0, r.out);
      assert.deepEqual(JSON.parse(r.out), { nombre: "Ana", descuentoCents: 999 });
    } finally { await crm.cerrar(); }
  });
});
