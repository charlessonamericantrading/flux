/**
 * Lo MEDIDO (Apéndice A del plan v1.1: A-150 a A-154, A-161 y A-162), reproducido. Cada prueba afirma el hecho que respalda una
 * decisión de `specification/12-edge.md`, con cotas anchas (la máquina va cargada): si Node o el broker cambian de comportamiento,
 * falla ESTA prueba y no el gateway «de rebote». Las cifras de la tabla del Apéndice A salen de aquí.
 *
 * Las de HTTP no necesitan broker; A-150 sí (sin `nats-server` se SALTA con aviso, salvo con FLUX_REQUIRE_BROKER=1, donde FALLA).
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { execFileSync } from "node:child_process";
import http from "node:http";
import https from "node:https";
import net from "node:net";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

import { connect } from "@flux/sdk";

import { describeBroker, dormir } from "./helpers/entorno.mjs";
import { levantarEcosistema } from "./helpers/ecosistema.mjs";
import { cargar } from "./helpers/fuente.mjs";

const { enviar } = await cargar("entrega");

const escucha = async (servidor) => {
  await new Promise((r) => servidor.listen(0, "127.0.0.1", r));
  return servidor.address().port;
};
const cierra = async (servidor) => {
  servidor.closeAllConnections?.();
  await new Promise((r) => servidor.close(r));
};

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════
describeBroker(describe, test, "A-150: varias identidades NATS en UN proceso (lo que aloja el gateway)", () => {
  test("tres connect() con SU nkey cada uno: cada petición recibe SU respuesta; el que no tiene `calls` lo deniega el broker; los inboxes no se mezclan", async () => {
    const eco = await levantarEcosistema();
    const buses = [];
    try {
      const base = (service) => ({ servers: eco.url, service, environment: "produccion", version: "1.0.0", tenantId: "acme", schemaBaseUrl: "https://schemas.internal", credentials: eco.credenciales(service) });
      const abre = async (s) => { const b = await connect(base(s)); buses.push(b); return b; };
      const precios = await abre("precios-api");
      let ejecutadas = 0;
      await precios.respond("precios.tarifa.v1.calcular", async (req) => { ejecutadas++; return { eco: req.n }; });
      const tienda = await abre("tienda-web");
      const tienda2 = await abre("tienda-web"); // la misma identidad, otra conexión
      const catalogo = await abre("catalogo-web");
      const pasarela = await abre("pasarela-web");

      assert.deepEqual(await tienda.request("precios.tarifa.v1.calcular", { n: 1 }), { eco: 1 });

      // catalogo-web no tiene `calls`: lo deniega el BROKER, en milisegundos, y el dueño no ejecuta nada
      const t0 = Date.now();
      await assert.rejects(catalogo.request("precios.tarifa.v1.calcular", { n: 2 }), (e) => e.code === "PERMISSION_DENIED");
      assert.ok(Date.now() - t0 < 2000, `denegado en ${Date.now() - t0} ms`);

      const res = await Promise.all(Array.from({ length: 100 }, (_, i) => (i % 2 ? tienda : tienda2).request("precios.tarifa.v1.calcular", { n: i }).then((r) => r.eco === i)));
      assert.ok(res.every(Boolean), "las 100 concurrentes reciben cada una LA SUYA");
      assert.equal(ejecutadas, 101, "1 + 100: la denegada no se ejecutó");

      // pasarela-web no publica en el dominio de otro
      await assert.rejects(pasarela.publish("pedidos.pedido.v1.creado", { x: 1 }), (e) => e.code === "PERMISSION_DENIED");

      // el inbox de otro y el común: denegados por el broker
      const nc = await eco.conectar("catalogo-web");
      try {
        nc.subscribe("_INBOX.tienda-web.>");
        nc.subscribe("_INBOX.>");
        await nc.flush();
        await dormir(300);
        assert.ok(nc.violaciones.some((v) => v.includes('Subscription to "_INBOX.tienda-web.>"')));
        assert.ok(nc.violaciones.some((v) => v.includes('Subscription to "_INBOX.>"')));
      } finally {
        await nc.close();
      }

      // cerrar una conexión no afecta a las demás
      await tienda2.close();
      assert.deepEqual(await tienda.request("precios.tarifa.v1.calcular", { n: 999 }), { eco: 999 });
    } finally {
      await Promise.allSettled(buses.map((b) => b.close()));
      await eco.parar();
    }
  });
});

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════
describe("A-151: un cliente que corta a mitad de una petición", () => {
  test("el handler no se entera: termina, el efecto se aplica UNA vez, `close` llega sin `writableFinished`, y escribir después no lanza", async () => {
    let efecto = 0;
    let cierre = null;
    let escritura = null;
    const srv = http.createServer(async (req, res) => {
      res.on("close", () => { cierre = { writableFinished: res.writableFinished, destroyed: res.destroyed }; });
      await dormir(600);
      efecto++;
      try {
        res.writeHead(200);
        res.end("{}");
        escritura = "sin error";
      } catch (e) {
        escritura = String(e);
      }
    });
    const puerto = await escucha(srv);
    try {
      const c = net.connect(puerto, "127.0.0.1");
      c.on("error", () => {});
      c.write("POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\n{}");
      await dormir(100);
      c.destroy();
      const t0 = Date.now();
      while (escritura === null && Date.now() - t0 < 10_000) await dormir(50); // holgado: la máquina puede ir cargada
      assert.equal(efecto, 1, "el efecto se aplicó UNA vez");
      assert.deepEqual(cierre, { writableFinished: false, destroyed: true });
      assert.equal(escritura, "sin error", "escribir en una respuesta cuyo cliente se fue NO lanza");
    } finally {
      await cierra(srv);
    }
  });
});

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════
describe("A-152: el límite de cuerpo de node:http", () => {
  const LIM = 1024 * 1024;

  /** Un cliente que declara 50 MiB y los va enviando mientras el servidor responde 413 de inmediato. */
  async function declarando50MiB({ cerrar }) {
    let leidos = 0;
    const srv = http.createServer((req, res) => {
      req.on("data", (d) => (leidos += d.length));
      res.writeHead(413, cerrar ? { connection: "close" } : {});
      res.end("{}", () => { if (cerrar) req.destroy(); });
    });
    const puerto = await escucha(srv);
    const c = net.connect(puerto, "127.0.0.1");
    let respuesta = "";
    let cerrada = false;
    let enviados = 0;
    c.on("data", (d) => (respuesta += d));
    c.on("error", () => {});
    c.once("close", () => (cerrada = true));
    c.write(`POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: ${50 * LIM}\r\n\r\n`);
    const trozo = Buffer.alloc(64 * 1024, 0x61);
    const t0 = Date.now();
    while (Date.now() - t0 < 1500 && !c.destroyed) {
      if (!c.write(trozo)) await Promise.race([new Promise((r) => c.once("drain", r)), dormir(200)]);
      enviados += trozo.length;
    }
    c.destroy();
    await cierra(srv);
    return { estado: respuesta.split("\r\n")[0], leidos, enviados, cerrada };
  }

  test("sin cerrar la conexión, Node LEE Y DESCARTA todo lo declarado (50 MiB) aunque ya haya respondido 413; cerrándola, lee ~64 KiB", async () => {
    const sin = await declarando50MiB({ cerrar: false });
    assert.match(sin.estado, /413/);
    assert.ok(sin.leidos > 20 * LIM, `sin cerrar, el servidor leyó ${sin.leidos} bytes de los 52 428 800 declarados`);
    const con = await declarando50MiB({ cerrar: true });
    assert.match(con.estado, /413/);
    assert.ok(con.leidos < 2 * LIM, `cerrando, el servidor leyó solo ${con.leidos} bytes`);
    assert.ok(con.cerrada, "y la conexión se cerró");
  });

  test("un cuerpo *chunked* sin longitud declarada se corta a un trozo del límite", async () => {
    let leidos = 0;
    let cortado = false;
    const srv = http.createServer((req, res) => {
      req.on("data", (d) => {
        leidos += d.length;
        if (leidos > LIM && !cortado) {
          cortado = true;
          res.writeHead(413, { connection: "close" });
          res.end("{}", () => req.destroy());
        }
      });
    });
    const puerto = await escucha(srv);
    try {
      const c = net.connect(puerto, "127.0.0.1");
      c.on("error", () => {});
      c.on("data", () => {});
      c.write("POST /x HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n");
      const trozo = Buffer.alloc(64 * 1024, 0x61);
      const t0 = Date.now();
      while (Date.now() - t0 < 1500 && !c.destroyed) {
        c.write(`${trozo.length.toString(16)}\r\n`);
        if (!c.write(Buffer.concat([trozo, Buffer.from("\r\n")]))) await Promise.race([new Promise((r) => c.once("drain", r)), dormir(200)]);
      }
      c.destroy();
      assert.ok(cortado);
      assert.ok(leidos - LIM < 256 * 1024, `se cortó a ${leidos - LIM} bytes del límite`);
    } finally {
      await cierra(srv);
    }
  });
});

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════
describe("A-153: clientes lentos y tiempos de node:http", () => {
  const goteo = (puerto, inicio, relleno, ms) =>
    new Promise((resolve) => {
      const c = net.connect(puerto, "127.0.0.1");
      let resp = "";
      const t0 = Date.now();
      c.on("data", (d) => (resp += d));
      c.on("error", () => {});
      c.write(inicio);
      const f = setInterval(() => { if (!c.destroyed) c.write(relleno); }, 150);
      const fin = setTimeout(() => c.destroy(), ms);
      c.once("close", () => { clearInterval(f); clearTimeout(fin); resolve({ respuesta: resp.split("\r\n")[0], ms: Date.now() - t0 }); });
    });

  test("los valores por defecto de Node 22", () => {
    const s = http.createServer();
    assert.equal(s.headersTimeout, 60_000);
    assert.equal(s.requestTimeout, 300_000);
    assert.equal(s.keepAliveTimeout, 5000);
    assert.equal(http.maxHeaderSize, 16_384);
  });

  test("`headersTimeout` y `requestTimeout` SOLO se aplican cada `connectionsCheckingInterval` (30 s por defecto): con él nada se corta; con 100 ms, 408", async () => {
    const opts = (extra) => ({ ...extra });
    // por defecto: ni a los 3,7 s se corta un cuerpo a cuentagotas con requestTimeout = 1000
    const porDefecto = http.createServer(opts({}), (req, res) => req.on("end", () => res.end("ok")).resume());
    porDefecto.headersTimeout = 500;
    porDefecto.requestTimeout = 1000;
    const p1 = await escucha(porDefecto);
    const a = await goteo(p1, "POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nab", "x", 3700);
    await cierra(porDefecto);
    assert.equal(a.respuesta, "", "sin bajar el intervalo, el servidor NO lo cortó");
    assert.ok(a.ms >= 3600, `solo se cerró cuando lo hizo el cliente (${a.ms} ms)`);

    const rapido = http.createServer(opts({ connectionsCheckingInterval: 100 }), (req, res) => req.on("end", () => res.end("ok")).resume());
    rapido.headersTimeout = 500;
    rapido.requestTimeout = 1000;
    const p2 = await escucha(rapido);
    try {
      const cuerpo = await goteo(p2, "POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nab", "x", 6000);
      assert.match(cuerpo.respuesta, /408/);
      assert.ok(cuerpo.ms < 4000, `408 a los ${cuerpo.ms} ms (requestTimeout 1000)`);
      const cab = await goteo(p2, "POST /x HTTP/1.1\r\nHost: x\r\n", "X-A: b\r\n", 6000);
      assert.match(cab.respuesta, /408/);
      assert.ok(cab.ms < 3000, `408 a los ${cab.ms} ms (headersTimeout 500)`);
    } finally {
      await cierra(rapido);
    }
  });

  test("`server.timeout` cierra una conexión muda, pero mata también una petición legítima que espera al broker más que él: debe superar el timeout máximo del rpc", async () => {
    const srv = http.createServer({ connectionsCheckingInterval: 100 }, (req, res) => setTimeout(() => res.end("ok"), Number(req.url.slice(1))));
    srv.timeout = 1000;
    const puerto = await escucha(srv);
    try {
      const muda = net.connect(puerto, "127.0.0.1");
      muda.on("error", () => {});
      const t0 = Date.now();
      const cerro = await new Promise((r) => { muda.once("close", () => r(Date.now() - t0)); setTimeout(() => r(null), 3000); });
      assert.ok(cerro !== null && cerro < 2500, `la muda se cerró a los ${cerro} ms`);

      const c = net.connect(puerto, "127.0.0.1");
      let resp = "";
      c.on("data", (d) => (resp += d));
      c.on("error", () => {});
      c.write("GET /2500 HTTP/1.1\r\nHost: x\r\n\r\n");
      const t1 = Date.now();
      const murio = await new Promise((r) => { c.once("close", () => r(Date.now() - t1)); setTimeout(() => r(null), 4000); });
      assert.equal(resp, "", "no llegó respuesta");
      assert.ok(murio !== null && murio < 2400, `una petición cuyo handler tarda 2500 ms murió a los ${murio} ms con server.timeout = 1000`);
    } finally {
      await cierra(srv);
    }
  });
});

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════
describe("A-154: comparar tokens y firmas con node:crypto", () => {
  test("`timingSafeEqual` LANZA con longitudes distintas: por eso se comparan los digests SHA-256 (32 bytes siempre)", () => {
    assert.throws(() => crypto.timingSafeEqual(Buffer.from("ab"), Buffer.from("abc")), (e) => e instanceof RangeError && e.code === "ERR_CRYPTO_TIMING_SAFE_EQUAL_LENGTH");
    const d1 = crypto.createHash("sha256").update("un-token-muy-largo-largo-largo").digest();
    const d2 = crypto.createHash("sha256").update("x").digest();
    assert.equal(d1.length, d2.length);
    assert.equal(crypto.timingSafeEqual(d1, d2), false);
  });

  test("`Buffer.from(x, 'hex')` NO falla con un hexadecimal impar o inválido: lo trunca. Se valida el formato ANTES de decodificar", () => {
    assert.equal(Buffer.from("abc", "hex").length, 1, "tres dígitos dan UN byte, sin error");
    assert.equal(Buffer.from("zz", "hex").length, 0);
    assert.equal(Buffer.from("abzz", "hex").length, 1);
  });
});

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════
describe("A-161: el cliente HTTP de salida", () => {
  const POST = (url, extra = {}) => enviar({ url, cuerpo: Buffer.from("{}"), cabeceras: { "content-length": "2" }, timeoutMs: 1500, ...extra }).then((r) => r, (e) => ({ error: e.code ?? e.name }));

  test("una redirección NO se sigue; un servidor que no contesta se corta por el plazo TOTAL; las cabeceras llegan sin esperar al cuerpo; `Retry-After` se lee", async () => {
    const redir = http.createServer((q, r) => { r.writeHead(302, { location: "http://127.0.0.1:1/otro" }); r.end(); });
    const mudo = http.createServer(() => {});
    const eterno = http.createServer((q, r) => { r.writeHead(200); r.write("x"); });
    const r503 = http.createServer((q, r) => { r.writeHead(503, { "retry-after": "7" }); r.end(); });
    const [pr, pm, pe, p5] = await Promise.all([redir, mudo, eterno, r503].map(escucha));
    try {
      const a = await POST(`http://127.0.0.1:${pr}/h`);
      assert.equal(a.status, 302, "el estado de la redirección, sin seguirla");
      const t0 = Date.now();
      const m = await POST(`http://127.0.0.1:${pm}/h`, { timeoutMs: 400 });
      assert.equal(m.error, "PLAZO");
      assert.ok(Date.now() - t0 >= 380 && Date.now() - t0 < 3000);
      const t1 = Date.now();
      const e = await POST(`http://127.0.0.1:${pe}/h`);
      assert.equal(e.status, 200);
      assert.ok(Date.now() - t1 < 1000, "el estado llega aunque el cuerpo no termine nunca");
      const c = await POST(`http://127.0.0.1:${p5}/h`);
      assert.deepEqual([c.status, c.retryAfter], [503, "7"]);
    } finally {
      await Promise.all([redir, mudo, eterno, r503].map(cierra));
    }
  });

  test("puerto cerrado: ECONNREFUSED; nombre inexistente: ENOTFOUND", async () => {
    const s = http.createServer();
    const p = await escucha(s);
    await new Promise((r) => s.close(r));
    assert.equal((await POST(`http://127.0.0.1:${p}/h`)).error, "ECONNREFUSED");
    assert.equal((await POST("http://no-existe.invalid/h")).error, "ENOTFOUND");
  });

  test("TLS: un certificado autofirmado, uno con otro nombre y un puerto que no habla TLS dan los códigos medidos; con la CA, 200", async (t) => {
    const dir = mkdtempSync(path.join(tmpdir(), "flux-gw-tls-"));
    try {
      const cert = (nombre, cn) => {
        execFileSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", `${dir}/${nombre}.key`, "-out", `${dir}/${nombre}.pem`, "-days", "2", "-subj", `/CN=${cn}`, "-addext", `subjectAltName=DNS:${cn}`], { stdio: "ignore" });
        return { key: readFileSync(`${dir}/${nombre}.key`), cert: readFileSync(`${dir}/${nombre}.pem`) };
      };
      const ok = https.createServer(cert("ok", "localhost"), (q, r) => r.end("ok"));
      const otro = https.createServer(cert("otro", "otro.example"), (q, r) => r.end("ok"));
      const plano = http.createServer((q, r) => r.end());
      const [pok, potro, pplano] = await Promise.all([ok, otro, plano].map(escucha));
      try {
        assert.equal((await POST(`https://localhost:${pok}/h`)).error, "DEPTH_ZERO_SELF_SIGNED_CERT");
        assert.equal((await POST(`https://localhost:${pok}/h`, { ca: readFileSync(`${dir}/ok.pem`) })).status, 200);
        assert.equal((await POST(`https://localhost:${potro}/h`, { ca: readFileSync(`${dir}/otro.pem`) })).error, "ERR_TLS_CERT_ALTNAME_INVALID");
        assert.equal((await POST(`https://127.0.0.1:${pplano}/h`)).error, "EPROTO");
      } finally {
        await Promise.all([ok, otro, plano].map(cierra));
      }
    } catch (e) {
      if (/openssl/.test(String(e))) return t.skip("no hay openssl");
      throw e;
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════
describe("A-162: la parada de node:http", () => {
  async function paradaCon({ conexionClose }) {
    let parando = false;
    const srv = http.createServer((req, res) => setTimeout(() => { res.writeHead(200, parando && conexionClose ? { connection: "close" } : {}); res.end("{}"); }, Number(req.url.slice(1))));
    const puerto = await escucha(srv);
    const agente = new http.Agent({ keepAlive: true });
    const get = (ruta) => new Promise((resolve) => {
      const rq = http.get({ host: "127.0.0.1", port: puerto, path: ruta, agent: agente }, (res) => { res.resume(); res.on("end", () => resolve({ status: res.statusCode, conexion: res.headers.connection })); });
      rq.on("error", (e) => resolve({ error: e.code }));
    });
    await get("/0");
    const enVuelo = get("/800");
    await dormir(100);
    parando = true;
    const t0 = Date.now();
    let cerrado = null;
    srv.close(() => (cerrado = Date.now() - t0));
    srv.closeIdleConnections();
    const nueva = await get("/0");
    const respuesta = await enVuelo;
    const espera = conexionClose ? 4000 : 7500;
    for (let i = 0; i < espera / 50 && cerrado === null; i++) await dormir(50);
    agente.destroy();
    srv.closeAllConnections();
    return { nueva, respuesta, cerrado };
  }

  test("`server.close()` deja de escuchar, pero la última respuesta en vuelo sale `keep-alive` y close() tarda el keepAliveTimeout; con `Connection: close` + closeIdleConnections(), termina al acabar la petición", async () => {
    const con = await paradaCon({ conexionClose: true });
    assert.equal(con.nueva.error, "ECONNREFUSED", "deja de aceptar conexiones");
    assert.equal(con.respuesta.status, 200, "lo que volaba termina");
    assert.equal(con.respuesta.conexion, "close");
    assert.ok(con.cerrado !== null && con.cerrado < 3500, `close() terminó a los ${con.cerrado} ms (la petición dura 800; sin Connection: close serían ~6700)`);

    const sin = await paradaCon({ conexionClose: false });
    assert.equal(sin.respuesta.status, 200);
    assert.equal(sin.respuesta.conexion, "keep-alive", "sin Connection: close la conexión de la última respuesta sigue viva");
    assert.ok(sin.cerrado === null || sin.cerrado > 4000, `close() tardó ${sin.cerrado} ms: espera al keepAliveTimeout (5 s)`);
  });
});
