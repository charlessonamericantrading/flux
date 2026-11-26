/**
 * El gateway como PROCESO, contra un nats-server REAL: el límite por token, los clientes lentos, la parada ordenada, el arranque que
 * falla entero y el ejecutable de verdad (`flux-gateway --config gateway.json`, con SIGTERM). Specification/12-edge.md §7.
 *
 * Los tiempos son holgados a propósito (la máquina de CI va cargada): se comprueban cotas anchas y relaciones —«antes que»,
 * «no más de»—, no milisegundos exactos.
 *
 * Sin `nats-server` estas pruebas se SALTAN con aviso, salvo con FLUX_REQUIRE_BROKER=1, donde FALLAN.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import http from "node:http";
import net from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { dec, nuevaClave } from "../../scripts/lib/nats-harness.mjs";

import { connect } from "@flux/sdk";

import { SECRETOS, describeBroker, dormir, esperarHasta, levantarTodo } from "./helpers/entorno.mjs";
import { levantarEcosistema } from "./helpers/ecosistema.mjs";
import { cargar } from "./helpers/fuente.mjs";

const { firmar } = await cargar("firma");
const { cargarConfig } = await cargar("config");
const { iniciarGateway } = await cargar("gateway");
const { crearRegistro } = await cargar("registro");
const { nuevoToken, sha256Hex } = await cargar("auth");

const AQUI = path.dirname(fileURLToPath(import.meta.url));
const INDEX = path.join(AQUI, "..", "src", "index.mjs");
const ahora = () => Math.floor(Date.now() / 1000);

describeBroker(describe, test, "el límite de peticiones por token", () => {
  test("pasada la ráfaga: 429 con Retry-After, sin publicar; cada token tiene su cubo; los webhooks tienen el suyo por NOMBRE de hook", async () => {
    const e = await levantarTodo({
      ajusta(cfg) {
        cfg.limits = { rateLimit: { requests: 5, perSeconds: 60 } };
        cfg.clients[2].hooks.pasarela.rateLimit = { requests: 3, perSeconds: 60 };
      },
    });
    try {
      const evento = (i) => e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { token: e.tokens["tienda-web"], cuerpo: { pedidoId: `rl-${i}` } });
      const rs = [];
      for (let i = 0; i < 6; i++) rs.push(await evento(i));
      assert.deepEqual(rs.map((r) => r.status), [202, 202, 202, 202, 202, 429], "5 de ráfaga y la sexta, rechazada");
      assert.equal(rs[5].json.error.code, "EDGE_RATE_LIMITED");
      assert.equal(rs[5].json.error.class, "retryable");
      assert.ok(Number(rs[5].cabeceras["retry-after"]) >= 1, "con Retry-After");
      assert.match(rs[5].json.error.message, /reintenta en \d+ s/);
      const n = (await e.eco.ops.jsm.streams.info("EVT_PEDIDOS")).state.messages;
      assert.equal(n, 5, "la rechazada NO se publicó");

      // otro token (otro cliente), otro cubo
      const otro = await e.llamar("PUT", "/v1/state/catalogo.producto.v1/rl", { token: e.tokens["catalogo-web"], cuerpo: { aggregateVersion: 1 } });
      assert.equal(otro.status, 200, "agotar un token no afecta a otro");
      // el mismo token sigue agotado en TODAS las rutas
      assert.equal((await e.llamar("POST", "/v1/rpc/precios.tarifa.v1.calcular", { token: e.tokens["tienda-web"], cuerpo: {} })).status, 429);

      // webhooks: su cubo es por nombre, y es de 3
      const cuerpo = JSON.stringify({ id: "evt_rl", created: 1787221600, amount: 1, currency: "EUR" });
      const hook = () => e.llamar("POST", "/v1/hooks/pasarela", { cuerpo, cabeceras: { "x-pasarela-firma": firmar({ secretos: [SECRETOS.PASARELA_HOOK], t: ahora(), cuerpo }) } });
      const hs = [];
      for (let i = 0; i < 4; i++) hs.push(await hook());
      assert.deepEqual(hs.map((r) => r.status), [202, 202, 202, 429]);
      assert.equal(hs[3].json.error.code, "EDGE_RATE_LIMITED");
      // y el otro hook (github, sin límite propio: el de por defecto de 5) no se ve afectado
      assert.equal((await e.llamar("POST", "/v1/hooks/github", { cuerpo: "{}" })).status, 401, "llega a verificar la firma (no está limitado)");
    } finally {
      await e.parar();
    }
  });
});

describeBroker(describe, test, "clientes lentos (A-153)", () => {
  test("un cuerpo o unas cabeceras a cuentagotas se cortan con 408 y NO se publica nada (con el intervalo de comprobación de Node bajado)", async () => {
    const e = await levantarTodo({ limits: { connectionsCheckingIntervalMs: 100, headersTimeoutMs: 800, requestTimeoutMs: 1500, serverTimeoutMs: 30_000 } });
    try {
      const puerto = Number(new URL(e.url).port);
      const gotea = (inicio, relleno, vecesMax = 60) =>
        new Promise((resolve) => {
          const s = net.connect(puerto, "127.0.0.1");
          let resp = "";
          const t0 = Date.now();
          s.on("data", (d) => (resp += d));
          s.on("error", () => {});
          s.write(inicio);
          const f = setInterval(() => { if (!s.destroyed) s.write(relleno); }, 150);
          setTimeout(() => { clearInterval(f); s.destroy(); }, vecesMax * 150);
          s.once("close", () => { clearInterval(f); resolve({ respuesta: resp.split("\r\n")[0], ms: Date.now() - t0 }); });
        });

      const cuerpo = await gotea(`POST /v1/events/pedidos.pedido.v1.creado HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer ${e.tokens["tienda-web"]}\r\nContent-Type: application/json\r\nContent-Length: 500\r\n\r\n{"pedidoId":"`, "x");
      assert.match(cuerpo.respuesta, /408/, "el cuerpo a cuentagotas se corta con 408");
      assert.ok(cuerpo.ms < 8000, `cortado a los ${cuerpo.ms} ms (requestTimeout 1500)`);

      const cabeceras = await gotea("POST /v1/events/pedidos.pedido.v1.creado HTTP/1.1\r\nHost: x\r\n", "X-Lento: a\r\n");
      assert.match(cabeceras.respuesta, /408/, "las cabeceras a cuentagotas, también");
      assert.ok(cabeceras.ms < 8000, `cortado a los ${cabeceras.ms} ms (headersTimeout 800)`);

      assert.equal((await e.eco.ops.jsm.streams.info("EVT_PEDIDOS")).state.messages, 0, "un cuerpo a medias NO se publica");
      assert.equal((await e.llamar("GET", "/healthz")).status, 200, "el gateway sigue sirviendo");
      assert.equal((await e.llamar("POST", "/v1/events/pedidos.pedido.v1.creado", { token: e.tokens["tienda-web"], cuerpo: { pedidoId: "tras-lentos" } })).status, 202);
    } finally {
      await e.parar();
    }
  });
});

describeBroker(describe, test, "la parada ordenada (A-162)", () => {
  test("una petición `rpc` en vuelo TERMINA con 200 y `Connection: close`; lo nuevo se rechaza; todo se cierra rápido y las conexiones NATS se drenan", async () => {
    const e = await levantarTodo();
    const precios = await e.nativo("precios-api");
    await precios.respond("precios.tarifa.v1.calcular", async (req) => { await dormir(req.ms ?? 0); return { ok: true }; });
    try {
      // una conexión keep-alive ociosa y una petición lenta en vuelo
      const agente = new http.Agent({ keepAlive: true });
      const get = (ruta, cuerpo) => new Promise((resolve) => {
        const rq = http.request(`${e.url}${ruta}`, { method: cuerpo ? "POST" : "GET", agent: agente, headers: { authorization: `Bearer ${e.tokens["tienda-web"]}`, ...(cuerpo && { "content-type": "application/json", "content-length": Buffer.byteLength(cuerpo) }) } }, (res) => {
          let t = "";
          res.on("data", (d) => (t += d));
          res.on("end", () => resolve({ status: res.statusCode, conexion: res.headers.connection, t }));
        });
        rq.on("error", (x) => resolve({ error: x.code }));
        rq.end(cuerpo);
      });
      assert.equal((await get("/healthz")).status, 200); // deja la conexión keep-alive ociosa
      const enVuelo = get("/v1/rpc/precios.tarifa.v1.calcular", JSON.stringify({ ms: 800 }));
      await dormir(150);
      assert.equal(e.gw.estado.enVuelo >= 1, true, "la petición está en vuelo");

      const t0 = Date.now();
      const parada = e.gw.cerrar(10_000);
      const r = await enVuelo;
      assert.equal(r.status, 200, "lo que volaba TERMINA, no se corta");
      assert.equal(r.conexion, "close", "con Connection: close: sin esto la conexión de la última respuesta sigue keep-alive y close() tarda 5 s más");
      await parada;
      const ms = Date.now() - t0;
      assert.ok(ms < 4500, `la parada terminó en ${ms} ms (sin Connection: close + closeIdleConnections serían 5 s más: A-162)`);
      assert.ok((await get("/healthz")).error === "ECONNREFUSED", "ya no acepta conexiones");
      for (const { bus } of e.gw.clientes.values()) assert.equal(bus.connected, false, "las conexiones NATS se cerraron (drain)");
      agente.destroy();
      assert.ok(e.lineas.some((l) => /"gateway parado"/.test(l)));
    } finally {
      await precios.close().catch(() => {});
      await e.parar();
    }
  });

  test("una petición que NO termina se corta al vencer el plazo de la parada (no se queda colgado)", async () => {
    const e = await levantarTodo();
    try {
      const s = net.connect(Number(new URL(e.url).port), "127.0.0.1");
      s.on("error", () => {});
      s.resume();
      s.write(`POST /v1/events/pedidos.pedido.v1.creado HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer ${e.tokens["tienda-web"]}\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{"a":`);
      assert.ok(await esperarHasta(() => e.gw.estado.enVuelo >= 1, 3000));
      const t0 = Date.now();
      await e.gw.cerrar(600);
      assert.ok(Date.now() - t0 < 5000, "la parada no espera eternamente");
      assert.ok(e.lineas.some((l) => /parada con peticiones en vuelo/.test(l)), "y lo dice");
      s.destroy();
    } finally {
      await e.parar();
    }
  });
});

describeBroker(describe, test, "el arranque falla ENTERO si un cliente no puede conectar", () => {
  test("credenciales que el broker no acepta: CONNECTION_REJECTED, ningún cliente queda conectado y no se escucha en ningún puerto", async () => {
    const eco = await levantarEcosistema();
    try {
      const intrusa = nuevaClave(); // una nkey que el broker no conoce
      const env = { OK_NKEY: dec.decode(eco.claves["tienda-web"].seed), MALA_NKEY: dec.decode(intrusa.seed) };
      const config = cargarConfig({
        listen: { host: "127.0.0.1", port: 0 }, nats: { servers: eco.url }, environment: "produccion", version: "1.0.0",
        clients: [
          { service: "tienda-web", tenantId: "acme", credentials: { nkeySeedEnv: "OK_NKEY" }, tokens: [{ id: "t-1", sha256: sha256Hex(nuevoToken()) }] },
          { service: "catalogo-web", tenantId: "acme", credentials: { nkeySeedEnv: "MALA_NKEY" }, tokens: [{ id: "c-1", sha256: sha256Hex(nuevoToken()) }] },
        ],
      }, { env });
      const lineas = [];
      const abiertos = [];
      const conectar = async (opciones) => {
        const bus = await connect(opciones);
        abiertos.push(bus);
        return bus;
      };
      await assert.rejects(
        iniciarGateway(config, { registro: crearRegistro({ escribir: (l) => lineas.push(l) }), conectar }),
        (x) => x.code === "CONNECTION_REJECTED" && x.reason === "auth",
      );
      assert.ok(!lineas.some((l) => /gateway escuchando/.test(l)), "no llegó a escuchar");
      assert.ok(abiertos.length >= 1, "al menos la conexión buena llegó a abrirse");
      assert.ok(await esperarHasta(() => abiertos.every((b) => b.connected === false), 5000), "…y se CERRÓ: un gateway a medias no deja conexiones abiertas");
    } finally {
      await eco.parar();
    }
  });
});

describeBroker(describe, test, "el ejecutable de verdad", () => {
  test("`flux-gateway --config gateway.json` arranca como PROCESO, atiende, registra en JSON y para con SIGTERM (exit 0)", async () => {
    const eco = await levantarEcosistema();
    const dir = mkdtempSync(path.join(tmpdir(), "flux-gw-bin-"));
    try {
      const token = nuevoToken();
      const cfg = {
        $comment: "una configuración real: SIN secretos, solo nombres de variables de entorno y el SHA-256 del token",
        listen: { host: "127.0.0.1", port: 0 },
        nats: { servers: eco.url },
        environment: "produccion",
        version: "1.0.0",
        clients: [{ service: "tienda-web", tenantId: "acme", credentials: { nkeySeedEnv: "TIENDA_WEB_NKEY_SEED" }, tokens: [{ id: "tienda-web-1", sha256: sha256Hex(token) }] }],
      };
      const fichero = path.join(dir, "gateway.json");
      writeFileSync(fichero, JSON.stringify(cfg, null, 2));
      const semilla = dec.decode(eco.claves["tienda-web"].seed);
      assert.ok(!JSON.stringify(cfg).includes(semilla) && !JSON.stringify(cfg).includes(token), "gateway.json no contiene ni la semilla ni el token");

      const hijo = spawn(process.execPath, [INDEX, "--config", fichero], { env: { ...process.env, TIENDA_WEB_NKEY_SEED: semilla }, stdio: ["ignore", "pipe", "pipe"] });
      let out = "";
      let err = "";
      hijo.stdout.on("data", (d) => (out += d));
      hijo.stderr.on("data", (d) => (err += d));
      const salida = new Promise((r) => hijo.once("exit", (codigo, senal) => r({ codigo, senal })));
      try {
        assert.ok(await esperarHasta(() => /"gateway escuchando"/.test(out), 15_000), `arrancó:\n${out}\n${err}`);
        const puerto = JSON.parse(out.split("\n").find((l) => l.includes("gateway escuchando"))).puerto;
        const r = await new Promise((resolve) => http.request(`http://127.0.0.1:${puerto}/v1/events/pedidos.pedido.v1.creado`, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" } }, (res) => {
          let t = "";
          res.on("data", (d) => (t += d));
          res.on("end", () => resolve({ status: res.statusCode, t }));
        }).end('{"pedidoId":"bin-1"}'));
        assert.equal(r.status, 202);
        // cada línea de la salida es JSON, sin el token ni la semilla
        for (const l of out.split("\n").filter(Boolean)) JSON.parse(l);
        assert.ok(!out.includes(token) && !out.includes(semilla) && !err.includes(semilla));
        hijo.kill("SIGTERM");
        const { codigo } = await salida;
        assert.equal(codigo, 0, `salió con 0 tras SIGTERM:\n${out}\n${err}`);
        assert.match(out, /"gateway parado"/);
      } finally {
        if (hijo.exitCode === null) hijo.kill("SIGKILL");
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
      await eco.parar();
    }
  });

  test("con una configuración inválida NO arranca: sale con 1 y dice qué arreglar, sin repetir el secreto", async () => {
    const dir = mkdtempSync(path.join(tmpdir(), "flux-gw-bin-"));
    try {
      const fichero = path.join(dir, "gateway.json");
      const semilla = dec.decode(nuevaClave().seed);
      writeFileSync(fichero, JSON.stringify({ environment: "produccion", version: "1", nats: { servers: "nats://x" }, clients: [{ service: "a-web", tenantId: "acme", credentials: { nkeySeed: semilla }, tokens: [] }] }));
      const r = await new Promise((resolve) => {
        const h = spawn(process.execPath, [INDEX, "--config", fichero], { stdio: ["ignore", "pipe", "pipe"] });
        let out = "";
        h.stdout.on("data", (d) => (out += d));
        h.stderr.on("data", (d) => (out += d));
        h.on("exit", (codigo) => resolve({ codigo, out }));
      });
      assert.equal(r.codigo, 1);
      assert.match(r.out, /SECRETO EN CLARO/);
      assert.ok(!r.out.includes(semilla), "el mensaje no repite el secreto");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
