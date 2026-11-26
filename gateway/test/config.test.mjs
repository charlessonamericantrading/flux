/**
 * `gateway.json` — specification/12-edge.md §7.5. Sin broker.
 *
 * La configuración NO contiene secretos: solo nombres de variables de entorno y SHA-256 de tokens. Aquí se intenta, de todas las
 * formas razonables, colar un secreto en claro y se comprueba que el gateway se NIEGA a arrancar diciendo dónde está y cómo se arregla.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import crypto from "node:crypto";

import { cargar } from "./helpers/fuente.mjs";

const { ConfigError, cargarConfig, cargarConfigDeFichero, DEFECTOS } = await cargar("config");
const { nuevoToken, sha256Hex } = await cargar("auth");
const { main } = await cargar("index");

const SEMILLA = "SUAIBDPBAUTWCWBKIO6XHQNINK5FWJW4OHLXC3HQ2KFE4PEJUA44CNHTC4"; // forma de una semilla de NATS de usuario

const ENV = { TIENDA_SEED: "valor-que-no-importa", TIENDA_ENTREGA: "secreto-entrega", HOOK_SECRETO: "secreto-hook", HOOK_NUEVO: "secreto-nuevo" };
const hash = sha256Hex(nuevoToken());

/** Una configuración válida y mínima, que cada prueba estropea en UNA cosa. */
const base = () => ({
  environment: "produccion",
  version: "1.0.0",
  nats: { servers: "nats://127.0.0.1:4222" },
  clients: [
    {
      service: "tienda-web",
      tenantId: "acme",
      credentials: { nkeySeedEnv: "TIENDA_SEED" },
      tokens: [{ id: "tienda-web-1", sha256: hash }],
      hooks: {
        pasarela: {
          subject: "pagos.cobro.v1.recibido",
          verify: { scheme: "hmac-sha256", secretEnv: "HOOK_SECRETO", signature: { header: "X-Firma", format: "t-v1" } },
          externalKey: { from: "body", pointer: "/id" },
          time: { from: "body", pointer: "/created", unit: "seconds" },
        },
      },
      deliveries: [{ subject: "inventario.stock.v1.ajustado", url: "https://tienda.example.com/hooks/flux", secretEnv: "TIENDA_ENTREGA" }],
    },
  ],
});
const cliente = (c) => c.clients[0];

function problemas(cfg, env = ENV) {
  try {
    cargarConfig(cfg, { env });
  } catch (e) {
    assert.ok(e instanceof ConfigError, `debe ser ConfigError, es ${e?.constructor?.name}: ${e?.stack}`);
    return e.problemas;
  }
  return [];
}
const dice = (ps, re) => ps.some((p) => re.test(p.m) || re.test(p.hint ?? ""));

describe("una configuración válida", () => {
  test("se normaliza, con los defectos de la spec", () => {
    const c = cargarConfig(base(), { env: ENV });
    assert.equal(c.clientes.length, 1);
    assert.equal(c.limits.maxBodyBytes, 1_048_576);
    assert.equal(c.limits.serverTimeoutMs, 60_000);
    assert.equal(c.limits.connectionsCheckingIntervalMs, 1000);
    assert.deepEqual(c.clientes[0].credentials, { nkeySeed: ENV.TIENDA_SEED }, "el secreto sale del ENTORNO");
    assert.deepEqual(c.clientes[0].tenants, ["acme"]);
    assert.equal(c.clientes[0].hooks[0].classification, "confidential", "un hook es confidential por defecto");
    assert.deepEqual(c.clientes[0].deliveries[0].allowClassifications, ["public", "internal"]);
    assert.equal(c.clientes[0].deliveries[0].timeoutMs, 10_000);
    assert.equal(c.clientes[0].hooks[0].verify.toleranciaSeg, 300);
    assert.deepEqual(DEFECTOS.limits.rateLimit, { requests: 120, perSeconds: 60 });
  });

  test("la configuración normalizada NO contiene los tokens (solo sus digests)", () => {
    const token = nuevoToken();
    const cfg = base();
    cliente(cfg).tokens = [{ id: "t", sha256: sha256Hex(token) }];
    const c = cargarConfig(cfg, { env: ENV });
    assert.ok(!JSON.stringify(c).includes(token));
  });
});

describe("ningún secreto en claro", () => {
  for (const [clave, valor] of [["nkeySeed", SEMILLA], ["credsContent", "-----BEGIN NATS USER JWT-----"], ["token", "tok-123"], ["pass", "hunter2"], ["password", "x"], ["secret", "s"], ["seed", "x"]]) {
    test(`una clave \`${clave}\` con un valor se rechaza y se explica cómo arreglarlo`, () => {
      const cfg = base();
      cliente(cfg).credentials = { [clave]: valor };
      const ps = problemas(cfg);
      assert.ok(dice(ps, /SECRETO EN CLARO/), JSON.stringify(ps));
      assert.ok(dice(ps, new RegExp(`${clave}Env`)), "dice qué clave usar: <clave>Env");
      assert.ok(dice(ps, /ROTA/), "dice que se rote si ya se versionó");
    });
  }

  test("un secreto en CUALQUIER sitio: la semilla de NATS en un valor, una clave privada PEM, un `secret` de un hook", () => {
    let cfg = base();
    cliente(cfg).hooks.pasarela.verify.secret = "whsec_loquesea";
    assert.ok(dice(problemas(cfg), /SECRETO EN CLARO/), "un secret en claro dentro de verify");

    cfg = base();
    cliente(cfg).credentials = { creds: SEMILLA };
    assert.ok(dice(problemas(cfg), /SEMILLA de NATS/), "una semilla donde iría una ruta");

    cfg = base();
    cliente(cfg).deliveries[0].caFile = "-----BEGIN PRIVATE KEY-----\nMIIE…\n-----END PRIVATE KEY-----";
    assert.ok(dice(problemas(cfg), /CLAVE PRIVADA/));

    cfg = base();
    cfg.$comentario = SEMILLA;
    assert.ok(dice(problemas(cfg), /SEMILLA de NATS/), "ni siquiera en un comentario");
  });

  test("el token de un cliente no puede ir en claro: `sha256` debe ser un SHA-256, y se dice qué falta", () => {
    const cfg = base();
    cliente(cfg).tokens = [{ id: "t", sha256: nuevoToken() }];
    assert.ok(dice(problemas(cfg), /SHA-256 del token/));
    cliente(cfg).tokens = [{ id: "t", token: nuevoToken(), sha256: hash }];
    assert.ok(dice(problemas(cfg), /SECRETO EN CLARO|desconocida «token»/));
  });

  test("una variable de entorno con un nombre que no lo es (el valor del secreto en lugar del nombre) se rechaza SIN citar el valor largo", () => {
    const cfg = base();
    cliente(cfg).credentials = { nkeySeedEnv: SEMILLA };
    const ps = problemas(cfg);
    assert.ok(dice(ps, /NOMBRE de una variable de entorno/));
    assert.ok(!dice(ps, /faltan .*SUAIBD/), "una semilla NO se trata como «variable que falta»: acabaría en un log");
    for (const p of ps) assert.ok(!(p.m + (p.hint ?? "")).includes(SEMILLA.slice(0, 12)), `el mensaje cita el secreto: ${p.m}`);
  });

  test("falta una variable de entorno: dice CUÁL (por su nombre) y dónde se usa, nunca su valor", () => {
    const ps = problemas(base(), { TIENDA_SEED: "x", HOOK_SECRETO: "y" }); // falta TIENDA_ENTREGA
    assert.ok(dice(ps, /faltan 1 variable\(s\) de entorno: TIENDA_ENTREGA \(clients\[0\] \(tienda-web\) deliveries\[0\] secretEnv\)/), JSON.stringify(ps));
    // vacía es como ausente
    assert.ok(dice(problemas(base(), { ...ENV, TIENDA_SEED: "" }), /TIENDA_SEED/));
  });
});

describe("lo que de aceptarse haría algo distinto de lo que parece", () => {
  test("un hook sin `verify` NO arranca: sería un endpoint público que publica eventos", () => {
    const cfg = base();
    delete cliente(cfg).hooks.pasarela.verify;
    const ps = problemas(cfg);
    assert.ok(dice(ps, /falta `verify`/));
    assert.ok(dice(ps, /endpoint público/));
  });

  test("un hook solo admite HMAC-SHA256, con un formato y una cabecera", () => {
    for (const [mut, re] of [
      [(v) => { v.scheme = "none"; }, /solo se admite "hmac-sha256"/],
      [(v) => { v.scheme = "basic"; }, /solo se admite "hmac-sha256"/],
      [(v) => { v.signature.format = "base64"; }, /debe ser hex, sha256-prefix, t-v1/],
      [(v) => { delete v.signature.header; }, /la cabecera que lleva la firma/],
      [(v) => { delete v.secretEnv; }, /secretEnv/],
      [(v) => { v.toleranceSeconds = 0; }, /entero positivo/],
    ]) {
      const cfg = base();
      mut(cliente(cfg).hooks.pasarela.verify);
      assert.ok(dice(problemas(cfg), re), String(re));
    }
  });

  test("un hook idempotente necesita `externalKey` y `time`; sin instante del hecho, se declara `idempotent: false`", () => {
    let cfg = base();
    delete cliente(cfg).hooks.pasarela.time;
    let ps = problemas(cfg);
    assert.ok(dice(ps, /falta `time`/));
    assert.ok(dice(ps, /idempotent\\?": false|idempotent": false/));
    cfg = base();
    delete cliente(cfg).hooks.pasarela.externalKey;
    assert.ok(dice(problemas(cfg), /falta `externalKey`/));
    // con idempotent:false no hacen falta (y no se admiten, para que no parezca que se usan)
    cfg = base();
    cliente(cfg).hooks.pasarela.idempotent = false;
    delete cliente(cfg).hooks.pasarela.externalKey;
    delete cliente(cfg).hooks.pasarela.time;
    assert.deepEqual(problemas(cfg), []);
    cliente(cfg).hooks.pasarela.time = { from: "body", pointer: "/created", unit: "seconds" };
    assert.ok(dice(problemas(cfg), /idempotent: false` con/));
  });

  test("las fuentes de un hook se validan: JSON Pointer, cabecera, unidad", () => {
    for (const [campo, valor, re] of [
      ["externalKey", { from: "query", name: "x" }, /from": "body"/],
      ["externalKey", { from: "body", pointer: "id" }, /JSON Pointer/],
      ["externalKey", { from: "header" }, /nombre de la cabecera/],
      ["time", { from: "body", pointer: "/created", unit: "days" }, /`unit` debe ser seconds, milliseconds, iso/],
    ]) {
      const cfg = base();
      cliente(cfg).hooks.pasarela[campo] = valor;
      assert.ok(dice(problemas(cfg), re), `${campo} ${JSON.stringify(valor)}`);
    }
  });

  test("`data.pick` solo con camelCase y JSON Pointers: lo que respeta la regla 5 (sin PII)", () => {
    let cfg = base();
    cliente(cfg).hooks.pasarela.data = { pick: { pagoId: "/id", importeCents: "/amount" } };
    assert.deepEqual(problemas(cfg), []);
    cfg = base();
    cliente(cfg).hooks.pasarela.data = { pick: { pago_id: "/id" } };
    assert.ok(dice(problemas(cfg), /camelCase/));
    cfg = base();
    cliente(cfg).hooks.pasarela.data = { pick: { email: "customer.email" } };
    assert.ok(dice(problemas(cfg), /JSON Pointers/));
  });

  test("un hook no puede publicar `restricted`", () => {
    const cfg = base();
    cliente(cfg).hooks.pasarela.classification = "restricted";
    assert.ok(dice(problemas(cfg), /restricted/));
  });

  test("dos entregas para el mismo subject se rechazan: comparten durable y se reparten los mensajes (A-47)", () => {
    const cfg = base();
    cliente(cfg).deliveries.push({ subject: "inventario.stock.v1.ajustado", url: "https://otra.example.com/h", secretEnv: "TIENDA_ENTREGA" });
    const ps = problemas(cfg);
    assert.ok(dice(ps, /ya hay una entrega para inventario.stock.v1.ajustado/));
    assert.ok(dice(ps, /REPARTE los mensajes/));
  });

  test("dos hooks con el mismo nombre, en clientes distintos, se rechazan: la URL identifica al cliente por el nombre", () => {
    const cfg = base();
    cfg.clients.push({ ...structuredClone(cliente(cfg)), service: "otra-web", tokens: [{ id: "otra-1", sha256: sha256Hex(nuevoToken()) }], deliveries: [] });
    const ps = problemas(cfg);
    assert.ok(dice(ps, /el nombre ya es de tienda-web/));
  });

  test("un token repetido o compartido entre clientes, o un id repetido, se rechazan", () => {
    let cfg = base();
    cfg.clients.push({ ...structuredClone(cliente(cfg)), service: "otra-web", tokens: [{ id: "otra-1", sha256: hash }], hooks: {}, deliveries: [] });
    assert.ok(dice(problemas(cfg), /ese token ya es de otro cliente/));
    cfg = base();
    cfg.clients.push({ ...structuredClone(cliente(cfg)), service: "otra-web", tokens: [{ id: "tienda-web-1", sha256: sha256Hex(nuevoToken()) }], hooks: {}, deliveries: [] });
    assert.ok(dice(problemas(cfg), /el id «tienda-web-1» está repetido/));
  });

  test("el mismo servicio dos veces se rechaza: una identidad NATS por cliente", () => {
    const cfg = base();
    cfg.clients.push({ ...structuredClone(cliente(cfg)), tokens: [{ id: "x", sha256: sha256Hex(nuevoToken()) }], hooks: {}, deliveries: [] });
    assert.ok(dice(problemas(cfg), /aparece dos veces/));
  });

  test("credenciales: exactamente UN método", () => {
    let cfg = base();
    cliente(cfg).credentials = { nkeySeedEnv: "TIENDA_SEED", tokenEnv: "HOOK_SECRETO" };
    assert.ok(dice(problemas(cfg), /UN método \(hay 2\)/));
    cfg = base();
    cliente(cfg).credentials = {};
    assert.ok(dice(problemas(cfg), /UN método \(hay 0\)/));
    cfg = base();
    cliente(cfg).credentials = { userEnv: "TIENDA_SEED" };
    assert.ok(dice(problemas(cfg), /`userEnv` y `passEnv` a la vez/));
    cfg = base();
    cliente(cfg).credentials = { creds: "/etc/flux/tienda-web.creds" };
    assert.deepEqual(cargarConfig(cfg, { env: ENV }).clientes[0].credentials, { creds: "/etc/flux/tienda-web.creds" }, "una RUTA no es un secreto");
  });

  test("una entrega: https, o http con la decisión explícita; sin credenciales en la URL; plazo por debajo del ack_wait", () => {
    for (const [mut, re] of [
      [(e) => { e.url = "http://tienda.example.com/h"; }, /integridad, no confidencialidad/],
      [(e) => { e.url = "ftp://x/h"; }, /URL http\(s\) absoluta/],
      [(e) => { e.url = "no es una url"; }, /URL http\(s\) absoluta/],
      [(e) => { e.url = "https://usuario:clave@tienda.example.com/h"; }, /usuario o contraseña/],
      [(e) => { e.timeoutMs = 30_000; }, /entre 1 y 25000/],
      [(e) => { e.timeoutMs = 0; }, /entre 1 y 25000/],
      [(e) => { delete e.secretEnv; }, /falta `secretEnv`/],
      [(e) => { e.allowClassifications = ["restricted"]; }, /restricted/],
      [(e) => { e.allowClassifications = []; }, /allowClassifications/],
      [(e) => { delete e.subject; }, /`subject` .* o `contract`/],
      [(e) => { e.contract = "catalogo.producto.v1"; }, /no ambos ni ninguno/],
    ]) {
      const cfg = base();
      mut(cliente(cfg).deliveries[0]);
      assert.ok(dice(problemas(cfg), re), String(re));
    }
    const cfg = base();
    cliente(cfg).deliveries[0].url = "http://10.0.0.5/h";
    cliente(cfg).deliveries[0].allowInsecureHttp = true;
    assert.deepEqual(problemas(cfg), []);
  });

  test("una entrega de ESTADO necesita tenant y autorizar `confidential` de forma explícita", () => {
    const cfg = base();
    cliente(cfg).deliveries = [{ contract: "catalogo.producto.v1", tenant: "acme", url: "https://x.example.com/h", secretEnv: "TIENDA_ENTREGA" }];
    assert.ok(dice(problemas(cfg), /confidential/), "sin autorizar confidential");
    cliente(cfg).deliveries[0].allowClassifications = ["public", "internal", "confidential"];
    assert.deepEqual(problemas(cfg), []);
    delete cliente(cfg).deliveries[0].tenant;
    assert.ok(dice(problemas(cfg), /necesita `tenant`/));
  });

  test("tokens: al menos uno, con id y SHA-256", () => {
    let cfg = base();
    cliente(cfg).tokens = [];
    assert.ok(dice(problemas(cfg), /al menos un token/));
    cfg = base();
    delete cliente(cfg).tokens;
    assert.ok(dice(problemas(cfg), /al menos un token/));
  });

  test("tenant por defecto obligatorio; `tenants` incluye el por defecto", () => {
    let cfg = base();
    delete cliente(cfg).tenantId;
    assert.ok(dice(problemas(cfg), /`tenantId` es obligatorio/));
    cfg = base();
    cliente(cfg).tenants = ["globex"];
    assert.ok(dice(problemas(cfg), /debe incluir el `tenantId`/));
    cfg = base();
    cliente(cfg).tenants = ["acme", "globex"];
    assert.deepEqual(cargarConfig(cfg, { env: ENV }).clientes[0].tenants, ["acme", "globex"]);
  });

  test("los límites: 1 MiB como máximo, y los tiempos con el intervalo de comprobación (A-153)", () => {
    for (const [limits, re] of [
      [{ maxBodyBytes: 2_000_000 }, /maxBodyBytes/],
      [{ serverTimeoutMs: 10_000 }, /serverTimeoutMs.*25000|supere/i],
      [{ connectionsCheckingIntervalMs: 30_000 }, /connectionsCheckingIntervalMs/],
      [{ headersTimeoutMs: 40_000 }, /headersTimeoutMs.*requestTimeoutMs/],
      [{ rateLimit: { requests: 0 } }, /requests.*perSeconds/],
    ]) {
      const cfg = base();
      cfg.limits = limits;
      assert.ok(dice(problemas(cfg), re), JSON.stringify(limits));
    }
  });

  test("una clave desconocida (una errata) se rechaza en cada nivel: no puede ser «sin esa opción»", () => {
    for (const [donde, mut] of [
      ["raíz", (c) => { c.clientes = []; }],
      ["cliente", (c) => { cliente(c).deliverys = []; }],
      ["hook", (c) => { cliente(c).hooks.pasarela.idempotente = false; }],
      ["entrega", (c) => { cliente(c).deliveries[0].timeout = 5; }],
      ["credenciales", (c) => { cliente(c).credentials.seed = "x"; }],
    ]) {
      const cfg = base();
      mut(cfg);
      assert.ok(problemas(cfg).length > 0, donde);
    }
  });

  test("recoge TODOS los problemas de una vez, no el primero", () => {
    const cfg = base();
    cliente(cfg).credentials = {};
    delete cliente(cfg).tokens;
    delete cliente(cfg).hooks.pasarela.verify;
    cliente(cfg).deliveries[0].url = "http://x/h";
    assert.ok(problemas(cfg).length >= 4);
  });

  test("L3 estricta necesita el bundle", () => {
    const cfg = base();
    cfg.validation = "strict";
    assert.ok(dice(problemas(cfg), /bundleFile/));
  });
});

describe("leer un fichero y los comandos", () => {
  test("un fichero ilegible o que no es JSON da un ConfigError con la causa", () => {
    assert.throws(() => cargarConfigDeFichero("/no/existe/gateway.json"), (e) => e instanceof ConfigError && /ENOENT/.test(e.message));
    const dir = mkdtempSync(join(tmpdir(), "flux-gw-cfg-"));
    try {
      writeFileSync(join(dir, "roto.json"), "{ no es json");
      assert.throws(() => cargarConfigDeFichero(join(dir, "roto.json")), (e) => e instanceof ConfigError && /no es JSON válido/.test(e.message));
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("`flux-gateway token` imprime el token UNA vez y su SHA-256, y el hash corresponde", async () => {
    let salida = "";
    let error = "";
    const codigo = await main(["token"], { stdout: { write: (s) => (salida += s) }, stderr: { write: (s) => (error += s) } });
    assert.equal(codigo, 0);
    const token = /token:  (flx_\S+)/.exec(salida)[1];
    const sha = /sha256: ([0-9a-f]{64})/.exec(salida)[1];
    assert.equal(sha, crypto.createHash("sha256").update(token).digest("hex"));
    assert.ok(!error.includes(token), "el token no va a stderr");
    assert.match(error, /no guarda el token/);
  });

  test("`--check` valida el fichero y las variables de entorno, y sale con 1 si no es válido", async () => {
    const dir = mkdtempSync(join(tmpdir(), "flux-gw-cfg-"));
    try {
      const f = join(dir, "gateway.json");
      const cfg = base();
      cliente(cfg).credentials = { nkeySeed: SEMILLA };
      writeFileSync(f, JSON.stringify(cfg));
      let error = "";
      const codigo = await main(["--check", "--config", f], { stdout: { write() {} }, stderr: { write: (s) => (error += s) } });
      assert.equal(codigo, 1);
      assert.match(error, /SECRETO EN CLARO/);
      assert.ok(!error.includes(SEMILLA), "el mensaje de error no repite el secreto");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
