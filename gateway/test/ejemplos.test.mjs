/**
 * El ejemplo (`gateway/examples/`) es coherente: `gateway.json` y `services.json` dicen lo mismo, y todo lo que el gateway hace por un
 * cliente está CONCEDIDO por el manifiesto (el gateway no necesita ningún permiso especial: specification/12-edge.md §3). Sin broker.
 *
 * Una incoherencia aquí es un fallo que el broker descubriría en producción como un `PERMISSION_DENIED`: un hook que publica en un
 * dominio que el cliente no posee, una entrega de un evento que el cliente no consume, un cliente que el manifiesto no marca `via: gateway`.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { analizarManifiesto, expandConsumes, loadBundle, permisosDe } from "../../scripts/lib/manifest.mjs";

import { cargar } from "./helpers/fuente.mjs";

const { cargarConfig } = await cargar("config");

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const EJ = path.join(RAIZ, "gateway", "examples");
const leer = (f) => JSON.parse(readFileSync(path.join(EJ, f), "utf8"));

/** Todas las variables de entorno que nombra una configuración, con un valor de relleno (una semilla de NATS bien formada no hace falta aquí). */
function entornoDe(cfg) {
  const env = {};
  (function recorre(v) {
    if (Array.isArray(v)) return v.forEach(recorre);
    if (v && typeof v === "object") {
      for (const [k, x] of Object.entries(v)) {
        if (/Env$/.test(k)) for (const n of [].concat(x)) env[n] = "valor-de-relleno";
        recorre(x);
      }
    }
  })(cfg);
  return env;
}

/** Lo que gateway.json configura y el manifiesto no concede. */
function incoherencias(cfg, manifiesto, bundle) {
  const problemas = [];
  const { servicios } = analizarManifiesto(manifiesto, bundle);
  const porNombre = new Map(servicios.map((s) => [s.name, s]));
  const crudo = new Map(manifiesto.services.map((s) => [s.name, s]));
  const config = cargarConfig(cfg, { env: entornoDe(cfg) });

  for (const c of config.clientes) {
    const s = porNombre.get(c.service);
    if (!s) { problemas.push(`${c.service}: no está en el manifiesto`); continue; }
    if (crudo.get(c.service).via !== "gateway") problemas.push(`${c.service}: el manifiesto no lo marca \`"via": "gateway"\``);
    const dominio = (subject) => subject.split(".")[0];
    for (const h of c.hooks) if (!s.owns.includes(dominio(h.subject))) problemas.push(`${c.service} hook «${h.nombre}»: publica en ${h.subject}, de un dominio que ${c.service} no posee (${s.owns.join(", ") || "ninguno"})`);
    const permisos = permisosDe(s);
    for (const d of c.deliveries) {
      if (d.kind === "event" && !s.subjects.includes(d.subject)) problemas.push(`${c.service}: entrega ${d.subject}, que el manifiesto no le concede en \`consumes\``);
      if (d.kind === "state" && !s.watches.some((w) => w.contract === d.contract && (!w.tenants || w.tenants.includes(d.tenant)))) problemas.push(`${c.service}: entrega el estado de ${d.contract} (${d.tenant}), que el manifiesto no le concede en \`watches\``);
    }
    for (const t of c.tenants) if (t !== c.tenantId) void t; // los tenants adicionales los confina el broker (watches con tenants): no se comprueban aquí
    void permisos;
  }
  for (const s of manifiesto.services) if (s.via === "gateway" && !config.clientes.some((c) => c.service === s.name)) problemas.push(`${s.name}: el manifiesto lo marca \`via: gateway\` y gateway.json no tiene ese cliente`);
  return problemas;
}

describe("el ejemplo del gateway", () => {
  test("services.json pasa `gen-acl.mjs --check` con el bundle del ejemplo, y los clientes de borde reciben permisos normales", () => {
    const r = spawnSync(process.execPath, [path.join(RAIZ, "scripts", "gen-acl.mjs"), path.join(EJ, "services.json"), "--bundle", path.join(EJ, "bundle.json"), "--json"], { encoding: "utf8" });
    assert.equal(r.status, 0, r.stderr);
    const filas = JSON.parse(r.stdout);
    const borde = filas.filter((f) => f.via === "gateway").map((f) => f.name).sort();
    assert.deepEqual(borde, ["catalogo-web", "pasarela-web", "tienda-web"]);
    for (const f of filas) {
      assert.ok(!f.publish.some((p) => p === "$JS.API.>" || p === ">"), `${f.name}: ningún permiso ancho`);
      assert.deepEqual(f.subscribe.filter((x) => !x.startsWith("rpc.")), [`_INBOX.${f.name}.>`], `${f.name}: solo su inbox privado`);
    }
    // el gateway no necesita permisos especiales: ninguno de los clientes de borde sirve operaciones
    for (const f of filas.filter((x) => x.via === "gateway")) assert.deepEqual([f.serves, f.allowResponses], [[], null]);
  });

  test("gateway.json es válido y no contiene secretos (solo nombres de variables y SHA-256)", () => {
    const cfg = leer("gateway.json");
    const config = cargarConfig(cfg, { env: entornoDe(cfg) });
    assert.equal(config.clientes.length, 3);
    const texto = JSON.stringify(cfg);
    assert.ok(!/SU[A-Z2-7]{50,}/.test(texto) && !/whsec_/.test(texto) && !/flx_/.test(texto), "ni semillas, ni secretos de webhook, ni tokens");
    assert.ok(config.clientes.every((c) => c.tokens.every((t) => /^[0-9a-f]{64}$/.test(t.sha256))));
  });

  test("gateway.json y services.json son COHERENTES: todo lo que el gateway hace por un cliente está concedido por el manifiesto", async () => {
    const bundle = await loadBundle(path.join(EJ, "bundle.json"));
    assert.deepEqual(incoherencias(leer("gateway.json"), leer("services.json"), bundle), []);
  });

  describe("y la comprobación de coherencia DETECTA lo incoherente (en negativo)", async () => {
    const bundle = await loadBundle(path.join(EJ, "bundle.json"));
    const casos = {
      "un cliente que el manifiesto no marca via: gateway": (c, m) => { delete m.services.find((s) => s.name === "tienda-web").via; },
      "un cliente que el manifiesto no conoce": (c) => { c.clients.push({ ...structuredClone(c.clients[0]), service: "otro-web", tokens: [{ id: "otro", sha256: "a".repeat(64) }], deliveries: [] }); },
      "un hook que publica en un dominio ajeno": (c) => { c.clients[2].hooks.pasarela.subject = "pedidos.pedido.v1.creado"; },
      "una entrega de un evento que no consume": (c) => { c.clients[0].deliveries[0].subject = "pedidos.pedido.v1.creado"; },
      "una entrega de estado que no observa": (c) => { c.clients[1].deliveries = [{ contract: "catalogo.producto.v1", tenant: "acme", url: "https://x.example.com/h", secretEnv: "OTRO_SECRETO", allowClassifications: ["confidential"] }]; },
      "un cliente de borde del manifiesto sin su entrada en gateway.json": (c) => { c.clients.pop(); },
    };
    for (const [nombre, estropea] of Object.entries(casos)) {
      test(nombre, () => {
        const cfg = leer("gateway.json");
        const man = leer("services.json");
        estropea(cfg, man);
        assert.ok(incoherencias(cfg, man, bundle).length > 0, "no se detectó");
      });
    }
  });

  test("demo.sh (el script de curl del README) es sintácticamente válido y falla con un mensaje claro si falta algo", () => {
    const sintaxis = spawnSync("bash", ["-n", path.join(EJ, "demo.sh")], { encoding: "utf8" });
    assert.equal(sintaxis.status, 0, sintaxis.stderr);
    const sinEntorno = spawnSync("bash", [path.join(EJ, "demo.sh")], { encoding: "utf8", env: { PATH: process.env.PATH } });
    assert.notEqual(sinEntorno.status, 0);
    assert.match(sinEntorno.stderr, /GATEWAY_URL/);
  });
});
