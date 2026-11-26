/**
 * El código y `protocol.json` → `edge` dicen LO MISMO: los valores por defecto, los límites y las rutas. `check-protocol-json.mjs` vigila
 * que el documento sea coherente consigo mismo y con la spec; esto vigila que el gateway de referencia sea coherente con el documento. Sin broker.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { cargar } from "./helpers/fuente.mjs";

const { DEFECTOS, cargarConfig } = await cargar("config");
const { parseRetryAfter } = await cargar("entrega");
const { nuevoToken } = await cargar("auth");

const p = JSON.parse(readFileSync(new URL("../../protocol.json", import.meta.url), "utf8"));
const g = p.edge.gateway;
const d = p.edge.delivery;

describe("los valores por defecto del gateway son los de protocol.json", () => {
  test("límites", () => {
    assert.equal(DEFECTOS.limits.maxBodyBytes, g.limits.maxBodyBytes);
    assert.equal(DEFECTOS.limits.maxBodyBytes, p.data.maxMessageBytes, "el límite del cuerpo es el del evento (1 MiB)");
    assert.equal(DEFECTOS.limits.serverTimeoutMs, g.limits.serverTimeoutMs);
    assert.equal(DEFECTOS.limits.connectionsCheckingIntervalMs, g.limits.connectionsCheckingIntervalMs);
    assert.equal(DEFECTOS.limits.headersTimeoutMs, g.limits.headersTimeoutMs);
    assert.equal(DEFECTOS.limits.requestTimeoutMs, g.limits.requestTimeoutMs);
    assert.ok(DEFECTOS.limits.serverTimeoutMs > p.rpc.timeout.maxMs, "server.timeout supera el timeout máximo del rpc");
  });

  test("la entrega: plazo por defecto, máximo, clasificaciones permitidas y tolerancia de los hooks", () => {
    assert.equal(DEFECTOS.delivery.timeoutMs, d.timeout.defaultMs);
    assert.deepEqual(DEFECTOS.delivery.allowClassifications, d.classification.allowedByDefault);
    assert.equal(DEFECTOS.hookToleranceSeconds, d.signature.receiver.toleranceSeconds);
    assert.equal(DEFECTOS.hookToleranceSeconds, g.auth.hookToleranceSecondsDefault);
    // el máximo (25 s) es el del rpc: la configuración lo aplica
    const env = { X: "x", S: "x" };
    const cfg = (timeoutMs) => ({ environment: "dev", version: "1", nats: { servers: "nats://x" }, clients: [{ service: "a-web", tenantId: "acme", credentials: { nkeySeedEnv: "X" }, tokens: [{ id: "t", sha256: "a".repeat(64) }], deliveries: [{ subject: "a.b.v1.c", url: "https://x/h", secretEnv: "S", timeoutMs }] }] });
    assert.doesNotThrow(() => cargarConfig(cfg(d.timeout.maxMs), { env }));
    assert.throws(() => cargarConfig(cfg(d.timeout.maxMs + 1), { env }));
  });

  test("`Retry-After`: el tope es el de protocol.json", () => {
    assert.equal(parseRetryAfter("99999999"), d.retryAfter.maxMs);
  });

  test("el límite por defecto de peticiones y la forma de un token", () => {
    assert.deepEqual(DEFECTOS.limits.rateLimit, { requests: 120, perSeconds: 60 });
    assert.equal(Buffer.from(nuevoToken().slice(4), "base64url").length, g.auth.tokenBytes);
  });
});

describe("las rutas del gateway son las de protocol.json", () => {
  test("cada ruta documentada existe en el código del servidor, y el código no tiene rutas sin documentar", () => {
    const codigo = readFileSync(new URL("../src/servidor.mjs", import.meta.url), "utf8");
    const rutas = [...codigo.matchAll(/case "(events|state|rpc)"/g)].map((m) => m[1]);
    assert.deepEqual(rutas.sort(), ["events", "rpc", "state"]);
    assert.ok(/partes\[1\] === "hooks"/.test(codigo) && /\/healthz/.test(codigo));
    const documentadas = new Set(g.routes.map((r) => (r.path.startsWith("/v1/") ? r.path.split("/")[2] : r.path.slice(1))));
    assert.deepEqual([...documentadas].sort(), ["events", "healthz", "hooks", "rpc", "state"]);
  });

  test("las cabeceras `Flux-*` del código son las documentadas", () => {
    const codigo = readFileSync(new URL("../src/servidor.mjs", import.meta.url), "utf8");
    const usadas = new Set([...codigo.matchAll(/"(flux-[a-z-]+|idempotency-key)"/g)].map((m) => m[1]));
    const documentadas = new Set(Object.values(g.headers).map((h) => h.toLowerCase()));
    assert.deepEqual([...usadas].sort(), [...documentadas].sort());
  });
});
