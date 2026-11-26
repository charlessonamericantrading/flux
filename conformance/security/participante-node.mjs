#!/usr/bin/env node
/**
 * El participante del SDK de NODE en los escenarios de petición/respuesta con SDKs reales
 * (`sdk_script` de `security/interprete.mjs`; casos S30 en adelante de `security-acl.json` y los de
 * `cases/interop-rpc.json`).
 *
 * No es un test: es un proceso que un orquestador lanza y gobierna por STDIN, con UNA línea JSON por orden, y que
 * contesta por STDOUT con UNA línea `{"id":n,…}` por orden. Habla con el broker SOLO por la API pública del SDK (`connect`,
 * `bus.request`, `bus.respond`, `bus.subscribe`, `bus.state.watch`, `bus.publish`): lo que el SDK haga por debajo
 * —sus suscripciones, sus permisos, sus tiempos— es justo lo que el escenario mide. El otro participante,
 * `sdk-rust/examples/rpc_participant.rs`, habla el MISMO protocolo con el SDK de Rust: el orquestador no sabe cuál es cuál.
 *
 * Entorno (todo lo que un servicio real recibiría de su despliegue)
 *   FLUX_P_URL        `tls://127.0.0.1:<puerto>`
 *   FLUX_P_CA         CA que firma el certificado del broker
 *   FLUX_P_SERVICE    el nombre del servicio (su identidad y su inbox privado `_INBOX.<servicio>`)
 *   FLUX_P_SEED       su semilla nkey
 *   FLUX_P_TENANT     el tenant de `connect()` (por defecto `acme`)
 *   FLUX_P_LABEL      cómo se llama esta INSTANCIA en `instancia` de las respuestas (por defecto, el servicio)
 *   FLUX_P_SIGNING    JSON opcional: `{ keyId, privateKey, publicKeys: {id: PEM}, verify: off|warn|require }`
 *   FLUX_P_TRAMPOSO   `1`: un responder cuya conexión publica DOS veces cada respuesta (ver `instalarTramposo`)
 *   FLUX_SDK_NODE_DIST  otro `dist/` del SDK (una copia estropeada a propósito: `security/mutantes-sdk.mjs`)
 *
 * Órdenes (`cmd`) y lo que contestan: ver `ordenes` más abajo y el vocabulario `sdk_script` de `security-acl.json`.
 *
 * El SDK NO se parchea aquí ni se «ayuda»: si un permiso falta o un código no sale, el participante lo cuenta tal cual.
 */

import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";

const AQUI = path.dirname(fileURLToPath(import.meta.url));
const RAIZ = path.join(AQUI, "..", "..");
const DIST = process.env.FLUX_SDK_NODE_DIST ?? path.join(RAIZ, "sdk-node", "dist");
const sdk = await import(path.join(DIST, "index.js"));

const env = (k, porDefecto) => {
  const v = process.env[k] ?? porDefecto;
  if (v === undefined || v === "") {
    console.error(`falta la variable de entorno ${k}`);
    process.exit(2);
  }
  return v;
};

const servicio = env("FLUX_P_SERVICE");
const etiqueta = env("FLUX_P_LABEL", servicio);
const firma = process.env.FLUX_P_SIGNING ? JSON.parse(process.env.FLUX_P_SIGNING) : undefined;

// ─── Lo que el SDK cuenta de sí mismo: su log ─────────────────────────────────

const logs = [];
const logger = {
  warn: (m) => logs.push(`WARN ${m}`),
  error: (m) => logs.push(`ERROR ${m}`),
};

// ─── El responder «tramposo» (A-104) ──────────────────────────────────────────

/**
 * Hace que la conexión del SDK publique DOS veces cada respuesta. Con la API pública no hay forma de contestar dos
 * veces a una petición —y esa es justo la garantía—, así que el tramposo se coloca debajo: sobre la librería de NATS
 * que el SDK usa (el MISMO módulo, el de `sdk-node/node_modules`), repite cada publicación a un inbox. Es lo que
 * haría un handler comprometido con acceso a la conexión, y lo que `allow_responses: { max: 1 }` existe para parar.
 */
async function instalarTramposo() {
  const interno = await import(path.join(RAIZ, "sdk-node", "node_modules", "@nats-io", "nats-core", "lib", "internal_mod.js"));
  const original = interno.NatsConnectionImpl.prototype.publish;
  interno.NatsConnectionImpl.prototype.publish = function (subject, data, options) {
    original.call(this, subject, data, options);
    if (typeof subject === "string" && subject.startsWith("_INBOX.") && !String(subject).startsWith("_INBOX._tramposo")) {
      logs.push(`TRAMPOSO segunda publicación en ${subject}`);
      original.call(this, subject, data, options);
    }
  };
}
if (process.env.FLUX_P_TRAMPOSO === "1") await instalarTramposo();

// ─── Conexión ─────────────────────────────────────────────────────────────────

const conexion = {
  servers: env("FLUX_P_URL"),
  service: servicio,
  environment: "test",
  version: "1.0.0",
  tenantId: env("FLUX_P_TENANT", "acme"),
  schemaBaseUrl: "https://schemas.internal",
  credentials: { nkeySeed: env("FLUX_P_SEED") },
  tls: { caFile: env("FLUX_P_CA") },
  pendingPollMs: -1,
  logger,
  ...(firma && {
    signing: {
      keyId: firma.keyId,
      privateKeyPem: firma.privateKey,
      publicKeys: firma.publicKeys,
      verify: firma.verify ?? "off",
    },
  }),
};

let bus;
let errorDeConexion;
try {
  bus = await sdk.connect(conexion);
} catch (e) {
  errorDeConexion = e;
}

// ─── Lo que se cuenta de un error ─────────────────────────────────────────────

/** Un error del SDK en el formato del protocolo del participante: lo mismo que sacaría el de Rust. */
function describirError(e, ms) {
  const clase =
    e?.fluxClass === sdk.ErrorClass.RETRYABLE ? "retryable" : e?.fluxClass === sdk.ErrorClass.PERMANENT ? "permanent" : e?.fluxClass === sdk.ErrorClass.POISON ? "poison" : "desconocida";
  return {
    name: e?.name ?? "Error",
    code: e?.code ?? e?.opts?.code ?? null,
    class: clase,
    message: String(e?.message ?? e),
    ...(typeof (e?.retryAfterMs ?? e?.opts?.retryAfterMs) === "number" && { retryAfterMs: e.retryAfterMs ?? e.opts.retryAfterMs }),
    ...(ms !== undefined && { ms }),
  };
}

const ahora = () => Number(process.hrtime.bigint()) / 1e6;

// ─── Los comportamientos del responder ────────────────────────────────────────

const comportamientos = new Map(); // operación → comportamiento actual
const atendidas = new Map(); // operación → nº de peticiones que llegaron al handler
const idsVistos = new Map(); // operación → ids de petición recibidos (para ver duplicados)

/** El handler de una operación: hace lo que el comportamiento VIGENTE diga (el escenario lo cambia con `behavior`). */
function manejador(operacion) {
  return async (peticion, ctx) => {
    atendidas.set(operacion, (atendidas.get(operacion) ?? 0) + 1);
    (idsVistos.get(operacion) ?? idsVistos.set(operacion, []).get(operacion)).push(ctx.id);
    const b = comportamientos.get(operacion) ?? { kind: "echo" };
    if (b.afterMs) await new Promise((r) => setTimeout(r, b.afterMs));
    switch (b.kind) {
      case "echo":
        return {
          instancia: etiqueta,
          tenant: ctx.tenantId,
          correlationId: ctx.correlationId,
          correlacionEsElId: ctx.correlationId === ctx.id,
          peticionId: ctx.id,
          ...(ctx.aggregateId !== undefined && { aggregateId: ctx.aggregateId }),
          eco: peticion,
        };
      case "error":
        if (b.class === "retryable") {
          throw new sdk.RetryableError(b.message ?? "el servicio de tarifas no responde", { code: b.code, retryAfterMs: b.retryAfterMs });
        }
        throw new sdk.PermanentError(b.message ?? "pedido ya cancelado", { code: b.code });
      case "unknown":
        // Una excepción que NO lanzó a propósito la aplicación: su texto puede llevar valores de `data`, rutas…
        throw new Error(b.text ?? "SECRETO-DE-LA-EXCEPCION");
      case "http":
        // Un 503 (o el status que sea) de una dependencia: lo que el clasificador RECONOCE, con el texto de la excepción por medio.
        throw Object.assign(new Error(b.text ?? "SECRETO-DEL-PROVEEDOR"), { status: b.status ?? 503 });
      case "econnreset":
        throw Object.assign(new Error(b.text ?? "SECRETO-DEL-PROVEEDOR"), { code: "ECONNRESET" });
      case "big":
        return { instancia: etiqueta, relleno: "x".repeat(b.bytes ?? 1_100_000) };
      case "no-object":
        return "esto no es un objeto";
      default:
        throw new Error(`comportamiento desconocido «${b.kind}»`);
    }
  };
}

// ─── Las órdenes ──────────────────────────────────────────────────────────────

const responders = new Map(); // operación → Responder
const consumidos = [];
const observados = [];
let suscripcion;

const opcionesDeLlamada = (o = {}) => ({
  ...(o.timeoutMs !== undefined && { timeoutMs: o.timeoutMs }),
  ...(o.tenantId !== undefined && { tenantId: o.tenantId }),
  ...(o.aggregateId !== undefined && { aggregateId: o.aggregateId }),
  ...(o.classification !== undefined && { classification: o.classification }),
  ...(o.retries !== undefined && { retries: o.retries }),
});

async function llamar(operacion, data, opciones) {
  const t0 = ahora();
  try {
    const r = await bus.request(operacion, data ?? {}, opcionesDeLlamada(opciones));
    return { ok: true, data: r, ms: ahora() - t0 };
  } catch (e) {
    return { ok: false, error: describirError(e, ahora() - t0), ms: ahora() - t0 };
  }
}

const mediana = (xs) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];

const ordenes = {
  /** Atiende una operación con un comportamiento. Falla con el error del SDK (p. ej. PERMISSION_DENIED por falta de permiso). */
  async respond(o) {
    comportamientos.set(o.operation, o.behavior ?? { kind: "echo" });
    try {
      const r = await bus.respond(o.operation, manejador(o.operation));
      responders.set(o.operation, r);
      return { ok: true, subject: r.subject };
    } catch (e) {
      return { ok: false, error: describirError(e) };
    }
  },

  /** Cambia el comportamiento de una operación YA atendida. */
  behavior(o) {
    comportamientos.set(o.operation, o.behavior);
    return { ok: true };
  },

  /** Parada ordenada de UNA operación (`Responder.stop()`). */
  async stop(o) {
    const r = responders.get(o.operation);
    if (!r) return { ok: false, error: { name: "Error", message: `no se atiende ${o.operation}` } };
    const t0 = ahora();
    await r.stop();
    responders.delete(o.operation);
    return { ok: true, ms: ahora() - t0 };
  },

  /** UNA llamada. */
  async call(o) {
    return llamar(o.operation, o.data, o.options);
  },

  /** `repeat` llamadas seguidas: la mediana y el máximo de los tiempos y los códigos que salieron (una sola medición engaña). */
  async call_repeat(o) {
    const tiempos = [];
    const codigos = {};
    let ultimo;
    for (let i = 0; i < (o.repeat ?? 5); i++) {
      const r = await llamar(o.operation, o.data, o.options);
      tiempos.push(r.ms);
      const codigo = r.ok ? "OK" : (r.error.code ?? r.error.name);
      codigos[codigo] = (codigos[codigo] ?? 0) + 1;
      ultimo = r;
    }
    return { ok: true, calls: tiempos.length, p50Ms: mediana(tiempos), maxMs: Math.max(...tiempos), minMs: Math.min(...tiempos), codes: codigos, last: ultimo };
  },

  /** `n` llamadas con `concurrency` a la vez: cuántas se contestaron, quién las atendió y qué falló. */
  async call_many(o) {
    const n = o.n ?? 100;
    const porInstancia = {};
    const errores = {};
    let contestadas = 0;
    let siguiente = 0;
    const trabajador = async () => {
      while (siguiente < n) {
        const i = siguiente++;
        const r = await llamar(o.operation, { ...(o.data ?? {}), n: i }, o.options);
        if (r.ok) {
          contestadas++;
          const quien = r.data?.instancia ?? "?";
          porInstancia[quien] = (porInstancia[quien] ?? 0) + 1;
        } else {
          const c = r.error.code ?? r.error.name;
          errores[c] = (errores[c] ?? 0) + 1;
        }
      }
    };
    await Promise.all(Array.from({ length: o.concurrency ?? 20 }, trabajador));
    const reparto = Object.values(porInstancia);
    return {
      ok: true,
      answered: contestadas,
      failed: n - contestadas,
      byInstance: porInstancia,
      minShare: reparto.length ? Math.min(...reparto) : 0,
      errors: errores,
    };
  },

  /** Consume un subject de eventos por pull (`bus.subscribe`): el servicio que consume y llama por la MISMA conexión. */
  async consume(o) {
    try {
      suscripcion = await bus.subscribe(o.subject, async (evento) => {
        consumidos.push(evento.id);
      });
      return { ok: true, durable: suscripcion.durable };
    } catch (e) {
      return { ok: false, error: describirError(e) };
    }
  },

  /** `bus.state.watch` de un contrato de estado. */
  async watch(o) {
    try {
      let sincronizado = false;
      await bus.state.watch(
        o.contract,
        async (entrada) => {
          observados.push(`${entrada.tenantId}/${entrada.key}`);
        },
        { ...(o.tenantId !== undefined && { tenantId: o.tenantId }), onSynced: () => (sincronizado = true) },
      );
      return { ok: true, synced: () => sincronizado };
    } catch (e) {
      return { ok: false, error: describirError(e) };
    }
  },

  /** Publica un EVENTO (para que el que consume tenga algo que recibir). */
  async publish(o) {
    try {
      const ids = [];
      for (let i = 0; i < (o.n ?? 1); i++) {
        const aggregateId = o.options?.aggregateId ?? (o.n > 1 ? `ped-${i}` : undefined);
        const e = await bus.publish(o.subject, { ...o.data, ...(o.n > 1 && { pedidoId: `ped-${i}` }) }, aggregateId ? { aggregateId } : {});
        ids.push(e.id);
      }
      return { ok: true, eventId: ids[0], published: ids.length };
    } catch (e) {
      return { ok: false, error: describirError(e) };
    }
  },

  /** Escribe una entrada de ESTADO (para que el `watch` tenga algo que observar). */
  async state_put(o) {
    try {
      const r = await bus.state.put(o.contract, o.key, o.data, o.options ?? {});
      return { ok: true, outcome: r.outcome, revision: r.revision };
    } catch (e) {
      return { ok: false, error: describirError(e) };
    }
  },

  /** Lo que el participante vio: peticiones por operación, eventos consumidos, entradas observadas y su log. */
  stats() {
    return {
      ok: true,
      handled: Object.fromEntries(atendidas),
      total: [...atendidas.values()].reduce((a, b) => a + b, 0),
      requestIds: Object.fromEntries(idsVistos),
      uniqueRequestIds: new Set([...idsVistos.values()].flat()).size,
      consumed: consumidos.length,
      watched: observados.length,
      logs,
      logCount: logs.length,
    };
  },

  async close() {
    const t0 = ahora();
    await bus?.close().catch(() => {});
    return { ok: true, ms: ahora() - t0 };
  },
};

// ─── El bucle de órdenes ──────────────────────────────────────────────────────

const salir = (o) => process.stdout.write(`${JSON.stringify(o)}\n`);
salir({ id: 0, ready: true, sdk: "node", service: servicio, label: etiqueta, ...(errorDeConexion && { ok: false, error: describirError(errorDeConexion) }) });
if (errorDeConexion) process.exit(0);

const lector = readline.createInterface({ input: process.stdin });
let pendientes = Promise.resolve();
lector.on("line", (linea) => {
  // Las órdenes se atienden EN PARALELO (un `call_many` no bloquea un `stats`), salvo `close`, que espera a las demás.
  let o;
  try {
    o = JSON.parse(linea);
  } catch {
    return salir({ id: -1, ok: false, error: { name: "Error", message: `línea que no es JSON: ${linea.slice(0, 80)}` } });
  }
  const ejecutar = async () => {
    if (process.env.FLUX_P_DEBUG) console.error(`[${etiqueta}] → ${linea.slice(0, 200)}`);
    const f = ordenes[o.cmd];
    if (!f) return salir({ id: o.id, ok: false, error: { name: "Error", message: `orden desconocida «${o.cmd}»` } });
    try {
      const r = await f(o);
      const limpio = JSON.parse(JSON.stringify(r, (k, v) => (typeof v === "function" ? v() : v)));
      salir({ ...limpio, id: o.id });
    } catch (e) {
      salir({ id: o.id, ok: false, error: describirError(e) });
    }
  };
  if (o.cmd === "close") {
    pendientes = pendientes.then(async () => {
      await ejecutar();
      process.exit(0);
    });
  } else {
    pendientes = pendientes.then(() => undefined);
    void ejecutar();
  }
});
lector.on("close", async () => {
  await bus?.close().catch(() => {});
  process.exit(0);
});
