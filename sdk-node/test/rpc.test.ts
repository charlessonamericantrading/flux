/**
 * Petición/respuesta — pruebas UNITARIAS (sin broker). specification/11-request-reply.md.
 *
 * Aquí se fija lo que no necesita un nats-server: los bytes exactos de la petición, la respuesta y los dos errores
 * (los de 11-request-reply.md §3.5 y el caso cruzado), las reglas de validación que corren ANTES de la red, los seis
 * códigos de 04-errors.md §7, las cinco comprobaciones que quien llama hace de la respuesta (§4.3), la tabla de errores
 * del handler (§5.3), el algoritmo de quien responde (§5.1), la ventana de `STALE_REQUEST`, el límite de handlers
 * concurrentes, la parada ordenada y las métricas, contra una red EN MEMORIA que hace solo lo que se midió
 * (`rpc-fakes.ts`).
 *
 * Lo que sí necesita broker está en `integration/rpc.test.ts` (los permisos exactos, el reparto entre dos instancias,
 * `RPC_UNAVAILABLE` en menos de 100 ms y «quita un permiso y se nota»). Toda comprobación va en los DOS sentidos: lo
 * correcto pasa y lo incorrecto falla.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import net from "node:net";
import { readFile } from "node:fs/promises";

import { connect as natsConnect, errors } from "@nats-io/transport-node";

import {
  ErrorClass,
  FluxBus,
  InMemoryMetrics,
  InvalidDataClassificationError,
  InvalidRpcCallError,
  PermanentError,
  PermissionDeniedError,
  PoisonError,
  ReservedDomainError,
  RetryableError,
  RpcInvalidReplyError,
  RpcMessageTooLargeError,
  RpcPermanentError,
  RpcRetryableError,
  RpcSchemaError,
  RpcTimeoutError,
  RpcUnavailableError,
  SchemaNotFoundError,
  StaleRequestError,
  buildRpcError,
  buildRpcReply,
  buildRpcRequest,
  classify,
  createSigner,
  encodeRpcMessage,
  generateKeyPair,
  parseEvent,
  parseRpcOperation,
  resolveRpcTimeout,
  rpcDataschema,
  rpcErrorDataProblem,
  rpcReplyType,
  rpcRequestType,
  rpcSubject,
  type ConnectOptions,
  type FluxEvent,
  type MetricsSink,
  type Responder,
  type RpcErrorData,
} from "../src/index.js";
import { contextFromEvent, runWithContext } from "../src/context.js";
import { esErrorDeUnaPeticionIlegible } from "../src/rpc.js";
import { normalizeDeniedSubject } from "../src/security.js";
import { ConexionFalsa, RedEnMemoria, sleep, type MensajeFalso } from "./rpc-fakes.js";

// ─── Utilidades ──────────────────────────────────────────────────────────────

/**
 * La raíz se busca por un marcador, no contando `../`: el fuente vive en `test/` y el compilado en
 * `dist-test/test/`, y cualquier ruta relativa fija es correcta en uno de los dos y silenciosamente incorrecta en el otro.
 */
async function repoRoot(): Promise<URL> {
  let dir = new URL("./", import.meta.url);
  for (let i = 0; i < 6; i++) {
    try {
      await readFile(new URL("protocol.json", dir), "utf8");
      return dir;
    } catch {
      dir = new URL("../", dir);
    }
  }
  throw new Error("no se encontró la raíz del repo (protocol.json)");
}
const RAIZ = await repoRoot();
const leer = (ruta: string) => readFile(new URL(ruta, RAIZ), "utf8");

const protocolo = JSON.parse(await leer("protocol.json"));
const rpc = protocolo.rpc;
const fixtures = JSON.parse(await leer("conformance/cases/cross-sdk-rpc-envelope.json"));
const bundle = JSON.parse(await leer("schemas/bundle.json"));
const especificacion = await leer("specification/11-request-reply.md");

const sha256 = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");
const texto = (b: Uint8Array) => new TextDecoder().decode(b);
const bytesDe = (s: string) => new TextEncoder().encode(s);
const fixture = (id: string) => fixtures.fixtures.find((f: { id: string }) => f.id === id);

const OP = "clientes.cliente.v1.calcular-descuento";
const SUBJECT = `rpc.${OP}`;
const PETICION = { clienteId: "cli-1", totalCents: 9990, moneda: "EUR" };
const UUID_V7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const ISO_3_DECIMALES = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

// ═════════════════════════════════════════════════════════════════════════════
// 1. Los bytes exactos
// ═════════════════════════════════════════════════════════════════════════════

describe("bytes exactos — conformance/cases/cross-sdk-rpc-envelope.json y 11-request-reply.md §3.5", () => {
  const construir: Record<string, () => Uint8Array> = {
    "rpc-request": () => {
      const i = fixture("rpc-request").input;
      return encodeRpcMessage(
        buildRpcRequest({
          operation: i.operation,
          tenantId: i.tenantId,
          aggregateId: i.aggregateId,
          id: i.id,
          source: i.source,
          time: i.time,
          correlationid: i.correlationid,
          producerversion: i.producerversion,
          classification: i.classification,
          traceparent: i.traceparent,
          data: i.data,
        }),
      );
    },
    "rpc-reply": () => {
      const i = fixture("rpc-reply").input;
      return encodeRpcMessage(
        buildRpcReply({
          request: i.request,
          id: i.id,
          source: i.source,
          time: i.time,
          producerversion: i.producerversion,
          classification: i.classification,
          traceparent: i.traceparent,
          data: i.data,
        }),
      );
    },
    "rpc-error-handler": () => construirError("rpc-error-handler"),
    "rpc-error-sdk": () => construirError("rpc-error-sdk"),
  };
  function construirError(id: string): Uint8Array {
    const i = fixture(id).input;
    return encodeRpcMessage(
      buildRpcError({
        request: i.request,
        id: i.id,
        source: i.source,
        time: i.time,
        producerversion: i.producerversion,
        traceparent: i.traceparent,
        error: i.error,
      }),
    );
  }

  for (const [id, fn] of Object.entries(construir)) {
    test(`${id}: los bytes, la longitud y el SHA-256 son los del fixture`, () => {
      const f = fixture(id);
      const bytes = fn();
      assert.equal(Buffer.from(bytes).toString("base64"), f.base64, "los bytes difieren");
      assert.equal(bytes.byteLength, f.bytes);
      assert.equal(sha256(bytes), f.sha256);
      // y es EXACTAMENTE lo que serializa el fixture: el orden de claves es normativo
      assert.equal(texto(bytes), JSON.stringify(f.event));
    });
  }

  test("las cuatro líneas de 11-request-reply.md §3.5 (el texto de la spec) son las que se construyen", () => {
    // El fixture y la spec dicen lo mismo; aquí se lee la SPEC, no el JSON de datos.
    const lineas = especificacion.split("\n").filter((l) => l.startsWith('{"specversion":"1.0"'));
    assert.equal(lineas.length, 4, "11-request-reply.md §3.5 debe tener cuatro ejemplos byte a byte");
    assert.equal(lineas[0], texto(construir["rpc-request"]()));
    assert.equal(lineas[1], texto(construir["rpc-reply"]()));
    assert.equal(lineas[2], texto(construir["rpc-error-handler"]()));
    assert.equal(lineas[3], texto(construir["rpc-error-sdk"]()));
  });

  test("los SHA-256 de la spec: 43ea71ad…, b164fa71…, fdff1249…, c6f80891…", () => {
    assert.equal(sha256(construir["rpc-request"]()), "43ea71ade9fb6ff81ccb7c1f93047ffebd604d602492229e452b1c97dadf83f5");
    assert.equal(sha256(construir["rpc-reply"]()), "b164fa71fa3e2943f3cb160f1a7cf3c520b09c2d93a0a1da81eef7f17da1f01c");
    assert.equal(sha256(construir["rpc-error-handler"]()), "fdff124948254dfa3263971aec86bf4d0184db77babe44da5e0714f28fc427f8");
    assert.equal(sha256(construir["rpc-error-sdk"]()), "c6f80891c94a6c64defa825288f9f98201bed5824d7c6e94c58362dccbfc446d");
  });

  test("CONTROL: cambiar el `tenantid`, o poner `data` antes de `partitionkey`, cambia el hash (la comprobación sabe fallar)", () => {
    const f = fixture("rpc-request");
    const i = f.input;
    const otro = encodeRpcMessage(
      buildRpcRequest({ ...i, tenantId: "globex", aggregateId: i.aggregateId, data: i.data }),
    );
    assert.notEqual(sha256(otro), f.sha256);
    const { data, partitionkey, ...resto } = f.event;
    const desordenado = bytesDe(JSON.stringify({ ...resto, data, partitionkey }));
    assert.notEqual(sha256(desordenado), f.sha256, "el orden de las claves ES el contrato");
    // y un `ñ` en lugar de `ñ` son otros bytes aunque sean el mismo JSON
    const escapado = bytesDe(texto(construir["rpc-request"]()).replace("Otoño", "Oto\\u00f1o"));
    assert.notEqual(sha256(escapado), f.sha256);
  });

  test("UTF-8 literal: ningún escape \\u en ninguno de los cuatro; `data` es siempre el último atributo", () => {
    for (const id of Object.keys(construir)) {
      const bytes = construir[id]();
      assert.ok(!/\\u[0-9a-fA-F]{4}/.test(texto(bytes)), `${id} lleva un escape \\u`);
      const keys = Object.keys(JSON.parse(texto(bytes)));
      assert.equal(keys.at(-1), "data", `${id}: data no es el último`);
    }
    assert.match(texto(construir["rpc-request"]()), /"campana":"Otoño"/);
    assert.match(texto(construir["rpc-reply"]()), /"motivo":"Campaña de otoño"/);
  });

  test("las aserciones del caso: la respuesta hereda correlationid, tenantid y subject; su causationid es el id de la petición", () => {
    const req = JSON.parse(texto(construir["rpc-request"]()));
    const rep = JSON.parse(texto(construir["rpc-reply"]()));
    assert.equal(req.correlationid, req.id, "una petición que no nace de otra se correlaciona consigo misma");
    assert.ok(!("causationid" in req));
    assert.equal(rep.causationid, req.id);
    assert.equal(rep.correlationid, req.correlationid);
    assert.equal(rep.tenantid, req.tenantid);
    assert.equal(rep.subject, req.subject);
    assert.equal(rep.partitionkey, req.partitionkey);
    assert.notEqual(rep.id, req.id, "la respuesta lleva un id NUEVO");
    for (const id of ["rpc-error-handler", "rpc-error-sdk"]) {
      const e = JSON.parse(texto(construir[id]()));
      assert.equal(e.type, "com.flux.rpc.error.v1");
      assert.equal(e.dataclassification, "internal", "el error es siempre internal");
      assert.deepEqual(Object.keys(e.data).slice(0, 3), ["class", "code", "message"]);
    }
  });

  test("el error es `internal` aunque quien lo construye pida otra cosa: la clasificación no se elige", () => {
    const i = fixture("rpc-error-handler").input;
    const e = buildRpcError({ ...i, classification: "restricted", error: i.error } as never);
    assert.equal(e.dataclassification, "internal");
  });

  test("la respuesta no hereda a mano: sin `subject` en la petición no lleva `subject` ni `partitionkey`", () => {
    const i = fixture("rpc-reply").input;
    const { subject: _s, ...sinSubject } = i.request;
    const r = buildRpcReply({ ...i, request: sinSubject });
    assert.ok(!("subject" in r));
    assert.ok(!("partitionkey" in r));
    // y con un `partitionkey` propio en la petición, ese gana sobre el `subject`
    const r2 = buildRpcReply({ ...i, request: { ...i.request, partitionkey: "p-9" } });
    assert.equal(r2.partitionkey, "p-9");
  });

  test("`retryAfterMs` solo va con `retryable`, y `poison` no viaja: el constructor del error los rechaza", () => {
    const i = fixture("rpc-error-handler").input;
    const e = (error: Record<string, unknown>) => () => buildRpcError({ ...i, error } as never);
    // Un código estable (INVALID_RPC_CALL, 04-errors.md §7), no un `TypeError` sin código: el arnés cruzado lo compara entre SDKs.
    const invalida = (x: unknown) => x instanceof InvalidRpcCallError && x.code === "INVALID_RPC_CALL" && x.fluxClass === ErrorClass.PERMANENT;
    assert.throws(e({ class: "poison", code: "X", message: "m" }), invalida);
    assert.throws(e({ class: "permanent", code: "X", message: "m", retryAfterMs: 5 }), invalida);
    assert.throws(e({ class: "retryable", code: "x_minuscula", message: "m" }), invalida);
    assert.throws(e({ class: "retryable", code: "X", message: "" }), invalida);
    assert.doesNotThrow(e({ class: "retryable", code: "X", message: "m", retryAfterMs: 0 }));
  });
});

describe("la operación y el timeout — los vectores del caso cruzado", () => {
  for (const v of fixtures.subjectVectors as Array<{ operation: string; natsSubject?: string; errorCode?: string; note?: string }>) {
    if (v.natsSubject !== undefined) {
      test(`${JSON.stringify(v.operation)} → ${v.natsSubject}`, () => {
        assert.equal(rpcSubject(v.operation), v.natsSubject);
      });
    } else {
      test(`${JSON.stringify(v.operation)} → ${v.errorCode} (${v.note})`, () => {
        assert.throws(
          () => parseRpcOperation(v.operation),
          (e: { code?: string }) => e.code === v.errorCode,
        );
      });
    }
  }

  for (const v of fixtures.timeoutVectors as Array<{ timeoutMs?: unknown; effectiveMs?: number; errorCode?: string }>) {
    const dado = "timeoutMs" in v ? JSON.stringify(v.timeoutMs) : "(ausente)";
    if (v.effectiveMs !== undefined) {
      test(`timeoutMs ${dado} → ${v.effectiveMs} ms`, () => {
        assert.equal(resolveRpcTimeout(v.timeoutMs), v.effectiveMs);
      });
    } else {
      test(`timeoutMs ${dado} → ${v.errorCode} (no se recorta)`, () => {
        assert.throws(
          () => resolveRpcTimeout(v.timeoutMs),
          (e: { code?: string }) => e.code === v.errorCode,
        );
      });
    }
  }

  test("las constantes son las de protocol.json → rpc", () => {
    assert.equal(resolveRpcTimeout(undefined), rpc.timeout.defaultMs);
    assert.equal(resolveRpcTimeout(rpc.timeout.minMs), rpc.timeout.minMs);
    assert.equal(resolveRpcTimeout(rpc.timeout.maxMs), rpc.timeout.maxMs);
    assert.throws(() => resolveRpcTimeout(rpc.timeout.maxMs + 1));
    assert.ok(rpc.timeout.maxMs < rpc.timeout.ackWaitMs, "el máximo está por debajo del ack_wait");
  });

  test("los vectores de operación del propio protocol.json (rpc.operation.valid / invalid) coinciden con el SDK", () => {
    for (const ok of rpc.operation.valid as string[]) assert.doesNotThrow(() => parseRpcOperation(ok), ok);
    for (const mal of rpc.operation.invalid as string[]) {
      assert.throws(() => parseRpcOperation(mal), (e: { code?: string }) => e.code === "INVALID_RPC_CALL" || e.code === "RESERVED_DOMAIN", mal);
    }
    for (const ok of rpc.subject.valid as string[]) assert.equal(rpcSubject(ok.replace(/^rpc\./, "")), ok);
  });

  test("el `type` y el `dataschema` salen de la operación (rpc.type, rpc.dataschema)", () => {
    assert.match(rpcRequestType(OP), new RegExp(rpc.type.request.pattern));
    assert.match(rpcReplyType(OP), new RegExp(rpc.type.reply.pattern));
    assert.equal(rpcRequestType(OP), rpc.type.request.example);
    assert.equal(rpcReplyType(OP), rpc.type.reply.example);
    assert.match(rpcDataschema(OP, "request"), new RegExp(rpc.dataschema.request.pattern));
    assert.match(rpcDataschema(OP, "reply"), new RegExp(rpc.dataschema.reply.pattern));
    assert.equal(rpcDataschema("logistica.linea-envio.v2.reservar-stock", "reply"), "https://schemas.internal/rpc/logistica/linea-envio/reservar-stock/reply/2.0.0.json");
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 2. Los seis códigos
// ═════════════════════════════════════════════════════════════════════════════

describe("los seis códigos de 04-errors.md §7, con su clase", () => {
  const casos: Array<{ code: string; clase: ErrorClass; crea: () => Error }> = [
    { code: "RPC_UNAVAILABLE", clase: ErrorClass.RETRYABLE, crea: () => new RpcUnavailableError(OP) },
    { code: "RPC_TIMEOUT", clase: ErrorClass.RETRYABLE, crea: () => new RpcTimeoutError(OP, 2000) },
    { code: "STALE_REQUEST", clase: ErrorClass.PERMANENT, crea: () => new StaleRequestError("2026-08-20T10:00:00.000Z", 300, "2026-08-20T10:10:00.000Z") },
    { code: "RPC_INVALID_REPLY", clase: ErrorClass.PERMANENT, crea: () => new RpcInvalidReplyError(OP, "su `causationid` no es el id") },
    { code: "RPC_MESSAGE_TOO_LARGE", clase: ErrorClass.PERMANENT, crea: () => new RpcMessageTooLargeError(2_000_000, 1_048_576, "petición", OP) },
    { code: "INVALID_RPC_CALL", clase: ErrorClass.PERMANENT, crea: () => new InvalidRpcCallError("`timeoutMs` fuera de rango") },
  ];
  const catalogo = protocolo.errors.codes;

  for (const c of casos) {
    test(`${c.code}: clase ${c.clase}, y coincide con protocol.json → errors.codes`, () => {
      const e = c.crea() as Error & { code: string; fluxClass: ErrorClass; opts: { code: string } };
      assert.equal(e.code, c.code);
      assert.equal(e.opts.code, c.code, "el clasificador lee el código de opts");
      assert.equal(e.fluxClass, c.clase);
      assert.equal(catalogo[c.code].class, c.clase);
      assert.equal(catalogo[c.code].capability, "rpc");
      // la clase RETRYABLE / PERMANENT la lleva el tipo, así que un handler de eventos que la deje escapar se clasifica solo
      assert.equal(classify(e).class, c.clase);
      assert.equal(classify(e).code, c.code);
    });
  }

  test("son exactamente los seis de protocol.json (la lista es cerrada)", () => {
    const delProtocolo = Object.entries(catalogo)
      .filter(([k, v]) => !k.startsWith("$") && (v as { capability: string }).capability === "rpc")
      .map(([k]) => k)
      .sort();
    assert.deepEqual(delProtocolo, casos.map((c) => c.code).sort());
  });

  test("RPC_UNAVAILABLE es retryable e inmediato; RPC_TIMEOUT dice que la operación PUEDE haberse ejecutado", () => {
    assert.ok(new RpcUnavailableError(OP) instanceof RetryableError);
    assert.match(new RpcTimeoutError(OP, 2000).message, /PUEDE haberse ejecutado/);
    assert.match(new RpcTimeoutError(OP, 2000).message, /2000 ms/);
    assert.match(new RpcTimeoutError(OP, 2000).message, /mismo id|MISMO id/);
  });

  test("los mensajes nombran QUÉ está mal y CÓMO se arregla (04-errors.md §7)", () => {
    assert.match(new RpcMessageTooLargeError(2_000_000, 1_048_576, "respuesta", OP).message, /2000000 bytes/);
    assert.match(new RpcMessageTooLargeError(2_000_000, 1_048_576, "respuesta", OP).message, /1048576/);
    assert.match(new RpcMessageTooLargeError(2_000_000, 1_048_576, "respuesta", OP).message, /claim-check/);
    assert.match(new StaleRequestError("2026-08-20T10:00:00.000Z", 300, "2026-08-20T10:10:00.000Z").message, /2026-08-20T10:00:00\.000Z/);
    assert.match(new StaleRequestError("2026-08-20T10:00:00.000Z", 300, "2026-08-20T10:10:00.000Z").message, /300 s/);
    assert.match(new StaleRequestError("2026-08-20T10:00:00.000Z", 300, "2026-08-20T10:10:00.000Z").message, /2026-08-20T10:10:00\.000Z/);
    assert.match(new RpcUnavailableError(OP).message, new RegExp(OP));
  });

  test("RPC_INVALID_REPLY apunta a `flux doctor` cuando el cuerpo parece el acuse de un stream (y solo entonces)", () => {
    assert.match(new RpcInvalidReplyError(OP, "no es un CloudEvent", { pistaStream: true }).message, /flux doctor/);
    assert.match(new RpcInvalidReplyError(OP, "no es un CloudEvent", { pistaStream: true }).message, /rpc\.>/);
    assert.ok(!/flux doctor/.test(new RpcInvalidReplyError(OP, "causationid", { pistaStream: false }).message));
  });

  test("ningún mensaje cita un valor de `data`", () => {
    const secreto = "ana@example.com";
    for (const m of [
      new InvalidRpcCallError("data debe ser un objeto").message,
      new RpcInvalidReplyError(OP, "no es un CloudEvent").message,
      new RpcTimeoutError(OP, 1).message,
    ]) {
      assert.ok(!m.includes(secreto));
    }
  });

  test("el error de quien responde se convierte en RetryableError / PermanentError con su `code` y `retryAfterMs`", () => {
    const r = new RpcRetryableError(OP, "HTTP_503", "el servicio de tarifas no responde", 5000);
    assert.ok(r instanceof RetryableError);
    assert.equal(r.code, "HTTP_503");
    assert.equal(classify(r).retryAfterMs, 5000);
    const p = new RpcPermanentError(OP, "PEDIDO_YA_CANCELADO", "el pedido ya está cancelado");
    assert.ok(p instanceof PermanentError);
    assert.equal(classify(p).code, "PEDIDO_YA_CANCELADO");
    assert.equal(classify(p).class, ErrorClass.PERMANENT);
  });
});

describe("el esquema del error (la quinta comprobación) coincide con schemas/system/rpc/error/1.0.0.json", () => {
  const ok = { class: "retryable", code: "HTTP_503", message: "m", retryAfterMs: 5000 };
  test("lo válido pasa", () => {
    assert.equal(rpcErrorDataProblem(ok), null);
    assert.equal(rpcErrorDataProblem({ class: "permanent", code: "X", message: "m" }), null);
    assert.equal(rpcErrorDataProblem({ class: "retryable", code: "A_B_C1", message: "m", retryAfterMs: 0 }), null);
  });
  for (const [nombre, d] of Object.entries({
    "class poison": { ...ok, class: "poison" },
    "sin class": { code: "X", message: "m" },
    "código en minúsculas": { ...ok, code: "http_503" },
    "código con guion": { ...ok, code: "HTTP-503" },
    "código vacío": { ...ok, code: "" },
    "código de 65 caracteres": { ...ok, code: `A${"B".repeat(64)}` },
    "mensaje vacío": { ...ok, message: "" },
    "mensaje de 1001 caracteres": { ...ok, message: "x".repeat(1001) },
    "mensaje que no es texto": { ...ok, message: 42 },
    "retryAfterMs decimal": { ...ok, retryAfterMs: 1.5 },
    "retryAfterMs negativo": { ...ok, retryAfterMs: -1 },
    "retryAfterMs con permanent": { class: "permanent", code: "X", message: "m", retryAfterMs: 5 },
    "atributo de más": { ...ok, extra: 1 },
    "un array": [],
    "null": null,
  })) {
    test(`rechaza: ${nombre}`, () => assert.notEqual(rpcErrorDataProblem(d), null));
  }

  test("CONTROL: el esquema JSON de verdad, con ajv, dice lo mismo en cada caso", async () => {
    const { default: Ajv } = await import("ajv/dist/2020.js" as string);
    const validar = new Ajv({ allErrors: true, strict: false }).compile(JSON.parse(await leer("schemas/system/rpc/error/1.0.0.json")));
    for (const d of [ok, { class: "permanent", code: "X", message: "m" }, { ...ok, class: "poison" }, { ...ok, code: "http_503" }, { ...ok, message: "" },
      { class: "permanent", code: "X", message: "m", retryAfterMs: 5 }, { ...ok, extra: 1 }, { ...ok, retryAfterMs: -1 }, { ...ok, retryAfterMs: 1.5 }]) {
      assert.equal(rpcErrorDataProblem(d) === null, validar(d) === true, JSON.stringify(d));
    }
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 3. Un bus con una red EN MEMORIA
// ═════════════════════════════════════════════════════════════════════════════

const REDTOCADA = "se tocó la red";
const prohibido = (): never => {
  throw new Error(REDTOCADA);
};
const explosivo = (base: Record<string, unknown> = {}): never =>
  new Proxy(base, {
    get: (t, p) => (p in t ? (t as Record<string | symbol, unknown>)[p] : p === "then" ? undefined : prohibido),
  }) as never;

const OPCIONES: ConnectOptions = {
  servers: "nats://falso:4222",
  service: "facturacion-api",
  environment: "produccion",
  version: "3.4.1",
  tenantId: "acme",
  schemaBaseUrl: "https://schemas.internal",
};

interface Servicio {
  bus: FluxBus;
  nc: ConexionFalsa;
  logs: string[];
  metricas: InMemoryMetrics;
  poison: Array<{ subject: string; error: Error }>;
}

/** Un servicio sobre una red. JetStream EXPLOTA al tocarlo: la petición/respuesta es core NATS y no lo usa. */
async function servicio(red: RedEnMemoria, extra: Partial<ConnectOptions> = {}): Promise<Servicio> {
  const nc = red.conectar();
  const logs: string[] = [];
  const poison: Array<{ subject: string; error: Error }> = [];
  const metricas = new InMemoryMetrics();
  const bus = new FluxBus(nc as never, explosivo(), explosivo(), {
    ...OPCIONES,
    metrics: metricas,
    logger: { warn: (m: string) => void logs.push(m), error: (m: string) => void logs.push(m) },
    onPoison: ({ subject, error }) => void poison.push({ subject, error }),
    ...extra,
  });
  await bus.initValidation();
  return { bus, nc, logs, metricas, poison };
}

const CRM: Partial<ConnectOptions> = { service: "crm-api", version: "2.1.0" };

/** Rechaza con ESE código y sin haber tocado la red. */
async function rechazaCon(codigo: string, p: Promise<unknown>): Promise<Error & { code?: string }> {
  let capturado: (Error & { code?: string }) | undefined;
  await assert.rejects(p, (e: Error & { code?: string }) => {
    assert.notEqual(e.message, REDTOCADA, `${codigo}: se llegó a tocar la red`);
    assert.equal(e.code, codigo, `esperaba ${codigo}: ${e.message}`);
    capturado = e;
    return true;
  });
  return capturado!;
}

const evento = (bytes: Uint8Array): FluxEvent & Record<string, unknown> => JSON.parse(texto(bytes));

async function sinRed(extra: Partial<ConnectOptions> = {}): Promise<FluxBus> {
  const nc = {
    status: () => ({ [Symbol.asyncIterator]: () => ({ next: () => new Promise<never>(() => {}) }) }),
    request: prohibido,
    publish: prohibido,
    subscribe: prohibido,
    flush: prohibido,
    isClosed: () => false,
  };
  const bus = new FluxBus(nc as never, explosivo(), explosivo(), { ...OPCIONES, ...extra });
  await bus.initValidation();
  return bus;
}

describe("dominios reservados y llamadas mal formadas — request() y respond() los rechazan ANTES de la red", () => {
  for (const dominio of protocolo.naming.reservedDomains.names as string[]) {
    test(`request("${dominio}.…") y respond("${dominio}.…") → RESERVED_DOMAIN`, async () => {
      const bus = await sinRed();
      await rechazaCon("RESERVED_DOMAIN", bus.request(`${dominio}.cliente.v1.obtener`, {}));
      await rechazaCon("RESERVED_DOMAIN", bus.respond(`${dominio}.cliente.v1.obtener`, () => ({})));
    });
  }

  test("con el prefijo `rpc.` puesto por quien llama: RESERVED_DOMAIN, y el mensaje dice que el prefijo lo pone el SDK", async () => {
    const bus = await sinRed();
    const e = await rechazaCon("RESERVED_DOMAIN", bus.request("rpc.clientes.cliente.v1.obtener", {}));
    assert.ok(e instanceof ReservedDomainError);
    assert.match(e.message, /lo pone el SDK/);
    assert.match(e.message, /clientes\.cliente\.v1\.obtener/);
    assert.equal(e.fluxClass, ErrorClass.PERMANENT);
    await rechazaCon("RESERVED_DOMAIN", bus.respond("rpc.clientes.cliente.v1.obtener", () => ({})));
  });

  test("CONTROL: un dominio parecido NO está reservado (`rpcx`, `estado`): llega al patrón y pasa", async () => {
    assert.doesNotThrow(() => parseRpcOperation("rpcx.cliente.v1.obtener"));
    assert.doesNotThrow(() => parseRpcOperation("estado.cliente.v1.obtener"));
  });

  test("una operación que no cumple el patrón → INVALID_RPC_CALL, con el patrón en el mensaje", async () => {
    const bus = await sinRed();
    for (const mal of ["clientes.cliente.v1", "clientes.cliente.v1.obtener.x", "Clientes.cliente.v1.obtener", "clientes.cliente.v1.Obtener",
      "clientes.cliente.v1.obtener_cliente", "clientes.cliente.v0.obtener", "clientes.cliente.v1.*", "clientes.cliente.v1.>", "clientes.cliente.v1.obtener\n", "", "a b.c.v1.d"]) {
      const e = await rechazaCon("INVALID_RPC_CALL", bus.request(mal, {}));
      assert.match(e.message, /<dominio>\.<agregado>\.v<major>\.<operacion>/, mal);
      await rechazaCon("INVALID_RPC_CALL", bus.respond(mal, () => ({})));
    }
    for (const noCadena of [undefined, null, 42, {}, []]) {
      await rechazaCon("INVALID_RPC_CALL", bus.request(noCadena as never, {}));
    }
  });

  test("`timeoutMs` fuera de rango o que no es un entero → INVALID_RPC_CALL, y dice el rango (no se recorta)", async () => {
    const bus = await sinRed();
    for (const t of [0, -1, 25_001, 30_000, 1.5, "5000", null, NaN, Infinity, true]) {
      const e = await rechazaCon("INVALID_RPC_CALL", bus.request(OP, PETICION, { timeoutMs: t as never }));
      assert.match(e.message, /entre 1 y 25000/);
    }
  });

  test("`data` que no es un objeto → INVALID_RPC_CALL: array, escalar, null, undefined", async () => {
    const bus = await sinRed();
    for (const d of [[], [1], "texto", 42, null, undefined, true]) {
      await rechazaCon("INVALID_RPC_CALL", bus.request(OP, d as never));
    }
  });

  test("`retries`, `aggregateId` y `tenantId` mal formados → INVALID_RPC_CALL", async () => {
    const bus = await sinRed();
    for (const o of [{ retries: -1 }, { retries: 1.5 }, { retries: "2" }, { aggregateId: "" }, { aggregateId: 7 }, { tenantId: "" }, { tenantId: 7 }]) {
      await rechazaCon("INVALID_RPC_CALL", bus.request(OP, PETICION, o as never));
    }
  });

  test("una clasificación fuera del enum → INVALID_DATACLASSIFICATION (`restricted` SÍ es válida en una petición)", async () => {
    const bus = await sinRed();
    const e = await rechazaCon("INVALID_DATACLASSIFICATION", bus.request(OP, PETICION, { classification: "secreta" as never }));
    assert.ok(e instanceof InvalidDataClassificationError);
    assert.match(e.message, /public, internal, confidential o restricted/);
    // `restricted` no se rechaza por clasificación: el primer fallo que aparece es la red (la suscripción no existe)
    await assert.rejects(bus.request(OP, PETICION, { classification: "restricted" }), (x: Error) => x.message === REDTOCADA);
  });

  test("el orden de §4.1 paso 0: el primero que falla gana", async () => {
    const bus = await sinRed();
    // RESERVED_DOMAIN gana a todo lo demás
    await rechazaCon("RESERVED_DOMAIN", bus.request("rpc.x", [] as never, { timeoutMs: 0, classification: "x" as never }));
    // una operación mal formada gana a un timeout y a un `data` malos
    await rechazaCon("INVALID_RPC_CALL", bus.request("Mal", [] as never, { timeoutMs: 0 }));
    assert.match((await rechazaCon("INVALID_RPC_CALL", bus.request(OP, [] as never, { timeoutMs: 0 }))).message, /timeoutMs/);
    // INVALID_RPC_CALL gana a INVALID_DATACLASSIFICATION
    await rechazaCon("INVALID_RPC_CALL", bus.request(OP, [] as never, { classification: "x" as never }));
    // y la clasificación gana a L3 (aquí no hay L3: basta con que se llegue a ella antes de tocar la red)
    await rechazaCon("INVALID_DATACLASSIFICATION", bus.request(OP, PETICION, { classification: "x" as never }));
  });

  test("respond: un handler que no es una función, `maxConcurrent` inválido, o una clasificación fuera del enum", async () => {
    const bus = await sinRed();
    await rechazaCon("INVALID_RPC_CALL", bus.respond(OP, "no soy una función" as never));
    await rechazaCon("INVALID_RPC_CALL", bus.respond(OP, () => ({}), { maxConcurrent: 0 }));
    await rechazaCon("INVALID_RPC_CALL", bus.respond(OP, () => ({}), { maxConcurrent: 2.5 }));
    await rechazaCon("INVALID_DATACLASSIFICATION", bus.respond(OP, () => ({}), { classification: "x" as never }));
  });

  test("sin bundle ni `schemaBaseUrl` no hay `dataschema`: se dice qué hacer, sin tocar la red", async () => {
    const bus = await sinRed({ schemaBaseUrl: undefined });
    await assert.rejects(bus.request(OP, PETICION), (e: Error) => /schemaBaseUrl/.test(e.message) && e.message !== REDTOCADA);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 4. request(): la petición que sale
// ═════════════════════════════════════════════════════════════════════════════

/** Un responder en bruto (sin SDK): contesta con lo que `fn` devuelva a partir de la petición ya leída. */
function responderCon(red: RedEnMemoria, fn: (req: FluxEvent & Record<string, unknown>, m: MensajeFalso) => Uint8Array | string | undefined): void {
  const conn = red.conectar();
  red.escuchaCrudo(SUBJECT, (m) => {
    const respuesta = fn(evento(m.data), m);
    if (respuesta !== undefined) conn.publish(m.reply, typeof respuesta === "string" ? bytesDe(respuesta) : respuesta);
  });
}

/** La respuesta CORRECTA a una petición leída: la que construiría un SDK. */
function respuestaBuena(req: FluxEvent & Record<string, unknown>, data: object = { descuentoCents: 500, moneda: "EUR" }, extra: Record<string, unknown> = {}) {
  return buildRpcReply({
    request: { operation: OP, id: req.id, correlationid: req.correlationid, tenantid: req.tenantid, ...(req.subject !== undefined && { subject: req.subject }) },
    id: "01924f91-5d0f-7e03-9b6a-1c2d3e4f5a67",
    source: "/produccion/crm-api",
    time: "2026-08-20T10:41:07.329Z",
    producerversion: "2.1.0",
    classification: "internal",
    data,
    ...extra,
  });
}

describe("request() — lo que se publica", () => {
  test("el sobre lo rellena el SDK: id UUIDv7, source, type, time, dataschema, subject = aggregateId, correlationid propio y NINGUNA cabecera", async () => {
    const red = new RedEnMemoria();
    responderCon(red, (req) => encodeRpcMessage(respuestaBuena(req)));
    const { bus, nc } = await servicio(red);
    const r = await bus.request(OP, PETICION, { aggregateId: "cli-1", timeoutMs: 1500 });
    assert.deepEqual(r, { descuentoCents: 500, moneda: "EUR" });

    assert.equal(nc.enviadas.length, 1);
    assert.equal(nc.enviadas[0].subject, SUBJECT, "al subject de NATS (con `rpc.`), no al subject del sobre");
    assert.equal(nc.enviadas[0].timeout, 1500);
    const e = evento(nc.enviadas[0].bytes);
    assert.match(e.id, UUID_V7);
    assert.equal(e.source, "/produccion/facturacion-api");
    assert.equal(e.type, "com.flux.rpc.clientes.cliente.calcular-descuento.request.v1");
    assert.match(e.time, ISO_3_DECIMALES);
    assert.equal(e.dataschema, "https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json");
    assert.equal(e.subject, "cli-1", "el atributo `subject` es el aggregateId, NO el subject de NATS");
    assert.equal(e.partitionkey, "cli-1");
    assert.equal(e.correlationid, e.id, "una petición que no nace de otra se correlaciona consigo misma");
    assert.ok(!("causationid" in e));
    assert.equal(e.tenantid, "acme");
    assert.equal(e.producerversion, "3.4.1");
    assert.equal(e.dataclassification, "internal");
    assert.deepEqual(e.data, PETICION);
    assert.equal(Object.keys(e).at(-1), "data");
    assert.equal(nc.publicadas.length, 0, "request no publica por su cuenta: usa nc.request, con su inbox privado");
  });

  test("el timeout por defecto es 5000, y sin `aggregateId` no hay `subject` ni `partitionkey`", async () => {
    const red = new RedEnMemoria();
    responderCon(red, (req) => encodeRpcMessage(respuestaBuena(req)));
    const { bus, nc } = await servicio(red);
    await bus.request(OP, PETICION);
    assert.equal(nc.enviadas[0].timeout, 5000);
    const e = evento(nc.enviadas[0].bytes);
    assert.ok(!("subject" in e));
    assert.ok(!("partitionkey" in e));
  });

  test("el tenant: la opción > el evento en curso > connect() > `system` (como los eventos); un estado exigiría uno explícito, esto no", async () => {
    const red = new RedEnMemoria();
    responderCon(red, (req) => encodeRpcMessage(respuestaBuena(req)));
    const a = await servicio(red);
    await a.bus.request(OP, PETICION, { tenantId: "globex" });
    assert.equal(evento(a.nc.enviadas[0].bytes).tenantid, "globex");
    await a.bus.request(OP, PETICION);
    assert.equal(evento(a.nc.enviadas[1].bytes).tenantid, "acme");
    const enCurso = { specversion: "1.0", id: "ev-1", source: "/x/y", type: "t", time: "t", datacontenttype: "application/json", dataschema: "d",
      correlationid: "corr-1", tenantid: "initech", producerversion: "1", dataclassification: "internal", data: {} } as unknown as FluxEvent;
    await runWithContext(contextFromEvent(enCurso), () => a.bus.request(OP, PETICION));
    assert.equal(evento(a.nc.enviadas[2].bytes).tenantid, "initech", "el contexto heredado gana sobre el default de conexión");
    await runWithContext(contextFromEvent(enCurso), () => a.bus.request(OP, PETICION, { tenantId: "umbrella" }));
    assert.equal(evento(a.nc.enviadas[3].bytes).tenantid, "umbrella");
    const sinTenant = await servicio(red, { tenantId: undefined });
    await sinTenant.bus.request(OP, PETICION);
    assert.equal(evento(sinTenant.nc.enviadas[0].bytes).tenantid, "system");
  });

  test("dentro de un handler la llamada CONTINÚA la cadena: hereda correlationid y tenantid; su causationid es el id de lo que está en curso", async () => {
    const red = new RedEnMemoria();
    responderCon(red, (req) => encodeRpcMessage(respuestaBuena(req)));
    const { bus, nc } = await servicio(red);
    const enCurso = { specversion: "1.0", id: "ev-1", source: "/x/y", type: "t", time: "t", datacontenttype: "application/json", dataschema: "d",
      correlationid: "corr-1", causationid: "ev-0", tenantid: "initech", producerversion: "1", dataclassification: "internal",
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", data: {} } as unknown as FluxEvent;
    await runWithContext(contextFromEvent(enCurso), () => bus.request(OP, PETICION));
    const e = evento(nc.enviadas[0].bytes);
    assert.equal(e.correlationid, "corr-1");
    assert.equal(e.causationid, "ev-1");
    assert.equal(e.tenantid, "initech");
    assert.equal(e.traceparent, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
    assert.notEqual(e.id, "ev-1");
  });

  test("la clasificación: la de la llamada, y si no la de connect(), y si no `internal`", async () => {
    const red = new RedEnMemoria();
    responderCon(red, (req) => encodeRpcMessage(respuestaBuena(req)));
    const a = await servicio(red, { classification: "confidential" });
    await a.bus.request(OP, PETICION);
    await a.bus.request(OP, PETICION, { classification: "restricted" });
    assert.equal(evento(a.nc.enviadas[0].bytes).dataclassification, "confidential");
    assert.equal(evento(a.nc.enviadas[1].bytes).dataclassification, "restricted");
    const b = await servicio(red);
    await b.bus.request(OP, PETICION);
    assert.equal(evento(b.nc.enviadas[0].bytes).dataclassification, "internal");
  });
});

describe("request() — lo que ve quien llama (§4.3): cada situación se traduce a un código estable", () => {
  test("nadie escucha → RPC_UNAVAILABLE (retryable), sin esperar al timeout", async () => {
    const red = new RedEnMemoria();
    const { bus } = await servicio(red);
    const t0 = performance.now();
    const e = await assert.rejects(bus.request(OP, PETICION, { timeoutMs: 5000 }), (x: Error & { code?: string }) => {
      assert.ok(x instanceof RpcUnavailableError);
      assert.equal(x.code, "RPC_UNAVAILABLE");
      assert.equal(x.fluxClass, ErrorClass.RETRYABLE);
      assert.match(x.message, new RegExp(OP));
      return true;
    });
    void e;
    assert.ok(performance.now() - t0 < 500, "no esperó al timeout de 5 s");
  });

  test("no llega respuesta en el timeout → RPC_TIMEOUT (retryable, ambiguo), y la respuesta TARDÍA se ignora", async () => {
    const red = new RedEnMemoria();
    const ejecuciones: number[] = [];
    responderCon(red, (req, m) => {
      ejecuciones.push(1);
      setTimeout(() => red.conectar().publish(m.reply, encodeRpcMessage(respuestaBuena(req))), 120); // más tarde que el timeout
      return undefined;
    });
    const { bus, metricas } = await servicio(red);
    await assert.rejects(bus.request(OP, PETICION, { timeoutMs: 40 }), (x: Error & { code?: string }) => {
      assert.ok(x instanceof RpcTimeoutError);
      assert.equal(x.code, "RPC_TIMEOUT");
      assert.match(x.message, /PUEDE haberse ejecutado/);
      assert.match(x.message, /40 ms/);
      return true;
    });
    await sleep(160); // llega la respuesta tardía: no rompe nada
    assert.equal(ejecuciones.length, 1, "el responder ejecutó UNA vez (A-61): quien llama no lo sabe");
    assert.equal(metricas.snapshot().counters.get(`flux_rpc_client_requests_total{operation="${OP}",outcome="timeout"}`), 1);
  });

  test("una denegación de la librería (`RequestError` con causa `PermissionViolationError`) → PERMISSION_DENIED, nombrando el subject", async () => {
    const red = new RedEnMemoria();
    const { bus, nc } = await servicio(red);
    nc.falloDeLaPeticion = () =>
      new errors.RequestError(`Permissions Violation for Publish to "${SUBJECT}"`, {
        cause: errors.PermissionViolationError.parse(`Permissions Violation for Publish to "${SUBJECT}"`)!,
      });
    await assert.rejects(bus.request(OP, PETICION), (x: PermissionDeniedError) => {
      assert.ok(x instanceof PermissionDeniedError);
      assert.equal(x.code, "PERMISSION_DENIED");
      assert.equal(x.subject, SUBJECT);
      assert.equal(x.operation, "publish");
      assert.match(x.message, new RegExp(SUBJECT.replace(/\./g, "\\.")));
      assert.match(x.message, /EXACTO/);
      return true;
    });
  });

  test("una librería que NO entrega la denegación (async-nats, A-72): el `-ERR` de la conexión se compite con la respuesta", async () => {
    const red = new RedEnMemoria();
    const { bus, nc } = await servicio(red);
    nc.mudo = true; // la petición sale, y ni se contesta ni se falla: solo habría un timeout
    const t0 = performance.now();
    const llamada = bus.request(OP, PETICION, { timeoutMs: 5000 });
    await sleep(20);
    nc.emiteEstado({ type: "error", error: errors.PermissionViolationError.parse(`Permissions Violation for Publish to "${SUBJECT}"`) });
    await assert.rejects(llamada, (x: PermissionDeniedError) => x instanceof PermissionDeniedError && x.code === "PERMISSION_DENIED" && x.subject === SUBJECT);
    assert.ok(performance.now() - t0 < 1000, "falló rápido, no tras el timeout de 5 s");
  });

  test("CONTROL: ese mismo `-ERR` sobre OTRO subject no hace fallar una llamada que va bien", async () => {
    const red = new RedEnMemoria();
    responderCon(red, (req) => encodeRpcMessage(respuestaBuena(req)));
    const { bus, nc } = await servicio(red);
    nc.emiteEstado({ type: "error", error: errors.PermissionViolationError.parse('Permissions Violation for Publish to "rpc.otra.cosa.v1.x"') });
    await sleep(5);
    assert.deepEqual(await bus.request(OP, PETICION), { descuentoCents: 500, moneda: "EUR" });
  });

  test("un fallo de transporte que no es de la petición/respuesta (conexión cerrada) NO se disfraza de uno de los seis", async () => {
    const red = new RedEnMemoria();
    const { bus, nc } = await servicio(red);
    nc.falloDeLaPeticion = () => new errors.ClosedConnectionError();
    await assert.rejects(bus.request(OP, PETICION), (x: Error) => x instanceof errors.ClosedConnectionError);
  });
});

describe("request() — las cinco comprobaciones de la respuesta (§4.3) → RPC_INVALID_REPLY", () => {
  /** Una petición cuya respuesta se altera con `muta`: debe dar RPC_INVALID_REPLY. */
  async function conRespuesta(muta: (r: Record<string, unknown>, req: FluxEvent & Record<string, unknown>) => unknown, esperaPista = false) {
    const red = new RedEnMemoria();
    responderCon(red, (req) => {
      const buena = JSON.parse(texto(encodeRpcMessage(respuestaBuena(req))));
      const alterada = muta(buena, req);
      return alterada === undefined ? encodeRpcMessage(buena) : typeof alterada === "string" ? alterada : bytesDe(JSON.stringify(alterada));
    });
    const { bus, metricas } = await servicio(red);
    return bus.request(OP, PETICION, { timeoutMs: 1000 }).then(
      () => assert.fail("debía fallar con RPC_INVALID_REPLY"),
      (e: Error & { code?: string }) => {
        assert.ok(e instanceof RpcInvalidReplyError, `${e.name}: ${e.message}`);
        assert.equal(e.code, "RPC_INVALID_REPLY");
        assert.equal(e.fluxClass, ErrorClass.PERMANENT);
        assert.equal(/flux doctor/.test(e.message), esperaPista);
        assert.equal(metricas.snapshot().counters.get(`flux_rpc_client_requests_total{operation="${OP}",outcome="invalid_reply"}`), 1, "tiene su propio outcome");
        return e;
      },
    );
  }

  test("(0) CONTROL: la respuesta buena pasa, así que todo lo de abajo falla por LO QUE SE ALTERA y no por otra cosa", async () => {
    const red = new RedEnMemoria();
    responderCon(red, (req) => encodeRpcMessage(respuestaBuena(req)));
    const { bus } = await servicio(red);
    assert.deepEqual(await bus.request(OP, PETICION), { descuentoCents: 500, moneda: "EUR" });
  });

  test("(1) no es un CloudEvent válido: JSON roto, no es un objeto, falta una extensión, un tipo equivocado", async () => {
    await conRespuesta(() => "esto no es json");
    await conRespuesta(() => "[1,2,3]");
    await conRespuesta((r) => { delete r.tenantid; });
    await conRespuesta((r) => { delete r.dataclassification; });
    await conRespuesta((r) => { r.tenantid = 42; }); // los tipos son exactos: 42 no es el tenant "42"
    await conRespuesta((r) => { r.specversion = "0.3"; });
    await conRespuesta((r) => { r.extra = 1; }); // un atributo raíz que no existe
  });

  test("(2) su `type` no es el de la respuesta de ESTA operación ni el del error", async () => {
    await conRespuesta((r) => { r.type = "com.flux.rpc.clientes.cliente.obtener.reply.v1"; }); // la respuesta de OTRA operación
    await conRespuesta((r) => { r.type = "com.flux.rpc.clientes.cliente.calcular-descuento.request.v1"; }); // la petición, no la respuesta
    await conRespuesta((r) => { r.type = "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v2"; }); // otra versión mayor
    await conRespuesta((r) => { r.type = "com.flux.rpc.error.v2"; });
    await conRespuesta((r) => { r.type = "com.flux.clientes.cliente.creado.v1"; }); // un evento
  });

  test("(3) su `causationid` no es el `id` de la petición (o no lo trae)", async () => {
    await conRespuesta((r) => { r.causationid = "01924f91-0000-7000-8000-000000000000"; });
    await conRespuesta((r) => { delete r.causationid; });
    await conRespuesta((_r, req) => ({ ..._r, causationid: undefined, correlationid: req.id })); // no lo trae
  });

  test("(4) su `correlationid` o su `tenantid` no son los de la petición", async () => {
    await conRespuesta((r) => { r.correlationid = "otra-cadena"; });
    await conRespuesta((r) => { r.tenantid = "globex"; });
  });

  test("(5) si es un error, su `data` no cumple el esquema del error", async () => {
    const error = (req: FluxEvent & Record<string, unknown>, e: Record<string, unknown>) =>
      JSON.parse(texto(encodeRpcMessage(buildRpcError({
        request: { operation: OP, id: req.id, correlationid: req.correlationid, tenantid: req.tenantid },
        id: "01924f91-5d11-7a62-8f0d-4e5f6a7b8c91", source: "/produccion/crm-api", time: "2026-08-20T10:41:07.331Z", producerversion: "2.1.0",
        error: { class: "permanent", code: "X", message: "m" },
      }))));
    for (const malo of [
      { class: "poison", code: "X", message: "m" },
      { class: "permanent", code: "x_minuscula", message: "m" },
      { class: "permanent", code: "X" },
      { class: "permanent", code: "X", message: "m", otro: 1 },
      { class: "permanent", code: "X", message: "m", retryAfterMs: 5 },
      "no es un objeto",
    ]) {
      await conRespuesta((_r, req) => ({ ...error(req, {}), data: malo }));
    }
  });

  describe("la excepción única de la comprobación 4 (§4.3): el error de una petición que quien responde no pudo leer", () => {
    const ID_ERROR = "01924f91-5d13-7b85-a3e1-6a7b8c9d0e22";
    /** Lo que contesta un responder EN BRUTO a una petición ilegible (§5.2): `correlationid` = su propio `id` y `tenantid: system`. */
    const errorIlegible = (req: FluxEvent & Record<string, unknown>, data: object = { class: "permanent", code: "MALFORMED_JSON", message: "la petición no es un CloudEvent válido" }, muta: (e: Record<string, unknown>) => void = () => {}) => {
      const e = {
        specversion: "1.0", id: ID_ERROR, source: "/produccion/crm-api", type: "com.flux.rpc.error.v1",
        time: "2026-08-20T10:41:07.333Z", datacontenttype: "application/json",
        dataschema: "https://schemas.internal/system/rpc/error/1.0.0.json",
        correlationid: ID_ERROR, tenantid: "system", producerversion: "2.1.0", dataclassification: "internal",
        causationid: req.id, data,
      } as Record<string, unknown>;
      muta(e);
      return JSON.stringify(e);
    };

    test("SÍ pasa: quien llama ve el código del sobre (MALFORMED_JSON, INVALID_RPC_CALL…) como PermanentError, no RPC_INVALID_REPLY", async () => {
      for (const code of ["MALFORMED_JSON", "MISSING_REQUIRED_EXTENSION", "INVALID_RPC_CALL", "WRONG_ATTRIBUTE_TYPE"]) {
        const red = new RedEnMemoria();
        responderCon(red, (req) => errorIlegible(req, { class: "permanent", code, message: "la petición no se pudo leer" }));
        const { bus, metricas } = await servicio(red);
        await assert.rejects(bus.request(OP, PETICION, { timeoutMs: 1000 }), (e: RpcPermanentError) => {
          assert.ok(e instanceof RpcPermanentError, `${e.name}: ${e.message}`);
          assert.equal(e.code, code);
          assert.equal(e.fluxClass, ErrorClass.PERMANENT);
          return true;
        });
        // es un error del que responde, no un incidente de respuesta ajena: cuenta como `permanent`, no `invalid_reply`
        assert.equal(metricas.snapshot().counters.get(`flux_rpc_client_requests_total{operation="${OP}",outcome="permanent"}`), 1);
        assert.equal(metricas.snapshot().counters.get(`flux_rpc_client_requests_total{operation="${OP}",outcome="invalid_reply"}`), undefined);
      }
    });

    /** Debe dar RPC_INVALID_REPLY: la excepción es ESTRECHA. */
    async function noPasa(contesta: (req: FluxEvent & Record<string, unknown>) => string) {
      const red = new RedEnMemoria();
      responderCon(red, contesta);
      const { bus } = await servicio(red);
      await assert.rejects(bus.request(OP, PETICION, { timeoutMs: 1000 }), (e: Error & { code?: string }) => {
        assert.equal(e.code, "RPC_INVALID_REPLY", `${e.name}: ${e.message}`);
        return true;
      });
    }

    test("la comprobación 3 SIGUE en pie: sin `causationid`, o con el de otra petición, es RPC_INVALID_REPLY", async () => {
      await noPasa((req) => errorIlegible(req, undefined, (e) => { delete e.causationid; }));
      await noPasa((req) => errorIlegible(req, undefined, (e) => { e.causationid = "01924f91-0000-7000-8000-000000000000"; }));
    });

    test("es ESTRECHA: solo un ERROR con `tenantid: system` Y `correlationid` igual a su propio `id`", async () => {
      // `tenantid: system` pero con un `correlationid` que no es el suyo ni el de la petición
      await noPasa((req) => errorIlegible(req, undefined, (e) => { e.correlationid = "otro-flujo"; }));
      // `correlationid` propio pero con un tenant que no es `system` ni el de la petición
      await noPasa((req) => errorIlegible(req, undefined, (e) => { e.tenantid = "globex"; }));
      // una RESPUESTA (no un error) con la misma forma: no es lo que la excepción cubre
      await noPasa((req) => JSON.stringify({
        ...JSON.parse(texto(encodeRpcMessage(respuestaBuena(req)))), id: ID_ERROR, correlationid: ID_ERROR, tenantid: "system",
      }));
    });

    test("la quinta comprobación SIGUE en pie: el `data` del error ilegible debe cumplir el esquema del error", async () => {
      await noPasa((req) => errorIlegible(req, { class: "poison", code: "MALFORMED_JSON", message: "m" }));
      await noPasa((req) => errorIlegible(req, { class: "permanent", code: "malformed_json", message: "m" }));
    });

    test("CONTROL: un error legítimo de una petición LEÍDA sigue exigiendo heredar `correlationid` y `tenantid`", async () => {
      await noPasa((req) => errorIlegible(req, undefined, (e) => { e.correlationid = req.correlationid; e.tenantid = "globex"; }));
      await noPasa((req) => errorIlegible(req, undefined, (e) => { e.correlationid = "otra-cadena"; e.tenantid = req.tenantid; }));
    });

    test("el predicado: la forma exacta, y solo para un error", () => {
      const e = { id: ID_ERROR, correlationid: ID_ERROR, tenantid: "system" } as unknown as FluxEvent;
      assert.equal(esErrorDeUnaPeticionIlegible(e, true), true);
      assert.equal(esErrorDeUnaPeticionIlegible(e, false), false, "una respuesta no es un error");
      assert.equal(esErrorDeUnaPeticionIlegible({ ...e, tenantid: "acme" } as FluxEvent, true), false);
      assert.equal(esErrorDeUnaPeticionIlegible({ ...e, correlationid: "x" } as FluxEvent, true), false);
    });
  });

  test("el acuse de un stream (`{\"stream\":…,\"seq\":…}`, A-18/A-67) es RPC_INVALID_REPLY y APUNTA a `flux doctor`", async () => {
    const e = await conRespuesta(() => '{"stream":"TRAMPA","seq":1}', true);
    assert.match(e.message, /rpc\.>/);
    // y con una respuesta rota que NO es un acuse, no se sugiere una causa que no es
    await conRespuesta(() => "basura", false);
  });

  test("un error legítimo SÍ pasa las cinco comprobaciones: llega como RetryableError / PermanentError con su `code`", async () => {
    const red = new RedEnMemoria();
    responderCon(red, (req) =>
      encodeRpcMessage(buildRpcError({
        request: { operation: OP, id: req.id, correlationid: req.correlationid, tenantid: req.tenantid },
        id: "01924f91-5d11-7a62-8f0d-4e5f6a7b8c91", source: "/produccion/crm-api", time: "2026-08-20T10:41:07.331Z", producerversion: "2.1.0",
        error: { class: "retryable", code: "HTTP_503", message: "el servicio de tarifas no responde", retryAfterMs: 5000 },
      })),
    );
    const { bus, metricas } = await servicio(red);
    await assert.rejects(bus.request(OP, PETICION), (x: RpcRetryableError) => {
      assert.ok(x instanceof RetryableError);
      assert.equal(x.code, "HTTP_503");
      assert.equal(x.message, "el servicio de tarifas no responde");
      // un handler de eventos que la deja escapar hereda la clasificación sin hacer nada
      const c = classify(x);
      assert.equal(c.class, ErrorClass.RETRYABLE);
      assert.equal(c.code, "HTTP_503");
      assert.equal(c.retryAfterMs, 5000);
      return true;
    });
    assert.equal(metricas.snapshot().counters.get(`flux_rpc_client_requests_total{operation="${OP}",outcome="retryable"}`), 1);
  });
});

describe("request() — reintentos (`retries`, por defecto 0) con el MISMO id y los mismos bytes", () => {
  test("por defecto UN intento: un timeout no se reintenta", async () => {
    const red = new RedEnMemoria();
    red.escuchaCrudo(SUBJECT, () => {}); // escucha y no contesta
    const { bus, nc } = await servicio(red);
    await assert.rejects(bus.request(OP, PETICION, { timeoutMs: 30 }), RpcTimeoutError);
    assert.equal(nc.enviadas.length, 1);
  });

  test("`retries: 2` ante timeouts: tres intentos, y el `id`, el `time` y los BYTES son los mismos", async () => {
    const red = new RedEnMemoria();
    let n = 0;
    responderCon(red, (req) => (++n < 3 ? undefined : encodeRpcMessage(respuestaBuena(req))));
    const { bus, nc, metricas } = await servicio(red);
    assert.deepEqual(await bus.request(OP, PETICION, { timeoutMs: 30, retries: 2 }), { descuentoCents: 500, moneda: "EUR" });
    assert.equal(nc.enviadas.length, 3);
    const ids = nc.enviadas.map((e) => evento(e.bytes).id);
    assert.equal(new Set(ids).size, 1, "un `id` nuevo por intento haría imposible reconocer la orden repetida");
    assert.equal(new Set(nc.enviadas.map((e) => texto(e.bytes))).size, 1, "los mismos bytes, `time` incluido");
    assert.equal(metricas.snapshot().counters.get(`flux_rpc_client_requests_total{operation="${OP}",outcome="ok"}`), 1, "UNA métrica por llamada, no por intento");
  });

  test("`retries` ante RPC_UNAVAILABLE: el servicio aparece entre intentos y la llamada acaba bien", async () => {
    const red = new RedEnMemoria();
    const { bus, nc } = await servicio(red);
    setTimeout(() => responderCon(red, (req) => encodeRpcMessage(respuestaBuena(req))), 80);
    assert.deepEqual(await bus.request(OP, PETICION, { retries: 3 }), { descuentoCents: 500, moneda: "EUR" });
    assert.ok(nc.enviadas.length >= 2 && nc.enviadas.length <= 4);
    assert.equal(new Set(nc.enviadas.map((e) => evento(e.bytes).id)).size, 1);
  });

  test("agotados los reintentos, se lanza el ÚLTIMO error y se cuentan N+1 envíos", async () => {
    const red = new RedEnMemoria();
    const { bus, nc } = await servicio(red);
    await assert.rejects(bus.request(OP, PETICION, { retries: 2 }), RpcUnavailableError);
    assert.equal(nc.enviadas.length, 3);
  });

  test("NUNCA se reintenta un error permanente, una respuesta inválida ni una denegación", async () => {
    const casos: Array<[string, (red: RedEnMemoria, nc: ConexionFalsa) => void, new (...a: never[]) => Error]> = [
      ["error permanent del que responde", (red) => responderCon(red, (req) => encodeRpcMessage(buildRpcError({
        request: { operation: OP, id: req.id, correlationid: req.correlationid, tenantid: req.tenantid },
        id: "01924f91-5d11-7a62-8f0d-4e5f6a7b8c91", source: "/x/y", time: "2026-08-20T10:41:07.331Z", producerversion: "2.1.0",
        error: { class: "permanent", code: "NO", message: "no" } }))), RpcPermanentError as never],
      ["un error retryable del que responde (no es UNAVAILABLE ni TIMEOUT)", (red) => responderCon(red, (req) => encodeRpcMessage(buildRpcError({
        request: { operation: OP, id: req.id, correlationid: req.correlationid, tenantid: req.tenantid },
        id: "01924f91-5d11-7a62-8f0d-4e5f6a7b8c91", source: "/x/y", time: "2026-08-20T10:41:07.331Z", producerversion: "2.1.0",
        error: { class: "retryable", code: "OCUPADO", message: "ocupado" } }))), RpcRetryableError as never],
      ["una respuesta que no es de esta petición", (red) => responderCon(red, () => "basura"), RpcInvalidReplyError as never],
      ["una denegación", (_red, nc) => { nc.falloDeLaPeticion = () => new errors.RequestError("x", { cause: errors.PermissionViolationError.parse(`Permissions Violation for Publish to "${SUBJECT}"`)! }); }, PermissionDeniedError as never],
    ];
    for (const [nombre, prepara, clase] of casos) {
      const red = new RedEnMemoria();
      const { bus, nc } = await servicio(red);
      prepara(red, nc);
      await assert.rejects(bus.request(OP, PETICION, { retries: 3, timeoutMs: 200 }), clase, nombre);
      assert.equal(nc.enviadas.length, 1, `${nombre}: se reintentó`);
    }
  });
});

describe("request() — el tamaño se comprueba ANTES de publicar (§4.4) → RPC_MESSAGE_TOO_LARGE", () => {
  /** Una petición cuyo mensaje serializado ocupa EXACTAMENTE `bytes`. */
  async function conTamano(nc: ConexionFalsa, bus: FluxBus, bytes: number): Promise<unknown> {
    const vacio = encodeRpcMessage(buildRpcRequest({ operation: OP, tenantId: "acme", id: "0".repeat(36), source: "/produccion/facturacion-api",
      time: "2026-08-20T10:41:07.318Z", correlationid: "0".repeat(36), producerversion: "3.4.1", classification: "internal", data: { relleno: "" },
      dataschema: "https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json" })).byteLength;
    void nc;
    return bus.request(OP, { relleno: "x".repeat(bytes - vacio) }, { timeoutMs: 100 });
  }

  test("el límite es el `max_payload` del broker si es menor que 1 MiB: EXACTAMENTE ese tamaño pasa, uno más falla sin tocar la red", async () => {
    const red = new RedEnMemoria();
    red.escuchaCrudo(SUBJECT, () => {});
    const { bus, nc } = await servicio(red);
    nc.info = { max_payload: 4096 };
    // el `id` y el `correlationid` reales miden 36 caracteres, como los de `conTamano`: el tamaño sale exacto
    await assert.rejects(conTamano(nc, bus, 4096), RpcTimeoutError, "exactamente max_payload pasa (llega a esperar su timeout)");
    assert.equal(nc.enviadas.length, 1);
    nc.enviadas.length = 0;
    await assert.rejects(conTamano(nc, bus, 4097), (e: RpcMessageTooLargeError) => {
      assert.equal(e.code, "RPC_MESSAGE_TOO_LARGE");
      assert.equal(e.fluxClass, ErrorClass.PERMANENT);
      assert.equal(e.bytes, 4097);
      assert.equal(e.limit, 4096);
      assert.match(e.message, /4097 bytes/);
      assert.match(e.message, /4096/);
      assert.match(e.message, /claim-check/);
      assert.match(e.message, /cursor/);
      return true;
    });
    assert.equal(nc.enviadas.length, 0, "falló ANTES de abrir nada");
  });

  test("el límite es 1 MiB aunque el broker anuncie más, y sin `info` (una conexión que no lo da) también", async () => {
    const red = new RedEnMemoria();
    red.escuchaCrudo(SUBJECT, () => {});
    const { bus, nc } = await servicio(red);
    nc.info = { max_payload: 64 * 1024 * 1024 };
    await assert.rejects(conTamano(nc, bus, 1_048_577), (e: RpcMessageTooLargeError) => e.limit === 1_048_576 && e.bytes === 1_048_577);
    nc.info = undefined;
    await assert.rejects(conTamano(nc, bus, 1_048_577), RpcMessageTooLargeError);
    await assert.rejects(conTamano(nc, bus, 1_048_576), RpcTimeoutError);
    assert.equal(nc.enviadas.length, 1);
  });

  test("con la firma encima el mensaje crece: se comprueba el tamaño FIRMADO, y en el borde también es RPC_MESSAGE_TOO_LARGE", async () => {
    const { privateKeyPem } = generateKeyPair();
    const red = new RedEnMemoria();
    red.escuchaCrudo(SUBJECT, () => {});
    const { bus, nc } = await servicio(red, { signing: { privateKeyPem, keyId: "facturacion-api-1" } });
    nc.info = { max_payload: 8192 };
    // sin firma cabría con holgura de 100 bytes; firmado (signkeyid + signature) se pasa
    await assert.rejects(conTamano(nc, bus, 8192 - 100), RpcMessageTooLargeError);
    assert.equal(nc.enviadas.length, 0);
    // y en el borde de 1 MiB, el firmante de eventos lanzaría su propio error: aquí es el mismo código
    nc.info = undefined;
    await assert.rejects(conTamano(nc, bus, 1_048_576 - 100), RpcMessageTooLargeError);
  });

  test("una petición demasiado grande se cuenta como `permanent`", async () => {
    const red = new RedEnMemoria();
    const { bus, nc, metricas } = await servicio(red);
    nc.info = { max_payload: 2048 };
    await assert.rejects(conTamano(nc, bus, 3000), RpcMessageTooLargeError);
    assert.equal(metricas.snapshot().counters.get(`flux_rpc_client_requests_total{operation="${OP}",outcome="permanent"}`), 1);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 5. respond(): quien atiende
// ═════════════════════════════════════════════════════════════════════════════

const ID_PETICION = "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10";
const TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

/** Una petición bien formada, como la construiría un SDK. `extra` la altera. */
function peticion(extra: Partial<Parameters<typeof buildRpcRequest>[0]> = {}, operation = OP): Uint8Array {
  return encodeRpcMessage(
    buildRpcRequest({
      operation,
      tenantId: "acme",
      aggregateId: "cli-1",
      id: ID_PETICION,
      source: "/produccion/facturacion-api",
      time: new Date().toISOString(),
      correlationid: ID_PETICION,
      producerversion: "3.4.1",
      classification: "internal",
      traceparent: TRACEPARENT,
      data: PETICION,
      dataschema: rpcDataschema(operation, "request"),
      ...extra,
    }),
  );
}

/** Espera a que `nc` publique en `inbox` y devuelve el mensaje leído (o `undefined` si no lo hace en `ms`). */
async function esperaPublicada(nc: ConexionFalsa, inbox: string, ms = 1000): Promise<(FluxEvent & Record<string, unknown>) | undefined> {
  const limite = Date.now() + ms;
  while (Date.now() < limite) {
    const p = nc.publicadas.find((x) => x.subject === inbox);
    if (p) return evento(p.bytes);
    await sleep(2);
  }
  return undefined;
}

/** Entrega una petición al responder y espera su respuesta. */
async function servir(s: Servicio, bytes: Uint8Array | string, inbox = "_INBOX.llama.1"): Promise<(FluxEvent & Record<string, unknown>) | undefined> {
  s.nc.entrega(SUBJECT, bytes, inbox);
  return esperaPublicada(s.nc, inbox);
}

async function atiende(handler: Parameters<FluxBus["respond"]>[1] = () => ({ descuentoCents: 500, moneda: "EUR" }), extra: Partial<ConnectOptions> = {}, opts?: Parameters<FluxBus["respond"]>[2]) {
  const red = new RedEnMemoria();
  const s = await servicio(red, { ...CRM, ...extra });
  const r = await s.bus.respond(OP, handler, opts);
  return { ...s, red, r };
}

describe("respond() — la suscripción", () => {
  test("UNA suscripción al subject EXACTO de la operación, con el nombre del servicio como grupo de cola; nunca `rpc.<dominio>.>`", async () => {
    const { nc, r } = await atiende();
    assert.equal(nc.argumentosDeSubscribe.length, 1);
    assert.equal(nc.argumentosDeSubscribe[0].subject, SUBJECT, "el subject exacto");
    assert.deepEqual(nc.argumentosDeSubscribe[0].opts, { queue: "crm-api" }, "el grupo de cola es el servicio");
    assert.ok(!nc.argumentosDeSubscribe.some((a) => /[*>]/.test(a.subject)), "ningún comodín");
    assert.equal(r.subject, SUBJECT);
    assert.equal(r.operation, OP);
  });

  test("dos operaciones del mismo servicio: dos suscripciones, el MISMO grupo de cola", async () => {
    const red = new RedEnMemoria();
    const s = await servicio(red, CRM);
    await s.bus.respond(OP, () => ({}));
    await s.bus.respond("clientes.cliente.v1.obtener", () => ({}));
    assert.deepEqual(s.nc.argumentosDeSubscribe.map((a) => a.subject), [SUBJECT, "rpc.clientes.cliente.v1.obtener"]);
    assert.ok(s.nc.argumentosDeSubscribe.every((a) => (a.opts as { queue: string }).queue === "crm-api"));
  });

  test("dos instancias del mismo servicio se REPARTEN las peticiones: cada petición la atiende UNA", async () => {
    const red = new RedEnMemoria();
    const a = await servicio(red, CRM);
    const b = await servicio(red, CRM);
    const cuenta = { a: 0, b: 0 };
    await a.bus.respond(OP, () => (cuenta.a++, { descuentoCents: 1, moneda: "EUR" }));
    await b.bus.respond(OP, () => (cuenta.b++, { descuentoCents: 1, moneda: "EUR" }));
    const llama = await servicio(red);
    for (let i = 0; i < 100; i++) await llama.bus.request(OP, PETICION);
    assert.equal(cuenta.a + cuenta.b, 100, "ni se pierde ni se duplica una petición");
    assert.ok(cuenta.a > 10 && cuenta.b > 10, `reparto ${JSON.stringify(cuenta)}`);
  });

  test("dos SERVICIOS (dos grupos) sobre la misma operación: CADA UNO ejecuta la petición (A-60): por eso solo el dueño responde", async () => {
    const red = new RedEnMemoria();
    const a = await servicio(red, CRM);
    const b = await servicio(red, { service: "otro-api" });
    let ejecuciones = 0;
    await a.bus.respond(OP, () => (ejecuciones++, { descuentoCents: 1, moneda: "EUR" }));
    await b.bus.respond(OP, () => (ejecuciones++, { descuentoCents: 1, moneda: "EUR" }));
    const llama = await servicio(red);
    await llama.bus.request(OP, PETICION);
    await sleep(20);
    assert.equal(ejecuciones, 2);
  });

  test("registrar dos veces la misma operación en un servicio es INVALID_RPC_CALL; tras `stop()` se puede volver a registrar", async () => {
    const red = new RedEnMemoria();
    const s = await servicio(red, CRM);
    const r = await s.bus.respond(OP, () => ({}));
    const e = await rechazaCon("INVALID_RPC_CALL", s.bus.respond(OP, () => ({})));
    assert.match(e.message, /ya hay un `respond\(\)`/);
    await r.stop();
    await assert.doesNotReject(s.bus.respond(OP, () => ({})));
  });

  test("un dueño SIN permiso de suscripción: `respond()` FALLA con PERMISSION_DENIED nombrando el subject (no arranca mudo)", async () => {
    const red = new RedEnMemoria();
    const s = await servicio(red, CRM);
    // el broker no lanza al suscribirse: el `-ERR` llega y hace que el iterador de la suscripción lance (A-8f)
    const original = s.nc.subscribe.bind(s.nc);
    s.nc.subscribe = (subject, opts) => {
      const sub = original(subject, opts);
      queueMicrotask(() => sub.falla(errors.PermissionViolationError.parse(`Permissions Violation for Subscription to "${subject}" using queue "${opts?.queue}"`)));
      return sub;
    };
    const e = await rechazaCon("PERMISSION_DENIED", s.bus.respond(OP, () => ({})));
    assert.ok(e instanceof PermissionDeniedError);
    assert.equal(e.operation, "subscribe");
    assert.equal(e.subject, SUBJECT);
    assert.match(e.message, /rpc\.<dominio>\.>/);
    assert.match(e.message, /RPC_UNAVAILABLE/, "dice por qué importa: parecería un servicio caído");
    // y quien llama ve RPC_UNAVAILABLE: la operación no tiene interés
    const llama = await servicio(red);
    await assert.rejects(llama.bus.request(OP, PETICION), RpcUnavailableError);
    // un dueño que falló al arrancar no deja la operación «registrada»: se puede volver a intentar
    s.nc.subscribe = original;
    await assert.doesNotReject(s.bus.respond(OP, () => ({})));
  });
});

describe("respond() — la respuesta (§3.2)", () => {
  test("hereda correlationid, tenantid, subject y partitionkey; su causationid es el id de la petición; id NUEVO; source y producerversion son del que responde", async () => {
    const { nc } = await atiende();
    const e = await servir({ nc } as Servicio, peticion());
    assert.ok(e, "no contestó");
    assert.match(e.id, UUID_V7);
    assert.notEqual(e.id, ID_PETICION, "nunca el id de la petición");
    assert.equal(e.source, "/produccion/crm-api");
    assert.equal(e.type, "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1");
    assert.match(e.time, ISO_3_DECIMALES);
    assert.equal(e.dataschema, "https://schemas.internal/rpc/clientes/cliente/calcular-descuento/reply/1.0.0.json");
    assert.equal(e.subject, "cli-1");
    assert.equal(e.partitionkey, "cli-1");
    assert.equal(e.correlationid, ID_PETICION);
    assert.equal(e.tenantid, "acme");
    assert.equal(e.producerversion, "2.1.0");
    assert.equal(e.causationid, ID_PETICION);
    assert.equal(e.dataclassification, "internal");
    assert.equal(e.traceparent, TRACEPARENT, "sin span activo, la misma traza de la petición");
    assert.deepEqual(e.data, { descuentoCents: 500, moneda: "EUR" });
    assert.equal(Object.keys(e).at(-1), "data");
    assert.deepEqual(Object.keys(e).slice(0, 4), ["specversion", "id", "source", "type"]);
  });

  test("NINGUNA cabecera de NATS: la publicación lleva solo (subject, bytes)", async () => {
    const { nc } = await atiende();
    await servir({ nc } as Servicio, peticion());
    assert.equal(nc.publicadas.length, 1);
    assert.equal(nc.publicadas[0].argumentos, 2, "ni `headers` ni `reply`: el sobre es la única fuente de metadatos");
  });

  test("una respuesta de una operación sin `aggregateId` no lleva `subject` ni `partitionkey`", async () => {
    const { nc } = await atiende();
    const { subject: _s, partitionkey: _p, ...sin } = evento(peticion());
    const e = await servir({ nc } as Servicio, JSON.stringify({ ...sin, data: PETICION }));
    assert.ok(e);
    assert.ok(!("subject" in e) && !("partitionkey" in e));
  });

  test("la clasificación de la respuesta: la de la operación, si no la de connect(), si no `internal`", async () => {
    const a = await atiende(undefined, { classification: "confidential" });
    assert.equal((await servir({ nc: a.nc } as Servicio, peticion()))?.dataclassification, "confidential");
    const b = await atiende(undefined, { classification: "confidential" }, { classification: "restricted" });
    assert.equal((await servir({ nc: b.nc } as Servicio, peticion()))?.dataclassification, "restricted");
    const c = await atiende();
    assert.equal((await servir({ nc: c.nc } as Servicio, peticion()))?.dataclassification, "internal");
  });

  test("el handler recibe `(petición, ctx)`: `data` y el sobre, el id, el correlationId, el tenantId, el aggregateId y el time", async () => {
    let visto: { data: unknown; ctx: Parameters<Parameters<FluxBus["respond"]>[1]>[1] } | undefined;
    const { nc } = await atiende((data, ctx) => ((visto = { data, ctx }), { descuentoCents: 1, moneda: "EUR" }));
    await servir({ nc } as Servicio, peticion());
    assert.ok(visto);
    assert.deepEqual(visto.data, PETICION);
    assert.equal(visto.ctx.operation, OP);
    assert.equal(visto.ctx.id, ID_PETICION);
    assert.equal(visto.ctx.correlationId, ID_PETICION);
    assert.equal(visto.ctx.tenantId, "acme");
    assert.equal(visto.ctx.aggregateId, "cli-1");
    assert.equal(visto.ctx.event.type, "com.flux.rpc.clientes.cliente.calcular-descuento.request.v1");
    assert.match(visto.ctx.time, ISO_3_DECIMALES);
  });

  test("el responder NO filtra por el tenant de `connect()`: una petición de otro tenant llega al handler (es suyo autorizarlo, §5.6)", async () => {
    let tenant: string | undefined;
    const { nc } = await atiende((_d, ctx) => ((tenant = ctx.tenantId), { descuentoCents: 1, moneda: "EUR" }), { tenantId: "acme", tenantIsolation: "strict" });
    const e = await servir({ nc } as Servicio, peticion({ tenantId: "globex" }));
    assert.equal(tenant, "globex");
    assert.equal(e?.tenantid, "globex", "y la respuesta lo hereda sin modificar");
  });

  test("una llamada hecha DESDE el handler continúa la cadena: hereda correlationid y tenantid, y su causationid es el id de la petición", async () => {
    const red = new RedEnMemoria();
    const crm = await servicio(red, CRM);
    const tarifas = await servicio(red, { service: "tarifas-api" });
    let vistoEnTarifas: FluxEvent | undefined;
    await tarifas.bus.respond("tarifas.tarifa.v1.calcular", (_d, ctx) => ((vistoEnTarifas = ctx.event), { precioCents: 100 }));
    await crm.bus.respond(OP, async () => {
      await crm.bus.request("tarifas.tarifa.v1.calcular", { sku: "ABC-1" }, { aggregateId: "cli-1" });
      return { descuentoCents: 500, moneda: "EUR" };
    });
    const e = await servir(crm, peticion({ tenantId: "globex", correlationid: "corr-raiz" }));
    assert.ok(e);
    assert.ok(vistoEnTarifas);
    assert.equal(vistoEnTarifas.correlationid, "corr-raiz", "un flujo que cruza operaciones se sigue con un solo correlationid");
    assert.equal(vistoEnTarifas.tenantid, "globex");
    assert.equal(vistoEnTarifas.causationid, ID_PETICION);
    assert.equal(vistoEnTarifas.source, "/produccion/crm-api");
  });
});

describe("respond() — los errores del handler (§5.3)", () => {
  /** Hace que el handler lance `e` y devuelve el `data` de la respuesta de error. */
  async function lanza(e: unknown, extra: Partial<ConnectOptions> = {}): Promise<{ ev: FluxEvent & Record<string, unknown>; data: RpcErrorData; s: Awaited<ReturnType<typeof atiende>> }> {
    const s = await atiende(() => { throw e; }, extra);
    const ev = await servir({ nc: s.nc } as Servicio, peticion());
    assert.ok(ev, "no contestó: quien llama vería un timeout");
    assert.equal(ev.type, "com.flux.rpc.error.v1");
    assert.equal(ev.dataschema, "https://schemas.internal/system/rpc/error/1.0.0.json");
    assert.equal(ev.dataclassification, "internal");
    assert.equal(ev.causationid, ID_PETICION);
    assert.equal(ev.correlationid, ID_PETICION);
    assert.equal(rpcErrorDataProblem(ev.data), null, "cumple el esquema del error");
    return { ev, data: ev.data as RpcErrorData, s };
  }

  test("RetryableError → `retryable`, con SU code, SU mensaje y `retryAfterMs`", async () => {
    const { data } = await lanza(new RetryableError("el servicio de tarifas no responde", { code: "HTTP_503", retryAfterMs: 5000 }));
    assert.deepEqual(data, { class: "retryable", code: "HTTP_503", message: "el servicio de tarifas no responde", retryAfterMs: 5000 });
    assert.deepEqual(Object.keys(data), ["class", "code", "message", "retryAfterMs"], "las claves van en ESTE orden");
  });

  test("PermanentError → `permanent`, con SU code y SU mensaje; sin code: `PERMANENT`; RetryableError sin code: `RETRYABLE`", async () => {
    assert.deepEqual((await lanza(new PermanentError("el pedido ya está cancelado", { code: "PEDIDO_YA_CANCELADO" }))).data,
      { class: "permanent", code: "PEDIDO_YA_CANCELADO", message: "el pedido ya está cancelado" });
    assert.equal((await lanza(new PermanentError("no"))).data.code, "PERMANENT");
    assert.equal((await lanza(new RetryableError("luego"))).data.code, "RETRYABLE");
  });

  test("`retryAfterMs` en un error permanent no viaja (el esquema lo prohíbe); uno inválido en un retryable tampoco", async () => {
    const a = (await lanza(new PermanentError("no", { code: "X" }))).data as unknown as Record<string, unknown>;
    assert.ok(!("retryAfterMs" in a));
    // aunque un `PermanentError` llegue con uno (el tipo no lo admite, JavaScript sí): el esquema del error lo prohíbe con `permanent`
    const a2 = (await lanza(new PermanentError("no", { code: "X", retryAfterMs: 5000 } as never))).data as unknown as Record<string, unknown>;
    assert.ok(!("retryAfterMs" in a2));
    const b = (await lanza(new RetryableError("luego", { code: "X", retryAfterMs: -5 }))).data as unknown as Record<string, unknown>;
    assert.ok(!("retryAfterMs" in b));
    const c = (await lanza(new RetryableError("luego", { code: "X", retryAfterMs: 1.5 }))).data as unknown as Record<string, unknown>;
    assert.ok(!("retryAfterMs" in c));
  });

  test("un `code` que no cumple el patrón (minúsculas, guiones) NO viaja tal cual —quien llama lo vería como RPC_INVALID_REPLY—: se sustituye y se avisa", async () => {
    const { data, s } = await lanza(new PermanentError("no", { code: "pedido-cancelado" }));
    assert.equal(data.code, "PERMANENT");
    assert.ok(s.logs.some((l) => /pedido-cancelado/.test(l) && /\^\[A-Z\]/.test(l)), "se avisa con el código y el patrón");
    assert.equal((await lanza(new RetryableError("m", { code: `A${"B".repeat(70)}` }))).data.code, "RETRYABLE");
  });

  test("un mensaje de más de 1000 caracteres se trunca (el esquema lo exige); uno vacío se sustituye", async () => {
    const { data } = await lanza(new PermanentError("x".repeat(5000), { code: "GRANDE" }));
    assert.equal(data.message.length, 1000);
    assert.equal(data.message.at(-1), "…");
    assert.ok(data.message.length >= 1);
    assert.ok((await lanza(new PermanentError("", { code: "VACIO" }))).data.message.length >= 1);
    // sin partir un par sustituto: una mitad se serializaría como `\ud83d`
    const emoji = (await lanza(new PermanentError("😀".repeat(900), { code: "EMOJI" }))).ev;
    assert.ok(!/\\ud8/i.test(JSON.stringify(emoji)));
  });

  test("PoisonError lanzado por el handler: `poison` NO viaja → `permanent`", async () => {
    const { data } = await lanza(new PoisonError("no me puedo leer", { code: "MALFORMED_JSON" }));
    assert.equal(data.class, "permanent");
    assert.equal(data.code, "MALFORMED_JSON");
    // Sin código propio, `PERMANENT` (§5.3): el código por defecto es el de la clase con la que viaja, no el de la que no viaja. Node
    // contestaba `POISON`, que Rust no.
    assert.deepEqual((await lanza(new PoisonError("no me puedo leer"))).data, { class: "permanent", code: "PERMANENT", message: "no me puedo leer" });
  });

  test("un mensaje solo de espacios es como uno vacío: se sustituye (§5.3); los espacios Unicode cuentan, U+FEFF no", async () => {
    const porDefecto = "el servicio rechazó la petición";
    for (const m of ["", " ", "  \t\n", "\u00a0\u2003\u3000"]) {
      assert.equal((await lanza(new PermanentError(m, { code: "VACIO" }))).data.message, porDefecto, JSON.stringify(m));
    }
    assert.equal((await lanza(new PermanentError(" a ", { code: "X" }))).data.message, " a ");
  });

  test("`message` se acota en PUNTOS DE CÓDIGO (como `maxLength` de JSON Schema), no en unidades UTF-16: 1000 emojis caben, 1001 se truncan a 999 + «…»", async () => {
    const caben = (await lanza(new PermanentError("😀".repeat(1000), { code: "X" }))).data.message;
    assert.equal([...caben].length, 1000);
    assert.equal(caben, "😀".repeat(1000), "no se recorta nada");
    const largo = (await lanza(new PermanentError("😀".repeat(1001), { code: "X" }))).data.message;
    assert.equal([...largo].length, 1000);
    assert.equal(largo, `${"😀".repeat(999)}…`);
  });

  test("un error que el CLASIFICADOR reconoce (un 503, ECONNRESET) → su clase y su código, con mensaje GENÉRICO: el texto de la excepción no viaja", async () => {
    const secreto = "postgres://admin:hunter2@db.interno:5432/clientes";
    const { data, ev, s } = await lanza(Object.assign(new Error(`falló ${secreto}`), { status: 503, response: { headers: { "retry-after": "7" } } }));
    assert.equal(data.class, "retryable");
    assert.equal(data.code, "HTTP_503");
    assert.equal(data.message, "dependencia no disponible");
    assert.equal(data.retryAfterMs, 7000, "el Retry-After de la dependencia acorta el primer reintento");
    assert.ok(!JSON.stringify(ev).includes("hunter2"));
    assert.ok(s.logs.some((l) => l.includes(ID_PETICION)), "el log lleva el id de la petición para encontrarlo");

    const reset = (await lanza(Object.assign(new Error(`read ECONNRESET ${secreto}`), { code: "ECONNRESET" }))).data;
    assert.deepEqual([reset.class, reset.code, reset.message], ["retryable", "ECONNRESET", "dependencia no disponible"]);

    const noEncontrado = (await lanza(Object.assign(new Error("x"), { status: 404 }))).data;
    assert.deepEqual([noEncontrado.class, noEncontrado.code], ["permanent", "HTTP_404"]);
    assert.ok(!("retryAfterMs" in noEncontrado));
  });

  test("un error DESCONOCIDO → `permanent` UNKNOWN con un mensaje genérico; su texto va al LOG con el id de la petición, nunca a la respuesta", async () => {
    const secreto = "ana@example.com";
    const { data, ev, s } = await lanza(new Error(`no existe el cliente ${secreto}`));
    assert.deepEqual(data, { class: "permanent", code: "UNKNOWN", message: "error interno del servicio" });
    assert.ok(!JSON.stringify(ev).includes(secreto));
    const log = s.logs.find((l) => l.includes(ID_PETICION));
    assert.ok(log, "el log lleva el id de la petición");
    assert.ok(log.includes(secreto), "y el texto de la excepción: ahí es donde se investiga");
    // algo que ni siquiera es un Error
    assert.equal((await lanza("una cadena lanzada")).data.code, "UNKNOWN");
    assert.equal((await lanza({ x: 1 })).data.code, "UNKNOWN");
  });

  test("lo desconocido es `permanent` con la política por defecto Y con `permanent`; con `retryable` es `retryable` (§5.3)", async () => {
    assert.equal((await lanza(new Error("raro"), { classifier: { unknownErrorPolicy: "retryable-bounded" } })).data.class, "permanent");
    assert.equal((await lanza(new Error("raro"), { classifier: { unknownErrorPolicy: "permanent" } })).data.class, "permanent");
    const r = (await lanza(new Error("raro"), { classifier: { unknownErrorPolicy: "retryable" } })).data;
    assert.deepEqual([r.class, r.code], ["retryable", "UNKNOWN"]);
  });

  test("una regla de la aplicación SÍ reconoce un error: su clase y su código, mensaje genérico", async () => {
    const { data } = await lanza(new Error("reglas"), { classifier: { rules: [(e) => (e instanceof Error && e.message === "reglas" ? { class: ErrorClass.RETRYABLE, code: "REGLA_PROPIA" } : undefined)] } });
    assert.deepEqual([data.class, data.code, data.message], ["retryable", "REGLA_PROPIA", "dependencia no disponible"]);
  });

  test("en una petición `confidential` el texto de la excepción NO se escribe ni en el log", async () => {
    const s = await atiende(() => { throw new Error("el email de ana@example.com no existe"); });
    await servir({ nc: s.nc } as Servicio, peticion({ classification: "confidential" }));
    assert.ok(s.logs.length > 0);
    assert.ok(!s.logs.some((l) => l.includes("ana@example.com")), "confidential y restricted no se escriben en logs (06-security.md §5)");
  });

  test("devolver algo que no es un objeto → `permanent` INVALID_SCHEMA «la respuesta no es un objeto»", async () => {
    for (const malo of [null, undefined, "texto", 42, [1, 2], true]) {
      const s = await atiende(() => malo as never);
      const ev = await servir({ nc: s.nc } as Servicio, peticion());
      assert.ok(ev, String(malo));
      const d = ev.data as RpcErrorData;
      assert.deepEqual([d.class, d.code], ["permanent", "INVALID_SCHEMA"]);
      assert.match(d.message, /la respuesta no es un objeto/);
    }
  });
});

describe("respond() — lo que no cumple (§5.2): SIEMPRE se contesta con un error, nunca en silencio", () => {
  test("JSON roto → error `permanent` MALFORMED_JSON; el handler NO se invoca; `correlationid` propio, `tenantid: system`, y SIN causationid (no hay id legible)", async () => {
    let llamado = false;
    const s = await atiende(() => ((llamado = true), {}));
    const ev = await servir({ nc: s.nc } as Servicio, "esto no es json");
    assert.ok(ev);
    assert.equal(llamado, false);
    assert.equal(ev.type, "com.flux.rpc.error.v1");
    const d = ev.data as RpcErrorData;
    assert.deepEqual([d.class, d.code], ["permanent", "MALFORMED_JSON"]);
    assert.match(d.message, /01-envelope\.md/);
    assert.equal(ev.correlationid, ev.id, "sin sobre no hay correlationid que heredar: el propio id");
    assert.equal(ev.tenantid, "system");
    assert.ok(!("causationid" in ev));
    assert.equal(s.metricas.snapshot().counters.get(`flux_rpc_server_requests_total{operation="${OP}",outcome="poison"}`), 1);
    assert.equal(s.poison.length, 1, "POISON es lo que despierta a alguien: onPoison se invoca");
    assert.equal(s.poison[0].subject, SUBJECT);
  });

  test("un sobre ilegible con un `id` que SÍ es una cadena válida: el error lo cita como causationid; con un `id` que no lo es (número, vacío, enorme), no", async () => {
    const s = await atiende();
    const conId = await servir({ nc: s.nc } as Servicio, JSON.stringify({ id: "abc-123", specversion: "1.0" }), "_INBOX.llama.a");
    assert.equal(conId?.causationid, "abc-123");
    assert.equal((conId?.data as RpcErrorData).code, "MISSING_REQUIRED_ATTRIBUTE");
    // Una cadena válida: no vacía, de 128 caracteres (puntos de código) como máximo y sin caracteres de control. Los DOS SDKs aplican
    // la misma regla (Node aceptaba hasta 256 y controles; Rust, hasta 128 y sin controles).
    for (const [n, id] of [["g", "x".repeat(128)], ["h", "😀".repeat(128)]] as const) {
      const ev = await servir({ nc: s.nc } as Servicio, JSON.stringify({ id, specversion: "1.0" }), `_INBOX.llama.${n}`);
      assert.equal(ev?.causationid, id, `el id de ${n} sí es válido`);
    }
    for (const [n, id] of [["b", 42], ["c", ""], ["d", "x".repeat(300)], ["e", null], ["f", { a: 1 }], ["i", "x".repeat(129)], ["j", "😀".repeat(129)], ["k", "a\u0001b"], ["l", "a\u007fb"], ["m", "a\nb"]] as const) {
      const ev = await servir({ nc: s.nc } as Servicio, JSON.stringify({ id, specversion: "1.0" }), `_INBOX.llama.${n}`);
      assert.ok(ev, `id ${JSON.stringify(id).slice(0, 10)}`);
      assert.ok(!("causationid" in ev), `el id ${JSON.stringify(id).slice(0, 10)} no es una cadena válida`);
    }
  });

  test("cada código del sobre de 01-envelope.md §3.1 sale en el error: falta una extensión, un tipo equivocado, una specversion desconocida", async () => {
    const buena = JSON.parse(texto(peticion()));
    const casos: Array<[string, Record<string, unknown>]> = [
      ["MISSING_REQUIRED_EXTENSION", { ...buena, tenantid: undefined }],
      ["WRONG_ATTRIBUTE_TYPE", { ...buena, tenantid: 42 }],
      ["UNSUPPORTED_SPECVERSION", { ...buena, specversion: "0.3" }],
      ["UNKNOWN_ROOT_ATTRIBUTE", { ...buena, extra: 1 }],
      ["INVALID_DATACLASSIFICATION", { ...buena, dataclassification: "secreta" }],
    ];
    for (const [i, [codigo, cuerpo]] of casos.entries()) {
      const s = await atiende();
      const ev = await servir({ nc: s.nc } as Servicio, JSON.stringify(cuerpo), `_INBOX.llama.${i}`);
      assert.equal((ev?.data as RpcErrorData | undefined)?.code, codigo);
      assert.equal(s.metricas.snapshot().counters.get(`flux_rpc_server_requests_total{operation="${OP}",outcome="poison"}`), 1);
    }
  });

  test("una petición SIN subject de respuesta no se atiende (nadie espera el resultado de una orden): no se invoca al handler, no se publica nada, `poison`, log", async () => {
    let llamado = false;
    const s = await atiende(() => ((llamado = true), {}));
    s.nc.entrega(SUBJECT, peticion(), ""); // `nats pub` a mano: sin reply
    await sleep(40);
    assert.equal(llamado, false, "ejecutar a ciegas aplicaría una orden que nadie espera");
    assert.equal(s.nc.publicadas.length, 0);
    assert.equal(s.metricas.snapshot().counters.get(`flux_rpc_server_requests_total{operation="${OP}",outcome="poison"}`), 1);
    assert.ok(s.logs.some((l) => /SIN subject de respuesta/.test(l)));
    assert.equal(s.poison.length, 1);
  });

  test("un `type` que no es el de la petición de ESTA operación → `permanent` INVALID_RPC_CALL, handler no invocado, `poison`", async () => {
    let llamado = false;
    const s = await atiende(() => ((llamado = true), {}));
    for (const [i, tipo] of [
      "com.flux.rpc.clientes.cliente.obtener.request.v1", // la petición de OTRA operación
      "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1", // una respuesta
      "com.flux.rpc.clientes.cliente.calcular-descuento.request.v2",
      "com.flux.clientes.cliente.creado.v1",
    ].entries()) {
      const ev = await servir({ nc: s.nc } as Servicio, JSON.stringify({ ...JSON.parse(texto(peticion())), type: tipo }), `_INBOX.llama.t${i}`);
      assert.ok(ev, tipo);
      const d = ev.data as RpcErrorData;
      assert.deepEqual([d.class, d.code], ["permanent", "INVALID_RPC_CALL"]);
      assert.equal(ev.correlationid, ID_PETICION, "la petición se pudo leer: SÍ hay correlationid que heredar");
      assert.equal(ev.causationid, ID_PETICION);
    }
    assert.equal(llamado, false);
    assert.equal(s.metricas.snapshot().counters.get(`flux_rpc_server_requests_total{operation="${OP}",outcome="poison"}`), 4);
  });

  test("CONTROL: la petición buena SÍ llega al handler (así que los casos de arriba fallan por lo que se altera)", async () => {
    let llamado = false;
    const s = await atiende(() => ((llamado = true), { descuentoCents: 1, moneda: "EUR" }));
    const ev = await servir({ nc: s.nc } as Servicio, peticion());
    assert.equal(llamado, true);
    assert.equal(ev?.type, "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1");
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 6. L3: el esquema, resuelto en el índice `rpc` del bundle, sin red
// ═════════════════════════════════════════════════════════════════════════════

describe("L3 — la operación se resuelve en el índice `rpc` del bundle (sin red) y los esquemas se aplican en los DOS lados", () => {
  const L3: Partial<ConnectOptions> = { validation: { mode: "strict", bundle }, schemaBaseUrl: undefined };
  const EMAIL = "ana@example.com";

  test("el `dataschema` de la petición y de la respuesta salen del bundle (`1.0.0` ahí), sin `schemaBaseUrl`", async () => {
    const red = new RedEnMemoria();
    const crm = await servicio(red, { ...CRM, ...L3 });
    await crm.bus.respond(OP, () => ({ descuentoCents: 500, moneda: "EUR" }));
    const llama = await servicio(red, L3);
    await llama.bus.request(OP, PETICION);
    assert.equal(evento(llama.nc.enviadas[0].bytes).dataschema, bundle.rpc[OP].request);
    assert.equal(evento(crm.nc.publicadas[0].bytes).dataschema, bundle.rpc[OP].reply);
    // con un bundle cuyo índice dice otra versión, se usa esa
    const otro = { ...bundle, rpc: { [OP]: { request: "https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.3.0.json", reply: bundle.rpc[OP].reply } } };
    const b = await servicio(new RedEnMemoria(), { validation: { mode: "warn", bundle: otro }, schemaBaseUrl: undefined });
    b.nc.falloDeLaPeticion = () => new errors.ClosedConnectionError();
    await assert.rejects(b.bus.request(OP, PETICION), errors.ClosedConnectionError);
    assert.match(evento(b.nc.enviadas[0].bytes).dataschema, /request\/1\.3\.0\.json$/, "el MINOR real lo da el bundle");
  });

  test("quien llama valida la petición ANTES de la red: INVALID_SCHEMA con TODAS las rutas y NINGÚN valor, y dice que el fallo es de la petición", async () => {
    const red = new RedEnMemoria();
    const { bus, nc, metricas } = await servicio(red, L3);
    await assert.rejects(bus.request(OP, { clienteId: EMAIL, totalCents: 99.5 }), (e: RpcSchemaError) => {
      assert.ok(e instanceof RpcSchemaError);
      assert.equal(e.code, "INVALID_SCHEMA");
      assert.equal(e.fluxClass, ErrorClass.PERMANENT);
      assert.match(e.message, /^la petición de "clientes\.cliente\.v1\.calcular-descuento" no cumple/);
      assert.match(e.message, /\/clienteId/);
      assert.match(e.message, /\/totalCents/);
      assert.match(e.message, /moneda/);
      assert.ok(!e.message.includes(EMAIL), "nunca el valor");
      return true;
    });
    assert.equal(nc.enviadas.length, 0, "ni se abrió la petición");
    assert.equal(metricas.snapshot().counters.get(`flux_rpc_client_requests_total{operation="${OP}",outcome="invalid_schema"}`), 1);
  });

  test("quien responde valida la petición: error `permanent` INVALID_SCHEMA con TODAS las rutas, el handler NO se invoca, `invalid_schema`", async () => {
    let llamado = false;
    const s = await atiende(() => ((llamado = true), { descuentoCents: 1, moneda: "EUR" }), L3);
    const ev = await servir({ nc: s.nc } as Servicio, peticion({ data: { clienteId: EMAIL, totalCents: 99.5 } }));
    assert.ok(ev);
    assert.equal(llamado, false);
    const d = ev.data as RpcErrorData;
    assert.deepEqual([d.class, d.code], ["permanent", "INVALID_SCHEMA"]);
    assert.match(d.message, /^la petición no cumple https:\/\/schemas\.internal\/rpc\/clientes\/cliente\/calcular-descuento\/request\/1\.0\.0\.json: /);
    assert.match(d.message, /\/clienteId/);
    assert.match(d.message, /\/totalCents/);
    assert.ok(!d.message.includes(EMAIL), "cita rutas, nunca valores");
    assert.equal(s.metricas.snapshot().counters.get(`flux_rpc_server_requests_total{operation="${OP}",outcome="invalid_schema"}`), 1);
  });

  test("quien responde valida SU respuesta: el fallo es suyo, y el mensaje dice «la respuesta no cumple» (no «la petición»)", async () => {
    const s = await atiende(() => ({ descuentoCents: -5, moneda: "euro" }) as never, L3);
    const ev = await servir({ nc: s.nc } as Servicio, peticion());
    assert.ok(ev);
    const d = ev.data as RpcErrorData;
    assert.deepEqual([d.class, d.code], ["permanent", "INVALID_SCHEMA"]);
    assert.match(d.message, /^la respuesta no cumple /);
    assert.match(d.message, /del que responde|de quien responde/);
    assert.equal(s.metricas.snapshot().counters.get(`flux_rpc_server_requests_total{operation="${OP}",outcome="invalid_schema"}`), 1);
  });

  test("quien llama valida la respuesta: INVALID_SCHEMA «la respuesta de … no cumple», y dice que el fallo es de quien responde", async () => {
    const red = new RedEnMemoria();
    // un responder SIN L3 (o con otro bundle) que contesta algo que no cumple el esquema de la respuesta
    const crm = await servicio(red, CRM);
    await crm.bus.respond(OP, () => ({ descuentoCents: -5, moneda: "euro" }) as never);
    const { bus, metricas } = await servicio(red, L3);
    await assert.rejects(bus.request(OP, PETICION), (e: RpcSchemaError) => {
      assert.ok(e instanceof RpcSchemaError);
      assert.match(e.message, /^la respuesta de "clientes\.cliente\.v1\.calcular-descuento" no cumple/);
      assert.match(e.message, /de quien responde/);
      return true;
    });
    assert.equal(metricas.snapshot().counters.get(`flux_rpc_client_requests_total{operation="${OP}",outcome="invalid_schema"}`), 1);
  });

  test("SDK ↔ SDK con L3 a los dos lados: lo válido pasa; el error de esquema del que responde llega como PermanentError INVALID_SCHEMA", async () => {
    const red = new RedEnMemoria();
    const crm = await servicio(red, { ...CRM, ...L3 });
    await crm.bus.respond(OP, () => ({ descuentoCents: 500, moneda: "EUR", motivo: "Campaña de otoño" }));
    const { bus } = await servicio(red, { validation: { mode: "off", bundle }, schemaBaseUrl: undefined });
    assert.deepEqual(await bus.request(OP, { ...PETICION, campana: "Otoño" }), { descuentoCents: 500, moneda: "EUR", motivo: "Campaña de otoño" });
    // quien llama SIN L3 envía un dato malo: lo rechaza quien responde y llega como error permanent con su código
    await assert.rejects(bus.request(OP, { clienteId: EMAIL }), (e: RpcPermanentError) => {
      assert.ok(e instanceof PermanentError);
      assert.equal(e.code, "INVALID_SCHEMA");
      assert.match(e.message, /la petición no cumple/);
      assert.ok(!e.message.includes(EMAIL));
      return true;
    });
  });

  test("modo estricto: una operación que NO está en el bundle es SCHEMA_NOT_FOUND al llamar y AL ARRANCAR `respond()` (no en cada petición)", async () => {
    const { bus } = await servicio(new RedEnMemoria(), L3);
    await assert.rejects(bus.request("clientes.cliente.v1.no-existe", {}), (e: SchemaNotFoundError) => e instanceof SchemaNotFoundError && e.code === "SCHEMA_NOT_FOUND");
    const s = await servicio(new RedEnMemoria(), { ...CRM, ...L3 });
    await assert.rejects(s.bus.respond("clientes.cliente.v1.no-existe", () => ({})), (e: SchemaNotFoundError) => e instanceof SchemaNotFoundError);
    assert.equal(s.nc.argumentosDeSubscribe.length, 0, "ni se suscribió");
  });

  test("modo `warn`: el fallo se registra y se CUENTA, y la petición SE ENVÍA / el handler SE EJECUTA", async () => {
    const red = new RedEnMemoria();
    let ejecutado = false;
    const crm = await servicio(red, { ...CRM, validation: { mode: "warn", bundle }, schemaBaseUrl: undefined });
    await crm.bus.respond(OP, () => ((ejecutado = true), { descuentoCents: 1, moneda: "EUR" }));
    const avisos: string[] = [];
    const original = console.warn;
    console.warn = (m: string) => void avisos.push(String(m));
    try {
      const ev = await servir(crm, peticion({ data: { clienteId: EMAIL, totalCents: 99.5 } }));
      assert.ok(ev);
      assert.equal(ev.type, "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1", "contestó con una respuesta, no con un error");
      assert.equal(ejecutado, true);
      const llama = await servicio(red, { validation: { mode: "warn", bundle }, schemaBaseUrl: undefined });
      await llama.bus.request(OP, { clienteId: EMAIL, totalCents: 99.5 });
      assert.equal(llama.nc.enviadas.length, 1, "warn no detiene la petición");
      assert.equal(llama.metricas.snapshot().counters.get(`flux_rpc_client_requests_total{operation="${OP}",outcome="invalid_schema"}`), 1);
    } finally {
      console.warn = original;
    }
    assert.ok(avisos.length >= 2 && avisos.every((a) => !a.includes(EMAIL)));
    // dos peticiones llegaron al responder con L3 en `warn` (la entregada a mano y la de `llama`): cada fallo se cuenta
    assert.equal(crm.metricas.snapshot().counters.get(`flux_rpc_server_requests_total{operation="${OP}",outcome="invalid_schema"}`), 2);
  });

  test("con L3 apagado no se valida nada", async () => {
    const s = await atiende(() => ({ descuentoCents: -5 }) as never);
    const ev = await servir({ nc: s.nc } as Servicio, peticion({ data: { cualquier: "cosa" } }));
    assert.equal(ev?.type, "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1");
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 7. La firma y la ventana de 300 s (11-request-reply.md §7.4)
// ═════════════════════════════════════════════════════════════════════════════

describe("firma — la petición y la respuesta van firmadas; con `require`, la ventana de 300 s es simétrica", () => {
  const llamante = generateKeyPair();
  const dueno = generateKeyPair();
  const otra = generateKeyPair();
  const publicKeys = { "facturacion-api-1": llamante.publicKeyPem, "crm-api-1": dueno.publicKeyPem };
  const firmaDe = (b: typeof llamante, keyId: string) => ({ privateKeyPem: b.privateKeyPem, keyId });

  const requireCrm = (verify: "require" | "warn" | "off" = "require"): Partial<ConnectOptions> => ({ ...CRM, signing: { ...firmaDe(dueno, "crm-api-1"), publicKeys, verify } });
  const requireFact: Partial<ConnectOptions> = { signing: { ...firmaDe(llamante, "facturacion-api-1"), publicKeys, verify: "require" } };

  /** Una petición firmada por `quien` con `time` desplazado `desplazaMs` respecto de ahora. */
  const firmada = (quien: { privateKeyPem: string }, keyId: string, desplazaMs = 0, extra: Record<string, unknown> = {}) =>
    encodeRpcMessage(
      createSigner({ privateKeyPem: quien.privateKeyPem, keyId })!.sign(
        buildRpcRequest({
          operation: OP, tenantId: "acme", aggregateId: "cli-1", id: ID_PETICION, source: "/produccion/facturacion-api",
          time: new Date(Date.now() + desplazaMs).toISOString(), correlationid: ID_PETICION, producerversion: "3.4.1",
          classification: "internal", data: PETICION, dataschema: rpcDataschema(OP, "request"), ...extra,
        }),
      ),
    );

  test("SDK ↔ SDK con `require` a los dos lados: la petición sale firmada, la respuesta vuelve firmada, y las dos verifican", async () => {
    const red = new RedEnMemoria();
    const crm = await servicio(red, requireCrm());
    await crm.bus.respond(OP, () => ({ descuentoCents: 500, moneda: "EUR" }));
    const llama = await servicio(red, requireFact);
    assert.deepEqual(await llama.bus.request(OP, PETICION), { descuentoCents: 500, moneda: "EUR" });
    const pet = evento(llama.nc.enviadas[0].bytes);
    assert.equal(pet.signkeyid, "facturacion-api-1");
    assert.equal(typeof pet.signature, "string");
    const rep = evento(crm.nc.publicadas[0].bytes);
    assert.equal(rep.signkeyid, "crm-api-1");
    assert.equal(typeof rep.signature, "string");
    for (const e of [pet, rep]) {
      const claves = Object.keys(e);
      assert.equal(claves.at(-1), "data", "`data` sigue siendo el último con la firma puesta");
      assert.ok(claves.indexOf("signature") < claves.indexOf("data"));
    }
  });

  test("el que responde rechaza una petición SIN firma, con firma de datos manipulados o con una clave desconocida: error `permanent`, handler NO invocado, `invalid_signature`", async () => {
    let llamado = 0;
    const s = await atiende(() => (llamado++, { descuentoCents: 1, moneda: "EUR" }), requireCrm());
    const sinFirma = evento(peticion());
    const manipulada = evento(firmada(llamante, "facturacion-api-1"));
    (manipulada.data as Record<string, unknown>).totalCents = 1;
    const claveDesconocida = firmada(otra, "intruso-1");
    for (const [i, [codigo, bytes]] of ([
      ["MISSING_SIGNATURE", JSON.stringify(sinFirma)],
      ["INVALID_SIGNATURE", JSON.stringify(manipulada)],
      ["UNKNOWN_SIGNING_KEY", claveDesconocida],
    ] as const).entries()) {
      const ev = await servir({ nc: s.nc } as Servicio, bytes, `_INBOX.llama.f${i}`);
      assert.ok(ev, codigo);
      const d = ev.data as RpcErrorData;
      assert.deepEqual([d.class, d.code], ["permanent", codigo]);
      assert.equal(ev.signkeyid, "crm-api-1", "el error también va firmado por el que responde");
    }
    assert.equal(llamado, 0);
    assert.equal(s.metricas.snapshot().counters.get(`flux_rpc_server_requests_total{operation="${OP}",outcome="invalid_signature"}`), 3);
    // CONTROL: la buena, firmada con una clave conocida, pasa
    const ev = await servir({ nc: s.nc } as Servicio, firmada(llamante, "facturacion-api-1"), "_INBOX.llama.ok");
    assert.equal(ev?.type, "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1");
    assert.equal(llamado, 1);
  });

  test("STALE_REQUEST: una petición firmada con `time` a MÁS de 300 s —por detrás Y por delante— se rechaza; dentro de la ventana pasa", async () => {
    let llamado = 0;
    const s = await atiende(() => (llamado++, { descuentoCents: 1, moneda: "EUR" }), requireCrm());
    const casos: Array<[string, number, boolean]> = [
      ["400 s por detrás", -400_000, true],
      ["300,5 s por detrás", -300_500, true],
      ["299,5 s por detrás", -299_500, false],
      ["un instante atrás", -50, false],
      ["299,5 s por delante", 299_500, false],
      ["300,5 s por delante (un reloj adelantado es tan sospechoso como uno atrasado)", 300_500, true],
      ["un año por delante", 365 * 86_400_000, true],
    ];
    for (const [i, [nombre, desplaza, rechaza]] of casos.entries()) {
      const ev = await servir({ nc: s.nc } as Servicio, firmada(llamante, "facturacion-api-1", desplaza), `_INBOX.llama.s${i}`);
      assert.ok(ev, nombre);
      if (rechaza) {
        const d = ev.data as RpcErrorData;
        assert.deepEqual([d.class, d.code], ["permanent", "STALE_REQUEST"], nombre);
        assert.match(d.message, /300 s/);
        assert.match(d.message, /\d{4}-\d{2}-\d{2}T[\d:.]+Z/, "dice el `time` y la hora local");
        assert.equal(ev.signkeyid, "crm-api-1", "el error va firmado");
      } else {
        assert.equal(ev.type, "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1", nombre);
      }
    }
    assert.equal(llamado, casos.filter((c) => !c[2]).length, "el handler solo se invocó para lo que cabía en la ventana");
    assert.equal(s.metricas.snapshot().counters.get(`flux_rpc_server_requests_total{operation="${OP}",outcome="stale_request"}`), casos.filter((c) => c[2]).length);
  });

  test("la ventana solo se aplica con `require`: en `warn` y en `off` una petición vieja NO se rechaza", async () => {
    for (const modo of ["warn", "off"] as const) {
      const s = await atiende(() => ({ descuentoCents: 1, moneda: "EUR" }), requireCrm(modo));
      const avisos: string[] = [];
      const original = console.warn;
      console.warn = (m: string) => void avisos.push(String(m));
      try {
        const ev = await servir({ nc: s.nc } as Servicio, firmada(llamante, "facturacion-api-1", -3_600_000));
        assert.equal(ev?.type, "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1", modo);
      } finally {
        console.warn = original;
      }
    }
    // y sin ninguna firma configurada tampoco: `time` es un campo que cualquiera escribe
    const s = await atiende();
    assert.equal((await servir({ nc: s.nc } as Servicio, peticion({ time: "2020-01-01T00:00:00.000Z" })))?.type, "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1");
  });

  test("el orden de §5.1: la FIRMA se comprueba antes que la ventana (una petición vieja y sin firma es MISSING_SIGNATURE, no STALE_REQUEST)", async () => {
    const s = await atiende(undefined, requireCrm());
    const ev = await servir({ nc: s.nc } as Servicio, peticion({ time: "2020-01-01T00:00:00.000Z" }));
    assert.equal((ev?.data as RpcErrorData).code, "MISSING_SIGNATURE");
  });

  test("un `time` ilegible se trata como fuera de ventana con `require` (NaN no es «dentro»)", async () => {
    const s = await atiende(undefined, requireCrm());
    // Un `time` que ni siquiera es un instante no sale de un SDK (el constructor lo rechaza): lo escribe quien firma a mano.
    const base = buildRpcRequest({
      operation: OP, tenantId: "acme", aggregateId: "cli-1", id: ID_PETICION, source: "/produccion/facturacion-api",
      time: new Date().toISOString(), correlationid: ID_PETICION, producerversion: "3.4.1",
      classification: "internal", data: PETICION, dataschema: rpcDataschema(OP, "request"),
    });
    const ilegible = encodeRpcMessage(createSigner({ privateKeyPem: llamante.privateKeyPem, keyId: "facturacion-api-1" })!.sign({ ...base, time: "ayer por la tarde" } as FluxEvent));
    const ev = await servir({ nc: s.nc } as Servicio, ilegible);
    assert.equal((ev?.data as RpcErrorData).code, "STALE_REQUEST");
  });

  test("quien llama con `require` verifica la respuesta Y los errores: sin firma, con la firma rota o de una clave desconocida → PermanentError con el código de la firma", async () => {
    const respuestaDe = (req: FluxEvent & Record<string, unknown>, firmante?: { privateKeyPem: string; keyId: string }, altera?: (e: FluxEvent) => FluxEvent) => {
      let e: FluxEvent = respuestaBuena(req);
      if (firmante) e = createSigner(firmante)!.sign(e);
      return encodeRpcMessage(altera ? altera(e) : e);
    };
    const manipular = (e: FluxEvent): FluxEvent => ({ ...e, data: { descuentoCents: 1, moneda: "EUR" } });
    const casos: Array<[string, (req: FluxEvent & Record<string, unknown>) => Uint8Array]> = [
      ["MISSING_SIGNATURE", (req) => respuestaDe(req)],
      ["INVALID_SIGNATURE", (req) => respuestaDe(req, firmaDe(dueno, "crm-api-1"), manipular)],
      ["UNKNOWN_SIGNING_KEY", (req) => respuestaDe(req, firmaDe(otra, "intruso-1"))],
    ];
    for (const [codigo, construye] of casos) {
      const red = new RedEnMemoria();
      responderCon(red, (req) => construye(req));
      const { bus, metricas } = await servicio(red, requireFact);
      await assert.rejects(bus.request(OP, PETICION), (e: RpcPermanentError) => {
        assert.ok(e instanceof PermanentError);
        assert.equal(e.code, codigo);
        assert.equal(e.fluxClass, ErrorClass.PERMANENT);
        return true;
      });
      assert.equal(metricas.snapshot().counters.get(`flux_rpc_client_requests_total{operation="${OP}",outcome="permanent"}`), 1, "un fallo de firma cuenta como permanent");
    }
    // un ERROR sin firma tampoco pasa (llegaría como el error que dice ser, sin que nadie lo avale)
    const red = new RedEnMemoria();
    responderCon(red, (req) => encodeRpcMessage(buildRpcError({ request: { operation: OP, id: req.id, correlationid: req.correlationid, tenantid: req.tenantid },
      id: "01924f91-5d11-7a62-8f0d-4e5f6a7b8c91", source: "/x/y", time: "2026-08-20T10:41:07.331Z", producerversion: "2.1.0", error: { class: "retryable", code: "X", message: "m" } })));
    const { bus } = await servicio(red, requireFact);
    await assert.rejects(bus.request(OP, PETICION), (e: RpcPermanentError) => e.code === "MISSING_SIGNATURE");
    // CONTROL: la respuesta bien firmada pasa
    const ok = new RedEnMemoria();
    responderCon(ok, (req) => respuestaDe(req, firmaDe(dueno, "crm-api-1")));
    assert.deepEqual(await (await servicio(ok, requireFact)).bus.request(OP, PETICION), { descuentoCents: 500, moneda: "EUR" });
  });

  test("las comprobaciones de la respuesta van ANTES que la firma: una respuesta que no es de esta petición es RPC_INVALID_REPLY aunque no esté firmada", async () => {
    const red = new RedEnMemoria();
    responderCon(red, () => '{"stream":"TRAMPA","seq":1}');
    const { bus } = await servicio(red, requireFact);
    await assert.rejects(bus.request(OP, PETICION), RpcInvalidReplyError);
  });

  test("en `warn` quien llama sigue con una respuesta sin firma (y se avisa); la petición SÍ sale firmada si hay clave", async () => {
    const red = new RedEnMemoria();
    responderCon(red, (req) => encodeRpcMessage(respuestaBuena(req)));
    const { bus, nc } = await servicio(red, { signing: { ...firmaDe(llamante, "facturacion-api-1"), publicKeys, verify: "warn" } });
    const original = console.warn;
    const avisos: string[] = [];
    console.warn = (m: string) => void avisos.push(String(m));
    try {
      assert.deepEqual(await bus.request(OP, PETICION), { descuentoCents: 500, moneda: "EUR" });
    } finally {
      console.warn = original;
    }
    assert.ok(avisos.some((a) => /sin firma/.test(a)));
    assert.equal(evento(nc.enviadas[0].bytes).signkeyid, "facturacion-api-1");
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 8. El tamaño de la respuesta (§5.4), la concurrencia (§5.6) y la parada ordenada (§5.5)
// ═════════════════════════════════════════════════════════════════════════════

describe("respond() — el tamaño de la respuesta se comprueba ANTES de publicar (§5.4): si no cabe, se contesta con RPC_MESSAGE_TOO_LARGE", () => {
  /** Una respuesta cuyo mensaje serializado ocupa EXACTAMENTE `bytes`, dado el sobre de `peticion()`. */
  const conTamano = (bytes: number) => {
    const vacio = encodeRpcMessage(buildRpcReply({
      request: { operation: OP, id: ID_PETICION, correlationid: ID_PETICION, tenantid: "acme", subject: "cli-1" },
      id: "0".repeat(36), source: "/produccion/crm-api", time: "2026-08-20T10:41:07.329Z", producerversion: "2.1.0", classification: "internal",
      traceparent: TRACEPARENT, data: { relleno: "" }, dataschema: rpcDataschema(OP, "reply"),
    })).byteLength;
    return { relleno: "x".repeat(bytes - vacio) };
  };

  test("EXACTAMENTE `max_payload` pasa; un byte más se contesta con un error PERMANENT que dice los bytes y el límite", async () => {
    const s = await atiende(() => conTamano(4096) as never);
    s.nc.info = { max_payload: 4096 };
    const ok = await servir({ nc: s.nc } as Servicio, peticion(), "_INBOX.llama.ok");
    assert.equal(ok?.type, "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1");
    assert.equal(s.nc.publicadas[0].bytes.byteLength, 4096);

    const grande = await atiende(() => conTamano(4097) as never);
    grande.nc.info = { max_payload: 4096 };
    const ev = await servir({ nc: grande.nc } as Servicio, peticion(), "_INBOX.llama.grande");
    assert.ok(ev, "si la respuesta que no cabe se publicara, la librería lanzaría EN QUIEN RESPONDE y quien llama solo vería un timeout (A-65)");
    const d = ev.data as RpcErrorData;
    assert.deepEqual([d.class, d.code], ["permanent", "RPC_MESSAGE_TOO_LARGE"]);
    assert.match(d.message, /4097 bytes/);
    assert.match(d.message, /4096/);
    assert.match(d.message, /claim-check/);
    assert.ok(grande.nc.publicadas.every((p) => p.bytes.byteLength <= 4096), "NADA que supere el límite llegó a publicarse");
    assert.equal(grande.metricas.snapshot().counters.get(`flux_rpc_server_requests_total{operation="${OP}",outcome="permanent"}`), 1);
  });

  test("con el límite de 1 MiB (el default) también; y quien llama lo ve como RPC_MESSAGE_TOO_LARGE, NO como un timeout", async () => {
    const red = new RedEnMemoria();
    const crm = await servicio(red, CRM);
    await crm.bus.respond(OP, () => ({ relleno: "x".repeat(1_100_000) }) as never);
    const { bus } = await servicio(red);
    await assert.rejects(bus.request(OP, PETICION, { timeoutMs: 5000 }), (e: RpcPermanentError) => {
      assert.ok(e instanceof PermanentError);
      assert.equal(e.code, "RPC_MESSAGE_TOO_LARGE");
      assert.match(e.message, /bytes/);
      return true;
    });
    assert.ok(crm.nc.publicadas.every((p) => p.bytes.byteLength <= 1_048_576));
  });
});

describe("respond() — límite de handlers concurrentes (§5.6): el exceso espera en la cola de la suscripción", () => {
  /** Un handler que se queda esperando a `abre()` y cuenta cuántos hay dentro a la vez. */
  function conPuerta() {
    let dentro = 0;
    let maximo = 0;
    let terminados = 0;
    let abre!: () => void;
    const puerta = new Promise<void>((r) => (abre = r));
    return {
      handler: async () => {
        dentro++;
        maximo = Math.max(maximo, dentro);
        await puerta;
        dentro--;
        terminados++;
        return { descuentoCents: 1, moneda: "EUR" };
      },
      abre,
      get dentro() { return dentro; },
      get maximo() { return maximo; },
      get terminados() { return terminados; },
    };
  }

  test("`maxConcurrent: 3` con 10 peticiones: nunca hay más de 3 handlers a la vez, y las 10 acaban respondidas", async () => {
    const p = conPuerta();
    const s = await atiende(p.handler, {}, { maxConcurrent: 3 });
    for (let i = 0; i < 10; i++) s.nc.entrega(SUBJECT, peticion({ id: `req-${i}` }), `_INBOX.llama.c${i}`);
    await sleep(60);
    assert.equal(p.dentro, 3, "tres dentro, siete esperando en la cola");
    p.abre();
    for (let i = 0; i < 10; i++) assert.ok(await esperaPublicada(s.nc, `_INBOX.llama.c${i}`), `la petición ${i} no se contestó`);
    assert.equal(p.maximo, 3, "el máximo fue 3");
    assert.equal(p.terminados, 10);
  });

  test("por defecto son 256 (el orden de `max_ack_pending`)", async () => {
    const p = conPuerta();
    const s = await atiende(p.handler);
    for (let i = 0; i < 300; i++) s.nc.entrega(SUBJECT, peticion({ id: `req-${i}` }), `_INBOX.llama.d${i}`);
    await sleep(120);
    assert.equal(p.dentro, 256);
    p.abre();
    for (let i = 0; i < 300; i += 37) assert.ok(await esperaPublicada(s.nc, `_INBOX.llama.d${i}`, 3000));
    await esperaPublicada(s.nc, "_INBOX.llama.d299", 3000);
    assert.equal(p.maximo, 256);
    assert.equal(p.terminados, 300);
  });

  test("el límite es POR OPERACIÓN: una operación saturada no impide atender a otra", async () => {
    const p = conPuerta();
    const red = new RedEnMemoria();
    const s = await servicio(red, CRM);
    await s.bus.respond(OP, p.handler, { maxConcurrent: 1 });
    await s.bus.respond("clientes.cliente.v1.obtener", () => ({ clienteId: "cli-1", nombre: "Ana", email: "a@b.c" }));
    for (let i = 0; i < 3; i++) s.nc.entrega(SUBJECT, peticion({ id: `req-${i}` }), `_INBOX.llama.e${i}`);
    await sleep(30);
    s.nc.entrega("rpc.clientes.cliente.v1.obtener", peticion({ data: { clienteId: "cli-1" } }, "clientes.cliente.v1.obtener"), "_INBOX.llama.otra");
    assert.ok(await esperaPublicada(s.nc, "_INBOX.llama.otra", 500), "la otra operación se atendió con la primera saturada");
    p.abre();
  });
});

describe("respond() — parada ordenada (§5.5): dejar de escuchar, esperar a lo que está en vuelo, cerrar", () => {
  /** Un handler que tarda hasta que se le abre la puerta. */
  const lento = () => {
    let abre!: () => void;
    const puerta = new Promise<void>((r) => (abre = r));
    return { abre, handler: async () => (await puerta, { descuentoCents: 1, moneda: "EUR" }) };
  };

  test("`stop()` usa el DRENAJE de la librería (no una baja que descarta), y NO retorna hasta que la petición en vuelo tiene su respuesta", async () => {
    const l = lento();
    const s = await atiende(l.handler);
    s.nc.entrega(SUBJECT, peticion(), "_INBOX.llama.v");
    await sleep(20);
    let parado = false;
    const parada = s.r.stop().then(() => (parado = true));
    await sleep(80);
    assert.equal(parado, false, "esperó al handler en vuelo");
    assert.deepEqual(s.nc.orden, [`sub.drain:${SUBJECT}`], "drain, no unsubscribe: una baja descartaría lo que aún llega (A-62)");
    l.abre();
    await parada;
    assert.ok(s.nc.publicadas.some((p) => p.subject === "_INBOX.llama.v"), "la respuesta de la petición en vuelo SE PUBLICÓ antes de retornar");
  });

  test("tras `stop()` las peticiones NUEVAS dan RPC_UNAVAILABLE al instante (no caen en una instancia que se va)", async () => {
    const red = new RedEnMemoria();
    const crm = await servicio(red, CRM);
    const r = await crm.bus.respond(OP, () => ({ descuentoCents: 1, moneda: "EUR" }));
    const llama = await servicio(red);
    await llama.bus.request(OP, PETICION);
    await r.stop();
    const t0 = performance.now();
    await assert.rejects(llama.bus.request(OP, PETICION, { timeoutMs: 5000 }), RpcUnavailableError);
    assert.ok(performance.now() - t0 < 100);
  });

  test("el tope de espera: un handler que no acaba no cuelga la parada (se avisa y se sigue)", async () => {
    const l = lento();
    const s = await atiende(l.handler);
    s.nc.entrega(SUBJECT, peticion(), "_INBOX.llama.v");
    await sleep(20);
    const t0 = performance.now();
    await s.r.stop({ timeoutMs: 80 });
    const dura = performance.now() - t0;
    assert.ok(dura >= 70 && dura < 500, `tardó ${dura.toFixed(0)} ms`);
    assert.ok(s.logs.some((l2) => /siguen en vuelo/.test(l2)));
    l.abre();
  });

  test("tras dejar de escuchar espera un margen breve antes de volver: en un cluster el interés se propaga de forma asíncrona (A-70)", async () => {
    const s = await atiende();
    const t0 = performance.now();
    await s.r.stop();
    const dura = performance.now() - t0;
    assert.ok(dura >= 20, `volvió en ${dura.toFixed(1)} ms: sin margen, la petición que sale justo al irse el último puede agotar su timeout en vez de recibir \`no responders\``);
  });

  test("`stop()` es idempotente", async () => {
    const s = await atiende();
    await Promise.all([s.r.stop(), s.r.stop()]);
    await s.r.stop();
    assert.equal(s.nc.orden.filter((o) => o.startsWith("sub.")).length, 1);
  });

  test("`bus.close()` para los responders ANTES de cerrar la conexión: la petición en vuelo no se pierde", async () => {
    const l = lento();
    const s = await atiende(l.handler);
    s.nc.entrega(SUBJECT, peticion(), "_INBOX.llama.v");
    await sleep(20);
    const cierre = s.bus.close();
    await sleep(50);
    assert.equal(s.nc.publicadasAlDrenar, undefined, "la conexión NO se cerró con un handler en vuelo");
    l.abre();
    await cierre;
    assert.deepEqual(s.nc.orden, [`sub.drain:${SUBJECT}`, "nc.drain"], "primero dejar de escuchar, DESPUÉS cerrar");
    assert.equal(s.nc.publicadasAlDrenar, 1, "cuando se cerró, la respuesta YA estaba publicada (cerrar de golpe la pierde)");
  });

  test("una publicación que falla (la conexión se cerró con la petición en la mano) no tumba al responder: se registra", async () => {
    const s = await atiende();
    s.nc.falloDelPublish = new errors.ClosedConnectionError();
    s.nc.entrega(SUBJECT, peticion(), "_INBOX.llama.x");
    await sleep(40);
    assert.ok(s.logs.some((l) => /no se pudo publicar la respuesta/.test(l)));
    s.nc.falloDelPublish = undefined;
    assert.ok(await servir({ nc: s.nc } as Servicio, peticion(), "_INBOX.llama.y"), "sigue atendiendo");
  });
});

describe("denegaciones — se ven en el log, y el aviso de la respuesta de un dueño sin `allow_responses` dice qué falta", () => {
  test("el subject de una respuesta denegada se normaliza a `_INBOX.<servicio>.>`: un aviso por servicio, no uno por petición", () => {
    assert.equal(normalizeDeniedSubject("_INBOX.llama.AbC123.tok456"), "_INBOX.llama.>");
    assert.equal(normalizeDeniedSubject("_INBOX.llama.>"), "_INBOX.llama.>");
    assert.equal(normalizeDeniedSubject("_INBOX.llama"), "_INBOX.llama");
    assert.equal(normalizeDeniedSubject("rpc.clientes.cliente.v1.obtener"), "rpc.clientes.cliente.v1.obtener");
  });

  test("un dueño cuya respuesta deniega el broker: el logger dice que falta `allow_responses` y que quien llama verá un timeout, UNA sola vez", async () => {
    const s = await atiende();
    for (const inbox of ["_INBOX.llama.AAA.1", "_INBOX.llama.BBB.2"]) {
      s.nc.emiteEstado({ type: "error", error: errors.PermissionViolationError.parse(`Permissions Violation for Publish to "${inbox}"`) });
    }
    await sleep(20);
    const avisos = s.logs.filter((l) => /allow_responses/.test(l));
    assert.equal(avisos.length, 1);
    assert.match(avisos[0], /_INBOX\.llama\.>/);
    assert.match(avisos[0], /RPC_TIMEOUT/);
    assert.match(avisos[0], /max: 1, expires: 30s/);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 9. Métricas (08-observability.md §2.5)
// ═════════════════════════════════════════════════════════════════════════════

describe("métricas flux_rpc_* — nombres y etiquetas son contrato", () => {
  const cuenta = (m: InMemoryMetrics, nombre: string, op: string, outcome: string) =>
    m.snapshot().counters.get(`${nombre}{operation="${op}",outcome="${outcome}"}`) ?? 0;

  test("quien llama cuenta UN outcome por llamada, y los siete de protocol.json aparecen", async () => {
    const vistos = new Set<string>();
    const registra = async (outcome: string, prepara: (red: RedEnMemoria, nc: ConexionFalsa) => void, extra: Partial<ConnectOptions> = {}, data: object = PETICION) => {
      const red = new RedEnMemoria();
      const s = await servicio(red, extra);
      prepara(red, s.nc);
      await s.bus.request(OP, data, { timeoutMs: 40 }).catch(() => {});
      assert.equal(cuenta(s.metricas, "flux_rpc_client_requests_total", OP, outcome), 1, outcome);
      const total = [...s.metricas.snapshot().counters.entries()].filter(([k]) => k.startsWith("flux_rpc_client_requests_total")).reduce((a, [, v]) => a + v, 0);
      assert.equal(total, 1, `${outcome}: una métrica por llamada`);
      vistos.add(outcome);
    };
    const error = (clase: "retryable" | "permanent") => (red: RedEnMemoria) =>
      responderCon(red, (req) => encodeRpcMessage(buildRpcError({ request: { operation: OP, id: req.id, correlationid: req.correlationid, tenantid: req.tenantid },
        id: "01924f91-5d11-7a62-8f0d-4e5f6a7b8c91", source: "/x/y", time: "2026-08-20T10:41:07.331Z", producerversion: "2.1.0", error: { class: clase, code: "X", message: "m" } })));
    await registra("ok", (red) => responderCon(red, (req) => encodeRpcMessage(respuestaBuena(req))));
    await registra("unavailable", () => {});
    await registra("timeout", (red) => red.escuchaCrudo(SUBJECT, () => {}));
    await registra("retryable", error("retryable"));
    await registra("permanent", error("permanent"));
    await registra("invalid_reply", (red) => responderCon(red, () => "basura"));
    await registra("invalid_schema", () => {}, { validation: { mode: "strict", bundle }, schemaBaseUrl: undefined }, { clienteId: "x" });
    const declarados = protocolo.rpc.metrics.flux_rpc_client_requests_total.outcomes as string[];
    assert.deepEqual([...vistos].sort(), [...declarados].sort(), "los outcomes del cliente son los de protocol.json, ni más ni menos");
  });

  test("PERMISSION_DENIED, STALE_REQUEST, RPC_MESSAGE_TOO_LARGE y un fallo de firma cuentan como `permanent`", async () => {
    const red = new RedEnMemoria();
    const s = await servicio(red);
    s.nc.falloDeLaPeticion = () => new errors.RequestError("x", { cause: errors.PermissionViolationError.parse(`Permissions Violation for Publish to "${SUBJECT}"`)! });
    await s.bus.request(OP, PETICION).catch(() => {});
    s.nc.falloDeLaPeticion = undefined;
    s.nc.info = { max_payload: 100 };
    await s.bus.request(OP, PETICION).catch(() => {});
    assert.equal(cuenta(s.metricas, "flux_rpc_client_requests_total", OP, "permanent"), 2);
  });

  test("quien responde cuenta UN outcome por petición, y los siete de protocol.json aparecen", async () => {
    const vistos = new Set<string>();
    const registra = async (outcome: string, handler: Parameters<FluxBus["respond"]>[1], bytes: Uint8Array | string, extra: Partial<ConnectOptions> = {}) => {
      const s = await atiende(handler, extra);
      await servir({ nc: s.nc } as Servicio, bytes);
      const m = s.metricas.snapshot().counters;
      assert.equal(m.get(`flux_rpc_server_requests_total{operation="${OP}",outcome="${outcome}"}`), 1, outcome);
      assert.equal([...m.entries()].filter(([k]) => k.startsWith("flux_rpc_server_requests_total")).reduce((a, [, v]) => a + v, 0), 1, `${outcome}: uno por petición`);
      vistos.add(outcome);
    };
    const kp = generateKeyPair();
    const firmado: Partial<ConnectOptions> = { signing: { publicKeys: { "x-1": kp.publicKeyPem }, verify: "require" } };
    await registra("ok", () => ({ descuentoCents: 1, moneda: "EUR" }), peticion());
    await registra("retryable", () => { throw new RetryableError("luego", { code: "X" }); }, peticion());
    await registra("permanent", () => { throw new PermanentError("no", { code: "X" }); }, peticion());
    await registra("poison", () => ({}), "basura");
    await registra("invalid_schema", () => ({}), peticion({ data: { clienteId: "x" } }), { validation: { mode: "strict", bundle }, schemaBaseUrl: undefined });
    await registra("invalid_signature", () => ({}), peticion(), firmado);
    await registra("stale_request", () => ({}), encodeRpcMessage(createSigner({ privateKeyPem: kp.privateKeyPem, keyId: "x-1" })!.sign(
      buildRpcRequest({ operation: OP, tenantId: "acme", id: ID_PETICION, source: "/x/y", time: "2020-01-01T00:00:00.000Z", correlationid: ID_PETICION, producerversion: "1", classification: "internal", data: PETICION, dataschema: rpcDataschema(OP, "request") }))), firmado);
    const declarados = protocolo.rpc.metrics.flux_rpc_server_requests_total.outcomes as string[];
    assert.deepEqual([...vistos].sort(), [...declarados].sort(), "los outcomes del servidor son los de protocol.json, ni más ni menos");
  });

  test("un error DESCONOCIDO del handler se cuenta `permanent` (no `retryable`)", async () => {
    const s = await atiende(() => { throw new Error("raro"); });
    await servir({ nc: s.nc } as Servicio, peticion());
    assert.equal(cuenta(s.metricas, "flux_rpc_server_requests_total", OP, "permanent"), 1);
  });

  test("la duración se mide en los dos roles: el histograma lleva `operation` y `role`, con los buckets de §3", async () => {
    const red = new RedEnMemoria();
    const crm = await servicio(red, CRM);
    await crm.bus.respond(OP, async () => (await sleep(30), { descuentoCents: 1, moneda: "EUR" }));
    const llama = await servicio(red);
    await llama.bus.request(OP, PETICION);
    for (const [s, rol] of [[llama, "client"], [crm, "server"]] as const) {
      const salida = s.metricas.render();
      assert.match(salida, new RegExp(`flux_rpc_duration_seconds_count\\{operation="${OP}",role="${rol}"\\} 1`), rol);
      assert.match(salida, /flux_rpc_duration_seconds_bucket\{[^}]*le="30"\}/);
      assert.match(salida, /# TYPE flux_rpc_duration_seconds histogram/);
    }
    assert.match(llama.metricas.render(), /# TYPE flux_rpc_client_requests_total counter/);
    assert.match(crm.metricas.render(), /# TYPE flux_rpc_server_requests_total counter/);
    // el del servidor mide lo que tarda el handler: ≥ 30 ms, o sea FUERA del primer bucket (≤ 0,025 s)
    assert.match(crm.metricas.render(), new RegExp(`flux_rpc_duration_seconds_bucket\\{operation="${OP}",role="server",le="0\\.025"\\} 0`));
  });

  test("NUNCA el tenant, el id, el correlationid ni quién llama como etiqueta: cada serie lleva solo `operation` y `outcome` (o `role`)", async () => {
    const red = new RedEnMemoria();
    const crm = await servicio(red, CRM);
    await crm.bus.respond(OP, () => ({ descuentoCents: 1, moneda: "EUR" }));
    const llama = await servicio(red, { tenantId: "tenant-secreto" });
    await llama.bus.request(OP, PETICION, { aggregateId: "agregado-secreto" });
    for (const s of [llama, crm]) {
      const salida = s.metricas.render();
      for (const prohibido of ["tenant-secreto", "agregado-secreto", ID_PETICION, "facturacion-api", "crm-api", "tenantid", "correlationid", "caller"]) {
        assert.ok(!salida.includes(prohibido), `${prohibido} aparece en las métricas`);
      }
      for (const k of s.metricas.snapshot().counters.keys()) {
        if (k.startsWith("flux_rpc_")) assert.match(k, /^flux_rpc_(client|server)_requests_total\{operation="[^"]+",outcome="[a-z_]+"\}$/);
      }
    }
    const etiquetas = new Set<string>(protocolo.rpc.metrics.forbiddenLabels);
    for (const l of ["tenantid", "tenant", "id", "correlationid", "caller", "client", "service"]) assert.ok(etiquetas.has(l), l);
  });

  test("INVALID_RPC_CALL y RESERVED_DOMAIN NO se cuentan: son errores de programación", async () => {
    const red = new RedEnMemoria();
    const s = await servicio(red);
    await s.bus.request("Mal.operacion", {}).catch(() => {});
    await s.bus.request("rpc.clientes.cliente.v1.obtener", {}).catch(() => {});
    await s.bus.request(OP, PETICION, { timeoutMs: 0 }).catch(() => {});
    assert.equal([...s.metricas.snapshot().counters.keys()].filter((k) => k.startsWith("flux_rpc_")).length, 0);
  });

  test("un `MetricsSink` escrito ANTES de la capacidad (sin los tres métodos rpc) sigue funcionando", async () => {
    const red = new RedEnMemoria();
    responderCon(red, (req) => encodeRpcMessage(respuestaBuena(req)));
    const viejo: MetricsSink = {
      eventPublished() {}, eventConsumed() {}, handlerDuration() {}, eventDlq() {}, eventRetried() {}, consumerPending() {}, connectionState() {},
    };
    const { bus } = await servicio(red, { metrics: viejo });
    assert.deepEqual(await bus.request(OP, PETICION), { descuentoCents: 500, moneda: "EUR" });
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 10. Contra protocol.json, y la conexión que pide `no_responders`
// ═════════════════════════════════════════════════════════════════════════════

describe("las constantes del SDK son las de protocol.json → rpc", () => {
  test("timeouts, ventana, patrones, tipo y esquema del error", async () => {
    const S = await import("../src/rpc.js");
    assert.equal(S.RPC_TIMEOUT_DEFAULT_MS, rpc.timeout.defaultMs);
    assert.equal(S.RPC_TIMEOUT_MIN_MS, rpc.timeout.minMs);
    assert.equal(S.RPC_TIMEOUT_MAX_MS, rpc.timeout.maxMs);
    assert.equal(S.RPC_STALE_WINDOW_SECONDS, rpc.signing.staleRequestWindowSeconds);
    assert.equal(S.RPC_ERROR_TYPE, rpc.type.error.value);
    assert.equal(S.RPC_ERROR_DATASCHEMA, rpc.dataschema.error);
    assert.equal(S.RPC_OPERATION_PATTERN.source, rpc.operation.pattern);
    assert.equal(S.RPC_ERROR_CODE_PATTERN.source, protocolo.errors.codePattern);
    assert.equal(S.RPC_STOP_TIMEOUT_MS, rpc.timeout.maxMs, "se espera a los handlers en vuelo lo que dura el timeout máximo");
    assert.equal(S.RPC_MAX_CONCURRENT_HANDLERS, protocolo.consumer.maxAckPending, "el mismo orden que max_ack_pending");
    assert.equal(rpc.queueGroup.wildcardSubscriptionForbidden, true);
    assert.equal(rpc.responder.tenantFilterByConnection, false);
    assert.equal(rpc.caller.attempts, 1);
    assert.deepEqual(rpc.idempotency.retryOnlyOn, ["RPC_UNAVAILABLE", "RPC_TIMEOUT"]);
    assert.equal(rpc.idempotency.sdkAutomaticRetries, "optional-default-0");
    assert.deepEqual(rpc.noResponders.connectFlags, ["headers", "no_responders"]);
    assert.deepEqual(rpc.caller.replyChecks.length, 5, "las cinco comprobaciones");
  });

  test("la capacidad `rpc` ya está definida en protocol.json con la evidencia que exige, y este SDK todavía NO la declara (lo hace la integración)", () => {
    assert.ok(protocolo.capabilities.definitions.rpc);
    assert.ok(protocolo.capabilities.definitions.rpc.requires.includes("exact-subscription-per-operation"));
    assert.ok(!protocolo.capabilities.sdks.node.capabilities.includes("rpc"));
  });

  test("CADA código de la capacidad aparece como literal en este fichero (check-sdk-codes.mjs exige que la evidencia los nombre)", () => {
    for (const c of ["RPC_UNAVAILABLE", "RPC_TIMEOUT", "STALE_REQUEST", "RPC_INVALID_REPLY", "RPC_MESSAGE_TOO_LARGE", "INVALID_RPC_CALL"]) {
      assert.ok(new RegExp(`(["'\`])${c}\\1`).test(FUENTE_DE_ESTE_TEST), c);
    }
  });
});

const FUENTE_DE_ESTE_TEST = await readFile(new URL(import.meta.url.includes("dist-test") ? "sdk-node/test/rpc.test.ts" : "sdk-node/test/rpc.test.ts", RAIZ), "utf8");

describe("la conexión anuncia `headers` y `no_responders` en CONNECT (A-57, A-79): sin ellos RPC_UNAVAILABLE es un timeout", () => {
  /** Un «broker» de mentira que solo dice INFO y captura el CONNECT que le manda la librería con las opciones del SDK. */
  async function capturaConnect(infoHeaders: boolean): Promise<Record<string, unknown>> {
    const { natsOptionsFor } = await import("../src/client.js");
    const { rangoDePuertos } = await import("./integration/broker.js");
    let connect: Record<string, unknown> | undefined;
    const srv = net.createServer((sock) => {
      sock.write(`INFO ${JSON.stringify({ server_id: "X", server_name: "X", version: "2.14.5", proto: 1, go: "go", host: "127.0.0.1", port: 0, headers: infoHeaders, max_payload: 1048576 })}\r\n`);
      let buf = "";
      sock.on("data", (d) => {
        buf += d.toString();
        const m = /CONNECT (.*)\r\n/.exec(buf);
        if (m && !connect) {
          connect = JSON.parse(m[1]);
          sock.write("PONG\r\n");
        }
        if (/PING\r\n/.test(buf)) sock.write("PONG\r\n");
      });
      sock.on("error", () => {});
    });
    const [desde, hasta] = rangoDePuertos();
    let puerto = 0;
    for (let i = 0; i < 40 && !puerto; i++) {
      const candidato = desde + Math.floor(Math.random() * (hasta - desde + 1));
      puerto = await new Promise<number>((res) => {
        srv.once("error", () => res(0));
        srv.listen(candidato, "127.0.0.1", () => res(candidato));
      });
    }
    assert.ok(puerto, `no hay un puerto libre en ${desde}-${hasta}`);
    try {
      const nc = await natsConnect({
        ...natsOptionsFor({ ...OPCIONES, servers: `nats://127.0.0.1:${puerto}` }),
        timeout: 2000,
        maxReconnectAttempts: 0,
      });
      await nc.close();
    } finally {
      await new Promise<void>((r) => srv.close(() => r()));
    }
    assert.ok(connect, "la librería no mandó CONNECT");
    return connect;
  }

  test("con las opciones que `connect()` le pasa a la librería, el CONNECT lleva headers:true y no_responders:true", async () => {
    const c = await capturaConnect(true);
    assert.equal(c.headers, true);
    assert.equal(c.no_responders, true);
    assert.equal(c.name, "facturacion-api@produccion");
  });

  test("CONTROL: si el servidor no ofrece cabeceras en su INFO, la librería no las pide: la prueba sabe fallar", async () => {
    const c = await capturaConnect(false);
    assert.equal(c.headers, undefined);
    assert.equal(c.no_responders, undefined);
  });
});

describe("la API pública", () => {
  test("`bus.request` y `bus.respond` son métodos del bus; el SDK exporta los ocho errores y las funciones puras", async () => {
    const S = await import("../src/index.js");
    for (const n of ["request", "respond"]) assert.equal(typeof (S.FluxBus.prototype as unknown as Record<string, unknown>)[n], "function", n);
    for (const n of ["RpcUnavailableError", "RpcTimeoutError", "StaleRequestError", "RpcInvalidReplyError", "RpcMessageTooLargeError", "InvalidRpcCallError",
      "RpcRetryableError", "RpcPermanentError", "buildRpcRequest", "buildRpcReply", "buildRpcError", "parseRpcOperation", "resolveRpcTimeout", "rpcSubject"]) {
      assert.equal(typeof (S as unknown as Record<string, unknown>)[n], "function", n);
    }
  });
});
