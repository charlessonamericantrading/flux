/**
 * El modelo de petición/respuesta de la CLI, SIN broker: el solape de subjects de `flux doctor`, la
 * validación de la llamada, el sobre de la petición BYTE A BYTE contra la spec, lo que se comprueba
 * de una respuesta y el aprovisionamiento. Cada comprobación va en los dos sentidos: lo correcto pasa
 * y lo incorrecto FALLA (una comprobación que solo se ha visto en verde no demuestra nada).
 *
 * Los casos salen de las MISMAS fuentes que usa un SDK, no de una copia: `protocol.json` (bloque
 * `rpc`), `conformance/cases/cross-sdk-rpc-envelope.json` (los bytes esperados y los vectores de
 * operación y de timeout) y `conformance/cases/rpc-semantics.json` (los 16 patrones medidos contra el
 * broker, A-69). Si la CLI y un SDK discreparan sobre qué es una operación, esto lo vería.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  ErrorRpc, acuseDeStream, ahoraRfc3339, canonRpc, capturaOperaciones, comprobarTamano, construirPeticion,
  erroresDatosError, esErrorDeUnaPeticionIlegible, limiteBytes, parsearTimeout, serializar, solapan, uuidv7, validarClasificacion,
  validarData, validarEntorno, validarOperacion, validarRespuesta, validarTenantLlamada, validarTimeout,
} from "../src/rpc-model.mjs";
import { auditarStreamsRpc } from "../src/doctor-rpc.mjs";
import { recursosDeseados, leerCanonico } from "../src/provision.mjs";
import { leerData } from "../src/call.mjs";
import { bytes, construirError, construirRespuesta } from "./rpc-helpers.mjs";

const RAIZ = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const leer = (...r) => JSON.parse(readFileSync(join(RAIZ, ...r), "utf8"));
const P = leer("protocol.json").rpc;
const ENV = leer("conformance", "cases", "cross-sdk-rpc-envelope.json");
const SEM = leer("conformance", "cases", "rpc-semantics.json");
const fixture = (id) => ENV.fixtures.find((f) => f.id === id);
const sha = (b) => createHash("sha256").update(b).digest("hex");

const falla = (fn, code) =>
  assert.throws(fn, (e) => e instanceof ErrorRpc && (code === undefined || e.code === code), `debía lanzar ${code ?? "ErrorRpc"}`);

// ─── El solape: la comprobación de `flux doctor` (11-request-reply.md §8, A-69) ──────────────────

describe("solapan / capturaOperaciones (token a token, contra rpc.*.*.*.*)", () => {
  test("la forma es la de protocol.json: `rpc.*.*.*.*`", () => assert.equal(canonRpc().forma, "rpc.*.*.*.*"));

  for (const f of P.doctor.flagged) test(`protocol.json marca «${f}»`, () => assert.equal(capturaOperaciones(f), true));
  for (const f of P.doctor.notFlagged) test(`protocol.json NO marca «${f}»`, () => assert.equal(capturaOperaciones(f), false));

  test("los 16 patrones medidos contra el broker (A-69): `overlapsRpc` de cada paso del caso", () => {
    const caso = SEM.cases.find((c) => c.id === "which-stream-subjects-capture-rpc");
    const pasos = caso.steps.filter((s) => s.op === "stream_captures");
    assert.equal(pasos.length, 16);
    for (const s of pasos) assert.equal(capturaOperaciones(s.pattern), s.expect.overlapsRpc, `«${s.pattern}»`);
  });

  test("`>` pide AL MENOS un token más: `rpc.*.*.*.*.>` (seis o más) y `rpc` solo NO se solapan con cinco tokens", () => {
    assert.equal(capturaOperaciones("rpc.*.*.*.*.>"), false);
    assert.equal(capturaOperaciones("rpc.*.*.*.*.*"), false);
    assert.equal(capturaOperaciones("rpc"), false);
    assert.equal(capturaOperaciones("rpc.>"), true);
    assert.equal(capturaOperaciones("rpc.a.b.c.>"), true);
    assert.equal(capturaOperaciones("*.*.*.*.>"), true);
  });

  test("un comodín en medio de un filtro ancho es tan peligroso como `rpc.>`", () => {
    for (const f of ["*.a.*.*.*", "*.rpc.*.*.*", "*.>", "rpc.*.cliente.v1.*", "rpc.otro.>"]) assert.equal(capturaOperaciones(f), true, f);
    for (const f of ["x.*.*.*.*", "clientes.>", "dlq.>", "state.>", "*.*.*.*", "*.*.*.*.*.*", "rpc.a.b.c"]) assert.equal(capturaOperaciones(f), false, f);
  });

  test("es simétrico", () => {
    const xs = ["rpc.>", ">", "*.>", "rpc.*.*.*.*", "clientes.>", "rpc.clientes.cliente.v1.obtener", "*.*.*.*", "rpc.*.*.*.*.>", "a.b"];
    for (const a of xs) for (const b of xs) assert.equal(solapan(a, b), solapan(b, a), `${a} ↔ ${b}`);
  });
});

// ─── La operación y el timeout, con los vectores de los SDKs ─────────────────────────────────────

describe("validarOperacion (11-request-reply.md §2.2, §2.6), con los vectores de cross-sdk-rpc-envelope.json", () => {
  for (const v of ENV.subjectVectors) {
    if (v.natsSubject) test(`«${v.operation}» → ${v.natsSubject}`, () => assert.equal(validarOperacion(v.operation).subject, v.natsSubject));
    else test(`«${JSON.stringify(v.operation)}» (${v.note}) → ${v.errorCode}`, () => falla(() => validarOperacion(v.operation), v.errorCode));
  }
  for (const o of P.operation.valid) test(`protocol.json: «${o}» es válida`, () => assert.equal(validarOperacion(o).operacion, o));
  for (const o of P.operation.invalid) {
    // Una con el prefijo `rpc.` es dominio reservado; el resto, INVALID_RPC_CALL.
    test(`protocol.json: «${o}» es inválida`, () => falla(() => validarOperacion(o), o.startsWith("rpc.") ? "RESERVED_DOMAIN" : "INVALID_RPC_CALL"));
  }
  test("lo que no es un texto (undefined, null, número) es INVALID_RPC_CALL", () => {
    for (const x of [undefined, null, 5, {}]) falla(() => validarOperacion(x), "INVALID_RPC_CALL");
  });
  test("deriva el `type` de la petición y de la respuesta de la operación", () => {
    const o = validarOperacion("logistica.linea-envio.v2.reservar-stock");
    assert.equal(o.tipoPeticion, "com.flux.rpc.logistica.linea-envio.reservar-stock.request.v2");
    assert.equal(o.tipoRespuesta, "com.flux.rpc.logistica.linea-envio.reservar-stock.reply.v2");
  });
  test("con el prefijo `rpc.` el mensaje dice que lo pone la CLI y propone la forma buena; el que no lo lleva, no", () => {
    assert.throws(() => validarOperacion("rpc.clientes.cliente.v1.obtener"), /escribe «clientes\.cliente\.v1\.obtener»/);
    assert.throws(() => validarOperacion("state.cliente.v1.obtener"), /dominio reservado/);
  });
  test("el mensaje de una operación mal formada dice QUÉ está mal y el formato", () => {
    assert.throws(() => validarOperacion("clientes.cliente.v1.obtener_cliente"), /guion bajo/);
    assert.throws(() => validarOperacion("clientes.cliente.v1.*"), /comodín/);
    assert.throws(() => validarOperacion("clientes.cliente.v1"), /3 tokens y deben ser 4/);
    assert.throws(() => validarOperacion("clientes.cliente.v0.obtener"), /versión mayor/);
    assert.throws(() => validarOperacion("Clientes.cliente.v1.obtener"), /mayúsculas.*fantasma/);
    assert.doesNotThrow(() => validarOperacion("clientes.cliente.v1.obtener"));
  });
});

describe("validarTimeout / parsearTimeout (11-request-reply.md §4.2), con los vectores", () => {
  for (const v of ENV.timeoutVectors) {
    const entrada = "timeoutMs" in v ? v.timeoutMs : undefined;
    if (v.effectiveMs !== undefined) test(`${JSON.stringify(entrada)} → ${v.effectiveMs} ms`, () => assert.equal(validarTimeout(entrada), v.effectiveMs));
    else test(`${JSON.stringify(entrada)} → ${v.errorCode} (no se recorta)`, () => falla(() => validarTimeout(entrada), v.errorCode));
  }
  test("los límites salen de protocol.json: 1, 25000 y 5000 por defecto", () => {
    assert.deepEqual([P.timeout.minMs, P.timeout.maxMs, P.timeout.defaultMs], [1, 25000, 5000]);
  });
  test("el texto de `--timeout`: solo dígitos; `1.5`, `1e3`, `-1`, `abc`, `` y ` 5` NO", () => {
    assert.equal(parsearTimeout(undefined), 5000);
    assert.equal(parsearTimeout("1"), 1);
    assert.equal(parsearTimeout("2000"), 2000);
    assert.equal(parsearTimeout("25000"), 25000);
    for (const t of ["0", "-1", "25001", "30000", "1.5", "1e3", "abc", "", " 5", "5 ", "0x10", "+5"]) falla(() => parsearTimeout(t), "INVALID_RPC_CALL");
  });
  test("el mensaje explica el rango, que NO se recorta y por qué 25 s (por debajo del ack_wait de 30 s)", () => {
    assert.throws(() => parsearTimeout("60000"), /entre 1 y 25000.*No se recorta.*ack_wait de 30 s/s);
  });
});

describe("el resto de entradas de la llamada", () => {
  test("clasificación: las cuatro valen; otra es INVALID_DATACLASSIFICATION", () => {
    for (const c of ["public", "internal", "confidential", "restricted"]) assert.equal(validarClasificacion(c), c);
    for (const c of ["secret", "", "Internal", undefined, 3]) falla(() => validarClasificacion(c), "INVALID_DATACLASSIFICATION");
  });
  test("`data` es un objeto: ni array, ni escalar, ni null", () => {
    assert.deepEqual(validarData({}), {});
    for (const d of [[], [1], "x", 3, true, null, undefined]) falla(() => validarData(d), "INVALID_RPC_CALL");
  });
  test("entorno: [a-z0-9-]+ (el patrón de `source` de protocol.json); sin él, un error que dice cómo darlo", () => {
    assert.equal(validarEntorno("produccion"), "produccion");
    assert.equal(validarEntorno("pre-prod"), "pre-prod");
    assert.throws(() => validarEntorno(undefined), /--env.*\$FLUX_ENV/);
    for (const e of ["", "Prod", "a/b", "a b", "a_b"]) assert.throws(() => validarEntorno(e));
  });
  test("tenant: explícito; vacío o ausente NO cae en `system`", () => {
    assert.equal(validarTenantLlamada("acme"), "acme");
    assert.equal(validarTenantLlamada("system"), "system");
    for (const t of [undefined, "", null]) assert.throws(() => validarTenantLlamada(t), /--tenant.*ni `system`/);
  });
  test("--data: JSON directo; un error NO reproduce el texto (acaba en logs); un fichero ilegible dice cuál", async () => {
    assert.deepEqual(await leerData('{"a":1}'), { a: 1 });
    assert.deepEqual(await leerData("@-", { leerStdin: async () => '{"b":2}' }), { b: 2 });
    await assert.rejects(leerData(undefined), /falta `--data`/);
    await assert.rejects(leerData('{"clienteId":"secreto-123",}'), (e) => e.code === "INVALID_RPC_CALL" && /posición \d+/.test(e.message) && !/secreto-123/.test(e.message));
    await assert.rejects(leerData("[1,2]"), /no un array/);
    await assert.rejects(leerData("@/no/existe.json"), /no se pudo leer el fichero «\/no\/existe\.json» de `--data` \(ENOENT\)/);
  });
});

// ─── El sobre de la petición, byte a byte (11-request-reply.md §3.1, §3.5) ───────────────────────

describe("construirPeticion + serializar: los MISMOS bytes que la spec (cross-sdk-rpc-envelope.json)", () => {
  const f = fixture("rpc-request");
  const i = f.input;
  const evento = () => construirPeticion({
    operacion: i.operation, tenantId: i.tenantId, aggregateId: i.aggregateId, id: i.id, source: i.source, time: i.time,
    correlationid: i.correlationid, producerversion: i.producerversion, classification: i.classification, traceparent: i.traceparent, data: i.data,
  });

  test("la petición de §3.5: 668 bytes y el SHA-256 de la spec", () => {
    const b = serializar(evento());
    assert.equal(b.byteLength, f.bytes);
    assert.equal(b.byteLength, 668);
    assert.equal(sha(b), f.sha256);
    assert.equal(Buffer.from(b).toString("base64"), f.base64);
    assert.equal(new TextDecoder().decode(b), JSON.stringify(f.event));
  });
  test("`data` es el último atributo y el orden es el de declaración", () => {
    assert.deepEqual(Object.keys(evento()), Object.keys(f.event));
    assert.equal(Object.keys(evento()).at(-1), "data");
  });
  test("UTF-8 literal: `Otoño` son esos bytes, no `\\u00f1`", () => {
    const b = new TextDecoder().decode(serializar(evento()));
    assert.ok(b.includes("Otoño"));
    assert.doesNotMatch(b, /\\u[0-9a-f]{4}/i);
  });
  test("sin `aggregateId` no hay `subject` ni `partitionkey`; sin traza activa, ni `traceparent`; y NUNCA `causationid`", () => {
    const e = construirPeticion({ ...{ operacion: i.operation, tenantId: "acme", id: i.id, source: "/produccion/flux-cli", time: i.time, producerversion: "0.1.0", classification: "internal", data: {} } });
    assert.deepEqual(Object.keys(e), [
      "specversion", "id", "source", "type", "time", "datacontenttype", "dataschema", "correlationid", "tenantid", "producerversion", "dataclassification", "data",
    ]);
    assert.equal(e.correlationid, e.id, "una petición que no nace de otro evento lleva su propio id como correlationid");
    assert.equal("causationid" in e, false);
  });
  test("sin bundle, el `dataschema` es el <major>.0.0 de la operación", () => {
    const e = construirPeticion({ operacion: "logistica.linea-envio.v2.reservar-stock", tenantId: "t", id: "x", source: "/a/b", time: "t", producerversion: "1", classification: "internal", data: {} });
    assert.equal(e.dataschema, "https://schemas.internal/rpc/logistica/linea-envio/reservar-stock/request/2.0.0.json");
    assert.match(e.dataschema, new RegExp(P.dataschema.request.pattern));
    assert.match(e.type, new RegExp(P.type.request.pattern));
  });
  test("una operación mal formada o un `data` que no es un objeto no producen sobre", () => {
    falla(() => construirPeticion({ operacion: "mal", tenantId: "t", data: {} }), "INVALID_RPC_CALL");
    falla(() => construirPeticion({ operacion: i.operation, tenantId: "t", data: [] }), "INVALID_RPC_CALL");
  });
});

describe("uuidv7 y la hora", () => {
  test("UUIDv7 canónico, con el milisegundo en los 48 primeros bits y creciente", () => {
    const re = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
    const ms = Date.UTC(2026, 7, 20, 10, 41, 7, 318);
    const u = uuidv7(ms);
    assert.match(u, re);
    assert.equal(Number.parseInt(u.replace(/-/g, "").slice(0, 12), 16), ms, "los 48 primeros bits son el instante");
    const a = uuidv7(1000);
    const b = uuidv7(2000);
    assert.ok(a < b, "ordenado por tiempo");
    assert.equal(new Set(Array.from({ length: 1000 }, () => uuidv7())).size, 1000, "no se repiten");
    for (let k = 0; k < 200; k++) assert.match(uuidv7(), re);
  });
  test("`time`: exactamente 3 decimales y Z, incluso con 0 ms (Go recortaría el cero)", () => {
    assert.equal(ahoraRfc3339(new Date("2026-08-20T10:41:07.000Z")), "2026-08-20T10:41:07.000Z");
    assert.equal(ahoraRfc3339(new Date("2026-08-20T10:41:07.410Z")), "2026-08-20T10:41:07.410Z");
    assert.match(ahoraRfc3339(), new RegExp(canonRpc().reTime.source));
  });
});

describe("el tamaño (11-request-reply.md §4.4)", () => {
  test("el límite es el mínimo entre 1 MiB y el max_payload del broker", () => {
    assert.equal(limiteBytes(undefined), 1048576);
    assert.equal(limiteBytes(1048576), 1048576);
    assert.equal(limiteBytes(65536), 65536);
    assert.equal(limiteBytes(8 * 1048576), 1048576);
  });
  test("EXACTAMENTE el límite pasa; uno más, RPC_MESSAGE_TOO_LARGE con los bytes, el límite y el remedio", () => {
    comprobarTamano(new Uint8Array(1048576), undefined);
    assert.throws(() => comprobarTamano(new Uint8Array(1048577), undefined), (e) => e.code === "RPC_MESSAGE_TOO_LARGE" && /1048577 bytes.*1048576.*claim-check.*cursor/s.test(e.message));
    comprobarTamano(new Uint8Array(65536), 65536);
    assert.throws(() => comprobarTamano(new Uint8Array(65537), 65536), /max_payload que anuncia el broker/);
  });
});

// ─── Lo que se comprueba de lo que llega (§4.3) ─────────────────────────────────────────────────

describe("validarRespuesta: las cinco comprobaciones (11-request-reply.md §4.3)", () => {
  const peticion = { operacion: "clientes.cliente.v1.calcular-descuento", id: "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10", correlationid: "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10", tenantid: "acme" };
  const buena = () => structuredClone(fixture("rpc-reply").event);
  const malo = (cambia, esperado) => {
    const e = buena();
    cambia(e);
    assert.throws(
      () => validarRespuesta(peticion, bytes(e)),
      (x) => x instanceof ErrorRpc && x.code === "RPC_INVALID_REPLY" && x.clase === "permanent" && esperado.test(x.message),
      `debía ser RPC_INVALID_REPLY ${esperado}`,
    );
  };

  test("las respuestas de la spec (§3.5) son válidas: la respuesta y los dos errores", () => {
    const r = validarRespuesta(peticion, new Uint8Array(Buffer.from(fixture("rpc-reply").base64, "base64")));
    assert.equal(r.clase, "reply");
    assert.deepEqual(r.evento.data, { descuentoCents: 500, moneda: "EUR", motivo: "Campaña de otoño" });
    const h = validarRespuesta(peticion, new Uint8Array(Buffer.from(fixture("rpc-error-handler").base64, "base64")));
    assert.equal(h.clase, "error");
    assert.deepEqual(h.error, { class: "retryable", code: "HTTP_503", message: "el servicio de tarifas no responde", retryAfterMs: 5000 });
    const s = validarRespuesta(peticion, new Uint8Array(Buffer.from(fixture("rpc-error-sdk").base64, "base64")));
    assert.equal(s.error.code, "INVALID_SCHEMA");
    assert.equal(s.error.class, "permanent");
  });

  test("1. no es un CloudEvent: no es JSON, no es un objeto, sin specversion, sin cada extensión, tipos exactos", () => {
    assert.throws(() => validarRespuesta(peticion, new TextEncoder().encode("hola")), /no es JSON/);
    assert.throws(() => validarRespuesta(peticion, new Uint8Array([0xff, 0xfe, 0xfd])), /no es UTF-8/);
    assert.throws(() => validarRespuesta(peticion, new TextEncoder().encode("[1]")), /no un objeto/);
    malo((e) => { e.specversion = "0.3"; }, /specversion/);
    for (const a of ["id", "source", "type", "time", "datacontenttype", "dataschema", "correlationid", "tenantid", "producerversion", "dataclassification"]) {
      malo((e) => { delete e[a]; }, new RegExp(`\`${a}\` falta o está vacío`));
    }
    malo((e) => { e.tenantid = 42; }, /`tenantid` debe ser una cadena.*42 no es "42"/);
    malo((e) => { e.dataclassification = "secreto"; }, /dataclassification/);
    malo((e) => { e.data = [1]; }, /`data` falta o no es un objeto/);
    malo((e) => { e.causationid = 7; }, /`causationid` debe ser una cadena/);
  });
  test("2. su `type` ni es el de la respuesta de esta operación ni el del error", () => {
    malo((e) => { e.type = "com.flux.rpc.clientes.cliente.obtener.reply.v1"; }, /no es el de la respuesta de esta operación/);
    malo((e) => { e.type = "com.flux.rpc.clientes.cliente.calcular-descuento.request.v1"; }, /no es el de la respuesta/);
    malo((e) => { e.type = "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v2"; }, /no es el de la respuesta/);
  });
  test("3. su `causationid` no es el `id` de la petición (o falta)", () => {
    malo((e) => { e.causationid = "01924f91-ffff-7c1a-8e22-9a4b3c2d1e10"; }, /causationid.*no es el `id` de esta petición/);
    malo((e) => { delete e.causationid; }, /causationid/);
  });
  test("4. no hereda `correlationid` o `tenantid`", () => {
    malo((e) => { e.correlationid = "otro"; }, /correlationid.*HEREDA/);
    malo((e) => { e.tenantid = "globex"; }, /tenantid.*hereda sin modificar/);
  });
  describe("la excepción única de la comprobación 4 (§4.3): el error de una petición que quien responde no pudo leer", () => {
    const ID_ERROR = "01924f91-5d13-7b85-a3e1-6a7b8c9d0e22";
    /** Lo que contesta un SDK a una petición que no pudo leer (§5.2): `correlationid` = su propio `id` y `tenantid: system`. */
    const ilegible = (cambia = () => {}, data = { class: "permanent", code: "MALFORMED_JSON", message: "la petición no es un CloudEvent válido de flux" }) => {
      const e = structuredClone(fixture("rpc-error-sdk").event);
      e.id = ID_ERROR; e.correlationid = ID_ERROR; e.tenantid = "system"; e.causationid = peticion.id; e.data = data;
      delete e.subject; delete e.partitionkey;
      cambia(e);
      return e;
    };
    const noPasa = (e, esperado) =>
      assert.throws(() => validarRespuesta(peticion, bytes(e)), (x) => x instanceof ErrorRpc && x.code === "RPC_INVALID_REPLY" && esperado.test(x.message));

    test("SÍ pasa: es un error con el código del sobre, no RPC_INVALID_REPLY", () => {
      for (const code of ["MALFORMED_JSON", "MISSING_REQUIRED_EXTENSION", "INVALID_RPC_CALL", "WRONG_ATTRIBUTE_TYPE"]) {
        const r = validarRespuesta(peticion, bytes(ilegible(() => {}, { class: "permanent", code, message: "la petición no se pudo leer" })));
        assert.equal(r.clase, "error");
        assert.equal(r.error.code, code);
        assert.equal(r.error.class, "permanent");
      }
    });
    test("la comprobación 3 SIGUE en pie: sin `causationid`, o con el de otra petición", () => {
      noPasa(ilegible((e) => { delete e.causationid; }), /causationid/);
      noPasa(ilegible((e) => { e.causationid = "01924f91-ffff-7c1a-8e22-9a4b3c2d1e10"; }), /causationid.*no es el `id` de esta petición/);
    });
    test("es ESTRECHA: solo un ERROR con `tenantid: system` Y `correlationid` igual a su propio `id`", () => {
      noPasa(ilegible((e) => { e.correlationid = "otro-flujo"; }), /correlationid.*HEREDA/);
      noPasa(ilegible((e) => { e.tenantid = "globex"; }), /correlationid|tenantid/);
      // una RESPUESTA (no un error) con la misma forma
      const r = buena(); r.id = ID_ERROR; r.correlationid = ID_ERROR; r.tenantid = "system";
      noPasa(r, /correlationid.*HEREDA/);
      // CONTROL: un error de una petición leída sigue heredando
      noPasa(ilegible((e) => { e.correlationid = "otra-cadena"; e.tenantid = peticion.tenantid; }), /correlationid.*HEREDA/);
    });
    test("la quinta comprobación SIGUE en pie: el `data` del error ilegible debe cumplir el esquema del error", () => {
      noPasa(ilegible(() => {}, { class: "poison", code: "MALFORMED_JSON", message: "m" }), /`poison` no viaja/);
      noPasa(ilegible(() => {}, { class: "permanent", code: "malformed_json", message: "m" }), /\/code debe ser un identificador estable/);
    });
    test("el predicado: la forma exacta, y solo para un error", () => {
      const e = { id: ID_ERROR, correlationid: ID_ERROR, tenantid: "system" };
      assert.equal(esErrorDeUnaPeticionIlegible(e, true), true);
      assert.equal(esErrorDeUnaPeticionIlegible(e, false), false);
      assert.equal(esErrorDeUnaPeticionIlegible({ ...e, tenantid: "acme" }, true), false);
      assert.equal(esErrorDeUnaPeticionIlegible({ ...e, correlationid: "x" }, true), false);
    });
  });

  test("5. un error con un `data` que no cumple el esquema del error", () => {
    const err = () => structuredClone(fixture("rpc-error-handler").event);
    const malError = (cambia, esperado) => {
      const e = err();
      cambia(e.data);
      assert.throws(() => validarRespuesta(peticion, bytes(e)), (x) => x.code === "RPC_INVALID_REPLY" && esperado.test(x.message));
    };
    malError((d) => { d.class = "poison"; }, /`poison` no viaja/);
    malError((d) => { d.class = "fatal"; }, /\/class debe ser retryable o permanent/);
    malError((d) => { d.code = "http_503"; }, /\/code debe ser un identificador estable/);
    malError((d) => { d.message = ""; }, /\/message debe ser un texto no vacío/);
    malError((d) => { delete d.message; }, /\/message es obligatorio/);
    malError((d) => { d.extra = 1; }, /\/extra no es un campo/);
    malError((d) => { d.retryAfterMs = -1; }, /retryAfterMs debe ser un entero/);
    malError((d) => { d.class = "permanent"; }, /retryAfterMs solo tiene sentido con class retryable/);
    malError((d) => { d.code = "A".repeat(65); }, /\/code no puede pasar de 64/);
    malError((d) => { d.message = "x".repeat(1001); }, /\/message no puede pasar de 1000/);
    assert.deepEqual(erroresDatosError({ class: "permanent", code: "X", message: "m" }), []);
  });

  test("EL ACUSE DE UN STREAM (A-18, A-67): RPC_INVALID_REPLY que nombra el stream, apunta a `flux doctor` y lleva el acuse", () => {
    const acuse = new TextEncoder().encode('{"stream":"TRAMPA","seq":1}');
    assert.deepEqual(acuseDeStream({ stream: "TRAMPA", seq: 1 }), { stream: "TRAMPA", seq: 1 });
    assert.equal(acuseDeStream({ stream: "X" }), null);
    assert.equal(acuseDeStream({ specversion: "1.0", stream: "X", seq: 1 }), null);
    assert.throws(
      () => validarRespuesta(peticion, acuse),
      (e) => e.code === "RPC_INVALID_REPLY" && e.acuse.stream === "TRAMPA" && /stream TRAMPA/.test(e.message) && /flux doctor/.test(e.message) && /rpc\.clientes\.cliente\.v1\.calcular-descuento/.test(e.message),
    );
  });
  test("lo que no es un CloudEvent NO reproduce el cuerpo (puede llevar datos personales) salvo con --show-data", () => {
    const cuerpo = new TextEncoder().encode("tarjeta 4111-1111");
    assert.throws(() => validarRespuesta(peticion, cuerpo), (e) => !/4111/.test(e.message));
    assert.throws(() => validarRespuesta(peticion, cuerpo, { showData: true }), /4111-1111/);
  });
});

// ─── El responder de prueba habla como la spec ───────────────────────────────────────────────────

describe("los constructores del responder de prueba dan los bytes de la spec (no solo lo que la CLI espera)", () => {
  test("rpc-reply: 703 bytes y su SHA-256", () => {
    const f = fixture("rpc-reply");
    const i = f.input;
    const b = bytes(construirRespuesta({ operation: i.request.operation, request: i.request, id: i.id, source: i.source, time: i.time, producerversion: i.producerversion, classification: i.classification, traceparent: i.traceparent, data: i.data }));
    assert.equal(b.byteLength, 703);
    assert.equal(sha(b), f.sha256);
  });
  for (const id of ["rpc-error-handler", "rpc-error-sdk"]) {
    test(`${id}: los bytes y el SHA-256`, () => {
      const f = fixture(id);
      const i = f.input;
      const b = bytes(construirError({ request: i.request, id: i.id, source: i.source, time: i.time, producerversion: i.producerversion, traceparent: i.traceparent, error: i.error }));
      assert.equal(b.byteLength, f.bytes);
      assert.equal(sha(b), f.sha256);
    });
  }
});

// ─── El doctor: la parte pura ────────────────────────────────────────────────────────────────────

describe("auditarStreamsRpc: qué stream y cómo se arregla", () => {
  const s = (name, subjects, extra = {}) => ({ config: { name, subjects, ...extra } });

  test("sin streams, o con los de siempre (EVT_, DLQ_, STATE_), está bien", () => {
    assert.equal(auditarStreamsRpc([]).ok, true);
    const r = auditarStreamsRpc([s("EVT_PEDIDOS", ["pedidos.>"]), s("DLQ_PEDIDOS", ["dlq.pedidos.>", "dlq.state.pedidos.>"]), s("STATE_CLIENTES", ["state.clientes.>"]), s("EVT_X", ["x.>"])]);
    assert.equal(r.ok, true);
    assert.equal(r.revisados, 4);
    assert.deepEqual(r.findings, []);
  });
  test("un stream que captura `rpc.>` es un ERROR que nombra el stream, el subject y el arreglo exacto", () => {
    const r = auditarStreamsRpc([s("EVT_PEDIDOS", ["pedidos.>"]), s("TRAMPA", ["rpc.>"])]);
    assert.equal(r.ok, false);
    assert.equal(r.findings.length, 1);
    const f = r.findings[0];
    assert.equal(f.level, "error");
    assert.equal(f.target, "TRAMPA");
    assert.match(f.message, /`rpc\.>`.*rpc\.\*\.\*\.\*\.\*.*stream con acuse/);
    assert.match(f.hint, /acuse.*GUARDA cada petición.*datos personales/s);
    assert.match(f.hint, /nats stream rm TRAMPA/, "no le queda nada más: se borra");
  });
  test("si el stream captura además lo suyo, el arreglo conserva lo demás", () => {
    const f = auditarStreamsRpc([s("MIXTO", ["clientes.>", "rpc.x.>", "dlq.>"])]).findings[0];
    assert.match(f.hint, /nats stream edit MIXTO --subjects "clientes\.>,dlq\.>"/);
    assert.deepEqual(f.restantes, ["clientes.>", "dlq.>"]);
  });
  test("un `no_ack` captura sin acuse, pero cuenta como interés: RPC_UNAVAILABLE pasa a RPC_TIMEOUT", () => {
    const f = auditarStreamsRpc([s("AUDITORIA", ["rpc.x.>"], { no_ack: true })]).findings[0];
    assert.equal(f.noAck, true);
    assert.match(f.message, /stream sin acuse, no_ack/);
    assert.match(f.hint, /CUENTA COMO INTERÉS.*RPC_UNAVAILABLE.*RPC_TIMEOUT/s);
  });
  test("los filtros anchos que solo se crean sin acuse (`>`, `*.>`, `*.*.*.*.*`) se marcan y se dice que son anchos", () => {
    for (const f of [">", "*.>", "*.*.*.*.*"]) {
      const r = auditarStreamsRpc([s("TODO", [f], { no_ack: true })]);
      assert.equal(r.ok, false, f);
      assert.deepEqual(r.capturas[0].ancho, [f]);
      assert.match(r.findings[0].hint, /filtro ancho.*solo deja crearlo sin acuse.*cada respuesta viaja también/s, f);
    }
  });
  test("lo que NO captura una operación, aunque sea `no_ack` o ancho de otro tamaño, no se marca", () => {
    assert.equal(auditarStreamsRpc([s("A", ["*.*.*.*"], { no_ack: true }), s("B", ["*.*.*.*.*.*"], { no_ack: true }), s("C", ["clientes.>"]), s("D", ["rpc.a.b.c"])]).ok, true);
  });
  test("`EVT_RPC` (el dominio reservado aprovisionado a mano) dice por qué", () => {
    assert.match(auditarStreamsRpc([s("EVT_RPC", ["rpc.>"])]).findings[0].hint, /dominio» llamado `rpc`.*reservado \(RESERVED_DOMAIN\)/s);
  });
  test("varios streams, varios hallazgos: cada uno el suyo", () => {
    const r = auditarStreamsRpc([s("UNO", ["rpc.>"]), s("OK", ["a.>"]), s("DOS", ["rpc.*.>", "b.>"], { no_ack: true })]);
    assert.deepEqual(r.findings.map((f) => f.target), ["UNO", "DOS"]);
    assert.deepEqual(r.capturas.map((c) => c.stream), ["UNO", "DOS"]);
  });
});

// ─── El aprovisionamiento NUNCA crea un stream que capture una operación ─────────────────────────

describe("provision: ningún stream que crea captura rpc.*.*.*.* (11-request-reply.md §8)", () => {
  const manifiesto = leer("examples", "services.json");

  test("los streams del manifiesto de ejemplo (eventos, DLQ y estado) no capturan ninguna operación", async () => {
    const { analizarManifiesto, loadBundle } = await import("../../scripts/lib/manifest.mjs");
    const { problemas, servicios } = analizarManifiesto(manifiesto, await loadBundle(join(RAIZ, "schemas", "bundle.json")));
    assert.deepEqual(problemas, [], "el manifiesto de ejemplo es válido");
    const { streams } = recursosDeseados(servicios, await leerCanonico(), 1);
    assert.ok(streams.length >= 8, `hay streams de las tres familias (${streams.length})`);
    assert.ok(streams.some((x) => x.name.startsWith("EVT_")) && streams.some((x) => x.name.startsWith("DLQ_")) && streams.some((x) => x.name.startsWith("STATE_")));
    for (const st of streams) for (const sj of st.config.subjects) assert.equal(capturaOperaciones(sj), false, `${st.name}: ${sj}`);
  });

  test("un dominio llamado `rpc` produciría EVT_RPC (`rpc.>`): se niega ANTES de crear nada, con el motivo", async () => {
    const servicio = { name: "intruso-api", owns: ["rpc"], consumes: [], watches: [], stateOwns: [] };
    await assert.rejects(
      async () => recursosDeseados([servicio], await leerCanonico(), 1),
      /aprovisionamiento crearía EVT_RPC \(rpc\.>\).*captura operaciones.*rpc\.\*\.\*\.\*\.\*.*acuse.*datos personales.*dominio reservado/s,
    );
  });
  test("`rpc-gateway` NO es el dominio `rpc` (kebab-case): su `EVT_RPC_GATEWAY` (`rpc-gateway.>`) no captura nada y se crea", async () => {
    assert.equal(capturaOperaciones("rpc-gateway.>"), false);
  });
});
