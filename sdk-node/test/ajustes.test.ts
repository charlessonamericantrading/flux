/**
 * Los ajustes de la spec tras medir las divergencias Node↔Rust fuera de los vectores (conformance/harness/vectors.json →
 * `ajustesVectors`; los vectores cruzados los ejecuta `node conformance/cross-sdk.mjs`). Aquí, lo que un vector no puede decir:
 * las funciones puras (`text.ts`, `time.ts`), los constructores de eventos —que el arnés cruzado ejecuta con UNA operación general
 * que los otros cinco SDKs no alinean aún— y el resto de sitios donde se aplica la misma regla. Cada comprobación va en los dos
 * sentidos: lo correcto pasa y lo incorrecto falla.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";

import {
  EnvelopeError,
  InvalidDataClassificationError,
  InvalidRpcCallError,
  InvalidSubjectError,
  ReservedDomainError,
  buildEvent,
  buildRpcRequest,
  buildStateEvent,
  buildStateTombstone,
  parseEvent,
  parseStateContract,
  parseSubject,
  serialize,
  subjectToType,
  toDlqEvent,
  stateSubject,
} from "../src/index.js";
import { InvalidEnvelopeInputError } from "../src/envelope.js";
import { codePointLength, truncateCodePoints, truncateUtf8Bytes } from "../src/text.js";
import { canonicalTime } from "../src/time.js";

const bytesDe = (s: string) => new TextEncoder().encode(s).byteLength;

// ═════════════════════════════════════════════════════════════════════════════
// text.ts
// ═════════════════════════════════════════════════════════════════════════════

describe("text.ts — longitudes en puntos de código y truncado en bytes UTF-8", () => {
  test("codePointLength cuenta caracteres Unicode, no unidades UTF-16: mil emojis son mil", () => {
    assert.equal(codePointLength(""), 0);
    assert.equal(codePointLength("abc"), 3);
    assert.equal(codePointLength("ñ"), 1);
    assert.equal(codePointLength("😀"), 1);
    assert.equal("😀".length, 2, "CONTROL: `length` de JavaScript cuenta 2");
    assert.equal(codePointLength("😀".repeat(1000)), 1000);
    assert.equal(codePointLength("a😀b"), 3);
  });

  test("truncateCodePoints: lo que cabe pasa entero; si no, max−1 caracteres y «…»; nunca parte un par sustituto", () => {
    assert.equal(truncateCodePoints("abc", 3), "abc");
    assert.equal(truncateCodePoints("abcd", 3), "ab…");
    assert.equal(truncateCodePoints("😀".repeat(1000), 1000), "😀".repeat(1000));
    assert.equal(truncateCodePoints("😀".repeat(1001), 1000), `${"😀".repeat(999)}…`);
    assert.equal(truncateCodePoints("😀😀😀", 2), "😀…");
    for (const s of [truncateCodePoints("😀".repeat(5), 3), truncateCodePoints("a😀b😀c", 4)]) {
      assert.doesNotMatch(JSON.stringify(s), /\\ud[89ab]/i, "ninguna mitad de un sustituto");
    }
  });

  test("truncateUtf8Bytes: 1024 bytes sin partir un carácter (los vectores ajuste-dlqerror-*)", () => {
    const corta = (s: string) => truncateUtf8Bytes(s, 1024);
    // 1500 × «é» (2 bytes): caben 512
    assert.equal(corta("é".repeat(1500)), "é".repeat(512));
    // 1023 «a» + «é»: la «é» no cabe entera y se descarta
    assert.equal(corta("a".repeat(1023) + "é"), "a".repeat(1023));
    // 1022 «a» + «é» son EXACTAMENTE 1024 bytes: cabe
    assert.equal(corta("a".repeat(1022) + "é"), "a".repeat(1022) + "é");
    // 1022 «a» + «€» (3 bytes): no cabe
    assert.equal(corta("a".repeat(1022) + "€"), "a".repeat(1022));
    // 1021 «a» + 😀 (4 bytes) + «b»: el emoji no cabe, y NO se parte
    assert.equal(corta("a".repeat(1021) + "😀b"), "a".repeat(1021));
    // 300 × 😀: caben 256
    assert.equal(corta("😀".repeat(300)), "😀".repeat(256));
    assert.equal(bytesDe(corta("😀".repeat(300))), 1024);
    // lo corto no se toca, ni lo vacío
    assert.equal(corta("hola"), "hola");
    assert.equal(corta(""), "");
    // CONTROL: `slice(0, 1024)` de Node (unidades UTF-16) NO daba esto: con 1500 «é» dejaba 2048 bytes
    assert.equal(bytesDe("é".repeat(1500).slice(0, 1024)), 2048);
    for (const s of ["é".repeat(1500), "😀".repeat(300), "a".repeat(1021) + "😀b", "日本語".repeat(500)]) {
      const c = corta(s);
      assert.ok(bytesDe(c) <= 1024, "nunca pasa de 1024 bytes");
      assert.equal(new TextDecoder("utf-8", { fatal: true }).decode(new TextEncoder().encode(c)), c, "sigue siendo UTF-8 válido");
      assert.ok(s.startsWith(c), "es un prefijo del original");
    }
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// time.ts
// ═════════════════════════════════════════════════════════════════════════════

describe("time.ts — el `time` que da quien llama se normaliza a la forma canónica o se rechaza (01-envelope.md §2.2)", () => {
  // La MISMA tabla que `parse_instant_normaliza_y_rechaza` de sdk-rust/src/envelope.rs: lo que admite Node y lo que admite Rust.
  const CANON: Array<[string, string]> = [
    ["2026-08-20T10:41:07.318Z", "2026-08-20T10:41:07.318Z"],
    ["2026-08-20T10:41:07Z", "2026-08-20T10:41:07.000Z"],
    ["2026-08-20T10:41:07.3Z", "2026-08-20T10:41:07.300Z"],
    ["2026-08-20T10:41:07.318927Z", "2026-08-20T10:41:07.318Z"],
    ["2026-08-20T10:41:07.9999Z", "2026-08-20T10:41:07.999Z"],
    ["2026-08-20T12:41:07.318+02:00", "2026-08-20T10:41:07.318Z"],
    ["2026-08-20T05:11:07.318-05:30", "2026-08-20T10:41:07.318Z"],
    ["2026-08-20T10:41:07.318-00:00", "2026-08-20T10:41:07.318Z"],
    ["2026-08-20t10:41:07.318z", "2026-08-20T10:41:07.318Z"],
    ["2026-08-20 10:41:07.318Z", "2026-08-20T10:41:07.318Z"],
    ["2028-02-29T10:41:07.318Z", "2028-02-29T10:41:07.318Z"],
    ["0000-01-01T00:00:00.000Z", "0000-01-01T00:00:00.000Z"],
    ["0099-12-31T23:59:59.999Z", "0099-12-31T23:59:59.999Z"],
    ["9999-12-31T23:59:59.999Z", "9999-12-31T23:59:59.999Z"],
  ];
  const MALOS = [
    "", "ayer", "2026-08-20", "2026-08-20T10:41:07", "2026-13-20T10:41:07.318Z", "2026-00-20T10:41:07.318Z",
    "2026-02-30T10:41:07.318Z", "2026-02-29T10:41:07.318Z", "2026-04-31T10:41:07.318Z", "2026-08-20T24:00:00.000Z",
    "2026-08-20T10:60:00.000Z", "2026-08-20T10:41:61.000Z", "12026-08-20T10:41:07.318Z", " 2026-08-20T10:41:07.318Z",
    "2026-08-20T10:41:07.318Z ", "2026-08-20T12:41:07.318+02", "2026-08-20T12:41:07.318+0200", "2026-08-20T12:41:07.318+24:00",
    "2026-08-20T12:41:07.318+02:60", "2026-08-20T10:41:07,318Z", "2026-08-20T10:41:07.Z", "٢٠٢٦-08-20T10:41:07.318Z",
    // un segundo intercalar no existe en UTC de POSIX ni en `Date`
    "2026-06-30T23:59:60.000Z",
    // el año en UTC fuera de 0000–9999
    "9999-12-31T23:59:59.999-01:00", "0000-01-01T00:30:00.000+01:00",
  ];
  for (const [entra, sale] of CANON) test(`${entra} → ${sale}`, () => assert.equal(canonicalTime(entra), sale));
  for (const malo of MALOS) test(`se rechaza ${JSON.stringify(malo)}`, () => assert.equal(canonicalTime(malo), null));
  test("lo que no es una cadena se rechaza", () => {
    for (const x of [undefined, null, 5, 1787000000000, {}, [], true, new Date()]) assert.equal(canonicalTime(x), null);
  });
  test("el año 0–99 NO se convierte en 1900–1999 (`Date.UTC` lo haría)", () => {
    assert.equal(canonicalTime("0050-06-15T12:00:00.000Z"), "0050-06-15T12:00:00.000Z");
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// buildEvent / toDlqEvent
// ═════════════════════════════════════════════════════════════════════════════

const ev = (extra: Record<string, unknown> = {}) => ({
  subject: "pedidos.pedido.v1.creado",
  data: { pedidoId: "ped-123" },
  id: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  source: "/produccion/pedidos-api",
  producerversion: "3.4.1",
  tenantid: "acme",
  dataclassification: "internal" as const,
  dataschema: "https://schemas.internal/pedidos/pedido/creado/1.0.0.json",
  correlationid: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  time: "2025-08-20T10:25:39.410Z",
  ...extra,
});

describe("buildEvent — rechaza lo que su propio parser rechazaría, con el mismo código", () => {
  const codeOf = (fn: () => unknown): string | undefined => {
    try {
      fn();
      return undefined;
    } catch (e) {
      return (e as { code?: string; opts?: { code?: string } }).code ?? (e as { opts?: { code?: string } }).opts?.code ?? (e as Error).name;
    }
  };

  test("un atributo vacío: MISSING_REQUIRED_ATTRIBUTE (id, source, dataschema) o MISSING_REQUIRED_EXTENSION (las otras tres)", () => {
    for (const [campo, code] of [
      ["id", "MISSING_REQUIRED_ATTRIBUTE"], ["source", "MISSING_REQUIRED_ATTRIBUTE"], ["dataschema", "MISSING_REQUIRED_ATTRIBUTE"],
      ["correlationid", "MISSING_REQUIRED_EXTENSION"], ["tenantid", "MISSING_REQUIRED_EXTENSION"], ["producerversion", "MISSING_REQUIRED_EXTENSION"],
    ] as const) {
      assert.equal(codeOf(() => buildEvent(ev({ [campo]: "" }))), code, campo);
      assert.equal(codeOf(() => buildEvent(ev({ [campo]: undefined }))), code, `${campo} ausente`);
      assert.equal(codeOf(() => buildEvent(ev({ [campo]: 5 }))), code, `${campo} no es una cadena`);
      try { buildEvent(ev({ [campo]: "" })); } catch (e) { assert.ok(e instanceof InvalidEnvelopeInputError); assert.match((e as Error).message, new RegExp(`\`${campo}\``)); }
    }
    assert.ok(buildEvent(ev()), "CONTROL: el evento bueno se construye");
  });

  test("`time`: se normaliza a la forma canónica, o WRONG_ATTRIBUTE_TYPE", () => {
    assert.equal(buildEvent(ev({ time: "2025-08-20T12:25:39.410+02:00" })).time, "2025-08-20T10:25:39.410Z");
    assert.equal(buildEvent(ev({ time: "2025-08-20T10:25:39Z" })).time, "2025-08-20T10:25:39.000Z");
    assert.match(buildEvent(ev({ time: undefined })).time, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/, "sin `time`, el reloj");
    for (const malo of ["ayer", "", "2025-08-20", "2025-08-20T10:25:39", 5]) {
      assert.equal(codeOf(() => buildEvent(ev({ time: malo }))), "WRONG_ATTRIBUTE_TYPE", String(malo));
    }
  });

  test("un dominio reservado y una clasificación fuera del enum", () => {
    for (const subject of ["rpc.pedido.v1.creado", "state.pedido.v1.creado", "dlq.pedido.v1.creado", "system.pedido.v1.creado"]) {
      assert.throws(() => buildEvent(ev({ subject })), (e) => e instanceof ReservedDomainError, subject);
    }
    assert.throws(() => buildEvent(ev({ dataclassification: "secreto" })), (e) => e instanceof InvalidDataClassificationError);
    assert.throws(() => buildEvent(ev({ data: [1] })), (e) => e instanceof EnvelopeError);
  });

  test("el orden: `data` objeto → subject → clasificación → atributos vacíos → time (el primero que falla gana)", () => {
    assert.equal(codeOf(() => buildEvent(ev({ data: null, subject: "x" }))), "EnvelopeError");
    assert.equal(codeOf(() => buildEvent(ev({ subject: "rpc.a.v1.b", dataclassification: "x", id: "", time: "ayer" }))), "RESERVED_DOMAIN");
    assert.equal(codeOf(() => buildEvent(ev({ dataclassification: "x", id: "", time: "ayer" }))), "INVALID_DATACLASSIFICATION");
    assert.equal(codeOf(() => buildEvent(ev({ id: "", time: "ayer" }))), "MISSING_REQUIRED_ATTRIBUTE");
  });
});

describe("la versión mayor son de 1 a 4 dígitos en todos los sitios donde Node la parsea", () => {
  test("parseSubject, subjectToType, parseStateContract y stateSubject: v9999 sí, v10000 y lo enorme no", () => {
    for (const v of ["v1", "v9", "v10", "v99", "v100", "v9999"]) {
      assert.equal(parseSubject(`pedidos.pedido.${v}.creado`).major, Number(v.slice(1)));
      assert.equal(subjectToType(`pedidos.pedido.${v}.creado`), `com.flux.pedidos.pedido.creado.${v}`);
      assert.equal(parseStateContract(`clientes.cliente.${v}`).major, Number(v.slice(1)));
      assert.equal(stateSubject(`clientes.cliente.${v}`, "acme", "cli-1"), `state.clientes.cliente.${v}.acme.cli-1`);
    }
    for (const v of ["v10000", "v99999", "v4294967296", "v99999999999999999999", "v0", "v01", "v", "V1"]) {
      assert.throws(() => parseSubject(`pedidos.pedido.${v}.creado`), (e) => e instanceof InvalidSubjectError, v);
      assert.throws(() => parseStateContract(`clientes.cliente.${v}`), (e: Error & { code?: string }) => e.code === "INVALID_STATE_CONTRACT", v);
    }
  });
  test("el `type` ya no puede salir `v100000000000000000000` (el entero pasaba por `Number`, que redondea)", () => {
    assert.throws(() => subjectToType("pedidos.pedido.v99999999999999999999.creado"));
    assert.throws(() => buildRpcRequest({
      operation: "clientes.cliente.v99999999999999999999.obtener", tenantId: "acme", id: "x", source: "/p/s", time: "2026-08-20T10:41:07.318Z",
      correlationid: "x", producerversion: "1", classification: "internal", data: {},
    }), (e) => e instanceof InvalidRpcCallError);
  });
});

describe("toDlqEvent — `dlqerror` mide 1024 BYTES UTF-8, sin partir un carácter (04-errors.md §3)", () => {
  const original = parseEvent(serialize(buildEvent(ev())));
  const info = (error: string) => ({ reason: "permanent" as const, attempts: 1, consumer: "facturacion-api__pedidos_pedido_v1_creado", error });

  test("1500 «é» quedan en 512 (1024 bytes); antes quedaban 1024 unidades UTF-16 = 2048 bytes", () => {
    const d = toDlqEvent(original, info("é".repeat(1500)));
    assert.equal(d.dlqerror, "é".repeat(512));
    assert.equal(bytesDe(d.dlqerror as string), 1024);
  });
  test("no parte un carácter en el corte: la «é», el «€» y el emoji que no caben se descartan enteros", () => {
    assert.equal(toDlqEvent(original, info("a".repeat(1023) + "é")).dlqerror, "a".repeat(1023));
    assert.equal(toDlqEvent(original, info("a".repeat(1022) + "é")).dlqerror, "a".repeat(1022) + "é");
    assert.equal(toDlqEvent(original, info("a".repeat(1022) + "€")).dlqerror, "a".repeat(1022));
    const emoji = toDlqEvent(original, info("a".repeat(1021) + "😀b")).dlqerror as string;
    assert.equal(emoji, "a".repeat(1021));
    assert.doesNotMatch(JSON.stringify(emoji), /\\ud8/i);
  });
  test("`dlqtime` dado se normaliza o se rechaza; sin él, el reloj; y `data` sigue siendo el último atributo", () => {
    assert.equal(toDlqEvent(original, info("x"), "2025-08-20T12:25:40+02:00").dlqtime, "2025-08-20T10:25:40.000Z");
    assert.throws(() => toDlqEvent(original, info("x"), "ayer"), (e: Error & { code?: string }) => e.code === "WRONG_ATTRIBUTE_TYPE");
    assert.match(toDlqEvent(original, info("x")).dlqtime as string, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
    assert.equal(Object.keys(toDlqEvent(original, info("x"), "2025-08-20T10:25:40.117Z")).at(-1), "data");
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// estado: los constructores
// ═════════════════════════════════════════════════════════════════════════════

describe("los constructores de estado rechazan lo vacío con el código del parser y normalizan `time`", () => {
  const put = (extra: Record<string, unknown> = {}) => ({
    contract: "clientes.cliente.v1", tenantId: "acme", key: "cli-1", id: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55", source: "/produccion/crm-api",
    time: "2026-08-20T10:25:39.412Z", correlationid: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55", producerversion: "2.1.0",
    classification: "confidential" as const, data: { clienteId: "cli-1", aggregateVersion: 7 }, ...extra,
  });
  const code = (fn: () => unknown) => { try { fn(); return undefined; } catch (e) { return (e as { opts?: { code?: string } }).opts?.code ?? (e as Error).name; } };
  test("id y source vacíos son MISSING_REQUIRED_ATTRIBUTE; correlationid y producerversion, MISSING_REQUIRED_EXTENSION", () => {
    assert.equal(code(() => buildStateEvent(put({ id: "" }))), "MISSING_REQUIRED_ATTRIBUTE");
    assert.equal(code(() => buildStateEvent(put({ source: "" }))), "MISSING_REQUIRED_ATTRIBUTE");
    assert.equal(code(() => buildStateEvent(put({ correlationid: "" }))), "MISSING_REQUIRED_EXTENSION");
    assert.equal(code(() => buildStateEvent(put({ producerversion: "" }))), "MISSING_REQUIRED_EXTENSION");
    assert.ok(buildStateEvent(put()), "CONTROL");
    const { data: _d, classification: _c, ...lapida } = put();
    assert.ok(buildStateTombstone({ ...lapida, aggregateVersion: 8 }), "CONTROL: la lápida buena");
    assert.equal(code(() => buildStateTombstone({ ...lapida, id: "", aggregateVersion: 8 })), "MISSING_REQUIRED_ATTRIBUTE");
    assert.equal(code(() => buildStateTombstone({ ...lapida, time: "ayer", aggregateVersion: 8 })), "WRONG_ATTRIBUTE_TYPE");
  });
  test("`time`: forma canónica o WRONG_ATTRIBUTE_TYPE; el error de estado manda sobre el del sobre (§3.6 va antes)", () => {
    assert.equal(buildStateEvent(put({ time: "2026-08-20T12:25:39.412+02:00" })).time, "2026-08-20T10:25:39.412Z");
    assert.equal(code(() => buildStateEvent(put({ time: "ayer" }))), "WRONG_ATTRIBUTE_TYPE");
    assert.equal(code(() => buildStateEvent(put({ key: "a.b", id: "", time: "ayer" }))), "INVALID_STATE_KEY");
    assert.equal(code(() => buildStateEvent(put({ id: "", time: "ayer" }))), "MISSING_REQUIRED_ATTRIBUTE");
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// parseEvent: UTF-8 estricto y tipos exactos de lo no obligatorio
// ═════════════════════════════════════════════════════════════════════════════

describe("parseEvent — UTF-8 estricto, y tipos exactos también en lo no obligatorio (01-envelope.md §2.4)", () => {
  const buena = serialize(buildEvent(ev({ aggregateId: "ped-123" })));
  const codigo = (b: Uint8Array) => { try { parseEvent(b); return "ok"; } catch (e) { return (e as { opts?: { code?: string } }).opts?.code ?? (e as Error).name; } };
  const objeto = () => JSON.parse(new TextDecoder().decode(buena)) as Record<string, unknown>;
  const conAtributo = (k: string, v: unknown) => {
    const { data, ...resto } = objeto();
    return new TextEncoder().encode(JSON.stringify({ ...resto, [k]: v, data }));
  };

  test("CONTROL: el evento bueno parsea", () => assert.equal(codigo(buena), "ok"));
  test("un BOM al principio y un byte que no es UTF-8 son MALFORMED_JSON (Node los aceptaba: `TextDecoder` come el BOM y sustituye lo inválido)", () => {
    assert.equal(codigo(new Uint8Array([0xef, 0xbb, 0xbf, ...buena])), "MALFORMED_JSON");
    const i = buena.indexOf(0x70); // una «p» cualquiera
    assert.equal(codigo(new Uint8Array([...buena.slice(0, i), 0xff, ...buena.slice(i)])), "MALFORMED_JSON");
    assert.equal(codigo(new Uint8Array([0xc3, 0x28])), "MALFORMED_JSON");
  });
  test("un atributo NO obligatorio con el tipo equivocado es INVALID_ATTRIBUTE_TYPE: subject, causationid, traceparent, partitionkey, dlq*", () => {
    for (const [k, v] of [["subject", 5], ["causationid", 7], ["traceparent", 5], ["tracestate", {}], ["partitionkey", 5], ["dataschema", 5], ["dlqattempts", "1"], ["dlqattempts", 1.5], ["dlqattempts", -1], ["dlqreason", "x"], ["dlqconsumer", 5], ["dlqerror", 5], ["dlqtime", 5], ["signkeyid", 5], ["signature", 5]] as const) {
      assert.equal(codigo(conAtributo(k, v)), "INVALID_ATTRIBUTE_TYPE", `${k}=${JSON.stringify(v)}`);
    }
    // CONTROL: `null` es «ausente» y los valores bien tipados pasan
    for (const [k, v] of [["causationid", null], ["dlqattempts", 3], ["dlqreason", "poison"], ["dlqattempts", 4294967295]] as const) assert.equal(codigo(conAtributo(k, v)), "ok", k);
  });
  test("el orden: lo obligatorio gana a lo opcional (un `tenantid: 42` sigue siendo WRONG_ATTRIBUTE_TYPE aunque `subject` también esté mal)", () => {
    const o = { ...objeto(), tenantid: 42, subject: 5 };
    assert.equal(codigo(new TextEncoder().encode(JSON.stringify(o))), "WRONG_ATTRIBUTE_TYPE");
  });
});
