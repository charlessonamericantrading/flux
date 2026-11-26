/**
 * El `id` determinista de un hecho importado — specification/01-envelope.md §2.6.1.
 *
 * Los vectores salen de `conformance/cases/cross-sdk-external-id.json` (calculados con la fórmula de la spec y comprobados con
 * Python, no con este SDK). Las pruebas deben poder FALLAR: un `id` que no sea determinista, que no sea un UUIDv7 válido o que no
 * dependa de la clave, del `source` o del instante no pasa ninguna de ellas.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { validate as uuidValidate, version as uuidVersion } from "uuid";

import { EnvelopeError } from "../src/envelope.js";
import { EXTERNAL_KEY_MAX_BYTES, UUIDV7_MAX_MS, externalEventId, instantToMs } from "../src/external-id.js";

async function leerRepo(ruta: string): Promise<string> {
  let dir = new URL("./", import.meta.url);
  for (let i = 0; i < 8; i++) {
    try {
      return await readFile(new URL(ruta, dir), "utf8");
    } catch {
      dir = new URL("../", dir);
    }
  }
  throw new Error(`no se encontró ${ruta} subiendo desde el test`);
}
const casos = JSON.parse(await leerRepo("conformance/cases/cross-sdk-external-id.json"));

const UUIDV7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const T = "2026-08-20T10:25:39.412Z";
const S = "/produccion/pasarela-web";

describe("los vectores de conformance/cases/cross-sdk-external-id.json", () => {
  for (const f of casos.fixtures) {
    test(f.title, () => {
      assert.equal(externalEventId(f.input), f.expected.id);
    });
  }

  for (const f of casos.invalid) {
    test(`rechaza: ${f.title}`, () => {
      const k = f.input.externalKey;
      const clave = k !== null && typeof k === "object" ? String(k.repeat).repeat(k.times) : k;
      assert.throws(() => externalEventId({ ...f.input, externalKey: clave }), EnvelopeError);
    });
  }

  test("el fichero tiene los casos que dice (si alguien borra uno, esta prueba falla)", () => {
    assert.equal(casos.fixtures.length, 6);
    assert.equal(casos.invalid.length, 5);
  });
});

describe("propiedades", () => {
  test("es un UUIDv7 válido: versión 7, variante 10, y lo acepta una librería de UUID", () => {
    const id = externalEventId({ source: S, externalKey: "k", time: T });
    assert.match(id, UUIDV7);
    assert.equal(uuidValidate(id), true);
    assert.equal(uuidVersion(id), 7);
  });

  test("es siempre el mismo", () => {
    const a = externalEventId({ source: S, externalKey: "pasarela:evt_1", time: T });
    for (let i = 0; i < 50; i++) assert.equal(externalEventId({ source: S, externalKey: "pasarela:evt_1", time: T }), a);
  });

  test("100 000 claves distintas dan 100 000 ids distintos y todos son UUIDv7 válidos", () => {
    const vistos = new Set<string>();
    for (let i = 0; i < 100_000; i++) {
      const id = externalEventId({ source: S, externalKey: `clave-${i}`, time: T });
      assert.match(id, UUIDV7);
      vistos.add(id);
    }
    assert.equal(vistos.size, 100_000);
  });

  test("depende de la clave, del source y del instante", () => {
    const base = externalEventId({ source: S, externalKey: "k", time: T });
    assert.notEqual(externalEventId({ source: S, externalKey: "k2", time: T }), base);
    assert.notEqual(externalEventId({ source: "/produccion/otra", externalKey: "k", time: T }), base);
    assert.notEqual(externalEventId({ source: S, externalKey: "k", time: "2026-08-20T10:25:39.413Z" }), base);
  });

  test("los 12 primeros dígitos son los milisegundos del instante, y ordenan como él", () => {
    const ms = Date.parse(T);
    const id = externalEventId({ source: S, externalKey: "k", time: T });
    assert.equal(parseInt(id.replace(/-/g, "").slice(0, 12), 16), ms);
    const despues = externalEventId({ source: S, externalKey: "k", time: new Date(ms + 1000) });
    assert.ok(id < despues, "ordenar por id es ordenar por el instante del hecho (01-envelope.md §2.6)");
  });

  test("un instante da el mismo id como RFC 3339, como Date y como milisegundos", () => {
    const ms = Date.parse(T);
    const a = externalEventId({ source: S, externalKey: "k", time: T });
    assert.equal(externalEventId({ source: S, externalKey: "k", time: new Date(ms) }), a);
    assert.equal(externalEventId({ source: S, externalKey: "k", time: ms }), a);
  });

  test("la clave es UTF-8 literal: ñ es el mismo id con independencia de cómo se escriba el fichero", () => {
    const a = externalEventId({ source: S, externalKey: "pedido-ñandú", time: T });
    assert.equal(externalEventId({ source: S, externalKey: "pedido-ñandú", time: T }), a);
    // pero NO es la forma descompuesta (NFD): son bytes distintos y no se normaliza en silencio
    assert.notEqual(externalEventId({ source: S, externalKey: "pedido-ñandú", time: T }), a);
  });

  test("los límites: 512 bytes caben y 513 no; el instante 0 y 2^48-1 caben", () => {
    assert.match(externalEventId({ source: S, externalKey: "k".repeat(EXTERNAL_KEY_MAX_BYTES), time: T }), UUIDV7);
    assert.throws(() => externalEventId({ source: S, externalKey: "k".repeat(EXTERNAL_KEY_MAX_BYTES + 1), time: T }), EnvelopeError);
    // el límite es en BYTES: 257 `ñ` son 514 bytes
    assert.throws(() => externalEventId({ source: S, externalKey: "ñ".repeat(257), time: T }), EnvelopeError);
    assert.match(externalEventId({ source: S, externalKey: "k", time: 0 }), UUIDV7);
    assert.equal(externalEventId({ source: S, externalKey: "k", time: UUIDV7_MAX_MS }).slice(0, 13), "ffffffff-ffff");
    assert.throws(() => externalEventId({ source: S, externalKey: "k", time: UUIDV7_MAX_MS + 1 }), EnvelopeError);
    assert.throws(() => externalEventId({ source: S, externalKey: "k", time: -1 }), EnvelopeError);
    assert.throws(() => externalEventId({ source: S, externalKey: "k", time: 1.5 }), EnvelopeError);
  });

  test("instantToMs rechaza lo que no es un instante", () => {
    for (const malo of ["", "ayer", "2026-13-45T00:00:00Z", Number.NaN, undefined as never]) {
      assert.throws(() => instantToMs(malo), EnvelopeError);
    }
  });

  test("los mensajes de error dicen cómo se arregla y no citan la clave", () => {
    assert.throws(() => externalEventId({ source: S, externalKey: "", time: T }), /no vacía/);
    assert.throws(() => externalEventId({ source: S, externalKey: "k", time: "ayer" }), /instante válido/);
    try {
      externalEventId({ source: S, externalKey: "secreto-que-no-debe-salir", time: "ayer" });
    } catch (e) {
      assert.doesNotMatch((e as Error).message, /secreto-que-no-debe-salir/);
    }
  });
});
