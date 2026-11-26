/**
 * La autenticación por token y el límite por token — specification/12-edge.md §6.2 y §7.1. Sin broker.
 *
 * Lo que se prueba, en los dos sentidos: el gateway guarda SOLO el SHA-256; compara en TIEMPO CONSTANTE (con
 * `crypto.timingSafeEqual` de los digests, sobre TODAS las entradas, sin salir en la primera); un token ausente y uno desconocido dan lo
 * mismo; el token de un cliente no es el de otro. `mutantes.mjs` cambia la comparación por `===` y exige que esto falle.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";

import { cargar } from "./helpers/fuente.mjs";

const { crearAutenticador, nuevoToken, sha256Hex } = await cargar("auth");
const { LimitePorClave } = await cargar("limite");

const tokenA = nuevoToken();
const tokenB = nuevoToken();
const tokenC = nuevoToken();
const tabla = [
  { tokenId: "tienda-web-1", sha256: sha256Hex(tokenA), cliente: "tienda-web" },
  { tokenId: "catalogo-web-1", sha256: sha256Hex(tokenB), cliente: "catalogo-web" },
  { tokenId: "tienda-web-2", sha256: sha256Hex(tokenC), cliente: "tienda-web" },
];

describe("autenticar", () => {
  const { autenticar } = crearAutenticador(tabla);

  test("un token es de UN cliente, y se sabe cuál (y cuál de sus tokens)", () => {
    assert.deepEqual(autenticar(`Bearer ${tokenA}`), { cliente: "tienda-web", tokenId: "tienda-web-1" });
    assert.deepEqual(autenticar(`Bearer ${tokenB}`), { cliente: "catalogo-web", tokenId: "catalogo-web-1" });
    assert.deepEqual(autenticar(`Bearer ${tokenC}`), { cliente: "tienda-web", tokenId: "tienda-web-2" }, "varios tokens por cliente: rotación sin corte");
    assert.equal(autenticar(`bearer ${tokenA}`)?.cliente, "tienda-web", "el esquema no distingue mayúsculas (RFC 7235)");
  });

  test("un token ausente, mal formado o desconocido da LO MISMO: null", () => {
    const desconocido = nuevoToken();
    for (const mal of [undefined, "", "Bearer", "Bearer ", `Basic ${tokenA}`, tokenA, `Bearer ${desconocido}`, `Bearer ${tokenA}x`, `Bearer ${tokenA.slice(0, -1)}`, `Bearer ${tokenA.toUpperCase()}`, "Bearer corto", `Bearer ${"a".repeat(600)}`, `Bearer ${tokenA} ${tokenB}`]) {
      assert.equal(autenticar(mal), null, String(mal).slice(0, 40));
    }
  });

  test("el SHA-256 de un token es el de la configuración, y el token NO es el digest", () => {
    assert.match(sha256Hex(tokenA), /^[0-9a-f]{64}$/);
    assert.equal(autenticar(`Bearer ${sha256Hex(tokenA)}`), null, "presentar el digest (lo que está en gateway.json) no autentica");
  });

  test("rechaza una tabla con un digest que no es SHA-256", () => {
    assert.throws(() => crearAutenticador([{ tokenId: "x", sha256: tokenA, cliente: "c" }]), /SHA-256/);
    assert.throws(() => crearAutenticador([{ tokenId: "x", sha256: "abc", cliente: "c" }]), /SHA-256/);
  });

  test("compara en TIEMPO CONSTANTE: con `crypto.timingSafeEqual` de los digests (32 bytes), una vez por entrada, SIN salir en la primera (A-154)", (t) => {
    const espia = t.mock.method(crypto, "timingSafeEqual");
    assert.equal(autenticar(`Bearer ${tokenA}`)?.tokenId, "tienda-web-1"); // acierta en la PRIMERA entrada
    assert.equal(espia.mock.callCount(), tabla.length, "recorre todas las entradas aunque acierte la primera: el tiempo no dice cuál es ni cuántas hay");
    for (const c of espia.mock.calls) {
      assert.equal(c.arguments[0].length, 32, "compara digests de 32 bytes (con longitudes distintas timingSafeEqual lanzaría)");
      assert.equal(c.arguments[1].length, 32);
    }
    const antes = espia.mock.callCount();
    assert.equal(autenticar(`Bearer ${nuevoToken()}`), null);
    assert.equal(espia.mock.callCount() - antes, tabla.length, "un token desconocido hace la misma cuenta");
    const antes2 = espia.mock.callCount();
    assert.equal(autenticar(undefined), null);
    assert.equal(espia.mock.callCount() - antes2, tabla.length, "y uno ausente también: el tiempo no dice si había cabecera");
  });

  test("no guarda el token en ningún sitio: la tabla solo tiene digests", () => {
    const a = crearAutenticador(tabla);
    const volcado = JSON.stringify(a, (_, v) => (Buffer.isBuffer(v) ? v.toString("hex") : v)) + JSON.stringify(tabla);
    for (const t of [tokenA, tokenB, tokenC]) assert.ok(!volcado.includes(t), "el token en claro no está en ninguna estructura");
  });

  test("los tokens nuevos tienen 32 bytes de entropía y son todos distintos", () => {
    const vistos = new Set(Array.from({ length: 2000 }, () => nuevoToken()));
    assert.equal(vistos.size, 2000);
    for (const t of [...vistos].slice(0, 20)) {
      assert.match(t, /^flx_[A-Za-z0-9_-]{43}$/);
      assert.equal(Buffer.from(t.slice(4), "base64url").length, 32);
    }
  });
});

describe("el límite por token (cubo de tokens)", () => {
  /** Un reloj que se mueve a mano: sin esperas reales ni cargas que lo hagan inestable. */
  const reloj = () => {
    let ms = 1000;
    const f = () => ms;
    f.avanza = (d) => { ms += d; };
    return f;
  };

  test("deja pasar la ráfaga, rechaza el resto con `retryAfterMs`, y se rellena con el tiempo", () => {
    const r = reloj();
    const l = new LimitePorClave({ requests: 5, perSeconds: 10 }, r);
    for (let i = 0; i < 5; i++) assert.equal(l.tomar("t1").ok, true, `petición ${i + 1}`);
    const sexta = l.tomar("t1");
    assert.equal(sexta.ok, false);
    assert.equal(sexta.retryAfterMs, 2000, "5 por 10 s = una cada 2 s");
    r.avanza(1999);
    assert.equal(l.tomar("t1").ok, false, "aún no");
    r.avanza(1);
    assert.equal(l.tomar("t1").ok, true, "a los 2 s, una más");
    assert.equal(l.tomar("t1").ok, false);
  });

  test("cada token tiene su cubo: agotar uno no afecta a otro", () => {
    const l = new LimitePorClave({ requests: 2, perSeconds: 60 }, reloj());
    assert.equal(l.tomar("a").ok && l.tomar("a").ok, true);
    assert.equal(l.tomar("a").ok, false);
    assert.equal(l.tomar("b").ok, true);
  });

  test("no acumula más que la ráfaga: una hora parado no da una hora de peticiones", () => {
    const r = reloj();
    const l = new LimitePorClave({ requests: 3, perSeconds: 1, burst: 4 }, r);
    r.avanza(3_600_000);
    let ok = 0;
    for (let i = 0; i < 20; i++) if (l.tomar("x").ok) ok++;
    assert.equal(ok, 4);
  });
});
