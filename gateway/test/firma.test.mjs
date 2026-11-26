/**
 * `Flux-Signature` — specification/12-edge.md §4.2 (NORMATIVO) y A-154/A-155.
 *
 * Los vectores salen de `protocol.json` → `edge.delivery.signature.vectors`, que `check-protocol-json.mjs` RECALCULA (con su propio
 * HMAC) y que se comprobaron con `openssl dgst -sha256 -hmac` y con Python `hmac`: lo que firma el gateway NO se verifica contra sí
 * mismo. Cada garantía tiene su prueba en los DOS sentidos (lo correcto pasa y lo incorrecto falla); `mutantes.mjs` estropea
 * `firma.mjs` y exige que estas pruebas lo detecten.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";

import { cargar } from "./helpers/fuente.mjs";

const { firmar, verificar, verificarHmacSimple, hmacHex } = await cargar("firma");
const protocolo = JSON.parse(readFileSync(new URL("../../protocol.json", import.meta.url), "utf8"));
const sig = protocolo.edge.delivery.signature;

const SECRETO = "whsec_flux_prueba_0123456789abcdef";
const CUERPO = '{"specversion":"1.0","id":"x","data":{"almacen":"Málaga"}}';
const T = 1787221600;
const AHORA = T * 1000;

describe("los vectores de protocol.json", () => {
  for (const v of sig.vectors.filter((x) => x.t !== null)) {
    test(`firmar: ${v.name}`, () => {
      assert.equal(firmar({ secretos: [v.secret], t: v.t, cuerpo: v.body }), `t=${v.t},v1=${v.v1}`);
      assert.equal(hmacHex(v.secret, v.t, v.body), v.v1);
    });
    test(`verificar: ${v.name}`, () => {
      assert.deepEqual(verificar({ cabecera: `t=${v.t},v1=${v.v1}`, cuerpo: v.body, secretos: [v.secret], ahoraMs: v.t * 1000 }), { ok: true, t: v.t });
    });
  }

  test("el vector de GitHub: la primitiva HMAC es la de la documentación de GitHub", () => {
    const gh = sig.vectors.find((x) => x.t === null);
    assert.equal(hmacHex(gh.secret, null, gh.body), gh.v1);
    assert.equal(verificarHmacSimple({ valor: `sha256=${gh.v1}`, formato: "sha256-prefix", cuerpo: gh.body, secretos: [gh.secret] }), true);
    assert.equal(verificarHmacSimple({ valor: gh.v1, formato: "hex", cuerpo: gh.body, secretos: [gh.secret] }), true);
  });

  test("coincide con `openssl dgst -sha256 -hmac` y con Python `hmac` (implementaciones independientes) — A-155", () => {
    const mensaje = Buffer.concat([Buffer.from(`${T}.`), Buffer.from(CUERPO, "utf8")]);
    const esperado = hmacHex(SECRETO, T, CUERPO);
    const ssl = execFileSync("openssl", ["dgst", "-sha256", "-hmac", SECRETO, "-hex"], { input: mensaje }).toString().trim().split(" ").pop();
    const py = execFileSync("python3", ["-I", "-c", "import hmac,hashlib,sys; print(hmac.new(sys.argv[1].encode(), sys.stdin.buffer.read(), hashlib.sha256).hexdigest())", SECRETO], { input: mensaje }).toString().trim();
    assert.equal(ssl, esperado);
    assert.equal(py, esperado);
  });
});

describe("firmar", () => {
  test("el instante es el que se le da: `t` se renueva en cada intento y el cuerpo no cambia", () => {
    const a = firmar({ secretos: [SECRETO], t: T, cuerpo: CUERPO });
    const b = firmar({ secretos: [SECRETO], t: T + 1800, cuerpo: CUERPO });
    assert.notEqual(a, b, "otro t, otra firma");
    assert.match(a, new RegExp(sig.pattern));
    assert.match(b, new RegExp(sig.pattern));
    // las dos verifican en su momento
    assert.equal(verificar({ cabecera: a, cuerpo: CUERPO, secretos: [SECRETO], ahoraMs: AHORA }).ok, true);
    assert.equal(verificar({ cabecera: b, cuerpo: CUERPO, secretos: [SECRETO], ahoraMs: (T + 1800) * 1000 }).ok, true);
  });

  test("la firma cubre el INSTANTE: cambiar `t` sin recalcular invalida", () => {
    const original = firmar({ secretos: [SECRETO], t: T, cuerpo: CUERPO });
    const manipulada = original.replace(`t=${T}`, `t=${T + 10}`);
    assert.deepEqual(verificar({ cabecera: manipulada, cuerpo: CUERPO, secretos: [SECRETO], ahoraMs: AHORA }), { ok: false, motivo: "firma" });
  });

  test("la firma cubre el CUERPO, byte a byte: un espacio, o reserializar, invalida", () => {
    const cabecera = firmar({ secretos: [SECRETO], t: T, cuerpo: CUERPO });
    for (const otro of [`${CUERPO} `, CUERPO.replace('"id":"x"', '"id": "x"'), CUERPO.replace("Málaga", "Malaga"), JSON.stringify(JSON.parse(CUERPO)).replace("á", "\\u00e1")]) {
      assert.equal(verificar({ cabecera, cuerpo: otro, secretos: [SECRETO], ahoraMs: AHORA }).ok, false);
    }
  });

  test("con varios secretos, uno por v1 (rotación sin corte): verifica con cualquiera de los dos", () => {
    const nuevo = "whsec_flux_nueva_fedcba9876543210";
    const cabecera = firmar({ secretos: [SECRETO, nuevo], t: T, cuerpo: CUERPO });
    assert.equal(cabecera.match(/v1=/g).length, 2);
    for (const secretos of [[SECRETO], [nuevo], [nuevo, SECRETO]]) {
      assert.equal(verificar({ cabecera, cuerpo: CUERPO, secretos, ahoraMs: AHORA }).ok, true);
    }
    assert.equal(verificar({ cabecera, cuerpo: CUERPO, secretos: ["otro"], ahoraMs: AHORA }).ok, false);
    const rot = sig.rotationVector;
    assert.equal(firmar({ secretos: rot.secrets, t: rot.t, cuerpo: rot.body }), `t=${rot.t},${rot.v1.map((x) => `v1=${x}`).join(",")}`);
  });

  test("sin secretos o con un instante inválido no firma", () => {
    assert.throws(() => firmar({ secretos: [], t: T, cuerpo: "" }));
    assert.throws(() => firmar({ secretos: [SECRETO], t: 1.5, cuerpo: "" }));
    assert.throws(() => firmar({ secretos: [SECRETO], t: -1, cuerpo: "" }));
  });
});

describe("verificar: lo que un receptor rechaza", () => {
  const buena = firmar({ secretos: [SECRETO], t: T, cuerpo: CUERPO });
  const v1 = /v1=([0-9a-f]{64})/.exec(buena)[1];
  const v = (cabecera, extra = {}) => verificar({ cabecera, cuerpo: CUERPO, secretos: [SECRETO], ahoraMs: AHORA, ...extra });

  test("ausente, vacía o sin formato", () => {
    for (const c of [undefined, "", null]) assert.equal(v(c).motivo, "ausente");
    for (const c of ["basura", "t=1", `v1=${v1}`, `t=${T}`, `t=,v1=${v1}`, `t=1.5,v1=${v1}`, `t=-3,v1=${v1}`, `t=${T},t=${T},v1=${v1}`, `t=${T},=${v1}`]) assert.equal(v(c).motivo, "formato", c);
  });

  test("un hexadecimal que no es de 64 minúsculas se IGNORA, no se decodifica (A-154)", () => {
    // `Buffer.from("abc", "hex")` da un byte sin error: validar el formato ANTES de decodificar.
    for (const malo of [v1.slice(0, 63), v1.toUpperCase(), `${v1}0`, "zz".repeat(32), v1.slice(0, 62)]) {
      assert.equal(v(`t=${T},v1=${malo}`).motivo, "formato", malo);
    }
    // …pero una mala junto a una buena no impide verificar con la buena
    assert.equal(v(`t=${T},v1=${"0".repeat(63)},v1=${v1}`).ok, true);
  });

  test("una firma correcta de OTRO secreto o de OTRO cuerpo es «firma», no «tolerancia»", () => {
    assert.equal(v(firmar({ secretos: ["otro-secreto"], t: T, cuerpo: CUERPO })).motivo, "firma");
    assert.equal(v(firmar({ secretos: [SECRETO], t: T, cuerpo: "otro" })).motivo, "firma");
  });

  test("la tolerancia es de 300 s, SIMÉTRICA, y es lo último que se mira", () => {
    const en = (delta) => v(firmar({ secretos: [SECRETO], t: T + delta, cuerpo: CUERPO }));
    assert.equal(en(0).ok, true);
    assert.equal(en(-300).ok, true, "300 s por detrás: dentro");
    assert.equal(en(300).ok, true, "300 s por delante: dentro");
    assert.equal(en(-301).motivo, "tolerancia", "301 s por detrás: fuera (una captura no sirve para siempre)");
    assert.equal(en(301).motivo, "tolerancia", "301 s por delante: fuera TAMBIÉN (simétrica)");
    assert.equal(en(-86_400).motivo, "tolerancia");
    // y una firma que NO verifica no dice «tolerancia», aunque también esté fuera: no filtra nada a un atacante
    assert.equal(v(`t=${T + 86_400},v1=${"0".repeat(64)}`).motivo, "firma");
    // la tolerancia es configurable
    assert.equal(verificar({ cabecera: firmar({ secretos: [SECRETO], t: T - 3000, cuerpo: CUERPO }), cuerpo: CUERPO, secretos: [SECRETO], ahoraMs: AHORA, toleranciaSeg: 3600 }).ok, true);
  });

  test("los campos desconocidos se ignoran (`v2=…`): el esquema puede crecer", () => {
    assert.equal(v(`${buena},v2=lo-que-venga,extra=1`).ok, true);
    assert.match(sig.valid.at(-1), new RegExp(sig.pattern));
  });

  test("usa la comparación en TIEMPO CONSTANTE (`timingSafeEqual`), no `===`", async (t) => {
    const espia = t.mock.method(crypto, "timingSafeEqual");
    assert.equal(v(buena).ok, true);
    assert.ok(espia.mock.callCount() >= 1, "verificar debe comparar con crypto.timingSafeEqual");
    assert.equal(espia.mock.calls[0].arguments[0].length, 32, "compara los 32 bytes decodificados, no cadenas");
    const antes = espia.mock.callCount();
    v(`t=${T},v1=${"0".repeat(64)}`); // una que no acierta también compara
    assert.ok(espia.mock.callCount() > antes);
  });

  test("compara TODAS las parejas (secreto × v1), sin salir en la primera: el tiempo no dice cuál acertó", async (t) => {
    const espia = t.mock.method(crypto, "timingSafeEqual");
    const dos = firmar({ secretos: [SECRETO, "otro"], t: T, cuerpo: CUERPO });
    verificar({ cabecera: dos, cuerpo: CUERPO, secretos: [SECRETO, "otro"], ahoraMs: AHORA });
    assert.equal(espia.mock.callCount(), 4, "2 secretos × 2 v1");
  });
});

describe("verificarHmacSimple (webhooks de terceros)", () => {
  const cuerpo = "Hello, World!";
  const secreto = "It's a Secret to Everybody";
  const hex = "757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17";

  test("hex y sha256=hex, en los dos sentidos", () => {
    assert.equal(verificarHmacSimple({ valor: hex, formato: "hex", cuerpo, secretos: [secreto] }), true);
    assert.equal(verificarHmacSimple({ valor: `sha256=${hex}`, formato: "sha256-prefix", cuerpo, secretos: [secreto] }), true);
    assert.equal(verificarHmacSimple({ valor: hex, formato: "sha256-prefix", cuerpo, secretos: [secreto] }), false, "sin el prefijo");
    assert.equal(verificarHmacSimple({ valor: `sha256=${hex}`, formato: "hex", cuerpo, secretos: [secreto] }), false, "con un prefijo que no toca");
    assert.equal(verificarHmacSimple({ valor: hex.toUpperCase(), formato: "hex", cuerpo, secretos: [secreto] }), false, "mayúsculas");
    assert.equal(verificarHmacSimple({ valor: hex, formato: "hex", cuerpo: `${cuerpo}!`, secretos: [secreto] }), false);
    assert.equal(verificarHmacSimple({ valor: hex, formato: "hex", cuerpo, secretos: ["otro"] }), false);
    assert.equal(verificarHmacSimple({ valor: undefined, formato: "hex", cuerpo, secretos: [secreto] }), false);
    assert.equal(verificarHmacSimple({ valor: hex.slice(0, 63), formato: "hex", cuerpo, secretos: [secreto] }), false);
  });

  test("con rotación: cualquiera de los secretos", () => {
    assert.equal(verificarHmacSimple({ valor: hex, formato: "hex", cuerpo, secretos: ["nuevo", secreto] }), true);
  });
});
