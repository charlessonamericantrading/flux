/**
 * Los receptores de ejemplo (`examples/receptor.php` y `examples/receptor.py`): un tercero que verifica `Flux-Signature` en OTRO
 * lenguaje que el del gateway. Se ejecutan contra los vectores de `protocol.json` (recalculados con `openssl` y Python por
 * `check-protocol-json.mjs`) y contra firmas que produce el gateway: si el formato normativo de 12-edge.md §4.2 se pudiera leer de dos
 * maneras, aquí se vería. Sin broker.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { cargar } from "./helpers/fuente.mjs";

const { firmar } = await cargar("firma");

const AQUI = path.dirname(fileURLToPath(import.meta.url));
const EJ = path.join(AQUI, "..", "examples");
const protocolo = JSON.parse(readFileSync(path.join(AQUI, "..", "..", "protocol.json"), "utf8"));
const V = protocolo.edge.delivery.signature.vectors.filter((x) => x.t !== null);
const SECRETO = V[2].secret;
const CUERPO = V[2].body;
const T = V[2].t;

const hay = (cmd) => spawnSync(cmd, ["--version"], { encoding: "utf8" }).status === 0;

/** Ejecuta el receptor de un lenguaje con un caso en JSON por stdin y devuelve su veredicto. */
const receptores = {
  php: {
    disponible: () => hay("php"),
    verifica: ({ cabecera, cuerpo, secretos, ahora, tolerancia }) =>
      spawnSync("php", ["-r", `require "${path.join(EJ, "receptor.php")}"; $c = json_decode(stream_get_contents(STDIN), true); echo verificar_flux_signature($c["cabecera"], $c["cuerpo"], $c["secretos"], $c["ahora"], $c["tolerancia"] ?? 300);`], { input: JSON.stringify({ cabecera, cuerpo, secretos, ahora, tolerancia }), encoding: "utf8" }),
  },
  python: {
    disponible: () => hay("python3"),
    verifica: ({ cabecera, cuerpo, secretos, ahora, tolerancia }) =>
      spawnSync("python3", ["-I", "-c", `import json,sys; sys.path.insert(0, ${JSON.stringify(EJ)}); from receptor import verificar_flux_signature as v; c = json.load(sys.stdin); print(v(c["cabecera"], c["cuerpo"].encode("utf-8"), c["secretos"], c["ahora"], c.get("tolerancia") or 300), end="")`], { input: JSON.stringify({ cabecera, cuerpo, secretos, ahora, tolerancia }), encoding: "utf8" }),
  },
};

for (const [lenguaje, r] of Object.entries(receptores)) {
  describe(`receptor en ${lenguaje}`, { skip: r.disponible() ? false : `no hay ${lenguaje} en este entorno` }, () => {
    const veredicto = (caso) => {
      const x = r.verifica({ ahora: T, ...caso });
      assert.equal(x.status, 0, x.stderr);
      return x.stdout.trim();
    };

    test("los vectores de protocol.json, y lo que firma el gateway, verifican (con no-ASCII)", () => {
      for (const v of V) assert.equal(veredicto({ cabecera: `t=${v.t},v1=${v.v1}`, cuerpo: v.body, secretos: [v.secret], ahora: v.t }), "ok", v.name);
      assert.equal(veredicto({ cabecera: firmar({ secretos: [SECRETO], t: T, cuerpo: CUERPO }), cuerpo: CUERPO, secretos: [SECRETO] }), "ok");
    });

    test("la rotación: con dos v1, verifica con cualquiera de los dos secretos", () => {
      const rot = protocolo.edge.delivery.signature.rotationVector;
      const cabecera = `t=${rot.t},${rot.v1.map((x) => `v1=${x}`).join(",")}`;
      for (const secretos of [[rot.secrets[0]], [rot.secrets[1]], rot.secrets.slice().reverse()]) assert.equal(veredicto({ cabecera, cuerpo: rot.body, secretos, ahora: rot.t }), "ok");
      assert.equal(veredicto({ cabecera, cuerpo: rot.body, secretos: ["otro"], ahora: rot.t }), "firma");
    });

    test("lo que rechaza: sin cabecera, mal formada, otro secreto, cuerpo manipulado (un espacio, reserializado)", () => {
      const buena = firmar({ secretos: [SECRETO], t: T, cuerpo: CUERPO });
      assert.equal(veredicto({ cabecera: "", cuerpo: CUERPO, secretos: [SECRETO] }), "ausente");
      for (const c of ["basura", "t=1", `t=${T}`, `v1=${buena.slice(-64)}`, `t=${T},t=${T},v1=${buena.slice(-64)}`, `t=${T},v1=${buena.slice(-64)}x`, `t=${T}x,v1=${buena.slice(-64)}`, `t=-5,v1=${buena.slice(-64)}`]) {
        assert.equal(veredicto({ cabecera: c, cuerpo: CUERPO, secretos: [SECRETO] }), "formato", JSON.stringify(c));
      }
      assert.equal(veredicto({ cabecera: `t=${T},v1=${buena.slice(-63)}`, cuerpo: CUERPO, secretos: [SECRETO] }), "formato", "v1 corto");
      assert.equal(veredicto({ cabecera: `t=${T},v1=${buena.slice(-64).toUpperCase()}`, cuerpo: CUERPO, secretos: [SECRETO] }), "formato", "v1 en mayúsculas");
      assert.equal(veredicto({ cabecera: buena, cuerpo: CUERPO, secretos: ["otro-secreto"] }), "firma");
      assert.equal(veredicto({ cabecera: buena, cuerpo: `${CUERPO} `, secretos: [SECRETO] }), "firma");
      assert.equal(veredicto({ cabecera: buena, cuerpo: CUERPO.replace('"delta":-3', '"delta": -3'), secretos: [SECRETO] }), "firma", "reserializar cambia los bytes");
    });

    test("la tolerancia: 300 s dentro, 301 fuera, simétrica, y se mira DESPUÉS de la firma", () => {
      const en = (delta, ahora = T) => veredicto({ cabecera: firmar({ secretos: [SECRETO], t: T + delta, cuerpo: CUERPO }), cuerpo: CUERPO, secretos: [SECRETO], ahora });
      assert.equal(en(0), "ok");
      assert.equal(en(-300), "ok");
      assert.equal(en(300), "ok");
      assert.equal(en(-301), "tolerancia");
      assert.equal(en(301), "tolerancia", "un `t` del futuro también");
      assert.equal(veredicto({ cabecera: `t=${T + 86_400},v1=${"0".repeat(64)}`, cuerpo: CUERPO, secretos: [SECRETO] }), "firma", "una firma que no verifica no dice «caducada»");
    });
  });
}
