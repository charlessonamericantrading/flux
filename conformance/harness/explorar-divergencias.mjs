#!/usr/bin/env node
/**
 * Alimenta a los arneses de Node y de Rust con las MISMAS entradas y dice dónde difieren — la herramienta con la que se midieron las
 * divergencias que NO estaban en los vectores (conformance/harness/README.md → «Ajustes» y «Lo que no se admite»).
 *
 *   node conformance/harness/explorar-divergencias.mjs               # las ~360 entradas
 *   node conformance/harness/explorar-divergencias.mjs "time"        # solo las que contienen «time»
 *   FLUX_HARNESS_RUST=/ruta/al/conformance-harness  node …           # un binario ya compilado (si no, `cargo run` por entrada: lento)
 *
 * Cada entrada dice qué se espera de ella:
 *   · sin marca → Node y Rust DEBEN dar la MISMA salida (los mismos bytes, o el mismo código estable de rechazo);
 *   · con una marca de «lo que no se admite» (`NO[...]`) → la diferencia es de LENGUAJE y está DOCUMENTADA: no hay vector que la
 *     iguale, y esta herramienta lo comprueba en los dos sentidos: si pasa a coincidir, hay que quitar la marca (y documentar lo que
 *     cambió); si diverge sin marca, es una regresión.
 * Sale con 1 si hay una divergencia sin marca o una marca que ya no hace falta. No está en CI (necesita los dos SDKs compilados): se
 * ejecuta al tocar `data`, `time`, los límites de longitud o los constructores del sobre.
 */
import { readFileSync, existsSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const RAIZ = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const FILTRO = process.argv[2];
const registro = JSON.parse(readFileSync(join(RAIZ, "conformance", "harnesses.json"), "utf8")).harnesses;
const de = (sdk) => registro.find((h) => h.sdk === sdk);
const H = {
  node: { cmd: de("node").cmd, args: de("node").args, cwd: join(RAIZ, de("node").cwd) },
  rust: process.env.FLUX_HARNESS_RUST
    ? { cmd: process.env.FLUX_HARNESS_RUST, args: [], cwd: join(RAIZ, "sdk-rust") }
    : { cmd: de("rust").cmd, args: de("rust").args, cwd: join(RAIZ, de("rust").cwd) },
};
const run = (h, texto) => {
  const r = spawnSync(h.cmd, h.args, { cwd: h.cwd, input: texto, encoding: "utf8" });
  try { return JSON.parse(r.stdout); } catch { return { ok: false, code: "ARNES_ROTO", detail: (r.stderr ?? "").slice(0, 200) }; }
};
const dec = (r) => (r.bytes ? Buffer.from(r.bytes, "base64").toString("utf8") : r.ok === false ? `RECHAZO ${r.code}` : JSON.stringify(r));
function diff(a, b) { let i = 0; while (i < a.length && a[i] === b[i]) i++; return `byte ${i}: NODE …${a.slice(Math.max(0, i - 25), i + 50)}…\n          RUST …${b.slice(Math.max(0, i - 25), i + 50)}…`; }

const fx = JSON.parse(readFileSync(join(RAIZ, "conformance/cases/cross-sdk-rpc-envelope.json"), "utf8"));
const f = (id) => fx.fixtures.find((x) => x.id === id);

/** Las marcas de «lo que no se admite» (conformance/harness/README.md → «Lo que no se admite»). */
const NO = {
  ENTERO_GRANDE: "un entero fuera de ±(2^53−1): JavaScript lo redondea, Rust lo conserva",
  FLOTANTE: "un flotante o un exponente (`4995.00`, `1E+2`, `-0`): JavaScript lo reescribe (`4995`, `100`, `0`); Rust no toca los números (01-envelope.md §2.5)",
  CLAVE_INDICE: "una clave que es un índice de array (`\"1\"`): JavaScript las ordena primero",
  SUSTITUTO: "un sustituto UTF-16 suelto (`\\ud83d`): JavaScript lo admite; Rust lo rechaza (no es UTF-8)",
  PROFUNDIDAD: "más de 128 niveles de anidamiento: Rust lo rechaza",
  FUERA_DE_DOUBLE: "un número fuera del rango de un `double` (`1e400`): JavaScript lo lee como `Infinity`; el JSON de Rust lo rechaza",
  TIPADO: "un valor de otro tipo JSON del que pide la operación: el arnés de Rust lo rechaza al leer la entrada (`HARNESS_INPUT`, tipos cerrados); el de Node deja que el SDK lo rechace",
  VERSION_7_0: "`7.0` donde el lenguaje distingue entero de flotante (10-state.md §3.4, ya documentado)",
  SIN_CODIGO: "un rechazo sin código estable en la spec (un subject de evento mal formado, un `data` que no es objeto en un evento): cada SDK lo nombra a su manera",
  RAIZ_REPETIDA: "una clave repetida en la RAIZ de un mensaje recibido: Node (`JSON.parse`) se queda con la última; Rust la rechaza — pendiente (el hallazgo no está cerrado)",
};

let div = 0, n = 0, sinMarcaQueDivergen = 0, marcasInnecesarias = 0;
const marcadas = [];
function caso(grupo, nombre, textoEntrada, marca) {
  if (FILTRO && !`${grupo} ${nombre}`.includes(FILTRO)) return;
  n++;
  const a = run(H.node, textoEntrada), b = run(H.rust, textoEntrada);
  const x = dec(a), y = dec(b);
  const igual = x === y;
  if (igual && !marca) { console.log(`  = ${nombre}`); return; }
  if (igual && marca) { marcasInnecesarias++; console.log(`  ! ${nombre}\n      coincide, pero está marcada como «${NO[marca]}»: quita la marca (y documenta lo que cambió)`); return; }
  div++;
  const detalle = a.bytes && b.bytes ? diff(x, y) : `NODE ${x.slice(0, 120)}  |  RUST ${y.slice(0, 120)}`;
  if (marca) { marcadas.push(marca); console.log(`  ≈ ${nombre}  [NO: ${marca}]\n      ${detalle}`); return; }
  sinMarcaQueDivergen++;
  console.log(`  ≠ ${nombre}\n      ${detalle}`);
}

// ─── rpc-request ──────────────────────────────────────────────────────────────
const baseReq = JSON.stringify({ op: "rpc-request", ...f("rpc-request").input });
const conData = (crudo) => baseReq.replace(/"data":\{.*\}\}$/, `"data":${crudo}}`);
const conCampo = (clave, valorJson) => baseReq.replace(new RegExp(`"${clave}":"[^"]*"`), `"${clave}":${valorJson}`);
const G1 = "rpc-request: data";
caso(G1, "escape \\u00f1 en data", conData(String.raw`{"a":"caña"}`));

caso(G1, "entero 2^53+1", conData(`{"a":9007199254740993}`), "ENTERO_GRANDE");
caso(G1, "entero 2^53-1", conData(`{"a":9007199254740991}`));

caso(G1, "entero -2^53+1", conData(`{"a":-9007199254740991}`));

caso(G1, "entero 2^64+1", conData(`{"a":18446744073709551617}`), "ENTERO_GRANDE");
caso(G1, "decimal 4995.00", conData(`{"a":4995.00}`), "FLOTANTE");
caso(G1, "decimal 0.1", conData(`{"a":0.1}`));

caso(G1, "decimal 1.5", conData(`{"a":1.5}`));

caso(G1, "decimal 4995.5", conData(`{"a":4995.5}`));

caso(G1, "exponente 1E+2", conData(`{"a":1E+2}`), "FLOTANTE");
caso(G1, "exponente 1e-7", conData(`{"a":1e-7}`));

caso(G1, "exponente 1e21", conData(`{"a":1e21}`), "FLOTANTE");
caso(G1, "exponente 1.5e300", conData(`{"a":1.5e300}`), "FLOTANTE");
caso(G1, "-0", conData(`{"a":-0}`), "FLOTANTE");
caso(G1, "-0.0", conData(`{"a":-0.0}`), "FLOTANTE");
caso(G1, "0.0", conData(`{"a":0.0}`), "FLOTANTE");
caso(G1, "clave duplicada", conData(`{"a":1,"a":2}`));

caso(G1, "clave duplicada con otra en medio", conData(`{"a":1,"b":3,"a":2}`));

caso(G1, "orden z,a", conData(`{"z":1,"a":2}`));

caso(G1, "claves numéricas (JS las reordena)", conData(`{"b":1,"2":2,"1":3}`), "CLAVE_INDICE");
caso(G1, "clave __proto__", conData(`{"__proto__":1,"a":2}`));

caso(G1, "emoji fuera del BMP literal", conData(`{"a":"😀"}`));

caso(G1, "emoji como par sustituto escapado", conData(String.raw`{"a":"😀"}`));

caso(G1, "sustituto suelto escapado", conData(String.raw`{"a":"\ud83d"}`), "SUSTITUTO");
caso(G1, "U+2028 literal", conData(`{"a":"x y"}`));

caso(G1, "U+2028 escapado", conData(String.raw`{"a":"x y"}`));

caso(G1, "DEL y control escapados", conData(String.raw`{"a":"\u007f\u0001\n"}`));

caso(G1, "U+0000 escapado", conData(String.raw`{"a":"\u0000"}`));

caso(G1, "barra escapada \\/", conData(String.raw`{"a":"a\/b"}`));

caso(G1, "comillas y barra invertida", conData(String.raw`{"a":"\"\\"}`));

caso(G1, "< > & literales", conData(`{"a":"<b>&</b>"}`));

caso(G1, "NUL literal NFC/NFD (é compuesta y descompuesta)", conData(`{"a":"é","b":"é"}`));

caso(G1, "espacios dentro de data", conData(`{ "a" : 1 , "b" : [ 1 , 2 ] }`));

caso(G1, "saltos de línea y tabuladores", conData(`{\n\t"a":1,\n\t"b":[1,\n2]}`));

caso(G1, "data array", conData(`[1]`));

caso(G1, "data null", conData(`null`));

caso(G1, "data escalar", conData(`5`));

caso(G1, "data vacío {}", conData(`{}`));

caso(G1, "data anidado profundo (200)", conData(`${"{\"a\":".repeat(200)}1${"}".repeat(200)}`), "PROFUNDIDAD");
caso(G1, "data con arrays anidados", conData(`{"a":[[1,2],[3,[4,{"b":null}]]]}`));

caso(G1, "data con null y booleanos", conData(`{"a":null,"b":true,"c":false}`));


const G2 = "rpc-request: atributos";
caso(G2, "time sin milis", conCampo("time", '"2026-08-20T10:41:07Z"'));

caso(G2, "time con 1 decimal", conCampo("time", '"2026-08-20T10:41:07.3Z"'));

caso(G2, "time con 2 decimales", conCampo("time", '"2026-08-20T10:41:07.31Z"'));

caso(G2, "time con microsegundos", conCampo("time", '"2026-08-20T10:41:07.318927Z"'));

caso(G2, "time con nanosegundos", conCampo("time", '"2026-08-20T10:41:07.318927123Z"'));

caso(G2, "time con 10 decimales", conCampo("time", '"2026-08-20T10:41:07.3189271234Z"'));

caso(G2, "time con .9999 (redondeo/truncado)", conCampo("time", '"2026-08-20T10:41:07.9999Z"'));

caso(G2, "time con +02:00", conCampo("time", '"2026-08-20T12:41:07.318+02:00"'));

caso(G2, "time con -05:30", conCampo("time", '"2026-08-20T05:11:07.318-05:30"'));

caso(G2, "time con +00:00", conCampo("time", '"2026-08-20T10:41:07.318+00:00"'));

caso(G2, "time con -00:00", conCampo("time", '"2026-08-20T10:41:07.318-00:00"'));

caso(G2, "time con z minúscula", conCampo("time", '"2026-08-20T10:41:07.318z"'));

caso(G2, "time con t minúscula", conCampo("time", '"2026-08-20t10:41:07.318Z"'));

caso(G2, "time con espacio en vez de T", conCampo("time", '"2026-08-20 10:41:07.318Z"'));

caso(G2, "time sin zona", conCampo("time", '"2026-08-20T10:41:07.318"'));

caso(G2, "time solo fecha", conCampo("time", '"2026-08-20"'));

caso(G2, "time inválido", conCampo("time", '"ayer"'));

caso(G2, "time vacío", conCampo("time", '""'));

caso(G2, "time con mes 13", conCampo("time", '"2026-13-20T10:41:07.318Z"'));

caso(G2, "time 30 de febrero", conCampo("time", '"2026-02-30T10:41:07.318Z"'));

caso(G2, "time 29 de febrero bisiesto", conCampo("time", '"2028-02-29T10:41:07.318Z"'));

caso(G2, "time 29 de febrero NO bisiesto", conCampo("time", '"2026-02-29T10:41:07.318Z"'));

caso(G2, "time con hora 24", conCampo("time", '"2026-08-20T24:00:00.000Z"'));

caso(G2, "time con segundo 60 (salto)", conCampo("time", '"2026-06-30T23:59:60.000Z"'));

caso(G2, "time con segundo 61", conCampo("time", '"2026-08-20T10:41:61.000Z"'));

caso(G2, "time año 0000", conCampo("time", '"0000-01-01T00:00:00.000Z"'));

caso(G2, "time año 9999", conCampo("time", '"9999-12-31T23:59:59.999Z"'));

caso(G2, "time año 9999 con -01:00 (pasa a 10000)", conCampo("time", '"9999-12-31T23:59:59.999-01:00"'));

caso(G2, "time año 0000 con +01:00 (pasa a -0001)", conCampo("time", '"0000-01-01T00:30:00.000+01:00"'));

caso(G2, "time año de 5 dígitos", conCampo("time", '"12026-08-20T10:41:07.318Z"'));

caso(G2, "time con año +2026", conCampo("time", '"+2026-08-20T10:41:07.318Z"'));

caso(G2, "time con espacios alrededor", conCampo("time", '" 2026-08-20T10:41:07.318Z "'));

caso(G2, "time con offset +2 (sin minutos)", conCampo("time", '"2026-08-20T12:41:07.318+02"'));

caso(G2, "time con offset +0200 (sin dos puntos)", conCampo("time", '"2026-08-20T12:41:07.318+0200"'));

caso(G2, "time con offset +24:00", conCampo("time", '"2026-08-20T12:41:07.318+24:00"'));

caso(G2, "time con dígitos unicode (árabe-índicos)", conCampo("time", '"٢٠٢٦-08-20T10:41:07.318Z"'));

caso(G2, "time con coma decimal", conCampo("time", '"2026-08-20T10:41:07,318Z"'));

caso(G2, "time número en vez de cadena", baseReq.replace(/"time":"[^"]*"/, '"time":1787000000000'), "TIPADO");
caso(G2, "id vacío", conCampo("id", '""'));

caso(G2, "id con espacios", conCampo("id", '" "'));

caso(G2, "id no UUID", conCampo("id", '"abc"'));

caso(G2, "source vacío", conCampo("source", '""'));

caso(G2, "source sin barra", conCampo("source", '"produccion/crm"'));

caso(G2, "tenantId vacío", conCampo("tenantId", '""'));

caso(G2, "correlationid vacío", conCampo("correlationid", '""'));

caso(G2, "producerversion vacío", conCampo("producerversion", '""'));

caso(G2, "classification inválida", conCampo("classification", '"secreto"'));

caso(G2, "classification restricted", conCampo("classification", '"restricted"'));

caso(G2, "aggregateId con unicode", conCampo("aggregateId", '"cli-ñ"'));

caso(G2, "aggregateId vacío", conCampo("aggregateId", '""'));

caso(G2, "con causationid", baseReq.replace('"data"', '"causationid":"abc","data"'));

caso(G2, "con causationid vacío", baseReq.replace('"data"', '"causationid":"","data"'));

caso(G2, "con tracestate", baseReq.replace('"data"', '"tracestate":"a=b","data"'));


caso(G2, "operation con versión enorme", conCampo("operation", '"a.b.v99999999999999999999.c"'));

caso(G2, "operation v9999", conCampo("operation", '"a.b.v9999.c"'));

caso(G2, "operation v10000", conCampo("operation", '"a.b.v10000.c"'));

caso(G2, "operation v4294967296", conCampo("operation", '"a.b.v4294967296.c"'));

caso(G2, "operation v0", conCampo("operation", '"a.b.v0.c"'));

caso(G2, "operation v01", conCampo("operation", '"a.b.v01.c"'));

caso(G2, "operation con unicode", conCampo("operation", '"clíentes.cliente.v1.obtener"'));

caso(G2, "operation con salto de línea final", conCampo("operation", '"clientes.cliente.v1.obtener\\n"'));

caso(G2, "operation con prefijo rpc.", conCampo("operation", '"rpc.clientes.cliente.v1.obtener"'));

caso(G2, "operation de dominio reservado dlq", conCampo("operation", '"dlq.cliente.v1.obtener"'));

caso(G2, "operation con 3 tokens", conCampo("operation", '"clientes.cliente.v1"'));

caso(G2, "operation vacía", conCampo("operation", '""'));

caso(G2, "operation numérica", baseReq.replace(/"operation":"[^"]*"/, '"operation":5'), "TIPADO");

// ─── rpc-reply / rpc-error ────────────────────────────────────────────────────
const G3 = "rpc-reply / rpc-error";
const baseRep = f("rpc-reply").input, baseErr = f("rpc-error-handler").input;
caso(G3, "reply con partitionkey distinta", JSON.stringify({ op: "rpc-reply", ...baseRep, request: { ...baseRep.request, partitionkey: "otra" } }));

caso(G3, "reply sin subject", JSON.stringify({ op: "rpc-reply", ...baseRep, request: { operation: baseRep.request.operation, id: baseRep.request.id, correlationid: baseRep.request.correlationid, tenantid: baseRep.request.tenantid } }));

caso(G3, "reply sin traceparent", JSON.stringify({ op: "rpc-reply", ...baseRep, traceparent: undefined }));

caso(G3, "reply con data no objeto", JSON.stringify({ op: "rpc-reply", ...baseRep, data: [1] }));

caso(G3, "reply con correlationid de primer nivel distinto", JSON.stringify({ op: "rpc-reply", ...baseRep, correlationid: "OTRO" }));

caso(G3, "reply con id igual al de la petición", JSON.stringify({ op: "rpc-reply", ...baseRep, id: baseRep.request.id }));

caso(G3, "reply con request.id vacío", JSON.stringify({ op: "rpc-reply", ...baseRep, request: { ...baseRep.request, id: "" } }));

caso(G3, "reply con request.correlationid vacío", JSON.stringify({ op: "rpc-reply", ...baseRep, request: { ...baseRep.request, correlationid: "" } }));

caso(G3, "reply con request.tenantid vacío", JSON.stringify({ op: "rpc-reply", ...baseRep, request: { ...baseRep.request, tenantid: "" } }));

caso(G3, "reply con id vacío", JSON.stringify({ op: "rpc-reply", ...baseRep, id: "" }));

caso(G3, "reply con source vacío", JSON.stringify({ op: "rpc-reply", ...baseRep, source: "" }));

caso(G3, "reply con time no canónico", JSON.stringify({ op: "rpc-reply", ...baseRep, time: "2026-08-20T12:41:07.3+02:00" }));

caso(G3, "reply con classification inválida", JSON.stringify({ op: "rpc-reply", ...baseRep, classification: "x" }));

const err = (e) => JSON.stringify({ op: "rpc-error", ...baseErr, error: e });
caso(G3, "error con class poison", err({ class: "poison", code: "X", message: "m" }));

caso(G3, "error con class fatal", err({ class: "fatal", code: "X", message: "m" }));

caso(G3, "error code en minúsculas", err({ class: "permanent", code: "http_503", message: "m" }));

caso(G3, "error code vacío", err({ class: "permanent", code: "", message: "m" }));

caso(G3, "error message vacío", err({ class: "permanent", code: "X", message: "" }));

caso(G3, "error message solo espacios", err({ class: "permanent", code: "X", message: "   " }));

caso(G3, "error message de 1000", err({ class: "permanent", code: "X", message: "a".repeat(1000) }));

caso(G3, "error message de 1001", err({ class: "permanent", code: "X", message: "a".repeat(1001) }));

caso(G3, "error message de 1000 ñ", err({ class: "permanent", code: "X", message: "ñ".repeat(1000) }));

caso(G3, "error message de 1000 emojis (2000 unidades UTF-16)", err({ class: "permanent", code: "X", message: "😀".repeat(1000) }));

caso(G3, "error message de 1001 emojis", err({ class: "permanent", code: "X", message: "😀".repeat(1001) }));

caso(G3, "error message 999 emojis + 1 ñ", err({ class: "permanent", code: "X", message: "😀".repeat(999) + "ñ" }));

caso(G3, "error message con sustituto suelto", err({ class: "permanent", code: "X", message: "a\ud83db" }), "SUSTITUTO");
caso(G3, "error message con control", err({ class: "permanent", code: "X", message: "a\u0001b\n" }));

caso(G3, "error retryAfterMs con permanent", err({ class: "permanent", code: "X", message: "m", retryAfterMs: 5 }));

caso(G3, "error retryAfterMs 0", err({ class: "retryable", code: "X", message: "m", retryAfterMs: 0 }));

caso(G3, "error retryAfterMs -1", err({ class: "retryable", code: "X", message: "m", retryAfterMs: -1 }));

caso(G3, "error retryAfterMs null", err({ class: "retryable", code: "X", message: "m", retryAfterMs: null }));

caso(G3, "error retryAfterMs cadena", err({ class: "retryable", code: "X", message: "m", retryAfterMs: "5" }));

const baseErrTxt = JSON.stringify({ op: "rpc-error", ...baseErr });
caso(G3, "error retryAfterMs 1.5", baseErrTxt.replace('"retryAfterMs":5000', '"retryAfterMs":1.5'));

caso(G3, "error retryAfterMs 2^53+1", baseErrTxt.replace('"retryAfterMs":5000', '"retryAfterMs":9007199254740993'), "ENTERO_GRANDE");
caso(G3, "error retryAfterMs 5000.0", baseErrTxt.replace('"retryAfterMs":5000', '"retryAfterMs":5000.0'));

caso(G3, "error retryAfterMs 5e3", baseErrTxt.replace('"retryAfterMs":5000', '"retryAfterMs":5e3'));

caso(G3, "error code de 64", err({ class: "permanent", code: "A".repeat(64), message: "m" }));

caso(G3, "error code de 65", err({ class: "permanent", code: "A".repeat(65), message: "m" }));

caso(G3, "error code con dígito inicial", err({ class: "permanent", code: "1A", message: "m" }));

caso(G3, "error code con doble guion bajo", err({ class: "permanent", code: "A__B", message: "m" }));

caso(G3, "error code con guion bajo final", err({ class: "permanent", code: "A_", message: "m" }));

caso(G3, "error code con unicode", err({ class: "permanent", code: "ÑA", message: "m" }));

caso(G3, "error con clave de más", err({ class: "permanent", code: "X", message: "m", otra: 1 }), "TIPADO");
caso(G3, "error sin class", err({ code: "X", message: "m" }), "TIPADO");
caso(G3, "error no objeto", err("x"), "TIPADO");
caso(G3, "error con code no cadena", err({ class: "permanent", code: 5, message: "m" }), "TIPADO");
caso(G3, "error con time inválido", JSON.stringify({ op: "rpc-error", ...baseErr, time: "ayer" }));

caso(G3, "error con id vacío", JSON.stringify({ op: "rpc-error", ...baseErr, id: "" }));

caso(G3, "error con source vacío", JSON.stringify({ op: "rpc-error", ...baseErr, source: "" }));


// ─── rpc-timeout / rpc-operation ──────────────────────────────────────────────
const G4 = "rpc-timeout / rpc-operation";
for (const t of ["5000.0", "2e3", "-0", "0.0", "25000.0", "25000.5", "25001", "1e0", "9007199254740993", "true", "[]", "{}", '"5000"', "null", "0", "-1", "1", "1.5", "0.5", "1e5"]) {
  caso(G4, `timeoutMs ${t}`, `{"op":"rpc-timeout","timeoutMs":${t}}`);
}
caso(G4, "timeoutMs 1e400", `{"op":"rpc-timeout","timeoutMs":1e400}`, "FUERA_DE_DOUBLE");
caso(G4, "timeoutMs -1e400", `{"op":"rpc-timeout","timeoutMs":-1e400}`, "FUERA_DE_DOUBLE");
caso(G4, "rpc-timeout sin clave", `{"op":"rpc-timeout"}`);

caso(G4, "rpc-operation sin operation", `{"op":"rpc-operation"}`);

caso(G4, "rpc-operation operation numérica", `{"op":"rpc-operation","operation":5}`);

caso(G4, "rpc-operation operation null", `{"op":"rpc-operation","operation":null}`);

caso(G4, "rpc-operation con NBSP final", `{"op":"rpc-operation","operation":"clientes.cliente.v1.obtener "}`);

for (const o of ["a.b.v9999.c", "a.b.v10000.c", "a.b.v99999999999999999999.c", "a.b.v4294967296.c", "a.b.v1.c", "a.b.v09.c", "a.b.V1.c", "A.b.v1.c", "a.b.v1.c-", "a.b.v1.-c", "a.b.v1.c--d", "a-.b.v1.c", "a.b.v1.", ".b.v1.c", "a.b.v1.c.d", "a.b.v1.ñ", "rpc.a.b.v1.c", "state.a.b.v1.c", "system.a.b.v1.c", "dlq.a.b.v1.c", "rpcs.a.b.v1.c", "a.b.v1.*", "a.b.v1.>", "a.b.v1.c\n", "a.b.v1.c\r\n", "a.b.v1.c\u0000", "a.b.v1.c\u0085", "a.b.v1.c ", "a.b.v1.c ", "ａ.b.v1.c", "a.b.v٣.c", "a.b.v1.c\ud83d"]) {
  caso(G4, `rpc-operation ${JSON.stringify(o)}`, JSON.stringify({ op: "rpc-operation", operation: o }), /[\ud800-\udfff]/.test(o) && !/[\ud800-\udbff][\udc00-\udfff]/.test(o) ? "SUSTITUTO" : undefined);
}

// ─── Estado ───────────────────────────────────────────────────────────────────
const G5 = "estado";
const base = {
  contract: "clientes.cliente.v1", tenantId: "acme", key: "cli-1",
  id: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55", source: "/produccion/crm-api",
  time: "2026-08-20T10:25:39.412Z", correlationid: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  producerversion: "2.1.0", classification: "internal", data: { clienteId: "cli-1", aggregateVersion: 1 },
};
const D = (error) => JSON.stringify({
  op: "state-dlq", original: base, revision: 41,
  dlq: { reason: "permanent", attempts: 1, consumer: "tienda-web__state_clientes_cliente_v1__acme", error, dlqtime: "2026-08-20T10:25:40.117Z" },
});
const S = (campos) => JSON.stringify({ op: "state-build", ...base, ...campos });
caso(G5, "dlqerror de 1500 × é", D("é".repeat(1500)));

caso(G5, "dlqerror 1023 a + é (justo en el corte)", D("a".repeat(1023) + "é"));

caso(G5, "dlqerror 1024 a", D("a".repeat(1024)));

caso(G5, "dlqerror 1025 a", D("a".repeat(1025)));

caso(G5, "dlqerror 1022 a + € (3 bytes)", D("a".repeat(1022) + "€"));

caso(G5, "dlqerror 1021 a + 😀 (4 bytes) + b", D("a".repeat(1021) + "😀b"));

caso(G5, "dlqerror 1022 a + 😀", D("a".repeat(1022) + "😀"));

caso(G5, "dlqerror 300 × 😀", D("😀".repeat(300)));

caso(G5, "dlqerror con sustituto suelto", D("a\ud83db"), "SUSTITUTO");
caso(G5, "dlqerror vacío", D(""));

caso(G5, "dlqerror con controles", D("a\u0001\n\tb"));

caso(G5, "dlqerror con U+2028 y U+007F", D("a \u007fb"));

caso(G5, "contrato major 2^32", S({ contract: "clientes.cliente.v4294967296" }));

caso(G5, "contrato major 10^20 - 1", S({ contract: "clientes.cliente.v99999999999999999999" }));

caso(G5, "contrato major 9999", S({ contract: "clientes.cliente.v9999" }));

caso(G5, "contrato major 10000", S({ contract: "clientes.cliente.v10000" }));

caso(G5, "contrato major 0", S({ contract: "clientes.cliente.v0" }));

caso(G5, "contrato mal formado (2 tokens)", S({ contract: "clientes.cliente" }));

caso(G5, "contrato mayúsculas", S({ contract: "Clientes.cliente.v1" }));

caso(G5, "id vacío", S({ id: "" }));

caso(G5, "source vacío", S({ source: "" }));

caso(G5, "correlationid vacío", S({ correlationid: "" }));

caso(G5, "producerversion vacío", S({ producerversion: "" }));

caso(G5, "time con 4 decimales", S({ time: "2026-08-20T10:25:39.4123Z" }));

caso(G5, "time sin decimales", S({ time: "2026-08-20T10:25:39Z" }));

caso(G5, "time con +02:00", S({ time: "2026-08-20T12:25:39.412+02:00" }));

caso(G5, "time inválido", S({ time: "ayer" }));

caso(G5, "aggregateVersion 2^53", S({ data: { aggregateVersion: 9007199254740992 } }));

caso(G5, "aggregateVersion 2^53-1", S({ data: { aggregateVersion: 9007199254740991 } }));

caso(G5, "aggregateVersion 7.0", S({ data: { aggregateVersion: 7.0 } }));

caso(G5, "aggregateVersion 1e0", JSON.stringify({ op: "state-build", ...base }).replace('"aggregateVersion":1', '"aggregateVersion":1e0'), "VERSION_7_0");
caso(G5, "aggregateVersion 7.0 (texto)", JSON.stringify({ op: "state-build", ...base }).replace('"aggregateVersion":1', '"aggregateVersion":7.0'), "VERSION_7_0");
caso(G5, "aggregateVersion 9007199254740993 (texto)", JSON.stringify({ op: "state-build", ...base }).replace('"aggregateVersion":1', '"aggregateVersion":9007199254740993'));

caso(G5, "aggregateVersion 0", S({ data: { aggregateVersion: 0 } }));

caso(G5, "aggregateVersion cadena", S({ data: { aggregateVersion: "1" } }));

caso(G5, "data con espacios (estado)", JSON.stringify({ op: "state-build", ...base }).replace(/"data":\{.*\}\}$/, '"data":{ "clienteId" : "cli-1" , "aggregateVersion" : 1 }}'));

caso(G5, "state-tombstone aggregateVersion 2^53", JSON.stringify({ op: "state-tombstone", contract: base.contract, tenantId: "acme", key: "cli-1", id: base.id, source: base.source, time: base.time, correlationid: base.correlationid, producerversion: "2.1.0", aggregateVersion: 9007199254740992 }));

caso(G5, "state-tombstone time no canónico", JSON.stringify({ op: "state-tombstone", contract: base.contract, tenantId: "acme", key: "cli-1", id: base.id, source: base.source, time: "2026-08-20T12:25:39+02:00", correlationid: base.correlationid, producerversion: "2.1.0", aggregateVersion: 3 }));

caso(G5, "state-tombstone id vacío", JSON.stringify({ op: "state-tombstone", contract: base.contract, tenantId: "acme", key: "cli-1", id: "", source: base.source, time: base.time, correlationid: base.correlationid, producerversion: "2.1.0", aggregateVersion: 3 }));

for (const c of ["clientes.cliente.v1", "clientes.cliente.v9999", "clientes.cliente.v10000", "clientes.cliente.v4294967296", "clientes.cliente.v99999999999999999999", "x.y.v1\n"]) {
  caso(G5, `state-subject ${JSON.stringify(c)}`, JSON.stringify({ op: "state-subject", contract: c, tenantId: "acme", key: "cli-1" }));
}
for (const t of ["acme", "a".repeat(64), "a".repeat(65), "Acme", "ac me", "ñ"]) {
  caso(G5, `state-subject tenant ${t.length > 20 ? t.length + " letras" : JSON.stringify(t)}`, JSON.stringify({ op: "state-subject", contract: "clientes.cliente.v1", tenantId: t, key: "cli-1" }));
}
for (const k of ["a".repeat(128), "a".repeat(129), "_a", "a.b", "a\n", "ñ", "a b", "*", ">", ""]) {
  caso(G5, `state-subject key ${k.length > 20 ? k.length + " letras" : JSON.stringify(k)}`, JSON.stringify({ op: "state-subject", contract: "clientes.cliente.v1", tenantId: "acme", key: k }));
}

// ─── Eventos (ops generales) ──────────────────────────────────────────────────
const G6 = "eventos";
const ev0 = { subject: "pedidos.pedido.v1.creado", id: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55", source: "/produccion/pedidos-api", time: "2025-08-20T10:25:39.410Z", dataschema: "https://schemas.internal/pedidos/pedido/creado/1.0.0.json", correlationid: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55", tenantid: "acme", producerversion: "3.4.1", dataclassification: "internal", aggregateId: "ped-123", data: { pedidoId: "ped-123" } };
const B = (campos) => JSON.stringify({ op: "build", event: { ...ev0, ...campos } });
caso(G6, "build base", B({}));

caso(G6, "build id vacío", B({ id: "" }));

caso(G6, "build source vacío", B({ source: "" }));

caso(G6, "build dataschema vacío", B({ dataschema: "" }));

caso(G6, "build correlationid vacío", B({ correlationid: "" }));

caso(G6, "build tenantid vacío", B({ tenantid: "" }));

caso(G6, "build producerversion vacío", B({ producerversion: "" }));

caso(G6, "build time sin milis", B({ time: "2025-08-20T10:25:39Z" }));

caso(G6, "build time +02:00", B({ time: "2025-08-20T12:25:39.410+02:00" }));

caso(G6, "build time inválido", B({ time: "ayer" }));

caso(G6, "build subject v9999", B({ subject: "pedidos.pedido.v9999.creado" }));

caso(G6, "build subject v10000", B({ subject: "pedidos.pedido.v10000.creado" }), "SIN_CODIGO");
caso(G6, "build subject v99999999999999999999", B({ subject: "pedidos.pedido.v99999999999999999999.creado" }), "SIN_CODIGO");
caso(G6, "build subject v4294967296", B({ subject: "pedidos.pedido.v4294967296.creado" }), "SIN_CODIGO");
caso(G6, "build subject con dominio reservado rpc", B({ subject: "rpc.pedido.v1.creado" }));

caso(G6, "build subject con prefijo state", B({ subject: "state.pedido.v1.creado" }));

caso(G6, "build clasificación inválida", B({ dataclassification: "x" }));

caso(G6, "build aggregateId vacío", B({ aggregateId: "" }));

caso(G6, "build partitionkey distinta", B({ partitionkey: "otra" }));

caso(G6, "build data array", B({ data: [1] }), "SIN_CODIGO");
caso(G6, "build data null", B({ data: null }), "SIN_CODIGO");
const Bt = JSON.stringify({ op: "build", event: ev0 });
caso(G6, "build data 4995.00", Bt.replace('"data":{"pedidoId":"ped-123"}', '"data":{"a":4995.00}'), "FLOTANTE");
caso(G6, "build data espacios", Bt.replace('"data":{"pedidoId":"ped-123"}', '"data":{ "a" : 1 }'));

caso(G6, "build data \\u007f", Bt.replace('"data":{"pedidoId":"ped-123"}', '"data":{"a":"\\u007f"}'));

caso(G6, "build data duplicada", Bt.replace('"data":{"pedidoId":"ped-123"}', '"data":{"a":1,"a":2}'));

const dlq0 = { reason: "permanent", attempts: 1, consumer: "facturacion-api__pedidos_pedido_v1_creado", error: "PEDIDO_YA_CANCELADO", dlqtime: "2025-08-20T10:25:40.117Z" };
const DQ = (e) => JSON.stringify({ op: "dlq", event: ev0, dlq: { ...dlq0, error: e } });
caso(G6, "dlq error 1500 é", DQ("é".repeat(1500)));

caso(G6, "dlq error 1023 a + é", DQ("a".repeat(1023) + "é"));

caso(G6, "dlq error 1021 a + 😀 b", DQ("a".repeat(1021) + "😀b"));

caso(G6, "dlq error 300 😀", DQ("😀".repeat(300)));

caso(G6, "dlq attempts 0", JSON.stringify({ op: "dlq", event: ev0, dlq: { ...dlq0, attempts: 0 } }));

caso(G6, "dlq dlqtime no canónico", JSON.stringify({ op: "dlq", event: ev0, dlq: { ...dlq0, dlqtime: "2025-08-20T10:25:40Z" } }));

caso(G6, "dlq consumer vacío", JSON.stringify({ op: "dlq", event: ev0, dlq: { ...dlq0, consumer: "" } }));


// ─── parse (consumo) ──────────────────────────────────────────────────────────
const G7 = "parse";
const baseBytes = (() => { const r = run(H.node, B({})); return Buffer.from(r.bytes, "base64"); })();
const P = (buf) => JSON.stringify({ op: "parse", bytes: Buffer.from(buf).toString("base64") });
const objBase = JSON.parse(baseBytes.toString("utf8"));
const Pj = (mut) => P(Buffer.from(JSON.stringify(mut(structuredClone(objBase))), "utf8"));
caso(G7, "base", P(baseBytes));

caso(G7, "con BOM", P(Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), baseBytes])));

caso(G7, "UTF-8 inválido dentro de data", P(Buffer.from(baseBytes.toString("utf8").replace("ped-123", "ped-\u0001").replace("\u0001", "ÿ"), "latin1")));

caso(G7, "UTF-8 inválido (0xff) en un valor", P(Buffer.concat([baseBytes.subarray(0, baseBytes.indexOf("ped-123") + 4), Buffer.from([0xff]), baseBytes.subarray(baseBytes.indexOf("ped-123") + 4)])));

caso(G7, "espacios y salto al final", P(Buffer.concat([baseBytes, Buffer.from(" \n")])));

caso(G7, "basura al final", P(Buffer.concat([baseBytes, Buffer.from("x")])));

caso(G7, "dos objetos", P(Buffer.concat([baseBytes, baseBytes])));

caso(G7, "vacío", P(Buffer.alloc(0)));

caso(G7, "solo espacios", P(Buffer.from("   ")));

caso(G7, "null", P(Buffer.from("null")));

caso(G7, "array", P(Buffer.from("[]")));

caso(G7, "clave raíz repetida (id)", P(Buffer.from(baseBytes.toString("utf8").replace('"id":', '"id":"x","id":'))), "RAIZ_REPETIDA");
caso(G7, "clave raíz en mayúsculas (ID)", Pj((o) => ({ ...o, ID: "x" })));

caso(G7, "data con sustituto suelto", P(Buffer.from(baseBytes.toString("utf8").replace("ped-123", "\\ud83d"))), "SUSTITUTO");
caso(G7, "data con escape \\u00e9", P(Buffer.from(baseBytes.toString("utf8").replace("ped-123", "\\u00e9"))));

caso(G7, "data profundo 200", P(Buffer.from(baseBytes.toString("utf8").replace('{"pedidoId":"ped-123"}', `${"[".repeat(200)}${"]".repeat(200)}`).replace(`"data":${"[".repeat(200)}`, `"data":{"a":${"[".repeat(200)}`).replace(`${"]".repeat(200)}}`, `${"]".repeat(200)}}}`))));

caso(G7, "data profundo 100000", P(Buffer.from(baseBytes.toString("utf8").replace('"data":{"pedidoId":"ped-123"}', `"data":{"a":${"[".repeat(100000)}${"]".repeat(100000)}}`))));

caso(G7, "número enorme 1e999 en data", P(Buffer.from(baseBytes.toString("utf8").replace('"pedidoId":"ped-123"', '"x":1e999'))));

caso(G7, "NaN en data", P(Buffer.from(baseBytes.toString("utf8").replace('"pedidoId":"ped-123"', '"x":NaN'))));

caso(G7, "coma final", P(Buffer.from(baseBytes.toString("utf8").replace('"pedidoId":"ped-123"}', '"pedidoId":"ped-123",}'))));

caso(G7, "comillas simples", P(Buffer.from(baseBytes.toString("utf8").replace('"pedidoId"', "'pedidoId'"))));

caso(G7, "comentarios", P(Buffer.from(baseBytes.toString("utf8").replace('{"specversion"', '/*x*/{"specversion"'))));

caso(G7, "id vacío", Pj((o) => ({ ...o, id: "" })));

caso(G7, "source vacío", Pj((o) => ({ ...o, source: "" })));

caso(G7, "time vacío", Pj((o) => ({ ...o, time: "" })));

caso(G7, "time no canónico", Pj((o) => ({ ...o, time: "2025-08-20T10:25:39Z" })));

caso(G7, "time inválido", Pj((o) => ({ ...o, time: "ayer" })));

caso(G7, "type vacío", Pj((o) => ({ ...o, type: "" })));

caso(G7, "dataschema vacío", Pj((o) => ({ ...o, dataschema: "" })));

caso(G7, "datacontenttype distinto", Pj((o) => ({ ...o, datacontenttype: "text/plain" })));

caso(G7, "datacontenttype con charset", Pj((o) => ({ ...o, datacontenttype: "application/json; charset=utf-8" })));

caso(G7, "specversion 1.0 número", Pj((o) => ({ ...o, specversion: 1.0 })));

caso(G7, "dlqattempts 0", Pj((o) => ({ ...o, dlqattempts: 0, data: o.data })));

caso(G7, "dlqattempts 1.5", Pj((o) => { const { data, ...r } = o; return { ...r, dlqattempts: 1.5, data }; }));

caso(G7, "dlqattempts cadena", Pj((o) => { const { data, ...r } = o; return { ...r, dlqattempts: "1", data }; }));

caso(G7, "dlqreason inválido", Pj((o) => { const { data, ...r } = o; return { ...r, dlqreason: "x", data }; }));

caso(G7, "subject (aggregateId) número", Pj((o) => ({ ...o, subject: 5 })));

caso(G7, "subject vacío", Pj((o) => ({ ...o, subject: "" })));

caso(G7, "causationid número", Pj((o) => ({ ...o, causationid: 5 })));

caso(G7, "traceparent número", Pj((o) => ({ ...o, traceparent: 5 })));

caso(G7, "partitionkey número", Pj((o) => ({ ...o, partitionkey: 5 })));

caso(G7, "producerversion null", Pj((o) => ({ ...o, producerversion: null })));

caso(G7, "dataclassification vacío", Pj((o) => ({ ...o, dataclassification: "" })));

caso(G7, "dataclassification mayúsculas", Pj((o) => ({ ...o, dataclassification: "INTERNAL" })));

caso(G7, "data array", Pj((o) => ({ ...o, data: [1] })));

caso(G7, "data null", Pj((o) => ({ ...o, data: null })));

caso(G7, "data cadena", Pj((o) => ({ ...o, data: "x" })));

caso(G7, "sin data", Pj((o) => { delete o.data; return o; }));

caso(G7, "atributo desconocido", Pj((o) => ({ ...o, extra: 1 })));

caso(G7, "data no es el último", Pj((o) => { const { data, ...r } = o; return { data, ...r }; }));

caso(G7, "type de otro subject", Pj((o) => ({ ...o, type: "com.flux.otro.v1" })));


console.log(`\n${n} entradas · ${n - div - marcasInnecesarias} idénticas · ${marcadas.length} con una diferencia de lenguaje documentada (lo que no se admite) · ${sinMarcaQueDivergen} divergencias SIN marca · ${marcasInnecesarias} marcas que ya no hacen falta`);
process.exit(sinMarcaQueDivergen || marcasInnecesarias ? 1 : 0);
