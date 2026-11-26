/**
 * Los errores HTTP y la clasificación de la respuesta del receptor — specification/12-edge.md §4.4 (NORMATIVO) y §6.4. Sin broker.
 *
 * Las tablas del documento (`protocol.json` → `edge`) y el código dicen lo mismo: aquí se recorren los DOS lados, y todo el rango
 * de estados HTTP, no un puñado de ejemplos.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { ErrorClass, EnvelopeError, InvalidSubjectError, PermanentError, PermissionDeniedError, RetryableError } from "@flux/sdk";

import { cargar } from "./helpers/fuente.mjs";

const { clasificarFalloDeRed, clasificarRespuesta, parseRetryAfter, FalloDeEntrega, reglaDeClasificacion } = await cargar("entrega");
const { CODIGOS_EDGE, CODIGOS_PROTOCOLO, ErrorHttp, cabecerasDeError, cuerpoDeError, errorEdge, traducir } = await cargar("errores");

const protocolo = JSON.parse(readFileSync(new URL("../../protocol.json", import.meta.url), "utf8"));
const edge = protocolo.edge;

describe("clasificarRespuesta: el estado HTTP del receptor (12-edge.md §4.4)", () => {
  const clase = (st) => {
    const c = clasificarRespuesta(st);
    return c === null ? "ack" : c.class === ErrorClass.PERMANENT ? "permanent" : c.maxAttempts ? "retryable-bounded" : "retryable";
  };

  test("los ejemplos del documento", () => {
    for (const { status, outcome } of edge.delivery.response.examples) assert.equal(clase(status), outcome, `HTTP ${status}`);
  });

  test("TODO el rango 100–599: 2xx ack; 3xx permanent; 4xx permanent salvo 408/425 (acotado) y 429; 5xx reintentable", () => {
    for (let st = 100; st <= 599; st++) {
      const c = clase(st);
      if (st >= 200 && st <= 299) assert.equal(c, "ack", String(st));
      else if (st >= 300 && st <= 399) assert.equal(c, "permanent", `${st}: una redirección no se sigue`);
      else if (st === 408 || st === 425) assert.equal(c, "retryable-bounded", String(st));
      else if (st === 429) assert.equal(c, "retryable", String(st));
      else if (st >= 400 && st <= 499) assert.equal(c, "permanent", `${st}: un 4xx va a la DLQ sin reintentos`);
      else if ([502, 503, 504].includes(st)) assert.equal(c, "retryable", String(st));
      else if (st >= 500 && st <= 599) assert.equal(c, "retryable-bounded", `${st}: no es un 503`);
      else assert.equal(c, "permanent", `${st}: un 1xx no es una respuesta final`);
    }
  });

  test("429, 502, 503 y 504 son los de errors.classes.retryable.httpStatus", () => {
    for (const st of protocolo.errors.classes.retryable.httpStatus) assert.equal(clase(st), "retryable");
    for (const st of protocolo.errors.classes.permanent.httpStatus) assert.equal(clase(st), "permanent");
  });

  test("el presupuesto acotado es el de lo desconocido (2) y el code es HTTP_<n>", () => {
    assert.equal(clasificarRespuesta(500).maxAttempts, protocolo.errors.unknownRetryBudget);
    assert.equal(clasificarRespuesta(503).maxAttempts, undefined, "un 503 tiene el presupuesto completo");
    assert.equal(clasificarRespuesta(404).code, "HTTP_404");
    assert.match(clasificarRespuesta(500).code, new RegExp(edge.delivery.response.codePattern));
  });

  test("`Retry-After` se pasa tal cual a lo reintentable (y no a lo permanente)", () => {
    assert.equal(clasificarRespuesta(503, 1000).retryAfterMs, 1000);
    assert.equal(clasificarRespuesta(500, 1000).retryAfterMs, 1000);
    assert.equal(clasificarRespuesta(404, 1000).retryAfterMs, undefined);
    assert.equal("retryAfterMs" in clasificarRespuesta(404, 1000), false);
  });
});

describe("clasificarFalloDeRed", () => {
  test("lo transitorio se reintenta con el presupuesto completo", () => {
    for (const code of edge.delivery.response.network.retryable.filter((c) => c !== "TIMEOUT")) {
      assert.deepEqual(clasificarFalloDeRed({ code }), { class: ErrorClass.RETRYABLE, code });
    }
    assert.deepEqual(clasificarFalloDeRed({ code: "PLAZO" }), { class: ErrorClass.RETRYABLE, code: "TIMEOUT" });
  });

  test("un certificado que no verifica es PERMANENT con un code estable", () => {
    for (const code of edge.delivery.response.network.tlsVerificationFailure.systemCodes) {
      assert.deepEqual(clasificarFalloDeRed({ code }), { class: ErrorClass.PERMANENT, code: "TLS_VERIFICATION_FAILED" });
    }
  });

  test("lo desconocido (ENOTFOUND, EPROTO, sin código) es reintentable ACOTADO", () => {
    for (const e of [{ code: "ENOTFOUND" }, { code: "EPROTO" }, {}, new Error("raro")]) {
      const c = clasificarFalloDeRed(e);
      assert.equal(c.class, ErrorClass.RETRYABLE);
      assert.equal(c.maxAttempts, protocolo.errors.unknownRetryBudget);
    }
  });
});

describe("parseRetryAfter", () => {
  const AHORA = Date.parse("2026-08-20T10:00:00Z");
  test("segundos y fecha HTTP, recortado a 300 s", () => {
    assert.equal(parseRetryAfter("7", AHORA), 7000);
    assert.equal(parseRetryAfter(" 1 ", AHORA), 1000);
    assert.equal(parseRetryAfter("Thu, 20 Aug 2026 10:00:30 GMT", AHORA), 30_000);
    assert.equal(parseRetryAfter("86400", AHORA), edge.delivery.retryAfter.maxMs, "un día se recorta a 300 s");
    assert.equal(parseRetryAfter("Wed, 20 Aug 2031 10:00:00 GMT", AHORA), edge.delivery.retryAfter.maxMs, "una fecha del año que viene también");
  });
  test("lo que no dice nada útil es undefined", () => {
    for (const v of [undefined, "", "0", "-5", "abc", "1.5", "Wed, 20 Aug 2020 10:00:00 GMT", "99999999999"]) assert.equal(parseRetryAfter(v, AHORA), undefined, String(v));
    assert.equal(parseRetryAfter("99999999", AHORA), 300_000, "un valor enorme pero válido se recorta, no se descarta");
  });
});

describe("la regla que se le da al SDK", () => {
  test("solo reconoce un FalloDeEntrega: lo demás cae en la política normal", () => {
    const c = { class: ErrorClass.PERMANENT, code: "HTTP_400" };
    assert.deepEqual(reglaDeClasificacion(new FalloDeEntrega("x", c)), c);
    assert.equal(reglaDeClasificacion(new Error("x")), undefined);
    assert.equal(reglaDeClasificacion(undefined), undefined);
  });
});

describe("traducir: los códigos estables del protocolo → estado HTTP (12-edge.md §6.4)", () => {
  test("la tabla del código es la de protocol.json", () => {
    assert.deepEqual(
      Object.fromEntries(Object.entries(CODIGOS_PROTOCOLO).map(([k, v]) => [k, { status: v.status, class: v.clase }])),
      Object.fromEntries(Object.entries(edge.gateway.errors.protocolCodes).map(([k, v]) => [k, { status: v.status, class: v.class }])),
    );
    assert.deepEqual(
      Object.fromEntries(Object.entries(CODIGOS_EDGE).map(([k, v]) => [k, { status: v.status, class: v.clase }])),
      Object.fromEntries(Object.entries(edge.gateway.errors.codes).map(([k, v]) => [k, { status: v.status, class: v.class }])),
    );
  });

  test("todo código de errors.codes tiene estado HTTP, o está declarado como no expuesto", () => {
    for (const c of Object.keys(protocolo.errors.codes).filter((k) => !k.startsWith("$"))) {
      assert.ok(c in CODIGOS_PROTOCOLO || c in edge.gateway.errors.notSurfaced, `${c} no tiene estado HTTP ni se declara no expuesto`);
    }
  });

  test("la regla de los estados: 502, 503 y 504 SIEMPRE reintentables; un 4xx reintentable solo 408, 409 o 429", () => {
    for (const [c, v] of [...Object.entries(CODIGOS_PROTOCOLO), ...Object.entries(CODIGOS_EDGE)]) {
      if ([502, 503, 504].includes(v.status)) assert.equal(v.clase, "retryable", c);
      if (v.clase === "retryable" && v.status < 500) assert.ok([408, 409, 429].includes(v.status), c);
    }
  });

  test("PERMISSION_DENIED del SDK es 403; RPC_TIMEOUT es 504 y AMBIGUO; RPC_UNAVAILABLE es 503 con Retry-After", () => {
    const denegado = traducir(new PermissionDeniedError("catalogo-web", "publish", "rpc.precios.tarifa.v1.calcular"));
    assert.equal(denegado.status, 403);
    assert.equal(denegado.code, "PERMISSION_DENIED");
    assert.equal(denegado.clase, "permanent");

    const timeout = traducir(Object.assign(new RetryableError("sin respuesta", { code: "RPC_TIMEOUT" }), { code: "RPC_TIMEOUT" }));
    assert.equal(timeout.status, 504);
    assert.match(cuerpoDeError(timeout).error.message, /PUEDE haberse ejecutado/, "el cuerpo dice que la operación pudo ejecutarse");

    const nadie = traducir(Object.assign(new RetryableError("nadie", { code: "RPC_UNAVAILABLE" }), { code: "RPC_UNAVAILABLE" }));
    assert.equal(nadie.status, 503);
    assert.equal(cabecerasDeError(nadie)["retry-after"], "1");
  });

  test("un error del que responde conserva su code y su clase: retryable → 503, permanent → 422", () => {
    const r = traducir(new RetryableError("el proveedor de tarifas no responde", { code: "HTTP_503", retryAfterMs: 5000 }));
    assert.equal(r.status, 503);
    assert.equal(r.code, "HTTP_503");
    assert.equal(r.clase, "retryable");
    assert.equal(cabecerasDeError(r)["retry-after"], "5");
    const p = traducir(new PermanentError("el pedido ya estaba cancelado", { code: "PEDIDO_YA_CANCELADO" }));
    assert.equal(p.status, 422);
    assert.equal(p.code, "PEDIDO_YA_CANCELADO");
    assert.equal(p.clase, "permanent");
    assert.equal(cabecerasDeError(p)["retry-after"], undefined, "un permanente no lleva Retry-After");
  });

  test("lo que lanza el SDK sin código: subject inválido → 400; un evento de más de 1 MiB → 413 (A-152)", () => {
    assert.equal(traducir(new InvalidSubjectError("pedidos.crear", "tiene 2 tokens")).status, 400);
    // El texto exacto lo fija el SDK: si cambia, este test falla y el 413 no se degrada en silencio a un 400.
    const grande = traducir(new EnvelopeError("evento de 1048898 bytes supera el límite de 1048576 (1 MiB). Aplica claim-check"));
    assert.equal(grande.status, 413);
    assert.equal(grande.code, "EDGE_BODY_TOO_LARGE");
    assert.equal(traducir(new EnvelopeError("`data` debe ser un objeto JSON en la raíz")).status, 400);
  });

  test("lo desconocido es 500 con un mensaje GENÉRICO: no se reenvía el texto de la excepción", () => {
    const e = traducir(new Error("ENOENT: /srv/secreto/clave-de-ana@example.com.json"));
    assert.equal(e.status, 500);
    assert.equal(e.code, "EDGE_INTERNAL");
    assert.doesNotMatch(JSON.stringify(cuerpoDeError(e)), /secreto|ana@example/);
    assert.equal(traducir(undefined).status, 500);
    assert.equal(traducir("texto").status, 500);
  });

  test("un 401 lleva `WWW-Authenticate`; un error del borde con retryAfterMs lleva `Retry-After` en segundos (hacia arriba)", () => {
    assert.equal(cabecerasDeError(errorEdge("EDGE_UNAUTHENTICATED", "x"))["www-authenticate"], "Bearer");
    assert.equal(cabecerasDeError(errorEdge("EDGE_RATE_LIMITED", "x", { retryAfterMs: 1200 }))["retry-after"], "2");
    assert.equal(cabecerasDeError(errorEdge("EDGE_RATE_LIMITED", "x", { retryAfterMs: 10 }))["retry-after"], "1");
  });

  test("el cuerpo de un error es { error: { code, class, message, retryAfterMs? } } y nada más", () => {
    const c = cuerpoDeError(new ErrorHttp(503, "X", "retryable", "m", { retryAfterMs: 1000 }));
    assert.deepEqual(Object.keys(c), ["error"]);
    assert.deepEqual(Object.keys(c.error), ["code", "class", "message", "retryAfterMs"]);
    assert.deepEqual(Object.keys(cuerpoDeError(errorEdge("EDGE_NOT_FOUND", "m")).error), ["code", "class", "message"]);
  });

  test("un código del borde desconocido lanza (un typo no se convierte en un 500 mudo)", () => {
    assert.throws(() => errorEdge("EDGE_NO_EXISTE", "x"));
  });
});
