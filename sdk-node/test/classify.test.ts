import { test, describe } from "node:test";
import assert from "node:assert/strict";

import { classify, createClassifier } from "../src/classify.js";
import { ErrorClass, RetryableError, PermanentError, PoisonError } from "../src/errors.js";

describe("errores tipados de flux", () => {
  test("la aplicación siempre gana sobre la heurística del SDK", () => {
    // Un 503 sería RETRYABLE por defecto, pero si la app lo marca PERMANENT, manda ella.
    const e = Object.assign(new PermanentError("no reintentar", { code: "X" }), { status: 503 });
    assert.equal(classify(e).class, ErrorClass.PERMANENT);
    assert.equal(classify(e).code, "X");
  });

  test("RetryableError propaga retryAfterMs", () => {
    const c = classify(new RetryableError("503", { retryAfterMs: 5000 }));
    assert.equal(c.class, ErrorClass.RETRYABLE);
    assert.equal(c.retryAfterMs, 5000);
  });

  test("PermanentError nunca lleva retryAfterMs", () => {
    assert.equal(classify(new PermanentError("x")).retryAfterMs, undefined);
  });

  test("PoisonError conserva su clase", () => {
    assert.equal(classify(new PoisonError("json roto")).class, ErrorClass.POISON);
  });
});

describe("status HTTP", () => {
  for (const s of [429, 502, 503, 504]) {
    test(`${s} es RETRYABLE`, () => {
      assert.equal(classify({ status: s }).class, ErrorClass.RETRYABLE);
      assert.equal(classify({ status: s }).code, `HTTP_${s}`);
    });
  }

  for (const s of [400, 401, 403, 404, 422]) {
    test(`${s} es PERMANENT — reintentar da la misma respuesta`, () => {
      assert.equal(classify({ status: s }).class, ErrorClass.PERMANENT);
    });
  }

  test("lee statusCode y response.status además de status", () => {
    assert.equal(classify({ statusCode: 503 }).class, ErrorClass.RETRYABLE);
    assert.equal(classify({ response: { status: 503 } }).class, ErrorClass.RETRYABLE);
  });

  test("respeta Retry-After de la dependencia", () => {
    const e = { status: 429, response: { headers: { "retry-after": "7" } } };
    assert.equal(classify(e).retryAfterMs, 7000);
  });
});

describe("errores de sistema", () => {
  for (const code of ["ECONNRESET", "ETIMEDOUT", "EAI_AGAIN", "ECONNREFUSED"]) {
    test(`${code} es RETRYABLE`, () => {
      assert.equal(classify(Object.assign(new Error(), { code })).class, ErrorClass.RETRYABLE);
    });
  }

  test("un código de sistema desconocido cae en el default acotado", () => {
    // EPERM no está en la lista de transitorios, así que no es un RETRYABLE
    // reconocido: recibe el presupuesto acotado, no los 6 intentos.
    const c = classify(Object.assign(new Error(), { code: "EPERM" }));
    assert.equal(c.class, ErrorClass.RETRYABLE);
    assert.equal(c.maxAttempts, 2);
    assert.equal(c.code, "EPERM", "el código se conserva para métricas");
  });
});

describe("política ante lo desconocido", () => {
  test("el default es RETRYABLE con presupuesto acotado de 2 entregas", () => {
    // Domina a las dos alternativas: el transitorio se recupera en el 2º intento,
    // el sistemático llega a la DLQ en ~30s sin atascar la cola. 04-errors.md §2.1.
    const c = classify(new Error("¿?"));
    assert.equal(c.class, ErrorClass.RETRYABLE);
    assert.equal(c.code, "UNKNOWN");
    assert.equal(c.maxAttempts, 2);
  });

  test("el presupuesto acotado NO recorta los RETRYABLE reconocidos", () => {
    // Un ECONNRESET conserva sus 6 intentos: el presupuesto es por error, no por
    // consumidor. Bajarlo en max_deliver los habría recortado a todos.
    assert.equal(classify({ status: 503 }).maxAttempts, undefined);
    assert.equal(
      classify(Object.assign(new Error(), { code: "ECONNRESET" })).maxAttempts,
      undefined,
    );
  });

  test("el presupuesto es configurable", () => {
    const c = createClassifier({ unknownRetryBudget: 3 });
    assert.equal(c(new Error("¿?")).maxAttempts, 3);
  });

  test("unknownErrorPolicy: permanent no gasta ningún reintento", () => {
    const c = createClassifier({ unknownErrorPolicy: "permanent" });
    assert.equal(c(new Error("¿?")).class, ErrorClass.PERMANENT);
    assert.equal(c(new Error("¿?")).maxAttempts, undefined);
  });

  test("unknownErrorPolicy: retryable da el backoff completo", () => {
    const c = createClassifier({ unknownErrorPolicy: "retryable" });
    assert.equal(c(new Error("¿?")).class, ErrorClass.RETRYABLE);
    assert.equal(c(new Error("¿?")).maxAttempts, undefined, "sin tope propio = el del consumidor");
  });

  test("los timeouts son RETRYABLE por defecto y configurables", () => {
    const t = Object.assign(new Error("abort"), { name: "AbortError" });
    assert.equal(classify(t).class, ErrorClass.RETRYABLE);
    assert.equal(
      createClassifier({ timeoutPolicy: "permanent" })(t).class,
      ErrorClass.PERMANENT,
    );
  });

  test("las reglas propias se evalúan antes que la heurística", () => {
    const c = createClassifier({
      rules: [
        (e) =>
          /deadlock/i.test(String((e as Error)?.message))
            ? { class: ErrorClass.RETRYABLE, code: "DB_DEADLOCK", retryAfterMs: 250 }
            : undefined,
      ],
    });
    // Sin la regla, un 400 sería PERMANENT.
    const e = Object.assign(new Error("deadlock detected"), { status: 400 });
    assert.equal(c(e).class, ErrorClass.RETRYABLE);
    assert.equal(c(e).code, "DB_DEADLOCK");
  });

  test("una regla que devuelve undefined cede a la siguiente etapa", () => {
    const c = createClassifier({ rules: [() => undefined] });
    assert.equal(c({ status: 503 }).class, ErrorClass.RETRYABLE);
  });
});
