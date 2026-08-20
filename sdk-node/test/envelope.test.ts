import { test, describe } from "node:test";
import assert from "node:assert/strict";

import {
  buildEvent,
  parseEvent,
  serialize,
  toDlqEvent,
  stripDlqExtensions,
  EnvelopeError,
  type FluxEvent,
} from "../src/envelope.js";
import { PoisonError } from "../src/errors.js";

const base = {
  subject: "pedidos.pedido.v1.creado",
  id: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  source: "/produccion/pedidos-api",
  producerversion: "3.4.1",
  tenantid: "acme",
  dataclassification: "confidential" as const,
  dataschema: "https://schemas.internal/pedidos/pedido/creado/1.0.0.json",
  correlationid: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
};

describe("construcción de envelope", () => {
  test("deriva el type del subject", () => {
    const e = buildEvent({ ...base, data: { pedidoId: "ped-1" } });
    assert.equal(e.type, "com.flux.pedidos.pedido.creado.v1");
    assert.equal(e.specversion, "1.0");
    assert.equal(e.datacontenttype, "application/json");
  });

  test("aggregateId va al atributo `subject` de CloudEvents, no al de NATS", () => {
    // La confusión más frecuente al adoptar CloudEvents sobre NATS.
    const e = buildEvent({ ...base, data: {}, aggregateId: "ped-123" });
    assert.equal(e.subject, "ped-123");
    assert.notEqual(e.subject, "pedidos.pedido.v1.creado");
  });

  test("partitionkey usa aggregateId por defecto", () => {
    const e = buildEvent({ ...base, data: {}, aggregateId: "ped-123" });
    assert.equal(e.partitionkey, "ped-123");
  });

  test("rechaza data que no sea un objeto en la raíz", () => {
    // Un array o escalar impide añadir campos sin romper el esquema.
    assert.throws(() => buildEvent({ ...base, data: [1, 2] as never }), EnvelopeError);
    assert.throws(() => buildEvent({ ...base, data: "hola" as never }), EnvelopeError);
    assert.throws(() => buildEvent({ ...base, data: null as never }), EnvelopeError);
  });

  test("omite las extensiones opcionales ausentes en vez de ponerlas a null", () => {
    const e = buildEvent({ ...base, data: {} });
    assert.ok(!("causationid" in e));
    assert.ok(!("traceparent" in e));
  });
});

describe("serialización", () => {
  test("falla por encima de 1 MiB mencionando claim-check", () => {
    const e = buildEvent({ ...base, data: { blob: "x".repeat(1_100_000) } });
    assert.throws(() => serialize(e), /claim-check/);
  });

  test("ida y vuelta preserva el evento", () => {
    const e = buildEvent({ ...base, data: { pedidoId: "ped-1", totalCents: 9990 } });
    assert.deepEqual(parseEvent(serialize(e)), e);
  });
});

describe("parseo — todo fallo es POISON", () => {
  const enc = (o: unknown) => new TextEncoder().encode(JSON.stringify(o));

  test("JSON malformado", () => {
    assert.throws(
      () => parseEvent(new TextEncoder().encode("{no soy json")),
      (e: Error) => e instanceof PoisonError && (e as PoisonError).opts.code === "MALFORMED_JSON",
    );
  });

  test("specversion desconocida", () => {
    assert.throws(
      () => parseEvent(enc({ specversion: "0.3" })),
      (e: Error) => (e as PoisonError).opts.code === "UNSUPPORTED_SPECVERSION",
    );
  });

  test("falta un atributo obligatorio", () => {
    assert.throws(
      () => parseEvent(enc({ specversion: "1.0", id: "x" })),
      (e: Error) => (e as PoisonError).opts.code === "MISSING_REQUIRED_ATTRIBUTE",
    );
  });

  test("falta una extensión obligatoria del perfil flux → POISON", () => {
    // La spec las llamaba obligatorias y ningún parser las exigía. Asumir un default
    // es peligroso en las cuatro: un dataclassification ausente tomado como
    // "internal" haría circular PII con 30 días de retención en vez de 7.
    const e = buildEvent({ ...base, data: {} });
    for (const ext of ["correlationid", "tenantid", "producerversion", "dataclassification"]) {
      const { [ext]: _, ...sinExt } = e as unknown as Record<string, unknown>;
      assert.throws(
        () => parseEvent(enc(sinExt)),
        (err: Error) => (err as PoisonError).opts.code === "MISSING_REQUIRED_EXTENSION",
        `debería rechazar un evento sin ${ext}`,
      );
    }
  });

  test("dataclassification fuera del enum → POISON", () => {
    const e = buildEvent({ ...base, data: {} });
    assert.throws(
      () => parseEvent(enc({ ...e, dataclassification: "secreto" })),
      (err: Error) => (err as PoisonError).opts.code === "INVALID_DATACLASSIFICATION",
    );
  });

  test("un tipo coaccionable → POISON, no coerción silenciosa", () => {
    // Jackson convertiría {"tenantid": 42} en "42" sin avisar. Ver 01-envelope.md §2.4.
    const e = buildEvent({ ...base, data: {} });
    assert.throws(
      () => parseEvent(enc({ ...e, tenantid: 42 })),
      (err: Error) => (err as PoisonError).opts.code === "WRONG_ATTRIBUTE_TYPE",
    );
  });

  test("rechaza atributos raíz desconocidos", () => {
    // Estricto a propósito: permitirlos lleva a serializar JSON dentro de un string
    // y el envelope deja de ser interoperable.
    const e = buildEvent({ ...base, data: {} });
    const contaminado = { ...e, miMetadato: { algo: 1 } };
    assert.throws(
      () => parseEvent(enc(contaminado)),
      (err: Error) => (err as PoisonError).opts.code === "UNKNOWN_ROOT_ATTRIBUTE",
    );
  });
});

describe("DLQ", () => {
  test("preserva el CloudEvent original íntegro y añade extensiones dlq*", () => {
    const e = buildEvent({ ...base, data: { pedidoId: "ped-1" } });
    const d = toDlqEvent(e, {
      reason: "permanent",
      attempts: 1,
      consumer: "facturacion-api__pedidos_pedido_v1_creado",
      error: "PEDIDO_YA_CANCELADO",
    });
    assert.equal(d.id, e.id, "el id original se conserva");
    assert.equal(d.dlqreason, "permanent");
    assert.equal(d.dlqattempts, 1);
    assert.deepEqual(d.data, e.data);
  });

  test("quitar las extensiones dlq* devuelve el evento original — el replay es trivial", () => {
    const e = buildEvent({ ...base, data: { pedidoId: "ped-1" } });
    const d = toDlqEvent(e, { reason: "retryable", attempts: 6, consumer: "c", error: "x" });
    assert.deepEqual(stripDlqExtensions(d), e as FluxEvent);
  });

  test("`data` va el último — el orden de claves es normativo", () => {
    // JSON no define orden, pero flux depende de la secuencia de BYTES para el replay
    // verbatim y la firma futura. Node construía esto con {...event, dlq*}, dejando
    // las extensiones después de `data`, mientras Python/Go/Java las ponían antes:
    // mismo evento, dos secuencias distintas. Ver 01-envelope.md §6.
    const e = buildEvent({ ...base, data: { pedidoId: "ped-1" } });
    const d = toDlqEvent(e, { reason: "permanent", attempts: 1, consumer: "c", error: "X" });
    const keys = Object.keys(d);

    assert.equal(keys.at(-1), "data", "`data` debe ser el último atributo");
    assert.ok(
      keys.indexOf("dlqreason") < keys.indexOf("data"),
      "las extensiones dlq* van ANTES de data",
    );
    assert.equal(Object.keys(e).at(-1), "data", "también en el envelope normal");
  });

  test("dlqattempts distingue un PERMANENT de un RETRYABLE agotado", () => {
    const e = buildEvent({ ...base, data: {} });
    assert.equal(toDlqEvent(e, { reason: "permanent", attempts: 1, consumer: "c", error: "" }).dlqattempts, 1);
    assert.equal(toDlqEvent(e, { reason: "retryable", attempts: 6, consumer: "c", error: "" }).dlqattempts, 6);
  });
});
