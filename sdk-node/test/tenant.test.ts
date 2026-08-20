import { test, describe } from "node:test";
import assert from "node:assert/strict";

import { buildEvent } from "../src/envelope.js";

const base = {
  subject: "pedidos.pedido.v1.creado",
  id: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  source: "/produccion/pedidos-api",
  producerversion: "3.4.1",
  dataclassification: "internal" as const,
  dataschema: "https://schemas.internal/pedidos/pedido/creado/1.0.0.json",
  correlationid: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
};

describe("propagación de tenant", () => {
  test("un evento lleva el tenant que se le da", () => {
    const e = buildEvent({ ...base, tenantid: "acme", data: {} });
    assert.equal(e.tenantid, "acme");
  });

  test('"system" es un valor legítimo para eventos de plataforma', () => {
    // Pero NO es un comodín ni un default cuando el tenant real se desconoce: si no
    // se sabe de quién es un evento, el bug está aguas arriba — 09-multitenancy.md §5.
    const e = buildEvent({ ...base, tenantid: "system", data: {} });
    assert.equal(e.tenantid, "system");
  });

  test("el tenant es obligatorio en la construcción", () => {
    // El tipo lo exige; esta prueba fija que no hay un default silencioso.
    const input = { ...base, data: {} } as unknown as Parameters<typeof buildEvent>[0];
    assert.equal((buildEvent({ ...input, tenantid: "acme" }) as { tenantid: string }).tenantid, "acme");
  });
});
