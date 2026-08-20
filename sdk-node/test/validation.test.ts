import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import {
  createValidator,
  schemaUriFor,
  SchemaValidationError,
  SchemaNotFoundError,
  type SchemaBundle,
} from "../src/validation.js";
import { buildEvent, type FluxEvent } from "../src/envelope.js";

/**
 * La raíz se busca por un marcador, no contando `../`: el fuente vive en `test/` y el
 * compilado en `dist-test/test/`, así que cualquier ruta relativa fija es correcta en
 * uno de los dos y silenciosamente incorrecta en el otro.
 */
async function repoRoot(): Promise<URL> {
  let dir = new URL("./", import.meta.url);
  for (let i = 0; i < 6; i++) {
    try {
      await readFile(new URL("protocol.json", dir), "utf8");
      return dir;
    } catch {
      dir = new URL("../", dir);
    }
  }
  throw new Error("no se encontró la raíz del repo (protocol.json)");
}

const bundle: SchemaBundle = JSON.parse(
  await readFile(new URL("schemas/bundle.json", await repoRoot()), "utf8"),
);

const SUBJECT = "pedidos.pedido.v1.creado";
const URI = schemaUriFor(bundle, SUBJECT)!;

function evento(data: object): FluxEvent {
  return buildEvent({
    subject: SUBJECT,
    data,
    id: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
    source: "/produccion/pedidos-api",
    producerversion: "3.4.1",
    tenantid: "acme",
    dataclassification: "internal",
    dataschema: URI,
    correlationid: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  });
}

const valido = {
  pedidoId: "ped-123",
  clienteId: "cli-987",
  aggregateVersion: 1,
  totalCents: 9990,
  moneda: "EUR",
  lineas: [{ sku: "ABC-1", cantidad: 2, precioUnitarioCents: 4995 }],
};

describe("bundle de esquemas", () => {
  test("indexa el subject hacia su URI de dataschema", () => {
    assert.ok(URI, "el bundle debe resolver el subject de ejemplo");
    assert.match(URI, /^https:\/\/schemas\.internal\/pedidos\/pedido\/creado\/\d+\.\d+\.\d+\.json$/);
  });

  test("el $id del esquema coincide con la clave del bundle", () => {
    assert.equal((bundle.schemas[URI] as { $id: string }).$id, URI);
  });
});

describe("validación L3 — strict", () => {
  test("un payload válido pasa", async () => {
    const v = await createValidator({ mode: "strict", bundle });
    assert.doesNotThrow(() => v!(evento(valido), SUBJECT));
  });

  test("falta un campo requerido → lanza", async () => {
    const v = await createValidator({ mode: "strict", bundle });
    const { totalCents: _, ...sinTotal } = valido;
    assert.throws(() => v!(evento(sinTotal), SUBJECT), SchemaValidationError);
  });

  test("tipo incorrecto → lanza", async () => {
    const v = await createValidator({ mode: "strict", bundle });
    // El caso que la spec llama el más peligroso: "9990" en vez de 9990.
    assert.throws(
      () => v!(evento({ ...valido, totalCents: "9990" }), SUBJECT),
      SchemaValidationError,
    );
  });

  test("campo desconocido → lanza (additionalProperties: false)", async () => {
    const v = await createValidator({ mode: "strict", bundle });
    // Un campo mal escrito debe fallar, no colarse en silencio.
    assert.throws(
      () => v!(evento({ ...valido, totalCemts: 9990 }), SUBJECT),
      SchemaValidationError,
    );
  });

  test("patrón incumplido → lanza", async () => {
    const v = await createValidator({ mode: "strict", bundle });
    assert.throws(
      () => v!(evento({ ...valido, moneda: "euros" }), SUBJECT),
      SchemaValidationError,
    );
  });

  test("reporta TODOS los errores, no solo el primero", async () => {
    const v = await createValidator({ mode: "strict", bundle });
    // Reportar de uno en uno convierte arreglarlo en tres despliegues.
    try {
      v!(evento({ ...valido, totalCents: "x", moneda: "euros", cantidad: 1 }), SUBJECT);
      assert.fail("debería haber lanzado");
    } catch (e) {
      assert.ok(e instanceof SchemaValidationError);
      assert.ok(e.errors.length >= 2, `esperaba ≥2 errores, hubo ${e.errors.length}`);
    }
  });

  test("esquema ausente del bundle → lanza SchemaNotFoundError", async () => {
    const v = await createValidator({ mode: "strict", bundle });
    const e = { ...evento(valido), dataschema: "https://schemas.internal/no/existe/1.0.0.json" };
    assert.throws(() => v!(e as FluxEvent, SUBJECT), SchemaNotFoundError);
  });
});

describe("validación L3 — warn y off", () => {
  test("warn registra pero no lanza", async () => {
    const v = await createValidator({ mode: "warn", bundle });
    const original = console.warn;
    let avisos = 0;
    console.warn = () => avisos++;
    try {
      assert.doesNotThrow(() => v!(evento({ ...valido, totalCents: "x" }), SUBJECT));
      assert.equal(avisos, 1);
    } finally {
      console.warn = original;
    }
  });

  test("off no compila nada — L2 no paga el coste de L3", async () => {
    assert.equal(await createValidator({ mode: "off" }), null);
    assert.equal(await createValidator({}), null);
  });

  test("strict sin bundle falla con un mensaje accionable", async () => {
    await assert.rejects(
      () => createValidator({ mode: "strict" }),
      /bundle-schemas\.mjs/,
    );
  });
});
