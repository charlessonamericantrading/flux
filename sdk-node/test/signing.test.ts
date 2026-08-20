import { test, describe } from "node:test";
import assert from "node:assert/strict";

import {
  createSigner,
  createVerifier,
  generateKeyPair,
  SigningKeyError,
} from "../src/signing.js";
import { buildEvent, toDlqEvent, serialize, parseEvent, type FluxEvent } from "../src/envelope.js";
import { PoisonError } from "../src/errors.js";

const { privateKeyPem, publicKeyPem } = generateKeyPair();
const KEY_ID = "pedidos-api-1";

const base = {
  subject: "pedidos.pedido.v1.creado",
  id: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  source: "/produccion/pedidos-api",
  time: "2025-08-20T10:25:39.410Z",
  producerversion: "3.4.1",
  tenantid: "acme",
  dataclassification: "internal" as const,
  dataschema: "https://schemas.internal/pedidos/pedido/creado/1.0.0.json",
  correlationid: "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
};

const signer = createSigner({ privateKeyPem, keyId: KEY_ID })!;
const verifier = createVerifier({
  publicKeys: { [KEY_ID]: publicKeyPem },
  verify: "require",
})!;

const evento = (data: object = { pedidoId: "ped-123" }) =>
  buildEvent({ ...base, data });

describe("firma", () => {
  test("añade signkeyid y signature, ambos antes de `data`", () => {
    const firmado = signer.sign(evento());
    const keys = Object.keys(firmado);

    assert.equal((firmado as FluxEvent & { signkeyid: string }).signkeyid, KEY_ID);
    assert.match((firmado as FluxEvent & { signature: string }).signature, /^[A-Za-z0-9_-]+$/);
    assert.equal(keys.at(-1), "data", "`data` sigue siendo el último — 01-envelope.md §6");
    assert.ok(keys.indexOf("signature") < keys.indexOf("data"));
  });

  test("una firma válida verifica", () => {
    assert.doesNotThrow(() => verifier.check(signer.sign(evento())));
  });

  test("es determinista: el mismo evento produce la misma firma", () => {
    // Solo es cierto porque 01-envelope.md §1.1, §2.2 y §6 fijan una única
    // representación en bytes. Sin ellas, firmar sería imposible entre lenguajes.
    const a = signer.sign(evento()) as FluxEvent & { signature: string };
    const b = signer.sign(evento()) as FluxEvent & { signature: string };
    assert.equal(a.signature, b.signature);
  });

  test("sobrevive a un round-trip de serialización", () => {
    const firmado = signer.sign(evento());
    assert.doesNotThrow(() => verifier.check(parseEvent(serialize(firmado))));
  });
});

describe("detección de manipulación", () => {
  test("alterar `data` invalida la firma", () => {
    const firmado = signer.sign(evento()) as unknown as Record<string, unknown>;
    const manipulado = { ...firmado, data: { pedidoId: "ped-999" } };
    assert.throws(
      () => verifier.check(manipulado as unknown as FluxEvent),
      (e: Error) => (e as PoisonError).opts.code === "INVALID_SIGNATURE",
    );
  });

  test("alterar el tenantid invalida la firma", () => {
    // El caso que la ACL del broker no cubre: un evento sacado del stream, editado
    // y reinyectado. Ver 07-signing.md §1.
    const firmado = signer.sign(evento()) as unknown as Record<string, unknown>;
    assert.throws(
      () => verifier.check({ ...firmado, tenantid: "otro" } as unknown as FluxEvent),
      (e: Error) => (e as PoisonError).opts.code === "INVALID_SIGNATURE",
    );
  });

  test("cambiar signkeyid no permite eludir la verificación", () => {
    // signkeyid va DENTRO de lo firmado justo para esto — 07-signing.md §5.
    const firmado = signer.sign(evento()) as unknown as Record<string, unknown>;
    assert.throws(
      () => verifier.check({ ...firmado, signkeyid: "otro-1" } as unknown as FluxEvent),
      (e: Error) => (e as PoisonError).opts.code === "UNKNOWN_SIGNING_KEY",
    );
  });

  test("una firma de otra clave no verifica", () => {
    const otra = generateKeyPair();
    const impostor = createSigner({ privateKeyPem: otra.privateKeyPem, keyId: KEY_ID })!;
    assert.throws(
      () => verifier.check(impostor.sign(evento())),
      (e: Error) => (e as PoisonError).opts.code === "INVALID_SIGNATURE",
    );
  });
});

describe("DLQ y replay", () => {
  test("la firma sigue verificando tras pasar por la DLQ", () => {
    // Las extensiones dlq* se añaden DESPUÉS de firmar y no están cubiertas. Si la
    // verificación no las ignorara, todo evento en la DLQ parecería manipulado.
    const firmado = signer.sign(evento());
    const enDlq = toDlqEvent(firmado, {
      reason: "permanent",
      attempts: 1,
      consumer: "facturacion-api__pedidos_pedido_v1_creado",
      error: "PEDIDO_YA_CANCELADO",
    });
    assert.doesNotThrow(() => verifier.check(enDlq));
  });

  test("un evento reproducido conserva su firma válida", () => {
    // El replay redistribuye un hecho ya emitido, no crea uno nuevo — 07-signing.md §5.1.
    const firmado = signer.sign(evento());
    const enDlq = toDlqEvent(firmado, { reason: "retryable", attempts: 6, consumer: "c", error: "x" });
    const reproducido = parseEvent(serialize(enDlq));
    assert.doesNotThrow(() => verifier.check(reproducido));
  });
});

describe("modos de verificación", () => {
  test("require: un evento sin firma es POISON", () => {
    assert.throws(
      () => verifier.check(evento()),
      (e: Error) => (e as PoisonError).opts.code === "MISSING_SIGNATURE",
    );
  });

  test("warn: registra pero acepta", () => {
    const v = createVerifier({ publicKeys: { [KEY_ID]: publicKeyPem }, verify: "warn" })!;
    const original = console.warn;
    let avisos = 0;
    console.warn = () => avisos++;
    try {
      assert.doesNotThrow(() => v.check(evento()));
      assert.equal(avisos, 1);
    } finally {
      console.warn = original;
    }
  });

  test("off no construye verificador — no se paga lo que no se usa", () => {
    assert.equal(createVerifier({ verify: "off" }), null);
    assert.equal(createVerifier({}), null);
  });
});

describe("gestión de claves", () => {
  test("firmar sin keyId falla con un mensaje accionable", () => {
    assert.throws(() => createSigner({ privateKeyPem }), SigningKeyError);
  });

  test("rechaza una clave que no sea Ed25519", async () => {
    const { generateKeyPairSync } = await import("node:crypto");
    const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
    assert.throws(
      () =>
        createSigner({
          privateKeyPem: privateKey.export({ type: "pkcs8", format: "pem" }).toString(),
          keyId: "x-1",
        }),
      /ed25519/,
    );
  });

  test("verificar sin claves públicas falla explicando la retención", () => {
    assert.throws(() => createVerifier({ verify: "require" }), /RETIRADAS/);
  });

  test("una clave retirada sigue verificando si se conserva la pública", () => {
    // Retirar una clave impide EMITIR con ella, no VERIFICAR lo ya emitido. Tratarla
    // como inválida convertiría una rotación rutinaria en la invalidación retroactiva
    // de todo el historial — 07-signing.md §6.
    const vieja = generateKeyPair();
    const nueva = generateKeyPair();
    const firmadoConVieja = createSigner({
      privateKeyPem: vieja.privateKeyPem,
      keyId: "pedidos-api-1",
    })!.sign(evento());

    const v = createVerifier({
      publicKeys: {
        "pedidos-api-1": vieja.publicKeyPem, // retirada, conservada
        "pedidos-api-2": nueva.publicKeyPem, // activa
      },
      verify: "require",
    })!;
    assert.doesNotThrow(() => v.check(firmadoConVieja));
  });
});
