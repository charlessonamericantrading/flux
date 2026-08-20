/**
 * Tests de las reglas del protocolo que no necesitan broker.
 * Los que sí lo necesitan viven en conformance/.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";

import {
  parseSubject,
  isValidSubject,
  subjectToType,
  streamName,
  dlqStreamName,
  durableName,
  dlqSubject,
  sourceUri,
  InvalidSubjectError,
  CONSUMER_DEFAULTS,
} from "../src/protocol.js";

describe("naming de subjects", () => {
  test("acepta la forma canónica de 4 tokens", () => {
    const p = parseSubject("pedidos.pedido.v1.creado");
    assert.deepEqual(p, {
      domain: "pedidos",
      aggregate: "pedido",
      major: 1,
      event: "creado",
    });
  });

  test("acepta kebab-case en dominio, agregado y evento", () => {
    const p = parseSubject("logistica.linea-envio.v2.entrega-fallida");
    assert.equal(p.aggregate, "linea-envio");
    assert.equal(p.event, "entrega-fallida");
    assert.equal(p.major, 2);
  });

  test("rechaza mayúsculas con un mensaje que explica el subject fantasma", () => {
    // Es el fallo más peligroso: NATS lo acepta sin error y el evento no llega a nadie.
    assert.throws(
      () => parseSubject("Pedidos.pedido.v1.creado"),
      (e: Error) => e instanceof InvalidSubjectError && /minúsculas/.test(e.message),
    );
  });

  test("rechaza un número de tokens distinto de 4", () => {
    assert.throws(() => parseSubject("pedidos.crear-pedido"), /exactamente 4 tokens/);
    assert.throws(() => parseSubject("pedidos.pedido.v1.creado.retry"), /exactamente 4 tokens/);
  });

  test("rechaza un mayor ausente o mal formado", () => {
    assert.equal(isValidSubject("pedidos.pedido.1.creado"), false);
    assert.equal(isValidSubject("pedidos.pedido.v0.creado"), false);
    assert.equal(isValidSubject("pedidos.pedido.vx.creado"), false);
  });

  test("rechaza guiones bajos: solo kebab-case", () => {
    assert.equal(isValidSubject("pedidos.linea_envio.v1.creado"), false);
  });
});

describe("derivación de type", () => {
  test("mapea subject a reverse-DNS con el mayor al final", () => {
    assert.equal(
      subjectToType("pedidos.pedido.v1.creado"),
      "com.flux.pedidos.pedido.creado.v1",
    );
    assert.equal(
      subjectToType("facturacion.factura.v2.emitida"),
      "com.flux.facturacion.factura.emitida.v2",
    );
  });
});

describe("nombres que NATS restringe", () => {
  test("los streams no llevan puntos", () => {
    assert.equal(streamName("pedidos"), "EVT_PEDIDOS");
    assert.equal(dlqStreamName("pedidos"), "DLQ_PEDIDOS");
    assert.ok(!streamName("logistica-inversa").includes("."));
    assert.equal(streamName("logistica-inversa"), "EVT_LOGISTICA_INVERSA");
  });

  test("los durable consumers no llevan puntos y son reversibles", () => {
    const d = durableName("facturacion-api", "pedidos.pedido.v1.creado");
    assert.equal(d, "facturacion-api__pedidos_pedido_v1_creado");
    assert.ok(!d.includes("."));
    // Partir por `__` recupera el servicio: sin eso, `nats consumer ls` es inútil.
    assert.equal(d.split("__")[0], "facturacion-api");
  });

  test("el durable valida el subject en vez de generar un nombre corrupto", () => {
    assert.throws(() => durableName("svc", "no-es-un-subject"), InvalidSubjectError);
  });
});

describe("subject de DLQ", () => {
  test("es prefijo, nunca sufijo", () => {
    // Un sufijo encajaría con `pedidos.>` y el stream principal capturaría sus
    // propios muertos. Verificado contra NATS real en conformance/.
    const d = dlqSubject("pedidos.pedido.v1.creado");
    assert.equal(d, "dlq.pedidos.pedido.v1.creado");
    assert.ok(d.startsWith("dlq."));
    assert.ok(!d.endsWith(".dlq"));
  });
});

describe("source", () => {
  test("es /<entorno>/<servicio>", () => {
    assert.equal(sourceUri("produccion", "pedidos-api"), "/produccion/pedidos-api");
  });
});

describe("invariantes de la configuración de consumidor", () => {
  test("ack_wait es igual a backoff[0]", () => {
    // JetStream sobrescribe ack_wait con backoff[0] sin avisar. Si este test falla,
    // los handlers se ejecutan en concurrencia consigo mismos. Ver 03-delivery.md §2.1.
    assert.equal(CONSUMER_DEFAULTS.ackWaitMs, CONSUMER_DEFAULTS.backoffMs[0]);
  });

  test("max_deliver cuadra con el número de entradas de backoff", () => {
    // 1 entrega inicial + N reintentos. Si no cuadran, la última entrada de backoff
    // no se aplicaría nunca y la config mentiría sobre su comportamiento.
    assert.equal(CONSUMER_DEFAULTS.maxDeliver, CONSUMER_DEFAULTS.backoffMs.length + 1);
  });

  test("backoff[0] deja margen suficiente al handler", () => {
    assert.ok(
      CONSUMER_DEFAULTS.backoffMs[0] >= 30_000,
      "backoff[0] es el presupuesto de duración del handler; por debajo de 30s se reentrega mientras aún se ejecuta",
    );
  });
});
