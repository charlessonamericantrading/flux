import { test, describe } from "node:test";
import assert from "node:assert/strict";

import { InMemoryMetrics, NO_METRICS, DURATION_BUCKETS } from "../src/metrics.js";
import { CONSUMER_DEFAULTS } from "../src/protocol.js";

describe("buckets del histograma", () => {
  test("el último bucket es el ack_wait", () => {
    // 08-observability.md §3: un handler en el bucket superior está a punto de que su
    // mensaje se reentregue mientras aún se ejecuta. Si el bucket no coincide con el
    // plazo real, mide algo que no le importa a nadie.
    const ultimo = DURATION_BUCKETS[DURATION_BUCKETS.length - 1];
    assert.equal(ultimo, CONSUMER_DEFAULTS.ackWaitMs / 1000);
  });

  test("están ordenados de forma ascendente", () => {
    for (let i = 1; i < DURATION_BUCKETS.length; i++) {
      assert.ok(DURATION_BUCKETS[i] > DURATION_BUCKETS[i - 1]);
    }
  });
});

describe("recolector", () => {
  test("cuenta publicaciones por subject y resultado", () => {
    const m = new InMemoryMetrics();
    m.eventPublished("pedidos.pedido.v1.creado", "ok");
    m.eventPublished("pedidos.pedido.v1.creado", "ok");
    m.eventPublished("pedidos.pedido.v1.creado", "invalid_schema");
    const { counters } = m.snapshot();
    assert.equal(
      counters.get('flux_events_published_total{outcome="ok",subject="pedidos.pedido.v1.creado"}'),
      2,
    );
    assert.equal(
      counters.get(
        'flux_events_published_total{outcome="invalid_schema",subject="pedidos.pedido.v1.creado"}',
      ),
      1,
    );
  });

  test("las etiquetas se ordenan para que la clave sea estable", () => {
    // Sin orden estable, la misma serie temporal aparecería con dos claves según el
    // orden en que se construyó el objeto.
    const a = new InMemoryMetrics();
    const b = new InMemoryMetrics();
    a.eventDlq("s", "c", "permanent", "X");
    b.eventDlq("s", "c", "permanent", "X");
    assert.deepEqual([...a.snapshot().counters.keys()], [...b.snapshot().counters.keys()]);
  });

  test("el histograma acumula en todos los buckets que superan el valor", () => {
    const m = new InMemoryMetrics();
    m.handlerDuration("s", "c", 0.03); // cae por encima de 0.025
    const salida = m.render();
    assert.match(salida, /_bucket\{[^}]*le="0\.025"\} 0/);
    assert.match(salida, /_bucket\{[^}]*le="0\.05"\} 1/);
    assert.match(salida, /_bucket\{[^}]*le="\+Inf"\} 1/);
    assert.match(salida, /_count\{[^}]*\} 1/);
  });

  test("un gauge sin etiquetas no deja llaves vacías en la salida", () => {
    const m = new InMemoryMetrics();
    m.connectionState(1);
    assert.match(m.render(), /^flux_connection_state 1$/m);
  });

  test("las líneas de exposición tienen forma válida de Prometheus", () => {
    const m = new InMemoryMetrics();
    m.eventPublished("pedidos.pedido.v1.creado", "ok");
    m.eventConsumed("pedidos.pedido.v1.creado", "facturacion-api__x", "ok");
    m.eventDlq("pedidos.pedido.v1.creado", "facturacion-api__x", "permanent", "HTTP_404");
    m.eventRetried("pedidos.pedido.v1.creado", "facturacion-api__x", 3);
    m.consumerPending("pedidos.pedido.v1.creado", "facturacion-api__x", 42);
    m.connectionState(1);
    m.handlerDuration("pedidos.pedido.v1.creado", "facturacion-api__x", 0.4);

    for (const linea of m.render().split("\n")) {
      if (linea === "" || linea.startsWith("#")) continue;
      assert.match(
        linea,
        /^[a-z_]+(\{[^}]*\})? -?[0-9.]+$/,
        `línea no válida para Prometheus: ${JSON.stringify(linea)}`,
      );
    }
  });

  test("escapa las comillas CONSERVANDO el valor", () => {
    // Se escapa, no se sustituye: reemplazar por `_` no rompería el scrape pero
    // dejaría dos errores distintos bajo la misma etiqueta. Python y Go hacen lo
    // mismo; una divergencia aquí partiría la agregación entre SDKs.
    const m = new InMemoryMetrics();
    m.eventDlq("s", "c", "permanent", 'dice "hola"');
    const salida = m.render();
    assert.match(salida, /code="dice \\"hola\\""/);
  });

  test("escapa la barra invertida ANTES que las comillas", () => {
    // Al revés, se escaparían las barras recién introducidas y una `"` acabaría como
    // `\\\\"` — el valor saldría corrupto sin que nada fallara.
    const m = new InMemoryMetrics();
    m.eventDlq("s", "c", "permanent", 'ruta C:\\tmp');
    const linea = m
      .render()
      .split("\n")
      .find((l) => l.startsWith("flux_events_dlq_total"))!;
    assert.ok(linea.includes("C:\\\\tmp"), `barra mal escapada: ${linea}`);
  });

  test("comillas desbalanceadas nunca rompen el scrape", () => {
    // Un `code` con comillas rompería el formato de exposición y Prometheus
    // descartaría el scrape entero, no solo esa línea.
    const m = new InMemoryMetrics();
    m.eventDlq("s", "c", "permanent", 'con "comillas"');
    for (const linea of m.render().split("\n")) {
      if (linea.startsWith("flux_events_dlq_total")) {
        assert.equal((linea.match(/"/g) ?? []).length % 2, 0, "comillas desbalanceadas");
      }
    }
  });
});

describe("NO_METRICS", () => {
  test("no lanza y no guarda nada — es el default", () => {
    // Un SDK no debe imponer un backend de métricas.
    assert.doesNotThrow(() => {
      NO_METRICS.eventPublished("s", "ok");
      NO_METRICS.eventDlq("s", "c", "poison", "X");
      NO_METRICS.connectionState(0);
    });
  });
});
