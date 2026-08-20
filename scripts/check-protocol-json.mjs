#!/usr/bin/env node
/**
 * Verifica que los patrones de protocol.json compilan y aceptan/rechazan lo que dicen.
 *
 * Existe por un bug real: un script de edición se comió las barras invertidas y dejó
 * `"^d{4}-d{2}-..."` en lugar de `"^\\d{4}-\\d{2}-..."`. El JSON seguía siendo válido,
 * el regex seguía compilando, y rechazaba TODOS los timestamps correctos.
 *
 * protocol.json existe para que un agente VALIDE en vez de recordar. Un patrón roto
 * ahí no es un typo: es un agente concluyendo con seguridad que un evento correcto
 * es inválido.
 */

import { readFile } from "node:fs/promises";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const p = JSON.parse(await readFile(join(ROOT, "protocol.json"), "utf8"));

const get = (path) => path.split(".").reduce((o, k) => o?.[k], p);

/** [ruta del patrón, ejemplos que DEBEN pasar, ejemplos que DEBEN fallar] */
const CASES = [
  ["naming.subject.pattern",
    ["pedidos.pedido.v1.creado", "logistica.linea-envio.v2.entrega-fallida"],
    ["Pedidos.pedido.v1.creado", "pedidos.crear-pedido", "pedidos.pedido.v1.creado.retry",
     "pedidos.pedido.1.creado", "pedidos.pedido.v0.creado", "pedidos.linea_envio.v1.creado"]],

  ["naming.type.pattern",
    ["com.flux.pedidos.pedido.creado.v1", "com.flux.facturacion.factura.emitida.v2"],
    ["pedidos.pedido.v1.creado", "com.flux.pedidos.pedido.creado"]],

  ["naming.stream.eventPattern", ["EVT_PEDIDOS", "EVT_LOGISTICA_INVERSA"], ["EVT.PEDIDOS", "evt_pedidos", "DLQ_PEDIDOS"]],
  ["naming.stream.dlqPattern", ["DLQ_PEDIDOS"], ["DLQ.PEDIDOS", "EVT_PEDIDOS"]],

  ["naming.durableConsumer.pattern",
    ["facturacion-api__pedidos_pedido_v1_creado"],
    ["facturacion-api.pedidos", "FacturacionAPI__pedidos", "sinservicio"]],

  ["naming.service.pattern", ["pedidos-api", "facturacion"], ["FacturacionAPI", "facturacion_api", "svc.api", ""]],

  ["cloudevents.requiredAttributes.source.pattern",
    ["/produccion/pedidos-api"],
    ["produccion/pedidos-api", "/produccion", "/Produccion/pedidos-api"]],

  // El que estaba roto. Exactamente 3 decimales y sufijo Z — 01-envelope.md §2.2.
  ["cloudevents.requiredAttributes.time.pattern",
    ["2025-08-20T10:25:39.410Z", "2025-08-20T10:25:39.000Z"],
    ["2025-08-20T10:25:39.41Z",           // ceros recortados (Go RFC3339Nano)
     "2025-08-20T10:25:39.410000+00:00",  // microsegundos + offset (Python isoformat)
     "2025-08-20T10:25:39Z",              // sin decimales
     "2025-08-20T10:25:39.4100000Z"]],    // 7 decimales (.NET "O")

  ["cloudevents.extensionNameRule.pattern",
    ["correlationid", "tenantid", "dlqattempts"],
    ["correlation_id", "correlationId", "correlation-id", ""]],
];

const fallos = [];

for (const [path, deben, noDeben] of CASES) {
  const src = get(path);
  if (typeof src !== "string") {
    fallos.push(`${path}: no existe o no es una cadena`);
    continue;
  }
  let re;
  try {
    re = new RegExp(src);
  } catch (e) {
    fallos.push(`${path}: no compila — ${e.message}`);
    continue;
  }
  for (const ej of deben) {
    if (!re.test(ej)) fallos.push(`${path}: DEBERÍA aceptar ${JSON.stringify(ej)}\n      patrón: ${JSON.stringify(src)}`);
  }
  for (const ej of noDeben) {
    if (re.test(ej)) fallos.push(`${path}: DEBERÍA rechazar ${JSON.stringify(ej)}\n      patrón: ${JSON.stringify(src)}`);
  }
}

// Coherencias numéricas que la prosa afirma y que se pueden comprobar.
const c = p.consumer;
if (c.ackWaitSeconds * 1000 !== c.backoffMs[0]) {
  fallos.push(`consumer: ackWaitSeconds (${c.ackWaitSeconds}s) != backoffMs[0] (${c.backoffMs[0]}ms). JetStream sobrescribe ack_wait con backoff[0] — 03-delivery.md §2.1`);
}
if (c.maxDeliver !== c.backoffMs.length + 1) {
  fallos.push(`consumer: maxDeliver (${c.maxDeliver}) != backoffMs.length + 1 (${c.backoffMs.length + 1}). La última entrada de backoff no se aplicaría nunca`);
}
const suma = c.backoffMs.reduce((a, b) => a + b, 0) / 1000;
if (suma !== c.totalTimeToDlqSeconds) {
  fallos.push(`consumer: totalTimeToDlqSeconds (${c.totalTimeToDlqSeconds}) != sum(backoffMs) (${suma})`);
}
if (p.errors.budgets.recognizedRetryable !== c.maxDeliver) {
  fallos.push(`errors.budgets.recognizedRetryable (${p.errors.budgets.recognizedRetryable}) != consumer.maxDeliver (${c.maxDeliver})`);
}
if (p.errors.budgets.unknown !== p.errors.unknownRetryBudget) {
  fallos.push(`errors.budgets.unknown != errors.unknownRetryBudget`);
}

if (fallos.length) {
  console.error(`\n✗ ${fallos.length} problema(s) en protocol.json\n`);
  for (const f of fallos) console.error(`  ✗ ${f}`);
  process.exit(1);
}

console.log(`✓ protocol.json coherente (${CASES.length} patrones + invariantes numéricas)`);
