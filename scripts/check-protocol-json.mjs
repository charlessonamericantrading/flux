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

import { permisosDe } from "./lib/manifest.mjs";

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

  // ── Permisos de la ACL — 06-security.md §3 ───────────────────────────────────
  // Una lista CERRADA de formas concedibles. Cada patrón se prueba en los dos sentidos: lo
  // exacto pasa, y la forma ancha («para que funcione») falla. Es la diferencia entre un
  // permiso por durable y `$JS.API.>`.

  // El inbox es PRIVADO: `_INBOX.>` común lo lee todo (A-2).
  ["security.acl.inbox.permissionPattern",
    ["_INBOX.pedidos-api.>", "_INBOX.a.>"],
    ["_INBOX.>", "_INBOX.pedidos.api.>", "_INBOX.Pedidos.>", "_INBOX.pedidos-api", "_INBOX.*",
     "_INBOX.pedidos-api.*", "_INBOX.pedidos_api.>"]],

  ["security.acl.permissionPatterns.accountInfo",
    ["$JS.API.INFO"],
    ["$JS.API.INFO.x", "$JS.API.>", "$JS.API.STREAM.INFO.EVT_PEDIDOS", "$JS.API.info"]],

  ["security.acl.permissionPatterns.ownDomain",
    ["pedidos.>", "linea-envio.>"],
    [">", "pedidos.pedido.>", "Pedidos.>", "*.>", "pedidos.*", "pedidos", "dlq.pedidos.pedido.v1.creado"]],

  ["security.acl.permissionPatterns.streamInfo",
    ["$JS.API.STREAM.INFO.EVT_PEDIDOS", "$JS.API.STREAM.INFO.DLQ_LINEA_ENVIO"],
    ["$JS.API.STREAM.INFO.>", "$JS.API.STREAM.INFO.*", "$JS.API.STREAM.INFO.evt_pedidos",
     "$JS.API.STREAM.INFO.EVT.PEDIDOS", "$JS.API.STREAM.CREATE.EVT_PEDIDOS", "$JS.API.STREAM.MSG.GET.EVT_PEDIDOS",
     "$JS.API.STREAM.INFO"]],

  // El filtro va EN el subject (A-4): subject exacto, nunca un comodín.
  ["security.acl.permissionPatterns.eventConsumerCreate",
    ["$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.pedidos.pedido.v1.creado",
     "$JS.API.CONSUMER.CREATE.EVT_LINEA_ENVIO.svc__linea_envio_v1_x.linea-envio.cosa.v2.hecha"],
    ["$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.pedidos.pedido.v1.>",
     "$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.pedidos.>",
     "$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.pedidos.pedido.v1.*",
     "$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado",
     "$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.>", "$JS.API.CONSUMER.CREATE.>",
     "$JS.API.CONSUMER.DURABLE.CREATE.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.pedidos.pedido.v1.creado",
     "$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.sinservicio.pedidos.pedido.v1.creado",
     "$JS.API.CONSUMER.CREATE.DLQ_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.pedidos.pedido.v1.creado"]],

  // La DLQ propia se lee con un durable de filtro `dlq.<dominio>.>` (06-security.md §3.1).
  ["security.acl.permissionPatterns.dlqReaderCreate",
    ["$JS.API.CONSUMER.CREATE.DLQ_PEDIDOS.pedidos-api__dlq_pedidos.dlq.pedidos.>"],
    ["$JS.API.CONSUMER.CREATE.DLQ_PEDIDOS.pedidos-api__dlq_pedidos.dlq.>",
     "$JS.API.CONSUMER.CREATE.DLQ_PEDIDOS.pedidos-api__dlq_pedidos.dlq.pedidos.pedido.v1.creado",
     "$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.pedidos-api__dlq_pedidos.dlq.pedidos.>",
     "$JS.API.CONSUMER.CREATE.DLQ_PEDIDOS.pedidos-api__pedidos_pedido_v1_creado.dlq.pedidos.>",
     "$JS.API.CONSUMER.CREATE.DLQ_PEDIDOS.pedidos-api__dlq_pedidos.dlq.pedidos.*"]],

  ["security.acl.permissionPatterns.consumerInfo",
    ["$JS.API.CONSUMER.INFO.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado",
     "$JS.API.CONSUMER.INFO.DLQ_PEDIDOS.pedidos-api__dlq_pedidos"],
    ["$JS.API.CONSUMER.INFO.EVT_PEDIDOS.>", "$JS.API.CONSUMER.INFO.EVT_PEDIDOS.*", "$JS.API.CONSUMER.INFO.EVT_PEDIDOS",
     "$JS.API.CONSUMER.INFO.>", "$JS.API.CONSUMER.NAMES.EVT_PEDIDOS", "$JS.API.CONSUMER.LIST.EVT_PEDIDOS",
     "$JS.API.CONSUMER.INFO.EVT_PEDIDOS.sinservicio"]],

  ["security.acl.permissionPatterns.consumerNext",
    ["$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado",
     "$JS.API.CONSUMER.MSG.NEXT.DLQ_PEDIDOS.pedidos-api__dlq_pedidos"],
    ["$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.>", "$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.*",
     "$JS.API.CONSUMER.MSG.NEXT.>", "$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS", "$JS.API.CONSUMER.MSG.GET.EVT_PEDIDOS.x__y"]],

  ["security.acl.permissionPatterns.ack",
    ["$JS.ACK.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.>",
     "$JS.ACK.DLQ_PEDIDOS.pedidos-api__dlq_pedidos.>"],
    ["$JS.ACK.>", "$JS.ACK.EVT_PEDIDOS.>", "$JS.ACK.EVT_PEDIDOS.*.>",
     "$JS.ACK.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado", "$JS.ACK.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.*"]],

  // En la DLQ se publica en el subject EXACTO de lo que se consume, no en `dlq.>`.
  ["security.acl.permissionPatterns.dlqPublish",
    ["dlq.pedidos.pedido.v1.creado", "dlq.logistica.linea-envio.v2.entrega-fallida"],
    ["dlq.>", "dlq.pedidos.>", "dlq.pedidos.pedido.v1.>", "dlq.pedidos.pedido.v1.creado.retry",
     "pedidos.pedido.v1.creado", "DLQ.pedidos.pedido.v1.creado", "dlq.Pedidos.pedido.v1.creado", "dlq.pedidos.pedido.*.creado"]],

  ["security.acl.dlqReaderDurable.pattern",
    ["pedidos-api__dlq_pedidos", "facturacion-api__dlq_linea_envio"],
    ["pedidos-api__pedidos_pedido_v1_creado", "pedidos-api__dlq", "PedidosApi__dlq_pedidos", "dlq_pedidos",
     "pedidos.api__dlq_pedidos"]],

  // Lo que NUNCA se concede. Aquí «debe pasar» significa «es denegable»: el patrón tiene que
  // CAZAR lo peligroso y NO cazar nada de lo concedible.
  ["security.acl.neverGrantedPattern",
    [">", "$JS.>", "$JS.API.>", "$JS.API.*", "$JS.API.*.>",
     "$JS.API.CONSUMER.DURABLE.CREATE.>", "$JS.API.CONSUMER.DURABLE.CREATE.EVT_PEDIDOS.x",
     "$JS.API.STREAM.MSG.GET.>", "$JS.API.STREAM.MSG.GET.EVT_PEDIDOS", "$JS.API.DIRECT.GET.EVT_PEDIDOS",
     "$JS.API.STREAM.CREATE.EVT_X", "$JS.API.STREAM.UPDATE.EVT_PEDIDOS", "$JS.API.STREAM.DELETE.EVT_PEDIDOS",
     "$JS.API.STREAM.PURGE.EVT_PEDIDOS", "$JS.API.STREAM.NAMES", "$JS.API.STREAM.LIST",
     "$JS.API.CONSUMER.DELETE.EVT_PEDIDOS.x__y", "$JS.API.CONSUMER.NAMES.EVT_PEDIDOS", "$JS.API.CONSUMER.LIST.EVT_PEDIDOS"],
    ["$JS.API.INFO", "$JS.API.STREAM.INFO.EVT_PEDIDOS",
     "$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.pedidos.pedido.v1.creado",
     "$JS.API.CONSUMER.INFO.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado",
     "$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado",
     "$JS.ACK.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.>",
     "pedidos.>", "dlq.pedidos.pedido.v1.creado", "_INBOX.pedidos-api.>"]],
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

// ─── La ACL: lo concedible y lo denegable no se solapan, y el generador lo cumple ─────────────
const acl = p.security.acl;
const patrones = Object.entries(acl.permissionPatterns)
  .filter(([k]) => !k.startsWith("$"))
  .map(([k, v]) => [k, new RegExp(v)]);
const denegable = new RegExp(acl.neverGrantedPattern);

for (const literal of acl.neverGranted) {
  if (!denegable.test(literal)) fallos.push(`security.acl.neverGranted: "${literal}" no lo caza neverGrantedPattern`);
  const cazado = patrones.find(([, re]) => re.test(literal));
  if (cazado) fallos.push(`security.acl: "${literal}" es denegable pero lo concede permissionPatterns.${cazado[0]}`);
}
if (p.security.subscribeScope !== "private-inbox-only") fallos.push(`security.subscribeScope debe ser "private-inbox-only", es ${JSON.stringify(p.security.subscribeScope)}`);
if (acl.subscribeToDomainSubjects !== "never") fallos.push("security.acl.subscribeToDomainSubjects debe ser \"never\"");
if (acl.inbox.private !== true || acl.inbox.setBySdkFromServiceName !== true) fallos.push("security.acl.inbox: el inbox debe ser privado y fijado por el SDK");
if (acl.ackSubject.format.split(".").length !== acl.ackSubject.tokens) fallos.push(`security.acl.ackSubject: el formato tiene ${acl.ackSubject.format.split(".").length} tokens y dice ${acl.ackSubject.tokens}`);
if (p.security.provisioning.notProvisionedErrorCode !== "NOT_PROVISIONED") fallos.push("security.provisioning.notProvisionedErrorCode debe ser NOT_PROVISIONED");
if (p.security.provisioning.neverDeletes !== true || p.security.provisioning.neverOverwritesDifferentConfig !== true) fallos.push("security.provisioning: nunca borra ni sobrescribe");

// El generador y protocol.json tienen que decir lo mismo: cada permiso que emite encaja con UNA
// categoría concedible y con ninguna denegable, y su inbox con el patrón del inbox.
const servicioDePrueba = {
  name: "logistica-api",
  owns: ["logistica"],
  subjects: ["facturacion.factura.v2.emitida", "pedidos.pedido.v1.cancelado", "pedidos.pedido.v1.creado"],
};
const emitido = permisosDe(servicioDePrueba);
for (const permiso of emitido.publish) {
  const categorias = patrones.filter(([, re]) => re.test(permiso)).map(([k]) => k);
  if (categorias.length !== 1) fallos.push(`el generador emite "${permiso}", que encaja con ${categorias.length} categoría(s) de security.acl.permissionPatterns: [${categorias.join(", ")}]`);
  if (denegable.test(permiso)) fallos.push(`el generador emite "${permiso}", que security.acl.neverGrantedPattern declara denegable`);
}
for (const permiso of emitido.subscribe) {
  if (!new RegExp(acl.inbox.permissionPattern).test(permiso)) fallos.push(`el generador emite una suscripción "${permiso}" que no es el inbox privado`);
}
if (emitido.subscribe.length !== 1) fallos.push(`el generador emite ${emitido.subscribe.length} suscripciones; solo debe haber una (el inbox)`);
// …y el nombre del durable de DLQ que emite cumple el patrón que protocol.json le da.
const dlqDurables = emitido.durables.filter((d) => d.kind === "dlq").map((d) => d.durable);
if (!dlqDurables.length || !dlqDurables.every((d) => new RegExp(acl.dlqReaderDurable.pattern).test(d))) fallos.push(`los durables de DLQ que emite el generador (${dlqDurables.join(", ")}) no cumplen security.acl.dlqReaderDurable.pattern`);

if (fallos.length) {
  console.error(`\n✗ ${fallos.length} problema(s) en protocol.json\n`);
  for (const f of fallos) console.error(`  ✗ ${f}`);
  process.exit(1);
}

console.log(`✓ protocol.json coherente (${CASES.length} patrones + invariantes numéricas)`);
