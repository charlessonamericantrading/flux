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

  // ── Códigos de error estables — 04-errors.md §5 ──────────────────────────────
  // MAYÚSCULAS y guiones bajos: es lo que va en `code` de las métricas y encabeza `dlqerror`.
  // Un código con minúsculas o con guiones es otra serie temporal, no el mismo código.
  ["errors.codePattern",
    ["NOT_PROVISIONED", "PERMISSION_DENIED", "INVALID_SIGNATURE", "HTTP_503", "PEDIDO_YA_CANCELADO",
     "CONNECTION_REJECTED", "INVALID_CREDENTIALS", "INVALID_TLS_OPTIONS"],
    ["not_provisioned", "NotProvisioned", "NOT-PROVISIONED", "NOT PROVISIONED", "_NOT_PROVISIONED",
     "NOT_PROVISIONED_", "NOT__PROVISIONED", "NOT_PROVISIONED ", "503_HTTP", "",
     // La forma de Node en la clase, no el código: `ConnectionRejectedError` no es un `code`.
     "ConnectionRejectedError", "connection_rejected", "INVALID-TLS-OPTIONS"]],

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

// ─── Los tres códigos de connect() — 04-errors.md §5.3 ───────────────────────────────────────
// Se escriben como función PURA sobre el documento para poder probarla en los dos sentidos: la
// de abajo se ejecuta sobre el protocol.json real (debe dar cero fallos) y sobre copias
// estropeadas a propósito (cada una debe dar al menos uno). Una invariante que solo se ha visto
// en verde no demuestra nada.
const CODIGOS_CONNECT = ["CONNECTION_REJECTED", "INVALID_CREDENTIALS", "INVALID_TLS_OPTIONS"];

/** @returns {string[]} */
function fallosDeConnect(doc) {
  const f = [];
  const cs = doc.errors?.codes ?? {};
  for (const c of CODIGOS_CONNECT) {
    const def = cs[c];
    if (!def) { f.push(`errors.codes.${c}: falta (04-errors.md §5.3)`); continue; }
    if (def.phase !== "connect") f.push(`errors.codes.${c}.phase debe ser "connect"`);
  }
  // CONNECTION_REJECTED: dos motivos, y ni uno más — la librería de NATS no distingue más.
  if (JSON.stringify(cs.CONNECTION_REJECTED?.reasons) !== JSON.stringify(["auth", "tls"])) {
    f.push(`errors.codes.CONNECTION_REJECTED.reasons debe ser ["auth","tls"], es ${JSON.stringify(cs.CONNECTION_REJECTED?.reasons)}`);
  }
  // Qué los distingue es si hubo red: las dos opciones mal formadas fallan ANTES de abrir un
  // socket; el rechazo supone que el broker contestó.
  if (cs.CONNECTION_REJECTED?.failsBeforeNetwork !== false) f.push("errors.codes.CONNECTION_REJECTED.failsBeforeNetwork debe ser false: el broker contestó");
  for (const c of ["INVALID_CREDENTIALS", "INVALID_TLS_OPTIONS"]) {
    if (cs[c]?.failsBeforeNetwork !== true) f.push(`errors.codes.${c}.failsBeforeNetwork debe ser true: se detecta sin hablar con el broker`);
  }
  // Ninguno se emite con un broker que aún no ha arrancado: se espera.
  for (const c of CODIGOS_CONNECT) {
    if (!cs[c]?.neverEmittedFor?.includes("broker-not-started")) f.push(`errors.codes.${c}.neverEmittedFor debe incluir "broker-not-started": un broker caído se espera`);
  }
  // La tabla de 03-delivery.md §6.1: cada causa que falla rápido tiene su código, y existe.
  const fc = doc.security?.sdk?.firstConnect ?? {};
  if ("stableCode" in fc) f.push("security.sdk.firstConnect.stableCode (singular) ya no existe: ahora son stableCodes (04-errors.md §5.3)");
  const stable = fc.stableCodes ?? {};
  const causas = [...(fc.failFastOn ?? []), ...(fc.shouldFailFastOn ?? [])];
  for (const causa of causas) {
    if (!(causa in stable)) f.push(`security.sdk.firstConnect.stableCodes no dice qué código lleva «${causa}»`);
  }
  for (const [causa, cod] of Object.entries(stable)) {
    for (const c of [].concat(cod)) {
      if (!CODIGOS_CONNECT.includes(c)) f.push(`stableCodes.${causa}: "${c}" no es uno de los tres códigos de connect()`);
    }
  }
  // Los dos únicos fallos «sin red» son opciones mal formadas; el resto son rechazos.
  for (const causa of ["credentials-rejected", "tls-verification-failed", "tls-required-not-offered"]) {
    if (stable[causa] !== "CONNECTION_REJECTED") f.push(`stableCodes.${causa} debe ser CONNECTION_REJECTED`);
  }
  if (JSON.stringify(stable["malformed-local-options"]) !== JSON.stringify(["INVALID_CREDENTIALS", "INVALID_TLS_OPTIONS"])) {
    f.push("stableCodes.malformed-local-options debe ser [INVALID_CREDENTIALS, INVALID_TLS_OPTIONS]");
  }
  // 06-security.md §3.7: el pull denegado DEBE hacer fallar subscribe() (ya no DEBERÍA).
  const den = doc.security?.sdk?.denials ?? {};
  if ("shouldFailSubscribe" in den) f.push("security.sdk.denials.shouldFailSubscribe ya no existe: es mustFailSubscribe (06-security.md §3.7, DEBE)");
  if (!den.mustFailSubscribe?.includes("pull")) f.push('security.sdk.denials.mustFailSubscribe debe incluir "pull"');
  if (den.mustFailSubscribeWith !== "PERMISSION_DENIED" || !cs.PERMISSION_DENIED) f.push("security.sdk.denials.mustFailSubscribeWith debe ser PERMISSION_DENIED, que debe existir en errors.codes");
  if (!den.mustLog?.includes("ack") || !den.mustLog?.includes("pull")) f.push('security.sdk.denials.mustLog debe incluir "ack" y "pull": se registran los dos');
  if (JSON.stringify(den.mustLogOnly) !== JSON.stringify(["ack"])) f.push('security.sdk.denials.mustLogOnly debe ser ["ack"]: lo único que SOLO puede salir por el logger');
  return f;
}

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
if (acl.createGrantedByDefault !== false) fallos.push("security.acl.createGrantedByDefault debe ser false: CONSUMER.CREATE solo con --dev");
if (JSON.stringify(acl.devOnlyPatterns) !== JSON.stringify(["eventConsumerCreate", "dlqReaderCreate"])) fallos.push("security.acl.devOnlyPatterns debe ser [eventConsumerCreate, dlqReaderCreate]");
if (acl.subscribeToDomainSubjects !== "never") fallos.push("security.acl.subscribeToDomainSubjects debe ser \"never\"");
if (acl.inbox.private !== true || acl.inbox.setBySdkFromServiceName !== true) fallos.push("security.acl.inbox: el inbox debe ser privado y fijado por el SDK");
if (acl.ackSubject.format.split(".").length !== acl.ackSubject.tokens) fallos.push(`security.acl.ackSubject: el formato tiene ${acl.ackSubject.format.split(".").length} tokens y dice ${acl.ackSubject.tokens}`);
if (p.security.provisioning.notProvisionedErrorCode !== "NOT_PROVISIONED") fallos.push("security.provisioning.notProvisionedErrorCode debe ser NOT_PROVISIONED");
if (p.security.provisioning.neverDeletes !== true || p.security.provisioning.neverOverwritesDifferentConfig !== true) fallos.push("security.provisioning: nunca borra ni sobrescribe");

// ─── Códigos de error y capacidades: 04-errors.md §5 y 00-protocol.md §5 ──────────────────────
// Lo que mira el SDK en `connect()` y en `publish()` es lo que dice aquí, así que un código
// huérfano (sin capacidad), una clase que no existe o una capacidad declarada pero sin definir
// son errores de esta tabla y no de ningún SDK. Que cada SDK CONTENGA sus códigos lo comprueba
// scripts/check-sdk-codes.mjs, que necesita leer sus fuentes.
const codePat = new RegExp(p.errors.codePattern);
const capDefs = p.capabilities.definitions;
const codigosDe = Object.entries(p.errors.codes).filter(([k]) => !k.startsWith("$"));
for (const [codigo, def] of codigosDe) {
  if (!codePat.test(codigo)) fallos.push(`errors.codes.${codigo}: no cumple errors.codePattern`);
  if (!Object.hasOwn(p.errors.classes, def.class)) fallos.push(`errors.codes.${codigo}: class "${def.class}" no es una clase de errors.classes`);
  if (!Object.hasOwn(capDefs, def.capability)) fallos.push(`errors.codes.${codigo}: capability "${def.capability}" no está en capabilities.definitions`);
}
for (const codigo of Object.keys(p.signing.poisonCodes)) {
  if (!codePat.test(codigo)) fallos.push(`signing.poisonCodes.${codigo}: no cumple errors.codePattern`);
  if (p.errors.codes[codigo]) fallos.push(`${codigo} está en errors.codes y en signing.poisonCodes: una sola fuente por código`);
}
// 04-errors.md §5.1: una denegación no se arregla esperando, así que NO es RETRYABLE ni queda
// a la política de lo desconocido.
for (const codigo of ["NOT_PROVISIONED", "PERMISSION_DENIED", ...CODIGOS_CONNECT]) {
  if (p.errors.codes[codigo]?.class !== "permanent") fallos.push(`errors.codes.${codigo}.class debe ser "permanent"`);
  if (p.errors.codes[codigo]?.capability !== "security") fallos.push(`errors.codes.${codigo}.capability debe ser "security"`);
}
fallos.push(...fallosDeConnect(p));
if (!Object.hasOwn(p.errors.codes, p.security.provisioning.notProvisionedErrorCode)) fallos.push("security.provisioning.notProvisionedErrorCode no está en errors.codes");
if (!Object.hasOwn(capDefs, p.security.sdk.capability)) fallos.push(`security.sdk.capability "${p.security.sdk.capability}" no está en capabilities.definitions`);

for (const [sdk, decl] of Object.entries(p.capabilities.sdks)) {
  const dadas = decl.capabilities;
  if (!Array.isArray(dadas)) { fallos.push(`capabilities.sdks.${sdk}.capabilities debe ser una lista (vacía si no declara ninguna)`); continue; }
  if (new Set(dadas).size !== dadas.length) fallos.push(`capabilities.sdks.${sdk}: capacidad repetida en [${dadas.join(", ")}]`);
  for (const cap of dadas) {
    if (!Object.hasOwn(capDefs, cap)) { fallos.push(`capabilities.sdks.${sdk}: declara "${cap}", que no está en capabilities.definitions`); continue; }
    if (capDefs[cap].evidence === "sdk-files" && !decl.evidence?.[cap]?.length) {
      fallos.push(`capabilities.sdks.${sdk}: declara "${cap}" sin citar su evidencia (capabilities.sdks.${sdk}.evidence.${cap}). Una capacidad sin evidencia ejecutada es una intención`);
    }
  }
  for (const cap of Object.keys(decl.evidence ?? {})) {
    if (!dadas.includes(cap)) fallos.push(`capabilities.sdks.${sdk}: cita evidencia de "${cap}" sin declararla`);
  }
}

// El generador y protocol.json tienen que decir lo mismo: cada permiso que emite encaja con UNA
// categoría concedible y con ninguna denegable, y su inbox con el patrón del inbox.
const servicioDePrueba = {
  name: "logistica-api",
  owns: ["logistica"],
  subjects: ["facturacion.factura.v2.emitida", "pedidos.pedido.v1.cancelado", "pedidos.pedido.v1.creado"],
};
// El modo por defecto se prueba SIN pasar opciones: es lo que ejecuta quien no pide nada.
for (const dev of [false, true]) {
  const modo = dev ? "--dev" : "por defecto";
  const emitidoModo = dev ? permisosDe(servicioDePrueba, { dev }) : permisosDe(servicioDePrueba);
  for (const permiso of emitidoModo.publish) {
    const categorias = patrones.filter(([, re]) => re.test(permiso)).map(([k]) => k);
    if (categorias.length !== 1) fallos.push(`el generador (${modo}) emite "${permiso}", que encaja con ${categorias.length} categoría(s) de security.acl.permissionPatterns: [${categorias.join(", ")}]`);
    if (denegable.test(permiso)) fallos.push(`el generador (${modo}) emite "${permiso}", que security.acl.neverGrantedPattern declara denegable`);
    // CONSUMER.CREATE solo con --dev: es lo que protocol.json declara, y lo que el generador cumple.
    if (!dev && categorias.some((k) => acl.devOnlyPatterns.includes(k))) fallos.push(`el generador (por defecto) emite "${permiso}", que solo debe concederse con --dev`);
  }
  const creates = emitidoModo.publish.filter((x) => patrones.some(([k, re]) => acl.devOnlyPatterns.includes(k) && re.test(x)));
  const esperados = dev ? emitidoModo.durables.length : 0;
  if (creates.length !== esperados) fallos.push(`el generador (${modo}) emite ${creates.length} permisos de CONSUMER.CREATE y debería emitir ${esperados} (uno por durable propio, solo con --dev)`);
  for (const permiso of emitidoModo.subscribe) {
    if (!new RegExp(acl.inbox.permissionPattern).test(permiso)) fallos.push(`el generador (${modo}) emite una suscripción "${permiso}" que no es el inbox privado`);
  }
  if (emitidoModo.subscribe.length !== 1) fallos.push(`el generador (${modo}) emite ${emitidoModo.subscribe.length} suscripciones; solo debe haber una (el inbox)`);
  // En NATS `allow: []` significa «sin restricción»: ninguna lista puede salir vacía.
  if (!emitidoModo.publish.length) fallos.push(`el generador (${modo}) emite una lista de publicación vacía, que NATS interpreta como «sin restricción»`);
}
const emitido = permisosDe(servicioDePrueba);
// …y el nombre del durable de DLQ que emite cumple el patrón que protocol.json le da.
const dlqDurables = emitido.durables.filter((d) => d.kind === "dlq").map((d) => d.durable);
if (!dlqDurables.length || !dlqDurables.every((d) => new RegExp(acl.dlqReaderDurable.pattern).test(d))) fallos.push(`los durables de DLQ que emite el generador (${dlqDurables.join(", ")}) no cumplen security.acl.dlqReaderDurable.pattern`);

// Autocomprobación de `fallosDeConnect`: cada copia estropeada DEBE dar un fallo, o la invariante
// no vigila nada.
{
  const mutaciones = {
    "quitar CONNECTION_REJECTED": (d) => { delete d.errors.codes.CONNECTION_REJECTED; },
    "un tercer motivo": (d) => { d.errors.codes.CONNECTION_REJECTED.reasons.push("mtls"); },
    "quitar un motivo": (d) => { d.errors.codes.CONNECTION_REJECTED.reasons = ["tls"]; },
    "INVALID_CREDENTIALS con red": (d) => { d.errors.codes.INVALID_CREDENTIALS.failsBeforeNetwork = false; },
    "INVALID_TLS_OPTIONS con red": (d) => { d.errors.codes.INVALID_TLS_OPTIONS.failsBeforeNetwork = false; },
    "CONNECTION_REJECTED sin red": (d) => { d.errors.codes.CONNECTION_REJECTED.failsBeforeNetwork = true; },
    "un código esperaría a un broker caído": (d) => { d.errors.codes.INVALID_CREDENTIALS.neverEmittedFor = []; },
    "volver al stableCode sin decidir": (d) => { d.security.sdk.firstConnect.stableCode = null; },
    "una causa sin código": (d) => { delete d.security.sdk.firstConnect.stableCodes["tls-required-not-offered"]; },
    "una causa con el código equivocado": (d) => { d.security.sdk.firstConnect.stableCodes["credentials-rejected"] = "INVALID_CREDENTIALS"; },
    "un código ajeno en stableCodes": (d) => { d.security.sdk.firstConnect.stableCodes["tls-verification-failed"] = "PERMISSION_DENIED"; },
    "opciones mal formadas con un solo código": (d) => { d.security.sdk.firstConnect.stableCodes["malformed-local-options"] = ["INVALID_CREDENTIALS"]; },
    "el pull vuelve a DEBERÍA": (d) => { d.security.sdk.denials.shouldFailSubscribe = ["pull"]; delete d.security.sdk.denials.mustFailSubscribe; },
    "subscribe falla con otro código": (d) => { d.security.sdk.denials.mustFailSubscribeWith = "NOT_PROVISIONED"; },
    "el pull deja de registrarse": (d) => { d.security.sdk.denials.mustLog = ["ack"]; },
    "el pull pasa a ser solo-log": (d) => { d.security.sdk.denials.mustLogOnly = ["ack", "pull"]; },
  };
  for (const [nombre, estropea] of Object.entries(mutaciones)) {
    const copia = structuredClone(p);
    estropea(copia);
    if (!fallosDeConnect(copia).length) fallos.push(`autocomprobación: la mutación «${nombre}» NO la detecta fallosDeConnect`);
  }
}

if (fallos.length) {
  console.error(`\n✗ ${fallos.length} problema(s) en protocol.json\n`);
  for (const f of fallos) console.error(`  ✗ ${f}`);
  process.exit(1);
}

console.log(`✓ protocol.json coherente (${CASES.length} patrones + invariantes numéricas, de códigos y de capacidades)`);
