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

import { createHash, createHmac } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

import {
  OPERATION_RE,
  RESERVED_DOMAINS,
  RPC_ALLOW_RESPONSES,
  SERVICE_KEYS,
  STATE_CONTRACT_RE,
  STATE_TENANT_MAX,
  STATE_TENANT_RE,
  VIA_VALUES,
  analizarManifiesto,
  permisosDe,
} from "./lib/manifest.mjs";

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

  // ── Estado replicado — specification/10-state.md ─────────────────────────────────────────
  // Cada patrón, en los dos sentidos. Los de ACL son los que gen-acl.mjs debe cumplir cuando
  // genere permisos de estado; lo ancho («para que funcione») tiene que FALLAR aquí.
  ["naming.stream.statePattern",
    ["STATE_CLIENTES", "STATE_LOGISTICA_INVERSA"],
    ["STATE.CLIENTES", "state_clientes", "EVT_CLIENTES", "DLQ_CLIENTES", "STATE_", "STATE CLIENTES"]],
  ["state.stream.pattern",
    ["STATE_CLIENTES", "STATE_LOGISTICA_INVERSA"],
    ["STATE.CLIENTES", "state_clientes", "EVT_CLIENTES", "STATE_"]],

  ["state.subject.pattern",
    ["state.clientes.cliente.v1.acme.cli-1", "state.logistica.linea-envio.v2.acme-corp.Env_00-9",
     "state.clientes.cliente.v1.system.0", "state.clientes.cliente.v1.acme." + "a".repeat(128)],
    ["state.clientes.cliente.v1.acme",                    // sin clave: 5 tokens
     "state.clientes.cliente.v1.acme.cli.1",              // un punto en la clave: 7 tokens (el broker lo admite: A-41)
     "State.clientes.cliente.v1.acme.cli-1",              // NATS distingue mayúsculas: subject fantasma
     "state.clientes.cliente.v1.Acme.cli-1",              // tenant en mayúsculas
     "state.clientes.cliente.v1.acme_corp.cli-1",         // tenant con guion bajo
     "state.clientes.cliente.v1.acme.*", "state.clientes.cliente.v1.acme.>",   // comodines: el broker los acepta (A-41)
     "state.clientes.cliente.v1.acme._reservada",         // prefijo `_` reservado
     "state.clientes.cliente.v1.acme.-uno",
     "state.clientes.cliente.v1.acme." + "a".repeat(129), // 129 caracteres
     "state.clientes.cliente.v0.acme.k", "state.clientes.cliente.acme.cli-1",
     "pedidos.pedido.v1.creado", "dlq.state.clientes.cliente.v1", "state.clientes.cliente.v1.acme.café"]],

  ["state.contract.pattern",
    ["clientes.cliente.v1", "logistica.linea-envio.v2"],
    ["clientes.cliente", "clientes.cliente.v1.creado", "state.clientes.cliente.v1", "Clientes.cliente.v1", "clientes.cliente.v0", "clientes.cliente.1"]],

  ["state.key.pattern",
    ["cli-1", "Cli_1", "a", "0", "a".repeat(128)],
    ["", "_reservada", "-uno", "a.b", "a b", "*", ">", "café", "a".repeat(129), "a\n"]],

  ["state.tenant.pattern",
    ["acme", "acme-corp", "system", "t0"],
    ["", "Acme", "acme_corp", "acme corp", "-acme", "acme-", "acme--corp", "acme.corp", "acme\n"]],

  ["state.type.value.pattern",
    ["com.flux.state.clientes.cliente.v1", "com.flux.state.logistica.linea-envio.v2"],
    ["com.flux.state.clientes.cliente.deleted.v1", "com.flux.clientes.cliente.creado.v1", "com.flux.state.clientes.v1", "state.clientes.cliente.v1"]],
  ["state.type.tombstone.pattern",
    ["com.flux.state.clientes.cliente.deleted.v1", "com.flux.state.logistica.linea-envio.deleted.v2"],
    ["com.flux.state.clientes.cliente.v1", "com.flux.state.clientes.cliente.eliminado.v1", "com.flux.state.clientes.cliente.deleted", "com.flux.clientes.cliente.deleted.v1"]],

  // El durable de un watch: tres partes separadas por `__`, y el tenant con sus guiones a `_`.
  ["state.watch.durable.pattern",
    ["tienda-web__state_clientes_cliente_v1", "tienda-web__state_clientes_cliente_v1__acme",
     "tienda-web__state_logistica_linea_envio_v2__acme_corp"],
    ["tienda-web__state_clientes_v1",                     // falta el agregado
     "tienda-web__clientes_cliente_v1",                   // es el durable de un evento, no el de estado
     "tienda-web.state_clientes_cliente_v1", "Tienda__state_clientes_cliente_v1",
     "tienda-web__state_clientes_cliente_v1__",           // sufijo de tenant vacío
     "tienda-web__state_clientes_cliente_v1__acme-corp",  // el guion del tenant pasa a `_`
     "tienda-web__state_clientes_cliente_v1__Acme"]],

  ["state.dlq.subject.pattern",
    ["dlq.state.clientes.cliente.v1", "dlq.state.logistica.linea-envio.v2"],
    ["dlq.state.clientes.cliente.v1.acme.cli-1",           // la DLQ de estado NO lleva tenant ni clave
     "dlq.clientes.cliente.v1.creado", "dlq.state.clientes.cliente", "state.clientes.cliente.v1", "dlq.state.clientes.>"]],
  ["state.dlq.readerDurable.pattern",
    ["crm-api__dlq_state_clientes", "crm-api__dlq_state_logistica_inversa"],
    ["crm-api__dlq_clientes", "crm-api__dlq_state", "CrmApi__dlq_state_clientes", "crm-api__state_clientes_cliente_v1"]],

  ["state.metrics.flux_state_writes_total.contractLabelPattern",
    ["clientes.cliente.v1", "logistica.linea-envio.v2"],
    ["clientes.cliente.v1.acme", "clientes.cliente.v1.acme.cli-1", "acme", "state.clientes.cliente.v1"]],

  // ACL de estado — 10-state.md §8. El dueño escribe y lee lo suyo; el observador, solo su tenant.
  ["state.acl.ownerPublish",
    ["state.clientes.>", "state.linea-envio.>"],
    [">", "state.>", "state.clientes.cliente.>", "state.*.>", "clientes.>", "dlq.state.clientes.>", "state.clientes.cliente.v1.acme.cli-1"]],
  ["state.acl.ownerDirectGet",
    ["$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.>"],
    ["$JS.API.DIRECT.GET.>", "$JS.API.DIRECT.GET.STATE_CLIENTES", "$JS.API.DIRECT.GET.STATE_CLIENTES.>",
     "$JS.API.DIRECT.GET.STATE_CLIENTES.state.>", "$JS.API.DIRECT.GET.EVT_PEDIDOS.pedidos.>",
     "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.cli-1"]],
  ["state.acl.observerDirectGet",
    ["$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.*",
     "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.*.*"],
    ["$JS.API.DIRECT.GET.>", "$JS.API.DIRECT.GET.STATE_CLIENTES",                // la forma de CUERPO: ninguna ACL ve el cuerpo (A-38)
     "$JS.API.DIRECT.GET.STATE_CLIENTES.>", "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.>",
     "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.>",
     "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.*",             // sin la clave: otra forma
     "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.>",
     "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.cli-1",    // una clave concreta: no es un permiso de contrato
     "$JS.API.DIRECT.GET.EVT_PEDIDOS.pedidos.pedido.v1.*.*", "$JS.API.STREAM.MSG.GET.STATE_CLIENTES"]],
  ["state.acl.consumerInfo",
    ["$JS.API.CONSUMER.INFO.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme"],
    ["$JS.API.CONSUMER.INFO.STATE_CLIENTES.>", "$JS.API.CONSUMER.INFO.STATE_CLIENTES.*", "$JS.API.CONSUMER.INFO.STATE_CLIENTES",
     "$JS.API.CONSUMER.INFO.EVT_PEDIDOS.tienda-web__state_clientes_cliente_v1", "$JS.API.STREAM.INFO.STATE_CLIENTES"]],
  ["state.acl.consumerNext",
    ["$JS.API.CONSUMER.MSG.NEXT.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1"],
    ["$JS.API.CONSUMER.MSG.NEXT.STATE_CLIENTES.>", "$JS.API.CONSUMER.MSG.NEXT.>", "$JS.API.CONSUMER.MSG.NEXT.STATE_CLIENTES"]],
  ["state.acl.ack",
    ["$JS.ACK.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme.>"],
    ["$JS.ACK.>", "$JS.ACK.STATE_CLIENTES.>", "$JS.ACK.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme"]],
  ["state.acl.consumerCreate",
    ["$JS.API.CONSUMER.CREATE.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme.state.clientes.cliente.v1.acme.*",
     "$JS.API.CONSUMER.CREATE.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1.state.clientes.cliente.v1.*.*"],
    ["$JS.API.CONSUMER.CREATE.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme.state.clientes.cliente.v1.acme.>",
     "$JS.API.CONSUMER.CREATE.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme.state.clientes.>",
     "$JS.API.CONSUMER.CREATE.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme.state.clientes.cliente.v1.acme",
     "$JS.API.CONSUMER.CREATE.STATE_CLIENTES.>", "$JS.API.CONSUMER.DURABLE.CREATE.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme.state.clientes.cliente.v1.acme.*"]],
  ["state.acl.dlqPublish",
    ["dlq.state.clientes.cliente.v1"],
    ["dlq.state.>", "dlq.state.clientes.>", "dlq.state.clientes.cliente.v1.acme.cli-1", "dlq.clientes.cliente.v1.creado"]],
  ["state.acl.dlqReaderCreate",
    ["$JS.API.CONSUMER.CREATE.DLQ_CLIENTES.crm-api__dlq_state_clientes.dlq.state.clientes.>"],
    ["$JS.API.CONSUMER.CREATE.DLQ_CLIENTES.crm-api__dlq_state_clientes.dlq.state.>",
     "$JS.API.CONSUMER.CREATE.DLQ_CLIENTES.crm-api__dlq_state_clientes.dlq.>",
     "$JS.API.CONSUMER.CREATE.DLQ_CLIENTES.crm-api__dlq_clientes.dlq.state.clientes.>",
     "$JS.API.CONSUMER.CREATE.EVT_CLIENTES.crm-api__dlq_state_clientes.dlq.state.clientes.>"]],

  // ── Petición/respuesta — specification/11-request-reply.md ───────────────────────────────────────
  // Cinco tokens `rpc.<dominio>.<agregado>.v<major>.<operacion>`. El nombre de la operación (cuatro tokens, sin `rpc.`)
  // tiene la forma sintáctica de un subject de evento; lo que los separa es el prefijo.
  ["rpc.subject.pattern",
    ["rpc.clientes.cliente.v1.obtener", "rpc.clientes.cliente.v1.calcular-descuento", "rpc.logistica.linea-envio.v2.reservar-stock", "rpc.a.b.v1.c"],
    ["clientes.cliente.v1.obtener",                        // sin el prefijo: es el nombre de la operación, no su subject
     "rpc.clientes.cliente.v1",                            // sin operación
     "rpc.clientes.cliente.v1.obtener.x",                  // seis tokens
     "Rpc.clientes.cliente.v1.obtener", "rpc.Clientes.cliente.v1.obtener", "rpc.clientes.cliente.v1.Obtener",
     "rpc.clientes.cliente.v1.obtener_cliente", "rpc.clientes.cliente.v0.obtener", "rpc.clientes.cliente.1.obtener",
     "rpc.clientes.cliente.v1.*", "rpc.clientes.cliente.v1.>", "rpc.clientes.>", "rpc.*.cliente.v1.obtener",   // comodines: el broker los acepta
     "rpc.clientes.cliente.v1.obtener\n", "rpc.clientes.cliente.v1.",
     "state.clientes.cliente.v1.acme.cli-1", "dlq.clientes.cliente.v1.obtener", "pedidos.pedido.v1.creado"]],

  ["rpc.operation.pattern",
    ["clientes.cliente.v1.obtener", "clientes.cliente.v1.calcular-descuento", "logistica.linea-envio.v2.reservar-stock"],
    ["clientes.cliente.v1", "rpc.clientes.cliente.v1.obtener", "Clientes.cliente.v1.obtener", "clientes.cliente.v1.Obtener",
     "clientes.cliente.v1.obtener_cliente", "clientes.cliente.v1.obtener.x", "clientes.cliente.v0.obtener", "clientes.cliente.v1.",
     "clientes.cliente.v1.*", "clientes.cliente.v1.>", "clientes.cliente.v1.obtener\n", ""]],

  ["rpc.operationName.pattern",
    ["obtener", "crear", "calcular-precio", "reservar-stock", "validar-direccion", "a1"],
    ["", "Obtener", "obtener_cliente", "obtener cliente", "obtener.cliente", "-obtener", "obtener-", "obtener--cliente", "*", ">", "obtener\n"]],

  ["rpc.type.request.pattern",
    ["com.flux.rpc.clientes.cliente.calcular-descuento.request.v1", "com.flux.rpc.logistica.linea-envio.reservar-stock.request.v2"],
    ["com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1", "com.flux.rpc.error.v1", "com.flux.clientes.cliente.creado.v1",
     "com.flux.rpc.clientes.cliente.request.v1", "com.flux.rpc.clientes.cliente.calcular-descuento.request", "com.flux.state.clientes.cliente.v1"]],
  ["rpc.type.reply.pattern",
    ["com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1", "com.flux.rpc.logistica.linea-envio.reservar-stock.reply.v2"],
    ["com.flux.rpc.clientes.cliente.calcular-descuento.request.v1", "com.flux.rpc.error.v1", "com.flux.clientes.cliente.creado.v1",
     "com.flux.rpc.clientes.cliente.reply.v1", "com.flux.rpc.clientes.cliente.calcular-descuento.reply"]],
  ["rpc.type.error.pattern",
    ["com.flux.rpc.error.v1"],
    ["com.flux.rpc.error.v2", "com.flux.rpc.error", "com.flux.error.v1", "com.flux.rpc.clientes.cliente.obtener.reply.v1", "com.flux.rpc.error.v1.x"]],

  ["rpc.dataschema.request.pattern",
    ["https://schemas.internal/rpc/clientes/cliente/obtener/request/1.0.0.json", "https://schemas.internal/rpc/logistica/linea-envio/reservar-stock/request/2.3.1.json"],
    ["https://schemas.internal/rpc/clientes/cliente/obtener/reply/1.0.0.json", "https://schemas.internal/state/clientes/cliente/1.0.0.json",
     "https://schemas.internal/clientes/cliente/obtener/request/1.0.0.json", "https://schemas.internal/rpc/clientes/cliente/obtener/request/1.0.json",
     "https://schemas.internal/system/rpc/error/1.0.0.json"]],
  ["rpc.dataschema.reply.pattern",
    ["https://schemas.internal/rpc/clientes/cliente/obtener/reply/1.0.0.json"],
    ["https://schemas.internal/rpc/clientes/cliente/obtener/request/1.0.0.json", "https://schemas.internal/system/rpc/error/1.0.0.json",
     "https://schemas.internal/rpc/clientes/cliente/obtener/reply/v1.json"]],

  // Permisos de la petición/respuesta (medidos: A-66, A-68, A-71, A-73). Quien llama publica en el subject EXACTO de cada
  // operación concedida; el dueño se suscribe a su dominio. Lo ancho («para que funcione») tiene que FALLAR aquí.
  ["rpc.acl.callerPublish",
    ["rpc.clientes.cliente.v1.obtener", "rpc.logistica.linea-envio.v2.reservar-stock"],
    ["rpc.>", "rpc.clientes.>", "rpc.clientes.cliente.>", "rpc.clientes.cliente.v1.*", "rpc.clientes.cliente.v1.>", "rpc.*.*.*.*",
     "clientes.cliente.v1.obtener", "rpc.clientes.cliente.v1", "rpc.clientes.cliente.v1.obtener.x", ">", "_INBOX.llama.>"]],
  ["rpc.acl.ownerSubscribe",
    ["rpc.clientes.>", "rpc.linea-envio.>"],
    ["rpc.>", "rpc.*.>", ">", "rpc.clientes.cliente.>", "rpc.clientes.*", "rpc.clientes", "clientes.>", "state.clientes.>", "_INBOX.crm.>", "dlq.clientes.>"]],

  ["rpc.metrics.operationLabelPattern",
    ["clientes.cliente.v1.obtener", "logistica.linea-envio.v2.reservar-stock"],
    ["rpc.clientes.cliente.v1.obtener", "clientes.cliente.v1", "clientes.cliente.v1.obtener.x", "Clientes.cliente.v1.obtener"]],

  // El borde (12-edge.md): lo que recibe un receptor HTTP y el id determinista. Cada patrón, en los dos sentidos.
  ["edge.delivery.signature.pattern",
    ["t=1700000000,v1=8dbbbbf4523b10bbb793e74d854144c45acccc2d233667b1c06b805b6ded8a84"],
    ["v1=8dbbbbf4523b10bbb793e74d854144c45acccc2d233667b1c06b805b6ded8a84", "t=1700000000", "t=1700000000,v1=", "t=1700000000,v1=ABC",
     "t=1700000000,v1=8dbbbbf4523b10bbb793e74d854144c45acccc2d233667b1c06b805b6ded8a8", "t=1700000000, v1=8dbbbbf4523b10bbb793e74d854144c45acccc2d233667b1c06b805b6ded8a84",
     "t=-1,v1=8dbbbbf4523b10bbb793e74d854144c45acccc2d233667b1c06b805b6ded8a84", "sha256=8dbbbbf4523b10bbb793e74d854144c45acccc2d233667b1c06b805b6ded8a84"]],
  ["edge.delivery.response.codePattern",
    ["HTTP_503", "HTTP_429", "HTTP_400", "HTTP_302"],
    ["HTTP_99", "HTTP_1000", "HTTP_600", "http_503", "HTTP-503", "503", "HTTP_5O3"]],
  ["edge.externalId.pattern",
    ["01a01eb4-7a54-7325-ac9b-67da707ec1d4", "00000000-0000-7fdc-8906-90c15f53aa90", "ffffffff-ffff-7000-bfff-ffffffffffff"],
    ["01a01eb4-7a54-4325-ac9b-67da707ec1d4",   // versión 4
     "01a01eb4-7a54-7325-1c9b-67da707ec1d4",   // variante 0
     "01a01eb4-7a54-7325-cc9b-67da707ec1d4",   // variante 11
     "01A01EB4-7A54-7325-AC9B-67DA707EC1D4",   // mayúsculas
     "01a01eb47a54-7325-ac9b-67da707ec1d4", "01a01eb4-7a54-7325-ac9b-67da707ec1d", "01a01eb4-7a54-7325-ac9b-67da707ec1d4\n"]],
];

// ─── La ACL de ESTADO y su generador — 06-security.md §3.8 ───────────────────────────────────────
// Se escriben como función PURA sobre el documento y sobre un EMISOR de permisos (por defecto, el
// generador real) para probarlas en los dos sentidos: sobre el protocol.json y el generador reales
// deben dar cero fallos, y sobre copias estropeadas a propósito —un documento con una excepción de
// más o de menos, un generador que concede `state.>` o la forma de cuerpo de DIRECT.GET— cada una
// debe dar al menos uno. Una invariante que solo se ha visto en verde no demuestra nada.

/** Los servicios de prueba: el dueño, un observador confinado, uno sin confinar y uno mixto. */
const SERVICIOS_DE_ESTADO = [
  { name: "crm-api", owns: ["clientes"], subjects: [], stateOwns: ["clientes"], watches: [] },
  { name: "tienda-web", owns: [], subjects: [], stateOwns: [], watches: [{ contract: "clientes.cliente.v1", domain: "clientes", tenants: ["acme"] }] },
  { name: "informes-api", owns: [], subjects: [], stateOwns: [], watches: [{ contract: "clientes.cliente.v1", domain: "clientes", tenants: null }] },
  {
    name: "mixto-api",
    owns: ["pedidos"],
    subjects: ["inventario.stock.v1.ajustado"],
    stateOwns: [],
    watches: [
      { contract: "clientes.cliente.v1", domain: "clientes", tenants: ["acme-corp", "globex"] },
      { contract: "logistica.linea-envio.v2", domain: "logistica", tenants: null },
    ],
  },
];

/** @returns {string[]} */
function fallosDeAclDeEstado(doc, emitir = permisosDe) {
  const f = [];
  const eq = (a, b) => JSON.stringify(a) === JSON.stringify(b);
  const re = (src) => { try { return new RegExp(src); } catch { return /(?!)/; } };
  const acl = doc.security.acl;
  const st = doc.state.acl;
  const resolver = (ruta) => ruta.split(".").reduce((o, k) => o?.[k], doc);

  // ── Las excepciones de lo denegable: exactamente las dos formas de DIRECT.GET de estado ──────
  const rutas = acl.neverGrantedExceptions;
  if (!eq(rutas, ["state.acl.ownerDirectGet", "state.acl.observerDirectGet"])) {
    f.push(`security.acl.neverGrantedExceptions debe ser exactamente [state.acl.ownerDirectGet, state.acl.observerDirectGet]; es ${JSON.stringify(rutas)}`);
  }
  const excepciones = [];
  for (const ruta of rutas ?? []) {
    const v = resolver(ruta);
    if (typeof v !== "string") f.push(`security.acl.neverGrantedExceptions: «${ruta}» no existe o no es un patrón`);
    else excepciones.push(re(v));
  }
  const caza = re(acl.neverGrantedPattern);
  const denegable = (x) => caza.test(x) && !excepciones.some((r) => r.test(x));

  // Lo que sigue denegado AUNQUE haya excepciones: la forma de cuerpo, los comodines por encima del
  // tenant, una clave suelta, el stream de otro dominio, y el resto de lo que ya lo era.
  for (const x of [
    "$JS.API.DIRECT.GET.>", "$JS.API.DIRECT.GET.EVT_PEDIDOS", "$JS.API.DIRECT.GET.EVT_PEDIDOS.pedidos.pedido.v1.*.*",
    "$JS.API.DIRECT.GET.STATE_CLIENTES", "$JS.API.DIRECT.GET.STATE_CLIENTES.>", "$JS.API.DIRECT.GET.STATE_CLIENTES.state.>",
    "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.>", "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.*",
    "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.>", "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.cli-1",
    "$JS.API.STREAM.MSG.GET.STATE_CLIENTES", ">", "$JS.API.>",
  ]) {
    if (!denegable(x)) f.push(`«${x}» debe seguir siendo denegable (las excepciones de neverGrantedExceptions son solo las dos formas por subject de un estado)`);
  }
  // Las dos formas concedibles: las caza neverGrantedPattern (si no, sobraría la excepción) y NO son denegables.
  for (const x of [
    "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.>",
    "$JS.API.DIRECT.GET.STATE_LINEA_ENVIO.state.linea-envio.>",
    "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.*",
    "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.*.*",
  ]) {
    if (!caza.test(x)) f.push(`«${x}» ni siquiera lo caza neverGrantedPattern: la excepción sobra o el patrón se ha estropeado`);
    if (denegable(x)) f.push(`«${x}» es una forma de estado concedible y sale como denegable: falta su excepción`);
  }
  // STREAM.INFO.STATE_<DOM> no lo concede ningún patrón de eventos ni de estado (A-39): lista claves y tenants.
  const patronesEventos = Object.entries(acl.permissionPatterns).filter(([k]) => !k.startsWith("$")).map(([k, v]) => [`security.acl.permissionPatterns.${k}`, re(v)]);
  const CLAVES_ESTADO = ["ownerPublish", "ownerDirectGet", "observerDirectGet", "consumerInfo", "consumerNext", "ack", "consumerCreate", "dlqPublish", "dlqReaderCreate"];
  const patronesEstado = CLAVES_ESTADO.map((k) => [`state.acl.${k}`, re(st[k] ?? "(?!)")]);
  for (const [k, r] of [...patronesEventos, ...patronesEstado]) {
    if (r.test("$JS.API.STREAM.INFO.STATE_CLIENTES")) f.push(`${k} concede STREAM.INFO.STATE_CLIENTES: con subjects_filter lista todas las claves y todos los tenants (A-39)`);
    if (r.test("$JS.API.DIRECT.GET.STATE_CLIENTES") || r.test("$JS.API.DIRECT.GET.>")) f.push(`${k} concede la forma de CUERPO de DIRECT.GET: lee cualquier mensaje y ninguna ACL ve el cuerpo (A-38)`);
    // `ownDomain` casa con `state.>` (es un patrón de dominio): lo impide el generador, que rechaza los
    // dominios reservados (comprobado abajo), no el patrón.
    if (k !== "state.acl.ownerPublish" && k !== "security.acl.permissionPatterns.ownDomain" && r.test("state.>")) f.push(`${k} concede state.>: escribir el estado de todos los dominios`);
  }
  // ownerPermissions: el durable de la DLQ de estado del dueño vive en DLQ_<DOM>, no en STATE_<DOM>. Sus permisos NO son
  // los `consumerInfo`/`consumerNext`/`ack` de state.acl (de STATE_ y del observador) sino los de
  // security.acl.permissionPatterns, que ya admiten `<servicio>__dlq_state_<dom>`. Medido con regex, no afirmado.
  {
    const dur = "crm-api__dlq_state_clientes";
    const muestras = { consumerInfo: `$JS.API.CONSUMER.INFO.DLQ_CLIENTES.${dur}`, consumerNext: `$JS.API.CONSUMER.MSG.NEXT.DLQ_CLIENTES.${dur}`, ack: `$JS.ACK.DLQ_CLIENTES.${dur}.>` };
    for (const [k, x] of Object.entries(muestras)) {
      if (!re(acl.permissionPatterns?.[k] ?? "(?!)").test(x)) f.push(`security.acl.permissionPatterns.${k} debe admitir el durable de la DLQ de estado del dueño (${x})`);
      if (re(st[k] ?? "(?!)").test(x)) f.push(`state.acl.${k} admite ${x}: es de STATE_<DOM>, y el durable de la DLQ del dueño va en DLQ_<DOM> (sus permisos son los de security.acl.permissionPatterns)`);
    }
    const op = st.ownerPermissions ?? [];
    for (const k of ["consumerInfo", "consumerNext", "ack"]) {
      if (op.some((e) => e.split(" ")[0] === k)) f.push(`state.acl.ownerPermissions cita «${k}» a secas: eso es el patrón de STATE_<DOM> (state.acl.${k}); los del durable de la DLQ del dueño son security.acl.permissionPatterns.${k}`);
      if (!op.includes(`security.acl.permissionPatterns.${k}`)) f.push(`state.acl.ownerPermissions debe citar security.acl.permissionPatterns.${k} (el durable de la DLQ de estado del dueño)`);
    }
    for (const e of [...op, ...(st.observerPermissions ?? [])]) {
      const ruta = e.split(" ")[0];
      const existe = ruta.startsWith("security.") ? typeof resolver(ruta) === "string" : typeof st[ruta] === "string";
      if (!existe) f.push(`state.acl: «${e}» no se resuelve a ningún patrón (state.acl.<clave> o security.acl.permissionPatterns.<clave>)`);
    }
    if (op.some((e) => /STREAM\.INFO|streamInfo/.test(e))) f.push("state.acl.ownerPermissions incluye STREAM.INFO: el dueño NO recibe STREAM.INFO.STATE_<DOM> (10-state.md §8)");
    if (/PUEDE tenerlo/.test(st.$commentStreamInfo ?? "")) f.push("state.acl.$commentStreamInfo dice que el dueño PUEDE tener STREAM.INFO.STATE_<DOM>: no lo recibe");
  }
  // El dueño escribe en SU dominio de estado, no en todos: `state.clientes.>` sí, `state.>` y `state.*.>` no.
  const owp = re(st.ownerPublish ?? "(?!)");
  if (!owp.test("state.clientes.>") || ["state.>", "state.*.>", ">", "state.clientes.cliente.>"].some((x) => owp.test(x))) f.push("state.acl.ownerPublish debe aceptar state.<dominio>.> y rechazar state.>, state.*.> y lo más estrecho que un dominio");
  if (re(acl.permissionPatterns.ownDomain).test("state.clientes.>")) f.push("security.acl.permissionPatterns.ownDomain concede state.clientes.>: los de estado van en state.acl");
  // …y ese «dueño» de un dominio reservado no puede existir: el generador se niega a emitir sus permisos.
  for (const d of doc.naming.reservedDomains.names) {
    let lanzo = false;
    try {
      emitir({ name: "x-api", owns: [d], subjects: [], stateOwns: [], watches: [] });
    } catch {
      lanzo = true;
    }
    if (!lanzo) f.push(`el generador emite permisos para el dueño del dominio RESERVADO «${d}» (recibiría ${d}.>): ${d === "state" ? "escribiría el estado de todos los dominios" : "publicaría en el espacio de nombres del protocolo"}`);
  }

  // ── El generador: cada permiso que emite, y la forma exacta de cada rol ───────────────────────
  const devSolo = new Set([...(acl.devOnlyPatterns ?? []).map((k) => `security.acl.permissionPatterns.${k}`), ...(st.devOnlyPatterns ?? []).map((k) => `state.acl.${k}`)]);
  const categoriasDe = (x) => [...patronesEventos, ...patronesEstado].filter(([, r]) => r.test(x)).map(([k]) => k);
  const durableEstado = re(doc.state.watch.durable.pattern);
  const durableDlqEstado = re(doc.state.dlq.readerDurable.pattern);
  for (const dev of [false, true]) {
    const modo = dev ? "--dev" : "por defecto";
    for (const svc of SERVICIOS_DE_ESTADO) {
      const e = dev ? emitir(svc, { dev }) : emitir(svc);
      const donde = `${svc.name} (${modo})`;
      for (const x of e.publish) {
        const cats = categoriasDe(x);
        if (cats.length !== 1) f.push(`${donde} emite «${x}», que encaja con ${cats.length} categoría(s): [${cats.join(", ")}]`);
        if (denegable(x)) f.push(`${donde} emite «${x}», que neverGrantedPattern declara denegable (y ninguna excepción lo cubre)`);
        if (!dev && cats.some((k) => devSolo.has(k))) f.push(`${donde} emite «${x}», que solo debe concederse con --dev`);
      }
      const creates = e.publish.filter((x) => categoriasDe(x).some((k) => devSolo.has(k)));
      if (creates.length !== (dev ? e.durables.length : 0)) f.push(`${donde} emite ${creates.length} CONSUMER.CREATE y debería emitir ${dev ? e.durables.length : 0}`);
      if (e.publish.some((x) => /^\$JS\.API\.STREAM\.INFO\.STATE_/.test(x))) f.push(`${donde} emite STREAM.INFO.STATE_<DOM>: lista todas las claves y todos los tenants (A-39)`);
      if (!eq(e.subscribe, [`_INBOX.${svc.name}.>`])) f.push(`${donde} no se suscribe SOLO a su inbox: ${JSON.stringify(e.subscribe)}`);

      // Escribir estado: solo el dueño, y solo en los dominios con estado que posee.
      const escribe = e.publish.filter((x) => /^state\./.test(x));
      if (!eq(escribe, svc.stateOwns.map((d) => `state.${d}.>`))) f.push(`${donde} concede para escribir estado ${JSON.stringify(escribe)}; debería ser ${JSON.stringify(svc.stateOwns.map((d) => `state.${d}.>`))}`);

      // Leer estado por DIRECT.GET: el dueño, su dominio; el observador, cada contrato con el tenant FIJADO.
      const esperadosGet = [
        ...svc.stateOwns.map((d) => `$JS.API.DIRECT.GET.STATE_${d.replace(/-/g, "_").toUpperCase()}.state.${d}.>`),
        ...svc.watches.flatMap((w) => (w.tenants ?? ["*"]).map((t) => `$JS.API.DIRECT.GET.STATE_${w.domain.replace(/-/g, "_").toUpperCase()}.state.${w.contract}.${t}.*`)),
      ].sort();
      const get = e.publish.filter((x) => x.includes("DIRECT.GET")).sort();
      if (!eq(get, esperadosGet)) f.push(`${donde} concede DIRECT.GET ${JSON.stringify(get)}; debería ser ${JSON.stringify(esperadosGet)}`);

      // Los durables de estado: uno por (contrato, tenant), con el nombre y el filtro de 10-state.md.
      const deEstado = e.durables.filter((d) => d.kind === "state");
      const esperadosDur = svc.watches.flatMap((w) => (w.tenants ?? [null]).map((t) => ({ contract: w.contract, tenant: t, filter: `state.${w.contract}.${t ?? "*"}.*` })));
      if (deEstado.length !== esperadosDur.length) f.push(`${donde} declara ${deEstado.length} durable(s) de estado y debería declarar ${esperadosDur.length}`);
      for (const d of deEstado) {
        if (!durableEstado.test(d.durable)) f.push(`${donde}: el durable de estado «${d.durable}» no cumple state.watch.durable.pattern`);
        if (d.stream !== `STATE_${d.contract.split(".")[0].replace(/-/g, "_").toUpperCase()}`) f.push(`${donde}: el durable «${d.durable}» va sobre ${d.stream}`);
        if (!esperadosDur.some((x) => x.contract === d.contract && x.tenant === d.tenant && x.filter === d.filter)) f.push(`${donde}: el durable «${d.durable}» tiene un filtro (${d.filter}) que no es el del contrato y su tenant`);
        // El nombre, derivado AQUÍ y no con manifest.mjs: un durable compartido reparte los mensajes
        // entre quienes lo usan (A-47), así que el tenant tiene que ir en el nombre y solo si confina.
        const nombre = `${svc.name}__state_${d.contract.replace(/[.-]/g, "_")}${d.tenant ? `__${d.tenant.replace(/-/g, "_")}` : ""}`;
        if (d.durable !== nombre) f.push(`${donde}: el durable «${d.durable}» debería llamarse «${nombre}» (el tenant va en el nombre si confina, y solo entonces: A-47)`);
      }
      for (const d of e.durables.filter((x) => x.kind === "dlq-state")) {
        if (!durableDlqEstado.test(d.durable)) f.push(`${donde}: el durable de DLQ de estado «${d.durable}» no cumple state.dlq.readerDurable.pattern`);
        for (const [k, x] of [["consumerInfo", `$JS.API.CONSUMER.INFO.${d.stream}.${d.durable}`], ["consumerNext", `$JS.API.CONSUMER.MSG.NEXT.${d.stream}.${d.durable}`], ["ack", `$JS.ACK.${d.stream}.${d.durable}.>`]]) {
          if (!e.publish.includes(x)) f.push(`${donde}: falta el permiso ${x} del durable de su DLQ de estado`);
          const cats = categoriasDe(x);
          if (!eq(cats, [`security.acl.permissionPatterns.${k}`])) f.push(`${donde}: «${x}» (durable de la DLQ del dueño) debe casar solo con security.acl.permissionPatterns.${k}; casa con [${cats.join(", ")}]`);
        }
        if (!svc.stateOwns.includes(d.stream.replace(/^DLQ_/, "").toLowerCase().replace(/_/g, "-"))) f.push(`${donde}: lee la DLQ de estado de un dominio que no es suyo (${d.stream})`);
      }
      if (e.durables.filter((x) => x.kind === "dlq-state").length !== svc.stateOwns.length) f.push(`${donde}: debe leer la DLQ de estado de cada dominio con estado que posee`);

      // La DLQ de estado: un `dlq.state.<contrato>` por contrato observado (no por tenant) y la DLQ del dominio.
      const dlq = e.publish.filter((x) => /^dlq\.state\./.test(x)).sort();
      if (!eq(dlq, svc.watches.map((w) => `dlq.state.${w.contract}`).sort())) f.push(`${donde} concede publicar en ${JSON.stringify(dlq)}; debería ser un dlq.state.<contrato> por contrato observado`);
      for (const w of svc.watches) {
        if (!e.publish.includes(`$JS.API.STREAM.INFO.DLQ_${w.domain.replace(/-/g, "_").toUpperCase()}`)) f.push(`${donde}: falta STREAM.INFO.DLQ_<DOM> de ${w.domain}: el SDK comprueba la DLQ al abrir`);
      }
    }
  }
  return f;
}

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

// ─── Estado replicado — specification/10-state.md ───────────────────────────────────────────
// Como `fallosDeConnect`: una función PURA sobre el documento, que se ejecuta sobre el protocol.json
// real (cero fallos) y sobre copias estropeadas a propósito (cada una DEBE dar al menos uno). Una
// invariante que solo se ha visto en verde no demuestra nada.
const CODIGOS_ESTADO = {
  INVALID_STATE_CONTRACT: "permanent",
  INVALID_TENANT_FOR_STATE: "permanent",
  INVALID_STATE_KEY: "permanent",
  MISSING_AGGREGATE_VERSION: "permanent",
  STATE_SUBJECT_MISMATCH: "poison",
  STATE_CONFLICT: "retryable",
  RESERVED_DOMAIN: "permanent",
};
const DOMINIOS_RESERVADOS = ["dlq", "state", "rpc", "system"];

/** @returns {string[]} */
function fallosDeEstado(doc) {
  const f = [];
  const st = doc.state;
  if (!st) return ["falta el bloque `state` (10-state.md)"];
  const cs = doc.errors?.codes ?? {};
  const eq = (a, b) => JSON.stringify(a) === JSON.stringify(b);
  const re = (src) => { try { return new RegExp(src); } catch { return /(?!)/; } };

  if (doc.version !== "1.1.0") f.push(`version debe ser "1.1.0" (la fase 2 la sube), es ${JSON.stringify(doc.version)}`);

  // 04-errors.md §6: la lista de códigos es CERRADA, con su clase, y todos cuelgan de la capacidad `state`.
  for (const [c, clase] of Object.entries(CODIGOS_ESTADO)) {
    if (!cs[c]) { f.push(`errors.codes.${c}: falta (04-errors.md §6)`); continue; }
    if (cs[c].class !== clase) f.push(`errors.codes.${c}.class debe ser "${clase}", es ${JSON.stringify(cs[c].class)}`);
    if (cs[c].capability !== "state") f.push(`errors.codes.${c}.capability debe ser "state"`);
  }
  const deEstado = Object.entries(cs).filter(([k, v]) => !k.startsWith("$") && v?.capability === "state").map(([k]) => k).sort();
  if (!eq(deEstado, Object.keys(CODIGOS_ESTADO).sort())) f.push(`los códigos de la capacidad state son una lista CERRADA de siete; hay [${deEstado.join(", ")}]`);
  // El único POISON de estado es el de identidad: miente sobre su tenant o su clave.
  if (cs.STATE_SUBJECT_MISMATCH?.action !== "term+dlq+alert") f.push('errors.codes.STATE_SUBJECT_MISMATCH.action debe ser "term+dlq+alert" (es POISON)');
  if (cs.STATE_SUBJECT_MISMATCH?.neverApplied !== true) f.push("errors.codes.STATE_SUBJECT_MISMATCH.neverApplied debe ser true: la entrada no se aplica");
  // El séptimo: un contrato mal formado. Falla antes de la red y NO se cuenta (su valor es la etiqueta `contract`).
  if (cs.INVALID_STATE_CONTRACT?.failsBeforeNetwork !== true || cs.INVALID_STATE_CONTRACT?.notCountedInMetrics !== true) f.push("errors.codes.INVALID_STATE_CONTRACT: falla antes de la red y no se cuenta en flux_state_writes_total (un contrato mal formado crearía una serie por cada error de tipeo)");
  if (!eq(cs.INVALID_STATE_CONTRACT?.thrownBy, ["put", "get", "delete", "watch"])) f.push("errors.codes.INVALID_STATE_CONTRACT.thrownBy debe ser [put, get, delete, watch]");
  if (st.contract?.errorCode !== "INVALID_STATE_CONTRACT") f.push('state.contract.errorCode debe ser "INVALID_STATE_CONTRACT"');
  if (!(st.metrics?.flux_state_writes_total?.notCounted ?? []).includes("INVALID_STATE_CONTRACT")) f.push("state.metrics.flux_state_writes_total.notCounted debe incluir INVALID_STATE_CONTRACT");
  if (!doc.capabilities?.definitions?.state) f.push("capabilities.definitions.state: falta la definición de la capacidad (con su evidencia)");
  else if (doc.capabilities.definitions.state.evidence !== "sdk-files") f.push('capabilities.definitions.state.evidence debe ser "sdk-files", como `security`');
  else if (!doc.capabilities.definitions.state.requires?.includes("seven-state-error-codes") || doc.capabilities.definitions.state.requires?.includes("six-state-error-codes")) f.push("capabilities.definitions.state.requires debe pedir seven-state-error-codes (la lista de códigos de estado es de siete)");

  // 02-naming.md §1.3: dominios reservados.
  const rd = doc.naming?.reservedDomains;
  if (!eq([...(rd?.names ?? [])].sort(), [...DOMINIOS_RESERVADOS].sort())) f.push(`naming.reservedDomains.names debe ser ${JSON.stringify(DOMINIOS_RESERVADOS)}`);
  if (rd?.errorCode !== "RESERVED_DOMAIN" || !cs.RESERVED_DOMAIN) f.push("naming.reservedDomains.errorCode debe ser RESERVED_DOMAIN, que debe existir en errors.codes");
  // …y la reserva NO la hace el patrón del subject: la hace RESERVED_DOMAIN. Si un día el patrón los
  // rechazara, un SDK dejaría de dar el código estable y daría «subject inválido».
  const subj = re(doc.naming?.subject?.pattern ?? "");
  for (const dom of DOMINIOS_RESERVADOS) {
    if (!subj.test(`${dom}.agregado.v1.creado`)) f.push(`naming.subject.pattern debería aceptar sintácticamente "${dom}.agregado.v1.creado": la reserva la hace RESERVED_DOMAIN, no el patrón`);
  }

  // 10-state.md §2.1: la configuración del stream. Mal puestas, estas cuatro NO dan error y rompen la semántica.
  const sm = st.stream ?? {};
  if (sm.maxMsgsPerSubject !== 1) f.push("state.stream.maxMsgsPerSubject debe ser 1: es lo que convierte el stream en un mapa (A-31)");
  if (sm.allowDirect !== true) f.push("state.stream.allowDirect debe ser true: sin él get y el paso 1 de put no reciben respuesta (A-32)");
  if (sm.maxAge !== 0) f.push("state.stream.maxAge debe ser 0: el estado no caduca");
  if (sm.discard !== "old") f.push('state.stream.discard debe ser "old": con "new" el broker rechazaría la segunda escritura (A-46)');
  if (sm.sdkQueriesStreamInfo !== false) f.push("state.stream.sdkQueriesStreamInfo debe ser false: STREAM.INFO lista claves y tenants (A-39)");
  if (!eq([...(sm.auditedByDoctor ?? [])].sort(), ["allowDirect", "discard", "maxAge", "maxMsgsPerSubject"])) f.push("state.stream.auditedByDoctor debe listar las cuatro: allowDirect, discard, maxAge, maxMsgsPerSubject");

  // El subject: 6 tokens, y su formato los cuenta.
  const fmtSubject = st.subject?.format ?? "";
  if (st.subject?.tokens !== 6 || fmtSubject.split(".").length !== 6) f.push(`state.subject: debe tener 6 tokens (tokens=${st.subject?.tokens}, formato con ${fmtSubject.split(".").length})`);
  const fmtDlq = st.dlq?.subject?.format ?? "";
  if (st.dlq?.subject?.tokens !== 5 || fmtDlq.split(".").length !== 5) f.push(`state.dlq.subject: debe tener 5 tokens, sin tenant ni clave (tokens=${st.dlq?.subject?.tokens}, formato con ${fmtDlq.split(".").length})`);
  // Los ejemplos del propio documento pasan y fallan donde dicen.
  for (const [ruta, nodo] of [["state.subject", st.subject], ["state.contract", st.contract], ["state.key", st.key], ["state.tenant", st.tenant],
                              ["state.watch.durable", st.watch?.durable], ["state.dlq.subject", st.dlq?.subject]]) {
    const r = re(nodo?.pattern ?? "");
    for (const v of nodo?.valid ?? []) if (!r.test(v)) f.push(`${ruta}.valid: el patrón rechaza ${JSON.stringify(v)}`);
    for (const v of nodo?.invalid ?? []) if (r.test(v)) f.push(`${ruta}.invalid: el patrón acepta ${JSON.stringify(v)}`);
  }
  // La clave: el prefijo reservado lo hace cumplir el patrón, y su longitud sale de él.
  if (st.key?.reservedPrefix !== "_" || re(st.key?.pattern ?? "").test("_x")) f.push("state.key: el prefijo `_` está reservado al protocolo y el patrón no debe admitir una clave que empiece por él");
  if (!(st.key?.pattern ?? "").includes(`{0,${(st.key?.maxLength ?? 0) - 1}}`)) f.push(`state.key.maxLength (${st.key?.maxLength}) no sale del patrón`);
  if (st.key?.maxLength !== 128) f.push("state.key.maxLength debe ser 128");
  if (st.key?.caseSensitive !== true || st.key?.pii !== "forbidden") f.push("state.key: es sensible a mayúsculas y nunca PII");
  if (st.tenant?.maxLength !== 64) f.push("state.tenant.maxLength debe ser 64: el durable lleva el tenant y NATS limita los nombres de consumidor a 255 (A-42)");
  if (st.tenant?.default !== "none") f.push('state.tenant.default debe ser "none": en estado no hay tenant por defecto');
  // 10-state.md §2.4 y §5.2: `put`/`delete`/`get` heredan el tenant del evento en curso; `watch`, NUNCA (no corre dentro de un handler).
  if (!eq(st.tenant?.resolutionOrder, ["call-option", "event-in-progress", "connection-tenantId"]) || !eq(st.tenant?.resolutionOrderAppliesTo, ["put", "delete", "get"])) f.push("state.tenant.resolutionOrder (put, delete, get) debe ser [call-option, event-in-progress, connection-tenantId]");
  if (!eq(st.tenant?.watchResolutionOrder, ["call-option", "connection-tenantId"]) || st.tenant?.watchNeverUsesEventInProgress !== true) f.push("state.tenant.watchResolutionOrder debe ser [call-option, connection-tenantId], sin el evento en curso: un watch no corre dentro de un handler y heredar su tenant haría depender el filtro de qué evento llegó primero");
  // El durable de un watch no puede confundirse con el de un evento ni con el de DLQ.
  const durEvento = re(doc.naming?.durableConsumer?.pattern ?? "");
  const durEstado = re(st.watch?.durable?.pattern ?? "");
  if (durEstado.test("facturacion-api__pedidos_pedido_v1_creado")) f.push("state.watch.durable.pattern acepta el durable de un evento");
  // …pero SÍ es un durable de NATS (sin puntos, `<servicio>__<resto>`): el patrón general lo acepta.
  for (const v of st.watch?.durable?.valid ?? []) {
    if (!durEvento.test(v)) f.push(`naming.durableConsumer.pattern rechaza el durable de estado ${JSON.stringify(v)}: un durable de estado también es un durable`);
  }

  // El sobre: la identidad está en el subject; el sobre solo la repite.
  const env = st.envelope ?? {};
  if (env.tenantidEqualsSubjectToken !== true || env.subjectEqualsKeyToken !== true || env.typeMatchesContract !== true) f.push("state.envelope: tenantid, subject y type DEBEN coincidir con el subject de NATS");
  if (env.mismatchCode !== "STATE_SUBJECT_MISMATCH" || env.mismatchClass !== "poison") f.push("state.envelope: la discrepancia es POISON con STATE_SUBJECT_MISMATCH");
  if (!eq(env.newRootAttributes, [])) f.push("state.envelope.newRootAttributes debe ser []: el estado no añade atributos al sobre (01-envelope.md §3.4)");
  if (env.attributeOrder !== doc.cloudevents?.attributeOrder) f.push("state.envelope.attributeOrder debe ser el de cloudevents.attributeOrder: data va siempre el último");

  // Clasificación: restricted no se admite en estado; lo admitido es un subconjunto de lo que existe.
  const cl = st.classification ?? {};
  const enumCe = doc.cloudevents?.requiredExtensions?.dataclassification?.enum ?? [];
  if (!cl.forbidden?.includes("restricted") || cl.allowed?.includes("restricted")) f.push("state.classification: `restricted` NO se admite en estado");
  if (!(cl.allowed ?? []).every((c) => enumCe.includes(c))) f.push("state.classification.allowed contiene un valor que no existe en cloudevents.requiredExtensions.dataclassification");
  if (cl.putDefault !== "confidential" || !cl.allowed?.includes(cl.putDefault)) f.push("state.classification.putDefault debe ser confidential (equivocarse hacia arriba cuesta un log redactado)");
  if (cl.tombstone !== "internal" || st.tombstone?.dataclassification !== "internal") f.push("la lápida es siempre internal");
  if (doc.security?.piiInPayload?.event !== "forbidden" || !doc.security?.piiInPayload?.state) f.push("security.piiInPayload debe distinguir por primitiva: event=forbidden y state");

  // La versión del agregado: entero >= 1, comprobado siempre, y con el máximo que Node representa.
  const av = st.aggregateVersion ?? {};
  if (av.required !== true || av.type !== "integer" || av.minimum !== 1) f.push("state.aggregateVersion debe ser un entero >= 1 y obligatorio");
  if (av.maximum !== Number.MAX_SAFE_INTEGER) f.push("state.aggregateVersion.maximum debe ser 2^53 - 1 (9007199254740991): el mayor entero que Node representa sin perder precisión");
  if (av.checkedWithoutValidation !== true) f.push("state.aggregateVersion.checkedWithoutValidation debe ser true: es una regla del protocolo, no del esquema");
  if (st.schemaPath?.mustDeclare?.aggregateVersion?.required !== true) f.push("state.schemaPath.mustDeclare.aggregateVersion debe ser required");
  // `7.0`: se rechaza donde el parser distingue entero de flotante; en Node es indistinguible de `7` (límite del lenguaje, no un fallo).
  const sp = av.sevenPointZero ?? {};
  if (sp.rejectedWhenParserDistinguishesIntFromFloat !== true || !eq(sp.singleNumberLanguages, ["node"]) || sp.inStateSemanticsJson !== false) f.push("state.aggregateVersion.sevenPointZero: 7.0 se rechaza donde el parser distingue entero de flotante; Node es un lenguaje de número único (indistinguible de 7) y el caso no va en state-semantics.json");
  if (av.decimalIsInvalid !== true) f.push("state.aggregateVersion.decimalIsInvalid debe ser true");

  // La lápida: fija, con versión, permanente.
  const tb = st.tombstone ?? {};
  if (tb.dataschema !== "https://schemas.internal/system/state/eliminado/1.0.0.json") f.push("state.tombstone.dataschema debe ser https://schemas.internal/system/state/eliminado/1.0.0.json");
  if (tb.dataschema?.replace("https://schemas.internal/", "schemas/") !== tb.schemaPath) f.push("state.tombstone.schemaPath debe ser la ruta del $id de su dataschema");
  if (!eq(tb.data, ["aggregateVersion"]) || tb.carriesVersion !== true) f.push("state.tombstone.data debe ser [aggregateVersion]: la lápida lleva versión porque put la lee");
  if (tb.permanent !== true || tb.replacesPreviousValue !== true) f.push("state.tombstone: permanente en 1.1, y sustituye al valor anterior");
  if (tb.recreateRequires !== "aggregateVersion-strictly-greater") f.push("state.tombstone.recreateRequires: recrear una clave borrada exige una versión estrictamente mayor");
  if (!re(st.type?.tombstone?.pattern ?? "").test("com.flux.state.clientes.cliente.deleted.v1") || re(st.type?.value?.pattern ?? "").test("com.flux.state.clientes.cliente.deleted.v1")) f.push("state.type: el patrón de la lápida y el del valor no se distinguen");

  // La escritura condicional: el corazón.
  const w = st.write ?? {};
  if (w.conditional !== true || w.expectedHeader !== "Nats-Expected-Last-Subject-Sequence" || w.msgIdHeader !== "Nats-Msg-Id") f.push("state.write: condicional, con Nats-Expected-Last-Subject-Sequence y Nats-Msg-Id");
  if (w.expectedForNewKey !== 0) f.push("state.write.expectedForNewKey debe ser 0 (A-33)");
  if (w.maxAttempts !== 3) f.push("state.write.maxAttempts debe ser 3");
  // Un SDK que solo reconoce el 10071 pasa contra un nodo y falla en un cluster (A-49).
  if (!eq([...(w.conflictErrCodes ?? [])].sort((a, b) => a - b), [10071, 10164])) f.push("state.write.conflictErrCodes debe ser [10071, 10164]: el 10164 es el conflicto de una carrera en un cluster y sin él un SDK no reintenta (A-49)");
  if (w.sameIdAndBytesAcrossAttempts !== true || w.recognizeOwnWriteById !== true) f.push("state.write: el mismo id y los mismos bytes en todos los intentos, y reconocer la propia escritura por el id (un acuse perdido no es un stale)");
  if (w.publishViaCoreForbidden !== true) f.push("state.write.publishViaCoreForbidden debe ser true: un publish de core sobrescribe sin condición (A-45)");
  if (!eq(w.outcomes, ["ok", "stale"]) || !eq(w.result, ["applied", "outcome", "revision"])) f.push('state.write: outcomes ["ok","stale"] y resultado ["applied","outcome","revision"]');
  if (w.conflictCode !== "STATE_CONFLICT" || !cs.STATE_CONFLICT) f.push("state.write.conflictCode debe ser STATE_CONFLICT, que debe existir en errors.codes");
  if (w.unreadableCurrentValue !== "overwrite") f.push("state.write.unreadableCurrentValue debe ser overwrite: un valor ilegible no puede bloquear la clave para siempre");

  // Lectura y watch.
  const rd2 = st.read ?? {};
  if (rd2.bodyFormAllowedToServices !== false) f.push("state.read.bodyFormAllowedToServices debe ser false: la forma de cuerpo de DIRECT.GET lee cualquier mensaje y ninguna ACL la ve (A-38)");
  const wt = st.watch ?? {};
  if (wt.deliverPolicy !== "last_per_subject") f.push('state.watch.deliverPolicy debe ser "last_per_subject"');
  if (!eq(wt.entry, ["key", "tenantId", "deleted", "data", "aggregateVersion", "revision"])) f.push("state.watch.entry debe ser [key, tenantId, deleted, data, aggregateVersion, revision]");
  if (wt.replacedMessagesAreNotRedelivered !== true || wt.usesStreamInfoOnState !== false) f.push("state.watch: un valor reemplazado no se reentrega, y no usa STREAM.INFO.STATE_<DOM>");
  if (wt.onSynced?.emptyStream !== "immediately-before-any-entry" || wt.onSynced?.invocations !== "once-per-watch") f.push("state.watch.onSynced: con el stream vacío se invoca de inmediato, una vez por watch");
  // Un watch VIVO durante un resync: reabre el consumo (nats.js tarda 43 s en repetir el pull; async-nats da el flujo por terminado y un bucle que lo
  // trata como «parar» sale en silencio). Sin CONSUMER.CREATE y comprobando la configuración: un durable recreado con otro filtro entrega otro tenant.
  const dd = wt.durableDeletedWhileWatching ?? {};
  if (dd.action !== "reopen-the-consumption" || !(dd.mustNot ?? []).includes("stop-silently") || !(dd.mustNot ?? []).includes("treat-end-of-stream-as-stop-request")) f.push("state.watch.durableDeletedWhileWatching: reabrir el consumo; nunca parar en silencio ni tratar el fin del flujo como una petición de parar");
  if (dd.needsConsumerCreate !== false || !eq(dd.reusesPermissions, ["consumerInfo", "consumerNext"])) f.push("state.watch.durableDeletedWhileWatching: reabrir NO necesita CONSUMER.CREATE (usa CONSUMER.INFO y MSG.NEXT del observador)");
  if (dd.checksConfigBeforeConsuming !== true || dd.afterResyncOnSyncedFiresAgain !== false || dd.stoppedByClose !== true || dd.warnsThroughLogger !== true) f.push("state.watch.durableDeletedWhileWatching: comprueba la configuración antes de consumir, avisa por el logger, lo detiene close(), y onSynced no se invoca de nuevo");
  if (!dd.measured?.withoutHandling?.["nats.js 3.4.0"] || !dd.measured?.withoutHandling?.["async-nats 0.50"] || !dd.measured?.reopening) f.push("state.watch.durableDeletedWhileWatching.measured: lo medido (sin tratarlo y reabriendo) en nats.js y async-nats");
  if (wt.onSynced?.severalTenantsInOneCall !== "once-when-all-synced" || wt.onSynced?.apiTakesOneTenantPerWatch !== true || wt.onSynced?.afterResyncFiresAgain !== false) f.push("state.watch.onSynced: con varios tenants en una llamada, una vez cuando todos están sincronizados; la API toma un tenant por watch; un resync no la invoca de nuevo");

  // La DLQ de estado: sin dato, sin firma, sin replay.
  const dq = st.dlq ?? {};
  if (!eq(dq.dataReplacedBy, ["key", "revision"]) || dq.dataNeverCopied !== true) f.push("state.dlq.dataReplacedBy debe ser [key, revision]: un dato personal borrado no puede seguir 90 días en la DLQ");
  if (dq.replay !== "forbidden" || dq.recovery !== "flux state resync") f.push("state.dlq: el estado no se reproduce desde la DLQ; la recuperación es flux state resync");
  if (dq.dropsSignature !== true || dq.dlqerrorNeverContainsDataValues !== true || dq.unreadableMessageBodyNeverCopied !== true) f.push("state.dlq: sin firma, sin valores en dlqerror y sin el cuerpo de un mensaje ilegible");
  if (!eq(dq.streamSubjectsWhenDomainHasNoState, ["dlq.<dominio>.>"])) f.push("state.dlq.streamSubjectsWhenDomainHasNoState: un dominio sin estado conserva el DLQ_<DOM> de la 1.0 (1.1 es aditiva)");
  // Un mensaje ilegible sintetiza su sobre: time = ahora y partitionkey = la clave (lo único fiable es el subject).
  const sy = dq.unreadableSynthesizedEnvelope ?? {};
  if (sy.time !== "now" || sy.partitionkey !== "key-from-subject" || sy.subject !== "key-from-subject" || sy.tenantid !== "tenant-from-subject") f.push("state.dlq.unreadableSynthesizedEnvelope: time = el instante actual, y partitionkey, subject y tenantid salen del subject de NATS");
  if (sy.dataclassification !== "internal" || sy.dlqreason !== "poison" || sy.idIsNewUuidV7 !== true || !eq(sy.notReproducibleByteForByte, ["id", "correlationid", "time"])) f.push("state.dlq.unreadableSynthesizedEnvelope: internal, poison, id UUIDv7 nuevo; solo id, correlationid y time no son reproducibles");
  // `dlqerror` de un error que NO es de flux: solo el código y una frase fija; nunca el mensaje del driver (puede citar la fila).
  if (dq.dlqerrorOfNonFluxErrors !== "code-and-fixed-phrase-only" || dq.dlqerrorFluxErrorsKeepOwnMessage !== true || dq.dlqerrorNeverConcatenatesCauseChain !== true) f.push("state.dlq.dlqerrorOfNonFluxErrors debe ser code-and-fixed-phrase-only: el mensaje de un driver puede citar la fila entera y la DLQ guarda 90 días; los errores de flux conservan el suyo, sin su cadena de causas");
  if (dq.dlqerrorFixedPhrase !== "error sin clasificar de la aplicación; el mensaje se omite para no copiar datos a la DLQ (10-state.md §6)") f.push("state.dlq.dlqerrorFixedPhrase debe ser la frase que emiten los SDKs: «error sin clasificar de la aplicación; el mensaje se omite para no copiar datos a la DLQ (10-state.md §6)»");

  // ACL: lo concedible de estado, sin STREAM.INFO.STATE y con el tenant fijado.
  const acl = st.acl ?? {};
  if (acl.observerStreamInfoState !== "never") f.push('state.acl.observerStreamInfoState debe ser "never": STREAM.INFO con subjects_filter lista claves y tenants (A-39)');
  if (!eq(acl.devOnlyPatterns, ["consumerCreate", "dlqReaderCreate"])) f.push("state.acl.devOnlyPatterns debe ser [consumerCreate, dlqReaderCreate]");
  if (!re(acl.observerDirectGet ?? "").test("$JS.API.DIRECT.GET.STATE_X.state.x.y.v1.acme.*") || re(acl.observerDirectGet ?? "").test("$JS.API.DIRECT.GET.STATE_X")) f.push("state.acl.observerDirectGet: la forma por subject con el tenant fijado sí, la de cuerpo nunca");
  // Los permisos de estado no pueden solaparse con los de eventos: serían dos categorías para un permiso.
  for (const [k, v] of Object.entries(doc.security?.acl?.permissionPatterns ?? {})) {
    if (k.startsWith("$")) continue;
    for (const ej of ["$JS.API.CONSUMER.INFO.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1", "$JS.ACK.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1.>"]) {
      if (re(v).test(ej)) f.push(`security.acl.permissionPatterns.${k} concede un permiso de estado (${ej}): los de estado van en state.acl`);
    }
  }

  // Observabilidad: la métrica de estado NO va entre las siete (las suites de los SDKs cuentan siete).
  const ms = doc.observability?.metrics ?? {};
  if (Object.keys(ms).length !== 7) f.push(`observability.metrics debe tener SIETE métricas (las suites de los SDKs cuentan siete y comparan con las expuestas); tiene ${Object.keys(ms).length}. La de estado va en state.metrics`);
  const mw = st.metrics?.flux_state_writes_total;
  if (!eq(mw?.labels, ["contract", "outcome"])) f.push("state.metrics.flux_state_writes_total.labels debe ser [contract, outcome]");
  if (!eq(mw?.outcomes, ["ok", "stale", "conflict", "invalid_schema"])) f.push("state.metrics.flux_state_writes_total.outcomes debe ser [ok, stale, conflict, invalid_schema]");
  // Igual que publish() con un evento: TODO fallo de L3 es invalid_schema, también SCHEMA_NOT_FOUND (modo estricto, esquema ausente del bundle).
  if (!eq(mw?.invalidSchemaCodes, ["MISSING_AGGREGATE_VERSION", "INVALID_SCHEMA", "SCHEMA_NOT_FOUND"])) f.push("state.metrics.flux_state_writes_total.invalidSchemaCodes debe ser [MISSING_AGGREGATE_VERSION, INVALID_SCHEMA, SCHEMA_NOT_FOUND]: SCHEMA_NOT_FOUND en modo estricto cuenta como invalid_schema, igual que en un evento");
  for (const prohibida of doc.observability?.forbiddenLabels ?? []) {
    if ((mw?.labels ?? []).includes(prohibida)) f.push(`state.metrics.flux_state_writes_total usa la etiqueta prohibida "${prohibida}"`);
  }
  for (const prohibida of ["tenantid", "key"]) {
    if (!(st.metrics?.forbiddenLabels ?? []).includes(prohibida)) f.push(`state.metrics.forbiddenLabels debe incluir "${prohibida}": nunca la clave ni el tenant`);
  }
  if (st.metrics?.reusedConsumptionMetrics?.consumer !== "durable-without-tenant-suffix") f.push("state.metrics.reusedConsumptionMetrics.consumer: el durable SIN el sufijo de tenant (con él, cada tenant crea series)");
  return f;
}

// ─── Petición/respuesta — specification/11-request-reply.md ──────────────────────────────────────────
// Como `fallosDeEstado`: una función PURA sobre el documento, que se ejecuta sobre el protocol.json real (cero fallos) y
// sobre copias estropeadas a propósito (cada una DEBE dar al menos uno). Una invariante que solo se ha visto en verde no
// demuestra nada.
const CODIGOS_RPC = {
  RPC_UNAVAILABLE: "retryable",
  RPC_TIMEOUT: "retryable",
  STALE_REQUEST: "permanent",
  RPC_INVALID_REPLY: "permanent",
  RPC_MESSAGE_TOO_LARGE: "permanent",
  INVALID_RPC_CALL: "permanent",
};

/** ¿Hay algún subject concreto que casen a la vez dos patrones de NATS? `*` casa un token; `>`, uno o más al final.
 *  Es el algoritmo de `flux doctor` (11-request-reply.md §8): el runner de conformance lo mide contra un broker. */
/**
 * Las decisiones de los AJUSTES (conformance/harness/vectors.json → `ajustesVectors`): lo que la spec fijó tras medir las
 * divergencias de Node y Rust fuera de los vectores. Cada afirmación de protocol.json se comprueba aquí contra lo que dicen la
 * prosa y los vectores, y cada una tiene su mutación abajo.
 */
function fallosDeAjustes(doc) {
  const f = [];
  const eq = (a, b) => JSON.stringify(a) === JSON.stringify(b);
  const ce = doc.cloudevents ?? {};
  // 04-errors.md §3: `dlqerror` mide 1024 BYTES UTF-8, sin partir un carácter.
  const dq = ce.optionalExtensions?.dlqerror ?? ce.optionalExtensions?.dlqerror;
  if (dq?.maxBytes !== 1024 || dq?.truncation !== "utf8-bytes-never-split-a-character") f.push("cloudevents.optionalExtensions.dlqerror: 1024 BYTES UTF-8 como máximo, sin partir un carácter (no unidades UTF-16 ni puntos de código)");
  // 01-envelope.md §2.2: el `time` que da quien llama se normaliza o se rechaza.
  const bi = ce.requiredAttributes?.time?.builderInput ?? {};
  if (bi.accepts !== "rfc3339-date-time" || bi.normalizesTo !== "exactly-3-decimals-and-Z-in-UTC" || bi.fractionBeyondMillis !== "truncated") f.push("cloudevents.requiredAttributes.time.builderInput: un constructor admite un date-time de RFC 3339, lo normaliza a 3 decimales y Z en UTC y TRUNCA los decimales de más");
  if (bi.rejectsLeapSecond !== true || !eq(bi.yearRangeUtc, [0, 9999])) f.push("cloudevents.requiredAttributes.time.builderInput: no admite un segundo 60 ni un año en UTC fuera de 0000–9999");
  if (bi.invalidCode !== "WRONG_ATTRIBUTE_TYPE" || bi.invalidCodeInRpc !== "INVALID_RPC_CALL") f.push("cloudevents.requiredAttributes.time.builderInput: un time inválido es WRONG_ATTRIBUTE_TYPE (INVALID_RPC_CALL en petición/respuesta)");
  // 01-envelope.md §4.1: la forma canónica de `data`.
  const cf = doc.data?.canonicalForm ?? {};
  if (cf.reference !== "ecmascript-json-stringify-with-literal-utf8" || cf.insignificantWhitespace !== false || cf.numbers !== "untouched") f.push("data.canonicalForm: la de JSON.stringify con UTF-8 literal, sin espacios y con los números SIN tocar (01-envelope.md §2.5)");
  if (cf.keyOrder !== "as-given-repeated-key-keeps-first-position-and-last-value") f.push("data.canonicalForm.keyOrder: las claves en el orden en que se dan; una repetida conserva la posición de la primera y el valor de la última");
  const NO_PORTABLES = ["integer-outside-2^53-1", "float-or-exponent-notation", "array-index-like-key", "lone-utf16-surrogate", "nesting-over-128", "number-outside-double-range"];
  if (!eq(cf.notPortable, NO_PORTABLES)) f.push(`data.canonicalForm.notPortable debe listar lo que no tiene forma común entre lenguajes: ${JSON.stringify(NO_PORTABLES)}`);
  // 11-request-reply.md §3.3: `message` de un error, en puntos de código.
  const ml = doc.rpc?.error?.messageLength ?? {};
  if (ml.unit !== "unicode-code-points" || ml.min !== 1 || ml.max !== 1000) f.push("rpc.error.messageLength: de 1 a 1000 caracteres UNICODE (puntos de código, como maxLength de JSON Schema), no unidades UTF-16");
  // 11-request-reply.md §5.2: el `id` que cita el error de una petición ilegible.
  const un = doc.rpc?.envelope?.errorFallbackWhenRequestUnreadable ?? {};
  if (un.validIdMaxCodePoints !== 128 || un.validIdAllowsControlCharacters !== false) f.push("rpc.envelope.errorFallbackWhenRequestUnreadable: el `id` que cita el error es una cadena de 128 caracteres como máximo y sin caracteres de control");
  // 11-request-reply.md §4.3.4: la excepción es ESTRECHA.
  const ex = un.callerExemptedFromCheck4 ?? {};
  if (ex.only !== "error-with-tenantid-system-and-correlationid-equal-to-own-id" || !eq(ex.checksStillApplied, ["causationid-is-request-id", "error-data-fits-error-schema"])) f.push("rpc.envelope.errorFallbackWhenRequestUnreadable.callerExemptedFromCheck4: solo un error con tenantid system y correlationid igual a su propio id; las comprobaciones 3 y 5 siguen");
  return f;
}

function solapa(a, b) {
  const x = a.split("."), y = b.split(".");
  for (let i = 0; ; i++) {
    if (x[i] === ">") return y.length > i;
    if (y[i] === ">") return x.length > i;
    if (i >= x.length || i >= y.length) return x.length === y.length;
    if (x[i] === "*" || y[i] === "*" || x[i] === y[i]) continue;
    return false;
  }
}

/** @returns {string[]} */
function fallosDeRpc(doc) {
  const f = [];
  const r = doc.rpc;
  if (!r) return ["falta el bloque `rpc` (11-request-reply.md)"];
  const cs = doc.errors?.codes ?? {};
  const eq = (a, b) => JSON.stringify(a) === JSON.stringify(b);
  const re = (src) => { try { return new RegExp(src); } catch { return /(?!)/; } };

  // 04-errors.md §7: la lista de códigos es CERRADA, con su clase, y todos cuelgan de la capacidad `rpc`.
  for (const [c, clase] of Object.entries(CODIGOS_RPC)) {
    if (!cs[c]) { f.push(`errors.codes.${c}: falta (04-errors.md §7)`); continue; }
    if (cs[c].class !== clase) f.push(`errors.codes.${c}.class debe ser "${clase}", es ${JSON.stringify(cs[c].class)}`);
    if (cs[c].capability !== "rpc") f.push(`errors.codes.${c}.capability debe ser "rpc"`);
  }
  const deRpc = Object.entries(cs).filter(([k, v]) => !k.startsWith("$") && v?.capability === "rpc").map(([k]) => k).sort();
  if (!eq(deRpc, Object.keys(CODIGOS_RPC).sort())) f.push(`los códigos de la capacidad rpc son una lista CERRADA de seis; hay [${deRpc.join(", ")}]`);
  if (cs.RPC_UNAVAILABLE?.immediate !== true) f.push("errors.codes.RPC_UNAVAILABLE.immediate debe ser true: el broker lo dice en milisegundos y no se espera");
  for (const c of ["INVALID_RPC_CALL", "RPC_MESSAGE_TOO_LARGE"]) if (cs[c]?.failsBeforeNetwork !== true) f.push(`errors.codes.${c}.failsBeforeNetwork debe ser true`);
  if (cs.RPC_UNAVAILABLE?.failsBeforeNetwork === true) f.push("errors.codes.RPC_UNAVAILABLE no falla antes de la red: es la respuesta del broker");
  for (const c of ["request", "respond"]) if (!(cs.RESERVED_DOMAIN?.thrownBy ?? []).includes(c)) f.push(`errors.codes.RESERVED_DOMAIN.thrownBy debe incluir «${c}»: un dominio reservado se rechaza también en las llamadas de rpc`);
  if (!doc.capabilities?.definitions?.rpc) f.push("capabilities.definitions.rpc: falta la definición de la capacidad (con su evidencia)");
  else if (doc.capabilities.definitions.rpc.evidence !== "sdk-files") f.push('capabilities.definitions.rpc.evidence debe ser "sdk-files", como `security` y `state`');
  if ((doc.outOfScopeV1 ?? []).includes("request/reply")) f.push("outOfScopeV1 ya no debe incluir «request/reply»: entra en 1.1 como extensión opcional (00-protocol.md §6)");
  if (!(doc.naming?.reservedDomains?.names ?? []).includes("rpc")) f.push("naming.reservedDomains.names debe incluir «rpc»: el subject lleva ese prefijo");

  // Sin stream: core NATS.
  const t = r.transport ?? {};
  if (t.kind !== "core-nats" || t.stream !== "none" || t.publishViaJetStream !== false) f.push("rpc.transport: core NATS, sin stream y sin publicar por JetStream (A-56: si nadie responde, quien llama lo sabe al instante)");
  if (t.persistence !== "none" || t.dlq !== "none" || t.replies !== 1) f.push("rpc.transport: sin persistencia, sin DLQ y UNA respuesta por petición");

  // El subject: cinco tokens, y el nombre de la operación tiene la forma de un subject de evento.
  const fmt = r.subject?.format ?? "";
  if (r.subject?.tokens !== 5 || fmt.split(".").length !== 5 || !fmt.startsWith("rpc.")) f.push(`rpc.subject: debe tener 5 tokens y empezar por \`rpc.\` (tokens=${r.subject?.tokens}, formato «${fmt}»)`);
  if (r.operation?.tokens !== 4 || (r.operation?.format ?? "").split(".").length !== 4) f.push("rpc.operation: el nombre de la operación tiene 4 tokens, sin `rpc.`");
  if (r.operation?.pattern !== doc.naming?.subject?.pattern || r.operation?.equalsEventSubjectPattern !== true) f.push("rpc.operation.pattern debe ser EXACTAMENTE naming.subject.pattern: el nombre de una operación tiene la forma sintáctica de un subject de evento y lo que las separa es el prefijo `rpc.`");
  if (r.subject?.pattern !== `^rpc\\.${(r.operation?.pattern ?? "").slice(1)}`) f.push("rpc.subject.pattern debe ser `^rpc\\.` + rpc.operation.pattern: el subject es el nombre de la operación con el prefijo");
  for (const [ruta, nodo] of [["rpc.subject", r.subject], ["rpc.operation", r.operation], ["rpc.operationName", r.operationName]]) {
    const rx = re(nodo?.pattern ?? "");
    for (const v of nodo?.valid ?? []) if (!rx.test(v)) f.push(`${ruta}.valid: el patrón rechaza ${JSON.stringify(v)}`);
    for (const v of nodo?.invalid ?? []) if (rx.test(v)) f.push(`${ruta}.invalid: el patrón acepta ${JSON.stringify(v)}`);
  }
  if (r.operationName?.form !== "infinitive-kebab-case") f.push("rpc.operationName.form debe ser infinitive-kebab-case: la operación va en infinitivo, el evento en pasado");
  // Un dominio reservado no lo rechaza el patrón: lo rechaza RESERVED_DOMAIN (si no, un SDK daría «operación inválida» y no el código estable).
  for (const d of doc.naming?.reservedDomains?.names ?? []) {
    if (!re(r.operation?.pattern ?? "").test(`${d}.cliente.v1.obtener`)) f.push(`rpc.operation.pattern debería aceptar sintácticamente \`${d}.cliente.v1.obtener\`: lo rechaza RESERVED_DOMAIN, no el patrón`);
  }

  // Reservados: `request` y `respond` rechazan un dominio reservado antes de tocar la red, con el código estable.
  const rv = r.reserved ?? {};
  if (rv.domains !== "naming.reservedDomains.names" || rv.errorCode !== "RESERVED_DOMAIN" || !cs.RESERVED_DOMAIN || !eq(rv.appliesTo, ["request", "respond"]) || rv.prefixInOperationName !== "RESERVED_DOMAIN") f.push("rpc.reserved: request y respond rechazan los dominios reservados (naming.reservedDomains.names) con RESERVED_DOMAIN, y un nombre con el prefijo `rpc.` también");

  // El grupo de cola y la suscripción exacta.
  const q = r.queueGroup ?? {};
  if (q.name !== "service-name" || q.pattern !== "naming.service.pattern") f.push("rpc.queueGroup: el grupo de cola es el NOMBRE DE SERVICIO (naming.service.pattern): dos servicios distintos son dos grupos y cada uno ejecuta la petición (A-60)");
  if (q.subscription !== "exact-subject-per-operation" || q.wildcardSubscriptionForbidden !== true) f.push("rpc.queueGroup: una suscripción por operación, al subject EXACTO: con `rpc.<dominio>.>` una operación sin handler da timeout en vez de `no responders` (A-64)");

  // Tipos y esquemas.
  if (r.type?.error?.value !== "com.flux.rpc.error.v1" || r.type?.error?.sameForEveryOperation !== true) f.push("rpc.type.error.value debe ser com.flux.rpc.error.v1, el mismo para toda operación");
  if (r.dataschema?.error !== "https://schemas.internal/system/rpc/error/1.0.0.json" || r.error?.dataschema !== r.dataschema?.error) f.push("rpc.dataschema.error y rpc.error.dataschema deben ser https://schemas.internal/system/rpc/error/1.0.0.json");
  if (r.dataschema?.error?.replace("https://schemas.internal/", "schemas/") !== r.error?.schemaPath || r.schemaPath?.errorSchema !== r.error?.schemaPath) f.push("rpc.error.schemaPath y rpc.schemaPath.errorSchema deben ser la ruta del $id del dataschema del error");
  if (!re(r.type?.error?.pattern ?? "").test("com.flux.rpc.error.v1") || re(r.type?.request?.pattern ?? "").test("com.flux.rpc.error.v1") || re(r.type?.reply?.pattern ?? "").test("com.flux.rpc.error.v1")) f.push("rpc.type: el patrón del error y los de la petición y la respuesta no se distinguen");
  if (re(doc.naming?.type?.pattern ?? "").test("com.flux.rpc.error.v1") || re(doc.naming?.type?.pattern ?? "").test("com.flux.rpc.clientes.cliente.calcular-descuento.request.v1")) f.push("naming.type.pattern (el de los eventos) casa con un type de rpc: son espacios de nombres disjuntos");

  // Los errores: clase binaria, forma, y qué se hace con lo desconocido.
  const e = r.error ?? {};
  if (!eq(e.classes, ["retryable", "permanent"])) f.push("rpc.error.classes debe ser [retryable, permanent]: `poison` no viaja, para quien llama una petición que el otro lado no pudo interpretar es permanent");
  if (!eq(e.dataKeys, ["class", "code", "message", "retryAfterMs"]) || !eq(e.required, ["class", "code", "message"])) f.push("rpc.error: data es { class, code, message, retryAfterMs? } en ESE orden (el orden de claves es normativo) con los tres primeros obligatorios");
  if (e.dataclassification !== "internal" || e.messageNeverContainsDataValues !== true) f.push("rpc.error: el error va `internal` y su mensaje nunca cita valores de data");
  if (e.retryAfterMsOnlyWhen !== "retryable") f.push("rpc.error.retryAfterMsOnlyWhen debe ser retryable");
  if (e.handlerErrors?.unknown !== "permanent") f.push("rpc.error.handlerErrors.unknown debe ser permanent: una llamada no tiene numero de entrega, y retryable haria que el handler de quien llama reintentase 51 minutos un bug ajeno");
  if (e.handlerErrors?.unknownMessage !== "generic-never-the-exception-text") f.push("rpc.error.handlerErrors.unknownMessage: el texto de una excepcion no clasificada no se reenvia (puede llevar valores, rutas o PII)");
  if (!eq(e.callerMapsTo, { retryable: "RetryableError", permanent: "PermanentError" })) f.push("rpc.error.callerMapsTo debe ser retryable → RetryableError y permanent → PermanentError");
  if (e.codePattern !== "errors.codePattern") f.push("rpc.error.codePattern debe referir errors.codePattern (una sola fuente)");

  // El timeout: por debajo del ack_wait.
  const to = r.timeout ?? {};
  if (to.defaultMs !== 5000 || to.maxMs !== 25000 || to.minMs !== 1) f.push("rpc.timeout debe ser 5000 por defecto, 25000 como máximo y 1 como mínimo");
  if (!(to.maxMs < doc.consumer?.ackWaitSeconds * 1000) || to.ackWaitMs !== doc.consumer?.ackWaitSeconds * 1000 || to.maxBelowAckWait !== true) f.push("rpc.timeout.maxMs debe quedar por debajo del ack_wait (consumer.ackWaitSeconds): una llamada hecha desde un handler no debe provocar su reentrega");
  if (to.integerOnly !== true || to.outOfRangeCode !== "INVALID_RPC_CALL" || !cs.INVALID_RPC_CALL) f.push("rpc.timeout: entero, y fuera de rango es INVALID_RPC_CALL (no se recorta en silencio), que debe existir en errors.codes");
  if (to.attemptsPerCall !== 1) f.push("rpc.timeout.attemptsPerCall debe ser 1: `request` hace UN intento");

  // Idempotencia.
  const id = r.idempotency ?? {};
  if (id.responderMustDeduplicateById !== true || id.retryReusesIdAndBytes !== true) f.push("rpc.idempotency: el que responde deduplica por id, y un reintento reutiliza el mismo id y los mismos bytes");
  if (id.coreNatsDeduplicates !== false) f.push("rpc.idempotency.coreNatsDeduplicates debe ser false: duplicate_window es de JetStream (A-74)");
  if (id.timeoutIsAmbiguous !== true) f.push("rpc.idempotency.timeoutIsAmbiguous debe ser true: un timeout no dice que la operación no se ejecutó (A-61, A-62, A-63, A-70)");
  if (!eq(id.retryOnlyOn, ["RPC_UNAVAILABLE", "RPC_TIMEOUT"]) || id.sdkAutomaticRetries !== "optional-default-0") f.push("rpc.idempotency: reintentos opcionales, por defecto 0, y solo ante RPC_UNAVAILABLE y RPC_TIMEOUT");

  // La firma: la ventana solo con `require`.
  const sg = r.signing ?? {};
  if (sg.staleRequestWindowSeconds !== 300 || sg.appliesOnlyWithSigningMode !== "require" || sg.symmetric !== true || sg.code !== "STALE_REQUEST") f.push("rpc.signing: ventana de 300 s, simétrica, solo con la firma en modo require, código STALE_REQUEST");
  if (!(doc.signing?.verificationModes ?? []).includes("require")) f.push("signing.verificationModes debe incluir require: la ventana de STALE_REQUEST solo existe con él");
  if (sg.responderDoesNotRememberIds !== true) f.push("rpc.signing.responderDoesNotRememberIds debe ser true: rechazar un id repetido rompería un reintento legítimo; la defensa es la idempotencia");

  // El tamaño.
  const sz = r.size ?? {};
  if (sz.maxMessageBytes !== doc.data?.maxMessageBytes || sz.brokerMaxPayloadDefault !== 1048576) f.push("rpc.size.maxMessageBytes debe ser data.maxMessageBytes (1 MiB) y el max_payload por defecto del broker, 1048576 (A-65)");
  if (sz.limitCountsHeaders !== true || sz.code !== "RPC_MESSAGE_TOO_LARGE" || !cs.RPC_MESSAGE_TOO_LARGE) f.push("rpc.size: las cabeceras cuentan, y el código es RPC_MESSAGE_TOO_LARGE");
  if (sz.replyTooLarge !== "responder-replies-error-event") f.push("rpc.size.replyTooLarge: el que responde contesta con un error; si intentara publicarla, quien llama solo vería un timeout (A-65)");

  // Sin respuestas: no responders.
  const nr = r.noResponders ?? {};
  if (nr.status !== 503 || nr.code !== "RPC_UNAVAILABLE" || !eq(nr.connectFlags, ["headers", "no_responders"]) || nr.withoutFlagsCallerSeesTimeout !== true) f.push("rpc.noResponders: estado 503, RPC_UNAVAILABLE, y exige las banderas headers y no_responders en CONNECT (sin ellas, timeout: A-57)");

  // El sobre y las cabeceras.
  const ev = r.envelope ?? {};
  if (!eq(ev.newRootAttributes, [])) f.push("rpc.envelope.newRootAttributes debe ser []: la petición/respuesta no añade atributos al sobre (01-envelope.md §3.4)");
  if (ev.attributeOrder !== doc.cloudevents?.attributeOrder) f.push("rpc.envelope.attributeOrder debe ser el de cloudevents.attributeOrder: data va siempre el último");
  if (ev.reply?.causationid !== "request.id" || ev.reply?.correlationid !== "request.correlationid" || ev.reply?.tenantid !== "request.tenantid") f.push("rpc.envelope.reply: la respuesta hereda correlationid y tenantid y su causationid es el id de la petición");
  if (!eq(r.headers?.required, []) || r.headers?.envelopeIsTheOnlySourceOfMetadata !== true) f.push("rpc.headers: ninguna cabecera obligatoria, y el sobre es la única fuente de traceparent, correlationid y tenantid");
  if (r.headers?.travelMeasured !== true) f.push("rpc.headers.travelMeasured debe ser true: las cabeceras SÍ viajan (A-74) y por eso la decisión de no usarlas es deliberada");

  // Quien llama y quien responde.
  if (r.caller?.invalidReplyCode !== "RPC_INVALID_REPLY" || r.caller?.deniedCode !== "PERMISSION_DENIED" || r.caller?.deniedMustFailFast !== true) f.push("rpc.caller: una respuesta que no es de esta petición es RPC_INVALID_REPLY, y una petición denegada falla rápido con PERMISSION_DENIED (async-nats no lo hace solo: A-72)");
  if (!eq(r.caller?.replyChecks, ["is-cloudevent", "type-is-reply-or-error", "causationid-is-request-id", "correlationid-equals", "tenantid-equals"])) f.push("rpc.caller.replyChecks: la respuesta se valida contra la petición (las cinco comprobaciones)");
  if (r.caller?.ignoresLateReplies !== true) f.push("rpc.caller.ignoresLateReplies debe ser true: una respuesta tardía llega al inbox (A-61)");
  if (r.responder?.oneReplyPerRequest !== true || r.responder?.tenantFilterByConnection !== false) f.push("rpc.responder: una respuesta por petición, y el responder NO filtra por el tenant de connect() (el grupo de cola no sabe de tenants)");
  if (r.responder?.stop !== "unsubscribe-then-wait-inflight-then-close") f.push("rpc.responder.stop debe ser unsubscribe-then-wait-inflight-then-close: cerrar la conexión de golpe pierde lo que se atiende (A-62)");

  // El doctor: la trampa A-18 y el algoritmo de solape, probado en los dos sentidos.
  const dc = r.doctor ?? {};
  if (dc.check !== "no-stream-captures-rpc" || dc.severity !== "error" || dc.provisioningNeverCreates !== "stream-whose-subjects-overlap-rpc") f.push("rpc.doctor: ningún stream captura rpc (error), y flux provision no crea ninguno");
  const forma = dc.overlapsWith ?? "";
  if (forma !== "rpc.*.*.*.*" || !solapa(forma, "rpc.clientes.cliente.v1.obtener")) f.push("rpc.doctor.overlapsWith debe ser rpc.*.*.*.*, la forma de todo subject de operación");
  for (const x of dc.flagged ?? []) if (!solapa(x, forma)) f.push(`rpc.doctor.flagged: «${x}» no se solapa con ${forma}, así que el doctor no lo marcaría`);
  for (const x of dc.notFlagged ?? []) if (solapa(x, forma)) f.push(`rpc.doctor.notFlagged: «${x}» se solapa con ${forma}, así que el doctor lo marcaría`);
  for (const x of ["rpc.>", ">", "*.>", "rpc.*.>", "rpc.clientes.>", "*.*.*.*.*"]) if (!(dc.flagged ?? []).includes(x)) f.push(`rpc.doctor.flagged debe incluir «${x}»`);
  for (const x of dc.withoutAckOnly ?? []) if (!solapa(x, forma)) f.push(`rpc.doctor.withoutAckOnly: «${x}» no se solapa con rpc, y entonces no es un filtro que el doctor deba marcar`);

  // La ACL.
  const ac = r.acl ?? {};
  const pub = re(ac.callerPublish ?? "(?!)");
  const sub = re(ac.ownerSubscribe ?? "(?!)");
  if (!pub.test("rpc.clientes.cliente.v1.obtener")) f.push("rpc.acl.callerPublish debe aceptar el subject exacto de una operación");
  for (const x of ac.callerNeverPublish ?? []) if (pub.test(x)) f.push(`rpc.acl.callerNeverPublish: «${x}» lo concede callerPublish`);
  for (const x of ["rpc.>", "rpc.clientes.>"]) if (!(ac.callerNeverPublish ?? []).includes(x)) f.push(`rpc.acl.callerNeverPublish debe incluir «${x}»`);
  if (!sub.test("rpc.clientes.>")) f.push("rpc.acl.ownerSubscribe debe aceptar rpc.<dominio>.>");
  for (const x of ac.ownerNeverSubscribe ?? []) if (sub.test(x)) f.push(`rpc.acl.ownerNeverSubscribe: «${x}» lo concede ownerSubscribe`);
  for (const x of [">", "rpc.>", "rpc.*.>"]) if (!(ac.ownerNeverSubscribe ?? []).includes(x)) f.push(`rpc.acl.ownerNeverSubscribe debe incluir «${x}»`);
  if (ac.callerSubscribe !== "security.acl.inbox.permissionPattern") f.push("rpc.acl.callerSubscribe debe referir el inbox privado de security.acl.inbox (sin él, cualquiera lee las respuestas ajenas: A-71)");
  if (ac.ownerPublishOnInboxes !== "never") f.push('rpc.acl.ownerPublishOnInboxes debe ser "never": el dueño responde con allow_responses, no con un permiso sobre inboxes (A-68)');
  const ar = ac.ownerAllowResponses ?? {};
  if (ar.required !== true || ar.maxReplies !== 1 || ar.defaultExpires !== "none") f.push("rpc.acl.ownerAllowResponses: obligatorio, una respuesta por petición y SIN caducidad por defecto (A-68)");
  if (!((ar.recommendedExpiresSeconds ?? 0) * 1000 > (r.timeout?.maxMs ?? Infinity))) f.push("rpc.acl.ownerAllowResponses.recommendedExpiresSeconds debe superar el timeout máximo: no puede romper ninguna respuesta legítima");
  // Los permisos de rpc no se confunden con los de eventos ni con lo denegable.
  for (const [k, v] of Object.entries(doc.security?.acl?.permissionPatterns ?? {})) {
    if (k.startsWith("$")) continue;
    for (const ej of ["rpc.clientes.cliente.v1.obtener", "rpc.clientes.>"]) if (re(v).test(ej)) f.push(`security.acl.permissionPatterns.${k} concede un permiso de rpc (${ej}): los de rpc van en rpc.acl`);
  }
  if (re(doc.security?.acl?.neverGrantedPattern ?? "(?!)").test("rpc.clientes.cliente.v1.obtener")) f.push("security.acl.neverGrantedPattern caza el subject de una operación: es concedible");

  // Los esquemas.
  const sp = r.schemaPath ?? {};
  if (sp.pairRequired !== true || sp.pairSameMajor !== true || sp.errorSchemaRequiredWithAnyRpcSchema !== true || sp.bundleIndex !== "rpc" || sp.dataRootType !== "object") f.push("rpc.schemaPath: una operación es un PAR del mismo mayor, el esquema del error es obligatorio, `data` es un objeto y el bundle tiene el índice `rpc`");
  if (r.compatibility?.request?.extensibleEnumRequiredToAddValue !== false || r.compatibility?.reply?.extensibleEnumRequiredToAddValue !== true) f.push("rpc.compatibility: la dirección se invierte: añadir un valor a un enum de una RESPUESTA exige x-extensible-enum (leen los llamantes) y de una PETICIÓN no (lee el dueño)");
  if (r.compatibility?.deployOrder !== "responder-first-then-callers") f.push("rpc.compatibility.deployOrder debe ser responder-first-then-callers");

  // Observabilidad: las tres métricas van en su bloque, no entre las siete.
  const ms = r.metrics ?? {};
  const nombres = Object.keys(ms).filter((k) => k.startsWith("flux_")).sort();
  if (!eq(nombres, ["flux_rpc_client_requests_total", "flux_rpc_duration_seconds", "flux_rpc_server_requests_total"])) f.push(`rpc.metrics debe tener exactamente flux_rpc_client_requests_total, flux_rpc_server_requests_total y flux_rpc_duration_seconds; tiene [${nombres.join(", ")}]`);
  if (!eq(ms.flux_rpc_client_requests_total?.labels, ["operation", "outcome"]) || !eq(ms.flux_rpc_server_requests_total?.labels, ["operation", "outcome"])) f.push("rpc.metrics: los dos contadores llevan las etiquetas [operation, outcome]");
  if (!eq(ms.flux_rpc_duration_seconds?.labels, ["operation", "role"]) || !eq(ms.flux_rpc_duration_seconds?.roles, ["client", "server"]) || ms.flux_rpc_duration_seconds?.type !== "histogram") f.push("rpc.metrics.flux_rpc_duration_seconds: histograma con [operation, role], role ∈ client | server");
  if (ms.flux_rpc_duration_seconds?.buckets !== "observability.durationBucketsSeconds") f.push("rpc.metrics.flux_rpc_duration_seconds.buckets debe referir observability.durationBucketsSeconds (el último bucket es el ack_wait)");
  if (!eq(ms.flux_rpc_client_requests_total?.outcomes, ["ok", "unavailable", "timeout", "retryable", "permanent", "invalid_schema", "invalid_reply"])) f.push("rpc.metrics.flux_rpc_client_requests_total.outcomes debe ser ok, unavailable, timeout, retryable, permanent, invalid_schema, invalid_reply");
  if (!eq(ms.flux_rpc_server_requests_total?.outcomes, ["ok", "retryable", "permanent", "poison", "invalid_schema", "invalid_signature", "stale_request"])) f.push("rpc.metrics.flux_rpc_server_requests_total.outcomes debe ser ok, retryable, permanent, poison, invalid_schema, invalid_signature, stale_request");
  for (const prohibida of [...(doc.observability?.forbiddenLabels ?? []), "tenant", "caller", "client", "service"]) {
    if (!(ms.forbiddenLabels ?? []).includes(prohibida)) f.push(`rpc.metrics.forbiddenLabels debe incluir "${prohibida}": nunca el tenant, el id ni quién llama como etiqueta`);
    for (const m of nombres) if ((ms[m]?.labels ?? []).includes(prohibida)) f.push(`rpc.metrics.${m} usa la etiqueta prohibida "${prohibida}"`);
  }
  if (Object.keys(doc.observability?.metrics ?? {}).length !== 7) f.push(`observability.metrics debe tener SIETE métricas; tiene ${Object.keys(doc.observability?.metrics ?? {}).length}. Las de rpc van en rpc.metrics`);
  if (doc.observability?.capabilityMetrics?.rpc !== "rpc.metrics") f.push("observability.capabilityMetrics.rpc debe ser rpc.metrics");
  if (!re(ms.operationLabelPattern ?? "").test("clientes.cliente.v1.obtener") || re(ms.operationLabelPattern ?? "").test("rpc.clientes.cliente.v1.obtener")) f.push("rpc.metrics.operationLabelPattern: el nombre de la operación, sin el prefijo rpc.");

  // PII: la petición/respuesta tiene la suya.
  if (!doc.security?.piiInPayload?.rpc || doc.security.piiInPayload.rpc === "forbidden") f.push("security.piiInPayload.rpc: el dato personal PUEDE ir como parámetro de una orden (no se persiste), con x-pii");
  return f;
}


// ─── El borde: gateway HTTP — specification/12-edge.md ───────────────────────────────────────────────────────────
// Como `fallosDeRpc`: una función PURA sobre el documento, que se ejecuta sobre el protocol.json real (cero fallos) y sobre
// copias estropeadas a propósito (cada una DEBE dar al menos uno). Además RECALCULA lo que el documento afirma con vectores:
// los HMAC de la entrega saliente (con `node:crypto`, no con el gateway) y los `id` deterministas (con BigInt, no con el SDK).
// Una invariante que solo se ha visto en verde no demuestra nada, y un vector copiado de la salida de quien lo ejecuta tampoco.

/** El id determinista de 01-envelope.md §2.6.1, escrito otra vez aquí: independiente del SDK que se verifica. */
function idDeterminista(source, clave, ms) {
  const h = createHash("sha256").update(`${source}|${clave}`, "utf8").digest();
  const r = BigInt(`0x${h.subarray(0, 10).toString("hex")}`) >> 6n;
  const u = (BigInt(ms) << 80n) | (0x7n << 76n) | ((r >> 62n) << 64n) | (0b10n << 62n) | (r & ((1n << 62n) - 1n));
  const x = u.toString(16).padStart(32, "0");
  return `${x.slice(0, 8)}-${x.slice(8, 12)}-${x.slice(12, 16)}-${x.slice(16, 20)}-${x.slice(20)}`;
}

/** El HMAC de la entrega saliente: `t + "." + cuerpo` (o solo el cuerpo si no hay `t`: el formato de GitHub). */
function hmacHex(secreto, t, cuerpo) {
  const h = createHmac("sha256", secreto);
  if (t !== null && t !== undefined) h.update(`${t}.`, "utf8");
  return h.update(cuerpo, "utf8").digest("hex");
}

/** Lo que el documento dice de una respuesta HTTP del receptor, como función: el contrato de 12-edge.md §4.4. */
function claseDeRespuesta(doc, status) {
  const r = doc.edge?.delivery?.response ?? {};
  if (status >= 200 && status <= 299) return "ack";
  if ((doc.errors?.classes?.retryable?.httpStatus ?? []).includes(status)) return "retryable";
  if ((r.retryableBounded?.httpStatus ?? []).includes(status) || (r.retryableBounded?.alsoEveryOther5xx === true && status >= 500 && status <= 599)) return "retryable-bounded";
  return "permanent";
}

/** Un manifiesto mínimo para comprobar `via`: lo que dice `edge.manifest` frente a lo que hace el analizador y el generador. */
const BUNDLE_DE_EDGE = { subjects: ["pedidos.pedido.v1.creado", "inventario.stock.v1.ajustado"], state: [], rpc: ["clientes.cliente.v1.obtener"] };
const clienteDeBorde = (extra = {}) => ({
  name: "tienda-web", owns: ["pedidos"], consumes: ["inventario.stock.v1.ajustado"], calls: ["clientes.cliente.v1.obtener"], ...extra,
});
const manifiestoDeEdge = (cliente) => ({
  services: [
    cliente,
    { name: "inventario-api", owns: ["inventario"] },
    { name: "crm-api", owns: ["clientes"], serves: ["clientes.cliente.v1.obtener"] },
  ],
});

/** @returns {string[]} */
function fallosDeEdge(doc, { analizar = analizarManifiesto, emitir = permisosDe } = {}) {
  const f = [];
  const e = doc.edge;
  if (!e) return ["falta el bloque `edge` (12-edge.md)"];
  const eq = (a, b) => JSON.stringify(a) === JSON.stringify(b);
  const re = (src) => { try { return new RegExp(src); } catch { return /(?!)/; } };
  const cs = doc.errors?.codes ?? {};

  // No es una capacidad de SDK: ni definida ni declarada.
  if (e.capability !== null) f.push("edge.capability debe ser null: el gateway no es una capacidad de SDK (12-edge.md encabezado)");
  if (doc.capabilities?.definitions?.edge) f.push("capabilities.definitions.edge no debe existir: el gateway no es una capacidad de SDK");
  for (const [sdk, v] of Object.entries(doc.capabilities?.sdks ?? {})) if ((v?.capabilities ?? []).includes("edge")) f.push(`capabilities.sdks.${sdk} declara «edge»: ningún SDK implementa el gateway`);
  if (!eq(e.normative, ["delivery", "externalId"])) f.push("edge.normative debe ser [delivery, externalId]: es normativo SOLO el formato de la entrega saliente y el id determinista");
  if (!eq(e.implementationOnly, ["gateway", "manifest"])) f.push("edge.implementationOnly debe ser [gateway, manifest]: el resto es una implementación, no el protocolo");
  if (e.gateway?.implementation !== true) f.push("edge.gateway.implementation debe ser true");

  // ── La entrega saliente ──────────────────────────────────────────────────────────────────────────────────────
  const d = e.delivery ?? {};
  if (d.method !== "POST") f.push("edge.delivery.method debe ser POST");
  if (d.contentType !== "application/cloudevents+json; charset=utf-8" || d.contentTypeIsStructuredMode !== true) f.push("edge.delivery.contentType debe ser application/cloudevents+json; charset=utf-8, el structured mode de 01-envelope.md §1");
  if (d.body?.isEventVerbatim !== true || d.body?.sameBytesEveryAttempt !== true || d.body?.dlqExtensionsStripped !== true || d.body?.serialization !== "cloudevents.attributeOrder") f.push("edge.delivery.body: el CloudEvent tal cual (orden de atributos de cloudevents.attributeOrder, sin extensiones dlq*) y los mismos bytes en cada intento");
  if (d.headers?.idempotencyKey?.name !== "Idempotency-Key" || d.headers?.idempotencyKey?.value !== "event.id" || d.headers?.idempotencyKey?.sameEveryAttempt !== true) f.push("edge.delivery.headers.idempotencyKey: `Idempotency-Key` = el id del evento, el mismo en cada intento");
  if (d.headers?.signature?.name !== "Flux-Signature") f.push("edge.delivery.headers.signature.name debe ser Flux-Signature");
  const sg = d.signature ?? {};
  if (sg.algorithm !== "HMAC-SHA256" || sg.signedMessage !== "<t>.<body-bytes>" || sg.encoding !== "hex-lowercase" || sg.hexLength !== 64 || sg.timestampUnit !== "seconds" || sg.key !== "secret-utf8-bytes") f.push("edge.delivery.signature: HMAC-SHA256 de `<t>.<bytes del cuerpo>`, en hexadecimal minúsculas de 64 caracteres, t en segundos");
  if (sg.timestampRenewedEveryAttempt !== true) f.push("edge.delivery.signature.timestampRenewedEveryAttempt debe ser true: con un t fijo, la tolerancia rechazaría los reintentos de 51 min");
  if (sg.multipleV1ForRotation !== true || sg.unknownFieldsIgnored !== true) f.push("edge.delivery.signature: varios v1 para rotar secretos sin corte, y los campos desconocidos se ignoran");
  const rc = sg.receiver ?? {};
  if (rc.verifyRawBytes !== true || rc.validateHexFormatBeforeDecoding !== true) f.push("edge.delivery.signature.receiver: se verifica sobre los bytes en bruto y el formato hexadecimal se valida ANTES de decodificar (en Node, Buffer.from('abc','hex') da un byte sin error: A-154)");
  if (rc.compare !== "constant-time") f.push("edge.delivery.signature.receiver.compare debe ser constant-time");
  if (rc.toleranceSeconds !== 300 || rc.toleranceSymmetric !== true) f.push("edge.delivery.signature.receiver: tolerancia de 300 s y simétrica (un t del futuro también se rechaza)");
  if (rc.rejectedAs !== "4xx") f.push("edge.delivery.signature.receiver.rejectedAs debe ser 4xx: una firma rechazada es PERMANENT, reintentarla no la arregla");
  if (rc.deduplicateBy !== "Idempotency-Key") f.push("edge.delivery.signature.receiver.deduplicateBy debe ser Idempotency-Key");
  if (sg.secret?.neverInConfiguration !== true || !(sg.secret?.recommendedMinBytes >= 32)) f.push("edge.delivery.signature.secret: nunca en la configuración y de al menos 32 bytes");
  const sigRe = re(sg.pattern ?? "(?!)");
  for (const v of sg.valid ?? []) if (!sigRe.test(v)) f.push(`edge.delivery.signature.valid: el patrón rechaza ${JSON.stringify(v)}`);
  for (const v of sg.invalid ?? []) if (sigRe.test(v)) f.push(`edge.delivery.signature.invalid: el patrón acepta ${JSON.stringify(v)}`);
  // Los vectores se RECALCULAN: un v1 copiado de la salida del gateway no demuestra nada.
  if ((sg.vectors ?? []).length < 4) f.push("edge.delivery.signature.vectors: faltan vectores (cuerpo mínimo, vacío, el evento con no-ASCII y el de GitHub)");
  for (const v of sg.vectors ?? []) {
    const calculado = hmacHex(v.secret, v.t, v.body);
    if (calculado !== v.v1) f.push(`edge.delivery.signature.vectors «${v.name}»: HMAC-SHA256 recalculado ${calculado} ≠ ${v.v1}`);
    if (!/^[0-9a-f]{64}$/.test(v.v1 ?? "")) f.push(`edge.delivery.signature.vectors «${v.name}»: v1 no es hexadecimal de 64 minúsculas`);
  }
  if (!(sg.vectors ?? []).some((v) => /[^\x00-\x7f]/.test(v.body ?? ""))) f.push("edge.delivery.signature.vectors: debe haber un cuerpo con no-ASCII: el HMAC es sobre bytes UTF-8, no sobre caracteres");
  if (!(sg.vectors ?? []).some((v) => v.t === null && v.secret === "It's a Secret to Everybody" && v.body === "Hello, World!")) f.push("edge.delivery.signature.vectors: falta el vector publicado de GitHub (un HMAC que no es el nuestro: comprueba la primitiva, no el formato)");
  const rot = sg.rotationVector ?? {};
  if ((rot.secrets ?? []).length < 2 || !eq((rot.secrets ?? []).map((s) => hmacHex(s, rot.t, rot.body)), rot.v1)) f.push("edge.delivery.signature.rotationVector: dos secretos, y sus v1 recalculados");
  else if (!sigRe.test(`t=${rot.t},${rot.v1.map((x) => `v1=${x}`).join(",")}`)) f.push("edge.delivery.signature.rotationVector: la cabecera con dos v1 no cumple el patrón");

  const rs = d.response ?? {};
  if (rs.bodyIgnored !== true || rs.bodyNeverLogged !== true) f.push("edge.delivery.response: el cuerpo de la respuesta del receptor se descarta y no se registra");
  if (rs.redirectsFollowed !== false) f.push("edge.delivery.response.redirectsFollowed debe ser false: seguirla enviaría el cuerpo firmado a una URL que el operador no configuró");
  if (rs.firmRejectedIsPermanent !== true) f.push("edge.delivery.response.firmRejectedIsPermanent debe ser true: reintentar una firma rechazada no la arregla");
  if (rs.retryable?.httpStatus !== "errors.classes.retryable.httpStatus" || rs.retryable?.budget !== doc.consumer?.maxDeliver || rs.retryable?.budget !== doc.errors?.budgets?.recognizedRetryable) f.push("edge.delivery.response.retryable: los de errors.classes.retryable.httpStatus, con el presupuesto completo (consumer.maxDeliver)");
  if (rs.retryableBounded?.budget !== doc.errors?.unknownRetryBudget || rs.retryableBounded?.budget !== doc.errors?.budgets?.unknown) f.push("edge.delivery.response.retryableBounded.budget debe ser el de lo desconocido (errors.unknownRetryBudget): un 500 no es un 503");
  if (rs.permanent?.budget !== doc.errors?.budgets?.permanent) f.push("edge.delivery.response.permanent.budget debe ser errors.budgets.permanent (1: a la DLQ sin reintentos)");
  if (rs.ack?.httpStatus !== "2xx") f.push("edge.delivery.response.ack debe ser 2xx");
  if (re(rs.codePattern ?? "(?!)").source === "(?:)" || !re(doc.errors?.codePattern ?? "(?!)").test("HTTP_503")) f.push("edge.delivery.response.codePattern / errors.codePattern: el código HTTP_<n> debe ser un code válido");
  // La función de clasificación, probada sobre TODO el rango de estados HTTP.
  for (const x of rs.examples ?? []) if (claseDeRespuesta(doc, x.status) !== x.outcome) f.push(`edge.delivery.response.examples: el ${x.status} es «${claseDeRespuesta(doc, x.status)}» según el documento y el ejemplo dice «${x.outcome}»`);
  for (let st = 100; st <= 599; st++) {
    const c = claseDeRespuesta(doc, st);
    if ((st >= 200 && st <= 299) !== (c === "ack")) { f.push(`edge.delivery.response: el estado ${st} es «${c}»: ack debe ser exactamente 2xx`); break; }
    if (st >= 300 && st <= 399 && c !== "permanent") { f.push(`edge.delivery.response: la redirección ${st} es «${c}», debe ser permanent`); break; }
    if (st >= 400 && st <= 499 && ![408, 425, 429].includes(st) && c !== "permanent") { f.push(`edge.delivery.response: el ${st} es «${c}»: un 4xx que no es 408, 425 ni 429 es permanent y va a la DLQ sin reintentos`); break; }
    if (st >= 500 && st <= 599 && c === "permanent") { f.push(`edge.delivery.response: el ${st} es permanent: todo 5xx se reintenta (completo si es de errors.classes.retryable.httpStatus, acotado si no)`); break; }
  }
  for (const st of [429, 502, 503, 504]) if (claseDeRespuesta(doc, st) !== "retryable") f.push(`edge.delivery.response: el ${st} debe ser retryable (presupuesto completo)`);
  for (const st of [500, 501, 505, 599]) if (claseDeRespuesta(doc, st) !== "retryable-bounded") f.push(`edge.delivery.response: el ${st} debe ser retryable-bounded (presupuesto de 2): no es un 503`);
  const nw = rs.network ?? {};
  const ejemplosRed = [...(doc.errors?.classes?.retryable?.syscallCodesExample ?? []), "TIMEOUT"];
  if (!eq([...(nw.retryable ?? [])].sort(), [...ejemplosRed].sort())) f.push("edge.delivery.response.network.retryable debe ser errors.classes.retryable.syscallCodesExample más TIMEOUT");
  if (nw.retryableBounded !== "anything-else") f.push("edge.delivery.response.network.retryableBounded: lo desconocido es retryable acotado (04-errors.md §2.1)");
  const tl = nw.tlsVerificationFailure ?? {};
  if (tl.class !== "permanent" || !re(doc.errors?.codePattern ?? "(?!)").test(tl.code ?? "") || !(tl.systemCodes ?? []).includes("DEPTH_ZERO_SELF_SIGNED_CERT") || !(tl.systemCodes ?? []).includes("ERR_TLS_CERT_ALTNAME_INVALID")) f.push("edge.delivery.response.network.tlsVerificationFailure: un certificado que no verifica es permanent, con un code estable y los códigos de Node medidos (A-161)");
  const to = d.timeout ?? {};
  if (to.defaultMs !== 10000 || to.maxMs !== doc.rpc?.timeout?.maxMs || !(to.maxMs < (doc.consumer?.ackWaitSeconds ?? 0) * 1000) || to.maxBelowAckWait !== true || to.scope !== "total-attempt") f.push("edge.delivery.timeout: 10 s por defecto y como máximo el del rpc (25 s, por debajo del ack_wait), total por intento");
  const ra = d.retryAfter ?? {};
  if (ra.appliesTo !== "first-retry-only" || ra.maxMs !== 300000 || !eq(ra.formats, ["delta-seconds", "http-date"]) || ra.onlyOn !== "retryable-outcomes") f.push("edge.delivery.retryAfter: solo acorta o alarga el PRIMER reintento (03-delivery.md §2.2), recortado a 300 s, en segundos o fecha HTTP, y solo en respuestas reintentables");
  if (d.retries !== "consumer-canonical-config" || d.ordering !== "none") f.push("edge.delivery: los reintentos son los del consumidor canónico y no hay orden");
  if (d.concurrencyPerDelivery !== "sequential" || d.deliveriesPerSubjectPerClient !== 1) f.push("edge.delivery: una entrega por subject y cliente (un durable compartido reparte los mensajes: A-47) y secuencial");
  const cl = d.classification ?? {};
  if (!eq(cl.allowedByDefault, ["public", "internal"]) || cl.confidentialRequiresExplicitOptIn !== true || cl.confidentialRequiresHttps !== true || cl.restricted !== "never") f.push("edge.delivery.classification: por defecto public e internal; confidential solo con autorización explícita y https; restricted nunca");
  if (cl.deniedClass !== "permanent" || !re(doc.errors?.codePattern ?? "(?!)").test(cl.deniedCode ?? "") || cl.deniedMessageNeverQuotesData !== true) f.push("edge.delivery.classification: una clasificación no permitida es permanent, con un code estable y sin citar data");
  if (!eq(d.receiverMust, ["verify-raw-bytes", "validate-hex-format", "constant-time-compare", "check-tolerance-symmetric", "deduplicate-by-idempotency-key", "2xx-only-after-storing"])) f.push("edge.delivery.receiverMust: lo que DEBE el receptor, en orden");
  if (d.state?.normative !== false || d.state?.bodyIsEvent !== false || d.state?.defaultClassification !== "confidential") f.push("edge.delivery.state: la entrega de estado NO es normativa (watch entrega la entrada, no el CloudEvent) y su clasificación por defecto es confidential");

  // ── El id determinista ───────────────────────────────────────────────────────────────────────────────────────
  const x = e.externalId ?? {};
  if (x.layout !== "uuidv7" || x.timeBits !== 48 || x.randomBits !== 74 || x.randABits !== 12 || x.randBBits !== 62 || x.randABits + x.randBBits !== x.randomBits || x.timeBits + x.randomBits + 6 !== 128) f.push("edge.externalId: UUIDv7 de 128 bits = 48 de tiempo + 6 fijos (versión y variante) + 74 aleatorios (12 de rand_a y 62 de rand_b)");
  if (x.hash !== "SHA-256" || x.separator !== "|" || x.encoding !== "UTF-8-literal" || x.derivation !== "first-74-bits-of-SHA-256(source|externalKey)") f.push("edge.externalId: SHA-256 de `<source>|<clave>` en UTF-8 literal, los 74 primeros bits");
  if (x.timeIs !== "the-instant-of-the-external-fact" || x.requiresTime !== true || x.timeNeverNow !== true) f.push("edge.externalId: el tiempo es el del hecho externo, exigido y nunca «ahora» (con «ahora» cada reenvío tendría su id)");
  if (x.externalKeyMinBytes !== 1 || x.externalKeyMaxBytes !== 512 || x.sourceMustNotContain !== "|" || !eq(x.msRange, [0, 2 ** 48 - 1])) f.push("edge.externalId: la clave tiene de 1 a 512 bytes, el source no lleva `|` y los ms caben en 48 bits");
  if (x.pureFunction !== true || x.doesNotReplaceConsumerDedupe !== true || x.keyIsUniquePerFactWithinSource !== true) f.push("edge.externalId: función pura; la clave es única por hecho dentro del source; no sustituye a la deduplicación del consumidor");
  if (x.providerWithoutFactTime?.hasDeterministicId !== false || x.providerWithoutFactTime?.msZeroAdopted !== false || x.providerWithoutFactTime?.gatewayHook !== "idempotent-false") f.push("edge.externalId.providerWithoutFactTime: sin instante del hecho no hay id determinista (el hook es idempotent:false) y no se adopta «ms = 0»");
  if (x.pattern !== doc.cloudevents?.requiredAttributes?.id?.pattern && !re(x.pattern ?? "(?!)").test("01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55")) f.push("edge.externalId.pattern debe aceptar un UUIDv7 normal: el determinista ES un UUIDv7 válido");
  if (doc.cloudevents?.requiredAttributes?.id?.deterministicWhenImported !== "edge.externalId" || doc.cloudevents?.requiredAttributes?.id?.uuidVersion !== 7) f.push("cloudevents.requiredAttributes.id: debe remitir a edge.externalId para un hecho importado y seguir siendo un UUIDv7");
  const idRe = re(x.pattern ?? "(?!)");
  for (const v of x.valid ?? []) if (!idRe.test(v)) f.push(`edge.externalId.valid: el patrón rechaza ${JSON.stringify(v)}`);
  for (const v of x.invalid ?? []) if (idRe.test(v)) f.push(`edge.externalId.invalid: el patrón acepta ${JSON.stringify(v)}`);
  if ((x.invalid ?? []).length < 4) f.push("edge.externalId.invalid: faltan ejemplos inválidos (versión 4, variante equivocada, mayúsculas)");
  if ((x.vectors ?? []).length < 6) f.push("edge.externalId.vectors: faltan vectores");
  for (const v of x.vectors ?? []) {
    const calculado = idDeterminista(v.source, v.externalKey, Date.parse(v.time));
    if (calculado !== v.id) f.push(`edge.externalId.vectors ${JSON.stringify(v.externalKey)}: id recalculado ${calculado} ≠ ${v.id}`);
    if (!idRe.test(v.id)) f.push(`edge.externalId.vectors ${v.id}: no es un UUIDv7 válido según su propio patrón`);
  }
  const [v0, v1, v2, v3] = x.vectors ?? [];
  if (v0 && v1 && v2 && v3) {
    if (new Set([v0.id, v1.id, v2.id, v3.id]).size !== 4) f.push("edge.externalId.vectors: los cuatro primeros deben tener ids distintos (clave, source y milisegundo)");
    if (v0.id.slice(14) !== v3.id.slice(14) || v0.id.slice(0, 14) === v3.id.slice(0, 14)) f.push("edge.externalId.vectors: un milisegundo más cambia SOLO los 48 bits de tiempo y no los 74 aleatorios");
  }
  if (!(x.vectors ?? []).some((v) => /[^\x00-\x7f]/.test(v.externalKey))) f.push("edge.externalId.vectors: debe haber una clave con no-ASCII (el hash es sobre UTF-8, no sobre UTF-16)");
  if (!(x.vectors ?? []).some((v) => Date.parse(v.time) === 0)) f.push("edge.externalId.vectors: debe haber el instante 0 (los 48 bits de tiempo van en los primeros 6 bytes)");

  // ── El manifiesto: `via` ─────────────────────────────────────────────────────────────────────────────────────
  const m = e.manifest ?? {};
  if (m.key !== "via" || !eq(m.values, [...VIA_VALUES]) || m.unknownValueRejected !== true) f.push(`edge.manifest: la clave es \`via\` y sus valores los de manifest.mjs (${JSON.stringify(VIA_VALUES)}); uno desconocido se rechaza`);
  if (m.annotationOnly !== true || m.extraPermissions !== "none" || m.gatewayNeedsSpecialPermissions !== false || m.serviceHasItsOwnIdentity !== true) f.push("edge.manifest: `via` es una anotación: sin permisos extra, el gateway no necesita ninguno especial y cada cliente tiene su propia identidad");
  if (m.servesRejected !== true) f.push("edge.manifest.servesRejected debe ser true: un cliente de borde no sostiene la suscripción viva que exige `serves`");
  if (!(doc.security?.manifest?.serviceKeys ?? []).includes("via") || doc.security?.manifest?.via?.servesRejected !== true) f.push("security.manifest: serviceKeys incluye `via` y la clave documentada rechaza `serves`");
  // …y el analizador y el generador lo cumplen. Aquí es donde un `via` que concede permisos de más se vería.
  {
    const ok = analizar(manifiestoDeEdge(clienteDeBorde({ via: "gateway" })), BUNDLE_DE_EDGE);
    if (ok.problemas.length) f.push(`edge.manifest: un cliente de borde válido da problemas en el analizador: ${ok.problemas[0].m}`);
    const borde = ok.servicios.find((s) => s.name === "tienda-web");
    const nativo = analizar(manifiestoDeEdge(clienteDeBorde()), BUNDLE_DE_EDGE).servicios.find((s) => s.name === "tienda-web");
    if (!borde || !nativo) f.push("edge.manifest: no se pudo analizar el cliente de borde de comprobación");
    else {
      const a = emitir(borde), b = emitir(nativo);
      if (!eq(a.publish, b.publish) || !eq(a.subscribe, b.subscribe) || !eq(a.allowResponses, b.allowResponses) || !eq(a.durables, b.durables)) f.push("edge.manifest: un cliente de borde (`via: gateway`) debe tener EXACTAMENTE los permisos del mismo servicio sin `via`: ni uno más, ni uno menos");
      if (a.publish.some((p) => /^(\$JS\.API\.>|>|\*\.>)$/.test(p)) || a.subscribe.some((p) => /^(>|\*\.>)$/.test(p))) f.push("edge.manifest: el cliente de borde recibe un permiso ancho");
    }
    const raro = analizar(manifiestoDeEdge(clienteDeBorde({ via: "proxy" })), BUNDLE_DE_EDGE);
    if (!raro.problemas.some((p) => /via/.test(p.m))) f.push("edge.manifest: un `via` desconocido debe ser un error del manifiesto: no puede degradar a «nativo» en silencio");
    const sirve = analizar(manifiestoDeEdge({ name: "crm-web", via: "gateway", owns: ["clientes"], serves: ["clientes.cliente.v1.obtener"] }), BUNDLE_DE_EDGE);
    if (!sirve.problemas.some((p) => /serves/.test(p.m) && /borde/.test(p.m))) f.push("edge.manifest: un cliente de borde con `serves` debe ser un error del manifiesto");
  }

  // ── El gateway de referencia (implementación) ────────────────────────────────────────────────────────────────
  const g = e.gateway ?? {};
  if (g.framework !== "none" || g.httpServer !== "node:http" || g.package !== "gateway/") f.push("edge.gateway: sin framework web, node:http, en gateway/");
  const rutas = (g.routes ?? []).map((r) => `${r.method} ${r.path}`).sort();
  const esperadas = ["DELETE /v1/state/{contract}/{key}", "GET /healthz", "GET /v1/state/{contract}/{key}", "POST /v1/events/{subject}", "POST /v1/hooks/{name}", "POST /v1/rpc/{operation}", "PUT /v1/state/{contract}/{key}"];
  if (!eq(rutas, esperadas)) f.push(`edge.gateway.routes debe ser exactamente ${JSON.stringify(esperadas)}; es ${JSON.stringify(rutas)}`);
  if ((g.routes ?? []).find((r) => r.path === "/v1/events/{subject}")?.response !== 202 || (g.routes ?? []).find((r) => r.path === "/v1/hooks/{name}")?.response !== 202) f.push("edge.gateway.routes: publicar un evento y recibir un hook responden 202");
  if ((g.routes ?? []).find((r) => r.path === "/v1/hooks/{name}")?.auth !== "hmac" || (g.routes ?? []).find((r) => r.path === "/healthz")?.auth !== "none") f.push("edge.gateway.routes: los hooks se autentican por HMAC, y /healthz no necesita autenticación");
  if (g.envelopeNeverWrittenByHand !== true || g.bodyIsDataObject !== true) f.push("edge.gateway: el cuerpo es `data` (un objeto) y el gateway nunca escribe el sobre (AGENTS.md regla 2)");
  if (g.idempotencyKeyRequiresEventTime !== true) f.push("edge.gateway.idempotencyKeyRequiresEventTime debe ser true: sin el instante del hecho el id determinista no es estable");
  const au = g.auth ?? {};
  if (au.scheme !== "Bearer" || au.storedAs !== "sha256" || au.plaintextTokenStored !== false) f.push("edge.gateway.auth: Bearer, y solo se guarda el SHA-256 del token");
  if (au.compare !== "timingSafeEqual-of-sha256-digests" || au.compareLoopsAllClients !== true) f.push("edge.gateway.auth.compare: timingSafeEqual de los digests SHA-256 (mismas longitudes), recorriendo todos los clientes");
  if (au.sameResponseForUnknownAndMissing !== true || au.tokenBytes !== 32) f.push("edge.gateway.auth: el mismo 401 para un token ausente y uno desconocido; tokens de 32 bytes aleatorios");
  if (au.hooksAuthenticatedBy !== "hmac-not-bearer" || au.hookWithoutVerifyRefusesToStart !== true || !eq(au.hookVerifySchemes, ["hmac-sha256"])) f.push("edge.gateway.auth: un hook se autentica por HMAC y sin `verify` no arranca (un endpoint público que publica eventos es una puerta abierta)");
  if (au.hookToleranceSecondsDefault !== sg.receiver?.toleranceSeconds) f.push("edge.gateway.auth.hookToleranceSecondsDefault debe ser el de la entrega saliente (300 s)");
  const hk = g.hooks ?? {};
  if (hk.keyNamespace !== "<hook>:<providerKey>") f.push("edge.gateway.hooks.keyNamespace debe ser <hook>:<providerKey>: dos proveedores pueden numerar sus eventos igual");
  if (hk.defaultClassification !== "confidential" || hk.dataSelection !== "pick-or-whole-body") f.push("edge.gateway.hooks: la clasificación por defecto es confidential y el propietario elige qué campos pasan");
  if (hk.duplicateReturnsSameId !== true || hk.duplicateStatus !== 202 || hk.publishesOnlyInOwnDomain !== true) f.push("edge.gateway.hooks: el mismo webhook devuelve 202 con el mismo id, y solo publica en el dominio del cliente");
  if (hk.idempotentFalseRequiresExplicit !== true) f.push("edge.gateway.hooks.idempotentFalseRequiresExplicit debe ser true: un hook sin id determinista es una decisión explícita");
  if (!eq(hk.timeFrom, ["body", "header"]) || !eq(hk.timeUnits, ["seconds", "milliseconds", "iso"])) f.push("edge.gateway.hooks: el instante del hecho se lee del cuerpo o de una cabecera, en segundos, milisegundos o ISO");
  const lm = g.limits ?? {};
  if (lm.maxBodyBytes !== doc.data?.maxMessageBytes || lm.maxBodyBytesIsEventLimit !== true) f.push("edge.gateway.limits.maxBodyBytes debe ser data.maxMessageBytes (1 MiB)");
  if (lm.rejectsBeforeReading !== true || lm.closesConnectionOnOversize !== true || lm.chunkedStopsAtLimit !== true) f.push("edge.gateway.limits: un cuerpo demasiado grande se rechaza antes de leerlo y se cierra la conexión (A-152: sin cerrarla, Node lee y descarta todo lo declarado)");
  if (lm.rateLimitPer !== "token" || lm.hookRateLimitPer !== "hook-name" || lm.rateLimitIsPerInstance !== true || lm.rateLimitAlgorithm !== "token-bucket") f.push("edge.gateway.limits: límite por token (y por nombre de hook), cubo de tokens, por instancia");
  if (!(lm.serverTimeoutMs > (doc.rpc?.timeout?.maxMs ?? Infinity)) || lm.serverTimeoutAboveRpcMaxMs !== true) f.push("edge.gateway.limits.serverTimeoutMs debe superar el timeout máximo del rpc: mide inactividad del socket mientras el gateway espera al broker (A-153)");
  if (!(lm.connectionsCheckingIntervalMs <= 5000) || !(lm.connectionsCheckingIntervalMs < lm.headersTimeoutMs) || !(lm.headersTimeoutMs < lm.requestTimeoutMs) || !(lm.requestTimeoutMs <= lm.serverTimeoutMs)) f.push("edge.gateway.limits: connectionsCheckingIntervalMs ≤ 5000 y menor que headersTimeoutMs < requestTimeoutMs ≤ serverTimeoutMs (con el intervalo por defecto de Node, 30 s, los otros límites no se aplican: A-153)");
  if (!eq(g.tls?.modes, ["terminates-in-front", "in-gateway"]) || g.tls?.atLeastOneRequiredInProduction !== true || g.tls?.natsTlsSeparate !== true) f.push("edge.gateway.tls: dos modos (termina delante o en el propio gateway), uno obligatorio en producción, y el TLS hacia NATS es aparte");
  const lg = g.logging ?? {};
  for (const n of ["data", "authorization", "token", "secrets", "response-body"]) if (!(lg.neverLogs ?? []).includes(n)) f.push(`edge.gateway.logging.neverLogs debe incluir «${n}»`);
  if (lg.logsDataOfConfidential !== false || lg.logsDataOfAnyClassification !== false) f.push("edge.gateway.logging: no se registra `data` de ninguna clasificación (en particular, nunca el de un confidential)");
  const cf = g.config ?? {};
  if (cf.holdsSecrets !== false || cf.secretsByEnvVarName !== true || cf.tokensAreSha256Hex !== true) f.push("edge.gateway.config: gateway.json no contiene secretos: nombres de variables de entorno y SHA-256 de tokens");
  for (const k of ["nkeySeed", "token", "secret", "password", "pass"]) if (!(cf.refusesPlaintextSecretKeys ?? []).includes(k)) f.push(`edge.gateway.config.refusesPlaintextSecretKeys debe incluir «${k}»`);
  if (cf.refusesNatsSeedShapedValues !== true || cf.missingEnvVarNamedInError !== true || cf.onePerSubjectPerClient !== true || cf.hookNamesUniqueAcrossClients !== true) f.push("edge.gateway.config: rechaza semillas de NATS en claro, nombra la variable que falta, una entrega por subject y cliente, y nombres de hook únicos");
  if (!eq(g.shutdown?.order, ["stop-accepting", "answer-in-flight-with-connection-close", "close-idle", "drain-nats"]) || g.shutdown?.hardDeadline !== true) f.push("edge.gateway.shutdown: dejar de aceptar, contestar lo que vuela con Connection: close, cerrar las ociosas y drenar NATS, con un plazo máximo (A-162)");

  // Los errores HTTP: la tabla de la spec, contra errors.codes y la regla de los estados.
  const er = g.errors ?? {};
  if (!eq(er.classes, ["retryable", "permanent"]) || er.messageNeverQuotesData !== true || er.retryAfterHeaderOnRetryable !== true) f.push("edge.gateway.errors: clase retryable o permanent, sin citar data, con Retry-After en lo reintentable");
  const mapa = er.protocolCodes ?? {};
  const noSale = er.notSurfaced ?? {};
  for (const c of Object.keys(cs).filter((k) => !k.startsWith("$"))) {
    const enMapa = c in mapa, enNo = c in noSale;
    if (!enMapa && !enNo) f.push(`edge.gateway.errors: el código «${c}» de errors.codes no está ni en protocolCodes ni en notSurfaced: un código nuevo no puede quedar sin estado HTTP`);
    if (enMapa && enNo) f.push(`edge.gateway.errors: «${c}» está a la vez en protocolCodes y en notSurfaced`);
    if (enMapa && cs[c].class !== "poison" && mapa[c].class !== cs[c].class) f.push(`edge.gateway.errors.protocolCodes.${c}.class (${mapa[c].class}) ≠ errors.codes.${c}.class (${cs[c].class})`);
  }
  for (const c of Object.keys(mapa)) if (!(c in cs) && mapa[c].definedIn !== "reused") f.push(`edge.gateway.errors.protocolCodes.${c}: no está en errors.codes y no se marca como reutilizado (definedIn: "reused")`);
  for (const [c, v] of [...Object.entries(mapa), ...Object.entries(er.codes ?? {})]) {
    if (!Number.isInteger(v.status) || v.status < 400 || v.status > 599) f.push(`edge.gateway.errors: «${c}» tiene un estado HTTP que no es de error (${v.status})`);
    if (!["retryable", "permanent"].includes(v.class)) f.push(`edge.gateway.errors: «${c}» tiene una clase que no es retryable ni permanent`);
    // La regla de los estados: 4xx = reintentar igual da lo mismo (salvo 408, 409, 429); 5xx = lo dice `class`.
    if (v.class === "retryable" && v.status >= 400 && v.status <= 499 && ![408, 409, 429].includes(v.status)) f.push(`edge.gateway.errors: «${c}» es retryable con un ${v.status}: un 4xx retryable solo puede ser 408, 409 o 429`);
    if (v.class === "permanent" && v.status >= 502 && v.status <= 599) f.push(`edge.gateway.errors: «${c}» es permanent con un ${v.status}: un 502, 503 o 504 invita a reintentar`);
    if (!re(doc.errors?.codePattern ?? "(?!)").test(c)) f.push(`edge.gateway.errors: «${c}» no cumple errors.codePattern`);
  }
  for (const c of Object.keys(er.codes ?? {})) {
    if (!c.startsWith("EDGE_")) f.push(`edge.gateway.errors.codes.${c}: los códigos solo del borde llevan el prefijo EDGE_ para no confundirse con los del protocolo`);
    if (c in cs || c in mapa) f.push(`edge.gateway.errors.codes.${c}: choca con un código del protocolo`);
  }
  if (er.codesAreNotProtocolCodes !== true) f.push("edge.gateway.errors.codesAreNotProtocolCodes debe ser true: no se añaden a errors.codes (listas cerradas)");
  if (mapa.RPC_TIMEOUT?.status !== 504 || mapa.RPC_TIMEOUT?.ambiguous !== true) f.push("edge.gateway.errors.protocolCodes.RPC_TIMEOUT debe ser 504 y ambiguo: la operación pudo ejecutarse");
  if (mapa.RPC_UNAVAILABLE?.status !== 503 || mapa.RPC_UNAVAILABLE?.retryAfterSeconds !== 1) f.push("edge.gateway.errors.protocolCodes.RPC_UNAVAILABLE debe ser 503 con Retry-After de 1 s");
  if (mapa.PERMISSION_DENIED?.status !== 403 || mapa.PERMISSION_DENIED?.deniedByBroker !== true) f.push("edge.gateway.errors.protocolCodes.PERMISSION_DENIED debe ser 403, y lo deniega el BROKER (no solo el gateway)");
  if (mapa.NOT_PROVISIONED?.status !== 500 || mapa.NOT_PROVISIONED?.class !== "permanent") f.push("edge.gateway.errors.protocolCodes.NOT_PROVISIONED: un fallo del ecosistema que arregla quien opera es 500 permanent");
  if (mapa.STATE_CONFLICT?.status !== 409) f.push("edge.gateway.errors.protocolCodes.STATE_CONFLICT debe ser 409");
  if (er.remoteErrors?.retryable !== 503 || er.remoteErrors?.permanent !== 422 || er.remoteErrors?.keepsTheirCode !== true) f.push("edge.gateway.errors.remoteErrors: un error del que responde es 503 (retryable) o 422 (permanent) y conserva su code");
  if (er.codes?.EDGE_UNAUTHENTICATED?.status !== 401 || er.codes?.EDGE_RATE_LIMITED?.status !== 429 || er.codes?.EDGE_BODY_TOO_LARGE?.status !== 413) f.push("edge.gateway.errors.codes: sin token es 401, el límite por token es 429 y un cuerpo grande es 413");
  if (er.codes?.EDGE_INVALID_WEBHOOK_SIGNATURE?.status !== 401 || er.codes?.EDGE_WEBHOOK_TIMESTAMP_OUT_OF_TOLERANCE?.status !== 401 || er.codes?.EDGE_INVALID_WEBHOOK_SIGNATURE?.class !== "permanent") f.push("edge.gateway.errors.codes: una firma de webhook inválida o fuera de tolerancia es 401 y permanent (códigos distintos para no confundir un reloj con un secreto)");

  // PII
  if (!doc.security?.piiInPayload?.edge || doc.security.piiInPayload.edge === "forbidden") f.push("security.piiInPayload.edge: el cuerpo de un webhook de un tercero no está libre de PII por construcción");
  return f;
}


// ─── La ACL de PETICIÓN/RESPUESTA y su generador — 06-security.md §3.9 ───────────────────────────
// Como la de estado: una función PURA sobre el documento y sobre un EMISOR de permisos (por defecto, el generador real).
// Sobre el protocol.json y el generador reales da cero fallos; sobre copias estropeadas a propósito —un documento con un
// allow_responses sin acotar, un generador que concede `rpc.<dominio>.>` a quien llama o `rpc.>` al dueño— cada una da
// al menos uno. Es lo que hace que `rpc.acl` sea lo que el generador PRODUCE y no una promesa.

/** Los servicios de prueba: el dueño de dos operaciones, uno que solo llama a una, uno que solo llama (dueño de nada), uno
 * mixto con dos dominios servidos, y uno sin peticiones. */
const SERVICIOS_DE_RPC = [
  { name: "crm-api", owns: ["clientes"], subjects: [], stateOwns: [], watches: [], calls: [], serves: ["clientes.cliente.v1.calcular-descuento", "clientes.cliente.v1.obtener"] },
  { name: "pedidos-api", owns: ["pedidos"], subjects: [], stateOwns: [], watches: [], calls: ["clientes.cliente.v1.calcular-descuento"], serves: [] },
  { name: "web-api", owns: [], subjects: [], stateOwns: [], watches: [], calls: ["clientes.cliente.v1.obtener", "logistica.linea-envio.v2.reservar-stock"], serves: [] },
  { name: "mixto-api", owns: ["logistica", "envios"], subjects: [], stateOwns: [], watches: [], calls: ["clientes.cliente.v1.obtener"], serves: ["envios.envio.v1.recoger", "logistica.linea-envio.v2.reservar-stock"] },
  { name: "sin-rpc-api", owns: ["inventario"], subjects: [], stateOwns: [], watches: [], calls: [], serves: [] },
];

/** @returns {string[]} */
function fallosDeAclDeRpc(doc, emitir = permisosDe) {
  const f = [];
  const eq = (a, b) => JSON.stringify(a) === JSON.stringify(b);
  const re = (src) => { try { return new RegExp(src); } catch { return /(?!)/; } };
  const ac = doc.rpc?.acl ?? {};
  const m = ac.manifest ?? {};
  const ar = ac.ownerAllowResponses ?? {};

  // ── El documento ──────────────────────────────────────────────────────────────────────────────
  if (!eq(m.keys, ["calls", "serves"])) f.push(`rpc.acl.manifest.keys debe ser [calls, serves]; es ${JSON.stringify(m.keys)}`);
  for (const k of m.keys ?? []) if (!(doc.security?.manifest?.serviceKeys ?? []).includes(k)) f.push(`security.manifest.serviceKeys no incluye «${k}»: el generador rechazaría una clave de rpc como una errata`);
  if (m.entryPattern !== "rpc.operation.pattern") f.push("rpc.acl.manifest.entryPattern debe referir rpc.operation.pattern (una sola fuente)");
  if (OPERATION_RE.source !== doc.rpc?.operation?.pattern) f.push(`manifest.mjs OPERATION_RE (${OPERATION_RE.source}) != rpc.operation.pattern (${doc.rpc?.operation?.pattern})`);
  for (const k of ["reservedDomainsRejected", "wildcardsRejected", "duplicatesRejected", "nonListRejected", "operationMustExistInBundleRpcIndex", "tenantsModelBSamePermissions"]) {
    if (m[k] !== true) f.push(`rpc.acl.manifest.${k} debe ser true`);
  }
  if (m.entryWithRpcPrefix !== "RESERVED_DOMAIN") f.push("rpc.acl.manifest.entryWithRpcPrefix debe ser RESERVED_DOMAIN: el prefijo `rpc.` lo pone el SDK y `rpc` es un dominio reservado");
  if (!eq(m.calls?.grants, ["callerPublish"]) || m.calls?.domainMustHaveOwner !== true || m.calls?.ownDomainRejected !== true || m.calls?.operationMustBeServed !== true) {
    f.push("rpc.acl.manifest.calls: concede solo callerPublish; el dominio debe tener dueño, no puede ser el propio y la operación debe estar atendida");
  }
  if (!eq(m.serves?.grants, ["ownerSubscribe", "ownerAllowResponses"]) || m.serves?.onlyOwnDomain !== true || m.serves?.subscribePerDomain !== true || m.serves?.noPublishOnInboxes !== true) {
    f.push("rpc.acl.manifest.serves: concede ownerSubscribe y ownerAllowResponses, solo del dominio propio, una suscripción por dominio y ninguna publicación sobre inboxes");
  }
  const esperadoAr = { max: ar.maxReplies, expires: `${ar.recommendedExpiresSeconds}s` };
  if (!eq(m.generatedAllowResponses, esperadoAr)) f.push(`rpc.acl.manifest.generatedAllowResponses debe ser ${JSON.stringify(esperadoAr)} (ownerAllowResponses); es ${JSON.stringify(m.generatedAllowResponses)}`);
  if (!eq(RPC_ALLOW_RESPONSES, esperadoAr)) f.push(`manifest.mjs RPC_ALLOW_RESPONSES ${JSON.stringify(RPC_ALLOW_RESPONSES)} != rpc.acl.ownerAllowResponses ${JSON.stringify(esperadoAr)}`);

  // ── El generador ──────────────────────────────────────────────────────────────────────────────
  const pub = re(ac.callerPublish ?? "(?!)");
  const sub = re(ac.ownerSubscribe ?? "(?!)");
  const inboxRe = re(doc.security.acl.inbox.permissionPattern);
  const patronesDeOtrasCategorias = [
    ...Object.entries(doc.security.acl.permissionPatterns).filter(([k]) => !k.startsWith("$")).map(([k, v]) => [`security.acl.permissionPatterns.${k}`, re(v)]),
    ...Object.entries(doc.state?.acl ?? {}).filter(([k, v]) => typeof v === "string" && !k.startsWith("$") && k !== "observerStreamInfoState").map(([k, v]) => [`state.acl.${k}`, re(v)]),
  ];
  for (const dev of [false, true]) {
    const modo = dev ? "--dev" : "por defecto";
    for (const svc of SERVICIOS_DE_RPC) {
      const e = dev ? emitir(svc, { dev }) : emitir(svc);
      const donde = `${svc.name} (${modo})`;
      const inbox = `_INBOX.${svc.name}.>`;

      // QUIEN LLAMA: un subject exacto por operación llamada, y nada más de rpc.
      const llama = e.publish.filter((x) => /^rpc\b/.test(x));
      const esperadas = svc.calls.map((op) => `rpc.${op}`);
      if (!eq([...llama].sort(), [...esperadas].sort())) f.push(`${donde} concede para llamar ${JSON.stringify(llama)}; debería ser el subject EXACTO de cada operación llamada ${JSON.stringify(esperadas)}`);
      for (const x of llama) {
        if (!pub.test(x)) f.push(`${donde} emite «${x}», que no es el subject exacto de una operación (rpc.acl.callerPublish)`);
        if (/[*>]/.test(x)) f.push(`${donde} emite «${x}», con un comodín: quien llama podría llamar a TODAS las operaciones`);
        if ((ac.callerNeverPublish ?? []).includes(x)) f.push(`${donde} emite «${x}», que rpc.acl.callerNeverPublish prohíbe`);
        const otras = patronesDeOtrasCategorias.filter(([, r]) => r.test(x)).map(([k]) => k);
        if (otras.length) f.push(`${donde}: el permiso de rpc «${x}» encaja con una categoría que no es de rpc: [${otras.join(", ")}]`);
      }
      // …y ningún permiso de publicación sobre inboxes, ni quien llama ni el dueño (el dueño responde con allow_responses: A-68).
      if (e.publish.some((x) => /^_INBOX\b/.test(x))) f.push(`${donde} concede publicar sobre un inbox (${e.publish.filter((x) => /^_INBOX\b/.test(x)).join(", ")}): el dueño responde con allow_responses, no con un permiso (A-68)`);
      // Lo que ningún servicio recibe nunca.
      if (e.publish.some((x) => x === "rpc.>" || /^rpc\.[^.]+\.>$/.test(x) || /^rpc\.\*/.test(x))) f.push(`${donde} concede un comodín de rpc para publicar: quien llama podría llamar a todo`);

      // SUSCRIPCIÓN: el inbox privado y, solo para quien atiende, `rpc.<dominio>.>` de cada dominio servido.
      const dominios = [...new Set(svc.serves.map((op) => op.split(".")[0]))].sort();
      const esperadaSub = [inbox, ...dominios.map((d) => `rpc.${d}.>`)];
      if (!eq(e.subscribe, esperadaSub)) f.push(`${donde} se suscribe a ${JSON.stringify(e.subscribe)}; debería ser su inbox privado y, solo si atiende operaciones, rpc.<dominio>.> de cada dominio servido: ${JSON.stringify(esperadaSub)}`);
      for (const x of e.subscribe) {
        if (x === inbox) continue;
        if (!sub.test(x)) f.push(`${donde} se suscribe a «${x}», que no es rpc.<dominio>.> (rpc.acl.ownerSubscribe)`);
        if ((ac.ownerNeverSubscribe ?? []).includes(x)) f.push(`${donde} se suscribe a «${x}», que rpc.acl.ownerNeverSubscribe prohíbe`);
      }
      if (e.subscribe.some((x) => x !== inbox && x.startsWith("_INBOX"))) f.push(`${donde} se suscribe a un inbox que no es el suyo: ${JSON.stringify(e.subscribe)}`);
      if (e.subscribe.some((x) => x !== inbox && !inboxRe.test(x) && !/^rpc\./.test(x))) f.push(`${donde} se suscribe a «${e.subscribe.find((x) => x !== inbox && !/^rpc\./.test(x))}», que no es el inbox ni un subject de rpc`);

      // ALLOW_RESPONSES: una respuesta por petición y que caduque, solo para quien atiende.
      const real = e.allowResponses ?? null;
      const debeTener = svc.serves.length ? esperadoAr : null;
      if (!eq(real, debeTener)) f.push(`${donde} lleva allow_responses ${JSON.stringify(real)}; debería ser ${JSON.stringify(debeTener)} (${svc.serves.length ? "atiende operaciones: una respuesta por petición y que caduque" : "no atiende ninguna: un servicio que solo llama no responde a nadie"})`);
    }

    // Lo que el generador NO emite: se niega a generar permisos de un manifiesto que rompe el modelo.
    const rechazos = {
      "una llamada con el prefijo `rpc.` (dominio reservado)": { name: "x-api", owns: [], subjects: [], stateOwns: [], watches: [], calls: ["rpc.clientes.cliente.v1.obtener"], serves: [] },
      "una llamada con comodín de token": { name: "x-api", owns: [], subjects: [], stateOwns: [], watches: [], calls: ["clientes.cliente.v1.*"], serves: [] },
      "una llamada con `>` en el dominio": { name: "x-api", owns: [], subjects: [], stateOwns: [], watches: [], calls: ["clientes.>"], serves: [] },
      "una llamada a un dominio de estado": { name: "x-api", owns: [], subjects: [], stateOwns: [], watches: [], calls: ["state.clientes.cliente.v1"], serves: [] },
      "atender una operación de un dominio que no es suyo": { name: "x-api", owns: ["pedidos"], subjects: [], stateOwns: [], watches: [], calls: [], serves: ["clientes.cliente.v1.obtener"] },
      "atender una operación sin ser dueño de nada": { name: "x-api", owns: [], subjects: [], stateOwns: [], watches: [], calls: [], serves: ["clientes.cliente.v1.obtener"] },
      "atender con comodín": { name: "x-api", owns: ["clientes"], subjects: [], stateOwns: [], watches: [], calls: [], serves: ["clientes.cliente.v1.*"] },
    };
    for (const [que, svc] of Object.entries(rechazos)) {
      let lanzo = false;
      try {
        if (dev) emitir(svc, { dev }); else emitir(svc);
      } catch {
        lanzo = true;
      }
      if (!lanzo) f.push(`el generador (${modo}) emite permisos para ${que}`);
    }
  }
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

// ── La versión mayor son de 1 a 4 dígitos (02-naming.md §1): `v1`…`v9999` ──────────────────────────────────────────────
// Sin tope, Node escribía `v100000000000000000000` (pasaba por `Number`) y Rust rechazaba lo que no cabía en un `u32`: el mismo
// subject, dos veredictos. El tope vive en CADA patrón donde aparece la versión (el subject, el `type`, el estado, la operación, la
// ACL…), así que se comprueba en todos —no solo en el de los eventos— con una muestra por cada uno que se construye con la versión
// que se pide, y el patrón que la acepte sin tope tiene que fallar aquí.
{
  const mayor = p.naming?.subject?.majorVersion;
  if (mayor?.minDigits !== 1 || mayor?.maxDigits !== 4 || mayor?.max !== 9999) {
    fallos.push("naming.subject.majorVersion debe ser { minDigits: 1, maxDigits: 4, max: 9999 }: la versión mayor es `v1`…`v9999` (02-naming.md §1)");
  }
  const MUESTRAS = {
    "naming.subject.pattern": (v) => `pedidos.pedido.${v}.creado`,
    "naming.type.pattern": (v) => `com.flux.pedidos.pedido.creado.${v}`,
    "naming.durableConsumer.pattern": () => null, // no lleva la versión como token propio
    "state.subject.pattern": (v) => `state.clientes.cliente.${v}.acme.cli-1`,
    "state.contract.pattern": (v) => `clientes.cliente.${v}`,
    "state.dlq.subject.pattern": (v) => `dlq.state.clientes.cliente.${v}`,
    "state.type.value.pattern": (v) => `com.flux.state.clientes.cliente.${v}`,
    "state.type.tombstone.pattern": (v) => `com.flux.state.clientes.cliente.deleted.${v}`,
    "rpc.subject.pattern": (v) => `rpc.clientes.cliente.${v}.obtener`,
    "rpc.operation.pattern": (v) => `clientes.cliente.${v}.obtener`,
    "rpc.type.request.pattern": (v) => `com.flux.rpc.clientes.cliente.obtener.request.${v}`,
    "rpc.type.reply.pattern": (v) => `com.flux.rpc.clientes.cliente.obtener.reply.${v}`,
    "rpc.acl.callerPublish": (v) => `rpc.clientes.cliente.${v}.obtener`,
  };
  for (const [ruta, muestra] of Object.entries(MUESTRAS)) {
    if (muestra("v1") === null) continue;
    const src = get(ruta);
    if (typeof src !== "string") { fallos.push(`${ruta}: no existe o no es una cadena (versión mayor)`); continue; }
    const re = new RegExp(src);
    for (const v of ["v1", "v9", "v10", "v9999"]) {
      if (!re.test(muestra(v))) fallos.push(`${ruta}: DEBERÍA aceptar la versión mayor ${v} (${JSON.stringify(muestra(v))}): son de 1 a 4 dígitos`);
    }
    for (const v of ["v10000", "v99999", "v4294967296", "v99999999999999999999", "v0", "v01", "v"]) {
      if (re.test(muestra(v))) fallos.push(`${ruta}: DEBERÍA rechazar la versión mayor ${v} (${JSON.stringify(muestra(v))}): el tope es 9999 y no hay ceros a la izquierda`);
    }
    // Autocomprobación: el mismo patrón SIN tope tiene que fallar esta comprobación (si no, no detectaría nada).
    if (src.includes("[1-9][0-9]{0,3}")) {
      const sinTope = new RegExp(src.replace("[1-9][0-9]{0,3}", "[1-9][0-9]*"));
      if (!sinTope.test(muestra("v10000"))) fallos.push(`autocomprobación: ${ruta} sin tope de versión mayor NO aceptaría v10000: la comprobación de arriba no detectaría la mutación`);
    } else {
      fallos.push(`${ruta}: no lleva el tope de la versión mayor (\`[1-9][0-9]{0,3}\`)`);
    }
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
fallos.push(...fallosDeEstado(p));
fallos.push(...fallosDeAclDeEstado(p));
fallos.push(...fallosDeRpc(p));
fallos.push(...fallosDeAclDeRpc(p));
fallos.push(...fallosDeEdge(p));
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

// El manifiesto y protocol.json dicen lo mismo: los dominios reservados, las claves de un servicio y los
// patrones de contrato y de tenant que el generador aplica a `watches` son los del documento.
{
  const eqj = (a, b) => JSON.stringify(a) === JSON.stringify(b);
  if (!eqj(RESERVED_DOMAINS, p.naming.reservedDomains.names)) fallos.push(`manifest.mjs RESERVED_DOMAINS ${JSON.stringify(RESERVED_DOMAINS)} != naming.reservedDomains.names ${JSON.stringify(p.naming.reservedDomains.names)}`);
  if (!eqj(SERVICE_KEYS, p.security.manifest.serviceKeys)) fallos.push(`manifest.mjs SERVICE_KEYS ${JSON.stringify(SERVICE_KEYS)} != security.manifest.serviceKeys ${JSON.stringify(p.security.manifest.serviceKeys)}`);
  if (STATE_TENANT_MAX !== p.state.tenant.maxLength || STATE_TENANT_MAX !== p.security.manifest.watches.tenantMaxLength) fallos.push("manifest.mjs STATE_TENANT_MAX != state.tenant.maxLength / security.manifest.watches.tenantMaxLength");
  for (const [nombre, regex, def] of [["state.contract", STATE_CONTRACT_RE, p.state.contract], ["state.tenant", STATE_TENANT_RE, p.state.tenant]]) {
    if (regex.source !== def.pattern) fallos.push(`manifest.mjs: el patrón de ${nombre} (${regex.source}) no es el de protocol.json (${def.pattern})`);
    for (const v of def.valid ?? []) if (!regex.test(v)) fallos.push(`manifest.mjs: el patrón de ${nombre} debería aceptar ${JSON.stringify(v)}`);
    for (const v of def.invalid ?? []) if (regex.test(v)) fallos.push(`manifest.mjs: el patrón de ${nombre} debería rechazar ${JSON.stringify(v)}`);
  }
  const w = p.security.manifest.watches;
  if (w.contractPattern !== "state.contract.pattern" || w.tenantPattern !== "state.tenant.pattern" || w.durable !== "state.watch.durable.pattern") fallos.push("security.manifest.watches debe referir los patrones de state.* (una sola fuente)");
  if (w.emptyTenantsListRejected !== true || w.unknownEntryKeysRejected !== true || w.ownDomainRejected !== true || w.stateInConsumesRejected !== true) fallos.push("security.manifest.watches: la lista de tenants vacía, las claves desconocidas, observar el propio dominio y el estado en `consumes` se rechazan");
  if (w.confinementEnforcedBy !== "broker" || w.durablePerTenant !== true) fallos.push("security.manifest.watches: el confinamiento por tenant lo aplica el broker, con un durable por tenant");
  const so = p.security.manifest.stateOwnership;
  if (so.ownerStreamInfoState !== "never" || so.observerStreamInfoState !== "never") fallos.push('security.manifest.stateOwnership: STREAM.INFO.STATE_<DOM> no se concede a ningún rol ("never")');
  if (p.security.provisioning.state?.widensExistingDlqStream !== true || p.security.provisioning.state?.stateDurableDeliverPolicy !== "last_per_subject") fallos.push("security.provisioning.state: provision amplía el DLQ_<DOM> existente y crea el durable de estado con last_per_subject");
}

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

// Autocomprobación de `fallosDeEstado`: cada copia estropeada DEBE dar un fallo, o la invariante no
// vigila nada. Son las equivocaciones reales que 10-state.md existe para impedir.
let mutacionesEstado = 0;
{
  const mutaciones = {
    "volver a la versión 1.0.0": (d) => { d.version = "1.0.0"; },
    "quitar un código de estado": (d) => { delete d.errors.codes.STATE_CONFLICT; },
    "STATE_SUBJECT_MISMATCH como PERMANENT": (d) => { d.errors.codes.STATE_SUBJECT_MISMATCH.class = "permanent"; },
    "STATE_CONFLICT como PERMANENT": (d) => { d.errors.codes.STATE_CONFLICT.class = "permanent"; },
    "un octavo código de estado": (d) => { d.errors.codes.OTRO_CODIGO_DE_ESTADO = { class: "permanent", capability: "state" }; },
    "quitar INVALID_STATE_CONTRACT": (d) => { delete d.errors.codes.INVALID_STATE_CONTRACT; },
    "INVALID_STATE_CONTRACT como RETRYABLE": (d) => { d.errors.codes.INVALID_STATE_CONTRACT.class = "retryable"; },
    "INVALID_STATE_CONTRACT de otra capacidad": (d) => { d.errors.codes.INVALID_STATE_CONTRACT.capability = "security"; },
    "INVALID_STATE_CONTRACT cuenta en la métrica": (d) => { d.errors.codes.INVALID_STATE_CONTRACT.notCountedInMetrics = false; },
    "INVALID_STATE_CONTRACT con red": (d) => { d.errors.codes.INVALID_STATE_CONTRACT.failsBeforeNetwork = false; },
    "el contrato sin su código": (d) => { delete d.state.contract.errorCode; },
    "la métrica cuenta INVALID_STATE_CONTRACT": (d) => { d.state.metrics.flux_state_writes_total.notCounted = d.state.metrics.flux_state_writes_total.notCounted.filter((c) => c !== "INVALID_STATE_CONTRACT"); },
    "la capacidad pide seis códigos": (d) => { d.capabilities.definitions.state.requires = d.capabilities.definitions.state.requires.map((r) => (r === "seven-state-error-codes" ? "six-state-error-codes" : r)); },
    "watch hereda el tenant del evento en curso": (d) => { d.state.tenant.watchResolutionOrder = ["call-option", "event-in-progress", "connection-tenantId"]; },
    "watch sin prohibir el evento en curso": (d) => { d.state.tenant.watchNeverUsesEventInProgress = false; },
    "put sin el evento en curso": (d) => { d.state.tenant.resolutionOrder = ["call-option", "connection-tenantId"]; },
    "SCHEMA_NOT_FOUND no cuenta como invalid_schema": (d) => { d.state.metrics.flux_state_writes_total.invalidSchemaCodes = ["MISSING_AGGREGATE_VERSION", "INVALID_SCHEMA"]; },
    "7.0 exigido también en Node": (d) => { d.state.aggregateVersion.sevenPointZero.singleNumberLanguages = []; },
    "7.0 aceptado donde se distingue": (d) => { d.state.aggregateVersion.sevenPointZero.rejectedWhenParserDistinguishesIntFromFloat = false; },
    "7.0 en state-semantics.json": (d) => { d.state.aggregateVersion.sevenPointZero.inStateSemanticsJson = true; },
    "la DLQ sintetizada con time fijo": (d) => { d.state.dlq.unreadableSynthesizedEnvelope.time = "from-original"; },
    "la DLQ sintetizada sin partitionkey": (d) => { d.state.dlq.unreadableSynthesizedEnvelope.partitionkey = "absent"; },
    "dlqerror con el mensaje del driver": (d) => { d.state.dlq.dlqerrorOfNonFluxErrors = "driver-message"; },
    "dlqerror concatena la cadena de causas": (d) => { d.state.dlq.dlqerrorNeverConcatenatesCauseChain = false; },
    "dlqerror sin su frase fija": (d) => { d.state.dlq.dlqerrorFixedPhrase = "error"; },
    "onSynced se invoca de nuevo tras un resync": (d) => { d.state.watch.onSynced.afterResyncFiresAgain = true; },
    "un watch vivo se para en silencio al borrarse el durable": (d) => { d.state.watch.durableDeletedWhileWatching.action = "stop"; },
    "se admite parar en silencio": (d) => { d.state.watch.durableDeletedWhileWatching.mustNot = []; },
    "reabrir necesita CONSUMER.CREATE": (d) => { d.state.watch.durableDeletedWhileWatching.needsConsumerCreate = true; },
    "reabrir sin comprobar la configuración": (d) => { d.state.watch.durableDeletedWhileWatching.checksConfigBeforeConsuming = false; },
    "sin lo medido en async-nats": (d) => { delete d.state.watch.durableDeletedWhileWatching.measured.withoutHandling["async-nats 0.50"]; },
    "sin el bloque de reapertura": (d) => { delete d.state.watch.durableDeletedWhileWatching; },
    "onSynced de varios tenants, una por tenant": (d) => { d.state.watch.onSynced.severalTenantsInOneCall = "once-per-tenant"; },
    "una capacidad state sin definir": (d) => { delete d.capabilities.definitions.state; },
    "quitar un dominio reservado": (d) => { d.naming.reservedDomains.names = ["state", "rpc", "system"]; },
    "max_msgs_per_subject de 5": (d) => { d.state.stream.maxMsgsPerSubject = 5; },
    "sin allow_direct": (d) => { d.state.stream.allowDirect = false; },
    "el estado caduca": (d) => { d.state.stream.maxAge = 2592000; },
    "discard new": (d) => { d.state.stream.discard = "new"; },
    "el SDK consulta STREAM.INFO del estado": (d) => { d.state.stream.sdkQueriesStreamInfo = true; },
    "el subject de estado con 5 tokens": (d) => { d.state.subject.format = "state.<dominio>.<agregado>.v<major>.<clave>"; },
    "la clave admite un punto": (d) => { d.state.key.pattern = "^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"; },
    "la clave admite el prefijo reservado": (d) => { d.state.key.pattern = "^[A-Za-z0-9_][A-Za-z0-9_-]{0,127}$"; },
    "el tenant admite mayúsculas": (d) => { d.state.tenant.pattern = "^[A-Za-z0-9]+(-[A-Za-z0-9]+)*$"; },
    "el tenant sin límite": (d) => { d.state.tenant.maxLength = 255; },
    "tenant por defecto": (d) => { d.state.tenant.default = "system"; },
    "el durable de estado acepta el de un evento": (d) => { d.state.watch.durable.pattern = "^[a-z0-9-]+__[a-z0-9_]+$"; },
    "tenantid del sobre sin atarlo al subject": (d) => { d.state.envelope.tenantidEqualsSubjectToken = false; },
    "un atributo raíz nuevo en el sobre": (d) => { d.state.envelope.newRootAttributes = ["tenantkey"]; },
    "restricted admitido en estado": (d) => { d.state.classification.allowed.push("restricted"); },
    "sin restricted prohibido": (d) => { d.state.classification.forbidden = []; },
    "la clasificación por defecto baja a internal": (d) => { d.state.classification.putDefault = "internal"; },
    "aggregateVersion que admite 0": (d) => { d.state.aggregateVersion.minimum = 0; },
    "aggregateVersion solo con validación L3": (d) => { d.state.aggregateVersion.checkedWithoutValidation = false; },
    "la lápida con otro dataschema": (d) => { d.state.tombstone.dataschema = "https://schemas.internal/system/state/deleted/1.0.0.json"; },
    "la lápida sin versión": (d) => { d.state.tombstone.data = []; d.state.tombstone.carriesVersion = false; },
    "recrear sin exigir versión mayor": (d) => { d.state.tombstone.recreateRequires = "anything"; },
    "put sin condición": (d) => { d.state.write.conditional = false; },
    "solo el 10071 como conflicto": (d) => { d.state.write.conflictErrCodes = [10071]; },
    "solo el 10164 como conflicto": (d) => { d.state.write.conflictErrCodes = [10164]; },
    "un id nuevo en cada intento": (d) => { d.state.write.sameIdAndBytesAcrossAttempts = false; },
    "sin reconocer la propia escritura": (d) => { d.state.write.recognizeOwnWriteById = false; },
    "publicar por core": (d) => { d.state.write.publishViaCoreForbidden = false; },
    "5 intentos": (d) => { d.state.write.maxAttempts = 5; },
    "un valor ilegible bloquea la clave": (d) => { d.state.write.unreadableCurrentValue = "fail"; },
    "la forma de cuerpo de DIRECT.GET concedible": (d) => { d.state.read.bodyFormAllowedToServices = true; },
    "watch con deliver_policy all": (d) => { d.state.watch.deliverPolicy = "all"; },
    "la entrada sin tenantId": (d) => { d.state.watch.entry = ["key", "deleted", "data", "aggregateVersion", "revision"]; },
    "un valor reemplazado se reentrega": (d) => { d.state.watch.replacedMessagesAreNotRedelivered = false; },
    "onSynced espera a una entrada en un stream vacío": (d) => { d.state.watch.onSynced.emptyStream = "waits-for-first-entry"; },
    "la DLQ de estado conserva el dato": (d) => { d.state.dlq.dataReplacedBy = ["key", "revision", "data"]; },
    "el estado se reproduce desde la DLQ": (d) => { d.state.dlq.replay = "allowed"; },
    "la DLQ de estado conserva la firma": (d) => { d.state.dlq.dropsSignature = false; },
    "la DLQ de un dominio sin estado cambia": (d) => { d.state.dlq.streamSubjectsWhenDomainHasNoState = ["dlq.<dominio>.>", "dlq.state.<dominio>.>"]; },
    "el subject de DLQ de estado lleva tenant": (d) => { d.state.dlq.subject.format = "dlq.state.<dominio>.<agregado>.v<major>.<tenant>"; },
    "el observador recibe STREAM.INFO del estado": (d) => { d.state.acl.observerStreamInfoState = "granted"; },
    "DIRECT.GET de observador sin tenant fijado": (d) => { d.state.acl.observerDirectGet = "^\\$JS\\.API\\.DIRECT\\.GET\\.STATE_[A-Z0-9_]+\\.>$"; },
    "CONSUMER.CREATE de estado concedido por defecto": (d) => { d.state.acl.devOnlyPatterns = ["dlqReaderCreate"]; },
    "la métrica de estado entre las siete": (d) => { d.observability.metrics.flux_state_writes_total = { type: "counter", labels: ["contract", "outcome"] }; },
    "la métrica de estado con tenant como etiqueta": (d) => { d.state.metrics.flux_state_writes_total.labels = ["contract", "tenantid", "outcome"]; },
    "un outcome de estado de más": (d) => { d.state.metrics.flux_state_writes_total.outcomes.push("error"); },
    "el consumer de estado con el sufijo de tenant": (d) => { d.state.metrics.reusedConsumptionMetrics.consumer = "durable-with-tenant-suffix"; },
    "PII permitida en el evento": (d) => { d.security.piiInPayload.event = "allowed"; },
    "piiInPayload otra vez sin distinguir primitivas": (d) => { d.security.piiInPayload = "forbidden"; },
  };
  const base = fallosDeEstado(p);
  if (base.length) fallos.push(`autocomprobación: fallosDeEstado ya falla sobre el protocol.json real (${base.length}); no se puede autocomprobar`);
  for (const [nombre, estropea] of Object.entries(mutaciones)) {
    const copia = structuredClone(p);
    estropea(copia);
    if (!fallosDeEstado(copia).length) fallos.push(`autocomprobación: la mutación «${nombre}» NO la detecta fallosDeEstado`);
  }
  mutacionesEstado = Object.keys(mutaciones).length;
}

// Autocomprobación de `fallosDeAclDeEstado`: la misma disciplina, sobre el documento Y sobre el generador.
let mutacionesAcl = 0;
{
  const base = fallosDeAclDeEstado(p);
  if (base.length) fallos.push(`autocomprobación: fallosDeAclDeEstado ya falla sobre el protocol.json y el generador reales (${base.length}); no se puede autocomprobar`);
  const delDocumento = {
    "sin la excepción de DIRECT.GET de estado": (d) => { d.security.acl.neverGrantedExceptions = []; },
    "solo la excepción del dueño": (d) => { d.security.acl.neverGrantedExceptions = ["state.acl.ownerDirectGet"]; },
    "solo la excepción del observador": (d) => { d.security.acl.neverGrantedExceptions = ["state.acl.observerDirectGet"]; },
    "una tercera excepción": (d) => { d.security.acl.neverGrantedExceptions.push("state.acl.dlqPublish"); },
    "una excepción que no existe": (d) => { d.security.acl.neverGrantedExceptions.push("state.acl.noExiste"); },
    "la excepción del dueño admite DIRECT.GET.>": (d) => { d.state.acl.ownerDirectGet = "^\\$JS\\.API\\.DIRECT\\.GET\\.>$"; },
    "la excepción del dueño admite la forma de cuerpo": (d) => { d.state.acl.ownerDirectGet = "^\\$JS\\.API\\.DIRECT\\.GET\\.STATE_[A-Z0-9_]+(\\..*)?$"; },
    "la excepción del observador sin el tenant fijado": (d) => { d.state.acl.observerDirectGet = "^\\$JS\\.API\\.DIRECT\\.GET\\.STATE_[A-Z0-9_]+\\.state\\.[a-z0-9]+(-[a-z0-9]+)*\\.[a-z0-9]+(-[a-z0-9]+)*\\.v[1-9][0-9]{0,3}\\.>$"; },
    "neverGrantedPattern ya no caza DIRECT.GET": (d) => { d.security.acl.neverGrantedPattern = d.security.acl.neverGrantedPattern.replace("|\\$JS\\.API\\.DIRECT\\.GET(\\..*)?", ""); },
    "streamInfo admite STATE_": (d) => { d.security.acl.permissionPatterns.streamInfo = "^\\$JS\\.API\\.STREAM\\.INFO\\.(EVT|DLQ|STATE)_[A-Z0-9_]+$"; },
    "ownerPublish admite state.>": (d) => { d.state.acl.ownerPublish = "^state\\.([a-z0-9]+(-[a-z0-9]+)*\\.)?>$"; },
    "el patrón de estado casa con el de dominio propio": (d) => { d.state.acl.ownerPublish = d.security.acl.permissionPatterns.ownDomain; },
    "ownerPermissions con el consumerInfo de STATE_ a secas": (d) => { d.state.acl.ownerPermissions[2] = "consumerInfo (durable dlq_state)"; },
    "ownerPermissions sin el ack del durable de DLQ": (d) => { d.state.acl.ownerPermissions = d.state.acl.ownerPermissions.slice(0, 4); },
    "ownerPermissions cita un patrón que no existe": (d) => { d.state.acl.ownerPermissions.push("security.acl.permissionPatterns.noExiste"); },
    "ownerPermissions incluye STREAM.INFO de estado": (d) => { d.state.acl.ownerPermissions.push("security.acl.permissionPatterns.streamInfo (STATE_<DOM>)"); },
    "el comentario dice que el dueño PUEDE tener STREAM.INFO": (d) => { d.state.acl.$commentStreamInfo += " El dueno PUEDE tenerlo."; },
    "state.acl.consumerInfo admite el durable de DLQ_": (d) => { d.state.acl.consumerInfo = "^\\$JS\\.API\\.CONSUMER\\.INFO\\.(STATE|DLQ)_[A-Z0-9_]+\\.[a-z0-9]+(-[a-z0-9]+)*__[a-z0-9_]+$"; },
    "security.acl.consumerInfo ya no admite DLQ_": (d) => { d.security.acl.permissionPatterns.consumerInfo = "^\\$JS\\.API\\.CONSUMER\\.INFO\\.EVT_[A-Z0-9_]+\\.[a-z0-9]+(-[a-z0-9]+)*__[a-z0-9_]+$"; },
  };
  for (const [nombre, estropea] of Object.entries(delDocumento)) {
    const copia = structuredClone(p);
    estropea(copia);
    if (!fallosDeAclDeEstado(copia).length) fallos.push(`autocomprobación: la mutación del documento «${nombre}» NO la detecta fallosDeAclDeEstado`);
  }
  // Generadores estropeados: lo que haría un `permisosDe` que concede de más o de menos.
  const sobre = (modifica) => (svc, opts) => {
    const e = permisosDe(svc, opts);
    const r = { ...e, publish: [...e.publish], durables: e.durables.map((x) => ({ ...x })) };
    modifica(r, svc, opts);
    return r;
  };
  const delGenerador = {
    "un observador que ESCRIBE (state.<dom>.>)": sobre((r, s) => { if (s.name === "tienda-web") r.publish.push("state.clientes.>"); }),
    "el dueño que escribe TODO el estado (state.>)": sobre((r, s) => { if (s.name === "crm-api") r.publish.push("state.>"); }),
    "un observador con STREAM.INFO.STATE_<DOM>": sobre((r, s) => { if (s.name === "tienda-web") r.publish.push("$JS.API.STREAM.INFO.STATE_CLIENTES"); }),
    "el dueño con STREAM.INFO.STATE_<DOM>": sobre((r, s) => { if (s.name === "crm-api") r.publish.push("$JS.API.STREAM.INFO.STATE_CLIENTES"); }),
    "un observador confinado con DIRECT.GET de TODOS los tenants": sobre((r, s) => { if (s.name === "tienda-web") r.publish = r.publish.map((x) => x.replace(".acme.*", ".*.*")); }),
    "un observador con la forma de CUERPO de DIRECT.GET": sobre((r, s) => { if (s.name === "tienda-web") r.publish.push("$JS.API.DIRECT.GET.STATE_CLIENTES"); }),
    "un observador con DIRECT.GET.> «para arreglarlo»": sobre((r, s) => { if (s.name === "tienda-web") r.publish.push("$JS.API.DIRECT.GET.>"); }),
    "un observador sin DIRECT.GET": sobre((r, s) => { r.publish = r.publish.filter((x) => !(s.name === "tienda-web" && x.includes("DIRECT.GET"))); }),
    "el dueño con DIRECT.GET de TODO su stream de estado": sobre((r, s) => { if (s.name === "crm-api") r.publish = r.publish.map((x) => x.replace("STATE_CLIENTES.state.clientes.>", "STATE_CLIENTES.state.>")); }),
    "CONSUMER.CREATE de estado por defecto": (svc, opts) => {
      const e = permisosDe(svc, opts);
      return opts?.dev ? e : { ...e, publish: [...e.publish, ...e.durables.filter((d) => d.kind === "state").map((d) => `$JS.API.CONSUMER.CREATE.${d.stream}.${d.durable}.${d.filter}`)] };
    },
    "un durable por tenant sin el tenant en el nombre": sobre((r) => { for (const d of r.durables) if (d.kind === "state" && d.tenant) d.durable = d.durable.replace(/__[a-z0-9_]+$/, ""); }),
    "un durable que observa todos con sufijo de tenant": sobre((r) => { for (const d of r.durables) if (d.kind === "state" && !d.tenant) d.durable += "__acme"; }),
    "un dlq.state.<contrato> por tenant": sobre((r, s) => { if (s.name === "mixto-api") r.publish.push("dlq.state.clientes.cliente.v1.globex"); }),
    "el observador sin STREAM.INFO.DLQ_<DOM>": sobre((r, s) => { r.publish = r.publish.filter((x) => !(s.name === "tienda-web" && x === "$JS.API.STREAM.INFO.DLQ_CLIENTES")); }),
    "el dueño que no lee su DLQ de estado": sobre((r, s) => { r.durables = r.durables.filter((d) => !(s.name === "crm-api" && d.kind === "dlq-state")); }),
    "el dueño de un dominio reservado, sin guarda": (svc, opts) => {
      if (svc.owns.some((d) => RESERVED_DOMAINS.includes(d))) return { publish: svc.owns.map((d) => `${d}.>`), subscribe: [`_INBOX.${svc.name}.>`], durables: [], grupos: [] };
      return permisosDe(svc, opts);
    },
    "una suscripción a state.>": sobre((r, s) => { if (s.name === "tienda-web") r.subscribe = [...r.subscribe, "state.>"]; }),
  };
  for (const [nombre, emisor] of Object.entries(delGenerador)) {
    if (!fallosDeAclDeEstado(p, emisor).length) fallos.push(`autocomprobación: el generador estropeado «${nombre}» NO lo detecta fallosDeAclDeEstado`);
  }
  mutacionesAcl = Object.keys(delDocumento).length + Object.keys(delGenerador).length;
}

// Autocomprobación de `fallosDeRpc`: cada copia estropeada DEBE dar un fallo, o la invariante no vigila nada. Son las
// equivocaciones reales que 11-request-reply.md existe para impedir.
let mutacionesRpc = 0;
{
  const mutaciones = {
    "quitar un código de rpc": (d) => { delete d.errors.codes.RPC_TIMEOUT; },
    "un séptimo código de rpc": (d) => { d.errors.codes.OTRO_CODIGO_RPC = { class: "permanent", capability: "rpc" }; },
    "RPC_UNAVAILABLE como PERMANENT": (d) => { d.errors.codes.RPC_UNAVAILABLE.class = "permanent"; },
    "RPC_TIMEOUT como PERMANENT": (d) => { d.errors.codes.RPC_TIMEOUT.class = "permanent"; },
    "STALE_REQUEST como RETRYABLE": (d) => { d.errors.codes.STALE_REQUEST.class = "retryable"; },
    "RPC_INVALID_REPLY como RETRYABLE": (d) => { d.errors.codes.RPC_INVALID_REPLY.class = "retryable"; },
    "RPC_UNAVAILABLE esperando al timeout": (d) => { d.errors.codes.RPC_UNAVAILABLE.immediate = false; },
    "INVALID_RPC_CALL con red": (d) => { d.errors.codes.INVALID_RPC_CALL.failsBeforeNetwork = false; },
    "RESERVED_DOMAIN solo en publish y state": (d) => { d.errors.codes.RESERVED_DOMAIN.thrownBy = ["publish", "put"]; },
    "una capacidad rpc sin definir": (d) => { delete d.capabilities.definitions.rpc; },
    "request/reply otra vez fuera de alcance": (d) => { d.outOfScopeV1.push("request/reply"); },
    "quitar rpc de los dominios reservados": (d) => { d.naming.reservedDomains.names = ["dlq", "state", "system"]; },
    "request no rechaza los dominios reservados": (d) => { d.rpc.reserved.appliesTo = ["respond"]; },
    "el prefijo rpc. en la operación como operación inválida": (d) => { d.rpc.reserved.prefixInOperationName = "INVALID_RPC_CALL"; },
    "las peticiones por JetStream": (d) => { d.rpc.transport.publishViaJetStream = true; },
    "un stream de rpc": (d) => { d.rpc.transport.stream = "RPC_CLIENTES"; },
    "una DLQ de rpc": (d) => { d.rpc.transport.dlq = "dlq.rpc"; },
    "el subject de rpc con 4 tokens": (d) => { d.rpc.subject.format = "rpc.<dominio>.<agregado>.<operacion>"; d.rpc.subject.tokens = 4; },
    "el subject de rpc admite comodines": (d) => { d.rpc.subject.pattern = "^rpc\\.[a-z0-9*>-]+\\.[a-z0-9-]+\\.v[1-9][0-9]{0,3}\\.[a-z0-9-]+$"; },
    "el subject de rpc admite mayúsculas": (d) => { d.rpc.subject.pattern = d.rpc.subject.pattern.replaceAll("a-z", "A-Za-z"); },
    "la operación distinta de un subject de evento": (d) => { d.rpc.operation.pattern = "^[a-z]+\\.[a-z]+\\.v[1-9]\\.[a-z]+$"; },
    "la operación admite el prefijo rpc.": (d) => { d.rpc.operation.pattern = "^(rpc\\.)?[a-z0-9]+(-[a-z0-9]+)*\\.[a-z0-9]+(-[a-z0-9]+)*\\.v[1-9][0-9]{0,3}\\.[a-z0-9]+(-[a-z0-9]+)*$"; },
    "el nombre de operación admite guiones bajos": (d) => { d.rpc.operationName.pattern = "^[a-z0-9]+([-_][a-z0-9]+)*$"; },
    "suscripción al comodín rpc.<dominio>.>": (d) => { d.rpc.queueGroup.subscription = "domain-wildcard"; d.rpc.queueGroup.wildcardSubscriptionForbidden = false; },
    "el grupo de cola no es el servicio": (d) => { d.rpc.queueGroup.name = "random"; },
    "un error con class poison": (d) => { d.rpc.error.classes = ["retryable", "permanent", "poison"]; },
    "las claves del error en otro orden": (d) => { d.rpc.error.dataKeys = ["code", "class", "message", "retryAfterMs"]; },
    "el error con clasificación confidential": (d) => { d.rpc.error.dataclassification = "confidential"; },
    "un error desconocido del handler como retryable": (d) => { d.rpc.error.handlerErrors.unknown = "retryable"; },
    "el mensaje de una excepción desconocida se reenvía": (d) => { d.rpc.error.handlerErrors.unknownMessage = "exception-text"; },
    "el tipo del error distinto": (d) => { d.rpc.type.error.value = "com.flux.rpc.error.v2"; },
    "el timeout por defecto de 30 s": (d) => { d.rpc.timeout.defaultMs = 30000; },
    "el timeout máximo por encima del ack_wait": (d) => { d.rpc.timeout.maxMs = 60000; },
    "un timeout fuera de rango que se recorta": (d) => { d.rpc.timeout.outOfRangeCode = "clamp"; },
    "tres intentos por llamada": (d) => { d.rpc.timeout.attemptsPerCall = 3; },
    "el responder no deduplica por id": (d) => { d.rpc.idempotency.responderMustDeduplicateById = false; },
    "un reintento con id nuevo": (d) => { d.rpc.idempotency.retryReusesIdAndBytes = false; },
    "core NATS deduplica": (d) => { d.rpc.idempotency.coreNatsDeduplicates = true; },
    "reintentar ante un error permanente": (d) => { d.rpc.idempotency.retryOnlyOn = ["RPC_UNAVAILABLE", "RPC_TIMEOUT", "INVALID_SCHEMA"]; },
    "la ventana de STALE_REQUEST de 30 s": (d) => { d.rpc.signing.staleRequestWindowSeconds = 30; },
    "la ventana sin la firma en require": (d) => { d.rpc.signing.appliesOnlyWithSigningMode = "always"; },
    "el SDK recuerda ids": (d) => { d.rpc.signing.responderDoesNotRememberIds = false; },
    "el límite de mensaje de 2 MiB": (d) => { d.rpc.size.maxMessageBytes = 2097152; },
    "las cabeceras no cuentan": (d) => { d.rpc.size.limitCountsHeaders = false; },
    "una respuesta grande que se publica": (d) => { d.rpc.size.replyTooLarge = "publish-and-throw"; },
    "no responders sin exigir las banderas": (d) => { d.rpc.noResponders.connectFlags = ["headers"]; },
    "no responders con otro estado": (d) => { d.rpc.noResponders.status = 404; },
    "un atributo raíz nuevo en el sobre": (d) => { d.rpc.envelope.newRootAttributes = ["rpcdeadline"]; },
    "la respuesta sin causationid": (d) => { d.rpc.envelope.reply.causationid = "none"; },
    "la respuesta con otro tenant": (d) => { d.rpc.envelope.reply.tenantid = "responder-tenant"; },
    "cabeceras obligatorias": (d) => { d.rpc.headers.required = ["traceparent"]; },
    "la respuesta sin validar contra la petición": (d) => { d.rpc.caller.replyChecks = ["is-cloudevent"]; },
    "una petición denegada que espera al timeout": (d) => { d.rpc.caller.deniedMustFailFast = false; },
    "el responder filtra por el tenant de connect": (d) => { d.rpc.responder.tenantFilterByConnection = true; },
    "parar cerrando la conexión de golpe": (d) => { d.rpc.responder.stop = "close"; },
    "el doctor compara con rpc.>": (d) => { d.rpc.doctor.overlapsWith = "rpc.>"; },
    "el doctor deja pasar un comodín en medio": (d) => { d.rpc.doctor.flagged = d.rpc.doctor.flagged.filter((x) => x !== "*.>"); },
    "el doctor marca de más": (d) => { d.rpc.doctor.notFlagged.push("rpc.>"); },
    "el doctor solo avisa": (d) => { d.rpc.doctor.severity = "warn"; },
    "la ACL de quien llama admite rpc.>": (d) => { d.rpc.acl.callerPublish = "^rpc\\.(.+|>)$"; },
    "la ACL del dueño admite rpc.>": (d) => { d.rpc.acl.ownerSubscribe = "^rpc\\.([a-z0-9]+(-[a-z0-9]+)*\\.)?>$"; },
    "el dueño con permiso de publicar en inboxes": (d) => { d.rpc.acl.ownerPublishOnInboxes = "_INBOX.>"; },
    "inbox común para quien llama": (d) => { d.rpc.acl.callerSubscribe = "_INBOX.>"; },
    "allow_responses con 10 respuestas": (d) => { d.rpc.acl.ownerAllowResponses.maxReplies = 10; },
    "allow_responses que caduca antes del timeout": (d) => { d.rpc.acl.ownerAllowResponses.recommendedExpiresSeconds = 5; },
    "una operación sin su par": (d) => { d.rpc.schemaPath.pairRequired = false; },
    "el esquema del error opcional": (d) => { d.rpc.schemaPath.errorSchemaRequiredWithAnyRpcSchema = false; },
    "enum de petición con extensible-enum obligatorio": (d) => { d.rpc.compatibility.request.extensibleEnumRequiredToAddValue = true; },
    "enum de respuesta sin extensible-enum": (d) => { d.rpc.compatibility.reply.extensibleEnumRequiredToAddValue = false; },
    "llamantes antes que el dueño": (d) => { d.rpc.compatibility.deployOrder = "callers-first"; },
    "la métrica de rpc entre las siete": (d) => { d.observability.metrics.flux_rpc_client_requests_total = { type: "counter", labels: ["operation", "outcome"] }; },
    "una métrica de rpc de menos": (d) => { delete d.rpc.metrics.flux_rpc_duration_seconds; },
    "el tenant como etiqueta de rpc": (d) => { d.rpc.metrics.flux_rpc_client_requests_total.labels = ["operation", "tenantid", "outcome"]; },
    "quién llama como etiqueta de rpc": (d) => { d.rpc.metrics.flux_rpc_server_requests_total.labels = ["operation", "caller", "outcome"]; },
    "un outcome de cliente de menos": (d) => { d.rpc.metrics.flux_rpc_client_requests_total.outcomes = ["ok", "timeout"]; },
    "role de la duración con otro valor": (d) => { d.rpc.metrics.flux_rpc_duration_seconds.roles = ["caller", "callee"]; },
    "la duración sin los buckets del ack_wait": (d) => { d.rpc.metrics.flux_rpc_duration_seconds.buckets = "custom"; },
    "PII prohibida en rpc": (d) => { d.security.piiInPayload.rpc = "forbidden"; },
  };
  const base = fallosDeRpc(p);
  if (base.length) fallos.push(`autocomprobación: fallosDeRpc ya falla sobre el protocol.json real (${base.length}); no se puede autocomprobar`);
  for (const [nombre, estropea] of Object.entries(mutaciones)) {
    const copia = structuredClone(p);
    estropea(copia);
    if (!fallosDeRpc(copia).length) fallos.push(`autocomprobación: la mutación «${nombre}» NO la detecta fallosDeRpc`);
  }
  mutacionesRpc = Object.keys(mutaciones).length;
}


// Autocomprobación de `fallosDeAclDeRpc`: la misma disciplina, sobre el documento Y sobre el generador. Las equivocaciones
// reales que 06-security.md §3.9 existe para impedir: conceder `rpc.<dominio>.>` o `rpc.>` a quien llama, un permiso de
// publicación sobre inboxes al dueño, un allow_responses sin acotar o ausente, una suscripción de dominio a quien solo llama.
let mutacionesAclRpc = 0;
{
  const base = fallosDeAclDeRpc(p);
  if (base.length) fallos.push(`autocomprobación: fallosDeAclDeRpc ya falla sobre el protocol.json y el generador reales (${base.length}); no se puede autocomprobar`);
  const delDocumento = {
    "las claves de rpc fuera de serviceKeys": (d) => { d.security.manifest.serviceKeys = ["name", "owns", "consumes", "watches"]; },
    "solo `calls`": (d) => { d.rpc.acl.manifest.keys = ["calls"]; },
    "la entrada de rpc con su propio patrón": (d) => { d.rpc.acl.manifest.entryPattern = "^[a-z.]+$"; },
    "el prefijo rpc. admitido en el manifiesto": (d) => { d.rpc.acl.manifest.entryWithRpcPrefix = "INVALID_RPC_CALL"; },
    "los dominios reservados admitidos": (d) => { d.rpc.acl.manifest.reservedDomainsRejected = false; },
    "los comodines admitidos": (d) => { d.rpc.acl.manifest.wildcardsRejected = false; },
    "las operaciones repetidas admitidas": (d) => { d.rpc.acl.manifest.duplicatesRejected = false; },
    "una operación sin su par en el bundle": (d) => { d.rpc.acl.manifest.operationMustExistInBundleRpcIndex = false; },
    "quien llama concede también la suscripción del dueño": (d) => { d.rpc.acl.manifest.calls.grants = ["callerPublish", "ownerSubscribe"]; },
    "una llamada a una operación que nadie atiende": (d) => { d.rpc.acl.manifest.calls.operationMustBeServed = false; },
    "una llamada al propio dominio": (d) => { d.rpc.acl.manifest.calls.ownDomainRejected = false; },
    "atender el dominio de otro": (d) => { d.rpc.acl.manifest.serves.onlyOwnDomain = false; },
    "el dueño con permiso de publicación sobre inboxes (en el manifiesto)": (d) => { d.rpc.acl.manifest.serves.noPublishOnInboxes = false; },
    "el dueño sin allow_responses": (d) => { d.rpc.acl.manifest.serves.grants = ["ownerSubscribe"]; },
    "allow_responses generado con 2 respuestas": (d) => { d.rpc.acl.manifest.generatedAllowResponses.max = 2; },
    "allow_responses generado sin caducidad": (d) => { delete d.rpc.acl.manifest.generatedAllowResponses.expires; },
    "allow_responses generado que caduca antes del timeout": (d) => { d.rpc.acl.manifest.generatedAllowResponses.expires = "5s"; },
    "allow_responses recomendado distinto del generado": (d) => { d.rpc.acl.ownerAllowResponses.recommendedExpiresSeconds = 60; },
    "el Modelo B con otros permisos": (d) => { d.rpc.acl.manifest.tenantsModelBSamePermissions = false; },
    "OPERATION_RE distinta de rpc.operation.pattern": (d) => { d.rpc.operation.pattern = d.rpc.operation.pattern.replace("v[1-9]", "v[0-9]"); },
  };
  for (const [nombre, estropea] of Object.entries(delDocumento)) {
    const copia = structuredClone(p);
    estropea(copia);
    if (!fallosDeAclDeRpc(copia).length) fallos.push(`autocomprobación: la mutación del documento «${nombre}» NO la detecta fallosDeAclDeRpc`);
  }
  // Generadores estropeados: lo que haría un `permisosDe` que concede de más o de menos.
  const sobre = (modifica) => (svc, opts) => {
    const e = permisosDe(svc, opts);
    const r = { ...e, publish: [...e.publish], subscribe: [...e.subscribe], allowResponses: e.allowResponses ? { ...e.allowResponses } : null };
    modifica(r, svc, opts);
    return r;
  };
  const delGenerador = {
    "quien llama con `rpc.<dominio>.>`": sobre((r, s) => { if (s.name === "web-api") r.publish.push("rpc.clientes.>"); }),
    "quien llama con `rpc.>`": sobre((r, s) => { if (s.name === "pedidos-api") r.publish.push("rpc.>"); }),
    "quien llama con un comodín por token": sobre((r, s) => { if (s.name === "web-api") r.publish.push("rpc.clientes.cliente.v1.*"); }),
    "quien llama con `rpc.*.*.*.*`": sobre((r, s) => { if (s.name === "web-api") r.publish.push("rpc.*.*.*.*"); }),
    "quien llama con una operación de más": sobre((r, s) => { if (s.name === "pedidos-api") r.publish.push("rpc.clientes.cliente.v1.obtener"); }),
    "quien llama sin su permiso": sobre((r, s) => { if (s.name === "web-api") r.publish = r.publish.filter((x) => x !== "rpc.clientes.cliente.v1.obtener"); }),
    "quien llama con el prefijo duplicado": sobre((r, s) => { if (s.name === "pedidos-api") r.publish = r.publish.map((x) => x.replace(/^rpc\./, "rpc.rpc.")); }),
    "quien llama con `_INBOX.>` para publicar": sobre((r, s) => { if (s.name === "web-api") r.publish.push("_INBOX.>"); }),
    "quien llama con el inbox común": sobre((r, s) => { if (s.name === "web-api") r.subscribe = ["_INBOX.>"]; }),
    "quien llama sin su inbox privado": sobre((r, s) => { if (s.name === "web-api") r.subscribe = []; }),
    "quien solo llama con una suscripción de dominio": sobre((r, s) => { if (s.name === "web-api") r.subscribe.push("rpc.clientes.>"); }),
    "quien solo llama con allow_responses": sobre((r, s) => { if (s.name === "web-api") r.allowResponses = { max: 1, expires: "30s" }; }),
    "un dueño sin servir nada con rpc.<dominio>.>": sobre((r, s) => { if (s.name === "sin-rpc-api") r.subscribe.push("rpc.inventario.>"); }),
    "el dueño con `rpc.>`": sobre((r, s) => { if (s.name === "crm-api") r.subscribe = [r.subscribe[0], "rpc.>"]; }),
    "el dueño con `rpc.*.>`": sobre((r, s) => { if (s.name === "crm-api") r.subscribe.push("rpc.*.>"); }),
    "el dueño con `>`": sobre((r, s) => { if (s.name === "crm-api") r.subscribe.push(">"); }),
    "el dueño con el rpc.<dominio>.> de otro dominio": sobre((r, s) => { if (s.name === "crm-api") r.subscribe.push("rpc.pedidos.>"); }),
    "el dueño sin su suscripción de dominio": sobre((r, s) => { if (s.name === "crm-api") r.subscribe = [r.subscribe[0]]; }),
    "el dueño de dos dominios con uno solo": sobre((r, s) => { if (s.name === "mixto-api") r.subscribe = r.subscribe.filter((x) => x !== "rpc.envios.>"); }),
    "el dueño con `_INBOX.>` para publicar": sobre((r, s) => { if (s.name === "crm-api") r.publish.push("_INBOX.>"); }),
    "el dueño con permiso de publicación sobre el inbox de otro": sobre((r, s) => { if (s.name === "crm-api") r.publish.push("_INBOX.pedidos-api.>"); }),
    "el dueño sin allow_responses": sobre((r, s) => { if (s.name === "crm-api") r.allowResponses = null; }),
    "allow_responses: true": sobre((r, s) => { if (s.name === "crm-api") r.allowResponses = true; }),
    "allow_responses con 10 respuestas": sobre((r, s) => { if (s.name === "crm-api") r.allowResponses = { max: 10, expires: "30s" }; }),
    "allow_responses sin caducidad": sobre((r, s) => { if (s.name === "crm-api") r.allowResponses = { max: 1 }; }),
    "allow_responses que caduca antes del timeout": sobre((r, s) => { if (s.name === "crm-api") r.allowResponses = { max: 1, expires: "5s" }; }),
    "el dueño de un dominio reservado ya no se rechaza": (svc, opts) => {
      if (svc.calls?.some((op) => op.startsWith("rpc.") || op.startsWith("state."))) return { publish: svc.calls.map((op) => `rpc.${op}`), subscribe: [`_INBOX.${svc.name}.>`], durables: [], grupos: [], allowResponses: null };
      return permisosDe(svc, opts);
    },
    "atender el dominio de otro ya no se rechaza": (svc, opts) => {
      if (svc.serves?.length && !svc.owns.includes(svc.serves[0].split(".")[0])) return { publish: ["$JS.API.INFO"], subscribe: [`_INBOX.${svc.name}.>`, `rpc.${svc.serves[0].split(".")[0]}.>`], durables: [], grupos: [], allowResponses: { max: 1, expires: "30s" } };
      return permisosDe(svc, opts);
    },
    "un comodín ya no se rechaza": (svc, opts) => {
      if ([...(svc.calls ?? []), ...(svc.serves ?? [])].some((op) => /[*>]/.test(op))) return { publish: ["$JS.API.INFO", ...(svc.calls ?? []).map((op) => `rpc.${op}`)], subscribe: [`_INBOX.${svc.name}.>`], durables: [], grupos: [], allowResponses: null };
      return permisosDe(svc, opts);
    },
  };
  for (const [nombre, emisor] of Object.entries(delGenerador)) {
    if (!fallosDeAclDeRpc(p, emisor).length) fallos.push(`autocomprobación: el generador estropeado «${nombre}» NO lo detecta fallosDeAclDeRpc`);
  }
  mutacionesAclRpc = Object.keys(delDocumento).length + Object.keys(delGenerador).length;
}

// Autocomprobación de `fallosDeEdge`: cada copia estropeada DEBE dar un fallo, o la invariante no vigila nada. Son las
// equivocaciones reales que 12-edge.md existe para impedir: firmar sin el instante, no renovarlo, comparar con `===`, seguir una
// redirección, reintentar un 4xx permanente, un id que no es un UUIDv7 o que no depende de la clave, un `via` que concede de más,
// un token en claro, un `data` en los registros, un código del protocolo sin estado HTTP…
let mutacionesEdge = 0;
{
  const base = fallosDeEdge(p);
  if (base.length) fallos.push(`autocomprobación: fallosDeEdge ya falla sobre el protocol.json real (${base.length}); no se puede autocomprobar`);
  const cambia = (d, ruta, valor) => { const k = ruta.split("."); let o = d; for (const x of k.slice(0, -1)) o = o[x]; o[k.at(-1)] = valor; };
  const mutaciones = {
    // No es una capacidad de SDK
    "el gateway como capacidad de SDK": (d) => { d.capabilities.definitions.edge = { spec: "specification/12-edge.md", evidence: "sdk-files" }; },
    "un SDK que declara edge": (d) => { d.capabilities.sdks.node.capabilities.push("edge"); },
    "edge.capability distinta de null": (d) => { d.edge.capability = "edge"; },
    "el gateway entero como normativo": (d) => { d.edge.normative = ["delivery", "externalId", "gateway"]; },
    "el gateway que no se dice implementación": (d) => { d.edge.gateway.implementation = false; },
    // La entrega saliente
    "la entrega con GET": (d) => { d.edge.delivery.method = "GET"; },
    "el Content-Type application/json": (d) => { d.edge.delivery.contentType = "application/json"; },
    "un cuerpo que no es el evento tal cual": (d) => { d.edge.delivery.body.isEventVerbatim = false; },
    "bytes distintos en cada intento": (d) => { d.edge.delivery.body.sameBytesEveryAttempt = false; },
    "las extensiones dlq* en el cuerpo": (d) => { d.edge.delivery.body.dlqExtensionsStripped = false; },
    "la Idempotency-Key aleatoria": (d) => { d.edge.delivery.headers.idempotencyKey.value = "random-uuid"; },
    "la Idempotency-Key distinta en cada intento": (d) => { d.edge.delivery.headers.idempotencyKey.sameEveryAttempt = false; },
    "otra cabecera de firma": (d) => { d.edge.delivery.headers.signature.name = "X-Signature"; },
    "HMAC-SHA1": (d) => { d.edge.delivery.signature.algorithm = "HMAC-SHA1"; },
    "firmar solo el cuerpo (sin el instante)": (d) => { d.edge.delivery.signature.signedMessage = "<body-bytes>"; },
    "la firma en base64": (d) => { d.edge.delivery.signature.encoding = "base64"; },
    "el instante fijo en los reintentos": (d) => { d.edge.delivery.signature.timestampRenewedEveryAttempt = false; },
    "un solo v1 (sin rotación)": (d) => { d.edge.delivery.signature.multipleV1ForRotation = false; },
    "el receptor compara con ==": (d) => { d.edge.delivery.signature.receiver.compare = "equals"; },
    "una tolerancia de una hora": (d) => { d.edge.delivery.signature.receiver.toleranceSeconds = 3600; },
    "una tolerancia asimétrica": (d) => { d.edge.delivery.signature.receiver.toleranceSymmetric = false; },
    "verificar sobre el JSON reserializado": (d) => { d.edge.delivery.signature.receiver.verifyRawBytes = false; },
    "decodificar el hexadecimal sin validarlo": (d) => { d.edge.delivery.signature.receiver.validateHexFormatBeforeDecoding = false; },
    "una firma rechazada con 5xx": (d) => { d.edge.delivery.signature.receiver.rejectedAs = "5xx"; },
    "el secreto en la configuración": (d) => { d.edge.delivery.signature.secret.neverInConfiguration = false; },
    "un vector con un v1 cambiado": (d) => { d.edge.delivery.signature.vectors[0].v1 = d.edge.delivery.signature.vectors[0].v1.replace(/^./, "0"); },
    "un vector con el instante cambiado": (d) => { d.edge.delivery.signature.vectors[2].t += 1; },
    "un vector con el cuerpo reserializado": (d) => { d.edge.delivery.signature.vectors[2].body = d.edge.delivery.signature.vectors[2].body.replace('"delta":-3', '"delta": -3'); },
    "sin el vector de GitHub": (d) => { d.edge.delivery.signature.vectors.pop(); },
    "sin ningún vector con no-ASCII": (d) => { d.edge.delivery.signature.vectors[2].body = d.edge.delivery.signature.vectors[2].body.replace("Málaga", "Malaga"); d.edge.delivery.signature.vectors[2].v1 = hmacHex(d.edge.delivery.signature.vectors[2].secret, d.edge.delivery.signature.vectors[2].t, d.edge.delivery.signature.vectors[2].body); },
    "el vector de rotación con un secreto de menos": (d) => { d.edge.delivery.signature.rotationVector.secrets.pop(); },
    "el patrón admite hexadecimal en mayúsculas": (d) => { d.edge.delivery.signature.pattern = d.edge.delivery.signature.pattern.replaceAll("0-9a-f", "0-9a-fA-F"); },
    "el patrón admite una firma sin v1": (d) => { d.edge.delivery.signature.pattern = "^t=[0-9]{1,15}(,v1=[0-9a-f]{64})*$"; },
    "el patrón admite una firma corta": (d) => { d.edge.delivery.signature.pattern = d.edge.delivery.signature.pattern.replace("{64}", "{32,64}"); },
    // La respuesta del receptor
    "seguir las redirecciones": (d) => { d.edge.delivery.response.redirectsFollowed = true; },
    "una firma rechazada que se reintenta": (d) => { d.edge.delivery.response.firmRejectedIsPermanent = false; },
    "registrar el cuerpo de la respuesta": (d) => { d.edge.delivery.response.bodyNeverLogged = false; },
    "un 404 reintentable": (d) => { d.errors.classes.retryable.httpStatus.push(404); },
    "un 400 reintentable acotado": (d) => { d.edge.delivery.response.retryableBounded.httpStatus.push(400); },
    "el 503 fuera de los reintentables": (d) => { d.errors.classes.retryable.httpStatus = d.errors.classes.retryable.httpStatus.filter((x) => x !== 503); },
    "un 500 permanente": (d) => { d.edge.delivery.response.retryableBounded.alsoEveryOther5xx = false; },
    "un 500 con el presupuesto completo": (d) => { d.errors.classes.retryable.httpStatus.push(500); },
    "un 408 permanente": (d) => { d.edge.delivery.response.retryableBounded.httpStatus = [425]; },
    "un presupuesto acotado de 6": (d) => { d.edge.delivery.response.retryableBounded.budget = 6; },
    "un permanente con presupuesto de 2": (d) => { d.edge.delivery.response.permanent.budget = 2; },
    "un ejemplo que no es lo que dice el documento": (d) => { d.edge.delivery.response.examples.find((x) => x.status === 302).outcome = "retryable"; },
    "el 3xx como ack": (d) => { d.edge.delivery.response.examples.push({ status: 301, outcome: "ack" }); },
    "la red sin ECONNRESET": (d) => { d.edge.delivery.response.network.retryable = d.edge.delivery.response.network.retryable.filter((x) => x !== "ECONNRESET"); },
    "un certificado que no verifica como reintentable": (d) => { d.edge.delivery.response.network.tlsVerificationFailure.class = "retryable"; },
    "el code de TLS en minúsculas": (d) => { d.edge.delivery.response.network.tlsVerificationFailure.code = "tls_verification_failed"; },
    "lo desconocido de la red como permanente": (d) => { d.edge.delivery.response.network.retryableBounded = "permanent"; },
    // Plazos, reintentos y orden
    "un plazo por intento de 30 s": (d) => { d.edge.delivery.timeout.defaultMs = 30000; },
    "un plazo máximo por encima del ack_wait": (d) => { d.edge.delivery.timeout.maxMs = 60000; },
    "un plazo de inactividad en vez de total": (d) => { d.edge.delivery.timeout.scope = "idle"; },
    "Retry-After en todos los reintentos": (d) => { d.edge.delivery.retryAfter.appliesTo = "every-retry"; },
    "Retry-After sin tope": (d) => { d.edge.delivery.retryAfter.maxMs = 86400000; },
    "Retry-After también en un 4xx": (d) => { d.edge.delivery.retryAfter.onlyOn = "any-response"; },
    "dos entregas por subject": (d) => { d.edge.delivery.deliveriesPerSubjectPerClient = 2; },
    "una entrega con orden garantizado": (d) => { d.edge.delivery.ordering = "per-aggregate"; },
    "reintentos propios de la entrega": (d) => { d.edge.delivery.retries = "own-schedule"; },
    // Clasificación de los datos
    "confidential por defecto": (d) => { d.edge.delivery.classification.allowedByDefault = ["public", "internal", "confidential"]; },
    "confidential sin autorización explícita": (d) => { d.edge.delivery.classification.confidentialRequiresExplicitOptIn = false; },
    "confidential por http": (d) => { d.edge.delivery.classification.confidentialRequiresHttps = false; },
    "restricted con autorización": (d) => { d.edge.delivery.classification.restricted = "opt-in"; },
    "una clasificación denegada reintentable": (d) => { d.edge.delivery.classification.deniedClass = "retryable"; },
    "el estado hacia HTTP como normativo": (d) => { d.edge.delivery.state.normative = true; },
    "el estado hacia HTTP como CloudEvent": (d) => { d.edge.delivery.state.bodyIsEvent = true; },
    "el receptor sin deduplicar": (d) => { d.edge.delivery.receiverMust = d.edge.delivery.receiverMust.filter((x) => x !== "deduplicate-by-idempotency-key"); },
    // El id determinista
    "el id con 47 bits de tiempo": (d) => { d.edge.externalId.timeBits = 47; },
    "el id con 64 bits aleatorios": (d) => { d.edge.externalId.randomBits = 64; },
    "rand_a de 16 bits": (d) => { d.edge.externalId.randABits = 16; },
    "otro separador": (d) => { d.edge.externalId.separator = ","; },
    "el hash MD5": (d) => { d.edge.externalId.hash = "MD5"; },
    "el hash sobre UTF-16": (d) => { d.edge.externalId.encoding = "UTF-16"; },
    "el id con el «ahora» como tiempo": (d) => { d.edge.externalId.timeIs = "now"; d.edge.externalId.timeNeverNow = false; },
    "el id sin exigir el instante": (d) => { d.edge.externalId.requiresTime = false; },
    "una clave externa de 4 KiB": (d) => { d.edge.externalId.externalKeyMaxBytes = 4096; },
    "un source con separador": (d) => { d.edge.externalId.sourceMustNotContain = ""; },
    "el id que no es una función pura": (d) => { d.edge.externalId.pureFunction = false; },
    "el id que sustituye al consumidor": (d) => { d.edge.externalId.doesNotReplaceConsumerDedupe = false; },
    "ms = 0 para un proveedor sin instante": (d) => { d.edge.externalId.providerWithoutFactTime.msZeroAdopted = true; },
    "un id determinista sin instante del hecho": (d) => { d.edge.externalId.providerWithoutFactTime.hasDeterministicId = true; },
    "un vector del id con un dígito cambiado": (d) => { d.edge.externalId.vectors[0].id = d.edge.externalId.vectors[0].id.replace(/.$/, "5"); },
    "un vector del id con otro source": (d) => { d.edge.externalId.vectors[0].source = "/produccion/otro-web"; },
    "un vector del id que es un UUIDv4": (d) => { d.edge.externalId.vectors[0].id = d.edge.externalId.vectors[0].id.replace("-7325-", "-4325-"); },
    "sin el vector con no-ASCII": (d) => { d.edge.externalId.vectors = d.edge.externalId.vectors.filter((v) => !/[^\x00-\x7f]/.test(v.externalKey)); },
    "sin el vector del instante 0": (d) => { d.edge.externalId.vectors = d.edge.externalId.vectors.filter((v) => Date.parse(v.time) !== 0); },
    "el patrón del id que admite un UUIDv4": (d) => { d.edge.externalId.pattern = d.edge.externalId.pattern.replace("-7[0-9a-f]{3}-", "-[0-9a-f]{4}-"); },
    "el patrón del id que admite mayúsculas": (d) => { d.edge.externalId.pattern = d.edge.externalId.pattern.replaceAll("0-9a-f", "0-9a-fA-F"); },
    "el atributo id sin remitir a edge.externalId": (d) => { delete d.cloudevents.requiredAttributes.id.deterministicWhenImported; },
    // El manifiesto
    "un valor de via más": (d) => { d.edge.manifest.values = ["gateway", "proxy"]; },
    "via que concede permisos": (d) => { d.edge.manifest.extraPermissions = "admin"; },
    "via que no es solo una anotación": (d) => { d.edge.manifest.annotationOnly = false; },
    "un gateway con permisos especiales": (d) => { d.edge.manifest.gatewayNeedsSpecialPermissions = true; },
    "un cliente de borde con serves": (d) => { d.edge.manifest.servesRejected = false; },
    "via fuera de las claves del manifiesto": (d) => { d.security.manifest.serviceKeys = d.security.manifest.serviceKeys.filter((k) => k !== "via"); },
    // El gateway de referencia
    "un framework web": (d) => { d.edge.gateway.framework = "express"; },
    "falta la ruta de los hooks": (d) => { d.edge.gateway.routes = d.edge.gateway.routes.filter((r) => !r.path.includes("hooks")); },
    "una ruta de más": (d) => { d.edge.gateway.routes.push({ method: "POST", path: "/v1/admin", does: "admin", response: 200 }); },
    "los hooks con Bearer": (d) => { d.edge.gateway.routes.find((r) => r.path.includes("hooks")).auth = "bearer"; },
    "publicar con 200": (d) => { d.edge.gateway.routes.find((r) => r.path.includes("events")).response = 200; },
    "el gateway que escribe el sobre": (d) => { d.edge.gateway.envelopeNeverWrittenByHand = false; },
    "Idempotency-Key sin el instante": (d) => { d.edge.gateway.idempotencyKeyRequiresEventTime = false; },
    "el token en claro": (d) => { d.edge.gateway.auth.plaintextTokenStored = true; d.edge.gateway.auth.storedAs = "plaintext"; },
    "comparar el token con ===": (d) => { d.edge.gateway.auth.compare = "strict-equality"; },
    "comparar solo hasta el primer acierto": (d) => { d.edge.gateway.auth.compareLoopsAllClients = false; },
    "un 401 distinto si el token no existe": (d) => { d.edge.gateway.auth.sameResponseForUnknownAndMissing = false; },
    "un token de 8 bytes": (d) => { d.edge.gateway.auth.tokenBytes = 8; },
    "los hooks sin autenticar": (d) => { d.edge.gateway.auth.hooksAuthenticatedBy = "none"; },
    "un hook sin verify que arranca": (d) => { d.edge.gateway.auth.hookWithoutVerifyRefusesToStart = false; },
    "un esquema de verificación de hook más": (d) => { d.edge.gateway.auth.hookVerifySchemes = ["hmac-sha256", "none"]; },
    "otra tolerancia por defecto en los hooks": (d) => { d.edge.gateway.auth.hookToleranceSecondsDefault = 3600; },
    "la clave externa sin el nombre del hook": (d) => { d.edge.gateway.hooks.keyNamespace = "<providerKey>"; },
    "un hook internal por defecto": (d) => { d.edge.gateway.hooks.defaultClassification = "internal"; },
    "un hook que devuelve otro id en el duplicado": (d) => { d.edge.gateway.hooks.duplicateReturnsSameId = false; },
    "un hook que publica en cualquier dominio": (d) => { d.edge.gateway.hooks.publishesOnlyInOwnDomain = false; },
    "un hook no idempotente sin pedirlo": (d) => { d.edge.gateway.hooks.idempotentFalseRequiresExplicit = false; },
    "un límite de cuerpo de 10 MiB": (d) => { d.edge.gateway.limits.maxBodyBytes = 10485760; },
    "leer el cuerpo antes de rechazarlo": (d) => { d.edge.gateway.limits.rejectsBeforeReading = false; },
    "no cerrar la conexión ante un cuerpo grande": (d) => { d.edge.gateway.limits.closesConnectionOnOversize = false; },
    "un chunked sin límite": (d) => { d.edge.gateway.limits.chunkedStopsAtLimit = false; },
    "el límite de peticiones por IP": (d) => { d.edge.gateway.limits.rateLimitPer = "ip"; },
    "server.timeout por debajo del rpc": (d) => { d.edge.gateway.limits.serverTimeoutMs = 10000; },
    "el intervalo de comprobación por defecto de Node": (d) => { d.edge.gateway.limits.connectionsCheckingIntervalMs = 30000; },
    "headersTimeout mayor que requestTimeout": (d) => { d.edge.gateway.limits.headersTimeoutMs = 60000; },
    "un solo modo de TLS": (d) => { d.edge.gateway.tls.modes = ["terminates-in-front"]; },
    "TLS opcional en producción": (d) => { d.edge.gateway.tls.atLeastOneRequiredInProduction = false; },
    "registrar el data": (d) => { d.edge.gateway.logging.neverLogs = d.edge.gateway.logging.neverLogs.filter((x) => x !== "data"); },
    "registrar el data de los confidential": (d) => { d.edge.gateway.logging.logsDataOfConfidential = true; },
    "registrar el data de lo que no es confidential": (d) => { d.edge.gateway.logging.logsDataOfAnyClassification = true; },
    "registrar el token": (d) => { d.edge.gateway.logging.neverLogs = d.edge.gateway.logging.neverLogs.filter((x) => x !== "token"); },
    "secretos en gateway.json": (d) => { d.edge.gateway.config.holdsSecrets = true; },
    "gateway.json sin rechazar un token en claro": (d) => { d.edge.gateway.config.refusesPlaintextSecretKeys = d.edge.gateway.config.refusesPlaintextSecretKeys.filter((x) => x !== "token"); },
    "gateway.json sin rechazar una semilla de NATS": (d) => { d.edge.gateway.config.refusesNatsSeedShapedValues = false; },
    "dos hooks con el mismo nombre": (d) => { d.edge.gateway.config.hookNamesUniqueAcrossClients = false; },
    "dos entregas por subject en la configuración": (d) => { d.edge.gateway.config.onePerSubjectPerClient = false; },
    "drenar NATS antes de contestar lo que vuela": (d) => { d.edge.gateway.shutdown.order = ["stop-accepting", "drain-nats", "answer-in-flight-with-connection-close", "close-idle"]; },
    "una parada sin plazo máximo": (d) => { d.edge.gateway.shutdown.hardDeadline = false; },
    // Los errores HTTP
    "un código del protocolo sin estado HTTP": (d) => { delete d.edge.gateway.errors.protocolCodes.STATE_CONFLICT; },
    "un código nuevo del protocolo sin estado HTTP": (d) => { d.errors.codes.OTRO_CODIGO_NUEVO = { class: "permanent", capability: "rpc" }; },
    "un código a la vez mapeado y no expuesto": (d) => { d.edge.gateway.errors.notSurfaced.PERMISSION_DENIED = "connect"; },
    "PERMISSION_DENIED reintentable": (d) => { d.edge.gateway.errors.protocolCodes.PERMISSION_DENIED.class = "retryable"; },
    "PERMISSION_DENIED con un 401": (d) => { d.edge.gateway.errors.protocolCodes.PERMISSION_DENIED.status = 401; },
    "PERMISSION_DENIED que no lo deniega el broker": (d) => { d.edge.gateway.errors.protocolCodes.PERMISSION_DENIED.deniedByBroker = false; },
    "RPC_TIMEOUT como 408": (d) => { d.edge.gateway.errors.protocolCodes.RPC_TIMEOUT.status = 408; },
    "RPC_TIMEOUT no ambiguo": (d) => { d.edge.gateway.errors.protocolCodes.RPC_TIMEOUT.ambiguous = false; },
    "RPC_UNAVAILABLE como 400": (d) => { d.edge.gateway.errors.protocolCodes.RPC_UNAVAILABLE.status = 400; },
    "RPC_UNAVAILABLE sin Retry-After": (d) => { delete d.edge.gateway.errors.protocolCodes.RPC_UNAVAILABLE.retryAfterSeconds; },
    "NOT_PROVISIONED como 503": (d) => { d.edge.gateway.errors.protocolCodes.NOT_PROVISIONED.status = 503; },
    "un permanente con 502": (d) => { d.edge.gateway.errors.protocolCodes.RPC_INVALID_REPLY.status = 502; },
    "STATE_CONFLICT como 400": (d) => { d.edge.gateway.errors.protocolCodes.STATE_CONFLICT.status = 400; },
    "un 4xx reintentable que no es 408, 409 ni 429": (d) => { d.edge.gateway.errors.protocolCodes.STATE_CONFLICT.status = 403; },
    "un estado de éxito en un error": (d) => { d.edge.gateway.errors.protocolCodes.INVALID_SCHEMA.status = 200; },
    "una clase que no existe": (d) => { d.edge.gateway.errors.protocolCodes.INVALID_SCHEMA.class = "poison"; },
    "un código del borde sin el prefijo EDGE_": (d) => { d.edge.gateway.errors.codes.RATE_LIMITED = d.edge.gateway.errors.codes.EDGE_RATE_LIMITED; delete d.edge.gateway.errors.codes.EDGE_RATE_LIMITED; },
    "un código del borde que choca con el protocolo": (d) => { d.edge.gateway.errors.protocolCodes.EDGE_UNAUTHENTICATED = { status: 401, class: "permanent", definedIn: "reused" }; },
    "un código del borde en errors.codes": (d) => { d.edge.gateway.errors.codesAreNotProtocolCodes = false; },
    "sin token es 403": (d) => { d.edge.gateway.errors.codes.EDGE_UNAUTHENTICATED.status = 403; },
    "el límite por token como 503": (d) => { d.edge.gateway.errors.codes.EDGE_RATE_LIMITED.status = 503; },
    "un cuerpo grande como 400": (d) => { d.edge.gateway.errors.codes.EDGE_BODY_TOO_LARGE.status = 400; },
    "una firma de webhook inválida como 200": (d) => { d.edge.gateway.errors.codes.EDGE_INVALID_WEBHOOK_SIGNATURE.status = 200; },
    "una firma de webhook inválida reintentable": (d) => { d.edge.gateway.errors.codes.EDGE_INVALID_WEBHOOK_SIGNATURE.class = "retryable"; },
    "un error del que responde sin conservar su code": (d) => { d.edge.gateway.errors.remoteErrors.keepsTheirCode = false; },
    "un error permanente del que responde como 503": (d) => { d.edge.gateway.errors.remoteErrors.permanent = 503; },
    "los mensajes de error que citan data": (d) => { d.edge.gateway.errors.messageNeverQuotesData = false; },
    // PII
    "el webhook de un tercero sin tratar su PII": (d) => { delete d.security.piiInPayload.edge; },
  };
  const delAnalizador = {
    "un via desconocido que se acepta": (m, b) => { const c = structuredClone(m); for (const s of c.services) if (s.via && s.via !== "gateway") delete s.via; return analizarManifiesto(c, b); },
    "un cliente de borde con serves que se acepta": (m, b) => { const c = structuredClone(m); for (const s of c.services) if (s.via === "gateway") delete s.serves, delete s.via; return analizarManifiesto(c, b); },
  };
  const delGenerador = {
    "un cliente de borde con `$JS.API.>`": (svc, opts) => { const r = permisosDe(svc, opts); return svc.via === "gateway" ? { ...r, publish: [...r.publish, "$JS.API.>"] } : r; },
    "un cliente de borde con `_INBOX.>`": (svc, opts) => { const r = permisosDe(svc, opts); return svc.via === "gateway" ? { ...r, subscribe: ["_INBOX.>"] } : r; },
    "un cliente de borde con un permiso más": (svc, opts) => { const r = permisosDe(svc, opts); return svc.via === "gateway" ? { ...r, publish: [...r.publish, "pedidos.>"] } : r; },
    "un cliente de borde con un permiso menos": (svc, opts) => { const r = permisosDe(svc, opts); return svc.via === "gateway" ? { ...r, publish: r.publish.filter((x) => !x.startsWith("rpc.")) } : r; },
    "un cliente de borde con allow_responses": (svc, opts) => { const r = permisosDe(svc, opts); return svc.via === "gateway" ? { ...r, allowResponses: { max: 1, expires: "30s" } } : r; },
  };
  for (const [nombre, estropea] of Object.entries(mutaciones)) {
    const copia = structuredClone(p);
    try { estropea(copia); } catch (e) { fallos.push(`autocomprobación: la mutación «${nombre}» no se pudo aplicar (${e.message})`); continue; }
    if (!fallosDeEdge(copia).length) fallos.push(`autocomprobación: la mutación «${nombre}» NO la detecta fallosDeEdge`);
  }
  for (const [nombre, analizador] of Object.entries(delAnalizador)) {
    if (!fallosDeEdge(p, { analizar: analizador }).length) fallos.push(`autocomprobación: el analizador estropeado «${nombre}» NO lo detecta fallosDeEdge`);
  }
  for (const [nombre, emisor] of Object.entries(delGenerador)) {
    if (!fallosDeEdge(p, { emitir: emisor }).length) fallos.push(`autocomprobación: el generador estropeado «${nombre}» NO lo detecta fallosDeEdge`);
  }
  mutacionesEdge = Object.keys(mutaciones).length + Object.keys(delAnalizador).length + Object.keys(delGenerador).length;
}

// ── Los ajustes: el documento real y sus mutaciones ────────────────────────────────────────────────────────────────────────
let mutacionesAjustes = 0;
{
  for (const f of fallosDeAjustes(p)) fallos.push(f);
  const delDocumento = {
    "dlqerror por unidades UTF-16": (d) => { d.cloudevents.optionalExtensions.dlqerror.truncation = "utf16-units"; },
    "dlqerror sin tope en bytes": (d) => { d.cloudevents.optionalExtensions.dlqerror.maxBytes = 1000; },
    "el time de un constructor se copia": (d) => { d.cloudevents.requiredAttributes.time.builderInput.accepts = "anything"; },
    "los decimales de más se redondean": (d) => { d.cloudevents.requiredAttributes.time.builderInput.fractionBeyondMillis = "rounded"; },
    "se admite un segundo 60": (d) => { d.cloudevents.requiredAttributes.time.builderInput.rejectsLeapSecond = false; },
    "el año en UTC sin acotar": (d) => { d.cloudevents.requiredAttributes.time.builderInput.yearRangeUtc = [-1, 10000]; },
    "un time inválido sin código": (d) => { d.cloudevents.requiredAttributes.time.builderInput.invalidCode = "ERROR"; },
    "data con espacios": (d) => { d.data.canonicalForm.insignificantWhitespace = true; },
    "los números de data se reescriben": (d) => { d.data.canonicalForm.numbers = "rewritten"; },
    "una clave repetida conserva las dos": (d) => { d.data.canonicalForm.keyOrder = "as-given-repeated-key-kept"; },
    "se quita un número no portable": (d) => { d.data.canonicalForm.notPortable.pop(); },
    "message en unidades UTF-16": (d) => { d.rpc.error.messageLength.unit = "utf16-code-units"; },
    "message de 1001": (d) => { d.rpc.error.messageLength.max = 1001; },
    "el id citado sin tope": (d) => { d.rpc.envelope.errorFallbackWhenRequestUnreadable.validIdMaxCodePoints = 256; },
    "el id citado con controles": (d) => { d.rpc.envelope.errorFallbackWhenRequestUnreadable.validIdAllowsControlCharacters = true; },
    "la excepción de la comprobación 4 ancha": (d) => { d.rpc.envelope.errorFallbackWhenRequestUnreadable.callerExemptedFromCheck4.only = "any-error-with-tenantid-system"; },
    "la excepción sin la comprobación 3": (d) => { d.rpc.envelope.errorFallbackWhenRequestUnreadable.callerExemptedFromCheck4.checksStillApplied = ["error-data-fits-error-schema"]; },
  };
  const base = fallosDeAjustes(p);
  if (base.length) fallos.push(`autocomprobación: fallosDeAjustes ya falla sobre el protocol.json real (${base.length}); no se puede autocomprobar`);
  else {
    for (const [nombre, estropea] of Object.entries(delDocumento)) {
      const copia = structuredClone(p);
      estropea(copia);
      if (!fallosDeAjustes(copia).length) fallos.push(`autocomprobación: la mutación «${nombre}» NO la detecta fallosDeAjustes`);
    }
    mutacionesAjustes = Object.keys(delDocumento).length;
  }
}

if (fallos.length) {
  console.error(`\n✗ ${fallos.length} problema(s) en protocol.json\n`);
  for (const f of fallos) console.error(`  ✗ ${f}`);
  process.exit(1);
}

console.log(`✓ protocol.json coherente (${CASES.length} patrones + invariantes numéricas, de códigos, de capacidades y de estado · autocomprobación de estado: ${mutacionesEstado} mutaciones del documento y ${mutacionesAcl} de la ACL y su generador · de rpc: ${mutacionesRpc} mutaciones del documento y ${mutacionesAclRpc} de su ACL y su generador · del borde: ${mutacionesEdge} mutaciones del documento, del analizador y del generador · de los ajustes: ${mutacionesAjustes} mutaciones, todas detectadas)`);
