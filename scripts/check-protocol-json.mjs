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

import {
  RESERVED_DOMAINS,
  SERVICE_KEYS,
  STATE_CONTRACT_RE,
  STATE_TENANT_MAX,
  STATE_TENANT_RE,
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
  if (!eq(deEstado, Object.keys(CODIGOS_ESTADO).sort())) f.push(`los códigos de la capacidad state son una lista CERRADA de seis; hay [${deEstado.join(", ")}]`);
  // El único POISON de estado es el de identidad: miente sobre su tenant o su clave.
  if (cs.STATE_SUBJECT_MISMATCH?.action !== "term+dlq+alert") f.push('errors.codes.STATE_SUBJECT_MISMATCH.action debe ser "term+dlq+alert" (es POISON)');
  if (cs.STATE_SUBJECT_MISMATCH?.neverApplied !== true) f.push("errors.codes.STATE_SUBJECT_MISMATCH.neverApplied debe ser true: la entrada no se aplica");
  if (!doc.capabilities?.definitions?.state) f.push("capabilities.definitions.state: falta la definición de la capacidad (con su evidencia)");
  else if (doc.capabilities.definitions.state.evidence !== "sdk-files") f.push('capabilities.definitions.state.evidence debe ser "sdk-files", como `security`');

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

  // La DLQ de estado: sin dato, sin firma, sin replay.
  const dq = st.dlq ?? {};
  if (!eq(dq.dataReplacedBy, ["key", "revision"]) || dq.dataNeverCopied !== true) f.push("state.dlq.dataReplacedBy debe ser [key, revision]: un dato personal borrado no puede seguir 90 días en la DLQ");
  if (dq.replay !== "forbidden" || dq.recovery !== "flux state resync") f.push("state.dlq: el estado no se reproduce desde la DLQ; la recuperación es flux state resync");
  if (dq.dropsSignature !== true || dq.dlqerrorNeverContainsDataValues !== true || dq.unreadableMessageBodyNeverCopied !== true) f.push("state.dlq: sin firma, sin valores en dlqerror y sin el cuerpo de un mensaje ilegible");
  if (!eq(dq.streamSubjectsWhenDomainHasNoState, ["dlq.<dominio>.>"])) f.push("state.dlq.streamSubjectsWhenDomainHasNoState: un dominio sin estado conserva el DLQ_<DOM> de la 1.0 (1.1 es aditiva)");

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
fallos.push(...fallosDeEstado(p));
fallos.push(...fallosDeAclDeEstado(p));
fallos.push(...fallosDeRpc(p));
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
    "un séptimo código de estado": (d) => { d.errors.codes.OTRO_CODIGO_DE_ESTADO = { class: "permanent", capability: "state" }; },
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
    "la excepción del observador sin el tenant fijado": (d) => { d.state.acl.observerDirectGet = "^\\$JS\\.API\\.DIRECT\\.GET\\.STATE_[A-Z0-9_]+\\.state\\.[a-z0-9]+(-[a-z0-9]+)*\\.[a-z0-9]+(-[a-z0-9]+)*\\.v[1-9][0-9]*\\.>$"; },
    "neverGrantedPattern ya no caza DIRECT.GET": (d) => { d.security.acl.neverGrantedPattern = d.security.acl.neverGrantedPattern.replace("|\\$JS\\.API\\.DIRECT\\.GET(\\..*)?", ""); },
    "streamInfo admite STATE_": (d) => { d.security.acl.permissionPatterns.streamInfo = "^\\$JS\\.API\\.STREAM\\.INFO\\.(EVT|DLQ|STATE)_[A-Z0-9_]+$"; },
    "ownerPublish admite state.>": (d) => { d.state.acl.ownerPublish = "^state\\.([a-z0-9]+(-[a-z0-9]+)*\\.)?>$"; },
    "el patrón de estado casa con el de dominio propio": (d) => { d.state.acl.ownerPublish = d.security.acl.permissionPatterns.ownDomain; },
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
    "el subject de rpc admite comodines": (d) => { d.rpc.subject.pattern = "^rpc\\.[a-z0-9*>-]+\\.[a-z0-9-]+\\.v[1-9][0-9]*\\.[a-z0-9-]+$"; },
    "el subject de rpc admite mayúsculas": (d) => { d.rpc.subject.pattern = d.rpc.subject.pattern.replaceAll("a-z", "A-Za-z"); },
    "la operación distinta de un subject de evento": (d) => { d.rpc.operation.pattern = "^[a-z]+\\.[a-z]+\\.v[1-9]\\.[a-z]+$"; },
    "la operación admite el prefijo rpc.": (d) => { d.rpc.operation.pattern = "^(rpc\\.)?[a-z0-9]+(-[a-z0-9]+)*\\.[a-z0-9]+(-[a-z0-9]+)*\\.v[1-9][0-9]*\\.[a-z0-9]+(-[a-z0-9]+)*$"; },
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

if (fallos.length) {
  console.error(`\n✗ ${fallos.length} problema(s) en protocol.json\n`);
  for (const f of fallos) console.error(`  ✗ ${f}`);
  process.exit(1);
}

console.log(`✓ protocol.json coherente (${CASES.length} patrones + invariantes numéricas, de códigos, de capacidades y de estado · autocomprobación de estado: ${mutacionesEstado} mutaciones del documento y ${mutacionesAcl} de la ACL y su generador · de rpc: ${mutacionesRpc} mutaciones del documento, todas detectadas)`);
