#!/usr/bin/env node
/**
 * El ejemplo del README, VIVO: un nats-server real con las ACLs que genera `gen-acl.mjs` para `services.json` (tres clientes de borde
 * `"via": "gateway"` y tres servicios nativos), los streams de `flux provision`, el gateway con `gateway.json`, un receptor HTTP que
 * verifica la firma de las entregas, y los servicios nativos de siempre (precios-api responde, contabilidad-api consume, inventario-api
 * publica un ajuste cada 15 s). Imprime lo que hay que exportar para ejecutar `demo.sh` —el script de curl— desde otra terminal.
 *
 *   (cd sdk-node && npm ci && npm run build) && (cd conformance && npm ci) && (cd cli && npm ci) && (cd gateway && npm ci)
 *   NATS_SERVER=/ruta/a/nats-server node gateway/examples/ejemplo.mjs
 *
 * Es el MISMO entorno que usan las pruebas de extremo a extremo (`gateway/test/helpers/entorno.mjs`): lo que ves aquí es lo que se prueba.
 * Para parar: Ctrl-C.
 */

import { levantarTodo, SECRETOS } from "../test/helpers/entorno.mjs";

const e = await levantarTodo();
const url = e.url;

// ── los servicios NATIVOS del ecosistema ──────────────────────────────────────────────────────────────────────────────────────
const precios = await e.nativo("precios-api");
await precios.respond("precios.tarifa.v1.calcular", async (req) => ({ totalCents: (req.cantidad ?? 1) * 3300, moneda: "EUR", sku: req.sku }));

const contabilidad = await e.nativo("contabilidad-api");
const vistos = new Set();
for (const subject of ["pagos.cobro.v1.recibido", "pedidos.pedido.v1.creado"]) {
  await contabilidad.subscribe(subject, async (ev) => {
    const nuevo = !vistos.has(ev.id);
    vistos.add(ev.id);
    console.log(`  [contabilidad-api] ${subject} id=${ev.id} ${nuevo ? "→ UN efecto" : "→ duplicado: ya aplicado, no se repite"}`);
  });
}

const inventario = await e.nativo("inventario-api");
let n = 0;
const publicar = () => inventario.publish("inventario.stock.v1.ajustado", { sku: `sku-${++n}`, delta: n, aggregateVersion: 1 }, { aggregateId: `sku-${n}` }).catch((x) => console.error(x.message));
const periodico = setInterval(publicar, 15_000);
publicar();

// ── lo que recibe el sistema HTTP de la tienda (entregas firmadas) ─────────────────────────────────────────────────────────
let impresas = 0;
const mirar = setInterval(() => {
  for (; impresas < e.receptor.peticiones.length; impresas++) {
    const p = e.receptor.peticiones[impresas];
    console.log(`  [tienda.example.com] POST ${p.url} Idempotency-Key=${p.idempotencyKey} firma ${p.verificacion.ok ? "VERIFICA" : `NO verifica (${p.verificacion.motivo})`}`);
  }
}, 300);

console.log(`
✓ gateway escuchando en ${url}

Desde otra terminal (el script de curl de gateway/examples/demo.sh):

  export GATEWAY_URL=${url}
  export TOKEN_TIENDA=${e.tokens["tienda-web"]}
  export TOKEN_CATALOGO=${e.tokens["catalogo-web"]}
  export PASARELA_SECRETO=${SECRETOS.PASARELA_HOOK}
  bash gateway/examples/demo.sh

(Los tokens son efímeros: se generan al arrancar y no se guardan en ningún sitio. Los secretos de los webhooks y de las entregas son valores de
prueba PÚBLICOS, los de gateway/test/helpers/entorno.mjs: no los uses en ningún otro sitio.)
Ctrl-C para parar.
`);

const parar = async () => {
  clearInterval(periodico);
  clearInterval(mirar);
  await Promise.allSettled([precios.close(), contabilidad.close(), inventario.close()]);
  await e.parar();
  process.exit(0);
};
process.once("SIGINT", parar);
process.once("SIGTERM", parar);
