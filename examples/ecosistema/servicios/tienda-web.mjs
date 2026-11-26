#!/usr/bin/env node
/**
 * tienda-web — la tienda. OBSERVA el estado de `clientes` y PUBLICA `pedidos`; no llama a nadie.
 *
 *   ESTADO      `bus.state.watch("clientes.cliente.v1", …)`: al arrancar recibe la FOTO de todos los clientes y después cada cambio,
 *               por el mismo canal. Guarda una copia local en SU base de datos y lee de ella: no hay `GET` al CRM por cada visita ni
 *               un `cron` que pregunte «¿hay cambios?». Si el CRM cambia un cliente, la tienda se entera sola; si lo da de baja
 *               (lápida), la tienda DEBE borrar su copia (regla 14).
 *   EVENTO      publica `pedidos.pedido.v1.creado` (un hecho que pasó, con el cliente por REFERENCIA) y consume
 *               `facturacion.factura.v1.emitida` para saber que su pedido ya tiene factura.
 *
 * Sin `calls` en el manifiesto: el broker le DENIEGA llamar a una operación. Lo que necesita de un cliente lo tiene en la réplica.
 */

import path from "node:path";

import { abrirAlmacen } from "./almacen.mjs";
import { ejecutar, esPrincipal } from "./comun.mjs";

/**
 * Aplica una entrada de `state.watch` a la copia local. Es lo que se llama «`UPDATE … WHERE aggregate_version < $n`»: no asume orden ni
 * entregas únicas. Una versión que no supera a la ya vista —una reentrega, una entrada vieja— se descarta; y una lápida deja la versión
 * (sin dato personal) para que una entrada vieja rezagada no resucite al cliente borrado.
 *
 * @returns {"aplicada" | "borrada" | "obsoleta"}
 */
export function aplicarEntrada(db, entrada) {
  return db.transaccion((d) => {
    if (entrada.aggregateVersion <= (d.versiones[entrada.key] ?? 0)) return "obsoleta";
    d.versiones[entrada.key] = entrada.aggregateVersion;
    if (entrada.deleted) {
      delete d.clientes[entrada.key];
      return "borrada";
    }
    const { nombre, email, plan, puntos } = entrada.data;
    d.clientes[entrada.key] = { nombre, email, plan, puntos, aggregateVersion: entrada.aggregateVersion };
    return "aplicada";
  });
}

/**
 * Guarda el aviso de «factura emitida» de un pedido. Operación NATURALMENTE idempotente (la clave es el pedido y solo avanza la versión):
 * recibirlo dos veces, o desordenado, deja lo mismo — la estrategia B de AGENTS.md §4, la mejor cuando se puede.
 */
export function aplicarFactura(db, factura) {
  return db.transaccion((d) => {
    const actual = d.facturas[factura.pedidoId];
    if (actual && actual.aggregateVersion >= factura.aggregateVersion) return "obsoleta";
    d.facturas[factura.pedidoId] = { facturaId: factura.facturaId, numero: factura.numero, totalCents: factura.totalCents, aggregateVersion: factura.aggregateVersion };
    return "aplicada";
  });
}

export async function iniciar({ bus, cfg, registro }) {
  const db = abrirAlmacen(path.join(cfg.datos, "tienda-web.json"), { clientes: {}, versiones: {}, facturas: {} });

  // fragmento:tienda-watch
  await bus.state.watch(
    "clientes.cliente.v1",
    async (entrada) => {
      registro({ evento: `cliente-${aplicarEntrada(db, entrada)}`, clienteId: entrada.key, aggregateVersion: entrada.aggregateVersion });
    },
    {
      tenantId: cfg.tenant,
      // Una vez, al terminar la foto inicial: a partir de aquí la réplica local ya puede contestar.
      onSynced: () => registro({ evento: "foto-inicial", clientes: Object.keys(db.leer().clientes).length }),
    },
  );
  // /fragmento:tienda-watch

  // fragmento:tienda-aviso
  await bus.subscribe("facturacion.factura.v1.emitida", async (evento) => {
    registro({ evento: `factura-${aplicarFactura(db, evento.data)}`, pedidoId: evento.data.pedidoId, eventoId: evento.id });
  });
  // /fragmento:tienda-aviso

  const comandos = {
    /** El «botón comprar»: el cliente se conoce por la RÉPLICA local, sin llamar al CRM. */
    async "crear-pedido"({ pedidoId, clienteId, lineas }) {
      if (!db.leer().clientes[clienteId]) {
        throw Object.assign(new Error("el cliente no está en la réplica local"), { code: "CLIENTE_DESCONOCIDO" });
      }
      const totalCents = lineas.reduce((suma, l) => suma + l.cantidad * l.precioUnitarioCents, 0);
      // fragmento:tienda-pedido
      const evento = await bus.publish(
        "pedidos.pedido.v1.creado",
        { pedidoId, clienteId, aggregateVersion: 1, totalCents, moneda: "EUR", lineas },
        { aggregateId: pedidoId },
      );
      // /fragmento:tienda-pedido
      return { pedidoId, eventoId: evento.id };
    },

    /**
     * Una lectura PUNTUAL del valor actual, directamente del stream (`DIRECT.GET`): lo que antes era `GET /clientes/123`. Para lo que se lee
     * de continuo está la réplica local de arriba; esto es para el dato que se necesita una vez. `null` si no existe o está borrado.
     */
    async "leer-cliente"({ clienteId }) {
      // fragmento:tienda-get
      const entrada = await bus.state.get("clientes.cliente.v1", clienteId);
      // /fragmento:tienda-get
      return { encontrado: entrada !== null, ...(entrada && { plan: entrada.data.plan, aggregateVersion: entrada.aggregateVersion }) };
    },
  };

  return { comandos, db };
}

if (esPrincipal(import.meta.url)) await ejecutar("tienda-web", iniciar);
