#!/usr/bin/env node
/**
 * facturacion — CONSUME `pedidos` y `pagos`, y LLAMA al CRM. Todo consumidor es idempotente y no asume orden:
 *
 *   `pedidos.pedido.v1.creado`   emite una factura con NÚMERO CORRELATIVO: el efecto que NO se puede repetir (una factura de más es un
 *                                hueco en la contabilidad). Estrategia A de AGENTS.md §4: una tabla de eventos procesados, en la MISMA
 *                                transacción que la factura. El broker entrega al menos una vez: la segunda entrega del mismo `id`
 *                                —tras una caída, un replay, un productor que reintenta— llega, y aquí es un no-op.
 *   `pagos.cobro.v1.recibido`    un cobro que llega por el webhook de un tercero. Naturalmente idempotente (estrategia B): el
 *                                `pagoId` es la clave, `INSERT … ON CONFLICT DO NOTHING`. Y NO asume que llegue después de la factura:
 *                                si llega antes, se guarda y la factura nace ya pagada.
 *   `clientes.cliente.v1.registrado`  abre la cuenta de facturación del cliente (también naturalmente idempotente).
 *
 * LLAMA al CRM (petición/respuesta, no HTTP): `calcular-descuento` (un cálculo) y `sumar-puntos` (una ORDEN: va con una clave de negocio,
 * `referencia`, porque este handler PUEDE ejecutarse dos veces para el mismo pedido y el CRM deduplica por ella).
 *
 * Publicar el aviso de factura emitida es el problema de la doble escritura: la factura se guarda en la base de datos Y se publica un
 * evento, y no hay transacción entre las dos. Solución: **bandeja de salida** (outbox). El evento a publicar se guarda en la MISMA
 * transacción que la factura, y se publica después; si el servicio muere entre las dos cosas, al reiniciar —o en la siguiente entrega—
 * se publica lo que quedó. Es «al menos una vez» para el aviso, y lo de arriba (consumidores idempotentes) lo hace inocuo.
 */

import path from "node:path";

import { abrirAlmacen } from "./almacen.mjs";
import { ejecutar, esPrincipal } from "./comun.mjs";

export async function iniciar({ bus, cfg, registro }) {
  const db = abrirAlmacen(path.join(cfg.datos, "facturacion.json"), {
    procesados: {},
    facturas: [],
    cobros: {},
    cuentas: {},
    salida: [],
    siguienteNumero: 1,
  });

  // ─── la bandeja de salida ─────────────────────────────────────────────────────────────────────────────────────

  let cola = Promise.resolve();
  /** Publica lo pendiente, de uno en uno y sin que dos llamadas se pisen. Un fallo lanza y lo pendiente se conserva. */
  const drenar = () => (cola = cola.catch(() => {}).then(async () => {
    for (const pendiente of [...db.leer().salida]) {
      await bus.publish(pendiente.subject, pendiente.data, { aggregateId: pendiente.aggregateId });
      db.transaccion((d) => { d.salida = d.salida.filter((x) => x.clave !== pendiente.clave); });
      registro({ evento: "aviso-publicado", subject: pendiente.subject, clave: pendiente.clave });
    }
  }));
  await drenar(); // lo que quedó sin publicar de una ejecución anterior

  // ─── pedidos: el efecto que no se puede repetir ───────────────────────────────────────────────────────────────

  await bus.subscribe("pedidos.pedido.v1.creado", async (evento, ctx) => {
    // fragmento:fact-idempotencia
    if (db.leer().procesados[evento.id]) {
      registro({ evento: "duplicado-ignorado", eventoId: evento.id, entrega: ctx.attempt });
      await drenar();
      return ctx.ack();
    }
    // /fragmento:fact-idempotencia
    const pedido = evento.data;

    // 1. Llamar al CRM, ANTES de escribir nada. Si falla (`RPC_UNAVAILABLE`, un timeout) el error ya viaja clasificado como RETRYABLE:
    //    el handler lo deja subir y el evento se reentrega. Y si el CRM responde `CLIENTE_DESCONOCIDO`, es PERMANENT: a la DLQ.
    // fragmento:fact-llamar
    const { descuentoCents } = await bus.request("clientes.cliente.v1.calcular-descuento", {
      clienteId: pedido.clienteId,
      totalCents: pedido.totalCents,
      moneda: pedido.moneda,
    });
    const totalCents = pedido.totalCents - descuentoCents;
    const puntos = Math.floor(totalCents / 100);
    // Una ORDEN: va con la clave de negocio. Si este handler muere después de esta línea, la reentrega la repite y el CRM no suma dos veces.
    if (puntos > 0) await bus.request("clientes.cliente.v1.sumar-puntos", { clienteId: pedido.clienteId, puntos, referencia: pedido.pedidoId });
    // /fragmento:fact-llamar

    // 2. UNA transacción: la factura (con su número), la marca «este evento ya está procesado» y el aviso por publicar.
    // fragmento:fact-transaccion
    const factura = db.transaccion((d) => {
      const numero = d.siguienteNumero++;
      const facturaId = `fac-${numero}`;
      const f = {
        facturaId, numero, pedidoId: pedido.pedidoId, clienteId: pedido.clienteId,
        subtotalCents: pedido.totalCents, descuentoCents, totalCents, moneda: pedido.moneda,
        estado: Object.values(d.cobros).some((c) => c.pedidoId === pedido.pedidoId) ? "pagada" : "emitida", // un cobro que llegó ANTES
      };
      d.facturas.push(f);
      d.procesados[evento.id] = new Date().toISOString();
      d.salida.push({
        clave: facturaId, subject: "facturacion.factura.v1.emitida", aggregateId: facturaId,
        data: {
          facturaId, pedidoId: f.pedidoId, clienteId: f.clienteId, aggregateVersion: 1, numero,
          subtotalCents: f.subtotalCents, descuentoCents, totalCents, moneda: f.moneda,
        },
      });
      return f;
    });
    // /fragmento:fact-transaccion
    registro({ evento: "factura-emitida", pedidoId: pedido.pedidoId, numero: factura.numero, eventoId: evento.id });

    // 3. Publicar. Si falla, lanza: el evento se reentrega, entra por «duplicado-ignorado» y `drenar()` lo reintenta.
    await drenar();
  });

  // ─── cobros: naturalmente idempotente, y sin asumir orden ─────────────────────────────────────────────────────

  // fragmento:fact-cobro
  await bus.subscribe("pagos.cobro.v1.recibido", async (evento) => {
    const cobro = evento.data;
    const nuevo = db.transaccion((d) => {
      if (d.cobros[cobro.pagoId]) return false; // el mismo cobro otra vez (el proveedor reenvía): no hay nada que hacer
      d.cobros[cobro.pagoId] = { pedidoId: cobro.pedidoId, importeCents: cobro.importeCents, moneda: cobro.moneda };
      const f = d.facturas.find((x) => x.pedidoId === cobro.pedidoId);
      if (f) f.estado = "pagada"; // si todavía no hay factura, nacerá pagada (ver arriba)
      return true;
    });
    registro({ evento: nuevo ? "cobro-registrado" : "cobro-repetido", pagoId: cobro.pagoId, eventoId: evento.id });
  });
  // /fragmento:fact-cobro

  // ─── altas: naturalmente idempotente ──────────────────────────────────────────────────────────────────────────

  // fragmento:fact-alta
  await bus.subscribe("clientes.cliente.v1.registrado", async (evento) => {
    db.transaccion((d) => { d.cuentas[evento.data.clienteId] ??= { abierta: evento.time }; });
    registro({ evento: "cuenta-abierta", clienteId: evento.data.clienteId });
  });
  // /fragmento:fact-alta

  return { db };
}

if (esPrincipal(import.meta.url)) await ejecutar("facturacion", iniciar);
