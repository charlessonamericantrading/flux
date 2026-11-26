#!/usr/bin/env node
/**
 * crm — DUEÑO de `clientes`. Usa las tres primitivas, cada una para lo que es:
 *
 *   ESTADO      `bus.state.put("clientes.cliente.v1", …)`: el valor ACTUAL de cada cliente, con su dato personal (clasificado
 *               `confidential`, con `x-pii` en el esquema y borrable). Los demás lo OBSERVAN (`state.watch`): nadie llama al CRM
 *               para saber cómo se llama un cliente, y no hay `GET /clientes/123` ni polling.
 *   EVENTO      `clientes.cliente.v1.registrado`: avisa de que algo PASÓ. Lleva solo la referencia `clienteId`: el nombre y el correo
 *               van por estado, nunca por un evento (regla 5).
 *   OPERACIÓN   `bus.respond(…)`: `calcular-descuento` es un CÁLCULO (las reglas de descuento viven aquí) y `sumar-puntos` una ORDEN
 *               (cambia datos del CRM y quien llama necesita la respuesta ya). Quien llama no sabe en qué proceso están: pregunta por
 *               el nombre.
 *
 * El `aggregateVersion` es el contador del CRM en SU base de datos y sobrevive a los borrados (`baja: true`, no `DELETE`): un cliente
 * dado de baja y recreado continúa desde la versión de su lápida, o `put` lo daría por obsoleto sin error.
 */

import path from "node:path";

import { PermanentError, RetryableError } from "@flux/sdk";

import { abrirAlmacen } from "./almacen.mjs";
import { ejecutar, esPrincipal } from "./comun.mjs";

const CONTRATO = "clientes.cliente.v1";
/** Porcentaje de descuento por plan. Un plan que este servicio no conoce (el enum es extensible) no da descuento: rama por defecto no destructiva. */
const DESCUENTO_POR_PLAN = { bronce: 0, plata: 5, oro: 10 };

export async function iniciar({ bus, cfg, registro }) {
  const db = abrirAlmacen(path.join(cfg.datos, "crm.json"), { clientes: {}, puntosAplicados: {} });
  const clienteDe = (id) => db.leer().clientes[id];

  /**
   * Publica el estado de un cliente (o su lápida si está de baja). Es IDEMPOTENTE: `put` es condicional y una versión que no avanza da
   * `stale`, que no es un error. Por eso se puede llamar cuantas veces haga falta: tras cada cambio, al arrancar y al repetir una orden.
   */
  // fragmento:crm-publicar-estado
  const publicarEstado = async (c) => {
    try {
      if (c.baja) return await bus.state.delete(CONTRATO, c.clienteId, { aggregateVersion: c.aggregateVersion });
      const { clienteId, nombre, email, plan, puntos, aggregateVersion } = c;
      return await bus.state.put(CONTRATO, clienteId, { clienteId, nombre, email, plan, puntos, aggregateVersion });
    } catch (e) {
      // La base de datos ya tiene el cambio y el broker no lo ha recibido: quien llama (o el arranque) debe volver a intentarlo.
      throw new RetryableError("el cambio está guardado pero no se pudo publicar su estado: reintenta", { code: "ESTADO_NO_PUBLICADO", cause: e });
    }
  };
  // /fragmento:crm-publicar-estado

  // Carga inicial: lo que el CRM tenía antes de arrancar se republica entero. Es lo que hace un dueño al migrar y tras cada reinicio.
  for (const c of Object.values(db.leer().clientes)) await publicarEstado(c);

  // ─── operaciones ───────────────────────────────────────────────────────────────────────────────────────────────

  // Un CÁLCULO: sin efectos, así que repetirlo da lo mismo y no hace falta deduplicar.
  // fragmento:crm-calculo
  await bus.respond("clientes.cliente.v1.calcular-descuento", async (peticion) => {
    const c = clienteDe(peticion.clienteId);
    if (!c || c.baja) throw new PermanentError("el cliente no existe", { code: "CLIENTE_DESCONOCIDO" });
    const pct = DESCUENTO_POR_PLAN[c.plan] ?? 0;
    // Enteros: nunca un float para dinero.
    return { descuentoCents: Math.floor((peticion.totalCents * pct) / 100), moneda: peticion.moneda, plan: c.plan };
  });
  // /fragmento:crm-calculo

  // Una ORDEN que NO es idempotente por naturaleza (`puntos = puntos + n`). Quien llama puede repetirla —un timeout es AMBIGUO: la
  // orden pudo ejecutarse— y una reentrega del evento que la provoca construye una petición NUEVA, con otro `id`. Por eso se
  // deduplica por la CLAVE DE NEGOCIO (`referencia`), que viaja en `data` y sobrevive a las dos cosas (11-request-reply.md §4.5).
  // fragmento:crm-orden
  await bus.respond("clientes.cliente.v1.sumar-puntos", async (peticion, ctx) => {
    const previa = db.leer().puntosAplicados[peticion.referencia];
    if (previa) {
      registro({ evento: "orden-repetida", operacion: ctx.operation, peticionId: ctx.id });
      await publicarEstado(clienteDe(previa.clienteId)); // por si la primera vez se guardó pero no llegó a publicarse
      return previa.respuesta;
    }
    const c = clienteDe(peticion.clienteId);
    if (!c || c.baja) throw new PermanentError("el cliente no existe", { code: "CLIENTE_DESCONOCIDO" });
    const respuesta = db.transaccion((d) => {
      const cliente = d.clientes[peticion.clienteId];
      cliente.puntos += peticion.puntos;
      cliente.aggregateVersion += 1;
      const r = { clienteId: cliente.clienteId, puntos: cliente.puntos, aggregateVersion: cliente.aggregateVersion };
      d.puntosAplicados[peticion.referencia] = { clienteId: cliente.clienteId, respuesta: r };
      return r;
    });
    registro({ evento: "puntos-sumados", clienteId: peticion.clienteId, peticionId: ctx.id });
    await publicarEstado(clienteDe(peticion.clienteId));
    return respuesta;
  });
  // /fragmento:crm-orden

  // ─── lo que hace la aplicación del CRM (aquí, comandos) ────────────────────────────────────────────────────────

  const comandos = {
    async "registrar-cliente"({ clienteId, nombre, email, plan = "bronce" }) {
      const alta = db.transaccion((d) => {
        if (d.clientes[clienteId]) return undefined;
        d.clientes[clienteId] = { clienteId, nombre, email, plan, puntos: 0, aggregateVersion: 1 };
        return d.clientes[clienteId];
      });
      if (!alta) return { creado: false };
      await publicarEstado(alta);
      // El EVENTO avisa; el dato personal ya ha ido por estado.
      // fragmento:crm-evento
      await bus.publish("clientes.cliente.v1.registrado", { clienteId, aggregateVersion: alta.aggregateVersion, plan }, { aggregateId: clienteId });
      // /fragmento:crm-evento
      return { creado: true, aggregateVersion: alta.aggregateVersion };
    },

    async "cambiar-plan"({ clienteId, plan }) {
      return cambiar(clienteId, (c) => { c.plan = plan; });
    },

    async "cambiar-email"({ clienteId, email }) {
      return cambiar(clienteId, (c) => { c.email = email; });
    },

    /** Derecho de supresión: el valor sale del stream (`delete` publica una lápida), y el contador de versión se conserva. */
    async "dar-de-baja"({ clienteId }) {
      const c = db.transaccion((d) => {
        const cliente = d.clientes[clienteId];
        if (!cliente) throw Object.assign(new Error("desconocido"), { code: "CLIENTE_DESCONOCIDO" });
        Object.assign(cliente, { baja: true, nombre: "", email: "", aggregateVersion: cliente.aggregateVersion + 1 });
        return { ...cliente };
      });
      await publicarEstado(c);
      return { aggregateVersion: c.aggregateVersion };
    },
  };

  async function cambiar(clienteId, aplica) {
    const c = db.transaccion((d) => {
      const cliente = d.clientes[clienteId];
      if (!cliente || cliente.baja) throw Object.assign(new Error("desconocido"), { code: "CLIENTE_DESCONOCIDO" });
      aplica(cliente);
      cliente.aggregateVersion += 1;
      return { ...cliente };
    });
    await publicarEstado(c);
    return { aggregateVersion: c.aggregateVersion };
  }

  return { comandos, db };
}

if (esPrincipal(import.meta.url)) await ejecutar("crm", iniciar);
