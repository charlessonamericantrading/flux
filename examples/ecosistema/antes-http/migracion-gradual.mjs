/**
 * La migración gradual (docs/migracion.md §4): mientras haya un solo consumidor que dependa del webhook o del polling, el productor hace las DOS cosas.
 * Funciona (el test lo ejecuta con el `avisarAltaDeCliente` de verdad y un bus de mentira que solo cuenta lo que se le publica: la API real es la
 * del ejemplo).
 */

import { avisarAltaDeCliente } from "./http.mjs";

// fragmento:doble-publicacion
// El CRM, durante la migración: el evento NUEVO y el webhook VIEJO, a la vez. El orden importa: primero lo que va a quedarse.
export async function registrarCliente({ bus, suscriptoresHttp, cliente }) {
  await bus.publish("clientes.cliente.v1.registrado", { clienteId: cliente.clienteId, aggregateVersion: 1, plan: cliente.plan }, { aggregateId: cliente.clienteId });
  await avisarAltaDeCliente(suscriptoresHttp, cliente);
}
// /fragmento:doble-publicacion

// fragmento:apagar-el-polling
// Cuántas peticiones recibe el endpoint viejo, por quién y cuándo fue la última: cuando esta cuenta lleva una semana a cero, se borra.
export function contarUso(servirCambios) {
  const uso = new Map();
  const medido = (req, res) => {
    const quien = req.headers["x-forwarded-for"] ?? req.socket.remoteAddress;
    uso.set(quien, { peticiones: (uso.get(quien)?.peticiones ?? 0) + 1, ultima: new Date().toISOString() });
    return servirCambios(req, res);
  };
  return { medido, uso };
}
// /fragmento:apagar-el-polling
