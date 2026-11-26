/**
 * El gateway: junta la configuración, una conexión NATS POR CLIENTE, las entregas salientes y el servidor HTTP.
 * Implementación: specification/12-edge.md §3.
 *
 * «El gateway aloja servicios; no los suplanta»: cada cliente de borde es un servicio flux normal del manifiesto, con SUS
 * credenciales NATS y SUS permisos, y su conexión vive aquí dentro. Así `source`, los durables y la ACL son los suyos y el gateway
 * no necesita ningún permiso especial: lo que el cliente no tiene concedido lo deniega EL BROKER (A-150), aunque este código se
 * equivoque. Todo lo que se hace por un cliente se hace con la API del SDK: el gateway no escribe el sobre.
 */

import { readFileSync } from "node:fs";

import { connect } from "@flux/sdk";

import { crearAutenticador } from "./auth.mjs";
import { crearManejadorDeEntrega, crearManejadorDeEntregaDeEstado, reglaDeClasificacion } from "./entrega.mjs";
import { crearRegistro } from "./registro.mjs";
import { crearServidor } from "./servidor.mjs";

/** Las opciones de `connect()` de UN cliente. */
export function opcionesDeConexion(config, c, registro) {
  const bundle = config.validation === "strict" ? JSON.parse(readFileSync(config.bundleFile, "utf8")) : undefined;
  return {
    servers: config.nats.servers,
    service: c.service,
    environment: config.environment,
    version: config.version,
    tenantId: c.tenantId,
    schemaBaseUrl: config.schemaBaseUrl,
    credentials: c.credentials,
    ...(config.nats.tls && { tls: config.nats.tls }),
    ...(c.classification && { classification: c.classification }),
    // La entrega le dice al SDK cómo clasificar sus fallos (12-edge.md §4.4), incluido el presupuesto acotado.
    classifier: { rules: [reglaDeClasificacion] },
    ...(bundle && { validation: { mode: "strict", bundle } }),
    logger: registro.paraElSdk(),
  };
}

/**
 * Arranca el gateway. Falla ENTERO si un cliente no puede conectar o una entrega no puede suscribirse: un gateway a medias que
 * acepta peticiones de unos clientes y no de otros es peor que uno que no arranca.
 *
 * @param {object} config  la configuración normalizada (`cargarConfig`)
 * @param {{ registro?: ReturnType<typeof crearRegistro>, conectar?: Function }} [o]
 */
export async function iniciarGateway(config, { registro = crearRegistro(), conectar = connect } = {}) {
  const clientes = new Map();
  const suscripciones = [];

  const cerrarTodo = async () => {
    await Promise.allSettled(suscripciones.map((s) => s.unsubscribe()));
    await Promise.allSettled([...clientes.values()].map((c) => c.bus.close()));
  };

  try {
    // Una conexión NATS por cliente, en paralelo: cada una con su identidad. `allSettled` y no `all`: con `all`, el primer fallo
    // abandona el arranque mientras las demás conexiones siguen en vuelo, y las que terminan después NUNCA se cierran (una fuga que
    // encontró la prueba del arranque a medias). Se espera a TODAS, y si alguna falló se cierran las que sí conectaron.
    const resultados = await Promise.allSettled(
      config.clientes.map(async (c) => {
        const bus = await conectar(opcionesDeConexion(config, c, registro));
        clientes.set(c.service, { cliente: c, bus });
        registro.info("cliente conectado", { cliente: c.service, tenant: c.tenantId });
      }),
    );
    const fallo = resultados.find((r) => r.status === "rejected");
    if (fallo) throw fallo.reason;

    // Las entregas salientes: un consumidor por entrega, en la conexión de SU cliente.
    for (const { cliente, bus } of clientes.values()) {
      for (const e of cliente.deliveries) {
        const ca = e.caFile ? readFileSync(e.caFile) : undefined;
        if (e.kind === "event") {
          const manejador = crearManejadorDeEntrega({ entrega: e, registro, cliente: cliente.service, ...(ca && { ca }) });
          suscripciones.push(await bus.subscribe(e.subject, manejador));
          registro.info("entrega activa", { cliente: cliente.service, subject: e.subject, host: new URL(e.url).host });
        } else {
          const manejador = crearManejadorDeEntregaDeEstado({ entrega: e, registro, cliente: cliente.service, ...(ca && { ca }) });
          suscripciones.push(await bus.state.watch(e.contract, manejador, { tenantId: e.tenant }));
          registro.info("entrega de estado activa", { cliente: cliente.service, contrato: e.contract, tenant: e.tenant, host: new URL(e.url).host });
        }
      }
    }

    const autenticador = crearAutenticador(config.clientes.flatMap((c) => c.tokens));
    const hooks = new Map(config.clientes.flatMap((c) => c.hooks.map((h) => [h.nombre, { hook: h, cliente: c.service }])));
    const { servidor, parar, estado } = crearServidor({ config, clientes, autenticador, hooks, registro });

    await new Promise((resolve, reject) => {
      servidor.once("error", reject);
      servidor.listen(config.listen.port, config.listen.host, resolve);
    });
    const { port } = servidor.address();
    registro.info("gateway escuchando", { puerto: port, clientes: clientes.size });

    let cerrando;
    return {
      port,
      servidor,
      clientes,
      estado,
      /** La parada ordenada: HTTP primero (lo que vuela termina), después NATS (`drain`). Idempotente. */
      cerrar(plazoMs = config.shutdownTimeoutMs) {
        cerrando ??= (async () => {
          await parar(plazoMs);
          await cerrarTodo();
          registro.info("gateway parado");
        })();
        return cerrando;
      },
    };
  } catch (e) {
    await cerrarTodo();
    throw e;
  }
}
