/**
 * Conectar con el SDK de flux de Node COMO lo hace un servicio real: su nkey, el inbox privado que fija el SDK, la CA
 * si hay TLS y nada más. Ni validación L3 ni firma (son opcionales y el banco mide el camino por defecto).
 */

import { sdkUrl } from "./comun.mjs";

let sdk;
export async function cargarSdk() {
  return (sdk ??= await import(sdkUrl()));
}

export async function conectarServicio({ url, caFile, servicio, semilla, tenantId = "acme" }) {
  const { connect } = await cargarSdk();
  return connect({
    servers: url,
    service: servicio,
    environment: "bench",
    version: "1.0.0",
    tenantId,
    // Sin validación L3 el SDK no sabe el `dataschema` de un estado ni de una operación: se le da la base (como los tests del SDK).
    schemaBaseUrl: "https://schemas.internal",
    credentials: { nkeySeed: semilla },
    ...(caFile && { tls: { caFile } }),
  });
}

/** Ejecuta `f(x)` sobre `xs` con a lo sumo `n` a la vez: abrir 1 000 conexiones de golpe es una tormenta de conexión. */
export async function conLimite(xs, n, f) {
  let i = 0;
  const trabajadores = Array.from({ length: Math.min(n, xs.length) }, async () => {
    while (i < xs.length) {
      const k = i++;
      await f(xs[k], k);
    }
  });
  await Promise.all(trabajadores);
}

let nats;
/**
 * La librería de NATS (`@nats-io/transport-node`) tal y como la usa el SDK de Node: la MISMA instancia que resuelve
 * `sdk-node/node_modules`. Para la línea «NATS crudo» del escenario C: separa lo que cuesta el transporte de lo que añade
 * el SDK de flux por encima (sobre, validaciones, identificadores…).
 */
export async function cargarNats() {
  if (!nats) {
    const { createRequire } = await import("node:module");
    const { pathToFileURL } = await import("node:url");
    const { RAIZ } = await import("./comun.mjs");
    const req = createRequire(`${RAIZ}/sdk-node/package.json`);
    nats = await import(pathToFileURL(req.resolve("@nats-io/transport-node")).href);
  }
  return nats;
}

/** Conexión CRUDA con los mismos permisos que el servicio: su nkey, su inbox privado y la CA si hay TLS. */
export async function conectarCrudo({ url, caFile, servicio, semilla }) {
  const { connect, nkeyAuthenticator } = await cargarNats();
  return connect({
    servers: url,
    name: `${servicio}@bench-crudo`,
    authenticator: nkeyAuthenticator(new TextEncoder().encode(semilla)),
    inboxPrefix: `_INBOX.${servicio}`,
    ...(caFile && { tls: { caFile } }),
  });
}
