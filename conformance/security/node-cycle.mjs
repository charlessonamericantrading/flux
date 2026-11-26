#!/usr/bin/env node
/**
 * S10 — el SDK REAL de Node, de extremo a extremo, contra la ACL que genera `gen-acl.mjs`.
 *
 * No es un test: es el proceso que `conformance/security.mjs` lanza, gobernado SOLO por el
 * entorno, y que devuelve una línea con lo que vio. El runner comprueba después, desde el
 * broker, lo que de verdad quedó (acks, DLQ, violaciones en el log del servidor): lo que el SDK
 * diga de sí mismo no basta.
 *
 *   entra  FLUX_ACL_URL                 nats://… o tls://… del broker
 *          FLUX_ACL_CA                  CA que firma su certificado
 *          FLUX_ACL_PRODUCER/_SEED      servicio que publica y su semilla nkey
 *          FLUX_ACL_CONSUMER/_SEED      servicio que consume y su semilla nkey
 *          FLUX_ACL_SUBJECT             el subject que se publica y se consume
 *          FLUX_ACL_EVENTS              JSON: [{ caso, calls, data }]
 *   sale   FLUX_ACL_REPORT {json}
 *
 * El comportamiento del handler lo decide `caso`, igual que en el SDK de Rust: `ok` confirma,
 * `permanente` lanza un PermanentError (acaba en la DLQ) y `reintento` falla la primera entrega
 * con un RetryableError y acepta la segunda.
 *
 * Si el SDK necesita un permiso que el generador no concede, aquí falla, y eso es el hallazgo:
 * este fichero no parchea ni el SDK ni la ACL.
 */

import path from "node:path";
import { fileURLToPath } from "node:url";

const AQUI = path.dirname(fileURLToPath(import.meta.url));
const sdk = await import(path.join(AQUI, "..", "..", "sdk-node", "dist", "index.js"));

const env = (k) => {
  const v = process.env[k];
  if (!v) {
    console.error(`falta la variable de entorno ${k}`);
    process.exit(2);
  }
  return v;
};

const url = env("FLUX_ACL_URL");
const ca = env("FLUX_ACL_CA");
const subject = env("FLUX_ACL_SUBJECT");
const eventos = JSON.parse(env("FLUX_ACL_EVENTS"));
const esperadas = eventos.reduce((n, e) => n + (e.calls ?? 1), 0);

const informe = { sdk: "node", published: [], handlerCalls: [], errors: [], logs: [] };
const logger = {
  warn: (m) => informe.logs.push(`WARN ${m}`),
  error: (m) => informe.logs.push(`ERROR ${m}`),
};

const conectar = (servicio, semilla) =>
  sdk.connect({
    servers: url,
    service: servicio,
    environment: "test",
    version: "1.0.0",
    tenantId: "acme",
    schemaBaseUrl: "https://schemas.internal",
    credentials: { nkeySeed: semilla },
    tls: { caFile: ca },
    pendingPollMs: -1,
    logger,
  });

let productor;
let consumidor;
try {
  consumidor = await conectar(env("FLUX_ACL_CONSUMER"), env("FLUX_ACL_CONSUMER_SEED"));
  productor = await conectar(env("FLUX_ACL_PRODUCER"), env("FLUX_ACL_PRODUCER_SEED"));

  await consumidor.subscribe(subject, async (evento, ctx) => {
    const caso = evento.data.caso ?? "ok";
    informe.handlerCalls.push({ caso, attempt: ctx.attempt });
    if (caso === "permanente") {
      throw new sdk.PermanentError("pedido ya cancelado", { code: "PEDIDO_YA_CANCELADO" });
    }
    if (caso === "reintento" && ctx.attempt === 1) {
      throw new sdk.RetryableError("proveedor 503", { code: "HTTP_503", retryAfterMs: 300 });
    }
  });

  for (const [i, e] of eventos.entries()) {
    const publicado = await productor.publish(
      subject,
      { ...e.data, caso: e.caso, aggregateVersion: 1 },
      { aggregateId: e.data.pedidoId ?? `ped-${i}` },
    );
    informe.published.push({ caso: e.caso, id: publicado.id });
  }

  // Hasta que el handler haya visto todas las entregas esperadas (con un tope).
  const limite = Date.now() + 25_000;
  while (informe.handlerCalls.length < esperadas && Date.now() < limite) {
    await new Promise((r) => setTimeout(r, 50));
  }
  if (informe.handlerCalls.length < esperadas) {
    informe.errors.push(`el handler vio ${informe.handlerCalls.length} de ${esperadas} entregas en 25 s`);
  }
  // Un poco más para que los acks y la DLQ salgan antes de cerrar.
  await new Promise((r) => setTimeout(r, 500));
} catch (e) {
  informe.errors.push(`${e?.name ?? "Error"}: ${e?.message ?? e}`);
} finally {
  await productor?.close().catch(() => {});
  await consumidor?.close().catch(() => {});
}

console.log(`FLUX_ACL_REPORT ${JSON.stringify(informe)}`);
process.exit(informe.errors.length ? 1 : 0);
