#!/usr/bin/env node
/**
 * Runner de la suite de conformidad. Necesita un NATS con JetStream corriendo.
 *
 *   docker compose up -d
 *   node conformance/run.mjs
 *   node conformance/run.mjs --url nats://otro:4222
 *
 * Los casos viven en `cases/*.json` como DATOS, no como tests de un lenguaje: es lo
 * que impide que el SDK de Node se convierta en la spec de facto y los demás lo
 * persigan.
 *
 * Usa el cliente de NATS directamente, sin pasar por ningún SDK de flux. Un runner
 * construido sobre un SDK heredaría sus suposiciones, y son exactamente las
 * suposiciones lo que hay que verificar.
 */

import { readFile, readdir } from "node:fs/promises";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { connect } from "@nats-io/transport-node";
import {
  jetstream,
  jetstreamManager,
  AckPolicy,
  DiscardPolicy,
  RetentionPolicy,
  StorageType,
} from "@nats-io/jetstream";

const HERE = dirname(fileURLToPath(import.meta.url));
const url =
  process.argv.includes("--url")
    ? process.argv[process.argv.indexOf("--url") + 1]
    : (process.env.NATS_URL ?? "nats://127.0.0.1:4222");

const PREFIX = "conftest";
const STREAM = "EVT_CONFTEST";
const DLQ_STREAM = "DLQ_CONFTEST";

const results = [];
const enc = new TextEncoder();

function record(id, ok, detail, spec) {
  results.push({ id, ok, detail, spec });
  const mark = ok ? "\x1b[32m✓\x1b[0m" : "\x1b[31m✗\x1b[0m";
  console.log(`  ${mark} ${id}${detail ? `\n      ${detail}` : ""}`);
}

/** Ejecuta `fn`, esperando que lance. Devuelve el mensaje de error. */
async function expectThrows(fn) {
  try {
    await fn();
    return null;
  } catch (e) {
    return e?.message ?? String(e);
  }
}

async function resetStreams(jsm) {
  for (const s of [STREAM, DLQ_STREAM, "EVT_CONFTEST2"]) {
    try {
      await jsm.streams.delete(s);
    } catch {
      /* no existía */
    }
  }
}

async function addStream(jsm, name, subjects, extra = {}) {
  return jsm.streams.add({
    name,
    subjects,
    storage: StorageType.File,
    retention: RetentionPolicy.Limits,
    discard: DiscardPolicy.Old,
    ...extra,
  });
}

const count = async (jsm, name) => (await jsm.streams.info(name)).state.messages;

// ─── Casos ───────────────────────────────────────────────────────────────────

async function brokerInvariants(nc, js, jsm) {
  console.log("\n\x1b[1mbroker-invariants\x1b[0m  (02-naming.md, 03-delivery.md)");

  // Los nombres de stream rechazan puntos → justifica EVT_PEDIDOS
  const streamErr = await expectThrows(() =>
    addStream(jsm, "EVT.CONFTEST", [`${PREFIX}.>`]),
  );
  record(
    "stream-name-rejects-dots",
    streamErr !== null && /cannot contain|valid stream name|invalid/i.test(streamErr),
    streamErr ?? "ACEPTADO — la spec se equivoca",
    "02-naming.md §3",
  );

  await addStream(jsm, STREAM, [`${PREFIX}.>`], {
    duplicate_window: 120 * 1e9,
  });
  await addStream(jsm, DLQ_STREAM, [`dlq.${PREFIX}.>`]);

  // Los durable consumers rechazan puntos → justifica svc__subject_con_guiones
  const durErr = await expectThrows(() =>
    jsm.consumers.add(STREAM, {
      durable_name: "facturacion-api.pedidos",
      ack_policy: AckPolicy.Explicit,
    }),
  );
  record(
    "durable-name-rejects-dots",
    durErr !== null && /durable name|invalid/i.test(durErr),
    durErr ?? "ACEPTADO — la spec se equivoca",
    "02-naming.md §4",
  );

  // ...y aceptan el esquema que la spec propone
  let durOk = true;
  let durMsg = "";
  try {
    await jsm.consumers.add(STREAM, {
      durable_name: `facturacion-api__${PREFIX}_pedido_v1_creado`,
      ack_policy: AckPolicy.Explicit,
    });
  } catch (e) {
    durOk = false;
    durMsg = e.message;
  }
  record("durable-name-accepts-underscores", durOk, durMsg, "02-naming.md §4");

  // Caso NEGATIVO: un sufijo .dlq SÍ lo captura el stream principal.
  // Es la razón entera de que la DLQ vaya prefijada.
  const beforeSuffix = await count(jsm, STREAM);
  await js.publish(`${PREFIX}.pedido.v1.creado.dlq`, enc.encode("muerto"));
  const afterSuffix = await count(jsm, STREAM);
  record(
    "dlq-suffix-captured-by-main-stream",
    afterSuffix - beforeSuffix === 1,
    `el stream principal capturó ${afterSuffix - beforeSuffix} mensaje(s) — por eso la DLQ va PREFIJADA`,
    "02-naming.md §3.1",
  );

  // Caso POSITIVO: el prefijo produce espacios disjuntos.
  const beforeMain = await count(jsm, STREAM);
  const beforeDlq = await count(jsm, DLQ_STREAM);
  await js.publish(`dlq.${PREFIX}.pedido.v1.creado`, enc.encode("muerto"));
  const addedMain = (await count(jsm, STREAM)) - beforeMain;
  const addedDlq = (await count(jsm, DLQ_STREAM)) - beforeDlq;
  record(
    "dlq-prefix-disjoint",
    addedMain === 0 && addedDlq === 1,
    `principal +${addedMain}, dlq +${addedDlq}`,
    "02-naming.md §3.1",
  );

  // Nats-Msg-Id deduplica PUBLICACIONES (no reentregas de consumo)
  const beforeDupe = await count(jsm, STREAM);
  const msgID = "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55";
  for (let i = 0; i < 3; i++) {
    await js.publish(`${PREFIX}.pedido.v1.creado`, enc.encode("a"), { msgID });
  }
  const added = (await count(jsm, STREAM)) - beforeDupe;
  record(
    "nats-msg-id-dedupes-publishes",
    added === 1,
    `3 publicaciones con el mismo Nats-Msg-Id → ${added} mensaje(s)`,
    "03-delivery.md §3",
  );

  // Los subjects son case-sensitive. El matiz importa:
  //   - publish de CORE NATS   → silencioso, el evento se evapora sin error
  //   - publish de JETSTREAM   → error "no stream matched"
  // De ahí un requisito del protocolo: publicar SIEMPRE por JetStream, nunca por
  // core. Es lo que convierte una errata de mayúsculas en un fallo visible.
  const beforeCase = await count(jsm, STREAM);
  nc.publish(`Conftest.pedido.v1.creado`, enc.encode("core"));
  await nc.flush();
  const addedCore = (await count(jsm, STREAM)) - beforeCase;

  let jsThrew = null;
  try {
    await js.publish(`Conftest.pedido.v1.creado`, enc.encode("jetstream"));
  } catch (e) {
    jsThrew = e.message;
  }

  record(
    "core-publish-to-wrong-case-is-silent",
    addedCore === 0,
    `core publish: sin error, ${addedCore} mensajes almacenados — el evento se evapora`,
    "02-naming.md §1.1",
  );
  record(
    "jetstream-publish-to-wrong-case-errors",
    jsThrew !== null,
    jsThrew
      ? `jetstream publish sí falla: "${jsThrew}" — por eso el SDK publica siempre por JetStream`
      : "JetStream tampoco avisó — la errata sería invisible",
    "02-naming.md §1.1",
  );
}

async function consumerConfig(jsm) {
  console.log("\n\x1b[1mconsumer-config\x1b[0m  (03-delivery.md §2, §2.1)");

  const s = (n) => n * 1e9;

  // El contraejemplo: la config que la spec tenía ANTES de verificarse.
  const bad = {
    durable_name: "conf__counterexample",
    ack_policy: AckPolicy.Explicit,
    ack_wait: s(30),
    max_deliver: 6,
    backoff: [s(1), s(5), s(30), s(120), s(600)],
  };
  const badEff = (await jsm.consumers.add(STREAM, bad)).config;
  record(
    "counterexample-ack-wait-silently-overwritten",
    badEff.ack_wait !== bad.ack_wait && badEff.ack_wait === bad.backoff[0],
    `solicitado ack_wait ${bad.ack_wait / 1e9}s → efectivo ${badEff.ack_wait / 1e9}s ` +
      `(= backoff[0]). El servidor NO devolvió error.`,
    "03-delivery.md §2.1",
  );

  // La config canónica corregida.
  const good = {
    durable_name: "conf__consumer_config",
    ack_policy: AckPolicy.Explicit,
    ack_wait: s(30),
    max_deliver: 6,
    max_ack_pending: 256,
    backoff: [s(30), s(60), s(300), s(900), s(1800)],
  };
  const eff = (await jsm.consumers.add(STREAM, good)).config;

  record(
    "ack-wait-preserved",
    eff.ack_wait === good.ack_wait,
    `${eff.ack_wait / 1e9}s`,
    "03-delivery.md §2.1",
  );
  record(
    "ack-wait-equals-backoff-zero",
    eff.ack_wait === eff.backoff[0],
    `ack_wait ${eff.ack_wait / 1e9}s == backoff[0] ${eff.backoff[0] / 1e9}s`,
    "03-delivery.md §2.1",
  );
  record(
    "backoff-count-matches-retries",
    eff.backoff.length === eff.max_deliver - 1,
    `${eff.backoff.length} backoffs + 1 entrega inicial = max_deliver ${eff.max_deliver}`,
    "03-delivery.md §2",
  );
  record(
    "handler-budget-sane",
    eff.backoff[0] >= s(30),
    `backoff[0] = ${eff.backoff[0] / 1e9}s de presupuesto para el handler`,
    "03-delivery.md §2.1",
  );

  const total = eff.backoff.reduce((a, b) => a + b, 0) / 1e9;
  record(
    "time-to-dlq-documented",
    Math.abs(total - 3090) < 1,
    `${Math.floor(total / 60)}m ${total % 60}s hasta la DLQ (protocol.json dice 3090s)`,
    "03-delivery.md §2",
  );
}

// ─── Main ────────────────────────────────────────────────────────────────────

console.log(`\x1b[1mflux — suite de conformidad\x1b[0m`);
console.log(`servidor: ${url}`);

let nc;
try {
  nc = await connect({ servers: url, name: "flux-conformance" });
} catch (e) {
  console.error(
    `\n\x1b[31mNo se pudo conectar a ${url}\x1b[0m\n` +
      `  ${e.message}\n\n` +
      `  Levanta un NATS con JetStream:\n` +
      `    docker compose up -d\n` +
      `  o descarga el binario: https://github.com/nats-io/nats-server/releases\n` +
      `    nats-server -js -sd ./data\n`,
  );
  process.exit(2);
}

const jsm = await jetstreamManager(nc);
const js = jetstream(nc);

try {
  await resetStreams(jsm);
  await brokerInvariants(nc, js, jsm);
  await consumerConfig(jsm);
} finally {
  await resetStreams(jsm);
  await nc.drain();
}

const failed = results.filter((r) => !r.ok);
console.log(
  `\n${results.length - failed.length}/${results.length} casos pasan` +
    (failed.length ? `\n\n\x1b[31mFallan:\x1b[0m` : ""),
);
for (const f of failed) console.log(`  ${f.id}  (${f.spec})\n    ${f.detail}`);

// Un caso que falla significa que el broker ya no se comporta como la spec asume.
// Eso no es un test roto: es una regla de la spec que hay que revisar.
process.exit(failed.length ? 1 : 0);
