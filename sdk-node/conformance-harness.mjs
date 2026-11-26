#!/usr/bin/env node
/**
 * Arnés de conformidad cruzada — SDK de Node.
 * Contrato: conformance/harness/README.md
 *
 * Lee UNA operación por stdin, escribe UN resultado por stdout, sale con 0 siempre.
 * Deliberadamente delgado: toda lógica aquí es lógica que no está en el SDK y que el
 * runner, por tanto, no verifica.
 *
 * Es el arnés de REFERENCIA: su salida fija lo esperado. Además de `build`, `dlq`, `sign`,
 * `verify` y `parse`, implementa las cuatro operaciones de la capacidad `state` (`state-build`,
 * `state-tombstone`, `state-subject` y `state-dlq`) y las cinco de la capacidad `rpc` (`rpc-request`,
 * `rpc-reply`, `rpc-error`, `rpc-operation` y `rpc-timeout`: conformance/cases/cross-sdk-rpc-envelope.json),
 * que el runner solo ejecuta en los SDKs que las declaran en protocol.json. También `external-id` (el `id` determinista de un
 * hecho importado, 01-envelope.md §2.6.1): hoy solo la implementa este arnés y se integra en el cruzado más adelante.
 */

import { buildEvent, serialize, parseEvent, toDlqEvent } from "./dist/envelope.js";
import { externalEventId } from "./dist/external-id.js";
import { createSigner, createVerifier } from "./dist/signing.js";
import {
  buildStateDlqEvent,
  buildStateEvent,
  buildStateTombstone,
  stateSubject,
} from "./dist/state.js";
import {
  buildRpcError,
  buildRpcReply,
  buildRpcRequest,
  encodeRpcMessage,
  resolveRpcTimeout,
  rpcSubject,
} from "./dist/rpc.js";

const leerStdin = () =>
  new Promise((res) => {
    let s = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (d) => (s += d));
    process.stdin.on("end", () => res(s));
  });

/** El arnés NO rellena nada: todos los atributos vienen del vector, o no serían comparables. */
function construir(e) {
  return buildEvent({
    subject: e.subject,
    data: e.data,
    id: e.id,
    source: e.source,
    time: e.time,
    dataschema: e.dataschema,
    correlationid: e.correlationid,
    tenantid: e.tenantid,
    producerversion: e.producerversion,
    dataclassification: e.dataclassification,
    ...(e.aggregateId !== undefined && { aggregateId: e.aggregateId }),
    ...(e.causationid !== undefined && { causationid: e.causationid }),
    ...(e.partitionkey !== undefined && { partitionkey: e.partitionkey }),
    ...(e.traceparent !== undefined && { traceparent: e.traceparent }),
    ...(e.tracestate !== undefined && { tracestate: e.tracestate }),
  });
}

/**
 * Estado replicado (specification/10-state.md): lo mismo, el arnés NO rellena nada. `id` y `time`
 * vienen del vector. `dataschema` NO viene: lo deriva el SDK del contrato, que es parte de lo que
 * se compara.
 */
function construirEstado(e) {
  return {
    contract: e.contract,
    tenantId: e.tenantId,
    key: e.key,
    id: e.id,
    source: e.source,
    time: e.time,
    correlationid: e.correlationid,
    producerversion: e.producerversion,
    classification: e.classification,
    data: e.data,
    ...(e.causationid !== undefined && { causationid: e.causationid }),
    ...(e.traceparent !== undefined && { traceparent: e.traceparent }),
    ...(e.tracestate !== undefined && { tracestate: e.tracestate }),
  };
}

/**
 * Petición/respuesta (specification/11-request-reply.md §3.5): lo mismo, el arnés NO rellena nada. `id`, `time` y
 * `traceparent` vienen del vector. `dataschema` NO viene: lo deriva el SDK de la operación (es parte de lo que se compara).
 * La respuesta y el error HEREDAN de `request` (`correlationid`, `tenantid`, `subject`) y su `causationid` es `request.id`:
 * el `correlationid` de primer nivel que traen los vectores es redundante, y aquí no se usa.
 */
const construirReferencia = (r) => ({
  operation: r.operation,
  id: r.id,
  correlationid: r.correlationid,
  tenantid: r.tenantid,
  ...(r.subject !== undefined && { subject: r.subject }),
  ...(r.partitionkey !== undefined && { partitionkey: r.partitionkey }),
});

const b64 = (bytes) => Buffer.from(bytes).toString("base64");

async function ejecutar(entrada) {
  switch (entrada.op) {
    case "build":
      return { ok: true, bytes: b64(serialize(construir(entrada.event))) };

    // El `id` determinista de un hecho importado (specification/01-envelope.md §2.6.1; conformance/cases/cross-sdk-external-id.json).
    // Todavía NO está en conformance/harness/vectors.json: lo ejecuta, de momento, `sdk-node/test/external-id.test.ts` contra ese fichero.
    // `externalKey: { repeat, times }` se expande para no pegar 513 caracteres en el caso.
    case "external-id": {
      const e = entrada.input ?? entrada;
      const clave = e.externalKey !== null && typeof e.externalKey === "object" ? String(e.externalKey.repeat).repeat(e.externalKey.times) : e.externalKey;
      try {
        return { ok: true, id: externalEventId({ source: e.source, externalKey: clave, time: e.time }) };
      } catch {
        return { ok: false };
      }
    }

    case "dlq": {
      let ev = construir(entrada.event);
      if (entrada.signFirst && entrada.signing) {
        ev = createSigner(entrada.signing).sign(ev);
      }
      const d = entrada.dlq;
      const conDlq = toDlqEvent(ev, {
        reason: d.reason,
        attempts: d.attempts,
        consumer: d.consumer,
        error: d.error,
      });
      // `dlqtime` lo fija el vector: si lo pusiera el SDK, los bytes no serían
      // comparables entre ejecuciones, y mucho menos entre lenguajes.
      const { data, ...resto } = { ...conDlq, dlqtime: d.dlqtime };
      return { ok: true, bytes: b64(serialize({ ...resto, data })) };
    }

    case "sign": {
      const firmado = createSigner(entrada.signing).sign(construir(entrada.event));
      return { ok: true, bytes: b64(serialize(firmado)) };
    }

    case "verify": {
      const ev = parseEvent(new Uint8Array(Buffer.from(entrada.bytes, "base64")));
      const v = createVerifier({
        publicKeys: entrada.publicKeys,
        verify: entrada.mode ?? "require",
      });
      try {
        v.check(ev);
        return { ok: true };
      } catch (e) {
        return { ok: false, code: e.opts?.code ?? "VERIFY_FAILED" };
      }
    }

    case "parse":
      parseEvent(new Uint8Array(Buffer.from(entrada.bytes, "base64")));
      return { ok: true };

    // ─── Estado replicado (capacidad `state`) ───────────────────────────────────────────
    case "state-build":
      return { ok: true, bytes: b64(serialize(buildStateEvent(construirEstado(entrada)))) };

    case "state-tombstone":
      return {
        ok: true,
        bytes: b64(
          serialize(
            buildStateTombstone({
              contract: entrada.contract,
              tenantId: entrada.tenantId,
              key: entrada.key,
              id: entrada.id,
              source: entrada.source,
              time: entrada.time,
              correlationid: entrada.correlationid,
              producerversion: entrada.producerversion,
              aggregateVersion: entrada.aggregateVersion,
              ...(entrada.causationid !== undefined && { causationid: entrada.causationid }),
              ...(entrada.traceparent !== undefined && { traceparent: entrada.traceparent }),
              ...(entrada.tracestate !== undefined && { tracestate: entrada.tracestate }),
            }),
          ),
        ),
      };

    case "state-subject":
      return { ok: true, subject: stateSubject(entrada.contract, entrada.tenantId, entrada.key) };

    case "state-dlq": {
      const d = entrada.dlq;
      const ev = buildStateDlqEvent({
        original: construirEstado(entrada.original),
        revision: entrada.revision,
        // `dlqtime` lo fija el vector, como en `dlq`.
        dlq: { reason: d.reason, attempts: d.attempts, consumer: d.consumer, error: d.error, dlqtime: d.dlqtime },
      });
      return { ok: true, bytes: b64(serialize(ev)) };
    }

    // ─── Petición/respuesta (capacidad `rpc`) ───────────────────────────────────────────
    case "rpc-request":
      return {
        ok: true,
        bytes: b64(
          encodeRpcMessage(
            buildRpcRequest({
              operation: entrada.operation,
              tenantId: entrada.tenantId,
              id: entrada.id,
              source: entrada.source,
              time: entrada.time,
              correlationid: entrada.correlationid,
              producerversion: entrada.producerversion,
              classification: entrada.classification,
              data: entrada.data,
              ...(entrada.aggregateId !== undefined && { aggregateId: entrada.aggregateId }),
              ...(entrada.causationid !== undefined && { causationid: entrada.causationid }),
              ...(entrada.traceparent !== undefined && { traceparent: entrada.traceparent }),
              ...(entrada.tracestate !== undefined && { tracestate: entrada.tracestate }),
            }),
          ),
        ),
      };

    case "rpc-reply":
      return {
        ok: true,
        bytes: b64(
          encodeRpcMessage(
            buildRpcReply({
              request: construirReferencia(entrada.request),
              id: entrada.id,
              source: entrada.source,
              time: entrada.time,
              producerversion: entrada.producerversion,
              classification: entrada.classification,
              data: entrada.data,
              ...(entrada.traceparent !== undefined && { traceparent: entrada.traceparent }),
            }),
          ),
        ),
      };

    case "rpc-error":
      return {
        ok: true,
        bytes: b64(
          encodeRpcMessage(
            buildRpcError({
              request: construirReferencia(entrada.request),
              id: entrada.id,
              source: entrada.source,
              time: entrada.time,
              producerversion: entrada.producerversion,
              error: entrada.error,
              ...(entrada.traceparent !== undefined && { traceparent: entrada.traceparent }),
            }),
          ),
        ),
      };

    // El subject de NATS (`{ ok, subject }`) o el código estable del rechazo (`{ ok: false, code }`: INVALID_RPC_CALL o
    // RESERVED_DOMAIN). `rpcSubject` lanza con `code`, y el `catch` de abajo lo reporta.
    case "rpc-operation":
      return { ok: true, subject: rpcSubject(entrada.operation) };

    // Los milisegundos efectivos, o INVALID_RPC_CALL. `timeoutMs` ausente es 5000; `null` NO es «ausente»: es inválido.
    case "rpc-timeout":
      return { ok: true, effectiveMs: resolveRpcTimeout(entrada.timeoutMs) };

    default:
      return { ok: false, code: "UNSUPPORTED_OP", detail: entrada.op };
  }
}

const entrada = JSON.parse(await leerStdin());
let salida;
try {
  salida = await ejecutar(entrada);
} catch (e) {
  // Un fallo de la operación se REPORTA, no se propaga: exit != 0 significaría que el
  // arnés está roto, no que el caso falló.
  salida = { ok: false, code: e.opts?.code ?? e.name ?? "ERROR", detail: e.message };
}
process.stdout.write(JSON.stringify(salida));
