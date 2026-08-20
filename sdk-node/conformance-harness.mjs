#!/usr/bin/env node
/**
 * Arnés de conformidad cruzada — SDK de Node.
 * Contrato: conformance/harness/README.md
 *
 * Lee UNA operación por stdin, escribe UN resultado por stdout, sale con 0 siempre.
 * Deliberadamente delgado: toda lógica aquí es lógica que no está en el SDK y que el
 * runner, por tanto, no verifica.
 */

import { buildEvent, serialize, parseEvent, toDlqEvent } from "./dist/envelope.js";
import { createSigner, createVerifier } from "./dist/signing.js";

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

const b64 = (bytes) => Buffer.from(bytes).toString("base64");

async function ejecutar(entrada) {
  switch (entrada.op) {
    case "build":
      return { ok: true, bytes: b64(serialize(construir(entrada.event))) };

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
