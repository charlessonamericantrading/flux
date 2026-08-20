/**
 * Propagación automática de contexto entre eventos.
 * Contrato normativo: specification/01-envelope.md §5
 *
 * Si un desarrollador tiene que rellenar `correlationid` a mano, el SDK ha fallado en
 * su único trabajo. `AsyncLocalStorage` permite que un `publish()` en cualquier punto
 * de la pila de un handler herede el contexto del evento entrante, sin que nadie
 * tenga que pasarlo por parámetro.
 */

import { AsyncLocalStorage } from "node:async_hooks";
import type { FluxEvent } from "./envelope.js";

export interface EventContext {
  readonly correlationid: string;
  /** `id` del evento en curso — pasa a ser el `causationid` de lo que se publique. */
  readonly causationid: string;
  readonly tenantid: string;
  readonly traceparent?: string;
  readonly tracestate?: string;
}

const storage = new AsyncLocalStorage<EventContext>();

export function currentContext(): EventContext | undefined {
  return storage.getStore();
}

export function runWithContext<T>(ctx: EventContext, fn: () => T): T {
  return storage.run(ctx, fn);
}

export function contextFromEvent(event: FluxEvent): EventContext {
  return {
    correlationid: event.correlationid,
    causationid: event.id,
    tenantid: event.tenantid,
    ...(event.traceparent !== undefined && { traceparent: event.traceparent }),
    ...(event.tracestate !== undefined && { tracestate: event.tracestate }),
  };
}

/**
 * Lee `traceparent` de un span de OpenTelemetry activo, si lo hay.
 *
 * Se resuelve por importación dinámica y con fallo silencioso a propósito: flux no
 * depende de OpenTelemetry, pero lo aprovecha cuando está presente. Una dependencia
 * dura obligaría a instalarlo a todo servicio que use el SDK.
 */
export async function activeTraceparent(): Promise<string | undefined> {
  try {
    const otel = await import("@opentelemetry/api" as string).catch(() => null);
    if (!otel) return undefined;
    const span = otel.trace?.getActiveSpan?.();
    if (!span) return undefined;
    const c = span.spanContext();
    if (!c?.traceId || !c?.spanId) return undefined;
    const flags = (c.traceFlags ?? 0).toString(16).padStart(2, "0");
    return `00-${c.traceId}-${c.spanId}-${flags}`;
  } catch {
    return undefined;
  }
}
