# GitHub Copilot — flux Event Protocol v1

Fuente canónica: [`AGENTS.md`](../AGENTS.md). Spec completa: [`llms-full.txt`](../llms-full.txt).
Constantes verificables: [`protocol.json`](../protocol.json).

Este repositorio define un **protocolo de eventos**, no una librería. Al sugerir
código que publique o consuma eventos, aplica estas reglas:

## Naming

Subject: `<dominio>.<agregado>.v<major>.<evento>` — 4 tokens, minúsculas,
`kebab-case`, dominio plural, agregado singular, evento en **pasado**.

```
✅ pedidos.pedido.v1.creado          ❌ pedidos.crear-pedido
✅ facturacion.factura.v2.emitida    ❌ Pedidos.Pedido.V1.Creado
```

Nunca sugieras `…actualizado`: no dice qué cambió. Nombra el hecho concreto
(`…direccion-envio-cambiada`).

## Envelope

CloudEvents 1.0. **El SDK rellena `id`, `source`, `time`, `specversion`, `type`,
`dataschema`, `correlationid`, `causationid`, `producerversion` y `traceparent`.**
No sugieras código que los asigne manualmente. El desarrollador solo escribe subject,
`data` y opcionalmente `aggregateId`.

`data`: objeto JSON en la raíz, claves `camelCase`, dinero en enteros de unidad
mínima (`totalCents`) más ISO 4217, fechas RFC 3339 UTC, sin `null` para "ausente".

## Consumo

Entrega **at-least-once**: los duplicados llegan. Todo handler que sugieras debe ser
idempotente — comprobación del `event.id` contra una tabla de procesados en la misma
transacción, u operación naturalmente idempotente, o `Idempotency-Key` derivada del
`id` del evento.

Ack **explícito** siempre. Nunca auto-ack.

No asumas orden: usa `aggregateVersion` y `WHERE aggregate_version < $n`.

## Errores

`RETRYABLE` (timeout, ECONNRESET, HTTP 429/502/503/504) → `nak` con backoff.
`PERMANENT` (schema inválido, regla de negocio, HTTP 400/403/404/422) → `term()` + DLQ
inmediato, sin reintentos. `POISON` (JSON malformado) → `term()` + DLQ + alerta.
Default: **PERMANENT**. Nunca sugieras un `catch` que trague el error en silencio.

## Seguridad

Sin PII en `data` — referencias (`clienteId`), no valores (`email`, `dni`). Nunca
sugieras credenciales `.creds`/`.nk` en ficheros versionados.

## Trampas que no producen error

- `subject` de CloudEvents = id del agregado, no el subject de NATS.
- Nombres de stream y durable consumer de NATS no admiten puntos.
- `duplicate_window` deduplica publicaciones, no reentregas de consumo.
- Subjects de NATS son case-sensitive.
