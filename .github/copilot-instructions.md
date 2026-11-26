# GitHub Copilot — flux, protocolo de integración v1.1

Fuente canónica: [`AGENTS.md`](../AGENTS.md). Spec completa: [`llms-full.txt`](../llms-full.txt). Constantes verificables: [`protocol.json`](../protocol.json).
Un ecosistema que funciona: [`examples/ecosistema/`](../examples/ecosistema/). Migrar de APIs y webhooks: [`docs/migracion.md`](../docs/migracion.md).

Este repositorio define un **protocolo de integración**, no una librería: **eventos, estado y petición/respuesta**, más un gateway HTTP para el borde. Sustituye las APIs, los
webhooks y el polling entre sistemas propios. Este fichero resume AGENTS.md; si algo no está claro, manda AGENTS.md (`node scripts/check-agent-docs.mjs` comprueba que no divergen).

## ¿Qué primitiva uso?

- **Evento** (`bus.publish` / `bus.subscribe`): un **hecho** que pasó; cada interesado reacciona. Webhooks y polling de cambios.
- **Estado** (`bus.state.watch` / `get` / `put`): un **dato** que guarda otro sistema. `GET /clientes/123`, la carga inicial, una copia local al día.
- **Petición/respuesta** (`bus.request` / `bus.respond`): una **orden** o un **cálculo** con respuesta inmediata. **Nunca** para leer un dato guardado (eso es estado).
- **Evento + evento de resultado**, unidos por `correlationid`: el trabajo asíncrono que no tiene que esperarse.
- **Gateway** (HTTP solo en el borde): quien **no puede mantener una conexión NATS** (hosting compartido, `cron`, serverless, un tercero), y los webhooks de terceros.

Un contrato nuevo se crea con **`flux new event|state|rpc <nombre>`**: con su `$id` correcto y lo obligatorio, y el bundle regenerado. No sugieras escribir un esquema a mano.

## Reglas duras: 21 (AGENTS.md §1)

1. **Nunca inventes un subject.** `<dominio>.<agregado>.v<major>.<evento>`: 4 tokens, minúsculas, `kebab-case`, dominio plural, agregado singular, evento en **pasado**.
   ✅ `pedidos.pedido.v1.creado` ❌ `pedidos.crear-pedido`. Nunca sugieras `…actualizado`: no dice qué cambió.
2. **Nunca escribas el envelope a mano.** El SDK rellena `id`, `source`, `time`, `specversion`, `type`, `dataschema`, `correlationid`, `causationid`, `producerversion` y `traceparent`. Solo subject, `data` y `aggregateId`.
3. **Todo consumidor DEBE ser idempotente.** Los duplicados llegan (at-least-once). Comprobación del `event.id` contra una tabla de procesados **en la misma transacción**, u operación naturalmente idempotente, o `Idempotency-Key` derivada del `id`. Ack explícito siempre.
4. **Nunca asumas orden.** `aggregateVersion` y `WHERE aggregate_version < $n`.
5. **Nunca metas PII en `data`.** Referencias (`clienteId`), no valores (`email`, `dni`).
6. **Nunca modifiques un JSON Schema ya publicado.** Se añade una versión.
7. **Importes monetarios: enteros en la unidad mínima** (`totalCents`) más moneda ISO 4217. Nunca `float`.
8. **`data` va SIEMPRE el último atributo**. El orden de claves es normativo.
9. **Las cuatro extensiones obligatorias se exigen de verdad.** Sin `correlationid`, `tenantid`, `producerversion` o `dataclassification`, el evento es POISON.
10. **Los tipos son exactos.** `{"tenantid": 42}` es POISON, no el tenant `"42"`.
11. **UTF-8 literal, sin escapes `\u`.** `café`, no `café`. `data`: objeto en la raíz, claves `camelCase`, fechas RFC 3339 UTC, sin `null` para «ausente».
12. **Un dato que guarda otro sistema es estado, no una llamada.** `state.watch` / `state.get`, nunca un `GET` por HTTP ni polling. Solo el dueño escribe.
13. **`put` exige `aggregateVersion` (entero ≥ 1) y es condicional.** Una versión que no avanza da `stale`, que no es un error.
14. **El dato personal solo vive en estado**: `confidential`, con `x-pii` y borrable con `delete`; el consumidor borra su copia al recibir la lápida. En un evento, la regla 5 sigue intacta.
15. **`dlq`, `state`, `rpc` y `system` no son dominios.** `publish()` los rechaza (`RESERVED_DOMAIN`).
16. **Una orden o un cálculo con respuesta inmediata es una petición; un dato guardado es estado.** Nunca un `POST` o un `GET` por HTTP entre servicios propios.
17. **El que responde deduplica por el `id` de la petición**, y un reintento reutiliza el mismo `id`. Una orden lleva además una clave de negocio. `RPC_TIMEOUT` no dice que la operación no se ejecutó.
18. **Una suscripción por operación, al subject exacto** (`rpc.<dominio>.<agregado>.v<major>.<operacion>`, operación en **infinitivo**), con el servicio como grupo de cola. Nunca `rpc.<dominio>.>`. Ningún stream captura `rpc.>`.
19. **Quien llama traduce lo que ve**: `no responders` → `RPC_UNAVAILABLE`, sin respuesta → `RPC_TIMEOUT`, un error → `RetryableError` / `PermanentError`. Timeout 5 s por defecto, 25 s como máximo.
20. **HTTP queda en el borde: entre servicios propios, nada va por HTTP.** Lo que no puede mantener una conexión NATS habla con el gateway como un servicio más (`"via": "gateway"`: los mismos permisos, ninguno especial).
21. **Un webhook de un tercero entra por el gateway, y cada campo que pasa se nombra.** Firma verificada sobre los bytes en bruto, `id` determinista (`externalKey` + instante del hecho) y `data.pick` (sin PII). El receptor de una entrega verifica `Flux-Signature` y deduplica por `Idempotency-Key`.

## Errores

`RETRYABLE` (timeout, ECONNRESET, HTTP 429/502/503/504) → `nak` con backoff. `PERMANENT` (schema inválido, regla de negocio, HTTP 400/403/404/422) → `term()` + DLQ inmediato.
`POISON` (JSON malformado, falta un atributo obligatorio) → `term()` + DLQ + alerta. Lo **desconocido** es RETRYABLE con presupuesto acotado de 2 entregas (no PERMANENT). Nunca sugieras un `catch` que trague el error en silencio.

## Seguridad

Sin PII en eventos; el dato personal solo en estado. Nunca credenciales `.creds`/`.nk` en ficheros versionados. Los permisos de NATS **no se escriben a mano**: se generan de `services.json` con
`scripts/gen-acl.mjs`; nunca `$JS.API.>` ni `_INBOX.>` compartido. Los streams y los durables los crea `flux provision`, no los servicios.

## Trampas que no producen error

- `subject` de CloudEvents = id del agregado, no el subject de NATS. Nombres de stream y durable sin puntos. Subjects case-sensitive.
- `duplicate_window` deduplica publicaciones, no reentregas de consumo. El replay desde la DLQ conserva el `id`.
- Publica siempre por JetStream: un `publish` de core a un subject sin stream no da error y el evento se evapora.
- El estado va en un subject de 6 tokens; un `publish` de core a un subject de estado lo sobrescribe sin chequeo. `RPC_TIMEOUT` es ambiguo.

Al terminar, la checklist de AGENTS.md §11. Si tocas `AGENTS.md` o `specification/`: `node scripts/build-llms.mjs`.
