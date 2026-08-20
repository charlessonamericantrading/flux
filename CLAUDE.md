# CLAUDE.md

**La fuente canónica de instrucciones para agentes en este repo es
[`AGENTS.md`](AGENTS.md). Léela antes de escribir código que publique o consuma
eventos.** Este fichero solo repite lo que no puede fallar.

## Qué es este repo

`flux` es un **protocolo**, no una librería. La especificación normativa vive en
[`specification/`](specification/); los SDKs son clientes delgados intercambiables.
Al responder preguntas de diseño, la spec manda sobre cualquier código.

## Las reglas que no pueden fallar

1. Subject: `<dominio>.<agregado>.v<major>.<evento>` — 4 tokens, minúsculas,
   `kebab-case`, dominio plural, agregado singular, **evento en pasado**.
2. El SDK rellena todo el envelope. El desarrollador solo escribe **subject, `data`,
   y opcionalmente `aggregateId`**. Si generas código que asigna `id`, `source`,
   `time`, `type`, `correlationid` o `traceparent` a mano, está mal.
3. **Todo consumidor es idempotente.** at-least-once significa que los duplicados
   llegan. Un handler sin deduplicación está roto aunque los tests pasen.
4. **No asumas orden.** Usa `aggregateVersion` + `WHERE aggregate_version < $n`.
5. **Sin PII en `data`.** Publica referencias (`clienteId`), no valores (`email`).
6. Los JSON Schema publicados son **inmutables**. Se añade versión, no se edita.
7. Dinero: enteros en unidad mínima (`totalCents`) + ISO 4217. Nunca `float`.

## Trampas que fallan en silencio

- `subject` de CloudEvents = **id del agregado** (`"ped-123"`). El subject de NATS es
  otra cosa. En la API del SDK se llaman `aggregateId` y `subject`.
- Los nombres de stream y de durable consumer de NATS **no admiten puntos**.
- `duplicate_window` **no** deduplica reentregas de consumo, solo publicaciones.
- Los subjects de NATS son **case-sensitive**: `Pedidos.` crea un subject fantasma
  sin suscriptores y sin error.
- Al hacer replay desde la DLQ, **conserva el `id` original**.

## Al terminar una tarea

Repasa la checklist de [`AGENTS.md` §10](AGENTS.md).

Si tocas `AGENTS.md` o cualquier fichero de `specification/`, regenera el agregado:

```bash
node scripts/build-llms.mjs
```

`llms-full.txt` es **generado**. Nunca lo edites a mano.
