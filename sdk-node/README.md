# @flux/sdk — Node.js / TypeScript

Cliente delgado del [flux Event Protocol v1](../README.md).
**Nivel de conformidad: L2** (resiliente).

El SDK resuelve infraestructura, no negocio. Si te encuentras metiendo lógica de
dominio aquí, va en tu servicio.

## Instalación

```bash
npm install @flux/sdk
```

## Uso

```ts
import { connect, PermanentError, RetryableError } from "@flux/sdk";

const bus = await connect({
  servers: "nats://localhost:4222",
  service: "facturacion-api",
  environment: "produccion",
  version: "3.4.1",
  tenantId: "acme",
  schemaBaseUrl: "https://schemas.internal",
});

// Publicar — solo escribes subject, data y el id del agregado.
await bus.publish(
  "pedidos.pedido.v1.creado",
  { pedidoId: "ped-123", aggregateVersion: 1, totalCents: 9990, moneda: "EUR" },
  { aggregateId: "ped-123" },
);

// Consumir — la idempotencia es TUYA, y es obligatoria.
await bus.subscribe("pedidos.pedido.v1.creado", async (evento, ctx) => {
  if (await yaProcesado(evento.id)) return ctx.ack();

  try {
    await crearFactura(evento.data);
  } catch (e) {
    if (e.code === "PEDIDO_YA_CANCELADO") {
      throw new PermanentError("pedido cancelado", { code: e.code }); // → DLQ ya
    }
    throw new RetryableError("proveedor caído", { cause: e });        // → reintento
  }

  await marcarProcesado(evento.id);
});
```

## Lo que hace el SDK por ti

| | |
|---|---|
| Envelope | Rellena `id` (UUIDv7), `source`, `time`, `type`, `dataschema`, `specversion` |
| Contexto | Propaga `correlationid`, `causationid` y `traceparent` vía `AsyncLocalStorage` — un `publish()` dentro de un handler los hereda solo |
| Naming | Deriva `type`, nombres de stream y de durable consumer desde el subject |
| Streams | Crea `EVT_<DOMINIO>` y `DLQ_<DOMINIO>` si no existen, con la config canónica |
| Entrega | Ack explícito, WIP automático, backoff canónico, enrutado a DLQ |
| Errores | Clasifica en RETRYABLE / PERMANENT / POISON y actúa en consecuencia |
| Validación | Rechaza subjects inválidos, atributos raíz desconocidos y payloads > 1 MiB |

## Lo que NO hace, y es tu responsabilidad

- **Idempotencia.** at-least-once significa que los duplicados llegan. El SDK no
  puede saber qué significa "el mismo evento" en tu dominio.
- **Orden.** flux no lo garantiza. Usa `aggregateVersion` y
  `WHERE aggregate_version < $n`.
- **Validación contra el Schema Registry.** Llega con L3, en fase 4.

## Clasificación de errores — la decisión que es vuestra

El default de la spec para un error desconocido es `PERMANENT`: un evento en la DLQ es
recuperable, una cola atascada 51 minutos en hora punta no lo es. Si vuestras
dependencias internas tienen hipos frecuentes, esa asimetría puede no ser la vuestra:

```ts
const bus = await connect({
  // …
  classifier: {
    unknownErrorPolicy: "retryable",   // default: "permanent"
    timeoutPolicy: "permanent",        // default: "retryable"
    rules: [
      (e) => /deadlock/i.test(String(e?.message))
        ? { class: ErrorClass.RETRYABLE, code: "DB_DEADLOCK", retryAfterMs: 250 }
        : undefined,
    ],
  },
});
```

## Verificación de configuración de consumidor

Al crear un consumidor, el SDK **compara la config devuelta por el servidor con la
solicitada** y lanza `ConsumerConfigMismatchError` si difieren.

No es paranoia: JetStream sobrescribe `ack_wait` con `backoff[0]` sin devolver ningún
error. Sin esta comprobación, un `ack_wait` de 1 segundo pasa inadvertido hasta que en
producción los handlers empiezan a ejecutarse en concurrencia consigo mismos. Ver
[03-delivery.md §2.1](../specification/03-delivery.md).

## Desarrollo

```bash
npm run typecheck
npm test          # 54 tests, sin broker
npm run build

# Suite de conformidad — necesita NATS
docker compose up -d          # desde la raíz del repo
cd ../conformance && npm test
```

## Sobre la dependencia de NATS

Usa `@nats-io/transport-node` y `@nats-io/jetstream`, no el paquete `nats`
monolítico — quedó deprecado en favor de estos.

Ningún tipo de NATS aparece en la API pública de este SDK. Es deliberado: sustituir el
broker debe ser un cambio de las capas 0-1 del protocolo, sin tocar aplicaciones
([00-protocol.md §3](../specification/00-protocol.md)). Si un tipo de NATS se filtra a
la API pública, es un bug del SDK.
