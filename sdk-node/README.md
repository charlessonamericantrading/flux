# @flux/sdk — Node.js / TypeScript

Cliente delgado del [flux Event Protocol v1](../README.md).
**Nivel de conformidad: L3** (gobernado).

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
- **Aislamiento entre tenants.** El SDK filtra, pero el modelo por defecto no resiste a
  un servicio legítimo comprometido. Ver [09-multitenancy.md](../specification/09-multitenancy.md).

## Clasificación de errores — la decisión que es vuestra

El default para un error **desconocido** es `retryable-bounded`: reintenta, pero con un
presupuesto de 2 entregas en vez de las 6 completas. Domina a las alternativas — un
transitorio se recupera en el segundo intento y un sistemático llega a la DLQ en ~30 s
sin atascar la cola. Los RETRYABLE **reconocidos** (503, ECONNRESET) conservan sus 6.

Si vuestras dependencias fallan de otra manera, la política es vuestra:

```ts
const bus = await connect({
  // …
  classifier: {
    unknownErrorPolicy: "permanent",   // default: "retryable-bounded"
    unknownRetryBudget: 3,             // default: 2
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

## Validación L3 (opcional)

Sin esto, un productor puede publicar un payload que viola su propio `dataschema` y
nadie se entera hasta que un consumidor —posiblemente de otro equipo y otra semana— se
atraganta. El error aparece lejísimos de su causa.

```ts
import bundle from "../schemas/bundle.json" with { type: "json" };

const bus = await connect({
  // …
  validation: { mode: "strict", bundle, onConsume: false },
});
```

| Modo | Qué hace |
|---|---|
| `off` (default) | Nada. L2, sin coste. |
| `warn` | Registra y publica igual. Para introducir validación en un ecosistema en marcha. |
| `strict` | `publish()` **lanza**. Un contrato roto pasa a ser un fallo del productor. |

- `ajv` es **dependencia opcional**: L3 es opt-in, así que su coste también.
  `npm install ajv`.
- El bundle se despliega **con el servicio**; el `dataschema` nunca se resuelve por HTTP.
  Validar está en la ruta caliente y una caché con TTL abriría una ventana en la que dos
  servicios validan contra versiones distintas del mismo esquema.
- Tras cambiar un esquema: `node scripts/bundle-schemas.mjs`.
- Reporta **todos** los errores, no solo el primero: de uno en uno, arreglar un payload
  con tres campos mal cuesta tres despliegues.

> Los esquemas declaran `$schema: draft/2020-12`, así que se usa `ajv/dist/2020` y no el
> export por defecto. Un Ajv de draft-07 no da un error de versión: da
> `no schema with key or ref ".../2020-12/schema"`, que no dice nada.

## Firma de eventos (opcional)

Traslada la autenticidad **del canal al evento**: uno firmado sigue siendo verificable
dentro de un fichero, un backup o un correo, donde ya no hay ACL que lo respalde.

```ts
// Generar el par:  flux keygen pedidos-api 1
const productor = await connect({
  // …
  signing: { privateKeyPem, keyId: "pedidos-api-1" },
});

const consumidor = await connect({
  // …
  signing: { publicKeys: { "pedidos-api-1": publicKeyPem }, verify: "require" },
});
```

- **Ed25519 sin negociación de algoritmo**, desde `node:crypto`. Sin dependencias.
- Modos `off` (default) | `warn` | `require`.
- **Conserva la pública de las claves retiradas** mientras existan eventos firmados con
  ellas — mínimo 90 días, la retención de la DLQ. Retirar una clave impide *emitir* con
  ella, no *verificar* lo ya emitido.
- La firma sobrevive al paso por la DLQ y al replay: las extensiones `dlq*` se añaden
  después de firmar y la verificación las ignora.

Ver [`07-signing.md`](../specification/07-signing.md) para qué **no** resuelve.

## Métricas

Los nombres y etiquetas los fija el protocolo, no la aplicación: si cada SDK nombrara a
su manera, un panel del ecosistema sería imposible.

```ts
import { InMemoryMetrics } from "@flux/sdk";

const metrics = new InMemoryMetrics();
const bus = await connect({ /* … */ metrics });

// Sírvelo en /metrics
app.get("/metrics", (_req, res) => res.type("text/plain").send(metrics.render()));
```

El default es no-op: un SDK no debe imponer un backend. Implementa `MetricsSink` para
enchufar prom-client u OpenTelemetry **conservando los nombres**.

- **Nunca** etiquetes por `tenantid`, `id` ni `correlationid`: un tenant nuevo no debe
  crear series temporales. En trazas sí.
- `flux_consumer_pending` se alimenta de **dos** fuentes —metadatos del mensaje y sondeo
  periódico— porque si el bucle del consumidor muere dejan de llegar mensajes y una
  métrica alimentada solo desde metadatos se queda plana en vez de crecer.
- `pendingPollMs`: positivo = intervalo, `0` = el default de 15 s, negativo = desactivado.

## Aislamiento entre tenants

```ts
const bus = await connect({
  // …
  tenantId: "acme",
  tenantIsolation: "strict",   // toda suscripción filtra; olvidarlo LANZA
});
```

`strict` convierte "olvidé filtrar por tenant" en un error de configuración al
suscribirse. Un filtro que hay que acordarse de poner es un filtro que alguien
olvidará, y el fallo —ver los datos de otro tenant— **no produce ningún error**:
produce un incidente de privacidad que se descubre semanas después.

El evento de otro tenant se descarta con `ack` antes de llegar al handler: no es un
fallo, no es para nosotros.

> El modelo por defecto **no resiste a un servicio legítimo comprometido**. Si necesitas
> esa garantía, es account de NATS por tenant — ver
> [09-multitenancy.md §2](../specification/09-multitenancy.md), que lista el coste real.

## Desarrollo

```bash
npm run typecheck
npm test          # 105 tests, sin broker
npm run build

# Suite de conformidad contra NATS real
docker compose up -d          # desde la raíz del repo
cd ../conformance && npm test

# Conformidad CRUZADA: los mismos vectores en los siete SDKs, byte a byte
node conformance/cross-sdk.mjs --only node        # sin broker
node conformance/cross-sdk.mjs --verbose          # todos
```

## Sobre la dependencia de NATS

Usa `@nats-io/transport-node` y `@nats-io/jetstream`, no el paquete `nats`
monolítico — quedó deprecado en favor de estos.

Ningún tipo de NATS aparece en la API pública de este SDK. Es deliberado: sustituir el
broker debe ser un cambio de las capas 0-1 del protocolo, sin tocar aplicaciones
([00-protocol.md §3](../specification/00-protocol.md)). Si un tipo de NATS se filtra a
la API pública, es un bug del SDK.
