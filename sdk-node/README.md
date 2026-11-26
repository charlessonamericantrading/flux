# @flux/sdk — Node.js / TypeScript

Cliente delgado del [flux Event Protocol v1](../README.md).
**Nivel de conformidad: L3** (gobernado). Capacidades opcionales: `signing`, `security` y el
[estado replicado](#estado-replicado) (`bus.state`), que **todavía no está declarado** en
`protocol.json` → `capabilities`: se declara cuando el arnés cruzado reproduzca los vectores de estado.
La [petición/respuesta](#peticionrespuesta) (`bus.request`, `bus.respond`) está implementada y probada contra un broker
real, pero **tampoco está declarada**: la declaración se hace al integrar, con el arnés cruzado y los permisos juntos.

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
| Streams | En desarrollo, crea `EVT_<DOMINIO>` y `DLQ_<DOMINIO>` si no existen, con la config canónica. **En producción no puede**: se aprovisionan con `flux provision` y el SDK falla con `NotProvisionedError` — ver [Seguridad](#seguridad-permisos-inbox-tls-y-credenciales) |
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

## Estado replicado

Un evento dice **qué pasó**; el **estado** es el **valor actual** de un agregado, que su dueño
publica y los demás observan. Sustituye a `GET /clientes/123`, a la carga inicial
(`GET /clientes?page=`) y al polling de datos. Contrato normativo:
[10-state.md](../specification/10-state.md). Es una extensión **opcional**: un servicio que solo use
eventos no la toca.

```ts
// El DUEÑO escribe. aggregateVersion es suyo (su contador), el SDK no lo inventa.
await bus.state.put("clientes.cliente.v1", "cli-1",
  { clienteId: "cli-1", nombre: "Ana", email: "ana@example.com", aggregateVersion: 7 },
  { tenantId: "acme" });                       // → { applied: true, outcome: "ok", revision: 41 }

await bus.state.delete("clientes.cliente.v1", "cli-1", { aggregateVersion: 8, tenantId: "acme" });

// Un OBSERVADOR lee: una clave…
const actual = await bus.state.get("clientes.cliente.v1", "cli-1", { tenantId: "acme" });  // entrada | null

// …o todo el contrato: una foto completa y después los cambios, por el mismo canal.
await bus.state.watch("clientes.cliente.v1", async (entrada) => {
  if (entrada.deleted) return borrarLocal(entrada.tenantId, entrada.key);  // OBLIGATORIO: la lápida
  await guardarLocal(entrada);                  // UPDATE … WHERE aggregate_version < $n
}, { tenantId: "acme", onSynced: () => listo() });
```

`entrada` es `{ key, tenantId, deleted, data, aggregateVersion, revision }`: `data` es `null` si es
una lápida, y `tenantId` existe porque un `watch` sin tenant recibe `cli-1` de varios y la clave
sola mezclaría los tenants en tu copia local.

**Lo que hace `put`, y por qué** ([10-state.md §3](../specification/10-state.md)):

- Es **condicional**. Lee la clave con `DIRECT.GET`, no escribe si `aggregateVersion` no avanza y
  publica con `Nats-Expected-Last-Subject-Sequence`: dos instancias del dueño, o una lenta, no
  pisan un valor nuevo con uno viejo (y un consumidor nuevo no recibe el viejo como la verdad).
- Una versión que no avanza —**también una igual**— es **`stale`, y no es un error**
  (`{ applied: false, outcome: "stale", revision }`). Lo que sí puede ser un error es ignorarlo.
- **Reconoce su propia escritura** (paso 1b): si el acuse se pierde, el reintento relee, ve su propio
  `id` y devuelve `ok`; sin eso, una escritura buena se convertiría en un `stale`.
- Reconoce **los dos códigos** de conflicto de secuencia, `10071` y `10164` (el segundo, sin número,
  es el de un cluster bajo carrera; un SDK que solo conozca el primero pasa sus pruebas contra un nodo
  y no reintenta en producción). Tres intentos y, agotados, `STATE_CONFLICT` (**RETRYABLE**).
- **Valida antes de la red**, en este orden: dominio reservado, forma del contrato, clave, tenant,
  `aggregateVersion`, clasificación y —con L3— el esquema. Sin `classification`, usa **`confidential`** (no el default de
  la conexión); `restricted` se rechaza.
- **Nunca por core NATS.** Un `publish` de core a un subject de estado sobrescribe sin acuse ni chequeo.

**Borrar** es `put` de una **lápida** (`data: { aggregateVersion }` y nada más) en el mismo subject:
el valor anterior **sale del stream**. La versión de la lápida debe superar la del valor. Y la
trampa: un dueño que borra su fila y la recrea con `aggregateVersion: 1` recibe `stale` y **la clave
sigue borrada, sin error**; su contador tiene que sobrevivir al borrado (`deleted_at` en vez de `DELETE`).
`get` de una lápida da `null`, igual que de una clave que nunca existió.

**Reglas que el SDK aplica por ti**

| | |
|---|---|
| Subject | `state.<dom>.<agr>.v<M>.<tenant>.<clave>` — 6 tokens. La clave y el tenant van **dentro**: el tenant lo confina el **broker** |
| Clave | `^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`, coincidencia completa, sin codificar. El broker **no** la valida (acepta `*`, `a.b` y 2000 caracteres): `INVALID_STATE_KEY` |
| Tenant | `^[a-z0-9]+(-[a-z0-9]+)*$`, máximo 64, **sin valor por defecto** (ni `system` implícito). `put`, `delete` y `get`: la opción, el evento en curso o `connect()`; si no, `INVALID_TENANT_FOR_STATE`. **`watch`: solo la opción y `connect()`, nunca el evento en curso** (no corre dentro de un handler) |
| `aggregateVersion` | Entero ≥ 1 y ≤ 2^53 − 1, **siempre** (con o sin L3). **En Node `7.0` es el entero 7**: `JSON.parse("7.0")` y `7` son el mismo `number`, así que no se puede rechazar. Es un límite conocido del lenguaje (10-state.md §3.4), no un fallo: un SDK cuyo parser distingue entero de flotante (Rust) sí lo rechaza |
| Dominios | `dlq`, `state`, `rpc` y `system` no son dominios: `publish()`, `subscribe()` y todo `bus.state.*` lanzan `RESERVED_DOMAIN` |
| `watch` | Durable `<servicio>__state_<dom>_<agr>_v<M>` (+ `__<tenant>`), `deliver_policy: last_per_subject`; con `tenantIsolation: "strict"` exige tenant |
| `onSynced` | **Una vez** por `watch()` (ni por reconexión ni tras un `resync`): con el contrato vacío, de inmediato y antes de entregar nada; si no, cuando llega una entrega con `pending == 0` **y** el handler ha terminado. No promete que no lleguen entradas después. **`watch` toma un tenant**: un servicio que observa N tenants hace N llamadas y recibe N `onSynced`; combinarlas es de la aplicación |
| `watch` y `flux state resync` | Si el durable se **borra** con el `watch` abierto, el SDK reabre el consumo (`CONSUMER.INFO` por nombre, sin `CONSUMER.CREATE`, comprobando la configuración) y la foto vuelve a entregarse. Medido con `nats.js` 3.4.0 (tres veces): **43 s** hasta la primera entrada sin esto, 0,1 s con ello |

**Los siete códigos** ([04-errors.md §6](../specification/04-errors.md)) — `code` fijo, clase fija:

| Código | Clase | Cuándo |
|---|---|---|
| `INVALID_STATE_CONTRACT` | PERMANENT | Un contrato que no es `<dominio>.<agregado>.v<major>` (`clientes.cliente`); un dominio reservado es `RESERVED_DOMAIN` |
| `INVALID_TENANT_FOR_STATE` | PERMANENT | Tenant que no cumple el patrón, o ninguno |
| `INVALID_STATE_KEY` | PERMANENT | Clave que no cabe en un token de NATS |
| `MISSING_AGGREGATE_VERSION` | PERMANENT | Falta, o no es un entero ≥ 1 (también al consumir) |
| `STATE_SUBJECT_MISMATCH` | **POISON** | El `tenantid`, el `subject` o el `type` del sobre no coinciden con el subject de NATS: nunca se aplica ni llega al handler |
| `STATE_CONFLICT` | RETRYABLE | Tres conflictos de secuencia seguidos |
| `RESERVED_DOMAIN` | PERMANENT | Un dominio reservado en un subject o contrato |

Un contrato mal formado (`clientes.cliente`) lanza `InvalidStateContractError`, con su código
`INVALID_STATE_CONTRACT` (el séptimo: antes no tenía ninguno). Reutiliza, sin código nuevo, `INVALID_DATACLASSIFICATION`
(`restricted` en un `put`; POISON si llega en un mensaje), `INVALID_SCHEMA` (L3), `NOT_PROVISIONED` y
`PERMISSION_DENIED`. Ningún mensaje cita valores de `data`: una clave o un tenant sí, una versión por su tipo.

**`NOT_PROVISIONED` en estado.** El SDK no consulta ni crea el stream `STATE_<DOM>` (`STREAM.INFO` lista
todas las claves y todos los tenants). Su señal es otra: **nadie contesta** a `DIRECT.GET` —y un stream sin
`allow_direct` se ve igual que uno que no existe—, o `CONSUMER.INFO` dice «stream not found». El mensaje
nombra las dos causas y apunta a `flux provision` y `flux doctor`.

**La DLQ de estado** (`dlq.state.<dom>.<agr>.v<M>`, sin tenant ni clave) guarda el sobre original **sin el
dato**: `data` pasa a `{ key, revision }`, sin `signkeyid` ni `signature`; un mensaje ilegible no se copia
en ninguna forma, y el `dlqerror` de un error que no es de flux se reduce a su código y a una frase fija
(`UNKNOWN: error sin clasificar de la aplicación; el mensaje se omite…`: el de un driver puede citar la fila
entera). `onPoison` recibe `raw` **vacío**. El estado **no se reproduce** desde la DLQ (`flux dlq replay` lo
rechaza): se resincroniza con `flux state resync`. `watch` avisa al abrir si `DLQ_<DOM>` no captura
`dlq.state.<dom>.>`.

**L3.** `put` valida `data` contra el esquema que resuelve el índice `state` del bundle
(`validation.bundle.state`), sin red; la lápida, contra `schemas/system/state/eliminado/1.0.0.json`.
El mensaje reporta **todos** los errores y cita la **ruta** (`/email`), nunca el valor.

**Métrica.** `flux_state_writes_total{contract,outcome}` (`ok | stale | conflict | invalid_schema`) por el
`MetricsSink` (`stateWrite`, opcional: un sink anterior sigue funcionando). `invalid_schema` es todo fallo de
L3 —también `SCHEMA_NOT_FOUND` en modo estricto— y `MISSING_AGGREGATE_VERSION`, como en un evento. **Nunca** la clave ni el tenant
como etiqueta. Un `watch` reutiliza las métricas de consumo con `subject="state.<contrato>"` y el `consumer`
**sin** el sufijo de tenant; `flux_consumer_pending` es la **suma** de sus durables.

### Permisos que el estado necesita

Medidos contra un `nats-server` real y con la matriz «quita uno y falla» de
`test/integration/state.test.ts`. Coinciden con `protocol.json` → `state.acl`:

| Rol | `publish` | Sin él |
|---|---|---|
| Todo servicio | `$JS.API.INFO`; `subscribe`: `_INBOX.<servicio>.>` | — |
| **Dueño** del dominio `<dom>` | `state.<dom>.>` · `$JS.API.DIRECT.GET.STATE_<DOM>.state.<dom>.>` | `PermissionDeniedError` al `put`/`delete`/`get` |
| **Observador** del contrato `<dom>.<agr>.v<M>`, tenant `<t>` | `$JS.API.CONSUMER.INFO.STATE_<DOM>.<durable>` · `…MSG.NEXT.STATE_<DOM>.<durable>` · `$JS.ACK.STATE_<DOM>.<durable>.>` · `dlq.state.<dom>.<agr>.v<M>` · `$JS.API.STREAM.INFO.DLQ_<DOM>` | `watch` lanza `PermissionDeniedError` (info, pull, DLQ); **ack** y **DLQ** no lanzan (ver abajo) |
| …y su `get` | `$JS.API.DIRECT.GET.STATE_<DOM>.state.<dom>.<agr>.v<M>.<t>.*` (`….*.*` sin tenant) | `get` lanza `PermissionDeniedError`; `watch` no lo necesita |
| Quien **lee** la DLQ de estado de su dominio | `…CONSUMER.INFO`, `…MSG.NEXT` y `$JS.ACK…` de `DLQ_<DOM>.<servicio>__dlq_state_<dom>` | (no es del SDK: un pull consumer propio) |
| Solo en **desarrollo** | `$JS.API.CONSUMER.CREATE.STATE_<DOM>.<durable>.state.<dom>.<agr>.v<M>.<t>.*` | `NotProvisionedError` si el durable no existe |

- **Nunca** `$JS.API.STREAM.INFO.STATE_<DOM>` a un observador (lista claves y tenants), ni la forma de
  **cuerpo** de `DIRECT.GET` (`$JS.API.DIRECT.GET.<stream>` + `{"seq": n}` lee cualquier mensaje). El
  broker rechaza un cuerpo en la forma por subject (`408`).
- El tenant va **fijado** en el permiso de lectura y en el nombre del durable: un observador de `acme`
  no lee `globex` (`get` y `watch` lanzan `PermissionDeniedError`), y no escribe.
- Como con los eventos, un `ack` o un pull denegados **no lanzan**: el pull lo detecta `watch()` al abrir
  (lanza), el `ack` solo lo registra `logger.error` —la entrada se reentrega cada `ack_wait`, sin fin—, y una
  DLQ no escribible deja la entrada **sin terminar** (se reentrega y se avisa). **Configura un `logger`.**

### Funciones puras (para el arnés cruzado y para tus tests)

Construyen los **bytes exactos** de [10-state.md §2.6](../specification/10-state.md), sin reloj ni UUID por
dentro: `id` y `time` los das tú. Producen lo mismo en todos los SDKs.

```ts
import { stateSubject, buildStateEvent, buildStateTombstone, buildStateDlqEvent, serialize } from "@flux/sdk";

stateSubject("clientes.cliente.v1", "acme", "cli-1");   // "state.clientes.cliente.v1.acme.cli-1"
serialize(buildStateEvent({ contract, tenantId, key, id, source, time, correlationid, producerversion, classification, data }));
serialize(buildStateTombstone({ contract, tenantId, key, id, source, time, correlationid, producerversion, aggregateVersion }));
serialize(buildStateDlqEvent({ original, revision, dlq: { reason, attempts, consumer, error, dlqtime } }));
```

`test/state.test.ts` fija que coinciden con `conformance/cases/cross-sdk-state-envelope.json` y con el texto
de la spec (y que cambiar el `tenantid` cambia el hash).

### Lo que NO hace

- **No crea ni consulta `STATE_<DOM>`** (lo aprovisiona `flux provision`), ni ofrece una opción de desarrollo
  para crearlo. Sí crea `DLQ_<DOM>` (con los dos subjects) si no existe, como con los eventos.
- **No recuerda en memoria** la última secuencia escrita para ahorrarse la lectura (la spec lo permite): cada
  `put` lee primero.
- **`get` es una lectura de réplica.** Con `replicas: 3`, justo después de un `put` puede devolver el valor
  anterior (no medido con red real, [10-state.md §3.7](../specification/10-state.md)); quien necesite su
  propia escritura usa `revision`.
- **No resincroniza**: borrar y recrear el durable es `flux state resync`, con credenciales de operación.

## Petición/respuesta

Una **orden** o un **cálculo** con respuesta inmediata entre dos servicios, sin HTTP. Contrato normativo:
[11-request-reply.md](../specification/11-request-reply.md). Es core NATS: no se persiste, no hay stream ni DLQ, y si nadie
escucha, quien llama lo sabe **en milisegundos**. Es una extensión **opcional**: un servicio que solo use eventos o estado no la toca.

> **Si lo que pides es un dato que otro sistema guarda, es estado** (`bus.state.get` / `watch`), no una petición. Usar esto
> para leer datos es volver a tener una API con otro transporte.

```ts
// QUIEN LLAMA. Solo escribes la operación, `data` y, si quieres, el id del agregado: el SDK rellena el sobre.
const r = await bus.request("clientes.cliente.v1.calcular-descuento",
  { clienteId: "cli-1", totalCents: 9990, moneda: "EUR" }, { timeoutMs: 2000, aggregateId: "cli-1" });
// → el `data` de la respuesta: { descuentoCents: 500, moneda: "EUR" }

// QUIEN RESPONDE (solo el DUEÑO del dominio). Lanzar PermanentError / RetryableError responde un error.
await bus.respond("clientes.cliente.v1.calcular-descuento", async (peticion, ctx) => {
  if (await yaAplicada(ctx.id)) return resultadoGuardado(ctx.id);       // OBLIGATORIO si es una ORDEN: el id de la petición
  return { descuentoCents: 500, moneda: peticion.moneda };
});
```

La **operación** es `<dominio>.<agregado>.v<major>.<operacion>` (4 tokens, la operación en **infinitivo**, `kebab-case`): el subject
de NATS es `rpc.` más eso, y **el prefijo lo pone el SDK** (`rpc.clientes.…` es `RESERVED_DOMAIN`). `request` resuelve con el
`data`; un error de quien responde se lanza como `RetryableError` o `PermanentError` **con su `code`**, de modo que un handler de
eventos que llama a una operación hereda la clasificación sin hacer nada.

**Opciones de `request`**

| Opción | |
|---|---|
| `timeoutMs` | Entero entre **1 y 25 000** (5 000 por defecto): **un intento**. Fuera de rango es `INVALID_RPC_CALL`, **no se recorta** (la librería de NATS convertiría un `0` en 1000 ms sin avisar). El máximo está por debajo del `ack_wait` de 30 s para que una llamada desde un handler de eventos no provoque su reentrega |
| `aggregateId` | La identidad del agregado (`cli-1`): va al atributo `subject` del sobre y a `partitionkey`. **No** es el subject de NATS |
| `tenantId` | La opción > el evento (o la petición) en curso > el de `connect()` > `system`. A diferencia del estado, no exige uno explícito |
| `classification` | `public`, `internal`, `confidential` o `restricted`. Si no, la de `connect()` |
| `retries` | **0 por defecto**: `request` hace UN intento. Solo reintenta `RPC_UNAVAILABLE` y `RPC_TIMEOUT`, y **cada reintento reutiliza el mismo `id` y los mismos bytes** (`time` incluido); nunca un error permanente |

**Qué ve quien llama** ([11-request-reply.md §4.3](../specification/11-request-reply.md)): cada situación es un código estable.

| Situación | Lo que se lanza | Clase |
|---|---|---|
| Nadie escucha la operación (`no responders`, en ~1 ms) | `RpcUnavailableError` · **`RPC_UNAVAILABLE`** | RETRYABLE, inmediato |
| No llega respuesta en el timeout | `RpcTimeoutError` · **`RPC_TIMEOUT`**: **ambiguo**, la operación PUEDE haberse ejecutado | RETRYABLE |
| La ACL deniega la petición (en ~1 ms: no es un timeout) | `PermissionDeniedError` · `PERMISSION_DENIED`, con el subject | PERMANENT |
| Lo que llega **no es la respuesta a esta petición** (§4.3: CloudEvent válido, `type`, `causationid`, `correlationid`/`tenantid`, esquema del error) | `RpcInvalidReplyError` · **`RPC_INVALID_REPLY`**; si el cuerpo parece el acuse de un stream, apunta a `flux doctor` | PERMANENT |
| La petición no cabe en `min(1 MiB, max_payload)`, **antes de salir** | `RpcMessageTooLargeError` · **`RPC_MESSAGE_TOO_LARGE`** | PERMANENT |
| Operación mal formada, `timeoutMs` fuera de rango o `data` que no es un objeto | `InvalidRpcCallError` · **`INVALID_RPC_CALL`** | PERMANENT |
| Un error de quien responde (`com.flux.rpc.error.v1`) | `RpcRetryableError` / `RpcPermanentError`, con su `code`, su `message` y `retryAfterMs` | su `class` |
| Con la firma en `require` y la respuesta sin firma, con la firma rota o de una clave desconocida | `RpcPermanentError` con `MISSING_SIGNATURE`, `INVALID_SIGNATURE` o `UNKNOWN_SIGNING_KEY` | PERMANENT |

Los seis códigos propios son **cerrados** ([04-errors.md §7](../specification/04-errors.md)); `STALE_REQUEST` lo pone quien
responde, dentro de un error. Reutiliza `RESERVED_DOMAIN`, `INVALID_SCHEMA`, `INVALID_DATACLASSIFICATION`, `PERMISSION_DENIED`
y los de la firma. Ningún mensaje cita valores de `data`.

**Quien responde** ([§5](../specification/11-request-reply.md)):

- **Una suscripción al subject EXACTO** de cada operación, con el **nombre del servicio como grupo de cola**: dos instancias se
  reparten las peticiones, al azar. **Nunca** `rpc.<dominio>.>`: con el comodín el servicio tiene interés en lo que no atiende
  y quien llama espera su timeout en vez de recibir `no responders` en 1 ms (A-64). El permiso del dueño sí es
  `rpc.<dominio>.>`: lo que se **concede** no es lo que se **usa**.
- **Siempre contesta**, con un error si algo no cumple: sobre ilegible (`MALFORMED_JSON`, `MISSING_REQUIRED_EXTENSION`…), un
  `type` que no es el de la operación (`INVALID_RPC_CALL`), firma o ventana (`STALE_REQUEST`), esquema (`INVALID_SCHEMA`, con
  **todas** las rutas y ningún valor). Una petición **sin subject de respuesta** no se atiende (nadie espera el resultado de
  una orden): log, `onPoison` y `poison`.
- **Los errores del handler**: `RetryableError` / `PermanentError` viajan con **su** código y mensaje (el código debe cumplir
  `^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*$`: si no, se sustituye por `RETRYABLE`/`PERMANENT` y se avisa); uno que el clasificador
  **reconoce** (un 503, `ECONNRESET`) viaja con su clase y código y un mensaje **genérico**; uno **desconocido** es
  **`permanent` `UNKNOWN`** con «error interno del servicio». **El texto de una excepción no viaja nunca**: va al log, con el
  `id` de la petición (y en una petición `confidential` o `restricted`, ni siquiera ahí). `class` es **binaria**: `poison` no viaja.
- **El tamaño de la respuesta se comprueba ANTES de publicar**: si no cabe, la librería lanzaría en quien responde y quien llama
  solo vería un timeout (A-65); en su lugar se contesta `RPC_MESSAGE_TOO_LARGE`.
- **Hasta 256 handlers a la vez** por operación (`maxConcurrent`): el exceso espera en la cola de la suscripción. Core NATS no
  tiene acuse: un responder sobrecargado no puede decir «ahora no», y quien llama solo ve `RPC_TIMEOUT`.
- **El responder NO filtra por el tenant de `connect()`**: el grupo de cola es el servicio y no sabe de tenants. El tenant de la
  petición llega al handler (`ctx.tenantId`) y **es suyo autorizarlo**.
- Dentro del handler, `bus.request`, `bus.publish` y `bus.state.*` **continúan la cadena**: heredan `correlationid` y
  `tenantid`, y su `causationid` es el `id` de la petición.

`ctx` lleva `operation`, el sobre de la petición (`event`), su `id` —**la clave de deduplicación**—, `correlationId`, `tenantId`,
`aggregateId` y `time`. `respond()` devuelve un `Responder` con `stop()`.

**Parada ordenada** ([§5.5](../specification/11-request-reply.md)): **dejar de escuchar** (con el drenaje de la librería), **esperar a
los handlers en vuelo** —con un tope de 25 s— y **cerrar**. `responder.stop()` hace los dos primeros; **`bus.close()` los hace antes
de cerrar la conexión**. Cerrar la conexión de golpe pierde lo que se atiende: con un handler en vuelo, el `publish` de su
respuesta lanza `closed connection` y quien llama ve un timeout (A-83).

**Idempotencia — la regla que evita recrear el problema.** Una orden puede llegar dos veces (un reintento tras un timeout; el broker no
deduplica en core). **Quien responde deduplica por `ctx.id`**, con una de las tres estrategias de
[03-delivery.md §4](../specification/03-delivery.md); el SDK **no** recuerda ids (rechazaría un reintento legítimo). Un handler de
**eventos** que reintenta por reentrega construye una petición **nueva** (otro `id`): para exactamente-una-vez a través de reentregas,
deduplica por una **clave de negocio** que viaje en `data`.

### Permisos que la petición/respuesta necesita

Medidos contra un `nats-server` real y con la matriz «quita uno y falla» de `test/integration/rpc.test.ts`. Coinciden con
`protocol.json` → `rpc.acl`:

| Rol | Permisos | Sin él |
|---|---|---|
| Todo servicio | `publish`: `$JS.API.INFO` · `subscribe`: `_INBOX.<servicio>.>` (el inbox **privado**: sin él, cualquiera leería las respuestas ajenas, A-71) | `connect()` falla con `PERMISSION_DENIED` |
| **Quien llama** | `publish` sobre el subject **EXACTO** de cada operación concedida (`rpc.clientes.cliente.v1.obtener`). Nunca `rpc.>`, `rpc.<dominio>.>` ni un comodín por token | `request` lanza `PERMISSION_DENIED` **en ms**, con el subject (no un timeout) |
| **El dueño** del dominio | `subscribe`: `rpc.<dominio>.>` · `allow_responses` (**ningún** permiso de publicación sobre inboxes) | **`respond()` falla al arrancar** con `PERMISSION_DENIED`: sin ello parecería un servicio caído (`no responders`, A-73) |

`allow_responses: true` admite **una** respuesta y **no caduca** (A-68): se **recomienda** `{ max: 1, expires: 30s }` (una respuesta
legítima nunca llega después del timeout máximo de 25 s). **Sin `allow_responses` el dueño no puede contestar**: el handler se
ejecuta, quien llama ve `RPC_TIMEOUT`, y el SDK del dueño lo **dice en su `logger`** (una vez por servicio, no una por petición).
**Configura un `logger`.**

**Cómo llega cada denegación.** Con `@nats-io/*` una petición denegada falla en ~1 ms con `RequestError` (causa
`PermissionViolationError`), y es lo que el SDK traduce; además **escucha el `-ERR` de la conexión y lo compite con la respuesta**, que
es lo que hace falta con una librería que no entrega la denegación (`async-nats`, A-72). Una suscripción denegada **no lanza** al
crearla: tras un `flush` (un PING/PONG: el `-ERR` llega antes del PONG) su iterador lanza, y `respond()` lo convierte en el error de
arranque.

### Firma, L3 y métricas

- **Firma.** Con `signing`, la petición y la respuesta (y los errores) van firmadas; con `verify: "require"` quien llama verifica la
  respuesta y quien responde la petición, y **rechaza una cuyo `time` se aleje más de 300 s de su reloj** —por delante o por detrás—
  con `STALE_REQUEST`. La firma por sí sola no impide reenviar una petición capturada; dentro de la ventana, la defensa es la
  idempotencia por `id`. Solo con `require` (en `warn` y `off` no se aplica). Presupone relojes sincronizados (NTP).
- **L3.** Con `validation`, la petición se valida contra el esquema que resuelve el índice **`rpc`** del bundle
  (`validation.bundle.rpc`: **sin red**) **antes de salir**, y la respuesta antes de devolverla; quien responde valida los dos
  lados. El mensaje dice **«la petición no cumple…»** o **«la respuesta no cumple…»** (el mismo `INVALID_SCHEMA` en los dos casos:
  hay que saber a quién arreglar), con todas las rutas y ningún valor. En modo estricto una operación que no está en el bundle falla
  con `SCHEMA_NOT_FOUND` **al llamar y al arrancar `respond()`**. Sin bundle, el `dataschema` es `<major>.0.0` bajo `schemaBaseUrl`.
  En `warn` el fallo se registra, se cuenta (`invalid_schema`) y se sigue.
- **Métricas** ([08-observability.md §2.5](../specification/08-observability.md)): `flux_rpc_client_requests_total{operation,outcome}`,
  `flux_rpc_server_requests_total{operation,outcome}` y `flux_rpc_duration_seconds{operation,role}`, por el `MetricsSink`
  (`rpcClientRequest`, `rpcServerRequest`, `rpcDuration`: opcionales, un sink anterior sigue funcionando). `operation` es el nombre
  sin `rpc.`. **Nunca** el tenant, el `id`, el `correlationid` ni quién llama como etiqueta. `INVALID_RPC_CALL` y `RESERVED_DOMAIN` no se cuentan.
  Las alertas que importan: `poison` del servidor (alguien publica a mano o hay un SDK roto), `invalid_reply` del cliente (la trampa de
  un stream que captura `rpc.>`) y la tasa de `unavailable` y `timeout`.

### Funciones puras (para el arnés cruzado y para tus tests)

Construyen los **bytes exactos** de [11-request-reply.md §3.5](../specification/11-request-reply.md), sin reloj ni UUID por dentro:
`id` y `time` los das tú. Producen lo mismo en todos los SDKs.

```ts
import { rpcSubject, buildRpcRequest, buildRpcReply, buildRpcError, encodeRpcMessage, resolveRpcTimeout } from "@flux/sdk";

rpcSubject("clientes.cliente.v1.calcular-descuento");     // "rpc.clientes.cliente.v1.calcular-descuento"
resolveRpcTimeout(undefined);                              // 5000 (y 0, 25001 o 1.5 → INVALID_RPC_CALL)
encodeRpcMessage(buildRpcRequest({ operation, tenantId, aggregateId, id, source, time, correlationid, producerversion, classification, data }));
encodeRpcMessage(buildRpcReply({ request: { operation, id, correlationid, tenantid, subject }, id, source, time, producerversion, classification, data }));
encodeRpcMessage(buildRpcError({ request, id, source, time, producerversion, error: { class, code, message, retryAfterMs } }));
```

`test/rpc.test.ts` fija que coinciden con `conformance/cases/cross-sdk-rpc-envelope.json` (668, 703, 677 y 791 bytes, con su SHA-256) y
con el texto de la spec, y `conformance-harness.mjs` los expone como `rpc-request`, `rpc-reply`, `rpc-error`, `rpc-operation` y `rpc-timeout`.

### Lo que NO hace

- **No es exactamente-una-vez.** Un timeout es ambiguo y el broker no deduplica: la garantía es la idempotencia de quien responde.
- **No hay flujos, varias respuestas ni *scatter-gather***: una petición, una respuesta (`allow_responses` lo hace cumplir).
- **No propaga el plazo** de quien llama al que responde: un handler puede seguir cuando ya nadie espera, y su efecto se aplica.
- **No hay contrapresión**: core NATS no tiene acuse (ver `maxConcurrent`).
- **No elige instancias por salud**: una instancia colgada se lleva su parte (1/N) hasta que el broker nota que murió; se compensa con
  `retries` (mismo `id`).
- **No consulta ni crea nada de JetStream** para esto. `flux doctor` comprueba que ningún stream capture `rpc.>`.

## Seguridad: permisos, inbox, TLS y credenciales

Lo que [06-security.md](../specification/06-security.md) §2 y §3 exigen del SDK, y lo que
este hace. Todo lo de esta sección está **ejecutado contra un `nats-server` real** en
`test/integration/`: un permiso que solo se ha leído no es un permiso comprobado.

```ts
const bus = await connect({
  servers: "nats://nats.interno:4222",   // con `tls` el cliente exige el upgrade a TLS
  service: "facturacion-api",
  // …
  tls: { caFile: "/etc/flux/ca.pem" },                      // CA propia; + certFile/keyFile para mTLS
  credentials: { creds: "/run/secrets/facturacion.creds" }, // o credsContent, nkeySeed, token, user+pass
});
```

### Inbox privado — no es una opción

El SDK usa siempre `_INBOX.<servicio>` (derivado de `service`, que ya se valida como
`[a-z0-9-]`). Las respuestas de la API de JetStream y las **entregas de un pull consumer**
viajan por el inbox: con el `_INBOX.>` común, un servicio que se suscribe ahí lee los
eventos de dominios que no tiene concedidos. Es el hallazgo H2 del
[plan v1.1](../docs/plan-v1.1/PLAN.md), y se vio con claridad al probarlo: lo que llega al
espía es el evento completo, con su subject original.

> Que el SDK use inbox privado **no basta**: lo que confina es la ACL. Con la ACL antigua
> (`_INBOX.>`) un servicio sigue pudiendo espiar el inbox ajeno, aunque el SDK no use el
> común. `test/integration/acl.test.ts` (d) lo demuestra con un espía de control.

### Permisos que el SDK necesita

Mientras el generador de ACL v2 (`scripts/gen-acl.mjs`) no esté en `main`, la lista de
permisos requeridos es la del informe del PR de esta fase. La tabla es la que ejecutan los
tests (`test/integration/acl.ts`), y cada fila se probó **quitándola**: sin ella el SDK
falla de forma visible (`acl.test.ts`, «cada permiso de la lista es NECESARIO»).

`<DOM>` = dominio en mayúsculas con `_` (`EVT_PEDIDOS`); `<durable>` =
`<servicio>__<subject con . y - → _>`.

| Rol | `publish` | `subscribe` |
|---|---|---|
| Todo servicio | `$JS.API.INFO` | `_INBOX.<servicio>.>` |
| Dueño del dominio `<dom>` | `<dom>.>` · `$JS.API.STREAM.INFO.EVT_<DOM>` | — |
| Consumidor del subject `<S>` (dominio `<dom>`) | `dlq.<S>` · `$JS.API.STREAM.INFO.EVT_<DOM>` · `$JS.API.STREAM.INFO.DLQ_<DOM>` · `$JS.API.CONSUMER.INFO.EVT_<DOM>.<durable>` · `$JS.API.CONSUMER.MSG.NEXT.EVT_<DOM>.<durable>` · `$JS.ACK.EVT_<DOM>.<durable>.>` | — |
| Solo si el SDK ha de **crear** el consumidor (desarrollo) | `$JS.API.CONSUMER.CREATE.EVT_<DOM>.<durable>.<S>` | — |

- **Ningún permiso de suscripción sobre subjects de dominio**: el SDK consume por pull.
- `$JS.API.STREAM.INFO.DLQ_<DOM>` es el que la ACL «candidata» del plan no tenía: `subscribe()`
  comprueba que la DLQ existe **al arrancar**, no cuando llega el primer evento envenenado.
- `CONSUMER.CREATE` no hace falta con el consumidor aprovisionado: el SDK pregunta
  (`CONSUMER.INFO`) antes de crear. Si no existe y no puede crearlo → `NotProvisionedError`.
- Un `durable` propio en `subscribe({ durable })` debe llevar sus permisos con **ese** nombre.
- **Nunca** a un servicio: `$JS.API.CONSUMER.DURABLE.CREATE.*`, `$JS.API.STREAM.MSG.GET.*`, ni
  crear, modificar, borrar o purgar streams.
- ⚠️ En NATS, **`allow: []` no deniega nada: significa «sin restricción»**. Quitar el único
  permiso de suscripción de un usuario le da, de hecho, todos.

### Cómo llega cada denegación

El broker no siempre contesta. Medido contra NATS 2.14.5:

| Operación denegada | Qué ocurre | Qué hace el SDK |
|---|---|---|
| Cualquier petición de la API de JetStream (`connect()`, comprobar un stream, un consumidor) | Llega en ~1 ms como `RequestError` con `cause: PermissionViolationError`. **No es un timeout** | `PermissionDeniedError` (`PERMISSION_DENIED`), **PERMANENT** |
| `publish()` a un dominio ajeno | Lo mismo | `PermissionDeniedError` |
| Falta el stream o el consumidor y crearlo está denegado | Lo mismo, tras un «no existe» | `NotProvisionedError` (`NOT_PROVISIONED`), **PERMANENT** |
| Petición de mensajes de un pull (`CONSUMER.MSG.NEXT`) | **Silencio**: el consumidor queda inerte, parece sano | `subscribe()` lanza `PermissionDeniedError` |
| `ack` (`nak`, `term` y WIP publican en el mismo `$JS.ACK.…`; el denegado que se midió fue el ack) | **Silencio**: el evento se procesa y se **reentrega cada `ack_wait`, para siempre**, sin error | Solo un aviso en `logger.error` — **configura un `logger`** |
| Publicar en la DLQ (`dlq.<S>`) | La publicación falla | Se registra y no se termina el mensaje: se reentrega |

Las dos filas silenciosas existen porque `ack` y el pull son publicaciones sueltas, sin
respuesta que fallar; el broker solo manda un `-ERR` asíncrono. Por eso el SDK lo vigila, y
por eso la matriz de tests comprueba que cada permiso quitado se *nota*.

PERMANENT importa: un handler que provoque cualquiera de estos errores —por ejemplo
publicando en un dominio sin aprovisionar— va a la DLQ en la **primera** entrega
(04-errors.md) y no atasca la cola con reintentos que darían la misma respuesta.

### Aprovisionamiento: `flux provision`

Streams y consumidores se crean **fuera del SDK** en producción, con credenciales de
operación. Dejar que cualquier servicio cree el stream compartido de su dominio es una
carrera esperando a ocurrir, y su permiso de creación es justo el que la ACL exacta quita.

```
NotProvisionedError: el stream "EVT_NUEVOS" no existe y "fx-mixto" no tiene permiso para
crearlo (el broker denegó publicar en "$JS.API.STREAM.CREATE.EVT_NUEVOS"). En producción se
aprovisiona con `flux provision <services.json>`, usando credenciales de operación, no las
del servicio (06-security.md §3).
```

Con permiso de creación (desarrollo), el SDK sigue creándolos con la config canónica.

### TLS

```ts
tls: { caFile: "/etc/flux/ca.pem" }                                   // CA propia
tls: { caFile, certFile: "/etc/flux/cliente.pem", keyFile: "/etc/flux/cliente.key" }  // mTLS
tls: {}                                                                // TLS con las CA del sistema
```

Son **rutas**; las lee el cliente en cada (re)conexión. Una ruta ilegible, un `certFile` sin
`keyFile`, una clave desconocida o `tls: null` lanzan `InvalidTlsOptionsError` en `connect()`.

- Un certificado que no verifica (CA que falta o equivocada, host que no está en los SAN) hace
  fallar `connect()` en milisegundos con `ConnectionRejectedError` (`reason: "tls"`) y apunta a
  `tls.caFile`. La librería de NATS, con `waitOnFirstConnect`, **reintentaba para siempre sin
  decir nada**; el SDK distingue «el servidor no está» (se espera) de «esto no se arregla
  esperando» (se falla).
- Si se pide `tls` y el servidor no lo ofrece, falla: **nunca baja a texto plano**.
- mTLS sin certificado de cliente **no** da un error de TLS en el cliente: el servidor corta el
  handshake y el cliente solo ve un timeout. El motivo está en el log del servidor.

### Credenciales

Un **solo** método:

| Método | Opción |
|---|---|
| `.creds` por ruta (el recomendado) | `credentials: { creds: "/ruta/svc.creds" }` |
| `.creds` por contenido (gestor de secretos) | `credentials: { credsContent: "-----BEGIN NATS USER JWT-----…" }` |
| nkey | `credentials: { nkeySeed: "SU…" }` |
| token | `credentials: { token: "…" }` |
| usuario y contraseña | `credentials: { user: "…", pass: "…" }` |

- **`creds` es una ruta**, como `CredsFile` en Go y Rust. En v0.1 la opción existía pero el
  cliente NATS no tiene ninguna llamada `creds`: se **ignoraba en silencio** (H4) y el servicio
  conectaba sin autenticar o fallaba sin explicación.
- Todo lo mal formado lanza `InvalidCredentialsError` en `connect()`, antes de tocar la red: una
  clave que no existe (`password`, `credsFile`…), dos métodos a la vez, valores vacíos, una ruta
  ilegible, un `.creds` o una semilla que no se pueden usar. Los mensajes **nunca** incluyen el
  valor de la credencial.
- Credenciales que el broker rechaza → `ConnectionRejectedError` (`reason: "auth"`), sin
  reintentar.
- `connect()` **sí** espera a un broker que aún no ha arrancado (un servicio que levanta antes
  que NATS es lo normal en un compose).

## Desarrollo

```bash
npm run typecheck
npm test          # unitarios, y los de integración si hay nats-server (ver abajo)
npm run build

# Tests de integración: arrancan su PROPIO nats-server, con la config que cada caso escribe
GOBIN=$PWD/bin go install github.com/nats-io/nats-server/v2@v2.14.5
NATS_SERVER=$PWD/bin/nats-server npm test
# FLUX_REQUIRE_BROKER=1 hace que la ausencia del binario sea un FALLO y no un salto con aviso.
# Es lo que usa el CI: sin ella un binario que no compilara daría verde sin ejecutar nada.
# Puertos: 4400-4449 por defecto (FLUX_TEST_PORTS=desde-hasta). TLS necesita `openssl`.
# El estado replicado añade un cluster de TRES nodos (seis puertos: A-49, el conflicto 10164).
#   test/state.test.ts                         unitarios: bytes exactos, validación, algoritmo, métricas
#   test/integration/state.test.ts             permisos exactos: el ciclo de 100 claves y «quita un permiso y se nota»
#   test/integration/state-semantics.test.ts   conformance/cases/state-semantics.json ejecutado con esta API
# La petición/respuesta, contra una red EN MEMORIA que hace solo lo que se midió, y contra un broker real:
#   test/rpc.test.ts                           unitarios: bytes exactos, los seis códigos, las cinco comprobaciones, §5.3, firma, métricas
#   test/integration/rpc.test.ts               permisos exactos: dos instancias que reparten, RPC_UNAVAILABLE < 100 ms y «quita un permiso y se nota»

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
