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
