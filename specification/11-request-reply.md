# 11 — Petición/respuesta

> **Estado:** extensión **opcional** de v1.1 (capacidad `rpc`, [00-protocol.md §5](00-protocol.md)).
> Un SDK L1/L2/L3 conforme no necesita implementarla; un servicio que solo use eventos o estado
> sigue siendo conforme.
>
> Cada observación citada como `A-n` está medida contra **nats-server 2.14.5** y descrita en el
> [Apéndice A del plan v1.1](../docs/plan-v1.1/PLAN.md#apéndice-a--evidencia). Las que son del broker
> son casos ejecutables de [`conformance/cases/rpc-semantics.json`](../conformance/cases/rpc-semantics.json)
> que ejecuta [`conformance/rpc-broker.mjs`](../conformance/rpc-broker.mjs). A-15 a A-18 salieron del plan
> original y se **reprodujeron** (A-56, A-68, A-66 y A-67, en ese orden); la fase añadió el resto hasta A-76, entre
> ellas las que cambian el diseño: **la «respuesta» de un stream que captura `rpc.>` gana casi siempre a la real**
> (A-67), **una suscripción con comodín convierte `no responders` en un timeout** (A-64) y **el
> `allow_responses` por defecto admite UNA respuesta y no caduca** (A-68).

---

## 1. Qué problema resuelve

Un evento dice **qué pasó**. El estado ([10-state.md](10-state.md)) lleva **el dato**. Lo que falta es
la **orden que necesita respuesta ya** y el **cálculo**: reservar el stock y saber si se pudo, pedir el
precio de un carrito, validar una dirección. Hoy eso es `POST /accion` y `GET /precio?…` entre servicios
propios, con todo lo que arrastra un HTTP interno: otro transporte, otras credenciales, otro formato
de error y ninguna clasificación de reintentos.

La **petición/respuesta** es una llamada con respuesta inmediata entre dos servicios flux, con **el mismo
sobre, el mismo `tenantid`, la misma clasificación de errores y los mismos permisos** que el resto del
protocolo:

| Hoy (HTTP) | Con petición/respuesta |
|---|---|
| `POST /accion` que necesita respuesta ya | `bus.request("<dominio>.<agregado>.v1.<operacion>", data)` |
| `GET /precio?…`: un **cálculo**, no un dato guardado | ídem |
| El servicio de al lado está caído: `ECONNREFUSED`, a veces tras segundos | `RPC_UNAVAILABLE` en milisegundos (A-56) |
| Un 503 de la otra parte: ¿se reintenta? | El error **lleva su clase** (`retryable` / `permanent`) y quien llama hereda la clasificación |

**La regla que evita recrear el problema.** Si lo que pides es *un dato que otro sistema guarda*, es
**estado**, no petición/respuesta: `state.get` / `state.watch`. La petición/respuesta queda para
**órdenes** que necesitan respuesta inmediata y para **cálculos**. Usarla para leer datos es volver a
tener una API con otro transporte —y devolver el problema que el estado existe para quitar—.

| Necesitas | Usa |
|---|---|
| Avisar de que algo ocurrió (y que cada interesado reaccione) | **Evento** |
| Un dato que guarda otro sistema; la carga inicial; mantener una copia al día | **Estado** |
| Una **orden** que necesita respuesta ya; un **cálculo** con parámetros | **Petición/respuesta** |
| Encolar trabajo sin esperar | **Evento** en el dominio de quien pide, más un evento de resultado unidos por `correlationid` |

**Qué no es.** No persiste nada: no hay historia, ni reentrega, ni DLQ. No es difusión: una petición
la atiende **una** instancia. No es un flujo: **una petición, una respuesta** (§7.2 lo hace cumplir el
broker). No sustituye al evento: la orden que cambia el mundo sigue publicando **eventos** para que los
demás reaccionen; la respuesta solo contesta a quien preguntó.

## 2. Modelo

### 2.1 Core NATS, sin stream

La petición/respuesta usa **core NATS**, no JetStream. Una petición no se persiste: es un mensaje con
subject de respuesta, y si nadie escucha, quien llama **lo sabe al instante** y no tras un timeout. Medido
(A-56): con un timeout de 5000 ms, la petición a una operación sin nadie falla en **1-2 ms** con
`no responders`.

```
rpc.<dominio>.<agregado>.v<major>.<operacion>        subject de NATS. SIN stream. SIN DLQ.
```

- **Ningún stream DEBE capturar `rpc.>`.** Si uno lo hace, quien llama recibe el acuse de JetStream
  como si fuera la respuesta y el que responde ejecuta igualmente (A-18, A-67): es la trampa de [§8](#8-la-trampa-un-stream-que-captura-rpc).
  `flux doctor` lo comprueba y el aprovisionamiento no crea ninguno.
- Es el espacio de nombres hermano de `dlq.` y `state.`: disjunto por construcción de los eventos, y
  por eso `rpc` es un dominio **reservado** ([02-naming.md §1.3](02-naming.md)).
- **Una petición NO se reentrega.** Core NATS es *at-most-once*: una petición recibida por una instancia
  que muere se pierde (A-62). Quien llama no puede distinguirlo de una operación lenta: por eso
  `RPC_TIMEOUT` es **ambiguo** y el que responde deduplica por `id` ([§4.5](#45-reintentos-e-idempotencia)).

### 2.2 El subject y la operación

```
subject de NATS:        rpc.clientes.cliente.v1.calcular-descuento      (5 tokens)
nombre de la operación:     clientes.cliente.v1.calcular-descuento      (4 tokens: lo que reciben request y respond)
```

| Token | Regla |
|---|---|
| `rpc` | Literal. Espacio de nombres del protocolo |
| `dominio`, `agregado`, `v<major>` | Los del contrato, como en [02-naming.md §1](02-naming.md): bounded context en plural, agregado en singular |
| `operacion` | **Infinitivo**, `kebab-case`: `obtener`, `crear`, `calcular-precio`, `reservar-stock` |

El **nombre de la operación** —el subject sin `rpc.`— tiene exactamente la forma sintáctica de un subject
de evento (`naming.subject.pattern`): cuatro tokens. Lo que separa una operación de un evento es el
prefijo `rpc.` del subject de NATS, del `type` y de la ruta del esquema, y la **forma gramatical** del
último token: **el evento va en pasado, la operación en infinitivo**. `clientes.cliente.v1.creado` es un
hecho; `clientes.cliente.v1.crear` es una orden.

```
✅ rpc.clientes.cliente.v1.obtener              ❌ rpc.clientes.cliente.v1.creado          pasado: es un evento
✅ rpc.clientes.cliente.v1.calcular-descuento   ❌ rpc.clientes.cliente.v1.obtener_cliente  guion bajo
✅ rpc.logistica.linea-envio.v2.reservar-stock  ❌ rpc.Clientes.cliente.v1.obtener          mayúsculas: subject fantasma
                                                ❌ rpc.clientes.cliente.v1.*                un comodín: el broker lo acepta (A-41)
```

- **El patrón fija la sintaxis, no la gramática.** Que el último token sea un infinitivo es una convención
  de revisión del PR de esquema, como lo es el pasado de un evento: ninguna regex distingue `obtener` de
  `obtenido` en todos los casos. Una operación llamada `…v1.obtener-cliente-por-email` casi siempre es
  una consulta a un almacén: es **estado**, no una operación.
- **La versión mayor está en el subject**, como en los eventos, para que la incompatibilidad sea física
  ([05-compatibility.md §1](05-compatibility.md)): un cliente de `v1` no puede alcanzar un `v2`.
- Un nombre que lleva el prefijo (`rpc.clientes.cliente.v1.obtener`) es un dominio reservado:
  **`RESERVED_DOMAIN`**, con un mensaje que diga que el prefijo lo pone el SDK. Uno que no cumple el patrón
  —mayúsculas, guion bajo, tres tokens, un comodín— es **`INVALID_RPC_CALL`**, siempre antes de tocar la
  red. Los vectores de [`cross-sdk-rpc-envelope.json`](../conformance/cases/cross-sdk-rpc-envelope.json)
  fijan el código de cada entrada, con coincidencia **completa** (no `$`: en Python y PHP el `$` acepta un
  `\n` final).

### 2.3 Quién responde: un subject exacto por operación, un grupo de cola por servicio

```ts
await bus.respond("clientes.cliente.v1.calcular-descuento", async (peticion, ctx) => {
  return { descuentoCents: 500, moneda: peticion.moneda };       // lanzar PermanentError / RetryableError para fallar
});
```

- **Solo el dueño responde.** La ACL lo hace cumplir: el dueño de un dominio es el único con permiso de
  suscripción sobre `rpc.<dominio>.>` ([§7](#7-seguridad)). Quien no lo es, no puede.
- **Una suscripción por operación, al subject EXACTO.** El permiso del dueño es `rpc.<dominio>.>`, pero
  **el SDK NO se suscribe a eso**: se suscribe a `rpc.<dominio>.<agregado>.v<major>.<operacion>` por cada
  `respond()`. Medido (A-64): con la suscripción exacta, una petición a una operación que el servicio no
  atiende (una errata, una versión aún no desplegada) falla en ~1 ms con `no responders`; con
  `rpc.<dominio>.>`, el servicio **recibe interés sobre todas las operaciones** y esa petición agota su
  timeout, porque nadie sabe contestarla. Lo que se **concede** no es lo que se **usa**.
- **Grupo de cola = nombre de servicio** (`naming.service.pattern`), el mismo para todas las operaciones
  de ese servicio. Dos instancias del mismo servicio se reparten las peticiones (§2.4).
- **El permiso y el comodín**: un dueño que se suscribe a `rpc.<dominio>.>` sin necesitarlo no es un
  problema de seguridad (la ACL ya lo concedía), sino de **diagnóstico**: convierte RPC_UNAVAILABLE en
  RPC_TIMEOUT.

### 2.4 Reparto entre instancias

Medido en un nodo suelto, con la librería de NATS de los SDKs:

| Situación | Qué pasa | Caso |
|---|---|---|
| Dos instancias, mismo grupo | Se reparten las peticiones, **al azar** y no por turnos: 295 y 305 de 600; 979 y 1021 de 2000. **Cada petición la atiende una** | A-58 |
| Una instancia deja de escuchar | Todo va a las demás: 100 de 100. Si era la última, `no responders` en ~1 ms | A-59 |
| Una instancia recibe la petición y **muere** | La petición **se pierde**: quien llama ve un timeout. La siguiente ya da `no responders` | A-62 |
| Una instancia está **colgada** (conexión abierta, no contesta) | Se lleva **su parte** (1/N) hasta que el broker nota que murió: de 40 peticiones, 18 a 21 timeouts. Al cerrarse la conexión, vuelve la normalidad | A-63 |
| Un cluster de 3 nodos, una instancia por nodo | Reparte entre los tres (180 / 221 / 199 de 600): no se queda en el nodo local | A-75 |

El reparto **no mira la salud de nadie**. Una instancia colgada no se detecta desde el protocolo: se
compensa reintentando con el **mismo `id`** —que de todos modos exige la ambigüedad del timeout— y, aguas
abajo, es de quien despliega vigilar la salud de sus instancias.

### 2.5 Dos servicios sobre la misma operación

Un grupo de cola solo reparte **dentro de su grupo**: dos grupos (dos **servicios**) sobre el mismo
subject reciben **una copia cada uno**, y **cada uno ejecuta la petición**. Quien llama recibe **una**
respuesta —la del servicio que conteste antes— y la otra se pierde en silencio (A-60).

Es el modo de fallo de una operación con dos dueños: un renombrado de servicio a medias, o dos equipos
que atienden lo mismo. La orden se ejecuta dos veces y nadie lo ve. Por eso **solo el dueño responde**, lo
impone la ACL, y `gen-acl.mjs` rechaza un manifiesto con un dominio de dos dueños
([06-security.md §3.0](06-security.md)).

### 2.6 Dominios reservados

`rpc` es un dominio reservado ([02-naming.md §1.3](02-naming.md)), como `dlq`, `state` y `system`. Toda API
que reciba una operación —`request`, `respond`— **DEBE** rechazar un dominio reservado con
**`RESERVED_DOMAIN`** (PERMANENT, [04-errors.md §6](04-errors.md)) **antes de tocar la red**, igual que
`publish`, `subscribe` y las cuatro operaciones de estado. **`publish()` no puede escribir en `rpc.`**:
publicaría una petición sin respuesta posible, y por core NATS.

## 3. El sobre

Petición y respuesta son **CloudEvents normales** ([01-envelope.md](01-envelope.md)) en *structured mode*,
con las mismas extensiones obligatorias, la misma validación L3 y la misma firma. **No hay ningún
atributo raíz nuevo**: todo lo propio de la petición/respuesta está en el subject, en el `type` y en
`data`. El orden de los atributos es el de siempre ([01-envelope.md §6](01-envelope.md)) y **`data` va
el último**.

### 3.1 La petición

El desarrollador escribe **la operación, `data` y, opcionalmente, `aggregateId`** (más las opciones de la
llamada: `timeoutMs`, `tenantId`, `classification`). **Todo lo demás lo rellena el SDK** — la regla 2 de
`AGENTS.md` no cambia.

| Atributo | Valor en una petición |
|---|---|
| `specversion`, `datacontenttype` | `"1.0"`, `application/json` |
| `id` | UUIDv7 que genera el SDK. **El mismo en todos los intentos** de una llamada (§4.5) |
| `source` | `/<entorno>/<servicio>` de **quien llama** |
| `type` | `com.flux.rpc.<dominio>.<agregado>.<operacion>.request.v<major>` |
| `time` | El instante en que se **construye** la petición. No se pasa de fuera |
| `dataschema` | `https://schemas.internal/rpc/<dominio>/<agregado>/<operacion>/request/<semver>.json`, resuelto en el índice `rpc` del bundle; sin bundle, `<major>.0.0` ([00-protocol.md §5](00-protocol.md)) |
| `subject` | El `aggregateId`, **si se da**: la identidad del agregado sobre el que se pide (`cli-1`). **No** es el subject de NATS |
| `correlationid` | El del evento o la petición **en curso**; si la llamada no nace de otro, **el `id` propio** |
| `causationid` | El `id` del evento o la petición en curso; **ausente** si no hay |
| `tenantid` | El del evento en curso; si no, el `tenantId` de `connect()`; si no, `system` — **como en los eventos** ([09-multitenancy.md §5](09-multitenancy.md)). Un estado exige uno explícito; la petición, no |
| `producerversion` | La de `connect()` |
| `dataclassification` | La de la llamada; si no, la de la conexión (§3.6) |
| `partitionkey` | El `aggregateId`, si se da: la misma convención que en los eventos, aunque aquí no particione nada |
| `traceparent` | El contexto de trazas activo |
| `tracestate` | El del evento o la petición **en curso**, junto a su `traceparent`; ausente si no hay |
| `data` | Un **objeto** JSON ([01-envelope.md §4](01-envelope.md)) |

**Una llamada hecha dentro de un handler continúa la cadena**: dentro de un handler de eventos o de otra
operación, `correlationid` y `tenantid` se heredan y el `causationid` es el `id` de lo que está en curso. Es lo
que hace que un flujo que cruza eventos y operaciones se siga con un solo `correlationid`.

### 3.2 La respuesta

La respuesta **hereda** de la petición y **no se escribe a mano**:

| Atributo | Valor en una respuesta |
|---|---|
| `id` | UUIDv7 **nuevo**: nunca el de la petición |
| `source` | `/<entorno>/<servicio>` de **quien responde** |
| `type` | `com.flux.rpc.<dominio>.<agregado>.<operacion>.reply.v<major>`; el de un error, `com.flux.rpc.error.v1` (§3.3) |
| `time` | El instante en que se construye la respuesta |
| `dataschema` | El de la respuesta de la operación (`…/reply/<semver>.json`); el de un error, el fijo de §3.3 |
| `correlationid` | **El de la petición** |
| `causationid` | **El `id` de la petición** |
| `tenantid` | **El de la petición**, sin modificar |
| `subject`, `partitionkey` | Los de la petición, si los traía |
| `dataclassification` | La de la operación; si no, la de la conexión. Un error, `internal` |
| `traceparent` | El contexto del que responde, **de la misma traza** |
| `tracestate` | **Ausente**: la respuesta y el error llevan solo `traceparent`. Rust lo escribía si se lo daban y Node no |
| `data` | Lo que devuelve el handler: un **objeto** |

Va dirigida al subject de respuesta (`reply`) de la petición, que es un inbox privado y único por llamada
([§7.3](#73-el-inbox-privado)). **Una petición, una respuesta**: un segundo mensaje al mismo `reply` lo deniega
el broker ([§7.2](#72-allow_responses-una-respuesta-medida)) y quien llama ignora cualquiera que llegue
después.

### 3.3 El error

Cuando el handler falla, o cuando el SDK de quien responde rechaza la petición, la respuesta es un
**error**: un CloudEvent con `type: com.flux.rpc.error.v1` y

```json
"data": { "class": "retryable", "code": "HTTP_503", "message": "el servicio de tarifas no responde", "retryAfterMs": 5000 }
```

| Campo | Regla |
|---|---|
| `class` | **`retryable` o `permanent`**, y nada más. `poison` **no viaja** (más abajo) |
| `code` | Identificador **estable** (`errors.codePattern`): el que agrupa el panel y el que ramifica la aplicación. Nunca el mensaje |
| `message` | Qué falló y cómo se arregla. De **1 a 1000 caracteres Unicode** (puntos de código, como `maxLength` de JSON Schema: mil emojis caben; el «tamaño» en unidades UTF-16 de JavaScript o en bytes no cuenta). **NO DEBE** contener valores de `data` (acaba en logs) ni —para un error no lanzado a propósito— el texto de una excepción. Quien responde **trunca** el de la aplicación que no cabe a 999 caracteres + `…`, y sustituye uno vacío o solo de espacios por «el servicio rechazó la petición» |
| `retryAfterMs` | Opcional, entero ≥ 0, **solo con `retryable`**: una sugerencia para el *primer* reintento, como en [04-errors.md §2.2](04-errors.md) |

- **`dataclassification: internal`**, siempre, y `dataschema` fijo:
  `https://schemas.internal/system/rpc/error/1.0.0.json` (esquema en
  `schemas/system/rpc/error/1.0.0.json`). Es **el mismo tipo para toda operación**: la respuesta de error
  no depende del contrato de la operación.
- **Quien llama lo convierte** en `RetryableError` (con `code`, `message`, `retryAfterMs`) o
  `PermanentError`. **Un handler de eventos que llama a una operación hereda la clasificación sin
  hacer nada**: si el otro servicio dice `retryable`, el evento se reentrega con su backoff; si dice
  `permanent`, va a la DLQ en la primera entrega.
- **`poison` no viaja.** POISON es la calificación de quien *consume* un mensaje ilegible
  ([04-errors.md §1.3](04-errors.md)). Para quien llama, una petición que el otro lado no pudo interpretar
  es `permanent`: reintentarla da lo mismo. El que responde la cuenta como `poison` en **su** métrica
  (§10) y **alerta** —ese es el aviso que debe despertar a alguien—.
- **Un error desconocido del handler es `permanent`** (§5.3), no `retryable`. El presupuesto acotado de
  [04-errors.md §2.1](04-errors.md) se cuenta por **entregas** de un consumidor y una llamada no tiene número
  de entrega: con `retryable`, el handler de quien llama reintentaría seis veces en 51 minutos un bug del
  otro servicio. *Decidido provisionalmente.*
- **De dónde sale cada código.** Los de la aplicación (`PEDIDO_YA_CANCELADO`) los pone el handler con
  `PermanentError`/`RetryableError`; los que clasifica el SDK (`HTTP_503`, `ECONNRESET`) salen del
  clasificador ([04-errors.md §1.1](04-errors.md)); y los que pone el **SDK de quien responde** son
  `INVALID_SCHEMA`, `STALE_REQUEST`, `RPC_MESSAGE_TOO_LARGE`, `INVALID_RPC_CALL`, los de la firma
  ([07-signing.md §7](07-signing.md)) y los del sobre ([01-envelope.md §3.1](01-envelope.md)).

### 3.4 Sin cabeceras

**Ninguna cabecera de NATS es obligatoria ni la lee el SDK**, salvo el **estado 503** con el que el broker
dice `no responders` (§4.3). `traceparent`, `correlationid` y `tenantid` viajan **solo en el sobre**.

Se midió que las cabeceras **sí** viajan en peticiones y respuestas (A-74): el que responde ve las de la
petición y quien llama recibe las que el otro puso. Se decide no usarlas porque:

- **Un mensaje es un JSON completo** ([01-envelope.md §1](01-envelope.md)): se copia, se pega en un test y se
  reproduce. Con los metadatos en cabeceras, un `nats req` o una captura devuelven el cuerpo sin ellos.
- **Las cabeceras abren una segunda fuente de verdad** que puede discrepar del sobre (un `tenantid` en
  cada sitio) sin que nada lo detecte.
- **Ninguna ACL las ve** y **la firma no las cubre** ([07-signing.md §5](07-signing.md)).

`Nats-Msg-Id` **no deduplica** en core NATS: dos peticiones con el mismo se entregan **dos veces** (A-74).
La ventana de duplicados es de JetStream. Un SDK **NO DEBE** apoyarse en ella; la dedupe de una orden es
la del `id` del sobre.

### 3.5 Ejemplos byte a byte

Cuatro líneas, sin espacios ni saltos entre atributos, UTF-8 literal (`Otoño`, `Campaña`, `petición`: sin
escapes `\u`, [01-envelope.md §1.1](01-envelope.md)). Lo que está entre los bytes es el contrato: si un SDK
produce otra secuencia para la misma entrada, está mal. Las reproduce el caso
[`cross-sdk-rpc-envelope`](../conformance/cases/cross-sdk-rpc-envelope.json) con su SHA-256, y
`rpc-broker.mjs` los reconstruye desde sus datos.

**Petición** a `clientes.cliente.v1.calcular-descuento`, tenant `acme`, sobre el agregado `cli-1`. No nace
de otro evento: `correlationid` es su propio `id` y no hay `causationid` (subject de NATS:
`rpc.clientes.cliente.v1.calcular-descuento`; 668 bytes):

```
{"specversion":"1.0","id":"01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10","source":"/produccion/facturacion-api","type":"com.flux.rpc.clientes.cliente.calcular-descuento.request.v1","time":"2026-08-20T10:41:07.318Z","datacontenttype":"application/json","dataschema":"https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json","subject":"cli-1","correlationid":"01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10","tenantid":"acme","producerversion":"3.4.1","dataclassification":"internal","partitionkey":"cli-1","traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","data":{"clienteId":"cli-1","totalCents":9990,"moneda":"EUR","campana":"Otoño"}}
```

SHA-256: `43ea71ade9fb6ff81ccb7c1f93047ffebd604d602492229e452b1c97dadf83f5`.

**Respuesta** (703 bytes). Hereda `correlationid`, `tenantid`, `subject` y `partitionkey`; su `causationid` es el `id`
de la petición; `source`, `producerversion` y `traceparent` (misma traza, otro *span*) son del que responde:

```
{"specversion":"1.0","id":"01924f91-5d0f-7e03-9b6a-1c2d3e4f5a67","source":"/produccion/crm-api","type":"com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1","time":"2026-08-20T10:41:07.329Z","datacontenttype":"application/json","dataschema":"https://schemas.internal/rpc/clientes/cliente/calcular-descuento/reply/1.0.0.json","subject":"cli-1","correlationid":"01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10","tenantid":"acme","producerversion":"2.1.0","dataclassification":"internal","causationid":"01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10","partitionkey":"cli-1","traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01","data":{"descuentoCents":500,"moneda":"EUR","motivo":"Campaña de otoño"}}
```

SHA-256: `b164fa71fa3e2943f3cb160f1a7cf3c520b09c2d93a0a1da81eef7f17da1f01c`.

**Error de un handler** (el handler lanzó `RetryableError("el servicio de tarifas no responde", { code: "HTTP_503", retryAfterMs: 5000 })`;
677 bytes). `type` y `dataschema` fijos, `data` con las claves de §3.3 en ese orden:

```
{"specversion":"1.0","id":"01924f91-5d11-7a62-8f0d-4e5f6a7b8c91","source":"/produccion/crm-api","type":"com.flux.rpc.error.v1","time":"2026-08-20T10:41:07.331Z","datacontenttype":"application/json","dataschema":"https://schemas.internal/system/rpc/error/1.0.0.json","subject":"cli-1","correlationid":"01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10","tenantid":"acme","producerversion":"2.1.0","dataclassification":"internal","causationid":"01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10","partitionkey":"cli-1","traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01","data":{"class":"retryable","code":"HTTP_503","message":"el servicio de tarifas no responde","retryAfterMs":5000}}
```

SHA-256: `fdff124948254dfa3263971aec86bf4d0184db77babe44da5e0714f28fc427f8`.

**Error del SDK** de quien responde, que rechaza la petición por su esquema (791 bytes). El mensaje cita
**rutas** (`/totalCents`), nunca valores:

```
{"specversion":"1.0","id":"01924f91-5d13-7b85-a3e1-6a7b8c9d0e22","source":"/produccion/crm-api","type":"com.flux.rpc.error.v1","time":"2026-08-20T10:41:07.333Z","datacontenttype":"application/json","dataschema":"https://schemas.internal/system/rpc/error/1.0.0.json","subject":"cli-1","correlationid":"01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10","tenantid":"acme","producerversion":"2.1.0","dataclassification":"internal","causationid":"01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10","partitionkey":"cli-1","traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01","data":{"class":"permanent","code":"INVALID_SCHEMA","message":"la petición no cumple https://schemas.internal/rpc/clientes/cliente/calcular-descuento/request/1.0.0.json: /totalCents debe ser un entero, /moneda es obligatoria"}}
```

SHA-256: `c6f80891c94a6c64defa825288f9f98201bed5824d7c6e94c58362dccbfc446d`.

Se comprueban con `JSON.stringify` (Node) y con `json.dumps(ensure_ascii=False, separators=(",", ":"))`
(Python): los mismos bytes. Los tres últimos son alternativas para **la misma petición**: por eso comparten
su `id` en `causationid`.

### 3.6 Clasificación de datos y datos personales

- **`dataclassification`**: la de la llamada o la operación; si no, la de la conexión, como en un evento.
  Lo que decide es **qué se registra**: `confidential` y `restricted` no se escriben en logs
  ([06-security.md §5](06-security.md)). **El error es siempre `internal`.**
- **Leer un dato personal guardado por otro sistema NO es una operación: es estado**
  ([10-state.md §7](10-state.md)). `obtener-cliente` devolviendo su email es un `GET /clientes/123` con otro
  transporte.
- **Un dato personal como PARÁMETRO de una orden o RESULTADO de un cálculo SÍ PUEDE viajar** en `data`, con
  `"x-pii": true` en el esquema y la clasificación que corresponda (`confidential`; un dato de los de
  `restricted` exige además la aprobación que pide [06-security.md §5](06-security.md)). La diferencia con
  un evento es que **el broker no lo persiste**: no hay stream, ni 30 días de retención, ni derecho de supresión
  que un log append-only no pueda cumplir. *Decidido provisionalmente.*
- **Eso solo es verdad mientras ningún stream capture `rpc.>`.** Con uno, cada petición y su dato
  personal quedarían **guardados con la retención del stream** (A-67, el stream guardó las 20 peticiones):
  de ahí que `flux doctor` ([§8](#8-la-trampa-un-stream-que-captura-rpc)) sea obligatorio y no una comodidad.
- Un SDK **DEBERÍA** redactar los campos `x-pii` en logs y **NO DEBE** poner `data` en métricas ni en
  atributos de traza ([08-observability.md §6](08-observability.md)).

## 4. Llamar

```ts
const r = await bus.request("clientes.cliente.v1.calcular-descuento",
  { clienteId: "cli-1", totalCents: 9990, moneda: "EUR" }, { timeoutMs: 2000, aggregateId: "cli-1" });
// → { descuentoCents: 500, moneda: "EUR", motivo: "…" }
```

`request` resuelve con el **`data` de la respuesta**. Un SDK **PUEDE** exponer también el sobre
(`id`, `time`, `correlationid`).

### 4.1 El algoritmo

```
0. Validar y construir SIN red, en este orden (el primero que falla gana):
     RESERVED_DOMAIN → INVALID_RPC_CALL (operación, timeoutMs, data objeto) → INVALID_DATACLASSIFICATION →
     L3: data contra el esquema de la petición (INVALID_SCHEMA, todos los errores) →
     construir el sobre UNA vez (id, time, bytes) → tamaño (RPC_MESSAGE_TOO_LARGE).
1. Publicar por core NATS en rpc.<operación>, con un subject de respuesta propio (el inbox privado), por una
   conexión que anunció `headers` y `no_responders` en CONNECT.
2. Esperar, lo primero que ocurra:
     · un mensaje de estado 503 sin cuerpo          → RPC_UNAVAILABLE
     · un -ERR 'Permissions Violation' de la conexión
       que nombra el subject de la operación         → PERMISSION_DENIED
     · un mensaje de respuesta                       → paso 3
     · se agota el timeout                           → RPC_TIMEOUT
3. Comprobar que es la respuesta a ESTA petición (§4.3): si no, RPC_INVALID_REPLY.
   Con la firma en `require`: verificar (MISSING_SIGNATURE, INVALID_SIGNATURE, UNKNOWN_SIGNING_KEY).
4. Si es un error (`com.flux.rpc.error.v1`): lanzar RetryableError o PermanentError según su `class`.
5. Si es una respuesta: con L3, validar `data` contra el esquema de la respuesta (INVALID_SCHEMA); devolverla.
```

- **El broker no contesta a lo que deniega** (A-28): descarta el mensaje y avisa aparte con un
  `-ERR 'Permissions Violation for Publish to "…"'` por la conexión. Por eso el paso 2 compite el aviso con
  la respuesta ([§4.3](#43-qué-ve-quien-llama)).
- **Una petición, una respuesta.** Tras la primera respuesta válida, cualquier otra que llegue al mismo inbox se
  ignora. Una respuesta **tardía** —tras el timeout— también: el broker no la descarta (A-61), la ignora quien
  llama.
- **El tenant** de la llamada se resuelve como en los eventos (§3.1): el del evento en curso, el de `connect()` y,
  si no hay, `system`.

### 4.2 El timeout

| | |
|---|---|
| **Por defecto** | **5 s** (5000 ms) |
| **Mínimo / máximo** | 1 ms / **25 s** (25 000 ms): un entero. Fuera de rango es **`INVALID_RPC_CALL`**, no se recorta en silencio |
| **Por qué 25 s** | **Por debajo del `ack_wait` de 30 s** ([03-delivery.md §2](03-delivery.md)): para que una llamada hecha desde un handler de eventos no provoque su reentrega mientras espera |
| **Qué cuenta** | **Un intento.** La suma de varias llamadas dentro de un handler es de la aplicación (con *work-in-progress* el handler puede superar los 30 s) |

### 4.3 Qué ve quien llama

Cada fila está medida contra un broker real (los casos son los de [§2](#2-modelo) y [§7](#7-seguridad)).
**Todo lo que no es una respuesta se traduce a un código estable**:

| Situación | Qué ocurre | Qué ve quien llama | Clase |
|---|---|---|---|
| Nadie escucha la operación | El broker contesta `no responders` (estado 503) en 1-2 ms (A-56). **Solo si la conexión lo pidió en CONNECT**: sin ello, silencio (A-57) | **`RPC_UNAVAILABLE`** | RETRYABLE, **inmediato** |
| El servicio existe pero no atiende **esa** operación | Lo mismo, porque se suscribe al subject exacto (A-64) | `RPC_UNAVAILABLE` | RETRYABLE |
| Responde **más tarde** que el timeout | La operación **se ejecuta**; la respuesta llega al inbox y se ignora (A-61) | **`RPC_TIMEOUT`** | RETRYABLE |
| La instancia **muere** con la petición en la mano | Se pierde, sin reentrega (A-62) | `RPC_TIMEOUT` | RETRYABLE |
| Una instancia **colgada** se llevó su parte | Sin respuesta hasta que el broker note la caída (A-63) | `RPC_TIMEOUT` | RETRYABLE |
| La petición sale **justo** cuando se va el último que responde (cluster) | Una ventana de unos ms: timeout en vez de 503 (A-70) | `RPC_TIMEOUT` | RETRYABLE |
| La ACL **deniega** la petición | Con `@nats-io/*`: `RequestError` en ~1 ms que nombra el subject (A-66). Con `async-nats`: **solo un timeout**, y el `-ERR` por el evento de la conexión (A-72) | **`PERMISSION_DENIED`** — el SDK **DEBE** escuchar el aviso y competirlo con la respuesta; si no, se ve como `RPC_TIMEOUT` | PERMANENT |
| La **respuesta** no cabe en `max_payload` | La librería de **quien responde** lanza; sin más, quien llama solo ve un timeout (A-65). Por eso el que responde contesta con un error | **`RPC_MESSAGE_TOO_LARGE`** | PERMANENT |
| Un **stream** captura la operación | Quien llama recibe el acuse `{"stream":…}` (A-67). Con un stream `no_ack`: timeout (el stream cuenta como interés) | **`RPC_INVALID_REPLY`** (o `RPC_TIMEOUT`) | PERMANENT |
| El que responde falló | Un error `com.flux.rpc.error.v1` | `RetryableError` / `PermanentError` con su `code` | Su `class` |
| El dueño **no tiene permiso** de suscripción | Parece un servicio caído (A-73): `no responders` | `RPC_UNAVAILABLE` — el problema se ve en **el dueño**, que DEBE fallar al arrancar (§7.1) | — |

**Qué comprueba quien llama de lo que recibe** (si falla cualquiera: **`RPC_INVALID_REPLY`**, PERMANENT):

1. Es un **CloudEvent válido** (parsea, con las cuatro extensiones y los tipos exactos).
2. Su `type` es **el de la respuesta de esa operación** o `com.flux.rpc.error.v1`.
3. Su `causationid` es **el `id` de la petición**.
4. Su `correlationid` y su `tenantid` **son los de la petición**. **Excepción única**: el error de una petición que
   el que responde no pudo leer ([§5.2](#52-lo-que-no-cumple-y-qué-se-contesta)) lleva `correlationid` propio y
   `tenantid` = `system`; ahí quien llama **no** aplica esta comprobación, y le basta la 3 (su `causationid` es el
   `id` de la petición, que solo él conoce) para atribuirlo y ver el código del sobre (`INVALID_RPC_CALL`,
   `MALFORMED_JSON`…) en vez de `RPC_INVALID_REPLY`. Sin esa excepción, un error legítimo sería rechazado por la
   comprobación que existe para detectar lo ilegítimo. **La forma es estrecha a propósito**: la excepción exige que sea
   un **error** (`com.flux.rpc.error.v1`), con `tenantid` = `system` **y** `correlationid` **igual al `id` del propio
   error**. Un `tenantid: system` cualquiera, un `correlationid` cualquiera o una respuesta que no es un error siguen
   siendo `RPC_INVALID_REPLY`; y las comprobaciones 3 y 5 siguen en pie. Medida contra un broker real con un responder en
   bruto (Node, Rust y `flux call`; A-120).
5. Si es un error, su `data` cumple el esquema del error (`class` ∈ `retryable | permanent`, `code` y `message`).

**El SDK NO DEBE suscribirse, por la misma conexión que pide, a un subject que case con el `reply` de sus
peticiones** (`_INBOX.<servicio>.>` o `_INBOX.<servicio>.*.*`): el servidor ve interés y **no envía el
`no responders`**, de modo que `RPC_UNAVAILABLE` se convierte en `RPC_TIMEOUT` (A-101: 6 de 6 peticiones, con la
suscripción antes o después). Con otra conexión de la misma identidad sí llega. Usa el buzón único que ya crea la
librería para las peticiones.

Una respuesta que no es de esta petición es **un incidente** —un productor roto, un stream mal puesto,
algo hostil— y no un fallo que esperar: de ahí PERMANENT y su propio `outcome` en la métrica (§10). Si el
cuerpo parece el acuse de un stream (`{"stream":…,"seq":…}`), el mensaje **DEBERÍA** decir que algún stream
captura `rpc.>` y apuntar a `flux doctor`.

### 4.4 Tamaño

El mensaje serializado **NO DEBE** superar el **mínimo** entre **1 MiB** ([01-envelope.md §7](01-envelope.md)) y el
`max_payload` que anuncia el broker en su `INFO`. Las **cabeceras cuentan** (A-65); como no usamos ninguna, el
límite efectivo es el del cuerpo.

- Un SDK **DEBE** fallar **antes de abrir nada** con **`RPC_MESSAGE_TOO_LARGE`** y un mensaje que nombre los
  bytes y el límite y apunte al *claim-check* ([01-envelope.md §7](01-envelope.md)), o a paginar con un cursor
  en `data`. Medido (A-65): `max_payload` por defecto **1 048 576**; **exactamente** ese tamaño pasa; uno más lo
  rechaza la librería al instante (Node: `InvalidArgumentError`; `async-nats`: `MaxPayloadExceeded`); y un
  cliente en bruto que lo envía recibe `-ERR 'Maximum Payload Violation'` y **le cierran la conexión**.
- **La respuesta tiene el mismo límite** y es la que falla en silencio (§5.4).

### 4.5 Reintentos e idempotencia

**Una orden puede llegar dos veces.** Un reintento tras un timeout —la operación **pudo haberse ejecutado**
(A-61, A-62, A-63, A-70)— o dos copias. El broker **no deduplica** en core (A-74). Por eso:

- **El que responde DEBE deduplicar por el `id` de la petición**, con una de las tres estrategias de
  [03-delivery.md §4](03-delivery.md) (tabla de procesados, operación idempotente, clave de idempotencia
  externa derivada del `id`). Un handler de una orden sin deduplicación está roto aunque los tests pasen.
- **`request` hace UN intento.** Un SDK **PUEDE** ofrecer reintentos (`retries`, por defecto **0**), solo ante
  **`RPC_UNAVAILABLE` y `RPC_TIMEOUT`**; nunca ante un error permanente. **Cada reintento reutiliza el mismo
  `id` y los mismos bytes** —`time` incluido—: un `id` nuevo por intento haría imposible reconocerlo.
- **Lo que NO cubre.** Un handler de **eventos** que reintenta por *reentrega* construye una petición
  **nueva** (otro `id`). Si la orden debe aplicarse exactamente una vez a través de reentregas, el que
  responde deduplica por una **clave de negocio** que viaja en `data` (`pedidoId`), no solo por el `id`.
- **Sin ventana en el SDK.** Un SDK **NO DEBE** recordar ids para rechazar repetidos: rechazaría un reintento
  legítimo. La dedupe vive donde vive el efecto.

## 5. Responder

### 5.1 El algoritmo

```
1. Recibir en la suscripción exacta de la operación (grupo de cola = nombre de servicio).
2. Sin subject de respuesta → NO se atiende: nadie espera el resultado de una orden. Log + outcome `poison`.
3. Interpretar el sobre (01-envelope.md): JSON, atributos obligatorios, extensiones, tipos exactos.
     Falla → error `permanent` con el código del sobre (MALFORMED_JSON, MISSING_REQUIRED_EXTENSION, …). outcome `poison`.
4. `type` = el de la petición de ESTA operación. Si no → error `permanent` INVALID_RPC_CALL. outcome `poison`.
5. Con la firma en `require`: verificar (MISSING_SIGNATURE, INVALID_SIGNATURE, UNKNOWN_SIGNING_KEY → error
     `permanent`, outcome `invalid_signature`) y comprobar que |ahora − time| ≤ 300 s (si no, STALE_REQUEST, outcome `stale_request`).
6. Con L3: validar `data` contra el esquema de la petición. Falla → error `permanent` INVALID_SCHEMA con TODAS
     las rutas, el handler NO se invoca. outcome `invalid_schema`. (Modo `warn`: log y métrica, y se sigue.)
7. Invocar el handler con (data, ctx).
8. Su resultado es un objeto. Con L3: validar contra el esquema de la respuesta (si falla: error `permanent`
     INVALID_SCHEMA, outcome `invalid_schema`). Tamaño: si no cabe, error RPC_MESSAGE_TOO_LARGE.
9. Construir la respuesta (§3.2), firmarla si procede, publicarla al `reply` de la petición. outcome `ok`.
   Si el handler lanzó: §5.3.
```

`ctx` lleva **el sobre de la petición** (`event`), su `id`, `correlationId`, `tenantId`, `aggregateId` y `time`.
**Un handler de operación continúa la cadena**: lo que publique o llame desde él hereda `correlationid` y
`tenantid` y su `causationid` es el `id` de la petición ([§3.1](#31-la-petición)).

### 5.2 Lo que no cumple, y qué se contesta

**Siempre se contesta con un error, nunca en silencio**: callar dejaría a quien llama esperando su timeout
para un fallo que no se arregla esperando —el modo de fallo que la taxonomía existe para evitar—.

| Lo que llega | Qué hace el que responde | `code` | outcome |
|---|---|---|---|
| Sin subject de respuesta (`nats pub` a mano) | **No se atiende**; log | — | `poison` |
| JSON roto, falta una extensión, tipo equivocado, `specversion` desconocido | Error `permanent` con el código del sobre | los de [01-envelope.md §3.1](01-envelope.md) | `poison` |
| `type` que no es el de la petición de esta operación | Error `permanent` | `INVALID_RPC_CALL` | `poison` |
| Firma ausente, inválida o con clave desconocida (modo `require`) | Error `permanent` | `MISSING_SIGNATURE`, `INVALID_SIGNATURE`, `UNKNOWN_SIGNING_KEY` | `invalid_signature` |
| `time` a más de 300 s del reloj (modo `require`) | Error `permanent` | `STALE_REQUEST` | `stale_request` |
| `data` que no cumple el esquema (L3) | Error `permanent`, **sin invocar al handler**; **todas** las rutas, **ningún valor** | `INVALID_SCHEMA` | `invalid_schema` |
| La respuesta del handler no cumple su esquema (L3) | Error `permanent`: el fallo es **del que responde**, no de quien llama | `INVALID_SCHEMA` | `invalid_schema` |
| La respuesta no cabe en `max_payload` | Error `permanent` | `RPC_MESSAGE_TOO_LARGE` | `permanent` |
| El handler lanza | §5.3 | el del error | `retryable` / `permanent` |

- **Si la petición ni siquiera se puede leer**, no hay `correlationid` ni `tenantid` que heredar: el error lleva
  `correlationid` = su propio `id`, `tenantid` = `system` (es un aviso de plataforma sobre un mensaje que no se
  pudo atribuir) y `causationid` = el `id` de la petición **solo si es una cadena válida**: **no vacía, de 128
  caracteres Unicode como máximo y sin caracteres de control** (U+0000–U+001F ni U+007F–U+009F). Es lo que el error cita:
  un valor que quien llama no reconocería como suyo, o que llena un log, no se copia. Node aceptaba hasta 256 y
  controles; Rust, hasta 128 y sin ellos. *Decidido provisionalmente.*
- **El mensaje cita rutas, no valores**, con una frase que diga **qué** estaba mal —«la petición no cumple…» o
  «la respuesta no cumple…»— porque quien llama recibe el mismo `INVALID_SCHEMA` en los dos casos y debe saber a
  quién arreglar. Es la regla de [10-state.md §3.6](10-state.md), por la misma razón.
- **Con `warn`**, un fallo de esquema no detiene al handler: queda en el log y en la métrica
  (`invalid_schema`). Con L3 apagado no se valida nada.

### 5.3 Los errores del handler

| El handler… | Se responde | `code` | `message` |
|---|---|---|---|
| lanza `RetryableError` | `retryable` (con `retryAfterMs` si lo da) | el suyo, o `RETRYABLE` | el de la aplicación |
| lanza `PermanentError` | `permanent` | el suyo, o `PERMANENT` | el de la aplicación |
| lanza un `PoisonError` (la calificación de quien *consume*, no de quien responde) | `permanent`: `poison` no viaja | el suyo, o **`PERMANENT`** —el código de la clase con la que viaja, no el de la que no viaja—: Node contestaba `POISON` y Rust `PERMANENT` | el de la aplicación |
| lanza un error que el **clasificador reconoce** (un 503 de una dependencia, `ECONNRESET`) | su clase (`retryable` / `permanent`) | el del clasificador (`HTTP_503`) si cumple `errors.codePattern`; si no, `RETRYABLE` / `PERMANENT` | **genérico** («dependencia no disponible») |
| lanza un error **desconocido** | **`permanent`** | `UNKNOWN` | **genérico**: «error interno del servicio» |
| devuelve algo que no es un objeto | `permanent` | `INVALID_SCHEMA` | «la respuesta no es un objeto» |

- **Un error que no lanzó a propósito la aplicación NO reenvía el texto de la excepción**: puede llevar valores
  de `data`, rutas del servidor o datos personales. Va al log del que responde, con el `id` de la petición para
  encontrarlo.
- **Lo desconocido es `permanent`** y no «retryable acotado» (§3.3): sin número de entrega no se puede acotar.
  La política configurable de [04-errors.md §2.1](04-errors.md) (`permanent` / `retryable` / `retryable-bounded`)
  se aplica así: `permanent` y `retryable-bounded` dan `permanent`; `retryable` da `retryable`.
  *Decidido provisionalmente.*

### 5.4 El tamaño de la respuesta

Si es la **respuesta** la que no cabe, la librería de NATS lanza **en quien responde** y **quien llama solo ve un
timeout** (A-65: `async-nats` da `MaxPayloadExceeded` al que responde y `TimedOut` al que llama; `@nats-io/*`,
`InvalidArgumentError` y `TimeoutError`). Por eso el que responde **DEBE comprobar el tamaño serializado antes
de publicar** y, si excede, **contestar con un error `RPC_MESSAGE_TOO_LARGE`** (PERMANENT) que diga los bytes y el
límite. Es la única vía por la que quien llama se entera.

### 5.5 Parar un responder

El orden es **dejar de escuchar, esperar a lo que está en vuelo, cerrar**:

1. **Dejar de escuchar** cada operación con el mecanismo de **drenaje** de la librería (UNSUB y espera a lo que ya
   viaja), no con una baja que descarte lo que aún llega. Desde ese instante, las peticiones nuevas dan
   `no responders` al instante (A-62) y no caen en una instancia que se va.
2. **Esperar a los handlers en vuelo**, con un tope de **25 s** (el timeout máximo: una petición más lenta ya
   la dio por perdida quien llamó).
3. **Cerrar** la conexión.

**Cerrar la conexión de golpe pierde lo que se atiende** —y el `drain()` de la **conexión entera** tampoco espera a los
handlers: el que está en vuelo no puede publicar su respuesta y quien llama ve un timeout (A-83); hay que
drenar **cada operación** y esperar a los handlers *antes* de cerrar—: medido (A-62), con un handler de 400 ms, cerrar a los
100 ms deja la petición sin respuesta; con `unsubscribe` a los 100 ms, la petición en vuelo termina con su
respuesta y la siguiente falla en ~1 ms con `no responders`.

> **Medido con latencia de red real (A-91, `async-nats` 0.50).** El `drain` **no espera al `PONG`**: retira la
> suscripción en cuanto escribe el `UNSUB`, y lo que el servidor ya envió y aún viaja **se pierde**; quien llama ve
> `RPC_TIMEOUT`. Con la librería sola se pierden 0 / 4 / 13 / 23 mensajes con 0 / 5 / 15 / 30 ms por sentido, y
> `unsubscribe` casi lo mismo (1 / 5 / 15 / 28): ninguno de los dos lo evita, y la API pública no ofrece otra
> cosa. Con el SDK, 10 paradas bajo carga pierden de 1 a ~130 peticiones con 0 a 15 ms. **Por eso la parada
> ordenada no basta bajo carga y con red**: lo que hay que hacer es **sacar la instancia del reparto antes de
> pararla** (quitarla del balanceo o dejar de enviarle tráfico) y entonces parar. Localmente (0 ms) no se pierde
> nada. Y en un **cluster**, el interés se propaga de forma asíncrona: la petición que sale justo cuando se va
> el último puede agotar su timeout en vez de recibir `no responders` (A-70: 4 de 10 sin esperar, 0 de 10 a los 5
> ms). Un SDK **DEBERÍA** esperar un margen breve tras dejar de escuchar antes de cerrar.

### 5.6 Tenants y concurrencia

- **El responder NO filtra por el tenant de `connect()`.** El grupo de cola es **el servicio**, y no sabe de
  tenants: una instancia atada a `acme` y otra a `globex` bajo el mismo grupo recibirían cada una **la mitad
  de las peticiones de los dos**, y quien llama vería timeouts. El tenant de la petición llega al handler
  (`ctx.tenantId`) y **es suyo autorizar**. Si lo que se necesita es que un tenant no pueda alcanzar el servicio
  de otro, es el **Modelo B** (una account por tenant, [09-multitenancy.md](09-multitenancy.md)): cada account
  tiene su propio espacio `rpc.`. `tenantIsolation: "strict"` no afecta a `respond`. *Decidido provisionalmente.*
- **Concurrencia y contrapresión: no se especifican ni se han medido.** Core NATS no tiene acuse: un responder
  sobrecargado no puede decir «ahora no» ([§13](#13-lo-que-la-peticiónrespuesta-no-resuelve)). Un SDK **DEBERÍA** limitar los
  handlers concurrentes (el mismo orden que `max_ack_pending`, 256), y el exceso espera en la cola de la
  suscripción.

## 6. Errores

La lista de códigos propios es **cerrada**: **seis**. Un caso que no encaje en ninguno es un hueco de la spec,
no un código que cada SDK inventa. Se documentan en [04-errors.md §7](04-errors.md) y en `protocol.json` →
`errors.codes`, con su capacidad `rpc`.

| Código | Clase | Lo lanza | Cuándo |
|---|---|---|---|
| `RPC_UNAVAILABLE` | **RETRYABLE**, inmediato | `request` | Nadie escucha la operación: `no responders` (A-56) |
| `RPC_TIMEOUT` | **RETRYABLE** | `request` | No llega respuesta a tiempo. **Ambiguo: la operación pudo ejecutarse** |
| `STALE_REQUEST` | **PERMANENT** | quien responde (en un error) | Firma en `require` y `time` a más de 300 s |
| `RPC_INVALID_REPLY` | **PERMANENT** | `request` | Lo que llega no es la respuesta a esta petición (§4.3) |
| `RPC_MESSAGE_TOO_LARGE` | **PERMANENT** | `request`; quien responde (en un error) | El mensaje no cabe (§4.4, §5.4) |
| `INVALID_RPC_CALL` | **PERMANENT** | `request`, `respond`; quien responde (en un error); **los constructores del sobre** | Operación mal formada (la versión mayor, de 1 a 4 dígitos), `timeoutMs` fuera de rango, `data` que no es un objeto; o, en quien responde, un `type` que no es el de la operación. En un **constructor** (el arnés cruzado: `rpc-request`, `rpc-reply`, `rpc-error`): un atributo que rellena el SDK que llega vacío, un `time` que no es un instante RFC 3339, o un error que no cumple su esquema (§3.3): un código estable, nunca una excepción sin código |

**Por qué estos seis y no menos.** `RPC_UNAVAILABLE` y `RPC_TIMEOUT` son las dos situaciones que **solo existe
aquí** y con **clases distintas en la práctica**: la primera no gasta tiempo, la segunda sí y es ambigua.
`STALE_REQUEST` y `RPC_INVALID_REPLY` son las dos defensas del protocolo —contra una petición repetida y contra
una respuesta que no es de quien debe— y cada una se alerta por separado. `RPC_MESSAGE_TOO_LARGE` es el **único
fallo silencioso** (la respuesta que no cabe se ve como un timeout, A-65), y por eso necesita un código que lo
saque a la luz. `INVALID_RPC_CALL` agrupa tres equivocaciones de quien programa con **un** código, como
`INVALID_TLS_OPTIONS` con las opciones de TLS: el remedio y quien lo aplica son los mismos, y el mensaje dice
cuál.

**Lo que no es un código nuevo.** Se reutilizan:

| Situación | Código |
|---|---|
| Dominio reservado (`rpc.clientes…` con el prefijo, `state.x…`) | `RESERVED_DOMAIN` ([04-errors.md §6](04-errors.md)) |
| `data` no cumple el esquema de la petición o de la respuesta | `INVALID_SCHEMA` |
| Clasificación fuera del enum | `INVALID_DATACLASSIFICATION` |
| La ACL deniega la petición o la suscripción | `PERMISSION_DENIED` ([04-errors.md §5](04-errors.md)) |
| Firma ausente, inválida o con clave desconocida | `MISSING_SIGNATURE`, `INVALID_SIGNATURE`, `UNKNOWN_SIGNING_KEY` |
| Sobre ilegible o incompleto | Los de [01-envelope.md §3.1](01-envelope.md) |

`NOT_PROVISIONED` **no aplica**: no hay streams ni consumidores que aprovisionar. Un dueño sin permiso de
suscripción es `PERMISSION_DENIED` (§7.1), no `NOT_PROVISIONED`.

## 7. Seguridad

### 7.1 Quién puede qué

| Quién | Permisos |
|---|---|
| **Quien llama** | **Publicar** en el subject **exacto** de cada operación concedida (`rpc.clientes.cliente.v1.obtener`), y **suscribirse** a su inbox privado `_INBOX.<servicio>.>`. Nada más |
| **El dueño** | **Suscribirse** a `rpc.<dominio>.>` y `allow_responses`. **Ningún permiso de publicación sobre inboxes** |

- **Nunca** a quien llama `rpc.>`, `rpc.<dominio>.>` ni un comodín por token: podría llamar a todo.
  `protocol.json` → `rpc.acl` fija los patrones (`callerPublish`, `ownerSubscribe`) y lo que nunca se concede.
- **El generador** (`gen-acl.mjs`, las claves `calls` y `serves` del manifiesto y los casos de seguridad S21 a S29) está
  documentado en [06-security.md §3.9](06-security.md#39-peticiónrespuesta-calls-serves-y-sus-permisos); esta sección fija
  **lo medido** que produce.
- **Una petición denegada**: con `@nats-io/*` falla en ~1 ms con `Permissions Violation for Publish to
  "rpc.clientes.cliente.v1.obtener"` (A-66); con `async-nats`, **agota su timeout** y el aviso solo se ve en el
  evento de la conexión (A-72). La regla de [06-security.md §3.7](06-security.md) rige aquí: el SDK **DEBE**
  escuchar el `-ERR` y competirlo con la respuesta, y lanzar `PERMISSION_DENIED` con el subject. Si no, un
  permiso que falta se ve como `RPC_TIMEOUT` (RETRYABLE) y el handler que llama **reintenta 51 minutos** algo
  que nunca va a funcionar.
- **Un dueño sin permiso de suscripción parece un servicio caído**: quien llama recibe `no responders` (A-73) y el
  dueño arranca «sano». `respond()` **DEBE** fallar con `PERMISSION_DENIED` nombrando el subject —la
  suscripción denegada no lanza al suscribirse: el aviso llega aparte, A-8f—, no arrancar mudo.

### 7.2 `allow_responses`: una respuesta, medida

El dueño **no** necesita permiso de publicación sobre ningún inbox: lo da `allow_responses` (A-16). Medido
(A-68), con el dueño sin ningún permiso de publicación sobre `_INBOX.*`:

| | `allow_responses` | Resultado |
|---|---|---|
| El dueño responde **tres veces** a una petición | `true` | Se entrega **una**; las otras dos son una violación de permiso en su conexión |
| Responde a los **2 s** | `true` | Se entrega: **no caduca** por defecto |
| Cuatro respuestas | `{ max: 3 }` | Se entregan 3 y se deniega 1 |
| Responde a los **1,5 s** | `{ max: 1, expires: "1s" }` | **Se deniega**; la de 0,3 s sí pasa |
| Publica en **otro** inbox, no el `reply` de la petición | `true` | **Se deniega**; la respuesta legítima llega |
| Responde | *ausente* | **Se deniega**; quien llama ve un timeout |

- **`true` admite una respuesta y no caduca**: lo que cierra «una petición, una respuesta» (§3.2).
- **DEBERÍA acotarse**: `allow_responses: { max: 1, expires: 30s }`. Como el timeout máximo es 25 s (§4.2), una
  respuesta legítima **nunca** llega después, y un dueño comprometido deja de poder escribir en los inboxes
  que ha visto. Lo decide el generador de permisos.
- **Sin `allow_responses` el dueño no puede contestar** y desde fuera solo se ve `RPC_TIMEOUT`: es el fallo
  de una ACL que lo olvida. Un `-ERR` por cada respuesta denegada queda en el log del servidor y en los avisos
  de la conexión del que responde, que el SDK **DEBE** registrar ([06-security.md §3.7](06-security.md)).

### 7.3 El inbox privado

Las respuestas viajan por el inbox de quien llama. **Sin el inbox privado de la Fase 1, cualquiera leería las
respuestas ajenas**: medido (A-71, el H2 de [06-security.md §3.3](06-security.md) con respuestas), un servicio
con `_INBOX.>` recibió **la respuesta completa** dirigida a otro; uno con `_INBOX.<servicio>.>` vio denegada la
suscripción al inbox ajeno. En petición/respuesta esto importa más que en los eventos, porque una respuesta
**puede llevar datos personales** (§3.6). El SDK fija `_INBOX.<servicio>` desde el nombre de servicio; no es
una opción.

### 7.4 Firma y ventana de 300 s

La respuesta, y los errores, van **firmados por el que responde** si tiene la firma activa
([07-signing.md](07-signing.md)); quien llama las verifica en `require`.

**Con la firma en `require`, el que responde rechaza una petición cuyo `time` se aleje más de 300 s de su reloj**
—por delante o por detrás— con **`STALE_REQUEST`** (PERMANENT). La firma por sí sola **no impide reenviar una
petición capturada**: es un mensaje firmado y válido para siempre. La ventana acota cuánto tiempo sirve.

- **Solo con `require`.** Sin firma, `time` es un campo que cualquiera escribe y la ventana no protegería
  nada; en `warn` el SDK tampoco rechaza, y por coherencia tampoco aplica la ventana.
- **Es simétrica**: una petición del futuro (reloj adelantado) es tan sospechosa como una vieja. El mensaje
  dice el `time`, la ventana y la hora local.
- **Dentro de la ventana, la defensa es la idempotencia por `id`**, la misma que protege los reintentos. El
  SDK **NO** recuerda ids ([§4.5](#45-reintentos-e-idempotencia)).
- **Presupone relojes sincronizados** (NTP). Un servicio con un reloj desviado más de 300 s recibe
  `STALE_REQUEST` en todas sus llamadas: es PERMANENT para que se note, no para que se reintente.

## 8. La trampa: un stream que captura `rpc.>`

Un stream de JetStream que captura el subject de una operación contesta a cada petición con **su acuse**,
`{"stream":"TRAMPA","seq":1}`, **porque la petición es un `publish` más y el stream confirma lo que guarda**.
Medido (A-18, A-67):

- Con un stream sobre `rpc.x.>` **y un responder real escuchando**, de 20 peticiones **entre 18 y 20 de las
  «respuestas» fueron el acuse** (en una exploración de 400 seguidas, 394 y 6; con 400 concurrentes, 341 y
  59). Es una **carrera** entre el acuse y la respuesta real: casi siempre gana el acuse, **no siempre** —el
  síntoma es intermitente, peor de diagnosticar que uno constante—.
- **El que responde ejecuta igualmente.** La orden se aplica y quien llama recibe una respuesta que no es suya.
- **El stream persiste cada petición** —sus datos personales incluidos— con la retención del stream.
- Con el stream sin nadie escuchando, la petición **también «responde»**.
- Un stream **sin acuse** (`no_ack`) no contesta, pero **cuenta como interés**: `no responders` pasa a ser un
  timeout.

Es el mismo fallo que un `publish` de core a un subject sin stream, al revés: **acepta, no da error y aplica
otra cosa**.

**Cómo se evita:**

1. **`flux provision` no crea ningún stream cuyos subjects se solapen con `rpc.*.*.*.*`**: los suyos son
   `EVT_<DOM>` (`<dom>.>`, y `rpc` es un dominio reservado), `DLQ_<DOM>` (`dlq.…`) y `STATE_<DOM>` (`state.…`).
2. **`flux doctor` DEBE** comprobar, con credenciales de operación, que **ningún stream** de la account
   captura una operación: para cada subject de cada stream, **¿se solapa con `rpc.*.*.*.*`?** Es **error**, no
   aviso.
3. **Quien llama valida la respuesta** (§4.3): el acuse no es un CloudEvent y da `RPC_INVALID_REPLY` con la pista.

**El solape** es token a token: `*` casa un token y `>` casa uno o más al final; dos subjects se solapan si
**algún subject concreto casa con los dos**. Se compara contra `rpc.*.*.*.*` —**la forma de todo subject de
operación**, `rpc.` y cuatro tokens— y no contra `rpc.>`, que marcaría de más un filtro de seis tokens que no
puede capturar ninguna. El algoritmo está en `protocol.json` → `rpc.doctor` y el runner lo ejecuta contra un
broker con **16 patrones** (A-69): el broker captura exactamente donde el algoritmo predice.

| Filtro de stream | ¿Captura? | Se crea… |
|---|---|---|
| `rpc.>`, `rpc.*.>`, `rpc.clientes.>`, la operación, `rpc.*.*.*.*`, `rpc.*.cliente.v1.*` | **Sí** | con acuse: responde con él |
| `>`, `*.>`, `*.*.*.*.*` | **Sí** | **solo sin acuse**: JetStream exige `no_ack` cuando captura todo o se solapa con su API (`$JS.API.>`) |
| `clientes.>`, `dlq.>`, `state.>`, `rpc.otro.>`, `rpc.clientes.cliente.v1.crear` | No | con acuse (el doctor marca `rpc.otro.>` y la última: se solapan con la forma de rpc) |
| `*.*.*.*`, `*.*.*.*.*.*` | No | solo sin acuse; no capturan una operación de cinco tokens |

Un filtro de auditoría `>` es el caso real: un operador que «quiere verlo todo» lo crea sin acuse —única forma
posible— y **persiste cada petición y cada respuesta** sin que nadie se entere, porque no devuelve ningún
acuse. Por eso el doctor marca los anchos igual.

## 9. Un cluster de tres nodos

Medido con tres nodos locales (A-70, A-75):

- **El reparto no se queda en el nodo local**: quien llama en el nodo 0 y una instancia del grupo en cada nodo
  recibe 180 / 221 / 199 de 600 (286 / 312 / 302 de 900 en la exploración). No se asume lo contrario: una
  preferencia por el nodo local habría concentrado toda la carga en una instancia, sin ningún error.
- **Un responder en otro nodo** contesta en **p50 0,55 ms, p99 1,14 ms** (local, sin latencia de red real:
  exploración, sin caso).
- **Ventana al irse el último**: el interés se propaga de forma asíncrona. Si la petición sale **justo** cuando
  el último responder se va, puede **agotar su timeout** en vez de dar `no responders` (4 de 10 sin esperar, 0
  de 10 con 5 ms; en un nodo suelto, 1 de 10). Pasados ~150 ms, `no responders` en ~1 ms. Es la tercera razón
  —tras A-61 y A-62— de que `RPC_TIMEOUT` no diga «no se ejecutó».
- Lo que **no** se midió: la latencia entre nodos de una red real, ni una partición.

## 10. Observabilidad

[08-observability.md §2.5](08-observability.md) define tres métricas para los SDKs con la capacidad `rpc`, en su
bloque (`protocol.json` → `rpc.metrics`), **fuera de las siete** de todo SDK L2:

| Métrica | Tipo | Etiquetas |
|---|---|---|
| `flux_rpc_client_requests_total` | Counter | `operation`, `outcome` |
| `flux_rpc_server_requests_total` | Counter | `operation`, `outcome` |
| `flux_rpc_duration_seconds` | Histogram | `operation`, `role` |

```
operation = <dominio>.<agregado>.v<major>.<operacion>        ("clientes.cliente.v1.calcular-descuento")
role      = client | server
```

**Nunca** `tenantid`, `id`, `correlationid`, ni **quién llama** (servicio o cliente) como etiqueta: una serie
por cada uno. En **trazas** sí: un span por llamada y otro por petición atendida, con `traceparent` en el sobre.
Las alertas que importan: **`poison` del servidor** (alguien publica a mano o un SDK roto), **`invalid_reply`
del cliente** (la trampa de §8) y la tasa de **`unavailable`** y **`timeout`** por operación.

## 11. Esquemas y compatibilidad

### 11.1 Dónde viven

```
schemas/rpc/<dominio>/<agregado>/<operacion>/request/<major>.<minor>.<patch>.json
schemas/rpc/<dominio>/<agregado>/<operacion>/reply/<major>.<minor>.<patch>.json
schemas/system/rpc/error/1.0.0.json                       el error, fijo para toda operación
```

Inmutables una vez publicados, con el `$id` igual a su ruta ([05-compatibility.md §4](05-compatibility.md)). Dos
cosas que `bundle-schemas.mjs` y `check-compat.mjs` comprueban y que son **error con pista**, no aviso:

- **Una operación es un PAR**: petición y respuesta **del mismo mayor**. Un esquema sin su par —o suelto en la
  carpeta de la operación, o con un nombre que no es SemVer— falla.
- **`data` es un objeto en la raíz**, en los dos lados.

El bundle gana un índice **`rpc`**: nombre de la operación → `{ request, reply }`, con la URI de **la versión más
alta de su mayor**, de cada lado. El error **no** está en el índice (su URI es constante) pero sí en `schemas`.
Con alguna operación, el esquema del error es obligatorio.

### 11.2 Compatibilidad, con la dirección invertida

[05-compatibility.md](05-compatibility.md) vale igual, **con la dirección invertida donde corresponde**. Un
esquema describe datos que fluyen de quien **escribe** a quien **lee**, y en cada lado son otros:

| | Escribe | Lee | |
|---|---|---|---|
| **Respuesta** | el dueño | **los llamantes**: muchos, independientes, a su ritmo | Como un evento |
| **Petición** | **los llamantes** | el dueño: **uno**, que publica el esquema y lo despliega | **Invertido** |

- **Un valor nuevo en un enum de una RESPUESTA exige `x-extensible-enum`**: los llamantes que no lo conocen
  tienen que tolerarlo (log y seguir, nunca lanzar).
- **En una PETICIÓN no**: el que lee es el dueño, que se despliega con el esquema y ya conoce el valor. Añadirlo no
  rompe a ningún llamante.
- **Todo lo demás rige igual en los dos lados**: quitar o renombrar un campo, cambiar un tipo, pasar opcional a
  requerido (**incluido un campo requerido nuevo en la petición**: los llamantes viejos no lo envían), endurecer
  una restricción o cambiar la semántica obliga a un **mayor** nuevo y a doble publicación durante 90 días.
- **El orden de despliegue: primero quien responde, después quien llama.** Una petición con un campo opcional
  nuevo contra una instancia que aún valida con el esquema viejo (y `additionalProperties: false`) es
  `INVALID_SCHEMA`. Los llamantes toman el esquema nuevo del bundle cuando el dueño ya lo ha desplegado.

`check-compat.mjs` aplica la inversión: en un esquema `request/`, no exige `x-extensible-enum` al añadir un valor;
en `reply/` sí. Ver sus pruebas en negativo.

## 12. Qué DEBE un SDK `rpc`

Un SDK declara la capacidad `rpc` ([00-protocol.md §5](00-protocol.md)) solo si cumple **todo**:

1. `request` y `respond` según §4 y §5, y los **mismos bytes** que §3.5 para la petición, la respuesta y los dos
   errores (arnés cruzado: `rpc-request`, `rpc-reply`, `rpc-error`, `rpc-operation`, `rpc-timeout`).
2. Los **seis** códigos de [04-errors.md §7](04-errors.md), con su clase, y `RESERVED_DOMAIN` en `request` y `respond`
   (hoy solo lo comprueba `check-sdk-codes.mjs` en quien declara `state`, porque el código cuelga de esa capacidad).
3. **Una suscripción exacta por operación** y el grupo de cola = nombre de servicio.
4. La conexión anuncia **`headers` y `no_responders`**.
5. **Valida la respuesta contra la petición** (las cinco comprobaciones de §4.3).
6. **Una petición denegada falla rápido** con `PERMISSION_DENIED`, también con una librería que no entrega la
   denegación (§7.1), y un dueño sin permiso falla al arrancar.
7. **Parada ordenada** (§5.5) y el tamaño de la respuesta comprobado antes de publicar (§5.4).
8. Resuelve la operación en el índice `rpc` del bundle, no por red.
9. Ejecuta, contra un broker real y con los permisos que genera `gen-acl.mjs`, el ciclo del criterio de
   aceptación: **una operación atendida por dos instancias reparte las peticiones; al parar ambas, quien llama
   recibe `RPC_UNAVAILABLE` en menos de 100 ms**, y quitar un permiso **se nota**.

**Herramientas.** `flux doctor` comprueba §8. `flux call <operacion> --data '{…}'` **PUEDE** existir para depurar:
construye un sobre **como un SDK** (`id` generado, `source` `/<entorno>/flux-cli`, tenant explícito) y nunca lo
escribe a mano.

## 13. Lo que la petición/respuesta NO resuelve

Declarado para que nadie asuma de más:

- **No es exactamente-una-vez.** Un timeout es ambiguo y el broker no deduplica. La garantía es la **idempotencia
  del que responde** (§4.5), y a través de reentregas de un handler de eventos, una clave de negocio.
- **No hay flujos, varias respuestas ni *scatter-gather*.** Una petición, una respuesta (§7.2). Para pedir a todos,
  un evento; para paginar, un cursor en `data`.
- **No propaga el plazo** de quien llama al que responde: un handler puede seguir trabajando cuando ya nadie
  espera, y su efecto se aplica. La idempotencia por `id` cubre el caso peligroso.
- **No hay contrapresión ni *load shedding*.** Core NATS no tiene acuse: un responder sobrecargado no puede decir
  «ahora no», y quien llama solo ve `RPC_TIMEOUT`. **La cola de una suscripción tiene un límite y, llena, descarta
  sin error**: con `async-nats` 0.50 son **65 536** mensajes y los 4 463 que sobraron de 70 000 peticiones se
  perdieron (A-92); el SDK de Rust lo avisa por su `logger`. No se midió con `nats.js`.
- **No elige instancias por salud** (§2.4): una colgada se lleva su parte.
- **No oculta la existencia de una operación** a quien tenga permiso de publicar en ella, y `no responders` dice
  si **hay** alguien escuchando.
- **No se probó sobre una red real** (latencia entre nodos, particiones) ni con carga sostenida.

## 14. Decidido provisionalmente

Puntos que el plan dejaba abiertos y que se cierran aquí con el criterio que se explica en cada sección. Son del
dueño del proyecto si prefiere otra cosa; cambiar cualquiera de ellos más tarde es barato **ahora** y deja de
serlo cuando haya servicios en producción:

| # | Decisión | Sección |
|---|---|---|
| 1 | El nombre de la operación va en **infinitivo**, `kebab-case`; la regex fija la sintaxis, no la gramática | §2.2 |
| 2 | Una **suscripción exacta** por operación, nunca `rpc.<dominio>.>` (el permiso sí lo es) | §2.3 |
| 3 | Sin cabeceras de NATS: el sobre es la única fuente de `traceparent`, `correlationid` y `tenantid` | §3.4 |
| 4 | `class` del error es **binaria**; `poison` no viaja | §3.3 |
| 5 | Un error **desconocido** del handler es `permanent`, con mensaje genérico | §3.3, §5.3 |
| 6 | Si la petición no se puede leer, el error lleva `correlationid` propio y `tenantid: system` | §5.2 |
| 7 | Una petición **sin subject de respuesta** no se atiende | §5.1 |
| 8 | **Seis** códigos; `INVALID_RPC_CALL` agrupa operación, timeout y `data` | §6 |
| 9 | Timeout 1 ms – 25 s, por defecto 5 s; fuera de rango es un error, no se recorta | §4.2 |
| 10 | `request` hace **un** intento; reintentos opcionales, por defecto 0, con el mismo `id` | §4.5 |
| 11 | Ventana de `STALE_REQUEST` de **300 s**, simétrica, solo con la firma en `require`; el SDK no recuerda ids | §7.4 |
| 12 | El responder **no filtra** por el tenant de `connect()` | §5.6 |
| 13 | El dato personal **PUEDE** viajar como parámetro o resultado, con `x-pii`; el broker no lo persiste | §3.6 |
| 14 | La respuesta se valida contra la petición (cinco comprobaciones): `RPC_INVALID_REPLY` | §4.3 |
| 15 | `allow_responses: { max: 1, expires: 30s }` recomendado (DEBERÍA) | §7.2 |
| 16 | El doctor compara contra `rpc.*.*.*.*` y marca también los filtros anchos sin acuse | §8 |
| 17 | La compatibilidad se invierte para la petición: sin `x-extensible-enum`; el dueño despliega primero | §11.2 |
| 18 | El SDK limita los handlers concurrentes (~256) | §5.6 |
| 19 | La excepción de la comprobación 4 es **estrecha**: un error con `tenantid: system` y `correlationid` igual a su propio `id` | §4.3 |
| 20 | `message` del error: 1000 **caracteres Unicode**, no unidades UTF-16 ni bytes | §3.3 |
| 21 | El `id` que cita un error de una petición ilegible: no vacío, ≤ 128 caracteres, sin controles | §5.2 |
| 22 | Un `PoisonError` del handler lleva `PERMANENT` si no trae código; un mensaje solo de espacios se sustituye | §5.3 |
| 23 | Los constructores del sobre rechazan lo vacío, un `time` que no es un instante y un error mal formado con `INVALID_RPC_CALL` | §6 |
