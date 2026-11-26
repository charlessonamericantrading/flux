# flux — Event Protocol v1

> **Estado:** Draft normativo
> **Versión:** 1.1.0
> **Última revisión:** 2026-11-26

> **1.1 es aditiva.** Las decisiones de v1.0 no cambian; el estado replicado
> ([10-state.md](10-state.md)) y la petición/respuesta ([11-request-reply.md](11-request-reply.md)) entran
> como **extensiones opcionales**, igual que la firma: un servicio que solo use eventos sigue siendo conforme.

---

## 1. Propósito

Este documento define el contrato que **todo** productor y consumidor de eventos del
ecosistema debe cumplir, con independencia del lenguaje en que esté escrito.

El objetivo explícito es que **añadir un lenguaje nuevo al ecosistema sea un trabajo
de días, no de meses**. Eso solo es posible si la totalidad de la semántica vive en
el protocolo y ninguna vive en los SDKs.

## 2. Lenguaje normativo

Las palabras **DEBE**, **NO DEBE**, **DEBERÍA**, **NO DEBERÍA** y **PUEDE** se
interpretan según [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119).

## 3. Modelo de capas

Cada capa depende únicamente de la inmediatamente inferior. Una capa **NO DEBE**
filtrar detalles hacia arriba.

| # | Capa | Responsabilidad | ¿Reemplazable? |
|---|---|---|---|
| 4 | **Aplicación** | Lógica de negocio, handlers | — |
| 3 | **SDK** | Serialización, acks, retries, contexto, DLQ | Sí, por lenguaje |
| 2 | **Protocolo** | Envelope, naming, semántica de entrega, errores | No sin RFC |
| 1 | **Binding** | Mapeo protocolo ↔ broker concreto | Sí, por broker |
| 0 | **Broker** | Transporte y persistencia (NATS JetStream) | Sí |

La consecuencia práctica: sustituir NATS por Kafka es un cambio en las capas 0–1.
Las capas 2–4 no se enteran. **Ese desacoplamiento es la razón de ser de flux**; si
un SDK expone un tipo de NATS en su API pública, es un bug del SDK.

## 4. Qué es y qué no es un evento en flux

Un evento es **un hecho consumado del pasado, inmutable, emitido por el dueño del
dato.**

**DEBE** cumplir:
- Nombrarse en pasado (`creado`, `cancelado`, `enviado`), nunca en imperativo.
- Ser publicado únicamente por el servicio propietario del agregado.
- Contener suficiente información para que un consumidor razonable actúe sin tener
  que llamar de vuelta al productor.
- Ser válido para siempre: reprocesar un evento de hace un año **DEBE** seguir
  siendo posible.

**NO DEBE** usarse como:
- **Comando disfrazado.** `pedidos.pedido.v1.crear` no es un evento. Si necesitas una orden con
  respuesta inmediata, usa la **petición/respuesta** ([11-request-reply.md](11-request-reply.md)): una operación
  `rpc.pedidos.pedido.v1.crear`, en infinitivo, que no es un evento ni se persiste.
- **Transporte de blobs.** Si el payload supera **1 MiB**, publica una referencia
  (URI + checksum), no el contenido. Ver [01-envelope.md §7](01-envelope.md).
- **Sustituto de una tabla.** Un evento no es una fila. Un evento es un cambio. Si lo que
  necesitas es la fila —el valor actual de un agregado—, es **estado**
  ([10-state.md](10-state.md)).

## 5. Niveles de conformidad

Un SDK declara su nivel. Los consumidores del SDK saben así qué esperar.

### L1 — Publisher/Subscriber
- **DEBE** producir CloudEvents 1.0 válidos según [01-envelope.md](01-envelope.md).
- **DEBE** rellenar automáticamente `id`, `source`, `time`, `specversion`, `type`.
- **DEBE** usar ack explícito (nunca auto-ack).
- **DEBE** establecer la cabecera `Nats-Msg-Id` = `id` del CloudEvent.

### L2 — Resiliente
L1, más:
- **DEBE** implementar la taxonomía de errores de [04-errors.md](04-errors.md).
- **DEBE** aplicar el backoff canónico y enrutar a DLQ al agotarlo.
- **DEBE** propagar `correlationid` y `traceparent` desde el evento entrante al
  saliente sin intervención del desarrollador.
- **DEBE** reconectar con backoff exponencial y jitter, y en la **primera** conexión esperar
  a un broker que aún no ha arrancado pero **no** esperar lo que reintentar no arregla
  ([03-delivery.md §6.1](03-delivery.md)).

### L3 — Gobernado
L2, más:
- **DEBE** resolver y validar `dataschema` antes de publicar, y **fallar el
  `publish()`** si el payload no cumple su esquema.
- **DEBE** reportar TODOS los errores de validación, no solo el primero: de uno en
  uno, arreglar un payload con tres campos mal cuesta tres despliegues.
- **DEBERÍA** ofrecer un modo `warn` para introducir validación en un ecosistema en
  marcha sin romper nada el primer día.
- **PUEDE** validar también al consumir. Un fallo ahí se clasifica **PERMANENT**: el
  evento es sintácticamente correcto pero incumple su contrato, y reintentarlo dará
  exactamente el mismo resultado.

#### Resolución de esquemas: bundle, no HTTP

El `dataschema` es una URI, pero un SDK L3 **NO DEBE** resolverla por red en
`publish()`. Validar está en la ruta caliente: una petición por evento es
inaceptable, y una caché con TTL abre una ventana en la que dos servicios validan
contra versiones distintas del mismo esquema.

En su lugar, los esquemas se empaquetan y se despliegan **con el servicio**
([`scripts/bundle-schemas.mjs`](../scripts/bundle-schemas.mjs)). Así la versión del
esquema queda clavada a la versión del servicio — que es justo lo que
`producerversion` promete poder acotar.

El bundle resuelve además el `dataschema` exacto: dentro de un mayor todo es
BACKWARD-compatible, así que el MINOR más alto acepta todo lo que aceptan los
anteriores.

> **Nota de implementación.** Los esquemas de flux declaran `$schema: draft/2020-12`.
> Un validador configurado para draft-07 no falla con un error de versión: falla con
> `no schema with key or ref ".../draft/2020-12/schema"`, que no dice nada útil.
> En `ajv` hay que usar `ajv/dist/2020`, no el export por defecto; en Rust, el crate
> `jsonschema`; en PHP, `opis/json-schema` 2.4+; en Python, `jsonschema` 4.18+ eligiendo
> el validador por el `$schema` de cada esquema; en Go, `santhosh-tekuri/jsonschema/v6`;
> en Java, `com.networknt:json-schema-validator` fijando `VersionFlag.V202012`; en .NET,
> `JsonSchema.Net` — **`System.Text.Json` no valida JSON Schema**, y `NJsonSchema`, que es
> lo que suele aparecer al buscar, está anclado en draft-07 y produce exactamente ese
> mensaje inútil.

Los SDKs de **Node, Python, Go, Rust, PHP, Java y .NET** implementan **L3** de forma
opt-in. El coste de L3 se paga solo si se usa: en Rust va detrás de la feature
`validation`, en PHP de una dependencia opcional de Composer, en Python del extra
`[validation]` con importación diferida, en Java de una dependencia Maven
`<optional>true</optional>` que solo se carga si el modo no es `OFF`, y en .NET de un
paquete aparte (`Flux.Validation`), porque toda `PackageReference` de NuGet es transitiva y
ahí "opcional" no puede expresarse de otra forma. Un servicio que se conforma con L2 no
arrastra el validador en ninguno de los siete.

> **Go es la excepción, y conviene decirlo.** No existe allí la figura de la dependencia
> opcional, así que el validador va en `go.mod` y lo paga también quien se quede en L2
> (~1 MB de binario). En ejecución sigue sin costar nada: con el modo `off` no se compila
> ni un esquema. Es una limitación del ecosistema, no una decisión del protocolo.

### Capacidades opcionales

Los niveles miden cuánto del **núcleo** implementa un SDK. Una **capacidad** es otra cosa:
una extensión opcional, con su propio documento normativo, que un SDK **PUEDE** implementar
**sin cambiar de nivel**. Un SDK que no implementa una capacidad sigue siendo L1, L2 o L3
conforme; lo que no es, es compatible con aquello que la capacidad habilita.

| Capacidad | Normativa en | Un SDK sin ella… |
|---|---|---|
| `signing` | [07-signing.md](07-signing.md) | No firma ni verifica. Un evento sin firma sigue siendo válido: el modo por defecto es `off` |
| `security` | Esta sección, y lo que cita | **No es compatible con la configuración segura** de [06-security.md](06-security.md): con el inbox por defecto el broker le deniega la suscripción a su propio inbox y su primera petición falla |
| `state` | [10-state.md](10-state.md) | No escribe ni lee **estado replicado** (`put`, `delete`, `get`, `watch`): para leer un dato que guarda otro sistema tiene que llamarle por HTTP. Sigue siendo conforme |
| `rpc` | [11-request-reply.md](11-request-reply.md) | No hace ni atiende **peticiones con respuesta** (`request`, `respond`): para una orden con respuesta inmediata o un cálculo tiene que llamar por HTTP. Sigue siendo conforme |

El registro de qué SDK declara qué capacidad es [`protocol.json`](../protocol.json) →
`capabilities`, y la tabla «Estado de los SDKs» del [`README.md`](../README.md) lo muestra.
**Un SDK no anuncia una capacidad que no tiene**: si hay duda, no la declara.

#### `security`

Un SDK **declara** `security` solo si cumple **todo** lo siguiente:

1. **Inbox privado.** Fija el prefijo del inbox a `_INBOX.<servicio>` a partir del nombre de
   servicio; no es una opción ([06-security.md §3.3](06-security.md)).
2. **TLS con CA propia**, y opcionalmente mTLS. Si se exige TLS y el servidor no lo ofrece,
   falla: nunca baja a texto plano ([06-security.md §2](06-security.md)).
3. **Credenciales** `.creds` (JWT + NKey) y semilla NKey ([06-security.md §2](06-security.md)).
   Un valor mal formado falla en `connect()`, antes de tocar la red, con `INVALID_CREDENTIALS`
   (o `INVALID_TLS_OPTIONS` si es de TLS) — [04-errors.md §5.3](04-errors.md).
4. **`NOT_PROVISIONED` y `PERMISSION_DENIED`**, ambos PERMANENT y con el mensaje que exige
   [04-errors.md §5](04-errors.md). Y *info antes de crear*: un `CREATE` denegado sobre un
   recurso que existe no es un error ([06-security.md §3.4](06-security.md)).
5. **Primera conexión** que espera a un broker caído pero falla de inmediato ante lo que
   reintentar no arregla ([03-delivery.md §6.1](03-delivery.md)), con `CONNECTION_REJECTED`
   —motivo `auth` o `tls`— ([04-errors.md §5.3](04-errors.md)). Con los dos códigos de
   opciones mal formadas del punto 3, son los **tres** códigos de `connect()`.
6. **Denegaciones visibles**: una petición denegada falla en tiempo útil nombrando el subject;
   un pull denegado hace fallar `subscribe()` con `PERMISSION_DENIED` y un `ack` denegado queda
   registrado por el `logger` ([06-security.md §3.7](06-security.md)).

Se declara por **evidencia ejecutada**, no por intención: contra un broker real, con los
permisos exactos que genera [`scripts/gen-acl.mjs`](../scripts/gen-acl.mjs), y en los dos
sentidos —lo permitido funciona, y quitar un permiso **se nota**—.
[`scripts/check-sdk-codes.mjs`](../scripts/check-sdk-codes.mjs) comprueba que el código de cada
SDK contiene los códigos de error de las capacidades que declara y que existen los ficheros de
evidencia que cita. Es necesario y **no suficiente**: que un código esté escrito no prueba que
se emita.

#### `state`

Un SDK **declara** `state` solo si cumple **todo** lo de [10-state.md §10](10-state.md): las
cuatro operaciones, **los mismos bytes** que los ejemplos de [10-state.md §2.6](10-state.md), el
reconocimiento de **los dos** códigos de conflicto de secuencia, los **siete** códigos de
[04-errors.md §6](04-errors.md), el rechazo de los dominios reservados en `publish()` y un ciclo
completo contra un broker real con los permisos exactos de estado, en los dos sentidos.
`security` no es requisito, pero **sin ella el SDK no puede ejecutar** el ciclo bajo la
configuración segura.

#### `rpc`

Un SDK **declara** `rpc` solo si cumple **todo** lo de [11-request-reply.md §12](11-request-reply.md): `request` y
`respond`, **los mismos bytes** que los ejemplos de [11-request-reply.md §3.5](11-request-reply.md) para la petición, la
respuesta y los dos errores, los **seis** códigos de [04-errors.md §7](04-errors.md), una suscripción exacta por
operación con el grupo de cola del servicio, la conexión que pide `no responders`, una petición denegada que falla
rápido con `PERMISSION_DENIED` y un ciclo completo contra un broker real con los permisos exactos de la
petición/respuesta, en los dos sentidos —una operación atendida por dos instancias reparte las peticiones, y al
parar ambas quien llama recibe `RPC_UNAVAILABLE` en menos de 100 ms—. `security` no es requisito, pero **sin ella el
SDK no puede ejecutar** el ciclo bajo la configuración segura (el inbox privado es lo que impide leer las
respuestas ajenas). **Ningún SDK la declara todavía**: la evidencia llega con las fases de los SDKs.

Una capacidad nueva se añade aquí con su nombre, su documento normativo y la evidencia que la
acredita, y ningún SDK la declara hasta que pasa esa evidencia.

## 6. Fuera del alcance de v1

Declarado explícitamente para evitar scope creep:

> El **estado replicado** ([10-state.md](10-state.md)) y la **petición/respuesta**
> ([11-request-reply.md](11-request-reply.md)) no están en esta lista: entran en 1.1 como extensiones opcionales.
> Lo que sigue fuera de la petición/respuesta —flujos de varias respuestas, difusión, contrapresión— está declarado
> en [11-request-reply.md §13](11-request-reply.md).

- Sagas y orquestación de procesos de larga duración.
- Protobuf y Avro (opt-in por subject en v2).
- Cifrado a nivel de campo.
- Federación multi-cluster.

## 7. Índice normativo

| Documento | Contenido |
|---|---|
| [01-envelope.md](01-envelope.md) | Atributos CloudEvents, extensiones, payload |
| [02-naming.md](02-naming.md) | Subjects, types, streams, durable consumers |
| [03-delivery.md](03-delivery.md) | JetStream, acks, retries, idempotencia, orden |
| [04-errors.md](04-errors.md) | Taxonomía de errores, DLQ, replay |
| [05-compatibility.md](05-compatibility.md) | Evolución de esquemas, deprecación |
| [06-security.md](06-security.md) | Accounts, ACLs, clasificación de datos |
| [07-signing.md](07-signing.md) | Firma Ed25519 — extensión **opcional** |
| [08-observability.md](08-observability.md) | Métricas, trazas y alertas — normativo para L2 |
| [09-multitenancy.md](09-multitenancy.md) | Modelos de aislamiento entre tenants |
| [10-state.md](10-state.md) | Estado replicado — extensión **opcional**: `put`, `delete`, `get`, `watch` |
| [11-request-reply.md](11-request-reply.md) | Petición/respuesta — extensión **opcional**: `request`, `respond`, core NATS, sin stream |
