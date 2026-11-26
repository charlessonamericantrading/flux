# 04 — Errores y DLQ

El error más caro de un sistema de eventos no es perder un mensaje. Es **reintentar
durante 51 minutos algo que nunca va a funcionar**, mientras el consumidor se atasca
y los eventos sanos se acumulan detrás.

Por eso flux no tiene "una política de reintentos". Tiene una **taxonomía**.

---

## 1. Las tres clases

| Clase | Significado | Acción NATS | ¿Reintenta? |
|---|---|---|---|
| **RETRYABLE** | El fallo es del entorno y podría desaparecer solo. | `nak(delay)` | Sí, con backoff |
| **PERMANENT** | El evento es válido pero este consumidor nunca podrá procesarlo. | `term()` + DLQ | **No** |
| **POISON** | El mensaje ni siquiera es interpretable. | `term()` + DLQ + alerta | **No** |

La distinción entre RETRYABLE y PERMANENT es **la decisión de diseño más importante
de un consumidor**. Equivocarla en un sentido atasca la cola; equivocarla en el otro
tira eventos buenos a la basura por un hipo de red.

### 1.1 RETRYABLE

El fallo no dice nada sobre el evento — dice algo sobre el mundo en este instante.

La clasificación se define por **semántica**, no por una lista de códigos:

| Categoría | Qué significa |
|---|---|
| Red no disponible o interrumpida | Conexión rechazada, reseteada, ruta inalcanzable, tubería rota |
| Resolución de nombres temporal | El resolutor dice explícitamente "reinténtalo", no "no existe". **Si la plataforma no distingue** (Java y .NET usan `UnknownHostException` tanto para NXDOMAIN como para SERVFAIL), trátalo como RETRYABLE: un dominio que de verdad no existe agotará su presupuesto y acabará en la DLQ igualmente |
| Rechazo por carga | HTTP 429, 502, 503, 504 |
| Contención en base de datos | Deadlock, lock timeout, pool agotado |
| Dependencia arrancando o desplegándose | Aún no acepta tráfico |

> **Los códigos concretos son ejemplos, no norma.** `ECONNRESET`, `ETIMEDOUT` y
> `EAI_AGAIN` son nombres de libuv: existen en Node, aparecen con prefijo `WSA` en
> Windows (`WSAECONNRESET`), y en Go `EAI_AGAIN` **no existe como errno** — el mismo
> fallo se expresa como `*net.DNSError` con `IsTemporary`.
>
> Un SDK **DEBE** usar el mecanismo idiomático de su plataforma para reconocer estas
> categorías. **NO DEBE** hacer `strings.Contains` sobre mensajes de error, que es
> justo lo que invita a hacer una lista de códigos tratada como normativa.
>
> Un port literal de la lista de Node produjo un bug real: en Windows el mismo corte
> de red se clasificaba PERMANENT y en Linux RETRYABLE.

→ `nak` con el backoff de [03-delivery.md §2](03-delivery.md). Tras agotar
`max_deliver`, JetStream deja de entregar y el SDK enruta a la DLQ.

### 1.2 PERMANENT

El evento es sintácticamente correcto, pero este consumidor no puede actuar sobre él
**por mucho que espere**.

- Falla la validación contra `dataschema`
- Regla de negocio que rechaza el hecho (`el pedido ya estaba cancelado`)
- Referencia a una entidad que no existe y no va a existir
- HTTP 400, 403, 404, 422 de una dependencia
- Versión de contrato no soportada por este consumidor
- El broker deniega la operación por permisos, o falta un stream o un consumidor que
  nadie ha aprovisionado ([§5](#5-errores-de-la-capacidad-security))

→ `term()` **inmediato** y publicación en la DLQ. Reintentar es puro desperdicio: 51
minutos de cola bloqueada para llegar al mismo sitio.

### 1.3 POISON

El SDK no logra siquiera construir un evento a partir del mensaje.

- JSON malformado
- Falta un atributo obligatorio de CloudEvents
- `specversion` desconocida
- `datacontenttype` no soportado
- Una entrada de estado cuyo sobre **miente sobre su identidad**: su `tenantid`, su `subject` o su
  `type` no coinciden con el subject de NATS en el que llegó ([§6](#6-errores-de-la-capacidad-state))

→ `term()` + DLQ + **alerta inmediata**. Un POISON casi siempre significa que un
productor está roto o que alguien publicó a mano en el subject equivocado. Es el único
caso que **DEBE** despertar a alguien.

## 2. Cómo se clasifica un error

Un SDK L2 **DEBE**:

1. Clasificar POISON él mismo, antes de invocar al handler.
2. Ofrecer un default razonable para errores del handler.
3. **Permitir que la aplicación anule la clasificación**, porque solo ella conoce sus
   dependencias.

### 2.1 El default para lo desconocido: RETRYABLE acotado

El default de flux es **RETRYABLE con presupuesto reducido: 2 entregas, no 6.**

Las dos opciones obvias fallan cada una en un extremo:

| Política | Transitorio desconocido | Sistemático desconocido |
|---|---|---|
| PERMANENT | ❌ A la DLQ por un hipo de red. Alguien reproduce a mano cada mañana. | ✅ Falla rápido |
| RETRYABLE completo | ✅ Se recupera solo | ❌ 51 min de cola atascada, y el modo de fallo **se amplifica** con cada mensaje siguiente |
| **RETRYABLE acotado (2)** | ✅ Se recupera en el 2º intento | ✅ A la DLQ en ~30 s |

El presupuesto acotado **domina a ambas**: cuesta 30 segundos de latencia sobre los
errores genuinamente permanentes, y a cambio elimina los dos modos de fallo. No es un
punto medio, es estrictamente mejor.

Implementación: el presupuesto **NO** se configura en el consumidor. `max_deliver` es
por consumidor, no por mensaje, y bajarlo a 2 recortaría también los reintentos de los
errores que sí sabemos que son transitorios. El SDK lleva la cuenta con `attempt` y
enruta a la DLQ cuando un error de clase desconocida supera su presupuesto, dejando
intactos los 6 intentos de los RETRYABLE reconocidos.

```
Error reconocido como transitorio (ECONNRESET, 503) → 6 entregas, hasta 51 min
Error desconocido                                   → 2 entregas, ~30 s
Error reconocido como permanente (400, 422)         → 1 entrega, sin espera
```

Un SDK L2 **DEBE** exponer esta política como configurable
(`permanent` / `retryable` / `retryable-bounded` + presupuesto), porque el equilibrio
correcto depende de cómo fallen las dependencias de cada ecosistema. El default es
`retryable-bounded` con presupuesto 2.

Un SDK L2 **DEBE** exponer errores tipados para que la aplicación señale la clase:

```
throw new RetryableError("proveedor de pagos 503", { retryAfterMs: 5000 })
throw new PermanentError("pedido ya cancelado", { code: "PEDIDO_YA_CANCELADO" })
```

> 🔨 **La función que traduce un error cualquiera a una de estas tres clases se
> implementa en `sdk-node/src/classify.ts`.** Es una decisión de política, no de
> infraestructura — ver la nota al final de ese fichero.

## 3. Formato del mensaje en DLQ

El mensaje de DLQ es **el CloudEvent original, íntegro**, con extensiones añadidas.
No se envuelve en otro sobre.

```json
{
  "specversion": "1.0",
  "id": "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  "source": "/produccion/pedidos-api",
  "type": "com.flux.pedidos.pedido.creado.v1",
  "time": "2026-08-20T10:25:39.412Z",
  "dataschema": "https://schemas.internal/pedidos/pedido/creado/1.2.0.json",
  "correlationid": "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  "tenantid": "acme",
  "producerversion": "3.4.1",
  "dataclassification": "confidential",
  "data": { "pedidoId": "ped-123", "totalCents": 9990 },

  "dlqreason": "permanent",
  "dlqattempts": 1,
  "dlqconsumer": "facturacion-api__pedidos_pedido_v1_creado",
  "dlqerror": "PEDIDO_YA_CANCELADO: el pedido ped-123 estaba cancelado",
  "dlqtime": "2026-08-20T10:25:40.117Z"
}
```

**Por qué el original íntegro y no un wrapper:** para reproducir un mensaje de la DLQ
solo hay que borrar las extensiones `dlq*` y republicar en el subject original. Con
un wrapper habría que desenvolver, y cada consumidor de la DLQ tendría que conocer dos
formatos. El replay se convierte en `jq 'del(.dlq*)'`.

`dlqattempts` es el número de entrega en que el evento murió, **no** una propiedad de
su clase. Un PERMANENT suele registrar `1` porque no gasta reintentos, pero si el
handler falló dos veces con errores transitorios y a la tercera lanzó un PERMANENT,
registra `3` — y eso es información útil, no una incoherencia. Un RETRYABLE agotado
registra siempre `max_deliver` (`6`).

Subject de destino: `dlq.<subject original>` — ver
[02-naming.md §3.1](02-naming.md).

```
pedidos.pedido.v1.creado  →  dlq.pedidos.pedido.v1.creado
```

## 4. La DLQ no es un cementerio

Una DLQ que nadie mira es una pérdida de datos con pasos extra. El protocolo exige:

- **DEBE** existir una alerta sobre `dlq.>` con umbral distinto por razón:
  `poison` → inmediata; `permanent` → agregada; `retryable` → por tasa.
- **DEBERÍA** revisarse la DLQ en cada ciclo operativo, no cuando algo se rompe.
- `max_age: 90d` en el stream de DLQ da margen forense, pero **es un límite real**: a
  los 90 días el evento desaparece.

### 4.1 Replay

```bash
# Inspeccionar sin consumir
nats stream view DLQ_PEDIDOS --subject 'dlq.pedidos.pedido.v1.creado'

# Reproducir: quitar extensiones dlq* y republicar en el subject original
nats stream get DLQ_PEDIDOS --last-for 'dlq.pedidos.pedido.v1.creado' --json \
  | jq '.data | @base64d | fromjson | del(.dlqreason, .dlqattempts, .dlqconsumer, .dlqerror, .dlqtime)' \
  | nats pub 'pedidos.pedido.v1.creado' --force-stdin
```

**Antes de reproducir, dos comprobaciones obligatorias:**

1. **¿Se ha arreglado la causa?** Reproducir contra el mismo bug devuelve el evento a
   la DLQ y ensucia el rastro.
2. **¿Sigue siendo idempotente el consumidor para este `id`?** El replay conserva el
   `id` original, así que si el consumidor llegó a aplicar un efecto parcial antes de
   fallar, la tabla de eventos procesados **DEBE** ser lo que impida duplicarlo. Si el
   handler no es idempotente, el replay es más peligroso que el fallo original.

> El replay **DEBE** conservar el `id` original. Regenerarlo rompe la idempotencia de
> todos los consumidores aguas abajo y convierte una recuperación en un incidente
> nuevo.

> **El estado no se reproduce.** Un evento de `dlq.state.>` no lleva el dato (su `data` es
> `{ key, revision }`) y, aunque lo llevara, republicar un valor viejo pisaría uno nuevo.
> `flux dlq replay` **DEBE** rechazar `dlq.state.>`; la recuperación es `flux state resync`
> ([10-state.md §6](10-state.md)).

## 5. Errores de la capacidad `security`

Un SDK que declara la capacidad `security` ([00-protocol.md §5](00-protocol.md)) **DEBE**
emitir estos **cinco** códigos estables: dos para lo que se deniega o falta con el servicio ya
en marcha —la tabla de abajo— y tres para `connect()` ([§5.3](#53-errores-de-connect-la-conexión-inicial)).
Los dos primeros existen por algo medido: **cuando una ACL deniega una operación,
el broker no contesta.** Descarta el mensaje y avisa aparte, por la conexión, con un
`-ERR 'Permissions Violation …'` ([A-28](../docs/plan-v1.1/PLAN.md#apéndice-a--evidencia),
medido contra nats-server 2.14.5). Lo que ve la aplicación depende de la librería de NATS:
un *timeout* de 5 a 30 s si no entrega la denegación (`async-nats`, A-28), o un error de
petición genérico si sí lo hace (`@nats-io/*`, A-21). Ninguno de los dos dice si lo que falta
es un permiso o un recurso, ni cómo se arregla. Con estos códigos es un error en
milisegundos que lo dice.

| Código | Clase | Se emite cuando |
|---|---|---|
| `NOT_PROVISIONED` | **PERMANENT** | El SDK necesita un stream o un consumidor durable que **no existe**, y el broker le **deniega crearlo**. |
| `PERMISSION_DENIED` | **PERMANENT** | El broker deniega una operación por permisos y no se da lo anterior: ni siquiera se puede *consultar* el recurso, se publica en un subject que la ACL no concede, o se deniega una suscripción. |

**La frontera entre los dos es si se sabe que el recurso falta.** `NOT_PROVISIONED` significa
«falta y no puedo crearlo», y su señal medida es una **pareja**: crear → denegado **y**
consultar → no existe ([06-security.md §3.6](06-security.md)). Un SDK **NO DEBE** emitirlo por
una denegación sola: si el recurso existe, un `CREATE` denegado no es un error y el SDK sigue
con la consulta ([06-security.md §3.4](06-security.md)); decir «no está aprovisionado» de algo
que sí lo está mandaría a quien opera a buscar donde no es. Y cuando ni siquiera la consulta
está permitida —una ACL de la v1, que no concede nada de `$JS.API.*`— no se sabe si falta:
eso es `PERMISSION_DENIED`.

### 5.1 Son PERMANENT

Como un HTTP 403 ([§1.2](#12-permanent)): reintentar una denegación da la misma denegación, y
esperar no hace aparecer un recurso ni concede un permiso. Un handler que los provoque —por
ejemplo, publicando en un dominio sin aprovisionar— va a la DLQ en la **primera** entrega.
Sin una clase propia caerían en la política de lo desconocido ([§2.1](#21-el-default-para-lo-desconocido-retryable-acotado)):
dos entregas y ~30 s de cola ocupada para acabar en el mismo sitio.

Los tres de `connect()` ([§5.3](#53-errores-de-connect-la-conexión-inicial)) lo son por lo
mismo, aunque no nazcan en un handler y la clase no decida ningún `ack`: dice que reintentar
—el SDK, o un bucle de la aplicación alrededor de `connect()`— da el mismo fallo, y que quien
lo ve tiene que **cambiar algo** (la configuración del servicio o la del broker).

### 5.2 Qué DEBE decir el mensaje

Un error accionable es el que no obliga a abrir el log del broker para saber qué hacer.

| | `NOT_PROVISIONED` | `PERMISSION_DENIED` |
|---|---|---|
| **DEBE nombrar** | El **recurso** que falta: el stream, o el consumidor **y** su stream. Y el **subject denegado** (el de la creación), tal como lo comunicó el broker | Lo que el SDK intentaba (publicar o suscribirse y, en términos de flux, a qué: «abrir el stream `EVT_PEDIDOS`») y el **subject denegado**, completo y sin normalizar |
| **DEBE apuntar a** | **`flux provision`**, que se ejecuta con credenciales de **operación**, no con las del servicio ([06-security.md §3.6](06-security.md)) | La ACL del servicio, que es donde se arregla: [06-security.md §3](06-security.md) |

Además, los dos de esta tabla (los de `connect()` tienen la suya en [§5.3](#53-errores-de-connect-la-conexión-inicial)):

- Llevan un **código fijo que la aplicación no puede sustituir**: es lo que agrupa el panel
  (`code` de [08-observability.md §2.2](08-observability.md)), y dejar que cada servicio lo
  cambiara rompería esa agrupación.
- **NO DEBEN** incluir credenciales ni el contenido de `data`: el mensaje acaba en logs y en
  `dlqerror`.
- Encabezan `dlqerror` con el formato de siempre, `<código>: <mensaje>` ([§3](#3-formato-del-mensaje-en-dlq)).

**No se define ninguna métrica nueva.** Un handler cuyo error es uno de estos dos acaba en la
DLQ y queda contado por la que ya existe: `flux_events_dlq_total{reason="permanent",
code="NOT_PROVISIONED"}`, porque `code` es «el código estable de la clasificación»
([08-observability.md §2.1](08-observability.md)). Las denegaciones que no son un error
—`ack` y pull, [06-security.md §3.7](06-security.md)— no tienen contador: solo el log.

### 5.3 Errores de `connect()`: la conexión inicial

Un `connect()` que falla porque reintentar no lo arregla ([03-delivery.md §6.1](03-delivery.md))
**DEBE** lanzar un error que se distinga de «el broker aún no está» —es decir, que **no** se
espere—, que diga la **causa**, que diga **qué revisar** y que lleve uno de estos **tres**
códigos estables, todos **PERMANENT** ([§5.1](#51-son-permanent)):

| Código | Se emite cuando | Se arregla en |
|---|---|---|
| `CONNECTION_REJECTED` | El broker **contestó** y rechazó la **primera** conexión por algo que esperar no arregla. Lleva un motivo, `reason`, con dos valores: **`auth`** —el broker rechazó las credenciales— o **`tls`** —el certificado del servidor no verifica (CA ausente o equivocada, host fuera de los SAN, caducado), o se exigió TLS y el servidor no lo ofrece, o el broker exige certificado de cliente y no se presentó ninguno (esto último **DEBERÍA**: no todos los clientes NATS lo distinguen, [03-delivery.md §6.1](03-delivery.md))—. | El broker (el usuario, su certificado) o lo que el servicio le pasa (la CA, el host): la opción está **bien formada** y el broker no la acepta |
| `INVALID_CREDENTIALS` | Las opciones de credenciales están **mal formadas** y eso se sabe **sin hablar con el broker**: un valor vacío o que no es un texto (casi siempre una variable de entorno sin definir); un nombre de opción desconocido (uno equivocado, `password` por `pass`, no puede degradar a «sin credenciales»); `user` sin `pass`, o al revés; más de un método a la vez (es ambiguo: el servidor aceptaría uno y el otro sería ruido); un fichero `.creds` ilegible o sin JWT y semilla; una semilla que no es de usuario (`SU…`). Se emite **antes de abrir ningún socket**. | Lo que se pasó a `connect()` |
| `INVALID_TLS_OPTIONS` | Las opciones TLS están **mal formadas** y se sabe **sin red**: la opción no es del tipo esperado (un valor «ausente» que la librería de NATS leería como «sin TLS» es justo lo contrario de quien lo pasa); un nombre de opción desconocido; una ruta vacía, ilegible o —en los SDKs que lo comprueban— que no es un PEM; un certificado de cliente sin su clave, o al revés. Se emite **antes de abrir ningún socket**. | Lo que se pasó a `connect()` |

**Qué los distingue es si hubo red.** `INVALID_CREDENTIALS` e `INVALID_TLS_OPTIONS` se detectan
**sin hablar con el broker**: éste ni se entera, y el remedio está en la configuración local.
`CONNECTION_REJECTED` supone que hubo conversación y que el broker **contestó**: la opción
estaba bien formada y no es lo que él espera. De ahí tres consecuencias, que cada SDK **DEBE**
cumplir y que se comprueban **en los dos sentidos**:

- Una credencial **bien formada pero equivocada** es `CONNECTION_REJECTED` (`auth`), **nunca**
  `INVALID_CREDENTIALS`. Un `.creds` ilegible es `INVALID_CREDENTIALS`, **nunca**
  `CONNECTION_REJECTED`.
- Un certificado que el broker no acepta es `CONNECTION_REJECTED` (`tls`), **nunca**
  `INVALID_TLS_OPTIONS`. Una CA que no existe es `INVALID_TLS_OPTIONS`.
- Un broker que **aún no ha arrancado** (conexión rechazada, DNS, timeout) **no es ninguno de
  los tres**: se espera. Tampoco lo son el resto de fallos de `connect()` —un nombre de
  servicio inválido, una lista de servidores que no se entiende—, que **NO DEBEN** llevar
  ninguno de estos códigos: no son estas situaciones.

Por qué tres y no uno con un motivo: el remedio y **quién** lo aplica son distintos. Los dos
`INVALID_*` los arregla quien despliega el servicio, mirando lo que pasó a `connect()`;
`CONNECTION_REJECTED` lo arregla, normalmente, quien opera el broker o los certificados (una
CA rotada, un usuario que falta en la account). Un panel o una alerta agrupa por `code`
([08-observability.md §2.2](08-observability.md)): mezclar los tres mandaría la alerta al
equipo equivocado. Y el motivo de `CONNECTION_REJECTED` tiene **dos** valores y no más
porque más no los da la librería de NATS: que el TLS falle por un certificado que no verifica,
por un servidor sin TLS o por un mTLS sin certificado de cliente llega, según la librería,
como el mismo error (A-24, A-30); esos matices viajan en el **mensaje**, no en el motivo.

**Qué DEBE decir el mensaje**

| | `CONNECTION_REJECTED` | `INVALID_CREDENTIALS` | `INVALID_TLS_OPTIONS` |
|---|---|---|---|
| **DEBE nombrar** | A qué broker se intentaba conectar y la **causa** tal como la da la librería cuando la da (`authorization violation`, `certificate not valid for name`, `UnknownIssuer`). Si el SDK solo sabe que falló el handshake TLS, **DEBE** listar las causas candidatas (el certificado no verifica, el broker no ofrece TLS, exige mTLS) | **Qué** opción está mal y por qué (`la ruta del .creds no se puede leer`). Una **ruta** sí; **nunca el valor** de una credencial | **Qué** opción está mal y por qué. Una ruta sí |
| **DEBE apuntar a** | Con `auth`: las credenciales y que el usuario exista en la account. Con `tls`: la CA, los SAN del certificado, si el broker ofrece TLS y si exige mTLS ([06-security.md §2](06-security.md)) | Lo que se pasa a `connect()` ([06-security.md §2](06-security.md)) | Lo que se pasa a `connect()` ([06-security.md §2](06-security.md)) |

Y los tres: un **código fijo que la aplicación no puede sustituir**, y **ninguna credencial**
en el mensaje —acaba en logs—. **DEBE** decir también que reintentar no lo arregla, para que
nadie envuelva `connect()` en un bucle.

**Cada SDK lo expresa con su mecanismo.** Lo normativo es el `code` estable y, en el rechazo,
el motivo `auth | tls`. Node lanza `ConnectionRejectedError` (`code`, `reason`),
`InvalidCredentialsError` e `InvalidTlsOptionsError`; Rust devuelve
`FluxError::ConnectionRejected { reason, .. }`, `FluxError::InvalidCredentials` y
`FluxError::InvalidTlsOptions`, con `code()` y `class()`. Una aplicación **PUEDE** ramificar por
estos códigos: son estables.

> **Alcance.** Esto cubre la **primera** conexión. Qué debe hacer un SDK si el broker rechaza
> las credenciales durante una *re*conexión (una credencial que caduca o se rota) **no está
> medido**, y esta spec no lo fija: ninguno de estos códigos se define para ese caso.

## 6. Errores de la capacidad `state`

Un SDK que declara la capacidad `state` ([00-protocol.md §5](00-protocol.md),
[10-state.md](10-state.md)) **DEBE** emitir estos **siete** códigos estables. La lista es
**cerrada**: un caso que no encaje en ninguno es un hueco de la spec, no un código nuevo que cada
SDK inventa. (Nació con seis; el séptimo, `INVALID_STATE_CONTRACT`, es justo el hueco que dejó
abierto: un contrato mal formado —`clientes.cliente`, sin versión— no cabía en ninguno, y Node
lanzaba un error sin código mientras Rust lanzaba `InvalidSubject`, que no es de estado.)

| Código | Clase | Se lanza en | Se emite cuando |
|---|---|---|---|
| `INVALID_TENANT_FOR_STATE` | **PERMANENT** | `put`, `get`, `delete`, `watch` | El tenant no cumple `^[a-z0-9]+(-[a-z0-9]+)*$` con 64 caracteres como máximo, o no se puede resolver ninguno: el estado **no** cae en `system` por defecto ([10-state.md §2.4](10-state.md)) |
| `INVALID_STATE_CONTRACT` | **PERMANENT** | `put`, `get`, `delete`, `watch` | El contrato no es `<dominio>.<agregado>.v<major>` —el subject sin `state.`, sin tenant y sin clave—: no son tres tokens, un token no es `kebab-case` en minúsculas o la versión no es `v` y un entero ≥ 1 (`clientes.cliente`, `clientes.cliente.v1.creado`, `Clientes.cliente.v1`). Un dominio reservado **no** es este código: es `RESERVED_DOMAIN`, que se evalúa antes ([10-state.md §2.2](10-state.md)) |
| `INVALID_STATE_KEY` | **PERMANENT** | `put`, `get`, `delete` | La clave no cumple `^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`: un punto, un comodín, un espacio, vacía, de más de 128 caracteres, o que empieza por `_` ([10-state.md §2.3](10-state.md)) |
| `MISSING_AGGREGATE_VERSION` | **PERMANENT** | `put`, `delete` (y al consumir) | `data` no tiene `aggregateVersion`, o no es un entero ≥ 1 y ≤ 2^53 − 1 ([10-state.md §3.4](10-state.md)) |
| `STATE_SUBJECT_MISMATCH` | **POISON** | Al consumir: `watch` y `get` | El `tenantid`, el `subject` o el `type` del sobre no coinciden con el subject de NATS en el que llegó la entrada ([10-state.md §5.4](10-state.md)) |
| `STATE_CONFLICT` | **RETRYABLE** | `put`, `delete` | El chequeo de secuencia agotó sus 3 intentos: alguien escribió la misma clave entre la lectura y la publicación, las tres veces ([10-state.md §3.2](10-state.md)) |
| `RESERVED_DOMAIN` | **PERMANENT** | `publish`, `subscribe`, `put`, `get`, `delete`, `watch` | Un subject o un contrato usa un dominio reservado: `dlq`, `state`, `rpc` o `system` ([10-state.md §2.7](10-state.md)) |

**Por qué estas clases.**

- Los cinco que se lanzan al **llamar** (`INVALID_STATE_CONTRACT`, `INVALID_TENANT_FOR_STATE`,
  `INVALID_STATE_KEY`, `MISSING_AGGREGATE_VERSION`, `RESERVED_DOMAIN`) son **PERMANENT**: la llamada está mal y
  repetirla da el mismo error ([§5.1](#51-son-permanent)). Un `put` dentro de un handler que los
  provoque va a la DLQ en la **primera** entrega.
- **`STATE_SUBJECT_MISMATCH` es POISON**, no PERMANENT: el mensaje es legible, pero **miente sobre
  su identidad** —su tenant o su clave—, y un productor que lo hace está roto o es hostil. Es
  lo que [§1.3](#13-poison) pide despertar a alguien por: **alerta inmediata**.
- **`STATE_CONFLICT` es RETRYABLE**: el conflicto es del entorno —otra instancia escribe la misma
  clave—, no de la llamada. Un reintento relee y suele resolverlo, o termina en `stale`.
- `MISSING_AGGREGATE_VERSION` también se aplica **al consumir**: una entrada sin versión
  válida no se puede aplicar de forma condicional. Es PERMANENT y va a la DLQ de estado.

**Qué DEBE decir el mensaje.** Lo mismo que [§5.2](#52-qué-debe-decir-el-mensaje): el código es
fijo y la aplicación no puede sustituirlo; el mensaje nombra **qué** está mal y **cómo se
arregla** (la clave que no encaja, con el patrón; el tenant que falta y que `connect()` o la
opción `tenantId` lo dan); y **NO DEBE** incluir valores de `data`, por la misma razón que en
[§5.2](#52-qué-debe-decir-el-mensaje): acaba en logs y en `dlqerror`, y en estado `data` es dato
personal. Una **clave y un tenant sí pueden citarse**: son identificadores opacos, no PII.

**Lo que no es un código nuevo.** Estos ya existen y se reutilizan tal cual:

| Situación | Código |
|---|---|
| `classification: "restricted"` (o una fuera del enum) en un `put`; un mensaje de estado que la lleve | `INVALID_DATACLASSIFICATION` ([01-envelope.md §3.1](01-envelope.md)) |
| `data` no cumple el esquema del contrato, o el de la lápida | `INVALID_SCHEMA` |
| En modo estricto, el esquema del contrato (o el de la lápida) no está en el bundle | `SCHEMA_NOT_FOUND` (lo emiten los SDKs con L3; [00-protocol.md §5](00-protocol.md) todavía no lo nombra) |
| El stream de estado (o el consumidor) no existe o no responde, y el servicio no puede crearlo | `NOT_PROVISIONED` — [§5](#5-errores-de-la-capacidad-security) |
| El broker deniega la operación por permisos | `PERMISSION_DENIED` — [§5](#5-errores-de-la-capacidad-security) |

**`NOT_PROVISIONED` en estado, un matiz.** [§5](#5-errores-de-la-capacidad-security) lo define
por una **pareja** de señales: crear → denegado e info → no existe. Un SDK de estado no consulta
ni crea el stream de estado ([10-state.md §8](10-state.md)), así que su señal es otra, medida
(A-32, A-48): `CONSUMER.INFO` contesta «stream not found» (10059), o `DIRECT.GET` y la publicación
**no contestan nadie** (`no responders`). Un stream sin `allow_direct` se ve exactamente igual que
uno que no existe, y el mensaje **DEBE** nombrar las dos causas, y apuntar a `flux provision`
(lo crea) y a `flux doctor` (lo audita). Se mantiene la frontera de siempre con
`PERMISSION_DENIED`: si el broker **deniega** la operación, es un permiso, no un recurso.

**Métricas.** Un handler cuyo error es uno de estos acaba contado por las que ya existen
(`flux_events_dlq_total{code=…}`, [08-observability.md §2.1](08-observability.md)); un
`STATE_SUBJECT_MISMATCH` cuenta además como `outcome="poison"`. De los errores de la llamada de
`put` y `delete`, `flux_state_writes_total` solo cuenta `MISSING_AGGREGATE_VERSION`,
`INVALID_SCHEMA` y `SCHEMA_NOT_FOUND`, los tres como `invalid_schema`
([08-observability.md §2.4](08-observability.md)); el resto son errores de quien llama y quedan en
su excepción. `INVALID_STATE_CONTRACT` tampoco puede contarse por una razón añadida: la etiqueta
`contract` es el contrato, y uno mal formado es una cadena arbitraria que crearía una serie por
cada error de tipeo.
