# 06 — Seguridad

Un bus de eventos es, por diseño, el lugar donde converge todo el dato del ecosistema.
Eso lo convierte en el objetivo de mayor valor de la infraestructura. La postura por
defecto de flux es **denegar salvo concesión explícita**.

---

## 1. Aislamiento por NATS accounts

Las *accounts* de NATS son un límite de aislamiento duro: los subjects de una account
son invisibles desde otra salvo export/import explícito.

```
Account SYS          operación del cluster. Ningún servicio de negocio entra aquí.
Account PROD         eventos de producción
Account STAGING      eventos de staging
Account DEV          eventos de desarrollo
```

- Un entorno **NO DEBE** compartir account con otro. Sin esta separación, un bug en
  staging puede publicar en un subject de producción, y ningún ACL a nivel de subject
  lo impide de forma fiable.
- Los servicios **NO DEBEN** tener credenciales de la account `SYS`.

## 2. Identidad de servicio

- Cada servicio **DEBE** tener sus propias credenciales (JWT + NKey). **Ninguna
  credencial compartida entre servicios, jamás.** Una credencial compartida hace
  imposible responder "¿quién publicó esto?" e imposible rotar sin coordinación global.
- Las credenciales **DEBEN** inyectarse desde un gestor de secretos en tiempo de
  ejecución. **NO DEBEN** aparecer en imágenes, repos, variables de entorno de CI ni
  ficheros de config versionados.
- Rotación **DEBERÍA** ser ≤ 90 días, automatizada.
- TLS **DEBE** ser obligatorio en el listener. Sin `allow_non_tls`.
- Un SDK **DEBE** poder cumplir lo anterior, y eso se comprueba en su API, no en su
  README:
  - **Credenciales**: aceptar un fichero `.creds` (JWT + NKey) y una semilla NKey. Un SDK
    que solo acepta usuario y contraseña no puede usar el modelo de identidad de arriba, y
    uno que acepta `.creds` pero lo pasa a una opción que el cliente ignora conecta **sin
    credenciales** sin que nada lo diga (así estaba el SDK de Node y la CLI: A-8e).
  - **TLS**: aceptar una **CA propia** (`caFile`) y, opcionalmente, certificado y clave de
    cliente (mTLS). Un bus interno casi nunca usa una CA pública; un SDK que no puede
    fijar la suya obliga a desactivar la verificación, que es la forma más corriente de
    acabar con TLS «obligatorio» y sin protección.
  - **Inbox privado**: fijar el prefijo del inbox a `_INBOX.<servicio>` a partir del nombre
    de servicio de `connect()` — [§3.3](#33-el-inbox-es-privado). No es una opción.

  Mientras un SDK no lo cumpla, **no es compatible con la configuración segura** y su
  README **DEBE** decirlo: con el inbox por defecto (`_INBOX.<aleatorio>`) el broker le
  deniega la suscripción a su propio inbox y **su primera petición falla**
  (`Permissions Violation for Subscription to "_INBOX.….*"`, en milisegundos).

## 3. ACLs a nivel de subject

El principio es simple y se aplica sin excepciones:

> **Un servicio solo publica en el dominio del que es dueño. Solo se suscribe a lo
> que se le ha concedido explícitamente.**

«Se suscribe» ya **no** significa un permiso de suscripción sobre el subject del evento:
significa los permisos exactos de un consumidor durable ([§3.4](#34-permisos-exactos-por-consumidor-durable)).
Por qué cambia el significado, en [§3.2](#32-los-permisos-de-subject-no-gobiernan-jetstream).

```jsonc
// facturacion-api — dueño de facturacion; consume pedidos.pedido.v1.creado
{
  "pub": { "allow": [
    "$JS.API.INFO",                                       // connect()
    "facturacion.>",                                      // sus eventos
    "$JS.API.STREAM.INFO.EVT_FACTURACION",                // publish() comprueba su stream
    "$JS.API.STREAM.INFO.EVT_PEDIDOS",                    // subscribe() comprueba el stream…
    "$JS.API.STREAM.INFO.DLQ_PEDIDOS",                    // …y su DLQ
    // los tres permisos de SU durable, y de ningún otro:
    "$JS.API.CONSUMER.INFO.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado",
    "$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado",
    "$JS.ACK.EVT_PEDIDOS.facturacion-api__pedidos_pedido_v1_creado.>",
    "dlq.pedidos.pedido.v1.creado"                        // su DLQ de lo que consume (§3.1)
    // + los tres permisos de su durable de DLQ propia (§3.1)
  ] },
  "sub": { "allow": ["_INBOX.facturacion-api.>"] }        // y NADA más
}
// (con `--dev` se añade además CONSUMER.CREATE de cada durable propio: §3.4)
```

Lo que **no** se debe generar —y es exactamente lo que escribía la versión anterior de
este documento— es esto:

```jsonc
// ❌ facturacion-api, ACL v1
{
  "pub": { "allow": ["facturacion.>", "dlq.pedidos.pedido.v1.>"] },   // sin $JS.API: no consume nada
  "sub": { "allow": ["pedidos.pedido.v1.>", "_INBOX.>"] }              // _INBOX.> común: lee todo
}
```

Con ella, un servicio **no podía consumir** ni consultar su propio stream (A-1), y aun así
**leía un evento de un dominio que no tenía concedido** (A-2). El modelo de permisos nunca
se había ejecutado contra un broker; tal como estaba escrito, o no funcionaba o —si
alguien lo «arreglaba» concediendo `$JS.API.>`— no protegía ([§3.5](#35-lo-que-nunca-se-concede)).

Esto convierte el **ownership del dato en una regla aplicada por el broker**. Un
servicio no puede falsificar un evento de otro dominio aunque su código lo intente:
la publicación es rechazada en el servidor. Es el mismo principio del mayor en el
subject — el enrutado aplica el contrato, la documentación solo lo describe.

> Cada observación citada como `A-n` está medida contra **nats-server 2.14.5** y descrita
> en el [Apéndice A del plan v1.1](../docs/plan-v1.1/PLAN.md#apéndice-a--evidencia).
> [`scripts/verify-acl.mjs`](../scripts/verify-acl.mjs) las reproduce contra un servidor
> real con las ACLs que genera `gen-acl.mjs`, y termina con exit 0 solo si todas se cumplen.

### 3.0 Las ACLs se generan, no se escriben

Escritas a mano divergen del diseño en el primer mes: alguien añade un consumidor y
concede un dominio entero "temporalmente". Pero la regla de arriba es **derivable**:
si sabes quién es dueño de cada dominio y qué consume cada servicio, las ACLs se
calculan. Y con pull consumers son además **largas y exactas** —decenas de permisos por
servicio, un grupo por cada subject consumido y ni un comodín donde no toca—, que es
justo lo que un humano no mantiene.

El manifiesto de ownership ([`examples/services.json`](../examples/services.json)) es
la fuente, y [`scripts/gen-acl.mjs`](../scripts/gen-acl.mjs) deriva la configuración.

```bash
node scripts/gen-acl.mjs examples/services.json                       # genera (bloque `accounts`, incluible con `include`)
node scripts/gen-acl.mjs examples/services.json --nkeys claves.json   # con la clave PÚBLICA de cada servicio
node scripts/gen-acl.mjs examples/services.json --check               # valida (CI)
node scripts/gen-acl.mjs examples/services.json --json                # los permisos exactos, para asertarlos en tests
node scripts/gen-acl.mjs examples/services.json --dev                 # además CONSUMER.CREATE (desarrollo; §3.4)
```

`consumes` es un subject exacto o un patrón acabado en `>`, y el generador lo **expande
contra el bundle de esquemas** (`schemas/bundle.json`): el nombre de un durable es un único
token y NATS no admite comodines dentro de un token, así que para crearlo y concederlo hay
que conocer el subject exacto. `pedidos.pedido.v1.>` se convierte en un durable por cada
evento que tenga esquema.

El generador rechaza los manifiestos que rompen el modelo, no solo los mal formados:

| Rechaza | Por qué |
|---|---|
| Dos servicios dueños del mismo dominio | Con dos dueños nadie puede confiar en el `source` de un evento — [00-protocol.md §4](00-protocol.md) |
| Un consumo con comodín en el dominio (`*.>`) | Conceder un dominio entero por comodín es lo contrario de una concesión explícita |
| Un patrón sin coincidencias, o un subject exacto que no existe en el bundle | Sin el subject exacto no hay nombre de durable ni permiso. Casi siempre es un esquema que falta o una errata |
| Un comodín que no es el `>` final | No se puede expandir a un durable |
| Consumir un dominio sin dueño declarado | O falta el productor en el manifiesto, o el subject está mal escrito |
| Un servicio suscrito a su propio dominio | Suele indicar que ese flujo debería ser una llamada interna, no un evento |
| Nombre de servicio fuera de `[a-z0-9-]`, o repetido | Alimenta los nombres de durable consumer y el prefijo del inbox — [02-naming.md §4](02-naming.md) |
| Dos subjects que dan el mismo durable | Los puntos y los guiones del subject pasan a `_`: el segundo `consumers.add` reutilizaría el durable del primero con otro filtro, sin error |
| Con `--nkeys`: un servicio sin clave, una clave repetida, o una semilla | Sin clave no hay identidad; repetida es una credencial compartida (§2); una semilla es un secreto y nunca va a una config |

Con `--tenants` (Modelo B, [09-multitenancy.md](09-multitenancy.md)) los permisos de cada
servicio son **los mismos**: cambia que cada tenant es una account con sus propios streams
y durables. Las claves de `--nkeys` pasan a ser `<servicio>@<tenant>` porque NATS rechaza la
misma nkey en dos accounts (*Duplicate nkey*).

### 3.1 DLQ

Un consumidor **DEBE** poder publicar en `dlq.<subject que consume>`, y eso significa
concederle `pub` sobre un subject de un dominio ajeno. Es la única excepción al §3, y
es acotada: solo el prefijo `dlq.`, y **el subject exacto** de cada evento que ya consume
—no `dlq.pedidos.>`—.

**El dueño lee su propia DLQ con un pull consumer sobre `DLQ_<DOMINIO>`**, no con una
suscripción a `dlq.<dominio>.>`. Es lo mismo que cualquier otro consumo, y por eso recibe lo
mismo: los permisos de un durable (`<servicio>__dlq_<dominio>`, filtro
`dlq.<dominio>.>`), sin permiso de suscripción alguno. Medido (A-8a): con el
`CONSUMER.CREATE.DLQ_<DOM>.<durable>.dlq.<dom>.>` exacto el dueño crea y consume su DLQ; con
un filtro más amplio (`dlq.>`), o sobre la DLQ de otro dominio, el broker lo deniega. (En
producción ese durable lo crea `flux provision`, y el `CREATE` solo existe con `--dev`: §3.4.)

La alternativa descartada es la que tenía v1: una suscripción core a `dlq.<dominio>.>`.
Recibe los muertos *en vivo* y nada más: un dueño caído mientras muere un evento no lo ve
nunca, no hay acuse ni posición, y reintroduce un permiso de suscripción sobre un subject
que no es un inbox, que es justo lo que [§3.2](#32-los-permisos-de-subject-no-gobiernan-jetstream)
elimina. Con el pull consumer, además, el dueño puede **reproducir** sus propios eventos
muertos —publicar en `<dominio>.>` es lo único que ya puede hacer—.

### 3.2 Los permisos de subject no gobiernan JetStream

Los siete SDKs consumen **por pull**: piden mensajes publicando en
`$JS.API.CONSUMER.MSG.NEXT.<stream>.<durable>` y los reciben en su inbox. Un permiso de
suscripción sobre `pedidos.>` no interviene en ningún momento —los mensajes no viajan por
ese subject—, así que:

- **Concederlo no sirve**: con él y sin `$JS.API.*`, el servicio no consume nada (A-1,
  `Permissions Violation for Publish to "$JS.API.STREAM.INFO.EVT_PEDIDOS"`).
- **Denegarlo no protege**: lo que confina es `$JS.API`. Un servicio sin ninguna concesión
  sobre `inventario` pero con `_INBOX.>` leyó un evento de `inventario` (A-2), aunque su
  suscripción directa a `inventario.>` sí fue denegada.

**Regla: un servicio no recibe ningún permiso de suscripción sobre subjects de dominio.**
Su lista de suscripción es `_INBOX.<servicio>.>` y nada más.

### 3.3 El inbox es privado

Las entregas de un pull consumer —y los acuses de `publish`— llegan al **inbox** de quien
las pidió. Con un `_INBOX.>` común a todos, cualquier servicio puede suscribirse a él y leer
lo que reciban los demás, tenga o no concedido su dominio (A-2: `facturacion-api` leyó el
evento completo que consumía otro servicio). Es la trampa más barata de cometer, porque
`_INBOX.>` es lo que pone cualquier ejemplo de NATS.

- Cada servicio usa el prefijo **`_INBOX.<servicio>`**, y solo puede suscribirse a
  `_INBOX.<servicio>.>`. Ni `_INBOX.>` ni el de otro servicio (A-7: denegadas las dos).
- El SDK lo **fija solo** a partir del nombre de servicio de `connect()`. **No es una
  opción**: una opción es una forma de olvidarlo.
- El nombre de servicio no admite puntos (`[a-z0-9-]`), así que el prefijo de uno nunca
  puede ser un prefijo de otro: `_INBOX.pedidos.>` no captura `_INBOX.pedidos-api.x`.

### 3.4 Permisos exactos por consumidor durable

Por **cada subject exacto que consume**, y solo por esos, un servicio recibe estos tres
permisos de publicación (el `<durable>` es el de [02-naming.md §4](02-naming.md)):

```
$JS.API.CONSUMER.INFO.<STREAM>.<durable>
$JS.API.CONSUMER.MSG.NEXT.<STREAM>.<durable>
$JS.ACK.<STREAM>.<durable>.>
```

y, **solo en modo desarrollo** (`gen-acl.mjs --dev`), un cuarto:

```
$JS.API.CONSUMER.CREATE.<STREAM>.<durable>.<subject>
```

Más `$JS.API.INFO` y `$JS.API.STREAM.INFO.<STREAM>` de los streams que publica y de los que
consume **y de la DLQ de cada uno de estos**: `subscribe()` comprueba también `DLQ_<DOM>`
y, sin ese permiso, intenta crear el stream, el broker se lo deniega y `subscribe()` falla
(A-8b, medido con el SDK de Node).

Por qué cada uno, y por qué con esa forma:

| Permiso | Para qué | Nota |
|---|---|---|
| `CONSUMER.CREATE.<S>.<d>.<subject>` — **solo con `--dev`** | `consumers.add`: crea el durable o confirma el existente | **El filtro va en el subject**: así es la única forma de que *una ACL vea el filtro*. Con el mismo subject y otro filtro en el cuerpo el servidor responde `Consumer create request did not match filtered subject from create subject` (A-4): no se puede ampliar el consumo por esa vía |
| `CONSUMER.INFO.<S>.<d>` | `consumers.get`, el sondeo de `num_pending`, y el *fallback* de `subscribe()` | |
| `CONSUMER.MSG.NEXT.<S>.<d>` | Pedir mensajes | Es lo que confina el consumo: sin este permiso, el consumidor existe pero no entrega a nadie. Pedir mensajes del durable *de otro servicio* es una violación (A-8) |
| `$JS.ACK.<S>.<d>.>` | `ack`, `nak`, `term` y *work-in-progress* | Medido (A-3): `$JS.ACK.EVT_PEDIDOS.<durable>.1.1.1.<ts>.0`, nueve tokens. El `>` final es porque lo que sigue al durable lo pone el broker |

> **Riesgo declarado.** La forma del subject de ack es del servidor: con un dominio de
> JetStream, o en otra versión, puede llevar tokens delante del stream. Si cambia, la ACL
> **deja de coincidir y se rompe en alto** —los acks se deniegan y los mensajes se
> reentregan—, no en silencio. `verify-acl` fija el formato observado.

**Por qué `CONSUMER.CREATE` no se concede por defecto.** No hace falta con un durable
aprovisionado: el SDK de Node, si el broker le deniega el `add`, cae a `CONSUMER.INFO` y sigue
(A-8b). Y concederlo tiene un coste que no se ve hasta el día malo:

- Un durable **borrado** —a mano, por una limpieza— lo recrearía el propio servicio al arrancar,
  con `deliver_policy: all`: reentrega de hasta 30 días de eventos sin que nadie lo haya
  decidido. Sin el permiso, un durable que falta es un fallo **ruidoso**
  ([`NOT_PROVISIONED`, §3.6](#36-aprovisionamiento-los-streams-no-los-crea-el-servicio)), no un
  replay silencioso.
- Un servicio comprometido con `CREATE` puede cambiar el `ack_wait`, el `max_deliver` o el
  `backoff` de **su propio** consumidor (el filtro no: el subject lo fija).

`--dev` lo concede, solo sobre el durable **propio** y con su filtro exacto, para arrancar sin
`flux provision` (el SDK crea el stream y el consumidor al conectar). Es la configuración de
desarrollo, no la de producción. Los SDKs **DEBEN** hacer *info antes de crear* y tolerar que el
`add` se deniegue si el durable existe; los que solo saben *create-or-update* necesitan `--dev`.

Un `durable` explícito en `subscribe()` (en vez del derivado del servicio y el subject) **no
tiene permisos**: el nombre va dentro de cada uno de los de arriba. Se declara en el
manifiesto o no se usa.

Y el consumo se comprueba **de extremo a extremo**, no por la ausencia de violaciones: un
`ack` o un pull denegados **no fallan en el cliente**, el mensaje se reentrega sin fin y el
servicio «parece sano y no consume». Las entregas llegan además con el subject **original**
por el inbox, que es lo que hace posible la fuga de §3.3.

### 3.5 Lo que nunca se concede

Por omisión, y verificado uno a uno contra el broker:

| Nunca a un servicio | Por qué |
|---|---|
| `$JS.API.CONSUMER.DURABLE.CREATE.*` | La API antigua: el filtro va solo en el cuerpo y **ninguna ACL lo ve**. Concederla permite crear un consumidor sobre cualquier cosa (A-5) |
| `$JS.API.STREAM.MSG.GET.*` y `$JS.API.DIRECT.GET.*` | Leen cualquier mensaje del stream por secuencia, sin consumidor ni filtro: saltan todo lo anterior (A-6) |
| Crear, modificar, borrar o purgar streams; borrar consumidores; listar | Un stream es un recurso compartido de todo el dominio ([02-naming.md §3.2](02-naming.md)) |
| `$JS.API.>`, `$JS.API.*.>`, `>` | Es la forma de «arreglar» una denegación y la que anula el modelo entero: con `$JS.API.>` un servicio crea un consumidor sobre el stream de otro dominio, lee con `STREAM.MSG.GET` y pide mensajes de durables ajenos (reproducido en `verify-acl` con un usuario mutante) |

Lo que sí se expone, para no prometer más de lo cierto: `STREAM.INFO` revela **metadatos**
del stream (recuentos, subjects, tamaño), nunca contenido, y `$JS.API.INFO` los límites de la
cuenta. Un servicio ve eso de los streams que consume; de ningún otro.

### 3.6 Aprovisionamiento: los streams no los crea el servicio

**En producción los streams y los durables se aprovisionan fuera del SDK**
([02-naming.md §3.2](02-naming.md)), con [`flux provision`](../cli/README.md) y credenciales
de **operación**, que no son las de ningún servicio. `flux provision <services.json>` lee el
mismo manifiesto que el generador de ACL y crea, de forma idempotente, lo que esas ACLs
presuponen: `EVT_<DOM>` y `DLQ_<DOM>` por dominio y un durable por subject consumido. Si algo
existe con otra configuración, informa de la diferencia y **no lo sobrescribe**; no borra
nunca.

Un SDK que intente crear un stream o un consumidor y reciba una denegación **DEBE** fallar
con un error accionable, **`NOT_PROVISIONED`**, que nombre el recurso que falta y diga que
lo crea `flux provision`. **NO DEBE** acabar en un *timeout* de un minuto sin explicación. Si
el recurso existe, un `CREATE` denegado **NO** es un error: el SDK sigue con `INFO`. La señal
medida (`verify-acl`) es `consumers.add` → `Permissions Violation` **y** `consumers.info` →
`404`: esa pareja, y no solo la primera, es lo que significa «falta y no puedo crearlo».

Para quien implemente un SDK, dos cosas medidas (A-8f) sobre cómo se manifiestan las
denegaciones en las librerías `@nats-io/*` 3.4.0: una **petición** a un subject sin permiso
falla *en milisegundos* con `Permissions Violation for Publish to "…"` —no es un timeout—;
pero una **suscripción** denegada no lanza nada al suscribirse: el error llega por el canal
de estado y, además, hace que el **iterador de esa suscripción lance** al consumirlo. Si no
se captura, el proceso muere.

## 4. Multi-tenant

`tenantid` es obligatorio en el envelope, pero **el envelope no es un mecanismo de
seguridad**: es un dato que el productor rellena y que un productor comprometido puede
falsificar.

En v1, el aislamiento de tenant es **responsabilidad del consumidor**: filtrar por
`tenantid` antes de actuar y nunca cruzar datos entre tenants. Un SDK L2 **DEBERÍA**
ofrecer un filtro declarativo en `subscribe()` para que ese filtrado no dependa de que
cada handler se acuerde.

> Aislamiento real por tenant (account o subject prefix por tenant) queda en **fase 4**.
> Se documenta aquí para que nadie asuma que ya existe.

## 5. Clasificación de datos

`dataclassification` es obligatorio y toma uno de cuatro valores:

| Valor | Significado | Retención | Consecuencias |
|---|---|---|---|
| `public` | Difundible fuera de la organización | 30 d | — |
| `internal` | Datos internos sin PII | 30 d | — |
| `confidential` | PII, datos de negocio sensibles | **7 d** | Logs sin payload; DLQ con acceso restringido |
| `restricted` | Datos financieros, credenciales, salud | **24 h** | Prohibido en logs; DLQ auditada; requiere aprobación de seguridad para el subject |

- Los campos con PII **DEBEN** marcarse `"x-pii": true` en el JSON Schema.
- Un SDK L2 **DEBERÍA** redactar los campos `x-pii` al escribir el evento en logs.
- La retención más corta para `confidential`/`restricted` no es teatro de cumplimiento:
  **un stream de JetStream es una base de datos persistente**. Un evento con PII en un
  stream con `max_age: 30d` es PII almacenada 30 días, sujeta a las mismas obligaciones
  de GDPR que cualquier tabla — incluido el derecho de supresión, que un log
  append-only no puede satisfacer.

### 5.1 Derecho de supresión

Los eventos son inmutables; el RGPD exige poder borrar. La contradicción se resuelve
**no metiendo PII en el evento**:

- El evento lleva **referencias** (`clienteId`), no PII (`email`, `nombre`, `dni`).
- La PII vive en el servicio dueño, donde se puede borrar de verdad.
- Un consumidor que necesite la PII la pide al dueño y aplica su propia retención.

Un subject cuyo `data` contenga PII directa **DEBE** documentarlo y **DEBERÍA**
rediseñarse. La alternativa —cripto-borrado con clave por sujeto— queda fuera de v1.

## 6. Superficie operativa

- El puerto de monitorización (`8222`) **NO DEBE** exponerse fuera de la red de
  gestión.
- `nats-account-server` / el operador JWT **NO DEBEN** ser accesibles desde las redes
  de aplicación.
- Los `nats` CLI con credenciales de operador **DEBEN** vivir en un bastion auditado,
  no en portátiles.

## 7. Fuera de alcance en v1 (declarado, no olvidado)

| Tema | Estado | Nota |
|---|---|---|
| Firma de eventos | ✅ **Implementada** | Ed25519, extensión opcional — ver [07-signing.md](07-signing.md). Traslada la autenticidad del canal al evento. |
| Cifrado a nivel de campo | Fase 4 | Hoy: no metas ahí lo que no puedas guardar en claro. |
| Aislamiento real de tenant | ✅ **Documentado** | Tres modelos con su coste real — ver [09-multitenancy.md](09-multitenancy.md). El default de v1 (filtrado en consumidor) NO resiste a un servicio legítimo comprometido; el Modelo B sí. |
| Auditoría inmutable de acceso | Fase 4 | Hoy: logs del servidor NATS. |

Esta tabla existe para que nadie confunda "no está en el documento" con "está
resuelto".
