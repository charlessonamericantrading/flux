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
  - **Conexión inicial**: esperar a un broker que aún no ha arrancado, pero **fallar de
    inmediato** ante credenciales rechazadas o un TLS que no encaja — [03-delivery.md
    §6.1](03-delivery.md)—, con uno de los tres códigos estables de [04-errors.md
    §5.3](04-errors.md) (`CONNECTION_REJECTED`, `INVALID_CREDENTIALS`, `INVALID_TLS_OPTIONS`).
    Con TLS propio, el fallo que no se ve es el peor: un certificado que no verifica dejaba al
    servicio arrancado y mudo.
  - **Denegaciones visibles**: un permiso que falta no puede parecer un servicio sano —
    [§3.7](#37-las-denegaciones-se-ven).

  Un SDK que cumple todo esto —y el aprovisionamiento de [§3.6](#36-aprovisionamiento-los-streams-no-los-crea-el-servicio)—
  declara la capacidad **`security`** ([00-protocol.md §5](00-protocol.md)). Mientras un SDK
  no la declare, **no es compatible con la configuración segura** y su README **DEBE**
  decirlo: con el inbox por defecto (`_INBOX.<aleatorio>`) el broker le deniega la
  suscripción a su propio inbox y **su primera petición falla**
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
la fuente, y [`scripts/gen-acl.mjs`](../scripts/gen-acl.mjs) deriva la configuración. Cada servicio
declara `name`, `owns`, `consumes` y, si observa estado replicado, `watches`
([§3.8](#38-estado-replicado-watches-y-sus-permisos)).

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
| Una clave desconocida en un servicio, o ser dueño (o consumir, u observar) un dominio reservado (`dlq`, `state`, `rpc`, `system`) | Una errata no puede ser «un servicio sin permisos», y el «dueño» de `state` recibiría `state.>` — y las reglas de `watches`, en [§3.8](#38-estado-replicado-watches-y-sus-permisos) |
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
| `CONSUMER.INFO.<S>.<d>` | `consumers.get`, el sondeo de `num_pending`, y **lo primero que `subscribe()` pregunta**: *info antes de crear* | Sin él `subscribe()` no puede saber si el durable existe y falla con `PERMISSION_DENIED` ([§3.7](#37-las-denegaciones-se-ven)); **no** es `NOT_PROVISIONED`, porque no se sabe si falta |
| `CONSUMER.MSG.NEXT.<S>.<d>` | Pedir mensajes | Es lo que confina el consumo: sin este permiso, el consumidor existe pero no entrega a nadie —y **`subscribe()` falla con `PERMISSION_DENIED`**, en vez de dejarlo inerte ([§3.7](#37-las-denegaciones-se-ven))—. Pedir mensajes del durable *de otro servicio* es una violación (A-8) |
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
`ack` denegado **no falla en el cliente**, el mensaje se reentrega sin fin y el servicio «parece
sano y no consume»; un pull denegado tampoco falla al pedirlo, y por eso el SDK lo detecta al
suscribirse y hace fallar `subscribe()` ([§3.7](#37-las-denegaciones-se-ven)). Las entregas
llegan además con el subject **original** por el inbox, que es lo que hace posible la fuga de
§3.3.

### 3.5 Lo que nunca se concede

Por omisión, y verificado uno a uno contra el broker:

| Nunca a un servicio | Por qué |
|---|---|
| `$JS.API.CONSUMER.DURABLE.CREATE.*` | La API antigua: el filtro va solo en el cuerpo y **ninguna ACL lo ve**. Concederla permite crear un consumidor sobre cualquier cosa (A-5) |
| `$JS.API.STREAM.MSG.GET.*` y `$JS.API.DIRECT.GET.*` | Leen cualquier mensaje del stream por secuencia, sin consumidor ni filtro: saltan todo lo anterior (A-6). **Una única excepción acotada**: la forma *por subject* de un contrato de estado y un tenant fijado, o de su dominio para el dueño ([§3.8](#38-estado-replicado-watches-y-sus-permisos)); la forma de cuerpo sigue denegada |
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
presuponen: `EVT_<DOM>` y `DLQ_<DOM>` por dominio y un durable por subject consumido; y, para
los dominios con estado, `STATE_<DOM>`, la DLQ de estado y los durables de cada `watches`
([§3.8](#38-estado-replicado-watches-y-sus-permisos)). Si algo
existe con otra configuración, informa de la diferencia y **no lo sobrescribe**; no borra
nunca.

Un SDK que intente crear un stream o un consumidor y reciba una denegación **DEBE** fallar
con un error accionable, **`NOT_PROVISIONED`**, que nombre el recurso que falta y diga que
lo crea `flux provision` (contenido exacto del mensaje y frontera con `PERMISSION_DENIED`:
[04-errors.md §5](04-errors.md)). **NO DEBE** acabar en un *timeout* de un minuto sin
explicación. Si el recurso existe, un `CREATE` denegado **NO** es un error: el SDK sigue con
`INFO`. La señal medida (`verify-acl`) es `consumers.add` → `Permissions Violation` **y**
`consumers.info` → `404`: esa pareja, y no solo la primera, es lo que significa «falta y no
puedo crearlo».

Para quien implemente un SDK, dos cosas medidas (A-8f) sobre cómo se manifiestan las
denegaciones en las librerías `@nats-io/*` 3.4.0: una **petición** a un subject sin permiso
falla *en milisegundos* con `Permissions Violation for Publish to "…"` —no es un timeout—;
pero una **suscripción** denegada no lanza nada al suscribirse: el error llega por el canal
de estado y, además, hace que el **iterador de esa suscripción lance** al consumirlo. Si no
se captura, el proceso muere. Qué **DEBE** hacer un SDK ante cada denegación, en
[§3.7](#37-las-denegaciones-se-ven).

### 3.7 Las denegaciones se ven

Un servicio al que le falta un permiso **no puede parecer sano.** El broker no ayuda: cuando
una ACL deniega una publicación, el servidor la descarta y manda un `-ERR 'Permissions
Violation …'` aparte, por la conexión; no hay respuesta que pueda fallar (A-28). Lo que ve
cada operación, medido contra nats-server 2.14.5, y lo que el SDK **DEBE** hacer con ello:

| Operación denegada | Qué ve el cliente | El SDK |
|---|---|---|
| Petición a la API de JetStream (consultar o crear un stream o un consumidor) o `publish()` | Depende de la librería: un error en ~1 ms (`RequestError` con `cause: PermissionViolationError`, A-21) o **nada**, y la petición espera su *timeout* de 5 a 30 s (A-28) | **DEBE** fallar en tiempo útil, no por *timeout*, con `PERMISSION_DENIED` o `NOT_PROVISIONED` ([04-errors.md §5](04-errors.md)), **nombrando el subject denegado**. Si la librería no entrega la denegación, **DEBE** escuchar los `-ERR` de la conexión y competirlos con la respuesta |
| Suscripción core (la de su inbox) | No lanza al suscribirse: el error llega por el canal de estado y el **iterador** de esa suscripción lanza al consumirla (A-8f). Sin permiso sobre el inbox, ninguna respuesta puede llegar | **DEBE** fallar la operación que la necesita con `PERMISSION_DENIED`, y **NO DEBE** dejar que el error del iterador tumbe el proceso |
| `ack` (y `nak`, `term` y WIP, que publican en el mismo `$JS.ACK.…`: el denegado que se midió fue el ack) | **Silencio.** Son publicaciones sin respuesta: el evento se procesa y se **reentrega cada `ack_wait`, para siempre** (A-19) | **DEBE** registrarlo en el `logger` del SDK, a nivel de error, con el subject denegado y esa consecuencia |
| Petición de mensajes de un pull (`CONSUMER.MSG.NEXT`) | **Silencio.** El consumidor queda inerte y parece sano (A-20) | **DEBE** hacer fallar `subscribe()` con `PERMISSION_DENIED`, nombrando el subject denegado (`$JS.API.CONSUMER.MSG.NEXT.<STREAM>.<durable>`, completo), **antes de devolver** —no basta con registrarlo—, y **DEBE** además registrarlo en el `logger` como cualquier otra denegación. Un consumidor inerte que parece sano es el peor modo de fallo de todo el modelo: descubrirlo por la ausencia de eventos semanas después es el peor momento |

- El `ack` denegado **solo** tiene el log como salida: es una publicación sin respuesta, no se
  puede devolver como `Result`. Tampoco puede devolverse un pull que se deniega **después** de
  que `subscribe()` ya devolvió (un permiso que se pierde al recargar la ACL). Así que un SDK
  `security` **DEBE** ofrecer un `logger` configurable y su README **DEBE** pedir que se
  configure: sin él, un ack denegado no deja rastro en ninguna parte.
- Un ack denegado se repite con cada mensaje. Un SDK **DEBERÍA** avisar una vez por subject:
  un log por mensaje entierra la causa bajo su propio volumen.
- **Cómo detectar el pull denegado antes de devolver es cosa de cada SDK, pero hay una trampa
  medida.** La librería encola el primer pull al iterar el consumidor (A-20), así que mirar
  justo después de crearlo no ve nada: el `-ERR` llega **después**. El SDK de Node arranca el
  bucle, cede un turno y hace un `flush`, que allí es un PING/PONG: el `-ERR` llega antes que
  el PONG porque el protocolo es ordenado. En `async-nats` no hay equivalente —su `flush()` solo
  vacía el buffer de escritura, sin ida y vuelta, y el primer pull lo publica una tarea de fondo
  sin orden respecto a quien llama—: esperar «un poco» y mirar es una carrera (medido con el
  permiso quitado: `subscribe()` devolvía `Ok` a los ~4 ms y la denegación se registraba ~2 ms
  **después**). Por eso el SDK de Rust no espera al pull de la librería: pregunta lo mismo sobre
  **el mismo subject** con una petición propia, que el servidor contesta `400` sin entregar
  nada, y compite su respuesta con la denegación (la primera fila de esta tabla). La
  detección **NO DEBE** depender de un temporizador fijo. Ver `sdk-rust/tests/security.rs`
  (`g_*`).
- Cómo llega la denegación lo decide la librería. Cuando da un tipo (`PermissionViolationError`)
  se reconoce por él, **recorriendo la cadena de causas** —llega envuelta en el error de la
  petición— y por **nombre** y no por `instanceof`, que con dos copias de la librería en el
  árbol de dependencias falla sin ruido. Cuando solo da texto (`async-nats`: el `-ERR` llega
  como `ServerError::Other`), se interpreta ese texto, y es el único sitio donde hacerlo está
  justificado.
- **No se define ninguna métrica nueva.** El contrato de [08-observability.md](08-observability.md)
  no tiene una para esto y ampliarlo es otra decisión. Un handler cuyo error es
  `PERMISSION_DENIED` o `NOT_PROVISIONED` sí queda contado por la que ya existe
  (`flux_events_dlq_total{code=…}`, [04-errors.md §5.2](04-errors.md)); un ack o un pull
  denegados, no: solo el log.

Se comprueba **de extremo a extremo** y **en los dos sentidos** ([§3.4](#34-permisos-exactos-por-consumidor-durable)):
con el permiso quitado, la denegación se *nota* por el canal que corresponde a su fila.

### 3.8 Estado replicado: `watches` y sus permisos

El estado ([10-state.md](10-state.md)) añade dos roles al manifiesto: el **dueño** de un dominio que tiene
estado y el **observador** de un contrato. Los permisos de cada uno los fija `protocol.json` → `state.acl`
(derivados de A-38 a A-40 y A-48, medidos a mano contra un broker) y los **genera**
[`gen-acl.mjs`](../scripts/gen-acl.mjs); [`verify-acl.mjs`](../scripts/verify-acl.mjs) y los casos S15 a S20 de
la suite de seguridad los **ejecutan** contra un nats-server real —con los dos sentidos: lo concedido
funciona, y lo no concedido se deniega—. Todo lo de [§3.1 a §3.7](#31-dlq) rige igual: ningún permiso de
suscripción, inbox privado, `CONSUMER.CREATE` solo con `--dev`, y las denegaciones se ven.

#### El manifiesto

```jsonc
{ "services": [
  { "name": "crm-api",      "owns": ["clientes"] },                       // dueño: del estado de clientes también
  { "name": "tienda-web",   "watches": [ { "contract": "clientes.cliente.v1", "tenants": ["acme"] } ] },
  { "name": "informes-api", "watches": ["clientes.cliente.v1"] }          // todos los tenants
] }
```

- **Un dominio *tiene* estado si el bundle de esquemas tiene un contrato suyo**
  (`schemas/state/<dominio>/…`, índice `state` de `schemas/bundle.json`: [10-state.md §10](10-state.md)).
  El dueño no declara nada más que `owns`; sin contratos de estado en el bundle, no recibe ningún permiso de
  estado.
- **`watches`** es una lista de **contratos** (`<dominio>.<agregado>.v<major>`, sin evento ni tenant ni clave:
  `clientes.cliente.v1`). Cada entrada es el contrato a secas —observa **todos** los tenants— o un objeto
  `{ "contract": …, "tenants": ["acme", …] }`, que **confina** al servicio a esos tenants. Un contrato
  observado necesita dueño declarado y esquema en el bundle.
- **El confinamiento lo aplica el broker, no el SDK.** Con `tenants`, el generador concede un durable y un
  `DIRECT.GET` **por tenant**, con el tenant fijado en el subject del permiso; sin `tenants`, el tenant es
  `*`. Con el tenant solo en el sobre, ningún permiso podría confinar a nadie ([10-state.md §2.2](10-state.md)).
- **El durable lleva el tenant en el nombre si confina, y solo entonces**:
  `tienda-web__state_clientes_cliente_v1__acme` frente a `informes-api__state_clientes_cliente_v1`. Un durable
  compartido reparte los mensajes entre quienes lo usan (A-47): sin el tenant, dos instancias de un servicio
  —una por tenant— se quitarían la mitad de la foto.

| El generador rechaza | Por qué |
|---|---|
| `tenants` vacío (`[]`) | Leído como «ninguno» y aplicado como «todos» es la trampa de `allow: []` (A-25): para todos los tenants, el contrato a secas |
| Un `tenant` en singular, o cualquier clave desconocida en una entrada o en un servicio (`watch` por `watches`) | Una errata no puede degradarse a «todos los tenants» ni a «un servicio sin permisos» |
| Un tenant que no cumple `state.tenant.pattern` (mayúsculas, `*`, más de 64 caracteres) | El de un estado es más estricto que el de un evento ([10-state.md §2.4](10-state.md)); un comodín ampliaría el permiso |
| Un contrato que no existe en el bundle, o un subject de **evento** en `watches` | Sin esquema de estado no hay stream ni durable; un evento va en `consumes` |
| El estado en `consumes` (`state.clientes.cliente.v1.acme`) | `state` es un dominio reservado: el estado no se consume como un evento |
| Observar un dominio sin dueño, o el **propio** | Como con los eventos ([§3.0](#30-las-acls-se-generan-no-se-escriben)): el dueño ya lo escribe y lo lee con `get` |
| El mismo contrato dos veces en un servicio, o dos contratos que dan el mismo durable | Los puntos y los guiones pasan a `_`: el segundo `consumers.add` reutilizaría el durable del primero con otro filtro |
| Ser dueño de un dominio **reservado** (`dlq`, `state`, `rpc`, `system`: [02-naming.md §1.3](02-naming.md)) | `ownDomain` casa con `state.>`: el «dueño» de `state` recibiría permiso para escribir el estado de **todos** los dominios (`RESERVED_DOMAIN`) |

#### Qué recibe cada rol

Con el ejemplo de arriba (`gen-acl.mjs examples/services.json`), y solo los permisos **de estado**; el resto
—`$JS.API.INFO`, el inbox, los permisos de eventos— es el de §3.4:

```jsonc
// crm-api — dueño del estado de clientes
"state.clientes.>",                                                                   // escribe CUALQUIER clave de CUALQUIER tenant
"$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.>",                                 // el paso 1 de `put` y su `get`
"$JS.API.CONSUMER.INFO.DLQ_CLIENTES.crm-api__dlq_state_clientes",                     // lee su DLQ de estado con un pull consumer
"$JS.API.CONSUMER.MSG.NEXT.DLQ_CLIENTES.crm-api__dlq_state_clientes",
"$JS.ACK.DLQ_CLIENTES.crm-api__dlq_state_clientes.>",

// tienda-web — observa clientes.cliente.v1 confinada a acme
"$JS.API.CONSUMER.INFO.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme",   // los permisos de SU durable
"$JS.API.CONSUMER.MSG.NEXT.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme",
"$JS.ACK.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme.>",
"$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.*",                 // `get`, con el tenant FIJADO
"$JS.API.STREAM.INFO.DLQ_CLIENTES",                                                   // el SDK comprueba la DLQ al abrir…
"dlq.state.clientes.cliente.v1",                                                      // …y manda ahí lo que no puede aplicar

// informes-api — observa clientes.cliente.v1 sin confinar: mismo patrón, tenant `*`
"$JS.API.CONSUMER.INFO.STATE_CLIENTES.informes-api__state_clientes_cliente_v1", …
"$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.*.*",
```

y, **solo con `--dev`**, `CONSUMER.CREATE.STATE_CLIENTES.<durable>.state.clientes.cliente.v1.<tenant|*>.*`
sobre el durable propio (con el filtro exacto) y `CONSUMER.CREATE.DLQ_CLIENTES.<durable>.dlq.state.clientes.>` el
dueño, por la misma razón que en §3.4.

**Cada permiso, y por qué es esa forma exacta** (lo medido, en la suite como S15 a S20):

| Permiso | Para qué | Lo que impide la forma |
|---|---|---|
| `state.<dom>.>` (solo el dueño) | `put` y `delete`, que son publicaciones condicionales por JetStream ([10-state.md §3](10-state.md)) | Un observador, un servicio sin estado y el dueño de **otro** dominio no escriben estado ajeno. Un `publish` de **core** a un subject de estado lo captura el stream **sin acuse y sin chequeo de secuencia** y sobrescribe (A-45): la ACL es lo único que lo impide (S16) |
| `DIRECT.GET.STATE_<DOM>.state.<dom>.>` (dueño) | El paso 1 de `put` (leer la secuencia y la versión) y su `get` | Solo su stream y solo su dominio |
| `DIRECT.GET.STATE_<DOM>.<contrato>.<tenant\|*>.*` (observador) | `get` de lo que observa | **El tenant fijado**: con el permiso de `…acme.*`, una clave de `acme` se lee; `…globex.*`, `…v1.*.*` y `state.<dom>.>` se deniegan (A-14, A-38; S17). Un comodín **en la clave** (`…acme.>`) sí pasa: sigue siendo su tenant |
| `CONSUMER.INFO`, `MSG.NEXT` y `$JS.ACK…>` de **su** durable | `watch`: lo primero que pregunta, pedir la foto y confirmar | Los del durable de **otro** servicio (A-8); quitar uno rompe el `watch` de la forma de siempre (§3.7: S20) |
| `STREAM.INFO.DLQ_<DOM>` y `dlq.state.<contrato>` (observador) | El SDK comprueba la DLQ al abrir y le manda lo que no puede aplicar | Un solo subject de DLQ **por contrato** (cinco tokens, sin tenant ni clave): no `dlq.state.>`, ni la de otro contrato, ni la forma con clave (S19) |
| Durable `<servicio>__dlq_state_<dom>` (dueño) | Leer su DLQ de estado como la de eventos (§3.1) | Un observador no la lee, aunque no lleve el dato: sí lleva claves y errores |

**Nunca, a ningún rol, ni siquiera al dueño:**

- **`STREAM.INFO.STATE_<DOM>`.** Con `subjects_filter` en el cuerpo devuelve **todas las claves y todos los
  tenants** del stream con su recuento (A-39): con ese único permiso un servicio confinado a `acme` conoce los
  clientes de `globex`. El SDK no lo usa —`put`, `get` y `watch` funcionan sin él, porque `CONSUMER.INFO`
  distingue «no existe el consumidor» de «no existe el stream» (A-48)— y ningún patrón de `protocol.json` lo
  casa. [10-state.md §8](10-state.md) lo dice igual: **el dueño tampoco lo recibe**. Si un día
  hace falta, es un permiso más del dueño y nunca del observador (S18).
- **La forma de cuerpo de `DIRECT.GET`** (`$JS.API.DIRECT.GET.<stream>` + `{"seq": n}` o `{"last_by_subj": …}`)
  lee cualquier mensaje, y ninguna ACL ve el cuerpo. No casa con el permiso por subject, y el broker rechaza un
  cuerpo en la forma por subject (`408`, A-38): no se puede colar la lectura de otro mensaje por un permiso de
  contrato y tenant (S18).
- **Gestionar el stream ni sus durables** (`STREAM.CREATE|UPDATE|DELETE|PURGE`, `CONSUMER.DELETE|NAMES`,
  `CONSUMER.DURABLE.CREATE`): son de quien aprovisiona. Un durable de estado se recrea con `flux state resync`,
  con credenciales de operación.

#### La excepción a §3.5

[§3.5](#35-lo-que-nunca-se-concede) niega a un servicio `$JS.API.DIRECT.GET.*`. Este apartado la **acota**, no la
levanta: lo que se concede es la **forma por subject** de un contrato y un tenant concretos —o, para el dueño,
de su dominio—, y es lo único. En `protocol.json` la lista de lo denegable sigue siendo
`security.acl.neverGrantedPattern`, que caza todo `DIRECT.GET`; las dos formas concedibles figuran en
`security.acl.neverGrantedExceptions`, que **refiere** su patrón en `state.acl` (una sola fuente). Es una lista
aparte y no un `(?!…)` dentro del patrón porque los motores sin lookahead (Rust `regex`, Go `RE2`) tienen que
poder compilarlo; quien aplique el patrón ignorando las excepciones se equivoca hacia lo seguro. La forma de
cuerpo, cualquier comodín por encima del tenant y `DIRECT.GET.>` siguen denegados. `check-protocol-json.mjs` lo
prueba en los dos sentidos.

#### Aprovisionamiento

`flux provision` ([§3.6](#36-aprovisionamiento-los-streams-no-los-crea-el-servicio)) lee el mismo manifiesto y,
para cada dominio con estado:

- crea **`STATE_<DOM>`** con la configuración de `protocol.json` → `state.stream`
  (`max_msgs_per_subject: 1`, `allow_direct`, `max_age: 0`, `discard: old`, `duplicate_window: 2m`; réplicas con
  `--replicas`);
- hace que **`DLQ_<DOM>`** capture también `dlq.state.<dom>.>`. Si ya existía con solo `dlq.<dom>.>` —lo creó un
  `flux provision` de la 1.0—, **lo amplía**: es lo único que la herramienta cambia en un recurso existente, no
  pierde mensajes (A-44) y `--dry-run` lo muestra como `se ampliaría`. Cualquier otra diferencia se informa y no
  se toca;
- crea el durable de cada `watches` (uno por tenant si confina) con `deliver_policy: last_per_subject` y el resto
  de la configuración canónica ([03-delivery.md §2](03-delivery.md)), y el durable con el que el dueño lee su
  DLQ de estado.

Un dominio **sin** contratos de estado conserva su `DLQ_<DOM>` de la 1.0 y no recibe ningún permiso de estado:
1.1 es aditiva.

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
**separando lo que pasó de lo que se sabe**: el evento lleva referencias y el dato vive en el
**estado**, que sí se puede borrar ([10-state.md](10-state.md)).

| | Evento | Estado |
|---|---|---|
| ¿Datos personales? | **No.** Solo referencias (`clienteId`). La regla dura 5 de `AGENTS.md` no cambia | **Sí**, con `dataclassification: "confidential"` y `"x-pii": true` en los campos del esquema |
| ¿Se puede borrar? | No: es un log inmutable | **Sí**: `state.delete` sustituye el valor por una lápida |

- El evento dice **qué pasó**: `clienteId`, no `email` ni `nombre`.
- El estado lleva **el dato**: `put` guarda el valor actual de cada agregado y `delete` lo
  sustituye por una lápida, con lo que el valor anterior **sale del stream**. El consumidor
  ya no pide el dato al dueño —que es justo la llamada que flux quiere eliminar—: lo tiene
  porque le llegó, y lo mantiene al día con `state.watch`.
- Un consumidor **DEBE** borrar o anonimizar su copia local al recibir una lápida. Es lo que
  hace real el derecho de supresión **aguas abajo**.
- **`restricted` no se admite en estado** en 1.1 (credenciales, salud, financiero): su retención
  de 24 h no cabe en un almacén que no caduca, y no debe tomarse esa decisión por omisión.
- **La DLQ de estado no conserva el dato** (`data` pasa a `{ key, revision }`), para que un dato
  borrado no siga 90 días en la DLQ ([10-state.md §6](10-state.md)).
- Lo que el estado **no** garantiza: el borrado **físico** en disco —depende de la compactación
  de JetStream—, las copias fuera del broker (backups, réplicas locales que ignoren la lápida) y
  la privacidad de las **claves** y los **tenants**, visibles en los metadatos del stream
  ([10-state.md §7 y §8](10-state.md)). Por eso no son PII. La garantía fuerte es el cifrado del
  payload (fase 7).

Un subject de **evento** cuyo `data` contenga PII directa **DEBE** documentarlo y **DEBERÍA**
rediseñarse: pasar el dato al estado y dejar la referencia en el evento. La alternativa
—cripto-borrado con clave por sujeto— queda fuera de v1.

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
