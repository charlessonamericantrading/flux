# 10 — Estado replicado

> **Estado:** extensión **opcional** de v1.1 (capacidad `state`, [00-protocol.md §5](00-protocol.md)).
> Un SDK L1/L2/L3 conforme no necesita implementarla; un servicio que solo use eventos sigue
> siendo conforme.
>
> Cada observación citada como `A-n` está medida contra **nats-server 2.14.5** y descrita en el
> [Apéndice A del plan v1.1](../docs/plan-v1.1/PLAN.md#apéndice-a--evidencia). Las que son del
> broker son casos ejecutables de [`conformance/cases/state-broker.json`](../conformance/cases/state-broker.json).
> Las observaciones A-9 a A-14 del plan original se **reprodujeron** (A-31 a A-38: cada fila del
> Apéndice dice cuál reproduce) y la fase añadió A-39 a A-50, entre ellas dos que cambian el
> diseño: **el conflicto de secuencia tiene dos códigos** (A-33, A-49) y **`STREAM.INFO` lista
> las claves y los tenants** (A-39).

---

## 1. Qué problema resuelve

Un evento dice **qué pasó**. Quien llega tarde, o quien necesita *el dato* y no *el hecho*, no
tiene nada que hacer con él: ha de llamar al dueño (`GET /clientes/123`) o pedirle una carga
inicial (`GET /clientes?page=`). Es justo lo que flux quiere eliminar, y es el hueco que dejaba
la regla «los eventos no llevan datos personales; pídelos al dueño»
([06-security.md §5.1](06-security.md)).

El **estado** es el valor **actual** de un agregado, publicado por su dueño y replicado a quien
lo observa:

| Hoy (HTTP) | Con estado |
|---|---|
| `GET /clientes/123` — leer un dato que guarda otro sistema | `state.get` |
| `GET /clientes?page=…` — carga inicial | `state.watch` entrega **una foto completa** y después los cambios, por el mismo canal |
| Polling de `GET /cambios?desde=` para mantener una copia local | `state.watch`: el dato llega solo, cuando cambia |

**La regla que evita recrear el problema.** Si lo que necesitas es *un dato que otro sistema
guarda*, es estado. La petición/respuesta (fase 3) queda para órdenes que necesitan respuesta ya
y para cálculos; usarla para leer datos es volver a tener una API con otro transporte.

**Qué no es.** No es un evento: no tiene historia, solo el último valor. No es una base de datos:
no hay consultas, ni índices, ni transacciones entre claves. Es una copia clave → valor con una
sola regla de escritura (§3) y una sola de lectura (§5). Y no sustituye al evento: el dueño
sigue publicando **eventos** para que los demás *reaccionen*; el estado sirve para que
*conozcan*. Lo normal es publicar los dos — el evento lleva la referencia (`clienteId`) y el
estado lleva el dato.

## 2. Modelo

### 2.1 Un stream por dominio, un valor por clave

```
STATE_<DOMINIO>           subjects: state.<dominio>.>

  storage:               file
  retention:             limits
  max_msgs_per_subject:  1        # un valor por clave: el stream ES el estado actual
  allow_direct:          true     # lecturas puntuales sin consumidor
  max_age:               0        # sin caducidad: el estado vive hasta que se borra
  discard:               old
  duplicate_window:      2m
  replicas:              3        # producción; 1 en desarrollo
```

Por qué cada línea:

- **`max_msgs_per_subject: 1` + `discard: old`** es lo que convierte un log en un mapa: al entrar
  un mensaje nuevo en un subject, el anterior **sale** del stream. Tres escrituras sobre dos
  claves dejan dos mensajes (A-31). Con `discard: new` y `discard_new_per_subject` el broker
  **rechazaría** la segunda escritura (`maximum messages per subject exceeded`, err 10077):
  sería un «crear solo si no existe», no un estado (A-46).
- **`allow_direct: true`** habilita `$JS.API.DIRECT.GET.…`, la lectura puntual por subject sin
  crear un consumidor (§5.1). Sin él, `get` y el paso 1 de `put` reciben «sin respuesta» (A-32).
- **`max_age: 0`**: el estado no caduca. Un valor que desapareciera a los 30 días sería una
  copia que miente. Se borra con una lápida (§4).
- **`duplicate_window: 2m`**: igual que los eventos ([03-delivery.md §3](03-delivery.md)). Para
  las escrituras no interviene: el chequeo de secuencia se evalúa **antes** que el de duplicados
  (A-33), y la defensa contra el acuse perdido es otra (§3.2, paso 1b).
- **`replicas: 3`** en producción y **1** si el SDK lo crea en desarrollo, por la misma razón
  que en [02-naming.md §3.2](02-naming.md). Medido con 3 nodos (A-49): el servidor devuelve la
  configuración pedida, sin sobrescribirla.

> El servidor devolvió en todas las pruebas la configuración que se le pidió (A-31, A-49). Aun
> así, `flux doctor` **DEBE** auditar `max_msgs_per_subject`, `allow_direct`, `max_age` y
> `discard` de cada `STATE_<DOM>`: son los cuatro que, mal puestos, no dan error y rompen la
> semántica.

Un dominio **tiene estado** si el bundle de esquemas tiene algún contrato de estado suyo
(`schemas/state/<dominio>/…`, [§10](#10-qué-debe-un-sdk-state)). El stream **se aprovisiona fuera
del SDK** ([06-security.md §3.6](06-security.md)): `flux provision` lo crea para cada dominio que
tenga estado. Los SDKs **NO DEBEN** consultar el stream de estado con `STREAM.INFO` ni crearlo
por defecto (§8 explica por qué). Un SDK **PUEDE** ofrecer, solo como opción explícita de
desarrollo, crearlo con esta configuración.

### 2.2 El subject

```
state.<dominio>.<agregado>.v<major>.<tenant>.<clave>
```

Seis tokens, siempre. Es el subject de NATS de la entrada; **no** es el atributo `subject` de
CloudEvents ([01-envelope.md §2.1](01-envelope.md)), que aquí vale la clave.

| Token | Regla |
|---|---|
| `state` | Literal. Espacio de nombres del protocolo, como `dlq.` ([§2.7](#27-dominios-reservados)) |
| `dominio`, `agregado`, `v<major>` | Los del contrato, como en [02-naming.md §1](02-naming.md) |
| `tenant` | §2.4 |
| `clave` | §2.3 |

El **nombre de contrato** de un estado es `<dominio>.<agregado>.v<major>` (`clientes.cliente.v1`):
el subject sin `state.`, sin tenant y sin clave. Es lo que recibe `state.watch` y lo que figura
en las métricas. Una cadena que no tiene esa forma —`clientes.cliente` sin versión,
`clientes.cliente.v1.creado` con cuatro tokens, `Clientes.cliente.v1` con mayúsculas,
`clientes.cliente.v10000` con una versión de cinco dígitos —el tope es `v9999`,
[02-naming.md §1](02-naming.md)— es **`INVALID_STATE_CONTRACT`** (PERMANENT); un dominio reservado ([§2.7](#27-dominios-reservados)),
`RESERVED_DOMAIN`, que se evalúa antes (§3.6).

**Por qué el tenant va en el subject y no solo en el sobre.** Con el tenant solo en el sobre, dos
tenants con la misma clave (`cli-1`) escribirían **el mismo mensaje**: el segundo pisaría al
primero sin ningún error, porque un stream de estado guarda *un valor por subject*. Y el broker
no podría confinar a nadie por tenant: no ve el sobre. En el subject, el aislamiento por tenant lo
**aplica el broker** (A-38): un observador con permiso sobre `…v1.acme.*` no puede crear un
consumidor con otro filtro ni leer una clave de `globex`.

> Esto **no contradice** el rechazo del Modelo C en [09-multitenancy.md §2](09-multitenancy.md).
> El Modelo C ponía el tenant *delante de los eventos*, lo que rompía la regla de 4 tokens de
> la que dependen los wildcards, los nombres de stream y la derivación del `type`. El estado es
> otro espacio de nombres —`state.`, con su propio stream y su propia forma de seis tokens— y no
> toca nada de lo anterior. El coste es el que el Modelo C ya tenía: el aislamiento por tenant
> de un estado está en la **ACL**, y una ACL por tenant es más larga.

### 2.3 La clave

```
^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$
```

Un único token de NATS: sin puntos, sin comodines, sin espacios, de 1 a 128 caracteres. **Se
distinguen mayúsculas** (los subjects de NATS son case-sensitive: `Cli-1` y `cli-1` son dos
claves distintas, no la misma).

- **Sin codificación automática.** Una clave que no cabe en un token es un error
  (`INVALID_STATE_KEY`), no algo que cada SDK escape a su manera: dos escapes distintos serían
  dos claves distintas para el mismo agregado.
- **El broker no valida nada de esto, y por eso lo valida el SDK.** Medido (A-41): NATS acepta
  como clave `*`, `>`, `a.b` (que son *dos* tokens: la entrada quedaría fuera del filtro de
  todos los observadores, sin error), `café` y una cadena de 2000 caracteres. Solo rechaza los
  espacios. Una clave sin validar es un subject fantasma, la misma familia de trampas que las
  mayúsculas de [02-naming.md §1.1](02-naming.md).
- **Coincidencia completa, no `$`.** En Python (`re.match`) y en PHP (`preg_match` sin `D`) el
  `$` acepta **un `\n` final**: `"cli-1\n"` pasaría el patrón y llegaría con su salto de línea a
  la librería de NATS, que según cuál lo rechaza con un error poco claro o no. Se valida con
  `fullmatch`, `\z` o el modificador `D`; JavaScript, Go y Rust no tienen el problema. Los
  vectores incluyen esa clave.
- **No empieza por `_`.** El prefijo `_` queda **reservado al protocolo** (*decidido
  provisionalmente*, §12): endurecer la regla más tarde sería un cambio incompatible, relajarla
  no. Hoy ninguna clave reservada tiene uso.
- **La clave es un identificador opaco, no un dato.** Nunca PII ni secretos: es visible en los
  metadatos del stream ([§8](#8-permisos)) y en los logs. `cli-1`, no `ana@example.com`.

### 2.4 El tenant

```
^[a-z0-9]+(-[a-z0-9]+)*$        longitud máxima: 64
```

`kebab-case` en minúsculas, como los demás tokens del protocolo. `"system"` es un tenant válido.
Dos matices que importan:

- **El tenant de un evento es cualquier cadena** ([01-envelope.md §3.1](01-envelope.md)); el de
  un estado, no. Un servicio con `tenantId: "ACME_Corp"` publica eventos sin problema y no puede
  escribir estado: `INVALID_TENANT_FOR_STATE`. El límite de 64 sale de que el durable de un
  observador (§5.2) lleva el tenant en el nombre y NATS limita los nombres de consumidor a **255**
  (A-42).
- **No hay valor por defecto.** Un evento sin tenant explícito cae en `system`; un estado **no**:
  el tenant está en el subject, y un tenant equivocado no es una etiqueta mal puesta sino un dato
  guardado en otro sitio ([09-multitenancy.md §5](09-multitenancy.md) ya dice que `system` no es
  un comodín). Quien quiera `system` lo dice (*decidido provisionalmente*).
- **Cómo se resuelve, por operación.** No es el mismo orden para todas, y esa diferencia es
  deliberada:

  | Operación | Orden de resolución | Si no sale ninguno |
  |---|---|---|
  | `put`, `delete`, `get` | la opción de la llamada → el **evento en curso** → el `tenantId` de `connect()` | `INVALID_TENANT_FOR_STATE` |
  | `watch` | la opción de la llamada → el `tenantId` de `connect()`. **Nunca el evento en curso** | se observan **todos** los tenants (con `tenantIsolation: "strict"`, un error de configuración: [§5.2](#52-watch)) |

  Un `put` o un `get` **dentro de un handler** derivan de un evento, y un estado derivado
  pertenece al tenant del evento que lo causó. Un `watch` **no corre dentro de un handler**: se
  abre al arrancar y vive tanto como el servicio, y heredar el tenant de «el evento que se esté
  procesando» haría que su filtro dependiera de qué evento llegó primero, o de si alguien lo abrió
  desde un handler por descuido. Lo que un `watch` observa lo decide quien lo escribe o quien
  configura la conexión, nunca el tráfico.

### 2.5 El sobre

Un CloudEvent normal ([01-envelope.md](01-envelope.md)): las mismas extensiones obligatorias, la
misma validación L3, la misma firma. Lo que cambia:

| Atributo | Valor en una entrada de estado |
|---|---|
| `type` | `com.flux.state.<dominio>.<agregado>.v<major>`; el de una lápida, `com.flux.state.<dominio>.<agregado>.deleted.v<major>` (§4) |
| `subject` | **La clave.** Obligatorio, e **idéntico** al último token del subject de NATS |
| `tenantid` | **Idéntico** al token de tenant del subject de NATS |
| `dataschema` | El esquema del contrato (`schemas/state/<dominio>/<agregado>/<semver>.json`); el de una lápida, fijo (§4) |
| `dataclassification` | `public`, `internal` o `confidential`. **`restricted` no se admite** en estado (§7) |
| `partitionkey` | La clave, como en todo evento ([01-envelope.md §3.2](01-envelope.md)) |
| `data` | Un objeto con **`aggregateVersion`** (entero ≥ 1), §3.4 |

**`tenantid` y `subject` DEBEN coincidir con el subject de NATS.** La identidad de una entrada
está en el subject; el sobre la repite para quien lo lea fuera del broker (un backup, un fichero).
Si discrepan, uno de los dos miente, y quien consume no puede saber cuál: es un mensaje roto, no
un dato con un tenant raro (§5.4: **POISON**, `STATE_SUBJECT_MISMATCH`). Es la versión de estado
de [01-envelope.md §2.4](01-envelope.md): un tipo equivocado no se coacciona, se rechaza.

El orden de los atributos es el de siempre ([01-envelope.md §6](01-envelope.md)) y `data` va el
último. No hay ningún atributo nuevo en el sobre: todo lo propio del estado está en el subject, en
el `type` y en `data`.

### 2.6 Ejemplos byte a byte

Tres líneas, sin espacios ni saltos entre atributos, UTF-8 literal (`María`, `país`: sin
escapes `\u`, [01-envelope.md §1.1](01-envelope.md)). Lo que está entre los bytes es el contrato:
si un SDK produce otra secuencia para la misma entrada, está mal. Las reproduce el caso
[`cross-sdk-state-envelope`](../conformance/cases/cross-sdk-state-envelope.json) con su SHA-256.

**`put`** de `clientes.cliente.v1`, tenant `acme`, clave `cli-1`, `aggregateVersion` 7
(subject de NATS: `state.clientes.cliente.v1.acme.cli-1`; 563 bytes):

```
{"specversion":"1.0","id":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55","source":"/produccion/crm-api","type":"com.flux.state.clientes.cliente.v1","time":"2026-08-20T10:25:39.412Z","datacontenttype":"application/json","dataschema":"https://schemas.internal/state/clientes/cliente/1.0.0.json","subject":"cli-1","correlationid":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55","tenantid":"acme","producerversion":"2.1.0","dataclassification":"confidential","partitionkey":"cli-1","data":{"clienteId":"cli-1","nombre":"María Peña","email":"maria@example.com","aggregateVersion":7}}
```

SHA-256: `fa6b20beb64f90c06b0cfb2c82d809a8fd2672b365681d2cd9d1a99f29b13b40`.

**Lápida** (`delete` de esa clave, `aggregateVersion` 8; 495 bytes). Mismo subject de NATS:

```
{"specversion":"1.0","id":"01924f90-2b11-7a40-8c3d-6d5e4f3a2b10","source":"/produccion/crm-api","type":"com.flux.state.clientes.cliente.deleted.v1","time":"2026-08-20T10:31:02.057Z","datacontenttype":"application/json","dataschema":"https://schemas.internal/system/state/eliminado/1.0.0.json","subject":"cli-1","correlationid":"01924f90-2b11-7a40-8c3d-6d5e4f3a2b10","tenantid":"acme","producerversion":"2.1.0","dataclassification":"internal","partitionkey":"cli-1","data":{"aggregateVersion":8}}
```

SHA-256: `7a7868d2f643ccd584a354bce87c26d8d7f20826f23da26e99dbed78269bed3d`.

**Evento de DLQ de estado** (un observador, `tienda-web`, no pudo aplicar la revisión 41 del
`put` de arriba; subject de NATS: `dlq.state.clientes.cliente.v1`; 719 bytes). Es el sobre
**original** con las extensiones `dlq*` y **sin su `data`**: §6.

```
{"specversion":"1.0","id":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55","source":"/produccion/crm-api","type":"com.flux.state.clientes.cliente.v1","time":"2026-08-20T10:25:39.412Z","datacontenttype":"application/json","dataschema":"https://schemas.internal/state/clientes/cliente/1.0.0.json","subject":"cli-1","correlationid":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55","tenantid":"acme","producerversion":"2.1.0","dataclassification":"confidential","partitionkey":"cli-1","dlqreason":"permanent","dlqattempts":1,"dlqconsumer":"tienda-web__state_clientes_cliente_v1__acme","dlqerror":"PAIS_NO_SOPORTADO: el cliente cli-1 tiene un país fuera del catálogo","dlqtime":"2026-08-20T10:25:40.117Z","data":{"key":"cli-1","revision":41}}
```

SHA-256: `dbb6d882f7df49e4b88026f17fb7b83e9e6c2226aa731ad25d71fc41c8b8f6d9`.

Se comprueban con `JSON.stringify` (Node) y con `json.dumps(ensure_ascii=False,
separators=(",", ":"))` (Python): los mismos bytes.

### 2.7 Dominios reservados

`dlq`, `state`, `rpc` y `system` **NO DEBEN** usarse como dominio ([02-naming.md §1.3](02-naming.md)).
Cada uno es un espacio de nombres del protocolo, y un dominio con ese nombre colisionaría con él
**en el broker**: un `EVT_STATE` (`state.>`) no puede crearse si existe `STATE_CLIENTES`
(`state.clientes.>`), ni un `EVT_DLQ` (`dlq.>`) junto a `DLQ_PEDIDOS` (`dlq.pedidos.>`):
`subjects overlap with an existing stream`, err 10065 (A-43). `dlq` no estaba reservado en v1
y debería.

Toda API que reciba un subject o un contrato —`publish`, `subscribe`, `put`, `get`, `delete`,
`watch`— **DEBE** rechazar un dominio reservado con **`RESERVED_DOMAIN`** (PERMANENT) antes de
tocar la red. En particular, **`publish()` no puede escribir en `state.`, `rpc.` ni `dlq.`**: un
evento publicado ahí no pasaría por el chequeo de secuencia del estado (A-45). Los validan además
`gen-acl.mjs` y `flux validate` sobre el manifiesto, y `bundle-schemas.mjs` sobre `schemas/`.

## 3. Escribir

Solo el **dueño** del dominio escribe estado: la ACL del broker lo aplica igual que con los
eventos ([06-security.md §3](06-security.md)), y un servicio no puede escribir en un dominio
ajeno aunque su código lo intente.

### 3.1 `put` es condicional

```ts
await bus.state.put("clientes.cliente.v1", "cli-1",
  { clienteId: "cli-1", nombre: "Ana", email: "ana@example.com", aggregateVersion: 7 },
  { classification: "confidential" });      // → { applied: true, outcome: "ok", revision: 41 }
```

Un `put` que solo publicara dejaría que dos instancias del dueño, o una instancia lenta, pisaran
un valor nuevo con uno viejo: la última escritura en llegar al broker gana, no la de versión más
alta. Y **un consumidor nuevo recibiría ese valor viejo como la verdad**, porque en un stream de
estado no hay historia de la que corregirlo. Por eso `put` compara versiones y, además, hace la
escritura **condicional** en el broker.

### 3.2 El algoritmo

```
0. Validar sin red y construir el evento UNA vez (§3.3): id, time, bytes. (§3.6)
1. Leer el último mensaje de la clave con DIRECT.GET por subject.
     · 404 → no hay valor: S = 0, y no hay versión V.
     · si no → S = su secuencia (cabecera Nats-Sequence); V = data.aggregateVersion del cuerpo.
1b. Si el id del mensaje leído es el id de ESTE put → ya se aplicó (acuse perdido): outcome
     "ok", revision S. No se publica nada más.
2. Si hay V y nueva.aggregateVersion <= V → no escribir. Resultado "stale". No es un error.
3. Publicar por JetStream con
     Nats-Expected-Last-Subject-Sequence: S      Nats-Msg-Id: <id del evento>
4. Acuse → resultado "ok", revision = la secuencia del acuse.
   Error 10071 ("wrong last sequence: N") o 10164 ("wrong last sequence") → alguien escribió
   entre el 1 y el 3: volver a 1. Como máximo 3 intentos; agotados, STATE_CONFLICT.
```

`delete` ([§4](#4-borrar)) es **el mismo algoritmo**: una lápida es un valor más, con su versión.

Por qué cada paso, con lo medido:

- **Paso 1: `S = 0` significa «la clave no existe».** `Nats-Expected-Last-Subject-Sequence: 0`
  crea la clave si el subject no tiene ningún mensaje, y falla si ya lo tiene (A-33). Es lo que
  hace que dos escritores concurrentes sobre una clave nueva no se pisen: uno gana y el otro
  recibe el error del paso 4.
- **Paso 1b: sin él, un acuse perdido convierte una escritura buena en un `stale`.** El primer
  intento se aplica, el acuse se pierde, el SDK reintenta con la misma `S`: el broker contesta
  `wrong last sequence` porque el mensaje que cambió la secuencia es **el propio** (A-33), el SDK
  relee, ve su propia versión, y concluiría `stale` con su propio dato. Reconocerlo por el `id`
  es posible porque el broker **conserva las cabeceras** del mensaje almacenado, `Nats-Msg-Id`
  incluida (A-32), y porque el evento lleva el mismo `id` en todos los intentos (§3.3).
- **Paso 3: el chequeo de secuencia se evalúa antes que el de duplicados.** Un reenvío con el
  mismo `Nats-Msg-Id` y la misma `S` **no** devuelve `duplicate: true`: devuelve el error de
  secuencia (A-33). Por eso `duplicate_window` no protege a `put`, y por eso existe el paso 1b.
- **Paso 4: dos códigos, no uno.** Un nodo suelto, o un cluster sin carrera, responde
  **10071** con el número (`wrong last sequence: 402`). Un stream replicado, cuando el conflicto
  se detecta al aplicar la entrada, responde **10164** y **sin número** (A-49: 210 de 210 errores
  de una carrera de 8 escritores sobre un cluster de 3 nodos). Un SDK que solo reconozca el 10071
  trata un conflicto corriente como un error desconocido y **no reintenta**: pasa sus pruebas
  contra un NATS de un nodo y falla en producción. Los dos son «alguien escribió antes»; el
  número, cuando viene, no hace falta: se relee.
- **Un rechazo por secuencia no gasta el `Nats-Msg-Id`.** Reintentar con el mismo `id` y la
  `S` corregida se aplica, no cae como duplicado (A-33).

### 3.3 Lo que el algoritmo exige a quien lo implementa

- **El evento se construye una sola vez.** `id`, `time` y los bytes son **los mismos en todos los
  intentos** de un `put`. Con un `id` nuevo por intento, el paso 1b no reconocería nunca la propia
  escritura.
- **Siempre por JetStream y siempre con la cabecera de secuencia.** Un `publish` de **core** a un
  subject de estado sí lo captura el stream, pero **sin acuse y sin chequeo**: sobrescribe sin
  condición (A-45). Es el atajo que anula §3.1.
- **`Nats-Msg-Id` = `id` del evento**, como siempre ([03-delivery.md §3](03-delivery.md)).
- **3 intentos, sin espera obligatoria.** El SDK **PUEDE** esperar un poco (con jitter) entre
  intentos. Medido con 3 instancias escribiendo la **misma** clave sin pausa (A-49): de 90 `put`,
  49 se aplicaron, 27 fueron `stale` y 14 agotaron los 3 intentos. Es una carga artificial
  —ninguna aplicación escribe una clave en un bucle desde tres sitios—, pero dice qué pasa si la
  carga existe: `STATE_CONFLICT` es **RETRYABLE** y se reintenta con el backoff de siempre.
- **El SDK PUEDE recordar en memoria** la última secuencia de cada clave que él mismo escribió,
  para ahorrarse el paso 1 en el primer intento. Si el publish falla, relee.
- **Un valor ilegible no bloquea la clave.** Si el mensaje leído en el paso 1 no es JSON, o no
  tiene un `aggregateVersion` válido, se trata como «sin versión»: se sobrescribe, con el
  chequeo de secuencia sobre **su** `S`. De lo contrario una entrada rota dejaría la clave
  inescribible para siempre, porque ningún servicio puede purgar el stream
  ([06-security.md §3.5](06-security.md)). *Decidido provisionalmente.*

### 3.4 La versión

`aggregateVersion` **lo pone el dueño**: es el contador monotónico de su agregado, el mismo que
va en los eventos ([03-delivery.md §5.1](03-delivery.md)). El SDK no lo inventa ni lo incrementa.

- **DEBE** ser un **entero ≥ 1** y **≤ 2^53 − 1** (9007199254740991: el mayor que Node, y todo
  lo que use `double`, representa sin perder precisión). Ausente, `null`, una cadena (`"7"`), un
  decimal (`7.5`, y `7.0` donde el lenguaje lo distinga: véase el punto siguiente), `0`, un
  negativo o uno mayor: `MISSING_AGGREGATE_VERSION` (PERMANENT). El nombre cubre todo lo que no
  sea un entero ≥ 1, no solo la ausencia. Se comprueba **siempre**, con o sin validación L3: es
  una regla del protocolo, no del esquema.
- **`7.0` depende del lenguaje, y la spec lo dice en vez de fingir que no.** El contador es un
  entero, y `7.0` no lo es *para quien lo distingue*: un SDK cuyo parser de JSON separa el entero
  del flotante (Rust con `serde_json`, donde `7.0` es un `f64`) **DEBE** rechazarlo con
  `MISSING_AGGREGATE_VERSION`. En un lenguaje de **número JSON único** —Node: `JSON.parse("7.0")`
  y `7` son el mismo `number`— es **indistinguible** de `7`, y el SDK no puede saber qué escribió
  quien lo llamó: ahí se acepta como `7`. Es un **límite conocido del lenguaje**, no un fallo del
  SDK, y no hay nada que corregir en él. Por eso `7.0` **no** figura en
  `conformance/cases/state-semantics.json` (ese fichero es JSON, y lo leen los dos): lo comprueba
  el test de cada SDK que sí distingue, y el de Node documenta que no puede.
- **Una versión que no avanza es `stale`, y una igual tampoco avanza**: dos escrituras con la
  misma versión son, por contrato, el mismo dato, y la segunda no se aplica. Corregir un valor
  exige subir la versión.
- **`stale` no es un error.** Es la respuesta normal de un dueño que reproduce un evento viejo o
  de dos instancias que compiten. Lo que sí puede ser un error es *ignorarlo*: un dueño que
  necesita su valor en el estado y recibe `stale` tiene un contador de versión que retrocede.
- **El contador sobrevive a los borrados** (§4): una clave borrada y vuelta a crear continúa
  desde la versión de su lápida, no desde 1.

### 3.5 Resultado

```ts
{ applied: boolean, outcome: "ok" | "stale", revision: number }
```

| | `applied` | `outcome` | `revision` |
|---|---|---|---|
| Escrito | `true` | `"ok"` | La secuencia del stream del mensaje nuevo |
| Versión que no avanza | `false` | `"stale"` | La del mensaje **actual**, el que la hizo obsoleta |

`applied` y `outcome` dicen lo mismo; el booleano es para `if`, la cadena para métricas y logs.
**No hay un tercer valor**: un conflicto agotado es un error (`STATE_CONFLICT`), no un resultado.
Un `stale` puede darse sobre una escritura que sí se aplicó y la superó otra más nueva mientras
se perdía su acuse (§3.2, 1b); el estado final es el mismo.

### 3.6 Validación, clasificación y tenant

En este orden, **antes de abrir ninguna petición** (el primero que falla gana):

| Comprobación | Código | Clase |
|---|---|---|
| Dominio reservado ([§2.7](#27-dominios-reservados)) | `RESERVED_DOMAIN` | PERMANENT |
| Forma del contrato ([§2.2](#22-el-subject)) | `INVALID_STATE_CONTRACT` | PERMANENT |
| Clave ([§2.3](#23-la-clave)) | `INVALID_STATE_KEY` | PERMANENT |
| Tenant ([§2.4](#24-el-tenant)) | `INVALID_TENANT_FOR_STATE` | PERMANENT |
| `data` objeto; `aggregateVersion` ([§3.4](#34-la-versión)) | `MISSING_AGGREGATE_VERSION` | PERMANENT |
| `classification` ([§7](#7-datos-personales-y-supresión)) | `INVALID_DATACLASSIFICATION` | PERMANENT |
| **L3**: `data` contra el `dataschema` del contrato | `INVALID_SCHEMA` | PERMANENT |

- **L3 en `put`** valida `data` contra el esquema más alto del mayor del contrato, que resuelve el
  índice `state` del bundle ([§10](#10-qué-debe-un-sdk-state)); como en
  [00-protocol.md §5](00-protocol.md), no se resuelve por red. Falla **antes** de leer ni escribir
  y reporta **todos** los errores. **La lápida** valida contra el esquema fijo
  `https://schemas.internal/system/state/eliminado/1.0.0.json`.
- **Un fallo de validación nunca copia valores a su mensaje.** Un error como `'ana@example.com'
  no cumple el formato` acabaría en logs, y en `dlqerror` 90 días: el mensaje cita la **ruta** del
  campo (`/email`), no su contenido (§7).
- **Clasificación.** Sin `classification` explícita, un `put` usa **`confidential`**, no el valor
  por defecto de la conexión: un estado es donde se permite que viva el dato personal, y
  equivocarse hacia arriba cuesta un log redactado, mientras que equivocarse hacia abajo cuesta
  PII en un log o en una DLQ. La lápida lleva siempre `internal`. *Decidido provisionalmente.*

### 3.7 Lecturas de réplica — **no medido**

Con `replicas: 3`, `DIRECT.GET` lo puede contestar **cualquier réplica**, incluida una que aún no
ha aplicado la última escritura. Medido (A-49) en un cluster de 3 nodos locales: 400 `put`
seguidos de su lectura inmediata, 0 lecturas obsoletas. **Eso no demuestra que no ocurra**: sobre
una red real, con una réplica retrasada, `get` justo después de un `put` **PUEDE** devolver el
valor anterior. Para `put` es inocuo —el chequeo de secuencia del paso 3 rechaza una `S`
vieja— y se traduce en un intento más, o en un `STATE_CONFLICT` si la réplica sigue retrasada en
los tres (tampoco medido). Para `get` significa que **no hay lectura de
tu propia escritura garantizada**: quien la necesite la deriva del resultado del `put`
(`revision`), no relee. No se ha podido medir con latencia de red real.

## 4. Borrar

```ts
await bus.state.delete("clientes.cliente.v1", "cli-1", { aggregateVersion: 8 });
// → { applied: true, outcome: "ok", revision: 42 }
```

`delete` publica una **lápida** en el mismo subject: un CloudEvent con

- `type: com.flux.state.<dominio>.<agregado>.deleted.v<major>`;
- `data: { "aggregateVersion": n }` — y nada más;
- `dataclassification: "internal"`;
- `dataschema: https://schemas.internal/system/state/eliminado/1.0.0.json`, fijo.

Al entrar, **el valor anterior sale del stream** (A-34): sigue habiendo un solo mensaje por clave,
y el dato personal ya no está en el stream.

- **`delete` es `put` de una lápida.** Mismo algoritmo (§3.2), mismas reglas de versión, mismo
  resultado. `aggregateVersion` es obligatorio y es la versión **de la lápida**: debe superar la
  del valor que borra. Una lápida con versión ≤ la actual es `stale`.
- **Borrar una clave que nunca existió escribe la lápida** (`S = 0`, `applied: true`). Una lápida
  sobre una clave vacía es inocua, y la alternativa —un `delete` que a veces no hace nada— haría
  que el dueño tuviera que saber si algún observador llegó a ver el valor. *Decidido
  provisionalmente.*
- **La lápida lleva versión** (`data.aggregateVersion`) **porque `put` la lee** (§3.2, paso 1): para
  decidir si una escritura nueva es `stale` necesita la versión de lo que hay, y lo que hay puede
  ser una lápida.
- **La lápida es permanente en 1.1.** Compactarlas queda fuera de alcance. Una pesa unos
  cientos de bytes, y un observador que entra tarde las recibe en su foto inicial (A-35).
- **Se puede recrear una clave borrada, con una versión estrictamente mayor que la de la
  lápida.** `Nats-Expected-Last-Subject-Sequence` funciona contra la secuencia de la lápida, y
  con `0` falla porque el subject ya tiene mensaje (A-34): es el paso 1 de siempre, que lee la
  lápida como cualquier otro valor. Un `put` con versión ≤ la de la lápida es `stale`. *Decidido
  provisionalmente.*
  **La trampa:** un dueño que borra la fila de su base de datos y, más tarde, la recrea con
  `aggregateVersion: 1`, recibe `stale` y **la clave sigue borrada** sin ningún error. Su
  contador tiene que sobrevivir al borrado —una columna `deleted_at` en lugar de un `DELETE`, o
  un contador aparte— o no debería reutilizar la clave.
- Un consumidor **DEBE** borrar o anonimizar su copia local al recibir una lápida (§5.3). Es lo
  que hace real el derecho de supresión **aguas abajo**.
- `get` de una clave con lápida devuelve **`null`**, igual que de una que nunca existió (§5.1).
  `flux state get` (el CLI), en cambio, **muestra la lápida**: es una herramienta de diagnóstico, y
  su versión y su `revision` son justo lo que explica por qué un `put` da `stale`. La diferencia es
  **intencionada**: el SDK contesta «¿hay un valor?» y el CLI «¿qué hay en esta clave?».

## 5. Leer

### 5.1 `get`

```ts
const actual = await bus.state.get("clientes.cliente.v1", "cli-1");   // entrada | null
```

Una lectura puntual por `DIRECT.GET` sobre el subject exacto (A-32):

```
$JS.API.DIRECT.GET.STATE_<DOM>.state.<dom>.<agr>.v<M>.<tenant>.<clave>
```

La respuesta lleva el mensaje tal cual —cuerpo y cabeceras `Nats-Subject`, `Nats-Sequence`,
`Nats-Time-Stamp`, más las que se publicaron— y **no** consume nada: no hay consumidor ni acuse.

- **Clave inexistente → `404 Message Not Found`** (A-32), y el SDK devuelve `null`. Una **lápida**
  también devuelve `null` (el CLI la muestra: §4).
- **Un stream que no existe, o sin `allow_direct`, no devuelve 404: no contesta nadie**
  (`no responders`), y los dos casos se ven igual (A-32). El SDK lanza `NOT_PROVISIONED` con un
  mensaje que nombra las dos causas y apunta a `flux provision` y a `flux doctor`
  ([04-errors.md §6](04-errors.md)).
- Aplica las comprobaciones de §5.4: un mensaje cuyo `tenantid` o `subject` no coincide con el
  subject lanza `STATE_SUBJECT_MISMATCH` en vez de devolverlo.
- **Es una lectura de réplica** (§3.7).

**Un cuerpo no sirve para saltarse el permiso.** `DIRECT.GET` por subject **rechaza** una
petición con cuerpo (`408 Bad Request`, A-38), así que la forma `$JS.API.DIRECT.GET.<stream>` +
`{"seq": n}` —que sí lee cualquier mensaje— no puede colarse por un permiso de subject.

### 5.2 `watch`

```ts
await bus.state.watch("clientes.cliente.v1", async (entrada, ctx) => {
  if (entrada.deleted) return borrarLocal(entrada.tenantId, entrada.key);
  await guardarLocal(entrada);          // UPDATE … WHERE aggregate_version < $n
}, { onSynced: () => listo(), tenantId: "acme" });
```

`watch` entrega **una foto completa y después los cambios, por el mismo canal**: un consumidor
durable de `STATE_<DOM>`.

- **Durable:** `<servicio>__state_<dom>_<agr>_v<M>` y, si filtra por tenant, el sufijo
  `__<tenant>`. Los puntos y los guiones **del contrato y del tenant** pasan a `_`, como en
  [02-naming.md §4](02-naming.md): `tienda-web__state_clientes_cliente_v1__acme`. **El nombre del
  servicio no se toca**: conserva sus guiones (`tienda-web`, no `tienda_web`), porque es lo que
  `^[a-z0-9]+(-[a-z0-9]+)*$` y la ACL de `gen-acl.mjs` esperan delante del `__`. El tenant va en
  el nombre porque **un durable compartido reparte los mensajes entre quienes lo usan**
  (A-47): dos instancias del mismo servicio, una por tenant, con un durable sin sufijo, se
  quitarían la mitad de la foto cada una.
- **`deliver_policy: last_per_subject`**; todo lo demás, la configuración canónica de
  [03-delivery.md §2](03-delivery.md) (`ack_wait == backoff[0]`, `max_deliver: 6`, …).
- **Filtro:** `state.<dom>.<agr>.v<M>.<tenant>.*`, o `….*.*` sin tenant. Con
  `tenantIsolation: "strict"`, el segundo es un error de configuración
  ([09-multitenancy.md §3](09-multitenancy.md)). **El tenant de un `watch` sale de la opción de la
  llamada y, si no, de `connect()`** —y **nunca del evento en curso**: §2.4 explica por qué—. Con
  tenant en `connect()` **siempre** filtra, sea cual sea su valor: aquí `system` es un tenant como
  cualquier otro (*decidido provisionalmente*).
- **Un `watch` observa un tenant, o todos; no una lista.** La API de los SDKs toma **un**
  `tenantId` por llamada, y cada llamada es **un** durable. Un servicio que observa N tenants
  llama N veces, y cada llamada tiene su `onSynced` ([§5.5](#55-onsynced)).
- **Un consumidor que estuvo caído recibe solo el último valor de cada clave que cambió**, no los
  intermedios: no los hay (A-35). Un valor **reemplazado mientras estaba en vuelo no se
  reentrega**: si el handler hace `nak` de la revisión 10 y mientras tanto entra la 20, la 10 ha
  salido del stream y el broker no la devuelve (A-36). Nunca se aplica un valor que ya no existe.
- El `watch` **no necesita** `STREAM.INFO.STATE_<DOM>`: `CONSUMER.INFO` distingue «el consumidor
  no existe» (10014) de «el stream no existe» (10059) (A-48), que es todo lo que hace falta
  para dar `NOT_PROVISIONED` o `PERMISSION_DENIED` según [04-errors.md §5](04-errors.md).
- Como en todo consumo, un `ack` o un pull denegados no lanzan: rige
  [06-security.md §3.7](06-security.md).

**Resincronizar** es borrar el durable y volver a crearlo: el broker entrega otra vez el último
valor de cada clave (A-37). Lo hace `flux state resync` con credenciales de **operación**: un
servicio no puede borrar durables ([06-security.md §3.5](06-security.md)). **Un `resync` de un
durable que no existe se rechaza** (lo decidió el CLI): no hay nada que resincronizar, y crearlo
dejaría un durable fantasma por un nombre mal escrito —un `--tenant` que falta, un servicio
equivocado—; el CLI dice cuáles existen.

**Un `watch` vivo durante un `resync`.** El `resync` borra el durable **bajo un `watch` que sigue abierto**:
el broker contesta al pull pendiente `409 Consumer Deleted`. El SDK **DEBE** tratarlo como «repite el
pull» —no como un fallo, y menos como el final del `watch`—: vuelve a consultar el durable **por su
nombre** (`CONSUMER.INFO`, que el observador ya tiene) con backoff hasta que exista, **comprueba su
configuración como al abrir** (un durable recreado con otro filtro **no** se consume: entregaría las
claves de otro tenant sin ningún error) y vuelve a pedir mensajes. **No necesita `CONSUMER.CREATE`**:
quien recrea el durable es `flux state resync`, con credenciales de operación. Avisa por el logger al
perder el durable y al recuperarlo, y `close()` lo detiene. `onSynced` **no** se invoca de nuevo
(§5.5): es una vez por `watch`.

Medido (nats-server 2.14.5; 5 claves, un `watch` abierto y un `resync` inmediato —borrar y recrear—;
tiempo desde que se recrea el durable hasta la primera entrada de la foto nueva):

| Cliente | Sin tratar `consumer deleted` | Reabriendo el consumo |
|---|---|---|
| `nats.js` 3.4.0 (Node) | **43,0 s** (tres veces) | 0,1 s (tres veces) |
| `async-nats` 0.50 (Rust) | **nunca**: 120 s después el `watch` seguía sin recibir nada, ni un `put` en vivo | 0,2–0,3 s (tres veces) |

Cada cliente falla de forma distinta, y la de Rust es la peor. `nats.js` notifica `consumer_deleted` pero no
repite el pull hasta que detecta la falta de latidos (`heartbeats_missed`: dos de 15 s y el reintento): ~43 s
de un `watch` mudo, que luego se recupera solo. `async-nats` da por **terminado** el flujo de mensajes
(`Err(ConsumerDeleted)` y después `None`), y un bucle que trata el fin del flujo como «me pidieron parar»
sale **en silencio**: un `watch` muerto con la conexión sana y sin ningún error. Un cliente en el que no se
pueda reabrir así **DEBE** documentar su tiempo medido, no suponerlo.

### 5.3 La entrada

Lo que recibe el handler, y lo que devuelve `get`:

```ts
{
  key:              string,            // la clave
  tenantId:         string,            // el tenant del subject
  deleted:          boolean,           // true si es una lápida
  data:             object | null,     // el `data` del evento; null si deleted
  aggregateVersion: number,            // la del valor (o la de la lápida)
  revision:         number,            // la secuencia del stream
}
```

- **`tenantId` no estaba en el plan y es necesario.** Un `watch` sin tenant recibe entradas de
  todos; si la entrada solo trajera la clave, un handler que guarda por clave mezclaría los
  tenants en silencio: es el choque que el tenant en el subject evita en el broker (§2.2) y que
  no puede reaparecer en la réplica local. *Desviación del plan, §12.*
- **`data` es `null` (`None`, `nil`) cuando `deleted`**: la lápida no lleva dato (§4). `null`
  como valor de una entrada de la API es otra cosa que `null` dentro de `data` en el cable
  ([01-envelope.md §4](01-envelope.md)).
- **`revision` es la secuencia del stream**: crece con cada escritura de cualquier clave, y cambia
  cuando la clave cambia. Sirve para saber *qué mensaje* es; **no** para ordenar versiones de una
  clave —eso es `aggregateVersion`—, ni se compara entre contratos.
- **El SDK confirma (`ack`) cuando el handler termina sin error**, como con los eventos
  ([03-delivery.md §2](03-delivery.md)); lanzar clasifica según [04-errors.md](04-errors.md).
- **El handler DEBE ser idempotente y no asumir orden**, como siempre: la entrada de una clave
  puede llegar mientras se procesa la anterior (la anterior ya salió del stream, pero ya se había
  entregado). La defensa es la de [03-delivery.md §5.1](03-delivery.md): `UPDATE … WHERE
  aggregate_version < $n`, que aquí es la regla de aplicar la entrada, no una optimización.
- **Ante una lápida, borrar o anonimizar** la copia local y **recordar la versión** de la lápida
  (o no recrear la fila con una versión menor): una escritura vieja que llegue después no debe
  resucitar el dato. Una lápida de una clave que el observador nunca tuvo es un no-op.

### 5.4 Qué comprueba el SDK al consumir

Antes de llamar al handler, en este orden. Lo que falla **nunca** se aplica y **nunca** llega al
handler:

| Comprobación | Código | Clase |
|---|---|---|
| El mensaje es un CloudEvent válido con las cuatro extensiones ([01-envelope.md §3.1](01-envelope.md)) | los de siempre | POISON |
| `tenantid` = el tenant del subject; `subject` = la clave; `type` = el del contrato (o su lápida) | `STATE_SUBJECT_MISMATCH` | **POISON** |
| `dataclassification` ≠ `restricted` ([§7](#7-datos-personales-y-supresión)) | `INVALID_DATACLASSIFICATION` | **POISON** |
| `data` con `aggregateVersion` válido ([§3.4](#34-la-versión)) | `MISSING_AGGREGATE_VERSION` | PERMANENT |
| Si hay L3 al consumir: `data` contra su esquema | `INVALID_SCHEMA` | PERMANENT |

`STATE_SUBJECT_MISMATCH` es POISON: el mensaje es legible pero **miente sobre su identidad**, y un
productor que lo hace está roto o es hostil. `watch` lo trata como cualquier POISON
([04-errors.md §1.3](04-errors.md)): **`term()` + DLQ de estado + alerta**, métrica
`outcome="poison"`, y la entrada **no** se aplica, de modo que la clave conserva su valor
anterior. La clave que se registra en la DLQ es la **del subject** —la que decidió el broker—,
no la del sobre.

### 5.5 `onSynced`

```ts
{ onSynced: () => listo() }
```

Se invoca **una sola vez por `watch()`** —no una por reconexión, ni de nuevo tras un `resync`
(§5.2)— cuando el handler ya ha procesado **todo lo que había pendiente al abrirlo**. Es la señal
de «mi réplica local ya puede contestar lecturas». Concretamente:

1. Al abrir, el SDK pregunta por el consumidor (`CONSUMER.INFO`): `num_pending` y
   `num_ack_pending`.
2. **Stream vacío (o durable al día): `onSynced` se invoca de inmediato**, antes de entregar nada
   y aunque no llegue ninguna entrada después (A-35: un consumidor sobre un stream vacío informa
   `num_pending: 0` y no entrega nada). Es lo único correcto: esperar a una entrada que quizá no
   llegue nunca dejaría al servicio «sincronizando» para siempre.
3. Si no, cuando una entrega llega con **`pending == 0`** —el número que el broker pone en los
   metadatos de cada mensaje: cuántos quedan por entregar (A-35)— **y** todos los handlers
   lanzados hasta ese momento han terminado (con éxito o con error). `num_pending` solo no basta:
   cuenta lo no entregado y no lo entregado-y-sin-terminar, y con handlers concurrentes llega a 0
   cuando aún quedan hasta 256 en vuelo.

**Varios tenants.** El contrato es: si una sola llamada observa **N tenants** (N durables), `onSynced`
se invoca **una vez**, cuando **los N** están sincronizados —con uno solo sincronizado, la réplica
local aún no puede contestar por los otros—, y de inmediato solo si los N estaban al día. **La API
actual de Node y de Rust no ofrece esa llamada**: `watch` toma **un** `tenantId` (o ninguno, y observa
todos con un único durable), y un servicio que observa N tenants hace N llamadas, cada una con su
`onSynced`. Combinarlas —«listo cuando han llegado las N»— es de la aplicación, que es quien sabe
cuántos tenants tiene. No se ha inventado una API de lista para tapar esto; si un SDK la añade,
**DEBE** cumplir la regla de arriba.

**Escrituras durante la sincronización.** La foto no es una fotografía parada: el broker entrega
en orden de secuencia, y una clave reescrita mientras se sincroniza aparece **una vez, con su
valor más reciente** (A-35: la clave reescrita llega con la secuencia nueva; una clave nueva se
entrega al final). `onSynced` espera a que se alcance `pending == 0`; si el dueño escribe más
deprisa de lo que el handler consume, se retrasa, y eso es lo correcto. No promete que **no**
lleguen entradas después: promete que **ya se vio** lo que había al abrir. Una entrada que falla
con un error RETRYABLE **no** retrasa `onSynced`: se cuenta como terminada; su fallo va por el
camino de siempre, y esa clave puede faltar en la réplica local hasta que se reintente.

### 5.6 Orden

Igual que en los eventos ([03-delivery.md §5](03-delivery.md)): ni global ni por clave. Lo que
cambia es que **el estado tiene más defensas que un evento**: un valor reemplazado no se
reentrega (§5.2), y un consumidor nuevo parte de la foto, no de la historia. Lo que queda —dos
entradas de la misma clave en vuelo a la vez— lo resuelve `WHERE aggregate_version < $n`.

## 6. Errores y DLQ

Misma taxonomía que los eventos ([04-errors.md](04-errors.md)), con los códigos propios de
§3.6 y §5.4 y los de [04-errors.md §6](04-errors.md). Un `watch` cuyo handler lanza un
PERMANENT, o agota sus reintentos, manda la entrada a la **DLQ de estado**.

- **Subject:** `dlq.state.<dominio>.<agregado>.v<major>` — **sin tenant ni clave**, para que no
  se multiplique por ellos: son cinco tokens, uno por contrato. El stream `DLQ_<DOM>` captura
  `dlq.<dom>.>` **y** `dlq.state.<dom>.>` **cuando el dominio tiene un `STATE_<DOM>`**; un dominio
  solo de eventos conserva el `DLQ_<DOM>` de la v1.0 sin tocarlo: ampliar los subjects de un
  stream con datos es posible y no pierde mensajes (A-44), pero no se le pide a quien no usa
  estado. *Decidido provisionalmente.* `flux provision` lo hace; `flux doctor` lo audita.
- **El evento de DLQ de estado NO lleva el dato.** `data` se sustituye por
  `{ "key": "…", "revision": n }` (§2.6). La DLQ guarda 90 días
  ([02-naming.md §3.3](02-naming.md)); un dato personal que el dueño ya ha borrado seguiría vivo
  ahí tres meses y **el derecho de supresión no se cumpliría**. El resto del sobre queda como
  estaba, también `dataclassification`.
- **`dlqerror` NO DEBE contener valores de `data`**: código, y como mucho la ruta del campo
  (`INVALID_SCHEMA: /pais …`, nunca el valor). Es la misma razón, vista desde el texto. Y mide
  **1024 bytes UTF-8 como máximo**, cortado sin partir un carácter ([04-errors.md §3](04-errors.md)).
- **Un error que no es de flux no deja su mensaje en la DLQ.** Lo que lanza un driver de base de
  datos (`duplicate key value violates unique constraint … Key (email)=(ana@example.com)`) cita la
  fila entera, y la DLQ guarda 90 días. Un error **de flux** —los del SDK y los que lanza la
  aplicación con `PermanentError`/`RetryableError`, cuyo mensaje escribió quien sabía que iría a la
  DLQ— conserva el suyo. Cualquier otro se reduce a su código y a una frase fija:
  `UNKNOWN: error sin clasificar de la aplicación; el mensaje se omite para no copiar datos a la DLQ
  (10-state.md §6)`, con el código que le dé el clasificador (`UNKNOWN`, o el que reconozca:
  `HTTP_503`, `ECONNRESET`). Tampoco se concatena la cadena de causas de un error de flux: la causa
  es, justamente, el error del driver.
- **Si el original llevaba `signkeyid` y `signature`, el evento de DLQ de estado NO los
  conserva**: firman un `data` que ya no está, y una firma que no verifica sobre un sobre que
  parece el original es una afirmación falsa ([07-signing.md §5](07-signing.md)). *Decidido
  provisionalmente.*
- **Un mensaje ilegible** (JSON roto, sin sobre) **no se copia a la DLQ**: no se guarda su
  cuerpo en ninguna forma —ni los `rawBase64` que sí llevan los eventos—, porque podría contener
  el dato. No hay sobre original que conservar, así que el SDK sintetiza uno con lo único fiable,
  **el subject**: `id` UUIDv7 nuevo (y `correlationid` igual), `source` y `producerversion` del
  servicio que consume, `time` = **el instante actual** (el del consumo: no hay otro que decir),
  `type` y `dataschema` del contrato, `subject` = la clave y `tenantid` = el tenant, **ambos
  tomados del subject**, `partitionkey` = **la clave**, `dataclassification: "internal"`, las `dlq*`
  (`dlqreason: "poison"`) y `data: { key, revision }`. Lo único que **no** es reproducible byte a
  byte entre dos consumos es lo que depende del reloj y del azar: `id`, `correlationid` y `time`.
- **`Nats-Msg-Id` de la DLQ de estado:** `dlq-<id>-<dlqconsumer>`, como en los eventos.

**El estado no se reproduce desde la DLQ.** Republicar un valor viejo pisaría uno más nuevo —la
DLQ guarda el *fallo*, no el estado—, y ahora ni siquiera tiene el dato. `flux dlq replay`
**DEBE** rechazar `dlq.state.>`, con un mensaje que apunte a la recuperación: **`flux state
resync`** ([§5.2](#52-watch)), que entrega de nuevo el último valor de cada clave por el camino
normal. Es mejor que un replay: aplica lo **actual**, no lo que había cuando falló.

Un dueño lee la DLQ de estado de su dominio como la de eventos
([06-security.md §3.1](06-security.md)): con un pull consumer —durable
`<servicio>__dlq_state_<dominio>`, filtro `dlq.state.<dominio>.>`—. *Decidido provisionalmente.*

## 7. Datos personales y supresión

Aquí se resuelve la contradicción de [06-security.md §5.1](06-security.md): la spec manda no meter
datos personales en eventos, y «pedirlos al dueño» devuelve las llamadas que flux quiere
eliminar.

| | Evento | Estado |
|---|---|---|
| ¿Datos personales? | **No.** Solo referencias (`clienteId`) | **Sí**: `dataclassification: "confidential"`, y los campos con `"x-pii": true` en el esquema |
| ¿Se pueden borrar? | No: es un log inmutable | **Sí**: `delete` sustituye el valor por una lápida (§4) |

El evento dice **qué pasó**; el estado lleva **el dato**. El consumidor ya no llama al dueño:
tiene el dato porque le llegó.

- **`restricted` (credenciales, salud, financiero) no se admite en estado en 1.1.** Un `put` con
  `classification: "restricted"` lanza `INVALID_DATACLASSIFICATION`; un mensaje de estado que lo
  lleve es POISON. Meter un dato de 24 h de retención en un almacén sin caducidad no es una
  decisión que deba tomarse por omisión.
- **Los campos con PII DEBEN llevar `"x-pii": true`** en el esquema del contrato, y un SDK
  **DEBERÍA** redactarlos en logs ([06-security.md §5](06-security.md)). Las claves y los tenants
  **no** son PII: son identificadores opacos.
- **Un consumidor DEBE honrar la lápida** (§5.3). El derecho de supresión se cumple en el origen
  con `delete`, y se propaga a cada réplica local por la lápida. Un observador que ignora las
  lápidas guarda datos de personas que el dueño ya borró.
- **El estado reemplazado o borrado no conserva historia**, y la DLQ de estado tampoco (§6). Con
  eso el estado satisface la supresión en el broker. **Lo que NO garantiza**:
  - **Borrado físico en disco.** Que el valor «salga del stream» no es que sus bytes desaparezcan
    del fichero: depende de la compactación de JetStream. Mitigación: cifrado en reposo del
    broker; garantía fuerte, cifrado del payload (fase 7).
  - **Copias fuera del broker**: backups del stream, y las réplicas locales de observadores que
    no honren la lápida.
  - **Las claves y los tenants**: son visibles en los metadatos del stream (§8). Por eso no son
    PII.

## 8. Permisos

Los permisos de estado los **genera** `gen-acl.mjs` (clave `watches` del manifiesto) y se
documentan junto a los demás en [06-security.md §3.8](06-security.md#38-estado-replicado-watches-y-sus-permisos)
(el formato de `watches`, lo que recibe cada rol y los casos S15 a S20 que lo ejecutan); esto es lo que el estado
necesita, **medido** contra un broker con una ACL escrita a mano (A-38 a A-40), y lo que el
generador debe producir. `protocol.json` → `state.acl` fija los patrones.

**Dueño** del dominio (escribe, y lee lo suyo):

```
state.<dom>.>
$JS.API.DIRECT.GET.STATE_<DOM>.state.<dom>.>
+ el durable de su DLQ de estado: CONSUMER.INFO, MSG.NEXT y ACK de  <servicio>__dlq_state_<dom>
  (sobre DLQ_<DOM>: son los patrones de `security.acl.permissionPatterns`, no los de `STATE_<DOM>`)
```

**Observador** (`watch`, y `get` de lo que observa), por contrato y tenant:

```
$JS.API.CONSUMER.INFO.STATE_<DOM>.<durable>
$JS.API.CONSUMER.MSG.NEXT.STATE_<DOM>.<durable>
$JS.ACK.STATE_<DOM>.<durable>.>
$JS.API.DIRECT.GET.STATE_<DOM>.state.<dom>.<agr>.v<M>.<tenant>.*        (…v<M>.*.* sin tenant)
dlq.state.<dom>.<agr>.v<M>
$JS.API.STREAM.INFO.DLQ_<DOM>                                           (el SDK comprueba la DLQ al abrir)
```

y, solo con `--dev`, `CONSUMER.CREATE.STATE_<DOM>.<durable>.state.<dom>.<agr>.v<M>.<tenant>.*`.

Lo que se midió, y por qué la lista es esta:

- **`DIRECT.GET` por subject se confina por tenant** (A-38). Con el permiso de `…acme.*`: se lee
  una clave de `acme`; se deniega una de `globex`, la forma `v1.*.*` y la forma `state.<dom>.>`.
  Un comodín **en la clave** sí pasa (`…acme.*` devuelve el último mensaje escrito de `acme`): es
  su tenant, lo puede leer todo.
- **La forma de cuerpo no se concede nunca.** `$JS.API.DIRECT.GET.<stream>` con `{"seq": n}` o
  `{"last_by_subj": …}` lee cualquier mensaje y ninguna ACL ve el cuerpo; no coincide con el
  permiso por subject (A-38), y [06-security.md §3.5](06-security.md) la sigue negando. La
  excepción que esta sección introduce es **solo** la forma por subject de un estado, con el
  tenant fijado.
- **El filtro de un consumidor no se puede ampliar por el cuerpo** (A-40): con el subject
  `CREATE.<S>.<d>.<filtro>`, un `filter_subject` distinto da `Consumer create request did not
  match filtered subject from create subject` (10131), un `filter_subjects` da 10137, y
  tampoco hereda el del subject si el cuerpo no trae ninguno.
- **`STREAM.INFO.STATE_<DOM>` no se concede a observadores, y el SDK no lo necesita.**
  `STREAM.INFO` con `subjects_filter` en el cuerpo devuelve **todos los subjects del stream con
  su recuento**, es decir, **todas las claves y todos los tenants** (A-39). Con ese único permiso,
  un servicio confinado a `acme` conoce los clientes de `globex`. Sin él, `put`, `get` y `watch`
  funcionan igual (A-38, A-48). **El dueño tampoco lo recibe**: el SDK no lo usa —las claves y los tenants que
  necesita los conoce porque es quien los escribe, y `subjects_filter` es justo lo que lista todos—, y
  conceder «por si acaso» lo que nadie usa es el principio de un permiso que alguien acabará ampliando. Si
  un día hiciera falta, sería un permiso más del dueño, nunca del observador, y entraría aquí primero
  (06-security.md §3.8).
  Lo que **sí** queda visible para un observador es lo que expone `STREAM.INFO.DLQ_<DOM>`:
  los subjects de DLQ —cinco tokens, sin clave ni tenant— y sus recuentos.

> **Esto matiza [06-security.md §3.5](06-security.md)**, que niega a un servicio
> `$JS.API.DIRECT.GET.*`. La forma por subject de **un contrato y un tenant concretos** —lo que
> mide A-38— no abre la lectura de otros mensajes; la forma de cuerpo y cualquier comodín por
> encima del tenant siguen denegados.

## 9. Observabilidad

[08-observability.md §2.4](08-observability.md) define `flux_state_writes_total{contract,outcome}`
para los SDKs con la capacidad `state`. Las métricas de consumo existentes se **reutilizan** con
`subject = "state.<contrato>"` (`state.clientes.cliente.v1`) y `consumer` = el durable **sin el
sufijo de tenant**: con el sufijo, cada tenant nuevo crearía series nuevas, que es lo que
[08-observability.md §2.2](08-observability.md) prohíbe. Un `watch` con un durable por tenant
**DEBE** publicar `flux_consumer_pending` como la **suma** de los suyos. **Nunca** se etiqueta con
la clave ni con el tenant.

## 10. Qué DEBE un SDK `state`

**Los esquemas de estado** viven en `schemas/state/<dominio>/<agregado>/<semver>.json` (un fichero
por versión, inmutables, con el `$id` igual a su ruta: [05-compatibility.md §4](05-compatibility.md)),
y valen las mismas reglas de compatibilidad. Dos más, que `bundle-schemas.mjs` y
`check-compat.mjs` comprueban: `data` es un **objeto** en la raíz y declara **`aggregateVersion`**
como entero ≥ 1 y `required` (§3.4). La lápida es el esquema fijo
`schemas/system/state/eliminado/1.0.0.json`. El bundle gana un índice **`state`**: contrato
(`clientes.cliente.v1`) → URI del esquema de versión más alta de su mayor, igual que `subjects`
hace con los eventos. La lápida **no** está en el índice —su URI es constante—, pero sí en
`schemas`.

Un SDK declara la capacidad `state` ([00-protocol.md §5](00-protocol.md)) solo si cumple **todo**:

1. `put`, `delete`, `get` y `watch` según §3 a §5, y los **mismos bytes** que §2.6 y §6 para el
   sobre, la lápida, el subject y la DLQ de estado (arnés cruzado: `state-build`,
   `state-tombstone`, `state-subject`, `state-dlq`).
2. Reconoce **10071 y 10164** como conflicto de secuencia (§3.2) —se prueba contra un cluster, o
   con una respuesta fabricada de cada forma—.
3. Los **siete** códigos de [04-errors.md §6](04-errors.md), con su clase.
4. `publish()` rechaza los dominios reservados (§2.7) con `RESERVED_DOMAIN`.
5. Resuelve el contrato en el índice `state` del bundle, no por red.
6. Ejecuta, contra un broker real y con los permisos que genera `gen-acl.mjs`, el ciclo del
   criterio de aceptación: el dueño escribe 100 claves, un observador **nuevo** las recibe todas
   y recibe `onSynced`, se borra una, el observador recibe la lápida y `get` de esa clave da
   `null`. En los dos sentidos: quitar un permiso **se nota**.

## 11. Lo que el estado NO resuelve

Declarado para que nadie asuma de más:

- **No es consistente entre claves.** No hay transacciones: dos claves se actualizan por
  separado, y un observador puede ver una de las dos cambiada.
- **No es una consulta.** Solo se lee por clave exacta, o todo el contrato.
- **No hay lectura de la propia escritura garantizada con réplicas** (§3.7).
- **No resiste a un dueño que escribe versiones mal.** El broker no sabe qué es una versión
  correcta: aplica la regla de §3.4 al contador que le dan.
- **No oculta los nombres de claves ni de tenants** a quien tenga `STREAM.INFO` (§8).
- **No garantiza el borrado físico** (§7).

## 12. Decidido provisionalmente

Puntos que el plan dejaba abiertos y que se cierran aquí con el criterio que se explica en cada
sección. Son del dueño del proyecto si prefiere otra cosa; cambiar cualquiera de ellos más tarde
es barato **ahora** y deja de serlo cuando haya datos en un stream:

| # | Decisión | Sección |
|---|---|---|
| 1 | Una clave no puede empezar por `_` (reservado al protocolo) | §2.3 |
| 2 | Tenant de estado de **64** caracteres como máximo | §2.4 |
| 3 | Sin tenant por defecto en estado: ni `system` implícito ni el del evento ausente | §2.4 |
| 4 | `put` sin `classification` usa `confidential`; `restricted` se rechaza | §3.6, §7 |
| 5 | Un valor ilegible no bloquea la clave: se sobrescribe | §3.3 |
| 6 | `delete` de una clave que nunca existió escribe la lápida | §4 |
| 7 | La entrada lleva **`tenantId`** (no estaba en el plan) | §5.3 |
| 8 | `STATE_SUBJECT_MISMATCH` también cubre un `type` que no es el del contrato | §5.4 |
| 9 | Con tenant en `connect()`, `watch` filtra siempre, también con `system` | §5.2 |
| 10 | `onSynced`: `pending == 0` **y** handlers terminados; vacío, inmediato | §5.5 |
| 11 | El stream `DLQ_<DOM>` captura `dlq.state.<dom>.>` solo si el dominio tiene estado | §6 |
| 12 | El dueño lee la DLQ de estado con `<servicio>__dlq_state_<dominio>` | §6 |
| 13 | La DLQ de estado no conserva la firma, ni el cuerpo de un mensaje ilegible | §6 |
| 14 | `STREAM.INFO.STATE_<DOM>` no se concede a nadie —ni al dueño—; el SDK no lo usa | §8 |
| 15 | El `consumer` de las métricas de estado es el durable sin tenant, y el gauge la suma | §9 |
| 16 | `type` de la lápida con `deleted` (del plan) frente a `eliminado` en su esquema | §4 |
| 17 | El tenant de un `watch` sale de la opción y de `connect()`, nunca del evento en curso | §2.4, §5.2 |
| 18 | Un contrato mal formado es `INVALID_STATE_CONTRACT` (el séptimo código) | §2.2, §3.6 |
| 19 | `7.0` se rechaza donde el lenguaje distingue entero de flotante; en Node es indistinguible de `7` | §3.4 |
| 20 | Un error que no es de flux deja en `dlqerror` solo su código y una frase fija | §6 |
| 21 | `onSynced` de varios tenants: una vez, cuando todos están sincronizados; la API actual toma un tenant por `watch` | §5.5 |
| 22 | Un `watch` vivo reabre su consumo cuando el durable se borra (`resync`), sin `CONSUMER.CREATE` y comprobando la configuración | §5.2 |
