# 02 — Naming

Los nombres son la única parte del protocolo que un humano lee a las 3 de la mañana.
Se optimizan para **legibilidad y filtrabilidad**, no para brevedad.

---

## 1. Subject de NATS

```
<dominio>.<agregado>.v<major>.<evento>
```

| Token | Regla | Ejemplos |
|---|---|---|
| `dominio` | Bounded context. Sustantivo plural, `kebab-case`. | `pedidos`, `facturacion`, `logistica` |
| `agregado` | Raíz de agregado. Sustantivo **singular**, `kebab-case`. | `pedido`, `factura`, `linea-envio` |
| `v<major>` | Versión mayor del contrato. Literal `v` + entero ≥ 1. | `v1`, `v2` |
| `evento` | Hecho en **pasado**, `kebab-case`. | `creado`, `cancelado`, `pago-fallido` |

```
pedidos.pedido.v1.creado
pedidos.pedido.v1.cancelado
facturacion.factura.v2.emitida
logistica.envio.v1.entrega-fallida
```

### 1.1 Reglas duras

- **DEBE** tener exactamente 4 tokens. Ni más, ni menos.
- **DEBE** ir todo en minúsculas. NATS distingue mayúsculas y `Pedidos.` ≠ `pedidos.`
  — un solo despiste crea un subject fantasma al que nadie está suscrito.

  > **Requisito derivado — publicar siempre por JetStream, nunca por core NATS.**
  > Verificado contra NATS 2.14.5: un `publish` de **core** a un subject que ningún
  > stream captura **no da ningún error** y el evento se evapora sin rastro. El mismo
  > `publish` por **JetStream** sí falla, porque espera el acuse del stream.
  >
  > Esa es la única red de seguridad automática contra una errata de subject. Un SDK
  > **NO DEBE** exponer publicación por core NATS para eventos de dominio.
  > Ver [`conformance/run.mjs`](../conformance/run.mjs), casos
  > `core-publish-to-wrong-case-is-silent` y `jetstream-publish-to-wrong-case-errors`.
- **NO DEBE** contener `_`, espacios, `*` ni `>`. Solo `[a-z0-9-]` dentro de cada token.
- El plural del dominio y el singular del agregado son deliberados: leído en voz alta,
  `pedidos.pedido` es "el agregado *pedido*, del dominio *pedidos*".

### 1.2 Wildcards que esto habilita

El orden de los tokens está elegido para que los filtros útiles sean expresables:

| Patrón | Captura |
|---|---|
| `pedidos.>` | Todo el dominio de pedidos |
| `pedidos.pedido.v1.>` | Todos los eventos v1 del agregado pedido |
| `pedidos.pedido.*.creado` | `creado` en cualquier versión mayor — útil solo en migraciones |
| `*.*.v1.creado` | Todos los `creado` v1 del ecosistema — auditoría |

> Poner la versión **antes** del evento es intencionado. Con `pedidos.pedido.creado.v1`,
> el filtro "todos los eventos v1 de este agregado" sería `pedidos.pedido.*.v1` —
> que además captura `pedidos.pedido.cancelado.v1`, pero **no** captura eventos con
> nombres de dos tokens. Fijando la versión en la posición 3, la forma del subject es
> invariante y `>` siempre significa "de aquí hacia abajo".

### 1.3 Dominios reservados

`dlq`, `state`, `rpc` y `system` **NO DEBEN** usarse como dominio: cada uno es un espacio de
nombres del protocolo, y un dominio con su nombre colisionaría con él.

| Nombre | Qué ocupa |
|---|---|
| `dlq` | Los subjects de la DLQ: `dlq.<subject original>` (§3.1) y `dlq.state.<dominio>.…` |
| `state` | El estado replicado: `state.<dominio>.<agregado>.v<major>.<tenant>.<clave>` ([10-state.md](10-state.md)), y `schemas/state/` |
| `rpc` | La petición/respuesta (`rpc.<dominio>.…`, fase 3), y `schemas/rpc/` |
| `system` | Lo de la plataforma: `schemas/system/…` y el tenant `"system"` ([09-multitenancy.md §5](09-multitenancy.md)) |

**La colisión es del broker, no solo de estilo.** Medido contra NATS 2.14.5 (A-43): con
`STATE_CLIENTES` (`state.clientes.>`) creado, `EVT_STATE` (`state.>`) falla con `subjects overlap
with an existing stream` (err 10065), y lo mismo `EVT_DLQ` (`dlq.>`) junto a `DLQ_PEDIDOS`
(`dlq.pedidos.>`). O el dominio no se puede crear, o —si se crea primero— es el estado el que no
puede existir. `dlq` no estaba reservado en v1.0 y debería.

Un SDK **DEBE** rechazar un subject o un contrato en un dominio reservado con **`RESERVED_DOMAIN`**
([04-errors.md §6](04-errors.md)); `gen-acl.mjs` y `flux validate` rechazan el manifiesto que lo
declare como dominio, y `bundle-schemas.mjs`, un esquema de evento en `schemas/<reservado>/`.

## 2. `type` de CloudEvents

Reverse-DNS, derivado **determinísticamente** del subject:

```
com.flux.<dominio>.<agregado>.<evento>.v<major>
```

```
subject: pedidos.pedido.v1.creado
type:    com.flux.pedidos.pedido.creado.v1
```

La transformación es mecánica en ambos sentidos, así que un SDK **DEBE** calcularla y
**NO DEBE** pedirla al desarrollador. Los dos formatos existen porque sirven a
consumidores distintos: el subject enruta (y necesita la versión en posición fija
para los wildcards), el `type` identifica el contrato en un catálogo (y ahí lee mejor
con la versión al final, como un sufijo SemVer).

El estado tiene el suyo, con el mismo prefijo que su subject y su ruta de esquema
([10-state.md §2.5](10-state.md)):

```
subject (NATS):  state.clientes.cliente.v1.acme.cli-1
type:            com.flux.state.clientes.cliente.v1
lápida:          com.flux.state.clientes.cliente.deleted.v1
```

## 3. Streams de JetStream

```
EVT_<DOMINIO>        subjects: <dominio>.>
DLQ_<DOMINIO>        subjects: dlq.<dominio>.>
STATE_<DOMINIO>      subjects: state.<dominio>.>          (solo si el dominio tiene estado)
```

```
EVT_PEDIDOS          pedidos.>
DLQ_PEDIDOS          dlq.pedidos.>
STATE_CLIENTES       state.clientes.>
DLQ_CLIENTES         dlq.clientes.>  y  dlq.state.clientes.>
```

- `STATE_<DOMINIO>` guarda **un valor por clave** y no caduca; su configuración está en
  [10-state.md §2.1](10-state.md). Solo existe si el dominio tiene contratos de estado.
- **El stream `DLQ_<DOMINIO>` de un dominio con estado captura además `dlq.state.<dominio>.>`**:
  la DLQ de estado ([10-state.md §6](10-state.md)). Un dominio sin estado conserva el
  `DLQ_<DOMINIO>` de siempre, sin tocarlo.

- Un stream **por dominio**, no por evento. Un stream por evento multiplica ficheros
  y consumidores sin ganar aislamiento real.
- **Los nombres de stream de NATS no admiten `.`, `*`, `>`, `/`, `\` ni espacios.**
  De ahí `EVT_PEDIDOS` con guion bajo y no `EVT.PEDIDOS`. Mayúsculas por convención,
  para distinguir de un vistazo un stream de un subject en los logs.

### 3.1 Por qué la DLQ va en `dlq.<dominio>.…` y no en `<dominio>.….dlq`

Si el subject de DLQ fuese `pedidos.pedido.v1.creado.dlq`, encajaría con `pedidos.>`
y **el stream `EVT_PEDIDOS` lo capturaría también**. Consecuencias: los mensajes
fallidos contarían contra la retención del stream principal, un consumidor con
`pedidos.pedido.v1.>` recibiría los mensajes muertos, y un replay masivo desde la DLQ
podría reinyectarse en su propia DLQ.

Prefijando `dlq.` el espacio de nombres queda **disjunto por construcción**, y la DLQ
puede tener retención, replicación y ACLs propias.

### 3.2 Quién crea los streams

Un SDK **PUEDE** crear el stream de un dominio si no existe, como comodidad de
desarrollo, y en ese caso **DEBE** usar `replicas: 1` por defecto — con 3 no arrancaría
contra un NATS de un solo nodo.

**En producción los streams se provisionan por IaC, no desde el SDK.** Un stream es un
recurso compartido de todo el dominio: dejar que el primer servicio que arranque lo
cree con la configuración que le apetezca convierte la retención, la replicación y la
ventana de duplicados en una carrera. La configuración de abajo es la de producción y
es responsabilidad de operaciones.

### 3.3 Configuración de referencia

```
EVT_PEDIDOS
  subjects:     pedidos.>
  storage:      file
  retention:    limits
  max_age:      30d
  replicas:     3
  discard:      old
  duplicate_window: 2m        # ver 03-delivery.md §3

DLQ_PEDIDOS
  subjects:     dlq.pedidos.>
  storage:      file
  retention:    limits
  max_age:      90d           # más larga: la DLQ es material forense
  replicas:     3
  discard:      old
```

## 4. Durable consumers

```
<servicio>__<subject con los puntos y los guiones sustituidos por guiones bajos>
```

```
subject:  pedidos.pedido.v1.creado
servicio: facturacion-api
durable:  facturacion-api__pedidos_pedido_v1_creado
```

**Los nombres de durable consumer de NATS tampoco admiten `.`** — sustituirlos por
`_` y separar el servicio con `__` permite recuperar el servicio partiendo por `__`. Un
nombre de consumidor que no dice qué servicio lo tiene abierto es inútil en `nats consumer
ls` cuando hay que decidir a las 3 de la mañana si se puede borrar.

Los **guiones** del subject (`linea-envio`) pasan también a `_`, como hacen `durableName()` de
los SDKs y `scripts/lib/manifest.mjs`; esta sección solo decía los puntos. Como consecuencia
`a-b.c…` y `a.b-c…` dan el mismo durable, y el generador de ACLs rechaza un manifiesto que
consuma los dos ([06-security.md §3.0](06-security.md)).

Un SDK L1 **DEBE** derivar este nombre automáticamente a partir del nombre de servicio
de `connect()` y del subject de `subscribe()`.

### 4.1 Durables de estado

```
<servicio>__state_<dominio>_<agregado>_v<major>[__<tenant>]
```

```
contrato: clientes.cliente.v1     servicio: tienda-web      tenant: acme
durable:  tienda-web__state_clientes_cliente_v1__acme        (sin tenant: …_v1)
```

Es el de un `watch` ([10-state.md §5.2](10-state.md)), sobre `STATE_<DOMINIO>`. El contrato se
escribe como en un subject —`state.clientes.cliente.v1`— y se transforma igual: puntos y
guiones a `_`. **El tenant va al final, tras otro `__`, y con sus guiones también a `_`**: un
durable compartido reparte los mensajes entre quienes lo usan (A-47), así que dos instancias del
mismo servicio con tenants distintos **necesitan** durables distintos. Con `__` como separador
caben tres partes sin ambigüedad: ni el servicio (`[a-z0-9-]`) ni los tokens del contrato pueden
contener `__`.

El dueño de un dominio con estado lee su DLQ de estado con
`<servicio>__dlq_state_<dominio>` sobre `DLQ_<DOMINIO>` (filtro `dlq.state.<dominio>.>`), el
paralelo del `<servicio>__dlq_<dominio>` de [06-security.md §3.1](06-security.md).

Un nombre de consumidor admite **255 caracteres** como máximo (A-42); por eso el tenant de un
estado se limita a 64.

## 5. Antipatrones

| ❌ | Por qué | ✅ |
|---|---|---|
| `pedidos.crear-pedido` | Comando, no evento. Y le faltan tokens. | `pedidos.pedido.v1.creado` |
| `pedidos.pedido.v1.actualizado` | No dice **qué** cambió. Obliga a diffear. | `pedidos.pedido.v1.direccion-envio-cambiada` |
| `pedido.pedidos.v1.creado` | Dominio/agregado invertidos. | `pedidos.pedido.v1.creado` |
| `pedidos.pedido.v1.creado.retry` | Un 5º token rompe todos los wildcards. | Los reintentos son de JetStream, no del subject |
| `pedidos.pedido.v1.OrderCreated` | Mayúsculas + idioma mezclado. | `pedidos.pedido.v1.creado` |
| `core.entidad.v1.cambiado` | Dominio genérico = ningún dominio. | Nombra el bounded context real |
| `state.cliente.v1.creado`, `dlq.pedido.v1.creado` | Un dominio reservado: choca con un stream del protocolo, y el SDK lo rechaza (`RESERVED_DOMAIN`). | Nombra el bounded context real — [§1.3](#13-dominios-reservados) |

> **Sobre `actualizado`:** es el antipatrón más caro de los siete. Obliga a cada
> consumidor a comparar contra su propia copia para averiguar qué pasó, lo que
> significa que cada consumidor implementa —y equivoca— su propia lógica de diff.
> Un evento debe declarar el hecho, no delegar su descubrimiento.
