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
  — un solo despiste crea un subject fantasma al que nadie está suscrito y que no
  produce ningún error.
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

## 3. Streams de JetStream

```
EVT_<DOMINIO>        subjects: <dominio>.>
DLQ_<DOMINIO>        subjects: dlq.<dominio>.>
```

```
EVT_PEDIDOS          pedidos.>
DLQ_PEDIDOS          dlq.pedidos.>
```

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

### 3.2 Configuración de referencia

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
<servicio>__<subject con los puntos sustituidos por guiones bajos>
```

```
subject:  pedidos.pedido.v1.creado
servicio: facturacion-api
durable:  facturacion-api__pedidos_pedido_v1_creado
```

**Los nombres de durable consumer de NATS tampoco admiten `.`** — sustituirlos por
`_` y separar el servicio con `__` mantiene la reversibilidad: partiendo por `__`
recuperas servicio y subject exactos. Un nombre de consumidor que no dice qué servicio
lo tiene abierto es inútil en `nats consumer ls` cuando hay que decidir a las 3 de la
mañana si se puede borrar.

Un SDK L1 **DEBE** derivar este nombre automáticamente a partir del nombre de servicio
de `connect()` y del subject de `subscribe()`.

## 5. Antipatrones

| ❌ | Por qué | ✅ |
|---|---|---|
| `pedidos.crear-pedido` | Comando, no evento. Y le faltan tokens. | `pedidos.pedido.v1.creado` |
| `pedidos.pedido.v1.actualizado` | No dice **qué** cambió. Obliga a diffear. | `pedidos.pedido.v1.direccion-envio-cambiada` |
| `pedido.pedidos.v1.creado` | Dominio/agregado invertidos. | `pedidos.pedido.v1.creado` |
| `pedidos.pedido.v1.creado.retry` | Un 5º token rompe todos los wildcards. | Los reintentos son de JetStream, no del subject |
| `pedidos.pedido.v1.OrderCreated` | Mayúsculas + idioma mezclado. | `pedidos.pedido.v1.creado` |
| `core.entidad.v1.cambiado` | Dominio genérico = ningún dominio. | Nombra el bounded context real |

> **Sobre `actualizado`:** es el antipatrón más caro de los seis. Obliga a cada
> consumidor a comparar contra su propia copia para averiguar qué pasó, lo que
> significa que cada consumidor implementa —y equivoca— su propia lógica de diff.
> Un evento debe declarar el hecho, no delegar su descubrimiento.
