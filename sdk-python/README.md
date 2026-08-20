# flux SDK — Python

Cliente de **flux Event Protocol v1** (CloudEvents 1.0 sobre NATS JetStream).
Nivel de conformidad: **L2** ([00-protocol.md §5](../specification/00-protocol.md)).

Es un port fiel del SDK de referencia de Node (`sdk-node/src/`): misma semántica, mismos
defaults, mismos mensajes de error. Lo único que cambia son los nombres (`snake_case`) y
las piezas de plataforma que no existen igual en Python — todas anotadas más abajo.

```bash
pip install -e sdk-python          # requiere Python >= 3.11
```

---

## Publicar

```python
import flux

bus = await flux.connect(
    servers="nats://localhost:4222",
    service="pedidos-api",
    environment="produccion",
    version="3.4.1",
    tenant_id="acme",
    schema_base_url="https://schemas.internal",
)

await bus.publish(
    "pedidos.pedido.v1.creado",
    {
        "pedidoId": "ped-123",
        "clienteId": "cli-987",
        "aggregateVersion": 1,
        "totalCents": 9990,          # entero en la unidad mínima, nunca float
        "moneda": "EUR",
    },
    aggregate_id="ped-123",
)
```

Tú escribes **subject, `data` y opcionalmente `aggregate_id`**. El SDK rellena `id`
(UUIDv7), `source`, `time`, `specversion`, `type`, `dataschema`, `correlationid`,
`causationid`, `producerversion` y `traceparent`. Si tu código asigna alguno de esos a
mano, está mal ([01-envelope.md §5](../specification/01-envelope.md)).

> ⚠️ `aggregate_id` es el atributo `subject` de CloudEvents (`"ped-123"`), **no** el
> subject de NATS. Son dos cosas distintas con el mismo nombre y confundirlas es el error
> más frecuente al adoptar CloudEvents sobre NATS.

## Consumir

```python
async def handler(evento, ctx):
    if await ya_procesado(evento.id):    # OBLIGATORIO: la garantía es at-least-once
        return ctx.ack()
    await hacer_el_trabajo(evento.data)
    await marcar_procesado(evento.id)
    return ctx.ack()

await bus.subscribe("pedidos.pedido.v1.creado", handler)
```

- El handler puede ser `async` o síncrono.
- Devolver normalmente = `ack`. Lanzar = clasificar y decidir (ver abajo).
- **Todo consumidor DEBE ser idempotente.** Los duplicados llegan; no son un fallo.
  Elige una de las tres estrategias de [03-delivery.md §4](../specification/03-delivery.md).
- Un handler que tarde más de `ack_wait` (30 s) sin ceder control es un bug de la
  aplicación. Mientras esté vivo, el SDK emite work-in-progress cada 15 s.

## Errores

```python
from flux import RetryableError, PermanentError

raise RetryableError("proveedor 503", retry_after_ms=5000)   # nak + backoff
raise PermanentError("pedido ya cancelado", code="PEDIDO_YA_CANCELADO")  # term + DLQ
```

| Clase | Qué es | Acción |
|---|---|---|
| `RETRYABLE` | Timeout, ECONNRESET, HTTP 429/502/503/504 | `nak` + backoff canónico |
| `PERMANENT` | Falla el schema, regla de negocio, HTTP 400/403/404/422 | `term()` + DLQ inmediato |
| `POISON` | JSON malformado, falta un atributo CloudEvents | `term()` + DLQ + alerta |

**Lo desconocido es RETRYABLE con presupuesto acotado: 2 entregas, no 6**
([04-errors.md §2.1](../specification/04-errors.md)).

```
Error reconocido como transitorio (ECONNRESET, 503) → 6 entregas, hasta 51 min
Error desconocido                                   → 2 entregas, ~30 s
Error reconocido como permanente (400, 422)         → 1 entrega, sin espera
```

Las dos opciones obvias fallan cada una en un extremo: `permanent` manda a la DLQ un
evento válido por un hipo de red y alguien lo reproduce a mano; `retryable` completo
atasca la cola 51 minutos y el modo de fallo se amplifica con cada mensaje siguiente. El
acotado cuesta 30 segundos de latencia sobre los permanentes genuinos y elimina ambos
problemas — no es un punto medio, es estrictamente mejor.

El presupuesto **no** se configura en `max_deliver` del consumidor: `max_deliver` es por
consumidor, no por mensaje, y bajarlo a 2 recortaría también los reintentos de los
RETRYABLE reconocidos. El clasificador rellena `Classification.max_attempts` solo para
los errores desconocidos y el runtime aplica
`min(max_deliver, max_attempts)` a ese error concreto.

Todo ello es configurable, porque el equilibrio correcto depende de cómo fallen vuestras
dependencias:

```python
from flux import ClassifierOptions

bus = await flux.connect(
    ...,
    classifier=ClassifierOptions(
        unknown_error_policy="retryable-bounded",  # o "permanent" / "retryable"
        unknown_retry_budget=2,
        timeout_policy="permanent",
    ),
)
```

## Propagación de contexto

`correlationid`, `causationid` y `traceparent` se propagan solos por `contextvars`: un
`publish()` en cualquier punto de la pila del handler hereda el contexto del evento
entrante. No hay que pasar nada por parámetro.

La única diferencia con `AsyncLocalStorage` de Node: una `ContextVar` se copia al
**crear** la tarea. Una tarea lanzada desde dentro del handler hereda el contexto; un
worker de fondo creado antes, no — y publicará sin correlación.

---

## Diferencias de port respecto al SDK de Node

Ninguna cambia la semántica en el cable. Se documentan porque son los sitios donde un
lector que venga de Node se sorprendería.

| Node | Python | Por qué |
|---|---|---|
| `AsyncLocalStorage` | `contextvars.ContextVar` | Equivalente directo. `use_context(ctx)` es un context manager en vez de `runWithContext(ctx, fn)`: envuelve handlers sync y async con una sola API. |
| Duraciones en **nanosegundos** (`nanos()`) | Duraciones en **segundos** (`seconds()`) | `nats-py` convierte a nanosegundos él mismo dentro de `ConsumerConfig.as_dict()`. Pasarle nanosegundos no da error: da un `ack_wait` de 950 años. |
| `js.publish(subject, data, { msgID })` | `js.publish(subject, data, headers={"Nats-Msg-Id": id})` | `nats-py` no tiene parámetro `msg_id`; la cabecera es la misma. |
| `js` + `jsm` (dos objetos) | Un solo `js` | En `nats-py`, `JetStreamContext` hereda de `JetStreamManager`. |
| `consume()` (push) | `pull_subscribe_bind()` + `fetch(batch=1)` | `batch=1` no es una limitación: con `batch=N` los N-1 mensajes en espera gastan su `ack_wait` sin que nadie emita WIP por ellos, y JetStream los reentrega mientras el primero sigue en el handler. Justo la concurrencia que [03-delivery.md §2.1](../specification/03-delivery.md) obliga a evitar. |
| `reconnectJitter` | `reconnect_time_wait` aleatorizado por proceso | `nats-py` no expone jitter. Aleatorizar la espera al conectar consigue lo que el jitter persigue: que mil servicios no reconecten en el mismo milisegundo. |
| `interface Classification { class }` | `Classification.error_class` | `class` es palabra reservada en Python. Es el único renombrado semántico del port. |
| `uuid` (paquete) | `flux.protocol.uuid7` | Python < 3.14 no trae `uuid.uuid7()`. Se implementa con la stdlib, con contador en `rand_a` para conservar la monotonía intra-milisegundo de la que depende [01-envelope.md §2.2](../specification/01-envelope.md). |
| `import { connect } from "@flux/sdk"` | `flux.connect` se resuelve de forma diferida | Así los tests de naming y envelope no necesitan tener `nats-py` instalado. |
| — | `extract_syscall_code` normaliza el prefijo `WSA` | En Windows `errno.errorcode[10054]` es `WSAECONNRESET`. Sin normalizar, el mismo corte de red sería RETRYABLE en Linux y PERMANENT en Windows. |

Además, `parse_event` trata un atributo obligatorio con valor `null` como **ausente**
(POISON), donde Node solo comprueba `undefined`. `null` para significar "ausente" está
prohibido por [01-envelope.md §4](../specification/01-envelope.md), así que la
divergencia va en la dirección estricta a propósito.

## Tests

```bash
python -m pytest sdk-python/tests -q
```

No necesitan broker. Varios casos leen `protocol.json` directamente, de modo que una
divergencia entre el SDK y el contrato falla en CI en vez de en producción.

Para los invariantes que sí requieren un servidor real —que `ack_wait` sobrevive, que un
durable con puntos es rechazado, que `dlq.` queda disjunto— ver
[`conformance/cases/`](../conformance/cases/).
