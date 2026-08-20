# flux SDK — Python

Cliente de **flux Event Protocol v1** (CloudEvents 1.0 sobre NATS JetStream).
Nivel de conformidad: **L2** ([00-protocol.md §5](../specification/00-protocol.md)).

Es un port fiel del SDK de referencia de Node (`sdk-node/src/`): misma semántica, mismos
defaults, mismos mensajes de error. Lo único que cambia son los nombres (`snake_case`) y
las piezas de plataforma que no existen igual en Python — todas anotadas más abajo.

```bash
pip install -e sdk-python              # requiere Python >= 3.11
pip install -e "sdk-python[signing]"   # + firma Ed25519 (extensión opcional)
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

> ⚠️ `retry_after_ms` es una **sugerencia para el primer reintento**, no un control del
> calendario. Con `backoff` configurado —y flux lo configura siempre— JetStream honra el
> delay del `nak` solo en la primera reentrega; a partir de la segunda manda el array
> `backoff` y el delay **se ignora sin ningún aviso** (medido contra NATS 2.14.5,
> [03-delivery.md §2.2](../specification/03-delivery.md)). Un `Retry-After: 5` de un
> proveedor acorta el primer reintento y nada más.

| Clase | Qué es | Acción |
|---|---|---|
| `RETRYABLE` | Timeout, ECONNRESET, HTTP 429/502/503/504 | `nak` + backoff canónico |
| `PERMANENT` | Falla el schema, regla de negocio, HTTP 400/403/404/422 | `term()` + DLQ inmediato |
| `POISON` | JSON malformado, falta un atributo CloudEvents o una extensión obligatoria del perfil flux | `term()` + DLQ + alerta |

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

## Aislamiento entre tenants

```python
bus = await flux.connect(
    ...,
    tenant_id="acme",
    tenant_isolation="strict",   # toda suscripción filtra; olvidarlo lanza
)
```

- `"off"` (default): se filtra si hay `tenant_id`, pero olvidarlo no rompe nada.
- `"strict"`: suscribirse sin tenant configurado lanza `TenantIsolationError` **al
  arrancar**, antes de tocar el broker.

El filtrado ocurre **antes** del handler: un evento de otro tenant se `ack`ea y se
descarta — no es un fallo, no es para nosotros
([09-multitenancy.md §3](../specification/09-multitenancy.md)).

El modo estricto es el punto que importa. Un filtro que hay que acordarse de poner es un
filtro que alguien olvidará, y el fallo —ver los datos de otro tenant— **no produce ningún
error**: produce un incidente de privacidad que se descubre semanas después.

`"system"` **no cuenta** como filtro de tenant: es la ausencia de tenant, no un tenant
([§5](../specification/09-multitenancy.md)). Aceptarlo dejaría fuera todos los eventos de
negocio y daría por satisfecho el modo estricto sin filtrar nada.

Lo que este modelo **no** cubre: un servicio legítimo comprometido puede publicar con el
`tenantid` de otro, y un consumidor comprometido puede leer el subject entero. El filtro
del SDK evita errores, no adversarios; para eso está el Modelo B (una account de NATS por
tenant).

## Firma de eventos — extensión opcional

Traslada la autenticidad **del canal al evento**: un evento firmado sigue siendo
verificable dentro de un fichero, un backup o un correo, donde ya no hay ninguna ACL que
lo respalde ([07-signing.md](../specification/07-signing.md)).

```python
from flux import SigningOptions, generate_key_pair

par = generate_key_pair()   # PEM: PKCS#8 la privada, SPKI la pública

bus = await flux.connect(
    ...,
    signing=SigningOptions(
        private_key_pem=par.private_key_pem,     # firmar al publicar
        key_id="pedidos-api-3",
        public_keys={"pedidos-api-3": par.public_key_pem},
        verify="require",                        # off (default) | warn | require
    ),
)
```

- Ed25519, sin negociación de algoritmo. Los formatos con algoritmo negociable acumulan
  una familia de vulnerabilidades —de `alg: none` a la confusión HMAC/RSA— que solo existe
  porque hay algo que negociar.
- Se firma `serialize(evento sin signature y sin las extensiones dlq*)`. **No hay
  canonicalización aparte**: es el mismo `serialize()` del productor, y funciona porque
  [01-envelope.md](../specification/01-envelope.md) §1.1, §2.2 y §6 fijan una única
  representación en bytes.
- `signkeyid` va **dentro** de lo firmado. Si quedara fuera, un atacante lo cambiaría para
  que la verificación buscara otra clave.
- Un evento que pasa por la DLQ **sigue verificando**: las `dlq*` se añaden después de
  firmar y la verificación las ignora. El replay redistribuye un hecho ya emitido.

| Modo | Evento sin firma | Firma inválida |
|---|---|---|
| `off` (default) | Se acepta | Se acepta (no se mira) |
| `warn` | Se registra y se acepta | Se registra y se acepta |
| `require` | POISON `MISSING_SIGNATURE` | POISON `INVALID_SIGNATURE` / `UNKNOWN_SIGNING_KEY` |

`warn` existe porque adoptar la firma en un ecosistema en marcha exige un periodo en el
que unos productores firman y otros no. Pasar directo a `require` convierte en POISON todo
evento de un servicio aún no migrado.

> **Conserva las claves públicas retiradas** mientras exista algún evento firmado con
> ellas — mínimo 90 días, la retención de la DLQ. Retirar una clave impide **emitir** con
> ella, no **verificar** lo ya emitido; tratarla como inválida convierte una rotación
> rutinaria en la invalidación retroactiva de todo el historial
> ([§6](../specification/07-signing.md)).

## Métricas

Siete métricas con nombres y etiquetas fijados por el protocolo
([08-observability.md](../specification/08-observability.md)). No son una decisión del
SDK: si Python y Go nombraran distinto la tasa de DLQ, un panel del ecosistema sería
imposible.

```python
from flux import InMemoryMetrics

metrics = InMemoryMetrics()
bus = await flux.connect(..., metrics=metrics)

# En tu servidor HTTP:
#   return Response(metrics.render(), media_type="text/plain; version=0.0.4")
```

| Métrica | Tipo | Etiquetas |
|---|---|---|
| `flux_events_published_total` | Counter | `subject`, `outcome` |
| `flux_events_consumed_total` | Counter | `subject`, `consumer`, `outcome` |
| `flux_event_handler_duration_seconds` | Histogram | `subject`, `consumer` |
| `flux_events_dlq_total` | Counter | `subject`, `consumer`, `reason`, `code` |
| `flux_events_retried_total` | Counter | `subject`, `consumer`, `attempt` |
| `flux_consumer_pending` | Gauge | `subject`, `consumer` |
| `flux_connection_state` | Gauge | — |

- El default es **no-op**: un SDK no debe imponer un backend. `InMemoryMetrics` es una
  comodidad sin dependencias; si ya usas `prometheus_client`, implementa `MetricsSink`
  contra él y conserva los nombres.
- **Nunca** se etiqueta por `tenantid`, `id` ni `correlationid`. Por eso `MetricsSink`
  tiene parámetros nombrados y no un `labels: dict`: un diccionario genérico es
  exactamente por donde se cuela el `tenantid` que multiplica las series temporales. La
  cardinalidad no avisa — funciona con tres tenants en desarrollo y muere con diez mil en
  producción.
- El último bucket del histograma es `30` **porque es el `ack_wait`**: un handler que cae
  ahí está a punto de que su mensaje se reentregue mientras aún se ejecuta. Hay un test que
  falla si alguien cambia `ack_wait` y olvida el bucket.

- `flux_connection_state` (`1` conectado, `0` desconectado, `2` reconectando) va enganchado
  a los callbacks de `nats-py`: sin eso valdría `1` hasta el `close()` y no diría nada.

⚠️ `flux_consumer_pending` no lo alimenta el SDK todavía —tampoco el de Node—: sale de
`ConsumerInfo.num_pending` y hace falta un sondeo periódico contra el broker. El método está
en la interfaz para que quien lo sondee use el nombre correcto. Importa más de lo que
parece: un consumidor cuyo bucle murió **sigue reportando la conexión como sana**, y solo el
crecimiento de `pending` lo delata ([§4](../specification/08-observability.md)).

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
| `node:crypto` (sin dependencias) | `cryptography`, extra `[signing]` | La stdlib de Python no trae Ed25519. Es la única pieza de la fase que no sale gratis: se importa de forma diferida, así que un servicio que no firma no la necesita instalada. |
| `TenantIsolationError` en `client.ts` | `flux.tenant.TenantIsolationError` | La regla de aislamiento tiene que poder probarse **sin `nats-py`**. Una regla de seguridad que solo se ejecuta con infraestructura delante es una regla que nadie prueba. Se reexporta desde `flux` y desde `flux.client`. |
| Firma añadida al final del objeto | Orden fijado en `FluxEvent.to_dict` | En Node el orden de claves es el de inserción y hay que cuidarlo a mano; aquí `signkeyid` y `signature` están declarados entre `tracestate` y las `dlq*`, y `data` queda último sin esfuerzo. El resultado en el cable es idéntico. |
| `render()` sustituye `"` por `_` | `render()` **escapa** `\`, `"` y `\n` | Prometheus define el escapado; sustituir pierde el valor. Un `code` con comillas no rompe el scrape en ninguno de los dos, pero aquí además se lee. |

Además, `parse_event` trata un atributo obligatorio con valor `null` como **ausente**
(POISON), donde Node solo comprueba `undefined`. `null` para significar "ausente" está
prohibido por [01-envelope.md §4](../specification/01-envelope.md), así que la
divergencia va en la dirección estricta a propósito.

## Qué rechaza `parse_event`

Todo fallo de parseo es POISON: el mensaje ni siquiera es interpretable, así que nunca
llega al handler ([04-errors.md §1.3](../specification/04-errors.md)). El `code` es el
mismo que dan Node, Go y Java ante el mismo cuerpo.

| `code` | Cuándo |
|---|---|
| `MALFORMED_JSON` | El cuerpo no es JSON |
| `NOT_AN_OBJECT` | Es JSON pero no un objeto en la raíz |
| `UNSUPPORTED_SPECVERSION` | `specversion` != `"1.0"` |
| `MISSING_REQUIRED_ATTRIBUTE` | Falta —o vale `null`— un atributo del núcleo CloudEvents |
| `MISSING_REQUIRED_EXTENSION` | Falta —o vale `null` o `""`— `correlationid`, `tenantid`, `producerversion` o `dataclassification` |
| `INVALID_DATACLASSIFICATION` | `dataclassification` fuera de `{public, internal, confidential, restricted}` |
| `WRONG_ATTRIBUTE_TYPE` | Un atributo de texto llegó como número, booleano u objeto |
| `UNSUPPORTED_CONTENT_TYPE` | `datacontenttype` != `application/json` |
| `UNKNOWN_ROOT_ATTRIBUTE` | Un atributo raíz fuera de la lista cerrada |
| `MISSING_SIGNATURE` | Modo `require` y el evento no va firmado — [07-signing.md §7](../specification/07-signing.md) |
| `INVALID_SIGNATURE` | La firma no verifica: el evento fue manipulado o no lo emitió esa clave |
| `UNKNOWN_SIGNING_KEY` | El `signkeyid` no está entre las claves conocidas |

Las tres reglas del medio son las que un port suele dar por hechas y no lo están:

- Las cuatro extensiones son **obligatorias de verdad**
  ([§3.1](../specification/01-envelope.md)). No se les asume un default porque asumirlo
  es peligroso en las cuatro: un `dataclassification` ausente tomado como `internal` hace
  circular PII con 30 días de retención en vez de 7, y un `tenantid` ausente tomado como
  `system` cruza fronteras de tenant.
- Los tipos son **exactos** ([§2.4](../specification/01-envelope.md)): `{"tenantid": 42}`
  es POISON, no el tenant `"42"`. En Python hay que comprobarlo con `isinstance`, porque
  `json.loads` devuelve el `int` tal cual y nadie más lo mira hasta que una comparación
  con `==` falla en producción.

## Tests

```bash
python -m pytest sdk-python/tests -q
```

No necesitan broker. Varios casos leen `protocol.json` directamente, de modo que una
divergencia entre el SDK y el contrato falla en CI en vez de en producción — incluidos los
**nombres y etiquetas de las siete métricas**, que son contrato.

| Fichero | Qué fija |
|---|---|
| `test_protocol.py` | Naming, envelope, clasificación de errores, contexto |
| `test_signing.py` | Firma Ed25519: manipulación, DLQ, rotación de claves, modos |
| `test_metrics.py` | Nombres, etiquetas, buckets y formato de exposición |
| `test_tenant.py` | Filtro de tenant y modo estricto |

Un caso queda como `skipped` sin `nats-py` instalado: el que comprueba los campos de
`ConnectOptions`, que vive en el único módulo que necesita el cliente de NATS.

Para los invariantes que sí requieren un servidor real —que `ack_wait` sobrevive, que un
durable con puntos es rechazado, que `dlq.` queda disjunto— ver
[`conformance/cases/`](../conformance/cases/).
