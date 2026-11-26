# 01 — Envelope

**Base normativa:** [CloudEvents 1.0](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md).
Todo lo que CloudEvents 1.0 declara obligatorio lo es aquí. Este documento define el
**perfil flux**: qué extensiones añadimos, cuáles prohibimos y cómo se estructura
`data`.

---

## 1. Modo de codificación

flux v1 usa **structured mode**: el CloudEvent completo viaja serializado como JSON
en el cuerpo del mensaje NATS.

```
datacontenttype del mensaje NATS: application/cloudevents+json; charset=utf-8
```

**Por qué structured y no binary:** en binary mode los atributos viajan en cabeceras
`ce-*` y el payload en el cuerpo. Es más eficiente, pero fragmenta el evento entre
dos lugares — un `nats stream get` devuelve el cuerpo sin las cabeceras, y depurar
se vuelve doloroso. En structured mode, **un mensaje es un fichero JSON completo**:
lo copias, lo pegas en un test, lo reproduces. Esa propiedad vale más que los bytes
que ahorras.

> Binary mode **PUEDE** habilitarse por subject en v2, cuando el throughput lo
> justifique con datos medidos.

### 1.1 UTF-8 literal, sin escapes `\u`

El JSON **DEBE** emitirse en UTF-8 con los caracteres no-ASCII **literales**. `"café"`
se serializa como `café`, no como `café`.

Ambas formas son JSON válido y se parsean al mismo string, pero **no son los mismos
bytes** — y §6 explica por qué eso importa. El encoder por defecto de
`System.Text.Json` escapa todo lo no-ASCII además de `<>&'+`, así que un payload en
español producido por .NET no coincide byte a byte con el de Node o Go. Hace falta
`JavaScriptEncoder.UnsafeRelaxedJsonEscaping` — cuyo nombre asusta pero solo significa
"no escapes lo que no hace falta escapar en un contexto que no es HTML".

> No estaba en la spec porque los tres primeros SDKs no lo sufren: `JSON.stringify`,
> `json.dumps(ensure_ascii=False)` y `encoding/json` de Go emiten UTF-8 literal. Un
> ecosistema que solo hable inglés tampoco lo notaría nunca.

**Qué se escapa y qué no.** Solo `"`, `\` y los caracteres de control U+0000–U+001F (con su forma
corta —`\b \f \n \r \t`— y, si no la hay, `\u00xx` con los cuatro dígitos en **minúscula**). Todo lo demás va
**literal**, incluidos U+007F (DEL), U+2028 y U+2029, `<`, `>`, `&` y `/` (`\/` no se conserva). Es lo que escribe
`JSON.stringify`, y lo que fija la forma canónica de `data` ([§4.1](#41-la-forma-canónica-de-data)).

## 2. Atributos de contexto — núcleo CloudEvents

| Atributo | Tipo | Obl. | Regla flux |
|---|---|:--:|---|
| `specversion` | String | ✅ | **DEBE** ser exactamente `"1.0"`. |
| `id` | String | ✅ | **DEBE** ser un [UUIDv7](https://www.rfc-editor.org/rfc/rfc9562) en formato canónico. Lo genera el SDK. |
| `source` | URI-ref | ✅ | **DEBE** ser `/<entorno>/<servicio>`. Ej.: `/produccion/pedidos-api`. |
| `type` | String | ✅ | Reverse-DNS. Ver [02-naming.md §2](02-naming.md). |
| `time` | Timestamp | ✅ | RFC 3339 en **UTC**, **exactamente 3 decimales y sufijo `Z`**. Ver §2.3. **DEBE** ser el instante en que ocurrió el hecho, no el de publicación. |
| `datacontenttype` | String | ✅ | `application/json` en v1. |
| `dataschema` | URI | ✅ | URI absoluta al JSON Schema, con SemVer. Ver [05-compatibility.md](05-compatibility.md). |
| `subject` | String | ⚠️ | **DEBE** ser el identificador del agregado (ej. `"ped-123"`). Ver el aviso de §2.1. |

### 2.1 ⚠️ `subject` de CloudEvents ≠ subject de NATS

Son dos cosas distintas con el mismo nombre y confundirlas es el error más frecuente
al adoptar CloudEvents sobre NATS:

| | Significado | Ejemplo |
|---|---|---|
| **subject de NATS** | Dirección de enrutado. Determina quién recibe el mensaje. | `pedidos.pedido.v1.creado` |
| **`subject` de CloudEvents** | Identidad del agregado *dentro* de la fuente. Es un dato, no una dirección. | `ped-123` |

Un SDK **NO DEBE** exponer una API donde ambos se confundan. En flux el subject de
NATS se llama siempre `subject` en las firmas de `publish`/`subscribe`, y el atributo
de CloudEvents se llama `aggregateId` en la API del SDK, mapeándose a `subject` solo
al serializar.

### 2.2 ⚠️ Formato de `time`: exactamente 3 decimales

```
✅  2025-08-20T10:25:39.410Z
✅  2025-08-20T10:25:39.400Z
✅  2025-08-20T10:25:39.000Z
❌  2025-08-20T10:25:39.41Z          ← ceros recortados
❌  2025-08-20T10:25:39.410000+00:00 ← microsegundos y offset en vez de Z
❌  2025-08-20T10:25:39Z             ← sin decimales
```

"Precisión de milisegundos" no basta como regla, porque los formateadores por defecto
de cada lenguaje producen cosas distintas **todas válidas según RFC 3339**:

| | Por defecto | Resultado |
|---|---|---|
| Go | `time.RFC3339Nano` | `…39.41Z` — recorta ceros finales |
| Python | `datetime.isoformat()` | `…39.410000+00:00` — microsegundos y offset |
| Node | `Date.toISOString()` | `…39.410Z` — correcto por casualidad |

El resultado sería que **dos servicios del mismo ecosistema emiten `time` distintos
para el mismo instante**. Eso rompe:

- El **replay verbatim** desde la DLQ, que deja de ser byte a byte idéntico.
- Cualquier **firma criptográfica** sobre el evento serializado (fase 4).
- La **deduplicación por hash de contenido**.
- Los **fixtures compartidos** entre SDKs en la suite de conformidad.

Un SDK **DEBE** formatear explícitamente y **NO DEBE** usar el formateador ISO por
defecto de su lenguaje. Verificado byte a byte entre Node, Python y Go.

**El `time` que da quien llama.** El SDK lo pone él mismo y la API pública no lo recibe; pero los constructores del
sobre (los que ejecuta el arnés de conformidad, y quien construya un transporte propio) reciben un `time` de fuera.
Copiarlo tal cual dejaría pasar `…39Z` o `…+02:00` —otros bytes— o un valor que ni siquiera es un instante. Por eso un
constructor **normaliza** lo que le dan a la forma canónica o lo **rechaza**:

- Se admite un `date-time` de RFC 3339: `T` o `t` (o un espacio) entre fecha y hora; `Z` o `z`, o un desfase
  `±hh:mm`; decimales opcionales y de **cualquier** longitud. Se convierte a UTC y se escribe con **exactamente 3
  decimales y `Z`**; los decimales de más de tres se **truncan** (`.9999` es `.999`), no se redondean.
  `2026-08-20T12:25:39.412+02:00`, `2026-08-20T10:25:39.4120Z` y `2026-08-20T10:25:39.412345Z` dan los mismos
  bytes que `2026-08-20T10:25:39.412Z`.
- **No** se admite: algo que no es un instante (`ayer`, `2026-13-20…`, `2026-02-30…`, sin zona horaria, solo fecha, un
  desfase `+24:00`), **un segundo 60** (un segundo intercalar no existe en UTC de POSIX, y el `Date` de JavaScript lo
  desplazaría al minuto siguiente: dos SDKs darían dos instantes), ni un instante cuyo año en UTC caiga fuera de
  0000–9999 (`+10000-01-01…` y `-0001-12-31…` dejarían de ser RFC 3339).
- Un `time` rechazado es **`WRONG_ATTRIBUTE_TYPE`** (CloudEvents tipa `time` como `Timestamp`); en petición/respuesta,
  `INVALID_RPC_CALL` ([04-errors.md §7](04-errors.md)).

Node copiaba lo que le daban y Rust lo normalizaba: el mismo `time` daba dos secuencias de bytes. Los vectores
`ajuste-*-time-*` lo fijan para Node y Rust; **pendiente en Python, Go, Java, .NET y PHP** (Fase 6): hoy no los recorren.

### 2.3 Comparación de nombres de atributo: case-sensitive

Los nombres de atributo se comparan **respetando mayúsculas**. `{"ID": "..."}` **no**
es el atributo `id`; es un atributo raíz desconocido y por tanto POISON (§3.3).

La regla existe porque `encoding/json` de Go empareja campos **sin distinguir
mayúsculas** por defecto: un `{"ID": ...}` poblaría `Event.ID` en silencio. Es
exactamente el mismo fantasma que [02-naming.md §1.1](02-naming.md) combate en los
subjects. La regla de atributos raíz cerrados lo tapa por accidente; esta sección lo
hace explícito para que ningún SDK dependa de esa casualidad.

### 2.4 Los tipos son exactos: nada de coerción

Un atributo declarado `String` **DEBE** llegar como cadena JSON. `{"tenantid": 42}` es
**POISON**, no el tenant `"42"`.

No es pedantería: los deserializadores discrepan por defecto. Jackson convierte `42` en
`"42"` sin avisar; Go lo rechaza; Node lo acepta como número y lo propaga. El mismo
mensaje produce tres comportamientos distintos, y el que "funciona" es el peor —
propaga un tipo inesperado hasta que alguien compara `tenantid` con `===`.

Un SDK **DEBE** desactivar la coerción de tipos de su deserializador.

Lo mismo vale para los atributos **no obligatorios** y para `dataschema`: un `"subject": 5`, un
`"causationid": 7` o un `"dlqattempts": "seis"` no se propagan. Es un mensaje ininterpretable —**POISON**— con el código
**`INVALID_ATTRIBUTE_TYPE`**, que se comprueba **después** de todos los demás de §3.1 y de los atributos raíz
desconocidos (el orden es parte del contrato). `null` en un opcional es «ausente» (§3.3); `dlqattempts` es un entero de 0
a 2^32 − 1 y `dlqreason`, uno de `retryable`, `permanent` o `poison`. Rust, Java y .NET ya lo hacían; Node dejaba pasar el
valor.

### 2.5 Fidelidad numérica del payload

Un SDK **NO DEBE** reescribir los números de `data` al deserializar y volver a
serializar. `4995.00` **DEBE** volver a salir como `4995.00`.

Go y Python lo cumplen por accidente —guardan el payload como bytes crudos— pero
Jackson lo normaliza a `4995.0` salvo que se le pida lo contrario
(`USE_BIG_DECIMAL_FOR_FLOATS` sin recorte de ceros). El replay verbatim depende de
esto tanto como del orden de claves (§6).

> La forma más simple de no tener este problema: **no metas decimales en `data`.** Los
> importes van como entero en la unidad mínima (§4), y ahí no hay nada que normalizar.

Esta regla es del camino que **consume y reenvía** (replay, DLQ): los bytes recibidos salen igual. Un SDK que **construye** un
`data` nuevo emite su forma canónica ([§4.1](#41-la-forma-canónica-de-data)), y los números que un lenguaje no puede escribir
como otro no tienen forma común: ahí está documentado qué no se admite.

### 2.6 Unicidad

`id` + `source` **DEBEN** ser únicos en su conjunto. Esa pareja es la clave de
deduplicación del ecosistema entero. Como `id` es UUIDv7 (monotónico en el tiempo),
ordenar por `id` dentro de un mismo `source` equivale a ordenar por instante de
generación — útil al reconstruir historiales desde la DLQ.

#### 2.6.1 El `id` de un hecho importado es determinista

Un sistema externo —una pasarela de pagos, un proveedor, el sistema de un tercero— **reenvía el mismo aviso** durante horas: porque
no recibió el `2xx`, porque reintenta con su propio calendario, porque alguien pulsó «reenviar». Si cada reenvío recibe un `id`
nuevo, **ningún consumidor puede deduplicarlo**: para el ecosistema son tres hechos distintos. Y la dedupe por `id` es la única
que sobrevive a todo (`duplicate_window` dura 2 minutos y solo cubre publicaciones: [03-delivery.md §3](03-delivery.md)).

Para un **hecho importado**, el `id` **se deriva** de lo que identifica al hecho en lugar de generarse:

```
id = UUIDv7(
  unix_ts_ms = los milisegundos del `time` del hecho EXTERNO,
  rand_a     = los bits  0..11 de H,
  rand_b     = los bits 12..73 de H
)          con  H = SHA-256( <source> "|" <clave externa> )   (los bits se numeran desde el más significativo)
```

- **Es un UUIDv7 válido** ([RFC 9562](https://www.rfc-editor.org/rfc/rfc9562) §5.7): versión `7`, variante `10`, 48 bits de tiempo, y
  **74 bits** (12 de `rand_a` y 62 de `rand_b`) tomados de `H` en vez de un generador de azar. Cualquier consumidor que valide el
  formato del `id` (§2) lo acepta, y la ordenación por `id` dentro de un `source` sigue siendo la del instante del hecho.
- **Es siempre el mismo** para la misma terna `(source, clave externa, milisegundos)`. Es una función pura: no usa el reloj ni
  ninguna fuente de azar, y **todos los SDKs la calculan igual** (vectores en
  [`conformance/cases/cross-sdk-external-id.json`](../conformance/cases/cross-sdk-external-id.json)).
- **`source`** es el del que publica (`/<entorno>/<servicio>`, §2). No contiene `|`, así que el separador no es ambiguo: la
  derivación es inyectiva en `(source, clave externa)`. La clave externa es una cadena UTF-8 **no vacía** de **como máximo 512
  bytes**: el identificador del hecho **en el sistema externo** (`evt_1Nx…`), no un documento.
- **`time` es el instante del hecho externo**, el que da el sistema de origen, **no «ahora»**: es lo que hace estable el `id`
  (con «ahora» cada reenvío tendría milisegundos distintos y, por tanto, un `id` distinto) y es lo que §2 ya exige de `time`.
  Un SDK que ofrezca la clave externa **DEBE exigir `time`** y rechazar su ausencia con un error accionable, que explique que
  **si el sistema externo no da un instante no hay `id` determinista**. Los milisegundos han de caber en 48 bits (0 a 2^48 − 1).
- **Dos hechos distintos con la misma clave y distinto instante tienen `id` distinto**: la clave externa **debe ser única por
  hecho** dentro del `source` (un proveedor que reutiliza una clave para eventos distintos no es compatible con esta regla, y para
  él hay que **prefijar** la clave con lo que la distingue). Dos productores distintos con la misma clave no chocan: su `source`
  difiere.
- **No sustituye a la deduplicación del consumidor** ([03-delivery.md §4](03-delivery.md)): la hace posible. Dentro de
  `duplicate_window` JetStream ni siquiera guarda el duplicado; fuera, guarda los dos y los consumidores aplican **un** efecto por
  `id`.

API: `publish(subject, data, { externalKey, time })` en el SDK ([§5](#5-reglas-de-relleno-automático): `id` lo sigue generando el
SDK, aquí con esta derivación; el desarrollador sigue sin escribirlo). Quien reciba webhooks de terceros lo usa a través del
gateway ([12-edge.md §5](12-edge.md)).

Ejemplos (los comprobó Node y, de forma independiente, Python con `hashlib`):

| `source` | clave externa | `time` | `id` |
|---|---|---|---|
| `/produccion/pasarela-web` | `pasarela:evt_1Nx` | `2026-08-20T10:25:39.412Z` | `01a01eb4-7a54-7325-ac9b-67da707ec1d4` |
| `/produccion/pasarela-web` | `pasarela:evt_1Ny` | `2026-08-20T10:25:39.412Z` | `01a01eb4-7a54-7923-9653-1de6ff56d23a` |
| `/produccion/otra-web` | `pasarela:evt_1Nx` | `2026-08-20T10:25:39.412Z` | `01a01eb4-7a54-7b72-9f1e-b64778eeb41f` |
| `/produccion/pasarela-web` | `pasarela:evt_1Nx` | `2026-08-20T10:25:39.413Z` | `01a01eb4-7a55-7325-ac9b-67da707ec1d4` |
| `/dev/tienda-web` | `pedido-ñandú-ß-日本` | `2024-02-29T23:59:59.999Z` | `018df74f-83ff-72cc-9c87-4843a505450d` |
| `/dev/a` | `k` | `1970-01-01T00:00:00.000Z` | `00000000-0000-7fdc-8906-90c15f53aa90` |

Las filas 2, 3 y 4 difieren de la primera en **una sola cosa** cada una: la clave, el `source` o un milisegundo. Las tres dan un
`id` distinto, y los 74 bits de las filas 1 y 4 son **los mismos** (`7325-ac9b-67da707ec1d4`): solo cambia el tiempo.

## 3. Extensiones — perfil flux

CloudEvents restringe los nombres de extensión a **minúsculas y dígitos ASCII, sin
separadores**, con un máximo recomendado de 20 caracteres. De ahí `correlationid` y
no `correlation_id`. No es un capricho de estilo: es la especificación.

### 3.1 Obligatorias — y exigidas de verdad

> **Su ausencia es POISON.** Un SDK **DEBE** rechazar al parsear un evento al que le
> falte cualquiera de estas cuatro, igual que rechaza uno sin `id`.
>
> Esta regla se escribió tarde: la spec las llamaba obligatorias y **ningún parser las
> exigía**. Lo destapó el port a .NET, donde los tipos anulables obligan a decidir qué
> hacer con un `tenantid` ausente. Las tres opciones eran malas —marcarlas `required`
> y divergir de los otros SDKs, declararlas anulables y arrastrar comprobaciones de
> null por todas partes, o darles `""` y colapsar ausente con vacío (prohibido por
> §3.3)— y la raíz era que la spec no había decidido.
>
> **Por qué POISON y no un valor por defecto:** asumir un default es peligroso en las
> cuatro. Un `dataclassification` ausente tomado como `internal` hace circular PII con
> 30 días de retención en vez de 7 ([06-security.md §5](06-security.md)); un
> `tenantid` ausente tomado como `system` cruza fronteras de tenant (§4); un
> `correlationid` ausente rompe la trazabilidad sin que nadie lo note; un
> `producerversion` ausente impide acotar un bug de payload a un despliegue.
>
> Si no se exigieran, no serían obligatorias: serían recomendadas, y cada consumidor
> tendría que tratar el caso ausente — justo lo que declararlas obligatorias pretendía
> evitar.

**Ausente, `null` y `""` son lo mismo aquí.** §3.3 prohíbe darle significado propio a
un valor vacío, así que los tres producen el mismo error. Los códigos son estables y
todos los SDKs **DEBEN** devolver el mismo ante la misma entrada:

| Entrada | Código |
|---|---|
| Falta `tenantid` | `MISSING_REQUIRED_EXTENSION` |
| `"tenantid": null` | `MISSING_REQUIRED_EXTENSION` |
| `"tenantid": ""` | `MISSING_REQUIRED_EXTENSION` |
| `"tenantid": 42` | `WRONG_ATTRIBUTE_TYPE` |
| `"dataclassification": "secreto"` | `INVALID_DATACLASSIFICATION` |
| `"dataclassification": ""` | `MISSING_REQUIRED_EXTENSION` — vacío se evalúa antes que el enum |

El código acaba en la extensión `dlqerror` del evento en la DLQ y en las métricas. Si
dos SDKs emiten códigos distintos para el mismo fallo, agrupar por causa deja de
funcionar en cuanto el ecosistema es polyglot — que es siempre.

**Los constructores rechazan lo mismo que el parser, con el mismo código.** Un `id`, `source` o `dataschema` vacío
(`MISSING_REQUIRED_ATTRIBUTE`) o una extensión obligatoria vacía (`MISSING_REQUIRED_EXTENSION`) no sale de un
constructor del SDK: un evento que su propio parser rechazaría no se construye. Node los aceptaba y Rust los rechazaba sin
código. En petición/respuesta el conjunto de códigos es cerrado y el rechazo es `INVALID_RPC_CALL`
([04-errors.md §7](04-errors.md)).

| Extensión | Tipo | Descripción |
|---|---|---|
| `correlationid` | String | Identificador del flujo de negocio completo. Se propaga sin modificar a través de toda la cadena de eventos. Si un evento no nace de otro, el SDK lo inicializa con el `id` del propio evento. |
| `tenantid` | String | Tenant propietario del dato. `"system"` para eventos de plataforma. Base de las ACLs de [06-security.md](06-security.md). |
| `producerversion` | String | Versión SemVer del servicio emisor. Sin esto, un bug de payload en producción es imposible de acotar a un despliegue. |
| `dataclassification` | String | Uno de `public`, `internal`, `confidential`, `restricted`. Ver [06-security.md §5](06-security.md). |

### 3.2 Opcionales

| Extensión | Tipo | Descripción |
|---|---|---|
| `causationid` | String | `id` del evento concreto que provocó éste. `correlationid` responde "¿de qué flujo forma parte?"; `causationid` responde "¿quién lo causó exactamente?". |
| `partitionkey` | String | Clave de ordenación. Ver [03-delivery.md §5](03-delivery.md). Por convención, igual a `subject` (el id del agregado). |
| `traceparent` | String | W3C Trace Context. [CloudEvents Distributed Tracing extension](https://github.com/cloudevents/spec/blob/main/cloudevents/extensions/distributed-tracing.md). |
| `tracestate` | String | W3C Trace Context. |
| `dlqreason` | String | Solo presente en la DLQ. Lo añade el SDK. Ver [04-errors.md](04-errors.md). |
| `dlqattempts` | Integer | Solo presente en la DLQ. |

### 3.3 Ausente ≠ vacío

Un atributo opcional **DEBE** omitirse cuando no aplica, y **NO DEBE** usar un valor
vacío (`0`, `""`, `false`) con significado propio.

La razón es de interoperabilidad, no de estilo: `omitempty` de Go, el default de
`System.Text.Json` y varios serializadores más **colapsan cero y ausente**. Si
`dlqattempts` pudiese valer `0`, desaparecería del JSON al serializar en Go y
reaparecería como "ausente" al parsear en Node. Hoy no ocurre solo porque el mínimo
legal de `dlqattempts` resulta ser `1` — es decir, **el envelope depende de una
coincidencia en vez de una regla**.

La alternativa a esta regla son punteros y tipos anulables en Go, C# y Java para cada
opcional, que es exactamente la clase de fricción que un protocolo polyglot debe
evitar.

### 3.4 Prohibidas

Cualquier atributo de nivel raíz que no aparezca en §2 o §3. **Todo lo demás va
dentro de `data`.**

La razón es dura pero necesaria: las extensiones de CloudEvents solo admiten
`String`, `Integer`, `Boolean`, `Binary`, `URI` y `Timestamp` — **no admiten objetos
ni arrays**. Un equipo que empieza a colgar metadatos del nivel raíz acaba
inevitablemente serializando JSON dentro de un string, y a partir de ahí el envelope
deja de ser interoperable. Un SDK L2 **DEBE** rechazar en `publish()` cualquier
atributo raíz desconocido, en lugar de dejarlo pasar.

## 4. Payload (`data`)

- **DEBE** ser un objeto JSON. Ni array ni escalar en la raíz — para poder añadir
  campos después sin romper el esquema.
- **DEBE** usar `camelCase` en las claves. Elegido por coherencia con el envelope de
  CloudEvents y con JSON en general; la consistencia importa más que la elección.
- **DEBE** representar importes monetarios como **entero en la unidad mínima**
  (céntimos) junto a un campo de moneda ISO 4217. Nunca `float`.
- **DEBE** usar RFC 3339 UTC para fechas.
- **NO DEBE** contener `null` para significar "ausente". Se omite el campo.
- **DEBERÍA** ser autocontenido: el consumidor no debería necesitar llamar de vuelta
  al productor para actuar.

### 4.1 La forma canónica de `data`

Dos SDKs que reciben el **mismo** `data` deben escribir **los mismos bytes**: el orden de claves y los bytes son
normativos para el replay, la firma y la dedupe por hash (§6). Hasta aquí no lo eran: Rust guardaba `data` como texto
crudo y escribía lo que le daban —con espacios, con `\u00f1`, con una clave repetida—, y Node lo pasaba por
`JSON.parse` y `JSON.stringify`, que lo normaliza. La forma **canónica** es la de Node, la de `JSON.stringify` con UTF-8
literal:

- **Sin espacios** ni saltos de línea entre elementos.
- Las claves **en el orden en que se dan**; una clave **repetida** conserva la posición de la primera aparición y el valor
  de la última (`{"a":1,"b":3,"a":2}` es `{"a":2,"b":3}`).
- Las cadenas, **UTF-8 literal** salvo lo que ordena §1.1: `\u00f1` es `ñ`, `\/` es `/`, `\u007f` es DEL, un par
  sustituto escapado es el carácter; `"`, `\` y U+0000–U+001F siguen escapados.
- Los **números**, como los escribe cada lenguaje, **sin tocarlos** (§2.5): `4995` sale `4995`.

Un constructor reescribe lo que recibe a esta forma; un SDK que consume y reenvía conserva los bytes (§2.5). Node y Rust la
cumplen y la fijan los vectores `ajuste-data-*`; **pendiente en Python, Go, Java, .NET y PHP** (Fase 6): no se han medido contra
esos vectores y no se garantiza que escriban esta forma.

**Qué NO se admite en `data`** porque no tiene forma común entre lenguajes —no hay vector que lo iguale y **nada** garantiza que
dos SDKs coincidan—, así que si los bytes importan (firma, replay, dedupe por hash) **no lo uses**:

| Entrada | Qué pasa | Qué hacer |
|---|---|---|
| Un **entero fuera de ±(2^53 − 1)** (`9007199254740993`) | JavaScript lo **redondea** al `double` más cercano (`…992`); Rust lo conserva | Va como **cadena** (`"9007199254740993"`) o se parte en dos campos |
| Un **flotante** o un exponente (`4995.00`, `1E+2`, `1e21`, `-0`, `0.0`) | JavaScript lo reescribe (`4995`, `100`, `1e+21`, `0`, `0`); Rust y Python escriben `4995.00`, `1E+2`… | **El dinero va en enteros** (`totalCents`, §4): un importe no es un flotante |
| Una clave que es un **índice de array** (`"1"`, `"42"`) | JavaScript las ordena primero y en orden numérico, **por encima** del orden en que se dieron | Claves con letras (`camelCase`, §4) |
| Un **sustituto UTF-16 suelto** (`"\ud83d"`) | JavaScript lo admite y lo escribe escapado; Rust lo **rechaza** (no es UTF-8) | No hay texto con un sustituto suelto |
| Más de **128 niveles** de anidamiento | Rust lo rechaza; JavaScript admite más | `data` plano (§4: autocontenido, objetos poco profundos) |
| Un número **fuera del rango de un `double`** (`1e400`) | JavaScript lo lee como `Infinity` y `JSON.stringify` lo escribe `null`; Rust lo rechaza | No lo uses |

La tabla no es una lista de fallos de un SDK: son **diferencias de lenguaje** que ningún SDK puede evitar. Quien las necesite
tiene dos salidas: representarlas como cadena, o no firmar ni deduplicar por hash esos eventos.

```json
{
  "specversion": "1.0",
  "id": "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  "source": "/produccion/pedidos-api",
  "type": "com.flux.pedidos.pedido.creado.v1",
  "subject": "ped-123",
  "time": "2026-08-20T10:25:39.412Z",
  "datacontenttype": "application/json",
  "dataschema": "https://schemas.internal/pedidos/pedido/creado/1.2.0.json",
  "correlationid": "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  "causationid": "01924f8d-1a2b-7c3d-8e4f-5a6b7c8d9e01",
  "tenantid": "acme",
  "producerversion": "3.4.1",
  "dataclassification": "confidential",
  "partitionkey": "ped-123",
  "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
  "data": {
    "pedidoId": "ped-123",
    "clienteId": "cli-987",
    "totalCents": 9990,
    "moneda": "EUR",
    "lineas": [
      { "sku": "ABC-1", "cantidad": 2, "precioUnitarioCents": 4995 }
    ]
  }
}
```

## 5. Reglas de relleno automático

Un SDK L1 **DEBE** rellenar sin intervención del desarrollador:

| Atributo | Origen |
|---|---|
| `id` | UUIDv7 generado en `publish()` |
| `source` | Configuración de `connect()` |
| `time` | Reloj del sistema en UTC, salvo que se pase explícitamente |
| `specversion` | Constante `"1.0"` |
| `type` | Derivado del subject de NATS ([02-naming.md §2](02-naming.md)) |
| `dataschema` | Registro local subject → URI de esquema |
| `producerversion` | Configuración de `connect()` |
| `correlationid` | Heredado del evento en curso; si no hay, `= id` |
| `causationid` | `id` del evento en curso; ausente si no hay |
| `traceparent` | Contexto OpenTelemetry activo |

**El desarrollador solo escribe `subject`, `data` y opcionalmente `aggregateId`.**
Todo lo demás es responsabilidad del SDK. Si un desarrollador tiene que rellenar
`correlationid` a mano, el SDK ha fallado en su único trabajo.

## 6. Orden de los atributos — normativo

JSON no define orden de claves, pero flux sí lo necesita: **`data` va siempre el
último.** Los demás atributos van en el orden en que se declaran en §2 y §3.

Un objeto JSON con las mismas claves en otro orden es equivalente como dato y
**distinto como bytes**, y flux depende de la secuencia de bytes en cuatro sitios:

- El **replay verbatim** desde la DLQ deja de producir el mismo mensaje.
- Una **firma criptográfica** sobre el evento serializado (fase 4) no verifica.
- La **deduplicación por hash de contenido** ve dos eventos donde hay uno.
- Los **fixtures compartidos** entre SDKs dejan de comparar.

> Esta regla nació de una divergencia real. El SDK de Node construía el evento de DLQ
> con `{...event, dlq*}`, dejando las extensiones **después** de `data`; Python, Go y
> Java las ponían **antes**. El mismo evento, dos secuencias de bytes. La suite de
> conformidad no lo veía porque su fixture solo cubría el envelope normal — donde los
> cuatro SDKs sí coincidían.
>
> Poner `data` al final es además lo natural: los metadatos primero y el payload
> después, que es como se lee un mensaje.

## 7. Límite de tamaño

- El mensaje serializado **NO DEBE** superar **1 MiB**.
- Un SDK L2 **DEBE** fallar en `publish()` al superarlo, con un error accionable.
- Para payloads mayores, aplicar **claim-check**: subir el contenido a
  almacenamiento de objetos y publicar la referencia.

```json
"data": {
  "documentoId": "doc-778",
  "contenidoUri": "s3://facturas/2026/08/doc-778.pdf",
  "contenidoSha256": "e3b0c44298fc1c149afbf4c8996fb924...",
  "contenidoBytes": 4718592
}
```

El `sha256` no es decorativo: sin él, el consumidor no puede distinguir entre "el
fichero aún no está replicado" y "el fichero fue sustituido".
