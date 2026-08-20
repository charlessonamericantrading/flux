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

## 2. Atributos de contexto — núcleo CloudEvents

| Atributo | Tipo | Obl. | Regla flux |
|---|---|:--:|---|
| `specversion` | String | ✅ | **DEBE** ser exactamente `"1.0"`. |
| `id` | String | ✅ | **DEBE** ser un [UUIDv7](https://www.rfc-editor.org/rfc/rfc9562) en formato canónico. Lo genera el SDK. |
| `source` | URI-ref | ✅ | **DEBE** ser `/<entorno>/<servicio>`. Ej.: `/produccion/pedidos-api`. |
| `type` | String | ✅ | Reverse-DNS. Ver [02-naming.md §2](02-naming.md). |
| `time` | Timestamp | ✅ | RFC 3339 en **UTC**, precisión de milisegundos. **DEBE** ser el instante en que ocurrió el hecho, no el de publicación. |
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

### 2.2 Unicidad

`id` + `source` **DEBEN** ser únicos en su conjunto. Esa pareja es la clave de
deduplicación del ecosistema entero. Como `id` es UUIDv7 (monotónico en el tiempo),
ordenar por `id` dentro de un mismo `source` equivale a ordenar por instante de
generación — útil al reconstruir historiales desde la DLQ.

## 3. Extensiones — perfil flux

CloudEvents restringe los nombres de extensión a **minúsculas y dígitos ASCII, sin
separadores**, con un máximo recomendado de 20 caracteres. De ahí `correlationid` y
no `correlation_id`. No es un capricho de estilo: es la especificación.

### 3.1 Obligatorias

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

### 3.3 Prohibidas

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

## 6. Límite de tamaño

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
