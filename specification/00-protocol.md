# flux — Event Protocol v1

> **Estado:** Draft normativo
> **Versión:** 1.0.0
> **Última revisión:** 2026-08-20

---

## 1. Propósito

Este documento define el contrato que **todo** productor y consumidor de eventos del
ecosistema debe cumplir, con independencia del lenguaje en que esté escrito.

El objetivo explícito es que **añadir un lenguaje nuevo al ecosistema sea un trabajo
de días, no de meses**. Eso solo es posible si la totalidad de la semántica vive en
el protocolo y ninguna vive en los SDKs.

## 2. Lenguaje normativo

Las palabras **DEBE**, **NO DEBE**, **DEBERÍA**, **NO DEBERÍA** y **PUEDE** se
interpretan según [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119).

## 3. Modelo de capas

Cada capa depende únicamente de la inmediatamente inferior. Una capa **NO DEBE**
filtrar detalles hacia arriba.

| # | Capa | Responsabilidad | ¿Reemplazable? |
|---|---|---|---|
| 4 | **Aplicación** | Lógica de negocio, handlers | — |
| 3 | **SDK** | Serialización, acks, retries, contexto, DLQ | Sí, por lenguaje |
| 2 | **Protocolo** | Envelope, naming, semántica de entrega, errores | No sin RFC |
| 1 | **Binding** | Mapeo protocolo ↔ broker concreto | Sí, por broker |
| 0 | **Broker** | Transporte y persistencia (NATS JetStream) | Sí |

La consecuencia práctica: sustituir NATS por Kafka es un cambio en las capas 0–1.
Las capas 2–4 no se enteran. **Ese desacoplamiento es la razón de ser de flux**; si
un SDK expone un tipo de NATS en su API pública, es un bug del SDK.

## 4. Qué es y qué no es un evento en flux

Un evento es **un hecho consumado del pasado, inmutable, emitido por el dueño del
dato.**

**DEBE** cumplir:
- Nombrarse en pasado (`creado`, `cancelado`, `enviado`), nunca en imperativo.
- Ser publicado únicamente por el servicio propietario del agregado.
- Contener suficiente información para que un consumidor razonable actúe sin tener
  que llamar de vuelta al productor.
- Ser válido para siempre: reprocesar un evento de hace un año **DEBE** seguir
  siendo posible.

**NO DEBE** usarse como:
- **Comando disfrazado.** `pedidos.pedido.v1.crear` no es un evento. Si necesitas
  request/reply, usa NATS Core request/reply — está fuera del alcance de v1.
- **Transporte de blobs.** Si el payload supera **1 MiB**, publica una referencia
  (URI + checksum), no el contenido. Ver [01-envelope.md §7](01-envelope.md).
- **Sustituto de una tabla.** Un evento no es una fila. Un evento es un cambio.

## 5. Niveles de conformidad

Un SDK declara su nivel. Los consumidores del SDK saben así qué esperar.

### L1 — Publisher/Subscriber
- **DEBE** producir CloudEvents 1.0 válidos según [01-envelope.md](01-envelope.md).
- **DEBE** rellenar automáticamente `id`, `source`, `time`, `specversion`, `type`.
- **DEBE** usar ack explícito (nunca auto-ack).
- **DEBE** establecer la cabecera `Nats-Msg-Id` = `id` del CloudEvent.

### L2 — Resiliente
L1, más:
- **DEBE** implementar la taxonomía de errores de [04-errors.md](04-errors.md).
- **DEBE** aplicar el backoff canónico y enrutar a DLQ al agotarlo.
- **DEBE** propagar `correlationid` y `traceparent` desde el evento entrante al
  saliente sin intervención del desarrollador.
- **DEBE** reconectar con backoff exponencial y jitter.

### L3 — Gobernado
L2, más:
- **DEBE** resolver y validar `dataschema` antes de publicar, y **fallar el
  `publish()`** si el payload no cumple su esquema.
- **DEBE** reportar TODOS los errores de validación, no solo el primero: de uno en
  uno, arreglar un payload con tres campos mal cuesta tres despliegues.
- **DEBERÍA** ofrecer un modo `warn` para introducir validación en un ecosistema en
  marcha sin romper nada el primer día.
- **PUEDE** validar también al consumir. Un fallo ahí se clasifica **PERMANENT**: el
  evento es sintácticamente correcto pero incumple su contrato, y reintentarlo dará
  exactamente el mismo resultado.

#### Resolución de esquemas: bundle, no HTTP

El `dataschema` es una URI, pero un SDK L3 **NO DEBE** resolverla por red en
`publish()`. Validar está en la ruta caliente: una petición por evento es
inaceptable, y una caché con TTL abre una ventana en la que dos servicios validan
contra versiones distintas del mismo esquema.

En su lugar, los esquemas se empaquetan y se despliegan **con el servicio**
([`scripts/bundle-schemas.mjs`](../scripts/bundle-schemas.mjs)). Así la versión del
esquema queda clavada a la versión del servicio — que es justo lo que
`producerversion` promete poder acotar.

El bundle resuelve además el `dataschema` exacto: dentro de un mayor todo es
BACKWARD-compatible, así que el MINOR más alto acepta todo lo que aceptan los
anteriores.

> **Nota de implementación.** Los esquemas de flux declaran `$schema: draft/2020-12`.
> Un validador configurado para draft-07 no falla con un error de versión: falla con
> `no schema with key or ref ".../draft/2020-12/schema"`, que no dice nada útil.
> En `ajv` hay que usar `ajv/dist/2020`, no el export por defecto; en Rust, el crate
> `jsonschema`; en PHP, `opis/json-schema` 2.4+; en Python, `jsonschema` 4.18+ eligiendo
> el validador por el `$schema` de cada esquema; en Go, `santhosh-tekuri/jsonschema/v6`.

Los SDKs de fase 1 apuntan a **L2**; los de **Node, Python, Go, Rust y PHP** implementan ya
**L3** de forma opt-in. El coste de L3 se paga solo si se usa: en Rust va detrás de la
feature `validation`, en PHP de una dependencia opcional de Composer y en Python del extra
`[validation]` con importación diferida, así que un servicio que se conforma con L2 no
arrastra el validador.

> **Go es la excepción, y conviene decirlo.** No existe allí la figura de la dependencia
> opcional, así que el validador va en `go.mod` y lo paga también quien se quede en L2
> (~1 MB de binario). En ejecución sigue sin costar nada: con el modo `off` no se compila
> ni un esquema. Es una limitación del ecosistema, no una decisión del protocolo.

## 6. Fuera del alcance de v1

Declarado explícitamente para evitar scope creep:

- Request/reply y RPC.
- Sagas y orquestación de procesos de larga duración.
- Protobuf y Avro (opt-in por subject en v2).
- Cifrado a nivel de campo.
- Federación multi-cluster.

## 7. Índice normativo

| Documento | Contenido |
|---|---|
| [01-envelope.md](01-envelope.md) | Atributos CloudEvents, extensiones, payload |
| [02-naming.md](02-naming.md) | Subjects, types, streams, durable consumers |
| [03-delivery.md](03-delivery.md) | JetStream, acks, retries, idempotencia, orden |
| [04-errors.md](04-errors.md) | Taxonomía de errores, DLQ, replay |
| [05-compatibility.md](05-compatibility.md) | Evolución de esquemas, deprecación |
| [06-security.md](06-security.md) | Accounts, ACLs, clasificación de datos |
| [07-signing.md](07-signing.md) | Firma Ed25519 — extensión **opcional** |
| [08-observability.md](08-observability.md) | Métricas, trazas y alertas — normativo para L2 |
| [09-multitenancy.md](09-multitenancy.md) | Modelos de aislamiento entre tenants |
