<h1 align="center">flux</h1>
<p align="center"><em>Event Protocol v1 — el contrato de eventos polyglot del ecosistema.</em></p>

---

**La API pública de flux es el protocolo, no una librería.** Los SDKs son clientes
delgados e intercambiables; la especificación es el source of truth. Un servicio
escrito en cualquier lenguaje que hable este protocolo es un ciudadano de primera
clase del ecosistema, tenga o no un SDK oficial.

```
   Node · Python · Go · Java · .NET · Rust · PHP
                      │
              ┌───────▼────────┐
              │  flux SDK (L2) │   ← delgado: infraestructura, no negocio
              └───────┬────────┘
                      │
              ┌───────▼────────┐
              │ EVENT PROTOCOL │   ← esto es el producto
              │       v1       │
              └───────┬────────┘
                      │
              ┌───────▼────────┐
              │ NATS JetStream │   ← reemplazable sin tocar aplicaciones
              └────────────────┘
```

---

## Decisiones fundacionales (cerradas)

| Decisión | Elección | Documento |
|---|---|---|
| Envelope | **CloudEvents 1.0** + perfil de extensiones internas | [01-envelope.md](specification/01-envelope.md) |
| Transporte | **NATS JetStream**, structured mode | [03-delivery.md](specification/03-delivery.md) |
| Naming | `<dominio>.<agregado>.v<major>.<evento>` | [02-naming.md](specification/02-naming.md) |
| Versionado | **Mayor en el subject, minor/patch en `dataschema`** | [05-compatibility.md](specification/05-compatibility.md) |
| Entrega | **at-least-once**, idempotencia obligatoria en consumidor | [03-delivery.md](specification/03-delivery.md) |
| Formato | JSON + JSON Schema (Protobuf opt-in por subject en v2) | [01-envelope.md](specification/01-envelope.md) |
| Errores | Taxonomía de 3 clases → retry / DLQ / poison | [04-errors.md](specification/04-errors.md) |

Estas decisiones son **normativas**. Cambiarlas requiere un RFC y un bump del
protocolo a v2.

---

## Estructura

```
.
├── specification/          ← source of truth. Todo lo demás se deriva de aquí.
│   ├── 00-protocol.md      ← visión general, capas, niveles de conformidad
│   ├── 01-envelope.md      ← CloudEvents + extensiones obligatorias
│   ├── 02-naming.md        ← subjects, types, streams, consumers
│   ├── 03-delivery.md      ← JetStream, acks, retries, idempotencia
│   ├── 04-errors.md        ← taxonomía de errores y DLQ
│   ├── 05-compatibility.md ← reglas de evolución de esquemas
│   └── 06-security.md      ← accounts, ACLs, clasificación de datos
├── schemas/                ← JSON Schemas versionados por evento
│   └── <dominio>/<agregado>/<evento>/<semver>.json
├── sdk-node/               ← fase 1
├── sdk-python/             ← fase 1
└── sdk-go/                 ← fase 1
```

---

## Niveles de conformidad

Un SDK **no** necesita implementarlo todo para ser útil:

- **L1 — Publisher/Subscriber.** Publica y consume CloudEvents válidos con acks
  explícitos. Suficiente para integrar un servicio.
- **L2 — Resiliente.** L1 + backoff, DLQ, clasificación de errores, propagación
  de `traceparent`.
- **L3 — Gobernado.** L2 + validación contra Schema Registry en publish, y
  rechazo de eventos que violen su `dataschema`.

Los SDKs de fase 1 (Node, Python, Go) apuntan a **L2**. L3 llega con el Schema
Registry en fase 4.

---

## Ejemplo mínimo (semántica idéntica en todos los lenguajes)

```
bus.publish("pedidos.pedido.v1.creado", { pedidoId: "123", totalCents: 9990 })

bus.subscribe("pedidos.pedido.v1.creado", async (evento, ctx) => {
  if (await yaProcesado(evento.id)) return ctx.ack()   // idempotencia obligatoria
  await crearFactura(evento.data)
  await marcarProcesado(evento.id)
})
```

El SDK rellena `id`, `source`, `time`, `specversion`, `type`, `dataschema` y
propaga `correlationid` / `traceparent`. El desarrollador nunca los escribe a mano.

---

## Roadmap

| Fase | Alcance | Estado |
|---|---|---|
| **1 — Core** | Especificación v1, JetStream, SDK Node/Python/Go a nivel L2 | 🚧 en curso |
| **2 — Cobertura** | SDK Java, .NET, Rust, PHP | ⏳ |
| **3 — Operación** | CLI `flux tail`, replay desde DLQ, métricas | ⏳ |
| **4 — Gobierno** | Schema Registry, validación L3, ACLs multi-tenant | ⏳ |
