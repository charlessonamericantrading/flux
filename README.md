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

## 🤖 Para agentes de IA

Este repo está diseñado para que **cualquier agente cargue el contrato entero en una
sola descarga**, sin auth y sin clonar.

| Recurso | Qué es | Tamaño |
|---|---|---|
| [`llms-full.txt`](https://raw.githubusercontent.com/charlessonamericantrading/flux/main/llms-full.txt) | **La spec completa en un fichero.** Si solo cargas uno, es este. | ~55 KB |
| [`llms.txt`](https://raw.githubusercontent.com/charlessonamericantrading/flux/main/llms.txt) | Índice curado con enlaces raw, por si prefieres cargar solo una parte | ~4 KB |
| [`AGENTS.md`](AGENTS.md) | Reglas accionables: cómo publicar, cómo consumir, qué nunca hacer | ~9 KB |
| [`protocol.json`](https://raw.githubusercontent.com/charlessonamericantrading/flux/main/protocol.json) | Constantes verificables — regex de naming, defaults, taxonomía de errores. Para **validar**, no para recordar | ~8 KB |

**Pégale esto a tu agente:**

```
Lee https://raw.githubusercontent.com/charlessonamericantrading/flux/main/llms-full.txt
y sigue ese protocolo para publicar y consumir eventos.
```

**O por línea de comandos:**

```bash
curl -sL https://raw.githubusercontent.com/charlessonamericantrading/flux/main/llms-full.txt -o flux-spec.txt
```

### Convenciones soportadas

| Fichero | Lo leen |
|---|---|
| [`AGENTS.md`](AGENTS.md) | **Fuente canónica.** Codex, Cursor, Zed, Aider, Jules, Devin, Windsurf, Amp |
| [`CLAUDE.md`](CLAUDE.md) | Claude Code, Claude Desktop |
| [`.cursor/rules/flux.mdc`](.cursor/rules/flux.mdc) | Cursor |
| [`.github/copilot-instructions.md`](.github/copilot-instructions.md) | GitHub Copilot |
| [`llms.txt`](llms.txt) / [`llms-full.txt`](llms-full.txt) | Cualquier agente con acceso web — Grok, ChatGPT, Replit, Gemini, Perplexity |

Los tres últimos son **punteros finos** a `AGENTS.md`, no copias. La fuente es una.

> `llms-full.txt` está **generado**. Tras editar `AGENTS.md` o `specification/*.md`:
> `node scripts/build-llms.mjs`. CI rechaza el PR si diverge.

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
├── AGENTS.md               ← instrucciones canónicas para agentes de IA
├── CLAUDE.md               ← puntero fino a AGENTS.md
├── llms.txt                ← índice curado (llmstxt.org)
├── llms-full.txt           ← GENERADO: spec completa en un fichero
├── protocol.json           ← constantes verificables para validación/codegen
│
├── specification/          ← source of truth. Todo lo demás se deriva de aquí.
│   ├── 00-protocol.md      ← visión general, capas, niveles de conformidad
│   ├── 01-envelope.md      ← CloudEvents + extensiones obligatorias
│   ├── 02-naming.md        ← subjects, types, streams, consumers
│   ├── 03-delivery.md      ← JetStream, acks, retries, idempotencia
│   ├── 04-errors.md        ← taxonomía de errores y DLQ
│   ├── 05-compatibility.md ← reglas de evolución de esquemas
│   ├── 06-security.md      ← accounts, ACLs, clasificación de datos
│   ├── 07-signing.md       ← firma Ed25519 (extensión opcional)
│   ├── 08-observability.md ← métricas, trazas y alertas
│   └── 09-multitenancy.md  ← modelos de aislamiento entre tenants
├── schemas/                ← JSON Schemas versionados por evento
│   └── <dominio>/<agregado>/<evento>/<semver>.json
├── cli/                    ← flux doctor · tail · dlq · validate
├── conformance/            ← el contrato, ejecutable contra NATS real
├── docker-compose.yml      ← NATS JetStream local
├── examples/services.json  ← manifiesto de ownership → ACLs
├── scripts/                ← build-llms · check-compat · bundle-schemas · gen-acl
├── sdk-node/               ← fase 1
├── sdk-python/             ← fase 1
├── sdk-go/                 ← fase 1
├── sdk-java/               ← fase 2
├── sdk-dotnet/             ← fase 2
└── sdk-php/                ← fase 2
```

---

## Estado de los SDKs

| SDK | Nivel | Tests | Verificado |
|---|---|---|---|
| [Node / TypeScript](sdk-node/) | **L3** | 105 | ✅ |
| [Python](sdk-python/) | **L3** | 205 | ✅ |
| [Go](sdk-go/) | **L3** | 162 | ✅ (también con `-race`) |
| [Java](sdk-java/) | **L3** | 144 | ✅ (Maven en el runner) |
| [.NET](sdk-dotnet/) | **L3** | 189 | ✅ |
| [Rust](sdk-rust/) | L2 | 175 | ✅ (incl. 14 contra NATS real) |
| [PHP](sdk-php/) | **L3** | 304 | ✅ incl. 3 de integración contra NATS real |
| [PHP](sdk-php/) | L2 | 201 | ✅ (transporte no — ver abajo) |

Los cinco primeros se compilan y ejecutan en CI en cada push. La suite de conformidad corre
aparte, contra un NATS real: **14/14**.

> **PHP.** Su adaptador de transporte sobre `basis-company/nats` **no está verificado
> contra un broker real**: el ecosistema PHP de NATS no está consolidado y el SDK aísla esa
> incertidumbre tras un puerto (`Flux\Transport\NatsTransport`), probado con un cliente
> falso. Todo lo demás —envelope, naming, clasificación y el runtime del consumidor— se
> prueba sin broker y está en verde. Ver [`sdk-php/README.md`](sdk-php/).

---

## Niveles de conformidad

Un SDK **no** necesita implementarlo todo para ser útil:

- **L1 — Publisher/Subscriber.** Publica y consume CloudEvents válidos con acks
  explícitos. Suficiente para integrar un servicio.
- **L2 — Resiliente.** L1 + backoff, DLQ, clasificación de errores, propagación
  de `traceparent`.
- **L3 — Gobernado.** L2 + validación del payload contra su `dataschema` en `publish()`,
  que **falla la publicación** si no cumple, reportando **todos** los errores y no solo el
  primero.

Node, Python, Go, Java y .NET implementan **L3** de forma opt-in: el default es `off` y
entonces se comportan exactamente como L2, sin pagar ni la dependencia del validador. Los
esquemas **no** se resuelven por red: se empaquetan con `node scripts/bundle-schemas.mjs` y
se despliegan con el servicio, así que la versión del esquema queda clavada a la del
servicio.

> En Java la dependencia del validador es `<optional>true</optional>` dentro del mismo
> artefacto; en .NET, donde toda `PackageReference` es transitiva, "opcional" solo puede
> expresarse partiendo el paquete, y por eso la validación vive en `Flux.Validation`.

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
| **1 — Core** | Especificación v1, JetStream, SDK Node/Python/Go a nivel L2 | ✅ |
| **2 — Cobertura** | Java · .NET · Rust · PHP | ✅ |
| **3 — Operación** | CLI `flux`: doctor, tail, triaje y replay de DLQ | ✅ |
| **4 — Gobierno** | Validación L3, verificador de compatibilidad, generador de ACLs | ✅ |
| **5 — Confianza** | Firma Ed25519 ✅ · observabilidad ✅ · multi-tenant ✅ | ✅ |
