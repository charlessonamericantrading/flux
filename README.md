<h1 align="center">flux</h1>
<p align="center"><em>Protocolo de integración polyglot: eventos, estado y petición/respuesta — en lugar de APIs, webhooks y polling.</em></p>

---

**La API pública de flux es el protocolo, no una librería.** Los SDKs son clientes
delgados e intercambiables; la especificación es el source of truth. Un servicio
escrito en cualquier lenguaje que hable este protocolo es un ciudadano de primera
clase del ecosistema, tenga o no un SDK oficial.

flux es un **protocolo de integración**: lo que hoy se resuelve con una API, un webhook o un polling entre tus sistemas tiene una primitiva —**evento**, **estado** o
**petición/respuesta**— y lo que no puede mantener una conexión NATS (un PHP de hosting compartido, un `cron`, el sistema de un tercero) habla con un **gateway** HTTP.
El dato llega solo cuando cambia, y los permisos de cada servicio se **generan** de un manifiesto en vez de escribirse a mano.

### ¿Qué primitiva uso?

| Hoy haces… | Con flux | Primitiva |
|---|---|---|
| Un webhook («te aviso de que pasó algo») · un polling `GET /cambios?desde=` | Publicas un **hecho** en pasado; cada interesado se suscribe | **Evento** (`publish` / `subscribe`) |
| `GET /clientes/123` · la carga inicial `GET /clientes?page=` | El dueño publica el **valor actual**; los demás lo observan (foto + cambios) | **Estado** (`state.put` / `watch` / `get`) |
| `POST /accion` que necesita respuesta ya · `GET /precio?…` (un cálculo) | Una **operación** que atiende el dueño del dominio | **Petición/respuesta** (`request` / `respond`) |
| `POST /accion` asíncrona (encolar trabajo) | Un evento de quien pide y otro de resultado, unidos por `correlationid` | **Evento** + evento |
| El webhook de un tercero · un webhook saliente · un PHP de hosting compartido · un `cron` | El **gateway** (HTTP solo en el borde) con los mismos permisos que un servicio | **Gateway** (`gateway/`) |

La regla que evita recrear el problema: si lo que pides es *un dato que otro sistema guarda*, es **estado**, no una petición. El árbol de decisión completo está en
[`AGENTS.md`](AGENTS.md), y **cómo migrar** —fila a fila, con el antes (HTTP) y el después (flux) en código que se ejecuta, el patrón gradual, cuándo no migrar y una lista
de comprobación de seguridad— en [`docs/migracion.md`](docs/migracion.md).

### Un ecosistema que funciona, con un comando

[`examples/ecosistema/`](examples/ecosistema/) es un CRM, una tienda, facturación y una web en PHP, **bajo la configuración segura** (TLS, una nkey por servicio, ACLs generadas, `flux provision`;
nada de `--dev`), con una prueba de extremo a extremo que recorre el camino completo y comprueba que **un servicio no puede leer lo que no se le concedió**:

```bash
NATS_SERVER=/ruta/a/nats-server node examples/ecosistema/run.mjs
```

Un contrato nuevo se crea con `flux new event|state|rpc <nombre>` (con su `$id` y lo obligatorio, y el bundle regenerado), no a mano.

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
| [`llms-full.txt`](https://raw.githubusercontent.com/charlessonamericantrading/flux/main/llms-full.txt) | **La spec completa en un fichero.** Si solo cargas uno, es este. | ~400 KB |
| [`llms.txt`](https://raw.githubusercontent.com/charlessonamericantrading/flux/main/llms.txt) | Índice curado con enlaces raw, por si prefieres cargar solo una parte | ~10 KB |
| [`AGENTS.md`](AGENTS.md) | Reglas accionables: qué primitiva usar, cómo publicar y consumir eventos, estado y peticiones, qué nunca hacer | ~38 KB |
| [`protocol.json`](https://raw.githubusercontent.com/charlessonamericantrading/flux/main/protocol.json) | Constantes verificables — regex de naming, defaults, taxonomía de errores. Para **validar**, no para recordar | ~160 KB |

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

`.cursor/rules/flux.mdc` y `.github/copilot-instructions.md` son **resúmenes** de `AGENTS.md`, y `node scripts/check-agent-docs.mjs` (CI) comprueba que **no divergen** de él (cada regla dura, con su titular, está en los dos). La fuente es una.

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
│   ├── 09-multitenancy.md  ← modelos de aislamiento entre tenants
│   ├── 10-state.md         ← estado replicado (extensión opcional)
│   ├── 11-request-reply.md ← petición/respuesta por core NATS (extensión opcional)
│   └── 12-edge.md          ← el borde HTTP: entrega saliente firmada e `id` determinista (extensión opcional)
├── schemas/                ← JSON Schemas versionados por evento
│   ├── <dominio>/<agregado>/<evento>/<semver>.json
│   ├── state/<dominio>/<agregado>/<semver>.json
│   └── rpc/<dominio>/<agregado>/<operacion>/{request,reply}/<semver>.json
├── cli/                    ← flux doctor · tail · dlq · validate · new · provision · state · call
├── gateway/                ← gateway HTTP de borde (Node, `node:http`): webhooks y sistemas sin conexión NATS
├── docs/migracion.md       ← guía de migración de APIs y webhooks a flux, fila a fila
├── examples/ecosistema/    ← un CRM, una tienda, facturación y una web en PHP, de extremo a extremo y bajo la configuración segura
├── conformance/            ← el contrato, ejecutable contra NATS real
├── docker-compose.yml      ← NATS JetStream local
├── examples/services.json  ← manifiesto de ownership → ACLs
├── scripts/                ← build-llms · check-compat · bundle-schemas · gen-acl · dev-secure · check-migracion
├── sdk-node/               ← fase 1
├── sdk-python/             ← fase 1
├── sdk-go/                 ← fase 1
├── sdk-java/               ← fase 2
├── sdk-dotnet/             ← fase 2
└── sdk-php/                ← fase 2
```

---

## Estado de los SDKs

| SDK | Nivel | Tests | Verificado | Capacidades |
|---|---|---|---|---|
| [Node / TypeScript](sdk-node/) | **L3** | 105 | ✅ | `signing` `security` `state` `rpc` |
| [Python](sdk-python/) | **L3** | 205 | ✅ | `signing` |
| [Go](sdk-go/) | **L3** | 162 | ✅ (también con `-race`) | `signing` |
| [Java](sdk-java/) | **L3** | 144 | ✅ (Maven en el runner) | `signing` |
| [.NET](sdk-dotnet/) | **L3** | 189 | ✅ | `signing` |
| [Rust](sdk-rust/) | L2 | 175 | ✅ (incl. 14 contra NATS real) | `signing` `security` `state` `rpc` |
| [PHP](sdk-php/) | **L3** | 304 | ✅ incl. 3 de integración contra NATS real | `signing` |

Los cinco primeros se compilan y ejecutan en CI en cada push. La suite de conformidad corre
aparte, contra un NATS real: **14/14**.

> **PHP.** Su adaptador de transporte sobre `basis-company/nats` **no está verificado
> contra un broker real**: el ecosistema PHP de NATS no está consolidado y el SDK aísla esa
> incertidumbre tras un puerto (`Flux\Transport\NatsTransport`), probado con un cliente
> falso. Todo lo demás —envelope, naming, clasificación y el runtime del consumidor— se
> prueba sin broker y está en verde. Ver [`sdk-php/README.md`](sdk-php/).

**Capacidades** son extensiones opcionales que un SDK declara **sin cambiar de nivel**
([`00-protocol.md` §5](specification/00-protocol.md)). La fuente es `protocol.json` →
`capabilities`, y `node scripts/check-sdk-codes.mjs` comprueba que esta columna la repite.

| Capacidad | Qué es | Un SDK sin ella… |
|---|---|---|
| `signing` | Firma Ed25519 del evento — [`07-signing.md`](specification/07-signing.md) | No firma ni verifica; sigue siendo conforme |
| `security` | Inbox privado `_INBOX.<servicio>`, TLS con CA propia, credenciales `.creds`/nkey, `NOT_PROVISIONED`/`PERMISSION_DENIED`, primera conexión que falla rápido y denegaciones visibles — [`06-security.md`](specification/06-security.md) | **No es compatible con la configuración segura**: con el inbox por defecto el broker le deniega su propio inbox y su primera petición falla |
| `state` | Estado replicado: `put`/`get`/`delete`/`watch` de un valor por clave, con CAS, lápidas y DLQ de estado — [`10-state.md`](specification/10-state.md) | No replica estado; sigue siendo conforme |
| `rpc` | Petición/respuesta: `request` y `respond` por core NATS, con `no responders`, grupo de cola, error clasificado y sin stream — [`11-request-reply.md`](specification/11-request-reply.md) | No hace ni atiende peticiones; sigue siendo conforme |

El **gateway de borde** ([`gateway/`](gateway/), [`12-edge.md`](specification/12-edge.md)) **no es una capacidad de SDK**: es un servicio que aloja
clientes HTTP como servicios flux normales (`"via": "gateway"` en el manifiesto, con sus propios permisos). Es normativo solo el formato de la
entrega saliente (`Flux-Signature`, `Idempotency-Key`) y el `id` determinista de un hecho importado ([`01-envelope.md` §2.6.1](specification/01-envelope.md)).

Hoy solo Node y Rust declaran `security`, `state` y `rpc`: los otros cinco salen **N/A** en las operaciones de estado y de
petición/respuesta del arnés cruzado (`node conformance/cross-sdk.mjs`), y uno que **declara** una capacidad y no tiene la
operación en su arnés **falla**. Un SDK no anuncia una capacidad hasta que la ha ejecutado contra un broker real.

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
| **v1.1 — Integración** | Seguridad ejecutada contra un broker real ✅ · estado replicado ✅ · petición/respuesta ✅ · gateway de borde ✅ · ejemplo de ecosistema, guía de migración y `flux new` ✅ · medición reproducible (`bench/`) — en curso · paridad de los otros cinco SDKs (Fase 6) — pendiente | ✅ / ⏳ |
