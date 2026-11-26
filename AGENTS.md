# AGENTS.md — flux Event Protocol v1

> Instrucciones canónicas para agentes de IA (Claude Code, Codex, Cursor, Grok,
> Replit, Copilot, Aider, Zed, Windsurf, Jules…) que escriban código que **publique o
> consuma eventos** de este ecosistema.
>
> Este fichero es la fuente de verdad para agentes. `CLAUDE.md`,
> `.cursor/rules/flux.mdc` y `.github/copilot-instructions.md` son punteros a esto.

---

## 0. Carga el contrato antes de escribir código

Si vas a tocar código que publica o consume eventos, **primero descarga la spec
completa** (un solo fichero, ~100 KB, sin auth):

```
https://raw.githubusercontent.com/charlessonamericantrading/flux/main/llms-full.txt
```

Constantes verificables en JSON (regex de naming, defaults de consumidor, extensiones
obligatorias) — úsalo para **validar**, no para recordar:

```
https://raw.githubusercontent.com/charlessonamericantrading/flux/main/protocol.json
```

Índice curado por si prefieres cargar solo una parte:
[`llms.txt`](llms.txt) · Documentos individuales: [`specification/`](specification/)

---

## 1. Reglas duras (violarlas rompe el ecosistema)

1. **Nunca inventes un subject.** Formato exacto, 4 tokens:
   `<dominio>.<agregado>.v<major>.<evento>` — minúsculas, `kebab-case`,
   dominio plural, agregado singular, evento en **pasado**.
   ✅ `pedidos.pedido.v1.creado` ❌ `pedidos.crear-pedido` ❌ `Pedidos.Pedido.V1.Creado`

2. **Nunca escribas el envelope a mano.** El SDK rellena `id`, `source`, `time`,
   `specversion`, `type`, `dataschema`, `correlationid`, `causationid`,
   `producerversion`, `traceparent`. Si tu código asigna alguno de estos
   manualmente, está mal.
   El desarrollador solo escribe: **subject, `data`, y opcionalmente `aggregateId`**.

3. **Todo consumidor DEBE ser idempotente.** La garantía es *at-least-once*: los
   duplicados llegan, no son un fallo. Un handler sin deduplicación está roto aunque
   los tests pasen. Ver §4.

4. **Nunca asumas orden.** flux no garantiza orden ni global ni por agregado. Usa
   `aggregateVersion` y un `WHERE aggregate_version < $n`. Ver §5.

5. **Nunca metas PII en `data`.** Publica referencias (`clienteId`), no valores
   (`email`, `nombre`, `dni`). Un stream de JetStream es una base de datos persistente
   sujeta a RGPD, y un log append-only no puede satisfacer el derecho de supresión.

6. **Nunca modifiques un JSON Schema ya publicado.** Son inmutables. Se añade una
   versión nueva. Ver §6.

7. **Importes monetarios: enteros en la unidad mínima** (`totalCents: 9990`) más
   moneda ISO 4217. Nunca `float`.

8. **`data` va SIEMPRE el último atributo**, en el envelope normal y en el de DLQ. El
   orden de claves es normativo: flux depende de la secuencia de **bytes** para el
   replay verbatim, la firma y la deduplicación por hash.

9. **Las cuatro extensiones obligatorias se exigen de verdad.** Un evento sin
   `correlationid`, `tenantid`, `producerversion` o `dataclassification` es **POISON**.
   No hay defaults: asumir `internal` para una clasificación ausente hace circular PII
   con 30 días de retención en vez de 7.

10. **Los tipos son exactos.** `{"tenantid": 42}` es POISON, no el tenant `"42"`.
    Desactiva la coerción de tu deserializador.

11. **UTF-8 literal, sin escapes `\u`.** `café`, no `café`. Ambos son JSON válido
    y **no son los mismos bytes**.

**Estado replicado** (extensión opcional, [10-state.md](specification/10-state.md)):

12. **Un dato que guarda otro sistema es estado, no una llamada.** `state.watch` / `state.get`,
    nunca un `GET` por HTTP ni polling. Solo el **dueño** escribe (`put`, `delete`).
13. **`put` exige `aggregateVersion` (entero ≥ 1) y es condicional.** Una versión que no avanza da
    `stale`, que **no** es un error. Una clave borrada solo se recrea con una versión mayor que la
    de su lápida.
14. **El dato personal solo vive en estado**: clasificado `confidential`, con `x-pii` en el
    esquema, y borrable con `delete`. `restricted` no se admite. En un **evento**, la regla 5
    sigue intacta. Un consumidor **DEBE** borrar su copia local al recibir una lápida.
15. **`dlq`, `state`, `rpc` y `system` no son dominios.** `publish()` los rechaza
    (`RESERVED_DOMAIN`).

**Petición/respuesta** (extensión opcional, [11-request-reply.md](specification/11-request-reply.md)):

16. **Una orden o un cálculo con respuesta inmediata es una petición; un dato guardado es estado.**
    `bus.request` / `bus.respond`, nunca un `POST` o un `GET` por HTTP entre servicios propios. **Si lo que pides es un
    dato que otro sistema guarda, es estado, no petición/respuesta.**
17. **El que responde deduplica por el `id` de la petición**, y un reintento reutiliza el mismo `id`.
    `RPC_TIMEOUT` **no** dice que la operación no se ejecutó.
18. **Una suscripción por operación, al subject exacto** (`rpc.<dominio>.<agregado>.v<major>.<operacion>`, operación
    en **infinitivo**), con el servicio como grupo de cola. Nunca `rpc.<dominio>.>`. **Ningún stream captura `rpc.>`.**
19. **Quien llama traduce lo que ve**: `no responders` → `RPC_UNAVAILABLE`, sin respuesta → `RPC_TIMEOUT`, un error →
    `RetryableError` / `PermanentError`. Timeout **5 s** por defecto, **25 s** como máximo.

**¿Qué primitiva uso?**

| Necesito… | Uso |
|---|---|
| Avisar de que algo pasó; que cada interesado reaccione | **Evento** (`bus.publish`) |
| Un dato que guarda otro sistema; la carga inicial; una copia local al día | **Estado** (`state.get` / `state.watch`) |
| Una **orden** que necesita respuesta ya; un **cálculo** con parámetros | **Petición/respuesta** (`bus.request` / `bus.respond`) |
| Encolar trabajo sin esperar | **Evento** del que pide + un evento de resultado, unidos por `correlationid` |

---

## 2. Publicar un evento

```ts
await bus.publish("pedidos.pedido.v1.creado", {
  pedidoId: "ped-123",
  clienteId: "cli-987",
  aggregateVersion: 1,
  totalCents: 9990,
  moneda: "EUR",
  lineas: [{ sku: "ABC-1", cantidad: 2, precioUnitarioCents: 4995 }],
}, { aggregateId: "ped-123" });
```

Reglas:
- Solo el **servicio dueño del agregado** publica en su dominio. El broker lo aplica
  por ACL: publicar en un dominio ajeno es rechazado en el servidor, no en tu código.
- Payload > **1 MiB** → claim-check: publica `{ uri, sha256, bytes }`, no el contenido.
- `data` **DEBE** ser un objeto JSON en la raíz. Ni array ni escalar.
- Claves en `camelCase`. Sin `null` para "ausente" — se omite el campo.

## 3. Consumir un evento

```ts
await bus.subscribe("pedidos.pedido.v1.creado", async (evento, ctx) => {
  if (await yaProcesado(evento.id)) return ctx.ack();   // OBLIGATORIO
  await hacerElTrabajo(evento.data);
  await marcarProcesado(evento.id);
  return ctx.ack();
});
```

- Ack **explícito** siempre. Nunca auto-ack.
- Un handler que tarde > 30 s (`ack_wait`) provoca **reentrega concurrente del mismo
  evento**. Si el trabajo es largo, el SDK debe emitir work-in-progress; si tu handler
  bloquea 30 s sin ceder control, es un bug de la aplicación.

## 4. Idempotencia — elige UNA de las tres

| Estrategia | Cuándo | Cómo |
|---|---|---|
| **A. Tabla de procesados** | El efecto vive en tu BD transaccional | `INSERT` del `event_id` con PK única **en la misma transacción** que el efecto |
| **B. Operación naturalmente idempotente** | Siempre que sea posible — la mejor | `SET estado='confirmado'` ✅ · `SET intentos = intentos + 1` ❌ |
| **C. Clave de idempotencia externa** | El efecto es una API de terceros | `Idempotency-Key: <evento.id>` — **derivada del id del evento**, nunca generada en el handler |

Nunca dependas de `duplicate_window` de JetStream para esto: esa ventana deduplica
**publicaciones**, no reentregas de consumo. Es el malentendido más frecuente.

## 5. Orden — no lo pidas, diséñalo fuera

```json
"data": { "pedidoId": "ped-123", "aggregateVersion": 7, "estado": "confirmado" }
```
```sql
UPDATE pedidos SET estado = $2, aggregate_version = $3
 WHERE id = $1 AND aggregate_version < $3;
```

Convierte el orden de un problema de infraestructura a una condición del `WHERE`.
Incluye `aggregateVersion` en todo evento de cambio de estado.

## 6. Versionado

| Cambio | Resultado |
|---|---|
| Añadir campo **opcional**, relajar restricción, deprecar | MINOR en `dataschema`. Mismo subject. |
| Eliminar/renombrar campo, cambiar tipo, opcional→requerido, endurecer restricción, **cambiar unidades o semántica** | **MAYOR** → subject nuevo `…v2.…` + doble publicación ≥ 90 días |

⚠️ Cambiar semántica sin cambiar forma (euros → céntimos) **pasa toda validación
automática**. Si detectas uno, márcalo como breaking aunque el esquema valide.

Enums: solo se pueden añadir valores si están marcados `"x-extensible-enum": true`, y
el consumidor **DEBE** tener rama por defecto no destructiva (log + ack, nunca throw).

## 7. Errores — clasifica, no reintentes a ciegas

| Clase | Qué es | Acción |
|---|---|---|
| `RETRYABLE` | Timeout, ECONNRESET, HTTP 429/502/503/504, deadlock | `nak` + backoff |
| `PERMANENT` | Falla el schema, regla de negocio lo rechaza, HTTP 400/403/404/422 | `term()` + DLQ **inmediato** |
| `POISON` | JSON malformado, falta atributo CloudEvents obligatorio | `term()` + DLQ + **alerta** |

**Default para lo DESCONOCIDO = RETRYABLE con presupuesto acotado de 2 entregas**
(04-errors.md §2.1). Domina a las dos alternativas: un transitorio se recupera en el
segundo intento, y un sistemático llega a la DLQ en ~30 s sin atascar la cola.
Los RETRYABLE **reconocidos** (503, ECONNRESET) conservan sus 6 intentos: el
presupuesto es por error, no por consumidor.

Tiempo hasta la DLQ de un RETRYABLE reconocido: **51 min 30 s**.

```ts
throw new RetryableError("proveedor 503", { retryAfterMs: 5000 });
throw new PermanentError("pedido ya cancelado", { code: "PEDIDO_YA_CANCELADO" });
```

DLQ: `dlq.<subject original>` — **prefijo, no sufijo** (un sufijo encajaría con
`pedidos.>` y el stream principal se tragaría sus propios muertos).

---

## 8. Trampas específicas de NATS que fallan en silencio

Estas no dan error: dan comportamiento incorrecto. Un agente las comete casi siempre.

| Trampa | Realidad |
|---|---|
| `subject` de CloudEvents ≠ subject de NATS | El primero es el **id del agregado** (`"ped-123"`), el segundo la **dirección de enrutado**. En la API del SDK se llaman `aggregateId` y `subject`. |
| Puntos en nombres de stream o durable | NATS **no los admite**. `EVT_PEDIDOS`, no `EVT.PEDIDOS`. `facturacion-api__pedidos_pedido_v1_creado`, no con puntos. |
| `duplicate_window` deduplica reintentos | **No.** Solo deduplica publicaciones dentro de 2 min. Nunca sustituye a la idempotencia. |
| `ack_wait` y `backoff` son independientes | **No: el servidor sobrescribe `ack_wait` con `backoff[0]` sin avisar.** Verificado en NATS 2.14.5: pides `ack_wait: 30s` + `backoff: [1s, …]` y obtienes `ack_wait: 1s`, sin error. Cualquier handler de más de un segundo se ejecuta en concurrencia consigo mismo. **`backoff[0]` DEBE ser el presupuesto de duración del handler** — por eso el backoff canónico empieza en `30s`, no en `1s`. |
| `max_deliver` y `backoff` descuadrados | `max_deliver: 6` con 5 entradas de backoff = 1 entrega + 5 reintentos. Si `max_deliver` fuese 5, la última entrada nunca se aplica. |
| El servidor devuelve lo que le pides | No siempre. Tras crear un consumidor, **compara la config devuelta con la solicitada y falla en alto si difieren**. |
| Subjects case-insensitive | **Son sensibles.** `Pedidos.` ≠ `pedidos.` crea un subject fantasma sin suscriptores y sin error. |
| Regenerar el `id` al hacer replay | Rompe la idempotencia de todos los consumidores aguas abajo. El replay **conserva** el `id`. |
| `nak(delay)` controla el calendario de reintentos | **Solo el primero.** Con `backoff` configurado —y flux lo configura siempre— el servidor honra el delay únicamente en la primera reentrega; después manda el array `backoff` y lo ignora **sin avisar**. Un `Retry-After: 5` de un proveedor acorta el primer reintento y nada más. |
| Publicar por core NATS | Un `publish` de **core** a un subject que ningún stream captura **no da error** y el evento se evapora. Por JetStream sí falla. Publica **siempre** por JetStream. |
| Los permisos de subject gobiernan JetStream | **No.** Los SDKs consumen por *pull*: piden mensajes por `$JS.API.CONSUMER.MSG.NEXT.…` y los reciben en su inbox, no por el subject del evento. Un `subscribe: ["pedidos.>"]` no deja consumir nada (sin `$JS.API.*` ni siquiera se puede consultar el stream), y un `$JS.API.>` «para arreglarlo» deja leer y crear consumidores sobre cualquier dominio. Lo que confina son los permisos **exactos** de cada durable: genéralos con `scripts/gen-acl.mjs`, no los escribas a mano. |
| `_INBOX.>` común a todos los servicios | **Cualquiera lo lee todo.** Las entregas de un pull consumer viajan por el inbox: con `_INBOX.>` compartido, un servicio sin concesión sobre `inventario` leyó un evento completo de `inventario` (medido en NATS 2.14.5). Cada servicio usa **`_INBOX.<servicio>.>`**, fijado por el SDK a partir del nombre de servicio. |
| Una credencial o un TLS mal puestos fallan con un error | **No siempre.** `waitOnFirstConnect` (Node) y `retry_on_initial_connect` (Rust) reintentan **cualquier** error: un certificado que no verifica deja `connect()` colgado, o el servicio arrancado y mudo. `connect()` espera a un broker caído pero **falla al instante** ante lo que esperar no arregla ([03-delivery.md §6.1](specification/03-delivery.md)). Un `ack` o un pull denegados tampoco lanzan: el evento se reentrega para siempre ([06-security.md §3.7](specification/06-security.md)). |
| Los formateadores ISO por defecto sirven para `time` | No. Go recorta ceros (`.41Z`), Python da microsegundos y `+00:00`. Formatea explícitamente: **exactamente 3 decimales y sufijo `Z`**. |
| La clave y el tenant de un estado van en el sobre | **Van en el subject**: `state.<dom>.<agr>.v<M>.<tenant>.<clave>`, y `tenantid` y `subject` del sobre **DEBEN** coincidir (si no, POISON `STATE_SUBJECT_MISMATCH`). El broker **no valida la clave**: acepta `*`, `a.b` (dos tokens: la entrada queda fuera de todo filtro, sin error) y 2000 caracteres. Valídala tú: `^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`, con coincidencia **completa**: en Python y PHP el `$` acepta un `\n` final (medido). |
| `put` solo publica | **Es condicional**: lee la secuencia (`DIRECT.GET`), no escribe si la versión no avanza y publica con `Nats-Expected-Last-Subject-Sequence`. Un `publish` de core a un subject de estado sobrescribe **sin chequeo ni acuse**. El conflicto de secuencia tiene **dos** códigos: `10071` (un nodo) y `10164` (cluster, bajo carrera): reconoce los dos o no reintentarás en producción. Reintenta con **el mismo `id`**. |
| El estado se recupera desde la DLQ | **No se reproduce**: `dlq.state.>` no lleva el dato (`data` es `{ key, revision }`) y republicar un valor viejo pisaría uno nuevo. `flux dlq replay` lo rechaza; la recuperación es `flux state resync`. |
| Un estado con PII es «lo mismo que un evento con PII» | No: el estado va `confidential` con `x-pii` y **se puede borrar** (`delete` sustituye el valor por una lápida). Pero el borrado **físico** en disco y las **claves** y **tenants** (visibles en `STREAM.INFO`) no los cubre: la clave es un id opaco, nunca un email. |
| Una petición a algo que no escucha nadie espera su timeout | **No**: `no responders` llega en ~1 ms (A-56)… **si la conexión lo pidió** en CONNECT (`headers` y `no_responders`); sin ello, silencio y timeout (A-57). Y con una suscripción `rpc.<dominio>.>` el servicio *sí* tiene interés en lo que no atiende: timeout (A-64). Una suscripción **por operación, exacta**. |
| Un stream que captura `rpc.>` es inocuo | **Contesta a cada petición con su acuse** `{"stream":…,"seq":n}`, y casi siempre antes que la respuesta real (394 de 400, A-67). El que responde ejecuta igualmente y el stream **guarda cada petición**, datos personales incluidos. Un filtro `>` solo se crea sin acuse y convierte `no responders` en un timeout. `flux doctor` lo comprueba. |
| `RPC_TIMEOUT` = la operación no ocurrió | **Ambiguo**: un responder lento la ejecuta y su respuesta llega tarde al inbox (A-61); uno que muere con la petición la pierde (A-62); la que sale justo al irse el último, en un cluster, también (A-70). Por eso se deduplica por `id`. |
| Una petición denegada por la ACL falla rápido | Con `@nats-io/*` sí (~1 ms, A-66); con **`async-nats` (Rust) solo da un timeout** y el `-ERR` por el evento de la conexión (A-72). El SDK escucha el aviso y lanza `PERMISSION_DENIED`. |
| `allow_responses: true` basta y es estrecho | Admite **una** respuesta por petición y **no caduca** (A-68). Acótalo: `{ max: 1, expires: 30s }`. Sin él, el dueño no puede contestar y quien llama ve un timeout. |
| Una respuesta de más de 1 MiB falla con un error | **Falla en silencio**: la librería lanza en quien responde y quien llama solo ve un timeout (A-65). El que responde comprueba el tamaño y contesta `RPC_MESSAGE_TOO_LARGE`. |
| Parar un responder cerrando la conexión | **Pierde lo que atiende** (A-62). Dejar de escuchar, esperar a lo que está en vuelo y cerrar. |
| Dos servicios que atienden la misma operación se reparten el trabajo | Cada **grupo de cola** recibe **una copia**: los dos la ejecutan y quien llama ve una sola respuesta (A-60). Solo el dueño responde. |

> Las tres primeras trampas de JetStream comparten forma: **el servidor acepta la
> petición, no devuelve error, y aplica otra cosa.** Ninguna se detecta leyendo código;
> solo midiendo.

## 9. Extensiones (fases 4 y 5)

Opt-in. Un servicio que no las use sigue siendo conforme, pero si el proyecto ya las
tiene activas, tu código debe respetarlas.

**Capacidades por SDK:** cada SDK declara las que tiene (`signing`, `security`, `state`, `rpc`: columna *Capacidades* del [`README.md`](README.md)), y uno sin `security` no es compatible con las ACLs de `gen-acl.mjs` ([00-protocol.md §5](specification/00-protocol.md)). Uno sin `state` no puede escribir ni leer estado replicado, y uno sin `rpc` no hace ni atiende peticiones.

### Validación L3 — [00-protocol.md §5](specification/00-protocol.md)

`publish()` valida el payload contra su JSON Schema y **falla si no cumple**. Así un
contrato roto es un fallo del productor y no un misterio en un consumidor de otro
equipo la semana que viene.

- El bundle (`schemas/bundle.json`) se despliega **con el servicio**; nunca se resuelve
  el `dataschema` por HTTP en la ruta caliente.
- Tras cambiar un esquema: `node scripts/bundle-schemas.mjs`.
- Los esquemas declaran **draft 2020-12**. Un validador de draft-07 no da un error de
  versión: da `no schema with key or ref …/2020-12/schema`, que no dice nada.
- El bundle también resuelve el **`dataschema` exacto** de cada subject; sin él, el SDK
  solo puede asumir el `<major>.0.0`.
- El validador es una **dependencia opcional** con importación diferida (Node: `ajv`;
  Python: extra `[validation]`). Excepción: en Go no existe esa figura y va en `go.mod`.
- Un fallo al **consumir** se clasifica **PERMANENT**, y en Python/Go lo declara el propio
  error en vez de dejarlo en manos de la política de errores desconocidos.

### Estado replicado — [10-state.md](specification/10-state.md)

El **valor actual** de un agregado, que el dueño publica y los demás observan: sustituye a
`GET /clientes/123`, a la carga inicial y al polling de datos. Un evento dice *qué pasó*; el
estado lleva *el dato*.

```ts
await bus.state.put("clientes.cliente.v1", "cli-1",
  { clienteId: "cli-1", nombre: "Ana", email: "ana@example.com", aggregateVersion: 7 });
await bus.state.delete("clientes.cliente.v1", "cli-1", { aggregateVersion: 8 });
const actual = await bus.state.get("clientes.cliente.v1", "cli-1");       // entrada | null
await bus.state.watch("clientes.cliente.v1", async (entrada) => {         // foto + cambios
  if (entrada.deleted) return borrarLocal(entrada.tenantId, entrada.key);
  await guardarLocal(entrada);                  // UPDATE … WHERE aggregate_version < $n
}, { onSynced: () => listo() });
```

- Subject `state.<dom>.<agr>.v<M>.<tenant>.<clave>`: **6 tokens**, tenant y clave **dentro**. Un
  stream `STATE_<DOM>` guarda **un valor por clave**; `delete` publica una **lápida** (con su
  versión) que lo sustituye.
- **Tenant explícito**: sin valor por defecto (ni `system` implícito). Una clave que no cumple el
  patrón es `INVALID_STATE_KEY`, no algo que se escape. El tenant de `put`/`get`/`delete` sale de la
  opción, del evento en curso y de `connect()`; el de `watch`, solo de la opción y de `connect()`.
- `onSynced` se invoca **una vez** por `watch`: al terminar la foto inicial, o de inmediato si no hay nada.
- **Siete códigos**: `INVALID_STATE_CONTRACT`, `INVALID_TENANT_FOR_STATE`, `INVALID_STATE_KEY`,
  `MISSING_AGGREGATE_VERSION`, `RESERVED_DOMAIN` (PERMANENT), `STATE_SUBJECT_MISMATCH` (POISON) y
  `STATE_CONFLICT` (RETRYABLE) — [04-errors.md §6](specification/04-errors.md).
- La DLQ de estado (`dlq.state.<dom>.<agr>.v<M>`) **no lleva el dato**, y el estado **no se
  reproduce** desde ella.
- Métrica `flux_state_writes_total{contract,outcome}`; **nunca** la clave ni el tenant como
  etiqueta. Esquemas en `schemas/state/…`, con `aggregateVersion` obligatoria.

### Petición/respuesta — [11-request-reply.md](specification/11-request-reply.md)

Una **orden o un cálculo con respuesta inmediata** entre dos servicios, por core NATS: no se persiste, no tiene stream ni
DLQ. **Ningún SDK la declara todavía**: la spec y las herramientas llegan antes que las implementaciones.

```ts
const r = await bus.request("clientes.cliente.v1.calcular-descuento",
  { clienteId: "cli-1", totalCents: 9990, moneda: "EUR" }, { timeoutMs: 2000 });      // → el `data` de la respuesta

await bus.respond("clientes.cliente.v1.calcular-descuento", async (peticion, ctx) => {
  return { descuentoCents: 500, moneda: peticion.moneda };          // lanzar PermanentError / RetryableError para fallar
});
```

- Subject `rpc.<dominio>.<agregado>.v<M>.<operacion>`: **5 tokens**; el **nombre** de la operación (el que recibe la API) tiene
  4, sin `rpc.`, y la operación va en **infinitivo** (`obtener`, `calcular-precio`), no en pasado. Sin stream.
- **Sobre**: el CloudEvent de siempre. La respuesta **hereda** `correlationid` y `tenantid` y su `causationid` es el `id` de la
  petición. `type`: `com.flux.rpc.<dom>.<agr>.<op>.request.v<M>` / `.reply.v<M>`. **Sin cabeceras** de NATS.
- **Error**: `com.flux.rpc.error.v1` con `data: { class, code, message, retryAfterMs? }`; `class` es `retryable` o
  `permanent` (`poison` no viaja). Un error **desconocido** del handler es `permanent`.
- **Seis códigos**: `RPC_UNAVAILABLE`, `RPC_TIMEOUT` (RETRYABLE), `STALE_REQUEST`, `RPC_INVALID_REPLY`,
  `RPC_MESSAGE_TOO_LARGE`, `INVALID_RPC_CALL` (PERMANENT) — [04-errors.md §7](specification/04-errors.md).
- Con la firma en `require`, el que responde rechaza una petición cuyo `time` se aleje **más de 300 s** (`STALE_REQUEST`).
- Métricas `flux_rpc_client_requests_total{operation,outcome}`, `flux_rpc_server_requests_total{operation,outcome}` y
  `flux_rpc_duration_seconds{operation,role}`; **nunca** el tenant, el `id` ni quién llama como etiqueta. Esquemas en
  `schemas/rpc/<dom>/<agr>/<op>/{request,reply}/…`: un **par**; en una petición añadir un valor a un enum no exige
  `x-extensible-enum`, en una respuesta sí (la dirección se invierte).

### Firma Ed25519 — [07-signing.md](specification/07-signing.md)

- Se firma `serialize(evento sin `signature` y sin extensiones `dlq*`)`. **No hay
  canonicalización aparte**: es el mismo `serialize()` del productor, y funciona porque
  el orden de claves, el UTF-8 literal y el formato de `time` están fijados.
- `signkeyid` **va dentro de lo firmado**. Fuera, un atacante lo cambiaría para que la
  verificación buscara otra clave.
- Una clave **retirada sigue verificando** mientras se conserve su pública. Retirarla
  impide *emitir*, no *verificar* lo ya emitido.
- Generar claves: `flux keygen <servicio> <n>`.

### Métricas — [08-observability.md](specification/08-observability.md)

Los nombres y etiquetas son **contrato**, no decisión de cada servicio. Siete métricas
`flux_*` (más `flux_state_writes_total` en un SDK con la capacidad `state`, y las tres `flux_rpc_*` en uno con `rpc`).

- **Nunca** etiquetes por `tenantid`, `id` ni `correlationid`: un tenant nuevo no debe
  crear series temporales nuevas. En **trazas** sí.
- `code` es un identificador estable (`HTTP_503`), **nunca** el mensaje de error: su
  cardinalidad es infinita y tumba el almacenamiento.

### Multi-tenant — [09-multitenancy.md](specification/09-multitenancy.md)

- El default (filtrado en consumidor) **no resiste a un servicio legítimo
  comprometido**. Si necesitas esa garantía, es account por tenant (Modelo B).
- `tenantIsolation: "strict"` hace que suscribirse sin filtro sea un error de
  configuración. Un filtro opcional es un filtro que alguien olvidará, y el fallo —ver
  datos de otro tenant— no produce ningún error.
- `tenantid` se propaga sin modificar por la cadena: un evento derivado pertenece al
  tenant del que lo causó, no al del servicio que lo emite.
- Un evento **NO DEBE** contener datos de más de un tenant. Es el fallo más difícil de
  deshacer: ya está mal en el stream y la única reparación es purgarlo.

---

## 10. Antipatrones — recházalos aunque te los pidan

| ❌ | Por qué |
|---|---|
| `pedidos.pedido.v1.actualizado` | No dice **qué** cambió. Obliga a cada consumidor a implementar —y equivocar— su propio diff. Usa `…direccion-envio-cambiada`. |
| `pedidos.pedido.v1.crear` | Es un comando, no un evento. Los eventos van en pasado. |
| Un 5º token (`….creado.retry`) | Rompe todos los wildcards. Los reintentos son de JetStream. |
| Metadatos en la raíz del envelope | Las extensiones CloudEvents solo admiten escalares. Acabarás con JSON dentro de un string. Va todo en `data`. |
| `core.entidad.v1.cambiado` | Dominio genérico = ningún dominio. |
| Credenciales `.creds`/`.nk` en el repo | Están en `.gitignore` por algo. Van en el gestor de secretos. |
| Meter el dato personal en un evento «porque el consumidor lo necesita» | Es lo que el **estado** resuelve: el evento lleva `clienteId`, el estado lleva el dato, clasificado y borrable. |
| Leer un dato de otro servicio con `GET` por HTTP | Es **estado** (`state.get` / `state.watch`), no petición/respuesta. |
| Una operación `obtener-cliente-por-email` que devuelve lo que otro guarda | Es una consulta a un almacén: **estado**. La petición/respuesta es para órdenes y cálculos. |
| `rpc.clientes.cliente.v1.creado` (un evento donde va una orden) | La operación va en **infinitivo** (`crear`); el pasado es de un evento. |
| Suscribirse a `rpc.<dominio>.>` «para atenderlo todo» | Convierte `no responders` en un timeout. Un subject exacto por operación. |

## 11. Antes de dar por terminado

- [ ] El subject tiene exactamente 4 tokens, minúsculas, evento en pasado
- [ ] Existe el JSON Schema en `schemas/<dominio>/<agregado>/<evento>/<semver>.json`
- [ ] El `$id` del schema coincide con su ruta y con `dataschema`
- [ ] El handler es idempotente por una de las 3 estrategias de §4
- [ ] El handler no asume orden
- [ ] Los errores se clasifican; ninguno se traga en silencio
- [ ] Sin PII en `data`; los campos sensibles llevan `"x-pii": true`
- [ ] Importes en enteros de unidad mínima
- [ ] Ningún atributo del envelope escrito a mano
- [ ] `data` es el último atributo, también en el evento de DLQ
- [ ] Las cuatro extensiones obligatorias están presentes y con el tipo correcto
- [ ] Si tocaste un esquema: `node scripts/bundle-schemas.mjs` y
      `node scripts/check-compat.mjs`
- [ ] Si tocaste `AGENTS.md` o `specification/`: `node scripts/build-llms.mjs`
- [ ] Ninguna métrica etiquetada por `tenantid`, `id` ni `correlationid`
- [ ] Si escribes **estado**: `aggregateVersion` ≥ 1 en `data` y en el esquema (`schemas/state/…`);
      el dato personal con `x-pii` y `confidential`; el consumidor borra su copia al recibir una lápida
- [ ] Ningún dominio llamado `dlq`, `state`, `rpc` ni `system`; ninguna clave o tenant como etiqueta de métrica
- [ ] Si haces o atiendes **peticiones**: la operación en infinitivo con su **par** `request`/`reply` en `schemas/rpc/…`; el
      handler **deduplica por el `id`** de la petición; el timeout ≤ 25 s; los errores traducidos a `RETRYABLE` / `PERMANENT`;
      ninguna métrica etiquetada por tenant, `id` ni quién llama; ningún stream captura `rpc.>` (`flux doctor`)

---

**Ámbito:** este fichero aplica a todo el repositorio.
**Duda no resuelta aquí:** la spec normativa manda —
[`specification/`](specification/) o [`llms-full.txt`](llms-full.txt).
