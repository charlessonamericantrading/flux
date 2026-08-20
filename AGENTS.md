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
| Los formateadores ISO por defecto sirven para `time` | No. Go recorta ceros (`.41Z`), Python da microsegundos y `+00:00`. Formatea explícitamente: **exactamente 3 decimales y sufijo `Z`**. |

> Las tres primeras trampas de JetStream comparten forma: **el servidor acepta la
> petición, no devuelve error, y aplica otra cosa.** Ninguna se detecta leyendo código;
> solo midiendo.

## 9. Extensiones (fases 4 y 5)

Opt-in. Un servicio que no las use sigue siendo conforme, pero si el proyecto ya las
tiene activas, tu código debe respetarlas.

### Validación L3 — [00-protocol.md §5](specification/00-protocol.md)

`publish()` valida el payload contra su JSON Schema y **falla si no cumple**. Así un
contrato roto es un fallo del productor y no un misterio en un consumidor de otro
equipo la semana que viene.

- El bundle (`schemas/bundle.json`) se despliega **con el servicio**; nunca se resuelve
  el `dataschema` por HTTP en la ruta caliente.
- Tras cambiar un esquema: `node scripts/bundle-schemas.mjs`.
- Los esquemas declaran **draft 2020-12**. Un validador de draft-07 no da un error de
  versión: da `no schema with key or ref …/2020-12/schema`, que no dice nada.

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
`flux_*`.

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

---

**Ámbito:** este fichero aplica a todo el repositorio.
**Duda no resuelta aquí:** la spec normativa manda —
[`specification/`](specification/) o [`llms-full.txt`](llms-full.txt).
