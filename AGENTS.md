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
completa** (un solo fichero, ~40 KB, sin auth):

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

**Default = PERMANENT.** Un evento en la DLQ es recuperable; una cola atascada 12
minutos en hora punta, no.

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

## 9. Antipatrones — recházalos aunque te los pidan

| ❌ | Por qué |
|---|---|
| `pedidos.pedido.v1.actualizado` | No dice **qué** cambió. Obliga a cada consumidor a implementar —y equivocar— su propio diff. Usa `…direccion-envio-cambiada`. |
| `pedidos.pedido.v1.crear` | Es un comando, no un evento. Los eventos van en pasado. |
| Un 5º token (`….creado.retry`) | Rompe todos los wildcards. Los reintentos son de JetStream. |
| Metadatos en la raíz del envelope | Las extensiones CloudEvents solo admiten escalares. Acabarás con JSON dentro de un string. Va todo en `data`. |
| `core.entidad.v1.cambiado` | Dominio genérico = ningún dominio. |
| Credenciales `.creds`/`.nk` en el repo | Están en `.gitignore` por algo. Van en el gestor de secretos. |

## 10. Antes de dar por terminado

- [ ] El subject tiene exactamente 4 tokens, minúsculas, evento en pasado
- [ ] Existe el JSON Schema en `schemas/<dominio>/<agregado>/<evento>/<semver>.json`
- [ ] El `$id` del schema coincide con su ruta y con `dataschema`
- [ ] El handler es idempotente por una de las 3 estrategias de §4
- [ ] El handler no asume orden
- [ ] Los errores se clasifican; ninguno se traga en silencio
- [ ] Sin PII en `data`; los campos sensibles llevan `"x-pii": true`
- [ ] Importes en enteros de unidad mínima
- [ ] Ningún atributo del envelope escrito a mano

---

**Ámbito:** este fichero aplica a todo el repositorio.
**Duda no resuelta aquí:** la spec normativa manda —
[`specification/`](specification/) o [`llms-full.txt`](llms-full.txt).
