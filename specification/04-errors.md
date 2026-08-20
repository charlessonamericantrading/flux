# 04 — Errores y DLQ

El error más caro de un sistema de eventos no es perder un mensaje. Es **reintentar
durante 51 minutos algo que nunca va a funcionar**, mientras el consumidor se atasca
y los eventos sanos se acumulan detrás.

Por eso flux no tiene "una política de reintentos". Tiene una **taxonomía**.

---

## 1. Las tres clases

| Clase | Significado | Acción NATS | ¿Reintenta? |
|---|---|---|---|
| **RETRYABLE** | El fallo es del entorno y podría desaparecer solo. | `nak(delay)` | Sí, con backoff |
| **PERMANENT** | El evento es válido pero este consumidor nunca podrá procesarlo. | `term()` + DLQ | **No** |
| **POISON** | El mensaje ni siquiera es interpretable. | `term()` + DLQ + alerta | **No** |

La distinción entre RETRYABLE y PERMANENT es **la decisión de diseño más importante
de un consumidor**. Equivocarla en un sentido atasca la cola; equivocarla en el otro
tira eventos buenos a la basura por un hipo de red.

### 1.1 RETRYABLE

El fallo no dice nada sobre el evento — dice algo sobre el mundo en este instante.

La clasificación se define por **semántica**, no por una lista de códigos:

| Categoría | Qué significa |
|---|---|
| Red no disponible o interrumpida | Conexión rechazada, reseteada, ruta inalcanzable, tubería rota |
| Resolución de nombres temporal | El resolutor dice explícitamente "reinténtalo", no "no existe". **Si la plataforma no distingue** (Java y .NET usan `UnknownHostException` tanto para NXDOMAIN como para SERVFAIL), trátalo como RETRYABLE: un dominio que de verdad no existe agotará su presupuesto y acabará en la DLQ igualmente |
| Rechazo por carga | HTTP 429, 502, 503, 504 |
| Contención en base de datos | Deadlock, lock timeout, pool agotado |
| Dependencia arrancando o desplegándose | Aún no acepta tráfico |

> **Los códigos concretos son ejemplos, no norma.** `ECONNRESET`, `ETIMEDOUT` y
> `EAI_AGAIN` son nombres de libuv: existen en Node, aparecen con prefijo `WSA` en
> Windows (`WSAECONNRESET`), y en Go `EAI_AGAIN` **no existe como errno** — el mismo
> fallo se expresa como `*net.DNSError` con `IsTemporary`.
>
> Un SDK **DEBE** usar el mecanismo idiomático de su plataforma para reconocer estas
> categorías. **NO DEBE** hacer `strings.Contains` sobre mensajes de error, que es
> justo lo que invita a hacer una lista de códigos tratada como normativa.
>
> Un port literal de la lista de Node produjo un bug real: en Windows el mismo corte
> de red se clasificaba PERMANENT y en Linux RETRYABLE.

→ `nak` con el backoff de [03-delivery.md §2](03-delivery.md). Tras agotar
`max_deliver`, JetStream deja de entregar y el SDK enruta a la DLQ.

### 1.2 PERMANENT

El evento es sintácticamente correcto, pero este consumidor no puede actuar sobre él
**por mucho que espere**.

- Falla la validación contra `dataschema`
- Regla de negocio que rechaza el hecho (`el pedido ya estaba cancelado`)
- Referencia a una entidad que no existe y no va a existir
- HTTP 400, 403, 404, 422 de una dependencia
- Versión de contrato no soportada por este consumidor

→ `term()` **inmediato** y publicación en la DLQ. Reintentar es puro desperdicio: 51
minutos de cola bloqueada para llegar al mismo sitio.

### 1.3 POISON

El SDK no logra siquiera construir un evento a partir del mensaje.

- JSON malformado
- Falta un atributo obligatorio de CloudEvents
- `specversion` desconocida
- `datacontenttype` no soportado

→ `term()` + DLQ + **alerta inmediata**. Un POISON casi siempre significa que un
productor está roto o que alguien publicó a mano en el subject equivocado. Es el único
caso que **DEBE** despertar a alguien.

## 2. Cómo se clasifica un error

Un SDK L2 **DEBE**:

1. Clasificar POISON él mismo, antes de invocar al handler.
2. Ofrecer un default razonable para errores del handler.
3. **Permitir que la aplicación anule la clasificación**, porque solo ella conoce sus
   dependencias.

### 2.1 El default para lo desconocido: RETRYABLE acotado

El default de flux es **RETRYABLE con presupuesto reducido: 2 entregas, no 6.**

Las dos opciones obvias fallan cada una en un extremo:

| Política | Transitorio desconocido | Sistemático desconocido |
|---|---|---|
| PERMANENT | ❌ A la DLQ por un hipo de red. Alguien reproduce a mano cada mañana. | ✅ Falla rápido |
| RETRYABLE completo | ✅ Se recupera solo | ❌ 51 min de cola atascada, y el modo de fallo **se amplifica** con cada mensaje siguiente |
| **RETRYABLE acotado (2)** | ✅ Se recupera en el 2º intento | ✅ A la DLQ en ~30 s |

El presupuesto acotado **domina a ambas**: cuesta 30 segundos de latencia sobre los
errores genuinamente permanentes, y a cambio elimina los dos modos de fallo. No es un
punto medio, es estrictamente mejor.

Implementación: el presupuesto **NO** se configura en el consumidor. `max_deliver` es
por consumidor, no por mensaje, y bajarlo a 2 recortaría también los reintentos de los
errores que sí sabemos que son transitorios. El SDK lleva la cuenta con `attempt` y
enruta a la DLQ cuando un error de clase desconocida supera su presupuesto, dejando
intactos los 6 intentos de los RETRYABLE reconocidos.

```
Error reconocido como transitorio (ECONNRESET, 503) → 6 entregas, hasta 51 min
Error desconocido                                   → 2 entregas, ~30 s
Error reconocido como permanente (400, 422)         → 1 entrega, sin espera
```

Un SDK L2 **DEBE** exponer esta política como configurable
(`permanent` / `retryable` / `retryable-bounded` + presupuesto), porque el equilibrio
correcto depende de cómo fallen las dependencias de cada ecosistema. El default es
`retryable-bounded` con presupuesto 2.

Un SDK L2 **DEBE** exponer errores tipados para que la aplicación señale la clase:

```
throw new RetryableError("proveedor de pagos 503", { retryAfterMs: 5000 })
throw new PermanentError("pedido ya cancelado", { code: "PEDIDO_YA_CANCELADO" })
```

> 🔨 **La función que traduce un error cualquiera a una de estas tres clases se
> implementa en `sdk-node/src/classify.ts`.** Es una decisión de política, no de
> infraestructura — ver la nota al final de ese fichero.

## 3. Formato del mensaje en DLQ

El mensaje de DLQ es **el CloudEvent original, íntegro**, con extensiones añadidas.
No se envuelve en otro sobre.

```json
{
  "specversion": "1.0",
  "id": "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  "source": "/produccion/pedidos-api",
  "type": "com.flux.pedidos.pedido.creado.v1",
  "time": "2026-08-20T10:25:39.412Z",
  "dataschema": "https://schemas.internal/pedidos/pedido/creado/1.2.0.json",
  "correlationid": "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  "tenantid": "acme",
  "producerversion": "3.4.1",
  "dataclassification": "confidential",
  "data": { "pedidoId": "ped-123", "totalCents": 9990 },

  "dlqreason": "permanent",
  "dlqattempts": 1,
  "dlqconsumer": "facturacion-api__pedidos_pedido_v1_creado",
  "dlqerror": "PEDIDO_YA_CANCELADO: el pedido ped-123 estaba cancelado",
  "dlqtime": "2026-08-20T10:25:40.117Z"
}
```

**Por qué el original íntegro y no un wrapper:** para reproducir un mensaje de la DLQ
solo hay que borrar las extensiones `dlq*` y republicar en el subject original. Con
un wrapper habría que desenvolver, y cada consumidor de la DLQ tendría que conocer dos
formatos. El replay se convierte en `jq 'del(.dlq*)'`.

`dlqattempts` es el número de entrega en que el evento murió, **no** una propiedad de
su clase. Un PERMANENT suele registrar `1` porque no gasta reintentos, pero si el
handler falló dos veces con errores transitorios y a la tercera lanzó un PERMANENT,
registra `3` — y eso es información útil, no una incoherencia. Un RETRYABLE agotado
registra siempre `max_deliver` (`6`).

Subject de destino: `dlq.<subject original>` — ver
[02-naming.md §3.1](02-naming.md).

```
pedidos.pedido.v1.creado  →  dlq.pedidos.pedido.v1.creado
```

## 4. La DLQ no es un cementerio

Una DLQ que nadie mira es una pérdida de datos con pasos extra. El protocolo exige:

- **DEBE** existir una alerta sobre `dlq.>` con umbral distinto por razón:
  `poison` → inmediata; `permanent` → agregada; `retryable` → por tasa.
- **DEBERÍA** revisarse la DLQ en cada ciclo operativo, no cuando algo se rompe.
- `max_age: 90d` en el stream de DLQ da margen forense, pero **es un límite real**: a
  los 90 días el evento desaparece.

### 4.1 Replay

```bash
# Inspeccionar sin consumir
nats stream view DLQ_PEDIDOS --subject 'dlq.pedidos.pedido.v1.creado'

# Reproducir: quitar extensiones dlq* y republicar en el subject original
nats stream get DLQ_PEDIDOS --last-for 'dlq.pedidos.pedido.v1.creado' --json \
  | jq '.data | @base64d | fromjson | del(.dlqreason, .dlqattempts, .dlqconsumer, .dlqerror, .dlqtime)' \
  | nats pub 'pedidos.pedido.v1.creado' --force-stdin
```

**Antes de reproducir, dos comprobaciones obligatorias:**

1. **¿Se ha arreglado la causa?** Reproducir contra el mismo bug devuelve el evento a
   la DLQ y ensucia el rastro.
2. **¿Sigue siendo idempotente el consumidor para este `id`?** El replay conserva el
   `id` original, así que si el consumidor llegó a aplicar un efecto parcial antes de
   fallar, la tabla de eventos procesados **DEBE** ser lo que impida duplicarlo. Si el
   handler no es idempotente, el replay es más peligroso que el fallo original.

> El replay **DEBE** conservar el `id` original. Regenerarlo rompe la idempotencia de
> todos los consumidores aguas abajo y convierte una recuperación en un incidente
> nuevo.
