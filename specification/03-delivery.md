# 03 — Semántica de entrega

> **Garantía del protocolo: at-least-once.**
> Todo consumidor **DEBE** ser idempotente. No es una recomendación: es la condición
> de admisión al ecosistema.

---

## 1. La garantía, dicha en voz alta

flux entrega cada evento **al menos una vez**. Eso significa, literalmente:

- Un consumidor **verá duplicados**. No "podría": los verá.
- Los duplicados **no son un fallo**. Son el precio de no perder eventos.
- Un consumidor que asume unicidad **está roto**, aunque hoy funcione.

La alternativa —exactly-once— no existe de forma honesta en un sistema distribuido
con consumidores que tocan bases de datos externas. Lo que se vende como
exactly-once es siempre at-least-once más deduplicación en algún punto. flux pone
esa deduplicación **en el consumidor**, porque es el único lugar donde hay contexto
de negocio para decidir qué significa "el mismo evento".

## 2. Configuración canónica de consumidor

```
ack_policy:       explicit
ack_wait:         30s
max_deliver:      6
backoff:          [30s, 1m, 5m, 15m, 30m]
max_ack_pending:  256
deliver_policy:   all
replay_policy:    instant
```

> ✅ Verificado contra NATS Server 2.14.5. Ver
> [`conformance/cases/consumer-config.json`](../conformance/cases/consumer-config.json).

`max_deliver: 6` con 5 entradas de backoff = **1 entrega inicial + 5 reintentos**.
Los dos números tienen que cuadrar: si `max_deliver` fuese 5, la última entrada del
backoff (`30m`) no se aplicaría nunca y la configuración mentiría sobre su propio
comportamiento.

**Tiempo hasta la DLQ: entre 51 min 30 s y ~54 min.** El límite inferior es
`sum(backoff)` = 3090 s, y se da cuando el handler falla rápido: el `nak` es inmediato
y solo se espera el backoff. El límite superior añade hasta un `ack_wait` (30 s) por
intento, cuando el handler no falla sino que **agota el plazo** sin responder.
`protocol.json` publica el límite inferior (`totalTimeToDlqSeconds: 3090`) por ser el
determinista.

Ese número es una decisión de producto, no un detalle técnico: es cuánto tiempo estás
dispuesto a que un evento transitorio siga reintentando antes de que un humano se
entere. Es largo a propósito — los errores que llegan aquí son **solo** los RETRYABLE,
y la mayoría de fallos transitorios se resuelven solos. Un PERMANENT no gasta ni un
reintento ([04-errors.md](04-errors.md)).

### 2.1 ⚠️ `backoff[0]` **es** `ack_wait` — el servidor lo sobrescribe en silencio

Esta es la trampa más cara de JetStream y no aparece en ningún error:

```
Solicitado:  ack_wait = 30s,  backoff = [1s, 5s, 30s, 2m, 10m]
Efectivo:    ack_wait = 1s    ← el servidor lo reemplaza por backoff[0]
```

**El servidor acepta la petición, no avisa, y devuelve una configuración distinta de
la enviada.** Comprobado contra NATS 2.14.5 vía `$JS.API.CONSUMER.DURABLE.CREATE`.

La consecuencia es grave. Con `ack_wait` efectivo de 1 s, cualquier handler que tarde
más de un segundo —es decir, cualquier handler que escriba en una base de datos y
llame a una API— recibe **el mismo mensaje reentregado mientras aún se está
ejecutando**. Ejecución concurrente del mismo evento, en cada mensaje, bajo carga.

**Regla derivada:** `backoff[0]` **DEBE** ser el presupuesto de duración del handler,
porque es literalmente el `ack_wait`. De ahí que la config canónica empiece en `30s` y
no en `1s`. Un primer reintento rápido es imposible por construcción, y buscarlo es lo
que rompe la configuración.

Un SDK L2 **DEBE**:
- Establecer `ack_wait == backoff[0]` explícitamente, para que la config declarada
  coincida con la efectiva.
- **Verificar la config devuelta por el servidor** tras crear el consumidor y fallar
  en alto si difiere de la solicitada. Es la única defensa contra este tipo de
  sobrescritura silenciosa.
- Enviar `WIP` (work-in-progress) automáticamente cada `ack_wait / 2` mientras el
  handler siga vivo — esto sí extiende el plazo.
- Documentar que un handler que supere los 30 s sin emitir WIP es un bug de la
  aplicación.

### 2.2 ⚠️ Con `backoff` configurado, el delay de un `nak` solo cuenta la primera vez

La segunda trampa de la misma familia, y también silenciosa. Medido contra NATS 2.14.5
pidiendo `nak(300ms)` en **todas** las reentregas:

```
SIN backoff:  0ms → 300ms → 600ms → 900ms          ← el delay se honra siempre
CON backoff:  0ms → 300ms → 5300ms → 15300ms       ← solo la primera vez
              (backoff configurado: 300ms, 5s, 10s)
```

A partir de la segunda reentrega el servidor manda el array `backoff` y **el delay
solicitado se ignora sin ningún aviso**.

**Y flux configura `backoff` siempre**, así que la consecuencia es directa: un
`Retry-After: 5` de un proveedor **acorta el primer reintento y nada más**. Los
siguientes seguirán el backoff canónico (1 m, 5 m, 15 m, 30 m).

Por eso `Classification.retryAfterMs` es una **sugerencia para el primer reintento**,
no un control del calendario de reintentos. Un SDK **NO DEBE** documentarla como si
sobrescribiera el backoff, ni construir lógica que dependa de que se respete más allá
de la primera vez.

> Las tres trampas de JetStream que el protocolo ha tenido que documentar comparten
> forma: **el servidor acepta la petición, no devuelve error, y aplica otra cosa.**
> `ack_wait` reemplazado por `backoff[0]` (§2.1), el delay del `nak` ignorado (§2.2), y
> un `publish` de core a un subject sin stream que se evapora
> ([02-naming.md §1.1](02-naming.md)). Ninguna se detecta leyendo código: solo
> midiendo.

> **Nota no verificada:** un mensaje en espera de reintento consume una ranura de
> `max_ack_pending`. Con backoffs largos y muchos fallos simultáneos, el consumidor
> podría llenar la ventana de 256 y dejar de recibir mensajes nuevos. Monitorizad
> `num_ack_pending`; si se acerca al límite de forma sostenida, el problema no es la
> ventana sino la tasa de fallo.

## 3. Deduplicación de productor

Un SDK L1 **DEBE** establecer la cabecera NATS:

```
Nats-Msg-Id: <id del CloudEvent>
```

Con `duplicate_window: 2m` en el stream, JetStream descarta mensajes con el mismo
`Nats-Msg-Id` recibidos dentro de esa ventana.

**Qué protege esto exactamente:** un productor que hace `publish()`, no recibe el ACK
del broker por un corte de red, y reintenta. Sin `Nats-Msg-Id` el stream acabaría con
dos copias.

**Qué NO protege:**

- Reintentos **después** de 2 minutos → dos copias en el stream.
- Reintentos con un `id` nuevo → son eventos distintos para JetStream; solo la lógica
  de negocio del consumidor puede saber que son el mismo hecho.
- Reentregas al consumidor. La ventana de duplicados es del **stream**, no del
  consumidor. Un `nak` reentrega el mismo mensaje con el mismo `Nats-Msg-Id` y eso es
  correcto y deseado.

> Esa última línea es el malentendido más común: `duplicate_window` no deduplica
> reintentos de consumo. **Nunca sustituye a la idempotencia del consumidor.**

## 4. Idempotencia del consumidor — obligatoria

Un consumidor **DEBE** implementar una de estas tres estrategias. El SDK **NO** la
elige por ti, porque la elección correcta depende de qué hace el handler.

### A. Tabla de eventos procesados

Insertas el `id` del evento en una tabla con clave única, **en la misma transacción**
que el efecto de negocio.

```sql
BEGIN;
  INSERT INTO eventos_procesados (event_id, consumer, processed_at)
  VALUES ($1, 'facturacion-api', now());   -- PK compuesta → falla si ya existe
  INSERT INTO facturas (...) VALUES (...);
COMMIT;
```

Si el `INSERT` viola la clave, la transacción entera aborta y el evento se `ack`ea sin
efecto. **Atómico y correcto.** Requiere que el efecto de negocio viva en la misma base
de datos transaccional; si el handler llama a una API externa, esta estrategia no
aplica.

### B. Operación naturalmente idempotente

La mejor: no hay nada que deduplicar porque repetir la operación no cambia el
resultado.

```
UPDATE pedidos SET estado = 'confirmado' WHERE id = $1;   -- idempotente
UPDATE pedidos SET intentos = intentos + 1 WHERE id = $1; -- NO idempotente
```

### C. Clave de idempotencia hacia el tercero

Si el efecto es una llamada externa, se propaga el `id` del evento como clave de
idempotencia del proveedor.

```
POST /v1/charges
Idempotency-Key: 01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55
```

**Regla:** la clave **DEBE** derivarse del `id` del evento, nunca generarse en el
handler. Una clave generada en el handler es distinta en cada reintento y no
deduplica nada.

## 5. Orden

> **flux v1 no garantiza orden global. Tampoco lo garantiza por agregado.**

JetStream entrega en orden de stream a un consumidor, pero con `max_ack_pending > 1`
o varias instancias del mismo durable, el **procesamiento** se solapa. Un `nak` con
backoff reordena de forma explícita: el mensaje reintentado llega después de otros
más recientes.

Las tres salidas posibles, con su coste real:

| Opción | Cómo | Coste |
|---|---|---|
| Handlers insensibles al orden | Diseño | Ninguno. **Es la opción por defecto de flux.** |
| `max_ack_pending: 1` | Config de consumidor | Serializa el consumidor entero. Throughput ≈ 1/latencia del handler. |
| Consumidor por partición | Un durable con filtro por rango de `partitionkey` | Complejidad operativa; rebalanceo manual |

### 5.1 Cómo se escriben handlers insensibles al orden

Se incluye en `data` un contador monotónico del agregado, y el consumidor descarta lo
que ya ha superado:

```json
"data": {
  "pedidoId": "ped-123",
  "aggregateVersion": 7,
  "estado": "confirmado"
}
```

```sql
UPDATE pedidos
   SET estado = $2, aggregate_version = $3
 WHERE id = $1
   AND aggregate_version < $3;   -- un evento viejo no pisa uno nuevo
```

Esto convierte el orden de un problema de infraestructura (caro, frágil) en una
condición del `WHERE` (barata, local, verificable en un test). Un productor
**DEBERÍA** incluir `aggregateVersion` en todo evento que represente un cambio de
estado de un agregado con ciclo de vida.

## 6. Reconexión

Un SDK L2 **DEBE**:

- Reconectar indefinidamente con backoff exponencial **con jitter** — sin jitter,
  mil servicios reconectan en el mismo milisegundo y tiran el cluster que acaba de
  levantarse.
- Encolar los `publish()` en memoria durante la desconexión, con un límite explícito,
  y **fallar en alto** al alcanzarlo. Encolar sin límite convierte un corte de red en
  un OOM.
- **NO DEBE** perder acks pendientes en silencio: un mensaje sin ack antes de la caída
  se reentregará, y eso es correcto — es exactamente el caso que la idempotencia
  cubre.
- Exponer el estado de conexión como un healthcheck consultable.
