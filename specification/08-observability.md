# 08 — Observabilidad

> **Estado:** normativo para SDKs **L2**. Las métricas son parte del contrato, no un
> extra opcional de cada implementación.

---

## 1. Por qué está en el protocolo

Si cada SDK nombra sus métricas a su manera, un panel del ecosistema es imposible: la
tasa de DLQ de los servicios Node no se puede sumar con la de los de Go. Y como los
nombres de métrica son en la práctica un contrato con los dashboards y las alertas,
cambiarlos después cuesta más que acordarlos ahora.

El mismo argumento que el de los códigos POISON de
[01-envelope.md §3.1](01-envelope.md): **si dos SDKs emiten nombres distintos para lo
mismo, agrupar deja de funcionar en cuanto el ecosistema es polyglot — que es siempre.**

## 2. Métricas obligatorias

Un SDK L2 **DEBE** exponer estas siete. Nombres en formato Prometheus; un SDK con otro
backend **DEBE** conservar los nombres y las etiquetas.

| Métrica | Tipo | Etiquetas |
|---|---|---|
| `flux_events_published_total` | Counter | `subject`, `outcome` |
| `flux_events_consumed_total` | Counter | `subject`, `consumer`, `outcome` |
| `flux_event_handler_duration_seconds` | Histogram | `subject`, `consumer` |
| `flux_events_dlq_total` | Counter | `subject`, `consumer`, `reason`, `code` |
| `flux_events_retried_total` | Counter | `subject`, `consumer`, `attempt` |
| `flux_consumer_pending` | Gauge | `subject`, `consumer` |
| `flux_connection_state` | Gauge | — |

### 2.1 Valores de las etiquetas

```
outcome  = ok | retryable | permanent | poison | invalid_schema | invalid_signature
reason   = retryable | permanent | poison          (04-errors.md §1)
code     = el código estable de la clasificación   ("HTTP_503", "PEDIDO_YA_CANCELADO")
attempt  = 1..max_deliver
```

`flux_connection_state`: `1` conectado, `0` desconectado, `2` reconectando.

### 2.2 ⚠️ Cardinalidad

- `subject` es **acotado** —hay tantos como eventos declarados— así que sirve como
  etiqueta.
- `code` **DEBE** ser un identificador estable y agrupable, nunca el mensaje de error.
  Un mensaje contiene ids, timestamps y rutas: su cardinalidad es infinita y tumba el
  almacenamiento de métricas. Es la misma razón por la que
  [04-errors.md](04-errors.md) exige códigos estables para `dlqerror`.
- **NUNCA** se etiqueta con `tenantid`, `id` ni `correlationid`. Un tenant nuevo no
  debe crear series temporales nuevas; para eso están las trazas.

> La cardinalidad no avisa: el sistema funciona en desarrollo con tres tenants y muere
> en producción con diez mil. Y el fallo se manifiesta como "Prometheus se ha quedado
> sin memoria", no como "alguien etiquetó por tenant".

## 3. Histograma de duración

Buckets **obligatorios**, en segundos:

```
0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30
```

El último bucket es `30` a propósito: **es el `ack_wait`**
([03-delivery.md §2](03-delivery.md)). Un handler que cae en el bucket superior está
a punto de que su mensaje se reentregue mientras aún se ejecuta, así que
`flux_event_handler_duration_seconds_bucket{le="30"}` frente al total mide
directamente cuántos eventos rozan la ejecución concurrente.

Ese bucket **DEBE** moverse si se cambia `ack_wait`. Un bucket que no coincide con el
plazo real mide algo que no le importa a nadie.

## 4. Alertas mínimas

Un ecosistema en producción **DEBERÍA** tener al menos estas cuatro:

| Alerta | Condición | Por qué |
|---|---|---|
| **POISON** | `increase(flux_events_dlq_total{reason="poison"}[5m]) > 0` | Un productor roto o alguien publicando a mano en el subject equivocado. Es el único caso que **debe despertar a alguien** ([04-errors.md §1.3](04-errors.md)) |
| **Tasa de DLQ** | `rate(flux_events_dlq_total[15m]) / rate(flux_events_consumed_total[15m]) > 0.01` | Más del 1% muriendo indica un fallo sistemático, no casos aislados |
| **Handler lento** | `histogram_quantile(0.99, ...) > 15` | La mitad del `ack_wait`. Avisa **antes** de que empiece la reentrega concurrente, no después |
| **Consumidor atascado** | `flux_consumer_pending > 1000` durante 10m | No da abasto, o dejó de consumir en silencio |

La cuarta importa más de lo que parece: un consumidor cuyo bucle murió **sigue
reportando la conexión como sana**. Solo el crecimiento de `pending` lo delata — es
el bug que apareció de verdad en el SDK de Node.

## 5. Trazas

El SDK ya propaga `traceparent` ([01-envelope.md §3.2](01-envelope.md)). Un SDK L2
**DEBERÍA** abrir un span por evento consumido con estos atributos:

```
messaging.system            = "nats"
messaging.destination.name  = <subject>
messaging.operation         = "publish" | "process"
messaging.message.id        = <id del evento>
flux.correlation_id         = <correlationid>
flux.tenant_id              = <tenantid>
```

Aquí **sí** se etiqueta por tenant: una traza es un evento individual, no una serie
temporal, y su cardinalidad no es un problema. Es la distinción que §2.2 explica desde
el otro lado.

## 6. Qué NO instrumentar

- **El contenido de `data`.** Ni en métricas ni en atributos de traza. Puede contener
  PII y las trazas se exportan a terceros ([06-security.md §5](06-security.md)).
- **El payload de eventos `confidential` o `restricted` en logs.** El SDK **DEBE**
  redactarlo.
- **Una métrica por tenant.** Ver §2.2.
