# Suite de conformidad

**El contrato, ejecutable.** Los casos se definen como **datos** (JSON), no como tests
de un lenguaje concreto. Esa es la única forma de que "el SDK de Node" no se convierta
en la spec de facto y los demás lo persigan.

Un SDK está **terminado** cuando pasa esta suite. No cuando su autor lo dice.

```
conformance/
├── cases/
│   ├── broker-invariants.json   ← invariantes de NATS de las que depende el protocolo
│   └── consumer-config.json     ← la config canónica sobrevive al servidor sin alterarse
└── README.md
```

## Por qué existe

Esta especificación se escribió de arriba abajo, sin ejecutarse. Al contrastarla
contra un NATS real apareció **un bug grave en el primer intento**:

> La spec declaraba `ack_wait: 30s` junto a `backoff: [1s, 5s, 30s, 2m, 10m]`.
> **JetStream sobrescribe `ack_wait` con `backoff[0]` y no avisa.** El servidor acepta
> la petición, devuelve `ack_wait: 1s`, y cualquier handler que tarde más de un
> segundo —es decir, cualquiera que escriba en una base de datos— recibe el mismo
> mensaje reentregado mientras aún se está ejecutando.

Ese error habría acabado replicado en Node, Python y Go, y habría fallado bajo carga
en producción, no en los tests. Está fijado en
[`cases/consumer-config.json`](cases/consumer-config.json) con su contraejemplo, para
que ninguna implementación pueda reintroducirlo.

**Ese es el argumento entero de esta carpeta:** una spec no verificada es una
hipótesis bien redactada.

## Estado de verificación

Contrastado contra **nats-server 2.14.5** el 2026-08-20:

| Afirmación de la spec | Resultado |
|---|---|
| Los nombres de stream rechazan puntos | ✅ `"EVT.PEDIDOS" is not a valid stream name` |
| Los durable consumers rechazan puntos | ✅ `durable name can not contain '.', '*', '>'` |
| El naming `svc__subject_con_guiones` es construible | ✅ aceptado |
| El prefijo `dlq.` mantiene streams disjuntos | ✅ 0 mensajes cruzados |
| Un sufijo `.dlq` lo captura el stream principal | ✅ confirmado — justifica el prefijo |
| `Nats-Msg-Id` deduplica publicaciones | ✅ 3 publicaciones → 1 mensaje |
| Los subjects son case-sensitive y fallan en silencio | ✅ exit 0, 0 mensajes, ningún error |
| `ack_wait` es independiente de `backoff` | ❌ **FALSO** — corregido en la spec |

## Ejecutar

```bash
docker compose up -d
docker compose exec nats-box nats stream ls
```

Sin Docker, `nats-server` es un binario único sin instalación:
[releases](https://github.com/nats-io/nats-server/releases) →
`nats-server -js -sd ./data`.

## Niveles

| Nivel | Qué comprueba | Necesita |
|---|---|---|
| **broker** | Que las suposiciones sobre NATS siguen siendo ciertas | solo el `nats` CLI |
| **sdk** | Que una implementación cumple el protocolo | el SDK a probar |

Los casos `broker` corren hoy. Los casos `sdk` se definen aquí como datos y su runner
llega con el primer SDK — deliberadamente en ese orden, para que el runner no herede
las suposiciones de una implementación concreta.

## Añadir un caso

Un caso **DEBE** llevar:

- `spec`: la sección normativa que verifica. Un caso sin norma detrás es un test de
  implementación, y no pinta nada aquí.
- `rationale`: **qué se rompe si esto deja de ser cierto.** Sin esto, dentro de un año
  nadie sabrá si el caso sigue importando o solo estorba.
- `observed`: el resultado real medido, con versión y fecha. Un valor esperado que
  nadie ha visto nunca es otra hipótesis.
- `counterExample` cuando el caso nace de un bug real: la forma incorrecta, y qué
  provocaba. Es lo que impide que vuelva.
