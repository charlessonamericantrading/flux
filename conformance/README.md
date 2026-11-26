# Suite de conformidad

**El contrato, ejecutable.** Los casos se definen como **datos** (JSON), no como tests
de un lenguaje concreto. Esa es la única forma de que "el SDK de Node" no se convierta
en la spec de facto y los demás lo persigan.

Un SDK está **terminado** cuando pasa esta suite. No cuando su autor lo dice.

```
conformance/
├── run.mjs                 ← runner contra un NATS real (`npm test`): habla NATS directamente
├── cross-sdk.mjs           ← runner cruzado: los mismos vectores en los siete SDKs, byte a byte
├── harnesses.json          ← cómo se invoca el arnés de cada SDK; `pending` = no verificado
├── harness/
│   ├── README.md           ← el contrato del arnés: una operación por stdin, un resultado por stdout
│   ├── vectors.json        ← los vectores que ejecutan los siete arneses
│   ├── keys.json           ← claves de TEST para los vectores de firma
│   └── build-java.mjs      ← construye el arnés de Java (javac + jar)
├── cases/
│   ├── broker-invariants.json              ← invariantes de NATS de las que depende el protocolo
│   ├── consumer-config.json                ← la config canónica sobrevive al servidor sin alterarse
│   ├── nak-delay-ignored-with-backoff.json ← con backoff, el delay de un nak solo cuenta la primera vez
│   ├── cross-sdk-envelope.json             ← el envelope es idéntico byte a byte entre SDKs
│   └── cross-sdk-dlq-envelope.json         ← ídem para el evento de DLQ
├── package.json            ← dependencias de `run.mjs` (cliente NATS)
└── README.md
```

`run.mjs` ejecuta las comprobaciones de `broker-invariants` y `consumer-config`; las lleva
escritas en el código y los JSON las documentan (norma, porqué y medición). Los
`cross-sdk-*` son fixtures que leen los tests de Rust y copian los de Java y .NET. La
medición de `nak-delay-ignored-with-backoff` está documentada, pero **ningún runner la
ejecuta todavía**.

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
# broker — necesita un NATS con JetStream
docker compose up -d
(cd conformance && npm install && npm test)

# sdk — sin broker (prepara antes cada SDK, como hace el job `cross-sdk` de CI)
node conformance/cross-sdk.mjs                 # --only rust,php · --verbose
```

Sin Docker, `nats-server` es un binario único sin instalación:
[releases](https://github.com/nats-io/nats-server/releases) →
`nats-server -js -sd ./data`.

## Niveles

| Nivel | Qué comprueba | Necesita | Se ejecuta con |
|---|---|---|---|
| **broker** | Que las suposiciones sobre NATS siguen siendo ciertas | Node y un NATS con JetStream | `run.mjs` |
| **sdk** | Que los siete SDKs construyen, firman y parsean **los mismos bytes** | los SDKs a probar; ningún broker | `cross-sdk.mjs` |

Los dos niveles corren hoy. `run.mjs` habla NATS directamente, sin pasar por ningún SDK:
un runner construido sobre un SDK heredaría justo las suposiciones que debe verificar. El
runner `sdk` invoca el arnés de cada SDK (`<sdk>/conformance-harness.*`, contrato en
[`harness/README.md`](harness/README.md)) con los mismos vectores y compara las salidas
byte a byte; el SDK de Node es la referencia que fija lo esperado. Un SDK en `pending` de
`harnesses.json` se reporta como **no verificado**, nunca se salta en silencio.

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
