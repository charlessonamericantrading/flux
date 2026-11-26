# Suite de conformidad

**El contrato, ejecutable.** Los casos se definen como **datos** (JSON), no como tests
de un lenguaje concreto. Esa es la única forma de que "el SDK de Node" no se convierta
en la spec de facto y los demás lo persigan.

Un SDK está **terminado** cuando pasa esta suite. No cuando su autor lo dice.

```
conformance/
├── cases/
│   ├── broker-invariants.json   ← invariantes de NATS de las que depende el protocolo
│   ├── consumer-config.json     ← la config canónica sobrevive al servidor sin alterarse
│   ├── security-acl.json        ← los permisos generados, ejecutados contra un broker con TLS (S1–S34; S30–S34, con los SDKs reales)
│   ├── interop-rpc.json         ← petición/respuesta entre los SDKs de Node y de Rust, bajo los permisos generados (I1–I5)
│   ├── state-broker.json        ← estado replicado: lo que hace el BROKER (A-31 … A-49), con ACL y cluster
│   ├── state-semantics.json     ← estado replicado: el algoritmo de put/delete/get y la foto de watch (A-50)
│   ├── cross-sdk-state-envelope.json ← la entrada de estado, la lápida y la DLQ de estado, byte a byte
│   ├── rpc-semantics.json       ← petición/respuesta: lo que hace el BROKER (A-56 … A-76), con ACL, cluster y async-nats
│   └── cross-sdk-rpc-envelope.json ← la petición, la respuesta y los errores de rpc, byte a byte; vectores de operación y timeout
├── fixtures/
│   └── acl-v1.conf              ← la ACL v1 de gen-acl.mjs, congelada y rota a propósito (caso S9)
├── security.mjs                 ← runner de security-acl.json: los casos son DATOS
├── security/                    ← su intérprete, el entorno compartido (entorno.mjs), el ciclo del SDK de Node (S10), los
│                                  participantes de los SDKs reales (participante-node.mjs; el de Rust, en sdk-rust/examples/) y los
│                                  mutantes del generador (mutantes.mjs) y de los SDKs (mutantes-sdk.mjs)
├── interop-rpc.mjs              ← runner de interop-rpc.json: los casos son DATOS, con `matrix` (quien llama × quien responde)
├── state-broker.mjs             ← runner de los tres ficheros de estado: los casos son DATOS
├── cross-sdk.mjs                ← runner de la conformidad cruzada: los siete SDKs, byte a byte
├── harness/                     ← su contrato (README.md), los vectores y la clave de prueba
├── harnesses.json               ← cómo se invoca el arnés de cada SDK
├── rpc-broker.mjs               ← runner de los dos ficheros de rpc: los casos son DATOS; con --mutant
├── probes/rpc-async-nats/       ← la sonda de Rust (async-nats) que ejecuta rpc-broker.mjs
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
| **seguridad** | Que los permisos que genera `gen-acl.mjs` funcionan con los SDKs **y** confinan de verdad: TLS, nkeys, inbox privado, lo permitido y lo denegado, y el ciclo completo de los SDKs de Node y de Rust | `nats-server` 2.14.5, `openssl`, `sdk-node/dist` y `cargo` |

Los casos `broker` corren hoy. Los casos `sdk` se definen aquí como datos y su runner
llega con el primer SDK — deliberadamente en ese orden, para que el runner no herede
las suposiciones de una implementación concreta.

## La suite de seguridad

```bash
(cd conformance && npm install) && (cd cli && npm install) && (cd sdk-node && npm install && npm run build)
NATS_SERVER=/ruta/a/nats-server node conformance/security.mjs
node conformance/security.mjs --acl conformance/fixtures/acl-v1.conf   # DEBE fallar en S1 y S6
```

A diferencia de `run.mjs`, donde las comprobaciones están escritas en código y los `cases/*.json`
solo las documentan, aquí **los casos son datos y el runner los ejecuta**:
[`cases/security-acl.json`](cases/security-acl.json) define un vocabulario pequeño de operaciones
(publicar, suscribir, pedir a la API de JetStream, crear un consumidor con un filtro dado, pull,
ack, un espía que intenta leer lo que otro consume…) y, por caso, qué se ejecuta y qué debe
observarse. [`security.mjs`](security.mjs) no sabe qué es S1: monta el entorno —PKI efímera con
`openssl`, nkeys de prueba, la config que genera `gen-acl.mjs --nkeys`, un `nats-server` con TLS,
`flux provision`— y su [intérprete](security/interprete.mjs) ejecuta los pasos. Un campo
desconocido en un caso, o una expectativa sobre algo que la operación no observa, es un error al
cargar: una errata no puede degradarse a «no se comprueba nada».

Los casos S1–S8 y S12–S29 hablan NATS directamente, sin ningún SDK de flux. S15 a S20 son los del
**estado replicado** ([06-security.md §3.8](../specification/06-security.md)): el ciclo del dueño y de
los observadores bajo la ACL generada, que solo el dueño escribe, que un tenant no lee el de otro, que
nadie consulta el stream de estado más allá de lo concedido, la DLQ de estado y la minimalidad de cada
permiso. S21 a S29 son los de la **petición/respuesta** ([06-security.md §3.9](../specification/06-security.md)): quien llama
publica solo en el subject exacto de cada operación y la denegación es rápida, solo el dueño atiende (ni `rpc.>`, ni el grupo
de cola de otro), la respuesta no la lee otro servicio, `allow_responses` admite UNA respuesta y caduca, un dueño sin
suscripción parece caído, la minimalidad de cada permiso, el ciclo de aceptación (dos instancias, 100 peticiones
concurrentes, `no responders` al parar) y la trampa de la segunda suscripción al inbox, con clientes `@nats-io/*` en bruto.
S25 tarda ~1 min: espera la caducidad de `allow_responses` de verdad.
Los pasos `save` / `{ "ref": … }` llevan la secuencia de una escritura condicional a la siguiente. S10 y S11 son la
excepción deliberada: el SDK de Node y el de Rust, sin modificar, hacen un ciclo completo
(publicar, consumir, ack, reintento, DLQ) **contra la ACL generada**, y el runner mira después,
desde el broker, lo que dejaron. **S30 a S34 repiten la petición/respuesta con los SDKs reales** (ver más abajo). S9 vuelve a ejecutar S1 y S6 contra la ACL v1 congelada y exige
que **fallen** como se documenta (H1 y H2): un test que solo se ha visto en verde no demuestra
nada. Los pasos marcados `control` demuestran que las sondas pueden fallar.

- `FLUX_REQUIRE_BROKER=1`: la ausencia de `nats-server`, `openssl`, `sdk-node/dist` o `cargo` es un
  **fallo**, no un salto con aviso. En CI siempre.
- `FLUX_TEST_PORTS=desde-hasta` (por defecto `4600-4649`) y `NATS_SERVER`.
- `--only S1,S6` ejecuta solo esos casos; `-v` muestra lo observado en cada paso; `--keep` conserva
  el directorio temporal; `--list` los enumera.
- `--acl <fichero>` sustituye la ACL generada por otra (un `accounts { … }` con un usuario por
  servicio, `user: "<servicio>"` y `password: "pw"`, como el fixture): sirve para ver qué casos
  detectan una ACL rota. S9, S10 y S11 no aplican, y los pasos que necesitan partir de los
  permisos generados se omiten y se dicen.

```bash
node conformance/security/mutantes.mjs            # los 13 mutantes del generador: la suite DEBE fallar con cada uno
node conformance/security/mutantes.mjs --fast     # sin los dos de `expires` (~1 min cada uno)
node conformance/security/mutantes.mjs --list
```

Los pasos `control` demuestran que **una sonda** puede fallar; los mutantes demuestran que **la suite** detecta un
**generador** que concede de más o de menos. [`security/mutantes.mjs`](security/mutantes.mjs) copia `scripts/` a un temporal, le
aplica a la copia UN cambio de texto —`rpc.<dominio>.>` a quien llama, `rpc.>` al dueño, un permiso de publicación sobre inboxes,
quitar `allow_responses` o su `expires`, el inbox común…— y ejecuta la suite con `FLUX_GEN_ACL` apuntando a esa copia: cada
mutante declara los casos que DEBEN fallar, y antes se comprueba que sin mutar esos casos pasan. Un ancla que ya no aparece en el
generador es un error, no un mutante que «no cambia nada y pasa».

### Los SDKs reales: S30 a S34 y la interoperabilidad (I1 a I5)

S21 a S29 miden el broker con clientes en bruto. Lo que importa de verdad es que **los SDKs** funcionen bajo esos permisos, y eso solo
se ve con ellos conectados: lo que se mide es lo que el SDK hace con SU conexión (qué suscripciones abre, qué permisos usa, cuánto
tarda en decir `no responders`). Por eso el vocabulario gana una operación, `sdk_script`
([`security/sdk-escenario.mjs`](security/sdk-escenario.mjs)): lanza **cada instancia como un proceso** con un SDK real —
[`security/participante-node.mjs`](security/participante-node.mjs) y [`sdk-rust/examples/rpc_participant.rs`](../sdk-rust/examples/rpc_participant.rs),
que hablan el **mismo protocolo** por stdin/stdout y solo usan la API pública del SDK—, con la nkey de un servicio del manifiesto (o de
una **variante** suya con un permiso de menos), y les da órdenes en el orden de un guion. Cada paso del guion compara lo que el agente
contestó, más lo que el orquestador mira por su cuenta —las violaciones de permiso que el `nats-server` anotó (`serverViolations`) y las
suscripciones reales de cada conexión por su puerto de monitorización (`broker_subs`, `/connz`)—, con su `expect`.

- **S30 (Node) y S31 (Rust)**: el ciclo de [11-request-reply.md §12.9](../specification/11-request-reply.md): dos instancias reparten 100
  peticiones concurrentes (cada una, ejecutada una vez), al parar ambas `RPC_UNAVAILABLE` en < 100 ms (la **mediana** de 7 llamadas: una sola
  medición engaña), y quitar cada permiso concedido —publicar en la operación, el inbox privado, la suscripción del dueño,
  `allow_responses`— **se nota con el código que toca**. Más un servicio de solo llamada (sin ninguna suscripción de dominio).
- **S32 (Node) y S33 (Rust)**: A-101 con el SDK real. Un servicio que **consume por pull** (con 40 eventos de tráfico) o que observa
  **estado** (`watch`) y llama por la misma conexión recibe `RPC_UNAVAILABLE` en milisegundos; y el broker confirma que **una sola**
  suscripción de la conexión casa con el `reply`: el buzón único de la librería. Se mide, no se lee.
- **S34 (Node)**: `allow_responses { max: 1 }` con un responder «tramposo» (su conexión publica dos veces cada respuesta): la segunda se
  deniega y el SDK lo registra. Solo Node: el SDK de Rust no expone la conexión.
- **I1 a I5** ([`interop-rpc.mjs`](interop-rpc.mjs)): el SDK de Node llama a Rust y al revés (y los dos de control: cada caso con `matrix`
  se expande en cuatro) —el sobre, los tenants distintos, el `aggregateId`, el UTF-8; los errores `retryable` y `permanent` con su `code`; lo
  desconocido con un mensaje genérico; la respuesta que no cabe; la firma `require` y sus tres códigos; la parada ordenada; y que un
  reintento reutiliza el `id`—. Comparte el entorno con la suite de seguridad ([`security/entorno.mjs`](security/entorno.mjs)): los mismos
  permisos generados, no una ACL escrita a mano.

```bash
(cd conformance && npm install) && (cd cli && npm install) && (cd sdk-node && npm install && npm run build)
NATS_SERVER=/ruta/a/nats-server node conformance/interop-rpc.mjs            # 5 casos × 4 combinaciones (~30 s)
node conformance/interop-rpc.mjs --only "I2[node→rust]" -v                   # una combinación, con lo observado
node conformance/security.mjs --only S30,S31,S32,S33,S34 -v
node conformance/security/mutantes-sdk.mjs                                   # 11 mutantes de SDK: los casos DEBEN fallar con cada uno
node conformance/security/mutantes-sdk.mjs --no-rust                         # sin los que compilan Rust (~1 min cada uno)
```

`mutantes-sdk.mjs` es el equivalente de `mutantes.mjs` para el **SDK**: copia `sdk-node/dist` (o `sdk-rust`) a un temporal, le aplica UN
cambio de texto —que abra una suscripción `_INBOX.<servicio>.>` en `connect()`, en el pull o en el `watch` (A-101), una respuesta que no hereda
el tenant, el texto de una excepción desconocida reenviado, un reintento con otro `id`, un responder que no verifica la firma— y ejecuta los
casos con `FLUX_SDK_NODE_DIST` / `FLUX_SDK_RUST_DIR` apuntando a la copia. Cada mutante declara los casos que DEBEN fallar (y que solo
fallan los suyos: el del pull no rompe el `watch`), y antes se comprueba que sin mutar pasan.

`scripts/verify-acl.mjs` **se queda**: no es redundante con esta suite. Verifica el generador (sus
errores, `--nkeys`, `--tenants`), `flux provision` (idempotencia, diferencias, nunca borra), la
minimalidad de cada permiso —quitando uno, el ciclo se rompe—, `--dev` y el Modelo B; esta suite
verifica el contrato de seguridad de extremo a extremo y con TLS. Comparten la infraestructura
(`scripts/lib/nats-harness.mjs`).

## La suite de estado replicado

```bash
(cd conformance && npm install)
NATS_SERVER=/ruta/a/nats-server node conformance/state-broker.mjs
node conformance/state-broker.mjs --only conditional-write,one-value-per-key -v
node conformance/state-broker.mjs --mutant only-10071        # DEBE fallar: demuestra que el caso detecta el fallo
```

Como `security.mjs`, **los casos son datos**: [`cases/state-broker.json`](cases/state-broker.json) define el
vocabulario (crear un stream de estado, publicar con `Nats-Expected-Last-Subject-Sequence`, `DIRECT.GET`,
un consumidor `last_per_subject`, una carrera de escritores…) y [`state-broker.mjs`](state-broker.mjs) no
sabe qué es «un valor por clave». Habla NATS directamente, sin ningún SDK, y levanta y para sus servidores:
uno suelto, uno con la ACL escrita a mano de `acls.estado` y, para los casos `topology: cluster3`, un cluster
de **tres nodos** (se saltan con `--no-cluster`). Un campo desconocido, o una expectativa sobre algo que la
operación no observa, es un error al cargar.

- [`state-broker.json`](cases/state-broker.json): lo que hace el **broker** (A-31 … A-49): un valor por clave,
  `DIRECT.GET`, el chequeo de secuencia —y que va **antes** que el de duplicados—, la lápida, la foto de un
  consumidor, qué deniega una ACL por tenant, qué lista `STREAM.INFO`, y el código del conflicto de secuencia
  en un nodo (10071) y en un cluster (10164).
- [`state-semantics.json`](cases/state-semantics.json): el **protocolo** encima (A-50): el algoritmo de
  `put`/`delete`/`get` de [10-state.md §3.2](../specification/10-state.md) y la foto de `watch` (§5.5), escritos
  en el runner como implementación de **referencia** sobre NATS crudo. **Los SDKs de Node y Rust ejecutan estos
  mismos escenarios con su API**: lo esperado es lo de aquí.
- [`cross-sdk-state-envelope.json`](cases/cross-sdk-state-envelope.json): los ejemplos byte a byte de
  [10-state.md §2.6](../specification/10-state.md) con su SHA-256, las operaciones del arnés cruzado
  (`state-build`, `state-tombstone`, `state-subject`, `state-dlq`) y 23 vectores del subject con el
  **código estable** de cada rechazo. `state-broker.mjs` reconstruye los bytes desde los datos y comprueba el
  hash; **`cross-sdk.mjs` ejecuta las mismas entradas en los SDKs** y comprueba que `harness/vectors.json`
  sigue repitiéndolas sin cambios (véase [La conformidad cruzada](#la-conformidad-cruzada-entre-sdks)).
- `--mutant <nombre>` estropea a propósito la configuración del stream o la referencia (`no-max-msgs-per-subject`,
  `no-allow-direct`, `discard-new`, `only-10071`, `no-own-write-check`, `equal-version-applies`,
  `publish-via-core`, `fresh-id-per-attempt`, `no-cas-header`) y **cada uno DEBE hacer fallar el runner**.
  `only-10071` solo lo detecta el caso de cluster: en un nodo suelto nunca se ve un 10164.

Puertos: `FLUX_TEST_PORTS=desde-hasta` (por defecto `4600-4649`; se usan hasta 8), `NATS_SERVER` y
`FLUX_REQUIRE_BROKER=1`, como en la suite de seguridad.

## La conformidad cruzada entre SDKs

```bash
(cd sdk-node && npm install && npm run build)          # y lo que necesite cada SDK que quieras incluir
node conformance/cross-sdk.mjs --verbose               # sin broker
node conformance/cross-sdk.mjs --only node,rust        # un subconjunto
node conformance/cross-sdk.mjs --only node,rust --mutant rust-un-byte    # DEBE fallar
```

Cada SDK trae un arnés diminuto (una operación por stdin, un JSON por stdout) y el runner los invoca
con los **mismos vectores** y compara las salidas **byte a byte**: el contrato está en
[`harness/README.md`](harness/README.md). Es lo único que ejecuta la interoperabilidad entre lenguajes;
los `cases/cross-sdk-*.json` la describen y nada más. **Un fixture que nadie ejecuta aparenta
cobertura.**

**El estado replicado** ([10-state.md](../specification/10-state.md)) entra por las operaciones
`state-build`, `state-tombstone`, `state-subject` y `state-dlq`, con los ejemplos de §2.6, los
`subjectVectors` de [`cross-sdk-state-envelope.json`](cases/cross-sdk-state-envelope.json) y vectores
de límites, UTF-8 literal, versión, orden de validación (§3.6) y un **control** (otro `tenantid` da otros
bytes). Reglas del runner:

- Las operaciones de estado se ejecutan **solo en los SDKs que declaran `state`** en `protocol.json` →
  `capabilities` (hoy, Node y Rust). Para los demás imprime **`N/A: no declara state`** en el resumen y
  una `–` en cada fila: nunca un salto silencioso.
- Un SDK que **declara** `state` y no tiene la operación en su arnés **falla** (`UNSUPPORTED_OP`), y también
  si no tiene arnés registrado.
- Node, la referencia, fija lo esperado; el resto debe coincidir byte a byte. Los tres ejemplos de la
  spec llevan además su SHA-256: ahí el patrón es la spec y no Node.

**Las mutaciones** (`--mutant`, y CI las ejecuta) demuestran que el runner detecta lo que dice detectar: un
byte cambiado en Rust, un tenant intercambiado o ignorado, una operación ausente en un SDK que declara
`state`, un SDK sin arnés registrado —todos **fallan**— y un SDK que no declara `state`, que sale `N/A`
**sin** fallo. La tabla está en [`harness/README.md`](harness/README.md#mutaciones).

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

## La suite de petición/respuesta

```bash
(cd conformance && npm install)
NATS_SERVER=/ruta/a/nats-server node conformance/rpc-broker.mjs                       # 27 casos + los ejemplos byte a byte
node conformance/rpc-broker.mjs --list                                                 # los casos y lo que cubren (A-n)
node conformance/rpc-broker.mjs --mutant no-queue-group                                # DEBE fallar (también: wildcard-subscription,
                                                                                       #   no-allow-responses, stream-captures-rpc)
```

Como la de estado, **los casos son datos** ([`cases/rpc-semantics.json`](cases/rpc-semantics.json)): el runner solo ejecuta su
vocabulario contra un `nats-server` real —uno suelto, uno con la ACL de cada caso y un cluster de tres nodos— y compara lo
observado con `expect`. Mide el reparto por grupo de cola, `no responders`, qué ve quien llama si el que responde tarda más que el
timeout o muere, `allow_responses`, el tamaño máximo, la petición **denegada** con las dos librerías de NATS de los SDKs (Node, y
`async-nats` de Rust por [`probes/rpc-async-nats`](probes/rpc-async-nats), que compila con `cargo`) y la trampa del stream que
captura `rpc.>`. `FLUX_TEST_PORTS=6000-6049` fija el rango de puertos (usa hasta 8), `--no-rust` salta la sonda y `--no-cluster` el
cluster; `FLUX_REQUIRE_BROKER=1` convierte la falta de `nats-server` o de `cargo` en un fallo.
