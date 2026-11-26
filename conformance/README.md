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
│   └── security-acl.json        ← los permisos generados, ejecutados contra un broker con TLS (S1–S14)
├── fixtures/
│   └── acl-v1.conf              ← la ACL v1 de gen-acl.mjs, congelada y rota a propósito (caso S9)
├── security.mjs                 ← runner de security-acl.json: los casos son DATOS
├── security/                    ← su intérprete, y el ciclo del SDK de Node (S10)
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

Los casos S1–S8 y S12–S14 hablan NATS directamente, sin ningún SDK de flux. S10 y S11 son la
excepción deliberada: el SDK de Node y el de Rust, sin modificar, hacen un ciclo completo
(publicar, consumir, ack, reintento, DLQ) **contra la ACL generada**, y el runner mira después,
desde el broker, lo que dejaron. S9 vuelve a ejecutar S1 y S6 contra la ACL v1 congelada y exige
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

`scripts/verify-acl.mjs` **se queda**: no es redundante con esta suite. Verifica el generador (sus
errores, `--nkeys`, `--tenants`), `flux provision` (idempotencia, diferencias, nunca borra), la
minimalidad de cada permiso —quitando uno, el ciclo se rompe—, `--dev` y el Modelo B; esta suite
verifica el contrato de seguridad de extremo a extremo y con TLS. Comparten la infraestructura
(`scripts/lib/nats-harness.mjs`).

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
