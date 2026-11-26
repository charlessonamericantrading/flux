# Plan v1.1 — flux como sustituto completo de API y webhooks internos

> **Estado:** propuesto · **Fecha:** 2026-11-26
> **Verificado contra:** nats-server 2.14.5 — ver [Apéndice A](#apéndice-a--evidencia)

---

## 0. Para el agente que ejecuta este plan

**Cómo se trabaja**

- **Una fase = una rama de fase + uno o varios PRs en borrador**, uno por frente de
  trabajo (spec e infraestructura, Node, Rust…), apilados sobre la rama de la fase. Los
  frentes de una misma fase **pueden ir en paralelo**. Las fases van en orden: no empieces
  una sin cerrar la anterior, porque cada una construye sobre los nombres y permisos de la
  previa.
- **Orden de lenguajes — decisión del dueño, 2026-11-26:** Node (referencia) → **Rust** →
  Go, Java, .NET y PHP → **Python al final**. Cada primitiva se implementa primero en Node
  y Rust.
- Antes de tocar nada, lee `AGENTS.md`, `specification/` entera y este fichero entero.
- **La spec manda sobre el código**, y **lo medido manda sobre la spec.** Si algo de este
  plan choca con lo que observas contra un NATS real, gana lo observado: corrige el plan
  en el mismo PR y explica por qué.
- No amplíes el alcance. Lo que no está en una fase, no entra en su PR.

**Las reglas del repo que este plan hereda**

1. Toda suposición sobre el broker va a `conformance/cases/*.json` con `spec`,
   `rationale`, `observed` (versión y fecha) y, si nace de un bug, `counterExample`.
   Una regla sin caso ejecutado es una hipótesis.
2. Todo lo que deba ser idéntico entre SDKs va a `conformance/harness/vectors.json` y se
   compara byte a byte con `conformance/cross-sdk.mjs`.
3. Las pruebas se ejecutan **en los dos sentidos**: lo correcto pasa y lo incorrecto
   falla. Una comprobación que solo se ha visto en verde no demuestra nada.
4. `llms-full.txt` y `schemas/bundle.json` son generados. Los esquemas publicados son
   inmutables.
5. Nada de identificadores de modelo en commits, PRs ni ficheros.

**Definición de hecho — toda fase, sin excepción**

```bash
node scripts/build-llms.mjs --check
node scripts/check-protocol-json.mjs
node scripts/bundle-schemas.mjs --check
node scripts/check-compat.mjs
node scripts/gen-acl.mjs examples/services.json --check
python scripts/check-workflow.py
node conformance/cross-sdk.mjs
(cd conformance && npm test)            # necesita NATS
# + los tests de cada SDK tocado, + los comandos de «Aceptación» de la fase
```

Además: tabla de SDKs del `README.md` al día, y la checklist de `AGENTS.md` §11.

**Entorno**

- El contenedor cloud **no tiene el daemon de Docker**. NATS se obtiene así (probado, ~1 min):
  `GOBIN=$PWD/bin go install github.com/nats-io/nats-server/v2@v2.14.5`
- `dotnet` no está instalado en el contenedor: el SDK de .NET se verifica en CI.

---

## 1. Objetivo

Que un desarrollador con varias webs, programas y servicios propios los conecte con flux
**en lugar de** con APIs y webhooks entre ellos. Tres propiedades, las tres obligatorias:

- **Completo.** Todo lo que hoy se resuelve con una API o un webhook interno tiene una
  primitiva flux. Sin huecos que obliguen a volver a HTTP.
- **Más ligero.** El dato llega solo cuando cambia. Nadie pregunta «¿hay algo nuevo?».
- **Seguro.** Lo que la spec promete sobre permisos está ejecutado contra un broker real.

**Criterio de éxito — medible**

1. Ninguna fila del mapa de §2 queda sin primitiva.
2. Una suite aplica los permisos generados a un NATS real y demuestra lo permitido **y**
   lo denegado.
3. La mejora de carga está medida y publicada en `bench/RESULTS.md`. Lo que no se haya
   medido no se afirma en el `README.md`.
4. Un ecosistema de ejemplo arranca con un comando y usa las tres primitivas.

---

## 2. Mapa de sustitución

Esto es lo que «completo» significa. Cada fila es algo que hoy se hace con HTTP.

| # | Hoy | En flux | Estado |
|---|---|---|---|
| 1 | Webhook: «te aviso de que pasó algo» | **Evento** | ✅ existe |
| 2 | Polling: `GET /cambios?desde=` | **Evento** | ✅ existe |
| 3 | `GET /clientes/123`: leer un dato de otro sistema | **Estado replicado** | ❌ Fase 2 |
| 4 | Carga inicial: `GET /clientes?page=` | **Estado replicado** | ❌ Fase 2 |
| 5 | `POST /accion` que necesita respuesta ya | **Petición/respuesta** | ❌ Fase 3 |
| 6 | `GET /precio?…`: un cálculo, no un dato guardado | **Petición/respuesta** | ❌ Fase 3 |
| 7 | `POST /accion` asíncrona (encolar trabajo) | **Evento** en el dominio de quien pide + evento de resultado, unidos por `correlationid` | ✅ existe — falta documentarlo (Fase 5) |
| 8 | Webhook entrante de un tercero (pasarela de pago…) | **Gateway**: HTTP → evento | ❌ Fase 4 |
| 9 | Webhook saliente a quien no puede conectarse | **Gateway**: evento → HTTP firmado | ❌ Fase 4 |
| 10 | Sistema sin conexión permanente (hosting compartido, cron, serverless) | **Gateway** HTTP | ❌ Fase 4 |
| 11 | Ficheros grandes | Claim-check (`01-envelope.md` §7) | ✅ existe |
| 12 | Tiempo real hacia el navegador | Fuera de alcance — §6 | declarado |

**La regla que evita recrear el problema.** Si lo que pides es *un dato que otro sistema
guarda*, es **estado**, no petición/respuesta. La petición/respuesta queda para órdenes
que necesitan respuesta inmediata y para cálculos. Usarla para leer datos es volver a
tener una API, con otro transporte.

---

## 3. Lo que está roto hoy

Verificado ejecutando, no leyendo. Reproducible con
[`evidencia/ejecutar.sh`](evidencia/ejecutar.sh).

| # | Hallazgo | Evidencia |
|---|---|---|
| **H1** | **Con los permisos que genera `scripts/gen-acl.mjs`, un servicio no puede consumir nada.** No conceden nada sobre `$JS.API.*`, y JetStream se consume por ahí. El SDK falla también al publicar, porque antes consulta el stream. | A-1 |
| **H2** | **Con esos mismos permisos, un servicio lee dominios que no tiene concedidos.** `_INBOX.>` es común a todos y las entregas de JetStream viajan por el inbox. `facturacion-api` leyó un evento de `inventario`. | A-2 |
| **H3** | Ningún test aplica los permisos generados a un broker. CI solo valida el manifiesto (`--check`). | `.github/workflows/spec.yml` |
| **H4** | El SDK de Node no puede autenticarse con `.creds`: `credentials.creds` se pasa a una opción que el cliente ignora. Ningún SDK permite fijar una CA propia ni el prefijo del inbox. | `sdk-node/src/client.ts` (`connect`) |
| **H5** | La spec manda no meter datos personales en eventos y «pedirlos al dueño» (`06-security.md` §5.1). Eso devuelve las llamadas que flux quiere eliminar. | — |
| **H6** | No hay ninguna medición de rendimiento en el repo. | — |
| **H7** | Documentación desfasada. Incluye una regla dura ilegible: en `AGENTS.md` (regla 11) y en `01-envelope.md` §1.1 los dos ejemplos son `café`; se perdió el escape. | Fase 0 |

H1 y H2 juntos significan que **el modelo de permisos nunca se ha ejecutado**: tal como
se genera, o no funciona, o —si alguien lo «arregla» concediendo `$JS.API.>`— no protege.
Por eso la seguridad es la Fase 1 y no la última.

---

## 4. Decisiones de diseño — cerradas

### 4.1 Versión del protocolo

**1.1.0**, aditiva. Las siete decisiones fundacionales del `README.md` no se tocan. Estado
y petición/respuesta son **extensiones opcionales**, como la firma: un servicio que solo
use eventos sigue siendo conforme.

`00-protocol.md` §6 saca «request/reply» de «fuera de alcance» en la Fase 3.

### 4.2 Tres primitivas, un prefijo cada una

El prefijo es el mismo en el subject, en el `type` y en la ruta del esquema. Igual que
`dlq.` hoy: un espacio de nombres disjunto por construcción.

| | Evento (existe) | Estado (Fase 2) | Petición/respuesta (Fase 3) |
|---|---|---|---|
| **Qué es** | Un hecho que pasó | El valor actual de un agregado | Una orden o cálculo con respuesta |
| **Subject NATS** | `<dom>.<agr>.v<M>.<evento>` | `state.<dom>.<agr>.v<M>.<tenant>.<clave>` | `rpc.<dom>.<agr>.v<M>.<operacion>` |
| **Nombre de contrato** | el subject | `<dom>.<agr>.v<M>` | `<dom>.<agr>.v<M>.<operacion>` |
| **Stream** | `EVT_<DOM>` | `STATE_<DOM>` | ninguno — core NATS |
| **`type`** | `com.flux.<dom>.<agr>.<evento>.v<M>` | `com.flux.state.<dom>.<agr>.v<M>` | `com.flux.rpc.<dom>.<agr>.<op>.request.v<M>` / `.reply.v<M>` |
| **Esquema** | `schemas/<dom>/<agr>/<evento>/<semver>.json` | `schemas/state/<dom>/<agr>/<semver>.json` | `schemas/rpc/<dom>/<agr>/<op>/request/<semver>.json` y `…/reply/…` |
| **Quién escribe** | solo el dueño del dominio | solo el dueño | el dueño responde; llama quien tenga concesión |
| **Persistencia** | 30 d (según clasificación) | hasta que se borra | ninguna |
| **DLQ** | `dlq.<subject>` | `dlq.state.<dom>.<agr>.v<M>` | no aplica |

El sobre es **el mismo CloudEvent** en las tres. Así `tenantid`, `correlationid`,
`traceparent`, la clasificación, la validación L3 y la firma funcionan sin cambios.

### 4.3 Nombres de dominio reservados

`dlq`, `state`, `rpc`, `system`. Un dominio llamado así colisionaría con un espacio de
nombres del protocolo. **Hoy `dlq` no está reservado y debería**: se arregla en la Fase 2
(`02-naming.md`, `protocol.json`, `gen-acl.mjs`, `flux validate`).

### 4.4 Datos personales: la contradicción se resuelve con el estado

| | Evento | Estado |
|---|---|---|
| ¿Datos personales? | **No.** Solo referencias (`clienteId`). Regla 5 intacta. | **Sí**, clasificado `confidential` y con `x-pii` en el esquema. |
| ¿Se puede borrar? | No: es un log inmutable. | **Sí**: `state.delete` sustituye el valor por una lápida. |

El evento dice **qué pasó**. El estado lleva **el dato**. El consumidor ya no llama al
dueño: tiene el dato porque le llegó.

`restricted` (credenciales, salud, financiero) **no se admite en estado** en 1.1.

### 4.5 Capacidades por SDK

Los niveles L1–L3 no cambian. Cada SDK declara además qué extensiones implementa:
`signing`, `state`, `rpc`, `encryption`. La tabla del `README.md` gana esas columnas. Un
SDK no anuncia una capacidad hasta que pasa sus vectores cruzados.

### 4.6 Orden de implementación en los SDKs

Cada primitiva se implementa primero en **Node (referencia) y Rust**. Dos
implementaciones independientes antes de dar la spec por estable: el historial del repo
muestra que cada port destapó un fallo de la spec. Los otros cinco van en la Fase 6, en
este orden: Go, Java, .NET, PHP y Python al final.

---

## 5. Fases

### Fase 0 — Documentación al día

Pequeña. Un PR.

1. `AGENTS.md` (regla 11) y `specification/01-envelope.md` §1.1: el segundo ejemplo debe
   ser la forma escapada, barra invertida + `u00e9`. `CLAUDE.md` la tiene bien; cópiala.
   Comprobación: `grep -c 'caf\\u00e9' AGENTS.md specification/01-envelope.md` → `1` y `1`.
2. `README.md`, tabla «Estado de los SDKs»:
   - Una fila por SDK. Sobra la segunda de PHP (L2, 201).
   - Rust: L3 con la feature `validation` (`protocol.json` y `sdk-rust/README.md` lo dicen).
   - Borra la nota que dice que el transporte de PHP no está verificado:
     `sdk-php/README.md` dice que sí lo está.
   - «Los cinco primeros se compilan en CI» → los siete tienen job en `spec.yml`.
   - Recuentos de tests: ejecútalos o quítalos. No copies una cifra que no hayas visto.
3. `README.md`, árbol de estructura: falta `sdk-rust/`; sobran las etiquetas «fase 1/2».
4. Tamaños: `llms-full.txt` pesa ~110 KB (el README dice ~55, `AGENTS.md` ~100,
   `llms.txt` ~45); `protocol.json` ~28 KB (dice ~8); `AGENTS.md` ~15 KB (dice ~9).
5. `conformance/README.md`: el árbol lista 2 de los 5 casos y omite `run.mjs`,
   `cross-sdk.mjs` y `harness/`. El párrafo «el runner llega con el primer SDK» ya no es
   cierto.
6. `06-security.md` §4: la nota «queda en fase 4» contradice su propio §7.

**Aceptación:** la definición de hecho de §0.

#### Fase 0b — Huecos de CI y deuda de documentación

Los destapó la ejecución de la Fase 0 (PR #2). Verificados, no supuestos:

| # | Hueco | Dónde se cierra |
|---|---|---|
| 1 | El job de CI de Python instala solo `pytest cryptography`: los tests de `client` y de `validation` no corren en CI | `fase-0b-ci-docs` |
| 2 | El job de CI de Rust no usa `--all-features`: `validation` y `signing` no se prueban en CI y el README ya llama L3 a Rust | `fase-1-rust` |
| 3 | `sdk-php/README.md` se contradice (301 frente a 304 tests; «Sin verificar» frente a «verificado») | `fase-0b-ci-docs` |
| 4 | Etiquetas «fase 4» obsoletas en `01-envelope.md` y `06-security.md` §7; `verifiedAgainst` de los casos `cross-sdk-*` cita 3 y 4 SDKs, no 7 | `fase-0b-ci-docs` |
| 5 | `conformance/run.mjs` lleva las comprobaciones escritas en código; `cases/*.json` solo las documenta. Ningún runner ejecuta `nak-delay-ignored-with-backoff.json` | Fase 1.5: la suite de seguridad **debe** leer sus casos de datos y ejecutarlos, o no puede llamarse «el contrato, ejecutable» |

---

### Fase 1 — Seguridad que se ejecuta

**Objetivo:** que los permisos generados funcionen con los SDKs y confinen de verdad, y
que una suite lo demuestre en cada PR. Cierra H1–H4.

#### 1.1 Spec — `specification/06-security.md` §2 y §3

Reglas nuevas. Todas tienen evidencia en el Apéndice A.

- **Inbox privado.** Cada servicio usa `_INBOX.<servicio>.>` y solo puede suscribirse
  ahí. El SDK lo fija solo a partir del nombre de servicio; no es una opción. *(A-2, A-7)*
- **Un servicio no recibe ningún permiso de suscripción sobre subjects de dominio.** Los
  siete SDKs consumen por pull; lo que confina es `$JS.API`.
- **Permisos exactos por consumidor durable.** Por cada subject consumido, y solo esos:

  ```
  $JS.API.CONSUMER.CREATE.<STREAM>.<durable>.<subject>
  $JS.API.CONSUMER.INFO.<STREAM>.<durable>
  $JS.API.CONSUMER.MSG.NEXT.<STREAM>.<durable>
  $JS.ACK.<STREAM>.<durable>.>
  ```

  Más `$JS.API.INFO` y `$JS.API.STREAM.INFO.<STREAM>` de los streams propios y consumidos,
  **y de la DLQ de cada stream consumido**: `subscribe()` también consulta `DLQ_<DOM>`, y sin
  ese permiso intenta crearlo y falla. *(A-3, A-4, A-8b)*

  > **Corregido tras medir.** La versión original de este plan, y `evidencia/candidata.conf`,
  > no incluían `STREAM.INFO.DLQ_<DOM>`. Con la candidata, el SDK de Node **no** podía
  > suscribirse (`Permissions Violation for Publish to "$JS.API.STREAM.CREATE.DLQ_PEDIDOS"`).
- **El dueño lee su propia DLQ con un pull consumer**, no con una suscripción a
  `dlq.<dominio>.>`: un durable `<servicio>__dlq_<dominio>` sobre `DLQ_<DOM>`, con filtro
  `dlq.<dominio>.>` y los mismos permisos que cualquier durable, y ninguna suscripción. El plan no lo cubría
  (la v1 del generador le daba `sub: dlq.<dominio>.>`, que con pull consumers no sirve).
  Alternativa descartada: la suscripción core, que solo ve los muertos en vivo y reintroduce
  un permiso de suscripción fuera del inbox. *(A-8a)*

  > **Medido también en el SDK de Node** *(A-26, A-27)*: `subscribe()` comprueba que la DLQ
  > existe **al arrancar** (descubrirlo con el primer evento envenenado es el peor momento) y
  > sin el permiso lanza `PermissionDeniedError`; y cada permiso de la lista se probó
  > **quitándolo** (`sdk-node/test/integration/acl.test.ts`).

  Medido también en el SDK de Rust *(A-28)*:
  - `$JS.API.INFO` **no lo usa el SDK de Rust** (medido con un sniffer sobre el flujo completo).
    El cliente de Node sí lo emite con `jetstreamManager()` (A-3): confírmalo allí antes de
    quitarlo de la lista de Node.
  - `$JS.API.CONSUMER.CREATE.…` es **prescindible** si `flux provision` crea el consumidor:
    el SDK intenta crear, ve la denegación, consulta el que existe (`CONSUMER.INFO`) y lo usa.
  - `$JS.API.CONSUMER.INFO.…` solo lo usan el sondeo de `flux_consumer_pending` (si hay
    métricas) y ese camino de «consumidor ya aprovisionado».
- **Nunca a un servicio:** `$JS.API.CONSUMER.DURABLE.CREATE.*` (el filtro va en el cuerpo
  y ninguna ACL lo ve), `$JS.API.STREAM.MSG.GET.*`, y crear, modificar, borrar o purgar
  streams. *(A-5, A-6)*
- **TLS obligatorio y credencial por servicio** (ya estaba): ahora los SDKs **DEBEN**
  aceptar una CA propia y credenciales nkey / `.creds`.
- **Streams y consumidores se aprovisionan fuera del SDK** en producción. Si el SDK
  intenta crear y el broker lo deniega, **DEBE** fallar con un error accionable
  (`NOT_PROVISIONED`), no con un timeout. *(A-8b)*

  > **Decidido tras medir: `CONSUMER.CREATE` NO se concede por defecto.** El plan original lo
  > incluía en los permisos de cada durable. No es imprescindible con un durable aprovisionado
  > (el SDK de Node, si le deniegan el `add`, cae a `CONSUMER.INFO`), y concederlo permite que
  > un durable borrado lo recree el propio servicio con `deliver_policy: all` —reentrega
  > silenciosa de hasta 30 días de eventos— o que un servicio comprometido cambie el `ack_wait`
  > o el `backoff` de su consumidor. `gen-acl.mjs --dev` lo concede, solo sobre el durable
  > propio y con su filtro exacto, para arrancar sin `flux provision`. Los SDKs hacen *info
  > antes de crear* (Fase 1.4 y 6).

  **Ojo: el broker no contesta a una petición
  denegada** (A-28): el SDK tiene que escuchar los `-ERR 'Permissions Violation…'` de la
  conexión y competirlos con la respuesta, o el síntoma es un timeout de 5–30 s. Falta
  además un segundo código, `PERMISSION_DENIED`, para cuando ni siquiera hay permiso de
  *consultar* el recurso (una ACL de la v1): `NOT_PROVISIONED` se reserva para «el recurso
  falta y no puedo crearlo». Los dos son PERMANENT.

Añade a `AGENTS.md` §8 dos trampas: «los permisos de subject no gobiernan JetStream» y
«`_INBOX.>` común = cualquiera lo lee todo».

#### 1.2 `scripts/gen-acl.mjs` v2

- `consumes` acepta subjects exactos, o un patrón acabado en `>` que se **expande contra
  `schemas/bundle.json`**. Un patrón que no encaja con ningún subject declarado es un
  error. Motivo: el nombre del durable es un único token y NATS no permite comodines
  dentro de un token; hay que conocer el subject exacto.
- Emite los permisos de 1.1. La forma objetivo está escrita a mano en
  [`evidencia/candidata.conf`](evidencia/candidata.conf), con las dos diferencias de arriba
  (la DLQ de los streams consumidos y la DLQ propia).
- `--nkeys <mapa.json>`: sustituye cada `<NKEY_DE_*>` por la clave pública real. Rechaza una
  **semilla** (es un secreto), una clave repetida entre servicios (credencial compartida) y un
  servicio sin clave.
- Actualiza también la rama `--tenants` (Modelo B). Las claves de `--nkeys` son
  `<servicio>@<tenant>`: NATS rechaza la misma nkey en dos accounts (*Duplicate nkey*, A-8d).

**Interfaz fijada** (la suite de seguridad se construye sobre ella):

```
node scripts/gen-acl.mjs <services.json> [--bundle <bundle.json>] [--nkeys <mapa.json>]
                         [--check] [--tenants] [--json] [--dev]
```

- `--json` imprime un array con un elemento por servicio (por servicio y tenant con
  `--tenants`): `{ name, publish, subscribe, owns, consumes, durables }`, donde `consumes` ya
  está **expandido** a subjects exactos y `durables` es `[{ stream, durable, filter, kind }]`
  (`kind` ∈ `events | dlq`).
- `--dev` añade `CONSUMER.CREATE` de cada durable propio (ver arriba). Sin él, ningún permiso
  de `CREATE`. Una lista de permisos **nunca** sale vacía: en NATS `allow: []` significa «sin
  restricción», no «nada» (medido).
- La salida por defecto es un bloque `accounts { … }` **incluible** con `include`; no define
  puerto, TLS ni JetStream de servidor. No genera el usuario de operación: quien aprovisiona lo
  añade (p. ej. insertándolo tras `users: [`).
- `--bundle` por defecto es el `schemas/bundle.json` **de este repositorio**, no el del
  directorio actual.
- Un patrón de `consumes` sin coincidencias, o un subject exacto inexistente, es un error con
  pista; también un comodín que no sea el `>` final y dos subjects que den el mismo durable.
- La lógica compartida con `flux provision` vive en `scripts/lib/manifest.mjs`;
  `protocol.json` → `security.acl` fija la lista **cerrada** de permisos concedibles y la de los
  denegables, y `check-protocol-json.mjs` comprueba que el generador la cumple.

[`scripts/verify-acl.mjs`](../../scripts/verify-acl.mjs) aplica lo generado a un nats-server
real y comprueba lo permitido y lo denegado, **la minimalidad de cada permiso** (quitando uno,
el ciclo del servicio se rompe) y que las sondas **detectan** una ACL rota (dos mutantes
sembrados) y la ACL v1 congelada.
- `examples/services.json`: cada subject consumido debe existir en `schemas/`. Añade los
  esquemas de ejemplo que falten.

#### 1.3 `flux provision <services.json> [--dry-run]` — `cli/`

Crea streams `EVT_`/`DLQ_` (`02-naming.md` §3.3) y los consumidores durables con la
configuración canónica (`03-delivery.md` §2). Idempotente. Informa de diferencias. **Nunca
borra.** Usa credenciales de operación, no de servicio.

Implementado como `flux provision <services.json> [--dry-run] [--replicas n] [--bundle …]
[--server …] [--creds …]`. Crea, además, el durable con el que cada dueño lee su DLQ. Sale con
`1` si algún recurso existe con otra configuración (se informa y no se toca) o dio error.
`--replicas` por defecto 1; producción, 3. Los nombres y la expansión de `consumes` los comparte
con `gen-acl.mjs` (`scripts/lib/manifest.mjs`); las configuraciones salen de `protocol.json`.

> **Corregido tras medir.** `--creds` de la CLI **no funcionaba**: se pasaba a una opción
> `creds` que el cliente NATS ignora, y el servidor respondía *Authorization Violation* (A-8e).
> Ahora se entrega con `credsAuthenticator`, para todos los comandos.

#### 1.4 SDKs — Node y Rust ahora; el resto en la Fase 6

Mientras un SDK no lo implemente, no es compatible con la configuración segura (usaría el
inbox `_INBOX.>` común y el broker se lo denegaría). La tabla de capacidades del
`README.md` lo debe decir.

| Cambio | Detalle |
|---|---|
| Prefijo de inbox | `_INBOX.<service>`, siempre. Opción del cliente NATS: `inboxPrefix` (Node; confirmado en `@nats-io/transport-node` 3.4.0 y comprobado que rige también las peticiones de la API de JetStream y las entregas del pull), `inbox_prefix` (Python), `CustomInboxPrefix` (Go), `inboxPrefix` (Java), `InboxPrefix` (.NET), `custom_inbox_prefix` (Rust). **PHP usa inboxes `_REQS.n`: verifica cómo se configura en `basis-company/nats`.** Nombres por confirmar en cada librería. |
| TLS | Opción `tls: { caFile, certFile?, keyFile? }` que pasa al cliente. |
| Credenciales | Node: cablear `.creds` y semilla nkey con los autenticadores del cliente (H4). Los demás ya aceptan `.creds`; añade nkey donde falte. **Hecho en Node:** `creds` es una **ruta** (como `CredsFile` en Go y Rust), más `credsContent`, `nkeySeed`, `token` y `user`+`pass`; un solo método, y todo lo mal formado lanza en `connect()`. |
| `NOT_PROVISIONED` | Si falta el stream o el consumidor y crear está denegado. **Node:** `NotProvisionedError`, PERMANENT. Una petición de la API denegada llega en ~1 ms como `RequestError` con `cause: PermissionViolationError` *(A-21)*: reconócela por la `cause`, no por el texto. |
| Primera conexión | **Añadido tras medir *(A-24)*.** Con `waitOnFirstConnect` el cliente NATS reintenta **cualquier** error sin límite: un certificado que no verifica deja `connect()` esperando para siempre, sin error. El SDK debe esperar a un broker caído y **fallar** ante lo que no se arregla esperando (TLS que no verifica, credenciales rechazadas, TLS pedido y no ofrecido). Node: `ConnectionRejectedError`. |
| Denegaciones asíncronas | **Añadido tras medir *(A-19, A-20)*.** `ack` y el pull son publicaciones sueltas: si se deniegan no hay excepción. Sin vigilarlas, un ack denegado reentrega para siempre y un pull denegado deja un consumidor inerte «sano». Node: `subscribe()` lanza si el pull se deniega; el ack solo puede avisar por `logger`. |
| `NOT_PROVISIONED` (Rust) | Si falta el stream o el consumidor y crear está denegado. Más `PERMISSION_DENIED` si ni consultar está permitido. En `async-nats` la denegación solo llega como `Event::ServerError` (A-28) |
| Conexión inicial (Rust) | **Rust:** `retry_on_initial_connect` de `async-nats` reintenta para siempre y sin distinguir causas, así que una credencial rechazada o un TLS que no encaja dejaban un servicio mudo hasta el primer timeout. `connect()` espera ahora al broker, pero devuelve al instante lo que reintentar no arregla. **Comprueba el mismo patrón en el resto de SDKs** (`waitOnFirstConnect` en Node) |

#### 1.5 Suite de seguridad — `conformance/security.mjs` + `conformance/cases/security-acl.json`

Como `run.mjs`: habla NATS directamente, sin SDK. Pasos: genera claves nkey de prueba
(`@nats-io/nkeys` ya es dependencia transitiva), genera la config con `gen-acl.mjs`,
arranca un `nats-server` con TLS, aprovisiona con `flux provision`, y ejecuta la matriz:

| Caso | Debe |
|---|---|
| S1 | Un servicio publica en su dominio y consume lo concedido, de extremo a extremo |
| S2 | No puede crear un consumidor con un filtro más amplio *(A-4)* |
| S3 | No puede usar `CONSUMER.DURABLE.CREATE` *(A-5)* |
| S4 | No puede leer un stream con `STREAM.MSG.GET` *(A-6)* |
| S5 | No puede pedir mensajes del consumidor de otro servicio *(A-8)* |
| S6 | No puede suscribirse a `_INBOX.>` ni al inbox de otro *(A-7)* |
| S7 | No puede publicar en un dominio ajeno ni en la `dlq.` de lo que no consume |
| S8 | Una conexión sin TLS o con credencial inválida es rechazada |
| S9 | **Regresión:** con la ACL antigua, congelada como fixture, S1 falla y la fuga de A-2 ocurre |
| S10 | El SDK de Node pasa su test de integración bajo esta configuración |
| S11 | El SDK de Rust pasa su test de integración bajo esta configuración |

Job nuevo `security` en `.github/workflows/spec.yml`. Los jobs de integración que ya
levantan NATS (Rust, PHP) pasan a usar la configuración segura.

`docker-compose.yml`: perfil `secure` con TLS y la ACL generada, para desarrollo local.

**Aceptación**

```bash
node conformance/security.mjs                      # S1–S10 en verde
node conformance/security.mjs --acl fixtures/acl-v1.conf   # DEBE fallar en S1 y S6
```

Borra de `docs/plan-v1.1/evidencia/` lo que ya sea un caso (`acl-generada.mjs` y la
parte 3 de `diseno-candidato.mjs`).

---

### Fase 2 — Estado replicado

**Objetivo:** «el dato llega solo». Sustituye `GET /recurso/123`, la carga inicial y el
polling de datos. Filas 3 y 4 del mapa. Cierra H5.

Documento nuevo: `specification/10-state.md`.

#### 2.1 Modelo

- **Stream** `STATE_<DOMINIO>`, subjects `state.<dominio>.>`:

  ```
  max_msgs_per_subject: 1     # un valor por clave: el stream ES el estado actual
  allow_direct:         true  # lecturas puntuales sin consumidor
  max_age:              0     # sin caducidad: el estado vive hasta que se borra
  discard:              old
  duplicate_window:     2m
  replicas:             3 (producción) / 1 (SDK en desarrollo)
  ```

  *(A-9, A-10)*
- **Subject** `state.<dominio>.<agregado>.v<major>.<tenantid>.<clave>` — 6 tokens fijos.
  - El tenant va en el subject para que **el broker** filtre y confine *(A-14)*. Con el
    tenant solo en el sobre, dos tenants con el mismo id de agregado se pisarían en
    silencio.
  - `tenantid` **DEBE** cumplir `^[a-z0-9]+(-[a-z0-9]+)*$`. Si no, `INVALID_TENANT_FOR_STATE`.
  - `clave` **DEBE** cumplir `^[A-Za-z0-9_-]{1,128}$`. Si no, `INVALID_STATE_KEY`. Sin
    codificación automática: una clave que no cabe en un token es un error, no algo que
    cada SDK escape a su manera.
- **Sobre:** un CloudEvent normal. `subject` (el atributo) = la clave, obligatorio.
  `dataclassification` ∈ `public | internal | confidential`.
- **`data` DEBE incluir `aggregateVersion`** (entero ≥ 1, creciente). El esquema lo
  declara `required`; `bundle-schemas.mjs` rechaza un esquema de estado que no lo haga.

#### 2.2 Escribir — solo el dueño

`put` es condicional. Sin esto, dos instancias del dueño pueden dejar un valor viejo como
«actual», y un consumidor nuevo lo recibiría como verdad.

```
1. Leer el último mensaje de la clave (direct get) → secuencia S, versión V. Sin valor: S = 0.
2. Si nueva.aggregateVersion <= V → no escribir. Resultado `stale`. No es un error.
3. Publicar con  Nats-Expected-Last-Subject-Sequence: S   y   Nats-Msg-Id: <id>.
4. Si el broker responde «wrong last sequence» → volver a 1. Máximo 3 intentos.
```

*(A-11)*. El SDK **PUEDE** recordar en memoria la última secuencia de cada clave que él
mismo escribió, para ahorrarse el paso 1.

#### 2.3 Borrar

`delete` publica una **lápida** en el mismo subject: un CloudEvent con
`type: com.flux.state.<dom>.<agr>.deleted.v<M>`, `data: { "aggregateVersion": n }`,
`dataclassification: internal` y `dataschema` fijo
`https://schemas.internal/system/state/eliminado/1.0.0.json`. Al entrar, el valor anterior
sale del stream *(A-13)*.

- La lápida es permanente en 1.1. Compactarlas queda fuera — §6.
- Un consumidor **DEBE** borrar o anonimizar su copia local al recibir una lápida. Es lo
  que hace real el derecho de supresión aguas abajo.

#### 2.4 Leer

- **`watch`** — foto completa y después los cambios, por el mismo canal:
  - durable `<servicio>__state_<dom>_<agr>_v<M>`, con sufijo `__<tenant>` si filtra;
  - `deliver_policy: last_per_subject`; el resto, la config canónica de `03-delivery.md` §2;
  - filtro `state.<dom>.<agr>.v<M>.<tenant>.*` (o `*.*` sin tenant; `tenantIsolation:
    "strict"` lo prohíbe).

  *(A-12)*. Un consumidor que estuvo caído recibe **solo el último valor** de cada clave
  que cambió, no los intermedios.
- **`get`** — una clave, pedida al broker por subject:
  `$JS.API.DIRECT.GET.STATE_<DOM>.<subject>`. Clave inexistente → `404`. *(A-14)*
- La entrada que recibe el handler: `{ key, deleted, data, aggregateVersion, revision }`,
  donde `revision` es la secuencia del stream.
- `onSynced`: se invoca una vez, cuando `num_pending` llega a 0 por primera vez.

#### 2.5 Errores y DLQ

- Misma taxonomía que los eventos (`04-errors.md`).
- Subject de DLQ: `dlq.state.<dom>.<agr>.v<M>` — sin tenant ni clave, para que no se
  multiplique. El stream `DLQ_<DOM>` pasa a capturar `dlq.<dom>.>` **y** `dlq.state.<dom>.>`.
- **El evento de DLQ de estado no lleva el dato**: `data` se sustituye por
  `{ "key": "…", "revision": n }`. Si no, un dato personal borrado seguiría 90 días en la DLQ.
- **El estado no se reproduce desde la DLQ.** Republicar un valor viejo pisaría uno nuevo.
  `flux dlq replay` **DEBE** rechazar `dlq.state.>`. La recuperación es `flux state resync`.

#### 2.6 API de referencia (Node)

```ts
await bus.state.put("clientes.cliente.v1", "cli-1",
  { clienteId: "cli-1", nombre: "Ana", email: "ana@example.com", aggregateVersion: 7 },
  { classification: "confidential" });                       // → { applied: true, revision }

await bus.state.delete("clientes.cliente.v1", "cli-1", { aggregateVersion: 8 });

const actual = await bus.state.get("clientes.cliente.v1", "cli-1");   // entrada | null

await bus.state.watch("clientes.cliente.v1", async (entrada, ctx) => {
  if (entrada.deleted) return borrarLocal(entrada.key);
  await guardarLocal(entrada);        // UPDATE … WHERE aggregate_version < $n
}, { onSynced: () => listo() });
```

`publish()` **DEBE** rechazar subjects que empiecen por `state.`, `rpc.` o `dlq.`.

#### 2.7 Entregables

| Dónde | Qué |
|---|---|
| `specification/10-state.md` | Todo lo anterior, con el porqué de cada regla |
| `specification/06-security.md` §5.1 | Reescribir según §4.4 de este plan |
| `specification/02-naming.md` | Nombres reservados; stream `STATE_`; durable de estado |
| `specification/08-observability.md` | `flux_state_writes_total{contract,outcome}` con `outcome = ok \| stale \| conflict \| invalid_schema`. Las métricas de consumo existentes se reutilizan con `subject="state.<contrato>"`. **Nunca** la clave ni el tenant como etiqueta. |
| `protocol.json` | `version: "1.1.0"`; bloque `state`; patrones nuevos; `security.piiInPayload` por primitiva |
| `scripts/check-protocol-json.mjs` | Casos que deben pasar y fallar para cada patrón nuevo |
| `scripts/bundle-schemas.mjs`, `check-compat.mjs` | Recorrer `schemas/state/`; índice `state` en el bundle |
| `scripts/gen-acl.mjs` | Clave `watches`. Dueño: publica en `state.<dom>.>`. Observador: los cuatro permisos de 1.1 sobre `STATE_<DOM>`, `DIRECT.GET` acotado a su tenant y `dlq.state.<contrato>` |
| `cli/` | `flux state ls \| get \| resync`; `provision` crea `STATE_`; `doctor` audita su config; `tail` entiende `state.` |
| `sdk-node/`, `sdk-rust/` | La API de 2.6, con tests contra NATS real |
| `conformance/cases/` | Un caso por cada observación A-9 … A-14 |
| `conformance/harness/` | Operaciones `state-build`, `state-tombstone`, `state-subject`, `state-dlq`, con vectores |
| `conformance/security.mjs` | Otro servicio no escribe estado ajeno; un tenant no lee el de otro |
| `schemas/state/clientes/cliente/1.0.0.json` | Esquema de ejemplo, con `x-pii` |

**Aceptación:** la definición de hecho, más un test de extremo a extremo: el dueño escribe
100 claves, un observador nuevo las recibe todas, se borra una, el observador recibe la
lápida, y `get` de esa clave devuelve `null`.

---

### Fase 3 — Petición/respuesta

**Objetivo:** órdenes y cálculos con respuesta inmediata, sin HTTP. Filas 5 y 6.

Documento nuevo: `specification/11-request-reply.md`.

#### 3.1 Modelo

- **Subject** `rpc.<dominio>.<agregado>.v<major>.<operacion>`. La operación va en
  **infinitivo**, `kebab-case`: `obtener`, `crear`, `calcular-precio`.
- **Core NATS**, no JetStream. Una petición no se persiste: si nadie responde, quien
  llama lo sabe al instante *(A-15)*.
- **Ningún stream DEBE capturar `rpc.>`.** Si uno lo hace, quien llama recibe el acuse de
  JetStream como si fuera la respuesta *(A-18)*. `flux doctor` lo comprueba.
- **Solo el dueño responde**, suscrito a `rpc.<dominio>.>` con *queue group* = su nombre
  de servicio. Así varias instancias se reparten las peticiones.
- **Sobre:** petición y respuesta son CloudEvents. La respuesta hereda `correlationid` y
  `tenantid`; su `causationid` es el `id` de la petición.

#### 3.2 Errores

La respuesta de error es un CloudEvent `type: com.flux.rpc.error.v1` con
`data: { class, code, message, retryAfterMs? }`, donde `class` es la taxonomía de
`04-errors.md`. El SDK de quien llama lo convierte en `RetryableError` o `PermanentError`.
Así, un handler de eventos que llama a una operación hereda la clasificación sin hacer nada.

| Situación | Código | Clase |
|---|---|---|
| Nadie escucha | `RPC_UNAVAILABLE` | RETRYABLE — inmediato |
| No llega respuesta a tiempo | `RPC_TIMEOUT` | RETRYABLE |
| La petición no cumple su esquema | `INVALID_SCHEMA` | PERMANENT |
| Petición firmada fuera de la ventana | `STALE_REQUEST` | PERMANENT |

Timeout por defecto **5 s**, máximo **25 s**: por debajo del `ack_wait`, para que una
llamada hecha desde un handler no provoque su reentrega.

#### 3.3 Reglas

- **Idempotencia.** Una orden puede llegar dos veces. El handler **DEBE** deduplicar por
  el `id` de la petición. Si el SDK reintenta, **reutiliza el mismo `id`**.
- **Tamaño.** 1 MiB en cada sentido. Más: claim-check, o paginar con un cursor en `data`.
- **No es para leer datos guardados.** Eso es estado — §2.

#### 3.4 Seguridad

- Quien llama: publicación sobre el subject **exacto** de cada operación concedida. Clave
  `calls` en el manifiesto *(A-17)*.
- El dueño: suscripción a `rpc.<dominio>.>` y `allow_responses: true`. No necesita
  permiso de publicación sobre ningún inbox *(A-16)*.
- **Sin el inbox privado de la Fase 1, cualquiera leería las respuestas ajenas.**
- Con firma en modo `require`: el dueño rechaza peticiones cuyo `time` se aleje más de
  300 s. La firma por sí sola no impide reenviar una petición capturada. La respuesta va
  firmada por el dueño.

#### 3.5 API de referencia (Node)

```ts
const cliente = await bus.request("clientes.cliente.v1.calcular-descuento",
  { clienteId: "cli-1", totalCents: 9990 }, { timeoutMs: 2000 });

await bus.respond("clientes.cliente.v1.calcular-descuento", async (peticion, ctx) => {
  return { descuentoCents: 500 };            // lanzar PermanentError / RetryableError para fallar
});
```

#### 3.6 Entregables

| Dónde | Qué |
|---|---|
| `specification/11-request-reply.md` | Todo lo anterior |
| `specification/00-protocol.md` §6 | Quitar «request/reply» de fuera de alcance |
| `specification/08-observability.md` | `flux_rpc_client_requests_total{operation,outcome}`, `flux_rpc_server_requests_total{operation,outcome}`, `flux_rpc_duration_seconds{operation,role}` |
| `protocol.json`, `check-protocol-json.mjs` | Bloque `rpc`; quitar de `outOfScopeV1`; patrones |
| `scripts/bundle-schemas.mjs`, `check-compat.mjs` | `schemas/rpc/`; índice `rpc` |
| `scripts/gen-acl.mjs` | Clave `calls`; `allow_responses` para el dueño |
| `cli/` | `doctor`: ningún stream captura `rpc.>`. `flux call <operacion> --data '{…}'` para depurar |
| `sdk-node/`, `sdk-rust/` | `request` y `respond` |
| `conformance/cases/` | A-15 … A-18, y reparto por *queue group* |
| `conformance/harness/` | Operaciones `rpc-request`, `rpc-reply`, `rpc-error`, con vectores |
| `conformance/security.mjs` | Llamada no concedida denegada; respuesta ajena no legible |

**Aceptación:** la definición de hecho, más: una operación atendida por dos instancias
reparte las peticiones; al parar ambas, quien llama recibe `RPC_UNAVAILABLE` en menos de
100 ms.

Borra lo que quede en `docs/plan-v1.1/evidencia/`.

---

### Fase 4 — Gateway de borde

**Objetivo:** que lo que **no puede** mantener una conexión NATS siga dentro del
ecosistema. Filas 8, 9 y 10. HTTP queda solo en el borde; dentro, nada lo usa.

Paquete nuevo `gateway/`, en Node, sobre `sdk-node`. Sin framework web: `node:http`.

#### 4.1 Modelo

**El gateway aloja servicios; no los suplanta.** Cada cliente de borde es un servicio
flux normal del manifiesto, con sus credenciales y sus permisos, cuya conexión NATS vive
dentro del gateway. Así `source`, los durables y la ACL son los suyos, y el gateway no
necesita ningún permiso especial. En el manifiesto: `"via": "gateway"`.

#### 4.2 Entrada — HTTP → flux

| Ruta | Hace |
|---|---|
| `POST /v1/events/{subject}` | Publica. Cuerpo = `data`. Responde `202 { id }` |
| `PUT` · `DELETE` · `GET /v1/state/{contrato}/{clave}` | Estado |
| `POST /v1/rpc/{operacion}` | Petición/respuesta |
| `POST /v1/hooks/{nombre}` | Webhook de un tercero → evento |

- Autenticación: `Authorization: Bearer <token>`. El gateway guarda solo el SHA-256 del
  token y compara en tiempo constante.
- Webhooks de terceros: verificación HMAC-SHA256 configurable (cabecera, secreto por
  variable de entorno, tolerancia de tiempo).
- **Duplicados de terceros — `id` determinista.** Un proveedor reenvía el mismo aviso
  durante horas. Si cada reenvío recibe un `id` nuevo, ningún consumidor puede
  deduplicarlo. Regla nueva en `01-envelope.md` §2.6: para un hecho importado, el `id` es
  un UUIDv7 con los milisegundos del hecho externo y los 74 bits restantes tomados de
  `SHA-256(<source> "|" <clave externa>)`. Es un UUIDv7 válido y es siempre el mismo. API:
  `publish(…, { externalKey, time })`. Vector cruzado obligatorio.

#### 4.3 Salida — flux → HTTP

- Por cada cliente, una lista de entregas: subject o contrato de estado → URL.
- Cuerpo: el CloudEvent tal cual, `Content-Type: application/cloudevents+json`.
- Cabecera `Flux-Signature: t=<unix>,v1=<hmac-sha256(secreto, t + "." + cuerpo)>` e
  `Idempotency-Key: <id>`.
- Reintentos con la taxonomía de siempre: `2xx` → ack; `429`/`5xx`/timeout → RETRYABLE;
  otro `4xx` → PERMANENT → DLQ.

#### 4.4 Seguridad

- Límite de cuerpo: 1 MiB. Límite de peticiones por token.
- La configuración (`gateway.json`) no contiene secretos: solo nombres de variables.
- No se registra el `data` de eventos `confidential`.
- TLS termina delante (proxy inverso) o en el propio gateway; documenta las dos.

#### 4.5 Entregables

`gateway/` con tests de extremo a extremo contra NATS real, `Dockerfile`, entrada en
`docker-compose.yml`, job en `spec.yml`. `gen-acl.mjs` entiende `"via": "gateway"`.
Documento `specification/12-edge.md`: es normativo solo el formato de entrega saliente
y el `id` determinista; el resto del gateway es una implementación, no el protocolo.

**Aceptación:** un script con `curl` publica un evento, escribe y lee estado y hace una
llamada; un servidor HTTP de prueba recibe una entrega saliente y verifica su firma; el
mismo webhook de tercero enviado tres veces produce **un** efecto en el consumidor.

---

### Fase 5 — Medición, ejemplo y guía

**Objetivo:** demostrar la mejora y dejar el camino de adopción hecho. Cierra H6.

#### 5.1 `bench/`

| Escenario | Línea base HTTP | flux |
|---|---|---|
| **A. Enterarse de un cambio** — N consumidores, C cambios por minuto | Polling cada T segundos | `state.watch` |
| **B. Avisar a muchos** — 1 productor, N receptores | N webhooks por hecho | 1 evento |
| **C. Llamada con respuesta** | `node:http`, con y sin keep-alive | `request` |

Se mide: peticiones emitidas, segundos de CPU de cada proceso, bytes en la red, latencia
p50/p99. La línea base **DEBE** ser justa: HTTP con keep-alive, no el peor caso.

`bench/RESULTS.md` se genera, con la máquina y las versiones. **Se publica lo que salga,
favorezca o no.** El `README.md` solo cita cifras de ese fichero.

#### 5.2 `examples/ecosistema/`

Tres servicios y un cliente de borde, arrancables con un comando:

- `crm` — dueño de `clientes`: estado, eventos y una operación.
- `tienda-web` — observa `clientes`, publica `pedidos`.
- `facturacion` — consume `pedidos`, llama al `crm`.
- `web-php` — un script que habla solo con el gateway.

Test de extremo a extremo en CI. Bajo la configuración segura de la Fase 1.

#### 5.3 Guía y herramientas

- `docs/migracion.md`: el mapa de §2 fila a fila, con el antes (HTTP) y el después (flux).
  Incluye el patrón de la fila 7.
- `flux new event | state | rpc <nombre>`: crea el esquema con `$id` correcto y los campos
  obligatorios, y regenera el bundle.
- `README.md`, `AGENTS.md`, `llms.txt`, `.cursor/rules/flux.mdc`,
  `.github/copilot-instructions.md`: flux pasa de «contrato de eventos» a «protocolo de
  integración: eventos, estado y petición/respuesta». `AGENTS.md` gana las reglas duras
  de estado y de petición/respuesta, y una sección «qué primitiva usar».

**Aceptación:** `bench/RESULTS.md` existe y es reproducible con un comando; el ejemplo
pasa su test en CI.

---

### Fase 6 — Paridad de SDKs

Estado, petición/respuesta, `id` determinista y los permisos de la Fase 1 (inbox
privado, TLS, credenciales, `NOT_PROVISIONED`) en los cinco SDKs restantes. Cuatro PRs, en
este orden:

1. Go
2. Java y .NET
3. PHP
4. Python — el último, por decisión del dueño

Cada uno: arnés ampliado con las operaciones nuevas, vectores cruzados en verde, test de
integración contra NATS bajo la configuración segura, columna de capacidades en el
`README.md`.

**PHP:** `watch` y `respond` necesitan un proceso de larga duración. Documenta que en
hosting compartido el camino es el gateway.

**Aceptación:** `node conformance/cross-sdk.mjs` con los siete SDKs y todas las
operaciones nuevas; `"pending": []` en `conformance/harnesses.json`.

---

### Fase 7 — Cifrado del payload *(opcional — necesita decisión del dueño del repo)*

Solo si hace falta confidencialidad frente a quien opera el broker o lee sus copias de
seguridad. Las fases 1–6 no dependen de esta.

Documento `specification/13-encryption.md`. Diseño de partida:

- Se cifra `data`; el resto del sobre queda legible para enrutar y medir.
- **AES-256-GCM**, sin negociación. Disponible en los siete lenguajes sin dependencias exóticas.
- `datacontenttype: application/flux-encrypted+json` y
  `data: { "kid", "iv", "ct" }`. Un SDK que no entienda el cifrado lo trata como POISON
  por `datacontenttype` no soportado: falla cerrado, sin atributos nuevos en el sobre.
- Datos autenticados adicionales: `id`, `source`, `type` y `tenantid`.
- Orden: validar → cifrar → firmar. Al consumir: verificar → descifrar → validar.
- Una clave por contrato, repartida por el gestor de secretos; rotación como la de firma.
- Vectores cruzados con `iv` fijo, solo para test.

---

## 6. Fuera de alcance — declarado, no olvidado

| Tema | Por qué |
|---|---|
| Tiempo real hacia el navegador (SSE, WebSocket) | El navegador es cliente de una web, no un sistema del ecosistema. Cada web lo sirve desde su backend. |
| Compactar lápidas de estado | Optimización de almacenamiento. Una lápida pesa unos cientos de bytes. |
| Borrado físico garantizado en disco | Depende de la compactación del broker. Mitigación: cifrado en reposo de JetStream; garantía fuerte: Fase 7. |
| Propagar el plazo de una petición al que responde | La idempotencia por `id` cubre el caso peligroso. |
| Auditoría inmutable de accesos | Sigue siendo los logs y *advisories* del servidor NATS. |
| Sagas, Protobuf/Avro, federación multi-cluster | Igual que en v1. |

---

## 7. Riesgos y puntos por verificar

| Riesgo | Qué hacer |
|---|---|
| El formato de `$JS.ACK.…` cambia con dominios JetStream o entre versiones | Caso de conformidad que fije el formato observado (A-3). Si cambia, la ACL se rompe en alto, no en silencio. |
| Algún cliente NATS no permite fijar el prefijo de inbox, o usa la API antigua de creación | Verificar librería por librería en 1.4. Si una no puede, ese SDK exige consumidores preaprovisionados y se documenta. |
| Un `durable` personalizado en `subscribe()` no coincide con la ACL | Falla con violación de permisos. Decláralo en el manifiesto o no lo uses. |
| Dos instancias del mismo servicio con distinto `tenantId` comparten durable de **eventos** y cada una descarta los del otro | Existe hoy. Evalúalo en la Fase 2 al definir el sufijo de tenant del durable de estado; si se confirma, abre un caso. |
| `flux provision` y el auto-aprovisionado del SDK divergen | Una sola fuente de la config canónica: `protocol.json`. |
| El alcance es grande: siete SDKs por tres primitivas | Por eso Node y Rust primero. Las fases 1–5 ya dan un sistema completo; la 6 es paridad. |

---

## Apéndice A — Evidencia

Observado el **2026-11-26** contra **nats-server 2.14.5**, con `@nats-io/jetstream` 3.4.0.
Reproducible: [`evidencia/ejecutar.sh`](evidencia/ejecutar.sh).

| # | Qué se probó | Resultado observado |
|---|---|---|
| A-1 | Con la ACL generada hoy, `facturacion-api` consulta el stream que tiene concedido | `Permissions Violation for Publish to "$JS.API.STREAM.INFO.EVT_PEDIDOS"`. Igual para `$JS.API.INFO` y `$JS.API.CONSUMER.CREATE.…` |
| A-2 | Con la ACL generada hoy, `facturacion-api` (sin concesión sobre `inventario`) se suscribe a `_INBOX.>` mientras otro servicio consume `inventario.stock.v1.ajustado` | **Leyó el evento completo.** La suscripción directa a `inventario.>` sí fue denegada |
| A-3 | ACL candidata: crear el consumidor, consumir, WIP y ack | Funciona. Subject de ack: `$JS.ACK.EVT_PEDIDOS.<durable>.1.1.1.<ts>.0` |
| A-4 | Mismo subject de API, pero con un filtro más amplio en el cuerpo | `Consumer create request did not match filtered subject from create subject` |
| A-5 | API antigua `$JS.API.CONSUMER.DURABLE.CREATE.…` | `Permissions Violation` |
| A-6 | Leer el stream con `$JS.API.STREAM.MSG.GET.…` | `Permissions Violation` |
| A-7 | Otro servicio se suscribe a `_INBOX.>` y a `_INBOX.fx-fact.>` | Denegadas las dos |
| A-8 | Otro servicio pide mensajes del consumidor ajeno (`CONSUMER.MSG.NEXT`) | `Permissions Violation` |
| A-8a | El dueño de un dominio lee su DLQ con un pull consumer: `CONSUMER.CREATE.DLQ_<DOM>.<durable>.dlq.<dom>.>` exacto, y sin `STREAM.INFO` | Crea el durable y consume. Con filtro `dlq.>`, o sobre la DLQ de otro dominio: `Permissions Violation` |
| A-8b | El SDK de Node bajo la ACL generada, quitando permisos de uno en uno (`scripts/verify-acl.mjs`) | Cada permiso es necesario **salvo `CONSUMER.CREATE`** con un durable aprovisionado (cae a `CONSUMER.INFO`). Sin `STREAM.INFO.DLQ_<DOM>`, `subscribe()` intenta `STREAM.CREATE.DLQ_<DOM>` y falla |
| A-8c | El SDK real de Node, sin modificar, con `inboxPrefix` y autenticador nkey inyectados por `credentials` | `connect`, `subscribe`, `publish`, ruta a la DLQ y lectura de la DLQ por el dueño: todo funciona bajo la ACL generada |
| A-8d | La misma nkey en dos accounts de una config | `nats-server` no arranca: `Duplicate nkey` |
| A-8e | `.creds` con un JWT de relleno contra un servidor con nkeys estáticas; y la opción `creds` del cliente NATS | El `credsAuthenticator` autentica (solo importa la semilla). La opción `creds` se ignora: `Authorization Violation` |
| A-8f | Cómo se manifiesta una denegación en `@nats-io/*` 3.4.0 | Una petición sin permiso falla en ~2 ms con `Permissions Violation for Publish`. Con el inbox por defecto, la primera petición falla en ~3 ms (`…for Subscription to "_INBOX.….*"`). Una suscripción denegada **no lanza** al suscribirse, pero su iterador **lanza** `PermissionViolationError` al consumirlo |
| A-9 | Stream con `max_msgs_per_subject: 1`: 3 escrituras sobre 2 claves | 2 mensajes en el stream |
| A-10 | Lectura puntual con *direct get* (`last_by_subj`) | Devuelve el último valor |
| A-11 | Escribir con `Nats-Expected-Last-Subject-Sequence` desfasada, y crear una clave existente con `0` | `wrong last sequence: N` en ambos |
| A-12 | Consumidor con `deliver_policy: last_per_subject` y filtro con comodín | Recibe el último valor de cada clave y, después, los cambios en vivo |
| A-13 | Publicar una lápida sobre una clave | El valor anterior desaparece; queda 1 mensaje por clave |
| A-14 | Estado por tenant bajo ACL: crear el consumidor con filtro de todos los tenants; *direct get* por subject de otro tenant; clave inexistente | Denegados los dos primeros. La librería usa la forma `$JS.API.DIRECT.GET.<stream>.<subject>`, así que la ACL puede acotar por tenant. Clave inexistente: `404 Message Not Found` |
| A-15 | Petición a una operación sin nadie escuchando, con timeout de 5 s | `no responders`, en ~1 ms |
| A-16 | Petición atendida por un servicio con `allow_responses: true` y sin permiso de publicación sobre inboxes | Respuesta correcta en ~0,7 ms (local) |
| A-17 | Llamar a una operación no concedida | `Permissions Violation` |
| A-18 | Petición a un subject que captura un stream | La «respuesta» recibida es el acuse de JetStream: `{"stream":"TRAMPA","seq":1}` |
| A-19 | Pull consumer con permisos exactos **sin** `$JS.ACK.<S>.<D>.>` | Se entrega y se procesa; nunca se confirma: `ack_floor=0`, el evento se **reentrega cada `ack_wait`, sin fin**. Ninguna excepción: solo un evento de estado `PermissionViolationError` en `nc.status()` |
| A-20 | Pull consumer **sin** `$JS.API.CONSUMER.MSG.NEXT.<S>.<D>` | `consume()` no lanza y el consumidor queda inerte, parece sano. Solo el evento de estado. **El primer pull no se publica hasta que alguien itera** el consumidor |
| A-21 | Petición de la API de JetStream denegada; suscripción denegada | Petición: `RequestError` con `cause: PermissionViolationError`, en ~1 ms (no timeout). Suscripción denegada: el **iterador** de esa suscripción lanza `PermissionViolationError`. «Stream no existe» es `code 10059` y «consumidor no existe» `10014`, distintos de la denegación |
| A-22 | Espía con `_INBOX.>` mientras un servicio consume por pull con inbox privado | El espía recibe **el evento completo**. Las entregas de un pull llegan con el **subject original** del evento aunque viajen por el inbox: así funciona la fuga H2, y por eso solo la ACL la evita, no el SDK |
| A-23 | `.creds` contra un broker en modo operador/JWT (JWT firmados a mano con `nkeys`, sin `nsc`) | `.creds` válido conecta; con la semilla de otra clave y sin credenciales: `AuthorizationError`. Ruta y contenido se comportan igual |
| A-24 | `connect()` con `waitOnFirstConnect: true` y un certificado de servidor que no verifica | **No falla: se cuelga.** Con `false`, falla en ~5 ms con `UNABLE_TO_VERIFY_LEAF_SIGNATURE` (CA que falta o equivocada) o `ERR_TLS_CERT_ALTNAME_INVALID` (host fuera de los SAN). Un servidor sin TLS ante `tls` pedido: `ConnectionError "server does not support 'tls'"` (no baja a texto plano). mTLS sin certificado de cliente: solo `TimeoutError`; el motivo está en el log del servidor |
| A-25 | Usuario con `subscribe: { allow: [] }` | **Sin restricción**: una lista vacía no deniega. La variante «sin inbox propio» de la matriz de tests no fallaba hasta sustituir la lista vacía por el inbox de otro servicio |
| A-26 | SDK de Node con la candidata exacta | `subscribe()` falla con `Permissions Violation for Publish to "$JS.API.STREAM.INFO.DLQ_PEDIDOS"`: falta ese permiso en `candidata.conf` |
| A-27 | Consumidor aprovisionado por `ops`, servicio **sin** `CONSUMER.CREATE` | Funciona si el SDK pregunta antes de crear (`CONSUMER.INFO`); sin consumidor y sin ese permiso: `NotProvisionedError`, en milisegundos y no tras un timeout |
| A-28 | SDK de Rust (`async-nats` 0.50) con una publicación denegada por la ACL: ¿qué ve quien la hizo? | **Nada**: el servidor descarta el mensaje y manda `-ERR 'Permissions Violation for Publish to "…"'` por la conexión; la petición espera a su timeout. Solo es observable como `Event::ServerError(ServerError::Other(texto))`. El log **del servidor** dice otra cosa: `Publish Violation - Subject "…"`. Fuente: `sdk-rust/tests/security.rs` |
| A-29 | SDK de Rust bajo permisos exactos: subjects que publica un consumidor y un dueño en un flujo completo (publicar, consumir, ack, WIP, nak, term, DLQ, sondeo) | `STREAM.INFO.EVT_<D>`, `STREAM.INFO.DLQ_<D>` (¡no estaba en la candidata!), `CONSUMER.CREATE.<S>.<d>.<subject>`, `CONSUMER.INFO.<S>.<d>`, `CONSUMER.MSG.NEXT.<S>.<d>`, `$JS.ACK.<S>.<d>.<…>`, `dlq.<subject>`; ninguno de `$JS.API.INFO`. Toda petición responde bajo `_INBOX.<servicio>.` incluida la del pull (`MSG.NEXT`) |
| A-30 | Fallos de conexión inicial en `async-nats`: credencial rechazada, CA que no firmó, TLS exigido sin TLS en el broker, mTLS sin certificado de cliente | `AuthorizationViolation` el primero; **los tres de TLS llegan como `ConnectErrorKind::Io`** con un `io::Error InvalidData` dentro (`UnknownIssuer`, `InvalidContentType`, `CertificateRequired`), no como `Tls` |
A-19 a A-27 los reproduce `sdk-node/test/integration/` (`acl.test.ts`, `auth.test.ts`,
`tls.test.ts`), contra nats-server 2.14.5 y `@nats-io/*` 3.4.0. Les falta su caso en
`conformance/cases/` (Fase 1.5): son observaciones del broker, no solo del SDK de Node.
A-28 a A-30 los reproduce `sdk-rust/tests/security.rs`, contra nats-server 2.14.5 y
`async-nats` 0.50. Tampoco tienen aún su caso en `conformance/cases/`.
