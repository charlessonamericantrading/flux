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
| 5 | `POST /accion` que necesita respuesta ya | **Petición/respuesta** | ⏳ Fase 3: spec y esquemas hechos; SDKs, permisos y CLI pendientes |
| 6 | `GET /precio?…`: un cálculo, no un dato guardado | **Petición/respuesta** | ⏳ Fase 3: ídem |
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

Verificado ejecutando, no leyendo. Reproducible con `node conformance/security.mjs`,
`node conformance/state-broker.mjs` y `node conformance/rpc-broker.mjs` (los experimentos ad-hoc de `evidencia/` ya son casos).

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
nombres del protocolo, **y en el broker** (`subjects overlap with an existing stream`, A-43).
**Hoy `dlq` no está reservado y debería**: se arregla en la Fase 2. **Hecho** en `02-naming.md` §1.3,
`protocol.json` (`naming.reservedDomains`) y `bundle-schemas.mjs`/`check-compat.mjs`; la API de los
SDKs responde con `RESERVED_DOMAIN`. Pendiente: `gen-acl.mjs` y `flux validate`.

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

  > **Corregido tras medir.** La versión original de este plan, y la candidata a mano de entonces,
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
    el SDK **pregunta primero** (`CONSUMER.INFO`) y solo crea si no existe. *(Antes intentaba
    crear primero: cada arranque dejaba una `Publish Violation` en el log del servidor aunque
    el SDK la absorbiera.)*
  - `$JS.API.CONSUMER.INFO.…` es **imprescindible**: lo primero que pregunta `subscribe()`; sin
    él falla con `PERMISSION_DENIED`. Lo usa además el sondeo de `flux_consumer_pending` (si hay
    métricas).
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
- Emite los permisos de 1.1. La forma objetivo se escribió a mano en
  `evidencia/candidata.conf`, con las dos diferencias de arriba
  (la DLQ de los streams consumidos y la DLQ propia). Tras la 1.5 la genera `gen-acl.mjs` y la ejecuta
  la suite; los usuarios de estado y de petición/respuesta son `acls.estado` de `state-broker.json` y `acls.rpc` de
  `rpc-semantics.json` (la carpeta `evidencia/` ya no existe; está en el historial de git).
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
  (`kind` ∈ `events | dlq | state | dlq-state`; los de estado llevan además `contract` y `tenant`). Con la
  Fase 2, cada elemento trae también `stateOwns` y `watches`.
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

Como `run.mjs`: habla NATS directamente, sin SDK (salvo S10 y S11, que existen justo para lo
contrario). Pasos: genera claves nkey de prueba (`@nats-io/nkeys`, dependencia directa de
`conformance/`), genera la config con `gen-acl.mjs`, arranca un `nats-server` con TLS,
aprovisiona con `flux provision`, y ejecuta la matriz.

**Al revés que `run.mjs`: los casos son DATOS y el runner los ejecuta.**
[`conformance/cases/security-acl.json`](../../conformance/cases/security-acl.json) define el
vocabulario de operaciones y, por caso, `spec`, `rationale`, `observed` y `counterExample`;
[`conformance/security.mjs`](../../conformance/security.mjs) no sabe qué es S1. La matriz:

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

> **Hecho, y corregido tras medir** (nats-server 2.14.5, 2026-11-26). Además de S1–S11, la suite
> lleva S12 (A-19, A-20: sin permiso de ack o de pull el fallo es silencioso), S13 (A-21: denegado,
> «no existe» y «no hay respuesta» se distinguen) y S14 (A-25: una lista vacía no deniega). Lo que
> la ejecución cambió respecto a lo escrito arriba:
>
> - **S8, texto plano.** La librería de NATS sube sola a TLS contra un servidor que lo exige
>   (`nats://` acaba en `UNABLE_TO_VERIFY_LEAF_SIGNATURE`, no en texto plano), así que no prueba
>   que una credencial en claro se rechace. Se habla el protocolo a mano por un socket: el
>   servidor cierra sin contestar en cuanto llegan bytes que no son un ClientHello.
> - **`flux provision` por TLS.** La CLI no tiene `--ca`: la CA de la PKI efímera le llega por
>   `NODE_EXTRA_CA_CERTS`. Un operador con una CA privada tendría que hacer lo mismo.
> - **S11 destapó que Rust no cumplía «info antes de crear», y ya está corregido.** §1.1 decide que
>   los SDKs preguntan por el consumidor (`CONSUMER.INFO`) antes de crearlo, y Node lo hacía (S10:
>   cero violaciones en el log del servidor). El SDK de Rust intentaba `CONSUMER.CREATE` primero:
>   bajo la ACL generada, que no lo concede, cada `subscribe()` dejaba **una `Publish Violation`**
>   en el log del servidor aunque el ciclo funcionase. Una violación de permiso es una señal de
>   alerta: una por arranque la vuelve ruido y tapa las reales. La alineación de Rust lo corrigió, y
>   S11 exige ahora `serverViolations: 0`, igual que S10.
> - **Los jobs de integración de Rust y PHP siguen contra un NATS abierto.** Pasarlos a la
>   configuración segura queda para la Fase 6, junto a la paridad de los SDKs.

Job nuevo `security` en `.github/workflows/spec.yml`, justo después de `acl-verify`, que se queda:
no es redundante (verifica el generador, `flux provision`, la minimalidad de cada permiso, `--dev` y
el Modelo B). Que los jobs de integración que ya levantan NATS (Rust, PHP) pasen a usar la
configuración segura queda pendiente (ver arriba).

`docker-compose.yml`: perfil `secure` con TLS y la ACL generada, para desarrollo local.
[`scripts/dev-secure.mjs`](../../scripts/dev-secure.mjs) genera en `.dev-secure/` (ignorado por git)
la CA, las nkeys, la ACL y la configuración del servidor que el perfil monta. Verificado arrancando
`nats-server` directamente con esa configuración y haciendo un ciclo con el SDK de Node; el perfil
de compose en sí no se ha podido ejecutar (el contenedor de desarrollo no tiene Docker).

**Aceptación**

```bash
FLUX_REQUIRE_BROKER=1 node conformance/security.mjs                  # S1–S20 en verde
node conformance/security.mjs --acl conformance/fixtures/acl-v1.conf # DEBE fallar en S1 y S6
```

Borra de `docs/plan-v1.1/evidencia/` lo que ya sea un caso (`acl-generada.mjs` y la
parte 3 de `diseno-candidato.mjs`). *(Hecho; y la Fase 3 se llevó lo que quedaba.)*

---

### Fase 2 — Estado replicado

**Objetivo:** «el dato llega solo». Sustituye `GET /recurso/123`, la carga inicial y el
polling de datos. Filas 3 y 4 del mapa. Cierra H5.

Documento nuevo: [`specification/10-state.md`](../../specification/10-state.md), **normativo**. Lo que
sigue es el plan tal como quedó **tras medir y escribir la spec**: la fase se hizo en un frente de
spec e infraestructura de esquemas, y los de SDKs, permisos y CLI los implementan sobre ese
contrato. Donde esto y la spec discrepen, manda la spec.

> **Corregido tras medir.** Lo que la ejecución contra nats-server 2.14.5 (Apéndice A, A-31 … A-50)
> cambió respecto al plan original, y por qué:
>
> - **El conflicto de secuencia tiene DOS códigos, no uno.** El plan decía «wrong last sequence»
>   (10071). En un cluster de 3 nodos, una carrera da **10164** y sin número; el 10071 solo sale
>   sin carrera. Un SDK que reconozca solo el 10071 pasa todas las pruebas contra un nodo y, en
>   producción, no reintenta. *(A-33, A-49; el mutante `only-10071` solo lo detecta el caso de
>   cluster.)*
> - **El chequeo de secuencia va ANTES que el de duplicados.** Un reenvío con el mismo
>   `Nats-Msg-Id` y la `S` vieja da el error de secuencia, no `duplicate`: `duplicate_window` no
>   protege a `put`, y un acuse perdido convertiría una escritura buena en `stale`. Se añade el
>   **paso 1b** (reconocer la propia escritura por el `id`, que el broker conserva en las
>   cabeceras) y la regla de **mismo `id` y mismos bytes en todos los intentos**. *(A-32, A-33)*
> - **`STREAM.INFO` con `subjects_filter` lista todas las claves y todos los tenants.** Se **deja de
>   conceder `STREAM.INFO.STATE_<DOM>` a los observadores** y el SDK no lo usa: `watch` y `get`
>   funcionan sin él (`CONSUMER.INFO` distingue stream y consumidor ausentes). Eso obliga a una
>   excepción acotada a «`DIRECT.GET` nunca» de [06-security.md §3.5](../../specification/06-security.md): la forma **por subject** de
>   un contrato y un tenant; la forma de cuerpo sigue denegada, y el broker rechaza un cuerpo en la
>   forma por subject (408). *(A-38, A-39, A-48)*
> - **El broker no valida las claves**: acepta `*`, `a.b` (dos tokens: la entrada queda fuera de
>   todo filtro) y 2000 caracteres. La validación es del SDK, con coincidencia **completa** (en Python
>   y PHP el `$` acepta un `\n` final). La clave no puede empezar por `_` (reservado). *(A-41)*
> - **El tenant de estado** tiene un máximo de 64 caracteres (los nombres de consumidor admiten 255,
>   A-42) y **no tiene valor por defecto**: ni `system` implícito.
> - **La entrada lleva `tenantId`** además de `{ key, deleted, data, aggregateVersion, revision }`: un
>   `watch` sin tenant recibe `cli-1` de varios y la clave sola mezclaría los tenants en la copia local.
> - **`onSynced`** se define con precisión (§2.4): `num_pending` solo no basta.
> - **Dominios reservados**: también `system` (el plan lo daba por reservado solo en §4.3), y la API
>   rechaza con `RESERVED_DOMAIN`, no con «subject inválido». *(A-43)*
> - **`DLQ_<DOM>` captura `dlq.state.<dom>.>` solo si el dominio tiene estado**: 1.1 sigue siendo
>   aditiva. *(A-44)*

#### 2.1 Modelo

- **Stream** `STATE_<DOMINIO>`, subjects `state.<dominio>.>`, **un valor por clave**:

  ```
  max_msgs_per_subject: 1     # un valor por clave: el stream ES el estado actual
  allow_direct:         true  # lecturas puntuales sin consumidor
  max_age:              0     # sin caducidad: el estado vive hasta que se borra
  discard:              old   # NO `new`: con discard_new_per_subject sería un create-only (A-46)
  duplicate_window:     2m    # no interviene en las escrituras: el chequeo de secuencia va antes
  replicas:             3 (producción) / 1 (SDK en desarrollo)
  ```

  *(A-31, A-32, A-46, A-49)*. `flux doctor` audita las cuatro líneas que, mal puestas, no dan error.
- **Subject** `state.<dominio>.<agregado>.v<major>.<tenant>.<clave>` — 6 tokens fijos.
  - El tenant va en el subject para que **el broker** filtre y confine *(A-38)*. Con el tenant
    solo en el sobre, dos tenants con el mismo id de agregado se pisarían en silencio.
  - `tenant`: `^[a-z0-9]+(-[a-z0-9]+)*$`, **64 caracteres como máximo**. Si no,
    `INVALID_TENANT_FOR_STATE`. Sin valor por defecto.
  - `clave`: `^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$` (**no empieza por `_`**). Si no, `INVALID_STATE_KEY`.
    Sin codificación automática, con coincidencia completa *(A-41)*.
- **Sobre:** un CloudEvent normal. `subject` (el atributo) = la clave y `tenantid` = el token de
  tenant del subject de NATS: si discrepan, POISON `STATE_SUBJECT_MISMATCH`.
  `dataclassification` ∈ `public | internal | confidential`.
- **`data` DEBE incluir `aggregateVersion`** (entero ≥ 1 y ≤ 2^53 − 1). El esquema lo declara
  `required`; `bundle-schemas.mjs` y `check-compat.mjs` rechazan un esquema de estado que no lo
  haga. **Hecho.**

#### 2.2 Escribir — solo el dueño

`put` es condicional. Sin esto, dos instancias del dueño pueden dejar un valor viejo como
«actual», y un consumidor nuevo lo recibiría como verdad.

```
0. Validar sin red y construir el evento UNA vez (mismo id y bytes en todos los intentos).
1. Leer el último mensaje de la clave (direct get) → secuencia S, versión V. Sin valor: S = 0.
1b. Si el id leído es el de ESTE put → ya se aplicó (acuse perdido): resultado «ok».
2. Si nueva.aggregateVersion <= V → no escribir. Resultado `stale`. No es un error.
3. Publicar por JetStream con  Nats-Expected-Last-Subject-Sequence: S   y   Nats-Msg-Id: <id>.
4. Error 10071 o 10164 («wrong last sequence») → volver a 1. Máximo 3 intentos: STATE_CONFLICT.
```

*(A-32, A-33, A-49)*. El SDK **PUEDE** recordar en memoria la última secuencia de cada clave que él
mismo escribió, para ahorrarse el paso 1. Un `publish` de **core** a un subject de estado
sobrescribe **sin chequeo**: siempre por JetStream *(A-45)*. Resultado:
`{ applied, outcome: "ok" | "stale", revision }`. Detalle y porqués: `10-state.md` §3.

#### 2.3 Borrar

`delete` publica una **lápida** en el mismo subject con **el mismo algoritmo**: un CloudEvent con
`type: com.flux.state.<dom>.<agr>.deleted.v<M>`, `data: { "aggregateVersion": n }`,
`dataclassification: internal` y `dataschema` fijo
`https://schemas.internal/system/state/eliminado/1.0.0.json`. Al entrar, el valor anterior
sale del stream *(A-34)*.

- La lápida lleva versión porque `put` la lee para decidir si una escritura es `stale`.
- **Una clave borrada se recrea con una versión estrictamente mayor que la de la lápida** *(A-34)*.
  La trampa: un dueño que borra su fila y la recrea con `aggregateVersion: 1` recibe `stale` y la
  clave sigue borrada, sin error.
- Borrar una clave que nunca existió escribe la lápida. `get` de una lápida devuelve `null`.
- La lápida es permanente en 1.1. Compactarlas queda fuera — §6.
- Un consumidor **DEBE** borrar o anonimizar su copia local al recibir una lápida. Es lo
  que hace real el derecho de supresión aguas abajo.

#### 2.4 Leer

- **`watch`** — foto completa y después los cambios, por el mismo canal:
  - durable `<servicio>__state_<dom>_<agr>_v<M>`, con sufijo `__<tenant>` si filtra (los guiones del
    tenant pasan a `_`): **un durable compartido reparte los mensajes** entre quienes lo usan
    *(A-47)*;
  - `deliver_policy: last_per_subject`; el resto, la config canónica de `03-delivery.md` §2;
  - filtro `state.<dom>.<agr>.v<M>.<tenant>.*` (o `*.*` sin tenant; `tenantIsolation: "strict"`
    lo prohíbe). Con tenant en `connect()`, **siempre** filtra, también si es `system`.

  *(A-35)*. Un consumidor que estuvo caído recibe **solo el último valor** de cada clave que
  cambió, no los intermedios; un valor reemplazado mientras estaba en vuelo **no se reentrega**
  *(A-36)*. La recuperación es borrar y recrear el durable *(A-37)*.
- **`get`** — una clave, pedida al broker por subject:
  `$JS.API.DIRECT.GET.STATE_<DOM>.<subject>`. Clave inexistente → `404`; stream inexistente o sin
  `allow_direct` → **sin respuesta** (se ven igual). *(A-32)*
- La entrada que recibe el handler: `{ key, tenantId, deleted, data, aggregateVersion, revision }`,
  donde `revision` es la secuencia del stream y `data` es `null` si `deleted`.
- `onSynced`: se invoca **una vez por `watch`**. Si al abrir el consumidor no tiene nada pendiente
  (`num_pending` y `num_ack_pending` a 0: el stream está vacío), **de inmediato**, antes de
  entregar nada. Si no, cuando llega una entrega con `pending == 0` en sus metadatos **y** todos los
  handlers lanzados hasta ahí han terminado. Las escrituras durante la sincronización llegan en la
  misma foto (lo reescrito, una vez y con su valor más reciente) y retrasan `onSynced`.
  *(A-35)*

#### 2.5 Errores y DLQ

- Misma taxonomía que los eventos (`04-errors.md`), con los **seis códigos de estado** de §6 de ese
  documento: `INVALID_TENANT_FOR_STATE`, `INVALID_STATE_KEY`, `MISSING_AGGREGATE_VERSION`,
  `RESERVED_DOMAIN` (PERMANENT), `STATE_SUBJECT_MISMATCH` (POISON) y `STATE_CONFLICT` (RETRYABLE).
- Subject de DLQ: `dlq.state.<dom>.<agr>.v<M>` — sin tenant ni clave, para que no se multiplique. El
  stream `DLQ_<DOM>` de un dominio **con estado** captura `dlq.<dom>.>` **y** `dlq.state.<dom>.>`;
  uno sin estado no cambia. El dueño lee la DLQ de estado con `<servicio>__dlq_state_<dom>`.
- **El evento de DLQ de estado no lleva el dato**: `data` se sustituye por
  `{ "key": "…", "revision": n }`. Si no, un dato personal borrado seguiría 90 días en la DLQ. Por lo
  mismo: `dlqerror` no cita valores, un mensaje ilegible no se copia, y la firma original no se
  conserva (firma un `data` que ya no está).
- **El estado no se reproduce desde la DLQ.** Republicar un valor viejo pisaría uno nuevo.
  `flux dlq replay` **DEBE** rechazar `dlq.state.>`. La recuperación es `flux state resync`.

#### 2.6 API de referencia (Node)

```ts
await bus.state.put("clientes.cliente.v1", "cli-1",
  { clienteId: "cli-1", nombre: "Ana", email: "ana@example.com", aggregateVersion: 7 },
  { classification: "confidential" });        // → { applied: true, outcome: "ok", revision }

await bus.state.delete("clientes.cliente.v1", "cli-1", { aggregateVersion: 8 });

const actual = await bus.state.get("clientes.cliente.v1", "cli-1");   // entrada | null

await bus.state.watch("clientes.cliente.v1", async (entrada, ctx) => {
  if (entrada.deleted) return borrarLocal(entrada.tenantId, entrada.key);
  await guardarLocal(entrada);        // UPDATE … WHERE aggregate_version < $n
}, { onSynced: () => listo() });
```

`publish()` **DEBE** rechazar los dominios reservados `dlq`, `state`, `rpc` y `system` con
`RESERVED_DOMAIN`. Sin `classification`, `put` usa `confidential`; `restricted` se rechaza.

#### 2.7 Entregables

| Dónde | Qué | Estado |
|---|---|---|
| `specification/10-state.md` | Todo lo anterior, con el porqué de cada regla | **Hecho** |
| `specification/06-security.md` §5.1 | Reescribir según §4.4 de este plan | **Hecho** (§2, §3, §4 y §7: de la fase 1) |
| `specification/02-naming.md` | Nombres reservados; stream `STATE_`; durable de estado | **Hecho** |
| `specification/04-errors.md` §6 | Los seis códigos de estado | **Hecho** |
| `specification/08-observability.md` | `flux_state_writes_total{contract,outcome}` con `outcome = ok \| stale \| conflict \| invalid_schema`. Las métricas de consumo existentes se reutilizan con `subject="state.<contrato>"` y el `consumer` **sin el sufijo de tenant**. **Nunca** la clave ni el tenant como etiqueta | **Hecho** |
| `protocol.json` | `version: "1.1.0"`; bloque `state`; `errors.codes`; definición de la capacidad `state` (sin ningún SDK que la declare); `security.piiInPayload` por primitiva | **Hecho** |
| `scripts/check-protocol-json.mjs` | Casos que deben pasar y fallar para cada patrón nuevo, invariantes de estado y 56 mutaciones | **Hecho** |
| `scripts/bundle-schemas.mjs`, `check-compat.mjs` | Recorrer `schemas/state/`; índice `state` en el bundle; error con pista si falta `aggregateVersion` | **Hecho** (reglas en `scripts/schema-rules.mjs`) |
| `schemas/state/clientes/cliente/1.0.0.json`, `schemas/system/state/eliminado/1.0.0.json` | Esquema de ejemplo con `x-pii`, y la lápida | **Hecho** |
| `conformance/cases/` | `state-broker.json` (A-31 … A-49), `state-semantics.json` (A-50) y `cross-sdk-state-envelope.json`; los ejecuta `conformance/state-broker.mjs` | **Hecho** |
| `scripts/gen-acl.mjs` | Clave `watches`. Dueño: publica en `state.<dom>.>` y `DIRECT.GET` de su dominio. Observador: `CONSUMER.INFO`, `MSG.NEXT` y `ACK` de su durable, `DIRECT.GET` acotado a su tenant, `dlq.state.<contrato>` y `STREAM.INFO.DLQ_<DOM>`; **sin** `STREAM.INFO.STATE_<DOM>`. Patrones en `protocol.json` → `state.acl` | **Hecho** (`fase-2-acl`): `watches` es `"clientes.cliente.v1"` (todos los tenants) o `{ contract, tenants }` (confinado; un durable y un `DIRECT.GET` por tenant). **Difiere de `state.acl` en un punto**: el dueño tampoco recibe `STREAM.INFO.STATE_<DOM>` (el SDK no lo usa). Rechaza dominios reservados, `tenants: []`, claves desconocidas y observar el propio dominio. `06-security.md` §3.8 |
| `cli/` | `flux state ls \| get \| resync`; `provision` crea `STATE_` y amplía `DLQ_<DOM>`; `doctor` audita su config; `dlq replay` rechaza `dlq.state.>`; `tail` entiende `state.` | `provision`: **hecho** (`fase-2-acl`: `STATE_<DOM>`, durables `last_per_subject`, la DLQ de estado y la ampliación de `DLQ_<DOM>`, que es lo único que cambia en un recurso existente). Lo demás, pendiente (agente de CLI) |
| `sdk-node/`, `sdk-rust/` | La API de 2.6, con tests contra NATS real y los escenarios de `state-semantics.json` | Pendiente |
| `conformance/harness/` | Operaciones `state-build`, `state-tombstone`, `state-subject`, `state-dlq`, con los vectores de `cross-sdk-state-envelope.json` | Pendiente (integración) |
| `conformance/security.mjs` | Otro servicio no escribe estado ajeno; un tenant no lee el de otro. Los casos de datos de `state-broker.json` (`acls.estado`) ya lo miden contra una ACL a mano | **Hecho** (`fase-2-acl`): S15 (ciclo de extremo a extremo), S16 (solo el dueño escribe), S17 (un tenant no lee el de otro), S18 (el stream de estado), S19 (la DLQ de estado) y S20 (minimalidad); más la sección de estado de `verify-acl.mjs` |

**Aceptación:** la definición de hecho, más un test de extremo a extremo: el dueño escribe
100 claves, un observador nuevo las recibe todas y recibe `onSynced`, se borra una, el observador
recibe la lápida, y `get` de esa clave devuelve `null`.

---

### Fase 3 — Petición/respuesta

**Objetivo:** órdenes y cálculos con respuesta inmediata, sin HTTP. Filas 5 y 6.

Documento nuevo: [`specification/11-request-reply.md`](../../specification/11-request-reply.md), **normativo**. Lo que
sigue es el plan tal como quedó **tras medir y escribir la spec**: la fase se hizo, como la 2, en un frente de spec,
herramientas de esquemas y casos medidos, y los de SDKs (Node y Rust), permisos y CLI implementan sobre ese contrato. Donde
esto y la spec discrepen, manda la spec.

> **Corregido tras medir.** Lo que la ejecución contra nats-server 2.14.5 (Apéndice A, A-56 … A-76) cambió respecto al
> plan original, y por qué:
>
> - **El dueño se suscribe al subject EXACTO de cada operación, no a `rpc.<dominio>.>`.** El plan decía lo segundo. El
>   *permiso* sigue siendo `rpc.<dominio>.>`, pero con la suscripción ancha una operación que el servicio no atiende
>   recibe interés y quien llama espera un timeout en vez de `no responders` en 1 ms. *(A-64)*
> - **El `no responders` no llega solo.** Exige `headers` y `no_responders` en CONNECT; sin ellos, silencio. Un SDK nuevo cuya
>   librería no lo active convierte `RPC_UNAVAILABLE` en un timeout. *(A-57)*
> - **La trampa A-18 es una carrera, no un fallo constante.** El acuse del stream gana casi siempre (394 de 400), no
>   siempre: el síntoma es intermitente. Y los filtros más anchos (`>`, `*.>`, `*.*.*.*.*`) solo se pueden crear sin
>   acuse: no contestan, pero convierten `no responders` en un timeout y persisten la petición. `flux doctor` compara
>   contra `rpc.*.*.*.*`, la forma de toda operación. *(A-67, A-69)*
> - **`allow_responses: true` admite UNA respuesta y NO caduca.** El plan lo daba por suficiente; se recomienda
>   `{ max: 1, expires: 30s }`. *(A-68)*
> - **Una petición denegada falla rápido con `@nats-io/*` y NO con `async-nats`** (timeout; el `-ERR` solo se ve en el
>   evento de la conexión): el SDK de Rust tiene que competirlo con la respuesta. *(A-66, A-72)*
> - **Una respuesta que no cabe falla en silencio**: la librería lanza en quien responde y quien llama ve un timeout. El que
>   responde comprueba el tamaño y contesta con un error. *(A-65)*
> - **`RPC_TIMEOUT` es ambiguo por cuatro vías**: responder lento (A-61), responder que muere (A-62), instancia colgada
>   (A-63) y la ventana de un cluster al irse el último (A-70). Por eso el que responde deduplica por `id`.
> - **Los códigos son seis, no cuatro**: a los del plan (`RPC_UNAVAILABLE`, `RPC_TIMEOUT`, `STALE_REQUEST`, y `INVALID_SCHEMA`,
>   que ya existía) se añaden `RPC_INVALID_REPLY`, `RPC_MESSAGE_TOO_LARGE` e `INVALID_RPC_CALL`.
> - **El error binario**: `class` es `retryable` o `permanent`; `poison` no viaja. Un error desconocido del handler se responde
>   como `permanent`.
> - **La compatibilidad se invierte para la petición**: añadir un valor a un enum de una petición no exige
>   `x-extensible-enum` (el lector es el dueño); el orden de despliegue es primero quien responde.

#### 3.1 Modelo

- **Subject** `rpc.<dominio>.<agregado>.v<major>.<operacion>`. La operación va en
  **infinitivo**, `kebab-case`: `obtener`, `crear`, `calcular-precio`.
- **Core NATS**, no JetStream. Una petición no se persiste: si nadie responde, quien
  llama lo sabe al instante *(A-15)*.
- **Ningún stream DEBE capturar `rpc.>`.** Si uno lo hace, quien llama recibe el acuse de
  JetStream como si fuera la respuesta *(A-18)*. `flux doctor` lo comprueba.
- **Solo el dueño responde**: su permiso es `rpc.<dominio>.>` pero **se suscribe al subject exacto de cada operación** que
  atiende, con *queue group* = su nombre de servicio. Así varias instancias se reparten las peticiones *(A-58, A-64)*.
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
| No llega respuesta a tiempo | `RPC_TIMEOUT` | RETRYABLE — **ambiguo** |
| La petición no cumple su esquema | `INVALID_SCHEMA` | PERMANENT |
| Petición firmada fuera de la ventana | `STALE_REQUEST` | PERMANENT |
| Lo que llega no es la respuesta a esta petición (p. ej. el acuse de un stream) | `RPC_INVALID_REPLY` | PERMANENT |
| El mensaje no cabe en `max_payload` | `RPC_MESSAGE_TOO_LARGE` | PERMANENT |
| Operación mal formada, `timeoutMs` fuera de rango, `data` que no es un objeto | `INVALID_RPC_CALL` | PERMANENT |

La lista es **cerrada** y está en [04-errors.md §7](../../specification/04-errors.md).

Timeout por defecto **5 s**, máximo **25 s**: por debajo del `ack_wait`, para que una
llamada hecha desde un handler no provoque su reentrega.

#### 3.3 Reglas

- **Idempotencia.** Una orden puede llegar dos veces. El handler **DEBE** deduplicar por
  el `id` de la petición. Si el SDK reintenta, **reutiliza el mismo `id`**.
- **Tamaño.** 1 MiB en cada sentido (o el `max_payload` del broker, si es menor). Más: claim-check, o paginar con un cursor
  en `data`. Una respuesta que no cabe se contesta con `RPC_MESSAGE_TOO_LARGE`: de otro modo quien llama solo vería un timeout *(A-65)*.
- **No es para leer datos guardados.** Eso es estado — §2.

#### 3.4 Seguridad

- Quien llama: publicación sobre el subject **exacto** de cada operación concedida. Clave
  `calls` en el manifiesto *(A-17)*.
- El dueño: suscripción a `rpc.<dominio>.>` y `allow_responses`. No necesita permiso de publicación sobre ningún inbox *(A-16)*.
  Medido: `true` admite **una** respuesta y **no caduca**; se recomienda `{ max: 1, expires: 30s }` *(A-68)*. Un dueño sin permiso de
  suscripción parece un servicio caído (`no responders`): `respond()` falla al arrancar con `PERMISSION_DENIED` *(A-73)*.
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

| Dónde | Qué | Estado |
|---|---|---|
| `specification/11-request-reply.md` | Todo lo anterior, con el porqué y lo medido | **Hecho** |
| `specification/00-protocol.md` §5, §6 | La capacidad `rpc`; quitar «request/reply» de fuera de alcance | **Hecho** |
| `specification/02-naming.md`, `04-errors.md` §7, `05-compatibility.md` §6 | El subject y que no hay stream; los seis códigos; la dirección invertida | **Hecho** |
| `specification/08-observability.md` §2.5 | `flux_rpc_client_requests_total{operation,outcome}`, `flux_rpc_server_requests_total{operation,outcome}`, `flux_rpc_duration_seconds{operation,role}`, en su bloque (las siete siguen siendo siete) | **Hecho** |
| `protocol.json`, `check-protocol-json.mjs` | Bloque `rpc`; `errors.codes`; la capacidad `rpc` (sin ningún SDK que la declare); quitar de `outOfScopeV1`; 54 patrones y 78 mutaciones | **Hecho** |
| `scripts/bundle-schemas.mjs`, `check-compat.mjs`, `schema-rules.mjs` | `schemas/rpc/`; índice `rpc`; pares; la dirección invertida (`scripts/test-schema-tools.mjs` las prueba en negativo) | **Hecho** |
| `schemas/rpc/clientes/cliente/{obtener,calcular-descuento}/…`, `schemas/system/rpc/error/1.0.0.json` | Esquemas de ejemplo, con `x-pii` donde toca, y el error | **Hecho** |
| `conformance/cases/rpc-semantics.json`, `cross-sdk-rpc-envelope.json`; `conformance/rpc-broker.mjs` | A-56 … A-76 como casos con `observed`, cuatro mutantes, y los ejemplos byte a byte | **Hecho** |
| `scripts/gen-acl.mjs` | Claves de llamada y de servicio; `allow_responses` para el dueño | Pendiente (agente de permisos) |
| `cli/` | `doctor`: ningún stream captura `rpc.*.*.*.*` (algoritmo en `protocol.json` → `rpc.doctor`). `flux call <operacion> --data '{…}'` para depurar | Pendiente (agente de CLI) |
| `sdk-node/`, `sdk-rust/` | `request` y `respond` | Pendiente |
| `conformance/harness/` | Operaciones `rpc-request`, `rpc-reply`, `rpc-error`, `rpc-operation`, `rpc-timeout`, con los vectores de `cross-sdk-rpc-envelope.json` | Pendiente (integración) |
| `conformance/security.mjs` | Llamada no concedida denegada; respuesta ajena no legible | Pendiente (agente de permisos). Medido a mano contra una ACL escrita a mano en `rpc-semantics.json` (`acls.rpc`) |

**Aceptación:** la definición de hecho, más: una operación atendida por dos instancias
reparte las peticiones; al parar ambas, quien llama recibe `RPC_UNAVAILABLE` en menos de
100 ms.

`docs/plan-v1.1/evidencia/` **ya no existe**: lo que quedaba (A-15 … A-18) es ahora `rpc-semantics.json`.

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

**Seguridad (Fase 1) en Go, Java, .NET, PHP y Python — dos cosas más que el inbox privado, TLS
y credenciales.** Node y Rust ya las cumplen; los otros cinco las tienen **sin hacer ni medir**:

- **(a) Medir si su primera conexión se cuelga.** El agente de la spec midió que
  `waitOnFirstConnect` (Node, A-24) deja `connect()` esperando para siempre ante un certificado
  que no verifica, y que `retry_on_initial_connect` (`async-nats`, A-30) deja el servicio
  arrancado y mudo. **No midió los demás.** Go usa `RetryOnFailedConnect` y Java
  `connectReconnectOnConnect`: hay que ejecutarlos contra un broker real con una credencial
  rechazada, una CA que no firmó el certificado, un host fuera de los SAN y TLS exigido sin TLS
  en el broker, y apuntar qué hace cada uno (falla, se cuelga o se queda mudo). Si no falla
  rápido, el SDK escribe su propio bucle de espera (`03-delivery.md` §6.1). Lo medido va a
  `conformance/cases/` y al Apéndice A.
- **(b) Emitir los tres códigos y hacer fallar `subscribe()` ante un pull denegado.**
  `CONNECTION_REJECTED` (motivo `auth` o `tls`), `INVALID_CREDENTIALS` e `INVALID_TLS_OPTIONS`
  (`04-errors.md` §5.3), todos PERMANENT; y `subscribe()` con `PERMISSION_DENIED` si se deniega
  `$JS.API.CONSUMER.MSG.NEXT.…` (`06-security.md` §3.7: **DEBE**, no DEBERÍA). El pull denegado
  no da error y deja un consumidor inerte que parece sano; cómo detectarlo depende de si la
  librería permite un PING/PONG (Node) o no (Rust: una petición propia al mismo subject). Cada
  código se prueba **en los dos sentidos**: en su situación y en ninguna otra
  (`sdk-rust/tests/security.rs`, `h_*` y `g_*`). `node scripts/check-sdk-codes.mjs` exige los
  literales a todo SDK que declare `security`.

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
| Dos instancias del mismo servicio con distinto `tenantId` comparten durable de **eventos** y cada una descarta los del otro | **Confirmado en la Fase 2.** El broker reparte los mensajes de un durable compartido entre quienes lo usan (A-47, caso `shared-durable-splits-messages`); el SDK de Node confirma y descarta lo que no es de su tenant (`sdk-node/src/client.ts`, `#dispatch`), y el durable de eventos no lleva tenant: cada instancia pierde la mitad, en silencio. **El de estado lo evita** con el sufijo `__<tenant>`. El de eventos sigue igual: arreglarlo es cambiar el nombre del durable de eventos (decisión del dueño; ver el informe de la fase). |
| `flux provision` y el auto-aprovisionado del SDK divergen | Una sola fuente de la config canónica: `protocol.json`. |
| El alcance es grande: siete SDKs por tres primitivas | Por eso Node y Rust primero. Las fases 1–5 ya dan un sistema completo; la 6 es paridad. |

---

## Apéndice A — Evidencia

Observado el **2026-11-26** contra **nats-server 2.14.5**, con `@nats-io/jetstream` 3.4.0.
Reproducible con los runners de `conformance/` (cada fila dice cuál).

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
| A-31 | Stream de estado: `max_msgs_per_subject: 1`, `allow_direct`, `max_age: 0`, `discard: old`; 3 escrituras sobre 2 claves. Reproduce A-9 | **2 mensajes.** El servidor devuelve la configuración pedida, sin sobrescribirla (también con 3 réplicas). Sin `max_msgs_per_subject`, las mismas 3 escrituras dejan 3. Caso `one-value-per-key` |
| A-32 | `DIRECT.GET` por subject. Reproduce A-10 | Devuelve el cuerpo y las cabeceras `Nats-Stream`, `Nats-Subject`, `Nats-Sequence`, `Nats-Time-Stamp` **y las que se publicaron** (`Nats-Msg-Id`). Clave inexistente: `404 Message Not Found`. **Stream inexistente, o sin `allow_direct`: nadie contesta (`no responders`), y se ven igual.** Con comodín devuelve el último mensaje escrito que lo cumple. Caso `direct-get-by-subject` |
| A-33 | `Nats-Expected-Last-Subject-Sequence`: crear con `0`, S vieja, mismo `Nats-Msg-Id` reenviado. Reproduce A-11 | `0` crea si el subject no tiene mensaje; si lo tiene, err **10071** «wrong last sequence: N» (N = la real). **El chequeo de secuencia se evalúa ANTES que el de duplicados**: el mismo id con la S vieja da 10071, no `duplicate`. **Un publish rechazado por secuencia no gasta el `Nats-Msg-Id`.** Sin cabecera de secuencia el mismo id sí es `duplicate`. Caso `conditional-write` |
| A-34 | Lápida sobre una clave; recrearla. Reproduce A-13 | El valor anterior **sale del stream** (un mensaje por clave) y `DIRECT.GET` devuelve la lápida. Recrear con `expected=0`: 10071 con la secuencia de la lápida; con la secuencia de la lápida: se aplica. Caso `tombstone-replaces-and-recreates` |
| A-35 | Consumidor `last_per_subject` sobre estado: tamaño, orden, `pending`, stream vacío, escrituras durante la sincronización, igualdad con `all`. Reproduce A-12 | `num_pending` = número de **claves** del filtro, lápidas incluidas. Entrega en orden de secuencia con el `pending` de los metadatos bajando hasta **0** en la última. Stream vacío: `num_pending 0` y no entrega nada. Una clave reescrita durante la sincronización aparece **una vez**, con su secuencia nueva; una nueva, al final. En este stream `deliver_policy: all` entrega lo mismo. Casos `snapshot-*`, `writes-during-the-snapshot` |
| A-36 | Revisión 1 entregada y `nak`; entra la revisión 2 que la sustituye | **La 1 no se reentrega**: el siguiente pull entrega solo la 2; `num_redelivered 0`. Caso `replaced-message-is-not-redelivered` |
| A-37 | Borrar y recrear el durable (`flux state resync`) | Vuelve a entregar el último valor de cada clave. Caso `resync-by-recreating-the-durable` |
| A-38 | ACL por tenant sobre el estado, escrita a mano: dueño y observador de `acme`. Reproduce A-14 | El dueño escribe en todos sus tenants y lee; no escribe en otro dominio. El observador **no escribe**; lee `acme.*`; se le **deniega** `globex`, `v1.*.*`, `state.<dom>.>`, la forma de **cuerpo** de `DIRECT.GET` y `STREAM.MSG.GET`; el comodín en la clave pasa (es su tenant). Su `CONSUMER.INFO`, `MSG.NEXT` y `ACK` funcionan; los del durable de otro servicio, no. **`DIRECT.GET` por subject con un cuerpo `{seq}` da `408 Bad Request`**: no se puede colar un cuerpo por un permiso de subject. Casos `acl-owner-writes-observer-reads-only-its-tenant`, `subject-form-direct-get-rejects-a-body` |
| A-39 | `STREAM.INFO` con `subjects_filter` en el cuerpo, con y sin permiso | **Lista TODOS los subjects del stream con su recuento: todas las claves y todos los tenants.** Sin ese permiso, `put`, `get` y `watch` funcionan igual. Casos `stream-info-lists-keys-and-tenants`, `acl-stream-info-needs-its-own-permission` |
| A-40 | `CONSUMER.CREATE` por subject con un filtro más ancho en el cuerpo: `filter_subject`, `filter_subjects` o ninguno | 10131 «did not match filtered subject» para el primero y para ninguno (no hereda el del subject); **10137** «consumer with multiple subject filters cannot use subject based API» para el array. Caso `acl-consumer-create-filter-cannot-be-widened` |
| A-41 | Publicar a un subject de estado con claves `*`, `a>b`, `a.b`, `café`, 2000 caracteres y `a b` | **El broker acepta todas salvo la que lleva un espacio** (`illegal subject`). `a.b` son dos tokens: queda fuera del filtro `…acme.*`, sin error. Casos `broker-does-not-validate-keys`, `put-validates-before-the-network` |
| A-42 | Longitud de nombres | Consumidor: **255** como máximo (256 → err 10102): caso `name-length-limits`. Medido a mano, sin caso: un stream de 255 caracteres se acepta, y un subject de 3,9 KiB se publica pero uno de 5 KiB no recibe respuesta |
| A-43 | `EVT_STATE` (`state.>`) con `STATE_CLIENTES` existente; `EVT_DLQ` (`dlq.>`) con `DLQ_PEDIDOS` | Err **10065** «subjects overlap with an existing stream» en los dos. `state-clientes` no está reservado y convive. Caso `reserved-domains-collide-in-the-broker` |
| A-44 | Ampliar `DLQ_CLIENTES` con `dlq.state.clientes.>` teniendo mensajes; publicar antes y después; publicar a un subject sin stream | Se amplía sin perder mensajes. Publicar por JetStream a un subject que nadie captura **falla** (a diferencia de core). El `DLQ` de otro dominio no captura `dlq.state.clientes.>`. Caso `dlq-stream-can-be-widened` |
| A-45 | Publicar por **core** a un subject de estado | **Lo captura el stream sin acuse y sin chequeo de secuencia**: sobrescribe. Caso `core-publish-bypasses-the-conditional-write` |
| A-46 | Stream con `discard: new` y `discard_new_per_subject` | La segunda escritura en una clave da err **10077** «maximum messages per subject exceeded»: sería un create-only. Descartado. Caso `discard-new-per-subject-is-create-only` |
| A-47 | Dos clientes pidiendo del MISMO durable | **Reparten los mensajes** (3 y 3, disjuntos). Por eso el durable de estado lleva el tenant, y por eso el riesgo de §7 sobre eventos es real. Caso `shared-durable-splits-messages` |
| A-48 | `CONSUMER.INFO` de un consumidor inexistente y de un stream inexistente | 404 con err **10014** «consumer not found» y **10059** «stream not found»: se distinguen sin `STREAM.INFO`. Caso `consumer-info-distinguishes-missing-stream-and-consumer` |
| A-49 | Cluster de 3 nodos locales: stream R3 con la config de estado; carrera de 8 escritores con la misma S; lectura directa tras un `put`; 3 instancias del dueño | Config devuelta tal cual. **Bajo carrera: 210 de 210 rechazos con err 10164 «wrong last sequence», SIN número**; sin carrera sigue siendo 10071 con número. 400 `put` seguidos de su lectura inmediata: 0 lecturas obsoletas (**no** demuestra que no ocurra con latencia real). 3 instancias sobre la misma clave sin pausa: de 90 `put`, 49 ok, 27 `stale`, 14 `STATE_CONFLICT`. Casos `cluster-r3-cas-conflict-code`, `cluster-three-owner-instances-converge` |
| A-50 | El algoritmo de `put`/`delete`/`get` y la foto de `watch` (10-state.md §3.2, §5.5), como implementación de referencia sobre NATS crudo | Los escenarios de `state-semantics.json`: versiones y `stale`; recrear tras la lápida; clave inexistente; versión 0, decimal o > 2^53 − 1; propia escritura tras un acuse perdido; 3 conflictos → `STATE_CONFLICT`, un rival con versión mayor → `stale`; 3 instancias convergen; mismo `cli-1` en dos tenants; foto con lápida, vacía y con escrituras. Nueve mutantes de la referencia (`--mutant`) los hacen fallar todos |
| A-51 | Dos escrituras a claves **distintas** con el mismo `Nats-Msg-Id` dentro de `duplicate_window`, ambas con `Nats-Expected-Last-Subject-Sequence: 0` (hallado en S15) | **La segunda no se escribe y el acuse no es un error**: `duplicate: true` con la secuencia de la primera. El chequeo de secuencia pasa (la clave es nueva) y el de duplicados la descarta. Un `Nats-Msg-Id` que no sea el `id` único del evento —un fixture que lo reutilice— pierde escrituras en silencio |
| A-52 | Ampliar `DLQ_<DOM>` con `dlq.state.<dom>.>` por `flux provision` teniendo un mensaje; `--dry-run` antes | `se ampliaría`, y después `ampliado`: el mensaje sigue ahí (1), y repetir da «sin cambios». Reproduce A-44 por la CLI |
| A-53 | `streams.update` de `STATE_<DOM>` con `max_age: 60s` y `duplicate_window` de 2 m | Err «duplicates window can not be larger then max age»: un `max_age` corto no es una forma de que el estado caduque con la ventana de la 1.0; el estado usa `max_age: 0` |
| A-54 | La ACL generada ejecutada de extremo a extremo (S15 a S20): el dueño, un observador confinado a `acme` y uno sin confinar | Sin una sola violación en los ciclos positivos. Con `DIRECT.GET…v1.acme.*` se lee una clave de `acme` y un `…acme.>` (sigue siendo su tenant); se deniegan `globex`, `…v1.*.<clave>`, `state.<dom>.>` y la forma de cuerpo. Un `publish` de core a un subject de estado se deniega a todos menos al dueño. `STREAM.INFO.STATE_<DOM>`, `STREAM.MSG.GET`, `STREAM.PURGE` y el durable de otro: denegados a los cuatro roles. Quitar cualquiera de los permisos de estado rompe el ciclo (`verify-acl.mjs`, 5b) |
| A-55 | Qué pide `@nats-io/jetstream` 3.4.0 bajo los permisos de estado generados (observador confinado y dueño) | `DirectStreamAPIImpl.getMessage(stream, { last_by_subj })` emite exactamente `$JS.API.DIRECT.GET.<stream>.<subject>`, sin cuerpo y **sin ninguna violación**; **no es público** (`jetstream(nc).direct` es `undefined`), así que un SDK pide `nc.request` a ese subject y mira `headers.code` (404 = no hay valor; las cabeceras traen `Nats-Sequence`). `jsm.streams.getMessage` usa `STREAM.MSG.GET` y `jsm.streams.info("STATE_…")` usa `STREAM.INFO`: **denegados**, también al dueño. `consumers.get` (CONSUMER.INFO) y `publish` con `expect: { lastSubjectSequence }` funcionan; el conflicto llega como `JetStreamApiError` con `code` 10071 |
| A-56 | Petición a una operación sin nadie escuchando, con un timeout de 5000 ms. Reproduce A-15 | `no responders` en **1-2 ms** (`RequestError: no responders: '<subject>'`). Caso `no-responders-is-instant` |
| A-57 | La misma petición por un cliente en bruto, con y sin `no_responders` en CONNECT | Con `"headers":true,"no_responders":true`: un mensaje HMSG de estado `NATS/1.0 503` sin cuerpo en ~0,6 ms. **Sin la bandera: silencio** (en 500 ms no llega nada): quien llama espera su timeout. Caso `no-responders-needs-the-connect-flags` |
| A-58 | Dos instancias en el mismo grupo de cola; 600 y 2000 peticiones seguidas | Todas respondidas, **cada una por UNA instancia**, repartidas **al azar**, no por turnos: 295 / 305 de 600; 979 / 1021 de 2000. Sin grupo de cola cada instancia las recibe todas (mutante `no-queue-group`). Caso `queue-group-splits-requests` |
| A-59 | Una instancia deja de escuchar; después, la última | Las 100 siguientes van a la que queda (100 de 100); tras irse la última, `no responders` en ~1 ms. Caso `queue-group-instance-leaving` |
| A-60 | Dos SERVICIOS (dos grupos de cola) sobre la misma operación | **Cada grupo recibe y ejecuta la petición.** Quien llama ve UNA respuesta, la del que conteste antes (`svc-a` o `svc-b` según la ejecución). Caso `two-services-on-one-operation-both-execute` |
| A-61 | Un responder de 600 ms con un timeout de 300 ms; después, una petición en bruto sin timeout | `TimeoutError` a los ~300 ms, **pero el responder recibió y respondió una vez**; la respuesta tardía **llega al inbox** a los ~600 ms (el broker no la descarta) y la conexión de quien llamó sigue sana. Caso `slow-responder-times-out-but-executes-and-the-late-reply-arrives` |
| A-62 | La única instancia recibe la petición y cierra su conexión sin responder; y parar con un handler de 400 ms: cerrar la conexión a los 100 ms frente a `unsubscribe` a los 100 ms | `TimeoutError` a los ~1000 ms; la siguiente petición, `no responders`. **Cerrar de golpe pierde la petición en vuelo** (timeout); con `unsubscribe` la petición nueva falla en ~1 ms con `no responders` y la que estaba en vuelo termina con su respuesta. Casos `responder-dies-mid-request-looks-like-a-timeout`, `stopping-gracefully-does-not-lose-what-is-in-flight` |
| A-63 | Una instancia viva y un cliente en bruto suscrito al mismo grupo que **no responde**; 40 peticiones con timeout de 150 ms | Entre 19 y 22 respondidas y entre 18 y 21 con timeout (se lleva su parte, 1/N, al azar); al destruir su socket, 20 de 20. Caso `a-hung-instance-takes-its-share-of-the-requests` |
| A-64 | Suscripción al subject exacto de la operación frente a `rpc.<dominio>.>`, con una petición a una operación sin handler | Exacta: `no responders` en ~1 ms. Con el comodín el servicio **tiene interés** y quien llama espera su timeout. Caso `exact-subscription-gives-no-responders-for-an-unknown-operation` |
| A-65 | `max_payload` y qué pasa al superarlo: la librería de Node, un cliente en bruto y una respuesta que no cabe | `max_payload` anunciado: **1048576**. EXACTAMENTE ese tamaño se entrega y se responde; con 1048577 la librería lanza **al instante** (`InvalidArgumentError: 'payload' max_payload size exceeded`; `async-nats`, `MaxPayloadExceeded`) sin tocar la red; **las cabeceras cuentan** (1048576 de cuerpo y una cabecera ya lo supera). Un PUB en bruto de 1048577: `-ERR 'Maximum Payload Violation'` y **cierre de la conexión**. Una respuesta de 1048577: quien responde recibe el error de la librería y **quien llama, un timeout**. Caso `max-payload-and-oversize` |
| A-66 | Reproduce A-17: llamar a una operación no concedida con la librería de Node; y a una concedida sin nadie | `RequestError` con causa `PermissionViolationError` en **~1 ms**: `Permissions Violation for Publish to "rpc.clientes.cliente.v1.obtener"`. La concedida sin nadie: `no responders` en ~1 ms. Caso `denied-call-fails-fast-in-node` |
| A-67 | Reproduce A-18: un stream sobre `rpc.x.>` con un responder real; sin nadie; y un stream `no_ack` | De 20 peticiones, **entre 18 y 20 «respuestas» son el acuse del stream** `{"stream":"TRAMPA","seq":n}` (394 de 400 seguidas, 341 de 400 concurrentes: **una carrera**, gana casi siempre el acuse); el responder ejecuta todas y el stream **guarda las 20**. Sin nadie escuchando, la petición también se «responde» con el acuse. Con `no_ack`: sin acuse y **sin `no responders`**: timeout. Casos `stream-on-rpc-answers-with-its-ack`, `a-stream-without-acks-turns-no-responders-into-a-timeout` |
| A-68 | `allow_responses` en el dueño, sin permiso de publicación sobre inboxes. Reproduce A-16 | `true`: de **tres** respuestas se entrega **una** y las otras dos son `Permissions Violation for Publish to "_INBOX.llama.…"`; una respuesta a los **2 s** se entrega (**no caduca**). `{ max: 3 }`: de 4, 3. `{ max: 1, expires: "1s" }`: a los 1,5 s se **deniega**, a los 0,3 s pasa. Publicar a un inbox que no es el `reply` de la petición: **denegado**. Sin `allow_responses`: el dueño no puede contestar y quien llama ve un timeout. Casos `allow-responses-*`, `without-allow-responses-the-owner-cannot-reply`, `the-owner-can-only-reply-to-the-requests-reply-subject` |
| A-69 | 16 subjects de stream como filtro (`rpc.>`, `>`, `*.>`, `*.*.*.*.*`, `rpc.otro.>`…): ¿capturan una operación? ¿Se pueden crear con acuse? | El broker captura la operación **exactamente donde predice el algoritmo de solape** (token a token, contra `rpc.*.*.*.*`). **`>`, `*.>`, `*.*.*.*.*`, `*.*.*.*` y `*.*.*.*.*.*` solo se pueden crear SIN acuse** (`capturing all subjects requires no-ack to be true` / `subjects that overlap with jetstream api require no-ack to be true`); los tres primeros capturan (timeout), los dos últimos no. Caso `which-stream-subjects-capture-rpc` |
| A-70 | Cluster de 3 nodos: el responder (nodo 2) cierra y la petición del nodo 0 sale a 0, 5, 20 y 100 ms | **Ventana**: sin esperar, **4 de 10** peticiones agotan su timeout (el resto, `no responders`); a los 5 ms, 0 de 10; a 20 y 100 ms, 0 de 10. En un nodo suelto, 1 de 10 sin esperar. Pasados ~150 ms, `no responders` en ~1 ms. Caso `cluster-window-after-the-last-responder-leaves` |
| A-71 | Un servicio con `_INBOX.>` (la ACL v1) y otro con inbox privado, mientras `llama` recibe una respuesta de `crm` | El primero **oye la respuesta completa** dirigida a otro; el segundo ve **denegada** la suscripción a `_INBOX.llama.>` y su iterador lanza `PermissionViolationError`. Caso `shared-inbox-leaks-replies-and-a-private-inbox-does-not` |
| A-72 | `async-nats` 0.50 (el SDK de Rust): la petición DENEGADA de A-66 | **No falla rápido**: `TimedOut` a los ~2500 ms (el timeout); mientras, el evento `ServerError` de la conexión recibe `Permissions Violation for Publish to "rpc.clientes.cliente.v1.obtener"`. Caso `denied-call-times-out-in-async-nats` (sonda `conformance/probes/rpc-async-nats`) |
| A-73 | Un dueño con permiso de publicar pero **no de suscribirse** a `rpc.clientes.>`, y `llama` | El servidor registra `Permissions Violation for Subscription to "rpc.clientes.cliente.v1.obtener" using queue "crm"` en la conexión del dueño (la suscripción **no lanza al crearse**; el iterador sí) y `llama` recibe `no responders` en ~1 ms: **parece un servicio caído**. Caso `owner-without-subscribe-permission-looks-like-no-responders` |
| A-74 | Una petición con las cabeceras `traceparent` y `Nats-Msg-Id`; la misma `Nats-Msg-Id` otra vez; una publicación sin `reply` | **Las cabeceras viajan** en los dos sentidos (el responder ve las de la petición y quien llama recibe las que el otro puso). Dos peticiones con el mismo `Nats-Msg-Id`: **se entregan las dos** (core no deduplica). Una publicación sin subject de respuesta llega, con `reply` ausente. Caso `headers-travel-and-core-does-not-deduplicate` |
| A-75 | Cluster de 3 nodos: una instancia del mismo grupo en cada nodo; quien llama en el nodo 0 | **Reparte entre los tres** (180 / 221 / 199 de 600; 286 / 312 / 302 de 900), no se queda en el nodo local. Un responder solo en otro nodo: p50 0,55 ms, p99 1,14 ms (exploración; sin caso). Caso `cluster-queue-group-spreads-across-nodes` |
| A-76 | `async-nats` 0.50 frente a las demás situaciones de §4.3 | Sin nadie: `NoResponders` en 0-1 ms. Responder de 600 ms con timeout de 300: `TimedOut` a los ~301 ms. Petición de max_payload + 1: `MaxPayloadExceeded` al instante; justo max_payload se publica. Una respuesta de max_payload + 1: quien responde recibe `MaxPayloadExceeded` y quien llama, `TimedOut`. Atendida: ~0,5 ms. Caso `async-nats-no-responders-timeout-and-oversize` |
A-19 a A-27 los reproduce `sdk-node/test/integration/` (`acl.test.ts`, `auth.test.ts`,
`tls.test.ts`), contra nats-server 2.14.5 y `@nats-io/*` 3.4.0. A-28 a A-30 los reproduce
`sdk-rust/tests/security.rs`, contra nats-server 2.14.5 y `async-nats` 0.50.
A-31 a A-50 los ejecuta [`conformance/state-broker.mjs`](../../conformance/state-broker.mjs) contra
nats-server 2.14.5 y `@nats-io/*` 3.4.0 (`NATS_SERVER=… node conformance/state-broker.mjs`: 35 casos más 5 comprobaciones de los
ejemplos byte a byte; un nodo, una ACL escrita a mano y un cluster de tres nodos).
A-56 a A-76 los ejecuta [`conformance/rpc-broker.mjs`](../../conformance/rpc-broker.mjs) contra nats-server 2.14.5 y
`@nats-io/*` 3.4.0, y contra `async-nats` 0.50 por una sonda de Rust (`NATS_SERVER=… node conformance/rpc-broker.mjs`: 27 casos más 7
comprobaciones de los ejemplos byte a byte y de los vectores; un nodo, una ACL escrita a mano y un cluster de tres nodos; con cuatro
mutantes, `--mutant`, que deben hacer fallar el runner).

**Qué caso cubre cada una.** Las de la **petición/respuesta** (A-15 … A-18 y A-56 … A-76) son casos de
[`conformance/cases/rpc-semantics.json`](../../conformance/cases/rpc-semantics.json): cada caso lleva su lista `covers` (A-15 → A-56, A-16 → A-68,
A-17 → A-66, A-18 → A-67). **Sin caso**, medido a mano en una exploración: la latencia p50/p99 de un responder en otro nodo (A-75), el recuento
de la carrera del acuse con 400 peticiones (A-67) y que `unsubscribe` y `drain` no pierden nada con 400 peticiones seguidas contra un nodo local
(no concluyente: no se pudo reproducir la ventana de red).
Las observaciones **del broker** ya son casos de
[`conformance/cases/security-acl.json`](../../conformance/cases/security-acl.json) (el mapa
completo es su bloque `brokerObservations`): A-1 → S9 · A-2 → S6, S9 · A-3 → S1 · A-4 → S2 ·
A-5 → S3 · A-6 → S4 · A-7 → S6 · A-8 → S5 · A-8a → S1, S2 · A-8c → S10 · A-8f → S6, S13 ·
A-19, A-20 → S12 · A-21 → S13 · A-22 → S6 · A-24 → S8 (la parte del broker) · A-25 → S14 ·
A-27 → S10, S11 · A-29 → S11 · A-14 → S15, S17 · A-32, A-33 → S15 · A-38 → S15, S17, S18 · A-39 → S18 ·
A-40 → S17 · A-44 → S19 · A-45 → S16.
Las del **estado** (A-9 … A-14 y A-31 … A-50; las que dependen de la ACL —A-14, A-32, A-33, A-38 a A-40, A-44, A-45— se repiten bajo los permisos generados en S15 a S19) son casos de
[`conformance/cases/state-broker.json`](../../conformance/cases/state-broker.json) y
[`state-semantics.json`](../../conformance/cases/state-semantics.json): cada caso lleva su lista `covers`.
**No tienen caso** porque son del SDK o de la librería (A-8b, A-26, A-28, A-30; los cubren
`verify-acl.mjs` y los tests de los SDKs) o
necesitan otra forma de arrancar el servidor (A-8d, la misma nkey en dos accounts: no arranca;
A-23, el modo operador/JWT; A-8e y A-24, la parte de `connect()` que se cuelga: son de la
librería y de la CLI).
