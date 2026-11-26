# 12 — El borde: gateway HTTP

> **Estado:** extensión **opcional** de v1.1. **No es una capacidad de SDK** ([00-protocol.md §5](00-protocol.md)): ningún SDK
> la implementa y ninguno la declara. Un servicio que no la use sigue siendo conforme.
>
> **Lo normativo de este documento es poco, y está marcado.** El gateway del repositorio (`gateway/`) es **una
> implementación**, no el protocolo: dos equipos pueden escribir gateways distintos —en otro lenguaje, con otro servidor HTTP— y
> ser interoperables **si respetan solo dos cosas**:
>
> 1. el **formato de la entrega saliente** (§4): lo que recibe un sistema HTTP cuando flux le avisa de un evento; y
> 2. el **`id` determinista** de un hecho importado ([01-envelope.md §2.6.1](01-envelope.md), §5 de este documento).
>
> Todo lo demás (rutas de entrada, autenticación por token, límites, errores HTTP, configuración) está aquí **para que el
> gateway de referencia sea auditable**, no para que otro lo copie. Cada sección dice cuál de las dos cosas es.
>
> Cada observación citada como `A-n` está medida contra **nats-server 2.14.5** (o contra Node 22 para lo de HTTP) y descrita en
> el [Apéndice A del plan v1.1](../docs/plan-v1.1/PLAN.md#apéndice-a--evidencia). Las que reproducen las pruebas del gateway
> están en `gateway/test/`.

---

## 1. Qué problema resuelve

flux asume que quien habla con el broker **puede mantener una conexión NATS abierta**. No todo lo que forma parte de un
ecosistema puede:

| Fila del mapa ([plan §2](../docs/plan-v1.1/PLAN.md)) | Caso | Sin gateway |
|---|---|---|
| 8 | Un **webhook entrante** de un tercero (la pasarela de pagos avisa de un cobro) | Un endpoint HTTP casero por proveedor, cada uno con su verificación de firma y sus duplicados |
| 9 | Un **webhook saliente** hacia quien no puede conectarse (un cliente, un SaaS) | Un servicio propio que reintenta, firma y clasifica errores a su manera |
| 10 | Un sistema **sin conexión permanente**: hosting compartido, `cron`, *serverless* | Volver a la API HTTP entre servicios que flux quería eliminar |

El **gateway** es la frontera. **HTTP queda solo en el borde; dentro del ecosistema, nada lo usa.** Y no es una API nueva:
cada cosa que se puede hacer por HTTP es una de las tres primitivas de flux —un evento, el estado, una petición— con el
mismo sobre, el mismo `tenantid`, la misma clasificación y los mismos permisos. El gateway traduce; no decide.

## 2. Qué es normativo y qué no

| | Qué | Normativo |
|---|---|:--:|
| §4 | **La entrega saliente**: método, cabeceras, cuerpo, firma `Flux-Signature`, cómo se interpreta la respuesta | **sí** |
| §5 | **El `id` determinista** de un hecho importado (la regla está en [01-envelope.md §2.6.1](01-envelope.md)) | **sí** |
| §3 | El modelo: el gateway **aloja servicios**, no los suplanta | no — implementación |
| §6 | Entrada HTTP → flux: rutas, autenticación, webhooks de terceros, errores HTTP | no — implementación |
| §7 | Seguridad del gateway: límites, token, TLS, logs, configuración sin secretos | no — implementación |

**Por qué casi todo es implementación.** El formato de entrega lo implementan **los receptores**, que son sistemas que flux
no controla (un PHP en un hosting compartido, la API de un cliente): necesitan una especificación estable y verificable
con vectores. El `id` determinista lo tienen que **calcular igual** todos los productores del ecosistema, o el mismo hecho
tendría dos `id`. Las rutas y los tokens de entrada, en cambio, las implementa **un solo gateway** que controla quien despliega:
fijarlas como norma solo impediría mejorarlas.

## 3. Modelo (implementación)

**El gateway aloja servicios; no los suplanta.** Cada cliente de borde es un **servicio flux normal del manifiesto**, con su
nombre, sus credenciales NATS y sus permisos, cuya conexión vive **dentro del gateway**:

```json
{ "name": "tienda-web", "via": "gateway", "owns": ["pedidos"], "consumes": ["inventario.stock.v1.ajustado"],
  "calls": ["precios.tarifa.v1.calcular"], "watches": [{ "contract": "catalogo.producto.v1", "tenants": ["acme"] }] }
```

- `"via": "gateway"` es una **anotación** ([06-security.md §3](06-security.md)): `gen-acl.mjs` le da **exactamente** los permisos
  que daría a cualquier servicio con lo mismo declarado. **El gateway no necesita ningún permiso especial**, y un cliente de
  borde **no tiene más** que el que declara: un token robado vale lo que vale esa identidad, ni un permiso más.
- `source`, los durables (`tienda-web__inventario_stock_v1_ajustado`), el inbox privado (`_INBOX.tienda-web.>`) y la DLQ
  son **los suyos**, no los del gateway. Un gateway con 20 clientes abre 20 conexiones NATS con 20 identidades.
- **Esa separación es la que protege.** Con una sola identidad para todo el gateway, un fallo en el control de acceso HTTP
  daría acceso a todo; con una por cliente, **el broker deniega lo que el cliente no tiene concedido aunque el gateway se
  equivoque** (A-150: un token válido de `catalogo-web` que llama a una operación que no tiene concedida recibe
  `PERMISSION_DENIED` **del broker** en 1 ms; en un solo proceso conviven 3 identidades sin que se mezclen sus inboxes).
- **Un cliente de borde no atiende operaciones** (no puede llevar `serves`): atender exige una suscripción viva al subject
  de cada operación ([11-request-reply.md §2.3](11-request-reply.md)) y un cliente HTTP no la sostiene. `gen-acl.mjs` lo rechaza.
- **El gateway no puede ser más permisivo que el broker**: aunque su configuración admita un tenant que el manifiesto no concede, lo deniega el broker
  (A-165), y cada denegación queda en el log del servidor con el nombre del servicio (A-164).
- Todo lo que el gateway hace por un cliente lo hace con **la API del SDK** (`publish`, `state.put`, `request`,
  `subscribe`): el desarrollador no escribe el sobre y el gateway tampoco.

## 4. La entrega saliente — **NORMATIVO**

Por cada cliente de borde hay una lista de **entregas**: *subject de evento → URL*. Cuando llega un evento a ese subject, el
gateway lo **entrega** al receptor como una petición HTTP. Es un **consumidor** más: tiene su durable, su `ack_wait`, su
backoff y su DLQ, los de [03-delivery.md §2](03-delivery.md). Lo que sigue es el contrato con el receptor.

### 4.1 La petición

```
POST <url> HTTP/1.1
Content-Type: application/cloudevents+json; charset=utf-8
Idempotency-Key: 01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55
Flux-Signature: t=1787221600,v1=3f247f6e0826ff51981ff1403ffc24b9abf8ac981c53818dc897b06173e9eba8

{"specversion":"1.0","id":"01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55","source":"/produccion/inventario-api",…,"data":{…}}
```

| Elemento | Regla |
|---|---|
| **Método** | `POST` |
| **`Content-Type`** | `application/cloudevents+json; charset=utf-8`: el *structured mode* de [01-envelope.md §1](01-envelope.md) |
| **Cuerpo** | **El CloudEvent tal cual**: la serialización de [01-envelope.md §6](01-envelope.md) (el orden de atributos, `data` el último, UTF-8 literal), **sin** extensiones `dlq*`. Si el evento llevaba `signature` y `signkeyid` ([07-signing.md](07-signing.md)), viajan tal cual. **El mismo evento produce los mismos bytes en cada intento** |
| **`Idempotency-Key`** | El **`id` del evento**, el mismo en todos los intentos. Es la clave de la estrategia C de [03-delivery.md §4](03-delivery.md) vista desde el otro lado: el receptor deduplica por ella |
| **`Flux-Signature`** | §4.2 |
| Redirecciones | **No se siguen.** Una `3xx` es una respuesta, no un camino (§4.4) |

El receptor **NO DEBE** asumir ninguna otra cabecera: un gateway **PUEDE** añadir informativas (`User-Agent`).

### 4.2 `Flux-Signature`

```
Flux-Signature: t=<unix>,v1=<hex>[,v1=<hex>…]
```

- **`t`**: segundos desde la época Unix, en decimal, **del momento de ESTE intento** (el reloj del emisor).
- **`v1`**: `HMAC-SHA256( secreto, t + "." + cuerpo )`, en **hexadecimal en minúsculas** (64 caracteres). La clave es el
  secreto tal cual (sus bytes UTF-8); el mensaje es `t` en ASCII decimal, un punto (`0x2E`) y **los bytes del cuerpo exactamente
  como se enviaron**.
- Puede haber **varios `v1`**: uno por cada secreto vigente del emisor. Es la **rotación sin corte**: durante la ventana de
  cambio, el emisor firma con el secreto viejo y el nuevo; el receptor acepta si **cualquiera** coincide y retira el viejo
  cuando quiera. Los campos que el receptor no conozca (`v2=…`) se **ignoran**: así el esquema puede crecer.
- **`t` se renueva en cada intento.** Un evento reintentado puede llegar 51 minutos después del primero
  ([03-delivery.md §2](03-delivery.md)): con un `t` fijo, un receptor que aplica la tolerancia rechazaría justo los reintentos.
  **El cuerpo no cambia** entre intentos —lo que cambia es `t` y, con él, `v1`—.

**Qué DEBE hacer el receptor** (en este orden; si algo falla, responde `4xx` —§4.4— y **no** ejecuta nada):

1. **Leer el cuerpo en bruto** y verificar sobre **esos bytes**, **antes** de interpretarlo como JSON. Reserializar cambia los
   bytes (`4995.00` → `4995`; el orden de claves; `é` frente a `é`) y la firma deja de verificar.
2. Extraer `t` (un entero) y los `v1`. Sin `t` o sin ningún `v1` válido (`^[0-9a-f]{64}$`): rechazar. **Validar el formato antes
   de decodificar el hexadecimal**: `Buffer.from("abc","hex")` en Node devuelve **un** byte, sin error (A-154).
3. Recalcular el HMAC con cada secreto vigente y **comparar en tiempo constante** con cada `v1` (`timingSafeEqual`,
   `hmac.compare_digest`, `hash_equals`, `subtle.ConstantTimeCompare`…). Una comparación con `==` filtra, por el tiempo de
   respuesta, cuántos caracteres de la firma acertó un atacante.
4. Comprobar `|ahora − t| ≤ tolerancia`, **simétrica** (un `t` del futuro también se rechaza) y de **300 s por defecto**. Sin
   esta comprobación, una petición capturada sirve para siempre: la firma sola no impide **reenviarla**.
5. **Deduplicar por `Idempotency-Key`** (§4.6).

**La tolerancia no es la ventana de reintentos**: cada intento lleva su `t` nuevo.

Los vectores de esta sección están en [`protocol.json` → `edge.delivery.signature.vectors`](../protocol.json) y los comprueba
`scripts/check-protocol-json.mjs` recalculándolos (A-155: coinciden Node, `openssl dgst -sha256 -hmac` y Python `hmac`):

| Secreto | `t` | Cuerpo | `v1` |
|---|---|---|---|
| `s3cr3t` | `1700000000` | `{"a":1}` | `8dbbbbf4523b10bbb793e74d854144c45acccc2d233667b1c06b805b6ded8a84` |
| `s3cr3t` | `1700000000` | *(vacío)* | `f63f1341b06e485fe1fe78cbfd5f6d4cee0d492c21bbc3333af817254d0db38a` |
| `whsec_flux_prueba_0123456789abcdef` | `1787221600` | el evento de §4.1 (549 bytes, con `Málaga`) | `3f247f6e0826ff51981ff1403ffc24b9abf8ac981c53818dc897b06173e9eba8` |

El tercero es la petición completa de §4.1 con el evento de inventario de [01-envelope.md §4](01-envelope.md) y su `data`
`{"sku":"sku-77","almacen":"Málaga","delta":-3,"aggregateVersion":12}`; su cuerpo exacto está en `protocol.json`.

### 4.3 El secreto

Un secreto por entrega, **opaco** (se recomiendan ≥ 32 bytes aleatorios) y **que nunca está en la configuración**: la
configuración nombra la variable de entorno (o el gestor de secretos) donde vive ([§7.5](#75-configuración-sin-secretos)). Se
comparte con el receptor **por un canal que no sea el propio HTTP**, una vez, al dar de alta la entrega.

### 4.4 Cómo se interpreta la respuesta

El emisor mira **solo el código de estado**: el cuerpo de la respuesta se descarta sin leerlo (A-161: la cabecera llega a los 2 ms
y el cuerpo puede no terminar nunca) y **nunca se registra**. La clasificación es la taxonomía de siempre
([04-errors.md §1](04-errors.md)):

| Respuesta | Clase | Presupuesto | `code` |
|---|---|---|---|
| `2xx` | **ack** | | |
| `429`, `502`, `503`, `504` (`errors.classes.retryable.httpStatus`) | **RETRYABLE** | 6 entregas | `HTTP_<n>` |
| `408`, `425` y cualquier otro `5xx` (`500`, `501`, `505`…) | **RETRYABLE acotado** | 2 entregas | `HTTP_<n>` |
| `3xx` | **PERMANENT** (no se sigue la redirección) | 1 | `HTTP_<n>` |
| cualquier otro `4xx` (`400`, `401`, `403`, `404`, `410`, `413`, `422`…) | **PERMANENT** → DLQ | 1 | `HTTP_<n>` |
| Sin respuesta en el plazo; `ECONNRESET`, `ECONNREFUSED`, `ETIMEDOUT`, `EPIPE`, `EHOSTUNREACH`, `ENETUNREACH`, `EAI_AGAIN` | **RETRYABLE** | 6 entregas | `TIMEOUT` o el del sistema |
| El certificado del receptor no verifica (`DEPTH_ZERO_SELF_SIGNED_CERT`, `CERT_HAS_EXPIRED`, `ERR_TLS_CERT_ALTNAME_INVALID`…) | **PERMANENT** | 1 | `TLS_VERIFICATION_FAILED` |
| Cualquier otro fallo (`ENOTFOUND`, `EPROTO`…) | **RETRYABLE acotado** (lo desconocido: [04-errors.md §2.1](04-errors.md)) | 2 entregas | el del sistema |

Por qué cada fila:

- **Un `4xx` permanente va directo a la DLQ.** El receptor dijo «esto no»: reintentar da el mismo `4xx` durante 51 minutos
  mientras detrás se acumulan los eventos sanos. En particular **una firma rechazada (`401`/`403`) es PERMANENT**: reintentarla
  no la arregla, y es la señal que debe despertar a alguien (un secreto desincronizado, un reloj desfasado).
- **Un `500` no es un `503`.** El `503` es «ahora no puedo»; el `500` es casi siempre un fallo del receptor con **este** evento.
  Seis intentos en 51 minutos son demasiados para un bug y pocos para una caída: lo desconocido tiene presupuesto de **2** y
  llega a la DLQ en ~30 s (A-160: dos entregas y `dlqattempts: 2`).
- **Una `3xx` es PERMANENT** y no se sigue: seguirla enviaría el cuerpo firmado a una URL que **el operador no configuró**.
  Se corrige la URL.
- **Un certificado que no verifica es PERMANENT**, no una caída: no se arregla solo en 30 s y el evento espera en la DLQ
  (90 días) a que se renueve. Un receptor con TLS roto se ve en el panel (`code=TLS_VERIFICATION_FAILED`), no como un goteo
  de timeouts.
- **Los códigos nombran el estado** (`HTTP_503`), nunca el mensaje ni el cuerpo del receptor
  ([08-observability.md §2.1](08-observability.md)).

### 4.5 Plazos, reintentos y orden

- **Plazo de cada intento: 10 s por defecto, 25 s como máximo** (el del `rpc`, [11-request-reply.md §4.2](11-request-reply.md):
  por debajo del `ack_wait` de 30 s). Es un plazo **total** —cabeceras y estado—, no de inactividad.
- **Reintentos: los del consumidor canónico** ([03-delivery.md §2](03-delivery.md)): 6 entregas, `backoff` 30 s, 1 min, 5 min, 15 min,
  30 min; a la DLQ a los **51 min 30 s** como pronto. Nada de esto lo decide la entrega.
- **`Retry-After`** (segundos o fecha HTTP) en una respuesta **reintentable** se pasa al SDK como `retryAfterMs` y **acorta o
  alarga el PRIMER reintento y nada más**: con `backoff` configurado el servidor ignora el `delay` de un `nak` desde la segunda
  reentrega ([03-delivery.md §2.2](03-delivery.md)). Medido (A-158): `503` con `Retry-After: 1` → segunda entrega a los **1004 ms**.
  Un `Retry-After` mayor de **300 s** se recorta a 300 s: una fecha del año que viene no deja un evento aparcado.
- **No hay orden.** Dos entregas a la misma URL **PUEDEN** solaparse y un reintento de un evento viejo **PUEDE** llegar después
  de uno nuevo. El receptor usa `aggregateVersion` ([AGENTS.md §5](../AGENTS.md)).
- **Una entrega por subject y cliente.** Dos URLs para el mismo evento compartirían durable, y **un durable compartido reparte
  los mensajes** entre quienes lo usan (A-47): cada URL vería la mitad.

### 4.6 Lo que DEBE el receptor, aparte de verificar

- **Deduplicar por `Idempotency-Key`** (o por el `id` del cuerpo, que es el mismo): *at-least-once* significa que el mismo evento
  llega varias veces —un reintento tras un `503`, una reentrega tras un `ack` perdido—. Un receptor sin deduplicación está roto
  aunque sus pruebas pasen. Con `INSERT` del `id` con clave única **en la misma transacción** que el efecto, y respondiendo `2xx`
  si ya estaba.
- **Responder rápido** (`2xx` en cuanto el evento está **guardado**, no cuando está **procesado**) y no bloquear: una entrega
  lenta ocupa la cola de esa entrega, que es **secuencial**.
- **No devolver `2xx` ante algo que no ha guardado.** Un `2xx` es un `ack`: el evento se da por entregado y no vuelve.

### 4.7 Clasificación de los datos en la salida

La salida **saca el dato del ecosistema**: ya no lo protege ni la ACL ni la cuenta de NATS. De ahí tres reglas
([06-security.md §5](06-security.md)):

| `dataclassification` | Se entrega |
|---|---|
| `public`, `internal` | **Sí** |
| `confidential` | **Solo si la entrega lo permite de forma EXPLÍCITA** (en el gateway de referencia, `allowClassifications`), y **solo por `https`** salvo opt-in igual de explícito. Es la clasificación del estado y del dato personal |
| `restricted` | **Nunca.** Credenciales, salud, financiero: no salen por HTTP. El evento va a la DLQ como PERMANENT |

Un evento cuya clasificación la entrega no permite es **PERMANENT** (`DELIVERY_CLASSIFICATION_DENIED`): no se reintenta, y
el mensaje de la DLQ **no cita `data`**.

### 4.8 Estado hacia HTTP (implementación)

El gateway de referencia **PUEDE** entregar también un **contrato de estado** a una URL. **No es normativo**, porque `watch` entrega
una *entrada* (`key`, `tenantId`, `deleted`, `data`, `aggregateVersion`, `revision`) y no el CloudEvent: el cuerpo es un
`application/json` con esos campos, firmado igual (§4.2) y con `Idempotency-Key: <contrato>/<tenant>/<clave>@<revision>`. El
receptor aplica `WHERE aggregate_version < $n` ([10-state.md §3.4](10-state.md)) y borra al recibir `deleted: true` ([10-state.md §4](10-state.md)). Sigue la
misma taxonomía (§4.4) y la clasificación del estado es `confidential` por defecto: exige el opt-in de §4.7.

## 5. El `id` determinista — **NORMATIVO**

La regla completa está en [01-envelope.md §2.6.1](01-envelope.md). Aquí, para qué sirve en el borde:

Un proveedor reenvía **el mismo aviso** durante horas. Si cada reenvío recibe un `id` nuevo, ningún consumidor puede
deduplicarlo: el `id` es la clave de deduplicación del ecosistema. Para un **hecho importado** el `id` se deriva de lo que lo
identifica:

```
id = UUIDv7( ms = el instante del hecho EXTERNO ,  74 bits = los 74 primeros bits de SHA-256( <source> "|" <clave externa> ) )
```

Es un UUIDv7 **válido** (versión 7, variante 10) y es **siempre el mismo**: el mismo hecho reenviado tres veces produce el
mismo evento tres veces, y los consumidores —que ya deduplican por `id`— aplican **un** efecto. El SDK lo ofrece como
`publish(subject, data, { externalKey, time })`. **Exige `time`**: el instante que da el sistema externo, no «ahora».

**Cómo lo usa el gateway** (implementación):

- **Webhooks de terceros** (§6.3): la clave externa es `<nombre del hook>:<id del evento del proveedor>` y el instante, el del
  propio hecho, leído del cuerpo o de una cabecera del proveedor. **El nombre del hook va en la clave** porque dos
  proveedores pueden numerar sus eventos igual (`1`, `2`…) y, con la misma `source`, producirían el mismo `id`.
- **`Idempotency-Key` + `Flux-Event-Time`** en `POST /v1/events`: un cliente HTTP que reintenta tras un corte de red no duplica el
  evento.

**Lo que la regla no puede hacer.** Un proveedor que **no da el instante del hecho** (GitHub no lo trae en la cabecera de
entrega) no tiene `id` determinista: con «ahora», cada reenvío tendría el suyo. El hook se declara **no idempotente**
(`"idempotent": false`) y el consumidor deduplica por una clave de negocio de `data`, como ya hace con cualquier evento
([03-delivery.md §4](03-delivery.md)). La alternativa de fijar los milisegundos a `0` conserva el determinismo y pierde el orden
por `id`; **no se adopta** (§11).

**Dentro de la ventana de `duplicate_window`** (2 min) JetStream ni siquiera guarda el duplicado (A-157: tres envíos, **un**
mensaje en el stream). **Fuera**, guarda las dos copias y la que protege es la deduplicación por `id` del consumidor (A-157: cuatro
envíos con el último pasada la ventana → dos mensajes, dos entregas, **un** efecto). Por eso el `id` estable importa: la ventana
es una optimización, no la garantía.

## 6. Entrada HTTP → flux (implementación)

### 6.1 Rutas

| Ruta | Hace | Respuesta |
|---|---|---|
| `POST /v1/events/{subject}` | Publica un evento. El cuerpo es `data` | `202 { "id" }` |
| `PUT /v1/state/{contrato}/{clave}` | `state.put`. El cuerpo es `data` (con `aggregateVersion`) | `200 { applied, outcome, revision }` |
| `DELETE /v1/state/{contrato}/{clave}?aggregateVersion=n` | `state.delete` | `200 { applied, outcome, revision }` |
| `GET /v1/state/{contrato}/{clave}` | `state.get` | `200 { key, tenantId, deleted, data, aggregateVersion, revision }`, o `404` |
| `POST /v1/rpc/{operacion}` | `request`. El cuerpo es `data` | `200 <data de la respuesta>` |
| `POST /v1/hooks/{nombre}` | Webhook de un tercero → evento | `202 { "id" }` |
| `GET /healthz` | ¿están conectados los clientes? Sin autenticación ni datos | `200` / `503` |

- El **cuerpo** es siempre `application/json` y un **objeto**: es `data` ([01-envelope.md §4](01-envelope.md)). El desarrollador **no
  escribe el sobre** y el gateway tampoco: `id`, `source`, `time`, `type`, `correlationid` y `traceparent` los pone el SDK.
- **Cabeceras opcionales** (`Flux-*`): `Flux-Aggregate-Id` (→ `aggregateId`), `Flux-Tenant` (solo uno de los `tenants` que el
  cliente tiene declarados), `Flux-Classification`, `Flux-Event-Time` (RFC 3339, 3 decimales y `Z`), `Flux-Timeout-Ms` (solo `rpc`,
  1–25 000) e `Idempotency-Key` (§5: con ella, `Flux-Event-Time` es obligatoria).
- Un `PUT` con un `aggregateVersion` que no supera el actual **no es un error**: `200` con `applied: false, outcome: "stale"`
  ([10-state.md §3](10-state.md)). Una orden por `rpc` que da `RPC_TIMEOUT` **pudo ejecutarse** (11-request-reply.md §4.3): el
  cuerpo del `504` lo dice, y el cliente HTTP que reintenta lleva en `data` una **clave de negocio** que el que responde deduplica.
- **No hay `serves` por HTTP**: el gateway llama operaciones, no las atiende (§3).

### 6.2 Autenticación

`Authorization: Bearer <token>`. **El gateway guarda solo el SHA-256 del token** (en la configuración, que no es secreta porque
un hash de 256 bits aleatorios no se invierte) y **compara en tiempo constante**: calcula el SHA-256 del token recibido y
compara los 32 bytes con `timingSafeEqual`. Así las longitudes son siempre iguales (A-154: con longitudes distintas,
`timingSafeEqual` lanza `ERR_CRYPTO_TIMING_SAFE_EQUAL_LENGTH`) y el tiempo no depende de cuánto del token acertó quien lo prueba.

- Un token **identifica a un cliente** de borde (una identidad NATS) y, **por la ACL**, a lo que ese cliente puede hacer.
- Un cliente puede tener **varios tokens** (`id`s distintos): es la rotación sin corte, igual que los `v1` de §4.2.
- El token se genera con 32 bytes aleatorios (`node gateway/src/index.mjs token`): tiene la entropía de una clave, no de una
  contraseña, y por eso basta un SHA-256 sin sal ni coste.
- Sin token, o con uno que no es de nadie, es `401` y **el mismo** `401` en los dos casos.

### 6.3 Webhooks de terceros

`POST /v1/hooks/{nombre}` **no lleva** `Authorization`: un proveedor de pagos no envía tu token. Lo autentica **su firma**.

- **Verificación HMAC-SHA256 configurable**: la cabecera que lleva la firma, el formato (`hex`; `sha256=<hex>`, como GitHub; o
  `t=…,v1=…`, como Stripe y como §4.2), si el mensaje firmado es el cuerpo o `t.cuerpo`, el secreto **por variable de entorno** (una o
  varias: rotación) y la **tolerancia de tiempo** cuando la firma incluye un instante. Sin `verify`, el hook **no arranca**: un
  endpoint público que publica eventos sin autenticar es una puerta abierta.
- Los vectores: la firma de GitHub («`It's a Secret to Everybody`» sobre «`Hello, World!`» → `757107ea…3e17`, A-155) y el
  formato propio de §4.2.
- **El cuerpo del tercero es el `data`** (o una selección de él: `data.pick`). **No es PII-free por construcción**: un cobro
  trae el email del pagador. La **clasificación del hook es `confidential` por defecto** (7 días de retención, no 30: [06-security.md §5](06-security.md))
  y el propietario del hook decide qué campos pasan (`pick`) para respetar la regla 5 de `AGENTS.md`.
- El evento se publica **en el dominio del cliente** que posee el hook (un cliente solo publica en lo suyo: lo impone el broker),
  con el `id` determinista de §5. **El mismo webhook enviado tres veces responde tres veces `202` con el mismo `id`** y produce
  **un** efecto en el consumidor.
- Una firma ausente o inválida es `401 EDGE_INVALID_WEBHOOK_SIGNATURE`; fuera de tolerancia,
  `401 EDGE_WEBHOOK_TIMESTAMP_OUT_OF_TOLERANCE` (distintas para que un reloj desfasado no se confunda con un secreto equivocado).

### 6.4 Errores HTTP

El cuerpo de un error es siempre:

```json
{ "error": { "code": "PERMISSION_DENIED", "class": "permanent", "message": "…", "retryAfterMs": 1000 } }
```

`code` es un **código estable del protocolo** ([04-errors.md](04-errors.md)) siempre que existe uno; `class` es `retryable` o
`permanent` (la del protocolo); `message` dice **qué** y **cómo se arregla** y **nunca** cita valores de `data`.
`Retry-After` (segundos) acompaña a lo reintentable.

**La regla de los estados HTTP**: `4xx` = el cliente lo ha hecho mal y repetir igual da lo mismo (salvo `408`, `409` y `429`); `5xx` =
el fallo es del gateway o del ecosistema y **`class` dice si reintentar sirve**. Por eso **`502`, `503` y `504` son siempre
reintentables**: muchos clientes y proxies los reintentan solos, y un error permanente con uno de ellos invitaría a insistir. Los
permanentes del lado del ecosistema (`NOT_PROVISIONED`, `RPC_INVALID_REPLY`) son `500`.

| Código del protocolo | Estado | `class` |
|---|:--:|---|
| `INVALID_SCHEMA` | `422` | permanent |
| `INVALID_DATACLASSIFICATION`, `RESERVED_DOMAIN`, `INVALID_STATE_CONTRACT`, `INVALID_STATE_KEY`, `INVALID_TENANT_FOR_STATE`, `INVALID_RPC_CALL` | `400` | permanent |
| `MISSING_AGGREGATE_VERSION` | `422` | permanent |
| `STATE_CONFLICT` | `409` | retryable |
| `PERMISSION_DENIED` | `403` | permanent |
| `NOT_PROVISIONED`, `SCHEMA_NOT_FOUND` | `500` | permanent (lo arregla quien opera) |
| `RPC_UNAVAILABLE` | `503` | retryable (`Retry-After: 1`) |
| `RPC_TIMEOUT` | `504` | retryable — **ambiguo: la operación pudo ejecutarse** |
| `RPC_MESSAGE_TOO_LARGE` | `413` | permanent |
| `RPC_INVALID_REPLY` | `500` | permanent (un incidente: un productor roto o un stream que captura `rpc.>`) |
| `STALE_REQUEST` | `400` | permanent |
| Un error del que responde (`RetryableError` / `PermanentError` con su `code`) | `503` / `422` | el suyo |

Y los que **solo existen en el borde** (no son códigos del protocolo ni de ninguna capacidad: los define el gateway de referencia,
`protocol.json` → `edge.gateway.errors`):

| Código | Estado | Cuándo |
|---|:--:|---|
| `EDGE_UNAUTHENTICATED` | `401` | Sin token, o que no es de ningún cliente (`WWW-Authenticate: Bearer`) |
| `EDGE_RATE_LIMITED` | `429` | Se supera el límite de peticiones **por token** (`Retry-After`) |
| `EDGE_BODY_TOO_LARGE` | `413` | El cuerpo supera el límite (1 MiB); la conexión se cierra |
| `EDGE_INVALID_BODY` | `400` | No es JSON, o no es un objeto |
| `EDGE_UNSUPPORTED_MEDIA_TYPE` | `415` | El `Content-Type` no es `application/json` |
| `EDGE_INVALID_REQUEST` | `400` | Un subject, un contrato o una cabecera `Flux-*` mal formada |
| `EDGE_TENANT_NOT_ALLOWED` | `403` | `Flux-Tenant` no es de los que el cliente tiene declarados |
| `EDGE_NOT_FOUND` | `404` | Ruta, hook o clave de estado inexistente |
| `EDGE_METHOD_NOT_ALLOWED` | `405` | |
| `EDGE_INVALID_WEBHOOK_SIGNATURE` | `401` | La firma de un webhook falta o no verifica |
| `EDGE_WEBHOOK_TIMESTAMP_OUT_OF_TOLERANCE` | `401` | La firma verifica y su instante se aleja de la tolerancia |
| `EDGE_WEBHOOK_PAYLOAD_INVALID` | `422` | La clave o el instante que el hook declara no están en el cuerpo |
| `EDGE_SHUTTING_DOWN` | `503` | El gateway se está parando |
| `EDGE_INTERNAL` | `500` | Un fallo que el gateway no sabe clasificar; el mensaje es genérico y **no** cita la excepción |

## 7. Seguridad (implementación)

### 7.1 Límites

- **Cuerpo: 1 MiB** ([01-envelope.md §7](01-envelope.md)). Se comprueba **antes de leer**: un `Content-Length` mayor es `413` sin
  leer el cuerpo, **con `Connection: close` y cerrando el socket**. Medido (A-152): sin cerrarlo, Node **lee y descarta los
  52 428 800 bytes declarados** aunque ya haya respondido `413`; cerrando, lee 65 481. Sin `Content-Length` (*chunked*), se corta al
  pasar el límite (A-152: a 9 090 bytes del límite). El **evento** también tiene 1 MiB y el sobre ocupa unos 425 bytes: un cuerpo de
  1 047 673 bytes cabe y uno de 1 048 473, no (A-152), y es `413` igualmente.
- **Peticiones por token**: un cubo de tokens por token (`requests` por `perSeconds`, con ráfaga), en memoria: con **N
  instancias**, N veces el límite. Al agotarse, `429` con `Retry-After`. Los webhooks, que no llevan token, tienen **su cubo por
  nombre de hook**.
- **Clientes lentos**: `headersTimeout` y `requestTimeout` de Node **solo se comprueban cada `connectionsCheckingInterval`, que son
  30 s por defecto** (A-153: con los valores por defecto, un cliente que manda el cuerpo a cuentagotas no se corta en 3 s; con `100`
  ms, `408` a los 597 y 1103 ms para unos límites de 500 y 1000). Y **una conexión que no envía nada no la corta ninguno**: solo
  `server.timeout`, que **debe ser mayor que el timeout máximo de un `rpc`** (25 s) porque mide inactividad **del socket mientras el
  gateway espera al broker** (A-153: con `1000`, mata también una petición cuyo handler tarda 2500 ms). El gateway usa **60 s**.
- Un cliente que **corta a mitad de una petición `rpc`** no cancela nada: la operación **se ejecuta igualmente** (A-151: el efecto se
  aplicó una vez, `close` llegó con `writableFinished: false` y escribir después no lanza). Es la misma ambigüedad de
  [11-request-reply.md §13](11-request-reply.md): quien reintenta lleva una clave de negocio.

### 7.2 TLS: dos modos

Los dos son válidos y **uno de los dos es obligatorio en producción**: el token viaja en una cabecera.

| Modo | Cuándo | Cómo |
|---|---|---|
| **Termina delante** (proxy inverso: nginx, Caddy, un balanceador) | Lo habitual: el certificado y su renovación ya son de otro | El gateway escucha en HTTP **dentro de una red privada**. El proxy **DEBE** quitar y volver a poner `X-Forwarded-*` y no reenviar `Authorization` de fuera de su sitio |
| **En el propio gateway** | Sin proxy | `listen.tls: { certFile, keyFile }` (rutas, no el contenido): el gateway escucha con `https` |

La conexión **del gateway a NATS** es otra cosa: `nats.tls` con la CA propia y, si se quiere, certificado de cliente
([06-security.md §2](06-security.md)).

### 7.3 Registros

El gateway **no registra nunca `data`**, de ningún evento: ni el cuerpo de una petición ni el de una entrega ni el de una respuesta
(en particular, **nunca el de un `confidential`**: lo que entra a un `log` acaba en sistemas con otra retención y otros lectores).
Tampoco el token ni `Authorization` ni los secretos. Registra **qué pasó**: cliente, ruta, estado, duración, `id` del evento,
subject u operación, `code`, con una **lista blanca** de campos —no una lista negra— para que un campo nuevo no se cuele por olvido (A-163: un canario en el
`data` de un evento, un estado, una llamada y un webhook no aparece en ninguna línea ni en el cuerpo de un error). El `message` de un error, como exige [04-errors.md §5.2](04-errors.md), no cita valores de `data`.

### 7.4 Parada ordenada

Al recibir `SIGTERM`: dejar de aceptar conexiones, **contestar las peticiones en vuelo** con `Connection: close` y cerrar las ociosas, y
solo entonces `drain` de las conexiones NATS. Medido (A-162): `server.close()` solo **no** cierra la conexión por la que sale la
última respuesta en vuelo (sigue `keep-alive`) y termina a los **6701 ms** (la petición de 800 ms más el `keepAliveTimeout` de 5 s); con
`Connection: close` en las respuestas durante la parada y `closeIdleConnections()` termina a los **703 ms**, cuando acaba la petición de 800 ms. Con un plazo máximo, tras el cual se
cierra lo que quede.

### 7.5 Configuración sin secretos

`gateway.json` se puede **versionar y revisar**: no contiene ningún secreto, solo **nombres de variables de entorno**
(`nkeySeedEnv`, `secretEnv`) y los **SHA-256** de los tokens. El gateway **se niega a arrancar** si ve un secreto en claro
(una clave `nkeySeed`, `token`, `secret`, `password`, `pass`…, o un valor con la forma de una semilla de NATS) y si falta una de las
variables que nombra, **diciendo cuál**.

## 8. Lo medido

Observado el 2026-11-26 contra **nats-server 2.14.5** y **Node 22.23.5**. A-150 a A-166 están en el
[Apéndice A](../docs/plan-v1.1/PLAN.md#apéndice-a--evidencia):

| | Qué | Dónde se reproduce |
|---|---|---|
| A-150 | Tres identidades en un proceso, sin mezclar inboxes; el broker deniega al que no tiene concesión | `gateway/test/medidas.test.mjs` |
| A-151 | Un cliente que corta a mitad de un `rpc`: la operación se ejecuta | ídem |
| A-152 | El límite de cuerpo: `Content-Length` declarado, *chunked*, y el sobre | ídem |
| A-153 | Los clientes lentos y los tiempos de Node | ídem |
| A-154 | `timingSafeEqual` y el hexadecimal impar | ídem |
| A-155 | HMAC: GitHub, `openssl` y Python | `gateway/test/firma.test.mjs` |
| A-156 | El `id` determinista es un UUIDv7 válido | `sdk-node/test/external-id.test.ts` |
| A-157 | `duplicate_window` y la deduplicación por `id` | `gateway/test/e2e-hooks.test.mjs`, `sdk-node/test/integration/external-id.test.ts` |
| A-158 a A-160 | Reintentos, DLQ y el presupuesto acotado de la entrega | `gateway/test/e2e-salida.test.mjs` |
| A-161 | El cliente HTTP de salida | `gateway/test/medidas.test.mjs` |
| A-162 | La parada ordenada | ídem |
| A-163 | `data` no aparece en ningún registro (lista blanca de campos) | `gateway/test/e2e-entrada.test.mjs` |
| A-164 | Una denegación queda en el log del servidor con el nombre del servicio: lo deniega el broker | ídem |
| A-165 | El tenant: el gateway no puede ser más permisivo que el broker | ídem |
| A-166 | `Promise.all` fuga conexiones al fallar el arranque | `gateway/test/e2e-ciclo.test.mjs` |

## 9. Lo que el gateway NO resuelve

- **No es exactamente-una-vez.** Una entrega es *at-least-once*: el receptor deduplica (§4.6). El `id` determinista da un
  `id` estable **si el proveedor da el instante del hecho** (§5).
- **No es tiempo real hacia el navegador** (SSE, WebSocket): el navegador es cliente de una web, no un sistema del ecosistema
  ([plan §6](../docs/plan-v1.1/PLAN.md)).
- **No atiende operaciones por HTTP** (§3): llama, no responde.
- **No limita por IP ni protege de un DDoS**: es trabajo del proxy de delante. El límite por token acota a un cliente legítimo
  que se desboca, no a un atacante.
- **No ordena** (§4.5) y **no propaga el plazo** de la petición HTTP al que responde (11-request-reply.md §13).
- **No se probó con carga sostenida** ni con varias instancias del gateway detrás de un balanceador (el límite por token es por
  instancia).
- **La entrega de estado no es un CloudEvent** (§4.8) porque el SDK entrega la entrada y no el sobre.

## 10. Qué DEBE un gateway que se diga conforme

Solo lo normativo (§2):

1. La entrega saliente de §4.1: método, `Content-Type`, **el cuerpo es el CloudEvent tal cual** y los mismos bytes en cada intento,
   `Idempotency-Key` = `id`.
2. `Flux-Signature` de §4.2 con **los tres vectores**, `t` **renovado en cada intento**, y varios `v1` para rotar.
3. La clasificación de §4.4, **tabla entera**: un `4xx` permanente va a la DLQ sin reintentos, una `3xx` no se sigue, lo desconocido
   tiene presupuesto de 2.
4. Las reglas de clasificación de §4.7 (`restricted` nunca; `confidential` solo con autorización explícita).
5. El `id` determinista de [01-envelope.md §2.6.1](01-envelope.md) con **los vectores de
   `conformance/cases/cross-sdk-external-id.json`**.

## 11. Decidido provisionalmente

Puntos que el plan dejaba abiertos y que se cierran aquí con el criterio que explica cada sección. Son del dueño del proyecto si
prefiere otra cosa; cambiar cualquiera es barato **ahora** y deja de serlo cuando haya receptores en producción:

| # | Decisión | Sección |
|---|---|---|
| 1 | **`t` se renueva en cada intento** y el cuerpo no cambia: un `t` fijo haría que la tolerancia rechazase los reintentos de 51 min | §4.2 |
| 2 | **Varios `v1`** en la cabecera para rotar secretos sin corte; los campos desconocidos se ignoran | §4.2 |
| 3 | `429`, `502`, `503`, `504` son RETRYABLE completo; `408`, `425` y el resto de `5xx`, **acotado a 2**; el plan decía «5xx → RETRYABLE» a secas | §4.4 |
| 4 | Una **`3xx` es PERMANENT** y no se sigue | §4.4 |
| 5 | Un **certificado que no verifica es PERMANENT** (`TLS_VERIFICATION_FAILED`) | §4.4 |
| 6 | **Plazo por intento 10 s** (máximo 25 s); `Retry-After` recortado a 300 s | §4.5 |
| 7 | Una entrega por subject y cliente (un durable compartido reparte los mensajes) | §4.5 |
| 8 | **`restricted` nunca sale** por HTTP; `confidential` solo con autorización explícita de la entrega y `https` | §4.7 |
| 9 | La clave externa lleva el **nombre del hook** (`<hook>:<id del proveedor>`) | §5 |
| 10 | Un proveedor sin instante del hecho **no tiene `id` determinista** (el hook es `idempotent: false`); no se adopta «ms = 0» | §5 |
| 11 | La clasificación de un hook es **`confidential`** por defecto y el propietario elige qué campos pasan (`pick`) | §6.3 |
| 12 | **Hooks sin verificación no arrancan**; solo HMAC-SHA256 | §6.3 |
| 13 | `Idempotency-Key` en `POST /v1/events` exige `Flux-Event-Time` | §5, §6.1 |
| 14 | `PUT` con versión obsoleta es **`200 stale`**, no un error | §6.1 |
| 15 | `RPC_TIMEOUT` es **`504`** y su cuerpo dice que **pudo ejecutarse** | §6.4 |
| 16 | Los fallos del lado del ecosistema que arregla quien opera (`NOT_PROVISIONED`) son **`500` permanent**, no `503` | §6.4 |
| 17 | El gateway **no registra nunca `data`** (más estricto que «los `confidential`») | §7.3 |
| 18 | `server.timeout` de **60 s** (>25 s del `rpc`) y los comprobadores de Node a 1 s | §7.1 |
| 19 | El estado hacia HTTP **no es un CloudEvent**: es la entrada del `watch`, y no es normativo | §4.8 |
| 20 | Un cliente de borde **no puede llevar `serves`** | §3 |
