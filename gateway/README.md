# flux-gateway — el borde HTTP de flux

Un gateway **en Node, sobre el SDK de `sdk-node`, sin framework web** (`node:http`) para lo que **no puede mantener una conexión NATS**:
un PHP en un hosting compartido, un `cron`, una función *serverless*, el sistema de un tercero. **HTTP queda solo en el borde; dentro del
ecosistema, nada lo usa.**

| Dirección | Qué hace | Para |
|---|---|---|
| **HTTP → flux** | `POST /v1/events/{subject}` publica un evento · `PUT`/`DELETE`/`GET /v1/state/{contrato}/{clave}` · `POST /v1/rpc/{operacion}` · `POST /v1/hooks/{nombre}` convierte un **webhook de un tercero** en un evento | fila 8 y 10 del [mapa de sustitución](../docs/plan-v1.1/PLAN.md) |
| **flux → HTTP** | entrega un evento a una URL como `POST` **firmado** (`Flux-Signature`, `Idempotency-Key`), con los reintentos y la DLQ de siempre | fila 9 |

> **Qué es protocolo y qué es implementación.** Es **normativo** solo el formato de la **entrega saliente** (§ [Entregas salientes](#entregas-salientes-flux--http))
> y el **`id` determinista** de un hecho importado ([`01-envelope.md` §2.6.1](../specification/01-envelope.md)). Todo lo demás —las rutas de entrada, los
> tokens, los límites, los errores HTTP, `gateway.json`— es **este gateway**: otro puede hacerlo distinto y seguir siendo interoperable.
> Ver [`specification/12-edge.md`](../specification/12-edge.md), que lo dice sección por sección.

## El modelo: el gateway aloja servicios, no los suplanta

Cada cliente de borde es **un servicio flux normal del manifiesto**, con su nombre, sus credenciales NATS y sus permisos, cuya conexión vive
**dentro del gateway**:

```json
{ "name": "tienda-web", "via": "gateway", "owns": ["pedidos"], "consumes": ["inventario.stock.v1.ajustado"],
  "calls": ["precios.tarifa.v1.calcular"], "watches": [{ "contract": "catalogo.producto.v1", "tenants": ["acme"] }] }
```

`"via": "gateway"` es una **anotación**: `gen-acl.mjs` le da exactamente los permisos de cualquier servicio con lo mismo declarado. **El gateway no
necesita ningún permiso especial**, y un token robado vale lo que vale esa identidad. Un token válido sobre algo que el cliente no tiene concedido
**lo deniega el broker**, aunque el gateway se equivocara (medido: A-150). Un cliente de borde no puede atender operaciones (`serves`): HTTP no
sostiene la suscripción viva que eso exige.

## Un ejemplo completo, de principio a fin

Requisitos: Node ≥ 20 y un `nats-server` 2.14 (`GOBIN=$PWD/bin go install github.com/nats-io/nats-server/v2@v2.14.5`).

```bash
(cd sdk-node && npm ci && npm run build) && (cd conformance && npm ci) && (cd cli && npm ci) && (cd gateway && npm ci)
NATS_SERVER=$PWD/bin/nats-server node gateway/examples/ejemplo.mjs
```

Eso levanta **un nats-server real con las ACLs que genera `gen-acl.mjs`** para [`examples/services.json`](examples/services.json), aprovisiona
los streams con `flux provision`, arranca el gateway con [`examples/gateway.json`](examples/gateway.json), un receptor HTTP que verifica la firma de
las entregas y los servicios nativos (`precios-api` responde, `contabilidad-api` consume, `inventario-api` publica). Imprime los `export` para la
otra terminal, donde [`examples/demo.sh`](examples/demo.sh) hace, **solo con `curl`**:

```
== publicar un evento -> 202
{"id":"01a218ea-1819-754a-a0f3-fb36a7e52b4d"}
== escribir estado -> 200
{"applied":true,"outcome":"ok","revision":1}
== leer estado -> 200
{"key":"prod-1","tenantId":"acme","deleted":false,"data":{"productoId":"prod-1","nombre":"Mesa de roble","aggregateVersion":1},…}
== llamar a una operación -> 200
{"totalCents":9900,"moneda":"EUR","sku":"prod-1"}
== una operación no concedida (la deniega el broker) -> 403
{"error":{"code":"PERMISSION_DENIED","class":"permanent","message":"el broker denegó publicar en \"rpc.precios.tarifa.v1.calcular\" a \"catalogo-web\"…"}}
== webhook de la pasarela, envío 1 -> 202   {"id":"01a01eb5-6700-79f2-91fe-3c3b040faedf"}
== webhook de la pasarela, envío 2 -> 202   {"id":"01a01eb5-6700-79f2-91fe-3c3b040faedf"}
== webhook de la pasarela, envío 3 -> 202   {"id":"01a01eb5-6700-79f2-91fe-3c3b040faedf"}
✓ aceptación
```

y en la terminal del ejemplo, **un solo efecto** para los tres webhooks, y la entrega saliente verificada:

```
  [contabilidad-api] pagos.cobro.v1.recibido id=01a01eb5-6700-79f2-91fe-3c3b040faedf → UN efecto
  [tienda.example.com] POST /hooks/flux Idempotency-Key=01a218e9-eda1-717e-9ce5-35a0ebd37e21 firma VERIFICA
```

Este mismo entorno es el que ejecutan las pruebas (`npm test`): lo que ves es lo que se prueba, incluido el `demo.sh`.

## Entrada: HTTP → flux

Todas las rutas llevan `Authorization: Bearer <token>`, salvo los webhooks (los autentica su firma) y `/healthz`. **El cuerpo es `data`**: un objeto JSON.
El gateway **no escribe el sobre** —`id`, `source`, `time`, `type`, `correlationid`, `traceparent` los pone el SDK—.

| Ruta | Respuesta | Cabeceras opcionales |
|---|---|---|
| `POST /v1/events/{subject}` | `202 { "id" }` | `Flux-Aggregate-Id`, `Flux-Tenant`, `Flux-Classification`, `Flux-Event-Time` + `Idempotency-Key` |
| `PUT /v1/state/{contrato}/{clave}` | `200 { applied, outcome, revision }` (un put obsoleto es `outcome: "stale"`, **no** un error) | `Flux-Tenant`, `Flux-Classification` |
| `DELETE /v1/state/{contrato}/{clave}?aggregateVersion=n` | `200 { applied, outcome, revision }` | `Flux-Tenant` |
| `GET /v1/state/{contrato}/{clave}` | `200 { key, tenantId, deleted, data, aggregateVersion, revision }` o `404` | `Flux-Tenant` |
| `POST /v1/rpc/{operacion}` | `200 <data de la respuesta>` | `Flux-Timeout-Ms` (1–25000), `Flux-Aggregate-Id`, `Flux-Tenant` |
| `POST /v1/hooks/{nombre}` | `202 { "id" }` | las del proveedor |
| `GET /healthz` | `200 { status, clientes, conectados }` / `503` | — |

- **Reintentar sin duplicar.** `Idempotency-Key` junto a `Flux-Event-Time` da un **`id` determinista**: el mismo POST reintentado tras un corte de red no duplica el
  evento. Sin el instante del hecho no hay id estable, y el gateway lo rechaza.
- **Una llamada `rpc` que expira (`504 RPC_TIMEOUT`) pudo ejecutarse**, y lo mismo si el cliente HTTP corta a mitad (A-151): reintenta con una **clave de negocio** en `data` que
  quien responde deduplique ([`11-request-reply.md` §4.5](../specification/11-request-reply.md)).
- **Los errores** son `{ "error": { "code", "class", "message", "retryAfterMs"? } }`. `code` es el **código estable del protocolo** cuando existe
  (`PERMISSION_DENIED`→`403`, `RPC_UNAVAILABLE`→`503`, `RPC_TIMEOUT`→`504`, `INVALID_SCHEMA`→`422`, `STATE_CONFLICT`→`409`…) y `EDGE_*` cuando solo existe en el borde (`EDGE_UNAUTHENTICATED`→`401`,
  `EDGE_RATE_LIMITED`→`429`, `EDGE_BODY_TOO_LARGE`→`413`…). Un `4xx` es «repetir igual da lo mismo» (salvo `408`, `409`, `429`); un `5xx`, «el fallo es nuestro», y `class` dice si reintentar sirve.
  Tabla completa: [`12-edge.md` §6.4](../specification/12-edge.md).

### Webhooks de terceros

`POST /v1/hooks/{nombre}` **no lleva token**: lo autentica la **firma del proveedor** (HMAC-SHA256, configurable). Un hook **sin `verify` no arranca**: sería un endpoint
público que publica eventos. El cuerpo del tercero es el `data`… **y no está libre de PII** (un cobro trae el email del pagador): la clasificación del hook es `confidential` por
defecto y `data.pick` elige los campos que pasan (regla 5 de `AGENTS.md`).

```json
"pasarela": {
  "subject": "pagos.cobro.v1.recibido",
  "verify": { "scheme": "hmac-sha256", "secretEnv": "PASARELA_HOOK_SECRETO", "signature": { "header": "X-Pasarela-Firma", "format": "t-v1" }, "toleranceSeconds": 300 },
  "externalKey": { "from": "body", "pointer": "/id" },
  "time":        { "from": "body", "pointer": "/created", "unit": "seconds" },
  "data": { "pick": { "pagoId": "/id", "importeCents": "/amount", "moneda": "/currency" } }
}
```

- **Formatos de firma**: `hex`, `sha256-prefix` (`sha256=<hex>`, como GitHub) y `t-v1` (`t=<unix>,v1=<hex>`, como Stripe). Con `t`, la **tolerancia** es simétrica (300 s por defecto) y se mira
  después de la firma; `secretEnv` puede ser una lista para rotar sin corte.
- **El mismo webhook enviado tres veces produce un solo efecto.** La clave del evento del proveedor y **el instante del hecho** (no «ahora») dan un
  **`id` determinista** — un UUIDv7 válido y siempre el mismo —: dentro de `duplicate_window` JetStream guarda **un** mensaje; **fuera**, guarda los dos y los consumidores, que
  deduplican por `id`, aplican **uno** (A-157). La clave lleva el **nombre del hook** (`pasarela:evt_1Nx`): dos proveedores pueden numerar sus eventos igual.
- **Un proveedor que no da el instante del hecho** (GitHub no lo trae en la cabecera de entrega) **no tiene `id` determinista**: se declara `"idempotent": false` y el consumidor deduplica por una clave de negocio.

## Entregas salientes: flux → HTTP

```json
"deliveries": [{ "subject": "inventario.stock.v1.ajustado", "url": "https://tienda.example.com/hooks/flux", "secretEnv": "TIENDA_WEB_ENTREGA_SECRETO", "timeoutMs": 10000 }]
```

El gateway es **un consumidor más** (su durable, su `ack_wait`, su backoff, su DLQ) cuyo handler hace:

```
POST https://tienda.example.com/hooks/flux
Content-Type: application/cloudevents+json; charset=utf-8
Idempotency-Key: 01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55
Flux-Signature: t=1787221600,v1=3f247f6e0826ff51981ff1403ffc24b9abf8ac981c53818dc897b06173e9eba8

{"specversion":"1.0","id":"01924f8e-…","source":"/produccion/inventario-api",…,"data":{…}}
```

El cuerpo es **el CloudEvent tal cual** (los mismos bytes en cada intento). `v1 = HMAC-SHA256(secreto, t + "." + cuerpo)` en hexadecimal; **`t` se renueva en cada intento**
(un reintento puede llegar 51 minutos después y una tolerancia de 300 s lo rechazaría si fuera fijo); varios `v1` = rotación sin corte.

| El receptor responde | Qué pasa |
|---|---|
| `2xx` | **ack** |
| `429`, `502`, `503`, `504`, sin respuesta en el plazo, `ECONNRESET`… | **RETRYABLE**: 6 entregas, 51 min 30 s hasta la DLQ. `Retry-After` acorta o alarga **el primer** reintento |
| `408`, `425`, `500` y el resto de `5xx` | RETRYABLE **acotado a 2 entregas** (un `500` no es un `503`) |
| `3xx` | **PERMANENT**: **no se sigue** la redirección |
| cualquier otro `4xx` (también un `401` por una firma rechazada) | **PERMANENT** → DLQ, sin reintentos |
| certificado que no verifica | PERMANENT (`TLS_VERIFICATION_FAILED`) |

**Qué sale del ecosistema**: `public` e `internal` sí; `confidential` **solo si la entrega lo autoriza** (`allowClassifications`) y por `https`; `restricted`, **nunca**. `http://` exige
`"allowInsecureHttp": true`. **Una entrega por subject y cliente**: dos URLs compartirían durable y **se repartirían los mensajes** (A-47). También se pueden entregar contratos
de estado (`{ "contract", "tenant", … }`): el cuerpo es la *entrada* del `watch`, **no** un CloudEvent, y no es normativo.

### El receptor

Un receptor **DEBE**: (1) verificar sobre los **bytes en bruto**, antes de interpretarlos; (2) validar el formato del hexadecimal antes de decodificarlo; (3) **comparar en tiempo constante**;
(4) comprobar la tolerancia, **simétrica**; (5) **deduplicar por `Idempotency-Key`**; y responder `2xx` solo cuando haya **guardado**. Hay uno en PHP ([`examples/receptor.php`](examples/receptor.php), para el hosting compartido)
y otro en Python ([`examples/receptor.py`](examples/receptor.py)); las pruebas los ejecutan contra los vectores de [`protocol.json`](../protocol.json) (recalculados con `openssl` y Python):

```php
$r = verificar_flux_signature($_SERVER['HTTP_FLUX_SIGNATURE'] ?? '', file_get_contents('php://input'), [getenv('FLUX_SECRETO'), getenv('FLUX_SECRETO_NUEVO')]);
if ($r !== 'ok') { http_response_code(401); exit; }          // un 4xx es PERMANENT: el evento va a la DLQ, que es lo correcto
guardar_si_no_existe($_SERVER['HTTP_IDEMPOTENCY_KEY'], $cuerpo); // INSERT con clave única; si ya estaba, 2xx sin repetir el efecto
http_response_code(204);
```

## Configuración: `gateway.json`

**No contiene secretos**: solo **nombres de variables de entorno** (`nkeySeedEnv`, `secretEnv`) y el **SHA-256** de los tokens. El gateway **se niega a arrancar** si ve un secreto en
claro (una clave `nkeySeed`, `token`, `secret`, `password`, `pass`…, una semilla de NATS, una clave privada), si falta una variable (dice cuál) y recoge **todos** los problemas de una vez.
`flux-gateway --check --config gateway.json` valida sin arrancar.

| Clave | Para qué | Por defecto |
|---|---|---|
| `listen` | `host`, `port`, `tls: { certFile, keyFile }` (rutas) | `127.0.0.1:8080`, sin TLS |
| `nats` | `servers`, `tls: { caFile, certFile, keyFile }` | — |
| `environment`, `version` | `source` (`/<entorno>/<servicio>`) y `producerversion` | obligatorios |
| `limits` | `maxBodyBytes` (≤ 1 MiB), `rateLimit: { requests, perSeconds, burst }`, `headersTimeoutMs`, `requestTimeoutMs`, `serverTimeoutMs`, `connectionsCheckingIntervalMs` | 1 MiB · 120/60 s · 10 s · 30 s · 60 s · 1 s |
| `clients[]` | `service`, `tenantId`, `tenants`, `credentials`, `tokens`, `rateLimit`, `hooks`, `deliveries` | — |
| `credentials` | **un** método: `creds` (ruta), `nkeySeedEnv`, `credsContentEnv`, `tokenEnv` o `userEnv`+`passEnv` | — |
| `validation`, `bundleFile` | `"strict"` valida L3 contra `schemas/bundle.json` | `off` |

**Tokens.** `node src/index.mjs token` imprime un token nuevo (32 bytes aleatorios) **una vez**, y su SHA-256: el token es para el cliente, el hash para `tokens: [{ "id", "sha256" }]`.
El gateway **no guarda el token**, compara el SHA-256 en tiempo constante (`timingSafeEqual` sobre **todas** las entradas, sin salir en la primera) y un token ausente o desconocido da el mismo `401`.
Varios tokens por cliente = rotación sin corte.

## Seguridad y operación

- **Límites**: cuerpo de **1 MiB** (el del evento) rechazado **antes de leerlo** y con la conexión **cerrada** —sin cerrarla, Node lee y descarta todo lo declarado (A-152)—; **peticiones por token** con un cubo de tokens
  (por instancia: con N instancias, N veces el límite); clientes lentos cortados con `408` (ojo: los tiempos de Node solo se comprueban cada 30 s por defecto: A-153). **No limita por IP ni protege de un DDoS**: es del proxy.
- **TLS**: o **termina delante** (nginx, Caddy, un balanceador; el gateway escucha en HTTP **dentro de una red privada**) o **en el propio gateway** (`listen.tls`). Uno de los dos es obligatorio en producción: el token viaja en una cabecera.
- **Registros**: líneas JSON con una **lista blanca** de campos (cliente, ruta, estado, duración, `id`, `code`…). **No se registra nunca `data`** —de ningún evento, en particular el de un `confidential`—, ni el token, ni la firma, ni los secretos.
- **Parada ordenada** (`SIGTERM`): deja de aceptar, contesta lo que vuela con `Connection: close`, cierra las conexiones ociosas y drena NATS, con un plazo máximo (A-162).
- **El arranque falla entero** si un cliente no conecta (credenciales que el broker no acepta, TLS roto): un gateway a medias es peor que uno que no arranca.

## Docker y compose

```bash
docker build -f gateway/Dockerfile -t flux-gateway .          # la raíz del repo es el contexto (depende de sdk-node)
docker compose --profile gateway up -d --build                 # ver los export que pide docker-compose.yml
```

La imagen no lleva configuración ni secretos: se monta `gateway.json` y los secretos llegan por el entorno. Tiene `HEALTHCHECK` sobre `/healthz`.

## Pruebas

```bash
FLUX_REQUIRE_BROKER=1 NATS_SERVER=…/nats-server npm test      # ~200 pruebas; de extremo a extremo contra un NATS real con los permisos GENERADOS
FLUX_REQUIRE_BROKER=1 NATS_SERVER=…/nats-server node test/mutantes.mjs   # rompe a propósito lo importante: cada mutante DEBE hacer fallar las pruebas
```

`npm test` incluye: la aceptación (`demo.sh` con `curl`), el mismo webhook ×3 con y sin la ventana de JetStream, token malo, token de otro cliente sobre un recurso no concedido (lo deniega el broker, con la violación en el log del servidor), cuerpo
demasiado grande, firma de webhook inválida o fuera de tiempo, reintento tras un `503`, un `4xx` a la DLQ, `confidential` ausente de los registros, la parada ordenada y el ejecutable como proceso. Las medidas del
[Apéndice A](../docs/plan-v1.1/PLAN.md#apéndice-a--evidencia) (A-150 a A-166) las reproducen `test/medidas.test.mjs` y las de extremo a extremo. `test/mutantes.mjs` ejecuta 72 mutantes (la CI los reparte en tres trabajos con `--parte n/3`) (comparar el token con `===`, no verificar el HMAC, id aleatorio en el
webhook, firmar sin el instante, no renovarlo en los reintentos, un `4xx` que se reintenta, `data` en los registros, una identidad NATS compartida…).

## Lo que no hace

No es exactamente-una-vez (el receptor deduplica); no ordena; no atiende operaciones por HTTP; no hace tiempo real hacia el navegador (SSE/WebSocket); no se ha probado con carga sostenida ni con varias instancias detrás de un balanceador;
y la entrega de estado no es un CloudEvent. La lista completa y las 20 decisiones tomadas «provisionalmente», con su porqué: [`12-edge.md` §9 y §11](../specification/12-edge.md).
