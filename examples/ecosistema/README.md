# Ecosistema de ejemplo — un CRM, una tienda, facturación y una web en PHP

Un ecosistema pequeño pero **completo** que sustituye las APIs, los webhooks y el polling entre sistemas propios por flux, **bajo la configuración segura**
(permisos generados, nkeys, TLS, inbox privado; nada de `--dev`), con una prueba de extremo a extremo que lo recorre y rompe a propósito lo importante.
Es el compañero de la [guía de migración](../../docs/migracion.md): cada fragmento de código de la guía es un extracto literal de un fichero de aquí.

```
                       ┌────────────── NATS (TLS, ACLs generadas de services.json) ──────────────┐
                       │                                                                         │
  ┌─────────┐  estado  │  ┌──────────────┐   evento    ┌───────────────┐   operación   ┌──────┐  │
  │   crm   ├──────────┼─▶│  tienda-web  ├────────────▶│  facturacion  ├──────────────▶│ crm  │  │
  │ (dueño  │  evento  │  │ (observa el  │  pedidos…   │ (consume,     │ calcular-     │      │  │
  │ clientes)│ operación│  │  estado,     │             │ idempotente,  │ descuento y   └──────┘  │
  └─────────┘          │  │  publica)    │◀────────────┤ llama al crm) │ sumar-puntos            │
                       │  └──────────────┘ factura     └───────▲───────┘                         │
                       └──────────────────────────────────────┬┼─────────────────────────────────┘
                                                              ││ HTTP solo en el borde
                         ┌─────────────┐   gateway   ┌────────┴┴───┐   webhook firmado    ┌────────────┐
                         │   web-php   ├────────────▶│ flux-gateway │◀─────────────────────│ pasarela de│
                         │ (hosting    │◀────────────┤ (hospeda a   │  (un tercero)        │ pagos      │
                         │ compartido) │ entregas    │  web-php)    │                      └────────────┘
                         └─────────────┘ firmadas    └──────────────┘
```

## Arrancarlo, con un comando

```bash
(cd sdk-node && npm ci && npm run build) && (cd conformance && npm ci) && (cd cli && npm ci) && (cd gateway && npm ci) && (cd examples/ecosistema && npm ci)
NATS_SERVER=/ruta/a/nats-server node examples/ecosistema/run.mjs          # o: npm start, dentro de examples/ecosistema
```

Necesita **Node ≥ 20**, un **`nats-server` 2.14** (`GOBIN=$PWD/bin go install github.com/nats-io/nats-server/v2@v2.14.5`), `openssl` (la CA de desarrollo) y, para la parte de PHP, `php` con `curl`
(sin él se omite). **Sin Docker.** Los puertos salen de `FLUX_TEST_PORTS=desde-hasta` (por defecto 7000-7099) y todo se escribe en `.run/` (claves privadas: no se versiona).
`run.mjs` recorre el guion contando en cada paso **qué primitiva usa y por qué**, y se queda corriendo para que pruebes con `curl` y con `php`; con `--una-vez` sale al terminar.

Lo que hace por debajo, con los mismos comandos que usarás en tu proyecto:

1. `scripts/dev-secure.mjs services.json --bundle schemas/bundle.json` → una CA, **una nkey por servicio** y las **ACLs que genera `gen-acl.mjs`** del manifiesto.
2. `nats-server -c …` con **TLS obligatorio**.
3. `flux provision` con las credenciales de **operación**: los streams y los durables. **Ningún servicio puede crearlos** (no hay `--dev`).
4. El **gateway** (`gateway/`) con `gateway.json` (sin secretos: nombres de variables y el SHA-256 de un token nuevo por ejecución) y **PHP** (`php -S`) como un hosting compartido.
5. **Un proceso por servicio**, cada uno con **solo su semilla**: el `crm` no tiene la clave de `facturacion`.

## Qué primitiva se usa en cada punto, y por qué

| Dónde | Qué ocurre | Primitiva | Por qué, y qué sustituye |
|---|---|---|---|
| `crm` → `tienda-web`, `web-php` | El valor actual de cada cliente (con su nombre y su correo) | **Estado** `clientes.cliente.v1` (`state.put` / `state.watch`) | Es *un dato que guarda otro sistema*. Sustituye a `GET /clientes/123`, a la carga inicial paginada y al polling. Es el **único** sitio donde viaja el dato personal: clasificado `confidential`, con `x-pii`, **borrable** (la baja de `cli-2` deja una lápida y la tienda borra su copia). |
| `crm` | «Hay un cliente nuevo» | **Evento** `clientes.cliente.v1.registrado` | Avisa de un hecho. Lleva solo `clienteId` (regla 5: sin PII en eventos). Sustituye al webhook de alta. |
| `tienda-web` | Un cliente compra | **Evento** `pedidos.pedido.v1.creado` | Es un hecho, con el cliente **por referencia**. Quien pide (la tienda) no espera: facturación publica otro evento al terminar y el SDK los une por `correlationid` (fila 7 del mapa). |
| `facturacion` | Emite la factura | **Idempotencia** (estrategia A) | El broker entrega *al menos una vez*: la factura tiene un número correlativo, el efecto que no se puede repetir. La marca «evento procesado» va **en la misma transacción** que la factura, y sobrevive a un `kill -9`. |
| `facturacion` → `crm` | El descuento de un pedido | **Petición/respuesta** `calcular-descuento` | Es un **cálculo** (las reglas viven en el CRM), no un dato guardado. Sustituye a `GET /descuento`. Si el CRM no está, `RPC_UNAVAILABLE` llega en ~1 ms y la entrega se reintenta. |
| `facturacion` → `crm` | Sumar puntos de fidelidad | **Petición/respuesta** `sumar-puntos` (una **orden**) | Cambia datos y necesita la respuesta ya. No es idempotente por naturaleza (`puntos + n`), así que lleva una **clave de negocio** (`referencia`) y el CRM deduplica por ella: un timeout es ambiguo y una reentrega construye una petición nueva con otro `id`. |
| `facturacion` | Un cobro llega **antes** que su factura | **Sin asumir orden** | El cobro se guarda (estrategia B: `INSERT … ON CONFLICT DO NOTHING`) y la factura nace ya pagada. |
| `facturacion` → `tienda-web`, `web-php` | «La factura está lista» | **Evento** `facturacion.factura.v1.emitida` | Sale de una **bandeja de salida** escrita en la misma transacción que la factura: «guardar y publicar» son dos escrituras sin transacción entre ellas. |
| pasarela de pagos → `facturacion` | El webhook de un **tercero**, enviado tres veces | **Gateway**: HTTP → evento | El gateway verifica la firma, saca un **`id` determinista** del evento del proveedor (el mismo aviso, el mismo `id`), publica solo los campos de `data.pick` (el correo del pagador **no** pasa) y el consumidor aplica **un** efecto. |
| `web-php` | Lee un cliente y calcula un descuento | **Gateway**: HTTP ↔ flux | Un hosting compartido no puede mantener una conexión NATS. Habla **solo** HTTP con el gateway, que le da los permisos de su entrada en el manifiesto, **ni uno más**. |
| `web-php` | Recibe «factura emitida» | **Gateway**: evento → HTTP firmado | Sustituye a un webhook propio. `recibir.php` verifica la firma sobre los bytes en bruto y deduplica por `Idempotency-Key`. |

## Los ficheros

| | |
|---|---|
| [`services.json`](services.json) | El manifiesto: quién es dueño de qué y qué toca cada servicio. **De aquí salen todos los permisos.** |
| [`schemas/`](schemas/) | Los contratos (con su `$id`, `x-pii` donde toca, dinero en enteros) y su `bundle.json`. Se crean con `flux new` y los valida L3 estricta al publicar. |
| [`servicios/`](servicios/) | `crm.mjs`, `tienda-web.mjs` y `facturacion.mjs`: un proceso cada uno, configurado **por el entorno** (`NATS_URL`, `NKEY_SEED_FILE`…). `comun.mjs` conecta; `almacen.mjs` es una «base de datos» de juguete (un JSON con escritura atómica). |
| [`gateway.json`](gateway.json) | La configuración del gateway para `web-php`: un webhook, una entrega, sin secretos. |
| [`web-php/`](web-php/) | `flux.php` (cliente mínimo del gateway), `ficha.php`, `recibir.php` y los equivalentes con `curl` (`ficha-curl.sh`, `webhook-pasarela.sh`). |
| [`antes-http/`](antes-http/) | El **antes**: las mismas integraciones con HTTP, que funcionan y se prueban (la guía los cita). |
| [`lib/entorno.mjs`](lib/entorno.mjs), [`run.mjs`](run.mjs) | Levantan todo; `run.mjs` es el guion. |

## La prueba de extremo a extremo

```bash
NATS_SERVER=… FLUX_REQUIRE_BROKER=1 FLUX_REQUIRE_PHP=1 FLUX_TEST_PORTS=7200-7299 npm test       # en examples/ecosistema
NATS_SERVER=… FLUX_REQUIRE_BROKER=1 FLUX_REQUIRE_PHP=1 FLUX_TEST_PORTS=7200-7299 npm run mutantes
```

`test/ecosistema.test.mjs` levanta **todo lo anterior de verdad** (sin mocks) y comprueba, en orden: la foto inicial sin llamar a nadie · un cambio y una baja que llegan solos · un pedido → una factura con el descuento del CRM y el `correlationid` que las une ·
**la misma entrega dos veces —y tras un `kill -9`— → un efecto** · una orden repetida → un efecto · un webhook de tercero ×3 → un cobro (y uno que llega antes que su factura) · PHP hablando solo con el gateway · las **negativas de seguridad**
(un servicio con credenciales válidas **no puede** leer, escribir ni llamar lo que su manifiesto no le concede; el inbox de otro, tampoco; un token válido sobre lo no concedido lo deniega el **broker**; una firma mala no publica nada) ·
ni un dato personal en ningún stream salvo el de estado, y la DLQ vacía · y recuperar una réplica perdida con `flux state resync`. `test/guia.test.mjs` comprueba que cada fragmento de la guía es un extracto literal de un fichero real y que el «antes» en HTTP funciona.

`test/mutantes.mjs` **estropea una cosa a la vez** y exige que la prueba falle por la razón correcta: quitar la idempotencia de facturación (o dejar su marca solo en memoria, que un reinicio pierde) · quitar la clave de negocio de la orden ·
ignorar la lápida o la versión en la réplica · **conceder de más a un servicio en el manifiesto** (que la tienda consuma los cobros o llame al CRM, que facturación observe los clientes, que `web-php` sume puntos) · un receptor PHP sin firma o sin deduplicar ·
un webhook sin `id` determinista o sin `data.pick`. Una prueba que pasa con el código roto no vale.

## Lo que este ejemplo NO es (honestidad)

- **El «almacén» y los comandos por stdin son de juguete.** `almacen.mjs` sustituye a la base de datos de cada servicio (lo que importa de ella es el *todo o nada*) y los comandos JSON por la entrada estándar, a la interfaz de cada aplicación (el formulario del CRM, el botón «comprar»). En tu sistema son tu código.
- **Un solo tenant (`acme`) y una sola instancia por servicio.** El reparto entre instancias de una operación y el aislamiento entre tenants están medidos en la suite de seguridad, no aquí.
- **TLS en el broker, no en el gateway ni en PHP**: el gateway y `php -S` escuchan en `127.0.0.1` por HTTP. En producción, delante un proxy que termine TLS (el token viaja en una cabecera).
- **Un solo nodo de NATS, sin réplicas.** Producción: `flux provision --replicas 3`.
- **Docker.** No hace falta, y este ejemplo no trae su propio `docker-compose.yml`. Si prefieres contenedores: el broker seguro es el perfil `secure` de [`docker-compose.yml`](../../docker-compose.yml) (`node scripts/dev-secure.mjs …` genera lo que monta), el gateway tiene
  su imagen y su perfil `gateway`, y cada servicio es un `node servicios/<nombre>.mjs` con las variables de entorno de arriba. **Esa ruta no la ejecuta el CI de este ejemplo.**
- **Solo Node.** Un servicio en otro lenguaje es hoy un cliente de borde (`"via": "gateway"`) hasta que el SDK de ese lenguaje tenga estado, petición/respuesta y la configuración segura (Fase 6 del plan).

## Lo que cuesta adoptarlo (los pasos reales)

Lo que descubrió escribir este ejemplo, sin maquillar (el detalle, en el Apéndice A del [plan](../../docs/plan-v1.1/PLAN.md), A-180 a A-189):

1. **Ni el SDK ni la CLI están publicados**: `@flux/sdk` es `file:../../sdk-node` (hay que compilarlo) y `flux` es `node cli/src/index.mjs`.
2. **Hace falta un `nats-server`** (un binario) y `openssl` para la CA de desarrollo. Con Docker, la imagen `nats:2.14`.
3. `flux provision` no tiene `--ca`: con TLS de CA propia, `NODE_EXTRA_CA_CERTS=ca.pem`.
4. `dev-secure.mjs` **ignoraba el manifiesto que se le daba** y solo servía para el ecosistema de este repositorio; ahora acepta `services.json` y `--bundle`.
5. `bundle-schemas.mjs` y `check-compat.mjs` solo trabajaban sobre el `schemas/` del repositorio; ahora aceptan `--schemas <dir>`.
6. Un proyecto nuevo no puede generar su bundle con estado u operaciones sin copiar a mano los esquemas de plataforma (`system/state/eliminado`, `system/rpc/error`): `flux new` los copia.
7. Un outbox que publica **fuera** de un handler (al reiniciar, o desde otro evento) pierde el contexto causal: medido, el aviso nace como raíz de una cadena nueva (su `correlationid` es su propio `id` y no hay `causationid`), y el SDK no exporta cómo restaurar el contexto del evento que lo provocó.
8. **Cada servicio escribe su idempotencia** y su bandeja de salida: es código de negocio, no del SDK.
