# Migrar de APIs y webhooks a flux

> **Para quién.** Un desarrollador con varios webs y servicios propios —un CRM enlazado con varias webs, una tienda, una facturación— que hoy
> se hablan con **APIs, webhooks y polling**, y quiere pasar a flux **sin perder seguridad ni dejar huecos**.
>
> **Qué hay detrás de cada línea de código de esta guía.** Ninguna está escrita a mano: cada bloque es un **extracto literal** de un fichero que
> se ejecuta —el ecosistema de [`examples/ecosistema/`](../examples/ecosistema/) con su prueba de extremo a extremo, y las pruebas del «antes» en
> HTTP— y `node scripts/check-migracion.mjs` lo comprueba en CI. Si cambias el código, la guía se regenera (`--rellenar`); si no coinciden, el CI falla.
>
> **Qué NO hay aquí: cifras de rendimiento.** Están en [`bench/RESULTS.md`](../bench/RESULTS.md), que genera una medición reproducible; esta guía no las repite.

## 1. Qué vas a hacer, en pasos reales

El camino completo, tal como lo recorre el ejemplo (cada paso es un comando que ejecuta [`run.mjs`](../examples/ecosistema/run.mjs) o su test):

1. **Instalar lo que hace falta.** Node ≥ 20, un `nats-server` 2.14 (un binario: `go install github.com/nats-io/nats-server/v2@v2.14.5`, o la imagen `nats:2.14`) y `openssl`.
   El SDK de Node y la CLI **aún no están publicados en ningún registro**: son del repositorio (`@flux/sdk` es una dependencia `file:../sdk-node`, que hay que compilar,
   y la CLI es `node cli/src/index.mjs`). Es el coste de adopción más molesto y es real.
2. **Describir tus contratos.** `flux new event|state|rpc <nombre>` crea cada esquema con su `$id` y lo obligatorio, y regenera el bundle (§2).
3. **Escribir el manifiesto** `services.json`: quién es dueño de qué y qué toca cada servicio. De él salen **todos los permisos**, sin escribir uno a mano.
4. **Generar la configuración segura**: una nkey por servicio, TLS con CA propia y las ACLs de NATS.
5. **Aprovisionar** los streams y los durables (`flux provision`): ningún servicio puede crearlos.
6. **Conectar cada servicio** con su semilla, la CA y el bundle de esquemas, y escribir lo que antes eran endpoints: `publish`, `subscribe`, `state.put`, `state.watch`, `respond`, `request`.
7. **Poner el gateway** delante de lo que no pueda mantener una conexión NATS (un PHP de hosting compartido, un `cron`, el webhook de un tercero).
8. **Migrar de forma gradual** (§4) y **apagar** lo viejo cuando nadie lo use.

<!-- sin-fuente: los pasos 3 a 5 son exactamente los que ejecuta examples/ecosistema/lib/entorno.mjs en cada prueba -->
```bash
node scripts/dev-secure.mjs services.json --bundle schemas/bundle.json --out .dev-secure --port 4223    # CA, una nkey por servicio, ACLs generadas
nats-server -c .dev-secure/nats.conf
NODE_EXTRA_CA_CERTS=.dev-secure/ca.pem node cli/src/index.mjs provision services.json --bundle schemas/bundle.json \
    --server tls://127.0.0.1:4223 --creds .dev-secure/ops.creds
```

**Ver el ecosistema funcionando antes de tocar tu código** (un comando, sin Docker):

<!-- sin-fuente: el comando de arranque del ejemplo, que ejecuta el CI con --una-vez -->
```bash
(cd sdk-node && npm ci && npm run build) && (cd conformance && npm ci) && (cd cli && npm ci) && (cd gateway && npm ci) && (cd examples/ecosistema && npm ci)
NATS_SERVER=/ruta/a/nats-server node examples/ecosistema/run.mjs
```

**Qué lenguaje.** Hoy solo los SDKs de **Node y Rust** tienen estado, petición/respuesta y la configuración segura (columna *Capacidades* del
[`README.md`](../README.md)). Un servicio en PHP, Python, Go, Java o .NET **no es compatible con la configuración segura** de forma nativa: tiene que ser un **cliente
de borde** (`"via": "gateway"`, §3 filas 8 a 10), con los mismos permisos que cualquier servicio. La paridad de los otros cinco SDKs es la Fase 6 del plan.

## 2. Qué primitiva uso

```text
¿Quien habla puede mantener una conexión NATS (un servicio propio, con SDK de Node o Rust)?
├─ No (un PHP de hosting compartido, un cron, serverless, un tercero) ─────────────→  GATEWAY: HTTP en el borde, y dentro nada lo usa
└─ Sí
   ├─ ¿Avisas de que algo PASÓ, y cada interesado reacciona a su ritmo? ──────────→  EVENTO     (publish / subscribe)
   ├─ ¿Es un DATO que guarda otro sistema, y quieres leerlo o tener una copia? ───→  ESTADO     (state.watch / state.get)
   ├─ ¿Es una ORDEN o un CÁLCULO y necesitas la respuesta ya? ────────────────────→  PETICIÓN   (request / respond)
   └─ ¿Es trabajo que no tiene que esperarse? ───────────────────────────────────→  EVENTO del que pide + EVENTO de resultado, unidos por correlationid
```

**La regla que evita recrear el problema.** Si lo que pides es *un dato que otro sistema guarda*, es **estado**, no una petición. Una operación
«`obtener-cliente`» que devuelve lo que el CRM guarda es volver a tener una API con otro transporte. La petición es para órdenes y cálculos.

**Cómo se crean los contratos.** Con `flux new`, que rechaza lo que `protocol.json` rechaza y dice cómo arreglarlo:

<!-- sin-fuente: los ejecuta cli/test/new.test.mjs, con y sin broker -->
```bash
node cli/src/index.mjs new event clientes.cliente.v1.registrado                    # un hecho que pasó, en pasado
node cli/src/index.mjs new state clientes.cliente.v1                               # el valor actual (lleva aggregateVersion obligatoria)
node cli/src/index.mjs new rpc   clientes.cliente.v1.calcular-descuento            # una operación en infinitivo: el PAR request/reply
```

## 3. El mapa, fila a fila

Las filas son las del [mapa de sustitución](plan-v1.1/PLAN.md#2-mapa-de-sustitución). En cada una, el **antes** es código HTTP que funciona
([`antes-http/`](../examples/ecosistema/antes-http/), con sus pruebas) y el **después** es el del ecosistema de ejemplo.

### Fila 1 — Webhook: «te aviso de que pasó algo»  →  **evento**

**Antes.** El CRM avisa con un `POST` a cada sistema suscrito. Un receptor caído pierde el aviso, cada suscriptor nuevo es otra URL, y el cuerpo lleva el correo del cliente.

<!-- fuente: examples/ecosistema/antes-http/http.mjs#antes-fila-1 -->
```js
// Webhook: el CRM avisa a cada sistema suscrito con un POST. Si un receptor está caído, el aviso se pierde (o hay que escribir los reintentos),
// y cada alta nueva obliga a dar de alta una URL más. Nótese el correo del cliente en el cuerpo: un dato personal en un mensaje que viaja.
export async function avisarAltaDeCliente(suscriptores, cliente) {
  await Promise.all(suscriptores.map((url) => fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ evento: "cliente.registrado", clienteId: cliente.clienteId, email: cliente.email }),
  })));
}
```

**Después.** El CRM publica **un** evento con la referencia (`clienteId`), no el correo. Quien lo necesite se suscribe; el CRM no conoce a nadie. El broker reintenta, y lo que no se pueda entregar va a la DLQ.

<!-- fuente: examples/ecosistema/servicios/crm.mjs#crm-evento -->
```js
await bus.publish("clientes.cliente.v1.registrado", { clienteId, aggregateVersion: alta.aggregateVersion, plan }, { aggregateId: clienteId });
```

Y el consumidor —`facturacion` abre una cuenta de facturación— es **idempotente** sin necesitar una tabla: `??=` es «insertar si no existe» (estrategia B de [`AGENTS.md`](../AGENTS.md) §4):

<!-- fuente: examples/ecosistema/servicios/facturacion.mjs#fact-alta -->
```js
await bus.subscribe("clientes.cliente.v1.registrado", async (evento) => {
  db.transaccion((d) => { d.cuentas[evento.data.clienteId] ??= { abierta: evento.time }; });
  registro({ evento: "cuenta-abierta", clienteId: evento.data.clienteId });
});
```

### Fila 2 — Polling: `GET /cambios?desde=`  →  **evento** (o estado)

**Antes.** Cada cliente pregunta cada T segundos, haya cambiado algo o no; el servidor guarda un registro de cambios con números de secuencia.

<!-- fuente: examples/ecosistema/antes-http/http.mjs#antes-fila-2 -->
```js
// Polling, el servidor: el CRM guarda cada cambio con un número de secuencia y lo sirve a quien pregunte (`GET /cambios?desde=`).
export const cambios = [];
export function servirCambios(req, res) {
  const desde = Number(new URL(req.url, "http://crm").searchParams.get("desde") ?? 0);
  res.setHeader("content-type", "application/json");
  res.end(JSON.stringify(cambios.filter((c) => c.seq > desde)));
}
// Polling, el cliente: pregunta cada T milisegundos, haya cambiado algo o no, y por cada cliente que lo haga.
export function sondear(url, cadaMs, alCambiar) {
  let desde = 0;
  return setInterval(async () => {
    for (const c of await (await fetch(`${url}/cambios?desde=${desde}`)).json()) {
      desde = c.seq;
      alCambiar(c);
    }
  }, cadaMs);
}
```

**Después.** Nadie pregunta. Quien quiere enterarse de un hecho se suscribe; el handler aplica el aviso con una operación **naturalmente idempotente** (la clave es el pedido y solo avanza la versión), de modo que ni una entrega repetida ni una desordenada estropean nada:

<!-- fuente: examples/ecosistema/servicios/tienda-web.mjs#tienda-aviso -->
```js
await bus.subscribe("facturacion.factura.v1.emitida", async (evento) => {
  registro({ evento: `factura-${aplicarFactura(db, evento.data)}`, pedidoId: evento.data.pedidoId, eventoId: evento.id });
});
```

Si lo que se sondeaba era *un dato* y no un hecho (`GET /clientes?modificados_desde=`), es la fila 3 o la 4.

### Fila 3 — `GET /clientes/123`: leer un dato de otro sistema  →  **estado**

**Antes.** Una petición por cada lectura. Si el CRM cae, la tienda cae.

<!-- fuente: examples/ecosistema/antes-http/http.mjs#antes-fila-3 -->
```js
// `GET /clientes/123`: leer un dato que guarda otro sistema. Si el CRM está caído, la tienda también lo está; cada visita es una petición.
export async function leerCliente(base, clienteId) {
  const r = await fetch(`${base}/clientes/${clienteId}`);
  if (r.status === 404) return null;
  if (!r.ok) throw new Error(`el CRM respondió ${r.status}`);
  return r.json();
}
```

**Después.** El dueño publica el valor **actual** (`state.put`) y el que lo necesita lo lee del stream (`state.get`), sin pasar por el proceso del CRM:

<!-- fuente: examples/ecosistema/servicios/tienda-web.mjs#tienda-get -->
```js
const entrada = await bus.state.get("clientes.cliente.v1", clienteId);
```

Para lo que se lee **de continuo**, no hagas una lectura por visita: mantén una copia local (fila 4). El dueño escribe así —`put` es condicional: una versión que no avanza da `stale`, que **no** es un error—:

<!-- fuente: examples/ecosistema/servicios/crm.mjs#crm-publicar-estado -->
```js
const publicarEstado = async (c) => {
  try {
    if (c.baja) return await bus.state.delete(CONTRATO, c.clienteId, { aggregateVersion: c.aggregateVersion });
    const { clienteId, nombre, email, plan, puntos, aggregateVersion } = c;
    return await bus.state.put(CONTRATO, clienteId, { clienteId, nombre, email, plan, puntos, aggregateVersion });
  } catch (e) {
    // La base de datos ya tiene el cambio y el broker no lo ha recibido: quien llama (o el arranque) debe volver a intentarlo.
    throw new RetryableError("el cambio está guardado pero no se pudo publicar su estado: reintenta", { code: "ESTADO_NO_PUBLICADO", cause: e });
  }
};
```

El estado es el **único** sitio donde viaja el dato personal: va clasificado `confidential`, con `x-pii` en el esquema, y se borra con `state.delete` (el valor sale del stream; un observador **DEBE** borrar su copia al recibir la lápida).

### Fila 4 — Carga inicial: `GET /clientes?page=`  →  **estado**

**Antes.** Paginar hasta el final, y escribir a mano cómo se pasa de «cargado» a «al día» sin perder los cambios que llegan mientras tanto.

<!-- fuente: examples/ecosistema/antes-http/http.mjs#antes-fila-4 -->
```js
// La carga inicial: `GET /clientes?page=` hasta que se acaben, y a escribir a mano cómo se pasa de «cargado» a «al día».
export async function cargarTodos(base) {
  const todos = [];
  for (let pagina = 1; ; pagina++) {
    const lote = await (await fetch(`${base}/clientes?page=${pagina}`)).json();
    if (lote.length === 0) return todos;
    todos.push(...lote);
  }
}
```

**Después.** `state.watch` entrega **la foto** de todo el contrato y **después los cambios**, por el mismo canal. `onSynced` avisa una vez de que la foto terminó. La copia local se mantiene con `WHERE aggregate_version < $n`
(`aplicarEntrada`, en el mismo fichero: la prueba la ejercita con versiones repetidas, desordenadas y con lápidas):

<!-- fuente: examples/ecosistema/servicios/tienda-web.mjs#tienda-watch -->
```js
await bus.state.watch(
  "clientes.cliente.v1",
  async (entrada) => {
    registro({ evento: `cliente-${aplicarEntrada(db, entrada)}`, clienteId: entrada.key, aggregateVersion: entrada.aggregateVersion });
  },
  {
    tenantId: cfg.tenant,
    // Una vez, al terminar la foto inicial: a partir de aquí la réplica local ya puede contestar.
    onSynced: () => registro({ evento: "foto-inicial", clientes: Object.keys(db.leer().clientes).length }),
  },
);
```

Si una copia se pierde (un disco, una restauración), **no hace falta paginar nada**: `flux state resync <servicio> <contrato> --tenant <t> --confirm` y la foto vuelve (lo prueba el paso 10 del test del ejemplo).

### Fila 5 — `POST /accion` que necesita respuesta ya  →  **petición/respuesta**

**Antes.** Un `POST` con su `Idempotency-Key` —un convenio de esa API— y los códigos de estado que cada API interpreta a su manera.

<!-- fuente: examples/ecosistema/antes-http/http.mjs#antes-fila-5 -->
```js
// `POST /accion` que necesita respuesta ya: una orden. La `Idempotency-Key` es un convenio de esta API (otra, no la tendría), y nadie
// obliga al servidor a respetarla.
export async function sumarPuntos(base, clienteId, puntos, referencia) {
  const r = await fetch(`${base}/clientes/${clienteId}/puntos`, {
    method: "POST",
    headers: { "content-type": "application/json", "idempotency-key": referencia },
    body: JSON.stringify({ puntos }),
  });
  if (!r.ok) throw new Error(`el CRM respondió ${r.status}`);
  return r.json();
}
```

**Después.** La operación se llama por **nombre**, no por dirección. Una **orden** que no es idempotente por naturaleza (`puntos = puntos + n`) se defiende de su propio reintento: un timeout es **ambiguo** (la orden pudo ejecutarse),
y una reentrega del evento que la provoca construye una petición **nueva** con otro `id`. Por eso lleva una **clave de negocio** (`referencia`) y quien la atiende deduplica por ella:

<!-- fuente: examples/ecosistema/servicios/crm.mjs#crm-orden -->
```js
await bus.respond("clientes.cliente.v1.sumar-puntos", async (peticion, ctx) => {
  const previa = db.leer().puntosAplicados[peticion.referencia];
  if (previa) {
    registro({ evento: "orden-repetida", operacion: ctx.operation, peticionId: ctx.id });
    await publicarEstado(clienteDe(previa.clienteId)); // por si la primera vez se guardó pero no llegó a publicarse
    return previa.respuesta;
  }
  const c = clienteDe(peticion.clienteId);
  if (!c || c.baja) throw new PermanentError("el cliente no existe", { code: "CLIENTE_DESCONOCIDO" });
  const respuesta = db.transaccion((d) => {
    const cliente = d.clientes[peticion.clienteId];
    cliente.puntos += peticion.puntos;
    cliente.aggregateVersion += 1;
    const r = { clienteId: cliente.clienteId, puntos: cliente.puntos, aggregateVersion: cliente.aggregateVersion };
    d.puntosAplicados[peticion.referencia] = { clienteId: cliente.clienteId, respuesta: r };
    return r;
  });
  registro({ evento: "puntos-sumados", clienteId: peticion.clienteId, peticionId: ctx.id });
  await publicarEstado(clienteDe(peticion.clienteId));
  return respuesta;
});
```

### Fila 6 — `GET /precio?…`: un cálculo, no un dato guardado  →  **petición/respuesta**

**Antes.**

<!-- fuente: examples/ecosistema/antes-http/http.mjs#antes-fila-6 -->
```js
// `GET /descuento?…`: un cálculo, no un dato guardado.
export async function calcularDescuento(base, clienteId, totalCents) {
  const r = await fetch(`${base}/descuento?clienteId=${encodeURIComponent(clienteId)}&totalCents=${totalCents}`);
  if (!r.ok) throw new Error(`el CRM respondió ${r.status}`);
  return r.json();
}
```

**Después.** El responder (el dueño del dominio) y quien llama; la clasificación del error viaja sola (`RPC_UNAVAILABLE` en ~1 ms si nadie escucha, `CLIENTE_DESCONOCIDO` permanente si el cliente no existe):

<!-- fuente: examples/ecosistema/servicios/crm.mjs#crm-calculo -->
```js
await bus.respond("clientes.cliente.v1.calcular-descuento", async (peticion) => {
  const c = clienteDe(peticion.clienteId);
  if (!c || c.baja) throw new PermanentError("el cliente no existe", { code: "CLIENTE_DESCONOCIDO" });
  const pct = DESCUENTO_POR_PLAN[c.plan] ?? 0;
  // Enteros: nunca un float para dinero.
  return { descuentoCents: Math.floor((peticion.totalCents * pct) / 100), moneda: peticion.moneda, plan: c.plan };
});
```

<!-- fuente: examples/ecosistema/servicios/facturacion.mjs#fact-llamar -->
```js
const { descuentoCents } = await bus.request("clientes.cliente.v1.calcular-descuento", {
  clienteId: pedido.clienteId,
  totalCents: pedido.totalCents,
  moneda: pedido.moneda,
});
const totalCents = pedido.totalCents - descuentoCents;
const puntos = Math.floor(totalCents / 100);
// Una ORDEN: va con la clave de negocio. Si este handler muere después de esta línea, la reentrega la repite y el CRM no suma dos veces.
if (puntos > 0) await bus.request("clientes.cliente.v1.sumar-puntos", { clienteId: pedido.clienteId, puntos, referencia: pedido.pedidoId });
```

### Fila 7 — `POST /accion` asíncrona (encolar trabajo)  →  **evento + evento de resultado**

**Antes.** `202` con una dirección donde mirar, y a preguntar hasta que cambie (o un webhook de resultado, que es la fila 1). Nada une la petición con su resultado salvo la URL.

<!-- fuente: examples/ecosistema/antes-http/http.mjs#antes-fila-7 -->
```js
// `POST /facturas` asíncrono: responde 202 con una dirección donde mirar, y quien pidió pregunta hasta que el estado cambie (o monta un
// webhook de resultado, que es la fila 1 otra vez). Nada une la petición con su resultado salvo la URL.
export async function facturarYEsperar(base, pedido, { cadaMs = 200, intentos = 50 } = {}) {
  const r = await fetch(`${base}/facturas`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(pedido) });
  if (r.status !== 202) throw new Error(`facturación respondió ${r.status}`);
  const { location } = await r.json();
  for (let i = 0; i < intentos; i++) {
    const f = await (await fetch(`${base}${location}`)).json();
    if (f.estado === "lista") return f;
    await new Promise((ok) => setTimeout(ok, cadaMs));
  }
  throw new Error("la factura no estuvo lista a tiempo");
}
```

**Después.** No hay un «encolar»: **quien pide publica un evento** (`pedidos.pedido.v1.creado`) y **quien trabaja publica otro** al terminar (`facturacion.factura.v1.emitida`). El SDK los une solo: el segundo hereda el `correlationid` del primero y su
`causationid` es el `id` del pedido (lo comprueba el paso 3 del test). Quien pidió **no espera**: cuando la factura está, se entera.

<!-- fuente: examples/ecosistema/servicios/tienda-web.mjs#tienda-pedido -->
```js
const evento = await bus.publish(
  "pedidos.pedido.v1.creado",
  { pedidoId, clienteId, aggregateVersion: 1, totalCents, moneda: "EUR", lineas },
  { aggregateId: pedidoId },
);
```

El trabajador es **idempotente**, y esto es lo más importante de toda la guía. El broker entrega *al menos una vez*: la segunda entrega del mismo `id` llega (una caída, un replay de la DLQ, un productor que reintenta fuera de `duplicate_window`). La factura tiene un número correlativo, el efecto que
no se puede repetir; así que la marca «este evento ya está procesado» se guarda **en la misma transacción** que la factura (estrategia A de `AGENTS.md` §4):

<!-- fuente: examples/ecosistema/servicios/facturacion.mjs#fact-idempotencia -->
```js
if (db.leer().procesados[evento.id]) {
  registro({ evento: "duplicado-ignorado", eventoId: evento.id, entrega: ctx.attempt });
  await drenar();
  return ctx.ack();
}
```

<!-- fuente: examples/ecosistema/servicios/facturacion.mjs#fact-transaccion -->
```js
const factura = db.transaccion((d) => {
  const numero = d.siguienteNumero++;
  const facturaId = `fac-${numero}`;
  const f = {
    facturaId, numero, pedidoId: pedido.pedidoId, clienteId: pedido.clienteId,
    subtotalCents: pedido.totalCents, descuentoCents, totalCents, moneda: pedido.moneda,
    estado: Object.values(d.cobros).some((c) => c.pedidoId === pedido.pedidoId) ? "pagada" : "emitida", // un cobro que llegó ANTES
  };
  d.facturas.push(f);
  d.procesados[evento.id] = new Date().toISOString();
  d.salida.push({
    clave: facturaId, subject: "facturacion.factura.v1.emitida", aggregateId: facturaId,
    data: {
      facturaId, pedidoId: f.pedidoId, clienteId: f.clienteId, aggregateVersion: 1, numero,
      subtotalCents: f.subtotalCents, descuentoCents, totalCents, moneda: f.moneda,
    },
  });
  return f;
});
```

Dos detalles que el test del ejemplo comprueba y que un handler ingenuo no tiene: el aviso de «factura emitida» va por una **bandeja de salida** (`salida`, escrita en esa misma transacción), porque «guardar y publicar» son dos escrituras sin transacción entre ellas; y la marca sobrevive a un `kill -9`
(la prueba mata el proceso y vuelve a entregar el mismo pedido). **No uses `duplicate_window` de JetStream para esto**: deduplica publicaciones, no reentregas de consumo.

### Fila 8 — Webhook entrante de un tercero (la pasarela de pagos)  →  **gateway: HTTP → evento**

**Antes.** Un endpoint propio que verifica la firma sobre los bytes en bruto, compara en tiempo constante, mira la tolerancia y deduplica: cuatro cosas que cada receptor escribe —y puede equivocar—.

<!-- fuente: examples/ecosistema/antes-http/http.mjs#antes-fila-8 -->
```js
// Webhook entrante de un tercero (una pasarela de pagos): hay que verificar la firma sobre los bytes en bruto, comparar en tiempo constante,
// mirar la tolerancia y deduplicar por el id del evento: cuatro cosas que cada receptor escribe —y puede equivocar— por su cuenta.
export function recibirCobro({ secreto, yaVistos, guardar }) {
  return (req, res) => {
    const trozos = [];
    req.on("data", (t) => trozos.push(t));
    req.on("end", () => {
      const cuerpo = Buffer.concat(trozos);
      const [t, v1] = String(req.headers["x-pasarela-firma"] ?? "").split(",").map((p) => p.split("=")[1] ?? "");
      const esperado = createHmac("sha256", secreto).update(`${t}.`).update(cuerpo).digest("hex");
      const igual = v1.length === esperado.length && timingSafeEqual(Buffer.from(v1), Buffer.from(esperado));
      if (!igual || Math.abs(Date.now() / 1000 - Number(t)) > 300) { res.statusCode = 401; return res.end(); }
      const cobro = JSON.parse(cuerpo.toString("utf8"));
      if (!yaVistos.has(cobro.id)) { yaVistos.add(cobro.id); guardar(cobro); }
      res.statusCode = 200;
      res.end();
    });
  };
}
```

**Después.** Ese código desaparece. Se **declara** el webhook en la configuración del gateway: la firma, de dónde sale el `id` del hecho y su instante, y **qué campos pasan**. El cuerpo del tercero trae el correo del pagador y **no** pasa (`data.pick`, regla 5):

<!-- fuente-json: examples/ecosistema/gateway.json#/clients/0/hooks/pasarela -->
```json
{
  "subject": "pagos.cobro.v1.recibido",
  "verify": {
    "scheme": "hmac-sha256",
    "secretEnv": "PASARELA_HOOK_SECRETO",
    "signature": {
      "header": "X-Pasarela-Firma",
      "format": "t-v1"
    },
    "toleranceSeconds": 300
  },
  "externalKey": {
    "from": "body",
    "pointer": "/id"
  },
  "time": {
    "from": "body",
    "pointer": "/created",
    "unit": "seconds"
  },
  "aggregateId": {
    "from": "body",
    "pointer": "/id"
  },
  "classification": "internal",
  "data": {
    "pick": {
      "pagoId": "/id",
      "pedidoId": "/metadata/pedidoId",
      "importeCents": "/amount",
      "moneda": "/currency"
    }
  }
}
```

El mismo aviso enviado tres veces produce **el mismo evento** (el `id` es determinista: UUIDv7 con los milisegundos del hecho y 74 bits de `SHA-256(source | clave externa)`) y un solo efecto en el consumidor. La pasarela —o tú, para probarlo— lo envía así:

<!-- fuente: examples/ecosistema/web-php/webhook-pasarela.sh#curl-webhook -->
```bash
cuerpo="{\"id\":\"$evento\",\"created\":$(date +%s),\"amount\":9000,\"currency\":\"EUR\",\"metadata\":{\"pedidoId\":\"$pedido\"},\"customer\":{\"email\":\"pagador@example.com\"}}"
t="$(date +%s)"
firma="$(printf '%s.%s' "$t" "$cuerpo" | openssl dgst -sha256 -hmac "$PASARELA_SECRETO" -hex | awk '{print $NF}')"
curl -sS --fail-with-body -X POST "$FLUX_GATEWAY_URL/v1/hooks/pasarela" \
  -H 'Content-Type: application/json' -H "X-Pasarela-Firma: t=$t,v1=$firma" -d "$cuerpo"
```

Un proveedor que **no** da el instante del hecho no tiene `id` determinista: declara `"idempotent": false` y deduplica por una clave de negocio en `data`. El consumidor (fila 7) deduplica igualmente.

### Fila 9 — Webhook saliente a quien no puede conectarse  →  **gateway: evento → HTTP firmado**

**Antes.** El bucle de reintentos, el backoff y qué hacer con lo que no se pudo entregar son tuyos.

<!-- fuente: examples/ecosistema/antes-http/http.mjs#antes-fila-9 -->
```js
// Webhook saliente a quien no puede conectarse: el bucle de reintentos, el backoff y qué hacer con lo que no se pudo entregar es tuyo.
export async function entregarConReintentos(url, evento, { intentos = 3, esperaMs = 100 } = {}) {
  for (let i = 1; ; i++) {
    let estado;
    try {
      const r = await fetch(url, { method: "POST", headers: { "content-type": "application/json", "idempotency-key": evento.id }, body: JSON.stringify(evento) });
      if (r.ok) return i;
      estado = r.status;
    } catch {
      estado = 0; // sin respuesta: se reintenta
    }
    // Un 4xx (salvo 429) no se arregla repitiéndolo: es un rechazo, y reintentarlo es ruido. Un 5xx o la red caída sí se reintentan.
    if (estado >= 400 && estado < 500 && estado !== 429) throw new Error(`el receptor rechazó el evento (${estado})`);
    if (i >= intentos) throw new Error("agotados los reintentos");
    await new Promise((ok) => setTimeout(ok, esperaMs * 2 ** i));
  }
}
```

**Después.** El gateway es un consumidor más —con su durable, su backoff y su DLQ— y entrega el evento con `Flux-Signature` e `Idempotency-Key`. Se declara:

<!-- fuente-json: examples/ecosistema/gateway.json#/clients/0/deliveries/0 -->
```json
{
  "subject": "facturacion.factura.v1.emitida",
  "url": "https://web.example.com/recibir.php",
  "secretEnv": "WEB_PHP_ENTREGA_SECRETO",
  "timeoutMs": 10000
}
```

Y el receptor, en PHP (un hosting compartido), **verifica la firma sobre los bytes en bruto** y **deduplica por `Idempotency-Key`**; un `4xx` lleva el evento a la DLQ sin reintentos, y un `429`, `502`, `503` o `504` se reintentan (un `500` también, pero acotado a 2 entregas):

<!-- fuente: examples/ecosistema/web-php/recibir.php#php-recibir -->
```php
$cuerpo = file_get_contents('php://input');                                  // los bytes EN BRUTO: nunca json_decode + json_encode antes de verificar
$resultado = verificar_flux_signature(
    $_SERVER['HTTP_FLUX_SIGNATURE'] ?? '',
    $cuerpo,
    [getenv('FLUX_ENTREGA_SECRETO'), getenv('FLUX_ENTREGA_SECRETO_NUEVO')],  // dos secretos vigentes = rotación sin corte
);
if ($resultado !== 'ok') {
    http_response_code(401);                                                 // un 4xx es PERMANENT: a la DLQ, que es lo correcto con una firma que no verifica
    exit;
}

$clave = $_SERVER['HTTP_IDEMPOTENCY_KEY'] ?? '';
if (!preg_match('/^[0-9a-f-]{36}$/D', $clave)) {
    http_response_code(400);
    exit;
}
// Deduplicar: la misma `Idempotency-Key` otra vez NO repite el efecto, y se responde 2xx igualmente (el gateway ya entregó).
$dir = getenv('FLUX_WEB_DATA_DIR') ?: __DIR__ . '/datos';
@mkdir($dir, 0775, true);
$f = fopen($dir . '/recibidas.jsonl', 'c+');
flock($f, LOCK_EX);                                                          // en tu sistema: INSERT con clave única en una tabla
$vistas = [];
while (($linea = fgets($f)) !== false) {
    $vistas[trim($linea)] = true;
}
if (!isset($vistas[$clave])) {
    fwrite($f, $clave . "\n");
    // …aquí, el efecto: avisar al cliente de que su factura ya está lista (el `data` del evento es `json_decode($cuerpo, true)['data']`).
}
flock($f, LOCK_UN);
fclose($f);
http_response_code(204);                                                     // 2xx solo cuando está GUARDADO
```

### Fila 10 — Un sistema sin conexión permanente (hosting compartido, cron, serverless)  →  **gateway HTTP**

**Antes.** Dos llamadas a las APIs internas del CRM, con su dirección escrita en el PHP.

<!-- fuente: examples/ecosistema/antes-http/ficha-antes.php#antes-fila-10 -->
```php
// PHP en un hosting compartido: dos llamadas HTTP al CRM, con la dirección interna de otro sistema escrita en este.
$crm = getenv('CRM_URL');
$cliente = json_decode(file_get_contents("$crm/clientes/" . rawurlencode($argv[1])), true);
$descuento = json_decode(file_get_contents("$crm/descuento?clienteId=" . rawurlencode($argv[1]) . "&totalCents=" . (int) $argv[2]), true);
echo json_encode(['nombre' => $cliente['nombre'], 'descuentoCents' => $descuento['descuentoCents']]), "\n";
```

**Después.** El PHP solo conoce **el gateway** y un token suyo. Lee el estado y llama a la operación **con los mismos permisos que un servicio** (los de `services.json`, ni uno más: lo deniega el broker, no el gateway):

<!-- fuente: examples/ecosistema/web-php/ficha.php#php-ficha -->
```php
$entrada = flux_leer_estado('clientes.cliente.v1', $clienteId);
if ($entrada === null) {
    responder(404, ['error' => 'cliente desconocido']);
}
$descuento = flux_llamar_operacion('clientes.cliente.v1.calcular-descuento', ['clienteId' => $clienteId, 'totalCents' => $total, 'moneda' => 'EUR']);
```

<!-- fuente: examples/ecosistema/web-php/flux.php#php-leer-estado -->
```php
/** El valor ACTUAL de un dato que guarda otro sistema (`GET /clientes/123` antes). `null` si no existe o está borrado. */
function flux_leer_estado(string $contrato, string $clave): ?array
{
    try {
        [, $entrada] = flux_llamar('GET', '/v1/state/' . rawurlencode($contrato) . '/' . rawurlencode($clave));
    } catch (FluxError $e) {
        if ($e->estado === 404) {
            return null;
        }
        throw $e;
    }
    return $entrada;
}
```

<!-- fuente: examples/ecosistema/web-php/flux.php#php-llamar-operacion -->
```php
/**
 * Una operación (un cálculo o una orden con respuesta inmediata). `RPC_TIMEOUT` es AMBIGUO: la operación PUDO ejecutarse; si es una
 * orden, reintenta con una clave de negocio en `$datos` que quien responde deduplique.
 */
function flux_llamar_operacion(string $operacion, array $datos, int $timeoutMs = 5000): array
{
    [, $respuesta] = flux_llamar('POST', '/v1/rpc/' . rawurlencode($operacion), $datos, ['Flux-Timeout-Ms' => (string) $timeoutMs], intdiv($timeoutMs, 1000) + 5);
    return $respuesta;
}
```

Y si ni siquiera hay PHP —un `cron`, un script de shell— es lo mismo con `curl`:

<!-- fuente: examples/ecosistema/web-php/ficha-curl.sh#curl-ficha -->
```bash
curl -sS --fail-with-body "$FLUX_GATEWAY_URL/v1/state/clientes.cliente.v1/$cliente" \
  -H "Authorization: Bearer $FLUX_GATEWAY_TOKEN"
curl -sS --fail-with-body -X POST "$FLUX_GATEWAY_URL/v1/rpc/clientes.cliente.v1.calcular-descuento" \
  -H "Authorization: Bearer $FLUX_GATEWAY_TOKEN" -H 'Content-Type: application/json' -H 'Flux-Timeout-Ms: 5000' \
  -d "{\"clienteId\":\"$cliente\",\"totalCents\":$total,\"moneda\":\"EUR\"}"
```

Una llamada `rpc` que expira (`504 RPC_TIMEOUT`) **pudo ejecutarse**; si es una orden, reintenta con una clave de negocio en el cuerpo (como `referencia` en la fila 5). Un servicio que atiende operaciones **no puede** ser cliente de borde: HTTP no sostiene la suscripción viva que eso exige.

### Fila 11 — Ficheros grandes  →  **claim-check**

Un evento lleva como mucho **1 MiB**. Un fichero mayor no viaja por el bus: se sube a un almacén (S3, un volumen) y el evento lleva `{ uri, sha256, bytes }` ([`01-envelope.md` §7](../specification/01-envelope.md)). El receptor descarga por la `uri`, comprueba el `sha256`
y deduplica por el `id` del evento como siempre. El tráfico del fichero sigue siendo HTTP(S) hacia el almacén; lo que flux sustituye es **el aviso**, no la transferencia.

### Fila 12 — Tiempo real hacia el navegador  →  **fuera de alcance**

El navegador es cliente de una web, no un sistema del ecosistema. Cada web lo sirve desde su backend (SSE, WebSocket): el backend **observa** el estado o se suscribe al evento con flux y **empuja** al navegador. flux alimenta a ese backend; no sustituye al canal hacia el navegador.

## 4. Migrar sin apagar nada: el patrón gradual

Nada de «el sábado cambiamos todo». Cada integración se migra por separado y en este orden, porque cada paso es reversible hasta el último:

1. **Describe el contrato y levanta flux al lado.** `flux new`, el manifiesto, la configuración segura. No cambia nada de lo que corre.
2. **El productor publica los dos: doble publicación.** El dueño del dato (el CRM) sigue haciendo el `POST`/el endpoint de siempre **y** publica el evento o el estado. Quien más tiempo tarda en migrarse es quien más depende de lo viejo, así que se empieza por **el productor**: no rompe a nadie.

<!-- fuente: examples/ecosistema/antes-http/migracion-gradual.mjs#doble-publicacion -->
```js
// El CRM, durante la migración: el evento NUEVO y el webhook VIEJO, a la vez. El orden importa: primero lo que va a quedarse.
export async function registrarCliente({ bus, suscriptoresHttp, cliente }) {
  await bus.publish("clientes.cliente.v1.registrado", { clienteId: cliente.clienteId, aggregateVersion: 1, plan: cliente.plan }, { aggregateId: cliente.clienteId });
  await avisarAltaDeCliente(suscriptoresHttp, cliente);
}
```

3. **Migra los consumidores de uno en uno, de menor a mayor riesgo.** Primero los que **solo leen** (una réplica, un informe), después los que ejecutan efectos. Cada consumidor nuevo se prueba **en paralelo** con el viejo (mismo resultado, dos caminos) antes de quitar el viejo. Para el estado: el dueño republica todo
   al arrancar (esa es la carga inicial) y los observadores sustituyen sus `GET` por una copia local.
4. **Una operación se migra al revés: primero el que responde, después los que llaman.** La operación nueva existe junto al endpoint HTTP, y los llamantes cambian uno a uno (un campo nuevo en la petición contra un responder viejo es `INVALID_SCHEMA`).
5. **Mide quién usa lo viejo y apágalo cuando sea cero.** El polling se apaga **contando**, no suponiendo:

<!-- fuente: examples/ecosistema/antes-http/migracion-gradual.mjs#apagar-el-polling -->
```js
// Cuántas peticiones recibe el endpoint viejo, por quién y cuándo fue la última: cuando esta cuenta lleva una semana a cero, se borra.
export function contarUso(servirCambios) {
  const uso = new Map();
  const medido = (req, res) => {
    const quien = req.headers["x-forwarded-for"] ?? req.socket.remoteAddress;
    uso.set(quien, { peticiones: (uso.get(quien)?.peticiones ?? 0) + 1, ultima: new Date().toISOString() });
    return servirCambios(req, res);
  };
  return { medido, uso };
}
```

   Cuando el endpoint lleva una semana a cero peticiones, se responde `410 Gone` durante un ciclo de despliegue y se borra. Un cron que nadie recordaba aparece en esa cuenta, no en una incidencia.
6. **Quita la doble publicación** del productor. Si un contrato tiene que cambiar durante la migración: un MAYOR nuevo (`…v2.…`) y doble publicación de **al menos 90 días** ([`05-compatibility.md`](../specification/05-compatibility.md)).

Dos reglas que ahorran incidencias: **migra el contrato antes que el código** (el esquema es el acuerdo; el código es lo que cambia), y **no mezcles migrar con cambiar el comportamiento** (el mismo importe, el mismo plazo, la misma semántica: un cambio de unidades que valida sigue siendo una ruptura).

## 5. Cuándo NO migrar: lo que sigue siendo HTTP

flux sustituye las **integraciones entre tus sistemas**. Sigue siendo HTTP:

- **Todo lo que no puede mantener una conexión NATS** —un hosting compartido, un `cron`, una función *serverless*, el sistema de un tercero—. **No** se queda hablando HTTP con tus otros servicios: habla HTTP **con el gateway**, y el gateway con el ecosistema (filas 8 a 10). HTTP queda en el borde; dentro, nada lo usa.
- **Tu API pública de producto** (la que usan tus clientes o terceros). flux no es una API pública: es la integración interna. Puede estar **detrás** de ella (el backend de tu API publica y observa con flux).
- **Las llamadas salientes a APIs de terceros** (cobrar con la API de la pasarela, enviar un correo). El gateway entrega *eventos* a una URL (fila 9), no hace llamadas arbitrarias. Esa llamada la hace un servicio tuyo, y es un efecto: la idempotencia (`Idempotency-Key` derivada del `id` del evento) es tuya.
- **Tiempo real hacia el navegador** (fila 12) y **la transferencia de ficheros grandes** (fila 11).
- **Consultas ad hoc** (buscar, filtrar, agregar). El estado es *un valor por clave*, no una base de datos consultable: no hay «lista los clientes de Madrid». Lo que necesites consultar, lo consultas **en tu copia local** (la fila 4: la mantienes tú, en tu base de datos).
- **Un servicio escrito en un lenguaje sin SDK con estado/petición/respuesta** (todos menos Node y Rust, hoy): va por el gateway como cliente de borde hasta la Fase 6.

## 6. Lista de comprobación de seguridad

Cada punto está **ejecutado** en el test del ejemplo, no solo escrito. Es una plantilla: cópiala y ejecútala contra tu ecosistema.

**Identidad y permisos**
- [ ] **Una nkey por servicio**; las semillas, en un gestor de secretos y por entorno, nunca en el repositorio ni compartidas (`.dev-secure/` está en `.gitignore`).
- [ ] **TLS** en el broker con tu CA; cada cliente con `tls.caFile`. Sin `allow_non_tls`.
- [ ] Las **ACLs salen de `services.json`** (`gen-acl.mjs`), nunca a mano; `gen-acl.mjs … --check` en CI. Nunca `$JS.API.>` «para que funcione» ni un `_INBOX.>` común: **cada servicio, su inbox** (`_INBOX.<servicio>.>`, lo fija el SDK).
- [ ] **Los streams y los durables los crea `flux provision`**, con credenciales de operación, en cada despliegue (es idempotente). Ningún servicio tiene permiso de crearlos (no uses `--dev` fuera de tu máquina).
- [ ] `flux doctor` en CI y tras cada cambio de infraestructura: ningún stream captura `rpc.>`.
- [ ] **Las negativas se prueban**: un servicio con credenciales válidas **no** puede leer, escribir ni llamar lo que su manifiesto no le concede. El paso 8 del test del ejemplo lo hace (y un mutante que concede de más hace fallar la prueba).

**Datos**
- [ ] **Sin PII en eventos**: referencias (`clienteId`), no valores. El dato personal vive **solo en estado**, clasificado `confidential`, con `x-pii`, y borrable (`state.delete`); el observador borra su copia.
- [ ] **L3 estricta** al publicar (`validation: { mode: "strict", bundle }`): un contrato roto falla en quien publica. El bundle se despliega con el servicio.
- [ ] **Esquemas inmutables**: `check-compat.mjs` y `bundle-schemas.mjs --check` en CI. Dinero en enteros de la unidad mínima + ISO 4217.
- [ ] Ni un dato personal **en ningún stream salvo el de estado**, y **la DLQ vacía**: el paso 9 del test recorre todos los streams.

**Consumidores y llamadas**
- [ ] **Todo consumidor es idempotente** (A, B o C de `AGENTS.md` §4) y **no asume orden** (`aggregateVersion`). Lo prueba una **segunda entrega** del mismo evento, también tras reiniciar el servicio.
- [ ] Una **orden** lleva una clave de negocio y quien responde deduplica. `RPC_TIMEOUT` **no** dice que no se ejecutó.
- [ ] Un `logger` configurado en cada `connect()`: un `ack` o un pull denegados **no lanzan**, solo el logger los dice.
- [ ] Relojes sincronizados (NTP): la firma y la ventana de 300 s dependen de ello.

**El borde**
- [ ] El gateway detrás de **TLS** (un proxy que lo termine, o el propio): el token viaja en una cabecera. **Un token por cliente**, con su SHA-256 en `gateway.json` (que **no contiene secretos**) y rotación sin corte (varios tokens).
- [ ] Un cliente de borde **no tiene permisos especiales**: un token válido sobre lo que no tiene concedido lo deniega el **broker** (403). Probado en el paso 8.
- [ ] Cada webhook de terceros declara su **firma** (un hook sin `verify` no arranca) y qué campos pasan (`data.pick`). Una firma mala o fuera de tolerancia es 401 y **no publica nada**.
- [ ] Los receptores de entregas **verifican la firma sobre los bytes en bruto**, en tiempo constante, con tolerancia simétrica, y deduplican por `Idempotency-Key`.
- [ ] El gateway **no registra nunca `data`**; límites de cuerpo (1 MiB) y de peticiones por token.

## 7. Lo que no se ve en el código

- **Operar un NATS**: almacenamiento de JetStream (disco, copias de seguridad, réplicas: `flux provision --replicas 3` en producción), TLS y rotación de claves. Es un servicio más que hay que mantener, y un solo nodo es un punto único de fallo.
- **El borrado físico** en disco de lo que `state.delete` sustituye por una lápida, y las claves y los tenants (visibles en `STREAM.INFO`), no los cubre el protocolo: la clave es un identificador opaco, nunca un correo.
- **Al menos una vez** significa que los duplicados llegan **siempre**: la idempotencia no es opcional ni «para más adelante».
- **Orden**: flux no lo garantiza. Si tu lógica dependía del orden de los webhooks, se rediseña con `aggregateVersion`.
- **Rendimiento**: lo medido, con sus condiciones y sin maquillar, en [`bench/RESULTS.md`](../bench/RESULTS.md).
