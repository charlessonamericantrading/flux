# flux CLI

Herramienta operativa del [Event Protocol v1](../README.md): `doctor`, `tail`, `dlq`, `state`,
`validate` y `provision`.

```bash
cd cli && npm install && npm link     # o: node cli/src/index.mjs <cmd>
```

---

## `flux doctor` — el que justifica la CLI

Audita un cluster vivo contra el protocolo.

```
$ flux doctor

Consumidores
STREAM       DURABLE                                    ACK   ACK_WAIT  MAX_DEL  ESTADO
EVT_PEDIDOS  envios-api__pedidos_pedido_v1_creado       ✓     1s        6        ack_wait corto
EVT_PEDIDOS  facturacion-api__pedidos_pedido_v1_creado  ✓     30s       6        ok
EVT_PEDIDOS  legacy_importer                            none  —         -1       auto-ack, reintentos infinitos

Diagnóstico
  ✗ EVT_PEDIDOS/envios-api__pedidos_pedido_v1_creado  ack_wait=1s < 30s
      Todo handler que tarde más recibe el mensaje REENTREGADO mientras aún se
      ejecuta: ejecución concurrente del mismo evento — 03-delivery.md §2.1
  ✗ EVT_LOGISTICA  no existe DLQ_LOGISTICA
  ...
```

Sale con código **1** si encuentra errores, así que sirve tal cual en CI o en un
smoke test post-despliegue.

**Por qué existe:** los fallos que este protocolo persigue —`ack_wait` sobrescrito,
streams solapados, auto-ack, reintentos infinitos— **no producen ningún error en
producción**. Producen comportamiento incorrecto silencioso. La única forma de
detectarlos es preguntarle al servidor qué tiene configurado de verdad.

Comprueba: nombres de stream y durable, solape entre streams, `.dlq` como sufijo,
retención, `duplicate_window`, existencia de DLQ por dominio, `ack_wait` vs
`backoff[0]`, `ack_policy`, `max_deliver`, y profundidad de las DLQ.

### El estado replicado ([10-state.md](../specification/10-state.md))

Audita cada `STATE_<DOM>` contra el bloque `state` de `protocol.json`. Son ajustes que, mal puestos,
**no dan ningún error** y rompen la semántica:

| Hallazgo | Por qué importa |
|---|---|
| `max_msgs_per_subject` ≠ 1 | Sin él es un log, no un mapa: el valor viejo de una clave no sale y un observador nuevo recibe el historial como si fuera la foto |
| `allow_direct` ≠ true | `get` y el paso 1 de `put` reciben «sin respuesta», que se ve igual que un stream inexistente (A-32) |
| `max_age` ≠ 0 | El estado caduca: una copia que miente. (En `EVT_`/`DLQ_`, `max_age: 0` sigue siendo un aviso; en estado es lo correcto) |
| `discard` ≠ `old` | Con `discard: new` por subject el broker rechaza la segunda escritura de una clave (err 10077): un create-only (A-46) |
| `storage` ≠ `file`, `retention` ≠ `limits`, falta `state.<dom>.>` | El estado desaparece al reiniciar el broker, o se descarta por consumo, o no lo captura nadie |
| `DLQ_<DOM>` no captura `dlq.state.<dom>.>` (o no existe) | El fallo de un observador no tiene dónde caer: el publish de su DLQ falla (A-44) |
| Un durable de estado con `deliver_policy` ≠ `last_per_subject` | Con `new` el observador nace sin la foto; con `all` solo sirve mientras `max_msgs_per_subject` siga en 1 (A-35) |
| Un durable cuyo **filtro no encaja con su nombre** (contrato o tenant) | Un durable de un tenant sin el tenant en el nombre es compartido: el broker reparte los mensajes entre sus instancias y cada una ve la mitad de la foto (A-47) |
| Config del durable ≠ canónica (`max_ack_pending`, `max_deliver`, `backoff`, `replay_policy`) | Los de `ack_wait` y `ack_policy` los da ya la comprobación general |

`replicas=1` es una nota (`-v`): el valor de desarrollo. Un tope global (`max_msgs`/`max_bytes`) en un
estado es un aviso: con `discard: old` el broker borraría la clave más antigua **sin lápida y sin error**
(esto último no está en la spec: es de la misma familia).

```
Diagnóstico
  ✗ STATE_CLIENTES  allow_direct=false, debe ser true
      Sin él DIRECT.GET no contesta (ni 404): `state.get` y el paso 1 de `put` reciben «sin respuesta» …
  ✗ STATE_CLIENTES  DLQ_CLIENTES no captura dlq.state.clientes.> (subjects: dlq.clientes.>)
  ✗ STATE_CLIENTES/tienda-web__state_clientes_cliente_v1  deliver_policy=all, debe ser last_per_subject
  ✗ STATE_CLIENTES/tienda-web__state_clientes_cliente_v1  el nombre no lleva tenant y el filtro es del tenant «acme»
```

## `flux tail` — eventos en vivo

```bash
flux tail 'pedidos.>'                          # solo lo nuevo
flux tail 'pedidos.>' --since 5m               # incluye los últimos 5 minutos
flux tail 'pedidos.pedido.v1.creado' --full    # JSON completo
flux tail 'dlq.pedidos.>' --tenant acme        # solo los muertos de un tenant
flux tail 'state.clientes.>'                   # las escrituras de estado de un dominio
flux tail 'dlq.state.clientes.>' --since 1h    # los fallos de los observadores de estado
flux tail 'pedidos.>' --json | jq .            # para encadenar
```

```
12:06:51.032 pedidos.pedido.v1.creado ped-123 ⟨corr-aaa⟩ {"pedidoId":"ped-1","totalCents":9990}
```

El `⟨corr-aaa⟩` es el `correlationid` abreviado: permite seguir un flujo a ojo entre
subjects distintos sin abrir un trazador.

Con `state.` entiende el estado: en vez del `subject` del sobre (que ahí es la clave) muestra lo que
decidió el broker —**tenant/clave, revisión y versión**—, marca las **lápidas** y los sobres que
discrepan de su subject:

```
12:06:51.032 state.clientes.cliente.v1.acme.cli-1 acme/cli-1 rev 41 · v7 ⟨01924f8e⟩ [confidential]
12:09:02.057 state.clientes.cliente.v1.acme.cli-1 acme/cli-1 rev 42 · v8 LÁPIDA ⟨01924f90⟩ [lápida]
```

`dlq.state.<dom>.>` va a `DLQ_<DOM>` (antes se buscaba un `DLQ_STATE`, que no existe) y muestra la clave
y la revisión que fallaron.

Usa un consumidor **efímero de solo lectura**. Mirar el bus nunca altera el estado de
un consumidor de producción — un `tail` que consuma de un durable ajeno es una
herramienta de depuración que provoca incidentes.

## `flux dlq` — triaje y recuperación

```bash
flux dlq ls pedidos                            # resumen agrupado por causa
flux dlq inspect pedidos --subject pedidos.pedido.v1.creado
flux dlq replay pedidos                        # SIMULACIÓN
flux dlq replay pedidos --confirm              # ejecuta, tras confirmar
```

```
$ flux dlq ls pedidos
N  RAZÓN      SUBJECT                   CÓDIGO               INTENTOS  ÚLTIMO
4  permanent  pedidos.pedido.v1.creado  PEDIDO_YA_CANCELADO  1         hace 10s
2  retryable  pedidos.pedido.v1.creado  HTTP_503             6         hace 10s
1  poison     pedidos.pedido.v1.creado  MALFORMED_JSON       1         hace 10s
```

Agrupa por `(subject, razón, código)` porque un incidente produce N eventos idénticos,
y verlos de uno en uno esconde que son el mismo problema.

### Seguridad del `replay`

El replay es la operación más peligrosa de la herramienta, así que:

- **Simulación por defecto.** Sin `--confirm` no publica nada.
- **Confirmación interactiva** que recuerda las dos comprobaciones de
  [04-errors.md §4.1](../specification/04-errors.md): ¿se ha arreglado la causa?
  ¿es idempotente el consumidor para estos `id`? (`--yes` la salta, para automatizar.)
- **Conserva el `id` original.** Regenerarlo rompería la idempotencia de todos los
  consumidores aguas abajo y convertiría una recuperación en un incidente nuevo.
- **Omite los POISON**: no son eventos válidos, republicarlos solo los devuelve a la DLQ.
- **Copia, no mueve.** Los originales siguen en la DLQ hasta que confirmes el
  reproceso y los purgues a mano.
- **No reproduce estado.** `replay` **rechaza `dlq.state.>`** (con `--subject dlq.state.…`, `state.…` o
  el «dominio» `state`) y apunta a [`flux state resync`](#flux-state--mirar-y-recuperar-el-estado-replicado).
  Republicar un valor viejo pisaría uno nuevo, y la DLQ de estado ni siquiera guarda el dato: su `data` es
  `{ key, revision }` ([10-state.md §6](../specification/10-state.md)). Sin `--subject` reproduce
  `dlq.<dominio>.>` —los eventos— y **nombra** los mensajes de estado que haya, para que quien reproduce
  «todo» sepa que quedan observadores con fallos pendientes. (Antes derivaba el destino del `type` y
  publicaba `state.clientes.v1.cliente`: una clave fantasma en el stream de estado, sin ningún error.)

### Datos sensibles

`tail`, `dlq inspect` y `state get` **no imprimen el payload** de lo que no es `public` ni `internal` —
una terminal acaba en un log. `--show-data` lo fuerza. Es más urgente con el estado: `confidential` es la
clasificación **por defecto** de un `put`, y es donde el dato personal tiene permiso para vivir.

- Falla **cerrado**: un sobre sin `dataclassification` (POISON) tampoco se imprime.
- Vale también para `--json` y `--full`. Antes saltaban la regla: `flux tail … --full` volcaba el
  `data` de un evento `confidential` (medido), y `--show-data` no hacía nada en `tail` ni en `dlq inspect`.
- El `data` de la DLQ de estado (`{ key, revision }`) se muestra: ni la clave ni la revisión son PII. Si
  lleva **más** (un SDK copiando el dato a una DLQ de 90 días), se avisa y no se imprime.

## `flux state` — mirar y recuperar el estado replicado

```bash
flux state ls clientes.cliente.v1                     # todas las claves, de todos los tenants
flux state ls clientes.cliente.v1 --tenant acme --json
flux state get clientes.cliente.v1 acme cli-1         # el sobre tal cual está en el stream
flux state resync tienda-web clientes.cliente.v1 --tenant acme             # SIMULACIÓN
flux state resync tienda-web clientes.cliente.v1 --tenant acme --confirm   # ejecuta
```

Las tres son **de operación**: usan credenciales de operación (`--creds`), no las de un servicio.
`ls` necesita `STREAM.INFO.STATE_<DOM>`, que **no** se concede a los observadores porque lista todas las
claves y todos los tenants (A-39); `resync` borra un durable, y un servicio no puede
([06-security.md §3.5](../specification/06-security.md)).

### `ls`

Una petición `STREAM.INFO` con `subjects_filter` da todas las claves, y una lectura directa por clave da
la revisión, el tamaño y si es una lápida:

```
$ flux state ls clientes.cliente.v1 --tenant acme
TENANT  CLAVE    REVISIÓN  VERSIÓN  TAMAÑO  ESCRITO   ESTADO
acme    cli-1    1         1        564 B   hace 21m  ok
acme    cli-2    2         1        564 B   hace 21m  ok
…
acme    cli-7    103       2        495 B   hace 20m  LÁPIDA
…
100 clave(s) de clientes.cliente.v1 en STATE_CLIENTES · 1 tenant(s) · 1 lápida(s)
```

La **revisión** es la secuencia del stream (crece con cada escritura de cualquier clave) y la **versión**
es el `aggregateVersion` del dueño. Una **lápida** es una clave borrada: sigue en el stream (es permanente
en 1.1) y `get` del SDK devuelve `null`. `--limit n` lista solo las n primeras; `--json` da un array.

Filtra por `state.<contrato>.>` y no por `….*.*` a propósito: así aparecen también las entradas que el
broker acepta y **ningún observador recibe** —una clave `a.b` son dos tokens (A-41)— y las que discrepan
de su subject (`STATE_SUBJECT_MISMATCH`), marcadas en la columna `ESTADO` en vez de escondidas.

### `get`

`DIRECT.GET` por subject, como `state.get` del SDK, pero mostrando el sobre **entero** y sin traducir una
lápida a `null`: el operador necesita ver que la clave está borrada.

```
$ flux state get clientes.cliente.v1 acme cli-1
state.clientes.cliente.v1.acme.cli-1
  revisión       1
  escrito        2026-11-26T13:01:36.286619856Z  hace 21m
  versión        1
  tamaño         564 B
  clasificación  confidential  data oculto — usa --show-data si de verdad lo necesitas
…
$ flux state get clientes.cliente.v1 acme cli-999
✗ 404: no hay valor en state.clientes.cliente.v1.acme.cli-999
```

Salida `1` si no hay valor (404); `0` con una lápida, que se muestra. Si el stream no existe, o no tiene
`allow_direct`, nadie contesta a un `DIRECT.GET` y los dos casos se ven igual (A-32): la CLI sí puede
preguntar y dice **cuál** de los dos es. La clave, el tenant y el contrato se validan antes de leer
(`INVALID_STATE_KEY`, `INVALID_TENANT_FOR_STATE`, `RESERVED_DOMAIN`): un `*` llegaría al broker como
un comodín y devolvería **otra** clave.

### `resync` — la recuperación del estado

Un observador que perdió su copia (o cuyo handler descartó entradas) **no** se recupera reproduciendo su DLQ:
la DLQ de estado guarda el *fallo*, no el dato, y republicar un valor viejo pisaría uno nuevo. Se recupera
**borrando y recreando su durable**, con el nombre y la configuración exactos de
[10-state.md §5.2](../specification/10-state.md): el broker vuelve a entregar el **último valor de cada
clave** (A-37), por el camino normal. Aplica lo *actual*, no lo que había cuando falló.

```
$ flux state resync tienda-web clientes.cliente.v1 --tenant acme
flux state resync tienda-web clientes.cliente.v1 --tenant acme   SIMULACIÓN
  stream      STATE_CLIENTES
  durable     tienda-web__state_clientes_cliente_v1__acme
  filtro      state.clientes.cliente.v1.acme.*
  hoy         pendientes 0 · en vuelo 0 · reentregas 0 · última entrega: secuencia 103
  claves      100 en el filtro: se entregarán otra vez, lápidas incluidas

  Haría, por este orden:
    1. BORRAR el durable: el broker olvida hasta dónde llegó (entregados, en vuelo, ack_floor).
    2. RECREARLO con deliver_policy=last_per_subject y la configuración canónica de
       03-delivery.md §2: el broker entrega otra vez el último valor de cada clave (A-37).
  No toca: el stream, ni una clave, ni los durables de otros servicios o tenants.
  Efecto en el servicio: su handler recibirá otra vez entradas que ya aplicó, así que DEBE ser
  idempotente (UPDATE … WHERE aggregate_version < $n). `onSynced` no se vuelve a invocar.

Simulación. No se ha cambiado nada. Para ejecutarlo de verdad:
  flux state resync tienda-web clientes.cliente.v1 --tenant acme --confirm
```

| Garantía | Por qué |
|---|---|
| **Simulación por defecto** | Como `dlq replay`: sin `--confirm` no se cambia nada y la salida dice qué se haría. Con `--confirm` pregunta (`--yes` lo salta; sin terminal y sin `--yes` **falla** en vez de colgarse) |
| **Nunca toca el stream ni una clave** | Solo el durable de **un** servicio y, con `--tenant`, de **un** tenant. Los de otros servicios o tenants quedan como están |
| **No crea un durable que no existe** | Un nombre mal escrito dejaría un durable fantasma que acumula pendientes sin que nadie lo lea. Si falta, lo dice y **enseña los que sí existen** de ese servicio (¿falta `--tenant`?) |
| **Recrea con la config canónica** | Si el durable tenía otra (`deliver_policy: new`, `max_ack_pending: 10`…), la simulación enseña la diferencia y se recrea con la buena. Tras crearlo **compara** lo que devolvió el servidor con lo pedido y falla en alto si difiere |
| **Si la creación falla, no calla** | Entre el borrado y la creación el servicio no tiene durable: reintenta, y si no puede dice que el durable **se borró** y cómo recrearlo (`flux provision`) |

El `--tenant` es parte del nombre del durable: sin él se recrea `<servicio>__state_<contrato>` (el de un
`watch` sin tenant, filtro `….*.*`); con él, `…__<tenant>`. No hay «todos los tenants a la vez»: un
servicio con un durable por tenant se resincroniza tenant a tenant.

**Qué ve el servicio.** Medido con `nats.js` 3.4.0: un `consume()` que seguía conectado recibe
`consumer_deleted` y **no vuelve a pedir** hasta que detecta la falta de heartbeats, unos **45 s** después;
entonces recibe la foto completa sin reiniciar nada. Otros clientes (el SDK de Rust, `async-nats`) no se
han medido.

---

## `flux validate` — sin broker

```
$ flux validate pedidos.pedido.v1.creado
✓ pedidos.pedido.v1.creado
  type       com.flux.pedidos.pedido.creado.v1
  stream     EVT_PEDIDOS
  dlq        dlq.pedidos.pedido.v1.creado
  durable    <servicio>__pedidos_pedido_v1_creado
  schema     schemas/pedidos/pedido/creado/1.0.0.json

$ flux validate Pedidos.pedido.v1.actualizado
✗ Pedidos.pedido.v1.actualizado
  • mayúsculas
    NATS es case-sensitive: crea un subject fantasma al que nadie está suscrito,
    y no produce ningún error
  • "actualizado" no dice QUÉ cambió
    obliga a cada consumidor a implementar —y equivocar— su propio diff.
    Nombra el hecho: 'direccion-envio-cambiada'
```

Útil en un hook de pre-commit o al revisar un PR que añade un evento.

---

## `flux provision` — crea lo que las ACLs presuponen

```bash
flux provision examples/services.json --dry-run --creds ops.creds      # qué crearía
flux provision examples/services.json --creds ops.creds                # lo crea
flux provision examples/services.json --replicas 3 --creds ops.creds   # producción
```

**Por qué existe.** En producción los servicios **no crean** streams ni consumidores: no
tienen permiso, y es deliberado ([06-security.md §3.6](../specification/06-security.md)).
Un stream es un recurso compartido de todo el dominio, y dejar que el primer servicio que
arranque lo cree con la configuración que le apetezca convierte la retención, las réplicas
y la ventana de duplicados en una carrera. Alguien tiene que crearlos, y ese alguien es esto,
con credenciales de **operación** —no las de un servicio—.

Lee el mismo manifiesto que [`scripts/gen-acl.mjs`](../scripts/gen-acl.mjs) y crea, por cada
dominio, `EVT_<DOM>` y `DLQ_<DOM>` ([02-naming.md §3.3](../specification/02-naming.md)), y,
por cada subject consumido tras expandir los patrones de `consumes` contra el bundle, el
durable `<servicio>__<subject>` con la configuración canónica de
[03-delivery.md §2](../specification/03-delivery.md). Más, por cada dueño, el durable con el
que lee su propia DLQ (`<servicio>__dlq_<dominio>`).

**Estado replicado** ([10-state.md](../specification/10-state.md)). Por cada dominio que **tiene** estado
—el bundle tiene un contrato suyo en `schemas/state/`— crea además `STATE_<DOM>` con la
configuración de `protocol.json` → `state.stream` (`max_msgs_per_subject: 1`, `allow_direct`,
`max_age: 0`, `discard: old`, ventana de 2 m), hace que `DLQ_<DOM>` capture también
`dlq.state.<dom>.>`, y crea el durable de cada `watches` del manifiesto —uno por tenant si confina,
`<servicio>__state_<dom>_<agr>_v<M>[__<tenant>]`, con `deliver_policy: last_per_subject`— y el
durable con el que el dueño lee su DLQ de estado (`<servicio>__dlq_state_<dominio>`). Un dominio sin
estado no cambia. El formato de `watches` y los permisos que genera, en
[06-security.md §3.8](../specification/06-security.md#38-estado-replicado-watches-y-sus-permisos).

```
$ flux provision examples/services.json --dry-run
flux provision --dry-run  examples/services.json · 11 stream(s), 16 consumidor(es) · réplicas 1
  Simulación: no se crea nada.

  SE CREARÍA  EVT_FACTURACION                                                   facturacion.>
  SE CREARÍA  DLQ_FACTURACION                                                   dlq.facturacion.>
  …
  SE CREARÍA  STATE_CLIENTES                                                    state.clientes.>
  SE CREARÍA  EVT_PEDIDOS / facturacion-api__pedidos_pedido_v1_creado           filtro pedidos.pedido.v1.creado
  SE CREARÍA  STATE_CLIENTES / tienda-web__state_clientes_cliente_v1__acme      filtro state.clientes.cliente.v1.acme.*
  …
  27 se crearía(n)

$ flux provision examples/services.json        # y otra vez
  …
  27 sin cambios

✓ Sin cambios: el broker ya tiene todo lo que el manifiesto exige.
```

| Garantía | Por qué |
|---|---|
| **Idempotente** | Una segunda ejecución no cambia nada y lo dice. Es lo que permite llamarla en cada despliegue sin miedo |
| **Nunca borra** | Un stream borrado es el historial de un dominio. Lo que el manifiesto no menciona se ignora; auditarlo es trabajo de `flux doctor` |
| **Nunca sobrescribe** | Si un stream o un consumidor existe con otra configuración, **informa de la diferencia** y lo deja como está. Cambiar la retención de un stream vivo, o `max_ack_pending` de un consumidor con mensajes en vuelo, es una decisión de una persona. **Una sola excepción, que solo añade**: un `DLQ_<DOM>` de la 1.0 (solo `dlq.<dom>.>`) cuyo dominio ha ganado estado se **amplía** con `dlq.state.<dom>.>`: no pierde ningún mensaje (A-44). `--dry-run` lo muestra como `SE AMPLIARÍA`, y una DLQ con un subject de más se informa como diferencia y no se toca |
| **`--dry-run` no crea nada** | Pero sí consulta el broker, así que distingue «se crearía» de «ya existe» |
| **Una sola fuente** | Los nombres y la expansión de `consumes` salen del mismo módulo que usa el generador de ACL ([`scripts/lib/manifest.mjs`](../scripts/lib/manifest.mjs)), y las configuraciones, de `protocol.json`. Si cada uno calculase el nombre de un durable por su cuenta, el servicio arrancaría y el broker le denegaría su propio consumidor |

```
  DIFIERE     EVT_PEDIDOS
      max_age: existente 7d, deseado 30d
  DIFIERE     EVT_PEDIDOS / facturacion-api__pedidos_pedido_v1_creado
      max_ack_pending: existente 10, deseado 256
```

**Código de salida:** `0` si todo está creado o sin cambios; `1` si algún recurso **difiere** o
dio error (sirve en CI para detectar deriva, igual que `flux doctor`); `2` si el manifiesto o el
bundle no se pueden leer.

`--replicas` (por defecto **1**, para que arranque contra un NATS de un solo nodo) es de los
streams; en producción, `--replicas 3`. Ejecutado con el valor por defecto contra un clúster
que ya tiene 3, informará de la diferencia y no tocará nada.

Con el **Modelo B** (`gen-acl.mjs --tenants`) cada tenant es una account con sus propios
streams: se ejecuta **una vez por account**, con las credenciales de operación de cada una.

**Es obligatorio en producción**: con las ACLs generadas por defecto un servicio no tiene
`CONSUMER.CREATE`, así que un durable que no exista no se recrea solo —el servicio falla con
`NOT_PROVISIONED`—. Solo con `gen-acl.mjs --dev` el SDK puede crearlo (desarrollo).

**Credenciales de operación.** `--creds` es un fichero `.creds`. El usuario de operación **no**
lo genera `gen-acl.mjs`: vive fuera del manifiesto y fuera de la config de los servicios.

---

## Opciones globales

| | |
|---|---|
| `-s, --server <url>` | Por defecto `$NATS_URL` o `nats://127.0.0.1:4222` |
| `--creds <fichero>` | Credenciales NATS (`.creds`). Hasta la Fase 1 se pasaba a una opción que el cliente NATS **ignora**: la conexión se abría sin credenciales y fallaba con *Authorization Violation* sin decir que el fichero nunca se leyó |
| `-v, --verbose` | Incluye las notas informativas |

Opciones de `state`: `--tenant <t>` (`ls`: solo ese tenant; `resync`: el durable de ese tenant), `--json`
(`ls`, `get`), `--show-data` (`get`, `tail`, `dlq inspect`), `--limit <n>` (`ls`), `--confirm` y `--yes`
(`resync`).

## Pruebas

```bash
cd cli && npm install
NATS_SERVER=/ruta/a/nats-server npm test
FLUX_REQUIRE_BROKER=1 npm test          # sin nats-server, las pruebas con broker FALLAN en vez de saltarse
```

`node --test`, sin dependencias nuevas. Las pruebas con broker levantan **su propio** `nats-server` en un
puerto libre de `FLUX_TEST_PORTS` (por defecto `4650-4699`), ejecutan la CLI **como proceso** y escriben el
estado **a mano**, con el algoritmo de `put`/`delete` de [10-state.md §3.2](../specification/10-state.md)
sobre NATS crudo: ni la CLI ni sus pruebas usan el SDK de flux.

Van **en los dos sentidos**: la simulación de `resync` no cambia nada y `--confirm` sí; `doctor` sale `0` con
la configuración buena y `1` con cada una de las doce rotas (y vuelve a `0` al arreglarla); `dlq replay`
rechaza el estado y sigue reproduciendo los eventos. `FLUX_REQUIRE_BROKER=1` es lo que usa CI: que falte
`nats-server` es un fallo, no un salto con aviso. Los vectores válidos e inválidos de claves, tenants,
contratos y durables salen de `protocol.json`, la fuente que también leen los SDKs.

## Sobre las dependencias

La CLI **no usa el SDK de flux**, igual que la suite de conformidad: una herramienta
de diagnóstico construida sobre el SDK no puede diagnosticar los fallos del SDK.

Los comandos de estado repiten el algoritmo de nombres y las reglas de validación de
[10-state.md](../specification/10-state.md) (`state-model.mjs`), y para que no puedan divergir de la spec en
silencio **no llevan ninguna constante propia**: los patrones, los límites y la configuración canónica se
leen de `protocol.json` (bloque `state`). El módulo se carga **de forma perezosa**: `validate` y `keygen`
siguen funcionando en un checkout sin `protocol.json`.

`flux provision` es la excepción a «la CLI no sale de `cli/`»: importa
[`scripts/lib/manifest.mjs`](../scripts/lib/manifest.mjs) (nombres, expansión de `consumes` y
permisos por servicio) y lee `protocol.json`, los dos de la raíz del repositorio. Es deliberado:
duplicarlos haría que el aprovisionador y el generador de ACL pudieran discrepar en el nombre
de un durable, y esa discrepancia no da error en ningún sitio salvo un `Permissions Violation`
en el log de un servicio. La CLI es `private` y se usa desde un checkout del repositorio
(`npm link` o `node cli/src/index.mjs`), así que la ruta relativa siempre resuelve; si algún
día se publica como paquete, ese módulo y `protocol.json` tendrán que viajar con ella.
