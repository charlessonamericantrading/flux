# flux CLI

Herramienta operativa del [Event Protocol v1](../README.md): `doctor`, `tail`, `dlq`, `validate`
y `provision`.

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

## `flux tail` — eventos en vivo

```bash
flux tail 'pedidos.>'                          # solo lo nuevo
flux tail 'pedidos.>' --since 5m               # incluye los últimos 5 minutos
flux tail 'pedidos.pedido.v1.creado' --full    # JSON completo
flux tail 'dlq.pedidos.>' --tenant acme        # solo los muertos de un tenant
flux tail 'pedidos.>' --json | jq .            # para encadenar
```

```
12:06:51.032 pedidos.pedido.v1.creado ped-123 ⟨corr-aaa⟩ {"pedidoId":"ped-1","totalCents":9990}
```

El `⟨corr-aaa⟩` es el `correlationid` abreviado: permite seguir un flujo a ojo entre
subjects distintos sin abrir un trazador.

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

### Datos sensibles

`tail` y `dlq inspect` **no imprimen el payload** de eventos con
`dataclassification: confidential` o `restricted` — una terminal acaba en un log.
`--show-data` lo fuerza.

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

## Sobre las dependencias

La CLI **no usa el SDK de flux**, igual que la suite de conformidad: una herramienta
de diagnóstico construida sobre el SDK no puede diagnosticar los fallos del SDK.

`flux provision` es la excepción a «la CLI no sale de `cli/`»: importa
[`scripts/lib/manifest.mjs`](../scripts/lib/manifest.mjs) (nombres, expansión de `consumes` y
permisos por servicio) y lee `protocol.json`, los dos de la raíz del repositorio. Es deliberado:
duplicarlos haría que el aprovisionador y el generador de ACL pudieran discrepar en el nombre
de un durable, y esa discrepancia no da error en ningún sitio salvo un `Permissions Violation`
en el log de un servicio. La CLI es `private` y se usa desde un checkout del repositorio
(`npm link` o `node cli/src/index.mjs`), así que la ruta relativa siempre resuelve; si algún
día se publica como paquete, ese módulo y `protocol.json` tendrán que viajar con ella.
