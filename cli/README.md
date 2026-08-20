# flux CLI

Herramienta operativa del [Event Protocol v1](../README.md). Cuatro comandos.

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

## Opciones globales

| | |
|---|---|
| `-s, --server <url>` | Por defecto `$NATS_URL` o `nats://127.0.0.1:4222` |
| `--creds <fichero>` | Credenciales NATS |
| `-v, --verbose` | Incluye las notas informativas |

## Sobre las dependencias

La CLI **no usa el SDK de flux**, igual que la suite de conformidad: una herramienta
de diagnóstico construida sobre el SDK no puede diagnosticar los fallos del SDK.
