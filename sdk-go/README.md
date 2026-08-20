# flux SDK para Go

Cliente del **flux Event Protocol v1** — CloudEvents 1.0 sobre NATS JetStream.
Nivel de conformidad objetivo: **L2**.

El contrato normativo vive en [`specification/`](../specification/); si algo de este
README diverge de la spec, manda la spec.

```bash
go get github.com/charlessonamericantrading/flux/sdk-go
```

El paquete se llama `flux` aunque el último elemento de la ruta sea `sdk-go`:
Go no admite guiones en identificadores de paquete.

```go
import flux "github.com/charlessonamericantrading/flux/sdk-go"
```

Requiere Go 1.22+, `github.com/nats-io/nats.go` v1.37+ (paquete `jetstream`, no la
API legacy `nc.JetStream()`) y `github.com/google/uuid` v1.6+.

---

## Publicar

```go
bus, err := flux.Connect(ctx, flux.ConnectOptions{
    Servers:       "nats://localhost:4222",
    Service:       "pedidos-api",
    Environment:   "produccion",
    Version:       "3.4.1",
    TenantID:      "acme",
    SchemaBaseURL: "https://schemas.internal",
})
if err != nil { return err }
defer bus.Close()

_, err = bus.Publish(ctx, "pedidos.pedido.v1.creado", PedidoCreado{
    PedidoID:         "ped-123",
    ClienteID:        "cli-987",
    AggregateVersion: 1,
    TotalCents:       9990,
    Moneda:           "EUR",
}, flux.WithAggregateID("ped-123"))
```

**Solo escribes subject, `data` y opcionalmente `AggregateID`.** El SDK rellena `id`
(UUIDv7), `source`, `time`, `specversion`, `type`, `dataschema`, `correlationid`,
`causationid`, `producerversion` y `traceparent`. Si tu código asigna alguno de esos
a mano, está mal — [01-envelope.md §5](../specification/01-envelope.md).

## Consumir

```go
sub, err := bus.Subscribe(ctx, "pedidos.pedido.v1.creado",
    func(ctx context.Context, ev flux.Event, d flux.Delivery) error {
        p, err := flux.UnmarshalData[PedidoCreado](ev)
        if err != nil { return err }          // PERMANENT → DLQ inmediato

        if yaProcesado(ev.ID) { return nil }  // idempotencia: OBLIGATORIA
        if err := hacerElTrabajo(ctx, p); err != nil {
            return flux.NewRetryableError("proveedor caído", flux.WithCause(err))
        }
        return nil                            // return nil == ack explícito
    })
defer sub.Unsubscribe()
```

- **Devolver `nil` ACK-ea.** Devolver un error lo clasifica y produce `nak`, `term`
  o `term`+alerta.
- **Todo handler DEBE ser idempotente.** La garantía es *at-least-once*: los
  duplicados llegan, no son un fallo. Elige una de las tres estrategias de
  [03-delivery.md §4](../specification/03-delivery.md).
- **Nunca asumas orden.** Incluye `aggregateVersion` en `data` y filtra con
  `WHERE aggregate_version < $n`.

## Errores

```go
return flux.NewRetryableError("proveedor 503", flux.WithRetryAfter(5*time.Second))
return flux.NewPermanentError("pedido ya cancelado", flux.WithCode("PEDIDO_YA_CANCELADO"))
```

| Clase | Qué es | Acción |
|---|---|---|
| `ClassRetryable` | Timeout, ECONNRESET, HTTP 429/502/503/504 | `nak` + backoff |
| `ClassPermanent` | Falla el schema, regla de negocio, HTTP 400/403/404/422 | `term` + DLQ inmediato |
| `ClassPoison` | JSON malformado, falta atributo CloudEvents | `term` + DLQ + alerta |

**Default de lo desconocido: `RETRYABLE` con presupuesto acotado de 2 entregas**
([04-errors.md §2.1](../specification/04-errors.md)).

```
Error reconocido como transitorio (ECONNRESET, 503) → 6 entregas, hasta 51 min
Error desconocido                                   → 2 entregas, ~30 s
Error reconocido como permanente (400, 422)         → 1 entrega, sin espera
```

Las dos opciones obvias fallan cada una en un extremo: `UnknownPermanent` manda a la DLQ
un evento válido por un hipo de red y alguien lo reproduce a mano; `UnknownRetryable`
completo atasca la cola 51 minutos y el modo de fallo se amplifica con cada mensaje
siguiente. El acotado cuesta 30 segundos de latencia sobre los permanentes genuinos y
elimina ambos problemas — no es un punto medio, es estrictamente mejor.

El presupuesto **no** se configura en `max_deliver`: eso es por consumidor, no por
mensaje, y bajarlo a 2 recortaría también los reintentos de los `RETRYABLE` reconocidos.
El clasificador rellena `Classification.MaxAttempts` solo para los errores desconocidos
y el runtime aplica `min(max_deliver, MaxAttempts)` a ese error concreto. `MaxAttempts`
es un `int` con cero = "sin tope propio", igual convenio que `RetryAfter`.

```go
bus, err := flux.Connect(ctx, flux.ConnectOptions{
    // ...
    Classifier: flux.ClassifierOptions{
        UnknownErrorPolicy: flux.UnknownRetryableBounded, // o UnknownPermanent / UnknownRetryable
        UnknownRetryBudget: 2,
    },
})
```

Para que el clasificador reconozca un status HTTP, envuelve el fallo con
`flux.NewHTTPError(status, msg, retryAfter)` o implementa `flux.HTTPStatusError`.

---

## Diferencias con el SDK de referencia (Node)

El envelope, el naming, la taxonomía de errores y la config de consumidor son
**idénticos byte a byte**. Estas cinco divergencias son de lenguaje, no de contrato.

### 1. Contexto explícito en vez de `AsyncLocalStorage`

Node propaga `correlationid` y `traceparent` de forma implícita: un `publish()` en
cualquier punto de la pila de un handler hereda el contexto del evento entrante sin
que nadie pase nada. **Go no tiene equivalente** — no hay almacenamiento ligado al
goroutine, y emularlo por goroutine ID es un antipatrón que además se rompe en cuanto
el handler lanza un goroutine hijo.

Aquí el contexto viaja en el `context.Context` que el SDK entrega al handler:

```go
func(ctx context.Context, ev flux.Event, d flux.Delivery) error {
    // este ctx lleva dentro el contexto del evento entrante
    _, err := bus.Publish(ctx, "facturacion.factura.v1.emitida", payload)
    return err   // correlationid, causationid y traceparent se propagan solos
}
```

> ⚠️ **Si pasas `context.Background()` a `Publish` en lugar del `ctx` del handler, la
> cadena de correlación se rompe en silencio.** En Node eso no puede pasar. Es el
> precio de no tener magia; a cambio la propagación es visible en cada firma y por
> tanto auditable en una revisión de código.

### 2. `traceparent` inyectado, no autodetectado

Node hace un `import()` dinámico de `@opentelemetry/api` y falla en silencio si no
está. Go no tiene imports dinámicos y una dependencia dura obligaría a instalar
OpenTelemetry a todo servicio que use el SDK. Se invierte: la aplicación pasa
`ConnectOptions.Traceparent`. Con OTel es una línea (ver `context.go`).

### 3. Status HTTP por interfaz, no por reflexión

Node hurga en `err.status`, `err.statusCode` y `err.response.status` porque en JS
cualquier objeto puede tener cualquier propiedad. En Go el contrato es explícito:
`flux.HTTPStatusError`, localizado con `errors.As` — lo que además funciona a través
de errores envueltos con `%w`, cosa que el `instanceof` de Node no hace.

### 4. `return nil` en vez de `ctx.ack()`

El `ctx.ack()` de Node es un no-op (devolver del handler ya hace ack). Aquí
`return nil` **es** el ack explícito. El requisito del protocolo —nunca auto-ack— se
cumple igual: el SDK jamás confirma antes de que el handler termine.

Extra de Go: un pánico en el handler se convierte en `PERMANENT` en vez de matar el
proceso y con él las demás suscripciones.

### 5. Política de lo desconocido: tipo propio y cero como "sin tope"

Node usa una unión de literales (`"permanent" | "retryable" | "retryable-bounded"`) y un
`maxAttempts?: number` opcional. En Go la política es el tipo `UnknownPolicy` —no un
`ErrorClass`— porque "retryable acotado" no es una clase del protocolo y meterlo en
`ErrorClass` contaminaría el valor que acaba escrito en `dlqreason`.

`Classification.MaxAttempts` es un `int` y no un `*int`: cero no es un presupuesto válido
—un mensaje se entrega al menos una vez, así que el mínimo con sentido es 1— y el campo
vecino `RetryAfter` ya usa ese mismo convenio. Un puntero añadiría una asignación y un
alias mutable a un struct que se copia por valor en cada despacho, a cambio de distinguir
un caso que no existe. Los valores cero de `ClassifierOptions` siguen significando el
default de la spec: `retryable-bounded` con presupuesto 2.

---

## Fricciones: dónde el envelope no encaja limpio en un lenguaje de tipado estricto

Go es el SDK que valida el contrato de verdad. Lo que sigue son señales **sobre la
spec**, no sobre Go — y son las mismas que encontrarán Java y .NET.

### A. `omitempty` colapsa "cero" y "ausente"

El envelope distingue "atributo ausente" de "atributo presente con valor vacío"; los
tipos de valor de Go, C# y Java no. Concretamente:

- `dlqattempts` con `omitempty` **desaparecería si valiese 0**. Hoy es inocuo porque
  el mínimo legal es 1 —siempre hubo al menos una entrega— pero el envelope depende
  de esa coincidencia, no de una regla escrita.
- Un `AggregateID` de `""` se omite en vez de emitirse como `"subject": ""`. Es el
  comportamiento deseado, pero por accidente.

**Sugerencia para la spec:** declarar explícitamente que ningún atributo opcional
admite el valor vacío como significativo (`dlqattempts >= 1`, strings no vacíos), de
modo que `omitempty` sea correcto por contrato y no por suerte. Lo contrario obliga a
punteros (`*int`, `*string`) en tres lenguajes y ensucia toda la API.

### B. `Event` no es comparable con `==`

`data` es un objeto JSON arbitrario, así que se guarda como `json.RawMessage` (un
slice) y eso hace **todo el struct no comparable**. Se añadió `Event.Equal` para que
cada aplicación no acabe llamando a `reflect.DeepEqual`. En C# y Java el efecto
equivalente es que `record`/`equals` generados no sirven sobre el payload.

### C. `time` es `string`, no `time.Time`

El tipo natural sería `time.Time`, pero el marshaller de Go emite RFC3339**Nano**,
que recorta ceros finales: `…39.410Z` saldría como `…39.41Z`. Sigue siendo RFC 3339
válido, pero **deja de ser byte a byte igual** al `toISOString()` de Node. Como el
replay desde la DLQ exige preservar el evento verbatim, `time` se guarda como string
y se ofrece `Event.EventTime()`.

**Sugerencia para la spec:** fijar por escrito que la precisión es de **exactamente 3
decimales**, no "milisegundos". Sin ese "exactamente", cada SDK elige y los fixtures
de conformidad dejan de cruzarse.

### D. `subject` significa dos cosas y solo una encaja en el struct

Ya está avisado en la spec, pero en Go duele más: el campo se llama `AggregateID` y
lleva `json:"subject"`, así que el nombre del struct y el del JSON **no coinciden**
justo en el atributo más confundible del protocolo. Es la mejor solución disponible,
pero es un lugar donde un lector desprevenido se equivocará. Renombrar el atributo de
CloudEvents no es opción (lo fija CloudEvents 1.0); merece un test de conformidad
dedicado.

### E. `encoding/json` empareja campos SIN distinguir mayúsculas

Trampa específica de Go y muy peligrosa aquí: `{"ID": "..."}` poblaría `Event.ID` en
silencio, y `Id`, `iD` e `ID` acabarían siendo el mismo atributo — precisamente el
tipo de subject/atributo fantasma que la spec combate en NATS. **La regla de atributos
raíz cerrados de [01-envelope.md §3.3](../specification/01-envelope.md) tapa el
agujero por accidente**, porque las claves se comparan exactas contra
`AllowedRootAttributes` antes de decodificar. Vale la pena que la spec diga que la
comparación de nombres de atributo es **case-sensitive**: hoy se deduce, no se afirma.

### F. La lista de errores transitorios está escrita en códigos de Node

`protocol.json` lista `["ECONNRESET", …, "EAI_AGAIN"]`, que son códigos de libuv.
`EAI_AGAIN` **no existe como errno en Go**: se expone como `*net.DNSError` con
`IsTemporary`. Los demás sí existen en `syscall` y se comparan con `errors.Is`, que es
más robusto que comparar cadenas.

**Sugerencia para la spec:** describir esa lista por **semántica** ("fallo temporal de
resolución DNS", "conexión reiniciada por el par") con los códigos de cada plataforma
como ejemplo no normativo. Tal cual está, invita a cada SDK a hacer `strings.Contains`
sobre el mensaje de error.

### G. `AckWait == BackOff[0]` no puede expresarse en el sistema de tipos

Es la invariante más cara del protocolo y en los cuatro lenguajes acaba siendo dos
constantes que alguien debe mantener sincronizadas. Aquí se defiende con un test
(`TestInvarianteAckWaitIgualBackoffCero`) y con una comprobación en
`assertConfigHonored` que valida la invariante **sobre la config efectiva del
servidor**, no solo sobre la solicitada.

---

## Ficheros

| Fichero | Contenido |
|---|---|
| `protocol.go` | Constantes verificadas y naming (`ParseSubject`, `SubjectToType`, `StreamName`, `DurableName`, `DLQSubject`, `SourceURI`) |
| `envelope.go` | `Event`, `BuildEvent`, `Serialize`, `ParseEvent`, `UnmarshalData`, `ToDLQEvent`, `StripDLQExtensions` |
| `errors.go` | `ErrorClass`, `RetryableError`, `PermanentError`, `PoisonError`, `Classification` |
| `classify.go` | `NewClassifier`, `HTTPStatusError`, políticas configurables |
| `context.go` | Propagación explícita vía `context.Context` |
| `client.go` | `Connect`, `Bus.Publish`, `Bus.Subscribe`, `Bus.Close` |

## Desarrollo

```bash
go vet ./...
go test ./...
```

Los tests no requieren un broker: cubren naming, envelope y clasificación, que es
donde vive la semántica del protocolo. La conformidad contra un NATS real se verifica
con [`conformance/`](../conformance/).
