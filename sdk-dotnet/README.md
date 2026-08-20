# flux SDK para .NET

Cliente del **flux Event Protocol v1** — CloudEvents 1.0 sobre NATS JetStream.
Nivel de conformidad objetivo: **L2**.

El contrato normativo vive en [`specification/`](../specification/); si algo de este
README diverge de la spec, manda la spec.

```bash
dotnet add package Flux
```

Requiere **.NET 8** o superior, `NATS.Net` 2.5+ (el cliente oficial de segunda
generación: namespaces `NATS.Client.Core` / `NATS.Client.JetStream`, no el paquete
`NATS.Client` en mantenimiento) y `System.Text.Json`.

---

## Publicar

```csharp
using Flux;

await using var bus = await FluxBus.ConnectAsync(new ConnectOptions
{
    Servers       = "nats://localhost:4222",
    Service       = "pedidos-api",
    Environment   = "produccion",
    Version       = "3.4.1",
    TenantId      = "acme",
    SchemaBaseUrl = "https://schemas.internal",
});

await bus.PublishAsync(
    "pedidos.pedido.v1.creado",
    new
    {
        pedidoId         = "ped-123",
        clienteId        = "cli-987",
        aggregateVersion = 1,
        totalCents       = 9990,      // entero en la unidad mínima, nunca decimal ni float
        moneda           = "EUR",
    },
    new PublishOptions { AggregateId = "ped-123" });
```

**Solo escribes subject, `data` y opcionalmente `AggregateId`.** El SDK rellena `id`
(UUIDv7), `source`, `time`, `specversion`, `type`, `dataschema`, `correlationid`,
`causationid`, `producerversion` y `traceparent`. Si tu código asigna alguno de esos a
mano, está mal — [01-envelope.md §5](../specification/01-envelope.md).

> ⚠️ `AggregateId` es el atributo `subject` de CloudEvents (`"ped-123"`), **no** el
> subject de NATS. Son dos cosas distintas con el mismo nombre y confundirlas es el error
> más frecuente al adoptar CloudEvents sobre NATS.

## Consumir

```csharp
await using var suscripcion = await bus.SubscribeAsync(
    "pedidos.pedido.v1.creado",
    async (evento, entrega, ct) =>
    {
        var pedido = evento.DataAs<PedidoCreado>();   // desajuste de tipo → PERMANENT

        if (await YaProcesado(evento.Id, ct)) return; // idempotencia: OBLIGATORIA

        try
        {
            await HacerElTrabajo(pedido, ct);
        }
        catch (HttpRequestException e)
        {
            throw new RetryableException("proveedor caído", innerException: e);
        }

        await MarcarProcesado(evento.Id, ct);
        // volver del handler == ack explícito
    });
```

- **Volver del handler ACK-ea.** Lanzar clasifica el error y produce `nak`, `term` o
  `term`+alerta. El SDK **nunca** confirma antes de que el handler termine.
- **Todo handler DEBE ser idempotente.** La garantía es *at-least-once*: los duplicados
  llegan, no son un fallo. Elige una de las tres estrategias de
  [03-delivery.md §4](../specification/03-delivery.md).
- **Nunca asumas orden.** Incluye `aggregateVersion` en `data` y filtra con
  `WHERE aggregate_version < $n`.
- Un `PublishAsync` desde dentro del handler **hereda solo** `correlationid`,
  `causationid`, `tenantid` y `traceparent`. No hay nada que pasar: ver §"Contexto".

## Errores

```csharp
throw new RetryableException("proveedor 503", retryAfter: TimeSpan.FromSeconds(5));
throw new PermanentException("pedido ya cancelado", code: "PEDIDO_YA_CANCELADO");
```

| Clase | Qué es | Acción |
|---|---|---|
| `ErrorClass.Retryable` | Timeout, ECONNRESET, HTTP 429/502/503/504 | `nak` + backoff |
| `ErrorClass.Permanent` | Falla el schema, regla de negocio, HTTP 400/403/404/422 | `term` + DLQ inmediato |
| `ErrorClass.Poison` | JSON malformado, falta atributo CloudEvents | `term` + DLQ + alerta |

**Default de lo desconocido: `RETRYABLE` con presupuesto acotado de 2 entregas**
([04-errors.md §2.1](../specification/04-errors.md)).

```
Error reconocido como transitorio (ECONNRESET, 503) → 6 entregas, hasta 51 min
Error desconocido                                   → 2 entregas, ~30 s
Error reconocido como permanente (400, 422)         → 1 entrega, sin espera
```

Las dos opciones obvias fallan cada una en un extremo: `Permanent` manda a la DLQ un
evento válido por un hipo de red y alguien lo reproduce a mano; `Retryable` completo
atasca la cola 51 minutos y el modo de fallo se amplifica con cada mensaje siguiente. El
acotado cuesta 30 segundos de latencia sobre los permanentes genuinos y elimina ambos
problemas — no es un punto medio, es estrictamente mejor.

El presupuesto **no** se configura en `max_deliver`: eso es por consumidor, no por
mensaje, y bajarlo a 2 recortaría también los reintentos de los `RETRYABLE` reconocidos.
El clasificador rellena `Classification.MaxAttempts` solo para los errores desconocidos y
el runtime aplica `Classifier.EffectiveBudget(max_deliver, clasificación)` a ese error
concreto.

```csharp
Classifier = new ClassifierOptions
{
    UnknownErrorPolicy = UnknownPolicy.RetryableBounded, // o Permanent / Retryable
    UnknownRetryBudget = 2,
    TimeoutPolicy      = ErrorClass.Retryable,
    Rules = new Func<Exception, Classification?>[]
    {
        e => e is NpgsqlException { SqlState: "40P01" }   // deadlock
            ? new Classification(ErrorClass.Retryable, "DEADLOCK")
            : null,
    },
}
```

Para que el clasificador reconozca un status HTTP no hace falta hacer nada: lee
`HttpRequestException.StatusCode`, que rellena `HttpClient` desde .NET 5. Si envuelves los
fallos de tu cliente en un tipo propio, implementa `IHttpStatusError` (o usa
`HttpStatusException`).

## Contexto

`correlationid`, `causationid`, `tenantid` y `traceparent` se propagan solos a través de
`AsyncLocal<T>` — el equivalente exacto del `AsyncLocalStorage` de Node. Un
`PublishAsync` en cualquier punto de la pila de llamadas de un handler hereda el contexto
del evento entrante sin que nadie pase nada por parámetro, y eso incluye continuaciones de
`await`, `Task.Run` y tareas hijas.

Para reanudar una cadena de correlación desde un trabajo diferido (un `BackgroundService`
que recoge trabajo de una tabla, por ejemplo):

```csharp
using (FluxContext.Push(new EventContext { CorrelationId = idDelFlujo }))
{
    await bus.PublishAsync("facturacion.factura.v1.emitida", payload);
}
```

El `traceparent` sale de `Activity.Current` cuando su formato es W3C, que es el default
desde .NET 5. Con OpenTelemetry, ASP.NET Core o `HttpClient` instrumentados no hay que
configurar nada.

---

## Diferencias con el SDK de referencia (Node)

El envelope, el naming, la taxonomía de errores y la config de consumidor son **idénticos
byte a byte**. Estas divergencias son de lenguaje, no de contrato.

### 1. El contexto se propaga solo — y aquí sí

`AsyncLocal<T>` reproduce el `AsyncLocalStorage` de Node **exactamente**. Éste es el punto
donde .NET se parece a Node y no a Go: en Go no hay almacenamiento ligado a la goroutine,
así que el SDK se vio obligado a pasar el contexto explícito en el `context.Context`, con
la consecuencia —documentada en su README— de que pasar `context.Background()` a `Publish`
rompe la cadena de correlación en silencio. Aquí eso no puede pasar.

El precio es el mismo que en Node: la propagación no se ve en ninguna firma, así que no se
audita leyendo el código. A cambio no se puede romper por olvido.

### 2. `traceparent` autodetectado sin dependencias

Node hace un `import()` dinámico de `@opentelemetry/api` y falla en silencio si no está.
Go invierte la responsabilidad y pide que la aplicación inyecte una función. .NET no
necesita ninguna de las dos cosas: el trace context W3C vive en el BCL
(`System.Diagnostics.Activity`), que es lo que instrumentan OpenTelemetry, ASP.NET Core y
`HttpClient`. `FluxContext.ActiveTraceparent()` lo lee de ahí, y comprueba que el formato
sea W3C antes de emitirlo — un `Activity.Id` jerárquico no es un `traceparent` válido y
emitirlo produciría un atributo sintácticamente roto.

`ConnectOptions.TraceparentProvider` sigue existiendo para quien propague el trace por otro
canal.

### 3. Status HTTP del BCL, no por reflexión ni por interfaz

Node hurga en `err.status`, `err.statusCode` y `err.response.status` porque en JS cualquier
objeto puede tener cualquier propiedad. Go define la interfaz `HTTPStatusError` y obliga a
implementarla. .NET tiene `HttpRequestException.StatusCode` en el BCL desde .NET 5, así que
la mayoría de aplicaciones no implementan nada. `IHttpStatusError` existe para quien
envuelve los fallos en un tipo propio.

Además, el clasificador recorre `InnerException` **y** las ramas de `AggregateException`
—que es como llegan los fallos de un `Task.WhenAll`—, cosa que el `instanceof` de Node no
hace.

### 4. Volver del handler en vez de `ctx.ack()`

El `ctx.ack()` de Node es un no-op (volver del handler ya hace ack). Aquí, como en Go,
volver **es** el ack explícito. El requisito del protocolo —nunca auto-ack— se cumple
igual.

### 5. Presupuesto de lo desconocido: `int` con cero, `TimeSpan?` sin cero

`Classification.MaxAttempts` es `int` con el convenio "0 = sin tope propio", igual que Go:
un presupuesto de 0 entregas no existe —todo mensaje se entrega al menos una vez— así que
el cero queda libre sin colapsar ningún valor legítimo.

`Classification.RetryAfter` **sí** es `TimeSpan?`, a diferencia de Go. Ahí el convenio del
cero sería un colapso real: "no me han dicho cuánto esperar" y "reintenta ya mismo" son dos
cosas distintas y ambas tienen sentido. Go paga ese colapso porque no tiene opcionales
baratos; en C# no hay motivo para pagarlo.

### 6. La verificación de config del consumidor no conoce NATS

En Node y Go, `assertConfigHonored` habla directamente con los tipos del cliente, así que
solo se ejerce con un broker delante. Aquí la comprobación trabaja sobre
`ConsumerConfigSnapshot`, un POCO del BCL, y `FluxBus` traduce la config del servidor a él.
El requisito L2 más importante —detectar que JetStream ha sobrescrito `ack_wait`— tiene
tests unitarios que corren sin Docker.

---

## Fricciones: dónde el protocolo no encaja limpio en C#/.NET

Esto es señal **sobre la spec**, no sobre .NET. Las letras A–G se corresponden con las del
README de Go, para poder leerlas en paralelo; H–K son nuevas y específicas de este port.

### A. "Ausente ≠ vacío" obliga a anulables — y la spec cita a System.Text.Json por su nombre

[01-envelope.md §3.3](../specification/01-envelope.md) prohíbe que un opcional use un valor
vacío con significado propio, y avisa de que `omitempty` de Go y el default de
`System.Text.Json` colapsan cero y ausente. En este SDK eso se traduce en:

- `JsonIgnoreCondition.WhenWritingNull` y **nunca** `WhenWritingDefault`. Con la segunda,
  un `dlqattempts` de `0` desaparecería del JSON.
- `int?` y nunca `int` en `dlqattempts`. Hay un test dedicado
  (`UnDlqAttemptsDeCeroNoDesaparece`) porque es un bug que hoy sería **invisible**: el
  mínimo legal de `dlqattempts` resulta ser 1, así que el envelope depende de una
  coincidencia y no de una regla.

**Sugerencia para la spec:** declarar explícitamente que ningún atributo opcional admite el
valor vacío como significativo (`dlqattempts >= 1`, strings no vacíos), de modo que la
omisión por defecto sea correcta *por contrato* y no por suerte.

### B. `record` no arregla la igualdad del payload — al contrario que en Java

Java se libra de la fricción de Go porque `JsonNode.equals` es estructural y profundo. C#
**no**: `JsonElement` es un struct cuya igualdad por defecto compara la referencia al
`JsonDocument` subyacente y el índice del token, así que el `Equals` que genera el `record`
diría que dos eventos con exactamente el mismo JSON son distintos —y sin ruido, sin
warning, sin nada—. `FluxEvent.Equals` está escrito a mano por eso.

Es el mismo problema que en Go (`json.RawMessage` es un slice y hace el struct no
comparable), pero **peor**: en Go el compilador te lo impide; en C# compila y miente.

### C. `time` es `string`, no `DateTimeOffset`

`DateTimeOffset.ToString("O")` emite **siete** decimales y offset
(`2025-08-20T10:25:39.4100000+00:00`). Es RFC 3339 válido y no sirve. El formato es fijo:
`"yyyy-MM-dd'T'HH:mm:ss.fff'Z'"` con `CultureInfo.InvariantCulture` en UTC, `.fff` que
**trunca** (no redondea), y `T`/`Z` entrecomilladas para no depender de cómo lea el
compilador de formatos un carácter que no es un especificador.

Que la spec diga "exactamente 3 decimales" y no "precisión de milisegundos" es lo que hace
esto verificable. Sin ese "exactamente", .NET habría elegido `"O"` —que es el formato
*recomendado* por la documentación de .NET para round-trip— y el envelope habría dejado de
cruzarse con el de Node.

### D. `subject` significa dos cosas y solo una encaja en el modelo

La propiedad se llama `AggregateId` y lleva `[JsonPropertyName("subject")]`, así que el
nombre del modelo y el del cable **no coinciden** justo en el atributo más confundible del
protocolo. Igual que en Go y Java. Es la mejor solución disponible y sigue siendo un sitio
donde un lector desprevenido se equivocará.

### E. La comparación case-insensitive es un default que hay que apagar dos veces

Go tiene el problema en `encoding/json` por defecto. .NET lo tiene **a medias**, y eso es
más peligroso:

- `JsonSerializerDefaults.General` → `PropertyNameCaseInsensitive = false`. Correcto.
- `JsonSerializerDefaults.Web` → `PropertyNameCaseInsensitive = **true**`. Y `Web` es lo
  que inyecta ASP.NET Core en todas partes y lo que un desarrollador copia por costumbre.

Es decir: el fantasma de `{"ID": …}` no está encendido por defecto, pero está a una línea de
distancia y esa línea es la que la gente escribe sin pensar. Aquí se fija explícito, hay un
test que lo afirma (`LasOpcionesDeJsonSonCaseSensitivePorContrato`), y la regla de atributos
raíz cerrados se comprueba con `StringComparer.Ordinal` **antes** de deserializar.

### F. La lista de transitorios está escrita en códigos de libuv — y .NET ya la resolvió

`protocol.json` lista `["ECONNRESET", …, "EAI_AGAIN"]`. En .NET no existe ninguno de esos
identificadores: existe `SocketError`, un enum que el BCL **ya normaliza** entre Windows y
Unix. El bug real que la spec documenta —el port literal de la lista de Node clasificaba el
mismo corte de red como PERMANENT en Windows (`WSAECONNRESET`) y RETRYABLE en Linux— es
imposible aquí, porque `SocketError.ConnectionReset` es el mismo valor en las dos
plataformas.

Este SDK emite igualmente el nombre POSIX (`ECONNRESET`) como `Classification.Code` para
que las métricas de un consumidor .NET se agreguen con las de uno de Node o Go: es el mismo
hecho operativo y merece la misma etiqueta.

**Sugerencia para la spec:** la §1.1 de 04-errors.md ya describe las categorías por
semántica; convendría que `protocol.json` marcase `syscallCodes` como *ejemplo no
normativo* en el propio nombre del campo (`syscallCodesExample`), porque tal cual está
invita a que alguien lo trate como la lista.

### G. `ack_wait == backoff[0]` sigue sin poder expresarse en el sistema de tipos

Cuarto lenguaje, mismo resultado: es la invariante más cara del protocolo y acaba siendo
dos constantes que alguien debe mantener sincronizadas. Aquí se defiende en tres sitios —un
constructor estático que impide cargar la clase si se rompe (igual que el bloque `static`
de Java), un test, y la comprobación sobre la config **efectiva** del servidor.

### H. La spec llama obligatorias a cuatro extensiones que el parser de referencia no exige

Ésta es nueva y es la más incómoda del port.

[01-envelope.md §3.1](../specification/01-envelope.md) declara `correlationid`, `tenantid`,
`producerversion` y `dataclassification` **obligatorias**. Pero la lista de atributos que el
parser trata como obligatorios —en Node, en Python y en Go— son solo los ocho del núcleo
CloudEvents. Un evento sin `tenantid` **no es POISON** para los tres SDKs existentes: llega
al handler con el valor cero del lenguaje.

En un lenguaje con anulables eso obliga a elegir:

1. Marcarlas `required` y que System.Text.Json lance al deserializar → .NET clasificaría
   como POISON mensajes que los otros tres entregan. **Divergencia de comportamiento en un
   protocolo polyglot.**
2. Declararlas anulables → `evento.TenantId` es `string?` y cada consumidor escribe `!`.
3. Darles `= ""` por defecto → colapsa "ausente" y "vacío", que es justo lo que prohíbe
   §3.3.

Se ha elegido (2): compatibilidad por encima de ergonomía, con el `?` como cicatriz
visible. Java tomó la misma decisión (`Integer` en vez de `int`) por el mismo motivo.

**Sugerencia para la spec:** decidir explícitamente si la ausencia de una extensión
obligatoria es POISON. Si lo es, añadirlas a la lista del parser en los cuatro SDKs a la
vez. Si no lo es, decir en §3.1 que son obligatorias **al publicar** pero tolerables **al
consumir**, y entonces la nulabilidad de este SDK deja de ser una cicatriz y pasa a ser el
tipo correcto.

### I. `dataclassification` es un enum cerrado con un valor ausente representable

Consecuencia de (H) y del hecho de que la spec cierre el conjunto a cuatro valores: la
propiedad es `DataClassification?`. Un valor **fuera** del enum (por ejemplo
`"Confidential"` con mayúscula) sí es POISON, porque el conversor lanza — que es lo
correcto y coincide con Java. Pero un valor **ausente** no lo es. Las dos reglas conviven
en el mismo atributo y no hay forma de que el sistema de tipos las exprese juntas.

### J. El encoder por defecto de System.Text.Json rompe el envelope en español

`JsonSerializerOptions` escapa por defecto `<`, `>`, `&`, `'`, `+` **y todo lo que no sea
ASCII**: una `é` sale como `é`. Node emite el carácter tal cual y Go llama a
`SetEscapeHTML(false)` por la misma razón. Sin
`JavaScriptEncoder.UnsafeRelaxedJsonEscaping`, **cualquier payload con una tilde** produce
un envelope distinto byte a byte del de los otros SDKs, y el replay verbatim y la firma de
fase 4 dejan de funcionar.

Es una trampa gratuita y no aparece en ninguna parte de la spec, porque los tres SDKs
existentes no la tienen. Merece una línea en 01-envelope.md §1: *"el JSON se emite en UTF-8
sin escapes `\uXXXX` para caracteres imprimibles"*.

### K. UUIDv7 no existe en .NET 8

`Guid.CreateVersion7()` llega en .NET 9. En .NET 8 —la LTS vigente— hay que implementarlo:
48 bits de milisegundos Unix, versión 7, `rand_a` usado como **contador** dentro del mismo
milisegundo (método 2 de la RFC 9562, sin el cual una ráfaga rompe la monotonía justo
cuando más se usa) y `rand_b` aleatorio.

Detalle que muerde: el constructor `Guid(byte[])` interpreta los tres primeros campos en
**little-endian**, así que un array en orden RFC produce un texto con el timestamp
invertido y el `id` deja de ser ordenable como cadena — que es la única propiedad por la
que la spec pide v7 y no v4 ([01-envelope.md §2.4](../specification/01-envelope.md)). Aquí
el `Guid` se construye por componentes para evitarlo.

Cuando el consumidor mueva el TFM a `net9.0` puede sustituirse por la API del framework sin
cambiar un byte del formato.

---

## Ficheros

| Fichero | Contenido |
|---|---|
| `Protocol.cs` | Constantes verificadas y naming (`ParseSubject`, `SubjectToType`, `StreamName`, `DurableName`, `DlqSubject`, `SourceUri`, `NewEventId`) |
| `FluxEvent.cs` | El envelope tipado, `DataClassification`, `DlqReason` y sus conversores |
| `Envelope.cs` | `BuildEvent`, `Serialize`, `ParseEvent`, `DataAs`, `ToDlqEvent`, `StripDlqExtensions`, `FormatTime` |
| `ErrorClass.cs` | Las tres clases del protocolo |
| `FluxExceptions.cs` | `RetryableException`, `PermanentException`, `PoisonException`, `EnvelopeException`, `Classification` |
| `Classifier.cs` | Políticas configurables, transitorios por semántica, `EffectiveBudget` |
| `EventContext.cs` | Propagación implícita vía `AsyncLocal<T>` y `Activity.Current` |
| `ConsumerConfigMismatchException.cs` | Verificación L2 de la config efectiva, sin tipos de NATS |
| `FluxBus.cs` | `ConnectAsync`, `PublishAsync`, `SubscribeAsync`, `DisposeAsync`. **Único fichero que conoce NATS** |

## Desarrollo

```bash
dotnet build
dotnet test
```

Los tests no requieren un broker: cubren naming, envelope, clasificación, contexto y la
verificación de config de consumidor, que es donde vive la semántica del protocolo. La
conformidad contra un NATS real se verifica con [`conformance/`](../conformance/).

## Robustez del bucle de consumo

Dos bugs reales encontrados en el SDK de Node y corregidos aquí desde el primer día:

1. **Si la publicación en la DLQ falla, NO se hace `Term()`.** Terminar el mensaje borraría
   el evento sin haberlo guardado en ningún sitio. Se deja sin resolver para que JetStream
   lo reentregue: reprocesar un evento es preferible a perderlo sin rastro, y la causa suele
   ser un problema del broker, no del evento.
2. **Un fallo inesperado del despacho no mata el bucle.** Sin el `catch` que envuelve cada
   iteración, una excepción rompería el `await foreach` y el consumidor dejaría de consumir
   **en silencio**: sin log, y con `Connected == true`, así que el healthcheck seguiría
   diciendo que todo va bien. Un consumidor muerto que parece vivo es peor que uno que se
   cae. Si el bucle entero muere de todas formas, se registra en alto y con mayúsculas.
