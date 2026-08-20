# flux SDK para .NET

Cliente del **flux Event Protocol v1** — CloudEvents 1.0 sobre NATS JetStream.
Nivel de conformidad: **L3**, con la validación de esquema **opt-in**; sin activarla, el
SDK se comporta exactamente como en L2 y no instala nada de más.

El contrato normativo vive en [`specification/`](../specification/); si algo de este
README diverge de la spec, manda la spec.

```bash
dotnet add package Flux
dotnet add package Flux.Signing      # solo si vas a firmar o verificar eventos
dotnet add package Flux.Validation   # solo si vas a validar contra JSON Schema (L3)
```

Requiere **.NET 8** o superior, `NATS.Net` 2.5+ (el cliente oficial de segunda
generación: namespaces `NATS.Client.Core` / `NATS.Client.JetStream`, no el paquete
`NATS.Client` en mantenimiento) y `System.Text.Json`.

> **`Flux.Signing` es un paquete aparte, y es opt-in.** Ed25519 **no existe en la BCL de
> .NET 8**, así que la firma —una extensión *opcional* del protocolo cuyo default es
> `off`— necesita una librería de criptografía. Meterla en el paquete base castigaría a
> todo servicio que solo quiere publicar un evento sin firmar. Ver §"Firma de eventos".

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

> ⚠️ **`RetryAfter` es una sugerencia para el PRIMER reintento, y solo para él.** Con
> `backoff` configurado —y flux lo configura siempre— JetStream honra el delay del `nak` en
> la primera reentrega y a partir de la segunda impone el array `backoff`, **sin devolver
> error ni avisar** ([03-delivery.md §2.2](../specification/03-delivery.md), medido contra
> NATS 2.14.5):
>
> ```
> SIN backoff:  0ms → 300ms → 600ms  → 900ms     ← el delay se honra siempre
> CON backoff:  0ms → 300ms → 5300ms → 15300ms   ← solo la primera vez
> ```
>
> Un `Retry-After: 5` de un proveedor acorta el primer reintento y nada más: los siguientes
> seguirán 1 m, 5 m, 15 m, 30 m. No construyas lógica que dependa de que se respete más
> allá de la primera vez.

## Validación L3 (paquete `Flux.Validation`, opcional)

Nivel **L3** de [00-protocol.md §5](../specification/00-protocol.md): el SDK resuelve y
valida el `dataschema` **antes de publicar**, y `PublishAsync` **falla** si el payload no
cumple su propio esquema.

Sin esto, un productor puede publicar un payload que viola su contrato y nadie se entera
hasta que un consumidor —posiblemente de otro equipo, otro lenguaje y otra semana— se
atraganta: el error aparece lejísimos de su causa. Validar al publicar lo convierte en un
fallo del servicio que lo provocó.

```csharp
using Flux;

await using var bus = await FluxBus.ConnectAsync(new ConnectOptions
{
    // …
    Validation = new ValidationOptions
    {
        Mode      = ValidationMode.Strict,                         // Off (default) | Warn | Strict
        Bundle    = SchemaBundle.FromFile("schemas/bundle.json"),
        OnConsume = true,                                          // opcional
    }.WithSchemaValidator(),                                       // ← lo aporta Flux.Validation
});
```

| Modo | Al publicar | Cuándo |
|---|---|---|
| `Off` (default) | no valida | L2. Coste cero: ni se compila un esquema. |
| `Warn` | registra en `ConnectOptions.Logger` y publica | Introducir L3 en un ecosistema en marcha sin romper a nadie el primer día. |
| `Strict` | **lanza `SchemaValidationException`** | L3 de verdad. |

- **Reporta TODOS los errores**, no solo el primero (`SchemaValidationException.Errors`).
  De uno en uno, arreglar un payload con tres campos mal cuesta tres despliegues.
- `OnConsume = true` valida también al consumir. Un fallo ahí es **PERMANENT**, no
  RETRYABLE: el evento es sintácticamente correcto pero incumple su contrato, y reintentarlo
  seis veces da el mismo resultado bloqueando la cola 51 minutos para nada.
- La métrica lo etiqueta `outcome="invalid_schema"` —al publicar y al consumir— mientras que
  el `dlqreason` del evento sigue siendo `permanent`, que es el enum cerrado de
  [04-errors.md §1](../specification/04-errors.md).
- Pasar el `Bundle` **también resuelve el `dataschema` exacto** de cada subject: el MINOR
  real, no el `.0.0` que se asume sin él.
- Con `Mode` distinto de `Off` y sin `Validator`, `ConnectAsync` **falla al arrancar**
  diciendo qué paquete falta. Descubrir en la primera publicación que la validación que
  creías tener encendida no existía es peor que no tenerla.

### El bundle es un dato, no una descarga

El `dataschema` es una URI, pero un SDK L3 **NO DEBE resolverla por red** al publicar
(00-protocol.md §5). Validar está en la ruta caliente —una petición por evento es
inaceptable— y una caché con TTL abre una ventana en la que dos servicios validan contra
versiones distintas del mismo esquema: eso no produce un error, produce dos verdades.

Los esquemas se empaquetan con `node scripts/bundle-schemas.mjs` y se despliegan **con el
servicio**, así que la versión del esquema queda clavada a la versión del servicio — que es
justo lo que `producerversion` promete poder acotar.

`SchemaValidator` no descarga nada: registra los esquemas del bundle en un `SchemaRegistry`
**local** (no el global de la librería, para que dos buses del mismo proceso no se pisen) y
deja el `Fetch` de JsonSchema.Net como viene, que por defecto devuelve `null` en vez de
bajar la URI. Un `$ref` a algo que no está en el bundle es un error explícito en el
arranque, no una petición HTTP silenciosa.

### Por qué es un paquete aparte, y qué librería

**`System.Text.Json` no valida JSON Schema.** Trae `JsonDocument`, `JsonNode` y
serialización, pero no hay evaluador de esquemas en la BCL de .NET 8: hace falta un paquete.
Es la misma situación que con Ed25519 (§"Firma"), y se resuelve igual — con un paquete
opt-in, para que el paquete `Flux` siga dependiendo solo de `NATS.Net` y `System.Text.Json`.

Se elige **`JsonSchema.Net` 8.0.5** (json-everything), la implementación de referencia de
2020-12 en .NET. Las alternativas no sirven aquí:

- **NJsonSchema** está anclado en draft-07. Y el fallo no se manifiesta como "versión no
  soportada": se manifiesta como *no encuentro el esquema `.../2020-12/schema`*, que manda
  al operador a buscar un fichero que no existe. La spec avisa de esta trampa por su nombre.
- **Corvus.JsonSchema** es un **generador de código**: exige los esquemas en tiempo de
  compilación, lo que hace imposible el bundle desplegado con el servicio.

⚠️ **Versión clavada a 8.0.5, y no por costumbre.** La serie **9.x cambia la licencia del
binario**: el código sigue siendo MIT, pero los paquetes 9.x se publican en NuGet bajo el
*Open Source Maintenance Fee* EULA, que cobra una cuota mensual a quien los use en actividad
con ingresos ≥ 10.000 USD anuales. 8.0.5 es la última con licencia MIT limpia en el paquete.
Un SDK de protocolo no debe imponer una obligación de pago a quien solo activa la validación;
quien quiera la 9.x puede subirla en su propio proyecto a sabiendas.

**Coste real de la dependencia** (lo que entra al añadir `Flux.Validation`):

```
JsonSchema.Net 8.0.5 → JsonPointer.Net 6.0.1 → Json.More.Net 2.2.0
                                             → Humanizer.Core 3.0.1
```

Cuatro paquetes, uno de ellos Humanizer. Por eso están aquí y no en `Flux`: un servicio en
L2 no debe pagar nada de esto.

> **Divergencia con Java, y es del ecosistema, no del port.** Allí la validación L3 es una
> `<dependency>` marcada `<optional>true</optional>` dentro del **mismo** artefacto: Maven
> permite que una dependencia no se propague a quien consume la librería. NuGet no tiene
> equivalente —toda `PackageReference` es transitiva—, así que en .NET "opcional" solo puede
> expresarse partiendo el paquete. Es la misma razón por la que `Flux.Signing` existe.

## Firma de eventos (paquete `Flux.Signing`, opcional)

Extensión **opcional** de v1 — [07-signing.md](../specification/07-signing.md). El default
es `off` y **un evento sin firma sigue siendo válido**.

```csharp
using Flux;

var par = Ed25519Signing.GenerateKeyPair();   // PKCS#8 + SPKI en PEM

var (signer, verifier) = Ed25519Signing.Create(new SigningOptions
{
    PrivateKeyPem = par.PrivateKeyPem,        // omitir para solo verificar
    KeyId         = "pedidos-api-1",
    PublicKeys    = new Dictionary<string, string> { ["pedidos-api-1"] = par.PublicKeyPem },
    Verify        = VerificationMode.Require,
});

await using var bus = await FluxBus.ConnectAsync(new ConnectOptions
{
    // …
    Signer   = signer,
    Verifier = verifier,
});
```

### Por qué es un paquete aparte, y qué librería

**Ed25519 no está en la BCL de .NET 8.** `System.Security.Cryptography` trae `RSA`,
`ECDsa` y `ECDiffieHellman`, pero no EdDSA; `ECCurve` modela curvas de Weierstrass y
Ed25519 es de Edwards retorcida, así que ni siquiera cabe como "una curva más". Es la única
diferencia de plataforma real de esta fase: en Node está en `node:crypto`, en Java en
`java.security` desde el JDK 15, en Go en `crypto/ed25519`, y **aquí hace falta un
paquete**.

De los dos candidatos serios se elige **`BouncyCastle.Cryptography` 2.4.0** sobre
`NSec.Cryptography`, y la razón es operativa, no de API:

| | BouncyCastle | NSec |
|---|---|---|
| Implementación | 100 % código gestionado | envoltorio de **libsodium** (nativo) |
| RIDs exóticos (Alpine/musl, ARM64, distroless) | funciona | depende de que el RID traiga la libsodium correcta |
| Single-file / AOT | sin colocar binarios a mano | hay que gestionar el asset nativo |
| Modo de fallo si falta el binario | no aplica | **`DllNotFoundException` en EJECUCIÓN**, la primera vez que alguien firma |
| Rendimiento | menor | mayor (C) |

NSec es más rápido. Da igual: se firma una vez por evento sobre unos cientos de bytes, y
eso no es la ruta caliente de nada. Lo que sí importa es el modo de fallo — un
`DllNotFoundException` en el primer `publish()` de producción es exactamente la forma de
error que este protocolo lleva tres documentos evitando: el servicio arranca, el
healthcheck dice que todo va bien, y se rompe cuando importa.

La versión está **fijada** (`2.4.0`, no un rango): una librería de criptografía que se
actualiza sola en un `restore` es un cambio de comportamiento que nadie ha revisado.

### Cómo queda repartido

| Paquete | Qué lleva | Dependencias |
|---|---|---|
| `Flux` | `IEventSigner`, `IEventVerifier`, `VerificationMode`, `EventSigning.SignablePayload`, los códigos POISON | NATS.Net, System.Text.Json |
| `Flux.Signing` | `Ed25519Signing`, `SigningOptions` — la única criptografía del SDK | `Flux` + BouncyCastle |

Es decir: el paquete base define **el contrato y la política**; el de firma aporta **solo la
primitiva**. Un servicio que no firma no instala BouncyCastle, y aun así puede *leer* un
evento firmado: `signkeyid` y `signature` son atributos raíz válidos con la verificación
apagada. Si no lo fueran, adoptar la firma de forma gradual convertiría en POISON los
eventos de los productores ya migrados.

### Política

| Modo | Evento sin firma | Firma inválida |
|---|---|---|
| `Off` (default) | se acepta | se acepta (no se mira) |
| `Warn` | se registra y se acepta | se registra y se acepta |
| `Require` | **POISON** `MISSING_SIGNATURE` | **POISON** `INVALID_SIGNATURE` / `UNKNOWN_SIGNING_KEY` |

`Warn` existe porque adoptar la firma en un ecosistema en marcha exige un periodo en el que
unos productores firman y otros no. Pasar directo a `Require` convierte en POISON todo
evento de un servicio aún no migrado.

Tres cosas que conviene tener claras:

- **`signkeyid` va dentro de lo firmado.** Si quedara fuera, un atacante lo cambiaría por el
  id de una clave suya y la firma seguiría "verificando".
- **Una clave RETIRADA sigue verificando** mientras se conserve su pública. Retirarla impide
  *emitir* con ella, no *verificar* lo ya emitido; tratarla como inválida convierte una
  rotación rutinaria en la invalidación retroactiva de todo el historial. Mínimo de
  retención: **90 días**, la de la DLQ.
- **La firma sobrevive a la DLQ y al replay.** Las extensiones `dlq*` se añaden después de
  firmar y se quitan antes de verificar, así que un evento reproducido conserva su firma
  válida — que es lo correcto: el replay redistribuye un hecho ya emitido.

Lo que **no** resuelve: confidencialidad, replay legítimo, autenticación del broker, ni las
ACLs. La ACL controla **quién puede escribir**; la firma, **quién lo escribió**.

## Métricas

Normativo para L2 — [08-observability.md](../specification/08-observability.md). Los
nombres y las etiquetas son **contrato entre SDKs**: si .NET y Go nombran distinto la tasa
de DLQ, no se pueden sumar y un panel del ecosistema es imposible.

```csharp
var metrics = new InMemoryMetrics();
await using var bus = await FluxBus.ConnectAsync(new ConnectOptions { /* … */ Metrics = metrics });

app.MapGet("/metrics", () => Results.Text(metrics.Render(), "text/plain; version=0.0.4"));
```

El default es `NoMetrics.Instance` (no-op): un SDK de protocolo no impone un backend de
métricas. Para enchufar `System.Diagnostics.Metrics`, prometheus-net u OpenTelemetry,
implementa `IMetricsSink`.

| Métrica | Tipo | Etiquetas |
|---|---|---|
| `flux_events_published_total` | Counter | `subject`, `outcome` |
| `flux_events_consumed_total` | Counter | `subject`, `consumer`, `outcome` |
| `flux_event_handler_duration_seconds` | Histogram | `subject`, `consumer` |
| `flux_events_dlq_total` | Counter | `subject`, `consumer`, `reason`, `code` |
| `flux_events_retried_total` | Counter | `subject`, `consumer`, `attempt` |
| `flux_consumer_pending` | Gauge | `subject`, `consumer` |
| `flux_connection_state` | Gauge | — |

- **`IMetricsSink` tiene un método por métrica con parámetros nombrados, no un
  `IDictionary<string,string>` genérico.** Es deliberado: un diccionario de etiquetas es
  justo por donde se cuela un `tenantid` que multiplica las series temporales. La
  cardinalidad no avisa — funciona con tres tenants en desarrollo y mata a Prometheus con
  diez mil en producción. Etiquetar por `tenantid`, `id` o `correlationid` está
  **prohibido**; para eso están las trazas
  ([§2.2](../specification/08-observability.md)).
- **El último bucket del histograma es `30` porque *es* el `ack_wait`.** Un handler que cae
  ahí está a punto de que su mensaje se reentregue mientras aún se ejecuta. Hay un test que
  lo ata a `Protocol.DefaultAckWait`: cambiar uno sin el otro rompe la suite.
- **`flux_consumer_pending` tiene DOS fuentes y el SDK usa las dos**
  ([§2.3](../specification/08-observability.md)), porque cada una falla justo donde la otra
  sirve:
  1. **Los metadatos** del mensaje entregado (`NumPending`) — gratis y frescos en cada
     evento, pero **fallan cuando no llegan mensajes**, que es el caso que importa: si el
     bucle del consumidor muere, el gauge se queda plano en su último valor y el panel
     muestra una línea horizontal, indistinguible de "no pasa nada".
  2. **El sondeo periódico** de `num_pending` al servidor, cada
     `ConnectOptions.PendingPollInterval` (15 s por defecto, `TimeSpan.Zero` lo desactiva).
     Sigue corriendo aunque no se entregue nada, y es el que reporta el pending creciente.
     Ésa es la señal.

  Un fallo del sondeo **no afecta al consumo**: se registra como `Warn` y el bucle continúa
  —capturar dentro del bucle es deliberado, porque una excepción que escapara apagaría el
  sondeo para siempre tras un corte de red de dos segundos—. Sin `Metrics` configurado no se
  crea ni la tarea.
- **Un payload que incumple su JSON Schema se contabiliza como `outcome="invalid_schema"`**,
  al publicar y al consumir, mientras que el `dlqreason` sigue siendo `permanent`. Ver
  §"Validación L3".
- **Un fallo de firma se contabiliza como `outcome="invalid_signature"`**, aunque el
  `dlqreason` del evento siga siendo `poison`. Son dos incidentes distintos —basura frente
  a suplantación— con dos respuestas distintas. La traducción vive en
  `MetricLabels.ConsumeOutcomeFor` y es el mismo criterio que Go, Rust y PHP.

## Aislamiento entre tenants

[09-multitenancy.md §3](../specification/09-multitenancy.md). flux v1 usa el **Modelo A**:
un stream por dominio con todos los tenants mezclados, y el SDK filtra antes del handler.

```csharp
new ConnectOptions
{
    // …
    TenantId        = "acme",
    TenantIsolation = TenantIsolation.Strict,   // olvidar el filtro LANZA
}
```

- En `Strict`, suscribirse sin tenant configurado lanza `TenantIsolationException` **antes
  de crear el durable consumer**. No es celo: el fallo que previene —ver los datos de otro
  tenant— no produce ninguna señal. No hay excepción, no hay log, no hay métrica; hay un
  incidente de privacidad que se descubre semanas después.
- **`"system"` no cuenta como filtro.** Es la ausencia de tenant, no un tenant: se reserva
  para eventos de plataforma y no debe usarse como comodín ni como valor por defecto.
- El evento de otro tenant se **`Ack`ea y se descarta**. Nakearlo lo reentregaría seis veces
  y acabaría en la DLQ, convirtiendo el aislamiento en una fábrica de ruido.

La política vive en `TenantFilterPolicy`, fuera de `FluxBus` y sin tipos de NATS —igual que
`ConsumerConfigVerifier`— para poder probarla entera sin broker.

Lo que el Modelo A **no** da: todo servicio con acceso al dominio sigue pudiendo leer los
datos de todos los tenants. El aislamiento duro exige una account de NATS por tenant
(Modelo B), y eso es topología, no SDK.

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
README de Go, para poder leerlas en paralelo; H–K son nuevas y específicas de este port, y
L–P salieron al portar la fase 5 (firma, métricas y multi-tenant).

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

### H. Cuatro extensiones obligatorias que el modelo no puede marcar `required`

[01-envelope.md §3.1](../specification/01-envelope.md) declara `correlationid`, `tenantid`,
`producerversion` y `dataclassification` **obligatorias**, y `ParseEvent` las exige: su
ausencia —o un `""`, o un `null`— es `MISSING_REQUIRED_EXTENSION`. Eso está resuelto (ver
el recuadro al final de esta sección).

Lo que queda es una fricción del *modelo*: aunque el parser garantice que están, el tipo
`FluxEvent` no puede declararlas no anulables.

En un lenguaje con anulables eso obliga a elegir:

1. Marcarlas `required` y que System.Text.Json lance al deserializar → el fallo llegaría
   como un error genérico de deserialización (`INVALID_ATTRIBUTE_TYPE`) en vez del código
   estable que los otros seis SDKs devuelven ante el mismo cuerpo, y el mismo mensaje
   quedaría agrupado bajo dos causas distintas según el lenguaje del consumidor.
2. Declararlas anulables y **exigirlas en el parser** → `evento.TenantId` es `string?` y
   algún consumidor escribe `!`, pero el código de POISON es el del contrato.
3. Darles `= ""` por defecto → colapsa "ausente" y "vacío", que es justo lo que prohíbe
   §3.3.

Se ha elegido (2): el código correcto por encima de la ergonomía del tipo, con el `?` como
cicatriz visible. Java tomó la misma decisión (`Integer` en vez de `int`) por el mismo
motivo. El precio es que la garantía vive en `ParseEvent` y no en el sistema de tipos:
quien construya un `FluxEvent` a mano —no por el SDK— puede dejarlas a `null`.

> ✅ **Corregido.** Durante un tiempo esta sección decía que las extensiones no se exigían
> "porque el parser de referencia tampoco las exige". Eso dejó de ser cierto: Node, Python,
> Go, Java, Rust y PHP las exigen hoy, y `Envelope.ParseEvent` de este SDK también, en el
> mismo orden que fija `sdk-node/src/envelope.ts`:
>
> 1. núcleo de CloudEvents ausente o `null` → `MISSING_REQUIRED_ATTRIBUTE`
>    (`TryGetProperty` da `true` para un valor `null`, así que se comprueba el `ValueKind`);
> 2. extensión ausente, `null` o `""` → `MISSING_REQUIRED_EXTENSION`;
> 3. `dataclassification` fuera del enum → `INVALID_DATACLASSIFICATION` — **después** del
>    punto 2, así que `dataclassification: ""` es una extensión ausente y no un valor
>    inválido;
> 4. `id`, `source`, `type`, `time`, `correlationid`, `tenantid` o `producerversion` que no
>    sean cadena JSON → `WRONG_ATTRIBUTE_TYPE`.
>
> No es una diferencia cosmética de códigos: un `tenantid` ausente aceptado en silencio
> entra al handler y se cuela por cualquier filtro de tenant
> ([06-security.md §4](../specification/06-security.md),
> [09-multitenancy.md §3](../specification/09-multitenancy.md)).
>
> Lo fijan `EnvelopeTests` y los doce vectores POISON de
> [`conformance/harness/vectors.json`](../conformance/harness/vectors.json).

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

> 🔴 **Y `UnsafeRelaxedJsonEscaping` tampoco basta: escapa todo lo que está FUERA del BMP.**
> Emite `é` y `✅` literales, pero un emoji U+1F680 sale como los doce caracteres ASCII de
> la pareja de suplentes —barra invertida, `u` y cuatro dígitos, dos veces— en vez de como
> sus cuatro octetos `F0 9F 9A 80`. No es configurable: los `UnicodeRange` de .NET
> llegan hasta U+FFFF, así que ni `JavaScriptEncoder.Create(UnicodeRanges.All)` lo evita, y
> un `JavaScriptEncoder` propio exigiría `unsafe` en el paquete base (sus miembros
> abstractos usan punteros). Por eso `Envelope.Serialize` deshace esas parejas sobre los
> bytes ya serializados (`UnescapeAstralPlane`), saltándose las barras invertidas escapadas
> para no "descodificar" un `\uD83D` que el productor escribiera como texto.
>
> Es **exactamente** el mismo fallo que tuvo Java con el generador de bytes de Jackson
> (ver `sdk-java/README.md` §R), y lo caza el mismo vector: `utf8-literal` del arnés
> cross-SDK. Ningún test de un solo SDK podía verlo, porque todos cubrían solo el BMP —
> tener un emoji en el vector fue lo que lo destapó en dos lenguajes distintos.

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

### L. 🔴 Ed25519 no existe en la BCL — y la spec da por hecho que sí

[07-signing.md §3](../specification/07-signing.md) justifica la elección del algoritmo
diciendo que está "disponible en la biblioteca estándar o equivalente de todos los lenguajes
del ecosistema", y lista `System.Security.Cryptography` entre ellos. **No es cierto para
.NET 8**, que es la LTS vigente: el namespace trae `RSA`, `ECDsa` y `ECDiffieHellman`, y
EdDSA no aparece por ninguna parte. Ni siquiera cabe como "una curva más" en `ECCurve`, que
modela curvas de Weierstrass mientras que Ed25519 es de Edwards retorcida.

Es el único lenguaje del ecosistema donde la extensión de firma **no es gratis**: obliga a
un paquete de terceros y, por tanto, a una decisión de empaquetado que los otros cinco SDKs
no tienen que tomar. Aquí se resuelve partiendo el SDK en dos (`Flux` + `Flux.Signing`), de
modo que quien no firma no paga la dependencia; ver §"Firma de eventos".

**Sugerencia para la spec:** corregir esa lista. La frase importa más de lo que parece
porque es la que sostiene "sin negociación de algoritmo": si Ed25519 costara una dependencia
pesada en algún lenguaje, la presión por admitir un segundo algoritmo volvería — y esa
presión es exactamente lo que §3 existe para eliminar. Con un paquete gestionado de 3 MB el
argumento se sostiene, pero conviene decirlo en vez de dar por hecho lo contrario.

### M. `warn` no define dónde se registra, y aquí no hay un `console.warn`

[§7](../specification/07-signing.md) exige tres modos y dice que `warn` "se registra y se
acepta". No dice **dónde**. El SDK de Node usa `console.warn`; en .NET no existe un canal
equivalente que no imponga `Microsoft.Extensions.Logging` a toda aplicación que use el SDK.

`SigningOptions.OnWarn` acepta un `Action<string>` y cae a `Console.Error` si no se pasa.
Es lo razonable, pero significa que el mismo evento no firmado produce salidas distintas en
cada SDK, y una alerta sobre "cuántos productores faltan por migrar" no se puede escribir
contra los logs.

**Sugerencia:** que `warn` incremente además una métrica. El valor de etiqueta ya existe
—`flux_events_consumed_total{outcome="invalid_signature"}`,
[08-observability.md §2.1](../specification/08-observability.md)— pero hoy **solo se emite
cuando el evento muere en modo `Require`**, que es justo el escenario en el que ya no hay
migración que pilotar (ver §N). Un log es para leer; una migración se pilota con una
métrica.

### N. 🔴 Node es el único SDK que NO emite `outcome="invalid_signature"`

§2.1 lista `invalid_signature` entre los valores de `outcome`, y hay dos lecturas de cómo
contabilizar un fallo de firma en modo `Require`:

| SDK | `outcome` de un fallo de firma |
|---|---|
| Go, Rust, PHP, Java, **.NET** | `invalid_signature` (con `dlqreason` = `poison`) |
| **Node** (la referencia) | `poison` |

Este SDK sigue a la mayoría (`MetricLabels.ConsumeOutcomeFor`): la firma inválida se separa
del POISON común porque son dos incidentes distintos —basura frente a suplantación— con dos
respuestas distintas. Un pico de firmas rotas apunta a un productor con la clave equivocada
o a alguien reinyectando eventos; un pico de JSON corrupto, a un productor roto.
Confundirlos hace que la alerta no diga qué hacer. El `dlqreason` del evento **no** cambia,
porque ése sí es el enum cerrado de [04-errors.md §1](../specification/04-errors.md).

Pero mientras Node emita `poison`, `rate(flux_events_consumed_total{outcome="poison"})` mide
cosas distintas según el lenguaje del servicio — que es exactamente lo que
08-observability.md existe para evitar.

**Sugerencia:** que §2.1 diga explícitamente **cuándo** se emite `invalid_signature`, en vez
de limitarse a listarlo entre los valores posibles. Con eso, corregir `sdk-node` es una
línea; sin eso, cada SDK seguirá eligiendo, que es como se llegó aquí.

### O. La firma es la única parte del protocolo con **una sola** implementación correcta

Todo lo demás tolera divergencias menores: dos SDKs pueden ordenar las claves de `data` de
forma distinta y el ecosistema sigue funcionando, porque nadie compara bytes. La firma no.
Un byte de diferencia en `Serialize()` y la firma de .NET no verifica en Node — y el fallo
no aparece como "los SDKs divergen", aparece como **`INVALID_SIGNATURE` en producción**,
indistinguible de un ataque.

Eso convierte en requisitos de seguridad tres reglas que parecían de estilo, y en .NET las
tres estaban a una línea de romperse: el encoder por defecto escapa los no-ASCII (§J),
`ToString("O")` emite siete decimales (§C), y el orden de serialización no está garantizado
por contrato (de ahí los `[JsonPropertyOrder]` explícitos). Cualquiera de las tres, sola,
invalida todas las firmas del servicio.

**Sugerencia:** que la suite de conformidad incluya un caso de **firma cruzada** —una clave
fija, un evento fijo y la firma esperada en base64url— y no solo el envelope. Hoy cada SDK
comprueba que su propia firma verifica con su propia verificación, que es exactamente la
prueba que no demuestra nada.

### P. `Classification.RetryAfter` prometía más de lo que JetStream cumple

No es una fricción de .NET —afecta a los seis SDKs— pero se corrigió aquí al portar la
fase 5. La documentación decía "sobrescribe el backoff canónico para este intento", lo que
invita a construir lógica de reintentos sobre él. Y
[03-delivery.md §2.2](../specification/03-delivery.md) mide lo contrario: con `backoff`
configurado —y flux lo configura siempre— el delay del `nak` se honra **solo en la primera
reentrega**, y a partir de la segunda el servidor impone el array `backoff` sin devolver
error.

Ahora se documenta como **sugerencia para el primer reintento**. Es la tercera trampa de
JetStream de la misma familia (`ack_wait` sobrescrito por `backoff[0]`, el delay del `nak`
ignorado, y el publish de core que se evapora): **el servidor acepta la petición, no
devuelve error, y aplica otra cosa.** Ninguna se detecta leyendo código; solo midiendo.

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
| `Signing.cs` | Contrato y política de firma: `IEventSigner`, `IEventVerifier`, `VerificationMode`, `EventSigning.SignablePayload`, códigos POISON. **Sin criptografía** |
| `Validation.cs` | Contrato y política de validación L3: `ValidationMode`, `ValidationOptions`, `IEventValidator`, `SchemaValidationException`, `SchemaNotFoundException`. **Sin evaluador de esquemas** |
| `SchemaBundle.cs` | El `bundle.json` leído: subject → URI y URI → esquema. Solo `System.Text.Json` |
| `Metrics.cs` | `IMetricsSink`, `NoMetrics`, `InMemoryMetrics` y los enums de etiquetas |
| `TenantIsolation.cs` | `TenantIsolation`, `TenantFilterPolicy`, `TenantIsolationException` |
| `FluxBus.cs` | `ConnectAsync`, `PublishAsync`, `SubscribeAsync`, `DisposeAsync`. **Único fichero que conoce NATS** |

Y en el paquete aparte `Flux.Signing`:

| Fichero | Contenido |
|---|---|
| `Ed25519Signing.cs` | `SigningOptions`, `CreateSigner`, `CreateVerifier`, `GenerateKeyPair`. **Único fichero que conoce criptografía** |

Y en el paquete aparte `Flux.Validation`:

| Fichero | Contenido |
|---|---|
| `SchemaValidator.cs` | `Create`, `WithSchemaValidator` y el evaluador. **Único fichero que conoce JSON Schema** |

Y el utillaje del repositorio, que no se publica:

| Fichero | Contenido |
|---|---|
| `tools/Flux.ConformanceHarness/Program.cs` | Arnés de [conformidad cruzada](../conformance/harness/README.md): una operación por stdin, un resultado por stdout, exit 0 siempre |

## Desarrollo

```bash
dotnet build sdk-dotnet/Flux.sln
dotnet test sdk-dotnet/Flux.sln

# El arnés cross-SDK: compara los bytes de este SDK con los de los otros seis
dotnet build sdk-dotnet/Flux.sln -v quiet
node conformance/cross-sdk.mjs --only node,dotnet --verbose
```

Los tests no requieren un broker: cubren naming, envelope, clasificación, contexto, la
verificación de config de consumidor, la firma, las métricas y el aislamiento de tenant —
que es donde vive la semántica del protocolo. La conformidad contra un NATS real se
verifica con [`conformance/`](../conformance/).

> **Nota de procedencia.** Este SDK se escribió en una máquina **sin `dotnet` instalado**,
> así que la suite la ejecuta el CI (`.github/workflows/spec.yml`, job *SDK .NET*). Las
> partes que sí se pudieron verificar localmente fueron las que se comparten con el SDK de
> Java, que sí se compiló y ejecutó: el orden de los atributos del envelope firmado, el
> DER de las claves PKCS#8/SPKI —comprobado byte a byte contra el que emite
> `KeyPairGenerator` de Java, que a su vez verifica contra Node— y el formato de exposición
> de Prometheus.
>
> **Actualización.** La tanda que trajo la validación L3, el sondeo de `num_pending`, el
> endurecimiento de `ParseEvent` y el arreglo del escapado fuera del BMP **sí se compiló y
> se ejecutó**, aunque en esa máquina siga sin haber `dotnet`: se descargaron el compilador
> (`Microsoft.Net.Compilers.Toolset`, que corre sobre .NET Framework) y el runtime de .NET 8
> por separado, se compilaron los cuatro proyectos contra las DLL de referencia de `net8.0`
> y se ejecutó la suite con un runner de xunit por reflexión. Sirve para lo que sirve —el
> `dotnet test` de verdad y el `restore` de NuGet los hace el CI—, pero significa que estos
> cambios no se escribieron a ciegas: el bug del emoji, por ejemplo, se encontró **corriendo
> los vectores**, no leyendo el código.

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
