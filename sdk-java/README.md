# flux SDK para Java

Cliente del **flux Event Protocol v1** — CloudEvents 1.0 sobre NATS JetStream.
Nivel de conformidad objetivo: **L2**.

El contrato normativo vive en [`specification/`](../specification/); si algo de este
README diverge de la spec, manda la spec.

```xml
<dependency>
  <groupId>com.flux</groupId>
  <artifactId>flux-sdk</artifactId>
  <version>1.0.0</version>
</dependency>
```

Requiere **Java 17+**, `io.nats:jnats` 2.20+ (por la API de contextos:
`StreamContext` / `ConsumerContext` / `MessageConsumer`) y `jackson-databind`. Nada más:
el envelope es JSON plano y `time` se formatea a mano, así que no hacen falta
`jackson-datatype-jsr310` ni módulos de parámetros.

---

## Publicar

```java
FluxBus bus = FluxBus.connect(new FluxBus.ConnectOptions()
        .servers("nats://localhost:4222")
        .service("pedidos-api")
        .environment("produccion")
        .version("3.4.1")
        .tenantId("acme")
        .schemaBaseUrl("https://schemas.internal"));

record PedidoCreado(String pedidoId, String clienteId, int aggregateVersion,
                    long totalCents, String moneda) {}

bus.publish("pedidos.pedido.v1.creado",
        new PedidoCreado("ped-123", "cli-987", 1, 9990L, "EUR"),
        new FluxBus.PublishOptions().aggregateId("ped-123"));
```

**Solo escribes subject, `data` y opcionalmente `aggregateId`.** El SDK rellena `id`
(UUIDv7), `source`, `time`, `specversion`, `type`, `dataschema`, `correlationid`,
`causationid`, `producerversion` y `traceparent`. Si tu código asigna alguno de esos a
mano, está mal — [01-envelope.md §5](../specification/01-envelope.md).

## Consumir

```java
FluxBus.Subscription sub = bus.subscribe("pedidos.pedido.v1.creado",
        (ctx, evento, entrega) -> {
            PedidoCreado p = Envelope.dataAs(evento, PedidoCreado.class); // PERMANENT → DLQ

            if (yaProcesado(evento.id())) return;      // idempotencia: OBLIGATORIA
            hacerElTrabajo(p);
            bus.publish(ctx, "facturacion.factura.v1.emitida", factura);
            // volver sin excepción == ack explícito
        });
```

- **Volver del handler ACK-ea.** Lanzar clasifica el error y produce `nak`, `term` o
  `term`+alerta.
- **Todo handler DEBE ser idempotente.** La garantía es *at-least-once*: los duplicados
  llegan, no son un fallo. Elige una de las tres estrategias de
  [03-delivery.md §4](../specification/03-delivery.md).
- **Nunca asumas orden.** Incluye `aggregateVersion` en `data` y filtra con
  `WHERE aggregate_version < $n`.
- El `ctx` que llega es el contexto del evento entrante. Pásalo a `publish` para que la
  cadena de correlación siga viva (ver §1 de "Diferencias").

## Errores

```java
throw new FluxErrors.RetryableException("proveedor 503", "PROVEEDOR_503",
        Duration.ofSeconds(5), causa);
throw new FluxErrors.PermanentException("pedido ya cancelado", "PEDIDO_YA_CANCELADO");
```

| Clase | Qué es | Acción | Presupuesto |
|---|---|---|---|
| `RETRYABLE` reconocido | `SocketException`, `ConnectException`, HTTP 429/502/503/504 | `nak` + backoff | 6 entregas (~51 min) |
| **desconocido (default)** | cualquier otra excepción | `nak` acotado | **2 entregas (~30 s)** |
| `PERMANENT` | falla el schema, regla de negocio, HTTP 400/403/404/422 | `term` + DLQ | 1 entrega |
| `POISON` | JSON malformado, falta un atributo CloudEvents | `term` + DLQ + alerta | 1 entrega |

**El default de lo desconocido es `RETRYABLE_BOUNDED` con presupuesto 2**
([04-errors.md §2.1](../specification/04-errors.md)). Domina a las dos alternativas: un
transitorio desconocido se recupera en el segundo intento y un sistemático desconocido
llega a la DLQ en ~30 s sin atascar la cola. El presupuesto **no** se configura en el
consumidor —`max_deliver` es por consumidor, no por mensaje— sino que viaja en
`Classification.maxAttempts`, y el runtime aplica
`min(max_deliver, classification.maxAttempts)`.

```java
new FluxBus.ConnectOptions().classifier(new Classifier.ClassifierOptions()
        .unknownErrorPolicy(Classifier.UnknownErrorPolicy.PERMANENT)   // o RETRYABLE
        .unknownRetryBudget(3)
        .timeoutPolicy(Classifier.TimeoutPolicy.PERMANENT)
        .addRule(e -> esDeadlock(e)
                ? Optional.of(new Classification(ErrorClass.RETRYABLE, "DB_DEADLOCK"))
                : Optional.empty()));
```

Para que el clasificador reconozca un status HTTP, lanza
`new Classifier.HttpException(status, msg, retryAfter)` o implementa
`Classifier.HttpStatusAware`.

---

## Diferencias con los SDKs de referencia (Node y Go)

El envelope, el naming, la taxonomía de errores y la config de consumidor son
**idénticos**. Lo que sigue son divergencias de lenguaje, no de contrato.

### 1. Contexto explícito, como en Go — y Java sí tenía la alternativa

Node propaga `correlationid` y `traceparent` con `AsyncLocalStorage`: un `publish()` en
cualquier punto de la pila de un handler hereda el contexto sin que nadie pase nada.

Java **sí** tiene un mecanismo parecido (`ThreadLocal`) y aun así **no se usa**, por la
misma razón por la que Go rechaza el mapa por goroutine ID: el handler puede delegar en
un `ExecutorService`, un `CompletableFuture` o un pool, y el `ThreadLocal` no cruza
ninguna de esas fronteras. Funcionaría en los tests —donde todo es síncrono— y se
rompería en silencio en el primer handler que use un pool. `InheritableThreadLocal`
tampoco sirve: hereda al *crear* el hilo, y los hilos de un pool se crean antes de que
exista el evento.

```java
bus.subscribe(subject, (ctx, evento, entrega) -> {
    bus.publish(ctx, "facturacion.factura.v1.emitida", payload);  // ✅ propaga
    bus.publish("facturacion.factura.v1.emitida", payload);       // ❌ rompe la cadena
});
```

> ⚠️ Igual que en Go: usar la sobrecarga **sin** `ctx` dentro de un handler rompe la
> correlación **en silencio**. Es el precio de no tener magia; a cambio la propagación es
> visible en la firma y auditable en una revisión de código.

### 2. `traceparent` inyectado, no autodetectado

Node hace un `import()` dinámico de `@opentelemetry/api`. El equivalente en Java sería
reflexión sobre `io.opentelemetry.api.trace.Span` —frágil y opaca—, y una dependencia
dura obligaría a instalar OTel a todo servicio que use el SDK. Se invierte igual que en
Go: la aplicación pasa `ConnectOptions.traceparentSupplier`.

### 3. Status HTTP por interfaz, no por reflexión

Node hurga en `err.status`, `err.statusCode` y `err.response.status`. Aquí el contrato es
explícito: `Classifier.HttpStatusAware`, localizado recorriendo la cadena de causas — lo
que además funciona a través de `CompletionException`, `UncheckedIOException` y cualquier
envoltorio de la aplicación, cosa que el `instanceof` de Node no hace.

### 4. Los tipos de error se llaman `...Exception`, no `...Error`

La spec y los SDKs de Node/Go usan `RetryableError`, `PermanentError`, `PoisonError`.
Aquí son `FluxErrors.RetryableException`, `PermanentException` y `PoisonException`,
porque en Java `Error` es la rama de `Throwable` reservada a fallos irrecuperables
(`OutOfMemoryError`). Ver §H de "Fricciones".

### 5. Un fallo del handler no mata la suscripción, y un `Error` tampoco

El despacho envuelve al handler en un `catch (Throwable)` —equivalente al `recover()` de
Go— y además envuelve el propio despacho, para que un fallo inesperado (por ejemplo que
la publicación en la DLQ falle) no rompa el bucle de consumo dejando un consumidor muerto
que parece vivo.

Divergencia con Go: allí un pánico se convierte en `PERMANENT`. Aquí el error pasa por el
clasificador como cualquier otro, porque en Java lanzar es el canal normal de error y el
default de la spec para lo desconocido ya es el adecuado para un bug: DLQ en ~30 s.

### 6. `mvn test` no necesita broker

Igual que en Go y Node: los tests cubren naming, envelope, clasificación y verificación de
config de consumidor, que es donde vive la semántica del protocolo. La conformidad contra
un NATS real se verifica con [`conformance/`](../conformance/).

---

## Fricciones: dónde el protocolo no encaja limpio en Java

Java es, con Go, el otro SDK que valida el contrato de verdad. Lo que sigue son señales
**sobre la spec**, no sobre Java.

### A. Ausente ≠ vacío obliga a tipos envoltorio en cada opcional

Igual que en Go con `omitempty`. Aquí `dlqattempts` es `Integer` y no `int`, y el record
lleva `@JsonInclude(NON_NULL)`: con `int` primitivo, un `dlqattempts` ausente se leería
como `0` y se re-emitiría como `0`, colapsando cero y ausente. Hoy es inocuo solo porque
el mínimo legal resulta ser 1 — **el envelope depende de una coincidencia, no de una
regla**.

**Sugerencia para la spec (repetida desde el SDK de Go):** declarar explícitamente que
ningún atributo opcional admite el valor vacío como significativo (`dlqattempts >= 1`,
strings no vacíos). Este SDK ya lo aplica: `Envelope.DlqInfo` rechaza `attempts < 1`.

### B. `record` sí da igualdad por valor — pero es *semántica*, no byte a byte

Donde Go no puede (`json.RawMessage` es un slice y hace el struct no comparable), el
`record` de Java genera `equals` y `JsonNode.equals` compara el payload de forma profunda.

El problema es que **los dos SDKs responden cosas distintas a "¿son el mismo evento?"**:
Go compara bytes (dos payloads con las mismas claves en distinto orden son distintos),
Java compara estructura (son iguales). La spec no dice cuál es la correcta, y en cuanto
alguien escriba deduplicación por igualdad de evento o firma criptográfica (fase 4), la
diferencia importa.

**Sugerencia:** que la spec diga si la identidad de un evento es su representación
serializada o su estructura. Para replay y firma, casi seguro la primera.

### C. La fidelidad byte a byte del *payload* no está especificada, y el replay depende de ella

La spec fija el formato de `time` "byte a byte" (§2.2) y el replay desde la DLQ se define
como "borrar `dlq*` y republicar", lo que exige que `data` sobreviva a un
parse→serialize **sin cambiar**. Pero eso solo se dice del envelope, no del payload.

En Java el default de Jackson **no** lo cumple: un `4995.00` se reparsea como `double` y
se re-emite como `4995.0`. Ha hecho falta configurar el mapper con
`USE_BIG_DECIMAL_FOR_FLOATS` y `STRIP_TRAILING_BIGDECIMAL_ZEROES = false`. Go lo cumple
por accidente (guarda `json.RawMessage`, bytes sin tocar) y Python también.

**Sugerencia:** decir explícitamente que `data` debe preservarse verbatim en el
parse→serialize, o bien que el replay solo garantiza equivalencia semántica. Hoy se
deduce del uso, no se afirma — y un SDK que use el serializador por defecto de su
lenguaje lo incumple sin enterarse.

### D. La spec no dice nada sobre la COERCIÓN de tipos de los atributos

§2.3 fija que los *nombres* de atributo se comparan respetando mayúsculas. No dice nada de
los *valores*. Y ahí los SDKs divergen hoy:

| Cuerpo | Go | Node | Java (default de Jackson) |
|---|---|---|---|
| `"tenantid": 42` | POISON | acepta, `tenantid` es `42` | **aceptaba** como `"42"` |

Jackson convierte por su cuenta escalares a `String`. Este SDK lo desactiva
(`CoercionAction.Fail` para Integer/Float/Boolean → Textual) para alinearse con Go, pero
es una decisión que ha tenido que tomar el SDK, no la spec.

**Sugerencia:** añadir a §2.3 que los tipos de los atributos son exactos y que un tipo
incompatible es POISON. Es la misma clase de fantasma que la comparación case-insensitive.

### E. `subject` significa dos cosas, y en Java el desajuste queda por escrito

El componente del record se llama `aggregateId` y lleva `@JsonProperty("subject")`: el
nombre en Java y el nombre en JSON **no coinciden** justo en el atributo más confundible
del protocolo. Es la mejor solución disponible —renombrar el atributo de CloudEvents no es
opción—, pero merece un test de conformidad dedicado. Aquí lo tiene
(`serializeUsaLosNombresDeCloudEvents`).

### F. La categoría "resolución de nombres temporal" no es expresable en Java

[04-errors.md §1.1](../specification/04-errors.md) pide distinguir "el resolutor dice
**reinténtalo**" de "el nombre **no existe**". Go puede: `*net.DNSError` con
`IsTemporary`. **Java no**: el JDK usa `UnknownHostException` para NXDOMAIN y para
SERVFAIL indistintamente, y no expone el código de respuesta DNS.

Este SDK reintenta en ambos casos, porque el coste de reintentar un host inexistente está
acotado por `max_deliver` y el de no reintentar un SERVFAIL es tirar un evento bueno a la
DLQ. Pero es una aproximación, no la regla.

**Sugerencia:** que la spec reconozca que esta categoría es "best-effort según lo que
exponga la plataforma" en vez de normativa. Tal como está, ningún SDK de Java o .NET puede
cumplirla del todo.

Por lo demás, en Java el mecanismo idiomático es el **tipo** de la excepción, no un código:
`ConnectException`, `NoRouteToHostException`, `PortUnreachableException`,
`SocketException`, `UnknownHostException` para red; `SocketTimeoutException`,
`HttpTimeoutException`, `TimeoutException` para plazos. Ningún `contains` sobre mensajes:
el texto de un `SocketException` lo escribe el sistema operativo y difiere entre Windows y
Linux, que es exactamente el bug que la spec documenta.

### G. `ackWait == backoff[0]` sigue sin poder expresarse en el sistema de tipos

En los cuatro lenguajes acaba siendo dos constantes que alguien debe mantener
sincronizadas. En Java se puede hacer algo un poco mejor que un test: un **bloque
`static`** en `Protocol` comprueba la invariante al cargar la clase, así que un SDK con la
config incoherente **no arranca** en vez de fallar en producción a las 3 de la mañana. Se
valida además sobre la config **efectiva del servidor** en `assertConfigHonored`.

Sigue sin ser el sistema de tipos: es una comprobación en tiempo de ejecución que se
ejecuta antes que cualquier otra cosa.

### H. Los nombres `RetryableError` / `PermanentError` chocan con `java.lang.Error`

La spec fija los nombres de los tipos en sus ejemplos de código. En Java, `Error` es la
rama de `Throwable` reservada a fallos irrecuperables: una clase `RetryableError extends
RuntimeException` invitaría a `catch (Error e)` —que captura otra cosa— y los linters
marcan su captura como sospechosa.

**Sugerencia:** que la spec nombre los conceptos por su **clase** (`RETRYABLE`,
`PERMANENT`, `POISON`) y deje el nombre del tipo a cada lenguaje. Su prosa ya lo hace; los
bloques de código, no.

### I. 🔴 Los eventos de DLQ **no** son byte a byte iguales entre los SDKs existentes

No es una fricción de Java: es una divergencia real entre los SDKs ya escritos, encontrada
al decidir el orden de serialización de este.

| SDK | Orden en el evento de DLQ |
|---|---|
| Go (`ToDLQEvent`) | los campos `DLQ*` están declarados antes de `Data` en el struct → `dlq*`, luego `data` |
| Python (`to_dict`) | emite `dlq*` en el bucle de opcionales y `data` al final → `dlq*`, luego `data` |
| **Node** (`toDlqEvent`) | `{...event, dlqreason, …}` → **`data` primero y `dlq*` después** |

El fixture `conformance/cases/cross-sdk-envelope.json` solo cubre el envelope normal, así
que la suite no lo detecta. Consecuencias: un replay verbatim desde la DLQ no es idéntico
según qué SDK escribió el mensaje, y cualquier hash o firma sobre el evento de DLQ
diverge.

Este SDK **sigue el orden de Go y Python** (`dlq*` antes de `data`): es la mayoría, agrupa
las extensiones con el resto de atributos de contexto y deja el payload al final, igual que
el envelope normal.

**Sugerencia:** fijar el orden de los atributos del evento de DLQ en 04-errors.md §3,
corregir `sdk-node`, y añadir un caso de conformidad `cross-sdk-dlq-envelope`.

### J. Un método de conveniencia se convirtió en un atributo del envelope

Trampa específica de Java: Jackson descubre las propiedades de un `record` por sus
componentes **y además** aplica la convención de beans a los métodos. Un `isDlq()` público
—un helper de comodidad— se serializaba como `"dlq": false` en la raíz del envelope.

Lo cazó la regla de **atributos raíz cerrados** de §3.3, comprobada en un test. Es el
tercer caso (tras el emparejamiento case-insensitive de Go y la coerción de tipos de
Jackson) en el que esa regla salva a un SDK **por accidente** de un problema distinto del
que fue escrita para resolver. Merece la pena decirlo en la spec: la lista cerrada no es
solo una política de diseño, es la red que atrapa los automatismos del serializador de
cada lenguaje.

### K. `class` es palabra reservada

`Classification.class` no es un nombre legal en Java, así que el componente se llama
`errorClass`. Node usa `class` y Go `Class`. Trivial, pero es una diferencia de nombre en
un tipo que aparece en la API pública de los tres SDKs.

### L. 🐛 El patrón de `time` en `protocol.json` está roto

```json
"pattern": "^d{4}-d{2}-d{2}Td{2}:d{2}:d{2}.d{3}Z$"
```

Faltan las barras invertidas de `\d`: tal cual, solo casa con la cadena literal
`dddd-dd-ddTdd:dd:dd.dddZ` y **rechaza todos los timestamps válidos**. Un generador de
código o un agente que valide contra `protocol.json` —que es literalmente para lo que ese
fichero existe— daría por inválido cualquier evento correcto. Debería ser:

```json
"pattern": "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$"
```

(El fixture de `conformance/cases/cross-sdk-envelope.json` sí lo tiene bien escapado, así
que el error está solo en `protocol.json`.)

### M. `sdk-go` está desactualizado respecto al nuevo default de errores

`sdk-go/classify.go` sigue con `unknownClass = ClassPermanent`, su `Classification` no
tiene `MaxAttempts` y su README dice "Default de lo desconocido: `PERMANENT`". La spec
([04-errors.md §2.1](../specification/04-errors.md)) y `sdk-node` ya usan
`retryable-bounded` con presupuesto 2. Este SDK sigue la spec, así que **hoy Java y Node
reintentan un error desconocido y Go lo manda directo a la DLQ**.

---

## Ficheros

| Fichero | Contenido |
|---|---|
| `Protocol.java` | Constantes verificadas, naming (`parseSubject`, `subjectToType`, `streamName`, `durableName`, `dlqSubject`, `sourceUri`) y `uuidV7()` |
| `FluxEvent.java` | El record del envelope, `DataClassification`, `DlqReason` |
| `Envelope.java` | `buildEvent`, `serialize`, `parseEvent`, `dataAs`, `toDlqEvent`, `stripDlqExtensions`, `formatTime` y la configuración del `ObjectMapper` |
| `ErrorClass.java` | Las tres clases de la taxonomía |
| `FluxErrors.java` | `RetryableException`, `PermanentException`, `PoisonException`, `Classification`, `asClassified` |
| `Classifier.java` | `ClassifierOptions`, políticas configurables, `HttpStatusAware` |
| `EventContext.java` | Propagación explícita de `correlationid` / `causationid` / `traceparent` |
| `FluxBus.java` | `connect`, `publish`, `subscribe`, `close`, despacho, WIP, DLQ |
| `ConsumerConfigMismatchException.java` | Requisito L2: el servidor no honró la config |

## Desarrollo

```bash
mvn -q -f sdk-java/pom.xml test
```

Los tests no requieren un broker. Cubren:

- **naming** — 4 tokens, minúsculas, durable reversible, DLQ por prefijo, validación del
  nombre de servicio;
- **envelope** — `time` con exactamente 3 decimales, ausente ≠ vacío, atributos raíz
  cerrados, comparación case-sensitive, fidelidad del payload, límite de 1 MiB, DLQ y
  replay, y el **fixture cross-SDK byte a byte** de
  [`conformance/cases/cross-sdk-envelope.json`](../conformance/cases/cross-sdk-envelope.json);
- **clasificación** — errores tipados, status HTTP, red por tipo de excepción, timeouts,
  el default acotado y sus políticas;
- **config de consumidor** — `assertConfigHonored` contra el contraejemplo verificado de
  [`conformance/cases/consumer-config.json`](../conformance/cases/consumer-config.json), y
  la aritmética del presupuesto de reintentos;
- **UUIDv7** — versión, variante, timestamp embebido y monotonía dentro del mismo
  milisegundo.
