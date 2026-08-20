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
| `POISON` | JSON malformado, falta un atributo CloudEvents o una extensión obligatoria del perfil flux | `term` + DLQ + alerta | 1 entrega |

`Envelope.parseEvent` etiqueta cada fallo con un `code` estable —el mismo que dan Node,
Python y Go ante el mismo cuerpo— porque es lo que acaba en la columna de la DLQ y en las
métricas. Además de `MALFORMED_JSON`, `NOT_AN_OBJECT`, `UNSUPPORTED_SPECVERSION`,
`MISSING_REQUIRED_ATTRIBUTE`, `UNSUPPORTED_CONTENT_TYPE`, `UNKNOWN_ROOT_ATTRIBUTE` e
`INVALID_ATTRIBUTE_TYPE`:

| `code` | Cuándo |
|---|---|
| `MISSING_REQUIRED_EXTENSION` | Falta —o vale `null` o `""`— `correlationid`, `tenantid`, `producerversion` o `dataclassification`. No se les asume un default: uno tomado como `internal` haría circular PII con 30 días de retención en vez de 7, y un `tenantid` tomado como `system` cruzaría fronteras de tenant ([§3.1](../specification/01-envelope.md)) |
| `INVALID_DATACLASSIFICATION` | `dataclassification` fuera de `{public, internal, confidential, restricted}`. Gana al `@JsonCreator` del enum, que daría el código genérico de deserialización |
| `WRONG_ATTRIBUTE_TYPE` | Un atributo de texto llegó como número, booleano u objeto ([§2.4](../specification/01-envelope.md)) |

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

> ⚠️ **`retryAfter` es una sugerencia para el PRIMER reintento, y solo para él.** Con
> `backoff` configurado —y flux lo configura siempre— JetStream honra el delay del `nak`
> en la primera reentrega y a partir de la segunda impone el array `backoff`, **sin
> devolver error ni avisar** ([03-delivery.md §2.2](../specification/03-delivery.md),
> medido contra NATS 2.14.5). Un `Retry-After: 5` de un proveedor acorta el primer
> reintento y nada más: los siguientes seguirán 1 m, 5 m, 15 m, 30 m. No construyas lógica
> que dependa de que se respete más allá de la primera vez.

---

## Firma de eventos (opcional)

Extensión **opcional** de v1 — [07-signing.md](../specification/07-signing.md). El default
es `off` y **un evento sin firma sigue siendo válido**.

```java
Signing.KeyPairPem par = Signing.generateKeyPair();   // PKCS#8 + SPKI en PEM

FluxBus bus = FluxBus.connect(new FluxBus.ConnectOptions()
        // …
        .signing(new Signing.SigningOptions()
                .privateKeyPem(par.privateKeyPem())        // firmar al publicar
                .keyId("pedidos-api-1")
                .publicKey("pedidos-api-1", par.publicKeyPem())
                .verify(Signing.VerificationMode.REQUIRE)));
```

**Sin dependencias nuevas.** `java.security` trae `Ed25519` desde el JDK 15 (JEP 339) y
este SDK compila con `release 17`, así que no hace falta BouncyCastle. Es la diferencia
con el SDK de .NET, donde Ed25519 **no** está en la BCL y la firma vive en un paquete
aparte.

| Modo | Evento sin firma | Firma inválida |
|---|---|---|
| `OFF` (default) | se acepta | se acepta (no se mira) |
| `WARN` | se registra y se acepta | se registra y se acepta |
| `REQUIRE` | **POISON** `MISSING_SIGNATURE` | **POISON** `INVALID_SIGNATURE` / `UNKNOWN_SIGNING_KEY` |

`WARN` existe porque adoptar la firma en un ecosistema en marcha exige un periodo en el
que unos productores firman y otros no. Pasar directo a `REQUIRE` convierte en POISON todo
evento de un servicio aún no migrado.

Tres cosas que conviene tener claras:

- **`signkeyid` va dentro de lo firmado.** Si quedara fuera, un atacante lo cambiaría por
  el id de una clave suya y la firma seguiría "verificando".
- **Una clave RETIRADA sigue verificando** mientras se conserve su pública. Retirarla
  impide *emitir* con ella, no *verificar* lo ya emitido; tratarla como inválida convierte
  una rotación rutinaria en la invalidación retroactiva de todo el historial. Mínimo de
  retención: **90 días**, la de la DLQ.
- **La firma sobrevive a la DLQ y al replay.** Las extensiones `dlq*` se añaden después de
  firmar y se quitan antes de verificar, así que un evento reproducido conserva su firma
  válida — que es lo correcto: el replay redistribuye un hecho ya emitido.

Lo que **no** resuelve: confidencialidad, replay legítimo, autenticación del broker, ni las
ACLs. La ACL controla **quién puede escribir**; la firma, **quién lo escribió**.

## Métricas

Normativo para L2 — [08-observability.md](../specification/08-observability.md). Los
nombres y las etiquetas son **contrato entre SDKs**: si Java y Go nombran distinto la tasa
de DLQ, no se pueden sumar y un panel del ecosistema es imposible.

```java
InMemoryMetrics metrics = new InMemoryMetrics();
FluxBus bus = FluxBus.connect(new FluxBus.ConnectOptions()./* … */.metrics(metrics));

// en tu servidor HTTP:
responder(metrics.render());   // formato de exposición de Prometheus, sin dependencias
```

El default es `MetricsSink.NONE` (no-op): un SDK de protocolo no impone un backend de
métricas. Para enchufar Micrometer u OpenTelemetry, implementa `MetricsSink`.

| Métrica | Tipo | Etiquetas |
|---|---|---|
| `flux_events_published_total` | Counter | `subject`, `outcome` |
| `flux_events_consumed_total` | Counter | `subject`, `consumer`, `outcome` |
| `flux_event_handler_duration_seconds` | Histogram | `subject`, `consumer` |
| `flux_events_dlq_total` | Counter | `subject`, `consumer`, `reason`, `code` |
| `flux_events_retried_total` | Counter | `subject`, `consumer`, `attempt` |
| `flux_consumer_pending` | Gauge | `subject`, `consumer` |
| `flux_connection_state` | Gauge | — |

- **`MetricsSink` tiene un método por métrica con parámetros nombrados, no un
  `Map<String,String>` genérico.** Es deliberado: un mapa de etiquetas es justo por donde
  se cuela un `tenantid` que multiplica las series temporales. La cardinalidad no avisa —
  funciona con tres tenants en desarrollo y mata a Prometheus con diez mil en producción.
  Etiquetar por `tenantid`, `id` o `correlationid` está **prohibido**; para eso están las
  trazas ([§2.2](../specification/08-observability.md)).
- **El último bucket del histograma es `30` porque *es* el `ack_wait`.** Un handler que cae
  ahí está a punto de que su mensaje se reentregue mientras aún se ejecuta. Hay un test que
  lo ata a `Protocol.DEFAULT_ACK_WAIT`: cambiar uno sin el otro rompe la suite.
- **`flux_consumer_pending` se alimenta en cada entrega**, con el `pendingCount()` que ya
  viene en los metadatos del mensaje de JetStream: no hace falta sondear al servidor. Es la
  única señal que delata a un consumidor cuyo bucle murió, porque la conexión sigue
  reportándose sana y el healthcheck dice que todo va bien
  ([§4](../specification/08-observability.md)).
- **Un fallo de firma se contabiliza como `outcome="invalid_signature"`**, aunque el
  `dlqreason` del evento siga siendo `poison`. Son dos incidentes distintos —basura frente
  a suplantación— con dos respuestas distintas. Es el mismo criterio que Go, Rust y PHP.

## Aislamiento entre tenants

[09-multitenancy.md §3](../specification/09-multitenancy.md). flux v1 usa el **Modelo A**:
un stream por dominio con todos los tenants mezclados, y el SDK filtra antes del handler.

```java
FluxBus.connect(new FluxBus.ConnectOptions()
        .tenantId("acme")
        .tenantIsolation(FluxBus.TenantIsolation.STRICT));   // olvidar el filtro LANZA
```

- En `STRICT`, suscribirse sin tenant configurado lanza `TenantIsolationException`
  **antes de crear el durable consumer**. No es celo: el fallo que previene —ver los datos
  de otro tenant— no produce ninguna señal. No hay excepción, no hay log, no hay métrica;
  hay un incidente de privacidad que se descubre semanas después.
- **`"system"` no cuenta como filtro.** Es la ausencia de tenant, no un tenant: se reserva
  para eventos de plataforma y no debe usarse como comodín ni como valor por defecto.
- El evento de otro tenant se **`ack`ea y se descarta**. Nakearlo lo reentregaría seis
  veces y acabaría en la DLQ, convirtiendo el aislamiento en una fábrica de ruido.

Lo que el Modelo A **no** da: todo servicio con acceso al dominio sigue pudiendo leer los
datos de todos los tenants. El aislamiento duro exige una account de NATS por tenant
(Modelo B), y eso es topología, no SDK.

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

### D. ✅ La COERCIÓN de tipos ya está en la spec — y Java es el lenguaje que la necesitaba

§2.3 fijaba que los *nombres* de atributo se comparan respetando mayúsculas y no decía nada
de los *valores*, así que cada SDK hacía una cosa:

| Cuerpo | Go | Node | Java (default de Jackson) |
|---|---|---|---|
| `"tenantid": 42` | POISON | aceptaba, `tenantid` era `42` | aceptaba como `"42"` |

Los tres comportamientos eran distintos y el que "funcionaba" era el peor: propagar `"42"`
significa que el mensaje se acepta con un valor que el productor nunca escribió.
[§2.4](../specification/01-envelope.md) lo cierra: los tipos son exactos y un tipo
incompatible es POISON.

Aquí se defiende **por partida doble**, y a propósito. El mapper desactiva la coerción
(`CoercionAction.Fail` para Integer/Float/Boolean → Textual), y `parseEvent` comprueba
además sobre el `JsonNode` que los siete atributos de texto son textuales. La comprobación
explícita no sobra: la configuración del mapper es un default que una versión futura de
Jackson podría cambiar, y sin ella el fallo llegaría con el código genérico
`INVALID_ATTRIBUTE_TYPE`, indistinguible de un `"dlqattempts": "seis"`. El contrato pide
`WRONG_ATTRIBUTE_TYPE`.

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

### I. ✅ RESUELTO — los eventos de DLQ ya son byte a byte iguales

No es una fricción de Java: es una divergencia real entre los SDKs ya escritos, encontrada
al decidir el orden de serialización de este.

| SDK | Orden en el evento de DLQ |
|---|---|
| Go (`ToDLQEvent`) | los campos `DLQ*` están declarados antes de `Data` en el struct → `dlq*`, luego `data` |
| Python (`to_dict`) | emite `dlq*` en el bucle de opcionales y `data` al final → `dlq*`, luego `data` |
| **Node** (`toDlqEvent`) | emitía `{...event, dlqreason, …}` → **`data` primero y `dlq*` después** |

Este SDK **sigue el orden de Go y Python** (`dlq*` antes de `data`): es la mayoría, agrupa
las extensiones con el resto de atributos de contexto y deja el payload al final, igual que
el envelope normal.

**Estado actual: resuelto.** `sdk-node` ya destructura `data` y lo reemite al final, y
[01-envelope.md §6](../specification/01-envelope.md) recoge la regla como normativa citando
esta divergencia por su nombre. Comprobado al portar la fase 5: un evento **firmado** que
pasa por la DLQ produce exactamente los mismos bytes en Java y en Node, incluida la
posición de `signkeyid` y `signature` (después de `tracestate`, antes de las `dlq*`).

**Lo que sigue faltando:** el caso de conformidad `cross-sdk-dlq-envelope`. El fixture
`cross-sdk-envelope.json` solo cubre el envelope normal, así que la regla está escrita y
comprobada a mano, pero la suite no la vigila. Y ahora importa el doble: con la firma
activa, un SDK que reordene las `dlq*` no solo rompe el replay verbatim — invalida la firma
de todo lo que pase por su DLQ.

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

### N. La firma es la única parte del protocolo con **una sola** implementación correcta

Todo lo demás tolera divergencias menores: dos SDKs pueden ordenar las claves de `data` de
forma distinta y el ecosistema sigue funcionando, porque nadie compara bytes. La firma no.
Un byte de diferencia en `serialize()` y la firma de Java no verifica en Node — y el fallo
no aparece como "los SDKs divergen", aparece como **`INVALID_SIGNATURE` en producción**,
indistinguible de un ataque.

Eso convierte tres reglas que parecían de estilo en requisitos de seguridad:
[§1.1](../specification/01-envelope.md) (UTF-8 literal), [§2.2](../specification/01-envelope.md)
(exactamente 3 decimales) y [§6](../specification/01-envelope.md) (orden de claves). La spec
ya lo dice ([07-signing.md §2](../specification/07-signing.md)); lo que falta es la
consecuencia operativa.

**Sugerencia:** que la suite de conformidad incluya un caso de **firma cruzada** —una clave
fija, un evento fijo y la firma esperada en base64url— y no solo el envelope. Hoy cada SDK
comprueba que su propia firma verifica con su propia verificación, que es exactamente la
prueba que no demuestra nada. Al portar este SDK hubo que montar el cruce contra Node a
mano; debería ser un fixture.

### O. `warn` necesita un canal de log que el protocolo no define

[07-signing.md §7](../specification/07-signing.md) exige tres modos y dice que `warn` "se
registra y se acepta". No dice **dónde**. El SDK de Node usa `console.warn` directamente,
que es una decisión que ningún SDK de Java tomaría: aquí no hay un logger universal, y
elegir SLF4J impondría una fachada a toda aplicación que use el SDK.

Este SDK acepta un `Consumer<String>` en `SigningOptions.onWarn` y cae a
`System.Logger("flux.signing")` si no se pasa. Es lo razonable, pero significa que **el
mismo evento no firmado produce salidas distintas en cada SDK**, y una alerta sobre "cuántos
productores faltan por migrar" no se puede escribir contra los logs.

**Sugerencia:** que el modo `warn` incremente además una métrica. El valor de etiqueta ya
existe —`flux_events_consumed_total{outcome="invalid_signature"}`,
[08-observability.md §2.1](../specification/08-observability.md)— pero hoy **solo se emite
cuando el evento muere en modo `require`**, que es justo el escenario en el que ya no hay
migración que pilotar. Un log es para leer; una migración se pilota con una métrica.

### P. 🔴 Node es el único SDK que NO emite `outcome="invalid_signature"`

§2.1 lista `invalid_signature` entre los valores de `outcome`, y hay dos lecturas de cómo
contabilizar un fallo de firma en modo `require`:

| SDK | `outcome` de un fallo de firma |
|---|---|
| Go, Rust, PHP, **Java**, **.NET** | `invalid_signature` (con `dlqreason` = `poison`) |
| **Node** (la referencia) | `poison` |

Este SDK sigue a la mayoría: la firma inválida se separa del POISON común porque son dos
incidentes distintos —basura frente a suplantación— con dos respuestas distintas. Un pico
de firmas rotas apunta a un productor con la clave equivocada o a alguien reinyectando
eventos; un pico de JSON corrupto, a un productor roto. Confundirlos hace que la alerta no
diga qué hacer. El `dlqreason` del evento **no** cambia, porque ése sí es el enum cerrado de
[04-errors.md §1](../specification/04-errors.md).

Pero mientras Node emita `poison`, `rate(flux_events_consumed_total{outcome="poison"})`
mide cosas distintas según el lenguaje del servicio — que es exactamente lo que
08-observability.md existe para evitar.

**Sugerencia:** que §2.1 diga explícitamente **cuándo** se emite `invalid_signature`, en vez
de limitarse a listarlo entre los valores posibles. Con eso, corregir `sdk-node` es una
línea; sin eso, cada SDK seguirá eligiendo, que es como se llegó aquí.

### Q. `Classification.retryAfter` prometía más de lo que JetStream cumple

No es una fricción de Java —afecta a los seis SDKs— pero se corrigió aquí al portar la
fase 5. El javadoc decía "sobrescribe el backoff canónico para ESTE intento", lo que invita
a construir lógica de reintentos sobre él. Y
[03-delivery.md §2.2](../specification/03-delivery.md) mide lo contrario: con `backoff`
configurado —y flux lo configura siempre— el delay del `nak` se honra **solo en la primera
reentrega** y a partir de la segunda el servidor impone el array `backoff`, sin devolver
error.

Ahora se documenta como **sugerencia para el primer reintento**. Es la tercera trampa de
JetStream de la misma familia (`ack_wait` sobrescrito por `backoff[0]`, el delay del `nak`
ignorado, y el publish de core que se evapora): **el servidor acepta la petición, no
devuelve error, y aplica otra cosa.** Ninguna se detecta leyendo código.

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
| `FluxBus.java` | `connect`, `publish`, `subscribe`, `close`, despacho, WIP, DLQ, firma, métricas, aislamiento de tenant |
| `ConsumerConfigMismatchException.java` | Requisito L2: el servidor no honró la config |
| `Signing.java` | Ed25519 sobre `java.security`: `SigningOptions`, `Signer`, `Verifier`, `generateKeyPair`, códigos POISON |
| `MetricsSink.java` | El contrato de métricas: siete métodos con parámetros nombrados, buckets y `NONE` |
| `InMemoryMetrics.java` | Recolector sin dependencias con salida en formato Prometheus |
| `TenantIsolationException.java` | Suscripción sin filtro de tenant con el aislamiento en estricto |

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
  milisegundo;
- **firma** — orden de los atributos, determinismo, round-trip de serialización,
  supervivencia a la DLQ y al replay, detección de manipulación de `data`, `tenantid` y
  `signkeyid`, los tres modos y la clave retirada;
- **métricas** — el último bucket contra `Protocol.DEFAULT_ACK_WAIT`, los siete nombres con
  sus etiquetas exactas, ausencia de etiquetas de alta cardinalidad, formato de exposición
  línea a línea, escapado de comillas, y que un fallo de firma dé
  `outcome="invalid_signature"` sin tocar el `dlqreason`;
- **aislamiento de tenant** — `STRICT` sin tenant lanza, `"system"` no cuenta como filtro,
  precedencia suscripción → conexión.

> Estos tres bloques se verificaron además **contra el SDK de Node**, que es la referencia:
> un evento firmado en Java lo verifica el verificador de Node, la firma de los dos es el
> mismo string, el envelope firmado es idéntico byte a byte, y el volcado de
> `InMemoryMetrics.render()` coincide byte a byte con el de Node para el mismo conjunto de
> observaciones. Sin esa comprobación, "las dos suites pasan" solo demuestra que cada SDK
> es coherente consigo mismo.
