# flux SDK — PHP

Cliente de **flux Event Protocol v1** (CloudEvents 1.0 sobre NATS JetStream).
Nivel de conformidad objetivo: **L2** ([00-protocol.md §5](../specification/)).

Port del SDK de referencia de Node (`sdk-node/src/`), siguiendo de cerca al de Python por
ser el otro lenguaje dinámico: misma semántica, mismos defaults, mismos códigos de error.
Lo que cambia son los nombres (`camelCase`) y las piezas de plataforma que en PHP no
existen igual — todas anotadas más abajo, sin maquillaje.

```bash
composer require flux/sdk        # requiere PHP >= 8.2
```

> ### Estado de verificación
>
> ✅ **Suite ejecutada y en verde: 201 tests, 385 assertions** (PHP 8.3.30, PHPUnit 10.5.64).
> Cubre naming, envelope, serialización byte a byte, clasificación de errores y todo el
> runtime del consumidor. **Ninguno necesita broker.**
>
> ⚠️ `Flux\Transport\BasisNatsTransport` **sigue sin verificarse contra un servidor NATS
> real.** Sus tests usan un cliente falso: fijan cómo reacciona ante un PubAck correcto, un
> error del stream y el silencio, pero **no** demuestran que hable bien con NATS ni que la
> API de `basis-company/nats` sea la que asume. Lo que afirma del protocolo (subjects
> `$JS.API.*`, forma de las peticiones, nanosegundos) sale de la especificación de
> JetStream; lo que afirma de la librería sale de su README y puede cambiar entre versiones
> menores. Valídalo antes de producción, o implementa `Flux\Transport\NatsTransport` sobre
> el cliente que uses — para eso existe el puerto.

---

## El modelo de ejecución de PHP: léelo antes de nada

Esta es la diferencia real entre este SDK y los de Node, Python, Go, Java y .NET, y no
tiene arreglo dentro del SDK.

**El publisher es directamente utilizable desde cualquier petición.** `publish()` es una
llamada síncrona que termina antes de que responda tu controlador. No hay bucle de eventos
que mantener vivo, no hay nada que "arrancar". Un `FluxBus` por petición funciona.

**El consumidor necesita un proceso CLI de larga vida.** No es un handler web, no es una
tarea de cron de un minuto, y no puede vivir dentro de FPM. Concretamente:

| | Node / Python / Go | PHP |
|---|---|---|
| Consumir | `await bus.subscribe(...)` y el runtime hace el resto | `subscribe()` **registra**; `run()` conduce el bucle y bloquea |
| Handler concurrente | Sí, el bucle de eventos multiplexa | No. Un mensaje cada vez, en serie |
| Work-in-progress automático | El SDK lo emite cada `ack_wait / 2` desde un temporizador | **No es posible.** Ver abajo |

```php
// worker.php — se lanza con `php worker.php`, bajo supervisord/systemd/k8s
$bus->subscribe('pedidos.pedido.v1.creado', $handler);
$bus->run();   // bloquea
```

### Work-in-progress: hay que llamarlo a mano

[03-delivery.md §2.1](../specification/) exige que el SDK emita WIP cada `ack_wait / 2`
mientras el handler siga vivo. **En PHP no se puede.** Entre que el handler empieza y
termina, el proceso está dentro de tu código y el SDK no recupera el control: no hay hilos,
no hay bucle de eventos, y `pcntl_alarm` no existe en Windows ni es seguro dentro de
operaciones de E/S bloqueantes.

Lo que sí hay es un WIP explícito:

```php
$bus->subscribe('pedidos.pedido.v1.creado', function ($evento, $ctx) {
    foreach ($lotes as $lote) {
        procesar($lote);
        $ctx->workInProgress();   // el plazo de ack_wait vuelve a empezar
    }
});
```

**Si tu handler puede tardar más de 30 s y no llama a `workInProgress()`, recibirás el
mismo evento reentregado mientras aún se está ejecutando.** Eso es un bug de la aplicación
—lo dice la spec—, pero en PHP el SDK no puede taparlo por ti.

### `run()` y los workers desechables

`run()` acepta límites, porque un worker de PHP tiene vida útil finita en la práctica
(fugas de extensiones, despliegues, `opcache`) y reiniciarlo es la forma normal de operarlo.
Un durable consumer retoma exactamente donde lo dejó.

```php
$bus->run(maxMessages: 1000);                 // reinicia cada 1000 mensajes
$bus->run(maxSeconds: 300);                   // reinicia cada 5 minutos
$bus->run(stopWhenIdle: true);                // vacía la cola y sal (estilo cron)
$bus->run(shouldStop: fn () => $recibiSigterm); // apagado limpio
```

---

## Publicar

```php
use Flux\ConnectOptions;
use Flux\FluxBus;

$bus = FluxBus::connectToNats(
    new ConnectOptions(
        service: 'pedidos-api',          // kebab-case, se valida
        environment: 'produccion',
        version: '3.4.1',                // va en producerversion
        tenantId: 'acme',
        classification: 'confidential',
        schemaBaseUrl: 'https://schemas.internal',
    ),
    $natsClient,                          // Basis\Nats\Client ya configurado
);

$bus->publish('pedidos.pedido.v1.creado', [
    'pedidoId' => 'ped-123',
    'clienteId' => 'cli-987',
    'aggregateVersion' => 1,
    'totalCents' => 9990,                 // entero en la unidad mínima, nunca float
    'moneda' => 'EUR',
], aggregateId: 'ped-123');
```

Tú escribes **subject, `data` y opcionalmente `aggregateId`**. El SDK rellena `id`
(UUIDv7), `source`, `time`, `specversion`, `type`, `dataschema`, `correlationid`,
`causationid`, `producerversion` y `traceparent`. Si tu código asigna alguno de esos a
mano, está mal ([01-envelope.md §5](../specification/)).

> ⚠️ `aggregateId` es el atributo `subject` de CloudEvents (`"ped-123"`), **no** el subject
> de NATS. Son dos cosas distintas con el mismo nombre y confundirlas es el error más
> frecuente al adoptar CloudEvents sobre NATS.

## Consumir

```php
$bus->subscribe('pedidos.pedido.v1.creado', function (FluxEvent $evento, HandlerContext $ctx) {
    if (yaProcesado($evento->id)) {       // OBLIGATORIO: la garantía es at-least-once
        return $ctx->ack();
    }
    hacerElTrabajo($evento->data);
    marcarProcesado($evento->id);

    return $ctx->ack();
});

$bus->run();
```

- Devolver normalmente = `ack`. Lanzar = clasificar y decidir (ver abajo).
- **Todo consumidor DEBE ser idempotente.** Los duplicados llegan; no son un fallo. Elige
  una de las tres estrategias de [03-delivery.md §4](../specification/).
- `duplicate_window` **no** deduplica reentregas de consumo. Es el malentendido más común
  del protocolo.

## Errores

```php
use Flux\PermanentError;
use Flux\RetryableError;

throw new RetryableError('proveedor 503', retryAfterMs: 5000);              // nak + backoff
throw new PermanentError('pedido ya cancelado', 'PEDIDO_YA_CANCELADO');    // term + DLQ
```

| Clase | Qué es | Acción |
|---|---|---|
| `RETRYABLE` | Timeout, red caída, HTTP 429/502/503/504, deadlock de BD | `nak` + backoff canónico |
| `PERMANENT` | Falla el schema, regla de negocio, HTTP 400/403/404/422 | `term()` + DLQ inmediato |
| `POISON` | JSON malformado, falta un atributo CloudEvents o una extensión obligatoria del perfil flux | `term()` + DLQ + alerta |

**Lo desconocido es RETRYABLE con presupuesto acotado: 2 entregas, no 6**
([04-errors.md §2.1](../specification/)).

```
Error reconocido como transitorio (ECONNRESET, 503) → 6 entregas, hasta 51 min
Error desconocido                                   → 2 entregas, ~30 s
Error reconocido como permanente (400, 422)         → 1 entrega, sin espera
```

Las dos opciones obvias fallan cada una en un extremo: `permanent` manda a la DLQ un evento
válido por un hipo de red y alguien lo reproduce a mano cada mañana; `retryable` completo
atasca la cola 51 minutos y el modo de fallo se amplifica con cada mensaje siguiente. El
acotado cuesta 30 segundos de latencia sobre los permanentes genuinos y elimina ambos — no
es un punto medio, es estrictamente mejor.

El presupuesto **no** se configura en `max_deliver`: `max_deliver` es por consumidor, no
por mensaje, y bajarlo a 2 recortaría también los reintentos de los RETRYABLE reconocidos.
El clasificador rellena `Classification::$maxAttempts` solo para los desconocidos y el
runtime aplica `min(maxDeliver, maxAttempts)` a ese error concreto.

```php
new ClassifierOptions(
    unknownErrorPolicy: 'retryable-bounded',   // o 'permanent' / 'retryable'
    unknownRetryBudget: 2,
    timeoutPolicy: 'permanent',
    rules: [ fn (\Throwable $e) => $e instanceof MiError
        ? new Classification(ErrorClass::Retryable, 'MI_ERROR')
        : null ],
);
```

### Cómo se reconoce un error transitorio en PHP

[04-errors.md §1.1](../specification/) prohíbe hacer `str_contains()` sobre mensajes de
error: son texto para humanos, cambian con la versión, el locale y el sistema operativo. Un
port literal de la lista de códigos de libuv de Node produjo un bug real (en Windows el
mismo corte de red era PERMANENT, en Linux RETRYABLE).

PHP no tiene `error.code = 'ECONNRESET'` como Node ni `errno` en la excepción como Python.
Sus mecanismos idiomáticos son cuatro, y el clasificador usa los cuatro **en este orden**:

| Mecanismo | Qué reconoce |
|---|---|
| **Tipos de excepción** | `Psr\Http\Client\NetworkExceptionInterface` significa literalmente "no se pudo hablar con el servidor" → RETRYABLE. `RequestExceptionInterface`, "la petición nunca fue válida" → PERMANENT. Configurable con `transientExceptions` / `permanentExceptions` |
| **Status HTTP** | `getResponse()->getStatusCode()` (Guzzle, PSR-18), `getStatusCode()` (Symfony, Laravel), o una propiedad `status`/`statusCode`. **Nunca `getCode()`**: en el 90 % de las excepciones de PHP es un cero o un código de driver |
| **SQLSTATE** | Clase `08` (conexión), `40` (deadlock y fallo de serialización), `53` (recursos agotados), `57` (`57P03` = la base de datos está arrancando) → RETRYABLE. Se lee de `errorInfo[0]` (PDO) o `getSQLState()` (Doctrine DBAL) |
| **Errno de socket** | `SOCKET_ECONNRESET` y compañía, comparados **por nombre de constante**: en Windows valen 10054 y en Linux 104, y comparar valores reproduciría exactamente el bug de Node |

La única concesión textual es detectar timeouts por el **nombre corto de la clase** (PHP no
tiene una excepción de timeout estándar; cada librería trae la suya sin jerarquía común).
Es lo mismo que hace el SDK de Python, y un nombre de clase es API pública y estable — un
mensaje de error no lo es ni pretende serlo.

## Propagación de contexto

`correlationid`, `causationid`, `tenantid` y `traceparent` se propagan solos: un `publish()`
en cualquier punto de la pila del handler hereda el contexto del evento entrante. No hay
que pasar nada por parámetro.

Donde Node usa `AsyncLocalStorage` y Python `contextvars`, aquí basta una variable estática
con `try/finally`: PHP ejecuta un solo flujo por proceso, así que "el evento que se está
procesando ahora mismo" es un concepto literal.

> ⚠️ Si tu handler usa **Fibers** (ReactPHP, Amp, Swoole con corrutinas) para procesar
> varias cosas a la vez, ese almacenamiento estático se comparte entre fibras y el
> `correlationid` se cruzaría. El bucle del SDK es estrictamente secuencial y no las usa;
> si las usas por dentro, publica desde la fibra principal.

## Transporte

`FluxBus::connect()` recibe un `Flux\Transport\NatsTransport`. **Ningún tipo de NATS cruza
esa frontera** — si lo hiciera, sustituir el broker dejaría de ser un cambio de capa 0-1 y
pasaría a tocar las aplicaciones, que es lo que flux existe para evitar.

Vienen dos implementaciones:

- `BasisNatsTransport` — sobre `basis-company/nats`. Sin verificar (ver el aviso de arriba).
- `InMemoryTransport` — para tus tests. Inyéctalo y comprueba qué publica tu handler sin
  levantar nada:

```php
$transport = new InMemoryTransport();
$bus = FluxBus::connect($options, $transport);
$bus->subscribe('pedidos.pedido.v1.creado', $miHandler);

$transport->deliver('pedidos.pedido.v1.creado', $jsonDelEvento);
$bus->run(stopWhenIdle: true);

assert($transport->ackTokens() === ['+ACK']);
```

---

## Diferencias de port

Ninguna cambia la semántica en el cable salvo donde se dice explícitamente.

| Node / Python | PHP | Por qué |
|---|---|---|
| El SDK habla con el cliente de NATS | Interfaz `NatsTransport` + adaptador | El ecosistema PHP de NATS no está consolidado y el cliente más usado no expone con estabilidad el subject de respuesta de JetStream ni el WIP. Con un puerto, cambiar de cliente es un fichero; sin él, una reescritura. Y todo el runtime queda testeable sin broker |
| `subscribe()` arranca el consumo | `subscribe()` registra, `run()` consume | PHP no tiene bucle de eventos. Ver "El modelo de ejecución de PHP" |
| WIP automático cada `ack_wait / 2` | `$ctx->workInProgress()` manual | **Limitación real, no de diseño.** El SDK no recupera el control mientras el handler corre |
| Duraciones en ns (Node) / s (Python) | **Nanosegundos** | El adaptador habla con `$JS.API.*` directamente, y su JSON usa nanosegundos. Equivocar la unidad no da error: da un `ack_wait` de 950 años |
| `msg.ack()` / `msg.nak()` | Publicar `+ACK` / `-NAK {...}` en el reply subject | Es literalmente el protocolo de JetStream. Hacerlo así evita depender de la API de mensajes de un cliente concreto, y permite testear el parseo del número de entrega sin broker |
| `num_delivered` de los metadatos | Se extrae del subject `$JS.ACK.…` | Mismo motivo. Soporta los formatos de 9 y 12 tokens |
| `uuid` (Node) / `uuid7()` (Python) | `Protocol::uuid7()` | PHP no lo trae; `ext-uuid` no genera v7. Se implementa con la stdlib, con contador en `rand_a` para conservar la monotonía intra-milisegundo de la que depende [01-envelope.md §2.6](../specification/) |
| `JSON.stringify` / `json.dumps(ensure_ascii=False)` | `json_encode` con **3 banderas obligatorias** | Ver la sección siguiente. Es la trampa más cara del port |
| `Classification.class` | `Classification::$errorClass` | `$c->class` convive mal con `$c::class`, que en PHP 8 devuelve el nombre de la clase del objeto. Python lo renombró por el mismo tipo de motivo (`class` es palabra reservada) |
| `FluxError.code` | `FluxError::$errorCode` | `\Exception::$code` ya existe, es `protected` y es `int`. Redeclararlo como `?string` sería un error fatal de compilación |
| `errors.py` con 4 clases | Un fichero por clase | PSR-4 exige una clase por fichero. Es la única desviación respecto al árbol de ficheros propuesto |
| Handler `async` | Handler síncrono | No hay `await`. Un handler que bloquea bloquea el worker entero, y `max_ack_pending` deja de ser una ventana de concurrencia real |

### La trampa de `json_encode`

Los defaults de PHP violan [01-envelope.md §1.1](../specification/) **por partida doble**:

- escapa todo lo no-ASCII (una `é` acaba como una secuencia `\uXXXX`), y
- escapa las barras (`/` acaba como `\/`).

Un `dataschema` es siempre una URL, así que **sin las banderas correctas ningún evento
producido por PHP coincide byte a byte con los otros cinco SDKs** — y ningún test que
compare arrays decodificados lo vería, porque como dato son idénticos. Eso rompe el replay
verbatim desde la DLQ, la deduplicación por hash de contenido, los fixtures compartidos y
cualquier firma criptográfica futura.

`Envelope::JSON_FLAGS` usa:

```php
JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION | JSON_THROW_ON_ERROR
```

`tests/JsonEncodingTest.php` lo comprueba **a nivel de bytes**, incluida una comparación
contra un fixture literal.

### Donde PHP no encaja limpio

Cosas que el protocolo da por hechas y en PHP no salen gratis. Se documentan en vez de
fingir que no existen.

1. **`{}` frente a `[]`.** `json_decode(..., true)` colapsa el objeto vacío y la lista
   vacía en el mismo `array` de PHP. En la **raíz** de `data` la ambigüedad se resuelve por
   contrato (§4: `data` DEBE ser un objeto) y el SDK fuerza `{}`. Un objeto vacío
   **anidado** sigue siendo ambiguo: si necesitas uno, pon un `new \stdClass()`.
   Al parsear, el primer carácter no blanco del cuerpo distingue `{` de `[` sin volver a
   decodificar el mensaje entero.
2. **Fidelidad numérica ([§2.5](../specification/)).** `json_decode` convierte los números
   de `data` a `int`/`float`, así que un `4995.00` vuelve a salir como `4995.0` — igual que
   en Python, y a diferencia de Go y Java, que guardan bytes crudos.
   `JSON_PRESERVE_ZERO_FRACTION` evita al menos que `4995.0` se convierta en `4995`. La
   forma de no tener el problema es la que recomienda la propia spec: **no metas decimales
   en `data`**; los importes van como entero en la unidad mínima.
   Un entero por encima de `PHP_INT_MAX` se degrada a `float` al decodificar; si algún día
   hace falta, la salida sería `JSON_BIGINT_AS_STRING` y un cambio de tipo en la API.
3. **Coerción de tipos.** PHP coacciona con entusiasmo (`"42" == 42` es `true`), así que
   `is_string()` explícito no es una comprobación defensiva: es **la** comprobación. Sin
   ella, `{"tenantid": 42}` llegaría al handler como entero y "funcionaría" hasta que
   dejase de hacerlo.
4. **`declare(strict_types=1)` y el constructor.** Un `dlqattempts` que llegue como cadena
   lanza `TypeError` en vez de coaccionar. `Envelope::parse()` lo captura y lo convierte en
   `WRONG_ATTRIBUTE_TYPE`; sin ese `catch`, el proceso del consumidor se caería en lugar de
   mandar el mensaje a la DLQ.
5. **Recorte de `dlqerror`.** Node y Python recortan a 1024 **caracteres**; aquí se recorta
   a 1024 **bytes** (el presupuesto que importa es el del mensaje de 1 MiB) y se sueltan los
   bytes finales hasta que la cadena vuelve a ser UTF-8 válido. Un corte a mitad de
   secuencia haría fallar `json_encode` justo cuando más falta hace escribir en la DLQ.
6. **Reconexión con jitter.** [03-delivery.md §6](../specification/) la exige. Aquí es
   responsabilidad del cliente de NATS que inyectes, porque el SDK ya no gestiona la
   conexión — es la contrapartida del puerto. Configúralo al construir tu `Client`.

### Estricto donde los otros no lo son

Tres divergencias deliberadas, todas en la dirección de rechazar antes:

- **`time` se valida al publicar.** Node y Python dejan pasar cualquier cadena. Es más
  barato fallar en el productor que descubrir en el replay que un servicio lleva meses
  emitiendo `…39.41Z`. Pasa un `DateTimeInterface` y el SDK lo formatea.
- **`dataclassification` se valida al publicar.** Publicar un valor inventado produciría un
  POISON en cada consumidor del subject, y el productor no se enteraría nunca.
- **El nombre de servicio se valida en `connect()`.** NATS aceptaría
  `FacturacionAPI__pedidos_…` sin error; sin esta comprobación el incumplimiento solo se
  descubre al parsear nombres en una herramienta.

## Qué rechaza `Envelope::parse()`

Todo fallo de parseo es POISON: el mensaje ni siquiera es interpretable, así que nunca llega
al handler ([04-errors.md §1.3](../specification/)). Los códigos son **los mismos** que dan
Node, Python, Go, Java y .NET ante el mismo cuerpo — si divergieran, agrupar por causa en
las métricas dejaría de funcionar en cuanto el ecosistema es polyglot, que es siempre.

| `errorCode` | Cuándo |
|---|---|
| `MALFORMED_JSON` | El cuerpo no es JSON |
| `NOT_AN_OBJECT` | Es JSON pero no un objeto en la raíz |
| `UNSUPPORTED_SPECVERSION` | `specversion` != `"1.0"` |
| `MISSING_REQUIRED_ATTRIBUTE` | Falta —o vale `null`— un atributo del núcleo CloudEvents |
| `MISSING_REQUIRED_EXTENSION` | Falta —o vale `null` o `""`— `correlationid`, `tenantid`, `producerversion` o `dataclassification` |
| `INVALID_DATACLASSIFICATION` | `dataclassification` fuera de `{public, internal, confidential, restricted}` |
| `WRONG_ATTRIBUTE_TYPE` | Un atributo de texto llegó como número, booleano u objeto |
| `UNSUPPORTED_CONTENT_TYPE` | `datacontenttype` != `application/json` |
| `UNKNOWN_ROOT_ATTRIBUTE` | Un atributo raíz fuera de la lista cerrada |

Dos reglas que un port suele dar por hechas y no lo están:

- Las cuatro extensiones son **obligatorias de verdad**. No se les asume un default porque
  asumirlo es peligroso en las cuatro: un `dataclassification` ausente tomado como
  `internal` hace circular PII con 30 días de retención en vez de 7, y un `tenantid`
  ausente tomado como `system` cruza fronteras de tenant.
- **El vacío se evalúa antes que el enum**: `"dataclassification": ""` es
  `MISSING_REQUIRED_EXTENSION`, no `INVALID_DATACLASSIFICATION`. Lo fija la tabla de
  [01-envelope.md §3.1](../specification/).

## Tests

```bash
cd sdk-php
composer install
vendor/bin/phpunit          # 201 tests, 385 assertions
```

Ninguno necesita broker. Varios leen `protocol.json` del repositorio directamente, de modo
que una divergencia entre el SDK y el contrato falla en CI en vez de en producción con otro
SDK del ecosistema.

Un único test se salta sin `ext-sockets`: el que comprueba que `SOCKET_ECONNRESET` se
traduce a `"ECONNRESET"` por **nombre** de constante y no por valor (10054 en Windows, 104
en Linux). Con la extensión cargada pasa en ambos:

```bash
php -d extension=sockets vendor/bin/phpunit
```

Para los invariantes que sí requieren un servidor real —que `ack_wait` sobrevive, que un
durable con puntos es rechazado, que un publish de core a un subject mal escrito se evapora
sin error— ver [`conformance/cases/`](../conformance/).
