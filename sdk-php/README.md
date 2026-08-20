# flux SDK — PHP

Cliente de **flux Event Protocol v1** (CloudEvents 1.0 sobre NATS JetStream).
Nivel de conformidad: **L3** ([00-protocol.md §5](../specification/)) — la validación de
esquema es **opt-in**; sin activarla el comportamiento es exactamente el de L2 y no cuesta
ni una dependencia. Ver [Validación de esquema (L3)](#validación-de-esquema-l3).

Port del SDK de referencia de Node (`sdk-node/src/`), siguiendo de cerca al de Python por
ser el otro lenguaje dinámico: misma semántica, mismos defaults, mismos códigos de error.
Lo que cambia son los nombres (`camelCase`) y las piezas de plataforma que en PHP no
existen igual — todas anotadas más abajo, sin maquillaje.

```bash
composer require flux/sdk        # requiere PHP >= 8.2
```

> ### Estado de verificación
>
> ✅ **Suite ejecutada y en verde: 301 tests, 567 assertions** (PHP 8.3.30, PHPUnit
> 10.5.64, sin `ext-sodium`: 38 saltados). Cubre naming, envelope, serialización byte a
> byte, clasificación de errores, firma Ed25519, validación L3 contra el bundle real del
> repositorio, métricas, aislamiento de tenant y todo el runtime del consumidor.
> **Ninguno necesita broker.**
>
> Sin `ext-sodium` la suite sigue en verde con los tests de firma saltados, que es
> exactamente el comportamiento que se quiere: la firma es una extensión **opcional** del
> protocolo y no debe impedir usar el resto del SDK. Lo mismo vale para
> `opis/json-schema` y la validación L3.
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

> ⚠️ **`retryAfterMs` es una sugerencia para el PRIMER reintento, no un control del
> calendario de reintentos.** Con `backoff` configurado —y flux lo configura **siempre**—
> JetStream honra el delay de un `nak` solo en la primera reentrega; a partir de la segunda
> manda el array `backoff` y el delay pedido **se ignora sin ningún aviso**
> ([03-delivery.md §2.2](../specification/), medido contra NATS 2.14.5). Un `Retry-After: 5`
> de un proveedor acorta el primer reintento y nada más; los siguientes siguen el backoff
> canónico (1 m, 5 m, 15 m, 30 m). No construyas lógica que dependa de lo contrario.

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

## Firma de eventos (opcional)

[07-signing.md](../specification/). Traslada la autenticidad **del canal al evento**: hoy la
garantiza la ACL del broker, y eso deja tres huecos —un evento sacado del stream y
reinyectado, un evento exportado a un data lake donde ya no hay ACL, y un broker
comprometido que fabrica eventos—. Un evento firmado sigue siendo verificable dentro de un
fichero, un backup o un correo.

```php
use Flux\Signing\{Keys, SigningOptions, VerificationMode};

// Productor
new ConnectOptions(
    // …
    signing: new SigningOptions(privateKey: $pemPrivada, keyId: 'pedidos-api-3'),
);

// Consumidor
new ConnectOptions(
    // …
    signing: new SigningOptions(
        publicKeys: [
            'pedidos-api-3' => $pemPublica,   // activa
            'pedidos-api-2' => $pemRetirada,  // RETIRADA, conservada
        ],
        verify: VerificationMode::Require,
    ),
);

['privateKeyPem' => $priv, 'publicKeyPem' => $pub] = Keys::generateKeyPair();
```

| Modo | Evento sin firma | Firma inválida |
|---|---|---|
| `Off` (**default**) | Se acepta | Se acepta (no se mira) |
| `Warn` | Se registra por el `LoggerInterface` y se acepta | Se registra y se acepta |
| `Require` | POISON `MISSING_SIGNATURE` | POISON `INVALID_SIGNATURE` / `UNKNOWN_SIGNING_KEY` |

Cuatro cosas que un port suele dar por hechas y no lo están:

- **`Warn` no es un adorno.** Adoptar la firma en un ecosistema en marcha exige un periodo
  en el que unos productores firman y otros no; pasar directo a `Require` convierte en
  POISON todo evento de un servicio aún no migrado — es decir, tumba a los consumidores de
  los servicios que van por delante, no a los que van por detrás.
- **`Warn` DEBE ser observable** ([§7.1](../specification/)). Un evento aceptado en `warn`
  se cuenta igual como `flux_events_consumed_total{outcome="invalid_signature"}` — **no
  basta con escribir en el log**. Sin esa métrica, `warn` es inútil para lo único que
  existe, *pilotar la migración*: la pregunta "¿cuántos eventos siguen sin firma y de qué
  productores?" no la contesta un log, hay que buscarla a mano en siete servicios. Por eso
  `Verifier::check()` **devuelve el código** en vez de tragarse el fallo, y funciona sin
  `LoggerInterface` ninguno: §7.1 prohíbe explícitamente imponer una fachada de logging, la
  parte normativa es la métrica. El aviso **sustituye** al `ok` en vez de sumarse: contarlo
  dos veces rompería `sum by (outcome) == total consumido`.
- **Un fallo de firma se cuenta como `invalid_signature`, no como `poison`**
  ([§7.2](../specification/)), aunque su `dlqreason` en la DLQ sí sea `poison`. Son dos
  preguntas distintas: `poison` es "un productor publica basura" —un bug de
  serialización—; `invalid_signature` es "alguien publica eventos que no son suyos" —un
  incidente de seguridad, o una migración a medias—.
- **Una clave RETIRADA sigue verificando** mientras se conserve su pública (mínimo 90 días,
  la retención de la DLQ). Retirar una clave impide **emitir** con ella, no **verificar** lo
  ya emitido. Es la regla que más se equivoca, y equivocarla convierte una rotación
  rutinaria en la invalidación retroactiva de todo el historial.
- **Un evento que pasó por la DLQ sigue verificando.** Las extensiones `dlq*` se añaden
  después de firmar y la verificación las ignora; si no lo hiciera, todo evento en la DLQ
  parecería manipulado.

### `ext-sodium`: única dependencia, y no es de Composer

`sodium_crypto_sign_*` está en el **core de PHP desde 7.2**, así que la firma **no añade
ninguna dependencia de Composer**. Lo que sí puede faltar es la extensión: muchas
distribuciones la empaquetan aparte (`php-sodium`) y en Windows viene comentada en
`php.ini`. El SDK lo comprueba al construir el `Signer`/`Verifier` y falla con un mensaje
que dice qué instalar, en vez de con un `Call to undefined function`.

### El formato de clave **NO** diverge: es PEM, como en los otros cinco SDKs

Merece decirse explícitamente porque la tentación es la contraria. libsodium trabaja con
bytes crudos —32 de clave pública, **64 de "secret key"** (semilla ‖ pública)— y los otros
SDKs usan PEM: PKCS#8 la privada, SPKI la pública. Adoptar el formato nativo de libsodium
habría hecho que **una clave generada en PHP no sirviera en ningún otro SDK y al revés**, y
como [07-signing.md §6](../specification/) obliga a conservar las públicas retiradas 90 días,
eso multiplica por seis el material que hay que custodiar y traducir en cada rotación.

La conversión sale gratis porque la envoltura DER de una clave Ed25519 es de **tamaño fijo**
(RFC 8410): 16 bytes de cabecera constante + 32 de semilla en PKCS#8, y 12 + 32 en SPKI. No
hay que parsear ASN.1, hay que comparar un prefijo — 30 líneas en `Flux\Signing\Keys`, sin
`ext-openssl` (que además no sabe firmar Ed25519). El SDK de Rust hace exactamente lo mismo.

También se acepta la clave **cruda en base64** (32 bytes de semilla, o los 64 de libsodium),
porque es lo que devuelve `sodium_crypto_sign_keypair()` y lo que entregan algunos gestores
de secretos. Las dos formas producen firmas idénticas y hay un test que lo fija.

La interoperabilidad no se afirma, se fija con un test: `SigningTest` lleva un **vector
FIJO** —la semilla del TEST 1 de RFC 8032, un evento literal y su firma en base64url— que
producen y aceptan por igual este SDK, `node:crypto` y `ed25519-dalek` de Rust. Lleva además
el evento de DLQ firmado **byte a byte**, el mismo literal que tiene el SDK de Rust.

## Validación de esquema (L3)

[00-protocol.md §5](../specification/). Es lo que separa L2 de L3, y cierra el hueco más
grande que quedaba: **sin ella, un productor puede publicar un payload que viola su propio
`dataschema` y nadie se entera** hasta que un consumidor —posiblemente de otro equipo, otro
lenguaje y otra semana— se atraganta. Para entonces el evento malo ya está en el stream y
no hay forma de retirarlo. Validar en `publish()` lo convierte en un fallo del servicio que
lo provocó.

```bash
composer require opis/json-schema     # opcional: solo si vas a usar L3
node scripts/bundle-schemas.mjs       # genera schemas/bundle.json
```

```php
use Flux\Validation\{SchemaBundle, ValidationMode, ValidationOptions};

$bus = FluxBus::connect(new ConnectOptions(
    service: 'pedidos-api',
    environment: 'produccion',
    version: '3.4.1',
    validation: new ValidationOptions(
        mode: ValidationMode::Strict,           // off (default) | warn | strict
        bundle: SchemaBundle::fromFile(__DIR__ . '/schemas/bundle.json'),
        onConsume: false,                       // validar también al consumir
    ),
), $transport);
```

| Modo | Qué hace |
|---|---|
| `Off` (default) | No valida. Es L2 y **no cuesta nada**: no compila esquemas y la librería no hace falta |
| `Warn` | Registra el incumplimiento por el `logger` y publica igual. El modo de la migración |
| `Strict` | **`publish()` lanza `SchemaValidationError`.** El evento no llega al stream |

**Reporta TODOS los errores, no solo el primero.** No es un detalle de presentación: de uno
en uno, arreglar un payload con tres campos mal cuesta tres despliegues. Cada línea lleva su
ruta dentro del payload (`/lineas/0/cantidad`).

```
el payload de "pedidos.pedido.v1.creado" no cumple su esquema (https://…/1.0.0.json):
  · /totalCents The data (string) must match the type: integer
  · /moneda The string should match pattern: ^[A-Z]{3}$
```

### El bundle se despliega con el servicio; el `dataschema` NO se resuelve por red

El `dataschema` es una URI y la tentación evidente es resolverla por HTTP. Un SDK L3 **NO
DEBE** hacerlo, y las dos razones no se arreglan con más ingeniería:

1. **Validar está en la ruta caliente.** Una petición por evento publicado es inaceptable.
2. **Una caché con TTL abre una ventana de incoherencia** en la que dos servicios validan
   contra versiones distintas del mismo esquema, y ninguno se entera.

Por eso el bundle es un **dato que se despliega con el servicio**: así la versión del
esquema queda clavada a la versión del servicio, que es justo lo que `producerversion`
promete poder acotar. El bundle resuelve además el `dataschema` **exacto** de cada subject
(el MINOR más alto de su mayor); sin él solo se puede asumir el `.0.0`, que basta para L2
pero no para L3.

### Al consumir: PERMANENT, nunca reintento

Con `onConsume: true`, un evento que incumple su esquema va **directo a la DLQ** con
`dlqreason: permanent` y sin gastar reintentos. El evento parseó como CloudEvent, así que no
es POISON; y su `data` no va a cambiar entre entregas, así que reintentarlo son 51 minutos de
cola bloqueada para llegar al mismo sitio ([04-errors.md §1.2](../specification/)). En
métricas sale como `outcome="invalid_schema"`, no como `permanent`: "un productor publica
algo que incumple su contrato" es un bug de **otro servicio**, mientras que `permanent` es
"mi lógica de negocio rechazó este evento" —una decisión—, y sumarlos haría que un productor
roto se leyese como reglas de negocio funcionando.

### El coste de la dependencia, y qué pasa sin ella

`opis/json-schema` está en `suggest`, no en `require`. **L3 es opt-in, así que su coste
también debe serlo**: un servicio en L2 no debería arrastrar un validador de JSON Schema
—tres paquetes: `opis/json-schema`, `opis/string`, `opis/uri`— que no va a ejecutar nunca.
Es la misma decisión que `ext-sodium` para la firma.

Sin la librería instalada, `ValidationMode::Off` funciona con normalidad y cualquier otro
modo falla **al conectar** (no al publicar el primer evento) con un mensaje que dice qué
instalar y por qué.

> ⚠️ **Hace falta soporte de draft 2020-12** (`opis/json-schema ^2.4`; verificado con
> 2.6.0). Los esquemas de flux declaran `$schema: .../draft/2020-12/schema`, y un validador
> de draft-07 **no da un error de versión**: da `no schema with key or ref "…/2020-12/…"`,
> que no dice absolutamente nada sobre la causa real.

> ⚠️ **Trampa de la librería, ya resuelta dentro del SDK.** Opis arranca con
> `maxErrors = 1`: con los defaults reportaría un solo error e incumpliría el requisito de
> L3. El SDK lo sube al construir el validador. En sentido contrario, `stopAtFirstError` se
> deja en `true` a propósito — ponerlo en `false` hace que `additionalProperties` reporte el
> conjunto entero de propiedades examinadas, así que un payload con dos campos mal produce
> además un error que afirma que los campos **válidos** "no están permitidos".

## Métricas

[08-observability.md](../specification/), normativo para L2. Las siete métricas, con sus
nombres y etiquetas exactos, **son contrato entre SDKs**: si el de PHP y el de Go nombraran
distinto la tasa de DLQ, un panel del ecosistema sería imposible.

```php
use Flux\Metrics\InMemoryMetrics;

$metrics = new InMemoryMetrics();
$bus = FluxBus::connect(new ConnectOptions(/* … */, metrics: $metrics), $transport);

// …dentro del worker, o en un endpoint de scrape del propio proceso
echo $metrics->render();
```

El default es `NoMetrics`: un SDK no debe imponer un backend. `InMemoryMetrics` es un
recolector **sin dependencias** que renderiza el formato de texto de Prometheus; si ya usas
un cliente de Prometheus, implementa `Flux\Metrics\MetricsSink` contra él — lo que importa
es conservar los nombres.

⚠️ **`MetricsSink` tiene un método por métrica con parámetros propios, no un
`array $labels`.** No es estilo: un mapa de etiquetas es exactamente el agujero por el que
se cuela un `tenantid` que multiplica las series temporales. Con esta forma, etiquetar por
tenant exige cambiar la firma de la interfaz, y eso se ve en una revisión. **Nunca** se
etiqueta por `tenantid`, `id` ni `correlationid` (§2.2); para eso están las trazas.

El último bucket del histograma es `30` porque **es el `ack_wait`**: un handler que cae ahí
está a punto de que su mensaje se reentregue mientras aún se ejecuta.
`MetricsTest::testElUltimoBucketEsElAckWait()` lo ata a `Protocol::ACK_WAIT_MS` para que no
se desincronicen, y otro test los compara con `protocol.json`.

### `flux_consumer_pending`: dos fuentes, y una de ellas solo existe en el worker

[08-observability.md §2.3](../specification/) exige **las dos**, porque cada una falla justo
donde la otra sirve:

| Fuente | Coste | Falla cuando… |
|---|---|---|
| Metadatos del mensaje entregado | Gratis, fresco en cada evento | **No llegan mensajes** — y ese es el caso que importa |
| Sondeo de `num_pending` al servidor | Una petición cada ~15 s | Nunca, mientras haya conexión |

El razonamiento decide la regla: **si el bucle del consumidor muere, dejan de entregarse
mensajes**, así que un gauge alimentado solo desde los metadatos se queda **plano en su
último valor** en vez de crecer. En un panel eso es una línea horizontal, indistinguible de
"no pasa nada", mientras la cola crece sin techo y la conexión se sigue reportando sana.

El intervalo se configura con `pendingPollMs` (default `15000`; `0` lo desactiva), y **un
fallo del sondeo nunca afecta al consumo**: es telemetría, se registra y se sigue.

```php
new ConnectOptions(/* … */, pendingPollMs: 15_000);
```

> ⚠️ **Limitación real de PHP, sin maquillaje.** El sondeo vive **dentro de `run()`**, y en
> PHP no puede vivir en otro sitio: no hay temporizadores en segundo plano ni bucle de
> eventos donde colgar un `setInterval`. Consecuencias concretas:
>
> - **En el worker CLI funciona como en los demás SDKs.** El sondeo corre entre vueltas del
>   bucle, respeta el intervalo y emite el gauge aunque no llegue ni un mensaje — que es
>   exactamente el caso que la métrica existe para detectar.
> - **Bajo FPM no sondea, y no puede.** Un proceso que solo publica no tiene bucle. Pero eso
>   no deja ningún hueco de observabilidad: un proceso que no consume tampoco tiene
>   consumidores de los que reportar `num_pending`. La métrica pertenece al worker, y el
>   worker sí la emite.
> - **Si el worker entero muere**, no queda nadie sondeando. Ningún SDK puede resolver eso
>   desde dentro del proceso que se ha muerto: se detecta por ausencia de scrape o por
>   `up == 0`, no con esta métrica.

> ⚠️ **`InMemoryMetrics` vive en la memoria del proceso.** En un worker CLI de larga vida
> (`$bus->run()`) es justo lo que se quiere. **Bajo FPM no**: cada petición es un proceso
> nuevo y los contadores nacen a cero, así que un `/metrics` servido desde FPM reportaría
> casi nada siempre. Para publicar métricas de un publisher web hace falta un backend con
> estado compartido (APCu, Redis) implementando `MetricsSink`. Es la contrapartida del
> modelo de ejecución de PHP, no una limitación del recolector.

## Aislamiento de tenant

[09-multitenancy.md §3](../specification/). El Modelo A de v1 mezcla todos los tenants en un
stream por dominio y **el aislamiento es una convención del SDK, no una frontera del
broker**.

```php
new ConnectOptions(
    // …
    tenantId: 'acme',
    tenantIsolation: TenantIsolation::Strict,
);
```

- En `Strict`, **suscribirse sin filtro de tenant lanza `TenantIsolationException`**, no es
  un descuido silencioso. Ese es el punto entero de la sección: un filtro que hay que
  acordarse de poner es un filtro que alguien olvidará, y el fallo —ver los datos de otro
  tenant— no produce ningún error; produce un incidente de privacidad que se descubre
  semanas después.
- El error llega **antes** de crear el durable consumer: un servicio mal configurado no
  arranca, en vez de arrancar y fallar cuando ya está en producción.
- El filtrado ocurre **antes del handler**. El evento ajeno se **ACKea** y se descarta: no es
  un fallo, no es para nosotros.
- **`"system"` NO cuenta como filtro.** Es la *ausencia* de tenant, reservada a los eventos
  de plataforma, y está prohibido usarla como comodín (§5). Como además es el **valor por
  defecto** de `ConnectOptions::$tenantId`, si contase, el modo estricto no protegería
  precisamente en el caso más probable: el de quien olvidó configurar el tenant.

Lo que `Strict` **no** hace: cerrar las dos amenazas que §1 declara descubiertas —un
productor legítimo comprometido que publica con el `tenantid` de otro, y un consumidor
comprometido que lee el subject entero—. Para eso hace falta el Modelo B (una account de
NATS por tenant). Lo que sí añade la firma es que alterar el `tenantid` en tránsito o en
reposo **invalida la firma** (§4).

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
| `signing.ts` / `metrics.ts` sueltos | `Flux\Signing\*` y `Flux\Metrics\*` | Mismo motivo que arriba: PSR-4, una clase por fichero. La contrapartida es que `VerificationMode`, `ConnectionState` y los `outcome` son **enums de verdad** en vez de uniones de cadenas, así que un valor de etiqueta inventado no compila |
| `node:crypto` con PEM | `ext-sodium` **con PEM** | libsodium usa bytes crudos, pero el SDK convierte desde/hacia PEM para no romper la interoperabilidad de claves. Ver arriba |
| WIP automático → duración medida por el temporizador | `microtime(true)` alrededor del handler | Sin bucle de eventos no hay temporizador, pero la duración del handler sí se puede medir: es código estrictamente secuencial |

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
7. **Las métricas no sobreviven a la petición.** `InMemoryMetrics` vale en un worker CLI y
   no bajo FPM, donde cada petición arranca a cero. Ver la sección de Métricas. Es el mismo
   modelo de ejecución que obliga al WIP manual, visto desde otro lado.
8. **`flux_connection_state` es lo que el transporte diga.** Sin bucle de eventos no hay
   quien observe una reconexión, así que el gauge se escribe en `connect()` y en `close()`,
   y el valor `2` (reconectando) **no se emite nunca** desde este SDK. Existe en el enum
   porque la etiqueta es del protocolo; si tu transporte sabe distinguirlo, es una línea.
9. **Formatear un `float` para Prometheus no es `(string)`.** El casting de PHP puede dar
   notación científica (`1.0E-5`) y, en algunas builds, el separador decimal del locale.
   `InMemoryMetrics` formatea a mano, y además imprime `le="30"` y no `le="30.0"`: para
   Prometheus son **dos series distintas** y el dashboard del ecosistema agrupa por la
   primera.

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

### Fricción encontrada en este port: §4 no dice dónde van `signkeyid` y `signature`

[07-signing.md §4](../specification/) dice que van "entre las extensiones, antes de `data`".
Eso deja sin resolver **dónde exactamente respecto a las `dlq*`**, y hay dos respuestas
defendibles: la lista de atributos permitidos del SDK de Node los declara detrás de
`dlqtime`, pero su `toDlqEvent` los emite delante, porque construye el evento de DLQ como
`{...evento_firmado, dlq*, data}`.

Elegir mal **no rompe la firma** —la verificación quita las `dlq*` en cualquier caso, así que
el payload firmado sale idéntico— pero sí rompe la igualdad byte a byte del mensaje que
acaba en la DLQ, y de esos bytes dependen el replay verbatim, la deduplicación por hash de
contenido y los fixtures compartidos. Es exactamente la misma clase de divergencia que el
`{...event, dlq*}` que dio origen a [01-envelope.md §6](../specification/), y con la misma
forma: silenciosa, e invisible para cualquier test que compare datos en vez de bytes.

Este SDK las emite **antes** de las `dlq*`, que es lo que hacen Node, Python, Go, Java, .NET
y Rust. Está fijado con un vector literal (`SigningTest::DLQ_VECTOR`) que el SDK de Rust
lleva idéntico.

**Sugerencia para la spec:** §4 debería nombrar la posición completa —`… tracestate,
signkeyid, signature, dlqreason, …, dlqtime, data`— en vez de "antes de `data`". Un SDK
nuevo no puede deducirla, y el primero que la deduzca al revés no se enterará.

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

Y con `signing.verify` distinto de `Off`, tres más — [07-signing.md §7](../specification/):

| `errorCode` | Cuándo |
|---|---|
| `MISSING_SIGNATURE` | Falta `signature` en modo `require` |
| `INVALID_SIGNATURE` | La firma no verifica (evento alterado, o no lo emitió quien dice) |
| `UNKNOWN_SIGNING_KEY` | `signkeyid` desconocido, o `signature` sin `signkeyid` |

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
vendor/bin/phpunit                                    # 301 tests, 567 assertions (38 saltados)
php -d extension=sodium -d extension=sockets vendor/bin/phpunit   # 0 saltados
```

Ninguno necesita broker. Varios leen `protocol.json` del repositorio directamente, de modo
que una divergencia entre el SDK y el contrato falla en CI en vez de en producción con otro
SDK del ecosistema — incluidos los buckets del histograma, la lista de las siete métricas y
las etiquetas prohibidas.

| Suite | Tests | Se salta sin |
|---|--:|---|
| `EnvelopeTest`, `ProtocolTest`, `ClassifierTest`, `FluxBusTest`, `AckTest`, … | 203 | 1 de ellos, `ext-sockets` |
| `SigningTest` | 33 | `ext-sodium` |
| `ValidationTest` (bundle real del repositorio, publish, consumo y sondeo) | 29 | — |
| `MetricsTest` | 19 | — |
| `TenantIsolationTest` (incluye firma y métricas **a través del bus**) | 17 | 4 de ellos, `ext-sodium` |

Dos extensiones opcionales hacen que algunos tests se salten, y en los dos casos eso es
deliberado:

- **`ext-sodium`** (37 tests): la firma es una extensión **opcional** del protocolo, así que
  no tenerla no debe impedir usar el resto del SDK.
- **`ext-sockets`** (1 test): el que comprueba que `SOCKET_ECONNRESET` se traduce a
  `"ECONNRESET"` por **nombre** de constante y no por valor (10054 en Windows, 104 en Linux).

Para los invariantes que sí requieren un servidor real —que `ack_wait` sobrevive, que un
durable con puntos es rechazado, que un publish de core a un subject mal escrito se evapora
sin error— ver [`conformance/cases/`](../conformance/).
