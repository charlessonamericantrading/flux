# flux SDK para Rust

Cliente del **flux Event Protocol v1** — CloudEvents 1.0 sobre NATS JetStream.
Nivel de conformidad objetivo: **L2**.

El contrato normativo vive en [`specification/`](../specification/); si algo de este
README diverge de la spec, manda la spec.

```toml
[dependencies]
flux = { path = "../sdk-rust" }   # o git = "https://github.com/charlessonamericantrading/flux"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde = { version = "1", features = ["derive"] }
```

Requiere **Rust 1.83+** (por `io::ErrorKind::{HostUnreachable, NetworkUnreachable}`, que
el clasificador usa para reconocer lo transitorio por semántica), `async-nats` 0.50+ y un
runtime Tokio.

| Feature | Por defecto | Qué añade |
|---|---|---|
| `signing` | **no** | Firma Ed25519 de eventos ([07-signing.md](../specification/07-signing.md)) vía `ed25519-dalek` 2.x |

La firma va detrás de una feature porque es una **extensión opcional del protocolo** —un
evento sin firma sigue siendo válido— y arrastrar `curve25519-dalek` al árbol de
dependencias de todo servicio del ecosistema, con su tiempo de compilación y su superficie
de auditoría, sería cobrar a todos por lo que usan unos pocos. Las métricas y el
aislamiento de tenant **no** son opcionales: son parte del contrato L2 y van siempre.

---

## Publicar

```rust
use serde::Serialize;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PedidoCreado {
    pedido_id: String,
    cliente_id: String,
    aggregate_version: u64,
    total_cents: i64,
    moneda: String,
}

let bus = flux::connect(
    flux::ConnectOptions::new("nats://localhost:4222", "pedidos-api", "produccion", "3.4.1")
        .with_tenant_id("acme")
        .with_schema_base_url("https://schemas.internal"),
)
.await?;

bus.publish_with(
    "pedidos.pedido.v1.creado",
    &PedidoCreado {
        pedido_id: "ped-123".into(),
        cliente_id: "cli-987".into(),
        aggregate_version: 1,
        total_cents: 9990,
        moneda: "EUR".into(),
    },
    flux::PublishOptions::default().with_aggregate_id("ped-123"),
)
.await?;
```

**Solo escribes subject, `data` y opcionalmente el `aggregate_id`.** El SDK rellena `id`
(UUIDv7), `source`, `time`, `specversion`, `type`, `dataschema`, `correlationid`,
`causationid`, `producerversion` y `traceparent`. Si tu código asigna alguno de esos a
mano, está mal — [01-envelope.md §5](../specification/01-envelope.md).

## Consumir

```rust
let sub = bus
    .subscribe("pedidos.pedido.v1.creado", |ev: flux::Event, _d| async move {
        let pedido = ev.data_as::<PedidoCreado>()?;   // PERMANENT → DLQ inmediato

        if ya_procesado(&ev.id).await { return Ok(()); }   // idempotencia: OBLIGATORIA
        hacer_el_trabajo(&pedido)
            .await
            .map_err(|e| flux::FluxError::retryable("proveedor caído").with_source(e))?;
        Ok(())                                        // Ok(()) == ack explícito
    })
    .await?;

sub.unsubscribe();
```

- **`Ok(())` ACK-ea.** Devolver `Err` clasifica el error y produce `nak`, `term` o
  `term`+alerta. El SDK jamás confirma un mensaje antes de que el handler termine.
- **Todo handler DEBE ser idempotente.** La garantía es *at-least-once*: los duplicados
  llegan, no son un fallo. Elige una de las tres estrategias de
  [03-delivery.md §4](../specification/03-delivery.md).
- **Nunca asumas orden.** Incluye `aggregateVersion` en `data` y filtra con
  `WHERE aggregate_version < $n`.

> El tipo del primer parámetro del closure **hay que anotarlo** (`|ev: flux::Event, _d|`).
> Ver la fricción **H** más abajo: no es un capricho de estilo, es una limitación de la
> inferencia de Rust sobre closures que implementan un trait genérico.

## Errores

```rust
return Err(flux::FluxError::retryable("proveedor 503")
    .with_retry_after(Duration::from_secs(5))
    .into());
return Err(flux::FluxError::permanent("pedido ya cancelado")
    .with_code("PEDIDO_YA_CANCELADO")
    .into());
```

| Clase | Qué es | Acción |
|---|---|---|
| `ErrorClass::Retryable` | Timeout, `ECONNRESET`, HTTP 429/502/503/504 | `nak` + backoff |
| `ErrorClass::Permanent` | Falla el schema, regla de negocio, HTTP 400/403/404/422 | `term` + DLQ inmediato |
| `ErrorClass::Poison` | JSON malformado, falta un atributo CloudEvents o una extensión obligatoria del perfil flux | `term` + DLQ + alerta |

`parse_event` etiqueta cada fallo con un `code` estable —el mismo que dan Node, Python, Go,
Java y .NET ante el mismo cuerpo— porque es lo que acaba en la columna de la DLQ y en las
métricas. Además de `MALFORMED_JSON`, `NOT_AN_OBJECT`, `UNSUPPORTED_SPECVERSION`,
`MISSING_REQUIRED_ATTRIBUTE`, `UNSUPPORTED_CONTENT_TYPE` e `INVALID_ATTRIBUTE_TYPE`:

| `code` | Cuándo |
|---|---|
| `MISSING_REQUIRED_EXTENSION` | Falta —o vale `null` o `""`— `correlationid`, `tenantid`, `producerversion` o `dataclassification`. No se les asume un default: un `dataclassification` tomado como `internal` haría circular PII con 30 días de retención en vez de 7, y un `tenantid` tomado como `system` cruzaría fronteras de tenant ([§3.1](../specification/01-envelope.md)) |
| `INVALID_DATACLASSIFICATION` | `dataclassification` fuera de `{public, internal, confidential, restricted}`. El vacío se evalúa **antes**: `""` es `MISSING_REQUIRED_EXTENSION` |
| `UNKNOWN_ROOT_ATTRIBUTE` | Atributo raíz fuera de la lista cerrada. Incluye `{"ID": …}`: la comparación de nombres es case-sensitive ([§2.3](../specification/01-envelope.md)) |
| `WRONG_ATTRIBUTE_TYPE` | Un atributo de texto llegó como número, booleano u objeto. `serde` ya lo rechazaría al deserializar el struct, pero con el código genérico de desajuste de tipo; se comprueba antes para que el operador distinga un `{"tenantid": 42}` de un `{"dlqattempts": "seis"}` ([§2.4](../specification/01-envelope.md)) |

**Default de lo desconocido: `RETRYABLE` con presupuesto acotado de 2 entregas**
([04-errors.md §2.1](../specification/04-errors.md)).

```
Error reconocido como transitorio (ECONNRESET, 503) → 6 entregas, hasta 51 min
Error desconocido                                   → 2 entregas, ~30 s
Error reconocido como permanente (400, 422)         → 1 entrega, sin espera
```

El presupuesto **no** se configura en `max_deliver`: eso es por consumidor, no por mensaje,
y bajarlo a 2 recortaría también los reintentos de los `RETRYABLE` reconocidos. El
clasificador rellena `Classification::max_attempts` solo para los errores desconocidos y el
runtime aplica `min(max_deliver, max_attempts)` a ese error concreto.

```rust
flux::ConnectOptions::new(/* … */).with_classifier(flux::ClassifierOptions {
    unknown_policy: flux::UnknownPolicy::RetryableBounded, // o Permanent / Retryable
    unknown_retry_budget: 2,
    timeout_policy: flux::ErrorClass::Retryable,
    rules: vec![],
})
```

Para que el clasificador reconozca un status HTTP, envuelve el fallo con
`flux::HttpError::new(status, msg)` o registra una regla en `rules` que reconozca el error
de tu cliente HTTP.

> ⚠️ **`with_retry_after` es una sugerencia para el PRIMER reintento, no un control del
> calendario.** Con `backoff` configurado —y flux lo configura siempre— JetStream ignora el
> delay de un `nak` a partir de la segunda reentrega, sin avisar
> ([03-delivery.md §2.2](../specification/03-delivery.md)). Un `Retry-After: 5` de un
> proveedor acorta el primer reintento y nada más. Ver la fricción **G**.

---

## Firma de eventos (opcional)

[07-signing.md](../specification/07-signing.md). Traslada la autenticidad **del canal al
evento**: hoy la garantiza la ACL del broker, y eso deja tres huecos —un evento sacado del
stream y reinyectado, un evento exportado a un data lake donde ya no hay ACL, y un broker
comprometido que fabrica eventos—. Un evento firmado sigue siendo verificable dentro de un
fichero, un backup o un correo.

```toml
flux = { path = "../sdk-rust", features = ["signing"] }
```

```rust
// Productor
flux::ConnectOptions::new(/* … */).with_signing(
    flux::SigningOptions::default().with_private_key(&pem_privada, "pedidos-api-3"),
);

// Consumidor
flux::ConnectOptions::new(/* … */).with_signing(
    flux::SigningOptions::default()
        .with_public_key("pedidos-api-3", &pem_publica)   // activa
        .with_public_key("pedidos-api-2", &pem_retirada)  // RETIRADA, conservada
        .with_verify(flux::VerificationMode::Require),
);
```

| Modo | Evento sin firma | Firma inválida |
|---|---|---|
| `Off` (**default**) | Se acepta | Se acepta (no se mira) |
| `Warn` | Se registra por el `LogFn` y se acepta | Se registra y se acepta |
| `Require` | POISON `MISSING_SIGNATURE` | POISON `INVALID_SIGNATURE` / `UNKNOWN_SIGNING_KEY` |

Cuatro cosas que un port suele dar por hechas y no lo están:

- **`Warn` no es un adorno.** Adoptar la firma en un ecosistema en marcha exige un periodo
  en el que unos productores firman y otros no; pasar directo a `Require` convierte en
  POISON todo evento de un servicio aún no migrado.
- **`Warn` DEBE ser observable** ([§7.1](../specification/07-signing.md)). Un evento
  aceptado en `warn` se cuenta igual como
  `flux_events_consumed_total{outcome="invalid_signature"}` — **no basta con escribir en el
  log**. Sin esa métrica, `warn` es inútil para lo único que existe, *pilotar la migración*:
  la pregunta "¿cuántos eventos siguen sin firma y de qué productores?" no la contesta un
  log, hay que buscarla a mano en siete servicios. Por eso `Verifier::check` devuelve
  `Ok(Some(code))` en vez de tragarse el fallo, y por eso el aviso **sustituye** al `ok` en
  vez de sumarse: contarlo dos veces rompería `sum by (outcome) == total consumido`.
- **Un fallo de firma se cuenta como `invalid_signature`, no como `poison`**
  ([§7.2](../specification/07-signing.md)), aunque su `dlqreason` en la DLQ sí sea `poison`.
  Son dos preguntas distintas: `poison` es "un productor publica basura" —un bug de
  serialización—; `invalid_signature` es "alguien publica eventos que no son suyos" —un
  incidente de seguridad, o una migración a medias—.
- **Una clave RETIRADA sigue verificando** mientras se conserve su pública (mínimo 90 días,
  la retención de la DLQ). Retirar una clave impide **emitir** con ella, no **verificar** lo
  ya emitido. Es la regla que más se equivoca, y equivocarla convierte una rotación
  rutinaria en la invalidación retroactiva de todo el historial.
- **Un evento que pasó por la DLQ sigue verificando.** Las extensiones `dlq*` se añaden
  después de firmar y la verificación las ignora; si no lo hiciera, todo evento en la DLQ
  parecería manipulado.

**Formato de clave: PEM** —PKCS#8 la privada, SPKI la pública—, el mismo que Node, Python,
Go, Java, .NET y PHP. Una clave generada por cualquier SDK vale en todos los demás. También
se acepta la clave cruda en base64 (32 bytes de semilla, o los 64 de "secret key" de
libsodium), porque es la forma en que la entregan algunos gestores de secretos.
`flux::generate_key_pair()` devuelve el par en PEM.

La interoperabilidad no se afirma, se fija con un test: `signing::tests` lleva un **vector
FIJO** —la semilla del TEST 1 de RFC 8032, un evento literal y su firma en base64url— que
producen y aceptan por igual este SDK, `node:crypto` y `sodium_crypto_sign_detached` de PHP.
Si alguien toca el serializador, el orden de claves o el formato de `time`, ese test cae
antes que cualquier despliegue.

## Métricas

[08-observability.md](../specification/08-observability.md), normativo para L2. Las siete
métricas, con sus nombres y etiquetas exactos, **son contrato entre SDKs**: si el de Rust y
el de Go nombraran distinto la tasa de DLQ, un panel del ecosistema sería imposible.

```rust
let metrics = std::sync::Arc::new(flux::InMemoryMetrics::new());
let bus = flux::connect(
    flux::ConnectOptions::new(/* … */).with_metrics(metrics.clone()),
).await?;

// Sírvelo tal cual en /metrics
println!("{}", metrics.render());
```

El default es `NoMetrics`: un SDK no debe imponer un backend. `InMemoryMetrics` es un
recolector **sin dependencias** que renderiza el formato de texto de Prometheus; si ya usas
un cliente de Prometheus o OpenTelemetry, implementa `MetricsSink` contra él — lo que
importa es conservar los nombres.

⚠️ **`MetricsSink` tiene un método por métrica con parámetros propios, no un
`labels: HashMap`.** No es estilo: un mapa de etiquetas es exactamente el agujero por el que
se cuela un `tenantid` que multiplica las series temporales. Con esta forma, etiquetar por
tenant exige cambiar la firma del trait, y eso se ve en una revisión. **Nunca** se etiqueta
por `tenantid`, `id` ni `correlationid` (§2.2); para eso están las trazas.

El último bucket del histograma es `30` porque **es el `ack_wait`**: un handler que cae ahí
está a punto de que su mensaje se reentregue mientras aún se ejecuta. Un test
(`metrics::tests::el_ultimo_bucket_es_el_ack_wait`) lo ata a `DEFAULT_ACK_WAIT` para que no
se desincronicen.

## Aislamiento de tenant

[09-multitenancy.md §3](../specification/09-multitenancy.md). El Modelo A de v1 mezcla todos
los tenants en un stream por dominio y **el aislamiento es una convención del SDK, no una
frontera del broker**.

```rust
flux::ConnectOptions::new(/* … */)
    .with_tenant_id("acme")
    .with_tenant_isolation(flux::TenantIsolation::Strict);
```

- En `Strict`, **suscribirse sin filtro de tenant es un `FluxError::TenantIsolation`**, no
  un descuido silencioso. Ese es el punto entero de la sección: un filtro que hay que
  acordarse de poner es un filtro que alguien olvidará, y el fallo —ver los datos de otro
  tenant— no produce ningún error; produce un incidente de privacidad que se descubre
  semanas después.
- El filtrado ocurre **antes del handler**. El evento ajeno se **ACKea** y se descarta: no
  es un fallo, no es para nosotros.
- **`"system"` NO cuenta como filtro.** Es la *ausencia* de tenant, reservada a los eventos
  de plataforma, y está prohibido usarla como comodín (§5). Como además es el valor por
  defecto cuando no se configura ninguno, si contase el modo estricto no protegería
  precisamente en el caso más probable: el de quien olvidó configurar el tenant.

Lo que `Strict` **no** hace: cerrar las dos amenazas que §1 declara descubiertas —un
productor legítimo comprometido que publica con el `tenantid` de otro, y un consumidor
comprometido que lee el subject entero—. Para eso hace falta el Modelo B (una account de
NATS por tenant). Lo que sí añade la firma es que alterar el `tenantid` en tránsito o en
reposo **invalida la firma** (§4).

---

## Diferencias con los SDKs de referencia

El envelope, el naming, la taxonomía de errores y la config de consumidor son **idénticos
byte a byte**. Estas divergencias son de lenguaje, no de contrato.

### 1. Contexto por task-local — como Node, no como Go

Ésta es la única decisión donde Rust se separa de Go y se acerca a Node, y es porque
**Rust tiene el mecanismo que a Go le falta**. Go propaga el contexto de forma explícita
por `context.Context` porque no hay almacenamiento ligado al goroutine y emularlo por
goroutine ID es un antipatrón. Rust tiene [`tokio::task_local!`], que es de primera clase.

```rust
bus.subscribe("pedidos.pedido.v1.creado", move |ev: flux::Event, _d| {
    let bus = publicador.clone();
    async move {
        // correlationid, causationid, tenantid y traceparent se propagan solos
        bus.publish("facturacion.factura.v1.emitida", &payload).await?;
        Ok(())
    }
})
```

**El límite, dicho en voz alta:** un task-local **no cruza `tokio::spawn`**. Si el handler
lanza un task hijo y publica desde ahí, la cadena de correlación se rompe. La reparación es
una línea, y el camino explícito existe siempre:

```rust
let ctx = flux::context::current();               // capturado ANTES del spawn
tokio::spawn(flux::context::scope(ctx, async move { /* … */ }));

// o, explícito de principio a fin:
bus.publish_with(subject, &data, flux::PublishOptions::default().with_context(ctx)).await?;
```

El contexto explícito **gana** sobre el task-local, para que un job diferido pueda reanudar
una cadena de correlación leída de una tabla.

### 2. Status HTTP por tipo concreto, no por interfaz

Node hurga en `err.status`, `err.statusCode` y `err.response.status`. Go declara una
interfaz `HTTPStatusError` y la localiza con `errors.As`. **Rust no puede hacer ninguna de
las dos**: `dyn Error` solo permite `downcast_ref` a un tipo **concreto**, no a otro trait,
así que un trait `HttpStatusError` sería indetectable desde el clasificador.

El contrato es por tanto el tipo concreto `flux::HttpError`, localizado recorriendo la
cadena de `source()` — lo que sí funciona a través de errores envueltos, igual que en Go y
mejor que el `instanceof` de Node. Para un cliente HTTP de terceros que no quieras
envolver, el punto de extensión es `ClassifierOptions::rules`.

### 3. Log y `traceparent` inyectados

Node toma `pino` y Go `log/slog`, que son la elección obvia de cada ecosistema. **En Rust no
la hay** —`tracing` y `log` conviven— y una dependencia dura obligaría a todo servicio a
arrastrar la que no usa. Se invierte: la aplicación pasa un closure
(`ConnectOptions::with_logger`). Lo mismo con `traceparent`, por la misma razón que en Go:
Node hace un `import()` dinámico de `@opentelemetry/api` y Rust no tiene imports dinámicos.

### 4. Un solo enum de error en vez de tres clases

Node y Go definen `RetryableError`, `PermanentError` y `PoisonError` como tipos separados.
Aquí son tres variantes de `FluxError`, porque en Rust un enum permite `match` exhaustivo
sobre la taxonomía —el compilador avisa si añades una clase y olvidas tratarla— y hace
**imposible por construcción** un "retryable con clase permanent". El detalle común
(`message`, `code`, `retry_after`, `source`) vive en un único struct `Failure`.

El handler devuelve `Box<dyn Error + Send + Sync>` y no `FluxError`: así puede devolver el
error de *su* dominio (`sqlx::Error`, `reqwest::Error`) sin envolverlo, y el clasificador lo
recorre con `source()`.

### 5. `Classification::max_attempts` es `Option<u32>`, no un entero con cero mágico

En Go es un `int` donde `0` significa "sin tope propio", porque un `*int` añadiría una
asignación y un alias mutable a un struct que se copia en cada despacho. `Option<u32>` no
tiene ese coste y expresa exactamente lo que dice el protocolo: **ausente ≠ cero**
([§3.3](../specification/01-envelope.md)).

### 6. El backoff canónico es una constante, no una función

Go devuelve una copia nueva en cada llamada porque un slice a nivel de paquete sería mutable
desde fuera. En Rust `CANONICAL_BACKOFF` es un `[Duration; 5]` `const`: inmutable por
construcción, sin copia defensiva y comprobable en tiempo de compilación.

### 7. `#![deny(warnings)]` **no** se usa

El crate declara `#![forbid(unsafe_code)]` y `#![deny(missing_docs)]`, pero **no**
`deny(warnings)`: eso convierte cualquier lint nuevo del compilador en un build roto para
quien compile con un toolchain más moderno, y este SDK lo consumen servicios que no
controlan su versión de Rust. Los warnings se deniegan donde el toolchain sí está fijado:

```bash
cargo clippy --all-targets -- -D warnings
```

---

## Fricciones: dónde el protocolo no encaja limpio en Rust

Esta sección es el valor real del port. Lo que sigue son señales **sobre la spec**, no sobre
Rust.

### A. Rust es el primer SDK donde "ausente ≠ vacío" es expresable

La fricción **A** del SDK de Go decía que `omitempty` colapsa cero y ausente, y que
`dlqattempts` solo se salva porque su mínimo legal resulta ser `1` — "el envelope depende de
una coincidencia en vez de una regla". En Rust eso **no pasa**: `Option<u32>` +
`skip_serializing_if = "Option::is_none"` distinguen `dlqattempts: 0` de `dlqattempts`
ausente, y ambos se serializan distinto.

Es una confirmación, no una queja: la sugerencia de Go a la spec —declarar explícitamente
que ningún opcional admite el valor vacío como significativo— sigue siendo la correcta, pero
un lenguaje con `Option` de verdad hace que el SDK sea correcto **por tipo** en vez de por
suerte. Los seis SDKs coinciden hoy; solo dos de ellos lo garantizan.

### B. `Event` no deriva `PartialEq`, por la misma razón que en Go

`data` es JSON arbitrario, así que se guarda como `Box<RawValue>` —el equivalente exacto de
`json.RawMessage`— para preservar orden de claves y fidelidad numérica. `RawValue` no
implementa `PartialEq`, así que `Event` implementa la comparación a mano, campo por campo.
Es deliberado además por otro motivo: añadir un atributo al envelope sin tocar esa función
es un fallo visible en revisión, no una comparación que empieza a mentir en silencio.

**Efecto secundario bueno:** `Box<RawValue>` resuelve gratis §2.5 (fidelidad numérica).
`4995.00` vuelve a salir como `4995.00` y un entero de 21 dígitos no se convierte en
`f64`. Java necesita `USE_BIG_DECIMAL_FOR_FLOATS` para lo mismo, y aun así recorta ceros.

### C. `time` es `String`, igual que en Go y por lo mismo

El tipo natural sería `DateTime<Utc>`, pero cualquier round-trip por un tipo temporal
reformatea, y el replay verbatim exige preservar el evento tal cual llegó. Se guarda como
`String` y se ofrece `Event::event_time()`.

Chrono **sí** permite cumplir §2.2 sin trucos: `to_rfc3339_opts(SecondsFormat::Millis, true)`
da exactamente 3 decimales y sufijo `Z`. Es de los pocos sitios donde el formateador de la
librería estándar del ecosistema hace lo correcto sin pelearse.

### D. `subject` sigue significando dos cosas

El campo se llama `aggregate_id` y lleva `#[serde(rename = "subject")]`, así que el nombre
del struct y el del JSON **no coinciden** justo en el atributo más confundible del
protocolo. Es la mejor solución disponible —igual que en Go— y merece el test de conformidad
dedicado que Go ya pedía.

### E. La coerción de tipos: aquí serde ayuda, pero no basta

`serde` no coacciona (`{"tenantid": 42}` falla) y distingue mayúsculas (`{"ID": …}` no
puebla `id`), así que Rust está en el lado bueno de §2.3 y §2.4 por defecto — a diferencia
de `encoding/json` de Go, que empareja campos **sin** distinguir mayúsculas, y de Jackson,
que coacciona.

Pero **no basta**: si se dejase a serde, `{"tenantid": 42}` daría el código genérico de
desajuste de tipo, el mismo que `{"dlqattempts": "seis"}`, y el operador perdería la
distinción. Los atributos de texto se comprueban explícitamente **antes** de deserializar,
para emitir `WRONG_ATTRIBUTE_TYPE` igual que los otros cinco SDKs. La lección para la spec:
**el código de error es parte del contrato, no un detalle de implementación**, y ningún
lenguaje lo produce correcto por accidente.

### F. `AckWait == BackOff[0]` sigue sin poder expresarse en el sistema de tipos

Sexto lenguaje, misma conclusión. Aquí se defiende con tres cosas: un test unitario
(`ack_wait_es_backoff_cero`), una comprobación en `assert_config_honored` sobre la config
**efectiva del servidor**, y el hecho de que `CANONICAL_BACKOFF` sea `const` (nadie puede
mutarlo en caliente). Sigue siendo una invariante mantenida por convención entre dos
constantes.

### G. ✅ El `retry_after` solo se cumple en el primer reintento — hallazgo **ya en la spec**

**Hallazgo de este port, medido contra nats-server 2.14.5.** Se propuso a la spec y se
incorporó: hoy es [03-delivery.md §2.2](../specification/03-delivery.md), con el
experimento reproducible en
[`conformance/cases/nak-delay-ignored-with-backoff.json`](../conformance/cases/nak-delay-ignored-with-backoff.json).
Se deja documentado aquí porque el ciclo —lo encontró midiendo un port, no leyendo— es lo
que esta sección existe para registrar.

Cuando el consumidor tiene `backoff` configurado —y en flux lo tiene **siempre**—, el
servidor honra el delay de un `-NAK {"delay":…}` **solo en la primera reentrega**. A partir
de la segunda manda el array `backoff` y el delay pedido se ignora, sin error:

```
consumidor SIN backoff, nak(300 ms):  entregas a 0, 300 ms, 600 ms, 900 ms      ← honrado siempre
consumidor CON backoff, nak(300 ms):  entregas a 0, 300 ms, 5300 ms, 15300 ms   ← solo la primera
                                      (backoff configurado: 300 ms, 5 s, 10 s)
```

Consecuencia: un `Retry-After: 5` de un proveedor acorta el primer reintento y **nada más**;
del segundo en adelante manda el backoff canónico (1 m, 5 m, 15 m, 30 m). Es exactamente la
misma trampa familiar que `ack_wait ← backoff[0]` de §2.1 y que el publish de core NATS a un
subject sin stream de 02-naming.md §1.1: **el servidor acepta lo que le pides, no devuelve
error, y aplica otra cosa.** Ninguna de las tres se detecta leyendo código; solo midiendo.

**No hubo divergencia entre SDKs que arreglar** —los seis emiten el mismo `nak(delay)` y
obtienen el mismo comportamiento—; lo que había que arreglar era la documentación, que
afirmaba más de lo que el servidor cumple. La regla quedó así, y este SDK la refleja en el
doc comment de `Classification::retry_after` y en `retry_delay`:

> `Classification::retry_after` es una **sugerencia para el primer reintento**, no un
> control del calendario. Un SDK **NO DEBE** documentarla como si sobrescribiera el backoff
> ni construir lógica que dependa de que se respete más allá de la primera vez.

### H. La inferencia de tipos obliga a anotar el handler

`Handler` está implementado en blanco para todo `Fn(Event, Delivery) -> Future`, y Rust no
infiere los tipos de los parámetros de un closure que se pasa a un genérico con `impl Trait`
de por medio: hay que escribir `|ev: flux::Event, _d| …`. Es ruido en la firma más visible
del SDK.

Las alternativas son peores: una macro esconde el tipo del error, y un trait a implementar a
mano convierte un handler de tres líneas en un `struct` con `impl`. Se documenta y ya.

### I. `Bus` se clona antes del closure, no dentro

```rust
let publicador = bus.clone();          // ← fuera
bus.subscribe(subject, move |ev, d| {
    let bus = publicador.clone();      // ← dentro, una por invocación
    async move { /* … */ }
})
```

`subscribe` toma `&self` y el closure necesita su propia copia, así que el clon tiene que
salir del closure. No es un problema de rendimiento (`Bus` es un `Arc`), pero es el primer
sitio donde tropieza quien viene del SDK de Node o del de Go, y por eso está en todos los
ejemplos.

### J. La firma va **antes** de las `dlq*`, y el orden no lo dice la spec

`07-signing.md` §4 dice que `signkeyid` y `signature` van "entre las extensiones, antes de
`data`". Eso deja sin resolver **dónde exactamente respecto a las `dlq*`**, y hay dos
respuestas defendibles: la lista de atributos permitidos del SDK de Node los declara detrás
de `dlqtime`, pero su `toDlqEvent` los emite delante, porque construye el evento de DLQ como
`{...evento_firmado, dlq*, data}`.

Elegir mal **no rompe la firma** —la verificación quita las `dlq*` en cualquier caso, así que
el payload firmado sale idéntico— pero sí rompe la igualdad byte a byte del mensaje que
acaba en la DLQ, y de esos bytes dependen el replay verbatim, la deduplicación por hash y
los fixtures compartidos. Es exactamente la misma clase de divergencia que el
`{...event, dlq*}` que dio origen a [01-envelope.md §6](../specification/01-envelope.md), y
con la misma forma: silenciosa, invisible para cualquier test que compare datos en vez de
bytes.

Este SDK las declara **antes** de las `dlq*`, que es lo que emiten Node, Python, Go, Java y
.NET. Está fijado con un vector literal (`el_evento_de_dlq_firmado_es_byte_a_byte_el_de_php_y_node`)
que el SDK de PHP lleva idéntico.

**Sugerencia para la spec:** §4 debería nombrar la posición completa —`… tracestate,
signkeyid, signature, dlqreason, …, dlqtime, data`— en vez de "antes de `data`". Un SDK
nuevo no puede deducirla, y el primero que la deduzca al revés no se enterará.

### K. Que la firma sea opcional obliga a probar **dos** configuraciones

`signing` es una feature, así que hay dos crates posibles y los dos tienen que compilar y
pasar. Un `#[cfg(feature = "signing")]` mal puesto no lo detecta `cargo test --all-features`
—que es lo que apetece ejecutar— sino `cargo test` a secas, y al revés. Por eso la tabla de
abajo tiene dos columnas y CI ejecuta las dos.

Es el coste real de que la extensión sea opcional, y aun así compensa: la alternativa es que
todo servicio del ecosistema arrastre `curve25519-dalek` para no usarlo.

---

## Ficheros

| Fichero | Contenido |
|---|---|
| `protocol.rs` | Constantes verificadas y naming (`parse_subject`, `subject_to_type`, `stream_name`, `durable_name`, `dlq_subject`, `source_uri`, `validate_service_name`) |
| `envelope.rs` | `Event`, `build_event`, `serialize`, `parse_event`, `Event::data_as`, `to_dlq_event`, `strip_dlq_extensions` |
| `errors.rs` | `ErrorClass`, `FluxError`, `Failure`, `Classification`, `as_classified`, `describe` |
| `classify.rs` | `Classifier`, `ClassifierOptions`, `UnknownPolicy`, `HttpError` |
| `context.rs` | Propagación por task-local con override explícito |
| `metrics.rs` | `MetricsSink`, `NoMetrics`, `InMemoryMetrics`, `DURATION_BUCKETS` y los enums de etiqueta |
| `signing.rs` | `Signer`, `Verifier`, `SigningOptions`, `VerificationMode`, `generate_key_pair` — feature `signing` |
| `client.rs` | `connect`, `Bus::publish`, `Bus::subscribe`, `Bus::close`, `Bus::replay_from_dlq`, `TenantIsolation` |
| `tests/cross_sdk.rs` | Los fixtures compartidos de `conformance/cases/`, sin broker |
| `tests/integration.rs` | Conformidad contra un NATS real |

## Desarrollo

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features        # 175 tests
```

| Suite | Tests (`--all-features`) | Sin features | Requiere broker |
|---|--:|--:|---|
| Unitarios (en cada módulo) | 150 | 117 | no |
| `tests/cross_sdk.rs` | 3 | 3 | no |
| `tests/integration.rs` | 14 | 11 | **sí** |
| Doctests | 8 | 8 | no |

La diferencia entre las dos columnas son los tests de `signing`, que solo existen con la
feature activa. **Conviene ejecutar las dos formas en CI**: sin ella se comprueba que el
crate compila y pasa cuando nadie usa la firma —que es el caso por defecto—, y con ella que
la firma funciona.

Los unitarios cubren naming, envelope, clasificación, métricas y firma, que es donde vive la
semántica del protocolo. `cross_sdk.rs` ejecuta **los fixtures compartidos de
[`conformance/cases/`](../conformance/cases/)** —los mismos ficheros que verifican los otros
cinco SDKs— e incluye la comprobación del `expectedKeyOrder` exacto del evento de DLQ.

Los de integración requieren un broker y **se saltan solos** si no lo encuentran, para que
`cargo test` siga siendo verde en una máquina sin Docker:

```bash
docker compose up -d                             # en la raíz del repo
cargo test --all-features --test integration     # 14 tests contra NATS real
```

Cubren lo que los unitarios no pueden: que la config canónica de consumidor sobreviva al
servidor (`ack_wait = 30 s`, `max_deliver = 6`, `backoff = [30s, 1m, 5m, 15m, 30m]`
verificados en la respuesta), que el ack explícito funcione, que un PERMANENT no gaste
reintentos, que el presupuesto acotado mande a la DLQ en la 2ª entrega, que un mensaje
ilegible **nunca** llegue al handler, que las extensiones `dlq*` queden **antes** de `data`
en los bytes que acaban en la DLQ, que un evento firmado viaje y verifique de punta a punta,
que en modo `require` uno sin firma acabe en la DLQ con `MISSING_SIGNATURE`, que en modo
`warn` ese mismo evento llegue al handler **y aun así se cuente** como
`invalid_signature` (§7.1), que el modo estricto de tenant falle al suscribirse y que el
evento de otro tenant se ACKee sin llegar al handler.
