# flux SDK para Go

Cliente del **flux Event Protocol v1** — CloudEvents 1.0 sobre NATS JetStream.
Nivel de conformidad: **L3** — la validación de esquema es **opt-in**; sin activarla el
comportamiento es exactamente el de L2.

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
API legacy `nc.JetStream()`), `github.com/google/uuid` v1.6+ y
`github.com/santhosh-tekuri/jsonschema/v6` — ver
[El coste de la dependencia](#el-coste-de-la-dependencia).

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
| `ClassPoison` | JSON malformado, falta un atributo CloudEvents o una extensión obligatoria del perfil flux | `term` + DLQ + alerta |

`ParseEvent` etiqueta cada fallo con un `Code` estable —el mismo que dan Node, Python y
Java ante el mismo cuerpo— porque es lo que acaba en la columna de la DLQ y en las
métricas. Además de `MALFORMED_JSON`, `NOT_AN_OBJECT`, `UNSUPPORTED_SPECVERSION`,
`MISSING_REQUIRED_ATTRIBUTE`, `UNSUPPORTED_CONTENT_TYPE` y `UNKNOWN_ROOT_ATTRIBUTE`:

| `Code` | Cuándo |
|---|---|
| `MISSING_REQUIRED_EXTENSION` | Falta —o vale `null` o `""`— `correlationid`, `tenantid`, `producerversion` o `dataclassification`. No se les asume un default: uno tomado como `internal` haría circular PII con 30 días de retención en vez de 7, y un `tenantid` tomado como `system` cruzaría fronteras de tenant ([§3.1](../specification/01-envelope.md)) |
| `INVALID_DATACLASSIFICATION` | `dataclassification` fuera de `{public, internal, confidential, restricted}` |
| `WRONG_ATTRIBUTE_TYPE` | Un atributo de texto llegó como número, booleano u objeto. `encoding/json` ya lo rechazaría al decodificar el struct, pero con el código genérico de desajuste de tipo; se comprueba antes para que el operador distinga un `{"tenantid": 42}` de un `{"dlqattempts": "seis"}` ([§2.4](../specification/01-envelope.md)) |

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

> ⚠️ `RetryAfter` es una **sugerencia para el primer reintento**, no un control del
> calendario. Con `backoff` configurado —y flux lo configura siempre— JetStream honra el
> delay de un `nak` solo en la primera reentrega; a partir de la segunda manda el array
> `backoff` y el delay **se ignora sin ningún aviso**:
>
> ```
> SIN backoff:  0ms → 300ms → 600ms → 900ms      ← el delay se honra siempre
> CON backoff:  0ms → 300ms → 5300ms → 15300ms   ← solo la primera vez
> ```
>
> Medido contra NATS 2.14.5 ([03-delivery.md §2.2](../specification/03-delivery.md)). Un
> `Retry-After: 5` de un proveedor acorta el primer reintento y nada más; los siguientes
> siguen el backoff canónico. No construyas lógica que dependa de otra cosa.

---

## Seguridad: permisos, inbox, TLS y credenciales

Con la capacidad **`security`** el SDK funciona bajo la configuración segura de
[`06-security.md`](../specification/06-security.md): los permisos exactos que genera
[`scripts/gen-acl.mjs`](../scripts/gen-acl.mjs), sin ningún permiso de suscripción sobre subjects de
dominio y con un inbox privado por servicio. Sin esta capacidad, el broker le deniega al SDK la suscripción a su
propio inbox (`_INBOX.<aleatorio>`) y su primera petición falla.

```go
bus, err := flux.Connect(ctx, flux.ConnectOptions{
    Servers: "nats://nats.interno:4222", Service: "facturacion-api", Environment: "produccion", Version: "3.4.1",

    // UN método: .creds (el recomendado), semilla nkey, token, o usuario y contraseña.
    Credentials: flux.CredentialsFromFile("/run/secrets/facturacion-api.creds"),
    // `&flux.TLSOptions{}` EXIGE TLS con las CA del sistema; con CAFile, contra una CA propia. No hay forma de
    // desactivar la verificación.
    TLS: &flux.TLSOptions{CAFile: "/etc/flux/ca.pem"}, // + CertFile y KeyFile para mTLS

    // ⚠️ CONFIGÚRALO. Un `ack` denegado por la ACL no falla en ningún sitio (es una publicación sin respuesta):
    // el evento se procesa y se reentrega cada ack_wait, para siempre. La única salida es este logger.
    Logger: slog.Default(),
})
```

| Qué | Cómo lo cumple el SDK de Go |
|---|---|
| **Inbox privado** | Fija `nats.CustomInboxPrefix("_INBOX.<servicio>")` a partir de `Service`. **No es una opción**, y el nombre de servicio no admite puntos, así que el prefijo de un servicio nunca es prefijo de otro. |
| **TLS** | `TLSOptions{CAFile, CertFile, KeyFile}`: CA propia y mTLS. `ConnectOptions.TLS == nil` no pide TLS; `&TLSOptions{}` lo exige. |
| **Credenciales** | `.creds` por ruta (se **relee en cada reconexión**: rotar no exige reiniciar) o por contenido, semilla nkey de usuario, token y usuario+contraseña. Todo lo mal formado falla en `Connect()` **antes de abrir ningún socket**. |
| **La primera conexión** | Espera a un broker que aún no ha arrancado (con jitter) y **falla de inmediato** ante lo que esperar no arregla. Ver abajo: no usa `RetryOnFailedConnect`. |
| **`subscribe`** | *Info antes de crear*: pregunta por el consumidor (`CONSUMER.INFO`) y solo lo crea si no existe, así que en producción no necesita `CONSUMER.CREATE`. **Falla** con `PERMISSION_DENIED` si se deniega `$JS.API.CONSUMER.MSG.NEXT.…` (`06-security.md §3.7`: un pull denegado deja un consumidor inerte que parece sano). |
| **Denegaciones** | Una petición a la API de JetStream, o un `publish`, a un subject denegado falla en milisegundos con `PERMISSION_DENIED` y el subject **completo**, no por timeout. Un `ack` o un WIP denegados se registran en el `Logger`, una vez por subject. |

### Los cinco códigos

Todos **PERMANENT** (reintentar da lo mismo), con un `code` fijo que la aplicación no puede sustituir. Implementan
`ClassifiedError` y se reconocen con `errors.As`:

| Código | Tipo | Se emite cuando |
|---|---|---|
| `CONNECTION_REJECTED` | `*ConnectionRejectedError` (`Reason`: `auth` o `tls`) | El broker **contestó** y rechazó la primera conexión: credencial equivocada, certificado que no verifica (CA ausente o equivocada, host fuera de los SAN), TLS exigido y no ofrecido, o mTLS sin certificado de cliente |
| `INVALID_CREDENTIALS` | `*InvalidCredentialsError` | Opciones mal formadas, **sin hablar con el broker**: vacías (una variable de entorno sin definir), `User` sin `Password`, más de un método, un `.creds` ilegible o sin JWT y semilla, una semilla que no es de usuario |
| `INVALID_TLS_OPTIONS` | `*InvalidTLSOptionsError` | Una CA que no existe o no es un PEM, un certificado de cliente sin su clave (o al revés) o un par que no casa |
| `NOT_PROVISIONED` | `*NotProvisionedError` | Falta un stream o un consumidor **y** el broker deniega crearlo. Apunta a `flux provision` |
| `PERMISSION_DENIED` | `*PermissionDeniedError` | El broker deniega una operación y no se sabe que el recurso falte (ni siquiera se puede consultar), un `publish` o una suscripción |

La frontera importa y está probada **en los dos sentidos**: una credencial bien formada pero equivocada es `CONNECTION_REJECTED`
(`auth`), nunca `INVALID_CREDENTIALS`; un certificado que el broker no acepta es `CONNECTION_REJECTED` (`tls`), nunca
`INVALID_TLS_OPTIONS`; y un broker que aún no ha arrancado no es ninguno de los tres (se espera).

> **Divergencia con Node, por el cero de Go.** El cero de un `string` es «no lo he puesto» y no se distingue de «lo puse vacío»:
> `Credentials{CredsFile: os.Getenv("NATS_CREDS")}` con la variable sin definir conectaría **sin credenciales**. Para que falle con
> `INVALID_CREDENTIALS`, usa los constructores (`CredentialsFromFile`, `CredentialsFromNKeySeed`, `CredentialsFromToken`,
> `CredentialsFromUserPass`, `CredentialsFromContent`): recuerdan qué método se pidió. Con `TLSOptions` el equivalente es
> inocuo (una CA vacía verifica contra las CA del sistema y, si el broker usa una propia, falla con `CONNECTION_REJECTED`).

### Lo que se midió de `nats.go`

Medido contra `nats-server` 2.14.5 con `nats.go` v1.37.0 (Apéndice A del plan, A-190 a A-199). Es lo que justifica cada línea de
`security.go` y `denials.go`:

- **`RetryOnFailedConnect(true)` no sirve para la primera conexión.** `Connect()` vuelve en 0–2 ms **sin error** y con la
  conexión en `RECONNECTING` ante una credencial rechazada, una CA que no firmó el certificado, un host fuera de los SAN, TLS
  exigido sin TLS en el broker y un mTLS sin certificado de cliente. Solo la credencial rechazada acaba cerrándose, tras dos
  intentos y sin avisar; las demás reintentan **para siempre**, con la causa solo en `nc.LastError()`: un servicio arrancado y mudo.
  Con `RetryOnFailedConnect(false)` los mismos cinco fallos son un error en 1–3 ms con su causa (`ErrAuthorization`,
  `*tls.CertificateVerificationError`, `ErrSecureConnWanted`…) y un broker caído es `ErrNoServers`. Por eso el bucle de la primera
  conexión es del SDK (`connectWhenAvailable`); las reconexiones posteriores siguen siendo de la librería.
- **`nats.go` no entrega las denegaciones a la operación.** Una petición a un subject denegado es un `ErrTimeout`; una suscripción
  denegada devuelve `nil`. El `-ERR 'Permissions Violation…'` llega **solo** al `ErrorHandler` de la conexión. El SDK lo escucha y
  lo **compite con la respuesta** (`guard`): `PERMISSION_DENIED` en milisegundos en vez de un timeout de 5 s.
- **Detectar una suscripción denegada**: tras un `Flush()` (PING/PONG), `nc.LastError()` ya tiene la denegación (0 fallos en
  2 000 intentos), mientras que el `ErrorHandler` —una goroutine aparte— aún no se había ejecutado en 180 de 2 000. Por eso se mira
  `LastError()` y no se «espera un poco».
- **El pull denegado** se detecta con una petición propia sobre el **mismo subject** (`CONSUMER.MSG.NEXT.<stream>.<durable>`)
  con un cuerpo `{` que el servidor contesta `400` sin sacar ningún mensaje: se compite su respuesta con la denegación. No depende
  de ningún temporizador.

## Aislamiento entre tenants

```go
bus, err := flux.Connect(ctx, flux.ConnectOptions{
    // ...
    TenantID:        "acme",
    TenantIsolation: flux.TenantIsolationStrict, // toda suscripción filtra; olvidarlo falla
})
```

- `TenantIsolationOff` (el cero del tipo): se filtra si hay `TenantID`, pero olvidarlo no
  rompe nada.
- `TenantIsolationStrict`: `Subscribe` devuelve `*flux.TenantIsolationError` **antes de
  tocar el broker**, para no dejar a medias un durable en el servidor.

El filtrado ocurre **antes** del handler: un evento de otro tenant se `Ack`ea y se descarta
— no es un fallo, no es para nosotros
([09-multitenancy.md §3](../specification/09-multitenancy.md)).

El modo estricto es el punto que importa. Un filtro que hay que acordarse de poner es un
filtro que alguien olvidará, y el fallo —ver los datos de otro tenant— **no produce ningún
error**: produce un incidente de privacidad que se descubre semanas después.

`"system"` **no cuenta** como filtro: es la ausencia de tenant, no un tenant
([§5](../specification/09-multitenancy.md)). Aceptarlo dejaría fuera todos los eventos de
negocio y daría por satisfecho el modo estricto sin filtrar nada.

Lo que este modelo **no** cubre: un servicio legítimo comprometido puede publicar con el
`tenantid` de otro, y un consumidor comprometido puede leer el subject entero. El filtro
del SDK evita errores, no adversarios; para eso está el Modelo B (una account de NATS por
tenant).

## Firma de eventos — extensión opcional

Traslada la autenticidad **del canal al evento**: un evento firmado sigue siendo
verificable dentro de un fichero, un backup o un correo, donde ya no hay ninguna ACL que lo
respalde ([07-signing.md](../specification/07-signing.md)).

Usa `crypto/ed25519` de la stdlib: **cero dependencias nuevas**.

```go
par, _ := flux.GenerateKeyPair()   // PEM: PKCS#8 la privada, SPKI la pública

bus, err := flux.Connect(ctx, flux.ConnectOptions{
    // ...
    Signing: flux.SigningOptions{
        PrivateKeyPEM: par.PrivateKeyPEM,   // firmar al publicar
        KeyID:         "pedidos-api-3",
        PublicKeys:    map[string]string{"pedidos-api-3": par.PublicKeyPEM},
        Verify:        flux.VerifyRequire,  // VerifyOff (default) | VerifyWarn | VerifyRequire
    },
})
```

- Ed25519, sin negociación de algoritmo. Los formatos con algoritmo negociable acumulan una
  familia de vulnerabilidades —de `alg: none` a la confusión HMAC/RSA— que solo existe
  porque hay algo que negociar.
- Se firma `Serialize(evento sin Signature y sin las extensiones dlq*)`. **No hay
  canonicalización aparte**: es el mismo `Serialize` del productor, y funciona porque
  [01-envelope.md](../specification/01-envelope.md) §1.1, §2.2 y §6 fijan una única
  representación en bytes.
- `SignKeyID` va **dentro** de lo firmado. Si quedara fuera, un atacante lo cambiaría para
  que la verificación buscara otra clave.
- Un evento que pasa por la DLQ **sigue verificando**: las `dlq*` se añaden después de
  firmar y la verificación las ignora. El replay redistribuye un hecho ya emitido.

| Modo | Evento sin firma | Firma inválida |
|---|---|---|
| `VerifyOff` (default) | Se acepta | Se acepta (no se mira) |
| `VerifyWarn` | Se registra y se acepta | Se registra y se acepta |
| `VerifyRequire` | POISON `MISSING_SIGNATURE` | POISON `INVALID_SIGNATURE` / `UNKNOWN_SIGNING_KEY` |

`VerifyWarn` existe porque adoptar la firma en un ecosistema en marcha exige un periodo en
el que unos productores firman y otros no. Pasar directo a `VerifyRequire` convierte en
POISON todo evento de un servicio aún no migrado. Registra en el `Logger` de
`ConnectOptions`; sin logger, acepta en silencio.

> **Conserva las claves públicas retiradas** mientras exista algún evento firmado con ellas
> — mínimo 90 días, la retención de la DLQ. Retirar una clave impide **emitir** con ella, no
> **verificar** lo ya emitido; tratarla como inválida convierte una rotación rutinaria en la
> invalidación retroactiva de todo el historial
> ([§6](../specification/07-signing.md)).

⚠️ El orden de los campos `SignKeyID` y `Signature` en el struct `Event` es **normativo**:
`encoding/json` emite en orden de declaración, las `dlq*` se añaden después de firmar y el
orden de claves es parte del contrato. Moverlos bajo `DLQTime` haría que el mismo evento
firmado produjera bytes distintos en Go y en Node, y una firma de un SDK dejaría de
verificar en el otro. Hay un test que lo vigila.

## Validación de esquema — L3

Sin esto, un productor puede publicar un payload que viola su propio `dataschema` y nadie
se entera hasta que un consumidor —posiblemente de otro equipo, otro lenguaje y otra
semana— se atraganta. El error aparece lejísimos de su causa. Validar en `Publish` lo
convierte en un fallo del servicio que lo provocó
([00-protocol.md §5](../specification/00-protocol.md)).

```go
//go:embed schemas/bundle.json
var bundleJSON []byte

bundle, err := flux.ParseSchemaBundle(bundleJSON)
if err != nil { return err }

bus, err := flux.Connect(ctx, flux.ConnectOptions{
    /* ... */
    Validation: flux.ValidationOptions{
        Mode:      flux.ValidationStrict, // ValidationOff (default) | Warn | Strict
        Bundle:    bundle,
        OnConsume: true,                  // validar también al consumir
    },
})
```

| Modo | `Publish` con payload inválido | Coste |
|---|---|---|
| `ValidationOff` (cero del struct) | Publica. Es L2. | Cero: no se compila ningún esquema |
| `ValidationWarn` | Registra en el `Logger` y publica | Una validación por evento |
| `ValidationStrict` | **Devuelve `*SchemaValidationError`** | Una validación por evento |

- **Reporta TODOS los errores, no solo el primero.** De uno en uno, arreglar un payload
  con tres campos mal cuesta tres despliegues:

  ```
  el payload de "pedidos.pedido.v1.creado" no cumple su esquema (…/creado/1.0.0.json):
    · at '': additional properties 'cantidad' not allowed
    · at '': missing property 'clienteId'
    · at '/moneda': 'euros' does not match pattern '^[A-Z]{3}$'
    · at '/totalCents': got string, want integer
  ```

- Con `OnConsume: true` el evento que incumple su contrato va a la DLQ como **PERMANENT**:
  es sintácticamente correcto, así que reintentarlo daría exactamente el mismo resultado
  ([04-errors.md §1.2](../specification/04-errors.md)). La clase la declaran
  `*SchemaValidationError` y `*SchemaNotFoundError` con `FluxClass()`, así que no depende
  de `UnknownErrorPolicy`. La métrica lo etiqueta `outcome="invalid_schema"`.
- El bundle **resuelve además el `dataschema` exacto** de cada subject (gana sobre
  `SchemaBaseURL`, pierde ante `Schemas`). Sin él, el SDK solo puede asumir el
  `<major>.0.0`; con él usa el MINOR real, que es contra el que se valida.
- Un bundle ausente, un modo mal escrito o un esquema roto hacen fallar `Connect`. Un
  fallo de configuración debe romper el arranque, no la primera publicación — y un typo en
  el modo no puede significar "no valides nada en silencio".

> **El bundle se pasa como dato, no como URL.** Validar está en la ruta caliente: una
> petición de red por evento es inaceptable, y una caché con TTL abre una ventana en la que
> dos servicios validan contra versiones distintas del mismo esquema. `schemas/bundle.json`
> se genera con `node scripts/bundle-schemas.mjs` y **se despliega con el servicio** —con
> `go:embed`, dentro del propio binario—, así que la versión del esquema queda clavada a la
> del servicio, justo lo que `producerversion` promete poder acotar. Un `$ref` que no esté
> en el bundle **no se busca por red**: falla al arrancar diciendo qué regenerar.

### El coste de la dependencia

`github.com/santhosh-tekuri/jsonschema/v6` va en `go.mod`, sin más. En Go **no existe el
extra opcional** de pip ni el `optionalDependencies` de npm, así que —a diferencia de Node
(`ajv`) y Python (`jsonschema`, extra `[validation]`)— este coste lo pagan también los
servicios que se queden en L2:

| Qué | Cuánto |
|---|---|
| Tamaño del binario | ~1 MB más (la biblioteca lleva los metaesquemas embebidos) |
| Dependencias nuevas que auditar | 1 directa. `golang.org/x/text` ya entraba por `nats.go` |
| Coste en ejecución con `Mode: off` | Ninguno: no se compila ni un esquema |

La alternativa —sacar la validación a un módulo aparte (`sdk-go/validation`)— haría
opcional el binario a cambio de partir el SDK en dos y de que `ConnectOptions` no pudiera
mencionar el tipo. No compensa para 1 MB.

> ⚠️ Los esquemas declaran `$schema: draft/2020-12`. Un validador configurado para draft-07
> **no** falla con un error de versión: falla con `no schema with key or ref
> ".../2020-12/schema"`, que no dice nada. El compilador lee el `$schema` de cada esquema y
> tiene `Draft2020` como default; hay un test que comprueba que el bundle lo declara.

---

## Estado replicado: `State()` — capacidad `state`

Un valor por clave, replicado por el broker y leído por quien lo necesita sin preguntar al dueño
([`10-state.md`](../specification/10-state.md)). El dueño escribe con `Put` y `Delete`; los demás servicios lo observan con
`Watch` (la copia local) o lo leen con `Get`.

```go
st := bus.State()

// El DUEÑO. `aggregateVersion` es suyo (su contador monotónico): el SDK ni lo inventa ni lo incrementa.
res, err := st.Put(ctx, "clientes.cliente.v1", "cli-1",
    map[string]any{"clienteId": "cli-1", "nombre": "Ana", "aggregateVersion": 7},
    flux.WithStateTenant("acme"))
// res.Outcome: "ok", o "stale" si la versión no avanza (NO es un error: otro escribió una más nueva).

_, err = st.Delete(ctx, "clientes.cliente.v1", "cli-1", 8, flux.WithStateTenant("acme")) // lápida: lleva su versión

// El OBSERVADOR: la copia local. `WithOnSynced` avisa UNA vez, cuando ya tiene el último valor de cada clave.
sub, err := st.Watch(ctx, "clientes.cliente.v1", func(ctx context.Context, e flux.StateEntry, d flux.StateDelivery) error {
    // e.Deleted: una lápida. Un error devuelto se clasifica como el de un evento (reintento, DLQ).
    return nil
}, flux.WithOnSynced(func() { /* ya se puede servir lecturas */ }))
defer sub.Unsubscribe()
```

| Qué | Cómo lo cumple el SDK de Go |
|---|---|
| **Escritura condicional** | `Put` y `Delete` siguen el algoritmo de §3.2: leer la clave, **reconocer la propia escritura por su `id`** (un acuse perdido no se convierte en `stale`), descartar una versión que no avanza, y publicar por JetStream con `Nats-Expected-Last-Subject-Sequence`. Ante un conflicto de secuencia vuelve a leer, hasta **3 intentos**, y agotados da `STATE_CONFLICT` (RETRYABLE). El conflicto se reconoce por el **código** de la API de JetStream —**10071 y 10164**, no por el texto—. |
| **Identidad** | El tenant va en el subject (`state.<dominio>.<agregado>.v<major>.<tenant>.<clave>`) y **no hay tenant por defecto** ni `system` implícito: sin tenant, `INVALID_TENANT_FOR_STATE`. La clave se valida (`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`): el broker acepta `*`, `a.b` y 2000 caracteres, y una clave sin validar es un subject fantasma. |
| **Clasificación** | Sin `WithStateClassification`, `confidential` (no la de la conexión). `restricted` se rechaza al publicar y es **POISON** al consumir. La lápida es siempre `internal`. |
| **L3** | Con `Validation` activa, `data` se valida contra el esquema que resuelve el índice `state` del bundle —sin red— **antes de leer ni escribir**, y el mensaje cita la **ruta y la regla, nunca el valor** (un estado es donde vive el dato personal). |
| **Un observador** | Un durable `<servicio>__state_<contrato>` con `DeliverLastPerSubject`: un observador nuevo recibe solo el último valor de cada clave, no el historial. Si el durable se **borra** bajo el `Watch` (`flux state resync`), la librería para el consumo en ~1 ms y el SDK lo **reabre**. |
| **Lo que miente** | Una entrada cuyo `tenantid`, `subject` o `type` no coinciden con el subject de NATS es **POISON** (`STATE_SUBJECT_MISMATCH`) y no se aplica. La DLQ de estado guarda `{ key, revision }`: nunca el `data` (90 días de retención) ni la firma. |
| **Permisos** | `Put` y `Delete` fallan con `PERMISSION_DENIED` (no por timeout) si falta publicar en el subject del estado; un stream sin aprovisionar es `NOT_PROVISIONED` con las dos causas posibles (stream o `allow_direct` ausentes). |

Los siete códigos de §6 son `INVALID_TENANT_FOR_STATE`, `INVALID_STATE_CONTRACT`, `INVALID_STATE_KEY`,
`MISSING_AGGREGATE_VERSION`, `STATE_SUBJECT_MISMATCH`, `STATE_CONFLICT` y `RESERVED_DOMAIN`; cada uno es un tipo propio con
su clase y su código **fijos** y se reconoce con `errors.As`. `INVALID_DATACLASSIFICATION` y `INVALID_SCHEMA` (L3) completan
los de la capacidad.

### Lo que se midió de `nats.go` para el estado

`PublishMsg` con `WithExpectLastSequencePerSubject` devuelve un `*jetstream.APIError` con `ErrorCode` 10071 (239 de 239
rechazos con 8 escritores sobre una clave); un `DIRECT.GET` de una clave ausente contesta con la cabecera `Status: 404` y sin
error, y el de un stream que no existe da `ErrNoResponders`; y un durable borrado bajo un `Consume` vivo entrega
`jetstream.ErrConsumerDeleted` y la librería **para ella misma** el consumo en ~1 ms (en Node son 43 s). PLAN.md, A-196 y A-197.

## Petición/respuesta: `Request` y `Respond` — capacidad `rpc`

Una **orden** o un **cálculo** que necesita respuesta ya, a otro servicio ([`11-request-reply.md`](../specification/11-request-reply.md)).
Core NATS, sin stream ni DLQ: si nadie escucha, quien llama lo sabe en milisegundos. **No** es para publicar un hecho (eso es un evento)
ni para leer un dato que otro posee (eso es el estado).

```go
// QUIEN RESPONDE (el dueño del dominio `clientes`): una suscripción al subject EXACTO, con el nombre del servicio como grupo de cola.
r, err := bus.Respond(ctx, "clientes.cliente.v1.calcular-descuento",
    func(ctx context.Context, req flux.RPCRequest) (any, error) {
        entrada, err := flux.UnmarshalRPCData[PeticionDescuento](req)
        if err != nil { return nil, err }
        if entrada.ClienteID == "" {
            return nil, flux.NewPermanentError("falta clienteId", flux.WithCode("CLIENTE_REQUERIDO")) // viaja con su clase y su código
        }
        return map[string]any{"descuentoCents": 500, "moneda": "EUR"}, nil // la respuesta es un OBJETO
    })
defer r.Stop(ctx) // parada ordenada: deja de escuchar, espera a lo que está en vuelo

// QUIEN LLAMA: UN intento, con un timeout (5 s por defecto, de 1 ms a 25 s).
data, err := bus.Request(ctx, "clientes.cliente.v1.calcular-descuento",
    map[string]any{"clienteId": "cli-1", "totalCents": 9990, "moneda": "EUR"},
    flux.WithRPCTimeout(2*time.Second), flux.WithRPCAggregateID("cli-1"))
descuento, err := flux.RequestAs[Descuento](ctx, bus, "clientes.cliente.v1.calcular-descuento", peticion) // tipada
```

| Qué | Cómo lo cumple el SDK de Go |
|---|---|
| **El sobre** | Un CloudEvent normal (`type` `com.flux.rpc.<dominio>.<agregado>.<operacion>.request.v<M>`, `dataschema` `…/rpc/…/request/<M>.0.0.json` o el del bundle), **sin cabeceras de NATS**. La respuesta **hereda** `correlationid`, `tenantid` y `subject` de la petición y su `causationid` es el `id` de la petición. Los cuatro ejemplos de §3.5 salen **byte a byte** (668, 703, 677 y 791 bytes). |
| **Antes de la red** | RESERVED_DOMAIN → INVALID_RPC_CALL (operación, timeout, `data` que no es un objeto, reintentos) → INVALID_DATACLASSIFICATION → L3 → tamaño (RPC_MESSAGE_TOO_LARGE). El primero que falla gana, y **ninguno abre una petición**. |
| **`no responders`** | RPC_UNAVAILABLE en **milisegundos**, RETRYABLE, también con un `Subscribe` de eventos vivo en la misma conexión: el SDK usa **una sola** suscripción de respuestas por conexión (la de `nats.go`) y no abre una segunda a su propio inbox. |
| **Permisos** | Un `publish` a una operación no concedida, o una suscripción denegada a su inbox, es **PERMISSION_DENIED** en milisegundos con el subject completo —`nats.go` no entrega la denegación a la petición, que esperaría su timeout—. `Respond` **falla al arrancar** si falta la suscripción del dueño (`rpc.<dominio>.>`); sin `allow_responses` el handler se ejecuta pero no puede contestar: quien llama ve RPC_TIMEOUT y el dueño lo dice **una vez** en su `Logger`. |
| **Quien llama verifica** | Las cinco comprobaciones de §4.3: ¿es un CloudEvent?, ¿su `type`?, ¿su `causationid`?, ¿`correlationid` y `tenantid`?, ¿el `data` del error cumple su esquema? Si no, **RPC_INVALID_REPLY** (PERMANENT: es un incidente, no un dato). La única excepción es el error de una petición que quien responde no pudo leer. |
| **Errores del handler** | `NewPermanentError` / `NewRetryableError` viajan con su clase, **su código y su mensaje**; un error que el clasificador reconoce (un 503), con un mensaje **genérico**; cualquier otro, `permanent` `UNKNOWN` «error interno del servicio» —su texto puede llevar `data`—, y su detalle va al `Logger`. `retryAfterMs` es una sugerencia para el primer reintento. Un pánico es una respuesta, no un timeout. |
| **Reintentos** | `WithRPCRetries(n)`, solo ante RPC_UNAVAILABLE y RPC_TIMEOUT, **con el mismo `id` y los mismos bytes**: quien responde puede reconocer la orden repetida (RPC_TIMEOUT **no** dice que no se ejecutó). Hay que deduplicar por `req.ID`. |
| **Parada ordenada** | `Responder.Stop` y `Bus.Close` hacen `Drain` **de cada suscripción** —no `nc.Drain()` de la conexión, que pierde las respuestas de lo que está en vuelo—, esperan a los handlers (hasta 25 s) y dejan un margen de 25 ms para la propagación del interés en un cluster. |
| **Concurrencia** | Cada petición corre en su goroutine, con un **semáforo de 256 handlers** por operación (`WithMaxConcurrent`); el exceso espera en la cola de la suscripción. |
| **Firma** | Con `Verify: VerifyRequire` a los dos lados: la ida y la vuelta van firmadas, y quien responde rechaza una petición con `time` fuera de ±300 s (**STALE_REQUEST**). |
| **L3** | La petición se valida **antes de salir** y la respuesta al recibirla (con el índice `rpc` del bundle, sin red); quien responde valida lo que recibe y lo que devuelve. El mensaje cita **ruta y regla, nunca el valor**, y dice de quién es el fallo. |
| **Tenant** | Opción (`WithRPCTenant`) > el del evento en curso > el de `ConnectOptions` > `system` (a diferencia del estado, una petición no es un dato guardado). **Es del handler autorizar el tenant**: quien responde no filtra por el suyo. |

Los códigos de §7 de [`04-errors.md`](../specification/04-errors.md): `RPC_UNAVAILABLE`, `RPC_TIMEOUT`, `RPC_INVALID_REPLY`,
`RPC_MESSAGE_TOO_LARGE`, `STALE_REQUEST`, `INVALID_RPC_CALL` (más `RESERVED_DOMAIN`, `INVALID_SCHEMA`, `PERMISSION_DENIED`). Un error que
manda quien responde es un `*RPCRemoteError`, con **su** clase y **su** código, y se reconoce con `errors.As` como cualquier otro.
Las métricas `flux_rpc_client_requests_total`, `flux_rpc_server_requests_total` y `flux_rpc_duration_seconds` son una extensión opcional
de `MetricsSink` (`RPCMetricsSink`), que `InMemoryMetrics` implementa.

### Lo que se midió de `nats.go` para la petición/respuesta

`nc.Request` a un subject denegado da `ErrTimeout`: la denegación llega por el `ErrorHandler` ~1 ms después y por nada más, así que el SDK
la **compite** con la respuesta. `no responders` llega en 0 ms (la librería lo anuncia en CONNECT sin opción para quitarlo). Con
`sub.Drain()` las 3 respuestas en vuelo llegan; con `nc.Drain()` se pierden las 3. PLAN.md, A-192, A-193, A-194 y A-198.

## Métricas

Siete métricas con nombres y etiquetas fijados por el protocolo
([08-observability.md](../specification/08-observability.md)). No son una decisión del SDK:
si Go y Node nombraran distinto la tasa de DLQ, un panel del ecosistema sería imposible.

```go
metrics := flux.NewInMemoryMetrics()
bus, err := flux.Connect(ctx, flux.ConnectOptions{ /* ... */ Metrics: metrics })

http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
    w.Header().Set("Content-Type", "text/plain; version=0.0.4")
    _, _ = io.WriteString(w, metrics.Render())
})
```

| Métrica | Tipo | Etiquetas |
|---|---|---|
| `flux_events_published_total` | Counter | `subject`, `outcome` |
| `flux_events_consumed_total` | Counter | `subject`, `consumer`, `outcome` |
| `flux_event_handler_duration_seconds` | Histogram | `subject`, `consumer` |
| `flux_events_dlq_total` | Counter | `subject`, `consumer`, `reason`, `code` |
| `flux_events_retried_total` | Counter | `subject`, `consumer`, `attempt` |
| `flux_consumer_pending` | Gauge | `subject`, `consumer` |
| `flux_connection_state` | Gauge | — |

- El default es `NoMetrics{}`: un SDK no debe imponer un backend. `InMemoryMetrics` es una
  comodidad sin dependencias y segura desde varios goroutines; si ya usas
  `prometheus/client_golang`, implementa `MetricsSink` contra él y conserva los nombres.
- **Nunca** se etiqueta por `tenantid`, `id` ni `correlationid`. Por eso `MetricsSink` tiene
  parámetros nombrados y no un `map[string]string`: un mapa genérico es exactamente por
  donde se cuela el `tenantid` que multiplica las series temporales. La cardinalidad no
  avisa — funciona con tres tenants en desarrollo y muere con diez mil en producción.
- El último bucket del histograma es `30` **porque es el `ack_wait`**: un handler que cae
  ahí está a punto de que su mensaje se reentregue mientras aún se ejecuta. Hay un test que
  falla si alguien cambia `DefaultAckWait` y olvida el bucket.

- `flux_connection_state` (`1` conectado, `0` desconectado, `2` reconectando) va enganchado
  a `DisconnectErrHandler` / `ReconnectHandler` / `ClosedHandler`: sin eso valdría `1` hasta
  el `Close()` y no diría nada.

- `flux_consumer_pending` se alimenta de **dos fuentes**, y el SDK usa las dos
  ([§2.3](../specification/08-observability.md)):

  | Fuente | Coste | Falla cuando… |
  |---|---|---|
  | `msg.Metadata().NumPending` de cada entrega | Gratis, fresco en cada evento | **No llegan mensajes** — y ese es el caso que importa |
  | Sondeo de `consumer.Info(ctx).NumPending` | Una petición cada `PendingPollInterval` (15 s por defecto) | Nunca, mientras haya conexión |

  El razonamiento decide la regla: **si el bucle del consumidor muere, dejan de entregarse
  mensajes**, así que una métrica alimentada solo desde los metadatos se queda **plana en
  su último valor** en vez de crecer. Un panel mostraría una línea horizontal,
  indistinguible de "no pasa nada". El sondeo sigue corriendo y reporta el `NumPending`
  creciente: **esa** es la señal, y es la métrica de la cuarta alerta mínima — la que
  delata a un consumidor cuyo bucle murió mientras la conexión sigue reportándose sana.

  Un fallo del sondeo **no afecta al consumo**: se registra y se reintenta en el ciclo
  siguiente. Es telemetría. Y no se sondea con `NoMetrics{}`: sería una petición cada 15 s
  por consumidor para tirar el resultado.

  ⚠️ **`PendingPollInterval: 0` NO desactiva el sondeo: significa "el default"**. Para
  apagarlo hay que escribir `flux.PendingPollDisabled` (un valor negativo). Es una
  divergencia deliberada con Node y Python, donde `0` apaga: en Go el cero de un campo es
  "no lo he puesto", así que un cero que desactivara dejaría la métrica muda en todo
  servicio que no conozca el campo — justo los que nadie vigila. Es la misma ambigüedad
  que documenta [la fricción A](#a-omitempty-colapsa-cero-y-ausente).

  Qué mide, con precisión: los mensajes del stream **aún no entregados** a ese consumidor.
  Un handler lento **no** la hace crecer (sus mensajes ya se entregaron y esperan ack: eso
  se ve en `flux_event_handler_duration_seconds`); un consumidor muerto sí, y sin techo.

---

## Diferencias con el SDK de referencia (Node)

El envelope, el naming, la taxonomía de errores y la config de consumidor son
**idénticos byte a byte**. Estas divergencias son de lenguaje, no de contrato.

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

### 6. La firma va sin dependencias, y el modo `warn` necesita un logger

Node usa `node:crypto`; aquí `crypto/ed25519` de la stdlib. Ninguno de los dos añade una
dependencia — el SDK de Python sí la necesita, porque su stdlib no trae Ed25519.

Donde Node escribe el aviso del modo `warn` con `console.warn`, aquí va al `*slog.Logger`
de `ConnectOptions` (nil = silencio, como el resto del SDK). `NewVerifier` lo recibe como
segundo parámetro para que un verificador construido a mano en un test sea observable sin
tocar variables globales.

Lo mismo vale para el modo `warn` de la **validación L3**: sin `Logger` no verás nada.
Es coherente con el resto del SDK, pero conviene saberlo — `Mode: ValidationWarn` sin
logger equivale a no validar, solo que pagando el coste.

### 7. Valores de etiqueta escapados, no sustituidos

Node sustituye `"`, `\` y los saltos de línea de un valor de etiqueta por `_`. Aquí se
**escapan** según el formato de exposición de Prometheus. El objetivo compartido es el
mismo —un `code` con comillas no puede partir la línea y tumbar el scrape entero— pero
escapar conserva el valor, que es lo que un operador quiere leer cuando el `code` viene de
un error mal formado.

### 8. Los errores de validación declaran su propia clase

En Node, `SchemaValidationError` es un `Error` a secas: al consumir, su clasificación acaba
dependiendo de `unknownErrorPolicy`, que por defecto es "retryable acotado". Aquí los dos
errores de `validation.go` implementan `FluxClass() ErrorClass` y el clasificador los
reconoce con `errors.As`, así que van a la DLQ como **PERMANENT** sin gastar reintentos.

Un evento que incumple su contrato es sintácticamente correcto y reintentarlo dará
exactamente el mismo resultado ([00-protocol.md §5](../specification/00-protocol.md)):
gastar seis entregas y 51 minutos de cola para llegar a la misma conclusión no ayuda a
nadie. El SDK de Python hace lo mismo heredando de `PermanentError`.

### 9. El sondeo de `num_pending` se desactiva con un negativo, no con un cero

Ver [Métricas](#métricas). En Node y Python `0` apaga el sondeo; aquí `0` es "no
configurado" y significa el default. Lo impone el lenguaje: el cero de un campo de struct
no se distingue de "no lo he puesto", y un cero que apagara dejaría la métrica muda
precisamente en los servicios cuyo autor no sabe que el campo existe.

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
`AllowedRootAttributes` antes de decodificar. La spec ya no lo deja al accidente:
[§2.3](../specification/01-envelope.md) afirma que la comparación de nombres de atributo
es **case-sensitive**, y [§2.4](../specification/01-envelope.md) hace lo propio con los
*valores* — los tipos son exactos y `{"tenantid": 42}` es POISON, no el tenant `"42"`.

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
| `signing.go` | `NewSigner`, `NewVerifier`, `GenerateKeyPair`, `SigningOptions` (extensión opcional) |
| `validation.go` | `ValidationOptions`, `SchemaBundle`, `ParseSchemaBundle`, `LoadSchemaBundle`, `SchemaURIFor`, `SchemaValidationError`, `SchemaNotFoundError` (L3, opt-in) |
| `metrics.go` | `MetricsSink`, `NoMetrics`, `InMemoryMetrics`, `DurationBuckets` |
| `tenant.go` | `TenantIsolation`, `TenantIsolationError`, resolución del filtro de tenant |
| `client.go` | `Connect`, `Bus.Publish`, `Bus.Subscribe`, `Bus.Close` |
| `security.go` | `Credentials`, `TLSOptions`, `ConnectionRejectedError`, `InvalidCredentialsError`, `InvalidTLSOptionsError`, `NotProvisionedError`, `PermissionDeniedError`, y la traducción de las opciones a `nats.go` |
| `denials.go` | Las denegaciones de permisos que `nats.go` solo entrega de forma asíncrona, y `guard` (competirlas con la respuesta) |
| `text.go`, `canonical.go` | UTF-8 literal y la forma canónica de `data` (`01-envelope.md §4.1`) |
| `state.go`, `state_pure.go` | `State()`: `Put`, `Delete`, `Get`, `Watch` (`10-state.md`); la parte pura —subject, contrato, clave, `aggregateVersion`, sobre de estado y de DLQ— es lo que ejerce el arnés cruzado |
| `validation.go` | también el mensaje de L3 **sin valores** de un estado |
| `rpc.go`, `rpc_pure.go`, `errors_rpc.go` | `Bus.Request`, `Bus.Respond`, `Responder.Stop` (`11-request-reply.md`); la parte pura —operación, timeout, sobre de la petición, de la respuesta y del error— es lo que ejerce el arnés cruzado |
| `cmd/conformance-harness` | El arnés de la conformidad cruzada (`conformance/harness/README.md`): `build`, `dlq`, `sign`, `verify`, `parse`, `state-*`, `rpc-*` y `external-id` |
| `cmd/rpc-participant` | El participante del SDK de Go en `conformance/interop-rpc.mjs`: un proceso que el orquestador gobierna por stdin y que habla con el broker solo por la API pública |
| `external_id.go` | `WithExternalKey`: el `id` determinista de un hecho importado (`01-envelope.md §2.6.1`) |

## Desarrollo

```bash
go vet ./...
go test -race ./...
gofmt -l .          # sin salida
```

Las **pruebas de integración** (`security_*_test.go`) levantan un `nats-server` **real** con TLS, la ACL que genera
`scripts/gen-acl.mjs` y `flux provision` (`testenv_test.go`): un broker abierto no prueba nada de seguridad. Necesitan
`nats-server` (`NATS_SERVER=…`), `node` y `(cd cli && npm install)`; sin ellos **se saltan con un aviso**, salvo con
`FLUX_REQUIRE_BROKER=1`, que lo convierte en un fallo (CI lo pone: un salto no es un pase). Los puertos salen de
`FLUX_TEST_PORTS=desde-hasta` (por defecto 7300-7399).

```bash
NATS_SERVER=/ruta/a/nats-server FLUX_REQUIRE_BROKER=1 go test -race ./...
node sdk-go/tools/mutantes.mjs      # rompe a propósito cada garantía: las pruebas DEBEN fallar
```

Los demás tests no requieren un broker: cubren naming, envelope, clasificación, firma, métricas,
validación L3 y aislamiento de tenant, que es donde vive la semántica del protocolo. Los de
métricas leen `../protocol.json` —incluido el intervalo por defecto del sondeo de
`num_pending`— y los de validación leen `../schemas/bundle.json`, para que una divergencia
entre el SDK y el contrato falle en CI en vez de dejar un panel vacío o un esquema sin
comprobar.

`-race` no es decorativo: `InMemoryMetrics` se renderiza desde el handler HTTP mientras el
bucle de consumo sigue registrando, y hay un test que ejerce exactamente eso. El sondeo de
`num_pending` corre en su propia goroutine y se prueba igual: `pollPending` recibe sus
dependencias como funciones, así que se le puede hacer fallar sin broker y comprobar que
**el bucle sigue vivo** — un error que lo matara apagaría la métrica para siempre tras un
hipo del broker, y nadie se enteraría.

La conformidad contra un NATS real se verifica con [`conformance/`](../conformance/).
