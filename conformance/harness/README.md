# Arnés de conformidad cruzada

Un programa diminuto por SDK que lee **una** operación por stdin y escribe **un**
resultado por stdout. El runner (`conformance/cross-sdk.mjs`) invoca los siete con los
mismos vectores y **compara las salidas byte a byte**. Las operaciones de una
**capacidad** opcional (`state`) las ejecuta solo quien la declara: [Capacidades](#capacidades).

## Por qué existe

`conformance/cases/*.json` describía la interoperabilidad entre SDKs, pero nada la
ejecutaba: eran hallazgos documentados, no verificación. La interoperabilidad de los
siete SDKs dependía de que alguien la comprobara a mano, y eso no sobrevive al
siguiente cambio.

Un fixture que nadie ejecuta **aparenta cobertura**, que es el modo de fallo dominante
de este proyecto.

## Contrato

- **Una** invocación = **una** operación. Sin estado entre llamadas.
- Entrada: un objeto JSON por **stdin**.
- Salida: un objeto JSON por **stdout**, y nada más. Los diagnósticos van a stderr.
- **Exit 0 siempre**, incluso al fallar la operación: el error se reporta en el JSON.
  Un exit distinto de 0 significa que el arnés está roto, no que el caso falló.
- Sin red y sin broker. El arnés ejercita envelope, firma y parseo — nada de NATS.

### Entrada

```jsonc
{ "op": "build" | "dlq" | "sign" | "verify" | "parse"
      | "state-build" | "state-tombstone" | "state-subject" | "state-dlq", ... }
```

Todos los campos de entrada son **deterministas** (`id`, `time` y `dlqtime` vienen
dados) para que dos SDKs puedan producir exactamente los mismos bytes.

### Salida

```jsonc
{ "ok": true,  "bytes": "<base64 estándar del evento serializado>" }
{ "ok": true,  "subject": "state.clientes.cliente.v1.acme.cli-1" }   // solo `state-subject`
{ "ok": false, "code": "MISSING_REQUIRED_EXTENSION" }
```

`code` es el código **estable** del protocolo ([04-errors.md](../../specification/04-errors.md)).
Un rechazo que la spec no ha dotado de código (hoy, un contrato de estado mal formado) no es
comparable entre SDKs: cada uno lo llama a su manera (Node por el nombre de su excepción, Rust
`ERROR`). **Ningún vector depende de uno**.

`bytes` va en **base64** y no como texto para que ningún paso intermedio pueda
reescribir el UTF-8 o los saltos de línea — que es justo lo que se está comprobando.

## Operaciones

### `build`

```jsonc
{
  "op": "build",
  "event": {
    "subject": "pedidos.pedido.v1.creado",
    "id": "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
    "source": "/produccion/pedidos-api",
    "time": "2025-08-20T10:25:39.410Z",
    "dataschema": "https://schemas.internal/pedidos/pedido/creado/1.0.0.json",
    "correlationid": "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
    "tenantid": "acme",
    "producerversion": "3.4.1",
    "dataclassification": "internal",
    "aggregateId": "ped-123",        // opcional
    "causationid": "…",              // opcional
    "traceparent": "…",              // opcional
    "data": { "pedidoId": "ped-123" }
  }
}
```
→ `{ "ok": true, "bytes": "…" }`

### `dlq`

`build`, más:
```jsonc
{ "op": "dlq", "event": { … }, "dlq": {
    "reason": "permanent", "attempts": 1,
    "consumer": "facturacion-api__pedidos_pedido_v1_creado",
    "error": "PEDIDO_YA_CANCELADO", "dlqtime": "2025-08-20T10:25:40.117Z" } }
```

`dlqtime` viene dado: si lo generase el SDK, los bytes no serían comparables.

### `sign`

`build`, más `{ "signing": { "privateKeyPem": "…", "keyId": "pedidos-api-1" } }`.

La clave privada es la del vector **TEST 1 de RFC 8032**, en PEM PKCS#8. Es pública y
está en `vectors.json`: **no es un secreto y no debe usarse jamás fuera de los tests.**

### `verify`

```jsonc
{ "op": "verify", "bytes": "<base64>",
  "publicKeys": { "pedidos-api-1": "-----BEGIN PUBLIC KEY-----…" },
  "mode": "require" }
```
→ `{ "ok": true }` o `{ "ok": false, "code": "INVALID_SIGNATURE" }`

### `parse`

```jsonc
{ "op": "parse", "bytes": "<base64>" }
```
→ `{ "ok": true }` o `{ "ok": false, "code": "WRONG_ATTRIBUTE_TYPE" }`

Comprueba la tabla de códigos POISON de
[01-envelope.md §3.1](../../specification/01-envelope.md): **todos los SDKs deben
devolver el mismo código ante la misma entrada**, o agrupar la DLQ por causa deja de
funcionar en cuanto el ecosistema es polyglot.

### Operaciones de estado (capacidad `state`)

Cuatro operaciones para [10-state.md](../../specification/10-state.md), con la entrada **plana**: es
la misma que el `input` de los fixtures de
[`cross-sdk-state-envelope.json`](../cases/cross-sdk-state-envelope.json), más el `op`. Todo es
determinista: `id`, `time` y `dlqtime` vienen dados. **`dataschema` no viene**: lo deriva el SDK del
contrato (`stateDataschema` en Node, `state_dataschema` en Rust), porque es parte de lo que se compara.
`time` DEBE llegar ya en su forma canónica (3 decimales y `Z`): el arnés no la normaliza.

#### `state-build`

```jsonc
{ "op": "state-build",
  "contract": "clientes.cliente.v1", "tenantId": "acme", "key": "cli-1",
  "id": "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55", "source": "/produccion/crm-api",
  "time": "2026-08-20T10:25:39.412Z", "correlationid": "01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
  "producerversion": "2.1.0", "classification": "confidential",
  "causationid": "…", "traceparent": "…", "tracestate": "…",   // opcionales
  "data": { "clienteId": "cli-1", "aggregateVersion": 7 } }
```
→ `{ "ok": true, "bytes": "…" }`: la entrada de un `put` (§2.5, §2.6). `classification` es obligatoria
en el arnés (el valor por defecto de `put` es del SDK, no de los bytes).

#### `state-tombstone`

Lo mismo sin `classification` ni `data`, y con `aggregateVersion` (la versión **de la lápida**):
→ `{ "ok": true, "bytes": "…" }` (§4).

#### `state-subject`

```jsonc
{ "op": "state-subject", "contract": "clientes.cliente.v1", "tenantId": "acme", "key": "cli-1" }
```
→ `{ "ok": true, "subject": "state.clientes.cliente.v1.acme.cli-1" }` o `{ "ok": false, "code":
"INVALID_STATE_KEY" }` (también `INVALID_TENANT_FOR_STATE` y `RESERVED_DOMAIN`, en el orden de §3.6).

#### `state-dlq`

```jsonc
{ "op": "state-dlq",
  "original": { …la entrada de `state-build`… },
  "revision": 41,
  "dlq": { "reason": "permanent", "attempts": 1,
           "consumer": "tienda-web__state_clientes_cliente_v1__acme",
           "error": "PAIS_NO_SOPORTADO: …", "dlqtime": "2026-08-20T10:25:40.117Z" } }
```
→ `{ "ok": true, "bytes": "…" }`: el sobre original con las `dlq*` y `data` sustituido por
`{ "key", "revision" }` (§6). Si `original` no es una entrada legal, falla con el código de
`state-build`.

`state-build` y `state-tombstone` también devuelven `{ "ok": false, "code": … }` con
`MISSING_AGGREGATE_VERSION`, `INVALID_DATACLASSIFICATION`, los tres anteriores…, en el orden de
[10-state.md §3.6](../../specification/10-state.md): el primero que falla gana.

## Capacidades

Una **capacidad** es una extensión opcional que un SDK declara en `protocol.json` →
`capabilities.sdks.<sdk>.capabilities` ([00-protocol.md §5](../../specification/00-protocol.md)). Las
operaciones de estado solo tienen sentido para quien la declara, y el runner lo aplica en los dos
sentidos:

| El SDK… | Qué hace el runner |
|---|---|
| **declara** `state` y su arnés implementa la operación | la ejecuta y compara |
| **declara** `state` y su arnés **no** la implementa (responde `UNSUPPORTED_OP`) | **FALLO**: declarar una capacidad sin ejecutarla es lo que este runner existe para impedir |
| **no declara** `state` | no la ejecuta, la marca `–` en la tabla e imprime **`N/A: no declara state`** en el resumen. No es un fallo; tampoco es un salto silencioso |

Hoy declaran `state` Node y Rust; Python, Go, Java, .NET y PHP salen `N/A`.

## Vectores

`vectors.json` tiene un bloque por clase de operación (`vectors`: bytes; `poisonVectors` y
`verifyVectors`: códigos) y, para la capacidad, **`stateVectors`**. Cada vector de estado lleva
`input` (la entrada **sin** el `op`) y dice qué espera:

| Campo | Qué comprueba |
|---|---|
| `sha256` | los bytes son los **de la spec** (§2.6), no «lo que dice el SDK de referencia». Lo llevan los tres ejemplos de la spec |
| `expectSubject` | el subject de NATS |
| `expect` | el **código estable** del rechazo |
| *(ninguno)* | los bytes los fija el SDK de **referencia** (Node) y los demás deben coincidir |
| `differsFrom: <id>` | **control**: los bytes DEBEN ser distintos de los de ese otro vector en el mismo SDK |
| `caseFixture` | enlaza con un fixture de `cross-sdk-state-envelope.json` |

Además de comparar, el runner comprueba lo que la **spec** dice de los bytes de referencia
(`data` es el último atributo, `subject` y `partitionkey` son la clave, `tenantid` es el tenant,
UTF-8 literal salvo los escapes de control, la DLQ de estado sin la firma y con solo `{ key,
revision }`…): que todos coincidan con Node no prueba que Node acierte.

`vectors.json` repite los fixtures y los 23 `subjectVectors` del caso `cross-sdk-state-envelope.json`
(el caso los documenta; el runner los ejecuta). Dos copias se desincronizan en silencio, así que el
runner **aborta** (exit 2) si dejan de coincidir.

**El orden de validación está en los vectores**: cada uno de `state-put-orden-…` rompe dos cosas a la
vez, porque dos SDKs que validan en otro orden dan el mismo código ante cada defecto suelto y
distinto ante dos.

> **Al editar `vectors.json` a mano** ojo con los escapes: las herramientas que sustituyen texto
> convierten un escape de unicode (la barra invertida, `u` y cuatro dígitos hexadecimales) en el
> carácter, y un carácter de control en crudo no es JSON válido. El vector
> `state-put-escapes-obligatorios` necesita esos escapes como **texto** (los de control, U+007F, U+00A0
> y U+2028 van escritos con escape para que ningún editor los pierda). Se escribió con un script, y
> se comprueba con `grep -c 'u0001' vectors.json`.

## Mutaciones

Un runner que solo se ha visto en verde no demuestra que detecte nada. `--mutant <nombre>` estropea a
propósito lo que un SDK entrega o declara —sin tocar su código— y el runner **DEBE** reaccionar:

```bash
node conformance/cross-sdk.mjs --only node,rust --mutant rust-un-byte       # DEBE fallar
```

| Mutante | Qué simula | Resultado |
|---|---|---|
| `rust-un-byte` | (a) el arnés de Rust devuelve los bytes de estado con **un byte** cambiado (`1.0` → `1.1`, el byte 18) | **falla**: todos los vectores de bytes de estado, con el byte que difiere y su contexto |
| `rust-tenant-cambiado` | (b) Rust intercambia los tenants `acme` y `globex` | **falla**: bytes y subjects distintos (`"tenantid":"acme"` ≠ `"globex"`) |
| `rust-ignora-tenant` | (b) Rust escribe siempre `acme`, vaya el tenant que vaya | **falla**, y el **control** `state-put-control-tenant` lo dice por sí solo: «los bytes son IGUALES a los de `state-put-minimo`» |
| `rust-sin-state-subject` | (c) Rust declara `state` y su arnés no implementa `state-subject` | **falla**: `declara state … pero su arnés no implementa state-subject (UNSUPPORTED_OP)` |
| `python-declara-state` | (c) Python pasa a declarar `state` sin ninguna de las cuatro operaciones | **falla**: todos sus vectores de estado dan `UNSUPPORTED_OP` en el arnés **real** |
| `rust-sin-arnes-registrado` | (c) Rust declara `state` y su arnés desaparece de `harnesses.json` | **falla**: sin arnés no hay evidencia |
| `rust-no-declara-state` | (d) Rust deja de declarar `state` | **pasa** (exit 0) e imprime `N/A: no declara state — rust` |

CI los ejecuta todos (`.github/workflows/spec.yml`, job `cross-sdk`): los que **deben fallar** y el que
**debe pasar con N/A**. Un SDK que declara `state` y ni siquiera tiene arnés registrado (o figura en
`pending`) falla igual aunque se ejecute con `--only`: excluir un SDK de una ejecución no es quitarle el
arnés.

## Divergencias conocidas, sin vector

Medidas explorando los arneses de Node y de Rust con entradas fuera de los vectores. **No son vectores**
porque la spec no dice cuál de los dos acierta (o porque la entrada no es una que un SDK reciba de
verdad); un vector así decidiría la spec desde un fichero de pruebas, y dejaría CI en rojo.

| Entrada | Node | Rust | Qué falta |
|---|---|---|---|
| `dlqerror` > 1024 con no-ASCII (`state-dlq`, y `dlq` igual): 1500 × `é` | corta a 1024 **unidades UTF-16** (2693 bytes en total) | corta a 1024 **bytes** (1669 bytes); el primer byte que difiere es el 1593 | la spec no fija **cómo** se trunca `dlqerror`: Node cuenta unidades UTF-16, Python puntos de código, Go runas y Rust bytes |
| `contract` con un major > 2^32 (`clientes.cliente.v4294967296`) | lo acepta | lo rechaza (sin código) | la spec dice «entero ≥ 1» sin tope; el `u32` de Rust no es de la spec |
| `contract` con un major > 2^53 (`…v99999999999999999999`) | lo acepta y escribe `type` `…v100000000000000000000` (pasa por `Number`) | lo rechaza | ídem; en Node, además, el `type` ya no coincide con el contrato |
| `id` o `source` vacíos en `state-build` | construye los bytes (un evento que su propio `parseEvent` rechazaría como `MISSING_REQUIRED_ATTRIBUTE`) | los rechaza (sin código) | los constructores puros de Node no comprueban que `id` y `source` no estén vacíos (los rellena el SDK en `put`, que no se ha medido aquí) |
| `contract` mal formado (`clientes.cliente`, `v0`, mayúsculas…) | rechaza con `InvalidStateContractError` (sin código) | rechaza (sin código: `ERROR`) | [04-errors.md §6](../../specification/04-errors.md) no tiene código para esto; es un hueco de la spec, no de un SDK |
| `time` no canónico (`…39.4123Z`, `…39Z`, con `+02:00`) | lo copia tal cual | lo normaliza a UTC con 3 decimales | el arnés **exige** `time` canónico: Rust recibe un instante y Node, una cadena |

## Registro de arneses

`conformance/harnesses.json` dice cómo invocar cada uno. Un SDK sin entrada ahí se
**salta con aviso**, nunca en silencio: saltarse un SDK sin decirlo es exactamente el
fallo que este arnés existe para evitar. Qué capacidades declara cada uno **no** está ahí: la
fuente es `protocol.json`.

## Añadir el arnés de un SDK

1. Escríbelo en `<sdk>/conformance-harness.<ext>`.
2. Regístralo en `conformance/harnesses.json`.
3. `node conformance/cross-sdk.mjs` — el runner lo incluye solo.
4. Si el SDK declara `state`: implementa también las cuatro operaciones de estado. El runner no
   las pide hasta que `protocol.json` lo declare; **desde entonces, su ausencia es un fallo**.

Debe ser **delgado**: parsear la entrada, llamar al SDK, imprimir el resultado. Toda
lógica en el arnés es lógica que no está en el SDK y que el runner no verifica.
