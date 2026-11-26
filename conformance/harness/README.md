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

**`ajustesVectors`** (93) fijan lo decidido en «Ajustes». Son de **cualquier** operación de estado o de petición/respuesta (`state-*`,
`rpc-*`: la operación manda, no la capacidad) y los ejecuta **`conformance/ajustes.mjs`**, un runner aparte de `cross-sdk.mjs`, para los SDKs de
`ajustesSdks` (hoy Node y Rust). Dicen qué esperan como los de estado (`sha256`, `expectSubject`, `expect`, `differsFrom`) y los que coinciden con
la spec llevan **su** SHA-256. Un vector con **`dataTexto`** quiere que `data` llegue como **texto crudo** (con espacios, escapes
o una clave repetida: lo que un objeto JSON no puede expresar): el runner lo inyecta sin reserializar.

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

## Ajustes: lo que Node y Rust hacían distinto fuera de los vectores, ya decidido

Explorando los arneses de Node y de Rust con **361 entradas** fuera de los vectores (`explorar-divergencias.mjs`,
abajo) aparecieron **157** salidas distintas. Cada una se clasificó: **(a)** un SDK incumple la spec → se arregla; **(b)** la
spec calla o es ambigua → se decide, se escribe en la spec y `protocol.json`, y se arreglan los dos; **(c)** es una diferencia
de lenguaje que ningún SDK puede evitar → se documenta como norma («Lo que no se admite»). Lo decidido tiene su vector en
`ajustesVectors` (93 vectores), que ejecuta `node conformance/ajustes.mjs` para los SDKs de `ajustesSdks` —hoy Node y Rust— y que **pasan idénticos**:

| Entrada | Node antes | Rust antes | Decisión | Ahora | Spec |
|---|---|---|---|---|---|
| `dlqerror` de 1500 × `é`, o con un emoji en el corte | 1024 **unidades UTF-16** (2693 bytes en total) | 1024 **bytes**; en el envío de un POISON, 1024 **caracteres** | **1024 bytes UTF-8 sin partir un carácter** | 512 `é`; el emoji que no cabe se descarta entero | 04-errors.md §3 |
| `message` de un error de rpc: 1000 emojis | lo **rechaza** (cuenta 2000 unidades UTF-16) | lo acepta | `maxLength` cuenta **caracteres Unicode** (puntos de código) | lo aceptan los dos; 1001, ninguno | 11-request-reply.md §3.3 |
| Versión mayor `v99999999999999999999`, `v4294967296`, `v10000` | la **acepta**, y escribe el `type` `v100000000000000000000` | la rechaza (o la conserva, según el sitio) | **1 a 4 dígitos** (`v1`…`v9999`) | los dos la rechazan; `v9999` la aceptan | 02-naming.md §1 |
| `id` o `source` vacíos (y `correlationid`, `producerversion`, `tenantId`) | construye el sobre | los rechaza **sin código** | se rechazan, con el código del parser (`MISSING_REQUIRED_ATTRIBUTE` / `_EXTENSION`; `INVALID_RPC_CALL` en rpc) | los dos | 01-envelope.md §3.1 |
| `time` no canónico (`…39Z`, `…+02:00`, `…412345Z`) | lo **copia** tal cual | lo normaliza | se **normaliza** a 3 decimales y `Z` (UTC, truncando) o se rechaza | los dos normalizan | 01-envelope.md §2.2 |
| `time` que no es un instante (`ayer`, mes 13, sin zona, segundo 60, año fuera de 0000–9999) | lo **copia** | `HARNESS_INPUT` (o `+10000-01-01…`) | `WRONG_ATTRIBUTE_TYPE` (`INVALID_RPC_CALL` en rpc) | los dos | 01-envelope.md §2.2 |
| `data` con espacios, `\u00f1`, `\/`, `\u007f` o una clave repetida | lo **compacta** y escribe UTF-8 literal (`JSON.parse` + `stringify`) | lo guarda **tal cual** (texto crudo) | la **forma canónica** es la de `JSON.stringify` con UTF-8 literal | Rust la reescribe; los bytes son los de la spec | 01-envelope.md §4.1 |
| Un error que se construye mal (`class: poison`, `code` en minúsculas, `message` vacío o de 1001, `retryAfterMs` inválido) | `TypeError` **sin código** | `INVALID_RPC_CALL` | código estable del conjunto cerrado de 04-errors.md §7 | `INVALID_RPC_CALL` en los dos | 04-errors.md §7 |
| `retryAfterMs: null` | `INVALID_RPC_CALL` | lo confundía con la clave **ausente** | `null` no es «ausente» | `INVALID_RPC_CALL` en los dos | — |
| `rpc-request`/`rpc-reply` con una clasificación inválida **y** un `data` que no es un objeto | `INVALID_RPC_CALL` | `INVALID_DATACLASSIFICATION` (el arnés convertía la clasificación antes de llamar al SDK) | el orden de §4.1 paso 0: operación → `data` objeto → clasificación | `INVALID_RPC_CALL` en los dos | 11-request-reply.md §4.1 |
| `rpc-error` con una operación mal formada o con el prefijo `rpc.` | `INVALID_RPC_CALL` / `RESERVED_DOMAIN` | construye el error sin mirarla | se valida como en `rpc-request` | los dos | 11-request-reply.md §2.2 |
| `rpc-reply` / `rpc-error` con `tracestate` | no lo escribe | lo **escribe** | §3.2 no lo prevé: la respuesta y el error llevan solo `traceparent` | los dos lo omiten | 11-request-reply.md §3.2 |
| `rpc-request` con la versión mayor `v99999999999999999999` | `type` `…request.v100000000000000000000` | `type` `…request.v99999999999999999999` | tope de 4 dígitos | `INVALID_RPC_CALL` en los dos | 02-naming.md §1 |

**Sin vector**, porque la operación del arnés cruzado es general (`build`, `dlq`, `parse`) y los otros cinco SDKs no están
alineados todavía: lo comprueban los tests unitarios de Node (`sdk-node/test/ajustes.test.ts`) y de Rust (`envelope.rs`, `rpc.rs`).

| Entrada | Node antes | Rust antes | Decisión | Ahora |
|---|---|---|---|---|
| `build` con un dominio reservado (`rpc.pedido.v1.creado`) | `RESERVED_DOMAIN` | lo **construye** | un constructor puro tampoco escribe en `rpc.`, `state.`, `dlq.` ni `system.` | los dos `RESERVED_DOMAIN` |
| `build` con una clasificación fuera del enum | la **acepta** | `INVALID_DATACLASSIFICATION` | la rechaza | los dos |
| `parse` con un BOM, o un byte que no es UTF-8 | lo **acepta** (`TextDecoder` come el BOM y sustituye lo inválido) | `MALFORMED_JSON` | no es JSON (RFC 8259 §8.1): `MALFORMED_JSON` | los dos |
| `parse` con `"subject": 5`, `"causationid": 7`, `"dlqattempts": "1"`… | lo **acepta** | `INVALID_ATTRIBUTE_TYPE` | los tipos son exactos también en lo no obligatorio | los dos `INVALID_ATTRIBUTE_TYPE` |
| Error del handler: `PoisonError` sin código | `POISON` | `PERMANENT` | `PERMANENT` (`poison` no viaja; 11-request-reply.md §5.3) | los dos `PERMANENT` |
| Error del handler con un mensaje solo de espacios | lo envía | lo sustituye por «error del servicio» | se sustituye por «el servicio rechazó la petición» | los dos |
| `id` de una petición ilegible que se cita como `causationid` | hasta 256 caracteres, con controles | hasta 128, sin controles | **128 caracteres, sin controles** (§5.2) | los dos |

### Qué SDKs llevan lo decidido

`ajustesSdks` es hoy **Node y Rust** (y `flux call` para §4.3.4 y la versión mayor). De los otros cinco SDKs —Python, Go,
Java, .NET y PHP— solo llevan el **tope de 1 a 4 dígitos de la versión mayor, y solo en su patrón de subject**
(`SUBJECT_PATTERN`/`SubjectPattern`), con un caso `v99999` rechazado y `v9999` aceptado en su test de unidad: sus tests
comparan o recorren `protocol.json`, así que el patrón no podía quedarse atrás. **Todo lo demás está pendiente en esos
cinco SDKs (Fase 6)**: `dlqerror` en bytes, `data` canónico, `time` normalizado, atributos vacíos, UTF-8 estricto, tipos de
lo opcional y el código de los errores de construcción. Al alinearlos, se añaden a `ajustesSdks`.

### Lo que no se admite

Diferencias de **lenguaje**, no de SDK: ningún vector las iguala, porque ningún SDK puede evitarlas. Están en la spec
([01-envelope.md §4.1](../../specification/01-envelope.md)) y `explorar-divergencias.mjs` las comprueba en los dos sentidos (marca `NO[…]`):

| Entrada | Node | Rust | Qué hacer |
|---|---|---|---|
| Un **entero fuera de ±(2^53−1)** en `data` (`9007199254740993`) | lo redondea al `double` más cercano | lo conserva | Va como **cadena** |
| Un **flotante o un exponente** (`4995.00`, `1E+2`, `1e21`, `-0`, `0.0`) | lo reescribe (`4995`, `100`, `1e+21`, `0`) | no toca los números (§2.5) | El dinero va en **enteros** (`totalCents`) |
| Una **clave que es un índice de array** (`"1"`) | JavaScript la ordena primero | respeta el orden dado | Claves con letras (`camelCase`) |
| Un **sustituto UTF-16 suelto** (`"\ud83d"`) en un texto | lo admite y lo escribe escapado | lo rechaza (no es UTF-8) | No existe en texto UTF-8 |
| **Más de 128 niveles** de anidamiento | lo admite | lo rechaza | `data` plano |
| Un número **fuera del rango de un `double`** (`1e400`, `rpc-timeout`) | `Infinity` (y `JSON.stringify` lo escribe `null`) | el JSON del arnés lo rechaza (`HARNESS_INPUT`) | No lo uses |
| Un valor de **otro tipo JSON** del que pide la operación (`time: 5`, `operation: 5`, un error sin `class`, con una clave de más, o `code` que no es una cadena) | el SDK lo rechaza (`INVALID_RPC_CALL`) | el arnés lo rechaza al leer la entrada (`HARNESS_INPUT`): tipos cerrados | — (es del **arnés**, no de la API pública, que es tipada) |
| `aggregateVersion` **`7.0`** o `1e0` | indistinguible de `7`: lo acepta | lo rechaza (`MISSING_AGGREGATE_VERSION`) | Ya documentado: [10-state.md §3.4](../../specification/10-state.md) |
| Un **subject de evento** mal formado, o un `data` que no es un objeto, al construir un evento | `InvalidSubjectError` / `EnvelopeError` | `ERROR` | **Hueco de la spec**: ningún código estable (04-errors.md); cada SDK lo nombra a su manera |

**Pendiente, sin cerrar:** una **clave repetida en la raíz de un mensaje recibido** (`{"id":"a","id":"b",…}`). Node (`JSON.parse`) se
queda con la última y no lo nota; Rust la rechaza (`INVALID_ATTRIBUTE_TYPE`, por accidente del deserializador). La spec no dice
nada. Propuesta: POISON `MALFORMED_JSON` en todos (I-JSON, RFC 7493, exige nombres únicos), que en Node pide un escáner del
texto. Va marcada `NO[RAIZ_REPETIDA]` en `explorar-divergencias.mjs`.

### Explorar divergencias

```bash
FLUX_HARNESS_RUST=sdk-rust/target/debug/conformance-harness node conformance/harness/explorar-divergencias.mjs
```

Alimenta a los dos arneses con las mismas ~360 entradas y dice cuáles difieren. Sale con 1 si hay una divergencia **sin** marca
(una regresión) o una marca que ya no hace falta. No está en CI (necesita los dos SDKs compilados): se ejecuta al tocar `data`,
`time`, los límites de longitud o los constructores del sobre.

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
