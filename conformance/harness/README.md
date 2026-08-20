# Arnés de conformidad cruzada

Un programa diminuto por SDK que lee **una** operación por stdin y escribe **un**
resultado por stdout. El runner (`conformance/cross-sdk.mjs`) invoca los siete con los
mismos vectores y **compara las salidas byte a byte**.

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
{ "op": "build" | "dlq" | "sign" | "verify" | "parse", ... }
```

Todos los campos de entrada son **deterministas** (`id`, `time` y `dlqtime` vienen
dados) para que dos SDKs puedan producir exactamente los mismos bytes.

### Salida

```jsonc
{ "ok": true,  "bytes": "<base64 estándar del evento serializado>" }
{ "ok": false, "code": "MISSING_REQUIRED_EXTENSION" }
```

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

## Registro de arneses

`conformance/harnesses.json` dice cómo invocar cada uno. Un SDK sin entrada ahí se
**salta con aviso**, nunca en silencio: saltarse un SDK sin decirlo es exactamente el
fallo que este arnés existe para evitar.

## Añadir el arnés de un SDK

1. Escríbelo en `<sdk>/conformance-harness.<ext>`.
2. Regístralo en `conformance/harnesses.json`.
3. `node conformance/cross-sdk.mjs` — el runner lo incluye solo.

Debe ser **delgado**: parsear la entrada, llamar al SDK, imprimir el resultado. Toda
lógica en el arnés es lógica que no está en el SDK y que el runner no verifica.
