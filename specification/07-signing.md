# 07 — Firma de eventos

> **Estado:** extensión **opcional** de v1. Un SDK L1/L2/L3 conforme no necesita
> implementarla; un evento sin firma sigue siendo válido.

---

## 1. Qué problema resuelve

Hoy la autenticidad de un evento la garantiza **la ACL del broker**
([06-security.md §3](06-security.md)): un servicio no puede publicar en un dominio
ajeno porque el servidor lo rechaza.

Eso deja tres huecos:

- **Un evento sacado del stream y reinyectado** es indistinguible de uno legítimo. Un
  `flux dlq replay` mal usado, o cualquiera con permiso de escritura, puede reinyectar
  eventos antiguos.
- **La confianza termina en el broker.** Si el evento se exporta a un data lake, se
  reenvía a un socio o se archiva, ya no hay ACL que lo respalde.
- **Un broker comprometido puede fabricar eventos**, y nada aguas abajo lo detecta.

La firma traslada la autenticidad **del canal al evento**. Un evento firmado sigue
siendo verificable dentro de un fichero, un backup o un correo.

## 2. Por qué solo ahora

Firmar exige que el evento tenga **una única representación en bytes**. Hasta que el
protocolo no fijó las tres reglas siguientes, un mismo evento producía secuencias
distintas según el lenguaje y una firma de Node no verificaba en Go:

| Regla | Documento |
|---|---|
| `data` va siempre el último, resto en orden de declaración | [01-envelope.md §6](01-envelope.md) |
| UTF-8 literal, sin escapes `\u` | [01-envelope.md §1.1](01-envelope.md) |
| `time` con exactamente 3 decimales y sufijo `Z` | [01-envelope.md §2.2](01-envelope.md) |

Las tres se escribieron para resolver otros problemas —replay verbatim, fixtures
compartidos, interoperabilidad— y juntas resultaron ser exactamente la
canonicalización que la firma necesitaba. **No hay una forma canónica aparte para
firmar: es el mismo `serialize()` que usa el productor.**

## 3. Algoritmo

**Ed25519.** Sin negociación de algoritmo, sin parámetros, sin curvas que elegir.

La razón es de superficie de error, no de rendimiento: los formatos de firma con
algoritmo negociable acumulan una familia de vulnerabilidades bien conocida —desde
`alg: none` hasta la confusión entre HMAC y RSA— que solo existe porque hay algo que
negociar. Aquí no lo hay.

- Firma de 64 bytes, clave pública de 32.
- Disponible en la biblioteca estándar o equivalente de todos los lenguajes del
  ecosistema (`node:crypto`, `cryptography`, `crypto/ed25519`, `java.security`,
  `System.Security.Cryptography`, `ring`/`ed25519-dalek`, `sodium`).

## 4. Atributos

| Extensión | Tipo | Descripción |
|---|---|---|
| `signkeyid` | String | Identifica la clave pública. Formato `<servicio>-<n>`, p. ej. `pedidos-api-3`. **Va firmado**. |
| `signature` | String | Firma Ed25519 en **base64url sin padding**. **No va firmada** (no puede firmarse a sí misma). |

Ambas van entre las extensiones, **antes de `data`**, en el orden declarado arriba.

## 5. Qué se firma

```
firma = Ed25519(clave_privada, serialize(evento SIN el atributo `signature`))
```

- Se firma **el envelope completo**, incluidos `data`, `signkeyid` y todas las demás
  extensiones. Firmar solo unos atributos elegidos deja el resto manipulable.
- `signkeyid` **DEBE** ir dentro de lo firmado. Si quedara fuera, un atacante podría
  cambiarlo para que la verificación buscara otra clave.
- Las extensiones `dlq*` se añaden **después** de firmar y **NO** están cubiertas.

### 5.1 Verificar

```
1. Si no hay `signature`, el evento no está firmado. Aplicar la política de §7.
2. Quitar las extensiones `dlq*` (si las hay).
3. Quitar el atributo `signature`.
4. Serializar según 01-envelope.md §6.
5. Resolver `signkeyid` a una clave pública.
6. Verificar Ed25519.
```

Los pasos 2 y 3 son exactamente lo que hace el replay desde la DLQ, así que **un
evento reproducido conserva su firma válida** — que es lo correcto: el replay
redistribuye un hecho ya emitido, no crea uno nuevo.

## 6. Rotación de claves

- `signkeyid` **DEBE** cambiar en cada rotación. Nunca se reutiliza un id con una clave
  distinta: eso convertiría la verificación de eventos históricos en un juego de azar.
- Las claves públicas retiradas **DEBEN** conservarse mientras exista algún evento
  firmado con ellas. Como el stream de eventos tiene `max_age: 30d` y la DLQ `90d`, el
  mínimo es **90 días** más el margen de los backups.
- Un verificador **NO DEBE** rechazar un evento por estar firmado con una clave
  retirada: la firma era válida cuando se emitió, y un evento es un hecho del pasado.
  Retirar una clave impide **emitir** con ella, no **verificar** lo ya emitido.

> Esa última regla es la que más se equivoca. Tratar una clave retirada como inválida
> convierte una rotación rutinaria en la invalidación retroactiva de todo el historial.

## 7. Política de verificación

Un SDK que implemente esta extensión **DEBE** ofrecer tres modos:

| Modo | Evento sin firma | Firma inválida |
|---|---|---|
| `off` (default) | Se acepta | Se acepta (no se mira) |
| `warn` | Se registra y se acepta | Se registra y se acepta |
| `require` | **POISON** | **POISON** |

`warn` existe porque adoptar la firma en un ecosistema en marcha exige un periodo en
el que unos productores firman y otros no. Pasar directo a `require` convierte en
POISON todo evento de un servicio aún no migrado.

Códigos POISON:

| Situación | Código |
|---|---|
| Falta `signature` en modo `require` | `MISSING_SIGNATURE` |
| La firma no verifica | `INVALID_SIGNATURE` |
| `signkeyid` desconocido | `UNKNOWN_SIGNING_KEY` |

## 8. Lo que la firma NO resuelve

Declarado para que nadie asuma de más:

- **No es confidencialidad.** El evento sigue en claro; cualquiera con acceso al stream
  lo lee. El cifrado a nivel de campo sigue fuera de alcance.
- **No impide el replay legítimo.** Una firma válida sigue siéndolo al reinyectar el
  evento. Contra el replay malicioso protege la ACL del broker, no la firma.
- **No autentica al broker.** Solo al productor.
- **No sustituye a las ACLs.** Un servicio sin permiso de publicación no debería poder
  publicar aunque tenga una clave válida. Las dos capas son complementarias:
  la ACL controla **quién puede escribir**, la firma **quién lo escribió**.
