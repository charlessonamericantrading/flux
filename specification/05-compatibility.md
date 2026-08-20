# 05 — Compatibilidad y versionado

**Regla:** SemVer aplicado al contrato, con el **mayor visible en el router** y el
**minor/patch en el esquema**.

```
subject:    pedidos.pedido.v1.creado                             ← MAYOR
dataschema: https://schemas.internal/pedidos/pedido/creado/1.4.0.json  ← MINOR.PATCH
```

---

## 1. Qué significa cada número

| Componente | Dónde vive | Cuándo sube | ¿Afecta a consumidores? |
|---|---|---|---|
| **MAYOR** | Subject NATS | Cambio incompatible | Sí — deben re-suscribirse |
| **MINOR** | `dataschema` | Adición compatible | No |
| **PATCH** | `dataschema` | Doc, descripción, ejemplo | No |

El mayor está en el subject **para que la incompatibilidad sea física, no
contractual**. Un consumidor suscrito a `pedidos.pedido.v1.creado` no puede recibir un
v2 aunque el productor se equivoque, aunque el Schema Registry esté caído, aunque
nadie haya leído esta página. El enrutado es el mecanismo de aplicación.

## 2. Compatibilidad BACKWARD dentro de un mayor

Dentro de `v1`, todo cambio **DEBE** ser BACKWARD-compatible: **un consumidor escrito
contra 1.0.0 debe seguir funcionando con 1.9.0 sin tocar una línea.**

### 2.1 Permitido (sube MINOR)

| Cambio | Condición |
|---|---|
| Añadir un campo **opcional** | Nunca `required` |
| Añadir un valor a un enum | Solo si los consumidores lo tratan como abierto — ver §2.3 |
| Relajar una restricción | `maxLength: 50 → 100`, quitar un `pattern` |
| Marcar un campo como deprecado | `"deprecated": true` + `x-deprecated-since` |
| Añadir documentación | Es PATCH, no MINOR |

### 2.2 Prohibido (obliga a MAYOR nuevo)

| Cambio | Por qué rompe |
|---|---|
| Eliminar un campo | El consumidor lee `undefined` y falla o, peor, calcula mal en silencio |
| Renombrar un campo | Eliminar + añadir. Doblemente roto |
| Cambiar el tipo | `string → number`, `number → string`. Incluye `"9990" → 9990` |
| Opcional → requerido | Los productores viejos no lo envían |
| Endurecer una restricción | `maxLength: 100 → 50` invalida datos ya emitidos |
| Cambiar unidades o semántica | **El más peligroso: el esquema valida igual.** Euros → céntimos pasa todos los tests y descuadra la contabilidad |

> El último merece énfasis. Un cambio de semántica sin cambio de forma es invisible
> para cualquier validador automático. **La única defensa es la revisión humana del
> PR de esquema**, y es la razón por la que los esquemas viven en este repo con
> CODEOWNERS y no se generan desde el código del productor.

### 2.3 Enums: la trampa

Añadir un valor a un enum es BACKWARD-compatible **solo si los consumidores tratan los
valores desconocidos con gracia**. Si un consumidor hace `switch` exhaustivo con
`default: throw`, añadir `"reembolsado"` lo rompe — y el esquema decía que era
compatible.

flux resuelve esto por contrato, no por esperanza:

- Un esquema **DEBE** marcar los enums extensibles con `"x-extensible-enum": true`.
- Un consumidor **DEBE** tener una rama por defecto no destructiva para esos enums:
  registrar y `ack`, nunca lanzar.
- Un enum **cerrado** (sin la marca) **NO DEBE** recibir valores nuevos dentro del
  mismo mayor. Nunca.

## 3. Ciclo de vida de un mayor

```
        ┌──────────┐
   ─────► ACTIVE   │  Único estado que acepta nuevos consumidores.
        └────┬─────┘
             │  se publica v(N+1)
        ┌────▼─────┐
        │DEPRECATED│  Se sigue publicando. Prohibido añadir consumidores.
        └────┬─────┘   Mínimo 90 días.
             │  0 consumidores durante 30 días
        ┌────▼─────┐
        │  SUNSET  │  Se deja de publicar. Stream retenido 90 días más.
        └──────────┘
```

### 3.1 Migración v1 → v2

1. Publicar el esquema `2.0.0` y el subject `…v2.…`.
2. **Doble publicación.** El productor emite en `v1` y `v2` simultáneamente. Un solo
   hecho, dos representaciones. El productor asume la traducción — no los consumidores.
3. Los consumidores migran a su ritmo. Ningún despliegue coordinado.
4. Cuando `nats consumer ls EVT_PEDIDOS` no muestre consumidores de v1 durante 30
   días, parar la publicación de v1.
5. Borrar el subject v1 tras el sunset.

**El paso 2 es innegociable.** Sin doble publicación, la migración exige un despliegue
sincronizado de todos los consumidores, que es exactamente el acoplamiento que este
protocolo existe para eliminar. Si la doble publicación resulta demasiado cara, es
señal de que el cambio no merecía un mayor.

## 4. Ubicación y forma de los esquemas

```
schemas/<dominio>/<agregado>/<evento>/<major>.<minor>.<patch>.json
schemas/pedidos/pedido/creado/1.0.0.json
```

- Un fichero **por versión**. Los esquemas son **inmutables una vez publicados**.
  Editar `1.0.0` in situ invalida todos los eventos ya emitidos que lo referencian.
- El `$id` **DEBE** coincidir exactamente con la URI que aparece en `dataschema`.
- Extensiones flux permitidas en el esquema: `x-pii`, `x-extensible-enum`,
  `x-deprecated-since`.

## 5. Verificación en CI

Un pipeline **DEBE** rechazar el PR si:

1. Un fichero de esquema existente ha sido **modificado** (solo se permite añadir).
2. Un esquema nuevo con el mismo mayor no es BACKWARD-compatible con el anterior.
3. `$id` no coincide con la ruta del fichero.
4. Un evento nuevo no tiene un `CODEOWNER` que sea el equipo dueño del dominio.

Sin (1) y (2) automatizados, el resto de este documento es una sugerencia amable.
