# 06 — Seguridad

Un bus de eventos es, por diseño, el lugar donde converge todo el dato del ecosistema.
Eso lo convierte en el objetivo de mayor valor de la infraestructura. La postura por
defecto de flux es **denegar salvo concesión explícita**.

---

## 1. Aislamiento por NATS accounts

Las *accounts* de NATS son un límite de aislamiento duro: los subjects de una account
son invisibles desde otra salvo export/import explícito.

```
Account SYS          operación del cluster. Ningún servicio de negocio entra aquí.
Account PROD         eventos de producción
Account STAGING      eventos de staging
Account DEV          eventos de desarrollo
```

- Un entorno **NO DEBE** compartir account con otro. Sin esta separación, un bug en
  staging puede publicar en un subject de producción, y ningún ACL a nivel de subject
  lo impide de forma fiable.
- Los servicios **NO DEBEN** tener credenciales de la account `SYS`.

## 2. Identidad de servicio

- Cada servicio **DEBE** tener sus propias credenciales (JWT + NKey). **Ninguna
  credencial compartida entre servicios, jamás.** Una credencial compartida hace
  imposible responder "¿quién publicó esto?" e imposible rotar sin coordinación global.
- Las credenciales **DEBEN** inyectarse desde un gestor de secretos en tiempo de
  ejecución. **NO DEBEN** aparecer en imágenes, repos, variables de entorno de CI ni
  ficheros de config versionados.
- Rotación **DEBERÍA** ser ≤ 90 días, automatizada.
- TLS **DEBE** ser obligatorio en el listener. Sin `allow_non_tls`.

## 3. ACLs a nivel de subject

El principio es simple y se aplica sin excepciones:

> **Un servicio solo publica en el dominio del que es dueño. Solo se suscribe a lo
> que se le ha concedido explícitamente.**

```jsonc
// pedidos-api — dueño del dominio de pedidos
{
  "pub": { "allow": ["pedidos.>"] },
  "sub": {
    "allow": [
      "inventario.stock.v1.>",         // concesión explícita
      "_INBOX.>"                       // respuestas de request/reply
    ]
  }
}

// facturacion-api — consume pedidos, no puede publicarlos
{
  "pub": { "allow": ["facturacion.>", "dlq.facturacion.>"] },
  "sub": { "allow": ["pedidos.pedido.v1.>", "_INBOX.>"] }
}
```

Esto convierte el **ownership del dato en una regla aplicada por el broker**. Un
servicio no puede falsificar un evento de otro dominio aunque su código lo intente:
la publicación es rechazada en el servidor. Es el mismo principio del mayor en el
subject — el enrutado aplica el contrato, la documentación solo lo describe.

### 3.0 Las ACLs se generan, no se escriben

Escritas a mano divergen del diseño en el primer mes: alguien añade un consumidor y
concede un dominio entero "temporalmente". Pero la regla de arriba es **derivable**:
si sabes quién es dueño de cada dominio y qué consume cada servicio, las ACLs se
calculan.

El manifiesto de ownership ([`examples/services.json`](../examples/services.json)) es
la fuente, y [`scripts/gen-acl.mjs`](../scripts/gen-acl.mjs) deriva la configuración.

```bash
node scripts/gen-acl.mjs examples/services.json          # genera
node scripts/gen-acl.mjs examples/services.json --check  # valida (CI)
```

El generador rechaza los manifiestos que rompen el modelo, no solo los mal formados:

| Rechaza | Por qué |
|---|---|
| Dos servicios dueños del mismo dominio | Con dos dueños nadie puede confiar en el `source` de un evento — [00-protocol.md §4](00-protocol.md) |
| Un consumo con comodín en el dominio (`*.>`) | Conceder un dominio entero por comodín es lo contrario de una concesión explícita |
| Consumir un dominio sin dueño declarado | O falta el productor en el manifiesto, o el subject está mal escrito |
| Un servicio suscrito a su propio dominio | Suele indicar que ese flujo debería ser una llamada interna, no un evento |
| Nombre de servicio fuera de `[a-z0-9-]` | Alimenta los nombres de durable consumer — [02-naming.md §4](02-naming.md) |

### 3.1 DLQ

Un consumidor **DEBE** poder publicar en `dlq.<subject que consume>`, y eso significa
concederle `pub` sobre un subject de un dominio ajeno. Es la única excepción al §3, y
es acotada: solo el prefijo `dlq.`, solo los subjects que ya consume.

## 4. Multi-tenant

`tenantid` es obligatorio en el envelope, pero **el envelope no es un mecanismo de
seguridad**: es un dato que el productor rellena y que un productor comprometido puede
falsificar.

En v1, el aislamiento de tenant es **responsabilidad del consumidor**: filtrar por
`tenantid` antes de actuar y nunca cruzar datos entre tenants. Un SDK L2 **DEBERÍA**
ofrecer un filtro declarativo en `subscribe()` para que ese filtrado no dependa de que
cada handler se acuerde.

> Aislamiento real por tenant (account o subject prefix por tenant) queda en **fase 4**.
> Se documenta aquí para que nadie asuma que ya existe.

## 5. Clasificación de datos

`dataclassification` es obligatorio y toma uno de cuatro valores:

| Valor | Significado | Retención | Consecuencias |
|---|---|---|---|
| `public` | Difundible fuera de la organización | 30 d | — |
| `internal` | Datos internos sin PII | 30 d | — |
| `confidential` | PII, datos de negocio sensibles | **7 d** | Logs sin payload; DLQ con acceso restringido |
| `restricted` | Datos financieros, credenciales, salud | **24 h** | Prohibido en logs; DLQ auditada; requiere aprobación de seguridad para el subject |

- Los campos con PII **DEBEN** marcarse `"x-pii": true` en el JSON Schema.
- Un SDK L2 **DEBERÍA** redactar los campos `x-pii` al escribir el evento en logs.
- La retención más corta para `confidential`/`restricted` no es teatro de cumplimiento:
  **un stream de JetStream es una base de datos persistente**. Un evento con PII en un
  stream con `max_age: 30d` es PII almacenada 30 días, sujeta a las mismas obligaciones
  de GDPR que cualquier tabla — incluido el derecho de supresión, que un log
  append-only no puede satisfacer.

### 5.1 Derecho de supresión

Los eventos son inmutables; el RGPD exige poder borrar. La contradicción se resuelve
**no metiendo PII en el evento**:

- El evento lleva **referencias** (`clienteId`), no PII (`email`, `nombre`, `dni`).
- La PII vive en el servicio dueño, donde se puede borrar de verdad.
- Un consumidor que necesite la PII la pide al dueño y aplica su propia retención.

Un subject cuyo `data` contenga PII directa **DEBE** documentarlo y **DEBERÍA**
rediseñarse. La alternativa —cripto-borrado con clave por sujeto— queda fuera de v1.

## 6. Superficie operativa

- El puerto de monitorización (`8222`) **NO DEBE** exponerse fuera de la red de
  gestión.
- `nats-account-server` / el operador JWT **NO DEBEN** ser accesibles desde las redes
  de aplicación.
- Los `nats` CLI con credenciales de operador **DEBEN** vivir en un bastion auditado,
  no en portátiles.

## 7. Fuera de alcance en v1 (declarado, no olvidado)

| Tema | Estado | Nota |
|---|---|---|
| Firma de eventos | ✅ **Implementada** | Ed25519, extensión opcional — ver [07-signing.md](07-signing.md). Traslada la autenticidad del canal al evento. |
| Cifrado a nivel de campo | Fase 4 | Hoy: no metas ahí lo que no puedas guardar en claro. |
| Aislamiento real de tenant | ✅ **Documentado** | Tres modelos con su coste real — ver [09-multitenancy.md](09-multitenancy.md). El default de v1 (filtrado en consumidor) NO resiste a un servicio legítimo comprometido; el Modelo B sí. |
| Auditoría inmutable de acceso | Fase 4 | Hoy: logs del servidor NATS. |

Esta tabla existe para que nadie confunda "no está en el documento" con "está
resuelto".
