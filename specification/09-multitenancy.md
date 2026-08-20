# 09 — Multi-tenant

> **Estado:** normativo. Define qué garantiza flux sobre el aislamiento entre tenants
> y —sobre todo— **qué no**.

---

## 1. El problema, dicho sin adornos

`tenantid` es obligatorio en el envelope ([01-envelope.md §3.1](01-envelope.md)), pero
**el envelope no es un mecanismo de seguridad por sí solo**: es un dato que el
productor rellena.

La pregunta que hay que responder es *contra quién* protege cada capa, no *si*
protege:

| Amenaza | ¿Cubierta? | Por qué |
|---|---|---|
| Un consumidor que olvida filtrar por tenant | ✅ | El SDK filtra, ver §3 |
| Un intermediario que altera el `tenantid` en tránsito | ✅ | TLS + firma ([07-signing.md](07-signing.md)) |
| Un broker comprometido que fabrica eventos | ✅ | Firma |
| Un evento sacado del stream, editado y reinyectado | ✅ | Firma |
| Un servicio que publica en un dominio ajeno | ✅ | ACL del broker ([06-security.md §3](06-security.md)) |
| **Un servicio legítimo comprometido que publica con el `tenantid` de otro** | ❌ | Su clave es válida y su ACL le permite ese dominio |
| **Un consumidor comprometido que lee eventos de todos los tenants** | ❌ | Su suscripción abarca el subject entero |

Las dos últimas filas son el límite real, y **ninguna cantidad de validación de
envelope las cierra**. Cerrarlas exige que el broker sepa de tenants, y eso solo es
posible si el tenant está en el enrutado o en la identidad de la conexión.

## 2. Tres modelos, con su coste real

### Modelo A — Filtrado en consumidor (**default de v1**)

Un stream por dominio, todos los tenants mezclados. El SDK filtra por `tenantid` antes
de invocar el handler.

- ✅ Sin cambios de topología. Un stream por dominio, sin importar cuántos tenants.
- ✅ Los eventos de plataforma (`tenantid: "system"`) fluyen sin fontanería extra.
- ❌ **Todo servicio con acceso al dominio ve los datos de todos los tenants.** El
  aislamiento es una convención del SDK, no una frontera.
- ❌ Un `max_age` compartido: no se puede dar retención distinta a un tenant.

**Cuándo basta:** cuando todos los servicios del dominio son de confianza equivalente
y el requisito es evitar errores, no resistir a un adversario interno.

### Modelo B — Account de NATS por tenant (**aislamiento duro**)

Las *accounts* de NATS son un límite de aislamiento real: los subjects de una account
son invisibles desde otra salvo export/import explícito.

```
Account PROD_ACME     →  EVT_PEDIDOS, DLQ_PEDIDOS, …
Account PROD_GLOBEX   →  EVT_PEDIDOS, DLQ_PEDIDOS, …   (mismos nombres, otro universo)
```

- ✅ **Aislamiento aplicado por el broker.** Un servicio con credenciales de `acme` no
  puede leer datos de `globex` aunque su código lo intente.
- ✅ Retención, replicación y cuotas por tenant.
- ❌ Los streams se multiplican por el número de tenants. Con 500 tenants, 500 copias
  de cada stream — y JetStream tiene límites de recursos por servidor.
- ❌ Un servicio que atiende a N tenants necesita **N conexiones**, una por account.
- ❌ Los eventos de plataforma requieren export/import entre accounts, que es
  fontanería que hay que mantener.

**Cuándo compensa:** pocos tenants, grandes, con requisitos contractuales de
aislamiento. No para SaaS con miles de cuentas pequeñas.

### Modelo C — Tenant en el subject

`<tenant>.<dominio>.<agregado>.v<major>.<evento>`, con ACLs por prefijo.

**Rechazado en v1.** Rompe la regla de 4 tokens de
[02-naming.md §1.1](02-naming.md), de la que dependen todos los wildcards, los nombres
de stream, los de durable consumer y la derivación del `type`. Sería un cambio de
protocolo mayor, y el Modelo B ofrece un aislamiento igual de fuerte sin tocar el
formato.

Se documenta porque es la propuesta que surge sola, y merece un "no, y por esto".

## 3. Obligaciones del SDK — Modelo A

Un SDK **DEBE**:

1. Aceptar un `tenantId` a nivel de conexión y usarlo como valor por defecto al
   publicar.
2. **Filtrar al consumir** cuando se configure aislamiento, **antes** de invocar el
   handler. El evento descartado se `ack`ea: no es un fallo, no es para nosotros.
3. Ofrecer un modo **estricto** en el que consumir sin filtro de tenant sea un error
   de configuración, no un descuido silencioso.

El punto 3 es el que importa. Un filtro que hay que acordarse de poner es un filtro que
alguien olvidará, y el fallo —ver los datos de otro tenant— **no produce ningún
error**: produce un incidente de privacidad que se descubre semanas después.

```ts
const bus = await connect({
  // …
  tenantId: "acme",
  tenantIsolation: "strict",   // toda suscripción filtra; olvidarlo lanza
});
```

## 4. Qué garantiza la firma sobre el `tenantid`

Con [07-signing.md](07-signing.md) activo, el `tenantid` queda **criptográficamente
ligado a la clave del productor**. Eso convierte tres amenazas de "posibles" en
"detectables":

- Alterar el `tenantid` de un evento en tránsito o en reposo invalida la firma.
- Un broker comprometido no puede fabricar eventos con un `tenantid` arbitrario.
- Un evento reinyectado con el tenant cambiado se detecta.

Lo que **no** cambia: un productor legítimo comprometido firma lo que quiera con su
propia clave. La firma responde "¿quién lo escribió?", no "¿tenía derecho a
escribirlo?". Para eso está la ACL, y para el aislamiento duro entre tenants, el
Modelo B.

## 5. Reglas transversales

Independientemente del modelo:

- `tenantid` **DEBE** propagarse sin modificar a lo largo de una cadena de eventos. Un
  evento derivado pertenece **al tenant del evento que lo causó**, no al del servicio
  que lo emite. El SDK lo hereda del contexto automáticamente
  ([01-envelope.md §5](01-envelope.md)).
- `"system"` se reserva para eventos de plataforma sin tenant. **NO DEBE** usarse como
  comodín ni como valor por defecto cuando el tenant real se desconoce: si no se sabe
  de quién es un evento, el bug está aguas arriba.
- **NUNCA** se etiqueta una métrica por `tenantid`
  ([08-observability.md §2.2](08-observability.md)). En trazas sí.
- Un evento **NO DEBE** contener datos de más de un tenant. Si un hecho afecta a
  varios, son varios eventos.

> Esa última regla evita el fallo más difícil de deshacer: un evento con datos
> mezclados no se puede filtrar después. Ya está mal en el stream, y la única
> reparación es purgarlo.

## 6. Migrar de A a B

Si el Modelo A deja de bastar:

1. Crear las accounts por tenant y provisionar sus streams por IaC.
2. Doble publicación temporal: el productor emite en su account actual y en la del
   tenant. La misma técnica que una migración de mayor
   ([05-compatibility.md §3.1](05-compatibility.md)).
3. Migrar consumidores tenant a tenant.
4. Retirar la account compartida cuando no queden consumidores.

**No hay atajo.** Los eventos históricos del stream compartido no se reparten
retroactivamente entre accounts: o se reproducen hacia las nuevas, o se acepta que el
historial anterior al corte vive en el modelo viejo. Decidir esto **antes** de migrar
evita descubrirlo a mitad.
