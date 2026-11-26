/**
 * Errores de configuración del cliente.
 *
 * Viven aparte de `client.ts` para que `state.ts` los use sin un ciclo de importación
 * (`client.ts` importa `state.ts`). `client.ts` los re-exporta: la API pública no cambia.
 */

export class ConsumerConfigMismatchError extends Error {
  constructor(
    readonly durable: string,
    readonly differences: Array<{ field: string; requested: unknown; effective: unknown }>,
  ) {
    super(
      `el servidor devolvió una configuración distinta de la solicitada para "${durable}":\n` +
        differences
          .map((d) => `  ${d.field}: solicitado ${JSON.stringify(d.requested)}, efectivo ${JSON.stringify(d.effective)}`)
          .join("\n") +
        `\nJetStream sobrescribe algunos campos en silencio (03-delivery.md §2.1).`,
    );
    this.name = "ConsumerConfigMismatchError";
  }
}

export class TenantIsolationError extends Error {
  constructor(subject: string, motivo: string) {
    super(
      `tenantIsolation="strict" pero ${motivo} al suscribirse a "${subject}". ` +
        `Sin filtro de tenant, este consumidor vería los eventos de TODOS los tenants ` +
        `y eso no produce ningún error visible (09-multitenancy.md §3).`,
    );
    this.name = "TenantIsolationError";
  }
}
