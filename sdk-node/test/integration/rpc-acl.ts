/**
 * Los permisos EXACTOS de la petición/respuesta, escritos como los escribiría `gen-acl.mjs` (claves `calls` y `owns`
 * del manifiesto): lo que el SDK de Node necesita de verdad, MEDIDO contra un broker. Cada fila de aquí sale de
 * specification/11-request-reply.md §7 y de protocol.json → `rpc.acl`.
 *
 *   · **Quien llama** publica en el subject EXACTO de cada operación concedida (`rpc.clientes.cliente.v1.obtener`) y se
 *     suscribe a su inbox privado `_INBOX.<servicio>.>`. Nunca `rpc.>`, `rpc.<dominio>.>` ni un comodín por token.
 *   · **El dueño** se suscribe a `rpc.<dominio>.>` —aunque el SDK se suscribe al subject exacto de cada operación: lo que se
 *     CONCEDE no es lo que se USA (A-64)— y lleva `allow_responses`; **ningún** permiso de publicación sobre inboxes (A-16).
 *     Se recomienda `{ max: 1, expires: 30s }` (A-68): una respuesta legítima nunca llega después del timeout máximo (25 s).
 *   · Todo servicio publica `$JS.API.INFO`: `connect()` lo consulta (06-security.md §3).
 *
 * El generador de permisos lo escribe otro agente (`fase-3-acl`): esto NO lo usa ni lo sustituye. Un test
 * (`rpc.test.ts`, «los permisos del test encajan en protocol.json») ancla cada permiso a los patrones de `rpc.acl`, de
 * modo que un cambio en la spec que este fichero no siga hace fallar la suite. Y los nombres se derivan aquí con su PROPIA
 * implementación, no con la del SDK: si el SDK nombrara mal un subject, un helper que reutilizase su código concedería
 * exactamente el permiso equivocado y el test pasaría.
 */

export type AllowResponses = true | { max?: number; expires?: string };

export interface ServicioRpc {
  service: string;
  /** Dominios de los que es DUEÑO: se suscribe a `rpc.<dominio>.>` y puede contestar. */
  owns?: string[];
  /** Operaciones que LLAMA (nombre sin `rpc.`): publica en el subject exacto. */
  calls?: string[];
  /**
   * `allow_responses` del dueño. Por defecto `{ max: 1, expires: "30s" }`, lo que se recomienda; `"ausente"` lo omite (el dueño
   * no puede contestar: A-68), y `true` es el de por defecto de NATS (admite UNA respuesta y no caduca).
   */
  allowResponses?: AllowResponses | "ausente";
}

export interface PermisosRpc {
  publish: string[];
  subscribe: string[];
  allowResponses?: AllowResponses;
}

export const ALLOW_RESPONSES_RECOMENDADO = { max: 1, expires: "30s" } as const;

export function permisosRpc(s: ServicioRpc): PermisosRpc {
  const publish = new Set<string>(["$JS.API.INFO"]);
  for (const op of s.calls ?? []) publish.add(`rpc.${op}`);
  const subscribe = [`_INBOX.${s.service}.>`];
  for (const dom of s.owns ?? []) subscribe.push(`rpc.${dom}.>`);
  const ar = s.allowResponses ?? ALLOW_RESPONSES_RECOMENDADO;
  return {
    publish: [...publish],
    subscribe,
    ...((s.owns?.length ?? 0) > 0 && ar !== "ausente" && { allowResponses: ar }),
  };
}

const lista = (xs: string[]) => `[${xs.map((x) => JSON.stringify(x)).join(", ")}]`;

/** Un usuario con contraseña (en producción es nkey o JWT) con unos permisos dados. */
export function usuarioRpc(user: string, p: PermisosRpc, pass = "pw"): string {
  const ar =
    p.allowResponses === undefined
      ? ""
      : p.allowResponses === true
        ? `\n    allow_responses: true`
        : `\n    allow_responses: { max: ${p.allowResponses.max ?? 1}${p.allowResponses.expires ? `, expires: "${p.allowResponses.expires}"` : ""} }`;
  return (
    `{ user: ${JSON.stringify(user)}, password: ${JSON.stringify(pass)}, permissions: {\n` +
    `    publish: { allow: ${lista(p.publish)} }\n` +
    `    subscribe: { allow: ${lista(p.subscribe)} }${ar}\n` +
    `} }`
  );
}
