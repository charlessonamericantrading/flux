/**
 * El modelo de permisos que el SDK necesita, escrito como lo escribiría `gen-acl.mjs` v2.
 *
 * La forma sale de docs/plan-v1.1/evidencia/candidata.conf (la ACL «candidata»,
 * verificada a mano contra NATS 2.14.5), ampliada con lo que este SDK usa de verdad y la
 * candidata no tenía — se ve ejecutando, no leyendo, en `acl.test.ts`:
 *
 *   · `$JS.API.STREAM.INFO.DLQ_<DOMINIO>` de cada dominio consumido. `subscribe()` verifica
 *     que la DLQ existe al ARRANCAR: descubrir que no existe cuando por fin llega el primer
 *     evento envenenado es el peor momento.
 *
 * Y se QUITA una cosa que la candidata sí tenía y el SDK ya no necesita cuando el consumidor
 * está aprovisionado (`flux provision`): `CONSUMER.CREATE`. Se mantiene como opción porque
 * sin aprovisionar el SDK lo crea (desarrollo), y es lo que el plan concede.
 *
 * Los nombres de stream y de durable se derivan aquí con su PROPIA implementación de
 * 02-naming.md, no con la del SDK: si el SDK nombrara mal un durable, un helper que
 * reutilizase su código concedería exactamente el permiso equivocado y el test pasaría.
 */

const streamName = (dominio: string) => `EVT_${dominio.replace(/-/g, "_").toUpperCase()}`;
const durableName = (service: string, subject: string) =>
  `${service}__${subject.replace(/[.-]/g, "_")}`;

export interface ServicioAcl {
  service: string;
  /** Dominios de los que es dueño: publica en `<dominio>.>`. */
  owns?: string[];
  /** Subjects exactos que consume. */
  consumes?: string[];
  /** Conceder `CONSUMER.CREATE` (el plan lo concede; con consumidores aprovisionados sobra). */
  crearConsumidor?: boolean;
}

export interface Permisos {
  publish: string[];
  subscribe: string[];
}

const dominioDe = (subject: string) => subject.split(".")[0];
const dlqStream = (dominio: string) => `DLQ_${dominio.replace(/-/g, "_").toUpperCase()}`;

export function permisosExactos(s: ServicioAcl): Permisos {
  const publish = new Set<string>(["$JS.API.INFO"]);
  for (const d of s.owns ?? []) {
    publish.add(`${d}.>`);
    publish.add(`$JS.API.STREAM.INFO.${streamName(d)}`);
  }
  for (const subject of s.consumes ?? []) {
    const d = dominioDe(subject);
    const evt = streamName(d);
    const durable = durableName(s.service, subject);
    publish.add(`dlq.${subject}`);
    publish.add(`$JS.API.STREAM.INFO.${evt}`);
    publish.add(`$JS.API.STREAM.INFO.${dlqStream(d)}`);
    publish.add(`$JS.API.CONSUMER.INFO.${evt}.${durable}`);
    publish.add(`$JS.API.CONSUMER.MSG.NEXT.${evt}.${durable}`);
    publish.add(`$JS.ACK.${evt}.${durable}.>`);
    if (s.crearConsumidor !== false) {
      publish.add(`$JS.API.CONSUMER.CREATE.${evt}.${durable}.${subject}`);
    }
  }
  // Inbox PRIVADO. Ningún permiso de suscripción sobre subjects de dominio: el SDK
  // consume por pull, y lo que confina es `$JS.API`.
  return { publish: [...publish], subscribe: [`_INBOX.${s.service}.>`] };
}

const lista = (xs: string[]) => `[${xs.map((x) => JSON.stringify(x)).join(", ")}]`;

/** Un usuario por contraseña (en producción es nkey o JWT) con unos permisos dados. */
export function usuarioConf(user: string, perms: Permisos, pass = "pw"): string {
  return (
    `{ user: ${JSON.stringify(user)}, password: ${JSON.stringify(pass)}, permissions: {\n` +
    `    publish: { allow: ${lista(perms.publish)} }\n` +
    `    subscribe: { allow: ${lista(perms.subscribe)} }\n` +
    `} }`
  );
}

/**
 * La ACL ANTIGUA, congelada: lo que `node scripts/gen-acl.mjs examples/services.json`
 * generaba para `facturacion-api` antes de la v1.1 (solo cambia `nkey` por usuario y
 * contraseña, para poder conectar sin generar claves).
 *
 * Es un fixture y NO se regenera: el generador v2 ya no la produce, y justo por eso hay
 * que conservarla — es la regresión que este test guarda.
 */
export const ACL_ANTIGUA_FACTURACION_API: Permisos = {
  publish: ["facturacion.>", "dlq.pedidos.pedido.v1.>"],
  subscribe: ["pedidos.pedido.v1.>", "dlq.facturacion.>", "_INBOX.>"],
};
