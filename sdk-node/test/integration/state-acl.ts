/**
 * Los permisos EXACTOS del estado replicado, escritos como los escribiría `gen-acl.mjs` (clave
 * `watches` del manifiesto): lo que el SDK de Node necesita de verdad, MEDIDO contra un broker.
 * Cada fila de aquí sale de specification/10-state.md §8 y de protocol.json → `state.acl`.
 *
 * Igual que `acl.ts`, los nombres de stream y de durable se derivan con su PROPIA
 * implementación de 02-naming.md y 10-state.md, no con la del SDK: si el SDK nombrara mal un
 * durable, un helper que reutilizase su código concedería exactamente el permiso equivocado y
 * el test pasaría.
 *
 * El generador de permisos lo escribe otro agente (`fase-2-acl`): esto NO lo usa ni lo sustituye.
 * Un test (`state.test.ts`, «los permisos del test encajan en protocol.json») ancla cada permiso
 * a los patrones de `state.acl` y `security.acl`, de modo que un cambio en la spec que este
 * fichero no siga hace fallar la suite.
 */

const STATE_STREAM = (dom: string) => `STATE_${dom.replace(/-/g, "_").toUpperCase()}`;
const DLQ_STREAM = (dom: string) => `DLQ_${dom.replace(/-/g, "_").toUpperCase()}`;

/** `<servicio>__state_<dom>_<agr>_v<M>` y, con tenant, `__<tenant>` (los `-` del tenant pasan a `_`). */
export const durableDeEstado = (service: string, contract: string, tenant?: string): string =>
  `${service}__state_${contract.replace(/[.-]/g, "_")}${tenant ? `__${tenant.replace(/-/g, "_")}` : ""}`;

/** El durable con el que el DUEÑO lee la DLQ de estado de su dominio (10-state.md §6). */
export const durableDeDlqDeEstado = (service: string, dom: string): string =>
  `${service}__dlq_state_${dom.replace(/-/g, "_")}`;

export interface ObservaEstado {
  /** `clientes.cliente.v1`. */
  contract: string;
  /** Un tenant concreto, o ninguno = todos (`….*.*`). */
  tenant?: string;
  /** Conceder `CONSUMER.CREATE`: solo en desarrollo (`gen-acl.mjs --dev`). */
  crear?: boolean;
}

export interface ServicioEstado {
  service: string;
  /** Dominios de los que es DUEÑO: escribe `state.<dom>.>` y lee lo suyo. */
  owns?: string[];
  /** Dominios cuya DLQ de estado lee, con un pull consumer propio. */
  readsDlq?: string[];
  /** Contratos que observa (`watch`, y `get` de lo que observa). */
  observes?: ObservaEstado[];
}

export interface Permisos {
  publish: string[];
  subscribe: string[];
}

const dominioDe = (contract: string) => contract.split(".")[0];

export function permisosDeEstado(s: ServicioEstado): Permisos {
  // Todo servicio: `connect()` consulta `$JS.API.INFO`; su inbox es PRIVADO.
  const publish = new Set<string>(["$JS.API.INFO"]);

  for (const dom of s.owns ?? []) {
    publish.add(`state.${dom}.>`);
    publish.add(`$JS.API.DIRECT.GET.${STATE_STREAM(dom)}.state.${dom}.>`);
    // A propósito NO `STREAM.INFO.STATE_<DOM>`: el dueño TAMPOCO lo recibe (10-state.md §8). El SDK no lo usa,
    // y concederlo «por si acaso» es lo que A-39 desaconseja.
  }

  for (const dom of s.readsDlq ?? []) {
    const d = durableDeDlqDeEstado(s.service, dom);
    publish.add(`$JS.API.CONSUMER.INFO.${DLQ_STREAM(dom)}.${d}`);
    publish.add(`$JS.API.CONSUMER.MSG.NEXT.${DLQ_STREAM(dom)}.${d}`);
    publish.add(`$JS.ACK.${DLQ_STREAM(dom)}.${d}.>`);
  }

  for (const o of s.observes ?? []) {
    const dom = dominioDe(o.contract);
    const stream = STATE_STREAM(dom);
    const durable = durableDeEstado(s.service, o.contract, o.tenant);
    const tenant = o.tenant ?? "*";
    publish.add(`$JS.API.CONSUMER.INFO.${stream}.${durable}`);
    publish.add(`$JS.API.CONSUMER.MSG.NEXT.${stream}.${durable}`);
    publish.add(`$JS.ACK.${stream}.${durable}.>`);
    // `get`: la forma por SUBJECT, con el tenant FIJADO. La de cuerpo no se concede nunca.
    publish.add(`$JS.API.DIRECT.GET.${stream}.state.${o.contract}.${tenant}.*`);
    publish.add(`dlq.state.${o.contract}`);
    // `watch` comprueba al abrir que la DLQ existe: no cuando muera el primer mensaje.
    publish.add(`$JS.API.STREAM.INFO.${DLQ_STREAM(dom)}`);
    if (o.crear) {
      publish.add(`$JS.API.CONSUMER.CREATE.${stream}.${durable}.state.${o.contract}.${tenant}.*`);
    }
  }

  return { publish: [...publish], subscribe: [`_INBOX.${s.service}.>`] };
}

const lista = (xs: string[]) => `[${xs.map((x) => JSON.stringify(x)).join(", ")}]`;

/** Un usuario con contraseña (en producción es nkey o JWT) con unos permisos dados. */
export function usuarioDeEstado(user: string, perms: Permisos, pass = "pw"): string {
  return (
    `{ user: ${JSON.stringify(user)}, password: ${JSON.stringify(pass)}, permissions: {\n` +
    `    publish: { allow: ${lista(perms.publish)} }\n` +
    `    subscribe: { allow: ${lista(perms.subscribe)} }\n` +
    `} }`
  );
}
