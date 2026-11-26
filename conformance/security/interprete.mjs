/**
 * El intérprete de `conformance/cases/security-acl.json`.
 *
 * La regla de esta suite es la contraria a la de `conformance/run.mjs`: allí las comprobaciones
 * están escritas en código y el JSON solo las documenta; aquí **el JSON decide qué se
 * ejecuta**. Este fichero no sabe qué es S1 ni S6: sabe ejecutar las operaciones del
 * vocabulario (`vocabulary` del JSON) y comparar lo observado con lo esperado. Añadir un caso
 * es escribir JSON; solo añadir una OPERACIÓN toca este fichero.
 *
 * Habla NATS directamente (las mismas librerías que `run.mjs`), no a través de ningún SDK de
 * flux: un runner construido sobre un SDK heredaría las suposiciones que debe verificar.
 */

import { randomUUID } from "node:crypto";

import net from "node:net";

import { EscenarioSdk, validarEscenario } from "./sdk-escenario.mjs";
import {
  connect,
  dec,
  enc,
  conectar,
  configConsumidorCanonica,
  jetstream,
  nkeyAuthenticator,
  nkeys,
  nuevaClave,
  pedir,
  publicarCrudo,
  recibirUno,
  resumen,
  sleep,
  vigilar,
} from "../../scripts/lib/nats-harness.mjs";

// ─── Vocabulario: validación del JSON ────────────────────────────────────────

/** Campos que cualquier paso admite, además de los argumentos de su operación. */
const CAMPOS_COMUNES = ["op", "as", "id", "title", "note", "control", "onlyWith", "knownDeviation", "expect", "save"];

/** Campos de un actor. Los de variante derivan los permisos de los que genera `gen-acl.mjs`. */
const CAMPOS_ACTOR = ["service", "inbox", "instance", "without", "withoutSubscribe", "extraPublish", "extraSubscribe", "replaceSubscribe", "allowResponses", "dev"];
const CAMPOS_VARIANTE = ["without", "withoutSubscribe", "extraPublish", "extraSubscribe", "replaceSubscribe", "allowResponses", "dev"];

/**
 * Valida un caso contra el vocabulario del propio JSON. Una errata en una clave NO puede
 * degradarse a «no se comprueba nada»: `exepct` en vez de `expect` dejaría un paso sin
 * expectativa y pasaría siempre. Lanza con la ruta del fallo.
 */
export function validarCaso(caso, vocabulario) {
  const donde = (i) => `${caso.id}, paso ${i + 1}`;
  for (const campo of ["id", "title", "spec", "rationale", "observed"]) {
    if (caso[campo] === undefined) throw new Error(`${caso.id ?? "(caso sin id)"}: falta \`${campo}\``);
  }
  for (const campo of ["server", "date", "result"]) {
    if (!caso.observed?.[campo]) throw new Error(`${caso.id}: \`observed.${campo}\` es obligatorio (versión y fecha medidas)`);
  }
  if (caso.bugs?.length && !caso.counterExample) {
    throw new Error(`${caso.id}: nace de un bug (${caso.bugs.join(", ")}) y le falta \`counterExample\``);
  }
  if (!Array.isArray(caso.steps) || caso.steps.length === 0) throw new Error(`${caso.id}: sin pasos`);

  // `save` guarda el campo `seq` observado; `{ "ref": nombre }` (con `plus` opcional) lo usa DESPUÉS.
  // Una referencia a algo que no se ha guardado antes es un error al cargar, no un `undefined`.
  const guardados = new Set();
  caso.steps.forEach((paso, i) => {
    for (const nombre of referenciasDe({ ...paso, save: undefined })) {
      if (!guardados.has(nombre)) throw new Error(`${caso.id}, paso ${i + 1}: la referencia «${nombre}» no se guardó antes con \`save\``);
    }
    if (paso.save !== undefined) guardados.add(paso.save);
  });

  caso.steps.forEach((paso, i) => {
    const def = vocabulario.ops[paso.op];
    if (!def) throw new Error(`${donde(i)}: operación desconocida «${paso.op}». Las del vocabulario: ${Object.keys(vocabulario.ops).join(", ")}`);
    const admitidos = new Set([...CAMPOS_COMUNES, ...(def.args ?? []), ...(def.optional ?? [])]);
    for (const k of Object.keys(paso)) {
      if (!admitidos.has(k)) throw new Error(`${donde(i)} (${paso.op}): campo desconocido «${k}». Admite: ${[...admitidos].join(", ")}`);
    }
    for (const k of def.args ?? []) {
      if (paso[k] === undefined) throw new Error(`${donde(i)} (${paso.op}): falta el argumento obligatorio «${k}»`);
    }
    if (paso.op === "sdk_script") validarEscenario(paso, donde(i));
    if (def.needsActor && paso.as === undefined) {
      throw new Error(`${donde(i)} (${paso.op}): falta \`as\` (quién lo hace)`);
    }
    for (const ruta of Object.keys(paso.expect ?? {})) {
      const campo = ruta.split(".")[0];
      if (!(def.observes ?? []).includes("*") && !(def.observes ?? []).includes(campo)) {
        throw new Error(`${donde(i)} (${paso.op}): «${ruta}» no es algo que esta operación observe. Observa: ${(def.observes ?? []).join(", ")}`);
      }
    }
    for (const a of actoresDe(paso)) {
      if (typeof a === "string") continue;
      for (const k of Object.keys(a)) {
        if (!CAMPOS_ACTOR.includes(k)) throw new Error(`${donde(i)}: campo de actor desconocido «${k}». Admite: ${CAMPOS_ACTOR.join(", ")}`);
      }
    }
  });
}

/** Los nombres que un valor referencia con `{ "ref": nombre }`, en todo su árbol. */
function referenciasDe(valor) {
  if (Array.isArray(valor)) return valor.flatMap(referenciasDe);
  if (valor && typeof valor === "object") {
    if (typeof valor.ref === "string" && Object.keys(valor).every((k) => k === "ref" || k === "plus")) return [valor.ref];
    return Object.values(valor).flatMap(referenciasDe);
  }
  return [];
}

/** Sustituye cada `{ "ref": nombre, "plus": n }` por el número guardado (+ n). */
function resolverRefs(valor, guardados) {
  if (Array.isArray(valor)) return valor.map((v) => resolverRefs(v, guardados));
  if (valor && typeof valor === "object") {
    if (typeof valor.ref === "string" && Object.keys(valor).every((k) => k === "ref" || k === "plus")) {
      if (!guardados.has(valor.ref)) throw new Error(`la referencia «${valor.ref}» no tiene valor guardado: el paso que la guarda falló o no observó \`seq\``);
      return guardados.get(valor.ref) + (valor.plus ?? 0);
    }
    return Object.fromEntries(Object.entries(valor).map(([k, v]) => [k, resolverRefs(v, guardados)]));
  }
  return valor;
}

/** Todos los actores que un paso menciona (`as`, `spy`, `victim`, `caller` y los `owners` de un ciclo). */
export function actoresDe(paso) {
  return [...["as", "spy", "victim", "caller"].map((k) => paso[k]), ...(paso.owners ?? []), ...(paso.agents ?? []).map((a) => a.as)].filter((a) => a !== undefined);
}

// ─── Actores ─────────────────────────────────────────────────────────────────

/** `"facturacion-api"` o `{ service, … }` → objeto canónico. */
export function normalizarActor(as) {
  const o = typeof as === "string" ? { service: as } : { ...as };
  if (!o.service) throw new Error(`actor sin \`service\`: ${JSON.stringify(as)}`);
  return o;
}

export const esVariante = (a) => CAMPOS_VARIANTE.some((k) => a[k] !== undefined);

/** La clave estable de una variante: dos pasos con la misma definición comparten identidad. */
export function claveVariante(a) {
  const o = {};
  for (const k of ["service", ...CAMPOS_VARIANTE]) if (a[k] !== undefined) o[k] = a[k];
  return JSON.stringify(o);
}

/**
 * Los permisos de una variante: los que genera `gen-acl.mjs` para el servicio, menos
 * `without` (publicación) y `withoutSubscribe` (suscripción), más `extraPublish`/`extraSubscribe`, o
 * `replaceSubscribe` en lugar de la suscripción; y `allowResponses` cambia el `allow_responses` del dueño
 * (`null` lo quita, `true` pone la forma desnuda, `{ max, expires }` otra distinta). `dev` parte de la salida de
 * `gen-acl.mjs --dev`.
 *
 * Un permiso que se quita y no existe LANZA: una variante que no cambia nada pasaría las mismas
 * pruebas que el servicio sin tocar, y el caso diría haber probado una denegación que nunca
 * ocurrió. Lo mismo con quitar un `allow_responses` que el servicio no tiene.
 *
 * Una lista de suscripción que se quedara VACÍA por quitar permisos no deniega nada (`allow: []` es «sin restricción»,
 * A-25): se escribe con un subject que nadie usa, `_INBOX.variante-sin-concesion.>`.
 */
export function permisosDeVariante(a, { prod, dev }) {
  const base = (a.dev ? dev : prod).find((s) => s.name === a.service);
  if (!base) throw new Error(`variante de un servicio que el manifiesto no declara: ${a.service}`);
  for (const p of a.without ?? []) {
    if (!base.publish.includes(p)) {
      throw new Error(`la variante quita «${p}», que no está entre los permisos que genera gen-acl.mjs para ${a.service}:\n  ${base.publish.join("\n  ")}`);
    }
  }
  for (const p of a.withoutSubscribe ?? []) {
    if (!base.subscribe.includes(p)) {
      throw new Error(`la variante quita la suscripción «${p}», que no está entre las que genera gen-acl.mjs para ${a.service}:\n  ${base.subscribe.join("\n  ")}`);
    }
  }
  const publish = base.publish.filter((p) => !(a.without ?? []).includes(p)).concat(a.extraPublish ?? []);
  let subscribe = a.replaceSubscribe ?? base.subscribe.filter((p) => !(a.withoutSubscribe ?? []).includes(p)).concat(a.extraSubscribe ?? []);
  // (`replaceSubscribe: []` SÍ escribe la lista vacía: es lo que mide S14, que `allow: []` no deniega nada.)
  if (a.replaceSubscribe === undefined && subscribe.length === 0) subscribe = ["_INBOX.variante-sin-concesion.>"];
  let allowResponses = base.allowResponses ?? null;
  if (a.allowResponses !== undefined) {
    if (a.allowResponses === null && !base.allowResponses) {
      throw new Error(`la variante quita el allow_responses de ${a.service}, que gen-acl.mjs no le concede: no cambiaría nada`);
    }
    allowResponses = a.allowResponses;
  }
  return { publish, subscribe, allowResponses };
}

// ─── Evaluar lo observado contra lo esperado ─────────────────────────────────

const obtener = (obj, ruta) => ruta.split(".").reduce((o, k) => (o == null ? undefined : o[k]), obj);
const mostrar = (v) => (typeof v === "string" ? JSON.stringify(v.length > 160 ? `${v.slice(0, 160)}…` : v) : JSON.stringify(v));

function coincide(esperado, real) {
  if (esperado !== null && typeof esperado === "object" && !Array.isArray(esperado)) {
    if ("match" in esperado) return typeof real === "string" && new RegExp(esperado.match).test(real);
    if ("includes" in esperado) return typeof real === "string" && real.includes(esperado.includes);
    if ("lte" in esperado) return typeof real === "number" && real <= esperado.lte;
    if ("gte" in esperado) return typeof real === "number" && real >= esperado.gte;
    if ("not" in esperado) return !coincide(esperado.not, real);
    if ("present" in esperado) return (real !== undefined && real !== null) === esperado.present;
    // Sobre una LISTA de cadenas (el log de un SDK, las violaciones del servidor): alguna casa / ninguna casa.
    if ("anyMatch" in esperado) return Array.isArray(real) && real.some((x) => new RegExp(esperado.anyMatch).test(String(x)));
    if ("noneMatch" in esperado) return Array.isArray(real) && !real.some((x) => new RegExp(esperado.noneMatch).test(String(x)));
    if ("in" in esperado) return esperado.in.some((e) => igual(e, real));
  }
  return igual(esperado, real);
}

/** Igualdad profunda; el orden de las claves de un objeto no importa. */
function igual(a, b) {
  if (a === b) return true;
  if (a === null || b === null || typeof a !== "object" || typeof b !== "object") return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  const ka = Object.keys(a);
  const kb = Object.keys(b);
  return ka.length === kb.length && ka.every((k) => k in b && igual(a[k], b[k]));
}

const describir = (e) =>
  e !== null && typeof e === "object" && !Array.isArray(e)
    ? Object.entries(e).map(([k, v]) => `${k} ${mostrar(v)}`).join(" ")
    : mostrar(e);

/** Devuelve la lista de discrepancias (vacía si todo coincide). */
export function evaluar(esperado, observado) {
  const fallos = [];
  for (const [ruta, e] of Object.entries(esperado ?? {})) {
    const real = obtener(observado, ruta);
    if (!coincide(e, real)) fallos.push(`${ruta}: esperado ${describir(e)}, observado ${mostrar(real)}`);
  }
  return fallos;
}

// ─── Plantillas ──────────────────────────────────────────────────────────────

/** `{me}` (el servicio que actúa) y `{marker}` (único por caso y ejecución), en todo el árbol. */
function expandir(valor, vars) {
  if (typeof valor === "string") return valor.replaceAll("{me}", vars.me ?? "").replaceAll("{marker}", vars.marker);
  if (Array.isArray(valor)) return valor.map((v) => expandir(v, vars));
  if (valor && typeof valor === "object") return Object.fromEntries(Object.entries(valor).map(([k, v]) => [k, expandir(v, vars)]));
  return valor;
}

/**
 * Lo que ve quien llama cuando su petición de core NO recibe respuesta, con los nombres de 11-request-reply.md §4.3:
 * `denied` (el broker deniega la publicación: ~1 ms con `@nats-io/*`, A-66), `no_responders` (estado 503: nadie
 * escucha, A-56) o `timeout` (ni respuesta ni aviso). La denegación llega a veces en `cause` y a veces en el propio
 * mensaje.
 */
export function clasificarPeticion(e) {
  const texto = [...new Set([e?.message, e?.cause?.message].filter(Boolean))].join(" | ") || String(e);
  const nombre = `${e?.name ?? ""} ${e?.cause?.name ?? ""}`;
  if (/Permissions Violation/i.test(texto) || /PermissionViolation/.test(nombre)) return { outcome: "denied", text: texto };
  if (/no responders/i.test(texto) || /NoResponders/.test(nombre)) return { outcome: "no_responders", text: texto };
  if (/timeout/i.test(texto) || /Timeout/.test(nombre)) return { outcome: "timeout", text: texto };
  return { outcome: "error", text: texto };
}

/** Una operación que no se puede ejecutar en este modo (p. ej. con `--acl`): no es un fallo. */
export class Omitido extends Error {}

/**
 * La identidad (nkey o usuario) con la que entra un actor: la del servicio, la de `ops` o la de una VARIANTE suya con
 * permisos estropeados a propósito. La comparten el intérprete (clientes en bruto) y los escenarios con SDKs reales.
 */
export function identidadDeActor(ctx, a) {
  let ident;
  if (a.service === "ops") {
    ident = ctx.identidades.get("ops");
  } else if (esVariante(a)) {
    ident = ctx.variantes.get(claveVariante(a));
    if (!ident) throw new Omitido("una variante de permisos necesita los que genera gen-acl.mjs (no aplica con --acl)");
    if (ident.error) throw new Error(`no se pudo construir la variante: ${ident.error}`);
  } else {
    ident = ctx.identidades.get(a.service);
    if (!ident) throw new Error(`el servicio «${a.service}» no está en el manifiesto`);
  }
  return ident;
}

// ─── El intérprete ───────────────────────────────────────────────────────────

export class Interprete {
  /**
   * @param {object} ctx
   * @param {string} ctx.url            `tls://127.0.0.1:<puerto>`
   * @param {string} ctx.caFile         la CA que firma el certificado del servidor
   * @param {string} ctx.otraCa         una CA que NO lo firmó
   * @param {object} ctx.ops            `{ nc, jsm, js }`: el usuario de operación, sin restricciones
   * @param {object} ctx.canon          protocol.json → consumer
   * @param {Map}    ctx.identidades    servicio → `{ seed }` | `{ user, pass }`; incluye `ops`
   * @param {Map}    ctx.variantes      claveVariante → `{ seed }`; vacío con `--acl`
   * @param {boolean} ctx.generada      `true` si la ACL es la que genera gen-acl.mjs
   * @param {object} ctx.invalida       una clave pública-privada que el servidor NO conoce
   * @param {Function} ctx.nuevaSemilla semilla de usuario que el servidor NO conoce
   */
  constructor(ctx) {
    this.ctx = ctx;
  }

  /** Ejecuta un caso. Devuelve `{ id, ok, pasos: [{ descripcion, ok, omitido, observado, fallos }] }`. */
  async ejecutarCaso(caso) {
    if (caso.onlyWith === "generated-acl" && !this.ctx.generada) {
      return { id: caso.id, ok: true, pasos: [], omitido: "necesita los permisos que genera gen-acl.mjs (no aplica con --acl)" };
    }
    const estado = { actores: new Map(), guardados: new Map(), marker: `${caso.id}-${randomUUID().slice(0, 8)}` };
    const pasos = [];
    try {
      for (const paso of caso.steps) {
        // Un escenario con SDKs reales da un paso por cada paso de su guion (cada uno con su `expect`).
        if (paso.op === "sdk_script") pasos.push(...(await this.#escenarioSdk(paso, estado)));
        else pasos.push(await this.#ejecutarPaso(paso, estado));
      }
    } finally {
      for (const a of estado.actores.values()) await a.nc.close().catch(() => {});
    }
    const ok = pasos.every((p) => p.ok || p.omitido);
    const omitidos = pasos.filter((p) => p.omitido).length;
    // Un caso con TODOS los pasos omitidos no ha comprobado nada: se dice, no se da por bueno.
    return { id: caso.id, ok, pasos, omitidos, omitido: omitidos === pasos.length ? "todos los pasos omitidos" : undefined };
  }

  /** `sdk_script`: ver `sdk-escenario.mjs`. */
  async #escenarioSdk(paso, estado) {
    if (paso.onlyWith === "generated-acl" && !this.ctx.generada) {
      return [{ id: paso.id, descripcion: paso.title ?? "sdk_script", ok: true, omitido: "necesita los permisos que genera gen-acl.mjs (no aplica con --acl)" }];
    }
    const escenario = new EscenarioSdk(this.ctx, identidadDeActor, normalizarActor);
    try {
      return await escenario.ejecutar(expandir(paso, { marker: estado.marker }), { marker: estado.marker, evaluar });
    } catch (e) {
      return [{ id: paso.id, descripcion: paso.title ?? "sdk_script", ok: false, observado: { outcome: "error" }, fallos: [e?.message ?? String(e)] }];
    }
  }

  async #ejecutarPaso(paso, estado) {
    const descripcion = this.describirPaso(paso);
    const base = { id: paso.id, descripcion, control: !!paso.control, note: paso.note, knownDeviation: paso.knownDeviation };
    if (paso.onlyWith === "generated-acl" && !this.ctx.generada) {
      return { ...base, ok: true, omitido: "necesita los permisos que genera gen-acl.mjs (no aplica con --acl)" };
    }
    const vars = { me: paso.as !== undefined ? normalizarActor(paso.as).service : undefined, marker: estado.marker };
    let observado;
    try {
      observado = await this.#operar(resolverRefs(expandir(paso, vars), estado.guardados), estado);
    } catch (e) {
      if (e instanceof Omitido) return { ...base, ok: true, omitido: e.message };
      observado = { outcome: "error", text: e?.message ?? String(e) };
    }
    if (paso.save !== undefined && typeof observado?.seq === "number") estado.guardados.set(paso.save, observado.seq);
    let esperado;
    try {
      esperado = resolverRefs(expandir(paso.expect, vars), estado.guardados);
    } catch (e) {
      return { ...base, ok: false, observado, fallos: [e.message] };
    }
    const fallos = evaluar(esperado, observado);
    return { ...base, ok: fallos.length === 0, observado, fallos, esperado };
  }

  describirPaso(p) {
    const quien = p.as !== undefined ? `[${etiquetaActor(p.as)}] ` : "";
    const cosa = {
      connect: () => `connect tls=${p.tls ?? "ca"} auth=${p.auth ?? "ok"}`,
      publish: () => `publish ${p.subject}`,
      subscribe: () => `subscribe ${p.subject}`,
      request: () => `request ${p.subject}`,
      js_publish: () => `js_publish ${p.subject}${p.expectedLastSubjectSeq !== undefined ? ` (esperada ${typeof p.expectedLastSubjectSeq === "object" ? `ref ${p.expectedLastSubjectSeq.ref}` : p.expectedLastSubjectSeq})` : ""}`,
      direct_get: () => `direct_get ${p.stream}${p.subject !== undefined ? `.${p.subject}` : " (forma de cuerpo)"}`,
      create_consumer: () => `create_consumer ${p.stream}/${p.durable} filtro=${p.filter}${p.subjectFilter !== undefined && p.subjectFilter !== p.filter ? ` (subject: ${p.subjectFilter})` : ""}${p.api ? ` api=${p.api}` : ""}`,
      reset_consumer: () => `reset_consumer ${p.stream}/${p.durable} (${p.deliver ?? "all"})`,
      delete_consumer: () => `delete_consumer ${p.stream}/${p.durable}`,
      pull: () => `pull ${p.stream}/${p.durable}`,
      ack: () => `${p.kind ?? "ack"}`,
      consumer_state: () => `consumer_state ${p.stream}/${p.durable}`,
      rpc_call: () => `rpc_call ${p.subject}`,
      rpc_exchange: () =>
        `rpc_exchange ${p.subject}: [${etiquetaActor(p.caller)}] llama, [${p.as !== undefined ? etiquetaActor(p.as) : "nadie"}] atiende` +
        `${p.replies ? ` (respuestas: ${p.replies.map((r) => `${r.afterMs ?? 0} ms${r.to ? ` a ${r.to}` : ""}`).join(", ")})` : ""}`,
      rpc_cycle: () => `rpc_cycle ${p.subject}: ${p.requests} peticiones a ${p.owners.length} instancias de ${normalizarActor(p.owners[0]).service}`,
      violations: () => `violations`,
      leak_probe: () => `leak_probe espía [${etiquetaActor(p.spy)}] suscrito a ${p.subscribe.join(", ")}; víctima [${etiquetaActor(p.victim)}] lee ${p.reads.durable}`,
      sdk_cycle: () => `sdk_cycle ${p.sdk}: ${p.producer} → ${p.consumer} (${p.subject})`,
      regression: () => `regression ${p.acl}: vuelve a ejecutar ${p.reruns.join(", ")}`,
    }[p.op]?.() ?? p.op;
    return `${quien}${p.title ?? cosa}`;
  }

  // ─── Actores ───────────────────────────────────────────────────────────────

  async #actor(as, estado) {
    const a = normalizarActor(as);
    const clave = JSON.stringify(a);
    if (estado.actores.has(clave)) return estado.actores.get(clave);

    const ident = identidadDeActor(this.ctx, a);
    const inbox = a.inbox ?? (a.service === "ops" ? "default" : "private");
    const inboxPrefix = inbox === "default" ? undefined : inbox === "private" ? `_INBOX.${a.service}` : inbox;
    const nc = await conectar(this.ctx.url, {
      nombre: etiquetaActor(as),
      ...ident,
      inboxPrefix,
      tls: { caFile: this.ctx.caFile },
      etiqueta: "flux-security",
    });
    const actor = { nc, nombre: a.service, ultimo: null, inboxPrefix };
    estado.actores.set(clave, actor);
    return actor;
  }

  // ─── Operaciones ───────────────────────────────────────────────────────────

  /** `p` llega con las plantillas ya expandidas. */
  async #operar(p, estado) {
    const ctx = this.ctx;
    const t0 = Date.now();

    switch (p.op) {
      case "connect":
        return this.#conectarPrueba(normalizarActor(p.as), { tls: p.tls ?? "ca", auth: p.auth ?? "ok" });

      case "publish": {
        const a = await this.#actor(p.as, estado);
        const antes = a.nc.violaciones.length;
        const r = await publicarCrudo(a.nc, p.subject);
        return { outcome: r === "denegado" ? "denied" : "permitted", text: a.nc.violaciones.slice(antes).join(" | ") };
      }

      case "subscribe": {
        const a = await this.#actor(p.as, estado);
        const antes = a.nc.violaciones.length;
        const sub = a.nc.subscribe(p.subject, p.queue ? { queue: p.queue } : undefined);
        await a.nc.flush();
        await sleep(250);
        const nuevas = a.nc.violaciones.slice(antes);
        const denegada = nuevas.some((v) => v.includes(`Subscription to "${p.subject}"`));
        if (denegada) sub.unsubscribe();
        return { outcome: denegada ? "denied" : "permitted", text: nuevas.join(" | ") };
      }

      case "request": {
        const a = await this.#actor(p.as, estado);
        const r = await pedir(a.nc, p.subject, p.body ?? {}, p.timeoutMs ?? 1500);
        return this.#observarPeticion(r);
      }

      case "js_publish": {
        const a = await this.#actor(p.as, estado);
        try {
          const ack = await jetstream(a.nc, { timeout: 1500 }).publish(p.subject, enc.encode(typeof p.data === "string" ? p.data : JSON.stringify(p.data ?? {})), {
            msgID: p.msgId ?? randomUUID(),
            ...(p.expectedLastSubjectSeq !== undefined && { expect: { lastSubjectSequence: p.expectedLastSubjectSeq } }),
          });
          return { outcome: "ok", stream: ack.stream, seq: ack.seq, ms: Date.now() - t0 };
        } catch (e) {
          const denegado = /Permissions Violation/i.test(`${e?.message} ${e?.cause?.message}`);
          // `errCode` es el err_code de JetStream (10071 «wrong last sequence», 10164 en un cluster).
          return { outcome: denegado ? "denied" : "error", errCode: typeof e?.code === "number" ? e.code : undefined, text: resumen(e), ms: Date.now() - t0 };
        }
      }

      case "direct_get": {
        // La lectura por subject (`$JS.API.DIRECT.GET.<stream>.<subject>`, A-32) con las cabeceras que
        // trae, o —sin `subject`— la forma de CUERPO, que lee cualquier mensaje y que ninguna ACL ve.
        const a = await this.#actor(p.as, estado);
        const subj = p.subject === undefined ? `$JS.API.DIRECT.GET.${p.stream}` : `$JS.API.DIRECT.GET.${p.stream}.${p.subject}`;
        const cuerpo = p.body === undefined ? "" : JSON.stringify(p.body);
        try {
          const r = await a.nc.request(subj, cuerpo, { timeout: p.timeoutMs ?? 1500 });
          const code = r.headers?.code ?? 0;
          const ms = Date.now() - t0;
          if (code === 404) return { outcome: "not_found", status: 404, text: r.headers.description, ms };
          if (code >= 400) return { outcome: "bad_request", status: code, text: r.headers.description, ms };
          return { outcome: "found", status: 200, seq: Number(r.headers.get("Nats-Sequence")), subject: r.headers.get("Nats-Subject"), data: dec.decode(r.data), ms };
        } catch (e) {
          const texto = [...new Set([e?.message, e?.cause?.message].filter(Boolean))].join(" | ");
          if (/Permissions Violation/i.test(texto)) return { outcome: "denied", text: texto, ms: Date.now() - t0 };
          // Un stream que no existe, o sin allow_direct, no contesta nadie (A-32).
          return { outcome: /no responders/i.test(texto) ? "no_answer" : "error", text: texto, ms: Date.now() - t0 };
        }
      }

      case "create_consumer": {
        const a = await this.#actor(p.as, estado);
        const filtroSubject = p.subjectFilter === undefined ? p.filter : p.subjectFilter;
        const subject =
          p.api === "durable_create"
            ? `$JS.API.CONSUMER.DURABLE.CREATE.${p.stream}.${p.durable}`
            : `$JS.API.CONSUMER.CREATE.${p.stream}.${p.durable}${filtroSubject === null ? "" : `.${filtroSubject}`}`;
        const cuerpo = { stream_name: p.stream, config: { ...configConsumidorCanonica(ctx.canon, p.durable, p.filter), deliver_policy: p.deliver ?? "all" } };
        return this.#observarPeticion(await pedir(a.nc, subject, cuerpo, 1500));
      }

      case "reset_consumer": {
        await ctx.ops.jsm.consumers.delete(p.stream, p.durable).catch(() => {});
        const cfg = { ...configConsumidorCanonica(ctx.canon, p.durable, p.filter), deliver_policy: p.deliver ?? "all" };
        await ctx.ops.jsm.consumers.add(p.stream, cfg);
        return { outcome: "ok" };
      }

      case "delete_consumer": {
        await ctx.ops.jsm.consumers.delete(p.stream, p.durable);
        return { outcome: "ok" };
      }

      case "pull": {
        const a = await this.#actor(p.as, estado);
        return this.#pull(a, p);
      }

      case "ack": {
        const a = await this.#actor(p.as, estado);
        const m = a.ultimo;
        if (!m) throw new Error("no hay un mensaje recibido al que responder: falta un `pull` antes");
        const antes = a.nc.violaciones.length;
        const kind = p.kind ?? "ack";
        if (kind === "ack") m.ack();
        else if (kind === "wip") m.working();
        else if (kind === "nak") m.nak(p.delayMs);
        else if (kind === "term") m.term();
        else throw new Error(`ack: tipo desconocido «${kind}»`);
        await a.nc.flush();
        await sleep(300);
        const v = a.nc.violaciones.slice(antes).find((x) => x.includes("$JS.ACK")) ?? null;
        return { outcome: "sent", violation: v };
      }

      case "consumer_state": {
        const i = await ctx.ops.jsm.consumers.info(p.stream, p.durable);
        return {
          outcome: "ok",
          ackPending: i.num_ack_pending,
          ackFloor: i.ack_floor.stream_seq,
          numPending: i.num_pending,
          redelivered: i.num_redelivered,
          delivered: i.delivered.consumer_seq,
          deliveredStream: i.delivered.stream_seq,
        };
      }

      case "violations": {
        const a = await this.#actor(p.as, estado);
        return { count: a.nc.violaciones.length, text: a.nc.violaciones.join(" | ") };
      }

      case "leak_probe":
        return this.#leakProbe(p, estado);

      case "rpc_call":
        return this.#rpc(p, estado, { quienLlama: p.as, dueño: null });

      case "rpc_exchange":
        return this.#rpc(p, estado, { quienLlama: p.caller, dueño: p.as === undefined ? null : await this.#actor(p.as, estado) });

      case "rpc_cycle":
        return this.#cicloRpc(p, estado);

      default:
        throw new Error(`la operación «${p.op}» no la ejecuta el intérprete`);
    }
  }


  /**
   * S8: una conexión que NO debe establecerse. Cada eje se estropea por separado:
   *
   *   tls   `ca` (la buena), `wrong_ca` (confía en una CA que no firmó el certificado),
   *         `system` (solo las CA del sistema: la efímera no está), `none` (texto plano)
   *   auth  `ok`, `unknown_key` (una nkey bien formada que el servidor no conoce), `none`
   *
   * `none` NO pasa por la librería: la de NATS sube sola a TLS cuando el servidor lo exige
   * (medido: `nats://` contra un servidor con TLS acaba en UNABLE_TO_VERIFY_LEAF_SIGNATURE, no
   * en texto plano), y eso no probaría que unas credenciales en claro se rechazan. Se habla
   * el protocolo a mano por un socket: el servidor anuncia `tls_required` y, en cuanto llegan
   * bytes que no son un ClientHello, cierra sin contestar.
   */
  async #conectarPrueba(actor, { tls, auth }) {
    const ctx = this.ctx;
    const ident = actor.service === "ops" ? ctx.identidades.get("ops") : ctx.identidades.get(actor.service);
    if (!ident) throw new Error(`el servicio «${actor.service}» no está en el manifiesto`);
    const credencial = auth === "ok" ? ident : auth === "unknown_key" ? { seed: nuevaClave().seed } : {};
    const t0 = Date.now();

    if (tls === "none") return this.#conectarEnPlano(credencial, t0);

    const caFile = tls === "ca" ? ctx.caFile : tls === "wrong_ca" ? ctx.otraCa : undefined;
    try {
      const nc = await connect({
        servers: ctx.url,
        name: `flux-security:s8:${tls}:${auth}`,
        timeout: 3000,
        reconnect: false,
        tls: caFile ? { caFile } : {},
        ...(credencial.seed ? { authenticator: nkeyAuthenticator(credencial.seed) } : credencial.user ? { user: credencial.user, pass: credencial.pass } : {}),
      });
      await nc.flush();
      await nc.close();
      return { outcome: "connected", text: "", ms: Date.now() - t0 };
    } catch (e) {
      return { outcome: "rejected", text: `${e?.code ?? e?.name ?? ""} ${e?.message ?? e}`.trim(), ms: Date.now() - t0 };
    }
  }

  #conectarEnPlano(credencial, t0) {
    const url = new URL(this.ctx.url.replace(/^tls:/, "nats:"));
    return new Promise((resolver) => {
      const s = net.connect(Number(url.port), url.hostname);
      let buf = "";
      let enviado = false;
      const fin = (outcome, text) => {
        s.destroy();
        resolver({ outcome, text, ms: Date.now() - t0 });
      };
      const plazo = setTimeout(() => fin("rejected", "sin respuesta del servidor"), 1500);
      s.on("data", (d) => {
        buf += d;
        if (!enviado && buf.includes("\r\n")) {
          enviado = true;
          const info = JSON.parse(buf.slice(buf.indexOf("{"), buf.indexOf("\r\n")));
          const conn = { verbose: false, pedantic: false, tls_required: false, name: "flux-security:plano", lang: "node", version: "0", protocol: 1 };
          if (credencial.seed) {
            const kp = nkeys.fromSeed(credencial.seed);
            conn.nkey = kp.getPublicKey();
            conn.sig = Buffer.from(kp.sign(enc.encode(info.nonce ?? ""))).toString("base64url");
          } else if (credencial.user) {
            conn.user = credencial.user;
            conn.pass = credencial.pass;
          }
          s.write(`CONNECT ${JSON.stringify(conn)}\r\nPING\r\n`);
          return;
        }
        if (buf.includes("PONG") || buf.includes("+OK")) {
          clearTimeout(plazo);
          fin("connected", "el servidor aceptó una sesión en texto plano");
        } else if (buf.includes("-ERR")) {
          clearTimeout(plazo);
          fin("rejected", buf.slice(buf.indexOf("-ERR")).split("\r\n")[0]);
        }
      });
      s.on("close", () => {
        clearTimeout(plazo);
        fin("rejected", "el servidor cerró la conexión sin contestar (exige TLS antes de leer el CONNECT)");
      });
      s.on("error", (e) => {
        clearTimeout(plazo);
        fin("rejected", `error de socket: ${e.message}`);
      });
    });
  }

  #observarPeticion(r) {
    return {
      outcome: r.estado === "denegado" ? "denied" : r.estado === "respondido" ? "answered" : "no_answer",
      text: r.texto ?? r.error ?? "",
      reply: r.json ?? null,
      ms: r.ms,
    };
  }

  /** Como un SDK: `consumers.get` (CONSUMER.INFO), pedir UN mensaje (MSG.NEXT) y mirarlo. */
  async #pull(a, p) {
    const js = jetstream(a.nc, { timeout: 2000 });
    let consumidor;
    try {
      consumidor = await js.consumers.get(p.stream, p.durable);
    } catch (e) {
      const denegado = /Permissions Violation/i.test(`${e?.message} ${e?.cause?.message}`);
      return { outcome: denegado ? "denied" : "error", text: resumen(e) };
    }
    const antes = a.nc.violaciones.length;
    const m = await recibirUno(consumidor, p.timeoutMs ?? 2500);
    if (!m) {
      // Una petición de mensajes denegada no lanza: el broker avisa por la conexión (A-20).
      const v = a.nc.violaciones.slice(antes).find((x) => x.includes("MSG.NEXT"));
      return v ? { outcome: "denied", text: v } : { outcome: "none" };
    }
    a.ultimo = m;
    return {
      outcome: "message",
      subject: m.subject,
      data: dec.decode(m.data),
      seq: m.seq,
      deliveryCount: m.info.deliveryCount,
      ackTokens: m.reply.split(".").length,
      ackSubject: m.reply,
    };
  }

  /**
   * Una petición/respuesta de extremo a extremo, con lo que haría un SDK `rpc` pero SIN ningún SDK de flux
   * (11-request-reply.md §4 y §5): el dueño (`as`, si lo hay) se suscribe al subject EXACTO de la operación con su
   * nombre de servicio como grupo de cola y responde según `replies`; quien llama (`caller`, o `as` en `rpc_call`)
   * pide por core con su inbox privado.
   *
   * `watchInbox`: `other-connection` (por defecto) cuenta por otra conexión; `same-connection`, por la misma, que es la trampa.
   *
   * `replies` es el PLAN de respuestas del dueño a cada petición: `{ afterMs, to }`, y por cada entrada publica un
   * mensaje (`to` es el `reply` de la petición por defecto). Con dos entradas contesta DOS veces: es como se prueba
   * `allow_responses: { max: 1 }`. Que el broker deniegue una publicación no lanza nada: el aviso llega por la
   * conexión (A-19, A-28), y por eso se cuentan los `-ERR` del dueño (`ownerDenied`).
   *
   * Observa lo que ve quien llama (`outcome`: answered | denied | no_responders | timeout), cuántas respuestas
   * llegaron DE VERDAD a su inbox (`delivered`, con una suscripción propia a su inbox privado), si el dueño pudo
   * suscribirse (`subscription`) y qué leyó un espía, si lo hay (`leaked`, y qué suscripciones suyas denegó el broker:
   * `spySubscriptions`).
   */
  async #rpc(p, estado, { quienLlama, dueño }) {
    const llamante = await this.#actor(quienLlama, estado);
    const marca = `SECRETO-${randomUUID()}`;
    const planes = p.replies ?? [{ afterMs: 0 }];
    const limpiar = [];
    const o = { recibidas: 0, suscripcion: "none" };

    // El dueño escucha EXACTAMENTE la operación, en su grupo de cola (el SDK no se suscribe al comodín: A-64).
    let violacionesDueño0 = 0;
    if (dueño) {
      violacionesDueño0 = dueño.nc.violaciones.length;
      const sub = dueño.nc.subscribe(p.subject, { queue: p.queue ?? dueño.nombre });
      limpiar.push(() => sub.unsubscribe());
      (async () => {
        try {
          for await (const m of sub) {
            o.recibidas++;
            if (!m.reply) continue;
            planes.forEach((r, i) => {
              setTimeout(() => {
                try {
                  dueño.nc.publish(r.to ?? m.reply, enc.encode(JSON.stringify({ n: i + 1, marker: marca })));
                } catch {
                  /* la conexión ya se cerró */
                }
              }, r.afterMs ?? 0);
            });
          }
        } catch {
          /* suscripción denegada: el iterador lanza (A-8f) */
        }
      })();
      await dueño.nc.flush();
      await sleep(250);
      o.suscripcion = dueño.nc.violaciones.slice(violacionesDueño0).some((v) => v.includes(`Subscription to "${p.subject}"`)) ? "denied" : "permitted";
    }

    // Un espía, si lo hay, mira lo que se le pida mientras tanto (A-71).
    const visto = [];
    let suscripcionesEspia;
    if (p.spy !== undefined) {
      const espia = await this.#actor(p.spy, estado);
      const antes = espia.nc.violaciones.length;
      for (const subject of p.spySubscribe ?? []) vigilar(espia.nc.subscribe(subject), marca, visto);
      await espia.nc.flush();
      await sleep(300);
      const nuevas = espia.nc.violaciones.slice(antes);
      suscripcionesEspia = Object.fromEntries((p.spySubscribe ?? []).map((s) => [s, nuevas.some((v) => v.includes(`Subscription to "${s}"`)) ? "denied" : "permitted"]));
    }

    // Cuántas respuestas llegan DE VERDAD al inbox de quien llama: una suscripción a su inbox privado, que su ACL
    // permite, las ve todas. Va por OTRA conexión con la misma identidad: una segunda suscripción de la MISMA conexión que
    // case con el `reply` (`_INBOX.<servicio>.>`) hace que el 503 de `no responders` no llegue a la librería y se
    // convierta en un timeout (medido, A-101). `watchInbox: "same-connection"` lo hace a propósito, para observarlo.
    let entregadas = 0;
    const cuenta = p.watchInbox === "same-connection" ? llamante : await this.#actor({ ...normalizarActor(quienLlama), instance: "contador" }, estado);
    const contador = cuenta.nc.subscribe(cuenta.inboxPrefix ? `${cuenta.inboxPrefix}.>` : "_INBOX.>");
    limpiar.push(() => contador.unsubscribe());
    (async () => {
      try {
        for await (const m of contador) if (dec.decode(m.data).includes(marca)) entregadas++;
      } catch {
        /* suscripción denegada (una variante sin su inbox) */
      }
    })();
    await cuenta.nc.flush();
    await sleep(150);

    // La petición.
    const v0 = llamante.nc.violaciones.length;
    const t0 = Date.now();
    let resultado;
    try {
      const r = await llamante.nc.request(p.subject, enc.encode(JSON.stringify(p.body ?? { marker: marca })), { timeout: p.timeoutMs ?? 1500 });
      let json = null;
      try {
        json = JSON.parse(dec.decode(r.data));
      } catch {
        /* no era JSON */
      }
      resultado = { outcome: "answered", reply: json, text: "" };
    } catch (e) {
      resultado = clasificarPeticion(e);
    }
    const ms = Date.now() - t0;

    // Lo que el dueño aún tenga que contestar (un plan con respuestas tardías) y lo que viaja.
    const hasta = Math.max(0, ...planes.map((r) => r.afterMs ?? 0)) + 500;
    if (dueño && hasta > ms) await sleep(hasta - ms);
    else await sleep(300);

    const deDueño = dueño ? dueño.nc.violaciones.slice(violacionesDueño0) : [];
    const denegadas = deDueño.filter((v) => /Publish to "_INBOX\./.test(v));
    for (const f of limpiar) f();
    return {
      outcome: resultado.outcome,
      text: resultado.text,
      reply: resultado.reply ?? null,
      ms,
      delivered: entregadas,
      received: o.recibidas,
      subscription: o.suscripcion,
      ownerDenied: denegadas.length,
      ownerViolations: deDueño.join(" | "),
      callerViolations: llamante.nc.violaciones.slice(v0).join(" | "),
      leaked: visto.length > 0,
      via: [...new Set(visto)],
      spySubscriptions: suscripcionesEspia,
    };
  }

  /**
   * El ciclo de la aceptación de la Fase 3 (11-request-reply.md §12.9) con permisos generados y SIN SDK: varias
   * instancias del dueño (cada una, su conexión con la misma identidad) atienden la operación en el mismo grupo de
   * cola; `requests` peticiones CONCURRENTES las reparten y cada una recibe UNA respuesta; después las instancias
   * dejan de escuchar (UNSUB y flush, que es lo que hace el drenaje) y la siguiente petición da `no responders`
   * al instante. Ejecuta `allow_responses` con muchas respuestas a la vez: ninguna se deniega.
   */
  async #cicloRpc(p, estado) {
    const llamante = await this.#actor(p.caller, estado);
    const dueños = await Promise.all(p.owners.map((o) => this.#actor(o, estado)));
    const marca = `CICLO-${randomUUID()}`;
    const porInstancia = dueños.map(() => 0);
    const antes = dueños.map((d) => d.nc.violaciones.length);
    const subs = dueños.map((d, i) => {
      const sub = d.nc.subscribe(p.subject, { queue: d.nombre });
      (async () => {
        try {
          for await (const m of sub) {
            porInstancia[i]++;
            if (m.reply) d.nc.publish(m.reply, enc.encode(JSON.stringify({ instancia: i, marker: marca })));
          }
        } catch {
          /* suscripción denegada */
        }
      })();
      return sub;
    });
    await Promise.all(dueños.map((d) => d.nc.flush()));
    await sleep(250);

    const peticiones = await Promise.all(
      Array.from({ length: p.requests }, async () => {
        try {
          const r = await llamante.nc.request(p.subject, enc.encode(JSON.stringify({ marker: marca })), { timeout: p.timeoutMs ?? 3000 });
          return JSON.parse(dec.decode(r.data)).marker === marca ? "answered" : "wrong";
        } catch (e) {
          return clasificarPeticion(e).outcome;
        }
      }),
    );
    await sleep(300);
    const denegadas = dueños.flatMap((d, i) => d.nc.violaciones.slice(antes[i])).filter((v) => /Publish to "_INBOX\./.test(v));

    // Parar: dejar de escuchar, y comprobar que el servidor lo ha procesado (el PONG llega tras el UNSUB).
    for (const sub of subs) sub.unsubscribe();
    await Promise.all(dueños.map((d) => d.nc.flush()));
    const t0 = Date.now();
    let parado;
    try {
      await llamante.nc.request(p.subject, enc.encode(JSON.stringify({ marker: marca })), { timeout: 2000 });
      parado = { outcome: "answered" };
    } catch (e) {
      parado = clasificarPeticion(e);
    }
    return {
      outcome: "ok",
      answered: peticiones.filter((x) => x === "answered").length,
      failed: peticiones.filter((x) => x !== "answered").length,
      byInstance: porInstancia,
      minShare: Math.min(...porInstancia),
      ownerDenied: denegadas.length,
      stopOutcome: parado.outcome,
      stopMs: Date.now() - t0,
    };
  }

  /**
   * El escenario de A-2 / A-22: un espía se suscribe a lo que el generador no le concede
   * mientras una víctima LEGÍTIMA consume por pull. Si el espía recibe el contenido, hay fuga.
   * La víctima puede ser `ops` con el prefijo de inbox de otro servicio: lo que importa de ella
   * es por dónde viajan sus entregas, no sus permisos.
   */
  async #leakProbe(p, estado) {
    const ctx = this.ctx;
    const spy = await this.#actor(p.spy, estado);
    const victim = await this.#actor(p.victim, estado);
    const marca = `SECRETO-${randomUUID()}`;
    await ctx.ops.jsm.consumers.delete(p.reads.stream, p.reads.durable).catch(() => {});
    await ctx.ops.jsm.consumers.add(p.reads.stream, {
      ...configConsumidorCanonica(ctx.canon, p.reads.durable, p.reads.filter),
      deliver_policy: "new",
    });

    const visto = [];
    const antes = spy.nc.violaciones.length;
    for (const subject of p.subscribe) vigilar(spy.nc.subscribe(subject), marca, visto);
    await spy.nc.flush();
    await sleep(300);
    const nuevas = spy.nc.violaciones.slice(antes);
    const suscripciones = Object.fromEntries(
      p.subscribe.map((s) => [s, nuevas.some((v) => v.includes(`Subscription to "${s}"`)) ? "denied" : "permitted"]),
    );

    await ctx.ops.js.publish(p.reads.filter, enc.encode(JSON.stringify({ sku: marca })));
    const consumidor = await jetstream(victim.nc).consumers.get(p.reads.stream, p.reads.durable);
    const m = await recibirUno(consumidor, 2500);
    m?.ack();
    await sleep(700);
    return {
      victimRead: !!m && dec.decode(m.data).includes(marca),
      leaked: visto.length > 0,
      subject: visto[0],
      via: [...new Set(visto)],
      subscriptions: suscripciones,
    };
  }
}

export const etiquetaActor = (as) => {
  const a = typeof as === "string" ? { service: as } : as;
  if (!a) return "?";
  const extras = [
    a.instance !== undefined && `instancia ${a.instance}`,
    a.without?.length && `sin ${a.without.length} permiso(s)`,
    a.withoutSubscribe?.length && `sin ${a.withoutSubscribe.length} suscripción(es)`,
    a.allowResponses !== undefined && `allow_responses=${JSON.stringify(a.allowResponses)}`,
    a.extraPublish?.length && `+${a.extraPublish.length} publish`,
    a.extraSubscribe?.length && `+${a.extraSubscribe.length} subscribe`,
    a.replaceSubscribe && `subscribe=${JSON.stringify(a.replaceSubscribe)}`,
    a.dev && "--dev",
    a.inbox && a.inbox !== "private" && `inbox=${a.inbox}`,
  ].filter(Boolean);
  return extras.length ? `${a.service}: ${extras.join(", ")}` : a.service;
};
