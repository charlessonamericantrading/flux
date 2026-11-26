/**
 * `sdk_script`: un escenario con SDKs REALES de flux, escrito como DATOS en el JSON de los casos.
 *
 * El intérprete de `security-acl.json` habla NATS en bruto, y eso es justo lo que no puede hacer aquí: lo que se mide son las
 * suscripciones, los permisos y los tiempos DEL SDK. Un escenario lanza procesos (`agentes.mjs`: uno por instancia de un
 * servicio, en Node o en Rust, con la identidad nkey de un servicio del manifiesto o de una VARIANTE suya con un permiso de
 * menos) y les da órdenes en el orden del guion; después compara cada respuesta con lo esperado, y mira por su cuenta lo que
 * el broker anotó (violaciones de permiso en su log, suscripciones reales de cada conexión por su puerto de monitorización).
 *
 *   { "op": "sdk_script",
 *     "agents": [ { "id": "crm1", "sdk": "node", "as": "crm-api" }, { "id": "fact", "sdk": "rust", "as": "facturacion-api", "tenant": "globex" } ],
 *     "script": [ { "agent": "crm1", "cmd": "respond", "operation": "clientes.cliente.v1.obtener", "expect": { "ok": true } }, … ] }
 *
 * `signing` de un agente (sin él, no firma ni verifica): `"require"` | `"warn"` | `"sign"` (firma y no verifica), o `{ sign?, verify?, knows?: [servicios
 * cuya clave pública conoce], swap?: { servicio: otro } }`. Las claves Ed25519 las crea el orquestador, una por servicio (`<servicio>-1`).
 *
 * Un agente se lanza la primera vez que un paso lo nombra y todos se cierran al acabar el escenario. Cada paso del guion es un
 * paso del caso: pasa o falla por su `expect`, que se evalúa sobre lo que el agente contestó MÁS lo que añade el orquestador:
 *
 *   wallMs                  lo que tardó la orden, medido aquí
 *   serverViolations[]      las violaciones de permiso que el nats-server anotó durante el paso (`serverViolationCount`: cuántas;
 *                           `scenarioViolations[]` y `scenarioViolationCount`: las de todo el escenario). Un paso que las mira
 *                           espera antes a que el log del servidor deje de crecer (100 ms): llega por otra tubería que la respuesta
 *
 * Órdenes de los agentes (el mismo protocolo en los dos SDKs; el vocabulario completo, en `security-acl.json`):
 *   respond · behavior · stop · call · call_repeat · call_many · consume · watch · publish · state_put · stats · close
 * Órdenes del orquestador:
 *   sleep {ms}              espera
 *   stats {agents:[…]}      suma `stats` de varios agentes (`total`, `perAgent`)
 *   broker_subs {agent}     las suscripciones de la conexión del agente, MIRADAS EN EL BROKER (`/connz`), y cuántas casan con
 *                           el `reply` de una petición suya: solo la del buzón único de la librería (A-101)
 *   acl {service}           los permisos que genera gen-acl.mjs para un servicio del manifiesto
 *   kill {agent}            SIGKILL: un servicio que muere
 *
 * `until` (en cualquier paso): repite la orden cada 200 ms hasta que se cumpla o pasen `waitMs` (por defecto 20 s): para
 * esperar a un efecto asíncrono —un evento que llega— sin dormir a ciegas.
 */

import { generateKeyPairSync } from "node:crypto";
import http from "node:http";

import { dec } from "../../scripts/lib/nats-harness.mjs";
import { Agente, binarioDeGo, binarioDeRust } from "./agentes.mjs";

export const CMDS_AGENTE = ["respond", "behavior", "stop", "call", "call_repeat", "call_many", "consume", "watch", "publish", "state_put", "stats", "close"];
export const CMDS_ORQUESTADOR = ["sleep", "stats", "broker_subs", "acl", "kill"];
const CAMPOS_AGENTE = ["id", "sdk", "as", "tenant", "label", "signing", "tramposo"];
const CAMPOS_PASO = ["agent", "agents", "cmd", "title", "note", "control", "expect", "until", "waitMs", "knownDeviation", "id",
  "operation", "behavior", "data", "options", "n", "concurrency", "repeat", "subject", "contract", "key", "tenantId", "ms", "service"];

// ─── Validación del JSON ──────────────────────────────────────────────────────

/** Lanza con la ruta si el escenario está mal escrito: una errata no puede degradarse a «no se comprueba nada». */
export function validarEscenario(paso, donde) {
  if (!Array.isArray(paso.agents) || paso.agents.length === 0) throw new Error(`${donde}: sdk_script sin \`agents\``);
  if (!Array.isArray(paso.script) || paso.script.length === 0) throw new Error(`${donde}: sdk_script sin \`script\``);
  const ids = new Set();
  for (const a of paso.agents) {
    for (const k of Object.keys(a)) if (!CAMPOS_AGENTE.includes(k)) throw new Error(`${donde}: campo de agente desconocido «${k}». Admite: ${CAMPOS_AGENTE.join(", ")}`);
    for (const k of ["id", "sdk", "as"]) if (a[k] === undefined) throw new Error(`${donde}: un agente sin \`${k}\``);
    if (!["node", "rust", "go"].includes(a.sdk) && !/^\{\w+\}$/.test(a.sdk)) throw new Error(`${donde}: sdk «${a.sdk}» desconocido (node | rust | go)`);
    if (ids.has(a.id)) throw new Error(`${donde}: dos agentes con el id «${a.id}»`);
    ids.add(a.id);
  }
  paso.script.forEach((s, i) => {
    const d = `${donde}, guion ${i + 1}`;
    for (const k of Object.keys(s)) if (!CAMPOS_PASO.includes(k)) throw new Error(`${d}: campo desconocido «${k}». Admite: ${CAMPOS_PASO.join(", ")}`);
    if (!s.cmd) throw new Error(`${d}: falta \`cmd\``);
    const propia = CMDS_ORQUESTADOR.includes(s.cmd) && !(s.cmd === "stats" && s.agent !== undefined);
    if (!propia && !CMDS_AGENTE.includes(s.cmd)) throw new Error(`${d}: orden desconocida «${s.cmd}». Las hay: ${[...new Set([...CMDS_AGENTE, ...CMDS_ORQUESTADOR])].join(", ")}`);
    if (s.cmd === "stats" && s.agent !== undefined && s.agents !== undefined) throw new Error(`${d}: \`stats\` lleva \`agent\` o \`agents\`, no los dos`);
    for (const a of [...(s.agents ?? []), ...(s.agent !== undefined ? [s.agent] : [])]) {
      if (!ids.has(a)) throw new Error(`${d}: el agente «${a}» no está en \`agents\` (${[...ids].join(", ")})`);
    }
    if (!propia && s.agent === undefined) throw new Error(`${d}: «${s.cmd}» necesita \`agent\``);
    if (s.expect === undefined && !s.control) throw new Error(`${d}: sin \`expect\`: un paso que no comprueba nada pasaría siempre`);
  });
}

// ─── Las identidades de firma ────────────────────────────────────────────────

/** Un par Ed25519 en PEM (PKCS#8 y SPKI): el formato que aceptan los dos SDKs. */
function parDeClaves(servicio) {
  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  return {
    keyId: `${servicio}-1`,
    privateKey: privateKey.export({ type: "pkcs8", format: "pem" }),
    publicKey: publicKey.export({ type: "spki", format: "pem" }),
  };
}

// ─── Utilidades ──────────────────────────────────────────────────────────────

const esViolacion = (l) => /\b(?:Publish|Subscription) Violation\b/.test(l);

/** Un patrón de subject de NATS (`*`, `>`) contra un subject concreto. */
export function casaSubject(patron, subject) {
  const p = patron.split(".");
  const s = subject.split(".");
  for (let i = 0; i < p.length; i++) {
    if (p[i] === ">") return s.length > i;
    if (i >= s.length) return false;
    if (p[i] !== "*" && p[i] !== s[i]) return false;
  }
  return p.length === s.length;
}

function getJson(url) {
  return new Promise((resolver, rechazar) => {
    http
      .get(url, (r) => {
        let t = "";
        r.on("data", (d) => (t += d));
        r.on("end", () => {
          try {
            resolver(JSON.parse(t));
          } catch (e) {
            rechazar(new Error(`${url}: ${e.message}`));
          }
        });
      })
      .on("error", rechazar);
  });
}

const dormir = (ms) => new Promise((r) => setTimeout(r, ms));

// ─── El escenario ────────────────────────────────────────────────────────────

export class EscenarioSdk {
  /**
   * @param {object} ctx  el del Interprete: `url`, `caFile`, `identidades`, `variantes`, `srv`, `monitor`, `generadas` (permisos)
   * @param {Function} identidadDe (ctx, actor) → `{ service, seed }`
   */
  constructor(ctx, identidadDe, normalizarActor) {
    this.ctx = ctx;
    this.identidadDe = identidadDe;
    this.normalizarActor = normalizarActor;
    this.agentes = new Map();
    this.definiciones = new Map();
    this.claves = new Map();
  }

  /** La clave de firma de un servicio, creada la primera vez que algún agente la necesita. */
  #clave(servicio) {
    if (!this.claves.has(servicio)) this.claves.set(servicio, parDeClaves(servicio));
    return this.claves.get(servicio);
  }

  #firmaDe(def, servicios) {
    if (def.signing === undefined) return undefined;
    const s = typeof def.signing === "string" ? { verify: def.signing === "sign" ? "off" : def.signing } : def.signing;
    const propio = this.normalizarActor(def.as).service;
    const conoce = s.knows ?? servicios;
    // `swap`: { "<servicio>": "<otro>" } — este agente cree que la clave pública de <servicio> es la de <otro> (una clave equivocada).
    const publica = (n) => this.#clave(s.swap?.[n] ?? n).publicKey;
    return {
      keyId: this.#clave(propio).keyId,
      ...(s.sign !== false && { privateKey: this.#clave(propio).privateKey }),
      publicKeys: Object.fromEntries(conoce.map((n) => [this.#clave(n).keyId, publica(n)])),
      verify: s.verify ?? "off",
    };
  }

  async #agente(id, paso, marker) {
    if (this.agentes.has(id)) return this.agentes.get(id);
    const def = this.definiciones.get(id);
    const actor = this.normalizarActor(def.as);
    const ident = this.identidadDe(this.ctx, actor);
    const servicios = [...new Set([...this.definiciones.values()].map((d) => this.normalizarActor(d.as).service))];
    const firma = this.#firmaDe(def, servicios);
    const env = {
      FLUX_P_URL: this.ctx.url,
      FLUX_P_CA: this.ctx.caFile,
      FLUX_P_SERVICE: actor.service,
      FLUX_P_SEED: dec.decode(ident.seed),
      FLUX_P_TENANT: def.tenant ?? "acme",
      FLUX_P_LABEL: def.label ?? def.id,
      ...(firma && { FLUX_P_SIGNING: JSON.stringify(firma) }),
      ...(def.tramposo && { FLUX_P_TRAMPOSO: "1" }),
    };
    const a = new Agente({ id, sdk: def.sdk, env });
    const listo = await a.lanzar();
    this.agentes.set(id, a);
    if (listo.ok === false) a.errorDeConexion = listo.error;
    return a;
  }

  /** Ejecuta el escenario entero y devuelve un paso por cada paso del guion. */
  async ejecutar(paso, { marker, evaluar }) {
    for (const d of paso.agents) this.definiciones.set(d.id, d);
    this.logAlEmpezar = this.ctx.srv?.salida?.length ?? 0;
    // La compilación del participante de Rust (la primera vez, segundos) no debe contarse en el tiempo de un paso.
    if (paso.agents.some((d) => d.sdk === "rust")) binarioDeRust();
    if (paso.agents.some((d) => d.sdk === "go")) binarioDeGo();
    const pasos = [];
    try {
      for (const s of paso.script) pasos.push(await this.#paso(s, { marker, evaluar }));
    } finally {
      await Promise.all([...this.agentes.values()].map((a) => a.cerrar()));
    }
    return pasos;
  }

  async #paso(s, { marker, evaluar }) {
    const descripcion = this.describir(s);
    const base = { descripcion, control: !!s.control, note: s.note, knownDeviation: s.knownDeviation, id: s.id };
    const logAntes = this.ctx.srv?.salida?.length ?? 0;
    const t0 = Date.now();
    let obs;
    try {
      obs = await this.#orden(s, marker);
      if (s.until) {
        const limite = Date.now() + (s.waitMs ?? 20_000);
        while (evaluar(s.until, obs).length && Date.now() < limite) {
          await dormir(200);
          obs = await this.#orden(s, marker);
        }
      }
    } catch (e) {
      obs = { ok: false, outcome: "error", error: { name: e?.name ?? "Error", message: e?.message ?? String(e) } };
    }
    // El log del servidor llega por una tubería y la respuesta del agente por otra: sin esperar, la línea de una denegación puede
    // llegar DESPUÉS de la respuesta del agente que la provocó. Solo se espera en los pasos que miran lo que el servidor anotó.
    if (Object.keys(s.expect ?? {}).some((k) => /^(server|scenario)Violation/.test(k))) await this.#asentarLog();
    const limpia = (l) => l.replace(/^\[\d+\] \S+ \S+ /, "");
    const lineas = (this.ctx.srv?.salida ?? "").slice(logAntes).split("\n").filter(esViolacion).map(limpia);
    const desdeElPrincipio = (this.ctx.srv?.salida ?? "").slice(this.logAlEmpezar).split("\n").filter(esViolacion).map(limpia);
    obs = {
      ...obs,
      wallMs: Date.now() - t0,
      serverViolations: lineas,
      serverViolationCount: lineas.length,
      scenarioViolations: desdeElPrincipio,
      scenarioViolationCount: desdeElPrincipio.length,
    };
    const fallos = evaluar(this.#plantilla(s.expect, marker), obs);
    return { ...base, ok: fallos.length === 0, observado: obs, fallos };
  }

  /** `{marker}` en cualquier cadena del paso. */
  #plantilla(v, marker) {
    if (typeof v === "string") return v.replaceAll("{marker}", marker);
    if (Array.isArray(v)) return v.map((x) => this.#plantilla(x, marker));
    if (v && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, this.#plantilla(x, marker)]));
    return v;
  }

  /** Espera a que el log del servidor deje de crecer durante 100 ms (con un tope de 2 s). */
  async #asentarLog() {
    const srv = this.ctx.srv;
    if (!srv) return;
    const limite = Date.now() + 2000;
    let largo = srv.salida.length;
    let desde = Date.now();
    while (Date.now() < limite && Date.now() - desde < 100) {
      await dormir(25);
      if (srv.salida.length !== largo) {
        largo = srv.salida.length;
        desde = Date.now();
      }
    }
  }

  describir(s) {
    if (s.title) return `${s.agent !== undefined ? `[${s.agent}] ` : ""}${s.title}`;
    return `${s.agent !== undefined ? `[${s.agent}] ` : ""}${s.cmd}${s.operation ? ` ${s.operation}` : ""}`;
  }

  async #orden(s, marker) {
    const args = this.#plantilla(Object.fromEntries(Object.entries(s).filter(([k]) => !["agent", "title", "note", "control", "expect", "until", "waitMs", "knownDeviation", "id"].includes(k))), marker);
    switch (s.cmd) {
      case "sleep":
        await dormir(s.ms ?? 100);
        return { ok: true };
      case "acl": {
        const p = this.ctx.generadas?.find((x) => x.name === s.service);
        if (!p) return { ok: false, error: { message: `el servicio «${s.service}» no está en el manifiesto` } };
        return { ok: true, publish: p.publish, subscribe: p.subscribe, allowResponses: p.allowResponses ?? null };
      }
      case "kill":
        (await this.#agente(s.agent, s, marker)).matar();
        return { ok: true };
      case "broker_subs":
        return this.#suscripcionesEnElBroker(s.agent, marker);
      case "stats":
        if (s.agents !== undefined) {
          const perAgent = {};
          let total = 0;
          for (const id of s.agents) {
            const r = await (await this.#agente(id, s, marker)).orden({ cmd: "stats" });
            perAgent[id] = r;
            total += r.total ?? 0;
          }
          return { ok: true, total, perAgent };
        }
      // falls through: `stats` de UN agente
      default: {
        const a = await this.#agente(s.agent, s, marker);
        if (a.errorDeConexion) return { ok: false, connectError: true, error: a.errorDeConexion, outcome: "connect_error" };
        return await a.orden({ ...args, cmd: s.cmd });
      }
    }
  }

  /**
   * Las suscripciones de la conexión de un agente tal y como las ve el BROKER (`/connz?subs=detail`), y cuántas casan con el
   * `reply` de una petición suya. La librería abre UN buzón único (`_INBOX.<servicio>.<nuid>.*`) para sus peticiones: ese casa y es
   * lo esperado; cualquier OTRA suscripción que case con un `reply` (`_INBOX.<servicio>.>`, `_INBOX.<servicio>.*.*`…) hace que el
   * servidor vea interés y NO envíe `no responders` (A-101).
   */
  async #suscripcionesEnElBroker(id, marker) {
    const a = await this.#agente(id, null, marker);
    const def = this.definiciones.get(id);
    const servicio = this.normalizarActor(def.as).service;
    const nombre = `${servicio}@test`;
    if (!this.ctx.monitor) return { ok: false, error: { message: "el servidor se arrancó sin puerto de monitorización" } };
    const connz = await getJson(`${this.ctx.monitor}/connz?subs=detail&limit=1024`);
    const conexiones = (connz.connections ?? []).filter((c) => c.name === nombre);
    const subjects = conexiones.flatMap((c) => (c.subscriptions_list_detail ?? []).map((x) => x.subject));
    const buzon = subjects.find((x) => new RegExp(`^_INBOX\\.${servicio}\\.[^.]+\\.\\*$`).test(x));
    const reply = buzon ? buzon.replace(/\*$/, "xxxxxxxx") : `_INBOX.${servicio}.nuid.xxxxxxxx`;
    const casan = subjects.filter((x) => casaSubject(x, reply));
    return {
      ok: true,
      connections: conexiones.length,
      subjects,
      mailbox: buzon ?? null,
      sampleReply: reply,
      replyMatches: casan.length,
      otherReplyMatches: casan.filter((x) => x !== buzon),
      pid: a.hijo?.pid,
    };
  }
}
