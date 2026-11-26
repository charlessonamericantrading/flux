/**
 * Los escenarios de `conformance/cases/state-semantics.json` ejecutados con LA API DE NODE
 * (`bus.state.put | delete | get | watch`) contra un nats-server real.
 *
 * Los casos son DATOS: este fichero no sabe qué es «una versión que no avanza» ni qué es un
 * `onSynced` inmediato; solo traduce el vocabulario del caso (`state_put`, `state_watch`…) a
 * llamadas del SDK y compara lo observado con `expect`, exactamente como lo hace el runner de
 * referencia (`conformance/state-broker.mjs`) con su implementación sobre NATS crudo. Si el SDK
 * se desvía de la referencia, el caso falla; y si el caso cambia, esto sigue sin tocarse.
 *
 * Las observaciones `attempts` y `ownWrite` no son parte de la API (`{ applied, outcome,
 * revision }` es todo el resultado): se miden DESDE FUERA, contando las lecturas y las
 * publicaciones que el SDK hace a través de unas conexiones que las cuentan. Y los «ganchos»
 * (`beforePublish`: un rival escribe entre la lectura y la publicación de un intento;
 * `lostAckOnce`: el primer publish se aplica pero quien lo hizo ve un error de secuencia) se
 * inyectan igual, envolviendo el cliente de JetStream que recibe el bus. Ninguno es un asidero
 * de test dentro del SDK de producción.
 *
 * Los casos de cluster necesitan tres nodos (A-49: bajo carrera el conflicto es 10164): se
 * ejecutan en un cluster local de tres nodos.
 */

import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import { connect as natsConnect, type NatsConnection } from "@nats-io/transport-node";
import {
  DiscardPolicy,
  JetStreamApiError,
  StorageType,
  jetstream,
  jetstreamManager,
  type JetStreamClient,
  type JetStreamManager,
} from "@nats-io/jetstream";

import { FluxBus, type StateEntry } from "../../src/index.js";
import { nanos } from "../../src/protocol.js";
import { describeBroker, startBroker, type Broker } from "./broker.js";
import { startCluster3, type Cluster } from "./cluster.js";

async function leerRepo(ruta: string): Promise<string> {
  let dir = new URL("./", import.meta.url);
  for (let i = 0; i < 8; i++) {
    try {
      return await readFile(new URL(ruta, dir), "utf8");
    } catch {
      dir = new URL("../", dir);
    }
  }
  throw new Error(`no se encontró ${ruta} subiendo desde el test`);
}

const semantica = JSON.parse(await leerRepo("conformance/cases/state-semantics.json"));
const dec = new TextDecoder();
const dormir = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

// ─── El lenguaje de expectativas (el mismo de conformance/state-broker.mjs) ──────────────────────

const camino = (o: unknown, ruta: string) =>
  ruta.split(".").reduce<unknown>((x, k) => (x == null ? undefined : (x as Record<string, unknown>)[k]), o);

function cumple(valor: unknown, esperado: unknown): boolean {
  if (esperado !== null && typeof esperado === "object" && !Array.isArray(esperado)) {
    const e = esperado as Record<string, unknown>;
    if ("match" in e) return typeof valor === "string" && new RegExp(e.match as string).test(valor);
    if ("includes" in e) return Array.isArray(valor) ? valor.includes(e.includes) : String(valor).includes(String(e.includes));
    if ("gte" in e) return (valor as number) >= (e.gte as number);
    if ("lte" in e) return (valor as number) <= (e.lte as number);
    if ("not" in e) return !cumple(valor, e.not);
    if ("present" in e) return (valor !== undefined && valor !== null) === e.present;
    if ("oneOf" in e) return (e.oneOf as unknown[]).some((x) => cumple(valor, x));
  }
  return JSON.stringify(valor) === JSON.stringify(esperado);
}

const resuelve = (v: unknown): unknown => {
  if (Array.isArray(v)) return v.map(resuelve);
  if (v && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, resuelve(x)]));
  if (typeof v === "string") return v.replace(/\{repeat:([^:}]+):(\d+)\}/g, (_, s: string, n: string) => s.repeat(Number(n)));
  return v;
};

// ─── Conexiones que MIDEN y a las que se les pueden inyectar los ganchos ─────────────────────────

interface Ganchos {
  /** Publicaciones por JetStream hechas en el paso en curso (1-based en `antesDePublicar`). */
  publicaciones: number;
  /** Lecturas `DIRECT.GET` del paso en curso: una por intento. */
  lecturas: number;
  antesDePublicar?: (n: number) => Promise<void>;
  perderAcuseUnaVez?: boolean;
  /** Se ejecuta justo antes de que `watch` pida el consumidor para consumir: tras crearlo y leer su `num_pending`. */
  antesDeConsumir?: () => Promise<void>;
}

/** Envuelve un objeto: lo que no se sustituye se delega, con `this` en el original (la librería usa campos privados). */
function envuelve<T extends object>(objetivo: T, sustituye: Record<string | symbol, unknown>): T {
  return new Proxy(objetivo, {
    get(t, p) {
      if (p in sustituye) return sustituye[p];
      const v = (t as Record<string | symbol, unknown>)[p];
      return typeof v === "function" ? (v as (...a: unknown[]) => unknown).bind(t) : v;
    },
  });
}

interface Instancia {
  bus: FluxBus;
  nc: NatsConnection;
  g: Ganchos;
}

async function nuevaInstancia(servers: string[], service: string, tenantId?: string): Promise<Instancia> {
  const nc = await natsConnect({ servers, inboxPrefix: `_INBOX.${service}` });
  const js = jetstream(nc);
  const jsm = await jetstreamManager(nc);
  const g: Ganchos = { publicaciones: 0, lecturas: 0 };

  const ncP = envuelve(nc, {
    request: (subject: string, ...a: unknown[]) => {
      if (subject.startsWith("$JS.API.DIRECT.GET.")) g.lecturas++;
      return (nc.request as (...x: unknown[]) => unknown)(subject, ...a);
    },
  });
  const jsP: JetStreamClient = envuelve(js, {
    publish: async (subject: string, data: Uint8Array, opts: unknown) => {
      const n = ++g.publicaciones;
      if (g.antesDePublicar) await g.antesDePublicar(n);
      const ack = await js.publish(subject, data, opts as never);
      if (g.perderAcuseUnaVez && n === 1) {
        // El publish SE APLICÓ, pero quien lo hizo ve el error del REENVÍO de la librería tras una reconexión: el
        // servidor, con la S ya vieja, contesta «wrong last sequence». La forma es la de la librería de verdad.
        throw new JetStreamApiError({ code: 400, err_code: 10071, description: "wrong last sequence: (simulado)" });
      }
      return ack;
    },
    consumers: envuelve(js.consumers, {
      get: async (...a: unknown[]) => {
        if (g.antesDeConsumir) await g.antesDeConsumir();
        return (js.consumers.get as (...x: unknown[]) => unknown)(...a);
      },
    }),
  });

  const bus = new FluxBus(ncP, jsP, jsm, {
    servers,
    service,
    environment: "test",
    version: "1.0.0",
    schemaBaseUrl: "https://schemas.internal",
    ...(tenantId !== undefined && { tenantId }),
  });
  await bus.initValidation();
  return { bus, nc, g };
}

// ─── Ejecutar un caso ────────────────────────────────────────────────────────────────────────────

interface Mundo {
  servers: string[];
  opsNc: NatsConnection;
  jsm: JetStreamManager;
  /** El bus con el que se escribe/lee en los pasos `state_put|delete|get`. */
  principal: Instancia;
  /** Otra instancia del dueño: los rivales de `beforePublish` y de `afterCreate`. */
  rival: Instancia;
  abiertas: Instancia[];
}

const streamDe = (contract: string) => `STATE_${contract.split(".")[0].toUpperCase().replace(/-/g, "_")}`;
const subjectDe = (contract: string, tenant: string, key: string) => {
  const [d, a, v] = contract.split(".");
  return `state.${d}.${a}.${v}.${tenant}.${key}`;
};

async function paso(w: Mundo, st: Record<string, any>): Promise<Record<string, unknown>> {
  const a = resuelve(st) as Record<string, any>;
  switch (a.op) {
    case "stream_add": {
      // La configuración de estado de referencia (10-state.md §2.1) con lo que el caso le cambie.
      try {
        const s = await w.jsm.streams.add({
          name: a.name,
          subjects: a.subjects,
          storage: StorageType.File,
          max_msgs_per_subject: 1,
          allow_direct: true,
          max_age: 0,
          discard: DiscardPolicy.Old,
          duplicate_window: nanos(120_000),
          num_replicas: 1,
          ...(a.config ?? {}),
        } as never);
        return { outcome: "ok", config: s.config };
      } catch (e) {
        return { outcome: "error", errCode: (e as { code?: unknown }).code, text: (e as Error).message };
      }
    }
    case "stream_state": {
      const i = await w.jsm.streams.info(a.name);
      return { messages: i.state.messages, numSubjects: i.state.num_subjects, firstSeq: i.state.first_seq, lastSeq: i.state.last_seq };
    }
    case "state_put":
    case "state_delete":
      return escribe(w, a);
    case "state_get":
      return lee(w, a);
    case "state_watch":
      return vigila(w, a);
    case "state_race":
      return carrera(w, a);
    default:
      throw new Error(`operación del caso sin implementar en este runner: «${a.op}»`);
  }
}

const datoDe = (key: string, version: unknown) => ({ clienteId: key, aggregateVersion: version }) as never;

async function escribe(w: Mundo, a: Record<string, any>): Promise<Record<string, unknown>> {
  const { bus, g } = w.principal;
  g.publicaciones = 0;
  g.lecturas = 0;
  g.perderAcuseUnaVez = !!a.hook?.lostAckOnce;
  g.antesDePublicar = a.hook?.beforePublish
    ? async (n) => {
        // Un rival escribe entre la lectura y la publicación del intento `n`.
        const r = (a.hook.beforePublish as Array<{ attempt: number; fromVersion: number; count?: number }>).find((x) => x.attempt === n);
        if (!r) return;
        for (let v = r.fromVersion; v < r.fromVersion + (r.count ?? 1); v++) {
          await w.rival.bus.state.put(a.contract, a.key, datoDe(a.key, v), { tenantId: a.tenant });
        }
      }
    : undefined;
  try {
    const r =
      a.op === "state_put"
        ? await bus.state.put(a.contract, a.key, datoDe(a.key, a.version), {
            tenantId: a.tenant,
            ...(a.classification !== undefined && { classification: a.classification }),
          })
        : await bus.state.delete(a.contract, a.key, { aggregateVersion: a.version, tenantId: a.tenant });
    return {
      outcome: r.outcome,
      applied: r.applied,
      result: r.outcome,
      revision: r.revision,
      attempts: g.lecturas,
      // el acuse se perdió y el reintento reconoció su propio mensaje: hubo MÁS lecturas que publicaciones
      ownWrite: r.outcome === "ok" && g.lecturas > g.publicaciones,
    };
  } catch (e) {
    return { outcome: "error", errCode: (e as { code?: string }).code, text: (e as Error).message, attempts: g.lecturas };
  } finally {
    g.antesDePublicar = undefined;
    g.perderAcuseUnaVez = false;
  }
}

async function lee(w: Mundo, a: Record<string, any>): Promise<Record<string, unknown>> {
  const e = await w.principal.bus.state.get(a.contract, a.key, { tenantId: a.tenant });
  if (e) return { entry: e, isNull: false, revision: e.revision, aggregateVersion: e.aggregateVersion, deleted: false };
  // `null` es TODO lo que el SDK dice de una clave inexistente y de una lápida. Para distinguirlas (y para
  // comprobar que la lápida lleva la versión que se escribió) se mira el stream SIN el SDK, como la referencia.
  const r = await w.opsNc.request(`$JS.API.DIRECT.GET.${streamDe(a.contract)}.${subjectDe(a.contract, a.tenant, a.key)}`, "", { timeout: 2000 });
  if (r.headers?.code === 404) return { entry: null, isNull: true };
  const ev = JSON.parse(dec.decode(r.data));
  return {
    entry: null,
    isNull: true,
    revision: Number(r.headers!.get("Nats-Sequence")),
    aggregateVersion: ev.data.aggregateVersion,
    deleted: /\.deleted\.v[1-9][0-9]*$/.test(ev.type),
  };
}

async function vigila(w: Mundo, a: Record<string, any>): Promise<Record<string, unknown>> {
  // El durable lo deriva el SDK: <servicio>__state_<dom>_<agr>_v<M>[__<tenant>]. El caso dice cuál DEBE salir.
  const service = (a.service as string | undefined) ?? (a.durable as string).split("__state_")[0];
  const inst = await nuevaInstancia(w.servers, service, a.tenant);
  w.abiertas.push(inst);
  if (a.afterCreate) {
    // «Escrituras durante la sincronización»: con el consumidor ya creado y su `num_pending` ya leído, antes de consumir.
    inst.g.antesDeConsumir = async () => {
      for (const x of a.afterCreate as Array<{ key: string; version: number; tenant?: string }>) {
        await w.rival.bus.state.put(a.contract, x.key, datoDe(x.key, x.version), { tenantId: x.tenant ?? a.tenant ?? "acme" });
      }
    };
  }
  const entradas: StateEntry[] = [];
  let sincronizadoTras: number | undefined;
  const sub = await inst.bus.state.watch(a.contract, async (e) => void entradas.push(e), {
    ...(a.tenant !== undefined && { tenantId: a.tenant }),
    onSynced: () => (sincronizadoTras ??= entradas.length),
  });
  assert.equal(sub.durable, a.durable, "el durable que deriva el SDK no es el que dice el caso");

  // Se espera a la sincronización y a que ya no llegue nada más (un caso que dice `count: 3` debe fallar si llegan 4).
  const limite = Date.now() + 15_000;
  while (sincronizadoTras === undefined && Date.now() < limite) await dormir(25);
  for (let quieto = 0, n = -1; quieto < 600 && Date.now() < limite; await dormir(50)) {
    quieto = entradas.length === n ? quieto + 50 : 0;
    n = entradas.length;
  }
  await inst.bus.close();
  w.abiertas.splice(w.abiertas.indexOf(inst), 1);
  return {
    count: entradas.length,
    keys: entradas.map((e) => e.key),
    entries: entradas,
    syncedImmediately: sincronizadoTras === 0,
    syncedAtIndex: sincronizadoTras === undefined ? undefined : sincronizadoTras - 1,
  };
}

async function carrera(w: Mundo, a: Record<string, any>): Promise<Record<string, unknown>> {
  const res = { ok: 0, stale: 0, conflict: 0, other: 0 };
  const instancias = await Promise.all(Array.from({ length: a.instances }, (_, i) => nuevaInstancia(w.servers, `crm-${i}`)));
  w.abiertas.push(...instancias);
  const escribir = (inst: Instancia, v: number) =>
    inst.bus.state.put(a.contract, a.key, datoDe(a.key, v), { tenantId: a.tenant });
  await Promise.all(
    instancias.map(async (inst, n) => {
      for (let v = a.from; v <= a.to; v++) {
        if (!(v % a.instances === n || Math.random() < 0.3)) continue;
        await dormir(Math.random() * 4);
        try {
          const r = await escribir(inst, v);
          if (r.outcome === "ok") res.ok++;
          else res.stale++;
        } catch (e) {
          if ((e as { code?: string }).code === "STATE_CONFLICT") res.conflict++;
          else res.other++;
        }
      }
    }),
  );
  // un conflicto es RETRYABLE: la aplicación reintenta hasta que no lo haya
  let fin = await instancias[0].bus.state.get(a.contract, a.key, { tenantId: a.tenant });
  for (let i = 0; i < 10 && fin?.aggregateVersion !== a.to; i++) {
    await escribir(instancias[0], a.to).catch(() => {});
    fin = await instancias[0].bus.state.get(a.contract, a.key, { tenantId: a.tenant });
  }
  for (const inst of instancias) {
    await inst.bus.close().catch(() => {});
    w.abiertas.splice(w.abiertas.indexOf(inst), 1);
  }
  return { finalVersion: fin?.aggregateVersion, ...res };
}

async function ejecutaCaso(w: Mundo, c: { id: string; steps: Array<Record<string, any>> }): Promise<{ fallos: string[]; expectativas: number }> {
  // cada caso parte de un broker sin streams
  for (const s of await w.jsm.streams.list().next().then((l) => l.map((x) => x.config.name)).catch(() => [] as string[])) {
    await w.jsm.streams.delete(s).catch(() => {});
  }
  const fallos: string[] = [];
  let expectativas = 0;
  for (const [i, st] of c.steps.entries()) {
    let obs: Record<string, unknown>;
    try {
      obs = await paso(w, st);
    } catch (e) {
      fallos.push(`paso ${i + 1} (${st.op}): ${(e as Error).stack ?? e}`);
      break;
    }
    for (const [ruta, esperado] of Object.entries(st.expect ?? {})) {
      expectativas++;
      const v = camino(obs, ruta);
      if (!cumple(v, resuelve(esperado))) {
        fallos.push(`paso ${i + 1} (${st.title ?? st.op}): «${ruta}» = ${JSON.stringify(v)}, se esperaba ${JSON.stringify(esperado)}  [observado: ${JSON.stringify(obs).slice(0, 300)}]`);
      }
    }
  }
  return { fallos, expectativas };
}

async function abrirMundo(servers: string[]): Promise<Mundo> {
  const opsNc = await natsConnect({ servers });
  const jsm = await jetstreamManager(opsNc);
  const principal = await nuevaInstancia(servers, "crm-api");
  const rival = await nuevaInstancia(servers, "crm-rival");
  return { servers, opsNc, jsm, principal, rival, abiertas: [] };
}

async function cerrarMundo(w: Mundo | undefined): Promise<void> {
  if (!w) return;
  for (const i of w.abiertas.splice(0)) await i.bus.close().catch(() => {});
  await w.principal.bus.close().catch(() => {});
  await w.rival.bus.close().catch(() => {});
  await w.opsNc.close().catch(() => {});
}

// ─── Los casos ───────────────────────────────────────────────────────────────────────────────────

const casos = semantica.cases as Array<{ id: string; title: string; topology?: string; acl?: string; steps: Array<Record<string, any>> }>;

describe("state-semantics.json: los casos están en el fichero y este runner los entiende", () => {
  test("hay casos, y cada paso usa solo operaciones que el runner implementa", () => {
    assert.ok(casos.length >= 10, `solo ${casos.length} casos`);
    const conocidas = new Set(["stream_add", "stream_state", "state_put", "state_delete", "state_get", "state_watch", "state_race"]);
    for (const c of casos) for (const s of c.steps) assert.ok(conocidas.has(s.op), `${c.id}: operación «${s.op}» sin implementar`);
    assert.ok(casos.every((c) => !c.acl), "ningún caso de semántica pide una ACL: los de permisos están en state.test.ts");
  });
});

describeBroker("estado: state-semantics.json con la API de Node — un nodo", () => {
  let broker: Broker;
  let mundo: Mundo;
  before(async () => {
    broker = await startBroker(({ port, store }) => `listen: 127.0.0.1:${port}\njetstream { store_dir: "${store}" }\n`);
    mundo = await abrirMundo([`127.0.0.1:${broker.port}`]);
  });
  after(async () => {
    await cerrarMundo(mundo);
    await broker?.stop();
  });

  for (const c of casos.filter((x) => (x.topology ?? "single") === "single")) {
    test(c.id, { timeout: 60_000 }, async () => {
      const { fallos, expectativas } = await ejecutaCaso(mundo, c);
      assert.deepEqual(fallos, []);
      assert.ok(expectativas > 0, "un caso sin ninguna expectativa comprobada no demuestra nada");
    });
  }
});

describeBroker("estado: state-semantics.json con la API de Node — cluster de tres nodos (A-49: 10164)", () => {
  let cluster: Cluster;
  let mundo: Mundo;
  before(async () => {
    cluster = await startCluster3();
    // La capa de metadatos tarda en elegir líder: hasta entonces ni `$JS.API.INFO` contesta, y un R3 no se acepta.
    const sonda = await natsConnect({ servers: cluster.servers });
    const limite = Date.now() + 30_000;
    try {
      for (;;) {
        try {
          const r = JSON.parse(dec.decode((await sonda.request("$JS.API.INFO", "", { timeout: 1000 })).data));
          if (!r.error) {
            const jsm = await jetstreamManager(sonda);
            await jsm.streams.add({ name: "SONDA", subjects: ["sonda.>"], num_replicas: 3 });
            await jsm.streams.delete("SONDA");
            break;
          }
        } catch (e) {
          if (Date.now() > limite) throw new Error(`el cluster no eligió líder en 30 s: ${(e as Error).message}`);
          await dormir(250);
        }
      }
    } finally {
      await sonda.close();
    }
    mundo = await abrirMundo(cluster.servers);
  });
  after(async () => {
    await cerrarMundo(mundo);
    await cluster?.stop();
  });

  for (const c of casos.filter((x) => x.topology === "cluster3")) {
    test(c.id, { timeout: 90_000 }, async () => {
      const { fallos, expectativas } = await ejecutaCaso(mundo, c);
      assert.deepEqual(fallos, []);
      assert.ok(expectativas > 0);
    });
  }

  test("CONTROL: bajo carrera el broker de verdad contesta el 10164 y el SDK lo reintenta (no es un 10071)", { timeout: 60_000 }, async () => {
    // El caso `cluster-three-owner-instances-converge` pasa si el SDK reconoce el 10164... pero también podría pasar si
    // nunca hubiera conflicto. Aquí se mide: 8 escritores con la MISMA S, y se cuenta qué códigos devuelve el broker.
    await mundo.jsm.streams.add({ name: "STATE_CARRERA", subjects: ["state.carrera.>"], storage: StorageType.File, max_msgs_per_subject: 1, allow_direct: true, discard: DiscardPolicy.Old, num_replicas: 3 });
    const instancias = await Promise.all(Array.from({ length: 8 }, (_, i) => nuevaInstancia(cluster.servers, `crm-c${i}`)));
    try {
      const codigos = new Set<number>();
      const js = jetstream(mundo.opsNc);
      const { headers: nh } = await import("@nats-io/transport-node");
      const subject = "state.carrera.cosa.v1.acme.k";
      for (let ronda = 0; ronda < 15; ronda++) {
        let S = 0;
        const g = await mundo.opsNc.request(`$JS.API.DIRECT.GET.STATE_CARRERA.${subject}`, "", { timeout: 2000 });
        if (g.headers?.code !== 404) S = Number(g.headers!.get("Nats-Sequence"));
        const r = await Promise.allSettled(
          Array.from({ length: 8 }, (_, i) => {
            const h = nh();
            h.set("Nats-Expected-Last-Subject-Sequence", String(S));
            return js.publish(subject, new TextEncoder().encode(`{"r":${ronda},"i":${i}}`), { headers: h });
          }),
        );
        for (const x of r) if (x.status === "rejected") codigos.add((x.reason as { code: number }).code);
      }
      assert.ok(codigos.size > 0, "no hubo ningún conflicto: la carga no mide nada");
      assert.ok([...codigos].every((c) => c === 10071 || c === 10164), `códigos de conflicto inesperados: ${[...codigos]}`);
      // y el SDK, con esos mismos códigos, converge: 3 instancias × 20 versiones sobre UNA clave
      const total = { ok: 0, stale: 0, otro: 0 };
      await Promise.all(
        instancias.slice(0, 3).map(async (inst, n) => {
          for (let v = 1; v <= 20; v++) {
            if (v % 3 !== n) continue;
            for (let t = 0; t < 10; t++) {
              try {
                const r = await inst.bus.state.put("carrera.cosa.v1", "k2", datoDe("k2", v), { tenantId: "acme" });
                total[r.outcome]++;
                break;
              } catch (e) {
                if ((e as { code?: string }).code !== "STATE_CONFLICT") {
                  total.otro++;
                  break;
                }
              }
            }
          }
        }),
      );
      assert.equal(total.otro, 0);
      assert.equal((await instancias[0].bus.state.get("carrera.cosa.v1", "k2", { tenantId: "acme" }))!.aggregateVersion, 20);
    } finally {
      for (const i of instancias) await i.bus.close().catch(() => {});
    }
  });
});
