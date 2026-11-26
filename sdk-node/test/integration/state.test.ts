/**
 * Estado replicado contra un nats-server REAL, con los permisos EXACTOS de `state.acl`.
 *
 * Es el ciclo del criterio de aceptación de la Fase 2 (PLAN §2.7, 10-state.md §10.6):
 *
 *   el dueño escribe 100 claves → un observador NUEVO las recibe todas y `onSynced` →
 *   se borra una → el observador recibe la lápida → `get` de esa clave da `null`
 *
 * y, **en los dos sentidos**, que quitar un permiso SE NOTA: cada permiso de la lista se retira
 * de uno en uno y el SDK falla de forma visible y con el error que toca (PERMISSION_DENIED,
 * NOT_PROVISIONED…) o —en los dos casos en que el broker calla— con el aviso que el SDK
 * promete. Los permisos se escriben aquí a mano (ver state-acl.ts); no los genera `gen-acl.mjs`.
 *
 * Sin `nats-server` estos tests se SALTAN con aviso, salvo con FLUX_REQUIRE_BROKER=1, donde
 * FALLAN (broker.ts).
 */

import { test, describe, before, after, afterEach } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import { connect as natsConnect, type NatsConnection } from "@nats-io/transport-node";
import {
  AckPolicy,
  DeliverPolicy,
  DiscardPolicy,
  ReplayPolicy,
  StorageType,
  jetstream,
  type JetStreamManager,
} from "@nats-io/jetstream";

import {
  ConsumerConfigMismatchError,
  InMemoryMetrics,
  NotProvisionedError,
  PermanentError,
  PermissionDeniedError,
  StateSubjectMismatchError,
  TenantIsolationError,
  connect,
  type FluxBus,
  type MetricsSink,
  type StateEntry,
} from "../../src/index.js";
import { CONSUMER_DEFAULTS, STREAM_DEFAULTS, nanos } from "../../src/protocol.js";
import {
  durableDeDlqDeEstado,
  durableDeEstado,
  permisosDeEstado,
  usuarioDeEstado,
  type Permisos,
  type ServicioEstado,
} from "./state-acl.js";
import { conectarOps, describeBroker, esperarHasta, startBroker, type Broker } from "./broker.js";

// ═════════════════════════════════════════════════════════════════════════════
// Los permisos del test, anclados a protocol.json
// ═════════════════════════════════════════════════════════════════════════════

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
const protocolo = JSON.parse(await leerRepo("protocol.json"));
const bundle = JSON.parse(await leerRepo("schemas/bundle.json"));

const C = "clientes.cliente.v1";
const STREAM = "STATE_CLIENTES";

const SERVICIOS: Record<string, ServicioEstado> = {
  // El DUEÑO del dominio `clientes`: escribe, lee lo suyo y lee la DLQ de estado de su dominio.
  "crm-api": { service: "crm-api", owns: ["clientes"], readsDlq: ["clientes"] },
  // Observadores. El tenant va FIJADO en el permiso de lectura directa y en el nombre del durable.
  "tienda-web": { service: "tienda-web", observes: [{ contract: C, tenant: "acme" }] },
  "tienda-globex": { service: "tienda-globex", observes: [{ contract: C, tenant: "globex" }] },
  "tienda-todos": { service: "tienda-todos", observes: [{ contract: C }] },
  // El observador NUEVO del ciclo de aceptación: nadie lo había visto antes.
  "tienda-nueva": { service: "tienda-nueva", observes: [{ contract: C, tenant: "acme" }] },
  // Con CONSUMER.CREATE: solo en desarrollo (`gen-acl.mjs --dev`).
  "tienda-dev": { service: "tienda-dev", observes: [{ contract: C, tenant: "acme", crear: true }] },
  // Un durable con la config mal aprovisionada.
  "tienda-mal": { service: "tienda-mal", observes: [{ contract: C, tenant: "acme" }] },
  // Dos tenants del mismo servicio: dos durables, UNA serie de métricas.
  "tienda-multi": {
    service: "tienda-multi",
    observes: [{ contract: C, tenant: "acme" }, { contract: C, tenant: "globex" }],
  },
  // Dueño de dominios sin stream de estado, o con él pero sin allow_direct.
  "pagos-api": { service: "pagos-api", owns: ["pagos"] },
  "sd-api": { service: "sd-api", owns: ["sindirecto"] },
  // Observador de un contrato cuyo stream no existe (pero su DLQ sí).
  "tienda-pagos": { service: "tienda-pagos", observes: [{ contract: "pagos.pago.v1", tenant: "acme" }] },
  // Dueño de OTRO dominio: no puede escribir `clientes` aunque su código lo intente.
  "ajeno-api": { service: "ajeno-api", owns: ["otros"] },
};

/**
 * Para la matriz «cada permiso es NECESARIO»: un servicio por permiso, con ese permiso quitado.
 * Se salta `$JS.API.INFO` y el inbox: son de `connect()`, y los prueba acl.test.ts.
 */
interface Variante { service: string; quitado: string; perms: Permisos; rol: "observador" | "dueño" }
function variantes(rol: Variante["rol"], prefijo: string, definicion: (service: string) => ServicioEstado): Variante[] {
  const sinInfo = (service: string) => permisosDeEstado(definicion(service)).publish.filter((p) => p !== "$JS.API.INFO");
  return sinInfo(`${prefijo}0`).map((_, i) => {
    const service = `${prefijo}${i}`;
    const base = permisosDeEstado(definicion(service));
    const quitado = sinInfo(service)[i];
    return { service, quitado, rol, perms: { publish: base.publish.filter((p) => p !== quitado), subscribe: base.subscribe } };
  });
}
const VARIANTES: Variante[] = [
  ...variantes("observador", "tw-v", (service) => ({ service, observes: [{ contract: C, tenant: "acme" }] })),
  ...variantes("dueño", "cr-v", (service) => ({ service, owns: ["clientes"] })),
];

describe("los permisos del test de estado encajan en protocol.json (state.acl + security.acl)", () => {
  const re = (s: string) => new RegExp(s);
  const st = protocolo.state.acl;
  const sec = protocolo.security.acl;
  const PATRONES = [
    re(sec.permissionPatterns.accountInfo),
    re(sec.permissionPatterns.streamInfo),
    re(sec.permissionPatterns.consumerInfo), // los durables de la DLQ del dueño (DLQ_<DOM>)
    re(sec.permissionPatterns.consumerNext),
    re(sec.permissionPatterns.ack),
    re(st.ownerPublish),
    re(st.ownerDirectGet),
    re(st.observerDirectGet),
    re(st.consumerInfo),
    re(st.consumerNext),
    re(st.ack),
    re(st.consumerCreate),
    re(st.dlqPublish),
  ];
  const nunca = re(sec.neverGrantedPattern);
  // La ÚNICA excepción a «DIRECT.GET nunca»: la forma por SUBJECT de un estado, con el tenant fijado.
  const excepcion = [re(st.ownerDirectGet), re(st.observerDirectGet)];

  const todos = Object.values(SERVICIOS).flatMap((s) => permisosDeEstado(s).publish);

  test("cada permiso de publicación que se concede encaja en un patrón de protocol.json", () => {
    for (const p of new Set(todos)) {
      assert.ok(PATRONES.some((r) => r.test(p)), `«${p}» no encaja en ningún patrón de state.acl ni de security.acl`);
    }
  });

  test("ninguno cae en la lista de «nunca», salvo la forma por subject de DIRECT.GET del estado", () => {
    for (const p of new Set(todos)) {
      if (nunca.test(p)) assert.ok(excepcion.some((r) => r.test(p)), `«${p}» está en neverGranted y no es la excepción de estado`);
    }
    // CONTROL: la forma de CUERPO de DIRECT.GET, y un comodín por encima del tenant, NO son la excepción
    assert.ok(nunca.test("$JS.API.DIRECT.GET.STATE_CLIENTES"));
    assert.ok(!excepcion.some((r) => r.test("$JS.API.DIRECT.GET.STATE_CLIENTES")));
    assert.ok(!re(st.observerDirectGet).test("$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.>"));
    assert.ok(!re(st.observerDirectGet).test("$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.>"));
  });

  test("ningún observador recibe STREAM.INFO.STATE_<DOM> (lista todas las claves y todos los tenants: A-39)", () => {
    for (const s of Object.values(SERVICIOS)) {
      assert.ok(!permisosDeEstado(s).publish.some((p) => p.startsWith("$JS.API.STREAM.INFO.STATE_")), s.service);
    }
    assert.equal(st.observerStreamInfoState, "never");
  });

  test("ningún servicio recibe permiso de suscripción sobre subjects de dominio, ni un inbox común", () => {
    for (const s of Object.values(SERVICIOS)) {
      assert.deepEqual(permisosDeEstado(s).subscribe, [`_INBOX.${s.service}.>`]);
    }
  });

  test("CONSUMER.CREATE solo con `crear` (desarrollo), por durable y con el filtro EXACTO", () => {
    for (const s of Object.values(SERVICIOS)) {
      const conCrear = permisosDeEstado(s).publish.filter((p) => p.includes("CONSUMER.CREATE"));
      assert.equal(conCrear.length, s.observes?.some((o) => o.crear) ? 1 : 0, s.service);
    }
    const dev = permisosDeEstado(SERVICIOS["tienda-dev"]).publish.find((p) => p.includes("CONSUMER.CREATE"))!;
    assert.equal(dev, `$JS.API.CONSUMER.CREATE.${STREAM}.${durableDeEstado("tienda-dev", C, "acme")}.state.${C}.acme.*`);
  });

  test("los durables del test son los que dicen protocol.json → state.watch.durable y la DLQ del dueño", () => {
    assert.match(durableDeEstado("tienda-web", C, "acme"), re(protocolo.state.watch.durable.pattern));
    assert.match(durableDeEstado("tienda-web", C), re(protocolo.state.watch.durable.pattern));
    assert.match(durableDeDlqDeEstado("crm-api", "clientes"), re(protocolo.state.dlq.readerDurable.pattern));
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// El broker
// ═════════════════════════════════════════════════════════════════════════════

function configDelBroker(port: number, store: string): string {
  const usuarios = [
    `{ user: "ops", password: "pw" }`, // IaC: aprovisiona. Sin restricciones.
    ...Object.values(SERVICIOS).map((s) => usuarioDeEstado(s.service, permisosDeEstado(s))),
    ...VARIANTES.map((v) => usuarioDeEstado(v.service, v.perms)),
  ];
  return `listen: 127.0.0.1:${port}
jetstream { store_dir: "${store}" }
accounts {
  PROD: {
    jetstream: enabled
    users: [
      ${usuarios.join(",\n      ")}
    ]
  }
}
`;
}

const dec = new TextDecoder();
const enc = new TextEncoder();

describeBroker("SDK Node: estado replicado bajo ACL exacta", () => {
  let broker: Broker;
  let opsNc: NatsConnection;
  let jsm: JetStreamManager;
  const abiertos: FluxBus[] = [];

  const base = (service: string, extra: Record<string, unknown> = {}) => ({
    servers: broker.url,
    service,
    environment: "test",
    version: "1.0.0",
    schemaBaseUrl: "https://schemas.internal",
    credentials: { user: service, pass: "pw" },
    ...extra,
  });
  const logs = () => {
    const l: string[] = [];
    return { l, logger: { warn: (m: string) => void l.push(m), error: (m: string) => void l.push(m) } };
  };
  async function abrir(service: string, extra: Record<string, unknown> = {}): Promise<FluxBus> {
    const bus = await connect(base(service, extra));
    abiertos.push(bus);
    return bus;
  }

  /** Lo que `flux provision` hará: el durable del observador con la config canónica y `last_per_subject`. */
  async function provisionar(service: string, tenant?: string, extra: Record<string, unknown> = {}): Promise<string> {
    const durable = durableDeEstado(service, C, tenant);
    await jsm.consumers.delete(STREAM, durable).catch(() => {});
    await jsm.consumers.add(STREAM, {
      durable_name: durable,
      filter_subject: `state.${C}.${tenant ?? "*"}.*`,
      deliver_policy: DeliverPolicy.LastPerSubject,
      ack_policy: AckPolicy.Explicit,
      ack_wait: nanos(CONSUMER_DEFAULTS.ackWaitMs),
      max_deliver: CONSUMER_DEFAULTS.maxDeliver,
      backoff: CONSUMER_DEFAULTS.backoffMs.map(nanos),
      max_ack_pending: CONSUMER_DEFAULTS.maxAckPending,
      replay_policy: ReplayPolicy.Instant,
      ...extra,
    });
    return durable;
  }

  const dato = (key: string, v = 1, extra: Record<string, unknown> = {}) => ({
    clienteId: key,
    nombre: "Ana Secreta",
    email: "ana.secreta@example.com",
    aggregateVersion: v,
    ...extra,
  });

  /** Lo que el broker tiene de verdad en el stream, sin pasar por el SDK. */
  async function enElStream(subject: string): Promise<Record<string, any> | null> {
    const m = await jsm.streams.getMessage(STREAM, { last_by_subj: subject }).catch(() => null);
    return m ? JSON.parse(dec.decode(m.data)) : null;
  }

  async function limpiar(): Promise<void> {
    for (const s of [STREAM, "DLQ_CLIENTES"]) await jsm.streams.purge(s).catch(() => {});
    const durables = await jsm.consumers.list(STREAM).next().then((l) => l.map((c) => c.name)).catch(() => [] as string[]);
    for (const d of durables) await jsm.consumers.delete(STREAM, d).catch(() => {});
  }

  /** Un mensaje que escribe `ops` por JetStream, saltándose al SDK: para fabricar lo que un SDK conforme no emite. */
  async function escribeCrudo(subject: string, cuerpo: string | object): Promise<number> {
    const a = await jetstream(opsNc).publish(subject, enc.encode(typeof cuerpo === "string" ? cuerpo : JSON.stringify(cuerpo)));
    return a.seq;
  }

  before(async () => {
    broker = await startBroker(({ port, store }) => configDelBroker(port, store));
    ({ nc: opsNc, jsm } = await conectarOps(broker));
    // Lo que `flux provision` hará para un dominio con estado (10-state.md §2.1, §6)
    await jsm.streams.add({
      name: STREAM,
      subjects: ["state.clientes.>"],
      storage: StorageType.File,
      max_msgs_per_subject: 1,
      allow_direct: true,
      max_age: 0,
      discard: DiscardPolicy.Old,
      duplicate_window: nanos(STREAM_DEFAULTS.duplicateWindowMs),
      num_replicas: 1,
    });
    await jsm.streams.add({
      name: "DLQ_CLIENTES",
      subjects: ["dlq.clientes.>", "dlq.state.clientes.>"],
      max_age: nanos(STREAM_DEFAULTS.dlqMaxAgeMs),
    });
    // Un dominio con DLQ pero SIN stream de estado; y uno con el stream SIN allow_direct
    await jsm.streams.add({ name: "DLQ_PAGOS", subjects: ["dlq.pagos.>", "dlq.state.pagos.>"] });
    await jsm.streams.add({
      name: "STATE_SINDIRECTO",
      subjects: ["state.sindirecto.>"],
      max_msgs_per_subject: 1,
      allow_direct: false,
      discard: DiscardPolicy.Old,
    });
  });

  afterEach(async () => {
    for (const b of abiertos.splice(0)) await b.close().catch(() => {});
  });

  after(async () => {
    await opsNc?.close().catch(() => {});
    await broker?.stop();
  });

  // ══════════════════════════════════════════════════════════════════════════
  // El ciclo de aceptación
  // ══════════════════════════════════════════════════════════════════════════

  test(
    "CICLO: el dueño escribe 100 claves → un observador NUEVO las recibe todas y `onSynced` → se borra una → recibe la lápida → get da null",
    { timeout: 90_000 },
    async () => {
      await limpiar();
      const dueno = await abrir("crm-api", { tenantId: "acme" });
      const llaves = Array.from({ length: 100 }, (_, i) => `cli-${String(i + 1).padStart(3, "0")}`);

      // 1. el dueño escribe 100 claves (de diez en diez)
      for (let i = 0; i < llaves.length; i += 10) {
        const r = await Promise.all(llaves.slice(i, i + 10).map((k) => dueno.state.put(C, k, dato(k), { classification: "confidential" })));
        for (const x of r) assert.equal(x.outcome, "ok");
      }
      // y una del OTRO tenant con el mismo nombre de clave: no se pisan, y no debe llegar al observador de acme
      await dueno.state.put(C, "cli-001", dato("cli-001", 5), { tenantId: "globex" });
      assert.equal((await dueno.state.get(C, "cli-001"))!.aggregateVersion, 1);
      assert.equal((await dueno.state.get(C, "cli-001", { tenantId: "globex" }))!.aggregateVersion, 5);

      // 2. un observador NUEVO (nadie lo había visto): recibe la foto completa y `onSynced`
      await provisionar("tienda-nueva", "acme");
      const { l: avisos, logger } = logs();
      const nueva = await abrir("tienda-nueva", { tenantId: "acme", logger });
      const recibidas: Array<StateEntry> = [];
      let sincronizadoTras = -1;
      let sincronizaciones = 0;
      const sub = await nueva.state.watch(C, async (e) => void recibidas.push(e), {
        onSynced: () => {
          sincronizaciones++;
          sincronizadoTras = recibidas.length;
        },
      });
      assert.equal(sub.durable, durableDeEstado("tienda-nueva", C, "acme"));
      assert.ok(await esperarHasta(() => recibidas.length >= 100 && sincronizaciones >= 1, 20_000), `recibió ${recibidas.length}`);
      await new Promise((r) => setTimeout(r, 300)); // por si llegara algo de más

      assert.equal(recibidas.length, 100, "ni una menos ni la de globex");
      assert.deepEqual(new Set(recibidas.map((e) => e.key)), new Set(llaves));
      for (const e of recibidas) {
        assert.equal(e.tenantId, "acme");
        assert.equal(e.deleted, false);
        assert.equal(e.aggregateVersion, 1);
        assert.equal((e.data as { clienteId: string }).clienteId, e.key);
        assert.ok(e.revision > 0);
      }
      assert.equal(sincronizaciones, 1, "onSynced se invoca UNA vez");
      assert.equal(sincronizadoTras, 100, "…y cuando el handler ya procesó TODA la foto");

      // 3. se borra una: el observador (ya en vivo) recibe la lápida
      const antes = recibidas.length;
      assert.equal((await dueno.state.delete(C, "cli-042", { aggregateVersion: 2 })).outcome, "ok");
      assert.ok(await esperarHasta(() => recibidas.length > antes, 10_000), "no llegó la lápida");
      const lapida = recibidas.at(-1)!;
      assert.equal(lapida.key, "cli-042");
      assert.equal(lapida.deleted, true);
      assert.equal(lapida.data, null, "la lápida no lleva dato");
      assert.equal(lapida.aggregateVersion, 2);
      assert.equal(lapida.tenantId, "acme");
      assert.equal(sincronizaciones, 1, "ni una segunda vez");

      // 4. get de esa clave da null (igual que si nunca hubiera existido); las demás siguen ahí
      assert.equal(await nueva.state.get(C, "cli-042"), null);
      assert.equal(await dueno.state.get(C, "cli-042"), null);
      assert.equal((await nueva.state.get(C, "cli-043"))!.aggregateVersion, 1);
      assert.equal(await nueva.state.get(C, "no-existe"), null);

      // 5. y la lápida SUSTITUYÓ al valor: el dato personal ya no está en el stream
      const guardado = await enElStream("state.clientes.cliente.v1.acme.cli-042");
      assert.equal(guardado!.type, "com.flux.state.clientes.cliente.deleted.v1");
      assert.ok(!JSON.stringify(guardado).includes("ana.secreta@example.com"));
      assert.equal((await jsm.streams.info(STREAM)).state.messages, 101, "100 claves de acme (una ya es lápida) + 1 de globex: un mensaje por clave");

      // 6. un observador que llega DESPUÉS recibe la lápida en su foto (A-35)
      const dev = await abrir("tienda-dev", { tenantId: "acme", logger });
      const foto: StateEntry[] = [];
      let sync2 = 0;
      await dev.state.watch(C, async (e) => void foto.push(e), { onSynced: () => sync2++ }); // CREA su durable: tiene CONSUMER.CREATE
      assert.ok(await esperarHasta(() => foto.length >= 100 && sync2 >= 1, 20_000), `la foto tardía tiene ${foto.length}`);
      assert.equal(foto.length, 100);
      assert.equal(foto.filter((e) => e.deleted).length, 1);
      assert.equal(foto.find((e) => e.deleted)!.key, "cli-042");
      assert.deepEqual(avisos, [], "ningún aviso del SDK en todo el ciclo");
    },
  );

  test("el observador de OTRO tenant solo ve el suyo (el filtro y el durable llevan el tenant)", async () => {
    await limpiar();
    const dueno = await abrir("crm-api");
    await dueno.state.put(C, "cli-1", dato("cli-1", 1), { tenantId: "acme" });
    await dueno.state.put(C, "cli-1", dato("cli-1", 7), { tenantId: "globex" });
    await provisionar("tienda-globex", "globex");
    const g = await abrir("tienda-globex", { tenantId: "globex" });
    const vistas: StateEntry[] = [];
    await g.state.watch(C, async (e) => void vistas.push(e));
    assert.ok(await esperarHasta(() => vistas.length >= 1, 10_000));
    await new Promise((r) => setTimeout(r, 200));
    assert.deepEqual(vistas.map((e) => [e.tenantId, e.key, e.aggregateVersion]), [["globex", "cli-1", 7]]);
  });

  test("un watch SIN tenant recibe `cli-1` de los dos tenants, y la entrada dice de cuál", async () => {
    await limpiar();
    const dueno = await abrir("crm-api");
    await dueno.state.put(C, "cli-1", dato("cli-1", 1), { tenantId: "acme" });
    await dueno.state.put(C, "cli-1", dato("cli-1", 7), { tenantId: "globex" });
    await provisionar("tienda-todos");
    const t = await abrir("tienda-todos");
    const vistas: StateEntry[] = [];
    await t.state.watch(C, async (e) => void vistas.push(e));
    assert.ok(await esperarHasta(() => vistas.length >= 2, 10_000));
    assert.deepEqual(
      vistas.map((e) => `${e.tenantId}/${e.key}@${e.aggregateVersion}`).sort(),
      ["acme/cli-1@1", "globex/cli-1@7"],
      "sin el tenantId de la entrada, una copia local por clave mezclaría los tenants en silencio",
    );
    // con `tenantIsolation: "strict"`, ese mismo watch es un error de configuración
    const estricto = await abrir("tienda-todos", { tenantIsolation: "strict" });
    await assert.rejects(estricto.state.watch(C, () => {}), TenantIsolationError);
  });

  test("un servicio con un watch por tenant: `flux_consumer_pending` es la SUMA de sus durables, con el consumer SIN el sufijo del tenant", async () => {
    await limpiar();
    const dueno = await abrir("crm-api");
    for (const k of ["a1", "a2", "a3"]) await dueno.state.put(C, k, dato(k), { tenantId: "acme" });
    for (const k of ["g1", "g2", "g3", "g4"]) await dueno.state.put(C, k, dato(k), { tenantId: "globex" });
    await provisionar("tienda-multi", "acme");
    await provisionar("tienda-multi", "globex");

    const llamadas: Array<[string, string, number]> = [];
    const sink: MetricsSink = {
      eventPublished() {},
      eventConsumed() {},
      handlerDuration() {},
      eventDlq() {},
      eventRetried() {},
      connectionState() {},
      consumerPending: (subject, consumer, pending) => void llamadas.push([subject, consumer, pending]),
    };
    const m = await abrir("tienda-multi", { metrics: sink, pendingPollMs: -1 });
    // El handler de acme se BLOQUEA en su primera entrada: su `pending` queda en 2 (las 2 que faltaban por entregar)
    // mientras el de globex drena los suyos (3, 2, 1, 0). Sin sumar, el segundo pisaría al primero.
    let abrirPuerta!: () => void;
    const puerta = new Promise<void>((r) => (abrirPuerta = r));
    const deGlobex: string[] = [];
    try {
      await m.state.watch(C, async () => void (await puerta), { tenantId: "acme" });
      await m.state.watch(C, async (e) => void deGlobex.push(e.key), { tenantId: "globex" });
      assert.ok(await esperarHasta(() => deGlobex.length === 4, 10_000));
      const valores = llamadas.map((l) => l[2]);
      assert.ok(valores.includes(5), `acme (2, bloqueado) + globex (3) = 5; se publicó ${valores.join(", ")}`);
      assert.ok(llamadas.every((l) => l[0] === "state.clientes.cliente.v1" && l[1] === "tienda-multi__state_clientes_cliente_v1"), "subject=state.<contrato>; consumer sin el sufijo del tenant");
      abrirPuerta();
      assert.ok(await esperarHasta(() => llamadas.at(-1)![2] === 0, 10_000), `al final no queda nada pendiente: ${valores.join(", ")}`);
    } finally {
      abrirPuerta(); // un handler bloqueado impediría cerrar el bus si una aserción falla
    }
  });

  test("`onSynced` con el contrato VACÍO se invoca de inmediato, antes de ninguna entrada; y una sola vez", async () => {
    await limpiar();
    await provisionar("tienda-web", "acme");
    const w = await abrir("tienda-web", { tenantId: "acme" });
    const orden: string[] = [];
    await w.state.watch(C, async (e) => void orden.push(`entrada:${e.key}`), { onSynced: () => void orden.push("synced") });
    assert.deepEqual(orden, ["synced"], "de inmediato: ni espera una entrada que quizá no llegue nunca");
    const dueno = await abrir("crm-api", { tenantId: "acme" });
    await dueno.state.put(C, "cli-1", dato("cli-1"));
    assert.ok(await esperarHasta(() => orden.length >= 2, 10_000));
    assert.deepEqual(orden, ["synced", "entrada:cli-1"], "y no se vuelve a invocar con las entradas en vivo");
  });

  // ══════════════════════════════════════════════════════════════════════════
  // Aislamiento por tenant y escritura solo del dueño: lo hace el BROKER
  // ══════════════════════════════════════════════════════════════════════════

  test("el broker confina por tenant: acme no lee globex (get ni watch) y un observador no escribe", async () => {
    await limpiar();
    const dueno = await abrir("crm-api");
    await dueno.state.put(C, "cli-1", dato("cli-1", 1), { tenantId: "globex" });
    await provisionar("tienda-web", "acme");
    const w = await abrir("tienda-web", { tenantId: "acme" });

    // get de otro tenant: la forma por SUBJECT con el tenant fijado en el permiso lo deniega
    await assert.rejects(w.state.get(C, "cli-1", { tenantId: "globex" }), (e: PermissionDeniedError) => {
      assert.ok(e instanceof PermissionDeniedError);
      assert.equal(e.code, "PERMISSION_DENIED");
      assert.equal(e.subject, "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.globex.cli-1");
      return true;
    });
    // watch del durable de otro tenant: CONSUMER.INFO de ese durable no está concedido
    await assert.rejects(w.state.watch(C, () => {}, { tenantId: "globex" }), (e: PermissionDeniedError) => {
      assert.ok(e instanceof PermissionDeniedError);
      assert.equal(e.subject, "$JS.API.CONSUMER.INFO.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__globex");
      return true;
    });
    // un observador NO escribe: el broker lo deniega, no el código
    await assert.rejects(w.state.put(C, "cli-9", dato("cli-9"), { tenantId: "acme" }), (e: PermissionDeniedError) => {
      assert.ok(e instanceof PermissionDeniedError);
      assert.equal(e.subject, "state.clientes.cliente.v1.acme.cli-9");
      return true;
    });
    assert.equal(await enElStream("state.clientes.cliente.v1.acme.cli-9"), null);
    // y el dueño de OTRO dominio tampoco escribe `clientes`
    const ajeno = await abrir("ajeno-api", { tenantId: "acme" });
    await assert.rejects(ajeno.state.put(C, "cli-9", dato("cli-9")), PermissionDeniedError);
    await assert.rejects(ajeno.state.get(C, "cli-1"), PermissionDeniedError);
    // CONTROL: el mismo observador SÍ lee lo suyo
    await dueno.state.put(C, "cli-1", dato("cli-1", 1), { tenantId: "acme" });
    assert.equal((await w.state.get(C, "cli-1"))!.tenantId, "acme");
  });

  test("la validación previa a la red, medida CONTRA un broker que denegaría la petición: gana el error de validación", async () => {
    // `tienda-web` es un observador: si una escritura llegara al broker, éste la denegaría (PERMISSION_DENIED).
    // Que lo que se lance sea el error de VALIDACIÓN prueba que no se abrió ninguna petición.
    const w = await abrir("tienda-web", { tenantId: "acme" });
    const codigo = (c: string) => (e: { code?: string }) => e.code === c;
    await assert.rejects(w.state.put(C, "a.b", dato("x")), codigo("INVALID_STATE_KEY"));
    await assert.rejects(w.state.put(C, "ok", { clienteId: "ok" } as never), codigo("MISSING_AGGREGATE_VERSION"));
    await assert.rejects(w.state.delete(C, "ok", { aggregateVersion: 0 }), codigo("MISSING_AGGREGATE_VERSION"));
    await assert.rejects(w.state.put(C, "ok", dato("ok"), { tenantId: "ACME" }), codigo("INVALID_TENANT_FOR_STATE"));
    await assert.rejects(w.state.put("state.clientes.v1", "ok", dato("ok")), codigo("RESERVED_DOMAIN"));
    await assert.rejects(w.state.get("dlq.cliente.v1", "ok"), codigo("RESERVED_DOMAIN"));
    await assert.rejects(w.publish("state.clientes.v1.creado", {}), codigo("RESERVED_DOMAIN"));
    await assert.rejects(w.publish("system.cosa.v1.hecha", {}), codigo("RESERVED_DOMAIN"));
    // CONTROL (el otro sentido): con todo válido, la MISMA escritura sí llega al broker, y éste la deniega
    await assert.rejects(w.state.put(C, "ok", dato("ok")), PermissionDeniedError);
  });

  test("CONTROL del permiso: el observador NO tiene STREAM.INFO.STATE_CLIENTES (lista claves y tenants) y todo funciona igual", async () => {
    await limpiar();
    const dueno = await abrir("crm-api");
    await dueno.state.put(C, "cli-1", dato("cli-1"), { tenantId: "globex" });
    // el broker lo deniega...
    const crudo = await natsConnect({ servers: broker.url, user: "tienda-web", pass: "pw", inboxPrefix: "_INBOX.tienda-web" });
    try {
      await assert.rejects(
        crudo.request(`$JS.API.STREAM.INFO.${STREAM}`, JSON.stringify({ subjects_filter: ">" }), { timeout: 1500 }),
        (e: { cause?: { name?: string } }) => e.cause?.name === "PermissionViolationError",
      );
      // ...y la forma de CUERPO de DIRECT.GET, que lee cualquier mensaje, tampoco
      await assert.rejects(
        crudo.request(`$JS.API.DIRECT.GET.${STREAM}`, JSON.stringify({ seq: 1 }), { timeout: 1500 }),
        (e: { cause?: { name?: string } }) => e.cause?.name === "PermissionViolationError",
      );
    } finally {
      await crudo.close();
    }
    // el SDK no lo necesita: put (dueño sin STREAM.INFO), get y watch (observador sin él) funcionan
    await provisionar("tienda-web", "acme");
    const w = await abrir("tienda-web", { tenantId: "acme" });
    await dueno.state.put(C, "cli-2", dato("cli-2"), { tenantId: "acme" });
    assert.equal((await w.state.get(C, "cli-2"))!.key, "cli-2");
    const vistas: StateEntry[] = [];
    await w.state.watch(C, async (e) => void vistas.push(e));
    assert.ok(await esperarHasta(() => vistas.length >= 1, 10_000));
  });

  // ══════════════════════════════════════════════════════════════════════════
  // Quitar un permiso SE NOTA — matriz de permisos
  // ══════════════════════════════════════════════════════════════════════════

  for (const v of VARIANTES.filter((x) => x.rol === "dueño")) {
    test(`dueño sin \`${v.quitado}\`: put falla de forma visible`, async () => {
      await limpiar();
      const bus = await abrir(v.service, { tenantId: "acme" });
      await assert.rejects(bus.state.put(C, "cli-1", dato("cli-1")), (e: PermissionDeniedError) => {
        assert.ok(e instanceof PermissionDeniedError, `lanzó ${(e as Error)?.constructor?.name}: ${(e as Error)?.message}`);
        assert.equal(e.code, "PERMISSION_DENIED");
        // lo que falta es justo lo que se quitó (el subject del permiso, con su comodín, cubre el denegado)
        const quitado = v.quitado.replace(/\.>$/, "");
        assert.ok(e.subject.startsWith(quitado), `se denegó «${e.subject}» y se quitó «${v.quitado}»`);
        return true;
      });
      assert.equal(await enElStream("state.clientes.cliente.v1.acme.cli-1"), null, "no se escribió nada");
    });
  }

  for (const v of VARIANTES.filter((x) => x.rol === "observador")) {
    test(`observador sin \`${v.quitado}\``, { timeout: 30_000 }, async () => {
      await limpiar();
      const dueno = await abrir("crm-api");
      await dueno.state.put(C, "cli-1", dato("cli-1"), { tenantId: "acme" });
      const durable = await provisionar(v.service, "acme");
      const { l, logger } = logs();
      const bus = await abrir(v.service, { tenantId: "acme", logger });
      const vistas: StateEntry[] = [];
      const abre = () => bus.state.watch(C, async (e) => void vistas.push(e));
      const denegado = (e: unknown) => {
        assert.ok(e instanceof PermissionDeniedError, `lanzó ${(e as Error)?.constructor?.name}: ${(e as Error)?.message}`);
        assert.equal((e as PermissionDeniedError).code, "PERMISSION_DENIED");
        assert.equal((e as PermissionDeniedError).subject, v.quitado);
        return true;
      };

      if (v.quitado.startsWith("$JS.API.CONSUMER.INFO.") || v.quitado.startsWith("$JS.API.CONSUMER.MSG.NEXT.") || v.quitado.startsWith("$JS.API.STREAM.INFO.DLQ_")) {
        // CONSUMER.INFO y STREAM.INFO.DLQ: peticiones con respuesta → la denegación llega en ~1 ms.
        // MSG.NEXT: un publish suelto, sin respuesta → el broker CALLA y el consumidor quedaría inerte,
        // parece sano (A-20); el SDK lo comprueba al abrir y lanza.
        await assert.rejects(abre(), denegado);
      } else if (v.quitado.startsWith("$JS.ACK.")) {
        // El ack es un publish suelto: el evento se procesa y se REENTREGA cada ack_wait, sin fin y sin
        // error (A-19). Lo único observable es el aviso asíncrono del broker: el SDK lo registra.
        await abre();
        assert.ok(await esperarHasta(() => vistas.length >= 1, 10_000), "el watch debe seguir entregando");
        assert.ok(await esperarHasta(() => l.some((m) => m.includes(v.quitado)), 5_000), `sin aviso del ack denegado: ${l.join(" | ")}`);
        const info = await jsm.consumers.info(STREAM, durable);
        // (la secuencia del stream NO empieza en 0 tras un purge: se compara con lo entregado)
        assert.equal(info.num_ack_pending, 1, "se entregó y nunca se confirmó");
        assert.ok(info.ack_floor.stream_seq < info.delivered.stream_seq, "el suelo de acks no avanzó");
      } else if (v.quitado.includes("DIRECT.GET")) {
        // get usa DIRECT.GET; watch NO: lo que se quita solo rompe la lectura puntual
        await assert.rejects(bus.state.get(C, "cli-1"), (e: unknown) => {
          assert.ok(e instanceof PermissionDeniedError);
          assert.ok((e as PermissionDeniedError).subject.startsWith("$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme."));
          return true;
        });
        await abre();
        assert.ok(await esperarHasta(() => vistas.length >= 1, 10_000), "el watch no necesita DIRECT.GET");
      } else if (v.quitado.startsWith("dlq.state.")) {
        // Publicar en la DLQ: sin el permiso, una entrada que muere NO se pierde —no se termina— y se reentrega.
        const buena = await enElStream("state.clientes.cliente.v1.acme.cli-1");
        await escribeCrudo("state.clientes.cliente.v1.acme.mala", { ...buena, id: "sembrado-mala", tenantid: "globex", subject: "mala", partitionkey: "mala" });
        await abre();
        assert.ok(await esperarHasta(() => l.some((m) => /no se pudo publicar en la DLQ de estado/.test(m)), 10_000), `sin aviso: ${l.join(" | ")}`);
        assert.equal((await jsm.streams.info("DLQ_CLIENTES")).state.messages, 0, "nada llegó a la DLQ");
        assert.equal(vistas.length, 1, "lo bueno sí se entrega");
        assert.ok((await jsm.consumers.info(STREAM, durable)).num_ack_pending >= 1, "lo que no se pudo guardar NO se terminó");
      } else {
        assert.fail(`permiso sin caso en la matriz: ${v.quitado}`);
      }
    });
  }

  test("la matriz cubre CADA permiso de observador y de dueño (si la lista crece, esto falla)", () => {
    const obs = VARIANTES.filter((v) => v.rol === "observador").map((v) => v.quitado);
    assert.equal(obs.length, 6, `observador: ${obs.join(", ")}`);
    assert.equal(VARIANTES.filter((v) => v.rol === "dueño").length, 2);
  });

  // ══════════════════════════════════════════════════════════════════════════
  // Aprovisionamiento: NOT_PROVISIONED y los durables mal puestos
  // ══════════════════════════════════════════════════════════════════════════

  test("sin el durable y sin CONSUMER.CREATE → NOT_PROVISIONED (consumidor), PERMANENT, apunta a `flux provision`", async () => {
    await limpiar();
    const w = await abrir("tienda-web", { tenantId: "acme" });
    await assert.rejects(w.state.watch(C, () => {}), (e: NotProvisionedError) => {
      assert.ok(e instanceof NotProvisionedError);
      assert.equal(e.code, "NOT_PROVISIONED");
      assert.deepEqual(e.resource, { kind: "consumer", name: durableDeEstado("tienda-web", C, "acme"), stream: STREAM });
      assert.match(e.message, /flux provision/);
      return true;
    });
  });

  test("el stream de estado NO existe → NOT_PROVISIONED, con las dos causas y sin pedir STREAM.INFO", async () => {
    // `pagos-api` es dueño de `pagos` y no hay STATE_PAGOS: nadie contesta al DIRECT.GET
    const p = await abrir("pagos-api", { tenantId: "acme" });
    for (const op of [
      () => p.state.get("pagos.pago.v1", "p-1"),
      () => p.state.put("pagos.pago.v1", "p-1", { aggregateVersion: 1, importeCents: 1 }),
      () => p.state.delete("pagos.pago.v1", "p-1", { aggregateVersion: 2 }),
    ]) {
      await assert.rejects(op(), (e: NotProvisionedError) => {
        assert.ok(e instanceof NotProvisionedError, `lanzó ${(e as Error)?.constructor?.name}: ${(e as Error)?.message}`);
        assert.equal(e.code, "NOT_PROVISIONED");
        assert.match(e.message, /STATE_PAGOS/);
        assert.match(e.message, /no existe, o existe sin/);
        assert.match(e.message, /allow_direct/);
        assert.match(e.message, /flux provision/);
        assert.match(e.message, /flux doctor/);
        return true;
      });
    }
    // un watch sobre un contrato sin stream: CONSUMER.INFO contesta «stream not found» (10059), que se distingue
    // de «consumer not found» (10014) sin STREAM.INFO (A-48)
    const t = await abrir("tienda-pagos", { tenantId: "acme" });
    await assert.rejects(t.state.watch("pagos.pago.v1", () => {}), (e: NotProvisionedError) => {
      assert.ok(e instanceof NotProvisionedError, `lanzó ${(e as Error)?.constructor?.name}: ${(e as Error)?.message}`);
      assert.match(e.message, /STATE_PAGOS/);
      assert.match(e.message, /allow_direct/);
      return true;
    });
  });

  test("el stream existe pero SIN allow_direct: se ve EXACTAMENTE igual que uno que no existe (A-32)", async () => {
    const sd = await abrir("sd-api", { tenantId: "acme" });
    await assert.rejects(sd.state.get("sindirecto.cosa.v1", "k"), (e: NotProvisionedError) => {
      assert.ok(e instanceof NotProvisionedError);
      assert.match(e.message, /STATE_SINDIRECTO/);
      assert.match(e.message, /allow_direct/);
      return true;
    });
    await assert.rejects(sd.state.put("sindirecto.cosa.v1", "k", { aggregateVersion: 1 }), NotProvisionedError);
    // CONTROL: un stream con allow_direct SÍ responde (404 → null)
    const dueno = await abrir("crm-api", { tenantId: "acme" });
    assert.equal(await dueno.state.get(C, "no-existe"), null);
  });

  test("un durable aprovisionado con otra política de entrega o con otro filtro NO se acepta: ConsumerConfigMismatchError", async () => {
    await limpiar();
    // deliver_policy: all (el broker entregaría lo mismo en este stream, pero la spec dice last_per_subject)
    await provisionar("tienda-mal", "acme", { deliver_policy: DeliverPolicy.All });
    const mal = await abrir("tienda-mal", { tenantId: "acme" });
    await assert.rejects(mal.state.watch(C, () => {}), (e: ConsumerConfigMismatchError) => {
      assert.ok(e instanceof ConsumerConfigMismatchError);
      assert.ok(e.differences.some((d) => d.field === "deliver_policy"));
      return true;
    });
    // un filtro de TODOS los tenants en un durable que dice ser de acme: entregaría lo de globex sin error
    await provisionar("tienda-mal", "acme", { filter_subject: `state.${C}.*.*` });
    await assert.rejects(mal.state.watch(C, () => {}), (e: ConsumerConfigMismatchError) => {
      assert.ok(e.differences.some((d) => d.field === "filter_subject"));
      return true;
    });
    // CONTROL: bien aprovisionado, abre
    await provisionar("tienda-mal", "acme");
    await mal.state.watch(C, () => {});
  });

  test("sin el tenant en connect() ni en la llamada, `get` y `put` no escriben en `system`: INVALID_TENANT_FOR_STATE", async () => {
    await limpiar();
    const dueno = await abrir("crm-api"); // sin tenantId
    await assert.rejects(dueno.state.put(C, "cli-1", dato("cli-1")), (e: { code?: string }) => e.code === "INVALID_TENANT_FOR_STATE");
    await assert.rejects(dueno.state.get(C, "cli-1"), (e: { code?: string }) => e.code === "INVALID_TENANT_FOR_STATE");
    assert.equal((await jsm.streams.info(STREAM)).state.messages, 0, "nada se escribió en `system`");
  });

  // ══════════════════════════════════════════════════════════════════════════
  // Entradas rotas, DLQ de estado y PII
  // ══════════════════════════════════════════════════════════════════════════

  test(
    "DLQ de estado: lo POISON y lo PERMANENT mueren sin llevar el dato, y el dueño lo lee con SU durable",
    { timeout: 60_000 },
    async () => {
      await limpiar();
      const dueno = await abrir("crm-api", { tenantId: "acme" });
      const SECRETO = "ana.secreta@example.com";

      // 1. una entrada buena, y tres que un SDK conforme NO emite (las fabrica ops, saltándose al SDK)
      await dueno.state.put(C, "bueno-1", dato("bueno-1"));
      const bueno = await enElStream("state.clientes.cliente.v1.acme.bueno-1");
      const sembrar = (k: string, cambios: object) =>
        // cada una con SU id: el `Nats-Msg-Id` de la DLQ es `dlq-<id>-<consumer>`, y dos con el mismo id se deduplicarían
        escribeCrudo(`state.clientes.cliente.v1.acme.${k}`, { ...bueno, id: `sembrado-${k}`, subject: k, partitionkey: k, data: dato(k), ...cambios });
      const revMal = await sembrar("mal-1", { tenantid: "globex" }); // el sobre MIENTE sobre su tenant
      const revRoto = await escribeCrudo("state.clientes.cliente.v1.acme.roto-1", `{"esto no es un sobre": "${SECRETO}"`); // JSON roto
      const revRestringido = await sembrar("restringido-1", { dataclassification: "restricted" });
      await dueno.state.put(C, "err-tipado", dato("err-tipado"));
      await dueno.state.put(C, "err-plano", dato("err-plano"));
      await dueno.state.put(C, "bueno-2", dato("bueno-2"));

      // 2. un observador cuyo handler falla en dos claves
      await provisionar("tienda-web", "acme");
      const metricas = new InMemoryMetrics();
      const { l, logger } = logs();
      const poison: Array<{ subject: string; error: Error; raw: Uint8Array }> = [];
      const w = await abrir("tienda-web", {
        tenantId: "acme",
        logger,
        metrics: metricas,
        onPoison: (i: { subject: string; error: Error; raw: Uint8Array }) => void poison.push(i),
        classifier: { unknownErrorPolicy: "permanent" }, // un error sin clasificar va a la DLQ ya, sin esperar 30 s
      });
      const aplicadas: string[] = [];
      await w.state.watch(C, async (e) => {
        if (e.key === "err-tipado") throw new PermanentError("país fuera del catálogo", { code: "PAIS_NO_SOPORTADO" });
        if (e.key === "err-plano") throw new Error(`duplicate key value violates unique constraint (${SECRETO})`);
        aplicadas.push(e.key);
      });
      assert.ok(await esperarHasta(async () => (await jsm.streams.info("DLQ_CLIENTES")).state.messages >= 5, 15_000), "deben llegar 5 a la DLQ");
      await new Promise((r) => setTimeout(r, 300));

      // 3. lo que NO se aplicó, y lo que sí
      assert.deepEqual(aplicadas.sort(), ["bueno-1", "bueno-2"], "las entradas rotas NUNCA llegan al handler");
      assert.equal(poison.length, 3, "mal-1, roto-1 y restringido-1: ALERTA por cada POISON");
      for (const p of poison) assert.equal(p.raw.byteLength, 0, "`raw` va vacío: el cuerpo de un estado puede llevar datos personales");
      assert.deepEqual(poison.map((p) => p.subject).sort(), [
        "state.clientes.cliente.v1.acme.mal-1",
        "state.clientes.cliente.v1.acme.restringido-1",
        "state.clientes.cliente.v1.acme.roto-1",
      ]);

      // 4. el dueño lee la DLQ de estado con su durable `<servicio>__dlq_state_<dom>` (los permisos exactos del rol «DLQ»)
      const durableDlq = durableDeDlqDeEstado("crm-api", "clientes");
      await jsm.consumers.add("DLQ_CLIENTES", {
        durable_name: durableDlq,
        filter_subject: "dlq.state.clientes.>",
        ack_policy: AckPolicy.Explicit,
        ack_wait: nanos(CONSUMER_DEFAULTS.ackWaitMs),
        max_deliver: CONSUMER_DEFAULTS.maxDeliver,
        backoff: CONSUMER_DEFAULTS.backoffMs.map(nanos),
        max_ack_pending: CONSUMER_DEFAULTS.maxAckPending,
      });
      const crmNc = await natsConnect({ servers: broker.url, user: "crm-api", pass: "pw", inboxPrefix: "_INBOX.crm-api" });
      const dlq: Record<string, any>[] = [];
      const crudoDlq: string[] = [];
      try {
        const consumidor = await jetstream(crmNc).consumers.get("DLQ_CLIENTES", durableDlq);
        for await (const m of await consumidor.fetch({ max_messages: 10, expires: 2000 })) {
          crudoDlq.push(dec.decode(m.data));
          dlq.push(JSON.parse(dec.decode(m.data)));
          assert.equal(m.subject, "dlq.state.clientes.cliente.v1", "el subject de la DLQ de estado: sin tenant ni clave");
          m.ack();
        }
      } finally {
        await crmNc.close();
      }
      assert.equal(dlq.length, 5);
      const de = (k: string) => dlq.find((d) => d.data.key === k)!;

      // 5. NI UN dato personal en ninguna forma: ni en `data`, ni en `dlqerror`, ni como `rawBase64`
      for (const crudo of crudoDlq) {
        assert.ok(!crudo.includes(SECRETO), `el dato personal llegó a la DLQ: ${crudo.slice(0, 200)}`);
        assert.ok(!crudo.includes("Ana Secreta"));
        assert.ok(!crudo.includes("rawBase64"), "un mensaje ilegible NO se copia en ninguna forma");
      }
      for (const d of dlq) {
        assert.deepEqual(Object.keys(d.data), ["key", "revision"]);
        assert.equal(Object.keys(d).at(-1), "data");
        assert.ok(!("signature" in d) && !("signkeyid" in d));
        assert.equal(d.dlqconsumer, durableDeEstado("tienda-web", C, "acme"));
      }

      // 6. cada una, con su razón y la clave del SUBJECT y la revisión del stream
      assert.equal(de("mal-1").dlqreason, "poison");
      assert.match(de("mal-1").dlqerror, /^STATE_SUBJECT_MISMATCH: /);
      assert.equal(de("mal-1").data.revision, revMal);
      assert.equal(de("mal-1").tenantid, "globex", "el resto del sobre queda como estaba, mentira incluida");
      assert.equal(de("roto-1").dlqreason, "poison");
      assert.equal(de("roto-1").data.revision, revRoto);
      assert.equal(de("roto-1").subject, "roto-1", "sintetizado con lo único fiable: el subject");
      assert.equal(de("roto-1").tenantid, "acme");
      assert.equal(de("roto-1").source, "/test/tienda-web", "el servicio que consume");
      assert.equal(de("roto-1").dataclassification, "internal");
      assert.equal(de("roto-1").id, de("roto-1").correlationid);
      assert.equal(de("restringido-1").dlqreason, "poison");
      assert.match(de("restringido-1").dlqerror, /^INVALID_DATACLASSIFICATION: /);
      assert.equal(de("restringido-1").data.revision, revRestringido);
      assert.equal(de("err-tipado").dlqreason, "permanent");
      assert.equal(de("err-tipado").dlqerror, "PAIS_NO_SOPORTADO: país fuera del catálogo");
      assert.equal(de("err-plano").dlqreason, "permanent");
      assert.match(de("err-plano").dlqerror, /^UNKNOWN: error sin clasificar/, "el mensaje de un error que no es de flux se omite: podría citar la fila entera");

      // 7. métricas: el consumer SIN el sufijo del tenant; los POISON cuentan como `poison`
      const c = metricas.snapshot().counters;
      const serie = `consumer="tienda-web__state_clientes_cliente_v1"`;
      assert.equal(c.get(`flux_events_consumed_total{${serie},outcome="poison",subject="state.clientes.cliente.v1"}`), 3);
      assert.equal(c.get(`flux_events_consumed_total{${serie},outcome="ok",subject="state.clientes.cliente.v1"}`), 2);
      assert.equal(c.get(`flux_events_consumed_total{${serie},outcome="permanent",subject="state.clientes.cliente.v1"}`), 2);
      assert.equal(c.get(`flux_events_dlq_total{code="STATE_SUBJECT_MISMATCH",${serie},reason="poison",subject="state.clientes.cliente.v1"}`), 1);
      assert.ok(![...c.keys()].some((k) => k.includes("acme") || k.includes("mal-1")), "ni el tenant ni la clave como etiqueta");
      assert.ok(l.some((m) => /POISON STATE_SUBJECT_MISMATCH/.test(m)), "el POISON se anuncia en el log");
      assert.ok(l.every((m) => !m.includes(SECRETO)), "ningún log lleva el dato");

      // 8. `get` de una entrada que miente: lanza (POISON), no la devuelve
      await assert.rejects(w.state.get(C, "mal-1"), StateSubjectMismatchError);
      // 9. y quien NO es dueño no lee la DLQ de otro: tienda-web no tiene CONSUMER.INFO sobre `crm-api__dlq_state_clientes`
      const web = await natsConnect({ servers: broker.url, user: "tienda-web", pass: "pw", inboxPrefix: "_INBOX.tienda-web" });
      try {
        await assert.rejects(
          web.request(`$JS.API.CONSUMER.INFO.DLQ_CLIENTES.${durableDlq}`, "", { timeout: 1500 }),
          (e: { cause?: { name?: string } }) => e.cause?.name === "PermissionViolationError",
        );
      } finally {
        await web.close();
      }
    },
  );

  test("L3 contra el bundle REAL: put válido pasa, uno roto falla antes de escribir, y la lápida valida contra `eliminado`", async () => {
    await limpiar();
    const dueno = await abrir("crm-api", { tenantId: "acme", validation: { mode: "strict", bundle }, schemaBaseUrl: undefined });
    assert.equal((await dueno.state.put(C, "cli-1", dato("cli-1"))).outcome, "ok");
    const guardado = await enElStream("state.clientes.cliente.v1.acme.cli-1");
    assert.equal(guardado!.dataschema, "https://schemas.internal/state/clientes/cliente/1.0.0.json", "el índice `state` del bundle, no la red");
    await assert.rejects(dueno.state.put(C, "cli-2", { ...dato("cli-2"), email: "no-es-un-email" }), (e: { opts: { code: string }; message: string }) => {
      assert.equal(e.opts.code, "INVALID_SCHEMA");
      assert.ok(!e.message.includes("no-es-un-email"), "no copia valores a su mensaje");
      return true;
    });
    assert.equal(await enElStream("state.clientes.cliente.v1.acme.cli-2"), null, "falló ANTES de escribir");
    assert.equal((await dueno.state.delete(C, "cli-1", { aggregateVersion: 2 })).outcome, "ok");
    assert.equal((await enElStream("state.clientes.cliente.v1.acme.cli-1"))!.dataschema, "https://schemas.internal/system/state/eliminado/1.0.0.json");
  });

  test("el sobre que llega al broker lleva `data` el último, UTF-8 literal y los atributos que el SDK rellena", async () => {
    await limpiar();
    const dueno = await abrir("crm-api", { tenantId: "acme" });
    await dueno.state.put(C, "cli-1", { ...dato("cli-1"), nombre: "María Peña", pais: "ES" });
    const m = await jsm.streams.getMessage(STREAM, { last_by_subj: "state.clientes.cliente.v1.acme.cli-1" });
    const texto = dec.decode(m!.data);
    assert.ok(texto.includes("María Peña"));
    assert.ok(!/\\u[0-9a-fA-F]{4}/.test(texto));
    const ev = JSON.parse(texto);
    assert.equal(Object.keys(ev).at(-1), "data");
    assert.equal(ev.subject, "cli-1");
    assert.equal(ev.tenantid, "acme");
    assert.equal(ev.partitionkey, "cli-1");
    assert.equal(ev.dataclassification, "confidential");
    assert.equal(ev.source, "/test/crm-api");
    // el id viaja también como Nats-Msg-Id y el broker lo conserva en las cabeceras (A-32): es lo que reconoce el paso 1b
    assert.equal(m!.header.get("Nats-Msg-Id"), ev.id);
  });

  test("tres instancias del dueño escribiendo la misma clave convergen en la versión más alta (STATE_CONFLICT es RETRYABLE)", { timeout: 60_000 }, async () => {
    await limpiar();
    const instancias = [await abrir("crm-api", { tenantId: "acme" }), await abrir("crm-api", { tenantId: "acme" }), await abrir("crm-api", { tenantId: "acme" })];
    const resultados = { ok: 0, stale: 0, conflicto: 0 };
    const corre = async (bus: FluxBus, n: number) => {
      for (let v = 1; v <= 30; v++) {
        if (!(v % 3 === n || Math.random() < 0.3)) continue;
        for (let intento = 0; intento < 10; intento++) {
          try {
            const r = await bus.state.put(C, "carrera", dato("carrera", v));
            resultados[r.outcome]++;
            break;
          } catch (e) {
            // un conflicto agotado es RETRYABLE: la aplicación reintenta, y el SDK ya habrá releído
            if ((e as { code?: string }).code !== "STATE_CONFLICT") throw e;
            resultados.conflicto++;
          }
        }
      }
    };
    await Promise.all(instancias.map((b, n) => corre(b, n)));
    const final = await instancias[0].state.get(C, "carrera");
    assert.equal(final!.aggregateVersion, 30, `resultados: ${JSON.stringify(resultados)}`);
    assert.equal((await jsm.streams.info(STREAM)).state.messages, 1, "un valor por clave");
  });
});
