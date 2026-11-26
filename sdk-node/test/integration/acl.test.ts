/**
 * El SDK contra un nats-server REAL con permisos exactos y sin `_INBOX.>` común.
 *
 * Cierra el hallazgo H3 del plan v1.1 para el SDK de Node: ningún test aplicaba los
 * permisos a un broker. Aquí cada caso se prueba en los DOS sentidos: lo correcto pasa
 * y lo incorrecto falla.
 *
 * Los permisos son los de docs/plan-v1.1/evidencia/candidata.conf ampliados con lo que el
 * SDK usa de verdad (ver acl.ts).
 */

import { test, describe, before, after, afterEach } from "node:test";
import assert from "node:assert/strict";

import type { NatsConnection } from "@nats-io/transport-node";
import { AckPolicy, DeliverPolicy, ReplayPolicy, type JetStreamManager } from "@nats-io/jetstream";

import {
  connect,
  ErrorClass,
  NotProvisionedError,
  PermanentError,
  PermissionDeniedError,
  RetryableError,
  TenantIsolationError,
  classify,
  type FluxBus,
} from "../../src/index.js";
import { CONSUMER_DEFAULTS, STREAM_DEFAULTS, nanos } from "../../src/protocol.js";
import { permissionViolation } from "../../src/security.js";
import {
  ACL_ANTIGUA_FACTURACION_API,
  permisosExactos,
  usuarioConf,
  type Permisos,
  type ServicioAcl,
} from "./acl.js";
import {
  conectarCrudo,
  conectarOps,
  describeBroker,
  dormir,
  esperarHasta,
  mensajesDe,
  startBroker,
  type Broker,
} from "./broker.js";

// ─── Modelo de permisos: anclado a candidata.conf ────────────────────────────

describe("el modelo de permisos del test coincide con candidata.conf", () => {
  // Copiado LITERAL de docs/plan-v1.1/evidencia/candidata.conf (usuario `fx-fact`). No se
  // lee el fichero: esa carpeta es temporal y se borra al cerrar la Fase 3.
  const CANDIDATA_FX_FACT = [
    "facturacion.>",
    "dlq.pedidos.pedido.v1.creado",
    "$JS.API.INFO",
    "$JS.API.STREAM.INFO.EVT_PEDIDOS",
    "$JS.API.STREAM.INFO.EVT_FACTURACION",
    "$JS.API.CONSUMER.INFO.EVT_PEDIDOS.fx-fact__pedidos_pedido_v1_creado",
    "$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.fx-fact__pedidos_pedido_v1_creado.pedidos.pedido.v1.creado",
    "$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.fx-fact__pedidos_pedido_v1_creado",
    "$JS.ACK.EVT_PEDIDOS.fx-fact__pedidos_pedido_v1_creado.>",
  ];

  test("fx-fact: la candidata + `STREAM.INFO.DLQ_PEDIDOS`, y nada más", () => {
    const p = permisosExactos({ service: "fx-fact", owns: ["facturacion"], consumes: ["pedidos.pedido.v1.creado"] });
    assert.deepEqual(
      new Set(p.publish.filter((x) => !CANDIDATA_FX_FACT.includes(x))),
      new Set(["$JS.API.STREAM.INFO.DLQ_PEDIDOS"]),
      "lo que el SDK usa y la candidata no tenía",
    );
    assert.deepEqual(CANDIDATA_FX_FACT.filter((x) => !p.publish.includes(x)), []);
    assert.deepEqual(p.subscribe, ["_INBOX.fx-fact.>"]);
  });

  test("ningún servicio recibe permiso de suscripción sobre subjects de dominio", () => {
    const p = permisosExactos({ service: "fx-fact", owns: ["facturacion"], consumes: ["pedidos.pedido.v1.creado"] });
    assert.ok(p.subscribe.every((s) => s.startsWith("_INBOX.fx-fact.")));
    assert.ok(!p.subscribe.includes("_INBOX.>"));
  });
});

// ─── El broker ───────────────────────────────────────────────────────────────

const SUBJECT = "pedidos.pedido.v1.creado";

const SERVICIOS: Record<string, ServicioAcl> = {
  "fx-ped": { service: "fx-ped", owns: ["pedidos"] },
  "fx-fact": { service: "fx-fact", owns: ["facturacion"], consumes: [SUBJECT] },
  // No tiene concedido nada ajeno: es el que intenta espiar.
  "fx-chis": { service: "fx-chis", owns: ["chismes"] },
  "fx-ten": { service: "fx-ten", consumes: [SUBJECT] },
  "fx-tenb": { service: "fx-tenb", consumes: [SUBJECT] },
  // Dueño de `nuevos` (sin aprovisionar) y de `otros` (aprovisionado): sin CREATE de stream.
  "fx-mixto": { service: "fx-mixto", owns: ["nuevos", "otros"], consumes: [SUBJECT] },
  // Consumidor sin permiso para crear su durable.
  "fx-sincons": { service: "fx-sincons", consumes: [SUBJECT], crearConsumidor: false },
  "fx-prov": { service: "fx-prov", consumes: [SUBJECT], crearConsumidor: false },
};

/**
 * Para la matriz «cada permiso es necesario»: un servicio por permiso, con ese permiso
 * quitado. Se salta `CONSUMER.CREATE`: es necesario solo SI el consumidor no está
 * aprovisionado, y eso lo prueba el caso (c) por separado.
 */
interface Variante { service: string; quitado: string; perms: Permisos }
const VARIANTES: Variante[] = (() => {
  const out: Variante[] = [];
  const base = (service: string) =>
    permisosExactos({ service, owns: ["facturacion"], consumes: [SUBJECT] });
  const n = base("fx-v0").publish.length + 1;
  for (let i = 0; i < n; i++) {
    const service = `fx-v${i}`;
    const p = base(service);
    const todos = [...p.publish.map((x) => ["publish", x] as const), ["subscribe", p.subscribe[0]] as const];
    const [lado, quitado] = todos[i];
    if (quitado.startsWith("$JS.API.CONSUMER.CREATE.")) continue;
    out.push({
      service,
      quitado,
      perms: {
        publish: p.publish.filter((x) => !(lado === "publish" && x === quitado)),
        // Un `allow: []` NO deniega: en NATS una lista vacía significa «sin restricción». Quitar
        // el inbox propio es, por tanto, dejar solo el de OTRO servicio.
        subscribe: lado === "subscribe" ? ["_INBOX.otro-servicio.>"] : p.subscribe,
      },
    });
  }
  return out;
})();

function configDelBroker(port: number, store: string): string {
  const usuarios = [
    `{ user: "ops", password: "pw" }`, // IaC: aprovisiona. Sin restricciones.
    ...Object.values(SERVICIOS).map((s) => usuarioConf(s.service, permisosExactos(s))),
    usuarioConf("fx-vb", permisosExactos({ service: "fx-vb", owns: ["facturacion"], consumes: [SUBJECT] })),
    ...VARIANTES.map((v) => usuarioConf(v.service, v.perms)),
    // La ACL ANTIGUA, congelada.
    usuarioConf("facturacion-api", ACL_ANTIGUA_FACTURACION_API),
    // Un espía con la forma de la ACL antigua: puede suscribirse a `_INBOX.>`.
    usuarioConf("fx-espia-viejo", { publish: ["$JS.API.INFO"], subscribe: ["_INBOX.>"] }),
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

// ─── Utilidades ──────────────────────────────────────────────────────────────

const nombre = (e: unknown) => (e as Error)?.constructor?.name ?? typeof e;
/** Qué lanzó, y —si es una denegación— de qué subject. */
const detalle = (e: unknown) =>
  e instanceof PermissionDeniedError ? `PermissionDeniedError(${e.operation} ${e.subject})` : nombre(e);

describeBroker("SDK Node bajo ACL exacta, con inbox privado", () => {
  let broker: Broker;
  let opsNc: NatsConnection;
  let jsm: JetStreamManager;
  let productor: FluxBus;
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
  /** Abre un bus y lo registra para cerrarlo siempre. */
  async function abrir(service: string, extra: Record<string, unknown> = {}): Promise<FluxBus> {
    const bus = await connect(base(service, extra));
    abiertos.push(bus);
    return bus;
  }

  /** Lo que `flux provision` hará (03-delivery.md §2): la config canónica del consumidor. */
  async function provisionarConsumidor(service: string, subject = SUBJECT): Promise<void> {
    const dom = subject.split(".")[0];
    await jsm.consumers.add(`EVT_${dom.toUpperCase()}`, {
      durable_name: `${service}__${subject.replace(/[.-]/g, "_")}`,
      filter_subject: subject,
      ack_policy: AckPolicy.Explicit,
      ack_wait: nanos(CONSUMER_DEFAULTS.ackWaitMs),
      max_deliver: CONSUMER_DEFAULTS.maxDeliver,
      backoff: CONSUMER_DEFAULTS.backoffMs.map(nanos),
      max_ack_pending: CONSUMER_DEFAULTS.maxAckPending,
      deliver_policy: DeliverPolicy.All,
      replay_policy: ReplayPolicy.Instant,
    });
  }

  /** Vacía los streams compartidos: cada caso parte sin la historia del anterior. */
  async function limpiar(): Promise<void> {
    for (const s of ["EVT_PEDIDOS", "DLQ_PEDIDOS"]) await jsm.streams.purge(s);
  }
  const dlq = (marca: string) =>
    mensajesDe(jsm, "DLQ_PEDIDOS").then((ms) => ms.filter((m) => m.data?.marca === marca));

  before(async () => {
    broker = await startBroker(({ port, store }) => configDelBroker(port, store));
    ({ nc: opsNc, jsm } = await conectarOps(broker));

    // Lo que hará `flux provision`: los streams EVT_/DLQ_ de cada dominio. A propósito NO
    // existe EVT_NUEVOS: es el stream «sin aprovisionar» del caso (c).
    const evt = (name: string, subjects: string[]) =>
      jsm.streams.add({
        name,
        subjects,
        max_age: nanos(STREAM_DEFAULTS.maxAgeMs),
        duplicate_window: nanos(STREAM_DEFAULTS.duplicateWindowMs),
      });
    await evt("EVT_PEDIDOS", ["pedidos.>"]);
    await evt("EVT_FACTURACION", ["facturacion.>"]);
    await evt("EVT_OTROS", ["otros.>"]);
    await jsm.streams.add({ name: "DLQ_PEDIDOS", subjects: ["dlq.pedidos.>"], max_age: nanos(STREAM_DEFAULTS.dlqMaxAgeMs) });

    // El productor vive todo el fichero; el resto de buses, lo que dura cada test.
    productor = await connect(base("fx-ped"));
  });

  // Un bus que sigue suscrito tras su test competiría por el mismo durable con el del
  // test siguiente y le robaría entregas: el estado NO debe filtrarse entre casos.
  afterEach(async () => {
    for (const b of abiertos.splice(0)) await b.close().catch(() => {});
  });

  after(async () => {
    await productor?.close().catch(() => {});
    await opsNc?.close().catch(() => {});
    await broker?.stop();
  });

  // ── (a) extremo a extremo ──────────────────────────────────────────────────

  test(
    "(a) publish, subscribe, ack, WIP, reintento y DLQ de extremo a extremo con permisos exactos",
    { timeout: 90_000 },
    async () => {
      await limpiar();
      const { l: registro, logger } = logs();
      const intentos = new Map<string, number[]>();
      const dlqAvisos: string[] = [];
      const fact = await abrir("fx-fact", { logger, onDlq: (i: { subject: string }) => dlqAvisos.push(i.subject) });

      // Lo que viaja por los subjects del protocolo JetStream y por el inbox, visto desde fuera.
      const verbosAck = new Set<string>();
      const inboxes = new Set<string>();
      // Las entregas de un pull viajan por el inbox pero llegan con el subject ORIGINAL del
      // evento: así se ve la fuga H2 con `_INBOX.>`, y así se distinguen aquí de las respuestas.
      const entregas = new Set<string>();
      const espiaAck = opsNc.subscribe("$JS.ACK.>");
      void (async () => {
        for await (const m of espiaAck) verbosAck.add(new TextDecoder().decode(m.data).split(" ")[0]);
      })();
      const espiaInbox = opsNc.subscribe("_INBOX.>");
      void (async () => {
        for await (const m of espiaInbox) (m.subject.startsWith("_INBOX.") ? inboxes : entregas).add(m.subject);
      })();

      const sub = await fact.subscribe(SUBJECT, async (ev, ctx) => {
        const tipo = (ev.data as { tipo: string }).tipo;
        intentos.set(tipo, [...(intentos.get(tipo) ?? []), ctx.attempt]);
        if (tipo === "retry" && ctx.attempt === 1) {
          throw new RetryableError("transitorio", { retryAfterMs: 100 });
        }
        if (tipo === "perm") throw new PermanentError("regla de negocio", { code: "PEDIDO_CANCELADO" });
        // Más de la mitad de ack_wait (15 s): el SDK debe enviar WIP o el broker reentregaría.
        if (tipo === "lento") await dormir(16_500);
      });
      assert.equal(sub.durable, "fx-fact__pedidos_pedido_v1_creado");

      const marca = "e2e";
      const ids: Record<string, string> = {};
      for (const tipo of ["ok", "retry", "perm", "lento"]) {
        ids[tipo] = (await productor.publish(SUBJECT, { marca, tipo }, { aggregateId: `ped-${tipo}` })).id;
      }

      const completo = await esperarHasta(
        async () =>
          intentos.get("ok")?.length === 1 &&
          intentos.get("retry")?.length === 2 &&
          intentos.get("perm")?.length === 1 &&
          intentos.get("lento")?.length === 1 &&
          (await dlq(marca)).length === 1 &&
          (await jsm.consumers.info("EVT_PEDIDOS", sub.durable)).num_ack_pending === 0,
        30_000,
        100,
      );
      assert.ok(completo, `el flujo no se completó: ${JSON.stringify([...intentos])}\n${registro.join("\n")}`);

      // Publicar: llegó al stream por el subject del dominio propio.
      assert.equal((await mensajesDe(jsm, "EVT_PEDIDOS")).length, 4);
      // Reintento: la 2ª entrega del transitorio, y UNA sola del resto (WIP evitó la reentrega del lento).
      assert.deepEqual(intentos.get("retry"), [1, 2]);
      assert.deepEqual(intentos.get("lento"), [1]);
      // DLQ por error permanente: a la primera, con el id original.
      const [muerto] = await dlq(marca);
      assert.equal(muerto.dlqreason, "permanent");
      assert.equal(muerto.dlqattempts, 1);
      assert.equal(muerto.dlqconsumer, sub.durable);
      assert.equal(muerto.id, ids.perm);
      assert.match(muerto.dlqerror, /PEDIDO_CANCELADO/);
      assert.deepEqual(dlqAvisos, [SUBJECT]);
      // Los cuatro verbos de ack pasaron por `$JS.ACK.<S>.<D>.>`: ack, nak con retraso, term y WIP.
      for (const v of ["+ACK", "-NAK", "+TERM", "+WPI"]) {
        assert.ok(verbosAck.has(v), `no se vio ${v}; vistos: ${[...verbosAck]}`);
      }
      // Todo ello sin UNA denegación: la lista de permisos alcanza.
      assert.deepEqual(registro.filter((m) => /denegó|DLQ.*no se pudo/.test(m)), []);
      // Y por el inbox PRIVADO: ni un inbox con el prefijo común `_INBOX.<nuid>`.
      const ajenos = [...inboxes].filter((s) => !/^_INBOX\.(fx-fact|fx-ped|ops)\./.test(s));
      assert.deepEqual(ajenos, [], "el SDK usó un inbox que no es el suyo");
      assert.ok([...inboxes].some((s) => s.startsWith("_INBOX.fx-fact.")), "el espía no vio el inbox de fx-fact: no prueba nada");
      assert.ok(entregas.has(SUBJECT), "el espía no vio las entregas del pull: no prueba nada");
      espiaAck.unsubscribe();
      espiaInbox.unsubscribe();
    },
  );

  // ── (a-) cada permiso de la lista es necesario ─────────────────────────────

  describe("(a-) cada permiso de la lista es NECESARIO: sin él el SDK falla, no se degrada en silencio", () => {
    /** El ciclo completo de un servicio dueño de `facturacion` que consume `pedidos`. */
    async function ciclo(service: string): Promise<{ problemas: string[]; logs: string[] }> {
      await limpiar();
      const { l: registro, logger } = logs();
      const problemas: string[] = [];
      let bus: FluxBus;
      try {
        bus = await connect(base(service, { logger }));
      } catch (e) {
        return { problemas: [`connect() lanzó ${detalle(e)}`], logs: registro };
      }
      abiertos.push(bus);
      const marca = `ciclo-${service}`;
      const vistos: string[] = [];
      try {
        try {
          await bus.publish("facturacion.factura.v1.emitida", { marca });
        } catch (e) {
          problemas.push(`publish en el dominio propio lanzó ${detalle(e)}`);
        }
        let durable: string | undefined;
        try {
          const sub = await bus.subscribe(SUBJECT, async (ev) => {
            const d = ev.data as { marca: string; tipo: string };
            if (d.marca !== marca) return;
            vistos.push(d.tipo);
            if (d.tipo === "perm") throw new PermanentError("a la DLQ", { code: "PRUEBA" });
          });
          durable = sub.durable;
        } catch (e) {
          problemas.push(`subscribe lanzó ${detalle(e)}`);
        }
        if (durable) {
          await productor.publish(SUBJECT, { marca, tipo: "ok" });
          await productor.publish(SUBJECT, { marca, tipo: "perm" });
          const completo = await esperarHasta(async () => {
            if (vistos.length !== 2) return false;
            if ((await dlq(marca)).length !== 1) return false;
            return (await jsm.consumers.info("EVT_PEDIDOS", durable!)).num_ack_pending === 0;
          }, 2500);
          if (!completo) {
            const i = await jsm.consumers.info("EVT_PEDIDOS", durable);
            problemas.push(
              `el flujo no se completó: vistos=${JSON.stringify(vistos)} dlq=${(await dlq(marca)).length} ` +
                `ack_pending=${i.num_ack_pending} pending=${i.num_pending}`,
            );
          }
        }
      } finally {
        await bus.close().catch(() => {});
      }
      problemas.push(...registro.filter((m) => /denegó/.test(m)).map((m) => `aviso de denegación: ${m}`));
      return { problemas, logs: registro };
    }

    test("control: con la lista COMPLETA no hay ningún problema", async () => {
      const r = await ciclo("fx-vb");
      assert.deepEqual(r.problemas, [], r.logs.join("\n"));
    });

    for (const v of VARIANTES) {
      test(`sin «${v.quitado.replace(v.service, "<servicio>")}» falla`, async (t) => {
        const r = await ciclo(v.service);
        // Qué vio el SDK: queda en la salida del test como evidencia de CÓMO llega cada denegación.
        t.diagnostic(r.problemas.map((p) => p.slice(0, 170)).join(" | "));
        assert.ok(r.problemas.length > 0, `con el permiso quitado el SDK no notó nada: el permiso sobra`);
        // Y falla POR ESE permiso, no por otra cosa: el aviso cita el subject que se quitó.
        const citado = v.quitado === "facturacion.>"
          ? "facturacion.factura.v1.emitida" // el comodín concede; lo que se deniega es el subject concreto
          : v.quitado.startsWith("_INBOX.")
            ? `_INBOX.${v.service}.`
            : v.quitado;
        assert.ok(
          r.problemas.some((p) => p.includes(citado)),
          `ningún aviso cita «${citado}»:\n${r.problemas.join("\n")}`,
        );
        // Todo lo que se puede detectar al arrancar LANZA al arrancar. Solo dos permisos fallan
        // más tarde, y lo hacen sin excepción —un ack o un publish a la DLQ—: ahí lo único que
        // queda es el aviso del logger.
        const tardio = v.quitado.startsWith("$JS.ACK.") || v.quitado.startsWith("dlq.");
        assert.equal(
          r.problemas.some((p) => /lanzó PermissionDeniedError\(/.test(p)),
          !tardio,
          `¿lanzó en el arranque? esperado=${!tardio}\n${r.problemas.join("\n")}`,
        );
      });
    }

    test("hay una variante por cada permiso (la matriz no se queda corta si el SDK pide más)", () => {
      const completa = permisosExactos({ service: "fx-vb", owns: ["facturacion"], consumes: [SUBJECT] });
      const esperados = completa.publish.filter((x) => !x.startsWith("$JS.API.CONSUMER.CREATE.")).length + 1;
      assert.equal(VARIANTES.length, esperados);
    });
  });

  // ── (b) la ACL antigua ─────────────────────────────────────────────────────

  test("(b) con la ACL ANTIGUA el SDK falla de forma clara y a tiempo, no cuelga", { timeout: 15_000 }, async () => {
    const t0 = Date.now();
    const intento = connect(base("facturacion-api", { credentials: { user: "facturacion-api", pass: "pw" } }));
    await assert.rejects(intento, (e: unknown) => {
      assert.ok(e instanceof PermissionDeniedError, `lanzó ${nombre(e)}: ${(e as Error).message}`);
      assert.equal(e.code, "PERMISSION_DENIED");
      assert.equal(e.subject, "$JS.API.INFO");
      assert.equal(e.operation, "publish");
      assert.equal(e.service, "facturacion-api");
      assert.match(e.message, /ACL generada antes de la v1\.1/);
      // PERMANENT: un servicio mal configurado no se arregla reintentando.
      assert.equal(classify(e).class, ErrorClass.PERMANENT);
      return true;
    });
    // Un timeout de la API de JetStream son 5 s: esto tiene que ser una denegación, no una espera.
    assert.ok(Date.now() - t0 < 2500, `tardó ${Date.now() - t0} ms: parece un timeout`);
  });

  // ── (c) sin aprovisionar ───────────────────────────────────────────────────

  test("(c) stream sin aprovisionar y sin permiso de crearlo → NotProvisionedError, no un timeout", { timeout: 15_000 }, async () => {
    const { logger } = logs();
    const bus = await abrir("fx-mixto", { logger });

    const t0 = Date.now();
    await assert.rejects(bus.publish("nuevos.nuevo.v1.creado", { x: 1 }), (e: unknown) => {
      assert.ok(e instanceof NotProvisionedError, `lanzó ${nombre(e)}: ${(e as Error).message}`);
      assert.equal(e.code, "NOT_PROVISIONED");
      assert.deepEqual(e.resource, { kind: "stream", name: "EVT_NUEVOS" });
      assert.equal(e.deniedSubject, "$JS.API.STREAM.CREATE.EVT_NUEVOS");
      assert.match(e.message, /EVT_NUEVOS/);
      assert.match(e.message, /flux provision/);
      // PERMANENT, para que un handler que lo provoque no atasque la cola.
      assert.deepEqual([classify(e).class, classify(e).code], [ErrorClass.PERMANENT, "NOT_PROVISIONED"]);
      return true;
    });
    assert.ok(Date.now() - t0 < 2500, `tardó ${Date.now() - t0} ms: parece un timeout`);

    // Lo correcto pasa: el mismo servicio, con un stream SÍ aprovisionado, publica.
    const ev = await bus.publish("otros.otro.v1.creado", { x: 1 });
    assert.ok(ev.id);
  });

  test("(c2) consumidor sin aprovisionar y sin CONSUMER.CREATE → NotProvisionedError; aprovisionado, funciona SIN ese permiso", { timeout: 20_000 }, async () => {
    await limpiar();
    const { l: registro, logger } = logs();
    const bus = await abrir("fx-sincons", { logger });
    const durable = "fx-sincons__pedidos_pedido_v1_creado";

    const t0 = Date.now();
    await assert.rejects(bus.subscribe(SUBJECT, async () => {}), (e: unknown) => {
      assert.ok(e instanceof NotProvisionedError, `lanzó ${nombre(e)}: ${(e as Error).message}`);
      assert.deepEqual(e.resource, { kind: "consumer", name: durable, stream: "EVT_PEDIDOS" });
      assert.match(e.deniedSubject, /^\$JS\.API\.CONSUMER\.CREATE\.EVT_PEDIDOS\./);
      assert.match(e.message, /flux provision/);
      return true;
    });
    assert.ok(Date.now() - t0 < 2500, `tardó ${Date.now() - t0} ms: parece un timeout`);

    // `flux provision` lo crea (aquí, con ops); el SDK lo encuentra y NO intenta crearlo.
    registro.length = 0; // el aviso de la fase anterior era legítimo
    await provisionarConsumidor("fx-prov");
    const recibidos: string[] = [];
    const prov = await abrir("fx-prov", { logger });
    await prov.subscribe(SUBJECT, async (ev) => void recibidos.push((ev.data as { tipo: string }).tipo));
    await productor.publish(SUBJECT, { marca: "c2", tipo: "ok" });
    assert.ok(await esperarHasta(() => recibidos.length === 1, 3000), "el consumidor aprovisionado no recibió");
    assert.deepEqual(registro.filter((m) => /denegó/.test(m)), [], "intentó crear un consumidor que ya existía");
  });

  test("(c3) un handler que provoca NotProvisionedError o PermissionDeniedError va a la DLQ a la primera y NO atasca la cola", { timeout: 30_000 }, async () => {
    await limpiar();
    const bus = await abrir("fx-mixto");
    const tratados: string[] = [];
    await bus.subscribe(SUBJECT, async (ev) => {
      const d = ev.data as { marca: string; tipo: string };
      if (d.marca !== "c3") return;
      tratados.push(d.tipo);
      if (d.tipo === "sin-stream") await bus.publish("nuevos.nuevo.v1.creado", { x: 1 });
      if (d.tipo === "ajeno") await bus.publish(SUBJECT, { marca: "x", tipo: "y" }); // dominio que no es suyo
    });
    for (const tipo of ["sin-stream", "ajeno", "siguiente"]) await productor.publish(SUBJECT, { marca: "c3", tipo });

    assert.ok(
      await esperarHasta(async () => tratados.length === 3 && (await dlq("c3")).length === 2, 15_000, 100),
      `no se completó: tratados=${JSON.stringify(tratados)}`,
    );
    // Cada uno, UNA entrega: PERMANENT va a la DLQ sin gastar reintentos (UNKNOWN habría dado dos).
    assert.deepEqual(tratados, ["sin-stream", "ajeno", "siguiente"]);
    const muertos = await dlq("c3");
    assert.deepEqual(muertos.map((m) => [m.data.tipo, m.dlqreason, m.dlqattempts]), [
      ["sin-stream", "permanent", 1],
      ["ajeno", "permanent", 1],
    ]);
    assert.match(muertos[0].dlqerror, /^NOT_PROVISIONED:/);
    assert.match(muertos[1].dlqerror, /^PERMISSION_DENIED:/);
  });

  // ── (d) aislamiento ────────────────────────────────────────────────────────

  test("(d) sin concesión sobre un dominio, un servicio ni recibe sus eventos ni espía el inbox ajeno", { timeout: 30_000 }, async () => {
    await limpiar();
    const { logger } = logs();

    // El espía: un servicio legítimo de OTRO dominio, que intenta leer el de pedidos.
    const chis = await conectarCrudo(broker, "fx-chis");
    const violaciones: Array<{ operation: string; subject: string }> = [];
    void (async () => {
      for await (const s of chis.status()) {
        const v = s.type === "error" ? permissionViolation(s.error) : undefined;
        if (v) violaciones.push(v);
      }
    })();
    const interceptado: string[] = [];
    const espiar = ["_INBOX.>", "_INBOX.fx-fact.>", "pedidos.>", SUBJECT, "dlq.>"];
    // Su propio inbox sí puede, y no se anota: otra conexión del MISMO servicio (el SDK de
    // abajo) responde por ahí, y eso no es espiar a nadie.
    chis.subscribe("_INBOX.fx-chis.>");
    for (const s of espiar) {
      const sub = chis.subscribe(s);
      void (async () => {
        try {
          // Una suscripción DENEGADA no es silenciosa: su iterador lanza la violación.
          for await (const m of sub) interceptado.push(m.subject);
        } catch {
          /* es lo esperado: la violación ya se recoge por `status()` */
        }
      })();
    }
    // Y el espía con la forma de la ACL ANTIGUA: es el control, prueba que espiar HABRÍA funcionado.
    const viejo = await conectarCrudo(broker, "fx-espia-viejo");
    const interceptadoViejo: string[] = [];
    const subViejo = viejo.subscribe("_INBOX.>");
    void (async () => {
      for await (const m of subViejo) interceptadoViejo.push(new TextDecoder().decode(m.data));
    })();
    await chis.flush();
    await viejo.flush();

    // Un consumo legítimo, mientras tanto.
    const fact = await abrir("fx-fact");
    const recibidos: string[] = [];
    await fact.subscribe(SUBJECT, async (ev) => void recibidos.push((ev.data as { tipo: string }).tipo));
    await productor.publish(SUBJECT, { marca: "d", tipo: "SECRETO-DE-PEDIDOS" });
    assert.ok(await esperarHasta(() => recibidos.length === 1, 3000), "el consumidor legítimo no recibió");
    await dormir(400);

    // Con el SDK: suscribirse a un dominio no concedido lanza, y no llega nada.
    const busChis = await abrir("fx-chis", { logger });
    const chismes: string[] = [];
    await assert.rejects(
      busChis.subscribe(SUBJECT, async (ev) => void chismes.push(ev.id)),
      (e: unknown) => e instanceof PermissionDeniedError && e.subject === "$JS.API.STREAM.INFO.EVT_PEDIDOS",
    );
    await productor.publish(SUBJECT, { marca: "d", tipo: "otro" });
    await dormir(400);
    assert.deepEqual(chismes, []);

    // Sin SDK: lo que intenta un espía. Cada intento denegado, y NADA interceptado.
    assert.deepEqual(interceptado, [], "el espía leyó algo");
    for (const s of espiar) {
      assert.ok(
        violaciones.some((v) => v.operation === "subscribe" && v.subject === s),
        `la suscripción a ${s} no fue denegada; denegadas: ${JSON.stringify(violaciones)}`,
      );
    }
    assert.ok(!violaciones.some((v) => v.subject === "_INBOX.fx-chis.>"), "su propio inbox SÍ debe poder");

    // Pedir mensajes del consumidor ajeno, o crearse uno propio sobre el stream: denegado y rápido.
    const t0 = Date.now();
    await assert.rejects(
      chis.request(`$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.${"fx-fact__pedidos_pedido_v1_creado"}`, "{}", { timeout: 2000 }),
      (e: unknown) => permissionViolation(e)?.subject.startsWith("$JS.API.CONSUMER.MSG.NEXT.") === true,
    );
    await assert.rejects(
      chis.request(`$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.espia.${SUBJECT}`, "{}", { timeout: 2000 }),
      (e: unknown) => permissionViolation(e) !== undefined,
    );
    assert.ok(Date.now() - t0 < 1500);

    // El control: con `_INBOX.>` (la ACL antigua) el espía SÍ lee el evento de un dominio que
    // no tiene concedido, aunque el SDK use inbox privado: es el hallazgo H2, y lo que lo
    // evita es la ACL, no el SDK.
    assert.ok(
      interceptadoViejo.some((t) => t.includes("SECRETO-DE-PEDIDOS")),
      "el espía con `_INBOX.>` no vio el evento: el control no demuestra nada",
    );
    await chis.close();
    await viejo.close();
  });

  // ── (e) tenantIsolation: "strict" ──────────────────────────────────────────

  test('(e) tenantIsolation "strict" sigue filtrando bajo ACL exacta', { timeout: 30_000 }, async () => {
    await limpiar();
    const { l: registro, logger } = logs();
    const estricto = await abrir("fx-ten", { tenantId: "acme", tenantIsolation: "strict", logger });
    const abierto = await abrir("fx-tenb", { logger });
    const verEstricto: string[] = [];
    const verAbierto: string[] = [];
    const sub = await estricto.subscribe(SUBJECT, async (ev) => void verEstricto.push(`${ev.tenantid}:${(ev.data as { n: number }).n}`));
    await abierto.subscribe(SUBJECT, async (ev) => void verAbierto.push(`${ev.tenantid}:${(ev.data as { n: number }).n}`));

    await productor.publish(SUBJECT, { n: 1 }, { tenantId: "acme" });
    await productor.publish(SUBJECT, { n: 2 }, { tenantId: "globex" });
    await productor.publish(SUBJECT, { n: 3 }, { tenantId: "acme" });

    assert.ok(
      await esperarHasta(() => verAbierto.length === 3 && verEstricto.length === 2, 5000),
      `estricto=${JSON.stringify(verEstricto)} abierto=${JSON.stringify(verAbierto)}\n${registro.join("\n")}`,
    );
    // El estricto no ve a globex; el abierto (el control) sí: el filtro es lo que cambia, no los permisos.
    assert.deepEqual(verEstricto, ["acme:1", "acme:3"]);
    assert.deepEqual(verAbierto, ["acme:1", "globex:2", "acme:3"]);
    // El de globex no se queda sin confirmar: se descarta con ack, no es un fallo.
    assert.ok(
      await esperarHasta(async () => {
        const i = await jsm.consumers.info("EVT_PEDIDOS", sub.durable);
        return i.num_ack_pending === 0 && i.num_pending === 0 && i.delivered.stream_seq >= 3;
      }, 3000),
    );

    // Y suscribirse sin tenant LANZA, antes de tocar nada del broker.
    const sinTenant = await abrir("fx-tenb", { tenantIsolation: "strict" });
    await assert.rejects(sinTenant.subscribe(SUBJECT, async () => {}), TenantIsolationError);
  });
});
