/**
 * Petición/respuesta contra un nats-server REAL, con los permisos EXACTOS de `rpc.acl`.
 *
 * Es el ciclo del criterio de aceptación de la Fase 3 (PLAN §3.6, 11-request-reply.md §12.9):
 *
 *   una operación atendida por DOS instancias reparte las peticiones (cada una la atiende una sola) →
 *   al parar ambas, quien llama recibe `RPC_UNAVAILABLE` en menos de 100 ms
 *
 * y, **en los dos sentidos**, que quitar un permiso SE NOTA: sin el permiso de llamada falla rápido con `PERMISSION_DENIED`
 * (no con un timeout); un dueño sin permiso de suscripción falla AL ARRANCAR (no arranca mudo: parecería un servicio caído); y
 * sin `allow_responses` el dueño no puede contestar y lo dice en su log. Los permisos se escriben aquí a mano (ver
 * rpc-acl.ts); no los genera `gen-acl.mjs`.
 *
 * Y lo que solo un broker real demuestra: `no responders` en milisegundos (A-56), la suscripción exacta (A-64), el reparto
 * (A-58), la respuesta tardía (A-61), el límite de `max_payload` (A-65), el acuse de un stream (A-67), la parada ordenada
 * (A-62) y la respuesta de más de 1 MiB, que quien responde contesta como error en vez de dejar a quien llama con un timeout.
 *
 * Sin `nats-server` estos tests se SALTAN con aviso, salvo con FLUX_REQUIRE_BROKER=1, donde FALLAN (broker.ts).
 */

import { test, describe, before, after, afterEach } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import type { NatsConnection } from "@nats-io/transport-node";
import { StorageType, type JetStreamManager } from "@nats-io/jetstream";

import {
  ErrorClass,
  InMemoryMetrics,
  PermanentError,
  PermissionDeniedError,
  RetryableError,
  RpcInvalidReplyError,
  RpcPermanentError,
  RpcRetryableError,
  RpcTimeoutError,
  RpcUnavailableError,
  buildRpcRequest,
  classify,
  connect,
  createSigner,
  encodeRpcMessage,
  generateKeyPair,
  rpcDataschema,
  type FluxBus,
  type FluxEvent,
  type Responder,
  type RpcHandlerContext,
} from "../../src/index.js";
import { permisosRpc, usuarioRpc, type PermisosRpc, type ServicioRpc } from "./rpc-acl.js";
import { conectarCrudo, conectarOps, describeBroker, dormir, esperarHasta, startBroker, type Broker } from "./broker.js";

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

const OP = "clientes.cliente.v1.calcular-descuento";
const OBTENER = "clientes.cliente.v1.obtener";
const TARIFA = "tarifas.tarifa.v1.calcular";
const SUBJECT = `rpc.${OP}`;
const PETICION = { clienteId: "cli-1", totalCents: 9990, moneda: "EUR" };
const DESCUENTO = { descuentoCents: 500, moneda: "EUR" };

const SERVICIOS: Record<string, ServicioRpc> = {
  // Quien llama: publica en el subject EXACTO de cada operación concedida.
  "facturacion-api": { service: "facturacion-api", calls: [OP, OBTENER] },
  // El DUEÑO del dominio `clientes`: se suscribe a `rpc.clientes.>` y contesta con `allow_responses` acotado.
  // Además llama a `tarifas` (la cadena de dos saltos).
  "crm-api": { service: "crm-api", owns: ["clientes"], calls: [TARIFA] },
  "tarifas-api": { service: "tarifas-api", owns: ["tarifas"] },
  // Dueño de OTRO dominio: no puede atender `clientes` aunque su código lo intente.
  "ajeno-api": { service: "ajeno-api", owns: ["otros"] },
  // Sin concesión de llamada.
  "sin-calls-api": { service: "sin-calls-api" },
  // Dueño SIN `allow_responses`: no puede contestar (A-68). Y con el de por defecto de NATS (`true`): una respuesta, sin caducidad.
  "sin-allow-api": { service: "sin-allow-api", owns: ["clientes"], allowResponses: "ausente" },
  "allow-true-api": { service: "allow-true-api", owns: ["clientes"], allowResponses: true },
  // Un espía con inbox propio: no debe poder leer las respuestas ajenas (A-71).
  "espia-api": { service: "espia-api" },
};

/** Un dueño sin el permiso de SUSCRIPCIÓN a `rpc.clientes.>`: parece un servicio caído (A-73). */
const SIN_SUSCRIPCION: PermisosRpc = (() => {
  const p = permisosRpc({ service: "sin-sub-api", owns: ["clientes"] });
  return { ...p, subscribe: p.subscribe.filter((s) => !s.startsWith("rpc.")) };
})();

describe("los permisos del test de petición/respuesta encajan en protocol.json (rpc.acl + security.acl)", () => {
  const re = (s: string) => new RegExp(s);
  const acl = protocolo.rpc.acl;
  const todos = Object.values(SERVICIOS).map((s) => permisosRpc(s));

  test("cada permiso de PUBLICACIÓN es `$JS.API.INFO` (de connect()) o el subject EXACTO de una operación (callerPublish)", () => {
    for (const p of new Set(todos.flatMap((t) => t.publish))) {
      assert.ok(p === "$JS.API.INFO" || re(acl.callerPublish).test(p), `«${p}» no encaja en rpc.acl.callerPublish`);
    }
  });

  test("ninguno es `rpc.>`, `rpc.<dominio>.>` ni un comodín por token (callerNeverPublish)", () => {
    for (const p of new Set(todos.flatMap((t) => t.publish))) {
      assert.ok(!acl.callerNeverPublish.includes(p), p);
      assert.ok(!/[*>]/.test(p), `«${p}» lleva un comodín`);
    }
    // CONTROL: el patrón de verdad rechaza los que la spec prohíbe, así que el test de arriba sabe fallar
    for (const malo of acl.callerNeverPublish as string[]) assert.ok(!re(acl.callerPublish).test(malo), malo);
  });

  test("la suscripción de un dueño es `rpc.<dominio>.>` (ownerSubscribe), nunca `rpc.>`; la de todos, su inbox PRIVADO", () => {
    for (const s of Object.values(SERVICIOS)) {
      const subs = permisosRpc(s).subscribe;
      assert.ok(subs.includes(`_INBOX.${s.service}.>`), `${s.service}: sin su inbox privado`);
      assert.ok(!subs.includes("_INBOX.>"));
      for (const x of subs.filter((y) => y.startsWith("rpc."))) {
        assert.match(x, re(acl.ownerSubscribe));
        assert.ok(!acl.ownerNeverSubscribe.includes(x), x);
      }
    }
    for (const malo of acl.ownerNeverSubscribe as string[]) assert.ok(!re(acl.ownerSubscribe).test(malo), malo);
  });

  test("ningún permiso de PUBLICACIÓN sobre inboxes (ownerPublishOnInboxes: never): lo da `allow_responses`", () => {
    assert.equal(acl.ownerPublishOnInboxes, "never");
    for (const t of todos) assert.ok(!t.publish.some((p) => p.startsWith("_INBOX")));
  });

  test("`allow_responses` solo en los dueños, acotado a `{ max: 1, expires: 30s }` (lo que se RECOMIENDA, A-68)", () => {
    const ar = acl.ownerAllowResponses;
    assert.equal(ar.required, true);
    assert.equal(ar.maxReplies, 1);
    assert.equal(ar.recommendedExpiresSeconds, 30);
    for (const s of Object.values(SERVICIOS)) {
      const p = permisosRpc(s);
      if (!s.owns?.length || s.allowResponses === "ausente") assert.equal(p.allowResponses, undefined, s.service);
      else if (s.allowResponses === true) assert.equal(p.allowResponses, true);
      else assert.deepEqual(p.allowResponses, { max: 1, expires: "30s" }, s.service);
    }
  });

  test("el timeout máximo (25 s) deja `expires: 30s` por encima: una respuesta legítima nunca llega después", () => {
    const expires = Number((permisosRpc(SERVICIOS["crm-api"]).allowResponses as { expires: string }).expires.replace("s", ""));
    assert.ok(protocolo.rpc.timeout.maxMs / 1000 < expires, `${protocolo.rpc.timeout.maxMs} ms frente a expires ${expires} s`);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// El broker
// ═════════════════════════════════════════════════════════════════════════════

function configDelBroker(port: number, store: string, extra = ""): string {
  const usuarios = [
    `{ user: "ops", password: "pw" }`, // IaC: aprovisiona. Sin restricciones.
    ...Object.values(SERVICIOS).map((s) => usuarioRpc(s.service, permisosRpc(s))),
    usuarioRpc("sin-sub-api", SIN_SUSCRIPCION),
    // Un servicio «roto» o hostil que contesta en bruto, sin SDK: lo que el SDK de quien llama debe poder detectar.
    usuarioRpc("rogue", { publish: ["$JS.API.INFO"], subscribe: ["_INBOX.rogue.>", "rpc.clientes.>"], allowResponses: true }),
  ];
  return `listen: 127.0.0.1:${port}
${extra}
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
const UUID_V7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

describeBroker("SDK Node: petición/respuesta bajo ACL exacta", () => {
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

  /** Cuenta cuántas veces se invocó y guarda lo que vio el handler. */
  function contador<T extends object>(resultado: T | (() => T | Promise<T>) = DESCUENTO as unknown as T) {
    const c = {
      n: 0,
      ids: [] as string[],
      contextos: [] as RpcHandlerContext[],
      handler: async (_d: unknown, ctx: RpcHandlerContext) => {
        c.n++;
        c.ids.push(ctx.id);
        c.contextos.push(ctx);
        return typeof resultado === "function" ? resultado() : resultado;
      },
    };
    return c;
  }

  before(async () => {
    broker = await startBroker(({ port, store }) => configDelBroker(port, store));
    ({ nc: opsNc, jsm } = await conectarOps(broker));
  });

  afterEach(async () => {
    for (const b of abiertos.splice(0)) await b.close().catch(() => {});
  });

  after(async () => {
    await opsNc?.close().catch(() => {});
    await broker?.stop();
  });

  // ══════════════════════════════════════════════════════════════════════════
  // El ciclo de aceptación (11-request-reply.md §12.9)
  // ══════════════════════════════════════════════════════════════════════════

  test(
    "CICLO (A-86): dos instancias atienden la operación y REPARTEN las peticiones (cada una la atiende UNA) → al parar ambas, RPC_UNAVAILABLE en < 100 ms",
    { timeout: 60_000 },
    async () => {
      const crm1 = await abrir("crm-api");
      const crm2 = await abrir("crm-api"); // la MISMA cuenta de servicio: dos instancias
      const a = contador();
      const b = contador();
      const r1 = await crm1.respond(OP, a.handler);
      const r2 = await crm2.respond(OP, b.handler);
      const llama = await abrir("facturacion-api");

      const N = 200;
      for (let i = 0; i < N; i += 20) {
        const lote = await Promise.all(
          Array.from({ length: 20 }, (_, k) => llama.request<typeof DESCUENTO>(OP, { ...PETICION, totalCents: 1000 + i + k }, { timeoutMs: 5000 })),
        );
        for (const r of lote) assert.deepEqual(r, DESCUENTO);
      }
      assert.equal(a.n + b.n, N, "todas se atendieron, y UNA sola vez cada una (ni se pierde ni se duplica)");
      assert.equal(new Set([...a.ids, ...b.ids]).size, N, "ningún `id` lo vieron las dos instancias");
      assert.ok(a.n >= 50 && b.n >= 50, `el reparto no es parejo: ${a.n} / ${b.n} (al azar, no por turnos: A-58)`);

      // PARAR ambas (la parada ordenada: dejar de escuchar → esperar a lo que está en vuelo)
      await r1.stop();
      // con una instancia menos, todo va a la que queda (A-59)
      const antes = b.n;
      for (let i = 0; i < 20; i++) await llama.request(OP, PETICION);
      assert.equal(b.n - antes, 20, "todo va a la instancia que queda");
      await r2.stop();

      for (let i = 0; i < 5; i++) {
        const t0 = performance.now();
        await assert.rejects(llama.request(OP, PETICION, { timeoutMs: 5000 }), (e: RpcUnavailableError) => {
          assert.ok(e instanceof RpcUnavailableError);
          assert.equal(e.code, "RPC_UNAVAILABLE");
          assert.equal(e.fluxClass, ErrorClass.RETRYABLE);
          return true;
        });
        const ms = performance.now() - t0;
        assert.ok(ms < 100, `RPC_UNAVAILABLE tardó ${ms.toFixed(1)} ms (debe ser < 100 ms, y no esperar al timeout de 5 s)`);
      }
    },
  );

  test("CICLO (cierre): `bus.close()` de las dos instancias también deja a quien llama con RPC_UNAVAILABLE en < 100 ms", async () => {
    const crm1 = await abrir("crm-api");
    const crm2 = await abrir("crm-api");
    await crm1.respond(OP, contador().handler);
    await crm2.respond(OP, contador().handler);
    const llama = await abrir("facturacion-api");
    assert.deepEqual(await llama.request(OP, PETICION), DESCUENTO);
    await Promise.all([crm1.close(), crm2.close()]);
    const t0 = performance.now();
    await assert.rejects(llama.request(OP, PETICION, { timeoutMs: 5000 }), RpcUnavailableError);
    assert.ok(performance.now() - t0 < 100);
  });

  // ══════════════════════════════════════════════════════════════════════════
  // La suscripción exacta, y `no responders` (A-56, A-57, A-64)
  // ══════════════════════════════════════════════════════════════════════════

  test("una operación que el servicio NO atiende da RPC_UNAVAILABLE en ms, aunque su permiso sea `rpc.clientes.>` (suscripción EXACTA, A-64)", async () => {
    const crm = await abrir("crm-api");
    await crm.respond(OBTENER, async () => ({ clienteId: "cli-1", nombre: "Ana", email: "ana@example.com" }));
    const llama = await abrir("facturacion-api");
    // `obtener` la atiende; `calcular-descuento` (también concedida a quien llama) no la atiende NADIE
    assert.equal((await llama.request<{ clienteId: string }>(OBTENER, { clienteId: "cli-1" })).clienteId, "cli-1");
    const t0 = performance.now();
    await assert.rejects(llama.request(OP, PETICION, { timeoutMs: 5000 }), RpcUnavailableError);
    assert.ok(performance.now() - t0 < 100, "con una suscripción a `rpc.clientes.>` el servicio habría tenido interés y esto habría agotado su timeout");
  });

  // ══════════════════════════════════════════════════════════════════════════
  // El sobre en el cable
  // ══════════════════════════════════════════════════════════════════════════

  test("lo que viaja: el sobre completo, sin NINGUNA cabecera de NATS, y la respuesta hereda lo que debe (visto por un espía en bruto)", async () => {
    const crm = await abrir("crm-api");
    await crm.respond(OP, async (d) => ({ descuentoCents: 500, moneda: (d as { moneda: string }).moneda, motivo: "Campaña de otoño" }));
    const llama = await abrir("facturacion-api", { tenantId: "acme" });

    // `ops` ve todo: un suscriptor en bruto al subject de la operación
    const vistas: Array<{ subject: string; reply: string; headers: unknown; data: Uint8Array }> = [];
    const sub = opsNc.subscribe(SUBJECT, { callback: (_e, m) => void vistas.push({ subject: m.subject, reply: m.reply ?? "", headers: m.headers, data: m.data }) });
    await opsNc.flush();
    await llama.request(OP, { ...PETICION, campana: "Otoño" }, { aggregateId: "cli-1" });
    sub.unsubscribe();
    assert.equal(vistas.length, 1);
    assert.equal(vistas[0].headers, undefined, "ninguna cabecera: traceparent, correlationid y tenantid viajan SOLO en el sobre");
    assert.match(vistas[0].reply, /^_INBOX\.facturacion-api\./, "el inbox PRIVADO del servicio, no `_INBOX.>`");
    const pet = JSON.parse(dec.decode(vistas[0].data));
    assert.equal(pet.type, "com.flux.rpc.clientes.cliente.calcular-descuento.request.v1");
    assert.equal(pet.source, "/test/facturacion-api");
    assert.match(pet.id, UUID_V7);
    assert.equal(pet.subject, "cli-1");
    assert.equal(pet.correlationid, pet.id);
    assert.equal(pet.tenantid, "acme");
    assert.equal(Object.keys(pet).at(-1), "data");
    assert.ok(!dec.decode(vistas[0].data).includes("\\u"), "UTF-8 literal: `Otoño`, no `Oto\\u00f1o`");

    // la respuesta, leída en bruto: una petición construida a mano con el sobre de un SDK
    const peticion = encodeRpcMessage(buildRpcRequest({
      operation: OP, tenantId: "globex", aggregateId: "cli-9", id: "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10", source: "/test/crudo", time: new Date().toISOString(),
      correlationid: "01924f91-0000-7000-8000-00000000c0c0", producerversion: "1.0.0", classification: "internal", data: PETICION, dataschema: rpcDataschema(OP, "request"),
    }));
    const m = await opsNc.request(SUBJECT, peticion, { timeout: 2000 });
    assert.equal(m.headers, undefined, "la respuesta tampoco lleva cabeceras");
    const rep = JSON.parse(dec.decode(m.data));
    assert.equal(rep.type, "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1");
    assert.equal(rep.source, "/test/crm-api");
    assert.match(rep.id, UUID_V7);
    assert.notEqual(rep.id, "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10");
    assert.equal(rep.causationid, "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10");
    assert.equal(rep.correlationid, "01924f91-0000-7000-8000-00000000c0c0");
    assert.equal(rep.tenantid, "globex");
    assert.equal(rep.subject, "cli-9");
    assert.equal(rep.partitionkey, "cli-9");
    assert.equal(Object.keys(rep).at(-1), "data");
    assert.ok(dec.decode(m.data).includes("Campaña de otoño"), "UTF-8 literal en la respuesta");
  });

  test("en una cadena de dos saltos el `correlationid` es el de la primera petición y cada `causationid` apunta a su padre", async () => {
    const crm = await abrir("crm-api");
    const tarifas = await abrir("tarifas-api");
    const enTarifas = contador({ precioCents: 100 });
    await tarifas.respond(TARIFA, enTarifas.handler);
    const enCrm = contador(async () => {
      await crm.request(TARIFA, { sku: "ABC-1" });
      return DESCUENTO;
    });
    await crm.respond(OP, enCrm.handler);
    const llama = await abrir("facturacion-api", { tenantId: "acme" });
    await llama.request(OP, PETICION);
    const salto1 = enCrm.contextos[0].event;
    const salto2 = enTarifas.contextos[0].event;
    assert.equal(salto1.correlationid, salto1.id, "nace en la llamada de facturación");
    assert.equal(salto2.correlationid, salto1.correlationid, "la cadena se sigue con UN correlationid");
    assert.equal(salto2.causationid, salto1.id, "el causationid del segundo salto es el id de la petición que lo causó");
    assert.equal(salto2.tenantid, "acme", "el tenant se propaga sin modificar");
    assert.equal(salto2.source, "/test/crm-api");
  });

  // ══════════════════════════════════════════════════════════════════════════
  // Los errores
  // ══════════════════════════════════════════════════════════════════════════

  test("el error del handler cruza el broker con su clase y su código; un handler de eventos que lo deja escapar hereda la clasificación", async () => {
    const crm = await abrir("crm-api", logs());
    let modo: "retryable" | "permanent" | "desconocido" = "retryable";
    await crm.respond(OP, async () => {
      if (modo === "retryable") throw new RetryableError("el servicio de tarifas no responde", { code: "HTTP_503", retryAfterMs: 5000 });
      if (modo === "permanent") throw new PermanentError("el pedido ya está cancelado", { code: "PEDIDO_YA_CANCELADO" });
      throw new Error("detalle interno: postgres://admin:hunter2@db");
    });
    const llama = await abrir("facturacion-api");
    await assert.rejects(llama.request(OP, PETICION), (e: RpcRetryableError) => {
      assert.ok(e instanceof RetryableError);
      assert.deepEqual([e.code, e.message], ["HTTP_503", "el servicio de tarifas no responde"]);
      assert.deepEqual([classify(e).class, classify(e).retryAfterMs], [ErrorClass.RETRYABLE, 5000]);
      return true;
    });
    modo = "permanent";
    await assert.rejects(llama.request(OP, PETICION), (e: RpcPermanentError) => e instanceof PermanentError && e.code === "PEDIDO_YA_CANCELADO" && classify(e).class === ErrorClass.PERMANENT);
    modo = "desconocido";
    await assert.rejects(llama.request(OP, PETICION), (e: RpcPermanentError) => {
      assert.equal(e.code, "UNKNOWN");
      assert.equal(e.message, "error interno del servicio");
      assert.ok(!e.message.includes("hunter2"));
      return true;
    });
  });

  test("RPC_TIMEOUT: el responder ejecuta pero tarda más que el timeout; la respuesta TARDÍA llega al inbox y se ignora, y la conexión sigue sana (A-61)", async () => {
    const crm = await abrir("crm-api");
    const c = contador(async () => (await dormir(500), DESCUENTO));
    await crm.respond(OP, c.handler);
    const llama = await abrir("facturacion-api");
    const t0 = performance.now();
    await assert.rejects(llama.request(OP, PETICION, { timeoutMs: 150 }), (e: RpcTimeoutError) => {
      assert.ok(e instanceof RpcTimeoutError);
      assert.equal(e.code, "RPC_TIMEOUT");
      assert.match(e.message, /PUEDE haberse ejecutado/);
      return true;
    });
    const ms = performance.now() - t0;
    assert.ok(ms >= 140 && ms < 400, `tardó ${ms.toFixed(0)} ms`);
    await dormir(450); // llega la respuesta tardía
    assert.equal(c.n, 1, "la operación SE EJECUTÓ aunque quien llama viera un timeout");
    // y nada se rompió: la siguiente llamada, con un timeout holgado, funciona
    assert.deepEqual(await llama.request(OP, PETICION, { timeoutMs: 3000 }), DESCUENTO);
  });

  test("`retries` tras un timeout: el MISMO id llega dos veces al responder (que deduplica), y la llamada acaba bien", async () => {
    const crm = await abrir("crm-api");
    let n = 0;
    const c = contador(async () => (++n === 1 ? (await dormir(400), DESCUENTO) : DESCUENTO));
    await crm.respond(OP, c.handler);
    const llama = await abrir("facturacion-api");
    assert.deepEqual(await llama.request(OP, PETICION, { timeoutMs: 120, retries: 2 }), DESCUENTO);
    await dormir(350);
    assert.equal(c.n, 2, "el responder recibió la orden DOS veces");
    assert.equal(c.ids[0], c.ids[1], "con el MISMO id: así puede reconocer la repetida");
    assert.ok(c.contextos[0].time === c.contextos[1].time, "y el mismo `time`: los mismos bytes");
  });

  test("`retries` ante RPC_UNAVAILABLE: el servicio arranca entre intentos y la llamada acaba bien", async () => {
    const llama = await abrir("facturacion-api");
    const c = contador();
    setTimeout(() => void abrir("crm-api").then((crm) => crm.respond(OP, c.handler)), 150);
    assert.deepEqual(await llama.request(OP, PETICION, { retries: 6 }), DESCUENTO);
    assert.equal(c.n, 1);
  });

  // ══════════════════════════════════════════════════════════════════════════
  // Las cinco comprobaciones de la respuesta, contra un responder en bruto
  // ══════════════════════════════════════════════════════════════════════════

  describe("quien llama valida que lo que llega es la respuesta a ESTA petición (§4.3)", () => {
    /** Un servicio `rogue` que contesta en bruto: `fn` recibe la petición leída y devuelve lo que contesta. */
    async function rogue(fn: (peticion: FluxEvent & Record<string, any>) => string | Record<string, unknown>) {
      const nc = await conectarCrudo(broker, "rogue");
      const sub = nc.subscribe(SUBJECT, {
        callback: (_e, m) => {
          const p = JSON.parse(dec.decode(m.data));
          const r = fn(p);
          m.respond(enc.encode(typeof r === "string" ? r : JSON.stringify(r)));
        },
      });
      await nc.flush();
      return async () => {
        sub.unsubscribe();
        await nc.close();
      };
    }
    const buena = (p: Record<string, any>, extra: Record<string, unknown> = {}) => ({
      specversion: "1.0", id: "01924f91-5d0f-7e03-9b6a-1c2d3e4f5a67", source: "/test/rogue",
      type: "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1", time: new Date().toISOString(), datacontenttype: "application/json",
      dataschema: rpcDataschema(OP, "reply"), correlationid: p.correlationid, tenantid: p.tenantid, producerversion: "1", dataclassification: "internal",
      causationid: p.id, data: DESCUENTO, ...extra,
    });

    test("CONTROL: una respuesta bien formada de un responder en bruto pasa", async () => {
      const para = await rogue((p) => buena(p));
      const llama = await abrir("facturacion-api");
      assert.deepEqual(await llama.request(OP, PETICION), DESCUENTO);
      await para();
    });

    test("el `causationid` equivocado, otro `type`, otro `correlationid` o `tenantid`, el error mal formado o no-JSON → RPC_INVALID_REPLY (PERMANENT)", async () => {
      const casos: Array<[string, (p: Record<string, any>) => string | Record<string, unknown>]> = [
        ["causationid de otra petición", (p) => buena(p, { causationid: "01924f91-0000-7000-8000-000000000000" })],
        ["la respuesta de OTRA operación", (p) => buena(p, { type: "com.flux.rpc.clientes.cliente.obtener.reply.v1" })],
        ["otro correlationid", (p) => buena(p, { correlationid: "otro" })],
        ["otro tenantid", (p) => buena(p, { tenantid: "globex" })],
        ["un error con class `poison`", (p) => buena(p, { type: "com.flux.rpc.error.v1", dataschema: "https://schemas.internal/system/rpc/error/1.0.0.json", data: { class: "poison", code: "X", message: "m" } })],
        ["no es JSON", () => "esto no es un CloudEvent"],
      ];
      const llama = await abrir("facturacion-api");
      for (const [nombre, fn] of casos) {
        const para = await rogue(fn);
        await assert.rejects(llama.request(OP, PETICION, { timeoutMs: 3000 }), (e: RpcInvalidReplyError) => {
          assert.ok(e instanceof RpcInvalidReplyError, `${nombre}: ${e.name}: ${e.message}`);
          assert.equal(e.code, "RPC_INVALID_REPLY");
          assert.equal(e.fluxClass, ErrorClass.PERMANENT);
          return true;
        }, nombre);
        await para();
      }
    });
  });

  describe("la excepción única de la comprobación 4 (§4.3): el error de una petición ilegible, con un responder en bruto contra el broker", () => {
    /** Un responder en bruto: contesta a lo que llegue con `fn(petición)`. */
    async function rogueCrudo(fn: (peticion: FluxEvent & Record<string, any>) => Record<string, unknown>) {
      const nc = await conectarCrudo(broker, "rogue");
      const sub = nc.subscribe(SUBJECT, { callback: (_e, m) => void m.respond(enc.encode(JSON.stringify(fn(JSON.parse(dec.decode(m.data)))))) });
      await nc.flush();
      return async () => { sub.unsubscribe(); await nc.close(); };
    }
    // Lo que contesta un SDK a una petición que no pudo leer (§5.2): `correlationid` = su propio `id`, `tenantid: system`, `causationid` = el `id` de la petición.
    const ilegible = (p: Record<string, any>, extra: Record<string, unknown> = {}) => ({
      specversion: "1.0", id: "01924f91-5d13-7b85-a3e1-6a7b8c9d0e22", source: "/test/rogue", type: "com.flux.rpc.error.v1",
      time: new Date().toISOString(), datacontenttype: "application/json", dataschema: "https://schemas.internal/system/rpc/error/1.0.0.json",
      correlationid: "01924f91-5d13-7b85-a3e1-6a7b8c9d0e22", tenantid: "system", producerversion: "1", dataclassification: "internal",
      causationid: p.id, data: { class: "permanent", code: "MALFORMED_JSON", message: "la petición no es un CloudEvent válido de flux" }, ...extra,
    });

    test("el error de una petición ilegible llega como PermanentError con el código del sobre (MALFORMED_JSON), NO como RPC_INVALID_REPLY", async () => {
      const para = await rogueCrudo((p) => ilegible(p));
      const llama = await abrir("facturacion-api");
      await assert.rejects(llama.request(OP, PETICION, { timeoutMs: 3000 }), (e: RpcPermanentError) => {
        assert.ok(e instanceof RpcPermanentError, `${e.name}: ${e.message}`);
        assert.equal(e.code, "MALFORMED_JSON");
        assert.equal(e.fluxClass, ErrorClass.PERMANENT);
        return true;
      });
      await para();
    });

    test("CONTROL: la excepción es estrecha: sin `causationid` de la petición, con otro `correlationid` o con un tenant ajeno sigue siendo RPC_INVALID_REPLY", async () => {
      const llama = await abrir("facturacion-api");
      const casos: Array<[string, (p: Record<string, any>) => Record<string, unknown>]> = [
        ["causationid de otra petición", (p) => ilegible(p, { causationid: "01924f91-0000-7000-8000-000000000000" })],
        ["correlationid que no es el propio", (p) => ilegible(p, { correlationid: "otro-flujo" })],
        ["tenant ajeno (ni `system` ni el de la petición)", (p) => ilegible(p, { tenantid: "globex" })],
      ];
      for (const [nombre, fn] of casos) {
        const para = await rogueCrudo(fn);
        await assert.rejects(llama.request(OP, PETICION, { timeoutMs: 3000 }), (e: RpcInvalidReplyError) => {
          assert.ok(e instanceof RpcInvalidReplyError, `${nombre}: ${e.name}: ${e.message}`);
          return true;
        }, nombre);
        await para();
      }
    });
  });

  test("A-67, A-88: un STREAM que captura `rpc.>` contesta con su acuse → RPC_INVALID_REPLY que apunta a `flux doctor`", async () => {
    await jsm.streams.add({ name: "TRAMPA", subjects: ["rpc.clientes.>"], storage: StorageType.Memory });
    try {
      const llama = await abrir("facturacion-api");
      await assert.rejects(llama.request(OP, PETICION, { timeoutMs: 3000 }), (e: RpcInvalidReplyError) => {
        assert.ok(e instanceof RpcInvalidReplyError);
        assert.match(e.message, /flux doctor/);
        assert.match(e.message, /rpc\.>/);
        return true;
      });
      // y el stream guardó la petición, datos incluidos: por eso `flux doctor` es obligatorio (§8)
      const info = await jsm.streams.info("TRAMPA");
      assert.ok(info.state.messages >= 1);
    } finally {
      await jsm.streams.delete("TRAMPA");
    }
  });

  // ══════════════════════════════════════════════════════════════════════════
  // Tamaño (A-65)
  // ══════════════════════════════════════════════════════════════════════════

  test("una RESPUESTA de más de 1 MiB: quien responde contesta RPC_MESSAGE_TOO_LARGE y quien llama lo ve en ms, NO un timeout (A-65)", async () => {
    const crm = await abrir("crm-api", logs());
    await crm.respond(OP, () => ({ relleno: "x".repeat(1_100_000) }) as never);
    const llama = await abrir("facturacion-api");
    const t0 = performance.now();
    await assert.rejects(llama.request(OP, PETICION, { timeoutMs: 5000 }), (e: RpcPermanentError) => {
      assert.ok(e instanceof PermanentError);
      assert.equal(e.code, "RPC_MESSAGE_TOO_LARGE");
      assert.match(e.message, /1048576/);
      return true;
    });
    assert.ok(performance.now() - t0 < 1000, "falló rápido: la librería habría lanzado EN QUIEN RESPONDE y quien llama habría esperado su timeout");
  });

  test("una PETICIÓN de más de 1 MiB falla ANTES de salir (RPC_MESSAGE_TOO_LARGE) y EXACTAMENTE 1 MiB, de ida y de vuelta, atraviesa el broker", async () => {
    const crm = await abrir("crm-api");
    const c = contador();
    await crm.respond(OP, c.handler);
    const llama = await abrir("facturacion-api");
    const vacia = encodeRpcMessage(buildRpcRequest({ operation: OP, tenantId: "system", id: "0".repeat(36), source: "/test/facturacion-api", time: new Date().toISOString(),
      correlationid: "0".repeat(36), producerversion: "1.0.0", classification: "internal", data: { relleno: "" }, dataschema: rpcDataschema(OP, "request") })).byteLength;
    await assert.rejects(llama.request(OP, { relleno: "x".repeat(1_048_577 - vacia) }), (e: { code?: string; bytes?: number; limit?: number }) => e.code === "RPC_MESSAGE_TOO_LARGE" && e.bytes === 1_048_577 && e.limit === 1_048_576);
    assert.equal(c.n, 0, "no salió");
    // exactamente 1 MiB: el broker lo entrega
    assert.deepEqual(await llama.request(OP, { relleno: "x".repeat(1_048_576 - vacia) }, { timeoutMs: 10_000 }), DESCUENTO);
    assert.equal(c.n, 1);
  });

  // ══════════════════════════════════════════════════════════════════════════
  // Parada ordenada y concurrencia
  // ══════════════════════════════════════════════════════════════════════════

  test("PARADA ORDENADA (A-62): con un handler de 400 ms, parar a los 100 ms deja que la petición en vuelo TERMINE y la siguiente falla en ms con RPC_UNAVAILABLE", async () => {
    const crm = await abrir("crm-api");
    const c = contador(async () => (await dormir(400), DESCUENTO));
    const r: Responder = await crm.respond(OP, c.handler);
    const llama = await abrir("facturacion-api");
    const enVuelo = llama.request(OP, PETICION, { timeoutMs: 3000 });
    await dormir(100);
    const parada = r.stop();
    await dormir(30);
    const t0 = performance.now();
    await assert.rejects(llama.request(OP, PETICION, { timeoutMs: 3000 }), RpcUnavailableError);
    assert.ok(performance.now() - t0 < 100, "la petición NUEVA no cae en una instancia que se va");
    assert.deepEqual(await enVuelo, DESCUENTO, "la que estaba en vuelo terminó con su respuesta");
    await parada;
    assert.equal(c.n, 1);
  });

  test("`bus.close()` con un handler en vuelo NO pierde su respuesta (cerrar la conexión de golpe sí: A-62, A-83)", async () => {
    const crm = await abrir("crm-api");
    await crm.respond(OP, contador(async () => (await dormir(300), DESCUENTO)).handler);
    const llama = await abrir("facturacion-api");
    const enVuelo = llama.request(OP, PETICION, { timeoutMs: 3000 });
    await dormir(100);
    await crm.close();
    assert.deepEqual(await enVuelo, DESCUENTO);
  });

  test("A-89: `maxConcurrent`: con 4 y 12 peticiones de 100 ms nunca hay más de 4 handlers a la vez, y las 12 se contestan", async () => {
    const crm = await abrir("crm-api");
    let dentro = 0;
    let maximo = 0;
    await crm.respond(OP, async () => {
      dentro++;
      maximo = Math.max(maximo, dentro);
      await dormir(100);
      dentro--;
      return DESCUENTO;
    }, { maxConcurrent: 4 });
    const llama = await abrir("facturacion-api");
    const t0 = performance.now();
    const rs = await Promise.all(Array.from({ length: 12 }, () => llama.request(OP, PETICION, { timeoutMs: 10_000 })));
    assert.equal(rs.length, 12);
    assert.equal(maximo, 4);
    assert.ok(performance.now() - t0 >= 290, "tres tandas de 100 ms: el exceso esperó en la cola");
  });

  test("A-89: por defecto son 256 handlers a la vez: 300 peticiones de 50 ms → pico de 256, las 300 contestadas (el exceso espera en la cola, sin perderse)", async () => {
    const crm = await abrir("crm-api");
    let dentro = 0;
    let maximo = 0;
    await crm.respond(OP, async () => {
      dentro++;
      maximo = Math.max(maximo, dentro);
      await dormir(50);
      dentro--;
      return DESCUENTO;
    });
    const llama = await abrir("facturacion-api");
    const rs = await Promise.allSettled(Array.from({ length: 300 }, () => llama.request(OP, PETICION, { timeoutMs: 10_000 })));
    assert.equal(rs.filter((r) => r.status === "fulfilled").length, 300, JSON.stringify(rs.filter((r) => r.status === "rejected").slice(0, 2)));
    assert.equal(maximo, 256);
  });

  // ══════════════════════════════════════════════════════════════════════════
  // La firma y la ventana de 300 s, contra un broker real
  // ══════════════════════════════════════════════════════════════════════════

  test("firma `require` a los dos lados: ida y vuelta firmadas; una petición vieja, o sin firma, la rechaza quien responde con su error", async () => {
    const f = generateKeyPair();
    const c = generateKeyPair();
    const publicKeys = { "facturacion-api-1": f.publicKeyPem, "crm-api-1": c.publicKeyPem };
    const crm = await abrir("crm-api", { signing: { privateKeyPem: c.privateKeyPem, keyId: "crm-api-1", publicKeys, verify: "require" } });
    const cuenta = contador();
    await crm.respond(OP, cuenta.handler);
    const llama = await abrir("facturacion-api", { signing: { privateKeyPem: f.privateKeyPem, keyId: "facturacion-api-1", publicKeys, verify: "require" } });
    assert.deepEqual(await llama.request(OP, PETICION), DESCUENTO);

    const firmante = createSigner({ privateKeyPem: f.privateKeyPem, keyId: "facturacion-api-1" })!;
    const cruda = (desplazaMs: number) => encodeRpcMessage(firmante.sign(buildRpcRequest({
      operation: OP, tenantId: "acme", id: "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10", source: "/test/crudo", time: new Date(Date.now() + desplazaMs).toISOString(),
      correlationid: "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10", producerversion: "1", classification: "internal", data: PETICION, dataschema: rpcDataschema(OP, "request") })));
    const respuesta = async (bytes: Uint8Array) => JSON.parse(dec.decode((await opsNc.request(SUBJECT, bytes, { timeout: 2000 })).data));
    const dentro = await respuesta(cruda(-299_000));
    assert.equal(dentro.type, "com.flux.rpc.clientes.cliente.calcular-descuento.reply.v1");
    for (const desplaza of [-301_000, 301_000]) {
      const fuera = await respuesta(cruda(desplaza));
      assert.equal(fuera.type, "com.flux.rpc.error.v1");
      assert.deepEqual([fuera.data.class, fuera.data.code], ["permanent", "STALE_REQUEST"]);
      assert.equal(fuera.signkeyid, "crm-api-1", "el error va firmado");
    }
    const sinFirma = await respuesta(encodeRpcMessage(buildRpcRequest({
      operation: OP, tenantId: "acme", id: "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e11", source: "/test/crudo", time: new Date().toISOString(),
      correlationid: "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e11", producerversion: "1", classification: "internal", data: PETICION, dataschema: rpcDataschema(OP, "request") })));
    assert.equal(sinFirma.data.code, "MISSING_SIGNATURE");
    assert.equal(cuenta.n, 2, "el handler solo vio la llamada del SDK y la petición firmada dentro de la ventana");
  });

  // ══════════════════════════════════════════════════════════════════════════
  // «Quitar un permiso se NOTA» (11-request-reply.md §7.1, §12.6)
  // ══════════════════════════════════════════════════════════════════════════

  describe("quitar un permiso se nota — con el permiso, funciona; sin él, falla de forma visible y con el error que toca", () => {
    test("CONTROL: con el permiso de llamada, `facturacion-api` llama; con `allow_responses` acotado, `crm-api` contesta", async () => {
      const crm = await abrir("crm-api");
      await crm.respond(OP, contador().handler);
      assert.deepEqual(await (await abrir("facturacion-api")).request(OP, PETICION), DESCUENTO);
    });

    test("sin permiso de PUBLICACIÓN sobre la operación: PERMISSION_DENIED en ms (no un timeout), nombrando el subject EXACTO", async () => {
      const crm = await abrir("crm-api");
      await crm.respond(OP, contador().handler);
      const { l, logger } = logs();
      const sin = await abrir("sin-calls-api", { logger });
      for (let i = 0; i < 3; i++) {
        const t0 = performance.now();
        await assert.rejects(sin.request(OP, PETICION, { timeoutMs: 5000 }), (e: PermissionDeniedError) => {
          assert.ok(e instanceof PermissionDeniedError);
          assert.equal(e.code, "PERMISSION_DENIED");
          assert.equal(e.fluxClass, ErrorClass.PERMANENT, "reintentar una denegación da la misma denegación");
          assert.equal(e.operation, "publish");
          assert.equal(e.subject, SUBJECT);
          assert.match(e.message, /calls/, "dice dónde se arregla");
          return true;
        });
        assert.ok(performance.now() - t0 < 100, `la denegación tardó ${(performance.now() - t0).toFixed(0)} ms`);
      }
      assert.ok(l.some((x) => x.includes(SUBJECT) && /denegó publicar/.test(x)), "además se registró en el logger");
      // CINCO a la vez al mismo subject denegado: cada `-ERR` rechaza a una petición distinta, ninguna espera su timeout (A-82)
      const t1 = performance.now();
      const cinco = await Promise.allSettled(Array.from({ length: 5 }, () => sin.request(OP, PETICION, { timeoutMs: 5000 })));
      assert.ok(cinco.every((r) => r.status === "rejected" && r.reason instanceof PermissionDeniedError), JSON.stringify(cinco.map((r) => r.status)));
      assert.ok(performance.now() - t1 < 200, `las cinco tardaron ${(performance.now() - t1).toFixed(0)} ms`);
    });

    test("una operación concedida a otro NO la puede llamar quien solo tiene la suya: `rpc.<dominio>.>` no es un permiso de llamada", async () => {
      const llama = await abrir("facturacion-api");
      await assert.rejects(llama.request("clientes.cliente.v1.crear", {}), (e: PermissionDeniedError) => e instanceof PermissionDeniedError && e.subject === "rpc.clientes.cliente.v1.crear");
    });

    test("un dueño SIN permiso de suscripción: `respond()` FALLA con PERMISSION_DENIED al arrancar (no arranca mudo), y quien llama ve RPC_UNAVAILABLE (A-73)", async () => {
      const dueno = await abrir("sin-sub-api");
      await assert.rejects(dueno.respond(OP, contador().handler), (e: PermissionDeniedError) => {
        assert.ok(e instanceof PermissionDeniedError);
        assert.equal(e.operation, "subscribe");
        assert.equal(e.subject, SUBJECT);
        assert.match(e.message, /RPC_UNAVAILABLE/);
        return true;
      });
      await assert.rejects((await abrir("facturacion-api")).request(OP, PETICION), RpcUnavailableError);
    });

    test("un dueño de OTRO dominio no puede atender `clientes`: PERMISSION_DENIED (solo el dueño responde: la ACL lo hace cumplir)", async () => {
      const ajeno = await abrir("ajeno-api");
      await assert.rejects(ajeno.respond(OP, contador().handler), PermissionDeniedError);
      await assert.doesNotReject(ajeno.respond("otros.cosa.v1.hacer", contador().handler), "en SU dominio sí puede");
    });

    test("A-68, A-87: un dueño SIN `allow_responses` no puede contestar: quien llama ve RPC_TIMEOUT, y el dueño LO DICE en su log (sin inundarlo)", async () => {
      const { l, logger } = logs();
      const dueno = await abrir("sin-allow-api", { logger });
      const c = contador();
      await dueno.respond(OP, c.handler);
      const llama = await abrir("facturacion-api");
      for (let i = 0; i < 3; i++) await assert.rejects(llama.request(OP, PETICION, { timeoutMs: 250 }), RpcTimeoutError);
      assert.equal(c.n, 3, "el handler SE EJECUTÓ las tres veces: la orden se aplicó y nadie lo supo");
      assert.ok(await esperarHasta(() => l.some((x) => /allow_responses/.test(x)), 2000), `el dueño no avisó: ${JSON.stringify(l)}`);
      assert.equal(l.filter((x) => /allow_responses/.test(x)).length, 1, "un aviso por servicio, no uno por petición");
      assert.match(l.find((x) => /allow_responses/.test(x))!, /_INBOX\.sin-allow-api\.>|_INBOX\.facturacion-api\.>/);
    });

    test("`allow_responses: true` (el de NATS) también contesta, pero admite UNA respuesta y no caduca: se RECOMIENDA acotarlo (A-68)", async () => {
      const dueno = await abrir("allow-true-api");
      await dueno.respond(OP, contador().handler);
      assert.deepEqual(await (await abrir("facturacion-api")).request(OP, PETICION), DESCUENTO);
    });

    test("el inbox es PRIVADO (A-71): un servicio sin concesión no puede leer las respuestas de `facturacion-api`", async () => {
      const espia = await conectarCrudo(broker, "espia-api");
      const errores: string[] = [];
      (async () => { for await (const s of espia.status()) if (s.type === "error") errores.push(String((s as { error: Error }).error.message)); })();
      const sub = espia.subscribe("_INBOX.facturacion-api.>");
      (async () => { try { for await (const _m of sub) { /* nada */ } } catch { /* PermissionViolationError: lo esperado */ } })();
      await espia.flush();
      await dormir(50);
      assert.ok(errores.some((e) => /Permissions Violation for Subscription to "_INBOX\.facturacion-api\.>"/.test(e)), JSON.stringify(errores));
      await espia.close();
    });
  });

  // ══════════════════════════════════════════════════════════════════════════
  // Lo que la librería hace y el SDK sabe (A-81, A-84, A-85)
  // ══════════════════════════════════════════════════════════════════════════

  test("A-81: una petición SIN subject de respuesta (`nats pub` a mano) llega con `reply` vacío y NO se atiende: el handler no se invoca y se cuenta `poison`", async () => {
    const ms = new InMemoryMetrics();
    const { l, logger } = logs();
    const poison: string[] = [];
    const crm = await abrir("crm-api", { metrics: ms, logger, onPoison: ({ subject }: { subject: string }) => void poison.push(subject) });
    const c = contador();
    await crm.respond(OP, c.handler);
    const cuerpo = encodeRpcMessage(buildRpcRequest({
      operation: OP, tenantId: "acme", id: "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10", source: "/test/crudo", time: new Date().toISOString(),
      correlationid: "01924f91-5d0b-7c1a-8e22-9a4b3c2d1e10", producerversion: "1", classification: "internal", data: PETICION, dataschema: rpcDataschema(OP, "request") }));
    opsNc.publish(SUBJECT, cuerpo); // sin `reply`
    await opsNc.flush();
    assert.ok(await esperarHasta(() => poison.length === 1, 2000), "no se contó como POISON");
    assert.equal(c.n, 0, "ejecutar a ciegas aplicaría una orden que nadie espera");
    assert.equal(ms.snapshot().counters.get(`flux_rpc_server_requests_total{operation="${OP}",outcome="poison"}`), 1);
    assert.ok(l.some((x) => /SIN subject de respuesta/.test(x)));
    // y sigue atendiendo
    assert.deepEqual(await (await abrir("facturacion-api")).request(OP, PETICION), DESCUENTO);
  });

  test("A-84: el límite es el `max_payload` que ANUNCIA el broker (8192 aquí): exactamente eso pasa, un byte más es RPC_MESSAGE_TOO_LARGE, de ida y de vuelta", async () => {
    const pequeno = await startBroker(({ port, store }) => configDelBroker(port, store, "max_payload: 8192"));
    const abrirEn = async (service: string) => {
      const b = await connect({ ...base(service), servers: pequeno.url });
      abiertos.push(b);
      return b;
    };
    try {
      const crm = await abrirEn("crm-api");
      let respuesta: object = DESCUENTO;
      await crm.respond(OP, () => respuesta as never);
      const llama = await abrirEn("facturacion-api");
      const vacia = encodeRpcMessage(buildRpcRequest({ operation: OP, tenantId: "system", id: "0".repeat(36), source: "/test/facturacion-api", time: new Date().toISOString(),
        correlationid: "0".repeat(36), producerversion: "1.0.0", classification: "internal", data: { relleno: "" }, dataschema: rpcDataschema(OP, "request") })).byteLength;
      // la PETICIÓN: exactamente 8192 pasa; 8193 no sale
      assert.deepEqual(await llama.request(OP, { relleno: "x".repeat(8192 - vacia) }), DESCUENTO);
      await assert.rejects(llama.request(OP, { relleno: "x".repeat(8193 - vacia) }), (e: { code?: string; bytes?: number; limit?: number }) => e.code === "RPC_MESSAGE_TOO_LARGE" && e.bytes === 8193 && e.limit === 8192);
      // la RESPUESTA: más de 8192 → quien responde contesta el error (la librería lanzaría en él, A-65) y quien llama lo ve en ms
      respuesta = { relleno: "x".repeat(9000) };
      const t0 = performance.now();
      await assert.rejects(llama.request(OP, PETICION, { timeoutMs: 5000 }), (e: { code?: string; message: string }) => e.code === "RPC_MESSAGE_TOO_LARGE" && /8192/.test(e.message));
      assert.ok(performance.now() - t0 < 1000);
    } finally {
      for (const b of abiertos.splice(0)) await b.close().catch(() => {});
      await pequeno.stop();
    }
  });

  test("A-85: la librería de NATS convierte un `timeout` de 0 o NaN en 1000 ms SIN avisar, y rechaza 0,5 y los negativos: por eso el SDK valida `timeoutMs` antes y no lo recorta", async () => {
    const mudos = opsNc.subscribe("rpc.clientes.cliente.v1.callado", { callback: () => {} }); // escucha y no contesta
    await opsNc.flush();
    try {
      for (const t of [0, Number.NaN]) {
        const t0 = performance.now();
        await assert.rejects(opsNc.request("rpc.clientes.cliente.v1.callado", enc.encode("{}"), { timeout: t }), (e: Error) => e.name === "TimeoutError");
        const ms = performance.now() - t0;
        assert.ok(ms >= 900 && ms < 1500, `timeout ${t}: ${ms.toFixed(0)} ms (la librería lo sustituye por 1000)`);
      }
      for (const t of [0.5, -1]) {
        await assert.rejects(opsNc.request("rpc.clientes.cliente.v1.callado", enc.encode("{}"), { timeout: t }), (e: Error) => e.name === "InvalidArgumentError");
      }
    } finally {
      mudos.unsubscribe();
    }
    // el SDK no se lo pasa: 0, NaN, 0,5 y -1 son INVALID_RPC_CALL antes de tocar la red
    const llama = await abrir("facturacion-api");
    for (const t of [0, Number.NaN, 0.5, -1]) {
      await assert.rejects(llama.request(OP, PETICION, { timeoutMs: t }), (e: { code?: string }) => e.code === "INVALID_RPC_CALL");
    }
  });

  // ══════════════════════════════════════════════════════════════════════════
  // Métricas: lo que cuenta el SDK contra el broker
  // ══════════════════════════════════════════════════════════════════════════

  test("las métricas de un ciclo real: cliente y servidor cuentan lo mismo, sin tenant, id ni servicio como etiqueta", async () => {
    const mc = new InMemoryMetrics();
    const ms = new InMemoryMetrics();
    const crm = await abrir("crm-api", { metrics: ms });
    await crm.respond(OP, contador().handler);
    const llama = await abrir("facturacion-api", { metrics: mc, tenantId: "tenant-que-no-debe-salir" });
    for (let i = 0; i < 5; i++) await llama.request(OP, PETICION);
    await assert.rejects(llama.request(OBTENER, { clienteId: "x" }), RpcUnavailableError);
    const c = mc.snapshot().counters;
    assert.equal(c.get(`flux_rpc_client_requests_total{operation="${OP}",outcome="ok"}`), 5);
    assert.equal(c.get(`flux_rpc_client_requests_total{operation="${OBTENER}",outcome="unavailable"}`), 1);
    assert.equal(ms.snapshot().counters.get(`flux_rpc_server_requests_total{operation="${OP}",outcome="ok"}`), 5);
    for (const salida of [mc.render(), ms.render()]) {
      assert.ok(!salida.includes("tenant-que-no-debe-salir"));
      assert.ok(!salida.includes("facturacion-api") && !salida.includes("crm-api"));
    }
    assert.match(mc.render(), new RegExp(`flux_rpc_duration_seconds_count\\{operation="${OP}",role="client"\\} 5`));
    assert.match(ms.render(), new RegExp(`flux_rpc_duration_seconds_count\\{operation="${OP}",role="server"\\} 5`));
  });

  test("un servicio que atiende y llama con el MISMO bus: una respuesta dentro de un handler hereda la clasificación sin hacer nada", async () => {
    const crm = await abrir("crm-api");
    // crm llama a `tarifas`, que no está: RPC_UNAVAILABLE (retryable) se escapa del handler y quien llama lo ve como retryable
    await crm.respond(OP, async () => {
      await crm.request(TARIFA, { sku: "ABC-1" });
      return DESCUENTO;
    });
    const llama = await abrir("facturacion-api");
    await assert.rejects(llama.request(OP, PETICION), (e: RpcRetryableError) => {
      assert.ok(e instanceof RetryableError, "quien llama lo ve como RETRYABLE");
      assert.equal(e.code, "RPC_UNAVAILABLE", "con el código del fallo que lo causó, no uno genérico");
      return true;
    });
  });
});

// El bundle se lee para que un cambio en el índice `rpc` rompa la suite, no para usarlo aquí
void bundle;
