/**
 * Estado replicado — pruebas UNITARIAS (sin broker). specification/10-state.md.
 *
 * Aquí se fija lo que no necesita un nats-server: los bytes exactos, las reglas de validación
 * (que corren ANTES de la red), los seis códigos de 04-errors.md §6, el algoritmo de `put`
 * contra un broker en memoria que hace lo que se midió (A-33: chequeo de secuencia con DOS
 * códigos de conflicto; A-32: las cabeceras se conservan) y las métricas.
 *
 * Lo que sí necesita broker está en `integration/state.test.ts` (permisos exactos y ciclo de
 * 100 claves) y `integration/state-semantics.test.ts` (los escenarios de
 * conformance/cases/state-semantics.json con esta API). Toda comprobación va en los DOS
 * sentidos: lo correcto pasa y lo incorrecto falla.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

import { errors } from "@nats-io/transport-node";
import { JetStreamApiError } from "@nats-io/jetstream";

import {
  FluxBus,
  InMemoryMetrics,
  AGGREGATE_VERSION_MAX,
  ErrorClass,
  InvalidDataClassificationError,
  InvalidStateKeyError,
  InvalidTenantForStateError,
  MissingAggregateVersionError,
  NotProvisionedError,
  PermissionDeniedError,
  ReservedDomainError,
  StateConflictError,
  StateSubjectMismatchError,
  TenantIsolationError,
  assertStateKey,
  buildStateDlqEvent,
  buildStateEvent,
  buildStateTombstone,
  buildUnreadableStateDlqEvent,
  isSequenceConflict,
  isValidSubject,
  parseStateContract,
  serialize,
  stateDlqSubject,
  stateDurableName,
  stateFilterSubject,
  stateStreamName,
  stateSubject,
  toStateDlqEvent,
  createSigner,
  generateKeyPair,
  type ConnectOptions,
  type MetricsSink,
  type StateEventInput,
} from "../src/index.js";
import { isNoResponders } from "../src/security.js";
import { PendingSum } from "../src/state.js";
import * as publica from "../src/index.js";

// ─── Utilidades ──────────────────────────────────────────────────────────────

/**
 * La raíz se busca por un marcador, no contando `../`: el fuente vive en `test/` y el
 * compilado en `dist-test/test/`, y cualquier ruta relativa fija es correcta en uno de los
 * dos y silenciosamente incorrecta en el otro.
 */
async function repoRoot(): Promise<URL> {
  let dir = new URL("./", import.meta.url);
  for (let i = 0; i < 6; i++) {
    try {
      await readFile(new URL("protocol.json", dir), "utf8");
      return dir;
    } catch {
      dir = new URL("../", dir);
    }
  }
  throw new Error("no se encontró la raíz del repo (protocol.json)");
}
const RAIZ = await repoRoot();
const leer = (ruta: string) => readFile(new URL(ruta, RAIZ), "utf8");

const protocolo = JSON.parse(await leer("protocol.json"));
const fixtures = JSON.parse(await leer("conformance/cases/cross-sdk-state-envelope.json"));
const bundle = JSON.parse(await leer("schemas/bundle.json"));
const especificacion = await leer("specification/10-state.md");

const sha256 = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");
const texto = (b: Uint8Array) => new TextDecoder().decode(b);

/** La entrada de un fixture sin lo que es solo metadato del fixture. */
const entradaDe = (id: string) => fixtures.fixtures.find((f: { id: string }) => f.id === id);

// ═════════════════════════════════════════════════════════════════════════════
// 1. Los bytes exactos
// ═════════════════════════════════════════════════════════════════════════════

describe("bytes exactos — conformance/cases/cross-sdk-state-envelope.json", () => {
  const construir = {
    "state-put": () => serialize(buildStateEvent(entradaDe("state-put").input)),
    "state-tombstone": () => serialize(buildStateTombstone(entradaDe("state-tombstone").input)),
    "state-dlq": () => serialize(buildStateDlqEvent(entradaDe("state-dlq").input)),
  } as const;

  for (const [id, fn] of Object.entries(construir)) {
    test(`${id}: los bytes, la longitud y el SHA-256 son los del fixture`, () => {
      const f = entradaDe(id);
      const bytes = fn();
      assert.equal(Buffer.from(bytes).toString("base64"), f.base64, "los bytes difieren");
      assert.equal(bytes.byteLength, f.bytes);
      assert.equal(sha256(bytes), f.sha256);
      // y es EXACTAMENTE lo que serializa el fixture: el orden de claves es normativo
      assert.equal(texto(bytes), JSON.stringify(f.event));
    });
  }

  test("las tres líneas de 10-state.md §2.6 (el texto de la spec) son las que se construyen", () => {
    // El fixture y la spec dicen lo mismo; aquí se lee la SPEC, no el JSON de datos.
    const lineas = especificacion.split("\n").filter((l) => l.startsWith('{"specversion":"1.0"'));
    assert.equal(lineas.length, 3, "10-state.md §2.6 debe tener tres ejemplos byte a byte");
    assert.equal(lineas[0], texto(construir["state-put"]()));
    assert.equal(lineas[1], texto(construir["state-tombstone"]()));
    assert.equal(lineas[2], texto(construir["state-dlq"]()));
  });

  test("los SHA-256 de la spec: fa6b20be…, 7a7868d2…, dbb6d882…", () => {
    assert.equal(sha256(construir["state-put"]()), "fa6b20beb64f90c06b0cfb2c82d809a8fd2672b365681d2cd9d1a99f29b13b40");
    assert.equal(sha256(construir["state-tombstone"]()), "7a7868d2f643ccd584a354bce87c26d8d7f20826f23da26e99dbed78269bed3d");
    assert.equal(sha256(construir["state-dlq"]()), "dbb6d882f7df49e4b88026f17fb7b83e9e6c2226aa731ad25d71fc41c8b8f6d9");
  });

  test("CONTROL: cambiar el `tenantid` cambia el hash (la comprobación sabe fallar)", () => {
    // Una comprobación que solo se ha visto en verde no demuestra nada.
    const f = entradaDe("state-put");
    assert.notEqual(sha256(serialize(buildStateEvent({ ...f.input, tenantId: "globex" }))), f.sha256);
    // y el orden de claves: poner `data` antes de `partitionkey` rompe el hash
    const { data, partitionkey, ...resto } = buildStateEvent(f.input);
    const roto = { ...resto, data, partitionkey } as unknown as Parameters<typeof serialize>[0];
    assert.notEqual(sha256(serialize(roto)), f.sha256);
  });

  test("UTF-8 literal: `María` y `país`, sin ningún escape \\u", () => {
    for (const id of Object.keys(construir)) {
      const s = texto(construir[id as keyof typeof construir]());
      assert.ok(!/\\u[0-9a-fA-F]{4}/.test(s), `${id} lleva un escape \\u`);
    }
    assert.ok(texto(construir["state-put"]()).includes("María Peña"));
    assert.ok(texto(construir["state-dlq"]()).includes("país"));
  });

  test("`data` es siempre el ÚLTIMO atributo, también en la DLQ de estado", () => {
    for (const id of Object.keys(construir)) {
      const ev = JSON.parse(texto(construir[id as keyof typeof construir]()));
      assert.equal(Object.keys(ev).at(-1), "data", id);
    }
  });

  test("el sobre de un put: `subject` = la clave, `tenantid` = el del subject, `partitionkey` = la clave", () => {
    const f = entradaDe("state-put");
    const ev = buildStateEvent(f.input);
    const nats = stateSubject(f.input.contract, f.input.tenantId, f.input.key);
    assert.equal(nats, f.natsSubject);
    assert.equal(ev.subject, nats.split(".").at(-1));
    assert.equal(ev.tenantid, nats.split(".").at(-2));
    assert.equal(ev.partitionkey, ev.subject);
    assert.equal(ev.type, "com.flux.state.clientes.cliente.v1");
  });

  test("la lápida: type con `.deleted.`, dataschema fijo, `internal` y data = solo la versión", () => {
    const ev = buildStateTombstone(entradaDe("state-tombstone").input);
    assert.equal(ev.type, "com.flux.state.clientes.cliente.deleted.v1");
    assert.equal(ev.dataschema, "https://schemas.internal/system/state/eliminado/1.0.0.json");
    assert.equal(ev.dataclassification, "internal");
    assert.deepEqual(ev.data, { aggregateVersion: 8 });
  });

  test("son PURAS: la misma entrada da los mismos bytes, sin reloj ni UUID por dentro", () => {
    const f = entradaDe("state-put");
    const a = serialize(buildStateEvent(f.input));
    const b = serialize(buildStateEvent(structuredClone(f.input)));
    assert.deepEqual(a, b);
  });
});

describe("los vectores del subject — cross-sdk-state-envelope.json → subjectVectors", () => {
  for (const v of fixtures.subjectVectors as Array<{ contract: string; tenantId: string; key: string; natsSubject?: string; errorCode?: string }>) {
    const nombre = `${JSON.stringify(v.contract)} ${JSON.stringify(v.tenantId.slice(0, 12))} ${JSON.stringify(v.key.slice(0, 12))}`;
    if (v.natsSubject) {
      test(`${nombre} → ${v.natsSubject.slice(0, 50)}`, () => {
        assert.equal(stateSubject(v.contract, v.tenantId, v.key), v.natsSubject);
      });
    } else {
      test(`${nombre} → ${v.errorCode}`, () => {
        assert.throws(
          () => stateSubject(v.contract, v.tenantId, v.key),
          (e: { code?: string }) => e.code === v.errorCode,
        );
      });
    }
  }
});

// ═════════════════════════════════════════════════════════════════════════════
// 2. Los patrones de protocol.json (la fuente) contra lo que implementa el SDK
// ═════════════════════════════════════════════════════════════════════════════

describe("protocol.json → state: lo que el SDK acepta y rechaza", () => {
  const st = protocolo.state;

  test("claves: valid / invalid de protocol.json", () => {
    for (const k of st.key.valid) assert.doesNotThrow(() => assertStateKey(k), k);
    for (const k of st.key.invalid) assert.throws(() => assertStateKey(k), InvalidStateKeyError, JSON.stringify(k));
    // el límite: 128 sí, 129 no
    assert.doesNotThrow(() => assertStateKey("a".repeat(128)));
    assert.throws(() => assertStateKey("a".repeat(129)), InvalidStateKeyError);
  });

  test("la trampa de `$`: «cli-1\\n» NO es una clave (en Python y PHP sí pasaría el patrón)", () => {
    assert.throws(() => assertStateKey("cli-1\n"), InvalidStateKeyError);
    assert.throws(() => assertStateKey("\ncli-1"), InvalidStateKeyError);
    assert.throws(() => assertStateKey("cli-1\r\n"), InvalidStateKeyError);
  });

  test("la clave NO es de otro tipo: un número o null no se convierten", () => {
    for (const k of [1, null, undefined, {}, ["a"]]) {
      assert.throws(() => assertStateKey(k), InvalidStateKeyError);
    }
  });

  test("tenants: valid / invalid de protocol.json, y el máximo de 64", () => {
    for (const t of st.tenant.valid) assert.doesNotThrow(() => stateSubject("clientes.cliente.v1", t, "k"), t);
    for (const t of st.tenant.invalid) {
      assert.throws(() => stateSubject("clientes.cliente.v1", t, "k"), InvalidTenantForStateError, JSON.stringify(t));
    }
    assert.doesNotThrow(() => stateSubject("clientes.cliente.v1", "t".repeat(64), "k"));
    assert.throws(() => stateSubject("clientes.cliente.v1", "t".repeat(65), "k"), InvalidTenantForStateError);
  });

  test("el tenant de un EVENTO puede ser cualquier cadena; el de un estado, no", () => {
    // 01-envelope.md §3.1 frente a 10-state.md §2.4: «ACME_Corp» publica eventos y no escribe estado.
    assert.throws(() => stateSubject("clientes.cliente.v1", "ACME_Corp", "k"), InvalidTenantForStateError);
  });

  test("contratos: valid / invalid de protocol.json", () => {
    for (const c of st.contract.valid) assert.doesNotThrow(() => parseStateContract(c), c);
    for (const c of st.contract.invalid) assert.throws(() => parseStateContract(c), (e: unknown) => e instanceof Error, c);
  });

  test("subject: valid / invalid de protocol.json (el patrón completo de 6 tokens)", () => {
    const re = new RegExp(st.subject.pattern);
    for (const s of st.subject.valid) {
      const p = s.split(".");
      const contrato = p.slice(1, 4).join(".");
      assert.equal(stateSubject(contrato, p[4], p[5]), s);
      assert.ok(re.test(s));
    }
    for (const s of st.subject.invalid) {
      assert.ok(!re.test(s), s);
      const p = s.split(".");
      // los defectos que están en el prefijo (`State.`) no se pueden expresar con (contrato, tenant, clave)
      if (p[0] !== "state") continue;
      const rechaza = () => stateSubject(p.slice(1, 4).join("."), p[4] ?? "", p.slice(5).join("."));
      assert.throws(rechaza, (e: unknown) => e instanceof Error, s);
    }
  });

  test("durable: valid de protocol.json se reconstruye; el sufijo de tenant cambia `-` por `_`", () => {
    const re = new RegExp(st.watch.durable.pattern);
    assert.equal(stateDurableName("tienda-web", "clientes.cliente.v1"), "tienda-web__state_clientes_cliente_v1");
    assert.equal(stateDurableName("tienda-web", "clientes.cliente.v1", "acme"), "tienda-web__state_clientes_cliente_v1__acme");
    assert.equal(
      stateDurableName("tienda-web", "logistica.linea-envio.v2", "acme-corp"),
      "tienda-web__state_logistica_linea_envio_v2__acme_corp",
    );
    for (const d of st.watch.durable.valid) assert.ok(re.test(d), d);
    for (const d of st.watch.durable.invalid) assert.ok(!re.test(d), d);
    assert.ok(re.test(stateDurableName("tienda-web", "clientes.cliente.v1", "acme-corp")));
    // sin punto alguno: NATS no admite puntos en el nombre de un consumidor
    assert.ok(!stateDurableName("tienda-web", "logistica.linea-envio.v2", "acme").includes("."));
  });

  test("filtro, stream y subject de DLQ", () => {
    assert.equal(stateFilterSubject("clientes.cliente.v1", "acme"), "state.clientes.cliente.v1.acme.*");
    assert.equal(stateFilterSubject("clientes.cliente.v1"), "state.clientes.cliente.v1.*.*");
    assert.equal(stateStreamName("clientes"), "STATE_CLIENTES");
    assert.equal(stateStreamName("mi-dominio"), "STATE_MI_DOMINIO");
    assert.match(stateStreamName("mi-dominio"), new RegExp(st.stream.pattern));
    const re = new RegExp(st.dlq.subject.pattern);
    assert.equal(stateDlqSubject("clientes.cliente.v1"), st.dlq.subject.example);
    for (const s of st.dlq.subject.valid) assert.ok(re.test(s));
    for (const s of st.dlq.subject.invalid) assert.ok(!re.test(s), s);
  });

  test("aggregateVersion: entero ≥ 1 y ≤ 2^53 − 1", () => {
    assert.equal(AGGREGATE_VERSION_MAX, 9007199254740991);
    assert.equal(AGGREGATE_VERSION_MAX, st.aggregateVersion.maximum);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 3. Los seis códigos de 04-errors.md §6
// ═════════════════════════════════════════════════════════════════════════════

describe("los seis códigos de error de estado — 04-errors.md §6", () => {
  const casos: Array<{
    code: string;
    clase: ErrorClass;
    crea: () => Error & { code?: string; fluxClass?: ErrorClass; opts?: { code?: string } };
  }> = [
    { code: "INVALID_TENANT_FOR_STATE", clase: ErrorClass.PERMANENT, crea: () => new InvalidTenantForStateError("ACME") },
    { code: "INVALID_STATE_KEY", clase: ErrorClass.PERMANENT, crea: () => new InvalidStateKeyError("a.b") },
    { code: "MISSING_AGGREGATE_VERSION", clase: ErrorClass.PERMANENT, crea: () => new MissingAggregateVersionError(undefined) },
    { code: "STATE_SUBJECT_MISMATCH", clase: ErrorClass.POISON, crea: () => new StateSubjectMismatchError("tenantid", "globex", "acme") },
    { code: "STATE_CONFLICT", clase: ErrorClass.RETRYABLE, crea: () => new StateConflictError("clientes.cliente.v1", "cli-1", 3) },
    { code: "RESERVED_DOMAIN", clase: ErrorClass.PERMANENT, crea: () => new ReservedDomainError("state", ["dlq", "state", "rpc", "system"]) },
  ];

  for (const c of casos) {
    test(`${c.code}: código fijo y clase ${c.clase}`, () => {
      const e = c.crea();
      assert.equal(e.code, c.code);
      assert.equal(e.opts?.code, c.code, "el clasificador lee `opts.code`: sin él el código del panel sería el nombre de la clase");
      assert.equal(e.fluxClass, c.clase);
    });

    test(`${c.code}: la clase y la capacidad coinciden con protocol.json → errors.codes`, () => {
      const def = protocolo.errors.codes[c.code];
      assert.equal(def.capability, "state");
      assert.equal(def.class, c.clase);
    });
  }

  test("la lista es CERRADA: protocol.json declara exactamente estos seis para `state`", () => {
    const declarados = Object.entries(protocolo.errors.codes as Record<string, { capability?: string }>)
      .filter(([, d]) => d.capability === "state")
      .map(([k]) => k)
      .sort();
    assert.deepEqual(declarados, casos.map((c) => c.code).sort());
  });

  test("el clasificador del SDK los lee con su clase y su código, sin tocar la política de lo desconocido", async () => {
    const { classify } = await import("../src/classify.js");
    for (const c of casos) {
      const r = classify(c.crea());
      assert.equal(r.class, c.clase, c.code);
      assert.equal(r.code, c.code);
      // un RETRYABLE reconocido (STATE_CONFLICT) conserva los 6 intentos: no cae en el presupuesto de lo desconocido
      assert.equal(r.maxAttempts, undefined, `${c.code} no debe heredar el presupuesto acotado de lo desconocido`);
    }
  });

  test("ningún mensaje cita un valor de `data`: una versión se describe por su tipo", () => {
    const secreto = "ana@example.com";
    assert.ok(!new MissingAggregateVersionError(secreto).message.includes(secreto));
    assert.match(new MissingAggregateVersionError(secreto).message, /una cadena/);
    assert.match(new MissingAggregateVersionError(7.5).message, /7\.5/); // un número no es PII
    assert.match(new MissingAggregateVersionError(null).message, /null/);
  });

  test("los mensajes dicen QUÉ está mal y CÓMO se arregla", () => {
    assert.match(new InvalidTenantForStateError(undefined).message, /connect\(\)/);
    assert.match(new InvalidTenantForStateError(undefined).message, /no hay tenant por defecto|NO hay tenant por defecto/i);
    assert.match(new InvalidStateKeyError("a.b").message, /\^\[A-Za-z0-9\]\[A-Za-z0-9_-\]\{0,127\}\$/);
    assert.match(new ReservedDomainError("state", ["dlq", "state", "rpc", "system"]).message, /dlq, state, rpc, system/);
    assert.match(new StateConflictError("clientes.cliente.v1", "cli-1", 3).message, /3 intentos|3 veces/);
    assert.match(new InvalidDataClassificationError("restricted").message, /restricted/);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 4. Un bus con conexiones FALSAS
// ═════════════════════════════════════════════════════════════════════════════

/** Una conexión que explota si se la toca: lo que se comprueba es que NO se abrió ninguna petición. */
const REDTOCADA = "se tocó la red";
const prohibido = (): never => {
  throw new Error(REDTOCADA);
};
const explosivo = (base: Record<string, unknown> = {}): never =>
  new Proxy(base, {
    get: (t, p) => (p in t ? (t as Record<string | symbol, unknown>)[p] : p === "then" ? undefined : prohibido),
  }) as never;

/** `status()` que nunca emite: es lo que el constructor de FluxBus necesita. */
const statusInerte = () => ({ [Symbol.asyncIterator]: () => ({ next: () => new Promise<never>(() => {}) }) });

const OPCIONES: ConnectOptions = {
  servers: "nats://falso:4222",
  service: "crm-api",
  environment: "produccion",
  version: "2.1.0",
  tenantId: "acme",
  schemaBaseUrl: "https://schemas.internal",
};

/** Un bus cuya red EXPLOTA al tocarla. */
async function busSinRed(extra: Partial<ConnectOptions> = {}): Promise<FluxBus> {
  const nc = explosivo({ status: statusInerte, request: prohibido, publish: prohibido, flush: prohibido, isClosed: () => false });
  const bus = new FluxBus(nc, explosivo(), explosivo(), { ...OPCIONES, ...extra });
  await bus.initValidation();
  return bus;
}

const CONTRATO = "clientes.cliente.v1";
const dato = (...v: [unknown?]) =>
  ({ clienteId: "cli-1", nombre: "Ana", email: "ana@example.com", aggregateVersion: v.length === 0 ? 1 : v[0] }) as never;

/** Rechaza con ESE código y sin haber tocado la red. */
async function rechazaCon(codigo: string, p: Promise<unknown>): Promise<void> {
  await assert.rejects(p, (e: { code?: string; message: string }) => {
    assert.notEqual(e.message, REDTOCADA, `${codigo}: se llegó a tocar la red`);
    return e.code === codigo;
  });
}

describe("dominios reservados — publish() y subscribe() los rechazan ANTES de la red", () => {
  for (const dominio of protocolo.naming.reservedDomains.names as string[]) {
    test(`publish("${dominio}.…") → RESERVED_DOMAIN`, async () => {
      const bus = await busSinRed();
      await rechazaCon("RESERVED_DOMAIN", bus.publish(`${dominio}.cliente.v1.creado`, {}));
      // incluso con la forma de DLQ (5 tokens): antes el mensaje habría sido «debe tener 4 tokens»
      await rechazaCon("RESERVED_DOMAIN", bus.publish(`${dominio}.pedidos.pedido.v1.creado`, {}));
    });

    test(`subscribe("${dominio}.…") → RESERVED_DOMAIN`, async () => {
      const bus = await busSinRed();
      await rechazaCon("RESERVED_DOMAIN", bus.subscribe(`${dominio}.cliente.v1.creado`, () => {}));
    });
  }

  test("CONTROL: `state-clientes` y `estado` NO están reservados (un nombre parecido no es el reservado)", () => {
    assert.ok(isValidSubject("state-clientes.cliente.v1.creado"));
    assert.ok(isValidSubject("estado.cliente.v1.creado"));
    assert.ok(isValidSubject("pedidos.pedido.v1.creado"));
    assert.ok(!isValidSubject("state.cliente.v1.creado"));
  });

  test("el error es el mismo que el de estado, PERMANENT, con la lista en el mensaje", async () => {
    const bus = await busSinRed();
    await assert.rejects(bus.publish("system.x.v1.y", {}), (e: ReservedDomainError) => {
      assert.ok(e instanceof ReservedDomainError);
      assert.equal(e.fluxClass, ErrorClass.PERMANENT);
      assert.equal(e.domain, "system");
      assert.match(e.message, /dlq, state, rpc, system/);
      return true;
    });
  });
});

describe("validación previa a la red — put, delete, get y watch (10-state.md §3.6)", () => {
  test("put: clave inválida → INVALID_STATE_KEY, sin red", async () => {
    const bus = await busSinRed();
    for (const k of ["a.b", "a b", "*", ">", "", "_x", "cli-1\n", "café", "a".repeat(129)]) {
      await rechazaCon("INVALID_STATE_KEY", bus.state.put(CONTRATO, k, dato()));
    }
  });

  test("put: tenant inválido, o ninguno → INVALID_TENANT_FOR_STATE, sin red", async () => {
    const bus = await busSinRed();
    for (const t of ["ACME", "acme_corp", "acme.corp", "", "t".repeat(65)]) {
      await rechazaCon("INVALID_TENANT_FOR_STATE", bus.state.put(CONTRATO, "k", dato(), { tenantId: t }));
    }
    // sin tenant en ninguna parte: NO cae en `system`
    const sinTenant = await busSinRed({ tenantId: undefined });
    await rechazaCon("INVALID_TENANT_FOR_STATE", sinTenant.state.put(CONTRATO, "k", dato()));
    await rechazaCon("INVALID_TENANT_FOR_STATE", sinTenant.state.get(CONTRATO, "k"));
    await rechazaCon("INVALID_TENANT_FOR_STATE", sinTenant.state.delete(CONTRATO, "k", { aggregateVersion: 2 }));
  });

  test("el tenant de `connect()` también se valida (un evento sí admite «ACME_Corp»)", async () => {
    const bus = await busSinRed({ tenantId: "ACME_Corp" });
    await rechazaCon("INVALID_TENANT_FOR_STATE", bus.state.put(CONTRATO, "k", dato()));
  });

  test("put: aggregateVersion ausente, null, cadena, decimal, 0, negativo, > 2^53−1 → MISSING_AGGREGATE_VERSION", async () => {
    const bus = await busSinRed();
    for (const v of [undefined, null, "7", 7.5, 0, -1, 9007199254740992, NaN, Infinity, 10n]) {
      await rechazaCon("MISSING_AGGREGATE_VERSION", bus.state.put(CONTRATO, "k", dato(v)));
    }
    // y un `data` que no es objeto
    await rechazaCon("MISSING_AGGREGATE_VERSION", bus.state.put(CONTRATO, "k", null as never));
    await rechazaCon("MISSING_AGGREGATE_VERSION", bus.state.put(CONTRATO, "k", [1] as never));
    await rechazaCon("MISSING_AGGREGATE_VERSION", bus.state.put(CONTRATO, "k", "x" as never));
  });

  test("delete: aggregateVersion es OBLIGATORIO", async () => {
    const bus = await busSinRed();
    for (const v of [undefined, null, "8", 0, 8.5]) {
      await rechazaCon("MISSING_AGGREGATE_VERSION", bus.state.delete(CONTRATO, "k", { aggregateVersion: v as never }));
    }
    await rechazaCon("MISSING_AGGREGATE_VERSION", bus.state.delete(CONTRATO, "k", undefined as never));
  });

  test("put: `restricted` o una clasificación inventada → INVALID_DATACLASSIFICATION, sin red", async () => {
    const bus = await busSinRed();
    for (const c of ["restricted", "secreto", "", null]) {
      await rechazaCon("INVALID_DATACLASSIFICATION", bus.state.put(CONTRATO, "k", dato(), { classification: c as never }));
    }
  });

  test("el límite: la versión 2^53 − 1 y la clave de 128 caracteres SÍ pasan la validación", async () => {
    // Pasan la validación y llegan a la red falsa: el único error posible es el de tocarla.
    const bus = await busSinRed();
    await assert.rejects(bus.state.put(CONTRATO, "a".repeat(128), dato(AGGREGATE_VERSION_MAX)), (e: Error) => e.message === REDTOCADA);
  });

  test("get, delete y watch con dominio reservado o contrato mal formado, sin red", async () => {
    const bus = await busSinRed();
    for (const c of ["state.cliente.v1", "dlq.cliente.v1", "rpc.cliente.v1", "system.cliente.v1"]) {
      await rechazaCon("RESERVED_DOMAIN", bus.state.put(c, "k", dato()));
      await rechazaCon("RESERVED_DOMAIN", bus.state.get(c, "k"));
      await rechazaCon("RESERVED_DOMAIN", bus.state.delete(c, "k", { aggregateVersion: 2 }));
      await rechazaCon("RESERVED_DOMAIN", bus.state.watch(c, () => {}));
    }
    // un contrato mal formado es PERMANENT, pero NO lleva uno de los seis códigos: la lista es cerrada
    for (const c of ["clientes.cliente", "clientes.cliente.v1.creado", "Clientes.cliente.v1", "clientes.cliente.v0"]) {
      await assert.rejects(bus.state.put(c, "k", dato()), (e: { code?: string; fluxClass?: string; message: string }) => {
        assert.notEqual(e.message, REDTOCADA);
        assert.equal(e.code, undefined, "un código inventado rompería la lista cerrada de 04-errors.md §6");
        return e.fluxClass === ErrorClass.PERMANENT;
      });
    }
  });

  test("watch: tenant inválido → INVALID_TENANT_FOR_STATE; strict sin tenant → TenantIsolationError; sin red", async () => {
    const bus = await busSinRed();
    await rechazaCon("INVALID_TENANT_FOR_STATE", bus.state.watch(CONTRATO, () => {}, { tenantId: "ACME" }));
    const estricto = await busSinRed({ tenantId: undefined, tenantIsolation: "strict" });
    await assert.rejects(estricto.state.watch(CONTRATO, () => {}), (e: Error) => {
      assert.ok(e instanceof TenantIsolationError);
      assert.notEqual(e.message, REDTOCADA);
      return true;
    });
  });

  test("EL ORDEN de §3.6: dominio reservado → clave → tenant → versión → clasificación", async () => {
    const bus = await busSinRed({ tenantId: undefined });
    // todo mal a la vez, y se va quitando un fallo cada vez: gana el primero de la lista
    const todoMal = (c: string, k: string, t: string | undefined, v: unknown, cl: unknown) =>
      bus.state.put(c, k, dato(v), { tenantId: t, classification: cl as never });
    await rechazaCon("RESERVED_DOMAIN", todoMal("state.x.v1", "a.b", "ACME", 0, "restricted"));
    await rechazaCon("INVALID_STATE_KEY", todoMal(CONTRATO, "a.b", "ACME", 0, "restricted"));
    await rechazaCon("INVALID_TENANT_FOR_STATE", todoMal(CONTRATO, "ok", "ACME", 0, "restricted"));
    await rechazaCon("INVALID_TENANT_FOR_STATE", todoMal(CONTRATO, "ok", undefined, 0, "restricted"));
    await rechazaCon("MISSING_AGGREGATE_VERSION", todoMal(CONTRATO, "ok", "acme", 0, "restricted"));
    await rechazaCon("INVALID_DATACLASSIFICATION", todoMal(CONTRATO, "ok", "acme", 1, "restricted"));
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 5. El algoritmo de put, contra un broker EN MEMORIA que hace lo que se midió
// ═════════════════════════════════════════════════════════════════════════════

interface Almacenado {
  seq: number;
  bytes: Uint8Array;
  msgId: string;
}

/**
 * Un `STATE_CLIENTES` en memoria con la semántica MEDIDA (no inventada):
 *   · `DIRECT.GET` por subject: 404 si no hay valor; devuelve `Nats-Sequence` y las cabeceras que se publicaron (A-32);
 *   · el chequeo de `Nats-Expected-Last-Subject-Sequence` se evalúa ANTES que el de duplicados (A-33), y `0` = «no existe»;
 *   · un conflicto da err 10071 (un nodo) o 10164 (cluster, bajo carrera): el SDK debe reconocer los DOS (A-33, A-49).
 * El `JetStreamApiError` es el de la librería: la misma forma que ve el SDK de verdad (`code` = el `err_code`).
 */
class BrokerEnMemoria {
  seq = 0;
  readonly datos = new Map<string, Almacenado>();
  readonly lecturas: string[] = [];
  readonly publicaciones: Array<{ subject: string; bytes: Uint8Array; msgId?: string; esperada?: number }> = [];
  publicacionesCore = 0;
  codigoDeConflicto = 10071;
  sinStream = false;
  /** Corre ANTES de cada publish (1-based): un rival que escribe entre la lectura y la publicación. */
  antesDePublicar?: (n: number) => void | Promise<void>;
  /** El primer publish SE APLICA, pero quien lo hizo ve un error de secuencia (el reenvío de la librería tras reconectar). */
  perderAcuseEnElPrimero = false;
  /** Cualquier otro error del publish. */
  falloDelPublish?: (n: number) => Error | undefined;
  falloDeLaLectura?: Error;

  readonly nc = {
    status: statusInerte,
    isClosed: () => false,
    flush: async () => {},
    drain: async () => {},
    publish: () => {
      this.publicacionesCore++; // un publish de CORE a un subject de estado sobrescribe sin chequeo (A-45)
    },
    request: async (subject: string) => {
      this.lecturas.push(subject);
      if (this.falloDeLaLectura) throw this.falloDeLaLectura;
      if (this.sinStream) throw new errors.RequestError(`no responders: '${subject}'`, { cause: new errors.NoRespondersError(subject) });
      const s = subject.replace(/^\$JS\.API\.DIRECT\.GET\.STATE_[A-Z0-9_]+\./, "");
      const v = this.datos.get(s);
      if (!v) return { headers: cabeceras(404, "Message Not Found"), data: new Uint8Array(0) };
      return { headers: cabeceras(0, "", { "Nats-Sequence": String(v.seq), "Nats-Msg-Id": v.msgId, "Nats-Subject": s }), data: v.bytes };
    },
  };

  readonly js = {
    publish: async (subject: string, bytes: Uint8Array, opts: { msgID?: string; expect?: { lastSubjectSequence?: number } } = {}) => {
      this.publicaciones.push({ subject, bytes, msgId: opts.msgID, esperada: opts.expect?.lastSubjectSequence });
      const n = this.publicaciones.length;
      await this.antesDePublicar?.(n);
      const fallo = this.falloDelPublish?.(n);
      if (fallo) throw fallo;
      const actual = this.datos.get(subject);
      const S = opts.expect?.lastSubjectSequence;
      if (S !== undefined && S !== (actual?.seq ?? 0)) throw this.conflicto(actual?.seq ?? 0);
      this.seq++;
      this.datos.set(subject, { seq: this.seq, bytes, msgId: opts.msgID ?? "" });
      if (this.perderAcuseEnElPrimero && n === 1) throw this.conflicto(this.seq);
      return { stream: "STATE_CLIENTES", seq: this.seq, duplicate: false };
    },
  };

  conflicto(actual: number): JetStreamApiError {
    return new JetStreamApiError({
      code: 400,
      err_code: this.codigoDeConflicto,
      description: this.codigoDeConflicto === 10071 ? `wrong last sequence: ${actual}` : "wrong last sequence",
    });
  }

  /** Escribe un rival directamente (como otra instancia del dueño), sin pasar por el SDK. */
  escribeRival(subject: string, version: number, extra: Record<string, unknown> = {}): void {
    this.seq++;
    const ev = { specversion: "1.0", id: `rival-${this.seq}`, type: "com.flux.state.clientes.cliente.v1", data: { aggregateVersion: version }, ...extra };
    this.datos.set(subject, { seq: this.seq, bytes: new TextEncoder().encode(JSON.stringify(ev)), msgId: `rival-${this.seq}` });
  }

  /** Un valor ILEGIBLE (no es JSON) en la clave. */
  escribeBasura(subject: string, bytes = "esto no es json"): void {
    this.seq++;
    this.datos.set(subject, { seq: this.seq, bytes: new TextEncoder().encode(bytes), msgId: "basura" });
  }

  async bus(extra: Partial<ConnectOptions> = {}): Promise<FluxBus> {
    const bus = new FluxBus(this.nc as never, this.js as never, explosivo(), { ...OPCIONES, ...extra });
    await bus.initValidation();
    return bus;
  }
}

function cabeceras(code: number, description: string, valores: Record<string, string> = {}) {
  return { code, description, hasError: code >= 400, get: (k: string) => valores[k] ?? "" };
}

const SUBJECT = "state.clientes.cliente.v1.acme.cli-1";

describe("put — el algoritmo de 10-state.md §3.2", () => {
  test("clave nueva: lee (404), publica con S = 0 y el id del evento como Nats-Msg-Id", async () => {
    const b = new BrokerEnMemoria();
    const r = await (await b.bus()).state.put(CONTRATO, "cli-1", dato(1));
    assert.deepEqual(r, { applied: true, outcome: "ok", revision: 1 });
    assert.equal(b.lecturas.length, 1);
    assert.equal(b.lecturas[0], `$JS.API.DIRECT.GET.STATE_CLIENTES.${SUBJECT}`);
    assert.equal(b.publicaciones.length, 1);
    assert.equal(b.publicaciones[0].subject, SUBJECT);
    assert.equal(b.publicaciones[0].esperada, 0, "S = 0 significa «la clave no existe»: sin la cabecera, dos escritores se pisarían");
    assert.equal(b.publicaciones[0].msgId, JSON.parse(texto(b.publicaciones[0].bytes)).id, "Nats-Msg-Id = id del evento");
  });

  test("JAMÁS publica por core: un publish de core sobrescribe sin acuse ni chequeo (A-45)", async () => {
    const b = new BrokerEnMemoria();
    const bus = await b.bus();
    await bus.state.put(CONTRATO, "cli-1", dato(1));
    await bus.state.delete(CONTRATO, "cli-1", { aggregateVersion: 2 });
    assert.equal(b.publicacionesCore, 0);
    assert.ok(b.publicaciones.every((p) => p.esperada !== undefined), "toda escritura lleva la cabecera de secuencia");
  });

  test("clave existente: la cabecera lleva la secuencia LEÍDA, y revision es la del acuse", async () => {
    const b = new BrokerEnMemoria();
    b.escribeRival(SUBJECT, 1);
    b.escribeRival("state.clientes.cliente.v1.acme.otra", 1); // la secuencia del stream sube con cualquier clave
    const r = await (await b.bus()).state.put(CONTRATO, "cli-1", dato(2));
    assert.equal(b.publicaciones[0].esperada, 1, "S es la secuencia del mensaje DE ESA CLAVE, no la del stream");
    assert.deepEqual(r, { applied: true, outcome: "ok", revision: 3 });
  });

  test("una versión que no avanza es `stale` (y una igual también): no publica y revision es la del ACTUAL", async () => {
    const b = new BrokerEnMemoria();
    b.escribeRival(SUBJECT, 5);
    const bus = await b.bus();
    for (const v of [4, 5]) {
      const r = await bus.state.put(CONTRATO, "cli-1", dato(v));
      assert.deepEqual(r, { applied: false, outcome: "stale", revision: 1 }, `v${v}`);
    }
    assert.equal(b.publicaciones.length, 0, "stale NO es un error ni se publica");
    assert.equal((await bus.state.put(CONTRATO, "cli-1", dato(6))).outcome, "ok");
  });

  test("conflicto 10071 («wrong last sequence: N»): relee y reintenta", async () => {
    await conflictoSeReintenta(10071);
  });

  test("conflicto 10164 («wrong last sequence», SIN número — cluster bajo carrera): relee y reintenta", async () => {
    // El caso que un SDK que solo reconozca el 10071 NO pasa: trata un conflicto corriente como un
    // error desconocido y no reintenta. Pasa contra un nodo y falla en producción (A-49).
    await conflictoSeReintenta(10164);
  });

  async function conflictoSeReintenta(codigo: number) {
    const b = new BrokerEnMemoria();
    b.codigoDeConflicto = codigo;
    b.escribeRival(SUBJECT, 1);
    // un rival escribe la versión 2 entre la lectura y la publicación del primer intento
    b.antesDePublicar = (n) => {
      if (n === 1) b.escribeRival(SUBJECT, 2);
    };
    const r = await (await b.bus()).state.put(CONTRATO, "cli-1", dato(10));
    assert.equal(r.outcome, "ok");
    assert.equal(b.lecturas.length, 2, "una lectura por intento");
    assert.equal(b.publicaciones.length, 2);
    assert.equal(b.publicaciones[0].esperada, 1);
    assert.equal(b.publicaciones[1].esperada, 2, "el reintento usa la S RELEÍDA, no la vieja");
  }

  test("el reconocimiento: 10071 y 10164 son los ÚNICOS códigos de conflicto; ningún otro reintenta", async () => {
    assert.ok(isSequenceConflict(new JetStreamApiError({ code: 400, err_code: 10071, description: "wrong last sequence: 3" })));
    assert.ok(isSequenceConflict(new JetStreamApiError({ code: 400, err_code: 10164, description: "wrong last sequence" })));
    // y por la cadena de causas, como llegaría envuelto
    assert.ok(isSequenceConflict(new Error("x", { cause: new JetStreamApiError({ code: 400, err_code: 10164, description: "…" }) })));
    // otros err_code de JetStream NO son un conflicto de secuencia (p. ej. 10077, «maximum messages per subject exceeded»)
    for (const otro of [10077, 10059, 10014, 10070, 10165, 0]) {
      assert.ok(!isSequenceConflict(new JetStreamApiError({ code: 400, err_code: otro, description: "wrong last sequence" })), `${otro}`);
    }
    for (const x of [null, undefined, "10071", 10071, new Error("wrong last sequence"), { code: "ECONNRESET" }]) {
      assert.ok(!isSequenceConflict(x));
    }
    const b = new BrokerEnMemoria();
    b.falloDelPublish = () => new JetStreamApiError({ code: 400, err_code: 10077, description: "maximum messages per subject exceeded" });
    await assert.rejects((await b.bus()).state.put(CONTRATO, "cli-1", dato(1)), (e: Error) => e instanceof JetStreamApiError);
    assert.equal(b.publicaciones.length, 1, "un error que no es de secuencia NO se reintenta");
  });

  for (const codigo of [10071, 10164]) {
    test(`tres conflictos seguidos (${codigo}) → STATE_CONFLICT (RETRYABLE), y la métrica \`conflict\``, async () => {
      const b = new BrokerEnMemoria();
      b.codigoDeConflicto = codigo;
      let v = 0;
      b.antesDePublicar = () => b.escribeRival(SUBJECT, ++v); // un rival con versión menor, cada vez
      const m = new InMemoryMetrics();
      const bus = await b.bus({ metrics: m });
      await assert.rejects(bus.state.put(CONTRATO, "cli-1", dato(100)), (e: StateConflictError) => {
        assert.equal(e.code, "STATE_CONFLICT");
        assert.equal(e.fluxClass, ErrorClass.RETRYABLE);
        assert.equal(e.attempts, 3);
        return true;
      });
      assert.equal(b.publicaciones.length, 3, "exactamente 3 intentos");
      assert.equal(m.snapshot().counters.get(`flux_state_writes_total{contract="${CONTRATO}",outcome="conflict"}`), 1);
    });
  }

  test("un rival con una versión MÁS ALTA no es un conflicto: es `stale` (y se cuenta como tal)", async () => {
    const b = new BrokerEnMemoria();
    b.escribeRival(SUBJECT, 1);
    b.antesDePublicar = (n) => {
      if (n === 1) b.escribeRival(SUBJECT, 11);
    };
    const r = await (await b.bus()).state.put(CONTRATO, "cli-1", dato(10));
    assert.equal(r.outcome, "stale");
    assert.equal(r.applied, false);
    assert.equal(b.lecturas.length, 2);
  });

  test("mismo `id` y MISMOS BYTES en todos los intentos (si no, el paso 1b no reconocería nunca la propia escritura)", async () => {
    const b = new BrokerEnMemoria();
    let n = 0;
    b.antesDePublicar = (i) => {
      if (i <= 2) b.escribeRival(SUBJECT, ++n);
    };
    await (await b.bus()).state.put(CONTRATO, "cli-1", dato(100));
    assert.equal(b.publicaciones.length, 3);
    const ids = new Set(b.publicaciones.map((p) => p.msgId));
    assert.equal(ids.size, 1, "el mismo id en los 3 intentos");
    assert.equal(new Set(b.publicaciones.map((p) => Buffer.from(p.bytes).toString("base64"))).size, 1, "los mismos bytes");
  });

  test("paso 1b: un acuse perdido NO convierte una escritura buena en `stale`: reconoce la propia por el id", async () => {
    const b = new BrokerEnMemoria();
    b.perderAcuseEnElPrimero = true;
    const m = new InMemoryMetrics();
    const r = await (await b.bus({ metrics: m })).state.put(CONTRATO, "cli-1", dato(1));
    assert.deepEqual(r, { applied: true, outcome: "ok", revision: 1 });
    assert.equal(b.publicaciones.length, 1, "NO se publica de nuevo: ya estaba aplicada");
    assert.equal(b.lecturas.length, 2);
    assert.equal(b.datos.size, 1);
    assert.equal(m.snapshot().counters.get(`flux_state_writes_total{contract="${CONTRATO}",outcome="ok"}`), 1);
  });

  test("paso 1b: el id también se reconoce por la CABECERA Nats-Msg-Id si el cuerpo no se puede leer", async () => {
    // El broker conserva las cabeceras del mensaje almacenado (A-32).
    const b = new BrokerEnMemoria();
    b.perderAcuseEnElPrimero = true;
    // el cuerpo almacenado es el del SDK, pero comprobamos la rama de la cabecera sustituyendo el cuerpo tras aplicar
    const publicar = b.js.publish;
    b.js.publish = async (...a: Parameters<typeof publicar>) => {
      try {
        return await publicar(...a);
      } finally {
        const v = b.datos.get(a[0])!;
        b.datos.set(a[0], { ...v, bytes: new TextEncoder().encode("ilegible") });
      }
    };
    const r = await (await b.bus()).state.put(CONTRATO, "cli-1", dato(1));
    assert.equal(r.outcome, "ok");
    assert.equal(b.publicaciones.length, 1);
  });

  test("CONTROL del 1b: el MISMO escenario con el mensaje de OTRO sí es `stale` (la comprobación sabe fallar)", async () => {
    const b = new BrokerEnMemoria();
    b.escribeRival(SUBJECT, 7);
    const r = await (await b.bus()).state.put(CONTRATO, "cli-1", dato(7));
    assert.equal(r.outcome, "stale");
  });

  test("un valor ILEGIBLE no bloquea la clave: se sobrescribe, con el chequeo sobre SU secuencia", async () => {
    const b = new BrokerEnMemoria();
    b.escribeBasura(SUBJECT);
    const r = await (await b.bus()).state.put(CONTRATO, "cli-1", dato(1));
    assert.equal(r.outcome, "ok");
    assert.equal(b.publicaciones[0].esperada, 1, "S de la basura: sin ella no se podría sustituir");
    // un JSON sin `aggregateVersion` válido también se trata como «sin versión»
    const c = new BrokerEnMemoria();
    c.escribeRival(SUBJECT, 0);
    assert.equal((await (await c.bus()).state.put(CONTRATO, "cli-1", dato(1))).outcome, "ok");
    const d = new BrokerEnMemoria();
    d.escribeBasura(SUBJECT, "[1,2,3]");
    assert.equal((await (await d.bus()).state.put(CONTRATO, "cli-1", dato(1))).outcome, "ok");
  });

  test("delete es EL MISMO algoritmo con una lápida: stale con versión ≤, y borrar lo que no existe escribe la lápida", async () => {
    const b = new BrokerEnMemoria();
    b.escribeRival(SUBJECT, 3);
    const bus = await b.bus();
    assert.equal((await bus.state.delete(CONTRATO, "cli-1", { aggregateVersion: 3 })).outcome, "stale");
    assert.equal(b.publicaciones.length, 0);
    assert.equal((await bus.state.delete(CONTRATO, "cli-1", { aggregateVersion: 4 })).outcome, "ok");
    const lapida = JSON.parse(texto(b.publicaciones[0].bytes));
    assert.equal(lapida.type, "com.flux.state.clientes.cliente.deleted.v1");
    assert.deepEqual(lapida.data, { aggregateVersion: 4 });
    // una clave que nunca existió: S = 0 y applied
    const r = await bus.state.delete(CONTRATO, "fantasma", { aggregateVersion: 1 });
    assert.equal(r.applied, true);
    assert.equal(b.publicaciones.at(-1)!.esperada, 0);
  });

  test("get: 404 → null; una lápida → null; un valor → la entrada con tenantId y revision", async () => {
    const b = new BrokerEnMemoria();
    const bus = await b.bus();
    assert.equal(await bus.state.get(CONTRATO, "cli-1"), null);
    await bus.state.put(CONTRATO, "cli-1", dato(1));
    const e = await bus.state.get(CONTRATO, "cli-1");
    assert.deepEqual(
      { key: e!.key, tenantId: e!.tenantId, deleted: e!.deleted, aggregateVersion: e!.aggregateVersion, revision: e!.revision },
      { key: "cli-1", tenantId: "acme", deleted: false, aggregateVersion: 1, revision: 1 },
    );
    assert.equal((e!.data as { email: string }).email, "ana@example.com");
    await bus.state.delete(CONTRATO, "cli-1", { aggregateVersion: 2 });
    assert.equal(await bus.state.get(CONTRATO, "cli-1"), null, "una lápida es null, igual que una clave que nunca existió");
  });

  test("get: un mensaje cuyo tenantid, subject o type no coincide con el subject lanza STATE_SUBJECT_MISMATCH (POISON)", async () => {
    const mal = [
      { tenantid: "globex" },
      { subject: "otra-clave" },
      { type: "com.flux.state.clientes.pedido.v1" },
    ];
    for (const defecto of mal) {
      const b = new BrokerEnMemoria();
      const bus = await b.bus();
      await bus.state.put(CONTRATO, "cli-1", dato(1));
      const ev = { ...JSON.parse(texto(b.datos.get(SUBJECT)!.bytes)), ...defecto };
      b.datos.set(SUBJECT, { ...b.datos.get(SUBJECT)!, bytes: new TextEncoder().encode(JSON.stringify(ev)) });
      await assert.rejects(bus.state.get(CONTRATO, "cli-1"), (e: StateSubjectMismatchError) => {
        assert.equal(e.code, "STATE_SUBJECT_MISMATCH");
        assert.equal(e.fluxClass, ErrorClass.POISON);
        assert.equal(e.attribute, Object.keys(defecto)[0]);
        return true;
      });
    }
  });

  test("get: `restricted` en el mensaje es POISON (INVALID_DATACLASSIFICATION); sin aggregateVersion es PERMANENT", async () => {
    const b = new BrokerEnMemoria();
    const bus = await b.bus();
    await bus.state.put(CONTRATO, "cli-1", dato(1));
    const original = JSON.parse(texto(b.datos.get(SUBJECT)!.bytes));
    const poner = (ev: object) =>
      b.datos.set(SUBJECT, { ...b.datos.get(SUBJECT)!, bytes: new TextEncoder().encode(JSON.stringify(ev)) });
    poner({ ...original, dataclassification: "restricted" });
    await assert.rejects(bus.state.get(CONTRATO, "cli-1"), (e: { code?: string; fluxClass?: string }) => e.code === "INVALID_DATACLASSIFICATION" && e.fluxClass === ErrorClass.POISON);
    poner({ ...original, data: { clienteId: "cli-1" } });
    await assert.rejects(bus.state.get(CONTRATO, "cli-1"), (e: { code?: string; fluxClass?: string }) => e.code === "MISSING_AGGREGATE_VERSION" && e.fluxClass === ErrorClass.PERMANENT);
  });

  test("nadie contesta (el stream no existe, o no tiene allow_direct) → NOT_PROVISIONED, con las DOS causas", async () => {
    const b = new BrokerEnMemoria();
    b.sinStream = true;
    const bus = await b.bus();
    for (const op of [() => bus.state.get(CONTRATO, "cli-1"), () => bus.state.put(CONTRATO, "cli-1", dato(1)), () => bus.state.delete(CONTRATO, "cli-1", { aggregateVersion: 2 })]) {
      await assert.rejects(op(), (e: NotProvisionedError) => {
        assert.ok(e instanceof NotProvisionedError);
        assert.equal(e.code, "NOT_PROVISIONED");
        assert.equal(e.fluxClass, ErrorClass.PERMANENT);
        assert.match(e.message, /no existe/);
        assert.match(e.message, /allow_direct/, "debe nombrar la segunda causa: un stream sin allow_direct se ve igual");
        assert.match(e.message, /flux provision/);
        assert.match(e.message, /flux doctor/);
        assert.match(e.message, /STATE_CLIENTES/);
        return true;
      });
    }
    assert.equal(b.publicaciones.length, 0, "sin lectura no se publica nada");
  });

  test("el broker DENIEGA la lectura → PERMISSION_DENIED (un permiso, no un recurso), con el subject denegado", async () => {
    const b = new BrokerEnMemoria();
    const subjectDenegado = `$JS.API.DIRECT.GET.STATE_CLIENTES.${SUBJECT}`;
    b.falloDeLaLectura = new errors.RequestError("x", {
      cause: errors.PermissionViolationError.parse(`Permissions Violation for Publish to "${subjectDenegado}"`)!,
    });
    await assert.rejects((await b.bus()).state.get(CONTRATO, "cli-1"), (e: PermissionDeniedError) => {
      assert.ok(e instanceof PermissionDeniedError);
      assert.ok(!(e instanceof NotProvisionedError));
      assert.equal(e.subject, subjectDenegado);
      return true;
    });
  });

  test("isNoResponders: reconoce el error de la librería por la cadena de causas, y nada más", () => {
    assert.ok(isNoResponders(new errors.RequestError("x", { cause: new errors.NoRespondersError("a.b") })));
    assert.ok(isNoResponders(new errors.NoRespondersError("a.b")));
    for (const x of [new errors.TimeoutError(), new Error("no responders"), null, undefined, "no responders"]) {
      assert.ok(!isNoResponders(x));
    }
  });

  test("un timeout o un fallo de red de la lectura sube TAL CUAL: es RETRYABLE, no un recurso que falta", async () => {
    const b = new BrokerEnMemoria();
    b.falloDeLaLectura = new errors.TimeoutError();
    await assert.rejects((await b.bus()).state.put(CONTRATO, "cli-1", dato(1)), (e: Error) => e.name === "TimeoutError");
  });

  test("firma (07-signing.md): un put se firma UNA vez; `get` verifica, y una entrada manipulada o sin firma es POISON", async () => {
    const { privateKeyPem, publicKeyPem } = generateKeyPair();
    const firma = { privateKeyPem, keyId: "crm-api-1", publicKeys: { "crm-api-1": publicKeyPem }, verify: "require" as const };
    const b = new BrokerEnMemoria();
    const bus = await b.bus({ signing: firma });
    await bus.state.put(CONTRATO, "cli-1", dato(1));
    const guardado = JSON.parse(texto(b.datos.get(SUBJECT)!.bytes));
    assert.equal(guardado.signkeyid, "crm-api-1");
    assert.ok(guardado.signature);
    assert.equal(Object.keys(guardado).at(-1), "data", "`data` sigue siendo el último con la firma");
    assert.equal((await bus.state.get(CONTRATO, "cli-1"))!.aggregateVersion, 1, "firmada y verificada");

    // manipulada: el dato cambia y la firma ya no verifica
    const manipulada = { ...guardado, data: { ...guardado.data, nombre: "Otra Persona" } };
    b.datos.set(SUBJECT, { ...b.datos.get(SUBJECT)!, bytes: new TextEncoder().encode(JSON.stringify(manipulada)) });
    await assert.rejects(bus.state.get(CONTRATO, "cli-1"), (e: { opts?: { code?: string }; fluxClass?: string }) => e.opts?.code === "INVALID_SIGNATURE" && e.fluxClass === ErrorClass.POISON);

    // sin firma, con `require`
    const { signature: _s, signkeyid: _k, ...sinFirma } = guardado;
    b.datos.set(SUBJECT, { ...b.datos.get(SUBJECT)!, bytes: new TextEncoder().encode(JSON.stringify(sinFirma)) });
    await assert.rejects(bus.state.get(CONTRATO, "cli-1"), (e: { opts?: { code?: string } }) => e.opts?.code === "MISSING_SIGNATURE");

    // CONTROL: sin verificación, la misma entrada sin firma se lee
    const sinVerificar = await b.bus({});
    assert.equal((await sinVerificar.state.get(CONTRATO, "cli-1"))!.key, "cli-1");
    // y la lápida también va firmada
    await bus.state.delete(CONTRATO, "cli-1", { aggregateVersion: 2 });
    assert.ok(JSON.parse(texto(b.datos.get(SUBJECT)!.bytes)).signature);
  });

  test("hereda el contexto del evento en curso: tenant, correlationid y causationid", async () => {
    const b = new BrokerEnMemoria();
    const sinTenant = await b.bus({ tenantId: undefined });
    const { runWithContext } = await import("../src/context.js");
    await runWithContext({ correlationid: "corr-1", causationid: "ev-9", tenantid: "globex" }, () =>
      sinTenant.state.put(CONTRATO, "cli-1", dato(1)),
    );
    const ev = JSON.parse(texto(b.publicaciones[0].bytes));
    assert.equal(b.publicaciones[0].subject, "state.clientes.cliente.v1.globex.cli-1", "un estado derivado pertenece al tenant del evento que lo causó");
    assert.equal(ev.tenantid, "globex");
    assert.equal(ev.correlationid, "corr-1");
    assert.equal(ev.causationid, "ev-9");
    // la opción de la llamada gana sobre el contexto; y sin contexto, correlationid = id
    await runWithContext({ correlationid: "corr-1", causationid: "ev-9", tenantid: "globex" }, () =>
      sinTenant.state.put(CONTRATO, "cli-2", dato(1), { tenantId: "acme" }),
    );
    assert.equal(b.publicaciones[1].subject, "state.clientes.cliente.v1.acme.cli-2");
    await sinTenant.state.put(CONTRATO, "cli-3", dato(1), { tenantId: "acme" });
    const libre = JSON.parse(texto(b.publicaciones[2].bytes));
    assert.equal(libre.correlationid, libre.id);
  });

  test("NO escribe a mano ningún atributo del sobre: id UUIDv7, source, producerversion, time con 3 decimales y Z", async () => {
    const b = new BrokerEnMemoria();
    await (await b.bus()).state.put(CONTRATO, "cli-1", dato(1));
    const ev = JSON.parse(texto(b.publicaciones[0].bytes));
    assert.match(ev.id, /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    assert.equal(ev.source, "/produccion/crm-api");
    assert.equal(ev.producerversion, "2.1.0");
    assert.match(ev.time, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
    assert.equal(ev.dataclassification, "confidential", "un put sin classification usa `confidential`, NO el default de la conexión");
  });

  test("la clasificación: la de la conexión NO se usa; `public` e `internal` explícitas sí; la lápida va siempre `internal`", async () => {
    const b = new BrokerEnMemoria();
    const bus = await b.bus({ classification: "public" });
    await bus.state.put(CONTRATO, "a", dato(1));
    await bus.state.put(CONTRATO, "b", dato(1), { classification: "internal" });
    await bus.state.put(CONTRATO, "c", dato(1), { classification: "public" });
    await bus.state.delete(CONTRATO, "a", { aggregateVersion: 2 });
    const cl = b.publicaciones.map((p) => JSON.parse(texto(p.bytes)).dataclassification);
    assert.deepEqual(cl, ["confidential", "internal", "public", "internal"]);
  });

  test("el dataschema sale del mapa, del índice `state` del bundle o de schemaBaseUrl, y NUNCA de la red", async () => {
    const dsDe = async (extra: Partial<ConnectOptions>) => {
      const b = new BrokerEnMemoria();
      await (await b.bus({ schemaBaseUrl: undefined, ...extra })).state.put(CONTRATO, "k", dato(1));
      return JSON.parse(texto(b.publicaciones[0].bytes)).dataschema;
    };
    assert.equal(await dsDe({ schemaBaseUrl: "https://s.example/" }), "https://s.example/state/clientes/cliente/1.0.0.json");
    assert.equal(await dsDe({ validation: { mode: "off", bundle } }), "https://schemas.internal/state/clientes/cliente/1.0.0.json");
    assert.equal(await dsDe({ schemas: { [CONTRATO]: "https://x/y.json" }, schemaBaseUrl: "https://s.example" }), "https://x/y.json");
    // sin ninguno: un error con la receta, y sin tocar la red
    await assert.rejects(dsDe({}), /no hay dataschema para el estado/);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 6. Validación L3 en put y en delete
// ═════════════════════════════════════════════════════════════════════════════

describe("validación L3 en put y delete (el índice `state` del bundle)", () => {
  const estricto = { mode: "strict" as const, bundle };
  const valido = { clienteId: "cli-1", nombre: "Ana", email: "ana@example.com", aggregateVersion: 1 };

  test("un dato válido pasa", async () => {
    const b = new BrokerEnMemoria();
    const r = await (await b.bus({ validation: estricto })).state.put(CONTRATO, "cli-1", valido as never);
    assert.equal(r.outcome, "ok");
  });

  test("un dato que viola el esquema → INVALID_SCHEMA ANTES de leer ni escribir, y reporta TODOS los errores", async () => {
    const b = new BrokerEnMemoria();
    const bus = await b.bus({ validation: estricto });
    await assert.rejects(
      bus.state.put(CONTRATO, "cli-1", { clienteId: "cli-1", nombre: "", email: "no-es-un-email", pais: "ESP", aggregateVersion: 1 } as never),
      (e: { opts: { code?: string }; fluxClass?: string; message: string; errors: string[] }) => {
        assert.equal(e.opts.code, "INVALID_SCHEMA");
        assert.equal(e.fluxClass, ErrorClass.PERMANENT);
        assert.ok(e.errors.length >= 3, `debe reportar los tres campos mal, no solo el primero: ${e.errors.join(" | ")}`);
        return true;
      },
    );
    assert.equal(b.lecturas.length + b.publicaciones.length, 0, "falla ANTES de tocar el broker");
  });

  test("EL MENSAJE NO COPIA VALORES: cita la ruta del campo (`/email`), no su contenido", async () => {
    // Un error como `'ana@example.com' no cumple el formato` acabaría en logs, y en `dlqerror` 90 días.
    const b = new BrokerEnMemoria();
    const bus = await b.bus({ validation: estricto });
    const secretos = ["no-es-un-email-secreto", "Nombre Secreto"];
    await assert.rejects(
      bus.state.put(CONTRATO, "cli-1", { clienteId: "cli-1", nombre: secretos[1], email: secretos[0], pais: "x", aggregateVersion: 1 } as never),
      (e: Error & { errors: string[] }) => {
        for (const s of secretos) assert.ok(!e.message.includes(s), `el mensaje contiene el valor «${s}»: ${e.message}`);
        assert.match(e.message, /\/email/);
        assert.match(e.message, /\/pais/);
        return true;
      },
    );
  });

  test("la lápida valida contra el esquema fijo `eliminado`, y el bundle de eventos no basta", async () => {
    const b = new BrokerEnMemoria();
    const bus = await b.bus({ validation: estricto });
    assert.equal((await bus.state.delete(CONTRATO, "cli-1", { aggregateVersion: 1 })).outcome, "ok");
    // sin el esquema de la lápida en el bundle, `strict` falla con SCHEMA_NOT_FOUND (y no copia nada)
    const sinLapida = { ...bundle, schemas: Object.fromEntries(Object.entries(bundle.schemas).filter(([k]) => !k.includes("eliminado"))) };
    const c = new BrokerEnMemoria();
    await assert.rejects((await c.bus({ validation: { mode: "strict", bundle: sinLapida } })).state.delete(CONTRATO, "cli-1", { aggregateVersion: 1 }), /SCHEMA_NOT_FOUND|eliminado/);
  });

  test("CONTROL: con validation `off` el mismo dato roto SÍ se escribe (la validación es la que lo detiene)", async () => {
    const b = new BrokerEnMemoria();
    const r = await (await b.bus()).state.put(CONTRATO, "cli-1", { clienteId: "cli-1", nombre: "", email: "x", aggregateVersion: 1 } as never);
    assert.equal(r.outcome, "ok");
  });

  test("aggregateVersion se comprueba SIEMPRE, con o sin L3 (es una regla del protocolo, no del esquema)", async () => {
    const b = new BrokerEnMemoria();
    await rechazaCon("MISSING_AGGREGATE_VERSION", (await b.bus()).state.put(CONTRATO, "cli-1", { clienteId: "cli-1" } as never));
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 7. Métricas
// ═════════════════════════════════════════════════════════════════════════════

describe("flux_state_writes_total{contract,outcome} — 08-observability.md §2.4", () => {
  const cuenta = (m: InMemoryMetrics, outcome: string) =>
    m.snapshot().counters.get(`flux_state_writes_total{contract="${CONTRATO}",outcome="${outcome}"}`) ?? 0;

  test("ok, stale, conflict e invalid_schema", async () => {
    const m = new InMemoryMetrics();
    const b = new BrokerEnMemoria();
    const bus = await b.bus({ metrics: m, validation: { mode: "strict", bundle } });
    const ok = (v: number) => ({ clienteId: "cli-1", nombre: "Ana", email: "ana@example.com", aggregateVersion: v }) as never;
    await bus.state.put(CONTRATO, "cli-1", ok(2)); // ok
    await bus.state.put(CONTRATO, "cli-1", ok(2)); // stale
    await bus.state.delete(CONTRATO, "cli-1", { aggregateVersion: 3 }); // ok: el delete también cuenta
    await bus.state.delete(CONTRATO, "cli-1", { aggregateVersion: 3 }); // stale
    await assert.rejects(bus.state.put(CONTRATO, "cli-1", { clienteId: "cli-1" } as never)); // MISSING_AGGREGATE_VERSION
    await assert.rejects(bus.state.put(CONTRATO, "cli-1", { clienteId: "cli-1", nombre: "", email: "x", aggregateVersion: 9 } as never)); // INVALID_SCHEMA
    await assert.rejects(bus.state.delete(CONTRATO, "cli-1", { aggregateVersion: 0 })); // MISSING_AGGREGATE_VERSION
    assert.equal(cuenta(m, "ok"), 2);
    assert.equal(cuenta(m, "stale"), 2);
    assert.equal(cuenta(m, "invalid_schema"), 3, "se emite también cuando la llamada lanza");
    assert.equal(cuenta(m, "conflict"), 0);
  });

  test("los errores de programación NO se cuentan: clave, tenant, dominio reservado, clasificación y transporte", async () => {
    const m = new InMemoryMetrics();
    const b = new BrokerEnMemoria();
    const bus = await b.bus({ metrics: m });
    for (const op of [
      () => bus.state.put(CONTRATO, "a.b", dato(1)),
      () => bus.state.put(CONTRATO, "k", dato(1), { tenantId: "ACME" }),
      () => bus.state.put("state.x.v1", "k", dato(1)),
      () => bus.state.put(CONTRATO, "k", dato(1), { classification: "restricted" }),
    ]) {
      await assert.rejects(op());
    }
    b.falloDeLaLectura = new errors.TimeoutError();
    await assert.rejects(bus.state.put(CONTRATO, "k", dato(1)));
    assert.deepEqual([...m.snapshot().counters.keys()].filter((k) => k.startsWith("flux_state_writes_total")), []);
  });

  test("NUNCA la clave ni el tenant como etiqueta: la serie la fijan contract y outcome", async () => {
    const m = new InMemoryMetrics();
    const b = new BrokerEnMemoria();
    const bus = await b.bus({ metrics: m });
    for (const [t, k] of [["acme", "cli-1"], ["globex", "cli-2"], ["initech", "cli-3"]]) {
      await bus.state.put(CONTRATO, k, dato(1), { tenantId: t });
    }
    const series = [...m.snapshot().counters.keys()].filter((x) => x.startsWith("flux_state_writes_total"));
    assert.deepEqual(series, [`flux_state_writes_total{contract="${CONTRATO}",outcome="ok"}`], "tres tenants y tres claves son UNA serie");
    const salida = m.render();
    for (const prohibido of ["acme", "globex", "initech", "cli-1", "cli-2", "cli-3"]) {
      assert.ok(!salida.includes(prohibido), `la salida expone «${prohibido}»`);
    }
    assert.match(salida, /^# TYPE flux_state_writes_total counter$/m);
    assert.match(salida, /^flux_state_writes_total\{contract="clientes\.cliente\.v1",outcome="ok"\} 3$/m);
  });

  test("un MetricsSink recibe exactamente (contract, outcome): la firma no deja pasar otra cosa", async () => {
    const llamadas: unknown[][] = [];
    const sink: MetricsSink = {
      eventPublished() {},
      eventConsumed() {},
      handlerDuration() {},
      eventDlq() {},
      eventRetried() {},
      consumerPending() {},
      connectionState() {},
      stateWrite: (...a: unknown[]) => void llamadas.push(a),
    };
    const b = new BrokerEnMemoria();
    await (await b.bus({ metrics: sink })).state.put(CONTRATO, "cli-1", dato(1));
    assert.deepEqual(llamadas, [[CONTRATO, "ok"]]);
  });

  test("un MetricsSink de ANTES de la capacidad `state` (sin stateWrite) sigue funcionando", async () => {
    const viejo: MetricsSink = {
      eventPublished() {},
      eventConsumed() {},
      handlerDuration() {},
      eventDlq() {},
      eventRetried() {},
      consumerPending() {},
      connectionState() {},
    };
    const b = new BrokerEnMemoria();
    const r = await (await b.bus({ metrics: viejo })).state.put(CONTRATO, "cli-1", dato(1));
    assert.equal(r.outcome, "ok");
  });

  test("`observability.metrics` de protocol.json sigue con exactamente SIETE; la de estado vive en `state.metrics`", () => {
    assert.equal(Object.keys(protocolo.observability.metrics).length, 7);
    assert.ok(!("flux_state_writes_total" in protocolo.observability.metrics));
    const def = protocolo.state.metrics.flux_state_writes_total;
    assert.deepEqual(def.labels, ["contract", "outcome"]);
    assert.deepEqual(def.outcomes, ["ok", "stale", "conflict", "invalid_schema"]);
    // el SDK emite exactamente esos cuatro, y los siete de siempre más ésta (en total, ocho expuestas)
    const m = new InMemoryMetrics();
    for (const o of def.outcomes) m.stateWrite(CONTRATO, o);
    assert.equal([...m.snapshot().counters.keys()].length, 4);
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 8. La DLQ de estado: las funciones puras
// ═════════════════════════════════════════════════════════════════════════════

describe("DLQ de estado — 10-state.md §6", () => {
  const original = (): StateEventInput => entradaDe("state-put").input;
  const dlq = () => ({ reason: "permanent" as const, attempts: 1, consumer: "tienda-web__state_clientes_cliente_v1__acme", error: "PAIS_NO_SOPORTADO: x", dlqtime: "2026-08-20T10:25:40.117Z" });

  test("el dato NO se copia: data = { key, revision } y nada más", () => {
    const ev = buildStateDlqEvent({ original: original(), revision: 41, dlq: dlq() });
    assert.deepEqual(ev.data, { key: "cli-1", revision: 41 });
    const s = texto(serialize(ev));
    for (const pii of ["María Peña", "maria@example.com"]) assert.ok(!s.includes(pii), `la DLQ contiene «${pii}»`);
  });

  test("el resto del sobre queda como estaba, también `dataclassification`", () => {
    const ev = buildStateDlqEvent({ original: original(), revision: 41, dlq: dlq() });
    const o = buildStateEvent(original());
    for (const k of ["id", "source", "type", "time", "dataschema", "subject", "correlationid", "tenantid", "producerversion", "dataclassification", "partitionkey"] as const) {
      assert.equal(ev[k], o[k], k);
    }
    assert.equal(ev.dataclassification, "confidential");
  });

  test("NO conserva `signkeyid` ni `signature`: firman un `data` que ya no está", () => {
    const { privateKeyPem } = generateKeyPair();
    const firmado = createSigner({ privateKeyPem, keyId: "crm-api-1" })!.sign(buildStateEvent(original()));
    assert.ok("signature" in firmado && "signkeyid" in firmado, "el original sí iba firmado");
    const ev = toStateDlqEvent(firmado, { key: "cli-1", revision: 1, ...dlq() });
    assert.ok(!("signature" in ev) && !("signkeyid" in ev));
    assert.equal(Object.keys(ev).at(-1), "data");
  });

  test("la clave que se registra es la del SUBJECT, no la del sobre (que puede mentir)", () => {
    const mentiroso = buildStateEvent({ ...original(), key: "cli-1" });
    const ev = toStateDlqEvent({ ...mentiroso, subject: "otra" }, { key: "la-del-subject", revision: 9, ...dlq() });
    assert.equal(ev.data.key, "la-del-subject");
    assert.equal(ev.subject, "otra", "el resto del sobre queda como estaba");
  });

  test("`dlqerror` se corta a 1024 sin partir un par sustituto (una mitad se serializaría como \\ud83d)", () => {
    const largo = "x".repeat(1023) + "😀"; // el emoji empieza en el 1023 y se partiría
    const ev = toStateDlqEvent(buildStateEvent(original()), { key: "cli-1", revision: 1, ...dlq(), error: largo });
    assert.equal(ev.dlqerror!.length, 1023);
    assert.ok(!/\\u[0-9a-fA-F]{4}/.test(texto(serialize(ev))));
    assert.equal(toStateDlqEvent(buildStateEvent(original()), { key: "cli-1", revision: 1, ...dlq(), error: "y".repeat(2000) }).dlqerror!.length, 1024);
  });

  test("un mensaje ILEGIBLE: el sobre se sintetiza con lo único fiable —el subject— y NO lleva su cuerpo", () => {
    const ev = buildUnreadableStateDlqEvent({
      contract: CONTRATO,
      tenantId: "acme",
      key: "cli-9",
      revision: 77,
      source: "/produccion/tienda-web",
      producerversion: "5.0.0",
      id: "01924f8e-7c3a-7b2d-9e14-aaaaaaaaaaaa",
      time: "2026-08-20T10:25:39.412Z",
      dlq: { ...dlq(), reason: "poison" },
    });
    assert.equal(ev.id, ev.correlationid);
    assert.equal(ev.subject, "cli-9");
    assert.equal(ev.tenantid, "acme");
    assert.equal(ev.source, "/produccion/tienda-web", "el servicio que consume");
    assert.equal(ev.producerversion, "5.0.0");
    assert.equal(ev.dataclassification, "internal");
    assert.equal(ev.dlqreason, "poison");
    assert.equal(ev.type, "com.flux.state.clientes.cliente.v1");
    assert.deepEqual(ev.data, { key: "cli-9", revision: 77 });
    assert.ok(!texto(serialize(ev)).includes("rawBase64"));
    assert.equal(Object.keys(ev).at(-1), "data");
  });
});

// ═════════════════════════════════════════════════════════════════════════════
// 9. Lo que no necesita un broker y se rompe en silencio
// ═════════════════════════════════════════════════════════════════════════════

describe("flux_consumer_pending de estado es la SUMA de los durables, no el último sondeado", () => {
  test("dos durables (uno por tenant) publican las mismas etiquetas: se suman", () => {
    const p = new PendingSum();
    const serie = `state.${CONTRATO}|tienda-web__state_clientes_cliente_v1`;
    assert.equal(p.note(serie, "tienda-web__state_clientes_cliente_v1__acme", 5), 5);
    assert.equal(p.note(serie, "tienda-web__state_clientes_cliente_v1__globex", 7), 12, "sin sumar, el segundo pisaría al primero (7)");
    assert.equal(p.note(serie, "tienda-web__state_clientes_cliente_v1__acme", 1), 8, "el último valor de CADA durable");
    assert.equal(p.drop(serie, "tienda-web__state_clientes_cliente_v1__globex"), 1, "un watch que para deja de sumar");
    assert.equal(p.drop(serie, "tienda-web__state_clientes_cliente_v1__acme"), 0);
    assert.equal(p.drop(serie, "no-existe"), undefined);
  });

  test("las series de contratos distintos no se mezclan", () => {
    const p = new PendingSum();
    assert.equal(p.note("state.a.b.v1|svc", "d1", 3), 3);
    assert.equal(p.note("state.c.d.v1|svc", "d2", 4), 4);
  });
});

describe("la API pública: bus.state, los seis códigos y las funciones puras; sin tipos de NATS", () => {
  test("exporta lo que el arnés cruzado y las aplicaciones necesitan", () => {
    for (const n of [
      "stateSubject", "buildStateEvent", "buildStateTombstone", "buildStateDlqEvent", "toStateDlqEvent", "buildUnreadableStateDlqEvent",
      "stateDurableName", "stateFilterSubject", "stateStreamName", "stateDlqSubject", "parseStateContract", "isSequenceConflict",
      "InvalidTenantForStateError", "InvalidStateKeyError", "MissingAggregateVersionError", "StateSubjectMismatchError", "StateConflictError", "ReservedDomainError",
      "RESERVED_DOMAINS", "STATE_CONFLICT_CODES", "TOMBSTONE_DATASCHEMA", "stateSchemaUriFor",
    ]) {
      assert.ok(n in publica, `index.ts no exporta ${n}`);
    }
    assert.deepEqual([...publica.RESERVED_DOMAINS].sort(), [...protocolo.naming.reservedDomains.names].sort());
    assert.deepEqual([...publica.STATE_CONFLICT_CODES], protocolo.state.write.conflictErrCodes);
  });

  test("`StateClient` solo se exporta como TIPO: su constructor recibe una conexión de NATS", () => {
    assert.ok(!("StateClient" in publica));
  });

  test("las funciones puras no tocan el reloj: dos llamadas separadas en el tiempo dan lo mismo", async () => {
    const f = entradaDe("state-put").input;
    const a = serialize(buildStateEvent(f));
    await new Promise((r) => setTimeout(r, 15));
    assert.deepEqual(serialize(buildStateEvent(f)), a);
    assert.equal(JSON.parse(texto(a)).time, f.time);
  });

  test("buildStateEvent rechaza lo que `put` rechazaría (no se puede construir una entrada ilegal)", () => {
    const f = entradaDe("state-put").input;
    assert.throws(() => buildStateEvent({ ...f, key: "a.b" }), InvalidStateKeyError);
    assert.throws(() => buildStateEvent({ ...f, tenantId: "ACME" }), InvalidTenantForStateError);
    assert.throws(() => buildStateEvent({ ...f, contract: "state.x.v1" }), ReservedDomainError);
    assert.throws(() => buildStateEvent({ ...f, data: { x: 1 } }), MissingAggregateVersionError);
    assert.throws(() => buildStateEvent({ ...f, classification: "restricted" }), InvalidDataClassificationError);
    assert.throws(() => buildStateTombstone({ ...entradaDe("state-tombstone").input, aggregateVersion: 0 }), MissingAggregateVersionError);
  });
});
