/**
 * El modelo de estado de la CLI, SIN broker: validación, nombres, lectura del sobre y las
 * funciones puras de `flux doctor`. Cada comprobación va en los dos sentidos: lo correcto pasa
 * y lo incorrecto FALLA (una comprobación que solo se ha visto en verde no demuestra nada).
 *
 * Los casos válidos e inválidos salen de `protocol.json` (bloque `state`), que es la fuente que
 * también usan los SDKs: si la CLI y un SDK discreparan sobre qué es una clave, esto lo vería.
 */

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  canon, cubre, durableDe, ErrorEstado, filtroDe, inspeccionar, parsearSubject, streamDe, subjectDe,
  validarClave, validarContrato, validarServicio, validarTenant, resumenDlqEstado, configDurableEstado,
  diferenciasConfig,
} from "../src/state-model.mjs";
import { auditarDlqDeEstado, auditarDurableEstado, auditarStreamEstado } from "../src/doctor-state.mjs";
import { datosOcultos, redactarSobre } from "../src/privacidad.mjs";
import { resolverStream } from "../src/tail.mjs";

const P = JSON.parse(readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "..", "protocol.json"), "utf8")).state;
const NS = 1e9;

const falla = (fn, code) => assert.throws(fn, (e) => e instanceof ErrorEstado && (code === undefined || e.code === code), `debía lanzar ${code ?? "ErrorEstado"}`);

describe("validación (10-state.md §2.3, §2.4, §2.7), con los vectores de protocol.json", () => {
  for (const k of P.key.valid) test(`clave válida «${k}»`, () => assert.equal(validarClave(k), k));
  for (const k of P.key.invalid) test(`clave inválida «${k}» → INVALID_STATE_KEY`, () => falla(() => validarClave(k), "INVALID_STATE_KEY"));
  test("un salto de línea final NO pasa (en Python y PHP el `$` lo aceptaría)", () => falla(() => validarClave("cli-1\n"), "INVALID_STATE_KEY"));
  test("128 caracteres sí, 129 no", () => {
    assert.equal(validarClave("a".repeat(128)).length, 128);
    falla(() => validarClave("a".repeat(129)), "INVALID_STATE_KEY");
  });

  for (const t of P.tenant.valid) test(`tenant válido «${t}»`, () => assert.equal(validarTenant(t), t));
  for (const t of P.tenant.invalid) test(`tenant inválido «${t}» → INVALID_TENANT_FOR_STATE`, () => falla(() => validarTenant(t), "INVALID_TENANT_FOR_STATE"));
  test("tenant: 64 caracteres sí, 65 no", () => {
    assert.equal(validarTenant("a".repeat(64)).length, 64);
    falla(() => validarTenant("a".repeat(65)), "INVALID_TENANT_FOR_STATE");
  });

  for (const c of P.contract.valid) test(`contrato válido «${c}»`, () => assert.equal(validarContrato(c).contrato, c));
  for (const c of P.contract.invalid) {
    test(`contrato inválido «${c}»`, () => falla(() => validarContrato(c)));
  }
  test("`state.clientes.cliente.v1` (un subject, no un contrato) se rechaza con una pista", () => {
    assert.throws(() => validarContrato("state.clientes.cliente.v1"), /prefijo `state\.`/);
    assert.throws(() => validarContrato("clientes.cliente.v1.creado"), /más de 3 tokens/);
  });
  for (const dom of ["dlq", "state", "rpc", "system"]) {
    test(`dominio reservado «${dom}» → RESERVED_DOMAIN`, () => falla(() => validarContrato(`${dom}.cliente.v1`), "RESERVED_DOMAIN"));
  }
  test("`state-clientes` NO está reservado: convive con STATE_CLIENTES (A-43)", () => assert.equal(validarContrato("state-clientes.cliente.v1").dom, "state-clientes"));

  test("servicio: kebab-case en minúsculas", () => {
    assert.equal(validarServicio("tienda-web"), "tienda-web");
    for (const malo of ["Tienda", "tienda_web", "tienda.web", "", "-x"]) falla(() => validarServicio(malo));
  });
});

describe("nombres (10-state.md §2.1, §2.2, §5.2)", () => {
  test("stream, subject y filtro", () => {
    assert.equal(streamDe("clientes"), "STATE_CLIENTES");
    assert.equal(streamDe("linea-envio"), "STATE_LINEA_ENVIO");
    assert.equal(subjectDe("clientes.cliente.v1", "acme", "cli-1"), "state.clientes.cliente.v1.acme.cli-1");
    assert.equal(filtroDe("clientes.cliente.v1", "acme"), "state.clientes.cliente.v1.acme.*");
    assert.equal(filtroDe("clientes.cliente.v1"), "state.clientes.cliente.v1.*.*");
  });

  test("el durable de la spec, byte a byte, y sus variantes", () => {
    assert.equal(durableDe("tienda-web", "clientes.cliente.v1", "acme"), "tienda-web__state_clientes_cliente_v1__acme");
    assert.equal(durableDe("tienda-web", "clientes.cliente.v1"), "tienda-web__state_clientes_cliente_v1");
    // los guiones del contrato y del tenant pasan a `_`; los del servicio se quedan
    assert.equal(durableDe("tienda-web", "logistica.linea-envio.v2", "acme-corp"), "tienda-web__state_logistica_linea_envio_v2__acme_corp");
  });

  test("un durable que no cabe (255) se rechaza en vez de dejar que NATS conteste con un 10102", () => {
    falla(() => durableDe("s".repeat(250), "clientes.cliente.v1", "acme"));
  });

  for (const d of P.watch.durable.valid) test(`el patrón de durable de protocol.json acepta «${d}»`, () => assert.match(d, canon().reDurable));
  for (const d of P.watch.durable.invalid) test(`…y rechaza «${d}»`, () => assert.doesNotMatch(d, canon().reDurable));

  for (const s of P.subject.valid) test(`subject de estado válido «${s}»`, () => assert.ok(parsearSubject(s)));
  for (const s of P.subject.invalid) test(`subject de estado inválido «${s}»`, () => assert.equal(parsearSubject(s), null));
  test("una clave `a.b` deja un subject de 7 tokens: no es canónico (A-41)", () => assert.equal(parsearSubject("state.clientes.cliente.v1.acme.a.b"), null));
  test("parsearSubject separa tenant y clave", () => {
    assert.deepEqual(parsearSubject("state.clientes.cliente.v1.acme.cli-1"), {
      dom: "clientes", agr: "cliente", major: "v1", contrato: "clientes.cliente.v1", tenant: "acme", clave: "cli-1",
    });
  });
});

describe("cubre(): ¿captura un stream este subject? (la DLQ de estado)", () => {
  const casos = [
    ["dlq.state.clientes.>", "dlq.state.clientes.>", true],
    ["dlq.>", "dlq.state.clientes.>", true],
    ["dlq.state.>", "dlq.state.clientes.>", true],
    ["dlq.*.clientes.>", "dlq.state.clientes.>", true],
    ["dlq.clientes.>", "dlq.state.clientes.>", false],   // la DLQ de eventos NO captura la de estado (A-44)
    ["dlq.state.pedidos.>", "dlq.state.clientes.>", false],
    ["dlq.state.clientes.cliente.v1", "dlq.state.clientes.>", false], // más estrecho
    ["dlq.state.clientes.*", "dlq.state.clientes.>", false],          // `*` no cubre un `>`
    ["state.clientes.>", "dlq.state.clientes.>", false],
  ];
  for (const [patron, objetivo, esperado] of casos) {
    test(`${patron} ${esperado ? "cubre" : "NO cubre"} ${objetivo}`, () => assert.equal(cubre(patron, objetivo), esperado));
  }
});

describe("inspeccionar(): qué le pasa a una entrada (10-state.md §5.4)", () => {
  const info = parsearSubject("state.clientes.cliente.v1.acme.cli-1");
  const sobre = (cambios = {}, data = { clienteId: "cli-1", aggregateVersion: 7 }) => new TextEncoder().encode(JSON.stringify({
    specversion: "1.0", id: "i", source: "/x", type: "com.flux.state.clientes.cliente.v1", time: "t", subject: "cli-1",
    correlationid: "c", tenantid: "acme", producerversion: "1", dataclassification: "confidential", partitionkey: "cli-1", ...cambios, data,
  }));

  test("una entrada sana: sin problemas, con su versión", () => {
    const r = inspeccionar(sobre(), info);
    assert.deepEqual(r.problemas, []);
    assert.equal(r.aggregateVersion, 7);
    assert.equal(r.lapida, false);
  });
  test("una lápida se reconoce por su type, y es sana", () => {
    const r = inspeccionar(sobre({ type: "com.flux.state.clientes.cliente.deleted.v1" }, { aggregateVersion: 8 }), info);
    assert.equal(r.lapida, true);
    assert.deepEqual(r.problemas, []);
  });
  test("tenantid del sobre ≠ tenant del subject → STATE_SUBJECT_MISMATCH", () => {
    const r = inspeccionar(sobre({ tenantid: "globex" }), info);
    assert.ok(r.problemas.some((p) => p.code === "STATE_SUBJECT_MISMATCH" && /tenantid/.test(p.texto)));
  });
  test("subject del sobre ≠ clave del subject → STATE_SUBJECT_MISMATCH", () => {
    const r = inspeccionar(sobre({ subject: "cli-2" }), info);
    assert.ok(r.problemas.some((p) => p.code === "STATE_SUBJECT_MISMATCH" && /clave/.test(p.texto)));
  });
  test("un type que no es el del contrato → STATE_SUBJECT_MISMATCH (decisión 8)", () => {
    const r = inspeccionar(sobre({ type: "com.flux.state.pedidos.pedido.v1" }), info);
    assert.ok(r.problemas.some((p) => p.code === "STATE_SUBJECT_MISMATCH" && /type/.test(p.texto)));
  });
  test("aggregateVersion ausente, 0, decimal o cadena → MISSING_AGGREGATE_VERSION", () => {
    for (const v of [undefined, 0, 7.5, "7", -1, 2 ** 53]) {
      const r = inspeccionar(sobre({}, { aggregateVersion: v }), info);
      assert.ok(r.problemas.some((p) => p.code === "MISSING_AGGREGATE_VERSION"), `versión ${String(v)}`);
    }
  });
  test("restricted no se admite en estado → INVALID_DATACLASSIFICATION", () => {
    assert.ok(inspeccionar(sobre({ dataclassification: "restricted" }), info).problemas.some((p) => p.code === "INVALID_DATACLASSIFICATION"));
  });
  test("un cuerpo que no es JSON no revienta: se informa", () => {
    const r = inspeccionar(new TextEncoder().encode("{roto"), info);
    assert.equal(r.legible, false);
    assert.match(r.problemas[0].texto, /no es JSON/);
  });
  test("un cuerpo JSON que no es un objeto tampoco", () => {
    assert.equal(inspeccionar(new TextEncoder().encode("[1,2]"), info).legible, false);
  });
});

describe("privacidad: qué se imprime del data", () => {
  test("solo public e internal se muestran; confidential, restricted y AUSENTE se ocultan (falla cerrado)", () => {
    assert.equal(datosOcultos("public"), false);
    assert.equal(datosOcultos("internal"), false);
    for (const c of ["confidential", "restricted", undefined, null, "secreto"]) assert.equal(datosOcultos(c), true, String(c));
  });
  test("redactarSobre sustituye `data`, conserva el orden (data el último) y no toca el original", () => {
    const e = { id: "1", dataclassification: "confidential", data: { email: "ana@example.com" } };
    const r = redactarSobre(e, false);
    assert.equal(r.data, "[oculto: dataclassification=confidential]");
    assert.deepEqual(Object.keys(r), ["id", "dataclassification", "data"]);
    assert.deepEqual(e.data, { email: "ana@example.com" });
    assert.equal(redactarSobre(e, true), e, "con --show-data se devuelve tal cual");
    const i = { dataclassification: "internal", data: { x: 1 } };
    assert.equal(redactarSobre(i, false), i);
  });
});

describe("tail: qué stream hay detrás de un patrón", () => {
  const casos = [
    ["pedidos.>", "EVT_PEDIDOS", "evento"],
    ["pedidos.pedido.v1.creado", "EVT_PEDIDOS", "evento"],
    ["dlq.pedidos.>", "DLQ_PEDIDOS", "dlq"],
    ["state.clientes.>", "STATE_CLIENTES", "estado"],
    ["state.clientes.cliente.v1.acme.*", "STATE_CLIENTES", "estado"],
    ["state.linea-envio.>", "STATE_LINEA_ENVIO", "estado"],
    ["dlq.state.clientes.>", "DLQ_CLIENTES", "dlq-estado"], // antes caía en DLQ_STATE, que no existe
  ];
  for (const [patron, stream, tipo] of casos) {
    test(`${patron} → ${stream} (${tipo})`, () => assert.deepEqual(resolverStream(patron), { stream, tipo }));
  }
  for (const malo of [">", "*.pedido.v1.creado", "state.>", "state.*.cliente", "dlq.>", "dlq.state.>", "dlq.*.x"]) {
    test(`«${malo}» no tiene dominio concreto: se rechaza`, () => assert.throws(() => resolverStream(malo), /token concreto/));
  }
});

describe("DLQ de estado: su data es EXACTAMENTE { key, revision } (10-state.md §6)", () => {
  test("lo correcto", () => assert.deepEqual(resumenDlqEstado({ data: { key: "cli-1", revision: 41 } }), { ok: true, key: "cli-1", revision: 41, extra: [] }));
  test("con campos de más, no (es el dato filtrándose a una DLQ de 90 días)", () => {
    const r = resumenDlqEstado({ data: { key: "cli-1", revision: 41, email: "ana@example.com" } });
    assert.equal(r.ok, false);
    assert.deepEqual(r.extra, ["email"]);
  });
  test("sin key o con otra forma, tampoco", () => {
    assert.equal(resumenDlqEstado({ data: { revision: 1 } }).ok, false);
    assert.equal(resumenDlqEstado({ data: "x" }).ok, false);
    assert.equal(resumenDlqEstado({}).ok, false);
  });
});

// ─── flux doctor: las funciones puras, en los dos sentidos ───────────────────────────────────────

const streamSano = (cambios = {}) => ({
  name: "STATE_CLIENTES", subjects: ["state.clientes.>"], storage: "file", retention: "limits",
  max_msgs_per_subject: 1, allow_direct: true, max_age: 0, discard: "old", duplicate_window: 120 * NS,
  num_replicas: 3, max_msgs: -1, max_bytes: -1, ...cambios,
});
const niveles = (hs) => hs.map((h) => h.level);

describe("doctor · STATE_<DOM> (10-state.md §2.1)", () => {
  test("la configuración canónica, con 3 réplicas: ningún hallazgo", () => assert.deepEqual(auditarStreamEstado(streamSano()), []));
  test("con 1 réplica: solo una nota (desarrollo)", () => assert.deepEqual(niveles(auditarStreamEstado(streamSano({ num_replicas: 1 }))), ["info"]));
  test("max_age=0 NO es un fallo en estado (en EVT/DLQ el doctor lo avisa; aquí es lo correcto)", () => {
    assert.equal(auditarStreamEstado(streamSano()).some((h) => /max_age/.test(h.message)), false);
  });

  const roturas = [
    ["max_msgs_per_subject ausente (un log, no un mapa)", { max_msgs_per_subject: -1 }, /max_msgs_per_subject=-1/],
    ["max_msgs_per_subject sin valor", { max_msgs_per_subject: undefined }, /max_msgs_per_subject=\(sin valor\)/],
    ["max_msgs_per_subject: 2", { max_msgs_per_subject: 2 }, /max_msgs_per_subject=2/],
    ["allow_direct apagado", { allow_direct: false }, /allow_direct=false/],
    ["allow_direct ausente", { allow_direct: undefined }, /allow_direct=false/],
    ["max_age de 30 días (el estado caduca)", { max_age: 30 * 86400 * NS }, /max_age=30d/],
    ["discard=new", { discard: "new" }, /discard=new/],
    ["discard=old con discard_new_per_subject", { discard_new_per_subject: true }, /discard_new_per_subject/],
    ["almacenamiento en memoria", { storage: "memory" }, /storage=memory/],
    ["retención por interés", { retention: "interest" }, /retention=interest/],
    ["no captura state.<dom>.>", { subjects: ["state.otro.>"] }, /no captura state\.clientes\.>/],
    ["sin subjects", { subjects: [] }, /no captura/],
  ];
  for (const [titulo, cambios, patron] of roturas) {
    test(`ERROR: ${titulo}`, () => {
      const hs = auditarStreamEstado(streamSano(cambios));
      assert.ok(hs.some((h) => h.level === "error" && patron.test(h.message)), JSON.stringify(hs));
    });
  }
  test("subject de más: aviso, no error", () => {
    const hs = auditarStreamEstado(streamSano({ subjects: ["state.clientes.>", "extra.>"] }));
    assert.deepEqual(niveles(hs), ["warn"]);
  });
  test("duplicate_window distinto de 2m: solo una nota (no interviene en las escrituras, A-33)", () => {
    const hs = auditarStreamEstado(streamSano({ duplicate_window: 60 * NS }));
    assert.deepEqual(niveles(hs), ["info"]);
    assert.match(hs[0].message, /duplicate_window=1m, el canónico es 2m/);
  });
  test("réplicas pares: aviso", () => assert.deepEqual(niveles(auditarStreamEstado(streamSano({ num_replicas: 2 }))), ["warn"]));
  test("un tope global de bytes: aviso (borra la clave más antigua sin lápida)", () => {
    const hs = auditarStreamEstado(streamSano({ max_bytes: 1 << 20 }));
    assert.ok(hs.some((h) => h.level === "warn" && /max_bytes/.test(h.message)));
  });
  test("un dominio con guion: el subject manda, no el nombre", () => {
    const cfg = streamSano({ name: "STATE_LINEA_ENVIO", subjects: ["state.linea-envio.>"] });
    assert.deepEqual(auditarStreamEstado(cfg), []);
    assert.ok(auditarStreamEstado({ ...cfg, subjects: ["state.linea_envio.>"] }).some((h) => h.level === "error"));
  });
});

describe("doctor · la DLQ de un dominio con estado (10-state.md §6)", () => {
  const estado = streamSano();
  const dlq = (subjects) => ({ name: "DLQ_CLIENTES", subjects });
  test("captura dlq.<dom>.> y dlq.state.<dom>.>: bien", () => assert.deepEqual(auditarDlqDeEstado(estado, dlq(["dlq.clientes.>", "dlq.state.clientes.>"])), []));
  test("ERROR: la DLQ de v1.0, que solo captura dlq.<dom>.>", () => {
    const hs = auditarDlqDeEstado(estado, dlq(["dlq.clientes.>"]));
    assert.equal(hs[0].level, "error");
    assert.match(hs[0].message, /no captura dlq\.state\.clientes\.>/);
  });
  test("ERROR: no existe DLQ_<DOM>", () => {
    const hs = auditarDlqDeEstado(estado, undefined);
    assert.equal(hs[0].level, "error");
    assert.match(hs[0].message, /no existe DLQ_CLIENTES/);
  });
  test("un dominio con guion usa el subject con guion", () => {
    const e = streamSano({ name: "STATE_LINEA_ENVIO", subjects: ["state.linea-envio.>"] });
    assert.deepEqual(auditarDlqDeEstado(e, { name: "DLQ_LINEA_ENVIO", subjects: ["dlq.linea-envio.>", "dlq.state.linea-envio.>"] }), []);
    assert.equal(auditarDlqDeEstado(e, { name: "DLQ_LINEA_ENVIO", subjects: ["dlq.linea-envio.>"] })[0].level, "error");
  });
});

describe("doctor · el durable de un observador (10-state.md §5.2)", () => {
  const sano = (cambios = {}) => ({
    ...configDurableEstado("tienda-web__state_clientes_cliente_v1__acme", "state.clientes.cliente.v1.acme.*"), ...cambios,
  });
  const errores = (cfg) => auditarDurableEstado(cfg).findings.filter((f) => f.level === "error");

  test("el durable canónico: nada que decir", () => {
    const r = auditarDurableEstado(sano());
    assert.deepEqual(r.findings, []);
    assert.deepEqual(r.problemas, []);
  });
  test("sin tenant (filtro `*.*`) y sin sufijo: también canónico", () => {
    const cfg = configDurableEstado("tienda-web__state_clientes_cliente_v1", "state.clientes.cliente.v1.*.*");
    assert.deepEqual(auditarDurableEstado(cfg).findings, []);
  });
  test("filter_subjects con UN solo elemento vale (el servidor lo devuelve así)", () => {
    const { filter_subject, ...resto } = sano();
    assert.deepEqual(auditarDurableEstado({ ...resto, filter_subjects: [filter_subject] }).findings, []);
  });

  for (const d of ["all", "new", "last", "by_start_sequence", "by_start_time"]) {
    test(`ERROR: deliver_policy=${d}`, () => {
      const r = auditarDurableEstado(sano({ deliver_policy: d }));
      assert.ok(r.findings.some((f) => f.level === "error" && new RegExp(`deliver_policy=${d}`).test(f.message)));
      assert.ok(r.problemas.includes("deliver≠last_per_subject"));
    });
  }
  test("`all` explica por qué es casi inocuo (A-35) y `new` por qué no", () => {
    assert.match(auditarDurableEstado(sano({ deliver_policy: "all" })).findings[0].hint, /MIENTRAS el stream tenga max_msgs_per_subject=1/);
    assert.match(auditarDurableEstado(sano({ deliver_policy: "new" })).findings[0].hint, /NO recibe la foto completa/);
  });
  test("ERROR: un durable de un tenant SIN el tenant en el nombre (reparte mensajes, A-47)", () => {
    const cfg = configDurableEstado("tienda-web__state_clientes_cliente_v1", "state.clientes.cliente.v1.acme.*");
    const e = errores(cfg);
    assert.ok(e.some((f) => /no lleva tenant/.test(f.message) && /COMPARTIDO/.test(f.hint)));
  });
  test("ERROR: el nombre dice un tenant y el filtro admite todos", () => {
    const cfg = configDurableEstado("tienda-web__state_clientes_cliente_v1__acme", "state.clientes.cliente.v1.*.*");
    assert.ok(errores(cfg).some((f) => /lleva el tenant «acme»/.test(f.message)));
  });
  test("ERROR: el nombre y el filtro dicen tenants distintos", () => {
    const cfg = configDurableEstado("tienda-web__state_clientes_cliente_v1__acme", "state.clientes.cliente.v1.globex.*");
    assert.equal(errores(cfg).length, 1);
  });
  test("ERROR: el nombre dice un contrato y el filtro, otro", () => {
    const cfg = configDurableEstado("tienda-web__state_clientes_cliente_v1__acme", "state.pedidos.pedido.v1.acme.*");
    assert.ok(errores(cfg).some((f) => /contrato/.test(f.message)));
  });
  test("ERROR: un filtro más ancho (`>`) o varios", () => {
    assert.ok(errores(sano({ filter_subject: "state.clientes.>" })).length >= 1);
    const { filter_subject, ...resto } = sano();
    assert.ok(errores({ ...resto, filter_subjects: ["state.clientes.cliente.v1.acme.*", "state.clientes.cliente.v1.globex.*"] }).length >= 1);
  });
  test("tenant con guion: el nombre lo lleva con `_` y el filtro con `-`, y es coherente", () => {
    const cfg = configDurableEstado("tienda-web__state_logistica_linea_envio_v2__acme_corp", "state.logistica.linea-envio.v2.acme-corp.*");
    assert.deepEqual(auditarDurableEstado(cfg).findings, []);
  });
  test("AVISO: config canónica alterada (max_ack_pending, max_deliver, replay)", () => {
    const r = auditarDurableEstado(sano({ max_ack_pending: 10, max_deliver: 3, replay_policy: "original" }));
    assert.equal(r.findings.filter((f) => f.level === "warn").length, 3);
    assert.ok(r.problemas.includes("max_ack_pending≠canónico"));
  });

  test("diferenciasConfig: sin diferencias contra sí mismo, con diferencias contra otro", () => {
    assert.deepEqual(diferenciasConfig(sano(), sano()), []);
    assert.equal(diferenciasConfig(sano(), sano({ deliver_policy: "all" })).length, 1);
  });
});
