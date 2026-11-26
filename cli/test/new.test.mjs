/**
 * `flux new event | state | rpc <nombre>`: sin broker (los nombres, las plantillas, los ficheros, el deshacer) y con él (lo que crea lo acepta
 * `gen-acl`, `flux provision` y `flux doctor` contra un nats-server real). Cada garantía va en los dos sentidos: lo correcto se crea y lo
 * incorrecto NO deja ni un fichero.
 *
 *   NATS_SERVER=/ruta/a/nats-server npm test
 *   FLUX_REQUIRE_BROKER=1 npm test      → sin nats-server, las pruebas con broker FALLAN (no se saltan)
 */

import { test, describe, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { arrancaBroker, conecta, CLI, detectarBroker, flux, REQUIRE } from "./helpers.mjs";
import { campoId, plantillas, validarNombre } from "../src/new.mjs";

const RAIZ = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const PROTOCOLO = JSON.parse(readFileSync(join(RAIZ, "protocol.json"), "utf8"));
const GEN = join(RAIZ, "scripts", "gen-acl.mjs");

/** Una carpeta de proyecto nueva por prueba. */
const proyecto = () => mkdtempSync(join(tmpdir(), "flux-new-"));
const limpiar = (d) => rmSync(d, { recursive: true, force: true });

/** `flux new …` como proceso, en `cwd`, SIN broker. */
function fluxNew(cwd, ...args) {
  const r = spawnSync(process.execPath, [CLI, "new", ...args], { cwd, encoding: "utf8", env: { ...process.env, NO_COLOR: "1" } });
  return { code: r.status, out: r.stdout, err: r.stderr, todo: `${r.stdout}${r.stderr}` };
}
/** Todos los ficheros Y directorios de un árbol (estos, con una `/` al final), relativos, ordenados: un directorio vacío que se queda también es basura. */
function arbol(dir, base = dir) {
  if (!existsSync(dir)) return [];
  return readdirSync(dir, { withFileTypes: true })
    .flatMap((e) => (e.isDirectory() ? [`${join(dir, e.name).slice(base.length + 1)}/`, ...arbol(join(dir, e.name), base)] : [join(dir, e.name).slice(base.length + 1)]))
    .sort();
}
/** Solo los ficheros. */
const ficheros = (dir) => arbol(dir).filter((f) => !f.endsWith("/"));
const leer = (...ruta) => JSON.parse(readFileSync(join(...ruta), "utf8"));

// ─── Sin broker ──────────────────────────────────────────────────────────────────────────────────

describe("flux new — los nombres: rechaza lo que protocol.json rechaza", () => {
  const casos = [
    ["event", PROTOCOLO.naming.subject.valid, PROTOCOLO.naming.subject.invalid],
    ["state", PROTOCOLO.state.contract.valid, PROTOCOLO.state.contract.invalid],
    ["rpc", PROTOCOLO.rpc.operation.valid, PROTOCOLO.rpc.operation.invalid],
  ];
  for (const [tipo, validos, invalidos] of casos) {
    test(`${tipo}: acepta los ${validos.length} nombres válidos de protocol.json`, () => {
      for (const n of validos) {
        const r = validarNombre(tipo, n);
        assert.deepEqual(r.problemas, [], `«${n}» debería valer: ${JSON.stringify(r.problemas)}`);
      }
    });
    test(`${tipo}: rechaza los ${invalidos.length} inválidos, y CADA rechazo dice qué está mal y cómo se arregla`, () => {
      for (const n of invalidos) {
        const r = validarNombre(tipo, n);
        assert.ok(r.problemas.length > 0, `«${n}» debería rechazarse`);
        for (const [que, como] of r.problemas) {
          assert.ok(que.length > 5 && como.length > 15, `«${n}»: un rechazo sin qué o sin cómo arreglarlo: ${JSON.stringify([que, como])}`);
        }
      }
    });
  }

  test("los cuatro dominios reservados se rechazan en los tres tipos, con RESERVED_DOMAIN", () => {
    for (const dom of PROTOCOLO.naming.reservedDomains.names) {
      for (const [tipo, nombre] of [["event", `${dom}.cliente.v1.creado`], ["state", `${dom}.cliente.v1`], ["rpc", `${dom}.cliente.v1.obtener`]]) {
        const r = validarNombre(tipo, nombre);
        assert.ok(r.problemas.some(([q]) => q.includes("RESERVED_DOMAIN")), `${tipo} ${nombre}`);
      }
    }
    // el prefijo pegado: dice cuál es el nombre bueno
    assert.match(validarNombre("rpc", "rpc.clientes.cliente.v1.obtener").problemas[0][1], /escribe «clientes\.cliente\.v1\.obtener»/);
    assert.match(validarNombre("state", "state.clientes.cliente.v1").problemas[0][1], /escribe «clientes\.cliente\.v1»/);
  });

  test("el nombre de una operación que protocol.json rechaza (operationName.invalid) también se rechaza", () => {
    for (const op of PROTOCOLO.rpc.operationName.invalid.filter((x) => x && !x.includes("."))) {
      assert.ok(validarNombre("rpc", `clientes.cliente.v1.${op}`).problemas.length > 0, `«${op}»`);
    }
  });

  test("las confusiones habituales dicen cuál es el comando bueno", () => {
    assert.match(validarNombre("event", "clientes.cliente.v1").problemas[0][1], /flux new state clientes\.cliente\.v1/);
    assert.match(validarNombre("state", "clientes.cliente.v1.registrado").problemas[0][1], /clientes\.cliente\.v1/);
    assert.match(validarNombre("rpc", "clientes.cliente.v1").problemas[0][1], /obtener/);
    assert.match(validarNombre("event", "pedidos.pedido.v1.crear").problemas[0][1], /flux new rpc pedidos\.pedido\.v1\.crear/);
    assert.match(validarNombre("event", "pedidos.pedido.v1.actualizado").problemas[0][0], /no dice QUÉ cambió/);
    assert.match(validarNombre("event", "Clientes.Cliente.v1.registrado").problemas[0][1], /Usa «clientes\.cliente\.v1\.registrado»/);
    assert.match(validarNombre("event", "pedidos.pedido.v0.creado").problemas[0][0], /versión/);
    assert.match(validarNombre("event", "pedidos.pedido.v10000.creado").problemas[0][0], /versión/);
  });

  test("un evento que parece un infinitivo, y una operación que parece un participio, avisan pero se crean", () => {
    assert.equal(validarNombre("event", "pedidos.pedido.v1.cancelado").avisos.length, 0);
    assert.equal(validarNombre("event", "logistica.envio.v1.entrega-fallida").avisos.length, 0);
    assert.equal(validarNombre("rpc", "clientes.cliente.v1.calcular-descuento").avisos.length, 0, "«descuento» no es un participio");
    assert.equal(validarNombre("rpc", "clientes.cliente.v1.obtener").avisos.length, 0);
    assert.equal(validarNombre("rpc", "pedidos.pedido.v1.cancelado").problemas.length, 0);
    assert.equal(validarNombre("rpc", "pedidos.pedido.v1.cancelado").avisos.length, 1);
  });
});

describe("flux new — las plantillas", () => {
  test("campoId: la referencia al agregado en camelCase", () => {
    assert.equal(campoId("cliente"), "clienteId");
    assert.equal(campoId("linea-envio"), "lineaEnvioId");
  });

  test("evento y estado llevan `aggregateVersion` obligatoria (entero ≥ 1); el `$id` es la ruta; additionalProperties false", () => {
    for (const [tipo, nombre] of [["event", "pedidos.linea-envio.v2.cancelada"], ["state", "pedidos.linea-envio.v2"]]) {
      const { partes } = validarNombre(tipo, nombre);
      const [p] = plantillas(tipo, partes);
      assert.equal(p.esquema.$id, `https://schemas.internal/${p.ruta}`);
      assert.equal(p.esquema.$schema, "https://json-schema.org/draft/2020-12/schema");
      assert.equal(p.esquema.type, "object");
      assert.equal(p.esquema.additionalProperties, false);
      assert.deepEqual(p.esquema.required, ["lineaEnvioId", "aggregateVersion"]);
      assert.deepEqual([p.esquema.properties.aggregateVersion.type, p.esquema.properties.aggregateVersion.minimum], ["integer", 1]);
      assert.match(p.ruta, tipo === "event" ? /^pedidos\/linea-envio\/cancelada\/2\.0\.0\.json$/ : /^state\/pedidos\/linea-envio\/2\.0\.0\.json$/, "el mayor del nombre es el de la versión");
    }
  });

  test("una operación son DOS esquemas, request y reply, del mismo mayor y cada uno con su `$id`", () => {
    const { partes } = validarNombre("rpc", "clientes.cliente.v3.obtener");
    const ps = plantillas("rpc", partes);
    assert.deepEqual(ps.map((p) => p.ruta), ["rpc/clientes/cliente/obtener/request/3.0.0.json", "rpc/clientes/cliente/obtener/reply/3.0.0.json"]);
    for (const p of ps) assert.equal(p.esquema.$id, `https://schemas.internal/${p.ruta}`);
    assert.deepEqual(ps[0].esquema.required, ["clienteId"]);
  });

  test("`--description` sustituye al TODO", () => {
    const { partes } = validarNombre("event", "pedidos.pedido.v1.creado");
    assert.match(plantillas("event", partes)[0].esquema.description, /^TODO/);
    assert.equal(plantillas("event", partes, "Un pedido se ha creado")[0].esquema.description, "Un pedido se ha creado");
  });
});

describe("flux new — los ficheros y el bundle (sin broker)", () => {
  test("crea el esquema de un evento, regenera el bundle y todo lo pasa `check-compat` y `bundle --check`", () => {
    const d = proyecto();
    try {
      const r = fluxNew(d, "event", "clientes.cliente.v1.registrado");
      assert.equal(r.code, 0, r.todo);
      assert.deepEqual(ficheros(join(d, "schemas")), ["bundle.json", "clientes/cliente/registrado/1.0.0.json"]);
      const esquema = leer(d, "schemas/clientes/cliente/registrado/1.0.0.json");
      assert.equal(esquema.$id, "https://schemas.internal/clientes/cliente/registrado/1.0.0.json");
      const bundle = leer(d, "schemas/bundle.json");
      assert.equal(bundle.subjects["clientes.cliente.v1.registrado"], esquema.$id, "el bundle indexa el subject");
      for (const script of ["check-compat.mjs", "bundle-schemas.mjs"]) {
        const v = spawnSync(process.execPath, [join(RAIZ, "scripts", script), "--schemas", join(d, "schemas"), ...(script.startsWith("bundle") ? ["--check"] : [])], { encoding: "utf8" });
        assert.equal(v.status, 0, `${script}: ${v.stdout}${v.stderr}`);
      }
    } finally { limpiar(d); }
  });

  test("un estado trae también la lápida; una operación, el error de rpc (el bundle los exige y nadie recuerda copiarlos)", () => {
    const d = proyecto();
    try {
      assert.equal(fluxNew(d, "state", "clientes.cliente.v1").code, 0);
      assert.ok(existsSync(join(d, "schemas/system/state/eliminado/1.0.0.json")));
      assert.equal(fluxNew(d, "rpc", "clientes.cliente.v1.obtener").code, 0);
      assert.ok(existsSync(join(d, "schemas/system/rpc/error/1.0.0.json")));
      const b = leer(d, "schemas/bundle.json");
      assert.deepEqual(Object.keys(b.state), ["clientes.cliente.v1"]);
      assert.deepEqual(Object.keys(b.rpc["clientes.cliente.v1.obtener"]).sort(), ["reply", "request"]);
    } finally { limpiar(d); }
  });

  test("un nombre inválido no deja NI UN fichero (ni siquiera el directorio schemas/), y sale con 1", () => {
    const d = proyecto();
    try {
      for (const [tipo, nombre] of [["event", "Clientes.cliente.v1.registrado"], ["event", "clientes.cliente.v1.actualizado"], ["state", "state.clientes.cliente.v1"], ["rpc", "rpc.clientes.cliente.v1.obtener"], ["rpc", "clientes.cliente.v0.obtener"], ["nada", "x"]]) {
        const r = fluxNew(d, tipo, nombre);
        assert.equal(r.code, 1, `${tipo} ${nombre}: ${r.todo}`);
        assert.match(r.err, /•/);
      }
      assert.deepEqual(arbol(d), []);
    } finally { limpiar(d); }
  });

  test("NUNCA pisa un esquema que existe (son inmutables): el fichero queda byte a byte igual, y el mensaje dice cómo evolucionarlo", () => {
    const d = proyecto();
    try {
      assert.equal(fluxNew(d, "event", "pedidos.pedido.v1.creado").code, 0);
      const f = join(d, "schemas/pedidos/pedido/creado/1.0.0.json");
      writeFileSync(f, readFileSync(f, "utf8").replace("TODO", "EDITADO A MANO"));
      const antes = readFileSync(f);
      const bundleAntes = readFileSync(join(d, "schemas/bundle.json"));
      const r = fluxNew(d, "event", "pedidos.pedido.v1.creado");
      assert.equal(r.code, 1);
      assert.match(r.err, /INMUTABLES/);
      assert.match(r.err, /1\.1\.0\.json/);
      assert.deepEqual(readFileSync(f), antes);
      assert.deepEqual(readFileSync(join(d, "schemas/bundle.json")), bundleAntes);
      // …pero una versión MAYOR nueva sí: v2 es otro contrato
      assert.equal(fluxNew(d, "event", "pedidos.pedido.v2.creado").code, 0);
      assert.ok(existsSync(join(d, "schemas/pedidos/pedido/creado/2.0.0.json")));
    } finally { limpiar(d); }
  });

  test("una operación a medias (solo su petición) no se completa a escondidas: el árbol ya incumple y `flux new` se niega ANTES de escribir", () => {
    const d = proyecto();
    try {
      assert.equal(fluxNew(d, "rpc", "clientes.cliente.v1.obtener").code, 0);
      rmSync(join(d, "schemas/rpc/clientes/cliente/obtener/reply"), { recursive: true });
      const antes = arbol(join(d, "schemas"));
      const r = fluxNew(d, "event", "pedidos.pedido.v1.creado");
      assert.equal(r.code, 1);
      assert.match(r.err, /ya incumple las reglas/);
      assert.match(r.err, /petición pero no de respuesta/);
      assert.deepEqual(arbol(join(d, "schemas")), antes, "no se añadió nada");
    } finally { limpiar(d); }
  });

  test("si el contrato nuevo rompe `check-compat`, se DESHACE todo (ficheros y directorios) y el bundle no cambia", () => {
    const d = proyecto();
    try {
      // Un 1.1.0 huérfano al que le falta `aggregateVersion`: el 1.0.0 que crearía `flux new` →1.1.0 «elimina» ese campo.
      const huerfano = join(d, "schemas/pedidos/pedido/creado/1.1.0.json");
      mkdirSync(dirname(huerfano), { recursive: true });
      writeFileSync(huerfano, JSON.stringify({
        $schema: "https://json-schema.org/draft/2020-12/schema", $id: "https://schemas.internal/pedidos/pedido/creado/1.1.0.json", description: "x",
        type: "object", additionalProperties: false, required: ["pedidoId"], properties: { pedidoId: { type: "string" } },
      }));
      const antes = arbol(join(d, "schemas"));
      const r = fluxNew(d, "event", "pedidos.pedido.v1.creado");
      assert.equal(r.code, 1, r.todo);
      assert.match(r.err, /Se deshizo lo escrito/);
      assert.match(r.err, /campo eliminado/);
      assert.deepEqual(arbol(join(d, "schemas")), antes, "ni el 1.0.0 ni directorios sueltos");
    } finally { limpiar(d); }
  });

  test("si el bundle no se puede regenerar, se deshace todo, directorios nuevos incluidos", () => {
    const d = proyecto();
    try {
      mkdirSync(join(d, "schemas/bundle.json"), { recursive: true });   // un DIRECTORIO donde va el fichero: `bundle-schemas` no puede escribirlo
      const antes = arbol(join(d, "schemas"));
      const r = fluxNew(d, "event", "clientes.cliente.v1.registrado");
      assert.equal(r.code, 1, r.todo);
      assert.match(r.err, /no se pudo regenerar el bundle/);
      assert.deepEqual(arbol(join(d, "schemas")), antes, "no queda ni el esquema ni los directorios clientes/cliente/registrado/");
    } finally { limpiar(d); }
  });

  test("`--dry-run` no escribe nada; `--no-bundle` no toca el bundle; `--schemas` elige dónde", () => {
    const d = proyecto();
    try {
      const seco = fluxNew(d, "event", "pedidos.pedido.v1.creado", "--dry-run");
      assert.equal(seco.code, 0, seco.todo);
      assert.match(seco.out, /simulación/);
      assert.match(seco.out, /pedidos\/pedido\/creado\/1\.0\.0\.json/);
      assert.deepEqual(arbol(d), []);

      const sin = fluxNew(d, "event", "pedidos.pedido.v1.creado", "--no-bundle", "--schemas", "contratos");
      assert.equal(sin.code, 0, sin.todo);
      assert.deepEqual(ficheros(join(d, "contratos")), ["pedidos/pedido/creado/1.0.0.json"], "sin bundle.json");

      assert.equal(fluxNew(d, "event", "pedidos.pedido.v1.creado", "--schemas").code, 1);   // falta el valor
    } finally { limpiar(d); }
  });

  test("lo que crea valida lo bueno y rechaza lo malo (ajv, draft 2020-12): sin `aggregateVersion`, con 0 o con un campo de más NO pasa", async () => {
    const d = proyecto();
    let Ajv;
    try { ({ default: Ajv } = await import("ajv/dist/2020.js")); } catch { return; }   // ajv es dependencia opcional de la CLI
    try {
      for (const [t, n] of [["event", "pedidos.pedido.v1.creado"], ["state", "pedidos.pedido.v1"], ["rpc", "pedidos.pedido.v1.reservar"]]) assert.equal(fluxNew(d, t, n).code, 0);
      const compila = (ruta) => new Ajv({ strict: true, allErrors: true }).compile(leer(d, "schemas", ruta));
      for (const ruta of ["pedidos/pedido/creado/1.0.0.json", "state/pedidos/pedido/1.0.0.json"]) {
        const v = compila(ruta);
        assert.equal(v({ pedidoId: "ped-1", aggregateVersion: 1 }), true, ruta);
        assert.equal(v({ pedidoId: "ped-1" }), false, `${ruta}: sin aggregateVersion`);
        assert.equal(v({ pedidoId: "ped-1", aggregateVersion: 0 }), false, `${ruta}: versión 0`);
        assert.equal(v({ pedidoId: "ped-1", aggregateVersion: 1.5 }), false, `${ruta}: decimal`);
        assert.equal(v({ pedidoId: "ped-1", aggregateVersion: 1, email: "a@b.c" }), false, `${ruta}: un campo de más (un PII colado)`);
        assert.equal(v({ pedidoId: "ped.1", aggregateVersion: 1 }), false, `${ruta}: una clave con punto`);
      }
      assert.equal(compila("rpc/pedidos/pedido/reservar/request/1.0.0.json")({ pedidoId: "ped-1" }), true);
      assert.equal(compila("rpc/pedidos/pedido/reservar/request/1.0.0.json")({}), false);
      assert.equal(compila("rpc/pedidos/pedido/reservar/reply/1.0.0.json")({}), true);
    } finally { limpiar(d); }
  });
});

// ─── Con broker ──────────────────────────────────────────────────────────────────────────────────

const hay = detectarBroker();
if (!hay.ok) {
  if (REQUIRE) test("hay un nats-server (FLUX_REQUIRE_BROKER=1)", () => assert.fail(hay.motivo));
  else console.error(`# SALTADO: ${hay.motivo}  (FLUX_REQUIRE_BROKER=1 lo convierte en fallo)`);
}

(hay.ok ? describe : describe.skip)("flux new — con un nats-server real: lo que crea lo aceptan gen-acl, `flux provision` y `flux doctor`", () => {
  let broker, conn, d;
  before(async () => {
    broker = await arrancaBroker();
    conn = await conecta(broker.url);
    d = proyecto();
  });
  after(async () => {
    await conn?.nc.close();
    await broker?.parar();
    limpiar(d);
  });

  test("tres contratos nuevos → manifiesto → ACLs generadas → aprovisionado → doctor, sin tocar un fichero a mano", async () => {
    for (const [t, n] of [["event", "clientes.cliente.v1.registrado"], ["state", "clientes.cliente.v1"], ["rpc", "clientes.cliente.v1.calcular-descuento"]]) {
      const r = fluxNew(d, t, n);
      assert.equal(r.code, 0, r.todo);
    }
    const manifiesto = join(d, "services.json");
    writeFileSync(manifiesto, JSON.stringify({
      environment: "produccion",
      services: [
        { name: "crm", owns: ["clientes"], serves: ["clientes.cliente.v1.calcular-descuento"] },
        { name: "tienda", watches: ["clientes.cliente.v1"], calls: ["clientes.cliente.v1.calcular-descuento"], consumes: ["clientes.cliente.v1.registrado"] },
      ],
    }));
    const bundle = join(d, "schemas/bundle.json");

    const acl = spawnSync(process.execPath, [GEN, manifiesto, "--bundle", bundle, "--check"], { encoding: "utf8" });
    assert.equal(acl.status, 0, `gen-acl: ${acl.stdout}${acl.stderr}`);
    assert.match(acl.stdout, /1 dominio\(s\) con estado/);
    assert.match(acl.stdout, /1 operación\(es\) atendida\(s\)/);

    const p = await flux(broker.url, ["provision", manifiesto, "--bundle", bundle]);
    assert.equal(p.code, 0, `${p.out}${p.err}`);
    const streams = (await conn.jsm.streams.list().next()).map((s) => s.config.name).sort();
    assert.deepEqual(streams, ["DLQ_CLIENTES", "EVT_CLIENTES", "STATE_CLIENTES"], "eventos y estado tienen su stream; la operación NO (core NATS)");
    for (const s of await conn.jsm.streams.list().next()) {
      assert.ok(!s.config.subjects.some((x) => x.startsWith("rpc") || x === ">"), `ningún stream captura rpc.>: ${s.config.name}`);
    }
    const doctor = await flux(broker.url, ["doctor"]);
    assert.equal(doctor.code, 0, `${doctor.out}${doctor.err}`);
  });

  test("y lo que `flux new` NO acepta, tampoco lo acepta el aprovisionado: una operación sin su respuesta no llega al broker", async () => {
    const d2 = proyecto();
    try {
      assert.equal(fluxNew(d2, "rpc", "pedidos.pedido.v1.reservar").code, 0);
      rmSync(join(d2, "schemas/rpc/pedidos/pedido/reservar/reply"), { recursive: true });
      const r = spawnSync(process.execPath, [join(RAIZ, "scripts", "bundle-schemas.mjs"), "--schemas", join(d2, "schemas")], { encoding: "utf8" });
      assert.equal(r.status, 1);
      assert.match(r.stderr, /petición pero no de respuesta/);
    } finally { limpiar(d2); }
  });
});
