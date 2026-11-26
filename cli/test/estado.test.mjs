/**
 * `flux state`, `flux doctor`, `flux tail` y `flux dlq` sobre estado, CONTRA UN nats-server REAL.
 *
 * La CLI se ejecuta como proceso (lo que usa un operador) y el estado se escribe A MANO, con el
 * algoritmo de 10-state.md §3.2 sobre NATS crudo: ni la CLI ni estas pruebas dependen del SDK de
 * flux. Cada comprobación va en los dos sentidos: la simulación no cambia nada y `--confirm` sí;
 * el doctor sale 0 con la configuración buena y 1 con cada una de las rotas.
 *
 *   NATS_SERVER=/ruta/a/nats-server npm test
 *   FLUX_REQUIRE_BROKER=1 npm test      → sin nats-server, ESTAS pruebas FALLAN (no se saltan)
 */

import { test, describe, before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";

import {
  arrancaBroker, conecta, creaDlq, creaDurable, creaEvt, creaStreamEstado, detectarBroker, drena, escribeEstado,
  flux, fluxVivo, publicaCrudo, publicaDlq, REQUIRE, sembrar, sobreEstado, vaciaBroker,
} from "./helpers.mjs";

const hay = detectarBroker();
if (!hay.ok) {
  if (REQUIRE) {
    // FLUX_REQUIRE_BROKER=1: la AUSENCIA del broker es un fallo. Sin esto, un binario que no se
    // instalara dejaría el job verde sin haber ejecutado nunca la CLI contra un broker.
    test("hay un nats-server (FLUX_REQUIRE_BROKER=1)", () => assert.fail(hay.motivo));
  } else {
    console.error(`# SALTADO: ${hay.motivo}  (FLUX_REQUIRE_BROKER=1 lo convierte en fallo)`);
  }
}
const suite = hay.ok ? describe : describe.skip;

const C = "clientes.cliente.v1";
const sinColor = (s) => s.replace(/\x1b\[[0-9;]*m/g, "");

/** Un broker propio para una suite, con su conexión de pruebas. */
function conBroker(nombre, cuerpo) {
  suite(nombre, () => {
    const ctx = {};
    before(async () => {
      ctx.broker = await arrancaBroker();
      ctx.url = ctx.broker.url;
      ctx.conn = await conecta(ctx.url);
    });
    after(async () => {
      await ctx.conn?.nc.close();
      await ctx.broker?.parar();
    });
    cuerpo(ctx);
  });
}

// ─── ls / get ────────────────────────────────────────────────────────────────────────────────────

conBroker("flux state ls / get", (ctx) => {
  before(async () => {
    await creaStreamEstado(ctx.conn.jsm, "clientes");
  });

  test("100 claves escritas a mano → `ls` las lista todas, con revisión y tamaño; `get` lee una", async () => {
    await sembrar(ctx.conn, { n: 100 });
    await escribeEstado(ctx.conn, { contrato: C, tenant: "globex", clave: "cli-1", version: 3 });
    await escribeEstado(ctx.conn, { contrato: C, tenant: "globex", clave: "cli-2", version: 1 });

    // `ls` de todo el contrato: 102 claves, 2 tenants
    const todo = await flux(ctx.url, ["state", "ls", C, "--json"]);
    assert.equal(todo.code, 0, todo.err);
    const filas = JSON.parse(todo.out);
    assert.equal(filas.length, 102);
    assert.deepEqual([...new Set(filas.map((f) => f.tenant))].sort(), ["acme", "globex"]);
    assert.ok(filas.every((f) => Number.isInteger(f.revision) && f.revision >= 1 && f.size > 400), "revisión y tamaño de cada clave");
    assert.deepEqual(filas.map((f) => f.revision).sort((a, b) => a - b), Array.from({ length: 102 }, (_, i) => i + 1), "cada revisión es la secuencia de su mensaje");
    const g1 = filas.find((f) => f.tenant === "globex" && f.key === "cli-1");
    assert.equal(g1.aggregateVersion, 3);
    assert.equal(g1.revision, 101);
    // orden natural: cli-2 antes que cli-10
    const acme = filas.filter((f) => f.tenant === "acme").map((f) => f.key);
    assert.deepEqual(acme.slice(0, 3), ["cli-1", "cli-2", "cli-3"]);
    assert.equal(acme.at(-1), "cli-100");

    // `--tenant` acota
    const g = JSON.parse((await flux(ctx.url, ["state", "ls", C, "--tenant", "globex", "--json"])).out);
    assert.deepEqual(g.map((f) => f.key), ["cli-1", "cli-2"]);

    // salida humana
    const h = await flux(ctx.url, ["state", "ls", C, "--tenant", "acme"]);
    assert.equal(h.code, 0);
    assert.match(h.out, /^TENANT\s+CLAVE\s+REVISIÓN\s+VERSIÓN\s+TAMAÑO\s+ESCRITO\s+ESTADO/);
    assert.match(h.out, /100 clave\(s\) de clientes\.cliente\.v1 en STATE_CLIENTES · 1 tenant\(s\) · 0 lápida\(s\)/);
    assert.doesNotMatch(h.out, /LÁPIDA/, "antes de borrar nada no hay lápidas");

    // `--limit`
    const l = JSON.parse((await flux(ctx.url, ["state", "ls", C, "--limit", "5", "--json"])).out);
    assert.equal(l.length, 5);

    // `get` de una: el sobre, con el data OCULTO (confidential es la clasificación por defecto)
    const r = await flux(ctx.url, ["state", "get", C, "acme", "cli-7"]);
    assert.equal(r.code, 0, r.err);
    assert.match(r.out, /^state\.clientes\.cliente\.v1\.acme\.cli-7\n/);
    assert.match(r.out, /revisión\s+7\n/);
    assert.match(r.out, /versión\s+1\n/);
    assert.match(r.out, /"type": "com\.flux\.state\.clientes\.cliente\.v1"/);
    assert.match(r.out, /"data": "\[oculto: dataclassification=confidential\]"/);
    assert.doesNotMatch(r.out, /example\.com/, "el email NO se imprime sin --show-data");
    assert.doesNotMatch(r.out, /LÁPIDA/);
  });

  test("`get`: el data sale con --show-data, y el --json respeta lo mismo", async () => {
    const oculto = await flux(ctx.url, ["state", "get", C, "acme", "cli-8", "--json"]);
    const o = JSON.parse(oculto.out);
    assert.equal(o.envelope.data, "[oculto: dataclassification=confidential]");
    assert.equal(o.revision, 8);
    assert.equal(o.deleted, false);
    assert.ok(!oculto.out.includes("example.com"));
    const visible = JSON.parse((await flux(ctx.url, ["state", "get", C, "acme", "cli-8", "--show-data", "--json"])).out);
    assert.equal(visible.envelope.data.email, "cli-8@example.com");
    // el sobre conserva `data` como último atributo también con la marca
    assert.equal(Object.keys(o.envelope).at(-1), "data");
  });

  test("`get` de una clave `public` imprime el data sin pedir nada", async () => {
    await escribeEstado(ctx.conn, { contrato: C, tenant: "acme", clave: "publica-1", version: 1, clasificacion: "public" });
    const r = await flux(ctx.url, ["state", "get", C, "acme", "publica-1"]);
    assert.match(r.out, /"email": "publica-1@example\.com"/);
  });

  test("`get` de una clave que no existe: 404 claro y salida 1", async () => {
    const r = await flux(ctx.url, ["state", "get", C, "acme", "cli-999"]);
    assert.equal(r.code, 1);
    assert.match(r.err, /404: no hay valor en state\.clientes\.cliente\.v1\.acme\.cli-999/);
    assert.match(r.err, /El tenant es parte de la clave/);
    // el mismo `cli-1` de otro tenant SÍ existe: el tenant es parte de la clave
    assert.equal((await flux(ctx.url, ["state", "get", C, "globex", "cli-1"])).code, 0);
    assert.equal((await flux(ctx.url, ["state", "get", C, "globex", "cli-3"])).code, 1);
  });

  test("borrar una clave (lápida): `ls` la marca y `get` lo dice; el resto sigue igual", async () => {
    const antes = await flux(ctx.url, ["state", "get", C, "acme", "cli-9"]);
    assert.doesNotMatch(antes.out, /LÁPIDA/);

    const r = await escribeEstado(ctx.conn, { tipo: "delete", contrato: C, tenant: "acme", clave: "cli-9", version: 2 });
    assert.equal(r.applied, true);

    const ls = JSON.parse((await flux(ctx.url, ["state", "ls", C, "--tenant", "acme", "--json"])).out);
    const lapidas = ls.filter((f) => f.deleted);
    assert.deepEqual(lapidas.map((f) => f.key), ["cli-9"]);
    assert.equal(lapidas[0].aggregateVersion, 2);
    assert.equal(lapidas[0].revision, r.revision);
    assert.ok(lapidas[0].size < 520, "una lápida pesa unos cientos de bytes y no lleva el dato");

    const h = await flux(ctx.url, ["state", "ls", C, "--tenant", "acme"]);
    assert.match(h.out, /^acme\s+cli-9\s+\d+\s+2\s+\d+ B\s+\S+ \S+\s+LÁPIDA$/m);
    assert.match(h.out, /1 lápida\(s\)/);

    const g = await flux(ctx.url, ["state", "get", C, "acme", "cli-9"]);
    assert.equal(g.code, 0, "una lápida existe: se muestra, no es un 404");
    assert.match(g.out, /LÁPIDA: la clave está borrada desde la revisión \d+ \(versión 2\)/);
    assert.match(g.out, /`state\.get` del SDK devuelve null/);
    assert.match(g.out, /"type": "com\.flux\.state\.clientes\.cliente\.deleted\.v1"/);
    assert.match(g.out, /"aggregateVersion": 2/, "la lápida es `internal`: su data se ve");
    assert.equal(JSON.parse((await flux(ctx.url, ["state", "get", C, "acme", "cli-9", "--json"])).out).deleted, true);
  });

  test("entradas ANÓMALAS: se muestran marcadas en vez de esconderse", async () => {
    // una clave `a.b` son dos tokens: el broker la acepta y NINGÚN observador la recibe (A-41)
    await publicaCrudo(ctx.conn, "state.clientes.cliente.v1.acme.a.b", sobreEstado({ contrato: C, tenant: "acme", clave: "a", version: 1 }));
    // el sobre dice otro tenant que el subject: STATE_SUBJECT_MISMATCH
    await escribeEstado(ctx.conn, { contrato: C, tenant: "acme", clave: "miente", version: 1, cambios: { tenantid: "globex" } });
    // un cuerpo que no es JSON
    await publicaCrudo(ctx.conn, "state.clientes.cliente.v1.acme.rota", '{"este cuerpo lleva el dato: ana@example.com');

    const ls = JSON.parse((await flux(ctx.url, ["state", "ls", C, "--tenant", "acme", "--json"])).out);
    const por = (k) => ls.find((f) => f.key === k);
    assert.match(por("a.b").problems[0], /subject no canónico/);
    // …también sin --tenant (el filtro es `state.<contrato>.>`, no `….*.*`, que la esconde)
    const todas = JSON.parse((await flux(ctx.url, ["state", "ls", C, "--json"])).out);
    assert.ok(todas.some((f) => f.subject === "state.clientes.cliente.v1.acme.a.b"), "la entrada fantasma aparece en el ls completo");
    assert.deepEqual(por("miente").problems, ["STATE_SUBJECT_MISMATCH"]);
    assert.match(por("rota").problems[0], /no es JSON/);

    const h = await flux(ctx.url, ["state", "ls", C, "--tenant", "acme"]);
    assert.match(h.out, /3 entrada\(s\) anómala\(s\)/);
    assert.match(h.out, /tenantid del sobre «globex» ≠ «acme» del subject/);

    // `get` enseña el mismatch, y NO vuelca el cuerpo ilegible (podría llevar el dato)
    const m = await flux(ctx.url, ["state", "get", C, "acme", "miente"]);
    assert.match(m.out, /STATE_SUBJECT_MISMATCH: tenantid del sobre «globex»/);
    const rota = await flux(ctx.url, ["state", "get", C, "acme", "rota"]);
    assert.match(rota.out, /el cuerpo no es JSON/);
    assert.doesNotMatch(rota.out, /ana@example\.com/);
    assert.match((await flux(ctx.url, ["state", "get", C, "acme", "rota", "--show-data"])).out, /ana@example\.com/);
  });

  test("validación antes de tocar la red: clave, tenant, dominio reservado y contrato", async () => {
    const casos = [
      [["state", "get", C, "acme", "a.b"], /INVALID_STATE_KEY/],
      [["state", "get", C, "acme", "_reservada"], /INVALID_STATE_KEY/],
      [["state", "get", C, "Acme", "cli-1"], /INVALID_TENANT_FOR_STATE/],
      [["state", "get", C, "acme_corp", "cli-1"], /INVALID_TENANT_FOR_STATE/],
      [["state", "get", "state.cliente.v1", "acme", "cli-1"], /RESERVED_DOMAIN/],
      [["state", "get", "dlq.cliente.v1", "acme", "cli-1"], /RESERVED_DOMAIN/],
      [["state", "ls", "rpc.cliente.v1"], /RESERVED_DOMAIN/],
      [["state", "get", "clientes.cliente", "acme", "cli-1"], /contrato inválido/],
      [["state", "get", "state.clientes.cliente.v1", "acme", "cli-1"], /prefijo `state\.`/],
      [["state", "ls", C, "--tenant", "ACME"], /INVALID_TENANT_FOR_STATE/],
      [["state", "ls"], /uso: flux state ls/],
      [["state", "ls", C, "--limit", "abc"], /--limit debe ser un entero mayor que 0/],
      [["state", "ls", C, "--limit", "0"], /--limit debe ser un entero mayor que 0/],
      [["state", "get", C, "acme"], /uso: flux state get/],
      [["state", "resync", "tienda-web"], /uso: flux state resync/],
      [["state", "resync", "Tienda_Web", C], /nombre de servicio inválido/],
      [["state", "nada"], /acción desconocida: nada/],
    ];
    for (const [args, patron] of casos) {
      const r = await flux(ctx.url, args);
      assert.equal(r.code, 1, args.join(" "));
      assert.match(r.err, patron, args.join(" "));
    }
  });

  test("un dominio sin STATE_<DOM>: `ls` y `get` dicen que no está aprovisionado", async () => {
    const ls = await flux(ctx.url, ["state", "ls", "pedidos.pedido.v1"]);
    assert.equal(ls.code, 1);
    assert.match(ls.err, /no existe el stream STATE_PEDIDOS: el dominio no tiene estado aprovisionado/);
    const g = await flux(ctx.url, ["state", "get", "pedidos.pedido.v1", "acme", "ped-1"]);
    assert.equal(g.code, 1);
    assert.match(g.err, /el stream STATE_PEDIDOS no existe/);
  });

  test("un stream SIN allow_direct: «sin respuesta» se explica (A-32), no se confunde con un 404", async () => {
    await creaStreamEstado(ctx.conn.jsm, "sindirecto", { allow_direct: false });
    await ctx.conn.js.publish("state.sindirecto.cosa.v1.acme.k-1", new TextEncoder().encode("{}"));
    const g = await flux(ctx.url, ["state", "get", "sindirecto.cosa.v1", "acme", "k-1"]);
    assert.equal(g.code, 1);
    assert.match(g.err, /NO tiene allow_direct/);
    const ls = await flux(ctx.url, ["state", "ls", "sindirecto.cosa.v1"]);
    assert.equal(ls.code, 1);
    assert.match(ls.err, /NO tiene allow_direct/);
  });

  test("un listado GRANDE por una tubería no se trunca (`process.exit` cortaba la salida en el búfer de la tubería)", async () => {
    await creaStreamEstado(ctx.conn.jsm, "grande");
    const cuerpo = new TextEncoder().encode("{}");
    for (let i = 0; i < 3000; i += 500) {
      await Promise.all(Array.from({ length: 500 }, (_, k) => ctx.conn.js.publish(`state.grande.cosa.v1.acme.k-${i + k}`, cuerpo)));
    }
    const r = await flux(ctx.url, ["state", "ls", "grande.cosa.v1", "--json"]);
    assert.equal(r.code, 0, r.err);
    assert.ok(r.out.length > 3 * 65536, `el JSON mide ${r.out.length} bytes: más que el búfer de una tubería`);
    assert.equal(JSON.parse(r.out).length, 3000, "el JSON llega entero y se puede interpretar");
  });

  test("un contrato sin claves: se dice, con salida 0", async () => {
    const r = await flux(ctx.url, ["state", "ls", "clientes.otro.v1"]);
    assert.equal(r.code, 0);
    assert.match(r.out, /no hay ninguna clave en STATE_CLIENTES/);
  });
});

// ─── resync ──────────────────────────────────────────────────────────────────────────────────────

conBroker("flux state resync", (ctx) => {
  const D_ACME = "tienda-web__state_clientes_cliente_v1__acme";
  const D_GLOBEX = "tienda-web__state_clientes_cliente_v1__globex";
  const D_OTRO = "otro-svc__state_clientes_cliente_v1__acme";
  const F_ACME = "state.clientes.cliente.v1.acme.*";

  /** `{ pendientes, entregados, acks, config }` de un durable. */
  const estado = async (d) => {
    const i = await ctx.conn.jsm.consumers.info("STATE_CLIENTES", d);
    return { pendientes: i.num_pending, entregado: i.delivered.stream_seq, ackFloor: i.ack_floor.stream_seq, config: i.config };
  };
  const existe = (d) => ctx.conn.jsm.consumers.info("STATE_CLIENTES", d).then(() => true, () => false);

  beforeEach(async () => {
    await vaciaBroker(ctx.conn.jsm);
    await creaStreamEstado(ctx.conn.jsm, "clientes");
    await sembrar(ctx.conn, { n: 100, tenant: "acme" });
    await sembrar(ctx.conn, { n: 5, tenant: "globex" });
    await escribeEstado(ctx.conn, { tipo: "delete", contrato: C, tenant: "acme", clave: "cli-3", version: 2 });
    for (const [d, f, n] of [[D_ACME, F_ACME, 100], [D_GLOBEX, "state.clientes.cliente.v1.globex.*", 5], [D_OTRO, F_ACME, 100]]) {
      await creaDurable(ctx.conn.jsm, "STATE_CLIENTES", d, f);
      assert.equal((await drena(ctx.conn, "STATE_CLIENTES", d, n, 4000)).length, n);
    }
  });

  test("SIMULACIÓN por defecto: dice qué hará y NO cambia nada", async () => {
    const antes = await estado(D_ACME);
    assert.equal(antes.pendientes, 0);

    const r = await flux(ctx.url, ["state", "resync", "tienda-web", C, "--tenant", "acme"]);
    assert.equal(r.code, 0, r.err);
    assert.match(r.out, /SIMULACIÓN/);
    assert.match(r.out, /durable\s+tienda-web__state_clientes_cliente_v1__acme/);
    assert.match(r.out, /filtro\s+state\.clientes\.cliente\.v1\.acme\.\*/);
    assert.match(r.out, /claves\s+100 en el filtro/, "99 valores + la lápida de cli-3");
    assert.match(r.out, /BORRAR el durable/);
    assert.match(r.out, /RECREARLO con deliver_policy=last_per_subject/);
    assert.match(r.out, /No toca: el stream, ni una clave/);
    assert.match(r.out, /DEBE ser\s+idempotente/);
    assert.match(r.out, /No se ha cambiado nada\. Para ejecutarlo de verdad:\n\s+flux state resync tienda-web clientes\.cliente\.v1 --tenant acme --confirm/);

    // el durable es EL MISMO: mismo punto de entrega, nada pendiente, nada que redistribuir
    const despues = await estado(D_ACME);
    assert.deepEqual({ ...despues, config: undefined }, { ...antes, config: undefined });
    assert.deepEqual(await drena(ctx.conn, "STATE_CLIENTES", D_ACME, 0, 1000), []);
  });

  test("--confirm --yes: borra y recrea el durable, y el broker entrega otra vez el último valor de cada clave", async () => {
    const stream = await ctx.conn.jsm.streams.info("STATE_CLIENTES");
    const globexAntes = await estado(D_GLOBEX);
    const otroAntes = await estado(D_OTRO);

    const r = await flux(ctx.url, ["state", "resync", "tienda-web", C, "--tenant", "acme", "--confirm", "--yes"]);
    assert.equal(r.code, 0, r.err);
    assert.doesNotMatch(r.out, /SIMULACIÓN/);
    assert.match(r.out, /Hará, por este orden/);
    assert.match(r.out, /durable borrado/);
    assert.match(r.out, /durable recreado con la configuración canónica · pendientes: 100/);

    // el durable recreado trae la config EXACTA de 10-state.md §5.2
    const d = await estado(D_ACME);
    assert.equal(d.pendientes, 100);
    assert.equal(d.config.deliver_policy, "last_per_subject");
    assert.equal(d.config.filter_subject, F_ACME);
    assert.equal(d.config.ack_policy, "explicit");
    assert.equal(d.config.ack_wait, 30e9);
    assert.equal(d.config.max_deliver, 6);
    assert.deepEqual(d.config.backoff, [30e9, 60e9, 300e9, 900e9, 1800e9]);
    assert.equal(d.config.max_ack_pending, 256);
    assert.equal(d.config.replay_policy, "instant");

    // vuelve a entregar TODO: 99 valores y la lápida, una vez cada uno
    const claves = await drena(ctx.conn, "STATE_CLIENTES", D_ACME, 100, 4000);
    assert.equal(claves.length, 100);
    assert.equal(new Set(claves).size, 100);
    assert.ok(claves.includes("cli-3"), "la lápida también se entrega: el observador debe borrar su copia");

    // …y NO toca nada más: ni el stream (mismo nº de mensajes y misma última secuencia), ni los
    // durables de otro tenant o de otro servicio
    const despues = await ctx.conn.jsm.streams.info("STATE_CLIENTES");
    assert.equal(despues.state.messages, stream.state.messages);
    assert.equal(despues.state.last_seq, stream.state.last_seq);
    assert.deepEqual(await estado(D_GLOBEX), globexAntes);
    assert.deepEqual(await estado(D_OTRO), otroAntes);
  });

  test("un durable con la config alterada: la simulación lo enseña y --confirm lo deja canónico", async () => {
    await ctx.conn.jsm.consumers.delete("STATE_CLIENTES", D_ACME);
    await creaDurable(ctx.conn.jsm, "STATE_CLIENTES", D_ACME, F_ACME, { deliver_policy: "new", max_ack_pending: 10 });

    const sim = await flux(ctx.url, ["state", "resync", "tienda-web", C, "--tenant", "acme"]);
    assert.match(sim.out, /NO tiene la configuración canónica/);
    assert.match(sim.out, /deliver_policy: existente new, canónico last_per_subject/);
    assert.match(sim.out, /max_ack_pending: existente 10, canónico 256/);
    assert.equal((await estado(D_ACME)).config.deliver_policy, "new", "la simulación no lo toca");

    const r = await flux(ctx.url, ["state", "resync", "tienda-web", C, "--tenant", "acme", "--confirm", "--yes"]);
    assert.equal(r.code, 0, r.err);
    const d = await estado(D_ACME);
    assert.equal(d.config.deliver_policy, "last_per_subject");
    assert.equal(d.config.max_ack_pending, 256);
    assert.equal(d.pendientes, 100, "con `new` no habría recibido nada: ahora recibe la foto");
  });

  test("un durable que NO existe: no lo inventa (un nombre mal escrito dejaría un fantasma) y enseña los que hay", async () => {
    const r = await flux(ctx.url, ["state", "resync", "tienda-web", C, "--tenant", "inventado", "--confirm", "--yes"]);
    assert.equal(r.code, 1);
    assert.match(r.err, /no existe el durable tienda-web__state_clientes_cliente_v1__inventado en STATE_CLIENTES/);
    assert.match(r.err, /no inventa uno/);
    assert.match(r.err, /tienda-web__state_clientes_cliente_v1__acme/);
    assert.match(r.err, /tienda-web__state_clientes_cliente_v1__globex/);
    assert.doesNotMatch(r.err, /otro-svc__/, "solo los durables del servicio pedido");
    assert.equal(await existe("tienda-web__state_clientes_cliente_v1__inventado"), false, "NO se creó");

    const sinTenant = await flux(ctx.url, ["state", "resync", "tienda-web", C]);
    assert.equal(sinTenant.code, 1);
    assert.match(sinTenant.err, /¿Falta \(o sobra\) --tenant\?/);

    const otro = await flux(ctx.url, ["state", "resync", "no-existe", C, "--tenant", "acme"]);
    assert.equal(otro.code, 1);
    assert.match(otro.err, /«no-existe» no tiene ningún durable de estado/);
  });

  test("--confirm sin --yes y sin terminal: no se puede preguntar, y NO se hace nada", async () => {
    const antes = await estado(D_ACME);
    const r = await flux(ctx.url, ["state", "resync", "tienda-web", C, "--tenant", "acme", "--confirm"]);
    assert.equal(r.code, 1);
    assert.match(r.err, /sin terminal interactiva no se puede pedir confirmación: añade --yes/);
    assert.deepEqual(await estado(D_ACME), antes);
  });

  test("un durable sin tenant (filtro `*.*`): foto de todos los tenants", async () => {
    const d = "tienda-web__state_clientes_cliente_v1";
    await creaDurable(ctx.conn.jsm, "STATE_CLIENTES", d, "state.clientes.cliente.v1.*.*");
    await drena(ctx.conn, "STATE_CLIENTES", d, 105, 4000);
    const r = await flux(ctx.url, ["state", "resync", "tienda-web", C, "--confirm", "--yes"]);
    assert.equal(r.code, 0, r.err);
    assert.equal((await estado(d)).pendientes, 105, "100 de acme (con la lápida) + 5 de globex");
  });

  test("un stream que no existe", async () => {
    const r = await flux(ctx.url, ["state", "resync", "tienda-web", "pedidos.pedido.v1", "--tenant", "acme"]);
    assert.equal(r.code, 1);
    assert.match(r.err, /no existe el stream STATE_PEDIDOS/);
  });
});

// ─── dlq replay / inspect sobre estado ───────────────────────────────────────────────────────────

conBroker("flux dlq replay rechaza el estado", (ctx) => {
  /** El sobre de DLQ de estado de 10-state.md §2.6: el original con las dlq* y `data = { key, revision }`. */
  const dlqEstado = (extra = {}) => {
    const { data, ...sobre } = sobreEstado({ contrato: C, tenant: "acme", clave: "cli-1", version: 7 });
    return {
      ...sobre, dlqreason: "permanent", dlqattempts: 1, dlqconsumer: "tienda-web__state_clientes_cliente_v1__acme",
      dlqerror: "PAIS_NO_SOPORTADO: el cliente cli-1 tiene un país fuera del catálogo", dlqtime: "2026-08-20T10:25:40.117Z",
      data: { key: "cli-1", revision: 41, ...extra },
    };
  };
  const dlqEvento = {
    specversion: "1.0", id: "01924f8e-0000-7b2d-9e14-000000000001", source: "/produccion/crm-api",
    type: "com.flux.clientes.cliente.creado.v1", time: "2026-08-20T10:25:39.412Z", datacontenttype: "application/json",
    subject: "cli-1", correlationid: "c1", tenantid: "acme", producerversion: "2.1.0", dataclassification: "internal",
    dlqreason: "permanent", dlqattempts: 1, dlqconsumer: "facturacion-api__clientes_cliente_v1_creado",
    dlqerror: "YA_EXISTE: x", dlqtime: "2026-08-20T10:25:40.117Z", data: { clienteId: "cli-1", aggregateVersion: 1 },
  };
  const mensajes = async (s) => (await ctx.conn.jsm.streams.info(s)).state.messages;
  const raros = async () => {
    const i = await ctx.conn.jsm.streams.info("STATE_CLIENTES", { subjects_filter: "state.>" });
    return Object.keys(i.state.subjects ?? {}).filter((s) => s.split(".").length !== 6);
  };

  beforeEach(async () => {
    await vaciaBroker(ctx.conn.jsm);
    await creaStreamEstado(ctx.conn.jsm, "clientes");
    await creaDlq(ctx.conn.jsm, "clientes");
    await creaEvt(ctx.conn.jsm, "clientes");
    await sembrar(ctx.conn, { n: 3 });
    await publicaDlq(ctx.conn, "dlq.state.clientes.cliente.v1", dlqEstado(), "dlq-estado-1");
    await publicaDlq(ctx.conn, "dlq.clientes.cliente.v1.creado", dlqEvento, "dlq-evento-1");
  });

  test("`--subject dlq.state.…` (o `state.…`) se rechaza con un mensaje que apunta a `flux state resync`, y no publica nada", async () => {
    const estadoAntes = await mensajes("STATE_CLIENTES");
    for (const subj of ["dlq.state.clientes.cliente.v1", "dlq.state.clientes.>", "state.clientes.cliente.v1", "dlq.state.>"]) {
      const r = await flux(ctx.url, ["dlq", "replay", "clientes", "--subject", subj, "--confirm", "--yes"]);
      assert.equal(r.code, 1, subj);
      assert.match(r.err, /dlq replay no reproduce estado \(dlq\.state\.>\)/, subj);
      assert.match(r.err, /flux state resync <servicio> <contrato> \[--tenant t\]/, subj);
      assert.match(r.err, /pisaría uno nuevo/, subj);
    }
    const sinFlag = await flux(ctx.url, ["dlq", "replay", "state"]);
    assert.equal(sinFlag.code, 1);
    assert.match(sinFlag.err, /flux state resync/);
    assert.equal(await mensajes("STATE_CLIENTES"), estadoAntes, "el stream de estado queda intacto");
    assert.deepEqual(await raros(), []);
    assert.equal(await mensajes("EVT_CLIENTES"), 0);
  });

  test("sin --subject: el EVENTO se reproduce y el de estado se NOMBRA como omitido (no se publica en `state.`)", async () => {
    const estadoAntes = await mensajes("STATE_CLIENTES");
    const sim = await flux(ctx.url, ["dlq", "replay", "clientes"]);
    assert.equal(sim.code, 0, sim.err);
    assert.match(sim.out, /1 mensaje\(s\) de la DLQ de estado \(dlq\.state\.clientes\.>\) NO se reproducen/);
    assert.match(sim.out, /1 evento\(s\) reproducibles\n\s+1 → clientes\.cliente\.v1\.creado/);
    assert.match(sim.out, /Simulación\. Nada se ha publicado/);
    assert.equal(await mensajes("EVT_CLIENTES"), 0);

    const r = await flux(ctx.url, ["dlq", "replay", "clientes", "--confirm", "--yes"]);
    assert.equal(r.code, 0, r.err);
    assert.match(r.out, /1 evento\(s\) republicados conservando su id original/);
    assert.equal(await mensajes("EVT_CLIENTES"), 1, "el evento sí se republicó");
    assert.equal(await mensajes("STATE_CLIENTES"), estadoAntes, "y el stream de estado no ha recibido nada");
    assert.deepEqual(await raros(), [], "sin claves fantasma como `state.clientes.v1.cliente`");
  });

  test("con un patrón que abarca las dos (`dlq.>`) el de estado tampoco se reproduce", async () => {
    const estadoAntes = await mensajes("STATE_CLIENTES");
    const r = await flux(ctx.url, ["dlq", "replay", "clientes", "--subject", "dlq.>", "--confirm", "--yes"]);
    assert.equal(r.code, 0, r.err);
    assert.match(r.out, /1 mensaje\(s\) de la DLQ de estado .* NO se reproducen/);
    assert.equal(await mensajes("EVT_CLIENTES"), 1);
    assert.equal(await mensajes("STATE_CLIENTES"), estadoAntes);
    assert.deepEqual(await raros(), []);
  });

  test("un dominio SIN estado sigue reproduciendo como antes (y no menciona el estado)", async () => {
    await creaDlq(ctx.conn.jsm, "pedidos", ["dlq.pedidos.>"]);
    await creaEvt(ctx.conn.jsm, "pedidos");
    await publicaDlq(ctx.conn, "dlq.pedidos.pedido.v1.creado", { ...dlqEvento, id: "ped-1", type: "com.flux.pedidos.pedido.creado.v1" }, "dlq-ped-1");
    const r = await flux(ctx.url, ["dlq", "replay", "pedidos", "--confirm", "--yes"]);
    assert.equal(r.code, 0, r.err);
    assert.doesNotMatch(r.out, /DLQ de estado/);
    assert.equal(await mensajes("EVT_PEDIDOS"), 1);
  });

  test("`dlq inspect` de la DLQ de estado: clave y revisión (no son PII) y la orden de recuperación exacta", async () => {
    const r = await flux(ctx.url, ["dlq", "inspect", "clientes", "--subject", "state.clientes.cliente.v1"]);
    assert.equal(r.code, 0, r.err);
    assert.match(r.out, /estado\s+clave cli-1 · revisión 41 · tenant acme/);
    assert.match(r.out, /recuperar\s+flux state resync tienda-web clientes\.cliente\.v1 --tenant acme/);
  });

  test("`dlq inspect`: si el data de una DLQ de estado lleva MÁS que { key, revision }, avisa y no lo imprime", async () => {
    await publicaDlq(ctx.conn, "dlq.state.clientes.cliente.v1", dlqEstado({ email: "ana@example.com" }), "dlq-estado-sucio");
    const r = await flux(ctx.url, ["dlq", "inspect", "clientes", "--subject", "state.clientes.cliente.v1"]);
    assert.match(r.out, /data con campos de más \(email\): viola 10-state\.md §6, no se imprime/);
    assert.doesNotMatch(r.out, /ana@example\.com/);
  });

  test("`dlq inspect` honra --show-data para un evento confidential (antes se anunciaba y no hacía nada)", async () => {
    await publicaDlq(ctx.conn, "dlq.clientes.cliente.v1.creado", { ...dlqEvento, id: "conf-1", dataclassification: "confidential", data: { email: "ana@example.com" } }, "dlq-conf-1");
    const oculto = await flux(ctx.url, ["dlq", "inspect", "clientes", "--subject", "clientes.cliente.v1.creado"]);
    assert.doesNotMatch(oculto.out, /ana@example\.com/);
    assert.match(oculto.out, /oculto: dataclassification=confidential/);
    const visible = await flux(ctx.url, ["dlq", "inspect", "clientes", "--subject", "clientes.cliente.v1.creado", "--show-data"]);
    assert.match(visible.out, /ana@example\.com/);
  });
});

// ─── doctor ──────────────────────────────────────────────────────────────────────────────────────

conBroker("flux doctor audita el estado", (ctx) => {
  const D = "tienda-web__state_clientes_cliente_v1__acme";
  const F = "state.clientes.cliente.v1.acme.*";

  /** El entorno que `flux provision` dejaría para un dominio con estado, sano. */
  async function entornoSano() {
    await creaStreamEstado(ctx.conn.jsm, "clientes");
    await creaDlq(ctx.conn.jsm, "clientes");
    await creaEvt(ctx.conn.jsm, "clientes");
    await creaDurable(ctx.conn.jsm, "STATE_CLIENTES", D, F);
  }
  beforeEach(() => vaciaBroker(ctx.conn.jsm));

  test("la configuración buena: doctor sale 0 y NO trata max_age=0 de un estado como un fallo", async () => {
    await entornoSano();
    await sembrar(ctx.conn, { n: 3 });
    const r = await flux(ctx.url, ["doctor"]);
    assert.equal(r.code, 0, r.out);
    assert.match(r.out, /STATE_CLIENTES\s+state\.clientes\.>/);
    assert.match(r.out, /STATE_CLIENTES\s+tienda-web__state_clientes_cliente_v1__acme\s+✓\s+30s\s+6\s+\d+\s+ok/);
    assert.doesNotMatch(r.out, /sin max_age/);
    assert.match(r.out, /0 error\(es\)|conformes/);
    assert.doesNotMatch(r.out, /✗/);
  });

  // Cada fila rompe UNA cosa de un entorno que, sin ella, está verde (la prueba de arriba).
  const roturas = [
    {
      titulo: "max_msgs_per_subject ausente (un log, no un mapa)",
      rompe: (j) => creaStreamEstado(j, "clientes", { max_msgs_per_subject: -1 }),
      veo: /STATE_CLIENTES\s+max_msgs_per_subject=-1, debe ser 1/,
    },
    {
      titulo: "allow_direct apagado",
      rompe: (j) => creaStreamEstado(j, "clientes", { allow_direct: false }),
      veo: /STATE_CLIENTES\s+allow_direct=false, debe ser true/,
    },
    {
      titulo: "max_age de 30 días (el estado caduca)",
      rompe: (j) => creaStreamEstado(j, "clientes", { max_age: 30 * 86400 * 1e9 }),
      veo: /STATE_CLIENTES\s+max_age=30d, debe ser 0/,
    },
    {
      titulo: "discard=new por subject (un create-only)",
      rompe: (j) => creaStreamEstado(j, "clientes", { discard: "new", discard_new_per_subject: true }),
      veo: /STATE_CLIENTES\s+discard=new con discard_new_per_subject, debe ser 'old'/,
    },
    {
      titulo: "almacenamiento en memoria",
      rompe: (j) => creaStreamEstado(j, "clientes", { storage: "memory" }),
      veo: /STATE_CLIENTES\s+storage=memory, debe ser 'file'/,
    },
    {
      titulo: "el stream no captura state.<dom>.>",
      rompe: (j) => creaStreamEstado(j, "clientes", { subjects: ["state.otro.>"] }),
      veo: /STATE_CLIENTES\s+no captura state\.clientes\.>/,
    },
    {
      titulo: "la DLQ es la de v1.0 y no captura dlq.state.<dom>.>",
      rompe: async (j) => { await creaStreamEstado(j, "clientes"); await creaDlq(j, "clientes", ["dlq.clientes.>"]); await creaEvt(j, "clientes"); await creaDurable(j, "STATE_CLIENTES", D, F); },
      veo: /STATE_CLIENTES\s+DLQ_CLIENTES no captura dlq\.state\.clientes\.>/,
      completo: true,
    },
    {
      titulo: "no existe DLQ_<DOM>",
      rompe: async (j) => { await creaStreamEstado(j, "clientes"); await creaEvt(j, "clientes"); await creaDurable(j, "STATE_CLIENTES", D, F); },
      veo: /STATE_CLIENTES\s+no existe DLQ_CLIENTES/,
      completo: true,
    },
    {
      titulo: "un durable de estado con deliver_policy=all",
      rompe: (j) => creaDurable(j, "STATE_CLIENTES", "otro-svc__state_clientes_cliente_v1__acme", F, { deliver_policy: "all" }),
      veo: /STATE_CLIENTES\/otro-svc__state_clientes_cliente_v1__acme\s+deliver_policy=all, debe ser last_per_subject/,
      durable: true,
    },
    {
      titulo: "un durable de estado con deliver_policy=new (no recibe la foto)",
      rompe: (j) => creaDurable(j, "STATE_CLIENTES", "otro-svc__state_clientes_cliente_v1__acme", F, { deliver_policy: "new" }),
      veo: /deliver_policy=new, debe ser last_per_subject/,
      durable: true,
    },
    {
      titulo: "un durable de un tenant SIN el tenant en el nombre (reparte mensajes)",
      rompe: (j) => creaDurable(j, "STATE_CLIENTES", "otro-svc__state_clientes_cliente_v1", F),
      veo: /no lleva tenant y el filtro es del tenant «acme»/,
      durable: true,
    },
    {
      titulo: "un durable con ack_wait≠backoff[0] (alterado por el servidor)",
      rompe: (j) => creaDurable(j, "STATE_CLIENTES", "otro-svc__state_clientes_cliente_v1__acme", F, { backoff: [1e9, 5e9, 30e9, 120e9, 600e9], ack_wait: 30e9 }),
      veo: /ack_wait=1s < 30s/,
      durable: true,
    },
  ];

  for (const { titulo, rompe, veo, completo, durable } of roturas) {
    test(`ROJO: ${titulo}`, async () => {
      if (completo) {
        await rompe(ctx.conn.jsm);
      } else if (durable) {
        await entornoSano();
        await rompe(ctx.conn.jsm);
      } else {
        // se rompe el stream de estado y el resto es el entorno sano
        await rompe(ctx.conn.jsm);
        await creaDlq(ctx.conn.jsm, "clientes");
        await creaEvt(ctx.conn.jsm, "clientes");
      }
      const r = await flux(ctx.url, ["doctor"]);
      assert.equal(r.code, 1, `doctor debía salir 1:\n${r.out}`);
      assert.match(sinColor(r.out), veo);
    });
  }

  test("VERDE otra vez al arreglarlo: el mismo cluster roto, corregido, sale 0", async () => {
    await creaStreamEstado(ctx.conn.jsm, "clientes", { allow_direct: false });
    await creaDlq(ctx.conn.jsm, "clientes");
    await creaEvt(ctx.conn.jsm, "clientes");
    assert.equal((await flux(ctx.url, ["doctor"])).code, 1);
    await ctx.conn.jsm.streams.update("STATE_CLIENTES", { ...(await ctx.conn.jsm.streams.info("STATE_CLIENTES")).config, allow_direct: true });
    assert.equal((await flux(ctx.url, ["doctor"])).code, 0);
  });

  test("replicas=1 es una nota (-v), no un fallo", async () => {
    await entornoSano();
    const r = await flux(ctx.url, ["doctor", "-v"]);
    assert.equal(r.code, 0);
    assert.match(r.out, /STATE_CLIENTES\s+replicas=1/);
  });

  // El doctor original NO debe haber cambiado para un cluster sin estado: la receta del job de CI
  // «flux doctor detecta un cluster roto».
  test("sin estado: sigue verde con lo conforme y rojo con el ack_wait de 1s", async () => {
    const jsm = ctx.conn.jsm;
    const S = 1e9;
    await jsm.streams.add({ name: "EVT_CITEST", subjects: ["citest.>"], storage: "file", retention: "limits", discard: "old", max_age: 30 * 86400 * S, duplicate_window: 120 * S });
    await jsm.streams.add({ name: "DLQ_CITEST", subjects: ["dlq.citest.>"], storage: "file", retention: "limits", discard: "old", max_age: 90 * 86400 * S });
    await jsm.consumers.add("EVT_CITEST", {
      durable_name: "ok-api__citest_p_v1_creado", filter_subject: "citest.p.v1.creado", ack_policy: "explicit", ack_wait: 30 * S,
      max_deliver: 6, backoff: [30 * S, 60 * S, 300 * S, 900 * S, 1800 * S], max_ack_pending: 256,
    });
    assert.equal((await flux(ctx.url, ["doctor"])).code, 0);
    await jsm.consumers.add("EVT_CITEST", {
      durable_name: "roto-api__citest_p_v1_creado", filter_subject: "citest.p.v1.creado", ack_policy: "explicit", ack_wait: 30 * S,
      max_deliver: 6, backoff: [1 * S, 5 * S, 30 * S, 120 * S, 600 * S],
    });
    assert.equal((await flux(ctx.url, ["doctor"])).code, 1);
  });
});

// ─── tail ────────────────────────────────────────────────────────────────────────────────────────

conBroker("flux tail entiende el estado y no filtra PII", (ctx) => {
  beforeEach(async () => {
    await vaciaBroker(ctx.conn.jsm);
    await creaStreamEstado(ctx.conn.jsm, "clientes");
    await creaDlq(ctx.conn.jsm, "clientes");
    await creaEvt(ctx.conn.jsm, "secreto");
  });

  /** Arranca un tail, espera a que esté listo, ejecuta `escribe` y espera a ver `espera`. */
  async function conTail(args, escribe, espera) {
    const t = fluxVivo(ctx.url, ["tail", ...args]);
    try {
      assert.ok(await t.hasta((o) => /Ctrl\+C para salir/.test(o.err)), `el tail no arrancó:\n${t.salida.err}`);
      await escribe();
      assert.ok(await t.hasta((o) => espera.test(o.out)), `no apareció ${espera}:\n${t.salida.out}\n${t.salida.err}`);
      return t.salida.out;
    } finally {
      await t.parar();
    }
  }

  test("`tail state.<dom>.>` muestra tenant, clave, revisión, versión y LÁPIDA, y OCULTA el confidential", async () => {
    const out = sinColor(await conTail(["state.clientes.>"], async () => {
      await escribeEstado(ctx.conn, { contrato: C, tenant: "acme", clave: "cli-1", version: 1 });
      await escribeEstado(ctx.conn, { contrato: C, tenant: "globex", clave: "cli-1", version: 4 });
      await escribeEstado(ctx.conn, { tipo: "delete", contrato: C, tenant: "acme", clave: "cli-1", version: 2 });
    }, /LÁPIDA/));
    assert.match(out, /state\.clientes\.cliente\.v1\.acme\.cli-1 acme\/cli-1 rev 1 · v1 .*\[confidential\]/);
    assert.match(out, /globex\/cli-1 rev 2 · v4/);
    assert.match(out, /acme\/cli-1 rev 3 · v2 LÁPIDA .*\[lápida\]/);
    assert.doesNotMatch(out, /example\.com/);
  });

  test("`--tenant` filtra por el tenant del SUBJECT", async () => {
    const out = sinColor(await conTail(["state.clientes.>", "--tenant", "globex"], async () => {
      await escribeEstado(ctx.conn, { contrato: C, tenant: "acme", clave: "cli-1", version: 1 });
      await escribeEstado(ctx.conn, { contrato: C, tenant: "globex", clave: "cli-2", version: 1 });
    }, /globex\/cli-2/));
    assert.doesNotMatch(out, /acme\/cli-1/);
  });

  test("`--json` y `--full` pasan por la misma redacción; `--show-data` la levanta", async () => {
    const escribe = () => escribeEstado(ctx.conn, { contrato: C, tenant: "acme", clave: "cli-1", version: 1 });
    for (const flag of ["--json", "--full"]) {
      const oculto = await conTail(["state.clientes.>", flag], escribe, /dataclassification/);
      assert.doesNotMatch(oculto, /example\.com/, flag);
      assert.match(oculto, /\[oculto: dataclassification=confidential\]/, flag);
      await vaciaBroker(ctx.conn.jsm);
      await creaStreamEstado(ctx.conn.jsm, "clientes");
      const visible = await conTail(["state.clientes.>", flag, "--show-data"], escribe, /dataclassification/);
      assert.match(visible, /cli-1@example\.com/, `${flag} --show-data`);
      await vaciaBroker(ctx.conn.jsm);
      await creaStreamEstado(ctx.conn.jsm, "clientes");
    }
  });

  test("un cuerpo ilegible en estado no se vuelca (podría llevar el dato)", async () => {
    const out = await conTail(["state.clientes.>"], () => publicaCrudo(ctx.conn, "state.clientes.cliente.v1.acme.rota", "esto no es json: ana@example.com"), /POISON/);
    assert.doesNotMatch(out, /ana@example\.com/);
    assert.match(out, /cuerpo no mostrado/);
  });

  test("`tail dlq.state.<dom>.>` resuelve DLQ_<DOM> (antes DLQ_STATE) y enseña clave y revisión", async () => {
    const { data, ...sobre } = sobreEstado({ contrato: C, tenant: "acme", clave: "cli-1", version: 7 });
    const evento = { ...sobre, dlqreason: "permanent", dlqattempts: 1, dlqconsumer: "tienda-web__state_clientes_cliente_v1__acme", dlqerror: "PAIS_NO_SOPORTADO: x", dlqtime: "2026-08-20T10:25:40.117Z", data: { key: "cli-1", revision: 41 } };
    const out = sinColor(await conTail(["dlq.state.clientes.>"], () => publicaDlq(ctx.conn, "dlq.state.clientes.cliente.v1", evento, "d1"), /clave cli-1/));
    assert.match(out, /dlq\.state\.clientes\.cliente\.v1 cli-1 .*\[permanent ×1\].* clave cli-1 · revisión 41/);
    assert.match(out, /PAIS_NO_SOPORTADO/);
    assert.doesNotMatch(out, /example\.com/);
  });

  test("EVENTOS: `--full` y `--json` ya no vuelcan el confidential (antes lo hacían: README decía lo contrario)", async () => {
    const evento = { specversion: "1.0", id: "e-1", source: "/x", type: "com.flux.secreto.cosa.hecha.v1", time: "2026-08-20T10:25:39.412Z", subject: "c-1", correlationid: "corr12345678", tenantid: "acme", producerversion: "1", dataclassification: "confidential", data: { email: "ana@example.com" } };
    const pub = () => ctx.conn.js.publish("secreto.cosa.v1.hecha", new TextEncoder().encode(JSON.stringify({ ...evento, id: `e-${Math.random()}` })));
    for (const flag of [[], ["--full"], ["--json"]]) {
      const out = await conTail(["secreto.>", ...flag], pub, /secreto|specversion/);
      assert.doesNotMatch(out, /ana@example\.com/, `tail ${flag.join(" ")}`);
    }
    const visible = await conTail(["secreto.>", "--json", "--show-data"], pub, /specversion/);
    assert.match(visible, /ana@example\.com/);
  });
});
