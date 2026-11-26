#!/usr/bin/env node
/**
 * Aplica las ACLs que genera `scripts/gen-acl.mjs` a un nats-server REAL y comprueba, en los
 * dos sentidos, que lo permitido funciona y lo denegado se deniega.
 * Contrato normativo: specification/06-security.md §3.
 *
 *   (cd conformance && npm install) && (cd cli && npm install)
 *   node scripts/verify-acl.mjs
 *
 * Sale con 0 si TODAS las comprobaciones se cumplen. Cada línea es ✓ o ✗.
 *
 * Por qué existe (H3 de docs/plan-v1.1/PLAN.md): hasta ahora nada aplicaba los permisos
 * generados a un broker; CI solo validaba el manifiesto. Resultado: la ACL «funcionaba» en el
 * papel y un servicio no podía ni consumir (H1) mientras leía dominios ajenos (H2). Un
 * permiso que no se ha ejecutado contra un servidor es una hipótesis.
 *
 * Qué demuestra, por secciones:
 *   1. El generador: expansión de `consumes`, errores, `--nkeys`, `--json`, `--tenants`.
 *   2. `flux provision`: crea, es idempotente, `--dry-run` no crea, informa de una diferencia
 *      sembrada sin sobrescribirla, y nunca borra.
 *   3. El ciclo COMPLETO de cada servicio con sus permisos generados: connect, publicar,
 *      consumir, work-in-progress, ack, nak, term, DLQ, lectura de su DLQ y sondeo de métricas
 *      —las llamadas que hace sdk-node/src/client.ts—.
 *   4. Las denegaciones: dominio ajeno, DLQ ajena, filtro más amplio, DURABLE.CREATE,
 *      STREAM.MSG.GET, durable ajeno, inbox común y ajeno, gestión de streams, y el escenario
 *      de fuga de A-2 (un espía de `_INBOX.>` mientras otro servicio consume).
 *   5. MINIMALIDAD: quitando cada permiso concedido a un servicio, su ciclo se rompe. Si no, el
 *      permiso sobraba. (Con `--dev`, CONSUMER.CREATE es la única holgura: el SDK de Node la tolera.)
 *   6. Las comprobaciones pueden FALLAR: con una ACL mutilada a propósito (inbox `_INBOX.>`,
 *      o `$JS.API.>` «para arreglarlo») las mismas sondas detectan la fuga.
 *   7. Regresión: la ACL v1 congelada (conformance/fixtures/acl-v1.conf) sigue rota en la
 *      forma documentada: H1 (no consume) y H2 (fuga por el inbox común).
 *   8. Modelo B (`--tenants`): mismos permisos, una account por tenant, aislamiento real.
 *
 * Relación con la suite de seguridad (`node conformance/security.mjs`, Fase 1.5): NO es redundante.
 * La suite verifica el contrato de seguridad de extremo a extremo —con TLS, con los SDKs de Node
 * y de Rust— y sus casos son DATOS; este script verifica lo que ella no mira: el generador (sus
 * errores y `--nkeys`), `flux provision`, la minimalidad de cada permiso, `--dev` y el Modelo B.
 * Las secciones 4, 6 y 7 se solapan con los casos S2–S7 y S9 a propósito: la spec cita este
 * script por su nombre (06-security.md §3), y se quedarán aquí hasta que la spec cite la suite.
 * La infraestructura común (arrancar nats-server, claves, sondas) vive en
 * scripts/lib/nats-harness.mjs, que usan los dos.
 *
 * NO cubre TLS ni credenciales inválidas por TLS (caso S8 de la suite): aquí el listener es
 * plano, sobre 127.0.0.1.
 *
 * Entorno: NATS_SERVER (binario; por defecto `nats-server`), VERIFY_ACL_PORT (primer puerto a
 * probar; por defecto 4300). Los temporales viven en `os.tmpdir()` y se borran al terminar.
 */

import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";

import {
  dlqDurableName,
  dlqStreamName,
  domainOf,
  durableName,
  streamName,
} from "./lib/manifest.mjs";
// La infraestructura común con conformance/security.mjs: arrancar y parar nats-server, claves,
// conexiones que vigilan las violaciones de permiso y las sondas. Ver scripts/lib/nats-harness.mjs.
import {
  Banco,
  CLI,
  EJEMPLO,
  ROOT,
  conUsuarios,
  conTiempo,
  conectar,
  configConsumidorCanonica,
  credsDe,
  dec,
  enc,
  esViolacion,
  gen,
  jetstream,
  jetstreamManager,
  nanos,
  nuevaClave,
  pedir,
  permisosJson,
  publicarCrudo,
  publicarJs,
  recibirUno,
  resumen,
  sleep,
  suscribirCrudo,
  usuarioConf,
  vigilar,
} from "./lib/nats-harness.mjs";

const NATS_SERVER = process.env.NATS_SERVER ?? "nats-server";
const banco = new Banco({
  prefijo: "flux-verify-acl-",
  puertoBase: Number(process.env.VERIFY_ACL_PORT ?? 4300),
  puertos: 50,
  natsServer: NATS_SERVER,
});
let TMP;
const arrancar = (nombre, acl) => banco.arrancar(nombre, acl, { servidorNombre: `verify-${nombre}` });
const parar = (child) => banco.parar(child);

// ─── Informe ─────────────────────────────────────────────────────────────────

const R = { total: 0, fallos: 0 };

function afirmar(cond, texto, detalle) {
  R.total++;
  if (cond) {
    console.log(`  ✓ ${texto}`);
  } else {
    R.fallos++;
    console.log(`  ✗ ${texto}${detalle ? `\n      ${String(detalle).split("\n").join("\n      ")}` : ""}`);
  }
  return !!cond;
}
const seccion = (t) => console.log(`\n${t}`);
const nota = (t) => console.log(`  · ${t}`);

// ─── Identidades y conexiones ────────────────────────────────────────────────

/** Una conexión de servicio, como la haría el SDK: inbox privado `_INBOX.<servicio>`. */
const comoServicio = (url, nombre, seed, base = nombre) =>
  conectar(url, { nombre, seed, inboxPrefix: `_INBOX.${base}` });

// ─── Operación: lo que hace el usuario ops ───────────────────────────────────

let CANON;

/** La config canónica de consumidor que pide el SDK en subscribe(). protocol.json manda. */
async function configConsumidor(durable, filtro) {
  CANON ??= JSON.parse(await readFile(path.join(ROOT, "protocol.json"), "utf8")).consumer;
  return configConsumidorCanonica(CANON, durable, filtro);
}

async function conectarOps(url, seed) {
  const nc = await conectar(url, { nombre: "ops", seed });
  return { nc, jsm: await jetstreamManager(nc), js: jetstream(nc) };
}

/**
 * Un consumidor LEGÍTIMO de inventario lee un evento con marca. El durable arranca en `new`:
 * con `all` el evento marcado quedaría detrás de todo lo sembrado por las demás pruebas.
 */
async function lecturaLegitima(ops, nc, marca) {
  const [stream, durable, filtro] = ["EVT_INVENTARIO", "pedidos-api__inventario_stock_v1_ajustado", "inventario.stock.v1.ajustado"];
  await ops.jsm.consumers.delete(stream, durable).catch(() => {});
  await ops.jsm.consumers.add(stream, { ...(await configConsumidor(durable, filtro)), deliver_policy: "new" });
  await ops.js.publish(filtro, enc.encode(JSON.stringify({ sku: marca })));
  const m = await recibirUno(await jetstream(nc).consumers.get(stream, durable), 2500);
  m?.ack();
  return !!m && dec.decode(m.data).includes(marca);
}

/** Un durable limpio: borra el que haya y lo crea canónico, como lo dejaría `flux provision`. */
async function reiniciarDurable(ops, stream, durable, filtro) {
  await ops.jsm.consumers.delete(stream, durable).catch(() => {});
  await ops.jsm.consumers.add(stream, await configConsumidor(durable, filtro));
}

// ─── Ciclo de un servicio ────────────────────────────────────────────────────

/**
 * Lo que hace un servicio con el SDK de Node, paso a paso (sdk-node/src/client.ts). Cada
 * paso es independiente: un permiso que falta rompe SU paso y no oculta los demás.
 *
 * `ops` prepara el terreno (mensajes sembrados, durables limpios) y comprueba EFECTOS, no
 * solo ausencia de error: un ack denegado no falla en el cliente, deja el mensaje pendiente.
 */
async function ciclo({ nc, svc, ops, subjectPropio, nakTerm = false }) {
  const pasos = [];
  const paso = async (nombre, fn) => {
    try {
      const detalle = await conTiempo(fn(), 6000, nombre);
      pasos.push({ nombre, ok: true, detalle });
      return true;
    } catch (e) {
      pasos.push({ nombre, ok: false, detalle: resumen(e) });
      return false;
    }
  };

  const propio = svc.owns[0];
  const [sO, dO] = [streamName(propio), dlqStreamName(propio)];
  const durableDlq = dlqDurableName(svc.name, propio);
  const subjDlqPropia = `dlq.${propio}.verify.v1.muerto`;

  // Terreno: dos mensajes en cada subject consumido, uno en la DLQ propia, y durables limpios.
  for (const s of svc.consumes) {
    await ops.js.publish(s, enc.encode('{"sembrado":1}'));
    await ops.js.publish(s, enc.encode('{"sembrado":2}'));
    await reiniciarDurable(ops, streamName(domainOf(s)), durableName(svc.name, s), s);
  }
  await ops.js.publish(subjDlqPropia, enc.encode('{"muerto":1}'));
  await reiniciarDurable(ops, dO, durableDlq, `dlq.${propio}.>`);

  let jsm;
  const js = jetstream(nc, { timeout: 2000 });
  await paso("connect(): jetstreamManager → $JS.API.INFO", async () => {
    jsm = await jetstreamManager(nc, { timeout: 2000 });
  });
  jsm ??= await jetstreamManager(nc, { timeout: 2000, checkAPI: false });

  await paso(`publish(): consulta del stream propio → STREAM.INFO.${sO}`, () => jsm.streams.info(sO));
  await paso(`publish(): publicar en ${subjectPropio} por JetStream (el ack vuelve por el inbox)`, async () => {
    const r = await js.publish(subjectPropio, enc.encode('{"verify":true}'), { msgID: randomUUID() });
    if (r.stream !== sO) throw new Error(`lo capturó ${r.stream}, no ${sO}`);
  });

  for (const d of [...new Set(svc.consumes.map(domainOf))]) {
    await paso(`subscribe(): STREAM.INFO.${streamName(d)}`, () => jsm.streams.info(streamName(d)));
    await paso(`subscribe(): STREAM.INFO.${dlqStreamName(d)}`, () => jsm.streams.info(dlqStreamName(d)));
  }

  /** Como subscribe(): add y, si el broker lo deniega, info; y la config devuelta debe ser la pedida. */
  const crearOInformar = async (stream, durable, filtro) => {
    const pedido = await configConsumidor(durable, filtro);
    let efectiva;
    let viaAdd = true;
    try {
      efectiva = (await jsm.consumers.add(stream, pedido)).config;
    } catch {
      viaAdd = false;
      efectiva = (await jsm.consumers.info(stream, durable)).config;
    }
    for (const f of ["ack_wait", "max_deliver", "max_ack_pending"]) {
      if (efectiva[f] !== pedido[f]) throw new Error(`${f}: pedido ${pedido[f]}, efectivo ${efectiva[f]}`);
    }
    if (JSON.stringify(efectiva.backoff) !== JSON.stringify(pedido.backoff)) throw new Error("backoff distinto");
    return viaAdd;
  };
  const pasoCrear = async (nombre, stream, durable, filtro) => {
    let viaAdd;
    await paso(nombre, async () => {
      viaAdd = await crearOInformar(stream, durable, filtro);
      return viaAdd ? "CONSUMER.CREATE" : "CONSUMER.CREATE denegado → CONSUMER.INFO";
    });
    pasos.at(-1).creoViaAdd = viaAdd;
  };

  let primerAck;
  let primero = true;
  for (const s of svc.consumes) {
    const stream = streamName(domainOf(s));
    const durable = durableName(svc.name, s);
    await pasoCrear(`subscribe(${s}): consumers.add (o consumers.info si se deniega) con config honrada`, stream, durable, s);

    let consumidor;
    await paso(`consume(${s}): consumers.get → CONSUMER.INFO`, async () => {
      consumidor = await js.consumers.get(stream, durable);
    });

    let m;
    await paso(`consume(${s}): pedir mensajes → CONSUMER.MSG.NEXT, y llega uno (con el subject original)`, async () => {
      m = await recibirUno(consumidor, 2500);
      if (!m) throw new Error("no llegó ningún mensaje");
      // La entrega viaja por el inbox pero con el subject ORIGINAL del evento: por eso existe la
      // fuga H2, y por eso el ciclo comprueba qué llega, no solo que no haya violaciones.
      if (m.subject !== s) throw new Error(`llegó con el subject ${m.subject}, esperado ${s}`);
    });
    if (primero) primerAck = m?.reply;

    if (m) {
      const antes = nc.violaciones.length;
      m.working();
      await nc.flush();
      await sleep(200);
      await paso(`working(${s}): work-in-progress → $JS.ACK`, async () => {
        const nuevas = nc.violaciones.slice(antes).filter((v) => v.includes("$JS.ACK"));
        if (nuevas.length) throw new Error(nuevas[0]);
      });
      m.ack();
      await nc.flush();
      await sleep(300);
      await paso(`ack(${s}): el broker lo registra (num_ack_pending 0, ack_floor al día)`, async () => {
        const i = await ops.jsm.consumers.info(stream, durable);
        if (i.num_ack_pending !== 0 || i.ack_floor.stream_seq < m.seq) {
          throw new Error(`pendientes ${i.num_ack_pending}, ack_floor ${i.ack_floor.stream_seq}, esperado ≥ ${m.seq}`);
        }
      });
    } else {
      pasos.push({ nombre: `working(${s}): work-in-progress → $JS.ACK`, ok: false, detalle: "no hay mensaje" });
      pasos.push({ nombre: `ack(${s}): el broker lo registra (num_ack_pending 0, ack_floor al día)`, ok: false, detalle: "no hay mensaje" });
    }

    if (nakTerm && primero) {
      let m2;
      await paso(`nak(300 ms): el mensaje se reentrega con deliveryCount 2 → $JS.ACK`, async () => {
        const a = await recibirUno(consumidor, 2500);
        if (!a) throw new Error("no llegó el segundo mensaje");
        a.nak(300);
        await nc.flush();
        await sleep(500);
        // Si el stream tiene más mensajes, antes de la reentrega pueden llegar otros NUEVOS
        // (deliveryCount 1): se confirman y se sigue hasta ver el nak'd, que vuelve con 2.
        for (let i = 0; i < 8 && !m2; i++) {
          const x = await recibirUno(consumidor, 1500);
          if (!x) break;
          if (x.seq === a.seq) m2 = x;
          else x.ack();
        }
        if (!m2) throw new Error("no hubo reentrega tras nak");
        if (m2.info.deliveryCount !== 2) throw new Error(`deliveryCount ${m2.info.deliveryCount}, esperado 2`);
      });
      await paso(`term(): el broker lo da por terminado → $JS.ACK`, async () => {
        if (!m2) throw new Error("no hay mensaje reentregado");
        m2.term();
        await nc.flush();
        await sleep(300);
        const i = await ops.jsm.consumers.info(stream, durable);
        if (i.num_ack_pending !== 0) throw new Error(`num_ack_pending ${i.num_ack_pending}`);
      });
    }
    primero = false;

    await paso(`DLQ: publicar en dlq.${s}`, async () => {
      const r = await js.publish(`dlq.${s}`, enc.encode('{"dlq":true}'), { msgID: randomUUID() });
      if (r.stream !== dlqStreamName(domainOf(s))) throw new Error(`lo capturó ${r.stream}`);
    });
    await paso(`métricas(${s}): sondeo de num_pending → CONSUMER.INFO`, async () => {
      const i = await jsm.consumers.info(stream, durable);
      if (typeof i.num_pending !== "number") throw new Error("sin num_pending");
    });
  }

  // El dueño lee su propia DLQ con un pull consumer (06-security.md §3.1).
  await pasoCrear(`DLQ propia: consumers.add (o consumers.info) del durable ${durableDlq}`, dO, durableDlq, `dlq.${propio}.>`);
  await paso(`DLQ propia: leer ${dO} → CONSUMER.INFO, MSG.NEXT y ack`, async () => {
    const c = await js.consumers.get(dO, durableDlq);
    const d = await recibirUno(c, 2500);
    if (!d) throw new Error("no llegó el mensaje muerto");
    d.ack();
    await nc.flush();
    await sleep(300);
    const i = await ops.jsm.consumers.info(dO, durableDlq);
    if (i.num_ack_pending !== 0 || i.ack_floor.stream_seq < d.seq) throw new Error("el ack no se registró");
  });

  return { pasos, ok: pasos.every((p) => p.ok), fallidos: pasos.filter((p) => !p.ok).map((p) => p.nombre), ack: primerAck };
}

// ─── El generador (sin servidor) ─────────────────────────────────────────────

let nManifiesto = 0;
async function manifiestoTemporal(obj) {
  const f = path.join(TMP, `manifiesto-${++nManifiesto}.json`);
  await writeFile(f, JSON.stringify(obj));
  return f;
}


async function seccionGenerador(subjectsBundle) {
  seccion("1. El generador");

  const check = gen(EJEMPLO, "--check");
  afirmar(check.codigo === 0 && /12 durable/.test(check.salida), "--check valida el manifiesto de ejemplo y cuenta 12 durables", check.salida + check.errores);

  const json = permisosJson();
  afirmar(json.length === 4, "--json devuelve un elemento por servicio");

  // La lista cerrada de lo concedible y la de lo denegable salen de protocol.json: una sola
  // fuente, que check-protocol-json prueba en los dos sentidos.
  const acl = JSON.parse(await readFile(path.join(ROOT, "protocol.json"), "utf8")).security.acl;
  const permitidos = Object.entries(acl.permissionPatterns).filter(([k]) => !k.startsWith("$")).map(([, v]) => new RegExp(v));
  const peligroso = new RegExp(acl.neverGrantedPattern);
  for (const s of json) {
    afirmar(
      s.subscribe.length === 1 && s.subscribe[0] === `_INBOX.${s.name}.>`,
      `${s.name}: SOLO se suscribe a su inbox privado (ningún subject de dominio, ningún _INBOX.>)`,
      JSON.stringify(s.subscribe),
    );
    const malos = s.publish.filter((p) => peligroso.test(p) || !permitidos.some((re) => re.test(p)));
    afirmar(malos.length === 0, `${s.name}: ningún permiso de publicación fuera de la lista cerrada (nada de $JS.API.>, DURABLE.CREATE, MSG.GET, gestión de streams)`, malos.join("\n"));
  }

  // La forma exacta de un servicio, fijada a mano: si el generador cambia de forma, esto lo grita.
  const fact = json.find((s) => s.name === "facturacion-api");
  const esperado = [
    "$JS.API.INFO",
    "facturacion.>",
    "$JS.API.STREAM.INFO.EVT_FACTURACION",
    "$JS.API.STREAM.INFO.EVT_PEDIDOS",
    "$JS.API.STREAM.INFO.DLQ_PEDIDOS",
    ...["cancelado", "creado"].flatMap((e) => {
      const d = `facturacion-api__pedidos_pedido_v1_${e}`;
      return [
        `$JS.API.CONSUMER.INFO.EVT_PEDIDOS.${d}`,
        `$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.${d}`,
        `$JS.ACK.EVT_PEDIDOS.${d}.>`,
        `dlq.pedidos.pedido.v1.${e}`,
      ];
    }),
    "$JS.API.CONSUMER.INFO.DLQ_FACTURACION.facturacion-api__dlq_facturacion",
    "$JS.API.CONSUMER.MSG.NEXT.DLQ_FACTURACION.facturacion-api__dlq_facturacion",
    "$JS.ACK.DLQ_FACTURACION.facturacion-api__dlq_facturacion.>",
  ];
  afirmar(JSON.stringify(fact.publish) === JSON.stringify(esperado), "facturacion-api: lista de publicación EXACTA (pedidos.pedido.v1.> expandido a sus dos subjects)", fact.publish.join("\n"));
  afirmar(JSON.stringify(fact.consumes) === JSON.stringify(["pedidos.pedido.v1.cancelado", "pedidos.pedido.v1.creado"]), "`consumes` sale expandido a subjects exactos");

  // CONSUMER.CREATE: fuera por defecto (producción = flux provision), dentro con --dev, y solo del durable PROPIO.
  const esCreate = (x) => x.includes("CONSUMER.CREATE.");
  afirmar(json.every((x) => !x.publish.some(esCreate)), "por defecto NINGÚN servicio recibe CONSUMER.CREATE: los durables los crea flux provision");
  const dev = permisosJson("--dev");
  afirmar(
    dev.every((x, i) => {
      const crea = x.publish.filter(esCreate);
      const sinCrear = x.publish.filter((p) => !esCreate(p));
      return crea.length === x.durables.length && JSON.stringify(sinCrear) === JSON.stringify(json[i].publish) &&
        x.durables.every((d) => crea.includes(`$JS.API.CONSUMER.CREATE.${d.stream}.${d.durable}.${d.filter}`));
    }),
    "--dev añade EXACTAMENTE un CONSUMER.CREATE por durable propio (con su filtro) y nada más",
  );
  afirmar(
    [...json, ...dev].every((x) => x.publish.length > 0 && x.subscribe.length > 0),
    "ninguna lista de permisos sale vacía (en NATS `allow: []` significa «sin restricción», no «nada»)",
  );

  const sinMapa = gen(EJEMPLO);
  afirmar(sinMapa.codigo === 0 && /nkey: "<NKEY_DE_PEDIDOS_API>"/.test(sinMapa.salida), "sin --nkeys cada usuario lleva el placeholder <NKEY_DE_…>");
  afirmar(!/_INBOX\.>/.test(sinMapa.salida.replace(/^#.*$/gm, "")), "la config generada no contiene `_INBOX.>` en ningún permiso");

  // Errores: cada uno debe fallar con exit 1 y una pista.
  const base = { environment: "t", services: [{ name: "a-api", owns: ["pedidos"] }] };
  const consume = (c, extra = {}) => ({ ...base, services: [base.services[0], { name: "b-api", owns: ["otros"], consumes: c, ...extra }] });
  const casos = [
    ["un patrón sin coincidencias en el bundle", consume(["pedidos.pedido.v9.>"]), /no coincide con ningún subject/],
    ["un subject exacto que no existe en el bundle", consume(["pedidos.pedido.v1.inexistente"]), /no existe en el bundle[\s\S]*creado/],
    ["un comodín en medio del patrón", consume(["pedidos.*.v1.creado"]), /patrón no admitido/],
    ["un comodín en el dominio", consume(["*.pedido.v1.>"]), /comodín en el dominio/],
    ["consumir un dominio sin dueño", consume(["inventario.stock.v1.>"]), /nadie declara ser dueño/],
    ["dos dueños del mismo dominio", { ...base, services: [base.services[0], { name: "c-api", owns: ["pedidos"] }] }, /dos dueños/],
    ["un servicio que consume su propio dominio", { ...base, services: [{ name: "a-api", owns: ["pedidos"], consumes: ["pedidos.pedido.v1.>"] }] }, /su propio dominio/],
    ["un nombre de servicio repetido", { ...base, services: [base.services[0], { name: "a-api", owns: ["x"] }] }, /aparece dos veces/],
    ["`consumes` que no es una lista", consume("pedidos.pedido.v1.creado"), /debe ser una lista/],
  ];
  for (const [que, m, patron] of casos) {
    const r = gen(await manifiestoTemporal(m), "--check");
    afirmar(r.codigo === 1 && patron.test(r.errores), `rechaza ${que}`, `exit ${r.codigo}: ${r.errores.trim()}`);
  }
  // Y lo correcto pasa: un subject exacto y un patrón que sí coinciden.
  const bueno = gen(await manifiestoTemporal(consume(["pedidos.pedido.v1.creado"])), "--json");
  afirmar(
    bueno.codigo === 0 && JSON.parse(bueno.salida)[1].durables.length === 2,
    "acepta un subject exacto que existe: UN durable de eventos (más el de la DLQ propia)",
    bueno.errores,
  );

  // Dos subjects distintos que dan el mismo durable (los guiones pasan a `_`).
  const bundleFalso = path.join(TMP, "bundle-colision.json");
  await writeFile(bundleFalso, JSON.stringify({ subjects: { "a-b.c.v1.d": "x", "a.b-c.v1.d": "y" } }));
  const colision = gen(
    await manifiestoTemporal({ services: [{ name: "x", owns: ["a-b", "a"] }, { name: "y", owns: ["z"], consumes: ["a-b.c.v1.d", "a.b-c.v1.d"] }] }),
    "--bundle", bundleFalso, "--check",
  );
  afirmar(colision.codigo === 1 && /mismo durable/.test(colision.errores), "rechaza dos subjects que dan el mismo durable (y --bundle funciona)", colision.errores);

  // --nkeys
  const claves = Object.fromEntries(json.map((s) => [s.name, nuevaClave().pub]));
  const mapa = path.join(TMP, "nkeys-ok.json");
  await writeFile(mapa, JSON.stringify(claves));
  const conMapa = gen(EJEMPLO, "--nkeys", mapa);
  afirmar(
    conMapa.codigo === 0 && !/<NKEY_DE_/.test(conMapa.salida) && Object.values(claves).every((k) => conMapa.salida.includes(`nkey: "${k}"`)),
    "--nkeys sustituye cada placeholder por la clave pública del servicio",
  );
  const sinUna = path.join(TMP, "nkeys-incompleto.json");
  await writeFile(sinUna, JSON.stringify({ ...claves, "logistica-api": undefined }));
  const r1 = gen(EJEMPLO, "--nkeys", sinUna);
  afirmar(r1.codigo === 1 && /falta la clave de "logistica-api"/.test(r1.errores), "--nkeys: un servicio sin clave es un error");
  const conSemilla = path.join(TMP, "nkeys-semilla.json");
  const semilla = dec.decode(nuevaClave().seed);
  await writeFile(conSemilla, JSON.stringify({ ...claves, "pedidos-api": semilla }));
  const r2 = gen(EJEMPLO, "--nkeys", conSemilla);
  afirmar(
    r2.codigo === 1 && /SEMILLA/.test(r2.errores) && !r2.salida.includes(semilla) && !r2.errores.includes(semilla),
    "--nkeys: una SEMILLA se rechaza (es un secreto) y no se escribe en ninguna salida",
  );
  const repetida = path.join(TMP, "nkeys-repetida.json");
  await writeFile(repetida, JSON.stringify({ ...claves, "logistica-api": claves["pedidos-api"] }));
  const r3 = gen(EJEMPLO, "--nkeys", repetida);
  afirmar(r3.codigo === 1 && /comparten la misma clave/.test(r3.errores), "--nkeys: una clave repetida entre servicios (credencial compartida) es un error");
  const extra = path.join(TMP, "nkeys-extra.json");
  await writeFile(extra, JSON.stringify({ ...claves, "no-existe": claves["pedidos-api"] }));
  const r4 = gen(EJEMPLO, "--nkeys", extra, "--check");
  afirmar(r4.codigo === 0 && /aviso: --nkeys tiene una clave para "no-existe"/.test(r4.errores), "--nkeys: una clave de más es un aviso, no un error");

  const uso = gen();
  afirmar(uso.codigo === 2 && /uso:/.test(uso.errores), "sin argumentos: error de uso (exit 2)");
  const desconocida = gen(EJEMPLO, "--no-existe");
  afirmar(desconocida.codigo === 2, "una opción desconocida es un error de uso (exit 2)");

  // Modelo B: mismos permisos, otra account por tenant.
  const tenants = permisosJson("--tenants");
  afirmar(tenants.length === 8 && tenants.every((t) => ["acme", "globex"].includes(t.tenant)), "--tenants --json: un elemento por servicio y tenant (4 × 2)");
  afirmar(
    tenants.every((t) => {
      const o = json.find((s) => s.name === t.name);
      return JSON.stringify(o.publish) === JSON.stringify(t.publish) && JSON.stringify(o.subscribe) === JSON.stringify(t.subscribe);
    }),
    "--tenants: los permisos de cada servicio son IDÉNTICOS a los del Modelo A",
  );
  const tcfg = gen(EJEMPLO, "--tenants");
  afirmar(
    tcfg.codigo === 0 && /PRODUCCION_ACME: \{/.test(tcfg.salida) && /PRODUCCION_GLOBEX: \{/.test(tcfg.salida) && /<NKEY_DE_FACTURACION_API_ACME>/.test(tcfg.salida) && !/\n\s+"_INBOX\.>"/.test(tcfg.salida),
    "--tenants: una account por tenant, una clave por servicio y tenant, y sin _INBOX.> común",
  );

  return { json, claves, subjectsBundle };
}

// ─── Principal ───────────────────────────────────────────────────────────────

/** Las sondas de denegación. Devuelven `true` si lo PELIGROSO estuvo permitido. */
function sondasDe(nombre) {
  const ajenoDurable = "inventario-api__pedidos_pedido_v1_creado";
  const propioDurable = `${nombre}__pedidos_pedido_v1_creado`;
  const peticion = (subject, cuerpo) => async (nc) => (await pedir(nc, subject, cuerpo)).estado !== "denegado";
  const sinViolacion = (r) => r.estado === "respondido" && !r.error;
  return [
    ["publicar en un dominio ajeno (pedidos.pedido.v1.creado) por JetStream", async (nc) => {
      try {
        await publicarJs(nc, "pedidos.pedido.v1.creado");
        return true;
      } catch (e) {
        return esViolacion(e) ? false : `otro error: ${resumen(e)}`;
      }
    }],
    ["publicar en la DLQ de lo que NO consume (dlq.inventario.stock.v1.ajustado)", async (nc) => (await publicarCrudo(nc, "dlq.inventario.stock.v1.ajustado")) === "permitido"],
    ["crear su durable con un filtro más amplio (pedidos.>) por el subject concedido  [A-4]", async (nc) =>
      sinViolacion(await pedir(nc, `$JS.API.CONSUMER.CREATE.EVT_PEDIDOS.${propioDurable}.pedidos.pedido.v1.creado`, {
        stream_name: "EVT_PEDIDOS", config: { durable_name: propioDurable, filter_subject: "pedidos.>", ack_policy: "explicit" } }))],
    ["crear un consumidor sobre un stream no concedido (EVT_INVENTARIO, inventario.>)", async (nc) =>
      sinViolacion(await pedir(nc, `$JS.API.CONSUMER.CREATE.EVT_INVENTARIO.${nombre}__espia.inventario.>`, {
        stream_name: "EVT_INVENTARIO", config: { durable_name: `${nombre}__espia`, filter_subject: "inventario.>", ack_policy: "explicit" } }))],
    ["API antigua CONSUMER.DURABLE.CREATE (el filtro va solo en el cuerpo)  [A-5]", async (nc) =>
      (await pedir(nc, `$JS.API.CONSUMER.DURABLE.CREATE.EVT_INVENTARIO.${nombre}__espia2`, {
        stream_name: "EVT_INVENTARIO", config: { durable_name: `${nombre}__espia2`, filter_subject: "inventario.stock.v1.ajustado", ack_policy: "explicit" } })).estado !== "denegado"],
    ["leer un stream no concedido con STREAM.MSG.GET.EVT_INVENTARIO  [A-6]", async (nc) => !!(await pedir(nc, "$JS.API.STREAM.MSG.GET.EVT_INVENTARIO", { seq: 1 })).json?.message],
    ["leer el stream que SÍ consume con STREAM.MSG.GET.EVT_PEDIDOS", async (nc) => !!(await pedir(nc, "$JS.API.STREAM.MSG.GET.EVT_PEDIDOS", { seq: 1 })).json?.message],
    ["pedir mensajes del durable de OTRO servicio (CONSUMER.MSG.NEXT)  [A-8]", peticion(`$JS.API.CONSUMER.MSG.NEXT.EVT_PEDIDOS.${ajenoDurable}`, { batch: 1, expires: 300_000_000 })],
    ["consultar el durable de otro servicio (CONSUMER.INFO)", peticion(`$JS.API.CONSUMER.INFO.EVT_PEDIDOS.${ajenoDurable}`, {})],
    ["confirmar (ack) un mensaje del durable de otro servicio", async (nc) => (await publicarCrudo(nc, `$JS.ACK.EVT_PEDIDOS.${ajenoDurable}.1.1.1.1.0`)) === "permitido"],
    ["crear un durable sobre la DLQ de un dominio ajeno (DLQ_PEDIDOS)", async (nc) =>
      sinViolacion(await pedir(nc, `$JS.API.CONSUMER.CREATE.DLQ_PEDIDOS.${nombre}__dlq_pedidos.dlq.pedidos.>`, {
        stream_name: "DLQ_PEDIDOS", config: { durable_name: `${nombre}__dlq_pedidos`, filter_subject: "dlq.pedidos.>", ack_policy: "explicit" } }))],
    ["suscribirse a `_INBOX.>`  [A-7]", async (nc) => (await suscribirCrudo(nc, "_INBOX.>")) === "permitido"],
    ["suscribirse al inbox de otro servicio (_INBOX.inventario-api.>)  [A-7]", async (nc) => (await suscribirCrudo(nc, "_INBOX.inventario-api.>")) === "permitido"],
    ["suscribirse al dominio que consume (pedidos.>)", async (nc) => (await suscribirCrudo(nc, "pedidos.>")) === "permitido"],
    ["suscribirse a un dominio no concedido (inventario.>)", async (nc) => (await suscribirCrudo(nc, "inventario.>")) === "permitido"],
    ["suscribirse a las DLQ de todos (dlq.>)", async (nc) => (await suscribirCrudo(nc, "dlq.>")) === "permitido"],
    ...[
      ["crear un stream (STREAM.CREATE)", "$JS.API.STREAM.CREATE.EVT_ESPIA"],
      ["modificar un stream (STREAM.UPDATE)", "$JS.API.STREAM.UPDATE.EVT_NOEXISTE"],
      ["borrar un stream (STREAM.DELETE)", "$JS.API.STREAM.DELETE.EVT_NOEXISTE"],
      ["purgar un stream (STREAM.PURGE)", "$JS.API.STREAM.PURGE.EVT_NOEXISTE"],
      ["borrar un consumidor (CONSUMER.DELETE)", "$JS.API.CONSUMER.DELETE.EVT_NOEXISTE.x"],
      ["consultar un stream no concedido (STREAM.INFO.EVT_INVENTARIO)", "$JS.API.STREAM.INFO.EVT_INVENTARIO"],
      ["listar streams (STREAM.NAMES)", "$JS.API.STREAM.NAMES"],
      ["listar los consumidores de un stream (CONSUMER.NAMES)", "$JS.API.CONSUMER.NAMES.EVT_PEDIDOS"],
      ["lectura directa del stream (DIRECT.GET)", "$JS.API.DIRECT.GET.EVT_PEDIDOS"],
    ].map(([que, subj]) => [que, peticion(subj, {})]),
  ];
}

async function main() {
  const v = spawnSync(NATS_SERVER, ["--version"], { encoding: "utf8" });
  if (v.error || v.status !== 0) {
    console.error(`✗ no se encontró nats-server ("${NATS_SERVER}"). Define NATS_SERVER con su ruta.\n  Sin Docker: GOBIN=$PWD/bin go install github.com/nats-io/nats-server/v2@v2.14.5`);
    process.exit(2);
  }
  TMP = await banco.iniciar();
  console.log(`verify-acl — las ACLs generadas, contra un nats-server real`);
  console.log(`  ${v.stdout.trim()} · ${NATS_SERVER} · temporales en ${TMP}`);

  const bundle = JSON.parse(await readFile(path.join(ROOT, "schemas", "bundle.json"), "utf8"));
  const { json, claves } = await seccionGenerador(Object.keys(bundle.subjects));

  // ── Servidor A: el ejemplo, con la ACL generada y --nkeys ──────────────────
  const pares = Object.fromEntries(json.map((s) => [s.name, nuevaClave()]));
  const ops = nuevaClave();
  const mapaA = path.join(TMP, "nkeys-A.json");
  await writeFile(mapaA, JSON.stringify(Object.fromEntries(Object.entries(pares).map(([n, k]) => [n, k.pub]))));
  const generada = gen(EJEMPLO, "--nkeys", mapaA).salida;
  const credsOps = path.join(TMP, "ops.creds");
  await writeFile(credsOps, credsDe(ops.seed));

  // Usuarios EXTRA, solo para las secciones 5 y 6, con permisos renderizados desde el --json:
  //   · una variante por permiso de facturacion-api, SIN ese permiso (minimalidad);
  //   · dos mutantes: inbox común, y `$JS.API.>` «para arreglarlo».
  const fact = json.find((s) => s.name === "facturacion-api");
  const variantes = fact.publish.map((p, i) => ({ quitado: p, ...nuevaClave(), nombre: `fact-sin-${i}` }));
  const sinInbox = { quitado: `inbox: ${fact.subscribe[0]}`, ...nuevaClave(), nombre: "fact-sin-inbox" };
  const factDev = permisosJson("--dev").find((x) => x.name === "facturacion-api");
  const devOk = { ...nuevaClave(), nombre: "fact-dev" };
  // Con --dev, quitar CADA CREATE no rompe el ciclo (el SDK cae a INFO): se comprueba que es la única holgura.
  const devSinCreate = factDev.publish.filter((x) => x.includes("CONSUMER.CREATE.")).map((q, i) => ({ quitado: q, ...nuevaClave(), nombre: `fact-dev-sin-create-${i}` }));
  const mutanteInbox = { ...nuevaClave(), nombre: "fact-mutante-inbox" };
  const mutanteApi = { ...nuevaClave(), nombre: "logi-mutante-api" };
  const logi = json.find((s) => s.name === "logistica-api");
  const extra =
    variantes.map((x) => usuarioConf(x.pub, fact.publish.filter((p) => p !== x.quitado), fact.subscribe)).join("") +
    usuarioConf(sinInbox.pub, fact.publish, ["_INBOX.nadie.>"]) +
    usuarioConf(devOk.pub, factDev.publish, factDev.subscribe) +
    devSinCreate.map((x) => usuarioConf(x.pub, factDev.publish.filter((p) => p !== x.quitado), factDev.subscribe)).join("") +
    usuarioConf(mutanteInbox.pub, fact.publish, ["_INBOX.>"]) +
    usuarioConf(mutanteApi.pub, [...logi.publish, "$JS.API.>"], logi.subscribe) +
    `      { nkey: "${ops.pub}" },\n`;

  seccion("2. flux provision");
  let A;
  try {
    A = await arrancar("A", conUsuarios(generada, extra));
    afirmar(true, `nats-server carga la config generada con include (puerto ${A.port}); ${json.length} servicios + ops + ${variantes.length + devSinCreate.length + 4} usuarios de prueba`);
  } catch (e) {
    afirmar(false, "nats-server carga la config generada con include", e.message);
    return;
  }

  const opsA = await conectarOps(A.url, ops.seed);
  const provision = (...args) => {
    const r = spawnSync(process.execPath, [CLI, "provision", ...args, "--server", A.url, "--creds", credsOps], {
      encoding: "utf8", cwd: ROOT, env: { ...process.env, NO_COLOR: "1" },
    });
    return { codigo: r.status, salida: r.stdout, errores: r.stderr };
  };
  const nombresStreams = async () => {
    const out = [];
    for await (const s of opsA.jsm.streams.list()) out.push(s.config.name);
    return out.sort();
  };

  const dry = provision(EJEMPLO, "--dry-run");
  afirmar(dry.codigo === 0 && /20 se crearía/.test(dry.salida), "--dry-run cuenta lo que crearía (8 streams y 12 durables) y sale con 0", dry.salida + dry.errores);
  afirmar((await nombresStreams()).length === 0, "--dry-run NO crea nada: el broker sigue vacío");

  const crea = provision(EJEMPLO);
  afirmar(crea.codigo === 0 && /20 creado/.test(crea.salida), "provision crea 8 streams y 12 durables", crea.salida + crea.errores);
  const esperadosStreams = [...new Set(json.flatMap((s) => s.owns))].flatMap((d) => [streamName(d), dlqStreamName(d)]).sort();
  afirmar(JSON.stringify(await nombresStreams()) === JSON.stringify(esperadosStreams), `crea exactamente los streams EVT_/DLQ_ de cada dominio: ${esperadosStreams.join(", ")}`);
  let faltan = [];
  for (const s of json) {
    for (const d of s.durables) {
      const i = await opsA.jsm.consumers.info(d.stream, d.durable).catch(() => null);
      if (!i || i.config.filter_subject !== d.filter) faltan.push(`${d.stream}/${d.durable}`);
    }
  }
  afirmar(faltan.length === 0, "cada durable que el generador concede existe, con su filtro exacto", faltan.join(", "));
  const infoPedidos = await opsA.jsm.streams.info("EVT_PEDIDOS");
  afirmar(infoPedidos.config.max_age === nanos(30 * 86_400_000) && infoPedidos.config.duplicate_window === nanos(120_000) && infoPedidos.config.discard === "old" && infoPedidos.config.storage === "file", "el stream de eventos lleva la config de 02-naming.md §3.3 (file, 30 d, descarta lo viejo, ventana 2 m)");
  const infoDlq = await opsA.jsm.streams.info("DLQ_PEDIDOS");
  afirmar(infoDlq.config.max_age === nanos(90 * 86_400_000) && infoDlq.config.subjects[0] === "dlq.pedidos.>", "la DLQ lleva 90 d y captura dlq.<dominio>.>");

  const antes = JSON.stringify([(await opsA.jsm.streams.info("EVT_PEDIDOS")).created, (await opsA.jsm.consumers.info("EVT_PEDIDOS", "facturacion-api__pedidos_pedido_v1_creado")).created]);
  const repite = provision(EJEMPLO);
  const despues = JSON.stringify([(await opsA.jsm.streams.info("EVT_PEDIDOS")).created, (await opsA.jsm.consumers.info("EVT_PEDIDOS", "facturacion-api__pedidos_pedido_v1_creado")).created]);
  afirmar(repite.codigo === 0 && /20 sin cambios/.test(repite.salida) && /Sin cambios/.test(repite.salida), "repetir es IDEMPOTENTE: «20 sin cambios», exit 0", repite.salida);
  afirmar(antes === despues, "…y no recrea nada (las marcas de creación no cambian)");

  // Una diferencia sembrada: se informa, no se sobrescribe, y la salida es distinta de 0.
  await opsA.jsm.streams.update("EVT_PEDIDOS", { max_age: nanos(7 * 86_400_000) });
  await opsA.jsm.consumers.update("EVT_PEDIDOS", "facturacion-api__pedidos_pedido_v1_creado", { max_ack_pending: 10 });
  const dif = provision(EJEMPLO);
  afirmar(dif.codigo === 1 && /DIFIERE\s+EVT_PEDIDOS\s*\n\s+max_age: existente 7d, deseado 30d/.test(dif.salida), "una diferencia sembrada en un STREAM se informa (max_age: existente 7d, deseado 30d) y la salida es 1", dif.salida);
  afirmar(/DIFIERE\s+EVT_PEDIDOS \/ facturacion-api__pedidos_pedido_v1_creado\s*\n\s+max_ack_pending: existente 10, deseado 256/.test(dif.salida), "…y en un CONSUMIDOR (max_ack_pending: existente 10, deseado 256)");
  afirmar(
    (await opsA.jsm.streams.info("EVT_PEDIDOS")).config.max_age === nanos(7 * 86_400_000) &&
      (await opsA.jsm.consumers.info("EVT_PEDIDOS", "facturacion-api__pedidos_pedido_v1_creado")).config.max_ack_pending === 10,
    "…y NO sobrescribe: ambos siguen con el valor sembrado",
  );
  await opsA.jsm.streams.update("EVT_PEDIDOS", { max_age: nanos(30 * 86_400_000) });
  await opsA.jsm.consumers.update("EVT_PEDIDOS", "facturacion-api__pedidos_pedido_v1_creado", { max_ack_pending: 256 });
  const vuelta = provision(EJEMPLO);
  afirmar(vuelta.codigo === 0 && /Sin cambios/.test(vuelta.salida), "restituidos los valores, vuelve a «sin cambios»: la comparación no da falsos positivos");

  // Nunca borra: lo que el manifiesto no menciona se queda.
  await opsA.jsm.streams.add({ name: "EVT_AJENO", subjects: ["ajeno.>"] });
  await opsA.jsm.consumers.add("EVT_PEDIDOS", { durable_name: "legado", ack_policy: "explicit" });
  const ignora = provision(EJEMPLO);
  afirmar(
    ignora.codigo === 0 && (await nombresStreams()).includes("EVT_AJENO") && (await opsA.jsm.consumers.info("EVT_PEDIDOS", "legado").then(() => true, () => false)),
    "nunca borra: un stream y un consumidor que el manifiesto no menciona siguen ahí",
  );
  const mal = provision(path.join(TMP, "no-existe.json"));
  afirmar(mal.codigo === 2, "un manifiesto ilegible sale con 2");
  const conServicio = spawnSync(process.execPath, [CLI, "provision", EJEMPLO, "--server", A.url], { encoding: "utf8", cwd: ROOT, env: { ...process.env, NO_COLOR: "1" } });
  afirmar(conServicio.status !== 0, "sin credenciales de operación, provision no hace nada (el servidor las exige)");

  // ── 3. El ciclo de cada servicio ───────────────────────────────────────────
  seccion("3. El ciclo completo de cada servicio, con sus permisos generados");
  const subjectPropio = (s) => Object.keys(bundle.subjects).find((x) => domainOf(x) === s.owns[0]) ?? `${s.owns[0]}.cosa.v1.ocurrida`;
  let ackVisto;
  for (const svc of json) {
    const nc = await comoServicio(A.url, svc.name, pares[svc.name].seed);
    const r = await ciclo({ nc, svc, ops: opsA, subjectPropio: subjectPropio(svc), nakTerm: true });
    console.log(`  ${svc.name}  (dueño de ${svc.owns.join(", ")}; consume ${svc.consumes.join(", ")})`);
    for (const p of r.pasos) afirmar(p.ok, `    ${p.nombre}`, p.ok ? undefined : p.detalle);
    if (svc.name === "facturacion-api") ackVisto = r.ack;
    await nc.close();
  }

  // CONSUMER.CREATE denegado por defecto: el `add` del SDK cae a `info` con un durable aprovisionado.
  {
    const nc = await comoServicio(A.url, "facturacion-api", pares["facturacion-api"].seed);
    const svcF = json.find((x) => x.name === "facturacion-api");
    const r = await ciclo({ nc, svc: svcF, ops: opsA, subjectPropio: subjectPropio(svcF) });
    afirmar(
      r.ok && r.pasos.filter((p) => p.creoViaAdd !== undefined).every((p) => p.creoViaAdd === false),
      "por defecto, consumers.add se DENIEGA y el ciclo sigue por consumers.info: con durables aprovisionados no hace falta CREATE",
      r.fallidos.join("\n"),
    );
    await nc.close();

    // El durable que falta: sin CREATE no se recrea solo. Esta es la señal de NOT_PROVISIONED para un SDK.
    const [stream, durable, filtro] = ["EVT_PEDIDOS", "facturacion-api__pedidos_pedido_v1_creado", "pedidos.pedido.v1.creado"];
    await opsA.jsm.consumers.delete(stream, durable).catch(() => {});
    const n2 = await comoServicio(A.url, "facturacion-api", pares["facturacion-api"].seed);
    const jsm2 = await jetstreamManager(n2, { timeout: 2000 });
    const add = await jsm2.consumers.add(stream, await configConsumidor(durable, filtro)).then(() => "creado", (e) => (esViolacion(e) ? "denegado" : `otro: ${resumen(e)}`));
    const info = await jsm2.consumers.info(stream, durable).then(() => "existe", (e) => (e?.status === 404 ? "no existe (404)" : `otro: ${resumen(e)}`));
    afirmar(add === "denegado" && info === "no existe (404)", `NOT_PROVISIONED: con el durable borrado, consumers.add → ${add} y consumers.info → ${info}; el servicio NO lo recrea con deliver_policy all`);
    await n2.close();
    // …y --dev sí: el SDK crea su durable (desarrollo, sin flux provision).
    const n3 = await comoServicio(A.url, "fact-dev", devOk.seed, "facturacion-api");
    const jsm3 = await jetstreamManager(n3, { timeout: 2000 });
    const creado = await jsm3.consumers.add(stream, await configConsumidor(durable, filtro)).then((c) => c.config.filter_subject, (e) => `error: ${resumen(e)}`);
    afirmar(creado === filtro, "con --dev, consumers.add SÍ crea el durable propio, con su filtro exacto");
    await n3.close();
  }

  const toks = (ackVisto ?? "").split(".");
  afirmar(
    toks.length === 9 && toks[0] === "$JS" && toks[1] === "ACK" && toks[2] === "EVT_PEDIDOS" && toks[3] === "facturacion-api__pedidos_pedido_v1_cancelado",
    "formato observado del subject de ack: $JS.ACK.<STREAM>.<durable>.<n>.<n>.<n>.<ts>.<n> (9 tokens). Si cambia, la ACL se rompe en alto",
    ackVisto,
  );

  // ── 4. Denegaciones ────────────────────────────────────────────────────────
  seccion("4. Lo que un servicio NO puede hacer (facturacion-api: dueño de facturacion, consume pedidos)");
  const sondas = sondasDe("facturacion-api");
  const factNc = await comoServicio(A.url, "facturacion-api", pares["facturacion-api"].seed);
  const resultadoSondas = new Map();
  for (const [que, fn] of sondas) {
    let permitido;
    try {
      permitido = await fn(factNc);
    } catch (e) {
      permitido = `error: ${resumen(e)}`;
    }
    resultadoSondas.set(que, permitido);
    afirmar(permitido === false, `denegado: ${que}`, permitido === false ? undefined : `PERMITIDO o inconcluso (${permitido})`);
  }
  await factNc.close();

  // Con --dev el subject de CREATE SÍ está concedido: el filtro más amplio lo rechaza el servidor (A-4).
  const devNc = await comoServicio(A.url, "fact-dev", devOk.seed, "facturacion-api");
  const [qA4, fnA4] = sondas.find(([q]) => q.includes("[A-4]"));
  const a4 = await fnA4(devNc);
  afirmar(a4 === false, `con --dev, ${qA4.replace("crear su durable", "crear el durable")} → rechazado por el SERVIDOR`, `resultado: ${a4}`);
  await devNc.close();

  const ctrl = await comoServicio(A.url, "facturacion-api", pares["facturacion-api"].seed);
  afirmar((await suscribirCrudo(ctrl, "_INBOX.facturacion-api.verificacion")) === "permitido", "control: SU inbox privado sí está permitido");
  afirmar((await publicarCrudo(ctrl, "dlq.pedidos.pedido.v1.creado")) === "permitido", "control: publicar en la DLQ de lo que SÍ consume está permitido");
  afirmar((await publicarCrudo(ctrl, "facturacion.factura.v2.emitida")) === "permitido", "control: publicar en su propio dominio está permitido");
  await ctrl.close();

  // El escenario de A-2, con la ACL nueva: un espía de _INBOX.> mientras otro servicio consume.
  const escenarioFuga = async (espia, quienLee, marca) => {
    const visto = [];
    const subs = ["_INBOX.>", "inventario.>"].map((s) => espia.subscribe(s));
    for (const s of subs) vigilar(s, marca, visto);
    await espia.flush();
    await sleep(300);
    const leido = await quienLee(marca);
    await sleep(700);
    return { leido, visto };
  };
  const secreto = `SECRETO-${randomUUID()}`;
  const espia = await comoServicio(A.url, "facturacion-api", pares["facturacion-api"].seed);
  const victima = await comoServicio(A.url, "pedidos-api", pares["pedidos-api"].seed);
  const fuga = await escenarioFuga(espia, (marca) => lecturaLegitima(opsA, victima, marca), secreto);
  afirmar(fuga.leido, "escenario A-2: pedidos-api SÍ consume el evento de inventario (hay tráfico que espiar)");
  afirmar(fuga.visto.length === 0, "escenario A-2: facturacion-api, espiando `_INBOX.>` e `inventario.>`, NO ve nada", fuga.visto.join(", "));
  await espia.close();
  await victima.close();

  // ── 5. Minimalidad ─────────────────────────────────────────────────────────
  seccion("5. Minimalidad: sin CUALQUIERA de los permisos de facturacion-api, su ciclo se rompe");
  const svcFact = fact;
  const propioFact = subjectPropio(svcFact);
  for (const x of [...variantes, sinInbox]) {
    const nc = await comoServicio(A.url, x.nombre, x.seed, "facturacion-api");
    const r = await ciclo({ nc, svc: svcFact, ops: opsA, subjectPropio: propioFact });
    await nc.close();
    {
      const lista = r.fallidos;
      const corto = lista.length > 3 ? `${lista.slice(0, 3).join("; ")} (+${lista.length - 3} más)` : lista.join("; ");
      afirmar(!r.ok, `sin ${x.quitado} → el ciclo falla en: ${corto || "(nada: el permiso sobra)"}`);
    }
  }

  nota("con --dev: CONSUMER.CREATE es la ÚNICA holgura; quitar cualquiera de los tres deja el ciclo en pie");
  for (const x of devSinCreate) {
    const nc = await comoServicio(A.url, x.nombre, x.seed, "facturacion-api");
    const r = await ciclo({ nc, svc: svcFact, ops: opsA, subjectPropio: propioFact });
    await nc.close();
    afirmar(
      r.ok && r.pasos.some((p) => p.creoViaAdd === false),
      `--dev sin ${x.quitado} → el ciclo SIGUE (el SDK cae a CONSUMER.INFO)`,
      r.fallidos.join("\n"),
    );
  }

  // ── 6. Las sondas pueden fallar ────────────────────────────────────────────
  seccion("6. Las comprobaciones DETECTAN una ACL rota: dos mutantes sembrados");
  const mFuga = async (nc) => {
    const v2 = await comoServicio(A.url, "pedidos-api", pares["pedidos-api"].seed);
    const marca = `SECRETO-${randomUUID()}`;
    const r = await escenarioFuga(nc, (m) => lecturaLegitima(opsA, v2, m), marca);
    await v2.close();
    return r;
  };
  const mi = await comoServicio(A.url, "fact-mutante-inbox", mutanteInbox.seed, "facturacion-api");
  afirmar((await suscribirCrudo(mi, "_INBOX.>")) === "permitido", "mutante 1 (subscribe `_INBOX.>`): la sonda «suscribirse a _INBOX.>» lo detecta");
  const fugaMutante = await mFuga(mi);
  afirmar(fugaMutante.leido && fugaMutante.visto.length > 0, "mutante 1: el escenario A-2 REPRODUCE la fuga (el espía lee el evento de inventario)", JSON.stringify(fugaMutante));
  await mi.close();

  const mp = await comoServicio(A.url, "logi-mutante-api", mutanteApi.seed, "logistica-api");
  const sondasLogi = sondasDe("logistica-api");
  const debenFlip = [
    "API antigua CONSUMER.DURABLE.CREATE", "STREAM.MSG.GET.EVT_INVENTARIO", "crear un consumidor sobre un stream no concedido",
    "pedir mensajes del durable de OTRO servicio", "crear un stream (STREAM.CREATE)", "borrar un stream (STREAM.DELETE)", "listar streams (STREAM.NAMES)",
  ];
  const noDebenFlip = ["publicar en un dominio ajeno", "suscribirse a `_INBOX.>`", "suscribirse a un dominio no concedido"];
  for (const [que, fn] of sondasLogi) {
    const esperadoFlip = debenFlip.some((d) => que.includes(d));
    const noFlip = noDebenFlip.some((d) => que.includes(d));
    if (!esperadoFlip && !noFlip) continue;
    let permitido;
    try { permitido = await fn(mp); } catch (e) { permitido = `error: ${resumen(e)}`; }
    afirmar(
      esperadoFlip ? permitido === true : permitido === false,
      esperadoFlip ? `mutante 2 (+ \`$JS.API.>\`): la sonda «${que}» lo DETECTA (permitido)` : `mutante 2: «${que}» sigue denegado (\`$JS.API.>\` no abre lo que no es de $JS.API)`,
      `resultado: ${permitido}`,
    );
  }
  await mp.close();
  // Los mutantes han podido crear cosas: se barren, y se comprueba que el resto no se vio afectado.
  for (const n of ["logi-mutante-api__espia", "logi-mutante-api__espia2", "logistica-api__espia", "logistica-api__espia2", "logistica-api__dlq_pedidos", "facturacion-api__espia", "facturacion-api__espia2"]) {
    for (const s of ["EVT_INVENTARIO", "DLQ_PEDIDOS"]) await opsA.jsm.consumers.delete(s, n).catch(() => {});
  }

  await opsA.nc.close();
  await parar(A.child);

  await seccionRegresion(json, pares);
  await seccionTenants(json);
}

// ─── Regresión: la ACL v1 congelada ──────────────────────────────────────────

async function seccionRegresion(json) {
  seccion("7. Regresión: la ACL v1 congelada (conformance/fixtures/acl-v1.conf) sigue rota como se documenta");
  const fixture = await readFile(path.join(ROOT, "conformance", "fixtures", "acl-v1.conf"), "utf8");
  const ops = { user: "ops", pass: "pw" };
  let B;
  try {
    B = await arrancar("B", conUsuarios(fixture, `      { user: "ops", password: "pw" },\n`));
  } catch (e) {
    afirmar(false, "nats-server carga el fixture v1", e.message);
    return;
  }
  const nc0 = await conectar(B.url, { nombre: "ops", ...ops });
  const opsB = { nc: nc0, jsm: await jetstreamManager(nc0), js: jetstream(nc0) };
  for (const [name, subjects] of [["EVT_PEDIDOS", ["pedidos.>"]], ["DLQ_PEDIDOS", ["dlq.pedidos.>"]], ["EVT_INVENTARIO", ["inventario.>"]], ["EVT_FACTURACION", ["facturacion.>"]], ["DLQ_FACTURACION", ["dlq.facturacion.>"]]]) {
    await opsB.jsm.streams.add({ name, subjects });
  }

  const svc = json.find((s) => s.name === "facturacion-api");
  const nc = await conectar(B.url, { nombre: "facturacion-api", user: "facturacion-api", pass: "pw" });
  const r = await ciclo({ nc, svc, ops: opsB, subjectPropio: "facturacion.factura.v2.emitida" });
  afirmar(!r.ok, "H1: con la ACL v1, el MISMO ciclo que pasa con la v2 FALLA");
  afirmar(
    r.fallidos.some((f) => f.startsWith("connect()")) && r.fallidos.some((f) => f.includes("pedir mensajes")),
    "H1: …y falla donde el plan dice: no puede consultar la cuenta JetStream ni consumir",
    r.fallidos.join("\n"),
  );
  await nc.close();

  // H2: el espía de A-2. Lee ops en nombre del consumidor legítimo (con la v1 nadie más puede).
  const marca = `SECRETO-${randomUUID()}`;
  const espia = await conectar(B.url, { nombre: "espia", user: "facturacion-api", pass: "pw" });
  const visto = [];
  vigilar(espia.subscribe("_INBOX.>"), marca, visto);
  await espia.flush();
  await opsB.jsm.consumers.add("EVT_INVENTARIO", { durable_name: "pedidos-api__inventario_stock_v1_ajustado", filter_subject: "inventario.stock.v1.ajustado", ack_policy: "explicit" });
  await opsB.js.publish("inventario.stock.v1.ajustado", enc.encode(JSON.stringify({ sku: marca })));
  const c = await jetstream(opsB.nc).consumers.get("EVT_INVENTARIO", "pedidos-api__inventario_stock_v1_ajustado");
  const m = await recibirUno(c, 2500);
  m?.ack();
  await sleep(700);
  afirmar(!!m && visto.length > 0, "H2: con la ACL v1, facturacion-api (sin concesión sobre inventario) LEE el evento por `_INBOX.>`", `consumidor legítimo: ${!!m}, espía: ${visto.length}`);
  await espia.close();
  await opsB.nc.close();
  await parar(B.child);
}

// ─── Modelo B ────────────────────────────────────────────────────────────────

async function seccionTenants(json) {
  seccion("8. Modelo B (--tenants): una account por tenant, mismos permisos, aislamiento real");
  const tenants = ["acme", "globex"];
  const ids = tenants.flatMap((t) => json.map((s) => `${s.name}@${t}`));
  const pares = Object.fromEntries(ids.map((id) => [id, nuevaClave()]));
  const opsPorTenant = Object.fromEntries(tenants.map((t) => [t, nuevaClave()]));
  const mapa = path.join(TMP, "nkeys-B.json");
  await writeFile(mapa, JSON.stringify(Object.fromEntries(Object.entries(pares).map(([k, v]) => [k, v.pub]))));
  const g = gen(EJEMPLO, "--tenants", "--nkeys", mapa);
  if (!afirmar(g.codigo === 0, "--tenants --nkeys genera la config", g.errores)) return;
  // Cada account necesita SU usuario de operación (las nkeys no se pueden repetir entre accounts).
  const marcaUsers = "    users: [";
  const partes = g.salida.split(marcaUsers);
  const acl = partes[0] + tenants.map((t, i) => `${marcaUsers}\n      { nkey: "${opsPorTenant[t].pub}" },\n${partes[i + 1]}`).join("");
  let D;
  try {
    D = await arrancar("D", acl);
    afirmar(true, "nats-server carga la config del Modelo B (2 accounts)");
  } catch (e) {
    afirmar(false, "nats-server carga la config del Modelo B", e.message);
    return;
  }

  const provisionar = (t) => {
    const f = path.join(TMP, `ops-${t}.creds`);
    return writeFile(f, credsDe(opsPorTenant[t].seed)).then(() => {
      const r = spawnSync(process.execPath, [CLI, "provision", EJEMPLO, "--server", D.url, "--creds", f], {
        encoding: "utf8", cwd: ROOT, env: { ...process.env, NO_COLOR: "1" },
      });
      return r;
    });
  };
  for (const t of tenants) {
    const r = await provisionar(t);
    afirmar(r.status === 0 && /20 creado/.test(r.stdout), `provision en la account de ${t} (una vez por account)`, r.stdout + r.stderr);
  }

  const opsAcme = await conectarOps(D.url, opsPorTenant.acme.seed);
  const svc = json.find((s) => s.name === "facturacion-api");
  const nc = await comoServicio(D.url, "facturacion-api@acme", pares["facturacion-api@acme"].seed, "facturacion-api");
  const r = await ciclo({ nc, svc, ops: opsAcme, subjectPropio: "facturacion.factura.v2.emitida" });
  afirmar(r.ok, "facturacion-api@acme completa el ciclo con los permisos del Modelo B", r.fallidos.join("\n"));
  await nc.close();

  // Aislamiento: lo publicado en acme no existe en globex, aunque el servicio sea el mismo.
  const marca = `ACME-${randomUUID()}`;
  const pedidosAcme = await comoServicio(D.url, "pedidos-api@acme", pares["pedidos-api@acme"].seed, "pedidos-api");
  await jetstream(pedidosAcme).publish("pedidos.pedido.v1.creado", enc.encode(JSON.stringify({ marca })));
  await pedidosAcme.close();
  const lector = async (t) => {
    const n = await comoServicio(D.url, `facturacion-api@${t}`, pares[`facturacion-api@${t}`].seed, "facturacion-api");
    const c = await jetstream(n).consumers.get("EVT_PEDIDOS", "facturacion-api__pedidos_pedido_v1_creado");
    let visto = false;
    for (let i = 0; i < 6; i++) {
      const m = await recibirUno(c, 1200);
      if (!m) break;
      m.ack();
      if (dec.decode(m.data).includes(marca)) visto = true;
    }
    await n.close();
    return visto;
  };
  afirmar(await lector("acme"), "lo publicado en acme lo lee facturacion-api@acme");
  afirmar(!(await lector("globex")), "…y facturacion-api@globex NO lo ve: la account es un universo aparte");
  const globex = await conectarOps(D.url, opsPorTenant.globex.seed);
  afirmar((await globex.jsm.streams.info("EVT_PEDIDOS")).state.messages === 0, "el stream EVT_PEDIDOS de globex está vacío: no comparte nada con el de acme");
  await globex.nc.close();
  await opsAcme.nc.close();
  await parar(D.child);
}

// ─── Arranque y limpieza ─────────────────────────────────────────────────────

banco.instalarManejadores((e) => console.log(`\n  ✗ error no controlado: ${e?.stack ?? e}`));

try {
  await main();
} catch (e) {
  R.fallos++;
  console.log(`\n  ✗ la verificación se interrumpió: ${e?.stack ?? e}`);
} finally {
  await banco.limpiar();
}

console.log(`\n${R.fallos === 0 ? "✓" : "✗"} ${R.total} comprobaciones, ${R.fallos} fallo(s)`);
process.exit(R.fallos === 0 ? 0 : 1);
