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
 * Petición/respuesta (11-request-reply.md; permisos en 06-security.md §3.9), en las mismas secciones: 1c (el generador:
 * la forma exacta de quien llama y del dueño, lo que rechaza el manifiesto), 3 (cada servicio que llama o atiende
 * ejecuta su ciclo), 4c (una MATRIZ de sondas × servicios: quién puede llamar, suscribirse o publicar sobre inboxes),
 * 5c (sin la suscripción o sin `allow_responses`, el dueño se rompe), 6c (mutantes sembrados: `rpc.<dominio>.>`
 * a quien llama, `rpc.>` o un permiso de publicación sobre inboxes al dueño) y 8 (cada account tiene SU espacio `rpc.`).
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
import { readFileSync, writeFileSync } from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";

import {
  dlqDurableName,
  dlqStateDurableName,
  dlqStreamName,
  domainOf,
  durableName,
  stateStreamName,
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
 * La petición/respuesta de un servicio con sus permisos generados, como la haría un SDK `rpc` (11-request-reply.md §4 y
 * §5): quien llama publica en el subject EXACTO de la operación y recibe la respuesta por su inbox privado; el dueño se
 * suscribe al subject exacto, con su servicio como grupo de cola, y responde con `allow_responses` (sin ningún permiso de
 * publicación sobre inboxes). `ops`, sin restricciones, hace de la otra parte.
 *
 * Se comprueban EFECTOS, no la ausencia de error: una respuesta denegada no lanza nada en el dueño (el broker avisa por su
 * conexión) y para quien llama solo es un timeout, así que el paso del dueño mira las dos cosas.
 */
async function cicloRpc({ nc, svc, ops, paso }) {
  for (const op of svc.calls ?? []) {
    await paso(`request(${op}): publicar en el subject EXACTO rpc.${op} y recibir la respuesta por el inbox privado`, async () => {
      const resp = ops.nc.subscribe(`rpc.${op}`, { queue: "verify-acl" });
      (async () => {
        try {
          for await (const m of resp) m.respond(enc.encode('{"ok":true}'));
        } catch {
          /* suscripción denegada: el iterador lanza (A-8f) */
        }
      })();
      await ops.nc.flush();
      try {
        const r = await nc.request(`rpc.${op}`, enc.encode("{}"), { timeout: 2000 });
        if (!JSON.parse(dec.decode(r.data)).ok) throw new Error("respuesta inesperada");
      } finally {
        resp.unsubscribe();
        await ops.nc.flush();
      }
    });
  }
  for (const op of svc.serves ?? []) {
    await paso(`respond(${op}): suscribirse al subject EXACTO rpc.${op} (grupo de cola ${svc.name}) y responder con allow_responses`, async () => {
      const antes = nc.violaciones.length;
      const sub = nc.subscribe(`rpc.${op}`, { queue: svc.name });
      (async () => {
        try {
          for await (const m of sub) m.respond(enc.encode('{"ok":true}'));
        } catch {
          /* suscripción denegada: el iterador lanza (A-8f) y el paso lo ve en el aviso de la conexión */
        }
      })();
      await nc.flush();
      await sleep(150);
      try {
        const den = nc.violaciones.slice(antes).find((v) => v.includes("Subscription"));
        if (den) throw new Error(den);
        const r = await ops.nc.request(`rpc.${op}`, enc.encode("{}"), { timeout: 1500 });
        if (!JSON.parse(dec.decode(r.data)).ok) throw new Error("respuesta inesperada");
        await sleep(150);
        const v = nc.violaciones.slice(antes);
        if (v.length) throw new Error(v[0]);
      } finally {
        sub.unsubscribe();
        await nc.flush();
      }
    });
  }
}

/**
 * Lo que hace un servicio con el SDK de Node, paso a paso (sdk-node/src/client.ts). Cada
 * paso es independiente: un permiso que falta rompe SU paso y no oculta los demás.
 *
 * `ops` prepara el terreno (mensajes sembrados, durables limpios) y comprueba EFECTOS, no
 * solo ausencia de error: un ack denegado no falla en el cliente, deja el mensaje pendiente.
 */
async function ciclo({ nc, svc, ops, subjectPropio, nakTerm = false, rpc = true }) {
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

  // Petición/respuesta, si el servicio llama a alguna operación o atiende alguna. Las variantes que miden los permisos de
  // EVENTOS del dueño (sección 5b) la omiten: con `allow_responses` el `reply` de cada entrega —el subject de ack— admite
  // UNA publicación, y su ack pasa aunque falte `$JS.ACK…` (A-109).
  if (rpc) await cicloRpc({ nc, svc, ops, paso });

  return { pasos, ok: pasos.every((p) => p.ok), fallidos: pasos.filter((p) => !p.ok).map((p) => p.nombre), ack: primerAck };
}

// ─── El generador (sin servidor) ─────────────────────────────────────────────

let nManifiesto = 0;
async function manifiestoTemporal(obj) {
  const f = path.join(TMP, `manifiesto-${++nManifiesto}.json`);
  await writeFile(f, JSON.stringify(obj));
  return f;
}
function manifiestoTemporalSync(obj) {
  const f = path.join(TMP, `manifiesto-${++nManifiesto}.json`);
  writeFileSync(f, JSON.stringify(obj));
  return f;
}
/** Un bundle con los eventos de ejemplo y NINGÚN contrato de estado: el de antes de 1.1. */
function bundleSinEstadoSync() {
  const f = path.join(TMP, "bundle-sin-estado.json");
  const b = JSON.parse(readFileSync(path.join(ROOT, "schemas", "bundle.json"), "utf8"));
  delete b.state;
  writeFileSync(f, JSON.stringify(b));
  return f;
}


async function seccionGenerador(subjectsBundle) {
  seccion("1. El generador");

  const json = permisosJson();
  const nDurables = json.reduce((n, s) => n + s.durables.length, 0);
  const check = gen(EJEMPLO, "--check");
  afirmar(check.codigo === 0 && new RegExp(`${nDurables} durable`).test(check.salida), `--check valida el manifiesto de ejemplo y cuenta ${nDurables} durables`, check.salida + check.errores);

  const nServicios = JSON.parse(await readFile(EJEMPLO, "utf8")).services.length;
  afirmar(json.length === nServicios, `--json devuelve un elemento por servicio (${nServicios})`);

  // La lista cerrada de lo concedible y la de lo denegable salen de protocol.json: una sola
  // fuente, que check-protocol-json prueba en los dos sentidos.
  const proto = JSON.parse(await readFile(path.join(ROOT, "protocol.json"), "utf8"));
  const acl = proto.security.acl;
  // La lista cerrada es la de eventos MÁS la de estado (protocol.json → state.acl): un permiso que no
  // encaje en ninguna es un error del generador. Y lo denegable lo es salvo las dos formas de
  // DIRECT.GET de estado (`security.acl.neverGrantedExceptions`: se miden en las secciones de estado).
  const patronesEstado = Object.entries(proto.state.acl).filter(([k, v]) => typeof v === "string" && !k.startsWith("$") && k !== "observerStreamInfoState");
  // …y la de petición/respuesta (protocol.json → rpc.acl): quien llama publica en el subject EXACTO de cada operación.
  const patronesRpc = [["callerPublish", proto.rpc.acl.callerPublish]];
  const permitidos = [...Object.entries(acl.permissionPatterns), ...patronesEstado, ...patronesRpc].filter(([k]) => !k.startsWith("$")).map(([, v]) => new RegExp(v));
  const excepciones = (acl.neverGrantedExceptions ?? []).map((ruta) => new RegExp(ruta.split(".").reduce((o, k) => o?.[k], proto)));
  const peligroso = { test: (x) => new RegExp(acl.neverGrantedPattern).test(x) && !excepciones.some((re) => re.test(x)) };
  for (const s of json) {
    // Su inbox privado y, solo si atiende operaciones, `rpc.<dominio>.>` de cada dominio servido: ningún subject de evento.
    const dominiosRpc = [...new Set(s.serves.map(domainOf))].sort();
    afirmar(
      JSON.stringify(s.subscribe) === JSON.stringify([`_INBOX.${s.name}.>`, ...dominiosRpc.map((d) => `rpc.${d}.>`)]),
      `${s.name}: SOLO se suscribe a su inbox privado${dominiosRpc.length ? ` y a rpc.${dominiosRpc.join(".>, rpc.")}.> (atiende operaciones)` : ""} (ningún subject de dominio, ningún _INBOX.>)`,
      JSON.stringify(s.subscribe),
    );
    const malos = s.publish.filter((p) => peligroso.test(p) || !permitidos.some((re) => re.test(p)));
    afirmar(malos.length === 0, `${s.name}: ningún permiso de publicación fuera de la lista cerrada (nada de $JS.API.>, DURABLE.CREATE, MSG.GET, la forma de cuerpo de DIRECT.GET, STREAM.INFO.STATE_*, gestión de streams)`, malos.join("\n"));
    afirmar(!s.publish.some((p) => /^\$JS\.API\.STREAM\.INFO\.STATE_/.test(p)), `${s.name}: NINGÚN permiso STREAM.INFO.STATE_<DOM>: lista todas las claves y todos los tenants (A-39)`);
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
    // Quien llama: el subject EXACTO de cada operación de su `calls`, y nada más (06-security.md §3.9).
    "rpc.clientes.cliente.v1.calcular-descuento",
    "rpc.clientes.cliente.v1.obtener",
  ];
  afirmar(JSON.stringify(fact.publish) === JSON.stringify(esperado), "facturacion-api: lista de publicación EXACTA (pedidos.pedido.v1.> expandido a sus dos subjects, y un subject rpc EXACTO por operación que llama)", fact.publish.join("\n"));
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

  seccionGeneradorEstado(json, dev);
  await seccionGeneradorRpc(json, dev, proto);

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
  afirmar(tenants.length === 2 * json.length && tenants.every((t) => ["acme", "globex"].includes(t.tenant)), `--tenants --json: un elemento por servicio y tenant (${json.length} × 2)`);
  afirmar(
    tenants.every((t) => {
      const o = json.find((s) => s.name === t.name);
      return JSON.stringify(o.publish) === JSON.stringify(t.publish) && JSON.stringify(o.subscribe) === JSON.stringify(t.subscribe) &&
        JSON.stringify(o.allowResponses) === JSON.stringify(t.allowResponses);
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

/**
 * El generador y la petición/respuesta (11-request-reply.md §7, 06-security.md §3.9), sin servidor: la forma EXACTA de lo
 * que recibe quien llama y el dueño, lo que NUNCA recibe nadie, y lo que el manifiesto rechaza y acepta. Los permisos se
 * EJECUTAN contra un broker en las secciones 3, 4c, 5c, 6c y 8.
 */
async function seccionGeneradorRpc(json, dev, proto) {
  seccion("1c. El generador y la petición/respuesta (`calls`, `serves`): lo que recibe cada rol y lo que el manifiesto rechaza");
  const rol = (n) => json.find((s) => s.name === n);
  const crm = rol("crm-api");
  const pedidos = rol("pedidos-api");
  const fact = rol("facturacion-api");
  const ar = proto.rpc.acl.ownerAllowResponses;

  afirmar(
    JSON.stringify(crm.subscribe) === JSON.stringify(["_INBOX.crm-api.>", "rpc.clientes.>"]) &&
      JSON.stringify(crm.allowResponses) === JSON.stringify({ max: 1, expires: "30s" }) &&
      crm.serves.length === 2 && crm.calls.length === 0,
    "crm-api (el dueño, `serves` 2 operaciones): SE SUSCRIBE a su inbox y a `rpc.clientes.>`, y responde con allow_responses { max: 1, expires: 30s }",
    JSON.stringify({ subscribe: crm.subscribe, allowResponses: crm.allowResponses }),
  );
  afirmar(
    !crm.publish.some((p) => /^rpc\b|^_INBOX\b/.test(p)),
    "crm-api: NINGÚN permiso de publicación sobre `rpc.` ni sobre inboxes: responde con allow_responses, no con un permiso (A-16, A-68)",
    crm.publish.join("\n"),
  );
  afirmar(
    JSON.stringify(fact.publish.filter((p) => /^rpc\./.test(p))) === JSON.stringify(["rpc.clientes.cliente.v1.calcular-descuento", "rpc.clientes.cliente.v1.obtener"]) &&
      JSON.stringify(fact.subscribe) === JSON.stringify(["_INBOX.facturacion-api.>"]) && fact.allowResponses === null,
    "facturacion-api (solo llama): publica en el subject EXACTO de cada operación, se suscribe SOLO a su inbox y no tiene allow_responses",
    JSON.stringify({ rpc: fact.publish.filter((p) => /^rpc\./.test(p)), subscribe: fact.subscribe, allowResponses: fact.allowResponses }),
  );
  afirmar(
    JSON.stringify(pedidos.publish.filter((p) => /^rpc\./.test(p))) === JSON.stringify(["rpc.clientes.cliente.v1.calcular-descuento"]) &&
      JSON.stringify(pedidos.subscribe) === JSON.stringify(["_INBOX.pedidos-api.>"]) && pedidos.allowResponses === null,
    "pedidos-api (dueño de `pedidos` que solo llama a una operación): una concesión, ninguna suscripción de dominio y ninguna respuesta",
  );
  afirmar(
    json.filter((s) => s.calls.length === 0 && s.serves.length === 0).every((s) => !s.publish.some((p) => /^rpc\b/.test(p)) && s.subscribe.length === 1 && s.allowResponses === null),
    "un servicio sin `calls` ni `serves` no recibe NADA de rpc: ni publicación, ni suscripción, ni allow_responses",
  );
  // Lo que NUNCA recibe nadie, ni siquiera el dueño.
  const todos = [...json, ...dev];
  afirmar(
    todos.every((s) => s.publish.every((p) => !/^rpc\b/.test(p) || (new RegExp(proto.rpc.acl.callerPublish).test(p) && !/[*>]/.test(p)))),
    "NINGÚN servicio recibe `rpc.>`, `rpc.<dominio>.>` ni un comodín por token para publicar (solo el subject exacto de cada operación llamada)",
  );
  afirmar(
    todos.every((s) => s.subscribe.every((x) => x === `_INBOX.${s.name}.>` || (new RegExp(proto.rpc.acl.ownerSubscribe).test(x) && !proto.rpc.acl.ownerNeverSubscribe.includes(x)))),
    "NINGÚN servicio se suscribe a `rpc.>`, `rpc.*.>` ni a `>`: solo `rpc.<dominio>.>` del dominio que atiende",
  );
  afirmar(
    todos.every((s) => !s.publish.some((p) => /^_INBOX\b/.test(p))),
    "NINGÚN servicio recibe un permiso de publicación sobre inboxes (ni el dueño: allow_responses se lo da, y solo para el `reply` de la petición)",
  );
  afirmar(
    json.filter((s) => s.allowResponses).map((s) => s.name).join() === "crm-api" &&
      todos.every((s) => !s.allowResponses || (s.allowResponses.max === ar.maxReplies && parseInt(s.allowResponses.expires, 10) * 1000 > proto.rpc.timeout.maxMs)),
    `allow_responses solo para quien atiende, con max ${ar.maxReplies} y una caducidad (${crm.allowResponses?.expires}) por encima del timeout máximo de una llamada (${proto.rpc.timeout.maxMs / 1000} s): una respuesta legítima nunca llega después`,
  );
  afirmar(
    JSON.stringify(dev.map((s) => [s.subscribe, s.allowResponses, s.publish.filter((p) => /^rpc\./.test(p))])) === JSON.stringify(json.map((s) => [s.subscribe, s.allowResponses, s.publish.filter((p) => /^rpc\./.test(p))])),
    "--dev no cambia ningún permiso de rpc: CONSUMER.CREATE es lo único que añade",
  );

  const conf = gen(EJEMPLO);
  const lineasAllow = conf.salida.split("\n").filter((l) => /allow_responses/.test(l) && !/^\s*#/.test(l));
  afirmar(
    conf.codigo === 0 && lineasAllow.length === 1 && lineasAllow[0].trim() === 'allow_responses: { max: 1, expires: "30s" }' && /rpc\.clientes\.>/.test(conf.salida.replace(/^#.*$/gm, "")),
    "la config de nats-server lleva UN `allow_responses: { max: 1, expires: \"30s\" }` (el de crm-api) y su `rpc.clientes.>`",
    lineasAllow.join("\n"),
  );
  const check = gen(EJEMPLO, "--check");
  afirmar(check.codigo === 0 && /2 operación\(es\) atendida\(s\) y 3 llamada\(s\)/.test(check.salida), "--check cuenta las operaciones atendidas y llamadas", check.salida);

  // El manifiesto: lo que debe rechazar (con su pista) y lo que acepta.
  const dueño = { name: "crm-api", owns: ["clientes"], serves: ["clientes.cliente.v1.obtener", "clientes.cliente.v1.calcular-descuento"] };
  const con = (extra, servicios = [dueño]) => ({ services: [...servicios, ...(extra ? [extra] : [])] });
  const rechazos = [
    ["una llamada con el prefijo `rpc.` (dominio reservado)", con({ name: "web-api", calls: ["rpc.clientes.cliente.v1.obtener"] }), /RESERVADO[\s\S]*prefijo `rpc\.` lo pone el SDK/],
    ["una llamada con comodín de token", con({ name: "web-api", calls: ["clientes.cliente.v1.*"] }), /lleva un comodín[\s\S]*TODAS/],
    ["una llamada con `>` («para llamarlo todo»)", con({ name: "web-api", calls: ["clientes.>"] }), /lleva un comodín/],
    ["una operación de un dominio de estado (`state.…`)", con({ name: "web-api", calls: ["state.clientes.cliente.v1"] }), /RESERVADO/],
    ["una operación que no es una operación (3 tokens)", con({ name: "web-api", calls: ["clientes.cliente.v1"] }), /no es una operación válida[\s\S]*INFINITIVO/],
    ["una operación con mayúsculas", con({ name: "web-api", calls: ["Clientes.cliente.v1.obtener"] }), /no es una operación válida/],
    ["una entrada que no es una cadena", con({ name: "web-api", calls: [42] }), /entrada inválida/],
    ["`calls` que no es una lista", con({ name: "web-api", calls: "clientes.cliente.v1.obtener" }), /debe ser una lista/],
    ["`serves` que no es una lista", con(null, [{ ...dueño, serves: "clientes.cliente.v1.obtener" }]), /debe ser una lista/],
    ["una operación repetida", con({ name: "web-api", calls: ["clientes.cliente.v1.obtener", "clientes.cliente.v1.obtener"] }), /está dos veces/],
    ["una operación que no existe en el bundle", con({ name: "web-api", calls: ["clientes.cliente.v1.crear"] }), /no existe en el bundle[\s\S]*calcular-descuento/],
    ["una operación sin esquema en un dominio sin ninguno", con(null, [{ name: "x-api", owns: ["pedidos"], serves: ["pedidos.pedido.v1.crear"] }]), /no existe en el bundle[\s\S]*schemas\/rpc\/pedidos\/pedido\/crear\/\{request,reply\}\/1\.0\.0\.json/],
    ["llamar a una operación que nadie atiende", con({ name: "web-api", calls: ["clientes.cliente.v1.obtener"] }, [{ name: "crm-api", owns: ["clientes"] }]), /que nadie atiende[\s\S]*serves/],
    ["llamar a un dominio sin dueño", { services: [{ name: "web-api", calls: ["clientes.cliente.v1.obtener"] }] }, /nadie declara ser dueño/],
    ["llamar a la operación propia", con(null, [{ ...dueño, calls: ["clientes.cliente.v1.obtener"] }]), /su propia operación/],
    ["atender una operación del dominio de OTRO (dos servicios sobre la misma: los dos la ejecutan)", con({ name: "otro-api", owns: ["pedidos"], serves: ["clientes.cliente.v1.obtener"] }), /es de crm-api[\s\S]*A-60/],
    ["atender una operación sin ser dueño de nada", con({ name: "web-api", serves: ["clientes.cliente.v1.calcular-descuento"] }, [{ name: "crm-api", owns: ["clientes"] }]), /es de crm-api/],
    ["atender una operación con comodín", con(null, [{ ...dueño, serves: ["clientes.cliente.v1.*"] }]), /lleva un comodín/],
    ["una errata de clave (`serve`)", con(null, [{ ...dueño, serve: dueño.serves, serves: undefined }]), /clave desconocida «serve»[\s\S]*serves/],
  ];
  for (const [que, m, patron] of rechazos) {
    const r = gen(manifiestoTemporalSync(m), "--check");
    afirmar(r.codigo === 1 && patron.test(r.errores), `rechaza ${que}`, `exit ${r.codigo}: ${r.errores.trim()}`);
  }
  const sinRpc = path.join(TMP, "bundle-sin-rpc.json");
  const b = JSON.parse(readFileSync(path.join(ROOT, "schemas", "bundle.json"), "utf8"));
  delete b.rpc;
  writeFileSync(sinRpc, JSON.stringify(b));
  const antiguo = gen(manifiestoTemporalSync(con({ name: "web-api", calls: ["clientes.cliente.v1.obtener"] })), "--bundle", sinRpc, "--check");
  afirmar(antiguo.codigo === 1 && /no existe en el bundle/.test(antiguo.errores), "con un bundle SIN índice `rpc` (anterior a 1.1) ninguna operación existe: se rechaza, no se concede sin esquema");

  const bueno = gen(manifiestoTemporalSync(con({ name: "web-api", calls: ["clientes.cliente.v1.obtener"] })), "--json");
  const web = bueno.codigo === 0 ? JSON.parse(bueno.salida).find((x) => x.name === "web-api") : null;
  afirmar(
    !!web && JSON.stringify(web.publish) === JSON.stringify(["$JS.API.INFO", "rpc.clientes.cliente.v1.obtener"]) && JSON.stringify(web.subscribe) === JSON.stringify(["_INBOX.web-api.>"]) && web.allowResponses === null,
    "acepta un servicio que SOLO llama (sin `owns`): `$JS.API.INFO`, el subject exacto y su inbox; ni una suscripción de dominio ni allow_responses",
    bueno.errores + (web ? web.publish.join("\n") : bueno.salida),
  );
  const dos = gen(manifiestoTemporalSync({ services: [{ name: "a-api", owns: ["clientes"], serves: ["clientes.cliente.v1.obtener", "clientes.cliente.v1.calcular-descuento"] }, { name: "b-api", owns: ["pedidos"], serves: [] }] }), "--json");
  afirmar(
    dos.codigo === 0 && JSON.parse(dos.salida).find((x) => x.name === "b-api").subscribe.length === 1,
    "un dueño que no declara `serves` (b-api, dueño de pedidos) no recibe ninguna suscripción de rpc",
    dos.errores,
  );
}

/**
 * El generador y el estado (10-state.md §8, 06-security.md §3.8), sin servidor: la forma EXACTA de
 * lo que recibe cada rol, lo que el manifiesto rechaza y lo que acepta. Los permisos se EJECUTAN
 * contra un broker en las secciones 3b, 4b, 5b y 6b.
 */
function seccionGeneradorEstado(json, dev) {
  const rol = (n) => json.find((s) => s.name === n);
  const crm = rol("crm-api");
  const tienda = rol("tienda-web");
  const informes = rol("informes-api");

  afirmar(
    JSON.stringify(crm.publish) === JSON.stringify([
      "$JS.API.INFO",
      "clientes.>",
      "$JS.API.STREAM.INFO.EVT_CLIENTES",
      "$JS.API.CONSUMER.INFO.DLQ_CLIENTES.crm-api__dlq_clientes",
      "$JS.API.CONSUMER.MSG.NEXT.DLQ_CLIENTES.crm-api__dlq_clientes",
      "$JS.ACK.DLQ_CLIENTES.crm-api__dlq_clientes.>",
      "state.clientes.>",
      "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.>",
      "$JS.API.CONSUMER.INFO.DLQ_CLIENTES.crm-api__dlq_state_clientes",
      "$JS.API.CONSUMER.MSG.NEXT.DLQ_CLIENTES.crm-api__dlq_state_clientes",
      "$JS.ACK.DLQ_CLIENTES.crm-api__dlq_state_clientes.>",
    ]),
    "crm-api (dueño del estado de clientes): lista EXACTA — state.clientes.>, DIRECT.GET de su dominio y su durable de DLQ de estado",
    crm.publish.join("\n"),
  );
  afirmar(
    JSON.stringify(tienda.publish) === JSON.stringify([
      "$JS.API.INFO",
      "$JS.API.CONSUMER.INFO.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme",
      "$JS.API.CONSUMER.MSG.NEXT.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme",
      "$JS.ACK.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme.>",
      "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme.*",
      "$JS.API.STREAM.INFO.DLQ_CLIENTES",
      "dlq.state.clientes.cliente.v1",
    ]),
    "tienda-web (observador confinado a acme): lista EXACTA — su durable, DIRECT.GET con el tenant FIJADO, la DLQ de estado y nada de STREAM.INFO.STATE_CLIENTES",
    tienda.publish.join("\n"),
  );
  afirmar(
    informes.publish.includes("$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.*.*") &&
      informes.publish.includes("$JS.API.CONSUMER.INFO.STATE_CLIENTES.informes-api__state_clientes_cliente_v1") &&
      informes.durables.some((d) => d.kind === "state" && d.tenant === null && d.filter === "state.clientes.cliente.v1.*.*"),
    "informes-api (sin confinar): durable sin sufijo de tenant, filtro `…v1.*.*` y DIRECT.GET de todos los tenants",
    informes.publish.join("\n"),
  );
  afirmar(
    json.filter((s) => s.publish.some((p) => /^state\./.test(p))).map((s) => s.name).join() === "crm-api",
    "SOLO el dueño recibe un permiso de publicación sobre `state.…`: ningún observador ni ningún otro servicio escribe estado",
  );
  afirmar(
    json.every((s) => s.publish.every((p) => !/^\$JS\.API\.DIRECT\.GET\.STATE_[A-Z0-9_]+(\.>)?$/.test(p) && p !== "$JS.API.DIRECT.GET.>")),
    "ningún servicio recibe la forma de CUERPO de DIRECT.GET ($JS.API.DIRECT.GET.<stream>) ni DIRECT.GET.>",
  );
  afirmar(
    json.filter((s) => s.publish.some((p) => p.includes("DIRECT.GET"))).map((s) => s.name).join() === "crm-api,tienda-web,informes-api",
    "DIRECT.GET solo lo reciben quien tiene estado (el dueño y los observadores); pedidos-api, facturacion-api, inventario-api y logistica-api, ninguno",
  );
  afirmar(tienda.watches[0].tenants.join() === "acme" && informes.watches[0].tenants === null, "--json expone `watches` normalizado: tenants [acme] y null (todos)");

  const creaDev = dev.find((s) => s.name === "tienda-web").publish.filter((p) => p.includes("CONSUMER.CREATE."));
  afirmar(
    JSON.stringify(creaDev) === JSON.stringify(["$JS.API.CONSUMER.CREATE.STATE_CLIENTES.tienda-web__state_clientes_cliente_v1__acme.state.clientes.cliente.v1.acme.*"]),
    "--dev: CONSUMER.CREATE del durable de estado PROPIO con su filtro exacto (tenant fijado), y nada más",
    creaDev.join("\n"),
  );
  afirmar(
    json.every((s) => !s.publish.some((p) => p.includes("CONSUMER.CREATE."))),
    "por defecto, ningún CREATE tampoco de estado",
  );

  // Manifiestos que el generador DEBE rechazar y los que DEBE aceptar.
  const base = (watches, extra = {}, dueño = { name: "crm-api", owns: ["clientes"] }) => ({
    services: [dueño, { name: "web-api", watches, ...extra }],
  });
  const rechazos = [
    ["un contrato de estado que no existe en el bundle", base(["clientes.cliente.v9"]), /no existe en el bundle/],
    ["un subject de EVENTO en `watches`", base(["pedidos.pedido.v1.creado"]), /subject de EVENTO/],
    ["`tenants` vacío (leído como «todos» sería la trampa de allow: [])", base([{ contract: "clientes.cliente.v1", tenants: [] }]), /lista NO vacía/],
    ["`tenant` en singular (una errata no puede ser «todos los tenants»)", base([{ contract: "clientes.cliente.v1", tenant: "acme" }]), /campo desconocido «tenant»/],
    ["un tenant en mayúsculas", base([{ contract: "clientes.cliente.v1", tenants: ["Acme"] }]), /tenant inválido/],
    ["un comodín como tenant", base([{ contract: "clientes.cliente.v1", tenants: ["*"] }]), /tenant inválido/],
    ["un tenant de 65 caracteres", base([{ contract: "clientes.cliente.v1", tenants: ["a".repeat(65)] }]), /tenant inválido/],
    ["tenants repetidos", base([{ contract: "clientes.cliente.v1", tenants: ["acme", "acme"] }]), /repetidos/],
    ["el mismo contrato dos veces", base(["clientes.cliente.v1", { contract: "clientes.cliente.v1", tenants: ["acme"] }]), /observa dos veces/],
    ["`watches` que no es una lista", base("clientes.cliente.v1"), /debe ser una lista/],
    ["una clave de servicio desconocida (`watch`)", base([], { watch: ["clientes.cliente.v1"] }), /clave desconocida «watch»[\s\S]*watches/],
    ["observar un dominio sin dueño", { services: [{ name: "web-api", watches: ["clientes.cliente.v1"] }] }, /nadie declara ser dueño/],
    ["el dueño observa su propio estado", { services: [{ name: "crm-api", owns: ["clientes"], watches: ["clientes.cliente.v1"] }] }, /su propio estado/],
    ["consumir un subject de `state.` como un evento", base([], { consumes: ["state.clientes.cliente.v1.acme"] }), /RESERVADO[\s\S]*watches/],
    ["ser dueño del dominio reservado `state` (recibiría `state.>`)", { services: [{ name: "x-api", owns: ["state"] }] }, /RESERVADO[\s\S]*TODOS los dominios/],
    ["ser dueño del dominio reservado `dlq`", { services: [{ name: "x-api", owns: ["dlq"] }] }, /RESERVADO/],
    ["ser dueño del dominio reservado `rpc`", { services: [{ name: "x-api", owns: ["rpc"] }] }, /RESERVADO/],
    ["observar un contrato de un dominio reservado", base(["state.cliente.v1"]), /RESERVADO/],
  ];
  for (const [que, m, patron] of rechazos) {
    const r = gen(manifiestoTemporalSync(m), "--check");
    afirmar(r.codigo === 1 && patron.test(r.errores), `rechaza ${que}`, `exit ${r.codigo}: ${r.errores.trim()}`);
  }
  const dosTenants = gen(manifiestoTemporalSync(base([{ contract: "clientes.cliente.v1", tenants: ["globex", "acme-corp"] }])), "--json");
  const web = dosTenants.codigo === 0 ? JSON.parse(dosTenants.salida).find((x) => x.name === "web-api") : null;
  afirmar(
    !!web &&
      web.durables.map((d) => d.durable).join() === "web-api__state_clientes_cliente_v1__acme_corp,web-api__state_clientes_cliente_v1__globex" &&
      web.publish.filter((p) => p.includes("DIRECT.GET")).join() === "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.acme-corp.*,$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1.globex.*" &&
      web.publish.filter((p) => p === "dlq.state.clientes.cliente.v1").length === 1,
    "acepta varios tenants: un durable (con `-` → `_`) y un DIRECT.GET por tenant, y UN solo `dlq.state.<contrato>`",
    dosTenants.errores + (web ? web.publish.join("\n") : dosTenants.salida),
  );
  const sinEstado = gen(manifiestoTemporalSync({ services: [{ name: "crm-api", owns: ["clientes"] }] }), "--bundle", bundleSinEstadoSync(), "--json");
  afirmar(
    sinEstado.codigo === 0 && !JSON.parse(sinEstado.salida)[0].publish.some((p) => p.startsWith("state.") || p.includes("DIRECT.GET")),
    "con un bundle SIN contratos de estado, el dueño no recibe ningún permiso de estado (un dominio tiene estado si el bundle lo dice)",
  );
}

// ─── Estado: el ciclo de cada rol y las sondas ───────────────────────────────

const CONTRATO = "clientes.cliente.v1";
const STREAM_ESTADO = "STATE_CLIENTES";
const subjEstado = (tenant, clave) => `state.${CONTRATO}.${tenant}.${clave}`;
const datosEstado = (version, clave) => enc.encode(JSON.stringify({ clienteId: clave, aggregateVersion: version }));

/**
 * DIRECT.GET por subject, como lo pide un SDK (`$JS.API.DIRECT.GET.<stream>.<subject>`, A-32).
 * `denegado` es una violación de permiso; `no_existe` el 404 del broker; `leido` trae la secuencia
 * y el cuerpo. Un stream que no existe, o sin allow_direct, NO contesta (`sin_respuesta`).
 */
async function directGet(nc, subject, { stream = STREAM_ESTADO, cuerpo = "" } = {}) {
  try {
    const r = await nc.request(`$JS.API.DIRECT.GET.${stream}.${subject}`, cuerpo, { timeout: 1500 });
    const code = r.headers?.code ?? 0;
    if (code === 404) return { estado: "no_existe" };
    if (code >= 400) return { estado: "mala_peticion", code };
    return { estado: "leido", seq: Number(r.headers.get("Nats-Sequence")), subject: r.headers.get("Nats-Subject"), data: dec.decode(r.data) };
  } catch (e) {
    if (esViolacion(e)) return { estado: "denegado" };
    return { estado: /no responders/i.test(`${e?.message} ${e?.cause?.message}`) ? "sin_respuesta" : "otro", detalle: resumen(e) };
  }
}

/** Una escritura condicional por JetStream, como el paso 3 de `put` (10-state.md §3.2). */
async function escribirEstado(nc, subject, datos, { esperada, msgID = randomUUID() }) {
  const ack = await jetstream(nc, { timeout: 2000 }).publish(subject, datos, { msgID, expect: { lastSubjectSequence: esperada } });
  return ack;
}

/**
 * El ciclo ESTADO de un DUEÑO, con sus permisos generados: el paso 1 de `put` (DIRECT.GET), la
 * escritura condicional, el chequeo de secuencia, escribir en todos sus tenants y leer su DLQ de
 * estado con un pull consumer. Independiente: un permiso que falta rompe SU paso.
 */
async function cicloEstadoDueño({ nc, svc, ops, marca }) {
  const pasos = [];
  const paso = async (nombre, fn) => {
    try {
      pasos.push({ nombre, ok: true, detalle: await conTiempo(fn(), 8000, nombre) });
    } catch (e) {
      pasos.push({ nombre, ok: false, detalle: resumen(e) });
    }
  };
  const clave = `dueno-${marca}`;
  const [sAcme, sGlobex] = [subjEstado("acme", clave), subjEstado("globex", clave)];
  const durable = dlqStateDurableName(svc.name, svc.stateOwns[0]);
  const dlqStream = dlqStreamName(svc.stateOwns[0]);

  // Terreno: un mensaje en la DLQ de estado y el durable de su lectura, limpio.
  await ops.jsm.consumers.delete(dlqStream, durable).catch(() => {});
  await ops.jsm.consumers.add(dlqStream, await configConsumidor(durable, `dlq.state.${svc.stateOwns[0]}.>`));
  await ops.js.publish(`dlq.state.${CONTRATO}`, enc.encode(`{"muerto":"${marca}"}`), { msgID: randomUUID() });

  let s1;
  let s2;
  await paso("put(): DIRECT.GET de la clave nueva → 404 (no hay valor: S = 0)", async () => {
    const r = await directGet(nc, sAcme);
    if (r.estado !== "no_existe") throw new Error(`${r.estado} ${r.detalle ?? ""}`);
  });
  await paso(`put(): publicar con Nats-Expected-Last-Subject-Sequence 0 → el acuse es de ${STREAM_ESTADO}`, async () => {
    const ack = await escribirEstado(nc, sAcme, datosEstado(1, clave), { esperada: 0 });
    if (ack.stream !== STREAM_ESTADO) throw new Error(`lo capturó ${ack.stream}`);
    s1 = ack.seq;
  });
  await paso("get(): relee la clave y ve su propia secuencia", async () => {
    const r = await directGet(nc, sAcme);
    if (r.estado !== "leido" || r.seq !== s1 || !r.data.includes(clave)) throw new Error(JSON.stringify(r));
  });
  await paso("put(): actualiza con la secuencia leída → se aplica y la anterior sale del stream", async () => {
    s2 = (await escribirEstado(nc, sAcme, datosEstado(2, clave), { esperada: s1 })).seq;
    const info = await ops.jsm.streams.info(STREAM_ESTADO, { subjects_filter: sAcme });
    if (info.state.subjects?.[sAcme] !== 1) throw new Error(`mensajes en la clave: ${info.state.subjects?.[sAcme]}`);
  });
  await paso("put(): una secuencia vieja la rechaza el BROKER (10071): el chequeo condicional funciona bajo la ACL", async () => {
    try {
      await escribirEstado(nc, sAcme, datosEstado(3, clave), { esperada: s1 });
    } catch (e) {
      if (e?.code === 10071 || /wrong last sequence/.test(e?.message)) return;
      throw e;
    }
    throw new Error("el broker aceptó una escritura con la secuencia vieja");
  });
  await paso("put(): escribe también en OTRO tenant de su dominio (globex): el dueño escribe todos los suyos", async () => {
    const ack = await escribirEstado(nc, sGlobex, datosEstado(1, clave), { esperada: 0 });
    if (ack.stream !== STREAM_ESTADO) throw new Error(`lo capturó ${ack.stream}`);
  });
  await paso(`DLQ de estado: leer ${dlqStream} con su pull consumer → CONSUMER.INFO, MSG.NEXT y ack`, async () => {
    const c = await jetstream(nc, { timeout: 2000 }).consumers.get(dlqStream, durable);
    const m = await recibirUno(c, 2500);
    if (!m) throw new Error("no llegó el mensaje de la DLQ de estado");
    m.ack();
    await nc.flush();
    await sleep(300);
    const i = await ops.jsm.consumers.info(dlqStream, durable);
    if (i.num_ack_pending !== 0 || i.ack_floor.stream_seq < m.seq) throw new Error("el ack no se registró");
  });
  return { pasos, ok: pasos.every((p) => p.ok), fallidos: pasos.filter((p) => !p.ok).map((p) => p.nombre) };
}

/**
 * El ciclo ESTADO de un OBSERVADOR (`watch` y `get`): comprueba su DLQ, pregunta por su durable,
 * pide la foto, confirma, lee una clave por DIRECT.GET y manda a la DLQ de estado lo que no pudo
 * aplicar. Con `exhaustivo`, además drena el durable y exige que NUNCA llegue un tenant ajeno.
 */
async function cicloEstadoObservador({ nc, svc, ops, marca, exhaustivo = false }) {
  const pasos = [];
  const paso = async (nombre, fn) => {
    try {
      pasos.push({ nombre, ok: true, detalle: await conTiempo(fn(), 8000, nombre) });
    } catch (e) {
      pasos.push({ nombre, ok: false, detalle: resumen(e) });
    }
  };
  const w = svc.watches[0];
  const d = svc.durables.find((x) => x.kind === "state");
  const tenant = w.tenants?.[0] ?? "acme";
  const confinado = !!w.tenants;
  const claveAcme = `obs-${marca}`;
  const dlqStream = dlqStreamName(w.domain);

  // Terreno: el dueño (ops, aquí) escribe la clave en dos tenants y el durable arranca limpio, con la
  // configuración de `flux provision` (last_per_subject).
  await ops.js.publish(subjEstado("acme", claveAcme), datosEstado(1, claveAcme), { msgID: randomUUID() });
  await ops.js.publish(subjEstado("globex", claveAcme), datosEstado(1, claveAcme), { msgID: randomUUID() });
  await ops.jsm.consumers.delete(d.stream, d.durable).catch(() => {});
  await ops.jsm.consumers.add(d.stream, { ...(await configConsumidor(d.durable, d.filter)), deliver_policy: "last_per_subject" });

  const js = jetstream(nc, { timeout: 2000 });
  await paso("connect(): jetstreamManager → $JS.API.INFO", async () => {
    await jetstreamManager(nc, { timeout: 2000 });
  });
  await paso(`watch(): comprueba su DLQ → STREAM.INFO.${dlqStream}`, async () => {
    const r = await pedir(nc, `$JS.API.STREAM.INFO.${dlqStream}`);
    if (r.estado !== "respondido" || r.json?.config?.name !== dlqStream) throw new Error(r.estado === "respondido" ? JSON.stringify(r.json?.error) : r.detalle);
  });
  let consumidor;
  await paso(`watch(): consumers.get → CONSUMER.INFO de ${d.durable}`, async () => {
    consumidor = await js.consumers.get(d.stream, d.durable);
  });
  let m;
  await paso(`watch(): pide la foto (CONSUMER.MSG.NEXT) y llega una entrada de ${confinado ? `acme` : "un tenant"}, con el subject original`, async () => {
    m = await recibirUno(consumidor, 2500);
    if (!m) throw new Error("no llegó ninguna entrada");
    if (!m.subject.startsWith(`state.${CONTRATO}.`)) throw new Error(`llegó con el subject ${m.subject}`);
    if (confinado && !m.subject.startsWith(`state.${CONTRATO}.${tenant}.`)) throw new Error(`LLEGÓ UN TENANT AJENO: ${m.subject}`);
  });
  if (m) {
    m.ack();
    await nc.flush();
    await sleep(300);
    await paso("ack(): el broker lo registra (ack_floor al día)", async () => {
      const i = await ops.jsm.consumers.info(d.stream, d.durable);
      if (i.num_ack_pending !== 0 || i.ack_floor.stream_seq < m.seq) throw new Error(`pendientes ${i.num_ack_pending}, ack_floor ${i.ack_floor.stream_seq}`);
    });
  } else {
    pasos.push({ nombre: "ack(): el broker lo registra (ack_floor al día)", ok: false, detalle: "no hay entrada" });
  }
  await paso(`get(): DIRECT.GET de la clave del tenant ${tenant} (con el tenant fijado)`, async () => {
    const r = await directGet(nc, subjEstado(tenant, claveAcme));
    if (r.estado !== "leido" || !r.data.includes(claveAcme)) throw new Error(JSON.stringify(r));
  });
  await paso(`DLQ de estado: publicar en dlq.state.${CONTRATO} → lo captura ${dlqStream}`, async () => {
    const r = await js.publish(`dlq.state.${CONTRATO}`, enc.encode('{"key":"x","revision":1}'), { msgID: randomUUID() });
    if (r.stream !== dlqStream) throw new Error(`lo capturó ${r.stream}`);
  });
  if (exhaustivo) {
    await paso(confinado ? `confinamiento: drena el durable y NUNCA llega un tenant que no sea ${tenant}` : "sin confinar: drena el durable y llegan los dos tenants (acme y globex)", async () => {
      const vistos = new Set([m?.subject].filter(Boolean));
      for (let i = 0; i < 40; i++) {
        const x = await recibirUno(consumidor, 1000); // el pull exige `expires` ≥ 1000 ms
        if (!x) break;
        vistos.add(x.subject);
        x.ack();
      }
      const ajenos = [...vistos].filter((s) => confinado && !s.startsWith(`state.${CONTRATO}.${tenant}.`));
      if (ajenos.length) throw new Error(`tenant ajeno: ${ajenos.join(", ")}`);
      if (!confinado && ![...vistos].some((s) => s.includes(".acme.")) ) throw new Error(`no vio acme: ${[...vistos].join(", ")}`);
    });
  }
  return { pasos, ok: pasos.every((p) => p.ok), fallidos: pasos.filter((p) => !p.ok).map((p) => p.nombre) };
}

/**
 * Las sondas de estado: lo que cada servicio puede y NO puede hacer sobre `STATE_CLIENTES`. Cada
 * sonda dice a quién le está permitido; el resto DEBE recibir una violación. Devuelven `true` si
 * lo PELIGROSO estuvo permitido.
 */
function sondasEstado(marca) {
  const k = `sonda-${marca}`;
  const peticion = (subject, cuerpo = {}) => async (nc) => (await pedir(nc, subject, cuerpo)).estado !== "denegado";
  const crudo = (subject) => async (nc) => (await publicarCrudo(nc, subject)) === "permitido";
  const get = (subject, opts) => async (nc) => (await directGet(nc, subject, opts)).estado !== "denegado";
  const jsEscribe = (subject) => async (nc) => {
    try {
      await jetstream(nc, { timeout: 1500 }).publish(subject, enc.encode(JSON.stringify({ aggregateVersion: 1 })), { msgID: randomUUID() });
      return true;
    } catch (e) {
      return esViolacion(e) ? false : /no responders|no stream/i.test(e?.message ?? "") ? true : `otro error: ${resumen(e)}`;
    }
  };
  const duenos = ["crm-api"];
  const todosConEstado = ["crm-api", "tienda-web", "informes-api"];
  return [
    // ── Escribir ─────────────────────────────────────────────────────────────
    ["escribir estado por JetStream en state.clientes.cliente.v1.acme.<clave>", jsEscribe(subjEstado("acme", k)), duenos],
    ["…y por CORE, que sobrescribiría SIN chequeo de secuencia (A-45)", crudo(subjEstado("acme", `${k}-core`)), duenos],
    ["escribir el estado de OTRO tenant (globex)", crudo(subjEstado("globex", `${k}-core`)), duenos],
    ["escribir el estado de OTRO dominio (state.pedidos.pedido.v1.acme.<clave>)", crudo(`state.pedidos.pedido.v1.acme.${k}`), []],
    ["publicar un EVENTO en el dominio del estado (clientes.cliente.v1.registrado)", crudo("clientes.cliente.v1.registrado"), duenos],
    // ── Leer: DIRECT.GET por subject ─────────────────────────────────────────
    ["DIRECT.GET de una clave del tenant acme", get(subjEstado("acme", k)), todosConEstado],
    ["DIRECT.GET de una clave del tenant globex", get(subjEstado("globex", k)), ["crm-api", "informes-api"]],
    ["DIRECT.GET con un comodín en el tenant (…v1.*.<clave>)", get(subjEstado("*", k)), ["crm-api", "informes-api"]],
    ["DIRECT.GET con un comodín por encima del tenant (state.clientes.>)", get("state.clientes.>"), duenos],
    ["DIRECT.GET del estado de otro dominio", get(`state.pedidos.pedido.v1.acme.${k}`, { stream: "STATE_PEDIDOS" }), []],
    // ── Leer: la forma de CUERPO, que ninguna ACL ve (A-38) ─────────────────
    ["DIRECT.GET de CUERPO ($JS.API.DIRECT.GET.STATE_CLIENTES + {seq:1}): lee cualquier mensaje", async (nc) => (await pedir(nc, `$JS.API.DIRECT.GET.${STREAM_ESTADO}`, { seq: 1 })).estado !== "denegado", []],
    ["STREAM.MSG.GET.STATE_CLIENTES por secuencia", peticion(`$JS.API.STREAM.MSG.GET.${STREAM_ESTADO}`, { seq: 1 }), []],
    // ── Metadatos y gestión del stream ───────────────────────────────────────
    ["STREAM.INFO.STATE_CLIENTES con subjects_filter: lista TODAS las claves y tenants (A-39)", peticion(`$JS.API.STREAM.INFO.${STREAM_ESTADO}`, { subjects_filter: "state.clientes.>" }), []],
    ["STREAM.PURGE.STATE_CLIENTES", peticion(`$JS.API.STREAM.PURGE.${STREAM_ESTADO}`), []],
    ["STREAM.UPDATE.STATE_CLIENTES", peticion(`$JS.API.STREAM.UPDATE.${STREAM_ESTADO}`), []],
    ["STREAM.DELETE.STATE_CLIENTES", peticion(`$JS.API.STREAM.DELETE.${STREAM_ESTADO}`), []],
    ["CONSUMER.NAMES.STATE_CLIENTES (qué durables hay)", peticion(`$JS.API.CONSUMER.NAMES.${STREAM_ESTADO}`), []],
    ["crear un consumidor de estado con el filtro de TODOS los tenants", peticion(`$JS.API.CONSUMER.CREATE.${STREAM_ESTADO}.espia__state.state.clientes.cliente.v1.*.*`, { stream_name: STREAM_ESTADO, config: { durable_name: "espia__state", filter_subject: "state.clientes.cliente.v1.*.*", ack_policy: "explicit" } }), []],
    ["crear un consumidor de estado con la API antigua DURABLE.CREATE", peticion(`$JS.API.CONSUMER.DURABLE.CREATE.${STREAM_ESTADO}.espia2__state`, { stream_name: STREAM_ESTADO, config: { durable_name: "espia2__state", filter_subject: "state.clientes.cliente.v1.globex.*", ack_policy: "explicit" } }), []],
    // ── Los durables de los demás ────────────────────────────────────────────
    ["CONSUMER.INFO del durable de estado de tienda-web", peticion(`$JS.API.CONSUMER.INFO.${STREAM_ESTADO}.tienda-web__state_clientes_cliente_v1__acme`), ["tienda-web"]],
    ["CONSUMER.MSG.NEXT del durable de estado de tienda-web (quitarle la foto)", peticion(`$JS.API.CONSUMER.MSG.NEXT.${STREAM_ESTADO}.tienda-web__state_clientes_cliente_v1__acme`, { batch: 1, expires: 300_000_000 }), ["tienda-web"]],
    ["CONSUMER.MSG.NEXT del durable de estado de informes-api", peticion(`$JS.API.CONSUMER.MSG.NEXT.${STREAM_ESTADO}.informes-api__state_clientes_cliente_v1`, { batch: 1, expires: 300_000_000 }), ["informes-api"]],
    ["confirmar (ack) un mensaje del durable de estado de tienda-web", crudo(`$JS.ACK.${STREAM_ESTADO}.tienda-web__state_clientes_cliente_v1__acme.1.1.1.1.0`), ["tienda-web"]],
    ["CONSUMER.MSG.NEXT del durable de la DLQ de estado de crm-api", peticion("$JS.API.CONSUMER.MSG.NEXT.DLQ_CLIENTES.crm-api__dlq_state_clientes", { batch: 1, expires: 300_000_000 }), ["crm-api"]],
    // ── La DLQ de estado ─────────────────────────────────────────────────────
    ["publicar en dlq.state.clientes.cliente.v1 (la DLQ de estado de lo que observa)", crudo(`dlq.state.${CONTRATO}`), ["tienda-web", "informes-api"]],
    ["publicar en la DLQ de estado de un contrato que NO observa (dlq.state.pedidos.pedido.v1)", crudo("dlq.state.pedidos.pedido.v1"), []],
    ["publicar en dlq.state con tenant y clave (7 tokens: lo que la DLQ de estado NO lleva)", crudo(`dlq.state.${CONTRATO}.acme.${k}`), []],
    ["STREAM.INFO.DLQ_CLIENTES (el SDK comprueba la DLQ al abrir)", peticion("$JS.API.STREAM.INFO.DLQ_CLIENTES"), ["tienda-web", "informes-api"]],
    // ── Suscribirse ──────────────────────────────────────────────────────────
    ["suscribirse a state.clientes.> (los observadores no reciben nada por suscripción)", async (nc) => (await suscribirCrudo(nc, "state.clientes.>")) === "permitido", []],
    ["suscribirse a dlq.state.>", async (nc) => (await suscribirCrudo(nc, "dlq.state.>")) === "permitido", []],
  ];
}

/** Ejecuta las sondas de estado como `nombre` y compara con lo que le corresponde. Devuelve los desvíos. */
async function desviosDeSondasEstado(nc, nombre, sondas) {
  const desvios = [];
  for (const [que, fn, permitidoPara] of sondas) {
    let permitido;
    try {
      permitido = await fn(nc);
    } catch (e) {
      permitido = `error: ${resumen(e)}`;
    }
    const esperado = permitidoPara.includes(nombre);
    if (permitido !== esperado) desvios.push({ que, esperado, permitido });
  }
  return desvios;
}

/**
 * Las sondas de petición/respuesta: lo que cada servicio puede y NO puede hacer sobre `rpc.` y sobre los inboxes. Cada sonda
 * dice a quién le está permitido; el resto DEBE recibir una violación. Devuelven `true` si lo PELIGROSO estuvo permitido.
 * Como las de estado, se ejecutan como TODOS los servicios (una matriz) y comparan con lo que dice el manifiesto.
 * «Llamar» está permitido si el broker NO deniega la publicación: sin nadie escuchando, la respuesta es `no responders`.
 */
const OP_OBTENER = "clientes.cliente.v1.obtener";
const OP_DESCUENTO = "clientes.cliente.v1.calcular-descuento";
function sondasRpc(marca) {
  const llamar = (op) => async (nc) => (await pedir(nc, `rpc.${op}`, {}, 800)).estado !== "denegado";
  const crudo = (subject) => async (nc) => (await publicarCrudo(nc, subject)) === "permitido";
  /** Suscribirse y soltar la suscripción SIEMPRE: una suscripción permitida dejaría interés y falsearía los `no responders`. */
  const suscribir = (subject, queue) => async (nc) => {
    const antes = nc.violaciones.length;
    const sub = nc.subscribe(subject, queue ? { queue } : undefined);
    await nc.flush();
    await sleep(250);
    const denegada = nc.violaciones.slice(antes).some((v) => v.includes(`Subscription to "${subject}"`));
    sub.unsubscribe();
    await nc.flush();
    return !denegada;
  };
  const dueno = ["crm-api"];
  return [
    // ── Llamar ───────────────────────────────────────────────────────────────
    [`llamar a ${OP_OBTENER} (concedida solo a quien la declara en \`calls\`)`, llamar(OP_OBTENER), ["facturacion-api"]],
    [`llamar a ${OP_DESCUENTO}`, llamar(OP_DESCUENTO), ["facturacion-api", "pedidos-api"]],
    ["llamar a una operación NO concedida a nadie del MISMO dominio (clientes.cliente.v1.crear)  [A-66]", llamar("clientes.cliente.v1.crear"), []],
    ["llamar a una operación de OTRO dominio (pedidos.pedido.v1.obtener)", llamar("pedidos.pedido.v1.obtener"), []],
    ["publicar a pelo, sin respuesta, en rpc.clientes.cliente.v1.crear", crudo("rpc.clientes.cliente.v1.crear"), []],
    // ── Atender: suscribirse ─────────────────────────────────────────────────
    ["suscribirse a `rpc.>` (leería las peticiones de todos, con sus datos personales)", suscribir("rpc.>"), []],
    ["suscribirse a `rpc.*.>`", suscribir("rpc.*.>"), []],
    ["suscribirse a `rpc.clientes.>` (el permiso del dueño del dominio)", suscribir("rpc.clientes.>"), dueno],
    ["suscribirse a `rpc.pedidos.>` (un dominio cuyo dueño no atiende ninguna operación)", suscribir("rpc.pedidos.>"), []],
    ["suscribirse al subject de una operación (rpc.clientes.cliente.v1.obtener) con su grupo de cola", suscribir(`rpc.${OP_OBTENER}`, "verify-acl"), dueno],
    ["…suplantando el grupo de cola del dueño (`crm-api`) para llevarse la mitad de las peticiones  [A-60]", suscribir(`rpc.${OP_OBTENER}`, "crm-api"), dueno],
    // ── Los inboxes: nadie publica en uno por su cuenta ──────────────────────
    [`publicar en el inbox de otro servicio (_INBOX.verify-otro.${marca})`, crudo(`_INBOX.verify-otro.${marca}`), []],
    ["publicar en el inbox privado PROPIO: el dueño no tiene ningún permiso de publicación sobre inboxes (A-68)", async (nc) => (await publicarCrudo(nc, `${nc.options?.inboxPrefix ?? "_INBOX.x"}.${marca}`)) === "permitido", []],
  ];
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

  // Estado: una variante por permiso de tienda-web (el observador confinado) y de crm-api (el
  // dueño), SIN ese permiso; y mutantes sembrados que conceden DE MÁS (06-security.md §3.8).
  const tiendaP = json.find((s) => s.name === "tienda-web");
  const crmP = json.find((s) => s.name === "crm-api");
  const variantesTienda = tiendaP.publish.map((p, i) => ({ quitado: p, ...nuevaClave(), nombre: `tienda-sin-${i}` }));
  const variantesCrm = crmP.publish.map((p, i) => ({ quitado: p, ...nuevaClave(), nombre: `crm-sin-${i}` }));
  const direct = "$JS.API.DIRECT.GET.STATE_CLIENTES.state.clientes.cliente.v1";
  const mutantesEstado = [
    { que: "tienda-web + `state.clientes.>`: un observador que ESCRIBE", base: "tienda-web", anade: ["state.clientes.>"], flip: ["escribir estado por JetStream", "…y por CORE", "escribir el estado de OTRO tenant"] },
    { que: "tienda-web + STREAM.INFO.STATE_CLIENTES: lista claves y tenants (A-39)", base: "tienda-web", anade: ["$JS.API.STREAM.INFO.STATE_CLIENTES"], flip: ["STREAM.INFO.STATE_CLIENTES con subjects_filter"] },
    { que: "tienda-web con DIRECT.GET `…v1.*.*` en vez de `…acme.*`: SIN tenant fijado", base: "tienda-web", cambia: [[`${direct}.acme.*`, `${direct}.*.*`]], flip: ["DIRECT.GET de una clave del tenant globex", "DIRECT.GET con un comodín en el tenant"] },
    { que: "tienda-web + la forma de CUERPO de DIRECT.GET", base: "tienda-web", anade: ["$JS.API.DIRECT.GET.STATE_CLIENTES"], flip: ["DIRECT.GET de CUERPO"] },
    { que: "tienda-web + `$JS.API.DIRECT.GET.>` «para arreglarlo»", base: "tienda-web", anade: ["$JS.API.DIRECT.GET.>"], flip: ["DIRECT.GET de CUERPO", "DIRECT.GET de una clave del tenant globex", "DIRECT.GET con un comodín en el tenant", "DIRECT.GET con un comodín por encima del tenant", "DIRECT.GET del estado de otro dominio"] },
    { que: "tienda-web + CONSUMER.CREATE.STATE_CLIENTES.>: crea un consumidor de todos los tenants", base: "tienda-web", anade: ["$JS.API.CONSUMER.CREATE.STATE_CLIENTES.>"], flip: ["crear un consumidor de estado con el filtro de TODOS"] },
    { que: "tienda-web + MSG.NEXT del durable de informes-api: le quita la foto", base: "tienda-web", anade: ["$JS.API.CONSUMER.MSG.NEXT.STATE_CLIENTES.informes-api__state_clientes_cliente_v1"], flip: ["CONSUMER.MSG.NEXT del durable de estado de informes-api"] },
    { que: "crm-api + `state.>`: el dueño de un dominio escribe el estado de TODOS", base: "crm-api", anade: ["state.>"], flip: ["escribir el estado de OTRO dominio"] },
    { que: "crm-api + STREAM.INFO.STATE_CLIENTES (decisión: tampoco el dueño)", base: "crm-api", anade: ["$JS.API.STREAM.INFO.STATE_CLIENTES"], flip: ["STREAM.INFO.STATE_CLIENTES con subjects_filter"] },
  ].map((m, i) => ({ ...m, ...nuevaClave(), nombre: `m-estado-${i}` }));
  // Petición/respuesta: el dueño SIN su suscripción y SIN allow_responses (minimalidad), y mutantes sembrados que conceden DE
  // MÁS (06-security.md §3.9): `rpc.<dominio>.>` o `rpc.>` a quien llama, `rpc.>` o publicar en los inboxes al dueño.
  const factP = json.find((s) => s.name === "facturacion-api");
  const crmSinSub = { quitado: "la suscripción `rpc.clientes.>`", ...nuevaClave(), nombre: "crm-sin-sub-rpc" };
  const crmSinAllow = { quitado: "allow_responses", ...nuevaClave(), nombre: "crm-sin-allow" };
  const crmSinAckConAllow = { quitado: "$JS.ACK.DLQ_CLIENTES.crm-api__dlq_clientes.> (CON allow_responses)", ...nuevaClave(), nombre: "crm-sin-ack-con-allow" };
  const mutantesRpc = [
    { que: "facturacion-api + `rpc.clientes.>` de PUBLICACIÓN: quien llama podría llamar a todo el dominio", base: "facturacion-api", anadePub: ["rpc.clientes.>"], flip: ["una operación NO concedida a nadie del MISMO dominio", "publicar a pelo, sin respuesta"] },
    { que: "facturacion-api + `rpc.>` de PUBLICACIÓN: llamar a las operaciones de todos los dominios", base: "facturacion-api", anadePub: ["rpc.>"], flip: ["una operación NO concedida a nadie del MISMO dominio", "una operación de OTRO dominio", "publicar a pelo, sin respuesta"] },
    { que: "facturacion-api + `rpc.>` de SUSCRIPCIÓN: quien llama leería todas las peticiones", base: "facturacion-api", anadeSub: ["rpc.>"], flip: ["suscribirse a `rpc.>`", "suscribirse a `rpc.*.>`", "suscribirse a `rpc.clientes.>`", "suscribirse a `rpc.pedidos.>`", "con su grupo de cola", "suplantando el grupo de cola"] },
    { que: "crm-api + `rpc.>` de SUSCRIPCIÓN: el dueño atendería las operaciones de todos", base: "crm-api", anadeSub: ["rpc.>"], flip: ["suscribirse a `rpc.>`", "suscribirse a `rpc.*.>`", "suscribirse a `rpc.pedidos.>`"] },
    { que: "crm-api + `_INBOX.>` de PUBLICACIÓN: el dueño escribiría en los inboxes de todos y anularía `max: 1`", base: "crm-api", anadePub: ["_INBOX.>"], flip: ["el inbox de otro servicio", "el inbox privado PROPIO"] },
  ].map((m, i) => ({ ...m, ...nuevaClave(), nombre: `m-rpc-${i}` }));
  const permisosMutante = (m) => {
    const base = json.find((x) => x.name === m.base);
    let pub = base.publish.map((x) => m.cambia?.find(([de]) => de === x)?.[1] ?? x);
    if (m.cambia && JSON.stringify(pub) === JSON.stringify(base.publish)) throw new Error(`el mutante «${m.que}» no cambia nada`);
    pub = [...pub, ...(m.anade ?? [])];
    return { publish: pub, subscribe: base.subscribe };
  };
  const permisosMutanteRpc = (m) => {
    const base = json.find((x) => x.name === m.base);
    return { publish: [...base.publish, ...(m.anadePub ?? [])], subscribe: [...base.subscribe, ...(m.anadeSub ?? [])], allowResponses: base.allowResponses };
  };
  const extraEstado =
    variantesTienda.map((x) => usuarioConf(x.pub, tiendaP.publish.filter((p) => p !== x.quitado), tiendaP.subscribe)).join("") +
    // SIN `allow_responses`: con él, el ack de lo que recibe pasa aunque falte `$JS.ACK…` (A-109) y la minimalidad de los
    // permisos de eventos y de estado no se vería. La del dueño de una operación es la sección 5c.
    variantesCrm.map((x) => usuarioConf(x.pub, crmP.publish.filter((p) => p !== x.quitado), crmP.subscribe.filter((s) => !s.startsWith("rpc.")))).join("") +
    mutantesEstado.map((m) => { const pm = permisosMutante(m); return usuarioConf(m.pub, pm.publish, pm.subscribe); }).join("") +
    usuarioConf(crmSinSub.pub, crmP.publish, crmP.subscribe.filter((x) => x !== "rpc.clientes.>"), crmP.allowResponses) +
    usuarioConf(crmSinAllow.pub, crmP.publish, crmP.subscribe, null) +
    usuarioConf(crmSinAckConAllow.pub, crmP.publish.filter((p) => p !== "$JS.ACK.DLQ_CLIENTES.crm-api__dlq_clientes.>"), crmP.subscribe, crmP.allowResponses) +
    mutantesRpc.map((m) => { const pm = permisosMutanteRpc(m); return usuarioConf(m.pub, pm.publish, pm.subscribe, pm.allowResponses); }).join("");

  const extra =
    variantes.map((x) => usuarioConf(x.pub, fact.publish.filter((p) => p !== x.quitado), fact.subscribe)).join("") +
    usuarioConf(sinInbox.pub, fact.publish, ["_INBOX.nadie.>"]) +
    usuarioConf(devOk.pub, factDev.publish, factDev.subscribe) +
    devSinCreate.map((x) => usuarioConf(x.pub, factDev.publish.filter((p) => p !== x.quitado), factDev.subscribe)).join("") +
    usuarioConf(mutanteInbox.pub, fact.publish, ["_INBOX.>"]) +
    usuarioConf(mutanteApi.pub, [...logi.publish, "$JS.API.>"], logi.subscribe) +
    extraEstado +
    `      { nkey: "${ops.pub}" },\n`;

  seccion("2. flux provision");
  let A;
  try {
    A = await arrancar("A", conUsuarios(generada, extra));
    afirmar(true, `nats-server carga la config generada con include (puerto ${A.port}); ${json.length} servicios + ops + ${variantes.length + devSinCreate.length + 4 + variantesTienda.length + variantesCrm.length + mutantesEstado.length + 3 + mutantesRpc.length} usuarios de prueba`);
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

  // Lo que el manifiesto exige: EVT_ y DLQ_ por dominio, STATE_ por dominio con estado, y los durables.
  const dominiosDueños = [...new Set(json.flatMap((s) => s.owns))];
  const dominiosEstado = [...new Set(json.flatMap((s) => s.stateOwns))];
  const nDurables = json.reduce((n, s) => n + s.durables.length, 0);
  const nStreams = 2 * dominiosDueños.length + dominiosEstado.length;
  const nRecursos = nStreams + nDurables;
  const dry = provision(EJEMPLO, "--dry-run");
  afirmar(dry.codigo === 0 && new RegExp(`${nRecursos} se crearía`).test(dry.salida), `--dry-run cuenta lo que crearía (${nStreams} streams y ${nDurables} durables) y sale con 0`, dry.salida + dry.errores);
  afirmar((await nombresStreams()).length === 0, "--dry-run NO crea nada: el broker sigue vacío");

  const crea = provision(EJEMPLO);
  afirmar(crea.codigo === 0 && new RegExp(`${nRecursos} creado`).test(crea.salida), `provision crea ${nStreams} streams y ${nDurables} durables`, crea.salida + crea.errores);
  const esperadosStreams = [
    ...dominiosDueños.flatMap((d) => [streamName(d), dlqStreamName(d)]),
    ...dominiosEstado.map(stateStreamName),
  ].sort();
  afirmar(JSON.stringify(await nombresStreams()) === JSON.stringify(esperadosStreams), `crea exactamente los streams EVT_/DLQ_ de cada dominio y STATE_ de cada dominio con estado: ${esperadosStreams.join(", ")}`);
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
  afirmar(repite.codigo === 0 && new RegExp(`${nRecursos} sin cambios`).test(repite.salida) && /Sin cambios/.test(repite.salida), `repetir es IDEMPOTENTE: «${nRecursos} sin cambios», exit 0`, repite.salida);
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

  // ── Estado: lo que provision crea, amplía e informa ──────────────────────────
  // El stream de estado lleva la configuración de protocol.json → state.stream, y los durables de estado,
  // last_per_subject con el filtro del contrato y su tenant (10-state.md §2.1 y §5.2).
  const infoEstado = await opsA.jsm.streams.info("STATE_CLIENTES");
  const cE = infoEstado.config;
  afirmar(
    cE.max_msgs_per_subject === 1 && cE.allow_direct === true && cE.max_age === 0 && cE.discard === "old" && cE.storage === "file" &&
      cE.duplicate_window === nanos(120_000) && cE.num_replicas === 1 && JSON.stringify(cE.subjects) === '["state.clientes.>"]',
    "STATE_CLIENTES lleva la config de 10-state.md §2.1: max_msgs_per_subject 1, allow_direct, max_age 0, discard old, ventana 2 m, file",
    JSON.stringify(cE),
  );
  const durEstado = await opsA.jsm.consumers.info("STATE_CLIENTES", "tienda-web__state_clientes_cliente_v1__acme");
  afirmar(
    durEstado.config.deliver_policy === "last_per_subject" && durEstado.config.filter_subject === "state.clientes.cliente.v1.acme.*" &&
      durEstado.config.ack_wait === nanos(30_000) && durEstado.config.max_deliver === 6 && durEstado.config.max_ack_pending === 256,
    "el durable de estado confinado: last_per_subject, filtro `…v1.acme.*` y la config canónica (ack_wait == backoff[0], max_deliver 6)",
    JSON.stringify(durEstado.config),
  );
  const durTodos = await opsA.jsm.consumers.info("STATE_CLIENTES", "informes-api__state_clientes_cliente_v1");
  afirmar(durTodos.config.filter_subject === "state.clientes.cliente.v1.*.*" && durTodos.config.deliver_policy === "last_per_subject", "el durable de estado sin confinar: sin sufijo de tenant y filtro `…v1.*.*`");
  const durDlqEstado = await opsA.jsm.consumers.info("DLQ_CLIENTES", "crm-api__dlq_state_clientes");
  afirmar(durDlqEstado.config.filter_subject === "dlq.state.clientes.>" && durDlqEstado.config.deliver_policy === "all", "el durable con el que el dueño lee su DLQ de estado: filtro `dlq.state.clientes.>`");
  afirmar(JSON.stringify((await opsA.jsm.streams.info("DLQ_CLIENTES")).config.subjects) === '["dlq.clientes.>","dlq.state.clientes.>"]', "DLQ_CLIENTES de un dominio CON estado captura dlq.<dom>.> y dlq.state.<dom>.>");
  afirmar(JSON.stringify((await opsA.jsm.streams.info("DLQ_PEDIDOS")).config.subjects) === '["dlq.pedidos.>"]', "…y la de un dominio SIN estado no cambia: 1.1 es aditiva");

  // Una DLQ de la 1.0 (solo dlq.<dom>.>) que gana estado se AMPLÍA, sin perder mensajes: es lo único
  // que provision cambia en un recurso existente (A-44).
  await opsA.jsm.streams.update("DLQ_CLIENTES", { subjects: ["dlq.clientes.>"] });
  await opsA.js.publish("dlq.clientes.cliente.v1.registrado", enc.encode('{"muerto":"antes de ampliar"}'));
  const mensajesAntes = (await opsA.jsm.streams.info("DLQ_CLIENTES")).state.messages;
  const ensayo = provision(EJEMPLO, "--dry-run");
  afirmar(
    ensayo.codigo === 0 && /SE AMPLIARÍA\s+DLQ_CLIENTES\s+\+ dlq\.state\.clientes\.>/.test(ensayo.salida) && /1 se ampliaría/.test(ensayo.salida),
    "--dry-run con una DLQ de la 1.0 dice «se ampliaría» (+ dlq.state.clientes.>) y sale con 0",
    ensayo.salida,
  );
  afirmar(JSON.stringify((await opsA.jsm.streams.info("DLQ_CLIENTES")).config.subjects) === '["dlq.clientes.>"]', "…y no la toca");
  const amplia = provision(EJEMPLO);
  const dlqAmpliada = await opsA.jsm.streams.info("DLQ_CLIENTES");
  afirmar(
    amplia.codigo === 0 && /AMPLIADO\s+DLQ_CLIENTES\s+\+ dlq\.state\.clientes\.>/.test(amplia.salida) &&
      JSON.stringify(dlqAmpliada.config.subjects) === '["dlq.clientes.>","dlq.state.clientes.>"]' && dlqAmpliada.state.messages === mensajesAntes,
    `provision AMPLÍA DLQ_CLIENTES con dlq.state.clientes.> sin perder mensajes (${mensajesAntes} antes y después) y sale con 0`,
    amplia.salida,
  );
  const tras = provision(EJEMPLO);
  afirmar(tras.codigo === 0 && new RegExp(`${nRecursos} sin cambios`).test(tras.salida), "y repetir vuelve a ser «sin cambios»: la ampliación es idempotente");

  // Lo que NO es una ampliación se informa y no se toca: una DLQ con un subject de más, y un stream de
  // estado con allow_direct a false (sin él, get y el paso 1 de put no reciben respuesta: A-32).
  await opsA.jsm.streams.update("DLQ_CLIENTES", { subjects: ["dlq.clientes.>", "dlq.state.clientes.>", "dlq.otro.>"] });
  await opsA.jsm.streams.update("STATE_CLIENTES", { allow_direct: false });
  const rara = provision(EJEMPLO);
  afirmar(
    rara.codigo === 1 && /DIFIERE\s+DLQ_CLIENTES\s*\n\s+subjects:/.test(rara.salida) && /DIFIERE\s+STATE_CLIENTES\s*\n\s+allow_direct: existente false, deseado true/.test(rara.salida) && !/AMPLIADO/.test(rara.salida),
    "una DLQ con un subject DE MÁS y un STATE_ con allow_direct false se informan (DIFIERE), no se amplían y la salida es 1",
    rara.salida,
  );
  afirmar(
    (await opsA.jsm.streams.info("STATE_CLIENTES")).config.allow_direct === false && (await opsA.jsm.streams.info("DLQ_CLIENTES")).config.subjects.includes("dlq.otro.>"),
    "…y NO sobrescribe: ambos siguen como estaban",
  );
  await opsA.jsm.streams.update("STATE_CLIENTES", { allow_direct: true });
  await opsA.jsm.streams.update("DLQ_CLIENTES", { subjects: ["dlq.clientes.>", "dlq.state.clientes.>"] });
  const restituido = provision(EJEMPLO);
  afirmar(restituido.codigo === 0 && /Sin cambios/.test(restituido.salida), "restituidos, vuelve a «sin cambios»");

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
    if (!svc.owns.length) continue; // un observador de estado puro no tiene dominio propio: su ciclo es el de la 3b
    const nc = await comoServicio(A.url, svc.name, pares[svc.name].seed);
    const r = await ciclo({ nc, svc, ops: opsA, subjectPropio: subjectPropio(svc), nakTerm: true });
    console.log(`  ${svc.name}  (dueño de ${svc.owns.join(", ")}; consume ${svc.consumes.join(", ")})`);
    for (const p of r.pasos) afirmar(p.ok, `    ${p.nombre}`, p.ok ? undefined : p.detalle);
    if (svc.name === "facturacion-api") ackVisto = r.ack;
    await nc.close();
  }

  // ── 3b. El ciclo de ESTADO ─────────────────────────────────────────────────
  seccion("3b. El ciclo de ESTADO de cada rol, con sus permisos generados (10-state.md: put, get, watch, DLQ de estado)");
  for (const svc of json.filter((x) => x.stateOwns.length || x.watches.length)) {
    const nc = await comoServicio(A.url, svc.name, pares[svc.name].seed);
    const marca = randomUUID().slice(0, 8);
    const rol = svc.stateOwns.length ? `dueño del estado de ${svc.stateOwns.join(", ")}` : `observa ${svc.watches.map((w) => `${w.contract} (${w.tenants ? w.tenants.join(", ") : "todos los tenants"})`).join(", ")}`;
    console.log(`  ${svc.name}  (${rol})`);
    const r = svc.stateOwns.length ? await cicloEstadoDueño({ nc, svc, ops: opsA, marca }) : await cicloEstadoObservador({ nc, svc, ops: opsA, marca, exhaustivo: true });
    for (const p of r.pasos) afirmar(p.ok, `    ${p.nombre}`, p.ok ? undefined : p.detalle);
    afirmar(nc.violaciones.length === 0, `    ni una sola violación de permiso en toda la conversación de ${svc.name}`, nc.violaciones.join("\n"));
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

  // ── 4c. Petición/respuesta: la matriz de sondas × servicios ───────────────────────────────────
  seccion("4c. Petición/respuesta: quién puede llamar, atender o publicar sobre un inbox, y quién NO (matriz de sondas × servicios)");
  {
    const sondasR = sondasRpc(randomUUID().slice(0, 8));
    const usuarios = ["crm-api", "facturacion-api", "pedidos-api", "logistica-api", "tienda-web"];
    const desvios = new Map(sondasR.map(([que]) => [que, []]));
    for (const u of usuarios) {
      const nc = await comoServicio(A.url, u, pares[u].seed);
      for (const d of await desviosDeSondasEstado(nc, u, sondasR)) desvios.get(d.que).push(`${u}: esperado ${d.esperado ? "permitido" : "denegado"}, ${d.permitido === true ? "PERMITIDO" : d.permitido === false ? "denegado" : d.permitido}`);
      await nc.close();
    }
    for (const [que, , para] of sondasR) {
      afirmar(desvios.get(que).length === 0, `${que}  →  ${para.length ? `solo ${para.join(", ")}` : "NADIE"}`, desvios.get(que).join("\n"));
    }
    nota(`${sondasR.length} sondas × ${usuarios.length} servicios (el dueño crm-api, los que llaman facturacion-api y pedidos-api, y logistica-api y tienda-web, que no tienen nada de rpc)`);

    // «Una petición, una respuesta»: crm-api contesta DOS veces; llega una y el broker deniega la otra (allow_responses max 1).
    const nc = await comoServicio(A.url, "crm-api", pares["crm-api"].seed);
    const op = `rpc.${OP_OBTENER}`;
    const inbox = `_INBOX.verify-rpc.${randomUUID().slice(0, 8)}`;
    const recibidas = [];
    const escucha = opsA.nc.subscribe(inbox);
    (async () => { for await (const m of escucha) recibidas.push(dec.decode(m.data)); })();
    const dueñoSub = nc.subscribe(op, { queue: "crm-api" });
    (async () => { try { for await (const m of dueñoSub) { m.respond(enc.encode('{"n":1}')); m.respond(enc.encode('{"n":2}')); } } catch { /* denegada */ } })();
    await nc.flush();
    await opsA.nc.flush();
    opsA.nc.publish(op, enc.encode("{}"), { reply: inbox });
    await opsA.nc.flush();
    await sleep(500);
    afirmar(recibidas.length === 1, "«una petición, una respuesta»: crm-api contesta DOS veces y llega UNA (allow_responses max 1)", `llegaron ${recibidas.length}: ${recibidas.join(" ")}`);
    afirmar(
      nc.violaciones.filter((v) => v.includes(`Publish to "${inbox}"`)).length === 1,
      "…y el broker deniega la segunda, con un aviso `Permissions Violation` en la conexión del dueño (es la única forma de verlo: A-19, A-28)",
      nc.violaciones.join("\n"),
    );
    dueñoSub.unsubscribe();
    escucha.unsubscribe();
    await nc.close();
  }

  // ── 4b. Estado: lo que NO se puede hacer ──────────────────────────────────
  seccion("4b. Estado: quién puede y quién NO puede cada cosa (escribir, leer, el stream, los durables ajenos, la DLQ)");
  {
    const sondasE = sondasEstado(randomUUID().slice(0, 8));
    const usuarios = ["crm-api", "tienda-web", "informes-api", "facturacion-api"];
    const desvios = new Map(sondasE.map(([que]) => [que, []]));
    for (const u of usuarios) {
      const nc = await comoServicio(A.url, u, pares[u].seed);
      for (const d of await desviosDeSondasEstado(nc, u, sondasE)) desvios.get(d.que).push(`${u}: esperado ${d.esperado ? "permitido" : "denegado"}, ${d.permitido === true ? "PERMITIDO" : d.permitido === false ? "denegado" : d.permitido}`);
      await nc.close();
    }
    for (const [que, , para] of sondasE) {
      afirmar(desvios.get(que).length === 0, `${que}  →  ${para.length ? `solo ${para.join(", ")}` : "NADIE"}`, desvios.get(que).join("\n"));
    }
    nota(`${sondasE.length} sondas × ${usuarios.length} servicios (crm-api, tienda-web, informes-api y facturacion-api, que no tiene estado)`);
    // Los consumidores y los mensajes que las sondas hayan podido crear quedan fuera del camino de lo demás.
    for (const n of ["espia__state", "espia2__state"]) await opsA.jsm.consumers.delete(STREAM_ESTADO, n).catch(() => {});
  }

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

  // ── 5b. Minimalidad del estado ─────────────────────────────────────────────
  seccion("5b. Minimalidad del estado: sin CUALQUIERA de los permisos de tienda-web (observador) o crm-api (dueño), su ciclo se rompe");
  for (const x of variantesTienda) {
    const nc = await comoServicio(A.url, x.nombre, x.seed, "tienda-web");
    const r = await cicloEstadoObservador({ nc, svc: tiendaP, ops: opsA, marca: randomUUID().slice(0, 8) });
    await nc.close();
    const corto = r.fallidos.length > 2 ? `${r.fallidos.slice(0, 2).join("; ")} (+${r.fallidos.length - 2} más)` : r.fallidos.join("; ");
    afirmar(!r.ok, `tienda-web sin ${x.quitado} → el ciclo falla en: ${corto || "(nada: el permiso sobra)"}`);
  }
  const svcCrm = crmP;
  for (const x of variantesCrm) {
    const nc = await comoServicio(A.url, x.nombre, x.seed, "crm-api");
    // El ciclo de eventos de crm-api (dominio propio y su DLQ) más el de estado: cada permiso, en el suyo.
    const eventos = await ciclo({ nc, svc: svcCrm, ops: opsA, subjectPropio: subjectPropio(svcCrm), rpc: false });
    const estado = await cicloEstadoDueño({ nc, svc: svcCrm, ops: opsA, marca: randomUUID().slice(0, 8) });
    await nc.close();
    const fallidos = [...eventos.fallidos, ...estado.fallidos];
    const corto = fallidos.length > 2 ? `${fallidos.slice(0, 2).join("; ")} (+${fallidos.length - 2} más)` : fallidos.join("; ");
    afirmar(fallidos.length > 0, `crm-api sin ${x.quitado} → el ciclo falla en: ${corto || "(nada: el permiso sobra)"}`);
  }

  // ── 5c. Minimalidad de la petición/respuesta del dueño ─────────────────────────────────────────
  seccion("5c. Minimalidad de la petición/respuesta: sin la suscripción `rpc.clientes.>` o sin `allow_responses`, el dueño se rompe");
  for (const x of [crmSinSub, crmSinAllow]) {
    const nc = await comoServicio(A.url, x.nombre, x.seed, "crm-api");
    const pasos = [];
    await cicloRpc({
      nc,
      svc: crmP,
      ops: opsA,
      paso: async (nombre, fn) => {
        try {
          await conTiempo(fn(), 6000, nombre);
          pasos.push({ nombre, ok: true });
        } catch (e) {
          pasos.push({ nombre, ok: false, detalle: resumen(e) });
        }
      },
    });
    await nc.close();
    const fallidos = pasos.filter((p) => !p.ok);
    afirmar(
      fallidos.length === pasos.length && pasos.length === 2,
      `crm-api sin ${x.quitado} → sus ${pasos.length} operaciones fallan: ${fallidos[0]?.detalle ?? "(nada: el permiso sobra)"}`,
      pasos.map((p) => `${p.ok ? "ok" : "FALLA"}: ${p.nombre}`).join("\n"),
    );
  }
  nota("quien llama (facturacion-api) ya está en la sección 5: su `ciclo` incluye request() y quitar su permiso de publicación sobre una operación, o su inbox, lo rompe");

  // A-109: `allow_responses` admite UNA publicación al `reply` de lo que el servicio recibe, y el `reply` de una entrega de un pull
  // consumer es su subject de ack. Con él, el dueño confirma una entrega aunque le falte `$JS.ACK…`; el permiso explícito sigue
  // siendo necesario (`working()` seguido de `ack()` publica DOS veces al mismo `reply`), y se concede igual.
  {
    const [stream, durable, filtro] = ["DLQ_CLIENTES", "crm-api__dlq_clientes", "dlq.clientes.>"];
    const nc = await comoServicio(A.url, crmSinAckConAllow.nombre, crmSinAckConAllow.seed, "crm-api");
    const probar = async (accion) => {
      await reiniciarDurable(opsA, stream, durable, filtro);
      await opsA.js.publish("dlq.clientes.cosa.v1.muerto", enc.encode('{"muerto":1}'));
      const c = await jetstream(nc).consumers.get(stream, durable);
      const m = await recibirUno(c, 2500);
      const antes = nc.violaciones.length;
      await accion(m);
      await nc.flush();
      await sleep(400);
      return { m, pendientes: (await opsA.jsm.consumers.info(stream, durable)).num_ack_pending, violaciones: nc.violaciones.slice(antes) };
    };
    const solo = await probar((m) => m.ack());
    afirmar(
      !!solo.m && solo.pendientes === 0 && solo.violaciones.length === 0,
      "A-109: crm-api CON allow_responses y SIN `$JS.ACK…` confirma una entrega: el `reply` de la entrega admite UNA publicación (por eso 5b mide sus permisos de ack sin allow_responses)",
      JSON.stringify({ pendientes: solo.pendientes, violaciones: solo.violaciones }),
    );
    const doble = await probar(async (m) => {
      m.working();
      await nc.flush();
      await sleep(200);
      m.ack();
    });
    afirmar(
      !!doble.m && doble.pendientes === 1 && doble.violaciones.some((v) => v.includes("$JS.ACK")),
      "…pero `working()` seguido de `ack()` publica DOS veces al mismo `reply`: la segunda se deniega y la entrega queda pendiente. El permiso `$JS.ACK…` sigue siendo necesario",
      JSON.stringify({ pendientes: doble.pendientes, violaciones: doble.violaciones }),
    );
    await nc.close();
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

  // ── 6b. Mutantes de estado ─────────────────────────────────────────────────
  seccion("6b. Las sondas de ESTADO detectan una ACL que concede DE MÁS: mutantes sembrados");
  {
    const sondasE = sondasEstado(randomUUID().slice(0, 8));
    for (const m of mutantesEstado) {
      const nc = await comoServicio(A.url, m.nombre, m.seed, m.base);
      const desvios = await desviosDeSondasEstado(nc, m.base, sondasE);
      await nc.close();
      for (const que of m.flip) {
        const d = desvios.find((x) => x.que.includes(que));
        afirmar(!!d && d.permitido === true && d.esperado === false, `mutante «${m.que}»: la sonda «${que}» lo DETECTA (permitido)`, d ? JSON.stringify(d) : "la sonda no cambió");
      }
      const inesperados = desvios.filter((d) => !m.flip.some((q) => d.que.includes(q)));
      afirmar(inesperados.length === 0, `mutante «${m.que}»: no abre NADA más de lo que dice`, inesperados.map((d) => d.que).join("\n"));
    }
    for (const n of ["espia__state", "espia2__state"]) await opsA.jsm.consumers.delete(STREAM_ESTADO, n).catch(() => {});
  }

  // ── 6c. Mutantes de petición/respuesta ────────────────────────────────────
  seccion("6c. Las sondas de PETICIÓN/RESPUESTA detectan una ACL que concede DE MÁS: mutantes sembrados");
  {
    const sondasR = sondasRpc(randomUUID().slice(0, 8));
    for (const m of mutantesRpc) {
      const nc = await comoServicio(A.url, m.nombre, m.seed, m.base);
      const desvios = await desviosDeSondasEstado(nc, m.base, sondasR);
      await nc.close();
      for (const que of m.flip) {
        const d = desvios.find((x) => x.que.includes(que));
        afirmar(!!d && d.permitido === true && d.esperado === false, `mutante «${m.que}»: la sonda «${que}» lo DETECTA (permitido)`, d ? JSON.stringify(d) : "la sonda no cambió");
      }
      const inesperados = desvios.filter((d) => !m.flip.some((q) => d.que.includes(q)));
      afirmar(inesperados.length === 0, `mutante «${m.que}»: no abre NADA más de lo que dice`, inesperados.map((d) => d.que).join("\n"));
    }
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
  const nRecursosB =
    2 * new Set(json.flatMap((s) => s.owns)).size + new Set(json.flatMap((s) => s.stateOwns)).size + json.reduce((n, s) => n + s.durables.length, 0);
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
    afirmar(r.status === 0 && new RegExp(`${nRecursosB} creado`).test(r.stdout), `provision en la account de ${t} (una vez por account)`, r.stdout + r.stderr);
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

  // Petición/respuesta: cada account tiene SU espacio `rpc.`. crm-api@acme atiende; facturacion-api@acme recibe la respuesta y
  // facturacion-api@globex —el mismo servicio, con los mismos permisos— solo ve `no responders`: nadie atiende en su account.
  {
    const op = `rpc.${OP_OBTENER}`;
    const dueñoAcme = await comoServicio(D.url, "crm-api@acme", pares["crm-api@acme"].seed, "crm-api");
    const sub = dueñoAcme.subscribe(op, { queue: "crm-api" });
    (async () => { try { for await (const m of sub) m.respond(enc.encode('{"ok":true}')); } catch { /* denegada */ } })();
    await dueñoAcme.flush();
    const llama = async (t) => {
      const n = await comoServicio(D.url, `facturacion-api@${t}`, pares[`facturacion-api@${t}`].seed, "facturacion-api");
      const t0 = Date.now();
      const r = await n.request(op, enc.encode("{}"), { timeout: 1500 }).then(() => "respondida", (e) => (/no responders/i.test(e?.message ?? "") ? "no responders" : `error: ${resumen(e)}`));
      await n.close();
      return { r, ms: Date.now() - t0 };
    };
    const a = await llama("acme");
    const g = await llama("globex");
    afirmar(a.r === "respondida", "Modelo B: facturacion-api@acme llama a crm-api@acme y recibe la respuesta", JSON.stringify(a));
    afirmar(g.r === "no responders" && g.ms < 500, "…y facturacion-api@globex solo ve `no responders` en ms: el espacio `rpc.` es de la account, y nadie atiende en la de globex", JSON.stringify(g));
    sub.unsubscribe();
    await dueñoAcme.close();
  }
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
