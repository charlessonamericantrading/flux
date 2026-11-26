/**
 * `flux state` — mirar y recuperar el estado replicado (specification/10-state.md).
 *
 *   flux state ls <contrato> [--tenant t] [--json]
 *   flux state get <contrato> <tenant> <clave> [--show-data] [--json]
 *   flux state resync <servicio> <contrato> [--tenant t] [--confirm] [--yes]
 *
 * Las tres operaciones son de OPERACIÓN y usan credenciales de operación, no las de un
 * servicio: `ls` necesita `STREAM.INFO.STATE_<DOM>`, que **no** se concede a los observadores
 * porque lista todas las claves y todos los tenants (A-39); y `resync` borra un durable, que un
 * servicio no puede hacer (06-security.md §3.5).
 *
 * Hablan NATS directamente, sin el SDK de flux: una herramienta de diagnóstico construida sobre
 * el SDK no puede diagnosticar los fallos del SDK.
 */

import { createInterface } from "node:readline/promises";
import { JetStreamApiError } from "@nats-io/jetstream";

import { c, OK, FAIL, WARN, die, fmtAge, fmtBytes, fmtDuration, table } from "./ui.mjs";
import { datosOcultos, redactarSobre } from "./privacidad.mjs";
import {
  CAMPOS_DURABLE,
  ErrorEstado,
  configDurableEstado,
  diferenciasConfig,
  durableDe,
  filtroDe,
  inspeccionar,
  parsearSubject,
  streamDe,
  subjectDe,
  validarClave,
  validarContrato,
  validarServicio,
  validarTenant,
} from "./state-model.mjs";

const SIMULTANEAS = 32;

// `status` es el 404 de la API; `code`, el err_code (10059 stream, 10014 consumidor). Un permiso
// denegado o un timeout NO son «no existe»: tratarlos así enterraría el error real.
const noExiste = (e) => e instanceof JetStreamApiError && e.status === 404;

// ─── Lectura directa ─────────────────────────────────────────────────────────────────────────────

/** Nadie contestó a un DIRECT.GET: el stream no existe, o no tiene `allow_direct` (A-32). */
class SinRespuesta extends Error {}

/**
 * `DIRECT.GET` por subject (10-state.md §5.1): `null` si no hay valor (404); si no, el mensaje
 * tal cual, con la secuencia y la hora de escritura que pone el broker en las cabeceras.
 * No consume nada: no hay consumidor ni acuse.
 *
 * Se hace con una petición cruda y no con la librería de JetStream: es lo que hace el SDK y lo
 * que mide la suite, y así la CLI ve exactamente lo que vería un servicio.
 */
async function leerDirecto(nc, stream, subject, timeout = 3000) {
  let r;
  try {
    r = await nc.request(`$JS.API.DIRECT.GET.${stream}.${subject}`, "", { timeout });
  } catch (e) {
    if (/no responders/i.test(e.message) || e.cause?.name === "NoRespondersError") throw new SinRespuesta(e.message);
    throw e;
  }
  const h = r.headers;
  if (h?.code === 404) return null;
  if (h?.hasError) throw new Error(`DIRECT.GET respondió ${h.code} ${h.description}`);
  return {
    seq: Number(h.get("Nats-Sequence")),
    time: h.get("Nats-Time-Stamp"),
    data: r.data,
  };
}

/**
 * «Sin respuesta» se ve igual con un stream que no existe y con uno sin `allow_direct`
 * (A-32). Un operador sí puede distinguirlos —`STREAM.INFO`—, así que se le dice cuál es.
 */
async function explicaSinRespuesta(jsm, stream) {
  try {
    const info = await jsm.streams.info(stream);
    if (!info.config.allow_direct) {
      return `el stream ${stream} existe pero NO tiene allow_direct: DIRECT.GET no contesta y ni \`get\` ni \`put\` pueden leer. Ejecuta \`flux doctor\` (10-state.md §2.1, A-32).`;
    }
    return `nadie contestó a DIRECT.GET aunque ${stream} existe y tiene allow_direct: ¿JetStream sin quórum, o el servidor que lo atiende cayó? Ejecuta \`flux doctor\`.`;
  } catch (e) {
    if (noExiste(e)) {
      return `el stream ${stream} no existe: el dominio no tiene estado aprovisionado (\`flux provision\`, 10-state.md §2.1).`;
    }
    throw e;
  }
}

async function streamInfoOError(jsm, stream, opciones) {
  try {
    return await jsm.streams.info(stream, opciones);
  } catch (e) {
    if (noExiste(e)) {
      throw new ErrorEstado(
        null,
        `no existe el stream ${stream}: el dominio no tiene estado aprovisionado ` +
          `(\`flux provision\`; 10-state.md §2.1). Con \`flux doctor\` se ve qué streams hay.`,
      );
    }
    throw e;
  }
}

/** Ejecuta `fn` sobre cada elemento con como mucho `n` a la vez, conservando el orden. */
async function enConcurrencia(items, n, fn) {
  const out = new Array(items.length);
  let siguiente = 0;
  const trabajador = async () => {
    while (siguiente < items.length) {
      const i = siguiente++;
      out[i] = await fn(items[i], i);
    }
  };
  await Promise.all(Array.from({ length: Math.min(n, items.length) }, trabajador));
  return out;
}

const porNatural = new Intl.Collator("es", { numeric: true }).compare;

// ─── ls ──────────────────────────────────────────────────────────────────────────────────────────

/**
 * Lista las claves de un contrato: **una** petición `STREAM.INFO` con `subjects_filter` da
 * todos los subjects (con sus tenants), y una lectura directa por clave da la revisión, el
 * tamaño y si es una lápida. Es el único sitio donde se ven las claves de todos los tenants a
 * la vez, y por eso necesita credenciales de operación (A-39).
 *
 * Se filtra por `state.<contrato>.>` y no por `….*.*` a propósito: así aparecen también las
 * entradas que el broker acepta y NINGÚN observador recibe —una clave `a.b` son dos tokens
 * (A-41)— y se marcan en vez de esconderse.
 */
export async function stateLs(nc, jsm, contratoTxt, { tenant, jsonOut, limit } = {}) {
  const { contrato, dom } = validarContrato(contratoTxt);
  if (tenant !== undefined) validarTenant(tenant);
  const stream = streamDe(dom);
  const filtro = tenant ? `state.${contrato}.${tenant}.>` : `state.${contrato}.>`;

  const info = await streamInfoOError(jsm, stream, { subjects_filter: filtro });
  const todos = Object.keys(info.state.subjects ?? {}).sort(porNatural);
  const subjects = Number.isInteger(limit) && limit > 0 ? todos.slice(0, limit) : todos;

  let filas;
  try {
    filas = await enConcurrencia(subjects, SIMULTANEAS, async (subject) => {
      const p = parsearSubject(subject);
      if (!p) {
        const t = subject.split(".");
        return {
          subject, tenant: t[4] ?? "?", key: t.slice(5).join("."), revision: null, size: null, stored: null,
          aggregateVersion: null, deleted: false,
          problemas: [{ code: null, texto: "subject no canónico: ningún observador lo recibe (A-41)" }],
        };
      }
      const m = await leerDirecto(nc, stream, subject);
      if (!m) return null; // borrada entre el STREAM.INFO y esta lectura
      const ins = inspeccionar(m.data, p);
      return {
        subject, tenant: p.tenant, key: p.clave, revision: m.seq, size: m.data.length, stored: m.time,
        aggregateVersion: ins.aggregateVersion ?? null, deleted: ins.lapida, problemas: ins.problemas,
      };
    });
  } catch (e) {
    if (e instanceof SinRespuesta) throw new ErrorEstado(null, await explicaSinRespuesta(jsm, stream));
    throw e;
  }
  filas = filas.filter(Boolean).sort((a, b) => porNatural(a.tenant, b.tenant) || porNatural(a.key, b.key));

  if (jsonOut) {
    console.log(JSON.stringify(
      filas.map(({ problemas, ...f }) => ({ ...f, problems: problemas.map((p) => p.code ?? p.texto) })),
      null,
      2,
    ));
    return 0;
  }

  if (filas.length === 0) {
    console.log(
      `${OK} ${contrato}${tenant ? ` · tenant ${tenant}` : ""}: no hay ninguna clave en ${stream}` +
        (tenant ? c.gray("\n  Sin --tenant se listan todos los tenants.") : ""),
    );
    return 0;
  }

  const filasTabla = filas.map((f) => [
    f.tenant,
    f.key,
    f.revision === null ? c.gray("—") : String(f.revision),
    f.aggregateVersion === null ? c.gray("—") : String(f.aggregateVersion),
    f.size === null ? c.gray("—") : fmtBytes(f.size),
    f.stored ? fmtAge(f.stored) : c.gray("—"),
    f.problemas.length
      ? c.red([...new Set(f.problemas.map((p) => p.code ?? "ANÓMALA"))].join(", "))
      : f.deleted
        ? c.yellow("LÁPIDA")
        : c.green("ok"),
  ]);
  console.log(table(filasTabla, ["TENANT", "CLAVE", "REVISIÓN", "VERSIÓN", "TAMAÑO", "ESCRITO", "ESTADO"]));

  const lapidas = filas.filter((f) => f.deleted).length;
  const conProblemas = filas.filter((f) => f.problemas.length);
  const tenants = new Set(filas.map((f) => f.tenant)).size;
  console.log(
    c.gray(
      `\n${filas.length} clave(s) de ${contrato} en ${stream} · ${tenants} tenant(s) · ${lapidas} lápida(s)` +
        (subjects.length < todos.length ? ` · mostrando ${subjects.length} de ${todos.length} (--limit)` : ""),
    ),
  );
  if (lapidas) {
    console.log(c.gray("  Una lápida es una clave borrada: sigue en el stream (permanente en 1.1, 10-state.md §4) y `get` del SDK da null."));
  }
  if (conProblemas.length) {
    console.log(`\n${WARN} ${conProblemas.length} entrada(s) anómala(s): un observador las trataría como POISON o no las recibiría (10-state.md §5.4).`);
    for (const f of conProblemas.slice(0, 10)) {
      for (const p of f.problemas) console.log(`  ${c.bold(f.subject)}  ${p.code ? `${p.code}: ` : ""}${p.texto}`);
    }
  }
  console.log(c.gray(`  Detalle de una clave: flux state get ${contrato} <tenant> <clave>`));
  return 0;
}

// ─── get ─────────────────────────────────────────────────────────────────────────────────────────

/**
 * Una clave: `DIRECT.GET` por subject, tal cual lo hace `state.get` del SDK, pero mostrando el
 * sobre ENTERO y sin traducir una lápida a `null`: el operador necesita ver que está borrada.
 * El `data` de un valor `confidential` o `restricted` no se imprime sin `--show-data`.
 */
export async function stateGet(nc, jsm, [contratoTxt, tenantTxt, claveTxt], { showData, jsonOut } = {}) {
  const { contrato, dom } = validarContrato(contratoTxt);
  const tenant = validarTenant(tenantTxt);
  const clave = validarClave(claveTxt);
  const stream = streamDe(dom);
  const subject = subjectDe(contrato, tenant, clave);

  let m;
  try {
    m = await leerDirecto(nc, stream, subject);
  } catch (e) {
    if (e instanceof SinRespuesta) throw new ErrorEstado(null, await explicaSinRespuesta(jsm, stream));
    throw e;
  }
  if (!m) {
    throw new ErrorEstado(
      null,
      `404: no hay valor en ${subject}\n` +
        `  La clave nunca se escribió con ese tenant, o se purgó. El tenant es parte de la clave: «${clave}» de ` +
        `otro tenant es otra entrada.\n  ${c.gray(`flux state ls ${contrato}`)}${c.gray("  lista todas las claves y tenants")}`,
    );
  }

  const ins = inspeccionar(m.data, parsearSubject(subject));
  const e = ins.envelope;
  const oculto = ins.legible && datosOcultos(e.dataclassification) && !showData;
  const mostrado = ins.legible ? redactarSobre(e, showData) : null;

  if (jsonOut) {
    console.log(JSON.stringify({
      subject,
      revision: m.seq,
      stored: m.time,
      size: m.data.length,
      deleted: ins.lapida,
      aggregateVersion: ins.aggregateVersion ?? null,
      problems: ins.problemas.map((p) => p.code ?? p.texto),
      envelope: mostrado,
    }));
    return 0;
  }

  console.log(c.bold(subject));
  console.log(`  revisión       ${m.seq}`);
  console.log(`  escrito        ${m.time}  ${c.gray(fmtAge(m.time))}`);
  console.log(`  versión        ${ins.aggregateVersion ?? c.red("sin aggregateVersion válida")}`);
  console.log(`  tamaño         ${fmtBytes(m.data.length)}`);
  if (ins.legible) {
    console.log(
      `  clasificación  ${e.dataclassification ?? c.red("ausente")}` +
        (oculto ? `  ${c.yellow("data oculto")} ${c.gray("— usa --show-data si de verdad lo necesitas")}` : ""),
    );
  }
  if (ins.lapida) {
    console.log(
      `\n${WARN} ${c.yellow("LÁPIDA")}: la clave está borrada desde la revisión ${m.seq} (versión ${ins.aggregateVersion ?? "?"}). ` +
        `\`state.get\` del SDK devuelve null. Se recrea con una versión estrictamente mayor (10-state.md §4).`,
    );
  }
  for (const p of ins.problemas) {
    console.log(`\n${FAIL} ${p.code ? c.bold(p.code) + ": " : ""}${p.texto}`);
  }
  if (!ins.legible) {
    // El cuerpo podría llevar el dato: no se vuelca sin --show-data (es lo que hace la spec con la DLQ).
    console.log(
      showData
        ? `\n${c.gray(new TextDecoder().decode(m.data).slice(0, 500))}`
        : c.gray("  (cuerpo no mostrado: podría llevar datos; --show-data lo imprime)"),
    );
    return 0;
  }
  console.log(`\n${JSON.stringify(mostrado, null, 2)}`);
  return 0;
}

// ─── resync ──────────────────────────────────────────────────────────────────────────────────────

const legible = (campo, v) => {
  if (v === undefined || v === null) return c.gray("(sin valor)");
  if (campo === "ack_wait") return fmtDuration(v);
  if (campo === "backoff") return `[${v.map((x) => fmtDuration(x)).join(", ")}]`;
  return String(v);
};

/**
 * Recuperación de un observador que perdió su copia (o cuyo handler descartó entradas): borra
 * y RECREA su durable con la configuración exacta de 10-state.md §5.2, y el broker vuelve a
 * entregar el último valor de cada clave (A-37).
 *
 * Es lo que sustituye a `dlq replay` para el estado: la DLQ de estado guarda el *fallo*, no el
 * dato (§6), y republicar un valor viejo pisaría uno nuevo. Esto entrega lo ACTUAL.
 *
 * Reglas, todas con su razón:
 *   · SIMULACIÓN por defecto, como `dlq replay`. `--confirm` ejecuta.
 *   · NUNCA toca el stream ni una clave: solo el durable de UN servicio (y de un tenant).
 *   · NO crea un durable que no existe. Un nombre mal escrito dejaría un durable fantasma que
 *     acumula pendientes sin que nadie lo lea; si falta, se dice y se enseñan los que hay.
 */
export async function stateResync(jsm, [servicioTxt, contratoTxt], { tenant, confirm, yes } = {}) {
  const servicio = validarServicio(servicioTxt);
  const { contrato, dom } = validarContrato(contratoTxt);
  if (tenant !== undefined) validarTenant(tenant);
  const stream = streamDe(dom);
  const durable = durableDe(servicio, contrato, tenant);
  const filtro = filtroDe(contrato, tenant);
  const deseada = configDurableEstado(durable, filtro);
  const orden = `flux state resync ${servicio} ${contrato}${tenant ? ` --tenant ${tenant}` : ""}`;

  const info = await streamInfoOError(jsm, stream, { subjects_filter: filtro });
  const claves = Object.keys(info.state.subjects ?? {}).length;

  let actual;
  try {
    actual = await jsm.consumers.info(stream, durable);
  } catch (e) {
    if (!noExiste(e)) throw e;
    const suyos = [];
    for await (const k of jsm.consumers.list(stream)) {
      if (k.name.startsWith(`${servicio}__state_`)) suyos.push(k.name);
    }
    throw new ErrorEstado(
      null,
      `no existe el durable ${durable} en ${stream}\n` +
        `  resync recrea un durable que existe; no inventa uno (un nombre mal escrito dejaría un durable fantasma).\n` +
        (suyos.length
          ? `  Durables de estado de «${servicio}» en ${stream}:\n${suyos.map((n) => `    ${n}`).join("\n")}\n` +
            `  ¿Falta (o sobra) --tenant? El tenant va en el nombre del durable (10-state.md §5.2).`
          : `  «${servicio}» no tiene ningún durable de estado en ${stream}: ¿el nombre del servicio? ¿se aprovisionó (\`flux provision\`)?`),
    );
  }

  const difs = diferenciasConfig(deseada, actual.config, CAMPOS_DURABLE);
  const conectados = actual.num_waiting ?? 0;

  console.log(`${c.bold(orden)}${confirm ? "" : c.yellow("   SIMULACIÓN")}`);
  console.log(`  stream      ${stream}`);
  console.log(`  durable     ${durable}`);
  console.log(`  filtro      ${filtro}`);
  console.log(
    `  hoy         pendientes ${actual.num_pending} · en vuelo ${actual.num_ack_pending} · reentregas ${actual.num_redelivered}` +
      ` · última entrega: secuencia ${actual.delivered?.stream_seq ?? 0}` +
      (conectados ? ` · ${c.yellow(`${conectados} petición(es) de pull esperando: hay un watch conectado`)}` : ""),
  );
  console.log(`  claves      ${claves} en el filtro: se entregarán otra vez, lápidas incluidas`);
  if (difs.length) {
    console.log(`\n  El durable actual NO tiene la configuración canónica (se recreará con ella):`);
    for (const d of difs) {
      console.log(`    ${c.yellow(d.campo)}: existente ${legible(d.campo, d.existente)}, canónico ${legible(d.campo, d.deseado)}`);
    }
  }
  console.log(
    `\n  ${c.bold(confirm ? "Hará, por este orden:" : "Haría, por este orden:")}\n` +
      `    1. BORRAR el durable: el broker olvida hasta dónde llegó (entregados, en vuelo, ack_floor).\n` +
      `    2. RECREARLO con deliver_policy=last_per_subject y la configuración canónica de\n` +
      `       03-delivery.md §2: el broker entrega otra vez el último valor de cada clave (A-37).\n` +
      `  ${c.bold("No toca:")} el stream, ni una clave, ni los durables de otros servicios o tenants.\n` +
      `  ${c.bold("Efecto en el servicio:")} su handler recibirá otra vez entradas que ya aplicó, así que DEBE ser\n` +
      `  idempotente (UPDATE … WHERE aggregate_version < $n). \`onSynced\` no se vuelve a invocar.`,
  );

  if (!confirm) {
    console.log(
      `\n${c.bold("Simulación.")} No se ha cambiado nada. Para ejecutarlo de verdad:\n  ${c.cyan(`${orden} --confirm`)}`,
    );
    return 0;
  }

  if (!yes) {
    if (!process.stdin.isTTY) {
      throw new ErrorEstado(null, "sin terminal interactiva no se puede pedir confirmación: añade --yes si de verdad quieres ejecutarlo.");
    }
    const rl = createInterface({ input: process.stdin, output: process.stdout });
    const r = await rl.question(`\nBorrar y recrear ${durable}? [escribe "si"] `);
    rl.close();
    if (r.trim().toLowerCase() !== "si") {
      console.log(`${WARN} cancelado`);
      return 1;
    }
  }

  await jsm.consumers.delete(stream, durable);
  console.log(`\n  ${OK} durable borrado`);

  // Entre el borrado y la creación el servicio no tiene durable: si la creación falla, que no
  // quede así en silencio. Se reintenta, y si no se puede se dice cómo salir.
  let creado;
  let ultimo;
  for (let intento = 1; intento <= 3 && !creado; intento++) {
    try {
      creado = await jsm.consumers.add(stream, deseada);
    } catch (e) {
      ultimo = e;
      await new Promise((r) => setTimeout(r, 300 * intento));
    }
  }
  if (!creado) {
    throw new ErrorEstado(
      null,
      `el durable ${durable} SE BORRÓ pero no se pudo recrear: ${ultimo?.message}\n` +
        `  Hasta que exista, el servicio falla con NOT_PROVISIONED. Recréalo con \`flux provision\` o repite este comando.`,
    );
  }

  // Lo que el servidor devuelve no es siempre lo que se le pide (ack_wait se sobrescribe con
  // backoff[0]): se compara, y si difiere se falla en alto.
  const quedo = diferenciasConfig(deseada, creado.config, CAMPOS_DURABLE);
  if (quedo.length) {
    throw new ErrorEstado(
      null,
      `el durable se recreó pero el servidor lo dejó distinto de lo pedido: ` +
        quedo.map((d) => `${d.campo}: pedido ${legible(d.campo, d.deseado)}, devuelto ${legible(d.campo, d.existente)}`).join("; "),
    );
  }
  console.log(`  ${OK} durable recreado con la configuración canónica · pendientes: ${creado.num_pending}`);
  console.log(
    c.gray(
      `\n  El servicio recibirá la foto completa por su handler. Un watch() que seguía conectado con nats.js 3.4.0\n` +
        `  tarda unos 45 s en notarlo (recibe consumer_deleted y no vuelve a pedir hasta detectar la falta de\n` +
        `  heartbeats): medido. Otros clientes pueden tardar más o requerir reiniciar el servicio.\n` +
        `  Seguimiento: \`flux doctor\` (columna PEND del durable).`,
    ),
  );
  return 0;
}

// ─── Despacho ────────────────────────────────────────────────────────────────────────────────────

export const USO_STATE =
  "uso: flux state <ls|get|resync>\n" +
  "  flux state ls <contrato> [--tenant t] [--json]\n" +
  "  flux state get <contrato> <tenant> <clave> [--show-data] [--json]\n" +
  "  flux state resync <servicio> <contrato> [--tenant t] [--confirm]";

/**
 * Espera a que stdout haya entregado todo lo escrito. Hacia una tubería llena (`| jq`, `| wc`)
 * Node escribe de forma asíncrona, y `process.exit()` —que es lo que hace index.mjs al terminar—
 * cortaba la salida en el tamaño del búfer de la tubería (medido: un `ls --json` de 100 500 claves
 * llegaba truncado a 192 KiB, y el JSON truncado no se puede interpretar).
 */
const vaciarStdout = () => new Promise((fin) => process.stdout.write("", () => fin()));

/** Punto de entrada desde index.mjs. Devuelve el código de salida. */
export async function comandoState(conexion, accion, args, opts) {
  const codigo = await despachar(conexion, accion, args, opts);
  await vaciarStdout();
  return codigo;
}

async function despachar({ nc, jsm }, accion, args, opts) {
  switch (accion) {
    case "ls": {
      if (args.length !== 1) die("uso: flux state ls <contrato> [--tenant t] [--json]   p.ej. flux state ls clientes.cliente.v1");
      if (opts.limit !== undefined && !(Number.isInteger(opts.limit) && opts.limit > 0)) {
        die(`--limit debe ser un entero mayor que 0, no "${opts.limit}"`);
      }
      return stateLs(nc, jsm, args[0], { tenant: opts.tenant, jsonOut: opts.jsonOut, limit: opts.limit });
    }
    case "get": {
      if (args.length !== 3) {
        die("uso: flux state get <contrato> <tenant> <clave>   p.ej. flux state get clientes.cliente.v1 acme cli-1");
      }
      return stateGet(nc, jsm, args, { showData: opts.showData, jsonOut: opts.jsonOut });
    }
    case "resync": {
      if (args.length !== 2) {
        die("uso: flux state resync <servicio> <contrato> [--tenant t] [--confirm]   p.ej. flux state resync tienda-web clientes.cliente.v1 --tenant acme");
      }
      return stateResync(jsm, args, { tenant: opts.tenant, confirm: opts.confirm, yes: opts.yes });
    }
    default:
      die(accion ? `acción desconocida: ${accion}\n${USO_STATE}` : USO_STATE);
  }
}

// Reexportado para las pruebas.
export { leerDirecto };
