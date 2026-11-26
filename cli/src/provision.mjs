/**
 * `flux provision` — crea los streams y los durables que las ACLs de `gen-acl.mjs` presuponen.
 * Contrato normativo: specification/06-security.md §3.5, 02-naming.md §3.3, 03-delivery.md §2.
 *
 * Por qué existe: en producción los servicios NO crean streams ni consumidores (no tienen
 * permiso, y es deliberado: un stream es un recurso compartido de todo el dominio y dejar
 * que el primer servicio que arranque lo cree con la configuración que le apetezca convierte
 * retención, réplicas y ventana de duplicados en una carrera). Alguien tiene que crearlos, y
 * ese alguien es esta herramienta, con credenciales de OPERACIÓN.
 *
 * Reglas, todas con una razón:
 *   · IDEMPOTENTE. Una segunda ejecución no cambia nada y lo dice. Es lo que permite
 *     llamarla desde cada despliegue sin miedo.
 *   · NUNCA BORRA, y nunca SOBRESCRIBE. Si un stream o un consumidor ya existe con otra
 *     configuración, informa de la diferencia y lo deja como está: cambiar la retención
 *     de un stream vivo, o `max_ack_pending` de un consumidor con mensajes en vuelo, es una
 *     decisión de una persona, no un efecto colateral de un despliegue.
 *   · Lo que el manifiesto no menciona, se ignora. Auditarlo es trabajo de `flux doctor`.
 *   · Deriva TODO del mismo sitio que el generador de ACL (scripts/lib/manifest.mjs): si
 *     los dos calcularan el nombre de un durable por separado, el servicio arrancaría y el
 *     broker le denegaría su propio consumidor.
 *
 * Las configuraciones canónicas salen de protocol.json, no de constantes duplicadas aquí.
 *
 * Estado replicado (10-state.md §2.1, §5.2 y §6): para cada dominio que TIENE estado —el que
 * tiene un contrato en `schemas/state/`— crea además `STATE_<DOM>` con la configuración de
 * `protocol.json` → `state.stream`, hace que `DLQ_<DOM>` capture también `dlq.state.<dom>.>`, y
 * crea los durables de estado de cada `watches` (uno por tenant si confina) y el del dueño para
 * su DLQ de estado. Un dominio solo de eventos no cambia: 1.1 es aditiva.
 *
 * Petición/respuesta (11-request-reply.md §8): NINGÚN stream que se cree captura una operación
 * (`rpc.*.*.*.*`). Los suyos son `EVT_<DOM>` (`<dom>.>`; `rpc` es un dominio reservado), `DLQ_<DOM>`
 * (`dlq.…`) y `STATE_<DOM>` (`state.…`): disjuntos por construcción, y además `recursosDeseados` lo
 * COMPRUEBA antes de crear nada y se niega si no se cumple, en vez de fiarse de la construcción.
 * `flux doctor` lo audita después en lo que ya existe.
 *
 * Única excepción a «nunca sobrescribe»: si `DLQ_<DOM>` existe con SOLO `dlq.<dom>.>` (lo creó
 * un `flux provision` de la 1.0) y el dominio ha ganado estado, se AMPLÍA con `dlq.state.<dom>.>`.
 * Ampliar los subjects de un stream con mensajes no pierde ninguno (A-44) y es lo único que hace
 * falta para que la DLQ de estado exista; cualquier otra diferencia se sigue informando y no se
 * toca.
 */

import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  AckPolicy,
  DeliverPolicy,
  DiscardPolicy,
  JetStreamApiError,
  ReplayPolicy,
  RetentionPolicy,
  StorageType,
} from "@nats-io/jetstream";

import { c, OK, FAIL, WARN, fmtDuration } from "./ui.mjs";
import { capturaOperaciones } from "./rpc-model.mjs";
import {
  analizarManifiesto,
  dlqStreamName,
  loadBundle,
  permisosDe,
  stateStreamName,
  streamName,
} from "../../scripts/lib/manifest.mjs";

const RAIZ = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
export const BUNDLE_POR_DEFECTO = join(RAIZ, "schemas", "bundle.json");

const NS = 1_000_000n;
const nanos = (ms) => Number(BigInt(ms) * NS);

/** `30d`, `2m`, `90d`, `24h`, `7s` → milisegundos. Es el formato que usa protocol.json. */
export function duracionMs(texto) {
  const m = /^(\d+)(s|m|h|d)$/.exec(texto ?? "");
  if (!m) throw new Error(`duración "${texto}" no reconocida en protocol.json (esperaba 30d, 2m, 24h…)`);
  return Number(m[1]) * { s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 }[m[2]];
}

/** `state.stream.maxAge` es un número de segundos (0 = no caduca); el de los eventos, `30d`. */
function duracionState(v) {
  return typeof v === "number" ? nanos(v * 1000) : nanos(duracionMs(v));
}

// ─── Lo que DEBE existir ─────────────────────────────────────────────────────

/** Configuración canónica de stream y de consumidor, leída de protocol.json. */
export async function leerCanonico() {
  const p = JSON.parse(await readFile(join(RAIZ, "protocol.json"), "utf8"));
  return { stream: p.stream, consumer: p.consumer, state: p.state };
}

/**
 * Los streams y consumidores que el manifiesto exige.
 * `replicas` es de los streams; los consumidores heredan las del stream.
 */
export function recursosDeseados(servicios, canonico, replicas) {
  const dominios = [...new Set(servicios.flatMap((s) => s.owns))].sort();
  // Un dominio tiene estado si su dueño lo declara (`stateOwns`: el bundle tiene un contrato suyo).
  const conEstado = new Set(servicios.flatMap((s) => s.stateOwns ?? []));
  const { event, dlq } = canonico.stream;
  const st = canonico.state.stream;

  const streams = dominios.flatMap((d) => [
    {
      name: streamName(d),
      config: {
        name: streamName(d),
        subjects: [`${d}.>`],
        storage: StorageType.File,
        retention: RetentionPolicy.Limits,
        discard: DiscardPolicy.Old,
        max_age: nanos(duracionMs(event.maxAge)),
        duplicate_window: nanos(duracionMs(event.duplicateWindow)),
        num_replicas: replicas,
      },
      // Campos que se comparan al ver si ya existe. `duplicate_window` solo importa en el
      // stream de eventos: la DLQ no deduplica publicaciones de productor.
      comparar: ["subjects", "storage", "retention", "discard", "max_age", "duplicate_window", "num_replicas"],
    },
    {
      name: dlqStreamName(d),
      config: {
        name: dlqStreamName(d),
        // Con estado, la DLQ captura también la de estado: sin tenant ni clave, cinco tokens.
        subjects: conEstado.has(d) ? [`dlq.${d}.>`, `dlq.state.${d}.>`] : [`dlq.${d}.>`],
        storage: StorageType.File,
        retention: RetentionPolicy.Limits,
        discard: DiscardPolicy.Old,
        max_age: nanos(duracionMs(dlq.maxAge)),
        num_replicas: replicas,
      },
      comparar: ["subjects", "storage", "retention", "discard", "max_age", "num_replicas"],
      // Ampliar sus subjects con los de estado no pierde mensajes (A-44): único cambio que se hace.
      ampliable: conEstado.has(d),
    },
    // STATE_<DOM>: un valor por clave. Lo que, mal puesto, no da error y rompe la semántica son
    // las cuatro últimas líneas (`flux doctor` las audita).
    ...(conEstado.has(d)
      ? [
          {
            name: stateStreamName(d),
            config: {
              name: stateStreamName(d),
              subjects: [`state.${d}.>`],
              storage: StorageType.File,
              retention: RetentionPolicy.Limits,
              discard: DiscardPolicy.Old,
              max_age: duracionState(st.maxAge),
              duplicate_window: nanos(duracionMs(st.duplicateWindow)),
              max_msgs_per_subject: st.maxMsgsPerSubject,
              allow_direct: st.allowDirect,
              num_replicas: replicas,
            },
            comparar: [
              "subjects", "storage", "retention", "discard", "max_age", "duplicate_window",
              "max_msgs_per_subject", "allow_direct", "num_replicas",
            ],
          },
        ]
      : []),
  ]);

  // Petición/respuesta: un stream que capturara una operación contestaría a cada petición con su acuse
  // y guardaría sus datos personales (A-18, A-67). Ningún stream se crea con uno así.
  const capturan = streams.filter((s) => s.config.subjects.some(capturaOperaciones));
  if (capturan.length) {
    throw new Error(
      `el aprovisionamiento crearía ${capturan.map((s) => `${s.name} (${s.config.subjects.join(", ")})`).join("; ")}, ` +
        "que captura operaciones de petición/respuesta (rpc.*.*.*.*): contestaría a cada petición con su acuse y guardaría sus datos personales. " +
        "`rpc` es un dominio reservado: ningún servicio puede ser su dueño (11-request-reply.md §8).",
    );
  }

  const k = canonico.consumer;
  const consumers = servicios.flatMap((s) =>
    permisosDe(s).durables.map((d) => ({
      stream: d.stream,
      name: d.durable,
      kind: d.kind,
      // La config que el SDK pide en subscribe() (sdk-node/src/client.ts). Si difirieran, el
      // `consumers.add` del SDK chocaría con el durable aprovisionado.
      config: {
        durable_name: d.durable,
        filter_subject: d.filter,
        ack_policy: AckPolicy.Explicit,
        ack_wait: nanos(k.ackWaitSeconds * 1000),
        max_deliver: k.maxDeliver,
        backoff: k.backoffMs.map(nanos),
        max_ack_pending: k.maxAckPending,
        // Un `watch` entrega la FOTO —el último valor de cada clave— y después los cambios
        // (10-state.md §5.2): `last_per_subject`. El resto, la configuración canónica de siempre.
        deliver_policy: d.kind === "state" ? DeliverPolicy.LastPerSubject : DeliverPolicy.All,
        replay_policy: ReplayPolicy.Instant,
      },
      comparar: [
        "filter_subject", "ack_policy", "ack_wait", "max_deliver", "backoff",
        "max_ack_pending", "deliver_policy", "replay_policy",
      ],
    })),
  );

  return { streams, consumers };
}

// ─── Comparación ─────────────────────────────────────────────────────────────

const NANOS = new Set(["max_age", "duplicate_window", "ack_wait"]);

/** Un valor legible: las duraciones de JetStream van en nanosegundos. */
function legible(campo, v) {
  if (v === undefined || v === null) return c.gray("(sin valor)");
  if (campo === "max_age" && v === 0) return "0 (no caduca)";
  if (NANOS.has(campo)) return fmtDuration(v);
  if (campo === "backoff") return `[${v.map((x) => fmtDuration(x)).join(", ")}]`;
  if (Array.isArray(v)) return `[${v.join(", ")}]`;
  return String(v);
}

/** El filtro de un consumidor: `filter_subject`, o el único de `filter_subjects`. */
function configExistente(cfg) {
  const out = { ...cfg };
  if (out.filter_subject === undefined && cfg.filter_subjects?.length === 1) {
    out.filter_subject = cfg.filter_subjects[0];
  }
  if (Array.isArray(out.subjects)) out.subjects = [...out.subjects].sort();
  return out;
}

export function diferencias(deseado, existente, campos) {
  const ex = configExistente(existente);
  const de = { ...deseado };
  if (Array.isArray(de.subjects)) de.subjects = [...de.subjects].sort();
  const out = [];
  for (const campo of campos) {
    if (JSON.stringify(de[campo]) !== JSON.stringify(ex[campo])) {
      out.push({ campo, deseado: de[campo], existente: ex[campo] });
    }
  }
  return out;
}

// `status` es el 404 de la API; `code` es el err_code (10059 stream, 10014 consumidor). Un
// permiso denegado o un timeout NO son «no existe»: tratarlos así intentaría crear lo que
// quizá ya está y el error real quedaría enterrado.
const noExiste = (e) => e instanceof JetStreamApiError && e.status === 404;

// ─── Ejecución ───────────────────────────────────────────────────────────────

/**
 * Devuelve `{ estados, errores }`; cada estado es `{ recurso, accion }` con `accion` ∈
 * `creado | sin cambios | difiere | se crearía | error`.
 */
export async function provision(jsm, deseados, { dryRun }) {
  const resultado = [];
  // Columna del recurso alineada: con 20 líneas, leer el estado es mirar una columna.
  const ancho = Math.max(
    ...deseados.streams.map((s) => s.name.length),
    ...deseados.consumers.map((k) => `${k.stream} / ${k.name}`.length),
  );
  const registrar = (r) => {
    resultado.push(r);
    imprimir(r, ancho);
  };

  const streamsExistentes = new Set();

  for (const s of deseados.streams) {
    const recurso = s.name;
    try {
      let info = null;
      try {
        info = await jsm.streams.info(s.name);
      } catch (e) {
        if (!noExiste(e)) throw e;
      }

      if (info) {
        streamsExistentes.add(s.name);
        const dif = diferencias(s.config, info.config, s.comparar);
        if (dif.length && s.ampliable && esAmpliacion(dif, info.config.subjects, s.config.subjects)) {
          // El dominio ha ganado estado y su DLQ solo captura la de eventos: se amplía, sin perder
          // un mensaje (A-44). Lo único que esta herramienta cambia en un recurso que ya existe.
          const nuevos = s.config.subjects.filter((x) => !info.config.subjects.includes(x));
          if (dryRun) {
            registrar({ recurso, accion: "se ampliaría", detalle: `+ ${nuevos.join(", ")}` });
          } else {
            await jsm.streams.update(s.name, { subjects: s.config.subjects });
            registrar({ recurso, accion: "ampliado", detalle: `+ ${nuevos.join(", ")}` });
          }
        } else {
          registrar(
            dif.length
              ? { recurso, accion: "difiere", dif }
              : { recurso, accion: "sin cambios" },
          );
        }
      } else if (dryRun) {
        registrar({ recurso, accion: "se crearía", detalle: s.config.subjects.join(", ") });
      } else {
        await jsm.streams.add(s.config);
        streamsExistentes.add(s.name);
        registrar({ recurso, accion: "creado", detalle: s.config.subjects.join(", ") });
      }
    } catch (e) {
      registrar({ recurso, accion: "error", detalle: explicar(e) });
    }
  }

  for (const k of deseados.consumers) {
    const recurso = `${k.stream} / ${k.name}`;
    try {
      let info = null;
      // Si el stream no existe (o no se ha podido crear), el consumidor no puede existir.
      if (streamsExistentes.has(k.stream)) {
        try {
          info = await jsm.consumers.info(k.stream, k.name);
        } catch (e) {
          if (!noExiste(e)) throw e;
        }
      }

      if (info) {
        const dif = diferencias(k.config, info.config, k.comparar);
        registrar(
          dif.length
            ? { recurso, accion: "difiere", dif }
            : { recurso, accion: "sin cambios" },
        );
      } else if (dryRun) {
        registrar({ recurso, accion: "se crearía", detalle: `filtro ${k.config.filter_subject}` });
      } else if (!streamsExistentes.has(k.stream)) {
        registrar({ recurso, accion: "error", detalle: `su stream ${k.stream} no existe y no se pudo crear` });
      } else {
        await jsm.consumers.add(k.stream, k.config);
        registrar({ recurso, accion: "creado", detalle: `filtro ${k.config.filter_subject}` });
      }
    } catch (e) {
      registrar({ recurso, accion: "error", detalle: explicar(e) });
    }
  }

  return resultado;
}

/**
 * ¿La única diferencia es que a un stream le FALTAN subjects que debería capturar (y no tiene
 * ninguno de más)? Es el caso de `DLQ_<DOM>` de la 1.0 en un dominio que gana estado.
 */
export function esAmpliacion(dif, existentes, deseados) {
  return (
    dif.length === 1 &&
    dif[0].campo === "subjects" &&
    existentes.every((x) => deseados.includes(x)) &&
    deseados.some((x) => !existentes.includes(x))
  );
}

/** Un permiso denegado se explica por sí solo; un timeout, no. */
function explicar(e) {
  const m = e?.message ?? String(e);
  if (/Permissions Violation/i.test(m)) {
    return (
      `${m}\n      ¿Son credenciales de OPERACIÓN? Un servicio no puede crear streams ni ` +
      `consumidores, y es deliberado (06-security.md §3)`
    );
  }
  return m;
}

const ETIQUETA = {
  creado: (t) => c.green(t),
  "sin cambios": (t) => c.gray(t),
  difiere: (t) => c.yellow(t),
  "se crearía": (t) => c.cyan(t),
  ampliado: (t) => c.green(t),
  "se ampliaría": (t) => c.cyan(t),
  error: (t) => c.red(t),
};

function imprimir(r, ancho) {
  const etiqueta = ETIQUETA[r.accion](r.accion.toUpperCase().padEnd(12));
  const detalle = r.detalle && r.accion !== "error" ? c.gray(`  ${r.detalle}`) : "";
  console.log(`  ${etiqueta} ${r.recurso.padEnd(ancho)}${detalle}`.trimEnd());
  if (r.accion === "error") console.log(`      ${c.red(r.detalle)}`);
  for (const d of r.dif ?? []) {
    console.log(
      `      ${d.campo}: existente ${c.bold(legible(d.campo, d.existente))}, ` +
        `deseado ${c.bold(legible(d.campo, d.deseado))}`,
    );
  }
}

/** Punto de entrada de `flux provision`. Devuelve el código de salida. */
export async function comandoProvision(jsm, args) {
  const { manifiesto: ruta, dryRun, replicas, bundle } = args;

  let manifiesto;
  try {
    manifiesto = JSON.parse(await readFile(ruta, "utf8"));
  } catch (e) {
    console.error(`${FAIL} no se pudo leer el manifiesto "${ruta}": ${e.message}`);
    return 2;
  }
  let elBundle;
  try {
    elBundle = await loadBundle(bundle ?? BUNDLE_POR_DEFECTO);
  } catch (e) {
    console.error(`${FAIL} ${e.message}`);
    return 2;
  }

  const { problemas, servicios } = analizarManifiesto(manifiesto, elBundle);
  if (problemas.length) {
    console.error(`${FAIL} ${problemas.length} problema(s) en el manifiesto:`);
    for (const p of problemas) console.error(`  ${FAIL} ${p.m}${p.hint ? `\n    ${c.gray(p.hint)}` : ""}`);
    return 1;
  }

  const deseados = recursosDeseados(servicios, await leerCanonico(), replicas);
  console.log(
    c.bold(`flux provision${dryRun ? " --dry-run" : ""}`) +
      c.gray(`  ${ruta} · ${deseados.streams.length} stream(s), ${deseados.consumers.length} consumidor(es) · réplicas ${replicas}`),
  );
  if (dryRun) console.log(c.gray("  Simulación: no se crea nada."));
  console.log();

  const r = await provision(jsm, deseados, { dryRun });

  const cuenta = (a) => r.filter((x) => x.accion === a).length;
  const [creados, iguales, difieren, errores, pendientes, ampliados, porAmpliar] = [
    cuenta("creado"), cuenta("sin cambios"), cuenta("difiere"), cuenta("error"), cuenta("se crearía"),
    cuenta("ampliado"), cuenta("se ampliaría"),
  ];
  console.log();
  const partes = [
    pendientes && `${pendientes} se crearía(n)`,
    porAmpliar && `${porAmpliar} se ampliaría(n)`,
    creados && `${creados} creado(s)`,
    ampliados && `${ampliados} ampliado(s)`,
    iguales && `${iguales} sin cambios`,
    difieren && c.yellow(`${difieren} con diferencias`),
    errores && c.red(`${errores} con error`),
  ].filter(Boolean);
  console.log(`  ${partes.join(" · ")}`);

  if (errores) {
    console.log(`\n${FAIL} Hubo errores. Lo ya creado se queda como está: la herramienta nunca borra.`);
    return 1;
  }
  if (difieren) {
    console.log(
      `\n${WARN} Hay recursos que existen con OTRA configuración. No se han tocado: ` +
        `cambiarlos en un broker vivo es una decisión de una persona.\n` +
        `  Para alinearlos, actualízalos a mano (nats stream edit / nats consumer edit) y repite.`,
    );
    return 1;
  }
  if (!creados && !pendientes && !ampliados && !porAmpliar) console.log(`\n${OK} Sin cambios: el broker ya tiene todo lo que el manifiesto exige.`);
  else if (dryRun) console.log(`\n${OK} Simulación completa. Ejecuta sin --dry-run para crearlo.`);
  else console.log(`\n${OK} Aprovisionado.`);
  return 0;
}
