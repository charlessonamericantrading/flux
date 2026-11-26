/**
 * `flux doctor`, la parte de ESTADO (specification/10-state.md §2.1, §5.2, §6).
 *
 * Son funciones puras —reciben la configuración que el servidor devolvió y dicen qué está
 * mal— para que cada comprobación se pruebe en los dos sentidos sin broker. El comando las
 * alimenta con `jsm.streams.list()` y `jsm.consumers.list()`.
 *
 * Por qué hay que mirar: el servidor devolvió siempre la configuración que se le pidió (A-31,
 * A-49), pero los cuatro ajustes de abajo, mal puestos, **no dan ningún error** y rompen la
 * semántica del estado, y los hace mal un `nats stream add` a mano o un IaC distraído:
 *
 *   · sin `max_msgs_per_subject: 1` el stream es un log, no un mapa: el valor viejo no sale.
 *   · sin `allow_direct` `get` y el paso 1 de `put` reciben «sin respuesta» (A-32).
 *   · con `max_age` el estado CADUCA: una copia que miente.
 *   · con `discard: new` (+ `discard_new_per_subject`) la segunda escritura de una clave la
 *     RECHAZA el broker (10077): un create-only, no un estado (A-46).
 *
 * Un hallazgo es `{ level: "error" | "warn" | "info", message, hint }`, como los de doctor.mjs.
 */

import { canon, cubre, filtroDeConfig, diferenciasConfig, configDurableEstado, CAMPOS_DURABLE } from "./state-model.mjs";
import { fmtDuration } from "./ui.mjs";

const S = 1e9;
const REF = "10-state.md";

/** `2m`, `30s`, `24h`, `90d` → nanosegundos: el formato de duración de protocol.json. */
function duracionNs(texto) {
  const m = /^(\d+)(s|m|h|d)$/.exec(texto ?? "");
  if (!m) throw new Error(`duración "${texto}" no reconocida en protocol.json (esperaba 2m, 30s, 24h…)`);
  return Number(m[1]) * { s: S, m: 60 * S, h: 3600 * S, d: 86400 * S }[m[2]];
}

/** `STATE_LINEA_ENVIO` → `LINEA_ENVIO`: el dominio tal como lo escribe un nombre de stream. */
export const dominioDeStream = (nombre) => nombre.replace(/^STATE_/, "");

const nombreNats = (dom) => dom.replace(/-/g, "_").toUpperCase();

/**
 * El dominio de un stream de estado, tal como aparece en su subject (`state.<dom>.>`, con los
 * guiones que el nombre del stream no puede tener); si el subject no está, el del nombre.
 */
export function dominioDeSubjects(cfg) {
  const sufijo = dominioDeStream(cfg.name);
  for (const s of cfg.subjects ?? []) {
    const m = /^state\.([a-z0-9]+(?:-[a-z0-9]+)*)\.>$/.exec(s);
    if (m && nombreNats(m[1]) === sufijo) return m[1];
  }
  return sufijo.toLowerCase().replace(/_/g, "-");
}

/**
 * Audita un `STATE_<DOM>`: `cfg` es `StreamInfo.config`. Devuelve los hallazgos.
 */
export function auditarStreamEstado(cfg) {
  const k = canon().estado.stream;
  const out = [];
  const hallazgo = (level, message, hint) => out.push({ level, message, hint });
  const dom = dominioDeSubjects(cfg);

  // ─── Los cuatro que, mal puestos, no dan error y rompen la semántica ───
  if (cfg.max_msgs_per_subject !== k.maxMsgsPerSubject) {
    hallazgo(
      "error",
      `max_msgs_per_subject=${cfg.max_msgs_per_subject ?? "(sin valor)"}, debe ser ${k.maxMsgsPerSubject}`,
      `Es lo que convierte un log en un mapa: sin él el valor anterior de una clave NO sale del stream y un observador nuevo recibe el historial como si fuera la foto — ${REF} §2.1, A-31`,
    );
  }
  if (cfg.allow_direct !== k.allowDirect) {
    hallazgo(
      "error",
      `allow_direct=${cfg.allow_direct ?? false}, debe ser ${k.allowDirect}`,
      `Sin él DIRECT.GET no contesta (ni 404): \`state.get\` y el paso 1 de \`put\` reciben «sin respuesta» y se ve igual que un stream inexistente — ${REF} §2.1, A-32`,
    );
  }
  if ((cfg.max_age ?? 0) !== k.maxAge) {
    hallazgo(
      "error",
      `max_age=${fmtDuration(cfg.max_age)}, debe ser 0 (sin caducidad)`,
      `Un valor que desapareciera pasado el plazo sería una copia que miente: el estado se borra con una lápida, no con el tiempo — ${REF} §2.1`,
    );
  }
  if (cfg.discard !== k.discard || cfg.discard_new_per_subject) {
    hallazgo(
      "error",
      `discard=${cfg.discard}${cfg.discard_new_per_subject ? " con discard_new_per_subject" : ""}, debe ser '${k.discard}'`,
      `Con discard=new por subject el broker RECHAZA la segunda escritura de una clave (err 10077): sería un «crear solo si no existe», no un estado — ${REF} §2.1, A-46`,
    );
  }

  // ─── El resto de la configuración canónica ───
  if (cfg.storage !== k.storage) {
    hallazgo(
      "error",
      `storage=${cfg.storage}, debe ser '${k.storage}'`,
      `Un estado en memoria desaparece al reiniciar el broker, y los observadores nuevos recibirían una foto vacía sin error — ${REF} §2.1`,
    );
  }
  if (cfg.retention !== k.retention) {
    hallazgo(
      "error",
      `retention=${cfg.retention}, debe ser '${k.retention}'`,
      `Con interest o workqueue el broker descarta mensajes por consumo o por falta de consumidores: el estado se perdería sin error — ${REF} §2.1`,
    );
  }

  const esperado = `state.${dom}.>`;
  const subjects = cfg.subjects ?? [];
  if (!subjects.includes(esperado)) {
    hallazgo(
      "error",
      `no captura ${esperado} (subjects: ${subjects.join(", ") || "ninguno"})`,
      `Una escritura a un subject que ningún stream captura por JetStream falla; por core se evapora sin error (A-44, A-45) — ${REF} §2.1`,
    );
  } else if (subjects.length > 1) {
    hallazgo(
      "warn",
      `captura además de ${esperado}: ${subjects.filter((s) => s !== esperado).join(", ")}`,
      `Un STATE_<DOM> solo guarda estado de su dominio; otro subject mezclaría datos con otra semántica — ${REF} §2.1`,
    );
  }

  const replicas = cfg.num_replicas ?? 1;
  if (replicas === 1) {
    hallazgo("info", "replicas=1", `Producción usa ${k.replicasProduction}: un estado sin réplicas es el único ejemplar del dato — 02-naming.md §3.2`);
  } else if (replicas !== k.replicasProduction) {
    hallazgo("warn", `replicas=${replicas}, producción usa ${k.replicasProduction}`, `Con un número par no hay desempate de líder — 02-naming.md §3.2`);
  }

  // `duplicate_window` NO interviene en las escrituras (el chequeo de secuencia va antes, A-33):
  // un valor distinto es inocuo, pero no es el canónico.
  const ventana = duracionNs(k.duplicateWindow);
  if ((cfg.duplicate_window ?? 0) !== ventana) {
    hallazgo("info", `duplicate_window=${fmtDuration(cfg.duplicate_window)}, el canónico es ${k.duplicateWindow}`, `No interviene en las escrituras de estado (A-33) — ${REF} §2.1`);
  }

  // Fuera de la lista de la spec, pero de la misma familia (no da error y rompe la semántica):
  // con discard=old, un tope global saca la clave MÁS ANTIGUA del estado, no «lo más viejo».
  for (const [campo, nombre] of [["max_msgs", "mensajes"], ["max_bytes", "bytes"]]) {
    if (cfg[campo] !== undefined && cfg[campo] !== -1) {
      hallazgo(
        "warn",
        `${campo}=${cfg[campo]}: tope global de ${nombre}`,
        `Al llegar al tope (discard=old) el broker borra la clave más antigua: desaparece del estado sin lápida y sin error`,
      );
    }
  }
  return out;
}

/**
 * ¿Tiene `DLQ_<DOM>` donde caer el fallo de un observador? El SDK publica en
 * `dlq.state.<dom>.<agr>.v<M>` (§6): si ningún stream lo captura, el publish por JetStream
 * FALLA (A-44) y el mensaje que no se pudo aplicar no llega a la DLQ.
 *
 * `dlq` es el `StreamInfo.config` del `DLQ_<DOM>` del dominio, o `undefined` si no existe.
 */
export function auditarDlqDeEstado(cfgEstado, dlq) {
  const dom = dominioDeSubjects(cfgEstado);
  const dlqNombre = `DLQ_${dominioDeStream(cfgEstado.name)}`;
  const objetivo = `dlq.state.${dom}.>`;
  if (!dlq) {
    return [{
      level: "error",
      message: `no existe ${dlqNombre} (${cfgEstado.name} tiene estado)`,
      hint: `Sin ella, una entrada que un observador no puede aplicar no tiene dónde caer — ${REF} §6. \`flux provision\` la crea`,
    }];
  }
  if (!(dlq.subjects ?? []).some((s) => cubre(s, objetivo))) {
    return [{
      level: "error",
      message: `${dlqNombre} no captura ${objetivo} (subjects: ${(dlq.subjects ?? []).join(", ") || "ninguno"})`,
      hint:
        `Un dominio CON estado necesita que su DLQ capture \`dlq.state.<dom>.>\` además de \`dlq.<dom>.>\`: ` +
        `si no, el publish de la DLQ de estado falla (A-44) y el fallo del observador se pierde — ${REF} §6. ` +
        `Ampliar los subjects de un stream con datos no pierde mensajes; \`flux provision\` lo hace`,
    }];
  }
  return [];
}

/** El contrato y el tenant que el NOMBRE de un durable de estado dice que lleva, o `null`. */
function leerNombreDurable(nombre) {
  const partes = nombre.split("__");
  if (partes.length < 2 || partes.length > 3 || !partes[1].startsWith("state_")) return null;
  return { servicio: partes[0], contrato: partes[1].slice("state_".length), tenant: partes[2] };
}

/**
 * Audita el durable de un observador de estado: `cfg` es `ConsumerInfo.config`.
 * Devuelve `{ findings, problemas }`; `problemas` son etiquetas cortas para la tabla.
 */
export function auditarDurableEstado(cfg) {
  const k = canon();
  const nombre = cfg.durable_name ?? cfg.name;
  const out = [];
  const problemas = [];
  const hallazgo = (level, etiqueta, message, hint) => {
    out.push({ level, message, hint });
    if (etiqueta) problemas.push(etiqueta);
  };

  if (cfg.deliver_policy !== "last_per_subject") {
    hallazgo(
      "error",
      "deliver≠last_per_subject",
      `deliver_policy=${cfg.deliver_policy}, debe ser last_per_subject`,
      cfg.deliver_policy === "all"
        ? `Con \`all\` entrega lo mismo MIENTRAS el stream tenga max_msgs_per_subject=1 (A-35), pero deja de ser la foto en cuanto ese ajuste falle — ${REF} §5.2`
        : `Con \`${cfg.deliver_policy}\` el observador NO recibe la foto completa: se queda sin las claves que ya existían y la copia local nace incompleta, sin error — ${REF} §5.2`,
    );
  }

  const filtro = filtroDeConfig(cfg);
  const nom = leerNombreDurable(nombre);
  const mf = filtro && /^state\.([a-z0-9-]+)\.([a-z0-9-]+)\.(v[1-9][0-9]*)\.([a-z0-9-]+|\*)\.\*$/.exec(filtro);
  if (!k.reDurable.test(nombre)) {
    hallazgo("warn", null, `el durable no sigue <servicio>__state_<dom>_<agr>_v<M>[__<tenant>]`, `Sin el patrón no se sabe qué servicio lo tiene ni de qué tenant es — ${REF} §5.2`);
  }
  if (!mf) {
    hallazgo(
      "error",
      "filtro",
      `filtro ${filtro ?? "(ninguno o varios)"}: no es state.<dom>.<agr>.v<M>.<tenant|*>.*`,
      `Un filtro más ancho que un contrato mezcla contratos en un solo durable; uno con varios filtros no puede crearse por la API de subject y escapa a la ACL — ${REF} §5.2, A-40`,
    );
  } else if (nom) {
    const delFiltro = `${mf[1]}_${mf[2]}_${mf[3]}`.replace(/-/g, "_");
    if (delFiltro !== nom.contrato) {
      hallazgo("error", "filtro≠nombre", `el nombre dice el contrato «${nom.contrato}» y el filtro, «${delFiltro}»`, `El observador creería leer un contrato y recibiría otro — ${REF} §5.2`);
    }
    const tenantFiltro = mf[4] === "*" ? undefined : mf[4].replace(/-/g, "_");
    if (tenantFiltro !== nom.tenant) {
      hallazgo(
        "error",
        "tenant≠nombre",
        `el nombre ${nom.tenant ? `lleva el tenant «${nom.tenant}»` : "no lleva tenant"} y el filtro ${tenantFiltro ? `es del tenant «${tenantFiltro}»` : "los admite todos"}`,
        tenantFiltro && !nom.tenant
          ? `Un durable sin tenant en el nombre es COMPARTIDO por todas las instancias que lo abran, y el broker reparte los mensajes entre ellas: cada una vería la mitad de la foto sin error — ${REF} §5.2, A-47`
          : `El nombre y el filtro deben decir el mismo tenant, o dos instancias de tenants distintos se quitarían mensajes — ${REF} §5.2, A-47`,
      );
    }
  }

  // El resto de la config canónica. ack_policy y ack_wait los audita ya la parte genérica de
  // doctor (con sus propios hallazgos): aquí solo lo que esa no mira.
  const canonica = configDurableEstado(nombre, filtro ?? "");
  const difs = diferenciasConfig(canonica, cfg, CAMPOS_DURABLE.filter((c) => ["max_deliver", "backoff", "max_ack_pending", "replay_policy"].includes(c)));
  for (const d of difs) {
    hallazgo(
      "warn",
      `${d.campo}≠canónico`,
      `${d.campo}=${JSON.stringify(d.existente)} (canónico ${JSON.stringify(d.deseado)})`,
      `Los durables de estado llevan la configuración de 03-delivery.md §2, salvo deliver_policy — ${REF} §5.2`,
    );
  }
  return { findings: out, problemas };
}
