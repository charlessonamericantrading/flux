/**
 * `flux tail` — ver eventos en vivo.
 *
 * Usa un consumidor efímero de solo lectura: mirar el bus NUNCA debe alterar el
 * estado de un consumidor de producción. Un `tail` que consuma mensajes de un durable
 * ajeno es una herramienta de depuración que provoca incidentes.
 */

import { c, reasonColor } from "./ui.mjs";
import { datosOcultos, redactarSobre } from "./privacidad.mjs";
import { inspeccionar, parsearSubject, resumenDlqEstado } from "./state-model.mjs";

const dec = new TextDecoder();

/**
 * Qué stream hay detrás de un patrón, y de qué tipo de mensajes es:
 *   `pedidos.>`              → EVT_PEDIDOS     (eventos)
 *   `dlq.pedidos.>`          → DLQ_PEDIDOS     (DLQ de eventos)
 *   `state.clientes.>`       → STATE_CLIENTES  (estado: 10-state.md §2)
 *   `dlq.state.clientes.>`   → DLQ_CLIENTES    (DLQ de estado: 10-state.md §6)
 * Antes `dlq.state.…` caía en `DLQ_STATE`, un stream que no existe (y que no puede existir:
 * `state` es un dominio reservado).
 */
export function resolverStream(pattern) {
  const t = pattern.split(".");
  const nombre = (d) => d.replace(/-/g, "_").toUpperCase();
  const concreto = (x) => x !== undefined && !x.includes("*") && !x.includes(">");
  const exigir = (dom) => {
    if (!concreto(dom)) {
      throw new Error(
        `el dominio debe ser un token concreto (recibido "${dom ?? ""}"): ` +
          `los streams se organizan por dominio, así que no hay dónde suscribirse`,
      );
    }
  };
  if (t[0] === "dlq") {
    const estado = t[1] === "state";
    const dom = estado ? t[2] : t[1];
    exigir(dom);
    return { stream: `DLQ_${nombre(dom)}`, tipo: estado ? "dlq-estado" : "dlq" };
  }
  if (t[0] === "state") {
    exigir(t[1]);
    return { stream: `STATE_${nombre(t[1])}`, tipo: "estado" };
  }
  exigir(t[0]);
  return { stream: `EVT_${nombre(t[0])}`, tipo: "evento" };
}

export async function tail(js, jsm, pattern, { since, full, tenant, jsonOut, showData }) {
  const { stream, tipo } = resolverStream(pattern);

  await jsm.streams.info(stream).catch(() => {
    throw new Error(`no existe el stream ${stream} para el patrón "${pattern}"`);
  });

  const consumer = await jsm.consumers.add(stream, {
    ack_policy: "none",
    deliver_policy: since ? "by_start_time" : "new",
    ...(since && { opt_start_time: new Date(Date.now() - parseSince(since)).toISOString() }),
    replay_policy: "instant",
    filter_subject: pattern,
    inactive_threshold: 60 * 1e9,
  });

  console.error(
    c.gray(
      `tail ${pattern} en ${stream}${since ? ` desde hace ${since}` : ""}` +
        `${tenant ? ` (tenant=${tenant})` : ""} — Ctrl+C para salir\n`,
    ),
  );

  const cleanup = async () => {
    await jsm.consumers.delete(stream, consumer.name).catch(() => {});
  };
  process.on("SIGINT", async () => {
    await cleanup();
    process.exit(0);
  });

  try {
    const con = await js.consumers.get(stream, consumer.name);
    const messages = await con.consume();
    for await (const m of messages) {
      // En estado la identidad de la entrada (tenant y clave) está en el SUBJECT, que es lo que
      // decidió el broker; el sobre la repite y puede discrepar (STATE_SUBJECT_MISMATCH).
      const info = tipo === "estado" ? parsearSubject(m.subject) : null;
      const ins = tipo === "estado" ? inspeccionar(m.data, info) : null;

      let e;
      if (ins) {
        e = ins.envelope;
      } else {
        try {
          e = JSON.parse(dec.decode(m.data));
        } catch {
          e = undefined;
        }
      }
      if (e === undefined || e === null || typeof e !== "object") {
        // Un cuerpo ilegible puede llevar el dato (en estado, sin clasificar): solo con --show-data.
        const cuerpo = tipo === "estado" && !showData ? c.gray("(cuerpo no mostrado; --show-data)") : c.gray(dec.decode(m.data).slice(0, 120));
        console.log(`${c.magenta("POISON")} ${m.subject}  ${cuerpo}`);
        continue;
      }
      if (tenant && (info ? info.tenant : e.tenantid) !== tenant) continue;
      console.log(jsonOut ? JSON.stringify(paraSalida(e, tipo, showData)) : render(e, m, { full, showData, info, ins, tipo }));
    }
  } finally {
    await cleanup();
  }
}

/**
 * El sobre tal como se imprime con `--json` o `--full`. El `data` de un evento de DLQ de estado
 * es `{ key, revision }` (ni PII ni dato) y se deja ver aunque el sobre conserve su
 * `dataclassification: confidential` (10-state.md §6); si lleva MÁS, es justo lo que la regla
 * prohíbe, y se redacta como cualquier otro.
 */
function paraSalida(e, tipo, showData) {
  if (tipo === "dlq-estado" && resumenDlqEstado(e).ok) return e;
  return redactarSobre(e, showData);
}

function render(e, m, { full, showData, info, ins, tipo }) {
  const subject = m.subject;
  const hora = (e.time ?? "").slice(11, 23);

  // El estado se identifica por tenant/clave/revisión/versión, no por el `subject` del sobre
  // (que ahí es la clave): se enseña lo que decidió el broker.
  let identidad = e.subject ? ` ${c.bold(e.subject)}` : "";
  if (tipo === "estado") {
    identidad = info
      ? ` ${c.bold(`${info.tenant}/${info.clave}`)} ${c.gray(`rev ${m.seq}${ins.aggregateVersion ? ` · v${ins.aggregateVersion}` : ""}`)}` +
        (ins.lapida ? ` ${c.yellow("LÁPIDA")}` : "")
      : ` ${c.red("SUBJECT NO CANÓNICO")} ${c.gray(`rev ${m.seq}`)}`;
    for (const p of ins.problemas) identidad += ` ${c.red(p.code ?? "ANÓMALA")}`;
  }

  const cabecera =
    `${c.gray(hora)} ${c.cyan(subject)}${identidad}` +
    (e.dlqreason ? ` ${reasonColor(e.dlqreason)(`[${e.dlqreason} ×${e.dlqattempts}]`)}` : "");

  if (full) {
    // `--full` no es un permiso para volcar PII: pasa por la misma redacción que la línea corta.
    return `${cabecera}\n${JSON.stringify(paraSalida(e, tipo, showData), null, 2)}`;
  }

  // El correlationid abreviado es lo que permite seguir un flujo a ojo entre
  // subjects distintos sin abrir un trazador.
  const corr = e.correlationid ? c.gray(`⟨${e.correlationid.slice(0, 8)}⟩`) : "";

  let data;
  if (tipo === "dlq-estado") {
    // Su `data` es { key, revision } (10-state.md §6): ni PII ni dato. Si lleva MÁS, el SDK que
    // lo escribió está copiando el dato a una DLQ que guarda 90 días, y es justo lo que la
    // regla existe para evitar: se avisa y no se imprime.
    const r = resumenDlqEstado(e);
    data = r.ok
      ? c.gray(`clave ${r.key} · revisión ${r.revision}`)
      : c.red(`data con campos de más (${r.extra.join(", ")}): viola 10-state.md §6, no se imprime`);
  } else if (ins?.lapida) {
    data = c.gray("[lápida]");
  } else if (datosOcultos(e.dataclassification) && !showData) {
    // no se vuelca PII a una terminal que acaba en un log
    data = c.yellow(`[${e.dataclassification ?? "sin dataclassification"}]`);
  } else {
    data = truncate(JSON.stringify(e.data), 160);
  }

  return `${cabecera} ${corr} ${data}${e.dlqerror ? `\n    ${c.red(e.dlqerror)}` : ""}`;
}

const truncate = (s, n) => (s && s.length > n ? `${s.slice(0, n)}${c.gray("…")}` : s ?? "");

function parseSince(s) {
  const m = /^(\d+)([smhd])$/.exec(s);
  if (!m) throw new Error(`--since inválido: "${s}". Formato: 30s, 5m, 2h, 1d`);
  return Number(m[1]) * { s: 1e3, m: 6e4, h: 36e5, d: 864e5 }[m[2]];
}
