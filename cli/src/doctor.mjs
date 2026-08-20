/**
 * `flux doctor` — audita un cluster vivo contra el protocolo.
 *
 * Es el comando que justifica la CLI entera. Los fallos que flux persigue —ack_wait
 * sobrescrito, streams solapados, durables mal nombrados— **no producen ningún error**
 * en producción: producen comportamiento incorrecto silencioso. La única forma de
 * detectarlos es ir a preguntarle al servidor qué tiene configurado de verdad.
 */

import { c, OK, FAIL, WARN, fmtDuration, table } from "./ui.mjs";

const S = 1e9;

/** Config canónica, de protocol.json. Ver specification/03-delivery.md §2. */
const CANON = {
  ackWaitNs: 30 * S,
  maxDeliver: 6,
  backoffNs: [30 * S, 60 * S, 300 * S, 900 * S, 1800 * S],
};

const SUBJECT_RE =
  /^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]*\.[a-z0-9]+(-[a-z0-9]+)*$/;
const DURABLE_RE = /^[a-z0-9]+(-[a-z0-9]+)*__[a-z0-9_]+$/;

export async function doctor(jsm, { verbose } = {}) {
  const findings = [];
  const add = (level, target, message, hint) =>
    findings.push({ level, target, message, hint });

  const streams = [];
  for await (const s of jsm.streams.list()) streams.push(s);

  if (streams.length === 0) {
    console.log(`${WARN} No hay streams en este servidor.`);
    return { findings, streams: 0, consumers: 0 };
  }

  const evtStreams = streams.filter((s) => s.config.name.startsWith("EVT_"));
  const dlqStreams = streams.filter((s) => s.config.name.startsWith("DLQ_"));
  const otros = streams.filter(
    (s) => !s.config.name.startsWith("EVT_") && !s.config.name.startsWith("DLQ_"),
  );

  // ─── Streams ───────────────────────────────────────────────────────────────
  console.log(c.bold("\nStreams"));

  for (const s of streams) {
    const n = s.config.name;

    if (n.includes(".")) {
      // No debería poder existir: NATS lo rechaza. Si aparece, el servidor cambió.
      add("error", n, "el nombre contiene puntos", "02-naming.md §3");
    }

    for (const subj of s.config.subjects ?? []) {
      const isDlq = subj.startsWith("dlq.");
      const base = isDlq ? subj.slice(4) : subj;
      if (!base.endsWith(">") && !SUBJECT_RE.test(base)) {
        add("warn", n, `subject "${subj}" no sigue el patrón del protocolo`, "02-naming.md §1");
      }
      if (!isDlq && /\.dlq$/.test(subj)) {
        add(
          "error",
          n,
          `subject "${subj}" usa .dlq como SUFIJO`,
          "La DLQ va prefijada (dlq.<subject>). Un sufijo encaja con <dominio>.> y el stream principal captura sus propios muertos — 02-naming.md §3.1",
        );
      }
    }

    // Solape entre streams: dos streams que capturan el mismo subject duplican
    // cada mensaje y nadie se entera hasta que las cuentas no cuadran.
    for (const otro of streams) {
      if (otro.config.name >= n) continue;
      const solape = overlap(s.config.subjects ?? [], otro.config.subjects ?? []);
      if (solape) {
        add(
          "error",
          `${n} ↔ ${otro.config.name}`,
          `capturan el mismo subject (${solape})`,
          "Cada mensaje se almacena dos veces y se entrega dos veces",
        );
      }
    }

    if (s.config.discard !== "old") {
      add("warn", n, `discard=${s.config.discard}`, "El canónico es 'old' — 02-naming.md §3.3");
    }
    if (!s.config.max_age || s.config.max_age === 0) {
      add(
        "warn",
        n,
        "sin max_age: retención infinita",
        "Un stream sin límite es una base de datos que crece sin fin, con sus obligaciones de RGPD — 06-security.md §5",
      );
    }
    if (n.startsWith("EVT_") && (s.config.duplicate_window ?? 0) === 0) {
      add(
        "warn",
        n,
        "duplicate_window = 0: Nats-Msg-Id no deduplica nada",
        "El canónico son 2m — 03-delivery.md §3",
      );
    }
    if (s.config.num_replicas === 1) {
      add("info", n, "replicas=1", "Producción usa 3 — 02-naming.md §3.2");
    }
  }

  console.log(
    table(
      streams.map((s) => [
        s.config.name,
        (s.config.subjects ?? []).join(", "),
        String(s.state.messages),
        s.config.max_age ? fmtDuration(s.config.max_age) : c.yellow("∞"),
        String(s.config.num_replicas ?? 1),
      ]),
      ["STREAM", "SUBJECTS", "MSGS", "MAX_AGE", "REPL"],
    ),
  );

  // Todo dominio con stream de eventos necesita su DLQ, o los eventos muertos no
  // tienen dónde caer y el SDK falla al enrutarlos.
  for (const s of evtStreams) {
    const dominio = s.config.name.slice(4);
    if (!dlqStreams.some((d) => d.config.name.slice(4) === dominio)) {
      add(
        "error",
        s.config.name,
        `no existe DLQ_${dominio}`,
        "Sin stream de DLQ, un evento que agota reintentos no tiene dónde ir — 04-errors.md",
      );
    }
  }
  for (const o of otros) {
    add("info", o.config.name, "no sigue la convención EVT_/DLQ_", "02-naming.md §3");
  }

  // ─── Consumidores ──────────────────────────────────────────────────────────
  console.log(c.bold("\nConsumidores"));
  const rows = [];
  let nConsumers = 0;

  for (const s of streams) {
    for await (const cons of jsm.consumers.list(s.config.name)) {
      nConsumers++;
      const cfg = cons.config;
      const name = cfg.durable_name ?? cons.name;
      const target = `${s.config.name}/${name}`;
      const problemas = [];

      // LA comprobación. JetStream sobrescribe ack_wait con backoff[0] sin avisar, y
      // el síntoma en producción es ejecución concurrente del mismo evento.
      const b0 = cfg.backoff?.[0];
      if (b0 !== undefined && cfg.ack_wait !== b0) {
        problemas.push("ack_wait≠backoff[0]");
        add(
          "error",
          target,
          `ack_wait=${fmtDuration(cfg.ack_wait)} pero backoff[0]=${fmtDuration(b0)}`,
          "Imposible por diseño del servidor — si lo ves, la versión de NATS cambió de comportamiento",
        );
      }
      if (cfg.ack_wait < 30 * S) {
        problemas.push("ack_wait corto");
        add(
          "error",
          target,
          `ack_wait=${fmtDuration(cfg.ack_wait)} < 30s`,
          "Todo handler que tarde más recibe el mensaje REENTREGADO mientras aún se ejecuta: ejecución concurrente del mismo evento — 03-delivery.md §2.1",
        );
      }
      if (cfg.ack_policy !== "explicit") {
        problemas.push("auto-ack");
        add(
          "error",
          target,
          `ack_policy=${cfg.ack_policy}`,
          "Con auto-ack un handler que falla pierde el evento en silencio — 03-delivery.md §2",
        );
      }
      if (cfg.backoff?.length && cfg.max_deliver !== cfg.backoff.length + 1) {
        problemas.push("backoff descuadrado");
        add(
          "warn",
          target,
          `max_deliver=${cfg.max_deliver} con ${cfg.backoff.length} backoffs`,
          `Deberían ser ${cfg.backoff.length + 1}: sobran o faltan entradas y las últimas no se aplican nunca`,
        );
      }
      if (cfg.max_deliver === -1 || cfg.max_deliver === undefined) {
        problemas.push("reintentos infinitos");
        add(
          "error",
          target,
          "max_deliver ilimitado",
          "Un evento envenenado reintenta para siempre y nunca llega a la DLQ — 04-errors.md",
        );
      }
      if (cfg.durable_name && !DURABLE_RE.test(cfg.durable_name)) {
        add(
          "warn",
          target,
          "el durable no sigue <servicio>__<subject_con_guiones>",
          "Sin el patrón no se puede saber qué servicio lo tiene abierto — 02-naming.md §4",
        );
      }

      // Señales operativas, no de configuración.
      if (cons.num_redelivered > 0) {
        add("info", target, `${cons.num_redelivered} reentregas acumuladas`, null);
      }
      if (cons.num_pending > 1000) {
        add("warn", target, `${cons.num_pending} mensajes pendientes`, "El consumidor no da abasto");
      }

      rows.push([
        s.config.name,
        name,
        cfg.ack_policy === "explicit" ? OK : c.red(cfg.ack_policy),
        fmtDuration(cfg.ack_wait),
        String(cfg.max_deliver),
        String(cons.num_pending),
        problemas.length ? c.red(problemas.join(", ")) : c.green("ok"),
      ]);
    }
  }

  console.log(
    rows.length
      ? table(rows, ["STREAM", "DURABLE", "ACK", "ACK_WAIT", "MAX_DEL", "PEND", "ESTADO"])
      : c.gray("  (ninguno)"),
  );

  // ─── DLQ ───────────────────────────────────────────────────────────────────
  const conMuertos = dlqStreams.filter((d) => d.state.messages > 0);
  if (conMuertos.length) {
    console.log(c.bold("\nDLQ"));
    for (const d of conMuertos) {
      console.log(
        `  ${c.yellow(String(d.state.messages).padStart(6))} en ${d.config.name}  ${c.gray(
          `flux dlq ls ${d.config.name.slice(4).toLowerCase()}`,
        )}`,
      );
      add(
        "warn",
        d.config.name,
        `${d.state.messages} eventos muertos sin revisar`,
        "Una DLQ que nadie mira es pérdida de datos con pasos extra — 04-errors.md §4",
      );
    }
  }

  // ─── Resumen ───────────────────────────────────────────────────────────────
  const errores = findings.filter((f) => f.level === "error");
  const avisos = findings.filter((f) => f.level === "warn");
  const infos = findings.filter((f) => f.level === "info");

  console.log(c.bold("\nDiagnóstico"));
  for (const f of [...errores, ...avisos, ...(verbose ? infos : [])]) {
    const icon = { error: FAIL, warn: WARN, info: c.blue("i") }[f.level];
    console.log(`  ${icon} ${c.bold(f.target)}  ${f.message}`);
    if (f.hint) console.log(`      ${c.gray(f.hint)}`);
  }

  if (!errores.length && !avisos.length) {
    console.log(`  ${OK} ${streams.length} streams y ${nConsumers} consumidores conformes`);
  } else {
    console.log(
      `\n  ${errores.length} error(es), ${avisos.length} aviso(s)` +
        (infos.length && !verbose ? c.gray(`, ${infos.length} nota(s) — usa -v`) : ""),
    );
  }

  return { findings, streams: streams.length, consumers: nConsumers };
}

/** ¿Hay algún subject capturado por ambos conjuntos de patrones? */
function overlap(a, b) {
  for (const x of a) for (const y of b) if (subjectsIntersect(x, y)) return `${x} ∩ ${y}`;
  return null;
}

function subjectsIntersect(a, b) {
  const ta = a.split(".");
  const tb = b.split(".");
  for (let i = 0; i < Math.max(ta.length, tb.length); i++) {
    const x = ta[i];
    const y = tb[i];
    if (x === ">" || y === ">") return true; // `>` traga el resto
    if (x === undefined || y === undefined) return false;
    if (x === "*" || y === "*") continue;
    if (x !== y) return false;
  }
  return true;
}
