/**
 * `flux dlq` — triar y reproducir eventos muertos.
 *
 * "Una DLQ que nadie mira es una pérdida de datos con pasos extra" (04-errors.md §4).
 * Estos comandos existen para que mirarla sea barato.
 */

import { createInterface } from "node:readline/promises";
import { c, OK, FAIL, WARN, fmtAge, reasonColor, table } from "./ui.mjs";

const DLQ_EXTENSIONS = ["dlqreason", "dlqattempts", "dlqconsumer", "dlqerror", "dlqtime"];
const dec = new TextDecoder();

/** Lee la DLQ sin consumirla: un consumidor efímero no altera el estado de nadie. */
async function readDlq(js, jsm, stream, { limit = 500, subject } = {}) {
  const info = await jsm.streams.info(stream).catch(() => null);
  if (!info) return null;
  if (info.state.messages === 0) return [];

  const consumer = await jsm.consumers.add(stream, {
    ack_policy: "none",          // efímero y de solo lectura
    deliver_policy: "all",
    replay_policy: "instant",
    inactive_threshold: 30 * 1e9,
    ...(subject && { filter_subject: subject }),
  });

  const events = [];
  try {
    const c2 = await js.consumers.get(stream, consumer.name);
    const batch = await c2.fetch({ max_messages: limit, expires: 5000 });
    for await (const m of batch) {
      try {
        events.push({ seq: m.seq, subject: m.subject, event: JSON.parse(dec.decode(m.data)) });
      } catch {
        events.push({ seq: m.seq, subject: m.subject, event: null, raw: dec.decode(m.data) });
      }
      if (events.length >= limit) break;
    }
  } finally {
    await jsm.consumers.delete(stream, consumer.name).catch(() => {});
  }
  return events;
}

// ─── ls ──────────────────────────────────────────────────────────────────────

export async function dlqList(js, jsm, domain, { limit }) {
  const stream = `DLQ_${domain.replace(/-/g, "_").toUpperCase()}`;
  const events = await readDlq(js, jsm, stream, { limit });

  if (events === null) return console.log(`${WARN} no existe el stream ${stream}`);
  if (events.length === 0) return console.log(`${OK} ${stream} está vacía`);

  // Agrupar por (subject, razón, código de error): un incidente produce N eventos
  // idénticos, y verlos uno a uno esconde que son el mismo problema.
  const grupos = new Map();
  for (const { subject, event } of events) {
    const reason = event?.dlqreason ?? "?";
    const code = (event?.dlqerror ?? "").split(":")[0] || "?";
    const key = `${subject}|${reason}|${code}`;
    const g = grupos.get(key) ?? {
      subject, reason, code, count: 0, primero: null, ultimo: null, attempts: new Set(),
    };
    g.count++;
    if (event?.dlqattempts !== undefined) g.attempts.add(event.dlqattempts);
    const t = event?.dlqtime;
    if (t) {
      if (!g.primero || t < g.primero) g.primero = t;
      if (!g.ultimo || t > g.ultimo) g.ultimo = t;
    }
    grupos.set(key, g);
  }

  const rows = [...grupos.values()]
    .sort((a, b) => b.count - a.count)
    .map((g) => [
      String(g.count),
      reasonColor(g.reason)(g.reason),
      g.subject.replace(/^dlq\./, ""),
      g.code,
      [...g.attempts].sort().join(","),
      g.ultimo ? fmtAge(g.ultimo) : c.gray("?"),
    ]);

  console.log(table(rows, ["N", "RAZÓN", "SUBJECT", "CÓDIGO", "INTENTOS", "ÚLTIMO"]));
  console.log(
    c.gray(
      `\n${events.length} evento(s) leídos de ${stream}. ` +
        `Detalle: flux dlq inspect ${domain} --subject <subject>`,
    ),
  );

  const poison = [...grupos.values()].filter((g) => g.reason === "poison");
  if (poison.length) {
    console.log(
      `\n${FAIL} ${c.magenta("POISON detectado")} — casi siempre significa que un productor ` +
        `está roto o que alguien publicó a mano en el subject equivocado (04-errors.md §1.3)`,
    );
  }
}

// ─── inspect ─────────────────────────────────────────────────────────────────

export async function dlqInspect(js, jsm, domain, { subject, limit }) {
  const stream = `DLQ_${domain.replace(/-/g, "_").toUpperCase()}`;
  const events = await readDlq(js, jsm, stream, {
    limit,
    subject: subject ? (subject.startsWith("dlq.") ? subject : `dlq.${subject}`) : undefined,
  });

  if (!events?.length) return console.log(`${OK} nada que inspeccionar en ${stream}`);

  for (const { seq, subject: s, event, raw } of events) {
    console.log(c.bold(`\n─── seq ${seq}  ${s} ───`));
    if (!event) {
      console.log(`${FAIL} no es JSON: ${c.gray(raw?.slice(0, 200))}`);
      continue;
    }
    const rc = reasonColor(event.dlqreason);
    console.log(`  razón       ${rc(event.dlqreason)}  tras ${event.dlqattempts} intento(s)`);
    console.log(`  error       ${event.dlqerror ?? c.gray("—")}`);
    console.log(`  consumidor  ${event.dlqconsumer ?? c.gray("—")}`);
    console.log(`  id          ${event.id}  ${c.gray(fmtAge(event.dlqtime ?? event.time))}`);
    console.log(`  correlation ${event.correlationid ?? c.gray("—")}`);
    console.log(`  productor   ${event.source} ${c.gray(`v${event.producerversion ?? "?"}`)}`);
    if (event.dataclassification === "restricted" || event.dataclassification === "confidential") {
      // No se imprime el payload de datos sensibles: la terminal acaba en un log.
      console.log(
        `  data        ${c.yellow(`[oculto: dataclassification=${event.dataclassification}]`)} ` +
          c.gray("usa --show-data si de verdad lo necesitas"),
      );
    } else {
      console.log(`  data        ${JSON.stringify(event.data)}`);
    }
  }
}

// ─── replay ──────────────────────────────────────────────────────────────────

export async function dlqReplay(js, jsm, domain, { subject, limit, confirm, yes }) {
  const stream = `DLQ_${domain.replace(/-/g, "_").toUpperCase()}`;
  const filtro = subject
    ? subject.startsWith("dlq.") ? subject : `dlq.${subject}`
    : undefined;

  const events = await readDlq(js, jsm, stream, { limit, subject: filtro });
  if (!events?.length) return console.log(`${OK} nada que reproducir en ${stream}`);

  const reproducibles = events.filter((e) => e.event && e.event.dlqreason !== "poison");
  const poison = events.length - reproducibles.length;

  console.log(
    `${reproducibles.length} evento(s) reproducibles` +
      (poison ? c.gray(`, ${poison} POISON omitido(s) — no son eventos válidos`) : ""),
  );

  const porSubject = new Map();
  for (const { event } of reproducibles) {
    const dest = destino(event);
    porSubject.set(dest, (porSubject.get(dest) ?? 0) + 1);
  }
  for (const [s, n] of porSubject) console.log(`  ${String(n).padStart(5)} → ${s}`);

  if (!confirm) {
    console.log(
      `\n${c.bold("Simulación.")} Nada se ha publicado. Para ejecutarlo de verdad:\n` +
        `  ${c.cyan(`flux dlq replay ${domain}${subject ? ` --subject ${subject}` : ""} --confirm`)}`,
    );
    return;
  }

  // Las dos comprobaciones que 04-errors.md §4.1 exige antes de reproducir. Son
  // preguntas que solo un humano puede responder, así que se preguntan.
  if (!yes) {
    console.log(
      `\n${c.bold("Antes de reproducir:")}\n` +
        `  1. ¿Se ha arreglado la causa? Reproducir contra el mismo bug devuelve\n` +
        `     los eventos a la DLQ y ensucia el rastro.\n` +
        `  2. ¿El consumidor es idempotente para estos id? El replay CONSERVA el id\n` +
        `     original; si el handler llegó a aplicar un efecto parcial, la tabla de\n` +
        `     eventos procesados es lo único que impide duplicarlo.`,
    );
    const rl = createInterface({ input: process.stdin, output: process.stdout });
    const r = await rl.question(`\nPublicar ${reproducibles.length} evento(s)? [escribe "si"] `);
    rl.close();
    if (r.trim().toLowerCase() !== "si") return console.log(`${WARN} cancelado`);
  }

  let ok = 0;
  const fallos = [];
  for (const { event } of reproducibles) {
    const limpio = { ...event };
    for (const ext of DLQ_EXTENSIONS) delete limpio[ext];
    try {
      // msgID = el id ORIGINAL: regenerarlo rompería la idempotencia de todos los
      // consumidores aguas abajo y convertiría una recuperación en un incidente
      // nuevo — 04-errors.md §4.1.
      await js.publish(destino(event), new TextEncoder().encode(JSON.stringify(limpio)), {
        msgID: limpio.id,
      });
      ok++;
    } catch (e) {
      fallos.push({ id: limpio.id, error: e.message });
    }
  }

  console.log(`\n${OK} ${ok} evento(s) republicados conservando su id original`);
  if (fallos.length) {
    console.log(`${FAIL} ${fallos.length} fallo(s):`);
    for (const f of fallos.slice(0, 10)) console.log(`  ${f.id}  ${f.error}`);
  }
  console.log(
    c.gray(
      "\nLos originales siguen en la DLQ: este comando copia, no mueve. Bórralos con\n" +
        `  nats stream purge ${stream} --subject '<subject>'  cuando confirmes que se procesaron.`,
    ),
  );
}

/** El subject de destino sale del `type`, no del subject de DLQ: es la fuente fiable. */
function destino(event) {
  const m = /^com\.flux\.([a-z0-9-]+)\.([a-z0-9-]+)\.([a-z0-9-]+)\.v([1-9][0-9]*)$/.exec(
    event.type ?? "",
  );
  if (m) return `${m[1]}.${m[2]}.v${m[4]}.${m[3]}`;
  throw new Error(`no se puede derivar el subject original de type="${event.type}"`);
}
