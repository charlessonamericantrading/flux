/**
 * `flux tail` — ver eventos en vivo.
 *
 * Usa un consumidor efímero de solo lectura: mirar el bus NUNCA debe alterar el
 * estado de un consumidor de producción. Un `tail` que consuma mensajes de un durable
 * ajeno es una herramienta de depuración que provoca incidentes.
 */

import { c, reasonColor } from "./ui.mjs";

const dec = new TextDecoder();

export async function tail(js, jsm, pattern, { since, full, tenant, jsonOut }) {
  const domain = pattern.split(".")[0];
  if (domain.includes("*") || domain.includes(">")) {
    throw new Error(
      `el primer token debe ser un dominio concreto (recibido "${domain}"): ` +
        `los streams se organizan por dominio, así que no hay dónde suscribirse`,
    );
  }
  const stream = pattern.startsWith("dlq.")
    ? `DLQ_${pattern.split(".")[1].replace(/-/g, "_").toUpperCase()}`
    : `EVT_${domain.replace(/-/g, "_").toUpperCase()}`;

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
      let e;
      try {
        e = JSON.parse(dec.decode(m.data));
      } catch {
        console.log(`${c.magenta("POISON")} ${m.subject}  ${c.gray(dec.decode(m.data).slice(0, 120))}`);
        continue;
      }
      if (tenant && e.tenantid !== tenant) continue;
      console.log(jsonOut ? JSON.stringify(e) : render(e, m.subject, full));
    }
  } finally {
    await cleanup();
  }
}

function render(e, subject, full) {
  const hora = (e.time ?? "").slice(11, 23);
  const cabecera =
    `${c.gray(hora)} ${c.cyan(subject)}` +
    (e.subject ? ` ${c.bold(e.subject)}` : "") +
    (e.dlqreason ? ` ${reasonColor(e.dlqreason)(`[${e.dlqreason} ×${e.dlqattempts}]`)}` : "");

  if (full) {
    return `${cabecera}\n${JSON.stringify(e, null, 2)}`;
  }

  // El correlationid abreviado es lo que permite seguir un flujo a ojo entre
  // subjects distintos sin abrir un trazador.
  const corr = e.correlationid ? c.gray(`⟨${e.correlationid.slice(0, 8)}⟩`) : "";
  const oculto =
    e.dataclassification === "restricted" || e.dataclassification === "confidential";
  const data = oculto
    ? c.yellow(`[${e.dataclassification}]`) // no se vuelca PII a una terminal que acaba en un log
    : truncate(JSON.stringify(e.data), 160);

  return `${cabecera} ${corr} ${data}${e.dlqerror ? `\n    ${c.red(e.dlqerror)}` : ""}`;
}

const truncate = (s, n) => (s && s.length > n ? `${s.slice(0, n)}${c.gray("…")}` : s ?? "");

function parseSince(s) {
  const m = /^(\d+)([smhd])$/.exec(s);
  if (!m) throw new Error(`--since inválido: "${s}". Formato: 30s, 5m, 2h, 1d`);
  return Number(m[1]) * { s: 1e3, m: 6e4, h: 36e5, d: 864e5 }[m[2]];
}
