/**
 * Cómo se cuenta lo observado: una línea por paso, y las discrepancias de los que fallan. Lo comparten `conformance/security.mjs` y
 * `conformance/interop-rpc.mjs`.
 */

/** Lo observado, en una línea. */
export function lineaObservado(o) {
  if (!o) return "";
  const partes = [];
  if (o.outcome !== undefined) partes.push(String(o.outcome));
  for (const k of [
    "stream", "subject", "leaked", "victimRead", "count", "ackPending", "numPending", "ackTokens", "deliveryCount", "violation", "childExit", "serverViolationLines",
    "subscription", "received", "delivered", "ownerDenied", "callerViolations", "answered", "failed", "byInstance", "stopOutcome", "stopMs",
  ]) {
    if (o[k] !== undefined && o[k] !== null && o[k] !== "") partes.push(`${k}=${typeof o[k] === "object" ? JSON.stringify(o[k]) : o[k]}`);
  }
  if (o.reply?.error) partes.push(`error ${o.reply.error.err_code ?? ""} «${o.reply.error.description}»`);
  // Los escenarios con SDKs reales (`sdk_script`): el error del SDK (código, clase), los tiempos y lo que el broker anotó.
  if (o.ok === false && o.error) partes.push(`${o.error.name ?? "error"} ${o.error.code ?? ""}${o.error.class ? ` (${o.error.class})` : ""} «${String(o.error.message ?? "").slice(0, 110)}»`);
  for (const k of ["codes", "p50Ms", "maxMs", "wallMs", "serverViolationCount", "total", "replyMatches", "mailbox"]) {
    if (o[k] !== undefined && o[k] !== null) partes.push(`${k}=${typeof o[k] === "object" ? JSON.stringify(o[k]) : typeof o[k] === "number" ? Math.round(o[k] * 10) / 10 : o[k]}`);
  }
  if (o.data && typeof o.data === "object" && o.ok) partes.push(`data=${JSON.stringify(o.data).slice(0, 140)}`);
  if (Array.isArray(o.serverViolations) && o.serverViolations.length) partes.push(`servidor: ${o.serverViolations.map((l) => l.slice(0, 120)).join(" | ")}`);
  if (Array.isArray(o.logs) && o.logs.length) partes.push(`log SDK: ${o.logs.map((l) => l.slice(0, 160)).join(" | ")}`);
  if (o.text) partes.push(`«${String(o.text).split("\n")[0].slice(0, 130)}»`);
  if (typeof o.ms === "number") partes.push(`${o.ms} ms`);
  return partes.join(" · ");
}

export function imprimirPaso(p, verbose = false) {
  if (p.omitido) {
    console.log(`  - ${p.descripcion}\n      omitido: ${p.omitido}`);
    return;
  }
  const marca = p.ok ? "✓" : "✗";
  const control = p.control ? " (control)" : "";
  console.log(`  ${marca} ${p.descripcion}${control}`);
  if (p.knownDeviation) console.log(`      ⚠ desviación conocida: ${p.knownDeviation}`);
  if (!p.ok || verbose) console.log(`      → ${lineaObservado(p.observado)}`);
  for (const f of p.fallos ?? []) console.log(`      ✗ ${f}`);
  if (verbose && p.note) console.log(`      · ${p.note}`);
}

