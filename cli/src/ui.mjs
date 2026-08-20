/** Formato de salida. Sin dependencias: los colores ANSI son 8 líneas. */

const useColor = process.stdout.isTTY && !process.env.NO_COLOR;
const wrap = (code) => (s) => (useColor ? `\x1b[${code}m${s}\x1b[0m` : String(s));

export const c = {
  bold: wrap("1"),
  dim: wrap("2"),
  red: wrap("31"),
  green: wrap("32"),
  yellow: wrap("33"),
  blue: wrap("34"),
  magenta: wrap("35"),
  cyan: wrap("36"),
  gray: wrap("90"),
};

export const OK = c.green("✓");
export const FAIL = c.red("✗");
export const WARN = c.yellow("!");

/** Colorea por clase de DLQ: poison exige atención inmediata. */
export function reasonColor(reason) {
  return { poison: c.magenta, permanent: c.red, retryable: c.yellow }[reason] ?? c.gray;
}

export function fmtBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 ** 2).toFixed(1)} MB`;
}

export function fmtDuration(ns) {
  const s = Number(ns) / 1e9;
  // Un consumidor con ack_policy=none no trae ack_wait. Mostrar "NaNd" convertiría un
  // dato ausente en uno aparentemente absurdo, y el lector perseguiría el fantasma.
  if (!Number.isFinite(s)) return c.gray("—");
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  if (s < 86400) return `${Math.round(s / 3600)}h`;
  return `${Math.round(s / 86400)}d`;
}

/** Edad relativa: "hace 3m" pesa más que un timestamp al triar una DLQ. */
export function fmtAge(iso) {
  const ms = Date.now() - Date.parse(iso);
  if (!Number.isFinite(ms)) return c.gray("?");
  const s = Math.floor(ms / 1000);
  if (s < 60) return `hace ${s}s`;
  if (s < 3600) return `hace ${Math.floor(s / 60)}m`;
  if (s < 86400) return `hace ${Math.floor(s / 3600)}h`;
  return `hace ${Math.floor(s / 86400)}d`;
}

export function table(rows, headers) {
  if (rows.length === 0) return "";
  const widths = headers.map((h, i) =>
    Math.max(h.length, ...rows.map((r) => stripAnsi(String(r[i] ?? "")).length)),
  );
  const line = (cells) =>
    cells
      .map((cell, i) => {
        const pad = widths[i] - stripAnsi(String(cell ?? "")).length;
        return String(cell ?? "") + " ".repeat(Math.max(0, pad));
      })
      .join("  ")
      .trimEnd();
  return [c.bold(line(headers)), ...rows.map(line)].join("\n");
}

const stripAnsi = (s) => s.replace(/\x1b\[[0-9;]*m/g, "");

export function die(message, code = 1) {
  console.error(`${FAIL} ${message}`);
  process.exit(code);
}
