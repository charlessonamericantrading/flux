/**
 * Genera `RESULTS.md` a partir de los DATOS de una ejecución (`datos/*.json`). Todo número del informe sale de los datos; lo único
 * escrito a mano es la «Lectura» (`bench/lectura.md`), que se incluye tal cual y dice sobre qué ejecución se escribió.
 *
 * Cada celda es `mediana [mín–máx]` de las repeticiones del punto. Las latencias son la mediana (entre repeticiones) del percentil
 * de cada repetición. Una repetición que falló no entra en las cifras y se cuenta aparte.
 */

import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { mediana, resumir, suma } from "./estadistica.mjs";

const BENCH = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");

// ─── formato ─────────────────────────────────────────────────────────────────

const num = (x, d = 1) => (Number.isFinite(x) ? x.toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d }).replace(/,/g, "") : "—");
/** Un valor con decimales «razonables»: más cuanto más pequeño. */
const cifra = (x) => {
  if (!Number.isFinite(x)) return "—";
  const a = Math.abs(x);
  if (a === 0) return "0";
  if (a >= 1000) return num(x, 0);
  if (a >= 100) return num(x, 0);
  if (a >= 10) return num(x, 1);
  if (a >= 1) return num(x, 2);
  return num(x, 3);
};
/** Milisegundos → `ms` o `s` según su tamaño. */
const dur = (ms) => {
  if (!Number.isFinite(ms)) return "—";
  if (ms >= 1000) return `${num(ms / 1000, ms >= 10_000 ? 1 : 2)} s`;
  if (ms >= 100) return `${num(ms, 0)} ms`;
  if (ms >= 10) return `${num(ms, 1)} ms`;
  return `${num(ms, 2)} ms`;
};
const celda = (xs, f = cifra) => {
  const r = resumir(xs);
  if (!r.n) return "—";
  if (r.n === 1 || r.min === r.max) return f(r.mediana);
  return `${f(r.mediana)} [${f(r.min)}–${f(r.max)}]`;
};
const ok = (reps) => reps.filter((r) => !r.error);

// ─── métricas por repetición ─────────────────────────────────────────────────

const cpuTotal = (r) => r.cpu.servidor + r.cpu.clientes + r.cpu.nats;
const pct = (s, r) => (s / r.durS) * 100;
const kib = (b) => b / 1024;

const metA = {
  msgsMin: (r) => ((r.metodo === "flux" ? r.nats.in_msgs : r.http.peticiones) / r.durS) * 60,
  kibMin: (r) => kib((r.metodo === "flux" ? r.nats.in_bytes + r.nats.out_bytes : r.http.bytes) / r.durS) * 60,
  cpuPct: (r) => pct(cpuTotal(r), r),
  reparto: (r) => `${cifra(pct(r.cpu.clientes, r)).padStart(0)} / ${cifra(pct(r.cpu.servidor, r))} / ${cifra(pct(r.cpu.nats, r))}`,
};
const metB = {
  msgsHecho: (r) => (r.metodo === "flux" ? r.nats.in_msgs : r.http.recibidas) / Math.max(1, r.hechosEnVentana),
  kibHecho: (r) => kib(r.metodo === "flux" ? r.nats.in_bytes + r.nats.out_bytes : r.http.bytes) / Math.max(1, r.hechosEnVentana),
  cpuMsHecho: (r) => (cpuTotal(r) * 1000) / Math.max(1, r.hechosEnVentana),
};
const metC = {
  usLlamada: (s, r) => (s * 1e6) / Math.max(1, r.peticiones),
  cpuUs: (r) => (cpuTotal(r) * 1e6) / Math.max(1, r.peticiones),
  bytes: (r) => (r.metodo === "http-ka" || r.metodo === "http-cerrar" ? r.http.bytes : r.nats.in_bytes + r.nats.out_bytes) / Math.max(1, r.peticiones),
  msgs: (r) => (r.metodo === "http-ka" || r.metodo === "http-cerrar" ? 1 : r.nats.in_msgs / Math.max(1, r.peticiones)),
  conexiones: (r) => (r.http ? r.http.conexiones / Math.max(1, r.peticiones) : 0),
};

const fallos = (reps) => reps.flatMap((r) => (r.comprobaciones ?? []).filter((c) => !c.ok));
const estadoPunto = (reps) => {
  const e = reps.filter((r) => r.error).length;
  const f = ok(reps).filter((r) => (r.comprobaciones ?? []).some((c) => !c.ok && !/fiabilidad/.test(c.nombre))).length;
  const d = ok(reps).filter((r) => r.dudosa || (r.avisos ?? []).length).length;
  return `${e ? `✗${e} ` : ""}${f ? `⚠${f} ` : ""}${d ? `~${d}` : ""}`.trim() || "✓";
};

// ─── tablas ──────────────────────────────────────────────────────────────────

/** `izq`: cuántas columnas (las primeras) van alineadas a la izquierda; el resto, números, a la derecha. */
const tabla = (cabecera, filas, izq = 1) =>
  [`| ${cabecera.join(" | ")} |`, `|${cabecera.map((_, i) => (i < izq ? "---" : "---:")).join("|")}|`, ...filas.map((f) => `| ${f.join(" | ")} |`)].join("\n");

const NOMBRE = {
  ingenuo: "HTTP sondeo ingenuo", etag: "HTTP sondeo con ETag", flux: "flux `state.watch`",
  webhook: "HTTP webhooks (N POST)", "http-ka": "HTTP keep-alive", "http-cerrar": "HTTP sin keep-alive",
  "nats-crudo": "NATS crudo (sin SDK)", "flux-otel": "flux (con OpenTelemetry presente, experimento)",
};
const nombre = (m, escenario) => (escenario === "C" && m === "flux" ? "flux `request`/`respond`" : escenario === "B" && m === "flux" ? "flux 1 evento + N durables" : NOMBRE[m] ?? m);

function puntosDe(datos, escenario, filtro = () => true) {
  return datos.puntos.filter((p) => p.escenario === escenario && filtro(p.params));
}

function tablaA(datos, titulo, filtro, variable, etiquetaVar) {
  const ps = puntosDe(datos, "A", filtro);
  if (!ps.length) return "";
  const valores = [...new Set(ps.map((p) => p.params[variable]))].sort((a, b) => a - b);
  const filas = [];
  for (const v of valores) {
    let primera = true;
    for (const m of ["ingenuo", "etag", "flux"]) {
      const p = ps.find((q) => q.params[variable] === v && q.params.metodo === m);
      if (!p) continue;
      const rs = ok(p.reps);
      filas.push([
        primera ? String(v) : "",
        nombre(m, "A"),
        celda(rs.map(metA.msgsMin)),
        celda(rs.map(metA.cpuPct)),
        celda(rs.map((r) => pct(r.cpu.clientes, r))),
        celda(rs.map((r) => pct(r.cpu.servidor, r))),
        celda(rs.map((r) => pct(r.cpu.nats, r))),
        celda(rs.map(metA.kibMin)),
        celda(rs.map((r) => r.latencia?.p50).filter(Number.isFinite), dur),
        celda(rs.map((r) => r.latencia?.p99).filter(Number.isFinite), dur),
        celda(rs.map((r) => r.latencia?.max).filter(Number.isFinite), dur),
        String(suma(rs.map((r) => r.latencia?.n ?? 0))),
        estadoPunto(p.reps),
      ]);
      primera = false;
    }
  }
  return [
    `#### ${titulo}`,
    "",
    tabla(
      [etiquetaVar, "Método", "Peticiones o mensajes emitidos /min", "CPU total (% de un núcleo)", "· clientes", "· servidor", "· nats-server", "KiB en la red /min", "Enterarse p50", "p99", "máx", "n", "Estado"],
      filas,
      2,
    ),
    "",
  ].join("\n");
}

function tablaB(datos, titulo, filtro, variable, etiquetaVar) {
  const ps = puntosDe(datos, "B", filtro);
  if (!ps.length) return "";
  const valores = [...new Set(ps.map((p) => p.params[variable]))].sort((a, b) => a - b);
  const filas = [];
  for (const v of valores) {
    let primera = true;
    for (const m of ["webhook", "flux"]) {
      const p = ps.find((q) => q.params[variable] === v && q.params.metodo === m);
      if (!p) continue;
      const rs = ok(p.reps);
      filas.push([
        primera ? String(v) : "",
        nombre(m, "B"),
        celda(rs.map(metB.msgsHecho)),
        celda(rs.map(metB.cpuMsHecho)),
        celda(rs.map((r) => (r.cpu.servidor * 1000) / Math.max(1, r.hechosEnVentana))),
        celda(rs.map((r) => (r.cpu.clientes * 1000) / Math.max(1, r.hechosEnVentana))),
        celda(rs.map((r) => (r.cpu.nats * 1000) / Math.max(1, r.hechosEnVentana))),
        celda(rs.map(metB.kibHecho)),
        celda(rs.map((r) => r.latencia?.p50).filter(Number.isFinite), dur),
        celda(rs.map((r) => r.latencia?.p99).filter(Number.isFinite), dur),
        celda(rs.map((r) => r.latencia?.max).filter(Number.isFinite), dur),
        estadoPunto(p.reps),
      ]);
      primera = false;
    }
  }
  return [
    `#### ${titulo}`,
    "",
    tabla(
      [etiquetaVar, "Método", "Peticiones o mensajes emitidos /hecho", "CPU total (ms /hecho)", "· productor", "· receptores (suma)", "· nats-server", "KiB en la red /hecho", "Entrega p50", "p99", "máx", "Estado"],
      filas,
      2,
    ),
    "",
  ].join("\n");
}

function tablaC(datos, titulo, filtro) {
  const ps = puntosDe(datos, "C", filtro);
  if (!ps.length) return "";
  const concs = [...new Set(ps.map((p) => p.params.conc))].sort((a, b) => a - b);
  const filas = [];
  for (const c of concs) {
    let primera = true;
    for (const m of ["http-ka", "http-cerrar", "nats-crudo", "flux", "flux-otel"]) {
      const p = ps.find((q) => q.params.conc === c && q.params.metodo === m);
      if (!p) continue;
      const rs = ok(p.reps);
      filas.push([
        primera ? String(c) : "",
        nombre(m, "C"),
        celda(rs.map((r) => r.porSegundo), (x) => num(x, 0)),
        celda(rs.map((r) => r.latencia?.p50), dur),
        celda(rs.map((r) => r.latencia?.p99), dur),
        celda(rs.map((r) => r.latencia?.max), dur),
        celda(rs.map(metC.cpuUs)),
        celda(rs.map((r) => metC.usLlamada(r.cpu.clientes, r))),
        celda(rs.map((r) => metC.usLlamada(r.cpu.servidor, r))),
        celda(rs.map((r) => metC.usLlamada(r.cpu.nats, r))),
        celda(rs.map(metC.bytes), (x) => num(x, 0)),
        celda(rs.map(metC.conexiones), (x) => num(x, 3)),
        estadoPunto(p.reps),
      ]);
      primera = false;
    }
  }
  return [
    `#### ${titulo}`,
    "",
    tabla(
      ["Concurrencia", "Método", "Llamadas /s", "p50", "p99", "máx", "CPU total (µs /llamada)", "· cliente", "· servidor", "· nats-server", "Bytes /llamada", "Conexiones TCP nuevas /llamada", "Estado"],
      filas,
      2,
    ),
    "",
  ].join("\n");
}

// ─── lectura automática: cruces calculados con los datos ─────────────────────

function repsDe(datos, escenario, params) {
  const p = datos.puntos.find((q) => q.escenario === escenario && Object.entries(params).every(([k, v]) => (q.params[k] ?? false) === v));
  return p ? ok(p.reps) : [];
}
const med = (rs, f) => mediana(rs.map(f).filter(Number.isFinite));
const veces = (a, b) => (Number.isFinite(a) && Number.isFinite(b) && b > 0 ? a / b : NaN);
const x = (v) => (Number.isFinite(v) ? (v >= 10 ? `${num(v, 0)}×` : v >= 1 ? `${num(v, 1)}×` : `${num(v, 2)}×`) : "—");

function cruces(datos) {
  const { c, t } = datos.meta.centroA;
  const ns = [...new Set(datos.puntos.filter((p) => p.escenario === "A" && !p.params.tls).map((p) => p.params.n))].sort((a, b) => a - b);
  const out = [];
  for (const n of ns) {
    const e = repsDe(datos, "A", { metodo: "etag", n, c, t, tls: false });
    const f = repsDe(datos, "A", { metodo: "flux", n, c, t, tls: false });
    if (!e.length || !f.length) continue;
    out.push({
      n,
      cpu: veces(med(f, metA.cpuPct), med(e, metA.cpuPct)),
      kib: veces(med(f, metA.kibMin), med(e, metA.kibMin)),
      msgs: veces(med(f, metA.msgsMin), med(e, metA.msgsMin)),
      lat: veces(med(f, (r) => r.latencia?.p50), med(e, (r) => r.latencia?.p50)),
    });
  }
  return out;
}

function crucesB(datos) {
  const { h } = datos.meta.centroB;
  const ns = [...new Set(datos.puntos.filter((p) => p.escenario === "B" && !p.params.tls).map((p) => p.params.n))].sort((a, b) => a - b);
  const out = [];
  for (const n of ns) {
    const w = repsDe(datos, "B", { metodo: "webhook", n, h, tls: false });
    const f = repsDe(datos, "B", { metodo: "flux", n, h, tls: false });
    if (!w.length || !f.length) continue;
    out.push({
      n,
      cpu: veces(med(f, metB.cpuMsHecho), med(w, metB.cpuMsHecho)),
      prod: veces(med(f, (r) => r.cpu.servidor / r.hechosEnVentana), med(w, (r) => r.cpu.servidor / r.hechosEnVentana)),
      kib: veces(med(f, metB.kibHecho), med(w, metB.kibHecho)),
      lat: veces(med(f, (r) => r.latencia?.p50), med(w, (r) => r.latencia?.p50)),
    });
  }
  return out;
}

function lecturaAutomatica(datos) {
  const l = [];
  const A = cruces(datos);
  if (A.length) {
    const { c, t } = datos.meta.centroA;
    l.push(`**A. Enterarse de un cambio** (C = ${c} cambios/min, T = ${t} s; flux frente al sondeo con ETag; \`×\` = flux / sondeo: menos de 1 es que flux gasta menos):`, "");
    l.push(tabla(["N", "CPU total", "Bytes", "Mensajes emitidos", "Tiempo hasta enterarse (p50)"], A.map((a) => [String(a.n), x(a.cpu), x(a.kib), x(a.msgs), x(a.lat)])), "");
  }
  const Bc = crucesB(datos);
  if (Bc.length) {
    l.push(`**B. Avisar a muchos** (H = ${datos.meta.centroB.h} hechos/min; flux frente a N webhooks; \`×\` = flux / webhooks):`, "");
    l.push(tabla(["N", "CPU total por hecho", "CPU del productor por hecho", "Bytes por hecho", "Entrega (p50)"], Bc.map((a) => [String(a.n), x(a.cpu), x(a.prod), x(a.kib), x(a.lat)])), "");
  }
  const cs = [];
  for (const conc of [1, 16, 256]) {
    const ka = repsDe(datos, "C", { metodo: "http-ka", conc, tls: false });
    const fl = repsDe(datos, "C", { metodo: "flux", conc, tls: false });
    const cr = repsDe(datos, "C", { metodo: "nats-crudo", conc, tls: false });
    const ot = repsDe(datos, "C", { metodo: "flux-otel", conc, tls: false });
    if (!ka.length || !fl.length) continue;
    cs.push([
      String(conc),
      x(veces(med(ka, (r) => r.porSegundo), med(fl, (r) => r.porSegundo))),
      x(veces(med(fl, (r) => r.latencia?.p50), med(ka, (r) => r.latencia?.p50))),
      x(veces(med(fl, metC.cpuUs), med(ka, metC.cpuUs))),
      cr.length ? x(veces(med(cr, metC.cpuUs), med(ka, metC.cpuUs))) : "—",
      ot.length ? x(veces(med(ot, metC.cpuUs), med(ka, metC.cpuUs))) : "—",
    ]);
  }
  if (cs.length) {
    l.push("**C. Llamada con respuesta** (todo frente a HTTP con keep-alive; `×` mayor que 1 en latencia o CPU es PEOR que HTTP):", "");
    l.push(tabla(["Concurrencia", "HTTP rinde (llamadas/s) ×  más que flux", "Latencia p50 de flux ×", "CPU /llamada de flux ×", "CPU /llamada de NATS crudo ×", "CPU /llamada de flux con OTel ×"], cs), "");
  }
  return l.join("\n");
}

// ─── dónde se gasta la CPU ───────────────────────────────────────────────────

function tablaSistema(datos) {
  const filas = [];
  for (const esc of ["A", "B", "C"]) {
    const metodos = [...new Set(datos.puntos.filter((p) => p.escenario === esc && !p.params.tls).map((p) => p.params.metodo))];
    for (const m of metodos) {
      const rs = datos.puntos.filter((p) => p.escenario === esc && !p.params.tls && p.params.metodo === m).flatMap((p) => ok(p.reps));
      const f = (k) => {
        const v = rs.map((r) => r.sistema?.[k]).filter(Number.isFinite);
        return v.length ? `${num(mediana(v) * 100, 0)} %` : "—";
      };
      filas.push([esc, nombre(m, esc), f("clientes"), f("servidor"), f("nats")]);
    }
  }
  return tabla(["Escenario", "Método", "clientes", "servidor", "nats-server"], filas, 2);
}

// ─── calidad de la medida ────────────────────────────────────────────────────

function calidad(datos) {
  const reps = datos.puntos.flatMap((p) => p.reps.map((r) => ({ ...r, _p: p })));
  const buenas = reps.filter((r) => !r.error);
  const ajena = buenas.map((r) => r.ajena).filter(Number.isFinite).sort((a, b) => a - b);
  const q = (p) => ajena[Math.min(ajena.length - 1, Math.max(0, Math.ceil(p * ajena.length) - 1))] ?? NaN;
  const conAviso = (re) => buenas.filter((r) => (r.avisos ?? []).some((a) => re.test(a))).length;
  const intentos = suma(buenas.map((r) => (r.intentos ?? 1) - 1));
  const comandos = new Map();
  for (const r of buenas) for (const c of r.ajenosTop ?? []) comandos.set(c.comando, (comandos.get(c.comando) ?? 0) + 1);
  const topCmd = [...comandos.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8).map(([c, n]) => `\`${c}\` (${n})`).join(", ") || "ninguno";
  const cargaAntes = buenas.map((r) => r.cargaAntes?.[0]).filter(Number.isFinite);
  const cargaDesp = buenas.map((r) => r.cargaDespues?.[0]).filter(Number.isFinite);
  const cargada = Number.isFinite(q(0.5)) && (q(0.5) > 0.25 || q(0.95) > 1);
  return {
    cargada,
    texto: [
      tabla(
        ["", ""],
        [
          ["Repeticiones medidas (sin contar las que fallaron)", `${buenas.length} de ${reps.length}`],
          ["Repeticiones repetidas por aviso de fiabilidad", String(intentos)],
          ["Carga media (1 min) al empezar cada repetición: mediana / máx", `${cifra(mediana(cargaAntes))} / ${cifra(Math.max(...cargaAntes))}`],
          ["Carga media (1 min) al terminar cada repetición: mediana / máx", `${cifra(mediana(cargaDesp))} / ${cifra(Math.max(...cargaDesp))}`],
          ["CPU gastada por procesos AJENOS al banco (núcleos): mediana / p95 / máx", `${cifra(q(0.5))} / ${cifra(q(0.95))} / ${cifra(q(1))}`],
          ["Repeticiones con carga ajena > 0.25 núcleos", `${conAviso(/carga ajena/)} de ${buenas.length}`],
          ["Repeticiones con la máquina casi llena (> 85 % de las CPU)", `${conAviso(/máquina casi llena/)} de ${buenas.length}`],
          ["Repeticiones con algún proceso del banco saturado (> 85 % de un núcleo)", `${conAviso(/saturado/)} de ${buenas.length}`],
          ["Procesos ajenos más frecuentes entre los ocupados", topCmd],
        ],
      ),
    ].join("\n"),
  };
}

function comprobaciones(datos) {
  const reps = datos.puntos.flatMap((p) => p.reps.filter((r) => !r.error).map((r) => ({ r, p })));
  const por = new Map();
  for (const { r, p } of reps) {
    for (const c of r.comprobaciones ?? []) {
      const k = `${p.escenario}: ${c.nombre}`;
      const e = por.get(k) ?? { ok: 0, total: 0, fallos: [] };
      e.total++;
      if (c.ok) e.ok++;
      else if (e.fallos.length < 3) e.fallos.push(`${p.clave} → ${c.detalle}`);
      por.set(k, e);
    }
  }
  const filas = [...por.entries()].sort().map(([k, e]) => [k, `${e.ok} / ${e.total}`, e.ok === e.total ? "✓" : "⚠"]);
  const malas = [...por.entries()].filter(([, e]) => e.fallos.length);
  const detalle = malas.length
    ? ["", "Comprobaciones que fallaron (el punto se publica igualmente, marcado):", "", ...malas.flatMap(([k, e]) => [`- **${k}**`, ...e.fallos.map((f) => `  - ${f}`)])].join("\n")
    : "\nTodas las comprobaciones pasaron en todas las repeticiones medidas.";
  return { tabla: tabla(["Comprobación", "Repeticiones que la cumplen", ""], filas), detalle };
}

// ─── el documento ────────────────────────────────────────────────────────────

export function generarInforme(datos) {
  const e = datos.meta.entorno;
  const q = calidad(datos);
  const cmp = comprobaciones(datos);
  const lecturaFichero = path.join(BENCH, "lectura.md");
  const lectura = existsSync(lecturaFichero) && !datos.meta.rapido ? readFileSync(lecturaFichero, "utf8").trim() : "";
  const L = [];
  const push = (...xs) => L.push(...xs);

  push("# Resultados de `bench/`", "");
  push("> **Generado por `node bench/run.mjs`. No lo edites a mano:** cada número sale de los datos de la ejecución y se regenera con el mismo comando.");
  if (datos.meta.rapido) {
    push(">", "> **MODO RÁPIDO — PRUEBA DE HUMO.** Una repetición por punto, ventanas de segundos y una malla mínima. Comprueba que el banco corre y que este fichero tiene su estructura; **sus cifras NO son resultados** y no deben citarse.");
  }
  if (q.cargada) {
    push(">", `> **MEDIDO CON LA MÁQUINA CARGADA.** Durante la ejecución hubo otros procesos gastando CPU (mediana ${cifra(mediana(datos.puntos.flatMap((p) => p.reps).map((r) => r.ajena).filter(Number.isFinite)))} núcleos ajenos, ver «Calidad de la medida»). Fíate de las cifras **robustas a la carga** (mensajes y peticiones emitidos, bytes en la red, segundos de CPU por proceso) y trata como **orientativas** las **sensibles** (latencias p50/p99/máx y llamadas por segundo): un vecino ocupado alarga las esperas y recorta el rendimiento sin que flux ni HTTP tengan nada que ver. Una corrida con la máquina tranquila las sustituye.`);
  }
  push("", `Ejecución: ${datos.meta.inicio} · duración ${datos.meta.duracionTexto} · ${datos.meta.reps} repetición(es) por punto · modo ${datos.meta.rapido ? "rápido" : "completo"}.`, "");

  push("## Cómo leer las tablas", "");
  push(
    "- Cada celda es **mediana [mín–máx]** de las repeticiones del punto. Las latencias son la mediana, entre repeticiones, del percentil de cada una (`n` = muestras sumadas).",
    "- **Robustas a la carga de la máquina:** peticiones o mensajes emitidos, bytes en la red, segundos de CPU de cada proceso. **Sensibles:** latencias y llamadas por segundo.",
    "- CPU: `/proc/<pid>` de cada proceso desde fuera (clientes = suma de todos los procesos de consumidores; servidor = el CRM o el productor; `nats-server`). «% de un núcleo» es CPU / tiempo de ventana: 100 % = un núcleo entero.",
    "- Bytes: de APLICACIÓN en los sockets (HTTP: `bytesRead + bytesWritten` del lado servidor; NATS: `in_bytes + out_bytes` de `/varz`). Sin cabeceras TCP/IP y, con TLS, ya descifrados (ni `/varz` ni el `TLSSocket` de Node cuentan los registros TLS: el coste en bytes de TLS no está en estas tablas).",
    "- Peticiones o mensajes emitidos: HTTP = peticiones recibidas por el servidor; flux = `in_msgs` de `nats-server` (todo lo que los clientes le mandan: publicaciones, peticiones de mensajes, acuses…).",
    "- **Estado**: `✓` sin incidencias; `⚠n` = n repeticiones con alguna comprobación del banco fallida; `~n` = n repeticiones con avisos de fiabilidad (proceso saturado, carga ajena); `✗n` = n repeticiones que fallaron.",
    "",
  );

  push("## Lectura", "");
  push("### Cruces calculados con los datos", "", lecturaAutomatica(datos), "");
  if (lectura) push("### Lectura escrita a mano", "", lectura, "");

  push("## Escenario A — enterarse de un cambio", "");
  push(
    "N consumidores quieren saber cuanto antes que un cliente cambió; el CRM lo cambia C veces por minuto. Línea base: cada consumidor sondea `GET /clientes/cli-1` cada T segundos con keep-alive (`node:http`, un `Agent` y una conexión por consumidor), «ingenuo» (siempre 200 con el cuerpo) o con `If-None-Match` (304). flux: cada consumidor es un servicio con su `bus.state.watch`. El centro de los barridos es **N = " + datos.meta.centroA.n + ", C = " + datos.meta.centroA.c + " /min, T = " + datos.meta.centroA.t + " s**; cada tabla varía un factor.",
    "",
  );
  const cA = datos.meta.centroA;
  const cB = datos.meta.centroB;
  push(tablaA(datos, `A.1 — Barrido de N (C = ${cA.c} /min, T = ${cA.t} s)`, (p) => !p.tls && p.c === cA.c && p.t === cA.t, "n", "N"));
  push(tablaA(datos, `A.2 — Barrido de C (N = ${cA.n}, T = ${cA.t} s)`, (p) => !p.tls && p.n === cA.n && p.t === cA.t, "c", "C (cambios /min)"));
  push(tablaA(datos, `A.3 — Barrido de T (N = ${cA.n}, C = ${cA.c} /min; flux no sondea y no depende de T)`, (p) => !p.tls && p.n === cA.n && p.c === cA.c && p.metodo !== "flux", "t", "T (s)"));
  push(tablaA(datos, `A.4 — Con TLS (N = ${cA.n}, C = ${cA.c} /min, T = ${cA.t} s)`, (p) => p.tls, "n", "N"));

  push("## Escenario B — avisar a muchos", "");
  push(
    "Un productor, N receptores, un hecho (pedido creado) cada 60/H segundos. Línea base: N webhooks por hecho (un `POST` por receptor, keep-alive, hasta 3 intentos, sin cola). flux: 1 evento publicado y N consumidores durables (un servicio cada uno). Latencia = del instante en que el productor decide avisar al instante en que el receptor tiene el hecho.",
    "",
  );
  push(tablaB(datos, `B.1 — Barrido de N (H = ${cB.h} hechos/min)`, (p) => !p.tls && p.h === cB.h, "n", "N"));
  push(tablaB(datos, `B.2 — Barrido de H (N = ${cB.n})`, (p) => !p.tls && p.n === cB.n, "h", "H (hechos /min)"));
  push(tablaB(datos, `B.3 — Con TLS (N = ${cB.n}, H = ${cB.h} /min)`, (p) => p.tls, "n", "N"));

  push("## Escenario C — llamada con respuesta", "");
  push(
    "`calcular-descuento`: petición de ~60 B y respuesta de ~35 B de JSON, en bucle cerrado a concurrencia 1, 16 y 256 (cada hilo de ejecución lanza la siguiente al recibir la respuesta). Un proceso de Node para cada lado: a concurrencia ≥ 16 el proceso más lento llega a un núcleo, así que «llamadas/s» mide el techo de ese par de procesos, no de HTTP ni de NATS en abstracto; la **CPU por llamada** es lo que sigue siendo comparable. «NATS crudo» es core NATS sin el SDK de flux (el mismo broker y permisos): reparte el coste entre el transporte y lo que añade el SDK. «flux con OpenTelemetry presente» es el mismo SDK con `@opentelemetry/api` resolviendo (experimento: ver «Hallazgos del banco»).",
    "",
  );
  push(tablaC(datos, "C.1 — Sin TLS", (p) => !p.tls));
  push(tablaC(datos, "C.2 — Con TLS (concurrencia 16)", (p) => p.tls));

  push("## Hallazgos del banco", "");
  push(
    "Cosas que el banco descubrió y que no son una cifra. Están también, con su medida, en el Apéndice A de `docs/plan-v1.1/PLAN.md`.",
    "",
    "- **A-170 — `activeTraceparent()` hace un `import()` de `@opentelemetry/api` en CADA publicación, petición y respuesta.** Sin el paquete instalado, cada llamada recorre la resolución de módulos de Node y lanza una excepción que se descarta: ~85 µs de CPU por llamada (microbenchmark de `bench/`), en las dos puntas de una petición. Con `@opentelemetry/api` presente la llamada cuesta ~3 µs. Es la diferencia entre `flux` y «flux con OpenTelemetry presente» en C.",
    "",
  );

  push("## Dónde se gasta la CPU", "");
  push("Fracción de la CPU de cada proceso que se gasta **en el núcleo del sistema operativo** (red, `epoll`, `futex`, ficheros) y no en código de usuario: mediana de todas las repeticiones sin TLS de cada método. Un proceso que casi todo lo gasta en el núcleo está pagando entregas, despertares y escrituras más que cálculo.", "", tablaSistema(datos), "");
  push("## Calidad de la medida", "", q.texto, "");
  push("## Comprobaciones del propio banco", "");
  push("Antes de fiarse de un número hay que saber que mide lo que se cree. Cada repetición se comprueba contra sus propios contadores (keep-alive de verdad contando conexiones TCP, `304` en el sondeo con ETag, cada mensaje entregado una vez, ningún proceso saturado). `bench/autoprueba.mjs` estropea el banco a propósito y exige que estas comprobaciones lo detecten.", "");
  push(cmp.tabla, cmp.detalle, "");

  push("## La máquina y las versiones", "");
  push(
    tabla(
      ["", ""],
      [
        ["CPU", `${e.cpu} · ${e.nucleos} núcleos (${e.hilosPorNucleo ?? "?"} hilo/núcleo)`],
        ["Virtualización", e.virtualizacion],
        ["RAM", `${e.ramGb} GB`],
        ["SO", e.so],
        ["Gobernador de frecuencia", e.gobernador],
        ["Node", `${e.node} (V8 ${e.v8})`],
        ["nats-server", e.natsServer],
        ["`@nats-io/*`", Object.entries(e.nats).map(([k, v]) => `${k} ${v}`).join(" · ")],
        ["SDK de flux", `${e.sdkFlux} (commit ${e.commit}${e.commitSucio ? ", con cambios sin commitear" : ""})`],
        ["openssl (TLS)", e.openssl],
        ["Carga de la máquina al empezar / al terminar la ejecución (1, 5, 15 min)", `${datos.meta.cargaInicio.join(" ")} / ${datos.meta.cargaFin.join(" ")}`],
      ],
    ),
    "",
  );

  push("## Lo que NO se mide", "");
  push(
    "- **No hay red real.** Todo corre en `127.0.0.1`: la latencia de red (y sus pérdidas, y la pila TCP entre hosts) no está. Una llamada entre máquinas tarda más con HTTP y con NATS, y la diferencia entre ellos ahí es otra.",
    "- **Un solo nodo de `nats-server`, sin cluster ni réplicas** (`replicas: 1`): una réplica 3 de JetStream añade ida y vuelta entre nodos a cada publicación con acuse.",
    "- **Mismo host:** cliente, servidor y broker compiten por los mismos núcleos; la suma de CPU es comparable, el reparto entre procesos y la latencia bajo carga, menos.",
    "- **Un solo CRM y un solo recurso** en el escenario A; un solo tipo de hecho en B; una sola operación en C. No se mide un esquema grande ni validación L3 ni firma (opcionales; el banco mide el camino por defecto).",
    "- **TLS:** se mide aparte y solo en puntos escogidos (centro de cada escenario). Los bytes en la red no incluyen el sobrecoste de los registros TLS en ninguno de los dos lados; la CPU sí incluye el cifrado.",
    "- **Resolución de CPU:** los procesos se leen por `schedstat` (ns) con el tic de 10 ms de `/proc/<pid>/stat` como contraste; un proceso casi ocioso mide casi cero y eso es lo que hay.",
    "- **Los fallos** (receptor caído, 5xx, partición): no se miden. El reintento de flux (`backoff[0]` = 30 s) y el de un webhook (200 ms) son muy distintos por diseño.",
    "- **No se mide el trabajo de la aplicación** (parsear, guardar en base de datos): los receptores solo anotan.",
    "",
  );

  push("## Reproducir", "");
  push("```bash", "node bench/run.mjs            # completo; tarda " + (datos.meta.rapido ? "(ver bench/README.md)" : datos.meta.duracionTexto) + " con la máquina tranquila", "node bench/run.mjs --rapido   # prueba de humo, < 2 min", "```", "");
  return L.join("\n").replace(/\n{3,}/g, "\n\n").trimEnd() + "\n";
}

/** Los títulos y tablas que DEBE tener un `RESULTS.md` (lo exige `bench/comprobar-resultados.mjs`). */
export const SECCIONES = [
  "## Cómo leer las tablas",
  "## Lectura",
  "## Escenario A — enterarse de un cambio",
  "## Escenario B — avisar a muchos",
  "## Escenario C — llamada con respuesta",
  "## Hallazgos del banco",
  "## Dónde se gasta la CPU",
  "## Calidad de la medida",
  "## Comprobaciones del propio banco",
  "## La máquina y las versiones",
  "## Lo que NO se mide",
  "## Reproducir",
];
