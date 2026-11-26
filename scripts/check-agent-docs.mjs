#!/usr/bin/env node
/**
 * Comprueba que los resúmenes de `AGENTS.md` NO divergen de él: `.cursor/rules/flux.mdc` y `.github/copilot-instructions.md` (que un agente lee EN LUGAR de
 * AGENTS.md), `llms.txt` y el `README.md`.
 *
 * Por qué existe: los dos primeros se escribieron cuando flux era «un contrato de eventos», dijeron durante meses que el error por defecto era PERMANENT
 * (AGENTS.md dice RETRYABLE acotado a 2 entregas) y no sabían que existían el estado, la petición/respuesta ni el gateway. Un agente que lee un resumen
 * desactualizado actúa con la misma confianza que con el bueno. «Que no digan cosas distintas» se comprueba, no se espera.
 *
 *   node scripts/check-agent-docs.mjs
 *
 * Qué se exige (cada cosa con su razón):
 *   · cada REGLA DURA de AGENTS.md §1 aparece en los dos resúmenes con **su misma frase en negrita** (el cuerpo puede estar condensado; el titular, no):
 *     una regla que falta en el resumen es una regla que el agente no conoce;
 *   · los dos resúmenes dicen cuántas reglas hay (`Reglas duras: N`) y es el N de AGENTS.md;
 *   · los dos traen el árbol de «¿Qué primitiva uso?» (las cuatro: evento, estado, petición/respuesta, gateway) y apuntan a `docs/migracion.md`;
 *   · ninguno contiene una frase que AGENTS.md ya desmintió;
 *   · `llms.txt` enlaza la guía, el ejemplo y `12-edge.md`; y el README presenta a flux como protocolo de integración con su tabla de primitivas.
 */

import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const leer = (f) => readFileSync(path.join(RAIZ, f), "utf8");

const agents = leer("AGENTS.md");
const seccion1 = agents.slice(agents.indexOf("## 1. Reglas duras"), agents.indexOf("## 2. Publicar un evento"));
const reglas = [...seccion1.matchAll(/^(\d+)\. \*\*(.+?)\*\*/gm)].map((m) => ({ n: Number(m[1]), titular: m[2] }));

const problemas = [];
const falla = (f, m) => problemas.push(`${f}: ${m}`);

if (reglas.length < 21 || reglas.some((r, i) => r.n !== i + 1)) {
  falla("AGENTS.md", `las reglas duras de §1 deben ser una secuencia 1…N sin huecos (hay ${reglas.map((r) => r.n).join(",")}) y al menos las 21 conocidas`);
}
const N = reglas.length;

/** Frases que AGENTS.md desmintió y que un resumen no debe repetir. */
const DESMENTIDAS = [
  ["Default: **PERMANENT**", "el default de lo DESCONOCIDO es RETRYABLE con presupuesto acotado de 2 entregas (04-errors.md §2.1)"],
  ["Default: PERMANENT", "el default de lo DESCONOCIDO es RETRYABLE con presupuesto acotado de 2 entregas (04-errors.md §2.1)"],
  ["contrato de eventos polyglot", "flux es un protocolo de integración: eventos, estado y petición/respuesta"],
];

for (const f of [".cursor/rules/flux.mdc", ".github/copilot-instructions.md"]) {
  const t = leer(f);
  for (const r of reglas) {
    if (!t.includes(`**${r.titular}**`)) falla(f, `falta la regla ${r.n} de AGENTS.md: «${r.titular}»`);
  }
  const m = /Reglas duras:\s*(\d+)/.exec(t);
  if (!m) falla(f, "no dice cuántas reglas duras hay (`Reglas duras: N`)");
  else if (Number(m[1]) !== N) falla(f, `dice ${m[1]} reglas duras y AGENTS.md tiene ${N}`);
  for (const palabra of ["¿Qué primitiva uso?", "Evento", "Estado", "Petición/respuesta", "Gateway", "docs/migracion.md", "flux new"]) {
    if (!t.includes(palabra)) falla(f, `falta «${palabra}»`);
  }
  for (const [frase, por] of DESMENTIDAS) if (t.includes(frase)) falla(f, `repite «${frase}»: ${por}`);
}

const llms = leer("llms.txt");
for (const x of ["docs/migracion.md", "examples/ecosistema", "12-edge.md", "protocolo de integración", "flux new"]) {
  if (!llms.includes(x)) falla("llms.txt", `falta «${x}»`);
}
for (const [frase, por] of DESMENTIDAS) if (llms.includes(frase)) falla("llms.txt", `repite «${frase}»: ${por}`);

const readme = leer("README.md");
for (const x of ["protocolo de integración", "¿Qué primitiva uso?", "docs/migracion.md", "examples/ecosistema", "flux new"]) {
  if (!readme.includes(x)) falla("README.md", `falta «${x}»`);
}
for (const [frase, por] of DESMENTIDAS) if (readme.includes(frase)) falla("README.md", `repite «${frase}»: ${por}`);

if (problemas.length) {
  console.error(problemas.map((p) => `✗ ${p}`).join("\n"));
  console.error(`\n✗ ${problemas.length} divergencia(s) entre AGENTS.md y sus resúmenes`);
  process.exit(1);
}
console.log(`✓ las ${N} reglas duras de AGENTS.md están, con su titular, en los dos resúmenes; llms.txt y README.md al día`);
