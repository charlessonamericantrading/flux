#!/usr/bin/env node
/**
 * Genera llms-full.txt concatenando AGENTS.md + specification/*.md.
 *
 * Existe por una razón concreta: un fichero agregado mantenido a mano diverge de sus
 * fuentes en la primera semana, y un agente que carga documentación obsoleta es peor
 * que uno que no carga ninguna — actúa con confianza sobre reglas que ya no existen.
 *
 *   node scripts/build-llms.mjs          → escribe llms-full.txt
 *   node scripts/build-llms.mjs --check  → falla si está desactualizado (para CI)
 */

import { readFile, writeFile, readdir } from "node:fs/promises";
import { join, dirname, basename } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const OUT = join(ROOT, "llms-full.txt");
const RAW_BASE =
  "https://raw.githubusercontent.com/charlessonamericantrading/flux/main";

const HEADER = `# flux — Event Protocol v1 (documento completo)

> Contrato de eventos polyglot. Envelope CloudEvents 1.0 sobre NATS JetStream,
> entrega at-least-once con idempotencia obligatoria en el consumidor.
>
> Este fichero es la spec ENTERA en un solo documento, pensado para que un agente de
> IA la cargue de una sola descarga. GENERADO — no lo edites a mano; edita
> AGENTS.md o specification/*.md y ejecuta \`node scripts/build-llms.mjs\`.
>
> Fuente:      https://github.com/charlessonamericantrading/flux
> Constantes:  ${RAW_BASE}/protocol.json
> Este fichero: ${RAW_BASE}/llms-full.txt

---
`;

/** Orden deliberado: primero las reglas accionables, después el detalle normativo. */
async function collect() {
  const specDir = join(ROOT, "specification");
  const specs = (await readdir(specDir))
    .filter((f) => f.endsWith(".md"))
    .sort() // 00-, 01-, … el prefijo numérico es el orden de lectura
    .map((f) => join(specDir, f));

  return [join(ROOT, "AGENTS.md"), ...specs];
}

async function build() {
  const files = await collect();
  const parts = [HEADER];

  for (const path of files) {
    const rel = path.slice(ROOT.length + 1).replace(/\\/g, "/");
    const body = await readFile(path, "utf8");
    parts.push(
      `\n\n${"=".repeat(78)}\n== ${rel}\n${"=".repeat(78)}\n\n${body.trim()}\n`,
    );
  }

  parts.push(
    `\n\n${"=".repeat(78)}\n== FIN — ${files.length} documentos\n${"=".repeat(78)}\n`,
  );
  return parts.join("");
}

const generated = await build();

if (process.argv.includes("--check")) {
  const current = await readFile(OUT, "utf8").catch(() => "");
  if (current !== generated) {
    console.error(
      "✗ llms-full.txt está desactualizado.\n" +
        "  Ejecuta: node scripts/build-llms.mjs",
    );
    process.exit(1);
  }
  console.log("✓ llms-full.txt al día");
} else {
  await writeFile(OUT, generated, "utf8");
  const kb = (Buffer.byteLength(generated, "utf8") / 1024).toFixed(1);
  console.log(`✓ ${basename(OUT)} — ${kb} KB`);
}
