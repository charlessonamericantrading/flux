#!/usr/bin/env node
/**
 * Comprueba que cada fragmento de código de una guía es un EXTRACTO LITERAL de un fichero real: «un fragmento de documentación que no compila
 * es peor que ninguno». La guía (`docs/migracion.md`) no contiene código escrito a mano: cada bloque cita el fichero que lo contiene, y ese
 * fichero lo ejecuta una prueba (el ejemplo de `examples/ecosistema/`, su test de extremo a extremo y las pruebas del «antes» en HTTP).
 *
 *   node scripts/check-migracion.mjs [guia.md ...]       comprueba (por defecto docs/migracion.md); sale con 1 si algo no coincide
 *   node scripts/check-migracion.mjs --rellenar [guia.md]  reescribe los bloques desde sus fuentes (para actualizar la guía tras tocar el código)
 *
 * Cada bloque de código va precedido por UNA de estas marcas, en una línea propia:
 *
 *   <!-- fuente: ruta/al/fichero.mjs#etiqueta -->            el bloque es lo que hay entre `fragmento:etiqueta` y `/fragmento:etiqueta`
 *                                                              (en comentarios del lenguaje del fichero), sin la sangría común
 *   <!-- fuente-json: ruta/al/fichero.json#/a/0/b -->        el bloque es JSON y es igual al subárbol de ese puntero (sin las claves `$comment`)
 *   <!-- sin-fuente: motivo (≥ 15 caracteres) -->            un bloque que no es código del repo (una línea de shell con `npm ci`, un ASCII…)
 *
 * Un bloque ```text, ```console o sin lenguaje no necesita marca. Cualquier otro sin marca es un error.
 */

import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";

const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const LIBRES = new Set(["", "text", "console", "txt"]);

const args = process.argv.slice(2);
const rellenar = args.includes("--rellenar");
const guias = args.filter((a) => !a.startsWith("--"));
if (!guias.length) guias.push(path.join(RAIZ, "docs", "migracion.md"));

/** Quita la sangría común y los espacios finales. */
function normaliza(lineas) {
  const l = lineas.map((x) => x.replace(/\s+$/, ""));
  const sangria = Math.min(...l.filter((x) => x.trim()).map((x) => x.match(/^ */)[0].length), Infinity);
  return l.map((x) => x.slice(Number.isFinite(sangria) ? sangria : 0)).join("\n").replace(/^\n+|\n+$/g, "");
}

function fragmento(fichero, etiqueta) {
  const lineas = readFileSync(path.join(RAIZ, fichero), "utf8").split("\n");
  const abre = lineas.flatMap((l, i) => (new RegExp(`(^|[^/])fragmento:${etiqueta}\\s*$`).test(l) ? [i] : []));
  const cierra = lineas.flatMap((l, i) => (new RegExp(`/fragmento:${etiqueta}\\s*$`).test(l) ? [i] : []));
  if (abre.length !== 1 || cierra.length !== 1) throw new Error(`${fichero}: la etiqueta «${etiqueta}» debe abrirse (fragmento:${etiqueta}) y cerrarse (/fragmento:${etiqueta}) exactamente una vez (hay ${abre.length} y ${cierra.length})`);
  if (cierra[0] <= abre[0]) throw new Error(`${fichero}: «${etiqueta}» se cierra antes de abrirse`);
  const dentro = lineas.slice(abre[0] + 1, cierra[0]);
  if (dentro.some((l) => /(^|[^/])fragmento:/.test(l))) throw new Error(`${fichero}: «${etiqueta}» contiene otro fragmento (no se anidan)`);
  return normaliza(dentro);
}

const sinComentarios = (v) => (Array.isArray(v) ? v.map(sinComentarios) : v && typeof v === "object" ? Object.fromEntries(Object.entries(v).filter(([k]) => k !== "$comment").map(([k, x]) => [k, sinComentarios(x)])) : v);

function subarbol(fichero, puntero) {
  let v = JSON.parse(readFileSync(path.join(RAIZ, fichero), "utf8"));
  for (const parte of puntero.split("/").slice(1)) {
    if (v === undefined || v === null || !(parte in Object(v))) throw new Error(`${fichero}: el puntero ${puntero} no existe`);
    v = v[parte];
  }
  return sinComentarios(v);
}

let problemas = 0;
const falla = (guia, linea, msg) => { problemas++; console.error(`✗ ${path.relative(RAIZ, guia)}:${linea + 1}: ${msg}`); };
let bloques = 0;

for (const guia of guias) {
  const lineas = readFileSync(guia, "utf8").split("\n");
  const salida = [];
  for (let i = 0; i < lineas.length; i++) {
    const linea = lineas[i];
    salida.push(linea);
    const m = /^```(\S*)\s*$/.exec(linea);
    if (!m) continue;
    // un bloque: de la apertura al cierre
    const lenguaje = m[1];
    let j = i + 1;
    while (j < lineas.length && !/^```\s*$/.test(lineas[j])) j++;
    if (j >= lineas.length) { falla(guia, i, "bloque de código sin cerrar"); break; }
    const cuerpo = lineas.slice(i + 1, j);
    // la marca: la línea anterior no vacía
    let k = i - 1;
    while (k >= 0 && !lineas[k].trim()) k--;
    const marca = k >= 0 ? /^<!-- (fuente|fuente-json|sin-fuente): (.+?) -->$/.exec(lineas[k].trim()) : null;

    if (!marca) {
      if (!LIBRES.has(lenguaje)) falla(guia, i, `bloque \`\`\`${lenguaje} sin marca de fuente (<!-- fuente: fichero#etiqueta -->): todo el código de la guía debe ser un extracto de un fichero que se ejecuta`);
    } else {
      bloques++;
      const [, tipo, valor] = marca;
      try {
        if (tipo === "sin-fuente") {
          if (valor.trim().length < 15) throw new Error("`sin-fuente` necesita un motivo de al menos 15 caracteres");
        } else if (tipo === "fuente") {
          const [fichero, etiqueta] = valor.split("#");
          if (!fichero || !etiqueta) throw new Error(`la marca debe ser «fichero#etiqueta», es «${valor}»`);
          const esperado = fragmento(fichero, etiqueta);
          if (rellenar) {
            salida.push(...esperado.split("\n"), lineas[j]);
            i = j;
            continue;
          }
          if (normaliza(cuerpo) !== esperado) {
            const a = normaliza(cuerpo).split("\n");
            const b = esperado.split("\n");
            const n = a.findIndex((x, idx) => x !== b[idx]);
            throw new Error(`el bloque no coincide con ${fichero}#${etiqueta}; primera diferencia en la línea ${(n < 0 ? Math.min(a.length, b.length) : n) + 1} del bloque:\n      guía:    ${a[n < 0 ? b.length : n] ?? "(fin)"}\n      fichero: ${b[n < 0 ? a.length : n] ?? "(fin)"}\n    (si el cambio es del código, regenera la guía: node scripts/check-migracion.mjs --rellenar)`);
          }
        } else {
          const [fichero, puntero] = valor.split("#");
          const esperado = subarbol(fichero, puntero ?? "");
          if (rellenar) {
            salida.push(...JSON.stringify(esperado, null, 2).split("\n"), lineas[j]);
            i = j;
            continue;
          }
          let hallado;
          try { hallado = JSON.parse(cuerpo.join("\n")); } catch (e) { throw new Error(`el bloque no es JSON válido: ${e.message}`); }
          if (!isDeepStrictEqual(sinComentarios(hallado), esperado)) throw new Error(`el bloque JSON no es igual a ${fichero}#${puntero}`);
        }
      } catch (e) {
        falla(guia, i, e.message);
      }
    }
    // el bloque pasa tal cual (y su cierre: no es la apertura de otro)
    salida.push(...cuerpo, lineas[j]);
    i = j;
  }
  if (rellenar) {
    writeFileSync(guia, salida.join("\n"));
    console.log(`✓ ${path.relative(RAIZ, guia)} rellenada desde sus fuentes`);
  }
}

if (!rellenar) {
  if (problemas) { console.error(`\n✗ ${problemas} problema(s) en ${guias.length} guía(s)`); process.exit(1); }
  console.log(`✓ ${bloques} fragmento(s) de ${guias.length} guía(s): todos son extractos literales de ficheros reales`);
}
