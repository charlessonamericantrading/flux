/**
 * `flux new event | state | rpc <nombre>` — crea el esquema de un contrato nuevo con el `$id` correcto y lo que el protocolo exige, en el
 * sitio que toca, y regenera el bundle. Es lo que un desarrollador hace PRIMERO al añadir un evento, un estado o una operación, y lo que
 * más se equivoca a mano: un `$id` que no coincide con la ruta, un esquema de estado sin `aggregateVersion`, una operación con su petición
 * pero sin su respuesta (los tres los rechaza `bundle-schemas.mjs`, pero tras haberse equivocado).
 *
 *   flux new event clientes.cliente.v1.registrado        → schemas/clientes/cliente/registrado/1.0.0.json
 *   flux new state clientes.cliente.v1                   → schemas/state/clientes/cliente/1.0.0.json
 *   flux new rpc   clientes.cliente.v1.calcular-descuento → schemas/rpc/clientes/cliente/calcular-descuento/{request,reply}/1.0.0.json
 *
 * Las reglas de los nombres NO viven aquí: los patrones y los dominios reservados se leen de `protocol.json` (como los modelos de estado y de
 * petición/respuesta), de modo que `flux new` rechaza exactamente lo que el protocolo rechaza —y dice cómo arreglarlo—. Lo que añade son dos
 * antipatrones de AGENTS.md §10 (un evento en imperativo, `…actualizado`) y un aviso por una operación que parece estar en pasado.
 *
 * Orden de trabajo (cada paso, con su razón):
 *   1. validar el nombre                          — antes de tocar el disco
 *   2. negarse a pisar un esquema que ya existe   — los publicados son INMUTABLES (05-compatibility.md §4): se añade una versión
 *   3. `check-compat` del árbol tal como está     — no se apila un contrato nuevo sobre un árbol que ya incumple las reglas
 *   4. escribir (y, si falta, el esquema de plataforma: la lápida o el error de rpc, que el bundle exige y nadie recuerda copiar)
 *   5. `check-compat` otra vez, y regenerar el bundle — si algo falla, se DESHACE lo escrito: nunca queda un contrato a medias
 *
 * Sin red ni broker: es trabajo sobre ficheros. Los pasos 3 y 5 ejecutan `scripts/check-compat.mjs` y `scripts/bundle-schemas.mjs` de este repo
 * con `--schemas <dir>`, así que valen para el `schemas/` de cualquier proyecto.
 */

import { spawnSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, rmdirSync, writeFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { c, OK, FAIL, WARN } from "./ui.mjs";

const RAIZ = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
export const TIPOS = ["event", "state", "rpc"];

let _canon;
/** `protocol.json`, con los patrones compilados. Se lee una vez. */
function canon() {
  if (_canon) return _canon;
  const p = JSON.parse(readFileSync(join(RAIZ, "protocol.json"), "utf8"));
  _canon = {
    reEvento: new RegExp(p.naming.subject.pattern),
    reContrato: new RegExp(p.state.contract.pattern),
    reOperacion: new RegExp(p.rpc.operation.pattern),
    reNombreOperacion: new RegExp(p.rpc.operationName.pattern),
    reClave: new RegExp(p.state.key.pattern),
    reservados: p.naming.reservedDomains.names,
    ejemplos: { event: p.naming.subject.valid[0], state: p.state.contract.valid[0], rpc: p.rpc.operation.valid[1] ?? p.rpc.operation.valid[0] },
  };
  return _canon;
}

// ─── 1. Validar el nombre ────────────────────────────────────────────────────────────────────────

/** Los tokens de un nombre, y qué tiene de malo si algo: `[[qué, cómo se arregla]]`. Un nombre bien formado da `[]`. */
function formaDelNombre(nombre, tokensEsperados, formato, ejemplo) {
  const problemas = [];
  const tokens = nombre.split(".");
  if (/\s/.test(nombre)) problemas.push(["tiene un espacio", "los nombres no llevan espacios: kebab-case, `[a-z0-9-]`"]);
  if (/[*>]/.test(nombre)) problemas.push(["lleva un comodín (`*` o `>`)", "un nombre de contrato es concreto: el broker aceptaría un comodín y daría un subject que nadie atiende, sin error (A-41)"]);
  if (/[A-Z]/.test(nombre)) problemas.push(["lleva mayúsculas", `NATS distingue mayúsculas y minúsculas: crearía un subject fantasma sin suscriptores y sin error. Usa «${nombre.toLowerCase()}»`]);
  if (nombre.includes("_")) problemas.push(["tiene un guion bajo", `solo kebab-case: «${nombre.replace(/_/g, "-")}»`]);
  if (tokens.includes("")) problemas.push(["tiene un token vacío", "hay dos puntos seguidos, o uno al principio o al final"]);
  if (tokens.length !== tokensEsperados) {
    problemas.push([`tiene ${tokens.length} tokens y deben ser ${tokensEsperados}`, `el formato es ${formato}, p.ej. ${ejemplo}`]);
  } else if (!/^v[1-9][0-9]{0,3}$/.test(tokens[2])) {
    problemas.push([`la versión «${tokens[2]}» no es válida`, "el 3.er token es la versión mayor: `v1`…`v9999`, de 1 a 4 dígitos y sin ceros a la izquierda (`v0` no existe)"]);
  }
  return problemas;
}

/**
 * @param {"event"|"state"|"rpc"} tipo
 * @param {string} nombre
 * @returns {{ problemas: [string, string][], avisos: [string, string][], partes?: object }}
 *   `problemas` impiden crear nada; `avisos` se imprimen y se sigue.
 */
export function validarNombre(tipo, nombre) {
  const k = canon();
  const problemas = [];
  const avisos = [];
  if (!TIPOS.includes(tipo)) {
    return { problemas: [[`el tipo «${tipo}» no existe`, "usa `flux new event|state|rpc <nombre>`"]], avisos };
  }
  if (typeof nombre !== "string" || nombre === "") {
    return { problemas: [["falta el nombre", `p.ej. flux new ${tipo} ${k.ejemplos[tipo]}`]], avisos };
  }

  const dominio = nombre.split(".")[0];
  // Un prefijo de plataforma pegado a un nombre: lo más útil es decir cuál es el nombre bueno.
  if (k.reservados.includes(dominio)) {
    const resto = nombre.split(".").slice(1).join(".");
    const prefijo = {
      rpc: "el prefijo `rpc.` lo pone el SDK al construir el subject de la operación",
      state: "el prefijo `state.` lo pone el SDK: un contrato de estado es <dominio>.<agregado>.v<major>",
      dlq: "`dlq.` es el prefijo de la cola de muertos: la crea `flux provision`, no es un contrato",
    }[dominio];
    problemas.push([
      `«${dominio}» es un dominio reservado (RESERVED_DOMAIN)`,
      `${prefijo ? `${prefijo}; escribe «${resto}». ` : ""}${k.reservados.join(", ")} son espacios de nombres del protocolo y colisionan con él en el broker (A-43): nombra el bounded context real (\`clientes\`, \`pedidos\`…)`,
    ]);
    return { problemas, avisos };
  }

  let partes;
  if (tipo === "event") {
    const t = nombre.split(".");
    if (t.length === 3 && /^v[1-9][0-9]{0,3}$/.test(t[2]) && !/[A-Z_\s*>]/.test(nombre)) problemas.push(["falta el evento", `eso es un contrato de ESTADO: \`flux new state ${nombre}\`; un evento acaba en un hecho en pasado, p.ej. ${nombre}.creado`]);
    else problemas.push(...formaDelNombre(nombre, 4, "<dominio>.<agregado>.v<major>.<evento>", k.ejemplos.event));
    if (!problemas.length && !k.reEvento.test(nombre)) problemas.push(["no encaja con el patrón del protocolo", `el formato es <dominio>.<agregado>.v<major>.<evento>, p.ej. ${k.ejemplos.event}`]);
    const evento = t[3] ?? "";
    if (/^(crear|actualizar|borrar|eliminar|enviar|procesar|calcular|obtener|reservar|cancelar|confirmar)(-|$)/.test(evento)) {
      problemas.push(["el evento está en infinitivo", `un evento es un HECHO que pasó, en pasado: «creado», no «crear». Un infinitivo es una orden, y las órdenes son operaciones: \`flux new rpc ${t.slice(0, 3).join(".")}.${evento}\``]);
    }
    if (["actualizado", "cambiado", "modificado"].includes(evento)) {
      problemas.push([`«${evento}» no dice QUÉ cambió`, "obliga a cada consumidor a implementar —y equivocar— su propio diff. Nombra el hecho concreto: `direccion-envio-cambiada`, `plan-cambiado` (AGENTS.md §10)"]);
    }
    if (!problemas.length) {
      const [dom, agr, v, ev] = t;
      partes = { dom, agr, major: Number(v.slice(1)), nombre: ev };
      if (/(ar|er|ir)$/.test(ev.split("-").at(-1))) avisos.push([`«${ev}» parece un infinitivo`, "un evento va en pasado (`creado`, `entrega-fallida`). Si es una orden con respuesta, es una operación: `flux new rpc`"]);
    }
  } else if (tipo === "state") {
    problemas.push(...formaDelNombre(nombre, 3, "<dominio>.<agregado>.v<major>", k.ejemplos.state));
    const t = nombre.split(".");
    if (t.length === 4 && /^v[1-9][0-9]{0,3}$/.test(t[2])) {
      problemas.push(["tiene un 4.º token", `un contrato de estado es el subject SIN el evento, sin tenant y sin clave: «${t.slice(0, 3).join(".")}». El tenant y la clave los pone el SDK en cada \`put\`; si querías un evento, \`flux new event ${nombre}\``]);
    }
    if (!problemas.length && !k.reContrato.test(nombre)) problemas.push(["no encaja con el patrón del protocolo", `el formato es <dominio>.<agregado>.v<major>, p.ej. ${k.ejemplos.state}`]);
    if (!problemas.length) {
      const [dom, agr, v] = t;
      partes = { dom, agr, major: Number(v.slice(1)) };
    }
  } else {
    const t = nombre.split(".");
    if (t.length === 3 && /^v[1-9][0-9]{0,3}$/.test(t[2]) && !/[A-Z_\s*>]/.test(nombre)) problemas.push(["falta la operación", `una operación es <dominio>.<agregado>.v<major>.<operacion>, con la operación en infinitivo: p.ej. ${nombre}.obtener`]);
    else problemas.push(...formaDelNombre(nombre, 4, "<dominio>.<agregado>.v<major>.<operacion>", k.ejemplos.rpc));
    if (!problemas.length && !k.reOperacion.test(nombre)) problemas.push(["no encaja con el patrón del protocolo", `el formato es <dominio>.<agregado>.v<major>.<operacion>, p.ej. ${k.ejemplos.rpc}`]);
    if (!problemas.length && !k.reNombreOperacion.test(t[3])) problemas.push([`la operación «${t[3]}» no es kebab-case`, "`obtener`, `calcular-precio`, `reservar-stock`"]);
    if (!problemas.length) {
      const [dom, agr, v, op] = t;
      partes = { dom, agr, major: Number(v.slice(1)), nombre: op };
      // Solo el primer segmento: `calcular-descuento` es un infinitivo con su objeto, y «descuento» no es un participio.
      if (/(ado|ido|ada|ida)$/.test(op.split("-")[0])) avisos.push([`«${op}» parece un participio`, "una operación va en INFINITIVO (`obtener`, `calcular-precio`); el pasado es de un evento. Si es un hecho que ya pasó, es un evento: `flux new event`"]);
    }
  }
  return { problemas, avisos, partes };
}

// ─── 2. Los esquemas ─────────────────────────────────────────────────────────────────────────────

/** `linea-envio` → `lineaEnvioId`: la referencia al agregado, en camelCase como todo `data` (01-envelope.md §4). */
export const campoId = (agregado) => agregado.split("-").map((p, i) => (i ? p[0].toUpperCase() + p.slice(1) : p)).join("") + "Id";

const DRAFT = "https://json-schema.org/draft/2020-12/schema";
const id = (ruta) => `https://schemas.internal/${ruta}`;

const idPropiedad = (descripcion) => ({ type: "string", pattern: "^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$", description: descripcion });
const version = (descripcion) => ({ type: "integer", minimum: 1, description: descripcion });

/**
 * Los ficheros a crear: `[{ ruta (relativa a schemas/), esquema }]`. Todo lo obligatorio del protocolo está ya: el `$id` (igual que la ruta), el
 * objeto en la raíz con `additionalProperties: false`, la referencia al agregado, y —en un evento y en un estado— `aggregateVersion`.
 */
export function plantillas(tipo, partes, descripcion) {
  const { dom, agr, major, nombre } = partes;
  const referencia = campoId(agr);
  const base = (ruta, title, describe) => ({ $schema: DRAFT, $id: id(ruta), title, description: describe, type: "object", additionalProperties: false });

  if (tipo === "event") {
    const ruta = `${dom}/${agr}/${nombre}/${major}.0.0.json`;
    const subject = `${dom}.${agr}.v${major}.${nombre}`;
    return [{
      ruta,
      esquema: {
        ...base(ruta, subject, descripcion ?? `TODO: qué PASÓ, emitido por el dueño del dominio \`${dom}\`, y qué NO implica. Lleva solo referencias (\`${referencia}\`), nunca datos personales: el dato personal va por ESTADO (AGENTS.md, regla 5).`),
        required: [referencia, "aggregateVersion"],
        properties: {
          [referencia]: idPropiedad("Identificador del agregado; el atributo `subject` del CloudEvent. Una referencia opaca, nunca PII."),
          aggregateVersion: version("Contador monotónico del agregado. Sin él un consumidor no puede ignorar lo viejo ni lo repetido: `WHERE aggregate_version < $n` (AGENTS.md, regla 4)."),
        },
      },
    }];
  }
  if (tipo === "state") {
    const ruta = `state/${dom}/${agr}/${major}.0.0.json`;
    return [{
      ruta,
      esquema: {
        ...base(ruta, `state ${dom}.${agr}.v${major}`, descripcion ?? `TODO: el valor ACTUAL de un ${agr}, publicado por el dueño del dominio \`${dom}\` en \`state.${dom}.${agr}.v${major}.<tenant>.<${referencia}>\`. Aquí SÍ puede vivir el dato personal: va clasificado \`confidential\`, cada campo con PII lleva \`"x-pii": true\` y se borra con \`state.delete\` (10-state.md).`),
        required: [referencia, "aggregateVersion"],
        properties: {
          [referencia]: idPropiedad("Identificador del agregado: la CLAVE del estado y el atributo `subject` del CloudEvent. Un identificador opaco, nunca PII."),
          aggregateVersion: version("Contador monotónico del agregado: lo pone el DUEÑO (su contador, que sobrevive a los borrados). `put` no escribe una versión que no avanza (`stale`), y el observador aplica `WHERE aggregate_version < $n` — 10-state.md §3.4."),
        },
      },
    }];
  }
  const dir = `rpc/${dom}/${agr}/${nombre}`;
  const operacion = `${dom}.${agr}.v${major}.${nombre}`;
  const peticion = `${dir}/request/${major}.0.0.json`;
  const respuesta = `${dir}/reply/${major}.0.0.json`;
  return [
    {
      ruta: peticion,
      esquema: {
        ...base(peticion, `rpc ${operacion} — petición`, descripcion ?? `TODO: la PETICIÓN de \`${operacion}\`: una orden o un cálculo con respuesta inmediata (no un dato guardado: eso es ESTADO). Escriben los llamantes, lee el dueño. Referencias, no datos personales; importes en enteros de la unidad mínima.`),
        required: [referencia],
        properties: { [referencia]: idPropiedad("Referencia al agregado sobre el que se pide. Una referencia opaca, nunca PII.") },
      },
    },
    {
      ruta: respuesta,
      esquema: {
        ...base(respuesta, `rpc ${operacion} — respuesta`, `TODO: la RESPUESTA de \`${operacion}\`. Escribe el dueño y leen los llamantes: añadir un valor a un enum exige \`"x-extensible-enum": true\` (11-request-reply.md §11). Si la operación es una ORDEN que no es idempotente por naturaleza, la petición lleva una clave de negocio y repetirla devuelve esta misma respuesta.`),
        required: [],
        properties: {},
      },
    },
  ];
}

/** El esquema de plataforma que el bundle exige según lo que se crea, de la copia del repo si falta en el destino. */
const SISTEMA = { state: "system/state/eliminado/1.0.0.json", rpc: "system/rpc/error/1.0.0.json" };

// ─── 3. El comando ───────────────────────────────────────────────────────────────────────────────

function ejecutarScript(script, args) {
  const r = spawnSync(process.execPath, [join(RAIZ, "scripts", script), ...args], { encoding: "utf8", env: { ...process.env, NO_COLOR: "1" } });
  return { ok: r.status === 0, salida: `${r.stdout}${r.stderr}`.trim() };
}

/** Borra lo creado (ficheros y directorios que quedaron vacíos), de más profundo a menos. */
function deshacer(creados, directoriosNuevos) {
  for (const f of creados) rmSync(f, { force: true });
  for (const d of [...directoriosNuevos].sort((a, b) => b.length - a.length)) {
    try { if (readdirSync(d).length === 0) rmdirSync(d); } catch { /* ya no está */ }
  }
}

/**
 * @param {string} tipo
 * @param {string} nombre
 * @param {{ schemas?: string, description?: string, sinBundle?: boolean, dryRun?: boolean }} opts
 * @returns {number} el código de salida
 */
export function comandoNew(tipo, nombre, opts = {}, { log = console.log, err = console.error } = {}) {
  const { problemas, avisos, partes } = validarNombre(tipo, nombre);
  if (problemas.length) {
    err(`${FAIL} ${c.bold(`flux new ${tipo ?? ""} ${nombre ?? ""}`.trim())}`);
    for (const [q, como] of problemas) {
      err(`  ${c.red("•")} ${q}`);
      err(`    ${c.gray(como)}`);
    }
    err(c.gray("\n  Ver specification/02-naming.md"));
    return 1;
  }
  for (const [q, como] of avisos) log(`${WARN} ${q}\n    ${c.gray(como)}`);

  const dir = resolve(opts.schemas ?? "schemas");
  const relativo = (f) => relative(process.cwd(), f) || ".";
  const ficheros = plantillas(tipo, partes, opts.description).map((p) => ({ ...p, abs: join(dir, p.ruta) }));

  // Un esquema publicado es INMUTABLE: nunca se pisa.
  const existentes = ficheros.filter((f) => existsSync(f.abs));
  if (existentes.length) {
    err(`${FAIL} ya existe ${existentes.map((f) => c.bold(relativo(f.abs))).join(", ")}`);
    err(`    ${c.gray("Los esquemas publicados son INMUTABLES (05-compatibility.md §4): editar uno invalida los eventos ya emitidos. Para evolucionar el contrato añade una versión nueva a mano —un campo OPCIONAL es un MINOR: `1.1.0.json` junto al `1.0.0.json`; todo lo demás, un MAYOR: otro `v<n>` en el nombre— y `flux new` con ese nombre crea el `<n>.0.0`.")}`);
    return 1;
  }

  const sistema = SISTEMA[tipo];
  const faltaSistema = sistema && !existsSync(join(dir, sistema));
  if (opts.dryRun) {
    log(`${c.bold("flux new")} ${tipo} ${nombre} ${c.gray("(simulación: no se escribe nada)")}`);
    for (const f of ficheros) log(`  ${c.green("+")} ${relativo(f.abs)}\n${JSON.stringify(f.esquema, null, 2).split("\n").map((l) => `      ${l}`).join("\n")}`);
    if (faltaSistema) log(`  ${c.green("+")} ${relativo(join(dir, sistema))} ${c.gray("(copia del esquema de plataforma de este repo)")}`);
    if (!opts.sinBundle) log(`  ${c.gray("y se regeneraría")} ${relativo(join(dir, "bundle.json"))}`);
    return 0;
  }

  // El árbol de antes tiene que estar bien: no se apila un contrato nuevo sobre uno que ya incumple las reglas.
  if (existsSync(dir)) {
    const antes = ejecutarScript("check-compat.mjs", ["--schemas", dir]);
    if (!antes.ok) {
      err(`${FAIL} el árbol de esquemas ya incumple las reglas, antes de añadir nada (\`check-compat\`). Arréglalo primero:`);
      err(antes.salida.split("\n").map((l) => `    ${l}`).join("\n"));
      return 1;
    }
  }

  const creados = [];
  const directoriosNuevos = [];
  const crea = (abs, contenido, copiaDe) => {
    for (let d = dirname(abs); !existsSync(d); d = dirname(d)) directoriosNuevos.push(d);
    mkdirSync(dirname(abs), { recursive: true });
    if (copiaDe) copyFileSync(copiaDe, abs);
    else writeFileSync(abs, `${JSON.stringify(contenido, null, 2)}\n`);
    creados.push(abs);
  };
  try {
    for (const f of ficheros) crea(f.abs, f.esquema);
    if (faltaSistema) crea(join(dir, sistema), undefined, join(RAIZ, "schemas", sistema));

    const despues = ejecutarScript("check-compat.mjs", ["--schemas", dir]);
    if (!despues.ok) throw Object.assign(new Error("el contrato nuevo incumple las reglas (`check-compat`)"), { salida: despues.salida });
    if (!opts.sinBundle) {
      const b = ejecutarScript("bundle-schemas.mjs", ["--schemas", dir]);
      if (!b.ok) throw Object.assign(new Error("no se pudo regenerar el bundle"), { salida: b.salida });
      log(`${OK} ${b.salida.replace(/^✓ /, "")}`);
    }
  } catch (e) {
    deshacer(creados, directoriosNuevos);
    err(`${FAIL} ${e.message}. Se deshizo lo escrito:`);
    if (e.salida) err(e.salida.split("\n").map((l) => `    ${l}`).join("\n"));
    return 1;
  }

  for (const f of ficheros) log(`${OK} ${c.bold(relativo(f.abs))}`);
  if (faltaSistema) log(`${OK} ${relativo(join(dir, sistema))} ${c.gray("(el esquema de plataforma que el bundle exige con estado o con operaciones)")}`);
  const siguiente = {
    event: `Rellena sus campos y la \`description\` (hay un TODO). Lo publica el dueño de \`${partes.dom}\`; quien lo consuma lo declara en \`consumes\` de su servicio en services.json.`,
    state: `Añade los campos del valor (los que lleven datos personales, con \`"x-pii": true\`) y la \`description\`. El dueño de \`${partes.dom}\` lo escribe; quien lo observe lo declara en \`watches\`.`,
    rpc: `Rellena la petición y la respuesta (no son ficheros sueltos: son un PAR). Lo atiende el dueño de \`${partes.dom}\` (\`serves\` en services.json) y lo llama quien lo declare en \`calls\`.`,
  }[tipo];
  log(c.gray(`\n  ${siguiente}`));
  log(c.gray(`  Tras editar un esquema aún sin publicar: \`node scripts/bundle-schemas.mjs${opts.schemas ? ` --schemas ${opts.schemas}` : ""}\`. Uno ya publicado no se edita.`));
  return 0;
}
