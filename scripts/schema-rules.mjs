/**
 * Reglas de ubicación y de forma de los esquemas que comparten `bundle-schemas.mjs` y
 * `check-compat.mjs`. Una sola fuente: si cada script clasificara las rutas a su manera, un
 * esquema de estado sería «un evento del dominio `state`» para uno y «un contrato» para el otro.
 *
 *   schemas/<dominio>/<agregado>/<evento>/<major>.<minor>.<patch>.json    evento
 *   schemas/state/<dominio>/<agregado>/<major>.<minor>.<patch>.json       estado    (10-state.md §10)
 *   schemas/system/<…>/<…>/<major>.<minor>.<patch>.json                   plataforma (p. ej. la lápida)
 *
 * `state`, `system`, `dlq` y `rpc` son dominios RESERVADOS (02-naming.md §1.3): ningún evento puede
 * llamarse así. Los dos primeros tienen su propio árbol; `dlq` y `rpc` aún no tienen esquemas.
 */

export const SEMVER = /^(\d+)\.(\d+)\.(\d+)\.json$/;
export const DOMINIOS_RESERVADOS = ["dlq", "state", "rpc", "system"];

/** La lápida: el esquema fijo que valida todo `delete` (10-state.md §4). */
export const LAPIDA = {
  ruta: "system/state/eliminado/1.0.0.json",
  uri: "https://schemas.internal/system/state/eliminado/1.0.0.json",
};

const KEBAB = /^[a-z0-9]+(-[a-z0-9]+)*$/;

/**
 * Clasifica una ruta relativa a `schemas/`.
 *
 * @returns {{ tipo: "evento"|"estado"|"sistema"|"reservado"|"fuera", rel: string, [k: string]: any }}
 *   `fuera`: no sigue la convención (no se indexa). `reservado`: un dominio reservado donde no toca.
 */
export function clasifica(rel) {
  const partes = rel.split("/");
  if (partes.length !== 4) return { tipo: "fuera", rel, motivo: `${partes.length} niveles; deben ser 4` };
  const m = SEMVER.exec(partes[3]);
  if (!m) return { tipo: "fuera", rel, motivo: `«${partes[3]}» no es <major>.<minor>.<patch>.json` };
  const base = { rel, major: +m[1], minor: +m[2], patch: +m[3], expectedId: `https://schemas.internal/${rel}` };

  if (partes[0] === "state") {
    const [, dominio, agregado] = partes;
    if (DOMINIOS_RESERVADOS.includes(dominio)) {
      return { ...base, tipo: "reservado", dominio, motivo: `el dominio «${dominio}» está reservado (02-naming.md §1.3)` };
    }
    return {
      ...base, tipo: "estado", dominio, agregado,
      contrato: `${dominio}.${agregado}.v${m[1]}`,
      key: `state/${dominio}/${agregado}/${m[1]}`,
    };
  }
  if (partes[0] === "system") {
    return { ...base, tipo: "sistema", dominio: "system", agregado: partes[1], evento: partes[2], key: `system/${partes[1]}/${partes[2]}/${m[1]}` };
  }
  if (DOMINIOS_RESERVADOS.includes(partes[0])) {
    return { ...base, tipo: "reservado", dominio: partes[0], motivo: `el dominio «${partes[0]}» está reservado (02-naming.md §1.3): ${partes[0] === "rpc" ? "los esquemas de rpc/ llegan con la fase 3" : "no tiene esquemas"}` };
  }
  const [dominio, agregado, evento] = partes;
  return {
    ...base, tipo: "evento", dominio, agregado, evento,
    subject: `${dominio}.${agregado}.v${m[1]}.${evento}`,
    key: `${dominio}/${agregado}/${evento}/${m[1]}`,
  };
}

/** Nombres de dominio y agregado de un esquema de estado: `kebab-case` como en un subject. */
export function nombresDeEstadoValidos(meta) {
  const f = [];
  for (const [campo, valor] of [["dominio", meta.dominio], ["agregado", meta.agregado]]) {
    if (!KEBAB.test(valor)) f.push({ msg: `el ${campo} «${valor}» no es kebab-case en minúsculas`, hint: "Va en el subject de NATS (`state.<dominio>.<agregado>.v<major>.…`) y en el nombre del stream: 02-naming.md §1" });
  }
  return f;
}

/**
 * Lo que un esquema de ESTADO debe declarar además de lo de todo esquema — 10-state.md §3.4, §10.
 *
 *   · `data` es un objeto en la raíz (como en un evento, 01-envelope.md §4).
 *   · `aggregateVersion`: entero, `minimum` ≥ 1, y `required`. Es lo que hace que `put` pueda ser
 *     condicional: compara versiones para no pisar un valor nuevo con uno viejo.
 *
 * @returns {{msg: string, hint: string}[]}
 */
export function problemasDeEstado(schema) {
  const f = [];
  if (schema.type !== "object") {
    f.push({
      msg: `\`data\` no es un objeto en la raíz (type es ${JSON.stringify(schema.type)})`,
      hint: "El estado guarda un objeto por clave, y exige `aggregateVersion` dentro de él: un array o un escalar no puede llevarlo — 10-state.md §3.4",
    });
  }
  const av = schema.properties?.aggregateVersion;
  const pista = "Sin versión, `put` no puede ser condicional: dos instancias del dueño dejarían un valor viejo como «actual» y un consumidor nuevo lo recibiría como verdad — 10-state.md §3.1. Añade `\"aggregateVersion\": { \"type\": \"integer\", \"minimum\": 1 }` a `properties` y a `required`";
  if (!av) {
    f.push({ msg: "no declara `aggregateVersion`", hint: pista });
    return f;
  }
  if (av.type !== "integer") {
    f.push({ msg: `\`aggregateVersion\` debe ser de tipo integer, es ${JSON.stringify(av.type)}`, hint: "Un número decimal, o una cadena, no se puede comparar entre lenguajes sin perder precisión — 10-state.md §3.4" });
  }
  if (typeof av.minimum !== "number" || av.minimum < 1) {
    f.push({ msg: `\`aggregateVersion\` debe declarar \`minimum\` ≥ 1 (es ${JSON.stringify(av.minimum)})`, hint: "El contador empieza en 1; con minimum 0, la versión 0 pasaría el esquema y el protocolo la rechaza (MISSING_AGGREGATE_VERSION) — 10-state.md §3.4" });
  }
  if (av.maximum !== undefined && av.maximum > Number.MAX_SAFE_INTEGER) {
    f.push({ msg: `\`aggregateVersion\` admite hasta ${av.maximum}, más que 2^53 − 1`, hint: "Node, y todo lo que use `double`, deja de representar enteros exactos a partir de 2^53 — 10-state.md §3.4" });
  }
  if (!(schema.required ?? []).includes("aggregateVersion")) {
    f.push({ msg: "`aggregateVersion` no está en `required`", hint: pista });
  }
  return f;
}
