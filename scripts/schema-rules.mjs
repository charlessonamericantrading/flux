/**
 * Reglas de ubicación y de forma de los esquemas que comparten `bundle-schemas.mjs` y
 * `check-compat.mjs`. Una sola fuente: si cada script clasificara las rutas a su manera, un
 * esquema de estado sería «un evento del dominio `state`» para uno y «un contrato» para el otro.
 *
 *   schemas/<dominio>/<agregado>/<evento>/<major>.<minor>.<patch>.json    evento
 *   schemas/state/<dominio>/<agregado>/<major>.<minor>.<patch>.json       estado    (10-state.md §10)
 *   schemas/rpc/<dominio>/<agregado>/<operacion>/{request,reply}/<major>.<minor>.<patch>.json
 *                                                                          petición/respuesta (11-request-reply.md §11)
 *   schemas/system/<…>/<…>/<major>.<minor>.<patch>.json                   plataforma (p. ej. la lápida, el error de rpc)
 *
 * `state`, `system`, `dlq` y `rpc` son dominios RESERVADOS (02-naming.md §1.3): ningún evento puede
 * llamarse así. Tres tienen su propio árbol (`state`, `rpc` y `system`); `dlq` no tiene esquemas.
 */

export const SEMVER = /^(\d+)\.(\d+)\.(\d+)\.json$/;
export const DOMINIOS_RESERVADOS = ["dlq", "state", "rpc", "system"];

/** La lápida: el esquema fijo que valida todo `delete` (10-state.md §4). */
export const LAPIDA = {
  ruta: "system/state/eliminado/1.0.0.json",
  uri: "https://schemas.internal/system/state/eliminado/1.0.0.json",
};

/** El error de rpc: el esquema fijo de toda respuesta `com.flux.rpc.error.v1` (11-request-reply.md §3.3). */
export const ERROR_RPC = {
  ruta: "system/rpc/error/1.0.0.json",
  uri: "https://schemas.internal/system/rpc/error/1.0.0.json",
};

/** Los dos lados de una operación: quien la llama escribe la petición; quien la atiende, la respuesta. */
export const LADOS_RPC = ["request", "reply"];

const KEBAB = /^[a-z0-9]+(-[a-z0-9]+)*$/;

/**
 * Clasifica una ruta relativa a `schemas/`.
 *
 * @returns {{ tipo: "evento"|"estado"|"rpc"|"rpc-mal-ubicado"|"sistema"|"reservado"|"fuera", rel: string, [k: string]: any }}
 *   `fuera`: no sigue la convención (no se indexa). `reservado`: un dominio reservado donde no toca.
 *   `rpc-mal-ubicado`: algo bajo `schemas/rpc/` que no es `…/<operacion>/{request,reply}/<semver>.json`: es un
 *   ERROR (no se ignora como un `fuera`), porque un esquema de operación sin su lado no se detectaría nunca.
 */
export function clasifica(rel) {
  const partes = rel.split("/");
  if (partes[0] === "rpc") return clasificaRpc(rel, partes);
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
    return { ...base, tipo: "reservado", dominio: partes[0], motivo: `el dominio «${partes[0]}» está reservado (02-naming.md §1.3): no tiene esquemas de evento` };
  }
  const [dominio, agregado, evento] = partes;
  return {
    ...base, tipo: "evento", dominio, agregado, evento,
    subject: `${dominio}.${agregado}.v${m[1]}.${evento}`,
    key: `${dominio}/${agregado}/${evento}/${m[1]}`,
  };
}

/** `schemas/rpc/<dominio>/<agregado>/<operacion>/{request,reply}/<semver>.json` — o un error de ubicación. */
function clasificaRpc(rel, partes) {
  const m = SEMVER.exec(partes.at(-1));
  const esperado = "schemas/rpc/<dominio>/<agregado>/<operacion>/{request,reply}/<major>.<minor>.<patch>.json";
  // Un .json bajo rpc/ que no sigue la convención NO se ignora: un esquema de operación mal nombrado dejaría la
  // operación sin su par sin que ningún script lo viera.
  if (!m) return { tipo: "rpc-mal-ubicado", rel, motivo: `«${partes.at(-1)}» no es <major>.<minor>.<patch>.json (${esperado})` };
  const base = { rel, major: +m[1], minor: +m[2], patch: +m[3], expectedId: `https://schemas.internal/${rel}` };
  if (partes.length !== 6 || !LADOS_RPC.includes(partes[4])) {
    const sinLado = partes.length === 5;
    return {
      ...base, tipo: "rpc-mal-ubicado",
      motivo: sinLado
        ? `una operación rpc es un PAR: su esquema va en «request/» o en «reply/», no suelto en la carpeta de la operación`
        : `un esquema rpc va en ${esperado}; ${partes.length} niveles y «${partes[4] ?? ""}» no es request ni reply`,
    };
  }
  const [, dominio, agregado, operacion, lado] = partes;
  if (DOMINIOS_RESERVADOS.includes(dominio)) {
    return { ...base, tipo: "reservado", dominio, motivo: `el dominio «${dominio}» está reservado (02-naming.md §1.3)` };
  }
  return {
    ...base, tipo: "rpc", dominio, agregado, operacion, lado,
    operation: `${dominio}.${agregado}.v${m[1]}.${operacion}`,
    key: `rpc/${dominio}/${agregado}/${operacion}/${lado}/${m[1]}`,
  };
}

/** Nombres de dominio, agregado y operación de un esquema rpc: `kebab-case` como en un subject. */
export function nombresDeRpcValidos(meta) {
  const f = [];
  for (const [campo, valor] of [["dominio", meta.dominio], ["agregado", meta.agregado], ["operación", meta.operacion]]) {
    if (!KEBAB.test(valor)) f.push({ msg: `el ${campo} «${valor}» no es kebab-case en minúsculas`, hint: "Va en el subject de NATS (`rpc.<dominio>.<agregado>.v<major>.<operacion>`): 02-naming.md §1 y 11-request-reply.md §2.2. La operación va en infinitivo: `obtener`, `calcular-precio`" });
  }
  return f;
}

/**
 * Lo que un esquema de PETICIÓN o de RESPUESTA debe declarar — 11-request-reply.md §11:
 *   · `data` es un objeto en la raíz (como en un evento, 01-envelope.md §4): un array o un escalar no
 *     admite campos nuevos sin romper el contrato.
 *
 * @param {object} schema
 * @param {"request"|"reply"} lado
 * @returns {{msg: string, hint: string}[]}
 */
export function problemasDeRpc(schema, lado) {
  const f = [];
  if (schema.type !== "object") {
    f.push({
      msg: `\`data\` de la ${lado === "request" ? "petición" : "respuesta"} no es un objeto en la raíz (type es ${JSON.stringify(schema.type)})`,
      hint: "La petición y la respuesta son CloudEvents con `data` objeto: un array o un escalar en la raíz impide añadir campos sin romper el esquema — 01-envelope.md §4, 11-request-reply.md §3",
    });
  }
  return f;
}

/**
 * El esquema fijo del error de rpc: `class` binaria (`retryable` | `permanent`), `code` estable y `message`
 * obligatorios, `retryAfterMs` opcional. Es lo que quien llama convierte en `RetryableError` / `PermanentError`.
 */
export function problemasDelError(schema) {
  const f = [];
  const pista = "11-request-reply.md §3.3: `data: { class, code, message, retryAfterMs? }` con `class` ∈ retryable | permanent";
  const cls = schema.properties?.class;
  if (JSON.stringify(cls?.enum) !== JSON.stringify(["retryable", "permanent"])) {
    f.push({ msg: `el error de rpc debe declarar \`class\` con enum ["retryable","permanent"], tiene ${JSON.stringify(cls?.enum)}`, hint: `${pista}. \`poison\` no viaja: para quien llama es \`permanent\`` });
  }
  for (const campo of ["class", "code", "message"]) {
    if (!(schema.required ?? []).includes(campo)) f.push({ msg: `el error de rpc debe tener «${campo}» en \`required\``, hint: pista });
  }
  if ((schema.required ?? []).includes("retryAfterMs")) f.push({ msg: "`retryAfterMs` es opcional en el error de rpc", hint: pista });
  if (schema.properties?.retryAfterMs && schema.properties.retryAfterMs.type !== "integer") f.push({ msg: "`retryAfterMs` debe ser un entero (milisegundos)", hint: pista });
  return f;
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
