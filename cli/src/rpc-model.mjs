/**
 * Modelo de la petición/respuesta (specification/11-request-reply.md) para la CLI: la forma
 * de una operación, el timeout, el sobre de la petición, lo que se comprueba de una respuesta y
 * el solape de subjects de `flux doctor`. Sin red: todo lo de aquí se prueba sin broker.
 *
 * Como el modelo de estado, **no usa el SDK de flux** (una herramienta de diagnóstico construida
 * sobre el SDK no puede diagnosticar los fallos del SDK) y **ninguna constante vive aquí**: los
 * patrones, el rango del timeout, el tamaño máximo y la forma de toda operación se leen de
 * `protocol.json` (bloque `rpc`), que `scripts/check-protocol-json.mjs` vigila contra la prosa.
 *
 * Los mensajes de error llevan el código de 04-errors.md §7 para que lo que dice la CLI sea lo
 * mismo que dice un SDK ante la misma entrada: nombran QUÉ está mal y CÓMO se arregla, y **nunca
 * un valor de `data`** (04-errors.md §5.2: acaba en logs).
 */

import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const RAIZ = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const dec = new TextDecoder("utf-8", { fatal: true });

/** Un fallo de la llamada: el código estable de 04-errors.md §7 (o el que reutiliza), su clase y un mensaje. */
export class ErrorRpc extends Error {
  constructor(code, message, extra = {}) {
    super(`${code}: ${message}`);
    this.name = "ErrorRpc";
    this.code = code;
    this.detalle = message;
    Object.assign(this, extra);
  }
  /** `retryable` solo para los dos que lo son (04-errors.md §7); el resto, `permanent`. */
  get clase() {
    return this.claseRemota ?? CLASE_DE_CODIGO[this.code] ?? "permanent";
  }
}

const CLASE_DE_CODIGO = {
  RPC_UNAVAILABLE: "retryable",
  RPC_TIMEOUT: "retryable",
  RPC_INVALID_REPLY: "permanent",
  RPC_MESSAGE_TOO_LARGE: "permanent",
  INVALID_RPC_CALL: "permanent",
  RESERVED_DOMAIN: "permanent",
  INVALID_DATACLASSIFICATION: "permanent",
  INVALID_SCHEMA: "permanent",
  PERMISSION_DENIED: "permanent",
};

// ─── Lo canónico, de protocol.json (carga perezosa) ──────────────────────────────────────────────

let _canon;

/** `protocol.json` ya interpretado: los regex compilados y el bloque `rpc` tal cual. */
export function canonRpc() {
  if (_canon) return _canon;
  const p = JSON.parse(readFileSync(join(RAIZ, "protocol.json"), "utf8"));
  const r = p.rpc;
  if (!r) throw new Error("protocol.json no tiene el bloque `rpc`: ¿es un checkout anterior a la petición/respuesta?");
  let esquemaError = {};
  try {
    esquemaError = JSON.parse(readFileSync(join(RAIZ, r.schemaPath.errorSchema), "utf8"));
  } catch { /* sin el fichero se valida con lo de protocol.json; los límites de longitud no se aplican */ }
  _canon = {
    rpc: r,
    reservados: p.naming.reservedDomains.names,
    reOperacion: new RegExp(r.operation.pattern),
    reSubject: new RegExp(r.subject.pattern),
    reCodigo: new RegExp(p.errors.codePattern),
    reSource: new RegExp(p.cloudevents.requiredAttributes.source.pattern),
    reTime: new RegExp(p.cloudevents.requiredAttributes.time.pattern),
    clasificaciones: p.cloudevents.requiredExtensions.dataclassification.enum,
    forma: r.doctor.overlapsWith,
    timeout: r.timeout,
    maxBytes: r.size.maxMessageBytes,
    tipoError: r.type.error.value,
    dataschemaError: r.error.dataschema,
    clasesError: r.error.classes,
    maxCodigo: esquemaError.properties?.code?.maxLength,
    maxMensaje: esquemaError.properties?.message?.maxLength,
  };
  return _canon;
}

// ─── El solape de subjects (la comprobación de `flux doctor`, 11-request-reply.md §8) ────────────

/**
 * ¿Hay algún subject concreto que casen a la vez dos patrones de NATS? `*` casa un token y `>`
 * uno o MÁS al final: con `>` cuenta que al otro lado quede al menos un token (`rpc.*.*.*.*.>` y
 * `rpc.*.*.*.*` NO se solapan: el primero pide seis). Es el algoritmo de `protocol.json` →
 * `rpc.doctor.algorithm`, y coincide con el del broker: `STREAM.NAMES` con `subject` devuelve
 * exactamente los streams que éste marca (A-110).
 */
export function solapan(a, b) {
  const x = a.split(".");
  const y = b.split(".");
  for (let i = 0; ; i++) {
    if (x[i] === ">") return y.length > i;
    if (y[i] === ">") return x.length > i;
    if (i >= x.length || i >= y.length) return x.length === y.length;
    if (x[i] === "*" || y[i] === "*" || x[i] === y[i]) continue;
    return false;
  }
}

/** ¿Captura este filtro de stream alguna operación? Se compara con la FORMA de toda operación (`rpc.*.*.*.*`). */
export const capturaOperaciones = (filtro) => solapan(filtro, canonRpc().forma);

// ─── Validación de la llamada (11-request-reply.md §2.2, §4.1 paso 0, §4.2) ─────────────────────

/**
 * `clientes.cliente.v1.calcular-descuento` → `{ operacion, dom, agr, major, op, subject, tipoPeticion,
 * tipoRespuesta }`. `RESERVED_DOMAIN` primero (cualquier nombre cuyo dominio sea uno reservado, esté
 * bien formado o no: lo más útil que se le puede decir a `rpc.clientes…`); si no, `INVALID_RPC_CALL`.
 * Coincidencia COMPLETA: en Python y PHP el `$` acepta un `\n` final.
 */
export function validarOperacion(texto) {
  const k = canonRpc();
  if (typeof texto !== "string" || texto === "") {
    throw new ErrorRpc(
      "INVALID_RPC_CALL",
      "falta la operación. El formato es <dominio>.<agregado>.v<major>.<operacion>, p.ej. `flux call clientes.cliente.v1.calcular-descuento --tenant acme --data '{…}'`.",
    );
  }
  const dominio = texto.split(".")[0];
  if (k.reservados.includes(dominio)) {
    const conPrefijo = dominio === "rpc" && texto.split(".").length === 5;
    throw new ErrorRpc(
      "RESERVED_DOMAIN",
      `«${dominio}» es un dominio reservado (${k.reservados.join(", ")}: espacios de nombres del protocolo, no dominios). ` +
        (conPrefijo
          ? `El prefijo \`rpc.\` lo pone la CLI: escribe «${texto.slice(4)}», no «${texto}». `
          : "Una operación vive en el dominio del servicio que la atiende (`clientes`, `pedidos`…). ") +
        "Ver 11-request-reply.md §2.6.",
    );
  }
  if (!k.reOperacion.test(texto)) throw new ErrorRpc("INVALID_RPC_CALL", mensajeOperacion(texto));
  const [dom, agr, v, op] = texto.split(".");
  const major = Number(v.slice(1));
  return {
    operacion: texto,
    dom,
    agr,
    major,
    op,
    subject: `rpc.${texto}`,
    tipoPeticion: `com.flux.rpc.${dom}.${agr}.${op}.request.v${major}`,
    tipoRespuesta: `com.flux.rpc.${dom}.${agr}.${op}.reply.v${major}`,
  };
}

/** Qué está mal en una operación que no cumple el patrón, y cómo se arregla. SIN reproducir nada raro: solo se cita lo que cabe. */
function mensajeOperacion(t) {
  const tokens = t.split(".");
  const problemas = [];
  if (/[*>]/.test(t)) problemas.push("lleva un comodín (`*` o `>`): el broker lo aceptaría y daría un `no responders` o un timeout engañoso (A-41), así que se rechaza");
  if (/[\s]/.test(t)) problemas.push("tiene un espacio o un salto de línea");
  if (/[A-Z]/.test(t)) problemas.push("lleva mayúsculas: NATS distingue y crearía un subject fantasma sin suscriptores y sin error");
  if (t.includes("_")) problemas.push("tiene un guion bajo: solo `kebab-case` ([a-z0-9-])");
  if (tokens.includes("")) problemas.push("tiene un token vacío (dos puntos seguidos, o uno al final)");
  if (tokens.length !== 4) problemas.push(`tiene ${tokens.length} tokens y deben ser 4 (dominio, agregado, versión, operación)`);
  else if (!/^v[1-9][0-9]*$/.test(tokens[2])) problemas.push("el tercer token es la versión mayor: `v1`, `v2`… (`v0` no existe)");
  if (!problemas.length) problemas.push("no encaja con el patrón del protocolo");
  return (
    `la operación no es válida: ${problemas.join("; ")}. ` +
    "El formato es <dominio>.<agregado>.v<major>.<operacion> —4 tokens, minúsculas, kebab-case, la operación en infinitivo y SIN el prefijo `rpc.`, que pone la CLI—, " +
    "p.ej. clientes.cliente.v1.calcular-descuento."
  );
}

/**
 * `timeoutMs` ya como valor (no texto): `undefined` es el defecto (5 s). Un entero de 1 a 25 000;
 * lo demás es `INVALID_RPC_CALL` —NO se recorta en silencio: quien pidió 60 s espera 60 s y se
 * encontraría con 25— (11-request-reply.md §4.2). Un número en una cadena tampoco vale.
 */
export function validarTimeout(valor) {
  const t = canonRpc().timeout;
  if (valor === undefined) return t.defaultMs;
  if (typeof valor !== "number" || !Number.isInteger(valor) || valor < t.minMs || valor > t.maxMs) {
    throw errorTimeout(typeof valor === "number" && Number.isFinite(valor) ? String(valor) : "ese valor");
  }
  return valor;
}

/** Lo que se pasa por `--timeout`: texto, y solo dígitos (`1.5`, `1e3`, `-1`, `abc` y `` no son enteros). */
export function parsearTimeout(texto) {
  if (texto === undefined) return validarTimeout(undefined); // el defecto: 5 s
  if (typeof texto !== "string" || !/^[0-9]+$/.test(texto)) throw errorTimeout(`«${texto}»`);
  return validarTimeout(Number(texto));
}

function errorTimeout(queEs) {
  const t = canonRpc().timeout;
  return new ErrorRpc(
    "INVALID_RPC_CALL",
    `el timeout debe ser un entero entre ${t.minMs} y ${t.maxMs} ms (por defecto ${t.defaultMs}); ${queEs} no lo es. ` +
      `No se recorta en silencio. El máximo es ${t.maxMs / 1000} s porque debe quedar por debajo del ack_wait de ${t.ackWaitMs / 1000} s: ` +
      "una llamada hecha desde un handler de eventos no puede provocar su reentrega mientras espera (11-request-reply.md §4.2). " +
      "Si la operación es más lenta, no es una petición: es un evento y un evento de resultado unidos por `correlationid`.",
  );
}

export function validarClasificacion(c) {
  const k = canonRpc();
  if (typeof c !== "string" || !k.clasificaciones.includes(c)) {
    throw new ErrorRpc(
      "INVALID_DATACLASSIFICATION",
      `la clasificación debe ser una de ${k.clasificaciones.join(", ")}. Un dato ausente NO se toma por \`internal\`: haría circular datos personales con la retención del dato interno (06-security.md §5).`,
    );
  }
  return c;
}

/** `data` es SIEMPRE un objeto en la raíz (01-envelope.md §4): ni un array ni un escalar ni `null`. */
export function validarData(data) {
  if (data === null || typeof data !== "object" || Array.isArray(data)) {
    const que = data === null ? "null" : Array.isArray(data) ? "un array" : `un ${typeof data === "string" ? "texto" : typeof data}`;
    throw new ErrorRpc(
      "INVALID_RPC_CALL",
      `\`data\` debe ser un objeto JSON ({…}), no ${que}: así se le pueden añadir campos sin romper el contrato (01-envelope.md §4). ` +
        "Si la operación no lleva parámetros, pasa `{}`.",
    );
  }
  return data;
}

/** El entorno de `source` (`/<entorno>/flux-cli`): el de `source` de protocol.json (`[a-z0-9-]+`). */
export function validarEntorno(env) {
  if (typeof env !== "string" || !canonRpc().reSource.test(`/${env}/flux-cli`)) {
    throw new Error(
      env === undefined || env === ""
        ? "falta el entorno: `--env <entorno>` (o $FLUX_ENV). Va en el `source` del sobre (`/<entorno>/flux-cli`) y la CLI no lo adivina, igual que un SDK lo recibe en connect()."
        : `el entorno «${env}» no es válido: minúsculas, dígitos y guiones ([a-z0-9-]), p.ej. produccion, staging, desarrollo.`,
    );
  }
  return env;
}

/** El tenant: explícito y no vacío. NO hay `system` por defecto (la CLI no asume un tenant que no se le ha dado). */
export function validarTenantLlamada(t) {
  if (typeof t !== "string" || t === "") {
    throw new Error(
      "falta `--tenant <tenant>`: la CLI no asume uno, ni `system`. Un tenant equivocado no da ningún error: la petición se ejecuta contra los datos de OTRO tenant (09-multitenancy.md). " +
        "Pasa el del cliente con el que quieres probar, p.ej. `--tenant acme`.",
    );
  }
  return t;
}

// ─── UUIDv7 y el sobre de la petición (11-request-reply.md §3.1) ─────────────────────────────────

/** UUIDv7 (RFC 9562): 48 bits de milisegundos, versión 7, variante 10, el resto aleatorio. */
export function uuidv7(ms = Date.now()) {
  const b = randomBytes(16);
  let t = BigInt(ms);
  for (let i = 5; i >= 0; i--) {
    b[i] = Number(t & 0xffn);
    t >>= 8n;
  }
  b[6] = (b[6] & 0x0f) | 0x70;
  b[8] = (b[8] & 0x3f) | 0x80;
  const h = b.toString("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

/** `time`: exactamente 3 decimales y `Z` (01-envelope.md §2.3). `toISOString` ya lo hace, pero se COMPRUEBA: no es una promesa de la plataforma. */
export function ahoraRfc3339(fecha = new Date()) {
  const t = fecha.toISOString();
  if (!canonRpc().reTime.test(t)) throw new Error(`la hora «${t}» no tiene 3 decimales y sufijo Z`);
  return t;
}

/**
 * El sobre de una petición, **como lo construye un SDK**: los atributos en el orden de
 * declaración de 01-envelope.md §6 y `data` el ÚLTIMO. Una petición que no nace de otro evento
 * lleva `correlationid` = su propio `id` y NO lleva `causationid`; `traceparent` solo si hay una
 * traza activa (la CLI no la tiene: se omite, no se inventa). `subject` y `partitionkey` solo con
 * `aggregateId`. Todo lo que el SDK genera (id, time, source, correlationid…) se puede dar de
 * fuera para que la prueba sea determinista, igual que las operaciones del arnés cruzado.
 */
export function construirPeticion({
  operacion, tenantId, aggregateId, id, source, time, correlationid, producerversion,
  classification, traceparent, dataschema, data,
}) {
  const o = validarOperacion(operacion);
  validarData(data);
  const ev = {
    specversion: "1.0",
    id,
    source,
    type: o.tipoPeticion,
    time,
    datacontenttype: "application/json",
    dataschema: dataschema ?? esquemaPorDefecto(o, "request"),
    ...(aggregateId !== undefined && { subject: aggregateId }),
    correlationid: correlationid ?? id,
    tenantid: tenantId,
    producerversion,
    dataclassification: classification,
    ...(aggregateId !== undefined && { partitionkey: aggregateId }),
    ...(traceparent !== undefined && { traceparent }),
    data,
  };
  return ev;
}

/** Sin bundle solo se puede asumir el `<major>.0.0` (00-protocol.md §5). */
export const esquemaPorDefecto = (o, lado) =>
  `https://schemas.internal/rpc/${o.dom}/${o.agr}/${o.op}/${lado}/${o.major}.0.0.json`;

const enc = new TextEncoder();
/** Los bytes del mensaje: `JSON.stringify` da UTF-8 literal, sin escapes `\u` y sin espacios (01-envelope.md §1.1). */
export const serializar = (evento) => enc.encode(JSON.stringify(evento));

/** El mínimo entre 1 MiB y el `max_payload` del broker (§4.4); `maxPayload` es `undefined` hasta que hay conexión. */
export function limiteBytes(maxPayload) {
  const k = canonRpc().maxBytes;
  return Number.isInteger(maxPayload) && maxPayload > 0 ? Math.min(k, maxPayload) : k;
}

/** `RPC_MESSAGE_TOO_LARGE` si no cabe: dice los bytes, el límite y cómo se arregla (§4.4). */
export function comprobarTamano(bytes, maxPayload) {
  const limite = limiteBytes(maxPayload);
  if (bytes.byteLength > limite) {
    const delBroker = limite < canonRpc().maxBytes;
    throw new ErrorRpc(
      "RPC_MESSAGE_TOO_LARGE",
      `la petición serializada ocupa ${bytes.byteLength} bytes y el límite es ${limite} ` +
        `(${delBroker ? "el max_payload que anuncia el broker" : "1 MiB: 01-envelope.md §7"}). ` +
        "No se envía: la librería la rechazaría igual, y un cliente en bruto que la mandase recibiría `Maximum Payload Violation` y le cerrarían la conexión (A-65). " +
        "Aplica claim-check (sube el contenido a un almacén y manda `{ uri, sha256, bytes }`) o pagina con un cursor en `data`.",
    );
  }
}

// ─── Qué se comprueba de lo que llega (11-request-reply.md §4.3) ─────────────────────────────────

/** ¿Es el acuse de un stream de JetStream? `{"stream":"X","seq":n}` (y, a veces, `domain` o `duplicate`). */
export function acuseDeStream(obj) {
  if (obj === null || typeof obj !== "object" || Array.isArray(obj)) return null;
  if (typeof obj.stream !== "string" || !Number.isInteger(obj.seq) || "specversion" in obj) return null;
  return { stream: obj.stream, seq: obj.seq };
}

const OBLIGATORIOS = [
  "id", "source", "type", "time", "datacontenttype", "dataschema",
  "correlationid", "tenantid", "producerversion", "dataclassification",
];

/**
 * Lo que llegó al inbox, contra la petición que lo originó. Devuelve `{ clase: "reply"|"error",
 * evento, error? }` o lanza `RPC_INVALID_REPLY` con el motivo (y, si parece el acuse de un stream,
 * con la pista de `flux doctor`).
 *
 * Las cinco comprobaciones, en orden: (1) un CloudEvent válido, con las cuatro extensiones y los
 * tipos exactos; (2) su `type` es el de la respuesta de ESTA operación o el del error; (3) su
 * `causationid` es el `id` de la petición; (4) hereda `correlationid` y `tenantid`; (5) si es un
 * error, su `data` cumple el esquema del error. Una respuesta que no es de esta petición es un
 * INCIDENTE (un productor roto, un stream mal puesto, algo hostil), no un fallo que esperar.
 */
export function validarRespuesta(peticion, bytes, { showData = false } = {}) {
  const k = canonRpc();
  const op = validarOperacion(peticion.operacion);
  const invalida = (motivo, extra = {}) => new ErrorRpc("RPC_INVALID_REPLY", motivo, extra);

  // 1. un CloudEvent válido
  let texto;
  try {
    texto = dec.decode(bytes);
  } catch {
    throw invalida(
      "lo que llegó no es UTF-8, así que no es la respuesta de un servicio flux. Si el responder está sano, algo más publica en el inbox o en esta operación: ejecuta `flux doctor`.",
    );
  }
  let e;
  try {
    e = JSON.parse(texto);
  } catch {
    throw invalida(
      `lo que llegó no es JSON (${bytes.byteLength} bytes${showData ? `: «${texto.slice(0, 120)}»` : ""}), así que no es la respuesta de un servicio flux. ` +
        "Alguien publica en el inbox o en esta operación a mano; si nadie lo hace, ejecuta `flux doctor`.",
    );
  }
  const ack = acuseDeStream(e);
  if (ack) {
    throw invalida(
      `lo que llegó es el ACUSE del stream ${ack.stream} de JetStream ({"stream":"${ack.stream}","seq":${ack.seq}}), no la respuesta del servicio: algún stream captura \`${op.subject}\`. ` +
        "El stream contesta a cada petición con su acuse —casi siempre antes que la respuesta real—, guarda la petición (datos personales incluidos) y el responder la ejecuta igualmente. " +
        "Ejecuta `flux doctor`: dice qué stream es y cómo se arregla (11-request-reply.md §8, A-18, A-67).",
      { acuse: ack },
    );
  }
  if (e === null || typeof e !== "object" || Array.isArray(e)) {
    throw invalida("lo que llegó es JSON pero no un objeto: no es un CloudEvent.");
  }
  if (e.specversion !== "1.0") throw invalida("no es un CloudEvent: `specversion` no es \"1.0\".");
  for (const a of OBLIGATORIOS) {
    if (typeof e[a] !== "string" || e[a] === "") {
      throw invalida(`no es un CloudEvent válido: \`${a}\` ${a in e && e[a] !== null && typeof e[a] !== "string" ? "debe ser una cadena (los tipos son exactos: 42 no es \"42\")" : "falta o está vacío"}.`);
    }
  }
  if (!k.clasificaciones.includes(e.dataclassification)) {
    throw invalida(`no es un CloudEvent válido: \`dataclassification\` no es una de ${k.clasificaciones.join(", ")}.`);
  }
  if (e.data === null || typeof e.data !== "object" || Array.isArray(e.data)) {
    throw invalida("no es un CloudEvent válido: `data` falta o no es un objeto.");
  }
  if ("causationid" in e && typeof e.causationid !== "string") {
    throw invalida("no es un CloudEvent válido: `causationid` debe ser una cadena.");
  }

  // 2. su `type`
  const esError = e.type === k.tipoError;
  if (!esError && e.type !== op.tipoRespuesta) {
    throw invalida(
      `su \`type\` («${e.type}») no es el de la respuesta de esta operación (${op.tipoRespuesta}) ni el del error (${k.tipoError}): ` +
        "es la respuesta de otra operación, o no la mandó quien atiende esta. Comprueba que ningún otro servicio responde a esta operación (A-60).",
    );
  }
  // 3. su `causationid`
  if (e.causationid !== peticion.id) {
    throw invalida(
      "su `causationid` no es el `id` de esta petición: es la respuesta a otra (¿un responder que contesta al `reply` equivocado?) o no es una respuesta. " +
        "Una respuesta DEBE llevar como `causationid` el `id` de la petición (11-request-reply.md §3.2).",
    );
  }
  // 4. hereda `correlationid` y `tenantid`
  if (e.correlationid !== peticion.correlationid) {
    throw invalida("su `correlationid` no es el de la petición: una respuesta lo HEREDA sin modificar (11-request-reply.md §3.2).");
  }
  if (e.tenantid !== peticion.tenantid) {
    throw invalida(
      "su `tenantid` no es el de la petición: una respuesta lo hereda sin modificar. Un responder que cambia de tenant mezcla datos de dos (09-multitenancy.md).",
    );
  }

  // 5. si es un error, su `data` cumple el esquema del error
  if (esError) {
    const malos = erroresDatosError(e.data);
    if (malos.length) {
      throw invalida(`su \`data\` no cumple el esquema del error (${k.dataschemaError}): ${malos.join("; ")}.`);
    }
    return { clase: "error", evento: e, error: e.data };
  }
  return { clase: "reply", evento: e };
}

/** Lo que incumple `data` del esquema del error (`schemas/system/rpc/error/1.0.0.json`): solo rutas y reglas, nunca valores. */
export function erroresDatosError(d) {
  const k = canonRpc();
  const m = [];
  const claves = new Set(k.rpc.error.dataKeys);
  for (const c of Object.keys(d)) if (!claves.has(c)) m.push(`/${c} no es un campo del error (solo ${[...claves].join(", ")})`);
  for (const c of k.rpc.error.required) if (!(c in d)) m.push(`/${c} es obligatorio`);
  if ("class" in d && !k.clasesError.includes(d.class)) {
    m.push(`/class debe ser ${k.clasesError.join(" o ")} (\`poison\` no viaja: para quien llama es \`permanent\`)`);
  }
  if ("code" in d) {
    if (typeof d.code !== "string" || !k.reCodigo.test(d.code)) m.push("/code debe ser un identificador estable en MAYÚSCULAS_Y_GUIONES_BAJOS");
    else if (k.maxCodigo && d.code.length > k.maxCodigo) m.push(`/code no puede pasar de ${k.maxCodigo} caracteres`);
  }
  if ("message" in d) {
    if (typeof d.message !== "string" || d.message === "") m.push("/message debe ser un texto no vacío");
    else if (k.maxMensaje && d.message.length > k.maxMensaje) m.push(`/message no puede pasar de ${k.maxMensaje} caracteres`);
  }
  if ("retryAfterMs" in d) {
    if (!Number.isInteger(d.retryAfterMs) || d.retryAfterMs < 0) m.push("/retryAfterMs debe ser un entero ≥ 0");
    else if (d.class !== "retryable") m.push("/retryAfterMs solo tiene sentido con class retryable");
  }
  return m;
}
