/**
 * Errores de la capacidad `state`.
 * Contrato normativo: specification/04-errors.md §6 y specification/10-state.md.
 *
 * La lista es CERRADA: los siete códigos de abajo y ninguno más. Un caso que no encaje en
 * ninguno es un hueco de la spec, no un código nuevo que este SDK pueda inventar. (El séptimo,
 * `INVALID_STATE_CONTRACT`, es justo el hueco que dejó la lista de seis: un contrato mal formado
 * lanzaba un error sin código, y la spec lo cerró en vez de dejar que cada SDK lo resolviera.)
 *
 * Reglas comunes (04-errors.md §6, «Qué DEBE decir el mensaje»):
 *   · el `code` es fijo y la aplicación no puede sustituirlo: es lo que agrupa el panel
 *     (`flux_events_dlq_total{code}`) y encabeza `dlqerror`;
 *   · el mensaje dice QUÉ está mal y CÓMO se arregla;
 *   · **ningún mensaje lleva valores de `data`**: acaba en logs y en `dlqerror` durante 90
 *     días, y en estado `data` es dato personal. Una clave y un tenant sí pueden citarse:
 *     son identificadores opacos.
 */

import { PermanentError, PoisonError, RetryableError } from "./errors.js";

/** Cita un identificador opaco (clave, tenant) sin dejar que un valor largo o raro llene un log. */
function citar(v: unknown): string {
  if (typeof v !== "string") return `un valor de tipo ${v === null ? "null" : typeof v}`;
  const s = JSON.stringify(v);
  return s.length > 70 ? `${s.slice(0, 67)}…"` : s;
}

/**
 * Un contrato que no es `<dominio>.<agregado>.v<major>` —`clientes.cliente` sin versión, cuatro
 * tokens, mayúsculas—. PERMANENT. Un dominio reservado NO es este error: es `ReservedDomainError`,
 * que se evalúa antes (10-state.md §3.6). No se cuenta en `flux_state_writes_total`: su valor es la
 * etiqueta `contract`, y uno mal formado crearía una serie por cada error de tipeo.
 */
export class InvalidStateContractError extends PermanentError {
  readonly code = "INVALID_STATE_CONTRACT";
  constructor(readonly contract: unknown, motivo: string) {
    super(
      `contrato de estado inválido ${citar(contract)}: ${motivo}. ` +
        `Un contrato es \`<dominio>.<agregado>.v<major>\` —el subject del estado sin \`state.\`, sin ` +
        `tenant y sin clave—, en minúsculas y kebab-case: \`clientes.cliente.v1\` (10-state.md §2.2).`,
      { code: "INVALID_STATE_CONTRACT" },
    );
    this.name = "InvalidStateContractError";
  }
}

/** El tenant no es válido para estado, o no se pudo resolver ninguno. PERMANENT. */
export class InvalidTenantForStateError extends PermanentError {
  readonly code = "INVALID_TENANT_FOR_STATE";
  constructor(readonly tenant: unknown) {
    super(
      tenant === undefined
        ? `no hay tenant para esta operación de estado. En estado NO hay tenant por defecto ` +
            `(ni "system" implícito): el tenant va en el subject, y uno equivocado no es una ` +
            `etiqueta mal puesta sino un dato guardado en otro sitio. Pásalo en la opción ` +
            `\`tenantId\` o fija \`tenantId\` en connect() (10-state.md §2.4).`
        : `el tenant ${citar(tenant)} no es válido para estado: debe cumplir ` +
            `^[a-z0-9]+(-[a-z0-9]+)*$ y tener como máximo 64 caracteres. El tenant de un ` +
            `EVENTO puede ser cualquier cadena; el de un estado, no, porque va dentro del ` +
            `subject de NATS (10-state.md §2.4).`,
      { code: "INVALID_TENANT_FOR_STATE" },
    );
    this.name = "InvalidTenantForStateError";
  }
}

/** La clave no cabe en un token de NATS. PERMANENT. */
export class InvalidStateKeyError extends PermanentError {
  readonly code = "INVALID_STATE_KEY";
  constructor(readonly key: unknown) {
    super(
      `la clave ${citar(key)} no es válida: debe cumplir ^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$ ` +
        `(un único token de NATS: sin puntos, comodines ni espacios, de 1 a 128 caracteres y sin ` +
        `empezar por «_», que está reservado al protocolo). El broker NO la valida —acepta \`*\`, ` +
        `\`a.b\` y 2000 caracteres— y una clave sin validar es un subject fantasma que ningún ` +
        `observador recibe, sin error. Tampoco se escapa sola: dos escapes distintos serían dos ` +
        `claves para el mismo agregado. Usa un identificador opaco, nunca un email (10-state.md §2.3).`,
      { code: "INVALID_STATE_KEY" },
    );
    this.name = "InvalidStateKeyError";
  }
}

/**
 * `data` no lleva un `aggregateVersion` válido. PERMANENT.
 *
 * El mensaje dice QUÉ tipo de valor llegó, nunca el valor de un campo de texto: `data` es
 * dato personal. Un número sí se cita (una versión no es PII y `7.5` o `0` explican el fallo).
 */
export class MissingAggregateVersionError extends PermanentError {
  readonly code = "MISSING_AGGREGATE_VERSION";
  constructor(recibido: unknown, opts: { cause?: unknown } = {}) {
    const que =
      recibido === undefined
        ? "no hay ninguno"
        : typeof recibido === "number"
          ? `llegó ${recibido}`
          : recibido === null
            ? "llegó null"
            : `llegó ${typeof recibido === "string" ? "una cadena" : `un valor de tipo ${typeof recibido}`}`;
    super(
      `\`data.aggregateVersion\` debe ser un entero ≥ 1 y ≤ 9007199254740991 (2^53 − 1); ${que}. ` +
        `Lo pone el DUEÑO del agregado —es su contador monotónico, el mismo que llevan sus eventos—: ` +
        `el SDK ni lo inventa ni lo incrementa. Ausente, null, una cadena, un decimal, 0 o un ` +
        `negativo son todos este error (10-state.md §3.4).`,
      { code: "MISSING_AGGREGATE_VERSION", cause: opts.cause },
    );
    this.name = "MissingAggregateVersionError";
  }
}

/**
 * El sobre de una entrada no coincide con el subject de NATS en el que llegó. **POISON**.
 *
 * No es PERMANENT: el mensaje es legible pero MIENTE sobre su identidad —su tenant, su clave o
 * su tipo—, y un productor que lo hace está roto o es hostil. Es de lo que se despierta a
 * alguien (04-errors.md §1.3). Nunca se aplica ni llega al handler.
 */
export class StateSubjectMismatchError extends PoisonError {
  readonly code = "STATE_SUBJECT_MISMATCH";
  constructor(
    readonly attribute: "tenantid" | "subject" | "type",
    readonly envelopeValue: unknown,
    readonly subjectValue: string,
  ) {
    super(
      `el atributo \`${attribute}\` del sobre (${citar(envelopeValue)}) no coincide con el del subject ` +
        `de NATS en el que llegó la entrada (${citar(subjectValue)}). La identidad de una entrada ` +
        `está en el subject; el sobre la repite, y si discrepan uno de los dos miente y quien ` +
        `consume no puede saber cuál. La entrada NO se aplica: la clave conserva su valor ` +
        `anterior (10-state.md §5.4).`,
      { code: "STATE_SUBJECT_MISMATCH" },
    );
    this.name = "StateSubjectMismatchError";
  }
}

/**
 * El chequeo de secuencia agotó sus tres intentos. **RETRYABLE**: el conflicto es del
 * entorno —otra instancia escribe la misma clave—, no de la llamada.
 */
export class StateConflictError extends RetryableError {
  readonly code = "STATE_CONFLICT";
  constructor(
    readonly contract: string,
    readonly key: string,
    readonly attempts: number,
  ) {
    super(
      `la clave ${citar(key)} de "${contract}" cambió entre la lectura y la publicación las ` +
        `${attempts} veces (otra instancia del dueño escribe la misma clave): se agotaron los ` +
        `${attempts} intentos del put condicional. Es reintentable: un nuevo intento relee y ` +
        `suele resolverlo, o termina en \`stale\` (10-state.md §3.2).`,
      { code: "STATE_CONFLICT" },
    );
    this.name = "StateConflictError";
  }
}

/**
 * Un subject o un contrato usa un dominio reservado. PERMANENT.
 *
 * `dlq`, `state`, `rpc` y `system` son espacios de nombres del protocolo, y un dominio con ese
 * nombre colisiona con ellos EN EL BROKER: `subjects overlap with an existing stream` (A-43).
 */
export class ReservedDomainError extends PermanentError {
  readonly code = "RESERVED_DOMAIN";
  constructor(
    readonly domain: string,
    readonly reserved: readonly string[],
    readonly what: "subject" | "contrato" | "operación" = "subject",
  ) {
    super(
      `el dominio "${domain}" está reservado al protocolo (reservados: ${reserved.join(", ")}) y no ` +
        `puede usarse en ${what === "operación" ? "una" : "un"} ${what}. Cada uno es un ` +
        `espacio de nombres propio —\`state.\`, \`dlq.\`, \`rpc.\`—, y un evento publicado ahí no pasaría por el ` +
        `chequeo de secuencia del estado (10-state.md §2.7). Elige otro dominio.` +
        // El nombre de una operación NO lleva el prefijo: el SDK lo pone (11-request-reply.md §2.2).
        (what === "operación" && domain === "rpc"
          ? ` Si lo que escribiste es el subject de NATS (\`rpc.clientes.cliente.v1.obtener\`), quita el prefijo ` +
            `\`rpc.\`: lo pone el SDK, y el nombre de la operación es \`clientes.cliente.v1.obtener\` ` +
            `(11-request-reply.md §2.2).`
          : ""),
      { code: "RESERVED_DOMAIN" },
    );
    this.name = "ReservedDomainError";
  }
}

/**
 * La clasificación de un `put` no es admisible en estado (`restricted`, o una fuera del enum).
 * PERMANENT. El código ya existía (01-envelope.md §3.1): al consumir es POISON; al llamar, el
 * error es de quien llama.
 */
export class InvalidDataClassificationError extends PermanentError {
  readonly code = "INVALID_DATACLASSIFICATION";
  constructor(
    readonly classification: unknown,
    /** `rpc`: una petición admite las cuatro (`restricted` incluida); lo único inválido es salirse del enum. */
    readonly ambito: "estado" | "rpc" = "estado",
  ) {
    super(
      ambito === "rpc"
        ? `la clasificación ${citar(classification)} no es válida: public, internal, confidential o restricted ` +
            `(01-envelope.md §3.1). El error de una petición/respuesta es siempre \`internal\` y no se elige.`
        : `la clasificación ${citar(classification)} no se admite en estado: solo public, internal o ` +
            `confidential. \`restricted\` (credenciales, salud, financiero: 24 h de retención) no cabe ` +
            `en un almacén que no caduca, y no es una decisión que deba tomarse por omisión ` +
            `(10-state.md §7).`,
      { code: "INVALID_DATACLASSIFICATION" },
    );
    this.name = "InvalidDataClassificationError";
  }
}

/**
 * Una entrada de estado que LLEGA con `dataclassification: "restricted"`. **POISON**: al
 * consumir, es un mensaje que no debería existir (el `put` de un SDK conforme lo rechaza), y un
 * almacén sin caducidad no puede guardar un dato de 24 h de retención (10-state.md §7). Mismo
 * código que `InvalidDataClassificationError`, pero otra clase: al llamar es PERMANENT (el error
 * es de quien llama); al consumir es POISON (el error es de quien lo escribió).
 */
export class RestrictedStateEntryError extends PoisonError {
  readonly code = "INVALID_DATACLASSIFICATION";
  constructor() {
    super(
      `una entrada de estado no puede llevar dataclassification "restricted": credenciales, salud o ` +
        `datos financieros (24 h de retención) no caben en un almacén que no caduca. La entrada NO se ` +
        `aplica (10-state.md §7).`,
      { code: "INVALID_DATACLASSIFICATION" },
    );
    this.name = "RestrictedStateEntryError";
  }
}
