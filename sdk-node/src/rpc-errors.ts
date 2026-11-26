/**
 * Errores de la capacidad `rpc` (petición/respuesta).
 * Contrato normativo: specification/04-errors.md §7 y specification/11-request-reply.md §6.
 *
 * La lista es CERRADA: los seis códigos de abajo y ninguno más. Un caso que no encaje en ninguno
 * es un hueco de la spec, no un código nuevo que este SDK pueda inventar. Lo que se reutiliza
 * (`RESERVED_DOMAIN`, `INVALID_SCHEMA`, `INVALID_DATACLASSIFICATION`, `PERMISSION_DENIED`, los
 * de la firma y los del sobre) vive donde ya estaba.
 *
 * Reglas comunes (04-errors.md §7, «Qué DEBE decir el mensaje»):
 *   · el `code` es fijo y la aplicación no puede sustituirlo: es lo que agrupa el panel;
 *   · el mensaje dice QUÉ está mal y CÓMO se arregla (los bytes y el límite; el rango del
 *     timeout; la operación y el patrón; la ventana de 300 s y la hora local);
 *   · **ningún mensaje lleva valores de `data`**: acaba en logs, y una petición puede llevar datos
 *     personales (11-request-reply.md §3.6). Una operación, un `id` y un tenant sí pueden citarse.
 */

import { PermanentError, RetryableError } from "./errors.js";

/** Cita un texto corto sin dejar que un valor largo o raro llene un log. */
function citar(v: unknown): string {
  if (typeof v !== "string") return `un valor de tipo ${v === null ? "null" : typeof v}`;
  const s = JSON.stringify(v);
  return s.length > 90 ? `${s.slice(0, 87)}…"` : s;
}

/**
 * Nadie escucha la operación: el broker contestó `no responders` (estado 503) en milisegundos.
 * **RETRYABLE**, inmediato: no gasta el timeout. Es también lo que ve quien llama durante el hueco
 * de un despliegue, o con una errata en la operación (la suscripción de quien responde es al subject
 * EXACTO, de modo que una operación que no atiende nadie no tiene interés: A-64).
 */
export class RpcUnavailableError extends RetryableError {
  readonly code = "RPC_UNAVAILABLE";
  constructor(
    readonly operation: string,
    opts: { cause?: unknown } = {},
  ) {
    super(
      `nadie escucha la operación "${operation}": el broker contestó \`no responders\`. Ningún servicio la ` +
        `atiende ahora mismo (¿está desplegado y arrancado el servicio dueño? ¿el nombre y la versión mayor son ` +
        `los de su contrato? ¿hay un respond() para esta operación EXACTA?). Es reintentable, y falla al ` +
        `instante: no ha esperado al timeout (11-request-reply.md §4.3).`,
      { code: "RPC_UNAVAILABLE", cause: opts.cause },
    );
    this.name = "RpcUnavailableError";
  }
}

/**
 * No llegó una respuesta en el timeout. **RETRYABLE** y **AMBIGUO**: la operación PUEDE haberse
 * ejecutado (un responder lento la ejecuta y su respuesta llega tarde, A-61; uno que muere con la
 * petición, A-62; una instancia colgada, A-63; la ventana de un cluster al irse el último, A-70).
 */
export class RpcTimeoutError extends RetryableError {
  readonly code = "RPC_TIMEOUT";
  constructor(
    readonly operation: string,
    readonly timeoutMs: number,
    opts: { cause?: unknown } = {},
  ) {
    super(
      `no llegó una respuesta a "${operation}" en ${timeoutMs} ms. La operación PUEDE haberse ejecutado: ` +
        `el que responde pudo ser más lento que el timeout, o morir con la petición en la mano. Un reintento ` +
        `debe ser seguro: usa \`retries\` (reutiliza el MISMO id) y que quien responde deduplique por el id de ` +
        `la petición (11-request-reply.md §4.5). Si es un cálculo lento, sube \`timeoutMs\` (máximo 25000).`,
      { code: "RPC_TIMEOUT", cause: opts.cause },
    );
    this.name = "RpcTimeoutError";
  }
}

/**
 * Con la firma en `require`, el `time` de la petición se aleja más de 300 s del reloj de quien
 * responde. **PERMANENT**. Lo lanza quien responde, **dentro de un error** (`com.flux.rpc.error.v1`);
 * quien llama lo recibe como un `RpcPermanentError` con este código.
 */
export class StaleRequestError extends PermanentError {
  readonly code = "STALE_REQUEST";
  constructor(
    readonly time: string,
    readonly windowSeconds: number,
    readonly nowIso: string,
  ) {
    super(
      `la petición lleva \`time\` ${citar(time)} y la hora local de quien responde es ${nowIso}: se aleja más de ` +
        `${windowSeconds} s (por delante o por detrás). Con la firma en modo \`require\` la ventana acota cuánto ` +
        `tiempo sirve una petición capturada, porque la firma por sí sola no impide reenviarla. Sincroniza los ` +
        `relojes (NTP) de los dos servicios; reintentar la misma petición da lo mismo (11-request-reply.md §7.4).`,
      { code: "STALE_REQUEST" },
    );
    this.name = "StaleRequestError";
  }
}

/**
 * Lo que llegó NO es la respuesta a esta petición: no es un CloudEvent válido, su `type` no es el de
 * la respuesta ni el del error, su `causationid` no es el `id` de la petición, su `correlationid` o su
 * `tenantid` no los hereda, o el `data` del error no cumple su esquema. **PERMANENT**: es un
 * incidente (un productor roto, un stream que captura `rpc.>`, algo hostil), no un fallo que esperar.
 */
export class RpcInvalidReplyError extends PermanentError {
  readonly code = "RPC_INVALID_REPLY";
  constructor(
    readonly operation: string,
    readonly motivo: string,
    opts: { cause?: unknown; pistaStream?: boolean } = {},
  ) {
    super(
      `lo que llegó como respuesta a "${operation}" no es la respuesta a esta petición: ${motivo}. ` +
        (opts.pistaStream
          ? `El cuerpo parece el acuse de un stream de JetStream (\`{"stream":…,"seq":…}\`): algún stream captura ` +
            `\`rpc.>\` y contesta a cada petición con su acuse —y guarda cada petición—. Ejecuta \`flux doctor\` ` +
            `(11-request-reply.md §8). `
          : `Una respuesta que no es de esta petición es un incidente (un productor roto, un stream mal puesto ` +
            `o algo hostil), no un fallo que esperar: reintentar no lo arregla (11-request-reply.md §4.3). `) +
        `Se cuenta como \`invalid_reply\`: es la alerta que debe despertar a alguien.`,
      { code: "RPC_INVALID_REPLY", cause: opts.cause },
    );
    this.name = "RpcInvalidReplyError";
  }
}

/**
 * El mensaje serializado no cabe en el mínimo entre 1 MiB y el `max_payload` del broker.
 * **PERMANENT**. En la petición lo lanza quien llama **antes de abrir nada**; en la respuesta lo
 * detecta quien responde y contesta con un error de este código: publicarla lanzaría en quien
 * responde y quien llama solo vería un timeout (A-65).
 */
export class RpcMessageTooLargeError extends PermanentError {
  readonly code = "RPC_MESSAGE_TOO_LARGE";
  constructor(
    readonly bytes: number,
    readonly limit: number,
    readonly que: "petición" | "respuesta",
    readonly operation: string,
  ) {
    super(
      `la ${que} de "${operation}" serializa a ${bytes} bytes y el límite es ${limit} (el mínimo entre 1 MiB y el ` +
        `max_payload que anuncia el broker; las cabeceras cuentan). Aplica claim-check: guarda el contenido en un ` +
        `almacén de objetos y envía \`{ uri, sha256, bytes }\`, o pagina con un cursor en \`data\` ` +
        `(01-envelope.md §7, 11-request-reply.md §4.4).`,
      { code: "RPC_MESSAGE_TOO_LARGE" },
    );
    this.name = "RpcMessageTooLargeError";
  }
}

/**
 * La llamada, o el registro de quien responde, está mal formada y se sabe **sin red**: la operación
 * no cumple el patrón (cuatro tokens, sin el prefijo `rpc.`, minúsculas y kebab-case), `timeoutMs` no
 * es un entero entre 1 y 25 000, o `data` no es un objeto. En quien responde, además, un `type` que
 * no es el de la petición de esa operación. **PERMANENT**: un error de programación, no se cuenta.
 */
export class InvalidRpcCallError extends PermanentError {
  readonly code = "INVALID_RPC_CALL";
  constructor(motivo: string, opts: { cause?: unknown } = {}) {
    super(`llamada de petición/respuesta inválida: ${motivo}`, { code: "INVALID_RPC_CALL", cause: opts.cause });
    this.name = "InvalidRpcCallError";
  }
}

/**
 * La respuesta del que contesta ya traía un error `com.flux.rpc.error.v1` con `class: retryable`:
 * se convierte en un `RetryableError` que **conserva su `code`** y su `retryAfterMs`. Un handler de
 * eventos que llama a una operación hereda la clasificación sin hacer nada: si el otro servicio dice
 * `retryable`, el evento se reentrega con su backoff (11-request-reply.md §3.3).
 */
export class RpcRetryableError extends RetryableError {
  readonly code: string;
  constructor(
    readonly operation: string,
    code: string,
    message: string,
    retryAfterMs?: number,
  ) {
    super(message, { code, ...(retryAfterMs !== undefined && { retryAfterMs }) });
    this.code = code;
    this.name = "RpcRetryableError";
  }
}

/**
 * Un error `com.flux.rpc.error.v1` con `class: permanent` —o un fallo de firma de la respuesta, que
 * para quien llama es lo mismo: reintentar da lo mismo—, convertido en un `PermanentError` que
 * **conserva su `code`**. Va a la DLQ en la primera entrega si ocurre en un handler de eventos.
 */
export class RpcPermanentError extends PermanentError {
  readonly code: string;
  constructor(
    readonly operation: string,
    code: string,
    message: string,
  ) {
    super(message, { code });
    this.code = code;
    this.name = "RpcPermanentError";
  }
}
