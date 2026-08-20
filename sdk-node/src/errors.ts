/**
 * Taxonomía de errores de flux.
 * Contrato normativo: specification/04-errors.md
 */

/** Las tres clases del protocolo. Determinan la acción sobre el mensaje NATS. */
export enum ErrorClass {
  /** Fallo del entorno, podría desaparecer solo. → nak(delay), reintenta con backoff. */
  RETRYABLE = "retryable",
  /** Evento válido que este consumidor nunca podrá procesar. → term() + DLQ, sin reintentos. */
  PERMANENT = "permanent",
  /** El mensaje ni siquiera es interpretable. → term() + DLQ + alerta inmediata. */
  POISON = "poison",
}

/** Resultado de clasificar un error. Es lo que el runtime del consumidor consume. */
export interface Classification {
  readonly class: ErrorClass;
  /** Código estable para métricas y alertas. Ej. "HTTP_503", "PEDIDO_YA_CANCELADO". */
  readonly code: string;
  /**
   * Solo para RETRYABLE: **sugerencia para el PRIMER reintento**, no un control del
   * calendario completo.
   *
   * ⚠️ Con `backoff` configurado —y flux lo configura siempre— JetStream honra el
   * delay de un `nak` únicamente en la primera reentrega; a partir de la segunda
   * manda el array `backoff` y el delay se ignora **sin ningún aviso**. Medido contra
   * NATS 2.14.5, ver 03-delivery.md §2.2.
   *
   * Consecuencia práctica: un `Retry-After: 5` de un proveedor acorta el primer
   * reintento y nada más. No construyas lógica que dependa de que se respete después.
   */
  readonly retryAfterMs?: number;
  /**
   * Solo para RETRYABLE: número máximo de entregas para ESTE error, por debajo del
   * `max_deliver` del consumidor.
   *
   * Existe porque `max_deliver` es por consumidor, no por mensaje: bajarlo a 2 para
   * acotar los errores desconocidos recortaría también los reintentos de los que sí
   * sabemos transitorios. Ver 04-errors.md §2.1.
   */
  readonly maxAttempts?: number;
}

/**
 * La aplicación lanza esto para forzar reintento.
 * Úsalo cuando SABES que el fallo es transitorio.
 */
export class RetryableError extends Error {
  readonly fluxClass = ErrorClass.RETRYABLE;
  constructor(
    message: string,
    readonly opts: { code?: string; retryAfterMs?: number; cause?: unknown } = {},
  ) {
    super(message, { cause: opts.cause });
    this.name = "RetryableError";
  }
}

/**
 * La aplicación lanza esto para ir directo a la DLQ sin gastar reintentos.
 * Úsalo cuando el evento es válido pero tu lógica lo rechaza definitivamente.
 */
export class PermanentError extends Error {
  readonly fluxClass = ErrorClass.PERMANENT;
  constructor(
    message: string,
    readonly opts: { code?: string; cause?: unknown } = {},
  ) {
    super(message, { cause: opts.cause });
    this.name = "PermanentError";
  }
}

/**
 * Lo lanza el SDK, no la aplicación: el mensaje no pudo parsearse como CloudEvent.
 * Nunca llega al handler.
 */
export class PoisonError extends Error {
  readonly fluxClass = ErrorClass.POISON;
  constructor(
    message: string,
    readonly opts: { code?: string; cause?: unknown } = {},
  ) {
    super(message, { cause: opts.cause });
    this.name = "PoisonError";
  }
}

/** Errores de flux que ya declaran su propia clase. */
export type ClassifiedError = RetryableError | PermanentError | PoisonError;

export function isClassifiedError(e: unknown): e is ClassifiedError {
  return (
    e instanceof RetryableError ||
    e instanceof PermanentError ||
    e instanceof PoisonError
  );
}
