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
   * Solo para RETRYABLE: sobrescribe el backoff canónico para este intento.
   * Úsalo cuando la dependencia dice explícitamente cuánto esperar (Retry-After).
   */
  readonly retryAfterMs?: number;
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
