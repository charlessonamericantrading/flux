/**
 * Seguridad de la conexión: inbox privado, TLS, credenciales y errores de permisos.
 * Contrato normativo: specification/06-security.md §2 y §3
 *
 * Este fichero NO habla con la red. Todo lo que hace es validar opciones y traducirlas
 * a lo que entiende el cliente NATS, para que un valor mal formado falle en `connect()`
 * y no se ignore en silencio: es exactamente lo que pasaba con `credentials.creds`, que
 * se esparcía en una opción que el cliente no conoce.
 */

import { accessSync, constants, readFileSync, statSync } from "node:fs";
import {
  credsAuthenticator,
  nkeyAuthenticator,
  tokenAuthenticator,
  usernamePasswordAuthenticator,
  TE,
  type Authenticator,
} from "@nats-io/transport-node";

import { PermanentError } from "./errors.js";
import { InvalidServiceNameError, SERVICE_PATTERN } from "./protocol.js";

// ─── Opciones públicas ───────────────────────────────────────────────────────

/**
 * Credenciales NATS. NUNCA versionadas — 06-security.md §2. Se admite UN solo método.
 *
 * | Método | Campos |
 * |---|---|
 * | `.creds` (JWT + nkey), el recomendado | `creds` (ruta) o `credsContent` (contenido) |
 * | nkey | `nkeySeed` |
 * | token | `token` |
 * | usuario y contraseña | `user` + `pass` |
 *
 * `creds` es una **ruta**, igual que `CredsFile` en Go y Rust. Hasta v0.1 el campo existía
 * pero no hacía nada: el cliente NATS no conoce una opción `creds`, así que se ignoraba.
 */
export interface Credentials {
  /** Ruta a un fichero `.creds`. Se relee en cada (re)conexión: rotar no exige reiniciar. */
  creds?: string;
  /** Contenido de un `.creds`, por ejemplo leído de un gestor de secretos. */
  credsContent?: string;
  /** Semilla de una nkey de USUARIO (`SU…`). */
  nkeySeed?: string;
  token?: string;
  user?: string;
  pass?: string;
}

/**
 * TLS del listener. 06-security.md §2: es obligatorio en producción.
 *
 * Los ficheros son RUTAS y los lee el cliente en cada (re)conexión. `tls: {}` exige TLS
 * y verifica contra las CA del sistema.
 */
export interface TlsOptions {
  /** CA que firma el certificado del servidor NATS, si no es una CA pública. */
  caFile?: string;
  /** Certificado de cliente (mTLS). Exige `keyFile`. */
  certFile?: string;
  /** Clave del certificado de cliente. Exige `certFile`. */
  keyFile?: string;
}

// ─── Errores ─────────────────────────────────────────────────────────────────

export class InvalidCredentialsError extends Error {
  readonly code = "INVALID_CREDENTIALS";
  constructor(motivo: string, opts: { cause?: unknown } = {}) {
    // Nunca se interpola el valor de una credencial: este mensaje acaba en logs.
    super(`credenciales inválidas en connect(): ${motivo}`, { cause: opts.cause });
    this.name = "InvalidCredentialsError";
  }
}

export class InvalidTlsOptionsError extends Error {
  readonly code = "INVALID_TLS_OPTIONS";
  constructor(motivo: string) {
    super(`opciones TLS inválidas en connect(): ${motivo}`);
    this.name = "InvalidTlsOptionsError";
  }
}

/**
 * El broker rechazó la conexión por algo que reintentar no arregla: un certificado que
 * no verifica, o credenciales que no acepta.
 */
export class ConnectionRejectedError extends Error {
  readonly code = "CONNECTION_REJECTED";
  constructor(
    readonly reason: "tls" | "auth",
    message: string,
    opts: { cause?: unknown } = {},
  ) {
    super(message, { cause: opts.cause });
    this.name = "ConnectionRejectedError";
  }
}

/**
 * El broker denegó una operación por permisos (`Permissions Violation`).
 *
 * Es PERMANENT: reintentar una denegación da la misma denegación, como un HTTP 403 —
 * 04-errors.md §1.2. Un handler que la provoque va a la DLQ sin atascar la cola.
 */
export class PermissionDeniedError extends PermanentError {
  readonly code = "PERMISSION_DENIED";
  constructor(
    readonly service: string,
    readonly operation: "publish" | "subscribe",
    readonly subject: string,
    /** `hint`: cómo se arregla ESTA denegación, si el SDK lo sabe (la petición/respuesta lo sabe). */
    opts: { cause?: unknown; hint?: string } = {},
  ) {
    super(
      `el broker denegó ${operation === "publish" ? "publicar en" : "suscribirse a"} ` +
        `"${subject}" a "${service}": su ACL no concede ese permiso. ` +
        (opts.hint
          ? `${opts.hint} `
          : subject.startsWith("$JS.")
            ? `Es un permiso de JetStream, y se concede exacto por durable: una ACL generada ` +
              `antes de la v1.1 (que solo habla de subjects de dominio) no los incluye. ` +
              `Ver «Seguridad: permisos, inbox, TLS y credenciales» en sdk-node/README.md ` +
              `(06-security.md §3).`
            : `Ver 06-security.md §3.`),
      { code: "PERMISSION_DENIED", cause: opts.cause },
    );
    this.name = "PermissionDeniedError";
  }
}

/**
 * Falta un stream o un consumidor y este servicio no tiene permiso para crearlo.
 *
 * En producción los streams y los consumidores se aprovisionan FUERA del SDK
 * (06-security.md §3): dejar que cualquier servicio cree el stream compartido de un
 * dominio es una carrera esperando a ocurrir, y su permiso de creación es justo el que
 * la ACL exacta le quita. Sin este error lo que se vería es un timeout, que no dice nada.
 *
 * Es PERMANENT: reintentar no hace aparecer el recurso. Un handler que lo provoque
 * —por ejemplo publicando en un dominio sin aprovisionar— va a la DLQ en la primera
 * entrega en vez de atascar la cola.
 */
export class NotProvisionedError extends PermanentError {
  readonly code = "NOT_PROVISIONED";
  constructor(
    readonly resource: { kind: "stream" | "consumer"; name: string; stream?: string },
    readonly service: string,
    readonly deniedSubject: string,
    /**
     * `detail` sustituye al texto entero. Lo usa el estado (04-errors.md §6): ahí la señal no
     * es un `CREATE` denegado sino que **nadie contesta** a la lectura directa —un stream que
     * no existe y uno sin `allow_direct` se ven igual (A-32)—, y el mensaje DEBE nombrar las
     * dos causas, no una denegación que no ha habido.
     */
    opts: { cause?: unknown; detail?: string } = {},
  ) {
    const que =
      resource.kind === "stream"
        ? `el stream "${resource.name}"`
        : `el consumidor "${resource.name}" del stream "${resource.stream}"`;
    super(
      opts.detail ??
        `${que} no existe y "${service}" no tiene permiso para crearlo ` +
          `(el broker denegó publicar en "${deniedSubject}"). ` +
          `En producción se aprovisiona con \`flux provision <services.json>\`, usando ` +
          `credenciales de operación, no las del servicio (06-security.md §3).`,
      { code: "NOT_PROVISIONED", cause: opts.cause },
    );
    this.name = "NotProvisionedError";
  }
}

// ─── Inbox privado ───────────────────────────────────────────────────────────

/**
 * `_INBOX.<servicio>`. Las respuestas a las peticiones de JetStream —y las entregas de
 * un pull consumer— viajan por el inbox. Con el `_INBOX.>` común, un servicio que se
 * suscribe ahí lee los eventos de dominios que no tiene concedidos; medido contra NATS
 * 2.14.5 en docs/plan-v1.1/PLAN.md, hallazgo H2.
 *
 * No es una opción: lo deriva el SDK del nombre del servicio. Si fuera configurable,
 * volver al `_INBOX.>` común sería un valor por defecto olvidado.
 */
export function inboxPrefixFor(service: string): string {
  // El servicio pasa a ser un token de subject, no solo parte de un nombre de durable.
  if (!SERVICE_PATTERN.test(service)) throw new InvalidServiceNameError(service);
  return `_INBOX.${service}`;
}

// ─── Credenciales → autenticador ─────────────────────────────────────────────

const CREDENTIAL_KEYS = ["creds", "credsContent", "nkeySeed", "token", "user", "pass"] as const;

function nonEmptyString(campo: string, v: unknown): string {
  if (typeof v !== "string" || v.trim() === "") {
    throw new InvalidCredentialsError(`\`credentials.${campo}\` debe ser un texto no vacío`);
  }
  return v;
}

function assertReadableFile(campo: string, ruta: string, ctor: (m: string) => Error): void {
  try {
    if (!statSync(ruta).isFile()) throw new Error("no es un fichero");
    accessSync(ruta, constants.R_OK);
  } catch (e) {
    throw ctor(`\`${campo}\` apunta a "${ruta}", que no se puede leer (${(e as Error).message})`);
  }
}

/**
 * Traduce `credentials` a un autenticador del cliente NATS, o `undefined` si no hay.
 *
 * Falla en alto ante cualquier valor que el cliente ignoraría o que no podrá usar. Se
 * prueba el autenticador una vez con un nonce de mentira para que una semilla o un
 * `.creds` malformados fallen aquí y no durante el handshake.
 */
export function buildAuthenticator(credentials: Credentials | undefined): Authenticator | undefined {
  if (credentials === undefined) return undefined;
  if (typeof credentials !== "object" || credentials === null) {
    throw new InvalidCredentialsError("`credentials` debe ser un objeto");
  }

  const desconocidas = Object.keys(credentials).filter(
    (k) => !(CREDENTIAL_KEYS as readonly string[]).includes(k),
  );
  if (desconocidas.length > 0) {
    // Un nombre equivocado (`password`, `credsFile`…) no debe degradar a «sin auth».
    throw new InvalidCredentialsError(
      `claves desconocidas: ${desconocidas.join(", ")}. Se aceptan: ${CREDENTIAL_KEYS.join(", ")}`,
    );
  }

  const dadas = CREDENTIAL_KEYS.filter((k) => credentials[k] !== undefined);
  for (const k of dadas) nonEmptyString(k, credentials[k]);
  if (dadas.length === 0) return undefined;

  if ((credentials.user !== undefined) !== (credentials.pass !== undefined)) {
    throw new InvalidCredentialsError("`user` y `pass` van juntos");
  }
  const metodos = [
    credentials.creds !== undefined && "creds",
    credentials.credsContent !== undefined && "credsContent",
    credentials.nkeySeed !== undefined && "nkeySeed",
    credentials.token !== undefined && "token",
    credentials.user !== undefined && "user+pass",
  ].filter(Boolean);
  if (metodos.length > 1) {
    // Combinarlos es ambiguo: el servidor aceptaría uno y el otro quedaría como ruido.
    throw new InvalidCredentialsError(`elige UN método y no ${metodos.join(" + ")}`);
  }

  let auth: Authenticator;
  if (credentials.creds !== undefined) {
    const ruta = credentials.creds;
    assertReadableFile("credentials.creds", ruta, (m) => new InvalidCredentialsError(m));
    // Función y no bytes: se relee en cada (re)conexión y la rotación no exige reiniciar.
    auth = credsAuthenticator(() => readFileSync(ruta));
  } else if (credentials.credsContent !== undefined) {
    auth = credsAuthenticator(TE.encode(credentials.credsContent));
  } else if (credentials.nkeySeed !== undefined) {
    if (!credentials.nkeySeed.startsWith("SU")) {
      throw new InvalidCredentialsError(
        "`nkeySeed` debe ser la semilla de una nkey de USUARIO (empieza por «SU»)",
      );
    }
    auth = nkeyAuthenticator(TE.encode(credentials.nkeySeed));
  } else if (credentials.token !== undefined) {
    auth = tokenAuthenticator(credentials.token);
  } else {
    auth = usernamePasswordAuthenticator(credentials.user!, credentials.pass!);
  }

  try {
    auth("nonce-de-validacion");
  } catch (e) {
    throw new InvalidCredentialsError(
      `${metodos[0]} no es utilizable: ${(e as Error).message}`,
      { cause: e },
    );
  }
  return auth;
}

// ─── TLS → opciones del cliente ──────────────────────────────────────────────

const TLS_KEYS = ["caFile", "certFile", "keyFile"] as const;

/** Traduce `tls` a las opciones del cliente NATS, o `undefined` si no se pidió TLS. */
export function buildTls(tls: TlsOptions | undefined): TlsOptions | undefined {
  if (tls === undefined) return undefined;
  if (typeof tls !== "object" || tls === null) {
    // `null` en el cliente NATS significa «NO uses TLS»: justo lo contrario de lo que
    // quien pasa esta opción quiere, así que no se admite.
    throw new InvalidTlsOptionsError("`tls` debe ser un objeto (`{}` exige TLS con las CA del sistema)");
  }
  const desconocidas = Object.keys(tls).filter((k) => !(TLS_KEYS as readonly string[]).includes(k));
  if (desconocidas.length > 0) {
    throw new InvalidTlsOptionsError(
      `claves desconocidas: ${desconocidas.join(", ")}. Se aceptan: ${TLS_KEYS.join(", ")}`,
    );
  }

  const out: TlsOptions = {};
  for (const k of TLS_KEYS) {
    const v = tls[k];
    if (v === undefined) continue;
    if (typeof v !== "string" || v.trim() === "") {
      throw new InvalidTlsOptionsError(`\`tls.${k}\` debe ser una ruta no vacía`);
    }
    assertReadableFile(`tls.${k}`, v, (m) => new InvalidTlsOptionsError(m));
    out[k] = v;
  }
  if ((out.certFile === undefined) !== (out.keyFile === undefined)) {
    throw new InvalidTlsOptionsError("`certFile` y `keyFile` van juntos (certificado de cliente)");
  }
  return out;
}

// ─── Clasificación de lo que devuelve el cliente NATS ────────────────────────

/** Sube por `cause` hasta dar con un error que cumpla `pred`. */
function enCadena<T>(e: unknown, pred: (x: Record<string, unknown>) => T | undefined): T | undefined {
  for (let x = e, i = 0; typeof x === "object" && x !== null && i < 6; i++) {
    const r = pred(x as Record<string, unknown>);
    if (r !== undefined) return r;
    x = (x as { cause?: unknown }).cause;
  }
  return undefined;
}

/**
 * ¿Es una denegación de permisos del broker? Una petición de la API de JetStream que el
 * broker deniega llega como `RequestError` y la causa es el `PermissionViolationError`
 * (medido contra NATS 2.14.5, en ~1 ms: no es un timeout).
 *
 * Se reconoce por nombre y no por `instanceof`: si la aplicación acaba con dos copias de
 * `@nats-io/nats-core`, el `instanceof` falla sin ruido y la denegación se tomaría por un
 * error desconocido.
 */
export function permissionViolation(
  e: unknown,
): { operation: "publish" | "subscribe"; subject: string } | undefined {
  return enCadena(e, (x) => {
    if (x.name !== "PermissionViolationError") return undefined;
    const op = x.operation === "subscription" || x.operation === "subscribe" ? "subscribe" : "publish";
    return { operation: op, subject: String(x.subject ?? "") };
  });
}

/**
 * ¿Nadie contestó? `no responders`: no hay ningún suscriptor en el subject (medido en NATS
 * 2.14.5: A-15, y A-32 para `DIRECT.GET`). Con JetStream es lo que se ve cuando el stream no
 * existe… o existe sin `allow_direct`, que es indistinguible. Se reconoce por nombre, como
 * `permissionViolation`: una segunda copia de `@nats-io/nats-core` rompería el `instanceof`.
 */
export function isNoResponders(e: unknown): boolean {
  return (
    enCadena(e, (x) => {
      if (x.name === "NoResponders" || x.name === "NoRespondersError") return true;
      if (x.name === "RequestError" && typeof x.isNoResponders === "function") {
        return (x.isNoResponders as () => boolean).call(x) ? true : undefined;
      }
      return undefined;
    }) === true
  );
}

// Códigos de error de la API de JetStream. Son los de `JetStreamApiCodes` del cliente.
const STREAM_NOT_FOUND = 10059;
const CONSUMER_NOT_FOUND = 10014;

/** `kind`: qué no se encontró. Un stream y un consumidor llevan códigos distintos. */
export function isNotFound(e: unknown, kind: "stream" | "consumer"): boolean {
  const code = kind === "stream" ? STREAM_NOT_FOUND : CONSUMER_NOT_FOUND;
  return enCadena(e, (x) => (x.code === code ? true : undefined)) === true;
}

/**
 * Normaliza el subject de un ack (lleva secuencia y marca de tiempo) a `…<durable>.>`, y el de una
 * respuesta a una petición (`_INBOX.<servicio>.<nuid>.<token>`, distinto en cada petición) a
 * `_INBOX.<servicio>.>`: sin ello, un dueño sin `allow_responses` registraría una línea por cada
 * respuesta denegada, y el aviso de «una vez por subject» no avisaría de nada.
 */
export function normalizeDeniedSubject(subject: string): string {
  if (subject.startsWith("$JS.ACK.")) return subject.split(".").slice(0, 4).join(".") + ".>";
  if (subject.startsWith("_INBOX.") && subject.split(".").length > 3) {
    return subject.split(".").slice(0, 2).join(".") + ".>";
  }
  return subject;
}

// Verificación de certificados de `node:tls`. Un servidor cuyo certificado no encaja con
// el que el cliente espera no cambia por esperar, a diferencia de un servidor caído.
const TLS_VERIFICATION_CODES = new Set([
  "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
  "UNABLE_TO_GET_ISSUER_CERT",
  "UNABLE_TO_GET_ISSUER_CERT_LOCALLY",
  "SELF_SIGNED_CERT_IN_CHAIN",
  "DEPTH_ZERO_SELF_SIGNED_CERT",
  "CERT_HAS_EXPIRED",
  "CERT_NOT_YET_VALID",
  "CERT_REVOKED",
  "CERT_UNTRUSTED",
  "ERR_TLS_CERT_ALTNAME_INVALID",
  "HOSTNAME_MISMATCH",
]);

/**
 * Un fallo de la PRIMERA conexión que esperar no arregla, ya explicado; o `undefined`
 * si es transitorio (servidor caído, DNS, timeout) y merece otro intento.
 *
 * Por qué existe: `waitOnFirstConnect` de la librería reintenta TODO sin límite. Medido
 * contra NATS 2.14.5, con un certificado que no verifica `connect()` no falla: se queda
 * esperando para siempre sin decir nada, que es lo peor que le puede pasar a quien acaba
 * de configurar TLS.
 */
export function permanentConnectError(e: unknown): ConnectionRejectedError | undefined {
  const nombre = (e as { name?: string } | null)?.name;
  if (nombre === "AuthorizationError" || nombre === "UserAuthenticationExpiredError") {
    return new ConnectionRejectedError(
      "auth",
      `el broker rechazó las credenciales (${(e as Error).message}). Reintentar no lo arregla: ` +
        `revisa \`credentials\` y que el usuario exista en la account del servicio.`,
      { cause: e },
    );
  }
  // Se pidió `tls` y el servidor no lo ofrece. La librería NO degrada a texto plano (bien),
  // pero lo comunica como un `ConnectionError` más, indistinguible por tipo de un servidor
  // caído: sin esto se reintentaría para siempre algo que solo se arregla en la config.
  if (/server does not support 'tls'/i.test(String((e as Error | null)?.message))) {
    return new ConnectionRejectedError(
      "tls",
      `se pidió \`tls\` pero el servidor NATS no ofrece TLS. Reintentar no lo arregla, y el ` +
        `SDK no baja a texto plano: habilita TLS en el listener (06-security.md §2) o quita \`tls\`.`,
      { cause: e },
    );
  }
  const code = enCadena(e, (x) => (typeof x.code === "string" ? x.code : undefined));
  if (code !== undefined && TLS_VERIFICATION_CODES.has(code)) {
    return new ConnectionRejectedError(
      "tls",
      `no se pudo verificar el certificado del servidor NATS (${code}). Reintentar no lo ` +
        `arregla. Si el servidor usa una CA propia, indícala en \`tls.caFile\`; si el ` +
        `certificado no cubre el host al que te conectas, revisa sus SAN.`,
      { cause: e },
    );
  }
  return undefined;
}
