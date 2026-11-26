/**
 * `gateway.json`: se lee, se valida y se normaliza. Implementación: specification/12-edge.md §7.5.
 *
 * La configuración NO CONTIENE SECRETOS: se puede versionar y revisar. Todo secreto es el NOMBRE de una variable de entorno
 * (`nkeySeedEnv`, `secretEnv`…) y los tokens de entrada son SHA-256 (un hash de 32 bytes aleatorios no se invierte). El gateway:
 *   · se NIEGA a arrancar si ve un secreto en claro (una clave `nkeySeed`, `token`, `secret`, `password`, `pass`…, o un valor con
 *     la forma de una semilla de NATS o de una clave privada), diciendo dónde y cómo se arregla;
 *   · dice QUÉ variable falta, por su nombre, nunca su valor;
 *   · rechaza lo que, de aceptarse, haría algo distinto de lo que parece: un hook sin `verify`, dos entregas para el mismo subject
 *     (comparten durable y se reparten los mensajes: A-47), dos hooks con el mismo nombre, un token repetido.
 *
 * Se recogen TODOS los problemas de una vez (como `gen-acl.mjs`), no el primero: arreglar una configuración de uno en uno es
 * ejecutar el gateway N veces.
 */

import { readFileSync } from "node:fs";

export class ConfigError extends Error {
  /** @param {Array<{ m: string, hint?: string }>} problemas */
  constructor(problemas) {
    super(
      `gateway.json no es válido (${problemas.length} problema${problemas.length === 1 ? "" : "s"}):\n` +
        problemas.map((p) => `  ✗ ${p.m}${p.hint ? `\n    ${p.hint}` : ""}`).join("\n"),
    );
    this.name = "ConfigError";
    this.problemas = problemas;
  }
}

const NOMBRE = /^[a-z0-9]+(-[a-z0-9]+)*$/;
const VAR_ENTORNO = /^[A-Z_][A-Z0-9_]*$/;
const HEX64 = /^[0-9a-f]{64}$/;
const SEMILLA_NATS = /^S[UAONC][A-Z2-7]{50,}$/;
const CLASIFICACIONES = ["public", "internal", "confidential", "restricted"];
const FORMATOS_FIRMA = ["hex", "sha256-prefix", "t-v1"];
const UNIDADES_TIEMPO = ["seconds", "milliseconds", "iso"];

/** Claves que NUNCA llevan un valor en claro: sus equivalentes son `<clave>Env`. */
const CLAVES_SECRETAS = new Set(["nkeyseed", "credscontent", "token", "secret", "secrets", "password", "pass", "pwd", "apikey", "privatekey", "seed"]);

export const DEFECTOS = Object.freeze({
  listen: { host: "127.0.0.1", port: 8080 },
  limits: {
    maxBodyBytes: 1_048_576,
    headersTimeoutMs: 10_000,
    requestTimeoutMs: 30_000,
    serverTimeoutMs: 60_000,
    connectionsCheckingIntervalMs: 1000,
    rateLimit: { requests: 120, perSeconds: 60 },
  },
  shutdownTimeoutMs: 10_000,
  delivery: { timeoutMs: 10_000, allowClassifications: ["public", "internal"] },
  hookToleranceSeconds: 300,
});

/** Lee y valida un fichero. */
export function cargarConfigDeFichero(ruta, opciones) {
  let texto;
  try {
    texto = readFileSync(ruta, "utf8");
  } catch (e) {
    throw new ConfigError([{ m: `no se puede leer "${ruta}": ${e.code ?? e.message}`, hint: "Indica otro con --config <fichero>" }]);
  }
  let json;
  try {
    json = JSON.parse(texto);
  } catch (e) {
    throw new ConfigError([{ m: `"${ruta}" no es JSON válido: ${e.message}` }]);
  }
  return cargarConfig(json, opciones);
}

/**
 * Valida y normaliza.
 * @param {any} cfg
 * @param {{ env?: Record<string,string|undefined> }} [o]
 */
export function cargarConfig(cfg, { env = process.env } = {}) {
  const problemas = [];
  const err = (m, hint) => problemas.push({ m, hint });

  if (cfg === null || typeof cfg !== "object" || Array.isArray(cfg)) throw new ConfigError([{ m: "la configuración debe ser un objeto JSON" }]);

  // ── Ningún secreto en claro ────────────────────────────────────────────────────────────────────────────────────
  buscarSecretos(cfg, "", err);

  const conocidas = new Set(["listen", "limits", "nats", "environment", "version", "schemaBaseUrl", "bundleFile", "validation", "shutdownTimeoutMs", "clients"]);
  for (const k of Object.keys(cfg)) if (!conocidas.has(k) && !k.startsWith("$")) err(`clave desconocida «${k}» en la raíz`, `Las de la raíz son ${[...conocidas].join(", ")}`);

  const listen = { ...DEFECTOS.listen, ...(cfg.listen ?? {}) };
  if (!Number.isInteger(listen.port) || listen.port < 0 || listen.port > 65535) err("`listen.port` debe ser un entero entre 0 y 65535");
  if (typeof listen.host !== "string" || listen.host === "") err("`listen.host` debe ser una cadena");
  if (listen.tls !== undefined) {
    if (typeof listen.tls?.certFile !== "string" || typeof listen.tls?.keyFile !== "string") err("`listen.tls` necesita `certFile` y `keyFile` (rutas: el contenido es secreto y no va en la configuración)");
  }

  const limits = { ...DEFECTOS.limits, ...(cfg.limits ?? {}) };
  limits.rateLimit = { ...DEFECTOS.limits.rateLimit, ...(cfg.limits?.rateLimit ?? {}) };
  if (!(limits.maxBodyBytes > 0) || limits.maxBodyBytes > 1_048_576) err("`limits.maxBodyBytes` debe estar entre 1 y 1048576 (1 MiB, el límite del evento: 01-envelope.md §7)");
  comprobarLimite(limits.rateLimit, "limits.rateLimit", err);
  if (!(limits.serverTimeoutMs > 25_000)) err("`limits.serverTimeoutMs` debe superar 25000: mide inactividad del socket mientras el gateway espera al broker, y un `rpc` puede tardar 25 s (A-153)");
  if (!(limits.connectionsCheckingIntervalMs > 0 && limits.connectionsCheckingIntervalMs <= 5000)) err("`limits.connectionsCheckingIntervalMs` debe estar entre 1 y 5000: con el valor por defecto de Node (30 s) `headersTimeout` y `requestTimeout` no se aplican a tiempo (A-153)");
  if (!(limits.headersTimeoutMs < limits.requestTimeoutMs && limits.requestTimeoutMs <= limits.serverTimeoutMs)) err("debe cumplirse `headersTimeoutMs` < `requestTimeoutMs` ≤ `serverTimeoutMs`");

  const nats = cfg.nats ?? {};
  const servers = nats.servers;
  if (!(typeof servers === "string" && servers !== "") && !(Array.isArray(servers) && servers.length > 0 && servers.every((s) => typeof s === "string"))) err("`nats.servers` debe ser una URL o una lista de URLs (`nats://host:4222`, `tls://host:4222`)");
  const natsTls = nats.tls;
  if (natsTls !== undefined) {
    for (const k of Object.keys(natsTls)) if (!["caFile", "certFile", "keyFile"].includes(k)) err(`clave desconocida «${k}» en \`nats.tls\``, "Son rutas: caFile, certFile, keyFile");
  }

  if (typeof cfg.environment !== "string" || !NOMBRE.test(cfg.environment)) err("`environment` es obligatorio: `produccion`, `staging`… (kebab-case; alimenta `source`)");
  if (typeof cfg.version !== "string" || cfg.version === "") err("`version` es obligatoria: la SemVer del gateway (va en `producerversion`)");
  if (cfg.validation !== undefined && cfg.validation !== "off" && cfg.validation !== "strict") err("`validation` es `off` o `strict`");
  if (cfg.validation === "strict" && typeof cfg.bundleFile !== "string") err("`validation: \"strict\"` necesita `bundleFile` (schemas/bundle.json): L3 no resuelve esquemas por red");

  // ── Clientes ───────────────────────────────────────────────────────────────────────────────────────────────────
  const clientes = [];
  const servicios = new Set();
  const tokenIds = new Set();
  const digests = new Set();
  const hooksVistos = new Map();
  if (!Array.isArray(cfg.clients) || cfg.clients.length === 0) err("`clients` debe ser una lista con al menos un cliente de borde");

  const faltan = new Set();
  const env_ = (nombre, donde) => {
    // Una semilla de NATS tiene la forma de un nombre de variable (mayúsculas y dígitos): NUNCA se cita lo que se leyó, ni se trata
    // como «variable que falta»: acabaría en un mensaje de error, y de ahí en un log.
    if (typeof nombre !== "string" || !VAR_ENTORNO.test(nombre) || nombre.length > 64 || SEMILLA_NATS.test(nombre)) {
      err(`${donde}: el valor no es el NOMBRE de una variable de entorno`, "Debe ser MAYÚSCULAS con guiones bajos (`TIENDA_WEB_NKEY_SEED`). El valor del secreto no va en gateway.json: va en el entorno");
      return undefined;
    }
    const v = env[nombre];
    if (v === undefined || v === "") {
      faltan.add(`${nombre} (${donde})`);
      return undefined;
    }
    return v;
  };

  for (const [i, c] of (Array.isArray(cfg.clients) ? cfg.clients : []).entries()) {
    const donde = `clients[${i}]${typeof c?.service === "string" ? ` (${c.service})` : ""}`;
    if (c === null || typeof c !== "object" || Array.isArray(c)) {
      err(`${donde}: debe ser un objeto`);
      continue;
    }
    const permitidas = ["service", "tenantId", "tenants", "classification", "credentials", "tokens", "rateLimit", "hooks", "deliveries"];
    for (const k of Object.keys(c)) if (!permitidas.includes(k) && !k.startsWith("$")) err(`${donde}: clave desconocida «${k}»`, `Las de un cliente son ${permitidas.join(", ")}`);

    if (typeof c.service !== "string" || !NOMBRE.test(c.service)) err(`${donde}: \`service\` debe ser el nombre del servicio del manifiesto (kebab-case)`);
    else if (servicios.has(c.service)) err(`${donde}: el servicio «${c.service}» aparece dos veces`, "Cada cliente es UNA identidad NATS: dos entradas compartirían durables e inbox");
    servicios.add(c.service);

    const tenantId = c.tenantId;
    if (typeof tenantId !== "string" || !NOMBRE.test(tenantId) || tenantId.length > 64) err(`${donde}: \`tenantId\` es obligatorio (kebab-case, 64 caracteres como máximo): el tenant por defecto de lo que publica`, "El estado no tiene `system` implícito (10-state.md §2.4)");
    const tenants = c.tenants === undefined ? [tenantId] : c.tenants;
    if (!Array.isArray(tenants) || tenants.length === 0 || !tenants.every((t) => typeof t === "string" && NOMBRE.test(t))) err(`${donde}: \`tenants\` debe ser una lista no vacía de tenants (los que acepta la cabecera \`Flux-Tenant\`)`);
    else if (!tenants.includes(tenantId)) err(`${donde}: \`tenants\` debe incluir el \`tenantId\` por defecto (${tenantId})`);

    if (c.classification !== undefined && !CLASIFICACIONES.includes(c.classification)) err(`${donde}: \`classification\` debe ser ${CLASIFICACIONES.join(", ")}`);

    // Credenciales NATS: UN método, y todo lo secreto por nombre de variable.
    const cred = c.credentials ?? {};
    const metodos = [];
    const credenciales = {};
    if (cred.creds !== undefined) {
      if (typeof cred.creds !== "string" || cred.creds === "") err(`${donde}: \`credentials.creds\` es la RUTA de un fichero .creds`);
      else { metodos.push("creds"); credenciales.creds = cred.creds; }
    }
    for (const [clave, destino] of [["credsContentEnv", "credsContent"], ["nkeySeedEnv", "nkeySeed"], ["tokenEnv", "token"]]) {
      if (cred[clave] !== undefined) {
        metodos.push(clave);
        const v = env_(cred[clave], `${donde} credentials.${clave}`);
        if (v !== undefined) credenciales[destino] = v;
      }
    }
    if (cred.userEnv !== undefined || cred.passEnv !== undefined) {
      metodos.push("userEnv+passEnv");
      if (cred.userEnv === undefined || cred.passEnv === undefined) err(`${donde}: \`credentials\` necesita \`userEnv\` y \`passEnv\` a la vez`);
      else {
        const u = env_(cred.userEnv, `${donde} credentials.userEnv`);
        const p = env_(cred.passEnv, `${donde} credentials.passEnv`);
        if (u !== undefined && p !== undefined) { credenciales.user = u; credenciales.pass = p; }
      }
    }
    const permitidasCred = ["creds", "credsContentEnv", "nkeySeedEnv", "tokenEnv", "userEnv", "passEnv"];
    for (const k of Object.keys(cred)) if (!permitidasCred.includes(k) && !k.startsWith("$")) err(`${donde}: \`credentials.${k}\` no existe`, `Los métodos son ${permitidasCred.join(", ")}: lo secreto va por NOMBRE de variable de entorno (\`<clave>Env\`)`);
    if (metodos.length !== 1) err(`${donde}: \`credentials\` debe tener UN método (hay ${metodos.length})`, "Un .creds (`creds`: ruta), una nkey (`nkeySeedEnv`), un token (`tokenEnv`) o usuario y contraseña (`userEnv` + `passEnv`)");

    // Tokens de entrada: SHA-256.
    const tokens = [];
    if (!Array.isArray(c.tokens) || c.tokens.length === 0) err(`${donde}: \`tokens\` debe ser una lista con al menos un token (\`{ "id", "sha256" }\`)`, "Se genera con `flux-gateway token`: imprime el token (una vez, para el cliente) y su SHA-256 (para esta lista)");
    for (const [j, t] of (Array.isArray(c.tokens) ? c.tokens : []).entries()) {
      const dt = `${donde} tokens[${j}]`;
      if (typeof t?.id !== "string" || !NOMBRE.test(t.id)) { err(`${dt}: \`id\` debe ser un nombre (kebab-case): qué token es, para los registros y la rotación`); continue; }
      if (typeof t.sha256 !== "string" || !HEX64.test(t.sha256)) { err(`${dt}: \`sha256\` debe ser el SHA-256 del token en hexadecimal minúsculas (64 caracteres)`, "Nunca el token: `flux-gateway token` imprime las dos cosas"); continue; }
      for (const k of Object.keys(t)) if (k !== "id" && k !== "sha256" && !k.startsWith("$")) err(`${dt}: clave desconocida «${k}»`);
      if (tokenIds.has(t.id)) err(`${dt}: el id «${t.id}» está repetido`);
      if (digests.has(t.sha256)) err(`${dt}: ese token ya es de otro cliente`, "Un token identifica a UN cliente: compartirlo haría imposible saber quién llamó");
      tokenIds.add(t.id);
      digests.add(t.sha256);
      tokens.push({ tokenId: t.id, sha256: t.sha256, cliente: c.service });
    }

    const rateLimit = c.rateLimit === undefined ? undefined : { ...limits.rateLimit, ...c.rateLimit };
    if (rateLimit) comprobarLimite(rateLimit, `${donde} rateLimit`, err);

    // Hooks
    const hooks = [];
    for (const [nombre, def] of Object.entries(c.hooks ?? {})) {
      if (nombre.startsWith("$")) continue;
      const h = normalizarHook(nombre, def, `${donde} hook «${nombre}»`, env_, err);
      if (hooksVistos.has(nombre)) err(`${donde} hook «${nombre}»: el nombre ya es de ${hooksVistos.get(nombre)}`, "`POST /v1/hooks/{nombre}` identifica al cliente por el nombre: debe ser único en todo el gateway");
      hooksVistos.set(nombre, c.service);
      if (h) hooks.push({ ...h, cliente: c.service });
    }

    // Entregas
    const entregas = [];
    const vistas = new Set();
    for (const [j, def] of (c.deliveries ?? []).entries()) {
      const e = normalizarEntrega(def, `${donde} deliveries[${j}]`, env_, err);
      if (!e) continue;
      const clave = e.kind === "event" ? `e:${e.subject}` : `s:${e.contract}:${e.tenant}`;
      if (vistas.has(clave)) err(`${donde} deliveries[${j}]: ya hay una entrega para ${e.kind === "event" ? e.subject : `${e.contract} (${e.tenant})`}`, "Dos URLs para el mismo evento compartirían durable, y un durable compartido REPARTE los mensajes entre quienes lo usan: cada URL vería la mitad (A-47). Una entrega por subject y cliente");
      vistas.add(clave);
      entregas.push(e);
    }

    clientes.push({
      service: c.service, tenantId, tenants: Array.isArray(tenants) ? tenants : [tenantId], classification: c.classification,
      credentials: credenciales, tokens, rateLimit, hooks, deliveries: entregas,
    });
  }

  if (faltan.size) err(`faltan ${faltan.size} variable(s) de entorno: ${[...faltan].join(", ")}`, "El gateway lee los secretos del entorno (o de tu gestor de secretos, que las exporta): defínelas antes de arrancar");

  if (problemas.length) throw new ConfigError(problemas);

  return {
    listen,
    limits,
    nats: { servers, ...(natsTls && { tls: natsTls }) },
    environment: cfg.environment,
    version: cfg.version,
    schemaBaseUrl: cfg.schemaBaseUrl ?? "https://schemas.internal",
    ...(cfg.bundleFile && { bundleFile: cfg.bundleFile }),
    validation: cfg.validation ?? "off",
    shutdownTimeoutMs: cfg.shutdownTimeoutMs ?? DEFECTOS.shutdownTimeoutMs,
    clientes,
  };
}

// ─── Piezas ──────────────────────────────────────────────────────────────────────────────────────────────────────

function comprobarLimite(l, donde, err) {
  if (!(Number.isFinite(l.requests) && l.requests > 0) || !(Number.isFinite(l.perSeconds) && l.perSeconds > 0)) err(`\`${donde}\` necesita \`requests\` y \`perSeconds\` positivos`);
  if (l.burst !== undefined && !(Number.isFinite(l.burst) && l.burst >= 1)) err(`\`${donde}.burst\` debe ser ≥ 1`);
}

/** Recorre toda la configuración buscando un secreto en claro. */
function buscarSecretos(v, ruta, err) {
  if (Array.isArray(v)) return v.forEach((x, i) => buscarSecretos(x, `${ruta}[${i}]`, err));
  if (v !== null && typeof v === "object") {
    for (const [k, x] of Object.entries(v)) {
      const aqui = ruta ? `${ruta}.${k}` : k;
      if (CLAVES_SECRETAS.has(k.toLowerCase()) && typeof x === "string") {
        err(`\`${aqui}\` lleva un SECRETO EN CLARO`, `gateway.json no contiene secretos: pon el NOMBRE de una variable de entorno en \`${k}Env\` (y el valor, en el entorno). Si ya lo has versionado, ROTA ese secreto`);
      }
      buscarSecretos(x, aqui, err);
    }
    return;
  }
  if (typeof v === "string") {
    if (SEMILLA_NATS.test(v)) err(`\`${ruta}\` tiene la forma de una SEMILLA de NATS (\`${v.slice(0, 2)}…\`)`, "Una semilla es un secreto: va en una variable de entorno, no aquí. Si ya la has versionado, ROTA esa clave");
    if (/-----BEGIN [A-Z ]*PRIVATE KEY-----/.test(v)) err(`\`${ruta}\` contiene una CLAVE PRIVADA`, "Indica la RUTA del fichero (`keyFile`), nunca su contenido");
  }
}

/** Un valor extraído de la petición de un hook: `{ from: "body", pointer: "/id" }` o `{ from: "header", name: "X-Event-Id" }`. */
function normalizarFuente(f, donde, err, { unidad = false } = {}) {
  if (f === undefined) return undefined;
  if (f === null || typeof f !== "object" || (f.from !== "body" && f.from !== "header")) {
    err(`${donde}: debe ser \`{ "from": "body", "pointer": "/id" }\` o \`{ "from": "header", "name": "X-Event-Id" }\``);
    return undefined;
  }
  if (f.from === "body" && (typeof f.pointer !== "string" || !(f.pointer === "" || f.pointer.startsWith("/")))) err(`${donde}: \`pointer\` es un JSON Pointer (RFC 6901): \`/id\`, \`/data/object/id\``);
  if (f.from === "header" && (typeof f.name !== "string" || f.name === "")) err(`${donde}: \`name\` es el nombre de la cabecera`);
  if (unidad && !UNIDADES_TIEMPO.includes(f.unit)) err(`${donde}: \`unit\` debe ser ${UNIDADES_TIEMPO.join(", ")}`);
  return { ...f, ...(f.name && { name: f.name.toLowerCase() }) };
}

function normalizarHook(nombre, def, donde, env_, err) {
  if (!NOMBRE.test(nombre)) err(`${donde}: el nombre debe ser kebab-case en minúsculas (va en la URL: \`/v1/hooks/${nombre}\`)`);
  if (def === null || typeof def !== "object") { err(`${donde}: debe ser un objeto`); return undefined; }
  const permitidas = ["subject", "verify", "idempotent", "externalKey", "time", "aggregateId", "classification", "data", "rateLimit"];
  for (const k of Object.keys(def)) if (!permitidas.includes(k) && !k.startsWith("$")) err(`${donde}: clave desconocida «${k}»`, `Las de un hook son ${permitidas.join(", ")}`);

  if (typeof def.subject !== "string" || def.subject === "") err(`${donde}: \`subject\` es el subject del evento que se publica (en el dominio del cliente)`);

  // `verify` es obligatorio: un endpoint público que publica eventos sin autenticar es una puerta abierta.
  const v = def.verify;
  let verify;
  if (v === undefined || v === null || typeof v !== "object") {
    err(`${donde}: falta \`verify\``, "Un hook sin verificación no arranca: es un endpoint público que publica eventos. Verificación HMAC-SHA256: `{ \"scheme\": \"hmac-sha256\", \"secretEnv\": \"…\", \"signature\": { \"header\": \"…\", \"format\": \"hex|sha256-prefix|t-v1\" } }`");
  } else {
    if (v.scheme !== "hmac-sha256") err(`${donde} verify.scheme: solo se admite "hmac-sha256"`);
    const nombres = Array.isArray(v.secretEnv) ? v.secretEnv : [v.secretEnv];
    const secretos = [];
    if (v.secretEnv === undefined || nombres.length === 0) err(`${donde} verify.secretEnv: nombre de la variable de entorno con el secreto (o una lista, para rotar)`);
    else for (const n of nombres) { const s = env_(n, `${donde} verify.secretEnv`); if (s !== undefined) secretos.push(s); }
    const sig = v.signature ?? {};
    if (typeof sig.header !== "string" || sig.header === "") err(`${donde} verify.signature.header: la cabecera que lleva la firma`);
    if (!FORMATOS_FIRMA.includes(sig.format)) err(`${donde} verify.signature.format debe ser ${FORMATOS_FIRMA.join(", ")}`, "`hex`: el HMAC tal cual. `sha256-prefix`: `sha256=<hex>`, como GitHub. `t-v1`: `t=<unix>,v1=<hex>`, como Stripe y como Flux-Signature");
    const toleranciaSeg = v.toleranceSeconds ?? DEFECTOS.hookToleranceSeconds;
    if (!(Number.isInteger(toleranciaSeg) && toleranciaSeg > 0)) err(`${donde} verify.toleranceSeconds debe ser un entero positivo`);
    verify = { secretos, header: typeof sig.header === "string" ? sig.header.toLowerCase() : "", formato: sig.format, toleranciaSeg };
  }

  const idempotente = def.idempotent !== false;
  const externalKey = normalizarFuente(def.externalKey, `${donde} externalKey`, err);
  const time = normalizarFuente(def.time, `${donde} time`, err, { unidad: true });
  if (idempotente) {
    if (!externalKey) err(`${donde}: falta \`externalKey\``, "De dónde sale la clave del hecho en el proveedor (su id de evento), para el id determinista (01-envelope.md §2.6.1). Si el proveedor no da ninguna, declara `\"idempotent\": false` (y el consumidor deduplicará por una clave de negocio)");
    if (!time) err(`${donde}: falta \`time\``, "El instante del hecho EXTERNO es lo que hace estable el id (no «ahora»). Si el proveedor no lo da, no hay id determinista: declara `\"idempotent\": false`");
  } else if (externalKey || time) {
    err(`${donde}: \`idempotent: false\` con \`externalKey\` o \`time\``, "Sin id determinista no se usan: quita `idempotent: false` o quita las fuentes");
  }
  const aggregateId = normalizarFuente(def.aggregateId, `${donde} aggregateId`, err);

  const classification = def.classification ?? "confidential";
  if (!CLASIFICACIONES.includes(classification)) err(`${donde}: \`classification\` debe ser ${CLASIFICACIONES.join(", ")}`);
  else if (classification === "restricted") err(`${donde}: un webhook no puede publicar \`restricted\``, "Credenciales, salud o financiero no salen ni entran por HTTP (06-security.md §5)");

  let datos = { modo: "cuerpo" };
  if (def.data !== undefined) {
    const d = def.data;
    const p = d?.pick;
    if (d === null || typeof d !== "object" || Object.keys(d).some((k) => k !== "pick" && k !== "wrapAs" && !k.startsWith("$"))) err(`${donde} data: las claves son \`pick\` y \`wrapAs\``);
    if (p !== undefined && (p === null || typeof p !== "object" || Object.keys(p).length === 0 || !Object.entries(p).every(([k, x]) => /^[a-z][A-Za-z0-9]*$/.test(k) && typeof x === "string" && (x === "" || x.startsWith("/"))))) err(`${donde} data.pick: \`{ "pagoId": "/id", "importeCents": "/amount" }\`: claves en camelCase y JSON Pointers`, "Es lo que respeta la regla 5 de AGENTS.md (sin PII en data): solo pasan los campos que nombras");
    if (d?.wrapAs !== undefined && !/^[a-z][A-Za-z0-9]*$/.test(d.wrapAs)) err(`${donde} data.wrapAs debe ser una clave en camelCase`);
    datos = { modo: p ? "pick" : "cuerpo", ...(p && { pick: p }), ...(d?.wrapAs && { wrapAs: d.wrapAs }) };
  }

  return { nombre, subject: def.subject, verify, idempotente, externalKey, time, aggregateId, classification, datos, rateLimit: def.rateLimit };
}

function normalizarEntrega(def, donde, env_, err) {
  if (def === null || typeof def !== "object") { err(`${donde}: debe ser un objeto`); return undefined; }
  const permitidas = ["subject", "contract", "tenant", "url", "secretEnv", "timeoutMs", "allowClassifications", "allowInsecureHttp", "caFile"];
  for (const k of Object.keys(def)) if (!permitidas.includes(k) && !k.startsWith("$")) err(`${donde}: clave desconocida «${k}»`, `Las de una entrega son ${permitidas.join(", ")}`);

  const esEvento = def.subject !== undefined;
  const esEstado = def.contract !== undefined;
  if (esEvento === esEstado) { err(`${donde}: debe llevar \`subject\` (un evento) o \`contract\` (un contrato de estado), no ambos ni ninguno`); return undefined; }
  if (esEstado && (typeof def.tenant !== "string" || !NOMBRE.test(def.tenant))) err(`${donde}: una entrega de estado necesita \`tenant\` (el de su durable)`);
  if (esEvento && def.tenant !== undefined) err(`${donde}: \`tenant\` solo es de las entregas de estado`);

  let url;
  try {
    url = new URL(def.url);
    if (url.protocol !== "https:" && url.protocol !== "http:") throw new Error("protocolo");
    if (url.username || url.password) err(`${donde}: la URL lleva usuario o contraseña`, "Las credenciales de un receptor no van en la configuración: la firma `Flux-Signature` autentica la entrega");
  } catch {
    err(`${donde}: \`url\` debe ser una URL http(s) absoluta`);
  }
  if (url?.protocol === "http:" && def.allowInsecureHttp !== true) err(`${donde}: la URL es \`http://\``, "La firma da integridad, no confidencialidad: el cuerpo viajaría en claro. Usa https, o `\"allowInsecureHttp\": true` si es una red privada y lo has decidido");

  const secretos = [];
  const nombres = Array.isArray(def.secretEnv) ? def.secretEnv : [def.secretEnv];
  if (def.secretEnv === undefined || nombres.length === 0) err(`${donde}: falta \`secretEnv\``, "El NOMBRE de la variable de entorno con el secreto que firma la entrega (o una lista, para rotar sin corte)");
  else for (const n of nombres) { const s = env_(n, `${donde} secretEnv`); if (s !== undefined) secretos.push(s); }

  const timeoutMs = def.timeoutMs ?? DEFECTOS.delivery.timeoutMs;
  if (!(Number.isInteger(timeoutMs) && timeoutMs >= 1 && timeoutMs <= 25_000)) err(`${donde}: \`timeoutMs\` debe ser un entero entre 1 y 25000 (por debajo del ack_wait de 30 s)`);

  const permitidasClas = def.allowClassifications ?? DEFECTOS.delivery.allowClassifications;
  if (!Array.isArray(permitidasClas) || permitidasClas.length === 0 || !permitidasClas.every((x) => CLASIFICACIONES.includes(x))) err(`${donde}: \`allowClassifications\` es una lista de ${CLASIFICACIONES.join(", ")}`);
  else if (permitidasClas.includes("restricted")) err(`${donde}: \`restricted\` no puede salir por HTTP`, "Credenciales, salud o financiero: nunca (12-edge.md §4.7)");
  else if (permitidasClas.includes("confidential") && url?.protocol === "http:" && def.allowInsecureHttp !== true) err(`${donde}: \`confidential\` por \`http://\``);
  if (esEstado && Array.isArray(permitidasClas) && !permitidasClas.includes("confidential")) err(`${donde}: una entrega de estado es \`confidential\` por defecto y necesita autorizarlo`, "Añade `\"allowClassifications\": [\"public\", \"internal\", \"confidential\"]`: el estado lleva el dato (a veces personal) y sale del ecosistema");

  return {
    kind: esEvento ? "event" : "state",
    ...(esEvento ? { subject: def.subject } : { contract: def.contract, tenant: def.tenant }),
    url: def.url, secretos, timeoutMs, allowClassifications: permitidasClas, allowInsecureHttp: def.allowInsecureHttp === true,
    ...(def.caFile && { caFile: def.caFile }),
  };
}
