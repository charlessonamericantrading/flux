/**
 * Lo que `scripts/gen-acl.mjs` (permisos del broker) y `flux provision` (recursos del
 * broker) tienen que decidir IGUAL: qué subjects consume cada servicio, cómo se llaman
 * su stream y su durable, y qué permisos de `$JS.API` hacen falta para usarlos.
 *
 * Por qué es un módulo compartido y no dos copias: si el generador de ACL dice
 * `svc__pedidos_pedido_v1_creado` y el aprovisionador crea `svc__pedidos_pedido_v1_creado_`,
 * el servicio arranca, el broker le deniega su propio durable y el único síntoma es una
 * violación de permisos en un log. Es la misma clase de fallo que el repo persigue —el
 * servidor no se queja donde hay que mirar—, así que la derivación vive en UN sitio.
 *
 * Sin dependencias: `scripts/` no tiene package.json.
 *
 * Contrato normativo: specification/06-security.md §3 y 02-naming.md §3–4.
 */

import { readFile } from "node:fs/promises";

// ─── Patrones ────────────────────────────────────────────────────────────────

export const SERVICE_RE = /^[a-z0-9]+(-[a-z0-9]+)*$/;
export const DOMAIN_RE = SERVICE_RE;
export const SUBJECT_RE =
  /^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]{0,3}\.[a-z0-9]+(-[a-z0-9]+)*$/;
/** Contrato de ESTADO: el subject sin `state.`, sin tenant y sin clave — 10-state.md §2.2. */
export const STATE_CONTRACT_RE =
  /^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]{0,3}$/;
/** El tenant de un ESTADO (no el de un evento, que es cualquier cadena): 10-state.md §2.4. */
export const STATE_TENANT_RE = /^[a-z0-9]+(-[a-z0-9]+)*$/;
export const STATE_TENANT_MAX = 64;
/**
 * Dominios que no pueden ser de nadie — 02-naming.md §1.3. Cada uno es un espacio de nombres del
 * protocolo, y en la ACL el motivo es concreto: un servicio que «fuese dueño» de `state` recibiría
 * `state.>` y escribiría el estado de TODOS los dominios; el dueño de `dlq`, `dlq.>`.
 */
export const RESERVED_DOMAINS = ["dlq", "state", "rpc", "system"];
/** Las claves que admite un servicio del manifiesto. Una errata (`watch`) no puede ser «sin permisos». */
export const SERVICE_KEYS = ["name", "owns", "consumes", "watches", "calls", "serves", "via"];
/**
 * Por dónde se conecta un servicio. Ausente: es un servicio NATIVO, con su propio proceso y su conexión. `"gateway"`:
 * es un CLIENTE DE BORDE (specification/12-edge.md §2), un sistema que no puede mantener una conexión NATS (hosting
 * compartido, cron, serverless, el sistema de un tercero) y cuya conexión vive DENTRO del gateway. Es una ANOTACIÓN:
 * sus permisos son los de cualquier servicio con lo mismo declarado (`owns`, `consumes`, `watches`, `calls`) y el
 * gateway no necesita ninguno especial. Lo único que cambia es lo que el gateway no puede alojar: un `serves`, porque
 * el que atiende una operación necesita una suscripción viva y HTTP no la sostiene.
 * Lo fija `protocol.json` → `edge.manifest.via`.
 */
export const VIA_VALUES = Object.freeze(["gateway"]);
/**
 * El NOMBRE de una operación (`clientes.cliente.v1.obtener`, lo que reciben `request` y `respond`): cuatro tokens, la
 * misma sintaxis que el subject de un evento — 11-request-reply.md §2.2. Lo que separa una operación de un evento es
 * el prefijo `rpc.` del subject de NATS y la gramática del último token (infinitivo, no pasado), y eso no lo fija
 * ninguna regex.
 */
export const OPERATION_RE = SUBJECT_RE;
/**
 * Lo que el dueño de una operación puede responder: UNA respuesta por petición y que caduque. Medido (A-68):
 * `allow_responses: true` admite una y NO caduca; `{ max: 1, expires }` cierra «una petición, una respuesta» y
 * acota lo que haría un dueño comprometido con los inboxes que ha visto. 30 s está por encima del timeout máximo
 * de una llamada (25 s, 11-request-reply.md §4.2): una respuesta legítima nunca llega después.
 * Lo fija `protocol.json` → `rpc.acl.ownerAllowResponses` y lo comprueba `check-protocol-json.mjs`.
 */
export const RPC_ALLOW_RESPONSES = Object.freeze({ max: 1, expires: "30s" });
/** Las erratas de clave más probables, para que el aviso diga cuál era la buena. */
const ERRATAS_DE_CLAVE = { watch: "watches", call: "calls", serve: "serves", vía: "via", gateway: "via" };
/** Clave PÚBLICA de usuario de NATS: `U` + 55 caracteres base32. Una semilla empieza por `S`. */
export const NKEY_USER_RE = /^U[A-Z2-7]{55}$/;

// ─── Nombres (02-naming.md) ──────────────────────────────────────────────────

/** `pedidos` → `EVT_PEDIDOS`. NATS no admite puntos en nombres de stream — 02-naming.md §3. */
export const streamName = (domain) => `EVT_${domain.replace(/-/g, "_").toUpperCase()}`;

export const dlqStreamName = (domain) => `DLQ_${domain.replace(/-/g, "_").toUpperCase()}`;

/**
 * `facturacion-api` + `pedidos.pedido.v1.creado` → `facturacion-api__pedidos_pedido_v1_creado`.
 *
 * Igual que `durableName()` de los SDKs: los puntos Y los guiones del subject pasan a `_`.
 * (02-naming.md §4 solo menciona los puntos; los SDKs de Node y Rust sustituyen también los
 * guiones, y el nombre que importa es el que el SDK pide al broker.)
 */
export const durableName = (service, subject) => `${service}__${subject.replace(/[.-]/g, "_")}`;

/**
 * Durable con el que el dueño de un dominio lee su propia DLQ: `pedidos-api__dlq_pedidos`.
 *
 * No puede derivarse de un subject como los demás porque su filtro es un patrón
 * (`dlq.pedidos.>`) y NATS no admite `>` dentro de un nombre. Tampoco puede chocar con uno
 * de ellos: un durable de evento acaba en un subject de 4 tokens (`…_v1_creado`), y `dlq_<dominio>`
 * solo coincidiría con un subject de 2 tokens, que no es válido.
 */
export const dlqDurableName = (service, domain) =>
  `${service}__dlq_${domain.replace(/-/g, "_")}`;

export const domainOf = (subject) => subject.split(".")[0];

/** `clientes` → `STATE_CLIENTES` — 02-naming.md §3. Mismo criterio que `streamName`. */
export const stateStreamName = (domain) => `STATE_${domain.replace(/-/g, "_").toUpperCase()}`;

/**
 * Durable de un `watch`: `tienda-web__state_clientes_cliente_v1` y, si confina a un tenant,
 * `…__acme` (los guiones del tenant pasan a `_`) — 10-state.md §5.2. El tenant va en el nombre
 * porque un durable compartido REPARTE los mensajes entre quienes lo usan (A-47): dos instancias de
 * un servicio, una por tenant, con un durable sin sufijo se quitarían la mitad de la foto.
 */
export const stateDurableName = (service, contract, tenant) =>
  `${service}__state_${contract.replace(/[.-]/g, "_")}${tenant ? `__${tenant.replace(/-/g, "_")}` : ""}`;

/** Durable con el que el DUEÑO lee la DLQ de estado de su dominio — 10-state.md §6. */
export const dlqStateDurableName = (service, domain) =>
  `${service}__dlq_state_${domain.replace(/-/g, "_")}`;

// ─── Bundle ──────────────────────────────────────────────────────────────────

/**
 * Lo que el bundle dice que existe: `{ subjects, state, rpc }`, los subjects de evento con esquema, los
 * contratos de estado (`clientes.cliente.v1`) y las operaciones de petición/respuesta
 * (`clientes.cliente.v1.obtener`: su par request/reply, 11-request-reply.md §11.1). Un bundle sin índice
 * `state` es de antes de 1.1 y se lee como «ningún dominio tiene estado»; uno sin índice `rpc`, como «ninguna
 * operación existe».
 */
export async function loadBundle(path) {
  let texto;
  try {
    texto = await readFile(path, "utf8");
  } catch (e) {
    throw new Error(
      `no se pudo leer el bundle "${path}": ${e.code ?? e.message}\n` +
        `  Se genera con: node scripts/bundle-schemas.mjs   (o indica otro con --bundle)`,
    );
  }
  let bundle;
  try {
    bundle = JSON.parse(texto);
  } catch (e) {
    throw new Error(`el bundle "${path}" no es JSON válido: ${e.message}`);
  }
  if (!bundle || typeof bundle.subjects !== "object" || bundle.subjects === null) {
    throw new Error(
      `el bundle "${path}" no tiene el índice \`subjects\`: ¿es de una versión antigua? ` +
        `Regenéralo con node scripts/bundle-schemas.mjs`,
    );
  }
  const state = bundle.state && typeof bundle.state === "object" ? Object.keys(bundle.state).sort() : [];
  const rpc = bundle.rpc && typeof bundle.rpc === "object" ? Object.keys(bundle.rpc).sort() : [];
  return { subjects: Object.keys(bundle.subjects).sort(), state, rpc };
}

/** Los subjects que tienen esquema publicado: la fuente de verdad de «qué existe». */
export async function loadBundleSubjects(path) {
  return (await loadBundle(path)).subjects;
}

// ─── Expansión de `consumes` ─────────────────────────────────────────────────

/**
 * Un `consumes` es un subject exacto o un patrón acabado en `>`, y se EXPANDE contra los
 * subjects del bundle. El motivo es que el nombre del durable es un único token y NATS no
 * admite comodines dentro de un token: para crear `svc__pedidos_pedido_v1_creado` y
 * concederle `…CREATE.EVT_PEDIDOS.svc__pedidos_pedido_v1_creado.pedidos.pedido.v1.creado`
 * hay que conocer el subject exacto.
 *
 * Devuelve `{ subjects }` o `{ error, hint }`.
 */
export function expandConsumes(entrada, bundleSubjects) {
  if (typeof entrada !== "string" || entrada === "") {
    return { error: `no es una cadena: ${JSON.stringify(entrada)}`, hint: "Un subject exacto o un patrón acabado en `>`" };
  }
  const tokens = entrada.split(".");

  if (entrada.endsWith(".>") || entrada === ">") {
    const prefijo = tokens.slice(0, -1);
    const raro = prefijo.find((t) => t === "" || t.includes("*") || t.includes(">"));
    if (raro !== undefined) {
      return {
        error: `patrón no admitido "${entrada}"`,
        hint: "Solo se admite un comodín: el `>` final. `*` y `>` en medio del patrón no se expanden",
      };
    }
    const coincidencias = bundleSubjects.filter((s) => s.startsWith(`${prefijo.join(".")}.`));
    if (coincidencias.length === 0) {
      return {
        error: `el patrón "${entrada}" no coincide con ningún subject del bundle`,
        hint:
          "¿Falta el esquema en schemas/? ¿Está mal escrito el patrón? " +
          "Tras añadir un esquema: node scripts/bundle-schemas.mjs",
      };
    }
    return { subjects: coincidencias };
  }

  if (tokens.some((t) => t.includes("*") || t.includes(">"))) {
    return {
      error: `patrón no admitido "${entrada}"`,
      hint: "Solo se admite un subject exacto o un patrón acabado en `>` (p. ej. pedidos.pedido.v1.>)",
    };
  }
  if (!SUBJECT_RE.test(entrada)) {
    return {
      error: `"${entrada}" no es un subject válido`,
      hint: "<dominio>.<agregado>.v<major>.<evento>, 4 tokens en kebab-case — 02-naming.md §1",
    };
  }
  if (!bundleSubjects.includes(entrada)) {
    const cercanos = bundleSubjects.filter((s) => s.startsWith(`${tokens.slice(0, 3).join(".")}.`));
    return {
      error: `el subject "${entrada}" no existe en el bundle`,
      hint: cercanos.length
        ? `Los de ${tokens.slice(0, 3).join(".")} son: ${cercanos.join(", ")}`
        : "No hay ningún esquema para él. Créalo en schemas/ y ejecuta node scripts/bundle-schemas.mjs",
    };
  }
  return { subjects: [entrada] };
}

// ─── Validación del manifiesto ───────────────────────────────────────────────

/**
 * Lo que `analizarManifiesto` sabe del bundle. Acepta el array de subjects de antes de 1.1 (sin
 * contratos de estado ni operaciones) o `{ subjects, state, rpc }` de `loadBundle`.
 */
function normalizarBundle(b) {
  if (Array.isArray(b)) return { subjects: b, state: [], rpc: [] };
  return { subjects: b?.subjects ?? [], state: b?.state ?? [], rpc: b?.rpc ?? [] };
}

/**
 * Normaliza `watches` de un servicio a `{ contract, domain, tenants }`, o `{ error, hint }`.
 *
 * Una entrada es el contrato a secas, `"clientes.cliente.v1"` (TODOS los tenants), o un objeto
 * `{ "contract": "clientes.cliente.v1", "tenants": ["acme"] }` que confina el servicio a esos tenants:
 * el filtro de su durable y su `DIRECT.GET` llevan el tenant, y es el BROKER quien impide el resto
 * (A-14, A-38). `tenants` nunca puede ser una lista vacía: se leería como «ninguno» y significaría
 * «todos», la misma trampa de `allow: []` (A-25).
 */
function normalizarWatch(entrada) {
  if (typeof entrada !== "string" && (entrada === null || typeof entrada !== "object" || Array.isArray(entrada))) {
    return {
      error: `entrada inválida ${JSON.stringify(entrada)}`,
      hint: 'Un contrato ("clientes.cliente.v1") o { "contract": "clientes.cliente.v1", "tenants": ["acme"] }',
    };
  }
  const contract = typeof entrada === "string" ? entrada : entrada.contract;
  if (typeof entrada === "object") {
    const raros = Object.keys(entrada).filter((k) => k !== "contract" && k !== "tenants" && !k.startsWith("$"));
    if (raros.length) {
      return {
        error: `campo desconocido «${raros[0]}» en ${JSON.stringify(entrada)}`,
        hint:
          raros[0] === "tenant"
            ? "¿Querías `tenants`, una lista? Una errata aquí NO puede degradarse a «todos los tenants»"
            : "Un `watches` solo admite `contract` y `tenants`",
      };
    }
  }
  if (typeof contract !== "string" || !STATE_CONTRACT_RE.test(contract)) {
    return {
      error: `"${contract}" no es un contrato de estado válido`,
      hint:
        typeof contract === "string" && SUBJECT_RE.test(contract)
          ? "Es un subject de EVENTO (4 tokens). Un contrato de estado es <dominio>.<agregado>.v<major>, sin evento: clientes.cliente.v1. Un evento va en `consumes`"
          : "<dominio>.<agregado>.v<major>, kebab-case en minúsculas — 10-state.md §2.2",
    };
  }
  const domain = domainOf(contract);
  let tenants = null;
  if (typeof entrada === "object" && "tenants" in entrada) {
    const t = entrada.tenants;
    if (!Array.isArray(t) || t.length === 0) {
      return {
        error: `${contract}: \`tenants\` debe ser una lista NO vacía`,
        hint: 'Para todos los tenants, pon el contrato a secas ("clientes.cliente.v1"): una lista vacía sería «ninguno» escrito como «todos»',
      };
    }
    for (const x of t) {
      if (typeof x !== "string" || !STATE_TENANT_RE.test(x) || x.length > STATE_TENANT_MAX) {
        return {
          error: `${contract}: tenant inválido ${JSON.stringify(x)}`,
          hint: `kebab-case en minúsculas y como mucho ${STATE_TENANT_MAX} caracteres, sin comodines — 10-state.md §2.4. El de un estado es más estricto que el de un evento`,
        };
      }
    }
    if (new Set(t).size !== t.length) {
      return { error: `${contract}: tenants repetidos en ${JSON.stringify(t)}`, hint: "Un durable por tenant: dos entradas darían el mismo" };
    }
    tenants = [...t].sort();
  }
  return { contract, domain, tenants };
}

/**
 * Valida UNA entrada de `calls` o `serves`: el nombre de una operación (`clientes.cliente.v1.obtener`, sin `rpc.`).
 * Devuelve `{ op, domain }` o `{ error, hint }`.
 *
 * El orden es el de 11-request-reply.md §4.1: primero el dominio reservado (`RESERVED_DOMAIN`, con el aviso de que
 * el prefijo lo pone el SDK) y después la sintaxis. Un COMODÍN se rechaza aparte y con su motivo: `calls`
 * concede publicar en el subject EXACTO de cada operación, y `rpc.<dominio>.>` en una llamada es «poder llamar a
 * todo» (A-66, 06-security.md §3.9); en `serves`, el comodín ya es el permiso (`rpc.<dominio>.>`), no la entrada.
 */
export function normalizarOperacion(entrada, clave) {
  if (typeof entrada !== "string" || entrada === "") {
    return {
      error: `entrada inválida ${JSON.stringify(entrada)} en \`${clave}\``,
      hint: "El nombre de una operación: <dominio>.<agregado>.v<major>.<operacion>, p. ej. clientes.cliente.v1.calcular-descuento",
    };
  }
  if (/[*>]/.test(entrada)) {
    return {
      error: `"${entrada}" lleva un comodín`,
      hint:
        clave === "calls"
          ? "Se concede publicar en el subject EXACTO de cada operación: con un comodín (rpc.<dominio>.>) quien llama podría llamar a TODAS. Lista las operaciones una a una"
          : "Lista las operaciones una a una. El permiso de suscripción ya es `rpc.<dominio>.>`; el SDK se suscribe al subject EXACTO de cada una (A-64)",
    };
  }
  const domain = domainOf(entrada);
  if (RESERVED_DOMAINS.includes(domain)) {
    return {
      error: `"${entrada}" es de un dominio RESERVADO (RESERVED_DOMAIN)`,
      hint:
        domain === "rpc"
          ? `El prefijo \`rpc.\` lo pone el SDK, no el nombre de la operación: escribe "${entrada.split(".").slice(1).join(".")}" — 11-request-reply.md §2.2`
          : `${RESERVED_DOMAINS.join(", ")} son espacios de nombres del protocolo, no dominios — 02-naming.md §1.3`,
    };
  }
  if (!OPERATION_RE.test(entrada)) {
    return {
      error: `"${entrada}" no es una operación válida`,
      hint:
        "<dominio>.<agregado>.v<major>.<operacion>: cuatro tokens en kebab-case y la operación en INFINITIVO (obtener, calcular-descuento), " +
        "sin el prefijo `rpc.` — 11-request-reply.md §2.2",
    };
  }
  return { op: entrada, domain };
}

/**
 * Valida el manifiesto contra el modelo y expande cada `consumes`, cada `watches`, cada `calls` y cada `serves`.
 * Devuelve `{ problemas: [{ m, hint }], servicios: [...] }`; `servicios` solo es fiable si
 * no hay problemas.
 *
 * `bundle` es `loadBundle(...)` (`{ subjects, state, rpc }`) o, por compatibilidad, la lista de subjects.
 * Cada servicio sale con `watches` (`[{ contract, domain, tenants|null }]`) y `stateOwns`, los
 * dominios que posee y que TIENEN estado —los que tienen un contrato en `schemas/state/`,
 * 10-state.md §2.1—; y con `calls` y `serves` (las operaciones, ordenadas y sin repetir) y `rpcDomains`, los
 * dominios de las operaciones que atiende: los de su permiso de suscripción `rpc.<dominio>.>`.
 */
export function analizarManifiesto(manifiesto, bundle) {
  const { subjects: bundleSubjects, state: bundleState, rpc: bundleRpc } = normalizarBundle(bundle);
  const dominiosConEstado = new Set(bundleState.map(domainOf));
  const problemas = [];
  const err = (m, hint) => problemas.push({ m, hint });
  const lista = manifiesto?.services;

  if (!Array.isArray(lista) || lista.length === 0) {
    err("el manifiesto no declara ningún servicio", "Falta `services`, o está vacío");
    return { problemas, servicios: [], dueñoDe: new Map(), dominiosConEstado };
  }

  const dueñoDe = new Map(); // dominio → servicio
  const nombres = new Set();

  for (const svc of lista) {
    if (!SERVICE_RE.test(svc?.name ?? "")) {
      err(
        `nombre de servicio inválido: "${svc?.name}"`,
        "kebab-case en minúsculas — alimenta los nombres de durable consumer (02-naming.md §4) " +
          "y el prefijo del inbox privado (_INBOX.<servicio>.>)",
      );
    } else if (nombres.has(svc.name)) {
      err(
        `el servicio "${svc.name}" aparece dos veces`,
        "Cada servicio tiene su identidad: dos entradas con el mismo nombre compartirían durables e inbox — 06-security.md §2",
      );
    }
    nombres.add(svc?.name);

    // Una errata en una clave (`watch` por `watches`) dejaría al servicio sin concesión y sin
    // ningún aviso del generador: el primer síntoma sería un PERMISSION_DENIED en producción.
    for (const k of Object.keys(svc ?? {})) {
      if (!SERVICE_KEYS.includes(k) && !k.startsWith("$")) {
        err(
          `${svc?.name}: clave desconocida «${k}»`,
          `Las de un servicio son ${SERVICE_KEYS.join(", ")}` + (ERRATAS_DE_CLAVE[k] ? `. ¿Querías \`${ERRATAS_DE_CLAVE[k]}\`?` : ""),
        );
      }
    }

    for (const campo of ["owns", "consumes", "watches", "calls", "serves"]) {
      if (svc?.[campo] !== undefined && !Array.isArray(svc[campo])) {
        err(`${svc?.name}: \`${campo}\` debe ser una lista`, `Es ${JSON.stringify(svc[campo])}`);
      }
    }

    // `via`: un cliente de borde. Un valor desconocido no puede degradar a «nativo» en silencio, y un cliente de
    // borde no atiende operaciones: el gateway no puede sostener la suscripción viva que exige `respond()`.
    if (svc?.via !== undefined) {
      if (!VIA_VALUES.includes(svc.via)) {
        err(
          `${svc?.name}: \`via\` vale ${JSON.stringify(svc.via)}`,
          `El único valor es ${VIA_VALUES.map((v) => JSON.stringify(v)).join(", ")} (un cliente de borde, alojado en el gateway — 12-edge.md §2). Sin \`via\`, el servicio es nativo`,
        );
      } else if (Array.isArray(svc.serves) && svc.serves.length > 0) {
        err(
          `${svc.name} es un cliente de borde (\`via: "gateway"\") y atiende operaciones (\`serves\`)`,
          "El que atiende una operación necesita una suscripción VIVA al subject de cada una (11-request-reply.md §2.3), y un cliente HTTP no la sostiene. " +
            "Atiéndela con un servicio nativo, o quita `serves`",
        );
      }
    }

    for (const d of Array.isArray(svc?.owns) ? svc.owns : []) {
      if (!DOMAIN_RE.test(d)) err(`dominio inválido "${d}" en ${svc.name}`, "kebab-case en minúsculas");
      if (RESERVED_DOMAINS.includes(d)) {
        err(
          `${svc.name} es dueño de "${d}", un dominio RESERVADO (RESERVED_DOMAIN)`,
          `${RESERVED_DOMAINS.join(", ")} son espacios de nombres del protocolo — 02-naming.md §1.3. ` +
            `Con él, el permiso de dominio propio sería \`${d}.>\`: ` +
            (d === "state" ? "escribir el estado de TODOS los dominios" : "publicar en el espacio de nombres del protocolo"),
        );
      }
      // UN dueño por dominio. Dos servicios publicando el mismo dominio destruye la
      // premisa entera del protocolo: "solo el dueño del agregado publica sus eventos"
      // (00-protocol.md §4). Con dos dueños nadie puede confiar en el `source`.
      const previo = dueñoDe.get(d);
      if (previo && previo !== svc.name) {
        err(
          `el dominio "${d}" tiene dos dueños: ${previo} y ${svc.name}`,
          "Solo el servicio propietario del agregado publica sus eventos — 00-protocol.md §4",
        );
      }
      dueñoDe.set(d, svc.name);
    }
  }

  // ── Petición/respuesta: `serves` primero (de ahí sale quién atiende cada operación) y `calls` después ──────────────
  // Solo el DUEÑO responde (11-request-reply.md §2.3): dos servicios sobre la misma operación la ejecutan los dos y
  // quien llama ve UNA respuesta (A-60). La ACL lo hace cumplir; el manifiesto, antes de generarla.
  const dondeFalta = (op) => {
    const cercanos = bundleRpc.filter((o) => o.split(".").slice(0, 3).join(".") === op.split(".").slice(0, 3).join("."));
    return cercanos.length
      ? `Las operaciones de ${op.split(".").slice(0, 3).join(".")} son: ${cercanos.join(", ")}`
      : `No hay ningún esquema para ella. Crea el PAR request/reply en ` +
        `schemas/rpc/${op.split(".")[0]}/${op.split(".")[1]}/${op.split(".")[3]}/{request,reply}/${op.split(".")[2].slice(1)}.0.0.json ` +
        `y ejecuta node scripts/bundle-schemas.mjs — 11-request-reply.md §11`;
  };
  const atiende = new Map(); // operación → servicio que la atiende
  const operaciones = new Map(); // servicio → { calls, serves } normalizados
  for (const svc of lista) {
    const dichas = { calls: [], serves: [] };
    for (const clave of ["calls", "serves"]) {
      const vistas = new Set();
      for (const entrada of Array.isArray(svc?.[clave]) ? svc[clave] : []) {
        const n = normalizarOperacion(entrada, clave);
        if (n.error) {
          err(`${svc.name}: ${clave}: ${n.error}`, n.hint);
          continue;
        }
        if (vistas.has(n.op)) {
          err(`${svc.name}: ${clave}: la operación "${n.op}" está dos veces`, "Una vez basta: el permiso es el mismo");
          continue;
        }
        vistas.add(n.op);
        if (!bundleRpc.includes(n.op)) {
          err(`${svc.name}: ${clave}: la operación "${n.op}" no existe en el bundle`, dondeFalta(n.op));
          continue;
        }
        dichas[clave].push(n);
      }
    }
    operaciones.set(svc?.name, dichas);

    for (const { op, domain } of dichas.serves) {
      const dueño = dueñoDe.get(domain);
      if (dueño !== svc.name) {
        err(
          dueño
            ? `${svc.name} atiende "${op}", pero el dominio "${domain}" es de ${dueño}`
            : `${svc.name} atiende "${op}" sin ser dueño de "${domain}"`,
          dueño
            ? "Solo el dueño responde: con dos servicios sobre la misma operación los dos la ejecutan y quien llama ve UNA respuesta (A-60) — 11-request-reply.md §2.5"
            : `Añade "${domain}" a \`owns\` de ${svc.name}, o quita la operación: solo el dueño de un dominio responde sus operaciones`,
        );
        continue;
      }
      atiende.set(op, svc.name);
    }
  }

  const servicios = [];

  for (const svc of lista) {
    const consumidos = new Set();
    const escritos = Array.isArray(svc?.consumes) ? svc.consumes : [];

    for (const c of escritos) {
      const dominio = typeof c === "string" ? domainOf(c) : "";
      if (dominio.includes("*") || dominio.includes(">")) {
        err(
          `${svc.name} consume "${c}" con comodín en el dominio`,
          "Conceder un dominio entero por comodín es lo contrario de una concesión explícita — 06-security.md §3",
        );
        // Sin `continue`, un comodín produce además "nadie es dueño de *": dos hallazgos
        // para una sola causa raíz, y el segundo despista.
        continue;
      }
      if (RESERVED_DOMAINS.includes(dominio)) {
        err(
          `${svc.name} consume "${c}", de un dominio RESERVADO (RESERVED_DOMAIN)`,
          dominio === "state"
            ? 'El estado no se consume como un evento: declara el contrato en `watches` ("clientes.cliente.v1") — 06-security.md §3.8'
            : `${RESERVED_DOMAINS.join(", ")} son espacios de nombres del protocolo, no dominios — 02-naming.md §1.3`,
        );
        continue;
      }

      const r = expandConsumes(c, bundleSubjects);
      if (r.error) {
        err(`${svc.name}: ${r.error}`, r.hint);
        continue;
      }

      if (!dueñoDe.has(dominio)) {
        err(
          `${svc.name} consume "${c}" pero nadie declara ser dueño de "${dominio}"`,
          "O falta el productor en el manifiesto, o el subject está mal escrito",
        );
      }
      if (Array.isArray(svc.owns) && svc.owns.includes(dominio)) {
        err(
          `${svc.name} se suscribe a su propio dominio "${dominio}"`,
          "Un servicio que consume sus propios eventos suele ser una señal de que el flujo debería ser una llamada interna, no un evento",
        );
      }
      for (const s of r.subjects) consumidos.add(s);
    }

    // Dos subjects distintos pueden dar el mismo nombre de durable (`a-b.c…` y `a.b-c…`
    // → `a_b_c…`): el segundo `consumers.add` reutilizaría el durable del primero con otro
    // filtro y el servicio recibiría los eventos equivocados sin ningún error.
    const porDurable = new Map();
    for (const s of consumidos) {
      const d = durableName(svc.name, s);
      if (porDurable.has(d)) {
        err(
          `${svc.name}: "${porDurable.get(d)}" y "${s}" dan el mismo durable "${d}"`,
          "Los guiones y los puntos del subject pasan a `_`. Un durable por subject: renombra uno de los dos eventos",
        );
      }
      porDurable.set(d, s);
    }

    // ── Observaciones de estado (`watches`) ───────────────────────────────────
    const observados = [];
    const contratos = new Set();
    for (const w of Array.isArray(svc?.watches) ? svc.watches : []) {
      const n = normalizarWatch(w);
      if (n.error) {
        err(`${svc.name}: watches: ${n.error}`, n.hint);
        continue;
      }
      const { contract, domain } = n;
      if (RESERVED_DOMAINS.includes(domain)) {
        err(`${svc.name} observa "${contract}", de un dominio RESERVADO (RESERVED_DOMAIN)`, `${RESERVED_DOMAINS.join(", ")} no son dominios — 02-naming.md §1.3`);
        continue;
      }
      if (contratos.has(contract)) {
        err(
          `${svc.name}: observa dos veces "${contract}"`,
          "Un solo `watches` por contrato: con `tenants` si confina, sin él si observa todos. Dos darían, además, el mismo durable",
        );
        continue;
      }
      contratos.add(contract);
      if (!bundleState.includes(contract)) {
        const cercanos = bundleState.filter((s) => domainOf(s) === domain);
        err(
          `${svc.name} observa "${contract}", que no existe en el bundle`,
          cercanos.length
            ? `Los contratos de estado de ${domain} son: ${cercanos.join(", ")}`
            : `No hay ningún esquema de estado de "${domain}". Créalo en schemas/state/${domain}/<agregado>/<semver>.json ` +
                `y ejecuta node scripts/bundle-schemas.mjs — 10-state.md §10`,
        );
        continue;
      }
      if (!dueñoDe.has(domain)) {
        err(
          `${svc.name} observa "${contract}" pero nadie declara ser dueño de "${domain}"`,
          "O falta el dueño del estado en el manifiesto, o el contrato está mal escrito",
        );
      }
      if (Array.isArray(svc.owns) && svc.owns.includes(domain)) {
        err(
          `${svc.name} observa su propio estado "${contract}"`,
          "El dueño ya lo escribe y lo lee con `get` (DIRECT.GET de su dominio): una réplica local de lo que él mismo guarda " +
            "suele ser una señal de que ese dato debería vivir en su base de datos",
        );
      }
      observados.push(n);
    }
    // Un durable por (contrato, tenant): los guiones y los puntos pasan a `_`, y dos contratos
    // distintos pueden dar el mismo nombre.
    const porDurableEstado = new Map();
    for (const w of observados) {
      for (const t of w.tenants ?? [null]) {
        const d = stateDurableName(svc.name, w.contract, t);
        const previo = porDurableEstado.get(d);
        if (previo !== undefined && previo !== w.contract) {
          err(
            `${svc.name}: "${previo}" y "${w.contract}" dan el mismo durable de estado "${d}"`,
            "Los guiones y los puntos del contrato pasan a `_`. Un durable por contrato: renombra uno de los dos agregados",
          );
        }
        porDurableEstado.set(d, w.contract);
      }
    }

    // ── Peticiones: lo que llama y lo que atiende ─────────────────────────────────────────────────────────────────
    const { calls: llamadas, serves: atendidas } = operaciones.get(svc?.name) ?? { calls: [], serves: [] };
    for (const { op, domain } of llamadas) {
      if (!dueñoDe.has(domain)) {
        err(
          `${svc.name} llama a "${op}" pero nadie declara ser dueño de "${domain}"`,
          "Solo el dueño responde: falta el dueño en el manifiesto, o la operación está mal escrita",
        );
      } else if (Array.isArray(svc.owns) && svc.owns.includes(domain)) {
        err(
          `${svc.name} llama a su propia operación "${op}"`,
          "Una llamada a lo propio es una llamada de función, no una petición por el bus: no hace falta ningún permiso",
        );
      } else if (!atiende.has(op)) {
        err(
          `${svc.name} llama a "${op}", que nadie atiende`,
          `Añádela al \`serves\` de ${dueñoDe.get(domain)} (el dueño de "${domain}"), o quita la llamada: sin quien la atienda, ` +
            "quien llama solo vería `no responders` (RPC_UNAVAILABLE) — 11-request-reply.md §4.3",
        );
      }
    }

    const owns = Array.isArray(svc?.owns) ? [...svc.owns] : [];
    servicios.push({
      name: svc.name,
      via: svc.via === "gateway" ? "gateway" : null,
      owns,
      consumes: [...escritos],
      subjects: [...consumidos].sort(),
      // Dominios que posee y que tienen estado: los que reciben `state.<dom>.>` y su DLQ de estado.
      stateOwns: owns.filter((d) => dominiosConEstado.has(d) && !RESERVED_DOMAINS.includes(d)).sort(),
      watches: observados.sort((a, b) => (a.contract < b.contract ? -1 : 1)),
      // Operaciones: lo que llama y lo que atiende, ordenado; y los dominios de lo que atiende (`rpc.<dominio>.>`).
      calls: llamadas.map((x) => x.op).sort(),
      serves: atendidas.map((x) => x.op).sort(),
      rpcDomains: [...new Set(atendidas.map((x) => x.domain))].sort(),
    });
  }

  return { problemas, servicios, dueñoDe, dominiosConEstado };
}

// ─── Permisos de un servicio ─────────────────────────────────────────────────

/**
 * Lo que un servicio necesita del broker, derivado de lo que el SDK de Node hace de
 * verdad (sdk-node/src/client.ts) y comprobado por conformance/security.mjs (S1, S10 y S11):
 *
 *   connect()            jetstreamManager(nc)                        → $JS.API.INFO
 *   publish()            #ensureStream(dominio)                      → STREAM.INFO.EVT_<DOM>
 *   subscribe()          #ensureStream + #ensureDlqStream            → STREAM.INFO.EVT_/DLQ_<DOM>
 *                        consumers.add(…)  (solo con `dev`)          → CONSUMER.CREATE.<S>.<d>.<subject>
 *                        consumers.info(…)  (fallback y métricas)    → CONSUMER.INFO.<S>.<d>
 *                        consumers.get + consume()                   → CONSUMER.INFO + CONSUMER.MSG.NEXT
 *                        working() / ack() / nak() / term()          → $JS.ACK.<S>.<d>.<…>
 *                        #sendToDlq / #sendRawToDlq                  → publish dlq.<subject>
 *
 * CONSUMER.CREATE NO se concede por defecto (`dev: false`). En producción los durables los
 * crea `flux provision` y al servicio le basta CONSUMER.INFO: el SDK de Node, si el broker le
 * deniega el `add`, cae a `info` y sigue (medido). Concederlo tiene un coste real: un durable
 * borrado lo recrearía el propio servicio con `deliver_policy: all` —reentregando hasta 30 días
 * de eventos sin que nadie lo decida—, y un servicio comprometido podría cambiar el ack_wait, el
 * max_deliver o el backoff de su propio consumidor. Sin CREATE, un durable que falta es un fallo
 * ruidoso (NOT_PROVISIONED), no un replay silencioso. Con `dev: true` se concede, solo sobre el
 * durable PROPIO y con su filtro exacto, para arrancar sin aprovisionar — 06-security.md §3.4.
 *
 * Devuelve:
 *   grupos     [{ titulo, publish: [...] }] — para que la config generada se pueda auditar a ojo
 *   publish    la unión de los grupos, sin repetidos
 *   subscribe  el inbox privado y, si el servicio atiende alguna operación, `rpc.<dominio>.>` de cada dominio
 *   allowResponses  `{ max, expires }` si el servicio atiende alguna operación (el dueño responde con él, sin ningún
 *              permiso de publicación sobre inboxes); `null` si no
 *   durables   [{ stream, durable, filter, kind }] — lo que `flux provision` tiene que crear.
 *              `kind` ∈ `events | dlq | state | dlq-state`; los de `state` llevan además
 *              `contract` y `tenant` (`null` si observan todos)
 *
 * Nunca se emite, y por eso queda denegado por omisión: `CONSUMER.DURABLE.CREATE.*` (el
 * filtro va en el cuerpo y ninguna ACL lo ve), `STREAM.MSG.GET.*`, y todo lo que cree,
 * modifique, borre o purgue streams y consumidores ajenos. Ver 06-security.md §3.
 */
export function permisosDe(servicio, { dev = false } = {}) {
  // Defensa en profundidad: `analizarManifiesto` ya rechaza un dominio reservado, pero esta función
  // se llama también con servicios a mano. El «dueño» de `state` recibiría `state.>`.
  for (const d of servicio.owns) {
    if (RESERVED_DOMAINS.includes(d)) {
      throw new Error(`${servicio.name}: "${d}" es un dominio RESERVADO (${RESERVED_DOMAINS.join(", ")}): nadie puede ser su dueño — 02-naming.md §1.3`);
    }
  }
  const grupos = [];
  const durables = [];
  const visto = new Set();
  const grupo = (titulo, permisos) => {
    const nuevos = permisos.filter((p) => !visto.has(p));
    for (const p of nuevos) visto.add(p);
    if (nuevos.length) grupos.push({ titulo, publish: nuevos });
  };

  grupo("Cuenta JetStream: la consulta connect() del SDK", ["$JS.API.INFO"]);

  for (const d of servicio.owns) {
    grupo(`Dueño de "${d}": publica sus eventos y el SDK consulta su stream`, [
      `${d}.>`,
      `$JS.API.STREAM.INFO.${streamName(d)}`,
    ]);
  }

  const dominiosConsumidos = [...new Set(servicio.subjects.map(domainOf))].sort();
  for (const d of dominiosConsumidos) {
    grupo(`Consume de "${d}": el SDK comprueba que existen su stream y su DLQ`, [
      `$JS.API.STREAM.INFO.${streamName(d)}`,
      `$JS.API.STREAM.INFO.${dlqStreamName(d)}`,
    ]);
  }

  for (const s of servicio.subjects) {
    const stream = streamName(domainOf(s));
    const durable = durableName(servicio.name, s);
    durables.push({ stream, durable, filter: s, kind: "events" });
    grupo(`Consume ${s}  (durable ${durable})`, [
      ...(dev ? [`$JS.API.CONSUMER.CREATE.${stream}.${durable}.${s}`] : []),
      `$JS.API.CONSUMER.INFO.${stream}.${durable}`,
      `$JS.API.CONSUMER.MSG.NEXT.${stream}.${durable}`,
      `$JS.ACK.${stream}.${durable}.>`,
      `dlq.${s}`,
    ]);
  }

  // El dueño lee su propia DLQ con un pull consumer sobre DLQ_<DOM>, no con una
  // suscripción a `dlq.<dominio>.>`: ver 06-security.md §3.1.
  for (const d of servicio.owns) {
    const stream = dlqStreamName(d);
    const durable = dlqDurableName(servicio.name, d);
    const filter = `dlq.${d}.>`;
    durables.push({ stream, durable, filter, kind: "dlq" });
    grupo(`Lee la DLQ de "${d}"  (durable ${durable})`, [
      ...(dev ? [`$JS.API.CONSUMER.CREATE.${stream}.${durable}.${filter}`] : []),
      `$JS.API.CONSUMER.INFO.${stream}.${durable}`,
      `$JS.API.CONSUMER.MSG.NEXT.${stream}.${durable}`,
      `$JS.ACK.${stream}.${durable}.>`,
    ]);
  }

  // ── Estado replicado — 10-state.md §8 y 06-security.md §3.8 ────────────────
  // Medido contra nats-server 2.14.5 (A-38 a A-40, A-48) y ejecutado por la suite de seguridad
  // (S15 en adelante). NINGÚN rol recibe `STREAM.INFO.STATE_<DOM>`: con `subjects_filter` en el
  // cuerpo lista todas las claves y todos los tenants (A-39), y el SDK no lo usa (`watch`, `get` y
  // `put` funcionan sin él). La forma de CUERPO de `DIRECT.GET` (`$JS.API.DIRECT.GET.<stream>` +
  // `{ "seq": n }`) tampoco: lee cualquier mensaje y ninguna ACL ve el cuerpo.
  const estadoDe = servicio.stateOwns ?? [];
  const observados = servicio.watches ?? [];

  // El DUEÑO de un dominio con estado escribe en `state.<dom>.>` (todos sus tenants y claves) y lee
  // lo suyo con DIRECT.GET: el paso 1 de `put` y su `get` (10-state.md §3.2).
  for (const d of estadoDe) {
    grupo(`Dueño del estado de "${d}": escribe state.${d}.> y lee lo suyo (DIRECT.GET)`, [
      `state.${d}.>`,
      `$JS.API.DIRECT.GET.${stateStreamName(d)}.state.${d}.>`,
    ]);
  }

  // …y lee su DLQ de estado con un pull consumer, como la de eventos (10-state.md §6).
  for (const d of estadoDe) {
    const stream = dlqStreamName(d);
    const durable = dlqStateDurableName(servicio.name, d);
    const filter = `dlq.state.${d}.>`;
    durables.push({ stream, durable, filter, kind: "dlq-state" });
    grupo(`Lee la DLQ de estado de "${d}"  (durable ${durable})`, [
      ...(dev ? [`$JS.API.CONSUMER.CREATE.${stream}.${durable}.${filter}`] : []),
      `$JS.API.CONSUMER.INFO.${stream}.${durable}`,
      `$JS.API.CONSUMER.MSG.NEXT.${stream}.${durable}`,
      `$JS.ACK.${stream}.${durable}.>`,
    ]);
  }

  // El OBSERVADOR: los cuatro permisos de su durable (como un evento) y, además, DIRECT.GET por
  // subject con el tenant FIJADO —ese es el confinamiento: el broker deniega `…globex.*` y
  // `…v1.*.*` a quien solo tiene `…acme.*` (A-14, A-38)—. Con varios tenants, un durable y un
  // DIRECT.GET por cada uno. Sin `tenants`, `*` en lugar del tenant: observa todos.
  for (const w of observados) {
    const stream = stateStreamName(w.domain);
    for (const tenant of w.tenants ?? [null]) {
      const durable = stateDurableName(servicio.name, w.contract, tenant);
      const filter = `state.${w.contract}.${tenant ?? "*"}.*`;
      durables.push({ stream, durable, filter, kind: "state", contract: w.contract, tenant });
      grupo(`Observa el estado de ${w.contract}${tenant ? ` (tenant ${tenant})` : " (todos los tenants)"}  (durable ${durable})`, [
        ...(dev ? [`$JS.API.CONSUMER.CREATE.${stream}.${durable}.${filter}`] : []),
        `$JS.API.CONSUMER.INFO.${stream}.${durable}`,
        `$JS.API.CONSUMER.MSG.NEXT.${stream}.${durable}`,
        `$JS.ACK.${stream}.${durable}.>`,
        `$JS.API.DIRECT.GET.${stream}.${filter}`,
      ]);
    }
    // El SDK comprueba la DLQ al abrir (como con un evento) y manda a `dlq.state.<contrato>` lo que
    // no puede aplicar: sin tenant ni clave, cinco tokens (10-state.md §6).
    grupo(`Estado de ${w.contract}: el SDK comprueba su DLQ y le manda lo que no puede aplicar`, [
      `$JS.API.STREAM.INFO.${dlqStreamName(w.domain)}`,
      `dlq.state.${w.contract}`,
    ]);
  }

  // ── Petición/respuesta — 11-request-reply.md §7.1 y 06-security.md §3.9 ───────────────────────────────────
  // QUIEN LLAMA publica en el subject EXACTO de cada operación concedida (A-66) y nada más: ni `rpc.>`, ni
  // `rpc.<dominio>.>`, ni un comodín por token; su inbox privado ya está en `subscribe`. El DUEÑO se suscribe a
  // `rpc.<dominio>.>` (el SDK, al subject exacto de cada operación: A-64) y responde con `allow_responses`
  // acotado, SIN ningún permiso de publicación sobre inboxes (A-16, A-68): lo que se concede no es lo que se usa.
  const llamadas = servicio.calls ?? [];
  const atendidas = servicio.serves ?? [];
  for (const op of [...llamadas, ...atendidas]) {
    const n = normalizarOperacion(op, "calls");
    if (n.error) throw new Error(`${servicio.name}: ${n.error}: ${n.hint}`);
  }
  for (const op of atendidas) {
    if (!servicio.owns.includes(domainOf(op))) {
      throw new Error(`${servicio.name} atiende "${op}" sin ser dueño de "${domainOf(op)}": solo el dueño responde (11-request-reply.md §2.3)`);
    }
  }
  for (const op of llamadas) {
    grupo(`Llama a ${op}: publica en el subject EXACTO de la operación`, [`rpc.${op}`]);
  }
  const dominiosRpc = [...new Set(atendidas.map(domainOf))].sort();

  const publish = grupos.flatMap((g) => g.publish);
  // El inbox es PRIVADO: `_INBOX.>` común haría que cualquiera leyese las entregas de
  // otro (A-2) y las respuestas de las peticiones ajenas (A-71). El SDK fija el prefijo a partir del nombre de
  // servicio — 06-security.md §3. El dueño de una operación añade `rpc.<dominio>.>`, y solo eso.
  const subscribe = [`_INBOX.${servicio.name}.>`, ...dominiosRpc.map((d) => `rpc.${d}.>`)];
  // `allow_responses`: solo quien atiende alguna operación. `null` = ninguno (un servicio que solo llama no responde a nadie).
  const allowResponses = atendidas.length ? { ...RPC_ALLOW_RESPONSES } : null;

  // En NATS `allow: []` NO significa «nada»: significa «sin restricción» (medido: un usuario
  // con listas vacías publica y se suscribe donde quiere). Una lista vacía sería la ACL más
  // abierta posible escrita como la más cerrada, así que se prefiere no generar nada.
  if (publish.length === 0 || subscribe.length === 0) {
    throw new Error(`${servicio.name}: se generaría una lista de permisos vacía, que NATS interpreta como «sin restricción»`);
  }

  return { grupos, publish, subscribe, durables, allowResponses };
}
