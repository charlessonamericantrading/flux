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
  /^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]*\.[a-z0-9]+(-[a-z0-9]+)*$/;
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

// ─── Bundle ──────────────────────────────────────────────────────────────────

/** Los subjects que tienen esquema publicado: la fuente de verdad de «qué existe». */
export async function loadBundleSubjects(path) {
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
  return Object.keys(bundle.subjects).sort();
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
 * Valida el manifiesto contra el modelo y expande cada `consumes`.
 * Devuelve `{ problemas: [{ m, hint }], servicios: [...] }`; `servicios` solo es fiable si
 * no hay problemas.
 */
export function analizarManifiesto(manifiesto, bundleSubjects) {
  const problemas = [];
  const err = (m, hint) => problemas.push({ m, hint });
  const lista = manifiesto?.services;

  if (!Array.isArray(lista) || lista.length === 0) {
    err("el manifiesto no declara ningún servicio", "Falta `services`, o está vacío");
    return { problemas, servicios: [], dueñoDe: new Map() };
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

    for (const campo of ["owns", "consumes"]) {
      if (svc?.[campo] !== undefined && !Array.isArray(svc[campo])) {
        err(`${svc?.name}: \`${campo}\` debe ser una lista`, `Es ${JSON.stringify(svc[campo])}`);
      }
    }

    for (const d of Array.isArray(svc?.owns) ? svc.owns : []) {
      if (!DOMAIN_RE.test(d)) err(`dominio inválido "${d}" en ${svc.name}`, "kebab-case en minúsculas");
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

    servicios.push({
      name: svc.name,
      owns: Array.isArray(svc?.owns) ? [...svc.owns] : [],
      consumes: [...escritos],
      subjects: [...consumidos].sort(),
    });
  }

  return { problemas, servicios, dueñoDe };
}

// ─── Permisos de un servicio ─────────────────────────────────────────────────

/**
 * Lo que un servicio necesita del broker, derivado de lo que el SDK de Node hace de
 * verdad (sdk-node/src/client.ts) y de docs/plan-v1.1/evidencia/candidata.conf:
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
 *   subscribe  SOLO el inbox privado
 *   durables   [{ stream, durable, filter, kind }] — lo que `flux provision` tiene que crear
 *
 * Nunca se emite, y por eso queda denegado por omisión: `CONSUMER.DURABLE.CREATE.*` (el
 * filtro va en el cuerpo y ninguna ACL lo ve), `STREAM.MSG.GET.*`, y todo lo que cree,
 * modifique, borre o purgue streams y consumidores ajenos. Ver 06-security.md §3.
 */
export function permisosDe(servicio, { dev = false } = {}) {
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

  const publish = grupos.flatMap((g) => g.publish);
  // El inbox es PRIVADO: `_INBOX.>` común haría que cualquiera leyese las entregas de
  // otro (A-2). El SDK fija el prefijo a partir del nombre de servicio — 06-security.md §3.
  const subscribe = [`_INBOX.${servicio.name}.>`];

  // En NATS `allow: []` NO significa «nada»: significa «sin restricción» (medido: un usuario
  // con listas vacías publica y se suscribe donde quiere). Una lista vacía sería la ACL más
  // abierta posible escrita como la más cerrada, así que se prefiere no generar nada.
  if (publish.length === 0 || subscribe.length === 0) {
    throw new Error(`${servicio.name}: se generaría una lista de permisos vacía, que NATS interpreta como «sin restricción»`);
  }

  return { grupos, publish, subscribe, durables };
}
