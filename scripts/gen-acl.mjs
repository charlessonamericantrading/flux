#!/usr/bin/env node
/**
 * Genera las ACLs de NATS a partir de un manifiesto de servicios.
 * Contrato normativo: specification/06-security.md §2 y §3
 *
 *   node scripts/gen-acl.mjs <services.json> [--bundle <bundle.json>] [--nkeys <mapa.json>]
 *                            [--check] [--tenants] [--json] [--dev]
 *
 *   (sin flags)   imprime la config de nats-server: un bloque `accounts { … }` que se
 *                 incluye desde la config del servidor con `include "acl.conf"`. El
 *                 listener, TLS y JetStream los pone esa config, no este generador.
 *   --bundle      índice de subjects con esquema. Por defecto, `schemas/bundle.json` de
 *                 este repo (no el del directorio actual: el resultado no debe depender
 *                 de desde dónde se lanza).
 *   --nkeys       mapa `{ "<servicio>": "<clave PÚBLICA de usuario>" }`. Sin él, cada
 *                 usuario lleva el placeholder `<NKEY_DE_…>`. Con él, TODO servicio debe
 *                 tener clave, y se rechazan las semillas (son secretos) y las claves
 *                 repetidas (credencial compartida — 06-security.md §2).
 *   --check       valida el manifiesto (y el mapa de claves, si se da) y no genera nada.
 *   --tenants     Modelo B: una account por tenant (09-multitenancy.md §2). Las claves de
 *                 `--nkeys` son `<servicio>@<tenant>`: NATS rechaza la misma nkey en dos
 *                 accounts ("Duplicate nkey").
 *   --dev         concede además CONSUMER.CREATE de cada durable PROPIO (con su filtro
 *                 exacto), para que el SDK pueda crearlo sin `flux provision`. NO es el
 *                 default: ver más abajo y 06-security.md §3.4.
 *   --json        en vez de la config, `[ { name, publish, subscribe, … } ]`, un elemento
 *                 por servicio (por servicio y tenant con --tenants), para que los tests
 *                 asserten los permisos exactos sin parsear la config. Además de los tres
 *                 campos contractuales trae `owns`, `consumes` (ya EXPANDIDO a subjects
 *                 exactos) y `durables` (lo que `flux provision` crea).
 *
 * §3 dice: "un servicio solo publica en el dominio del que es dueño; solo se suscribe
 * a lo que se le ha concedido explícitamente". Eso es una regla derivable: si sabes
 * quién es dueño de cada dominio y qué consume cada servicio, las ACLs se calculan.
 * Escribirlas a mano garantiza que diverjan del diseño en el primer mes.
 *
 * Qué genera para cada servicio (el detalle y el porqué, en 06-security.md §3):
 *   · publish:   su dominio; y, por cada subject consumido, EXACTAMENTE los tres
 *                subjects de `$JS.API`/`$JS.ACK` de su durable (INFO, MSG.NEXT y ACK;
 *                cuatro con --dev, que añade CREATE), más `dlq.<subject>`.
 *   · subscribe: su inbox privado `_INBOX.<servicio>.>`. Ningún subject de dominio:
 *                los SDKs consumen por pull, y lo que confina es `$JS.API`. La ÚNICA excepción es el
 *                dueño de una operación de petición/respuesta, que añade `rpc.<dominio>.>` (abajo).
 *   · estado:    el dueño, `state.<dom>.>` y DIRECT.GET de su dominio; el observador, los
 *                permisos de su durable de estado y DIRECT.GET con el tenant fijado. Ninguno
 *                recibe `STREAM.INFO.STATE_<DOM>` (lista claves y tenants: A-39).
 *   · peticiones: quien llama (`calls`) publica SOLO en el subject exacto de cada operación (`rpc.<operación>`);
 *                el dueño (`serves`) se suscribe a `rpc.<dominio>.>` y responde con
 *                `allow_responses: { max: 1, expires: 30s }`, sin ningún permiso de publicación sobre inboxes.
 *                Nunca `rpc.>`, `rpc.<dominio>.>` ni un comodín por token a quien llama.
 *   · nunca:     `CONSUMER.DURABLE.CREATE.*`, `STREAM.MSG.GET.*`, la forma de cuerpo de
 *                DIRECT.GET, ni crear, modificar, borrar o purgar streams. Se deniegan por omisión.
 *   · CONSUMER.CREATE no se concede por defecto: en producción los durables los crea
 *                `flux provision`. Un durable borrado lo recrearía el propio servicio con
 *                deliver_policy `all`, reentregando hasta 30 días de eventos sin que nadie lo
 *                decida; sin el permiso falla ruidosamente (NOT_PROVISIONED). `--dev` lo
 *                concede, solo sobre el durable propio y con su filtro exacto.
 *
 * v1 de este generador concedía `_INBOX.>` y subscripción a los subjects consumidos, y
 * nada sobre `$JS.API`: con eso un servicio no podía consumir (H1) y, aun así, leía
 * dominios ajenos por el inbox común (H2). Queda congelada en
 * conformance/fixtures/acl-v1.conf como regresión.
 *
 * Manifiesto (services.json). `consumes` es un subject exacto o un patrón acabado en
 * `>`, que se expande contra el bundle:
 *   {
 *     "environment": "produccion",
 *     "services": [
 *       { "name": "pedidos-api",     "owns": ["pedidos"],
 *         "consumes": ["inventario.stock.v1.>"] },
 *       { "name": "facturacion-api", "owns": ["facturacion"],
 *         "consumes": ["pedidos.pedido.v1.creado"] },
 *       { "name": "crm-api",         "owns": ["clientes"] },
 *       { "name": "tienda-web",      "watches": [
 *           { "contract": "clientes.cliente.v1", "tenants": ["acme"] } ] },
 *       { "name": "informes-api",    "watches": ["clientes.cliente.v1"] }
 *     ]
 *   }
 *   (en el ejemplo real, `crm-api` también lleva `serves` y `pedidos-api` y `facturacion-api`, `calls`)
 *
 * Estado replicado (specification/10-state.md; permisos en 06-security.md §3.8):
 *   · DUEÑO. No hace falta declarar nada más que `owns`: un dominio TIENE estado si el bundle
 *     tiene algún contrato suyo (`schemas/state/<dominio>/…`), y entonces su dueño recibe
 *     `state.<dom>.>` y `DIRECT.GET` de su dominio, más los permisos de su durable de DLQ de estado.
 *   · OBSERVADOR. `watches` es una lista de contratos de estado (`<dominio>.<agregado>.v<major>`,
 *     sin evento: `clientes.cliente.v1`). Cada entrada es el contrato a secas —observa TODOS los
 *     tenants— o `{ "contract": …, "tenants": ["acme"] }`, que CONFINA al servicio a esos tenants:
 *     el filtro de su durable y su DIRECT.GET llevan el tenant, y el que lo aplica es el broker.
 *     `tenants` nunca puede ser una lista vacía (sería «ninguno» escrito como «todos»).
 *   · Un servicio no observa su propio dominio, y el estado NO va en `consumes`.
 *   · Una clave desconocida en un servicio (`watch` por `watches`) es un error: una errata no puede
 *     ser «un servicio sin permisos».
 *   · Los dominios `dlq`, `state`, `rpc` y `system` no pueden ser de nadie (RESERVED_DOMAIN): el
 *     «dueño» de `state` recibiría `state.>` y escribiría el estado de todos los dominios.
 *
 * Petición/respuesta (specification/11-request-reply.md; permisos en 06-security.md §3.9):
 *   · `calls` y `serves` son listas de OPERACIONES, `<dominio>.<agregado>.v<major>.<operacion>` —sin el prefijo
 *     `rpc.`, que pone el SDK—, y cada una debe tener su par request/reply en el bundle (índice `rpc`).
 *   · QUIEN LLAMA (`calls`): publicar en `rpc.<operación>`, el subject EXACTO, y nada más. Ni `rpc.>`, ni
 *     `rpc.<dominio>.>`, ni un comodín por token: podría llamar a todo. Su inbox privado ya es el de siempre.
 *   · EL DUEÑO (`serves`): suscribirse a `rpc.<dominio>.>` (el SDK se suscribe al subject exacto de cada
 *     operación: A-64) y `allow_responses: { max: 1, expires: 30s }` (A-68). NINGÚN permiso de publicación sobre
 *     inboxes: lo da `allow_responses`, y solo para el `reply` de la petición que recibió.
 *   · Solo el dueño de un dominio atiende sus operaciones; una operación llamada debe estar atendida; un servicio
 *     no se llama a sí mismo; una operación con dominio reservado (`rpc.…`, `state.…`) o con comodín se rechaza.
 *   · Un servicio que solo llama NO recibe ninguna suscripción de dominio ni `allow_responses`.
 */

import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  DOMAIN_RE,
  NKEY_USER_RE,
  analizarManifiesto,
  loadBundle,
  permisosDe,
} from "./lib/manifest.mjs";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

const USO =
  "uso: node scripts/gen-acl.mjs <services.json> [--bundle <bundle.json>] [--nkeys <mapa.json>]\n" +
  "                              [--check] [--tenants] [--json] [--dev]";

// ─── Argumentos ──────────────────────────────────────────────────────────────

function leerArgumentos(argv) {
  const o = { manifiesto: undefined, bundle: join(ROOT, "schemas", "bundle.json") };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--check") o.check = true;
    else if (a === "--tenants") o.tenants = true;
    else if (a === "--json") o.json = true;
    else if (a === "--dev") o.dev = true;
    else if (a === "--bundle" || a === "--nkeys") {
      const v = argv[++i];
      if (v === undefined || v.startsWith("--")) usoErroneo(`${a} necesita un valor`);
      o[a.slice(2)] = v;
    } else if (a.startsWith("--")) usoErroneo(`opción desconocida: ${a}`);
    else if (o.manifiesto === undefined) o.manifiesto = a;
    else usoErroneo(`argumento sobrante: ${a}`);
  }
  if (!o.manifiesto) usoErroneo("falta el manifiesto");
  return o;
}

function usoErroneo(mensaje) {
  console.error(`${mensaje}\n${USO}`);
  process.exit(2);
}

async function leerJson(ruta, que) {
  try {
    return JSON.parse(await readFile(ruta, "utf8"));
  } catch (e) {
    console.error(`✗ no se pudo leer ${que} "${ruta}": ${e.message}`);
    process.exit(2);
  }
}

function informar(titulo, problemas) {
  console.error(`\n✗ ${problemas.length} problema(s) ${titulo}\n`);
  for (const p of problemas) {
    console.error(`  ✗ ${p.m}`);
    if (p.hint) console.error(`    ${p.hint}`);
  }
  process.exit(1);
}

// ─── Principal ───────────────────────────────────────────────────────────────

const args = leerArgumentos(process.argv.slice(2));
const manifiesto = await leerJson(args.manifiesto, "el manifiesto");

let bundle;
try {
  bundle = await loadBundle(args.bundle);
} catch (e) {
  console.error(`✗ ${e.message}`);
  process.exit(2);
}

const { problemas, servicios, dueñoDe } = analizarManifiesto(manifiesto, bundle);
if (problemas.length) informar("en el manifiesto", problemas);

const env = manifiesto.environment ?? "produccion";
const up = (x) => x.toUpperCase().replace(/-/g, "_");

// Modelo B: la lista de tenants.
const tenants = args.tenants ? (manifiesto.tenants ?? []) : [];
if (args.tenants) {
  if (tenants.length === 0) {
    console.error(`\n✗ --tenants requiere una lista \`tenants\` en el manifiesto.\n  Ej.: "tenants": ["acme", "globex"]\n`);
    process.exit(1);
  }
  const malos = tenants.filter((t) => !DOMAIN_RE.test(t));
  if (malos.length) {
    console.error(`\n✗ tenant inválido "${malos[0]}": kebab-case en minúsculas\n`);
    process.exit(1);
  }
}

// Identidades que necesitan clave: una por servicio, o una por servicio y tenant.
const identidades = args.tenants
  ? tenants.flatMap((t) => servicios.map((s) => `${s.name}@${t}`))
  : servicios.map((s) => s.name);

const claves = args.nkeys ? await cargarClaves(args.nkeys, identidades) : null;

/** Valida el mapa `--nkeys` y lo devuelve. Todo fallo es del mapa, no del manifiesto. */
async function cargarClaves(ruta, necesarias) {
  const mapa = await leerJson(ruta, "el mapa de claves");
  const p = [];
  if (!mapa || typeof mapa !== "object" || Array.isArray(mapa)) {
    informar("en --nkeys", [{ m: "el mapa de claves debe ser un objeto { \"<servicio>\": \"U…\" }" }]);
  }

  for (const id of necesarias) {
    if (mapa[id] === undefined) {
      p.push({
        m: `falta la clave de "${id}"`,
        hint: args.tenants
          ? "Con --tenants las claves son `<servicio>@<tenant>`: NATS no admite la misma nkey en dos accounts"
          : "Con --nkeys todo servicio debe tener clave pública. Sin el mapa se emite el placeholder <NKEY_DE_…>",
      });
    }
  }
  const dueño = new Map();
  for (const [id, k] of Object.entries(mapa)) {
    if (!necesarias.includes(id)) {
      console.error(`aviso: --nkeys tiene una clave para "${id}", que no es ninguna identidad del manifiesto`);
      continue;
    }
    if (typeof k !== "string" || !NKEY_USER_RE.test(k)) {
      p.push({
        m: `la clave de "${id}" no es una clave pública de usuario de NATS`,
        hint:
          typeof k === "string" && k.startsWith("S")
            ? "Empieza por `S`: es una SEMILLA, un secreto. Nunca va a una config: aquí solo la clave pública (`U…`, 56 caracteres)"
            : "Una clave pública de usuario empieza por `U` y tiene 56 caracteres (nsc / @nats-io/nkeys: getPublicKey())",
      });
      continue;
    }
    if (dueño.has(k)) {
      p.push({
        m: `"${id}" y "${dueño.get(k)}" comparten la misma clave`,
        hint: "Una credencial compartida entre servicios hace imposible saber quién publicó y rotar sin coordinación global — 06-security.md §2",
      });
    }
    dueño.set(k, id);
  }
  if (p.length) informar("en --nkeys", p);
  return mapa;
}

if (args.check) {
  const durables = servicios.reduce((n, s) => n + permisosDe(s, { dev: !!args.dev }).durables.length, 0);
  const conEstado = new Set(servicios.flatMap((s) => s.stateOwns));
  const observaciones = servicios.reduce((n, s) => n + s.watches.length, 0);
  const llamadas = servicios.reduce((n, s) => n + s.calls.length, 0);
  const atendidas = servicios.reduce((n, s) => n + s.serves.length, 0);
  console.log(
    `✓ manifiesto válido: ${servicios.length} servicio(s), ${dueñoDe.size} dominio(s), ` +
      `${durables} durable(s) a aprovisionar` +
      (conEstado.size ? `, ${conEstado.size} dominio(s) con estado y ${observaciones} observación(es) de estado` : "") +
      (llamadas + atendidas ? `, ${atendidas} operación(es) atendida(s) y ${llamadas} llamada(s) entre servicios` : "") +
      (args.tenants ? `, ${tenants.length} tenant(s)` : "") +
      (claves ? `, ${identidades.length} clave(s) públicas` : ""),
  );
  process.exit(0);
}

// ─── Salida ──────────────────────────────────────────────────────────────────

if (args.json) {
  const filas = [];
  for (const t of args.tenants ? tenants : [null]) {
    for (const s of servicios) {
      const perm = permisosDe(s, { dev: !!args.dev });
      filas.push({
        name: s.name,
        ...(t && { tenant: t, account: `${env.toUpperCase()}_${up(t)}` }),
        publish: perm.publish,
        subscribe: perm.subscribe,
        owns: s.owns,
        consumes: s.subjects,
        stateOwns: s.stateOwns,
        watches: s.watches,
        calls: s.calls,
        serves: s.serves,
        allowResponses: perm.allowResponses,
        durables: perm.durables,
      });
    }
  }
  console.log(JSON.stringify(filas, null, 2));
  process.exit(0);
}

console.log(args.tenants ? renderTenantAccounts() : renderAccount());

// ─── Render de la config de nats-server ──────────────────────────────────────

/** La línea `nkey:` de un usuario: la clave pública del mapa, o el placeholder. */
function lineaNkey(id, placeholder) {
  return `        nkey: "${claves ? claves[id] : `<NKEY_DE_${placeholder}>`}"`;
}

/** Un usuario: sus comentarios, su nkey y sus permisos, con cada grupo comentado. */
function renderUsuario(svc, { id, placeholder, titulo }) {
  const perm = permisosDe(svc, { dev: !!args.dev });
  const l = [];
  l.push(``);
  l.push(`      # ${titulo}`);
  if (svc.owns.length) l.push(`      #   dueño de: ${svc.owns.join(", ")}`);
  if (svc.consumes.length) {
    l.push(`      #   consume: ${svc.consumes.join(", ")}`);
    if (svc.consumes.join(",") !== svc.subjects.join(",")) {
      l.push(`      #     → ${svc.subjects.join(", ")}`);
    }
  }
  if (svc.stateOwns.length) l.push(`      #   estado de: ${svc.stateOwns.join(", ")} (escribe state.<dominio>.>)`);
  if (svc.serves.length) l.push(`      #   atiende: ${svc.serves.join(", ")} (rpc.${svc.rpcDomains.join(".>, rpc.")}.> y allow_responses)`);
  if (svc.calls.length) l.push(`      #   llama a: ${svc.calls.join(", ")} (publica en el subject exacto de cada una)`);
  if (svc.watches.length) {
    l.push(
      `      #   observa estado: ` +
        svc.watches.map((w) => `${w.contract} (${w.tenants ? `tenant ${w.tenants.join(", ")}` : "todos los tenants"})`).join("; "),
    );
  }
  l.push(`      {`);
  l.push(lineaNkey(id, placeholder));
  l.push(`        permissions: {`);
  l.push(`          publish: { allow: [`);
  perm.grupos.forEach((g, i) => {
    if (i > 0) l.push(``);
    l.push(`            # ${g.titulo}`);
    for (const s of g.publish) l.push(`            "${s}",`);
  });
  l.push(`          ] }`);
  l.push(`          subscribe: { allow: [${perm.subscribe.map((s) => `"${s}"`).join(", ")}] }`);
  // El dueño de una operación responde con `allow_responses`, SIN permiso de publicación sobre inboxes (A-16, A-68):
  // una respuesta por petición, y que caduque. Medido: `true` admite una y NO caduca.
  if (perm.allowResponses) {
    l.push(`          allow_responses: { max: ${perm.allowResponses.max}, expires: "${perm.allowResponses.expires}" }`);
  }
  l.push(`        }`);
  l.push(`      },`);
  return l;
}

function cabeceraComun() {
  return [
    `# Reglas aplicadas (specification/06-security.md §3):`,
    `#   · Un servicio solo publica en el dominio del que es dueño.`,
    `#   · NINGÚN permiso de suscripción sobre subjects de dominio. Los SDKs consumen por`,
    `#     pull: lo que confina es $JS.API, y ahí cada servicio recibe los permisos EXACTOS`,
    `#     de sus durables, ni uno más. Los permisos de subject no gobiernan JetStream. La única`,
    `#     suscripción que no es el inbox es rpc.<dominio>.> del dueño de una operación (abajo).`,
    `#   · Inbox PRIVADO por servicio, _INBOX.<servicio>.>. Un _INBOX.> común permite a`,
    `#     cualquiera leer las entregas de otro: las de un pull consumer viajan por el inbox.`,
    `#   · Nunca se concede CONSUMER.DURABLE.CREATE.*, STREAM.MSG.GET.*, la forma de cuerpo de`,
    `#     DIRECT.GET, STREAM.INFO.STATE_<DOM> ni crear, modificar, borrar o purgar streams. Se`,
    `#     deniegan por omisión.`,
    `#   · Estado (06-security.md §3.8): el dueño escribe state.<dom>.> y lee lo suyo; el observador`,
    `#     lee con DIRECT.GET por subject CON EL TENANT FIJADO si el manifiesto lo confina.`,
    `#   · Peticiones (06-security.md §3.9): quien llama publica en el subject EXACTO de cada operación,`,
    `#     nunca rpc.> ni rpc.<dominio>.>; el dueño se suscribe a rpc.<dominio>.> y responde con`,
    `#     allow_responses { max: 1, expires: 30s }, sin ningún permiso de publicación sobre inboxes.`,
    ...(args.dev
      ? [
          `#   · MODO --dev: se concede además CONSUMER.CREATE de cada durable PROPIO, con su filtro`,
          `#     exacto, para arrancar sin \`flux provision\`. NO usar en producción.`,
        ]
      : [
          `#   · CONSUMER.CREATE no se concede (producción): los durables los crea \`flux provision\`.`,
          `#     Un durable borrado falla en alto (NOT_PROVISIONED) en vez de recrearse con`,
          `#     deliver_policy all y reentregar el historial. \`--dev\` lo concede.`,
        ]),
    `#`,
    `# Los streams y los durables que estos permisos presuponen NO los crea el servicio:`,
    `# se aprovisionan con \`flux provision\`, con credenciales de operación que no están en`,
    `# este fichero. El usuario de operación tampoco se genera aquí.`,
    `#`,
    `# Es un bloque \`accounts { … }\` incluible desde la config del servidor, que pone el`,
    `# listener, TLS (obligatorio, sin allow_non_tls) y JetStream:`,
    `#     include "acl.conf"`,
  ];
}

function renderAccount() {
  const out = [];
  out.push(`# GENERADO por scripts/gen-acl.mjs — no editar a mano.`);
  out.push(`# Entorno: ${env}`);
  out.push(`# Fuente:  ${args.manifiesto}`);
  out.push(`#`);
  out.push(...cabeceraComun());
  if (!claves) {
    out.push(`#`);
    out.push(`# Las credenciales NUNCA se versionan (06-security.md §2): cada <NKEY_DE_*> se`);
    out.push(`# sustituye por la clave PÚBLICA del servicio (--nkeys <mapa.json>) al desplegar.`);
  }
  out.push(``);
  out.push(`accounts {`);
  out.push(`  ${env.toUpperCase()}: {`);
  out.push(`    jetstream: enabled`);
  out.push(`    users: [`);
  for (const svc of servicios) {
    out.push(
      ...renderUsuario(svc, { id: svc.name, placeholder: up(svc.name), titulo: svc.name }),
    );
  }
  out.push(`    ]`);
  out.push(`  }`);
  out.push(`}`);
  return out.join("\n");
}

/** Modelo B — aislamiento duro por account de tenant. 09-multitenancy.md §2. */
function renderTenantAccounts() {
  const ENV = env.toUpperCase();
  const out = [];
  const w = (x = "") => out.push(x);

  w(`# GENERADO por scripts/gen-acl.mjs --tenants — no editar a mano.`);
  w(`# Modelo B de 09-multitenancy.md §2: una account de NATS POR TENANT.`);
  w(`# Fuente:  ${args.manifiesto}`);
  w(`#`);
  for (const x of cabeceraComun()) w(x);
  w(`#`);
  w(`# Los permisos de cada servicio son LOS MISMOS que en el Modelo A: lo que cambia es que`);
  w(`# cada tenant es una account distinta, con sus propios streams y durables. Hay que`);
  w(`# ejecutar \`flux provision\` UNA VEZ POR ACCOUNT, con las credenciales de operación de`);
  w(`# cada una.`);
  w(`#`);
  w(`# Las accounts de NATS son un límite de aislamiento REAL: los subjects de una son`);
  w(`# invisibles desde otra salvo export/import explícito. Un servicio con credenciales`);
  w(`# de un tenant no puede leer los datos de otro aunque su código lo intente.`);
  w(`#`);
  w(`# COSTE, para que nadie lo descubra en producción:`);
  w(`#   · ${tenants.length} tenant(s) × los streams de cada dominio = ${tenants.length}× streams.`);
  w(`#     JetStream tiene límites de recursos por servidor.`);
  w(`#   · Un servicio que atiende a N tenants necesita N CONEXIONES, una por account, y N`);
  w(`#     claves: NATS rechaza la misma nkey en dos accounts ("Duplicate nkey").`);
  w(`#   · Los eventos de plataforma (tenantid "system") requieren export/import entre`);
  w(`#     accounts: fontanería que hay que mantener y que este generador no cubre.`);
  w(`#`);
  w(`# Si esto parece caro, probablemente el Modelo A (filtrado en consumidor + firma)`);
  w(`# baste para vuestro modelo de amenaza. Ver 09-multitenancy.md §1.`);
  w();
  w(`accounts {`);

  for (const t of tenants) {
    w();
    w(`  # ─── tenant: ${t} ───`);
    w(`  ${ENV}_${up(t)}: {`);
    w(`    jetstream: enabled`);
    w(`    users: [`);
    for (const svc of servicios) {
      out.push(
        ...renderUsuario(svc, {
          id: `${svc.name}@${t}`,
          placeholder: `${up(svc.name)}_${up(t)}`,
          titulo: `${svc.name} @ ${t}`,
        }),
      );
    }
    w(`    ]`);
    w(`  }`);
  }

  w(`}`);
  w();
  w(`# Los subjects son IDÉNTICOS en todas las accounts, y eso es correcto: cada account`);
  w(`# es un universo aparte, así que "pedidos.pedido.v1.creado" de acme y el de globex`);
  w(`# no colisionan. El aislamiento lo da la account, no el nombre.`);

  return out.join("\n");
}
