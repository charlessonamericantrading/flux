#!/usr/bin/env node
/**
 * flux — CLI operativa del Event Protocol v1.
 *
 * No depende del SDK de flux, igual que la suite de conformidad: una herramienta
 * construida sobre el SDK no puede diagnosticar los fallos del propio SDK.
 */

import { readFile } from "node:fs/promises";

import { connect, credsAuthenticator } from "@nats-io/transport-node";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";

import { c, die, OK } from "./ui.mjs";
import { doctor } from "./doctor.mjs";
import { dlqList, dlqInspect, dlqReplay } from "./dlq.mjs";
import { tail } from "./tail.mjs";
import { comandoProvision } from "./provision.mjs";
import { comandoState } from "./state.mjs";

const SUBJECT_RE =
  /^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]*\.[a-z0-9]+(-[a-z0-9]+)*$/;

const HELP = `${c.bold("flux")} — CLI operativa del Event Protocol v1

${c.bold("USO")}
  flux <comando> [opciones]

${c.bold("COMANDOS")}
  ${c.cyan("doctor")}                    Audita streams y consumidores contra el protocolo
  ${c.cyan("tail")} <patrón>             Eventos en vivo (consumidor efímero, no consume)
  ${c.cyan("dlq ls")} <dominio>          Resumen de la DLQ agrupado por causa
  ${c.cyan("dlq inspect")} <dominio>     Detalle de eventos muertos
  ${c.cyan("dlq replay")} <dominio>      Reproduce desde la DLQ (simulación por defecto; no el estado)
  ${c.cyan("state ls")} <contrato>       Claves de un estado replicado: revisión, tamaño, lápidas
  ${c.cyan("state get")} <contrato> <tenant> <clave>
                              Una clave: el sobre tal cual está en el stream
  ${c.cyan("state resync")} <servicio> <contrato>
                              Recupera un observador de estado: borra y recrea su durable
                              (simulación por defecto; nunca toca el stream ni las claves)
  ${c.cyan("validate")} <subject>        Comprueba un subject contra el protocolo
  ${c.cyan("provision")} <services.json> Crea los streams y durables que exige el manifiesto
                              (idempotente, informa de diferencias, nunca borra)
  ${c.cyan("keygen")} <servicio> <n>     Genera un par Ed25519 para firmar eventos

${c.bold("OPCIONES GLOBALES")}
  -s, --server <url>        nats://127.0.0.1:4222  (o \$NATS_URL)
      --creds <fichero>     Credenciales NATS (.creds)
  -v, --verbose             Más detalle
  -h, --help

${c.bold("OPCIONES DE state")}
      --tenant <t>          ls: solo ese tenant · resync: el durable de ese tenant
      --json                ls, get: salida para máquinas
      --show-data           get (y tail, dlq inspect): imprime el data aunque sea confidential
      --limit <n>           ls: solo las n primeras claves
      --confirm             resync: ejecuta (sin él, simulación)
      --yes                 resync: no pide la confirmación interactiva

${c.bold("OPCIONES DE provision")}
      --dry-run             Muestra qué se crearía; no crea nada
      --replicas <n>        Réplicas de los streams (por defecto 1; producción: 3)
      --bundle <fichero>    Índice de subjects con esquema (por defecto schemas/bundle.json)

${c.bold("EJEMPLOS")}
  flux doctor
  flux tail 'pedidos.>' --since 5m
  flux tail 'pedidos.pedido.v1.creado' --full
  flux dlq ls pedidos
  flux dlq inspect pedidos --subject pedidos.pedido.v1.creado
  flux dlq replay pedidos --subject pedidos.pedido.v1.creado --confirm
  flux state ls clientes.cliente.v1 --tenant acme
  flux state get clientes.cliente.v1 acme cli-1
  flux state resync tienda-web clientes.cliente.v1 --tenant acme --confirm
  flux provision examples/services.json --dry-run --creds ops.creds
  flux provision examples/services.json --replicas 3 --creds ops.creds

${c.bold("PROVISION")}
  Usa credenciales de OPERACIÓN, no las de un servicio: un servicio no puede crear streams
  ni consumidores (06-security.md §3). Con el Modelo B (gen-acl.mjs --tenants) hay una
  account por tenant: se ejecuta una vez por account, con las credenciales de operación
  de cada una.

${c.bold("SEGURIDAD")}
  El payload de eventos con dataclassification confidential o restricted NO se
  imprime: una terminal acaba en un log. Usa --show-data si de verdad lo necesitas.
  Vale también para el estado (confidential es la clasificación por defecto de un put)
  y para --json y --full.
`;

function parseArgs(argv) {
  const opts = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "-h" || a === "--help") opts.help = true;
    else if (a === "-v" || a === "--verbose") opts.verbose = true;
    else if (a === "-s" || a === "--server") opts.server = argv[++i];
    else if (a === "--creds") opts.creds = argv[++i];
    else if (a === "--dry-run") opts.dryRun = true;
    else if (a === "--replicas") opts.replicas = Number(argv[++i]);
    else if (a === "--bundle") opts.bundle = argv[++i];
    else if (a === "--since") opts.since = argv[++i];
    else if (a === "--subject") opts.subject = argv[++i];
    else if (a === "--limit") opts.limit = Number(argv[++i]);
    else if (a === "--tenant") opts.tenant = argv[++i];
    else if (a === "--full") opts.full = true;
    else if (a === "--json") opts.jsonOut = true;
    else if (a === "--show-data") opts.showData = true;
    else if (a === "--confirm") opts.confirm = true;
    else if (a === "--yes") opts.yes = true;
    else if (a.startsWith("-")) die(`opción desconocida: ${a}`);
    else opts._.push(a);
  }
  return opts;
}

/** `validate` no necesita broker: es análisis puro del nombre. */
function validate(subject) {
  const problemas = [];
  if (subject !== subject.toLowerCase()) {
    problemas.push([
      "mayúsculas",
      "NATS es case-sensitive: crea un subject fantasma al que nadie está suscrito, y no produce ningún error",
    ]);
  }
  const tokens = subject.split(".");
  if (tokens.length !== 4) {
    problemas.push([
      `${tokens.length} tokens`,
      "el formato es <dominio>.<agregado>.v<major>.<evento>, exactamente 4",
    ]);
  }
  if (tokens.includes("")) problemas.push(["token vacío", "hay dos puntos seguidos"]);
  if (/_/.test(subject)) problemas.push(["guion bajo", "solo kebab-case: [a-z0-9-]"]);
  if (tokens[2] && !/^v[1-9][0-9]*$/.test(tokens[2])) {
    problemas.push([
      `versión "${tokens[2]}"`,
      "el 3.er token es la versión mayor: v1, v2, … (v0 no es válido)",
    ]);
  }
  if (tokens[3] && /^(crear|actualizar|borrar|enviar|procesar)/.test(tokens[3])) {
    problemas.push([
      "verbo en imperativo",
      "los eventos van en pasado: 'creado', no 'crear'. Un imperativo es un comando, y flux v1 no los cubre",
    ]);
  }
  if (tokens[3] === "actualizado" || tokens[3] === "cambiado" || tokens[3] === "modificado") {
    problemas.push([
      `"${tokens[3]}" no dice QUÉ cambió`,
      "obliga a cada consumidor a implementar —y equivocar— su propio diff. Nombra el hecho: 'direccion-envio-cambiada'",
    ]);
  }

  if (!problemas.length && SUBJECT_RE.test(subject)) {
    const [dominio, agregado, v, evento] = tokens;
    const major = v.slice(1);
    console.log(`${OK} ${c.bold(subject)}`);
    console.log(`  type       com.flux.${dominio}.${agregado}.${evento}.v${major}`);
    console.log(`  stream     EVT_${dominio.replace(/-/g, "_").toUpperCase()}`);
    console.log(`  dlq        dlq.${subject}`);
    console.log(`  durable    <servicio>__${subject.replace(/[.-]/g, "_")}`);
    console.log(`  schema     schemas/${dominio}/${agregado}/${evento}/${major}.0.0.json`);
    return 0;
  }

  console.log(`${c.red("✗")} ${c.bold(subject)}`);
  for (const [q, por] of problemas) {
    console.log(`  ${c.red("•")} ${q}`);
    console.log(`    ${c.gray(por)}`);
  }
  if (!problemas.length) console.log(`  ${c.red("•")} no encaja con el patrón del protocolo`);
  console.log(c.gray("\n  Ver specification/02-naming.md"));
  return 1;
}

// ─── main ────────────────────────────────────────────────────────────────────

const opts = parseArgs(process.argv.slice(2));
const [cmd, sub, ...rest] = opts._;

if (opts.help || !cmd) {
  console.log(HELP);
  process.exit(0);
}

if (cmd === "keygen") {
  const servicio = sub;
  const n = opts._[2] ?? "1";
  if (!servicio) die("uso: flux keygen <servicio> [n]   p.ej. flux keygen pedidos-api 1");
  if (!/^[a-z0-9]+(-[a-z0-9]+)*$/.test(servicio)) {
    die(`nombre de servicio inválido: "${servicio}". kebab-case en minúsculas.`);
  }
  const { generateKeyPairSync } = await import("node:crypto");
  const { privateKey, publicKey } = generateKeyPairSync("ed25519");
  const keyId = `${servicio}-${n}`;

  console.log(`${c.bold("signkeyid:")} ${keyId}
`);
  console.log(c.bold("Clave PÚBLICA") + c.gray("  — se distribuye a los consumidores y se VERSIONA"));
  console.log(publicKey.export({ type: "spki", format: "pem" }).toString());
  console.log(c.bold(c.red("Clave PRIVADA")) + c.gray("  — al gestor de secretos. NUNCA a un repositorio"));
  console.log(privateKey.export({ type: "pkcs8", format: "pem" }).toString());
  console.log(
    c.gray(
      `Al rotar, incrementa el número: ${servicio}-${Number(n) + 1}.
` +
        `CONSERVA la pública antigua mientras existan eventos firmados con ella —
` +
        `mínimo 90 días, la retención de la DLQ. Retirar una clave impide EMITIR con
` +
        `ella, no VERIFICAR lo ya emitido (07-signing.md §6).`,
    ),
  );
  process.exit(0);
}

if (cmd === "validate") {
  if (!sub) die("uso: flux validate <subject>");
  process.exit(validate(sub));
}

if (cmd === "provision") {
  if (!sub) die("uso: flux provision <services.json> [--dry-run] [--replicas n] [--bundle fichero]");
  opts.replicas ??= 1;
  if (!Number.isInteger(opts.replicas) || opts.replicas < 1 || opts.replicas > 5) {
    die(`--replicas debe ser un entero de 1 a 5, no "${opts.replicas}"`);
  }
}

// `--creds` se pasaba antes a una opción `creds` que el cliente NATS ignora: la conexión se
// abría SIN credenciales y el servidor respondía «Authorization Violation», sin mencionar
// que el fichero nunca se leyó. Un .creds se entrega con su autenticador.
let authenticator;
if (opts.creds) {
  try {
    authenticator = credsAuthenticator(new TextEncoder().encode(await readFile(opts.creds, "utf8")));
  } catch (e) {
    die(`no se pudo leer el fichero de credenciales "${opts.creds}": ${e.message}`, 2);
  }
}

const url = opts.server ?? process.env.NATS_URL ?? "nats://127.0.0.1:4222";
let nc;
try {
  nc = await connect({
    servers: url,
    name: "flux-cli",
    ...(authenticator && { authenticator }),
  });
} catch (e) {
  die(
    `no se pudo conectar a ${url}\n  ${e.message}\n\n` +
      `  Levanta un NATS con JetStream:  ${c.cyan("docker compose up -d")}\n` +
      `  O apunta a otro servidor:       ${c.cyan("flux <cmd> --server nats://host:4222")}`,
    2,
  );
}

const jsm = await jetstreamManager(nc);
const js = jetstream(nc);

try {
  switch (cmd) {
    case "doctor": {
      const r = await doctor(jsm, { verbose: opts.verbose });
      const errores = r.findings.filter((f) => f.level === "error").length;
      await nc.drain();
      process.exit(errores ? 1 : 0);
      break;
    }

    case "tail": {
      if (!sub) die("uso: flux tail <patrón>   p.ej. flux tail 'pedidos.>'");
      await tail(js, jsm, sub, opts);
      break;
    }

    case "provision": {
      const codigo = await comandoProvision(jsm, {
        manifiesto: sub,
        dryRun: opts.dryRun,
        replicas: opts.replicas,
        bundle: opts.bundle,
      });
      await nc.drain();
      process.exit(codigo);
      break;
    }

    case "dlq": {
      const accion = sub;
      const dominio = rest[0];
      if (!accion) die("uso: flux dlq <ls|inspect|replay> <dominio>");
      if (!dominio) die(`uso: flux dlq ${accion} <dominio>   p.ej. flux dlq ${accion} pedidos`);
      const o = { ...opts, limit: opts.limit ?? 500 };
      if (accion === "ls") await dlqList(js, jsm, dominio, o);
      else if (accion === "inspect") await dlqInspect(js, jsm, dominio, o);
      else if (accion === "replay") await dlqReplay(js, jsm, dominio, o);
      else die(`acción desconocida: ${accion}. Usa ls, inspect o replay`);
      break;
    }

    case "state": {
      const codigo = await comandoState({ nc, jsm }, sub, rest, opts);
      await nc.drain();
      process.exit(codigo);
      break;
    }

    default:
      die(`comando desconocido: ${cmd}. Usa 'flux --help'`);
  }
  await nc.drain();
} catch (e) {
  await nc.drain().catch(() => {});
  die(e.message);
}
