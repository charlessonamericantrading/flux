#!/usr/bin/env node
/**
 * Genera las ACLs de NATS a partir de un manifiesto de servicios.
 * Contrato normativo: specification/06-security.md §3
 *
 *   node scripts/gen-acl.mjs services.json            → imprime la config
 *   node scripts/gen-acl.mjs services.json --check    → valida el manifiesto
 *   node scripts/gen-acl.mjs services.json --tenants  → Modelo B: una account por
 *                                                       tenant (09-multitenancy.md §2)
 *
 * §3 dice: "un servicio solo publica en el dominio del que es dueño; solo se suscribe
 * a lo que se le ha concedido explícitamente". Eso es una regla derivable: si sabes
 * quién es dueño de cada dominio y qué consume cada servicio, las ACLs se calculan.
 * Escribirlas a mano garantiza que diverjan del diseño en el primer mes.
 *
 * Manifiesto (services.json):
 *   {
 *     "environment": "produccion",
 *     "services": [
 *       { "name": "pedidos-api",     "owns": ["pedidos"],
 *         "consumes": ["inventario.stock.v1.>"] },
 *       { "name": "facturacion-api", "owns": ["facturacion"],
 *         "consumes": ["pedidos.pedido.v1.>"] }
 *     ]
 *   }
 */

import { readFile } from "node:fs/promises";

const SERVICE_RE = /^[a-z0-9]+(-[a-z0-9]+)*$/;
const DOMAIN_RE = /^[a-z0-9]+(-[a-z0-9]+)*$/;

const [, , manifestPath, ...flags] = process.argv;
if (!manifestPath) {
  console.error("uso: node scripts/gen-acl.mjs <services.json> [--check]");
  process.exit(2);
}

const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
const problemas = [];
const err = (m, hint) => problemas.push({ m, hint });

// ─── Validación del manifiesto ───────────────────────────────────────────────

const dueñoDe = new Map(); // dominio → servicio

for (const svc of manifest.services ?? []) {
  if (!SERVICE_RE.test(svc.name ?? "")) {
    err(`nombre de servicio inválido: "${svc.name}"`,
      "kebab-case en minúsculas — alimenta los nombres de durable consumer (02-naming.md §4)");
  }
  for (const d of svc.owns ?? []) {
    if (!DOMAIN_RE.test(d)) err(`dominio inválido "${d}" en ${svc.name}`, "kebab-case en minúsculas");
    // UN dueño por dominio. Dos servicios publicando el mismo dominio destruye la
    // premisa entera del protocolo: "solo el dueño del agregado publica sus eventos"
    // (00-protocol.md §4). Con dos dueños nadie puede confiar en el `source`.
    const previo = dueñoDe.get(d);
    if (previo && previo !== svc.name) {
      err(`el dominio "${d}" tiene dos dueños: ${previo} y ${svc.name}`,
        "Solo el servicio propietario del agregado publica sus eventos — 00-protocol.md §4");
    }
    dueñoDe.set(d, svc.name);
  }
}

for (const svc of manifest.services ?? []) {
  for (const c of svc.consumes ?? []) {
    const dominio = c.split(".")[0];
    if (dominio.includes("*") || dominio.includes(">")) {
      err(`${svc.name} consume "${c}" con comodín en el dominio`,
        "Conceder un dominio entero por comodín es lo contrario de una concesión explícita — 06-security.md §3");
      // Sin `continue`, un comodín produce además "nadie es dueño de *": dos hallazgos
      // para una sola causa raíz, y el segundo despista.
      continue;
    }
    if (!dueñoDe.has(dominio)) {
      err(`${svc.name} consume "${c}" pero nadie declara ser dueño de "${dominio}"`,
        "O falta el productor en el manifiesto, o el subject está mal escrito");
    }
    if (svc.owns?.includes(dominio)) {
      err(`${svc.name} se suscribe a su propio dominio "${dominio}"`,
        "Un servicio que consume sus propios eventos suele ser una señal de que el flujo debería ser una llamada interna, no un evento");
    }
  }
}

if (problemas.length) {
  console.error(`\n✗ ${problemas.length} problema(s) en el manifiesto\n`);
  for (const p of problemas) {
    console.error(`  ✗ ${p.m}`);
    console.error(`    ${p.hint}`);
  }
  process.exit(1);
}

// Modelo B — aislamiento duro por account de tenant. 09-multitenancy.md §2.
if (flags.includes("--tenants")) {
  const tenants = manifest.tenants ?? [];
  if (tenants.length === 0) {
    console.error(
      `
✗ --tenants requiere una lista \`tenants\` en el manifiesto.
` +
        `  Ej.: "tenants": ["acme", "globex"]
`,
    );
    process.exit(1);
  }
  for (const t of tenants) {
    if (!DOMAIN_RE.test(t)) {
      console.error(`
✗ tenant inválido "${t}": kebab-case en minúsculas
`);
      process.exit(1);
    }
  }
  console.log(renderTenantAccounts(manifest, tenants));
  process.exit(0);
}

if (flags.includes("--check")) {
  console.log(
    `✓ manifiesto válido: ${manifest.services.length} servicio(s), ${dueñoDe.size} dominio(s)`,
  );
  process.exit(0);
}

// ─── Generación ──────────────────────────────────────────────────────────────

const env = manifest.environment ?? "produccion";
const lines = [];
const p = (s = "") => lines.push(s);

p(`# GENERADO por scripts/gen-acl.mjs — no editar a mano.`);
p(`# Entorno: ${env}`);
p(`# Fuente:  ${manifestPath}`);
p(`#`);
p(`# Regla aplicada (06-security.md §3): un servicio solo publica en el dominio del`);
p(`# que es dueño, y solo se suscribe a lo que se le concede explícitamente. Esto`);
p(`# convierte el ownership del dato en algo que aplica el BROKER: un servicio no`);
p(`# puede falsificar un evento de otro dominio aunque su código lo intente.`);
p();
p(`accounts {`);
p(`  ${env.toUpperCase()}: {`);
p(`    jetstream: enabled`);
p(`    users: [`);

for (const svc of manifest.services) {
  const pub = [
    ...(svc.owns ?? []).map((d) => `${d}.>`),
    // Excepción acotada de §3.1: un consumidor DEBE poder escribir en la DLQ de lo
    // que consume, y eso implica publicar bajo un dominio ajeno. Solo el prefijo
    // dlq., solo los subjects que ya consume.
    ...(svc.consumes ?? []).map((c) => `dlq.${c}`),
  ];
  const sub = [
    ...(svc.consumes ?? []),
    ...(svc.owns ?? []).map((d) => `dlq.${d}.>`), // el dueño puede leer su propia DLQ
    "_INBOX.>",
  ];

  p();
  p(`      # ${svc.name}`);
  if (svc.owns?.length) p(`      #   dueño de: ${svc.owns.join(", ")}`);
  if (svc.consumes?.length) p(`      #   consume: ${svc.consumes.join(", ")}`);
  p(`      {`);
  p(`        nkey: "<NKEY_DE_${svc.name.toUpperCase().replace(/-/g, "_")}>"`);
  p(`        permissions: {`);
  p(`          publish:   { allow: [${pub.map((s) => `"${s}"`).join(", ")}] }`);
  p(`          subscribe: { allow: [${sub.map((s) => `"${s}"`).join(", ")}] }`);
  p(`        }`);
  p(`      },`);
}

p(`    ]`);
p(`  }`);
p(`}`);
p();
p(`# Las credenciales NUNCA se versionan (06-security.md §2): sustituye cada`);
p(`# <NKEY_DE_*> desde el gestor de secretos al desplegar.`);

console.log(lines.join("\n"));

// ─── Modelo B: una account por tenant ────────────────────────────────────────

function renderTenantAccounts(manifest, tenants) {
  const env = (manifest.environment ?? "produccion").toUpperCase();
  const out = [];
  const w = (x = "") => out.push(x);
  const up = (x) => x.toUpperCase().replace(/-/g, "_");

  w(`# GENERADO por scripts/gen-acl.mjs --tenants — no editar a mano.`);
  w(`# Modelo B de 09-multitenancy.md §2: una account de NATS POR TENANT.`);
  w(`#`);
  w(`# Las accounts de NATS son un límite de aislamiento REAL: los subjects de una son`);
  w(`# invisibles desde otra salvo export/import explícito. Un servicio con credenciales`);
  w(`# de un tenant no puede leer los datos de otro aunque su código lo intente.`);
  w(`#`);
  w(`# COSTE, para que nadie lo descubra en producción:`);
  w(`#   · ${tenants.length} tenant(s) × los streams de cada dominio = ${tenants.length}× streams.`);
  w(`#     JetStream tiene límites de recursos por servidor.`);
  w(`#   · Un servicio que atiende a N tenants necesita N CONEXIONES, una por account.`);
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
    w(`  ${env}_${up(t)}: {`);
    w(`    jetstream: enabled`);
    w(`    users: [`);
    for (const svc of manifest.services) {
      const pub = [
        ...(svc.owns ?? []).map((d) => `${d}.>`),
        ...(svc.consumes ?? []).map((c) => `dlq.${c}`),
      ];
      const sub = [
        ...(svc.consumes ?? []),
        ...(svc.owns ?? []).map((d) => `dlq.${d}.>`),
        "_INBOX.>",
      ];
      w(`      # ${svc.name} @ ${t}`);
      w(`      {`);
      w(`        nkey: "<NKEY_DE_${up(svc.name)}_${up(t)}>"`);
      w(`        permissions: {`);
      w(`          publish:   { allow: [${pub.map((x) => `"${x}"`).join(", ")}] }`);
      w(`          subscribe: { allow: [${sub.map((x) => `"${x}"`).join(", ")}] }`);
      w(`        }`);
      w(`      },`);
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
