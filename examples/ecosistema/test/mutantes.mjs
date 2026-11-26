#!/usr/bin/env node
/**
 * «Una prueba que pasa con el código roto no vale.» Cada mutante copia el ejemplo a un temporal, estropea UNA cosa importante y ejecuta
 * `test/ecosistema.test.mjs` contra la copia (`ECOSISTEMA_DIR`): la prueba DEBE fallar, y por la razón correcta (el nombre de la prueba que
 * falla). Antes se ejecuta el ejemplo SIN estropear: tiene que pasar, o «todos los mutantes fallan» no demostraría nada.
 *
 *   NATS_SERVER=… FLUX_REQUIRE_BROKER=1 FLUX_REQUIRE_PHP=1 node test/mutantes.mjs [--solo <nombre>] [--lista]
 */

import { spawnSync } from "node:child_process";
import { cpSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const AQUI = path.dirname(fileURLToPath(import.meta.url));
const DIR = path.join(AQUI, "..");

/** `en`: el fichero; `de` → `a`: la sustitución (el texto `de` DEBE aparecer exactamente una vez); `falla`: lo que debe contener el nombre de la prueba que falla. */
const MUTANTES = [
  // ── el requisito de la tarea: quitar la idempotencia de facturación debe romper la prueba ───────────────────────────────────────────
  { nombre: "facturacion-sin-idempotencia", en: "servicios/facturacion.mjs", cambios: [["if (db.leer().procesados[evento.id]) {", "if (false) {"]], falla: "la MISMA entrega otra vez" },
  { nombre: "facturacion-marca-solo-en-memoria", en: "servicios/facturacion.mjs", cambios: [
    ["export async function iniciar({ bus, cfg, registro }) {", "const vistos = new Set();\nexport async function iniciar({ bus, cfg, registro }) {"],
    ["if (db.leer().procesados[evento.id]) {", "if (vistos.has(evento.id)) {"],
    ["d.procesados[evento.id] = new Date().toISOString();", "vistos.add(evento.id);"],
  ], falla: "la MISMA entrega otra vez" },
  { nombre: "crm-sin-clave-de-negocio", en: "servicios/crm.mjs", cambios: [["const previa = db.leer().puntosAplicados[peticion.referencia];", "const previa = undefined;"]], falla: "una ORDEN repetida" },
  { nombre: "tienda-ignora-la-lapida", en: "servicios/tienda-web.mjs", cambios: [["delete d.clientes[entrada.key];", "/* no borra */"]], falla: "borra la copia" },
  { nombre: "tienda-ignora-la-version", en: "servicios/tienda-web.mjs", cambios: [['if (entrada.aggregateVersion <= (d.versiones[entrada.key] ?? 0)) return "obsoleta";', ""]], falla: "no pisa una nueva" },
  // ── el requisito de la tarea: conceder de más a un servicio en el manifiesto debe romper la negativa de seguridad ─────────────────────
  { nombre: "manifiesto-tienda-consume-cobros", en: "services.json", cambios: [['"owns": ["pedidos"],\n      "consumes": ["facturacion.factura.v1.emitida"]', '"owns": ["pedidos"],\n      "consumes": ["facturacion.factura.v1.emitida", "pagos.cobro.v1.recibido"]']], falla: "tienda-web NO puede leer los cobros" },
  { nombre: "manifiesto-tienda-llama-al-crm", en: "services.json", cambios: [['"owns": ["pedidos"],', '"owns": ["pedidos"],\n      "calls": ["clientes.cliente.v1.calcular-descuento", "clientes.cliente.v1.sumar-puntos"],']], falla: "tienda-web NO puede llamar al CRM" },
  { nombre: "manifiesto-facturacion-observa-clientes", en: "services.json", cambios: [['"owns": ["facturacion"],', '"owns": ["facturacion"],\n      "watches": ["clientes.cliente.v1"],']], falla: "facturacion NO puede leer los clientes" },
  { nombre: "manifiesto-web-php-suma-puntos", en: "services.json", cambios: [['"calls": ["clientes.cliente.v1.calcular-descuento"]\n    }\n  ]', '"calls": ["clientes.cliente.v1.calcular-descuento", "clientes.cliente.v1.sumar-puntos"]\n    }\n  ]']], falla: "CLIENTE DE BORDE no tiene permisos especiales" },
  // ── PHP y el gateway ─────────────────────────────────────────────────────────────────────────────────────────────────────────────
  { nombre: "php-recibir-sin-deduplicar", en: "web-php/recibir.php", cambios: [["if (!isset($vistas[$clave])) {", "if (true) {"]], falla: "recibir.php recibe las facturas" },
  { nombre: "php-recibir-sin-verificar-la-firma", en: "web-php/recibir.php", cambios: [["if ($resultado !== 'ok') {", "if (false) {"]], falla: "recibir.php recibe las facturas" },
  { nombre: "gateway-webhook-con-id-aleatorio", en: "gateway.json", cambios: [['"externalKey": { "from": "body", "pointer": "/id" },', '"idempotent": false,']], falla: "enviado tres veces" },
  { nombre: "gateway-webhook-sin-pick", en: "gateway.json", cambios: [['"data": {\n            "pick": {\n              "pagoId": "/id",\n              "pedidoId": "/metadata/pedidoId",\n              "importeCents": "/amount",\n              "moneda": "/currency"\n            }\n          }', '"data": { "wrapAs": "cuerpo" }']], falla: "enviado tres veces" },
];

const solo = process.argv.includes("--solo") ? process.argv[process.argv.indexOf("--solo") + 1] : undefined;
if (process.argv.includes("--lista")) {
  for (const m of MUTANTES) console.log(m.nombre);
  process.exit(0);
}

function ejecutar(dir) {
  const r = spawnSync(process.execPath, ["--test", "--test-force-exit", "--test-reporter=tap", path.join(AQUI, "ecosistema.test.mjs")], {
    encoding: "utf8", cwd: DIR, env: { ...process.env, ECOSISTEMA_DIR: dir },
  });
  const salida = `${r.stdout}${r.stderr}`;
  const fallan = [...salida.matchAll(/^\s*not ok \d+ - (.*)$/gm)].map((m) => m[1]);
  return { estado: r.status, fallan, salida };
}

function copia(m) {
  const tmp = mkdtempSync(path.join(os.tmpdir(), "flux-eco-mutante-"));
  cpSync(DIR, tmp, { recursive: true, filter: (f) => !/[\\/](node_modules|\.run)([\\/]|$)/.test(f) });
  symlinkSync(path.join(DIR, "node_modules"), path.join(tmp, "node_modules"));
  for (const [de, a] of m?.cambios ?? []) {
    const f = path.join(tmp, m.en);
    const texto = readFileSync(f, "utf8");
    const n = texto.split(de).length - 1;
    if (n !== 1) throw new Error(`${m.nombre}: «${de.slice(0, 60)}…» aparece ${n} veces en ${m.en}; debe aparecer exactamente una (¿cambió el ejemplo?)`);
    writeFileSync(f, texto.replace(de, () => a));
  }
  return tmp;
}

console.log("Sin estropear: debe PASAR…");
const base = copia(undefined);
const b = ejecutar(base);
rmSync(base, { recursive: true, force: true });
if (b.estado !== 0) {
  console.error(`✗ el ejemplo SIN estropear no pasa su prueba:\n${b.salida.split("\n").slice(-60).join("\n")}`);
  process.exit(1);
}
console.log("  ✓ pasa");

let mal = 0;
let n = 0;
for (const m of MUTANTES) {
  if (solo && m.nombre !== solo) continue;
  n++;
  const dir = copia(m);
  const r = ejecutar(dir);
  rmSync(dir, { recursive: true, force: true });
  const detectado = r.estado !== 0;
  const porLaRazon = !m.falla || r.fallan.some((f) => f.includes(m.falla));
  if (detectado && porLaRazon) console.log(`  ✓ ${m.nombre}: detectado${m.falla ? ` (falla «${m.falla}»)` : ""}`);
  else {
    mal++;
    console.log(`  ✗ ${m.nombre}: ${detectado ? `falló, pero no por «${m.falla}»: ${r.fallan.join(" | ")}` : "NO lo detecta la prueba (pasó con el ejemplo roto)"}`);
  }
}
console.log(mal ? `\n✗ ${mal} de ${n} mutantes NO detectados` : `\n✓ ${n} mutantes: todos detectados`);
process.exit(mal ? 1 : 0);
