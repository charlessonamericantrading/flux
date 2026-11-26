#!/usr/bin/env node
/**
 * El ecosistema de ejemplo, con UN comando, bajo la configuración segura (TLS, una nkey por servicio, las ACLs que genera `gen-acl.mjs`,
 * `flux provision`; nada de `--dev`), y un guion que lo recorre contando QUÉ primitiva se usa en cada paso y POR QUÉ.
 *
 *   (cd sdk-node && npm ci && npm run build) && (cd conformance && npm ci) && (cd cli && npm ci) && (cd gateway && npm ci) && (cd examples/ecosistema && npm ci)
 *   NATS_SERVER=/ruta/a/nats-server node examples/ecosistema/run.mjs          # o:  npm start  (en examples/ecosistema)
 *
 *   --una-vez     recorre el guion y sale (por defecto se queda corriendo para que pruebes con curl y con `php ficha.php`)
 *
 * Necesita: Node ≥ 20, `nats-server` 2.14 (NATS_SERVER), `openssl` (la CA de desarrollo) y, para la parte de PHP, `php` con curl.
 * Puertos: FLUX_TEST_PORTS=desde-hasta (por defecto 7000-7099). Todo se escribe en examples/ecosistema/.run/ (claves privadas: no se versiona).
 */

import { mkdirSync, rmSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { dormir, esperarHasta, levantar } from "./lib/entorno.mjs";

const AQUI = path.dirname(fileURLToPath(import.meta.url));
const unaVez = process.argv.includes("--una-vez");
const C = process.stdout.isTTY && !process.env.NO_COLOR;
const negrita = (s) => (C ? `\x1b[1m${s}\x1b[0m` : s);
const tenue = (s) => (C ? `\x1b[90m${s}\x1b[0m` : s);
const verde = (s) => (C ? `\x1b[32m${s}\x1b[0m` : s);
const paso = (n, titulo, primitiva, porque) => console.log(`\n${negrita(`${n}. ${titulo}`)}\n   ${verde(primitiva)} ${tenue(`— ${porque}`)}`);

const run = path.join(AQUI, ".run");
rmSync(run, { recursive: true, force: true });   // cada ejecución empieza de cero
mkdirSync(run, { recursive: true });

console.log(negrita("Levantando el ecosistema bajo la configuración segura…"));
const e = await levantar({ dir: run });
console.log(`  ✓ nats-server con TLS y las ACLs GENERADAS de services.json ${tenue(`(${e.url})`)}`);
console.log("  ✓ `flux provision`: los streams y los durables (ningún servicio puede crearlos)");
console.log(`  ✓ gateway ${tenue(e.gatewayUrl)}${e.conPhp ? ` · «hosting compartido» con PHP ${tenue(e.phpUrl)}` : tenue(" · sin `php` en el PATH: se omite la parte de PHP")}`);

let parado = false;
const parar = async () => {
  if (parado) return;
  parado = true;
  console.log("\nParando…");
  await e.parar({ conservar: true });
  process.exit(0);
};
process.once("SIGINT", parar);
process.once("SIGTERM", parar);

try {
  const crm = await e.arrancar("crm");
  await e.arrancar("facturacion");
  console.log("  ✓ crm y facturacion escuchando (cada uno es un proceso con SOLO su semilla)");

  paso(1, "El CRM da de alta dos clientes", "ESTADO + EVENTO", "el dato personal (nombre, correo) viaja como estado, clasificado y borrable; el evento solo dice «hay un cliente nuevo» y lleva su referencia");
  await crm.comando("registrar-cliente", { clienteId: "cli-1", nombre: "Ana Pérez", email: "ana.perez@example.com", plan: "plata" });
  await crm.comando("registrar-cliente", { clienteId: "cli-2", nombre: "Luis Gómez", email: "luis.gomez@example.com", plan: "oro" });

  paso(2, "La tienda arranca después y recibe la foto de los clientes", "ESTADO (state.watch)", "sin GET /clientes?page=, sin carga inicial a mano: la foto y los cambios llegan por el mismo canal");
  const tienda = await e.arrancar("tienda-web");
  await tienda.esperar((l) => l.evento === "foto-inicial");
  console.log(`   la tienda tiene en su base de datos: ${Object.keys(e.leer("tienda-web").clientes).join(", ")}`);

  paso(3, "El CRM cambia el plan de cli-1", "ESTADO", "la tienda se entera sola, sin llamar al CRM y sin que nadie pregunte «¿hay cambios?»");
  await crm.comando("cambiar-plan", { clienteId: "cli-1", plan: "oro" });
  await tienda.esperar((l) => l.evento === "cliente-aplicada" && l.aggregateVersion === 2);
  console.log(`   plan de cli-1 en la tienda: ${e.leer("tienda-web").clientes["cli-1"].plan}`);

  paso(4, "La tienda crea un pedido; facturación emite la factura", "EVENTO + OPERACIÓN", "el pedido es un hecho (evento, con el cliente por referencia); para el descuento, facturación LLAMA al CRM (petición/respuesta: un cálculo y una orden con respuesta inmediata)");
  await tienda.comando("crear-pedido", { pedidoId: "ped-001", clienteId: "cli-1", lineas: [{ sku: "SILLA-1", cantidad: 2, precioUnitarioCents: 5000 }] });
  await tienda.esperar((l) => l.evento === "factura-aplicada");
  const f = e.leer("facturacion").facturas[0];
  console.log(`   factura nº ${f.numero}: ${f.subtotalCents} − ${f.descuentoCents} (descuento que CALCULÓ el CRM) = ${f.totalCents} céntimos`);

  paso(5, "El broker entrega el MISMO pedido otra vez (un replay, una caída…)", "IDEMPOTENCIA", "at-least-once: los duplicados llegan. Facturación guarda «evento procesado» en la misma transacción que la factura");
  await e.reinyectar("EVT_PEDIDOS", "pedidos.pedido.v1.creado");
  await facturacion().esperar((l) => l.evento === "duplicado-ignorado");
  console.log(`   facturas: ${e.leer("facturacion").facturas.length} (la segunda entrega no hizo nada)`);

  paso(6, "La pasarela de pagos (un tercero) avisa del cobro, TRES veces", "GATEWAY: HTTP → evento", "el webhook entra por el gateway con su firma; el mismo aviso reenviado da el mismo `id` y el consumidor aplica UN efecto");
  const cuerpo = JSON.stringify({ id: "evt_demo_1", created: Math.floor(Date.now() / 1000), amount: 9000, currency: "EUR", metadata: { pedidoId: "ped-001" }, customer: { email: "pagador@example.com" } });
  for (let i = 1; i <= 3; i++) console.log(`   envío ${i}: HTTP ${(await e.webhook(cuerpo)).status}`);
  await facturacion().esperar((l) => l.evento === "cobro-registrado");
  await dormir(300);
  console.log(`   cobros registrados: ${Object.keys(e.leer("facturacion").cobros).length} · factura ${e.leer("facturacion").facturas[0].estado}`);

  if (e.conPhp) {
    paso(7, "Una web en PHP (hosting compartido) lee el estado y llama a una operación", "GATEWAY: HTTP ↔ flux", "no puede mantener una conexión NATS: solo habla HTTP con el gateway, que le da los mismos permisos que a un servicio, ni uno más");
    console.log(`   ${tenue(`curl '${e.phpUrl}/ficha.php?cliente=cli-1&total=9990'`)}`);
    console.log(`   → ${await (await fetch(`${e.phpUrl}/ficha.php?cliente=cli-1&total=9990`)).text()}`.trimEnd());
    await esperarHasta(() => e.leerWeb().length >= 1, 10_000);
    console.log(`   y recibió ${e.leerWeb().length} entrega(s) firmada(s) de facturas (flux → HTTP), deduplicadas por Idempotency-Key`);
  }

  paso(8, "La tienda intenta llamar al CRM (no tiene `calls` en el manifiesto)", "PERMISOS GENERADOS", "el broker lo deniega aunque la credencial sea válida: nada de esto se escribió a mano");
  const bus = await e.conectarComo("tienda-web");
  try {
    await bus.request("clientes.cliente.v1.calcular-descuento", { clienteId: "cli-1", totalCents: 100, moneda: "EUR" }, { timeoutMs: 2000 });
  } catch (err) {
    console.log(`   ✓ ${err.code}: ${String(err.message).slice(0, 120)}…`);
  } finally {
    await bus.close();
  }

  console.log(`\n${verde("✓ el recorrido completo funciona")}`);
  if (!unaVez) {
    console.log(`
Sigue corriendo. Prueba tú, con las credenciales de esta ejecución:

  export GATEWAY_URL=${e.gatewayUrl} TOKEN=${e.token}
  curl -s $GATEWAY_URL/v1/state/clientes.cliente.v1/cli-1 -H "Authorization: Bearer $TOKEN"
  curl -s -X POST $GATEWAY_URL/v1/rpc/clientes.cliente.v1.calcular-descuento -H "Authorization: Bearer $TOKEN" \\
       -H 'Content-Type: application/json' -d '{"clienteId":"cli-1","totalCents":9990,"moneda":"EUR"}'
${e.conPhp ? `  FLUX_GATEWAY_URL=$GATEWAY_URL FLUX_GATEWAY_TOKEN=$TOKEN php examples/ecosistema/web-php/ficha.php cli-1 9990\n` : ""}
Lo que ha escrito cada servicio: ${path.relative(process.cwd(), path.join(run, "datos")) || "."}/*.json   Ctrl-C para parar.`);
    await new Promise(() => {});
  }
  await parar();
} catch (err) {
  console.error(`\n✗ ${err.message}`);
  await e.parar({ conservar: true }).catch(() => {});
  process.exit(1);
}

function facturacion() {
  return e.procesos.facturacion;
}
