#!/usr/bin/env node
/**
 * flux-gateway — el gateway HTTP de borde de flux. Ver gateway/README.md y specification/12-edge.md.
 *
 *   flux-gateway [--config gateway.json]        arranca
 *   flux-gateway --check [--config gateway.json] valida la configuración (y que las variables de entorno existen) y sale
 *   flux-gateway token                           genera un token de cliente y su SHA-256 (el que va a gateway.json)
 *   flux-gateway hash                            lee un token por stdin e imprime su SHA-256
 */

import { fileURLToPath } from "node:url";

import { nuevoToken, sha256Hex } from "./auth.mjs";
import { ConfigError, cargarConfigDeFichero } from "./config.mjs";
import { iniciarGateway } from "./gateway.mjs";
import { crearRegistro } from "./registro.mjs";

const USO = `uso: flux-gateway [--config gateway.json] [--check]
     flux-gateway token
     flux-gateway hash   (el token por stdin)`;

export async function main(argv, { stdout = process.stdout, stderr = process.stderr, stdin = process.stdin } = {}) {
  const args = [...argv];
  const orden = args[0] && !args[0].startsWith("-") ? args.shift() : "start";

  if (orden === "token") {
    const t = nuevoToken();
    // El token, UNA vez, para quien lo va a usar; el hash, para gateway.json. El token no se guarda en ningún sitio.
    stdout.write(`token:  ${t}\nsha256: ${sha256Hex(t)}\n`);
    stderr.write("Entrega el token al cliente por un canal seguro y pon el sha256 en `tokens` de su cliente en gateway.json. El gateway no guarda el token.\n");
    return 0;
  }
  if (orden === "hash") {
    let texto = "";
    for await (const trozo of stdin) texto += trozo;
    stdout.write(`${sha256Hex(texto.trim())}\n`);
    return 0;
  }
  if (orden !== "start") {
    stderr.write(`orden desconocida «${orden}»\n${USO}\n`);
    return 2;
  }

  let ruta = process.env.FLUX_GATEWAY_CONFIG ?? "gateway.json";
  let soloComprobar = false;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--config") ruta = args[++i];
    else if (args[i] === "--check") soloComprobar = true;
    else { stderr.write(`opción desconocida «${args[i]}»\n${USO}\n`); return 2; }
  }

  let config;
  try {
    config = cargarConfigDeFichero(ruta);
  } catch (e) {
    if (e instanceof ConfigError) { stderr.write(`${e.message}\n`); return 1; }
    throw e;
  }
  if (soloComprobar) {
    stdout.write(`✓ ${ruta} es válido: ${config.clientes.length} cliente(s), ${config.clientes.reduce((n, c) => n + c.hooks.length, 0)} hook(s), ${config.clientes.reduce((n, c) => n + c.deliveries.length, 0)} entrega(s)\n`);
    return 0;
  }

  const registro = crearRegistro();
  let gateway;
  try {
    gateway = await iniciarGateway(config, { registro });
  } catch (e) {
    registro.error("el gateway no pudo arrancar", { code: e?.code, motivo: e instanceof Error ? e.message : String(e) });
    return 1;
  }
  const parar = async (senal) => {
    registro.info("parando", { motivo: senal });
    await gateway.cerrar();
    process.exit(0);
  };
  process.once("SIGTERM", () => void parar("SIGTERM"));
  process.once("SIGINT", () => void parar("SIGINT"));
  return undefined; // sigue vivo
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const codigo = await main(process.argv.slice(2));
  if (codigo !== undefined) process.exit(codigo);
}
