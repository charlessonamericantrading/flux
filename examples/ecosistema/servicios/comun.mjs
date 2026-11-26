/**
 * Lo que comparten los tres servicios nativos del ecosistema: leer la configuración del ENTORNO (como en producción: ni una
 * credencial en el repositorio), conectar con el SDK, escribir un registro y recibir «comandos del operador» por la entrada estándar.
 *
 * Los comandos por stdin SUSTITUYEN a la interfaz de cada aplicación (el formulario del CRM, el botón «comprar» de la tienda): en tu
 * sistema, lo que llame a `bus.state.put` o a `bus.publish` es tu código de negocio, no esto. Aquí existen para que el guion de
 * `run.mjs` y las pruebas puedan mover el ecosistema sin escribir HTTP.
 *
 * El registro NUNCA lleva `data`: un nombre o un correo en un log son PII fuera de su sitio (AGENTS.md, regla 5).
 */

import { readFileSync } from "node:fs";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath, pathToFileURL } from "node:url";

import { connect } from "@flux/sdk";

const AQUI = path.dirname(fileURLToPath(import.meta.url));

/** La configuración, toda del entorno. Falla —diciendo qué falta— en vez de arrancar a medias. */
export function leerConfig(env = process.env) {
  const falta = [];
  const pide = (nombre) => {
    if (!env[nombre]) falta.push(nombre);
    return env[nombre];
  };
  const semilla = env.NKEY_SEED ?? (env.NKEY_SEED_FILE ? readFileSync(env.NKEY_SEED_FILE, "utf8").trim() : undefined);
  if (!semilla) falta.push("NKEY_SEED (o NKEY_SEED_FILE)");
  const cfg = {
    servidor: pide("NATS_URL"),
    ca: env.NATS_CA_FILE,
    semilla,
    entorno: env.FLUX_ENV ?? "produccion",
    version: env.FLUX_VERSION ?? "1.0.0",
    tenant: env.FLUX_TENANT ?? "acme",
    datos: path.resolve(env.DATA_DIR ?? path.join(AQUI, "..", ".run", "datos")),
    bundle: path.resolve(env.BUNDLE_FILE ?? path.join(AQUI, "..", "schemas", "bundle.json")),
  };
  if (falta.length) throw new Error(`falta configuración en el entorno: ${falta.join(", ")}`);
  return cfg;
}

/** Una línea JSON por evento del servicio, a stdout. Lo que se pasa NO debe ser un `data`. */
export function crearRegistro(servicio, salida = process.stdout) {
  return (campos) => salida.write(`${JSON.stringify({ t: new Date().toISOString(), servicio, ...campos })}\n`);
}

/**
 * Conecta con el broker POR LA PUERTA SEGURA: TLS con la CA que se da, la nkey de ESTE servicio, el inbox privado
 * (`_INBOX.<servicio>.>`, que fija el SDK solo) y validación L3 estricta contra el bundle: publicar un `data` que no cumple su
 * contrato falla en quien publica, no en un consumidor de otro equipo la semana que viene.
 */
export async function conectarBus(servicio, cfg, registro) {
  const bundle = JSON.parse(readFileSync(cfg.bundle, "utf8"));
  return connect({
    servers: cfg.servidor,
    service: servicio,
    environment: cfg.entorno,
    version: cfg.version,
    tenantId: cfg.tenant,
    tenantIsolation: "strict",
    schemaBaseUrl: "https://schemas.internal",
    credentials: { nkeySeed: cfg.semilla },
    ...(cfg.ca && { tls: { caFile: cfg.ca } }),
    validation: { mode: "strict", bundle },
    // Un ack o un pull denegados no lanzan: solo el logger los dice. Siempre uno.
    logger: { warn: (...a) => registro({ nivel: "warn", mensaje: a.join(" ") }), error: (...a) => registro({ nivel: "error", mensaje: a.join(" ") }) },
    onPoison: ({ subject, error }) => registro({ nivel: "error", evento: "poison", subject, codigo: error.code ?? error.name }),
  });
}

/**
 * Lee de stdin líneas JSON `{ "n": 1, "cmd": "…", …args }` y responde con una línea
 * `{ "evento": "resultado", "n": 1, "cmd": "…", "ok": true, …resultado }` (o `ok: false` y un `codigo`).
 */
export function escucharComandos(manejadores, registro, entrada = process.stdin) {
  const rl = readline.createInterface({ input: entrada });
  rl.on("line", async (linea) => {
    if (!linea.trim()) return;
    let orden;
    try {
      orden = JSON.parse(linea);
    } catch {
      return registro({ evento: "resultado", ok: false, codigo: "COMANDO_ILEGIBLE" });
    }
    const { n, cmd, ...args } = orden;
    const manejador = manejadores[cmd];
    if (!manejador) return registro({ evento: "resultado", n, cmd, ok: false, codigo: "COMANDO_DESCONOCIDO" });
    try {
      registro({ evento: "resultado", n, cmd, ok: true, ...(await manejador(args)) });
    } catch (e) {
      // El código estable del error, nunca su mensaje: puede citar valores.
      registro({ evento: "resultado", n, cmd, ok: false, codigo: e.code ?? e.name ?? "ERROR" });
    }
  });
}

/** ¿Se ejecuta este módulo directamente (`node servicios/crm.mjs`) y no se importa? */
export const esPrincipal = (metaUrl) => process.argv[1] !== undefined && pathToFileURL(path.resolve(process.argv[1])).href === metaUrl;

/**
 * El `main` de cada servicio: conecta, llama a su `iniciar({ bus, cfg, registro })` y, con SIGTERM o SIGINT, para en orden
 * (`bus.close()` deja de escuchar, espera a lo que está en vuelo y SOLO ENTONCES cierra la conexión: cerrarla de golpe pierde las
 * respuestas de lo que se atiende, A-83).
 */
export async function ejecutar(servicio, iniciar) {
  const registro = crearRegistro(servicio);
  let bus;
  try {
    const cfg = leerConfig();
    bus = await conectarBus(servicio, cfg, registro);
    const app = await iniciar({ bus, cfg, registro });
    escucharComandos(app.comandos ?? {}, registro);
    registro({ evento: "listo" });
    let parando = false;
    const parar = async () => {
      if (parando) return;
      parando = true;
      await app.parar?.();
      await bus.close();
      process.exit(0);
    };
    process.once("SIGTERM", parar);
    process.once("SIGINT", parar);
  } catch (e) {
    registro({ nivel: "error", evento: "no-arranca", codigo: e.code ?? e.name, mensaje: e.message });
    await bus?.close().catch(() => {});
    process.exit(1);
  }
}
