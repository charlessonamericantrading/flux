/**
 * Un `nats-server` REAL para los tests de integración.
 *
 * Los permisos que prueba el SDK son los del broker: un mock no los aplica, y el plan
 * v1.1 existe precisamente porque el modelo de permisos «nunca se había ejecutado». Por
 * eso aquí no hay mocks.
 *
 * Sin binario, estos tests se SALTAN con aviso — salvo con `FLUX_REQUIRE_BROKER=1`, donde
 * FALLAN. Es lo que impide que el CI dé verde sin haberlos ejecutado nunca: un fixture
 * que nadie ejecuta aparenta cobertura.
 *
 *   NATS_SERVER=/ruta/a/nats-server        binario (si no, se busca en el PATH)
 *   FLUX_REQUIRE_BROKER=1                  su ausencia es un FALLO, no un salto
 *   FLUX_TEST_PORTS=4400-4449              rango de puertos (por defecto, ese)
 */

import { spawn, type ChildProcess } from "node:child_process";
import { accessSync, constants, mkdtempSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

import { connect, type NatsConnection } from "@nats-io/transport-node";
import { jetstreamManager, type JetStreamManager } from "@nats-io/jetstream";

// ─── Localizar el binario ────────────────────────────────────────────────────

/** `FLUX_REQUIRE_BROKER` definida y no vacía, `0` o `false`. */
export function brokerRequired(): boolean {
  const v = (process.env.FLUX_REQUIRE_BROKER ?? "").trim().toLowerCase();
  return v !== "" && v !== "0" && v !== "false";
}

function esEjecutable(ruta: string): boolean {
  try {
    if (!statSync(ruta).isFile()) return false;
    accessSync(ruta, constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

export function localizarNatsServer(): { ruta?: string; motivo?: string } {
  const explicito = process.env.NATS_SERVER;
  if (explicito) {
    // Si lo indican y no existe, NO se cae al PATH: usar otro binario sin avisar sería
    // probar contra algo distinto de lo que se cree.
    return esEjecutable(explicito)
      ? { ruta: explicito }
      : { motivo: `NATS_SERVER=${explicito} no es un fichero ejecutable` };
  }
  for (const d of (process.env.PATH ?? "").split(delimiter)) {
    const candidato = join(d, "nats-server");
    if (d && esEjecutable(candidato)) return { ruta: candidato };
  }
  return { motivo: "no hay `nats-server` en el PATH ni NATS_SERVER definida" };
}

/**
 * Como `describe`, pero con el contrato del broker: si falta el binario (o un requisito
 * extra, como `openssl`), salta con aviso o —con FLUX_REQUIRE_BROKER— falla.
 *
 * `requisito` devuelve el motivo si algo más falta, o `undefined` si todo está.
 */
export function describeBroker(
  nombre: string,
  cuerpo: () => void,
  requisito?: () => string | undefined,
): void {
  const { ruta, motivo } = localizarNatsServer();
  const falta = ruta ? requisito?.() : motivo;
  if (!falta) {
    describe(nombre, cuerpo);
    return;
  }
  if (brokerRequired()) {
    describe(nombre, () => {
      test("están todos los requisitos (FLUX_REQUIRE_BROKER está activada)", () => {
        assert.fail(
          `${falta}. Con FLUX_REQUIRE_BROKER los tests de integración son obligatorios: ` +
            `saltarlos daría un CI verde sin haber ejecutado nada contra el broker.`,
        );
      });
    });
    return;
  }
  console.warn(
    `\nAVISO: se SALTAN los tests de integración «${nombre}»: ${falta}.\n` +
      `       Define NATS_SERVER, o FLUX_REQUIRE_BROKER=1 para que su ausencia sea un fallo.\n`,
  );
  describe(nombre, { skip: falta }, () => {
    test("(saltado)", () => {});
  });
}

// ─── Arrancar y parar ────────────────────────────────────────────────────────

export interface Broker {
  readonly port: number;
  readonly url: string;
  /** Directorio temporal propio (store de JetStream, certificados…). */
  readonly dir: string;
  /** Últimas líneas del log del servidor, para diagnosticar. */
  log(): string;
  stop(): Promise<void>;
}

export interface ContextoConfig {
  port: number;
  /** `store_dir` de JetStream, ya dentro de `dir`. */
  store: string;
  dir: string;
}

export function rangoDePuertos(): [number, number] {
  const m = /^(\d+)-(\d+)$/.exec(process.env.FLUX_TEST_PORTS ?? "4400-4449");
  if (!m) throw new Error("FLUX_TEST_PORTS debe ser `desde-hasta`, por ejemplo 4400-4449");
  return [Number(m[1]), Number(m[2])];
}

/**
 * Arranca un nats-server con la config que escribe `config`.
 *
 * El puerto se elige al azar dentro del rango y se REINTENTA si el servidor no consigue
 * escuchar en él: varios ficheros de test (y otros procesos de la máquina) pueden
 * coincidir. Se da por bueno solo cuando el log DEL PROPIO servidor dice que escucha en
 * ese puerto: comprobar que el puerto responde engañaría si otro proceso ya lo ocupaba.
 */
export async function startBroker(
  config: (c: ContextoConfig) => string,
  /** `port`: un puerto concreto, sin reintentar con otro (para reabrir uno que se acaba de cerrar). */
  opciones: { port?: number } = {},
): Promise<Broker> {
  const { ruta, motivo } = localizarNatsServer();
  if (!ruta) throw new Error(motivo);
  const [desde, hasta] = rangoDePuertos();
  const dir = mkdtempSync(join(tmpdir(), "flux-nats-"));
  const intentos = opciones.port !== undefined ? 1 : Math.min(30, hasta - desde + 1);

  for (let i = 0; i < intentos; i++) {
    const port = opciones.port ?? desde + Math.floor(Math.random() * (hasta - desde + 1));
    const store = join(dir, `js-${port}`);
    const conf = join(dir, `nats-${port}.conf`);
    writeFileSync(conf, config({ port, store, dir }));

    const hijo = spawn(ruta, ["-c", conf], { stdio: ["ignore", "ignore", "pipe"] });
    const lineas: string[] = [];
    let salio: number | null | undefined;
    hijo.once("exit", (code) => (salio = code ?? -1));
    hijo.stderr!.setEncoding("utf8");
    hijo.stderr!.on("data", (d: string) => {
      lineas.push(...d.split("\n").filter(Boolean));
      if (lineas.length > 400) lineas.splice(0, lineas.length - 400);
    });

    const listo = await esperarListo(hijo, lineas, port, () => salio !== undefined);
    if (listo) {
      // Red de seguridad: un test abortado no debe dejar el servidor vivo. Solo se mata
      // el PID lanzado aquí, nunca por nombre — hay otros nats-server en la máquina.
      const matar = () => hijo.kill("SIGKILL");
      process.once("exit", matar);
      return {
        port,
        url: `nats://127.0.0.1:${port}`,
        dir,
        log: () => lineas.join("\n"),
        async stop() {
          process.removeListener("exit", matar);
          if (salio === undefined) {
            const fin = new Promise<void>((r) => hijo.once("exit", () => r()));
            hijo.kill("SIGTERM");
            const dura = setTimeout(() => hijo.kill("SIGKILL"), 3000);
            await fin;
            clearTimeout(dura);
          }
          rmSync(dir, { recursive: true, force: true });
        },
      };
    }
    if (salio === undefined) hijo.kill("SIGKILL");
    if (!lineas.some((l) => /address already in use|Error listening/i.test(l))) {
      // No es un puerto ocupado: es la config. Reintentar con otro puerto la repetiría.
      rmSync(dir, { recursive: true, force: true });
      throw new Error(`nats-server no arrancó con la config de test:\n${lineas.join("\n")}`);
    }
  }
  rmSync(dir, { recursive: true, force: true });
  throw new Error(`no se encontró un puerto libre en ${desde}-${hasta}`);
}

async function esperarListo(
  hijo: ChildProcess,
  lineas: string[],
  port: number,
  haSalido: () => boolean,
): Promise<boolean> {
  const limite = Date.now() + 10_000;
  while (Date.now() < limite) {
    if (haSalido()) return false;
    const escucha = lineas.some((l) => new RegExp(`Listening for client connections on .*:${port}\\b`).test(l));
    if (escucha && lineas.some((l) => /Server is ready/.test(l))) return true;
    await new Promise((r) => setTimeout(r, 25));
  }
  hijo.kill("SIGKILL");
  throw new Error(`nats-server no estuvo listo en 10 s:\n${lineas.join("\n")}`);
}

// ─── Utilidades de test ──────────────────────────────────────────────────────

/** Conexión cruda (sin el SDK). Con `inboxPrefix` propio para que sus inboxes no se confundan. */
export async function conectarCrudo(
  broker: Broker,
  user: string,
  pass = "pw",
  extra: Record<string, unknown> = {},
): Promise<NatsConnection> {
  return connect({
    servers: broker.url,
    user,
    pass,
    timeout: 3000,
    inboxPrefix: `_INBOX.${user}`,
    ...extra,
  });
}

export async function conectarOps(broker: Broker): Promise<{ nc: NatsConnection; jsm: JetStreamManager }> {
  const nc = await conectarCrudo(broker, "ops");
  return { nc, jsm: await jetstreamManager(nc) };
}

/** Espera a que `cond` se cumpla; devuelve si lo hizo (no lanza: el test decide). */
export async function esperarHasta(cond: () => boolean | Promise<boolean>, ms: number, cada = 25): Promise<boolean> {
  const limite = Date.now() + ms;
  while (Date.now() < limite) {
    if (await cond()) return true;
    await new Promise((r) => setTimeout(r, cada));
  }
  return cond();
}

export const dormir = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Todos los mensajes de un stream, parseados. Solo para inspeccionar la DLQ. */
export async function mensajesDe(jsm: JetStreamManager, stream: string): Promise<Array<Record<string, any>>> {
  const info = await jsm.streams.info(stream);
  const out: Array<Record<string, any>> = [];
  if (info.state.messages === 0) return out;
  for (let seq = info.state.first_seq; seq <= info.state.last_seq; seq++) {
    const m = await jsm.streams.getMessage(stream, { seq }).catch(() => null);
    if (m) out.push(JSON.parse(new TextDecoder().decode(m.data)));
  }
  return out;
}
