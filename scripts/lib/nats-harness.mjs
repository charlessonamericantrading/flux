/**
 * Lo que `scripts/verify-acl.mjs` y `conformance/security.mjs` necesitan IGUAL para aplicar
 * las ACLs generadas a un nats-server real: arrancarlo y pararlo sin dejar huérfanos, generar
 * claves de prueba, conectar vigilando las violaciones de permiso y hacer las sondas más
 * comunes (publicar, suscribir, pedir a la API de JetStream, recibir de un pull).
 *
 * Por qué es un módulo compartido: las dos herramientas empezaron siendo una sola, y copiar
 * este bloque habría dado dos versiones de «cómo se sabe que el broker denegó algo» —la
 * pregunta más fácil de responder mal, porque una publicación denegada NO lanza excepción:
 * el servidor descarta el mensaje y avisa por la conexión (A-19, A-28)—. Una sola respuesta,
 * probada por las dos.
 *
 * Las dependencias de NATS viven en `conformance/` (`scripts/` no tiene package.json): se
 * resuelven desde allí, y este módulo no añade ninguna.
 *
 *   (cd conformance && npm install)
 */

import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
/**
 * El generador bajo prueba. `FLUX_GEN_ACL` lo sustituye por otro (una COPIA estropeada a propósito: es como
 * `conformance/security/mutantes.mjs` comprueba que la suite DETECTA un generador que concede de más o de menos).
 */
export const GEN = process.env.FLUX_GEN_ACL ? path.resolve(process.env.FLUX_GEN_ACL) : path.join(ROOT, "scripts", "gen-acl.mjs");
export const CLI = path.join(ROOT, "cli", "src", "index.mjs");
export const EJEMPLO = path.join(ROOT, "examples", "services.json");

// ─── Dependencias de NATS ────────────────────────────────────────────────────

const require = createRequire(path.join(ROOT, "conformance", "package.json"));

/**
 * Carga las dependencias de NATS. Devuelve `null` si faltan: quien llama decide cómo
 * quejarse (un script de verificación sale con 2; la suite explica qué ejecutar).
 */
export function cargarNats() {
  try {
    const transporte = require("@nats-io/transport-node");
    const js = require("@nats-io/jetstream");
    const nk = require("@nats-io/nkeys");
    return {
      connect: transporte.connect,
      nkeyAuthenticator: transporte.nkeyAuthenticator,
      nkeys: nk,
      jetstream: js.jetstream,
      jetstreamManager: js.jetstreamManager,
      error: null,
    };
  } catch (e) {
    return { error: e };
  }
}

const N = cargarNats();
if (N.error) {
  console.error(`✗ faltan las dependencias de NATS: ${N.error.message}\n  Ejecuta: (cd conformance && npm install)`);
  process.exit(2);
}
export const { connect, nkeyAuthenticator, nkeys, jetstream, jetstreamManager } = N;

export const enc = new TextEncoder();
export const dec = new TextDecoder();
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ─── Servidores y temporales ─────────────────────────────────────────────────

/**
 * Un banco de pruebas: un directorio temporal, un rango de puertos y los nats-server que se
 * lanzan en él. `limpiar()` mata SOLO los procesos que este banco lanzó (nunca por nombre: en
 * una máquina compartida `pkill nats-server` se llevaría por delante los de otros) y borra el
 * temporal.
 */
export class Banco {
  /**
   * @param {object} o
   * @param {string} o.prefijo     prefijo del directorio temporal (`flux-verify-acl-`)
   * @param {number} o.puertoBase  primer puerto a probar
   * @param {number} o.puertos     cuántos puertos probar desde `puertoBase`
   * @param {string} o.natsServer  binario (por defecto `nats-server` del PATH)
   */
  constructor({ prefijo, puertoBase, puertos = 50, natsServer = "nats-server" }) {
    this.prefijo = prefijo;
    this.puertoBase = puertoBase;
    this.puertos = puertos;
    this.natsServer = natsServer;
    this.tmp = undefined;
    this.hijos = [];
    this.puertosUsados = new Set();
  }

  /** La salida de `nats-server --version`, o `null` si no se puede ejecutar. */
  version() {
    const v = spawnSync(this.natsServer, ["--version"], { encoding: "utf8" });
    return v.error || v.status !== 0 ? null : v.stdout.trim();
  }

  async iniciar() {
    this.tmp = await mkdtemp(path.join(os.tmpdir(), this.prefijo));
    return this.tmp;
  }

  /** Un puerto libre del rango, comprobado con un `bind` real. */
  async puertoLibre() {
    for (let p = this.puertoBase; p < this.puertoBase + this.puertos; p++) {
      if (this.puertosUsados.has(p)) continue;
      const libre = await new Promise((ok) => {
        const s = net.createServer();
        s.once("error", () => ok(false));
        s.listen(p, "127.0.0.1", () => s.close(() => ok(true)));
      });
      if (libre) {
        this.puertosUsados.add(p);
        return p;
      }
    }
    throw new Error(`no hay un puerto libre entre ${this.puertoBase} y ${this.puertoBase + this.puertos - 1}`);
  }

  /**
   * Arranca un nats-server con JetStream que incluye `acl`. Lanza si la config no carga.
   *
   * `top` es configuración de nivel superior que va antes del `include` (TLS…). Se da por
   * listo cuando el log DEL PROPIO servidor dice «Server is ready»: que el puerto responda no
   * basta, podría ser el servidor de otro.
   */
  async arrancar(nombre, acl, { top = "", servidorNombre } = {}) {
    const dir = path.join(this.tmp, nombre);
    await mkdir(dir, { recursive: true });
    const port = await this.puertoLibre();
    await writeFile(path.join(dir, "acl.conf"), acl);
    const conf = path.join(dir, "server.conf");
    await writeFile(
      conf,
      `host: "127.0.0.1"\nport: ${port}\nserver_name: "${servidorNombre ?? `flux-${nombre}`}"\n` +
        `jetstream { store_dir: "${path.join(dir, "js")}" }\n${top}\ninclude "acl.conf"\n`,
    );
    const child = spawn(this.natsServer, ["-c", conf], { stdio: ["ignore", "pipe", "pipe"] });
    const srv = { dir, port, child, nombre, salida: "", url: `nats://127.0.0.1:${port}` };
    child.stdout.on("data", (d) => (srv.salida += d));
    child.stderr.on("data", (d) => (srv.salida += d));
    let murio = false;
    child.once("exit", () => (murio = true));
    this.hijos.push(child);

    for (let i = 0; i < 100; i++) {
      if (murio) throw new Error(`nats-server no cargó la config:\n${srv.salida.trim()}`);
      if (srv.salida.includes("Server is ready")) return srv;
      await sleep(100);
    }
    throw new Error(`nats-server no llegó a estar listo:\n${srv.salida.trim()}`);
  }

  async parar(srv) {
    const child = srv.child ?? srv;
    if (child.exitCode !== null) return;
    child.kill("SIGTERM");
    await Promise.race([new Promise((r) => child.once("exit", r)), sleep(3000)]);
    if (child.exitCode === null) child.kill("SIGKILL");
  }

  async limpiar({ conservar = false } = {}) {
    for (const h of this.hijos) if (h.exitCode === null) h.kill("SIGKILL");
    if (this.tmp && !conservar) await rm(this.tmp, { recursive: true, force: true });
  }

  /**
   * Que una señal o un error no controlado NO dejen nats-server huérfano ni el temporal en
   * disco: antes de este manejador, un fallo del propio script dejaba servidores vivos en el
   * puerto.
   */
  instalarManejadores(alFallar) {
    for (const sig of ["SIGINT", "SIGTERM"]) {
      process.on(sig, async () => {
        await this.limpiar();
        process.exit(130);
      });
    }
    for (const ev of ["uncaughtException", "unhandledRejection"]) {
      process.on(ev, async (e) => {
        alFallar?.(e);
        await this.limpiar();
        process.exit(1);
      });
    }
  }
}

// ─── Identidades ─────────────────────────────────────────────────────────────

export const nuevaClave = () => {
  const kp = nkeys.createUser();
  return { pub: kp.getPublicKey(), seed: kp.getSeed() };
};

/** Un `.creds` con un JWT de relleno: un servidor con nkeys estáticas solo mira la semilla. */
export const credsDe = (seed) =>
  `-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QifQ.e30.relleno\n------END NATS USER JWT------\n\n` +
  `-----BEGIN USER NKEY SEED-----\n${typeof seed === "string" ? seed : dec.decode(seed)}\n------END USER NKEY SEED------\n`;

/**
 * Un usuario nkey con permisos dados, escrito como lo escribe `gen-acl.mjs`. OJO: una lista
 * vacía NO deniega nada —`allow: []` es «sin restricción»— (A-25).
 */
export const usuarioConf = (pub, publish, subscribe, allowResponses) =>
  `      { nkey: "${pub}"\n        permissions: {\n` +
  `          publish: { allow: [${publish.map((s) => `"${s}"`).join(", ")}] }\n` +
  `          subscribe: { allow: [${subscribe.map((s) => `"${s}"`).join(", ")}] }\n` +
  allowResponsesConf(allowResponses) +
  `        } },\n`;

/**
 * La línea `allow_responses` de un usuario, o nada. `true` es la forma desnuda (admite UNA respuesta y NO caduca:
 * A-68); un objeto es `{ max, expires }`, que es lo que genera `gen-acl.mjs` para el dueño de una operación.
 * Ausente o `null`: el usuario no puede responder a nadie.
 */
export function allowResponsesConf(ar) {
  if (!ar) return "";
  const v = ar === true ? "true" : `{ max: ${ar.max ?? 1}${ar.expires ? `, expires: "${ar.expires}"` : ""} }`;
  return `          allow_responses: ${v}\n`;
}

/** Un usuario sin restricciones: el de operación, que aprovisiona. */
export const usuarioOpsConf = (pub) => `      { nkey: "${pub}" },\n`;

/** Inserta usuarios tras la línea `users: [` de la account (la `n`-ésima, desde 0). */
export function conUsuarios(acl, extra, n = 0) {
  const marca = "    users: [";
  const partes = acl.split(marca);
  if (partes.length <= n + 1) throw new Error("la config generada no tiene `users: [` donde se esperaba");
  return partes.slice(0, n + 1).join(marca) + `${marca}\n${extra}` + partes.slice(n + 1).join(marca);
}

// ─── El generador (sin servidor) ─────────────────────────────────────────────

export function gen(...args) {
  const r = spawnSync(process.execPath, [GEN, ...args], { encoding: "utf8", cwd: ROOT });
  return { codigo: r.status, salida: r.stdout, errores: r.stderr };
}

/** Los permisos de TODOS los servicios del ejemplo, tal y como los afirma `--json`. */
export const permisosJson = (...args) => JSON.parse(gen(EJEMPLO, "--json", ...args).salida);

// ─── Conexiones ──────────────────────────────────────────────────────────────

/**
 * Conecta y guarda las violaciones de permiso que el servidor notifica. Una publicación sin
 * respuesta (un ack, un `publish` de core) no tiene otra forma de enterarse de que se denegó:
 * el broker no contesta a una petición denegada, manda `-ERR 'Permissions Violation…'` por la
 * conexión.
 *
 * `tls`: `{ caFile }` para hablar TLS con una CA propia. `etiqueta` prefija el nombre de
 * conexión (sale en el log del servidor).
 */
export async function conectar(url, { nombre, seed, user, pass, inboxPrefix, tls, etiqueta = "flux-acl" }) {
  const violaciones = [];
  const nc = await connect({
    servers: url,
    name: `${etiqueta}:${nombre}`,
    timeout: 4000,
    reconnect: false,
    ...(seed ? { authenticator: nkeyAuthenticator(seed) } : user ? { user, pass } : {}),
    ...(inboxPrefix && { inboxPrefix }),
    ...(tls && { tls }),
  });
  (async () => {
    for await (const s of nc.status()) {
      if (s.type === "error") violaciones.push(String(s.error?.message ?? s.error));
    }
  })();
  nc.violaciones = violaciones;
  return nc;
}

// ─── Utilidades de sondeo ────────────────────────────────────────────────────

export const esViolacion = (e) =>
  /Permissions Violation/i.test(e?.message ?? String(e)) || /Permissions Violation/i.test(e?.cause?.message ?? "");
export const resumen = (e) => (e?.message ?? String(e)).split("\n")[0].slice(0, 140);

export function conTiempo(promesa, ms, que) {
  let t;
  return Promise.race([
    promesa,
    new Promise((_, rj) => {
      t = setTimeout(() => rj(new Error(`sin respuesta en ${ms} ms (${que})`)), ms);
    }),
  ]).finally(() => clearTimeout(t));
}

/**
 * Pide algo al broker y lo clasifica: `denegado` (violación de permiso), `respondido`
 * (llegó una respuesta, buena o de error: el permiso existe) o `otro` (timeout, sin
 * respondedores…: NO es una denegación y no debe contarse como tal).
 */
export async function pedir(nc, subject, cuerpo = {}, timeout = 1500) {
  const t0 = Date.now();
  try {
    const r = await nc.request(subject, enc.encode(typeof cuerpo === "string" ? cuerpo : JSON.stringify(cuerpo)), { timeout });
    let json;
    try {
      json = JSON.parse(dec.decode(r.data));
    } catch {
      json = null;
    }
    return { estado: "respondido", json, error: json?.error?.description, ms: Date.now() - t0 };
  } catch (e) {
    // La denegación llega a veces en `cause` (RequestError) y a veces en el propio mensaje.
    const texto = [...new Set([e?.message, e?.cause?.message].filter(Boolean))].join(" | ") || String(e);
    return esViolacion(e)
      ? { estado: "denegado", detalle: resumen(e), texto, ms: Date.now() - t0 }
      : { estado: "otro", detalle: resumen(e), texto, ms: Date.now() - t0 };
  }
}

/** Publica sin esperar respuesta y mira si el servidor se quejó de ESE subject. */
export async function publicarCrudo(nc, subject) {
  nc.publish(subject, enc.encode("{}"));
  await nc.flush();
  await sleep(250);
  return nc.violaciones.some((v) => v.includes(`Publish to "${subject}"`)) ? "denegado" : "permitido";
}

export async function suscribirCrudo(nc, subject) {
  const sub = nc.subscribe(subject);
  await nc.flush();
  await sleep(250);
  const denegada = nc.violaciones.some((v) => v.includes(`Subscription to "${subject}"`));
  if (denegada) sub.unsubscribe();
  return denegada ? "denegado" : "permitido";
}

export async function publicarJs(nc, subject, cuerpo = "{}") {
  return jetstream(nc, { timeout: 1500 }).publish(subject, enc.encode(cuerpo), { msgID: randomUUID() });
}

/**
 * Anota los mensajes de una suscripción que contengan `marca`. Si el broker DENIEGA la
 * suscripción, su iterador lanza PermissionViolationError: es lo esperado, no un fallo.
 */
export function vigilar(sub, marca, visto) {
  (async () => {
    try {
      for await (const m of sub) if (dec.decode(m.data).includes(marca)) visto.push(m.subject);
    } catch {
      /* suscripción denegada */
    }
  })();
}

/** Recibe UN mensaje de un consumidor pull, o `null` si no llega en `ms`. */
export async function recibirUno(consumidor, ms = 3000) {
  const it = await consumidor.consume({ max_messages: 1, expires: ms });
  const t = setTimeout(() => it.stop(), ms);
  try {
    for await (const m of it) return m;
    return null;
  } finally {
    clearTimeout(t);
    it.stop();
  }
}

// ─── Operación: lo que hace el usuario ops ───────────────────────────────────

export const nanos = (ms) => Number(BigInt(ms) * 1_000_000n);

/**
 * La config canónica de consumidor que pide el SDK en subscribe(), leída de protocol.json:
 * una sola fuente con `flux provision`.
 */
export function configConsumidorCanonica(canon, durable, filtro) {
  return {
    durable_name: durable,
    filter_subject: filtro,
    ack_policy: "explicit",
    ack_wait: nanos(canon.ackWaitSeconds * 1000),
    max_deliver: canon.maxDeliver,
    backoff: canon.backoffMs.map(nanos),
    max_ack_pending: canon.maxAckPending,
    deliver_policy: "all",
    replay_policy: "instant",
  };
}
