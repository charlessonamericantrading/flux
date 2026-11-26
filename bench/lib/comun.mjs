/**
 * Piezas que comparten los roles: un generador pseudoaleatorio con semilla, la programación de los cambios
 * (o hechos) del productor, los contadores de un servidor HTTP y una URL del SDK de Node.
 */

import http from "node:http";
import https from "node:https";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { ahoraMs } from "./proc.mjs";

export const RAIZ = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
/** El SDK de Node COMPILADO (`npm run build` en sdk-node): el banco mide lo que se publica, no el TypeScript. */
export const sdkUrl = () => pathToFileURL(path.join(RAIZ, "sdk-node", "dist", "index.js")).href;

/** mulberry32: un PRNG de 32 bits con semilla. Mismas semillas, mismos instantes de cambio y mismas fases de sondeo. */
export function prng(semilla) {
  let a = semilla >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * Programa `alCambio()` a `porMinuto` veces por minuto: el intervalo medio es `60000 / porMinuto` ms, con un jitter
 * uniforme de ±50 % (sin él, un intervalo que divida al periodo de sondeo se sincronizaría con él y falsearía el
 * «tiempo hasta enterarse»), y el primero en un instante uniforme dentro del primer intervalo.
 *
 * `alCambio` puede ser asíncrona; el siguiente se programa al TERMINAR el anterior (un productor lento no se
 * solapa consigo mismo). Devuelve `parar()`.
 */
export function programar(porMinuto, semilla, alCambio) {
  const rnd = prng(semilla);
  const medio = 60_000 / porMinuto;
  let parado = false;
  let timer;
  const siguiente = (primero) => {
    const espera = primero ? rnd() * medio : medio * (0.5 + rnd());
    timer = setTimeout(async () => {
      if (parado) return;
      try {
        await alCambio();
      } catch (e) {
        console.error("[bench] el productor falló:", e?.message ?? e);
      }
      if (!parado) siguiente(false);
    }, espera);
  };
  siguiente(true);
  return () => {
    parado = true;
    clearTimeout(timer);
  };
}

/**
 * Cuenta lo que pasa por un servidor HTTP: conexiones TCP aceptadas y abiertas, y los bytes de APLICACIÓN
 * (`bytesRead + bytesWritten` de cada socket, los de las conexiones ya cerradas incluidos).
 * `conexiones` es la prueba de que el keep-alive es de verdad: con él son las de los clientes; sin él, una por petición.
 */
export function contarConexiones(servidor, evento = "connection") {
  const vivos = new Set();
  let cerradoBytes = 0;
  let conexiones = 0;
  servidor.on(evento, (s) => {
    conexiones++;
    vivos.add(s);
    s.once("close", () => {
      vivos.delete(s);
      cerradoBytes += s.bytesRead + s.bytesWritten;
    });
  });
  return () => {
    let bytes = cerradoBytes;
    for (const s of vivos) bytes += s.bytesRead + s.bytesWritten;
    return { bytes, conexiones, g_abiertas: vivos.size };
  };
}

/** Un servidor `http` o `https` (si hay `tls`), con los tiempos de keep-alive de un despliegue razonable. */
export function crearServidor(tls, manejador, { keepAliveS = 120 } = {}) {
  const s = tls
    ? https.createServer({ cert: readFileSync(tls.cert), key: readFileSync(tls.key) }, manejador)
    : http.createServer(manejador);
  // El de Node es de 5 s: una conexión de un sondeo cada 30 s se cerraría entre dos sondeos y cada uno pagaría un
  // handshake. Una línea base JUSTA mantiene viva la conexión (nginx: 75 s por defecto).
  s.keepAliveTimeout = keepAliveS * 1000;
  s.headersTimeout = (keepAliveS + 5) * 1000;
  s.requestTimeout = 0;
  return s;
}

export function escuchar(servidor, host, puerto) {
  return new Promise((ok, rj) => {
    servidor.once("error", rj);
    servidor.listen(puerto, host, () => {
      servidor.removeListener("error", rj);
      ok();
    });
  });
}

/** Un `Agent` con keep-alive y una sola conexión por destino: el cliente de UN servicio. */
export function agente(tls, { keepAlive = true, maxSockets = 1 } = {}) {
  const o = { keepAlive, maxSockets, ...(tls && { ca: readFileSync(tls.ca) }) };
  return tls ? new https.Agent(o) : new http.Agent(o);
}
export const cliente = (tls) => (tls ? https : http);

/** `GET`/`POST` con `node:http(s)`: resuelve `{ status, cuerpo, ms }` con el cuerpo YA leído entero. */
export function peticion(modulo, opciones, cuerpo) {
  return new Promise((ok, rj) => {
    const req = modulo.request(opciones, (res) => {
      const partes = [];
      res.on("data", (c) => partes.push(c));
      res.once("end", () => ok({ status: res.statusCode, cuerpo: Buffer.concat(partes), cabeceras: res.headers, fin: ahoraMs() }));
      res.once("error", rj);
    });
    req.once("error", rj);
    if (cuerpo !== undefined) req.write(cuerpo);
    req.end();
  });
}
