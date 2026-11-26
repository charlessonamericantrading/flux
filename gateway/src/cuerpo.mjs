/**
 * Leer el cuerpo de una petición CON LÍMITE. Implementación: specification/12-edge.md §7.1.
 *
 * Medido (A-152): si se responde `413` sin más, Node LEE Y DESCARTA todo el cuerpo declarado (52 428 800 bytes con un
 * `Content-Length` de 50 MiB) antes de cerrar. Por eso un cuerpo demasiado grande se rechaza ANTES de leerlo cuando se
 * declara, y al cruzar el límite cuando no (chunked), y quien responde cierra la conexión (`Connection: close` y `destroy`).
 */

export class CuerpoDemasiadoGrande extends Error {
  constructor(limite, declarado) {
    super(`el cuerpo supera el límite de ${limite} bytes${declarado ? ` (declara ${declarado})` : ""}`);
    this.name = "CuerpoDemasiadoGrande";
    this.limite = limite;
  }
}

export class ClienteAbandono extends Error {
  constructor() {
    super("el cliente cerró la conexión antes de terminar de enviar el cuerpo");
    this.name = "ClienteAbandono";
  }
}

/**
 * @param {import("node:http").IncomingMessage} req
 * @param {number} limite  bytes
 * @returns {Promise<Buffer>}
 */
export function leerCuerpo(req, limite) {
  const declarado = req.headers["content-length"];
  if (declarado !== undefined && Number(declarado) > limite) return Promise.reject(new CuerpoDemasiadoGrande(limite, Number(declarado)));

  return new Promise((resolve, reject) => {
    const trozos = [];
    let total = 0;
    let terminado = false;
    const fin = (fn, valor) => {
      if (terminado) return;
      terminado = true;
      req.off("data", alDato);
      fn(valor);
    };
    const alDato = (t) => {
      total += t.length;
      if (total > limite) return fin(reject, new CuerpoDemasiadoGrande(limite));
      trozos.push(t);
    };
    req.on("data", alDato);
    req.once("end", () => fin(resolve, Buffer.concat(trozos, total)));
    req.once("error", () => fin(reject, new ClienteAbandono()));
    req.once("aborted", () => fin(reject, new ClienteAbandono()));
    req.once("close", () => fin(reject, new ClienteAbandono()));
  });
}
