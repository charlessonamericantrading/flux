/**
 * La autenticación por token del gateway. Implementación: specification/12-edge.md §6.2.
 *
 * El gateway guarda SOLO el SHA-256 del token y compara en TIEMPO CONSTANTE:
 *   · se calcula el SHA-256 del token recibido y se comparan los 32 bytes con `timingSafeEqual`. Así las longitudes son
 *     siempre iguales (con longitudes distintas `timingSafeEqual` LANZA: A-154) y el tiempo no depende de cuánto del token
 *     acertó quien lo prueba;
 *   · se recorren TODAS las entradas, sin salir en la primera que acierta: el tiempo tampoco dice cuántos clientes hay ni cuál
 *     es el suyo;
 *   · un token ausente y uno desconocido dan lo MISMO.
 *
 * Un token son 32 bytes aleatorios (`flux-gateway token`): tiene la entropía de una clave, no de una contraseña, y por eso
 * basta un SHA-256 sin sal ni coste.
 */

import crypto from "node:crypto";

const BEARER = /^Bearer[ \t]+([A-Za-z0-9._~+\/=-]{16,512})$/i;
const HEX64 = /^[0-9a-f]{64}$/;

/** El SHA-256 de un token, en hexadecimal: lo que va a `gateway.json`. */
export const sha256Hex = (token) => crypto.createHash("sha256").update(token, "utf8").digest("hex");

/** Un token nuevo: `flx_` + 32 bytes aleatorios en base64url. */
export const nuevoToken = () => `flx_${crypto.randomBytes(32).toString("base64url")}`;

/**
 * @param {Array<{ tokenId: string, sha256: string, cliente: string }>} tokens  los digests de la configuración
 * @returns {{ autenticar(authorization: string|undefined): { cliente: string, tokenId: string } | null }}
 */
export function crearAutenticador(tokens) {
  const tabla = tokens.map((t) => {
    if (!HEX64.test(t.sha256)) throw new Error(`el token «${t.tokenId}» no es un SHA-256 en hexadecimal de 64 caracteres`);
    return { tokenId: t.tokenId, cliente: t.cliente, digest: Buffer.from(t.sha256, "hex") };
  });
  return {
    autenticar(authorization) {
      const m = typeof authorization === "string" ? BEARER.exec(authorization.trim()) : null;
      // Sin token válido en la forma, se hace igualmente la misma cuenta: el tiempo no dice si había cabecera.
      const presentado = crypto.createHash("sha256").update(m ? m[1] : "", "utf8").digest();
      let acierto = null;
      for (const t of tabla) {
        if (crypto.timingSafeEqual(presentado, t.digest) && m) acierto = acierto ?? t;
      }
      return acierto ? { cliente: acierto.cliente, tokenId: acierto.tokenId } : null;
    },
  };
}
