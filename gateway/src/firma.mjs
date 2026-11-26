/**
 * `Flux-Signature`: la firma de la entrega saliente y su verificación.
 * Contrato NORMATIVO: specification/12-edge.md §4.2.
 *
 *   Flux-Signature: t=<unix>,v1=<hex>[,v1=<hex>…]
 *   v1 = HMAC-SHA256( secreto, t + "." + cuerpo ), en hexadecimal minúsculas
 *
 * Este módulo lo usan dos lados: el gateway FIRMA con `firmar`, y los receptores —incluidos los de las pruebas y el ejemplo del
 * README— VERIFICAN con `verificar`. Es deliberadamente corto: lo que un receptor en otro lenguaje tiene que reescribir.
 *
 * Reglas que cada línea cumple (y las pruebas rompen a propósito, con mutantes):
 *   · `t` es el del momento de ESTE intento, y el cuerpo no cambia entre intentos.
 *   · Varios `v1` para rotar secretos sin corte; los campos que no se conocen se ignoran.
 *   · Se verifica sobre los BYTES del cuerpo, con el formato hexadecimal validado ANTES de decodificarlo (en Node,
 *     `Buffer.from("abc", "hex")` da un byte sin error: A-154) y comparando en TIEMPO CONSTANTE.
 *   · La tolerancia es simétrica y se comprueba DESPUÉS de la firma: una firma que no verifica nunca dice «caducada».
 */

import crypto from "node:crypto";

const HEX64 = /^[0-9a-f]{64}$/;
const ENTERO = /^[0-9]{1,15}$/;

/** HMAC-SHA256 en hexadecimal de `t.cuerpo` (con `t === null`: solo el cuerpo, el formato de GitHub). */
export function hmacHex(secreto, t, cuerpo) {
  const h = crypto.createHmac("sha256", secreto);
  if (t !== null && t !== undefined) h.update(`${t}.`, "utf8");
  return h.update(cuerpo).digest("hex");
}

/**
 * La cabecera `Flux-Signature`.
 * @param {object} o
 * @param {string[]} o.secretos  los secretos VIGENTES: uno por cada `v1` (rotación sin corte)
 * @param {number} o.t           segundos Unix del momento de este intento
 * @param {Buffer|string} o.cuerpo  los bytes que se van a enviar, exactamente
 */
export function firmar({ secretos, t, cuerpo }) {
  if (!Array.isArray(secretos) || secretos.length === 0) throw new Error("firmar necesita al menos un secreto");
  if (!Number.isInteger(t) || t < 0) throw new Error("`t` debe ser un entero de segundos Unix");
  return `t=${t},${secretos.map((s) => `v1=${hmacHex(s, t, cuerpo)}`).join(",")}`;
}

/** Compara dos hexadecimales YA validados (64 caracteres) en tiempo constante. */
function iguales(a, b) {
  return crypto.timingSafeEqual(Buffer.from(a, "hex"), Buffer.from(b, "hex"));
}

/**
 * Verifica una cabecera `Flux-Signature`. Nunca lanza.
 * @returns {{ ok: true, t: number } | { ok: false, motivo: "ausente"|"formato"|"firma"|"tolerancia", t?: number }}
 */
export function verificar({ cabecera, cuerpo, secretos, ahoraMs = Date.now(), toleranciaSeg = 300 }) {
  if (typeof cabecera !== "string" || cabecera === "") return { ok: false, motivo: "ausente" };

  let t;
  const v1 = [];
  let ts = 0;
  for (const campo of cabecera.split(",")) {
    const i = campo.indexOf("=");
    if (i < 1) return { ok: false, motivo: "formato" };
    const k = campo.slice(0, i).trim();
    const v = campo.slice(i + 1).trim();
    if (k === "t") {
      ts++;
      t = v;
    } else if (k === "v1") {
      // El formato se valida ANTES de decodificar: un hexadecimal impar o inválido se ignoraría en silencio (A-154).
      if (HEX64.test(v)) v1.push(v);
    }
    // cualquier otro campo (`v2=…`) se IGNORA: el esquema puede crecer
  }
  if (ts !== 1 || !ENTERO.test(t ?? "") || v1.length === 0) return { ok: false, motivo: "formato" };

  // Se comparan TODAS las parejas (secreto × v1) sin salir en la primera: el tiempo no depende de cuál acierta.
  let acierta = false;
  for (const secreto of secretos) {
    const esperado = hmacHex(secreto, t, cuerpo);
    for (const recibido of v1) if (iguales(esperado, recibido)) acierta = true;
  }
  const segundos = Number(t);
  if (!acierta) return { ok: false, motivo: "firma" };

  // La tolerancia es SIMÉTRICA: un `t` del futuro también se rechaza. Sin ella, una petición capturada sirve para siempre.
  if (Math.abs(ahoraMs / 1000 - segundos) > toleranciaSeg) return { ok: false, motivo: "tolerancia", t: segundos };
  return { ok: true, t: segundos };
}

/**
 * La firma de un webhook de un TERCERO que no usa `Flux-Signature` (12-edge.md §6.3): el valor de su cabecera es el HMAC en
 * hexadecimal (`hex`) o con el prefijo `sha256=` (`sha256-prefix`, como GitHub), sobre el cuerpo en bruto.
 * @returns {boolean}
 */
export function verificarHmacSimple({ valor, formato, cuerpo, secretos }) {
  if (typeof valor !== "string") return false;
  const hex = formato === "sha256-prefix" ? (valor.startsWith("sha256=") ? valor.slice(7) : "") : valor;
  if (!HEX64.test(hex)) return false;
  let acierta = false;
  for (const secreto of secretos) if (iguales(hmacHex(secreto, null, cuerpo), hex)) acierta = true;
  return acierta;
}
