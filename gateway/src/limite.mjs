/**
 * El límite de peticiones por token: un cubo de tokens por clave, en memoria. Implementación: specification/12-edge.md §7.1.
 *
 * `requests` por `perSeconds`, con una ráfaga de `burst` (por defecto, `requests`). Es POR INSTANCIA: con N instancias del gateway,
 * N veces el límite. Acota a un cliente legítimo que se desboca; no protege de un ataque (es trabajo del proxy de delante).
 * Las claves son los `tokenId` de la configuración y los nombres de los hooks: un conjunto acotado, así que el mapa no crece.
 */

export class LimitePorClave {
  /**
   * @param {{ requests: number, perSeconds: number, burst?: number }} def
   * @param {() => number} [reloj]  milisegundos monótonos (inyectable en las pruebas)
   */
  constructor(def, reloj = () => performance.now()) {
    this.capacidad = def.burst ?? def.requests;
    this.porMs = def.requests / (def.perSeconds * 1000);
    this.reloj = reloj;
    this.cubos = new Map();
  }

  /** Intenta tomar un token. `{ ok: true }` o `{ ok: false, retryAfterMs }`. */
  tomar(clave) {
    const ahora = this.reloj();
    let c = this.cubos.get(clave);
    if (!c) {
      c = { tokens: this.capacidad, ultimo: ahora };
      this.cubos.set(clave, c);
    }
    c.tokens = Math.min(this.capacidad, c.tokens + (ahora - c.ultimo) * this.porMs);
    c.ultimo = ahora;
    if (c.tokens >= 1) {
      c.tokens -= 1;
      return { ok: true };
    }
    return { ok: false, retryAfterMs: Math.ceil((1 - c.tokens) / this.porMs) };
  }
}
