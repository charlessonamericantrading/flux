/**
 * El registro del gateway: líneas JSON, solo con LISTA BLANCA de campos. Implementación: specification/12-edge.md §7.3.
 *
 * El gateway no registra NUNCA `data`, de ningún evento y de ninguna clasificación (en particular, nunca el de un
 * `confidential`): lo que entra a un log acaba en sistemas con otra retención y otros lectores. Tampoco el token, la cabecera
 * `Authorization`, los secretos ni el cuerpo de la respuesta de un receptor. Una LISTA BLANCA —en lugar de una lista negra de lo
 * que no se debe escribir— hace que un campo nuevo no se cuele por olvido: lo que no está aquí, no sale.
 */

const PERMITIDOS = new Set([
  "cliente", "ruta", "metodo", "estado", "ms", "id", "subject", "operacion", "contrato", "hook", "code", "clase", "intento",
  "host", "tenant", "motivo", "puerto", "clientes", "conectados", "tokenId", "outcome", "aplicado", "revision", "bytes",
]);

/** Corta un texto largo: un mensaje no debe llenar el log, ni arrastrar un valor que se coló en él. */
const corto = (v) => (typeof v === "string" && v.length > 300 ? `${v.slice(0, 297)}…` : v);

/**
 * @param {{ escribir?: (linea: string) => void, nivel?: "debug"|"info"|"warn"|"error" }} [o]
 */
export function crearRegistro({ escribir = (l) => process.stdout.write(`${l}\n`), nivel = "info" } = {}) {
  const orden = { debug: 10, info: 20, warn: 30, error: 40 };
  const emitir = (n, mensaje, campos) => {
    if (orden[n] < orden[nivel]) return;
    const fila = { ts: new Date().toISOString(), nivel: n, mensaje: corto(String(mensaje)) };
    for (const [k, v] of Object.entries(campos ?? {})) if (PERMITIDOS.has(k) && v !== undefined) fila[k] = corto(v);
    escribir(JSON.stringify(fila));
  };
  return {
    debug: (m, c) => emitir("debug", m, c),
    info: (m, c) => emitir("info", m, c),
    warn: (m, c) => emitir("warn", m, c),
    error: (m, c) => emitir("error", m, c),
    /** Para el `logger` del SDK (`warn` y `error`): sus mensajes los escribe el SDK y no llevan `data` (04-errors.md §5.2). */
    paraElSdk: () => ({ warn: (m) => emitir("warn", m), error: (m) => emitir("error", m) }),
  };
}
