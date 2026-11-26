/**
 * Una «base de datos» de juguete: un fichero JSON con escritura atómica. SUSTITUYE a la base de datos de cada servicio, y solo a eso:
 * en tu sistema esto es una tabla, y `transaccion()` es una transacción de verdad.
 *
 * Lo que importa de ella para el ejemplo es la propiedad que usa la idempotencia: **todo o nada**. Lo que escribe `transaccion(fn)`
 * —la fila de la factura Y la marca «este evento ya está procesado»— se guarda junto o no se guarda (se escribe a un fichero
 * temporal y se renombra: o está el fichero viejo o el nuevo, nunca uno a medias). Con ello, un servicio que se cae en mitad de un
 * handler y reinicia no pierde la marca de lo que ya hizo, que es justo lo que una `Set` en memoria perdería.
 */

import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import path from "node:path";

/**
 * @param {string} ruta     el fichero (se crea, con su directorio, si no existe)
 * @param {object} inicial  lo que contiene la primera vez
 */
export function abrirAlmacen(ruta, inicial) {
  mkdirSync(path.dirname(ruta), { recursive: true });
  let datos = existsSync(ruta) ? JSON.parse(readFileSync(ruta, "utf8")) : structuredClone(inicial);

  const guardar = () => {
    const tmp = `${ruta}.tmp`;
    writeFileSync(tmp, `${JSON.stringify(datos, null, 1)}\n`);
    renameSync(tmp, ruta);
  };
  if (!existsSync(ruta)) guardar();

  return {
    ruta,
    /** Solo lectura: no lo modifiques, usa `transaccion`. */
    leer: () => datos,
    /** Aplica `fn` a una copia y, si no lanza, la guarda entera. Si lanza, no se escribe nada. */
    transaccion(fn) {
      const copia = structuredClone(datos);
      const resultado = fn(copia);
      datos = copia;
      guardar();
      return resultado;
    },
  };
}
