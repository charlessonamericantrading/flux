/**
 * Un generador de carga de BUCLE CERRADO: `concurrencia` bucles, cada uno lanza su siguiente petición al recibir la respuesta
 * de la anterior. Registra, por petición, cuándo salió y cuánto tardó, en dos `Float64Array` preasignados (guardar objetos
 * en un array crecería la memoria y el GC distorsionaría justo lo que se mide).
 */

import { ahoraMs } from "./proc.mjs";

export function cargaCerrada({ concurrencia, hacerPeticion, capacidad = 1 << 21 }) {
  const inicios = new Float64Array(capacidad);
  const duraciones = new Float64Array(capacidad);
  const c = { hechas: 0, errores: 0, perdidasDeRegistro: 0 };
  let activo = false;
  let bucles = [];

  const bucle = async () => {
    while (activo) {
      const t = ahoraMs();
      try {
        await hacerPeticion();
        const d = ahoraMs() - t;
        if (c.hechas < capacidad) {
          inicios[c.hechas] = t;
          duraciones[c.hechas] = d;
        } else c.perdidasDeRegistro++;
        c.hechas++;
      } catch (e) {
        c.errores++;
        // Un error no debe convertir el bucle en una espera activa que gaste CPU a lo loco
        await new Promise((r) => setTimeout(r, 10));
      }
    }
  };

  return {
    arranca() {
      activo = true;
      bucles = Array.from({ length: concurrencia }, () => bucle());
    },
    async parar() {
      activo = false;
      await Promise.allSettled(bucles);
    },
    snap: () => ({ ...c }),
    /** Todo lo registrado: el runner se queda con las peticiones que SALIERON dentro de su ventana. */
    registros() {
      const n = Math.min(c.hechas, capacidad);
      return { inicios: inicios.slice(0, n), duraciones: duraciones.slice(0, n) };
    },
  };
}
