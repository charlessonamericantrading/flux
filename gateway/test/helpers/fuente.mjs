/**
 * De dónde se cargan los módulos del gateway que prueban los tests.
 *
 * Por defecto, `../../src/`. `test/mutantes.mjs` los apunta a una COPIA de `src/` estropeada a propósito
 * (`FLUX_GATEWAY_SRC`) para comprobar que los tests DETECTAN el fallo: una prueba que pasa con el código roto no vale.
 */

import path from "node:path";
import { pathToFileURL } from "node:url";

const AQUI = path.dirname(new URL(import.meta.url).pathname);
export const SRC = process.env.FLUX_GATEWAY_SRC ? path.resolve(process.env.FLUX_GATEWAY_SRC) : path.join(AQUI, "..", "..", "src");

/** `await cargar("auth")` → el módulo `src/auth.mjs` (o el de la copia estropeada). */
export const cargar = (nombre) => import(pathToFileURL(path.join(SRC, `${nombre}.mjs`)).href);
