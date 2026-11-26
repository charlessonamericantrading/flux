/**
 * Hace que `import("@opentelemetry/api")` RESUELVA, con un módulo mínimo sin ningún span activo: lo que ocurre en un servicio que
 * SÍ tiene `@opentelemetry/api` instalado y ningún span en curso. Sirve al EXPERIMENTO del escenario C («flux con OpenTelemetry
 * presente», fila A-170 del Apéndice A). No toca ningún fichero del SDK.
 *
 * Se usa `registerHooks` (los ganchos corren en el mismo hilo, de forma síncrona) y no `register`: este último ejecuta los ganchos en
 * OTRO hilo y cada `import()` pagaría un viaje entre hilos, con lo que el experimento mediría su propio estorbo.
 */
import { registerHooks } from "node:module";

const STUB = "data:text/javascript,export const trace = { getActiveSpan() { return undefined; } };";

registerHooks({
  resolve(especificador, contexto, siguiente) {
    if (especificador === "@opentelemetry/api") return { url: STUB, shortCircuit: true, format: "module" };
    return siguiente(especificador, contexto);
  },
});
