/**
 * Qué se imprime del `data` de un evento: una terminal acaba en un log.
 *
 * `tail` y `dlq inspect` ya prometían no volcar el payload de lo `confidential` o `restricted`
 * (README, «Datos sensibles»). El estado lo hace urgente: **`confidential` es la clasificación
 * por defecto de un `put`** (10-state.md §3.6) y es donde el dato personal tiene permiso para
 * vivir, así que una CLI que lo volcara por defecto sería la fuga que el protocolo existe para
 * evitar.
 *
 * Falla CERRADO: solo se muestra lo `public` o `internal`. Un sobre sin `dataclassification`
 * es POISON (01-envelope.md §3.1) y no se sabe qué lleva dentro; la spec rechaza «asumir
 * `internal`» por esa misma razón.
 */

const VISIBLES = new Set(["public", "internal"]);

export const datosOcultos = (clasificacion) => !VISIBLES.has(clasificacion);

export const marcaOculto = (clasificacion) =>
  `[oculto: ${clasificacion ? `dataclassification=${clasificacion}` : "sin dataclassification"}]`;

/**
 * Copia del sobre con `data` sustituido por la marca si no puede mostrarse. El orden de las
 * claves se conserva (`data` sigue siendo la última): la salida `--json` sigue teniendo la
 * forma de un sobre.
 */
export function redactarSobre(e, showData) {
  if (showData || !e || typeof e !== "object" || !("data" in e)) return e;
  if (!datosOcultos(e.dataclassification)) return e;
  return { ...e, data: marcaOculto(e.dataclassification) };
}
