/**
 * Estadística mínima y sin dependencias: mediana, percentiles y rango.
 *
 * El percentil es el de «rango más cercano» (nearest-rank): el valor de la muestra que deja por debajo al
 * `p`% de las demás. No interpola: con pocas muestras un p99 interpolado inventaría un número que nadie
 * observó, y este banco prefiere un valor que EXISTIÓ en la medición.
 */

/** Copia ordenada y sin no-finitos. */
function limpia(xs) {
  return Array.from(xs).filter((x) => Number.isFinite(x)).sort((a, b) => a - b);
}

export function percentil(xs, p) {
  const v = limpia(xs);
  if (v.length === 0) return NaN;
  const i = Math.min(v.length - 1, Math.max(0, Math.ceil((p / 100) * v.length) - 1));
  return v[i];
}

export const mediana = (xs) => {
  const v = limpia(xs);
  if (v.length === 0) return NaN;
  const m = v.length >> 1;
  return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2;
};

export const suma = (xs) => Array.from(xs).reduce((a, b) => a + b, 0);
export const media = (xs) => (xs.length ? suma(xs) / xs.length : NaN);

/** `{ n, min, mediana, max }` de un conjunto de valores (por ejemplo, uno por repetición). */
export function resumir(xs) {
  const v = limpia(xs);
  return { n: v.length, min: v[0] ?? NaN, mediana: mediana(v), max: v[v.length - 1] ?? NaN };
}

/** `{ n, p50, p99, max, media }` de una muestra de latencias. */
export function latencias(xs) {
  const v = limpia(xs);
  return { n: v.length, p50: percentil(v, 50), p99: percentil(v, 99), max: v[v.length - 1] ?? NaN, media: media(v) };
}
