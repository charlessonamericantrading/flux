/**
 * El `time` que da quien llama a un constructor — 01-envelope.md §2.2.
 *
 * Los constructores puros de cada SDK reciben `time` de fuera (el arnés de conformidad, un test, quien construya un transporte
 * propio). Copiarlo tal cual dejaría pasar `2026-08-20T10:41:07Z` o `…+02:00`, que **no son los mismos bytes** que la forma
 * canónica y rompen el replay, la firma y la dedupe por hash; y un valor que ni siquiera es un instante («ayer») saldría
 * en el sobre. Por eso se **normaliza** a la forma canónica —exactamente 3 decimales y sufijo `Z`, en UTC— o se **rechaza**.
 *
 * Se admite lo que RFC 3339 llama `date-time` —`T` o `t` (o un espacio) entre la fecha y la hora; `Z` o `z`, o un desfase
 * `±hh:mm`; los decimales son opcionales y de cualquier longitud— y que el SDK de Rust también admite. Los decimales de más de
 * tres se **truncan** (no se redondean). No se admiten: un segundo 60 (un segundo intercalar no existe en UTC de POSIX, y
 * `Date` de JavaScript lo desplazaría al minuto siguiente), un instante cuyo año en UTC cae fuera de 0000–9999, ni un
 * desfase de 24 horas o más.
 */

const RE_INSTANTE =
  /^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(?:[Zz]|([+-])(\d{2}):(\d{2}))$/;

const bisiesto = (y: number): boolean => (y % 4 === 0 && y % 100 !== 0) || y % 400 === 0;
const DIAS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/** La forma canónica de `texto` (`2026-08-20T10:41:07.318Z`), o `null` si no es un instante RFC 3339 válido. */
export function canonicalTime(texto: unknown): string | null {
  if (typeof texto !== "string") return null;
  const m = RE_INSTANTE.exec(texto);
  if (!m) return null;
  const [y, mo, d, h, mi, s] = [m[1], m[2], m[3], m[4], m[5], m[6]].map(Number);
  if (mo < 1 || mo > 12) return null;
  if (d < 1 || d > (mo === 2 && bisiesto(y) ? 29 : DIAS[mo - 1])) return null;
  if (h > 23 || mi > 59 || s > 59) return null;
  const ms = Number(`${m[7] ?? ""}000`.slice(0, 3));
  let desfaseMin = 0;
  if (m[8] !== undefined) {
    const oh = Number(m[9]);
    const om = Number(m[10]);
    if (oh > 23 || om > 59) return null;
    desfaseMin = (m[8] === "-" ? -1 : 1) * (oh * 60 + om);
  }
  const f = new Date(0);
  f.setUTCFullYear(y, mo - 1, d); // `Date.UTC` convertiría los años 0–99 en 1900–1999
  f.setUTCHours(h, mi, s, ms);
  const utc = new Date(f.getTime() - desfaseMin * 60_000);
  const anio = utc.getUTCFullYear();
  if (anio < 0 || anio > 9999) return null;
  return utc.toISOString();
}
