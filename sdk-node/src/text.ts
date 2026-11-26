/**
 * Cómo se miden y se cortan los textos del protocolo — la misma regla en todos los SDKs.
 *
 * JavaScript cuenta UNIDADES UTF-16 (`s.length`): un emoji son dos, y mil emojis «miden» 2000. JSON Schema
 * (`maxLength`) cuenta **caracteres Unicode** (puntos de código), y Rust cuenta de otra manera según lo que mida. Para
 * que dos SDKs acepten y rechacen lo mismo, todo límite de longitud del protocolo se cuenta **en puntos de código**
 * (`message` de un error de rpc, `code`…), y lo único que se acota en **bytes** es `dlqerror` (1024 bytes UTF-8, sin
 * partir un carácter): es un texto que acaba en un almacén con un tope de tamaño, no un campo con esquema.
 * (02-naming.md y 04-errors.md §3; vectores `ajustes-*` de conformance/harness/vectors.json.)
 */

/** Longitud en puntos de código Unicode: lo que cuenta `maxLength` de JSON Schema. Un sustituto suelto cuenta como uno. */
export function codePointLength(s: string): number {
  let n = 0;
  for (const _ of s) n++;
  return n;
}

/**
 * Corta a `max` puntos de código como mucho: si no cabe, `max − 1` más `…`. Nunca parte un par sustituto (una mitad se
 * serializaría como `\ud83d`, que no es UTF-8 válido).
 */
export function truncateCodePoints(s: string, max: number): string {
  if (s.length <= max) return s; // unidades UTF-16 ≥ puntos de código: si las primeras caben, las segundas también
  if (codePointLength(s) <= max) return s;
  let i = 0;
  let cuenta = 0;
  for (const ch of s) {
    if (cuenta === max - 1) break;
    i += ch.length;
    cuenta++;
  }
  return `${s.slice(0, i)}…`;
}

/**
 * Corta a `maxBytes` bytes UTF-8 como mucho **sin partir un carácter**: el corte cae en la frontera del último carácter
 * entero que cabe. Es la regla de `dlqerror` (1024 bytes), la misma que aplica `truncate_chars` de Rust.
 */
export function truncateUtf8Bytes(s: string, maxBytes: number): string {
  let bytes = 0;
  let i = 0;
  for (const ch of s) {
    const cp = ch.codePointAt(0) as number;
    // Un sustituto suelto se codifica como U+FFFD (3 bytes): es lo que escribiría `TextEncoder`.
    const b = cp < 0x80 ? 1 : cp < 0x800 ? 2 : cp < 0x10000 ? 3 : 4;
    if (bytes + b > maxBytes) break;
    bytes += b;
    i += ch.length;
  }
  return s.slice(0, i);
}
