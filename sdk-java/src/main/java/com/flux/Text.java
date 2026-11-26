/*
 * Como se miden y se cortan los textos del protocolo — la misma regla en todos los SDKs.
 * Contrato normativo: specification/04-errors.md §3 y specification/11-request-reply.md §3.3.
 *
 * Java cuenta UNIDADES UTF-16 ({@code String.length()}): un emoji son dos, y mil emojis
 * «miden» 2000. JSON Schema ({@code maxLength}) cuenta CARACTERES UNICODE (puntos de codigo),
 * y otros SDKs cuentan de otra manera segun lo que midan. Para que dos SDKs acepten y
 * rechacen lo mismo, todo limite de longitud del protocolo se cuenta EN PUNTOS DE CODIGO
 * (el {@code message} de un error de rpc, el {@code code}…), y lo unico que se acota en
 * BYTES es {@code dlqerror} (1024 bytes UTF-8, sin partir un caracter): es un texto que
 * acaba en un almacen con un tope de tamano, no un campo con esquema.
 */
package com.flux;

import java.nio.charset.StandardCharsets;

/** Clase de utilidad: no se instancia. */
final class Text {

    private Text() {
        throw new AssertionError("clase de utilidad");
    }

    /** Longitud en puntos de codigo Unicode: lo que cuenta {@code maxLength} de JSON Schema. */
    static int codePointLength(String s) {
        return s.codePointCount(0, s.length());
    }

    /**
     * Corta a {@code max} puntos de codigo como mucho: si no cabe, {@code max − 1} mas
     * {@code …}. Nunca parte un par suplente (una mitad no es UTF-8 valido).
     */
    static String truncateCodePoints(String s, int max) {
        if (s.length() <= max) {
            return s; // unidades UTF-16 >= puntos de codigo: si las primeras caben, las segundas tambien
        }
        if (codePointLength(s) <= max) {
            return s;
        }
        int end = s.offsetByCodePoints(0, max - 1);
        return s.substring(0, end) + "…";
    }

    /**
     * Corta a {@code maxBytes} bytes UTF-8 como mucho SIN partir un caracter: el corte cae
     * en la frontera del ultimo caracter entero que cabe. Es la regla de {@code dlqerror}
     * (1024 bytes) — 04-errors.md §3.
     *
     * <p>Un suplente suelto se codifica como U+FFFD (3 bytes), que es lo que escribiria
     * {@link String#getBytes(java.nio.charset.Charset)}.
     */
    static String truncateUtf8Bytes(String s, int maxBytes) {
        int bytes = 0;
        int i = 0;
        while (i < s.length()) {
            int cp = s.codePointAt(i);
            int units = Character.charCount(cp);
            int b = cp < 0x80 ? 1 : cp < 0x800 ? 2 : cp < 0x10000 ? 3 : 4;
            if (bytes + b > maxBytes) {
                break;
            }
            bytes += b;
            i += units;
        }
        return s.substring(0, i);
    }

    /** Longitud en bytes UTF-8 de un texto. */
    static int utf8Length(String s) {
        return s.getBytes(StandardCharsets.UTF_8).length;
    }

    /** Un texto que es solo espacio en blanco (White_Space de Unicode) no dice nada. */
    static boolean isBlank(String s) {
        for (int i = 0; i < s.length(); ) {
            int cp = s.codePointAt(i);
            boolean white = cp == 0x09 || cp == 0x0A || cp == 0x0B || cp == 0x0C || cp == 0x0D
                    || cp == 0x20 || cp == 0x85 || cp == 0xA0 || cp == 0x1680
                    || (cp >= 0x2000 && cp <= 0x200A) || cp == 0x2028 || cp == 0x2029
                    || cp == 0x202F || cp == 0x205F || cp == 0x3000;
            if (!white) {
                return false;
            }
            i += Character.charCount(cp);
        }
        return true;
    }
}
