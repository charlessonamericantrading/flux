/*
 * El {@code time} que da quien llama a un constructor — 01-envelope.md §2.2.
 *
 * Los constructores puros de cada SDK reciben {@code time} de fuera (el arnes de
 * conformidad, un test, quien construya un transporte propio). Copiarlo tal cual dejaria
 * pasar {@code 2026-08-20T10:41:07Z} o {@code …+02:00}, que NO son los mismos bytes que la
 * forma canonica y rompen el replay, la firma y la dedupe por hash; y un valor que ni
 * siquiera es un instante («ayer») saldria en el sobre. Por eso se NORMALIZA a la forma
 * canonica —exactamente 3 decimales y sufijo {@code Z}, en UTC— o se RECHAZA.
 *
 * Se admite lo que RFC 3339 llama {@code date-time} —{@code T} o {@code t} (o un espacio)
 * entre la fecha y la hora; {@code Z} o {@code z}, o un desfase {@code ±hh:mm}; los
 * decimales son opcionales y de cualquier longitud— y lo mismo que admiten los SDKs de
 * Node y de Rust. Los decimales de mas de tres se TRUNCAN (no se redondean). No se
 * admiten: un segundo 60, un instante cuyo ano en UTC cae fuera de 0000–9999, ni un
 * desfase de 24 horas o mas.
 */
package com.flux;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Clase de utilidad: no se instancia. */
final class CanonicalTime {

    private CanonicalTime() {
        throw new AssertionError("clase de utilidad");
    }

    // `\d` de Java es solo ASCII por defecto (sin UNICODE_CHARACTER_CLASS): ni digitos arabes ni de ancho completo.
    private static final Pattern INSTANT = Pattern.compile(
            "^(\\d{4})-(\\d{2})-(\\d{2})[Tt ](\\d{2}):(\\d{2}):(\\d{2})(?:\\.(\\d+))?"
                    + "(?:[Zz]|([+-])(\\d{2}):(\\d{2}))$");

    /** La forma canonica de {@code text}, o {@code null} si no es un instante RFC 3339 valido. */
    static String canonical(String text) {
        Instant instant = instant(text);
        return instant == null ? null : Envelope.formatTime(instant);
    }

    /** El instante de {@code text}, o {@code null} si no es un {@code date-time} RFC 3339 valido. */
    static Instant instant(String text) {
        if (text == null) {
            return null;
        }
        // `matches()` y no `find()` con `$`: en Java `$` acepta un `\n` final.
        Matcher m = INSTANT.matcher(text);
        if (!m.matches()) {
            return null;
        }
        int year = Integer.parseInt(m.group(1));
        int month = Integer.parseInt(m.group(2));
        int day = Integer.parseInt(m.group(3));
        int hour = Integer.parseInt(m.group(4));
        int minute = Integer.parseInt(m.group(5));
        int second = Integer.parseInt(m.group(6));
        if (hour > 23 || minute > 59 || second > 59) {
            return null;
        }
        // Los decimales de mas de tres se TRUNCAN: se toman los tres primeros, rellenando con ceros.
        String frac = m.group(7) == null ? "" : m.group(7);
        int millis = Integer.parseInt((frac + "000").substring(0, 3));
        int offsetSeconds = 0;
        if (m.group(8) != null) {
            int oh = Integer.parseInt(m.group(9));
            int om = Integer.parseInt(m.group(10));
            if (oh > 23 || om > 59) {
                return null;
            }
            offsetSeconds = (m.group(8).equals("-") ? -1 : 1) * (oh * 3600 + om * 60);
        }
        try {
            LocalDate date = LocalDate.of(year, month, day);
            LocalDateTime local = date.atTime(hour, minute, second, millis * 1_000_000);
            Instant utc = local.toInstant(ZoneOffset.ofTotalSeconds(offsetSeconds));
            int utcYear = utc.atOffset(ZoneOffset.UTC).getYear();
            if (utcYear < 0 || utcYear > 9999) {
                return null;
            }
            return utc;
        } catch (DateTimeException e) {
            return null;
        }
    }
}
