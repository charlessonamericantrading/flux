/*
 * El {@code id} DETERMINISTA de un hecho importado de un sistema externo.
 * Contrato normativo: specification/01-envelope.md §2.6.1.
 *
 * Un proveedor (una pasarela de pagos, un sistema de terceros) reenvia el mismo aviso durante horas.
 * Si cada reenvio recibe un {@code id} nuevo, ningun consumidor puede deduplicarlo: el {@code id} es
 * la clave de deduplicacion del ecosistema. Para un hecho importado, el {@code id} se DERIVA de lo
 * que identifica al hecho, y es siempre el mismo:
 *
 *   id = UUIDv7 cuyos 48 bits de tiempo son los milisegundos del {@code time} del hecho externo y
 *        cuyos 74 bits restantes ({@code rand_a} y {@code rand_b}) son los 74 primeros bits de
 *        SHA-256( <source> "|" <clave externa> ).
 *
 * Es un UUIDv7 VALIDO (version 7, variante 10): cualquier consumidor que valide el formato del
 * {@code id} lo acepta, y ordenar por {@code id} dentro de un {@code source} sigue siendo ordenar
 * por el instante del hecho. Es una funcion PURA: no usa el reloj ni ninguna fuente de azar, y es lo
 * que el arnes cruzado compara byte a byte entre SDKs.
 */
package com.flux;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;

/** Clase de utilidad: no se instancia. */
public final class ExternalId {

    private ExternalId() {
        throw new AssertionError("clase de utilidad");
    }

    /** 2^48 − 1: lo que cabe en el campo de tiempo de un UUIDv7. */
    public static final long UUIDV7_MAX_MS = (1L << 48) - 1;

    /** Longitud maxima de una clave externa, en bytes UTF-8: es un identificador, no un documento. */
    public static final int EXTERNAL_KEY_MAX_BYTES = 512;

    /** Los milisegundos de un instante, o {@link Envelope.EnvelopeException} si no caben en 48 bits. */
    static long instantToMs(Instant time) {
        long ms;
        try {
            ms = time.toEpochMilli();
        } catch (ArithmeticException e) {
            ms = -1;
        }
        if (ms < 0 || ms > UUIDV7_MAX_MS) {
            throw new Envelope.EnvelopeException(
                    "`time` no es un instante valido para el id determinista (se necesitan milisegundos "
                            + "enteros entre 0 y " + UUIDV7_MAX_MS + "). Pasa el instante del hecho externo "
                            + "(01-envelope.md §2.6.1)");
        }
        return ms;
    }

    /**
     * {@code SHA-256(<source> "|" <externalKey>)}, y de ahi el UUIDv7. Falla con
     * {@link Envelope.EnvelopeException} ante una clave vacia o demasiado larga, un {@code source}
     * con {@code |}, o un instante ausente o que no cabe en 48 bits.
     *
     * @param time el instante del hecho EXTERNO, el que da el sistema de origen, NO «ahora»: es lo que
     *             hace estable el {@code id}. Obligatorio.
     */
    public static String eventId(String source, String externalKey, Instant time) {
        if (source == null || source.isEmpty() || source.contains("|")) {
            throw new Envelope.EnvelopeException("`source` debe ser una cadena no vacia y sin `|` (es el "
                    + "separador de la derivacion del id determinista)");
        }
        if (externalKey == null || externalKey.isEmpty()) {
            throw new Envelope.EnvelopeException("`externalKey` debe ser una cadena no vacia: es la clave del "
                    + "hecho en el sistema externo (el id del evento del proveedor). Sin ella no hay nada de "
                    + "lo que derivar un `id` estable (01-envelope.md §2.6.1)");
        }
        if (externalKey.getBytes(StandardCharsets.UTF_8).length > EXTERNAL_KEY_MAX_BYTES) {
            throw new Envelope.EnvelopeException("`externalKey` ocupa mas de " + EXTERNAL_KEY_MAX_BYTES
                    + " bytes UTF-8: es un identificador, no un documento. Usa el id del hecho en el sistema "
                    + "externo");
        }
        if (time == null) {
            throw new Envelope.EnvelopeException("con `externalKey` hace falta `time`: el instante del hecho "
                    + "EXTERNO. Si el sistema externo no da un instante no hay `id` determinista: con «ahora» "
                    + "cada reenvio tendria el suyo (01-envelope.md §2.6.1)");
        }
        long ms = instantToMs(time);

        byte[] digest;
        try {
            // UTF-8 literal: Java usa UTF-16 por dentro, y un hash sobre otra codificacion daria otro `id`.
            digest = MessageDigest.getInstance("SHA-256").digest((source + "|" + externalKey).getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 no esta disponible en esta JVM", e);
        }
        // Los 74 primeros bits del digest: 80 bits (10 bytes) menos los 6 ultimos.
        BigInteger r = new BigInteger(1, java.util.Arrays.copyOfRange(digest, 0, 10)).shiftRight(6);
        BigInteger mask62 = BigInteger.ONE.shiftLeft(62).subtract(BigInteger.ONE);
        BigInteger randA = r.shiftRight(62); // bits 0..11
        BigInteger randB = r.and(mask62); // bits 12..73
        BigInteger uuid = BigInteger.valueOf(ms).shiftLeft(80)
                .or(BigInteger.valueOf(7).shiftLeft(76))
                .or(randA.shiftLeft(64))
                .or(BigInteger.valueOf(2).shiftLeft(62))
                .or(randB);
        String hex = String.format("%32s", uuid.toString(16)).replace(' ', '0');
        return hex.substring(0, 8) + "-" + hex.substring(8, 12) + "-" + hex.substring(12, 16) + "-"
                + hex.substring(16, 20) + "-" + hex.substring(20);
    }
}
