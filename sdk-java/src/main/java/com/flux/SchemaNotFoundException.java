/*
 * El bundle no contiene el esquema que el evento dice cumplir.
 * Contrato normativo: specification/00-protocol.md §5 (nivel L3).
 */
package com.flux;

/**
 * No hay esquema para el {@code dataschema} del evento dentro del bundle.
 *
 * <p>En {@link Validation.Mode#STRICT} es un error y no un aviso, y esa severidad es
 * deliberada: si "no lo encuentro" pasara en silencio, un bundle que se quedo atras
 * convertiria la validacion L3 en un no-op — el sistema seguiria arrancando, el panel
 * seguiria verde y NADA se estaria validando. Un fallo silencioso de la validacion es peor
 * que no validar, porque se cree que si.
 *
 * <p>En {@link Validation.Mode#WARN} se registra y el evento sigue su curso: es el modo
 * pensado para introducir L3 en un ecosistema en marcha.
 */
public final class SchemaNotFoundException extends FluxErrors.FluxException {

    private static final long serialVersionUID = 1L;

    /** Codigo estable para metricas y alertas — 08-observability.md §2.2. */
    public static final String CODE = "SCHEMA_NOT_FOUND";

    private final String subject;
    private final String dataschema;

    /**
     * @param subject    subject del evento.
     * @param dataschema la URI que el evento declara y que el bundle no conoce.
     */
    public SchemaNotFoundException(String subject, String dataschema) {
        super("no hay esquema para \"" + subject + "\" (" + dataschema + ") en el bundle. "
                + "Regeneralo con `node scripts/bundle-schemas.mjs`, o baja el modo de "
                + "validacion a WARN", CODE, null);
        this.subject = subject;
        this.dataschema = dataschema;
    }

    @Override
    public ErrorClass errorClass() {
        return ErrorClass.PERMANENT;
    }

    /** Subject del evento. */
    public String subject() {
        return subject;
    }

    /** La URI que no esta en el bundle. */
    public String dataschema() {
        return dataschema;
    }
}
