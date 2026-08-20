/*
 * El payload no cumple el JSON Schema que su propio `dataschema` declara.
 * Contrato normativo: specification/00-protocol.md §5 (nivel L3).
 */
package com.flux;

import java.util.Collections;
import java.util.List;

/**
 * El {@code data} de un evento no valida contra su esquema.
 *
 * <p>Al PUBLICAR sale de {@code publish()} y aborta la publicacion: es lo que convierte un
 * contrato roto en un fallo del servicio que lo provoco, en vez de un misterio que aparece
 * la semana que viene en un consumidor de otro equipo y otro lenguaje.
 *
 * <p>Al CONSUMIR se clasifica {@link ErrorClass#PERMANENT}: el evento es sintacticamente
 * correcto —llego a parsearse como CloudEvent, asi que no es POISON— pero incumple su
 * contrato, y reintentarlo seis veces dara exactamente el mismo resultado.
 *
 * <p>{@link #errors()} trae TODOS los fallos, no solo el primero. Reportar de uno en uno
 * convierte arreglar un payload con tres campos mal en tres despliegues — 00-protocol.md §5.
 */
public final class SchemaValidationException extends FluxErrors.FluxException {

    private static final long serialVersionUID = 1L;

    /** Codigo estable para metricas y alertas. Nunca el mensaje — 08-observability.md §2.2. */
    public static final String CODE = "SCHEMA_VALIDATION_FAILED";

    private final String subject;
    private final String dataschema;
    private final transient List<String> errors;

    /**
     * @param subject    subject del evento.
     * @param dataschema URI del esquema contra el que se valido.
     * @param errors     TODOS los fallos encontrados, en orden estable.
     */
    public SchemaValidationException(String subject, String dataschema, List<String> errors) {
        super(buildMessage(subject, dataschema, errors), CODE, null);
        this.subject = subject;
        this.dataschema = dataschema;
        this.errors = errors == null ? List.of() : Collections.unmodifiableList(List.copyOf(errors));
    }

    private static String buildMessage(String subject, String dataschema, List<String> errors) {
        StringBuilder sb = new StringBuilder()
                .append("el payload de \"").append(subject).append("\" no cumple su esquema (")
                .append(dataschema).append("):");
        if (errors != null) {
            for (String error : errors) {
                sb.append("\n  · ").append(error);
            }
        }
        return sb.toString();
    }

    @Override
    public ErrorClass errorClass() {
        return ErrorClass.PERMANENT;
    }

    /** Subject del evento que no valido. */
    public String subject() {
        return subject;
    }

    /** URI del esquema contra el que se valido. */
    public String dataschema() {
        return dataschema;
    }

    /** TODOS los fallos de validacion, no solo el primero. Inmutable. */
    public List<String> errors() {
        return errors;
    }
}
