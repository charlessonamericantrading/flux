/*
 * Errores de la capacidad `state`.
 * Contrato normativo: specification/04-errors.md §6 y specification/10-state.md.
 *
 * La lista es CERRADA: los siete codigos de abajo y ninguno mas (mas `RESERVED_DOMAIN`, que
 * vive en {@link ReservedDomainException}, e `INVALID_DATACLASSIFICATION`, comun al envelope).
 * Un caso que no encaje en ninguno es un hueco de la spec, no un codigo nuevo que este SDK
 * pueda inventar.
 *
 * Reglas comunes (04-errors.md §6, «Que DEBE decir el mensaje»):
 *   · el `code` es fijo y la aplicacion no puede sustituirlo: es lo que agrupa el panel
 *     (`flux_events_dlq_total{code}`) y encabeza `dlqerror`;
 *   · el mensaje dice QUE esta mal y COMO se arregla;
 *   · NINGUN mensaje lleva valores de `data`: acaba en logs y en `dlqerror` durante 90 dias,
 *     y en estado `data` es dato personal. Una clave y un tenant si pueden citarse: son
 *     identificadores opacos.
 */
package com.flux;

import java.util.List;

/** Contenedor de las excepciones de estado. Clase de utilidad: no se instancia. */
public final class StateErrors {

    private StateErrors() {
        throw new AssertionError("clase de utilidad");
    }

    /** Cita un identificador opaco (clave, tenant) sin dejar que un valor largo o raro llene un log. */
    static String quote(String v) {
        if (v == null) {
            return "null";
        }
        String s = Envelope.jsonString(v);
        return s.length() > 70 ? s.substring(0, 67) + "…\"" : s;
    }

    /**
     * Un contrato que no es {@code <dominio>.<agregado>.v<major>} —{@code clientes.cliente} sin
     * version, cuatro tokens, mayusculas—. PERMANENT. Un dominio reservado NO es este error: es
     * {@link ReservedDomainException}, que se evalua antes (10-state.md §3.6).
     */
    public static final class InvalidStateContractException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;

        public InvalidStateContractException(String contract, String reason) {
            super("contrato de estado invalido " + quote(contract) + ": " + reason + ". Un contrato es "
                    + "`<dominio>.<agregado>.v<major>` —el subject del estado sin `state.`, sin tenant y "
                    + "sin clave—, en minusculas y kebab-case: `clientes.cliente.v1` (10-state.md §2.2).",
                    "INVALID_STATE_CONTRACT", null);
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /** El tenant no es valido para estado, o no se pudo resolver ninguno. PERMANENT. */
    public static final class InvalidTenantForStateException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;

        /** @param tenant el tenant dado, o {@code null} si no hay ninguno. */
        public InvalidTenantForStateException(String tenant) {
            super(tenant == null
                    ? "no hay tenant para esta operacion de estado. En estado NO hay tenant por defecto "
                            + "(ni \"system\" implicito): el tenant va en el subject, y uno equivocado no es "
                            + "una etiqueta mal puesta sino un dato guardado en otro sitio. Pasalo en la opcion "
                            + "`tenantId` o fija `tenantId` en connect() (10-state.md §2.4)."
                    : "el tenant " + quote(tenant) + " no es valido para estado: debe cumplir "
                            + "^[a-z0-9]+(-[a-z0-9]+)*$ y tener como maximo 64 caracteres. El tenant de un "
                            + "EVENTO puede ser cualquier cadena; el de un estado, no, porque va dentro del "
                            + "subject de NATS (10-state.md §2.4).",
                    "INVALID_TENANT_FOR_STATE", null);
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /** La clave no cabe en un token de NATS. PERMANENT. */
    public static final class InvalidStateKeyException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;

        public InvalidStateKeyException(String key) {
            super("la clave " + quote(key) + " no es valida: debe cumplir ^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$ "
                    + "(un unico token de NATS: sin puntos, comodines ni espacios, de 1 a 128 caracteres y "
                    + "sin empezar por «_», que esta reservado al protocolo). El broker NO la valida —acepta "
                    + "`*`, `a.b` y 2000 caracteres— y una clave sin validar es un subject fantasma que "
                    + "ningun observador recibe, sin error. Tampoco se escapa sola: dos escapes distintos "
                    + "serian dos claves para el mismo agregado. Usa un identificador opaco, nunca un email "
                    + "(10-state.md §2.3).",
                    "INVALID_STATE_KEY", null);
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /**
     * {@code data} no lleva un {@code aggregateVersion} valido. PERMANENT.
     *
     * <p>El mensaje dice QUE tipo de valor llego, nunca el valor de un campo de texto: {@code data}
     * es dato personal. Un numero si se cita (una version no es PII y {@code 7.5} o {@code 0}
     * explican el fallo).
     */
    public static final class MissingAggregateVersionException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;

        /** @param received descripcion ya segura del valor que llego («no hay ninguno», «llego 7.5»…). */
        public MissingAggregateVersionException(String received) {
            super("`data.aggregateVersion` debe ser un entero >= 1 y <= 9007199254740991 (2^53 − 1); "
                    + received + ". Lo pone el DUENO del agregado —es su contador monotonico, el mismo que "
                    + "llevan sus eventos—: el SDK ni lo inventa ni lo incrementa. Ausente, null, una cadena, "
                    + "un decimal, 0 o un negativo son todos este error (10-state.md §3.4).",
                    "MISSING_AGGREGATE_VERSION", null);
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /**
     * El sobre de una entrada no coincide con el subject de NATS en el que llego. <b>POISON</b>.
     *
     * <p>No es PERMANENT: el mensaje es legible pero MIENTE sobre su identidad —su tenant, su clave
     * o su tipo—, y un productor que lo hace esta roto o es hostil. Es de lo que se despierta a
     * alguien (04-errors.md §1.3). Nunca se aplica ni llega al handler.
     */
    public static final class StateSubjectMismatchException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;

        public StateSubjectMismatchException(String attribute, String envelopeValue, String subjectValue) {
            super("el atributo `" + attribute + "` del sobre (" + quote(envelopeValue) + ") no coincide con el "
                    + "del subject de NATS en el que llego la entrada (" + quote(subjectValue) + "). La "
                    + "identidad de una entrada esta en el subject; el sobre la repite, y si discrepan uno de "
                    + "los dos miente y quien consume no puede saber cual. La entrada NO se aplica: la clave "
                    + "conserva su valor anterior (10-state.md §5.4).",
                    "STATE_SUBJECT_MISMATCH", null);
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.POISON;
        }
    }

    /**
     * El chequeo de secuencia agoto sus tres intentos. <b>RETRYABLE</b>: el conflicto es del
     * entorno —otra instancia escribe la misma clave—, no de la llamada.
     */
    public static final class StateConflictException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;

        public StateConflictException(String contract, String key, int attempts) {
            super("la clave " + quote(key) + " de \"" + contract + "\" cambio entre la lectura y la "
                    + "publicacion las " + attempts + " veces (otra instancia del dueno escribe la misma "
                    + "clave): se agotaron los " + attempts + " intentos del put condicional. Es reintentable: "
                    + "un nuevo intento relee y suele resolverlo, o termina en `stale` (10-state.md §3.2).",
                    "STATE_CONFLICT", null);
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.RETRYABLE;
        }
    }

    /**
     * La clasificacion no es admisible. PERMANENT. El codigo ya existia (01-envelope.md §3.1): al
     * consumir es POISON; al llamar, el error es de quien llama.
     */
    public static final class InvalidDataClassificationException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;

        /**
         * @param classification la clasificacion dada (puede ser {@code null}).
         * @param state          {@code true} si es la de un {@code put} de estado (admite tres, no
         *                       {@code restricted}); {@code false} si es la de un sobre cualquiera
         *                       (admite las cuatro, y lo unico invalido es salirse del enum).
         */
        public InvalidDataClassificationException(String classification, boolean state) {
            super(state
                    ? "la clasificacion " + quote(classification) + " no se admite en estado: solo public, "
                            + "internal o confidential. `restricted` (credenciales, salud, financiero: 24 h de "
                            + "retencion) no cabe en un almacen que no caduca, y no es una decision que deba "
                            + "tomarse por omision (10-state.md §7)."
                    : "la clasificacion " + quote(classification) + " no es valida: public, internal, "
                            + "confidential o restricted (01-envelope.md §3.1). El error de una "
                            + "peticion/respuesta es siempre `internal` y no se elige.",
                    "INVALID_DATACLASSIFICATION", null);
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /**
     * Una entrada de estado que LLEGA con {@code dataclassification: "restricted"}. <b>POISON</b>:
     * al consumir es un mensaje que no deberia existir (el {@code put} de un SDK conforme lo
     * rechaza), y un almacen sin caducidad no puede guardar un dato de 24 h de retencion
     * (10-state.md §7).
     */
    public static final class RestrictedStateEntryException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;

        public RestrictedStateEntryException() {
            super("una entrada de estado no puede llevar dataclassification \"restricted\": credenciales, "
                    + "salud o datos financieros (24 h de retencion) no caben en un almacen que no caduca. "
                    + "La entrada NO se aplica (10-state.md §7).",
                    "INVALID_DATACLASSIFICATION", null);
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.POISON;
        }
    }

    /** Los codigos de esta capacidad, en el orden de 04-errors.md §6. */
    static final List<String> CODES = List.of(
            "INVALID_STATE_CONTRACT", "INVALID_TENANT_FOR_STATE", "INVALID_STATE_KEY",
            "MISSING_AGGREGATE_VERSION", "RESERVED_DOMAIN", "STATE_SUBJECT_MISMATCH", "STATE_CONFLICT");
}
