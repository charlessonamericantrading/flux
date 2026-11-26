/*
 * Errores de la capacidad `security`.
 * Contrato normativo: specification/04-errors.md §5 y specification/06-security.md §3.
 *
 * Cinco codigos estables y todos PERMANENT (como un HTTP 403: reintentar da lo mismo):
 *   NOT_PROVISIONED, PERMISSION_DENIED  — con el servicio ya en marcha (04-errors.md §5)
 *   CONNECTION_REJECTED, INVALID_CREDENTIALS, INVALID_TLS_OPTIONS — de `connect()` (§5.3)
 *
 * Reglas comunes (04-errors.md §5.2): el `code` es fijo y la aplicacion no puede sustituirlo;
 * el mensaje dice QUE falta y COMO se arregla; NUNCA lleva credenciales ni el contenido de
 * `data`, porque acaba en logs y en `dlqerror`.
 */
package com.flux;

import java.util.List;

/** Contenedor de las excepciones de seguridad. Clase de utilidad: no se instancia. */
public final class SecurityErrors {

    private SecurityErrors() {
        throw new AssertionError("clase de utilidad");
    }

    /** Que se intentaba cuando el broker denego. */
    public enum Operation {
        PUBLISH("publicar en"),
        SUBSCRIBE("suscribirse a");

        private final String phrase;

        Operation(String phrase) {
            this.phrase = phrase;
        }

        String phrase() {
            return phrase;
        }
    }

    /**
     * El broker denego una operacion por permisos ({@code Permissions Violation}). <b>PERMANENT</b>:
     * reintentar una denegacion da la misma denegacion, como un HTTP 403 — 04-errors.md §1.2. Un
     * handler que la provoque va a la DLQ sin atascar la cola.
     */
    public static final class PermissionDeniedException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;
        private final String service;
        private final Operation operation;
        private final String subject;

        /**
         * @param hint como se arregla ESTA denegacion, si el SDK lo sabe (la peticion/respuesta lo
         *             sabe); {@code null} para el texto general.
         */
        public PermissionDeniedException(String service, Operation operation, String subject, String hint,
                                         Throwable cause) {
            super("el broker denego " + operation.phrase() + " \"" + subject + "\" a \"" + service
                    + "\": su ACL no concede ese permiso. "
                    + (hint != null
                            ? hint + " "
                            : subject.startsWith("$JS.")
                                    ? "Es un permiso de JetStream, y se concede exacto por durable: una ACL "
                                            + "generada antes de la v1.1 (que solo habla de subjects de dominio) "
                                            + "no los incluye. Ver «Seguridad» en sdk-java/README.md "
                                            + "(06-security.md §3)."
                                    : "Ver 06-security.md §3."),
                    "PERMISSION_DENIED", cause);
            this.service = service;
            this.operation = operation;
            this.subject = subject;
        }

        public String service() {
            return service;
        }

        public Operation operation() {
            return operation;
        }

        /** El subject denegado, completo y sin normalizar. */
        public String subject() {
            return subject;
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /** Que recurso falta. */
    public enum ResourceKind {
        STREAM,
        CONSUMER
    }

    /**
     * Falta un stream o un consumidor y este servicio no tiene permiso para crearlo.
     *
     * <p>En produccion los streams y los consumidores se aprovisionan FUERA del SDK
     * (06-security.md §3): dejar que cualquier servicio cree el stream compartido de un dominio es
     * una carrera esperando a ocurrir, y su permiso de creacion es justo el que la ACL exacta le
     * quita. Sin este error lo que se veria es un timeout, que no dice nada.
     *
     * <p><b>PERMANENT</b>: reintentar no hace aparecer el recurso.
     */
    public static final class NotProvisionedException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;
        private final ResourceKind kind;
        private final String resource;
        private final String stream;
        private final String deniedSubject;

        /**
         * @param stream el stream del consumidor (solo con {@code CONSUMER}); {@code null} si no aplica.
         * @param detail sustituye al texto entero (el estado: ahi la señal no es un {@code CREATE}
         *               denegado sino que NADIE contesta a la lectura directa); {@code null} para el general.
         */
        public NotProvisionedException(ResourceKind kind, String resource, String stream, String service,
                                       String deniedSubject, String detail, Throwable cause) {
            super(detail != null
                    ? detail
                    : (kind == ResourceKind.STREAM
                            ? "el stream \"" + resource + "\""
                            : "el consumidor \"" + resource + "\" del stream \"" + stream + "\"")
                            + " no existe y \"" + service + "\" no tiene permiso para crearlo (el broker "
                            + "denego publicar en \"" + deniedSubject + "\"). En produccion se aprovisiona con "
                            + "`flux provision <services.json>`, usando credenciales de operacion, no las del "
                            + "servicio (06-security.md §3).",
                    "NOT_PROVISIONED", cause);
            this.kind = kind;
            this.resource = resource;
            this.stream = stream;
            this.deniedSubject = deniedSubject;
        }

        public ResourceKind kind() {
            return kind;
        }

        /** El nombre del recurso que falta. */
        public String resource() {
            return resource;
        }

        /** El stream del consumidor que falta, o {@code null}. */
        public String stream() {
            return stream;
        }

        public String deniedSubject() {
            return deniedSubject;
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /** Por que el broker rechazo la primera conexion. */
    public enum RejectionReason {
        /** El broker rechazo las credenciales. */
        AUTH("auth"),
        /**
         * El certificado del servidor no verifica (CA ausente o equivocada, host fuera de los SAN,
         * caducado), se exigio TLS y el servidor no lo ofrece, o el broker exige certificado de
         * cliente.
         */
        TLS("tls");

        private final String wire;

        RejectionReason(String wire) {
            this.wire = wire;
        }

        /** {@code "auth"} o {@code "tls"}: el valor estable de 04-errors.md §5.3. */
        public String wire() {
            return wire;
        }
    }

    /**
     * El broker CONTESTO y rechazo la primera conexion por algo que esperar no arregla: un
     * certificado que no verifica, o credenciales que no acepta (04-errors.md §5.3).
     */
    public static final class ConnectionRejectedException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;
        private final RejectionReason reason;

        public ConnectionRejectedException(RejectionReason reason, String message, Throwable cause) {
            super(message, "CONNECTION_REJECTED", cause);
            this.reason = reason;
        }

        public RejectionReason reason() {
            return reason;
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /**
     * Las opciones de credenciales estan MAL FORMADAS y eso se sabe SIN hablar con el broker.
     * Nunca se interpola el valor de una credencial: este mensaje acaba en logs.
     */
    public static final class InvalidCredentialsException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;

        public InvalidCredentialsException(String reason) {
            this(reason, null);
        }

        public InvalidCredentialsException(String reason, Throwable cause) {
            super("credenciales invalidas en connect(): " + reason + ". Se detecta sin hablar con el broker: "
                    + "reintentar no lo arregla, cambia lo que se pasa a `credentials` "
                    + "(06-security.md §2, 04-errors.md §5.3).", "INVALID_CREDENTIALS", cause);
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /** Las opciones TLS estan MAL FORMADAS y se sabe SIN red. Una ruta si puede citarse. */
    public static final class InvalidTlsOptionsException extends FluxErrors.FluxException {
        private static final long serialVersionUID = 1L;

        public InvalidTlsOptionsException(String reason) {
            this(reason, null);
        }

        public InvalidTlsOptionsException(String reason, Throwable cause) {
            super("opciones TLS invalidas en connect(): " + reason + ". Se detecta sin hablar con el broker: "
                    + "reintentar no lo arregla, cambia lo que se pasa a `tls` (06-security.md §2, "
                    + "04-errors.md §5.3).", "INVALID_TLS_OPTIONS", cause);
        }

        @Override
        public ErrorClass errorClass() {
            return ErrorClass.PERMANENT;
        }
    }

    /** Los cinco codigos de la capacidad, en el orden de 04-errors.md §5. */
    static final List<String> CODES = List.of(
            "NOT_PROVISIONED", "PERMISSION_DENIED", "CONNECTION_REJECTED", "INVALID_CREDENTIALS",
            "INVALID_TLS_OPTIONS");
}
