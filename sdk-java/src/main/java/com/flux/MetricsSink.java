/*
 * Destino de las metricas del SDK.
 * Contrato normativo: specification/08-observability.md
 *
 * Los nombres y las etiquetas son parte del CONTRATO, no una decision de cada SDK: si el
 * de Java y el de Go nombran distinto la tasa de DLQ, la de los servicios Java no se puede
 * sumar con la de los de Go y un panel del ecosistema es imposible. Es el mismo argumento
 * que el de los codigos POISON de 01-envelope.md §3.1. Por eso esto vive aqui y no en la
 * aplicacion.
 */
package com.flux;

import java.util.List;

/**
 * Donde van las metricas del SDK. Impleméntalo para enchufar Micrometer, OpenTelemetry o
 * lo que uses; el default es {@link #NONE}.
 *
 * <p><b>Las firmas fuerzan las etiquetas del protocolo.</b> No hay un
 * {@code Map<String,String> labels} generico a proposito: es justo por ahi por donde se
 * cuela un {@code tenantid} que multiplica las series temporales. Un tenant nuevo NO debe
 * crear series nuevas — para eso estan las trazas, donde el tenant si se etiqueta
 * (08-observability.md §2.2 y §5).
 *
 * <p>La cardinalidad no avisa: el sistema funciona en desarrollo con tres tenants y muere
 * en produccion con diez mil. Y el fallo se manifiesta como "Prometheus se ha quedado sin
 * memoria", no como "alguien etiqueto por tenant".
 */
public interface MetricsSink {

    /**
     * Buckets obligatorios del histograma, en segundos — 08-observability.md §3.
     *
     * <p>El ultimo es {@code 30} a proposito: <b>es el {@code ack_wait}</b>
     * ({@link Protocol#DEFAULT_ACK_WAIT}). Un handler que cae en el bucket superior esta a
     * punto de que su mensaje se reentregue mientras aun se ejecuta, asi que
     * {@code flux_event_handler_duration_seconds_bucket{le="30"}} frente al total mide
     * directamente cuantos eventos rozan la ejecucion concurrente.
     *
     * <p>Ese bucket DEBE moverse si se cambia {@code ack_wait}. Un bucket que no coincide
     * con el plazo real mide algo que no le importa a nadie. Lo fija
     * {@code MetricsTest.elUltimoBucketEsElAckWait}.
     */
    List<Double> DURATION_BUCKETS = List.of(
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0);

    /** Etiqueta {@code outcome} de {@code flux_events_published_total}. */
    enum PublishOutcome {
        OK("ok"),
        /** El payload no cumple su JSON Schema y el productor L3 rechazo publicarlo. */
        INVALID_SCHEMA("invalid_schema"),
        /** El broker rechazo la publicacion. */
        ERROR("error");

        private final String wire;

        PublishOutcome(String wire) {
            this.wire = wire;
        }

        /** El literal que va en la etiqueta. */
        public String wire() {
            return wire;
        }
    }

    /** Etiqueta {@code outcome} de {@code flux_events_consumed_total} — §2.1. */
    enum ConsumeOutcome {
        OK("ok"),
        RETRYABLE("retryable"),
        PERMANENT("permanent"),
        POISON("poison"),
        INVALID_SCHEMA("invalid_schema"),
        /**
         * Un fallo de firma: {@code MISSING_SIGNATURE}, {@code INVALID_SIGNATURE} o
         * {@code UNKNOWN_SIGNING_KEY} — 07-signing.md §7.
         *
         * <p>Se separa del {@link #POISON} comun aunque el {@code dlqreason} del evento siga
         * siendo {@code poison}: son dos incidentes distintos —basura frente a
         * suplantacion— con dos respuestas distintas. Un pico de firmas rotas apunta a un
         * productor con la clave equivocada o a alguien reinyectando eventos; un pico de
         * JSON corrupto, a un productor roto. Confundirlos hace que la alerta no diga qué
         * hacer.
         */
        INVALID_SIGNATURE("invalid_signature");

        private final String wire;

        ConsumeOutcome(String wire) {
            this.wire = wire;
        }

        /** El literal que va en la etiqueta. */
        public String wire() {
            return wire;
        }
    }

    /** Valores de {@code flux_connection_state} — §2.1. */
    enum ConnectionState {
        DISCONNECTED(0),
        CONNECTED(1),
        RECONNECTING(2);

        private final int value;

        ConnectionState(int value) {
            this.value = value;
        }

        /** El numero que se expone en el gauge. */
        public int value() {
            return value;
        }
    }

    /** {@code flux_events_published_total{subject,outcome}}. */
    void eventPublished(String subject, PublishOutcome outcome);

    /** {@code flux_events_consumed_total{subject,consumer,outcome}}. */
    void eventConsumed(String subject, String consumer, ConsumeOutcome outcome);

    /** {@code flux_event_handler_duration_seconds{subject,consumer}}. */
    void handlerDuration(String subject, String consumer, double seconds);

    /**
     * {@code flux_events_dlq_total{subject,consumer,reason,code}}.
     *
     * @param code el codigo ESTABLE de la clasificacion ({@code "HTTP_503"},
     *             {@code "PEDIDO_YA_CANCELADO"}), nunca el mensaje del error: un mensaje
     *             lleva ids, timestamps y rutas, su cardinalidad es infinita y tumba el
     *             almacenamiento de metricas — §2.2.
     */
    void eventDlq(String subject, String consumer, FluxEvent.DlqReason reason, String code);

    /** {@code flux_events_retried_total{subject,consumer,attempt}}. {@code attempt} va de 1 a max_deliver. */
    void eventRetried(String subject, String consumer, int attempt);

    /**
     * {@code flux_consumer_pending{subject,consumer}}.
     *
     * <p>La alimenta el despacho en cada entrega, con el {@code pendingCount()} que ya
     * viene en los metadatos del mensaje de JetStream: no hace falta sondear al servidor.
     *
     * <p>Es la <b>unica</b> senal que delata a un consumidor cuyo bucle murio. La conexion
     * sigue reportandose sana y el healthcheck dice que todo va bien; solo el crecimiento de
     * pending lo evidencia — es el bug que aparecio de verdad en el SDK de Node, y de ahi la
     * cuarta alerta de §4.
     *
     * <p>Limitacion: si el consumidor deja de recibir mensajes <b>del todo</b>, tampoco se
     * actualiza este gauge — se queda en su ultimo valor. Para eso sirve la alerta de §4
     * sobre el valor sostenido, no sobre su derivada.
     */
    void consumerPending(String subject, String consumer, long pending);

    /** {@code flux_connection_state}. Sin etiquetas — §2. */
    void connectionState(ConnectionState state);

    /**
     * No-op. Es el DEFAULT: un SDK de protocolo no debe imponer un backend de metricas a
     * quien solo quiere publicar un evento.
     */
    MetricsSink NONE = new MetricsSink() {
        @Override
        public void eventPublished(String subject, PublishOutcome outcome) {
        }

        @Override
        public void eventConsumed(String subject, String consumer, ConsumeOutcome outcome) {
        }

        @Override
        public void handlerDuration(String subject, String consumer, double seconds) {
        }

        @Override
        public void eventDlq(String subject, String consumer, FluxEvent.DlqReason reason, String code) {
        }

        @Override
        public void eventRetried(String subject, String consumer, int attempt) {
        }

        @Override
        public void consumerPending(String subject, String consumer, long pending) {
        }

        @Override
        public void connectionState(ConnectionState state) {
        }
    };
}
