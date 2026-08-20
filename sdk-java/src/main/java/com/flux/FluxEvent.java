/*
 * El evento de flux: un CloudEvent 1.0 con el perfil de extensiones de flux.
 * Contrato normativo: specification/01-envelope.md
 */
package com.flux;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

/**
 * Un evento de flux.
 *
 * <p>Los nombres de las extensiones van en minuscula pegada ({@code correlationid}, no
 * {@code correlation_id}) porque CloudEvents restringe los nombres de extension a
 * minusculas y digitos ASCII sin separadores. No es estilo: es la especificacion
 * (01-envelope.md §3). De ahi que los {@code @JsonProperty} no sigan ninguna convencion
 * de Java.
 *
 * <p><b>Ausente != vacio (01-envelope.md §3.3).</b> Todos los atributos opcionales son
 * tipos de referencia anulables —{@code Integer} y no {@code int} en
 * {@code dlqattempts}— y la clase lleva {@link JsonInclude.Include#NON_NULL}. Un
 * opcional se OMITE cuando no aplica; nunca lleva {@code 0}, {@code ""} o {@code false}
 * con significado propio. Con {@code int} primitivo, un {@code dlqattempts} ausente se
 * leeria como {@code 0} y se re-emitiria como {@code 0}, que es exactamente el colapso
 * entre cero y ausente que la spec prohibe.
 *
 * <p><b>Es un {@code record} y eso funciona aqui</b>, a diferencia de Go: el
 * {@code equals} generado compara {@code data} con {@link JsonNode#equals}, que es
 * estructural y profundo. En Go el equivalente ({@code json.RawMessage}, un slice) hace
 * el struct no comparable y obliga a un {@code Equal} escrito a mano. Ojo al matiz: la
 * comparacion de {@code data} aqui es SEMANTICA (dos objetos con las mismas claves en
 * distinto orden son iguales), mientras que la de Go es byte a byte.
 *
 * @param specversion       DEBE ser exactamente "1.0".
 * @param id                UUIDv7 en formato canonico, generado por el SDK.
 * @param source            {@code /<entorno>/<servicio>}.
 * @param type              reverse-DNS derivado del subject de NATS — 02-naming.md §2.
 * @param time              RFC 3339 UTC con EXACTAMENTE 3 decimales y sufijo Z. Se guarda
 *                          como String y no como {@link Instant} — ver
 *                          {@link Envelope#formatTime(Instant)}.
 * @param datacontenttype   {@code application/json} en v1.
 * @param dataschema        URI absoluta al JSON Schema, con SemVer.
 * @param aggregateId       atributo {@code subject} de CloudEvents. NO es el subject de
 *                          NATS: el de NATS es una direccion de enrutado
 *                          ({@code pedidos.pedido.v1.creado}), este es la identidad del
 *                          agregado dentro de la fuente ({@code ped-123}). Confundirlos es
 *                          el error mas frecuente al adoptar CloudEvents sobre NATS, asi
 *                          que el SDK los nombra distinto y solo los une al serializar
 *                          — 01-envelope.md §2.1.
 * @param correlationid     identifica el flujo de negocio completo y se propaga sin
 *                          modificar. Si el evento no nace de otro, el SDK lo inicializa
 *                          con su propio {@code id}.
 * @param tenantid          {@code "system"} para eventos de plataforma.
 * @param producerversion   SemVer del servicio emisor. Sin esto, un bug de payload en
 *                          produccion no se puede acotar a un despliegue.
 * @param dataclassification una de public/internal/confidential/restricted.
 * @param causationid       {@code id} del evento concreto que provoco este.
 *                          {@code correlationid} responde "de que flujo forma parte";
 *                          {@code causationid}, "quien lo causo exactamente".
 * @param partitionkey      clave de ordenacion. Por convencion, igual al aggregateId.
 * @param traceparent       W3C Trace Context.
 * @param tracestate        W3C Trace Context.
 * @param dlqreason         solo presente en {@code dlq.<subject>}.
 * @param dlqattempts       numero de entrega en la que el evento murio, NO una propiedad
 *                          de su clase. Un PERMANENT suele registrar 1; un RETRYABLE
 *                          agotado registra siempre 6. {@code Integer} y no {@code int}:
 *                          ver la nota de la clase sobre ausente != vacio.
 * @param dlqconsumer       durable del consumidor que lo enruto.
 * @param dlqerror          mensaje del error, recortado a 1024 caracteres.
 * @param dlqtime           instante del enrutado a la DLQ.
 * @param data              payload sin interpretar. {@link JsonNode} a proposito: un
 *                          evento parseado y vuelto a serializar conserva claves, orden y
 *                          precision de enteros, que es lo que hace que el replay desde la
 *                          DLQ sea "borrar las extensiones dlq* y republicar". Para
 *                          tiparlo, {@link Envelope#dataAs(FluxEvent, Class)}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "specversion", "id", "source", "type", "time", "datacontenttype", "dataschema", "subject",
        "correlationid", "tenantid", "producerversion", "dataclassification",
        "causationid", "partitionkey", "traceparent", "tracestate",
        "dlqreason", "dlqattempts", "dlqconsumer", "dlqerror", "dlqtime",
        "data"
})
public record FluxEvent(
        // ── Nucleo CloudEvents — 01-envelope.md §2 ──
        @JsonProperty("specversion") String specversion,
        @JsonProperty("id") String id,
        @JsonProperty("source") String source,
        @JsonProperty("type") String type,
        @JsonProperty("time") String time,
        @JsonProperty("datacontenttype") String datacontenttype,
        @JsonProperty("dataschema") String dataschema,
        // ⚠️ El componente se llama aggregateId y el atributo JSON `subject`. Ver @param.
        @JsonProperty("subject") String aggregateId,

        // ── Extensiones obligatorias — 01-envelope.md §3.1 ──
        @JsonProperty("correlationid") String correlationid,
        @JsonProperty("tenantid") String tenantid,
        @JsonProperty("producerversion") String producerversion,
        @JsonProperty("dataclassification") DataClassification dataclassification,

        // ── Extensiones opcionales — 01-envelope.md §3.2 ──
        @JsonProperty("causationid") String causationid,
        @JsonProperty("partitionkey") String partitionkey,
        @JsonProperty("traceparent") String traceparent,
        @JsonProperty("tracestate") String tracestate,

        // ── Extensiones de DLQ — solo en dlq.<subject> — 04-errors.md §3 ──
        @JsonProperty("dlqreason") DlqReason dlqreason,
        @JsonProperty("dlqattempts") Integer dlqattempts,
        @JsonProperty("dlqconsumer") String dlqconsumer,
        @JsonProperty("dlqerror") String dlqerror,
        @JsonProperty("dlqtime") String dlqtime,

        // ── Payload — 01-envelope.md §4 ──
        @JsonProperty("data") JsonNode data) {

    /** Clasificacion del dato transportado — 06-security.md §5. Determina la retencion. */
    public enum DataClassification {
        PUBLIC("public"),
        INTERNAL("internal"),
        CONFIDENTIAL("confidential"),
        RESTRICTED("restricted");

        private final String wire;

        DataClassification(String wire) {
            this.wire = wire;
        }

        /** El valor que viaja en el JSON es siempre el de la spec, en minusculas. */
        @JsonValue
        public String wire() {
            return wire;
        }

        /**
         * Case-sensitive a proposito: {@code "Confidential"} no es un valor del protocolo.
         * Lanzar hace que el mensaje acabe clasificado como POISON, que es lo correcto: un
         * valor fuera del enum significa que el productor esta roto o que la version del
         * contrato no coincide.
         */
        @JsonCreator
        public static DataClassification fromWire(String value) {
            for (DataClassification c : values()) {
                if (c.wire.equals(value)) {
                    return c;
                }
            }
            throw new IllegalArgumentException("dataclassification \"" + value
                    + "\" no es valida: debe ser public, internal, confidential o restricted "
                    + "(06-security.md §5)");
        }
    }

    /** Por que un evento acabo en la DLQ — 04-errors.md §3. */
    public enum DlqReason {
        RETRYABLE("retryable"),
        PERMANENT("permanent"),
        POISON("poison");

        private final String wire;

        DlqReason(String wire) {
            this.wire = wire;
        }

        @JsonValue
        public String wire() {
            return wire;
        }

        @JsonCreator
        public static DlqReason fromWire(String value) {
            for (DlqReason r : values()) {
                if (r.wire.equals(value)) {
                    return r;
                }
            }
            throw new IllegalArgumentException(
                    "dlqreason \"" + value + "\" no es valida: debe ser retryable, permanent o poison");
        }
    }

    /** Interpreta el atributo {@code time} como instante. */
    public Instant eventTime() {
        return Envelope.parseTime(time);
    }

    /**
     * Deriva los tokens del subject de NATS a partir del {@code type}. Util en un handler
     * que se suscribio con wildcard y necesita saber que evento concreto llego.
     */
    public Protocol.ParsedSubject parsedType() {
        return Protocol.parseType(type);
    }

    /**
     * Informa si el evento lleva las extensiones {@code dlq*}.
     *
     * <p>⚠️ El {@code @JsonIgnore} NO es decorativo. Jackson descubre las propiedades de un
     * record por sus componentes, pero ademas aplica la convencion de bean a los metodos:
     * un {@code isDlq()} publico se convierte en el atributo raiz {@code "dlq": false} y
     * acaba en el JSON del envelope sin que nadie lo haya pedido. Es una trampa especifica
     * de Java —Go y TypeScript no tienen convencion de getters— y la caza la regla de
     * atributos raiz cerrados de 01-envelope.md §3.3, comprobada en EnvelopeTest. Cualquier
     * metodo {@code isX()} o {@code getX()} que se anada aqui necesita esta anotacion.
     */
    @JsonIgnore
    public boolean isDlq() {
        return dlqreason != null;
    }

    /**
     * Copia con las extensiones {@code dlq*} puestas. Lo usa {@link Envelope#toDlqEvent};
     * existe aqui para que la unica invocacion del constructor canonico de 22 componentes
     * viva junto a la definicion del record y no repartida por el SDK.
     */
    FluxEvent withDlq(DlqReason reason, Integer attempts, String consumer, String error, String when) {
        return new FluxEvent(specversion, id, source, type, time, datacontenttype, dataschema,
                aggregateId, correlationid, tenantid, producerversion, dataclassification,
                causationid, partitionkey, traceparent, tracestate,
                reason, attempts, consumer, error, when, data);
    }

    /** Copia sin las extensiones {@code dlq*}. El {@code id} original se CONSERVA. */
    FluxEvent withoutDlq() {
        return withDlq(null, null, null, null, null);
    }
}
