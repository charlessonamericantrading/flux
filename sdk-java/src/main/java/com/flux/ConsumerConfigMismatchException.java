/*
 * El servidor devolvio una configuracion de consumidor distinta de la solicitada.
 * Contrato normativo: specification/03-delivery.md §2.1 (requisito L2)
 */
package com.flux;

import java.util.List;

/**
 * Se lanza cuando el servidor aplico una configuracion distinta de la solicitada.
 *
 * <p>Es la UNICA defensa contra la sobrescritura silenciosa de {@code ack_wait} por
 * {@code backoff[0]}: JetStream acepta la peticion, no avisa, y devuelve otra cosa.
 * Verificado contra nats-server 2.14.5. Sin esta comprobacion, un handler de mas de un
 * segundo se ejecuta en concurrencia consigo mismo bajo carga y nada lo indica —
 * 03-delivery.md §2.1.
 *
 * <p>Se lanza en {@code subscribe()} y aborta la suscripcion a proposito: arrancar con una
 * config distinta de la declarada es peor que no arrancar, porque el fallo aparece
 * despues, bajo carga y sin traza.
 */
public class ConsumerConfigMismatchException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Un campo en el que el servidor no honro lo solicitado.
     *
     * @param field     nombre del campo tal como lo llama la API de JetStream.
     * @param requested lo que pidio el SDK.
     * @param effective lo que devolvio el servidor.
     */
    public record Difference(String field, Object requested, Object effective) {
    }

    private final transient String durable;
    private final transient List<Difference> differences;

    public ConsumerConfigMismatchException(String durable, List<Difference> differences) {
        super(buildMessage(durable, differences));
        this.durable = durable;
        this.differences = List.copyOf(differences);
    }

    private static String buildMessage(String durable, List<Difference> differences) {
        StringBuilder sb = new StringBuilder();
        sb.append("el servidor devolvio una configuracion distinta de la solicitada para \"")
                .append(durable).append("\":\n");
        for (Difference d : differences) {
            sb.append("  ").append(d.field())
                    .append(": solicitado ").append(d.requested())
                    .append(", efectivo ").append(d.effective()).append('\n');
        }
        sb.append("JetStream sobrescribe algunos campos en silencio (03-delivery.md §2.1). ")
                .append("Si el campo es ack_wait, comprueba que backoff[0] valga exactamente lo mismo.");
        return sb.toString();
    }

    /** Durable name del consumidor afectado. */
    public String durable() {
        return durable;
    }

    /** Campos en los que la config efectiva difiere de la solicitada. */
    public List<Difference> differences() {
        return differences;
    }
}
