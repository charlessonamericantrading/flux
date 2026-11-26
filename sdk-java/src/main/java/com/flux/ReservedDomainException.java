/*
 * Un subject, un contrato o una operacion usa un dominio reservado — 10-state.md §2.7.
 */
package com.flux;

import java.util.List;

/**
 * Un subject o un contrato usa un dominio reservado. PERMANENT.
 *
 * <p>{@code dlq}, {@code state}, {@code rpc} y {@code system} son espacios de nombres del
 * protocolo, y un dominio con ese nombre colisiona con ellos EN EL BROKER:
 * {@code subjects overlap with an existing stream} (A-43). Es el codigo
 * {@code RESERVED_DOMAIN}.
 */
public final class ReservedDomainException extends FluxErrors.FluxException {
    private static final long serialVersionUID = 1L;

    /** Los cuatro dominios reservados — {@code protocol.json → naming.reservedDomains}. */
    public static final List<String> RESERVED = List.of("dlq", "state", "rpc", "system");

    private final String domain;

    public ReservedDomainException(String domain, String what) {
        super("el dominio \"" + domain + "\" esta reservado al protocolo (reservados: "
                + String.join(", ", RESERVED) + ") y no puede usarse en "
                + ("operacion".equals(what) ? "una " : "un ") + what + ". Cada uno es un espacio de "
                + "nombres propio —`state.`, `dlq.`, `rpc.`—, y un evento publicado ahi no pasaria por "
                + "el chequeo de secuencia del estado (10-state.md §2.7). Elige otro dominio."
                // El nombre de una operacion NO lleva el prefijo: lo pone el SDK (11-request-reply.md §2.2).
                + ("operacion".equals(what) && "rpc".equals(domain)
                        ? " Si lo que escribiste es el subject de NATS (`rpc.clientes.cliente.v1.obtener`), "
                                + "quita el prefijo `rpc.`: lo pone el SDK, y el nombre de la operacion es "
                                + "`clientes.cliente.v1.obtener` (11-request-reply.md §2.2)."
                        : ""),
                "RESERVED_DOMAIN", null);
        this.domain = domain;
    }

    /** El dominio que choco. */
    public String domain() {
        return domain;
    }

    @Override
    public ErrorClass errorClass() {
        return ErrorClass.PERMANENT;
    }

    /**
     * Lanza si el primer token de un subject, un contrato o una operacion es un dominio
     * reservado. Se comprueba ANTES que la forma: {@code dlq.pedidos.pedido.v1.creado} tiene
     * cinco tokens y «debe tener cuatro» no diria que pasa de verdad.
     */
    static void assertNotReserved(String subjectOrContract, String what) {
        String domain = subjectOrContract == null ? "" : subjectOrContract.split("\\.", -1)[0];
        if (RESERVED.contains(domain)) {
            throw new ReservedDomainException(domain, what);
        }
    }
}
