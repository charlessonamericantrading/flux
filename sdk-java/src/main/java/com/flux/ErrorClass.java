/*
 * Las tres clases de la taxonomia de errores de flux.
 * Contrato normativo: specification/04-errors.md §1
 */
package com.flux;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Clase de un error. Determina la accion sobre el mensaje de NATS.
 *
 * <p>El error mas caro de un sistema de eventos no es perder un mensaje: es reintentar
 * durante 51 minutos algo que nunca va a funcionar mientras los eventos sanos se
 * acumulan detras. Por eso flux no tiene "una politica de reintentos": tiene una
 * taxonomia, y cada clase determina una accion distinta.
 */
public enum ErrorClass {

    /**
     * El fallo es del entorno y podria desaparecer solo.
     * → {@code nak(delay)} y reintento con el backoff canonico.
     */
    RETRYABLE("retryable"),

    /**
     * El evento es valido pero este consumidor nunca podra procesarlo por mucho que
     * espere. → {@code term()} + DLQ inmediato, sin reintentos.
     */
    PERMANENT("permanent"),

    /**
     * El mensaje ni siquiera es interpretable. → {@code term()} + DLQ + alerta.
     * Lo detecta el SDK antes del handler; casi siempre significa que un productor esta
     * roto. Es el unico caso que DEBE despertar a alguien.
     */
    POISON("poison");

    private final String wire;

    ErrorClass(String wire) {
        this.wire = wire;
    }

    /** Valor tal como aparece en {@code dlqreason} y en protocol.json. */
    @JsonValue
    public String wire() {
        return wire;
    }

    /** Traduce la clase a la razon que se escribe en la DLQ — 04-errors.md §3. */
    public FluxEvent.DlqReason toDlqReason() {
        return switch (this) {
            case RETRYABLE -> FluxEvent.DlqReason.RETRYABLE;
            case PERMANENT -> FluxEvent.DlqReason.PERMANENT;
            case POISON -> FluxEvent.DlqReason.POISON;
        };
    }
}
