/*
 * Propagacion de contexto entre eventos.
 * Contrato normativo: specification/01-envelope.md §5
 *
 * ⚠️ DIVERGENCIA DELIBERADA CON EL SDK DE NODE
 *
 * Node usa AsyncLocalStorage: un publish() en cualquier punto de la pila de llamadas de
 * un handler hereda el contexto del evento entrante sin que nadie pase nada por
 * parametro.
 *
 * Java SI tiene un mecanismo parecido —ThreadLocal— y aun asi NO se usa, por la misma
 * razon por la que Go rechaza el mapa por goroutine ID: el handler puede delegar el
 * trabajo en un ExecutorService, un CompletableFuture o un pool de conexiones, y el
 * ThreadLocal no cruza ninguna de esas fronteras. La propagacion funcionaria en los tests
 * —donde todo es sincrono— y se romperia EN SILENCIO en el primer handler que use un
 * pool, que es exactamente el fallo que este mecanismo existe para evitar. Un
 * InheritableThreadLocal tampoco sirve: se hereda al CREAR el hilo, y los hilos de un
 * pool se crean antes de que exista el evento.
 *
 * Por eso el contexto es EXPLICITO: el SDK se lo entrega al handler y la aplicacion lo
 * pasa de vuelta a publish().
 *
 *     bus.subscribe("pedidos.pedido.v1.creado", (ctx, evento, entrega) -> {
 *         // ctx lleva dentro el contexto del evento entrante
 *         bus.publish(ctx, "facturacion.factura.v1.emitida", payload);
 *         // correlationid, causationid y traceparent se propagan solos
 *     });
 *
 * La consecuencia practica: si la aplicacion pasa EventContext.ROOT (o usa la sobrecarga
 * sin contexto) dentro de un handler, la cadena de correlacion se ROMPE en silencio. En
 * Node eso no puede pasar; aqui si. Es el precio de no tener magia, y a cambio la
 * propagacion es visible en la firma de cada funcion que la necesita — y por tanto
 * auditable en una revision de codigo.
 */
package com.flux;

import java.util.Optional;

/**
 * Lo que un evento entrante lega a los eventos que provoque.
 *
 * @param correlationId se propaga SIN MODIFICAR por toda la cadena. Responde a "¿de que
 *                      flujo de negocio forma parte esto?".
 * @param causationId   {@code id} del evento en curso, que pasa a ser el
 *                      {@code causationid} de lo que se publique desde este handler:
 *                      "¿quien causo esto exactamente?".
 * @param tenantId      tenant del evento en curso. Gana sobre el default de la conexion:
 *                      un evento derivado pertenece al tenant del evento que lo causo, no
 *                      al del servicio.
 * @param traceparent   W3C Trace Context heredado, si lo habia.
 * @param tracestate    W3C Trace Context heredado, si lo habia.
 */
public record EventContext(
        String correlationId,
        String causationId,
        String tenantId,
        String traceparent,
        String tracestate) {

    /**
     * Contexto vacio: el evento nace de cero (una ruta HTTP, un cron) y
     * {@link FluxBus#publish} inicializara su {@code correlationid} con su propio
     * {@code id} — 01-envelope.md §3.1.
     *
     * <p>Se usa una instancia en vez de {@code null} para que las sobrecargas de
     * {@code publish} sin contexto no tengan que distinguir los dos casos.
     */
    public static final EventContext ROOT = new EventContext(null, null, null, null, null);

    /**
     * Deriva el contexto que un evento lega a sus descendientes.
     *
     * <p>Notese que {@code causationId} toma el {@code id} del evento, no su
     * {@code causationid}: la causa de lo que se publique ahora es ESTE evento, no el que
     * lo causo a el — 01-envelope.md §3.2.
     */
    public static EventContext fromEvent(FluxEvent event) {
        return new EventContext(
                event.correlationid(),
                event.id(),
                event.tenantid(),
                event.traceparent(),
                event.tracestate());
    }

    /** Informa si este contexto lleva algo que propagar. */
    public boolean isRoot() {
        return correlationId == null || correlationId.isEmpty();
    }

    public Optional<String> traceparentOpt() {
        return Optional.ofNullable(traceparent);
    }

    /**
     * Extrae un {@code traceparent} W3C del contexto de trazas activo.
     *
     * <p>Otra divergencia con Node: alli el SDK hace un {@code import()} dinamico de
     * {@code @opentelemetry/api} y falla en silencio si no esta instalado. En Java el
     * equivalente seria reflexion sobre {@code io.opentelemetry.api.trace.Span}, que es
     * fragil y opaca; una dependencia dura obligaria a instalar OpenTelemetry a todo
     * servicio que use el SDK aunque no lo use.
     *
     * <p>Asi que se invierte: la aplicacion inyecta la funcion en
     * {@code ConnectOptions.traceparentSupplier}. Con OpenTelemetry es una linea:
     *
     * <pre>{@code
     * .traceparentSupplier(() -> {
     *     SpanContext sc = Span.current().getSpanContext();
     *     return sc.isValid()
     *         ? "00-" + sc.getTraceId() + "-" + sc.getSpanId() + "-" + sc.getTraceFlags().asHex()
     *         : null;
     * })
     * }</pre>
     *
     * <p>Devolver {@code null} significa "no hay span activo" y el atributo se omite.
     */
    @FunctionalInterface
    public interface TraceparentSupplier {
        String get();
    }
}
