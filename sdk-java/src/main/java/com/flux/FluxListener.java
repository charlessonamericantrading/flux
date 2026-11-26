/*
 * Lo que jnats notifica de la conexion: errores del broker, excepciones y cambios de estado.
 * Contrato normativo: specification/06-security.md §3.7.
 *
 * jnats entrega todo esto por callbacks, en un hilo propio y SIN relacion con la llamada que lo causo. Este
 * oyente hace tres cosas con ello:
 *
 *   1. alimenta `Denials`: una denegacion de permisos despierta a quien espera ESE subject (falla rapido
 *      con PERMISSION_DENIED en vez de esperar el timeout) y se registra UNA vez por subject, con lo que
 *      significa para el servicio;
 *   2. recuerda las excepciones del intento de PRIMERA conexion: `Nats.connect` lanza un `IOException`
 *      generico y lo que dice POR QUE solo llega aqui;
 *   3. avisa de lo que de otro modo seria silencio: un consumidor cuyo durable se borra, una cola que se
 *      llena y descarta mensajes sin error, un latido de consumidor que no llega.
 */
package com.flux;

import com.flux.Denials.Denial;
import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.JetStreamSubscription;
import io.nats.client.support.Status;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** El oyente de una conexion. Package-private: lo crea {@link FluxBus#connect}. */
final class FluxListener implements ErrorListener, ConnectionListener {

    /** Lo que paso en UN intento de primera conexion. */
    static final class Attempt {
        private final List<Throwable> exceptions = new CopyOnWriteArrayList<>();
        private final CountDownLatch closed = new CountDownLatch(1);

        List<Throwable> exceptions() {
            return exceptions;
        }

        /**
         * Espera al CIERRE de ese intento. Los callbacks van por orden en un solo hilo, y la libreria notifica
         * la excepcion ANTES del cierre: cuando llega el cierre, la causa ya esta aqui. No es un temporizador
         * a ciegas, es esperar a un hecho; el limite es solo de seguridad.
         */
        boolean awaitClosed(long millis) throws InterruptedException {
            return closed.await(millis, TimeUnit.MILLISECONDS);
        }
    }

    private final String service;
    private final System.Logger logger;
    final Denials denials = new Denials();
    private final AtomicReference<Attempt> attempt = new AtomicReference<>();
    private final Map<String, Boolean> slowLogged = new ConcurrentHashMap<>();

    FluxListener(String service, System.Logger logger) {
        this.service = service;
        this.logger = logger;
    }

    Attempt beginAttempt() {
        Attempt a = new Attempt();
        attempt.set(a);
        return a;
    }

    void log(System.Logger.Level level, String message) {
        if (logger != null) {
            logger.log(level, "[flux] " + message);
        }
    }

    // ─── ErrorListener ───────────────────────────────────────────────────────

    @Override
    public void errorOccurred(Connection conn, String error) {
        Denial first = denials.onError(error);
        if (first != null) {
            logDenied(first);
        } else if (Denials.parse(error) == null) {
            log(System.Logger.Level.WARNING, "el broker aviso de un error: " + error);
        }
    }

    /**
     * Una denegacion SIN respuesta —un ack, un WIP, un pull— no tiene otra salida que el log: es una publicacion
     * sin respuesta, no se puede devolver. Se dice QUE falta y la CONSECUENCIA (06-security.md §3.7).
     */
    private void logDenied(Denial d) {
        String subject = Denials.normalize(d.subject());
        String effect;
        if (subject.startsWith("$JS.ACK.")) {
            effect = " Sin permiso de ack, cada evento se procesa y se REENTREGA cada ack_wait, sin fin y sin error.";
        } else if (subject.startsWith("$JS.API.CONSUMER.MSG.NEXT.")) {
            effect = " Sin permiso de pull, el consumidor no recibe nada y parece sano.";
        } else if (subject.startsWith("_INBOX.") && d.operation() == SecurityErrors.Operation.PUBLISH) {
            effect = " Es la RESPUESTA a una peticion: sin `allow_responses` en su ACL el dueno no puede contestar, y "
                    + "quien llama solo vera un timeout (RPC_TIMEOUT). Acotalo: `{ max: 1, expires: 30s }` "
                    + "(11-request-reply.md §7.2).";
        } else {
            effect = "";
        }
        log(System.Logger.Level.ERROR, "el broker denego " + d.operation().phrase() + " \"" + subject + "\" a \""
                + service + "\": falta ese permiso en su ACL." + effect + " (06-security.md §3)");
    }

    @Override
    public void exceptionOccurred(Connection conn, Exception exp) {
        Attempt a = attempt.get();
        if (a != null) {
            a.exceptions.add(exp);
        }
        log(System.Logger.Level.DEBUG, "excepcion de la conexion: " + exp);
    }

    @Override
    public void slowConsumerDetected(Connection conn, Consumer consumer) {
        String key = String.valueOf(System.identityHashCode(consumer));
        if (slowLogged.putIfAbsent(key, Boolean.TRUE) == null) {
            log(System.Logger.Level.ERROR, "la cola de una suscripcion se lleno (" + consumer.getPendingMessageCount()
                    + " mensajes pendientes): jnats DESCARTA los mensajes que no caben, sin ningun error "
                    + "(11-request-reply.md §13). Si es un dueno de una operacion, quien llamo vera RPC_TIMEOUT.");
        }
    }

    @Override
    public void pullStatusError(Connection conn, JetStreamSubscription sub, Status status) {
        if (status.getCode() == 409 && String.valueOf(status.getMessage()).contains("Consumer Deleted")) {
            // Lo trata quien consume (el watch de estado lo reabre al recibir la excepcion): aqui solo queda constancia.
            log(System.Logger.Level.DEBUG, "el consumidor \"" + sub.getConsumerName() + "\" se borro bajo un consumo abierto");
            return;
        }
        log(System.Logger.Level.WARNING, "el consumo de \"" + sub.getConsumerName() + "\" recibio un error del broker: "
                + status);
    }

    @Override
    public void heartbeatAlarm(Connection conn, JetStreamSubscription sub, long lastStreamSequence,
                               long lastConsumerSequence) {
        log(System.Logger.Level.WARNING, "no llegan los latidos del consumidor \"" + sub.getConsumerName()
                + "\": el broker no contesta a su consumo o el consumidor se borro");
    }

    // ─── ConnectionListener ──────────────────────────────────────────────────

    @Override
    public void connectionEvent(Connection conn, Events type) {
        if (type == Events.CLOSED) {
            Attempt a = attempt.get();
            if (a != null) {
                a.closed.countDown();
            }
        } else if (type == Events.DISCONNECTED) {
            log(System.Logger.Level.WARNING, "conexion con el broker perdida: se reconecta sola");
        } else if (type == Events.RECONNECTED) {
            log(System.Logger.Level.INFO, "reconectado al broker");
        }
    }
}
