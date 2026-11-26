/*
 * Las denegaciones de permisos del broker: como las entrega jnats y como se hacen visibles.
 * Contrato normativo: specification/06-security.md §3.7.
 *
 * Cuando una ACL deniega una operacion, el broker NO contesta: descarta el mensaje y avisa aparte,
 * por la conexion, con un `-ERR 'Permissions Violation for …'`. Lo que ve la aplicacion depende de
 * la libreria. Medido con jnats 2.20.5 contra nats-server 2.14.5 (Apendice A, A-200 a A-209):
 *
 *   · una peticion a la API de JetStream denegada NO falla: espera su timeout (2 s) y lanza un
 *     `IOException` generico; el aviso llega a `ErrorListener.errorOccurred` en ~3 ms;
 *   · un `request` core a un subject denegado devuelve `null` tras el timeout, igual que un
 *     `no responders`; con la variante de futuro, el timeout lo pone la limpieza de la libreria (5 s);
 *   · una suscripcion denegada no lanza nada: `isActive()` sigue en true y solo hay el aviso;
 *   · un `ack`, un `nak`, un WIP o el pull de un consumidor denegados: silencio.
 *
 * El SDK DEBE escuchar el aviso y competirlo con la respuesta (`guard`), o el sintoma es un timeout.
 *
 * El aviso llega por el hilo de callbacks de la libreria, que va por detras del hilo lector: tras un
 * `flush()` (PING/PONG) el `-ERR` ya se leyo, pero su callback puede no haberse ejecutado (medido: en
 * 816 de 2000 con la maquina cargada). `Statistics.getErrs()` SI se actualiza en el hilo lector, antes
 * del PONG: `fence` espera a que los callbacks alcancen esa cuenta. No hay temporizador fijo.
 */
package com.flux;

import com.flux.SecurityErrors.Operation;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Registro de denegaciones de una conexion. */
final class Denials {

    /** Una denegacion: que se intentaba y el subject, completo y sin normalizar. */
    record Denial(Operation operation, String subject) {
    }

    private static final Pattern VIOLATION =
            Pattern.compile("Permissions Violation for (Publish|Subscription) to \"([^\"]*)\"");

    /** Quien espera: subject exacto → futuros pendientes. */
    private final Map<String, List<CompletableFuture<Denial>>> waiters = new ConcurrentHashMap<>();

    /** Quien espera la denegacion de CUALQUIER subject con este prefijo (el inbox de las respuestas: su subject lleva un nuid). */
    private final Map<String, List<CompletableFuture<Denial>>> prefixWaiters = new ConcurrentHashMap<>();

    /** Subjects (normalizados) ya avisados: «avisar una vez por subject». */
    private final Map<String, Operation> seen = new ConcurrentHashMap<>();

    /** Cuantos callbacks de `errorOccurred` se han entregado: lo que `fence` compara con `getErrs()`. */
    private final AtomicLong delivered = new AtomicLong();

    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "flux-guard");
        t.setDaemon(true);
        return t;
    });

    /** La denegacion que describe {@code text}, o {@code null} si es otro {@code -ERR}. */
    static Denial parse(String text) {
        if (text == null) {
            return null;
        }
        Matcher m = VIOLATION.matcher(text);
        if (!m.find()) {
            return null;
        }
        return new Denial("Publish".equals(m.group(1)) ? Operation.PUBLISH : Operation.SUBSCRIBE, m.group(2));
    }

    /**
     * Normaliza el subject de un ack (lleva secuencia y marca de tiempo) a {@code …<durable>.>}, y el de
     * una respuesta a una peticion ({@code _INBOX.<servicio>.<nuid>.<token>}, distinto en cada peticion) a
     * {@code _INBOX.<servicio>.>}: sin ello, un dueno sin {@code allow_responses} registraria una linea por
     * cada respuesta denegada, y el aviso de «una vez por subject» no avisaria de nada.
     */
    static String normalize(String subject) {
        String[] t = subject.split("\\.", -1);
        if (subject.startsWith("$JS.ACK.") && t.length >= 4) {
            return String.join(".", java.util.Arrays.copyOfRange(t, 0, 4)) + ".>";
        }
        if (subject.startsWith("_INBOX.") && t.length > 3) {
            return t[0] + "." + t[1] + ".>";
        }
        return subject;
    }

    /**
     * Lo llama el callback {@code errorOccurred} de la libreria. Despierta a quien espera ESE subject y
     * devuelve la denegacion si es la primera vez que se ve (para registrarla), o {@code null}.
     */
    Denial onError(String text) {
        try {
            Denial d = parse(text);
            if (d == null) {
                return null;
            }
            // Quien espera una respuesta se entera ANTES de la deduplicacion: cada peticion denegada necesita su aviso.
            List<CompletableFuture<Denial>> w = waiters.get(d.subject());
            if (w != null) {
                for (CompletableFuture<Denial> f : w) {
                    f.complete(d);
                }
            }
            for (Map.Entry<String, List<CompletableFuture<Denial>>> e : prefixWaiters.entrySet()) {
                if (d.subject().startsWith(e.getKey())) {
                    for (CompletableFuture<Denial> f : e.getValue()) {
                        f.complete(d);
                    }
                }
            }
            return seen.putIfAbsent(normalize(d.subject()), d.operation()) == null ? d : null;
        } finally {
            // La cuenta de callbacks entregados se anota AL FINAL: `fence` la compara con la de `-ERR` leidos y, si se anotase antes,
            // podria dejar pasar a quien mira `seen` justo en el hueco entre contar y registrar (medido: 7 de 40 `respond()` sin
            // permiso arrancaron mudos).
            delivered.incrementAndGet();
        }
    }

    /** Que se denego sobre este subject (normalizado), o {@code null}. */
    Operation deniedOperation(String subject) {
        return seen.get(normalize(subject));
    }

    /** ¿Se ha visto denegado este subject (normalizado)? */
    boolean wasDenied(String subject) {
        return seen.containsKey(normalize(subject));
    }

    /** Olvida una denegacion: un intento anterior sobre este durable no debe condenar a este. */
    void forget(String subject) {
        seen.remove(normalize(subject));
    }

    /** Espera una denegacion de PUBLICAR en {@code subject}. {@link #unwatch} la quita. */
    CompletableFuture<Denial> watch(String subject) {
        CompletableFuture<Denial> f = new CompletableFuture<>();
        waiters.computeIfAbsent(subject, k -> new CopyOnWriteArrayList<>()).add(f);
        return f;
    }

    void unwatch(String subject, CompletableFuture<Denial> f) {
        List<CompletableFuture<Denial>> w = waiters.get(subject);
        if (w != null) {
            w.remove(f);
            if (w.isEmpty()) {
                waiters.remove(subject, w);
            }
        }
    }

    /**
     * Espera a que los callbacks de {@code errorOccurred} alcancen los {@code -ERR} que el lector ya leyo.
     * Se llama DESPUES de un {@code flush()}: el protocolo es ordenado, asi que un {@code -ERR} de algo
     * anterior se ha leido antes que el PONG. No espera «un poco»: espera a una cuenta, y solo cede si la
     * libreria no entrega alguno (limite de seguridad: 2 s).
     *
     * @return {@code true} si los callbacks alcanzaron la cuenta.
     */
    boolean fence(LongSupplier errsRead) throws InterruptedException {
        long target = errsRead.getAsLong();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (delivered.get() < target) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            java.util.concurrent.locks.LockSupport.parkNanos(50_000);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
        return true;
    }

    /**
     * Ejecuta {@code call} vigilando las denegaciones de {@code subjects} (los subjects EXACTOS a los que
     * publica: una denegacion de cualquier otro no es culpa de esta operacion). Si el broker deniega uno,
     * lanza {@link SecurityErrors.PermissionDeniedException} en milisegundos en vez de esperar el timeout
     * de la libreria.
     *
     * <p>La llamada va en otro hilo porque las de la API de JetStream de la libreria son bloqueantes y no
     * se pueden abortar; si gana la denegacion, la llamada sigue hasta su timeout y su resultado se
     * descarta.
     */
    <T> T guard(String service, List<String> subjects, String hint, Callable<T> call) throws Exception {
        List<CompletableFuture<Denial>> denials = new java.util.ArrayList<>();
        for (String s : subjects) {
            denials.add(watch(s));
        }
        CompletableFuture<T> result = new CompletableFuture<>();
        Future<?> task = executor.submit(() -> {
            try {
                result.complete(call.call());
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        try {
            List<CompletableFuture<?>> all = new java.util.ArrayList<>(denials);
            all.add(result);
            try {
                CompletableFuture.anyOf(all.toArray(new CompletableFuture[0])).get(GUARD_LIMIT_SECONDS, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                // la propia llamada fallo: se propaga abajo, con su causa
            }
            // Una respuesta que ya llego gana: la denegacion de otra operacion con el mismo subject no cuenta.
            if (result.isDone()) {
                try {
                    return result.get();
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof Exception ex) {
                        throw ex;
                    }
                    throw e;
                }
            }
            for (CompletableFuture<Denial> d : denials) {
                if (d.isDone()) {
                    task.cancel(true);
                    Denial denial = d.get();
                    throw new SecurityErrors.PermissionDeniedException(service, denial.operation(),
                            denial.subject(), hint, null);
                }
            }
            task.cancel(true);
            throw new TimeoutException("la llamada no termino en " + GUARD_LIMIT_SECONDS + " s");
        } finally {
            for (int i = 0; i < denials.size(); i++) {
                unwatch(subjects.get(i), denials.get(i));
            }
        }
    }

    /** Tope de seguridad de {@link #guard}: la libreria ya tiene sus propios timeouts (2 s por peticion). */
    private static final long GUARD_LIMIT_SECONDS = 60;

    /**
     * Para los que esperan sin hilo propio (una peticion core ya asincrona): lo que ocurra primero entre la respuesta, la denegacion
     * de PUBLICAR en {@code subject} y el timeout. {@code start} lanza la operacion: se llama DESPUES de registrar el aviso. Si se
     * registrase despues, un {@code -ERR} que llegase entre medias —el broker contesta en unas decenas de microsegundos— se perderia y
     * quien llama esperaria su timeout para algo que ya estaba denegado (medido: un {@code put} denegado, 5 s).
     */
    <T> T race(String service, String subject, String hint, Supplier<CompletableFuture<T>> start, long timeoutMillis)
            throws Exception {
        return race(service, subject, hint, null, null, start, timeoutMillis);
    }

    /**
     * Como {@link #race(String, String, String, Supplier, long)} y ademas vigila la SUSCRIPCION al inbox de las respuestas
     * ({@code inboxPrefix}, p. ej. {@code _INBOX.<servicio>.}): la libreria la crea al primer {@code request} y, sin permiso, la
     * respuesta nunca llegaria (core NATS no avisa) y quien llama solo veria un timeout. Una denegacion ya vista de esa suscripcion
     * falla al instante, tambien para las llamadas siguientes.
     */
    <T> T race(String service, String subject, String hint, String inboxPrefix, String inboxHint,
               Supplier<CompletableFuture<T>> start, long timeoutMillis) throws Exception {
        if (inboxPrefix != null) {
            Operation seenInbox = seen.get(normalize(inboxPrefix + "x.y"));
            if (seenInbox == Operation.SUBSCRIBE) {
                throw new SecurityErrors.PermissionDeniedException(service, Operation.SUBSCRIBE, inboxPrefix + ">", inboxHint, null);
            }
        }
        CompletableFuture<Denial> denial = watch(subject);
        CompletableFuture<Denial> inbox = null;
        if (inboxPrefix != null) {
            inbox = new CompletableFuture<>();
            prefixWaiters.computeIfAbsent(inboxPrefix, k -> new CopyOnWriteArrayList<>()).add(inbox);
        }
        try {
            CompletableFuture<T> pending = start.get();
            try {
                CompletableFuture<?>[] all = inbox == null ? new CompletableFuture<?>[] {pending, denial}
                        : new CompletableFuture<?>[] {pending, denial, inbox};
                CompletableFuture.anyOf(all).get(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                // la propia peticion fallo: se propaga abajo, con su causa
            }
            if (pending.isDone()) {
                return pending.get();
            }
            if (denial.isDone()) {
                Denial d = denial.get();
                pending.cancel(true);
                throw new SecurityErrors.PermissionDeniedException(service, d.operation(), d.subject(), hint, null);
            }
            if (inbox != null && inbox.isDone()) {
                Denial d = inbox.get();
                pending.cancel(true);
                throw new SecurityErrors.PermissionDeniedException(service, d.operation(), d.subject(), inboxHint, null);
            }
            pending.cancel(true);
            throw new TimeoutException();
        } finally {
            unwatch(subject, denial);
            if (inbox != null) {
                List<CompletableFuture<Denial>> w = prefixWaiters.get(inboxPrefix);
                if (w != null) {
                    w.remove(inbox);
                    if (w.isEmpty()) {
                        prefixWaiters.remove(inboxPrefix, w);
                    }
                }
            }
        }
    }

    /** Lo mismo con una operacion que ya se lanzo (solo para quien no puede reordenar). */
    <T> T race(String service, String subject, String hint, CompletableFuture<T> pending, long timeoutMillis)
            throws Exception {
        return race(service, subject, hint, () -> pending, timeoutMillis);
    }

    /** Las denegaciones vistas, para los tests. */
    Set<String> seenSubjects() {
        return seen.keySet();
    }

    void close() {
        executor.shutdownNow();
    }
}
