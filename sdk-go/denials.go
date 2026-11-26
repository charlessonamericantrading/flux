// Las denegaciones de permisos se ven.
// Contrato normativo: specification/06-security.md §3.7, 11-request-reply.md §7.1
//
// Un servicio al que le falta un permiso NO puede parecer sano. El broker no ayuda: cuando una ACL deniega
// una publicación o una suscripción, el servidor descarta el mensaje y manda un `-ERR 'Permissions
// Violation …'` aparte, por la conexión; no hay respuesta que pueda fallar.
//
// Lo que MIDIÓ este SDK de `nats.go` v1.37.0 contra nats-server 2.14.5 (PLAN.md, A-190 a A-199):
//
//   - Una petición a un subject denegado es un `nats.ErrTimeout` tras su plazo; la denegación llega
//     ~1 ms después de publicar, por el `ErrorHandler` de la conexión y nada más: `nats.go` no la
//     entrega a la operación. Ni la de la API de JetStream (`context deadline exceeded` a los 2 s).
//   - Una SUSCRIPCIÓN denegada no devuelve error: `QueueSubscribe` devuelve nil. El `-ERR` llega por el
//     mismo `ErrorHandler`, y TAMBIÉN queda en `nc.LastError()`, que se escribe en el lector de la
//     conexión ANTES de procesar el PONG de un `Flush()`: tras un Flush, `LastError()` ya la tiene
//     (0 fallos en 2 000 intentos), mientras que el `ErrorHandler` —una goroutine aparte— aún no se había
//     ejecutado en 180 de 2 000. Por eso la comprobación de una suscripción usa `LastError()`.
//   - Un `ack`, un WIP o un pull denegados no dan nada más que ese aviso asíncrono.
//
// Así que el SDK hace lo que 06-security.md §3.7 manda con una librería que no entrega la denegación:
// ESCUCHA el `-ERR` y lo COMPITE con la respuesta (`guard`), y registra las que no tienen operación a la
// que fallar (`ack`, pull) en el `Logger`, una vez por subject.

package flux

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// denial es una denegación que el broker notificó.
type denial struct {
	// op es "publish" o "subscribe".
	op string
	// subject es el subject tal como lo comunicó el broker, sin normalizar.
	subject string
	// queue es el grupo de cola de una suscripción denegada, si lo había.
	queue string
}

// permissionViolationRE reconoce el texto que `nats.go` entrega: `nats: Permissions Violation for
// Publish to "x"` o `… for Subscription to "x" using queue "q"`.
var permissionViolationRE = regexp.MustCompile(
	`Permissions Violation for (Publish|Subscription) to "([^"]*)"(?: using queue "([^"]*)")?`)

// parseDenial interpreta el error que `nats.go` entrega al `ErrorHandler`. Cuando la librería solo da texto,
// se interpreta el texto: es el único sitio donde hacerlo está justificado.
func parseDenial(err error) (denial, bool) {
	if err == nil {
		return denial{}, false
	}
	m := permissionViolationRE.FindStringSubmatch(err.Error())
	if m == nil {
		return denial{}, false
	}
	op := "publish"
	if m[1] == "Subscription" {
		op = "subscribe"
	}
	return denial{op: op, subject: m[2], queue: m[3]}, true
}

// normalizeDeniedSubject normaliza el subject de un ack (lleva secuencia y marca de tiempo) a
// `$JS.ACK.<stream>.<durable>.>` y el de una respuesta a una petición (`_INBOX.<servicio>.<nuid>.<token>`,
// distinto en cada petición) a `_INBOX.<servicio>.>`. Solo para el LOG: sin ello, un dueño sin
// `allow_responses` registraría una línea por cada respuesta denegada y el aviso de «una vez por subject»
// no avisaría de nada. El error de la operación lleva siempre el subject completo (04-errors.md §5.2).
func normalizeDeniedSubject(subject string) string {
	if strings.HasPrefix(subject, "$JS.ACK.") {
		return strings.Join(strings.Split(subject, ".")[:4], ".") + ".>"
	}
	if strings.HasPrefix(subject, "_INBOX.") {
		if partes := strings.Split(subject, "."); len(partes) > 3 {
			return strings.Join(partes[:2], ".") + ".>"
		}
	}
	return subject
}

// denialWatch es un aviso para quien espera algo y quiere enterarse si el broker lo deniega.
type denialWatch struct {
	hub   *denialHub
	match func(denial) bool
	// C recibe la primera denegación que cumple `match`.
	C chan denial
}

// Stop quita el aviso.
func (w *denialWatch) Stop() {
	if w.hub == nil {
		return
	}
	w.hub.mu.Lock()
	delete(w.hub.watchers, w)
	w.hub.mu.Unlock()
}

// denialHub reparte las denegaciones que `nats.go` entrega al `ErrorHandler`.
type denialHub struct {
	service string
	logger  *slog.Logger

	mu       sync.Mutex
	warned   map[string]struct{}
	watchers map[*denialWatch]struct{}
}

func newDenialHub(service string, logger *slog.Logger) *denialHub {
	return &denialHub{
		service: service, logger: logger,
		warned: map[string]struct{}{}, watchers: map[*denialWatch]struct{}{},
	}
}

// handle es el `ErrorHandler` de la conexión.
func (h *denialHub) handle(_ *nats.Conn, _ *nats.Subscription, err error) {
	d, ok := parseDenial(err)
	if !ok {
		if err != nil && h.logger != nil {
			h.logger.Log(context.Background(), slog.LevelWarn, "[flux] error asíncrono de la conexión NATS: "+err.Error())
		}
		return
	}

	h.mu.Lock()
	// Quien espera una respuesta se entera ANTES de la deduplicación: cada petición denegada necesita su aviso.
	for w := range h.watchers {
		if w.match(d) {
			select {
			case w.C <- d:
			default:
			}
		}
	}
	subject := normalizeDeniedSubject(d.subject)
	clave := d.op + " " + subject
	_, ya := h.warned[clave]
	h.warned[clave] = struct{}{}
	h.mu.Unlock()
	if ya || h.logger == nil {
		return // se avisa una vez por subject
	}

	efecto := ""
	switch {
	case strings.HasPrefix(subject, "$JS.ACK."):
		efecto = " Sin permiso de ack, cada evento se procesa y se REENTREGA cada ack_wait, sin fin y sin error."
	case strings.HasPrefix(subject, "$JS.API.CONSUMER.MSG.NEXT."):
		efecto = " Sin permiso de pull, el consumidor no recibe nada y parece sano."
	case strings.HasPrefix(subject, "_INBOX.") && d.op == "publish":
		efecto = " Es la RESPUESTA a una petición: sin `allow_responses` en su ACL el dueño no puede contestar, y quien " +
			"llama solo verá un timeout (RPC_TIMEOUT). Acótalo: `{ max: 1, expires: 30s }` (11-request-reply.md §7.2)."
	}
	accion := "publicar en"
	if d.op == "subscribe" {
		accion = "suscribirse a"
	}
	h.logger.Log(context.Background(), slog.LevelError, fmt.Sprintf(
		"[flux] el broker denegó %s %q a %q: falta ese permiso en su ACL.%s (06-security.md §3)",
		accion, subject, h.service, efecto))
}

// watch registra un aviso. Hay que llamar a Stop.
func (h *denialHub) watch(match func(denial) bool) *denialWatch {
	w := &denialWatch{hub: h, match: match, C: make(chan denial, 1)}
	if h == nil { // un Bus de prueba sin conexión: no hay nadie que avise
		return w
	}
	h.mu.Lock()
	h.watchers[w] = struct{}{}
	h.mu.Unlock()
	return w
}

// guard devuelve un contexto que se cancela con PERMISSION_DENIED si el broker deniega PUBLICAR en alguno
// de `subjects` (exactos), y la función que lo libera. Es cómo se compite una petición con el `-ERR` que
// la librería no entrega (06-security.md §3.7): sin ello, la petición esperaría su timeout.
//
//	ctx, done := b.guard(ctx, subject)
//	defer done()
//	resp, err := nc.RequestWithContext(ctx, subject, …)
//	if err != nil { return denied(ctx, err) }
func (b *Bus) guard(parent context.Context, subjects ...string) (context.Context, func()) {
	return b.guardMatch(parent, func(d denial) bool {
		if d.op != "publish" {
			return false
		}
		for _, s := range subjects {
			if d.subject == s {
				return true
			}
		}
		return false
	})
}

// guardMatch es guard con un predicado: para vigilar un subject que la librería compone por su cuenta (el de
// `CONSUMER.CREATE`, que lleva el filtro detrás) o una familia (`_INBOX.<servicio>.*`).
func (b *Bus) guardMatch(parent context.Context, match func(denial) bool) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	w := b.denials.watch(match)
	fin := make(chan struct{})
	go func() {
		select {
		case d := <-w.C:
			cancel(newPermissionDenied(b.opts.Service, d.op, d.subject, "", nil))
		case <-fin:
		}
	}()
	return ctx, func() {
		close(fin)
		w.Stop()
		cancel(nil)
	}
}

// denied traduce el error de una operación hecha bajo `guard`: si fue una denegación, devuelve
// PERMISSION_DENIED; si no, el error de la librería tal cual.
func denied(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if pd := deniedCause(ctx); pd != nil {
		return pd
	}
	return err
}

// deniedCause devuelve la denegación que canceló el contexto, o nil.
func deniedCause(ctx context.Context) *PermissionDeniedError {
	var pd *PermissionDeniedError
	if c := context.Cause(ctx); c != nil {
		if e, ok := c.(*PermissionDeniedError); ok {
			pd = e
		}
	}
	return pd
}

// lastDenial devuelve la última denegación que la conexión anotó, si es de esa operación y ese subject. Tras
// un `Flush()` es determinista (ver la cabecera): `nc.err` se escribe en el lector antes del PONG.
func lastDenial(nc *nats.Conn, op, subject string) (denial, bool) {
	d, ok := parseDenial(nc.LastError())
	if !ok || d.op != op || d.subject != subject {
		return denial{}, false
	}
	return d, true
}

// flushWithin hace un PING/PONG con plazo. Como el protocolo es ordenado, cuando vuelve el servidor ya ha
// procesado —y, si las denegó, contestado— todo lo que se publicó o se suscribió antes.
func flushWithin(ctx context.Context, nc *nats.Conn, max time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, max)
	defer cancel()
	return nc.FlushWithContext(ctx)
}
