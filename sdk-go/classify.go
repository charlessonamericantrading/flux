// Clasificación de errores del handler.
// Contrato normativo: specification/04-errors.md §2
//
// Este fichero es el punto donde el protocolo se encuentra con la realidad operativa
// del ecosistema. Todo lo demás en el SDK es mecánica; esto es política — y por eso
// la política es un parámetro, no una constante.

package flux

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"
)

// HTTPStatusError es cualquier error que sepa decir con qué status HTTP falló una
// dependencia. El status es la señal más fiable que da una dependencia sobre si
// merece la pena reintentar.
//
// ⚠️ Divergencia con Node, y es una divergencia de fondo: allí el clasificador hurga
// en `err.status`, `err.statusCode` y `err.response.status` porque en JavaScript
// cualquier objeto puede tener cualquier propiedad. Go no tiene equivalente sin
// reflexión, así que el contrato se hace explícito: implementa esta interfaz —o usa
// NewHTTPError— y el clasificador la encuentra con errors.As, incluso a través de
// errores envueltos con %w.
type HTTPStatusError interface {
	error
	HTTPStatus() int
}

// RetryAfterError es un error que sabe cuánto pide esperar la dependencia
// (la cabecera Retry-After de una respuesta 429 o 503).
type RetryAfterError interface {
	error
	RetryAfter() time.Duration
}

// HTTPError es la implementación de conveniencia de HTTPStatusError, para no obligar
// a cada aplicación a escribir la suya.
//
//	if resp.StatusCode >= 400 {
//	    return flux.NewHTTPError(resp.StatusCode, "POST /v1/charges", retryAfterOf(resp))
//	}
type HTTPError struct {
	Status  int
	Message string
	// Delay es el Retry-After anunciado por la dependencia; cero si no lo anunció.
	Delay time.Duration
	Cause error
}

// NewHTTPError construye un HTTPError. `delay` puede ser cero.
func NewHTTPError(status int, message string, delay time.Duration) *HTTPError {
	return &HTTPError{Status: status, Message: message, Delay: delay}
}

func (e *HTTPError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("HTTP %d", e.Status)
	}
	return fmt.Sprintf("HTTP %d: %s", e.Status, e.Message)
}

// HTTPStatus implementa HTTPStatusError.
func (e *HTTPError) HTTPStatus() int { return e.Status }

// RetryAfter implementa RetryAfterError.
func (e *HTTPError) RetryAfter() time.Duration { return e.Delay }

// Unwrap expone la causa a errors.Is y errors.As.
func (e *HTTPError) Unwrap() error { return e.Cause }

// ─── Tablas de la spec ───────────────────────────────────────────────────────

// RetryableHTTPStatus son los status que merecen reintento — 04-errors.md §1.1.
//
// Nótese qué NO está aquí: 400, 403, 404 y 422 son PERMANENT. Reintentarlos es
// gastar 51 minutos para obtener exactamente la misma respuesta.
var RetryableHTTPStatus = map[int]struct{}{
	429: {}, 502: {}, 503: {}, 504: {},
}

// transientSyscalls son los errno inequívocamente transitorios de protocol.json.
//
// Se comparan con errors.Is contra los valores de `syscall` en vez de con las cadenas
// "ECONNRESET" del SDK de Node: en Go el errno viaja tipado dentro de *net.OpError,
// así que la comparación es exacta y no depende de cómo formatee el mensaje la
// librería que lo envolvió.
var transientSyscalls = []struct {
	errno syscall.Errno
	name  string
}{
	{syscall.ECONNRESET, "ECONNRESET"},
	{syscall.ECONNREFUSED, "ECONNREFUSED"},
	{syscall.ETIMEDOUT, "ETIMEDOUT"},
	{syscall.EPIPE, "EPIPE"},
	{syscall.EHOSTUNREACH, "EHOSTUNREACH"},
	{syscall.ENETUNREACH, "ENETUNREACH"},
	{syscall.ECONNABORTED, "ECONNABORTED"},
}

// syscallName devuelve el nombre del errno transitorio, o "" si no lo es.
func syscallName(err error) string {
	for _, s := range transientSyscalls {
		if errors.Is(err, s.errno) {
			return s.name
		}
	}
	return ""
}

// isTimeout detecta las tres formas que tiene un timeout de llegar hasta aquí.
//
// El EAI_AGAIN de la lista de protocol.json —fallo temporal de DNS— no existe como
// errno en Go: se expone como *net.DNSError con IsTemporary, así que se trata aquí.
func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && (dnsErr.IsTimeout || dnsErr.IsTemporary) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// ─── Clasificador ────────────────────────────────────────────────────────────

// Rule es una regla de clasificación de la aplicación. Devuelve ok=false para ceder
// al resto de la cadena. Se evalúan antes que todo lo demás salvo los errores que ya
// declaran su clase.
type Rule func(err error) (Classification, bool)

// ClassifierOptions es la política de clasificación del consumidor.
type ClassifierOptions struct {
	// UnknownErrorPolicy decide qué hacer con un error que no encaja en ninguna
	// regla conocida.
	//
	// ClassPermanent (default de la spec) — va a la DLQ sin gastar reintentos. La
	// cola sigue fluyendo y el evento se puede reproducir cuando se entienda qué pasó.
	//
	// ClassRetryable — reintenta con el backoff completo. Elígelo solo si vuestras
	// dependencias internas tienen hipos frecuentes; el coste es que un modo de
	// fallo nuevo atasca la cola 51 minutos y se amplifica con cada mensaje que falle
	// igual.
	//
	// El cero del campo ("") significa el default de la spec: ClassPermanent.
	UnknownErrorPolicy ErrorClass

	// TimeoutPolicy responde a: un timeout, ¿es "el mundo va lento" o "esta
	// operación no cabe en la ventana"?
	//
	// El default es ClassRetryable: un timeout suele indicar saturación transitoria.
	// Si vuestros timeouts son casi siempre consultas que nunca van a terminar,
	// ClassPermanent evita reintentar lo imposible.
	//
	// El cero del campo ("") significa el default de la spec: ClassRetryable.
	TimeoutPolicy ErrorClass

	// Rules son reglas propias, evaluadas antes que todo lo demás.
	Rules []Rule
}

// Classifier traduce un error cualquiera a una de las tres clases del protocolo.
type Classifier func(err error) Classification

// NewClassifier construye un clasificador. El runtime del consumidor usa el
// resultado así:
//
//	ClassRetryable → msg.NakWithDelay(RetryAfter ?: backoff canónico)
//	ClassPermanent → msg.Term() + publicar en dlq.<subject> con dlqattempts = 1
//	ClassPoison    → msg.Term() + publicar en dlq.<subject> + alerta inmediata
//
// El orden de evaluación es deliberado: lo más específico primero y el default al
// final. Esa última línea es la decisión de política de verdad.
func NewClassifier(opts ClassifierOptions) Classifier {
	unknownClass := ClassPermanent
	if opts.UnknownErrorPolicy == ClassRetryable {
		unknownClass = ClassRetryable
	}
	timeoutClass := ClassRetryable
	if opts.TimeoutPolicy == ClassPermanent {
		timeoutClass = ClassPermanent
	}
	rules := append([]Rule(nil), opts.Rules...) // copia: la política no cambia bajo los pies

	return func(err error) Classification {
		if err == nil {
			// No debería ocurrir —el runtime solo clasifica errores— pero devolver
			// una clase falsa sería peor que devolver algo inerte.
			return Classification{Class: ClassPermanent, Code: "NIL_ERROR"}
		}

		// 1. Un error tipado de flux siempre gana: la aplicación sabe más que el SDK.
		if ce, ok := AsClassified(err); ok {
			c := Classification{Class: ce.FluxClass(), Code: ce.FluxCode()}
			if c.Class == ClassRetryable {
				c.RetryAfter = retryAfterOf(err)
			}
			return c
		}

		// 2. Reglas de la aplicación.
		for _, rule := range rules {
			if c, ok := rule(err); ok {
				return c
			}
		}

		// 3. Status HTTP: la señal más fiable que da una dependencia.
		var httpErr HTTPStatusError
		if errors.As(err, &httpErr) {
			status := httpErr.HTTPStatus()
			_, retryable := RetryableHTTPStatus[status]
			c := Classification{Code: fmt.Sprintf("HTTP_%d", status), Class: ClassPermanent}
			if retryable {
				c.Class = ClassRetryable
				var ra RetryAfterError
				if errors.As(err, &ra) {
					c.RetryAfter = ra.RetryAfter()
				}
			}
			return c
		}

		// 4. Errores de sistema: red y DNS son transitorios por definición.
		if name := syscallName(err); name != "" {
			return Classification{Class: ClassRetryable, Code: name}
		}

		// 5. Timeouts — política configurable.
		if isTimeout(err) {
			return Classification{Class: timeoutClass, Code: "TIMEOUT"}
		}

		// 6. Lo desconocido. Aquí se decide el comportamiento del ecosistema ante lo
		//    que nadie previó. El default de la spec es PERMANENT porque un evento en
		//    la DLQ es recuperable y una cola atascada en hora punta no lo es
		//    — 04-errors.md §2.
		return Classification{Class: unknownClass, Code: "UNKNOWN"}
	}
}

// Classify es el clasificador con los defaults de la spec: desconocido → PERMANENT,
// timeout → RETRYABLE.
var Classify = NewClassifier(ClassifierOptions{})
