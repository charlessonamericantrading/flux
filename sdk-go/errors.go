// Taxonomía de errores de flux.
// Contrato normativo: specification/04-errors.md
//
// El error más caro de un sistema de eventos no es perder un mensaje: es reintentar
// durante 51 minutos algo que nunca va a funcionar mientras los eventos sanos se
// acumulan detrás. Por eso flux no tiene "una política de reintentos": tiene una
// taxonomía de tres clases, y cada clase determina una acción distinta sobre el
// mensaje de NATS.

package flux

import (
	"errors"
	"time"
)

// ErrorClass es una de las tres clases del protocolo — 04-errors.md §1.
type ErrorClass string

const (
	// ClassRetryable: el fallo es del entorno y podría desaparecer solo.
	// → nak(delay) y reintento con el backoff canónico.
	ClassRetryable ErrorClass = "retryable"

	// ClassPermanent: el evento es válido pero este consumidor nunca podrá
	// procesarlo por mucho que espere. → term() + DLQ inmediato, sin reintentos.
	ClassPermanent ErrorClass = "permanent"

	// ClassPoison: el mensaje ni siquiera es interpretable. → term() + DLQ + alerta.
	// Lo detecta el SDK antes del handler; casi siempre significa que un productor
	// está roto. Es el único caso que DEBE despertar a alguien.
	ClassPoison ErrorClass = "poison"
)

// Classification es el resultado de clasificar un error: lo que el runtime del
// consumidor consume para decidir entre nak, term y alerta.
type Classification struct {
	// Class determina la acción sobre el mensaje.
	Class ErrorClass

	// Code es un código estable para métricas y alertas ("HTTP_503",
	// "PEDIDO_YA_CANCELADO"). El mensaje del error cambia; el código no debería.
	Code string

	// RetryAfter solo aplica a ClassRetryable y sobrescribe el backoff canónico
	// para ese intento. Úsalo cuando la dependencia dice explícitamente cuánto
	// esperar (cabecera Retry-After). Cero significa "usa el backoff canónico".
	RetryAfter time.Duration

	// MaxAttempts solo aplica a ClassRetryable: entregas máximas para ESTE error,
	// por debajo del max_deliver del consumidor. Cero significa "sin tope propio",
	// es decir, manda el del consumidor.
	//
	// Existe porque max_deliver es por consumidor, no por mensaje: bajarlo a 2 para
	// acotar los errores desconocidos recortaría también los reintentos de los que sí
	// sabemos transitorios (ECONNRESET, HTTP 503), que deben conservar sus 6 intentos
	// — 04-errors.md §2.1.
	//
	// Es int y no *int a propósito: cero no es un presupuesto válido —un mensaje se
	// entrega al menos una vez, así que el mínimo con sentido es 1— y el campo de al
	// lado, RetryAfter, ya usa el mismo convenio de "cero = sin especificar". Un
	// puntero añadiría una asignación y un alias mutable a un struct que se copia por
	// valor en cada despacho, a cambio de distinguir un caso que no existe.
	MaxAttempts int
}

// ClassifiedError es un error que ya declara su propia clase de flux. La aplicación
// lo implementa —normalmente vía los tipos de abajo— para anular al clasificador,
// porque solo ella conoce sus dependencias — 04-errors.md §2.
type ClassifiedError interface {
	error
	// FluxClass devuelve la clase declarada por el error.
	FluxClass() ErrorClass
	// FluxCode devuelve el código estable para métricas.
	FluxCode() string
}

// ─── Opciones de construcción ────────────────────────────────────────────────

// errorOptions recoge los campos opcionales comunes a los tres tipos.
type errorOptions struct {
	code       string
	retryAfter time.Duration
	cause      error
}

// ErrorOption configura un error de flux al construirlo.
//
// Go no tiene objetos de opciones anónimos como el `{ code, retryAfterMs }` del SDK
// de Node, así que se usa el patrón funcional. Semántica idéntica.
type ErrorOption func(*errorOptions)

// WithCode fija el código estable para métricas y alertas.
func WithCode(code string) ErrorOption {
	return func(o *errorOptions) { o.code = code }
}

// WithRetryAfter fija el retraso explícito del siguiente intento. Solo tiene efecto
// sobre un RetryableError; en las otras clases se ignora, igual que en Node.
func WithRetryAfter(d time.Duration) ErrorOption {
	return func(o *errorOptions) { o.retryAfter = d }
}

// WithCause encadena el error subyacente, recuperable con errors.Unwrap/errors.Is.
func WithCause(err error) ErrorOption {
	return func(o *errorOptions) { o.cause = err }
}

func applyOptions(opts []ErrorOption) errorOptions {
	var o errorOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// ─── RETRYABLE ───────────────────────────────────────────────────────────────

// RetryableError lo lanza la aplicación para forzar un reintento. Úsalo cuando SABES
// que el fallo es transitorio: timeout de red, 503 de un proveedor, deadlock de BD.
type RetryableError struct {
	Message string
	Code    string
	// RetryAfter sobrescribe el backoff canónico para este intento.
	RetryAfter time.Duration
	Cause      error
}

// NewRetryableError construye un RetryableError.
//
//	return flux.NewRetryableError("proveedor 503", flux.WithRetryAfter(5*time.Second))
func NewRetryableError(message string, opts ...ErrorOption) *RetryableError {
	o := applyOptions(opts)
	return &RetryableError{Message: message, Code: o.code, RetryAfter: o.retryAfter, Cause: o.cause}
}

func (e *RetryableError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

// Unwrap expone la causa a errors.Is y errors.As.
func (e *RetryableError) Unwrap() error { return e.Cause }

// FluxClass implementa ClassifiedError.
func (e *RetryableError) FluxClass() ErrorClass { return ClassRetryable }

// FluxCode implementa ClassifiedError. Sin código explícito se cae al nombre del
// tipo, para que las métricas nunca queden sin etiqueta.
func (e *RetryableError) FluxCode() string {
	if e.Code != "" {
		return e.Code
	}
	return "RetryableError"
}

// ─── PERMANENT ───────────────────────────────────────────────────────────────

// PermanentError lo lanza la aplicación para ir directo a la DLQ sin gastar
// reintentos. Úsalo cuando el evento es válido pero tu lógica lo rechaza de forma
// definitiva: reintentarlo son 51 minutos de cola bloqueada para llegar al mismo sitio.
type PermanentError struct {
	Message string
	Code    string
	Cause   error
}

// NewPermanentError construye un PermanentError.
//
//	return flux.NewPermanentError("pedido ya cancelado", flux.WithCode("PEDIDO_YA_CANCELADO"))
func NewPermanentError(message string, opts ...ErrorOption) *PermanentError {
	o := applyOptions(opts)
	return &PermanentError{Message: message, Code: o.code, Cause: o.cause}
}

func (e *PermanentError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

// Unwrap expone la causa a errors.Is y errors.As.
func (e *PermanentError) Unwrap() error { return e.Cause }

// FluxClass implementa ClassifiedError.
func (e *PermanentError) FluxClass() ErrorClass { return ClassPermanent }

// FluxCode implementa ClassifiedError.
func (e *PermanentError) FluxCode() string {
	if e.Code != "" {
		return e.Code
	}
	return "PermanentError"
}

// ─── POISON ──────────────────────────────────────────────────────────────────

// PoisonError lo produce el SDK, no la aplicación: el mensaje no pudo interpretarse
// como CloudEvent, así que el handler nunca llega a verlo — 04-errors.md §1.3.
type PoisonError struct {
	Message string
	Code    string
	Cause   error
}

// NewPoisonError construye un PoisonError. Normalmente solo lo llama el propio SDK.
func NewPoisonError(message string, opts ...ErrorOption) *PoisonError {
	o := applyOptions(opts)
	return &PoisonError{Message: message, Code: o.code, Cause: o.cause}
}

func (e *PoisonError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

// Unwrap expone la causa a errors.Is y errors.As.
func (e *PoisonError) Unwrap() error { return e.Cause }

// FluxClass implementa ClassifiedError.
func (e *PoisonError) FluxClass() ErrorClass { return ClassPoison }

// FluxCode implementa ClassifiedError.
func (e *PoisonError) FluxCode() string {
	if e.Code != "" {
		return e.Code
	}
	return "PoisonError"
}

// ─── Inspección ──────────────────────────────────────────────────────────────

// AsClassified recorre la cadena de errores buscando uno que declare su clase.
//
// Usa errors.As, así que funciona con errores envueltos con %w — a diferencia del
// `instanceof` de Node, que solo mira el objeto de arriba. Es una mejora del port:
// un RetryableError envuelto por una capa intermedia sigue clasificándose bien.
func AsClassified(err error) (ClassifiedError, bool) {
	var ce ClassifiedError
	if errors.As(err, &ce) {
		return ce, true
	}
	return nil, false
}

// IsClassified informa si el error (o alguno de los que envuelve) declara su clase.
func IsClassified(err error) bool {
	_, ok := AsClassified(err)
	return ok
}

// retryAfterOf extrae el RetryAfter cuando el error es concretamente RETRYABLE.
// Las otras dos clases no tienen reintento del que hablar.
func retryAfterOf(err error) time.Duration {
	var re *RetryableError
	if errors.As(err, &re) {
		return re.RetryAfter
	}
	return 0
}
