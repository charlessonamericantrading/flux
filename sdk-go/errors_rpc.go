// Errores de la capacidad `rpc`.
// Contrato normativo: specification/04-errors.md §7
//
// Como los de estado (errors_capabilities.go): un tipo por código, con su clase y su código FIJOS, y encajan en ClassifiedError.

package flux

import (
	"fmt"
	"time"
)

// Códigos estables de la capacidad `rpc` — 04-errors.md §7.
const (
	CodeRPCUnavailable     = "RPC_UNAVAILABLE"
	CodeRPCTimeout         = "RPC_TIMEOUT"
	CodeStaleRequest       = "STALE_REQUEST"
	CodeRPCInvalidReply    = "RPC_INVALID_REPLY"
	CodeRPCMessageTooLarge = "RPC_MESSAGE_TOO_LARGE"
	CodeInvalidRPCCall     = "INVALID_RPC_CALL"
)

// ─── Petición/respuesta ──────────────────────────────────────────────────────

// RPCUnavailableError: nadie escucha la operación (`no responders`, 503). RETRYABLE, inmediato.
type RPCUnavailableError struct {
	baseError
	Operation string
}

func (e *RPCUnavailableError) Error() string {
	return fmt.Sprintf("nadie escucha la operación %q: el broker contestó `no responders`. Ningún servicio la atiende "+
		"ahora mismo (¿está desplegado y arrancado el servicio dueño? ¿el nombre y la versión mayor son los de su "+
		"contrato? ¿hay un Respond() para esta operación EXACTA?). Es reintentable, y falla al instante: no ha "+
		"esperado al timeout (11-request-reply.md §4.3).", e.Operation)
}

// RPCTimeoutError: no llegó una respuesta en el timeout. RETRYABLE y AMBIGUO: la operación PUEDE haberse ejecutado.
type RPCTimeoutError struct {
	baseError
	Operation string
	Timeout   time.Duration
}

func (e *RPCTimeoutError) Error() string {
	return fmt.Sprintf("no llegó una respuesta a %q en %d ms. La operación PUEDE haberse ejecutado: el que responde "+
		"pudo ser más lento que el timeout, o morir con la petición en la mano. Un reintento debe ser seguro: usa "+
		"WithRetries (reutiliza el MISMO id) y que quien responde deduplique por el id de la petición "+
		"(11-request-reply.md §4.5). Si es un cálculo lento, sube el timeout (máximo 25000).",
		e.Operation, e.Timeout.Milliseconds())
}

// StaleRequestError: con la firma en `require`, el `time` de la petición se aleja más de 300 s del reloj de
// quien responde. PERMANENT.
type StaleRequestError struct {
	baseError
	Time          string
	WindowSeconds int
	Now           string
}

func (e *StaleRequestError) Error() string {
	return fmt.Sprintf("la petición lleva `time` %s y la hora local de quien responde es %s: se aleja más de %d s "+
		"(por delante o por detrás). Con la firma en modo `require` la ventana acota cuánto tiempo sirve una petición "+
		"capturada, porque la firma por sí sola no impide reenviarla. Sincroniza los relojes (NTP) de los dos "+
		"servicios; reintentar la misma petición da lo mismo (11-request-reply.md §7.4).",
		citar(e.Time, 90), e.Now, e.WindowSeconds)
}

// RPCInvalidReplyError: lo que llegó NO es la respuesta a esta petición. PERMANENT: es un incidente.
type RPCInvalidReplyError struct {
	baseError
	Operation string
	Reason    string
	// StreamAck es true si el cuerpo parece el acuse de un stream de JetStream.
	StreamAck bool
}

func (e *RPCInvalidReplyError) Error() string {
	pista := "Una respuesta que no es de esta petición es un incidente (un productor roto, un stream mal puesto o " +
		"algo hostil), no un fallo que esperar: reintentar no lo arregla (11-request-reply.md §4.3). "
	if e.StreamAck {
		pista = "El cuerpo parece el acuse de un stream de JetStream (`{\"stream\":…,\"seq\":…}`): algún stream " +
			"captura `rpc.>` y contesta a cada petición con su acuse —y guarda cada petición—. Ejecuta `flux doctor` " +
			"(11-request-reply.md §8). "
	}
	return fmt.Sprintf("lo que llegó como respuesta a %q no es la respuesta a esta petición: %s. %sSe cuenta como "+
		"`invalid_reply`: es la alerta que debe despertar a alguien.", e.Operation, e.Reason, pista)
}

// RPCMessageTooLargeError: el mensaje serializado no cabe. PERMANENT.
type RPCMessageTooLargeError struct {
	baseError
	Bytes     int
	Limit     int
	Operation string
	// Side es "petición" o "respuesta".
	Side string
}

func (e *RPCMessageTooLargeError) Error() string {
	return fmt.Sprintf("la %s de %q serializa a %d bytes y el límite es %d (el mínimo entre 1 MiB y el max_payload "+
		"que anuncia el broker; las cabeceras cuentan). Aplica claim-check: guarda el contenido en un almacén de "+
		"objetos y envía `{ uri, sha256, bytes }`, o pagina con un cursor en `data` (01-envelope.md §7, "+
		"11-request-reply.md §4.4).", e.Side, e.Operation, e.Bytes, e.Limit)
}

// InvalidRPCCallError: la llamada o el registro está mal formado y se sabe SIN red. PERMANENT.
type InvalidRPCCallError struct {
	baseError
	Reason string
}

func (e *InvalidRPCCallError) Error() string {
	return "llamada de petición/respuesta inválida: " + e.Reason
}

func invalidRPCCall(reason string, cause error) *InvalidRPCCallError {
	return &InvalidRPCCallError{baseError{CodeInvalidRPCCall, ClassPermanent, cause}, reason}
}

// RPCRemoteError es el error que CONTESTÓ quien responde (`com.flux.rpc.error.v1`): lo que el handler remoto
// lanzó, o lo que fabricó su SDK. Su clase es binaria —`retryable` o `permanent`: `poison` no viaja— y su
// código el que eligió el otro lado (11-request-reply.md §3.3).
type RPCRemoteError struct {
	baseError
	Operation  string
	Message    string
	retryAfter time.Duration
}

func (e *RPCRemoteError) Error() string { return e.Message }

// RetryAfter implementa RetryAfterError: la sugerencia de quien respondió para el PRIMER reintento, o cero.
func (e *RPCRemoteError) RetryAfter() time.Duration { return e.retryAfter }

func newRPCRemoteError(operation, class, code, message string, retryAfter time.Duration) *RPCRemoteError {
	c := ClassPermanent
	if class == "retryable" {
		c = ClassRetryable
	}
	return &RPCRemoteError{baseError{code, c, nil}, operation, message, retryAfter}
}
