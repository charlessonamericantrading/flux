// Errores de la capacidad `state` y los de construcción del sobre.
// Contrato normativo: specification/04-errors.md §6
//
// Cada código es un tipo propio, con su clase y su código FIJOS (la aplicación no puede sustituirlos: es
// lo que agrupa el panel, 04-errors.md §5.2), y encaja en la interfaz ClassifiedError: el clasificador del
// consumidor los reconoce por errors.As, también envueltos con %w. Los mensajes dicen QUÉ está mal y CÓMO se
// arregla, y NO incluyen valores de `data` (acaban en logs y en `dlqerror`); una clave y un tenant sí
// pueden citarse: son identificadores opacos, no datos personales.

package flux

import (
	"fmt"
	"strings"
)

// Códigos estables de la capacidad `state` — 04-errors.md §6.
const (
	CodeInvalidTenantForState = "INVALID_TENANT_FOR_STATE"
	CodeInvalidStateContract  = "INVALID_STATE_CONTRACT"
	CodeInvalidStateKey       = "INVALID_STATE_KEY"
	CodeMissingAggregateVer   = "MISSING_AGGREGATE_VERSION"
	CodeStateSubjectMismatch  = "STATE_SUBJECT_MISMATCH"
	CodeStateConflict         = "STATE_CONFLICT"
	CodeReservedDomain        = "RESERVED_DOMAIN"
	CodeInvalidDataClassif    = "INVALID_DATACLASSIFICATION"
	CodeMissingRequiredAttr   = "MISSING_REQUIRED_ATTRIBUTE"
	CodeMissingRequiredExt    = "MISSING_REQUIRED_EXTENSION"
	CodeWrongAttributeType    = "WRONG_ATTRIBUTE_TYPE"
)

// citar muestra un valor de usuario en un mensaje, acotado.
func citar(v string, max int) string {
	s := jsonString(v)
	if len(s) > max {
		return s[:max-3] + "…\""
	}
	return s
}

// ─── Estado ──────────────────────────────────────────────────────────────────

// InvalidStateContractError: el contrato no es `<dominio>.<agregado>.v<major>`. PERMANENT.
type InvalidStateContractError struct {
	baseError
	Contract string
	detail   string
}

func (e *InvalidStateContractError) Error() string {
	return fmt.Sprintf("contrato de estado inválido %s: %s. Un contrato es `<dominio>.<agregado>.v<major>` "+
		"—el subject del estado sin `state.`, sin tenant y sin clave—, en minúsculas y kebab-case: "+
		"`clientes.cliente.v1` (10-state.md §2.2).", citar(e.Contract, 70), e.detail)
}

// InvalidTenantForStateError: el tenant no sirve para un estado. PERMANENT.
type InvalidTenantForStateError struct {
	baseError
	Tenant string
	// Missing es true si no se pudo resolver ningún tenant: en estado NO hay tenant por defecto.
	Missing bool
}

func (e *InvalidTenantForStateError) Error() string {
	if e.Missing {
		return "no hay tenant para esta operación de estado. En estado NO hay tenant por defecto (ni \"system\" " +
			"implícito): el tenant va en el subject, y uno equivocado no es una etiqueta mal puesta sino un dato " +
			"guardado en otro sitio. Pásalo con WithStateTenant o fija TenantID en ConnectOptions (10-state.md §2.4)."
	}
	return fmt.Sprintf("el tenant %s no es válido para estado: debe cumplir ^[a-z0-9]+(-[a-z0-9]+)*$ y tener como "+
		"máximo 64 caracteres. El tenant de un EVENTO puede ser cualquier cadena; el de un estado, no, porque va "+
		"dentro del subject de NATS (10-state.md §2.4).", citar(e.Tenant, 70))
}

// InvalidStateKeyError: la clave no cumple `^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`. PERMANENT.
type InvalidStateKeyError struct {
	baseError
	Key string
}

func (e *InvalidStateKeyError) Error() string {
	return fmt.Sprintf("la clave %s no es válida: debe cumplir ^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$ (un único token de "+
		"NATS: sin puntos, comodines ni espacios, de 1 a 128 caracteres y sin empezar por «_», que está reservado al "+
		"protocolo). El broker NO la valida —acepta `*`, `a.b` y 2000 caracteres— y una clave sin validar es un "+
		"subject fantasma que ningún observador recibe, sin error. Tampoco se escapa sola: dos escapes distintos "+
		"serían dos claves para el mismo agregado. Usa un identificador opaco, nunca un email (10-state.md §2.3).",
		citar(e.Key, 70))
}

// MissingAggregateVersionError: `data.aggregateVersion` no es un entero ≥ 1 y ≤ 2^53 − 1. PERMANENT.
type MissingAggregateVersionError struct {
	baseError
	received string
}

func (e *MissingAggregateVersionError) Error() string {
	return fmt.Sprintf("`data.aggregateVersion` debe ser un entero ≥ 1 y ≤ 9007199254740991 (2^53 − 1); %s. "+
		"Lo pone el DUEÑO del agregado —es su contador monotónico, el mismo que llevan sus eventos—: el SDK ni lo "+
		"inventa ni lo incrementa. Ausente, null, una cadena, un decimal, 0 o un negativo son todos este error "+
		"(10-state.md §3.4).", e.received)
}

// StateSubjectMismatchError: el `tenantid`, el `subject` o el `type` del sobre no coinciden con el subject
// de NATS en el que llegó la entrada. POISON: el mensaje miente sobre su identidad.
type StateSubjectMismatchError struct {
	baseError
	Attribute     string
	EnvelopeValue string
	SubjectValue  string
}

func (e *StateSubjectMismatchError) Error() string {
	return fmt.Sprintf("el atributo `%s` del sobre (%s) no coincide con el del subject de NATS en el que llegó la "+
		"entrada (%s). La identidad de una entrada está en el subject; el sobre la repite, y si discrepan uno de los "+
		"dos miente y quien consume no puede saber cuál. La entrada NO se aplica: la clave conserva su valor "+
		"anterior (10-state.md §5.4).", e.Attribute, citar(e.EnvelopeValue, 70), citar(e.SubjectValue, 70))
}

// StateConflictError: el chequeo de secuencia agotó sus intentos. RETRYABLE.
type StateConflictError struct {
	baseError
	Contract string
	Key      string
	Attempts int
}

func (e *StateConflictError) Error() string {
	return fmt.Sprintf("la clave %s de %q cambió entre la lectura y la publicación las %d veces (otra instancia del "+
		"dueño escribe la misma clave): se agotaron los %d intentos del put condicional. Es reintentable: un nuevo "+
		"intento relee y suele resolverlo, o termina en `stale` (10-state.md §3.2).",
		citar(e.Key, 70), e.Contract, e.Attempts, e.Attempts)
}

// ReservedDomainError: un subject, un contrato o una operación usa un dominio reservado. PERMANENT.
type ReservedDomainError struct {
	baseError
	Domain string
	// What es "subject", "contrato" u "operación".
	What string
}

func (e *ReservedDomainError) Error() string {
	articulo := "un"
	if e.What == "operación" {
		articulo = "una"
	}
	msg := fmt.Sprintf("el dominio %q está reservado al protocolo (reservados: %s) y no puede usarse en %s %s. Cada "+
		"uno es un espacio de nombres propio —`state.`, `dlq.`, `rpc.`—, y un evento publicado ahí no pasaría por el "+
		"chequeo de secuencia del estado (10-state.md §2.7). Elige otro dominio.",
		e.Domain, strings.Join(reservedDomains[:], ", "), articulo, e.What)
	if e.What == "operación" && e.Domain == "rpc" {
		msg += " Si lo que escribiste es el subject de NATS (`rpc.clientes.cliente.v1.obtener`), quita el prefijo " +
			"`rpc.`: lo pone el SDK, y el nombre de la operación es `clientes.cliente.v1.obtener` (11-request-reply.md §2.2)."
	}
	return msg
}

// InvalidDataClassificationError: la clasificación está fuera del enum, o es `restricted` en un estado.
type InvalidDataClassificationError struct {
	baseError
	Classification string
	// Scope es "estado" o "rpc".
	Scope string
}

func (e *InvalidDataClassificationError) Error() string {
	if e.Scope == "rpc" {
		return fmt.Sprintf("la clasificación %s no es válida: public, internal, confidential o restricted "+
			"(01-envelope.md §3.1). El error de una petición/respuesta es siempre `internal` y no se elige.",
			citar(e.Classification, 70))
	}
	return fmt.Sprintf("la clasificación %s no se admite en estado: solo public, internal o confidential. "+
		"`restricted` (credenciales, salud, financiero: 24 h de retención) no cabe en un almacén que no caduca, y no "+
		"es una decisión que deba tomarse por omisión (10-state.md §7).", citar(e.Classification, 70))
}

// RestrictedStateEntryError: una entrada de estado recibida lleva `restricted`. POISON.
type RestrictedStateEntryError struct{ baseError }

func (e *RestrictedStateEntryError) Error() string {
	return "una entrada de estado no puede llevar dataclassification \"restricted\": credenciales, salud o datos " +
		"financieros (24 h de retención) no caben en un almacén que no caduca. La entrada NO se aplica (10-state.md §7)."
}

// InvalidEnvelopeInputError: un atributo que el SDK rellena llegó vacío, o `time` no es un instante. PERMANENT,
// con el código que daría el parser ante el mismo defecto (el conjunto de estado, 04-errors.md §6, es cerrado).
type InvalidEnvelopeInputError struct {
	baseError
	detail string
}

func (e *InvalidEnvelopeInputError) Error() string { return e.detail }

func invalidInput(code, detail string) *InvalidEnvelopeInputError {
	return &InvalidEnvelopeInputError{baseError{code, ClassPermanent, nil}, detail}
}
