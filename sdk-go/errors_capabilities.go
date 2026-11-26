// El error de un dominio reservado (RESERVED_DOMAIN). Los de las capacidades `state` y `rpc` llegan con ellas.
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

// CodeReservedDomain es el código de un subject, contrato u operación que usa un dominio reservado. Es de la capacidad `state`
// ([04-errors.md §6](../specification/04-errors.md)) pero lo emiten TODAS las llamadas: `Publish` y `Subscribe` también rechazan
// `dlq`, `state`, `rpc` y `system` (10-state.md §2.7).
const CodeReservedDomain = "RESERVED_DOMAIN"

// citar muestra un valor de usuario en un mensaje, acotado.
func citar(v string, max int) string {
	s := jsonString(v)
	if len(s) > max {
		return s[:max-3] + "…\""
	}
	return s
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
