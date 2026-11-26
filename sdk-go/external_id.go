// El `id` DETERMINISTA de un hecho importado de un sistema externo.
// Contrato normativo: specification/01-envelope.md §2.6.1
//
// Un proveedor (una pasarela de pagos, un sistema de terceros) reenvía el mismo aviso durante horas. Si cada reenvío recibe un `id`
// nuevo, ningún consumidor puede deduplicarlo: el `id` es la clave de deduplicación del ecosistema. Para un hecho importado, el `id`
// se DERIVA de lo que identifica al hecho, y es siempre el mismo:
//
//	id = UUIDv7 cuyos 48 bits de tiempo son los milisegundos del `time` del hecho externo y cuyos 74 bits restantes
//	     (`rand_a` y `rand_b`) son los 74 primeros bits de SHA-256( <source> "|" <clave externa> ).
//
// Es un UUIDv7 VÁLIDO (versión 7, variante 10): cualquier consumidor que valide el formato del `id` lo acepta, y ordenar por `id`
// dentro de un `source` sigue siendo ordenar por el instante del hecho (§2.6). Es una función PURA y no usa el reloj ni ninguna
// fuente de azar: es lo que el arnés cruzado compara byte a byte entre SDKs.

package flux

import (
	"crypto/sha256"
	"fmt"
	"math/big"
	"strings"
	"time"
)

const (
	// UUIDv7MaxMs es 2^48 − 1: lo que cabe en el campo de tiempo de un UUIDv7.
	UUIDv7MaxMs = 1<<48 - 1
	// ExternalKeyMaxBytes es la longitud máxima de una clave externa, en bytes UTF-8: es un identificador, no un documento.
	ExternalKeyMaxBytes = 512
)

// ExternalEventID es `SHA-256(<source> "|" <externalKey>)`, y de ahí el UUIDv7. Falla con *EnvelopeError ante una clave vacía o
// demasiado larga, un `source` con `|` o un instante que no cabe en 48 bits. `at` es el instante del hecho EXTERNO.
func ExternalEventID(source, externalKey string, at time.Time) (string, error) {
	if source == "" || strings.Contains(source, "|") {
		return "", &EnvelopeError{Message: "`source` debe ser una cadena no vacía y sin `|` (es el separador de la derivación del id determinista)"}
	}
	if externalKey == "" {
		return "", &EnvelopeError{Message: "`externalKey` debe ser una cadena no vacía: es la clave del hecho en el sistema externo " +
			"(el id del evento del proveedor). Sin ella no hay nada de lo que derivar un `id` estable (01-envelope.md §2.6.1)"}
	}
	if len(externalKey) > ExternalKeyMaxBytes {
		return "", &EnvelopeError{Message: fmt.Sprintf("`externalKey` ocupa más de %d bytes UTF-8: es un identificador, no un documento. "+
			"Usa el id del hecho en el sistema externo", ExternalKeyMaxBytes)}
	}
	ms := at.UnixMilli()
	if at.IsZero() || ms < 0 || ms > UUIDv7MaxMs {
		return "", &EnvelopeError{Message: fmt.Sprintf("`time` no es un instante válido para el id determinista (se necesitan milisegundos "+
			"enteros entre 0 y %d). Pasa el instante del hecho externo (01-envelope.md §2.6.1)", int64(UUIDv7MaxMs))}
	}

	digest := sha256.Sum256([]byte(source + "|" + externalKey))
	// Los 74 primeros bits del digest: 80 bits (10 bytes) menos los 6 últimos.
	r := new(big.Int).SetBytes(digest[:10])
	r.Rsh(r, 6)
	randA := new(big.Int).Rsh(r, 62) // bits 0..11
	randB := new(big.Int).And(r, new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 62), big.NewInt(1)))
	u := new(big.Int).Lsh(big.NewInt(ms), 80)
	u.Or(u, new(big.Int).Lsh(big.NewInt(0x7), 76))
	u.Or(u, new(big.Int).Lsh(randA, 64))
	u.Or(u, new(big.Int).Lsh(big.NewInt(0b10), 62))
	u.Or(u, randB)
	hex := fmt.Sprintf("%032x", u)
	return hex[:8] + "-" + hex[8:12] + "-" + hex[12:16] + "-" + hex[16:20] + "-" + hex[20:], nil
}
