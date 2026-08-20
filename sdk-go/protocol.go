// Package flux implementa el cliente de Go del flux Event Protocol v1
// (CloudEvents 1.0 sobre NATS JetStream). Nivel de conformidad objetivo: L2.
//
// El nombre del paquete es "flux" aunque el último elemento de la ruta de import
// sea "sdk-go": Go no admite guiones en identificadores de paquete y el repositorio
// nombra los SDKs por lenguaje. Impórtalo así:
//
//	import flux "github.com/charlessonamericantrading/flux/sdk-go"
//
// Especificación normativa: specification/ y llms-full.txt del repositorio.
package flux

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ─── Constantes del protocolo ────────────────────────────────────────────────
//
// Todo lo de aquí sale de protocol.json. Si divergen, protocol.json manda: es lo
// que consumen los demás SDKs y los agentes de IA.

const (
	// ProtocolName y ProtocolVersion identifican el contrato, no este SDK.
	ProtocolName    = "flux"
	ProtocolVersion = "1.0.0"

	// SpecVersion es el literal exigido por CloudEvents — 01-envelope.md §2.
	SpecVersion = "1.0"

	// DataContentType: flux v1 solo admite JSON — 01-envelope.md §2.
	DataContentType = "application/json"

	// ConformanceLevel declarado por este SDK — 00-protocol.md §5.
	ConformanceLevel = "L2"

	// MaxMessageBytes es el techo del mensaje serializado. Por encima, claim-check:
	// se publica {uri, sha256, bytes}, no el contenido — 01-envelope.md §6.
	MaxMessageBytes = 1_048_576
)

// Configuración canónica de consumidor — 03-delivery.md §2.
const (
	// DefaultAckWait DEBE coincidir con CanonicalBackOff()[0].
	//
	// ⚠️ JetStream SOBRESCRIBE ack_wait con backoff[0] y no devuelve error: pides
	// 30 s con un backoff que empieza en 1 s y obtienes un ack_wait efectivo de 1 s.
	// Cualquier handler que toque una base de datos se ejecuta entonces en
	// concurrencia consigo mismo, en cada mensaje, sin ninguna señal visible.
	// Verificado contra nats-server 2.14.5 — ver 03-delivery.md §2.1 y
	// conformance/cases/consumer-config.json.
	//
	// Consecuencia de diseño: backoff[0] ES el presupuesto de duración del handler.
	// Por eso el backoff canónico empieza en 30 s y no en 1 s; un primer reintento
	// rápido es imposible por construcción y buscarlo es lo que rompe la config.
	DefaultAckWait = 30 * time.Second

	// DefaultMaxDeliver: 1 entrega inicial + 5 reintentos, uno por entrada de
	// CanonicalBackOff(). Si fuese 5, la última entrada (30 m) no se aplicaría nunca
	// y la configuración mentiría sobre su propio comportamiento.
	DefaultMaxDeliver = 6

	// DefaultMaxAckPending. Ojo: un mensaje esperando reintento ocupa una ranura,
	// así que con backoffs largos y mucho fallo simultáneo esta ventana se llena
	// — 03-delivery.md §2.1, nota final.
	DefaultMaxAckPending = 256
)

// Sondeo de flux_consumer_pending — 08-observability.md §2.3.
const (
	// DefaultPendingPoll es cada cuánto se le pregunta al servidor por `num_pending`.
	// Es el observability.metrics.flux_consumer_pending.defaultPollMs de protocol.json.
	//
	// El sondeo NO es redundante con los metadatos del mensaje entregado: si el bucle
	// del consumidor muere, dejan de entregarse mensajes, así que un gauge alimentado
	// solo desde los metadatos se queda plano en su último valor en vez de crecer. Un
	// panel mostraría una línea horizontal, indistinguible de "no pasa nada" — que es
	// justo el fallo que esta métrica existe para detectar.
	DefaultPendingPoll = 15 * time.Second

	// PendingPollDisabled apaga el sondeo. Ver ConnectOptions.PendingPollInterval sobre
	// por qué no basta con un cero.
	PendingPollDisabled = -1 * time.Nanosecond
)

// Configuración canónica de stream — 02-naming.md §3.2.
const (
	StreamMaxAge = 30 * 24 * time.Hour
	// DLQStreamMaxAge es más larga a propósito: la DLQ es material forense. Pero es
	// un límite real, no un archivo — a los 90 días el evento desaparece.
	DLQStreamMaxAge = 90 * 24 * time.Hour

	// DuplicateWindow deduplica PUBLICACIONES con el mismo Nats-Msg-Id. NO deduplica
	// reentregas de consumo, y nunca sustituye a la idempotencia del consumidor: es
	// el malentendido más frecuente del protocolo — 03-delivery.md §3.
	DuplicateWindow = 2 * time.Minute
)

// CanonicalBackOff devuelve el backoff canónico [30s, 1m, 5m, 15m, 30m].
//
// Devuelve una copia nueva en cada llamada a propósito: un slice a nivel de paquete
// sería mutable desde fuera, y una entrada [0] alterada cambiaría en silencio el
// ack_wait efectivo de todo consumidor creado después.
//
// Tiempo total hasta la DLQ ≈ 51 min 30 s. Es una decisión de producto: cuánto
// tiempo aceptas que un fallo transitorio siga reintentando antes de que un humano
// se entere. Solo lo recorren los RETRYABLE; un PERMANENT no gasta ni un reintento.
func CanonicalBackOff() []time.Duration {
	return []time.Duration{
		30 * time.Second,
		1 * time.Minute,
		5 * time.Minute,
		15 * time.Minute,
		30 * time.Minute,
	}
}

// TotalTimeToDLQ es la suma del backoff canónico: lo que tarda un RETRYABLE en
// agotar los reintentos y caer en la DLQ.
func TotalTimeToDLQ() time.Duration {
	var total time.Duration
	for _, d := range CanonicalBackOff() {
		total += d
	}
	return total
}

// ─── Naming ──────────────────────────────────────────────────────────────────

// SubjectPattern valida `<dominio>.<agregado>.v<major>.<evento>` — 02-naming.md §1.
var SubjectPattern = regexp.MustCompile(
	`^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]*\.[a-z0-9]+(-[a-z0-9]+)*$`,
)

// ParsedSubject son los cuatro tokens de un subject ya validado.
type ParsedSubject struct {
	Domain    string
	Aggregate string
	Major     int
	Event     string
}

// Subject reconstruye el subject original. La transformación es biyectiva.
func (p ParsedSubject) Subject() string {
	return fmt.Sprintf("%s.%s.v%d.%s", p.Domain, p.Aggregate, p.Major, p.Event)
}

// InvalidSubjectError señala un subject que no cumple 02-naming.md §1.
type InvalidSubjectError struct {
	Subject string
	Reason  string
}

func (e *InvalidSubjectError) Error() string {
	return fmt.Sprintf("subject inválido %q: %s", e.Subject, e.Reason)
}

// ParseSubject valida y descompone un subject de NATS.
//
// ⚠️ No confundir con el atributo `subject` de CloudEvents, que es el ID DEL
// AGREGADO ("ped-123"). En este SDK ese atributo se llama AggregateID y solo se
// mapea a `subject` al serializar — 01-envelope.md §2.1.
func ParseSubject(subject string) (ParsedSubject, error) {
	// La comprobación de minúsculas va ANTES que el patrón para poder dar un mensaje
	// útil: los subjects de NATS son case-sensitive, así que "Pedidos.pedido.v1.creado"
	// crea un subject fantasma al que nadie está suscrito y no produce ningún error.
	// Sin este mensaje, el desarrollador solo ve "no llegan mis eventos".
	if subject != strings.ToLower(subject) {
		return ParsedSubject{}, &InvalidSubjectError{
			Subject: subject,
			Reason: "debe ir todo en minúsculas — NATS es case-sensitive y una mayúscula " +
				"crea un subject al que nadie está suscrito, sin producir error",
		}
	}

	if !SubjectPattern.MatchString(subject) {
		n := len(strings.Split(subject, "."))
		reason := "formato esperado <dominio>.<agregado>.v<major>.<evento> en kebab-case"
		if n != 4 {
			reason = fmt.Sprintf(
				"debe tener exactamente 4 tokens (<dominio>.<agregado>.v<major>.<evento>), tiene %d", n)
		}
		return ParsedSubject{}, &InvalidSubjectError{Subject: subject, Reason: reason}
	}

	tokens := strings.Split(subject, ".")
	// El patrón ya garantizó `v` + entero ≥ 1, así que el Atoi no puede fallar.
	major, _ := strconv.Atoi(tokens[2][1:])
	return ParsedSubject{
		Domain:    tokens[0],
		Aggregate: tokens[1],
		Major:     major,
		Event:     tokens[3],
	}, nil
}

// IsValidSubject informa si el subject cumple el contrato, sin exponer el motivo.
func IsValidSubject(subject string) bool {
	_, err := ParseSubject(subject)
	return err == nil
}

// SubjectToType deriva el `type` de CloudEvents:
// `pedidos.pedido.v1.creado` → `com.flux.pedidos.pedido.creado.v1` — 02-naming.md §2.
//
// Los dos formatos existen porque sirven a consumidores distintos: el subject enruta
// y necesita la versión en posición fija para que los wildcards funcionen; el `type`
// identifica el contrato en un catálogo y ahí lee mejor con la versión al final.
// La transformación es mecánica, así que jamás se le pide al desarrollador.
func SubjectToType(subject string) (string, error) {
	p, err := ParseSubject(subject)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("com.flux.%s.%s.%s.v%d", p.Domain, p.Aggregate, p.Event, p.Major), nil
}

// StreamName devuelve `EVT_PEDIDOS` para el dominio `pedidos`.
//
// NATS no admite `.`, `*`, `>`, `/`, `\` ni espacios en nombres de stream: de ahí el
// guion bajo. Las mayúsculas son convención, para distinguir de un vistazo un stream
// de un subject en los logs — 02-naming.md §3.
func StreamName(domain string) string {
	return "EVT_" + strings.ToUpper(strings.ReplaceAll(domain, "-", "_"))
}

// DLQStreamName devuelve `DLQ_PEDIDOS` — 02-naming.md §3.
func DLQStreamName(domain string) string {
	return "DLQ_" + strings.ToUpper(strings.ReplaceAll(domain, "-", "_"))
}

// DurableName devuelve `facturacion-api__pedidos_pedido_v1_creado`.
//
// NATS tampoco admite `.` en nombres de durable consumer. Separar el servicio con
// `__` y los tokens con `_` mantiene la reversibilidad: partiendo por `__` recuperas
// servicio y subject exactos. Un nombre de consumidor que no dice qué servicio lo
// tiene abierto es inútil en `nats consumer ls` a las 3 de la mañana — 02-naming.md §4.
func DurableName(service, subject string) (string, error) {
	if _, err := ParseSubject(subject); err != nil {
		return "", err
	}
	flat := strings.NewReplacer(".", "_", "-", "_").Replace(subject)
	return service + "__" + flat, nil
}

// DLQSubject antepone `dlq.` al subject original.
//
// PREFIJO, nunca sufijo. Un sufijo (`pedidos.pedido.v1.creado.dlq`) encajaría con
// `pedidos.>` y el stream EVT_PEDIDOS capturaría sus propios muertos: contarían
// contra su retención, un consumidor de `pedidos.pedido.v1.>` los recibiría, y un
// replay masivo podría reinyectarse en su propia DLQ — 02-naming.md §3.1.
func DLQSubject(subject string) string {
	return "dlq." + subject
}

// IsDLQSubject informa si el subject pertenece al espacio de nombres de la DLQ.
func IsDLQSubject(subject string) bool {
	return strings.HasPrefix(subject, "dlq.")
}

// SourceURI devuelve `/produccion/pedidos-api` — 01-envelope.md §2.
//
// `id` + `source` son la clave de deduplicación del ecosistema entero, así que el
// `source` tiene que identificar de forma estable entorno y servicio.
func SourceURI(environment, service string) string {
	return "/" + environment + "/" + service
}
