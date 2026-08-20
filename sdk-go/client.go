// Cliente de flux. Nivel de conformidad: L3 (validación de esquema opt-in, `off` por
// defecto; sin activarla el comportamiento es exactamente el de L2).
// Contrato normativo: specification/00-protocol.md §5
//
// Regla de diseño: este fichero NO expone ningún tipo de NATS en su API pública. Si
// lo hiciera, sustituir el broker dejaría de ser un cambio de capa 0-1 y pasaría a
// tocar las aplicaciones — que es exactamente lo que flux existe para evitar
// (00-protocol.md §3). Los tipos de jetstream viven detrás de campos privados.

package flux

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ─── Opciones ────────────────────────────────────────────────────────────────

// Credentials son las credenciales de NATS. NUNCA versionadas — 06-security.md §2.
type Credentials struct {
	// CredsFile es la ruta a un fichero .creds. Es la forma recomendada.
	CredsFile string
	Token     string
	User      string
	Password  string
}

// ConnectOptions configura la conexión al bus.
type ConnectOptions struct {
	// Servers es "nats://host:4222", o varios separados por coma para HA.
	Servers string

	// Service es el nombre del servicio. Alimenta `source` y los nombres de durable.
	Service string
	// Environment es "produccion", "staging", "dev". Alimenta `source`.
	Environment string
	// Version es el SemVer del servicio. Va en `producerversion`.
	Version string

	// TenantID por defecto de los eventos publicados, y filtro por defecto al consumir.
	// Vacío significa "system".
	TenantID string

	// TenantIsolation gobierna qué pasa al suscribirse sin filtro de tenant
	// — 09-multitenancy.md §3.
	//
	// Con TenantIsolationStrict, Subscribe devuelve *TenantIsolationError si no hay
	// tenant configurado. Un filtro que hay que acordarse de poner es un filtro que
	// alguien olvidará, y el fallo —ver los datos de otro tenant— no produce ningún
	// error: produce un incidente de privacidad que se descubre semanas después.
	//
	// El cero del tipo es TenantIsolationOff, que es el default del protocolo.
	TenantIsolation TenantIsolation

	// Classification por defecto. Vacío significa "internal" — 06-security.md §5.
	Classification DataClassification

	// Signing configura la firma Ed25519. Extensión OPCIONAL — 07-signing.md.
	//
	// Traslada la autenticidad DEL CANAL AL EVENTO: un evento firmado sigue siendo
	// verificable dentro de un fichero, un backup o un correo, donde ya no hay ACL. El
	// cero del struct no firma ni verifica, que es el default del protocolo.
	Signing SigningOptions

	// Metrics es el destino de las métricas. Nil significa NoMetrics{}.
	//
	// Los nombres y las etiquetas los fija el protocolo (08-observability.md), no la
	// aplicación: si cada SDK nombrara a su manera, un panel del ecosistema sería
	// imposible.
	Metrics MetricsSink

	// Schemas es el mapa exacto subject → URI de `dataschema`. Gana sobre SchemaBaseURL.
	Schemas map[string]string
	// SchemaBaseURL es la base para derivar `dataschema` cuando no está en Schemas.
	SchemaBaseURL string

	// Validation activa la validación L3 del payload contra su JSON Schema.
	//
	// Con Mode = ValidationStrict, publicar un payload que viola su contrato falla en el
	// productor en vez de aparecer como un misterio en un consumidor de otro equipo la
	// semana que viene — 00-protocol.md §5. El cero del struct es "off", que es L2.
	Validation ValidationOptions

	// PendingPollInterval es cada cuánto se sondea `num_pending` de cada consumidor.
	//
	// No es un capricho: flux_consumer_pending es la ÚNICA señal que delata a un
	// consumidor cuyo bucle murió, porque la conexión sigue reportándose sana y el
	// healthcheck dice que todo va bien (08-observability.md §4). Pero el dato solo se
	// obtiene preguntándole al servidor, así que hace falta un sondeo.
	//
	// El CERO significa el default (DefaultPendingPoll, 15 s). Para desactivarlo hay que
	// escribir un valor negativo — PendingPollDisabled.
	//
	// ⚠️ Divergencia deliberada con Node y Python, donde `0` desactiva: en Go el cero de
	// un campo es "no lo he puesto" y no se distingue de "ponlo a cero". Si el cero
	// desactivara, la métrica quedaría apagada en todo servicio que no conozca este
	// campo — es decir, justo en los que nadie está vigilando. Es la misma clase de
	// ambigüedad que documenta DLQAttempts en envelope.go.
	PendingPollInterval time.Duration

	// Classifier es la política de clasificación de errores. Ver classify.go.
	Classifier ClassifierOptions

	Credentials Credentials

	// Traceparent extrae el traceparent W3C del contexto. Ver TraceparentFunc.
	Traceparent TraceparentFunc

	// OnPoison se invoca ante un POISON. Es el único caso que debe despertar a
	// alguien: casi siempre significa que un productor está roto — 04-errors.md §1.3.
	OnPoison func(info PoisonInfo)

	// OnDLQ se invoca al enrutar cualquier evento a la DLQ.
	OnDLQ func(info DLQEventInfo)

	// Logger opcional. Nil significa silencio.
	Logger *slog.Logger
}

// PoisonInfo describe un mensaje que no se pudo interpretar.
type PoisonInfo struct {
	Subject string
	Err     error
	// Raw es el cuerpo tal cual llegó. Es lo único que queda para el forense.
	Raw []byte
}

// DLQEventInfo describe un evento enrutado a la DLQ.
type DLQEventInfo struct {
	Subject        string
	Event          Event
	Classification Classification
}

// ─── Opciones de publicación ─────────────────────────────────────────────────

type publishConfig struct {
	aggregateID    string
	tenantID       string
	classification DataClassification
	when           time.Time
	partitionKey   string
}

// PublishOption ajusta una publicación concreta.
type PublishOption func(*publishConfig)

// WithAggregateID fija el ID del agregado. Va al atributo `subject` de CloudEvents,
// NO al subject de NATS — 01-envelope.md §2.1.
func WithAggregateID(id string) PublishOption {
	return func(c *publishConfig) { c.aggregateID = id }
}

// WithTenantID sobrescribe el tenant para esta publicación.
func WithTenantID(id string) PublishOption {
	return func(c *publishConfig) { c.tenantID = id }
}

// WithClassification sobrescribe la clasificación para esta publicación.
func WithClassification(c DataClassification) PublishOption {
	return func(cfg *publishConfig) { cfg.classification = c }
}

// WithEventTime fija el instante en que OCURRIÓ el hecho, si no es "ahora".
// El `time` de CloudEvents no es el de publicación — 01-envelope.md §2.
func WithEventTime(t time.Time) PublishOption {
	return func(c *publishConfig) { c.when = t }
}

// WithPartitionKey sobrescribe la clave de partición, que por defecto es el
// AggregateID — 03-delivery.md §5.
func WithPartitionKey(k string) PublishOption {
	return func(c *publishConfig) { c.partitionKey = k }
}

// ─── Opciones de suscripción ─────────────────────────────────────────────────

type subscribeConfig struct {
	durable       string
	maxAckPending int
	tenantID      string
}

// SubscribeOption ajusta una suscripción.
type SubscribeOption func(*subscribeConfig)

// WithDurable fija un nombre de durable explícito. Por defecto se deriva del
// servicio y el subject con DurableName.
func WithDurable(name string) SubscribeOption {
	return func(c *subscribeConfig) { c.durable = name }
}

// WithMaxAckPending ajusta la ventana de mensajes sin confirmar.
func WithMaxAckPending(n int) SubscribeOption {
	return func(c *subscribeConfig) { c.maxAckPending = n }
}

// WithTenantFilter descarta —con ack— los eventos de otros tenants antes de invocar
// al handler — 06-security.md §4.
func WithTenantFilter(tenantID string) SubscribeOption {
	return func(c *subscribeConfig) { c.tenantID = tenantID }
}

// ─── Handler ─────────────────────────────────────────────────────────────────

// Delivery son los metadatos de ESTA entrega concreta del evento.
//
// Van como tercer parámetro y no dentro del context.Context a propósito: son datos
// que el handler consulta, no contexto que propague, y esconderlos en el ctx los
// haría invisibles en la firma.
type Delivery struct {
	// Attempt es el número de entrega, empezando en 1. Si es > 1, este evento YA se
	// intentó procesar antes: la idempotencia no es opcional — 03-delivery.md §4.
	Attempt int
	// MaxAttempts es DefaultMaxDeliver, el techo del consumidor. Al alcanzarlo, el
	// SDK enruta a la DLQ — antes si la clasificación del error impone un presupuesto
	// menor, cosa que aquí todavía no se sabe porque el handler aún no ha fallado
	// — 04-errors.md §2.1.
	MaxAttempts int
	// Subject es el subject de NATS por el que llegó el evento.
	Subject string
	// Durable es el nombre del consumidor. Aparece como `dlqconsumer` en la DLQ.
	Durable string
}

// Handler procesa un evento.
//
// Devolver nil ACK-ea el evento. Devolver un error lo clasifica según
// ClassifierOptions y produce nak, term o term+alerta.
//
// ⚠️ Divergencia con Node: allí el handler recibe un ctx con un método ack()
// explícito que además es un no-op (devolver del handler ya hace ack). Aquí
// `return nil` ES el ack explícito. El requisito del protocolo —nunca auto-ack— se
// cumple igual: el SDK jamás confirma un mensaje antes de que el handler termine.
//
// El ctx que llega lleva dentro el EventContext del evento entrante; pásalo a
// Publish para que correlationid y traceparent se propaguen solos. Ver context.go.
//
// El handler DEBE ser idempotente. La garantía es at-least-once: los duplicados
// llegan, no son un fallo — 03-delivery.md §1.
type Handler func(ctx context.Context, event Event, d Delivery) error

// Subscription es una suscripción viva.
type Subscription struct {
	// Subject de NATS suscrito.
	Subject string
	// Durable name del consumidor.
	Durable string

	consume jetstream.ConsumeContext
	// stopPoll para el sondeo de num_pending. Nil si el sondeo está desactivado.
	stopPoll chan struct{}
	bus      *Bus
	once     sync.Once
}

// Unsubscribe detiene la entrega. Los mensajes ya en el búfer se descartan y se
// reentregarán: eso es correcto, y es exactamente el caso que cubre la idempotencia.
func (s *Subscription) Unsubscribe() {
	s.once.Do(func() {
		s.consume.Stop()
		if s.stopPoll != nil {
			// Dentro del once: cerrar dos veces un canal es un pánico, y Close() llama a
			// Unsubscribe de todas las suscripciones vivas.
			close(s.stopPoll)
		}
		s.bus.forget(s)
	})
}

// ─── Errores del cliente ─────────────────────────────────────────────────────

// ConfigDifference es un campo en el que el servidor no honró lo solicitado.
type ConfigDifference struct {
	Field     string
	Requested any
	Effective any
}

// ConsumerConfigMismatchError se devuelve cuando el servidor aplicó una
// configuración distinta de la solicitada.
//
// Requisito L2 — 03-delivery.md §2.1. Es la ÚNICA defensa contra la sobrescritura
// silenciosa de ack_wait por backoff[0]: JetStream acepta la petición, no avisa, y
// devuelve otra cosa. Sin esta comprobación un handler de más de un segundo se
// ejecuta en concurrencia consigo mismo bajo carga y nada lo indica.
type ConsumerConfigMismatchError struct {
	Durable     string
	Differences []ConfigDifference
}

func (e *ConsumerConfigMismatchError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "el servidor devolvió una configuración distinta de la solicitada para %q:\n", e.Durable)
	for _, d := range e.Differences {
		fmt.Fprintf(&b, "  %s: solicitado %v, efectivo %v\n", d.Field, d.Requested, d.Effective)
	}
	b.WriteString("JetStream sobrescribe algunos campos en silencio (03-delivery.md §2.1). " +
		"Si el campo es ack_wait, comprueba que backoff[0] valga exactamente lo mismo.")
	return b.String()
}

// ─── Bus ─────────────────────────────────────────────────────────────────────

// Bus es el cliente de flux: publica y consume eventos.
//
// Es seguro usarlo desde varios goroutines.
type Bus struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	opts ConnectOptions

	classify Classifier
	source   string

	// Se construyen UNA vez en Connect y no por evento: parsear un PEM en la ruta
	// caliente sería tirar el throughput por comodidad de escritura.
	signer   *Signer
	verifier *Verifier
	// validate es nil en modo off: un servicio en L2 no compila ningún esquema.
	validate *schemaValidator
	metrics  MetricsSink

	mu            sync.Mutex
	subscriptions map[*Subscription]struct{}
	ensured       map[string]struct{}
}

// Connect abre la conexión al bus.
//
// Reconecta indefinidamente con backoff y JITTER: sin jitter, mil servicios
// reconectan en el mismo milisegundo y tiran el cluster que acaba de levantarse
// — 03-delivery.md §6.
func Connect(ctx context.Context, opts ConnectOptions) (*Bus, error) {
	if opts.Service == "" || opts.Environment == "" || opts.Version == "" {
		return nil, errors.New("flux: Service, Environment y Version son obligatorios: " +
			"alimentan `source` y `producerversion`, que son atributos obligatorios del envelope")
	}

	// Las claves se validan ANTES de abrir la conexión: una clave mal formada es un
	// fallo de configuración y debe romper el arranque, no la primera publicación.
	signer, err := NewSigner(opts.Signing)
	if err != nil {
		return nil, err
	}
	verifier, err := NewVerifier(opts.Signing, opts.Logger)
	if err != nil {
		return nil, err
	}

	// Y por lo mismo los esquemas: compilarlos por evento sería tirar el throughput, y un
	// bundle ausente o un esquema roto deben romper el ARRANQUE — 00-protocol.md §5.
	validate, err := newSchemaValidator(opts.Validation, opts.Logger)
	if err != nil {
		return nil, err
	}

	metrics := opts.Metrics
	if metrics == nil {
		metrics = NoMetrics{}
	}

	natsOpts := []nats.Option{
		nats.Name(opts.Service + "@" + opts.Environment),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(1 * time.Second),
		nats.ReconnectJitter(500*time.Millisecond, 1*time.Second),
		// RetryOnFailedConnect equivale al waitOnFirstConnect de Node: un servicio que
		// arranca antes que el broker no debe morir en el primer intento.
		nats.RetryOnFailedConnect(true),

		// flux_connection_state: 1 conectado, 0 desconectado, 2 reconectando
		// — 08-observability.md §2.1. Sin estos callbacks el gauge valdría 1 hasta
		// Close() y no diría nada, que es peor que no publicarlo.
		nats.DisconnectErrHandler(func(*nats.Conn, error) {
			// Con MaxReconnects(-1) una caída SIEMPRE es "reconectando".
			metrics.ConnectionState(StateReconnecting)
		}),
		nats.ReconnectHandler(func(*nats.Conn) { metrics.ConnectionState(StateConnected) }),
		nats.ClosedHandler(func(*nats.Conn) { metrics.ConnectionState(StateDisconnected) }),
	}
	switch {
	case opts.Credentials.CredsFile != "":
		natsOpts = append(natsOpts, nats.UserCredentials(opts.Credentials.CredsFile))
	case opts.Credentials.Token != "":
		natsOpts = append(natsOpts, nats.Token(opts.Credentials.Token))
	case opts.Credentials.User != "":
		natsOpts = append(natsOpts, nats.UserInfo(opts.Credentials.User, opts.Credentials.Password))
	}

	nc, err := nats.Connect(opts.Servers, natsOpts...)
	if err != nil {
		return nil, fmt.Errorf("flux: no se pudo conectar a %q: %w", opts.Servers, err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("flux: no se pudo inicializar JetStream: %w", err)
	}

	metrics.ConnectionState(StateConnected)
	return &Bus{
		nc:            nc,
		js:            js,
		opts:          opts,
		classify:      NewClassifier(opts.Classifier),
		source:        SourceURI(opts.Environment, opts.Service),
		signer:        signer,
		verifier:      verifier,
		validate:      validate,
		metrics:       metrics,
		subscriptions: make(map[*Subscription]struct{}),
		ensured:       make(map[string]struct{}),
	}, nil
}

// Connected informa si la conexión sigue viva. Expuesto para el healthcheck que
// exige 03-delivery.md §6.
func (b *Bus) Connected() bool {
	return b.nc != nil && !b.nc.IsClosed()
}

// Close detiene las suscripciones y drena la conexión.
func (b *Bus) Close() error {
	b.mu.Lock()
	subs := make([]*Subscription, 0, len(b.subscriptions))
	for s := range b.subscriptions {
		subs = append(subs, s)
	}
	b.mu.Unlock()

	for _, s := range subs {
		s.Unsubscribe()
	}
	b.metrics.ConnectionState(StateDisconnected)
	// Drain y no Close: los acks pendientes no se pierden en silencio.
	return b.nc.Drain()
}

func (b *Bus) forget(s *Subscription) {
	b.mu.Lock()
	delete(b.subscriptions, s)
	b.mu.Unlock()
}

// ─── publish ─────────────────────────────────────────────────────────────────

// Publish construye el envelope y publica el evento.
//
// El desarrollador solo escribe subject, data y opcionalmente AggregateID. Todo lo
// demás —id, source, time, specversion, type, dataschema, correlationid,
// causationid, producerversion, traceparent— lo rellena el SDK. Si tu código asigna
// alguno de esos a mano, está mal — 01-envelope.md §5.
//
// El ctx debe ser el que llegó al handler cuando se publica desde dentro de uno: es
// lo que propaga correlationid, causationid y traceparent. Ver context.go.
func (b *Bus) Publish(ctx context.Context, subject string, data any, opts ...PublishOption) (Event, error) {
	parsed, err := ParseSubject(subject) // falla temprano y con un mensaje útil
	if err != nil {
		return Event{}, err
	}

	var cfg publishConfig
	for _, o := range opts {
		o(&cfg)
	}

	if err := b.ensureStream(ctx, parsed.Domain); err != nil {
		return Event{}, err
	}

	schema, err := b.schemaFor(subject, parsed)
	if err != nil {
		return Event{}, err
	}

	// UUIDv7: monotónico en el tiempo, así que ordenar por `id` dentro de un mismo
	// `source` equivale a ordenar por instante de generación — útil al reconstruir
	// historiales desde la DLQ (01-envelope.md §2.2).
	id, err := uuid.NewV7()
	if err != nil {
		return Event{}, fmt.Errorf("flux: no se pudo generar el UUIDv7 del evento: %w", err)
	}
	eventID := id.String()

	inherited, hasContext := EventContextFrom(ctx)

	in := BuildEventInput{
		Subject:         subject,
		Data:            data,
		ID:              eventID,
		Source:          b.source,
		ProducerVersion: b.opts.Version,
		DataSchema:      schema,
		Time:            cfg.when,
		AggregateID:     cfg.aggregateID,
		PartitionKey:    cfg.partitionKey,
	}

	// El contexto heredado gana sobre el default de la conexión: un evento derivado
	// pertenece al tenant del evento que lo causó, no al del servicio.
	switch {
	case cfg.tenantID != "":
		in.TenantID = cfg.tenantID
	case hasContext && inherited.TenantID != "":
		in.TenantID = inherited.TenantID
	case b.opts.TenantID != "":
		in.TenantID = b.opts.TenantID
	default:
		in.TenantID = "system"
	}

	switch {
	case cfg.classification != "":
		in.DataClassification = cfg.classification
	case b.opts.Classification != "":
		in.DataClassification = b.opts.Classification
	default:
		in.DataClassification = ClassificationInternal
	}

	// Si el evento no nace de otro, correlationid se inicializa con su propio id
	// — 01-envelope.md §3.1.
	if hasContext && inherited.CorrelationID != "" {
		in.CorrelationID = inherited.CorrelationID
		in.CausationID = inherited.CausationID
	} else {
		in.CorrelationID = eventID
	}

	if hasContext && inherited.TraceParent != "" {
		in.TraceParent = inherited.TraceParent
		in.TraceState = inherited.TraceState
	} else if b.opts.Traceparent != nil {
		in.TraceParent = b.opts.Traceparent(ctx)
	}

	event, err := BuildEvent(in)
	if err != nil {
		return Event{}, err
	}

	// L3: validar ANTES de publicar. Un payload que viola su contrato debe fallar aquí,
	// en el servicio que lo generó, y no aparecer como un misterio en un consumidor de
	// otro equipo la semana que viene — 00-protocol.md §5.
	if b.validate != nil {
		if err := b.validate.check(event, subject); err != nil {
			b.metrics.EventPublished(subject, PublishInvalidSchema)
			return Event{}, err
		}
	}

	// Firmar es LO ÚLTIMO antes de serializar: la firma cubre el envelope completo, así
	// que cualquier atributo añadido después la invalidaría — 07-signing.md §5.
	if b.signer != nil {
		if event, err = b.signer.Sign(event); err != nil {
			return Event{}, err
		}
	}

	payload, err := Serialize(event)
	if err != nil {
		return Event{}, err
	}

	// WithMsgID pone la cabecera Nats-Msg-Id. Deduplica reintentos de PUBLICACIÓN
	// dentro de duplicate_window: un publish que no recibe el ACK del broker por un
	// corte de red y se reintenta no deja dos copias en el stream.
	//
	// NO deduplica reentregas de consumo. Un nak reentrega el mismo mensaje con el
	// mismo Nats-Msg-Id y eso es correcto y deseado. Nunca sustituye a la
	// idempotencia del consumidor — 03-delivery.md §3.
	if _, err := b.js.Publish(ctx, subject, payload, jetstream.WithMsgID(event.ID)); err != nil {
		b.metrics.EventPublished(subject, PublishError)
		return Event{}, fmt.Errorf("flux: fallo al publicar en %q: %w", subject, err)
	}
	b.metrics.EventPublished(subject, PublishOK)
	return event, nil
}

// schemaFor resuelve la URI de `dataschema` del subject.
func (b *Bus) schemaFor(subject string, p ParsedSubject) (string, error) {
	if s, ok := b.opts.Schemas[subject]; ok && s != "" {
		return s, nil
	}
	// El bundle L3 conoce el MINOR real de cada subject: dentro de un mayor todo es
	// BACKWARD-compatible, así que el más alto acepta lo que aceptan los anteriores
	// — 05-compatibility.md §2.
	if s := SchemaURIFor(b.opts.Validation.Bundle, subject); s != "" {
		return s, nil
	}
	if b.opts.SchemaBaseURL == "" {
		return "", fmt.Errorf(
			"flux: no hay dataschema para %q. Declara Schemas[%q], SchemaBaseURL, o pasa un "+
				"bundle en Validation.Bundle (ConnectOptions)",
			subject, subject)
	}
	// Sin bundle ni mapa explícito solo se puede asumir el .0.0 del mayor. Es suficiente
	// para L2 —el atributo es informativo— pero no para L3.
	return fmt.Sprintf("%s/%s/%s/%s/%d.0.0.json",
		strings.TrimRight(b.opts.SchemaBaseURL, "/"), p.Domain, p.Aggregate, p.Event, p.Major), nil
}

// ─── subscribe ───────────────────────────────────────────────────────────────

// Subscribe crea (o reutiliza) el durable consumer canónico y empieza a entregar.
//
// El ctx solo gobierna la creación del consumidor y de los streams; la entrega
// continúa hasta Unsubscribe o Close.
func (b *Bus) Subscribe(ctx context.Context, subject string, handler Handler, opts ...SubscribeOption) (*Subscription, error) {
	parsed, err := ParseSubject(subject)
	if err != nil {
		return nil, err
	}

	var cfg subscribeConfig
	for _, o := range opts {
		o(&cfg)
	}

	// ANTES de tocar el broker: un error de configuración debe romper el arranque, no
	// dejar a medias un consumidor durable en el servidor — 09-multitenancy.md §3.
	tenantFilter, err := resolveTenantFilter(subject, b.opts.TenantID, cfg.tenantID, b.opts.TenantIsolation)
	if err != nil {
		return nil, err
	}
	cfg.tenantID = tenantFilter

	if err := b.ensureStream(ctx, parsed.Domain); err != nil {
		return nil, err
	}
	if err := b.ensureDLQStream(ctx, parsed.Domain); err != nil {
		return nil, err
	}

	durable := cfg.durable
	if durable == "" {
		if durable, err = DurableName(b.opts.Service, subject); err != nil {
			return nil, err
		}
	}
	maxAckPending := cfg.maxAckPending
	if maxAckPending == 0 {
		maxAckPending = DefaultMaxAckPending
	}

	requested := jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		// ack_wait DEBE ser backoff[0]: JetStream lo sobrescribe con ese valor sin
		// avisar. Ver 03-delivery.md §2.1 y CanonicalBackOff.
		AckWait:       DefaultAckWait,
		MaxDeliver:    DefaultMaxDeliver,
		BackOff:       CanonicalBackOff(),
		MaxAckPending: maxAckPending,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
	}

	stream := StreamName(parsed.Domain)
	consumer, err := b.js.CreateOrUpdateConsumer(ctx, stream, requested)
	if err != nil {
		return nil, fmt.Errorf("flux: no se pudo crear el consumidor %q en %q: %w", durable, stream, err)
	}

	// El servidor no siempre devuelve lo que se le pide. Requisito L2.
	info, err := consumer.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("flux: no se pudo leer la config efectiva de %q: %w", durable, err)
	}
	if err := assertConfigHonored(durable, requested, info.Config); err != nil {
		return nil, err
	}

	sub := &Subscription{Subject: subject, Durable: durable, bus: b}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		b.dispatch(msg, subject, durable, handler, cfg)
	})
	if err != nil {
		return nil, fmt.Errorf("flux: no se pudo iniciar el consumo de %q: %w", durable, err)
	}
	sub.consume = consumeCtx

	// Sondeo de num_pending. Sin él, flux_consumer_pending solo se alimenta de los
	// metadatos de los mensajes entregados — y si el bucle muere dejan de llegar
	// mensajes, así que el gauge se queda PLANO en vez de crecer. Un panel mostraría una
	// línea horizontal, indistinguible de "no pasa nada" (08-observability.md §2.3 y §4).
	if every := b.pendingPollInterval(); every > 0 {
		sub.stopPoll = make(chan struct{})
		go pollPending(every, sub.stopPoll,
			func() (uint64, error) {
				// Un ctx con plazo: sin él, un servidor que acepta la conexión pero no
				// responde dejaría el sondeo colgado para siempre y la métrica muda.
				ctx, cancel := context.WithTimeout(context.Background(), every)
				defer cancel()
				info, err := consumer.Info(ctx)
				if err != nil {
					return 0, err
				}
				return info.NumPending, nil
			},
			func(pending uint64) { b.metrics.ConsumerPending(subject, durable, int(pending)) },
			func(err error) {
				// Un fallo del sondeo NO DEBE afectar al consumo: es telemetría.
				b.logf(slog.LevelWarn, "no se pudo sondear num_pending de %s: %v", durable, err)
			})
	}

	b.mu.Lock()
	b.subscriptions[sub] = struct{}{}
	b.mu.Unlock()

	return sub, nil
}

// assertConfigHonored verifica que el servidor aplicó lo solicitado.
//
// Requisito L2 y única defensa contra sobrescrituras silenciosas — 03-delivery.md
// §2.1. Se comprueban solo los campos que el SDK pide explícitamente: el resto lo
// rellena el servidor con sus defaults y compararlos daría falsos positivos.
func assertConfigHonored(durable string, requested, effective jetstream.ConsumerConfig) error {
	var diffs []ConfigDifference

	if effective.AckWait != requested.AckWait {
		diffs = append(diffs, ConfigDifference{"ack_wait", requested.AckWait, effective.AckWait})
	}
	if effective.MaxDeliver != requested.MaxDeliver {
		diffs = append(diffs, ConfigDifference{"max_deliver", requested.MaxDeliver, effective.MaxDeliver})
	}
	if effective.MaxAckPending != requested.MaxAckPending {
		diffs = append(diffs, ConfigDifference{"max_ack_pending", requested.MaxAckPending, effective.MaxAckPending})
	}
	// Ojo al leer esto: en el paquete jetstream AckExplicitPolicy vale 0, o sea que es
	// el cero del tipo. La comprobación no es redundante —detecta que el servidor
	// devuelva AckAll o AckNone— pero sí explica por qué "no fijar AckPolicy" produce
	// por casualidad el valor correcto. El protocolo exige ack explícito siempre.
	if effective.AckPolicy != requested.AckPolicy {
		diffs = append(diffs, ConfigDifference{"ack_policy", requested.AckPolicy, effective.AckPolicy})
	}
	if !equalDurations(requested.BackOff, effective.BackOff) {
		diffs = append(diffs, ConfigDifference{"backoff", requested.BackOff, effective.BackOff})
	}

	// Invariante del protocolo, no solo del SDK: aunque el servidor haya devuelto lo
	// mismo que se le pidió, ack_wait y backoff[0] tienen que coincidir. Si alguien
	// cambia CanonicalBackOff y olvida DefaultAckWait, esto lo caza aquí y no en
	// producción a las 3 de la mañana.
	if len(effective.BackOff) > 0 && effective.AckWait != effective.BackOff[0] {
		diffs = append(diffs, ConfigDifference{
			"ack_wait == backoff[0]", effective.BackOff[0], effective.AckWait})
	}

	if len(diffs) > 0 {
		return &ConsumerConfigMismatchError{Durable: durable, Differences: diffs}
	}
	return nil
}

func equalDurations(a, b []time.Duration) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ─── flux_consumer_pending ───────────────────────────────────────────────────

// pendingPollInterval resuelve el intervalo del sondeo, o <= 0 si no procede.
//
// No procede con un valor negativo —desactivado explícitamente— ni con el sumidero nulo:
// sería una petición cada 15 s por consumidor para tirar el resultado.
func (b *Bus) pendingPollInterval() time.Duration {
	if _, mudo := b.metrics.(NoMetrics); mudo || b.metrics == nil {
		return 0
	}
	switch {
	case b.opts.PendingPollInterval < 0:
		return 0
	case b.opts.PendingPollInterval == 0:
		return DefaultPendingPoll
	default:
		return b.opts.PendingPollInterval
	}
}

// pollPending sondea num_pending hasta que se cierre stop.
//
// Qué mide con precisión: los mensajes del stream AÚN NO ENTREGADOS a este consumidor.
// Un handler lento NO la hace crecer (sus mensajes ya se entregaron y esperan ack: eso se
// ve en el histograma de duración); un consumidor muerto sí, y sin techo, mientras la
// conexión sigue reportándose sana — 08-observability.md §2.3.
//
// Recibe las tres dependencias como funciones para poder probarse sin broker: el error de
// un sondeo NO puede terminar el bucle, y eso solo se demuestra haciéndolo fallar.
func pollPending(
	every time.Duration,
	stop <-chan struct{},
	fetch func() (uint64, error),
	report func(uint64),
	onErr func(error),
) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			pending, err := fetch()
			if err != nil {
				// Se registra y se vuelve a intentar en el siguiente ciclo. Si un error
				// terminara el bucle, la métrica se apagaría para siempre tras un hipo
				// del broker — y nadie se enteraría, porque el consumidor seguiría
				// consumiendo.
				onErr(err)
				continue
			}
			report(pending)
		}
	}
}

// ─── despacho ────────────────────────────────────────────────────────────────

func (b *Bus) dispatch(msg jetstream.Msg, subject, durable string, handler Handler, cfg subscribeConfig) {
	attempt := 1
	if md, err := msg.Metadata(); err == nil {
		if md.NumDelivered > 0 {
			attempt = int(md.NumDelivered)
		}
		// Los metadatos traen NumPending gratis en cada entrega: más fresco que el sondeo
		// y sin coste. Pero NO lo sustituye — si el bucle muere dejan de llegar mensajes
		// y el gauge se quedaría plano en su último valor en vez de crecer, que es
		// exactamente lo contrario de la señal que hace falta (08-observability.md §2.3).
		// Por eso el SDK usa las dos fuentes.
		b.metrics.ConsumerPending(subject, durable, int(md.NumPending))
	}

	// POISON se detecta ANTES del handler: el mensaje no es interpretable, así que el
	// handler nunca llega a verlo — 04-errors.md §1.3.
	event, err := ParseEvent(msg.Data())
	if err != nil {
		b.handlePoison(subject, durable, msg, err)
		return
	}

	// Filtrar ANTES del handler: un evento de otro tenant no es un fallo, no es para
	// nosotros. Se confirma y se descarta — 09-multitenancy.md §3.
	if cfg.tenantID != "" && event.TenantID != cfg.tenantID {
		_ = msg.Ack()
		return
	}

	// WIP mientras el handler vive: resetea el temporizador de reentrega en el
	// servidor y evita que el mismo evento se ejecute en concurrencia consigo mismo.
	// Cada ack_wait/2 para que un ciclo perdido no agote el plazo — 03-delivery.md §2.1.
	stopWIP := make(chan struct{})
	go func() {
		ticker := time.NewTicker(DefaultAckWait / 2)
		defer ticker.Stop()
		for {
			select {
			case <-stopWIP:
				return
			case <-ticker.C:
				// Un error aquí significa que el mensaje ya está resuelto.
				_ = msg.InProgress()
			}
		}
	}()
	defer close(stopWIP)

	// El contexto del evento entrante se inyecta aquí. En Node esto lo hace
	// AsyncLocalStorage de forma implícita; en Go viaja en el ctx y la aplicación
	// DEBE pasarlo a Publish. Ver context.go.
	ctx := WithEventContext(context.Background(), ContextFromEvent(event))

	delivery := Delivery{
		Attempt:     attempt,
		MaxAttempts: DefaultMaxDeliver,
		Subject:     subject,
		Durable:     durable,
	}

	// La firma se comprueba ANTES de invocar al handler: si el evento fue manipulado, su
	// payload puede ser perfectamente válido y aun así no venir del productor que dice
	// — 07-signing.md §5.1. En modo warn, Check registra y devuelve nil.
	inicio := time.Now()
	handlerErr := b.verify(event)
	// L3 al consumir: el evento es sintácticamente válido pero incumple su contrato.
	// Reintentarlo dará exactamente el mismo resultado, así que la clasificación correcta
	// es PERMANENT — y la declara el propio error (validation.go), no la política del
	// clasificador. Va DESPUÉS de la firma a propósito: si el evento fue manipulado, su
	// payload puede validar perfectamente y aun así no ser del productor que dice.
	if handlerErr == nil && b.opts.Validation.OnConsume && b.validate != nil {
		handlerErr = b.validate.check(event, subject)
	}
	if handlerErr == nil {
		handlerErr = b.invoke(ctx, handler, event, delivery)
	}
	if handlerErr == nil {
		_ = msg.Ack()
		b.metrics.EventConsumed(subject, durable, ConsumeOK)
		b.metrics.HandlerDuration(subject, durable, time.Since(inicio).Seconds())
		return
	}

	c := b.classify(handlerErr)

	// El presupuesto efectivo es el menor entre el del consumidor y el que la
	// clasificación imponga para ESTE error. Así un error desconocido agota su
	// presupuesto acotado sin recortar los 6 intentos de un ECONNRESET reconocido
	// — 04-errors.md §2.1.
	budget := DefaultMaxDeliver
	if c.MaxAttempts > 0 && c.MaxAttempts < budget {
		budget = c.MaxAttempts
	}

	if c.Class == ClassRetryable && attempt < budget {
		// Retraso explícito según el backoff canónico en vez de dejar expirar
		// ack_wait: no retiene la ranura de max_ack_pending 30 s de más.
		delay := c.RetryAfter
		if delay <= 0 {
			backoff := CanonicalBackOff()
			idx := attempt - 1
			if idx >= len(backoff) {
				idx = len(backoff) - 1
			}
			delay = backoff[idx]
		}
		b.logf(slog.LevelWarn, "RETRYABLE en %s: %s (intento %d/%d), reintento en %s",
			subject, c.Code, attempt, budget, delay)
		b.metrics.EventRetried(subject, durable, attempt)
		// ⚠️ El delay solo se honra en la PRIMERA reentrega: con backoff configurado —y
		// flux lo configura siempre— JetStream manda el array a partir de la segunda y
		// lo ignora sin avisar (03-delivery.md §2.2).
		_ = msg.NakWithDelay(delay)
		return
	}

	reason := DLQReasonPermanent
	switch c.Class {
	case ClassPoison:
		reason = DLQReasonPoison
	case ClassRetryable:
		reason = DLQReasonRetryable // agotó los reintentos
	}

	if err := b.sendToDLQ(subject, event, durable, attempt, reason, c.Code+": "+handlerErr.Error()); err != nil {
		// No se puede hacer más: si el term() se emitiera igualmente, el evento se
		// perdería sin rastro. Se deja sin resolver para que JetStream lo reentregue.
		b.logf(slog.LevelError, "no se pudo enrutar a la DLQ %s: %v — el mensaje se reentregará",
			DLQSubject(subject), err)
		return
	}

	b.metrics.EventConsumed(subject, durable, outcomeFor(reason, c.Code))
	b.metrics.EventDLQ(subject, durable, reason, c.Code)
	b.metrics.HandlerDuration(subject, durable, time.Since(inicio).Seconds())

	if b.opts.OnDLQ != nil {
		b.opts.OnDLQ(DLQEventInfo{Subject: subject, Event: event, Classification: c})
	}
	b.logf(slog.LevelError, "DLQ (%s) %s en %s tras %d intento(s): %v",
		reason, c.Code, subject, attempt, handlerErr)
	_ = msg.Term()
}

// verify aplica la política de firma, o nil si la extensión está apagada.
func (b *Bus) verify(event Event) error {
	if b.verifier == nil {
		return nil
	}
	return b.verifier.Check(event)
}

// outcomeFor traduce el motivo de DLQ al valor de la etiqueta `outcome`.
//
// La firma inválida se separa del POISON común porque son dos incidentes distintos
// —basura frente a suplantación— con dos respuestas distintas, y la etiqueta existe para
// eso (08-observability.md §2.1). El `reason` de la DLQ sigue siendo `poison`, que es el
// enum cerrado de 04-errors.md §1.
func outcomeFor(reason DLQReason, code string) ConsumeOutcome {
	switch code {
	case "MISSING_SIGNATURE", "INVALID_SIGNATURE", "UNKNOWN_SIGNING_KEY":
		return ConsumeInvalidSignature
	// Mismo motivo con la validación L3: "este consumidor rechaza el evento" y "el
	// productor publica payloads que violan su contrato" son dos incidentes con dos
	// dueños distintos. El `reason` de la DLQ sigue siendo `permanent`, que es el enum
	// cerrado de 04-errors.md §1.
	case CodeSchemaInvalid, CodeSchemaNotFound:
		return ConsumeInvalidSchema
	}
	switch reason {
	case DLQReasonRetryable:
		return ConsumeRetryable
	case DLQReasonPoison:
		return ConsumePoison
	default:
		return ConsumePermanent
	}
}

// invoke aísla el pánico del handler.
//
// No existe en el SDK de Node porque una excepción de JavaScript ya es un error
// capturable. En Go un nil-pointer dereference dentro del handler mataría el proceso
// entero y con él las demás suscripciones. Se convierte en PERMANENT: un pánico es
// un bug del consumidor, y reintentarlo cinco veces no lo arregla.
func (b *Bus) invoke(ctx context.Context, handler Handler, event Event, d Delivery) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = NewPermanentError(fmt.Sprintf("pánico en el handler de %s: %v", d.Subject, r),
				WithCode("HANDLER_PANIC"))
		}
	}()
	return handler(ctx, event, d)
}

func (b *Bus) handlePoison(subject, durable string, msg jetstream.Msg, err error) {
	if b.opts.OnPoison != nil {
		b.opts.OnPoison(PoisonInfo{Subject: subject, Err: err, Raw: msg.Data()})
	}
	code := "POISON"
	var pe *PoisonError
	if errors.As(err, &pe) && pe.Code != "" {
		code = pe.Code
	}
	b.metrics.EventConsumed(subject, durable, ConsumePoison)
	b.metrics.EventDLQ(subject, durable, DLQReasonPoison, code)
	b.logf(slog.LevelError, "POISON en %s: %v", subject, err)
	if dlqErr := b.sendRawToDLQ(subject, msg.Data(), durable, err.Error()); dlqErr != nil {
		b.logf(slog.LevelError, "no se pudo enrutar el POISON a la DLQ: %v", dlqErr)
		return // sin term(): mejor reentregar que perder el cuerpo sin rastro
	}
	_ = msg.Term()
}

// sendToDLQ publica el CloudEvent original íntegro más las extensiones dlq*.
func (b *Bus) sendToDLQ(subject string, event Event, consumer string, attempts int, reason DLQReason, errText string) error {
	dlqEvent := ToDLQEvent(event, DLQInfo{
		Reason: reason, Attempts: attempts, Consumer: consumer, Error: errText,
	})
	payload, err := Serialize(dlqEvent)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// El msgID incluye el consumidor: dos consumidores distintos del mismo evento
	// producen dos entradas de DLQ, y son dos hechos distintos.
	_, err = b.js.Publish(ctx, DLQSubject(subject), payload,
		jetstream.WithMsgID("dlq-"+event.ID+"-"+consumer))
	return err
}

// sendRawToDLQ guarda un mensaje ininterpretable.
//
// Un POISON no tiene CloudEvent que preservar, así que se envuelve el cuerpo crudo en
// un envelope sintético del dominio `system`. Es la única excepción a la regla de "el
// mensaje de DLQ es el original íntegro": no había original que preservar.
func (b *Bus) sendRawToDLQ(subject string, raw []byte, consumer, errText string) error {
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	eventID := id.String()
	now := FormatTime(time.Now())

	// El base64 se recorta para que el envelope quepa en 1 MiB con margen para el
	// resto de atributos.
	encoded := base64.StdEncoding.EncodeToString(raw)
	if len(encoded) > 700_000 {
		encoded = encoded[:700_000]
	}
	data, err := marshalNoHTMLEscape(map[string]any{
		"originalSubject": subject,
		"rawBase64":       encoded,
		"rawBytes":        len(raw),
	})
	if err != nil {
		return err
	}

	envelope := Event{
		SpecVersion:        SpecVersion,
		ID:                 eventID,
		Source:             b.source,
		Type:               "com.flux.system.poison.capturado.v1",
		Time:               now,
		DataContentType:    DataContentType,
		DataSchema:         "https://schemas.internal/system/poison/capturado/1.0.0.json",
		CorrelationID:      eventID,
		TenantID:           "system",
		ProducerVersion:    b.opts.Version,
		DataClassification: ClassificationInternal,
		DLQReason:          DLQReasonPoison,
		DLQAttempts:        1,
		DLQConsumer:        consumer,
		DLQError:           truncateRunes(errText, 1024),
		DLQTime:            now,
		Data:               data,
	}
	payload, err := Serialize(envelope)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = b.js.Publish(ctx, DLQSubject(subject), payload,
		jetstream.WithMsgID("poison-"+eventID))
	return err
}

// ─── provisión de streams ────────────────────────────────────────────────────

func (b *Bus) ensureStream(ctx context.Context, domain string) error {
	return b.ensure(ctx, StreamName(domain), jetstream.StreamConfig{
		Name:     StreamName(domain),
		Subjects: []string{domain + ".>"},
		Storage:  jetstream.FileStorage,
		// LimitsPolicy: el stream retiene por edad, no hasta que alguien consuma. Un
		// consumidor lento no debe poder hacer crecer el disco sin límite.
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardOld,
		MaxAge:    StreamMaxAge,
		// Duplicates solo aplica a publicaciones. Ver DuplicateWindow.
		Duplicates: DuplicateWindow,
	})
}

func (b *Bus) ensureDLQStream(ctx context.Context, domain string) error {
	// Prefijo `dlq.`, nunca sufijo: con sufijo encajaría con `<domain>.>` y el stream
	// principal capturaría sus propios muertos — 02-naming.md §3.1.
	return b.ensure(ctx, DLQStreamName(domain), jetstream.StreamConfig{
		Name:      DLQStreamName(domain),
		Subjects:  []string{"dlq." + domain + ".>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardOld,
		MaxAge:    DLQStreamMaxAge,
	})
}

// ensure crea el stream si no existe, y si existe lo deja tal cual.
//
// No se usa CreateOrUpdateStream a propósito: un SDK que actualiza streams ajenos en
// cada arranque puede pisar en silencio la configuración que puso el equipo de
// plataforma (replicas, límites, mirrors). En producción los streams los provisiona
// infraestructura; esto es una comodidad de desarrollo.
//
// Replicas se deja sin fijar: la spec pide 3, pero un cluster de un solo nodo —el
// docker-compose de desarrollo— rechazaría la creación.
func (b *Bus) ensure(ctx context.Context, name string, cfg jetstream.StreamConfig) error {
	b.mu.Lock()
	if _, ok := b.ensured[name]; ok {
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()

	_, err := b.js.Stream(ctx, name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		if _, err = b.js.CreateStream(ctx, cfg); err != nil &&
			!errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			return fmt.Errorf("flux: no se pudo crear el stream %q: %w", name, err)
		}
	} else if err != nil {
		return fmt.Errorf("flux: no se pudo consultar el stream %q: %w", name, err)
	}

	b.mu.Lock()
	b.ensured[name] = struct{}{}
	b.mu.Unlock()
	return nil
}

func (b *Bus) logf(level slog.Level, format string, args ...any) {
	if b.opts.Logger == nil {
		return
	}
	b.opts.Logger.Log(context.Background(), level, "[flux] "+fmt.Sprintf(format, args...))
}
