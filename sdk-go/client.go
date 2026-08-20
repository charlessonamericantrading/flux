// Cliente de flux. Nivel de conformidad: L2.
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

	// TenantID por defecto de los eventos publicados. Vacío significa "system".
	TenantID string
	// Classification por defecto. Vacío significa "internal" — 06-security.md §5.
	Classification DataClassification

	// Schemas es el mapa exacto subject → URI de `dataschema`. Gana sobre SchemaBaseURL.
	Schemas map[string]string
	// SchemaBaseURL es la base para derivar `dataschema` cuando no está en Schemas.
	SchemaBaseURL string

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
	// MaxAttempts es DefaultMaxDeliver. Al alcanzarlo, el SDK enruta a la DLQ.
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
	bus     *Bus
	once    sync.Once
}

// Unsubscribe detiene la entrega. Los mensajes ya en el búfer se descartan y se
// reentregarán: eso es correcto, y es exactamente el caso que cubre la idempotencia.
func (s *Subscription) Unsubscribe() {
	s.once.Do(func() {
		s.consume.Stop()
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

	natsOpts := []nats.Option{
		nats.Name(opts.Service + "@" + opts.Environment),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(1 * time.Second),
		nats.ReconnectJitter(500*time.Millisecond, 1*time.Second),
		// RetryOnFailedConnect equivale al waitOnFirstConnect de Node: un servicio que
		// arranca antes que el broker no debe morir en el primer intento.
		nats.RetryOnFailedConnect(true),
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

	return &Bus{
		nc:            nc,
		js:            js,
		opts:          opts,
		classify:      NewClassifier(opts.Classifier),
		source:        SourceURI(opts.Environment, opts.Service),
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
		return Event{}, fmt.Errorf("flux: fallo al publicar en %q: %w", subject, err)
	}
	return event, nil
}

// schemaFor resuelve la URI de `dataschema` del subject.
func (b *Bus) schemaFor(subject string, p ParsedSubject) (string, error) {
	if s, ok := b.opts.Schemas[subject]; ok && s != "" {
		return s, nil
	}
	if b.opts.SchemaBaseURL == "" {
		return "", fmt.Errorf(
			"flux: no hay dataschema para %q. Declara Schemas[%q] o SchemaBaseURL en ConnectOptions",
			subject, subject)
	}
	// Sin registro exacto solo se puede asumir el .0.0 del mayor. Un SDK L3 resolverá
	// el minor real contra el Schema Registry — 00-protocol.md §5.
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

// ─── despacho ────────────────────────────────────────────────────────────────

func (b *Bus) dispatch(msg jetstream.Msg, subject, durable string, handler Handler, cfg subscribeConfig) {
	attempt := 1
	if md, err := msg.Metadata(); err == nil && md.NumDelivered > 0 {
		attempt = int(md.NumDelivered)
	}

	// POISON se detecta ANTES del handler: el mensaje no es interpretable, así que el
	// handler nunca llega a verlo — 04-errors.md §1.3.
	event, err := ParseEvent(msg.Data())
	if err != nil {
		b.handlePoison(subject, durable, msg, err)
		return
	}

	if cfg.tenantID != "" && event.TenantID != cfg.tenantID {
		_ = msg.Ack() // no es para nosotros
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

	handlerErr := b.invoke(ctx, handler, event, delivery)
	if handlerErr == nil {
		_ = msg.Ack()
		return
	}

	c := b.classify(handlerErr)

	if c.Class == ClassRetryable && attempt < DefaultMaxDeliver {
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
			subject, c.Code, attempt, DefaultMaxDeliver, delay)
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

	if b.opts.OnDLQ != nil {
		b.opts.OnDLQ(DLQEventInfo{Subject: subject, Event: event, Classification: c})
	}
	b.logf(slog.LevelError, "DLQ (%s) %s en %s tras %d intento(s): %v",
		reason, c.Code, subject, attempt, handlerErr)
	_ = msg.Term()
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
