// Propagación de contexto entre eventos.
// Contrato normativo: specification/01-envelope.md §5
//
// ⚠️ DIVERGENCIA DELIBERADA CON EL SDK DE NODE
//
// Node usa AsyncLocalStorage: un publish() en cualquier punto de la pila de llamadas
// de un handler hereda el contexto del evento entrante sin que nadie pase nada por
// parámetro. Go no tiene equivalente — no hay almacenamiento ligado al goroutine, y
// cualquier emulación (mapa por goroutine ID) es un antipatrón reconocido que además
// se rompe en cuanto el handler lanza un goroutine hijo.
//
// Por eso en Go el contexto es EXPLÍCITO: viaja en el context.Context que el SDK le
// entrega al handler y que la aplicación debe pasar de vuelta a Publish.
//
//	bus.Subscribe(ctx, "pedidos.pedido.v1.creado",
//	    func(ctx context.Context, ev flux.Event, d flux.Delivery) error {
//	        // este ctx lleva dentro el contexto del evento entrante
//	        _, err := bus.Publish(ctx, "facturacion.factura.v1.emitida", payload)
//	        return err  // correlationid y traceparent se propagan solos
//	    })
//
// La consecuencia práctica: si la aplicación pasa context.Background() a Publish en
// lugar del ctx del handler, la cadena de correlación se ROMPE en silencio. En Node
// eso no puede pasar; en Go sí. Es el precio de no tener magia, y a cambio la
// propagación es visible en la firma de cada función que la necesita — que es lo que
// permitirá auditarla en una revisión de código.

package flux

import "context"

// EventContext es lo que un evento entrante lega a los eventos que provoque.
type EventContext struct {
	// CorrelationID se propaga SIN MODIFICAR por toda la cadena. Es la respuesta a
	// "¿de qué flujo de negocio forma parte esto?".
	CorrelationID string

	// CausationID es el ID del evento en curso, que pasa a ser el causationid de lo
	// que se publique desde este handler: "¿quién causó esto exactamente?".
	CausationID string

	// TenantID del evento en curso. Gana sobre el default de la conexión: un evento
	// derivado pertenece al tenant del evento que lo causó, no al del servicio.
	TenantID string

	TraceParent string
	TraceState  string
}

// contextKey es un tipo privado para la clave de context.WithValue.
//
// Tipo propio y no un string: una clave de tipo string podría colisionar con la de
// otra librería que use el mismo literal, y la colisión sería silenciosa.
type contextKey struct{}

var eventContextKey contextKey

// WithEventContext devuelve un contexto que lleva dentro el contexto de evento.
// Lo llama el SDK antes de invocar al handler; una aplicación solo lo necesita para
// tests o para reanudar una cadena de correlación desde un trabajo diferido.
func WithEventContext(ctx context.Context, ec EventContext) context.Context {
	return context.WithValue(ctx, eventContextKey, ec)
}

// EventContextFrom extrae el contexto de evento, si lo hay.
//
// El ok=false es el caso normal de un publish desde una ruta HTTP o un cron: el
// evento nace de cero y Publish inicializará su correlationid con su propio id.
func EventContextFrom(ctx context.Context) (EventContext, bool) {
	ec, ok := ctx.Value(eventContextKey).(EventContext)
	return ec, ok
}

// ContextFromEvent deriva el contexto que un evento lega a sus descendientes.
//
// Nótese que CausationID toma el ID del evento, no su CausationID: la causa de lo que
// se publique ahora es ESTE evento, no el que lo causó a él — 01-envelope.md §3.2.
func ContextFromEvent(e Event) EventContext {
	return EventContext{
		CorrelationID: e.CorrelationID,
		CausationID:   e.ID,
		TenantID:      e.TenantID,
		TraceParent:   e.TraceParent,
		TraceState:    e.TraceState,
	}
}

// TraceparentFunc extrae un traceparent W3C del contexto activo.
//
// Otra divergencia con Node: allí el SDK hace un `import()` dinámico de
// @opentelemetry/api y falla en silencio si no está instalado. Go no tiene imports
// dinámicos, y una dependencia dura de OpenTelemetry obligaría a instalarlo a todo
// servicio que use el SDK aunque no lo use.
//
// Así que se invierte: la aplicación inyecta la función en ConnectOptions. Con
// OpenTelemetry es una línea:
//
//	Traceparent: func(ctx context.Context) string {
//	    sc := trace.SpanContextFromContext(ctx)
//	    if !sc.IsValid() { return "" }
//	    return fmt.Sprintf("00-%s-%s-%s", sc.TraceID(), sc.SpanID(), sc.TraceFlags())
//	},
//
// Devolver "" significa "no hay span activo" y el atributo se omite.
type TraceparentFunc func(ctx context.Context) string
