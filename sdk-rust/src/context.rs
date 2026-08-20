//! Propagación de contexto entre eventos.
//!
//! Contrato normativo: `specification/01-envelope.md` §5
//!
//! # La decisión: task-local **con** override explícito
//!
//! Los SDKs anteriores se repartieron en dos bandos y por una razón concreta:
//!
//! - **Node** usa `AsyncLocalStorage`: un `publish()` en cualquier punto de la pila de
//!   llamadas de un handler hereda el contexto del evento entrante sin que nadie pase
//!   nada por parámetro.
//! - **Go** lo hace explícito, porque **no tiene equivalente**: no hay almacenamiento
//!   ligado al goroutine, y emularlo con un mapa por goroutine ID es un antipatrón
//!   reconocido que además se rompe en cuanto el handler lanza un goroutine hijo.
//!
//! Rust **sí** tiene el mecanismo que a Go le falta: [`tokio::task_local!`] es de
//! primera clase, sin trucos y sin coste cuando no se usa. Por eso este SDK propaga
//! **por task-local**, como Node, y el desarrollador no escribe `correlationid` jamás:
//!
//! ```no_run
//! # async fn ejemplo(bus: flux::Bus) -> Result<(), flux::FluxError> {
//! // El clon va FUERA del closure: `subscribe` toma `&self` y el closure necesita su
//! // propia copia. `Bus` es un Arc por dentro, así que clonar es gratis.
//! let publicador = bus.clone();
//! bus.subscribe("pedidos.pedido.v1.creado", move |evento: flux::Event, _entrega| {
//!     let bus = publicador.clone();
//!     async move {
//!         // correlationid, causationid, tenantid y traceparent se propagan solos:
//!         // basta con estar dentro del task del handler.
//!         bus.publish("facturacion.factura.v1.emitida", &evento.data).await?;
//!         Ok(())
//!     }
//! })
//! .await?;
//! # Ok(()) }
//! ```
//!
//! ## El límite, dicho en voz alta
//!
//! Un task-local **no cruza `tokio::spawn`**. Si el handler lanza un task hijo y publica
//! desde ahí, la cadena de correlación se rompe — igual que en Node se rompe al saltar a
//! un `EventEmitter` externo. La diferencia es que aquí la reparación es una línea:
//!
//! ```no_run
//! # async fn ejemplo(bus: flux::Bus) {
//! let ctx = flux::context::current();          // capturado ANTES del spawn
//! tokio::spawn(async move {
//!     flux::context::scope(ctx, async move {
//!         // aquí dentro el contexto vuelve a estar disponible
//!         let _ = bus.publish("pedidos.pedido.v1.creado", &serde_json::json!({})).await;
//!     })
//!     .await;
//! });
//! # }
//! ```
//!
//! Y cuando la propagación implícita no vale —un job diferido, un reintento leído de una
//! tabla— siempre existe el camino explícito:
//! [`PublishOptions::with_context`](crate::PublishOptions::with_context), que **gana**
//! sobre el task-local.

use std::future::Future;

use crate::envelope::Event;

/// Lo que un evento entrante lega a los eventos que provoque.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventContext {
    /// Se propaga **SIN MODIFICAR** por toda la cadena. Es la respuesta a "¿de qué flujo
    /// de negocio forma parte esto?".
    pub correlation_id: String,

    /// El `id` del evento en curso, que pasa a ser el `causationid` de lo que se
    /// publique desde este handler: "¿quién causó esto exactamente?".
    pub causation_id: String,

    /// Tenant del evento en curso. **Gana sobre el default de la conexión**: un evento
    /// derivado pertenece al tenant del evento que lo causó, no al del servicio.
    pub tenant_id: String,

    /// W3C Trace Context heredado.
    pub traceparent: Option<String>,

    /// W3C Trace Context heredado.
    pub tracestate: Option<String>,
}

impl EventContext {
    /// Deriva el contexto que un evento lega a sus descendientes.
    ///
    /// Nótese que `causation_id` toma el `id` del evento, **no su `causationid`**: la
    /// causa de lo que se publique ahora es ESTE evento, no el que lo causó a él
    /// — 01-envelope.md §3.2.
    #[must_use]
    pub fn from_event(event: &Event) -> Self {
        Self {
            correlation_id: event.correlationid.clone(),
            causation_id: event.id.clone(),
            tenant_id: event.tenantid.clone(),
            traceparent: event.traceparent.clone(),
            tracestate: event.tracestate.clone(),
        }
    }
}

tokio::task_local! {
    static CURRENT: EventContext;
}

/// El contexto del evento que se está procesando en este task, si lo hay.
///
/// `None` es el caso normal de un `publish` desde una ruta HTTP o un cron: el evento
/// nace de cero y el SDK inicializará su `correlationid` con su propio `id`.
#[must_use]
pub fn current() -> Option<EventContext> {
    CURRENT.try_with(Clone::clone).ok()
}

/// Ejecuta un future con un contexto de evento instalado.
///
/// Lo llama el SDK antes de invocar al handler. Una aplicación solo lo necesita para
/// tests, para reanudar una cadena de correlación desde un trabajo diferido, o para
/// volver a instalarla dentro de un `tokio::spawn` (ver el módulo).
///
/// Pasar `None` ejecuta el future **sin** contexto, lo que es lo correcto cuando no se
/// capturó ninguno: instalar un contexto vacío inventaría un `correlationid` de "".
pub async fn scope<F: Future>(ctx: Option<EventContext>, fut: F) -> F::Output {
    match ctx {
        Some(ctx) => CURRENT.scope(ctx, fut).await,
        None => fut.await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{build_event, data_to_raw, BuildEventInput, DataClassification};
    use serde_json::json;

    fn evento() -> Event {
        build_event(BuildEventInput {
            subject: "pedidos.pedido.v1.creado".into(),
            data: data_to_raw(&json!({ "pedidoId": "ped-123" })).unwrap(),
            id: "id-del-evento".into(),
            source: "/produccion/pedidos-api".into(),
            producerversion: "1.0.0".into(),
            tenantid: "acme".into(),
            dataclassification: DataClassification::Internal,
            dataschema: "https://schemas.internal/x/1.0.0.json".into(),
            correlationid: "corr-1".into(),
            time: None,
            aggregate_id: None,
            causationid: Some("causa-anterior".into()),
            partitionkey: None,
            traceparent: Some("00-abc-def-01".into()),
            tracestate: None,
        })
        .unwrap()
    }

    /// La causa de lo que se publique ahora es ESTE evento, no el que lo causó a él.
    #[test]
    fn el_causation_id_es_el_id_del_evento_en_curso() {
        let ctx = EventContext::from_event(&evento());
        assert_eq!(ctx.correlation_id, "corr-1");
        assert_eq!(ctx.causation_id, "id-del-evento");
        assert_ne!(ctx.causation_id, "causa-anterior");
        assert_eq!(ctx.tenant_id, "acme");
        assert_eq!(ctx.traceparent.as_deref(), Some("00-abc-def-01"));
    }

    #[tokio::test]
    async fn el_contexto_viaja_por_el_task() {
        assert!(current().is_none());
        let ctx = EventContext::from_event(&evento());
        scope(Some(ctx.clone()), async {
            // A cualquier profundidad de la pila de llamadas, como el AsyncLocalStorage
            // de Node.
            async {
                assert_eq!(current().unwrap(), ctx);
            }
            .await;
        })
        .await;
        assert!(current().is_none(), "no se filtra fuera del scope");
    }

    #[tokio::test]
    async fn scope_con_none_no_instala_nada() {
        scope(None, async {
            assert!(current().is_none());
        })
        .await;
    }

    /// El límite documentado: un task-local no cruza `tokio::spawn`. Este test existe
    /// para que la limitación esté escrita en código y no solo en prosa.
    #[tokio::test]
    async fn el_contexto_no_cruza_un_spawn_pero_scope_lo_recupera() {
        let ctx = EventContext::from_event(&evento());
        scope(Some(ctx.clone()), async {
            let perdido = tokio::spawn(async { current() }).await.unwrap();
            assert!(perdido.is_none(), "un task-local no cruza spawn");

            let capturado = current();
            let recuperado = tokio::spawn(scope(capturado, async { current() }))
                .await
                .unwrap();
            assert_eq!(recuperado, Some(ctx));
        })
        .await;
    }
}
