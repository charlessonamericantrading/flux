//! Cliente Rust del **flux Event Protocol v1** — CloudEvents 1.0 sobre NATS JetStream.
//! Nivel de conformidad objetivo: **L2**.
//!
//! El contrato normativo vive en `specification/` del repositorio; si algo de esta
//! documentación diverge de la spec, manda la spec.
//!
//! # Publicar
//!
//! ```no_run
//! use serde::Serialize;
//!
//! #[derive(Serialize)]
//! #[serde(rename_all = "camelCase")]
//! struct PedidoCreado {
//!     pedido_id: String,
//!     aggregate_version: u64,
//!     total_cents: i64,
//!     moneda: String,
//! }
//!
//! # async fn ejemplo() -> Result<(), flux::FluxError> {
//! let bus = flux::connect(
//!     flux::ConnectOptions::new("nats://localhost:4222", "pedidos-api", "produccion", "3.4.1")
//!         .with_tenant_id("acme")
//!         .with_schema_base_url("https://schemas.internal"),
//! )
//! .await?;
//!
//! bus.publish_with(
//!     "pedidos.pedido.v1.creado",
//!     &PedidoCreado {
//!         pedido_id: "ped-123".into(),
//!         aggregate_version: 1,
//!         total_cents: 9990,
//!         moneda: "EUR".into(),
//!     },
//!     flux::PublishOptions::default().with_aggregate_id("ped-123"),
//! )
//! .await?;
//! # Ok(()) }
//! ```
//!
//! **Solo escribes subject, `data` y opcionalmente el `aggregate_id`.** El SDK rellena
//! `id` (UUIDv7), `source`, `time`, `specversion`, `type`, `dataschema`, `correlationid`,
//! `causationid`, `producerversion` y `traceparent`. Si tu código asigna alguno de esos a
//! mano, está mal — 01-envelope.md §5.
//!
//! # Consumir
//!
//! ```no_run
//! # async fn ejemplo(bus: flux::Bus) -> Result<(), flux::FluxError> {
//! # async fn ya_procesado(_: &str) -> bool { false }
//! # async fn hacer_el_trabajo() -> Result<(), std::io::Error> { Ok(()) }
//! let sub = bus
//!     .subscribe("pedidos.pedido.v1.creado", |evento: flux::Event, _entrega| async move {
//!         // Idempotencia: OBLIGATORIA. La garantía es at-least-once y los duplicados
//!         // llegan — no son un fallo (03-delivery.md §1).
//!         if ya_procesado(&evento.id).await {
//!             return Ok(());
//!         }
//!         hacer_el_trabajo()
//!             .await
//!             .map_err(|e| flux::FluxError::retryable("proveedor caído").with_source(e))?;
//!         Ok(()) // Ok(()) == ack explícito
//!     })
//!     .await?;
//!
//! sub.unsubscribe();
//! # Ok(()) }
//! ```
//!
//! # Las cuatro reglas que no se negocian
//!
//! 1. **Nunca inventes un subject.** `<dominio>.<agregado>.v<major>.<evento>`, 4 tokens,
//!    minúsculas, evento en pasado — 02-naming.md §1.
//! 2. **Nunca escribas el envelope a mano.** Lo rellena el SDK — 01-envelope.md §5.
//! 3. **Todo consumidor DEBE ser idempotente.** Elige una de las tres estrategias de
//!    03-delivery.md §4.
//! 4. **Nunca asumas orden.** Incluye `aggregateVersion` en `data` y filtra con
//!    `WHERE aggregate_version < $n` — 03-delivery.md §5.

// `unsafe` no tiene ningún papel en un cliente de mensajería.
#![forbid(unsafe_code)]
// Todo elemento público del SDK documenta POR QUÉ existe, no solo qué hace.
#![deny(missing_docs)]
#![warn(clippy::pedantic)]
// Los mensajes de error del protocolo son largos a propósito: explican la trampa.
#![allow(clippy::doc_markdown, clippy::missing_errors_doc)]
// `deny(warnings)` a nivel de crate NO se usa a propósito: convierte cualquier lint nuevo
// del compilador en un build roto para quien compile con un toolchain más moderno, y este
// SDK lo consumen servicios que no controlan su versión de Rust. Los warnings se deniegan
// donde corresponde —en CI, con `cargo clippy -- -D warnings`—, que es el único sitio
// donde el toolchain está fijado.

pub mod classify;
pub mod client;
pub mod context;
pub mod envelope;
pub mod errors;
pub mod metrics;
pub mod protocol;

/// Firma Ed25519 de eventos — **extensión OPCIONAL**, detrás de la feature `signing`.
///
/// No se activa por defecto: un evento sin firma sigue siendo válido (07-signing.md), así
/// que imponer `ed25519-dalek` a todo el ecosistema sería cobrar a todos por lo que usan
/// unos pocos.
///
/// ```toml
/// flux = { path = "../sdk-rust", features = ["signing"] }
/// ```
#[cfg(feature = "signing")]
pub mod signing;

pub use classify::{Classifier, ClassifierOptions, HttpError, Rule, UnknownPolicy};
pub use client::{
    connect, Bus, ConnectOptions, Credentials, Delivery, DlqEventInfo, Handler, LogFn, LogLevel,
    PoisonInfo, PublishOptions, SubscribeOptions, Subscription, TenantIsolation, TraceparentFn,
};
pub use context::EventContext;
pub use envelope::{
    build_event, data_to_raw, format_time, parse_event, serialize, strip_dlq_extensions,
    to_dlq_event, BuildEventInput, DataClassification, DlqInfo, DlqReason, Event,
    ALLOWED_ROOT_ATTRIBUTES,
};
pub use errors::{
    as_classified, describe, Classification, ConfigDifference, ErrorClass, Failure, FluxError,
    HandlerError, HandlerResult,
};
pub use metrics::{
    ConnectionState, ConsumeOutcome, DlqReasonLabel, InMemoryMetrics, MetricsSink, NoMetrics,
    PublishOutcome, DURATION_BUCKETS,
};
pub use protocol::{
    dlq_stream_name, dlq_subject, durable_name, is_dlq_subject, is_valid_subject, parse_subject,
    source_uri, stream_name, subject_to_type, total_time_to_dlq, validate_service_name,
    ParsedSubject, CANONICAL_BACKOFF, CONFORMANCE_LEVEL, DATA_CONTENT_TYPE, DEFAULT_ACK_WAIT,
    DEFAULT_MAX_ACK_PENDING, DEFAULT_MAX_DELIVER, DLQ_STREAM_MAX_AGE, DUPLICATE_WINDOW,
    MAX_MESSAGE_BYTES, PROTOCOL_NAME, PROTOCOL_VERSION, SPEC_VERSION, STREAM_MAX_AGE,
};
#[cfg(feature = "signing")]
pub use signing::{generate_key_pair, Signer, SigningOptions, VerificationMode, Verifier};
