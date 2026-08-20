"""
flux SDK para Python — Event Protocol v1, nivel de conformidad L3.

L3 es **opt-in**: sin `validation=ValidationOptions(mode=...)` el SDK se comporta
exactamente como L2 y no importa `jsonschema` siquiera (ver flux/validation.py).

Especificación: https://github.com/charlessonamericantrading/flux

`connect`, `FluxBus` y compañía se importan de forma diferida (PEP 562). El motivo es
práctico: `client` es el único módulo que necesita `nats-py`, y sin la carga diferida
un test de naming o de envelope —que no toca el broker— exigiría tener instalado el
cliente de NATS. La API pública es la misma; solo cambia cuándo se resuelve.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .classify import (
    RETRYABLE_HTTP_STATUS,
    TRANSIENT_SYSCALL_CODES,
    ClassifierOptions,
    classify,
    create_classifier,
)
from .context import EventContext, current_context, use_context
from .envelope import (
    ALLOWED_ROOT_ATTRIBUTES,
    DataClassification,
    DlqReason,
    EnvelopeError,
    FluxEvent,
    build_event,
    parse_event,
    serialize,
    strip_dlq_extensions,
    to_dlq_event,
)
from .errors import (
    Classification,
    ErrorClass,
    FluxError,
    PermanentError,
    PoisonError,
    RetryableError,
    is_classified_error,
)
from .metrics import (
    DURATION_BUCKETS,
    NO_METRICS,
    ConnectionState,
    ConsumeOutcome,
    InMemoryMetrics,
    MetricsSink,
    NoMetrics,
    PublishOutcome,
)
from .protocol import (
    CONSUMER_DEFAULTS,
    DEFAULT_PENDING_POLL_MS,
    MAX_MESSAGE_BYTES,
    STREAM_DEFAULTS,
    SUBJECT_PATTERN,
    InvalidSubjectError,
    ParsedSubject,
    dlq_stream_name,
    dlq_subject,
    durable_name,
    is_dlq_subject,
    is_valid_subject,
    parse_subject,
    source_uri,
    stream_name,
    subject_to_type,
    uuid7,
)

# La firma es una extensión OPCIONAL y `cryptography` una dependencia opcional: el módulo
# se importa siempre —es barato— pero no toca `cryptography` hasta que alguien firma o
# verifica de verdad. Ver flux/signing.py.
from .tenant import TenantIsolation, TenantIsolationError, resolve_tenant_filter
from .signing import (
    KeyPair,
    Signer,
    SigningKeyError,
    SigningOptions,
    Verifier,
    VerificationMode,
    create_signer,
    create_verifier,
    generate_key_pair,
)

# Validación L3 — extensión opt-in y `jsonschema` una dependencia opcional: el módulo se
# importa siempre (es barato) pero no toca `jsonschema` hasta que alguien valida de
# verdad. Ver flux/validation.py.
from .validation import (
    SchemaBundle,
    SchemaNotFoundError,
    SchemaValidationError,
    ValidationMode,
    ValidationOptions,
    create_validator,
    load_bundle,
    schema_uri_for,
)

if TYPE_CHECKING:  # pragma: no cover - solo para los type checkers
    from .client import (
        ConnectOptions,
        ConsumerConfigMismatchError,
        Credentials,
        DlqInfo,
        FluxBus,
        Handler,
        HandlerContext,
        PoisonInfo,
        Subscription,
        connect,
    )

__version__ = "0.1.0"

_LAZY_CLIENT_EXPORTS = frozenset(
    {
        "connect",
        "FluxBus",
        "ConnectOptions",
        "Credentials",
        "ConsumerConfigMismatchError",
        "HandlerContext",
        "Handler",
        "Subscription",
        "PoisonInfo",
        "DlqInfo",
    }
)

__all__ = [
    # cliente
    "connect",
    "FluxBus",
    "ConnectOptions",
    "Credentials",
    "ConsumerConfigMismatchError",
    "TenantIsolationError",
    "TenantIsolation",
    "resolve_tenant_filter",
    "HandlerContext",
    "Handler",
    "Subscription",
    "PoisonInfo",
    "DlqInfo",
    # errores
    "ErrorClass",
    "FluxError",
    "RetryableError",
    "PermanentError",
    "PoisonError",
    "Classification",
    "is_classified_error",
    # clasificación
    "classify",
    "create_classifier",
    "ClassifierOptions",
    "RETRYABLE_HTTP_STATUS",
    "TRANSIENT_SYSCALL_CODES",
    # envelope
    "FluxEvent",
    "DataClassification",
    "DlqReason",
    "build_event",
    "parse_event",
    "serialize",
    "to_dlq_event",
    "strip_dlq_extensions",
    "EnvelopeError",
    "ALLOWED_ROOT_ATTRIBUTES",
    # protocolo
    "CONSUMER_DEFAULTS",
    "STREAM_DEFAULTS",
    "DEFAULT_PENDING_POLL_MS",
    "MAX_MESSAGE_BYTES",
    "SUBJECT_PATTERN",
    "ParsedSubject",
    "InvalidSubjectError",
    "parse_subject",
    "is_valid_subject",
    "subject_to_type",
    "stream_name",
    "dlq_stream_name",
    "durable_name",
    "dlq_subject",
    "is_dlq_subject",
    "source_uri",
    "uuid7",
    # multi-tenant (09-multitenancy.md)
    "TenantIsolation",
    "TenantIsolationError",
    "resolve_tenant_filter",
    # validación L3 (opt-in — 00-protocol.md §5)
    "ValidationMode",
    "ValidationOptions",
    "SchemaBundle",
    "SchemaValidationError",
    "SchemaNotFoundError",
    "create_validator",
    "load_bundle",
    "schema_uri_for",
    # firma (extensión opcional — 07-signing.md)
    "SigningOptions",
    "SigningKeyError",
    "VerificationMode",
    "Signer",
    "Verifier",
    "KeyPair",
    "create_signer",
    "create_verifier",
    "generate_key_pair",
    # métricas (08-observability.md)
    "MetricsSink",
    "NoMetrics",
    "NO_METRICS",
    "InMemoryMetrics",
    "DURATION_BUCKETS",
    "ConnectionState",
    "PublishOutcome",
    "ConsumeOutcome",
    # contexto
    "current_context",
    "use_context",
    "EventContext",
    "__version__",
]


def __getattr__(name: str) -> Any:
    if name in _LAZY_CLIENT_EXPORTS:
        from . import client

        return getattr(client, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
