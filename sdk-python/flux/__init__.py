"""
flux SDK para Python — Event Protocol v1, nivel de conformidad L2.

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
from .protocol import (
    CONSUMER_DEFAULTS,
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
