"""
Propagación automática de contexto entre eventos.
Contrato normativo: specification/01-envelope.md §5

Si un desarrollador tiene que rellenar `correlationid` a mano, el SDK ha fallado en su
único trabajo. Donde el SDK de Node usa `AsyncLocalStorage`, aquí se usa
`contextvars.ContextVar`: es el mecanismo equivalente y con la misma propiedad clave —
un `publish()` en cualquier punto de la pila de un handler hereda el contexto del
evento entrante sin que nadie tenga que pasarlo por parámetro.

Diferencia real, y es la que importa: una `ContextVar` se copia al **crear** una tarea
(`asyncio.create_task`), no al ejecutarla. Una tarea lanzada desde dentro del handler
hereda el contexto; una tarea de fondo creada antes, no. Es exactamente la misma
semántica que `AsyncLocalStorage`, pero conviene tenerlo presente si el handler delega
trabajo a un worker de larga vida: ese worker publicará **sin** correlación.
"""

from __future__ import annotations

import importlib
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator

__all__ = [
    "EventContext",
    "current_context",
    "use_context",
    "context_from_event",
    "active_traceparent",
]


@dataclass(frozen=True)
class EventContext:
    correlationid: str
    #: `id` del evento en curso — pasa a ser el `causationid` de lo que se publique.
    causationid: str
    tenantid: str
    traceparent: str | None = None
    tracestate: str | None = None


_storage: ContextVar[EventContext | None] = ContextVar("flux_event_context", default=None)


def current_context() -> EventContext | None:
    """Contexto del evento que se está procesando ahora mismo, si lo hay."""
    return _storage.get()


@contextmanager
def use_context(ctx: EventContext) -> Iterator[EventContext]:
    """
    Equivalente a `runWithContext` de Node. Se expone como context manager en vez de
    como función de orden superior porque en Python eso permite envolver tanto un
    handler síncrono como uno asíncrono sin duplicar la API.
    """
    token = _storage.set(ctx)
    try:
        yield ctx
    finally:
        _storage.reset(token)


def context_from_event(event: Any) -> EventContext:
    return EventContext(
        correlationid=event.correlationid,
        # El `id` del evento entrante es la CAUSA del saliente: correlationid responde
        # "¿de qué flujo forma parte?" y causationid "¿quién lo causó exactamente?".
        causationid=event.id,
        tenantid=event.tenantid,
        traceparent=event.traceparent,
        tracestate=event.tracestate,
    )


def active_traceparent() -> str | None:
    """
    Lee `traceparent` de un span de OpenTelemetry activo, si lo hay.

    Se resuelve por importación diferida y con fallo silencioso a propósito: flux no
    depende de OpenTelemetry, pero lo aprovecha cuando está presente. Una dependencia
    dura obligaría a instalarlo a todo servicio que use el SDK.

    A diferencia del SDK de Node, aquí es síncrona: `importlib` no necesita `await`.
    """
    try:
        trace = importlib.import_module("opentelemetry.trace")
    except Exception:
        return None
    try:
        span_context = trace.get_current_span().get_span_context()
        if span_context is None or not span_context.is_valid:
            return None
        return (
            f"00-{span_context.trace_id:032x}-{span_context.span_id:016x}"
            f"-{int(span_context.trace_flags):02x}"
        )
    except Exception:
        return None
