"""
Taxonomía de errores de flux.
Contrato normativo: specification/04-errors.md
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

__all__ = [
    "ErrorClass",
    "Classification",
    "FluxError",
    "RetryableError",
    "PermanentError",
    "PoisonError",
    "is_classified_error",
]


class ErrorClass(str, Enum):
    """
    Las tres clases del protocolo. Determinan la acción sobre el mensaje NATS.

    Hereda de `str` para que el valor sirva directamente como `dlqreason` del envelope
    sin conversiones intermedias.
    """

    #: Fallo del entorno, podría desaparecer solo. → nak(delay), reintenta con backoff.
    RETRYABLE = "retryable"
    #: Evento válido que este consumidor nunca podrá procesar. → term() + DLQ, sin reintentos.
    PERMANENT = "permanent"
    #: El mensaje ni siquiera es interpretable. → term() + DLQ + alerta inmediata.
    POISON = "poison"


@dataclass(frozen=True)
class Classification:
    """
    Resultado de clasificar un error. Es lo que el runtime del consumidor consume.

    `error_class` se llama así y no `class` porque `class` es palabra reservada en
    Python; es el único renombrado respecto al SDK de Node.
    """

    error_class: ErrorClass
    #: Código estable para métricas y alertas. Ej. "HTTP_503", "PEDIDO_YA_CANCELADO".
    code: str
    #: Solo para RETRYABLE: **sugerencia para el PRIMER reintento**, no un control del
    #: calendario completo.
    #:
    #: ⚠️ Con `backoff` configurado —y flux lo configura siempre— JetStream honra el delay
    #: de un `nak` únicamente en la primera reentrega; a partir de la segunda manda el
    #: array `backoff` y el delay se ignora **sin ningún aviso**. Medido contra NATS
    #: 2.14.5, ver 03-delivery.md §2.2::
    #:
    #:     SIN backoff:  0ms → 300ms → 600ms → 900ms      ← el delay se honra siempre
    #:     CON backoff:  0ms → 300ms → 5300ms → 15300ms   ← solo la primera vez
    #:
    #: Consecuencia práctica: un `Retry-After: 5` de un proveedor acorta el primer
    #: reintento y nada más; los siguientes siguen el backoff canónico (1 m, 5 m, 15 m,
    #: 30 m). No construyas lógica que dependa de que se respete después.
    retry_after_ms: float | None = None
    #: Solo para RETRYABLE: número máximo de entregas para ESTE error, por debajo del
    #: `max_deliver` del consumidor. `None` = sin tope propio, manda el del consumidor.
    #:
    #: Existe porque `max_deliver` es por consumidor, no por mensaje: bajarlo a 2 para
    #: acotar los errores desconocidos recortaría también los reintentos de los que sí
    #: sabemos transitorios (ECONNRESET, HTTP 503), que deben conservar sus 6 intentos.
    #: Ver 04-errors.md §2.1.
    max_attempts: int | None = None


class FluxError(Exception):
    """Base de los errores que ya declaran su propia clase de protocolo."""

    #: La clase de protocolo que esta excepción declara. La rellena cada subclase.
    flux_class: ErrorClass

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        cause: BaseException | None = None,
        **_extra: Any,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        if cause is not None:
            # Equivale a `new Error(msg, { cause })` de Node. Se asigna en vez de usar
            # `raise ... from ...` para que el productor del error pueda construirlo
            # sin estar dentro de un `except`.
            self.__cause__ = cause


class RetryableError(FluxError):
    """
    La aplicación lanza esto para forzar reintento.
    Úsalo cuando SABES que el fallo es transitorio.

    `retry_after_ms` es una **sugerencia para el primer reintento**: a partir de la
    segunda reentrega manda el backoff del consumidor. Ver `Classification.retry_after_ms`.
    """

    flux_class = ErrorClass.RETRYABLE

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        retry_after_ms: float | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message, code=code, cause=cause)
        self.retry_after_ms = retry_after_ms


class PermanentError(FluxError):
    """
    La aplicación lanza esto para ir directo a la DLQ sin gastar reintentos.
    Úsalo cuando el evento es válido pero tu lógica lo rechaza definitivamente.
    """

    flux_class = ErrorClass.PERMANENT


class PoisonError(FluxError):
    """
    Lo lanza el SDK, no la aplicación: el mensaje no pudo parsearse como CloudEvent.
    Nunca llega al handler.
    """

    flux_class = ErrorClass.POISON


def is_classified_error(e: object) -> bool:
    """¿Es un error de flux que ya declara su propia clase?"""
    return isinstance(e, FluxError)
