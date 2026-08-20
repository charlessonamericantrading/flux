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
    #: Solo para RETRYABLE: sobrescribe el backoff canónico para este intento.
    #: Úsalo cuando la dependencia dice explícitamente cuánto esperar (Retry-After).
    retry_after_ms: float | None = None


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
