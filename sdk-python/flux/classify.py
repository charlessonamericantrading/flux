"""
Clasificación de errores del handler.
Contrato normativo: specification/04-errors.md §2

Este fichero es el punto donde el protocolo se encuentra con la realidad operativa del
ecosistema. Todo lo demás en el SDK es mecánica; esto es política — y por eso la
política es un parámetro, no una constante.
"""

from __future__ import annotations

import asyncio
import errno as _errno
import socket
from dataclasses import dataclass, field
from typing import Any, Callable, Final, Literal, Mapping, Sequence

from .errors import Classification, ErrorClass, FluxError, RetryableError

__all__ = [
    "TRANSIENT_SYSCALL_CODES",
    "RETRYABLE_HTTP_STATUS",
    "ClassifierOptions",
    "create_classifier",
    "classify",
    "extract_http_status",
    "extract_retry_after_ms",
    "extract_syscall_code",
]

#: Códigos de sistema inequívocamente transitorios. Se conservan los nombres POSIX
#: (los mismos que usa `errno.errorcode`) para que la taxonomía sea idéntica en los dos
#: SDKs: un `ECONNRESET` debe producir el mismo `code` en las métricas venga de Node o
#: de Python.
TRANSIENT_SYSCALL_CODES: Final[frozenset[str]] = frozenset(
    {
        "ECONNRESET",
        "ECONNREFUSED",
        "ETIMEDOUT",
        "EPIPE",
        "EHOSTUNREACH",
        "ENETUNREACH",
        "EAI_AGAIN",  # fallo temporal de DNS
    }
)

#: Status HTTP que merecen reintento. Nótese qué NO está aquí: 400, 403, 404 y 422 son
#: PERMANENT — reintentarlos es gastar 51 minutos para obtener exactamente la misma
#: respuesta.
RETRYABLE_HTTP_STATUS: Final[frozenset[int]] = frozenset({429, 502, 503, 504})


def _int_attr(obj: Any, name: str) -> int | None:
    value = getattr(obj, name, None)
    # `bool` es subclase de `int` en Python: un `status=True` no es un status.
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def extract_http_status(e: object) -> int | None:
    """
    Busca el status HTTP donde lo dejan los clientes habituales: `aiohttp` lo pone en
    `.status`, `httpx` y `requests` en `.response.status_code`.
    """
    for attr in ("status", "status_code", "statusCode"):
        status = _int_attr(e, attr)
        if status is not None:
            return status
    response = getattr(e, "response", None)
    if response is not None:
        for attr in ("status_code", "status"):
            status = _int_attr(response, attr)
            if status is not None:
                return status
    return None


def extract_retry_after_ms(e: object) -> float | None:
    """Lee `Retry-After` (segundos) si la dependencia lo expone."""
    headers = getattr(getattr(e, "response", None), "headers", None)
    if headers is None:
        headers = getattr(e, "headers", None)
    if not isinstance(headers, Mapping):
        return None
    raw = headers.get("retry-after")
    if raw is None:
        raw = headers.get("Retry-After")
    try:
        seconds = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        # `Retry-After` admite también una fecha HTTP. Interpretarla mal daría un
        # retraso absurdo, así que se ignora y manda el backoff canónico.
        return None
    return seconds * 1000 if seconds > 0 else None


def extract_syscall_code(e: object) -> str | None:
    """
    Nombre POSIX del error de sistema, si lo hay.

    Solo se traduce `errno` cuando el sistema operativo lo ha rellenado de verdad. Es
    deliberado: en Python `asyncio.TimeoutError` **es** `TimeoutError`, que a su vez es
    un `OSError` sin `errno`. Si se clasificara por clase en vez de por `errno`, todo
    timeout de `asyncio.wait_for` se convertiría en `ETIMEDOUT` y `timeout_policy`
    dejaría de tener efecto.
    """
    if isinstance(e, socket.gaierror):
        # `gaierror.errno` usa la tabla EAI_*, no la de `errno`: hay que traducirla
        # aparte. Solo EAI_AGAIN es transitorio; un EAI_NONAME (el host no existe) no
        # mejora esperando y cae en la política de lo desconocido.
        eai_again = getattr(socket, "EAI_AGAIN", None)
        return "EAI_AGAIN" if eai_again is not None and e.errno == eai_again else "EAI_NONAME"
    if isinstance(e, OSError) and e.errno is not None:
        name = _errno.errorcode.get(e.errno)
        # En Windows, `errno.errorcode[10054]` es `WSAECONNRESET`, no `ECONNRESET`. Sin
        # normalizar el prefijo, el mismo corte de conexión sería RETRYABLE en Linux y
        # PERMANENT en Windows — y las métricas del ecosistema tendrían dos códigos para
        # el mismo fallo.
        if name is not None and name.startswith("WSA"):
            name = name[3:]
        return name
    code = getattr(e, "code", None)
    return code if isinstance(code, str) else None


def _is_timeout(e: object) -> bool:
    if isinstance(e, (asyncio.TimeoutError, TimeoutError)):
        return True
    # `httpx.ReadTimeout`, `requests.Timeout`, `nats.errors.TimeoutError`… no comparten
    # jerarquía; el nombre de la clase es la única señal común.
    return "timeout" in type(e).__name__.lower()


# ─────────────────────────────────────────────────────────────────────────────

Rule = Callable[[BaseException], Classification | None]


@dataclass(frozen=True)
class ClassifierOptions:
    #: Qué hacer con un error que no encaja en ninguna regla conocida.
    #:
    #: `"retryable-bounded"` (default de la spec) — reintenta, pero con un presupuesto
    #: reducido (`unknown_retry_budget`, 2 entregas) en vez de los 6 intentos completos.
    #: Un transitorio se recupera en el segundo intento; un sistemático llega a la DLQ
    #: en ~30 s sin atascar la cola. Ver 04-errors.md §2.1.
    #:
    #: `"permanent"` — va a la DLQ sin gastar reintentos. Falla rápido, pero un hipo de
    #: red manda a la DLQ un evento perfectamente válido y alguien lo reproduce a mano.
    #:
    #: `"retryable"` — reintenta con el backoff completo, 51 minutos. Elegir esto solo si
    #: vuestras dependencias internas tienen hipos frecuentes y podéis asumir que un modo
    #: de fallo nuevo atasque la cola y se amplifique con cada mensaje siguiente.
    unknown_error_policy: Literal["permanent", "retryable", "retryable-bounded"] = (
        "retryable-bounded"
    )

    #: Entregas máximas para un error desconocido cuando la política es
    #: `"retryable-bounded"`. Incluye la primera entrega, así que 2 = un reintento.
    unknown_retry_budget: int = 2

    #: Un timeout, ¿es "el mundo va lento" o "esta operación no cabe en la ventana"?
    #:
    #: El default es `"retryable"`: un timeout suele indicar saturación transitoria. Si
    #: vuestros timeouts son casi siempre consultas que nunca van a terminar,
    #: `"permanent"` evita reintentar lo imposible.
    timeout_policy: Literal["permanent", "retryable"] = "retryable"

    #: Reglas propias, evaluadas antes que todo lo demás. Devuelven `None` para ceder.
    rules: Sequence[Rule] = field(default_factory=tuple)


def create_classifier(
    options: ClassifierOptions | None = None,
) -> Callable[[BaseException], Classification]:
    """
    Construye un clasificador. El runtime del consumidor usa el resultado así:

        RETRYABLE  → msg.nak(retry_after_ms ?? backoff canónico)
        PERMANENT  → msg.term() + publicar en dlq.<subject> con dlqattempts = 1
        POISON     → msg.term() + publicar en dlq.<subject> + alerta inmediata

    El orden de evaluación es deliberado: lo más específico primero, y el default al
    final. Esa última línea es la decisión de política de verdad.
    """
    opts = options or ClassifierOptions()
    unknown_policy = opts.unknown_error_policy
    unknown_class = ErrorClass.PERMANENT if unknown_policy == "permanent" else ErrorClass.RETRYABLE
    # Solo la política acotada impone un tope propio; las otras dos dejan mandar al
    # `max_deliver` del consumidor.
    unknown_budget = opts.unknown_retry_budget if unknown_policy == "retryable-bounded" else None
    timeout_class = (
        ErrorClass.PERMANENT if opts.timeout_policy == "permanent" else ErrorClass.RETRYABLE
    )
    rules = tuple(opts.rules)

    def classify(e: BaseException) -> Classification:
        # 1. Una excepción tipada de flux siempre gana: la aplicación sabe más que el SDK.
        if isinstance(e, FluxError):
            return Classification(
                error_class=e.flux_class,
                code=e.code or type(e).__name__,
                retry_after_ms=e.retry_after_ms if isinstance(e, RetryableError) else None,
            )

        # 2. Reglas de la aplicación.
        for rule in rules:
            result = rule(e)
            if result is not None:
                return result

        # 3. Status HTTP: la señal más fiable que da una dependencia.
        status = extract_http_status(e)
        if status is not None:
            retryable = status in RETRYABLE_HTTP_STATUS
            return Classification(
                error_class=ErrorClass.RETRYABLE if retryable else ErrorClass.PERMANENT,
                code=f"HTTP_{status}",
                retry_after_ms=extract_retry_after_ms(e) if retryable else None,
            )

        # 4. Errores de sistema: red y DNS son transitorios por definición.
        syscall = extract_syscall_code(e)
        if syscall is not None and syscall in TRANSIENT_SYSCALL_CODES:
            return Classification(error_class=ErrorClass.RETRYABLE, code=syscall)

        # 5. Timeouts — política configurable.
        if _is_timeout(e):
            return Classification(error_class=timeout_class, code="TIMEOUT")

        # 6. Lo desconocido. Aquí es donde se decide el comportamiento del ecosistema
        #    ante lo que nadie previó. El default acotado da al transitorio una segunda
        #    oportunidad sin regalarle 51 minutos de cola al sistemático — y el tope va
        #    en la clasificación, no en `max_deliver`, para no recortar los reintentos
        #    de los RETRYABLE reconocidos — 04-errors.md §2.1.
        return Classification(
            error_class=unknown_class,
            code=syscall or "UNKNOWN",
            max_attempts=unknown_budget,
        )

    return classify


#: Clasificador con los defaults de la spec.
classify = create_classifier()
