"""
Constantes y derivaciones del protocolo.
Contrato normativo: specification/02-naming.md, specification/03-delivery.md

Todo lo de aquí sale de protocol.json. Si divergen, protocol.json manda —
es lo que consumen los demás SDKs y los agentes.
"""

from __future__ import annotations

import re
import secrets
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Final

__all__ = [
    "CONSUMER_DEFAULTS",
    "STREAM_DEFAULTS",
    "MAX_MESSAGE_BYTES",
    "SUBJECT_PATTERN",
    "ParsedSubject",
    "InvalidSubjectError",
    "seconds",
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
]


@dataclass(frozen=True)
class _ConsumerDefaults:
    """Configuración canónica de consumidor. Ver 03-delivery.md §2."""

    ack_policy: str
    #: ⚠️ ack_wait DEBE ser igual a backoff_ms[0]: JetStream lo sobrescribe con ese
    #: valor sin devolver error. Ver 03-delivery.md §2.1 y
    #: conformance/cases/consumer-config.json.
    ack_wait_ms: int
    max_deliver: int
    backoff_ms: tuple[int, ...]
    max_ack_pending: int


CONSUMER_DEFAULTS: Final = _ConsumerDefaults(
    ack_policy="explicit",
    ack_wait_ms=30_000,
    max_deliver=6,
    # 5 entradas + 1 entrega inicial = max_deliver 6. Si no cuadran, la última entrada
    # no se aplica nunca y la config miente sobre su propio comportamiento.
    backoff_ms=(30_000, 60_000, 300_000, 900_000, 1_800_000),
    max_ack_pending=256,
)


@dataclass(frozen=True)
class _StreamDefaults:
    max_age_ms: int
    dlq_max_age_ms: int
    duplicate_window_ms: int


STREAM_DEFAULTS: Final = _StreamDefaults(
    max_age_ms=30 * 24 * 60 * 60 * 1000,
    # Más larga que la del stream principal: la DLQ es material forense (02-naming.md §3.2).
    dlq_max_age_ms=90 * 24 * 60 * 60 * 1000,
    duplicate_window_ms=120_000,
)

MAX_MESSAGE_BYTES: Final = 1_048_576


def seconds(ms: float) -> float:
    """
    Milisegundos → segundos.

    Diferencia con el SDK de Node: allí las duraciones se envían al broker en
    nanosegundos (`nanos()`), porque el cliente JS pasa la config tal cual. `nats-py`
    hace la conversión a nanosegundos él mismo dentro de `ConsumerConfig.as_dict()`,
    así que aquí hay que entregarle **segundos**. Pasarle nanosegundos no da error:
    produce un `ack_wait` de 950 años, en silencio.
    """
    return ms / 1000.0


# ─── Naming ──────────────────────────────────────────────────────────────────

#: `<dominio>.<agregado>.v<major>.<evento>` — 02-naming.md §1
SUBJECT_PATTERN: Final = re.compile(
    r"^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]*\.[a-z0-9]+(-[a-z0-9]+)*$"
)


@dataclass(frozen=True)
class ParsedSubject:
    domain: str
    aggregate: str
    major: int
    event: str


class InvalidSubjectError(ValueError):
    def __init__(self, subject: str, reason: str) -> None:
        super().__init__(f'subject inválido "{subject}": {reason}')
        self.subject = subject
        self.reason = reason


def parse_subject(subject: str) -> ParsedSubject:
    """Valida y descompone un subject de NATS. Lanza `InvalidSubjectError` si no cumple."""
    if subject != subject.lower():
        # Se comprueba antes que el patrón para dar un mensaje útil: los subjects de NATS
        # son case-sensitive y una mayúscula crea un subject fantasma SIN ningún error.
        raise InvalidSubjectError(
            subject,
            "debe ir todo en minúsculas — NATS es case-sensitive y una mayúscula crea "
            "un subject al que nadie está suscrito, sin producir error",
        )
    if not SUBJECT_PATTERN.match(subject):
        n = len(subject.split("."))
        raise InvalidSubjectError(
            subject,
            (
                f"debe tener exactamente 4 tokens "
                f"(<dominio>.<agregado>.v<major>.<evento>), tiene {n}"
            )
            if n != 4
            else "formato esperado <dominio>.<agregado>.v<major>.<evento> en kebab-case",
        )
    domain, aggregate, v, event = subject.split(".")
    return ParsedSubject(domain=domain, aggregate=aggregate, major=int(v[1:]), event=event)


def is_valid_subject(subject: str) -> bool:
    try:
        parse_subject(subject)
        return True
    except InvalidSubjectError:
        return False


def subject_to_type(subject: str) -> str:
    """`pedidos.pedido.v1.creado` → `com.flux.pedidos.pedido.creado.v1` — 02-naming.md §2"""
    p = parse_subject(subject)
    return f"com.flux.{p.domain}.{p.aggregate}.{p.event}.v{p.major}"


def stream_name(domain: str) -> str:
    """`EVT_PEDIDOS`. NATS no admite puntos en nombres de stream — 02-naming.md §3."""
    return f"EVT_{domain.replace('-', '_').upper()}"


def dlq_stream_name(domain: str) -> str:
    return f"DLQ_{domain.replace('-', '_').upper()}"


def durable_name(service: str, subject: str) -> str:
    """
    `facturacion-api__pedidos_pedido_v1_creado`.
    NATS rechaza `.`, `*` y `>` en nombres de durable — 02-naming.md §4.
    Separar el servicio con `__` mantiene la reversibilidad: partiendo por `__` se
    recuperan servicio y subject exactos.
    """
    parse_subject(subject)
    return f"{service}__{subject.replace('.', '_').replace('-', '_')}"


def dlq_subject(subject: str) -> str:
    """
    `dlq.pedidos.pedido.v1.creado`.
    PREFIJO, no sufijo: un sufijo encajaría con `<dominio>.>` y el stream principal
    capturaría sus propios muertos — 02-naming.md §3.1.
    """
    return f"dlq.{subject}"


def is_dlq_subject(subject: str) -> bool:
    return subject.startswith("dlq.")


def source_uri(environment: str, service: str) -> str:
    """`/produccion/pedidos-api` — 01-envelope.md §2"""
    return f"/{environment}/{service}"


# ─── Identidad ───────────────────────────────────────────────────────────────

_uuid7_lock = threading.Lock()
_uuid7_last_ms = -1
_uuid7_counter = 0


def uuid7() -> str:
    """
    UUIDv7 canónico (RFC 9562) — 01-envelope.md §2.

    Python < 3.14 no trae `uuid.uuid7()`, así que se implementa aquí en vez de añadir
    una dependencia por 15 líneas.

    Los 12 bits de `rand_a` se usan como contador dentro del mismo milisegundo, igual
    que hace el paquete `uuid` que usa el SDK de Node. No es decorativo: 01-envelope.md
    §2.2 se apoya en que ordenar por `id` dentro de un mismo `source` equivale a
    ordenar por instante de generación, y sin contador dos eventos del mismo
    milisegundo quedan en orden aleatorio.
    """
    global _uuid7_last_ms, _uuid7_counter

    with _uuid7_lock:
        ms = time.time_ns() // 1_000_000
        if ms > _uuid7_last_ms:
            _uuid7_last_ms = ms
            # Arranque aleatorio en la mitad baja del contador: deja ~2048 ids
            # monotónicos por milisegundo antes de tener que avanzar el reloj.
            _uuid7_counter = secrets.randbits(11)
        else:
            # Mismo milisegundo, o el reloj del sistema ha ido hacia atrás (NTP).
            # Seguimos incrementando para no romper la monotonía.
            _uuid7_counter += 1
            if _uuid7_counter > 0xFFF:
                _uuid7_last_ms += 1
                _uuid7_counter = secrets.randbits(11)
            ms = _uuid7_last_ms
        rand_a = _uuid7_counter

    rand_b = secrets.randbits(62)
    value = (
        ((ms & 0xFFFF_FFFF_FFFF) << 80)  # unix_ts_ms
        | (0x7 << 76)  # version
        | (rand_a << 64)  # rand_a (contador)
        | (0b10 << 62)  # variant RFC 4122
        | rand_b  # rand_b
    )
    return str(uuid.UUID(int=value))
