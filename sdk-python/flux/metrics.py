"""
Métricas del SDK.
Contrato normativo: specification/08-observability.md

Los nombres y las etiquetas son parte del **contrato**, no una decisión de cada SDK: si
el de Python y el de Go nombran distinto la tasa de DLQ, un panel del ecosistema es
imposible. Por eso viven aquí y no en la aplicación.

La implementación es un recolector en memoria **sin dependencias**. Quien use
`prometheus_client`, OpenTelemetry o lo que sea implementa `MetricsSink` contra su
backend y conserva los mismos nombres — que es lo único que el protocolo exige.
"""

from __future__ import annotations

import threading
from enum import IntEnum
from typing import Final, Literal, Mapping, Protocol

__all__ = [
    "DURATION_BUCKETS",
    "PublishOutcome",
    "ConsumeOutcome",
    "DlqReason",
    "ConnectionState",
    "MetricsSink",
    "NoMetrics",
    "NO_METRICS",
    "InMemoryMetrics",
]

#: Buckets del histograma, en segundos — 08-observability.md §3.
#:
#: El último es 30 **porque ES el `ack_wait`** (03-delivery.md §2). Un handler que cae en
#: el bucket superior está a punto de que su mensaje se reentregue mientras aún se
#: ejecuta, así que ese bucket mide directamente cuántos eventos rozan la ejecución
#: concurrente. DEBE moverse si se cambia `ack_wait`: un bucket que no coincide con el
#: plazo real mide algo que no le importa a nadie. Hay un test que lo vigila.
DURATION_BUCKETS: Final[tuple[float, ...]] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1,
    2.5,
    5,
    10,
    30,
)

PublishOutcome = Literal["ok", "invalid_schema", "error"]
ConsumeOutcome = Literal[
    "ok", "retryable", "permanent", "poison", "invalid_schema", "invalid_signature"
]
DlqReason = Literal["retryable", "permanent", "poison"]


class ConnectionState(IntEnum):
    """Valor de `flux_connection_state` — 08-observability.md §2.1."""

    DISCONNECTED = 0
    CONNECTED = 1
    RECONNECTING = 2


class MetricsSink(Protocol):
    """
    Destino de las métricas. Impleméntalo para enchufar el backend que uses.

    Las firmas fuerzan las etiquetas del protocolo: **no hay un `labels: dict` genérico**
    a propósito, porque es justo por ahí por donde se cuela un `tenantid` que multiplica
    las series temporales. Y la cardinalidad no avisa — el sistema funciona en desarrollo
    con tres tenants y muere en producción con diez mil (§2.2).
    """

    def event_published(self, subject: str, outcome: PublishOutcome) -> None: ...

    def event_consumed(self, subject: str, consumer: str, outcome: ConsumeOutcome) -> None: ...

    def handler_duration(self, subject: str, consumer: str, seconds: float) -> None: ...

    def event_dlq(self, subject: str, consumer: str, reason: DlqReason, code: str) -> None: ...

    def event_retried(self, subject: str, consumer: str, attempt: int) -> None: ...

    def consumer_pending(self, subject: str, consumer: str, pending: int) -> None: ...

    def connection_state(self, state: ConnectionState) -> None: ...


class NoMetrics:
    """No-op. El default: un SDK no debe imponer un backend de métricas."""

    def event_published(self, subject: str, outcome: PublishOutcome) -> None:
        pass

    def event_consumed(self, subject: str, consumer: str, outcome: ConsumeOutcome) -> None:
        pass

    def handler_duration(self, subject: str, consumer: str, seconds: float) -> None:
        pass

    def event_dlq(self, subject: str, consumer: str, reason: DlqReason, code: str) -> None:
        pass

    def event_retried(self, subject: str, consumer: str, attempt: int) -> None:
        pass

    def consumer_pending(self, subject: str, consumer: str, pending: int) -> None:
        pass

    def connection_state(self, state: ConnectionState) -> None:
        pass


NO_METRICS: Final[MetricsSink] = NoMetrics()


# ─── Recolector en memoria ───────────────────────────────────────────────────

_COUNTERS: Final = (
    "flux_events_published_total",
    "flux_events_consumed_total",
    "flux_events_dlq_total",
    "flux_events_retried_total",
)
_GAUGES: Final = ("flux_consumer_pending", "flux_connection_state")
_HISTOGRAM: Final = "flux_event_handler_duration_seconds"


def _escape(value: str) -> str:
    r"""
    Escapa un valor de etiqueta según el formato de exposición de Prometheus.

    No es cosmética: un `code` con comillas partiría la línea y Prometheus descartaría
    **el scrape entero**, no solo esa serie. Y el `code` viene de la clasificación de un
    error, que es exactamente el sitio donde alguien acabará metiendo un mensaje con
    comillas dentro.
    """
    return value.replace("\\", r"\\").replace('"', r"\"").replace("\n", r"\n")


def _key(name: str, labels: Mapping[str, object]) -> str:
    if not labels:
        # Sin llaves vacías: `flux_connection_state{} 1` no es formato válido.
        return name
    pares = ",".join(f'{k}="{_escape(str(v))}"' for k, v in sorted(labels.items()))
    return f"{name}{{{pares}}}"


def _fmt(value: float) -> str:
    """
    Formato decimal SIN exponente.

    `str(1e-05)` produce `1e-05`, que el parser de Prometheus rechaza en una duración
    acumulada pequeña. Es el tipo de fallo que solo aparece con tráfico real.
    """
    if float(value).is_integer() and abs(value) < 1e15:
        return str(int(value))
    return f"{value:.9f}".rstrip("0").rstrip(".")


class InMemoryMetrics:
    """
    Recolector sin dependencias que expone el formato de texto de Prometheus.

    Suficiente para servir un `/metrics` real. Si ya usas `prometheus_client`, implementa
    `MetricsSink` contra él en vez de esto — lo que importa es conservar nombres y
    etiquetas.

    Es seguro desde varios hilos: `render()` puede servirse desde un hilo de HTTP
    mientras el bucle de consumo sigue registrando.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, tuple[list[int], float, int]] = {}

    # ─── registro ─────────────────────────────────────────────────────────────

    def _inc(self, name: str, labels: Mapping[str, object], by: float = 1) -> None:
        key = _key(name, labels)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + by

    def _set(self, name: str, labels: Mapping[str, object], value: float) -> None:
        with self._lock:
            self._gauges[_key(name, labels)] = value

    def _observe(self, name: str, labels: Mapping[str, object], value: float) -> None:
        key = _key(name, labels)
        with self._lock:
            buckets, total, count = self._histograms.get(
                key, ([0] * len(DURATION_BUCKETS), 0.0, 0)
            )
            for i, limit in enumerate(DURATION_BUCKETS):
                if value <= limit:
                    buckets[i] += 1
            self._histograms[key] = (buckets, total + value, count + 1)

    def event_published(self, subject: str, outcome: PublishOutcome) -> None:
        self._inc("flux_events_published_total", {"subject": subject, "outcome": outcome})

    def event_consumed(self, subject: str, consumer: str, outcome: ConsumeOutcome) -> None:
        self._inc(
            "flux_events_consumed_total",
            {"subject": subject, "consumer": consumer, "outcome": outcome},
        )

    def handler_duration(self, subject: str, consumer: str, seconds: float) -> None:
        self._observe(_HISTOGRAM, {"subject": subject, "consumer": consumer}, seconds)

    def event_dlq(self, subject: str, consumer: str, reason: DlqReason, code: str) -> None:
        # `code` es un identificador estable, nunca el mensaje: un mensaje lleva ids,
        # timestamps y rutas, y su cardinalidad infinita tumba el almacenamiento — §2.2.
        self._inc(
            "flux_events_dlq_total",
            {"subject": subject, "consumer": consumer, "reason": reason, "code": code},
        )

    def event_retried(self, subject: str, consumer: str, attempt: int) -> None:
        self._inc(
            "flux_events_retried_total",
            {"subject": subject, "consumer": consumer, "attempt": attempt},
        )

    def consumer_pending(self, subject: str, consumer: str, pending: int) -> None:
        self._set("flux_consumer_pending", {"subject": subject, "consumer": consumer}, pending)

    def connection_state(self, state: ConnectionState) -> None:
        self._set("flux_connection_state", {}, int(state))

    # ─── exposición ───────────────────────────────────────────────────────────

    def render(self) -> str:
        """Formato de exposición de Prometheus. Sírvelo en `/metrics`."""
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            histograms = dict(self._histograms)

        out: list[str] = []

        for kind, names, series in (
            ("counter", _COUNTERS, counters),
            ("gauge", _GAUGES, gauges),
        ):
            for name in names:
                muestras = sorted((k, v) for k, v in series.items() if _base(k) == name)
                if not muestras:
                    continue
                out.append(f"# TYPE {name} {kind}")
                out.extend(f"{k} {_fmt(v)}" for k, v in muestras)

        if histograms:
            out.append(f"# TYPE {_HISTOGRAM} histogram")
            for key, (buckets, total, count) in sorted(histograms.items()):
                labels = key[len(_HISTOGRAM) + 1 : -1] if "{" in key else ""
                sep = "," if labels else ""
                for limit, acumulado in zip(DURATION_BUCKETS, buckets):
                    out.append(
                        f'{_HISTOGRAM}_bucket{{{labels}{sep}le="{_fmt(limit)}"}} {acumulado}'
                    )
                out.append(f'{_HISTOGRAM}_bucket{{{labels}{sep}le="+Inf"}} {count}')
                sufijo = f"{{{labels}}}" if labels else ""
                out.append(f"{_HISTOGRAM}_sum{sufijo} {_fmt(total)}")
                out.append(f"{_HISTOGRAM}_count{sufijo} {count}")

        return "\n".join(out) + "\n" if out else ""

    def snapshot(self) -> dict[str, dict[str, float]]:
        """Solo para tests."""
        with self._lock:
            return {"counters": dict(self._counters), "gauges": dict(self._gauges)}


def _base(key: str) -> str:
    return key.split("{", 1)[0]
