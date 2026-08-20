"""
Envelope CloudEvents 1.0 — construcción, serialización y parseo.
Contrato normativo: specification/01-envelope.md
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Final, Generic, Literal, Mapping, TypeVar

from .errors import PoisonError
from .protocol import MAX_MESSAGE_BYTES, subject_to_type

__all__ = [
    "DataClassification",
    "DlqReason",
    "FluxEvent",
    "EnvelopeError",
    "ALLOWED_ROOT_ATTRIBUTES",
    "REQUIRED_ATTRIBUTES",
    "REQUIRED_EXTENSIONS",
    "STRING_ATTRIBUTES",
    "VALID_CLASSIFICATIONS",
    "build_event",
    "serialize",
    "parse_event",
    "to_dlq_event",
    "strip_dlq_extensions",
    "iso_utc",
    "now_iso",
]

DataClassification = Literal["public", "internal", "confidential", "restricted"]
DlqReason = Literal["retryable", "permanent", "poison"]

T = TypeVar("T")


def iso_utc(value: datetime | str) -> str:
    """
    RFC 3339 en UTC con precisión de **milisegundos** — 01-envelope.md §2.

    `datetime.isoformat()` no vale tal cual: emite microsegundos y `+00:00` en vez de
    `Z`, así que dos SDKs del mismo ecosistema producirían `time` distintos para el
    mismo instante y cualquier comparación textual entre ellos fallaría.
    """
    if isinstance(value, str):
        return value
    dt = value.astimezone(timezone.utc)
    return f"{dt:%Y-%m-%dT%H:%M:%S}.{dt.microsecond // 1000:03d}Z"


def now_iso() -> str:
    return iso_utc(datetime.now(timezone.utc))


@dataclass(frozen=True)
class FluxEvent(Generic[T]):
    """
    Un evento de flux.

    Los nombres de las extensiones van en minúscula pegada porque CloudEvents restringe
    los nombres de extensión a `[a-z0-9]` sin separadores — no es estilo, es la
    especificación (01-envelope.md §3). Por eso son la única excepción al `snake_case`
    del resto del SDK: renombrarlas a `correlation_id` rompería el envelope en el cable.

    Es inmutable (`frozen`) por lo mismo que en Node es `readonly`: un evento es un
    hecho consumado del pasado (00-protocol.md §4). Para derivar uno modificado se usa
    `dataclasses.replace`.
    """

    id: str
    source: str
    type: str
    time: str
    dataschema: str
    correlationid: str
    tenantid: str
    producerversion: str
    dataclassification: DataClassification
    data: T

    specversion: Literal["1.0"] = "1.0"
    datacontenttype: Literal["application/json"] = "application/json"

    #: ⚠️ Id del AGREGADO, no el subject de NATS. Ver 01-envelope.md §2.1.
    subject: str | None = None

    causationid: str | None = None
    partitionkey: str | None = None
    traceparent: str | None = None
    tracestate: str | None = None

    #: Firma Ed25519 — extensión OPCIONAL, 07-signing.md §4. Un evento sin ellas sigue
    #: siendo válido: el default de la verificación es `off`.
    #:
    #: `signkeyid` va DENTRO de lo firmado y `signature` no (no puede firmarse a sí
    #: misma). Van declaradas ANTES de las `dlq*` porque las `dlq*` se añaden después de
    #: firmar, y el orden de claves es normativo (01-envelope.md §6): así el mismo evento
    #: firmado produce los mismos bytes en Node, Python y Go.
    signkeyid: str | None = None
    signature: str | None = None

    dlqreason: DlqReason | None = None
    dlqattempts: int | None = None
    dlqconsumer: str | None = None
    dlqerror: str | None = None
    dlqtime: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """
        Vuelca el evento a un dict listo para JSON.

        Los atributos ausentes se **omiten**, no se emiten como `null`: `null` para
        significar "ausente" está prohibido (01-envelope.md §4) y un `causationid: null`
        en el cable obligaría a cada consumidor a distinguir los dos casos.
        El orden reproduce el del ejemplo normativo de 01-envelope.md §4.
        """
        out: dict[str, Any] = {
            "specversion": self.specversion,
            "id": self.id,
            "source": self.source,
            "type": self.type,
            "time": self.time,
            "datacontenttype": self.datacontenttype,
            "dataschema": self.dataschema,
        }
        if self.subject is not None:
            out["subject"] = self.subject
        out["correlationid"] = self.correlationid
        out["tenantid"] = self.tenantid
        out["producerversion"] = self.producerversion
        out["dataclassification"] = self.dataclassification
        for name in (
            "causationid",
            "partitionkey",
            "traceparent",
            "tracestate",
            "signkeyid",
            "signature",
            "dlqreason",
            "dlqattempts",
            "dlqconsumer",
            "dlqerror",
            "dlqtime",
        ):
            value = getattr(self, name)
            if value is not None:
                out[name] = value
        out["data"] = self.data
        return out


#: Atributos raíz permitidos. Todo lo demás va dentro de `data` — 01-envelope.md §3.3.
ALLOWED_ROOT_ATTRIBUTES: Final[frozenset[str]] = frozenset(
    {
        "specversion",
        "id",
        "source",
        "type",
        "time",
        "datacontenttype",
        "dataschema",
        "subject",
        "correlationid",
        "tenantid",
        "producerversion",
        "dataclassification",
        "causationid",
        "partitionkey",
        "traceparent",
        "tracestate",
        "signkeyid",
        "signature",
        "dlqreason",
        "dlqattempts",
        "dlqconsumer",
        "dlqerror",
        "dlqtime",
        "data",
    }
)

REQUIRED_ATTRIBUTES: Final[tuple[str, ...]] = (
    "specversion",
    "id",
    "source",
    "type",
    "time",
    "datacontenttype",
    "dataschema",
    "data",
)

#: Extensiones del perfil flux cuya ausencia es POISON — 01-envelope.md §3.1.
#:
#: Se exigen de verdad, no solo en la prosa. Si no se exigieran no serían obligatorias,
#: serían recomendadas, y cada consumidor tendría que tratar el caso ausente — que es
#: exactamente lo que declararlas obligatorias pretendía evitar.
#:
#: Además, asumir un valor por defecto es peligroso en las cuatro: un
#: `dataclassification` ausente tomado como `internal` hace que PII circule con 30 días
#: de retención en vez de 7, y un `tenantid` ausente tomado como `system` cruza fronteras
#: de tenant. Ver 06-security.md §4 y §5.
REQUIRED_EXTENSIONS: Final[tuple[str, ...]] = (
    "correlationid",
    "tenantid",
    "producerversion",
    "dataclassification",
)

VALID_CLASSIFICATIONS: Final[frozenset[str]] = frozenset(
    {"public", "internal", "confidential", "restricted"}
)

#: Atributos que DEBEN llegar como cadena JSON — 01-envelope.md §2.4.
#:
#: `json.loads` no coacciona nada, así que sin esta lista un `{"tenantid": 42}` se
#: colaría hasta el handler como el entero 42 y reventaría en la primera comparación con
#: `==`. Jackson en cambio lo convertiría en `"42"` sin avisar: el mismo cuerpo daría
#: tres comportamientos según el SDK, y el que "funciona" es el peor.
STRING_ATTRIBUTES: Final[tuple[str, ...]] = (
    "id",
    "source",
    "type",
    "time",
    "correlationid",
    "tenantid",
    "producerversion",
)


class EnvelopeError(ValueError):
    pass


# ─── Construcción ────────────────────────────────────────────────────────────


def build_event(
    *,
    subject: str,
    data: T,
    id: str,
    source: str,
    producerversion: str,
    tenantid: str,
    dataclassification: DataClassification,
    dataschema: str,
    correlationid: str,
    time: datetime | str | None = None,
    aggregate_id: str | None = None,
    causationid: str | None = None,
    partitionkey: str | None = None,
    traceparent: str | None = None,
    tracestate: str | None = None,
) -> FluxEvent[T]:
    """
    Monta el CloudEvent. Todos los parámetros son keyword-only a propósito: `source`,
    `id` y `correlationid` son tres strings seguidos y posicionalmente son
    indistinguibles.
    """
    if not isinstance(data, Mapping):
        # Un array o escalar en la raíz impide añadir campos después sin romper el
        # esquema — 01-envelope.md §4.
        raise EnvelopeError(
            "`data` debe ser un objeto JSON en la raíz (ni array ni escalar), para poder "
            "añadir campos sin romper el contrato"
        )

    # Por convención, la clave de partición es el id del agregado — 01-envelope.md §3.2.
    partition = partitionkey if partitionkey is not None else aggregate_id

    return FluxEvent(
        id=id,
        source=source,
        type=subject_to_type(subject),
        time=iso_utc(time) if time is not None else now_iso(),
        dataschema=dataschema,
        subject=aggregate_id,
        correlationid=correlationid,
        tenantid=tenantid,
        producerversion=producerversion,
        dataclassification=dataclassification,
        causationid=causationid,
        partitionkey=partition,
        traceparent=traceparent,
        tracestate=tracestate,
        data=data,
    )


def serialize(event: FluxEvent[Any]) -> bytes:
    """Serializa el evento y aplica el límite de 1 MiB — 01-envelope.md §6."""
    # separators sin espacios y ensure_ascii=False para producir byte a byte lo mismo
    # que `JSON.stringify` del SDK de Node: un envelope con acentos no debe medir
    # distinto según el lenguaje que lo emitió.
    payload = json.dumps(
        event.to_dict(), separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")

    if len(payload) > MAX_MESSAGE_BYTES:
        # Fallar aquí y no en el broker: el mensaje del broker no dice qué hacer.
        raise EnvelopeError(
            f"evento de {len(payload)} bytes supera el límite de {MAX_MESSAGE_BYTES} (1 MiB). "
            f"Aplica claim-check: sube el contenido a almacenamiento de objetos y publica "
            f"{{ uri, sha256, bytes }} en su lugar (01-envelope.md §6)."
        )
    return payload


# ─── Parseo ──────────────────────────────────────────────────────────────────


def parse_event(payload: bytes | bytearray | memoryview | str) -> FluxEvent[Any]:
    """
    Un fallo aquí es POISON: el mensaje ni siquiera es interpretable, así que nunca
    llega al handler (04-errors.md §1.3).
    """
    try:
        raw = json.loads(payload)
    except Exception as cause:
        raise PoisonError(
            "el cuerpo del mensaje no es JSON válido",
            code="MALFORMED_JSON",
            cause=cause,
        ) from cause

    if not isinstance(raw, dict):
        raise PoisonError("el cuerpo del mensaje no es un objeto JSON", code="NOT_AN_OBJECT")

    if raw.get("specversion") != "1.0":
        raise PoisonError(
            f"specversion no soportada: {json.dumps(raw.get('specversion'))} (se esperaba \"1.0\")",
            code="UNSUPPORTED_SPECVERSION",
        )

    missing = [a for a in REQUIRED_ATTRIBUTES if raw.get(a) is None]
    if missing:
        raise PoisonError(
            f"faltan atributos obligatorios de CloudEvents: {', '.join(missing)}",
            code="MISSING_REQUIRED_ATTRIBUTE",
        )

    # `""` cuenta como ausente igual que `null`: 01-envelope.md §3.3 prohíbe darle
    # significado propio a un valor vacío, así que un `tenantid: ""` no es un tenant.
    missing_ext = [a for a in REQUIRED_EXTENSIONS if raw.get(a) is None or raw.get(a) == ""]
    if missing_ext:
        raise PoisonError(
            f"faltan extensiones obligatorias del perfil flux: {', '.join(missing_ext)}. "
            f"Su ausencia no es recuperable asumiendo un valor: un dataclassification "
            f"ausente tomado como \"internal\" haría circular PII con la retención larga "
            f"(01-envelope.md §3.1)",
            code="MISSING_REQUIRED_EXTENSION",
        )

    classification = raw["dataclassification"]
    # El `isinstance` va delante y no sobra: `{"a": 1} in frozenset(...)` no es False,
    # es un TypeError, y el mensaje corrupto tumbaría el runtime del consumidor en vez
    # de acabar en la DLQ como POISON.
    if not isinstance(classification, str) or classification not in VALID_CLASSIFICATIONS:
        # Un valor fuera del enum no se puede degradar a algo seguro: la retención y las
        # ACLs de 06-security.md §5 se derivan de él, así que inventarle un sentido es
        # peor que rechazar el mensaje.
        raise PoisonError(
            f"dataclassification inválido: {json.dumps(classification)}. "
            f"Valores permitidos: {', '.join(sorted(VALID_CLASSIFICATIONS))}",
            code="INVALID_DATACLASSIFICATION",
        )

    # Los tipos son exactos: {"tenantid": 42} es POISON, no el tenant "42".
    # Aquí hay que comprobarlo a mano porque `json.loads` devuelve el int tal cual y
    # nadie más lo mira hasta que una comparación con `==` falla en producción
    # — 01-envelope.md §2.4.
    for attribute in STRING_ATTRIBUTES:
        value = raw[attribute]
        if not isinstance(value, str):
            raise PoisonError(
                f"{attribute} debe ser una cadena, llegó {type(value).__name__} "
                f"({json.dumps(value)})",
                code="WRONG_ATTRIBUTE_TYPE",
            )

    if raw.get("datacontenttype") != "application/json":
        raise PoisonError(
            f"datacontenttype no soportado: {json.dumps(raw.get('datacontenttype'))}",
            code="UNSUPPORTED_CONTENT_TYPE",
        )

    unknown = [k for k in raw if k not in ALLOWED_ROOT_ATTRIBUTES]
    if unknown:
        # Estricto a propósito: permitir atributos raíz ad-hoc lleva a serializar JSON
        # dentro de un string y el envelope deja de ser interoperable — 01-envelope.md §3.3.
        raise PoisonError(
            f"atributos raíz desconocidos: {', '.join(unknown)}. "
            f"Todo metadato adicional va dentro de `data`",
            code="UNKNOWN_ROOT_ATTRIBUTE",
        )

    return FluxEvent(**raw)


# ─── DLQ ─────────────────────────────────────────────────────────────────────


def to_dlq_event(
    event: FluxEvent[T],
    *,
    reason: DlqReason,
    attempts: int,
    consumer: str,
    error: str,
) -> FluxEvent[T]:
    """
    Prepara un evento para la DLQ: el CloudEvent original íntegro más las extensiones
    `dlq*`. Sin wrapper — así el replay es `del(.dlq*)` y republicar (04-errors.md §3).
    """
    return replace(
        event,
        dlqreason=reason,
        dlqattempts=attempts,
        dlqconsumer=consumer,
        # Un stacktrace entero cabría en el envelope pero se comería el presupuesto de
        # 1 MiB del propio evento. 1 KiB basta para identificar el fallo.
        dlqerror=error[:1024],
        dlqtime=now_iso(),
    )


def strip_dlq_extensions(event: FluxEvent[T]) -> FluxEvent[T]:
    """Quita las extensiones `dlq*`. El `id` original se conserva — 04-errors.md §4.1."""
    return replace(
        event,
        dlqreason=None,
        dlqattempts=None,
        dlqconsumer=None,
        dlqerror=None,
        dlqtime=None,
    )
