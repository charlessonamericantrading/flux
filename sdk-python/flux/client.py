"""
Cliente de flux. Nivel de conformidad: **L3** (validación de esquema opt-in, `off` por
defecto; sin activarla el comportamiento es exactamente el de L2).
Contrato normativo: specification/00-protocol.md §5

Regla de diseño: este fichero NO debe exponer ningún tipo de NATS en su API pública.
Si lo hiciera, sustituir el broker dejaría de ser un cambio de capa 0-1 y pasaría a
tocar las aplicaciones — que es exactamente lo que flux existe para evitar
(00-protocol.md §3).
"""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import logging
import math
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Final, Mapping, Sequence

import nats
import nats.errors
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    DeliverPolicy,
    DiscardPolicy,
    ReplayPolicy,
    RetentionPolicy,
    StorageType,
    StreamConfig,
)
from nats.js.errors import NotFoundError

from .classify import ClassifierOptions, create_classifier
from .context import active_traceparent, context_from_event, current_context, use_context
from .envelope import (
    DataClassification,
    DlqReason,
    FluxEvent,
    build_event,
    now_iso,
    parse_event,
    serialize,
    to_dlq_event,
)
from .errors import Classification, ErrorClass, PoisonError
from .metrics import NO_METRICS, ConnectionState, ConsumeOutcome, MetricsSink
from .protocol import (
    CONSUMER_DEFAULTS,
    DEFAULT_PENDING_POLL_MS,
    STREAM_DEFAULTS,
    dlq_stream_name,
    dlq_subject,
    durable_name,
    parse_subject,
    seconds,
    source_uri,
    stream_name,
    uuid7,
)
from .signing import Signer, SigningOptions, Verifier, create_signer, create_verifier
from .tenant import TenantIsolation, TenantIsolationError, resolve_tenant_filter
from .validation import (
    SchemaBundle,
    ValidationOptions,
    Validator,
    create_validator,
    schema_uri_for,
)

__all__ = [
    "Credentials",
    "ConnectOptions",
    "HandlerContext",
    "Handler",
    "Subscription",
    "PoisonInfo",
    "DlqInfo",
    "ConsumerConfigMismatchError",
    # Se reexportan desde flux.tenant: viven fuera de aquí para poder probarse sin
    # `nats-py` instalado, pero la API pública del SDK es la misma.
    "TenantIsolationError",
    "TenantIsolation",
    "resolve_tenant_filter",
    "FluxBus",
    "connect",
]

#: Códigos POISON que produce la verificación de firma — 07-signing.md §7.
#:
#: Se distinguen del resto de POISON solo para las métricas: `outcome="invalid_signature"`
#: separa "un productor está publicando basura" de "un productor está publicando eventos
#: que no son suyos", que son dos incidentes distintos con dos respuestas distintas.
_SIGNING_CODES: Final[frozenset[str]] = frozenset(
    {"MISSING_SIGNATURE", "INVALID_SIGNATURE", "UNKNOWN_SIGNING_KEY"}
)

#: Códigos que produce la validación L3 al consumir — 00-protocol.md §5.
#:
#: Mismo motivo que `_SIGNING_CODES`: el `dlqreason` sigue siendo `permanent` (es el enum
#: cerrado de 04-errors.md §1), pero el `outcome` de la métrica distingue "este consumidor
#: rechaza el evento" de "el productor está publicando payloads que violan su contrato".
#: Son dos incidentes con dos dueños distintos.
_SCHEMA_CODES: Final[frozenset[str]] = frozenset({"SCHEMA_INVALID", "SCHEMA_NOT_FOUND"})


# ─── Opciones ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Credentials:
    """Credenciales NATS. NUNCA versionadas — 06-security.md §2."""

    creds: str | None = None
    token: str | None = None
    user: str | None = None
    password: str | None = None


@dataclass(frozen=True)
class PoisonInfo:
    subject: str
    error: BaseException
    raw: bytes


@dataclass(frozen=True)
class DlqInfo:
    subject: str
    event: FluxEvent[Any]
    classification: Classification


@dataclass(frozen=True)
class ConnectOptions:
    """Configuración de la conexión. La construye `connect()` a partir de sus kwargs."""

    #: `nats://host:4222`, o varios para HA.
    servers: str | Sequence[str]
    #: Nombre del servicio. Alimenta `source` y los nombres de durable consumer.
    service: str
    #: `produccion`, `staging`, `dev`. Alimenta `source`.
    environment: str
    #: SemVer del servicio. Va en `producerversion`.
    version: str
    #: Tenant por defecto de los eventos publicados, y filtro por defecto al consumir.
    tenant_id: str = "system"
    #: Aislamiento entre tenants — 09-multitenancy.md §3.
    #:
    #: `"strict"`: toda suscripción filtra por tenant, y suscribirse sin tenant
    #: configurado es un **error de configuración**. Un filtro que hay que acordarse de
    #: poner es un filtro que alguien olvidará, y el fallo —ver los datos de otro tenant—
    #: no produce ningún error: produce un incidente de privacidad que se descubre
    #: semanas después.
    #:
    #: `"off"` (default): el filtrado sigue ocurriendo si hay `tenant_id`, pero olvidarlo
    #: no rompe nada.
    tenant_isolation: TenantIsolation = "off"
    #: Clasificación por defecto. Ver 06-security.md §5.
    classification: DataClassification = "internal"
    #: Firma Ed25519 de eventos. Extensión OPCIONAL — 07-signing.md. Traslada la
    #: autenticidad del canal al evento: un evento firmado sigue siendo verificable
    #: dentro de un fichero, un backup o un correo, donde ya no hay ACL.
    signing: SigningOptions | None = None
    #: Destino de métricas. Los nombres y etiquetas los fija el protocolo
    #: (08-observability.md), no la aplicación: si cada SDK nombrara a su manera, un panel
    #: del ecosistema sería imposible. El default no registra nada.
    metrics: MetricsSink = NO_METRICS
    #: Mapa exacto subject → URI de `dataschema`. Gana sobre `schema_base_url`.
    schemas: Mapping[str, str] = field(default_factory=dict)
    #: Base para derivar `dataschema` cuando no está en `schemas`.
    schema_base_url: str | None = None
    #: Validación L3 contra el JSON Schema del evento. Ver validation.py.
    #:
    #: Con `mode="strict"`, publicar un payload que viola su contrato falla en el
    #: productor en vez de aparecer como un misterio en un consumidor de otro equipo la
    #: semana que viene — 00-protocol.md §5. El default (`off`) es L2 y no cuesta nada.
    validation: ValidationOptions | None = None
    #: Cada cuánto sondear `num_pending` de cada consumidor, en ms. `0` lo desactiva.
    #:
    #: No es opcional por capricho: `flux_consumer_pending` es la ÚNICA señal que delata a
    #: un consumidor cuyo bucle murió, porque la conexión sigue reportándose sana y el
    #: healthcheck dice que todo va bien (08-observability.md §4). Pero el dato solo se
    #: obtiene preguntándole al servidor, así que hace falta un sondeo.
    pending_poll_ms: float = DEFAULT_PENDING_POLL_MS
    #: Política de clasificación de errores. Ver classify.py.
    classifier: ClassifierOptions | None = None
    credentials: Credentials | None = None
    #: Se invoca ante un POISON. Es el único caso que debe despertar a alguien.
    on_poison: Callable[[PoisonInfo], None] | None = None
    #: Se invoca al enrutar cualquier evento a la DLQ.
    on_dlq: Callable[[DlqInfo], None] | None = None
    #: Logger opcional. Por defecto, silencio.
    logger: logging.Logger | None = None


@dataclass(frozen=True)
class HandlerContext:
    """Lo que el handler recibe además del evento."""

    #: Nº de entrega, empezando en 1.
    attempt: int
    max_attempts: int
    subject: str

    def ack(self) -> None:
        """
        Confirma el evento. Es un no-op: devolver normalmente del handler hace lo mismo.

        Existe porque los ejemplos de la spec terminan en `return ctx.ack()` y porque
        escribirlo obliga a pensar en la idempotencia (03-delivery.md §4) — que es el
        único requisito de admisión al ecosistema.
        """


Handler = Callable[[FluxEvent[Any], HandlerContext], Awaitable[None] | None]


class Subscription:
    """Handle de una suscripción viva. No expone nada de NATS."""

    def __init__(self, subject: str, durable: str, stop: Callable[[], Awaitable[None]]) -> None:
        self.subject = subject
        self.durable = durable
        self._stop = stop

    async def unsubscribe(self) -> None:
        await self._stop()


# ─── Errores del cliente ─────────────────────────────────────────────────────


class ConsumerConfigMismatchError(RuntimeError):
    def __init__(self, durable: str, differences: Sequence[tuple[str, Any, Any]]) -> None:
        detail = "\n".join(
            f"  {field_}: solicitado {requested!r}, efectivo {effective!r}"
            for field_, requested, effective in differences
        )
        super().__init__(
            f'el servidor devolvió una configuración distinta de la solicitada para "{durable}":\n'
            f"{detail}\n"
            f"JetStream sobrescribe algunos campos en silencio (03-delivery.md §2.1)."
        )
        self.durable = durable
        self.differences = list(differences)


def _same_duration(requested_s: float, effective: Any) -> bool:
    """
    ¿Coinciden dos duraciones que pueden venir en unidades distintas?

    `nats-py` convierte `ack_wait` de nanosegundos a segundos al leer la respuesta del
    servidor, pero según la versión deja `backoff` tal cual llegó (nanosegundos).
    Aceptar ambas representaciones evita un falso positivo de "config alterada" que
    haría fallar el arranque de todo servicio Python del ecosistema.
    """
    if not isinstance(effective, (int, float)):
        return False
    return math.isclose(effective, requested_s, rel_tol=1e-6) or math.isclose(
        effective, requested_s * 1e9, rel_tol=1e-6
    )


# ─── Cliente ─────────────────────────────────────────────────────────────────


class FluxBus:
    def __init__(self, nc: nats.NATS, js: JetStreamContext, options: ConnectOptions) -> None:
        self._nc = nc
        # En `nats-py` el contexto de JetStream hereda de `JetStreamManager`, así que
        # no hay dos objetos como en Node (`js` + `jsm`): este único `js` sirve para
        # publicar y para administrar streams y consumidores.
        self._js = js
        self._opts = options
        self._classify = create_classifier(options.classifier)
        self._source = source_uri(options.environment, options.service)
        self._pumps: set[asyncio.Task[None]] = set()
        self._ensured: set[str] = set()
        self._metrics = options.metrics
        # Se construyen aquí y no por evento: parsear un PEM en la ruta caliente sería
        # tirar el throughput por comodidad de escritura.
        signing = options.signing or SigningOptions()
        self._signer: Signer | None = create_signer(signing)
        self._verifier: Verifier | None = create_verifier(signing, options.logger)
        # Igual con el validador L3: compilar los esquemas por evento sería tirar el
        # throughput, y un bundle ausente o un esquema roto deben romper el ARRANQUE, que
        # es donde se ve un fallo de configuración — 00-protocol.md §5.
        self._validation = options.validation or ValidationOptions()
        self._validate: Validator | None = create_validator(self._validation, options.logger)
        self._metrics.connection_state(ConnectionState.CONNECTED)

    # ─── publish ──────────────────────────────────────────────────────────────

    async def publish(
        self,
        subject: str,
        data: Mapping[str, Any],
        *,
        aggregate_id: str | None = None,
        tenant_id: str | None = None,
        classification: DataClassification | None = None,
        time: datetime | str | None = None,
        partition_key: str | None = None,
    ) -> FluxEvent[Any]:
        """
        Publica un evento. El desarrollador solo escribe `subject`, `data` y
        opcionalmente `aggregate_id` — 01-envelope.md §5.

        ⚠️ `aggregate_id` es el atributo `subject` de CloudEvents (`"ped-123"`), NO el
        subject de NATS. Son dos cosas distintas con el mismo nombre y confundirlas es
        el error más frecuente al adoptar CloudEvents sobre NATS (01-envelope.md §2.1).
        """
        parsed = parse_subject(subject)  # falla temprano y con un mensaje útil
        await self._ensure_stream(parsed.domain)

        event_id = uuid7()
        inherited = current_context()

        traceparent = inherited.traceparent if inherited is not None else None
        if traceparent is None:
            traceparent = active_traceparent()

        event = build_event(
            subject=subject,
            data=data,
            id=event_id,
            source=self._source,
            producerversion=self._opts.version,
            # El contexto heredado gana sobre el default de conexión: un evento derivado
            # pertenece al tenant del evento que lo causó, no al del servicio.
            tenantid=_first(
                tenant_id,
                inherited.tenantid if inherited is not None else None,
                self._opts.tenant_id,
                "system",
            ),
            dataclassification=_first(classification, self._opts.classification, "internal"),
            dataschema=self._schema_for(subject),
            # Si el evento no nace de otro, se inicializa con su propio id — 01-envelope.md §3.1.
            correlationid=inherited.correlationid if inherited is not None else event_id,
            causationid=inherited.causationid if inherited is not None else None,
            time=time,
            aggregate_id=aggregate_id,
            partitionkey=partition_key,
            traceparent=traceparent,
            tracestate=inherited.tracestate if inherited is not None else None,
        )

        # L3: validar ANTES de publicar. Un payload que viola su contrato debe fallar
        # aquí, en el servicio que lo generó, y no aparecer como un misterio en un
        # consumidor de otro equipo la semana que viene — 00-protocol.md §5.
        if self._validate is not None:
            try:
                self._validate(event, subject)
            except Exception:
                self._metrics.event_published(subject, "invalid_schema")
                raise

        # Firmar es LO ÚLTIMO antes de serializar: la firma cubre el envelope completo,
        # así que cualquier atributo añadido después la invalidaría — 07-signing.md §5.
        publicado = self._signer.sign(event) if self._signer is not None else event

        # La cabecera Nats-Msg-Id deduplica reintentos de PUBLICACIÓN dentro de
        # duplicate_window. NO deduplica reentregas de consumo — 03-delivery.md §3.
        try:
            await self._js.publish(
                subject, serialize(publicado), headers={"Nats-Msg-Id": publicado.id}
            )
        except Exception:
            self._metrics.event_published(subject, "error")
            raise
        self._metrics.event_published(subject, "ok")
        return publicado

    def _schema_for(self, subject: str) -> str:
        explicit = self._opts.schemas.get(subject)
        if explicit:
            return explicit

        # El bundle L3 conoce el MINOR real de cada subject: dentro de un mayor todo es
        # BACKWARD-compatible, así que el más alto acepta lo que aceptan los anteriores
        # — 05-compatibility.md §2.
        bundle: SchemaBundle | None = self._validation.bundle
        if bundle is not None:
            del_bundle = schema_uri_for(bundle, subject)
            if del_bundle:
                return del_bundle

        base = self._opts.schema_base_url
        if not base:
            raise ValueError(
                f'no hay dataschema para "{subject}". Declara `schemas["{subject}"]`, '
                f"`schema_base_url`, o pasa un bundle en `validation.bundle` (connect())."
            )
        p = parse_subject(subject)
        # Sin bundle ni mapa explícito solo se puede asumir el .0.0 del mayor. Es
        # suficiente para L2 —el atributo es informativo— pero no para L3.
        return f"{base.rstrip('/')}/{p.domain}/{p.aggregate}/{p.event}/{p.major}.0.0.json"

    # ─── subscribe ────────────────────────────────────────────────────────────

    async def subscribe(
        self,
        subject: str,
        handler: Handler,
        *,
        durable: str | None = None,
        max_ack_pending: int | None = None,
        tenant_id: str | None = None,
    ) -> Subscription:
        """
        Suscribe un handler. El durable se deriva del servicio y el subject
        (02-naming.md §4) salvo que se dé uno explícito.

        El handler puede ser síncrono o `async`. Devolver normalmente = ack; lanzar =
        clasificar y decidir entre nak, DLQ o alerta (04-errors.md).

        Con `tenant_isolation="strict"`, suscribirse sin tenant configurado lanza
        `TenantIsolationError` — 09-multitenancy.md §3.
        """
        parsed = parse_subject(subject)
        # ANTES de tocar el broker: un error de configuración debe romper el arranque,
        # no dejar un consumidor a medio crear.
        tenant_filter = resolve_tenant_filter(
            subject, self._opts.tenant_id, tenant_id, self._opts.tenant_isolation
        )
        await self._ensure_stream(parsed.domain)
        await self._ensure_dlq_stream(parsed.domain)

        stream = stream_name(parsed.domain)
        durable = durable or durable_name(self._opts.service, subject)

        requested = ConsumerConfig(
            durable_name=durable,
            filter_subject=subject,
            ack_policy=AckPolicy.EXPLICIT,
            # ack_wait DEBE ser backoff[0]: JetStream lo sobrescribe con ese valor sin
            # avisar. Ver 03-delivery.md §2.1. Y en segundos, no en nanosegundos:
            # `nats-py` hace la conversión (ver `protocol.seconds`).
            ack_wait=seconds(CONSUMER_DEFAULTS.ack_wait_ms),
            max_deliver=CONSUMER_DEFAULTS.max_deliver,
            backoff=[seconds(b) for b in CONSUMER_DEFAULTS.backoff_ms],
            max_ack_pending=max_ack_pending or CONSUMER_DEFAULTS.max_ack_pending,
            deliver_policy=DeliverPolicy.ALL,
            replay_policy=ReplayPolicy.INSTANT,
        )

        try:
            effective = (await self._js.add_consumer(stream, config=requested)).config
        except Exception:
            # El consumidor ya existe con otra config: el servidor rechaza el add. Se
            # lee la que hay para que el mismatch se reporte con detalle en vez de con
            # un error opaco del broker.
            effective = (await self._js.consumer_info(stream, durable)).config

        self._assert_config_honored(durable, requested, effective)

        psub = await self._js.pull_subscribe_bind(durable=durable, stream=stream)

        stopped = asyncio.Event()
        tareas = [
            asyncio.create_task(
                self._pump(psub, stopped, subject, durable, handler, tenant_filter),
                name=f"flux-pump:{durable}",
            )
        ]

        # Sondeo de num_pending. Sin él, `flux_consumer_pending` solo se alimenta de los
        # metadatos de los mensajes entregados — y si el bucle muere dejan de llegar
        # mensajes, así que el gauge se queda PLANO en vez de crecer. Un panel mostraría
        # una línea horizontal, indistinguible de "no pasa nada" (08-observability.md §2.3
        # y §4). No se lanza con el sumidero nulo: sería una petición cada 15 s por
        # consumidor para tirar el resultado.
        intervalo = self._pending_poll_seconds()
        if intervalo is not None:
            tareas.append(
                asyncio.create_task(
                    self._poll_pending(stream, durable, subject, stopped, intervalo),
                    name=f"flux-pending:{durable}",
                )
            )

        self._pumps.update(tareas)

        async def stop() -> None:
            stopped.set()
            for tarea in tareas:
                tarea.cancel()
            for tarea in tareas:
                self._pumps.discard(tarea)
                try:
                    await tarea
                except asyncio.CancelledError:
                    pass
            try:
                await psub.unsubscribe()
            except Exception:  # la conexión puede estar ya cerrada
                pass

        return Subscription(subject, durable, stop)

    def _assert_config_honored(
        self, durable: str, requested: ConsumerConfig, effective: ConsumerConfig
    ) -> None:
        """
        Requisito L2: verificar que el servidor aplicó lo que se le pidió.
        Es la única defensa contra sobrescrituras silenciosas — 03-delivery.md §2.1.
        """
        diffs: list[tuple[str, Any, Any]] = []

        if effective.ack_wait is not None and not _same_duration(
            requested.ack_wait or 0, effective.ack_wait
        ):
            diffs.append(("ack_wait", requested.ack_wait, effective.ack_wait))

        for name in ("max_deliver", "max_ack_pending"):
            eff = getattr(effective, name, None)
            req = getattr(requested, name)
            if eff is not None and eff != req:
                diffs.append((name, req, eff))

        req_backoff = list(requested.backoff or [])
        eff_backoff = list(effective.backoff or [])
        if len(eff_backoff) != len(req_backoff) or any(
            not _same_duration(r, e) for r, e in zip(req_backoff, eff_backoff)
        ):
            diffs.append(("backoff", req_backoff, eff_backoff))

        if diffs:
            raise ConsumerConfigMismatchError(durable, diffs)

    # ─── bucle de consumo ─────────────────────────────────────────────────────

    async def _pump(
        self,
        psub: JetStreamContext.PullSubscription,
        stopped: asyncio.Event,
        subject: str,
        durable: str,
        handler: Handler,
        tenant_filter: str | None,
    ) -> None:
        while not stopped.is_set():
            try:
                # batch=1 no es una limitación: es la consecuencia de procesar los
                # mensajes en serie. Con batch=N, los N-1 mensajes que esperan turno
                # gastan su ack_wait sin que nadie emita WIP por ellos y JetStream los
                # reentrega mientras el primero sigue en el handler — justo la ejecución
                # concurrente que 03-delivery.md §2.1 obliga a evitar.
                msgs = await psub.fetch(batch=1, timeout=5)
            except (nats.errors.TimeoutError, asyncio.TimeoutError):
                continue  # no había nada que entregar; se vuelve a pedir
            except asyncio.CancelledError:
                raise
            except nats.errors.ConnectionClosedError:
                return
            except Exception as e:  # error transitorio del broker o de la conexión
                self._log("warning", "[flux] fallo al recibir de %s: %s", subject, e)
                await asyncio.sleep(1)
                continue

            for m in msgs:
                if stopped.is_set():
                    break
                try:
                    await self._dispatch(m, subject, durable, handler, tenant_filter)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    # Un fallo del propio despacho (no del handler: eso ya está
                    # clasificado) no puede matar el bucle. Un consumidor muerto en
                    # silencio parece vivo en el healthcheck y no consume nada.
                    # Sin ack ni term: el mensaje se reentrega, que es correcto.
                    self._log("error", "[flux] fallo despachando %s: %s", subject, e)

    def _pending_poll_seconds(self) -> float | None:
        """
        Intervalo del sondeo en segundos, o `None` si no procede.

        No procede con `pending_poll_ms = 0` —desactivado explícitamente— ni con el
        sumidero nulo: sería una petición cada 15 s por consumidor para tirar el
        resultado.
        """
        ms = self._opts.pending_poll_ms
        if ms <= 0 or self._metrics is NO_METRICS:
            return None
        return ms / 1000

    async def _poll_pending(
        self,
        stream: str,
        durable: str,
        subject: str,
        stopped: asyncio.Event,
        intervalo_s: float,
    ) -> None:
        """
        Sondea `num_pending` cada `pending_poll_ms` — 08-observability.md §2.3.

        Qué mide con precisión: los mensajes del stream **aún no entregados** a este
        consumidor. Un handler lento NO la hace crecer (sus mensajes ya se entregaron y
        esperan ack: eso se ve en el histograma de duración); un consumidor muerto sí, y
        sin techo, mientras la conexión sigue reportándose sana.
        """
        while not stopped.is_set():
            try:
                # Esperar sobre el Event y no `sleep`: al desuscribir, la parada es
                # inmediata en vez de tardar hasta 15 s en notarse.
                await asyncio.wait_for(stopped.wait(), timeout=intervalo_s)
                return
            except (asyncio.TimeoutError, TimeoutError):
                pass
            except asyncio.CancelledError:
                raise

            try:
                info = await self._js.consumer_info(stream, durable)
                self._metrics.consumer_pending(subject, durable, info.num_pending)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Un fallo del sondeo NO DEBE afectar al consumo: es telemetría. Se
                # registra y se vuelve a intentar en el siguiente ciclo.
                self._log(
                    "warning", "[flux] no se pudo sondear num_pending de %s: %s", durable, e
                )

    async def _dispatch(
        self,
        m: Msg,
        subject: str,
        durable: str,
        handler: Handler,
        tenant_filter: str | None,
    ) -> None:
        attempt, pending = self._delivery_info(m)
        max_attempts = CONSUMER_DEFAULTS.max_deliver

        # Los metadatos traen `num_pending` gratis en cada entrega: más fresco que el
        # sondeo y sin coste. Pero NO lo sustituye — si el bucle muere dejan de llegar
        # mensajes y el gauge se quedaría plano en su último valor en vez de crecer, que
        # es exactamente lo contrario de la señal que hace falta (08-observability.md
        # §2.3). Por eso el SDK usa las dos fuentes.
        if pending is not None:
            self._metrics.consumer_pending(subject, durable, pending)

        # POISON se detecta antes del handler: el mensaje no es interpretable, así que
        # el handler nunca llega a verlo — 04-errors.md §1.3.
        try:
            event = parse_event(m.data)
        except Exception as e:
            err = e if isinstance(e, PoisonError) else PoisonError(str(e), cause=e)
            if self._opts.on_poison is not None:
                self._opts.on_poison(PoisonInfo(subject=subject, error=err, raw=m.data))
            self._metrics.event_consumed(subject, durable, "poison")
            self._metrics.event_dlq(subject, durable, "poison", err.code or "POISON")
            self._log("error", "[flux] POISON en %s: %s", subject, err)
            if await self._safely_send_raw_to_dlq(subject, m.data, durable, str(err)):
                await m.term()
            return

        # Filtrar ANTES del handler: un evento de otro tenant no es un fallo, no es para
        # nosotros. Se confirma y se descarta — 09-multitenancy.md §3.
        if tenant_filter is not None and event.tenantid != tenant_filter:
            await m.ack()
            return

        # WIP mientras el handler vive: extiende ack_wait y evita que el mismo evento se
        # ejecute en concurrencia consigo mismo — 03-delivery.md §2.1.
        wip = asyncio.create_task(self._keep_alive(m))
        inicio = time.monotonic()
        try:
            # La firma se comprueba ANTES de invocar al handler: si el evento fue
            # manipulado, su payload puede ser perfectamente válido y aun así no venir
            # del productor que dice — 07-signing.md §5.1.
            if self._verifier is not None:
                self._verifier.check(event)
            # L3 al consumir: el evento es sintácticamente válido pero incumple su
            # contrato. Reintentarlo dará exactamente el mismo resultado, así que la
            # clasificación correcta es PERMANENT — la fija el propio error, que hereda
            # de PermanentError (validation.py), no la política del clasificador.
            #
            # Va DESPUÉS de la firma a propósito: si el evento fue manipulado, su payload
            # puede validar perfectamente y aun así no ser del productor que dice.
            if self._validation.on_consume and self._validate is not None:
                self._validate(event, subject)
            with use_context(context_from_event(event)):
                result = handler(event, HandlerContext(attempt, max_attempts, subject))
                if inspect.isawaitable(result):
                    await result
            await m.ack()
            self._metrics.event_consumed(subject, durable, "ok")
            self._metrics.handler_duration(subject, durable, time.monotonic() - inicio)
        except asyncio.CancelledError:
            # El SDK se está cerrando: ni ack ni term. El mensaje se reentregará, que es
            # exactamente el caso que cubre la idempotencia del consumidor.
            raise
        except Exception as e:
            c = self._classify(e)
            message = str(e) or type(e).__name__

            # El presupuesto efectivo es el menor entre el del consumidor y el que la
            # clasificación imponga para ESTE error. Así un error desconocido agota su
            # presupuesto acotado sin recortar los 6 intentos de un ECONNRESET
            # reconocido — 04-errors.md §2.1.
            budget = min(max_attempts, c.max_attempts or max_attempts)

            if c.error_class is ErrorClass.RETRYABLE and attempt < budget:
                # Retraso explícito según el backoff canónico, en vez de dejar expirar
                # ack_wait: no retiene la ranura de ack_pending 30 s de más.
                delay_ms = c.retry_after_ms
                if delay_ms is None:
                    backoff = CONSUMER_DEFAULTS.backoff_ms
                    delay_ms = backoff[min(attempt - 1, len(backoff) - 1)]
                self._log(
                    "warning",
                    "[flux] RETRYABLE %s en %s (intento %d/%d), reintento en %dms",
                    c.code,
                    subject,
                    attempt,
                    budget,
                    delay_ms,
                )
                self._metrics.event_retried(subject, durable, attempt)
                # ⚠️ El delay solo se honra en la PRIMERA reentrega: con `backoff`
                # configurado —y flux lo configura siempre— JetStream manda el array a
                # partir de la segunda y lo ignora sin avisar (03-delivery.md §2.2).
                await m.nak(delay=delay_ms / 1000)
                return

            if c.error_class is ErrorClass.POISON:
                reason: DlqReason = "poison"
            elif c.error_class is ErrorClass.RETRYABLE:
                reason = "retryable"  # agotó los reintentos
            else:
                reason = "permanent"

            try:
                await self._send_to_dlq(
                    subject, event, durable, attempt, reason, f"{c.code}: {message}"
                )
            except Exception as dlq_error:
                # Si la DLQ no admite el evento, NO se hace term: un term sin registro
                # en la DLQ borra el evento sin dejar rastro. Sin ack ni term, JetStream
                # lo reentrega y el fallo sigue siendo visible.
                self._log(
                    "error",
                    "[flux] no se pudo escribir en la DLQ de %s (%s); el evento %s se "
                    "reentregará: %s",
                    subject,
                    reason,
                    event.id,
                    dlq_error,
                )
                return

            # `outcome` distingue la firma inválida del POISON común: son dos incidentes
            # distintos —basura frente a suplantación— y la etiqueta existe para eso
            # (08-observability.md §2.1). El `reason` de la DLQ sigue siendo `poison`,
            # que es el enum de 04-errors.md §1.
            outcome: ConsumeOutcome = reason
            if c.code in _SIGNING_CODES:
                outcome = "invalid_signature"
            elif c.code in _SCHEMA_CODES:
                outcome = "invalid_schema"
            self._metrics.event_consumed(subject, durable, outcome)
            self._metrics.event_dlq(subject, durable, reason, c.code)
            self._metrics.handler_duration(subject, durable, time.monotonic() - inicio)

            if self._opts.on_dlq is not None:
                self._opts.on_dlq(DlqInfo(subject=subject, event=event, classification=c))
            self._log(
                "error",
                "[flux] DLQ (%s) %s en %s tras %d intento(s): %s",
                reason,
                c.code,
                subject,
                attempt,
                message,
            )
            await m.term()
        finally:
            wip.cancel()

    @staticmethod
    def _delivery_info(m: Msg) -> tuple[int, int | None]:
        """
        `(intento, num_pending)` de esta entrega.

        Los dos salen del mismo objeto de metadatos y se leen a la vez porque `m.metadata`
        reparsea el subject de reply en cada acceso. `num_pending` puede no venir (un
        mensaje que no sea de JetStream no tiene metadatos), y entonces manda el sondeo.
        """
        try:
            meta = m.metadata
        except Exception:
            return 1, None
        pending = meta.num_pending
        return (meta.num_delivered or 1), (pending if isinstance(pending, int) else None)

    async def _keep_alive(self, m: Msg) -> None:
        """Emite WIP cada ack_wait/2 mientras el handler siga vivo — 03-delivery.md §2.1."""
        interval = seconds(CONSUMER_DEFAULTS.ack_wait_ms) / 2
        while True:
            await asyncio.sleep(interval)
            try:
                await m.in_progress()
            except Exception:
                return  # el mensaje ya está resuelto

    # ─── DLQ ──────────────────────────────────────────────────────────────────

    async def _send_to_dlq(
        self,
        subject: str,
        event: FluxEvent[Any],
        consumer: str,
        attempts: int,
        reason: DlqReason,
        error: str,
    ) -> None:
        dlq_event = to_dlq_event(
            event, reason=reason, attempts=attempts, consumer=consumer, error=error
        )
        await self._js.publish(
            dlq_subject(subject),
            serialize(dlq_event),
            # El id lleva el consumidor: dos consumidores distintos del mismo evento
            # tienen derecho a un registro cada uno en la DLQ.
            headers={"Nats-Msg-Id": f"dlq-{event.id}-{consumer}"},
        )

    async def _safely_send_raw_to_dlq(
        self, subject: str, raw: bytes, consumer: str, error: str
    ) -> bool:
        """
        ¿Se pudo dejar constancia del POISON? Solo entonces procede el `term()`: hacer
        term sin registro en la DLQ borraría la única prueba de que un productor está
        publicando basura.
        """
        try:
            await self._send_raw_to_dlq(subject, raw, consumer, error)
            return True
        except Exception as e:
            self._log(
                "error", "[flux] no se pudo escribir el POISON de %s en la DLQ: %s", subject, e
            )
            return False

    async def _send_raw_to_dlq(
        self, subject: str, raw: bytes, consumer: str, error: str
    ) -> None:
        """Un POISON no tiene CloudEvent que preservar; se guarda el cuerpo crudo."""
        envelope_id = uuid7()
        envelope = {
            "specversion": "1.0",
            "id": envelope_id,
            "source": self._source,
            "type": "com.flux.system.poison.capturado.v1",
            "time": now_iso(),
            "datacontenttype": "application/json",
            "dataschema": "https://schemas.internal/system/poison/capturado/1.0.0.json",
            "correlationid": envelope_id,
            "tenantid": "system",
            "producerversion": self._opts.version,
            "dataclassification": "internal",
            "dlqreason": "poison",
            "dlqattempts": 1,
            "dlqconsumer": consumer,
            "dlqerror": error[:1024],
            "dlqtime": now_iso(),
            "data": {
                "originalSubject": subject,
                # Recortado para no superar el límite de 1 MiB del propio mensaje de DLQ.
                "rawBase64": base64.b64encode(raw).decode("ascii")[:700_000],
                "rawBytes": len(raw),
            },
        }
        await self._js.publish(
            dlq_subject(subject),
            json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
            headers={"Nats-Msg-Id": f"poison-{envelope_id}"},
        )

    # ─── provisión de streams ─────────────────────────────────────────────────

    async def _ensure_stream(self, domain: str) -> None:
        name = stream_name(domain)
        if name in self._ensured:
            return
        await self._add_stream_if_absent(
            name,
            [f"{domain}.>"],
            max_age=seconds(STREAM_DEFAULTS.max_age_ms),
            duplicate_window=seconds(STREAM_DEFAULTS.duplicate_window_ms),
        )
        self._ensured.add(name)

    async def _ensure_dlq_stream(self, domain: str) -> None:
        name = dlq_stream_name(domain)
        if name in self._ensured:
            return
        # Prefijo `dlq.`, no sufijo: si fuese sufijo encajaría con `<domain>.>` y el
        # stream principal capturaría sus propios muertos — 02-naming.md §3.1.
        await self._add_stream_if_absent(
            name, [f"dlq.{domain}.>"], max_age=seconds(STREAM_DEFAULTS.dlq_max_age_ms)
        )
        self._ensured.add(name)

    async def _add_stream_if_absent(
        self, name: str, subjects: list[str], **extra: Any
    ) -> None:
        try:
            await self._js.stream_info(name)
        except NotFoundError:
            await self._js.add_stream(
                config=StreamConfig(
                    name=name,
                    subjects=subjects,
                    storage=StorageType.FILE,
                    retention=RetentionPolicy.LIMITS,
                    discard=DiscardPolicy.OLD,
                    **extra,
                )
            )

    # ─── ciclo de vida ────────────────────────────────────────────────────────

    @property
    def connected(self) -> bool:
        return not self._nc.is_closed

    async def close(self) -> None:
        for task in list(self._pumps):
            task.cancel()
        for task in list(self._pumps):
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self._log("warning", "[flux] error al parar un consumidor: %s", e)
        self._pumps.clear()
        self._metrics.connection_state(ConnectionState.DISCONNECTED)
        # drain, no close: vacía lo pendiente antes de cortar. Los acks que no lleguen
        # provocan reentrega, y eso es correcto — 03-delivery.md §6.
        await self._nc.drain()

    def _log(self, level: str, message: str, *args: Any) -> None:
        if self._opts.logger is not None:
            getattr(self._opts.logger, level)(message, *args)


def _first(*values: Any) -> Any:
    """Primer valor no `None`. Equivalente al `??` encadenado del SDK de Node."""
    for v in values:
        if v is not None:
            return v
    return None


async def connect(
    *,
    servers: str | Sequence[str],
    service: str,
    environment: str,
    version: str,
    tenant_id: str = "system",
    tenant_isolation: TenantIsolation = "off",
    classification: DataClassification = "internal",
    schemas: Mapping[str, str] | None = None,
    schema_base_url: str | None = None,
    classifier: ClassifierOptions | None = None,
    signing: SigningOptions | None = None,
    validation: ValidationOptions | None = None,
    metrics: MetricsSink | None = None,
    pending_poll_ms: float = DEFAULT_PENDING_POLL_MS,
    credentials: Credentials | None = None,
    on_poison: Callable[[PoisonInfo], None] | None = None,
    on_dlq: Callable[[DlqInfo], None] | None = None,
    logger: logging.Logger | None = None,
) -> FluxBus:
    """
    Conecta al bus. Reconecta indefinidamente: sin reconexión, un reinicio del cluster
    obliga a reiniciar todos los servicios — 03-delivery.md §6.

    Sobre el jitter: `nats-py` no expone un parámetro para él (el cliente de Node sí,
    con `reconnectJitter`), así que se aplica aquí desordenando el `reconnect_time_wait`
    de cada proceso al conectar. Consigue lo que el jitter persigue de verdad — que mil
    servicios no reconecten en el mismo milisegundo y tiren el cluster que acaba de
    levantarse — aunque los reintentos de un mismo proceso queden equiespaciados.
    """
    options = ConnectOptions(
        servers=servers,
        service=service,
        environment=environment,
        version=version,
        tenant_id=tenant_id,
        tenant_isolation=tenant_isolation,
        classification=classification,
        schemas=dict(schemas or {}),
        schema_base_url=schema_base_url,
        classifier=classifier,
        signing=signing,
        validation=validation,
        metrics=metrics if metrics is not None else NO_METRICS,
        pending_poll_ms=pending_poll_ms,
        credentials=credentials,
        on_poison=on_poison,
        on_dlq=on_dlq,
        logger=logger,
    )

    sink = options.metrics

    # flux_connection_state: 1 conectado, 0 desconectado, 2 reconectando
    # — 08-observability.md §2.1. Sin estos callbacks el gauge valdría 1 hasta close() y
    # no diría nada, que es peor que no publicarlo.
    async def _reconectando() -> None:
        # Con max_reconnect_attempts=-1 una caída SIEMPRE es "reconectando".
        sink.connection_state(ConnectionState.RECONNECTING)

    async def _reconectado() -> None:
        sink.connection_state(ConnectionState.CONNECTED)

    async def _cerrada() -> None:
        sink.connection_state(ConnectionState.DISCONNECTED)

    creds = credentials or Credentials()
    auth: dict[str, Any] = {}
    if creds.creds:
        auth["user_credentials"] = creds.creds
    if creds.token:
        auth["token"] = creds.token
    if creds.user:
        auth["user"] = creds.user
    if creds.password:
        auth["password"] = creds.password

    nc = await nats.connect(
        servers=list(servers) if not isinstance(servers, str) else servers,
        name=f"{service}@{environment}",
        max_reconnect_attempts=-1,
        reconnect_time_wait=1.0 + random.random() * 0.5,
        allow_reconnect=True,
        disconnected_cb=_reconectando,
        reconnected_cb=_reconectado,
        closed_cb=_cerrada,
        **auth,
    )
    return FluxBus(nc, nc.jetstream(), options)
