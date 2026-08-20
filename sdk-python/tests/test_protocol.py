"""
Tests unitarios del SDK de Python. No necesitan broker.

Lo que se comprueba aquí es lo que un agente o un humano puede romper sin que ningún
test de integración se entere: naming, forma del envelope y política de clasificación.
Varios casos leen `protocol.json` directamente para que una divergencia entre el SDK y
el contrato falle en CI en vez de en producción.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from flux import (
    ALLOWED_ROOT_ATTRIBUTES,
    CONSUMER_DEFAULTS,
    MAX_MESSAGE_BYTES,
    STREAM_DEFAULTS,
    ClassifierOptions,
    EnvelopeError,
    ErrorClass,
    EventContext,
    FluxEvent,
    InvalidSubjectError,
    PermanentError,
    PoisonError,
    RetryableError,
    build_event,
    classify,
    create_classifier,
    current_context,
    dlq_stream_name,
    dlq_subject,
    durable_name,
    is_dlq_subject,
    is_valid_subject,
    parse_event,
    parse_subject,
    serialize,
    source_uri,
    stream_name,
    strip_dlq_extensions,
    subject_to_type,
    to_dlq_event,
    use_context,
    uuid7,
)

# `context_from_event` es interno al SDK (el de Node tampoco lo exporta en index.ts),
# pero se testea porque es donde se decide que el `id` entrante pasa a ser el
# `causationid` saliente.
from flux.context import context_from_event

REPO_ROOT = Path(__file__).resolve().parents[2]
PROTOCOL = json.loads((REPO_ROOT / "protocol.json").read_text(encoding="utf-8"))

SUBJECT = "pedidos.pedido.v1.creado"


def _event(**overrides) -> FluxEvent:
    base = dict(
        subject=SUBJECT,
        data={"pedidoId": "ped-123", "aggregateVersion": 1},
        id="01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
        source="/produccion/pedidos-api",
        producerversion="3.4.1",
        tenantid="acme",
        dataclassification="confidential",
        dataschema="https://schemas.internal/pedidos/pedido/creado/1.2.0.json",
        correlationid="01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
    )
    base.update(overrides)
    return build_event(**base)


# ─── Naming ──────────────────────────────────────────────────────────────────


class TestSubject:
    def test_parsea_los_cuatro_tokens(self):
        p = parse_subject("logistica.envio.v2.entrega-fallida")
        assert (p.domain, p.aggregate, p.major, p.event) == (
            "logistica",
            "envio",
            2,
            "entrega-fallida",
        )

    @pytest.mark.parametrize("subject", PROTOCOL["naming"]["subject"]["valid"])
    def test_acepta_los_validos_de_protocol_json(self, subject):
        assert is_valid_subject(subject)

    @pytest.mark.parametrize("subject", PROTOCOL["naming"]["subject"]["invalid"])
    def test_rechaza_los_invalidos_de_protocol_json(self, subject):
        assert not is_valid_subject(subject)

    def test_el_patron_es_el_de_protocol_json(self):
        # Si divergen, protocol.json manda: es lo que consumen los demás SDKs.
        from flux import SUBJECT_PATTERN

        assert SUBJECT_PATTERN.pattern == PROTOCOL["naming"]["subject"]["pattern"]

    def test_las_mayusculas_dan_un_mensaje_sobre_el_subject_fantasma(self):
        # El mensaje importa: NATS acepta el publish sin error y el evento simplemente
        # no existe para nadie (conformance/cases/broker-invariants.json).
        with pytest.raises(InvalidSubjectError) as exc:
            parse_subject("Pedidos.Pedido.V1.Creado")
        assert "minúsculas" in str(exc.value)
        assert "case-sensitive" in str(exc.value)

    def test_un_quinto_token_rompe_los_wildcards(self):
        with pytest.raises(InvalidSubjectError) as exc:
            parse_subject("pedidos.pedido.v1.creado.retry")
        assert "4 tokens" in str(exc.value)

    @pytest.mark.parametrize(
        "subject",
        [
            "pedidos.crear-pedido",  # comando, y le faltan tokens
            "pedidos.pedido.1.creado",  # falta la 'v'
            "pedidos.pedido.v0.creado",  # el mayor empieza en 1
            "pedidos.pedido_v1.creado",  # guion bajo prohibido
            "pedidos..v1.creado",  # token vacío
            "pedidos.pedido.v1.creado ",  # espacio
        ],
    )
    def test_rechaza_formas_rotas(self, subject):
        assert not is_valid_subject(subject)


class TestDerivaciones:
    def test_type_es_reverse_dns_con_la_version_al_final(self):
        assert subject_to_type(SUBJECT) == "com.flux.pedidos.pedido.creado.v1"
        assert re.match(PROTOCOL["naming"]["type"]["pattern"], subject_to_type(SUBJECT))

    def test_stream_sin_puntos(self):
        assert stream_name("pedidos") == "EVT_PEDIDOS"
        assert dlq_stream_name("pedidos") == "DLQ_PEDIDOS"
        assert stream_name("linea-envio") == "EVT_LINEA_ENVIO"
        for char in PROTOCOL["naming"]["stream"]["forbiddenChars"]:
            assert char not in stream_name("linea-envio")
        assert re.match(PROTOCOL["naming"]["stream"]["eventPattern"], stream_name("pedidos"))
        assert re.match(PROTOCOL["naming"]["stream"]["dlqPattern"], dlq_stream_name("pedidos"))

    def test_durable_reversible_y_sin_puntos(self):
        durable = durable_name("facturacion-api", SUBJECT)
        assert durable == PROTOCOL["naming"]["durableConsumer"]["example"]
        assert re.match(PROTOCOL["naming"]["durableConsumer"]["pattern"], durable)
        for char in PROTOCOL["naming"]["durableConsumer"]["forbiddenChars"]:
            assert char not in durable
        # Reversible: partiendo por `__` se recuperan servicio y subject.
        service, _, encoded = durable.partition("__")
        assert service == "facturacion-api"
        assert encoded == SUBJECT.replace(".", "_")

    def test_durable_valida_el_subject(self):
        with pytest.raises(InvalidSubjectError):
            durable_name("facturacion-api", "Pedidos.pedido.v1.creado")

    def test_dlq_es_prefijo_y_queda_fuera_del_stream_principal(self):
        assert dlq_subject(SUBJECT) == "dlq.pedidos.pedido.v1.creado"
        assert is_dlq_subject(dlq_subject(SUBJECT))
        # La razón de que sea prefijo: un sufijo encajaría con `pedidos.>` y el stream
        # principal se tragaría sus propios muertos (02-naming.md §3.1).
        assert not dlq_subject(SUBJECT).startswith("pedidos.")

    def test_source_uri(self):
        uri = source_uri("produccion", "pedidos-api")
        assert uri == "/produccion/pedidos-api"
        assert re.match(
            PROTOCOL["cloudevents"]["requiredAttributes"]["source"]["pattern"], uri
        )


class TestUuid7:
    def test_version_y_variante(self):
        value = uuid7()
        assert re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", value
        )

    def test_monotono_dentro_del_mismo_milisegundo(self):
        # 01-envelope.md §2.2 se apoya en esto para reconstruir historiales desde la DLQ.
        ids = [uuid7() for _ in range(500)]
        assert ids == sorted(ids)
        assert len(set(ids)) == len(ids)


# ─── Configuración canónica ──────────────────────────────────────────────────


class TestConsumerDefaults:
    def test_coinciden_con_protocol_json(self):
        c = PROTOCOL["consumer"]
        assert CONSUMER_DEFAULTS.ack_wait_ms == c["ackWaitSeconds"] * 1000
        assert CONSUMER_DEFAULTS.max_deliver == c["maxDeliver"]
        assert list(CONSUMER_DEFAULTS.backoff_ms) == c["backoffMs"]
        assert CONSUMER_DEFAULTS.max_ack_pending == c["maxAckPending"]
        assert CONSUMER_DEFAULTS.ack_policy == c["ackPolicy"]

    def test_ack_wait_es_backoff_cero(self):
        # LA trampa: JetStream sobrescribe ack_wait con backoff[0] sin devolver error
        # (03-delivery.md §2.1). Si esta invariante se rompe, cualquier handler que
        # tarde más que backoff[0] se ejecuta en concurrencia consigo mismo.
        assert CONSUMER_DEFAULTS.ack_wait_ms == CONSUMER_DEFAULTS.backoff_ms[0]

    def test_backoff_y_max_deliver_cuadran(self):
        # 1 entrega inicial + N reintentos, uno por entrada de backoff.
        assert len(CONSUMER_DEFAULTS.backoff_ms) == CONSUMER_DEFAULTS.max_deliver - 1

    def test_presupuesto_de_handler_de_al_menos_30s(self):
        assert CONSUMER_DEFAULTS.backoff_ms[0] >= 30_000

    def test_tiempo_total_hasta_la_dlq(self):
        # ≈51 min 30 s. Es una decisión de producto (cuánto se tolera reintentar antes
        # de que un humano se entere), no un detalle técnico — 03-delivery.md §2.
        total = sum(CONSUMER_DEFAULTS.backoff_ms) / 1000
        assert total == PROTOCOL["consumer"]["totalTimeToDlqSeconds"]

    def test_stream_defaults(self):
        assert STREAM_DEFAULTS.duplicate_window_ms == 120_000
        assert STREAM_DEFAULTS.dlq_max_age_ms > STREAM_DEFAULTS.max_age_ms
        assert MAX_MESSAGE_BYTES == PROTOCOL["data"]["maxMessageBytes"]


# ─── Envelope ────────────────────────────────────────────────────────────────


class TestBuildEvent:
    def test_rellena_lo_que_el_desarrollador_no_escribe(self):
        e = _event()
        assert e.specversion == "1.0"
        assert e.datacontenttype == "application/json"
        assert e.type == "com.flux.pedidos.pedido.creado.v1"

    def test_time_es_rfc3339_utc_con_milisegundos(self):
        assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$", _event().time)

    def test_time_explicito_se_convierte_a_utc(self):
        e = _event(time=datetime(2026, 8, 20, 10, 25, 39, 412_000, tzinfo=timezone.utc))
        assert e.time == "2026-08-20T10:25:39.412Z"

    def test_aggregate_id_va_al_subject_de_cloudevents(self):
        # ⚠️ El `subject` del envelope es el ID DEL AGREGADO, no el subject de NATS.
        e = _event(aggregate_id="ped-123")
        assert e.subject == "ped-123"
        assert e.partitionkey == "ped-123"  # por convención, 01-envelope.md §3.2

    def test_partition_key_explicita_gana(self):
        assert _event(aggregate_id="ped-123", partitionkey="cli-987").partitionkey == "cli-987"

    @pytest.mark.parametrize("data", [[{"a": 1}], "texto", 42, None, True])
    def test_data_debe_ser_objeto_en_la_raiz(self, data):
        with pytest.raises(EnvelopeError):
            _event(data=data)

    def test_es_inmutable(self):
        with pytest.raises(Exception):
            _event().id = "otro"  # type: ignore[misc]


class TestSerialize:
    def test_omite_los_atributos_ausentes(self):
        raw = json.loads(serialize(_event()))
        assert "causationid" not in raw
        assert "subject" not in raw
        assert None not in raw.values()

    def test_solo_atributos_permitidos(self):
        assert set(json.loads(serialize(_event(aggregate_id="ped-1")))) <= ALLOWED_ROOT_ATTRIBUTES

    def test_limite_de_1_mib_menciona_claim_check(self):
        gordo = _event(data={"blob": "x" * (MAX_MESSAGE_BYTES + 10)})
        with pytest.raises(EnvelopeError) as exc:
            serialize(gordo)
        assert "claim-check" in str(exc.value)

    def test_justo_por_debajo_del_limite_pasa(self):
        assert len(serialize(_event(data={"blob": "x" * (MAX_MESSAGE_BYTES - 1000)}))) < (
            MAX_MESSAGE_BYTES
        )


class TestParseEvent:
    def test_round_trip(self):
        original = _event(aggregate_id="ped-123")
        assert parse_event(serialize(original)) == original

    def test_json_malformado(self):
        with pytest.raises(PoisonError) as exc:
            parse_event(b"{no soy json")
        assert exc.value.code == "MALFORMED_JSON"

    def test_no_es_un_objeto(self):
        with pytest.raises(PoisonError) as exc:
            parse_event(b"[1,2,3]")
        assert exc.value.code == "NOT_AN_OBJECT"

    def test_specversion_desconocida(self):
        raw = json.loads(serialize(_event()))
        raw["specversion"] = "0.3"
        with pytest.raises(PoisonError) as exc:
            parse_event(json.dumps(raw).encode())
        assert exc.value.code == "UNSUPPORTED_SPECVERSION"

    @pytest.mark.parametrize("attr", ["id", "source", "type", "time", "dataschema", "data"])
    def test_falta_un_atributo_obligatorio(self, attr):
        raw = json.loads(serialize(_event()))
        del raw[attr]
        with pytest.raises(PoisonError) as exc:
            parse_event(json.dumps(raw).encode())
        assert exc.value.code == "MISSING_REQUIRED_ATTRIBUTE"
        assert attr in str(exc.value)

    def test_content_type_no_soportado(self):
        raw = json.loads(serialize(_event()))
        raw["datacontenttype"] = "application/xml"
        with pytest.raises(PoisonError) as exc:
            parse_event(json.dumps(raw).encode())
        assert exc.value.code == "UNSUPPORTED_CONTENT_TYPE"

    def test_atributo_raiz_desconocido_es_poison(self):
        # Los atributos raíz están cerrados: permitir metadatos ad-hoc acaba en JSON
        # dentro de un string y el envelope deja de ser interoperable (01-envelope.md §3.3).
        raw = json.loads(serialize(_event()))
        raw["miMetadato"] = "algo"
        with pytest.raises(PoisonError) as exc:
            parse_event(json.dumps(raw).encode())
        assert exc.value.code == "UNKNOWN_ROOT_ATTRIBUTE"
        assert "`data`" in str(exc.value)

    def test_las_extensiones_dlq_si_estan_permitidas(self):
        # Un mensaje leído de la DLQ tiene que poder parsearse para poder reproducirlo.
        muerto = to_dlq_event(
            _event(), reason="permanent", attempts=1, consumer="c__s", error="boom"
        )
        assert parse_event(serialize(muerto)).dlqreason == "permanent"


class TestDlq:
    def test_conserva_el_evento_original_intacto(self):
        original = _event(aggregate_id="ped-123")
        muerto = to_dlq_event(
            original,
            reason="permanent",
            attempts=1,
            consumer="facturacion-api__pedidos_pedido_v1_creado",
            error="PEDIDO_YA_CANCELADO: el pedido ped-123 estaba cancelado",
        )
        assert muerto.dlqreason == "permanent"
        assert muerto.dlqattempts == 1
        assert muerto.dlqconsumer == "facturacion-api__pedidos_pedido_v1_creado"
        assert re.match(r"^\d{4}-\d{2}-\d{2}T.*Z$", muerto.dlqtime or "")
        # Sin wrapper: el replay es quitar las dlq* y republicar (04-errors.md §3).
        assert strip_dlq_extensions(muerto) == original

    def test_el_replay_conserva_el_id(self):
        # Regenerarlo rompería la idempotencia de todos los consumidores aguas abajo.
        original = _event()
        muerto = to_dlq_event(original, reason="poison", attempts=1, consumer="c", error="x")
        assert strip_dlq_extensions(muerto).id == original.id

    def test_el_error_se_recorta_a_1_kib(self):
        muerto = to_dlq_event(
            _event(), reason="permanent", attempts=1, consumer="c", error="x" * 5000
        )
        assert len(muerto.dlqerror or "") == 1024

    def test_dlqattempts_distingue_permanent_de_retryable_agotado(self):
        assert to_dlq_event(
            _event(), reason="permanent", attempts=1, consumer="c", error="x"
        ).dlqattempts == 1
        assert to_dlq_event(
            _event(), reason="retryable", attempts=6, consumer="c", error="x"
        ).dlqattempts == 6


# ─── Clasificación de errores ────────────────────────────────────────────────


class TestClassify:
    def test_los_errores_tipados_ganan(self):
        c = classify(RetryableError("proveedor 503", retry_after_ms=5000))
        assert c.error_class is ErrorClass.RETRYABLE
        assert c.retry_after_ms == 5000

        c = classify(PermanentError("pedido ya cancelado", code="PEDIDO_YA_CANCELADO"))
        assert c.error_class is ErrorClass.PERMANENT
        assert c.code == "PEDIDO_YA_CANCELADO"

        assert classify(PoisonError("json roto")).error_class is ErrorClass.POISON

    def test_sin_code_se_usa_el_nombre_de_la_clase(self):
        assert classify(RetryableError("x")).code == "RetryableError"

    @pytest.mark.parametrize("status", PROTOCOL["errors"]["classes"]["retryable"]["httpStatus"])
    def test_http_retryable(self, status):
        e = RuntimeError("dependencia caída")
        e.status = status  # type: ignore[attr-defined]
        c = classify(e)
        assert c.error_class is ErrorClass.RETRYABLE
        assert c.code == f"HTTP_{status}"

    @pytest.mark.parametrize("status", PROTOCOL["errors"]["classes"]["permanent"]["httpStatus"])
    def test_http_permanent(self, status):
        # Reintentar un 404 es gastar 51 minutos para obtener la misma respuesta.
        e = RuntimeError("no encontrado")
        e.status_code = status  # type: ignore[attr-defined]
        assert classify(e).error_class is ErrorClass.PERMANENT

    def test_lee_retry_after_de_la_respuesta(self):
        class Response:
            status_code = 429
            headers = {"retry-after": "12"}

        e = RuntimeError("rate limit")
        e.response = Response()  # type: ignore[attr-defined]
        c = classify(e)
        assert c.error_class is ErrorClass.RETRYABLE
        assert c.retry_after_ms == 12_000

    def test_retry_after_con_fecha_http_se_ignora(self):
        class Response:
            status_code = 503
            headers = {"retry-after": "Wed, 21 Oct 2026 07:28:00 GMT"}

        e = RuntimeError("mantenimiento")
        e.response = Response()  # type: ignore[attr-defined]
        assert classify(e).retry_after_ms is None  # manda el backoff canónico

    @pytest.mark.parametrize(
        "exc",
        [ConnectionResetError, ConnectionRefusedError, BrokenPipeError],
    )
    def test_errores_de_red_son_retryable(self, exc):
        import errno

        codes = {
            ConnectionResetError: errno.ECONNRESET,
            ConnectionRefusedError: errno.ECONNREFUSED,
            BrokenPipeError: errno.EPIPE,
        }
        e = exc(codes[exc], "red")
        c = classify(e)
        assert c.error_class is ErrorClass.RETRYABLE
        assert c.code in PROTOCOL["errors"]["classes"]["retryable"]["syscallCodes"]

    def test_default_de_lo_desconocido_es_permanent(self):
        # Un evento en la DLQ es recuperable; una cola atascada en hora punta, no.
        assert PROTOCOL["errors"]["default"] == "permanent"
        c = classify(ValueError("algo que nadie previó"))
        assert c.error_class is ErrorClass.PERMANENT
        assert c.code == "UNKNOWN"

    def test_la_politica_de_lo_desconocido_es_configurable(self):
        agresivo = create_classifier(ClassifierOptions(unknown_error_policy="retryable"))
        assert agresivo(ValueError("x")).error_class is ErrorClass.RETRYABLE

    def test_timeout_por_defecto_retryable(self):
        assert classify(TimeoutError()).error_class is ErrorClass.RETRYABLE
        assert classify(TimeoutError()).code == "TIMEOUT"

    def test_timeout_puede_hacerse_permanent(self):
        estricto = create_classifier(ClassifierOptions(timeout_policy="permanent"))
        assert estricto(TimeoutError()).error_class is ErrorClass.PERMANENT

    def test_las_reglas_de_la_aplicacion_van_primero(self):
        from flux import Classification

        def regla(e: BaseException):
            if "deadlock" in str(e):
                return Classification(ErrorClass.RETRYABLE, "DB_DEADLOCK")
            return None

        clasificador = create_classifier(ClassifierOptions(rules=[regla]))
        e = RuntimeError("deadlock detected")
        e.status = 400  # type: ignore[attr-defined]  # la regla gana sobre el status
        assert clasificador(e).code == "DB_DEADLOCK"


# ─── Contexto ────────────────────────────────────────────────────────────────


class TestContexto:
    def test_sin_contexto_no_hay_nada(self):
        assert current_context() is None

    def test_se_deriva_del_evento_entrante(self):
        entrante = _event(causationid="01924f8d-1a2b-7c3d-8e4f-5a6b7c8d9e01")
        ctx = context_from_event(entrante)
        # El id del entrante pasa a ser la causa del saliente.
        assert ctx.causationid == entrante.id
        assert ctx.correlationid == entrante.correlationid
        assert ctx.tenantid == entrante.tenantid

    def test_se_restaura_al_salir(self):
        ctx = EventContext(correlationid="c-1", causationid="e-1", tenantid="acme")
        with use_context(ctx):
            assert current_context() == ctx
        assert current_context() is None
