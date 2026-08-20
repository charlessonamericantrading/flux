"""
L3 en el cliente — specification/00-protocol.md §5.

Aquí se comprueba lo único que de verdad convierte un contrato roto en un fallo del
productor: que `publish()` **no publica** un payload que viola su esquema. Todo lo demás
—modos, mensajes, clasificación— vive en `test_validation.py`.

Necesita las dos piezas opcionales del entorno (`jsonschema` para validar, `nats-py`
porque `flux.client` lo importa), así que ambos `importorskip` van a nivel de módulo: una
dependencia que puede faltar nunca debe tumbar la recolección de la suite entera.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("jsonschema", reason='pip install "flux-sdk[validation]"')
pytest.importorskip("nats", reason="flux.client necesita nats-py")

from flux.client import ConnectOptions, FluxBus  # noqa: E402
from flux.metrics import InMemoryMetrics  # noqa: E402
from flux.validation import (  # noqa: E402
    SchemaValidationError,
    ValidationOptions,
    load_bundle,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
BUNDLE = load_bundle(REPO_ROOT / "schemas" / "bundle.json")
SUBJECT = "pedidos.pedido.v1.creado"

VALIDO = {
    "pedidoId": "ped-123",
    "clienteId": "cli-987",
    "aggregateVersion": 1,
    "totalCents": 9990,
    "moneda": "EUR",
    "lineas": [{"sku": "ABC-1", "cantidad": 2, "precioUnitarioCents": 4995}],
}


class _JetStreamFalso:
    """Lo mínimo que toca `publish()`: el stream ya existe y se registra lo publicado."""

    def __init__(self) -> None:
        self.publicados: list[tuple[str, bytes]] = []

    async def stream_info(self, name: str) -> SimpleNamespace:
        return SimpleNamespace(config=SimpleNamespace(name=name))

    async def publish(self, subject: str, payload: bytes, headers=None) -> SimpleNamespace:
        self.publicados.append((subject, payload))
        return SimpleNamespace(seq=len(self.publicados))


def _bus(mode: str, metrics: InMemoryMetrics) -> tuple[FluxBus, _JetStreamFalso]:
    js = _JetStreamFalso()
    opciones = ConnectOptions(
        servers="nats://localhost:4222",
        service="pedidos-api",
        environment="produccion",
        version="3.4.1",
        tenant_id="acme",
        metrics=metrics,
        validation=ValidationOptions(mode=mode, bundle=BUNDLE),  # type: ignore[arg-type]
    )
    return FluxBus(SimpleNamespace(is_closed=False), js, opciones), js  # type: ignore[arg-type]


class TestPublishEnStrict:
    def test_un_payload_valido_se_publica(self):
        bus, js = _bus("strict", InMemoryMetrics())
        evento = asyncio.run(bus.publish(SUBJECT, VALIDO, aggregate_id="ped-123"))
        assert len(js.publicados) == 1
        # Y el dataschema sale del bundle, no del `<major>.0.0` aproximado: sin bundle el
        # SDK no sabe el MINOR real, y L3 exige apuntar al esquema contra el que valida.
        assert evento.dataschema == BUNDLE.subjects[SUBJECT]

    def test_un_payload_invalido_no_llega_al_broker(self):
        # El requisito entero de L3: el fallo ocurre en el servicio que lo provocó, no en
        # un consumidor de otro equipo la semana que viene.
        metrics = InMemoryMetrics()
        bus, js = _bus("strict", metrics)
        with pytest.raises(SchemaValidationError):
            asyncio.run(bus.publish(SUBJECT, {**VALIDO, "totalCents": "9990"}))
        assert js.publicados == [], "no debe publicarse nada que no valide"

        contadores = metrics.snapshot()["counters"]
        assert contadores[f'flux_events_published_total{{outcome="invalid_schema",subject="{SUBJECT}"}}'] == 1
        assert f'flux_events_published_total{{outcome="ok",subject="{SUBJECT}"}}' not in contadores


class TestPublishEnWarn:
    def test_publica_igual_y_lo_registra(self, caplog):
        # `warn` existe para introducir validación en un ecosistema en marcha sin romper
        # nada el primer día.
        import logging

        bus, js = _bus("warn", InMemoryMetrics())
        with caplog.at_level(logging.WARNING, logger="flux"):
            asyncio.run(bus.publish(SUBJECT, {**VALIDO, "totalCents": "9990"}))
        assert len(js.publicados) == 1
        assert "no cumple su esquema" in caplog.text


class TestSinValidacion:
    def test_off_publica_cualquier_cosa(self):
        # Es L2: el atributo `dataschema` sigue siendo informativo y nadie lo comprueba.
        bus, js = _bus("off", InMemoryMetrics())
        asyncio.run(bus.publish(SUBJECT, {"cualquier": "cosa"}))
        assert len(js.publicados) == 1

    def test_off_con_bundle_sigue_resolviendo_el_dataschema(self):
        # El bundle resuelve el MINOR aunque no se valide: son dos usos independientes.
        bus, _ = _bus("off", InMemoryMetrics())
        assert bus._schema_for(SUBJECT) == BUNDLE.subjects[SUBJECT]

    def test_el_mapa_explicito_gana_sobre_el_bundle(self):
        js = _JetStreamFalso()
        bus = FluxBus(
            SimpleNamespace(is_closed=False),  # type: ignore[arg-type]
            js,  # type: ignore[arg-type]
            ConnectOptions(
                servers="nats://localhost:4222",
                service="pedidos-api",
                environment="produccion",
                version="3.4.1",
                schemas={SUBJECT: "https://schemas.internal/a/mano/2.1.0.json"},
                validation=ValidationOptions(mode="off", bundle=BUNDLE),
            ),
        )
        assert bus._schema_for(SUBJECT) == "https://schemas.internal/a/mano/2.1.0.json"
