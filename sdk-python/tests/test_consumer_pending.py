"""
`flux_consumer_pending` — specification/08-observability.md §2.3.

La métrica tiene DOS fuentes y el SDK DEBE usar las dos, porque cada una falla justo
donde la otra sirve:

  · los metadatos del mensaje entregado son gratis y frescos, pero **si el bucle del
    consumidor muere dejan de llegar mensajes** y el gauge se queda plano en su último
    valor — un panel mostraría una línea horizontal, indistinguible de "no pasa nada";
  · el sondeo al servidor cuesta una petición cada ~15 s y sigue creciendo. Es la señal.

Estos tests cubren las dos fuentes sin broker: uno con metadatos falsos y otro con un
JetStream falso que además falla, para fijar que un fallo del sondeo NO afecta al consumo.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

# `flux.client` es el único módulo que necesita `nats-py`. Es una dependencia OBLIGATORIA
# del paquete, no un extra, pero la suite se ejecuta también sin instalar nada (ver
# conftest.py), así que su ausencia salta estos tests en vez de tumbar la recolección.
pytest.importorskip("nats", reason="flux.client necesita nats-py")

from flux.client import ConnectOptions, FluxBus  # noqa: E402
from flux.metrics import NO_METRICS, InMemoryMetrics  # noqa: E402
from flux.protocol import DEFAULT_PENDING_POLL_MS  # noqa: E402

SUBJECT = "pedidos.pedido.v1.creado"
DURABLE = "facturacion-api__pedidos_pedido_v1_creado"
STREAM = "PEDIDOS"
GAUGE = f'flux_consumer_pending{{consumer="{DURABLE}",subject="{SUBJECT}"}}'


class _JetStreamFalso:
    """Devuelve `num_pending` en cada llamada; una excepción en la lista se lanza."""

    def __init__(self, *valores: object) -> None:
        self._valores = list(valores)
        self.llamadas = 0

    async def consumer_info(self, stream: str, durable: str) -> SimpleNamespace:
        valor = self._valores[min(self.llamadas, len(self._valores) - 1)]
        self.llamadas += 1
        if isinstance(valor, Exception):
            raise valor
        return SimpleNamespace(num_pending=valor)


def _bus(js: object, metrics: object, **extra: object) -> FluxBus:
    opciones = ConnectOptions(
        servers="nats://localhost:4222",
        service="facturacion-api",
        environment="produccion",
        version="1.0.0",
        metrics=metrics,  # type: ignore[arg-type]
        **extra,  # type: ignore[arg-type]
    )
    # El constructor no toca la conexión: solo guarda `nc` para `connected` y `close`.
    return FluxBus(SimpleNamespace(is_closed=False), js, opciones)  # type: ignore[arg-type]


# ─── Fuente 1: los metadatos de cada entrega ─────────────────────────────────


class TestMetadatosDeLaEntrega:
    def test_lee_el_intento_y_los_pendientes_del_mismo_objeto(self):
        msg = SimpleNamespace(metadata=SimpleNamespace(num_delivered=3, num_pending=17))
        assert FluxBus._delivery_info(msg) == (3, 17)

    def test_sin_metadatos_no_hay_pendientes_y_el_intento_es_1(self):
        # Un mensaje que no venga de JetStream no tiene metadatos. No es un fallo: el
        # sondeo sigue alimentando el gauge.
        class _SinMeta:
            @property
            def metadata(self):
                raise ValueError("no es un mensaje de JetStream")

        assert FluxBus._delivery_info(_SinMeta()) == (1, None)

    def test_num_delivered_cero_se_normaliza_a_1(self):
        msg = SimpleNamespace(metadata=SimpleNamespace(num_delivered=0, num_pending=0))
        assert FluxBus._delivery_info(msg) == (1, 0)


# ─── Fuente 2: el sondeo periódico ───────────────────────────────────────────


async def _sondear_hasta(bus: FluxBus, sink: InMemoryMetrics, intervalo: float) -> None:
    """Arranca el sondeo, espera a que emita algo y lo para."""
    parado = asyncio.Event()
    tarea = asyncio.create_task(bus._poll_pending(STREAM, DURABLE, SUBJECT, parado, intervalo))
    try:
        for _ in range(200):
            if GAUGE in sink.snapshot()["gauges"]:
                return
            await asyncio.sleep(0.005)
        raise AssertionError("el sondeo no emitió flux_consumer_pending")
    finally:
        parado.set()
        await asyncio.wait_for(tarea, timeout=1)


class TestSondeo:
    def test_emite_el_num_pending_del_servidor(self):
        sink = InMemoryMetrics()
        bus = _bus(_JetStreamFalso(42), sink)
        asyncio.run(_sondear_hasta(bus, sink, 0.005))
        assert sink.snapshot()["gauges"][GAUGE] == 42

    def test_un_fallo_del_sondeo_no_mata_el_bucle(self):
        # Un fallo del sondeo NO DEBE afectar al consumo: es telemetría. Si el primer
        # error terminara la tarea, la métrica se apagaría para siempre tras un hipo del
        # broker — y nadie se enteraría, porque el consumidor seguiría consumiendo.
        sink = InMemoryMetrics()
        js = _JetStreamFalso(RuntimeError("broker no disponible"), 9)
        bus = _bus(js, sink)
        asyncio.run(_sondear_hasta(bus, sink, 0.005))
        assert sink.snapshot()["gauges"][GAUGE] == 9
        assert js.llamadas >= 2

    def test_para_en_cuanto_se_desuscribe(self):
        # Esperar sobre el Event y no dormir el intervalo entero: si no, desuscribirse
        # tardaría hasta 15 s en notarse y `close()` se quedaría colgado.
        async def escenario() -> None:
            parado = asyncio.Event()
            bus = _bus(_JetStreamFalso(1), InMemoryMetrics())
            tarea = asyncio.create_task(
                bus._poll_pending(STREAM, DURABLE, SUBJECT, parado, 30.0)
            )
            await asyncio.sleep(0)
            parado.set()
            await asyncio.wait_for(tarea, timeout=1)

        asyncio.run(escenario())


class TestIntervalo:
    def test_el_default_es_el_del_protocolo(self):
        bus = _bus(_JetStreamFalso(0), InMemoryMetrics())
        assert bus._pending_poll_seconds() == DEFAULT_PENDING_POLL_MS / 1000

    def test_cero_lo_desactiva(self):
        bus = _bus(_JetStreamFalso(0), InMemoryMetrics(), pending_poll_ms=0)
        assert bus._pending_poll_seconds() is None

    def test_sin_sumidero_de_metricas_no_se_sondea(self):
        # Sería una petición cada 15 s por consumidor para tirar el resultado.
        bus = _bus(_JetStreamFalso(0), NO_METRICS)
        assert bus._pending_poll_seconds() is None
