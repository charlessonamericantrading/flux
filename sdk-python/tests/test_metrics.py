"""
Métricas — specification/08-observability.md.

Réplica de `sdk-node/test/metrics.test.ts` más los casos que el contrato pide y que solo
un test puede sostener: que los nombres y las etiquetas son los de `protocol.json`, y que
ninguna etiqueta es `tenantid`.
"""

from __future__ import annotations

import inspect
import json
import re
from pathlib import Path

import pytest

from flux import CONSUMER_DEFAULTS, DEFAULT_PENDING_POLL_MS
from flux.metrics import (
    DURATION_BUCKETS,
    NO_METRICS,
    ConnectionState,
    InMemoryMetrics,
    MetricsSink,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
PROTOCOL = json.loads((REPO_ROOT / "protocol.json").read_text(encoding="utf-8"))
OBSERVABILITY = PROTOCOL["observability"]

SUBJECT = "pedidos.pedido.v1.creado"
CONSUMER = "facturacion-api__pedidos_pedido_v1_creado"

#: Gramática del formato de exposición: `nombre{etiqueta="valor",…} valor`.
#: Las comillas dentro de un valor solo valen escapadas — de ahí `(?:[^"\\]|\\.)*`.
LINEA = re.compile(
    r'^[a-zA-Z_:][a-zA-Z0-9_:]*'
    r'(\{[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\]|\\.)*"'
    r'(,[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\]|\\.)*")*\})?'
    r" -?[0-9.]+$"
)


def _con_todas_las_metricas() -> InMemoryMetrics:
    m = InMemoryMetrics()
    m.event_published(SUBJECT, "ok")
    m.event_consumed(SUBJECT, CONSUMER, "ok")
    m.handler_duration(SUBJECT, CONSUMER, 0.4)
    m.event_dlq(SUBJECT, CONSUMER, "permanent", "HTTP_404")
    m.event_retried(SUBJECT, CONSUMER, 3)
    m.consumer_pending(SUBJECT, CONSUMER, 42)
    m.connection_state(ConnectionState.CONNECTED)
    return m


def _series(render: str) -> dict[str, set[str]]:
    """Del texto expuesto a `{nombre_de_métrica: {etiquetas}}`."""
    fuera: dict[str, set[str]] = {}
    for linea in render.splitlines():
        if not linea or linea.startswith("#"):
            continue
        casada = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})? ", linea)
        assert casada is not None, f"línea no parseable: {linea!r}"
        nombre, etiquetas = casada.group(1), casada.group(2) or ""
        fuera.setdefault(nombre, set()).update(
            re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)="', etiquetas)
        )
    return fuera


# ─── El contrato ─────────────────────────────────────────────────────────────


class TestContrato:
    def test_los_nombres_son_los_de_protocol_json(self):
        # Si dos SDKs nombran distinto lo mismo, agrupar deja de funcionar en cuanto el
        # ecosistema es polyglot — que es siempre (08-observability.md §1).
        expuestas = set(_series(_con_todas_las_metricas().render()))
        histograma = "flux_event_handler_duration_seconds"
        # El histograma se expone en tres familias derivadas del mismo nombre.
        expuestas -= {f"{histograma}_bucket", f"{histograma}_sum", f"{histograma}_count"}
        expuestas.add(histograma)
        assert expuestas == set(OBSERVABILITY["metrics"])

    def test_las_etiquetas_son_las_de_protocol_json(self):
        series = _series(_con_todas_las_metricas().render())
        for nombre, definicion in OBSERVABILITY["metrics"].items():
            if definicion["type"] == "histogram":
                # `le` es del formato del histograma, no una etiqueta de la métrica.
                assert series[f"{nombre}_bucket"] - {"le"} == set(definicion["labels"])
                assert series[f"{nombre}_count"] == set(definicion["labels"])
            else:
                assert series[nombre] == set(definicion["labels"])

    def test_ninguna_etiqueta_es_de_alta_cardinalidad(self):
        # NUNCA se etiqueta por tenantid, id ni correlationid: un tenant nuevo no debe
        # crear series temporales nuevas. Para eso están las trazas — §2.2.
        prohibidas = set(OBSERVABILITY["forbiddenLabels"])
        for etiquetas in _series(_con_todas_las_metricas().render()).values():
            assert not (etiquetas & prohibidas)

    def test_el_sink_no_acepta_un_diccionario_de_etiquetas(self):
        # La interfaz tiene parámetros nombrados y no un `labels: dict` genérico porque
        # es justo por ahí por donde se cuela un `tenantid`. La cardinalidad no avisa:
        # funciona con tres tenants en desarrollo y muere con diez mil en producción.
        prohibidas = set(OBSERVABILITY["forbiddenLabels"]) | {"labels", "tags", "attributes"}
        for nombre, metodo in inspect.getmembers(MetricsSink, inspect.isfunction):
            if nombre.startswith("_"):
                continue
            parametros = set(inspect.signature(metodo).parameters) - {"self"}
            assert not (parametros & prohibidas), f"{nombre} acepta {parametros & prohibidas}"


class TestSondeoDePendientes:
    def test_el_intervalo_por_defecto_es_el_de_protocol_json(self):
        # 08-observability.md §2.3: el gauge tiene DOS fuentes y el SDK DEBE usar las dos.
        # Los metadatos del mensaje son gratis pero fallan justo donde importa —si el
        # bucle muere dejan de llegar mensajes y la métrica se queda plana—, así que el
        # sondeo periódico no es redundante: es la señal.
        assert (
            DEFAULT_PENDING_POLL_MS
            == OBSERVABILITY["metrics"]["flux_consumer_pending"]["defaultPollMs"]
        )

    def test_la_metrica_se_declara_sondeada_desde_el_servidor(self):
        # Si alguien "optimizara" el sondeo dejando solo los metadatos, este test recuerda
        # que el contrato dice de dónde sale el dato.
        assert OBSERVABILITY["metrics"]["flux_consumer_pending"]["source"] == "polled-from-server"


class TestBuckets:
    def test_el_ultimo_bucket_es_el_ack_wait(self):
        # 08-observability.md §3: un handler en el bucket superior está a punto de que su
        # mensaje se reentregue mientras aún se ejecuta. Si el bucket deja de coincidir
        # con el plazo real, mide algo que no le importa a nadie — y el día que alguien
        # cambie `ack_wait` nadie se acordará de este número si no falla un test.
        assert DURATION_BUCKETS[-1] == CONSUMER_DEFAULTS.ack_wait_ms / 1000

    def test_coinciden_con_protocol_json(self):
        assert list(DURATION_BUCKETS) == OBSERVABILITY["durationBucketsSeconds"]

    def test_estan_ordenados_de_forma_ascendente(self):
        assert list(DURATION_BUCKETS) == sorted(DURATION_BUCKETS)
        assert len(set(DURATION_BUCKETS)) == len(DURATION_BUCKETS)


# ─── Recolector ──────────────────────────────────────────────────────────────


class TestRecolector:
    def test_cuenta_publicaciones_por_subject_y_resultado(self):
        m = InMemoryMetrics()
        m.event_published(SUBJECT, "ok")
        m.event_published(SUBJECT, "ok")
        m.event_published(SUBJECT, "invalid_schema")
        counters = m.snapshot()["counters"]
        assert counters[f'flux_events_published_total{{outcome="ok",subject="{SUBJECT}"}}'] == 2
        assert (
            counters[f'flux_events_published_total{{outcome="invalid_schema",subject="{SUBJECT}"}}']
            == 1
        )

    def test_las_etiquetas_se_ordenan_para_que_la_clave_sea_estable(self):
        # Sin orden estable, la misma serie temporal aparecería con dos claves según el
        # orden en que se construyó el diccionario.
        a, b = InMemoryMetrics(), InMemoryMetrics()
        a.event_dlq("s", "c", "permanent", "X")
        b.event_dlq("s", "c", "permanent", "X")
        assert list(a.snapshot()["counters"]) == list(b.snapshot()["counters"])

    def test_el_histograma_acumula_en_todos_los_buckets_que_superan_el_valor(self):
        m = InMemoryMetrics()
        m.handler_duration("s", "c", 0.03)  # cae por encima de 0.025
        salida = m.render()
        assert re.search(r'_bucket\{[^}]*le="0\.025"\} 0', salida)
        assert re.search(r'_bucket\{[^}]*le="0\.05"\} 1', salida)
        assert re.search(r'_bucket\{[^}]*le="\+Inf"\} 1', salida)
        assert re.search(r"_count\{[^}]*\} 1", salida)

    def test_un_handler_lento_cae_en_el_bucket_del_ack_wait(self):
        # La señal que el bucket superior existe para dar: 29 s cuenta en le="30" y nada
        # más allá; el evento está a un segundo de ejecutarse consigo mismo.
        m = InMemoryMetrics()
        m.handler_duration("s", "c", 29)
        salida = m.render()
        assert re.search(r'_bucket\{[^}]*le="10"\} 0', salida)
        assert re.search(r'_bucket\{[^}]*le="30"\} 1', salida)

    def test_un_gauge_sin_etiquetas_no_deja_llaves_vacias(self):
        m = InMemoryMetrics()
        m.connection_state(ConnectionState.CONNECTED)
        assert re.search(r"^flux_connection_state 1$", m.render(), re.M)

    def test_las_lineas_tienen_forma_valida_de_prometheus(self):
        for linea in _con_todas_las_metricas().render().splitlines():
            if not linea or linea.startswith("#"):
                continue
            assert LINEA.match(linea), f"línea no válida para Prometheus: {linea!r}"

    def test_escapa_las_comillas_de_los_valores_de_etiqueta(self):
        # Un `code` con comillas rompería el formato de exposición y Prometheus
        # descartaría el scrape ENTERO, no solo esa línea.
        m = InMemoryMetrics()
        m.event_dlq("s", "c", "permanent", 'con "comillas" y \\ barra')
        for linea in m.render().splitlines():
            if linea.startswith("flux_events_dlq_total"):
                assert LINEA.match(linea), f"el escapado no produjo una línea válida: {linea!r}"

    def test_un_salto_de_linea_en_un_code_no_parte_la_exposicion(self):
        m = InMemoryMetrics()
        m.event_dlq("s", "c", "poison", "dos\nlíneas")
        lineas = [x for x in m.render().splitlines() if x and not x.startswith("#")]
        assert len(lineas) == 1
        assert LINEA.match(lineas[0])

    def test_el_recolector_vacio_no_expone_nada(self):
        # Ni cabeceras `# TYPE` de métricas que nunca se han observado: un scrape con
        # familias vacías confunde más de lo que informa.
        assert InMemoryMetrics().render() == ""


class TestNoMetrics:
    def test_no_lanza_y_no_guarda_nada(self):
        # Es el default: un SDK no debe imponer un backend de métricas.
        NO_METRICS.event_published("s", "ok")
        NO_METRICS.event_consumed("s", "c", "poison")
        NO_METRICS.handler_duration("s", "c", 1.0)
        NO_METRICS.event_dlq("s", "c", "poison", "X")
        NO_METRICS.event_retried("s", "c", 1)
        NO_METRICS.consumer_pending("s", "c", 0)
        NO_METRICS.connection_state(ConnectionState.DISCONNECTED)

    @pytest.mark.parametrize(
        "metodo",
        [
            "event_published",
            "event_consumed",
            "handler_duration",
            "event_dlq",
            "event_retried",
            "consumer_pending",
            "connection_state",
        ],
    )
    def test_implementa_toda_la_interfaz(self, metodo):
        # Un sink al que le falte un método fallaría en la ruta caliente del consumidor,
        # no al arrancar.
        assert callable(getattr(NO_METRICS, metodo))
        assert callable(getattr(InMemoryMetrics(), metodo))
