"""
Aislamiento entre tenants — specification/09-multitenancy.md §3.

La regla que se prueba aquí es la única que, al fallar, no produce ningún error: un
consumidor sin filtro ve los eventos de todos los tenants y todo parece funcionar. Por
eso `resolve_tenant_filter` vive en un módulo sin dependencias y se prueba sin broker.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from flux import (
    TenantIsolationError,
    build_event,
    resolve_tenant_filter,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
PROTOCOL = json.loads((REPO_ROOT / "protocol.json").read_text(encoding="utf-8"))

SUBJECT = "pedidos.pedido.v1.creado"

BASE = dict(
    subject=SUBJECT,
    id="01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
    source="/produccion/pedidos-api",
    producerversion="3.4.1",
    dataclassification="internal",
    dataschema="https://schemas.internal/pedidos/pedido/creado/1.0.0.json",
    correlationid="01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
)


class TestPropagacionDeTenant:
    def test_un_evento_lleva_el_tenant_que_se_le_da(self):
        assert build_event(**BASE, tenantid="acme", data={}).tenantid == "acme"

    def test_system_es_legitimo_para_eventos_de_plataforma(self):
        # Pero NO es un comodín ni un default cuando el tenant real se desconoce: si no
        # se sabe de quién es un evento, el bug está aguas arriba — §5.
        assert build_event(**BASE, tenantid="system", data={}).tenantid == "system"

    def test_el_tenant_es_obligatorio_en_la_construccion(self):
        # Sin default silencioso: `build_event` lo exige como keyword.
        with pytest.raises(TypeError):
            build_event(**BASE, data={})  # type: ignore[call-arg]


class TestFiltroDeTenant:
    def test_el_de_la_suscripcion_gana_sobre_el_de_la_conexion(self):
        # Un servicio multi-tenant abre una conexión y una suscripción por tenant.
        assert resolve_tenant_filter(SUBJECT, "acme", "globex", "off") == "globex"

    def test_sin_filtro_en_la_suscripcion_manda_el_de_la_conexion(self):
        assert resolve_tenant_filter(SUBJECT, "acme", None, "off") == "acme"

    def test_sin_tenant_en_ningun_sitio_no_hay_filtro_en_modo_off(self):
        assert resolve_tenant_filter(SUBJECT, None, None, "off") is None

    @pytest.mark.parametrize("conexion", ["system", None, ""])
    def test_system_no_cuenta_como_filtro(self, conexion):
        # "system" es la AUSENCIA de tenant, no un tenant. Aceptarlo como filtro dejaría
        # fuera todos los eventos de negocio y —peor— daría por satisfecho el modo
        # estricto sin filtrar nada — §5.
        assert resolve_tenant_filter(SUBJECT, conexion, None, "off") is None

    def test_system_en_la_suscripcion_cae_al_de_la_conexion(self):
        assert resolve_tenant_filter(SUBJECT, "acme", "system", "off") == "acme"


class TestModoEstricto:
    def test_suscribirse_sin_tenant_es_un_error(self):
        # Un filtro que hay que acordarse de poner es un filtro que alguien olvidará, y
        # el fallo no produce ningún error: produce un incidente de privacidad que se
        # descubre semanas después — §3.
        with pytest.raises(TenantIsolationError) as exc:
            resolve_tenant_filter(SUBJECT, None, None, "strict")
        assert SUBJECT in str(exc.value)
        assert "TODOS los tenants" in str(exc.value)

    def test_el_tenant_system_no_satisface_el_modo_estricto(self):
        with pytest.raises(TenantIsolationError):
            resolve_tenant_filter(SUBJECT, "system", None, "strict")

    def test_con_tenant_configurado_el_modo_estricto_no_estorba(self):
        assert resolve_tenant_filter(SUBJECT, "acme", None, "strict") == "acme"
        assert resolve_tenant_filter(SUBJECT, None, "globex", "strict") == "globex"

    def test_el_error_es_de_configuracion_no_de_evento(self):
        # No es POISON ni PERMANENT: no hay ningún evento involucrado todavía. Rompe el
        # arranque, que es donde debe romper.
        with pytest.raises(RuntimeError):
            resolve_tenant_filter(SUBJECT, None, None, "strict")


class TestContrato:
    def test_el_sdk_cumple_los_tres_requisitos_de_protocol_json(self):
        # 1. tenantId a nivel de conexión → ConnectOptions.tenant_id.
        # 2. filtrar ANTES del handler → FluxBus._dispatch ackea y descarta antes de
        #    invocarlo; el evento de otro tenant no es un fallo, no es para nosotros.
        # 3. modo strict → el resto de esta clase.
        assert len(PROTOCOL["multitenancy"]["sdkRequirements"]) == 3

        # `client` es el único módulo que necesita `nats-py`. Sin él, el resto del
        # aislamiento sigue probándose: por eso la regla vive en `flux.tenant`.
        pytest.importorskip("nats", reason="ConnectOptions vive en client.py")
        from flux.client import ConnectOptions

        campos = ConnectOptions.__dataclass_fields__
        assert "tenant_id" in campos
        assert campos["tenant_isolation"].default == "off"

    def test_el_default_es_el_modelo_a(self):
        # Filtrado en consumidor: sin cambios de topología, y los eventos de plataforma
        # fluyen sin fontanería extra. El aislamiento duro es el Modelo B — §2.
        assert PROTOCOL["multitenancy"]["defaultModel"] == "A"
