"""
Validación L3 — specification/00-protocol.md §5.

Réplica de `sdk-node/test/validation.test.ts`: los mismos casos, porque un payload que
el SDK de Node rechaza y el de Python acepta convierte el contrato en una sugerencia.
Más los casos propios del port: que el bundle se resuelve SIN red y que un fallo al
consumir se clasifica PERMANENT.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import pytest

# `jsonschema` es un extra OPCIONAL, así que su ausencia debe SALTAR estos tests, no
# tumbar la recolección entera. Una dependencia opcional que rompe toda la suite no es
# opcional: el resto del SDK (envelope, naming, clasificación, métricas) no la necesita
# para nada.
#
# Va a NIVEL DE MÓDULO y no dentro de cada test porque el validador se construye al
# importar: sin el skip, el fallo ocurre en la fase de COLECCIÓN y pytest aborta con
# exit 2 sin llegar a ejecutar ningún test de ningún fichero. Pasó exactamente eso con
# `cryptography` en test_signing.py.
pytest.importorskip(
    "jsonschema",
    reason='la validación L3 necesita el extra: pip install "flux-sdk[validation]"',
)

from flux import ErrorClass, FluxEvent, build_event, classify  # noqa: E402
from flux.validation import (  # noqa: E402
    SchemaBundle,
    SchemaNotFoundError,
    SchemaValidationError,
    ValidationOptions,
    create_validator,
    load_bundle,
    schema_uri_for,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
BUNDLE_PATH = REPO_ROOT / "schemas" / "bundle.json"

BUNDLE = load_bundle(BUNDLE_PATH)
SUBJECT = "pedidos.pedido.v1.creado"
URI = schema_uri_for(BUNDLE, SUBJECT)

VALIDO = {
    "pedidoId": "ped-123",
    "clienteId": "cli-987",
    "aggregateVersion": 1,
    "totalCents": 9990,
    "moneda": "EUR",
    "lineas": [{"sku": "ABC-1", "cantidad": 2, "precioUnitarioCents": 4995}],
}


def evento(data: dict, dataschema: str | None = None) -> FluxEvent:
    return build_event(
        subject=SUBJECT,
        data=data,
        id="01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
        source="/produccion/pedidos-api",
        producerversion="3.4.1",
        tenantid="acme",
        dataclassification="internal",
        dataschema=dataschema or URI,
        correlationid="01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
    )


def estricto():
    return create_validator(ValidationOptions(mode="strict", bundle=BUNDLE))


# ─── El bundle ───────────────────────────────────────────────────────────────


class TestBundle:
    def test_indexa_el_subject_hacia_su_uri_de_dataschema(self):
        assert URI, "el bundle debe resolver el subject de ejemplo"
        assert re.fullmatch(
            r"https://schemas\.internal/pedidos/pedido/creado/\d+\.\d+\.\d+\.json", URI
        )

    def test_el_id_del_esquema_coincide_con_la_clave_del_bundle(self):
        assert BUNDLE.schemas[URI]["$id"] == URI

    def test_los_esquemas_declaran_draft_2020_12(self):
        # ⚠️ La trampa que documenta 00-protocol.md §5: un validador configurado para
        # draft-07 NO falla con un error de versión, falla con `no schema with key or ref
        # ".../2020-12/schema"`, que no dice nada. Si algún día un esquema declarara otro
        # draft, mejor enterarse aquí.
        for uri, esquema in BUNDLE.schemas.items():
            assert esquema["$schema"] == "https://json-schema.org/draft/2020-12/schema", uri

    def test_ignora_los_metadatos_del_fichero_generado(self):
        # `$comment`, `generatedFrom` y `count` son documentación del fichero, no del
        # contrato: añadir una clave nueva no debe romper a ningún SDK.
        crudo = json.loads(BUNDLE_PATH.read_text(encoding="utf-8"))
        assert set(crudo) > {"subjects", "schemas"}
        assert SchemaBundle.from_dict(crudo).subjects == BUNDLE.subjects

    def test_un_subject_desconocido_no_resuelve(self):
        assert schema_uri_for(BUNDLE, "pedidos.pedido.v9.inventado") is None


# ─── strict ──────────────────────────────────────────────────────────────────


class TestStrict:
    def test_un_payload_valido_pasa(self):
        estricto()(evento(VALIDO), SUBJECT)  # no lanza

    def test_falta_un_campo_requerido_lanza(self):
        sin_total = {k: v for k, v in VALIDO.items() if k != "totalCents"}
        with pytest.raises(SchemaValidationError):
            estricto()(evento(sin_total), SUBJECT)

    def test_tipo_incorrecto_lanza(self):
        # El caso que la spec llama el más peligroso: "9990" en vez de 9990.
        with pytest.raises(SchemaValidationError):
            estricto()(evento({**VALIDO, "totalCents": "9990"}), SUBJECT)

    def test_campo_desconocido_lanza(self):
        # additionalProperties: false. Un campo mal escrito debe fallar, no colarse.
        with pytest.raises(SchemaValidationError):
            estricto()(evento({**VALIDO, "totalCemts": 9990}), SUBJECT)

    def test_patron_incumplido_lanza(self):
        with pytest.raises(SchemaValidationError):
            estricto()(evento({**VALIDO, "moneda": "euros"}), SUBJECT)

    def test_reporta_todos_los_errores_no_solo_el_primero(self):
        # Reportar de uno en uno convierte arreglarlo en tres despliegues.
        with pytest.raises(SchemaValidationError) as capturado:
            estricto()(
                evento({**VALIDO, "totalCents": "x", "moneda": "euros", "cantidad": 1}),
                SUBJECT,
            )
        assert len(capturado.value.errors) >= 2, capturado.value.errors
        # Y el mensaje los lleva todos: un error que hay que sacar del `.errors` a mano no
        # aparece en el log del despliegue que falló.
        for detalle in capturado.value.errors:
            assert detalle in str(capturado.value)

    def test_valida_dentro_de_los_arrays(self):
        malo = {**VALIDO, "lineas": [{"sku": "ABC-1", "cantidad": 0, "precioUnitarioCents": 1}]}
        with pytest.raises(SchemaValidationError) as capturado:
            estricto()(evento(malo), SUBJECT)
        assert any("/lineas/0" in e for e in capturado.value.errors), capturado.value.errors

    def test_esquema_ausente_del_bundle_lanza_schema_not_found(self):
        with pytest.raises(SchemaNotFoundError):
            estricto()(evento(VALIDO, "https://schemas.internal/no/existe/1.0.0.json"), SUBJECT)


# ─── warn y off ──────────────────────────────────────────────────────────────


class TestWarnYOff:
    def test_warn_registra_pero_no_lanza(self, caplog):
        v = create_validator(ValidationOptions(mode="warn", bundle=BUNDLE))
        with caplog.at_level(logging.WARNING, logger="flux"):
            v(evento({**VALIDO, "totalCents": "x"}), SUBJECT)  # no lanza
        assert len(caplog.records) == 1
        assert "no cumple su esquema" in caplog.text

    def test_warn_tampoco_lanza_si_falta_el_esquema(self, caplog):
        v = create_validator(ValidationOptions(mode="warn", bundle=BUNDLE))
        with caplog.at_level(logging.WARNING, logger="flux"):
            v(evento(VALIDO, "https://schemas.internal/no/existe/1.0.0.json"), SUBJECT)
        assert "sin esquema" in caplog.text

    def test_off_no_compila_nada(self):
        # L2 no paga el coste de L3: sin validador no hay esquemas compilados ni import
        # de `jsonschema`.
        assert create_validator(ValidationOptions(mode="off", bundle=BUNDLE)) is None
        assert create_validator(ValidationOptions()) is None

    def test_el_default_es_off_y_sin_validar_al_consumir(self):
        assert ValidationOptions().mode == "off"
        assert ValidationOptions().on_consume is False

    def test_strict_sin_bundle_falla_con_un_mensaje_accionable(self):
        with pytest.raises(ValueError, match=r"bundle-schemas\.mjs"):
            create_validator(ValidationOptions(mode="strict"))

    def test_un_modo_desconocido_falla_al_arrancar(self):
        # Un typo en el modo no puede significar "no valides nada en silencio": ese fallo
        # solo se ve el día que alguien publica basura y nadie la para.
        with pytest.raises(ValueError, match="strict"):
            create_validator(ValidationOptions(mode="estricto", bundle=BUNDLE))  # type: ignore[arg-type]


# ─── Clasificación y resolución ──────────────────────────────────────────────


class TestClasificacion:
    def test_un_fallo_de_esquema_es_permanent(self):
        # 00-protocol.md §5: el evento es sintácticamente correcto pero incumple su
        # contrato, y reintentarlo dará exactamente el mismo resultado. La clase la
        # declara el propio error, así que no depende de `unknown_error_policy`.
        c = classify(SchemaValidationError(SUBJECT, URI, ["/totalCents no es integer"]))
        assert c.error_class is ErrorClass.PERMANENT
        assert c.code == "SCHEMA_INVALID"

    def test_un_esquema_ausente_tambien_es_permanent(self):
        c = classify(SchemaNotFoundError(SUBJECT, URI))
        assert c.error_class is ErrorClass.PERMANENT
        assert c.code == "SCHEMA_NOT_FOUND"

    def test_los_refs_se_resuelven_sin_red(self):
        # El bundle se despliega CON el servicio y nunca se resuelve el `dataschema` por
        # HTTP: validar está en la ruta caliente, y una caché con TTL abriría una ventana
        # en la que dos servicios validan contra versiones distintas del mismo esquema
        # (00-protocol.md §5). Un `$ref` que no esté en el bundle debe fallar diciendo qué
        # regenerar, no salir a la red.
        cojo = SchemaBundle(
            subjects={SUBJECT: URI},
            schemas={
                URI: {
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "$id": URI,
                    "$ref": "https://schemas.internal/no/empaquetado/1.0.0.json",
                }
            },
        )
        v = create_validator(ValidationOptions(mode="strict", bundle=cojo))
        with pytest.raises(Exception, match=r"bundle-schemas\.mjs"):
            v(evento(VALIDO), SUBJECT)
