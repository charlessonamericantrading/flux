"""
Firma Ed25519 — specification/07-signing.md.

Réplica de `sdk-node/test/signing.test.ts`: los mismos casos, porque una firma que
verifica en Node y no en Python (o al revés) rompería la premisa entera de la extensión
—que la autenticidad viaja con el evento, no con el canal.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from flux import (
    FluxEvent,
    PoisonError,
    build_event,
    parse_event,
    serialize,
    to_dlq_event,
)
from flux.signing import (
    SigningKeyError,
    SigningOptions,
    create_signer,
    create_verifier,
    generate_key_pair,
)

# `cryptography` es un extra OPCIONAL, así que su ausencia debe SALTAR estos tests, no
# tumbar la recolección entera. Una dependencia opcional que rompe toda la suite no es
# opcional: el resto del SDK (envelope, naming, clasificación) no la necesita para nada.
#
# Va a nivel de módulo y no dentro de cada test porque `generate_key_pair()` se llama al
# importar: sin el skip, el fallo ocurre en la fase de COLECCIÓN y pytest aborta con
# exit 2 sin llegar a ejecutar ningún test de ningún fichero.
pytest.importorskip(
    "cryptography",
    reason='la firma Ed25519 necesita el extra: pip install "flux-sdk[signing]"',
)

KEY_ID = "pedidos-api-1"
PAR = generate_key_pair()

BASE = dict(
    subject="pedidos.pedido.v1.creado",
    id="01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
    source="/produccion/pedidos-api",
    time="2025-08-20T10:25:39.410Z",
    producerversion="3.4.1",
    tenantid="acme",
    dataclassification="internal",
    dataschema="https://schemas.internal/pedidos/pedido/creado/1.0.0.json",
    correlationid="01924f8e-7c3a-7b2d-9e14-3f8a1c9d0e55",
)


def evento(data: dict | None = None) -> FluxEvent:
    return build_event(**BASE, data=data if data is not None else {"pedidoId": "ped-123"})


@pytest.fixture(scope="module")
def signer():
    return create_signer(SigningOptions(private_key_pem=PAR.private_key_pem, key_id=KEY_ID))


@pytest.fixture(scope="module")
def verifier():
    return create_verifier(
        SigningOptions(public_keys={KEY_ID: PAR.public_key_pem}, verify="require")
    )


class TestFirma:
    def test_anade_signkeyid_y_signature_antes_de_data(self, signer):
        firmado = signer.sign(evento())
        claves = list(firmado.to_dict())

        assert firmado.signkeyid == KEY_ID
        assert firmado.signature is not None
        # base64url SIN padding: ni `+`, ni `/`, ni `=` — 07-signing.md §4.
        assert set(firmado.signature) <= set(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        )
        assert claves[-1] == "data", "`data` sigue siendo el último — 01-envelope.md §6"
        assert claves.index("signature") < claves.index("data")

    def test_una_firma_valida_verifica(self, signer, verifier):
        verifier.check(signer.sign(evento()))

    def test_es_determinista(self, signer):
        # Solo es cierto porque 01-envelope.md §1.1, §2.2 y §6 fijan una única
        # representación en bytes. Sin ellas, firmar sería imposible entre lenguajes.
        assert signer.sign(evento()).signature == signer.sign(evento()).signature

    def test_sobrevive_a_un_round_trip_de_serializacion(self, signer, verifier):
        verifier.check(parse_event(serialize(signer.sign(evento()))))

    def test_firmar_no_muta_el_evento_original(self, signer):
        original = evento()
        signer.sign(original)
        assert original.signature is None and original.signkeyid is None


class TestDeteccionDeManipulacion:
    def test_alterar_data_invalida_la_firma(self, signer, verifier):
        firmado = signer.sign(evento())
        with pytest.raises(PoisonError) as exc:
            verifier.check(replace(firmado, data={"pedidoId": "ped-999"}))
        assert exc.value.code == "INVALID_SIGNATURE"

    def test_alterar_el_tenantid_invalida_la_firma(self, signer, verifier):
        # El caso que la ACL del broker no cubre: un evento sacado del stream, editado y
        # reinyectado — 07-signing.md §1.
        firmado = signer.sign(evento())
        with pytest.raises(PoisonError) as exc:
            verifier.check(replace(firmado, tenantid="otro"))
        assert exc.value.code == "INVALID_SIGNATURE"

    def test_cambiar_signkeyid_no_permite_eludir_la_verificacion(self, signer, verifier):
        # signkeyid va DENTRO de lo firmado justo para esto — 07-signing.md §5.
        firmado = signer.sign(evento())
        with pytest.raises(PoisonError) as exc:
            verifier.check(replace(firmado, signkeyid="otro-1"))
        assert exc.value.code == "UNKNOWN_SIGNING_KEY"

    def test_una_firma_de_otra_clave_no_verifica(self, verifier):
        otra = generate_key_pair()
        impostor = create_signer(
            SigningOptions(private_key_pem=otra.private_key_pem, key_id=KEY_ID)
        )
        with pytest.raises(PoisonError) as exc:
            verifier.check(impostor.sign(evento()))
        assert exc.value.code == "INVALID_SIGNATURE"

    def test_una_firma_que_ni_siquiera_es_base64url_es_invalida(self, signer, verifier):
        # No debe propagarse el error de decodificación: para el operador el hecho es el
        # mismo —esos bytes no los firmó esa clave— y el código tiene que ser estable.
        firmado = signer.sign(evento())
        with pytest.raises(PoisonError) as exc:
            verifier.check(replace(firmado, signature="no-es-una-firma!!"))
        assert exc.value.code == "INVALID_SIGNATURE"


class TestDlqYReplay:
    def test_la_firma_sigue_verificando_tras_pasar_por_la_dlq(self, signer, verifier):
        # Las extensiones dlq* se añaden DESPUÉS de firmar y no están cubiertas. Si la
        # verificación no las ignorara, todo evento en la DLQ parecería manipulado.
        en_dlq = to_dlq_event(
            signer.sign(evento()),
            reason="permanent",
            attempts=1,
            consumer="facturacion-api__pedidos_pedido_v1_creado",
            error="PEDIDO_YA_CANCELADO",
        )
        verifier.check(en_dlq)

    def test_un_evento_reproducido_conserva_su_firma_valida(self, signer, verifier):
        # El replay redistribuye un hecho ya emitido, no crea uno nuevo — 07-signing.md §5.1.
        en_dlq = to_dlq_event(
            signer.sign(evento()), reason="retryable", attempts=6, consumer="c", error="x"
        )
        verifier.check(parse_event(serialize(en_dlq)))


class TestModosDeVerificacion:
    def test_require_un_evento_sin_firma_es_poison(self, verifier):
        with pytest.raises(PoisonError) as exc:
            verifier.check(evento())
        assert exc.value.code == "MISSING_SIGNATURE"

    def test_warn_registra_pero_acepta(self, caplog):
        v = create_verifier(
            SigningOptions(public_keys={KEY_ID: PAR.public_key_pem}, verify="warn")
        )
        with caplog.at_level("WARNING", logger="flux"):
            v.check(evento())
        assert len(caplog.records) == 1
        assert "sin firma" in caplog.records[0].getMessage()

    def test_off_no_construye_verificador(self):
        # No se paga lo que no se usa: en modo `off` ni siquiera se importa `cryptography`.
        assert create_verifier(SigningOptions(verify="off")) is None
        assert create_verifier(SigningOptions()) is None


class TestGestionDeClaves:
    def test_firmar_sin_key_id_falla_con_un_mensaje_accionable(self):
        with pytest.raises(SigningKeyError) as exc:
            create_signer(SigningOptions(private_key_pem=PAR.private_key_pem))
        assert "key_id" in str(exc.value)

    def test_sin_clave_privada_no_hay_firmante(self):
        assert create_signer(SigningOptions()) is None

    def test_rechaza_una_clave_que_no_sea_ed25519(self):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        rsa_pem = (
            rsa.generate_private_key(public_exponent=65537, key_size=2048)
            .private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            .decode("ascii")
        )
        with pytest.raises(SigningKeyError, match="Ed25519"):
            create_signer(SigningOptions(private_key_pem=rsa_pem, key_id="x-1"))

    def test_verificar_sin_claves_publicas_falla_explicando_la_retencion(self):
        with pytest.raises(SigningKeyError, match="RETIRADAS"):
            create_verifier(SigningOptions(verify="require"))

    def test_una_clave_retirada_sigue_verificando_si_se_conserva_la_publica(self):
        # Retirar una clave impide EMITIR con ella, no VERIFICAR lo ya emitido. Tratarla
        # como inválida convertiría una rotación rutinaria en la invalidación retroactiva
        # de todo el historial — 07-signing.md §6.
        vieja, nueva = generate_key_pair(), generate_key_pair()
        firmado = create_signer(
            SigningOptions(private_key_pem=vieja.private_key_pem, key_id="pedidos-api-1")
        ).sign(evento())

        v = create_verifier(
            SigningOptions(
                public_keys={
                    "pedidos-api-1": vieja.public_key_pem,  # retirada, conservada
                    "pedidos-api-2": nueva.public_key_pem,  # activa
                },
                verify="require",
            )
        )
        v.check(firmado)


class TestEnvelope:
    def test_signkeyid_y_signature_son_atributos_raiz_permitidos(self, signer):
        # Sin esto, `parse_event` daría UNKNOWN_ROOT_ATTRIBUTE y un evento firmado sería
        # POISON para su propio consumidor.
        firmado = signer.sign(evento())
        assert parse_event(serialize(firmado)) == firmado

    def test_las_dlq_van_despues_de_la_firma_en_el_cable(self, signer):
        # El orden de claves es normativo (01-envelope.md §6) y aquí importa dos veces:
        # las dlq* se añaden después de firmar, así que quitarlas tiene que devolver
        # exactamente los bytes que se firmaron.
        en_dlq = to_dlq_event(
            signer.sign(evento()), reason="poison", attempts=1, consumer="c", error="x"
        )
        claves = list(en_dlq.to_dict())
        assert claves.index("signkeyid") < claves.index("signature")
        assert claves.index("signature") < claves.index("dlqreason")
        assert claves[-1] == "data"
