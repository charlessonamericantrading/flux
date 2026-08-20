#!/usr/bin/env python3
"""
Arnés de conformidad cruzada — SDK de Python.
Contrato: conformance/harness/README.md

Lee UNA operación por stdin, escribe UN resultado por stdout, sale con 0 siempre.
Deliberadamente delgado: toda lógica aquí es lógica que no está en el SDK y que el
runner, por tanto, no verifica.
"""

from __future__ import annotations

import base64
import json
import sys
from dataclasses import replace
from typing import Any

from flux.envelope import FluxEvent, build_event, parse_event, serialize, to_dlq_event
from flux.signing import Signer, SigningOptions, create_signer, create_verifier


def _construir(e: dict[str, Any]) -> FluxEvent[Any]:
    """El arnés NO rellena nada: todos los atributos vienen del vector, o no serían comparables."""
    return build_event(
        subject=e["subject"],
        data=e["data"],
        id=e["id"],
        source=e["source"],
        time=e["time"],
        dataschema=e["dataschema"],
        correlationid=e["correlationid"],
        tenantid=e["tenantid"],
        producerversion=e["producerversion"],
        dataclassification=e["dataclassification"],
        aggregate_id=e.get("aggregateId"),
        causationid=e.get("causationid"),
        partitionkey=e.get("partitionkey"),
        traceparent=e.get("traceparent"),
        tracestate=e.get("tracestate"),
    )


def _firmante(signing: dict[str, Any]) -> Signer:
    firmante = create_signer(
        SigningOptions(private_key_pem=signing["privateKeyPem"], key_id=signing["keyId"])
    )
    if firmante is None:
        # `create_signer` devuelve None sin clave privada: firmar es opcional en el SDK,
        # pero un vector de firma sin clave es una entrada inválida, no un caso que falle.
        raise ValueError("la operación de firma requiere signing.privateKeyPem")
    return firmante


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def _ejecutar(entrada: dict[str, Any]) -> dict[str, Any]:
    op = entrada.get("op")

    if op == "build":
        return {"ok": True, "bytes": _b64(serialize(_construir(entrada["event"])))}

    if op == "dlq":
        evento = _construir(entrada["event"])
        if entrada.get("signFirst") and entrada.get("signing"):
            evento = _firmante(entrada["signing"]).sign(evento)
        d = entrada["dlq"]
        con_dlq = to_dlq_event(
            evento,
            reason=d["reason"],
            attempts=d["attempts"],
            consumer=d["consumer"],
            error=d["error"],
        )
        # `dlqtime` lo fija el vector: si lo pusiera el SDK, los bytes no serían
        # comparables entre ejecuciones, y mucho menos entre lenguajes.
        return {"ok": True, "bytes": _b64(serialize(replace(con_dlq, dlqtime=d["dlqtime"])))}

    if op == "sign":
        firmado = _firmante(entrada["signing"]).sign(_construir(entrada["event"]))
        return {"ok": True, "bytes": _b64(serialize(firmado))}

    if op == "verify":
        evento = parse_event(base64.b64decode(entrada["bytes"]))
        verificador = create_verifier(
            SigningOptions(
                public_keys=entrada.get("publicKeys") or {},
                verify=entrada.get("mode") or "require",
            )
        )
        try:
            if verificador is not None:  # None en modo `off`: no hay nada que comprobar.
                verificador.check(evento)
            return {"ok": True}
        except Exception as error:
            return {"ok": False, "code": getattr(error, "code", None) or "VERIFY_FAILED"}

    if op == "parse":
        parse_event(base64.b64decode(entrada["bytes"]))
        return {"ok": True}

    return {"ok": False, "code": "UNSUPPORTED_OP", "detail": op}


def main() -> None:
    # Se lee en binario: la entrada es UTF-8 y la consola de Windows no lo es por defecto,
    # así que decodificarla con la codificación local rompería justo el vector de acentos.
    entrada = json.loads(sys.stdin.buffer.read().decode("utf-8"))
    try:
        salida = _ejecutar(entrada)
    except Exception as error:
        # Un fallo de la operación se REPORTA, no se propaga: exit != 0 significaría que el
        # arnés está roto, no que el caso falló.
        salida = {
            "ok": False,
            "code": getattr(error, "code", None) or type(error).__name__,
            "detail": str(error),
        }
    sys.stdout.write(json.dumps(salida))


if __name__ == "__main__":
    main()
