"""Un RECEPTOR de las entregas del gateway, en Python: verifica `Flux-Signature`.

Contrato NORMATIVO: specification/12-edge.md §4.2. Lo ejecutan las pruebas del gateway contra los vectores de protocol.json.

    resultado = verificar_flux_signature(request.headers.get("Flux-Signature", ""), request.get_data(), [os.environ["FLUX_SECRETO"]])

`request.get_data()` son los BYTES en bruto: NUNCA `json.loads` + `json.dumps` antes de verificar. Devuelve "ok", "ausente", "formato",
"firma" o "tolerancia". Con cualquier otro valor: 401 y no ejecutes nada. Con "ok": guarda el evento DEDUPLICANDO por `Idempotency-Key`
y responde 2xx en cuanto esté GUARDADO.
"""

import hashlib
import hmac
import re
import time

_HEX64 = re.compile(r"[0-9a-f]{64}")
_ENTERO = re.compile(r"[0-9]{1,15}")


def verificar_flux_signature(cabecera: str, cuerpo: bytes, secretos: list, ahora: float | None = None, tolerancia: int = 300) -> str:
    if not cabecera:
        return "ausente"
    ahora = time.time() if ahora is None else ahora
    t = None
    v1 = []
    veces = 0
    for campo in cabecera.split(","):
        k, igual, v = campo.partition("=")
        if not igual or not k.strip():
            return "formato"
        k, v = k.strip(), v.strip()
        if k == "t":
            veces += 1
            t = v
        elif k == "v1" and _HEX64.fullmatch(v):  # fullmatch: con `$`, Python acepta un `\n` final
            v1.append(v)
        # cualquier otro campo (`v2=…`) se IGNORA: el esquema puede crecer
    if veces != 1 or t is None or not _ENTERO.fullmatch(t) or not v1:
        return "formato"

    # Se comparan TODAS las parejas (secreto × v1), en tiempo constante (`compare_digest`), sin salir en la primera.
    acierta = False
    for secreto in secretos:
        esperado = hmac.new(secreto.encode("utf-8"), t.encode("ascii") + b"." + cuerpo, hashlib.sha256).hexdigest()
        for recibido in v1:
            if hmac.compare_digest(esperado, recibido):
                acierta = True
    if not acierta:
        return "firma"
    # La tolerancia es SIMÉTRICA (un `t` del futuro también se rechaza) y se mira DESPUÉS de la firma.
    return "tolerancia" if abs(ahora - int(t)) > tolerancia else "ok"
