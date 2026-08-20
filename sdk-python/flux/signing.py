"""
Firma de eventos con Ed25519.
Contrato normativo: specification/07-signing.md

Extensión **OPCIONAL** de v1: un evento sin firma sigue siendo válido y el default de
la verificación es `off`. Por eso `cryptography` es una dependencia opcional
(`pip install "flux-sdk[signing]"`) y no se importa hasta que alguien firma o verifica:
un servicio que no usa la extensión no debería cargar —ni auditar— una biblioteca de
criptografía.

Diferencia con el SDK de Node, que usa `node:crypto` sin dependencias: la stdlib de
Python no trae Ed25519, así que el port necesita `cryptography`. Es la única pieza de
la fase que no sale gratis.
"""

from __future__ import annotations

import base64
import logging
from dataclasses import dataclass, field, replace
from typing import Any, Literal, Mapping, TypeVar

from .envelope import FluxEvent, serialize, strip_dlq_extensions
from .errors import PoisonError

__all__ = [
    "VerificationMode",
    "SigningOptions",
    "SigningKeyError",
    "KeyPair",
    "Signer",
    "Verifier",
    "create_signer",
    "create_verifier",
    "generate_key_pair",
]

T = TypeVar("T")

VerificationMode = Literal["off", "warn", "require"]

_LOGGER = logging.getLogger("flux")


class SigningKeyError(ValueError):
    """Una clave mal formada, ausente o de otro algoritmo. Es un fallo de configuración
    local, no un mensaje ajeno corrupto: por eso no es POISON."""


@dataclass(frozen=True)
class SigningOptions:
    """Configuración de la extensión de firma."""

    #: Clave privada Ed25519 en PEM (PKCS#8) para firmar al publicar. `None` para solo
    #: verificar. **NUNCA se versiona** — 06-security.md §2.
    private_key_pem: str | None = None
    #: Id de la clave, formato `<servicio>-<n>`. Obligatorio si se firma.
    key_id: str | None = None
    #: Claves públicas conocidas: `signkeyid` → PEM (SPKI).
    #:
    #: **Incluye aquí las claves RETIRADAS mientras exista algún evento firmado con
    #: ellas** (mínimo 90 días, la retención de la DLQ). Retirar una clave impide emitir
    #: con ella, no verificar lo ya emitido — 07-signing.md §6.
    public_keys: Mapping[str, str] = field(default_factory=dict)
    #: `off` (default) | `warn` | `require` — 07-signing.md §7.
    verify: VerificationMode = "off"


@dataclass(frozen=True)
class KeyPair:
    private_key_pem: str
    public_key_pem: str


# ─── Carga perezosa de `cryptography` ────────────────────────────────────────

_ed25519_module: Any = None
_serialization_module: Any = None


def _crypto() -> tuple[Any, Any]:
    """
    Importa `cryptography` la primera vez que hace falta.

    El coste de no hacerlo a nivel de módulo es esta función; el beneficio es que
    `import flux` sigue funcionando —y sigue siendo barato— en un servicio que no firma,
    que es el caso por defecto del protocolo.
    """
    global _ed25519_module, _serialization_module
    if _ed25519_module is None:
        try:
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.asymmetric import ed25519
        except ImportError as cause:  # pragma: no cover - depende del entorno
            raise SigningKeyError(
                'la firma Ed25519 necesita el extra `signing`: pip install "flux-sdk[signing]". '
                "Es opcional porque la firma es una extensión OPCIONAL de v1: un evento sin "
                "firma sigue siendo válido (07-signing.md)"
            ) from cause
        _ed25519_module, _serialization_module = ed25519, serialization
    return _ed25519_module, _serialization_module


# ─── Payload firmado ─────────────────────────────────────────────────────────


def _signable_payload(event: FluxEvent[Any]) -> bytes:
    """
    Bytes canónicos que se firman: el evento sin `signature` y sin las extensiones
    `dlq*` (se añaden después de firmar). Ver 07-signing.md §5.

    No hay una canonicalización aparte para firmar: es el mismo `serialize()` que usa el
    productor, y funciona porque 01-envelope.md §1.1, §2.2 y §6 fijan una única
    representación en bytes. Si la hubiera, sería una segunda implementación del mismo
    formato y las dos divergirían.
    """
    return serialize(replace(strip_dlq_extensions(event), signature=None))


def _b64url(raw: bytes) -> str:
    """base64url **sin padding** — 07-signing.md §4."""
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _unb64url(value: str) -> bytes:
    # El `=` se quita al emitir, así que hay que reponerlo para decodificar.
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


# ─── Firmar ──────────────────────────────────────────────────────────────────


class Signer:
    """Firma eventos con una clave privada Ed25519."""

    def __init__(self, key: Any, key_id: str) -> None:
        self._key = key
        self._key_id = key_id

    @property
    def key_id(self) -> str:
        return self._key_id

    def sign(self, event: FluxEvent[T]) -> FluxEvent[T]:
        """Devuelve el evento con `signkeyid` y `signature`, ambos antes de `data`."""
        # `signkeyid` va DENTRO de lo firmado: si quedara fuera, un atacante podría
        # cambiarlo para que la verificación buscara otra clave — 07-signing.md §5.
        con_key_id = replace(event, signkeyid=self._key_id, signature=None)
        firma = self._key.sign(_signable_payload(con_key_id))
        # `data` sigue siendo el último atributo sin esfuerzo: el orden lo fija
        # `FluxEvent.to_dict`, no el orden de inserción como en Node.
        return replace(con_key_id, signature=_b64url(firma))


def create_signer(options: SigningOptions) -> Signer | None:
    """`None` si no hay clave privada: firmar es opcional, verificar también."""
    if not options.private_key_pem:
        return None
    if not options.key_id:
        raise SigningKeyError(
            "signing.private_key_pem requiere signing.key_id: sin él, un verificador no "
            "sabe qué clave pública usar (07-signing.md §4)"
        )

    ed25519, serialization = _crypto()
    try:
        key = serialization.load_pem_private_key(
            options.private_key_pem.encode("utf-8"), password=None
        )
    except Exception as cause:
        raise SigningKeyError(f"clave privada inválida: {cause}") from cause

    if not isinstance(key, ed25519.Ed25519PrivateKey):
        raise SigningKeyError(
            f"la clave es {type(key).__name__}, se esperaba Ed25519PrivateKey. El protocolo "
            f"no negocia algoritmo a propósito: los formatos con algoritmo negociable "
            f"acumulan una familia de vulnerabilidades que solo existe porque hay algo que "
            f"negociar (07-signing.md §3)"
        )
    return Signer(key, options.key_id)


# ─── Verificar ───────────────────────────────────────────────────────────────


class Verifier:
    """Aplica la política de verificación de 07-signing.md §7."""

    def __init__(
        self,
        keys: Mapping[str, Any],
        mode: VerificationMode,
        logger: logging.Logger | None = None,
    ) -> None:
        self._keys = dict(keys)
        self._mode = mode
        self._logger = logger or _LOGGER

    @property
    def mode(self) -> VerificationMode:
        return self._mode

    def _fail(self, code: str, message: str) -> None:
        if self._mode == "require":
            raise PoisonError(message, code=code)
        # `warn` existe porque adoptar la firma en un ecosistema en marcha exige un
        # periodo en el que unos productores firman y otros no. Pasar directo a
        # `require` convierte en POISON todo evento de un servicio aún no migrado.
        self._logger.warning("[flux] %s", message)

    def check(self, event: FluxEvent[Any]) -> None:
        """Lanza `PoisonError` en modo `require`; registra y acepta en modo `warn`."""
        if event.signature is None:
            self._fail("MISSING_SIGNATURE", f"evento {event.id} sin firma")
            return

        key = self._keys.get(event.signkeyid) if event.signkeyid else None
        if key is None:
            # Una clave desconocida NO es lo mismo que una retirada: si el id no está en
            # el mapa, el operador olvidó conservarla o el evento viene de fuera.
            self._fail(
                "UNKNOWN_SIGNING_KEY",
                f"evento {event.id} firmado con signkeyid={event.signkeyid!r}, que no está "
                f"entre las claves conocidas. ¿Se retiró sin conservar la pública?",
            )
            return

        try:
            key.verify(_unb64url(event.signature), _signable_payload(event))
        except Exception:
            # Cualquier fallo —firma alterada, base64 corrupto, evento manipulado— es el
            # mismo hecho para el operador: esos bytes no los firmó esa clave.
            self._fail("INVALID_SIGNATURE", f"la firma del evento {event.id} no verifica")


def create_verifier(
    options: SigningOptions, logger: logging.Logger | None = None
) -> Verifier | None:
    """`None` en modo `off`: no se paga —ni se importa `cryptography`— lo que no se usa."""
    mode = options.verify
    if mode == "off":
        return None

    ed25519, serialization = _crypto()
    keys: dict[str, Any] = {}
    for key_id, pem in options.public_keys.items():
        try:
            key = serialization.load_pem_public_key(pem.encode("utf-8"))
        except Exception as cause:
            raise SigningKeyError(f"clave pública {key_id!r} inválida: {cause}") from cause
        if not isinstance(key, ed25519.Ed25519PublicKey):
            raise SigningKeyError(
                f"la clave pública {key_id!r} es {type(key).__name__}, se esperaba "
                f"Ed25519PublicKey"
            )
        keys[key_id] = key

    if not keys:
        raise SigningKeyError(
            f'signing.verify = "{mode}" requiere signing.public_keys. Incluye también las '
            f"claves RETIRADAS mientras existan eventos firmados con ellas (07-signing.md §6)"
        )
    return Verifier(keys, mode, logger)


# ─── Utilidad ────────────────────────────────────────────────────────────────


def generate_key_pair() -> KeyPair:
    """Genera un par Ed25519 en PEM (PKCS#8 / SPKI). Comodidad para pruebas y `flux keygen`."""
    ed25519, serialization = _crypto()
    private = ed25519.Ed25519PrivateKey.generate()
    return KeyPair(
        private_key_pem=private.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode("ascii"),
        public_key_pem=private.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode("ascii"),
    )
