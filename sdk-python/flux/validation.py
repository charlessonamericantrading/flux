"""
Validación L3 contra el JSON Schema del evento.
Contrato normativo: specification/00-protocol.md §5 (nivel L3)

Cierra el hueco más grande que quedaba: sin esto, un productor puede publicar un
payload que viola su propio `dataschema` y nadie se entera hasta que un consumidor
—posiblemente en otro equipo, otro lenguaje y otra semana— se atraganta. El error
aparece lejísimos de su causa.

Validar en `publish()` lo convierte en un fallo del servicio que lo provocó.

Port de `sdk-node/src/validation.ts`: misma semántica, mismos modos, mismos mensajes.
Lo único que cambia es la biblioteca (`jsonschema` en vez de `ajv`) y el `snake_case`.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal, Mapping

from .envelope import FluxEvent
from .errors import PermanentError

__all__ = [
    "ValidationMode",
    "SchemaBundle",
    "ValidationOptions",
    "SchemaValidationError",
    "SchemaNotFoundError",
    "load_bundle",
    "create_validator",
    "schema_uri_for",
]

ValidationMode = Literal["off", "warn", "strict"]

_LOGGER = logging.getLogger("flux")

#: Validador de eventos: `(evento, subject) -> None`. Lanza en modo `strict`.
Validator = Callable[[FluxEvent[Any], str], None]


@dataclass(frozen=True)
class SchemaBundle:
    """
    Los esquemas empaquetados, tal cual los genera `scripts/bundle-schemas.mjs`.

    Se pasa como DATO, no como URL: validar está en la ruta caliente y resolver el
    `dataschema` por red sería una petición por evento. Una caché con TTL tampoco vale
    —abre una ventana en la que dos servicios validan contra versiones distintas del
    mismo esquema—, así que el bundle se despliega **con el servicio** y la versión del
    esquema queda clavada a la del servicio (00-protocol.md §5).
    """

    #: subject → URI del esquema con el MINOR más alto de su mayor. Dentro de un mayor
    #: todo es BACKWARD-compatible, así que el más alto acepta lo que aceptan los
    #: anteriores — 05-compatibility.md §2.
    subjects: Mapping[str, str] = field(default_factory=dict)
    #: URI → JSON Schema.
    schemas: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> SchemaBundle:
        """
        Construye el bundle desde el JSON generado.

        Ignora las claves de metadatos (`$comment`, `generatedFrom`, `count`) en vez de
        rechazarlas: son documentación del fichero, no del contrato, y añadir una no debe
        romper a los siete SDKs.
        """
        return cls(subjects=dict(raw.get("subjects") or {}), schemas=dict(raw.get("schemas") or {}))


def load_bundle(path: str | Path) -> SchemaBundle:
    """Carga `schemas/bundle.json`. Regenéralo con `node scripts/bundle-schemas.mjs`."""
    return SchemaBundle.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))


@dataclass(frozen=True)
class ValidationOptions:
    """Configuración de la validación L3."""

    #: `"strict"` — `publish()` lanza si el payload no valida. Es lo que hace que un
    #: contrato roto sea un fallo del productor y no un misterio del consumidor.
    #:
    #: `"warn"` — registra y publica igual. Útil para introducir validación en un
    #: ecosistema en marcha sin romper nada el primer día.
    #:
    #: `"off"` (default) — nivel L2. Sin coste: no se importa `jsonschema` siquiera.
    mode: ValidationMode = "off"
    #: Bundle generado por `scripts/bundle-schemas.mjs`. Obligatorio si `mode != "off"`.
    bundle: SchemaBundle | None = None
    #: Validar también al CONSUMIR. Un fallo se clasifica **PERMANENT**: el evento es
    #: sintácticamente correcto pero incumple su contrato, y reintentarlo dará
    #: exactamente el mismo resultado — 04-errors.md §1.2.
    on_consume: bool = False


# ─── Errores ─────────────────────────────────────────────────────────────────
#
# Divergencia deliberada con el SDK de Node, donde los dos son `Error` a secas: aquí
# heredan de `PermanentError` para que la clasificación PERMANENT que exige
# 00-protocol.md §5 NO dependa de la política del clasificador. Si fueran errores
# genéricos, `unknown_error_policy` los mandaría por la rama "retryable acotado" y un
# evento que nunca podrá validar gastaría reintentos antes de llegar a la DLQ.


class SchemaValidationError(PermanentError):
    """El payload no cumple el JSON Schema que su `dataschema` declara."""

    def __init__(self, subject: str, dataschema: str, errors: list[str]) -> None:
        detalle = "\n".join(f"  · {e}" for e in errors)
        super().__init__(
            f'el payload de "{subject}" no cumple su esquema ({dataschema}):\n{detalle}',
            code="SCHEMA_INVALID",
        )
        self.subject = subject
        self.dataschema = dataschema
        #: TODOS los errores, no solo el primero: de uno en uno, arreglar un payload con
        #: tres campos mal cuesta tres despliegues.
        self.errors = list(errors)


class SchemaNotFoundError(PermanentError):
    """El `dataschema` del evento no está en el bundle desplegado."""

    def __init__(self, subject: str, dataschema: str) -> None:
        super().__init__(
            f'no hay esquema para "{subject}" ({dataschema}) en el bundle. '
            f"Regenera con `node scripts/bundle-schemas.mjs`, o baja `validation.mode` a "
            f'"warn".',
            code="SCHEMA_NOT_FOUND",
        )
        self.subject = subject
        self.dataschema = dataschema


# ─── Compilación ─────────────────────────────────────────────────────────────


def _mensaje_sin_red(uri: object) -> str:
    return (
        f'el esquema referencia "{uri}", que no está en el bundle. Regenera con '
        f"`node scripts/bundle-schemas.mjs`. El SDK NO resuelve `dataschema` por red: "
        f"validar está en la ruta caliente y una caché con TTL abriría una ventana en la "
        f"que dos servicios validan contra versiones distintas del mismo esquema "
        f"(00-protocol.md §5)"
    )


def _sin_red(uri: str) -> Any:
    """
    Retrieve del registro: **nunca** hay red.

    Un `$ref` que no esté en el bundle es un bundle incompleto, y el arreglo es
    regenerarlo — no ir a buscarlo por HTTP en la ruta caliente (00-protocol.md §5).
    """
    raise ValueError(_mensaje_sin_red(uri))


def _compilar(bundle: SchemaBundle) -> tuple[dict[str, Any], type[Exception]]:
    """
    Compila un validador por URI del bundle.

    `jsonschema` se importa aquí y no arriba: es una dependencia OPCIONAL
    (`pip install "flux-sdk[validation]"`) porque L3 es opt-in, así que su coste también
    debe serlo. Un servicio en L2 no debería arrastrar —ni auditar— un validador de JSON
    Schema que no va a ejecutar. Es el mismo trato que `cryptography` en signing.py.
    """
    try:
        from jsonschema.validators import Draft202012Validator, validator_for
        from referencing import Registry, Resource
        from referencing.exceptions import Unresolvable
        from referencing.jsonschema import DRAFT202012
    except ImportError as cause:  # pragma: no cover - depende del entorno
        raise RuntimeError(
            'la validación L3 necesita el extra `validation`: pip install "flux-sdk[validation]". '
            "Es opcional porque L3 es opt-in: un servicio en L2 no debe cargar un validador "
            f"de JSON Schema que no ejecuta (00-protocol.md §5). Causa: {cause}"
        ) from cause

    # El registro resuelve los `$ref` ENTRE esquemas del bundle sin tocar la red: el
    # `retrieve` solo se invoca para lo que no está, y ahí falla con un mensaje que dice
    # qué regenerar.
    registro = Registry(retrieve=_sin_red).with_resources(  # type: ignore[call-arg]
        [
            # `default_specification` solo actúa si el esquema no declara `$schema`. Los
            # de flux lo declaran, así que cada uno se compila con SU draft.
            (uri, Resource.from_contents(esquema, default_specification=DRAFT202012))
            for uri, esquema in bundle.schemas.items()
        ]
    )

    compilados: dict[str, Any] = {}
    for uri, esquema in bundle.schemas.items():
        # ⚠️ Los esquemas de flux declaran `$schema: draft/2020-12`. `validator_for` lee
        # ese campo y elige `Draft202012Validator`; fijar a mano un validador de draft-07
        # NO daría un error de versión, daría `no schema with key or ref
        # ".../2020-12/schema"`, que no dice nada útil — 00-protocol.md §5. El `default`
        # solo actúa si el esquema no declara `$schema`, y entonces manda el draft del
        # protocolo, no el que la biblioteca considere "el último" el día de mañana.
        clase = validator_for(esquema, default=Draft202012Validator)
        # Un esquema roto debe romper el arranque, no la primera publicación.
        clase.check_schema(esquema)
        compilados[uri] = clase(esquema, registry=registro)
    # `Unresolvable` viaja de vuelta porque `referencing` envuelve lo que lance el
    # `retrieve` y se pierde su mensaje: quien valida lo vuelve a traducir a algo
    # accionable. La clase se captura aquí y no se importa arriba por lo mismo que todo
    # lo demás de este bloque — la dependencia es opcional.
    return compilados, Unresolvable


def _errores(validador: Any, data: Any) -> list[str]:
    """
    TODOS los errores del payload, ordenados y estables.

    `iter_errors` y no `validate`: reportar solo el primero convierte arreglar un payload
    con tres campos mal en tres despliegues (00-protocol.md §5).
    """
    salida = [
        f"{'/' + '/'.join(str(p) for p in e.absolute_path) if e.absolute_path else '(raíz)'} "
        f"{e.message}"
        for e in validador.iter_errors(data)
    ]
    # Ordenados para que el mismo payload produzca el mismo mensaje en cada ejecución: un
    # mensaje que cambia de orden es un diff inútil en los logs y en los tests.
    return sorted(salida)


def create_validator(
    options: ValidationOptions, logger: logging.Logger | None = None
) -> Validator | None:
    """
    Construye el validador, o `None` en modo `off`.

    Se llama UNA vez al conectar y no por evento: compilar un JSON Schema en la ruta
    caliente sería tirar el throughput por comodidad de escritura. Y un bundle ausente o
    un esquema roto rompen el arranque, que es donde debe verse un fallo de configuración.
    """
    mode = options.mode
    if mode == "off":
        return None

    if mode not in ("warn", "strict"):
        # Un typo en el modo NO puede significar "no valides nada en silencio": ese fallo
        # solo se ve el día que alguien publica basura y nadie la para. `ValidationMode` es
        # un `Literal`, pero eso lo comprueba un type checker, no el intérprete.
        raise ValueError(
            f'validation.mode = "{mode}"; los valores válidos son "off", "warn" y "strict"'
        )

    if options.bundle is None:
        raise ValueError(
            f'validation.mode = "{mode}" requiere validation.bundle. Genera el bundle con '
            f"`node scripts/bundle-schemas.mjs` y cárgalo:\n"
            f"  from flux.validation import load_bundle\n"
            f'  bundle = load_bundle("schemas/bundle.json")'
        )

    compilados, no_resoluble = _compilar(options.bundle)
    log = logger or _LOGGER

    def validar(event: FluxEvent[Any], subject: str) -> None:
        validador = compilados.get(event.dataschema)
        if validador is None:
            if mode == "strict":
                raise SchemaNotFoundError(subject, event.dataschema)
            log.warning("[flux] sin esquema para %s (%s)", subject, event.dataschema)
            return

        try:
            errores = _errores(validador, event.data)
        except no_resoluble as cause:
            # Un `$ref` a un esquema que no está en el bundle. NO se sale a la red a
            # buscarlo: se dice qué regenerar (00-protocol.md §5).
            raise ValueError(_mensaje_sin_red(getattr(cause, "ref", cause))) from cause

        if not errores:
            return

        error = SchemaValidationError(subject, event.dataschema, errores)
        if mode == "strict":
            raise error
        # `warn` existe porque adoptar la validación en un ecosistema en marcha exige un
        # periodo en el que unos productores ya cumplen y otros todavía no.
        log.warning("[flux] %s", error)

    return validar


def schema_uri_for(bundle: SchemaBundle, subject: str) -> str | None:
    """
    Resuelve el `dataschema` EXACTO de un subject desde el bundle.

    Sin bundle, el SDK solo puede asumir el `<major>.0.0` del mayor (ver
    `FluxBus._schema_for`): es suficiente para L2 —el atributo es informativo— pero no
    para L3, donde el evento debe apuntar al esquema contra el que se valida de verdad.
    """
    return bundle.subjects.get(subject)
