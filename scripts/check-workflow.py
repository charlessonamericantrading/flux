#!/usr/bin/env python3
"""
Valida .github/workflows/*.yml antes de subirlos.

Existe porque una clave duplicada rompe el workflow ENTERO —GitHub no ejecuta ni un
job, y el error solo se ve en la web— y **la mayoría de cargadores de YAML la aceptan
en silencio**, quedándose con la última. Pasó dos veces en este repo: `sdk-dotnet`
duplicado por un corte mal indexado, y `sdk-php` porque dos autores añadieron el mismo
job en paralelo.

Se usa PyYAML con un constructor estricto en vez de un parser propio: escribir uno a
mano produjo 43 falsos positivos porque confundía claves de padres distintos al mismo
nivel de indentación. El YAML no se parsea a ojo.

    python scripts/check-workflow.py
"""

import sys
from pathlib import Path

# La consola de Windows usa cp1252 por defecto y no puede imprimir los simbolos de
# estado. Sin esto, la herramienta revienta con UnicodeEncodeError justo cuando iba a
# decir que todo esta bien.
for flujo in (sys.stdout, sys.stderr):
    try:
        flujo.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

try:
    import yaml
except ImportError:
    sys.exit("necesita PyYAML:  pip install pyyaml")

RAIZ = Path(__file__).resolve().parent.parent
DIR = RAIZ / ".github" / "workflows"


class Estricto(yaml.SafeLoader):
    """SafeLoader que RECHAZA claves duplicadas en vez de quedarse con la última."""


def _sin_duplicados(loader, node, deep=False):
    mapa = {}
    for k_node, v_node in node.value:
        clave = loader.construct_object(k_node, deep=deep)
        if clave in mapa:
            raise yaml.constructor.ConstructorError(
                None, None,
                f'clave duplicada "{clave}" (línea {k_node.start_mark.line + 1}). '
                "GitHub rechaza el workflow ENTERO: no ejecuta ni un job.",
                k_node.start_mark,
            )
        mapa[clave] = loader.construct_object(v_node, deep=deep)
    return mapa


Estricto.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _sin_duplicados
)

problemas = []

for fichero in sorted(DIR.glob("*.y*ml")):
    try:
        doc = yaml.load(fichero.read_text(encoding="utf-8"), Estricto)
    except yaml.YAMLError as e:
        problemas.append(f"{fichero.name}: {e}")
        continue

    # `on:` lo parsea YAML 1.1 como el booleano True. No es un bug: es la razón por la
    # que muchos workflows lo escriben entrecomillado.
    if "on" not in doc and True not in doc:
        problemas.append(f"{fichero.name}: falta la clave `on:`")
    if "jobs" not in doc:
        problemas.append(f"{fichero.name}: falta la clave `jobs:`")
        continue

    for nombre, job in (doc.get("jobs") or {}).items():
        if not isinstance(job, dict):
            problemas.append(f"{fichero.name}: job '{nombre}' no es un mapa")
            continue
        if "uses" in job:      # workflow reutilizable: no lleva runs-on ni steps
            continue
        for req in ("runs-on", "steps"):
            if req not in job:
                problemas.append(f"{fichero.name}: job '{nombre}' no tiene `{req}`")
        for i, paso in enumerate(job.get("steps") or []):
            if not isinstance(paso, dict) or ("uses" not in paso and "run" not in paso):
                problemas.append(
                    f"{fichero.name}: job '{nombre}', paso {i}: ni `uses` ni `run`"
                )

if problemas:
    print(f"\n\u2717 {len(problemas)} problema(s)\n", file=sys.stderr)
    for p in problemas:
        print(f"  \u2717 {p}", file=sys.stderr)
    sys.exit(1)

n = len(list(DIR.glob("*.y*ml")))
print(f"\u2713 {n} workflow(s) v\u00e1lidos, sin claves duplicadas")
