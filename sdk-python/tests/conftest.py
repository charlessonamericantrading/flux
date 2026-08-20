"""
Deja `sdk-python/` en el path para poder ejecutar `python -m pytest sdk-python/tests`
desde la raíz del repo sin instalar el paquete.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
