"""
Aislamiento entre tenants — Modelo A.
Contrato normativo: specification/09-multitenancy.md §3

Vive fuera de `client.py` por la misma razón que `envelope.py`: es la regla de la que
depende que un consumidor no vea datos ajenos, y tiene que poder probarse **sin broker y
sin `nats-py` instalado**. Una regla de seguridad que solo se ejecuta con infraestructura
delante es una regla que nadie prueba.

Recordatorio de lo que el Modelo A NO cubre (§1): un servicio legítimo comprometido puede
publicar con el `tenantid` de otro, y un consumidor comprometido puede leer el subject
entero. El filtro del SDK evita **errores**, no adversarios; para eso está el Modelo B
(una account de NATS por tenant).
"""

from __future__ import annotations

from typing import Literal

__all__ = ["TenantIsolation", "TenantIsolationError", "resolve_tenant_filter"]

TenantIsolation = Literal["off", "strict"]


class TenantIsolationError(RuntimeError):
    """
    Suscribirse sin filtro de tenant con `tenant_isolation="strict"`.

    Es un error de arranque a propósito. El fallo que evita —un consumidor que ve los
    eventos de TODOS los tenants— no produce ninguna señal en tiempo de ejecución: no hay
    excepción, no hay log, no hay métrica. Solo hay un incidente de privacidad que
    alguien descubre semanas después (09-multitenancy.md §3).
    """

    def __init__(self, subject: str, motivo: str) -> None:
        super().__init__(
            f'tenant_isolation="strict" pero {motivo} al suscribirse a "{subject}". '
            f"Sin filtro de tenant, este consumidor vería los eventos de TODOS los tenants "
            f"y eso no produce ningún error visible (09-multitenancy.md §3)."
        )
        self.subject = subject
        self.motivo = motivo


def resolve_tenant_filter(
    subject: str,
    connection_tenant: str | None,
    subscription_tenant: str | None,
    isolation: TenantIsolation,
) -> str | None:
    """
    Filtro de tenant efectivo de una suscripción, o `None` si no hay ninguno.

    El de la suscripción gana sobre el de la conexión: un servicio multi-tenant puede
    tener una conexión sin tenant y una suscripción por cada uno.

    `"system"` **no cuenta como filtro**: es la ausencia de tenant, no un tenant
    (09-multitenancy.md §5). Aceptarlo dejaría fuera todos los eventos de negocio y
    —peor— daría por satisfecho el modo estricto sin filtrar nada.
    """
    for candidato in (subscription_tenant, connection_tenant):
        if candidato and candidato != "system":
            return candidato

    if isolation == "strict":
        raise TenantIsolationError(
            subject,
            'no hay tenant_id ni en connect() ni en subscribe() (o vale "system", que es '
            "la ausencia de tenant)",
        )
    return None
