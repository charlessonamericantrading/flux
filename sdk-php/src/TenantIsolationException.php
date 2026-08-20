<?php

declare(strict_types=1);

namespace Flux;

/**
 * `tenantIsolation: 'strict'` y una suscripción sin filtro de tenant.
 * Contrato normativo: specification/09-multitenancy.md §3.
 *
 * El punto 3 de las obligaciones del SDK es el que importa de toda la sección: consumir
 * sin filtro **DEBE** ser un error de configuración, no un descuido silencioso. Un filtro
 * que hay que acordarse de poner es un filtro que alguien olvidará, y el fallo —ver los
 * datos de otro tenant— **no produce ningún error**: produce un incidente de privacidad
 * que se descubre semanas después.
 *
 * Se lanza en `subscribe()`, **antes** de crear el durable consumer: si esperase al primer
 * evento, un servicio mal configurado arrancaría, pasaría el healthcheck y solo fallaría
 * cuando ya estuviera en producción.
 */
final class TenantIsolationException extends \RuntimeException
{
    public function __construct(public readonly string $subject, string $reason)
    {
        parent::__construct(
            "tenantIsolation = \"strict\", pero {$reason} al suscribirse a \"{$subject}\". Sin "
            . 'filtro de tenant este consumidor vería los eventos de TODOS los tenants, y eso no '
            . 'produce ningún error visible (09-multitenancy.md §3).'
        );
    }
}
