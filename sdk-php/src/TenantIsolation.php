<?php

declare(strict_types=1);

namespace Flux;

/**
 * Aislamiento entre tenants — 09-multitenancy.md §3.
 *
 * El Modelo A de v1 mezcla todos los tenants en un stream por dominio, y **el aislamiento
 * es una convención del SDK, no una frontera del broker**: todo servicio con acceso al
 * dominio ve los datos de todos los tenants. Con `Strict`, esa convención deja al menos de
 * depender de que alguien se acuerde.
 *
 * Lo que `Strict` **no** hace: cerrar las dos amenazas que §1 declara descubiertas —un
 * productor legítimo comprometido que publica con el `tenantid` de otro, y un consumidor
 * comprometido que lee el subject entero—. Para eso hace falta el Modelo B (una account de
 * NATS por tenant), y ninguna cantidad de validación de envelope lo sustituye.
 */
enum TenantIsolation: string
{
    /** **Default.** El filtrado es opcional por suscripción. */
    case Off = 'off';

    /**
     * Toda suscripción filtra por tenant, y suscribirse sin uno configurado lanza
     * `TenantIsolationException`.
     */
    case Strict = 'strict';
}
