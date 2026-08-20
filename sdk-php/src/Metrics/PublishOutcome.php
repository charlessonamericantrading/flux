<?php

declare(strict_types=1);

namespace Flux\Metrics;

/**
 * Valor de la etiqueta `outcome` de `flux_events_published_total`
 * — 08-observability.md §2.1.
 */
enum PublishOutcome: string
{
    /** El stream confirmó la publicación. */
    case Ok = 'ok';

    /**
     * El payload no cumple su JSON Schema (L3). El SDK de PHP es L2 y todavía no lo emite;
     * el valor existe porque la etiqueta es del protocolo, no del SDK.
     */
    case InvalidSchema = 'invalid_schema';

    /** El broker rechazó la publicación, o el envelope no se pudo construir. */
    case Error = 'error';
}
