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
     * El payload no cumple su JSON Schema, así que `publish()` falló y el evento **no**
     * llegó al stream — L3, 00-protocol.md §5.
     *
     * Se cuenta aparte de `Error` a propósito: "el broker rechazó la publicación" y "mi
     * servicio intentó publicar algo que incumple su propio contrato" son dos problemas con
     * dos dueños distintos, y un panel que los suma no sirve para ninguno de los dos.
     */
    case InvalidSchema = 'invalid_schema';

    /** El broker rechazó la publicación, o el envelope no se pudo construir. */
    case Error = 'error';
}
