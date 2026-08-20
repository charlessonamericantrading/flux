<?php

declare(strict_types=1);

namespace Flux\Metrics;

/**
 * Valor de `flux_connection_state` — 08-observability.md §2.1.
 *
 * ⚠️ Este gauge dice si la **conexión** está viva, no si el consumidor está consumiendo.
 * Un bucle de consumo muerto sigue reportando `1`; lo que lo delata es
 * `flux_consumer_pending` creciendo (§4). Por eso las dos métricas van juntas y ninguna
 * sustituye a la otra.
 */
enum ConnectionState: int
{
    case Disconnected = 0;
    case Connected = 1;
    case Reconnecting = 2;
}
