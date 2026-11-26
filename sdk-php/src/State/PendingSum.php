<?php

declare(strict_types=1);

namespace Flux\State;

/**
 * `flux_consumer_pending` de un contrato de estado es la SUMA de los durables del servicio, no el valor del último sondeado
 * (08-observability.md §2.4). Un `watch` por tenant crea un durable por tenant, y todos publican las MISMAS etiquetas —el `consumer`
 * va SIN el sufijo del tenant, o cada tenant nuevo crearía series nuevas—: sin sumar, se pisarían entre sí.
 */
final class PendingSum
{
    /** @var array<string,array<string,int>> */
    private array $bySeries = [];

    /** Anota el `pending` de un durable y devuelve la suma de la serie. */
    public function note(string $series, string $durable, int $pending): int
    {
        $this->bySeries[$series][$durable] = $pending;

        return array_sum($this->bySeries[$series]);
    }

    /** Un durable deja de sumar (su `watch` paró). Devuelve la suma que queda, o `null` si no había nada. */
    public function drop(string $series, string $durable): ?int
    {
        if (!isset($this->bySeries[$series][$durable])) {
            return null;
        }
        unset($this->bySeries[$series][$durable]);
        $sum = array_sum($this->bySeries[$series]);
        if ($this->bySeries[$series] === []) {
            unset($this->bySeries[$series]);
        }

        return $sum;
    }
}
