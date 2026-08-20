<?php

declare(strict_types=1);

namespace Flux;

/**
 * Las cuatro partes de un subject de NATS — 02-naming.md §1.
 *
 * ⚠️ Ojo con la palabra "subject": este es el subject de NATS (dirección de enrutado).
 * El atributo `subject` de CloudEvents es otra cosa —el id del agregado— y en la API de
 * este SDK se llama `aggregateId`. Ver 01-envelope.md §2.1.
 */
final readonly class ParsedSubject
{
    public function __construct(
        /** Bounded context. Sustantivo plural, kebab-case. `pedidos`. */
        public string $domain,
        /** Raíz de agregado. Sustantivo singular, kebab-case. `pedido`. */
        public string $aggregate,
        /** Versión mayor del contrato, entero >= 1. */
        public int $major,
        /** Hecho en pasado, kebab-case. `creado`, `entrega-fallida`. */
        public string $event,
    ) {
    }
}
