<?php

declare(strict_types=1);

namespace Flux;

/**
 * Las tres clases del protocolo — 04-errors.md §1.
 *
 * Determinan la acción sobre el mensaje de NATS. El valor de cada caso es exactamente
 * el que acaba en la extensión `dlqreason` del envelope, así que no hace falta ninguna
 * conversión intermedia (ni un `match` que alguien pueda desincronizar).
 */
enum ErrorClass: string
{
    /** Fallo del entorno, podría desaparecer solo. → `nak(delay)`, reintenta con backoff. */
    case Retryable = 'retryable';

    /** Evento válido que este consumidor nunca podrá procesar. → `term()` + DLQ, sin reintentos. */
    case Permanent = 'permanent';

    /** El mensaje ni siquiera es interpretable. → `term()` + DLQ + alerta inmediata. */
    case Poison = 'poison';
}
