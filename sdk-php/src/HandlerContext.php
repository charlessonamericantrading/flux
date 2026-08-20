<?php

declare(strict_types=1);

namespace Flux;

/**
 * Lo que el handler recibe además del evento.
 */
final readonly class HandlerContext
{
    /**
     * @param \Closure():void $sendWorkInProgress
     */
    public function __construct(
        /** Nº de entrega, empezando en 1. Lo dice JetStream, no el SDK. */
        public int $attempt,
        public int $maxAttempts,
        /** El subject de NATS del que vino el evento. */
        public string $subject,
        private \Closure $sendWorkInProgress,
    ) {
    }

    /**
     * Confirma el evento. Es un **no-op**: devolver normalmente del handler hace lo mismo.
     *
     * Existe porque los ejemplos de la spec terminan en `return $ctx->ack();` y porque
     * escribirlo obliga a pensar en la idempotencia (03-delivery.md §4), que es el único
     * requisito de admisión al ecosistema. El ack real lo emite el runtime cuando el
     * handler devuelve sin lanzar.
     */
    public function ack(): void
    {
    }

    /**
     * Extiende el plazo de `ack_wait` para este mensaje (work-in-progress de JetStream).
     *
     * ⚠️ **En PHP hay que llamarlo a mano.** Node y Python emiten WIP cada `ack_wait / 2`
     * desde un temporizador de fondo, pero PHP no tiene concurrencia en el proceso: entre
     * que el handler empieza y termina, el SDK no recupera el control y no puede emitir
     * nada. Ver 03-delivery.md §2.1 y la sección "Limitaciones" del README.
     *
     * Si tu handler puede tardar más de 30 s, llama a esto dentro de su bucle:
     *
     * ```php
     * foreach ($lotes as $lote) {
     *     $this->procesar($lote);
     *     $ctx->workInProgress();   // el plazo vuelve a empezar
     * }
     * ```
     *
     * Un handler que supere los 30 s sin emitir WIP recibirá **el mismo evento reentregado
     * mientras aún se está ejecutando**. Eso es un bug de la aplicación, no del SDK.
     */
    public function workInProgress(): void
    {
        ($this->sendWorkInProgress)();
    }
}
