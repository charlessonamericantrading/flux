<?php

declare(strict_types=1);

namespace Flux;

/**
 * Lo lanza el SDK, no la aplicación: el mensaje no pudo parsearse como CloudEvent, así
 * que **nunca llega al handler** — 04-errors.md §1.3.
 *
 * Un POISON casi siempre significa que un productor está roto o que alguien publicó a
 * mano en el subject equivocado. Es el único caso que DEBE despertar a alguien.
 *
 * Los `errorCode` son estables y compartidos por los seis SDKs: si dos emitieran códigos
 * distintos ante la misma entrada, agrupar por causa dejaría de funcionar en cuanto el
 * ecosistema es polyglot — que es siempre. Ver la tabla de 01-envelope.md §3.1.
 */
final class PoisonError extends FluxError
{
    public function fluxClass(): ErrorClass
    {
        return ErrorClass::Poison;
    }
}
