<?php

declare(strict_types=1);

namespace Flux;

/**
 * Un constructor se niega a construir un sobre: falta (o está vacío) un atributo que el SDK rellena, o `time` no es un instante.
 *
 * Lleva el **mismo código estable** que daría el parser ante el mismo defecto (01-envelope.md §3.1): un atributo obligatorio
 * vacío es `MISSING_REQUIRED_ATTRIBUTE`; una extensión obligatoria vacía, `MISSING_REQUIRED_EXTENSION`; un `time` que no es un
 * instante RFC 3339, `WRONG_ATTRIBUTE_TYPE` (CloudEvents lo tipa `Timestamp`). El arnés de conformidad compara ese código entre
 * SDKs.
 */
final class InvalidEnvelopeInputException extends EnvelopeException
{
    /** @param 'MISSING_REQUIRED_ATTRIBUTE'|'MISSING_REQUIRED_EXTENSION'|'WRONG_ATTRIBUTE_TYPE' $errorCode */
    public function __construct(string $message, public readonly string $errorCode)
    {
        parent::__construct($message);
    }
}
