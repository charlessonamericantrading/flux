<?php

declare(strict_types=1);

namespace Flux\Metrics;

use Flux\ErrorClass;

/**
 * Valor de la etiqueta `outcome` de `flux_events_consumed_total`
 * — 08-observability.md §2.1.
 */
enum ConsumeOutcome: string
{
    /** El handler devolvió sin lanzar. */
    case Ok = 'ok';

    /** Fallo transitorio: se reintentará, o agotó los reintentos. */
    case Retryable = 'retryable';

    /** El consumidor lo rechazó de forma definitiva. */
    case Permanent = 'permanent';

    /** El mensaje no era interpretable. */
    case Poison = 'poison';

    /** El payload no cumple su JSON Schema. */
    case InvalidSchema = 'invalid_schema';

    /**
     * Falta la firma, no verifica, o el `signkeyid` es desconocido — 07-signing.md §7.
     *
     * No es una clase de error aparte —un fallo de firma es POISON— pero sí un `outcome`
     * propio, para que un pico de firmas rotas no se confunda con un pico de JSON corrupto:
     * tienen causas y respuestas distintas.
     */
    case InvalidSignature = 'invalid_signature';

    /** La clase de error con la que un evento murió, traducida a etiqueta. */
    public static function fromErrorClass(ErrorClass $class): self
    {
        return match ($class) {
            ErrorClass::Retryable => self::Retryable,
            ErrorClass::Permanent => self::Permanent,
            ErrorClass::Poison => self::Poison,
        };
    }
}
