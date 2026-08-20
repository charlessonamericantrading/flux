<?php

declare(strict_types=1);

namespace Flux\Validation;

use Flux\ErrorClass;
use Flux\FluxError;

/**
 * El payload no cumple el JSON Schema que su propio `dataschema` declara — L3.
 *
 * Al publicar en modo `Strict` sale por `publish()`: el contrato roto es un fallo del
 * servicio que lo generó, y ahí es donde hay que arreglarlo.
 *
 * Al consumir se clasifica **PERMANENT** y va directo a la DLQ sin gastar reintentos. La
 * razón es la de 04-errors.md §1.2 y no admite matices: el evento ya parseó como
 * CloudEvent, así que no es POISON; y su `data` no va a cambiar entre entregas, así que
 * reintentarlo son 51 minutos de cola bloqueada para llegar exactamente al mismo sitio.
 */
final class SchemaValidationError extends FluxError
{
    /** Código estable para métricas y para la extensión `dlqerror` — 08-observability.md §2.2. */
    public const CODE = 'INVALID_SCHEMA';

    /**
     * @param list<string> $errors **TODOS** los incumplimientos, no solo el primero.
     */
    public function __construct(
        public readonly string $subject,
        public readonly string $dataschema,
        public readonly array $errors,
        ?\Throwable $previous = null,
    ) {
        parent::__construct(
            "el payload de \"{$subject}\" no cumple su esquema ({$dataschema}):\n"
            . implode("\n", array_map(static fn (string $e): string => "  · {$e}", $errors)),
            self::CODE,
            $previous,
        );
    }

    public function fluxClass(): ErrorClass
    {
        return ErrorClass::Permanent;
    }
}
