<?php

declare(strict_types=1);

namespace Flux\Validation;

/**
 * Configuración de la validación L3 — 00-protocol.md §5.
 *
 * Todo aquí es opt-in y el default no cuesta nada: sin `mode` no se compila ningún
 * esquema y `opis/json-schema` ni siquiera hace falta instalarlo. L3 es un nivel al que
 * se sube a propósito, así que su coste —una dependencia más y un validador por evento—
 * también debe pagarse a propósito.
 */
final readonly class ValidationOptions
{
    /**
     * @param ValidationMode $mode `Strict` hace fallar `publish()`; `Warn` registra y
     *        publica igual; `Off` (default) es L2.
     * @param SchemaBundle|null $bundle Los esquemas, desplegados CON el servicio. Es
     *        obligatorio si `mode` no es `Off`: sin él no hay nada contra lo que validar, y
     *        el SDK **NO** resuelve el `dataschema` por red (00-protocol.md §5).
     * @param bool $onConsume Validar también al CONSUMIR. Un fallo ahí se clasifica
     *        **PERMANENT**: el evento es sintácticamente correcto —parseó como CloudEvent—
     *        pero incumple su contrato, y reintentarlo cinco veces dará exactamente el mismo
     *        resultado mientras los eventos sanos esperan detrás (04-errors.md §1.2).
     *
     *        Está en `false` por defecto porque el sitio donde un contrato roto se arregla
     *        es el productor: validar al consumir convierte el problema en visible, no en
     *        resuelto. Actívalo cuando consumas de productores que no controlas.
     */
    public function __construct(
        public ValidationMode $mode = ValidationMode::Off,
        public ?SchemaBundle $bundle = null,
        public bool $onConsume = false,
    ) {
    }
}
