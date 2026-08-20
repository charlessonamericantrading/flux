<?php

declare(strict_types=1);

namespace Flux\Validation;

/**
 * Qué hacer con un payload que no cumple su `dataschema` — 00-protocol.md §5 (nivel L3).
 *
 * `Warn` no es un adorno, y es la misma razón por la que existe `VerificationMode::Warn`
 * en la firma: introducir validación en un ecosistema **en marcha** exige un periodo en el
 * que se sabe qué se está incumpliendo sin romper a nadie el primer día. Pasar directo a
 * `Strict` convierte en un fallo de publicación todo evento de un productor que llevaba
 * meses publicando algo ligeramente distinto de lo que su esquema decía — y esos existen,
 * que es precisamente el motivo de que L3 exista.
 */
enum ValidationMode: string
{
    /**
     * **Default.** No se valida nada. Es el nivel L2 y **no cuesta nada**: sin modo no se
     * compila ningún esquema y la librería de validación ni siquiera hace falta.
     */
    case Off = 'off';

    /**
     * Se registra el incumplimiento y se publica igual. El modo de la migración.
     *
     * Útil también como red permanente en un consumidor: enterarse de que un productor
     * incumple su contrato sin dejar de procesar sus eventos.
     */
    case Warn = 'warn';

    /**
     * `publish()` **falla**. Es lo que convierte un contrato roto en un fallo del servicio
     * que lo provocó, en vez de un misterio que aparece en un consumidor de otro equipo,
     * en otro lenguaje y otra semana.
     */
    case Strict = 'strict';
}
