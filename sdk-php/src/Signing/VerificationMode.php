<?php

declare(strict_types=1);

namespace Flux\Signing;

/**
 * Qué hacer con un evento cuya firma falta o no verifica — 07-signing.md §7.
 *
 * | Modo | Evento sin firma | Firma inválida |
 * |---|---|---|
 * | `off` (default) | Se acepta | Se acepta (no se mira) |
 * | `warn` | Se registra y se acepta | Se registra y se acepta |
 * | `require` | **POISON** | **POISON** |
 *
 * `Warn` no es un adorno de transición amable: adoptar la firma en un ecosistema en marcha
 * exige un periodo en el que unos productores firman y otros no. Pasar directo a `Require`
 * convierte en POISON todo evento de un servicio aún no migrado — es decir, tumba a los
 * consumidores de los servicios que van por delante, no a los que van por detrás.
 */
enum VerificationMode: string
{
    /** **Default.** No se mira la firma. Un evento sin firma sigue siendo válido. */
    case Off = 'off';

    /** Se registra y se acepta. El modo de la migración. */
    case Warn = 'warn';

    /** Falta la firma o no verifica → POISON. */
    case Require = 'require';
}
