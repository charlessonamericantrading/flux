<?php

declare(strict_types=1);

namespace Flux\Signing;

/**
 * Material de clave o configuración de firma inválidos — 07-signing.md.
 *
 * Es un error de **arranque**, no de mensaje: lo lanza `Signer`/`Verifier` al construirse,
 * antes de tocar la red. Un evento con la firma rota no produce esto, produce un
 * `Flux\PoisonError` con `INVALID_SIGNATURE`.
 *
 * No extiende `Flux\FluxError` a propósito: `FluxError` obliga a declarar una de las tres
 * clases del protocolo (04-errors.md §1) y ninguna encaja — esto no es un evento que
 * clasificar, es una configuración que no arranca.
 */
final class SigningKeyException extends \RuntimeException
{
}
