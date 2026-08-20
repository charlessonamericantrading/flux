<?php

declare(strict_types=1);

namespace Flux\Signing;

use Flux\Envelope;
use Flux\FluxEvent;

/**
 * Firma eventos al publicar con Ed25519.
 * Contrato normativo: specification/07-signing.md
 *
 * **Ed25519 sin negociación de algoritmo, a propósito** (§3). La razón es de superficie de
 * error, no de rendimiento: los formatos de firma con algoritmo negociable acumulan una
 * familia de vulnerabilidades bien conocida —desde `alg: none` hasta la confusión entre
 * HMAC y RSA— que solo existe porque hay algo que negociar. Aquí no lo hay.
 *
 * Usa `sodium_crypto_sign_detached`, que está en el **core de PHP desde 7.2**: la firma no
 * añade ninguna dependencia de Composer. Lo único que hay que comprobar es que la
 * extensión esté cargada, y de eso se encarga `Keys::assertSodiumAvailable()`.
 */
final class Signer
{
    /** 64 bytes: semilla ‖ pública, el formato que espera libsodium. */
    private readonly string $secretKey;

    /** @throws SigningKeyException */
    private function __construct(string $seed, public readonly string $keyId)
    {
        $this->secretKey = Keys::secretKeyFromSeed($seed);
    }

    /**
     * Construye el firmante, o `null` si no hay clave privada configurada.
     *
     * @throws SigningKeyException si hay clave pero no `keyId`, o si la clave no es una
     *         Ed25519 reconocible, o si `ext-sodium` no está disponible.
     */
    public static function fromOptions(?SigningOptions $options): ?self
    {
        if ($options === null || $options->privateKey === null) {
            return null;
        }

        Keys::assertSodiumAvailable();

        if ($options->keyId === null || $options->keyId === '') {
            throw new SigningKeyException(
                'signing.privateKey requiere signing.keyId: sin él, un verificador no sabe qué '
                . 'clave pública usar (07-signing.md §4). El formato es `<servicio>-<n>`, '
                . 'p. ej. "pedidos-api-3", y DEBE cambiar en cada rotación (§6).'
            );
        }

        return new self(Keys::privateKeySeed($options->privateKey), $options->keyId);
    }

    /**
     * Devuelve una copia del evento con `signkeyid` y `signature` puestos, ambos **antes de
     * `data`** — 07-signing.md §4.
     *
     * Firmar es **lo último antes de serializar**: la firma cubre el envelope completo, así
     * que cualquier atributo añadido después la invalidaría (§5).
     *
     * Es determinista — Ed25519 no consume aleatoriedad por firma—, así que el mismo evento
     * produce siempre exactamente la misma firma. Eso solo es cierto porque el envelope
     * tiene una única representación en bytes; sin 01-envelope.md §1.1, §2.2 y §6 una firma
     * de PHP no verificaría en Go.
     *
     * @throws \Flux\EnvelopeException
     */
    public function sign(FluxEvent $event): FluxEvent
    {
        // `signkeyid` se pone ANTES de calcular los bytes: va DENTRO de lo firmado (§5).
        // Si quedara fuera, un atacante podría cambiarlo para que la verificación buscara
        // otra clave.
        $conKeyId = $event->withSignature($this->keyId);

        $firma = sodium_crypto_sign_detached(
            Envelope::signablePayload($conKeyId),
            $this->secretKey,
        );

        return $conKeyId->withSignature($this->keyId, Base64Url::encode($firma));
    }
}
