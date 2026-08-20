<?php

declare(strict_types=1);

namespace Flux\Signing;

use Flux\Envelope;
use Flux\FluxEvent;
use Flux\PoisonError;
use Psr\Log\LoggerInterface;

/**
 * Verifica la firma de los eventos consumidos, según la política de 07-signing.md §7.
 *
 * Los tres códigos POISON son **contrato entre SDKs**, igual que los de `Envelope::parse()`:
 * si dos SDKs emitieran códigos distintos ante la misma entrada, agrupar por causa en las
 * métricas dejaría de funcionar en cuanto el ecosistema es polyglot — que es siempre.
 *
 * | Situación | `errorCode` |
 * |---|---|
 * | Falta `signature` en modo `require` | `MISSING_SIGNATURE` |
 * | La firma no verifica | `INVALID_SIGNATURE` |
 * | `signkeyid` desconocido | `UNKNOWN_SIGNING_KEY` |
 */
final class Verifier
{
    /**
     * @param array<string,string> $keys `signkeyid` → 32 bytes de clave pública.
     */
    private function __construct(
        private readonly array $keys,
        private readonly VerificationMode $mode,
        private readonly ?LoggerInterface $logger,
    ) {
    }

    /**
     * Construye el verificador, o `null` en modo `off` — no se paga lo que no se usa.
     *
     * @throws SigningKeyException si el modo no es `off` y no hay claves públicas, si
     *         alguna no es una Ed25519 reconocible, o si `ext-sodium` no está disponible.
     */
    public static function fromOptions(?SigningOptions $options, ?LoggerInterface $logger = null): ?self
    {
        if ($options === null || $options->verify === VerificationMode::Off) {
            return null;
        }

        Keys::assertSodiumAvailable();

        $keys = [];
        foreach ($options->publicKeys as $id => $material) {
            try {
                $keys[$id] = Keys::publicKeyBytes($material);
            } catch (SigningKeyException $e) {
                throw new SigningKeyException("clave pública \"{$id}\": " . $e->getMessage(), 0, $e);
            }
        }

        if ($keys === []) {
            throw new SigningKeyException(sprintf(
                'signing.verify = "%s" requiere signing.publicKeys. Incluye también las claves '
                . 'RETIRADAS mientras existan eventos firmados con ellas: retirar una clave '
                . 'impide EMITIR con ella, no VERIFICAR lo ya emitido, y tratarla como inválida '
                . 'convertiría una rotación rutinaria en la invalidación retroactiva de todo el '
                . 'historial (07-signing.md §6).',
                $options->verify->value,
            ));
        }

        return new self($keys, $options->verify, $logger);
    }

    /**
     * Aplica la política al evento.
     *
     * Devuelve `null` si la firma verificó. **Devuelve el código si el modo es `warn`: el
     * evento se acepta, pero el llamante DEBE contarlo** como
     * `flux_events_consumed_total{outcome="invalid_signature"}` — 07-signing.md §7.1.
     *
     * Que el código salga por el valor de retorno y no se quede en el log es deliberado:
     * sin esa métrica, `warn` es inútil para lo único que existe, **pilotar la migración**.
     * La pregunta que hay que poder responder antes de pasar a `require` es "¿cuántos
     * eventos siguen sin firma y de qué productores?", y un log no la contesta — hay que
     * buscarla a mano en siete servicios.
     *
     * En `require` lanza `PoisonError`.
     *
     * @return string|null El código del fallo si el modo es `warn`, `null` si verificó.
     *
     * @throws PoisonError
     * @throws \Flux\EnvelopeException
     */
    public function check(FluxEvent $event): ?string
    {
        if ($event->signature === null) {
            return $this->fail('MISSING_SIGNATURE', "el evento {$event->id} no está firmado");
        }

        if ($event->signkeyid === null) {
            return $this->fail(
                'UNKNOWN_SIGNING_KEY',
                "el evento {$event->id} trae `signature` pero no `signkeyid`, así que no hay "
                . 'forma de saber con qué clave verificarlo (07-signing.md §4)'
            );
        }

        $key = $this->keys[$event->signkeyid] ?? null;
        if ($key === null) {
            // Una clave DESCONOCIDA no es lo mismo que una RETIRADA: si el id no está en el
            // mapa, o el operador la retiró sin conservar la pública —y eso invalida
            // retroactivamente el historial— o el evento viene de fuera del ecosistema.
            return $this->fail(
                'UNKNOWN_SIGNING_KEY',
                "el evento {$event->id} está firmado con signkeyid=\"{$event->signkeyid}\", que "
                . 'no está entre las claves conocidas. ¿Se retiró sin conservar la pública? Las '
                . 'públicas retiradas DEBEN conservarse mientras exista algún evento firmado con '
                . 'ellas, mínimo 90 días (07-signing.md §6)'
            );
        }

        $firma = Base64Url::decode($event->signature);

        // `sodium_crypto_sign_verify_detached` lanza `SodiumException` si la firma no mide
        // exactamente 64 bytes, así que la longitud se comprueba antes: una firma corta es
        // un evento manipulado, y su respuesta es INVALID_SIGNATURE, no una excepción de
        // tipo distinto que el runtime clasificaría como un error cualquiera.
        $ok = $firma !== null
            && strlen($firma) === SODIUM_CRYPTO_SIGN_BYTES
            && sodium_crypto_sign_verify_detached($firma, Envelope::signablePayload($event), $key);

        if (!$ok) {
            return $this->fail(
                'INVALID_SIGNATURE',
                "la firma del evento {$event->id} no verifica con la clave "
                . "\"{$event->signkeyid}\". El evento fue alterado después de firmarse, o no lo "
                . 'emitió quien dice'
            );
        }

        return null;
    }

    /**
     * En `require` lanza; en `warn` registra, **devuelve el código para que el llamante lo
     * cuente** (§7.1) y acepta.
     *
     * @throws PoisonError
     */
    private function fail(string $code, string $message): string
    {
        if ($this->mode === VerificationMode::Require) {
            throw new PoisonError($message, $code);
        }

        // `off` no llega aquí: en ese modo no se construye verificador. Queda `warn`, que
        // es lo que permite migrar un ecosistema en el que unos productores ya firman y
        // otros no (§7).
        //
        // El log es un extra y cada plataforma lo resuelve a su manera —§7.1 prohíbe
        // explícitamente imponer una fachada de logging—; **la parte normativa es la
        // métrica**, y por eso el código sale por el valor de retorno.
        $this->logger?->warning("[flux] {$code}: {$message}");

        return $code;
    }
}
