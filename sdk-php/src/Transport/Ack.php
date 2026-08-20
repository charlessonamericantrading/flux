<?php

declare(strict_types=1);

namespace Flux\Transport;

/**
 * El protocolo de acuse de JetStream, que es más simple de lo que parece: **acusar recibo
 * es publicar un token en el subject de respuesta del mensaje**. Por eso el transporte de
 * este SDK solo necesita `publish()` y no una API de mensajes del cliente de NATS.
 *
 * Tenerlo aquí y no dentro del adaptador tiene un motivo práctico: el número de entrega
 * —del que depende todo el presupuesto de reintentos de 04-errors.md— se extrae del
 * subject de respuesta, y así se puede testear sin broker.
 */
final class Ack
{
    /** Procesado correctamente. JetStream lo da por entregado y no vuelve. */
    public const ACK = '+ACK';

    /** No procesado; reentrégalo. Admite `-NAK {"delay": <nanosegundos>}`. */
    public const NAK = '-NAK';

    /** No lo reintentes nunca más. Es lo que precede a escribir en la DLQ. */
    public const TERM = '+TERM';

    /**
     * Work-in-progress: "sigo vivo, reinicia el plazo de `ack_wait`".
     *
     * El token es `+WPI` (sí, con las letras en ese orden — es el literal que espera el
     * servidor, no una errata de "WIP").
     */
    public const WIP = '+WPI';

    private function __construct()
    {
    }

    /**
     * `-NAK {"delay": 30000000000}` — reintento explícito tras N milisegundos.
     *
     * Se prefiere al `-NAK` pelado porque un nak con retraso libera la ranura de
     * `max_ack_pending` de forma predecible, en vez de dejar expirar `ack_wait` y retener
     * la ranura 30 s de más (03-delivery.md, nota de §2.1 sobre `num_ack_pending`).
     */
    public static function nakWithDelay(float $delayMs): string
    {
        $nanos = (int) round($delayMs * 1_000_000);

        return self::NAK . ' ' . json_encode(['delay' => $nanos], JSON_THROW_ON_ERROR);
    }

    /**
     * Número de entrega de este mensaje, leído del subject de respuesta.
     *
     * JetStream lo codifica en el propio subject `$JS.ACK.…` en dos formatos según la
     * versión del servidor, y hay que soportar los dos:
     *
     *     v1 (9 tokens):  $JS.ACK.<stream>.<consumer>.<entregas>.<sseq>.<cseq>.<tm>.<pending>
     *     v2 (12 tokens): $JS.ACK.<dominio>.<hash cuenta>.<stream>.<consumer>.<entregas>.…
     *
     * Devuelve 1 si no se puede determinar. Ese default es deliberado y conservador: la
     * alternativa —asumir el último intento— mandaría a la DLQ un evento que aún tenía
     * reintentos por delante, y hacia ese lado el error no se recupera solo.
     */
    public static function deliveryAttempt(?string $replyTo): int
    {
        if ($replyTo === null) {
            return 1;
        }

        $tokens = explode('.', $replyTo);

        $offset = match (true) {
            count($tokens) === 9 => 4,
            count($tokens) >= 12 => 6,
            default => null,
        };

        if ($offset === null || !ctype_digit($tokens[$offset])) {
            return 1;
        }

        return max(1, (int) $tokens[$offset]);
    }
}
