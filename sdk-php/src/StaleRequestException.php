<?php

declare(strict_types=1);

namespace Flux;

/**
 * Con la firma en `require`, el `time` de la petición se aleja más de 300 s del reloj de quien responde, por delante o por
 * detrás (11-request-reply.md §7.4). PERMANENT: es lo que quien responde contesta como error, no se lanza al llamar.
 */
final class StaleRequestException extends CodedError
{
    public function __construct(string $time, int $windowSeconds, string $now, ?\Throwable $previous = null)
    {
        parent::__construct(
            'STALE_REQUEST',
            ErrorClass::Permanent,
            "la petición lleva `time` {$time}, a más de {$windowSeconds} s de la hora de quien responde ({$now}). Con la firma en "
            . '`require` la ventana es simétrica (también una petición del futuro es sospechosa): la firma por sí sola no impide '
            . 'reenviar una petición capturada. Sincroniza los relojes (NTP) o reconstruye la petición (11-request-reply.md §7.4).',
            $previous,
        );
    }
}
