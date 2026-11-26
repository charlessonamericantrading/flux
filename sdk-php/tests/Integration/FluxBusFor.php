<?php

declare(strict_types=1);

namespace Flux\Tests\Integration;

use Flux\ConnectOptions;
use Flux\FluxBus;

/** Un bus con la identidad de un servicio contra un broker concreto (cuando una prueba levanta más de uno). */
final class FluxBusFor
{
    /** @param array<string,mixed> $extra */
    public static function connect(SecureBroker $broker, string $service, array $extra = [], ?string $seed = null): FluxBus
    {
        return FluxBus::connect(new ConnectOptions(...($extra + [
            'service' => $service,
            'environment' => 'test',
            'version' => '1.0.0',
            'tenantId' => 'acme',
            'schemaBaseUrl' => 'https://schemas.internal',
            'servers' => $broker->url(),
            'credentials' => ['nkeySeed' => $seed ?? $broker->nkeySeed($service)],
            'tls' => ['caFile' => $broker->ca()],
            'pendingPollMs' => 0,
        ])));
    }
}
