<?php

declare(strict_types=1);

namespace Flux\Tests\Integration;

use Flux\ConnectOptions;
use Flux\FluxBus;
use Flux\Nats\Connection;
use Flux\Transport\SocketTransport;
use PHPUnit\Framework\TestCase;

/**
 * Lo común de las pruebas contra el broker seguro: construir un bus con la identidad de un servicio y hablar con el broker como
 * OPERACIÓN (el único que aprovisiona, y el testigo que mira lo que el SDK no ve).
 */
abstract class BrokerTestCase extends TestCase
{
    protected static SecureBroker $broker;

    /** @var list<FluxBus> */
    private array $buses = [];

    protected function tearDown(): void
    {
        foreach ($this->buses as $b) {
            try {
                $b->close();
            } catch (\Throwable) {
                // ya cerrado
            }
        }
        $this->buses = [];
    }

    /**
     * @param array<string,mixed> $extra argumentos de `ConnectOptions` que sustituyen a los de por defecto
     */
    protected function bus(string $service, array $extra = [], ?string $seed = null, ?ArrayLogger $logger = null): FluxBus
    {
        $bus = FluxBus::connect($this->options($service, $extra, $seed, $logger));
        $this->buses[] = $bus;

        return $bus;
    }

    /** @param array<string,mixed> $extra */
    protected function options(string $service, array $extra = [], ?string $seed = null, ?ArrayLogger $logger = null): ConnectOptions
    {
        return new ConnectOptions(...($extra + [
            'service' => $service,
            'environment' => 'test',
            'version' => '1.0.0',
            'tenantId' => 'acme',
            'schemaBaseUrl' => 'https://schemas.internal',
            'servers' => static::$broker->url(),
            'credentials' => ['nkeySeed' => $seed ?? static::$broker->nkeySeed($service)],
            'tls' => ['caFile' => static::$broker->ca()],
            'logger' => $logger,
            'pendingPollMs' => 0,
        ]));
    }

    /** Una conexión de OPERACIÓN: sin restricciones. Con ella se mira lo que el SDK no puede ver. */
    protected function ops(): SocketTransport
    {
        return SocketTransport::connect(new ConnectOptions(
            service: 'ops-tests',
            environment: 'test',
            version: '1.0.0',
            servers: static::$broker->url(),
            credentials: static::$broker->ops(),
            tls: ['caFile' => static::$broker->ca()],
        ));
    }

    protected static function connection(FluxBus $bus): Connection
    {
        $t = (new \ReflectionProperty($bus, 'transport'))->getValue($bus);
        self::assertInstanceOf(SocketTransport::class, $t);

        return $t->connection();
    }

    /** Milisegundos que tarda `$f`, y lo que lanzó (si lanzó). */
    protected static function medir(callable $f): array
    {
        $t0 = microtime(true);
        $error = null;
        $valor = null;
        try {
            $valor = $f();
        } catch (\Throwable $e) {
            $error = $e;
        }

        return [(microtime(true) - $t0) * 1000, $error, $valor];
    }
}
