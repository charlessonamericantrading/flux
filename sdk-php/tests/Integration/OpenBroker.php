<?php

declare(strict_types=1);

namespace Flux\Tests\Integration;

use Flux\ConnectOptions;
use Flux\Transport\SocketTransport;
use PHPUnit\Framework\Assert;

/**
 * Un `nats-server` real, ABIERTO (sin TLS ni ACL), de uno o de tres nodos, para lo que mide el PROTOCOLO y no la seguridad: los escenarios de
 * `conformance/cases/state-semantics.json` y de `rpc-semantics.json`, que ejecuta cada SDK con su API. Los de permisos van contra
 * `SecureBroker`; un broker abierto no prueba nada de seguridad.
 *
 * Tres nodos, porque en un stream R3 el conflicto de secuencia bajo carrera es el err **10164** y no el 10071 (A-49): un SDK que solo
 * reconozca el 10071 pasa todo contra un nodo y, en producción, no reintenta. Contra un nodo suelto no hay forma de verlo.
 *
 * Respeta `NATS_SERVER` (el binario) y `FLUX_TEST_PORTS` (`desde-hasta`). Mismo contrato que `SecureBroker`: sin binario, la prueba se
 * SALTA con aviso y con `FLUX_REQUIRE_BROKER=1` FALLA.
 */
final class OpenBroker
{
    /** @var list<resource> */
    private array $procs = [];
    /** @var list<int> */
    private array $clientPorts = [];
    private string $dir;

    private function __construct()
    {
    }

    public static function missing(): ?string
    {
        $nats = getenv('NATS_SERVER') ?: 'nats-server';
        if (!is_executable($nats) && trim((string) shell_exec('command -v ' . escapeshellarg($nats) . ' 2>/dev/null')) === '') {
            return "no hay `nats-server` ({$nats}): define NATS_SERVER";
        }

        return null;
    }

    public static function guard(): void
    {
        $motivo = self::missing();
        if ($motivo === null) {
            return;
        }
        if (SecureBroker::required()) {
            Assert::fail("{$motivo}. Con FLUX_REQUIRE_BROKER las pruebas de integración son obligatorias.");
        }
        Assert::markTestSkipped("{$motivo} (define FLUX_REQUIRE_BROKER=1 para que sea un fallo)");
    }

    public static function single(): self
    {
        return self::start(1);
    }

    public static function cluster3(): self
    {
        return self::start(3);
    }

    private static function start(int $nodes): self
    {
        self::guard();
        $b = new self();
        $nats = getenv('NATS_SERVER') ?: 'nats-server';
        $b->dir = sys_get_temp_dir() . '/flux-php-open-' . bin2hex(random_bytes(4));
        mkdir($b->dir, 0700, true);
        $ports = self::freePorts($nodes === 1 ? 1 : 6);
        for ($i = 0; $i < $nodes; $i++) {
            $d = "{$b->dir}/n{$i}";
            mkdir("{$d}/js", 0700, true);
            $conf = "listen: 127.0.0.1:{$ports[$i]}\nserver_name: n{$i}\njetstream { store_dir: \"{$d}/js\" }\n";
            if ($nodes > 1) {
                $routes = [];
                for ($j = 0; $j < $nodes; $j++) {
                    if ($j !== $i) {
                        $routes[] = 'nats://127.0.0.1:' . $ports[3 + $j];
                    }
                }
                $conf .= "cluster { name: fluxtest\n listen: 127.0.0.1:{$ports[3 + $i]}\n routes: [" . implode(', ', $routes) . "]\n}\n";
            }
            file_put_contents("{$d}/n.conf", $conf);
            $proc = proc_open([$nats, '-c', "{$d}/n.conf"], [0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', "{$d}/err.log", 'w']], $pipes);
            if (!is_resource($proc)) {
                $b->stop();
                Assert::fail('no se pudo lanzar nats-server');
            }
            $b->procs[] = $proc;
            $b->clientPorts[] = $ports[$i];
        }
        $b->waitReady($nodes === 3);

        return $b;
    }

    /** @return list<int> */
    private static function freePorts(int $n): array
    {
        [$from, $to] = array_map('intval', explode('-', getenv('FLUX_TEST_PORTS') ?: '7600-7699'));
        $chosen = [];
        for ($i = 0; $i < 400 && count($chosen) < $n; $i++) {
            $p = random_int($from, $to);
            if (in_array($p, $chosen, true)) {
                continue;
            }
            $s = @stream_socket_server("tcp://127.0.0.1:{$p}", $e, $m);
            if ($s !== false) {
                fclose($s);
                $chosen[] = $p;
            }
        }
        if (count($chosen) < $n) {
            Assert::fail("no hay {$n} puertos libres en {$from}-{$to}");
        }

        return $chosen;
    }

    private function waitReady(bool $replicated): void
    {
        $deadline = microtime(true) + 40;
        $last = '';
        while (microtime(true) < $deadline) {
            try {
                $ops = $this->ops();
                try {
                    $r = $ops->api('$JS.API.INFO');
                    if (!isset($r['error'])) {
                        if (!$replicated) {
                            return;
                        }
                        // La capa de metadatos tarda en elegir líder: hasta entonces un R3 no se acepta.
                        $c = $ops->api('$JS.API.STREAM.CREATE.SONDA', ['name' => 'SONDA', 'subjects' => ['sonda.>'], 'num_replicas' => 3]);
                        if (!isset($c['error'])) {
                            $ops->api('$JS.API.STREAM.DELETE.SONDA');

                            return;
                        }
                        $last = json_encode($c['error']);
                    }
                } finally {
                    $ops->close();
                }
            } catch (\Throwable $e) {
                $last = $e->getMessage();
            }
            usleep(250_000);
        }
        $this->stop();
        Assert::fail("el broker no estuvo listo en 40 s: {$last}");
    }

    /** Una conexión sin restricciones (no hay ACL): el aprovisionamiento y el testigo. */
    public function ops(string $service = 'ops-tests'): SocketTransport
    {
        return SocketTransport::connect(new ConnectOptions(
            service: $service,
            environment: 'test',
            version: '1.0.0',
            servers: $this->servers(),
            waitForBrokerMs: 2_000,
        ));
    }

    /** @return list<string> */
    public function servers(): array
    {
        return array_map(static fn (int $p): string => "nats://127.0.0.1:{$p}", $this->clientPorts);
    }

    public function stop(): void
    {
        foreach ($this->procs as $p) {
            if (is_resource($p)) {
                proc_terminate($p, SIGTERM);
            }
        }
        $deadline = microtime(true) + 3;
        foreach ($this->procs as $p) {
            while (is_resource($p) && microtime(true) < $deadline && proc_get_status($p)['running']) {
                usleep(50_000);
            }
            if (is_resource($p)) {
                if (proc_get_status($p)['running']) {
                    proc_terminate($p, SIGKILL);
                }
                proc_close($p);
            }
        }
        $this->procs = [];
        if (isset($this->dir) && is_dir($this->dir)) {
            // Solo el directorio propio, con un prefijo que no puede ser otro.
            if (str_contains($this->dir, 'flux-php-open-')) {
                shell_exec('rm -rf ' . escapeshellarg($this->dir));
            }
        }
    }
}
