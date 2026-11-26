<?php

declare(strict_types=1);

namespace Flux\Tests\Integration;

use PHPUnit\Framework\Assert;

/**
 * Un `nats-server` REAL con TLS, una nkey por servicio y las ACLs que genera `scripts/gen-acl.mjs`, aprovisionado con `flux provision`
 * (`entorno.mjs`, que reutiliza lo que usa la suite de seguridad del repo). **No es un broker abierto**: los permisos que prueba el SDK
 * son los del broker, y un mock no los aplica.
 *
 *   NATS_SERVER=/ruta/a/nats-server        binario (si no, el del PATH)
 *   FLUX_REQUIRE_BROKER=1                  su ausencia —o la de `node`, `openssl` o las dependencias de la suite— es un FALLO, no un salto
 *   FLUX_TEST_PORTS=7600-7699              rango de puertos (por defecto, ese)
 *
 * Sin los requisitos, las pruebas se SALTAN con aviso —salvo con `FLUX_REQUIRE_BROKER`, donde FALLAN—: un fixture que nadie ejecuta
 * aparenta cobertura.
 */
final class SecureBroker
{
    /** @var resource */
    private $proc;
    /** @var array<int,resource> */
    private array $pipes;
    /** @var array<string,mixed> */
    private array $env;

    private function __construct()
    {
    }

    public static function required(): bool
    {
        $v = strtolower(trim((string) getenv('FLUX_REQUIRE_BROKER')));

        return $v !== '' && $v !== '0' && $v !== 'false';
    }

    /** El motivo por el que no se puede arrancar, o `null`. */
    public static function missing(): ?string
    {
        $root = dirname(__DIR__, 3);
        $nats = getenv('NATS_SERVER') ?: 'nats-server';
        if (!is_executable($nats) && trim((string) shell_exec('command -v ' . escapeshellarg($nats) . ' 2>/dev/null')) === '') {
            return "no hay `nats-server` ({$nats}): define NATS_SERVER";
        }
        foreach (['node', 'openssl'] as $bin) {
            if (trim((string) shell_exec("command -v {$bin} 2>/dev/null")) === '') {
                return "no hay `{$bin}` en el PATH";
            }
        }
        foreach (['conformance', 'cli'] as $dir) {
            if (!is_dir("{$root}/{$dir}/node_modules/@nats-io")) {
                return "faltan las dependencias de {$dir}: (cd {$dir} && npm install)";
            }
        }
        if (!extension_loaded('sodium') || !extension_loaded('openssl')) {
            return 'faltan ext-sodium y ext-openssl';
        }

        return null;
    }

    /** Llamar desde `setUpBeforeClass`: salta (o falla) si no hay requisitos. */
    public static function guard(): void
    {
        $motivo = self::missing();
        if ($motivo === null) {
            return;
        }
        if (self::required()) {
            Assert::fail("{$motivo}. Con FLUX_REQUIRE_BROKER las pruebas de integración son obligatorias: saltarlas daría un CI verde sin haber ejecutado nada contra el broker.");
        }
        Assert::markTestSkipped("{$motivo} (define FLUX_REQUIRE_BROKER=1 para que sea un fallo)");
    }

    /** @param list<string> $args */
    public static function start(array $args = []): self
    {
        self::guard();
        $b = new self();
        $cmd = array_merge(['node', __DIR__ . '/entorno.mjs'], $args);
        $env = getenv();
        $env['FLUX_TEST_PORTS'] = getenv('FLUX_TEST_PORTS') ?: '7600-7699';
        $env['NO_COLOR'] = '1';
        $proc = proc_open($cmd, [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, dirname(__DIR__, 3), $env);
        if (!is_resource($proc)) {
            Assert::fail('no se pudo lanzar entorno.mjs');
        }
        $b->proc = $proc;
        $b->pipes = $pipes;
        stream_set_blocking($pipes[1], false);

        $line = '';
        $deadline = microtime(true) + 60;
        while (microtime(true) < $deadline) {
            $chunk = fread($pipes[1], 65_536);
            if ($chunk !== false && $chunk !== '') {
                $line .= $chunk;
                if (str_contains($line, "\n")) {
                    break;
                }
            } else {
                $st = proc_get_status($proc);
                if (!$st['running']) {
                    break;
                }
                usleep(20_000);
            }
        }
        $decoded = json_decode(trim($line), true);
        if (!is_array($decoded)) {
            $err = (string) stream_get_contents($pipes[2]);
            $b->stop();
            Assert::fail("entorno.mjs no arrancó: {$line}\n{$err}");
        }
        $b->env = $decoded;
        if (!in_array('--sin-aprovisionar', $args, true) && ($decoded['provision']['codigo'] ?? 1) !== 0) {
            $b->stop();
            Assert::fail('flux provision falló: ' . json_encode($decoded['provision']));
        }

        return $b;
    }

    public function stop(): void
    {
        if (isset($this->pipes[0]) && is_resource($this->pipes[0])) {
            fclose($this->pipes[0]); // cerrar stdin le dice a entorno.mjs que limpie
        }
        if (isset($this->proc) && is_resource($this->proc)) {
            $deadline = microtime(true) + 5;
            while (microtime(true) < $deadline && proc_get_status($this->proc)['running']) {
                usleep(50_000);
            }
            if (proc_get_status($this->proc)['running']) {
                proc_terminate($this->proc, SIGKILL);
            }
            foreach ($this->pipes as $p) {
                if (is_resource($p)) {
                    fclose($p);
                }
            }
            proc_close($this->proc);
        }
    }

    public function url(): string
    {
        return $this->env['url'];
    }

    public function port(): int
    {
        return (int) $this->env['puerto'];
    }

    public function ca(): string
    {
        return $this->env['ca'];
    }

    public function otraCa(): string
    {
        return $this->env['otraCa'];
    }

    /** Las semillas son SECRETOS de prueba, de un broker efímero. */
    public function nkeySeed(string $service): string
    {
        return $this->env['servicios'][$service]['semilla'] ?? Assert::fail("el manifiesto no tiene el servicio {$service}");
    }

    public function variantSeed(string $alias): string
    {
        return $this->env['variantes'][$alias]['semilla'] ?? Assert::fail("no se pidió la variante {$alias}");
    }

    /** @return array{creds:string} las credenciales de OPERACIÓN (`.creds`): el único usuario que aprovisiona. */
    public function ops(): array
    {
        return ['creds' => $this->env['credsOps']];
    }

    /** @return array<string,mixed> */
    public function extra(string $k): array
    {
        return $this->env[$k] ?? Assert::fail("entorno.mjs no levantó `{$k}`");
    }

    public function dir(): string
    {
        return $this->env['dir'];
    }

    /** Las conexiones que el servidor ve y las suscripciones de cada una (`/connz?subs=1`): lo que ningún cliente puede falsear. */
    /** @return list<array<string,mixed>> */
    public function connz(): array
    {
        $json = @file_get_contents($this->env['monitor'] . '/connz?subs=1&state=open');
        $d = is_string($json) ? json_decode($json, true) : null;

        return is_array($d) ? ($d['connections'] ?? []) : [];
    }

    /** Las suscripciones del servidor para la conexión de un servicio (por su `name`: `<servicio>@<entorno>`). */
    /** @return list<string> */
    public function subscriptionsOf(string $connectionName): array
    {
        $out = [];
        foreach ($this->connz() as $c) {
            if (($c['name'] ?? '') === $connectionName) {
                foreach ($c['subscriptions_list'] ?? [] as $s) {
                    $out[] = $s;
                }
            }
        }

        return $out;
    }

    /** El log de violaciones de permiso del servidor no está expuesto: se mide en la conexión (`Denial`). */
}
