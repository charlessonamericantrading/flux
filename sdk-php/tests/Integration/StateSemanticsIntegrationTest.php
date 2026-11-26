<?php

declare(strict_types=1);

namespace Flux\Tests\Integration;

use Flux\CodedError;
use Flux\ConnectOptions;
use Flux\FluxBus;
use Flux\State\StateEntry;
use Flux\Transport\SocketTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

/**
 * Los escenarios de `conformance/cases/state-semantics.json` ejecutados con LA API DE PHP (`$bus->state->put | delete | get | watch`) contra un
 * `nats-server` real. Son los mismos que ejecutan Node y Rust: lo esperado es lo del fichero, no lo que a este SDK le salga.
 *
 * Los casos son DATOS: este fichero no sabe qué es «una versión que no avanza» ni qué es un `onSynced` inmediato; traduce el vocabulario del
 * caso (`state_put`, `state_watch`…) a llamadas del SDK y compara lo observado con `expect`. Las observaciones `attempts` y `ownWrite` no son
 * parte de la API: se miden DESDE FUERA, contando lecturas y publicaciones en un transporte que las cuenta (`HookedStateTransport`), que es
 * también donde se inyectan los «ganchos» del caso.
 *
 * `state_race` usa **procesos**: la concurrencia de PHP son varios procesos escribiendo la misma clave (FPM, workers), no hilos.
 * Los casos de tres nodos (A-49: bajo carrera el conflicto es 10164) corren contra un cluster local de tres.
 */
#[Group('integration')]
final class StateSemanticsIntegrationTest extends TestCase
{
    private static ?OpenBroker $single = null;
    private static ?OpenBroker $cluster = null;

    /** @var list<FluxBus> */
    private array $open = [];
    private OpenBroker $broker;
    private ?SocketTransport $ops = null;
    /** @var array{bus:FluxBus,hook:HookedStateTransport} */
    private array $principal;
    /** @var array{bus:FluxBus,hook:HookedStateTransport} */
    private array $rival;

    public static function tearDownAfterClass(): void
    {
        self::$single?->stop();
        self::$cluster?->stop();
        self::$single = self::$cluster = null;
    }

    /** @return array<string,array{0:string}> */
    public static function casos(): array
    {
        $out = [];
        foreach (self::file()['cases'] as $c) {
            $out[$c['id']] = [$c['id']];
        }

        return $out;
    }

    /** @return array<string,mixed> */
    private static function file(): array
    {
        static $f = null;

        return $f ??= json_decode((string) file_get_contents(dirname(__DIR__, 3) . '/conformance/cases/state-semantics.json'), true, 512, JSON_THROW_ON_ERROR);
    }

    public function testElRunnerEntiendeCadaOperacionDeLosCasos(): void
    {
        $conocidas = ['stream_add', 'stream_state', 'state_put', 'state_delete', 'state_get', 'state_watch', 'state_race'];
        $casos = self::file()['cases'];
        self::assertGreaterThanOrEqual(10, count($casos));
        foreach ($casos as $c) {
            foreach ($c['steps'] as $s) {
                self::assertContains($s['op'], $conocidas, "{$c['id']}: operación «{$s['op']}» sin implementar en este runner");
            }
            self::assertEmpty($c['acl'] ?? null, 'ningún caso de semántica pide una ACL: los de permisos están en StateIntegrationTest');
        }
    }

    #[DataProvider('casos')]
    public function testCaso(string $id): void
    {
        $caso = null;
        foreach (self::file()['cases'] as $c) {
            if ($c['id'] === $id) {
                $caso = $c;
            }
        }
        self::assertNotNull($caso);
        $cluster = ($caso['topology'] ?? 'single') === 'cluster3';
        if ($cluster) {
            self::$cluster ??= OpenBroker::cluster3();
            $this->broker = self::$cluster;
        } else {
            self::$single ??= OpenBroker::single();
            $this->broker = self::$single;
        }
        $this->ops = $this->broker->ops();
        // cada caso parte de un broker sin streams
        $names = $this->ops->api('$JS.API.STREAM.NAMES')['streams'] ?? [];
        foreach ($names as $n) {
            $this->ops->api('$JS.API.STREAM.DELETE.' . $n);
        }
        $this->principal = $this->instance('crm-api');
        $this->rival = $this->instance('crm-rival');

        $fallos = [];
        $expectativas = 0;
        foreach ($caso['steps'] as $i => $step) {
            try {
                $obs = $this->step($step);
            } catch (\Throwable $e) {
                $fallos[] = 'paso ' . ($i + 1) . " ({$step['op']}): " . $e::class . ': ' . $e->getMessage() . "\n" . $e->getTraceAsString();
                break;
            }
            foreach ($step['expect'] ?? [] as $path => $esperado) {
                $expectativas++;
                $v = self::path($obs, $path);
                if (!self::cumple($v, self::resolve($esperado))) {
                    $fallos[] = 'paso ' . ($i + 1) . ' (' . ($step['title'] ?? $step['op']) . "): «{$path}» = " . self::j($v) . ', se esperaba ' . self::j($esperado)
                        . '  [observado: ' . substr(self::j($obs), 0, 400) . ']';
                }
            }
        }
        foreach ($this->open as $b) {
            try {
                $b->close();
            } catch (\Throwable) {
            }
        }
        $this->open = [];
        $this->ops->close();
        self::assertSame([], $fallos, implode("\n", $fallos));
        self::assertGreaterThan(0, $expectativas, 'un caso sin ninguna expectativa comprobada no demuestra nada');
    }

    // ─── El broker de tres nodos, medido ──────────────────────────────────────────────────────────

    public function testControlBajoCarreraElBrokerContestaElDiezMilCientoSesentaYCuatroYElSdkLoReintenta(): void
    {
        // El caso `cluster-three-owner-instances-converge` pasa si el SDK reconoce el 10164... pero también podría pasar si nunca hubiera
        // conflicto. Aquí se MIDE: varios escritores con la MISMA S, y se cuentan los códigos que devuelve el broker.
        self::$cluster ??= OpenBroker::cluster3();
        $ops = self::$cluster->ops();
        $r = $ops->api('$JS.API.STREAM.CREATE.STATE_CARRERA', [
            'name' => 'STATE_CARRERA', 'subjects' => ['state.carrera.>'], 'storage' => 'file', 'max_msgs_per_subject' => 1,
            'allow_direct' => true, 'discard' => 'old', 'num_replicas' => 3,
        ]);
        self::assertArrayNotHasKey('error', $r, json_encode($r));
        $subject = 'state.carrera.cosa.v1.acme.k';
        $codigos = [];
        $conns = [];
        for ($i = 0; $i < 8; $i++) {
            $conns[] = self::$cluster->ops("crm-c{$i}");
        }
        for ($ronda = 0; $ronda < 15; $ronda++) {
            $g = $ops->directGet('STATE_CARRERA', $subject);
            $s = $g === null ? 0 : $g['seq'];
            // 8 publicaciones con la MISMA S, a la vez: se abren todas y se recogen después (PHP no tiene hilos, el cable sí es paralelo)
            $replies = [];
            foreach ($conns as $i => $c) {
                $conn = $c->connection();
                $token = $conn->openReply();
                $mark = $conn->denialMark();
                $conn->publish($subject, json_encode(['r' => $ronda, 'i' => $i]), ['Nats-Expected-Last-Subject-Sequence' => (string) $s], $conn->replySubject($token));
                $replies[] = [$conn, $token, $mark];
            }
            foreach ($replies as [$conn, $token, $mark]) {
                $m = $conn->awaitReply($token, $subject, $mark, 5_000);
                $d = json_decode($m->payload, true);
                if (isset($d['error']['err_code'])) {
                    $codigos[$d['error']['err_code']] = true;
                }
                $conn->closeReply($token);
            }
        }
        foreach ($conns as $c) {
            $c->close();
        }
        $ops->close();
        self::assertNotEmpty($codigos, 'no hubo ningún conflicto: la carga no mide nada');
        foreach (array_keys($codigos) as $c) {
            self::assertContains($c, [10071, 10164], "códigos de conflicto inesperados: " . implode(',', array_keys($codigos)));
        }
    }

    // ─── Los pasos ────────────────────────────────────────────────────────────────────────────────

    /** @return array{bus:FluxBus,hook:HookedStateTransport} */
    private function instance(string $service, ?string $tenant = null): array
    {
        $options = new ConnectOptions(
            service: $service,
            environment: 'test',
            version: '1.0.0',
            tenantId: $tenant,
            schemaBaseUrl: 'https://schemas.internal',
            servers: $this->broker->servers(),
            pendingPollMs: 0,
            waitForBrokerMs: 5_000,
        );
        $hook = new HookedStateTransport(SocketTransport::connect($options));

        return ['bus' => FluxBus::connect($options, $hook), 'hook' => $hook];
    }

    /** @param array<string,mixed> $step @return array<string,mixed> */
    private function step(array $step): array
    {
        $a = self::resolve($step);
        switch ($a['op']) {
            case 'stream_add':
                $cfg = ($a['config'] ?? []) + [
                    'name' => $a['name'], 'subjects' => $a['subjects'], 'storage' => 'file', 'max_msgs_per_subject' => 1, 'allow_direct' => true,
                    'max_age' => 0, 'discard' => 'old', 'duplicate_window' => 120_000_000_000, 'num_replicas' => 1,
                ];
                $r = $this->ops->api('$JS.API.STREAM.CREATE.' . $a['name'], $cfg);
                if (isset($r['error'])) {
                    return ['outcome' => 'error', 'errCode' => $r['error']['err_code'] ?? $r['error']['code'] ?? null, 'text' => $r['error']['description'] ?? ''];
                }

                return ['outcome' => 'ok', 'config' => $r['config'] ?? []];
            case 'stream_state':
                $i = $this->ops->api('$JS.API.STREAM.INFO.' . $a['name']);

                return [
                    'messages' => $i['state']['messages'], 'numSubjects' => $i['state']['num_subjects'] ?? 0,
                    'firstSeq' => $i['state']['first_seq'], 'lastSeq' => $i['state']['last_seq'],
                ];
            case 'state_put':
            case 'state_delete':
                return $this->write($a);
            case 'state_get':
                return $this->read($a);
            case 'state_watch':
                return $this->watch($a);
            case 'state_race':
                return $this->race($a);
        }

        throw new \LogicException("operación sin implementar: {$a['op']}");
    }

    /** @param array<string,mixed> $a @return array<string,mixed> */
    private function write(array $a): array
    {
        ['bus' => $bus, 'hook' => $hook] = $this->principal;
        $hook->resetCounters();
        $hook->loseAckOnce = (bool) ($a['hook']['lostAckOnce'] ?? false);
        $hook->beforePublish = null;
        if (isset($a['hook']['beforePublish'])) {
            $rivals = $a['hook']['beforePublish'];
            $hook->beforePublish = function (int $n) use ($rivals, $a): void {
                // Un rival escribe entre la lectura y la publicación del intento `n`.
                foreach ($rivals as $r) {
                    if ($r['attempt'] === $n) {
                        for ($v = $r['fromVersion']; $v < $r['fromVersion'] + ($r['count'] ?? 1); $v++) {
                            $this->rival['bus']->state->put($a['contract'], $a['key'], self::data($a['key'], $v), tenantId: $a['tenant']);
                        }
                    }
                }
            };
        }
        try {
            $r = $a['op'] === 'state_put'
                ? $bus->state->put($a['contract'], $a['key'], self::data($a['key'], $a['version']), tenantId: $a['tenant'], classification: $a['classification'] ?? null)
                : $bus->state->delete($a['contract'], $a['key'], $a['version'], tenantId: $a['tenant']);

            return [
                'outcome' => $r->outcome, 'applied' => $r->applied, 'result' => $r->outcome, 'revision' => $r->revision,
                'attempts' => $hook->reads,
                // el acuse se perdió y el reintento reconoció su propio mensaje: hubo MÁS lecturas que publicaciones
                'ownWrite' => $r->outcome === 'ok' && $hook->reads > $hook->publications,
            ];
        } catch (\Throwable $e) {
            return ['outcome' => 'error', 'errCode' => $e instanceof CodedError ? $e->errorCode : null, 'text' => $e->getMessage(), 'attempts' => $hook->reads];
        } finally {
            $hook->beforePublish = null;
            $hook->loseAckOnce = false;
        }
    }

    /** @return array<string,mixed> */
    private static function data(string $key, mixed $version): array
    {
        return ['clienteId' => $key, 'aggregateVersion' => $version];
    }

    /** @param array<string,mixed> $a @return array<string,mixed> */
    private function read(array $a): array
    {
        $e = $this->principal['bus']->state->get($a['contract'], $a['key'], tenantId: $a['tenant']);
        if ($e !== null) {
            return ['entry' => self::entry($e), 'isNull' => false, 'revision' => $e->revision, 'aggregateVersion' => $e->aggregateVersion, 'deleted' => false];
        }
        // `null` es TODO lo que el SDK dice de una clave inexistente y de una lápida. Para distinguirlas (y comprobar que la lápida lleva la
        // versión que se escribió) se mira el stream SIN el SDK, como la referencia.
        [$d, $ag, $v] = explode('.', $a['contract']);
        $stream = 'STATE_' . strtoupper(str_replace('-', '_', $d));
        $raw = $this->ops->directGet($stream, "state.{$d}.{$ag}.{$v}.{$a['tenant']}.{$a['key']}");
        if ($raw === null) {
            return ['entry' => null, 'isNull' => true];
        }
        $ev = json_decode($raw['data'], true);

        return [
            'entry' => null, 'isNull' => true, 'revision' => $raw['seq'], 'aggregateVersion' => $ev['data']['aggregateVersion'],
            'deleted' => preg_match('/\.deleted\.v[1-9][0-9]*$/', $ev['type']) === 1,
        ];
    }

    /** @return array<string,mixed> */
    private static function entry(StateEntry $e): array
    {
        return ['key' => $e->key, 'tenantId' => $e->tenantId, 'deleted' => $e->deleted, 'data' => $e->data, 'aggregateVersion' => $e->aggregateVersion, 'revision' => $e->revision];
    }

    /** @param array<string,mixed> $a @return array<string,mixed> */
    private function watch(array $a): array
    {
        // El durable lo deriva el SDK: <servicio>__state_<dom>_<agr>_v<M>[__<tenant>]. El caso dice cuál DEBE salir.
        $service = $a['service'] ?? explode('__state_', $a['durable'])[0];
        $inst = $this->instance($service, $a['tenant'] ?? null);
        $this->open[] = $inst['bus'];
        if (isset($a['afterCreate'])) {
            // «Escrituras durante la sincronización»: con el consumidor ya creado y su `num_pending` ya leído, antes de consumir.
            $inst['hook']->afterEnsureConsumer = function () use ($a): void {
                foreach ($a['afterCreate'] as $x) {
                    $this->rival['bus']->state->put($a['contract'], $x['key'], self::data($x['key'], $x['version']), tenantId: $x['tenant'] ?? $a['tenant'] ?? 'acme');
                }
            };
        }
        $entries = [];
        $syncedAfter = null;
        $watch = $inst['bus']->state->watch(
            $a['contract'],
            function (StateEntry $e) use (&$entries): void {
                $entries[] = $e;
            },
            tenantId: $a['tenant'] ?? null,
            onSynced: function () use (&$entries, &$syncedAfter): void {
                $syncedAfter ??= count($entries);
            },
        );
        self::assertSame($a['durable'], $watch->durable, 'el durable que deriva el SDK no es el que dice el caso');

        // Se espera a la sincronización y a que ya no llegue nada más (un caso que dice `count: 3` debe fallar si llegan 4).
        $limit = microtime(true) + 15;
        while ($syncedAfter === null && microtime(true) < $limit) {
            $inst['bus']->state->poll(100);
        }
        $quiet = microtime(true);
        $n = count($entries);
        while (microtime(true) - $quiet < 0.6 && microtime(true) < $limit) {
            $inst['bus']->state->poll(100);
            if (count($entries) !== $n) {
                $n = count($entries);
                $quiet = microtime(true);
            }
        }
        $inst['bus']->close();

        return [
            'count' => count($entries),
            'keys' => array_map(static fn (StateEntry $e): string => $e->key, $entries),
            'entries' => array_map(self::entry(...), $entries),
            'syncedImmediately' => $syncedAfter === 0,
            'syncedAtIndex' => $syncedAfter === null ? null : $syncedAfter - 1,
        ];
    }

    /** @param array<string,mixed> $a @return array<string,mixed> */
    private function race(array $a): array
    {
        $procs = [];
        for ($n = 0; $n < $a['instances']; $n++) {
            $args = json_encode([
                'service' => "crm-{$n}", 'servers' => $this->broker->servers(), 'contract' => $a['contract'], 'tenant' => $a['tenant'], 'key' => $a['key'],
                'from' => $a['from'], 'to' => $a['to'], 'instances' => $a['instances'], 'n' => $n, 'pauseMs' => $a['pauseMs'] ?? null,
            ]);
            $p = proc_open([PHP_BINARY, __DIR__ . '/StateRaceWorker.php', $args], [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
            $procs[] = [$p, $pipes];
        }
        // Todos conectan y esperan; cuando el último está listo, arrancan A LA VEZ.
        foreach ($procs as [, $pipes]) {
            self::assertSame("ready\n", fgets($pipes[1]), 'un escritor no llegó a conectar');
        }
        foreach ($procs as [, $pipes]) {
            fwrite($pipes[0], "go\n");
            fclose($pipes[0]);
        }
        $total = ['ok' => 0, 'stale' => 0, 'conflict' => 0, 'other' => 0];
        $errors = [];
        foreach ($procs as [$p, $pipes]) {
            $out = stream_get_contents($pipes[1]);
            $err = stream_get_contents($pipes[2]);
            fclose($pipes[1]);
            fclose($pipes[2]);
            proc_close($p);
            $r = str_starts_with($out, 'result ') ? json_decode(substr($out, 7), true) : null;
            if (!is_array($r)) {
                $total['other']++;
                $errors[] = "el escritor no dio JSON: {$out} {$err}";
                continue;
            }
            foreach (['ok', 'stale', 'conflict', 'other'] as $k) {
                $total[$k] += $r[$k];
            }
            $errors = array_merge($errors, $r['errors']);
        }
        // un conflicto es RETRYABLE: la aplicación reintenta hasta que no lo haya
        $bus = $this->principal['bus'];
        $fin = $bus->state->get($a['contract'], $a['key'], tenantId: $a['tenant']);
        for ($i = 0; $i < 10 && $fin?->aggregateVersion !== $a['to']; $i++) {
            try {
                $bus->state->put($a['contract'], $a['key'], self::data($a['key'], $a['to']), tenantId: $a['tenant']);
            } catch (\Throwable) {
            }
            $fin = $bus->state->get($a['contract'], $a['key'], tenantId: $a['tenant']);
        }

        return ['finalVersion' => $fin?->aggregateVersion, 'errors' => $errors] + $total;
    }

    // ─── El lenguaje de expectativas (el mismo de conformance/state-broker.mjs) ───────────────────

    private static function path(mixed $o, string $path): mixed
    {
        foreach (explode('.', $path) as $k) {
            if (is_array($o) && array_key_exists($k, $o)) {
                $o = $o[$k];
            } elseif (is_object($o) && isset($o->{$k})) {
                $o = $o->{$k};
            } else {
                return null;
            }
        }

        return $o;
    }

    private static function cumple(mixed $v, mixed $e): bool
    {
        if (is_array($e) && !array_is_list($e)) {
            if (array_key_exists('match', $e)) {
                return is_string($v) && preg_match('/' . str_replace('/', '\/', $e['match']) . '/u', $v) === 1;
            }
            if (array_key_exists('includes', $e)) {
                return is_array($v) ? in_array($e['includes'], $v, true) : str_contains((string) $v, (string) $e['includes']);
            }
            if (array_key_exists('gte', $e)) {
                return $v >= $e['gte'];
            }
            if (array_key_exists('lte', $e)) {
                return $v <= $e['lte'];
            }
            if (array_key_exists('not', $e)) {
                return !self::cumple($v, $e['not']);
            }
            if (array_key_exists('present', $e)) {
                return ($v !== null) === $e['present'];
            }
            if (array_key_exists('oneOf', $e)) {
                foreach ($e['oneOf'] as $x) {
                    if (self::cumple($v, $x)) {
                        return true;
                    }
                }

                return false;
            }
        }

        return self::j($v) === self::j($e);
    }

    private static function j(mixed $v): string
    {
        return (string) json_encode($v, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }

    private static function resolve(mixed $v): mixed
    {
        if (is_array($v)) {
            return array_map(self::resolve(...), $v);
        }
        if (is_string($v)) {
            return preg_replace_callback('/\{repeat:([^:}]+):(\d+)\}/', static fn (array $m): string => str_repeat($m[1], (int) $m[2]), $v);
        }

        return $v;
    }
}
