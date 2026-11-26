<?php

declare(strict_types=1);

/**
 * Un escritor del dueño en SU PROPIO PROCESO (`StateSemanticsIntegrationTest`, caso `state_race`). PHP es síncrono: la concurrencia real de un
 * servicio PHP son varios procesos —FPM, workers— escribiendo la misma clave, y es eso lo que se mide, no hilos que PHP no tiene.
 *
 *   php StateRaceWorker.php '<json>'     →  una línea de JSON con los resultados
 */

use Flux\ConnectOptions;
use Flux\FluxBus;

require dirname(__DIR__, 2) . '/vendor/autoload.php';

$a = json_decode($argv[1] ?? '', true, 512, JSON_THROW_ON_ERROR);
$bus = FluxBus::connect(new ConnectOptions(
    service: $a['service'],
    environment: 'test',
    version: '1.0.0',
    schemaBaseUrl: 'https://schemas.internal',
    servers: $a['servers'],
    pendingPollMs: 0,
));
$res = ['ok' => 0, 'stale' => 0, 'conflict' => 0, 'other' => 0, 'errors' => []];
// Una salida en falso: todos los escritores conectan primero y arrancan A LA VEZ, cuando el padre escribe una línea. Sin esto el primero acaba
// antes de que el último haya conectado y no hay carrera.
fwrite(STDOUT, "ready\n");
fflush(STDOUT);
fgets(STDIN);
for ($v = $a['from']; $v <= $a['to']; $v++) {
    if (!($v % $a['instances'] === $a['n'] || random_int(0, 99) < 30)) {
        continue;
    }
    // `pauseMs` del caso: sin él, un respiro al azar de 0 a 4 ms; con 0, ninguno (la carga más dura).
    $pause = $a['pauseMs'] ?? null;
    usleep($pause === null ? random_int(0, 4_000) : (int) ($pause * 1_000));
    try {
        $r = $bus->state->put($a['contract'], $a['key'], ['clienteId' => $a['key'], 'aggregateVersion' => $v], tenantId: $a['tenant']);
        $r->outcome === 'ok' ? $res['ok']++ : $res['stale']++;
    } catch (\Flux\StateConflictException) {
        $res['conflict']++;
    } catch (\Throwable $e) {
        $res['other']++;
        $res['errors'][] = get_class($e) . ': ' . $e->getMessage();
    }
}
$bus->close();
echo 'result ', json_encode($res), "\n";
