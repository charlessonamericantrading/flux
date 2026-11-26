<?php

/**
 * Arnés de conformidad cruzada — SDK de PHP.
 * Contrato: conformance/harness/README.md
 *
 * Lee UNA operación por stdin, escribe UN objeto JSON por stdout y sale con 0 SIEMPRE:
 * un exit distinto de 0 significa que el arnés está roto, no que el caso falló. Los
 * diagnósticos van a stderr.
 *
 * Deliberadamente delgado: toda lógica aquí es lógica que no está en el SDK y que el
 * runner, por tanto, no verifica. En particular NO rellena nada — `id`, `time` y
 * `dlqtime` vienen del vector; si los generase el SDK, los bytes no serían comparables.
 *
 *   php conformance-harness.php < operacion.json
 *
 * `sign` y `verify` necesitan `ext-sodium`, que es la única dependencia de la extensión
 * OPCIONAL de firma. Sin ella el SDK lanza y el arnés lo REPORTA en el JSON (ok:false),
 * que es la respuesta correcta: el arnés sigue funcionando, la operación no.
 */

declare(strict_types=1);

// stdout es el canal del contrato: un warning de PHP impreso ahí convertiría la salida en
// algo que el runner no puede parsear. `display_errors=stderr` lo manda al canal que el
// contrato declara ignorable.
ini_set('display_errors', 'stderr');

require __DIR__ . '/vendor/autoload.php';

// La lógica vive en tests/Harness.php (autoload-dev), la misma que ejecuta PHPUnit.
$entrada = stream_get_contents(STDIN);

echo json_encode(\Flux\Tests\Harness::run($entrada === false ? '' : $entrada), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
exit(0);
