<?php
declare(strict_types=1);

/**
 * La ficha de un cliente para el carrito de una web en PHP, SIN `GET /api/clientes/{id}` ni `GET /api/descuento`: lee el estado y
 * llama a la operación a través del gateway.
 *
 *   php ficha.php cli-1 9990                      (en línea de comandos)
 *   curl 'http://…/ficha.php?cliente=cli-1&total=9990'   (como un script web)
 *
 * Responde JSON: { cliente: { nombre, plan, puntos }, descuentoCents, totalCents }. El correo del cliente NO sale de aquí: no hace falta.
 */

require __DIR__ . '/flux.php';

$cli = PHP_SAPI === 'cli';
$clienteId = $cli ? ($argv[1] ?? '') : (string) ($_GET['cliente'] ?? '');
$total = $cli ? (int) ($argv[2] ?? 0) : (int) ($_GET['total'] ?? 0);

function responder(int $estado, array $cuerpo): void
{
    if (PHP_SAPI !== 'cli') {
        http_response_code($estado);
        header('Content-Type: application/json; charset=utf-8');
    }
    echo json_encode($cuerpo, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES), "\n";
    exit($estado >= 400 ? 1 : 0);
}

if (!preg_match('/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/D', $clienteId)) {   // /D: sin ella, `$` acepta un `\n` final
    responder(400, ['error' => 'cliente inválido']);
}

try {
    // fragmento:php-ficha
    $entrada = flux_leer_estado('clientes.cliente.v1', $clienteId);
    if ($entrada === null) {
        responder(404, ['error' => 'cliente desconocido']);
    }
    $descuento = flux_llamar_operacion('clientes.cliente.v1.calcular-descuento', ['clienteId' => $clienteId, 'totalCents' => $total, 'moneda' => 'EUR']);
    // /fragmento:php-ficha
} catch (FluxError $e) {
    // `retryable`: el ecosistema no puede ahora (RPC_UNAVAILABLE, RPC_TIMEOUT); `permanent`: repetirlo igual da lo mismo.
    responder($e->clase === 'retryable' ? 503 : 502, ['error' => $e->codigo, 'reintentable' => $e->clase === 'retryable']);
}

responder(200, [
    'cliente' => ['nombre' => $entrada['data']['nombre'], 'plan' => $entrada['data']['plan'], 'puntos' => $entrada['data']['puntos']],
    'descuentoCents' => $descuento['descuentoCents'],
    'totalCents' => $total - $descuento['descuentoCents'],
]);
