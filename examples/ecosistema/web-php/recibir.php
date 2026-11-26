<?php
declare(strict_types=1);

/**
 * El RECEPTOR de las entregas del gateway (flux → HTTP): el evento llega como un `POST` firmado y esto lo verifica y lo guarda. Sustituye a un
 * webhook de tu propia API, con tres diferencias que importan: la firma se verifica sobre los BYTES en bruto, el evento se DEDUPLICA por
 * `Idempotency-Key` (el gateway entrega al menos una vez), y un `4xx` lleva el evento a la DLQ sin reintentos.
 *
 *   FLUX_ENTREGA_SECRETO        el secreto con el que el gateway firma las entregas a este receptor
 *   FLUX_ENTREGA_SECRETO_NUEVO  (opcional) el nuevo, durante una rotación sin corte
 *   FLUX_WEB_DATA_DIR           dónde guarda lo recibido (por defecto, ./datos)
 */

require __DIR__ . '/firma.php';

// fragmento:php-recibir
$cuerpo = file_get_contents('php://input');                                  // los bytes EN BRUTO: nunca json_decode + json_encode antes de verificar
$resultado = verificar_flux_signature(
    $_SERVER['HTTP_FLUX_SIGNATURE'] ?? '',
    $cuerpo,
    [getenv('FLUX_ENTREGA_SECRETO'), getenv('FLUX_ENTREGA_SECRETO_NUEVO')],  // dos secretos vigentes = rotación sin corte
);
if ($resultado !== 'ok') {
    http_response_code(401);                                                 // un 4xx es PERMANENT: a la DLQ, que es lo correcto con una firma que no verifica
    exit;
}

$clave = $_SERVER['HTTP_IDEMPOTENCY_KEY'] ?? '';
if (!preg_match('/^[0-9a-f-]{36}$/D', $clave)) {
    http_response_code(400);
    exit;
}
// Deduplicar: la misma `Idempotency-Key` otra vez NO repite el efecto, y se responde 2xx igualmente (el gateway ya entregó).
$dir = getenv('FLUX_WEB_DATA_DIR') ?: __DIR__ . '/datos';
@mkdir($dir, 0775, true);
$f = fopen($dir . '/recibidas.jsonl', 'c+');
flock($f, LOCK_EX);                                                          // en tu sistema: INSERT con clave única en una tabla
$vistas = [];
while (($linea = fgets($f)) !== false) {
    $vistas[trim($linea)] = true;
}
if (!isset($vistas[$clave])) {
    fwrite($f, $clave . "\n");
    // …aquí, el efecto: avisar al cliente de que su factura ya está lista (el `data` del evento es `json_decode($cuerpo, true)['data']`).
}
flock($f, LOCK_UN);
fclose($f);
http_response_code(204);                                                     // 2xx solo cuando está GUARDADO
// /fragmento:php-recibir
