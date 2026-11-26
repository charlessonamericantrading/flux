<?php
declare(strict_types=1);

/**
 * EL «ANTES» de web-php/ficha.php: la misma ficha de cliente, con dos APIs HTTP del CRM (`GET /clientes/{id}` y `GET /descuento`). Funciona (el test
 * lo ejecuta contra un servidor de verdad) y es lo que sustituye el gateway. Qué cambia en el «después»: el estado se lee de una réplica que el
 * CRM publica, el cálculo es una operación, y el PHP deja de conocer la dirección interna del CRM —solo conoce el gateway— y de guardar una clave
 * de API de otro sistema.
 *
 *   CRM_URL=http://crm.interno:3000 php ficha-antes.php cli-1 9990
 */

// fragmento:antes-fila-10
// PHP en un hosting compartido: dos llamadas HTTP al CRM, con la dirección interna de otro sistema escrita en este.
$crm = getenv('CRM_URL');
$cliente = json_decode(file_get_contents("$crm/clientes/" . rawurlencode($argv[1])), true);
$descuento = json_decode(file_get_contents("$crm/descuento?clienteId=" . rawurlencode($argv[1]) . "&totalCents=" . (int) $argv[2]), true);
echo json_encode(['nombre' => $cliente['nombre'], 'descuentoCents' => $descuento['descuentoCents']]), "\n";
// /fragmento:antes-fila-10
