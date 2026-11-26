<?php
/**
 * Un RECEPTOR de las entregas del gateway, en PHP (un hosting compartido, sin conexión NATS): verifica `Flux-Signature`.
 * Contrato NORMATIVO: specification/12-edge.md §4.2. Lo ejecutan las pruebas del gateway contra los vectores de protocol.json.
 *
 *   $resultado = verificar_flux_signature(
 *       $_SERVER['HTTP_FLUX_SIGNATURE'] ?? '',      // t=<unix>,v1=<hmac>[,v1=<hmac>…]
 *       file_get_contents('php://input'),           // los BYTES en bruto: NUNCA json_decode + json_encode antes de verificar
 *       [getenv('FLUX_SECRETO'), getenv('FLUX_SECRETO_NUEVO')]  // los secretos vigentes (rotación sin corte)
 *   );
 *
 * Devuelve "ok", "ausente", "formato", "firma" o "tolerancia". Con cualquier otro valor: responde 401 y NO ejecutes nada (un 4xx es
 * PERMANENT: el evento va a la DLQ, que es lo que debe pasar con una firma que no verifica). Con "ok": guarda el evento DEDUPLICANDO por
 * `Idempotency-Key` (la misma clave, otra vez: responde 2xx sin repetir el efecto) y responde 2xx en cuanto esté GUARDADO.
 */
function verificar_flux_signature(string $cabecera, string $cuerpo, array $secretos, ?int $ahora = null, int $tolerancia = 300): string
{
    if ($cabecera === '') {
        return 'ausente';
    }
    $ahora ??= time();
    $t = null;
    $v1 = [];
    $veces = 0;
    foreach (explode(',', $cabecera) as $campo) {
        $i = strpos($campo, '=');
        if ($i === false || $i < 1) {
            return 'formato';
        }
        $k = trim(substr($campo, 0, $i));
        $v = trim(substr($campo, $i + 1));
        if ($k === 't') {
            $veces++;
            $t = $v;
        } elseif ($k === 'v1' && preg_match('/^[0-9a-f]{64}$/D', $v)) { // /D: sin ella, `$` acepta un `\n` final
            $v1[] = $v;
        } // cualquier otro campo (`v2=…`) se IGNORA: el esquema puede crecer
    }
    if ($veces !== 1 || !preg_match('/^[0-9]{1,15}$/D', (string) $t) || $v1 === []) {
        return 'formato';
    }

    // Se comparan TODAS las parejas (secreto × v1), en tiempo constante (`hash_equals`), sin salir en la primera.
    $acierta = false;
    foreach ($secretos as $secreto) {
        if (!is_string($secreto) || $secreto === '') {
            continue;
        }
        $esperado = hash_hmac('sha256', $t . '.' . $cuerpo, $secreto);
        foreach ($v1 as $recibido) {
            if (hash_equals($esperado, $recibido)) {
                $acierta = true;
            }
        }
    }
    if (!$acierta) {
        return 'firma';
    }
    // La tolerancia es SIMÉTRICA (un `t` del futuro también se rechaza) y se mira DESPUÉS de la firma.
    return abs($ahora - (int) $t) > $tolerancia ? 'tolerancia' : 'ok';
}
