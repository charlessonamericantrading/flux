<?php
declare(strict_types=1);

/**
 * Un cliente MÍNIMO del gateway de flux, para un PHP SIN conexión NATS (un hosting compartido, un cron): solo `curl` y `json`.
 * HTTP queda en el borde: este script habla con el gateway y el gateway con el ecosistema. Contrato: specification/12-edge.md.
 *
 * Configuración, del ENTORNO (variables de PHP-FPM, de Apache `SetEnv`, de un `.env` fuera del directorio web) y nunca en el código:
 *   FLUX_GATEWAY_URL     https://gateway.example.com        (en producción, siempre https: el token viaja en una cabecera)
 *   FLUX_GATEWAY_TOKEN   el token de ESTE cliente (`flux-gateway token`); el gateway solo guarda su SHA-256
 */

/** Un error del gateway, con lo que hace falta para decidir si reintentar: `clase` es `retryable` o `permanent`. */
final class FluxError extends RuntimeException
{
    public function __construct(
        public readonly int $estado,
        public readonly string $codigo,
        public readonly string $clase,
        string $mensaje,
        public readonly ?int $reintentarMs = null,
    ) {
        parent::__construct("$codigo: $mensaje");
    }
}

/**
 * Una petición al gateway. Devuelve [estado HTTP, cuerpo decodificado o null]. Un 4xx/5xx es un FluxError con el código ESTABLE del protocolo
 * (`PERMISSION_DENIED`, `RPC_UNAVAILABLE`, `RPC_TIMEOUT`…), no un mensaje que haya que interpretar.
 */
function flux_llamar(string $metodo, string $ruta, ?array $cuerpo = null, array $cabeceras = [], int $plazoSeg = 30): array
{
    $base = getenv('FLUX_GATEWAY_URL');
    $token = getenv('FLUX_GATEWAY_TOKEN');
    if (!$base || !$token) {
        throw new RuntimeException('Define FLUX_GATEWAY_URL y FLUX_GATEWAY_TOKEN en el entorno del proceso (no en el código ni en el repositorio).');
    }
    $h = ['Authorization: Bearer ' . $token, 'Accept: application/json'];
    foreach ($cabeceras as $nombre => $valor) {
        $h[] = "$nombre: $valor";
    }
    $ch = curl_init(rtrim($base, '/') . $ruta);
    curl_setopt_array($ch, [
        CURLOPT_CUSTOMREQUEST => $metodo,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => false,        // el gateway no redirige; una redirección sería otra cosa
        CURLOPT_CONNECTTIMEOUT => 5,
        CURLOPT_TIMEOUT => $plazoSeg,
    ]);
    if ($cuerpo !== null) {
        $h[] = 'Content-Type: application/json';
        // UTF-8 literal, sin escapes \u: `café`, no `café` (AGENTS.md, regla 11).
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($cuerpo, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR));
    }
    curl_setopt($ch, CURLOPT_HTTPHEADER, $h);
    $texto = curl_exec($ch);
    if ($texto === false) {
        $motivo = curl_error($ch);
        curl_close($ch);
        throw new FluxError(0, 'GATEWAY_INALCANZABLE', 'retryable', $motivo);
    }
    $estado = (int) curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
    curl_close($ch);

    $json = $texto === '' ? null : json_decode($texto, true);
    if ($estado >= 400) {
        $e = is_array($json) ? ($json['error'] ?? []) : [];
        throw new FluxError($estado, (string) ($e['code'] ?? "HTTP_$estado"), (string) ($e['class'] ?? ($estado >= 500 ? 'retryable' : 'permanent')), (string) ($e['message'] ?? 'sin detalle'), isset($e['retryAfterMs']) ? (int) $e['retryAfterMs'] : null);
    }
    return [$estado, $json];
}

// fragmento:php-leer-estado
/** El valor ACTUAL de un dato que guarda otro sistema (`GET /clientes/123` antes). `null` si no existe o está borrado. */
function flux_leer_estado(string $contrato, string $clave): ?array
{
    try {
        [, $entrada] = flux_llamar('GET', '/v1/state/' . rawurlencode($contrato) . '/' . rawurlencode($clave));
    } catch (FluxError $e) {
        if ($e->estado === 404) {
            return null;
        }
        throw $e;
    }
    return $entrada;
}
// /fragmento:php-leer-estado

// fragmento:php-llamar-operacion
/**
 * Una operación (un cálculo o una orden con respuesta inmediata). `RPC_TIMEOUT` es AMBIGUO: la operación PUDO ejecutarse; si es una
 * orden, reintenta con una clave de negocio en `$datos` que quien responde deduplique.
 */
function flux_llamar_operacion(string $operacion, array $datos, int $timeoutMs = 5000): array
{
    [, $respuesta] = flux_llamar('POST', '/v1/rpc/' . rawurlencode($operacion), $datos, ['Flux-Timeout-Ms' => (string) $timeoutMs], intdiv($timeoutMs, 1000) + 5);
    return $respuesta;
}
// /fragmento:php-llamar-operacion
