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

use Flux\Envelope;
use Flux\ErrorClass;
use Flux\FluxError;
use Flux\FluxEvent;
use Flux\Signing\Signer;
use Flux\Signing\SigningOptions;
use Flux\Signing\VerificationMode;
use Flux\Signing\Verifier;

/**
 * El arnés NO rellena nada: todos los atributos vienen del vector, o no serían comparables.
 *
 * @param array<string,mixed> $e
 */
function construir(array $e): FluxEvent
{
    return Envelope::build(
        subject: $e['subject'],
        data: $e['data'],
        id: $e['id'],
        source: $e['source'],
        producerversion: $e['producerversion'],
        tenantid: $e['tenantid'],
        dataclassification: $e['dataclassification'],
        dataschema: $e['dataschema'],
        correlationid: $e['correlationid'],
        time: $e['time'],
        aggregateId: $e['aggregateId'] ?? null,
        causationid: $e['causationid'] ?? null,
        partitionkey: $e['partitionkey'] ?? null,
        traceparent: $e['traceparent'] ?? null,
        tracestate: $e['tracestate'] ?? null,
    );
}

/** @param array<string,mixed> $signing */
function firmante(array $signing): Signer
{
    $signer = Signer::fromOptions(new SigningOptions(
        privateKey: $signing['privateKeyPem'],
        keyId: $signing['keyId'],
    ));

    if ($signer === null) {
        throw new RuntimeException('`signing.privateKeyPem` ausente');
    }

    return $signer;
}

/** El cuerpo llega en base64 ESTÁNDAR para que ningún paso intermedio reescriba el UTF-8. */
function cuerpo(string $base64): string
{
    $bytes = base64_decode($base64, true);
    if ($bytes === false) {
        throw new RuntimeException('`bytes` no es base64 estándar');
    }

    return $bytes;
}

/**
 * @param array<string,mixed> $in
 *
 * @return array<string,mixed>
 */
function ejecutar(array $in): array
{
    switch ($in['op'] ?? null) {
        case 'build':
            return ['ok' => true, 'bytes' => base64_encode(Envelope::serialize(construir($in['event'])))];

        case 'dlq':
            $evento = construir($in['event']);
            if (($in['signFirst'] ?? false) === true) {
                // Firmar ANTES de añadir las `dlq*` es lo que fija la posición de
                // signkeyid/signature respecto a ellas — 07-signing.md §4.1, §5.1.
                $evento = firmante($in['signing'])->sign($evento);
            }

            $d = $in['dlq'];
            $conDlq = Envelope::toDlqEvent(
                $evento,
                ErrorClass::from($d['reason']),
                $d['attempts'],
                $d['consumer'],
                $d['error'],
            );

            // `dlqtime` lo fija el vector: `Envelope::toDlqEvent` lo pone con el reloj y
            // entonces los bytes no serían comparables entre ejecuciones, y mucho menos
            // entre lenguajes. Se reescribe SOLO ese atributo; el `dlqerror` se toma ya
            // truncado por el SDK, que es quien decide dónde corta.
            $conDlq = $conDlq->withDlq(
                $d['reason'],
                $d['attempts'],
                $d['consumer'],
                (string) $conDlq->dlqerror,
                $d['dlqtime'],
            );

            return ['ok' => true, 'bytes' => base64_encode(Envelope::serialize($conDlq))];

        case 'sign':
            $firmado = firmante($in['signing'])->sign(construir($in['event']));

            return ['ok' => true, 'bytes' => base64_encode(Envelope::serialize($firmado))];

        case 'verify':
            $evento = Envelope::parse(cuerpo($in['bytes']));
            $verifier = Verifier::fromOptions(new SigningOptions(
                publicKeys: $in['publicKeys'] ?? [],
                verify: VerificationMode::from($in['mode'] ?? 'require'),
            ));
            // `null` en modo `off`: no hay nada que comprobar. En `warn`, `check` acepta y
            // devuelve el código en vez de lanzar: sigue siendo ok, igual que en Node.
            $verifier?->check($evento);

            return ['ok' => true];

        case 'parse':
            Envelope::parse(cuerpo($in['bytes']));

            return ['ok' => true];

        default:
            return ['ok' => false, 'code' => 'UNSUPPORTED_OP', 'detail' => $in['op'] ?? null];
    }
}

$entrada = stream_get_contents(STDIN);

try {
    $salida = ejecutar(json_decode($entrada === false ? '' : $entrada, true, 512, JSON_THROW_ON_ERROR));
} catch (FluxError $e) {
    // El código estable del SDK es lo que se compara entre lenguajes; el texto no.
    $salida = ['ok' => false, 'code' => $e->errorCode ?? 'ERROR', 'detail' => $e->getMessage()];
} catch (Throwable $e) {
    // Un fallo de la operación se REPORTA, no se propaga: exit != 0 significaría que el
    // arnés está roto, no que el caso falló.
    $salida = [
        'ok' => false,
        'code' => (new ReflectionClass($e))->getShortName(),
        'detail' => $e->getMessage(),
    ];
}

echo json_encode($salida, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
exit(0);
