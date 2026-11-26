<?php

declare(strict_types=1);

namespace Flux\Tests;

use Flux\Envelope;
use Flux\ErrorClass;
use Flux\ExternalId;
use Flux\FluxEvent;
use Flux\Json;
use Flux\Rpc\RpcEnvelope;
use Flux\Rpc\RpcRequestRef;
use Flux\Signing\Signer;
use Flux\Signing\SigningOptions;
use Flux\Signing\VerificationMode;
use Flux\Signing\Verifier;
use Flux\State\StateEnvelope;

/**
 * La lógica del arnés de conformidad cruzada del SDK de PHP (conformance/harness/README.md): una operación de entrada, un resultado
 * de salida. Vive aquí y no en el script para que la suite de PHPUnit ejecute **la misma** lógica que el runner cruzado, en
 * proceso y sin lanzar un `php` por vector.
 *
 * Deliberadamente delgado: parsea la entrada, llama al SDK, devuelve el resultado. Toda lógica aquí es lógica que no está en el SDK
 * y que el runner, por tanto, no verifica. En particular NO rellena nada: `id`, `time` y `dlqtime` vienen del vector, o los bytes
 * no serían comparables entre SDKs.
 */
final class Harness
{

    /**
     * `data` de un nodo del árbol decodificado EN MODO OBJETO. PHP conflata `{}` y `[]` en el mismo `array` vacío; el árbol de objetos
     * no (`{}` es `stdClass`, `[]` es `array`). La API del SDK trata `[]` como el objeto vacío —es lo idiomático—, así que aquí hay un
     * único ajuste: un `[]` JSON se entrega como lista no vacía para que el SDK lo rechace como lo que es (una lista, no un objeto).
     */
    public static function dataDe(?object $nodo): mixed
    {
        if ($nodo === null || !property_exists($nodo, 'data')) {
            return null;
        }
        $data = Json::fromObjectTree($nodo->data);
        if (is_array($data) && $data === []) {
            return [null]; // un `[]` JSON: no es un objeto
        }

        return $data instanceof \stdClass ? get_object_vars($data) : $data;
    }

    /**
     * El arnés NO rellena nada: todos los atributos vienen del vector, o no serían comparables.
     *
     * @param array<string,mixed> $e
     */
    public static function construir(array $e, ?object $nodo): FluxEvent
    {
        return Envelope::build(
            subject: $e['subject'],
            data: self::dataDe($nodo),
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
    public static function firmante(array $signing): Signer
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
    public static function cuerpo(string $base64): string
    {
        $bytes = base64_decode($base64, true);
        if ($bytes === false) {
            throw new RuntimeException('`bytes` no es base64 estándar');
        }

        return $bytes;
    }

    /**
     * Los argumentos de `StateEnvelope::buildEvent` de la entrada PLANA de estado. El arnés NO rellena nada: `id` y `time` vienen del
     * vector. `dataschema` NO viene: lo deriva el SDK del contrato, que es parte de lo que se compara.
     *
     * @param array<string,mixed> $e
     *
     * @return array<string,mixed>
     */
    public static function entradaDeEstado(array $e, ?object $nodo): array
    {
        return [
            'contract' => $e['contract'],
            'tenantId' => $e['tenantId'],
            'key' => $e['key'],
            'id' => $e['id'],
            'source' => $e['source'],
            'time' => $e['time'],
            'correlationid' => $e['correlationid'],
            'producerversion' => $e['producerversion'],
            'classification' => $e['classification'] ?? null,
            'data' => self::dataDe($nodo),
            'causationid' => $e['causationid'] ?? null,
            'traceparent' => $e['traceparent'] ?? null,
            'tracestate' => $e['tracestate'] ?? null,
        ];
    }

    /**
     * Lo que la respuesta HEREDA de la petición. El `correlationid` de primer nivel que traen los vectores es redundante y aquí no se usa.
     *
     * @param array<string,mixed> $r
     */
    public static function referencia(array $r): RpcRequestRef
    {
        return new RpcRequestRef(
            $r['operation'],
            $r['id'],
            $r['correlationid'],
            $r['tenantid'],
            $r['subject'] ?? null,
            $r['partitionkey'] ?? null,
        );
    }

    /**
     * @param array<string,mixed> $in
     *
     * @return array<string,mixed>
     */
    public static function ejecutar(array $in, object $arbol): array
    {
        switch ($in['op'] ?? null) {
            case 'build':
                return ['ok' => true, 'bytes' => base64_encode(Envelope::serialize(self::construir($in['event'], $arbol->event ?? null)))];

            // El `id` determinista de un hecho importado (01-envelope.md §2.6.1; conformance/cases/cross-sdk-external-id.json).
            // `externalKey: { repeat, times }` se expande para no pegar 513 caracteres en el caso.
            case 'external-id':
                $e = $in['input'] ?? $in;
                $clave = $e['externalKey'] ?? null;
                if (is_array($clave)) {
                    $clave = str_repeat((string) $clave['repeat'], (int) $clave['times']);
                }
                if (!is_string($clave) || !is_string($e['source'] ?? null) || !is_string($e['time'] ?? null)) {
                    return ['ok' => false]; // sin `time` (o un tipo equivocado) no hay `id` determinista
                }
                try {
                    return ['ok' => true, 'id' => ExternalId::derive($e['source'], $clave, $e['time'])];
                } catch (\Flux\EnvelopeException) {
                    return ['ok' => false];
                }

            case 'dlq':
                $evento = self::construir($in['event'], $arbol->event ?? null);
                if (($in['signFirst'] ?? false) === true) {
                    // Firmar ANTES de añadir las `dlq*` es lo que fija la posición de
                    // signkeyid/signature respecto a ellas — 07-signing.md §4.1, §5.1.
                    $evento = self::firmante($in['signing'])->sign($evento);
                }

                $d = $in['dlq'];

                // `dlqtime` lo fija el vector: si lo pusiera el SDK, los bytes no serían comparables entre ejecuciones, y mucho menos
                // entre lenguajes. El SDK lo normaliza a la forma canónica (o lo rechaza).
                $conDlq = Envelope::toDlqEvent($evento, ErrorClass::from($d['reason']), $d['attempts'], $d['consumer'], $d['error'], $d['dlqtime']);

                return ['ok' => true, 'bytes' => base64_encode(Envelope::serialize($conDlq))];

            case 'sign':
                $firmado = self::firmante($in['signing'])->sign(self::construir($in['event'], $arbol->event ?? null));

                return ['ok' => true, 'bytes' => base64_encode(Envelope::serialize($firmado))];

            case 'verify':
                $evento = Envelope::parse(self::cuerpo($in['bytes']));
                $verifier = Verifier::fromOptions(new SigningOptions(
                    publicKeys: $in['publicKeys'] ?? [],
                    verify: VerificationMode::from($in['mode'] ?? 'require'),
                ));
                // `null` en modo `off`: no hay nada que comprobar. En `warn`, `check` acepta y
                // devuelve el código en vez de lanzar: sigue siendo ok, igual que en Node.
                $verifier?->check($evento);

                return ['ok' => true];

            case 'parse':
                Envelope::parse(self::cuerpo($in['bytes']));

                return ['ok' => true];

            // ─── Estado replicado (capacidad `state`) ───────────────────────────────────────────
            case 'state-build':
                return ['ok' => true, 'bytes' => base64_encode(Envelope::serialize(StateEnvelope::buildEvent(...self::entradaDeEstado($in, $arbol))))];

            case 'state-tombstone':
                return [
                    'ok' => true,
                    'bytes' => base64_encode(Envelope::serialize(StateEnvelope::buildTombstone(
                        contract: $in['contract'],
                        tenantId: $in['tenantId'],
                        key: $in['key'],
                        id: $in['id'],
                        source: $in['source'],
                        time: $in['time'],
                        correlationid: $in['correlationid'],
                        producerversion: $in['producerversion'],
                        aggregateVersion: $in['aggregateVersion'] ?? null,
                        causationid: $in['causationid'] ?? null,
                        traceparent: $in['traceparent'] ?? null,
                        tracestate: $in['tracestate'] ?? null,
                    ))),
                ];

            case 'state-subject':
                return ['ok' => true, 'subject' => StateEnvelope::subject($in['contract'], $in['tenantId'], $in['key'])];

            case 'state-dlq':
                $d = $in['dlq'];
                $ev = StateEnvelope::buildDlqEvent(
                    self::entradaDeEstado($in['original'], $arbol->original ?? null),
                    $in['revision'],
                    ErrorClass::from($d['reason']),
                    $d['attempts'],
                    $d['consumer'],
                    $d['error'],
                    $d['dlqtime'],
                );

                return ['ok' => true, 'bytes' => base64_encode(Envelope::encode($ev))];

            // ─── Petición/respuesta (capacidad `rpc`) ───────────────────────────────────────────
            case 'rpc-request':
                return [
                    'ok' => true,
                    'bytes' => base64_encode(Envelope::encode(RpcEnvelope::buildRequest(
                        operation: $in['operation'],
                        tenantId: $in['tenantId'],
                        id: $in['id'],
                        source: $in['source'],
                        time: $in['time'],
                        correlationid: $in['correlationid'],
                        producerversion: $in['producerversion'],
                        classification: $in['classification'],
                        data: self::dataDe($arbol),
                        aggregateId: $in['aggregateId'] ?? null,
                        causationid: $in['causationid'] ?? null,
                        traceparent: $in['traceparent'] ?? null,
                        tracestate: $in['tracestate'] ?? null,
                    ))),
                ];

            case 'rpc-reply':
                return [
                    'ok' => true,
                    'bytes' => base64_encode(Envelope::encode(RpcEnvelope::buildReply(
                        request: self::referencia($in['request']),
                        id: $in['id'],
                        source: $in['source'],
                        time: $in['time'],
                        producerversion: $in['producerversion'],
                        classification: $in['classification'],
                        data: self::dataDe($arbol),
                        traceparent: $in['traceparent'] ?? null,
                    ))),
                ];

            case 'rpc-error':
                return [
                    'ok' => true,
                    'bytes' => base64_encode(Envelope::encode(RpcEnvelope::buildError(
                        request: self::referencia($in['request']),
                        id: $in['id'],
                        source: $in['source'],
                        time: $in['time'],
                        producerversion: $in['producerversion'],
                        error: $in['error'],
                        traceparent: $in['traceparent'] ?? null,
                    ))),
                ];

            // El subject de NATS (`{ ok, subject }`) o el código estable del rechazo (INVALID_RPC_CALL o RESERVED_DOMAIN).
            case 'rpc-operation':
                return ['ok' => true, 'subject' => RpcEnvelope::subject($in['operation'] ?? null)];

            // Los milisegundos efectivos, o INVALID_RPC_CALL. `timeoutMs` ausente es 5000; `null` NO es «ausente»: es inválido.
            case 'rpc-timeout':
                return ['ok' => true, 'effectiveMs' => RpcEnvelope::resolveTimeout($in['timeoutMs'] ?? null, array_key_exists('timeoutMs', $in))];

            default:
                return ['ok' => false, 'code' => 'UNSUPPORTED_OP', 'detail' => $in['op'] ?? null];
        }
    }

    /**
     * Una operación (JSON) → su resultado. Un fallo de la operación se REPORTA, no se propaga: exit != 0 significaría que el arnés
     * está roto, no que el caso falló. El código estable del SDK es lo que se compara entre lenguajes; el texto no.
     *
     * @return array<string,mixed>
     */
    public static function run(string $texto): array
    {
        try {
            // Dos lecturas del mismo texto: en arrays (para los atributos) y en objetos (para `data`, que distingue `{}` de `[]`).
            $in = json_decode($texto, true, 512, JSON_THROW_ON_ERROR);
            $arbol = json_decode($texto, false, 512, JSON_THROW_ON_ERROR);

            return self::ejecutar($in, $arbol);
        } catch (\Throwable $e) {
            return [
                'ok' => false,
                'code' => property_exists($e, 'errorCode') && is_string($e->errorCode) ? $e->errorCode : (new \ReflectionClass($e))->getShortName(),
                'detail' => $e->getMessage(),
            ];
        }
    }
}
