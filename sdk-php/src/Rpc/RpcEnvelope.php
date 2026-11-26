<?php

declare(strict_types=1);

namespace Flux\Rpc;

use Flux\Envelope;
use Flux\FluxEvent;
use Flux\InvalidDataClassificationException;
use Flux\InvalidEnvelopeInputException;
use Flux\InvalidRpcCallException;
use Flux\Protocol;
use Flux\Text;

/**
 * Las reglas y el sobre de la petición/respuesta: funciones **puras** (11-request-reply.md §2.2, §3, §4.1 paso 0).
 *
 * Todo lo de aquí se hace SIN red y SIN reloj: `id` y `time` los da quien llama. Es lo que ejecuta el arnés de conformidad cruzada
 * (`rpc-request`, `rpc-reply`, `rpc-error`, `rpc-operation`, `rpc-timeout`) y lo que comparan los siete SDKs byte a byte.
 */
final class RpcEnvelope
{
    /** `rpc.timeout`: 5 s por defecto. */
    public const TIMEOUT_DEFAULT_MS = 5_000;
    public const TIMEOUT_MIN_MS = 1;
    /**
     * `rpc.timeout.maxMs`: 25 s, **por debajo del `ack_wait` de 30 s** (03-delivery.md §2): para que una llamada hecha desde un
     * handler de eventos no provoque su reentrega mientras espera.
     */
    public const TIMEOUT_MAX_MS = 25_000;
    /** `rpc.signing.staleRequestWindowSeconds`: la ventana simétrica de `STALE_REQUEST` (solo con la firma en `require`). */
    public const STALE_WINDOW_SECONDS = 300;
    /** Handlers concurrentes por operación: el mismo orden que `max_ack_pending` (11-request-reply.md §5.6). */
    public const MAX_CONCURRENT_HANDLERS = 256;
    public const ERROR_TYPE = 'com.flux.rpc.error.v1';
    public const ERROR_DATASCHEMA = 'https://schemas.internal/system/rpc/error/1.0.0.json';
    public const SCHEMA_BASE = 'https://schemas.internal';
    /** El nombre de la operación —el subject sin `rpc.`— tiene la MISMA forma sintáctica que el subject de un evento. Coincidencia COMPLETA (`\z`). */
    public const OPERATION_PATTERN = '/^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]{0,3}\.[a-z0-9]+(-[a-z0-9]+)*\z/';
    /** `errors.codePattern` y el `pattern` del esquema del error. */
    public const ERROR_CODE_PATTERN = '/^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*\z/';
    public const ERROR_CODE_MAX = 64;
    public const ERROR_MESSAGE_MAX = 1000;
    public const CLASSIFICATIONS = ['public', 'internal', 'confidential', 'restricted'];

    private function __construct()
    {
    }

    // ─── Validación — se hace SIEMPRE, antes de abrir ninguna petición ────────────────────────────

    /**
     * Valida el nombre de una operación. Primero el dominio reservado (`RESERVED_DOMAIN`: también `rpc.` puesto por quien llama),
     * después la forma (`INVALID_RPC_CALL`). Siempre antes de tocar la red.
     */
    public static function parseOperation(mixed $operation): RpcOperation
    {
        if (!is_string($operation)) {
            throw new InvalidRpcCallException(
                'la operación debe ser una cadena `<dominio>.<agregado>.v<major>.<operacion>`, y llegó ' . get_debug_type($operation) . '.'
            );
        }
        Protocol::assertNotReservedDomain($operation, 'operación');
        if (preg_match(self::OPERATION_PATTERN, $operation) !== 1) {
            $n = count(explode('.', $operation));
            $why = match (true) {
                $operation !== strtolower($operation) => 'debe ir todo en minúsculas —NATS distingue mayúsculas, y una errata crea un subject al que nadie está suscrito, sin error—',
                preg_match('/[*>]/', $operation) === 1 => 'no admite comodines (`*`, `>`): el broker los aceptaría, y es una operación concreta la que se llama o se atiende',
                $n !== 4 => "debe tener exactamente 4 tokens, tiene {$n}",
                default => 'la versión mayor es `v` y un entero de 1 a 4 dígitos sin ceros a la izquierda (`v1`…`v9999`), y cada token va en kebab-case (letras minúsculas, dígitos y guiones)',
            };
            $shown = Text::codePointLength($operation) > 90 ? Text::truncateCodePoints($operation, 90) : $operation;
            throw new InvalidRpcCallException(
                'la operación ' . json_encode($shown, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . " no es válida: {$why}. El nombre es "
                . '`<dominio>.<agregado>.v<major>.<operacion>` —el subject sin `rpc.`—, con la operación en infinitivo y kebab-case: '
                . '`clientes.cliente.v1.calcular-descuento` (11-request-reply.md §2.2).'
            );
        }
        [$domain, $aggregate, $v, $name] = explode('.', $operation);

        return new RpcOperation($operation, $domain, $aggregate, (int) substr($v, 1), $name);
    }

    /**
     * El `timeoutMs` efectivo: `null` en la API (`$present = false` en el arnés) → 5000; un entero entre 1 y 25 000; cualquier otra
     * cosa, `INVALID_RPC_CALL` (no se recorta). Un `float` NO se convierte: `1.5` y `2000.0` son inválidos.
     *
     * @param bool $present `false` = la opción no se dio. Un `null` DADO no es «ausente» y es inválido.
     */
    public static function resolveTimeout(mixed $timeoutMs, bool $present = true): int
    {
        if (!$present) {
            return self::TIMEOUT_DEFAULT_MS;
        }
        if (!is_int($timeoutMs) || $timeoutMs < self::TIMEOUT_MIN_MS || $timeoutMs > self::TIMEOUT_MAX_MS) {
            throw new InvalidRpcCallException(
                '`timeoutMs` debe ser un entero entre ' . self::TIMEOUT_MIN_MS . ' y ' . self::TIMEOUT_MAX_MS . ' (milisegundos; '
                . self::TIMEOUT_DEFAULT_MS . ' por defecto) y llegó '
                . (is_int($timeoutMs) ? (string) $timeoutMs : ($timeoutMs === null ? 'null' : 'un valor de tipo ' . get_debug_type($timeoutMs)))
                . '. No se recorta en silencio: un valor fuera de rango es un error. El máximo está por debajo del `ack_wait` de 30 s para '
                . 'que una llamada hecha desde un handler de eventos no provoque su reentrega (11-request-reply.md §4.2).'
            );
        }

        return $timeoutMs;
    }

    /** @param array<string,mixed>|object $data */
    public static function assertObject(mixed $data, string $what): void
    {
        $isObject = $data instanceof \stdClass || (is_array($data) && ($data === [] || !array_is_list($data)));
        if (!$isObject) {
            throw new InvalidRpcCallException(
                "{$what} debe ser un objeto JSON en la raíz (ni array ni escalar), para poder añadir campos sin romper el contrato "
                . '(01-envelope.md §4); llegó ' . (is_array($data) ? 'un array' : get_debug_type($data)) . '.'
            );
        }
    }

    public static function assertClassification(mixed $c): string
    {
        if (!is_string($c) || !in_array($c, self::CLASSIFICATIONS, true)) {
            throw new InvalidDataClassificationException($c, 'rpc');
        }

        return $c;
    }

    // ─── Nombres — 11-request-reply.md §2.2, §3 ───────────────────────────────────────────────────

    /** `rpc.clientes.cliente.v1.calcular-descuento`: el subject de NATS. Cinco tokens. **No** es el atributo `subject` del sobre. */
    public static function subject(string $operation): string
    {
        return 'rpc.' . self::parseOperation($operation)->operation;
    }

    /** `com.flux.rpc.<dominio>.<agregado>.<operacion>.request.v<major>`. */
    public static function requestType(string $operation): string
    {
        $o = self::parseOperation($operation);

        return "com.flux.rpc.{$o->domain}.{$o->aggregate}.{$o->name}.request.v{$o->major}";
    }

    /** `com.flux.rpc.<dominio>.<agregado>.<operacion>.reply.v<major>`. */
    public static function replyType(string $operation): string
    {
        $o = self::parseOperation($operation);

        return "com.flux.rpc.{$o->domain}.{$o->aggregate}.{$o->name}.reply.v{$o->major}";
    }

    /**
     * `https://schemas.internal/rpc/<dominio>/<agregado>/<operacion>/{request,reply}/<major>.0.0.json` (o con otra base): lo que
     * se puede asumir SIN bundle. El bundle conoce el MINOR real.
     *
     * @param 'request'|'reply' $side
     */
    public static function dataschema(string $operation, string $side, string $base = self::SCHEMA_BASE): string
    {
        $o = self::parseOperation($operation);

        return rtrim($base, '/') . "/rpc/{$o->domain}/{$o->aggregate}/{$o->name}/{$side}/{$o->major}.0.0.json";
    }

    // ─── El sobre — funciones PURAS (11-request-reply.md §3) ──────────────────────────────────────

    /**
     * Todo es `INVALID_RPC_CALL`: el conjunto de códigos de rpc es cerrado (04-errors.md §7) y agrupa «las equivocaciones de quien
     * programa». Devuelve `time` canónico.
     *
     * @param list<array{0:string,1:mixed,2?:bool}> $fields
     */
    private static function envelopeInput(array $fields, string $time): string
    {
        $reject = static fn (string $message): \Throwable => new InvalidRpcCallException($message);
        Envelope::requireAttributes($fields, static fn (string $m, string $c): \Throwable => $reject($m));
        try {
            return Envelope::coerceTime($time);
        } catch (InvalidEnvelopeInputException $e) {
            throw new InvalidRpcCallException($e->getMessage(), $e);
        }
    }

    /**
     * El sobre de una petición: un CloudEvent normal con `type` `com.flux.rpc.….request.v<M>`, `subject` y `partitionkey` =
     * `aggregateId` si se da, y `data` el ÚLTIMO atributo.
     *
     * **Pura y determinista**: `id` y `time` los da quien llama. Valida lo mismo que `request` (§4.1, paso 0, salvo L3).
     *
     * @param array<string,mixed>|\stdClass $data Un objeto JSON. En PHP `[]` es el objeto vacío; una lista, no.
     */
    public static function buildRequest(
        string $operation,
        string $tenantId,
        string $id,
        string $source,
        string $time,
        string $correlationid,
        string $producerversion,
        mixed $classification,
        mixed $data,
        ?string $aggregateId = null,
        ?string $causationid = null,
        ?string $dataschema = null,
        ?string $traceparent = null,
        ?string $tracestate = null,
    ): FluxEvent {
        $o = self::parseOperation($operation);
        self::assertObject($data, '`data`');
        $data = $data instanceof \stdClass ? get_object_vars($data) : $data;
        $classification = self::assertClassification($classification);
        $time = self::envelopeInput([
            ['id', $id],
            ['source', $source],
            ['correlationid', $correlationid, true],
            ['tenantid', $tenantId, true],
            ['producerversion', $producerversion, true],
        ], $time);

        return new FluxEvent(
            id: $id,
            source: $source,
            type: self::requestType($o->operation),
            time: $time,
            dataschema: $dataschema ?? self::dataschema($o->operation, 'request'),
            correlationid: $correlationid,
            tenantid: $tenantId,
            producerversion: $producerversion,
            dataclassification: $classification,
            data: $data,
            subject: $aggregateId,
            causationid: $causationid,
            partitionkey: $aggregateId,
            traceparent: $traceparent,
            tracestate: $tracestate,
        );
    }

    /**
     * El sobre de una respuesta: **hereda** `correlationid`, `tenantid`, `subject` y `partitionkey` de la petición, y su
     * `causationid` es el `id` de la petición. `id` NUEVO: nunca el de la petición. `source`, `producerversion` y `traceparent` son
     * del que responde. La respuesta lleva solo `traceparent`, nunca `tracestate` (§3.2).
     *
     * @param array<string,mixed>|\stdClass $data
     */
    public static function buildReply(
        RpcRequestRef $request,
        string $id,
        string $source,
        string $time,
        string $producerversion,
        mixed $classification,
        mixed $data,
        ?string $dataschema = null,
        ?string $traceparent = null,
    ): FluxEvent {
        $o = self::parseOperation($request->operation);
        self::assertObject($data, 'la respuesta');
        $data = $data instanceof \stdClass ? get_object_vars($data) : $data;
        $classification = self::assertClassification($classification);
        $time = self::envelopeInput([
            ['id', $id],
            ['source', $source],
            ['producerversion', $producerversion, true],
            ['request.id', $request->id],
            ['request.correlationid', $request->correlationid, true],
            ['request.tenantid', $request->tenantid, true],
        ], $time);

        return new FluxEvent(
            id: $id,
            source: $source,
            type: self::replyType($o->operation),
            time: $time,
            dataschema: $dataschema ?? self::dataschema($o->operation, 'reply'),
            correlationid: $request->correlationid,
            tenantid: $request->tenantid,
            producerversion: $producerversion,
            dataclassification: $classification,
            data: $data,
            subject: $request->subject,
            causationid: $request->id,
            partitionkey: $request->partition(),
            traceparent: $traceparent,
        );
    }

    /**
     * ¿`$d` cumple el esquema del error (`schemas/system/rpc/error/1.0.0.json`)? Devuelve el primer problema, o `null`. Es la quinta
     * comprobación de quien llama (§4.3) y lo que valida el constructor: se escribe a mano y no con un validador porque L3 es
     * opcional y esta comprobación no lo es.
     */
    public static function errorDataProblem(mixed $d): ?string
    {
        if ($d instanceof \stdClass) {
            $d = get_object_vars($d);
        }
        if (!is_array($d) || ($d !== [] && array_is_list($d))) {
            return '`data` del error no es un objeto';
        }
        $unknown = array_values(array_diff(array_map('strval', array_keys($d)), ['class', 'code', 'message', 'retryAfterMs']));
        if ($unknown !== []) {
            return 'el error lleva atributos que su esquema no admite (' . implode(', ', $unknown) . ')';
        }
        $class = $d['class'] ?? null;
        if ($class !== 'retryable' && $class !== 'permanent') {
            return '`class` del error debe ser `retryable` o `permanent` (`poison` no viaja)';
        }
        $code = $d['code'] ?? null;
        if (!is_string($code) || strlen($code) > self::ERROR_CODE_MAX || preg_match(self::ERROR_CODE_PATTERN, $code) !== 1) {
            return '`code` del error debe ser un identificador estable en MAYÚSCULAS y guiones bajos (máx. 64)';
        }
        // `maxLength` de JSON Schema cuenta caracteres Unicode (puntos de código): mil emojis caben.
        $message = $d['message'] ?? null;
        if (!is_string($message) || $message === '' || Text::codePointLength($message) > self::ERROR_MESSAGE_MAX) {
            return '`message` del error debe ser un texto de 1 a 1000 caracteres';
        }
        if (array_key_exists('retryAfterMs', $d)) {
            $r = $d['retryAfterMs'];
            if (!is_int($r) || $r < 0) {
                return '`retryAfterMs` del error debe ser un entero ≥ 0';
            }
            if ($class !== 'retryable') {
                return '`retryAfterMs` solo tiene sentido con `class: retryable`';
            }
        }

        return null;
    }

    /**
     * El sobre de un error `com.flux.rpc.error.v1`: lo que el que responde devuelve EN LUGAR de la respuesta, tanto el de un handler
     * que lanzó como el que fabrica su SDK. `type`, `dataschema` y `dataclassification: internal` son FIJOS —el error no depende del
     * contrato de la operación—, y `data` lleva las claves en el orden `class, code, message, retryAfterMs`.
     *
     * @param array{class:mixed,code:mixed,message:mixed,retryAfterMs?:mixed} $error
     */
    public static function buildError(
        RpcRequestRef $request,
        string $id,
        string $source,
        string $time,
        string $producerversion,
        array $error,
        ?string $traceparent = null,
    ): FluxEvent {
        self::parseOperation($request->operation);
        $problem = self::errorDataProblem($error);
        // Un error que no cumple su esquema es `INVALID_RPC_CALL` (04-errors.md §7): un código estable, no un `TypeError` sin código.
        if ($problem !== null) {
            throw new InvalidRpcCallException("el error que se iba a contestar no cumple su esquema: {$problem}");
        }
        $time = self::envelopeInput([
            ['id', $id],
            ['source', $source],
            ['producerversion', $producerversion, true],
            ['request.id', $request->id],
            ['request.correlationid', $request->correlationid, true],
            ['request.tenantid', $request->tenantid, true],
        ], $time);

        // El orden de las claves es normativo: class, code, message, retryAfterMs.
        $data = ['class' => $error['class'], 'code' => $error['code'], 'message' => $error['message']];
        if (array_key_exists('retryAfterMs', $error)) {
            $data['retryAfterMs'] = $error['retryAfterMs'];
        }

        return new FluxEvent(
            id: $id,
            source: $source,
            type: self::ERROR_TYPE,
            time: $time,
            dataschema: self::ERROR_DATASCHEMA,
            correlationid: $request->correlationid,
            tenantid: $request->tenantid,
            producerversion: $producerversion,
            dataclassification: 'internal',
            data: $data,
            subject: $request->subject,
            causationid: $request->id,
            partitionkey: $request->partition(),
            traceparent: $traceparent,
        );
    }
}
