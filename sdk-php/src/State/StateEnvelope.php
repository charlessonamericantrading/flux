<?php

declare(strict_types=1);

namespace Flux\State;

use Flux\Envelope;
use Flux\ErrorClass;
use Flux\FluxEvent;
use Flux\InvalidDataClassificationException;
use Flux\InvalidStateContractException;
use Flux\InvalidStateKeyException;
use Flux\InvalidTenantForStateException;
use Flux\MissingAggregateVersionException;
use Flux\Protocol;
use Flux\Text;

/**
 * Las reglas y el sobre del estado replicado: funciones **puras** (10-state.md §2, §3.6, §4, §6).
 *
 * Todo lo de aquí se hace SIN red y SIN reloj: `id` y `time` los da quien llama. Es lo que ejecuta el arnés de conformidad
 * cruzada (`state-build`, `state-tombstone`, `state-subject`, `state-dlq`) y lo que comparan los siete SDKs byte a byte.
 */
final class StateEnvelope
{
    /** `^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`: un único token de NATS. Coincidencia COMPLETA (`\z`, no `$`). */
    public const KEY_PATTERN = '/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}\z/';
    /** `^[a-z0-9]+(-[a-z0-9]+)*$` y, aparte (un patrón puro, sin lookahead), 64 como máximo. */
    public const TENANT_PATTERN = '/^[a-z0-9]+(-[a-z0-9]+)*\z/';
    /** NATS limita los nombres de consumidor a 255 y el durable de un observador lleva el tenant (A-42). */
    public const TENANT_MAX_LENGTH = 64;
    /** `<dominio>.<agregado>.v<major>`: el subject sin `state.`, sin tenant y sin clave. */
    public const CONTRACT_PATTERN = '/^[a-z0-9]+(-[a-z0-9]+)*\.[a-z0-9]+(-[a-z0-9]+)*\.v[1-9][0-9]{0,3}\z/';
    /** 2^53 − 1: el mayor entero que Node, y todo lo que use `double`, representa sin perder precisión. */
    public const AGGREGATE_VERSION_MAX = 9_007_199_254_740_991;
    /** Intentos de un `put`/`delete` ante un conflicto de secuencia (10-state.md §3.2, paso 4). */
    public const MAX_ATTEMPTS = 3;
    /**
     * Los DOS códigos del conflicto de secuencia (A-33, A-49): 10071 «wrong last sequence: N» (un nodo, o un cluster sin
     * carrera) y 10164 «wrong last sequence», SIN número (cluster, bajo carrera). Un SDK que solo reconozca el 10071 pasa sus
     * pruebas contra un nodo y, en producción, trata un conflicto corriente como un error desconocido y no reintenta.
     */
    public const CONFLICT_CODES = [10071, 10164];
    /** El `dataschema` FIJO de toda lápida, de cualquier contrato. */
    public const TOMBSTONE_DATASCHEMA = 'https://schemas.internal/system/state/eliminado/1.0.0.json';
    /** Base de los esquemas del repo: `schemas/state/<dom>/<agr>/<semver>.json` cuelga de aquí. */
    public const SCHEMA_BASE = 'https://schemas.internal';
    /** Clasificaciones admitidas en estado. `restricted` NO: 24 h de retención no caben en un almacén sin caducidad. */
    public const CLASSIFICATIONS = ['public', 'internal', 'confidential'];
    /** Un `put` sin `classification` usa ESTA, no la de la conexión (10-state.md §3.6). */
    public const DEFAULT_CLASSIFICATION = 'confidential';
    /**
     * Lo que sigue al código en el `dlqerror` de un error que **no es de flux** (10-state.md §6): el mensaje de un driver de base
     * de datos puede citar la fila entera, y la DLQ guarda 90 días. La misma que `protocol.json` → `state.dlq.dlqerrorFixedPhrase`.
     */
    public const UNCLASSIFIED_DLQ_ERROR = 'error sin clasificar de la aplicación; el mensaje se omite para no copiar datos a la DLQ (10-state.md §6)';

    private function __construct()
    {
    }

    // ─── Validación — se hace SIEMPRE, antes de abrir ninguna petición ────────────────────────────

    /**
     * Valida un contrato. Primero el dominio reservado (`RESERVED_DOMAIN`), después la forma (`INVALID_STATE_CONTRACT`): el
     * orden de 10-state.md §3.6.
     */
    public static function parseContract(mixed $contract): StateContract
    {
        if (!is_string($contract)) {
            throw new InvalidStateContractException($contract, 'debe ser una cadena');
        }
        Protocol::assertNotReservedDomain($contract, 'contrato');
        if (preg_match(self::CONTRACT_PATTERN, $contract) !== 1) {
            $n = count(explode('.', $contract));
            throw new InvalidStateContractException(
                $contract,
                $n !== 3
                    ? "debe tener exactamente 3 tokens, tiene {$n}"
                    : 'formato esperado <dominio>.<agregado>.v<major> (la versión mayor, de 1 a 4 dígitos: v1…v9999)',
            );
        }
        [$domain, $aggregate, $v] = explode('.', $contract);

        return new StateContract($contract, $domain, $aggregate, (int) substr($v, 1));
    }

    public static function assertKey(mixed $key): string
    {
        if (!is_string($key) || preg_match(self::KEY_PATTERN, $key) !== 1) {
            throw new InvalidStateKeyException($key);
        }

        return $key;
    }

    public static function assertTenant(mixed $tenant): string
    {
        if (!is_string($tenant) || strlen($tenant) > self::TENANT_MAX_LENGTH || preg_match(self::TENANT_PATTERN, $tenant) !== 1) {
            throw new InvalidTenantForStateException($tenant);
        }

        return $tenant;
    }

    /** ¿Es un `aggregateVersion` válido? Entero ≥ 1 y ≤ 2^53 − 1. En PHP `7.0` es un `float` y se rechaza (10-state.md §3.4). */
    public static function isValidAggregateVersion(mixed $v): bool
    {
        return is_int($v) && $v >= 1 && $v <= self::AGGREGATE_VERSION_MAX;
    }

    /**
     * Lee el `aggregateVersion` de un `data` o lanza `MISSING_AGGREGATE_VERSION`. El nombre cubre todo lo que no sea un entero
     * ≥ 1: ausente, null, una cadena, un decimal, 0, un negativo o uno mayor que 2^53 − 1. Se comprueba SIEMPRE, con o sin L3:
     * es una regla del protocolo.
     */
    public static function aggregateVersionOf(mixed $data): int
    {
        $v = null;
        if (is_array($data) && !array_is_list($data)) {
            $v = $data['aggregateVersion'] ?? null;
        } elseif ($data instanceof \stdClass) {
            $v = $data->aggregateVersion ?? null;
        }
        if (!self::isValidAggregateVersion($v)) {
            throw new MissingAggregateVersionException($v);
        }

        return $v;
    }

    /** `null` → `confidential`; `restricted` o una fuera del enum → `INVALID_DATACLASSIFICATION`. */
    public static function assertClassification(mixed $c): string
    {
        if ($c === null) {
            return self::DEFAULT_CLASSIFICATION;
        }
        if (!is_string($c) || !in_array($c, self::CLASSIFICATIONS, true)) {
            throw new InvalidDataClassificationException($c, 'state');
        }

        return $c;
    }

    // ─── Nombres — 02-naming.md, 10-state.md §2 ───────────────────────────────────────────────────

    /** `STATE_CLIENTES`. NATS no admite puntos en nombres de stream. */
    public static function streamName(string $domain): string
    {
        return 'STATE_' . strtoupper(str_replace('-', '_', $domain));
    }

    /**
     * `state.<dominio>.<agregado>.v<major>.<tenant>.<clave>`: el subject de NATS de una entrada. Seis tokens, siempre. **No** es
     * el atributo `subject` de CloudEvents, que aquí vale la clave.
     *
     * Valida en el orden de 10-state.md §3.6 —dominio reservado, forma del contrato, clave, tenant— y lanza con el código
     * estable de cada rechazo.
     */
    public static function subject(string $contract, mixed $tenantId, mixed $key): string
    {
        $c = self::parseContract($contract);
        self::assertKey($key);
        self::assertTenant($tenantId);

        return "state.{$c->name}.{$tenantId}.{$key}";
    }

    /** `com.flux.state.<dominio>.<agregado>.v<major>`. */
    public static function type(string $contract): string
    {
        $c = self::parseContract($contract);

        return "com.flux.state.{$c->domain}.{$c->aggregate}.v{$c->major}";
    }

    /** `com.flux.state.<dominio>.<agregado>.deleted.v<major>`. */
    public static function tombstoneType(string $contract): string
    {
        $c = self::parseContract($contract);

        return "com.flux.state.{$c->domain}.{$c->aggregate}.deleted.v{$c->major}";
    }

    /** `https://schemas.internal/state/<dominio>/<agregado>/<major>.0.0.json` (o con otra base). */
    public static function dataschema(string $contract, string $base = self::SCHEMA_BASE): string
    {
        $c = self::parseContract($contract);

        return rtrim($base, '/') . "/state/{$c->domain}/{$c->aggregate}/{$c->major}.0.0.json";
    }

    /**
     * `<servicio>__state_<dom>_<agr>_v<M>` y, si filtra por tenant, el sufijo `__<tenant>`. Los puntos y los guiones pasan a
     * `_`. El tenant va en el nombre porque **un durable compartido reparte los mensajes entre quienes lo usan** (A-47).
     */
    public static function durableName(string $service, string $contract, ?string $tenantId = null): string
    {
        Protocol::assertServiceName($service);
        $c = self::parseContract($contract);
        // El servicio conserva sus guiones (`tienda-web__…`); lo que viene después, no.
        $base = "{$service}__state_" . str_replace('-', '_', $c->domain) . '_' . str_replace('-', '_', $c->aggregate) . "_v{$c->major}";
        if ($tenantId === null) {
            return $base;
        }
        self::assertTenant($tenantId);

        return $base . '__' . str_replace('-', '_', $tenantId);
    }

    /** `state.<dom>.<agr>.v<M>.<tenant>.*`, o `….*.*` sin tenant. */
    public static function filterSubject(string $contract, ?string $tenantId = null): string
    {
        $c = self::parseContract($contract);
        if ($tenantId !== null) {
            self::assertTenant($tenantId);
        }

        return "state.{$c->name}." . ($tenantId ?? '*') . '.*';
    }

    /** `dlq.state.<dom>.<agr>.v<M>`: SIN tenant ni clave, para que no se multiplique por ellos. */
    public static function dlqSubject(string $contract): string
    {
        return 'dlq.state.' . self::parseContract($contract)->name;
    }

    // ─── El sobre — funciones PURAS (10-state.md §2.5, §2.6, §4, §6) ──────────────────────────────

    /**
     * Lo que comprueba todo constructor de estado además de la forma del contrato, la clave y el tenant: que los atributos que el
     * SDK rellena no lleguen vacíos y que `time` sea un instante (se normaliza a la forma canónica). El mismo código que daría el
     * parser ante el mismo defecto. Devuelve `time` canónico.
     */
    private static function envelopeInput(string $id, string $source, string $correlationid, string $producerversion, string $time): string
    {
        Envelope::requireAttributes([
            ['id', $id],
            ['source', $source],
            ['correlationid', $correlationid, true],
            ['producerversion', $producerversion, true],
        ]);

        return Envelope::coerceTime($time);
    }

    /**
     * El sobre de un `put`: un CloudEvent normal, con `subject` = la clave, `tenantid` = el token de tenant del subject de NATS,
     * `partitionkey` = la clave, y `data` el ÚLTIMO atributo.
     *
     * **Pura y determinista**: `id` y `time` los da quien llama. Valida lo mismo que `put` (§3.6, salvo L3).
     *
     * @param array<string,mixed> $data
     */
    public static function buildEvent(
        string $contract,
        string $tenantId,
        string $key,
        string $id,
        string $source,
        string $time,
        string $correlationid,
        string $producerversion,
        array $data,
        ?string $classification = null,
        ?string $dataschema = null,
        ?string $causationid = null,
        ?string $traceparent = null,
        ?string $tracestate = null,
    ): FluxEvent {
        $c = self::parseContract($contract);
        self::assertKey($key);
        self::assertTenant($tenantId);
        self::aggregateVersionOf($data);
        $classification = self::assertClassification($classification);
        $time = self::envelopeInput($id, $source, $correlationid, $producerversion, $time);

        return new FluxEvent(
            id: $id,
            source: $source,
            type: self::type($c->name),
            time: $time,
            dataschema: $dataschema ?? self::dataschema($c->name),
            correlationid: $correlationid,
            tenantid: $tenantId,
            producerversion: $producerversion,
            dataclassification: $classification,
            data: $data,
            subject: $key,
            causationid: $causationid,
            partitionkey: $key,
            traceparent: $traceparent,
            tracestate: $tracestate,
        );
    }

    /**
     * La lápida de un `delete`: el mismo subject de NATS que el valor, `type` con `.deleted.`, `dataschema` FIJO,
     * `dataclassification: "internal"` y `data: { aggregateVersion }` y nada más. Al entrar, el valor anterior SALE del stream
     * (A-34). Lleva versión porque `put` la lee para decidir si una escritura nueva es `stale`.
     */
    public static function buildTombstone(
        string $contract,
        string $tenantId,
        string $key,
        string $id,
        string $source,
        string $time,
        string $correlationid,
        string $producerversion,
        mixed $aggregateVersion,
        ?string $causationid = null,
        ?string $traceparent = null,
        ?string $tracestate = null,
    ): FluxEvent {
        $c = self::parseContract($contract);
        self::assertKey($key);
        self::assertTenant($tenantId);
        $version = self::aggregateVersionOf(['aggregateVersion' => $aggregateVersion]);
        $time = self::envelopeInput($id, $source, $correlationid, $producerversion, $time);

        return new FluxEvent(
            id: $id,
            source: $source,
            type: self::tombstoneType($c->name),
            time: $time,
            dataschema: self::TOMBSTONE_DATASCHEMA,
            correlationid: $correlationid,
            tenantid: $tenantId,
            producerversion: $producerversion,
            dataclassification: 'internal',
            data: ['aggregateVersion' => $version],
            subject: $key,
            causationid: $causationid,
            partitionkey: $key,
            traceparent: $traceparent,
            tracestate: $tracestate,
        );
    }

    /**
     * El evento de DLQ de estado: el sobre ORIGINAL con las extensiones `dlq*` y **sin su dato**: `data` pasa a
     * `{ key, revision }`.
     *
     *   · La DLQ guarda 90 días: un dato personal que el dueño ya borró seguiría vivo ahí tres meses y el derecho de supresión
     *     no se cumpliría (10-state.md §6).
     *   · **No conserva `signkeyid` ni `signature`**: firman un `data` que ya no está (07-signing.md §5).
     *   · `data` sigue siendo el ÚLTIMO atributo, con las `dlq*` justo antes.
     *
     * @param string $key La clave del SUBJECT de NATS —la que decidió el broker—, no la del sobre.
     * @param string $dlqtime Lo da quien llama: si lo pusiera esta función, los bytes no serían comparables.
     */
    public static function toDlqEvent(
        FluxEvent $event,
        string $key,
        int $revision,
        ErrorClass $reason,
        int $attempts,
        string $consumer,
        string $error,
        string $dlqtime,
    ): FluxEvent {
        // Se reconstruye SIN `signkeyid` ni `signature` (no se copian) y con `data` sustituido.
        $base = new FluxEvent(
            id: $event->id,
            source: $event->source,
            type: $event->type,
            time: $event->time,
            dataschema: $event->dataschema,
            correlationid: $event->correlationid,
            tenantid: $event->tenantid,
            producerversion: $event->producerversion,
            dataclassification: $event->dataclassification,
            data: ['key' => $key, 'revision' => $revision],
            specversion: $event->specversion,
            datacontenttype: $event->datacontenttype,
            subject: $event->subject,
            causationid: $event->causationid,
            partitionkey: $event->partitionkey,
            traceparent: $event->traceparent,
            tracestate: $event->tracestate,
        );

        return $base->withDlq(
            $reason->value,
            $attempts,
            $consumer,
            // 1024 BYTES UTF-8 sin partir un carácter (04-errors.md §3).
            Text::truncateUtf8Bytes($error, 1024),
            Envelope::coerceTime($dlqtime),
        );
    }

    /**
     * `toDlqEvent` sobre el sobre que `buildEvent` construye de `$original`. Pura. Es lo que ejecuta el arnés (`state-dlq`).
     *
     * @param array<string,mixed> $original Los argumentos de `buildEvent`.
     */
    public static function buildDlqEvent(array $original, int $revision, ErrorClass $reason, int $attempts, string $consumer, string $error, string $dlqtime): FluxEvent
    {
        $event = self::buildEvent(...$original);

        return self::toDlqEvent($event, $original['key'], $revision, $reason, $attempts, $consumer, $error, $dlqtime);
    }

    /**
     * Un mensaje ILEGIBLE (JSON roto, sin sobre) no se copia a la DLQ de estado: no se guarda su cuerpo en ninguna forma —ni los
     * `rawBase64` de los eventos—, porque podría contener el dato. No hay sobre original que conservar, así que se sintetiza uno con lo
     * único fiable, **el subject** (10-state.md §6). `id` y `time` los da quien llama.
     */
    public static function buildUnreadableDlqEvent(
        string $contract,
        string $tenantId,
        string $key,
        int $revision,
        string $source,
        string $producerversion,
        string $id,
        string $time,
        ErrorClass $reason,
        int $attempts,
        string $consumer,
        string $error,
        string $dlqtime,
        ?string $dataschema = null,
    ): FluxEvent {
        $c = self::parseContract($contract);
        $time = self::envelopeInput($id, $source, $id, $producerversion, $time);
        $synth = new FluxEvent(
            id: $id,
            source: $source,
            type: self::type($c->name),
            time: $time,
            dataschema: $dataschema ?? self::dataschema($c->name),
            correlationid: $id,
            tenantid: $tenantId,
            producerversion: $producerversion,
            dataclassification: 'internal',
            data: [],
            subject: $key,
            partitionkey: $key,
        );

        return self::toDlqEvent($synth, $key, $revision, $reason, $attempts, $consumer, $error, $dlqtime);
    }
}
